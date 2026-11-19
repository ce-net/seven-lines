//! The replicated state machine: every replica applies the same committed entries in the same order.
//!
//! Every effect is keyed by `(request key, step number)` and the first committed entry for a key wins. A leader
//! that dies after committing a step leaves that step in the log; the next leader replays the request, finds the
//! step and reuses its result. A second `Charge` for the same step (two leaders racing, a client retrying) adds no
//! ledger row and returns the first charge.
use crate::{AuditReceipt, Charge, Mode, Money, Note};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Entry {
    /// A test-mode charge: the ledger row is this entry.
    Charge { key: String, n: u32, amount: Money, mode: Mode, by: String },
    /// An AI note, counted against `agent`'s bucket for `day`.
    Ai { key: String, n: u32, agent: String, day: u64, note: Note },
    /// The record into the twin plus an audit row that names what it is evidence of.
    Twin { key: String, n: u32, record: String, body: Value, of: Value, by: String },
    /// The op finished: its output, kept for every retry of the key.
    Done { key: String, op: String, out: Value },
    /// A refused call, audited.
    Denied { op: String, right: String, holder: String, why: String, at: u64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerRow {
    pub seq: u64,
    pub key: String,
    pub n: u32,
    pub id: String,
    pub amount: Money,
    pub mode: Mode,
    pub by: String,
    pub log_idx: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditRow {
    pub seq: u64,
    pub kind: String,
    pub key: String,
    pub body: Value,
    pub prev: String,
    pub hash: String,
    pub log_idx: u64,
}

#[derive(Default)]
pub struct State {
    pub steps: BTreeMap<(String, u32), Value>,
    pub ledger: Vec<LedgerRow>,
    pub twin: BTreeMap<String, Value>,
    pub audit: Vec<AuditRow>,
    pub done: BTreeMap<String, Value>,
    pub ai_used: BTreeMap<(String, u64), u32>,
}

pub fn sha_hex(b: &[u8]) -> String {
    hex::encode(Sha256::digest(b))
}

impl State {
    fn audit(&mut self, idx: u64, kind: &str, key: &str, body: Value) -> AuditReceipt {
        let prev = self.audit.last().map(|a| a.hash.clone()).unwrap_or_else(|| "0".repeat(64));
        let seq = self.audit.len() as u64 + 1;
        let mut h = Sha256::new();
        h.update(prev.as_bytes());
        h.update(seq.to_le_bytes());
        h.update(kind.as_bytes());
        h.update(key.as_bytes());
        h.update(body.to_string().as_bytes());
        let hash = hex::encode(h.finalize());
        self.audit.push(AuditRow { seq, kind: kind.into(), key: key.into(), body, prev: prev.clone(), hash: hash.clone(), log_idx: idx });
        AuditReceipt { seq, hash, prev, log_idx: idx }
    }

    /// Recompute the audit hash chain from genesis.
    pub fn audit_chain_ok(&self) -> bool {
        let mut prev = "0".repeat(64);
        for a in &self.audit {
            let mut h = Sha256::new();
            h.update(prev.as_bytes());
            h.update(a.seq.to_le_bytes());
            h.update(a.kind.as_bytes());
            h.update(a.key.as_bytes());
            h.update(a.body.to_string().as_bytes());
            if a.prev != prev || hex::encode(h.finalize()) != a.hash {
                return false;
            }
            prev = a.hash.clone();
        }
        true
    }

    pub fn ai_used(&self, agent: &str, day: u64) -> u32 {
        self.ai_used.get(&(agent.to_string(), day)).copied().unwrap_or(0)
    }

    /// Keys that have committed steps but no `Done`: orders in flight.
    pub fn inflight(&self) -> Vec<(String, u32)> {
        let mut m: BTreeMap<String, u32> = BTreeMap::new();
        for (k, n) in self.steps.keys() {
            if !self.done.contains_key(k) {
                let e = m.entry(k.clone()).or_default();
                *e = (*e).max(*n);
            }
        }
        m.into_iter().collect()
    }
}

impl shop_replog::Machine for State {
    type Entry = Entry;
    type Out = Value;

    fn apply(&mut self, idx: u64, e: &Entry) -> Value {
        match e {
            Entry::Charge { key, n, amount, mode, by } => {
                if let Some(v) = self.steps.get(&(key.clone(), *n)) {
                    return v.clone();
                }
                let seq = self.ledger.len() as u64 + 1;
                let id = format!("ch_test_{}", &sha_hex(format!("{key}/{n}").as_bytes())[..16]);
                self.ledger.push(LedgerRow { seq, key: key.clone(), n: *n, id: id.clone(), amount: amount.clone(), mode: *mode, by: by.clone(), log_idx: idx });
                let c = Charge { id, key: key.clone(), amount: amount.clone(), mode: *mode, seq, log_idx: idx };
                let v = serde_json::to_value(&c).unwrap();
                self.steps.insert((key.clone(), *n), v.clone());
                v
            }
            Entry::Ai { key, n, agent, day, note } => {
                if let Some(v) = self.steps.get(&(key.clone(), *n)) {
                    return v.clone();
                }
                *self.ai_used.entry((agent.clone(), *day)).or_default() += 1;
                let v = serde_json::to_value(note).unwrap();
                self.steps.insert((key.clone(), *n), v.clone());
                v
            }
            Entry::Twin { key, n, record, body, of, by } => {
                if let Some(v) = self.steps.get(&(key.clone(), *n)) {
                    return v.clone();
                }
                self.twin.insert(format!("{record}/{key}"), body.clone());
                let r = self.audit(idx, "twin.put", key, json!({ "record": record, "body_sha256": sha_hex(body.to_string().as_bytes()), "of": of, "by": by }));
                let v = serde_json::to_value(&r).unwrap();
                self.steps.insert((key.clone(), *n), v.clone());
                v
            }
            Entry::Done { key, op, out } => {
                if let Some(v) = self.done.get(key) {
                    return v.clone();
                }
                self.done.insert(key.clone(), out.clone());
                let _ = op;
                out.clone()
            }
            Entry::Denied { op, right, holder, why, at } => {
                let r = self.audit(idx, "denied", "", json!({ "op": op, "right": right, "holder": holder, "why": why, "at": at }));
                serde_json::to_value(&r).unwrap()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shop_replog::Machine;

    fn charge(key: &str, n: u32) -> Entry {
        Entry::Charge { key: key.into(), n, amount: Money::new(4990, "SEK"), mode: Mode::Test, by: "t".into() }
    }

    #[test]
    fn a_second_charge_for_the_same_step_adds_no_row() {
        let mut s = State::default();
        let a = s.apply(1, &charge("k1", 0));
        let b = s.apply(2, &charge("k1", 0));
        assert_eq!(a, b);
        assert_eq!(s.ledger.len(), 1);
        s.apply(3, &charge("k2", 0));
        assert_eq!(s.ledger.len(), 2);
    }

    #[test]
    fn done_is_first_wins_and_inflight_clears() {
        let mut s = State::default();
        s.apply(1, &charge("k", 0));
        assert_eq!(s.inflight(), vec![("k".to_string(), 0)]);
        let first = s.apply(2, &Entry::Done { key: "k".into(), op: "place".into(), out: json!(1) });
        let again = s.apply(3, &Entry::Done { key: "k".into(), op: "place".into(), out: json!(2) });
        assert_eq!((first, again), (json!(1), json!(1)));
        assert!(s.inflight().is_empty());
    }

    #[test]
    fn audit_chain_verifies_and_detects_tampering() {
        let mut s = State::default();
        for i in 0..3 {
            s.apply(i + 1, &Entry::Twin { key: format!("k{i}"), n: 2, record: "Order".into(), body: json!({"i": i}), of: json!("c"), by: "t".into() });
        }
        s.apply(9, &Entry::Denied { op: "place".into(), right: "orders:place".into(), holder: "x".into(), why: "no right".into(), at: 0 });
        assert_eq!(s.audit.len(), 4);
        assert!(s.audit_chain_ok());
        s.audit[1].body = json!({"i": 99});
        assert!(!s.audit_chain_ok());
    }

    #[test]
    fn ai_steps_count_against_the_agents_bucket_once() {
        let mut s = State::default();
        let note = Note { text: "x".into(), vendor: "local".into(), model: "m".into(), device: "d".into(), ms: 0, bucket: "b".into(), asks_left: 1 };
        let e = Entry::Ai { key: "k".into(), n: 1, agent: "a".into(), day: 7, note };
        s.apply(1, &e);
        s.apply(2, &e);
        assert_eq!(s.ai_used("a", 7), 1);
    }
}
