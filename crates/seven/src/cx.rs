//! `Cx`: what an op gets besides its input. Three layers over cells that already run:
//!
//! - `cx.pay()`: a charge in test mode (the ledger row is a replicated log entry, keyed by request and step), the
//!   saga live-shop runs for checkout reduced to its one money step; `cx.pay().live()` needs the caller's chain to
//!   hold `pay:live` and a live provider, and this build has none, so it never moves real money.
//! - `cx.ai()`: an ask through `sub.gateway` (the owner's subscriptions, metered per bucket there) after the cell's
//!   own per-agent quota, which lives in the log too.
//! - `cx.twin()`: the record into the cell's twin and a row in its hash-chained audit stream.
//!
//! Each call is step `n` of the request. A step already in the log is returned as it was committed, never redone.
use crate::machine::Entry;
use crate::node::Node;
use crate::{AuditReceipt, Charge, Error, Mode, Money, Note, Quota, Record, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

pub struct Cx {
    pub(crate) node: Arc<Node>,
    pub(crate) key: String,
    pub(crate) op: String,
    pub(crate) holder: String,
    pub(crate) rights: Vec<String>,
    pub(crate) chain_id: String,
    n: AtomicU32,
}

impl Cx {
    pub(crate) fn new(node: Arc<Node>, key: String, op: String, holder: String, rights: Vec<String>, chain_id: String) -> Cx {
        Cx { node, key, op, holder, rights, chain_id, n: AtomicU32::new(0) }
    }
    /// The request's idempotency key: every retry of one order carries the same one.
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn op(&self) -> &str {
        &self.op
    }
    /// The calling agent: the public key holding the chain's leaf.
    pub fn caller(&self) -> &str {
        &self.holder
    }
    pub fn chain_id(&self) -> &str {
        &self.chain_id
    }
    pub fn holds(&self, right: &str) -> bool {
        self.rights.iter().any(|g| shop_chain::right_covers(g, right))
    }
    pub fn pay(&self) -> Pay<'_> {
        Pay { cx: self, mode: Mode::Test }
    }
    pub fn ai(&self) -> Ai<'_> {
        Ai { cx: self }
    }
    pub fn twin(&self) -> Twin<'_> {
        Twin { cx: self }
    }

    fn next(&self) -> u32 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
    fn recorded<T: DeserializeOwned>(&self, n: u32) -> Option<T> {
        let v = self.node.log.read(|s, _| s.steps.get(&(self.key.clone(), n)).cloned())?;
        serde_json::from_value(v).ok()
    }
    async fn commit<T: DeserializeOwned>(&self, n: u32, e: Entry) -> Result<T> {
        let v = self.node.propose(e).await?;
        self.node.after_step(n);
        serde_json::from_value::<T>(v).map_err(Error::input)
    }
    fn agent(&self) -> String {
        self.holder.chars().take(16).collect()
    }
}

pub struct Pay<'a> {
    cx: &'a Cx,
    mode: Mode,
}

impl<'a> Pay<'a> {
    /// Ask for a live charge. Refused unless the caller's chain holds `pay:live`; and then refused again, because
    /// this build ships no live provider.
    pub fn live(mut self) -> Pay<'a> {
        self.mode = Mode::Live;
        self
    }

    pub async fn charge(self, amount: Money) -> Result<Charge> {
        let cx = self.cx;
        let n = cx.next();
        if let Some(c) = cx.recorded::<Charge>(n) {
            return Ok(c);
        }
        if amount.minor <= 0 {
            return Err(Error::Input(format!("amount {amount} is not a positive amount")));
        }
        if self.mode == Mode::Live {
            if !cx.holds("pay:live") {
                return Err(Error::Denied("pay:live (live payments need an explicit passkey grant)".into()));
            }
            return Err(Error::Backend("no live payment provider in this build: test mode only, never real money".into()));
        }
        cx.commit(n, Entry::Charge { key: cx.key.clone(), n, amount, mode: Mode::Test, by: cx.agent() }).await
    }
}

pub struct Ai<'a> {
    cx: &'a Cx,
}

impl<'a> Ai<'a> {
    pub async fn ask(self, quota: Quota, prompt: impl Into<String>) -> Result<Note> {
        let cx = self.cx;
        let n = cx.next();
        if let Some(note) = cx.recorded::<Note>(n) {
            return Ok(note);
        }
        let agent = cx.agent();
        let day = shop_chain::now() / 86_400;
        let used = cx.node.log.read(|s, _| s.ai_used(&agent, day));
        if used >= quota.per_day {
            return Err(Error::Quota { agent, used, limit: quota.per_day });
        }
        let mut note = cx.node.ai.ask(&prompt.into()).await?;
        note.asks_left = quota.per_day - used - 1;
        note.bucket = format!("{} / agent:{agent}", note.bucket);
        cx.commit(n, Entry::Ai { key: cx.key.clone(), n, agent, day, note }).await
    }
}

pub struct Twin<'a> {
    cx: &'a Cx,
}

impl<'a> Twin<'a> {
    pub fn put<R: Record>(self, r: &R) -> TwinPut<'a> {
        TwinPut { cx: self.cx, record: R::NAME, body: serde_json::to_value(r).unwrap_or(Value::Null) }
    }
}

pub struct TwinPut<'a> {
    cx: &'a Cx,
    record: &'static str,
    body: Value,
}

impl<'a> TwinPut<'a> {
    /// Write the record and an audit row naming `of` (a charge, a note: whatever this write is evidence of).
    pub fn audit<T: Serialize>(self, of: &T) -> TwinAudit<'a> {
        TwinAudit { put: self, of: serde_json::to_value(of).unwrap_or(Value::Null) }
    }
}

pub struct TwinAudit<'a> {
    put: TwinPut<'a>,
    of: Value,
}

type Fut<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

async fn twin_write(put: TwinPut<'_>, of: Value) -> Result<AuditReceipt> {
    let cx = put.cx;
    let n = cx.next();
    if let Some(r) = cx.recorded::<AuditReceipt>(n) {
        return Ok(r);
    }
    cx.commit(n, Entry::Twin { key: cx.key.clone(), n, record: put.record.to_string(), body: put.body, of, by: cx.agent() }).await
}

impl<'a> IntoFuture for TwinAudit<'a> {
    type Output = Result<AuditReceipt>;
    type IntoFuture = Fut<'a, AuditReceipt>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(twin_write(self.put, self.of))
    }
}

impl<'a> IntoFuture for TwinPut<'a> {
    type Output = Result<AuditReceipt>;
    type IntoFuture = Fut<'a, AuditReceipt>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(twin_write(self, Value::Null))
    }
}
