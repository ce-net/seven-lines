//! `seven.worker@1`: the contract between `seven-host` and a cell worker written in TypeScript or Python.
//!
//! ```text
//! api! { pub SevenWorker = "seven.worker@1";
//!   1 => Describe(()) -> CellDef;                                  // host -> worker
//!   2 => Invoke(InvokeReq { op, request, caller, input }) -> Result<Value, Refused>;   // host -> worker
//!   3 => Cx(CxReq { request, step, layer, args }) -> Result<Value, CxError>; }        // worker -> host
//! ```
//!
//! Replication, the gate, the quorum log, `cx` and exactly-once stay in the Rust `seven` runtime. The worker is a
//! process beside one replica, on its stdin and stdout, one JSON frame per line ([`Frame`]). The gate runs in the host
//! before `Invoke`, so a refused caller never reaches the worker; the worker's only way out is `Cx`, and the host keys
//! every `Cx` by `(request, step)` in the log exactly as for a Rust op, so a retried `Invoke` on a new leader gets
//! the recorded results back and a charge happens once.
//!
//! One `Invoke` is in flight at a time. While it runs the worker may send any number of `Cx` frames, each answered
//! by one `CxReply`, and ends it with one `Return`.
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The contract name and version, as `api!` would carry it.
pub const CONTRACT: &str = "seven.worker@1";

/// The cell as the worker declares it (owned: it arrives at run time).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CellDef {
    pub name: String,
    #[serde(default = "one")]
    pub version: u32,
    #[serde(default)]
    pub replicas: Option<u32>,
    /// `"Fleet"` (default) or `"Device"`.
    #[serde(default = "fleet")]
    pub home: String,
    #[serde(default)]
    pub skill: String,
    #[serde(default)]
    pub records: Vec<RecordSpec>,
    pub ops: Vec<OpSpec>,
}

fn one() -> u32 {
    1
}
fn fleet() -> String {
    "Fleet".into()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordSpec {
    pub name: String,
    /// `[field, type]` in declaration order; types use the Rust names (`Sku`, `u32`, `Money`, `String`, ...).
    pub fields: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpSpec {
    pub name: String,
    /// `"<cell>.<right>"`, e.g. `orders.place`.
    pub cap: String,
    /// The input type's name (a record, or a scalar).
    pub input: String,
    /// The type it answers with when it succeeds.
    pub output: String,
    /// `"None"` (default) or `"Passkey"`.
    #[serde(default = "none")]
    pub grant: String,
}

fn none() -> String {
    "None".into()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InvokeReq {
    pub op: String,
    /// The request's idempotency key: every retry of one call carries the same one.
    pub request: String,
    /// The calling agent's key (hex), as the gate verified it.
    pub caller: String,
    pub input: Value,
}

/// The `Cx` layers. `args` per layer:
/// - `Pay`: `{ "charge": Money }` (test mode; `"live": true` is refused without `pay:live` and has no provider)
/// - `Ai`: `{ "quota": { "agent": true } | { "per_day": N }, "prompt": String }`
/// - `Twin`: `{ "record": name, "body": Value, "of": Value }` (the write and its audit row)
/// - `Store`: `{ "all": record name }` (a read, not a step)
/// - `Fleet`: `{ "snapshot": true }`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Layer {
    Pay,
    Ai,
    Twin,
    Store,
    Fleet,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CxReq {
    pub request: String,
    /// The worker's count of `Cx` calls in this request (0-based). The host keeps its own count; a mismatch is refused.
    pub step: u32,
    pub layer: Layer,
    pub args: Value,
}

/// One line on the pipe.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "m")]
pub enum Frame {
    /// host -> worker
    Describe,
    /// worker -> host: the answer to `Describe`
    Def { def: CellDef },
    /// host -> worker
    Invoke(InvokeReq),
    /// worker -> host, inside an `Invoke`
    Cx(CxReq),
    /// host -> worker: the answer to a `Cx` (`ok` or `err`; `err` is a seven `Error` as JSON)
    CxReply {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ok: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        err: Option<Value>,
    },
    /// worker -> host: the end of an `Invoke` (`ok`, or `err` as a seven `Error` JSON or a string)
    Return {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ok: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        err: Option<Value>,
    },
}

impl Frame {
    pub fn line(&self) -> String {
        let mut s = serde_json::to_string(self).unwrap_or_default();
        s.push('\n');
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_are_tagged_lines() {
        assert_eq!(Frame::Describe.line(), "{\"m\":\"Describe\"}\n");
        let f: Frame = serde_json::from_str(r#"{"m":"Cx","request":"A","step":0,"layer":"Pay","args":{"charge":{"minor":1,"cur":"SEK"}}}"#).unwrap();
        assert!(matches!(f, Frame::Cx(CxReq { layer: Layer::Pay, step: 0, .. })));
        let r: Frame = serde_json::from_str(r#"{"m":"Return","ok":{"x":1}}"#).unwrap();
        assert_eq!(r, Frame::Return { ok: Some(serde_json::json!({"x":1})), err: None });
        let d: Frame = serde_json::from_str(r#"{"m":"Def","def":{"name":"orders","ops":[{"name":"place","cap":"orders.place","input":"Order","output":"Receipt"}]}}"#).unwrap();
        let Frame::Def { def } = d else { panic!() };
        assert_eq!((def.version, def.home.as_str(), def.ops[0].grant.as_str()), (1, "Fleet", "None"));
    }
}
