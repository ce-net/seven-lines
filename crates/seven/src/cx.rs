//! `Cx`: what an op gets besides its input. Three layers over cells that already run:
//!
//! - `cx.pay()`: a charge in test mode (the ledger row is a replicated log entry, keyed by request and step), the
//!   saga live-shop runs for checkout reduced to its one money step; `cx.pay().live()` needs the caller's chain to
//!   hold `pay:live` and a live provider, and this build has none, so it never moves real money.
//! - `cx.ai()`: an ask through `sub.gateway` (the owner's subscriptions, metered per bucket there) after the cell's
//!   own per-agent quota, which lives in the log too.
//! - `cx.twin()`: the record into the cell's twin and a row in its hash-chained audit stream.
//! - `cx.fleet()`: a read-only snapshot of the cells on this node (or what `SEVEN_FLEET_CMD` reports for a whole
//!   mesh), and `apply` for one closed set of changes; today only `Restart { cell }`, of a cell the snapshot lists.
//!
//! Each call is step `n` of the request. A step already in the log is returned as it was committed, never redone.
use crate::machine::Entry;
use crate::node::Node;
use crate::{AuditReceipt, Charge, Error, Mode, Money, Note, Quota, Record, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
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
    pub fn fleet(&self) -> FleetRead<'_> {
        FleetRead { cx: self }
    }

    /// Run a side effect once for step `n` of this request: a replay returns the recorded result.
    pub async fn step<F, Fut2>(&self, kind: &str, f: F) -> Result<Value>
    where
        F: FnOnce() -> Fut2,
        Fut2: Future<Output = Result<Value>>,
    {
        let n = self.next();
        if let Some(v) = self.recorded::<Value>(n) {
            return Ok(v["out"].clone());
        }
        let out = f().await?;
        let v: Value = self.commit(n, Entry::Step { key: self.key.clone(), n, kind: kind.into(), out, by: self.agent() }).await?;
        Ok(v["out"].clone())
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

// ------------------------------------------------------------------------------------------------ fleet

/// One cell as the snapshot sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FleetCell {
    pub name: String,
    pub unit: String,
    pub active: String,
    pub sub: String,
    pub healthy: bool,
}

/// What `cx.fleet().snapshot()` returns: cells, plus whatever the deployment's `SEVEN_FLEET_CMD` adds (nodes, key
/// holdings and expiries, AI spend, incidents) under `more`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fleet {
    pub at: u64,
    pub source: String,
    pub cells: Vec<FleetCell>,
    #[serde(default)]
    pub more: Value,
}

impl Fleet {
    pub fn unhealthy(&self) -> Vec<&FleetCell> {
        self.cells.iter().filter(|c| !c.healthy).collect()
    }
    /// The one change this build can propose on its own: restart the first unhealthy cell.
    pub fn proposal(&self) -> FleetChange {
        match self.unhealthy().first() {
            Some(c) => FleetChange::Restart { cell: c.name.clone() },
            None => FleetChange::Other { what: "nothing to change: every cell is healthy".into() },
        }
    }
    /// The prompt for the owner's question over this snapshot.
    pub fn question(&self, q: &str) -> String {
        let rows: Vec<String> = self.cells.iter().map(|c| format!("{} {}/{}", c.name, c.active, c.sub)).collect();
        format!("You watch a fleet of cells. Answer the owner in one or two sentences, and name the cell to restart if one is down.\nQuestion: {q}\nCells: {}", rows.join("; "))
    }
}

/// The closed set of changes `apply` knows. Anything else is refused as not yet supported.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FleetChange {
    Restart { cell: String },
    Other { what: String },
}

/// What `apply` did.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Applied {
    pub change: FleetChange,
    pub unit: String,
    pub ok: bool,
    pub active_after: String,
    pub ms: u64,
}

pub struct FleetRead<'a> {
    cx: &'a Cx,
}

fn sh_out(cmd: &str) -> Result<String> {
    let o = std::process::Command::new("sh").args(["-c", cmd]).output().map_err(|e| Error::Backend(format!("{cmd}: {e}")))?;
    if !o.status.success() {
        return Err(Error::Backend(format!("{cmd}: {}", String::from_utf8_lossy(&o.stderr).trim())));
    }
    Ok(String::from_utf8_lossy(&o.stdout).to_string())
}

/// The snapshot without the log: `SEVEN_FLEET_CMD` (prints a `Fleet` as JSON), else the user's systemd units that
/// match `SEVEN_FLEET_UNITS` (default `seven-*`).
pub fn read_fleet() -> Result<Fleet> {
    if let Ok(cmd) = std::env::var("SEVEN_FLEET_CMD") {
        return serde_json::from_str(&sh_out(&cmd)?).map_err(Error::input);
    }
    let pat = std::env::var("SEVEN_FLEET_UNITS").unwrap_or_else(|_| "seven-*".into());
    let loaded: Vec<Value> = serde_json::from_str(&sh_out(&format!("systemctl --user list-units --all --plain --no-legend --output=json '{pat}'"))?).map_err(Error::input)?;
    // a stopped unit is often unloaded; its unit file still says the cell exists
    let files: Vec<Value> = serde_json::from_str(&sh_out(&format!("systemctl --user list-unit-files --no-legend --output=json '{pat}'"))?).map_err(Error::input)?;
    let mut cells: Vec<FleetCell> = loaded
        .iter()
        .map(|r| {
            let unit = r["unit"].as_str().unwrap_or("").to_string();
            let name = unit.trim_end_matches(".service").to_string();
            let active = r["active"].as_str().unwrap_or("").to_string();
            let sub = r["sub"].as_str().unwrap_or("").to_string();
            let healthy = active == "active" && sub == "running";
            FleetCell { name, unit, active, sub, healthy }
        })
        .collect();
    for f in &files {
        let unit = f["unit_file"].as_str().unwrap_or("").to_string();
        if !unit.ends_with(".service") || unit.contains('@') || cells.iter().any(|c| c.unit == unit) {
            continue;
        }
        let name = unit.trim_end_matches(".service").to_string();
        cells.push(FleetCell { name, unit, active: "inactive".into(), sub: "dead".into(), healthy: false });
    }
    cells.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Fleet { at: shop_chain::now(), source: format!("systemd --user {pat}"), cells, more: Value::Null })
}

fn restart(unit: &str) -> Result<(bool, String)> {
    let cmd = std::env::var("SEVEN_FLEET_RESTART_CMD").unwrap_or_else(|_| "systemctl --user restart '{unit}'".into());
    let ok = std::process::Command::new("sh").args(["-c", &cmd.replace("{unit}", unit)]).status().map(|s| s.success()).unwrap_or(false);
    let after = sh_out(&format!("systemctl --user is-active '{unit}' || true")).unwrap_or_default().trim().to_string();
    Ok((ok, after))
}

impl<'a> FleetRead<'a> {
    /// Read the fleet once for this request (a replay sees the same snapshot).
    pub async fn snapshot(self) -> Result<Fleet> {
        let v = self.cx.step("fleet.snapshot", || async { serde_json::to_value(read_fleet()?).map_err(Error::input) }).await?;
        serde_json::from_value(v).map_err(Error::input)
    }
    /// Apply one change. Only `Restart` of a cell the current snapshot lists; the result is an audit row.
    pub fn apply(self, c: FleetChange) -> FleetApply<'a> {
        FleetApply { cx: self.cx, change: c }
    }
}

pub struct FleetApply<'a> {
    cx: &'a Cx,
    change: FleetChange,
}

impl<'a> FleetApply<'a> {
    /// The change is always written to the audit stream; `.audit()` says so at the call site.
    pub fn audit(self) -> Self {
        self
    }
}

impl<'a> IntoFuture for FleetApply<'a> {
    type Output = Result<Applied>;
    type IntoFuture = Fut<'a, Applied>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let FleetChange::Restart { cell } = &self.change else {
                return Err(Error::Input(format!("NotYet: only Restart is applied in this build, not {:?}", self.change)));
            };
            let change = self.change.clone();
            let cell = cell.clone();
            let v = self
                .cx
                .step("fleet.restart", || async move {
                    let f = read_fleet()?;
                    let c = f.cells.iter().find(|c| c.name == cell || c.unit == cell).ok_or_else(|| Error::Input(format!("no cell {cell} in the fleet snapshot")))?;
                    let t0 = std::time::Instant::now();
                    let (ok, active_after) = restart(&c.unit)?;
                    let a = Applied { change, unit: c.unit.clone(), ok, active_after, ms: t0.elapsed().as_millis() as u64 };
                    serde_json::to_value(a).map_err(Error::input)
                })
                .await?;
            serde_json::from_value(v).map_err(Error::input)
        })
    }
}
