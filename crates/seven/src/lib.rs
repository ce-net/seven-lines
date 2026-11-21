//! `seven`: the runtime behind the seven lines.
//!
//! A cell is a set of `#[op]`s over `#[record]`s. Every replica runs the same binary; one leader per epoch is
//! elected over a quorum log (live-shop's `shop-replog`: majority vote, majority-acknowledged appends, epoch
//! fencing). Each effect an op makes through its [`Cx`] (a test charge, an AI note, a twin write with its audit row)
//! is a step in that log, keyed by the request's idempotency key and the step's number. A new leader that runs the
//! same request again finds every committed step and reuses it, so an order completes exactly once however many
//! leaders die under it.
use serde::{Deserialize, Serialize};
use std::fmt;
use std::future::Future;
use std::pin::Pin;

pub mod cli;
pub mod cx;
pub mod lock;
pub mod machine;
pub mod node;
pub mod wire;

pub use cx::{read_fleet, Ai, Applied, Cx, Fleet, FleetApply, FleetCell, FleetChange, FleetRead, Pay, Twin, TwinAudit, TwinPut};
pub use inventory;
pub use seven_macros::{cell, op, record};
pub use serde;
pub use serde_json;

/// What `use seven::prelude::*` brings in. `#[cell]` writes that line for you.
pub mod prelude {
    pub use crate::{op, record, summary, Applied, AuditReceipt, Charge, Cx, Error, Fleet, FleetChange, Mode, Money, Note, Quota, Receipt, Record, Result, Sku};
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type BoxFut<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;

/// The cell as `#[cell]` declares it.
#[derive(Debug, Clone, Copy)]
pub struct CellDef {
    pub name: &'static str,
    pub version: u32,
    pub replicas: u32,
    pub skill: &'static str,
    /// `home = Device`: one replica, bound to the device named in cell.lock (an account that is that device's own).
    pub home: Home,
}

/// Where a cell's replicas may live.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Home {
    /// Any nodes in cell.lock: 3, 5 or 7 replicas over the quorum log.
    Fleet,
    /// Exactly one replica, on the device cell.lock names; `serve` refuses to start anywhere else.
    Device,
}

/// What an op needs besides the capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Grant {
    /// The capability chain is enough.
    None,
    /// Also a one-use grant signed by the owner key itself (in ce: after a passkey tap), bound to the caller and to
    /// the sha256 of this exact input. Never delegated: the grant is a single link from the owner.
    Passkey,
}

/// The right a passkey grant names: `grant:<cell>.<op>:<sha256 of the input JSON text>`.
pub fn grant_right(cell: &str, op: &str, body: &str) -> String {
    grant_right_for(cell, op, &machine::sha_hex(body.as_bytes()))
}

/// [`grant_right`] from the input's hash (what a [`NeedsGrant`] answer carries).
pub fn grant_right_for(cell: &str, op: &str, input_hash: &str) -> String {
    format!("grant:{cell}.{op}:{input_hash}")
}

/// The answer to a `grant = Passkey` call that came without a grant: what the owner is asked to sign. The client posts
/// it as a card (in ce: `decision.card` kind `grant`); the owner's tap signs [`one_use`] with the owner's device key;
/// the card's answer is the grant and the client retries the same call (same key, same body) with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NeedsGrant {
    pub cell: String,
    pub op: String,
    /// The caller's key (hex): the only key the grant will admit.
    pub caller: String,
    /// sha256 of the exact input JSON text: the only input the grant will admit.
    pub input_hash: String,
    /// One line for the card: who asks to run what, with the start of the input.
    pub summary: String,
}

impl NeedsGrant {
    pub fn new(cell: &str, op: &str, caller: &str, body: &str) -> NeedsGrant {
        let start: String = body.chars().take(160).collect();
        let more = if body.chars().count() > 160 { " ..." } else { "" };
        NeedsGrant {
            cell: cell.into(),
            op: op.into(),
            caller: caller.into(),
            input_hash: machine::sha_hex(body.as_bytes()),
            summary: format!("key {} asks to run {cell}.{op} once with {start}{more}; this op needs the owner's passkey grant for this exact call", &caller[..caller.len().min(12)]),
        }
    }
}

/// The owner's tap: a one-use grant (`OneUse { cell, op, caller, input_hash, exp }`) for exactly what `n` asks, signed
/// by `device` (the owner root, or an owner device key that cell.lock lists in `owners`), valid `ttl_s` seconds.
pub fn one_use(device: &shop_chain::Identity, n: &NeedsGrant, ttl_s: u64) -> shop_chain::Chain {
    let cav = shop_chain::Caveats { tenant: n.cell.clone(), once: true, until: shop_chain::now() + ttl_s, ..Default::default() };
    shop_chain::Chain::root(device, &n.caller, vec![grant_right_for(&n.cell, &n.op, &n.input_hash)], cav)
}

/// The owner signs a grant for one call: `holder` (the caller's key) may run `op` once with exactly `body`, for
/// `ttl_s` seconds. In ce the owner key signs this only after a passkey tap on the owner's device.
pub fn passkey_grant(owner: &shop_chain::Identity, holder: &str, cell: &str, op: &str, body: &str, ttl_s: u64) -> shop_chain::Chain {
    let cav = shop_chain::Caveats { tenant: cell.into(), once: true, until: shop_chain::now() + ttl_s, ..Default::default() };
    shop_chain::Chain::root(owner, holder, vec![grant_right(cell, op, body)], cav)
}

/// One `#[op]`, registered at link time.
pub struct OpDef {
    pub name: &'static str,
    pub cap: &'static str,
    pub input: &'static str,
    /// The op's return type as written (`Result < Applied >`); [`cli::skill_md`] shows the inner type.
    pub output: &'static str,
    pub grant: Grant,
    pub call: fn(Cx, serde_json::Value) -> BoxFut<serde_json::Value>,
}
inventory::collect!(OpDef);

/// One `#[record]`, registered at link time.
pub struct RecordDef {
    pub cell: &'static str,
    pub name: &'static str,
    pub fields: &'static [(&'static str, &'static str)],
}
inventory::collect!(RecordDef);

pub fn ops() -> Vec<&'static OpDef> {
    let mut v: Vec<_> = inventory::iter::<OpDef>.into_iter().collect();
    v.sort_by_key(|o| o.name);
    v
}

pub fn records() -> Vec<&'static RecordDef> {
    inventory::iter::<RecordDef>.into_iter().collect()
}

/// A record bound to its cell (`#[record(cell = orders)]`).
pub trait Record: Serialize {
    const CELL: &'static str;
    const NAME: &'static str;
}

/// The generated `main`.
pub fn run(def: CellDef) {
    std::process::exit(cli::main(def));
}

/// The capability right a `cap = "orders.place"` names, in chain form: `orders:place` (so `orders:*` covers it).
pub fn right_of(cap: &str) -> String {
    match cap.rsplit_once('.') {
        Some((cell, r)) => format!("{cell}:{r}"),
        None => cap.to_string(),
    }
}

// ------------------------------------------------------------------------------------------------ values

/// A stock-keeping unit.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Sku(pub String);

impl From<&str> for Sku {
    fn from(s: &str) -> Sku {
        Sku(s.to_string())
    }
}

/// An amount in the currency's smallest unit (`{"minor": 49900, "cur": "SEK"}` is 499.00 SEK). `Copy`, so a record
/// can pay with its own field and still be written to the twin afterwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Money {
    pub minor: i64,
    pub cur: Cur,
}

/// An ISO 4217 code: three ASCII capitals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Cur(pub [u8; 3]);

impl Cur {
    pub fn parse(s: &str) -> Option<Cur> {
        let b: [u8; 3] = s.as_bytes().try_into().ok()?;
        b.iter().all(|c| c.is_ascii_uppercase()).then_some(Cur(b))
    }
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap_or("???")
    }
}

impl Serialize for Cur {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Cur {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Cur, D::Error> {
        let s = String::deserialize(d)?;
        Cur::parse(&s).ok_or_else(|| serde::de::Error::custom(format!("{s:?} is not a 3-letter currency code")))
    }
}

impl Money {
    pub fn new(minor: i64, cur: &str) -> Money {
        Money { minor, cur: Cur::parse(cur).expect("a 3-letter currency code") }
    }
}

impl fmt::Display for Money {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{:02} {}", self.minor / 100, (self.minor % 100).abs(), self.cur.as_str())
    }
}

/// Test mode is the default and the only mode this build can charge in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mode {
    Test,
    Live,
}

/// A committed charge: one ledger row on every replica.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Charge {
    pub id: String,
    pub key: String,
    pub amount: Money,
    pub mode: Mode,
    /// The ledger row's sequence number (1-based) and the log index that committed it.
    pub seq: u64,
    pub log_idx: u64,
}

/// An AI answer, as the backend returned it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Note {
    pub text: String,
    pub vendor: String,
    pub model: String,
    pub device: String,
    pub ms: u64,
    pub bucket: String,
    /// asks this agent has left today under the cell's own quota
    pub asks_left: u32,
}

/// A row in the cell's append-only audit stream: `hash = sha256(prev || body)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditReceipt {
    pub seq: u64,
    pub hash: String,
    pub prev: String,
    pub log_idx: u64,
}

/// What `place` returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    pub charge: Charge,
    pub note: Note,
}

impl Receipt {
    pub fn new(charge: Charge, note: Note) -> Receipt {
        Receipt { charge, note }
    }
}

/// An AI quota. `Quota::agent()` is a bucket per calling agent (the chain holder), `per_day` asks a day, kept in
/// the replicated log; the gateway applies its own bucket on top.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quota {
    pub per_day: u32,
}

impl Quota {
    pub fn agent() -> Quota {
        let per_day = std::env::var("SEVEN_AI_PER_DAY").ok().and_then(|v| v.parse().ok()).unwrap_or(20);
        Quota { per_day }
    }
    pub fn per_day(n: u32) -> Quota {
        Quota { per_day: n }
    }
}

/// The prompt for a one-line note about a record.
pub fn summary<R: Record>(r: &R) -> String {
    let body = serde_json::to_string(r).unwrap_or_default();
    format!("In one short sentence for the shop owner, note this new {} {} (amounts are in minor units): {body}", R::CELL, R::NAME)
}

// ------------------------------------------------------------------------------------------------ errors

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    /// The caller's chain does not hold the right (the denial is an audit row).
    Denied(String),
    /// A `grant = Passkey` op called without a grant: what the owner is asked to sign ([`one_use`]).
    NeedsGrant(NeedsGrant),
    /// This replica is not the leader (or lost leadership mid-op); the client retries at the leader.
    NotLeader(Option<String>),
    /// No majority answered in time; the client retries with the same key.
    NoQuorum,
    /// The cell's own AI quota for this agent is spent today.
    Quota { agent: String, used: u32, limit: u32 },
    /// A backend (AI gateway, payment provider) refused or is missing.
    Backend(String),
    Input(String),
}

impl Error {
    pub fn input(e: impl fmt::Display) -> Error {
        Error::Input(e.to_string())
    }
    /// May the client retry the same request (same key) elsewhere?
    pub fn retryable(&self) -> bool {
        matches!(self, Error::NotLeader(_) | Error::NoQuorum)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Denied(r) => write!(f, "denied: {r}"),
            Error::NeedsGrant(n) => write!(f, "needs a grant: {}", n.summary),
            Error::NotLeader(l) => write!(f, "not the leader (leader: {})", l.as_deref().unwrap_or("unknown")),
            Error::NoQuorum => write!(f, "no quorum"),
            Error::Quota { agent, used, limit } => write!(f, "AI quota spent for {agent}: {used}/{limit} today"),
            Error::Backend(e) => write!(f, "backend: {e}"),
            Error::Input(e) => write!(f, "input: {e}"),
        }
    }
}

impl std::error::Error for Error {}
