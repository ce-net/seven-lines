//! One replica: the quorum log, the TCP door, the op runner and the AI backend.
use crate::machine::{Entry, State};
use crate::wire::{admin_body, Msg};
use crate::{right_of, CellDef, Cx, Error, Note, Result};
use serde_json::{json, Value};
use shop_replog::{Config, Log, ProposeError, Role};
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

/// The op code a request proof signs: two bytes of sha256(op name), the same in every replica and client.
pub fn op_code(op: &str) -> u16 {
    let h = crate::machine::sha_hex(op.as_bytes());
    u16::from_str_radix(&h[..4], 16).unwrap_or(0)
}

/// The bytes a `Call` proof signs.
pub fn signed_body(key: &str, body: &str) -> String {
    format!("{key}\n{body}")
}

pub enum AiBackend {
    /// `ce call sub.gateway.Ask` on this node: the owner's subscriptions, the gateway's buckets.
    Gateway { ce: PathBuf, vendor: String, model: String },
    /// No gateway (CI, a laptop without ce): a deterministic note, labelled `vendor: local`.
    Local,
}

impl AiBackend {
    pub fn from_env() -> AiBackend {
        let want = std::env::var("SEVEN_AI").unwrap_or_else(|_| "auto".into());
        let ce = which("ce");
        match (want.as_str(), ce) {
            ("local", _) | ("auto", None) => AiBackend::Local,
            (_, Some(ce)) => AiBackend::Gateway {
                ce,
                vendor: std::env::var("SEVEN_AI_VENDOR").unwrap_or_else(|_| "claude".into()),
                model: std::env::var("SEVEN_AI_MODEL").unwrap_or_else(|_| "haiku".into()),
            },
            (_, None) => AiBackend::Local,
        }
    }

    pub fn name(&self) -> String {
        match self {
            AiBackend::Gateway { vendor, model, .. } => format!("sub.gateway ({vendor}/{model})"),
            AiBackend::Local => "local".into(),
        }
    }

    pub async fn ask(&self, prompt: &str) -> Result<Note> {
        let t0 = Instant::now();
        match self {
            AiBackend::Local => {
                // SEVEN_AI_LOCAL_MS: stand in for a model's latency, so a proof can kill the leader mid-order
                let ms = std::env::var("SEVEN_AI_LOCAL_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(0u64);
                tokio::time::sleep(Duration::from_millis(ms)).await;
                let digest = &crate::machine::sha_hex(prompt.as_bytes())[..8];
                Ok(Note {
                    text: format!("local note {digest}: {}", prompt.chars().rev().take(80).collect::<String>().chars().rev().collect::<String>()),
                    vendor: "local".into(),
                    model: "none".into(),
                    device: hostname(),
                    ms,
                    bucket: "local".into(),
                    asks_left: 0,
                })
            }
            AiBackend::Gateway { ce, vendor, model } => {
                let req = json!({ "vendor": vendor, "prompt": prompt, "model": model, "files": [], "bucket": "" });
                let out = tokio::time::timeout(
                    Duration::from_secs(180),
                    tokio::process::Command::new(ce).args(["call", "sub.gateway.Ask"]).arg(req.to_string()).arg("--json").kill_on_drop(true).output(),
                )
                .await
                .map_err(|_| Error::Backend("sub.gateway: no answer in 180 s".into()))?
                .map_err(|e| Error::Backend(format!("sub.gateway: {e}")))?;
                let text = String::from_utf8_lossy(&out.stdout);
                let v: Value = serde_json::from_str(text.trim()).map_err(|_| {
                    let err = String::from_utf8_lossy(&out.stderr);
                    Error::Backend(format!("sub.gateway: {}", err.trim().chars().take(300).collect::<String>()))
                })?;
                let v = v.get("Ok").cloned().unwrap_or(v);
                if v.get("text").is_none() {
                    return Err(Error::Backend(format!("sub.gateway refused: {}", v.to_string().chars().take(300).collect::<String>())));
                }
                let s = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or_default().to_string();
                Ok(Note {
                    text: s("text").trim().to_string(),
                    vendor: s("vendor"),
                    model: s("model"),
                    device: s("device"),
                    ms: v.get("ms").and_then(|x| x.as_u64()).unwrap_or(t0.elapsed().as_millis() as u64),
                    bucket: format!("sub.gateway:{}", s("bucket")),
                    asks_left: 0,
                })
            }
        }
    }
}

pub fn which(bin: &str) -> Option<PathBuf> {
    let home = std::env::var("HOME").unwrap_or_default();
    let path = std::env::var_os("PATH").unwrap_or_default();
    std::env::split_paths(&path)
        .chain([PathBuf::from(&home).join(".local/bin"), PathBuf::from(&home).join(".cargo/bin")])
        .map(|d| d.join(bin))
        .find(|p| p.is_file())
}

pub fn hostname() -> String {
    std::env::var("SEVEN_NODE")
        .ok()
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok().map(|s| s.trim().to_string()))
        .or_else(|| std::process::Command::new("hostname").output().ok().map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string()))
        .unwrap_or_else(|| "?".into())
}

pub struct Node {
    pub def: CellDef,
    pub me: usize,
    pub addrs: Vec<String>,
    pub root: String,
    pub log: Log<State>,
    pub ai: AiBackend,
    pub node_name: String,
    peer_ips: Vec<IpAddr>,
    keys: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    crash_after: AtomicI64,
    started: Instant,
}

impl Node {
    pub fn open(def: CellDef, me: usize, addrs: Vec<String>, root: String, dir: PathBuf, ai: AiBackend) -> Arc<Node> {
        let peers = Arc::new(crate::wire::TcpPeers::new(addrs.clone()));
        let log = Log::<State>::open(Config { me, peers: addrs.clone(), dir, unsafe_local_ack: false }, peers);
        let peer_ips = addrs.iter().filter_map(|a| a.parse::<std::net::SocketAddr>().ok()).map(|s| s.ip()).collect();
        let single = addrs.len() == 1;
        let node = Arc::new(Node {
            def,
            me,
            addrs,
            root,
            log,
            ai,
            node_name: hostname(),
            peer_ips,
            keys: Mutex::new(HashMap::new()),
            crash_after: AtomicI64::new(i64::MIN),
            started: Instant::now(),
        });
        if single {
            // A one-replica cell (`home = Device`) has no peer to acknowledge its epoch's no-op, so the log never
            // reports ready. One local `Epoch` entry commits it (with one member, a proposal commits on append).
            let n = node.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    if n.log.ready() {
                        continue;
                    }
                    if n.log.status().role == Role::Leader {
                        let _ = n.log.propose(Entry::Epoch).await;
                    }
                }
            });
        }
        node
    }

    /// Propose `e`; a lost majority is retried (the entry is idempotent), a lost leadership is the client's to retry.
    pub async fn propose(&self, e: Entry) -> Result<Value> {
        let deadline = Instant::now() + Duration::from_secs(12);
        loop {
            match self.log.propose(e.clone()).await {
                Ok(v) => return Ok(v),
                Err(ProposeError::NotLeader(l)) => return Err(Error::NotLeader(l.map(|i| self.addrs[i].clone()))),
                Err(ProposeError::NoQuorum) if Instant::now() < deadline => continue,
                Err(ProposeError::NoQuorum) => return Err(Error::NoQuorum),
            }
        }
    }

    /// Chaos hook: a replica armed with `Crash { after_step }` dies by SIGKILL right after that step commits.
    pub(crate) fn after_step(&self, n: u32) {
        if self.crash_after.load(Ordering::SeqCst) == n as i64 {
            kill_self();
        }
    }

    fn leader_addr(&self) -> Option<String> {
        self.log.status().leader.map(|i| self.addrs[i].clone())
    }

    pub fn status(&self) -> Value {
        let st = self.log.status();
        let (ledger, done, audit, head, chain_ok, inflight, twin) = self.log.read(|s, _| {
            (s.ledger.len(), s.done.len(), s.audit.len(), s.audit.last().map(|a| a.hash.clone()).unwrap_or_default(), s.audit_chain_ok(), s.inflight(), s.twin.len())
        });
        json!({
            "cell": self.def.name, "version": self.def.version, "me": self.me, "addr": self.addrs[self.me],
            "node": self.node_name, "pid": std::process::id(),
            "role": match st.role { Role::Leader => "leader", Role::Candidate => "candidate", Role::Follower => "follower" },
            "ready": self.log.ready(), "term": st.term, "commit": st.commit, "last": st.last,
            "leader": st.leader.map(|i| self.addrs[i].clone()),
            "ledger": ledger, "done": done, "audit": audit, "audit_head": head, "audit_chain_ok": chain_ok,
            "twin": twin, "inflight": inflight, "ai": self.ai.name(), "uptime_ms": self.started.elapsed().as_millis() as u64,
        })
    }

    fn dump(&self) -> Value {
        self.log.read(|s, applied| {
            json!({
                "applied": applied, "ledger": s.ledger, "done": s.done, "audit": s.audit,
                "audit_chain_ok": s.audit_chain_ok(), "twin": s.twin,
            })
        })
    }

    fn verify(&self, auth: &shop_chain::Auth, op: &str, body: &str, right: &str) -> std::result::Result<shop_chain::Verified, String> {
        let v = shop_chain::verify_request(&self.root, auth, self.def.name, op_code(op), body, shop_chain::now()).map_err(|e| e.0)?;
        if !v.allows(right) {
            return Err(format!("chain does not hold {right}"));
        }
        Ok(v)
    }

    async fn deny(&self, op: &str, right: &str, holder: &str, why: &str) -> Value {
        // the denial is an audit row when this replica can write one; a follower just refuses
        let _ = self
            .propose(Entry::Denied { op: op.into(), right: right.into(), holder: holder.into(), why: why.into(), at: shop_chain::now() })
            .await;
        json!({ "err": Error::Denied(format!("{right}: {why}")) })
    }

    pub async fn call(self: &Arc<Node>, op: &str, key: &str, body: &str, auth: &shop_chain::Auth) -> Value {
        self.call_granted(op, key, body, auth, None).await
    }

    /// A `grant = Passkey` op: the grant must be one link from the owner root, unexpired, `once`, held by the
    /// caller, naming this op and the sha256 of this exact body, and not spent on another request.
    fn check_grant(&self, op: &str, body: &str, holder: &str, grant: Option<&shop_chain::Chain>) -> std::result::Result<String, String> {
        let g = grant.ok_or("this op needs the owner's passkey grant for this exact call")?;
        if g.0.len() != 1 {
            return Err("a passkey grant is one link from the owner; it cannot be delegated".into());
        }
        let v = g.verify(&self.root, shop_chain::now()).map_err(|e| format!("grant: {}", e.0))?;
        if v.holder != holder {
            return Err("grant is for another caller".into());
        }
        if !v.caveats.once {
            return Err("grant must be once".into());
        }
        if v.rights != vec![crate::grant_right(self.def.name, op, body)] {
            return Err("grant is for another call (op or input hash differs)".into());
        }
        Ok(v.chain_id)
    }

    pub async fn call_granted(self: &Arc<Node>, op: &str, key: &str, body: &str, auth: &shop_chain::Auth, grant: Option<&shop_chain::Chain>) -> Value {
        let Some(def) = crate::ops().into_iter().find(|o| o.name == op) else {
            return json!({ "err": Error::Input(format!("no op {op}")) });
        };
        if !self.log.ready() {
            return json!({ "err": Error::NotLeader(self.leader_addr()) });
        }
        let right = right_of(def.cap);
        // the proof covers the key too: a captured request cannot be replayed under another key
        let v = match self.verify(auth, op, &signed_body(key, body), &right) {
            Ok(v) => v,
            Err(why) => return self.deny(op, &right, &auth.chain.leaf().holder, &why).await,
        };
        if key.is_empty() || key.len() > 128 {
            return json!({ "err": Error::Input("an idempotency key of 1-128 bytes is required".into()) });
        }
        let lock = self.keys.lock().unwrap().entry(key.to_string()).or_default().clone();
        let _g = lock.lock().await;
        if let Some(out) = self.log.read(|s, _| s.done.get(key).cloned()) {
            return json!({ "ok": true, "out": out, "replayed": true, "by": self.addrs[self.me], "term": self.log.status().term });
        }
        if def.grant == crate::Grant::Passkey {
            let id = match self.check_grant(op, body, &v.holder, grant) {
                Ok(id) => id,
                Err(why) => return self.deny(op, &right, &v.holder, &why).await,
            };
            let input_sha256 = crate::machine::sha_hex(body.as_bytes());
            match self.propose(Entry::Grant { id, key: key.into(), op: op.into(), input_sha256, holder: v.holder.clone() }).await {
                Ok(r) if r["spent_on"] == key => {}
                Ok(r) => return self.deny(op, &right, &v.holder, &format!("grant already used (on request {})", r["spent_on"])).await,
                Err(e) => return json!({ "err": e }),
            }
        }
        let resumed = self.log.read(|s, _| s.steps.range((key.to_string(), 0)..=(key.to_string(), u32::MAX)).count());
        let input: Value = match serde_json::from_str(body) {
            Ok(x) => x,
            Err(e) => return json!({ "err": Error::input(e) }),
        };
        let cx = Cx::new(self.clone(), key.to_string(), op.to_string(), v.holder.clone(), v.rights.clone(), v.chain_id.clone());
        let out = match (def.call)(cx, input).await {
            Ok(o) => o,
            Err(e) => return json!({ "err": e }),
        };
        match self.propose(Entry::Done { key: key.into(), op: op.into(), out }).await {
            Ok(out) => json!({ "ok": true, "out": out, "replayed": false, "resumed_steps": resumed, "by": self.addrs[self.me], "term": self.log.status().term }),
            Err(e) => json!({ "err": e }),
        }
    }

    async fn handle(self: &Arc<Node>, msg: Msg, from: IpAddr) -> Value {
        match msg {
            Msg::Vote { .. } | Msg::Append { .. } if !self.peer_ok(from) => json!({ "err": "not a peer" }),
            Msg::Vote { req } => serde_json::to_value(self.log.handle_vote(req)).unwrap(),
            Msg::Append { req } => serde_json::to_value(self.log.handle_append(req)).unwrap(),
            Msg::Status => self.status(),
            Msg::Call { op, key, body, auth, grant } => self.call_granted(&op, &key, &body, &auth, grant.as_ref()).await,
            ref m @ Msg::Dump { ref auth } => match self.verify(auth, "dump", &admin_body(m), &format!("{}:read", self.def.name)) {
                Ok(_) => self.dump(),
                Err(why) => json!({ "err": Error::Denied(why) }),
            },
            ref m @ Msg::Crash { after_step, ref auth } => match self.verify(auth, "crash", &admin_body(m), &format!("{}:chaos", self.def.name)) {
                Ok(_) if after_step < 0 => {
                    tokio::spawn(async {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        kill_self();
                    });
                    json!({ "ok": true, "crash": "now" })
                }
                Ok(_) => {
                    self.crash_after.store(after_step, Ordering::SeqCst);
                    json!({ "ok": true, "crash": format!("after step {after_step} of the next order") })
                }
                Err(why) => json!({ "err": Error::Denied(why) }),
            },
        }
    }

    fn peer_ok(&self, ip: IpAddr) -> bool {
        ip.is_loopback() || self.peer_ips.contains(&ip)
    }
}

fn kill_self() {
    let pid = std::process::id().to_string();
    let _ = std::process::Command::new("kill").args(["-KILL", &pid]).status();
    std::process::abort();
}

/// Accept connections on `bind` until the process ends.
pub async fn serve(node: Arc<Node>, bind: &str) -> std::io::Result<()> {
    let l = TcpListener::bind(bind).await?;
    loop {
        let (s, peer) = l.accept().await?;
        let node = node.clone();
        tokio::spawn(async move {
            let _ = s.set_nodelay(true);
            let (r, mut w) = s.into_split();
            let mut r = BufReader::new(r);
            let mut line = String::new();
            loop {
                line.clear();
                match r.read_line(&mut line).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
                let reply = match serde_json::from_str::<Msg>(&line) {
                    Ok(m) => node.handle(m, peer.ip()).await,
                    Err(e) => json!({ "err": format!("bad request: {e}") }),
                };
                let mut out = serde_json::to_vec(&reply).unwrap();
                out.push(b'\n');
                if w.write_all(&out).await.is_err() {
                    return;
                }
            }
        });
    }
}
