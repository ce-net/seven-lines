//! The generated `main`: one binary is the replica, its client, its deployer and its proof.
use crate::lock::{Lock, Pin, PinnedArtifact};
use crate::node::{hostname, op_code, signed_body, which, AiBackend, Node};
use crate::wire::{admin_body, request, Msg};
use crate::{right_of, CellDef};
use serde_json::{json, Value};
use shop_chain::{Caveats, Chain, Holder, Identity};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const USAGE: &str = "usage: <cell> <command>
  serve [--me N] [--dir D]        run one replica (from cell.lock)
  call <op> <json> [--key K] [--grant | --grant-file F]
                                  call an op at the leader (signed with SEVEN_OWNER_SEED, or SEVEN_RIGHTS narrower);
                                  --grant: the owner also signs a one-use grant for this exact call (grant = Passkey ops);
                                  --grant-file: the grant a card's answer carried (from `grant`); without either, a
                                  Passkey op answers NeedsGrant {cell, op, caller, input_hash, summary}
  grant <needs-grant json> [--ttl S]
                                  the owner's tap: sign a one-use grant for exactly that NeedsGrant with the device key
                                  in SEVEN_DEVICE_SEED (else SEVEN_OWNER_SEED); prints the grant JSON
  skill                           SKILL.md for this cell, from describe (up writes it next to cell.lock)
  status | dump                   every replica's role, term, ledger, audit head | the applied state
  crash [--after N]               chaos: SIGKILL the leader now, or right after step N of its next order
  describe                        ops, caps, records and the skill text
  keygen                          a new owner key: put the seed in the keys cell, the public half in cell.lock
  up [--dry-run]                  build (cell.build), pin, install (cell.install / systemd --user), start, replicate
  down                            stop the replicas `up` started on this node
  prove [--out F]                 order, AI note, twin + audit; kill the leader mid-order; check exactly-once";

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}

pub fn main(def: CellDef) -> i32 {
    main_with(def, std::env::args().skip(1).collect())
}

/// [`main`] over the arguments given.
pub fn main_with(def: CellDef, args: Vec<String>) -> i32 {
    if std::env::var("SHOP_BEAT_MS").is_err() {
        // replog's heartbeat: election after 4-8 beats of silence; 150 ms gives a 0.6-1.2 s failover
        std::env::set_var("SHOP_BEAT_MS", "150");
    }
    let rt = tokio::runtime::Runtime::new().expect("tokio");
    let r = rt.block_on(async {
        match args.first().map(String::as_str) {
            Some("serve") => serve(def, &args).await,
            Some("call") => call_cmd(def, &args).await,
            Some("status") => status_cmd(def).await,
            Some("dump") => dump_cmd(def).await,
            Some("crash") => crash_cmd(def, &args).await,
            Some("describe") => {
                println!("{}", serde_json::to_string_pretty(&describe(def)).unwrap());
                Ok(())
            }
            Some("skill") => {
                print!("{}", skill_md(def));
                Ok(())
            }
            Some("grant") => grant_cmd(&args),
            Some("keygen") => {
                let id = Identity::generate();
                println!("{{\"seed\":\"{}\",\"public\":\"{}\"}}", hex::encode(id.seed()), id.public_hex());
                Ok(())
            }
            Some("up") => up(def, &args).await,
            Some("down") => down(def),
            Some("prove") => prove(def, &args).await,
            _ => Err(USAGE.to_string()),
        }
    });
    match r {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{e}");
            1
        }
    }
}

fn lock() -> Result<(PathBuf, Lock), String> {
    let p = Lock::find().ok_or("no cell.lock here or above (SEVEN_LOCK names one)")?;
    let l = Lock::load(&p)?;
    if l.replicas.is_empty() {
        return Err(format!("{}: no [[replica]]", p.display()));
    }
    Ok((p, l))
}

pub fn describe(def: CellDef) -> Value {
    let ops: Vec<Value> =
        crate::ops().iter().map(|o| json!({ "op": o.name, "cap": o.cap, "right": right_of(o.cap), "input": o.input, "output": inner(o.output), "grant": o.grant })).collect();
    let recs: Vec<Value> = crate::records().iter().map(|r| json!({ "record": r.name, "cell": r.cell, "fields": r.fields })).collect();
    let mut d = json!({ "cell": def.name, "version": def.version, "replicas": def.replicas, "home": def.home, "skill": def.skill, "ops": ops, "records": recs,
            "admin_rights": [format!("{}:read", def.name), format!("{}:chaos", def.name)] });
    d["contract"] = json!(contract_hash(&d));
    d
}

/// sha256 of the describe JSON (keys sorted, compact, without `contract`): equal for a cell however it was
/// written (Rust macros, a TypeScript or a Python worker) when it declares the same ops, caps and records.
pub fn contract_hash(describe: &Value) -> String {
    let mut d = describe.clone();
    if let Some(m) = d.as_object_mut() {
        m.remove("contract");
    }
    crate::machine::sha_hex(serde_json::to_string(&d).unwrap_or_default().as_bytes())
}

/// `Result < Applied >` -> `Applied` (the type an op answers with when it succeeds).
fn inner(t: &str) -> String {
    let t = t.replace(' ', "");
    t.strip_prefix("Result<").and_then(|x| x.strip_suffix('>')).map(str::to_string).unwrap_or(t)
}

/// SKILL.md from [`describe`]: the skill text, each op with its cap, grant kind, typed input and output, one worked
/// `call` line, and the refusals. `up` writes it next to cell.lock; a test in each example checks the file equals this.
pub fn skill_md(def: CellDef) -> String {
    let d = describe(def);
    let mut s = format!("# {} (v{})\n\n{}\n\n", def.name, def.version, def.skill);
    let home = match def.home {
        crate::Home::Device => "one replica on the device cell.lock names (home = Device)".to_string(),
        crate::Home::Fleet => format!("{} replicas over one quorum log; every effect happens once", def.replicas),
    };
    s += &format!("Runs as {home}. Generated from `{} describe`; do not edit by hand.\n\n## Ops\n\n", def.name);
    for o in d["ops"].as_array().cloned().unwrap_or_default() {
        let (op, cap, right) = (o["op"].as_str().unwrap_or(""), o["cap"].as_str().unwrap_or(""), o["right"].as_str().unwrap_or(""));
        let passkey = o["grant"] == "Passkey";
        s += &format!("### `{op}`\n\n- cap `{cap}` (chain right `{right}`)\n- input `{}`, answers `{}`\n", o["input"].as_str().unwrap_or(""), o["output"].as_str().unwrap_or(""));
        if passkey {
            s += "- grant = Passkey: without a grant it answers `NeedsGrant { cell, op, caller, input_hash, summary }`; the owner's tap signs a one-use grant for that exact input (`grant`), and the same call is retried with it (`--grant-file`)\n";
        }
        s += &format!("- call: `{} call {op} '<{} as JSON>' --key <idempotency key>{}`\n\n", def.name, o["input"].as_str().unwrap_or(""), if passkey { " --grant-file <grant.json>" } else { "" });
    }
    let recs = d["records"].as_array().cloned().unwrap_or_default();
    if !recs.is_empty() {
        s += "## Records\n\n";
        for r in recs {
            let fields: Vec<String> = r["fields"].as_array().cloned().unwrap_or_default().iter().map(|f| format!("{}: {}", f[0].as_str().unwrap_or(""), f[1].as_str().unwrap_or(""))).collect();
            s += &format!("- `{}` {{ {} }}\n", r["record"].as_str().unwrap_or(""), fields.join(", "));
        }
        s += "\n";
    }
    s += "## Refusals\n\n- `Denied(why)`: the caller's chain does not hold the right (an audit row)\n- `NeedsGrant(..)`: a Passkey op without the owner's one-use grant\n- `NotLeader(addr)`, `NoQuorum`: retry with the same key; it runs once\n- `Quota { agent, used, limit }`: the cell's AI bucket for that agent is spent today\n- `Backend(why)`, `Input(why)`\n";
    s
}

/// The owner's tap on a NeedsGrant card: a one-use grant for exactly what it asks, signed with the device key.
fn grant_cmd(args: &[String]) -> Result<(), String> {
    let text = match args.get(1).filter(|a| !a.starts_with("--")) {
        Some(t) => t.clone(),
        None => std::io::read_to_string(std::io::stdin()).map_err(|e| e.to_string())?,
    };
    let v: Value = serde_json::from_str(text.trim()).map_err(|e| format!("NeedsGrant JSON: {e}"))?;
    // the whole reply, its err, or the NeedsGrant itself
    let v = v.pointer("/reply/err/NeedsGrant").or_else(|| v.pointer("/err/NeedsGrant")).or_else(|| v.get("NeedsGrant")).cloned().unwrap_or(v);
    let n: crate::NeedsGrant = serde_json::from_value(v).map_err(|e| format!("NeedsGrant: {e}"))?;
    let seed = std::env::var("SEVEN_DEVICE_SEED").or_else(|_| std::env::var("SEVEN_OWNER_SEED")).map_err(|_| "SEVEN_DEVICE_SEED (or SEVEN_OWNER_SEED) is not set")?;
    let device = Identity::from_seed(shop_chain::hex_seed(seed.trim()).ok_or("the device seed is not 64 hex")?);
    let ttl = flag(args, "--ttl").and_then(|t| t.parse().ok()).unwrap_or(300u64).min(3600);
    println!("{}", serde_json::to_string(&crate::one_use(&device, &n, ttl)).map_err(|e| e.to_string())?);
    Ok(())
}

// ------------------------------------------------------------------------------------------------ serve

async fn serve(def: CellDef, args: &[String]) -> Result<(), String> {
    let (_, l) = lock()?;
    if l.replicas.len() as u32 != def.replicas {
        eprintln!("note: #[cell] says replicas = {}, cell.lock lists {}", def.replicas, l.replicas.len());
    }
    let me = match flag(args, "--me").or_else(|| std::env::var("SEVEN_ME").ok()) {
        Some(m) => m.parse::<usize>().map_err(|_| "--me N")?,
        None => {
            // the first non-sim replica whose address this machine can bind
            let host = hostname();
            let mut order: Vec<usize> = (0..l.replicas.len()).filter(|&i| !l.replicas[i].sim).collect();
            order.sort_by_key(|&i| l.replicas[i].node != host);
            order.into_iter().find(|&i| std::net::TcpListener::bind(&l.replicas[i].addr).is_ok()).ok_or("no replica in cell.lock binds here (pass --me N)")?
        }
    };
    if def.home == crate::Home::Device {
        if l.replicas.len() != 1 {
            return Err(format!("{} is home = Device: cell.lock must list exactly one replica, it lists {}", def.name, l.replicas.len()));
        }
        let r = &l.replicas[0];
        if r.node != "local" && r.node != hostname() && std::env::var("SEVEN_DEVICE").ok().as_deref() != Some(r.node.as_str()) {
            return Err(format!("{} is home = Device and belongs to {}; this is {}", def.name, r.node, hostname()));
        }
    }
    if l.cell.root.len() != 64 {
        return Err("cell.lock [cell] root must be the owner's public key (64 hex)".into());
    }
    let dir = flag(args, "--dir").map(PathBuf::from).unwrap_or_else(|| {
        let base = std::env::var("SEVEN_DATA").map(PathBuf::from).unwrap_or_else(|_| PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(".local/share/seven"));
        base.join(format!("{}-{me}", def.name))
    });
    let ai = AiBackend::from_env();
    let addr = l.replicas[me].addr.clone();
    eprintln!("{} v{} replica {me}/{} on {addr} ({}), data {}, ai {}", def.name, def.version, l.replicas.len(), hostname(), dir.display(), ai.name());
    let node = Node::open_with(def, me, l.addrs(), l.cell.root.clone(), l.cell.owners.clone(), dir, ai);
    crate::node::serve(node, &addr).await.map_err(|e| format!("{addr}: {e}"))
}

// ------------------------------------------------------------------------------------------------ client

/// The caller: the owner key from SEVEN_OWNER_SEED (keys cell: `keys run <name>=SEVEN_OWNER_SEED -- ...`) grants an
/// ephemeral agent key a one-hour chain with SEVEN_RIGHTS (default `<cell>:*`).
fn holder(def: CellDef, rights: Option<Vec<String>>) -> Result<Holder, String> {
    let seed = std::env::var("SEVEN_OWNER_SEED").map_err(|_| "SEVEN_OWNER_SEED is not set (run under `keys run <name>=SEVEN_OWNER_SEED -- ...`)")?;
    let owner = Identity::from_seed(shop_chain::hex_seed(seed.trim()).ok_or("SEVEN_OWNER_SEED is not 64 hex")?);
    let rights = rights.unwrap_or_else(|| {
        std::env::var("SEVEN_RIGHTS").map(|r| r.split(',').map(String::from).collect()).unwrap_or_else(|_| vec![format!("{}:*", def.name)])
    });
    // SEVEN_AGENT_SEED: the same caller key across calls, so a NeedsGrant answer and its retry name one caller
    let agent = match std::env::var("SEVEN_AGENT_SEED") {
        Ok(s) => Identity::from_seed(shop_chain::hex_seed(s.trim()).ok_or("SEVEN_AGENT_SEED is not 64 hex")?),
        Err(_) => Identity::generate(),
    };
    let chain = Chain::root(&owner, &agent.public_hex(), rights, Caveats { tenant: def.name.into(), until: shop_chain::now() + 3600, ..Default::default() });
    Ok(Holder { id: agent, chain })
}

fn call_msg(def: CellDef, h: &Holder, op: &str, key: &str, body: &str) -> Msg {
    Msg::Call { op: op.into(), key: key.into(), body: body.into(), auth: h.auth(def.name, op_code(op), &signed_body(key, body)), grant: None }
}

fn admin_msg(def: CellDef, h: &Holder, m: Msg) -> Msg {
    let (op, body) = match &m {
        Msg::Crash { .. } => ("crash", admin_body(&m)),
        _ => ("dump", String::new()),
    };
    let auth = h.auth(def.name, op_code(op), &body);
    match m {
        Msg::Crash { after_step, .. } => Msg::Crash { after_step, auth },
        _ => Msg::Dump { auth },
    }
}

/// Send `msg` to the leader, following `NotLeader` hints and moving on from dead replicas, until `deadline`.
pub async fn call_leader(addrs: &[String], msg: &Msg, deadline: Duration) -> (Result<Value, String>, u32) {
    let t0 = Instant::now();
    let mut at = 0usize;
    let mut tries = 0;
    let mut last = String::new();
    while t0.elapsed() < deadline {
        tries += 1;
        let addr = &addrs[at % addrs.len()];
        match request(addr, msg, Duration::from_secs(200)).await {
            Ok(v) if v.get("ok").is_some() => return (Ok(v), tries),
            Ok(v) => {
                let err = v.get("err").cloned().unwrap_or(Value::Null);
                let retry = err.get("NotLeader").is_some() || err == json!("NoQuorum");
                if !retry {
                    return (Err(err.to_string()), tries);
                }
                last = err.to_string();
                match err.get("NotLeader").and_then(|l| l.as_str()).and_then(|l| addrs.iter().position(|a| a == l)) {
                    Some(i) if i != at % addrs.len() => at = i,
                    _ => {
                        at += 1;
                        tokio::time::sleep(Duration::from_millis(60)).await;
                    }
                }
            }
            Err(e) => {
                last = format!("{addr}: {e}");
                at += 1;
                tokio::time::sleep(Duration::from_millis(60)).await;
            }
        }
    }
    (Err(format!("no leader answered in {deadline:?}: {last}")), tries)
}

async fn call_cmd(def: CellDef, args: &[String]) -> Result<(), String> {
    let (_, l) = lock()?;
    let op = args.get(1).ok_or("call <op> <json>")?;
    let body = args.get(2).cloned().unwrap_or_else(|| "null".into());
    let key = flag(args, "--key").unwrap_or_else(|| format!("{op}-{}", shop_chain::now()));
    let h = holder(def, None)?;
    let mut msg = call_msg(def, &h, op, &key, &body);
    if args.iter().any(|a| a == "--grant") {
        // the owner key is here (SEVEN_OWNER_SEED), so the owner is the one signing: a grant for this exact call
        let seed = std::env::var("SEVEN_OWNER_SEED").unwrap_or_default();
        let owner = Identity::from_seed(shop_chain::hex_seed(seed.trim()).ok_or("SEVEN_OWNER_SEED is not 64 hex")?);
        if let Msg::Call { grant, .. } = &mut msg {
            *grant = Some(crate::passkey_grant(&owner, &h.id.public_hex(), def.name, op, &body, 300));
        }
    } else if let Some(f) = flag(args, "--grant-file") {
        let g: Chain = serde_json::from_str(&std::fs::read_to_string(&f).map_err(|e| format!("{f}: {e}"))?).map_err(|e| format!("{f}: {e}"))?;
        if let Msg::Call { grant, .. } = &mut msg {
            *grant = Some(g);
        }
    }
    let (r, tries) = call_leader(&l.addrs(), &msg, Duration::from_secs(60)).await;
    let v = r?;
    println!("{}", serde_json::to_string_pretty(&json!({ "key": key, "tries": tries, "reply": v })).unwrap());
    Ok(())
}

async fn statuses(addrs: &[String]) -> Vec<Value> {
    let fs = addrs.iter().cloned().map(|a| async move { request(&a, &Msg::Status, Duration::from_millis(800)).await.unwrap_or_else(|e| json!({ "addr": a, "down": e })) });
    futures_join(fs).await
}

async fn futures_join<F: std::future::Future<Output = Value> + Send + 'static>(fs: impl Iterator<Item = F>) -> Vec<Value> {
    let hs: Vec<_> = fs.map(tokio::spawn).collect();
    let mut out = Vec::new();
    for h in hs {
        out.push(h.await.unwrap_or(Value::Null));
    }
    out
}

async fn status_cmd(_def: CellDef) -> Result<(), String> {
    let (_, l) = lock()?;
    for (r, s) in l.replicas.iter().zip(statuses(&l.addrs()).await) {
        if s.get("down").is_some() {
            println!("{:<16} {:<22} down ({})", r.node, r.addr, s["down"].as_str().unwrap_or(""));
        } else {
            println!(
                "{:<16} {:<22} {:<9} term {:<4} commit {:<5} ledger {:<4} audit {:<4} head {} ai {}",
                r.node, r.addr, s["role"].as_str().unwrap_or(""), s["term"], s["commit"], s["ledger"], s["audit"],
                &s["audit_head"].as_str().unwrap_or("").chars().take(12).collect::<String>(), s["ai"].as_str().unwrap_or("")
            );
        }
    }
    Ok(())
}

async fn dump_cmd(def: CellDef) -> Result<(), String> {
    let (_, l) = lock()?;
    let h = holder(def, None)?;
    for a in l.addrs() {
        let v = request(&a, &admin_msg(def, &h, Msg::Dump { auth: dummy_auth(&h) }), Duration::from_secs(3)).await.unwrap_or_else(|e| json!({ "down": e }));
        println!("{}", json!({ "addr": a, "state": v }));
    }
    Ok(())
}

fn dummy_auth(h: &Holder) -> shop_chain::Auth {
    h.auth("", 0, "")
}

async fn crash_cmd(def: CellDef, args: &[String]) -> Result<(), String> {
    let (_, l) = lock()?;
    let after: i64 = flag(args, "--after").and_then(|v| v.parse().ok()).unwrap_or(-1);
    let h = holder(def, None)?;
    let leader = find_leader(&l.addrs(), Duration::from_secs(10)).await.ok_or("no leader")?;
    let v = request(&leader["addr"].as_str().unwrap_or_default().to_string(), &admin_msg(def, &h, Msg::Crash { after_step: after, auth: dummy_auth(&h) }), Duration::from_secs(3)).await?;
    println!("{}", json!({ "leader": leader["addr"], "reply": v }));
    Ok(())
}

async fn find_leader(addrs: &[String], wait: Duration) -> Option<Value> {
    let t0 = Instant::now();
    while t0.elapsed() < wait {
        if let Some(s) = statuses(addrs).await.into_iter().find(|s| s["ready"] == json!(true)) {
            return Some(s);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    None
}

// ------------------------------------------------------------------------------------------------ up / down

fn sh(cmd: &mut std::process::Command) -> Result<String, String> {
    let o = cmd.output().map_err(|e| format!("{cmd:?}: {e}"))?;
    if !o.status.success() {
        return Err(format!("{cmd:?}: {}{}", String::from_utf8_lossy(&o.stdout).trim(), String::from_utf8_lossy(&o.stderr).trim()));
    }
    Ok(String::from_utf8_lossy(&o.stdout).trim().to_string())
}

fn unit_name(def: CellDef, i: usize) -> String {
    format!("seven-{}-{i}", def.name)
}

/// `up`: the whole deployment in one verb.
/// 1 build: `cell.build Build{repo, rev}` (the commit, not the working tree), published to artifact.index;
/// 2 pin: the commit and the per-target sha256 into cell.lock;
/// 3 install: replicas on this node run the pinned binary under `systemd-run --user` with a CPU and memory cap;
///   replicas on other nodes go through that node's `cell.install Install{Commit{repo, rev}}`;
/// 4 start and replicate: wait until one leader is ready and every replica names it.
async fn up(def: CellDef, args: &[String]) -> Result<(), String> {
    let dry = args.iter().any(|a| a == "--dry-run");
    let (lock_path, mut l) = lock()?;
    // the repository up pins is the one it runs in (the lock may live elsewhere, e.g. a sim-only lock)
    let repo_dir = flag(args, "--repo").map(PathBuf::from).unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| lock_path.parent().unwrap().to_path_buf()));
    let ce = which("ce").ok_or("`ce` is not on PATH: up drives cell.build and cell.install through it")?;
    let rev = sh(std::process::Command::new("git").arg("-C").arg(&repo_dir).args(["rev-parse", "HEAD"]))?;
    let dirty = sh(std::process::Command::new("git").arg("-C").arg(&repo_dir).args(["status", "--porcelain", "--untracked-files=no"]))?;
    if !dirty.is_empty() {
        eprintln!("note: the working tree has changes; up builds and pins the commit {rev}, not them");
    }
    let me = hostname();
    let local = |n: &str| n == "local" || n == me;
    let t0 = Instant::now();
    let mut steps = Vec::new();

    // 0 SKILL.md next to cell.lock, from describe (an agent reads it before calling; a test keeps it equal)
    let skill = lock_path.with_file_name("SKILL.md");
    if std::fs::read_to_string(&skill).ok().as_deref() != Some(skill_md(def).as_str()) {
        std::fs::write(&skill, skill_md(def)).map_err(|e| format!("{}: {e}", skill.display()))?;
        eprintln!("0 skill  wrote {}", skill.display());
    }
    steps.push(json!({ "step": "skill", "file": skill.display().to_string() }));

    // 1 build + 2 pin
    let target = format!("{}-unknown-linux-gnu", std::env::consts::ARCH);
    let req = json!({ "repo": l.cell.repo, "rev": rev, "targets": [target], "channel": "test", "publish": true });
    eprintln!("1 build  {} @ {} for {target} (cell.build)", l.cell.repo, &rev[..12]);
    let built: Value = if dry {
        json!({ "artifacts": [] })
    } else {
        let out = sh(std::process::Command::new(&ce).args(["call", "cell.build.Build"]).arg(req.to_string()).arg("--json"))?;
        serde_json::from_str(&out).map_err(|e| format!("cell.build answer: {e}: {out}"))?
    };
    let built = built.get("Ok").cloned().unwrap_or(built);
    if let Some(f) = built.get("failed").and_then(|f| f.as_array()).filter(|f| !f.is_empty()) {
        return Err(format!("cell.build failed: {}", Value::Array(f.clone())));
    }
    let arts: Vec<PinnedArtifact> = built["artifacts"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|a| PinnedArtifact { target: a["target"].as_str().unwrap_or("").into(), bin: a["bin"].as_str().unwrap_or("").into(), sha256: a["sha256"].as_str().unwrap_or("").into() })
        .collect();
    steps.push(json!({ "step": "build", "rev": rev, "manifest_id": built["manifest_id"], "artifacts": arts.len(), "ms": t0.elapsed().as_millis() as u64 }));
    l.pin = Some(Pin { rev: rev.clone(), at: shop_chain::now().to_string(), artifacts: arts.clone() });
    if !dry {
        l.save(&lock_path)?;
    }
    eprintln!("2 pin    {} artifact(s), release {}", arts.len(), built["manifest_id"].as_str().unwrap_or("-"));

    // 3 install
    let bin = cached_artifact(&arts, &target, def.name)?;
    for (i, r) in l.replicas.iter().enumerate() {
        if local(&r.node) || r.sim {
            let unit = unit_name(def, i);
            eprintln!("3 start  replica {i} on {} ({}) as {unit}, CPU 50%, 256 MB", me, r.addr);
            if dry {
                continue;
            }
            let _ = std::process::Command::new("systemctl").args(["--user", "stop", &unit]).status();
            let _ = std::process::Command::new("systemctl").args(["--user", "reset-failed", &unit]).status();
            let mut cmd = std::process::Command::new("systemd-run");
            cmd.args(["--user", "--unit", &unit, "-p", "CPUQuota=50%", "-p", "MemoryMax=256M", "-p", "Restart=on-failure", "-p", "RestartSec=2"])
                .arg(format!("--setenv=SEVEN_LOCK={}", lock_path.display()))
                .arg(format!("--setenv=PATH={}", std::env::var("PATH").unwrap_or_default()))
                .arg(format!("--setenv=SEVEN_AI={}", std::env::var("SEVEN_AI").unwrap_or_else(|_| "auto".into())));
            // a TypeScript or Python cell: seven-host starts its worker from this (see seven-host)
            if let Ok(w) = std::env::var("SEVEN_WORKER") {
                cmd.arg(format!("--setenv=SEVEN_WORKER={w}"));
            }
            cmd.arg(&bin)
                .args(["serve", "--me", &i.to_string()]);
            sh(&mut cmd)?;
            steps.push(json!({ "step": "start", "replica": i, "node": me, "unit": unit, "bin_sha256": arts.first().map(|a| a.sha256.clone()) }));
        } else {
            eprintln!("3 install replica {i} on {} ({}) via cell.install Commit {}", r.node, r.addr, &rev[..12]);
            if dry {
                continue;
            }
            let req = json!({ "api": l.cell.api, "schema": 0, "source": { "Commit": { "repo": l.cell.repo, "rev": rev } } });
            let out = match sh(std::process::Command::new(&ce).args(["call", "cell.install.Install"]).arg(req.to_string()).args(["--on", &r.node, "--json"])) {
                Ok(o) => o,
                Err(e) => {
                    // a refused node is reported, not fatal: the replicas that did start still elect if they are a majority
                    let why = e.rsplit("Error: ").next().unwrap_or(&e).trim().to_string();
                    eprintln!("          refused: {why}");
                    steps.push(json!({ "step": "install", "replica": i, "node": r.node, "refused": why }));
                    continue;
                }
            };
            let job: Value = serde_json::from_str(&out).unwrap_or(json!(out));
            let id = job.get("Ok").cloned().unwrap_or(job.clone());
            let mut state = Value::Null;
            for _ in 0..240 {
                let o = sh(std::process::Command::new(&ce).args(["call", "cell.install.Job"]).arg(id.to_string()).args(["--on", &r.node, "--json"])).unwrap_or_default();
                state = serde_json::from_str(&o).unwrap_or(json!(o));
                let s = state.to_string();
                if s.contains("Done") || s.contains("Failed") {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            eprintln!("          {}", state.to_string().chars().take(300).collect::<String>());
            steps.push(json!({ "step": "install", "replica": i, "node": r.node, "job": id, "state": state }));
        }
    }

    // 4 replicate
    if !dry {
        let leader = find_leader(&l.addrs(), Duration::from_secs(120)).await.ok_or("no leader within 120 s")?;
        let st = statuses(&l.addrs()).await;
        let joined = st.iter().filter(|s| s["leader"] == leader["addr"]).count();
        eprintln!("4 up     leader {} ({}) term {}, {joined}/{} replicas follow it, {:.1} s", leader["addr"], leader["node"], leader["term"], l.replicas.len(), t0.elapsed().as_secs_f64());
        steps.push(json!({ "step": "replicate", "leader": leader["addr"], "node": leader["node"], "term": leader["term"], "joined": joined, "ms": t0.elapsed().as_millis() as u64 }));
    }
    println!("{}", json!({ "up": def.name, "rev": rev, "steps": steps }));
    Ok(())
}

/// The binary cell.build made for `target`: its out cache on this node, checked against the pinned sha256.
fn cached_artifact(arts: &[PinnedArtifact], target: &str, name: &str) -> Result<PathBuf, String> {
    let Some(a) = arts.iter().find(|a| a.target == target) else {
        // dry run, or no cell.build here: the binary that is running `up`
        return std::env::current_exe().map_err(|e| e.to_string());
    };
    let home = PathBuf::from(std::env::var("HOME").unwrap_or_default());
    let dest = home.join(".local/share/seven/bin").join(&a.sha256).join(name);
    if dest.is_file() {
        return Ok(dest);
    }
    let roots = [home.join(".cache/cell-build"), home.join("dev/cell.build")];
    for root in roots {
        let out = std::process::Command::new("find").arg(&root).args(["-type", "f", "-name", if a.bin.is_empty() { name } else { &a.bin }, "-size", "+100k"]).output();
        let Ok(out) = out else { continue };
        for p in String::from_utf8_lossy(&out.stdout).lines() {
            let Ok(bytes) = std::fs::read(p) else { continue };
            if crate::machine::sha_hex(&bytes) == a.sha256 {
                std::fs::create_dir_all(dest.parent().unwrap()).map_err(|e| e.to_string())?;
                std::fs::write(&dest, &bytes).map_err(|e| e.to_string())?;
                let _ = std::process::Command::new("chmod").arg("+x").arg(&dest).status();
                return Ok(dest);
            }
        }
    }
    Err(format!("the pinned {target} artifact {} is not in cell.build's cache on this node", a.sha256))
}

fn down(def: CellDef) -> Result<(), String> {
    let (_, l) = lock()?;
    for i in 0..l.replicas.len() {
        let unit = unit_name(def, i);
        if std::process::Command::new("systemctl").args(["--user", "stop", &unit]).status().is_ok_and(|s| s.success()) {
            eprintln!("stopped {unit}");
        }
    }
    Ok(())
}

// ------------------------------------------------------------------------------------------------ prove

/// The end-to-end proof against the replicas in cell.lock.
async fn prove(def: CellDef, args: &[String]) -> Result<(), String> {
    let (_, l) = lock()?;
    let addrs = l.addrs();
    let h = holder(def, None)?;
    let mut report = json!({ "cell": def.name, "replicas": l.replicas, "started": shop_chain::now() });
    let leader0 = find_leader(&addrs, Duration::from_secs(30)).await.ok_or("no leader within 30 s")?;
    report["leader_before"] = json!({ "addr": leader0["addr"], "node": leader0["node"], "term": leader0["term"] });
    eprintln!("leader {} on {} term {}", leader0["addr"], leader0["node"], leader0["term"]);

    let order = json!({ "sku": "TENT-2P", "qty": 1, "paid": { "minor": 249900, "cur": "SEK" } }).to_string();
    let run = shop_chain::now();

    // a chain without orders:place is refused, and the refusal is an audit row
    let reader = holder(def, Some(vec![format!("{}:read", def.name)]))?;
    let (denied, _) = call_leader(&addrs, &call_msg(def, &reader, "place", &format!("deny-{run}"), &order), Duration::from_secs(20)).await;
    report["denied"] = json!({ "refused": denied.is_err(), "why": denied.err() });

    // order A: the plain path
    let ka = format!("A-{run}");
    let t = Instant::now();
    let (ra, _) = call_leader(&addrs, &call_msg(def, &h, "place", &ka, &order), Duration::from_secs(120)).await;
    let ra = ra?;
    report["order_a"] = json!({ "key": ka, "ms": t.elapsed().as_millis() as u64, "receipt": ra["out"] });
    eprintln!("order A {} ms: charge {} note by {}", t.elapsed().as_millis(), ra["out"]["charge"]["id"], ra["out"]["note"]["vendor"]);

    // order B: kill the leader after its charge commits and before the order is done
    let leader = find_leader(&addrs, Duration::from_secs(30)).await.ok_or("no leader")?;
    let laddr = leader["addr"].as_str().unwrap_or_default().to_string();
    let local = leader["node"].as_str() == Some(hostname().as_str());
    let kb = format!("B-{run}");
    let how = if local { "external SIGKILL of the leader process once its charge step committed" } else { "leader armed to SIGKILL itself right after its charge step committed" };
    if !local {
        let v = request(&laddr, &admin_msg(def, &h, Msg::Crash { after_step: 0, auth: dummy_auth(&h) }), Duration::from_secs(3)).await?;
        if v.get("ok").is_none() {
            return Err(format!("crash arm refused: {v}"));
        }
    }
    let msg = call_msg(def, &h, "place", &kb, &order);
    let addrs2 = addrs.clone();
    let tb = Instant::now();
    let client = tokio::spawn(async move { call_leader(&addrs2, &msg, Duration::from_secs(180)).await });
    // wait until the charge for B is committed on the leader, then kill it (or watch it kill itself)
    let mut t_kill = None;
    let mut charged_before_kill = false;
    while tb.elapsed() < Duration::from_secs(60) {
        match request(&laddr, &Msg::Status, Duration::from_millis(500)).await {
            Ok(s) => {
                let inflight = s["inflight"].as_array().cloned().unwrap_or_default();
                if inflight.iter().any(|x| x[0] == json!(kb)) {
                    charged_before_kill = true;
                    if local {
                        let pid = s["pid"].as_u64().unwrap_or(0);
                        sh(std::process::Command::new("kill").args(["-KILL", &pid.to_string()]))?;
                        t_kill = Some(Instant::now());
                        break;
                    }
                }
            }
            Err(_) if !local => {
                t_kill = Some(Instant::now());
                charged_before_kill = true; // it only dies after the charge step commits
                break;
            }
            Err(e) => return Err(format!("leader {laddr} went away before the kill: {e}")),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let t_kill = t_kill.ok_or("the leader never reached the charge step for order B")?;
    let rest: Vec<String> = addrs.iter().filter(|a| **a != laddr).cloned().collect();
    let new_leader = find_leader(&rest, Duration::from_secs(60)).await.ok_or("no new leader within 60 s")?;
    let failover_ms = t_kill.elapsed().as_millis() as u64;
    eprintln!("killed {laddr}; new leader {} on {} term {} after {failover_ms} ms", new_leader["addr"], new_leader["node"], new_leader["term"]);
    let (rb, tries) = client.await.map_err(|e| e.to_string())?;
    let rb = rb?;
    let order_b_ms = tb.elapsed().as_millis() as u64;
    let charge_b = rb["out"]["charge"].clone();

    // the same request again: the stored receipt, no new charge
    let (again, _) = call_leader(&addrs, &call_msg(def, &h, "place", &kb, &order), Duration::from_secs(60)).await;
    let again = again?;

    // every replica's ledger: exactly one row for B (the killed one once it is back, if it comes back)
    tokio::time::sleep(Duration::from_millis(500)).await;
    let back = Instant::now();
    let mut per_replica = Vec::new();
    loop {
        per_replica.clear();
        for a in &addrs {
            let d = request(a, &admin_msg(def, &h, Msg::Dump { auth: dummy_auth(&h) }), Duration::from_secs(3)).await.unwrap_or_else(|e| json!({ "down": e }));
            let rows: Vec<&Value> = d["ledger"].as_array().map(|v| v.iter().filter(|r| r["key"] == json!(kb)).collect()).unwrap_or_default();
            let receipts_b = d["audit"].as_array().map(|v| v.iter().filter(|r| r["key"] == json!(kb) && r["kind"] == json!("twin.put")).count()).unwrap_or(0);
            per_replica.push(json!({
                "addr": a, "up": d.get("down").is_none(), "charges_for_b": rows.len(), "audit_receipts_for_b": receipts_b, "charge_ids": rows.iter().map(|r| r["id"].clone()).collect::<Vec<_>>(),
                "done_b": d["done"].get(&kb).is_some(), "audit_rows": d["audit"].as_array().map(|a| a.len()), "audit_chain_ok": d["audit_chain_ok"],
                "audit_head": d["audit"].as_array().and_then(|a| a.last()).map(|r| r["hash"].clone()), "ledger_rows": d["ledger"].as_array().map(|a| a.len()),
            }));
        }
        let all_up = per_replica.iter().all(|r| r["up"] == json!(true) && r["done_b"] == json!(true));
        if all_up || back.elapsed() > Duration::from_secs(40) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let up: Vec<&Value> = per_replica.iter().filter(|r| r["up"] == json!(true)).collect();
    let exactly_once = up.len() >= 2
        && up.iter().all(|r| r["charges_for_b"] == json!(1) && r["audit_receipts_for_b"] == json!(1) && r["charge_ids"][0] == charge_b["id"] && r["done_b"] == json!(true))
        && again["replayed"] == json!(true)
        && again["out"]["charge"]["id"] == charge_b["id"];
    let heads_equal = up.windows(2).all(|w| w[0]["audit_head"] == w[1]["audit_head"]);
    report["order_b"] = json!({
        "key": kb, "kill": how, "killed": { "addr": laddr, "node": leader["node"], "term": leader["term"] },
        "charged_before_kill": charged_before_kill, "new_leader": { "addr": new_leader["addr"], "node": new_leader["node"], "term": new_leader["term"] },
        "failover_ms": failover_ms, "order_ms": order_b_ms, "client_tries": tries, "resumed_steps": rb["resumed_steps"],
        "receipt": rb["out"], "retry_replayed": again["replayed"], "retry_charge_id": again["out"]["charge"]["id"],
    });
    report["replicas_after"] = json!(per_replica);
    report["exactly_once"] = json!(exactly_once);
    report["audit_heads_equal"] = json!(heads_equal);
    report["pass"] = json!(exactly_once && heads_equal && report["denied"]["refused"] == json!(true) && rb["resumed_steps"].as_u64().unwrap_or(0) >= 1);
    let text = serde_json::to_string_pretty(&report).unwrap();
    if let Some(p) = flag(args, "--out") {
        std::fs::write(&p, &text).map_err(|e| e.to_string())?;
    }
    println!("{text}");
    if report["pass"] == json!(true) {
        Ok(())
    } else {
        Err("proof FAILED".into())
    }
}
