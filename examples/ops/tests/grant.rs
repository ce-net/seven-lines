//! The ops cell as three real processes on loopback, over a fixed fleet (one cell stopped on purpose). `apply` is
//! `grant = Passkey`: with no grant it is refused and audited; a grant for another `Change` is refused; a spent
//! grant is refused; a valid grant restarts the cell, and plan, grant and result are three audit rows linked by the
//! hash of the `Change`.
use ce::cli::call_leader;
use ce::node::{op_code, signed_body};
use ce::wire::Msg;
use serde_json::{json, Value};
use shop_chain::{Caveats, Chain, Holder, Identity};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_ops");

#[test]
fn the_ops_cell_is_seven_lines() {
    assert_eq!(include_str!("../src/main.rs").lines().count(), 7);
}

#[test]
fn describe_marks_apply_as_passkey() {
    let d: Value = serde_json::from_slice(&Command::new(BIN).arg("describe").output().unwrap().stdout).unwrap();
    let apply = d["ops"].as_array().unwrap().iter().find(|o| o["op"] == "apply").unwrap().clone();
    assert_eq!(apply["grant"], "Passkey");
    assert_eq!(d["home"], "Fleet");
}

struct Cluster {
    kids: Vec<Child>,
    addrs: Vec<String>,
    owner: Identity,
    dir: std::path::PathBuf,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for k in &mut self.kids {
            let _ = k.kill();
            let _ = k.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn cluster(tag: &str) -> Cluster {
    let dir = std::env::temp_dir().join(format!("seven-ops-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let owner = Identity::generate();
    let ports: Vec<u16> = (0..3).map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()).collect();
    let mut lock = format!("[cell]\nname = \"ops\"\nroot = \"{}\"\n", owner.public_hex());
    for (i, p) in ports.iter().enumerate() {
        lock.push_str(&format!("[[replica]]\nnode = \"r{i}\"\naddr = \"127.0.0.1:{p}\"\n"));
    }
    std::fs::write(dir.join("cell.lock"), lock).unwrap();
    let fleet = json!({ "at": 0, "source": "test", "more": null, "cells": [
        { "name": "shop", "unit": "seven-shop.service", "active": "active", "sub": "running", "healthy": true },
        { "name": "demo-sick", "unit": "seven-demo-sick.service", "active": "inactive", "sub": "dead", "healthy": false } ] });
    std::fs::write(dir.join("fleet.json"), fleet.to_string()).unwrap();
    let restarts = dir.join("restarts.log");
    let kids = (0..3)
        .map(|i| {
            Command::new(BIN)
                .args(["serve", "--me", &i.to_string(), "--dir"])
                .arg(dir.join(format!("d{i}")))
                .env("SEVEN_LOCK", dir.join("cell.lock"))
                .env("SEVEN_AI", "local")
                .env("SEVEN_FLEET_CMD", format!("cat {}", dir.join("fleet.json").display()))
                .env("SEVEN_FLEET_RESTART_CMD", format!("echo {{unit}} >> {}", restarts.display()))
                .stderr(Stdio::null())
                .spawn()
                .unwrap()
        })
        .collect();
    let addrs = ports.iter().map(|p| format!("127.0.0.1:{p}")).collect();
    Cluster { kids, addrs, owner, dir }
}

fn agent(c: &Cluster) -> Holder {
    let id = Identity::generate();
    let chain = Chain::root(&c.owner, &id.public_hex(), vec!["ops:*".into()], Caveats { tenant: "ops".into(), until: shop_chain::now() + 600, ..Default::default() });
    Holder { id, chain }
}

fn msg(h: &Holder, op: &str, key: &str, body: &str, grant: Option<Chain>) -> Msg {
    Msg::Call { op: op.into(), key: key.into(), body: body.into(), auth: h.auth("ops", op_code(op), &signed_body(key, body)), grant }
}

fn call(rt: &tokio::runtime::Runtime, c: &Cluster, m: &Msg) -> Result<Value, String> {
    rt.block_on(call_leader(&c.addrs, m, Duration::from_secs(30))).0
}

fn dump(rt: &tokio::runtime::Runtime, c: &Cluster, h: &Holder) -> Value {
    std::thread::sleep(Duration::from_millis(500));
    let auth = h.auth("ops", op_code("dump"), "");
    let m = Msg::Dump { auth };
    c.addrs
        .iter()
        .filter_map(|a| rt.block_on(ce::wire::request(a, &m, Duration::from_secs(5))).ok())
        .max_by_key(|d| d["applied"].as_u64().unwrap_or(0))
        .expect("a replica answers dump")
}

fn audit_kinds(d: &Value) -> Vec<String> {
    d["audit"].as_array().map(|a| a.iter().map(|r| r["kind"].as_str().unwrap_or("").to_string()).collect()).unwrap_or_default()
}

#[test]
fn apply_needs_a_one_use_grant_for_this_exact_change() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let c = cluster("grant");
    let h = agent(&c);

    // propose: the snapshot shows demo-sick down, the plan is to restart it
    let q = json!("which cell is unhealthy?").to_string();
    let p = call(&rt, &c, &msg(&h, "propose", "p1", &q, None)).expect("propose");
    let change = p["out"].clone();
    assert_eq!(change["change"], json!({ "Restart": { "cell": "demo-sick" } }), "{p:#}");
    let body = change.to_string();

    // 1. no grant: refused, and the refusal is an audit row
    let e = call(&rt, &c, &msg(&h, "apply", "a0", &body, None)).unwrap_err();
    assert!(e.contains("passkey grant"), "{e}");

    // 2. a grant for another Change (another hash): refused
    let other = json!({ "change": { "Restart": { "cell": "shop" } }, "note": change["note"] }).to_string();
    let g_other = ce::passkey_grant(&c.owner, &h.id.public_hex(), "ops", "apply", &other, 300);
    let e = call(&rt, &c, &msg(&h, "apply", "a1", &body, Some(g_other))).unwrap_err();
    assert!(e.contains("another call"), "{e}");

    // a grant some other key holds: refused (bound to the caller)
    let stranger = Identity::generate();
    let g_stranger = ce::passkey_grant(&c.owner, &stranger.public_hex(), "ops", "apply", &body, 300);
    let e = call(&rt, &c, &msg(&h, "apply", "a2", &body, Some(g_stranger))).unwrap_err();
    assert!(e.contains("another caller"), "{e}");

    // a grant signed by someone who is not the owner: refused
    let g_forged = ce::passkey_grant(&Identity::generate(), &h.id.public_hex(), "ops", "apply", &body, 300);
    assert!(call(&rt, &c, &msg(&h, "apply", "a3", &body, Some(g_forged))).is_err());

    // 4. a valid grant: the restart runs once
    let g = ce::passkey_grant(&c.owner, &h.id.public_hex(), "ops", "apply", &body, 300);
    let ok = call(&rt, &c, &msg(&h, "apply", "a4", &body, Some(g.clone()))).expect("apply with grant");
    assert_eq!(ok["out"]["unit"], "seven-demo-sick.service", "{ok:#}");
    assert_eq!(ok["out"]["ok"], true);
    // the retry of the same request is answered from the log, not run again
    let again = call(&rt, &c, &msg(&h, "apply", "a4", &body, Some(g.clone()))).expect("retry");
    assert_eq!(again["replayed"], true);

    // 3. the spent grant on a new request: refused
    let e = call(&rt, &c, &msg(&h, "apply", "a5", &body, Some(g))).unwrap_err();
    assert!(e.contains("already used"), "{e}");

    let restarts = std::fs::read_to_string(c.dir.join("restarts.log")).unwrap();
    assert_eq!(restarts.lines().count(), 1, "exactly one restart: {restarts}");

    // plan, grant and result: three audit rows linked by the Change's hash
    let d = dump(&rt, &c, &h);
    let kinds = audit_kinds(&d);
    assert_eq!(kinds.iter().filter(|k| *k == "denied").count(), 5, "{kinds:?}");
    let sha = ce::machine::sha_hex(body.as_bytes());
    let rows = d["audit"].as_array().unwrap();
    let plan = rows.iter().find(|r| r["kind"] == "twin.put" && r["key"] == "p1").expect("plan row");
    assert_eq!(plan["body"]["body_sha256"], sha);
    let grant = rows.iter().find(|r| r["kind"] == "grant").expect("grant row");
    assert_eq!((grant["key"].clone(), grant["body"]["input_sha256"].clone()), (json!("a4"), json!(sha)));
    let result = rows.iter().find(|r| r["kind"] == "fleet.restart").expect("result row");
    assert_eq!(result["key"], "a4");
    assert!(grant["seq"].as_u64() < result["seq"].as_u64());
    assert_eq!(kinds.iter().filter(|k| *k == "grant").count(), 1);
}
