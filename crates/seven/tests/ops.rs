//! In-process: one replica (a majority of one), real chains, the op table the macros register.
use seven::node::{op_code, signed_body, AiBackend, Node};
use seven::prelude::*;
use seven::CellDef;
use serde_json::{json, Value};
use shop_chain::{Caveats, Chain, Holder, Identity};
use std::sync::Arc;
use std::time::Duration;

#[seven::record(cell = shop)]
struct Item {
    sku: Sku,
    paid: Money,
}

#[seven::op(cap = "shop.buy")]
async fn buy(cx: Cx, i: Item) -> Result<Charge> {
    cx.pay().charge(i.paid).await
}

#[seven::op(cap = "shop.live")]
async fn live(cx: Cx, i: Item) -> Result<Charge> {
    cx.pay().live().charge(i.paid).await
}

#[seven::op(cap = "shop.ask")]
async fn ask(cx: Cx, i: Item) -> Result<Note> {
    cx.ai().ask(Quota::per_day(2), summary(&i)).await
}

#[seven::op(cap = "shop.keep")]
async fn keep(cx: Cx, i: Item) -> Result<AuditReceipt> {
    cx.twin().put(&i).audit(&i.sku).await
}

const DEF: CellDef = CellDef { name: "shop", version: 1, replicas: 3, skill: "" };

/// Three replicas on loopback in this process; the answer is the elected leader.
async fn node(owner: &Identity) -> Arc<Node> {
    let ports: Vec<u16> = (0..3).map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()).collect();
    let addrs: Vec<String> = ports.iter().map(|p| format!("127.0.0.1:{p}")).collect();
    let base = std::env::temp_dir().join(format!("seven-ops-{}-{}", std::process::id(), ports[0]));
    let nodes: Vec<Arc<Node>> = (0..3)
        .map(|i| {
            let n = Node::open(DEF, i, addrs.clone(), owner.public_hex(), base.join(i.to_string()), AiBackend::Local);
            let (n2, a) = (n.clone(), addrs[i].clone());
            tokio::spawn(async move { seven::node::serve(n2, &a).await });
            n
        })
        .collect();
    for _ in 0..300 {
        if let Some(n) = nodes.iter().find(|n| n.log.ready()) {
            return n.clone();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no leader among three replicas");
}

fn holder(owner: &Identity, rights: &[&str]) -> Holder {
    let agent = Identity::generate();
    let chain = Chain::root(owner, &agent.public_hex(), rights.iter().map(|s| s.to_string()).collect(), Caveats { tenant: "shop".into(), until: shop_chain::now() + 600, ..Default::default() });
    Holder { id: agent, chain }
}

async fn call(n: &Arc<Node>, h: &Holder, op: &str, key: &str, body: &Value) -> Value {
    let body = body.to_string();
    n.call(op, key, &body, &h.auth("shop", op_code(op), &signed_body(key, &body))).await
}

fn item() -> Value {
    json!({ "sku": "MUG", "paid": { "minor": 9900, "cur": "SEK" } })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_key_charges_once_and_replays_the_receipt() {
    std::env::set_var("SHOP_BEAT_MS", "20");
    let owner = Identity::generate();
    let n = node(&owner).await;
    let h = holder(&owner, &["shop:*"]);
    let a = call(&n, &h, "buy", "k1", &item()).await;
    let b = call(&n, &h, "buy", "k1", &item()).await;
    assert_eq!(a["out"], b["out"], "{a} {b}");
    assert_eq!(b["replayed"], json!(true));
    call(&n, &h, "buy", "k2", &item()).await;
    assert_eq!(n.log.read(|s, _| s.ledger.len()), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chain_without_the_right_is_refused_and_audited() {
    std::env::set_var("SHOP_BEAT_MS", "20");
    let owner = Identity::generate();
    let n = node(&owner).await;
    let r = call(&n, &holder(&owner, &["shop:ask"]), "buy", "k", &item()).await;
    assert!(r["err"]["Denied"].as_str().unwrap().contains("shop:buy"), "{r}");
    let (rows, kind) = n.log.read(|s, _| (s.ledger.len(), s.audit.last().map(|a| a.kind.clone())));
    assert_eq!((rows, kind.as_deref()), (0, Some("denied")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stranger_root_and_a_forged_proof_are_refused() {
    std::env::set_var("SHOP_BEAT_MS", "20");
    let owner = Identity::generate();
    let n = node(&owner).await;
    let r = call(&n, &holder(&Identity::generate(), &["shop:*"]), "buy", "k", &item()).await;
    assert!(r["err"]["Denied"].is_string(), "{r}");
    // a real chain, but the proof is signed for another key: replaying it under a new key fails
    let h = holder(&owner, &["shop:*"]);
    let body = item().to_string();
    let auth = h.auth("shop", op_code("buy"), &signed_body("k-signed", &body));
    let r = n.call("buy", "k-other", &body, &auth).await;
    assert!(r["err"]["Denied"].is_string(), "{r}");
    assert_eq!(n.log.read(|s, _| s.ledger.len()), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_payments_need_pay_live_and_this_build_has_no_live_provider() {
    std::env::set_var("SHOP_BEAT_MS", "20");
    let owner = Identity::generate();
    let n = node(&owner).await;
    let r = call(&n, &holder(&owner, &["shop:*"]), "live", "k", &item()).await;
    assert!(r["err"]["Denied"].as_str().unwrap().contains("pay:live"), "{r}");
    let r = call(&n, &holder(&owner, &["shop:*", "pay:live"]), "live", "k2", &item()).await;
    assert!(r["err"]["Backend"].as_str().unwrap().contains("test mode only"), "{r}");
    assert_eq!(n.log.read(|s, _| s.ledger.len()), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_agent_quota_refuses_the_third_ask_of_the_day() {
    std::env::set_var("SHOP_BEAT_MS", "20");
    let owner = Identity::generate();
    let n = node(&owner).await;
    let h = holder(&owner, &["shop:*"]);
    let a = call(&n, &h, "ask", "q1", &item()).await;
    assert_eq!(a["out"]["asks_left"], json!(1), "{a}");
    call(&n, &h, "ask", "q2", &item()).await;
    let c = call(&n, &h, "ask", "q3", &item()).await;
    assert_eq!(c["err"]["Quota"]["limit"], json!(2), "{c}");
    // another agent has its own bucket
    let d = call(&n, &holder(&owner, &["shop:*"]), "ask", "q4", &item()).await;
    assert!(d["ok"].is_boolean(), "{d}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_twin_write_leaves_a_verifiable_audit_row() {
    std::env::set_var("SHOP_BEAT_MS", "20");
    let owner = Identity::generate();
    let n = node(&owner).await;
    let h = holder(&owner, &["shop:*"]);
    let r = call(&n, &h, "keep", "t1", &item()).await;
    assert_eq!(r["out"]["seq"], json!(1), "{r}");
    let (twin, ok, of) = n.log.read(|s, _| (s.twin.get("Item/t1").cloned(), s.audit_chain_ok(), s.audit[0].body["of"].clone()));
    assert_eq!(twin.unwrap()["sku"], json!("MUG"));
    assert!(ok);
    assert_eq!(of, json!("MUG"));
}

#[test]
fn the_registered_op_table_names_every_cap() {
    let ops: Vec<(&str, &str)> = seven::ops().iter().map(|o| (o.name, o.cap)).collect();
    assert_eq!(ops, vec![("ask", "shop.ask"), ("buy", "shop.buy"), ("keep", "shop.keep"), ("live", "shop.live")]);
    assert_eq!(seven::right_of("shop.buy"), "shop:buy");
    assert_eq!(seven::records()[0].fields, &[("sku", "Sku"), ("paid", "Money")]);
}
