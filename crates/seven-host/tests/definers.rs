//! The TypeScript and Python orders cells are the Rust one: 7 lines each, the same contract hash, and the same
//! proof (three real replicas on loopback, the leader SIGKILLed after the charge committed, the order completes
//! exactly once on every replica). Skipped with a note when `node` or `python3` is missing.
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_seven-host");

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap()
}

fn worker(lang: &str) -> Option<String> {
    let r = root();
    let (exe, w) = match lang {
        "ts" => ("node", format!("node {} {}", r.join("packages/seven-ts/run.mjs").display(), r.join("examples/orders-ts/cell.ts").display())),
        _ => ("python3", format!("python3 -I {} {}", r.join("packages/seven-py/run.py").display(), r.join("examples/orders-py/cell.py").display())),
    };
    let runner = r.join(if lang == "ts" { "packages/seven-ts/run.mjs" } else { "packages/seven-py/run.py" });
    if !runner.exists() {
        eprintln!("skip {lang}: {} is not in this checkout", runner.display());
        return None;
    }
    if Command::new(exe).arg("--version").output().is_ok_and(|o| o.status.success()) {
        Some(w)
    } else {
        eprintln!("skip {lang}: no {exe} on PATH");
        None
    }
}

fn describe(bin: &str, w: Option<&str>) -> Value {
    let mut c = Command::new(bin);
    if let Some(w) = w {
        c.env("SEVEN_WORKER", w);
    }
    let o = c.arg("describe").output().unwrap();
    serde_json::from_slice(&o.stdout).unwrap_or_else(|_| panic!("{}", String::from_utf8_lossy(&o.stderr)))
}

#[test]
fn the_ts_and_py_cells_are_seven_lines() {
    for f in ["examples/orders-ts/cell.ts", "examples/orders-py/cell.py"] {
        let Ok(src) = std::fs::read_to_string(root().join(f)) else {
            assert_ne!(f, "examples/orders-ts/cell.ts", "the TypeScript example is missing");
            continue;
        };
        assert_eq!(src.lines().count(), 7, "{f}:\n{src}");
    }
}

#[test]
fn describe_equals_the_rust_contract() {
    // the Rust orders binary, built beside this one by `cargo test --workspace`
    let orders = Path::new(BIN).with_file_name("orders");
    if !orders.exists() {
        panic!("{} missing: run `cargo test --workspace` (it builds examples/orders)", orders.display());
    }
    let rust = describe(orders.to_str().unwrap(), None);
    assert_eq!(rust["contract"].as_str().map(str::len), Some(64));
    for lang in ["ts", "py"] {
        let Some(w) = worker(lang) else { continue };
        let d = describe(BIN, Some(&w));
        assert_eq!(d["contract"], rust["contract"], "{lang} describe differs from the Rust cell:\n{d:#}\nvs\n{rust:#}");
    }
}

#[test]
fn a_worker_that_breaks_the_rules_is_refused() {
    let dir = std::env::temp_dir().join(format!("seven-host-bad-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let f = dir.join("cell.mjs");
    std::fs::write(&f, "export default cell(\"orders\", { replicas: 2 }).op(\"place\", { cap: \"orders.place\" }, async (cx, o) => o);\n").unwrap();
    let w = format!("node {} {}", root().join("packages/seven-ts/run.mjs").display(), f.display());
    let o = Command::new(BIN).env("SEVEN_WORKER", &w).arg("describe").output().unwrap();
    assert!(!o.status.success());
    assert!(String::from_utf8_lossy(&o.stderr).contains("replicas must be odd"), "{}", String::from_utf8_lossy(&o.stderr));
    let _ = std::fs::remove_dir_all(&dir);
}

fn prove(lang: &str) {
    let Some(w) = worker(lang) else { return };
    let dir = std::env::temp_dir().join(format!("seven-host-{lang}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let key: Value = serde_json::from_slice(&Command::new(BIN).env("SEVEN_WORKER", &w).arg("keygen").output().unwrap().stdout).unwrap();
    let ports: Vec<u16> = (0..3).map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()).collect();
    let mut lock = format!("[cell]\nname = \"orders\"\nroot = \"{}\"\n", key["public"].as_str().unwrap());
    for (i, p) in ports.iter().enumerate() {
        lock.push_str(&format!("[[replica]]\nnode = \"r{i}\"\naddr = \"127.0.0.1:{p}\"\n"));
    }
    let lock_path = dir.join("cell.lock");
    std::fs::write(&lock_path, lock).unwrap();
    let (d2, l2, w2) = (dir.clone(), lock_path.clone(), w.clone());
    let start = Arc::new(move |i: usize| -> Child {
        Command::new(BIN)
            .args(["serve", "--me", &i.to_string(), "--dir"])
            .arg(d2.join(format!("d{i}")))
            .env("SEVEN_WORKER", &w2)
            .env("SEVEN_LOCK", &l2)
            .env("SEVEN_AI", "local")
            .env("SEVEN_AI_LOCAL_MS", "1200")
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    });
    let kids: Arc<Mutex<Vec<Child>>> = Arc::new(Mutex::new((0..3).map(|i| start(i)).collect()));
    let (k2, start2) = (kids.clone(), start.clone());
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let s2 = stop.clone();
    let sup = std::thread::spawn(move || {
        while !s2.load(std::sync::atomic::Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(200));
            let mut k = k2.lock().unwrap();
            for i in 0..3 {
                if let Ok(Some(_)) = k[i].try_wait() {
                    std::thread::sleep(Duration::from_millis(1000));
                    k[i] = start2(i);
                }
            }
        }
    });
    let out = Command::new(BIN).arg("prove").env("SEVEN_WORKER", &w).env("SEVEN_LOCK", &lock_path).env("SEVEN_OWNER_SEED", key["seed"].as_str().unwrap()).output().unwrap();
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    sup.join().unwrap();
    for k in kids.lock().unwrap().iter_mut() {
        let _ = k.kill();
        let _ = k.wait();
    }
    let report: Value = serde_json::from_slice(&out.stdout).unwrap_or_else(|_| panic!("{lang}: {}", String::from_utf8_lossy(&out.stderr)));
    if let Ok(p) = std::env::var("SEVEN_PROVE_OUT") {
        std::fs::write(format!("{p}-{lang}.json"), serde_json::to_string_pretty(&report).unwrap()).unwrap();
    }
    let b = &report["order_b"];
    eprintln!("{lang}: failover {} ms, order {} ms, tries {}, resumed {}", b["failover_ms"], b["order_ms"], b["client_tries"], b["resumed_steps"]);
    assert_eq!(report["exactly_once"], Value::Bool(true), "{report:#}");
    assert_eq!(report["audit_heads_equal"], Value::Bool(true), "{report:#}");
    assert!(b["resumed_steps"].as_u64().unwrap() >= 1, "the new leader must resume a committed charge: {report:#}");
    assert_eq!(report["denied"]["refused"], Value::Bool(true));
    let replicas = report["replicas_after"].as_array().unwrap();
    assert!(replicas.iter().all(|r| r["charges_for_b"] == 1 && r["audit_receipts_for_b"] == 1), "{report:#}");
    assert_eq!(report["pass"], Value::Bool(true));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn typescript_cell_kill_the_leader_mid_order_exactly_once() {
    prove("ts");
}

#[test]
fn python_cell_kill_the_leader_mid_order_exactly_once() {
    prove("py");
}
