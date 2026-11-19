//! The seven-line binary as three real processes on loopback: an order, then SIGKILL of the leader after its
//! charge committed and before the order finished. The order must complete once: one ledger row on every replica,
//! the retry answered from the log, equal audit heads. A supervisor thread restarts a dead replica after 1 s, as
//! systemd's Restart=on-failure does in `orders up`.
use serde_json::Value;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_orders");

#[test]
fn the_seven_lines_are_seven_lines() {
    let src = include_str!("../src/main.rs");
    assert_eq!(src.lines().count(), 7, "{src}");
}

#[test]
fn kill_the_leader_mid_order_and_the_order_completes_exactly_once() {
    let dir = std::env::temp_dir().join(format!("seven-failover-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let key: Value = serde_json::from_slice(&Command::new(BIN).arg("keygen").output().unwrap().stdout).unwrap();
    let ports: Vec<u16> = (0..3).map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()).collect();
    let mut lock = format!("[cell]\nname = \"orders\"\nroot = \"{}\"\n", key["public"].as_str().unwrap());
    for (i, p) in ports.iter().enumerate() {
        lock.push_str(&format!("[[replica]]\nnode = \"r{i}\"\naddr = \"127.0.0.1:{p}\"\n"));
    }
    let lock_path = dir.join("cell.lock");
    std::fs::write(&lock_path, lock).unwrap();
    let (d2, l2) = (dir.clone(), lock_path.clone());
    let start = Arc::new(move |i: usize| -> Child {
        Command::new(BIN)
            .args(["serve", "--me", &i.to_string(), "--dir"])
            .arg(d2.join(format!("d{i}")))
            .env("SEVEN_LOCK", &l2)
            .env("SEVEN_AI", "local")
            .env("SEVEN_AI_LOCAL_MS", "1200")
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    });
    let kids: Arc<Mutex<Vec<Child>>> = Arc::new(Mutex::new((0..3).map(|i| start(i)).collect()));
    let k2 = kids.clone();
    let start2 = start.clone();
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
    let out = Command::new(BIN).arg("prove").env("SEVEN_LOCK", &lock_path).env("SEVEN_OWNER_SEED", key["seed"].as_str().unwrap()).output().unwrap();
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    sup.join().unwrap();
    for k in kids.lock().unwrap().iter_mut() {
        let _ = k.kill();
        let _ = k.wait();
    }
    let report: Value = serde_json::from_slice(&out.stdout).unwrap_or_else(|_| panic!("{}", String::from_utf8_lossy(&out.stderr)));
    let b = &report["order_b"];
    eprintln!("failover {} ms, order {} ms, tries {}, resumed {}", b["failover_ms"], b["order_ms"], b["client_tries"], b["resumed_steps"]);
    assert_eq!(report["exactly_once"], Value::Bool(true), "{report:#}");
    assert_eq!(report["audit_heads_equal"], Value::Bool(true), "{report:#}");
    assert!(b["resumed_steps"].as_u64().unwrap() >= 1, "the new leader must resume a committed charge: {report:#}");
    assert_eq!(report["denied"]["refused"], Value::Bool(true));
    let replicas = report["replicas_after"].as_array().unwrap();
    assert!(replicas.iter().all(|r| r["charges_for_b"] == 1), "{report:#}");
    assert_eq!(report["pass"], Value::Bool(true));
    let _ = std::fs::remove_dir_all(&dir);
}
