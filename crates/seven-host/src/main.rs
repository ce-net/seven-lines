//! `seven-host --worker "<runner> <file>" <command>`: a seven cell whose ops live in a TypeScript or Python worker.
//!
//! The host is the `seven` runtime unchanged: `serve` runs a replica (gate, quorum log, exactly-once `cx` steps),
//! `call`, `prove`, `up`, `describe`, `skill` work as for a Rust cell. Only the ops table differs: it is filled
//! from the worker's `Describe` instead of from the macros, and each op's body is an `Invoke` on the worker.
//!
//! - The gate runs here before `Invoke`: a refused caller never reaches the worker.
//! - The worker gets no network (a fresh network namespace, `unshare -rn`, when this host can make one; set
//!   `SEVEN_WORKER_JAIL=off` to skip), `nice 10` on top of the replica's own CPU cap, and an environment with no
//!   seeds or keys in it. Its only way out is `Cx`, and every `Cx` that has an effect is a step in the log.
//! - `SEVEN_WORKER` may stand in for `--worker`; `up` passes it to the replicas it starts.
use seven::serde_json::{self, json, Value};
use seven::{CellDef, Error, Grant, Home, Money, OpDef, Quota, RecordDef};
use seven_worker_api::{CxReq, Frame, InvokeReq, Layer};
use std::io::{BufRead, BufReader, Write};
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

const USAGE: &str = "usage: seven-host --worker \"<runner> <file>\" <command> [args]
  runner: `node <seven>/packages/seven-ts/run.mjs cell.ts` or `python3 <seven>/packages/seven-py/run.py cell.py`
  command: any command of a seven cell (serve, call, grant, skill, status, dump, crash, describe, keygen, up, down, prove)";

static WORKER: OnceLock<String> = OnceLock::new();
static RECORDS: OnceLock<Vec<String>> = OnceLock::new();
static PIPE: OnceLock<tokio::sync::Mutex<Option<Pipe>>> = OnceLock::new();

fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let worker = match args.iter().position(|a| a == "--worker") {
        Some(i) if i + 1 < args.len() => {
            let w = args.remove(i + 1);
            args.remove(i);
            Some(w)
        }
        _ => std::env::var("SEVEN_WORKER").ok(),
    };
    let Some(worker) = worker.filter(|w| !w.trim().is_empty()) else {
        eprintln!("{USAGE}");
        std::process::exit(2);
    };
    std::env::set_var("SEVEN_WORKER", &worker);
    let def = match describe(&worker) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("worker `{worker}`: {e}");
            std::process::exit(1);
        }
    };
    let cell = match define(def) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("worker `{worker}` describes a cell seven refuses: {e}");
            std::process::exit(1);
        }
    };
    let _ = WORKER.set(worker);
    seven::run_with(cell, args);
}

/// The worker's command line, jailed: no network when this host can make a namespace, nice 10, a clean environment.
fn command(worker: &str) -> std::process::Command {
    let jail = std::env::var("SEVEN_WORKER_JAIL").unwrap_or_else(|_| "auto".into());
    let netns = jail != "off"
        && std::process::Command::new("unshare").args(["-rn", "true"]).stdout(Stdio::null()).stderr(Stdio::null()).status().is_ok_and(|s| s.success());
    if jail == "on" && !netns {
        eprintln!("SEVEN_WORKER_JAIL=on but `unshare -rn` does not work here");
        std::process::exit(1);
    }
    let mut c = if netns {
        let mut c = std::process::Command::new("unshare");
        c.args(["-rn", "nice", "-n", "10", "sh", "-c"]);
        c
    } else {
        let mut c = std::process::Command::new("nice");
        c.args(["-n", "10", "sh", "-c"]);
        c
    };
    c.arg(format!("exec {worker}"));
    c.env_clear();
    for k in ["PATH", "HOME", "LANG", "TMPDIR", "NODE_PATH", "PYTHONPATH"] {
        if let Ok(v) = std::env::var(k) {
            c.env(k, v);
        }
    }
    c.env("SEVEN_WORKER_NET", if netns { "none" } else { "host" });
    c
}

/// `Describe` on a short-lived worker.
fn describe(worker: &str) -> Result<seven_worker_api::CellDef, String> {
    let mut child = command(worker).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().map_err(|e| format!("start: {e}"))?;
    let mut stdin = child.stdin.take().ok_or("no stdin")?;
    stdin.write_all(Frame::Describe.line().as_bytes()).map_err(|e| format!("write: {e}"))?;
    let mut line = String::new();
    BufReader::new(child.stdout.take().ok_or("no stdout")?).read_line(&mut line).map_err(|e| format!("read: {e}"))?;
    drop(stdin);
    let _ = child.kill();
    let _ = child.wait();
    match serde_json::from_str::<Frame>(line.trim()) {
        Ok(Frame::Def { def }) => Ok(def),
        Ok(other) => Err(format!("answered Describe with {other:?}")),
        Err(e) => Err(format!("answered Describe with {:?}: {e}", line.trim())),
    }
}

/// The worker's declaration into the runtime's ops table, checked as the macros check it.
fn define(d: seven_worker_api::CellDef) -> Result<CellDef, String> {
    let home = match d.home.as_str() {
        "Fleet" => Home::Fleet,
        "Device" => Home::Device,
        h => return Err(format!("home is Fleet or Device, not {h}")),
    };
    let replicas = d.replicas.unwrap_or(if home == Home::Device { 1 } else { 3 });
    if home == Home::Device && replicas != 1 {
        return Err("home = Device means replicas = 1".into());
    }
    if home == Home::Fleet && (replicas < 3 || replicas % 2 == 0) {
        return Err("replicas must be odd and at least 3 (3, 5, 7)".into());
    }
    if d.name.is_empty() || !d.name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return Err(format!("cell name {:?}", d.name));
    }
    let mut ops = Vec::new();
    for o in d.ops {
        if !o.cap.contains('.') {
            return Err(format!("op {}: cap is \"<cell>.<right>\", not {:?}", o.name, o.cap));
        }
        let grant = match o.grant.as_str() {
            "None" => Grant::None,
            "Passkey" => Grant::Passkey,
            g => return Err(format!("op {}: grant is Passkey or None, not {g}", o.name)),
        };
        ops.push(OpDef { name: leak(o.name), cap: leak(o.cap), input: leak(o.input), output: leak(o.output), grant, call });
    }
    let names: Vec<String> = d.records.iter().map(|r| r.name.clone()).collect();
    let cell: &'static str = leak(d.name);
    let recs = d
        .records
        .into_iter()
        .map(|r| {
            let fields: Vec<(&'static str, &'static str)> = r.fields.into_iter().map(|(f, t)| (leak(f), leak(t))).collect();
            RecordDef { cell, name: leak(r.name), fields: Box::leak(fields.into_boxed_slice()) }
        })
        .collect();
    let _ = RECORDS.set(names);
    seven::define(ops, recs);
    Ok(CellDef { name: cell, version: d.version, replicas, skill: leak(d.skill), home })
}

// ------------------------------------------------------------------------------------------------ invoke

struct Pipe {
    child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    stdout: tokio::io::BufReader<tokio::process::ChildStdout>,
    /// set while an `Invoke` is open: a call dropped half way leaves frames behind, so that worker is replaced
    busy: bool,
}

impl Pipe {
    fn start() -> Result<Pipe, Error> {
        let worker = WORKER.get().ok_or_else(|| Error::Backend("no worker".into()))?;
        let mut child = tokio::process::Command::from(command(worker))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| Error::Backend(format!("worker start: {e}")))?;
        let stdin = child.stdin.take().ok_or_else(|| Error::Backend("worker stdin".into()))?;
        let stdout = tokio::io::BufReader::new(child.stdout.take().ok_or_else(|| Error::Backend("worker stdout".into()))?);
        Ok(Pipe { child, stdin, stdout, busy: false })
    }
    async fn send(&mut self, f: &Frame) -> Result<(), Error> {
        self.stdin.write_all(f.line().as_bytes()).await.map_err(|e| Error::Backend(format!("worker write: {e}")))
    }
    async fn recv(&mut self) -> Result<Frame, Error> {
        let secs = std::env::var("SEVEN_WORKER_TIMEOUT_S").ok().and_then(|v| v.parse().ok()).unwrap_or(300u64);
        let mut line = String::new();
        match tokio::time::timeout(Duration::from_secs(secs), self.stdout.read_line(&mut line)).await {
            Err(_) => Err(Error::Backend(format!("worker silent for {secs} s"))),
            Ok(Err(e)) => Err(Error::Backend(format!("worker read: {e}"))),
            Ok(Ok(0)) => Err(Error::Backend("worker exited".into())),
            Ok(Ok(_)) => serde_json::from_str(line.trim()).map_err(|e| Error::Backend(format!("worker frame {:?}: {e}", line.trim()))),
        }
    }
}

/// Every worker op's body: one `Invoke`, its `Cx` calls served from this replica's `cx`.
fn call(cx: seven::Cx, input: Value) -> seven::BoxFut<Value> {
    Box::pin(async move {
        let mut g = PIPE.get_or_init(|| tokio::sync::Mutex::new(None)).lock().await;
        if g.as_mut().map_or(true, |p| p.busy || !matches!(p.child.try_wait(), Ok(None))) {
            *g = Some(Pipe::start()?);
        }
        let p = g.as_mut().expect("started");
        p.busy = true;
        let r = invoke(p, &cx, input).await;
        p.busy = false;
        match r {
            Ok(v) => Ok(v),
            Err(Fail::Op(e)) => Err(e),
            Err(Fail::Worker(e)) => {
                // a broken pipe or a protocol error: the next call starts a fresh worker; the retry replays the log
                *g = None;
                Err(e)
            }
        }
    })
}

enum Fail {
    /// the op's own refusal; the worker is fine
    Op(Error),
    /// the worker broke the contract or died
    Worker(Error),
}

async fn invoke(p: &mut Pipe, cx: &seven::Cx, input: Value) -> Result<Value, Fail> {
    let req = InvokeReq { op: cx.op().to_string(), request: cx.key().to_string(), caller: cx.caller().to_string(), input };
    p.send(&Frame::Invoke(req)).await.map_err(Fail::Worker)?;
    let mut steps = 0u32;
    loop {
        match p.recv().await.map_err(Fail::Worker)? {
            Frame::Cx(c) => {
                let reply = match layer(cx, &c, &mut steps).await {
                    Ok(v) => Frame::CxReply { ok: Some(v), err: None },
                    Err(e) => Frame::CxReply { ok: None, err: Some(serde_json::to_value(&e).unwrap_or(Value::Null)) },
                };
                p.send(&reply).await.map_err(Fail::Worker)?;
            }
            Frame::Return { ok: Some(v), err: None } => return Ok(v),
            Frame::Return { err: Some(e), .. } => {
                let e = serde_json::from_value::<Error>(e.clone()).unwrap_or_else(|_| Error::Backend(e.as_str().map(str::to_string).unwrap_or_else(|| e.to_string())));
                return Err(Fail::Op(e));
            }
            other => return Err(Fail::Worker(Error::Backend(format!("worker sent {other:?} inside an Invoke")))),
        }
    }
}

fn arg<T: seven::serde::de::DeserializeOwned>(args: &Value, k: &str) -> Result<T, Error> {
    serde_json::from_value(args.get(k).cloned().unwrap_or(Value::Null)).map_err(|e| Error::Input(format!("cx arg {k}: {e}")))
}

fn to<T: seven::serde::Serialize>(r: Result<T, Error>) -> Result<Value, Error> {
    r.and_then(|v| serde_json::to_value(v).map_err(Error::input))
}

fn declared(record: &str) -> Result<(), Error> {
    if RECORDS.get().is_some_and(|r| r.iter().any(|n| n == record)) {
        Ok(())
    } else {
        Err(Error::Input(format!("no record {record} in this cell")))
    }
}

/// One `Cx` from the worker, through the same layers a Rust op uses (so the same log steps, the same exactly-once).
async fn layer(cx: &seven::Cx, c: &CxReq, steps: &mut u32) -> Result<Value, Error> {
    if c.request != cx.key() {
        return Err(Error::Input(format!("cx for request {:?} inside request {:?}", c.request, cx.key())));
    }
    if c.layer != Layer::Store {
        if c.step != *steps {
            return Err(Error::Input(format!("cx step {} where step {} is next", c.step, steps)));
        }
        *steps += 1;
    }
    let a = &c.args;
    match c.layer {
        Layer::Pay => {
            let m: Money = arg(a, "charge")?;
            let pay = if a.get("live") == Some(&Value::Bool(true)) { cx.pay().live() } else { cx.pay() };
            to(pay.charge(m).await)
        }
        Layer::Ai => {
            let q = a.get("quota").and_then(|q| q.get("per_day")).and_then(Value::as_u64).map(|n| Quota::per_day(n as u32)).unwrap_or_else(Quota::agent);
            let prompt: String = arg(a, "prompt")?;
            to(cx.ai().ask(q, prompt).await)
        }
        Layer::Twin => {
            let record: String = arg(a, "record")?;
            declared(&record)?;
            let body = a.get("body").cloned().unwrap_or(Value::Null);
            let of = a.get("of").cloned().unwrap_or(Value::Null);
            to(cx.twin().put_named(&record, body).audit(&of).await)
        }
        Layer::Store => {
            let record: String = arg(a, "all")?;
            declared(&record)?;
            Ok(json!(cx.twin().all_named(&record)))
        }
        Layer::Fleet => match a.get("restart").and_then(Value::as_str) {
            Some(cell) => to(cx.fleet().apply(seven::FleetChange::Restart { cell: cell.to_string() }).await),
            None => to(cx.fleet().snapshot().await),
        },
    }
}
