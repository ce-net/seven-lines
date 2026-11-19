//! One TCP port per replica, one JSON object per line each way. Peer traffic (`vote`, `append`) is taken only from
//! the addresses in `cell.lock`; every `call` carries a capability chain and the holder's proof over the request.
use crate::machine::Entry;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use shop_chain::Auth;
use shop_replog::{AppendReq, AppendResp, Peers, VoteReq, VoteResp};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum Msg {
    Vote { req: VoteReq },
    Append { req: AppendReq<Entry> },
    /// `body` is the op's input as JSON text: the exact bytes the proof signs.
    Call { op: String, key: String, body: String, auth: Auth },
    Status,
    /// The applied state (ledger, done, audit, twin) for proofs; needs `<cell>:read`.
    Dump { auth: Auth },
    /// Chaos: the replica kills itself with SIGKILL after `after_step` of the next order commits (or now, if
    /// `after_step` is negative). Needs `<cell>:chaos`.
    Crash { after_step: i64, auth: Auth },
}

/// The bytes a `Dump`/`Crash` proof signs.
pub fn admin_body(m: &Msg) -> String {
    match m {
        Msg::Crash { after_step, .. } => format!("crash:{after_step}"),
        _ => String::new(),
    }
}

pub async fn send(stream: &mut BufReader<TcpStream>, msg: &Msg, wait: Duration) -> Result<Value, String> {
    let mut line = serde_json::to_vec(msg).map_err(|e| e.to_string())?;
    line.push(b'\n');
    let io = async {
        stream.get_mut().write_all(&line).await.map_err(|e| e.to_string())?;
        let mut buf = String::new();
        let n = stream.read_line(&mut buf).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("connection closed".to_string());
        }
        serde_json::from_str::<Value>(&buf).map_err(|e| e.to_string())
    };
    tokio::time::timeout(wait, io).await.map_err(|_| "timeout".to_string())?
}

pub async fn connect(addr: &str, wait: Duration) -> Result<BufReader<TcpStream>, String> {
    let s = tokio::time::timeout(wait, TcpStream::connect(addr)).await.map_err(|_| "connect timeout".to_string())?.map_err(|e| e.to_string())?;
    let _ = s.set_nodelay(true);
    Ok(BufReader::new(s))
}

/// One request on a fresh connection.
pub async fn request(addr: &str, msg: &Msg, wait: Duration) -> Result<Value, String> {
    let mut s = connect(addr, Duration::from_millis(1500).min(wait)).await?;
    send(&mut s, msg, wait).await
}

/// The replog's view of the other replicas: one kept connection per peer, reopened after any error.
pub struct TcpPeers {
    addrs: Vec<String>,
    conns: Vec<Arc<Mutex<Option<BufReader<TcpStream>>>>>,
}

impl TcpPeers {
    pub fn new(addrs: Vec<String>) -> TcpPeers {
        let conns = addrs.iter().map(|_| Arc::new(Mutex::new(None))).collect();
        TcpPeers { addrs, conns }
    }

    fn call(&self, peer: usize, msg: Msg) -> Pin<Box<dyn Future<Output = Option<Value>> + Send>> {
        let addr = self.addrs[peer].clone();
        let slot = self.conns[peer].clone();
        Box::pin(async move {
            let mut g = slot.lock().await;
            for _ in 0..2 {
                if g.is_none() {
                    *g = connect(&addr, Duration::from_millis(400)).await.ok();
                }
                let Some(s) = g.as_mut() else { return None };
                match send(s, &msg, Duration::from_millis(1500)).await {
                    Ok(v) => return Some(v),
                    Err(_) => *g = None,
                }
            }
            None
        })
    }
}

impl Peers<Entry> for TcpPeers {
    fn vote(&self, peer: usize, req: VoteReq) -> Pin<Box<dyn Future<Output = Option<VoteResp>> + Send>> {
        let f = self.call(peer, Msg::Vote { req });
        Box::pin(async move { f.await.and_then(|v| serde_json::from_value(v).ok()) })
    }
    fn append(&self, peer: usize, req: AppendReq<Entry>) -> Pin<Box<dyn Future<Output = Option<AppendResp>> + Send>> {
        let f = self.call(peer, Msg::Append { req });
        Box::pin(async move { f.await.and_then(|v| serde_json::from_value(v).ok()) })
    }
}
