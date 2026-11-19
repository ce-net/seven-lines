//! `cell.lock`: where the replicas run and which key roots the cell's capabilities. No line of the cell's code
//! names a machine; this file does, and `up` writes the pin into it.
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Lock {
    #[serde(default)]
    pub cell: LockCell,
    #[serde(default, rename = "replica")]
    pub replicas: Vec<Replica>,
    #[serde(default)]
    pub pin: Option<Pin>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LockCell {
    #[serde(default)]
    pub name: String,
    /// Public key (hex) of the owner root every capability chain must start at.
    #[serde(default)]
    pub root: String,
    /// Repository `up` builds and installs from.
    #[serde(default)]
    pub repo: String,
    /// The api name the node's door and cell.install know the cell by.
    #[serde(default)]
    pub api: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Replica {
    /// A node name from `ce nodes`; `local` (or this node's name) is started here.
    pub node: String,
    /// host:port the replica listens on and its peers dial.
    pub addr: String,
    /// `sim` = a second process on the same machine, standing in for a node we do not have.
    #[serde(default)]
    pub sim: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Pin {
    pub rev: String,
    pub at: String,
    #[serde(default)]
    pub artifacts: Vec<PinnedArtifact>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PinnedArtifact {
    pub target: String,
    pub bin: String,
    pub sha256: String,
}

impl Lock {
    pub fn find() -> Option<PathBuf> {
        if let Ok(p) = std::env::var("SEVEN_LOCK") {
            return Some(PathBuf::from(p));
        }
        let mut dirs = vec![std::env::current_dir().ok()?];
        if let Ok(exe) = std::env::current_exe() {
            dirs.extend(exe.ancestors().skip(1).map(Path::to_path_buf));
        }
        dirs.extend(std::env::current_dir().ok()?.ancestors().skip(1).map(Path::to_path_buf));
        dirs.into_iter().map(|d| d.join("cell.lock")).find(|p| p.is_file())
    }
    pub fn load(p: &Path) -> Result<Lock, String> {
        let t = std::fs::read_to_string(p).map_err(|e| format!("{}: {e}", p.display()))?;
        toml::from_str(&t).map_err(|e| format!("{}: {e}", p.display()))
    }
    pub fn save(&self, p: &Path) -> Result<(), String> {
        let t = toml::to_string_pretty(self).map_err(|e| e.to_string())?;
        std::fs::write(p, format!("# written by `<cell> up`; replicas and the root key are yours to edit\n{t}")).map_err(|e| e.to_string())
    }
    pub fn addrs(&self) -> Vec<String> {
        self.replicas.iter().map(|r| r.addr.clone()).collect()
    }
}
