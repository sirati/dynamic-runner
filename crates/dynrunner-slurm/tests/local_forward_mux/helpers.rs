//! Shared fixtures for the local_forward_mux integration tests —
//! the same sshd-backed harness shape as
//! `dynrunner-gateway/tests/master_lifetime.rs` and
//! `dynrunner-driver/tests/master_lifetime/helpers.rs` (copied per
//! the established per-test-crate convention; integration test
//! binaries cannot share a non-published support crate).
//!
//! Provides the per-test `serialise` mutex, the `AuthorizedKey` RAII
//! guard that provisions a temporary ed25519 keypair under
//! `~/.ssh/authorized_keys`, and the `make_config` builder.

use std::path::PathBuf;
use std::process::Command as StdCommand;
use std::sync::OnceLock;
use std::time::Duration;

use dynrunner_gateway::SshConfig;
use tokio::sync::{Mutex, OwnedMutexGuard};

/// True iff something is listening on `localhost:22`. Used to skip
/// these integration tests on hosts without sshd (most CI runners).
pub(crate) fn sshd_reachable() -> bool {
    std::net::TcpStream::connect_timeout(
        &"127.0.0.1:22".parse().unwrap(),
        Duration::from_millis(200),
    )
    .is_ok()
}

/// Serialise all tests in this file. They mutate `~/.ssh/authorized_keys`
/// via `AuthorizedKey::provision` / `Drop` and bind ports + spawn
/// `ssh -M -N` masters; running them in parallel races
/// authorized_keys (`Drop` strips lines that other in-flight tests
/// wrote) and produces flaky `exit 255` handshake failures.
pub(crate) async fn serialise() -> OwnedMutexGuard<()> {
    static SERIAL: OnceLock<std::sync::Arc<Mutex<()>>> = OnceLock::new();
    let m = SERIAL.get_or_init(|| std::sync::Arc::new(Mutex::new(())));
    m.clone().lock_owned().await
}

/// Generate a temporary ed25519 keypair under a tempdir and append
/// the pubkey to `~/.ssh/authorized_keys`. Returns the keypair-pair
/// (private path, pubkey contents) and a guard whose Drop removes
/// the pubkey line from authorized_keys.
pub(crate) struct AuthorizedKey {
    key_dir: tempfile::TempDir,
    pub(crate) private_path: PathBuf,
    pubkey_line: String,
    authorized_keys: PathBuf,
}

impl AuthorizedKey {
    pub(crate) fn provision() -> Result<Self, String> {
        let key_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
        let private_path = key_dir.path().join("id_ed25519");
        let out = StdCommand::new("ssh-keygen")
            .args([
                "-t",
                "ed25519",
                "-N",
                "",
                "-q",
                "-C",
                "dynrunner-driver-test",
                "-f",
            ])
            .arg(&private_path)
            .output()
            .map_err(|e| format!("ssh-keygen spawn: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "ssh-keygen failed: stdout={:?} stderr={:?}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr),
            ));
        }
        let pub_path = private_path.with_extension("pub");
        let pubkey_line = std::fs::read_to_string(&pub_path)
            .map_err(|e| format!("read pubkey: {e}"))?
            .trim()
            .to_owned();

        let home = std::env::var("HOME").map_err(|_| "HOME not set".to_string())?;
        let authorized_keys = PathBuf::from(home).join(".ssh/authorized_keys");
        let mut existing = std::fs::read_to_string(&authorized_keys).unwrap_or_default();
        if !existing.ends_with('\n') && !existing.is_empty() {
            existing.push('\n');
        }
        existing.push_str(&pubkey_line);
        existing.push('\n');
        std::fs::write(&authorized_keys, existing)
            .map_err(|e| format!("write authorized_keys: {e}"))?;

        Ok(Self {
            key_dir,
            private_path,
            pubkey_line,
            authorized_keys,
        })
    }

    pub(crate) fn ssh_config_path(&self) -> PathBuf {
        let cfg = self.key_dir.path().join("ssh_config");
        if !cfg.exists() {
            std::fs::write(
                &cfg,
                "Host 127.0.0.1\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n",
            )
            .unwrap();
        }
        cfg
    }
}

impl Drop for AuthorizedKey {
    fn drop(&mut self) {
        if let Ok(content) = std::fs::read_to_string(&self.authorized_keys) {
            let cleaned: String = content
                .lines()
                .filter(|l| l.trim() != self.pubkey_line.trim())
                .collect::<Vec<_>>()
                .join("\n");
            let mut cleaned = cleaned;
            if !cleaned.is_empty() && !cleaned.ends_with('\n') {
                cleaned.push('\n');
            }
            let _ = std::fs::write(&self.authorized_keys, cleaned);
        }
        let _ = &self.key_dir;
    }
}

pub(crate) fn make_config(authorized: &AuthorizedKey) -> SshConfig {
    SshConfig {
        host: "127.0.0.1".to_owned(),
        port: 22,
        user: std::env::var("USER").ok(),
        identity_file: Some(authorized.private_path.to_string_lossy().into_owned()),
        config_file: Some(authorized.ssh_config_path().to_string_lossy().into_owned()),
    }
}
