//! Shared fixtures for the master_lifetime integration tests.
//!
//! Provides the per-test `serialise` mutex, the `AuthorizedKey` RAII
//! guard that provisions a temporary ed25519 keypair under
//! `~/.ssh/authorized_keys`, the `make_config` builder, and the
//! `pid_alive` / `pick_free_port` / `re_probe_master_pid` utilities.
//! Items are `pub(crate)` so the lifecycle/adoption/invalidation/
//! panic_safety sub-test modules can reach them.

use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::OnceLock;
use std::time::Duration;

use dynrunner_driver::config::SshConfig;
use dynrunner_driver::ssh_target::SshTarget;
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

unsafe extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

/// True iff the PID is still alive. We use `kill(pid, 0)` (signal 0:
/// permission/existence probe, no actual signal sent) which returns
/// `ESRCH` once the process has been reaped.
pub(crate) fn pid_alive(pid: u32) -> bool {
    // SAFETY: kill with signal 0 is a pure existence/permission
    // probe with no side effect on the target.
    unsafe { kill(pid as i32, 0) == 0 }
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
                "-t", "ed25519",
                "-N", "",
                "-q",
                "-C", "dynrunner-driver-test",
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
    let user = std::env::var("USER").ok();
    let host = "127.0.0.1";
    SshConfig {
        port: 22,
        target: SshTarget::from_user_host(user.as_deref(), host),
        identity_file: Some(authorized.private_path.clone()),
        config_file: Some(authorized.ssh_config_path()),
        forwarded_ports: Vec::new(),
    }
}

pub(crate) fn re_probe_master_pid(authorized: &AuthorizedKey, cp: &Path, target: &str) -> u32 {
    let out = StdCommand::new("ssh")
        .args([
            "-i",
            authorized.private_path.to_str().unwrap(),
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "IdentityAgent=none",
            "-F",
            authorized.ssh_config_path().to_str().unwrap(),
            "-O",
            "check",
            "-o",
            &format!("ControlPath={}", cp.display()),
            target,
        ])
        .output()
        .expect("spawn ssh -O check");
    assert!(
        out.status.success(),
        "ssh -O check failed: stdout={:?} stderr={:?}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let marker = "Master running (pid=";
    let i = combined
        .find(marker)
        .expect("ssh -O check output missing `Master running (pid=`");
    let rest = &combined[i + marker.len()..];
    let pid_str: String = rest.chars().take_while(char::is_ascii_digit).collect();
    pid_str.parse().expect("ssh -O check pid was non-numeric")
}

/// Pick a free localhost TCP port by binding to :0 and letting the
/// kernel allocate. Used by `adopt_disconnect_partial_cleanup` to
/// produce two non-colliding port numbers for the forward.
pub(crate) fn pick_free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    l.local_addr().expect("local_addr").port()
}
