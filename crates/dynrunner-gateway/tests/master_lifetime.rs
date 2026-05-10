//! Integration tests for the SSH master's lifetime contract:
//!
//! - **T3** (`drop_cleans_master`): pinning the bug-(g) fix —
//!   dropping `SshGateway` without calling `disconnect()` first must
//!   take the master process down within ~1s, not leak it.
//! - **T4** (`master_died_observer_emits_log`): pinning task #5's
//!   acceptance gate (3) — when the master process exits while the
//!   gateway is connected, the periodic poller must emit a
//!   `tracing::error!("SSH master exited unexpectedly")` event so
//!   any future similar-class bug is observable instead of silent.
//!
//! These tests need an actual sshd they can authenticate against.
//! They:
//!   1. Probe TCP `localhost:22` first; skip with a clear message
//!      when sshd isn't running (most CI containers).
//!   2. Generate a temporary ed25519 keypair, append the pubkey to
//!      `~/.ssh/authorized_keys`, run the gateway against
//!      `localhost`, and clean the pubkey out of authorized_keys at
//!      test end.
//!
//! Run on a host with sshd:
//!   `cargo test -p dynrunner-gateway --test master_lifetime`

use std::path::PathBuf;
use std::process::Command as StdCommand;
use std::time::{Duration, Instant};

use dynrunner_gateway::config::SshConfig;
use dynrunner_gateway::ssh::SshGateway;
use dynrunner_gateway::traits::Gateway;

/// True iff something is listening on `localhost:22`. Used to skip
/// these integration tests on hosts without sshd (most CI runners).
fn sshd_reachable() -> bool {
    std::net::TcpStream::connect_timeout(
        &"127.0.0.1:22".parse().unwrap(),
        Duration::from_millis(200),
    )
    .is_ok()
}

unsafe extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

/// True iff the PID is still alive. We use `kill(pid, 0)` (signal 0:
/// permission/existence probe, no actual signal sent) which returns
/// `ESRCH` once the process has been reaped. PID-reuse is a
/// theoretical concern but irrelevant on a 1s test window.
fn pid_alive(pid: u32) -> bool {
    // SAFETY: kill with signal 0 is a pure existence/permission
    // probe with no side effect on the target.
    unsafe { kill(pid as i32, 0) == 0 }
}

/// Generate a temporary ed25519 keypair under a tempdir and append
/// the pubkey to `~/.ssh/authorized_keys`. Returns the keypair-pair
/// (private path, pubkey contents) and a guard whose Drop removes
/// the pubkey line from authorized_keys.
struct AuthorizedKey {
    key_dir: tempfile::TempDir,
    private_path: PathBuf,
    pubkey_line: String,
    authorized_keys: PathBuf,
}

impl AuthorizedKey {
    fn provision() -> Result<Self, String> {
        let key_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
        let private_path = key_dir.path().join("id_ed25519");
        // ssh-keygen with empty passphrase, ed25519, no comment.
        let out = StdCommand::new("ssh-keygen")
            .args([
                "-t", "ed25519",
                "-N", "",
                "-q",
                "-C", "dynrunner-gateway-test",
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

        let home = std::env::var("HOME")
            .map_err(|_| "HOME not set".to_string())?;
        let authorized_keys = PathBuf::from(home).join(".ssh/authorized_keys");
        // Append (preserve any pre-existing keys).
        let mut existing = std::fs::read_to_string(&authorized_keys)
            .unwrap_or_default();
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
}

impl Drop for AuthorizedKey {
    fn drop(&mut self) {
        // Remove our line and only our line. Leave anything else.
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
        // key_dir TempDir cleans up the keypair automatically.
        let _ = &self.key_dir;
    }
}

fn make_gateway(authorized: &AuthorizedKey) -> SshGateway {
    SshGateway::new(SshConfig {
        host: "127.0.0.1".into(),
        port: 22,
        user: std::env::var("USER").ok(),
        identity_file: Some(
            authorized
                .private_path
                .to_string_lossy()
                .into_owned(),
        ),
        // Use a throwaway ssh_config that disables host-key checking
        // for the test loopback host. Otherwise the test would prompt
        // or reject on first contact.
        config_file: {
            let cfg = authorized.key_dir.path().join("ssh_config");
            std::fs::write(
                &cfg,
                "Host 127.0.0.1\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n",
            )
            .unwrap();
            Some(cfg.to_string_lossy().into_owned())
        },
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t3_drop_cleans_master() {
    if !sshd_reachable() {
        eprintln!("[skip] sshd not reachable on localhost:22 — bug-(g) fix unverified");
        return;
    }
    let authorized = match AuthorizedKey::provision() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("[skip] could not provision authorized_keys: {e}");
            return;
        }
    };

    let mut gw = make_gateway(&authorized);
    gw.connect()
        .await
        .expect("connect to local sshd via temporary key");

    let pid = gw
        .master_pid()
        .await
        .expect("framework-spawned master must report a PID");
    assert!(pid_alive(pid), "master must be alive immediately after connect");

    // Drop the gateway WITHOUT calling disconnect(). Bug (g) was: the
    // master would persist after this drop because the framework had
    // discarded the std::process::Child on `setsid -f -- ssh -M -N -f`.
    // Post-fix: the retained tokio Child + kill_on_drop(true) take it
    // down within milliseconds.
    drop(gw);

    let deadline = Instant::now() + Duration::from_secs(1);
    while pid_alive(pid) {
        if Instant::now() >= deadline {
            panic!("master pid {pid} still alive 1s after gateway drop — bug (g) regression");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[tracing_test::traced_test]
async fn t4_master_died_observer_emits_log() {
    if !sshd_reachable() {
        eprintln!("[skip] sshd not reachable on localhost:22 — observer log unverified");
        return;
    }
    let authorized = match AuthorizedKey::provision() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("[skip] could not provision authorized_keys: {e}");
            return;
        }
    };

    let mut gw = make_gateway(&authorized);
    gw.connect().await.expect("connect");

    let pid = gw.master_pid().await.expect("master pid");
    // Kill the master externally with SIGKILL — simulates the
    // "master dies under us" class of failure.
    let rc = unsafe { kill(pid as i32, 9) };
    assert_eq!(rc, 0, "kill(SIGKILL) on master pid {pid} failed");

    // Watcher poll cadence is 1s; give it up to 5s to observe the
    // death and emit the log line. The macro-injected `logs_contain`
    // / `logs_assert` filter by the test-name span, but the watcher
    // task is spawned outside that span (it inherits the spawning
    // context's parent span, which here is none). We read the raw
    // `tracing-test` global buffer directly and match the message
    // verbatim — bypassing the span filter is intentional and
    // documented in the tracing-test rationale.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let raw = {
            let buf = tracing_test::internal::global_buf().lock().unwrap();
            String::from_utf8_lossy(&buf).into_owned()
        };
        if raw.contains("SSH master exited unexpectedly") {
            return; // pin satisfied
        }
        if Instant::now() >= deadline {
            panic!(
                "tracing::error! \"SSH master exited unexpectedly\" never fired \
                 within 5s of master SIGKILL — observer regression\n\
                 captured logs:\n{raw}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// `logs_contain(...)` and `logs_assert(...)` are injected into the
// test's scope by the `#[tracing_test::traced_test]` attribute
// macro — see its docs for the capture-and-search semantics.
