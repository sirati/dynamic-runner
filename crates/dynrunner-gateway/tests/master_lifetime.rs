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
//! - **T5** (`drop_kills_daemon_master`): the bug-(g)-redux
//!   regression-pin. The first attempted bug-(g) fix tracked the
//!   *launcher* PID (the `ssh -M -N` process we spawn), which exits
//!   ~120ms post-handshake under `ControlPersist=yes` regardless of
//!   anything we do. So `Child::kill()` / `kill_on_drop(true)`
//!   operated on the launcher zombie and the *daemon* — the
//!   reparented-to-init `ControlPersist` master that actually owns
//!   the control socket — survived `SshGateway::Drop`. T5 explicitly
//!   takes the daemon PID (now what `master_pid()` returns) and
//!   asserts it's gone within 1s of drop.
//! - **T-launcher-vs-daemon-pid** (`master_pid_is_daemon_not_launcher`):
//!   pin that `master_pid()` returns the daemon PID by re-deriving
//!   the daemon PID independently via a fresh `ssh -O check`
//!   subprocess and asserting both match.
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
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use dynrunner_gateway::config::SshConfig;
use dynrunner_gateway::ssh::SshGateway;
use dynrunner_gateway::traits::Gateway;
use tokio::sync::{Mutex, OwnedMutexGuard};

/// True iff something is listening on `localhost:22`. Used to skip
/// these integration tests on hosts without sshd (most CI runners).
fn sshd_reachable() -> bool {
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
/// wrote) and produces flaky `exit 255` handshake failures. Cargo's
/// default `--test-threads=N>1` would otherwise schedule them in
/// parallel.
///
/// `tokio::sync::Mutex` (not `std::sync::Mutex`) because every test
/// holds the guard across `.await` points, and the workspace lints
/// deny `clippy::await_holding_lock`. The async-aware mutex yields
/// cleanly while holding the guard. The witness type is `()` so
/// there's nothing to corrupt under contention.
async fn serialise() -> OwnedMutexGuard<()> {
    static SERIAL: OnceLock<std::sync::Arc<Mutex<()>>> = OnceLock::new();
    let m = SERIAL.get_or_init(|| std::sync::Arc::new(Mutex::new(())));
    m.clone().lock_owned().await
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
                "-t",
                "ed25519",
                "-N",
                "",
                "-q",
                "-C",
                "dynrunner-gateway-test",
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
        // Append (preserve any pre-existing keys).
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
        identity_file: Some(authorized.private_path.to_string_lossy().into_owned()),
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
    let _serial = serialise().await;
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
        .expect("framework-spawned master must report a PID");
    assert!(
        pid_alive(pid),
        "master must be alive immediately after connect"
    );

    // Drop the gateway WITHOUT calling disconnect(). Bug (g) was: the
    // master would persist after this drop because the framework had
    // discarded the std::process::Child on `setsid -f -- ssh -M -N -f`.
    // The first attempted fix (retained tokio Child + kill_on_drop)
    // tracked the *launcher* PID, which exits ~120ms post-handshake
    // anyway — so kill_on_drop hit a zombie and the daemon (the
    // actual long-lived `ControlPersist` process, reparented to
    // systemd --user) survived. Post-the-real-fix: `master_pid()`
    // returns the *daemon* PID and Drop sends SIGTERM/SIGKILL via
    // `nix::kill` directly to it.
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
    let _serial = serialise().await;
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

    let pid = gw.master_pid().expect("master pid");
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

/// T5: regression-pin for bug-(g)-redux — the first attempted fix
/// tracked the launcher PID, which exits ~120ms post-handshake on its
/// own under `ControlPersist=yes`. `kill_on_drop(true)` therefore
/// hit a zombie (no-op) and the daemon, reparented to systemd --user
/// / init, leaked. Distinct from T3 in *intent* (pin the explicit
/// daemon-vs-launcher contract) and *aggressiveness* (read the
/// daemon PID via the public `master_pid()` accessor explicitly,
/// then verify ESRCH after drop).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t5_drop_kills_daemon_master() {
    let _serial = serialise().await;
    if !sshd_reachable() {
        eprintln!("[skip] sshd not reachable on localhost:22 — bug-(g)-redux unverified");
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
    gw.connect().await.expect("connect to local sshd");

    let daemon_pid = gw
        .master_pid()
        .expect("master_pid() must report the daemon PID after connect");
    assert!(
        pid_alive(daemon_pid),
        "daemon pid {daemon_pid} must be alive immediately after connect"
    );

    // Drop the gateway WITHOUT calling disconnect(). Pre-bug-(g)-redux
    // fix: this would NOT take the daemon down because we tracked
    // the launcher PID. Post-fix: Drop sends SIGTERM (then SIGKILL
    // after 200ms grace) directly to the daemon via nix::kill.
    drop(gw);

    let deadline = Instant::now() + Duration::from_secs(1);
    while pid_alive(daemon_pid) {
        if Instant::now() >= deadline {
            panic!(
                "daemon pid {daemon_pid} still alive 1s after gateway drop — \
                 bug-(g)-redux regression: Drop is hitting the launcher \
                 zombie instead of the ControlPersist daemon"
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// T-launcher-vs-daemon-pid: assert that `master_pid()` returns the
/// daemon PID — the long-lived, reparented-to-init `ControlPersist`
/// process — by independently re-deriving it from a fresh
/// `ssh -O check` invocation. The two PIDs must agree. Pre-fix,
/// `master_pid()` returned the launcher PID (the `ssh -M -N` process
/// we spawned), which is a different PID than what `ssh -O check`
/// reports as the master.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn master_pid_is_daemon_not_launcher() {
    let _serial = serialise().await;
    if !sshd_reachable() {
        eprintln!("[skip] sshd not reachable on localhost:22 — daemon-PID contract unverified");
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

    let reported_pid = gw
        .master_pid()
        .expect("master_pid() must report the daemon PID after connect");

    // Independently parse the daemon PID from a fresh `ssh -O check`
    // — bypassing the gateway's cached value. We don't have direct
    // access to the control_path field (it's private), so reach for
    // it via a short-lived `Filesystem`-style probe: list `/tmp` for
    // `dynrunner-m-*.sock` and pick the most recent. With one
    // gateway instance per test process this is unambiguous; with
    // many it'd be racy but the test runs single-instance.
    let cp = std::fs::read_dir("/tmp")
        .expect("read /tmp")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|s| s.starts_with("dynrunner-m-") && s.ends_with(".sock"))
        })
        .max_by_key(|p| std::fs::metadata(p).and_then(|m| m.modified()).ok())
        .expect("could not locate the gateway's control socket under /tmp");

    let target = format!("{}@127.0.0.1", std::env::var("USER").expect("USER"));
    let out = StdCommand::new("ssh")
        .args([
            "-i",
            authorized.private_path.to_str().unwrap(),
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "IdentityAgent=none",
            "-F",
            authorized
                .key_dir
                .path()
                .join("ssh_config")
                .to_str()
                .unwrap(),
            "-O",
            "check",
            "-o",
            &format!("ControlPath={}", cp.display()),
            &target,
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
    let independent_pid: u32 = pid_str.parse().expect("ssh -O check pid was non-numeric");

    assert_eq!(
        reported_pid, independent_pid,
        "master_pid() ({reported_pid}) must match the daemon PID reported \
         by an independent `ssh -O check` ({independent_pid}). If they \
         differ, master_pid() is leaking the launcher PID — the very \
         class of bug we're pinning against."
    );

    // Clean up.
    gw.disconnect().await.expect("disconnect");
}

/// The owner-death babysitter end-to-end: when the process that owns
/// the master dies WITHOUT any Drop running (SIGKILL — simulated by a
/// `sleep` stand-in owner, since a test cannot SIGKILL itself), the
/// babysitter loop tears the orphaned `ControlPersist` daemon down
/// via `ssh -O exit`. This is the leak that left stale
/// `/tmp/dynrunner-m-*` masters from killed joiners accumulating on
/// the LMU gateway box.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn babysitter_reaps_master_when_owner_dies() {
    let _serial = serialise().await;
    if !sshd_reachable() {
        eprintln!("[skip] sshd not reachable on localhost:22 — babysitter unverified");
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
    let master_pid = gw.master_pid().expect("daemon pid");
    let control_path = gw.control_path().expect("control path").to_owned();

    // A disposable stand-in owner. Production watches
    // `std::process::id()` of the connecting process; the mechanism
    // is identical — only the watched PID differs.
    let mut fake_owner = StdCommand::new("sleep")
        .arg("600")
        .spawn()
        .expect("spawn fake owner");
    let config = SshConfig {
        host: "127.0.0.1".into(),
        port: 22,
        user: std::env::var("USER").ok(),
        identity_file: Some(authorized.private_path.to_string_lossy().into_owned()),
        config_file: None,
    };
    let mut babysitter =
        dynrunner_gateway::spawn_master_babysitter(fake_owner.id(), &control_path, &config)
            .expect("spawn babysitter");

    // Owner alive: the master must survive a full poll interval.
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(
        pid_alive(master_pid),
        "babysitter must not touch the master while the owner lives"
    );

    // Owner dies unreaped-by-framework (the SIGKILL shape).
    fake_owner.kill().expect("kill fake owner");
    fake_owner.wait().expect("reap fake owner");

    // Babysitter polls at 5s cadence; allow two cycles + exit cmd.
    let deadline = Instant::now() + Duration::from_secs(12);
    while pid_alive(master_pid) {
        if Instant::now() >= deadline {
            // Clean up before failing so the master doesn't leak.
            let _ = babysitter.kill();
            drop(gw);
            panic!("master pid {master_pid} still alive 12s after owner death");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = babysitter.wait();

    // The daemon is gone; the gateway object's own teardown is now a
    // no-op (terminate ladder fast-paths on ESRCH).
    drop(gw);
}

/// The babysitter must not outlive the orderly teardown paths: after
/// `disconnect()` the gateway has torn the master down itself, so the
/// owner-death watcher is killed + reaped — no sleeping `sh` parked
/// for the rest of the owner's lifetime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_reaps_babysitter() {
    let _serial = serialise().await;
    if !sshd_reachable() {
        eprintln!("[skip] sshd not reachable on localhost:22 — babysitter lifecycle unverified");
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
    let babysitter_pid = gw
        .babysitter_pid()
        .expect("owned master must have a babysitter");
    assert!(pid_alive(babysitter_pid), "babysitter alive post-connect");

    gw.disconnect().await.expect("disconnect");
    assert!(
        gw.babysitter_pid().is_none(),
        "disconnect must clear the babysitter"
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    while pid_alive(babysitter_pid) {
        if Instant::now() >= deadline {
            panic!("babysitter pid {babysitter_pid} still alive 2s after disconnect");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
