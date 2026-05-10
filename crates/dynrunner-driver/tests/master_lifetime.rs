//! Integration tests for [`dynrunner_driver::SshMaster`] that need a
//! live sshd. Carry-over of the gateway's `master_lifetime.rs` test
//! suite, re-targeted at the new `SshMaster` API + extended for the
//! locked-design points that only the driver crate tests:
//!
//! - **T3** (`drop_cleans_master`): pinning bug-(g) — dropping a
//!   spawn-master without disconnect must take the daemon down.
//! - **T4** (`master_died_observer_emits_log`): the watcher fires
//!   `tracing::error!("SSH master exited unexpectedly")` on
//!   external master death.
//! - **T5** (`drop_kills_daemon_master`): bug-(g)-redux. Read the
//!   daemon PID via `master_pid()`, drop the master, assert ESRCH.
//! - **T-pid-is-daemon** (`master_pid_is_daemon_not_launcher`):
//!   independently re-derive the daemon PID via a fresh
//!   `ssh -O check` and assert agreement.
//! - **T-invalidation** (`invalidation_semantics`): kill the daemon
//!   externally, assert master_pid() returns last_known_pid (Some),
//!   disconnect() returns Ok(()), Drop is a no-op. Locked points
//!   (h.1), (h.2), (h.3).
//! - **T-adopt-disconnect** (`adopt_disconnect_partial_cleanup`):
//!   spawn from one handle, adopt() from a second, register a
//!   forward via the second handle, disconnect the second, assert
//!   the master is STILL alive (adopt-disconnect is partial cleanup,
//!   not termination — locked point (b)).
//! - **T-panic-in-drop** (`drop_does_not_panic_on_unkillable_master`):
//!   inject a fake kill-ladder via the cfg(test) hook so Drop sees
//!   UnkillableMaster, run the gateway under `catch_unwind`, assert
//!   no panic propagates. Locked point (j).
//!
//! These tests need an actual sshd they can authenticate against.
//! They:
//!   1. Probe TCP `localhost:22` first; skip with a clear message
//!      when sshd isn't running (most CI containers).
//!   2. Generate a temporary ed25519 keypair, append the pubkey to
//!      `~/.ssh/authorized_keys`, run the master against
//!      `localhost`, and clean the pubkey out at test end.
//!
//! Run on a host with sshd:
//!   `cargo test -p dynrunner-driver --test master_lifetime`

use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use dynrunner_driver::config::SshConfig;
use dynrunner_driver::ssh_master::SshMaster;
use dynrunner_driver::ssh_target::SshTarget;
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
/// wrote) and produces flaky `exit 255` handshake failures.
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
/// `ESRCH` once the process has been reaped.
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

    fn ssh_config_path(&self) -> PathBuf {
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

fn make_config(authorized: &AuthorizedKey) -> SshConfig {
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

    let master = SshMaster::spawn(make_config(&authorized))
        .expect("spawn against local sshd via temporary key");

    let pid = master
        .master_pid()
        .expect("framework-spawned master must report a PID");
    assert!(pid_alive(pid), "master must be alive immediately after spawn");

    drop(master);

    let deadline = Instant::now() + Duration::from_secs(1);
    while pid_alive(pid) {
        if Instant::now() >= deadline {
            panic!("master pid {pid} still alive 1s after SshMaster drop — bug (g) regression");
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

    let master = SshMaster::spawn(make_config(&authorized)).expect("spawn");
    let pid = master.master_pid().expect("master pid");
    let rc = unsafe { kill(pid as i32, 9) };
    assert_eq!(rc, 0, "kill(SIGKILL) on master pid {pid} failed");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let raw = {
            let buf = tracing_test::internal::global_buf().lock().unwrap();
            String::from_utf8_lossy(&buf).into_owned()
        };
        if raw.contains("SSH master exited unexpectedly") {
            // Cleanup: master is already dead, but the watcher may
            // still be in its final tick. Drop synchronously runs
            // the no-op-on-invalidation path now.
            drop(master);
            return;
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

    let master = SshMaster::spawn(make_config(&authorized)).expect("spawn against local sshd");
    let daemon_pid = master
        .master_pid()
        .expect("master_pid() must report the daemon PID after spawn");
    assert!(
        pid_alive(daemon_pid),
        "daemon pid {daemon_pid} must be alive immediately after spawn"
    );

    drop(master);

    let deadline = Instant::now() + Duration::from_secs(1);
    while pid_alive(daemon_pid) {
        if Instant::now() >= deadline {
            panic!(
                "daemon pid {daemon_pid} still alive 1s after master drop — \
                 bug-(g)-redux regression: Drop is hitting the launcher \
                 zombie instead of the ControlPersist daemon"
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Pin that `master_pid()` returns the daemon PID — the long-lived
/// `ControlPersist` process — by independently re-deriving it from a
/// fresh `ssh -O check` invocation. The two PIDs must agree.
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

    let mut master = SshMaster::spawn(make_config(&authorized)).expect("spawn");
    let reported_pid = master
        .master_pid()
        .expect("master_pid() must report the daemon PID after spawn");

    let cp = master.control_path().to_path_buf();
    let target_str = master.target().as_str().to_owned();

    let independent_pid = re_probe_master_pid(&authorized, &cp, &target_str);

    assert_eq!(
        reported_pid, independent_pid,
        "master_pid() ({reported_pid}) must match the daemon PID reported \
         by an independent `ssh -O check` ({independent_pid})."
    );

    master.disconnect().expect("disconnect");
}

/// Locked design points (h.1), (h.2), (h.3): after watcher-observed
/// invalidation, `master_pid()` returns Some(last_known_pid),
/// `disconnect()` returns Ok(()), and Drop is a no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalidation_semantics() {
    let _serial = serialise().await;
    if !sshd_reachable() {
        eprintln!("[skip] sshd not reachable on localhost:22 — invalidation contract unverified");
        return;
    }
    let authorized = match AuthorizedKey::provision() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("[skip] could not provision authorized_keys: {e}");
            return;
        }
    };

    let mut master = SshMaster::spawn(make_config(&authorized)).expect("spawn");
    let pid = master.master_pid().expect("pid");
    assert!(!master.is_invalidated());

    // Kill the daemon externally — simulates the master dying under
    // us. The watcher polls at 1s cadence so we wait up to 3s for
    // it to set the invalidated flag.
    assert_eq!(unsafe { kill(pid as i32, 9) }, 0, "external SIGKILL");
    let deadline = Instant::now() + Duration::from_secs(3);
    while !master.is_invalidated() {
        if Instant::now() >= deadline {
            panic!("watcher did not set invalidated flag within 3s of external master death");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // (h.1): master_pid() returns last_known_pid (Some), not None.
    assert_eq!(
        master.master_pid(),
        Some(pid),
        "post-invalidation master_pid() must return last_known_pid (Some), not None — \
         the semantic is 'was alive, not is alive'"
    );

    // (h.2): disconnect() post-invalidation succeeds as no-op.
    master
        .disconnect()
        .expect("post-invalidation disconnect must succeed as no-op");

    // (h.3): Drop post-invalidation is a no-op. We can't directly
    // observe "Drop did nothing" from outside, but the absence of a
    // panic / hang / kill-of-our-own-pid is the contract. Drop
    // happens at end of scope.
    drop(master);
}

/// Locked design point (b): adopt-master `disconnect()` runs
/// `ssh -O cancel -R` per forwarded_ports entry. PARTIAL cleanup —
/// the master is still alive after disconnect.
///
/// Topology:
///   1. Spawn master via handle A.
///   2. Adopt the same control socket via handle B.
///   3. Register a runtime forward via B.add_forward(...).
///   4. B.disconnect(): forward must be released AND master (handle
///      A's daemon PID) must still be alive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adopt_disconnect_partial_cleanup() {
    let _serial = serialise().await;
    if !sshd_reachable() {
        eprintln!("[skip] sshd not reachable on localhost:22 — adopt-disconnect contract unverified");
        return;
    }
    let authorized = match AuthorizedKey::provision() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("[skip] could not provision authorized_keys: {e}");
            return;
        }
    };

    let master_a = SshMaster::spawn(make_config(&authorized)).expect("spawn A");
    let pid_a = master_a.master_pid().expect("pid A");
    assert!(pid_alive(pid_a));

    let cp = master_a.control_path().to_path_buf();
    let target_b = master_a.target().clone();

    // adopt() with minimal flags — locked point (k): the master
    // responds via the unix socket regardless of identity / config
    // flags. We pass the same target the master was spawned with
    // for telemetry consistency.
    let mut master_b = SshMaster::adopt(cp.clone(), target_b).expect("adopt B");
    assert!(!master_b.is_spawned(), "B is adopt-master");
    assert!(master_a.is_spawned(), "A is spawn-master");

    // Pick two free localhost ports for the forward. We don't
    // actually need the forward to *work* end-to-end; we just need
    // it registered against the master so disconnect()'s
    // `ssh -O cancel -R` has something to release.
    let local_port = pick_free_port();
    let remote_port = pick_free_port();

    master_b
        .add_forward(local_port, remote_port)
        .expect("register runtime forward via adopt-handle");

    // disconnect() exercises the per-forward `ssh -O cancel -R`
    // cleanup path.
    master_b
        .disconnect()
        .expect("adopt-disconnect runs ssh -O cancel per forward");

    // Master A's daemon must still be alive — adopt-disconnect
    // is partial cleanup, not termination.
    assert!(
        pid_alive(pid_a),
        "spawn-master daemon pid {pid_a} must still be alive after \
         adopt-disconnect of a sibling handle (locked point (b): \
         partial cleanup, not termination)"
    );

    // Cleanup: tear down master A explicitly. (Drop would also do
    // it, but explicit disconnect makes the test deterministic.)
    drop(master_a);
    let deadline = Instant::now() + Duration::from_secs(2);
    while pid_alive(pid_a) {
        if Instant::now() >= deadline {
            panic!("master A daemon {pid_a} did not die within 2s of A drop");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Locked design point (j): panic-in-Drop PROHIBITION. Inject a
/// fake kill-ladder via the `cfg(test)` hook so Drop sees an
/// `UnkillableMaster` outcome, run the master-drop scope under
/// `catch_unwind`, and assert no panic propagates. Drop must
/// `tracing::error!` and return.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_does_not_panic_on_unkillable_master() {
    let _serial = serialise().await;
    if !sshd_reachable() {
        eprintln!("[skip] sshd not reachable on localhost:22 — panic-in-Drop pin unverified");
        return;
    }
    let authorized = match AuthorizedKey::provision() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("[skip] could not provision authorized_keys: {e}");
            return;
        }
    };

    // Spawn for real, then install the test-only kill hook before
    // dropping. The hook returns an UnkillableMaster outcome —
    // simulating "even SIGKILL didn't take" without actually
    // sending signals.
    let mut master = SshMaster::spawn(make_config(&authorized)).expect("spawn");
    let pid = master.master_pid().expect("pid");

    use dynrunner_driver::error::{KillLadder, SshMasterError};
    let target_clone = master.target().clone();
    master.install_test_kill_hook(move |hook_pid: u32, _: &SshTarget| {
        Err(SshMasterError::UnkillableMaster {
            target: target_clone.clone(),
            last_known_pid: hook_pid,
            kill_ladder_reached: KillLadder::SigkillButPidStillExists,
        })
    });

    // Run the drop under `catch_unwind`. Drop MUST NOT panic.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        drop(master);
    }));
    assert!(
        result.is_ok(),
        "Drop panicked on UnkillableMaster — locked point (j) \
         prohibits panic-in-Drop. The fake hook simulated an \
         unkillable master; Drop must log via tracing::error! and \
         return, never panic."
    );

    // Real cleanup: the production daemon is still alive (the test
    // hook bypassed actually killing it). Send SIGKILL ourselves.
    let _ = unsafe { kill(pid as i32, 9) };
    let deadline = Instant::now() + Duration::from_secs(2);
    while pid_alive(pid) {
        if Instant::now() >= deadline {
            // Best-effort cleanup; don't fail the test on residue.
            eprintln!("[warn] failed to clean up spawned daemon pid {pid}");
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ---------- helpers ----------

/// Independent PID re-derivation via a fresh `ssh -O check`
/// invocation, used by `master_pid_is_daemon_not_launcher` to
/// cross-check the value the master reports.
fn re_probe_master_pid(authorized: &AuthorizedKey, cp: &Path, target: &str) -> u32 {
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
fn pick_free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    l.local_addr().expect("local_addr").port()
}
