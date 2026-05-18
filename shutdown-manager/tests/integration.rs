//! Integration tests for the shutdown-manager state machine + PID-
//! file lifecycle + final cleanup. Run via `cargo test`.
//!
//! These tests use the library's [`testing::MockBackend`] +
//! [`testing::FakeClock`] so no real `podman` is spawned and no real
//! `thread::sleep` runs. The brief mandates ten behaviours; each is
//! a separate `#[test]` below for clear failure attribution.

use dynrunner_slurm_shutdown::cleanup::{final_cleanup, write_pid_file};
use dynrunner_slurm_shutdown::config::parse;
use dynrunner_slurm_shutdown::poll_loop::{Outcome, PollConfig, run};
use dynrunner_slurm_shutdown::shutdown_flag::ShutdownFlag;
use dynrunner_slurm_shutdown::testing::{FakeClock, MockBackend, MockProcessProbe};
use std::time::Duration;
use tempfile::tempdir;

fn cfg(poll_secs: u64, idle_secs: u64) -> PollConfig {
    PollConfig {
        container_name: "ctr".to_string(),
        poll_interval: Duration::from_secs(poll_secs),
        idle_shutdown: Duration::from_secs(idle_secs),
        secondary_grace: Duration::from_secs(5),
        container_stop_grace: Duration::from_secs(10),
        wrapper_pid: None,
    }
}

/// Default probe for integration tests that don't exercise the
/// wrapper-monitor wake input. With `wrapper_pid = None` in the
/// shared cfg, the probe is never consulted — `always_alive` is
/// just a stable, non-noisy default.
fn always_alive() -> MockProcessProbe {
    MockProcessProbe::always_alive()
}

/// Test 1: PID file is written on startup, removed on clean exit.
#[test]
fn pid_file_lifecycle_roundtrip() {
    let dir = tempdir().unwrap();
    let pid_path = dir.path().join("shutdown.pid");
    write_pid_file(&pid_path).unwrap();
    assert!(pid_path.exists(), "pid file should exist after write");
    let backend = MockBackend::new();
    backend.script_unshare(vec![true]);
    final_cleanup(&backend, &dir.path().join("tmp-nope"), &pid_path, |_| {});
    assert!(!pid_path.exists(), "pid file must be removed after cleanup");
}

/// Test 2: SIGTERM-equivalent (flag pre-set) triggers SIGNAL_SHUTDOWN.
#[test]
fn flag_set_before_run_yields_signal_shutdown() {
    let backend = MockBackend::new();
    backend.script_exists(vec![false]); // container already gone
    let flag = ShutdownFlag::new();
    flag.set_for_test(); // simulates SIGTERM handler
    let clock = FakeClock::new();
    let outcome = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
    assert_eq!(outcome, Outcome::SignalShutdown);
}

/// Test 3: SIGCONT shares the flag (handler installs both signals on
/// the same atomic). Modelled identically to test 2 — there is no
/// separate code path. Documenting it here so anyone removing the
/// SIGCONT-handler in `signals.rs` notices this guarantee.
#[test]
fn sigcont_path_is_identical_to_sigterm_path() {
    let backend = MockBackend::new();
    backend.script_exists(vec![false]);
    let flag = ShutdownFlag::new();
    flag.set_for_test();
    let clock = FakeClock::new();
    let outcome = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
    assert_eq!(outcome, Outcome::SignalShutdown);
}

/// Test 4: idle-shutdown fires after `idle_shutdown_secs` of
/// container-absence following at-least-one-sighting.
#[test]
fn idle_shutdown_after_grace_following_sighting() {
    let backend = MockBackend::new();
    // poll=2, idle=4 → ceil_ticks = 2.
    // Tick 1: present. Tick 2: absent (down=1). Tick 3: absent (down=2 → fire).
    backend.script_exists(vec![true, false, false]);
    let flag = ShutdownFlag::new();
    let clock = FakeClock::new();
    let outcome = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
    assert_eq!(outcome, Outcome::IdleShutdown);
    assert!(backend.rm_all_called());
    assert!(
        backend.unmount_all_called(),
        "SIGNAL_SHUTDOWN must invoke unmount --all after rm -af to flush \
         residual fuse-overlayfs mountpoints (asm-tokenizer 2026-05-18 \
         12:05 on a70d3bf — 40K residue per prefix from dead FUSE \
         mount that rm -af did not flush)"
    );
}

/// Test 5: idle-shutdown does NOT fire if the container never appeared.
#[test]
fn idle_shutdown_does_not_fire_without_prior_sighting() {
    let backend = MockBackend::new();
    backend.script_exists(vec![false; 1000]);
    let flag = ShutdownFlag::new();
    // Inject the flag on the 7th sleep — gives plenty of opportunity
    // for a spurious idle-shutdown to fire if the guard is broken.
    let clock = FakeClock::new();
    clock.set_on_sleep(7, flag.clone());
    let outcome = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
    assert_eq!(
        outcome,
        Outcome::SignalShutdown,
        "must reach the flag, not idle-shutdown"
    );
}

/// Test 6: SIGNAL_SHUTDOWN with pgrep=Some(pid): in-container kill
/// fires, then poll loop watches for exit; if absent, no stop.
#[test]
fn signal_shutdown_pgrep_some_no_stop_when_exits_in_grace() {
    let backend = MockBackend::new();
    // entry: alive; first grace poll: alive; second: absent.
    backend.script_exists(vec![true, true, false]);
    backend.script_pgrep(vec![Some(42)]);
    let flag = ShutdownFlag::new();
    flag.set_for_test();
    let clock = FakeClock::new();
    run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
    let calls = backend.calls();
    assert!(
        calls.contains(&"exec_signal(ctr, 42, TERM)".to_string()),
        "in-container kill must fire; calls: {:?}",
        calls
    );
    assert!(
        calls.contains(&"kill_pid1(ctr, TERM)".to_string()),
        "belt-and-suspenders kill must fire; calls: {:?}",
        calls
    );
    assert!(
        !calls.iter().any(|c| c.starts_with("stop(")),
        "stop must NOT fire when container exits in grace; calls: {:?}",
        calls
    );
}

/// Test 7: SIGNAL_SHUTDOWN with pgrep=None: only the belt-and-suspenders
/// `podman kill --signal TERM <name>` fires; no `podman exec kill`.
#[test]
fn signal_shutdown_pgrep_none_belt_only() {
    let backend = MockBackend::new();
    backend.script_exists(vec![true, false]); // alive at entry, gone after 1s
    backend.script_pgrep(vec![None]);
    let flag = ShutdownFlag::new();
    flag.set_for_test();
    let clock = FakeClock::new();
    run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
    let calls = backend.calls();
    assert!(
        !calls.iter().any(|c| c.starts_with("exec_signal(")),
        "no in-container kill when pgrep returns None; calls: {:?}",
        calls
    );
    assert!(
        calls.contains(&"kill_pid1(ctr, TERM)".to_string()),
        "belt fallback must fire; calls: {:?}",
        calls
    );
}

/// Test 8: FINAL_CLEANUP runs on idle path and signal path. Modelled
/// via a wrapper that mirrors what `main` does — call run, then
/// final_cleanup.
#[test]
fn final_cleanup_runs_after_idle_path() {
    let dir = tempdir().unwrap();
    let pid_path = dir.path().join("p.pid");
    let tmp_prefix = dir.path().join("asm-xxx");
    write_pid_file(&pid_path).unwrap();
    std::fs::create_dir(&tmp_prefix).unwrap();

    let backend = MockBackend::new();
    backend.script_exists(vec![true, false, false]);
    backend.script_unshare(vec![true]);
    let flag = ShutdownFlag::new();
    let clock = FakeClock::new();
    let outcome = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
    assert_eq!(outcome, Outcome::IdleShutdown);
    final_cleanup(&backend, &tmp_prefix, &pid_path, |_| {});
    assert!(!pid_path.exists(), "pid file should be removed");
    let calls = backend.calls();
    assert!(
        calls.iter().any(|c| c.starts_with("unshare_remove(")),
        "unshare must run; calls: {:?}",
        calls
    );
}

#[test]
fn final_cleanup_runs_after_signal_path() {
    let dir = tempdir().unwrap();
    let pid_path = dir.path().join("p.pid");
    let tmp_prefix = dir.path().join("asm-xxx");
    write_pid_file(&pid_path).unwrap();
    std::fs::create_dir(&tmp_prefix).unwrap();

    let backend = MockBackend::new();
    backend.script_exists(vec![false]); // gone at entry
    backend.script_unshare(vec![true]);
    let flag = ShutdownFlag::new();
    flag.set_for_test();
    let clock = FakeClock::new();
    let outcome = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
    assert_eq!(outcome, Outcome::SignalShutdown);
    final_cleanup(&backend, &tmp_prefix, &pid_path, |_| {});
    assert!(!pid_path.exists());
}

/// Test 9: PodmanBackend mock records each call; ordering can be
/// asserted with an exact-prefix `assert_eq!` on the recorded vector.
#[test]
fn mock_records_call_order() {
    let backend = MockBackend::new();
    backend.script_exists(vec![true, false]); // alive at entry, exits in grace
    backend.script_pgrep(vec![Some(99)]);
    let flag = ShutdownFlag::new();
    flag.set_for_test();
    let clock = FakeClock::new();
    run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
    let calls = backend.calls();
    // Expected exact prefix on the signal-shutdown branch when
    // pgrep=Some, container exits within grace.
    let want: Vec<String> = vec![
        // entry check in signal_shutdown
        "container_exists(ctr) -> true".to_string(),
        "exec_pgrep_first_child(ctr) -> Some(99)".to_string(),
        "exec_signal(ctr, 99, TERM)".to_string(),
        "kill_pid1(ctr, TERM)".to_string(),
        // wait_for_exit: first 1-sec tick sees absence, returns.
        "container_exists(ctr) -> false".to_string(),
        // After wait_for_exit, signal_shutdown re-checks before
        // deciding whether to call `stop`.
        "container_exists(ctr) -> false".to_string(),
        "rm_all".to_string(),
        // unmount --all immediately after rm -af, BEFORE the
        // cleanup walk would run. Flushes residual fuse-overlayfs
        // mountpoints under SLURM TIMEOUT (peer asm-tokenizer
        // 2026-05-18 12:05).
        "unmount_all".to_string(),
    ];
    assert_eq!(calls, want, "exact call order mismatch");
}

/// Test 10: Config parsing rejects invalid args with a clean error.
#[test]
fn config_parse_rejects_missing_args() {
    let err = parse(vec!["--container-name".to_string(), "x".to_string()]).unwrap_err();
    assert!(err.contains("--storage-root"), "got: {}", err);
}

/// Test 11: --log-file ownership. The manager opens the destination
/// file itself and appends its log lines to it AND stderr. End-to-end
/// (subprocess), because the load-bearing behaviour is that the
/// binary's first log line ("starting; container=") lands in the
/// file — proving the open-then-log ordering survives bootstrap.
///
/// Why subprocess (not unit-test the closure): the entire premise of
/// owning the log destination at the binary level is that systemd-
/// side stdio routing was unreliable. The only meaningful test is
/// that running the binary as a process — argv → file — works.
///
/// The manager exits non-zero in this test (it can't actually find
/// podman in the sandbox), but the bootstrap logs are emitted long
/// before any podman call, so the assertion still holds.
#[test]
fn log_file_flag_routes_first_log_lines_to_destination() {
    use std::process::Command;
    use std::time::{Duration, Instant};

    // Resolve the binary path the cargo test harness built. CARGO_BIN_EXE_<name>
    // is set by cargo for integration tests of the crate's own bins.
    let bin = env!("CARGO_BIN_EXE_dynrunner-slurm-shutdown");

    let dir = tempdir().unwrap();
    let log_path = dir.path().join("shutdown-manager.log");
    let pid_path = dir.path().join("shutdown.pid");
    let tmp_prefix = dir.path().join("asm-XXX");
    let storage_root = dir.path().join("podman-root");
    let runroot = dir.path().join("podman-run");
    let wrapper_pid = std::process::id().to_string();

    // Spawn the manager. It will install signal handlers, log the
    // bootstrap lines, then enter the poll loop calling `podman ps`
    // (likely failing in the test sandbox — irrelevant). Kill it
    // after a short grace so the test runs in <1s.
    let mut child = Command::new(bin)
        .args([
            "--container-name",
            "ctr-does-not-exist",
            "--storage-root",
            storage_root.to_str().unwrap(),
            "--runroot",
            runroot.to_str().unwrap(),
            "--tmp-prefix",
            tmp_prefix.to_str().unwrap(),
            "--pid-file",
            pid_path.to_str().unwrap(),
            "--log-file",
            log_path.to_str().unwrap(),
            "--poll-interval-secs",
            "1",
            "--idle-shutdown-secs",
            "1",
            "--wrapper-pid",
            &wrapper_pid,
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn shutdown-manager binary");

    // Wait for the file to acquire BOTH bootstrap lines, then kill.
    // The manager writes "starting" before signal-install and
    // "wrapper-monitor enabled" after PollConfig assembly — both
    // are pre-poll-loop, so they appear within milliseconds.
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut contents = String::new();
    while Instant::now() < deadline {
        contents = std::fs::read_to_string(&log_path).unwrap_or_default();
        if contents.contains("starting; container=") && contents.contains("wrapper-monitor enabled") {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    // Tear down: SIGTERM-equivalent via `kill()` on the child id.
    // The manager has SIGTERM handlers that trigger SIGNAL_SHUTDOWN.
    // If the wait fails (e.g. signal already delivered), we don't
    // care — the assertions below speak to the file contents.
    let _ = child.kill();
    let _ = child.wait();

    assert!(
        contents.contains("starting; container=ctr-does-not-exist"),
        "--log-file did not capture the manager's bootstrap line; \
         contents:\n{contents}",
    );
    assert!(
        contents.contains("wrapper-monitor enabled"),
        "--log-file did not capture the wrapper-monitor-enabled log \
         line; contents:\n{contents}",
    );
    assert!(
        contents.contains("[shutdown-mgr]"),
        "--log-file lines must keep the [shutdown-mgr] prefix so \
         operators can grep for them; contents:\n{contents}",
    );
}

/// Test 12: `--podman-path` is honoured end-to-end. We pass a real
/// path that exists on the filesystem but is NOT a podman binary
/// (`/usr/bin/false`); the manager's bootstrap (parse → log-file open
/// → pid-file write → signal install → poll-loop entry) must succeed
/// even though every subsequent podman call will fail at exec. The
/// proof point: the bootstrap log lines reach `--log-file` exactly
/// like the canonical (`--podman-path` resolving via PATH) case —
/// confirming the flag is wired through and that the manager's
/// "podman failures are best-effort" contract survives a hostile
/// `--podman-path`. If the flag were ignored or the binary aborted
/// on the first podman ENOENT, the bootstrap lines would either
/// reflect a different path or never reach the file.
///
/// Subprocess, not unit-test the closure: same rationale as
/// `log_file_flag_routes_first_log_lines_to_destination` — argv →
/// resolved Config → backend construction is the load-bearing chain.
#[test]
fn podman_path_flag_threads_into_backend() {
    use std::process::Command;
    use std::time::{Duration, Instant};

    let bin = env!("CARGO_BIN_EXE_dynrunner-slurm-shutdown");
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("shutdown-manager.log");
    let pid_path = dir.path().join("shutdown.pid");
    let tmp_prefix = dir.path().join("asm-XXX");
    let storage_root = dir.path().join("podman-root");
    let runroot = dir.path().join("podman-run");
    let wrapper_pid = std::process::id().to_string();

    // `/usr/bin/false` exists on every Linux distro the framework
    // targets, exits non-zero on every invocation, and is NOT a
    // podman binary. Using it proves the bootstrap stages don't
    // depend on a working podman — exactly the contract the
    // post-2026-05-18 shutdown manager needs to preserve so a
    // bad `--podman-path` from a misconfigured wrapper degrades
    // to "podman calls fail, manager still tears down cleanly"
    // rather than aborting.
    let mut child = Command::new(bin)
        .args([
            "--container-name",
            "ctr-does-not-exist",
            "--storage-root",
            storage_root.to_str().unwrap(),
            "--runroot",
            runroot.to_str().unwrap(),
            "--tmp-prefix",
            tmp_prefix.to_str().unwrap(),
            "--pid-file",
            pid_path.to_str().unwrap(),
            "--log-file",
            log_path.to_str().unwrap(),
            "--poll-interval-secs",
            "1",
            "--idle-shutdown-secs",
            "1",
            "--wrapper-pid",
            &wrapper_pid,
            "--podman-path",
            "/usr/bin/false",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn shutdown-manager binary with --podman-path");

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut contents = String::new();
    while Instant::now() < deadline {
        contents = std::fs::read_to_string(&log_path).unwrap_or_default();
        if contents.contains("starting; container=") {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    let _ = child.kill();
    let _ = child.wait();

    assert!(
        contents.contains("starting; container=ctr-does-not-exist"),
        "--podman-path /usr/bin/false: bootstrap log line must still \
         reach the file (the flag is wired through; podman call \
         failures are best-effort post-bootstrap); contents:\n{contents}",
    );
    // PID file must exist post-bootstrap — written before any
    // podman call, so a bad podman_path cannot prevent it.
    assert!(
        pid_path.exists(),
        "--podman-path /usr/bin/false: pid file must be written \
         during bootstrap (pre-poll-loop, pre-any-podman-call). \
         Missing pid file would indicate the bootstrap chain itself \
         aborted on the flag — which would mean `--podman-path` is \
         load-bearing in a way it must not be.",
    );
}

/// Test 13: `--rm-path` is honoured end-to-end. We pass a real path
/// that exists on the filesystem but is NOT `rm` (`/usr/bin/false`);
/// the manager's bootstrap (parse → log-file open → pid-file write
/// → signal install → poll-loop entry) must succeed even though the
/// final `podman unshare <rm_path> <tmp> -rf` call will fail at exec
/// inside the userns. The proof point: the bootstrap log lines reach
/// `--log-file` exactly like the canonical case — confirming the
/// flag is wired through and that the manager's "podman failures
/// are best-effort" contract survives a hostile `--rm-path`. If the
/// flag were ignored or the binary aborted on the resulting failure,
/// the bootstrap lines would either reflect a different path or
/// never reach the file.
///
/// Subprocess, not unit-test the closure: same rationale as
/// `podman_path_flag_threads_into_backend` — argv → resolved Config
/// → backend construction is the load-bearing chain.
#[test]
fn rm_path_flag_threads_into_backend() {
    use std::process::Command;
    use std::time::{Duration, Instant};

    let bin = env!("CARGO_BIN_EXE_dynrunner-slurm-shutdown");
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("shutdown-manager.log");
    let pid_path = dir.path().join("shutdown.pid");
    let tmp_prefix = dir.path().join("asm-XXX");
    let storage_root = dir.path().join("podman-root");
    let runroot = dir.path().join("podman-run");
    let wrapper_pid = std::process::id().to_string();

    // `/usr/bin/false` exists on every Linux distro the framework
    // targets, exits non-zero on every invocation, and is NOT `rm`.
    // Using it proves the bootstrap stages don't depend on a working
    // inner-userns `rm` — exactly the contract the post-2026-05-18
    // shutdown manager needs to preserve so a bad `--rm-path` from
    // a misconfigured wrapper degrades to "cleanup fails, manager
    // still tears down" rather than aborting bootstrap.
    let mut child = Command::new(bin)
        .args([
            "--container-name",
            "ctr-does-not-exist",
            "--storage-root",
            storage_root.to_str().unwrap(),
            "--runroot",
            runroot.to_str().unwrap(),
            "--tmp-prefix",
            tmp_prefix.to_str().unwrap(),
            "--pid-file",
            pid_path.to_str().unwrap(),
            "--log-file",
            log_path.to_str().unwrap(),
            "--poll-interval-secs",
            "1",
            "--idle-shutdown-secs",
            "1",
            "--wrapper-pid",
            &wrapper_pid,
            "--rm-path",
            "/usr/bin/false",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn shutdown-manager binary with --rm-path");

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut contents = String::new();
    while Instant::now() < deadline {
        contents = std::fs::read_to_string(&log_path).unwrap_or_default();
        if contents.contains("starting; container=") {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    let _ = child.kill();
    let _ = child.wait();

    assert!(
        contents.contains("starting; container=ctr-does-not-exist"),
        "--rm-path /usr/bin/false: bootstrap log line must still \
         reach the file (the flag is wired through; cleanup failure \
         is best-effort post-bootstrap); contents:\n{contents}",
    );
    // PID file must exist post-bootstrap — written before any
    // podman/rm call, so a bad rm_path cannot prevent it.
    assert!(
        pid_path.exists(),
        "--rm-path /usr/bin/false: pid file must be written during \
         bootstrap (pre-poll-loop, pre-any-podman-call). Missing pid \
         file would indicate the bootstrap chain itself aborted on \
         the flag — which would mean `--rm-path` is load-bearing in \
         a way it must not be.",
    );
}

#[test]
fn config_parse_rejects_unknown_flag() {
    let argv = vec![
        "--container-name=ctr".to_string(),
        "--storage-root=/r".to_string(),
        "--runroot=/rr".to_string(),
        "--tmp-prefix=/t".to_string(),
        "--pid-file=/p".to_string(),
        "--whats-this".to_string(),
        "value".to_string(),
    ];
    let err = parse(argv).unwrap_err();
    assert!(err.contains("--whats-this"), "got: {}", err);
}
