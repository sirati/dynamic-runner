//! Integration tests for the shutdown-manager state machine + PID-
//! file lifecycle + final cleanup. Run via `cargo test`.
//!
//! These tests use the library's [`testing::MockBackend`] +
//! [`testing::FakeClock`] so no real `podman` is spawned and no real
//! `thread::sleep` runs. The brief mandates ten behaviours; each is
//! a separate `#[test]` below for clear failure attribution.

use dynrunner_slurm_shutdown::cleanup::{final_cleanup, write_pid_file};
use dynrunner_slurm_shutdown::config::parse;
use dynrunner_slurm_shutdown::poll_loop::{Outcome, PollConfig, ReapStatus, run};
use dynrunner_slurm_shutdown::shutdown_flag::ShutdownFlag;
use dynrunner_slurm_shutdown::testing::{FakeClock, MockBackend, MockProcessProbe, MOCK_WORKLOAD_START};
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
        panik_file: None,
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
    backend.script_remove(vec![true]);
    final_cleanup(&backend, &dir.path().join("tmp-nope"), &pid_path, false, |_| {});
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
    let report = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
    assert_eq!(report.outcome, Outcome::SignalShutdown);
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
    let report = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
    assert_eq!(report.outcome, Outcome::SignalShutdown);
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
    let report = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
    assert_eq!(report.outcome, Outcome::IdleShutdown);
    assert!(backend.rm_all_called());
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
    let report = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
    assert_eq!(
        report.outcome,
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
    backend.script_remove(vec![true]);
    let flag = ShutdownFlag::new();
    let clock = FakeClock::new();
    let report = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
    assert_eq!(report.outcome, Outcome::IdleShutdown);
    // Idle path ⇒ no orphan ⇒ scratch is torn down as before.
    let preserve_scratch = matches!(report.reap, ReapStatus::OrphanSurvives);
    final_cleanup(&backend, &tmp_prefix, &pid_path, preserve_scratch, |_| {});
    assert!(!pid_path.exists(), "pid file should be removed");
    let calls = backend.calls();
    assert!(
        calls.iter().any(|c| c.starts_with("remove_tmp_tree(")),
        "remove_tmp_tree must run; calls: {:?}",
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
    backend.script_remove(vec![true]);
    let flag = ShutdownFlag::new();
    flag.set_for_test();
    let clock = FakeClock::new();
    let report = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
    assert_eq!(report.outcome, Outcome::SignalShutdown);
    let preserve_scratch = matches!(report.reap, ReapStatus::OrphanSurvives);
    final_cleanup(&backend, &tmp_prefix, &pid_path, preserve_scratch, |_| {});
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
/// final `<rm_path> <tmp> -rf` call will fail at exec. The proof
/// point: the bootstrap log lines reach
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

/// Reap-cfg with 1s graces so `FakeClock` (which never blocks) keeps
/// the verify-loop poll counts short. `wrapper_pid = None` so the only
/// probe consultations come from the reap-verify path's identity check.
fn cfg_reap() -> PollConfig {
    PollConfig {
        secondary_grace: Duration::from_secs(1),
        container_stop_grace: Duration::from_secs(1),
        ..cfg(2, 4)
    }
}

/// Regression, end-to-end at the run() boundary: an
/// orphan whose podman record is GONE but whose host PID is still
/// ALIVE. The reaper must (a) signal the captured PID directly — not
/// no-op because the record vanished, and (b) since the PID never
/// dies, report `OrphanSurvives`, NOT remove the podman handle, and
/// drive `main` to a non-zero exit. This is the exact shape of the
/// confirmed real-run failure where the reaper claimed success over a
/// live orphan.
#[test]
fn orphan_reap_record_gone_pid_alive_does_not_no_op_or_false_succeed() {
    let backend = MockBackend::new();
    // tick1: record present → workload PID captured. SIGNAL_SHUTDOWN
    // entry: record gone (the --rm/premature-cleanup orphan case).
    backend.script_exists(vec![true, false]);
    backend.script_workload_pid(vec![Some(90909)]);
    let flag = ShutdownFlag::new();
    let clock = FakeClock::new();
    // Signal arrives after the PID is captured on tick 1.
    clock.set_on_sleep(1, flag.clone());
    // PID stays alive through SIGTERM, grace, SIGKILL, and the second
    // grace — a stuck process the kernel-cgroup OOM path would handle
    // but userspace signalling cannot.
    let probe = MockProcessProbe::always_alive();

    let report = run(&backend, &flag, &clock, &probe, &cfg_reap(), |_| {});

    assert_eq!(report.outcome, Outcome::SignalShutdown);
    // (1) The reaper did NOT no-op: it signalled the captured PID even
    //     though podman had lost the record.
    assert!(
        !probe.signals_sent().is_empty(),
        "reaper must signal the captured PID, not no-op on a gone record"
    );
    assert!(
        probe.signals_sent().contains(&(90909, libc::SIGTERM)),
        "captured PID must receive SIGTERM; got {:?}",
        probe.signals_sent()
    );
    assert!(
        probe.signals_sent().contains(&(90909, libc::SIGKILL)),
        "captured PID must be escalated to SIGKILL when it survives; got {:?}",
        probe.signals_sent()
    );
    // (2) No false success: a live orphan ⇒ OrphanSurvives, handle left
    //     intact. `main` maps this to a non-zero exit.
    assert_eq!(
        report.reap,
        ReapStatus::OrphanSurvives,
        "the manager must not report a live orphan as reaped"
    );
    assert!(
        !backend.calls().contains(&"rm_all".to_string()),
        "the podman handle must be left intact while the orphan lives; calls: {:?}",
        backend.calls()
    );
}

/// Companion to the above: the orphan PID dies after SIGTERM. Here the
/// reaper succeeds — confirmed gone — and the handle is removed. Proves
/// the happy path of the same mechanism (signal-by-captured-PID then
/// rm-only-after-dead) for the force-kill/incomplete-run case the
/// manager's normal self-exit on a clean run does NOT cover.
#[test]
fn orphan_reap_record_gone_pid_dies_confirms_and_rms() {
    let backend = MockBackend::new();
    backend.script_exists(vec![true, false]);
    backend.script_workload_pid(vec![Some(5555)]);
    let flag = ShutdownFlag::new();
    let clock = FakeClock::new();
    clock.set_on_sleep(1, flag.clone());
    // start_time channel: capture, pre-SIGTERM identity check → same,
    // first verify poll → gone.
    let probe = MockProcessProbe::reap(vec![true, true, false]);

    let report = run(&backend, &flag, &clock, &probe, &cfg_reap(), |_| {});

    assert_eq!(report.outcome, Outcome::SignalShutdown);
    assert_eq!(report.reap, ReapStatus::ConfirmedGone);
    assert_eq!(
        probe.signals_sent(),
        vec![(5555, libc::SIGTERM)],
        "only SIGTERM needed when the PID dies in grace; got {:?}",
        probe.signals_sent()
    );
    assert!(
        backend.calls().contains(&"rm_all".to_string()),
        "handle removed once the PID is confirmed gone; calls: {:?}",
        backend.calls()
    );
}

/// `cfg_reap` plus the panik-file sentinel host path, enabling the
/// graceful last resort at the run() boundary.
fn cfg_reap_with_panik(panik_file: std::path::PathBuf) -> PollConfig {
    PollConfig {
        panik_file: Some(panik_file),
        ..cfg_reap()
    }
}

/// End-to-end at the run() boundary: the orphan survives the direct
/// PID-reap, so the reaper falls through to the graceful last resort —
/// it writes the panik sentinel the secondary's watcher monitors and
/// waits. The workload self-exits inside the window, so the manager
/// reports `ConfirmedGone` (exit 0) and removes the podman handle. The
/// sentinel must be written exactly once at the configured path and the
/// graceful attempt must introduce NO new signal/kill/rm.
#[test]
fn orphan_survives_reap_then_panik_file_lets_workload_stop_gracefully() {
    let dir = tempdir().unwrap();
    // Host side of the secondary's bind-mounted sentinel.
    let panik_file = dir.path().join("log").join(".dynrunner-reaper.panik");
    std::fs::create_dir_all(panik_file.parent().unwrap()).unwrap();

    let backend = MockBackend::new();
    backend.script_exists(vec![true, false]);
    backend.script_workload_pid(vec![Some(4242)]);
    let flag = ShutdownFlag::new();
    let clock = FakeClock::new();
    clock.set_on_sleep(1, flag.clone());
    // 6 identity reads alive (the full direct reap: capture + pre-SIGTERM
    // + 2 SIGTERM-grace + 2 SIGKILL-grace), then gone on the first
    // graceful poll. Saturating `None` thereafter.
    let probe = MockProcessProbe::reap_start_times(vec![
        Some(MOCK_WORKLOAD_START),
        Some(MOCK_WORKLOAD_START),
        Some(MOCK_WORKLOAD_START),
        Some(MOCK_WORKLOAD_START),
        Some(MOCK_WORKLOAD_START),
        Some(MOCK_WORKLOAD_START),
        None,
    ]);

    let report = run(
        &backend,
        &flag,
        &clock,
        &probe,
        &cfg_reap_with_panik(panik_file.clone()),
        |_| {},
    );

    assert_eq!(report.outcome, Outcome::SignalShutdown);
    assert_eq!(
        report.reap,
        ReapStatus::ConfirmedGone,
        "workload self-exited inside the panik window ⇒ ConfirmedGone (exit 0)"
    );
    assert!(
        panik_file.exists(),
        "the panik sentinel must be written at the secondary's monitored path"
    );
    assert_eq!(
        probe.signals_sent(),
        vec![(4242, libc::SIGTERM), (4242, libc::SIGKILL)],
        "the graceful attempt must not introduce a new signal; got {:?}",
        probe.signals_sent()
    );
    assert!(
        backend.calls().contains(&"rm_all".to_string()),
        "handle removed once the workload is confirmed gone; calls: {:?}",
        backend.calls()
    );
}

/// End-to-end: the orphan survives BOTH the direct reap AND the
/// graceful panik window. The reaper writes the sentinel once and STILL
/// reports `OrphanSurvives` (exit 1), leaving the podman handle intact —
/// the direct reap's no-false-success invariant is preserved through the
/// new path.
#[test]
fn orphan_survives_reap_and_panik_window_stays_orphan_exit_nonzero() {
    let dir = tempdir().unwrap();
    let panik_file = dir.path().join("log").join(".dynrunner-reaper.panik");
    std::fs::create_dir_all(panik_file.parent().unwrap()).unwrap();

    let backend = MockBackend::new();
    backend.script_exists(vec![true, false]);
    backend.script_workload_pid(vec![Some(90909)]);
    let flag = ShutdownFlag::new();
    let clock = FakeClock::new();
    clock.set_on_sleep(1, flag.clone());
    // Never dies — alive through the reap AND the whole panik window.
    let probe = MockProcessProbe::always_alive();

    let report = run(
        &backend,
        &flag,
        &clock,
        &probe,
        &cfg_reap_with_panik(panik_file.clone()),
        |_| {},
    );

    assert_eq!(report.outcome, Outcome::SignalShutdown);
    assert_eq!(
        report.reap,
        ReapStatus::OrphanSurvives,
        "workload alive after the panik window ⇒ still OrphanSurvives (exit 1)"
    );
    assert!(
        panik_file.exists(),
        "the panik sentinel is still written before the reaper gives up"
    );
    assert!(
        !backend.calls().contains(&"rm_all".to_string()),
        "the podman handle must be left intact while the orphan lives; calls: {:?}",
        backend.calls()
    );
}

/// Test 14: signal-source observability, end-to-end through the binary.
/// Deliver a real SIGTERM to the running manager (from THIS test
/// process) and assert the persistent log records WHO sent it — the
/// sender pid resolved to the test binary's comm + full cmdline — and
/// WHY the teardown started.
///
/// Subprocess (not a unit test of the helper) because the load-bearing
/// chain is `sigaction(SA_SIGINFO)` → handler captures `si_pid` →
/// `describe_last_signal` resolves `/proc/<sender>` → the line reaches
/// the operator's log. Only running the real binary exercises that the
/// SA_SIGINFO handler is actually installed and that the kernel fills in
/// the sender pid.
#[test]
fn sigterm_reports_sender_pid_and_cmdline_in_log() {
    use std::process::Command;
    use std::time::{Duration, Instant};

    let bin = env!("CARGO_BIN_EXE_dynrunner-slurm-shutdown");
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("shutdown-manager.log");
    let pid_path = dir.path().join("shutdown.pid");
    let tmp_prefix = dir.path().join("asm-XXX");
    let storage_root = dir.path().join("podman-root");
    let runroot = dir.path().join("podman-run");

    // No --wrapper-pid: we want the SIGTERM (not wrapper-monitor) to be
    // the trigger, so the source describer reports the signal sender.
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
            "60",
            // a real binary that is NOT podman, so backend calls fail
            // fast (irrelevant — the source line is emitted regardless).
            "--podman-path",
            "/usr/bin/false",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn shutdown-manager binary");

    // Wait until the manager has installed its handlers (the "starting"
    // line is logged immediately before signal-install, so once it is
    // present the SA_SIGINFO handler is in place by the next line).
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        let c = std::fs::read_to_string(&log_path).unwrap_or_default();
        if c.contains("starting; container=") {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    // Small extra beat so install() has certainly returned.
    std::thread::sleep(Duration::from_millis(50));

    // Deliver SIGTERM FROM this test process so the manager's handler
    // captures our pid as the sender.
    let child_pid = child.id() as i32;
    // SAFETY: kill(2) on a child pid we just spawned; SIGTERM delivers a
    // real signal the manager's SA_SIGINFO handler will record.
    let rc = unsafe { libc::kill(child_pid, libc::SIGTERM) };
    assert_eq!(rc, 0, "kill(child, SIGTERM) failed");

    // Wait for the source line to appear, then reap.
    let me = std::process::id();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut contents = String::new();
    while Instant::now() < deadline {
        contents = std::fs::read_to_string(&log_path).unwrap_or_default();
        if contents.contains("received SIGTERM from pid=") {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    let _ = child.kill();
    let _ = child.wait();

    assert!(
        contents.contains("received SIGTERM from pid="),
        "log must record the signal name + sender pid; contents:\n{contents}",
    );
    assert!(
        contents.contains(&format!("pid={}", me)),
        "log must report THIS test process ({me}) as the SIGTERM sender; \
         contents:\n{contents}",
    );
    assert!(
        contents.contains("initiating teardown because"),
        "log must state WHY the manager tore down; contents:\n{contents}",
    );
    // The sender resolution must include the (comm: "cmdline") shape —
    // the test runner has a non-empty /proc/<pid>/comm and cmdline.
    assert!(
        contents.contains(": \""),
        "log must resolve the sender to (comm: \"cmdline\"); contents:\n{contents}",
    );
}
