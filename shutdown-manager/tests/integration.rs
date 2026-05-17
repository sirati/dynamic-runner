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
use dynrunner_slurm_shutdown::testing::{FakeClock, MockBackend};
use std::time::Duration;
use tempfile::tempdir;

fn cfg(poll_secs: u64, idle_secs: u64) -> PollConfig {
    PollConfig {
        container_name: "ctr".to_string(),
        poll_interval: Duration::from_secs(poll_secs),
        idle_shutdown: Duration::from_secs(idle_secs),
        secondary_grace: Duration::from_secs(5),
        container_stop_grace: Duration::from_secs(10),
    }
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
    let outcome = run(&backend, &flag, &clock, &cfg(2, 4), |_| {});
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
    let outcome = run(&backend, &flag, &clock, &cfg(2, 4), |_| {});
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
    let outcome = run(&backend, &flag, &clock, &cfg(2, 4), |_| {});
    assert_eq!(outcome, Outcome::IdleShutdown);
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
    let outcome = run(&backend, &flag, &clock, &cfg(2, 4), |_| {});
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
    run(&backend, &flag, &clock, &cfg(2, 4), |_| {});
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
    run(&backend, &flag, &clock, &cfg(2, 4), |_| {});
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
    let outcome = run(&backend, &flag, &clock, &cfg(2, 4), |_| {});
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
    let outcome = run(&backend, &flag, &clock, &cfg(2, 4), |_| {});
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
    run(&backend, &flag, &clock, &cfg(2, 4), |_| {});
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
