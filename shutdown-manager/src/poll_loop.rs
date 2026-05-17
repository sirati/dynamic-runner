//! Single concern: the shutdown-manager state machine.
//!
//! Inputs: a [`PodmanBackend`], a [`ShutdownFlag`], a [`Clock`], and a
//! [`PollConfig`] (the subset of `Config` that the state machine
//! actually reads).
//!
//! Output: an [`Outcome`] describing which branch fired. Filesystem
//! cleanup is *not* this module's concern — main runs it afterwards
//! using `cleanup::final_cleanup`.
//!
//! State machine (verbatim from the project brief):
//!
//! ```text
//! main loop:
//!   if shutdown flag set → SIGNAL_SHUTDOWN
//!   if container_exists:
//!     saw = true; down_count = 0
//!   else if saw:
//!     down_count += 1
//!     if down_count >= ceil(idle_shutdown / poll_interval):
//!       IDLE_SHUTDOWN
//!   sleep(poll_interval); repeat
//!
//! SIGNAL_SHUTDOWN:
//!   if container_exists:
//!     pid = pgrep -P 1 -o (Option)
//!     if Some(pid): podman exec kill -TERM pid (best-effort)
//!     podman kill --signal TERM <name> (belt-and-suspenders)
//!     wait up to secondary_grace; if alive: stop -t container_stop_grace
//!   podman rm -af
//!
//! IDLE_SHUTDOWN:
//!   podman rm -af
//! ```

use crate::clock::Clock;
use crate::podman::PodmanBackend;
use crate::shutdown_flag::ShutdownFlag;
use std::time::Duration;

/// Subset of `Config` that the state machine reads. Keeping this
/// narrow avoids coupling the loop to argv shape.
#[derive(Debug, Clone)]
pub struct PollConfig {
    pub container_name: String,
    pub poll_interval: Duration,
    pub idle_shutdown: Duration,
    pub secondary_grace: Duration,
    pub container_stop_grace: Duration,
}

/// Which branch of the state machine fired. Returned by `run` so the
/// caller (and tests) can observe the outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Reached because the shutdown flag was set (SIGTERM or SIGCONT).
    SignalShutdown,
    /// Reached because the container was absent for >= idle_shutdown
    /// after having been seen at least once.
    IdleShutdown,
}

/// Drive the state machine to completion. Returns when one of the two
/// branches has run to its end (signals issued, `rm -af` invoked).
/// Filesystem cleanup (PID-file, /tmp prefix) happens in the caller.
pub fn run<B: PodmanBackend, C: Clock, L: FnMut(&str)>(
    backend: &B,
    flag: &ShutdownFlag,
    clock: &C,
    cfg: &PollConfig,
    mut log: L,
) -> Outcome {
    let ticks_for_idle = ceil_ticks(cfg.idle_shutdown, cfg.poll_interval);
    let mut saw_once = false;
    let mut down_count: u64 = 0;
    loop {
        if flag.is_set() {
            log("signal observed; entering SIGNAL_SHUTDOWN");
            signal_shutdown(backend, clock, cfg, &mut log);
            return Outcome::SignalShutdown;
        }
        match backend.container_exists(&cfg.container_name) {
            true => {
                saw_once = true;
                down_count = 0;
            }
            false => {
                // saw_once gates the idle branch; before the container
                // first appears we just keep polling.
                if saw_once {
                    down_count += 1;
                    if down_count >= ticks_for_idle {
                        log(&format!(
                            "container absent for {} consecutive polls; entering IDLE_SHUTDOWN",
                            down_count
                        ));
                        idle_shutdown(backend, &mut log);
                        return Outcome::IdleShutdown;
                    }
                }
            }
        }
        clock.sleep(cfg.poll_interval);
    }
}

/// SIGNAL_SHUTDOWN branch. Public so tests can drive it directly with
/// a flag-already-set scenario; production reaches it via `run`.
pub fn signal_shutdown<B: PodmanBackend, C: Clock, L: FnMut(&str)>(
    backend: &B,
    clock: &C,
    cfg: &PollConfig,
    log: &mut L,
) {
    match backend.container_exists(&cfg.container_name) {
        false => log("container already gone at SIGNAL_SHUTDOWN entry"),
        true => {
            let pid = backend.exec_pgrep_first_child(&cfg.container_name);
            log(&format!("pgrep -P 1 -o → {:?}", pid));
            // The `if Some(pid)` branch is the brief's wording; we model
            // it without an if-ladder at the call site by handing the
            // Option to a dedicated helper.
            send_inside_container_term(backend, &cfg.container_name, pid, log);
            // Belt-and-suspenders fallback: always send SIGTERM to pid 1
            // of the container. Covers the case pgrep found nothing or
            // the in-container kill failed.
            let ok = backend.kill_pid1(&cfg.container_name, "TERM");
            log(&format!("podman kill --signal TERM → {}", ok));
            wait_for_exit(backend, clock, &cfg.container_name, cfg.secondary_grace, log);
            if backend.container_exists(&cfg.container_name) {
                log(&format!(
                    "container still alive after {}s grace; podman stop -t {}",
                    cfg.secondary_grace.as_secs(),
                    cfg.container_stop_grace.as_secs()
                ));
                let _ = backend.stop(
                    &cfg.container_name,
                    cfg.container_stop_grace.as_secs() as u32,
                );
            }
        }
    }
    let _ = backend.rm_all();
    log("podman rm -af invoked");
}

/// IDLE_SHUTDOWN branch.
pub fn idle_shutdown<B: PodmanBackend, L: FnMut(&str)>(backend: &B, log: &mut L) {
    let _ = backend.rm_all();
    log("podman rm -af invoked (idle path)");
}

/// Send SIGTERM to the in-container PID returned by pgrep, if any.
/// Best-effort: errors logged, not propagated.
fn send_inside_container_term<B: PodmanBackend, L: FnMut(&str)>(
    backend: &B,
    name: &str,
    pid: Option<u32>,
    log: &mut L,
) {
    match pid {
        None => log("pgrep returned no child; skipping in-container kill"),
        Some(p) => {
            let ok = backend.exec_signal(name, p, "TERM");
            log(&format!("podman exec kill -TERM {} → {}", p, ok));
        }
    }
}

/// Poll `container_exists` once per second up to `grace`, returning as
/// soon as the container is absent. The 1-second cadence is fixed by
/// the brief and intentionally independent of `poll_interval`.
fn wait_for_exit<B: PodmanBackend, C: Clock, L: FnMut(&str)>(
    backend: &B,
    clock: &C,
    name: &str,
    grace: Duration,
    log: &mut L,
) {
    let tick = Duration::from_secs(1);
    let mut elapsed = Duration::ZERO;
    while elapsed < grace {
        if !backend.container_exists(name) {
            log(&format!("container exited after {}s", elapsed.as_secs()));
            return;
        }
        clock.sleep(tick);
        elapsed += tick;
    }
    log(&format!("grace of {}s elapsed; container still alive", grace.as_secs()));
}

/// Ceiling division `idle_shutdown / poll_interval`, with at least 1
/// tick. Floating-point avoided to keep release-binary size down.
fn ceil_ticks(idle: Duration, poll: Duration) -> u64 {
    let idle_ms = idle.as_millis();
    let poll_ms = poll.as_millis().max(1);
    let raw = idle_ms.div_ceil(poll_ms);
    (raw.max(1)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{FakeClock, MockBackend};

    fn cfg(poll_secs: u64, idle_secs: u64) -> PollConfig {
        PollConfig {
            container_name: "ctr".to_string(),
            poll_interval: Duration::from_secs(poll_secs),
            idle_shutdown: Duration::from_secs(idle_secs),
            secondary_grace: Duration::from_secs(5),
            container_stop_grace: Duration::from_secs(10),
        }
    }

    #[test]
    fn ceil_ticks_rounds_up() {
        assert_eq!(
            ceil_ticks(Duration::from_secs(5), Duration::from_secs(2)),
            3,
            "5/2 -> 3 ticks"
        );
        assert_eq!(
            ceil_ticks(Duration::from_secs(4), Duration::from_secs(2)),
            2,
            "4/2 -> 2 ticks"
        );
        assert_eq!(
            ceil_ticks(Duration::from_millis(500), Duration::from_secs(2)),
            1,
            "sub-tick idle -> 1 tick (floor would underflow)"
        );
    }

    #[test]
    fn idle_does_not_fire_before_first_sighting() {
        // Container is absent forever — without a prior sighting, no
        // IDLE_SHUTDOWN should fire. To bound the test, set the flag
        // after a handful of polls and verify the outcome is signal.
        let backend = MockBackend::new();
        backend.script_exists(vec![false; 1000]); // saturates
        let flag = ShutdownFlag::new();
        let clock = FakeClock::new();
        // Inject a side-effect on the 5th sleep to set the flag.
        clock.set_on_sleep(5, flag.clone());
        let outcome = run(&backend, &flag, &clock, &cfg(2, 4), |_| {});
        assert_eq!(outcome, Outcome::SignalShutdown);
    }

    #[test]
    fn idle_fires_after_grace_following_sighting() {
        let backend = MockBackend::new();
        // Sighting on tick 1, then absent forever. idle=4s, poll=2s →
        // ceil_ticks=2; needs 2 consecutive absent polls AFTER sighting.
        backend.script_exists(vec![true, false, false, false]);
        let flag = ShutdownFlag::new();
        let clock = FakeClock::new();
        let outcome = run(&backend, &flag, &clock, &cfg(2, 4), |_| {});
        assert_eq!(outcome, Outcome::IdleShutdown);
        assert!(backend.rm_all_called());
    }

    #[test]
    fn signal_shutdown_with_pgrep_some_invokes_in_container_kill() {
        let backend = MockBackend::new();
        // container alive at SIGNAL_SHUTDOWN entry, exits after one
        // 1-sec grace tick.
        backend.script_exists(vec![true, true, false]);
        backend.script_pgrep(vec![Some(42)]);
        let flag = ShutdownFlag::new();
        flag.set_for_test();
        let clock = FakeClock::new();
        let outcome = run(&backend, &flag, &clock, &cfg(2, 4), |_| {});
        assert_eq!(outcome, Outcome::SignalShutdown);
        let calls = backend.calls();
        assert!(
            calls.contains(&"exec_signal(ctr, 42, TERM)".to_string()),
            "calls: {:?}",
            calls
        );
        assert!(
            calls.contains(&"kill_pid1(ctr, TERM)".to_string()),
            "calls: {:?}",
            calls
        );
        assert!(
            calls.contains(&"rm_all".to_string()),
            "rm_all must run; calls: {:?}",
            calls
        );
    }

    #[test]
    fn signal_shutdown_with_pgrep_none_skips_in_container_kill() {
        let backend = MockBackend::new();
        backend.script_exists(vec![true, false]); // alive at entry, exits after 1s
        backend.script_pgrep(vec![None]);
        let flag = ShutdownFlag::new();
        flag.set_for_test();
        let clock = FakeClock::new();
        let outcome = run(&backend, &flag, &clock, &cfg(2, 4), |_| {});
        assert_eq!(outcome, Outcome::SignalShutdown);
        let calls = backend.calls();
        assert!(
            !calls.iter().any(|c| c.starts_with("exec_signal")),
            "exec_signal must NOT fire when pgrep returns None; calls: {:?}",
            calls
        );
        // Belt-and-suspenders must still fire.
        assert!(
            calls.contains(&"kill_pid1(ctr, TERM)".to_string()),
            "calls: {:?}",
            calls
        );
        assert!(calls.contains(&"rm_all".to_string()));
    }

    #[test]
    fn signal_shutdown_falls_through_to_stop_after_grace() {
        let backend = MockBackend::new();
        // alive at entry, alive through all five 1-sec grace polls,
        // alive when checked after wait → stop must fire.
        backend.script_exists(vec![true; 10]);
        backend.script_pgrep(vec![Some(7)]);
        let flag = ShutdownFlag::new();
        flag.set_for_test();
        let clock = FakeClock::new();
        let outcome = run(&backend, &flag, &clock, &cfg(2, 4), |_| {});
        assert_eq!(outcome, Outcome::SignalShutdown);
        let calls = backend.calls();
        assert!(
            calls.iter().any(|c| c.starts_with("stop(ctr,")),
            "stop must fire when container survives grace; calls: {:?}",
            calls
        );
    }

    #[test]
    fn signal_shutdown_skips_kill_when_container_already_gone() {
        let backend = MockBackend::new();
        backend.script_exists(vec![false]); // absent at entry
        let flag = ShutdownFlag::new();
        flag.set_for_test();
        let clock = FakeClock::new();
        let outcome = run(&backend, &flag, &clock, &cfg(2, 4), |_| {});
        assert_eq!(outcome, Outcome::SignalShutdown);
        let calls = backend.calls();
        assert!(
            !calls.iter().any(|c| c.starts_with("exec_signal")),
            "no signals if container is gone; calls: {:?}",
            calls
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("kill_pid1")),
            "no kill_pid1 either; calls: {:?}",
            calls
        );
        assert!(
            calls.contains(&"rm_all".to_string()),
            "rm_all still runs; calls: {:?}",
            calls
        );
    }
}
