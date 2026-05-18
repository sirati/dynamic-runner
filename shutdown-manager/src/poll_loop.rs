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
use crate::process_probe::ProcessProbe;
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
    /// Optional PID of the wrapper script that spawned the manager.
    /// When `Some`, the poll loop treats wrapper disappearance as a
    /// third wake input (collapsed into the existing SIGNAL_SHUTDOWN
    /// branch). `None` disables the check; loop behaviour is then
    /// identical to the pre-monitor design.
    pub wrapper_pid: Option<u32>,
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
///
/// `probe` observes wrapper-script liveness. When `cfg.wrapper_pid`
/// is `None`, the probe is never consulted and the loop's wake-set
/// reduces to the original (flag, container-idle) pair.
pub fn run<B: PodmanBackend, C: Clock, P: ProcessProbe, L: FnMut(&str)>(
    backend: &B,
    flag: &ShutdownFlag,
    clock: &C,
    probe: &P,
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
        if wrapper_gone(probe, cfg.wrapper_pid) {
            log("wrapper PID gone; entering SIGNAL_SHUTDOWN (wrapper-monitor)");
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

/// Reduce "optional wrapper PID + probe" to a single boolean wake-input
/// for the poll loop. Returns `false` when no PID was configured so
/// the loop's decision table stays the same shape with and without
/// the wrapper-monitor enabled.
fn wrapper_gone<P: ProcessProbe>(probe: &P, pid: Option<u32>) -> bool {
    match pid {
        None => false,
        Some(p) => !probe.is_alive(p),
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
    use crate::testing::{FakeClock, MockBackend, MockProcessProbe};

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

    fn cfg_with_wrapper(poll_secs: u64, idle_secs: u64, pid: u32) -> PollConfig {
        PollConfig {
            wrapper_pid: Some(pid),
            ..cfg(poll_secs, idle_secs)
        }
    }

    /// Default probe used by tests that don't exercise the wrapper-
    /// monitor branch — always reports alive so the probe path is a
    /// no-op.
    fn always_alive() -> MockProcessProbe {
        MockProcessProbe::always_alive()
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
        let outcome = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
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
        let outcome = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
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
        let outcome = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
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
        let outcome = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
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
        let outcome = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
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
        let outcome = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
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

    // ----------- wrapper-monitor (third wake input) ----------------

    /// When `wrapper_pid` is `None`, the probe is never consulted —
    /// even a probe that would lie about the world has no effect.
    /// Encodes the backward-compat contract for callers that haven't
    /// opted in.
    #[test]
    fn wrapper_monitor_inert_when_pid_none() {
        let backend = MockBackend::new();
        // Sighting on tick 1, then absent forever → fall through to
        // IDLE_SHUTDOWN exactly as today.
        backend.script_exists(vec![true, false, false, false]);
        let flag = ShutdownFlag::new();
        let clock = FakeClock::new();
        // Probe says "always dead" — must be IGNORED when pid is None.
        let probe = MockProcessProbe::always_dead();
        let outcome = run(&backend, &flag, &clock, &probe, &cfg(2, 4), |_| {});
        assert_eq!(
            outcome,
            Outcome::IdleShutdown,
            "wrapper_pid=None ⇒ probe must not affect loop outcome"
        );
    }

    /// Wrapper alive for the first few ticks, then dead ⇒ the loop
    /// must enter SIGNAL_SHUTDOWN on the tick the probe flips, NOT
    /// wait for IDLE_SHUTDOWN's grace. Container is "present" the
    /// whole time (mimicking orphan-conmon survival post-SLURM-TERM)
    /// so the idle path can never trigger.
    #[test]
    fn wrapper_death_triggers_signal_shutdown() {
        let backend = MockBackend::new();
        // Container present for as long as the loop runs (>= 5 ticks).
        backend.script_exists(vec![true; 32]);
        // After SIGNAL_SHUTDOWN enters, the branch calls
        // `exec_pgrep_first_child` — script one return so the
        // in-container kill path is exercised.
        backend.script_pgrep(vec![Some(7)]);
        let flag = ShutdownFlag::new();
        let clock = FakeClock::new();
        // Alive on ticks 1..=3, dead from tick 4 onwards.
        let probe = MockProcessProbe::script(vec![true, true, true, false]);
        let outcome = run(&backend, &flag, &clock, &probe, &cfg_with_wrapper(2, 30, 4242), |_| {});
        assert_eq!(outcome, Outcome::SignalShutdown);
        let calls = backend.calls();
        // Signal-shutdown body must have run: rm_all is its terminal step.
        assert!(
            calls.contains(&"rm_all".to_string()),
            "SIGNAL_SHUTDOWN cleanup must run on wrapper-gone; calls: {:?}",
            calls
        );
        // The flag was NEVER set in this scenario — proves the wake
        // came from the probe, not from signals.
        assert!(!flag.is_set(), "flag must remain clear in wrapper-monitor wake path");
    }

    /// If the wrapper is already gone at the very first tick — e.g.
    /// the wrapper died between spawn and the manager's first poll —
    /// SIGNAL_SHUTDOWN must still fire (no "saw_once" gating, no
    /// extra grace).
    #[test]
    fn wrapper_already_dead_at_entry_triggers_signal_shutdown() {
        let backend = MockBackend::new();
        backend.script_exists(vec![true; 8]);
        backend.script_pgrep(vec![None]);
        let flag = ShutdownFlag::new();
        let clock = FakeClock::new();
        let probe = MockProcessProbe::always_dead();
        let outcome = run(&backend, &flag, &clock, &probe, &cfg_with_wrapper(2, 30, 4242), |_| {});
        assert_eq!(outcome, Outcome::SignalShutdown);
        assert!(backend.calls().contains(&"rm_all".to_string()));
    }

    /// The shutdown-flag path must STILL win when both flag and
    /// wrapper-gone fire on the same tick. (Both end at the same
    /// cleanup body, so this is mostly a log-ordering assertion —
    /// but the flag check sits first deliberately to preserve the
    /// "operator-initiated" log line over the "wrapper-monitor" one
    /// when an operator triggered shutdown.)
    #[test]
    fn flag_check_precedes_wrapper_check_when_both_fire() {
        let backend = MockBackend::new();
        backend.script_exists(vec![true; 4]);
        backend.script_pgrep(vec![Some(1)]);
        let flag = ShutdownFlag::new();
        flag.set_for_test();
        let clock = FakeClock::new();
        let probe = MockProcessProbe::always_dead();
        // Capture log lines to confirm which branch fired first.
        let mut lines: Vec<String> = Vec::new();
        let outcome = run(
            &backend,
            &flag,
            &clock,
            &probe,
            &cfg_with_wrapper(2, 30, 4242),
            |m| lines.push(m.to_string()),
        );
        assert_eq!(outcome, Outcome::SignalShutdown);
        let first_branch_line = lines
            .iter()
            .find(|l| l.contains("SIGNAL_SHUTDOWN"))
            .expect("a SIGNAL_SHUTDOWN log must appear");
        assert!(
            first_branch_line.contains("signal observed"),
            "flag check must be evaluated before wrapper-monitor; got: {:?}",
            lines
        );
    }
}
