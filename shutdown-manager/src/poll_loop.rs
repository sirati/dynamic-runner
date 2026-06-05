//! Single concern: the shutdown-manager state machine.
//!
//! Inputs: a [`PodmanBackend`], a [`ShutdownFlag`], a [`Clock`], and a
//! [`PollConfig`] (the subset of `Config` that the state machine
//! actually reads).
//!
//! Output: a [`RunReport`] — which branch fired ([`Outcome`]) plus
//! whether the captured container workload PID was confirmed dead
//! ([`ReapStatus`]). Filesystem cleanup is *not* this module's
//! concern — main runs it afterwards using `cleanup::final_cleanup`.
//!
//! The module's job is "ensure the container workload process is
//! actually dead before destroying the podman handle." Two facts drive
//! the design:
//!
//!   * The workload (e.g. python) is the container's main process,
//!     which runs as a child of **conmon** — never a child of host
//!     PID 1. So `pgrep -P 1` could never find it; the host PID comes
//!     from `podman inspect --format {{.State.Pid}}`.
//!   * Once podman loses the container record (`--rm` cleanup, or a
//!     premature `rm -af`), every name-based `podman kill`/`stop`
//!     no-ops while conmon+workload may still be alive. So the loop
//!     CAPTURES the host workload PID while the record exists, then in
//!     SIGNAL_SHUTDOWN signals+verifies that PID directly — via the
//!     [`crate::process_probe::ProcessProbe`], independent of podman's
//!     record. The podman handle (`rm -af`) is destroyed ONLY after
//!     the PID is confirmed gone.
//!
//! State machine:
//!
//! ```text
//! main loop:
//!   if shutdown flag set → SIGNAL_SHUTDOWN
//!   if wrapper PID gone   → SIGNAL_SHUTDOWN
//!   if container_exists:
//!     saw = true; down_count = 0
//!     workload_pid = inspect .State.Pid   (capture latest known)
//!   else if saw:
//!     down_count += 1
//!     if down_count >= ceil(idle_shutdown / poll_interval):
//!       IDLE_SHUTDOWN
//!   sleep(poll_interval); repeat
//!
//! SIGNAL_SHUTDOWN(captured_pid):
//!   # record-based signalling (best-effort; no-ops if record gone)
//!   if container_exists:
//!     pid = pgrep -P 1 -o (Option)
//!     if Some(pid): podman exec kill -TERM pid
//!     podman kill --signal TERM <name>
//!     wait up to secondary_grace; if alive: stop -t container_stop_grace
//!   # PID-based reap (independent of the podman record)
//!   reap = reap_workload_pid(captured_pid):
//!     if no pid captured: NotApplicable
//!     elif not alive:     ConfirmedGone
//!     else:
//!       signal SIGTERM; wait secondary_grace
//!       if alive: signal SIGKILL; wait container_stop_grace
//!       ConfirmedGone if !alive else OrphanSurvives
//!   if reap != OrphanSurvives: podman rm -af   # destroy handle only when dead
//!
//! IDLE_SHUTDOWN:
//!   podman rm -af
//! ```

use crate::podman::PodmanBackend;
use crate::shutdown_flag::ShutdownFlag;
use dynrunner_reap::clock::Clock;
use dynrunner_reap::process_probe::ProcessProbe;
use dynrunner_reap::reap::{reap_pids, wait_for_pid_gone, ReapGraces, ReapTarget};
use std::path::Path;
use std::time::Duration;

// The reap outcome type is owned by the shared reap crate; re-export it
// under `poll_loop` so the manager's `main` and the integration tests
// keep their historical `poll_loop::ReapStatus` import path.
pub use dynrunner_reap::reap::ReapStatus;

/// Graceful last-resort budget: how long the reaper waits, after
/// writing the panik sentinel, for the workload to stop on its own.
/// The secondary's in-container panik watcher polls its sentinel paths
/// at a 10s cadence by default, then runs worker-teardown before
/// exiting; two minutes leaves comfortable room for that cascade even
/// under load while still bounding how long the reaper lingers before
/// finally giving up. Mirrors the other grace constants
/// ([`PollConfig::secondary_grace`], [`PollConfig::container_stop_grace`])
/// as a named value rather than a magic literal at the call site.
const PANIK_GRACE: Duration = Duration::from_secs(120);

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
    /// Optional HOST-side path of the secondary's monitored panik
    /// sentinel (the host side of the file the secondary's in-container
    /// watcher polls; see [`crate::config::Config::panik_file`]). When
    /// `Some`, a surviving orphan triggers the graceful last resort:
    /// write this sentinel and wait [`PANIK_GRACE`] for the secondary's
    /// own watcher to shut its workers down and exit. `None` disables
    /// the last resort; a surviving orphan reports `OrphanSurvives`
    /// directly, exactly as before this knob existed.
    pub panik_file: Option<std::path::PathBuf>,
}

/// Which branch of the state machine fired. Returned by `run` so the
/// caller (and tests) can observe the outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Reached because the shutdown flag was set (SIGTERM or SIGCONT),
    /// or the monitored wrapper PID disappeared.
    SignalShutdown,
    /// Reached because the container was absent for >= idle_shutdown
    /// after having been seen at least once.
    IdleShutdown,
}

/// Precisely WHY the state machine entered teardown. `Outcome`
/// distinguishes signal-vs-idle; this distinguishes the two ways a
/// `SignalShutdown` can be reached so `main` can always log a WHY line —
/// closing the gap where the wrapper-monitor path tore down with no
/// recorded cause (`describe_last_signal` returns `None` there, because
/// no signal was ever captured).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownTrigger {
    /// The shutdown flag was set by a delivered signal (SIGTERM/SIGCONT).
    /// `main` resolves the sender via `signals::describe_last_signal`.
    Signal,
    /// The monitored wrapper PID disappeared — the SLURM job process that
    /// owns this container is gone, so we force teardown. Carries the PID
    /// that vanished so the WHY line names it. This is the modern,
    /// debounce-free replacement for the removed `squeue` poll: the
    /// wrapper PID vanishing IS the authoritative "job is gone" signal.
    WrapperGone { pid: u32 },
    /// The container was absent for >= idle_shutdown after a prior
    /// sighting (the IDLE_SHUTDOWN branch), which already logs its own
    /// cause line.
    Idle,
}

/// What `run` reports to `main`: which branch fired, WHY it fired, and
/// whether the captured PIDs were confirmed dead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunReport {
    pub outcome: Outcome,
    pub trigger: ShutdownTrigger,
    pub reap: ReapStatus,
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
) -> RunReport {
    let ticks_for_idle = ceil_ticks(cfg.idle_shutdown, cfg.poll_interval);
    let mut saw_once = false;
    let mut down_count: u64 = 0;
    // Latest captured host PIDs of the container's conmon supervisor AND
    // its workload, each paired with the start time captured for it,
    // recorded every poll the container record is present. The PIDs let
    // SIGNAL_SHUTDOWN reap both directly even after podman drops the
    // record; the captured start times let the reap confirm each PID
    // still names the SAME process before signalling, closing the
    // PID-reuse kill-path hazard. conmon is reaped too because it — not
    // the workload — is the process that re-parents to host PID 1 and
    // keeps NFS writers alive after a missed cgroup sweep.
    let mut captured = CapturedPids::default();
    loop {
        if flag.is_set() {
            log("signal observed; entering SIGNAL_SHUTDOWN");
            let reap = signal_shutdown(backend, clock, probe, cfg, &captured, &mut log);
            return RunReport {
                outcome: Outcome::SignalShutdown,
                trigger: ShutdownTrigger::Signal,
                reap,
            };
        }
        if let Some(pid) = wrapper_gone(probe, cfg.wrapper_pid) {
            log("wrapper PID gone; entering SIGNAL_SHUTDOWN (wrapper-monitor)");
            let reap = signal_shutdown(backend, clock, probe, cfg, &captured, &mut log);
            return RunReport {
                outcome: Outcome::SignalShutdown,
                trigger: ShutdownTrigger::WrapperGone { pid },
                reap,
            };
        }
        match backend.container_exists(&cfg.container_name) {
            true => {
                saw_once = true;
                down_count = 0;
                // Refresh the captured PIDs while the record still
                // exists. A `None` here (transient inspect failure) does
                // not clobber a previously-captured PID: the last known
                // good value is what we want to reap. Each PID's start
                // time is captured at the SAME instant (via the probe's
                // `/proc` read) so the reap can later confirm the PID
                // still names this exact process.
                if let Some(pid) = backend.conmon_pid(&cfg.container_name) {
                    captured.conmon = Some((pid, probe.start_time(pid)));
                }
                if let Some(pid) = backend.workload_pid(&cfg.container_name) {
                    captured.workload = Some((pid, probe.start_time(pid)));
                }
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
                        return RunReport {
                            outcome: Outcome::IdleShutdown,
                            trigger: ShutdownTrigger::Idle,
                            reap: ReapStatus::NotApplicable,
                        };
                    }
                }
            }
        }
        clock.sleep(cfg.poll_interval);
    }
}

/// The host PIDs the poll loop captures while the container record
/// exists, so SIGNAL_SHUTDOWN can reap them directly after podman drops
/// the record. Each is `Some((pid, captured_start_time))` once seen.
/// Both conmon and the workload are tracked: conmon is the supervisor
/// that survives a missed cgroup sweep and keeps NFS writers alive, so it
/// must be reaped too (not only the workload).
#[derive(Debug, Clone, Copy, Default)]
pub struct CapturedPids {
    conmon: Option<(u32, Option<u64>)>,
    workload: Option<(u32, Option<u64>)>,
}

impl CapturedPids {
    /// Build the reap target list — conmon first (supervisor before its
    /// child in the log narrative), then the workload. Each captured PID
    /// contributes one [`ReapTarget`]; absent ones are skipped.
    fn targets(&self) -> Vec<ReapTarget> {
        [self.conmon, self.workload]
            .into_iter()
            .flatten()
            .map(|(pid, start)| ReapTarget::new(pid, start))
            .collect()
    }
}

/// Reduce "optional wrapper PID + probe" to a wake-input for the poll
/// loop. Returns `Some(pid)` when the configured wrapper PID is gone (the
/// cue to force teardown, carrying the PID so the caller can record WHY),
/// `None` when the wrapper is still alive OR no PID was configured — so
/// the loop's decision table stays the same shape with and without the
/// wrapper-monitor enabled.
fn wrapper_gone<P: ProcessProbe>(probe: &P, pid: Option<u32>) -> Option<u32> {
    match pid {
        Some(p) if !probe.is_alive(p) => Some(p),
        _ => None,
    }
}

/// SIGNAL_SHUTDOWN branch. Public so tests can drive it directly with
/// a flag-already-set scenario; production reaches it via `run`.
///
/// Two stages, in order:
///
///   1. **Record-based signalling** — the original best-effort
///      `podman exec kill` / `podman kill` / `podman stop` path,
///      gated on `container_exists` because those subcommands no-op
///      once podman loses the record. This does NOT remove anything.
///   2. **PID-based reap** — signal+verify the captured host conmon AND
///      workload PIDs directly through the [`ProcessProbe`], independent
///      of the podman record. The podman handle (`rm -af`) is destroyed
///      ONLY when the reap confirms BOTH are gone; if any known PID is
///      still alive the handle is left intact and the returned
///      [`ReapStatus`] is `OrphanSurvives`.
pub fn signal_shutdown<B: PodmanBackend, C: Clock, P: ProcessProbe, L: FnMut(&str)>(
    backend: &B,
    clock: &C,
    probe: &P,
    cfg: &PollConfig,
    captured: &CapturedPids,
    log: &mut L,
) -> ReapStatus {
    record_based_signal(backend, clock, cfg, log);
    let reap = reap_captured_pids(probe, clock, cfg, captured, log);
    // When the direct PID-reap could not confirm the set dead, try the
    // graceful last resort BEFORE giving up: write the panik sentinel the
    // secondary's own watcher monitors and wait a bounded window for it
    // to shut down and exit on its own. This can only ever DOWNGRADE
    // OrphanSurvives → ConfirmedGone (the workload self-exited) — it never
    // escalates, never sends a signal, never removes anything. Any other
    // reap status (ConfirmedGone / NotApplicable) passes through
    // untouched. It keys on the WORKLOAD PID: the secondary's in-container
    // watcher stops the workload, and conmon exits after its child under
    // `--rm`, so a confirmed-gone workload clears the whole set.
    let reap = match reap {
        ReapStatus::OrphanSurvives => {
            graceful_panik_file_attempt(probe, clock, cfg, captured.workload, log)
        }
        other => other,
    };
    // Destroy the podman handle ONLY when nothing known is still alive.
    // Removing it while a PID survives is exactly the defect that empties
    // `podman ps -a` and turns every later name-based kill into a no-op.
    match reap {
        ReapStatus::OrphanSurvives => log(
            "a captured PID is still alive after SIGKILL grace; LEAVING podman \
             handle intact (not rm-ing) so the orphan stays inspectable",
        ),
        ReapStatus::ConfirmedGone | ReapStatus::NotApplicable => {
            let _ = backend.rm_all();
            log("podman rm -af invoked");
        }
    }
    reap
}

/// Graceful last resort for a surviving orphan: write the panik
/// sentinel into the secondary's monitored location and wait
/// [`PANIK_GRACE`] for the workload to stop on its own.
///
/// Reached ONLY from the `OrphanSurvives` branch of
/// [`signal_shutdown`], i.e. AFTER the direct PID-reap (SIGTERM →
/// grace → SIGKILL → grace) failed to confirm the workload dead. It is
/// the very last thing the reaper tries before reporting failure.
///
/// Behaviour:
///   * `cfg.panik_file == None` ⇒ no sentinel location is known; the
///     last resort is disabled and the orphan stays `OrphanSurvives`
///     (the pre-this-knob behaviour).
///   * no workload PID was captured ⇒ there is nothing to wait on;
///     return the input status unchanged (defensive: the caller only
///     reaches here on `OrphanSurvives`, which implies a captured PID,
///     but the function stays total without relying on that).
///   * otherwise: write the sentinel ONCE at `cfg.panik_file`, then
///     poll the captured PID's identity-aware liveness for
///     [`PANIK_GRACE`] via the SAME [`wait_for_pid_gone`] helper the
///     PID-reap uses (the PID-reuse-safe `is_same_process` check, never
///     a bare `is_alive`). If the workload self-exits within the
///     window → `ConfirmedGone`; otherwise → `OrphanSurvives`.
///
/// The ONLY new side effect is writing one sentinel file at the one
/// monitored path. No kill, no signal, no `rm`.
fn graceful_panik_file_attempt<P: ProcessProbe, C: Clock, L: FnMut(&str)>(
    probe: &P,
    clock: &C,
    cfg: &PollConfig,
    workload_pid: Option<(u32, Option<u64>)>,
    log: &mut L,
) -> ReapStatus {
    let Some(panik_file) = cfg.panik_file.as_deref() else {
        log(
            "no --panik-file configured; skipping graceful last resort, \
             orphan stays unreaped",
        );
        return ReapStatus::OrphanSurvives;
    };
    let Some((pid, captured_start)) = workload_pid else {
        // No PID to wait on — nothing the sentinel could resolve.
        return ReapStatus::OrphanSurvives;
    };

    write_panik_sentinel(panik_file, log);

    log(&format!(
        "waiting up to {}s for the secondary's panik watcher to stop workload PID {}",
        PANIK_GRACE.as_secs(),
        pid
    ));
    match wait_for_pid_gone(probe, clock, pid, captured_start, PANIK_GRACE, log) {
        true => {
            log(&format!(
                "workload PID {} exited after the panik sentinel; graceful last resort succeeded",
                pid
            ));
            ReapStatus::ConfirmedGone
        }
        false => {
            log(&format!(
                "workload PID {} still alive {}s after the panik sentinel; giving up",
                pid,
                PANIK_GRACE.as_secs()
            ));
            ReapStatus::OrphanSurvives
        }
    }
}

/// Write the panik sentinel file at the secondary's monitored host
/// path. The secondary's in-container watcher fires on the file's mere
/// existence, so an empty file is sufficient; we write a short marker
/// for operator legibility. Best-effort: a write failure is logged and
/// the caller falls through to the bounded wait anyway (which then
/// times out to `OrphanSurvives`) — losing the graceful path is
/// strictly less bad than aborting the reaper.
fn write_panik_sentinel<L: FnMut(&str)>(panik_file: &Path, log: &mut L) {
    match std::fs::write(panik_file, b"reaper last-resort: workload survived direct reap\n") {
        Ok(()) => log(&format!(
            "graceful last resort: wrote panik sentinel at {}",
            panik_file.display()
        )),
        Err(e) => log(&format!(
            "graceful last resort: could not write panik sentinel at {}: {}",
            panik_file.display(),
            e
        )),
    }
}

/// Stage 1: the original record-based signalling, unchanged except
/// that it no longer removes anything. Best-effort throughout — every
/// call no-ops once podman has dropped the record.
fn record_based_signal<B: PodmanBackend, C: Clock, L: FnMut(&str)>(
    backend: &B,
    clock: &C,
    cfg: &PollConfig,
    log: &mut L,
) {
    match backend.container_exists(&cfg.container_name) {
        false => log("container record already gone at SIGNAL_SHUTDOWN entry"),
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
}

/// Stage 2: reap the captured host conmon + workload PIDs directly,
/// independent of the podman record. Delegates the SIGTERM → grace →
/// SIGKILL → grace → verify state-machine to the shared
/// [`dynrunner_reap::reap`] crate (the SAME reap the wrapper's in-band
/// teardown uses), mapping the manager's configured graces onto
/// [`ReapGraces`]. Returns the [`ReapStatus`] the caller uses to decide
/// whether to destroy the podman handle and what exit code to report.
fn reap_captured_pids<P: ProcessProbe, C: Clock, L: FnMut(&str)>(
    probe: &P,
    clock: &C,
    cfg: &PollConfig,
    captured: &CapturedPids,
    log: &mut L,
) -> ReapStatus {
    let targets = captured.targets();
    let graces = ReapGraces {
        sigterm_grace: cfg.secondary_grace,
        sigkill_grace: cfg.container_stop_grace,
    };
    reap_pids(probe, clock, &targets, graces, log)
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
            panik_file: None,
        }
    }

    fn cfg_with_wrapper(poll_secs: u64, idle_secs: u64, pid: u32) -> PollConfig {
        PollConfig {
            wrapper_pid: Some(pid),
            ..cfg(poll_secs, idle_secs)
        }
    }

    /// Tiny-grace cfg for the PID-reap regression tests. `FakeClock`
    /// never actually sleeps, so the grace value only controls how
    /// many identity polls the verify loop performs — keeping it at
    /// 1s makes the probe scripts short and the call accounting
    /// obvious. `wrapper_pid` stays `None` so the ONLY probe
    /// consultations in these tests come from the reap-verify path's
    /// identity check, never the wrapper-monitor.
    fn cfg_reap() -> PollConfig {
        PollConfig {
            secondary_grace: Duration::from_secs(1),
            container_stop_grace: Duration::from_secs(1),
            ..cfg(2, 4)
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
        let report = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
        assert_eq!(report.outcome, Outcome::SignalShutdown);
    }

    #[test]
    fn idle_fires_after_grace_following_sighting() {
        let backend = MockBackend::new();
        // Sighting on tick 1, then absent forever. idle=4s, poll=2s →
        // ceil_ticks=2; needs 2 consecutive absent polls AFTER sighting.
        backend.script_exists(vec![true, false, false, false]);
        let flag = ShutdownFlag::new();
        let clock = FakeClock::new();
        let report = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
        assert_eq!(report.outcome, Outcome::IdleShutdown);
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
        let report = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
        assert_eq!(report.outcome, Outcome::SignalShutdown);
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
        let report = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
        assert_eq!(report.outcome, Outcome::SignalShutdown);
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
        let report = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
        assert_eq!(report.outcome, Outcome::SignalShutdown);
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
        let report = run(&backend, &flag, &clock, &always_alive(), &cfg(2, 4), |_| {});
        assert_eq!(report.outcome, Outcome::SignalShutdown);
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
        let report = run(&backend, &flag, &clock, &probe, &cfg(2, 4), |_| {});
        assert_eq!(
            report.outcome,
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
        let report = run(&backend, &flag, &clock, &probe, &cfg_with_wrapper(2, 30, 4242), |_| {});
        assert_eq!(report.outcome, Outcome::SignalShutdown);
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
        let report = run(&backend, &flag, &clock, &probe, &cfg_with_wrapper(2, 30, 4242), |_| {});
        assert_eq!(report.outcome, Outcome::SignalShutdown);
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
        let report = run(
            &backend,
            &flag,
            &clock,
            &probe,
            &cfg_with_wrapper(2, 30, 4242),
            |m| lines.push(m.to_string()),
        );
        assert_eq!(report.outcome, Outcome::SignalShutdown);
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

    // ----------- PID-based orphan reap ----------------
    //
    // The confirmed real-world failure: the reaper spawned, ran
    // SIGNAL_SHUTDOWN, but did NOT reap a live orphan. Three defects:
    //   1. `pgrep -P 1` never found the workload (it is conmon's child,
    //      not host-PID-1's) → in-container kill skipped.
    //   2. `rm -af` destroyed the podman handle while conmon+workload
    //      were still alive → every later name-based kill no-oped.
    //   3. the manager exited 0 despite the live process.
    //
    // The fix captures the workload host PID from
    // `podman inspect .State.Pid` while the record exists, then in
    // SIGNAL_SHUTDOWN signals+verifies THAT PID directly (independent
    // of the record), and only `rm`s once the PID is confirmed gone —
    // never exiting 0 with a known-live orphan. These tests pin all
    // three properties.

    /// Capture the workload PID while the container record exists, then
    /// the record vanishes BEFORE SIGNAL_SHUTDOWN (the `--rm`/premature-
    /// cleanup orphan case). The reaper must NOT no-op: it signals the
    /// captured PID, the PID dies within the SIGTERM grace, and only
    /// THEN is the podman handle removed.
    #[test]
    fn orphan_record_gone_pid_alive_reaped_by_pid_then_rm() {
        let backend = MockBackend::new();
        // tick1: record present (capture PID); SIGNAL_SHUTDOWN entry:
        // record gone. So container_exists yields [true (sighting),
        // false (record_based_signal entry)].
        backend.script_exists(vec![true, false]);
        backend.script_workload_pid(vec![Some(4242)]);
        let flag = ShutdownFlag::new();
        let clock = FakeClock::new();
        // Flag fires after the first sleep — i.e. after the PID is
        // captured on tick 1, modelling "signal arrives once the
        // workload is running".
        clock.set_on_sleep(1, flag.clone());
        // start_time channel: [capture (tick 1), pre-SIGTERM identity
        // check → still same, first verify poll → gone].
        let probe = MockProcessProbe::reap(vec![true, true, false]);
        let report = run(&backend, &flag, &clock, &probe, &cfg_reap(), |_| {});

        assert_eq!(report.outcome, Outcome::SignalShutdown);
        assert_eq!(
            report.reap,
            ReapStatus::ConfirmedGone,
            "captured PID died within SIGTERM grace ⇒ ConfirmedGone"
        );
        // The captured PID was signalled SIGTERM — the reaper did NOT
        // no-op just because the podman record was gone.
        assert_eq!(
            probe.signals_sent(),
            vec![(4242, libc::SIGTERM)],
            "SIGTERM must be delivered to the captured PID; got {:?}",
            probe.signals_sent()
        );
        // Handle removed only AFTER the PID was confirmed gone.
        assert!(
            backend.calls().contains(&"rm_all".to_string()),
            "rm_all must run once the PID is confirmed dead; calls: {:?}",
            backend.calls()
        );
    }

    /// Orphan PID survives SIGTERM but dies after SIGKILL escalation.
    /// Confirms the bounded-grace escalation: SIGTERM → grace → SIGKILL
    /// → grace → confirmed gone, in that order, against the one
    /// captured PID.
    #[test]
    fn orphan_pid_survives_term_dies_on_kill() {
        let backend = MockBackend::new();
        backend.script_exists(vec![true, false]);
        backend.script_workload_pid(vec![Some(777)]);
        let flag = ShutdownFlag::new();
        let clock = FakeClock::new();
        clock.set_on_sleep(1, flag.clone());
        // grace=1s ⇒ wait_for_pid_gone does 2 identity polls when same.
        // start_time channel: capture(true) + pre-SIGTERM(true) +
        // SIGTERM-grace(true in-loop, true final ⇒ survives) + SIGKILL-
        // grace(false ⇒ gone on first poll). Saturating gone thereafter.
        let probe = MockProcessProbe::reap(vec![true, true, true, true, false]);
        let report = run(&backend, &flag, &clock, &probe, &cfg_reap(), |_| {});

        assert_eq!(report.outcome, Outcome::SignalShutdown);
        assert_eq!(report.reap, ReapStatus::ConfirmedGone);
        assert_eq!(
            probe.signals_sent(),
            vec![(777, libc::SIGTERM), (777, libc::SIGKILL)],
            "must escalate SIGTERM → SIGKILL on the captured PID, in order; got {:?}",
            probe.signals_sent()
        );
        assert!(
            backend.calls().contains(&"rm_all".to_string()),
            "rm_all runs once SIGKILL confirms the PID gone; calls: {:?}",
            backend.calls()
        );
    }

    /// PID-reuse kill-path guard: the workload PID was captured while
    /// the container record existed, but by the time SIGNAL_SHUTDOWN
    /// reaches the reap the original workload has exited and the kernel
    /// has handed the SAME PID number to an unrelated process — so its
    /// `/proc/<pid>/starttime` no longer matches the captured value.
    /// The reaper MUST treat the captured PID as gone: send NO signal
    /// (so it never hits the innocent reuser), report `ConfirmedGone`,
    /// and remove the podman handle. This is the conservative property:
    /// the reap signal only ever reaches the genuine original workload.
    #[test]
    fn captured_pid_reused_before_signal_is_treated_as_gone_not_signaled() {
        let backend = MockBackend::new();
        // tick1: record present → PID + start time captured. Record
        // gone at SIGNAL_SHUTDOWN entry (the orphan/--rm shape).
        backend.script_exists(vec![true, false]);
        backend.script_workload_pid(vec![Some(8080)]);
        let flag = ShutdownFlag::new();
        let clock = FakeClock::new();
        clock.set_on_sleep(1, flag.clone());
        // start_time channel: capture records 1000; the pre-SIGTERM
        // identity re-check reads 2000 — the PID has been REUSED by a
        // new process (different start time). is_same_process(8080,
        // Some(1000)) is therefore false ⇒ gone, no signal.
        let probe = MockProcessProbe::reap_start_times(vec![Some(1000), Some(2000)]);
        let report = run(&backend, &flag, &clock, &probe, &cfg_reap(), |_| {});

        assert_eq!(report.outcome, Outcome::SignalShutdown);
        assert_eq!(
            report.reap,
            ReapStatus::ConfirmedGone,
            "a PID whose start time changed (reuse) must be treated as gone"
        );
        // The crux: NOTHING was signalled. The reaper did not deliver
        // SIGTERM/SIGKILL to the reused PID's innocent occupant.
        assert!(
            probe.signals_sent().is_empty(),
            "no signal may be sent to a PID whose identity cannot be confirmed; got {:?}",
            probe.signals_sent()
        );
        // The handle is removed — the original workload is gone.
        assert!(
            backend.calls().contains(&"rm_all".to_string()),
            "rm_all runs once the original workload is confirmed gone; calls: {:?}",
            backend.calls()
        );
    }

    /// Orphan PID survives BOTH SIGTERM and SIGKILL (a stuck /
    /// uninterruptible process). The reaper must NOT report success and
    /// must NOT destroy the podman handle: `ReapStatus::OrphanSurvives`,
    /// no `rm_all`. This is the property that prevents the "exit 0 with
    /// a live orphan" false-success — `main` maps OrphanSurvives to a
    /// non-zero exit.
    #[test]
    fn orphan_pid_survives_both_signals_no_rm_no_false_success() {
        let backend = MockBackend::new();
        backend.script_exists(vec![true, false]);
        backend.script_workload_pid(vec![Some(31337)]);
        let flag = ShutdownFlag::new();
        let clock = FakeClock::new();
        clock.set_on_sleep(1, flag.clone());
        // Always alive: the PID never dies, even after SIGKILL + grace.
        let probe = MockProcessProbe::always_alive();
        let report = run(&backend, &flag, &clock, &probe, &cfg_reap(), |_| {});

        assert_eq!(report.outcome, Outcome::SignalShutdown);
        assert_eq!(
            report.reap,
            ReapStatus::OrphanSurvives,
            "a PID alive after SIGKILL+grace must NOT be reported as reaped"
        );
        // Both signals were attempted against the captured PID.
        assert_eq!(
            probe.signals_sent(),
            vec![(31337, libc::SIGTERM), (31337, libc::SIGKILL)],
            "both SIGTERM and SIGKILL must be attempted; got {:?}",
            probe.signals_sent()
        );
        // The podman handle must be LEFT INTACT while the workload is
        // alive — removing it is exactly the defect that turned later
        // kills into no-ops.
        assert!(
            !backend.calls().contains(&"rm_all".to_string()),
            "rm_all must NOT run while the orphan PID is still alive; calls: {:?}",
            backend.calls()
        );
    }

    /// No workload PID was ever captured (container gone before any
    /// sighting reached the inspect call). The reap is `NotApplicable`
    /// and the handle is still removed — preserving the original
    /// best-effort teardown for the genuinely-nothing-to-reap case.
    #[test]
    fn no_pid_captured_reap_not_applicable_still_rms() {
        let backend = MockBackend::new();
        backend.script_exists(vec![false]); // gone at SIGNAL_SHUTDOWN entry
        let flag = ShutdownFlag::new();
        flag.set_for_test();
        let clock = FakeClock::new();
        // always_dead would matter only if is_alive were consulted; it
        // is not, because NotApplicable short-circuits before any probe
        // call. Asserting no signals proves that.
        let probe = MockProcessProbe::always_dead();
        let report = run(&backend, &flag, &clock, &probe, &cfg_reap(), |_| {});

        assert_eq!(report.outcome, Outcome::SignalShutdown);
        assert_eq!(report.reap, ReapStatus::NotApplicable);
        assert!(
            probe.signals_sent().is_empty(),
            "no PID captured ⇒ no signal sent; got {:?}",
            probe.signals_sent()
        );
        assert!(
            backend.calls().contains(&"rm_all".to_string()),
            "rm_all still runs when there is nothing to reap; calls: {:?}",
            backend.calls()
        );
    }

    // ----------- graceful panik-file last resort ----------------
    //
    // Reached ONLY from the OrphanSurvives branch, AFTER the direct
    // PID-reap (SIGTERM → grace → SIGKILL → grace) failed. The reaper
    // writes the panik sentinel the secondary's own watcher monitors
    // and waits PANIK_GRACE for the workload to self-exit. Properties
    // pinned here: (1) the sentinel is written exactly once at the
    // configured path; (2) workload-stops-within-budget ⇒ ConfirmedGone
    // + rm_all; (3) workload-survives-budget ⇒ OrphanSurvives + handle
    // intact; (4) NO new kill/signal/rm is introduced by the attempt.

    use crate::testing::MOCK_WORKLOAD_START;

    /// A `cfg_reap`-shaped config that also carries the panik-file
    /// sentinel host path, enabling the graceful last resort.
    fn cfg_reap_with_panik(panik_file: std::path::PathBuf) -> PollConfig {
        PollConfig {
            panik_file: Some(panik_file),
            ..cfg_reap()
        }
    }

    /// `--panik-file` set, workload survives the direct reap but then
    /// self-exits inside the graceful window: the reaper writes the
    /// sentinel exactly once at the configured path and reports
    /// `ConfirmedGone` (exit 0 in `main`), removing the podman handle.
    /// The start-time script keeps the PID alive through the entire
    /// PID-reap (6 identity reads: capture + pre-SIGTERM + 2 SIGTERM-
    /// grace + 2 SIGKILL-grace) so OrphanSurvives is reached, then
    /// flips to gone on the first graceful poll.
    #[test]
    fn orphan_then_panik_file_workload_stops_within_budget_confirms_gone() {
        let dir = tempfile::tempdir().unwrap();
        // The "host side" of the secondary's bind-mounted sentinel.
        let panik_file = dir.path().join("log").join(".dynrunner-reaper.panik");
        std::fs::create_dir_all(panik_file.parent().unwrap()).unwrap();

        let backend = MockBackend::new();
        backend.script_exists(vec![true, false]);
        backend.script_workload_pid(vec![Some(4242)]);
        let flag = ShutdownFlag::new();
        let clock = FakeClock::new();
        clock.set_on_sleep(1, flag.clone());
        // 6 reads alive (the full PID-reap), then gone on the graceful
        // poll. Saturating `None` thereafter.
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
            "workload self-exited inside the panik window ⇒ ConfirmedGone"
        );
        // The sentinel was written exactly once at the configured path.
        assert!(
            panik_file.exists(),
            "panik sentinel must be written at the configured path"
        );
        // Only SIGTERM + SIGKILL from the direct reap — the graceful
        // attempt sends NO new signal.
        assert_eq!(
            probe.signals_sent(),
            vec![(4242, libc::SIGTERM), (4242, libc::SIGKILL)],
            "graceful attempt must not send any new signal; got {:?}",
            probe.signals_sent()
        );
        // Handle removed only after the workload was confirmed gone.
        assert!(
            backend.calls().contains(&"rm_all".to_string()),
            "rm_all must run once the workload is confirmed gone; calls: {:?}",
            backend.calls()
        );
    }

    /// `--panik-file` set, workload survives BOTH the direct reap AND
    /// the graceful window: the reaper writes the sentinel exactly once
    /// and STILL reports `OrphanSurvives` (exit 1 in `main`), leaving
    /// the podman handle intact. No new kill/signal/rm.
    #[test]
    fn orphan_then_panik_file_workload_survives_budget_stays_orphan() {
        let dir = tempfile::tempdir().unwrap();
        let panik_file = dir.path().join("log").join(".dynrunner-reaper.panik");
        std::fs::create_dir_all(panik_file.parent().unwrap()).unwrap();

        let backend = MockBackend::new();
        backend.script_exists(vec![true, false]);
        backend.script_workload_pid(vec![Some(31337)]);
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
            "workload alive after the panik window ⇒ still OrphanSurvives"
        );
        // The sentinel was still written exactly once (the reaper tried
        // the graceful path before giving up).
        assert!(
            panik_file.exists(),
            "panik sentinel must be written even when the workload survives"
        );
        // Only the direct reap's SIGTERM + SIGKILL — nothing new.
        assert_eq!(
            probe.signals_sent(),
            vec![(31337, libc::SIGTERM), (31337, libc::SIGKILL)],
            "graceful attempt must not send any new signal; got {:?}",
            probe.signals_sent()
        );
        // Handle LEFT INTACT while the orphan lives — the no-false-
        // success invariant of the direct PID-reap.
        assert!(
            !backend.calls().contains(&"rm_all".to_string()),
            "rm_all must NOT run while the orphan PID is still alive; calls: {:?}",
            backend.calls()
        );
    }

    /// `--panik-file` UNSET (default): a surviving orphan must NOT write
    /// any sentinel and must report `OrphanSurvives` directly, exactly
    /// as before this knob existed. Guards the back-compat / opt-in
    /// contract.
    #[test]
    fn orphan_without_panik_file_writes_nothing_and_stays_orphan() {
        let backend = MockBackend::new();
        backend.script_exists(vec![true, false]);
        backend.script_workload_pid(vec![Some(777)]);
        let flag = ShutdownFlag::new();
        let clock = FakeClock::new();
        clock.set_on_sleep(1, flag.clone());
        let probe = MockProcessProbe::always_alive();
        // cfg_reap has panik_file: None.
        let report = run(&backend, &flag, &clock, &probe, &cfg_reap(), |_| {});

        assert_eq!(report.reap, ReapStatus::OrphanSurvives);
        assert!(
            !backend.calls().contains(&"rm_all".to_string()),
            "handle stays intact; calls: {:?}",
            backend.calls()
        );
    }

    /// Direct test of the graceful helper: a workload reported gone on
    /// the first identity poll yields `ConfirmedGone` and writes the
    /// sentinel exactly once. Isolated from the reap-read-count
    /// bookkeeping of the run-boundary tests.
    #[test]
    fn graceful_helper_writes_sentinel_once_and_confirms_gone_when_pid_exits() {
        let dir = tempfile::tempdir().unwrap();
        let panik_file = dir.path().join(".dynrunner-reaper.panik");
        let clock = FakeClock::new();
        // start_time None ⇒ is_same_process false ⇒ "gone" on first poll.
        let probe = MockProcessProbe::reap_start_times(vec![None]);
        let cfg = cfg_reap_with_panik(panik_file.clone());

        let mut writes = 0u32;
        let status = graceful_panik_file_attempt(
            &probe,
            &clock,
            &cfg,
            Some((4242, Some(MOCK_WORKLOAD_START))),
            &mut |m| {
                if m.contains("wrote panik sentinel") {
                    writes += 1;
                }
            },
        );

        assert_eq!(status, ReapStatus::ConfirmedGone);
        assert_eq!(writes, 1, "sentinel must be written exactly once");
        assert!(panik_file.exists(), "sentinel file must exist on disk");
        assert!(
            probe.signals_sent().is_empty(),
            "the graceful helper must never signal; got {:?}",
            probe.signals_sent()
        );
    }

    /// Direct test of the graceful helper: `panik_file == None` ⇒ no
    /// write, returns `OrphanSurvives` unchanged.
    #[test]
    fn graceful_helper_noop_when_panik_file_unset() {
        let clock = FakeClock::new();
        let probe = MockProcessProbe::always_alive();
        let cfg = cfg_reap(); // panik_file: None
        let mut writes = 0u32;
        let status = graceful_panik_file_attempt(
            &probe,
            &clock,
            &cfg,
            Some((4242, Some(MOCK_WORKLOAD_START))),
            &mut |m| {
                if m.contains("wrote panik sentinel") {
                    writes += 1;
                }
            },
        );
        assert_eq!(status, ReapStatus::OrphanSurvives);
        assert_eq!(writes, 0, "no sentinel write when --panik-file is unset");
    }
}
