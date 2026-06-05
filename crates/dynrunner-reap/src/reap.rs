//! Single concern: the PID-set reap state-machine.
//!
//! Reap a set of captured host PIDs directly, independent of any
//! container record: for each target, identity-checked
//! SIGTERM → bounded grace → SIGKILL → bounded grace → final verify.
//! Returns a [`ReapStatus`] the caller uses to decide what to do next
//! (the shutdown-manager: whether to destroy the podman handle and what
//! exit code to report; the wrapper: whether the container is genuinely
//! gone before it returns to SLURM).
//!
//! Conservative by construction:
//!   * signals only the EXACT captured PIDs — never a pattern/pkill;
//!   * before EVERY signal it re-confirms the PID still names the SAME
//!     process via the captured start time
//!     ([`ProcessProbe::is_same_process`]); a PID that is gone OR whose
//!     start time has changed (kernel PID reuse) short-circuits to
//!     `ConfirmedGone` with NO signal sent, so the reap signal only ever
//!     reaches the genuine original process;
//!   * escalation to SIGKILL only happens if the PID survives the SIGTERM
//!     grace; `OrphanSurvives` is returned only after SIGKILL plus its own
//!     grace fail to clear it (a stuck/uninterruptible process), so the
//!     reaper never reports success over a live PID.

use crate::clock::Clock;
use crate::process_probe::ProcessProbe;
use std::time::Duration;

/// One PID the reaper must kill, paired with the start time captured for
/// it at the moment its PID was learned. The start time closes the
/// PID-reuse kill-path hazard: the reap re-checks identity before every
/// signal, so a reused PID is never signalled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReapTarget {
    /// Host PID to reap.
    pub pid: u32,
    /// `/proc/<pid>/starttime` captured alongside the PID, or `None` if
    /// the capture raced the `/proc` read (treated as "cannot confirm
    /// identity" → not signalled).
    pub start_time: Option<u64>,
}

impl ReapTarget {
    /// Construct a target from a PID and its captured start time.
    pub fn new(pid: u32, start_time: Option<u64>) -> Self {
        Self { pid, start_time }
    }
}

/// The two grace windows the reap escalation honours: how long to wait
/// for SIGTERM to take effect before escalating, and how long to wait
/// after SIGKILL before declaring the PID an unkillable orphan. Kept a
/// caller-supplied value (not a constant) so each consumer sizes it to
/// its own deadline — the wrapper self-bounds within `KillWait`, the
/// shutdown-manager uses its configured `secondary_grace` /
/// `container_stop_grace`.
#[derive(Debug, Clone, Copy)]
pub struct ReapGraces {
    /// Grace after SIGTERM before escalating to SIGKILL.
    pub sigterm_grace: Duration,
    /// Grace after SIGKILL before declaring the PID an orphan.
    pub sigkill_grace: Duration,
}

/// Result of the PID-set reap. A caller must NOT treat the target set as
/// cleaned up while any captured PID is still alive — that was the
/// "false success" defect. The aggregate is the worst per-target outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReapStatus {
    /// No PID was ever captured (e.g. the container was gone before the
    /// first sighting), so there is nothing to verify.
    NotApplicable,
    /// Every captured PID is confirmed gone — each had already exited, or
    /// died within grace after SIGTERM / SIGKILL. Safe to proceed.
    ConfirmedGone,
    /// At least one captured PID is STILL ALIVE after SIGTERM, the grace
    /// window, SIGKILL, and a second grace window. The caller must treat
    /// the set as not cleaned up (the shutdown-manager leaves the podman
    /// handle intact and exits non-zero; the wrapper logs the survivor).
    OrphanSurvives,
}

impl ReapStatus {
    /// Fold two per-target outcomes into the aggregate. `OrphanSurvives`
    /// dominates (any survivor means the set is not clean);
    /// `ConfirmedGone` dominates `NotApplicable` (a real reap happened).
    fn worse(self, other: ReapStatus) -> ReapStatus {
        match (self, other) {
            (ReapStatus::OrphanSurvives, _) | (_, ReapStatus::OrphanSurvives) => {
                ReapStatus::OrphanSurvives
            }
            (ReapStatus::ConfirmedGone, _) | (_, ReapStatus::ConfirmedGone) => {
                ReapStatus::ConfirmedGone
            }
            (ReapStatus::NotApplicable, ReapStatus::NotApplicable) => ReapStatus::NotApplicable,
        }
    }
}

/// Reap every captured PID in `targets` in order, returning the worst
/// per-target [`ReapStatus`]. An empty target set is `NotApplicable`.
///
/// Targets are reaped SEQUENTIALLY — each runs the same identity-checked
/// SIGTERM → grace → SIGKILL → grace escalation [`reap_one`] performs.
/// Ordering matters only for the log narrative; the aggregate status is
/// order-independent. Callers that reap conmon + workload pass conmon
/// first so the supervisor is signalled before its child in the log.
pub fn reap_pids<P: ProcessProbe, C: Clock, L: FnMut(&str)>(
    probe: &P,
    clock: &C,
    targets: &[ReapTarget],
    graces: ReapGraces,
    log: &mut L,
) -> ReapStatus {
    if targets.is_empty() {
        log("no PID was captured; nothing to reap by PID");
        return ReapStatus::NotApplicable;
    }
    let mut status = ReapStatus::NotApplicable;
    for target in targets {
        status = status.worse(reap_one(probe, clock, *target, graces, log));
    }
    status
}

/// Reap a single captured PID: identity-checked SIGTERM → `sigterm_grace`
/// → SIGKILL → `sigkill_grace` → final verify. This is the verbatim
/// single-PID state-machine the shutdown-manager's `reap_workload_pid`
/// used, lifted here so both consumers share it.
fn reap_one<P: ProcessProbe, C: Clock, L: FnMut(&str)>(
    probe: &P,
    clock: &C,
    target: ReapTarget,
    graces: ReapGraces,
    log: &mut L,
) -> ReapStatus {
    let ReapTarget {
        pid,
        start_time: captured_start,
    } = target;
    if !probe.is_same_process(pid, captured_start) {
        log(&format!(
            "PID {} gone or reused (start time no longer matches); no signal sent",
            pid
        ));
        return ReapStatus::ConfirmedGone;
    }

    let term_ok = probe.signal(pid, libc::SIGTERM);
    log(&format!("kill -TERM {} → {}", pid, term_ok));
    if wait_for_pid_gone(probe, clock, pid, captured_start, graces.sigterm_grace, log) {
        return ReapStatus::ConfirmedGone;
    }

    log(&format!(
        "PID {} still alive after {}s; escalating to SIGKILL",
        pid,
        graces.sigterm_grace.as_secs()
    ));
    let kill_ok = probe.signal(pid, libc::SIGKILL);
    log(&format!("kill -KILL {} → {}", pid, kill_ok));
    match wait_for_pid_gone(probe, clock, pid, captured_start, graces.sigkill_grace, log) {
        true => ReapStatus::ConfirmedGone,
        false => {
            log(&format!(
                "PID {} STILL alive after SIGKILL + {}s grace",
                pid,
                graces.sigkill_grace.as_secs()
            ));
            ReapStatus::OrphanSurvives
        }
    }
}

/// Poll the captured PID's identity-aware liveness once per second up to
/// `grace`, returning `true` as soon as the PID is gone (or its start
/// time no longer matches — a reused PID is treated as gone, the
/// reap-success condition) and `false` if `grace` elapses with the SAME
/// process still alive. The 1-second cadence is intentionally independent
/// of any caller poll interval. Public so the shutdown-manager's
/// graceful-last-resort path can reuse the exact identity-aware wait.
pub fn wait_for_pid_gone<P: ProcessProbe, C: Clock, L: FnMut(&str)>(
    probe: &P,
    clock: &C,
    pid: u32,
    captured_start: Option<u64>,
    grace: Duration,
    log: &mut L,
) -> bool {
    let tick = Duration::from_secs(1);
    let mut elapsed = Duration::ZERO;
    while elapsed < grace {
        if !probe.is_same_process(pid, captured_start) {
            log(&format!("PID {} exited after {}s", pid, elapsed.as_secs()));
            return true;
        }
        clock.sleep(tick);
        elapsed += tick;
    }
    // Final check after the last sleep so a process that dies exactly on
    // the boundary is still observed as gone.
    !probe.is_same_process(pid, captured_start)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{FakeClock, MockProcessProbe, MOCK_WORKLOAD_START};

    fn graces() -> ReapGraces {
        ReapGraces {
            sigterm_grace: Duration::from_secs(1),
            sigkill_grace: Duration::from_secs(1),
        }
    }

    /// Empty target set: nothing to do, NotApplicable, no probe calls.
    #[test]
    fn empty_targets_is_not_applicable() {
        let probe = MockProcessProbe::always_alive();
        let clock = FakeClock::new();
        let status = reap_pids(&probe, &clock, &[], graces(), &mut |_| {});
        assert_eq!(status, ReapStatus::NotApplicable);
        assert!(probe.signals_sent().is_empty(), "no signals for empty set");
    }

    /// A captured PID that is already gone (start time no longer matches)
    /// is ConfirmedGone with NO signal sent — the PID-reuse guard.
    #[test]
    fn gone_or_reused_pid_not_signalled() {
        // Capture sees MOCK_WORKLOAD_START; reap re-check sees None (gone).
        let probe = MockProcessProbe::reap_start_times(vec![None]);
        let clock = FakeClock::new();
        let target = ReapTarget::new(123, Some(MOCK_WORKLOAD_START));
        let status = reap_pids(&probe, &clock, &[target], graces(), &mut |_| {});
        assert_eq!(status, ReapStatus::ConfirmedGone);
        assert!(
            probe.signals_sent().is_empty(),
            "a gone/reused PID must NOT be signalled; sent: {:?}",
            probe.signals_sent()
        );
    }

    /// A live PID that dies after SIGTERM: SIGTERM only, ConfirmedGone.
    #[test]
    fn dies_after_sigterm_only_term_sent() {
        // capture: alive; first wait re-check: gone.
        let probe = MockProcessProbe::reap_start_times(vec![Some(MOCK_WORKLOAD_START), None]);
        let clock = FakeClock::new();
        let target = ReapTarget::new(7, Some(MOCK_WORKLOAD_START));
        let status = reap_pids(&probe, &clock, &[target], graces(), &mut |_| {});
        assert_eq!(status, ReapStatus::ConfirmedGone);
        assert_eq!(
            probe.signals_sent(),
            vec![(7, libc::SIGTERM)],
            "only SIGTERM should be sent when the PID dies in the first grace"
        );
    }

    /// A PID that survives SIGTERM but dies after SIGKILL: SIGTERM then
    /// SIGKILL, ConfirmedGone. Drives the escalation ordering.
    #[test]
    fn survives_sigterm_dies_after_sigkill() {
        // capture: alive; sigterm-grace re-checks: alive (so escalate);
        // sigkill-grace re-check: gone.
        let probe = MockProcessProbe::reap_start_times(vec![
            Some(MOCK_WORKLOAD_START), // capture identity check (is_same_process at entry)
            Some(MOCK_WORKLOAD_START), // wait_for_pid_gone after SIGTERM: still alive
            Some(MOCK_WORKLOAD_START), // boundary final check after SIGTERM grace
            None,                      // wait_for_pid_gone after SIGKILL: gone
        ]);
        let clock = FakeClock::new();
        let target = ReapTarget::new(9, Some(MOCK_WORKLOAD_START));
        let status = reap_pids(&probe, &clock, &[target], graces(), &mut |_| {});
        assert_eq!(status, ReapStatus::ConfirmedGone);
        assert_eq!(
            probe.signals_sent(),
            vec![(9, libc::SIGTERM), (9, libc::SIGKILL)],
            "SIGTERM then SIGKILL on a PID that survives the first grace"
        );
    }

    /// A PID that never dies (always_alive) is OrphanSurvives after both
    /// SIGTERM and SIGKILL — the never-report-success-over-a-live-PID
    /// invariant. This is the regression guard for the orphan defect.
    #[test]
    fn never_dies_is_orphan_survives() {
        let probe = MockProcessProbe::always_alive();
        let clock = FakeClock::new();
        let target = ReapTarget::new(11, Some(MOCK_WORKLOAD_START));
        let status = reap_pids(&probe, &clock, &[target], graces(), &mut |_| {});
        assert_eq!(status, ReapStatus::OrphanSurvives);
        assert_eq!(
            probe.signals_sent(),
            vec![(11, libc::SIGTERM), (11, libc::SIGKILL)],
            "an unkillable PID gets both SIGTERM and SIGKILL before giving up"
        );
    }

    /// Multi-target aggregate: one PID dies, the other never does ⇒ the
    /// set is OrphanSurvives (any survivor dominates), and BOTH PIDs were
    /// signalled. Models reaping conmon (dies) + workload (orphan).
    #[test]
    fn multi_target_any_survivor_is_orphan_survives() {
        // Two targets reaped sequentially against ONE probe. The probe's
        // start_time channel is shared, so script it to make the FIRST
        // target die on SIGTERM and the SECOND survive forever.
        let probe = MockProcessProbe::reap_start_times(vec![
            // target A (pid 100): capture alive, then gone after SIGTERM
            Some(MOCK_WORKLOAD_START),
            None,
            // target B (pid 200): alive at capture, alive forever after
            // (saturates at the final scripted value).
            Some(MOCK_WORKLOAD_START),
        ]);
        let clock = FakeClock::new();
        let targets = [
            ReapTarget::new(100, Some(MOCK_WORKLOAD_START)),
            ReapTarget::new(200, Some(MOCK_WORKLOAD_START)),
        ];
        let status = reap_pids(&probe, &clock, &targets, graces(), &mut |_| {});
        assert_eq!(
            status,
            ReapStatus::OrphanSurvives,
            "one surviving target makes the aggregate OrphanSurvives"
        );
        let sent = probe.signals_sent();
        assert!(
            sent.contains(&(100, libc::SIGTERM)),
            "first target must be SIGTERM'd; sent: {:?}",
            sent
        );
        assert!(
            sent.contains(&(200, libc::SIGTERM)) && sent.contains(&(200, libc::SIGKILL)),
            "surviving target must get SIGTERM then SIGKILL; sent: {:?}",
            sent
        );
    }

    /// `ReapStatus::worse` fold: OrphanSurvives dominates, then
    /// ConfirmedGone, then NotApplicable.
    #[test]
    fn worse_fold_precedence() {
        use ReapStatus::*;
        assert_eq!(OrphanSurvives.worse(ConfirmedGone), OrphanSurvives);
        assert_eq!(ConfirmedGone.worse(OrphanSurvives), OrphanSurvives);
        assert_eq!(ConfirmedGone.worse(NotApplicable), ConfirmedGone);
        assert_eq!(NotApplicable.worse(ConfirmedGone), ConfirmedGone);
        assert_eq!(NotApplicable.worse(NotApplicable), NotApplicable);
    }
}
