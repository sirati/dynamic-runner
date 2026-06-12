//! Disconnect-time reclassifier: map a worker `Disconnected` event's
//! synthesised `ErrorType::Recoverable` onto the actual cause when the
//! kernel + exit-status say otherwise.
//!
//! Single concern: pure classification. Inputs are the values already
//! captured upstream (the protocol's default `Recoverable` synthesis,
//! the `try_reap_exit` waitpid result, and the OOM-watcher's
//! "kernel-OOM landed in the last window?" predicate); the output is
//! the corrected `ErrorType` that downstream `record_result` /
//! TaskFailed broadcasts will use.
//!
//! No I/O, no state — the classifier composes cleanly with the
//! per-event flow in `manager::events::handle_event` and
//! `secondary::processing::worker_event::handle_worker_event`.
//!
//! ## Rules
//!
//! Reclassification only fires when the upstream tag is
//! `ErrorType::Recoverable` (the default fallback for transport
//! EOF — see [`dynrunner_protocol_manager_worker::state::
//! RunnerProtocol::poll_status`]). Tasks the worker explicitly
//! tagged `NonRecoverable` on the wire (a real worker-reported
//! deterministic failure) bypass the classifier verbatim — the
//! framework never overrides an upstream-explicit verdict.
//!
//! With the Recoverable default, three exit-status shapes route:
//!
//!   * `was_killed() == true` AND `signal == SIGKILL` AND
//!     `kernel_oom_recent == true` → `ResourceExhausted(memory)`.
//!     The kernel ran the cgroup OOM-killer on the workers subgroup
//!     in the same window — the dying worker IS the kernel's
//!     victim. Counts against retry budget as a memory failure.
//!
//!   * `was_killed() == true` AND `signal ∈ {SIGSEGV, SIGABRT,
//!     SIGBUS, SIGFPE, SIGILL}` → `NonRecoverable`. The worker died
//!     from a deterministic bug (corrupted memory, assertion fired,
//!     misaligned access, etc.). Retrying just reproduces the
//!     crash; surfacing as NonRecoverable stops the retry loop.
//!
//!   * Everything else — clean exit code, SIGTERM, SIGKILL without
//!     an oom_kill correlate, missing exit status — stays
//!     `Recoverable`. This is the protocol's pre-existing default:
//!     environment glitch (host crash, signal we don't recognise);
//!     retry pass eventually surfaces persistent cases as
//!     permanent.
//!
//! The richer [`classify_disconnect_fault`] additionally answers the
//! ATTRIBUTION question (charge the task's retry budget vs requeue
//! uncharged) for callers that distinguish the two — see
//! [`DisconnectFault`]. Its one rule beyond the table above: a
//! SELF-EXIT with a nonzero code is `TaskFault(Recoverable)`
//! (executed-and-failed, budget-charged), while `classify_disconnect`
//! projects it back onto plain `Recoverable`.

use dynrunner_core::{ErrorType, ResourceKind};

use crate::worker::WorkerExitStatus;

/// The disconnect ATTRIBUTION verdict: does the worker's death
/// constitute an EXECUTED-AND-FAILED attempt of whatever task the
/// slot was responsible for (charge the task's retry budget), or a
/// transport/external loss with no process-fault evidence (requeue
/// the task uncharged)?
///
/// The boundary (production gap, asm-tokenizer run_20260612_095601:
/// a worker self-exiting nonzero at consumer arg-validation was
/// classified as a comm failure, so its task was requeued 24,323
/// times with zero failure accounting):
///
/// * `TaskFault(et)` — the worker process terminated by its own act:
///   an upstream-explicit wire error tag, a kernel-OOM SIGKILL, a
///   deterministic-bug signal (SIGSEGV class), or a SELF-EXIT with a
///   nonzero code. The attempt counts; `et` is the retry class the
///   accounting uses (`Recoverable` consumes the standard retry-pass
///   budget, then permanence).
/// * `InfraLoss` — external kill (SIGTERM / SIGKILL without an OOM
///   correlate), a clean `exit(0)`, or no reapable exit status at
///   all: nothing proves the attempt ran to a failure, so the task
///   requeues without consuming budget (a task is not at fault for
///   its environment dying).
#[derive(Debug, Clone, PartialEq)]
pub enum DisconnectFault {
    /// Executed-and-failed: charge the task's retry budget under the
    /// carried `ErrorType`.
    TaskFault(ErrorType),
    /// Environment/transport loss: requeue the task uncharged.
    InfraLoss,
}

/// Attribute a `Disconnected` event using the worker's captured
/// exit-status and the OOM-watcher's recent-kernel-OOM signal: the
/// single owner of the executed-and-failed vs infra-loss boundary.
///
/// Pure function — no side effects, no I/O.
pub fn classify_disconnect_fault(
    original: ErrorType,
    exit_status: Option<&WorkerExitStatus>,
    kernel_oom_recent: bool,
) -> DisconnectFault {
    // Worker-explicit NonRecoverable / Unfulfillable / ResourceExhausted
    // tags ride through verbatim: the worker REPORTED a real failure on
    // the wire before dying, which is executed-and-failed by definition.
    if !matches!(original, ErrorType::Recoverable) {
        return DisconnectFault::TaskFault(original);
    }

    let Some(status) = exit_status else {
        // Reap unavailable: the framework lost diagnostic visibility.
        // No process-fault evidence — stay uncharged.
        return DisconnectFault::InfraLoss;
    };

    if !status.was_killed() {
        // The worker EXITED by its own act. A nonzero code is the
        // process reporting its own deterministic failure (the
        // production shape: a consumer arg-validation raise unwinding
        // the worker with exit 1 before any wire error could be
        // sent) — executed-and-failed, charged as `Recoverable` so the
        // standard retry-pass budget still gives it bounded chances.
        // `exit(0)` mid-responsibility is NOT failure evidence; it
        // stays uncharged.
        return match status.code {
            Some(code) if code != 0 => DisconnectFault::TaskFault(ErrorType::Recoverable),
            _ => DisconnectFault::InfraLoss,
        };
    }

    let signal = match status.signal {
        Some(s) => s,
        None => return DisconnectFault::InfraLoss,
    };

    const SIGABRT: i32 = 6;
    const SIGBUS: i32 = 7;
    const SIGFPE: i32 = 8;
    const SIGKILL: i32 = 9;
    const SIGSEGV: i32 = 11;
    const SIGILL: i32 = 4;

    if signal == SIGKILL && kernel_oom_recent {
        // The kernel beat the userland scheduler — the dying worker
        // IS the cgroup-OOM victim. Surface as a memory failure so
        // downstream routes through the OOM retry channel rather
        // than the generic Recoverable retry pass.
        return DisconnectFault::TaskFault(ErrorType::ResourceExhausted(ResourceKind::memory()));
    }

    if matches!(signal, SIGSEGV | SIGABRT | SIGBUS | SIGFPE | SIGILL) {
        // Deterministic-bug class: retrying reproduces the crash.
        // Stops the retry loop early.
        return DisconnectFault::TaskFault(ErrorType::NonRecoverable);
    }

    // External SIGKILL without an OOM correlate, SIGTERM, anything
    // else: an environment event, not the task's fault.
    DisconnectFault::InfraLoss
}

/// Reclassify a `Disconnected` event's `ErrorType` using the worker's
/// captured exit-status and the OOM-watcher's recent-kernel-OOM
/// signal. Returns the corrected `ErrorType`; the caller substitutes
/// it into the `TaskResult` before the result is recorded.
///
/// The `ErrorType`-only projection of [`classify_disconnect_fault`]
/// for callers whose accounting charges every disconnect anyway (the
/// LocalManager's `record_result`): the fault dimension collapses —
/// `InfraLoss` and `TaskFault(Recoverable)` both surface as
/// `Recoverable`.
///
/// Pure function — no side effects, no I/O.
pub fn classify_disconnect(
    original: ErrorType,
    exit_status: Option<&WorkerExitStatus>,
    kernel_oom_recent: bool,
) -> ErrorType {
    match classify_disconnect_fault(original, exit_status, kernel_oom_recent) {
        DisconnectFault::TaskFault(et) => et,
        DisconnectFault::InfraLoss => ErrorType::Recoverable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn killed_with(signal: i32) -> WorkerExitStatus {
        WorkerExitStatus {
            code: None,
            signal: Some(signal),
            signal_name: None,
            core_dumped: false,
        }
    }

    fn exited_with(code: i32) -> WorkerExitStatus {
        WorkerExitStatus {
            code: Some(code),
            signal: None,
            signal_name: None,
            core_dumped: false,
        }
    }

    #[test]
    fn sigkill_with_kernel_oom_upgrades_to_resource_exhausted() {
        let result = classify_disconnect(ErrorType::Recoverable, Some(&killed_with(9)), true);
        assert_eq!(result, ErrorType::ResourceExhausted(ResourceKind::memory()));
    }

    #[test]
    fn sigkill_without_kernel_oom_stays_recoverable() {
        // External SIGKILL with no cgroup-OOM correlate (operator
        // kill, host signal): retrying is safe — the worker may
        // have been the victim of a transient external event.
        let result = classify_disconnect(ErrorType::Recoverable, Some(&killed_with(9)), false);
        assert_eq!(result, ErrorType::Recoverable);
    }

    #[test]
    fn sigsegv_routes_to_nonrecoverable() {
        let result = classify_disconnect(ErrorType::Recoverable, Some(&killed_with(11)), false);
        assert_eq!(result, ErrorType::NonRecoverable);
    }

    #[test]
    fn sigabrt_routes_to_nonrecoverable() {
        let result = classify_disconnect(ErrorType::Recoverable, Some(&killed_with(6)), false);
        assert_eq!(result, ErrorType::NonRecoverable);
    }

    #[test]
    fn sigsegv_with_kernel_oom_still_nonrecoverable_falls_through_first_match() {
        // Belt + braces: if a worker segfaulted in the same window
        // a different worker triggered oom_kill, the SIGKILL→OOM
        // arm requires `signal == SIGKILL`. SIGSEGV must still
        // route to NonRecoverable.
        let result = classify_disconnect(ErrorType::Recoverable, Some(&killed_with(11)), true);
        assert_eq!(result, ErrorType::NonRecoverable);
    }

    #[test]
    fn clean_exit_stays_recoverable() {
        // worker exited with a non-zero code but the framework
        // observed pipe-EOF first (rare; usually the worker would
        // have sent Response::Error). Without a signal there's
        // nothing to upgrade — stays Recoverable.
        let result = classify_disconnect(ErrorType::Recoverable, Some(&exited_with(1)), false);
        assert_eq!(result, ErrorType::Recoverable);
    }

    #[test]
    fn missing_exit_status_stays_recoverable() {
        // Reap returned None — framework lost diagnostic visibility.
        // Original Recoverable rides through.
        let result = classify_disconnect(ErrorType::Recoverable, None, true);
        assert_eq!(result, ErrorType::Recoverable);
    }

    #[test]
    fn upstream_nonrecoverable_never_overridden() {
        // Worker explicitly reported NonRecoverable on the wire;
        // even with a kernel-OOM correlate, the classifier must
        // preserve the upstream verdict.
        let result = classify_disconnect(ErrorType::NonRecoverable, Some(&killed_with(9)), true);
        assert_eq!(result, ErrorType::NonRecoverable);
    }

    // ── Attribution (fault) verdicts ────────────────────────────────

    #[test]
    fn nonzero_self_exit_is_a_charged_task_fault() {
        // THE production gap (asm-tokenizer run_20260612_095601): a
        // worker self-exiting nonzero (consumer arg-validation raise
        // unwinding the process with exit 1) is an EXECUTED-AND-FAILED
        // attempt — it must charge the task's retry budget.
        let result =
            classify_disconnect_fault(ErrorType::Recoverable, Some(&exited_with(1)), false);
        assert_eq!(result, DisconnectFault::TaskFault(ErrorType::Recoverable));
    }

    #[test]
    fn clean_self_exit_is_infra() {
        // `exit(0)` proves nothing failed; no charge.
        let result =
            classify_disconnect_fault(ErrorType::Recoverable, Some(&exited_with(0)), false);
        assert_eq!(result, DisconnectFault::InfraLoss);
    }

    #[test]
    fn missing_exit_status_is_infra() {
        let result = classify_disconnect_fault(ErrorType::Recoverable, None, false);
        assert_eq!(result, DisconnectFault::InfraLoss);
    }

    #[test]
    fn external_kill_without_oom_is_infra() {
        // SIGKILL with no oom correlate / SIGTERM: environment events,
        // never charged to the task.
        for sig in [9, 15] {
            let result =
                classify_disconnect_fault(ErrorType::Recoverable, Some(&killed_with(sig)), false);
            assert_eq!(result, DisconnectFault::InfraLoss, "signal {sig}");
        }
    }

    #[test]
    fn oom_kill_and_bug_signals_are_charged_faults() {
        assert_eq!(
            classify_disconnect_fault(ErrorType::Recoverable, Some(&killed_with(9)), true),
            DisconnectFault::TaskFault(ErrorType::ResourceExhausted(ResourceKind::memory()))
        );
        assert_eq!(
            classify_disconnect_fault(ErrorType::Recoverable, Some(&killed_with(11)), false),
            DisconnectFault::TaskFault(ErrorType::NonRecoverable)
        );
    }

    #[test]
    fn upstream_explicit_tag_is_a_charged_fault_verbatim() {
        // The worker REPORTED its failure on the wire before dying —
        // executed-and-failed by definition, class preserved.
        assert_eq!(
            classify_disconnect_fault(ErrorType::NonRecoverable, None, false),
            DisconnectFault::TaskFault(ErrorType::NonRecoverable)
        );
    }
}
