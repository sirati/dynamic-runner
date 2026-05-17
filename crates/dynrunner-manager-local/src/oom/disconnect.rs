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

use dynrunner_core::{ErrorType, ResourceKind};

use crate::worker::WorkerExitStatus;

/// Reclassify a `Disconnected` event's `ErrorType` using the worker's
/// captured exit-status and the OOM-watcher's recent-kernel-OOM
/// signal. Returns the corrected `ErrorType`; the caller substitutes
/// it into the `TaskResult` before the result is recorded.
///
/// Pure function — no side effects, no I/O.
pub fn classify_disconnect(
    original: ErrorType,
    exit_status: Option<&WorkerExitStatus>,
    kernel_oom_recent: bool,
) -> ErrorType {
    // Only the protocol-layer Recoverable default gets reclassified.
    // Worker-explicit NonRecoverable / Unfulfillable / ResourceExhausted
    // tags ride through verbatim.
    if !matches!(original, ErrorType::Recoverable) {
        return original;
    }

    let Some(status) = exit_status else {
        return ErrorType::Recoverable;
    };

    if !status.was_killed() {
        return ErrorType::Recoverable;
    }

    let signal = match status.signal {
        Some(s) => s,
        None => return ErrorType::Recoverable,
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
        return ErrorType::ResourceExhausted(ResourceKind::memory());
    }

    if matches!(signal, SIGSEGV | SIGABRT | SIGBUS | SIGFPE | SIGILL) {
        // Deterministic-bug class: retrying reproduces the crash.
        // Stops the retry loop early.
        return ErrorType::NonRecoverable;
    }

    ErrorType::Recoverable
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
        let result = classify_disconnect(
            ErrorType::Recoverable,
            Some(&killed_with(9)),
            true,
        );
        assert_eq!(result, ErrorType::ResourceExhausted(ResourceKind::memory()));
    }

    #[test]
    fn sigkill_without_kernel_oom_stays_recoverable() {
        // External SIGKILL with no cgroup-OOM correlate (operator
        // kill, host signal): retrying is safe — the worker may
        // have been the victim of a transient external event.
        let result = classify_disconnect(
            ErrorType::Recoverable,
            Some(&killed_with(9)),
            false,
        );
        assert_eq!(result, ErrorType::Recoverable);
    }

    #[test]
    fn sigsegv_routes_to_nonrecoverable() {
        let result = classify_disconnect(
            ErrorType::Recoverable,
            Some(&killed_with(11)),
            false,
        );
        assert_eq!(result, ErrorType::NonRecoverable);
    }

    #[test]
    fn sigabrt_routes_to_nonrecoverable() {
        let result = classify_disconnect(
            ErrorType::Recoverable,
            Some(&killed_with(6)),
            false,
        );
        assert_eq!(result, ErrorType::NonRecoverable);
    }

    #[test]
    fn sigsegv_with_kernel_oom_still_nonrecoverable_falls_through_first_match() {
        // Belt + braces: if a worker segfaulted in the same window
        // a different worker triggered oom_kill, the SIGKILL→OOM
        // arm requires `signal == SIGKILL`. SIGSEGV must still
        // route to NonRecoverable.
        let result = classify_disconnect(
            ErrorType::Recoverable,
            Some(&killed_with(11)),
            true,
        );
        assert_eq!(result, ErrorType::NonRecoverable);
    }

    #[test]
    fn clean_exit_stays_recoverable() {
        // worker exited with a non-zero code but the framework
        // observed pipe-EOF first (rare; usually the worker would
        // have sent Response::Error). Without a signal there's
        // nothing to upgrade — stays Recoverable.
        let result = classify_disconnect(
            ErrorType::Recoverable,
            Some(&exited_with(1)),
            false,
        );
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
        let result = classify_disconnect(
            ErrorType::NonRecoverable,
            Some(&killed_with(9)),
            true,
        );
        assert_eq!(result, ErrorType::NonRecoverable);
    }
}
