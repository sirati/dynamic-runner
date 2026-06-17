//! The observer's per-task operator narrator (#520).
//!
//! # Single concern
//!
//! Turn each [`crate::task_state_change::TaskStateChangeEvent`] the CRDT
//! merge join fires into ONE operator narration line at the level the
//! spec fixes (INFO assign/complete/state-change, WARN recoverable/oom
//! fail, ERROR terminal fail), routed to the tracing target that matches
//! its operator-wake class. Under the #583/#587 per-narration-kind
//! classification the per-task class is uniformly HIGH-VOLUME (a
//! per-task emit scales with the task count, drowning the wake stream
//! at the 46k-task or asm-dataset thousand-failure scale), so EVERY
//! arm here — INFO assign/complete/state-change, WARN recoverable/oom
//! fail, ERROR terminal fail — emits on
//! [`dynrunner_core::OBSERVER_TASK_TARGET`] via
//! [`dynrunner_core::high_volume_target`]: visible on the default stdio
//! stream, suppressed FROM stdio under `--important-stdio-only`, kept
//! on the full log at TRACE. The wake signal for per-task failures is
//! the rate-limited `ErrorAggregationPolicy` rollup on
//! [`dynrunner_core::IMPORTANT_TARGET`]
//! (`observer::failure_response::aggregation`), which already
//! aggregates the failure events into a periodic "task failures
//! (aggregated, last 60s): …" line — that is the operator-visible
//! wake line at scale.
//!
//! The once-per-run baseline summary stays on
//! [`dynrunner_core::IMPORTANT_TARGET`]: per-RUN, not per-task, so the
//! volume class is "normal".
//!
//! The narrator owns NO state beyond a single baseline-vs-live latch —
//! every narrated field rides the event, which the merge join derived
//! from the CRDT the primary already maintains (NO observer-only CRDT).
//!
//! # Module boundary
//!
//! The observer run loop owns the channel receiver (the
//! `respawn_exec`-outbox shape) and the catch-up sequencing; this module
//! owns only the event→line projection + the level mapping. The two
//! crossing points:
//!   - [`ObserverTaskNarrator::narrate_baseline`]: called ONCE at loop
//!     entry with the count of buffered (pre-loop) baseline transitions,
//!     emitting the single summary line that REPLACES narrating the
//!     66k-task bootstrap mirror as 66k "changes" (the bootstrap-flood
//!     guard).
//!   - [`ObserverTaskNarrator::narrate_live`]: called per live event
//!     drained inside the select! loop AFTER the baseline summary.
//!
//! # De-dup with `RunNarrator`
//!
//! `RunNarrator` already narrates phase started/complete (#508), setup
//! lifecycle (#508), retry-pass starts, the run-complete/aborted terminal
//! summary (#513), and the failover/peer-membership transitions (#333:
//! secondary joined/left, primary left/changed). This narrator emits ONLY
//! the per-TASK lines (assign / complete / fail / non-terminal state
//! change) — it touches no phase, no peer, no terminal-summary line. The
//! two narrators are sibling concerns over the same mirror; there is no
//! double-emit.

use dynrunner_core::{IMPORTANT_TARGET, high_volume_target};

/// Every per-task narration arm (assign / complete / fail-terminal /
/// fail-recoverable / fail-oom / non-terminal state change) is
/// classified HIGH-VOLUME under the #583/#587 contract: a per-task
/// emit is intrinsically per-task and scales with the task count
/// (a 46k-task build fires N assigns + N completes; an asm-dataset
/// untuned-packages run fires thousands of `non_recoverable`
/// terminal-failure ERRORs). The wake signal is the rate-limited
/// `ErrorAggregationPolicy` rollup on `IMPORTANT_TARGET`
/// (`observer::failure_response::aggregation`); the per-task lines
/// here go to `OBSERVER_TASK_TARGET` (visible in the default stdio
/// stream, suppressed under `--important-stdio-only`, captured
/// unconditionally on the full log).
///
/// The non-failure baseline summary (one line per run) stays on
/// `IMPORTANT_TARGET` — per-RUN, not per-task, so it is the
/// "normal" volume class. Every site routes through
/// [`dynrunner_core::high_volume_target`] so the
/// `is_high_volume → target` mapping has ONE owner; if a future
/// arm needs the other class, the call site flips its boolean and
/// the target follows.
const PER_TASK_TARGET: &str = high_volume_target(true);

use crate::cluster_state::StateCounts;
use crate::task_state_change::{TaskStateChange, TaskStateChangeEvent};

/// Per-task operator narrator. Holds only the baseline-vs-live latch
/// (narration bookkeeping, NOT replicated CRDT): live narration is armed
/// only AFTER the one-line bootstrap baseline summary fires, so a
/// late-joiner mirroring an N-task baseline narrates one summary, never N
/// "changes".
#[derive(Debug, Default)]
pub(crate) struct ObserverTaskNarrator {
    /// `true` once [`Self::narrate_baseline`] has run; live per-event
    /// narration is gated on it so a stray live event drained before the
    /// baseline summary cannot pre-empt the ordering.
    baseline_emitted: bool,
}

impl ObserverTaskNarrator {
    /// Emit the ONE-LINE bootstrap baseline summary and arm live
    /// narration. Called once at run-loop entry, AFTER the bootstrap
    /// restore has buffered the baseline transitions, with `buffered` =
    /// how many transitions were drained as baseline and `counts` = the
    /// converged mirror's per-state partition. The baseline is the run's
    /// initial STATE, not a stream of changes — so it gets one summary,
    /// derived from the same `StateCounts` projection the periodic
    /// reporter uses (NO observer-only tally).
    ///
    /// A cold fleet whose baseline is empty (`buffered == 0` and an empty
    /// mirror) still arms live narration but emits nothing — there is no
    /// baseline to summarise.
    pub(crate) fn narrate_baseline(&mut self, buffered: usize, counts: StateCounts) {
        self.baseline_emitted = true;
        // Nothing mirrored yet (a from-scratch cold-join before any
        // snapshot): no baseline to narrate, just arm live.
        if buffered == 0 && counts == StateCounts::default() {
            return;
        }
        tracing::info!(
            target: IMPORTANT_TARGET,
            baseline_transitions = buffered,
            pending = counts.pending,
            in_flight = counts.in_flight,
            completed = counts.completed,
            failed = counts.failed,
            blocked = counts.blocked,
            skipped_already_done = counts.skipped_already_done,
            setup_succeeded = counts.setup_succeeded,
            "observer mirroring baseline: {} pending / {} in-flight / {} completed / {} failed / {} blocked / {} skipped / {} setup-done — narrating live changes from here",
            counts.pending,
            counts.in_flight,
            counts.completed,
            counts.failed,
            counts.blocked,
            counts.skipped_already_done,
            counts.setup_succeeded,
        );
    }

    /// Narrate ONE live task transition. Emits a single line at the
    /// spec-fixed level. A no-op until [`Self::narrate_baseline`] has armed
    /// live narration (a transition drained before the baseline summary is
    /// folded into the baseline count by the caller, never narrated here).
    /// Returns whether a line was emitted — the caller's wake-stream
    /// piggyback seam (a narrated transition is a wake-stream HOST, exactly
    /// like `RunNarrator::observe`'s return).
    pub(crate) fn narrate_live(&self, event: &TaskStateChangeEvent) -> bool {
        if !self.baseline_emitted {
            return false;
        }
        let id = &event.task_id;
        let holder = Self::holder_str(event);
        match &event.change {
            TaskStateChange::Assigned => {
                tracing::info!(
                    target: PER_TASK_TARGET,
                    "task {id} assigned to {holder}",
                );
            }
            TaskStateChange::Completed => {
                tracing::info!(
                    target: PER_TASK_TARGET,
                    "task {id} completed on {holder}",
                );
            }
            TaskStateChange::TerminalFailure { reason, last_error } => {
                tracing::error!(
                    target: PER_TASK_TARGET,
                    "task {id} terminally failed on {holder}: {reason} — {last_error}",
                );
            }
            TaskStateChange::RecoverableFailure { reason } => {
                tracing::warn!(
                    target: PER_TASK_TARGET,
                    "task {id} failed (recoverable) on {holder}: {reason}",
                );
            }
            TaskStateChange::OomFailure { reason } => {
                tracing::warn!(
                    target: PER_TASK_TARGET,
                    "task {id} failed (oom) on {holder}: {reason}",
                );
            }
            TaskStateChange::Other { state } => {
                tracing::info!(
                    target: PER_TASK_TARGET,
                    "task {id} changed state to {state}",
                );
            }
        }
        true
    }

    /// The `{secondary}-{worker}` holder rendering, or a neutral
    /// `unknown-holder` when the event carries none (a completion / failure
    /// whose prior `InFlight` was never observed by this mirror — the
    /// terminal arrived over a snapshot that skipped the assignment). The
    /// operator still gets the terminal line; only the worker attribution
    /// is unknown.
    fn holder_str(event: &TaskStateChangeEvent) -> String {
        match &event.holder {
            Some((secondary, worker)) => format!("{secondary}-{worker}"),
            None => "unknown-holder".to_string(),
        }
    }
}

#[cfg(test)]
mod tests;
