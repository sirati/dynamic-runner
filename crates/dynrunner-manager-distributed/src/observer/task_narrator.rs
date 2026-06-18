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
use crate::task_state_change::{NarrationSource, TaskStateChange, TaskStateChangeEvent};

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
    /// The IN-PROGRESS catch-up batch accumulator: how many
    /// snapshot-restore-sourced ([`NarrationSource::CatchUp`]) transitions
    /// have drained since the last flush, and the DISTINCT tasks they
    /// touched. A relocated / late-join observer mirroring its whole
    /// InFlight partition over an in-loop bootstrap restore folds into ONE
    /// summary line on the batch's terminal package, NEVER N per-task
    /// "assigned" lines — the same fold [`Self::narrate_baseline`] applies
    /// to the PRE-loop bootstrap, extended to the IN-loop restore the
    /// CRDT-path-independent merge seam would otherwise narrate as live.
    catch_up_transitions: usize,
    catch_up_tasks: std::collections::HashSet<String>,
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
        // The line is partitioned BY KIND (the `counts()` categorization):
        // the generic buckets are WORK-only; SETUP tasks get their own
        // `setup-`prefixed per-state categories; per-secondary affine GATE
        // tokens are ONE flat `secondary-affine` count (phase-uncounted —
        // their readiness is the per-secondary bitvector, not a global
        // state). Neither setup nor affine inflates the generic `pending`.
        tracing::info!(
            target: IMPORTANT_TARGET,
            baseline_transitions = buffered,
            pending = counts.pending,
            in_flight = counts.in_flight,
            completed = counts.completed,
            failed = counts.failed,
            blocked = counts.blocked,
            skipped_already_done = counts.skipped_already_done,
            setup_pending = counts.setup_pending,
            setup_in_flight = counts.setup_in_flight,
            setup_blocked = counts.setup_blocked,
            setup_failed = counts.setup_failed,
            setup_done = counts.setup_succeeded,
            secondary_affine = counts.secondary_affine,
            "observer mirroring baseline: {} pending / {} in-flight / {} completed / {} failed / {} blocked / {} skipped | setup: {} setup-pending / {} setup-in-flight / {} setup-blocked / {} setup-failed / {} setup-done | {} secondary-affine — narrating live changes from here",
            counts.pending,
            counts.in_flight,
            counts.completed,
            counts.failed,
            counts.blocked,
            counts.skipped_already_done,
            counts.setup_pending,
            counts.setup_in_flight,
            counts.setup_blocked,
            counts.setup_failed,
            counts.setup_succeeded,
            counts.secondary_affine,
        );
    }

    /// Route ONE drained transition by its [`NarrationSource`] — the single
    /// owner of the live-vs-catch-up narration decision (#636-followup).
    ///
    /// - [`NarrationSource::LiveBroadcast`] → narrate INDIVIDUALLY via
    ///   [`Self::narrate_live`] (one operator line per transition).
    /// - [`NarrationSource::CatchUp`] → accumulate into the in-progress
    ///   batch (count + distinct task), emitting NOTHING now; the caller
    ///   emits the ONE summary line via [`Self::flush_catch_up`] when the
    ///   restore batch's terminal package is fully drained.
    ///
    /// Returns whether an operator line was emitted NOW (only the
    /// `LiveBroadcast` path can) — the caller's wake-stream piggyback seam,
    /// exactly like [`Self::narrate_live`].
    pub(crate) fn observe(&mut self, event: &TaskStateChangeEvent) -> bool {
        match event.source {
            NarrationSource::LiveBroadcast => self.narrate_live(event),
            NarrationSource::CatchUp => {
                self.catch_up_transitions += 1;
                self.catch_up_tasks.insert(event.task_id.clone());
                false
            }
        }
    }

    /// Emit the ONE-LINE summary for the just-completed catch-up batch and
    /// RESET the accumulator. Called by the observer when a snapshot-stream
    /// batch's terminal (`done`) package is processed AND the narration
    /// channel has drained (so every `CatchUp` transition the restore
    /// enqueued is already counted — the ordering the coordinator's
    /// flush-on-(pending && channel-empty) gate enforces).
    ///
    /// An EMPTY batch (a `done` over a restore that won no transitions — a
    /// converged re-stream) emits NOTHING and resets, so a quiescent
    /// observer stays silent. Mirrors `narrate_baseline`'s empty-baseline
    /// no-op.
    ///
    /// #662: this NO LONGER emits to [`IMPORTANT_TARGET`]. Under the #653
    /// INCREMENTAL catch-up, a single late-join / recovery catch-up drains as
    /// a STREAM of snapshot-stream packages, each firing one `flush_catch_up`
    /// — so the per-flush "observer caught up: N transitions" line produced a
    /// stream of importance-channel lines ("1 transitions", "2 transitions",
    /// …) interleaved with the 10-min periodic stats: spam under
    /// `--important-stdio-only`. The operator-facing signal is now ONE gated
    /// line folded into the periodic report, emitted ONLY when the observer
    /// has been NOT-CAUGHT-UP for ≥5 min (see
    /// [`crate::observer::reporting::status`]); the per-flush progress is
    /// kept on the FULL log at DEBUG for post-hoc diagnostics. The catch-up
    /// accumulator is still TRACKED + RESET here — its progress feeds the
    /// full-log line and the reset keeps the per-batch count honest. Returns
    /// `false` always: a catch-up flush is no longer a wake-stream host (it
    /// emits nothing on [`IMPORTANT_TARGET`]).
    pub(crate) fn flush_catch_up(&mut self) -> bool {
        let transitions = self.catch_up_transitions;
        let tasks = self.catch_up_tasks.len();
        self.catch_up_transitions = 0;
        self.catch_up_tasks.clear();
        if transitions == 0 {
            return false;
        }
        // FULL-LOG ONLY (no `IMPORTANT_TARGET`): a per-flush diagnostic the
        // operator never sees on the wake stream. The sustained-catch-up
        // status (≥5 min behind) surfaces as the periodic report's folded
        // line instead.
        tracing::debug!(
            catch_up_transitions = transitions,
            catch_up_tasks = tasks,
            "observer catch-up batch applied: {transitions} transitions over {tasks} tasks (from snapshot)",
        );
        false
    }

    /// Narrate ONE live task transition. Emits a single line at the
    /// spec-fixed level. A no-op until [`Self::narrate_baseline`] has armed
    /// live narration (a transition drained before the baseline summary is
    /// folded into the baseline count by the caller, never narrated here).
    /// Returns whether a line was emitted — the caller's wake-stream
    /// piggyback seam (a narrated transition is a wake-stream HOST, exactly
    /// like `RunNarrator::observe`'s return).
    ///
    /// #651 seed-flood guard: a CREATE (no prior state) INTO `pending` —
    /// the initial baseline-seed every task enters at spawn — is NOT a
    /// narration-worthy per-task transition (the converged seed is the
    /// once-per-run `narrate_baseline` / `flush_catch_up` summary's job),
    /// so it returns `false` without emitting. Sits ON TOP of #635's
    /// committed-transition gate (the seed IS a committed transition, but a
    /// CREATE-into-default-state is not operator-meaningful per task) and
    /// preserves #631 field hygiene + #633 role-prefix routing for every
    /// arm that DOES narrate. A RE-entry into pending (a known prior state)
    /// is a real transition and still narrates per-task.
    pub(crate) fn narrate_live(&self, event: &TaskStateChangeEvent) -> bool {
        if !self.baseline_emitted {
            return false;
        }
        let id = &event.task_id;
        let holder = Self::holder_str(event);
        // The `(from {prev}→…)` suffix the transition arms carry, or "" on
        // a CREATE (no prior state to name). One owner for the rendering so
        // every arm shows the from-state uniformly.
        let from = Self::from_suffix(event);
        // The CRDT transaction coordinates — appended to EVERY arm so the
        // operator can correlate any narration line to the originating CRDT
        // change (`crdt_txn=e0.v0.a0`). Formatted once via `TaskTxnId`'s
        // Display.
        let txn = event.txn;
        match &event.change {
            TaskStateChange::Assigned => {
                tracing::info!(
                    target: PER_TASK_TARGET,
                    "task {id} assigned to {holder}{from} crdt_txn={txn}",
                );
            }
            TaskStateChange::Completed => {
                tracing::info!(
                    target: PER_TASK_TARGET,
                    "task {id} completed on {holder}{from} crdt_txn={txn}",
                );
            }
            TaskStateChange::TerminalFailure { reason, last_error } => {
                tracing::error!(
                    target: PER_TASK_TARGET,
                    "task {id} terminally failed on {holder}{from}: {reason} — {last_error} crdt_txn={txn}",
                );
            }
            TaskStateChange::RecoverableFailure { reason } => {
                tracing::warn!(
                    target: PER_TASK_TARGET,
                    "task {id} failed (recoverable) on {holder}{from}: {reason} crdt_txn={txn}",
                );
            }
            TaskStateChange::OomFailure { reason } => {
                tracing::warn!(
                    target: PER_TASK_TARGET,
                    "task {id} failed (oom) on {holder}{from}: {reason} crdt_txn={txn}",
                );
            }
            TaskStateChange::Other { state } => match event.from {
                // The transition is the operator's primary interest for a
                // non-terminal change, so when a prior state is known the
                // line reads "changed state from {prev} to {new}". This
                // includes a RE-entry into pending (a requeue / cascade
                // resume from a known prior state) — a genuine transition
                // the operator wants per-task, distinct from the seed.
                Some(prev) => {
                    tracing::info!(
                        target: PER_TASK_TARGET,
                        "task {id} changed state from {prev} to {state} crdt_txn={txn}",
                    );
                }
                // A CREATE (spawn-time first write) names no prior state.
                //
                // #651 seed-flood guard: a CREATE INTO `pending` is the
                // initial baseline SEED — every task enters its DEFAULT
                // state at spawn (the `e<epoch>.v0.a0` seed batch). At 120k
                // tasks a relocated/late-join observer mirrors the new
                // primary's recompose-seed as 120k LIVE broadcasts; each is
                // a CREATE-into-pending. Narrating one operator line per
                // task makes narration O(tasks-at-seed) — a multi-hour,
                // 300MB+ replay that wedges the observer behind the
                // pending-seed before it can narrate a single completion.
                // Pending is not a meaningful per-task transition for an
                // observer to narrate; the converged seed is reported ONCE
                // by `narrate_baseline` (pre-loop mirror) / `flush_catch_up`
                // (in-loop restore). So a CREATE-into-pending narrates
                // NOTHING — narration stays O(meaningful transitions). Every
                // OTHER CREATE (blocked = a dependency wait,
                // skipped-already-done = a terminal, etc.) is a meaningful
                // first state and DOES narrate.
                None if *state == "pending" => return false,
                None => {
                    tracing::info!(
                        target: PER_TASK_TARGET,
                        "task {id} changed state to {state} crdt_txn={txn}",
                    );
                }
            },
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

    /// The ` (from {prev}→…)` transition suffix the holder-bearing arms
    /// (assign / complete / failure) append, naming the PRE-write state.
    /// Empty on a CREATE (no prior state) so a spawn-time first
    /// assignment reads cleanly with no dangling arrow. The `→…` head of
    /// the rendering is closed by the arm (e.g. assign → "(pending→in-flight)")
    /// — this owns ONLY the from-half; the to-half is the arm's own state.
    /// Returns the FULL parenthesised `(prev→{new})` so every arm renders
    /// the transition identically; the `new` tag is the arm's target state.
    fn from_suffix(event: &TaskStateChangeEvent) -> String {
        match (event.from, Self::to_tag(&event.change)) {
            (Some(prev), Some(new)) => format!(" ({prev}→{new})"),
            // A CREATE, or an arm with no canonical post-tag: no suffix.
            _ => String::new(),
        }
    }

    /// The post-write state tag for the holder-bearing arms, mirroring the
    /// `TaskState::state_tag` the FROM-state carries — so an
    /// `assigned`/`completed`/failed line renders the symmetric
    /// `(from→to)` pair (e.g. `(pending→in-flight)`,
    /// `(in-flight→completed)`, `(in-flight→failed)`). The non-terminal
    /// `Other` arm is handled inline (it owns the explicit
    /// "changed state from X to Y" wording) so it returns `None` here.
    fn to_tag(change: &TaskStateChange) -> Option<&'static str> {
        match change {
            TaskStateChange::Assigned => Some("in-flight"),
            TaskStateChange::Completed => Some("completed"),
            TaskStateChange::TerminalFailure { .. } => Some("failed"),
            TaskStateChange::RecoverableFailure { .. } => Some("failed"),
            TaskStateChange::OomFailure { .. } => Some("failed"),
            // The non-terminal arm renders its own "from {prev} to {state}".
            TaskStateChange::Other { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests;
