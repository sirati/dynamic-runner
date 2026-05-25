//! Primary-side in-flight ledger queries for the promoted secondary.
//!
//! Single concern: maintain `primary_in_flight` / `primary_pending`
//! invariants as items complete or fail. Pool retries are driven
//! at the phase-drain edge by `process_primary_phase_lifecycle`
//! (see `secondary/primary/lifecycle.rs`), which calls the shared
//! retry-bucket core; this module just records the failure into
//! `primary_failed` and hands control to the cascade.

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::PrimaryCommand;

use super::super::SecondaryCoordinator;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Test/inspection helper: number of queued items in the pool.
    /// Returns 0 if the pool isn't initialised yet.
    pub(in crate::secondary) fn primary_pending_len(&self) -> usize {
        self.primary_pending.as_ref().map(|p| p.len()).unwrap_or(0)
    }

    /// Record completion of an item the primary previously
    /// dispatched (via `handle_primary_task_request`). Decrements the
    /// pool's in-flight counter for that item's phase, then promotes
    /// any newly-`Drained` phase to `Done` so dependents can become
    /// `Active`. No-op if the hash wasn't dispatched by this node — a
    /// peer-completion the primary never issued belongs to a
    /// different in-flight ledger and is silently ignored.
    ///
    /// Mirrors `process_phase_lifecycle` on the local primary side: a
    /// single `mark_phase_done` may flip a `Blocked` dependent phase
    /// to `Active`, and that newly-active phase may itself be empty
    /// (dependency chain `0 → 1 → 2 → 3` with all items in phase 3,
    /// or any phase whose only item just completed with no follow-up
    /// items). Loop until no phase is `Drained` and call
    /// `drain_empty_active_phases` each iteration so the cascade
    /// continues all the way to the next populated phase. Without
    /// this loop the primary would stop one phase short and
    /// the next phase's items would sit in the pool with the phase
    /// still `Blocked`.
    pub(in crate::secondary) async fn note_primary_item_completed(
        &mut self,
        file_hash: &str,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        let (phase_id, task_id) = match self.primary_in_flight.remove(file_hash) {
            Some(item) => (item.phase_id, item.binary.task_id),
            None => return,
        };
        // Symmetric with retry: a successful completion supersedes any
        // earlier Recoverable failure recorded against the same hash.
        // Without this, a task that fails Recoverably (lands in
        // `primary_failed`) and then succeeds on a subsequent
        // attempt mid-pass — possible when the operational loop re-
        // dispatches before our drain-check fires — would still
        // trigger a pointless retry pass. Mirrors the live primary's
        // `failed_tasks.remove` in `handle_task_complete`.
        self.primary_failed.remove(file_hash);
        // Per-phase counter bump BEFORE the drain cascade so the
        // `on_phase_end(phase, completed, failed)` callback sees the
        // up-to-date count when this completion is the one that takes
        // the phase to `Drained`. Mirrors
        // `PrimaryCoordinator::note_item_completed`.
        *self
            .primary_phase_completed
            .entry(phase_id.clone())
            .or_insert(0) += 1;
        if let Some(pool) = self.primary_pending.as_mut() {
            pool.on_item_finished(&phase_id, Some(task_id.as_str()));
        }
        // Cascade drain + fire the registered `on_phase_end` /
        // `on_phase_start` callbacks (no-ops when no callback is
        // registered or `primary_pending` is `None`). See
        // `secondary/primary/lifecycle.rs` for the cascade semantics
        // and the `command_rx` thread-through rationale.
        self.process_primary_phase_lifecycle(command_rx).await;
    }

    /// Sibling to `note_primary_item_completed` for the failure path.
    /// Decrements the pool's in-flight counter for the item's phase
    /// (same as completion — phase machine doesn't distinguish
    /// success vs failure for in-flight bookkeeping). Stash the
    /// binary + error in `primary_failed` regardless of error
    /// class — mirrors the live primary's `failed_tasks.insert(...)`
    /// step in `task/failed.rs`. The per-phase retry-bucket cascade
    /// fired by `process_primary_phase_lifecycle` partitions the
    /// ledger by bucket kind (Recoverable / OOM); error types not
    /// matched by any bucket (`NonRecoverable`, `Unfulfillable`,
    /// non-memory `ResourceExhausted`) survive in the ledger
    /// permanently and surface through the run's outcome summary
    /// as `fail_final`. Same partition the live primary applies.
    ///
    /// Tasks not in `primary_in_flight` (i.e. not dispatched by this
    /// secondary as primary — e.g. peer-completion forwards
    /// for tasks dispatched elsewhere, or initial-assignment failures
    /// from the live-primary's pre-promotion authority) bypass both
    /// the ledger and the pool decrement: those tasks were never on
    /// this pool's books to begin with. Mirrors `note_primary_item_completed`'s
    /// silent-skip behaviour for unknown hashes.
    ///
    /// Called from every wire-arrival site that observes a TaskFailed
    /// for a primary-dispatched task: peer.rs (peer transport),
    /// processing.rs (own worker event), dispatch.rs (live-primary
    /// forward path). The retry-bucket partition is applied at the
    /// drain edge inside `process_primary_phase_lifecycle`; callers
    /// don't have to know which classes are retriable.
    pub(in crate::secondary) async fn note_primary_item_failed(
        &mut self,
        file_hash: &str,
        error_type: &dynrunner_core::ErrorType,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        let item = match self.primary_in_flight.remove(file_hash) {
            Some(item) => item,
            None => return,
        };
        let phase_id = item.phase_id.clone();
        // `task_id` is non-optional per the framework's boundary
        // contract; keep as `String` and convert to `&str` at the
        // pool call below. Behaviour preserved verbatim — the prior
        // code took `Option<String> -> Option<&str>` and passed it
        // through; with the new contract that's a `Some(id)` always.
        let task_id = item.binary.task_id.clone();
        // Mirror the live-primary's `failed_tasks.insert` step:
        // every error class lands in the ledger; the retry-bucket
        // cascade in `process_primary_phase_lifecycle` decides
        // which classes get a second-chance dispatch. Idempotent
        // — re-arrival overwrites with the latest ErrorType.
        self.primary_failed.insert(
            file_hash.to_string(),
            crate::secondary::FailedTaskEntry {
                binary: item.binary,
                error_type: error_type.clone(),
            },
        );
        // Per-phase counter bump BEFORE the drain cascade so the
        // `on_phase_end(phase, completed, failed)` callback observes
        // the failure when this failure is the one that takes the
        // phase to `Drained`. Mirrors
        // `PrimaryCoordinator::note_item_failed`.
        *self
            .primary_phase_failed
            .entry(phase_id.clone())
            .or_insert(0) += 1;
        if let Some(pool) = self.primary_pending.as_mut() {
            pool.on_item_finished(&phase_id, Some(task_id.as_str()));
        }
        self.process_primary_phase_lifecycle(command_rx).await;
    }

    /// Test/inspection helper: whether the pool has zero queued items.
    /// Treats "no pool yet" as empty so resource-loop predicates don't
    /// have to special-case the pre-snapshot state.
    pub(in crate::secondary) fn primary_pending_is_empty(&self) -> bool {
        self.primary_pending
            .as_ref()
            .map(|p| p.is_empty())
            .unwrap_or(true)
    }

    /// Eligibility predicate for the **promoted-primary natural-quiesce
    /// `RunComplete` broadcast** branch in `process_tasks`.
    ///
    /// Returns true exactly when every gate the branch requires
    /// holds: this secondary acts as primary, the demoted primary is
    /// still alive (the dead-demoted path has its own sibling branch),
    /// the local primary-side ledger is drained, the cluster-wide
    /// ledger has converged on a terminal partition
    /// (`task_count() > 0 && pending == 0 && in_flight == 0`), the
    /// post-promotion settle period has elapsed, and the
    /// `RunComplete` flag is not already set on the local
    /// cluster-state mirror.
    ///
    /// Single concern: the eligibility decision. The branch's side
    /// effects (apply `RunComplete`, fan out the broadcast, flush
    /// the primary transport) stay in `process_tasks`. Lifted to a
    /// method so the test suite can pin the gate semantics without
    /// driving the full operational `select!` loop.
    ///
    /// See `SecondaryCoordinator::promoted_at` and
    /// `SecondaryConfig::promoted_primary_quiesce_grace` for the
    /// settle-period gate's rationale (asm-dataset-nix T11
    /// regression: a promoted secondary fires `RunComplete` on a
    /// partial CRDT mirror and strands the in-flight remainder).
    pub(in crate::secondary) fn promoted_primary_natural_quiesce_eligible(
        &self,
    ) -> bool {
        if !self.is_primary
            || self.primary_disconnected
            || self.cluster_state.run_complete()
        {
            return false;
        }
        // Local-pool drained: gate (a) in the call-site comment.
        let local_drained = self.primary_in_flight.is_empty()
            && self.active_tasks.is_empty()
            && self.primary_pending_is_empty()
            && (self.primary_failed.is_empty()
                || !self.primary_retry_budget.should_retry());
        // Cluster-wide CRDT terminal partition: gate (b).
        let cluster_counts = self.cluster_state.counts();
        let cluster_quiesced = self.cluster_state.task_count() > 0
            && cluster_counts.pending == 0
            && cluster_counts.in_flight == 0;
        // Post-promotion settle period elapsed: gate (c). `None` is
        // treated as "not yet settled" so a code path that flips
        // `is_primary` without stamping `promoted_at` fails closed.
        let grace = self.config.promoted_primary_quiesce_grace;
        let promotion_settled = self
            .promoted_at
            .is_some_and(|t| std::time::Instant::now().duration_since(t) >= grace);
        local_drained && cluster_quiesced && promotion_settled
    }
}
