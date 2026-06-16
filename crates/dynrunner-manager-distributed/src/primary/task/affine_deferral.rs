//! Affine-deferral report handling — the primary half of the
//! SecondaryAffine local-import gate (#497).
//!
//! A work task `B` assigned to a secondary may depend on a SecondaryAffine
//! gate `I` that is RESOLVED (`AffineReady`) but whose per-secondary import
//! that secondary has not yet run locally. The secondary DEFERS `B`: it
//! parks `B` in its node-local `affine_running` queue (NOT `active_tasks`)
//! behind the single in-flight import and REPORTS
//! [`DistributedMessage::TaskQueuedAfterLocalDependency`]. When the import
//! finishes the secondary self-dispatches `B` onto its worker and REPORTS
//! [`DistributedMessage::LocalDependencyReleased`]. The secondary REPORTS;
//! the primary ORIGINATES the CRDT transitions (the work-split law) — these
//! two handlers are that origination.
//!
//! ## Why the in-flight ledger is the load-bearing piece
//!
//! `B` was assigned through the normal dispatch path
//! (`commit_assignment` → `originate_task_assigned`), so on the primary it
//! is `InFlight` in the CRDT AND tracked in `self.in_flight` (the hash-keyed
//! ledger) with its holding worker slot `Assigned`. The deferral handler
//! KEEPS `B` in the ledger and flips its `InFlightEntry::deferred` flag to
//! `true` (the slot stays `Assigned`, its type slot reserved), so EVERY
//! terminal/recovery path that resolves BY HASH stays symmetric across the
//! defer:
//!   * an affine-import FAILURE terminal (`TaskFailed`) flows through the
//!     normal `free_slot_on_terminal` → frees the slot, releases the type
//!     slot, removes the ledger entry, and runs `note_item_failed` — the
//!     phase in-flight counter stays correct;
//!   * a DEAD holder is recovered by `recover_inflight_for_dead_secondary`,
//!     which iterates the ledger by secondary and requeues `B` (not
//!     stranded in `QueuedAfterLocalDependency`).
//!
//! The ONE consumer that must NOT see a deferred entry is the
//! reconciliation probe, whose view is built EXCLUSIVELY from
//! `self.in_flight` (`reconciliation_probe.rs`) and whose verdict means
//! "the holder is not running this": a deferred dependent is genuinely NOT
//! awaiting a terminal — it is parked behind a local import the holder
//! intends to run — so the holder's `holds_task` (reading only
//! `active_tasks` + `pending_first_bind`) would deny it, the probe would
//! return `Lost`, and the task would requeue onto the same affine
//! secondary, which defers it again: an unbounded ~600s requeue+re-park
//! loop that re-originates `InFlight` and leaks the coordinator. The probe
//! therefore EXCLUDES `deferred = true` entries from its view (the only
//! behaviour the pre-symmetry `in_flight.remove` was protecting), so the
//! loop cannot start without blinding the other paths.
//!
//! The holding worker slot is deliberately LEFT `Assigned` to `B` (and its
//! per-type concurrency slot LEFT reserved) across the defer: the
//! secondary's worker is genuinely occupied — first by the gate body, then
//! by `B` itself on release — so vacating it on the primary would let the
//! scheduler over-dispatch a second task onto a busy worker (the #517
//! bounce hazard). Keeping the slot held makes the defer→release→terminal
//! arc symmetric: the eventual `TaskComplete`/`TaskFailed` frees the slot,
//! the ledger entry, and the type slot in one `free_slot_on_terminal` — the
//! same inverse `commit_assignment` would have had.
//!
//! ## The release half does NOT re-push a `TaskAssignment`
//!
//! The secondary SELF-DISPATCHES `B` onto its worker the moment its import
//! completes (`affine_exec::complete_affine_import` →
//! `dispatch_released_affine_dependent`) — the deferred assignment the gate
//! withheld. The `LocalDependencyReleased` report therefore only asks the
//! primary to move the CRDT/ledger state back to `InFlight`; it must NOT
//! re-send a `TaskAssignment` (that would double-dispatch). The handler
//! re-originates the EXISTING `TaskAssigned` (the standard `→ InFlight`
//! choke point, `originate_task_assigned`) and clears `B`'s `deferred`
//! flag — `B` never left `self.in_flight`, so the probe simply sees it
//! again against the slot it always held.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage, MessageType};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::cluster_state::TaskState;
use crate::primary::PrimaryCoordinator;
use crate::primary::command_channel::PrimaryCommand;

/// Is `ty` an affine-deferral REPORT — a `TaskQueuedAfterLocalDependency`
/// or a `LocalDependencyReleased`? These are the two message types whose
/// per-report handler each originates ONE `Destination::All` broadcast via
/// `apply_and_broadcast_cluster_mutations`; coalescing them is the concern
/// this module owns (see `dispatch_inbox_batch_coalescing_deferrals`).
fn is_deferral_report(ty: MessageType) -> bool {
    matches!(
        ty,
        MessageType::TaskQueuedAfterLocalDependency | MessageType::LocalDependencyReleased
    )
}

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Dispatch a heterogeneous inbox-drain batch, COALESCING the
    /// `Destination::All` broadcasts of any contiguous run of affine-
    /// deferral reports into ONE wire frame per run (#-cascade Commit 3).
    ///
    /// ## The concern
    ///
    /// A `build_compilers` affine burst lands as S secondaries × M
    /// dependents of `TaskQueuedAfterLocalDependency` / `LocalDependency-
    /// Released` reports back-to-back. Pre-fix each report's handler called
    /// `apply_and_broadcast_cluster_mutations` directly — one un-coalesced
    /// `Destination::All` broadcast PER report, serial on the oploop —
    /// flooding ingest cluster-wide and tripping the self-starvation
    /// false-election that seeds the failover cascade. (Before #497 these
    /// reports were debug-dropped, i.e. free.)
    ///
    /// ## The seam reused (mirrors #547 / the F5 atomic batch)
    ///
    /// The mutation-capture primitive (`begin_mutation_capture` /
    /// `take_mutation_capture` / `broadcast_applied_mutations`) DIVERTS
    /// every `apply_and_broadcast_cluster_mutations` call off the wire into
    /// a buffer while armed, and the owner flushes the accumulated batch as
    /// ONE frame. This method opens ONE capture window around each MAXIMAL
    /// CONTIGUOUS run of deferral reports, dispatches each report through
    /// the canonical `dispatch_message` (so every report's LOCAL apply +
    /// ledger drop happen identically), then flushes the run's accumulated
    /// mutations once. A `build_compilers` burst = O(1) broadcasts per
    /// inbox-drain run, not O(N).
    ///
    /// ## Why CONTIGUOUS runs, not a blanket window
    ///
    /// Order is preserved EXACTLY — the capture only defers the WIRE leg of
    /// the deferral handlers to the end of their own contiguous run; a run
    /// is broken at the first non-deferral frame, which is dispatched with
    /// normal per-call broadcast semantics. No frame is reordered relative
    /// to another, so causal ordering across types is untouched. A blanket
    /// window would also wrongly capture other handlers' broadcasts (and
    /// risk a nested capture from a handler that opens its own) — capture is
    /// NOT re-entrant. Deferral handlers open no capture of their own, so a
    /// run-scoped window is safe.
    pub(crate) async fn dispatch_inbox_batch_coalescing_deferrals(
        &mut self,
        batch: Vec<DistributedMessage<I>>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) -> Result<(), String> {
        let mut iter = batch.into_iter().peekable();
        while let Some(msg) = iter.next() {
            if !is_deferral_report(msg.msg_type()) {
                // Non-deferral frame: normal per-call broadcast semantics.
                self.dispatch_message(msg, command_rx).await?;
                continue;
            }
            // Start of a contiguous deferral run: open ONE capture window,
            // dispatch this report and every immediately-following deferral
            // report into it, then flush the run's mutations as ONE frame.
            self.begin_mutation_capture();
            self.dispatch_message(msg, command_rx).await?;
            while iter
                .peek()
                .is_some_and(|next| is_deferral_report(next.msg_type()))
            {
                let next = iter.next().expect("peeked Some");
                self.dispatch_message(next, command_rx).await?;
            }
            let coalesced = self.take_mutation_capture();
            self.broadcast_applied_mutations(coalesced).await;
        }
        Ok(())
    }

    /// A secondary reported that a work task `B` is now QUEUED behind its
    /// local SecondaryAffine import (#497). ORIGINATE
    /// `QueuedAfterLocalDependencySet` (the CRDT `InFlight | Pending →
    /// QueuedAfterLocalDependency` rank-drop) AND mark `B`'s ledger entry
    /// `deferred = true` so the reconciliation probe — the ONE consumer
    /// whose verdict would be wrong for a parked dependent — excludes it
    /// from its view (the never-wired-handler loop the brief pins).
    ///
    /// `B` STAYS in `self.in_flight`: its holding worker slot + per-type
    /// concurrency reservation are LEFT intact (the secondary's worker is
    /// occupied — gate body, then `B`), and the entry remains resolvable
    /// BY HASH so the terminal path (`free_slot_on_terminal` on an affine-
    /// import failure) and the dead-holder recovery
    /// (`recover_inflight_for_dead_secondary`) stay symmetric across the
    /// defer. The release half clears the flag against that same slot; the
    /// eventual terminal frees slot + type slot + ledger entry through
    /// `free_slot_on_terminal`.
    pub(crate) async fn handle_task_queued_after_local_dependency(
        &mut self,
        msg: DistributedMessage<I>,
    ) {
        let DistributedMessage::TaskQueuedAfterLocalDependency {
            secondary_id,
            task_hash,
            affine_hash,
            ..
        } = msg
        else {
            return;
        };
        tracing::debug!(
            secondary = %secondary_id,
            task_hash = %task_hash,
            affine_hash = %affine_hash,
            "affine-deferral report: work task queued behind a secondary's \
             local import; parking it as QueuedAfterLocalDependency and \
             marking its ledger entry deferred (probe excludes it; terminal \
             + dead-holder recovery still resolve it by hash)"
        );
        // ORIGINATE the rank-drop (the secondary reported; we originate —
        // the work-split law). The apply gates on `InFlight | Pending`; any
        // other state (a terminal that already settled `B`, or an idempotent
        // re-report) NoOps inside the apply.
        self.apply_and_broadcast_cluster_mutations(vec![
            ClusterMutation::QueuedAfterLocalDependencySet {
                hash: task_hash.clone(),
                secondary: secondary_id,
            },
        ])
        .await;
        // Mark `B`'s ledger entry deferred: the reconciliation probe
        // EXCLUDES `deferred = true` from its view, so this is THE step that
        // stops the probe looping on a parked dependent — without dropping
        // the entry, which would blind the terminal + dead-holder-recovery
        // paths (the slot↔ledger symmetry the SecondaryAffine deferral broke).
        // The slot stays `Assigned` and its type slot stays reserved (see the
        // module doc). A miss (the entry already settled / was recovered) is a
        // safe no-op — there is nothing parked to mark.
        if let Some(entry) = self.in_flight.get_mut(&task_hash) {
            entry.deferred = true;
        }
    }

    /// A secondary reported that its local SecondaryAffine import for a
    /// queued work task `B` is DONE — release it (#497). RE-ORIGINATE the
    /// EXISTING `TaskAssigned` (the standard `→ InFlight` choke point) so
    /// `B` transitions `QueuedAfterLocalDependency → InFlight`, and CLEAR
    /// `B`'s `deferred` flag against the ledger entry it never left.
    ///
    /// Does NOT re-push a `TaskAssignment`: the secondary already self-
    /// dispatched `B` onto its worker the moment its import completed (see
    /// the module doc). The handler only re-establishes the CRDT `InFlight`
    /// fact and un-defers the ledger entry so the reconciliation probe (and
    /// the death seam) cover `B` again.
    pub(crate) async fn handle_local_dependency_released(
        &mut self,
        msg: DistributedMessage<I>,
    ) {
        let DistributedMessage::LocalDependencyReleased {
            secondary_id,
            task_hash,
            worker_id,
            ..
        } = msg
        else {
            return;
        };
        // A CRDT miss means `B` already settled / was recovered — nothing to
        // re-establish (the ledger entry is gone too, freed in lockstep).
        if self.cluster_state.task_state(&task_hash).is_none() {
            tracing::debug!(
                secondary = %secondary_id,
                task_hash = %task_hash,
                "affine-release report for a hash absent from the ledger \
                 (already settled / recovered); no-op"
            );
            return;
        }
        tracing::debug!(
            secondary = %secondary_id,
            task_hash = %task_hash,
            worker_id,
            "affine-release report: secondary's local import done and it self-\
             dispatched the dependent; re-originating TaskAssigned and \
             un-deferring the in-flight ledger entry"
        );
        // Re-originate the EXISTING `TaskAssigned` (QueuedAfterLocalDependency
        // → InFlight; a freshly-minted higher version dominates the queued
        // entry in the join). The standard `→ InFlight` choke point — NOT a
        // second InFlight originator.
        self.originate_task_assigned(task_hash.clone(), secondary_id.clone(), worker_id)
            .await;
        // Only un-defer if the transition actually took (a raced terminal that
        // landed first leaves `B` non-InFlight — the `TaskAssigned` apply NoOps
        // and the terminal already freed the ledger entry).
        if !matches!(
            self.cluster_state.task_state(&task_hash),
            Some(TaskState::InFlight { .. })
        ) {
            tracing::debug!(
                secondary = %secondary_id,
                task_hash = %task_hash,
                "affine-release re-origination did not land InFlight (a \
                 terminal raced it); leaving the ledger as the terminal left it"
            );
            return;
        }
        // Clear the `deferred` flag against the slot `B` never left: the slot
        // is still `Assigned` to this hash and its type slot still reserved
        // (the defer left both intact, and `B` never left the ledger), so the
        // reconciliation probe and the death seam cover `B` again — a pure
        // flag flip, NO `reserve_type_slot` (which would double-count).
        if let Some(entry) = self.in_flight.get_mut(&task_hash) {
            entry.deferred = false;
        }
    }
}
