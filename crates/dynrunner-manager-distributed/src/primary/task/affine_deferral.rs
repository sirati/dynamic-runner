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
//! ledger) with its holding worker slot `Assigned`. The reconciliation
//! probe builds its view EXCLUSIVELY from `self.in_flight`
//! (`reconciliation_probe.rs`): a task that stays in the ledger past the
//! reconciliation deadline is probed at its holder, and the holder's
//! `holds_task` reads only `active_tasks` + `pending_first_bind` — a parked
//! dependent lives in NEITHER, so the holder denies it, the probe returns
//! `Lost`, and the task is requeued onto the same affine secondary, which
//! defers it again: an unbounded ~600s requeue+re-park loop that re-
//! originates `InFlight` and leaks the coordinator. The deferral handler
//! REMOVES `B` from `self.in_flight` (a deferred dependent is genuinely NOT
//! awaiting a terminal — it is parked behind a local import), so the probe
//! never sees it and the loop cannot start.
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
//! choke point, `originate_task_assigned`) and re-enters `B` into
//! `self.in_flight` against the slot it never left.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage, MessageType};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::cluster_state::TaskState;
use crate::primary::PrimaryCoordinator;
use crate::primary::command_channel::PrimaryCommand;
use crate::primary::coordinator::InFlightEntry;

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
    /// QueuedAfterLocalDependency` rank-drop) AND remove `B` from
    /// `self.in_flight` so the reconciliation probe — which views ONLY the
    /// ledger — stops treating the parked dependent as a task awaiting a
    /// terminal (the never-wired-handler loop the brief pins).
    ///
    /// The holding worker slot + its per-type concurrency reservation are
    /// LEFT intact: the secondary's worker is occupied (gate body, then
    /// `B`), so the slot must stay `Assigned` to avoid over-dispatch; the
    /// release half re-enters the ledger against that same slot, and the
    /// eventual terminal frees both through `free_slot_on_terminal`.
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
             dropping it from the in-flight ledger (probe no longer sees it)"
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
        // Drop `B` from the in-flight ledger: the reconciliation probe's
        // view is built solely from `self.in_flight`, so this is THE step
        // that stops the probe looping on a parked dependent. The slot stays
        // `Assigned` and its type slot stays reserved (see the module doc) —
        // the release re-enters the ledger, the terminal frees the slot.
        self.in_flight.remove(&task_hash);
    }

    /// A secondary reported that its local SecondaryAffine import for a
    /// queued work task `B` is DONE — release it (#497). RE-ORIGINATE the
    /// EXISTING `TaskAssigned` (the standard `→ InFlight` choke point) so
    /// `B` transitions `QueuedAfterLocalDependency → InFlight`, and re-enter
    /// it into `self.in_flight` against the worker slot it never left.
    ///
    /// Does NOT re-push a `TaskAssignment`: the secondary already self-
    /// dispatched `B` onto its worker the moment its import completed (see
    /// the module doc). The handler only re-establishes the CRDT/ledger
    /// `InFlight` fact so the task is tracked + death-seam-covered again.
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
        // Read `B`'s carried `TaskInfo` + phase from the CRDT BEFORE the
        // origination (the queued entry holds it). A miss means `B` already
        // settled / was recovered — nothing to re-establish.
        let Some((task, phase)) = self
            .cluster_state
            .task_state(&task_hash)
            .map(|s| (s.task().clone(), s.task().phase_id.clone()))
        else {
            tracing::debug!(
                secondary = %secondary_id,
                task_hash = %task_hash,
                "affine-release report for a hash absent from the ledger \
                 (already settled / recovered); no-op"
            );
            return;
        };
        tracing::debug!(
            secondary = %secondary_id,
            task_hash = %task_hash,
            worker_id,
            "affine-release report: secondary's local import done and it self-\
             dispatched the dependent; re-originating TaskAssigned and re-\
             entering the in-flight ledger"
        );
        // Re-originate the EXISTING `TaskAssigned` (QueuedAfterLocalDependency
        // → InFlight; a freshly-minted higher version dominates the queued
        // entry in the join). The standard `→ InFlight` choke point — NOT a
        // second InFlight originator.
        self.originate_task_assigned(task_hash.clone(), secondary_id.clone(), worker_id)
            .await;
        // Only re-enter the ledger if the transition actually took (a raced
        // terminal that landed first leaves `B` non-InFlight — the
        // `TaskAssigned` apply NoOps and there is nothing in flight to track).
        if !matches!(
            self.cluster_state.task_state(&task_hash),
            Some(TaskState::InFlight { .. })
        ) {
            tracing::debug!(
                secondary = %secondary_id,
                task_hash = %task_hash,
                "affine-release re-origination did not land InFlight (a \
                 terminal raced it); not re-entering the ledger"
            );
            return;
        }
        // Re-enter the in-flight ledger against the slot `B` never left: the
        // slot is still `Assigned` to this hash and its type slot is still
        // reserved (the defer left both intact), so this is a pure ledger
        // re-entry — NO `reserve_type_slot`, which would double-count.
        self.in_flight.insert(
            task_hash,
            InFlightEntry {
                phase,
                secondary_id,
                local_worker_id: Some(worker_id),
                task,
            },
        );
    }
}
