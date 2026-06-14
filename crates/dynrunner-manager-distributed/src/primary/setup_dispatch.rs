//! Primary-side SELECTION + terminal handling for `TaskKind::Setup` tasks.
//!
//! ## The one concern
//! Drive the setup-task lifecycle from the AUTHORITY side: pick each
//! `Setup`-kind task whose executor-affinity member is connected, route it
//! to that member's in-process executor, and turn the executor's outcome
//! into the authoritative CRDT terminal. A setup task is invisible to every
//! worker-dispatch path (the scheduling seam); this module is the ONLY thing
//! that ever moves one out of `Pending`.
//!
//! ## Module boundary (CLAUDE.md design-first)
//!   * Owner: the primary. The two seams it crosses are (a) the existing
//!     worker-management reaction (`react_to_worker_signal_batch` calls
//!     [`Self::dispatch_setup_tasks`] right after the worker recheck — a
//!     `TasksAdded` covers a new setup task entering the pool exactly as it
//!     covers a new work task), and (b) the existing primary inbound router
//!     (`connect.rs::dispatch_message` calls [`Self::handle_setup_terminal`]
//!     for an off-primary executor's `SetupTerminal` report).
//!   * API the callers see: TWO one-line delegates — "service setup
//!     dispatch" and "ingest a setup terminal". Neither caller learns how a
//!     setup task is selected, executed, or accounted; no `if kind == Setup`
//!     in the router or the loop.
//!   * What crosses to the EXECUTOR member: the directed
//!     `DistributedMessage::SetupAssignment { task_hash }` (the member reads
//!     the `TaskInfo` from its own replicated `cluster_state`); the executor
//!     reports back via `DistributedMessage::SetupTerminal`.
//!
//! ## Death coverage (reused, not re-built)
//! An off-primary setup task is committed `Pending → InFlight` through the
//! EXISTING `originate_task_assigned` choke point and recorded in the
//! primary's `in_flight` ledger with NO worker slot
//! (`local_worker_id: None`) and NO type-slot reservation (a setup task
//! never runs on a worker, so it must not consume worker-type concurrency
//! budget). The executor-death seam
//! (`coordinator::recover_inflight_for_dead_secondary`) already drives a
//! non-reassignable `InFlight` entry to `TaskFailed { NonRecoverable }` on
//! its holder's death — so death coverage is inherited for free.
//!
//! ## On-primary self-exec
//! When the affinity is the primary itself (or `None`, which defaults to the
//! primary), the task is executed SYNCHRONOUSLY here and its terminal
//! originated directly — no wire frame, no `in_flight` ledger entry (a
//! synchronous self-exec has no death window).

use dynrunner_core::{ErrorType, Identifier, PhaseId};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, PeerId,
};
use dynrunner_scheduler_api::{DispatchRank, ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::cluster_state::PeerMembership;
use crate::primary::PrimaryCoordinator;
use crate::primary::command_channel::PrimaryCommand;
use crate::primary::coordinator::InFlightEntry;
use crate::primary::wire::{compute_task_hash, timestamp_now};
use crate::setup_exec::{SetupOutcome, execute_setup_with_upload};

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Service the setup-task dispatch pass: route every dispatchable
    /// `Setup`-kind task whose affinity member is connected.
    ///
    /// Called from the worker-management reaction's `TasksAdded` branch
    /// (the same recheck that dispatches work tasks to idle workers). A
    /// setup task whose affinity member is not yet a live cluster member is
    /// SKIPPED this pass and retried on the next `TasksAdded` (it stays
    /// queued, holding its phase open) — exactly mirroring how a work task
    /// with no idle worker is left queued.
    ///
    /// Coalesced + idempotent: the pass drains every currently-routable
    /// setup task; one is routed at most once (it is removed from the pool
    /// on route). Re-running the pass with nothing routable is a cheap no-op.
    pub(crate) async fn dispatch_setup_tasks(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        // Pull each routable setup task in turn. The routability predicate
        // reads cluster MEMBERSHIP, but `take_first_match` borrows the pool
        // mutably — so the closure cannot also borrow `&self`. Resolve the
        // membership decision INSIDE the closure against `&self.cluster_state`
        // via a raw-pointer-free split: capture the primary's own id by
        // value, and call the `&self`-free membership helper through a
        // pre-borrowed reference to `cluster_state`. We borrow `cluster_state`
        // and `pool` from the SAME `&mut self` by going through the typed
        // field accessors in two separate statements (membership first into a
        // routable-id set, then the pool take), so there is no simultaneous
        // aliasing borrow.
        loop {
            let own_id = self.config.node_id.clone();
            // Snapshot the routability of each queued setup task's affinity
            // against the live membership, THEN take by hash. Two-phase to
            // avoid borrowing `cluster_state` (membership) and the pool
            // simultaneously inside one closure.
            let routable_hash = self
                .pool_ref_setup_routable(&own_id);
            let Some(hash) = routable_hash else {
                break;
            };
            // Remove exactly that task from the pool by hash.
            let Some(task) = self
                .pool_mut()
                .take_first_match(|t| compute_task_hash(t) == hash)
            else {
                // Raced away (e.g. retain dropped it). Stop the pass; the
                // next `TasksAdded` re-evaluates.
                break;
            };
            let affinity = task.setup_affinity.clone();
            if affinity.as_deref().is_none_or(|a| a == own_id) {
                // ON-PRIMARY self-exec: run synchronously, originate the
                // terminal directly. No `in_flight` ledger entry (no death
                // window) and no wire frame.
                self.execute_setup_locally(task, command_rx).await;
            } else {
                // OFF-PRIMARY: commit `Pending → InFlight` (death-seam
                // coverage) and send the directed assignment.
                self.assign_setup_off_primary(task, affinity.expect("checked Some above"))
                    .await;
            }
        }
    }

    /// Pick the routable queued setup task whose dependents we most want to
    /// start next — the upload whose gated WORK tasks have the BEST would-be
    /// dispatch standing — returning its content hash. `None` when no queued
    /// setup task is currently routable.
    ///
    /// PRIORITY, not FIFO (#336 P3): among the currently-routable setup tasks
    /// (the routability gate is UNCHANGED — [`Self::is_setup_routable`]) the
    /// pick is `min-by` the pool's [`dependent_dispatch_rank`], so an upload
    /// feeding a dispatch-imminent build (transitively, through a #497
    /// `SecondaryAffine` import gate) uploads ahead of one feeding a late
    /// phase. A setup task with no known dependent yet ranks
    /// [`DispatchRank::WORST`] → it routes LAST (deferred until a dependent
    /// spawns, never starved: it re-ranks the moment one appears). This only
    /// ROUTES a setup task — it never drops one, so no dependent is stranded.
    ///
    /// [`dependent_dispatch_rank`]: dynrunner_scheduler_api::PendingPool::dependent_dispatch_rank
    ///
    /// Reads cluster membership (`&self.cluster_state`) and the pool's queued
    /// view (`&self.cluster_state`-independent) in ONE `&self` borrow, so the
    /// caller can then take the task by hash without a borrow conflict.
    fn pool_ref_setup_routable(&self, own_id: &str) -> Option<String> {
        self.pool()
            .iter()
            .filter(|task| task.kind.is_setup() && self.is_setup_routable(task, own_id))
            .min_by_key(|task| {
                self.pool()
                    .dependent_dispatch_rank(&task.task_id)
                    .unwrap_or(DispatchRank::WORST)
            })
            .map(compute_task_hash)
    }

    /// Whether a setup task's executor affinity is routable right now: unset
    /// / self affinity is always routable; an explicit affinity is routable
    /// iff it is a live replicated member. The routability gate shared by the
    /// priority picker ([`Self::pool_ref_setup_routable`]) and its tests —
    /// extracted so the gate has ONE owner (the pick changed from FIFO to
    /// min-by-rank; the gate did not).
    fn is_setup_routable(&self, task: &dynrunner_core::TaskInfo<I>, own_id: &str) -> bool {
        match task.setup_affinity.as_deref() {
            None => true,
            Some(a) if a == own_id => true,
            Some(a) => self.cluster_state.peer_membership(a) == PeerMembership::AliveMember,
        }
    }

    /// Run a setup task IN-PROCESS on the primary and originate its terminal.
    ///
    /// Synchronous: the action runs to completion inside this call, so there
    /// is no in-flight death window — the terminal is originated immediately.
    /// The pool's phase in-flight is bumped around the run so the phase cannot
    /// drain mid-exec, then settled by [`Self::settle_setup_terminal`].
    async fn execute_setup_locally(
        &mut self,
        task: dynrunner_core::TaskInfo<I>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        let hash = compute_task_hash(&task);
        let phase = task.phase_id.clone();
        let task_id = task.task_id.clone();
        self.pool_mut().mark_in_flight(&phase);
        // The shared executor path (#336 P1): an upload-ref task uploads via
        // the registered action; a no-ref task keeps the #489 no-op success.
        let outcome = execute_setup_with_upload(&task, &self.upload_action).await;
        self.settle_setup_terminal(&hash, &phase, &task_id, outcome, command_rx)
            .await;
    }

    /// Commit an off-primary setup task `Pending → InFlight` and send its
    /// directed `SetupAssignment` to the affinity member.
    ///
    /// The `in_flight` ledger entry carries NO worker slot and reserves NO
    /// type slot (a setup task never runs on a worker). The CRDT transition
    /// rides the EXISTING `originate_task_assigned` choke point so the
    /// member's mirror also moves the task to `InFlight`; the death seam then
    /// covers it. On a send failure we roll the ledger + CRDT back to
    /// `Pending` (via the existing requeue mutation) and requeue the binary,
    /// so a failed send never strands the task.
    async fn assign_setup_off_primary(
        &mut self,
        task: dynrunner_core::TaskInfo<I>,
        member: String,
    ) {
        let hash = compute_task_hash(&task);
        let phase = task.phase_id.clone();
        // Ledger commit: a setup task's holder is its affinity MEMBER; the
        // entry carries no worker slot (`local_worker_id: None` — the
        // documented defensive arm) and we do NOT reserve a type slot.
        self.pool_mut().mark_in_flight(&phase);
        self.in_flight.insert(
            hash.clone(),
            InFlightEntry {
                phase: phase.clone(),
                secondary_id: member.clone(),
                local_worker_id: None,
                task,
            },
        );
        // CRDT `Pending → InFlight` via the single origination choke point
        // (worker id 0 is a placeholder — a setup task has no worker; the
        // CRDT `InFlight.worker` field is not consulted for a
        // non-reassignable setup task, and the death seam keys on the holder
        // secondary).
        self.originate_task_assigned(hash.clone(), member.clone(), 0)
            .await;
        let assignment = DistributedMessage::SetupAssignment {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: member.clone(),
            task_hash: hash.clone(),
        };
        if let Err(e) = self
            .send_to(
                Destination::Secondary(PeerId::from(member.clone())),
                assignment,
            )
            .await
        {
            tracing::warn!(
                member = %member,
                task_hash = %hash,
                error = %e,
                "setup-assignment send failed; rolling back the InFlight commit and requeuing"
            );
            // Roll the ledger + CRDT back to `Pending` (the existing requeue
            // mutation drops the InFlight), then requeue the binary for a
            // later pass. Symmetric inverse of the commit above.
            if let Some(entry) = self.in_flight.remove(&hash) {
                self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::TaskRequeued {
                    hash: hash.clone(),
                    version: Default::default(),
                }])
                .await;
                self.pool_mut().requeue(entry.task);
            }
            return;
        }
        tracing::info!(
            member = %member,
            task_hash = %hash,
            "setup task assigned to its in-process executor member"
        );
    }

    /// Ingest a `SetupTerminal` report from an OFF-PRIMARY executor and
    /// settle the authoritative CRDT terminal.
    ///
    /// The setup-task counterpart of the worker terminal ingest, kept
    /// SEPARATE because a setup task has no worker slot / type slot /
    /// `completed_tasks` membership — threading it through the worker
    /// terminal handlers would scatter setup special-casing there. Frees the
    /// `in_flight` ledger entry (no worker slot to vacate, no type slot to
    /// release) and settles the terminal + phase cascade through the shared
    /// sink.
    pub(crate) async fn handle_setup_terminal(
        &mut self,
        msg: DistributedMessage<I>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        let DistributedMessage::SetupTerminal {
            task_hash,
            success,
            error_message,
            ..
        } = msg
        else {
            return;
        };
        let outcome = if success {
            SetupOutcome::Success
        } else {
            SetupOutcome::Failure(error_message)
        };
        // Free the in-flight ledger entry (no worker/type slot for a setup
        // task) and capture the phase + task_id for the cascade. A terminal
        // for a hash no longer in flight (a duplicate report, or a death that
        // already settled it) is a safe no-op — the CRDT settle below is
        // idempotent and the cascade is skipped.
        let Some(entry) = self.in_flight.remove(&task_hash) else {
            tracing::debug!(
                task_hash = %task_hash,
                "setup terminal for a hash not in flight (duplicate / already settled); ignoring"
            );
            return;
        };
        let (phase, task_id) = (entry.phase, entry.task.task_id);
        self.settle_setup_terminal(&task_hash, &phase, &task_id, outcome, command_rx)
            .await;
    }

    /// Originate the authoritative CRDT terminal for a setup task, run the
    /// phase cascade, and drive the dependent unblock — the SINGLE terminal
    /// sink shared by the on-primary self-exec and the off-primary report
    /// ingest.
    ///
    /// `Success` → `ClusterMutation::SetupCompleted` (the setup-success
    /// terminal whose apply arm auto-resumes every `Blocked` dependent — a
    /// dependent build task becomes dispatchable for free, since
    /// `apply_and_broadcast_cluster_mutations` re-injects the resumed
    /// binaries into the pool and emits `TasksAdded`) followed by
    /// `note_item_completed` (the phase drain edge + hooks). `Failure` →
    /// `apply_fail_permanent` (the EXISTING non-recoverable terminal + the
    /// dependent cascade — the SAME path a worker non-recoverable terminal
    /// takes; it runs its OWN phase cascade, so no separate
    /// `note_item_completed` here).
    async fn settle_setup_terminal(
        &mut self,
        task_hash: &str,
        phase: &PhaseId,
        task_id: &str,
        outcome: SetupOutcome,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        match outcome {
            SetupOutcome::Success => {
                tracing::info!(task_hash = %task_hash, "setup task succeeded");
                self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::SetupCompleted {
                    hash: task_hash.to_string(),
                }])
                .await;
                // The setup task left the in-flight count: run the SAME phase
                // cascade a worker completion runs (drain edge, hooks).
                self.note_item_completed(phase, Some(task_id), command_rx)
                    .await;
            }
            SetupOutcome::Failure(reason) => {
                tracing::warn!(
                    task_hash = %task_hash,
                    reason = %reason,
                    "setup task failed (non-recoverable); dependents will cascade"
                );
                // Route the failure through the SAME permanent-failure path a
                // non-recoverable worker terminal uses, so a setup task's
                // dependents follow the identical cascade. `apply_fail_permanent`
                // originates the `TaskFailed { NonRecoverable }` terminal +
                // the dependent cascade AND runs its own phase bookkeeping, so
                // there is no separate `note_item_completed` here. An unknown
                // hash (already settled) returns `Err` — logged, not fatal.
                if let Err(e) = self
                    .apply_fail_permanent(
                        task_hash.to_string(),
                        ErrorType::NonRecoverable,
                        reason,
                        command_rx,
                    )
                    .await
                {
                    tracing::debug!(
                        task_hash = %task_hash,
                        error = %e,
                        "setup-failure terminal could not be applied (already settled?)"
                    );
                }
            }
        }
    }
}
