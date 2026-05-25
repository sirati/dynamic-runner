//! Post-promotion primary-pool rehydration from the replicated
//! `cluster_state` ledger.
//!
//! Single concern: `populate_primary_from_cluster_state` — turn the
//! in-memory CRDT into a fresh `PendingPool` (plus the matching
//! `primary_in_flight` ledger and `primary_completed` set) so a newly
//! promoted secondary can resume operational dispatch without a wire
//! round-trip.

use std::collections::HashSet;

use dynrunner_core::{Identifier, MessageReceiver, MessageSender, PhaseId, TaskInfo};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator, Scheduler};

use super::super::{PrimaryInFlightItem, SecondaryCoordinator};
use super::cascade_drain_done;
use crate::cluster_state::TaskState;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Build a fresh `PendingPool` for the primary view from the
    /// replicated `cluster_state` ledger.
    ///
    /// One concern: turn the in-memory CRDT ledger into a fresh
    /// `PendingPool` for post-promotion dispatch. The lattice
    /// (Pending / InFlight / Completed / Failed) is iterated once;
    /// only `Pending` entries enter the pool, terminal entries
    /// contribute their `task_id` to the dep-resolution seed, and
    /// `InFlight` entries are skipped (the originating dispatcher
    /// owns them; the post-promotion primary picks them up only on
    /// requeue via the heartbeat-driven dead-secondary handler, not
    /// from the initial pool seed).
    ///
    /// The pool is rebuilt on every call: the cluster ledger is the
    /// authoritative source, and a partial patch would risk
    /// double-counting in-flight items the new primary can't observe
    /// from outside.
    ///
    /// Why we seed completed task_ids: the new pool's `completed_tasks`
    /// set is keyed by task_id. Variants in the `Pending` set may
    /// declare `task_depends_on` against a toolchain task_id whose
    /// task is no longer pending (already terminal). Without seeding
    /// the new pool's `completed_tasks` with those task_ids,
    /// `extend()`'s validation rejects every variant whose toolchain
    /// finished pre-promotion as `UnknownTaskDep`.
    pub(in crate::secondary) fn populate_primary_from_cluster_state(&mut self) {
        let mut completed_task_ids: HashSet<String> = HashSet::new();
        let mut primary_completed: HashSet<String> = HashSet::new();
        let mut items: Vec<TaskInfo<I>> = Vec::new();
        let mut in_flight_pairs: Vec<(String, PhaseId)> = Vec::new();
        let mut in_flight_seed: Vec<(String, PhaseId, String, TaskInfo<I>)> = Vec::new();

        for (hash, state) in self.cluster_state.tasks_iter() {
            match state {
                // Terminal-ish for hydration: contribute task_id to the
                // dep-resolution seed and mark hash as completed in the
                // primary-side ledger. `Unfulfillable` is included
                // because the dep graph treats unfulfillable prereqs
                // the same way the legacy `Failed { Unfulfillable, .. }`
                // shape did — surviving variants' `task_depends_on`
                // references must still resolve so `extend()` accepts
                // them. The Unfulfillable entry itself stays in the
                // CRDT and is reinjectable via the command channel; no
                // pool work is needed for it.
                TaskState::Completed { task }
                | TaskState::Failed { task, .. }
                | TaskState::Unfulfillable { task, .. }
                // Panik-cancelled tasks land here too: terminal for
                // dep-resolution (their task_id seeds
                // `completed_task_ids` so dependents' `extend()` is
                // accepted) and the hash is marked completed in the
                // primary-side ledger so the new primary doesn't
                // attempt to re-dispatch. Cancelled is a panik-
                // sticky terminal — a hydrating new-primary inherits
                // the latched `panik_active` via the CRDT replica
                // and shouldn't be re-running this hash regardless.
                | TaskState::Cancelled { task, .. } => {
                    primary_completed.insert(hash.clone());
                    completed_task_ids.insert(task.task_id.clone());
                }
                // Cascade-paused dependent. Re-seed as Pending into the
                // new primary's pool: the prereq's TaskCompleted apply
                // arm has already (or will shortly) auto-resume the
                // CRDT entry to Pending across every replica, and the
                // pool needs the binary present to dispatch on the
                // next tick. If the prereq is still Unfulfillable when
                // this node promotes, the pool's dep-validation will
                // surface the unresolved dep as a normal blocked
                // state — same dormancy, owned by the pool's existing
                // dep machine rather than a parallel "Blocked" set.
                TaskState::Blocked { task, .. } => {
                    if !self.active_tasks.contains_key(hash) {
                        items.push(task.clone());
                    }
                }
                TaskState::Pending { task } => {
                    if !self.active_tasks.contains_key(hash) {
                        items.push(task.clone());
                    } else {
                        // Pending in cluster_state but actively
                        // running on a worker of THIS node. The
                        // production initial-assignment path
                        // dispatches TaskAssignment over the wire
                        // without firing a `TaskAssigned` cluster
                        // mutation, so cluster_state stays `Pending`
                        // even after dispatch. Locally we know
                        // the task is in flight (the hash is in
                        // `active_tasks`); treat it as in-flight
                        // for pool seeding purposes — same as the
                        // `InFlight` arm — so dependents validate
                        // and the pool counter drains correctly
                        // when this node's worker reports
                        // completion through `note_primary_item_completed`.
                        in_flight_pairs.push((task.task_id.clone(), task.phase_id.clone()));
                        in_flight_seed.push((
                            hash.clone(),
                            task.phase_id.clone(),
                            self.config.secondary_id.clone(),
                            task.clone(),
                        ));
                    }
                }
                TaskState::InFlight { task, secondary, .. } => {
                    // The originating dispatcher owns the work; this
                    // node will observe completion via the broadcast
                    // path (peer's TaskComplete on success / TaskFailed
                    // on terminal failure). To make that observation
                    // affect the new primary's pool correctly we need
                    // three things:
                    //   1. Seed the task_id into `in_flight_tasks` so
                    //      `extend()`'s dep validation accepts Pending
                    //      variants whose `task_depends_on` references
                    //      an in-flight task. Without this every such
                    //      dependent fails `UnknownTaskDep` and the new
                    //      primary degrades to "no pending tasks".
                    //   2. Bump `in_flight_per_phase` for the in-flight
                    //      task's phase so phase-lifecycle drains
                    //      correctly when completion arrives — the
                    //      counter must drop from N+1 to N, not from
                    //      0 to 0.
                    //   3. Insert into `primary_in_flight` keyed by
                    //      file_hash so when the broadcast TaskComplete
                    //      lands in `dispatch_message`'s
                    //      TaskComplete arm and triggers
                    //      `note_primary_item_completed`, the lookup
                    //      finds the (phase_id, task_id) pair and
                    //      forwards to `pool.on_item_finished`.
                    // (1) and (2) are owned by the pool via
                    // `mark_tasks_in_flight` below; (3) is local state.
                    in_flight_pairs.push((task.task_id.clone(), task.phase_id.clone()));
                    in_flight_seed.push((
                        hash.clone(),
                        task.phase_id.clone(),
                        secondary.clone(),
                        task.clone(),
                    ));
                }
            }
        }

        self.primary_completed = primary_completed;
        items.sort_by_key(|i| std::cmp::Reverse(i.size));

        let phase_deps = self.cluster_state.phase_deps().clone();

        // Phase set = union of (declared phases via deps map),
        // (phases observed in the items), and (phases of in-flight
        // tasks). The third source matters when a phase has had every
        // item dispatched pre-promotion: the items list is empty for
        // that phase, but `mark_tasks_in_flight` will bump its
        // counter and the phase must exist in `phase_state` for
        // drain transitions to fire.
        let mut phase_ids: HashSet<PhaseId> =
            items.iter().map(|i| i.phase_id.clone()).collect();
        for (_, phase_id) in &in_flight_pairs {
            phase_ids.insert(phase_id.clone());
        }
        for (_, phase_id, _, _) in &in_flight_seed {
            phase_ids.insert(phase_id.clone());
        }
        for (child, parents) in &phase_deps {
            phase_ids.insert(child.clone());
            for p in parents {
                phase_ids.insert(p.clone());
            }
        }

        let pool = match PendingPool::new(phase_ids, phase_deps) {
            Ok(mut p) => {
                p.mark_tasks_completed(completed_task_ids);
                p.mark_tasks_in_flight(in_flight_pairs);
                if let Err(e) = p.extend(items) {
                    tracing::error!(
                        error = %e,
                        "post-promotion: invalid task graph in cluster_state; primary will start with no pending tasks"
                    );
                    self.primary_pending = None;
                    return;
                }
                cascade_drain_done(&mut p);
                p
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "post-promotion: invalid phase graph in cluster_state; primary will start with no pending tasks"
                );
                self.primary_pending = None;
                return;
            }
        };

        // Seed `primary_in_flight` only after `extend` succeeded — a
        // failure on the items batch leaves `primary_pending = None`
        // and any in_flight ledger we'd populated would be stranded.
        for (hash, phase_id, secondary, binary) in in_flight_seed {
            self.primary_in_flight.insert(
                hash,
                PrimaryInFlightItem {
                    phase_id,
                    target_secondary_id: secondary,
                    binary,
                },
            );
        }

        let pending_count = pool.len();
        let in_flight_count = self.primary_in_flight.len();
        self.primary_pending = Some(pool);

        tracing::info!(
            pending = pending_count,
            in_flight = in_flight_count,
            succeeded = self.primary_completed.len(),
            "populated primary task list"
        );
    }
}
