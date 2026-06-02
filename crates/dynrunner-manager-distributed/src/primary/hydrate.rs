//! Authoritative-primary pool rehydration from the replicated
//! `cluster_state` ledger.
//!
//! Single concern: `hydrate_from_cluster_state` — turn the in-memory
//! CRDT into a fresh `PendingPool` (plus the matching
//! `pre_owned_in_flight` ledger and `completed_tasks` set) so a
//! freshly-composed authoritative `PrimaryCoordinator` resumes
//! operational dispatch seeded from the cluster view instead of an
//! empty pool.
//!
//! Faithful port of the secondary's
//! `populate_primary_from_cluster_state`
//! (`crate::secondary::primary::hydrate`); the two paths share the
//! `cascade_drain_done` pool-cascade primitive. One deviation: the
//! `PrimaryCoordinator` owns no local worker pool (workers are remote
//! `RemoteWorkerState` entries; there is no `active_tasks` set), so
//! the source's "Pending-in-cluster-state but locally-active" arm has
//! no analog here. A `Pending` / `Blocked` entry always becomes a
//! pool item; the loopback secondary's in-flight work is owned through
//! the `InFlight` arm as remote-in-flight, never double-counted as
//! local-active.

use std::collections::HashSet;

use dynrunner_core::{Identifier, PhaseId, TaskInfo};
use dynrunner_protocol_primary_secondary::{PeerTransport, SecondaryTransport};
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator, Scheduler};

use crate::cluster_state::TaskState;
use crate::primary::PrimaryCoordinator;
use crate::secondary::primary::cascade_drain_done;

impl<T: SecondaryTransport<I>, P: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, P, S, E, I> {
    /// Build a fresh `PendingPool` for the authoritative primary view
    /// from the replicated `cluster_state` ledger.
    ///
    /// One concern: turn the in-memory CRDT ledger into a fresh
    /// `PendingPool` for post-composition dispatch. The lattice
    /// (Pending / InFlight / Completed / Failed / Unfulfillable /
    /// Blocked) is iterated once; only `Pending` / `Blocked`
    /// entries enter the pool, terminal entries contribute their
    /// `task_id` to the dep-resolution seed, and `InFlight` entries are
    /// recorded as pre-owned in-flight (the originating dispatcher owns
    /// the work; this coordinator picks up completion only via the
    /// broadcast path).
    ///
    /// The pool is rebuilt on every call: the cluster ledger is the
    /// authoritative source, and a partial patch would risk
    /// double-counting in-flight items this coordinator can't observe
    /// from outside.
    ///
    /// Why we seed completed task_ids: the new pool's `completed_tasks`
    /// set is keyed by task_id. Variants in the `Pending` set may
    /// declare `task_depends_on` against a toolchain task_id whose
    /// task is no longer pending (already terminal). Without seeding
    /// the new pool's `completed_tasks` with those task_ids,
    /// `extend()`'s validation rejects every variant whose toolchain
    /// finished pre-composition as `UnknownTaskDep`.
    pub(crate) fn hydrate_from_cluster_state(&mut self) {
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
                | TaskState::Unfulfillable { task, .. } => {
                    primary_completed.insert(hash.clone());
                    completed_task_ids.insert(task.task_id.clone());
                }
                // Cascade-paused dependent. Re-seed as Pending into the
                // new primary's pool: the prereq's TaskCompleted apply
                // arm has already (or will shortly) auto-resume the
                // CRDT entry to Pending across every replica, and the
                // pool needs the binary present to dispatch on the
                // next tick. If the prereq is still Unfulfillable when
                // this coordinator composes, the pool's dep-validation
                // will surface the unresolved dep as a normal blocked
                // state — same dormancy, owned by the pool's existing
                // dep machine rather than a parallel "Blocked" set.
                TaskState::Blocked { task, .. } => {
                    items.push(task.clone());
                }
                // Unlike the secondary's hydration, the
                // `PrimaryCoordinator` owns no local `active_tasks`
                // set — its workers are remote `RemoteWorkerState`
                // entries and any work it itself dispatched is tracked
                // as `InFlight` in cluster_state. A `Pending` entry is
                // therefore always genuinely pending: into the pool.
                TaskState::Pending { task } => {
                    items.push(task.clone());
                }
                TaskState::InFlight { task, secondary, .. } => {
                    // The originating dispatcher owns the work; this
                    // coordinator will observe completion via the
                    // broadcast path (peer's TaskComplete on success /
                    // TaskFailed on terminal failure). To make that
                    // observation affect the pool correctly we need
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
                    //   3. Insert into `pre_owned_in_flight` keyed by
                    //      file_hash so when the broadcast TaskComplete
                    //      lands in `handle_task_complete` (which finds
                    //      no local worker holding the hash), the
                    //      fallback lookup finds the (phase_id,
                    //      secondary, binary) and forwards to
                    //      `note_item_completed`.
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

        self.completed_tasks = primary_completed;
        items.sort_by_key(|i| std::cmp::Reverse(i.size));

        let phase_deps = self.cluster_state.phase_deps().clone();

        // Phase set = union of (declared phases via deps map),
        // (phases observed in the items), and (phases of in-flight
        // tasks). The third source matters when a phase has had every
        // item dispatched pre-composition: the items list is empty for
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
                        "post-composition: invalid task graph in cluster_state; primary will start with no pending tasks"
                    );
                    self.pending = None;
                    return;
                }
                cascade_drain_done(&mut p);
                p
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "post-composition: invalid phase graph in cluster_state; primary will start with no pending tasks"
                );
                self.pending = None;
                return;
            }
        };

        // Seed `pre_owned_in_flight` only after `extend` succeeded — a
        // failure on the items batch leaves `pending = None` and any
        // in_flight ledger we'd populated would be stranded.
        for (hash, phase_id, secondary, binary) in in_flight_seed {
            self.pre_owned_in_flight
                .insert(hash, (phase_id, secondary, binary));
        }

        // Single source of truth for the run-completion accounting:
        // the cluster ledger's task count (`tasks.len()`), identical
        // to the reactive `mirror_mutation_to_accounting` refresh.
        self.total_tasks = self.cluster_state.task_count();

        let pending_count = pool.len();
        let in_flight_count = self.pre_owned_in_flight.len();
        self.pending = Some(pool);

        tracing::info!(
            pending = pending_count,
            in_flight = in_flight_count,
            succeeded = self.completed_tasks.len(),
            total = self.total_tasks,
            "hydrated primary task list from cluster_state"
        );
    }
}
