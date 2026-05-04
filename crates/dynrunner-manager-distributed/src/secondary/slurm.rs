use std::collections::{HashMap, HashSet};

use dynrunner_core::{Identifier, PhaseId, TaskInfo, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    DistributedBinaryInfo, DistributedMessage, PeerTransport, PrimaryTransport,
    TaskListEntry,
};
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator, Scheduler};


use super::SecondaryCoordinator;
use super::wire::timestamp_now;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: PrimaryTransport<I>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Build a fresh `PendingPool` for the SLURM-primary view from a
    /// `FullTaskList` snapshot.
    ///
    /// One concern: turn the wire-format snapshot (`TaskListEntry`s +
    /// completed-hash set + phase-deps map) into a `PendingPool`,
    /// dropping items that the cluster has already finished. The
    /// scheduler's soft-pin / phase machine inside the pool then
    /// governs dispatch; this function does no scheduling itself.
    ///
    /// The pool is rebuilt on every call: the wire snapshot is the
    /// authoritative source, and a partial patch would risk
    /// double-counting in-flight items the new primary can't observe
    /// from outside.
    pub(super) fn populate_slurm_tasks(
        &mut self,
        all_tasks: Vec<TaskListEntry<I>>,
        completed: HashSet<String>,
        phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    ) {
        self.slurm_completed = completed.clone();

        // Materialise items from the wire snapshot, skipping anything
        // the cluster (or this node) has already completed / has in
        // flight locally. Sort size-DESC up-front: the pool preserves
        // bucket-internal insertion order, and SLURM-primary dispatch
        // is first-fit-by-memory which benefits from biggest-first
        // packing (same heuristic as the legacy Vec-based path).
        // Filter first (immutable self borrow), then resolve in a
        // separate pass (mutable self borrow via the helper). Doing
        // both inside one iterator chain trips E0500 because
        // `resolve_for_dispatch` needs &mut self while the filter
        // closure still borrows self immutably.
        let kept: Vec<dynrunner_protocol_primary_secondary::TaskListEntry<I>> = all_tasks
            .into_iter()
            .filter(|task| {
                !completed.contains(&task.hash)
                    && !self.completed_tasks.contains(&task.hash)
                    && !self.active_tasks.contains_key(&task.hash)
            })
            .collect();
        let mut items: Vec<TaskInfo<I>> = Vec::with_capacity(kept.len());
        for task in kept {
            // Resolve via the three-mode helper (FR-2 opaque /
            // pre_staged / default). Use `task.local_path` (the
            // wire-relative form the primary stripped via
            // wire_local_path) — that's the form the resolver
            // expects in pre-staged mode. Falling back to
            // `task.file_path` (the primary's full view path) when
            // local_path is empty preserves the historical path for
            // normal mode where they're identical.
            let resolution_path = if !task.local_path.is_empty() {
                task.local_path.clone()
            } else {
                task.file_path.clone().unwrap_or_default()
            };
            let resolved =
                self.resolve_for_dispatch(None, &resolution_path, &task.hash);
            let binary_path = resolved
                .unwrap_or_else(|| std::path::PathBuf::from(&resolution_path));

            // Hydrate phase/type/affinity/payload from the wire.
            // Single source of truth for wire→TaskInfo lives in
            // `DistributedBinaryInfo::to_task_info` (Phase 4B).
            let mut binary = task.binary_info.to_task_info();
            binary.path = binary_path;
            items.push(binary);
        }
        items.sort_by_key(|i| std::cmp::Reverse(i.size));

        // Phase set = union of (declared phases via deps map) and
        // (phases observed in the items). Both directions are needed:
        // the deps map may declare an empty-but-real phase, and the
        // items may carry a phase the deps map omits.
        let mut phase_ids: HashSet<PhaseId> =
            items.iter().map(|i| i.phase_id.clone()).collect();
        for (child, parents) in &phase_deps {
            phase_ids.insert(child.clone());
            for p in parents {
                phase_ids.insert(p.clone());
            }
        }

        let pool = match PendingPool::new(phase_ids, phase_deps) {
            Ok(mut p) => {
                p.extend(items);
                p
            }
            Err(e) => {
                // The wire format should never deliver an inconsistent
                // graph, but if it does we degrade safely: an empty
                // pool causes the SLURM-primary to reply "no tasks" to
                // every request, which lets the run wind down rather
                // than crashing the new primary.
                tracing::error!(
                    error = %e,
                    "post-promotion: invalid phase graph in FullTaskList; SLURM-primary will start with no pending tasks"
                );
                self.slurm_pending = None;
                return;
            }
        };

        let pending_count = pool.len();
        self.slurm_pending = Some(pool);

        tracing::info!(
            pending = pending_count,
            completed = self.slurm_completed.len(),
            "populated SLURM-primary task list"
        );
    }

    /// Test/inspection helper: number of queued items in the pool.
    /// Returns 0 if the pool isn't initialised yet.
    pub(super) fn slurm_pending_len(&self) -> usize {
        self.slurm_pending.as_ref().map(|p| p.len()).unwrap_or(0)
    }

    /// Record completion of an item the SLURM-primary previously
    /// dispatched (via `handle_slurm_task_request`). Decrements the
    /// pool's in-flight counter for that item's phase, then promotes
    /// any newly-`Drained` phase to `Done` so dependents can become
    /// `Active`. No-op if the hash wasn't dispatched by this node — a
    /// peer-completion the SLURM-primary never issued belongs to a
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
    /// this loop the SLURM-primary would stop one phase short and
    /// the next phase's items would sit in the pool with the phase
    /// still `Blocked`.
    pub(super) fn note_slurm_item_completed(&mut self, file_hash: &str) {
        let phase_id = match self.slurm_in_flight.remove(file_hash) {
            Some(p) => p,
            None => return,
        };
        if let Some(pool) = self.slurm_pending.as_mut() {
            pool.on_item_finished(&phase_id);
            loop {
                let drained = pool.poll_drain_transitions();
                if drained.is_empty() {
                    break;
                }
                for p in &drained {
                    pool.mark_phase_done(p);
                }
                // Newly-active dependents may themselves be empty;
                // re-drain so the next poll_drain_transitions picks
                // them up and the cascade continues.
                pool.drain_empty_active_phases();
            }
        }
    }

    /// Test/inspection helper: whether the pool has zero queued items.
    /// Treats "no pool yet" as empty so resource-loop predicates don't
    /// have to special-case the pre-snapshot state.
    pub(super) fn slurm_pending_is_empty(&self) -> bool {
        self.slurm_pending
            .as_ref()
            .map(|p| p.is_empty())
            .unwrap_or(true)
    }

    /// Handle a task request from a peer when acting as SLURM-primary.
    /// Finds a suitable task and sends a TaskAssignment back.
    pub(super) async fn handle_slurm_task_request(
        &mut self,
        requesting_secondary_id: String,
        worker_id: WorkerId,
        available_memory: u64,
    ) -> Result<(), String> {
        if self.slurm_pending_is_empty() {
            tracing::debug!(
                secondary = %requesting_secondary_id,
                worker_id,
                "no pending tasks for SLURM-primary assignment"
            );
            return Ok(());
        }

        // Drop tasks completed elsewhere since population. The hash is
        // computed from path+identifier exactly the way the dispatch
        // path does so the same key space matches both sides.
        let completed_tasks = self.completed_tasks.clone();
        if let Some(pool) = self.slurm_pending.as_mut() {
            pool.retain(|item| !completed_tasks.contains(&task_file_hash(item)));
        }

        if self.slurm_pending_is_empty() {
            return Ok(());
        }

        // Find a task that fits the available memory; remove it from
        // the pool so it isn't handed out twice. `take_first_match`
        // walks bucket-key order, FIFO inside each bucket — same
        // ordering the original Vec produced after the size-DESC sort.
        let estimator = self.estimator.clone();
        let kind_memory = dynrunner_core::ResourceKind::memory();
        let assigned = self
            .slurm_pending
            .as_mut()
            .and_then(|pool| {
                pool.take_first_match(|item| {
                    let estimated = estimator.estimate(item);
                    estimated.get(&kind_memory) <= available_memory
                })
            });

        if let Some(binary) = assigned {
            let file_hash = task_file_hash(&binary);
            // The pool's `take_first_match` is a removal-only primitive
            // — it does not bump in-flight. Pair the dispatch with an
            // explicit `mark_in_flight` so the phase machine treats
            // the item as still belonging to the phase until the
            // cluster reports it finished. `slurm_in_flight` mirrors
            // the same fact at the per-item level so we can call
            // `on_item_finished(phase_id)` when TaskComplete /
            // TaskFailed arrives later.
            let dispatched_phase = binary.phase_id.clone();
            if let Some(pool) = self.slurm_pending.as_mut() {
                pool.mark_in_flight(&dispatched_phase);
            }
            self.slurm_in_flight
                .insert(file_hash.clone(), dispatched_phase);

            if requesting_secondary_id == self.config.secondary_id {
                // Assign directly to local worker (avoid recursive
                // dispatch_message cycle). Route through the
                // three-mode helper so pre-staged + uses-not-file-
                // based modes don't bypass the resolver and ship
                // unresolvable paths to the worker.
                //
                // Resolution-miss policy mirrors the historical
                // SLURM-primary self-assign behaviour: in
                // file-based + non-pre-staged mode the absolute
                // path is the worker's filesystem view (in-process
                // SLURM secondaries share the gateway's FS), so a
                // None resolution falls through to passthrough. In
                // pre-staged mode the absolute path is the
                // gateway-host view that the container can't see;
                // None there is a configuration error worth failing
                // loudly to avoid the misleading worker-level
                // "Not a valid binary file" buried in angr / ghidra.
                let resolution_path = binary.path.to_string_lossy().into_owned();
                let resolved =
                    self.resolve_for_dispatch(None, &resolution_path, &file_hash);
                let actual_binary = match resolved {
                    Some(path) => {
                        let mut b = binary.clone();
                        b.path = path;
                        b
                    }
                    None if self.pre_staged_mode() => {
                        tracing::error!(
                            worker_id,
                            file_hash = %file_hash,
                            path = %resolution_path,
                            "SLURM-primary self-assign in pre-staged mode: \
                             binary path unresolvable via src_network; \
                             dropping (likely pre-staged mode misconfiguration \
                             or stale wire path)"
                        );
                        return Ok(());
                    }
                    None => binary.clone(),
                };
                let estimated = self.estimator.estimate(&actual_binary);
                let wid = worker_id.min(self.pool.workers.len() as u32 - 1);
                if self.pool.workers[wid as usize].is_idle_state() {
                    match self.pool.workers[wid as usize]
                        .assign_task(actual_binary, estimated, false)
                        .await
                    {
                        Ok(()) => {
                            self.active_tasks.insert(file_hash, wid);
                            self.reset_request_backoff(wid);
                        }
                        Err(e) => {
                            tracing::error!(worker_id = wid, error = %e, "failed to assign SLURM task locally");
                        }
                    }
                }
            } else {
                // Send TaskAssignment to peer
                let msg = DistributedMessage::TaskAssignment {
                    sender_id: self.config.secondary_id.clone(),
                    timestamp: timestamp_now(),
                    secondary_id: requesting_secondary_id.clone(),
                    worker_id,
                    zip_file: None,
                    binary_info: DistributedBinaryInfo::from_task_info(&binary),
                    local_path: binary.path.to_string_lossy().into_owned(),
                    file_hash,
                };
                let _ = self
                    .peer_transport
                    .send_to_peer(&requesting_secondary_id, msg)
                    .await;
            }

            tracing::info!(
                secondary = %requesting_secondary_id,
                worker_id,
                binary = ?binary.identifier,
                remaining = self.slurm_pending_len(),
                "SLURM-primary assigned task"
            );
        }

        Ok(())
    }
}

/// Stable hash of a `TaskInfo`'s path+identifier, matching the wire
/// `file_hash` shape used elsewhere in the secondary. Pulled out as a
/// free function so SLURM-primary's "drop completed-elsewhere" filter
/// and the assignment path agree on the key space without duplicating
/// the hashing recipe.
fn task_file_hash<I: Identifier>(item: &TaskInfo<I>) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    item.path.hash(&mut h);
    item.identifier.hash(&mut h);
    format!("{:016x}", h.finish())
}
