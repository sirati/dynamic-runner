use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use dynrunner_core::{Identifier, MessageReceiver, MessageSender, PhaseId, TaskInfo, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedBinaryInfo, DistributedMessage, PeerTransport,
};
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator, Scheduler};

use crate::cluster_state::{apply_locally_for_broadcast, TaskState};
use super::{SecondaryCoordinator, PrimaryInFlightItem};
use super::wire::timestamp_now;

/// Backpressure backoff window applied to a peer that just rejected a
/// `TaskAssignment` with "No idle worker available". Mirrors the
/// 500ms window used by the regular primary
/// (`PrimaryCoordinator::handle_task_failed`); a single constant
/// keeps the two paths in lockstep so promoted runs feel the
/// same as live-primary runs.
const PRIMARY_BACKPRESSURE_WINDOW: Duration = Duration::from_millis(500);

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
    pub(super) fn populate_primary_from_cluster_state(&mut self) {
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
                    if let Some(id) = &task.task_id {
                        completed_task_ids.insert(id.clone());
                    }
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
                        if let Some(id) = &task.task_id {
                            in_flight_pairs.push((id.clone(), task.phase_id.clone()));
                        }
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
                    if let Some(id) = &task.task_id {
                        in_flight_pairs.push((id.clone(), task.phase_id.clone()));
                    }
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

    /// Test/inspection helper: number of queued items in the pool.
    /// Returns 0 if the pool isn't initialised yet.
    pub(super) fn primary_pending_len(&self) -> usize {
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
    pub(super) fn note_primary_item_completed(&mut self, file_hash: &str) {
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
        if let Some(pool) = self.primary_pending.as_mut() {
            pool.on_item_finished(&phase_id, task_id.as_deref());
            cascade_drain_done(pool);
        }
    }

    /// Sibling to `note_primary_item_completed` for the failure path.
    /// Decrements the pool's in-flight counter for the item's phase
    /// (same as completion — phase machine doesn't distinguish
    /// success vs failure for in-flight bookkeeping). For Recoverable
    /// failures of tasks THIS secondary dispatched as primary
    /// via `handle_primary_task_request`, also stash the binary in
    /// `primary_failed` so `primary_drain_check_and_retry`
    /// can re-inject it after the main pass drains. Non-Recoverable
    /// / OutOfMemory / Unknown failures bypass the ledger — they're
    /// terminal at the worker level and retry would likely fail
    /// again the same way.
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
    /// forward, no-op for Recoverable). The Recoverable filter is
    /// inside this function so the callers don't have to special-case
    /// the retry path.
    pub(super) fn note_primary_item_failed(
        &mut self,
        file_hash: &str,
        error_type: &dynrunner_core::ErrorType,
    ) {
        let item = match self.primary_in_flight.remove(file_hash) {
            Some(item) => item,
            None => return,
        };
        let phase_id = item.phase_id.clone();
        let task_id = item.binary.task_id.clone();
        if matches!(error_type, dynrunner_core::ErrorType::Recoverable) {
            // Stash for the retry pass. Idempotent — the same hash
            // appearing twice (e.g. after re-injection fails again)
            // overwrites with the same binary and the latest
            // ErrorType, which is harmless. The entry carries
            // `error_type` so the outcome-summary breakdown can
            // partition the ledger by class; today only Recoverable
            // lands here (retry-pass scope), but the structure is
            // ready when non-Recoverable accounting joins.
            self.primary_failed.insert(
                file_hash.to_string(),
                crate::secondary::FailedTaskEntry {
                    binary: item.binary,
                    error_type: error_type.clone(),
                },
            );
        }
        if let Some(pool) = self.primary_pending.as_mut() {
            pool.on_item_finished(&phase_id, task_id.as_deref());
            cascade_drain_done(pool);
        }
    }

    /// Primary-side equivalent of the local primary's
    /// `run_retry_passes`. Called once per keepalive tick from
    /// `process_tasks`. When the main pass has drained for THIS
    /// primary's view (pool empty, no items in flight, no local
    /// active tasks) AND there are Recoverable failures pending in
    /// `primary_failed` AND the retry budget hasn't been
    /// exhausted, take a snapshot of the failed binaries, clear the
    /// ledger, re-inject each into `primary_pending` via
    /// `pool.reinject`, bump the pass counter, and kick our own idle
    /// workers via `repoll_idle_workers` so the operational loop
    /// re-engages with the just-injected items.
    ///
    /// Why "drain-check" rather than a phase-explicit "main pass" /
    /// "retry pass" boundary: the primary's `process_tasks` loop
    /// has no notion of pass boundaries — it's a single select! that
    /// runs until shutdown. The drain-check fires whenever the loop
    /// is observably idle and there's leftover retry work, which is
    /// the same trigger condition the local primary's two-phase
    /// `operational_loop` → `run_retry_passes` design used (drain →
    /// re-inject → re-run). Repeated firing is gated by the budget
    /// counter: once `retry_passes_used == retry_max_passes`, the
    /// next drain-check leaves `primary_failed` populated and
    /// the run wraps up via the normal exit conditions.
    ///
    /// Peer secondaries' workers don't need a kickstart from here:
    /// they re-poll on their own keepalive tick via
    /// `repoll_idle_workers` (with backoff) so any peer worker that
    /// got "no work" before the re-injection will see new work on its
    /// next request. Only the primary's own workers need an
    /// immediate kick — they're the ones whose `request_task_for_worker`
    /// short-circuits through `handle_primary_task_request` directly.
    pub(super) async fn primary_drain_check_and_retry(&mut self) {
        if !self.is_primary {
            return;
        }
        if self.primary_failed.is_empty() {
            return;
        }
        if !self.primary_pending_is_empty()
            || !self.primary_in_flight.is_empty()
            || !self.active_tasks.is_empty()
        {
            return;
        }
        if !self.primary_retry_budget.should_retry() {
            // Budget exhausted on either axis (attempt-count cap OR
            // SLURM-wallclock deadline minus safety margin): the
            // residual entries are permanent failures. Keep them in
            // the ledger so test fixtures (and future operator-
            // visible probes) can count permanent failures from the
            // primary's perspective; the log-spam guard is
            // `exhaustion_warning_emitted` so we emit the warning
            // once per run rather than every drain check.
            if !self.exhaustion_warning_emitted {
                // Tasks in `primary_failed` at retry-exhaustion time
                // are now terminal (no further passes). Surface them
                // as `fail_final` so the operator's log-side
                // breakdown matches the actual disposition; the
                // class-of-error these tasks hit was Recoverable
                // (only Recoverable lands in primary_failed today),
                // but the run-level outcome class is "final" because
                // the retry policy gave up.
                //
                // `passes` reflects the legacy attempt-count cap; if
                // the cliff was the SLURM-wallclock side, the actual
                // attempts-used will be < retry_max_passes — operators
                // chasing "why didn't I get all my retries?" should
                // cross-reference `$SLURM_JOB_END_TIME` and the run
                // duration. Single log shape kept stable for
                // back-compat with the existing operator dashboards.
                tracing::warn!(
                    fail_final = self.primary_failed.len(),
                    passes = self.config.retry_max_passes,
                    "primary retry budget exhausted; failed tasks are permanent"
                );
                self.exhaustion_warning_emitted = true;
            }
            return;
        }

        // Drain the failed-ledger: each entry yields its `binary`
        // for re-injection into `primary_pending`. The `error_type`
        // recorded on the entry is intentionally discarded here —
        // the next pass will overwrite with whatever outcome the
        // retry produces.
        let to_retry: Vec<TaskInfo<I>> =
            std::mem::take(&mut self.primary_failed)
                .into_values()
                .map(|entry| entry.binary)
                .collect();
        let pass = self.primary_retry_budget.attempts_used() + 1;
        tracing::info!(
            pass,
            count = to_retry.len(),
            "primary retry pass: re-injecting failed tasks"
        );
        if let Some(pool) = self.primary_pending.as_mut() {
            for binary in to_retry {
                pool.reinject(binary);
            }
        }
        self.primary_retry_budget.record_attempt();

        // Kick our own idle workers — see method-level doc. Peer
        // workers self-recover on their next keepalive-driven repoll.
        self.repoll_idle_workers().await;
    }

    /// Test/inspection helper: whether the pool has zero queued items.
    /// Treats "no pool yet" as empty so resource-loop predicates don't
    /// have to special-case the pre-snapshot state.
    pub(super) fn primary_pending_is_empty(&self) -> bool {
        self.primary_pending
            .as_ref()
            .map(|p| p.is_empty())
            .unwrap_or(true)
    }

    /// Handle a task request from a peer when acting as primary.
    /// Finds a suitable task and sends a TaskAssignment back.
    pub(super) async fn handle_primary_task_request(
        &mut self,
        requesting_secondary_id: String,
        worker_id: WorkerId,
        available_memory: u64,
    ) -> Result<(), String> {
        if self.primary_pending_is_empty() {
            tracing::debug!(
                secondary = %requesting_secondary_id,
                worker_id,
                "no pending tasks for primary assignment"
            );
            return Ok(());
        }

        // Drop tasks completed elsewhere since population. The hash is
        // computed from path+identifier exactly the way the dispatch
        // path does so the same key space matches both sides.
        let completed_tasks = self.completed_tasks.clone();
        if let Some(pool) = self.primary_pending.as_mut() {
            pool.retain(|item| !completed_tasks.contains(&task_file_hash(item)));
        }

        if self.primary_pending_is_empty() {
            return Ok(());
        }

        // Per-peer backpressure backoff (mirrors regular primary's
        // `is_backpressured`): if this peer recently bounced an
        // assignment with "No idle worker available", skip the
        // dispatch entirely. The binary stays in the pool — another
        // peer's TaskRequest can pick it up in the meantime.
        // Self-assignments bypass the check because the local
        // `is_idle_state()` test below is the source of truth for
        // our own workers; the peer-side backpressure ledger is
        // populated only by remote rejections.
        if requesting_secondary_id != self.config.secondary_id
            && self.is_primary_peer_backpressured(&requesting_secondary_id)
        {
            tracing::trace!(
                secondary = %requesting_secondary_id,
                worker_id,
                "skipping primary dispatch: peer is in backpressure backoff"
            );
            return Ok(());
        }

        // Find a task that fits the available memory; remove it from
        // the pool so it isn't handed out twice. `take_first_match`
        // walks bucket-key order, FIFO inside each bucket — same
        // ordering the original Vec produced after the size-DESC sort.
        let estimator = self.estimator.clone();
        let kind_memory = dynrunner_core::ResourceKind::memory();
        let assigned = self
            .primary_pending
            .as_mut()
            .and_then(|pool| {
                pool.take_first_match(|item| {
                    let estimated = estimator.estimate(item);
                    estimated.get(&kind_memory) <= available_memory
                })
            });

        if let Some(binary) = assigned {
            let file_hash = task_file_hash(&binary);
            let log_task_hash = file_hash.clone();
            // The pool's `take_first_match` is a removal-only primitive
            // — it does not bump in-flight. Pair the dispatch with an
            // explicit `mark_in_flight` so the phase machine treats
            // the item as still belonging to the phase until the
            // cluster reports it finished. `primary_in_flight` mirrors
            // the same fact at the per-item level so we can call
            // `on_item_finished(phase_id)` when TaskComplete /
            // TaskFailed arrives later, AND retains the binary +
            // target so the rejection-recovery path
            // (`handle_primary_peer_rejection`) can `pool.requeue` it.
            let dispatched_phase = binary.phase_id.clone();
            if let Some(pool) = self.primary_pending.as_mut() {
                pool.mark_in_flight(&dispatched_phase);
            }
            self.primary_in_flight.insert(
                file_hash.clone(),
                PrimaryInFlightItem {
                    phase_id: dispatched_phase,
                    target_secondary_id: requesting_secondary_id.clone(),
                    binary: binary.clone(),
                },
            );

            if requesting_secondary_id == self.config.secondary_id {
                // Assign directly to local worker (avoid recursive
                // dispatch_message cycle). Route through the
                // three-mode helper so pre-staged + uses-not-file-
                // based modes don't bypass the resolver and ship
                // unresolvable paths to the worker.
                //
                // Resolution-miss policy mirrors the historical
                // primary self-assign behaviour: in
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
                        // Surface the locally-resolved on-disk path
                        // via `resolved_path`; keep `binary.path` as
                        // the wire-supplied identifier so the worker
                        // sees the consumer's relative-under-source
                        // string in `task.relative_path` for output
                        // mirroring.
                        let mut b = binary.clone();
                        b.resolved_path = Some(path);
                        b
                    }
                    None if self.pre_staged_mode() => {
                        tracing::error!(
                            worker_id,
                            file_hash = %file_hash,
                            path = %resolution_path,
                            "primary self-assign in pre-staged mode: \
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
                            self.primary_link.reset_backoff(wid);
                        }
                        Err(e) => {
                            // `assign_task` failed AFTER we already
                            // moved the binary out of the pool +
                            // bumped in_flight + recorded
                            // `primary_in_flight`. Walk every step
                            // back so the binary isn't silently
                            // dropped: `recover_in_flight_to_pool`
                            // pulls the binary out of `primary_in_flight`
                            // and `pool.requeue`s it (decrements
                            // in_flight + pushes to front of bucket).
                            // Mirrors the recovery shape of
                            // `handle_primary_peer_rejection` below.
                            //
                            // SendFailed (the typical `Err` shape
                            // here) means the worker subprocess died
                            // between tasks — the pipe is broken.
                            // Reap so the log carries the actual
                            // signal/code, not just "Broken pipe".
                            // No `Disconnected` event will fire on
                            // this path (poll_loop is only spawned
                            // for an Assigned task), so this is the
                            // only log line the operator sees for a
                            // self-assign-time death.
                            //
                            // Bug B: queue this worker for respawn
                            // so the next `process_tasks` tick
                            // brings a fresh subprocess up. Without
                            // this, the docstring contract on
                            // `NonRecoverableError` ("worker process
                            // is restarted on the next assignment")
                            // was unmet on the SLURM-secondary path:
                            // the dead pipe stayed dead and every
                            // subsequent assignment to this slot
                            // failed the same way.
                            //
                            // Bug C: the binary is recovered back to
                            // the local pool (via
                            // `recover_in_flight_to_pool`), NOT
                            // marked as failed. The task hasn't
                            // been attempted by a worker — the
                            // pipe-write never landed. Once the
                            // worker respawns, its first
                            // TaskRequest re-picks the binary from
                            // the pool. Task retried, not lost.
                            let exit_status =
                                self.pool.workers[wid as usize].try_reap_exit();
                            tracing::warn!(
                                worker_id = wid,
                                error = %e,
                                exit_status = exit_status.as_ref().map(|s| s.to_string()),
                                "primary self-assign failed; queuing worker respawn + re-queuing binary"
                            );
                            self.pending_worker_restarts.insert(wid);
                            self.recover_in_flight_to_pool(&file_hash);
                        }
                    }
                } else {
                    // `is_idle_state()` flipped between the worker's
                    // TaskRequest and this dispatch (race after a
                    // recent kickstart-assignment landed first). Pre-
                    // fix this branch was missing entirely — the
                    // binary stayed `primary_in_flight`-tracked but no
                    // worker was processing it and no completion
                    // would ever arrive ⇒ silent task loss + stuck
                    // phase counter. Recover by undoing the take +
                    // mark_in_flight via `recover_in_flight_to_pool`.
                    tracing::warn!(
                        worker_id = wid,
                        "primary self-assign skipped: worker not idle (race with kickstart); re-queuing binary"
                    );
                    self.recover_in_flight_to_pool(&file_hash);
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
                task_id = ?binary.task_id,
                phase = %binary.phase_id,
                task_type = %binary.type_id,
                task_hash = %log_task_hash,
                remaining = self.primary_pending_len(),
                "primary assigned task"
            );
        }

        Ok(())
    }

    /// Undo a primary dispatch that didn't reach a worker
    /// (self-assign race, peer rejected, peer-side route lost). Removes
    /// the `primary_in_flight` entry, re-queues the binary at the front
    /// of its bucket via `pool.requeue` (which also decrements the
    /// per-phase in_flight counter), and clears the `active_tasks`
    /// entry if any was created. No-op if the hash isn't tracked
    /// (idempotent — peer-broadcast TaskFailed and primary-forwarded
    /// TaskFailed both arrive on the primary, and either may
    /// race the other).
    pub(super) fn recover_in_flight_to_pool(&mut self, file_hash: &str) {
        let item = match self.primary_in_flight.remove(file_hash) {
            Some(item) => item,
            None => return,
        };
        // `active_tasks` was inserted only on the self-assign success
        // path; remove unconditionally to keep its set in sync (no-op
        // if the hash wasn't there).
        self.active_tasks.remove(file_hash);
        if let Some(pool) = self.primary_pending.as_mut() {
            pool.requeue(item.binary);
        }
    }

    /// Apply the primary side of a peer rejection: extract the
    /// binary back to the pool and put the peer in a backoff window
    /// so the next `handle_primary_task_request` from it skips dispatch.
    /// Returns the `target_secondary_id` that was backpressured (or
    /// `None` if the hash wasn't in flight, e.g. the peer rejection
    /// arrived after a successful retry path completed it).
    pub(super) fn handle_primary_peer_rejection(&mut self, file_hash: &str) -> Option<String> {
        let item = self.primary_in_flight.remove(file_hash)?;
        let target = item.target_secondary_id.clone();
        self.active_tasks.remove(file_hash);
        if let Some(pool) = self.primary_pending.as_mut() {
            pool.requeue(item.binary);
        }
        self.backpressured_secondaries.insert(
            target.clone(),
            Instant::now() + PRIMARY_BACKPRESSURE_WINDOW,
        );
        Some(target)
    }

    /// Clear backpressure backoff for a peer that just reported a
    /// successful TaskComplete (proves the peer is healthy and
    /// accepting work). Called from the TaskComplete handlers in
    /// `dispatch.rs` and `peer.rs`. Mirrors the regular primary's
    /// backpressure clear on TaskComplete.
    pub(super) fn clear_primary_peer_backpressure(&mut self, secondary_id: &str) {
        self.backpressured_secondaries.remove(secondary_id);
    }

    /// Originator-side apply + broadcast for a batch of
    /// `ClusterMutation`s the promoted-secondary is producing (not
    /// receiving). Mirrors the live primary's
    /// `apply_and_broadcast_cluster_mutations` — local apply runs first
    /// via the shared `apply_locally_for_broadcast` helper (so the
    /// `Applied`-vs-`NoOp` filter semantics stay identical), then the
    /// applied subset fans out over BOTH the peer mesh and the demoted-
    /// submitter link.
    ///
    /// Fan-out shape: `peer_transport.broadcast` reaches every
    /// surviving secondary in the cluster; `primary_transport.send`
    /// reaches the demoted local submitter (which sits on the other end
    /// of this node's secondary→primary channel — not a peer, mute-
    /// routing is asymmetric). Without the demoted-submitter loopback
    /// the submitter's `mirror_mutation_to_accounting` never observes
    /// our `TaskAdded` batch and its `total_tasks` accounting stays at
    /// 0, tripping the exit-counter check the moment it sees 0+0>=0.
    ///
    /// Errors are best-effort: per-peer `peer_transport` failure
    /// surfaces as a single error string (the trait's contract); a
    /// dropped `primary_transport.send` means the submitter already
    /// exited and we're past the point where the loopback matters.
    /// CRDT idempotency makes a missed mutation recoverable from the
    /// next snapshot RPC; we never block the originator on universal
    /// delivery.
    ///
    /// Single concern: "originate a batch on the promoted-secondary
    /// side". Receiver-side application of mutations OBSERVED on the
    /// wire goes through `apply_cluster_mutations` (in dispatch.rs)
    /// instead.
    pub(super) async fn apply_and_broadcast_mutations(
        &mut self,
        mutations: Vec<ClusterMutation<I>>,
    ) -> Result<(), String> {
        if mutations.is_empty() {
            return Ok(());
        }
        // `apply_locally_for_broadcast` also surfaces auto-resumed
        // Blocked dependents (see the live primary's mirror site).
        // The promoted secondary's `primary_pending` pool was seeded
        // from CRDT at promotion time (`populate_primary_from_cluster_state`)
        // so any Blocked entries already exist in the pool as
        // `task_depends_on`-tracked items — the pool's own dep
        // machinery will dispatch them when the prereq's
        // `on_item_finished` fires through the normal task-event
        // path. Re-injecting the resumed clones here would
        // duplicate them in the pool's buckets, so we deliberately
        // discard the list on this path. (Unfulfillable-cascade
        // sequences originated post-promotion would also leave the
        // dependents in the pool's `blocked` map for the same
        // dep-machinery to unblock; the live primary's mirror site
        // is the only one whose pool actually loses the items via
        // `on_item_failed_permanent`.)
        let batch = apply_locally_for_broadcast(&mut self.cluster_state, mutations);
        let crate::cluster_state::AppliedBatch {
            applied,
            resumed_for_dispatch: _,
        } = batch;
        if applied.is_empty() {
            return Ok(());
        }
        let msg = DistributedMessage::ClusterMutation {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            mutations: applied,
        };
        // Fan out to both transports. Order matters only for symmetry
        // with `processing.rs`'s RunComplete fan-out (peer-first there;
        // mirror it here so future maintainers see one pattern). Errors
        // are logged but not propagated — see method doc.
        if let Err(e) = self.primary_transport.send(msg.clone()).await {
            tracing::warn!(
                error = %e,
                "ClusterMutation send to demoted submitter failed (submitter likely exited)"
            );
        }
        if let Err(e) = self.peer_transport.broadcast(msg).await {
            tracing::warn!(
                error = %e,
                "ClusterMutation peer broadcast failed"
            );
        }
        Ok(())
    }

    /// Ingest the result of Python's `task.discover_items` after a
    /// `PromotePrimary { required_setup: true }` promotion.
    ///
    /// Pre-condition: `setup_pending == true` (set by the
    /// `PromotePrimary` handler in `dispatch.rs` for the setup-promote
    /// reason). The outer process-tasks loop yielded back to the PyO3
    /// wrapper, which ran `task.discover_items` against the locally
    /// bind-mounted staged source filesystem and is now feeding the
    /// result back into the Rust core.
    ///
    /// Sequence:
    ///   1. Build the mutation batch: one `PhaseDepsSet` carrying the
    ///      task graph's static phase dependency map (so every
    ///      receiver's `cluster_state.phase_deps()` is populated
    ///      before any post-promotion hydration consults it), then one
    ///      `TaskAdded` per discovered binary.
    ///   2. Originate the batch via `apply_and_broadcast_mutations` —
    ///      applies locally to `cluster_state` and fans out to peers +
    ///      the demoted submitter. The submitter's
    ///      `mirror_mutation_to_accounting` updates its `total_tasks`
    ///      counter as the `TaskAdded` mutations arrive, so its
    ///      exit-counter check sees a non-zero target instead of
    ///      tripping at 0+0>=0.
    ///   3. Clear `setup_pending` so the outer loop's next iteration
    ///      doesn't yield again.
    ///   4. Hydrate `primary_pending` from the now-populated
    ///      `cluster_state`. This is the same call the pre-seeded
    ///      (`required_setup_on_promote = false`) path makes in the
    ///      `PromotePrimary` handler; here it runs after
    ///      `setup_pending` clears so operational dispatch can begin
    ///      on the next tick.
    ///
    /// Idempotency: `ClusterMutation::TaskAdded` is no-op-on-duplicate
    /// (the CRDT silently drops it via the `apply` filter), and
    /// `apply_locally_for_broadcast` filters NoOp mutations out before
    /// broadcast, so a duplicate `ingest_setup_discovery` call (e.g.
    /// from a wrapper retry on transport hiccup) doesn't re-broadcast
    /// the same batch.
    pub async fn ingest_setup_discovery(
        &mut self,
        binaries: Vec<TaskInfo<I>>,
        phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    ) -> Result<(), String> {
        let mut mutations: Vec<ClusterMutation<I>> = Vec::with_capacity(binaries.len() + 1);
        mutations.push(ClusterMutation::PhaseDepsSet { deps: phase_deps });
        for b in &binaries {
            mutations.push(ClusterMutation::TaskAdded {
                hash: task_file_hash(b),
                task: b.clone(),
            });
        }
        let task_count = binaries.len();
        self.apply_and_broadcast_mutations(mutations).await?;
        self.setup_pending = false;
        self.populate_primary_from_cluster_state();
        tracing::info!(
            tasks = task_count,
            "ingested setup-discovery; primary pool hydrated"
        );
        // Empty-discovery happy path: when discovery surfaces zero
        // items (e.g. every binary's output already exists under a
        // `--skip-existing` filter), the pool is drained from
        // inception and there will never be a `TaskCompleted` to
        // trigger the normal counter-driven RunComplete broadcast.
        // Originate RunComplete directly so every peer's exit arm
        // observes the same authoritative terminal signal.
        if task_count == 0 {
            self.apply_and_broadcast_mutations(vec![ClusterMutation::RunComplete])
                .await?;
            tracing::info!(
                "empty-discovery: RunComplete broadcast — no tasks to run"
            );
        }
        Ok(())
    }
}

/// Stable hash of a `TaskInfo`'s path+identifier, matching the wire
/// `file_hash` shape used elsewhere in the secondary. Pulled out as a
/// free function so primary's "drop completed-elsewhere" filter
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

/// Run the phase-lifecycle drain cascade on a pool until quiescent.
/// Shared between `populate_primary_from_cluster_state` (catches phases
/// whose only items pre-completed elsewhere) and `note_primary_item_completed`
/// (catches phases whose only items just finished). Each iteration:
///   1. `drain_empty_active_phases` — moves any Active phase whose
///      `(queued, in_flight) == (0, 0)` to Drained, queues it for
///      `poll_drain_transitions`.
///   2. `poll_drain_transitions` — returns and clears the
///      drained-pending list.
///   3. `mark_phase_done` — flips Drained → Done, may unblock
///      dependent phases (Blocked → Active).
///
/// The loop terminates when no new drains surface (the next
/// `drain_empty_active_phases` finds nothing to transition AND
/// `poll_drain_transitions` returns empty).
///
/// Free function (rather than impl method) so the lifecycle test
/// in `tests.rs` can drive it on a hand-built pool without
/// instantiating a full `SecondaryCoordinator` — single concern,
/// single dependency on `&mut PendingPool`.
pub(super) fn cascade_drain_done<I: Identifier>(pool: &mut PendingPool<I>) {
    loop {
        pool.drain_empty_active_phases();
        let drained = pool.poll_drain_transitions();
        if drained.is_empty() {
            break;
        }
        for p in &drained {
            pool.mark_phase_done(p);
        }
    }
}
