
use dynrunner_core::{TaskInfo, Identifier, ResourceMap};
use dynrunner_protocol_primary_secondary::{
    DistributedMessage,
    SecondaryTransport,
};
use dynrunner_scheduler_api::{
    AssignmentDecision, ResourceEstimator, Scheduler, WorkerBudgetInfo,
};


use super::PrimaryCoordinator;
use super::wire::{binary_to_distributed, compute_task_hash, timestamp_now};

impl<T: SecondaryTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, S, E, I> {
    pub(super) async fn handle_task_request(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        if let DistributedMessage::TaskRequest {
            ref secondary_id,
            worker_id,
            ref available_resources,
            ..
        } = msg
        {
            let available_res: ResourceMap = available_resources.iter()
                .map(|r| (r.kind.clone(), r.amount))
                .collect();
            // Find matching worker
            let mut target_idx = None;
            let mut local_idx: u32 = 0;
            for (idx, w) in self.workers.iter().enumerate() {
                if w.secondary_id == *secondary_id {
                    if local_idx == worker_id {
                        target_idx = Some(idx);
                        break;
                    }
                    local_idx += 1;
                }
            }

            let mut assigned = false;

            if let Some(idx) = target_idx {
                // Mark worker idle
                self.workers[idx].current_task = None;
                self.workers[idx].estimated_resources = ResourceMap::new();
                self.workers[idx].is_idle = true;
                if !available_res.is_empty() {
                    self.workers[idx].resource_budgets = available_res.clone();
                }

                // Try to assign from local pending. The pool's
                // `view_for_worker` returns the soft-pin priority order
                // for this worker; the scheduler picks the index, the
                // pool commits the take.
                let global_wid = self.workers[idx].worker_id;
                let view = self.cap_filter_view(self.pool().view_for_worker(global_wid));
                if !view.is_empty() {
                    let worker_info = self.workers[idx].budget_info();
                    let all_infos: Vec<WorkerBudgetInfo<I>> =
                        self.workers.iter().map(|w| w.budget_info()).collect();
                    let max_res = self.workers[idx].resource_budgets.clone();

                    let decision = self.scheduler.assign_normal(
                        &worker_info,
                        &all_infos,
                        view.as_slice(),
                        &max_res,
                        &self.estimator,
                        false,
                    );

                    if let AssignmentDecision::Assign {
                        binary_index,
                        estimated_usage,
                        ..
                    } = decision
                    {
                        let binary = self.pool_mut().take_from_view(view, binary_index);
                        self.reserve_type_slot(&binary.type_id);
                        let sec_id = self.workers[idx].secondary_id.clone();
                        self.workers[idx].current_task = Some(binary.clone());
                        self.workers[idx].estimated_resources = estimated_usage;
                        self.workers[idx].is_idle = false;

                        let assignment_msg = DistributedMessage::TaskAssignment {
                            sender_id: self.config.node_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: sec_id.clone(),
                            worker_id,
                            zip_file: None,
                            binary_info: binary_to_distributed(&binary),
                            local_path: self.config.wire_local_path(&binary),
                            file_hash: compute_task_hash(&binary),
                        };
                        self.transport.send_to(&sec_id, assignment_msg).await?;

                        tracing::debug!(
                            secondary = %sec_id,
                            worker_id,
                            binary = ?binary.identifier,
                            "task assigned"
                        );
                        assigned = true;
                    }
                }
            }

            // If no local assignment was made, relay to SLURM-primary
            if !assigned {
                if let Some(slurm_id) = self.slurm_primary_id.clone() {
                    self.transport.send_to(&slurm_id, msg).await?;
                }
            }
        }
        Ok(())
    }

    pub(super) async fn handle_task_complete(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::TaskComplete {
            secondary_id,
            worker_id,
            task_hash,
            ..
        } = &msg
        {
            let secondary_id = secondary_id.clone();
            let worker_id = *worker_id;
            self.completed_tasks.insert(task_hash.clone());

            // Mark the specific worker idle using secondary_id + local worker_id.
            // Capture the phase + type of the just-finished item so we
            // can fold it into per-phase counters, release the
            // per-type concurrency slot, and run the phase lifecycle
            // cascade.
            let mut completed_meta: Option<(dynrunner_core::PhaseId, dynrunner_core::TypeId)> =
                None;
            let mut local_idx: u32 = 0;
            for w in &mut self.workers {
                if w.secondary_id == secondary_id {
                    if local_idx == worker_id {
                        if let Some(task) = w.current_task.take() {
                            completed_meta = Some((task.phase_id.clone(), task.type_id.clone()));
                        }
                        w.estimated_resources = ResourceMap::new();
                        w.is_idle = true;
                        break;
                    }
                    local_idx += 1;
                }
            }

            if let Some((phase, type_id)) = completed_meta {
                self.release_type_slot(&type_id);
                self.note_item_completed(&phase);
            }

            tracing::debug!(
                secondary = %secondary_id,
                worker_id,
                completed = self.completed_tasks.len(),
                "task complete"
            );

            // Kickstart dispatch to every idle worker. After
            // `note_item_completed` runs the phase-lifecycle cascade,
            // a previously-Blocked phase may have just transitioned
            // to Active. Workers that have been idle since startup
            // (because their initial TaskRequest got "no work" when
            // the new phase wasn't yet active) won't re-poll on their
            // own — they sent their last TaskRequest already, got
            // nothing, and are waiting for an unsolicited
            // TaskAssignment. Without this kickstart, a 2-phase task
            // graph where phase-N has 1 item and phase-(N+1) has the
            // rest would stall after the phase-N item finishes —
            // the originating secondary's worker DOES re-request via
            // its `request_task_for_worker(0)` in
            // `processing.rs:193`, but every OTHER secondary's
            // workers don't. Same kickstart pattern as
            // `run_retry_passes` uses after re-injection.
            //
            // Idempotent: if no phase advanced (the common case for
            // mid-phase completions where the phase still has queued
            // work), `dispatch_to_idle_workers` finds the soft-pin
            // soft-pin order returns the originating worker first and
            // the kickstart no-ops by definition. If multiple phases
            // cascaded done in one tick (chain of empty phases →
            // first populated phase), every newly-active phase's
            // items are seen.
            self.dispatch_to_idle_workers().await.ok();

            // Belt-and-suspenders: forward to every other secondary
            // so each one's `completed_tasks` cache stays current.
            // The originating secondary already broadcasts
            // peer-to-peer (processing.rs), but that's best-effort;
            // a primary-side forward closes the gap if a peer
            // broadcast was lost. Without this, on local-death-then-
            // failover, a secondary missing the peer broadcast
            // would re-dispatch the already-completed task. (Same
            // failover-survivability invariant as the FullTaskList
            // broadcast in 04d9012, applied to per-completion
            // updates.)
            self.forward_completion_to_secondaries(&msg, &secondary_id)
                .await;
        }
    }

    /// Send `msg` to every connected secondary except the one that
    /// originated it. Per-secondary failures are logged and continue
    /// — a missed completion forward just risks a re-dispatch on
    /// failover, not a run-wide failure.
    async fn forward_completion_to_secondaries(
        &mut self,
        msg: &DistributedMessage<I>,
        origin_secondary_id: &str,
    ) {
        let recipients: Vec<String> = self
            .secondaries
            .keys()
            .filter(|id| id.as_str() != origin_secondary_id)
            .cloned()
            .collect();
        for secondary_id in &recipients {
            if let Err(e) = self
                .transport
                .send_to(secondary_id, msg.clone())
                .await
            {
                tracing::debug!(
                    secondary = %secondary_id,
                    error = %e,
                    "failed to forward task completion; that secondary may re-dispatch on failover"
                );
            }
        }
    }

    pub(super) async fn handle_task_failed(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::TaskFailed {
            secondary_id,
            worker_id,
            task_hash,
            error_type,
            error_message,
            ..
        } = &msg
        {
            let secondary_id = secondary_id.clone();
            let worker_id = *worker_id;
            let task_hash = task_hash.clone();
            let error_type = error_type.clone();
            let error_message = error_message.clone();
            // Find the specific worker and recover the binary if it's a
            // recoverable error so it can be re-assigned to another worker.
            let mut recovered_binary: Option<TaskInfo<I>> = None;
            let mut local_idx: u32 = 0;
            for w in &mut self.workers {
                if w.secondary_id == secondary_id {
                    if local_idx == worker_id {
                        recovered_binary = w.current_task.take();
                        w.estimated_resources = ResourceMap::new();
                        w.is_idle = true;
                        break;
                    }
                    local_idx += 1;
                }
            }

            // Failure budget: one per task per pass. Recoverable
            // and NonRecoverable both terminate the dispatch slot
            // and add to `failed_tasks`. The `run()` pipeline calls
            // `retry_failed_tasks_pass` after the main operational
            // loop drains, which re-injects everything in
            // `failed_tasks` (clearing the set) and runs the loop
            // again. Up to `config.retry_max_passes` retry passes
            // (default 1) before failures are permanent.
            //
            // Critically NOT counted as a failure: secondary
            // disconnect → `requeue_dead_secondary` puts the
            // in-flight task back into the pool via
            // `pool.requeue` (NOT through this function). The task
            // never reached `failed_tasks`, so its retry budget
            // stays untouched.
            self.failed_tasks.insert(task_hash.clone());
            if let Some(binary) = recovered_binary {
                self.release_type_slot(&binary.type_id);
                self.note_item_failed(&binary.phase_id);
            }

            tracing::warn!(
                secondary = %secondary_id,
                worker_id,
                error_type = %error_type,
                error = %error_message,
                "task failed"
            );

            // Same kickstart rationale as `handle_task_complete`:
            // `note_item_failed` may have just cascaded a phase
            // through Drained → Done and activated a dependent
            // phase; idle workers across other secondaries won't
            // re-poll on their own. Idempotent.
            self.dispatch_to_idle_workers().await.ok();

            // Forward task-terminal outcomes to peer secondaries so
            // their `failed_tasks` / `completed_tasks` caches stay
            // current — required for SLURM-promoted-primary handoff
            // not to re-dispatch a task we just gave up on. Both
            // Recoverable and NonRecoverable are terminal in the
            // pass-based retry model: the retry pass re-injects into
            // the pool by re-running the operational loop, which is
            // the new "second chance"; an immediate requeue would
            // recreate the busy-loop bug.
            self.forward_completion_to_secondaries(&msg, &secondary_id)
                .await;
        }
    }
}
