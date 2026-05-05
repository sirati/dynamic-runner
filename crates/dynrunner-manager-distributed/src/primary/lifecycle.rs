use std::collections::HashSet;
use std::time::{Duration, Instant};

use dynrunner_core::{Identifier, ResourceMap, TaskInfo};
use dynrunner_protocol_primary_secondary::{
    DistributedMessage,
    SecondaryTransport, TaskListEntry,
};
use dynrunner_scheduler_api::{
    ResourceEstimator, Scheduler,
};


use super::PrimaryCoordinator;
use super::wire::{binary_to_distributed, compute_task_hash, timestamp_now};

impl<T: SecondaryTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, S, E, I> {
    pub(super) async fn send_transfer_complete(&mut self) -> Result<(), String> {
        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();
        for secondary_id in secondary_ids {
            let msg = DistributedMessage::TransferComplete {
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                total_files: 0,
                total_bytes: 0,
            };
            self.transport.send_to(&secondary_id, msg).await?;
        }
        tracing::info!("transfer complete sent to all secondaries");
        Ok(())
    }

    // ── Phase 7: Operational Loop ──

    pub(super) async fn operational_loop(&mut self) -> Result<(), String> {
        tracing::info!("entering operational loop");

        let mut heartbeat_tick =
            tokio::time::interval(self.config.keepalive_interval);
        // Skip the immediate first tick — secondaries might not have sent
        // their first keepalive yet at the moment we enter the loop.
        heartbeat_tick.tick().await;

        loop {
            // Check termination: all tasks accounted for AND no
            // worker is mid-dispatch. Both halves of the check are
            // necessary — counting `completed + failed >= total`
            // alone would orphan in-flight tasks if the bookkeeping
            // ever inflates (e.g. a TaskComplete arriving for a task
            // primary doesn't currently track as in-flight on a
            // worker — the insert grows the set while the in-flight
            // ledger stays as-is, so the counter check trips while a
            // sibling worker is still mid-dispatch and primary tears
            // down before that sibling's TaskComplete arrives).
            // Pairing the counter check with `active_workers == 0`
            // guarantees we only exit when every dispatched
            // assignment has been reconciled.
            let active_workers = self.workers.iter().filter(|w| w.current_task.is_some()).count();
            if self.completed_tasks.len() + self.failed_tasks.len() >= self.total_tasks
                && active_workers == 0
            {
                tracing::info!("all tasks completed or failed");
                break;
            }

            // Drain check: pool's `is_run_complete` returns true iff
            // queued + in-flight is zero AND no phase is Active or
            // Draining. The active-workers guard catches the edge
            // where in-flight is zero but a worker hasn't reported
            // completion yet (mostly defensive — `on_item_finished`
            // runs synchronously off the wire message).
            if self.pool().is_run_complete() && active_workers == 0 {
                tracing::info!("pool drained and no active workers");
                break;
            }

            // Fleet-dead detection. When every secondary has been
            // declared dead (via `requeue_dead_secondary`) and the
            // pool still has pending work, the loop would otherwise
            // sit forever waiting for events that no living
            // secondary can send. Track the first moment the fleet
            // is empty-but-pool-has-work; after
            // `config.fleet_dead_timeout` of continuous emptiness,
            // exit cleanly with pending tasks marked failed so the
            // operator gets a clear failure rather than a silent
            // idle. Cleared the moment a secondary is present
            // again (re-handshake / partial fleet survival).
            //
            // Tokenizer surfaced this on cohort-3 where SSH-tunnel
            // blips killed all 5 secondaries at once and the run
            // sat idle until manually killed.
            if self.secondaries.is_empty() && !self.pool().is_empty() {
                let now = Instant::now();
                let since = *self.fleet_dead_since.get_or_insert(now);
                let elapsed = now.duration_since(since);
                if elapsed >= self.config.fleet_dead_timeout {
                    let pending = self.pool_mut().drain_queued();
                    tracing::error!(
                        elapsed_s = elapsed.as_secs_f64(),
                        timeout_s = self.config.fleet_dead_timeout.as_secs_f64(),
                        marking_failed = pending.len(),
                        "fleet-dead timeout: every secondary gone with non-empty pool; \
                         marking pending tasks failed and exiting operational loop"
                    );
                    for binary in pending {
                        let hash = compute_task_hash(&binary);
                        self.failed_tasks.insert(hash);
                    }
                    break;
                }
            } else {
                // Fleet recovered (or never went empty); clear the
                // grace-period clock so a subsequent fleet-dead
                // event measures from its own start, not an old one.
                self.fleet_dead_since = None;
            }

            // Use a timeout on recv to avoid stalling indefinitely if a
            // secondary disconnects while processing a task. The timeout
            // is generous — if no message arrives in 5 minutes and there
            // are in-flight tasks, something is wrong.
            //
            // Cancellation safety: `transport.recv` is the mpsc-bridged
            // `NetworkServer::recv` (cancel-safe — see `MessageReceiver`
            // doc). The two timer arms (heartbeat tick + 5-min sleep)
            // are tokio time primitives which are themselves cancel-safe.
            tokio::select! {
                msg = self.transport.recv() => {
                    match msg {
                        Some(m) => self.dispatch_message(m).await?,
                        None => {
                            tracing::info!("transport closed");
                            break;
                        }
                    }
                }
                _ = heartbeat_tick.tick() => {
                    self.broadcast_primary_keepalive().await;
                    let report = self.collect_heartbeat_report();
                    for dead in report.dead {
                        self.requeue_dead_secondary(dead).await?;
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(300)) => {
                    let active = self.workers.iter().filter(|w| w.current_task.is_some()).count();
                    if active > 0 {
                        tracing::warn!(
                            active_workers = active,
                            completed = self.completed_tasks.len(),
                            failed = self.failed_tasks.len(),
                            total = self.total_tasks,
                            "operational loop timeout with active workers, marking in-flight tasks as failed"
                        );
                        // Mark all in-flight tasks as failed
                        for worker in &mut self.workers {
                            if let Some(binary) = worker.current_task.take() {
                                let hash = compute_task_hash(&binary);
                                self.failed_tasks.insert(hash);
                                worker.estimated_resources = ResourceMap::new();
                                worker.is_idle = true;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// After the main operational loop drains, run up to
    /// `config.retry_max_passes` retry passes. Each pass takes the
    /// current `failed_tasks` set, re-injects every matching binary
    /// from `all_binaries` into the pool (clearing the set), kicks
    /// dispatch to all currently-idle workers, then runs the
    /// operational loop again. Tasks that fail on the retry pass
    /// land back in `failed_tasks` for the next iteration — or stay
    /// there permanently if the retry budget is exhausted. No-op
    /// when `failed_tasks` is empty.
    ///
    /// The proactive idle-worker dispatch step is required because
    /// secondaries only send `TaskRequest` after they finish a task;
    /// after the main pass drains every worker is idle but already
    /// sent its last TaskRequest (which got `nothing-to-do` because
    /// the failed task wasn't in the pool yet). Without the
    /// kickstart the re-injected binaries would sit in the pool
    /// forever waiting for a TaskRequest that never comes.
    pub(super) async fn run_retry_passes(&mut self) -> Result<(), String> {
        for pass_idx in 0..self.config.retry_max_passes {
            if self.failed_tasks.is_empty() {
                break;
            }

            // Snapshot the failed-task hashes, clear the set, then
            // re-inject each matching binary into the pool. The
            // `failed_tasks` set tracks "currently considered failed"
            // — by clearing it here we let the upcoming operational
            // loop populate it fresh with the (smaller) set of
            // tasks that fail in THIS retry pass.
            let to_retry_hashes: HashSet<String> =
                std::mem::take(&mut self.failed_tasks);
            let to_retry: Vec<TaskInfo<I>> = self
                .all_binaries
                .iter()
                .filter(|b| to_retry_hashes.contains(&compute_task_hash(b)))
                .cloned()
                .collect();

            tracing::info!(
                pass = pass_idx + 1,
                count = to_retry.len(),
                "retry pass: re-injecting failed tasks"
            );

            for binary in to_retry {
                // Re-inject preserves phase state (flips Drained/Done
                // back to Active for this phase) so the operational
                // loop re-engages with the items.
                self.pool_mut().reinject(binary);
            }

            // Proactively dispatch to every idle worker before
            // entering the operational loop — see method-level
            // comment for the rationale.
            self.dispatch_to_idle_workers().await?;

            // The operational loop will dispatch the re-injected
            // tasks, observe their TaskComplete / TaskFailed
            // outcomes, and exit when the pool drains again. Tasks
            // that fail in this pass land in `failed_tasks` for the
            // next iteration of THIS for-loop.
            self.operational_loop().await?;
        }

        if !self.failed_tasks.is_empty() {
            tracing::warn!(
                permanent_failures = self.failed_tasks.len(),
                passes = self.config.retry_max_passes,
                "retry budget exhausted; tasks permanently failed"
            );
        }

        Ok(())
    }

    /// Iterate every idle worker and dispatch a task from the pool
    /// if one fits. Used by `run_retry_passes` to kickstart dispatch
    /// after re-injection (workers won't send a fresh TaskRequest on
    /// their own — see the run_retry_passes comment). Mirrors the
    /// per-worker logic in `handle_task_request` minus the
    /// SLURM-primary relay (which is irrelevant for the
    /// non-promoted-primary at this stage).
    pub(super) async fn dispatch_to_idle_workers(&mut self) -> Result<(), String> {
        for worker_idx in 0..self.workers.len() {
            if !self.workers[worker_idx].is_idle {
                continue;
            }
            // Skip workers belonging to a secondary that's currently
            // in backpressure backoff — see
            // `backpressured_secondaries` doc on `PrimaryCoordinator`.
            // Without this, the kickstart would re-target the same
            // unresponsive secondary in a tight loop, which is
            // exactly the failure storm 07ae301-followup is
            // designed to break.
            if self.is_backpressured(&self.workers[worker_idx].secondary_id) {
                continue;
            }
            let global_wid = self.workers[worker_idx].worker_id;
            let view = self.cap_filter_view(self.pool().view_for_worker(global_wid));
            if view.is_empty() {
                continue;
            }
            let worker_info = self.workers[worker_idx].budget_info();
            let all_infos: Vec<dynrunner_scheduler_api::WorkerBudgetInfo<I>> =
                self.workers.iter().map(|w| w.budget_info()).collect();
            let max_res = self.workers[worker_idx].resource_budgets.clone();

            let decision = self.scheduler.assign_normal(
                &worker_info,
                &all_infos,
                view.as_slice(),
                &max_res,
                &self.estimator,
                false,
            );

            if let dynrunner_scheduler_api::AssignmentDecision::Assign {
                binary_index,
                estimated_usage,
                ..
            } = decision
            {
                let binary = self.pool_mut().take_from_view(view, binary_index);
                self.reserve_type_slot(&binary.type_id);
                let sec_id = self.workers[worker_idx].secondary_id.clone();
                let local_worker_id = self.workers[..worker_idx + 1]
                    .iter()
                    .filter(|w| w.secondary_id == sec_id)
                    .count() as u32
                    - 1;
                self.workers[worker_idx].current_task = Some(binary.clone());
                self.workers[worker_idx].estimated_resources = estimated_usage;
                self.workers[worker_idx].is_idle = false;

                let assignment_msg = DistributedMessage::TaskAssignment {
                    sender_id: self.config.node_id.clone(),
                    timestamp: timestamp_now(),
                    secondary_id: sec_id.clone(),
                    worker_id: local_worker_id,
                    zip_file: None,
                    binary_info: binary_to_distributed(&binary),
                    local_path: self.config.wire_local_path(&binary),
                    file_hash: compute_task_hash(&binary),
                };
                self.transport.send_to(&sec_id, assignment_msg).await?;
            }
        }
        Ok(())
    }

    // ── Phase 6.5: Wait for secondary peer-meshes to settle ──

    /// Block on every connected secondary reporting `MeshReady`
    /// before letting `promote_slurm_primary` fire. The 750µs gap
    /// between "all secondaries cert-exchanged" and the previous
    /// promotion call left the SLURM-promoted secondary
    /// authoritative against a still-forming peer mesh — every
    /// pre-mesh-formation message went into the void for the
    /// 30s peer-dial budget. Closing the gap means waiting until
    /// each secondary has signalled its mesh has settled (mesh
    /// formed, watchdog elapsed, or no peers were expected for
    /// single-secondary).
    ///
    /// Bounded by `config.mesh_ready_timeout` (default 60s):
    /// stragglers past the deadline log a warning and the run
    /// proceeds anyway. A buggy secondary that never emits
    /// `MeshReady` must not be able to deadlock the entire
    /// dispatch pipeline; the post-promotion paths are all
    /// already failure-tolerant against an absent peer.
    ///
    /// Cancellation safety: `transport.recv` is the cancel-safe
    /// mpsc bridge; `sleep_until` is one-shot cancel-safe per
    /// tokio docs. The `select!` here mirrors the same shape
    /// `wait_for_connections` uses one phase up.
    pub(super) async fn wait_for_mesh_ready(&mut self) -> Result<(), String> {
        // The expected set is the live-secondaries set captured
        // AT this moment (post-quorum, post-cert-exchange). It is
        // not `config.num_secondaries` because the connect phase
        // may have dropped no-show secondaries on its own
        // timeout — we only wait for who's actually here.
        let expected: HashSet<String> = self.secondaries.keys().cloned().collect();
        if expected.is_empty() {
            tracing::debug!("no secondaries connected; skipping wait_for_mesh_ready");
            return Ok(());
        }

        // Fast path: messages may have already arrived before this
        // step ran (the welcome/cert-exchange/peer-info loop above
        // is event-driven and a fast secondary can emit MeshReady
        // before we enter the wait).
        if expected.is_subset(&self.mesh_ready_secondaries) {
            tracing::info!(
                count = expected.len(),
                "all secondaries reported MeshReady before wait step"
            );
            return Ok(());
        }

        let deadline = tokio::time::Instant::now() + self.config.mesh_ready_timeout;
        tracing::info!(
            expected = expected.len(),
            already_reported = self.mesh_ready_secondaries.len(),
            timeout_s = self.config.mesh_ready_timeout.as_secs_f64(),
            "waiting for peer-mesh formation across secondary fleet before \
             promoting SLURM-primary"
        );

        loop {
            if expected.is_subset(&self.mesh_ready_secondaries) {
                tracing::info!(
                    count = expected.len(),
                    "all secondaries reported MeshReady; releasing PromotePrimary"
                );
                return Ok(());
            }

            tokio::select! {
                msg = self.transport.recv() => {
                    match msg {
                        Some(m) => self.dispatch_message(m).await?,
                        None => return Err("transport closed during wait_for_mesh_ready".into()),
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    let missing: Vec<String> = expected
                        .difference(&self.mesh_ready_secondaries)
                        .cloned()
                        .collect();
                    tracing::warn!(
                        missing = ?missing,
                        reported = self.mesh_ready_secondaries.len(),
                        expected = expected.len(),
                        timeout_s = self.config.mesh_ready_timeout.as_secs_f64(),
                        "mesh-ready timeout: some secondaries never reported \
                         MeshReady; proceeding with PromotePrimary anyway. The \
                         SLURM-promoted secondary may briefly route into a \
                         partially-formed peer mesh until those secondaries \
                         finish (or fail) their dials."
                    );
                    return Ok(());
                }
            }
        }
    }

    // ── Phase 7: Promote SLURM-primary ──

    pub(super) async fn promote_slurm_primary(&mut self) -> Result<(), String> {
        if let Some(first_id) = self.secondaries.keys().next().cloned() {
            self.slurm_primary_id = Some(first_id.clone());
            tracing::info!(slurm_primary = %first_id, "promoting secondary to SLURM-primary");

            let msg = DistributedMessage::<I>::PromotePrimary {
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                new_primary_id: first_id.clone(),
            };
            self.transport.send_to(&first_id, msg).await?;
        }
        Ok(())
    }

    // ── Phase 8: Send full task list ──

    pub(super) async fn send_full_task_list(&mut self) -> Result<(), String> {
        // Bail out if no SLURM-primary was promoted (Phase 7 no-op
        // when there are no secondaries yet). Every secondary still
        // gets the broadcast below — promotion just controls who
        // gets the pre-designated routing pointer (`slurm_primary_id`).
        if self.slurm_primary_id.is_none() {
            return Ok(());
        }

        let all_tasks: Vec<TaskListEntry<I>> = self
            .all_binaries
            .iter()
            .map(|binary| {
                let hash = compute_task_hash(binary);
                TaskListEntry {
                    local_path: self.config.wire_local_path(binary),
                    binary_info: binary_to_distributed(binary),
                    hash: hash.clone(),
                    file_path: Some(binary.path.to_string_lossy().into_owned()),
                }
            })
            .collect();

        // Include both completed tasks and currently in-flight tasks as "completed"
        // so the SLURM-primary doesn't re-assign tasks that are already being processed
        let active_hashes: HashSet<String> = self
            .workers
            .iter()
            .filter_map(|w| w.current_task.as_ref().map(compute_task_hash))
            .collect();
        let excluded: HashSet<String> = self
            .completed_tasks
            .union(&active_hashes)
            .cloned()
            .collect();

        let completed_list: Vec<String> = excluded.iter().cloned().collect();
        let pending_list: Vec<String> = all_tasks
            .iter()
            .filter(|t| !excluded.contains(&t.hash))
            .map(|t| t.hash.clone())
            .collect();

        let msg = DistributedMessage::FullTaskList {
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            all_tasks,
            completed_tasks: completed_list,
            pending_tasks: pending_list,
            // Canonical phase-deps captured at `run()` start. Lets the
            // promoted SLURM-primary build its post-promotion pool
            // with the same dependency-machine the primary used —
            // otherwise every phase looks zero-deps to the new
            // primary and dependent phases dispatch out of order.
            phase_deps: self.phase_deps.clone(),
        };
        // Broadcast to every secondary, not just the pre-designated
        // SLURM-primary: F2 election picks a secondary on local-death
        // and that pick may not be the same node `promote_slurm_primary`
        // picked (the latter uses HashMap iteration order; the former
        // uses lowest-id-wins). Every secondary needs the cached task
        // list so any election outcome is survivable. Single-cast
        // would mean the user-stated invariant — "local can disconnect
        // once everything is transmitted, and the rest continues" —
        // only held for `--jobs 1`; broadcasting closes that gap.
        // SecondaryTransport doesn't have a broadcast primitive, so
        // we fan out via send_to in a loop. Failures on individual
        // secondaries are logged and continue — losing the cache on
        // one secondary just means F2 won't pick that one to promote.
        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();
        for secondary_id in &secondary_ids {
            if let Err(e) = self.transport.send_to(secondary_id, msg.clone()).await {
                tracing::warn!(
                    secondary = %secondary_id,
                    error = %e,
                    "failed to broadcast FullTaskList; that secondary won't be a viable failover target"
                );
            }
        }
        let slurm_id = self.slurm_primary_id.clone().unwrap();

        tracing::info!(
            slurm_primary = %slurm_id,
            total = self.all_binaries.len(),
            "sent full task list"
        );
        Ok(())
    }

}
