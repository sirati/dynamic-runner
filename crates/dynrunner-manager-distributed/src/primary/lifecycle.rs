use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use dynrunner_core::{Identifier, ResourceMap, TaskInfo};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage,
    SecondaryTransport,
};
use dynrunner_scheduler_api::{
    ResourceEstimator, Scheduler,
};


use super::{PrimaryCoordinator, RemoteWorkerState};
use super::wire::{binary_to_distributed, compute_task_hash, timestamp_now};

/// Order idle worker indices for a single dispatch tick, biasing
/// toward secondaries with fewer currently-running tasks. Stable
/// tie-break by `worker_id` so equal-loaded secondaries fall through
/// to the existing iteration order.
///
/// Pre-fix the flat `0..workers.len()` scan iterated workers grouped
/// by secondary (the order initial-assignment populates them), giving
/// the first-iterated secondary's idle workers systematic priority
/// when both sides had idle capacity. Tail-of-phase dispatches —
/// where the pool has fewer items than there are idle workers — then
/// concentrated remaining work on the already-loaded secondary
/// instead of spreading across the fleet.
pub(super) fn dispatch_order<I: Identifier>(workers: &[RemoteWorkerState<I>]) -> Vec<usize> {
    let mut load_per_secondary: HashMap<&str, usize> = HashMap::new();
    for w in workers {
        if w.current_task.is_some() {
            *load_per_secondary
                .entry(w.secondary_id.as_str())
                .or_default() += 1;
        }
    }
    let mut idle: Vec<usize> = workers
        .iter()
        .enumerate()
        .filter(|(_, w)| w.is_idle)
        .map(|(i, _)| i)
        .collect();
    idle.sort_by_key(|&i| {
        (
            load_per_secondary
                .get(workers[i].secondary_id.as_str())
                .copied()
                .unwrap_or(0),
            workers[i].worker_id,
        )
    });
    idle
}

impl<T: SecondaryTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, S, E, I> {
    /// Apply each mutation locally and broadcast the same batch so every
    /// secondary mirrors the change. Per-secondary delivery failures are
    /// logged at warn — the CRDT is idempotent, so a missed mutation is
    /// recoverable from the next snapshot RPC (Phase B); we never block
    /// dispatch on universal delivery.
    pub(super) async fn apply_and_broadcast_cluster_mutations(
        &mut self,
        mutations: Vec<ClusterMutation<I>>,
    ) {
        if mutations.is_empty() {
            return;
        }
        for m in &mutations {
            self.cluster_state.apply(m.clone());
        }
        let msg = DistributedMessage::ClusterMutation {
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            mutations,
        };
        if let Err(failures) = self.transport.broadcast(msg).await {
            for (secondary_id, error) in &failures {
                tracing::warn!(
                    secondary = %secondary_id,
                    error = %error,
                    "ClusterMutation broadcast delivery failed"
                );
            }
        }
    }

    /// Phase-S/B: seed the replicated cluster ledger with the run's
    /// task graph and phase-dependency graph. Emits one
    /// `PhaseDepsSet` (carrying the canonical per-run dep graph)
    /// followed by one `TaskAdded` per binary in `all_binaries`; the
    /// originator-side `apply_and_broadcast_cluster_mutations` applies
    /// locally and ships the batch to every secondary.
    ///
    /// `PhaseDepsSet` rides ahead of `TaskAdded` so receivers'
    /// `cluster_state.phase_deps()` is populated before any
    /// post-promotion hydration that consults it. The mutation is
    /// idempotent (re-application is a no-op when local is non-empty),
    /// so multiple snapshot sources or duplicate broadcasts are safe.
    ///
    /// Called once at run start, after every secondary has connected
    /// (so `transport.broadcast` reaches the full fleet) and before
    /// `perform_initial_assignment` runs (so the originator's mirror
    /// is non-empty when the first dispatch happens).
    pub(super) async fn seed_cluster_state(&mut self) {
        let mut mutations: Vec<ClusterMutation<I>> = Vec::with_capacity(
            self.all_binaries.len() + 1,
        );
        mutations.push(ClusterMutation::PhaseDepsSet {
            deps: self.phase_deps.clone(),
        });
        mutations.extend(
            self.all_binaries
                .iter()
                .map(|b| ClusterMutation::TaskAdded {
                    hash: compute_task_hash(b),
                    task: b.clone(),
                }),
        );
        let task_count = self.all_binaries.len();
        self.apply_and_broadcast_cluster_mutations(mutations).await;
        tracing::info!(tasks = task_count, "seeded cluster ledger");
    }

    pub(super) async fn send_transfer_complete(&mut self) -> Result<(), String> {
        let msg = DistributedMessage::TransferComplete {
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            total_files: 0,
            total_bytes: 0,
        };
        if let Err(failures) = self.transport.broadcast(msg).await {
            for (secondary_id, error) in &failures {
                tracing::warn!(
                    secondary = %secondary_id,
                    error = %error,
                    "TransferComplete delivery failed"
                );
            }
            return Err(format!(
                "TransferComplete broadcast failed for {} secondaries",
                failures.len()
            ));
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
                    // Demoted observer mode: the promoted
                    // primary owns dead-secondary detection and the
                    // associated requeue. If the local primary also
                    // requeued, the same in-flight task would be
                    // re-dispatched twice (once from each primary's
                    // pool view) — duplicating work and racing on
                    // ledger state. We still send keepalives so peers
                    // know we're alive, but skip the requeue path.
                    //
                    // `process_heartbeat_tick` runs the per-tick
                    // mass-death-aware death evaluator: resolve any
                    // already-deferred secondaries (recovery or grace
                    // expiry), then categorise newly-dead ones as
                    // either correlated (defer) or independent
                    // (requeue). See `process_heartbeat_tick` for
                    // detail and `PrimaryConfig.mass_death_grace` for
                    // the disable knob.
                    if !self.demoted {
                        self.process_heartbeat_tick().await?;
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
        // Demoted observer mode: the promoted primary owns
        // retry orchestration. The local primary's `failed_tasks`
        // set still receives forwarded outcomes (so the run-done
        // counter check trips when everything is accounted for),
        // but re-injecting tasks into the local pool and
        // dispatching them would create the very parallel-dispatch
        // race demotion is meant to eliminate. See `demoted` doc
        // on `PrimaryCoordinator`.
        if self.demoted {
            return Ok(());
        }
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
    /// primary relay (which is irrelevant for the
    /// non-promoted-primary at this stage).
    pub(super) async fn dispatch_to_idle_workers(&mut self) -> Result<(), String> {
        // Demoted observer mode: the promoted primary is the
        // sole authority for dispatch. Returning here covers every
        // call site (kickstart from handle_task_complete /
        // handle_task_failed, plus the retry-pass kickstart) without
        // sprinkling `if !self.demoted` across the message-handling
        // code. See `demoted` doc on `PrimaryCoordinator`.
        if self.demoted {
            return Ok(());
        }
        // Visit idle workers in load-aware order so a secondary with
        // many in-flight tasks doesn't keep winning tail-of-phase
        // dispatches against an idler peer. `dispatch_order` filters
        // to idle and sorts by (busy-workers-on-secondary, worker_id).
        let order = dispatch_order(&self.workers);
        for worker_idx in order {
            // Skip workers belonging to a secondary that's currently
            // in backpressure backoff — see
            // `backpressured_secondaries` doc on `PrimaryCoordinator`.
            // Without this, the kickstart would re-target the same
            // unresponsive secondary in a tight loop, which is
            // exactly the failure storm 07ae301-followup is
            // designed to break. Re-checked inline (not pre-filtered
            // in `dispatch_order`) so a backpressure window that
            // opens mid-tick takes effect immediately.
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

                let task_hash = compute_task_hash(&binary);
                let assignment_msg = DistributedMessage::TaskAssignment {
                    sender_id: self.config.node_id.clone(),
                    timestamp: timestamp_now(),
                    secondary_id: sec_id.clone(),
                    worker_id: local_worker_id,
                    zip_file: None,
                    binary_info: binary_to_distributed(&binary),
                    local_path: self.config.wire_local_path(&binary),
                    file_hash: task_hash.clone(),
                };
                self.transport.send_to(&sec_id, assignment_msg).await?;

                tracing::info!(
                    secondary = %sec_id,
                    worker_id = local_worker_id,
                    task_id = ?binary.task_id,
                    phase = %binary.phase_id,
                    task_type = %binary.type_id,
                    task_hash = %task_hash,
                    "task assigned"
                );
            }
        }
        Ok(())
    }

    // ── Phase 6.5: Wait for secondary peer-meshes to settle ──

    /// Block on every connected secondary reporting `MeshReady`
    /// before letting `promote_primary` fire. The 750µs gap
    /// between "all secondaries cert-exchanged" and the previous
    /// promotion call left the promoted secondary
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
             promoting primary"
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
                         promoted secondary may briefly route into a \
                         partially-formed peer mesh until those secondaries \
                         finish (or fail) their dials."
                    );
                    return Ok(());
                }
            }
        }
    }

    // ── Promote primary (atomic role-flip) ──

    pub(super) async fn promote_primary(&mut self) -> Result<(), String> {
        if let Some(first_id) = self.secondaries.keys().next().cloned() {
            self.primary_id = Some(first_id.clone());
            // Monotonic per-promotion epoch carried on the wire and
            // fed into `ClusterState::PrimaryChanged`'s last-writer-
            // wins resolver. Starting from the local mirror's current
            // epoch + 1 is sufficient at the bootstrap promotion
            // (epoch starts at 0 cluster-wide); the failover election
            // protocol's own `round` will supersede this when it
            // re-elects.
            let new_epoch = self.cluster_state.primary_epoch() + 1;
            tracing::info!(primary = %first_id, epoch = new_epoch, "promoting secondary to primary");

            // Apply locally so the originator's mirror flips atomically
            // with the broadcast, not after the broadcast round-trips
            // back to us. Lower-epoch races no-op against the higher
            // epoch we just installed.
            self.cluster_state.apply(ClusterMutation::PrimaryChanged {
                new: first_id.clone(),
                epoch: new_epoch,
            });

            let msg = DistributedMessage::<I>::PromotePrimary {
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                new_primary_id: first_id.clone(),
                epoch: new_epoch,
            };
            // Broadcast to every secondary, not unicast to the elected
            // node: every secondary needs the role-change to update
            // its `primary_link` routing target and clear its per-
            // worker backoff so idle workers re-issue at the new
            // primary on their next tick (otherwise the new primary's
            // peers stay quiet for one stale-window). The pre-Phase-P
            // unicast was Bug 2 in the trace at `feb1052` — only the
            // elected node logged the role change because only it
            // received the message.
            if let Err(failures) = self.transport.broadcast(msg).await {
                for (secondary_id, error) in &failures {
                    tracing::warn!(
                        secondary = %secondary_id,
                        error = %error,
                        "PromotePrimary broadcast delivery failed"
                    );
                }
            }

            // Hand-off complete: the local primary stops being
            // authoritative the moment `PromotePrimary` is on the
            // wire. We stay alive (transport open, message loop
            // still runs) so completion forwards keep
            // `completed_tasks` accurate for the run-done counter
            // check in `operational_loop`, but we no longer
            // dispatch, kickstart, or drive heartbeat-based
            // requeue — the promoted secondary owns all of that.
            // Without this, the local primary and the promoted
            // secondary both act as primaries simultaneously and
            // their parallel dispatch paths race for the same
            // workers. See `demoted` doc on `PrimaryCoordinator`.
            self.demoted = true;
            tracing::info!(
                primary = %first_id,
                "local primary demoted to observer; promoted secondary is sole authoritative primary"
            );
        }
        Ok(())
    }

}
