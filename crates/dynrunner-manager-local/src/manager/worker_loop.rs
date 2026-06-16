use std::collections::HashSet;

use dynrunner_core::{
    ErrorType, FailedTask, Identifier, ResourceKind, WorkerId, gather_predecessor_outputs,
};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{AssignmentDecision, ProcessingPhase, ResourceEstimator, Scheduler};

use crate::oom::{DEFAULT_HEARTBEAT_INTERVAL, OomWatcher, OomWatcherConfig, SAMPLE_SWEEP_INTERVAL};

use super::{LocalManager, WorkerFactory};

impl<M: ManagerEndpoint + 'static, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier>
    LocalManager<M, S, E, I>
{
    pub(super) async fn process_worker_loop(
        &mut self,
        active_workers: &mut HashSet<WorkerId>,
        allow_stop: bool,
        on_failure_increment_failed: bool,
        phase: ProcessingPhase,
        factory: &mut impl WorkerFactory<M>,
    ) {
        // The OOM memory accounting runs as ONE self-paced sweep (read
        // all workers' cgroup charges off the async runtime via
        // `spawn_blocking`, apply + decide inline), not per-fire timer
        // arms. See `crate::oom`. Derive the workers cgroup
        // `memory.events` path from the pool's nested cgroup handle
        // (when present). This activates the OomWatcher's kernel-OOM
        // detection so the disconnect reclassifier in `handle_event`
        // can upgrade pipe-EOF events from `Recoverable` to
        // `ResourceExhausted(memory)` whenever the kernel's `oom_kill`
        // counter incremented in the same sweep window.
        let workers_memory_events_path = self
            .pool
            .workers_cgroup()
            .map(|h| h.workers_path().join("memory.events"));
        let mut oom_watcher = OomWatcher::new_with_workers_cgroup(
            OomWatcherConfig {
                sample_interval: SAMPLE_SWEEP_INTERVAL,
                heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
                log_enabled: self.config.log_oom_watcher,
            },
            workers_memory_events_path,
        );
        // The sweep's wake deadline. Seeded to NOW so the first sweep
        // fires immediately; re-armed `sweep_interval` after EACH sweep
        // completes (await-before-resleep — a slow sweep cannot pile,
        // the next starts a full interval after the previous returned).
        // The periodic maintenance (`check_timeouts` / stuck-worker
        // report) that the old decision tick drove rides this same
        // sweep at the same cadence.
        let sweep_interval = oom_watcher.sweep_interval();
        let mut next_sweep_due = tokio::time::Instant::now();

        // Take the command-channel receiver out of `self.command_rx`
        // so we can `recv()` it inside `select!` without borrowing
        // `&mut self` twice. Restored at end-of-loop so subsequent
        // phase calls into `process_worker_loop` (retry, pressure,
        // unassigned) and the post-pipeline tail drain in
        // `process_binaries` can re-take it. Re-entrant
        // `process_worker_loop` calls would trip the expect here —
        // by design, only one phase loop runs at a time.
        let mut command_rx = self
            .command_rx
            .take()
            .expect("command_rx absent; process_worker_loop re-entrant?");

        while !active_workers.is_empty() {
            // Drain any commands queued during the prior iteration's
            // event handling (e.g. a `spawn_tasks` issued from
            // `on_phase_end` inside `process_drain_transitions`).
            // Applying these BEFORE `assign_idle_workers` ensures
            // the pool's `is_empty()` check sees freshly-spawned
            // tasks; without the drain a late `SpawnTasks` would
            // race the loop's break check and risk a missed
            // dispatch this iteration.
            while let Ok(cmd) = command_rx.try_recv() {
                crate::manager::command_channel::handle_local_command(self, cmd).await;
            }

            // Try to assign tasks to any idle workers
            self.assign_idle_workers(active_workers, allow_stop, phase, factory)
                .await;

            // If no workers are processing and no pending assignments, we're done
            let any_processing = active_workers.iter().any(|&wid| {
                let idx = wid as usize;
                self.pool.workers[idx].is_processing()
            });
            if !any_processing && self.pending_worker_assignments.is_empty() {
                break;
            }

            // Wait for either a worker event, an external command,
            // the sample tick, or the decision tick.
            //
            // Cancellation safety: `pool.recv_event` and
            // `command_rx.recv` are both `mpsc::Receiver::recv`
            // (cancel-safe). The interval ticks are cancel-safe per
            // tokio docs. Each arm dropping the others' futures is
            // harmless.
            //
            // `biased;` ensures the command arm wins against ticks
            // at the same instant — an external `spawn_tasks` /
            // `fail_permanent` from `PyPrimaryHandle` is operationally
            // ahead of forensic sampling.
            tokio::select! {
                biased;
                cmd = command_rx.recv() => {
                    if let Some(cmd) = cmd {
                        crate::manager::command_channel::handle_local_command(self, cmd).await;
                    }
                    // Channel-closed (`None`): the manager's
                    // command_tx (held by `self.command_tx`) is
                    // still alive for the manager's lifetime, so
                    // this arm only fires if all external clones
                    // have dropped — non-fatal; future `recv()`
                    // calls keep returning `None` and the arm
                    // becomes a no-op until a fresh clone surfaces.
                }
                event = self.pool.recv_event() => {
                    if let Some(event) = event {
                        self.handle_event(
                            event,
                            active_workers,
                            allow_stop,
                            on_failure_increment_failed,
                            phase,
                            factory,
                            &oom_watcher,
                        )
                        .await;
                        // Per-event drain-transition flush. A finishing
                        // task may have just emptied its phase's queue
                        // and zeroed its in-flight count; mid-run firing
                        // is what unblocks dependents (`Blocked → Active`
                        // happens inside `mark_phase_done`'s cascade).
                        // Phases whose items still live in a side queue
                        // are deferred — see
                        // `process_drain_transitions`.
                        self.process_drain_transitions();
                    }
                }
                // Self-paced OOM sweep arm. Parks on the stored
                // `next_sweep_due` deadline; on fire it reads ALL
                // workers' cgroup charges off the async runtime
                // (`spawn_blocking`), applies them, runs the pressure
                // decision inline, then runs the periodic maintenance
                // and re-arms the deadline. One operational-loop wakeup
                // per sweep, vs the former per-fire sample + decision
                // ticks (the 58%-of-wakeups blocking-IO hot path).
                //
                // Priority: this arm is LAST in source order. Under the
                // outer `biased;` the command + pool-event arms beat
                // the 20Hz forensic timer when they have ready work
                // (the same arm-priority discipline the secondary
                // process_tasks loop applies — #586).
                //
                // Cancel-safety: `sleep_until` consumes nothing and the
                // deadline is PERSISTENT local state, so a sibling arm
                // winning the race merely re-creates the future against
                // the SAME instant next iteration — the sweep cannot be
                // starved into never firing by a busy loop.
                _ = tokio::time::sleep_until(next_sweep_due) => {
                    // Collect the CURRENT worker set's read inputs
                    // (respawns / type-shifts picked up each sweep),
                    // then read off the runtime — no pool borrow held
                    // across the blocking call.
                    let inputs = oom_watcher.collect_sweep_inputs(&self.pool);
                    let sweep = tokio::task::spawn_blocking(move || inputs.read())
                        .await
                        .expect("oom charge sweep read panicked");
                    oom_watcher.apply_sweep(&mut self.pool, sweep);
                    if !self.pool_ref().is_empty() {
                        self.check_resource_pressure_via_watcher(&mut oom_watcher);
                    }
                    self.check_timeouts(active_workers, on_failure_increment_failed, factory).await;
                    self.report_stuck_workers();
                    // Await-before-resleep: arm the next sweep a full
                    // interval after THIS one completed.
                    next_sweep_due = tokio::time::Instant::now() + sweep_interval;
                }
            }

            // Handle pending worker reassignments after events
            if !self.pending_worker_assignments.is_empty() {
                let pending: Vec<WorkerId> =
                    self.pending_worker_assignments.iter().copied().collect();
                for worker_id in pending {
                    let idx = worker_id as usize;
                    if self.pool.workers[idx].current_binary.is_none()
                        && self.pool.workers[idx].is_ready()
                    {
                        self.try_assign_normal(worker_id, factory).await;
                        self.pending_worker_assignments.remove(&worker_id);
                    }
                }
            }
        }

        // Restore the command-channel receiver onto `self` so the
        // next phase's `process_worker_loop` call (or the
        // outer-loop tail drain in `process_binaries`) can re-take
        // it. The phase functions call us sequentially, never
        // concurrently — the `take` / restore dance keeps the
        // borrow checker happy without an `Arc<Mutex<...>>`.
        self.command_rx = Some(command_rx);

        // Move remaining pending to the recoverable retry channel at
        // the end of the retry phase (main-phase leftovers go to
        // `unassigned_tasks` in `run_main_phase`).
        //
        // Tag: `ErrorType::Recoverable`. The previous shape tagged
        // these as `ResourceExhausted(memory)`, which is wrong — the
        // leftover items are tasks that no worker's RESERVED budget
        // accepted at scheduling time (a scheduling-fit failure). The
        // OOM channel is reserved for actual memory-pressure kills
        // surfaced via `KillReason::OomOverBudget` /
        // `KillReason::OomLastResort`. Routing the scheduling-fit
        // leftovers to the Recoverable channel sends them through
        // `record_result`'s `failed_tasks` branch instead of
        // `resource_pressure_tasks`, matching the actual failure
        // class.
        if phase == ProcessingPhase::RetryPhase && !self.pool_ref().is_empty() {
            let remaining = self.pool_mut().drain_queued();
            for binary in remaining {
                self.failed_tasks.push(FailedTask {
                    binary: crate::manager::own_task(binary),
                    error_type: ErrorType::Recoverable,
                    error_message: "Could not fit in any worker budget".into(),
                    retry_count: 0,
                });
            }
        }
    }

    /// Try to assign tasks to all idle active workers.
    pub(super) async fn assign_idle_workers(
        &mut self,
        active_workers: &mut HashSet<WorkerId>,
        allow_stop: bool,
        phase: ProcessingPhase,
        factory: &mut impl WorkerFactory<M>,
    ) {
        let worker_ids: Vec<WorkerId> = active_workers.iter().copied().collect();
        for worker_id in worker_ids {
            let idx = worker_id as usize;

            // Poll not-yet-ready workers (still in WaitingForReady state)
            if !self.pool.workers[idx].is_ready() {
                self.pool.workers[idx].poll_ready().await;
                if !self.pool.workers[idx].is_ready() {
                    if self.pool_ref().is_empty() && allow_stop {
                        active_workers.remove(&worker_id);
                    }
                    continue;
                }
            }

            // Skip workers that are already processing
            if self.pool.workers[idx].is_processing() {
                continue;
            }

            if self.pool.workers[idx].current_binary.is_none() {
                // Worker has no task — try to assign
                if !self.handle_worker_without_task(worker_id, active_workers, allow_stop, phase) {
                    continue;
                }
                // If marked for assignment, do it now
                if self.pending_worker_assignments.contains(&worker_id) {
                    self.try_assign_normal(worker_id, factory).await;
                    self.pending_worker_assignments.remove(&worker_id);
                }
            }
        }
    }

    pub(super) fn handle_worker_without_task(
        &mut self,
        worker_id: WorkerId,
        active_workers: &mut HashSet<WorkerId>,
        allow_stop: bool,
        _phase: ProcessingPhase,
    ) -> bool {
        // Synchronous decision: peek at the affinity-ordered candidate
        // slice for this worker and ask the scheduler whether anything
        // fits. The actual `take` happens asynchronously in
        // `try_assign_normal` below; we just decide here whether to keep
        // the worker active or stop it.
        let worker_info = self.pool.workers[worker_id as usize].budget_info();
        let all_infos = self.pool.budget_infos();
        let max = self.max_resources();
        let view = self.pool_ref().view_for_worker(worker_id, None);
        let decision = self.scheduler.assign_normal(
            &worker_info,
            &all_infos,
            view.as_slice(),
            max,
            &self.estimator,
            false,
        );

        match decision {
            AssignmentDecision::Assign { .. } => {
                // Mark for async assignment; try_assign_normal will
                // re-run the scheduler against a fresh view and take.
                self.pending_worker_assignments.insert(worker_id);
                true
            }
            AssignmentDecision::NoFit => {
                // Retry with retry_attempt=true
                let view2 = self.pool_ref().view_for_worker(worker_id, None);
                let decision2 = self.scheduler.assign_normal(
                    &worker_info,
                    &all_infos,
                    view2.as_slice(),
                    max,
                    &self.estimator,
                    true,
                );
                match decision2 {
                    AssignmentDecision::Assign { .. } => {
                        self.pending_worker_assignments.insert(worker_id);
                        true
                    }
                    _ => {
                        if self.pool_ref().is_empty() {
                            if allow_stop {
                                tracing::info!(worker_id, "stopping (no more tasks)");
                            }
                            active_workers.remove(&worker_id);
                            false
                        } else {
                            true // idle but binaries remain
                        }
                    }
                }
            }
            AssignmentDecision::NoPendingTasks => {
                if allow_stop {
                    tracing::info!(worker_id, "stopping (no more tasks)");
                }
                active_workers.remove(&worker_id);
                false
            }
        }
    }

    pub(super) async fn try_assign_normal(
        &mut self,
        worker_id: WorkerId,
        factory: &mut impl WorkerFactory<M>,
    ) {
        let worker_info = self.pool.workers[worker_id as usize].budget_info();
        let all_infos = self.pool.budget_infos();
        let max = self.max_resources();
        let view = self.pool_ref().view_for_worker(worker_id, None);
        let decision = self.scheduler.assign_normal(
            &worker_info,
            &all_infos,
            view.as_slice(),
            max,
            &self.estimator,
            false,
        );

        match decision {
            AssignmentDecision::Assign {
                binary_index,
                estimated_usage,
                opportunistic,
                ..
            } => {
                // Owned consumption ticket — the view's last use,
                // releasing the pool borrow for the take below.
                let selection = view.select(binary_index);
                let binary = self.pool_mut().take_selected(selection);
                let name = binary
                    .path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default();
                let estimated_mb = estimated_usage.get(&ResourceKind::memory()) / (1024 * 1024);
                // Assemble the predecessor-outputs map BEFORE the
                // worker-mutation borrow opens — the gather reads
                // from `self.task_outputs_cache` (direct deps) and
                // `self.task_payloads` (inherit_outputs ancestry
                // walk via each predecessor's TaskInfo). Both
                // closures are shared-borrow over `self`, so the
                // gather must complete before `&mut self.pool` for
                // the `ensure_worker_for_type` / `assign_task`
                // path. Identical wire shape to the distributed
                // primary's call into the same core helper.
                let predecessor_outputs = gather_predecessor_outputs(
                    &binary.task_depends_on,
                    |phase_id, task_id| {
                        self.task_outputs_cache
                            .get(&(phase_id.clone(), task_id.to_string()))
                            .cloned()
                    },
                    |phase_id, task_id| {
                        self.task_payloads
                            .iter()
                            .find(|(t, _)| t.task_id == task_id && &t.phase_id == phase_id)
                            .map(|(t, _)| t.task_depends_on.clone())
                    },
                );
                // Per-type subprocess dispatch: if the worker's loaded
                // type does not match this task's `type_id`, the pool
                // kills + respawns the subprocess through
                // `WorkerFactory::spawn_worker_for_type` before the
                // assignment proceeds. Same-type assignments are a
                // no-op fast path.
                let print_pid = self.config.print_pid;
                if let Err(e) = self
                    .pool
                    .ensure_worker_for_type(worker_id, &binary.type_id, factory, print_pid)
                    .await
                {
                    self.pool_mut().requeue(binary);
                    self.handle_assignment_failure(worker_id, &e, factory).await;
                    return;
                }
                let worker = &mut self.pool.workers[worker_id as usize];
                match worker
                    .assign_task(
                        crate::manager::own_task(binary.clone()),
                        estimated_usage.clone(),
                        opportunistic,
                        predecessor_outputs,
                    )
                    .await
                {
                    Ok(()) => {
                        self.total_assigned_resources.add(&estimated_usage);
                        self.notify_sampler_assigned(worker_id, &binary);
                        tracing::info!(
                            worker_id,
                            binary = %name,
                            estimated_mb,
                            "assigned task"
                        );
                        // Reset failure count on success
                        self.pool.workers[worker_id as usize].assignment_failure_count = 0;
                    }
                    Err(e) => {
                        // Put binary back at the front of its bucket — it
                        // was in-flight (take_selected bumped in-flight)
                        // and now needs to be re-attempted.
                        self.pool_mut().requeue(binary);
                        self.handle_assignment_failure(worker_id, &e, factory).await;
                    }
                }
            }
            AssignmentDecision::NoFit | AssignmentDecision::NoPendingTasks => {}
        }
    }

    /// Handle assignment failure with restart and 3-attempt limit.
    pub(super) async fn handle_assignment_failure(
        &mut self,
        worker_id: WorkerId,
        error_msg: &str,
        factory: &mut impl WorkerFactory<M>,
    ) {
        let worker = &mut self.pool.workers[worker_id as usize];
        worker.assignment_failure_count += 1;
        let count = worker.assignment_failure_count;

        tracing::warn!(
            worker_id,
            failure_count = count,
            error = %error_msg,
            "assignment failure"
        );

        if count >= 3 {
            tracing::error!(
                worker_id,
                attempts = count,
                "worker failed to receive assignments after 3 attempts, communication broken"
            );
            // In Python this raises RuntimeError, crashing the manager.
            // Here we panic to match that behavior.
            panic!(
                "Worker {worker_id} failed to receive assignments after {count} attempts. \
                 Communication channel is broken."
            );
        }

        // Restart the worker
        tracing::info!(
            worker_id,
            attempt = count,
            "restarting worker after assignment failure"
        );
        self.restart_worker(worker_id, factory).await;
        self.pending_worker_assignments.insert(worker_id);
    }

    /// Restart a worker: stop the old one, spawn a new transport via factory.
    /// Mid-run respawn failures are logged and the worker is left stopped;
    /// the orchestrator continues with the remaining workers rather than
    /// aborting the whole run for one slot.
    pub(super) async fn restart_worker(
        &mut self,
        worker_id: WorkerId,
        factory: &mut impl WorkerFactory<M>,
    ) {
        if let Err(e) = self
            .pool
            .restart_worker(worker_id, factory, self.config.print_pid)
            .await
        {
            tracing::error!(worker_id, error = %e, "worker restart failed; slot will remain stopped");
        }
    }
}
