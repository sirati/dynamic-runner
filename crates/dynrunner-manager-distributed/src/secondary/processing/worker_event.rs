//! Worker-event handler: convert one inbound `WorkerEvent` (task
//! completed, task failed, worker exited unexpectedly, etc.) into the
//! appropriate cluster-mutation broadcast, per-pool bookkeeping, and
//! primary-side ledger update.
//!
//! Single concern: the secondary's worker→cluster bridge. Drives the
//! cluster-mutation apply-and-broadcast path for every authoritative
//! task-outcome the local worker emits.

use std::time::Duration;

use dynrunner_core::{ErrorType, Identifier, MessageReceiver, MessageSender, WorkerId};
use dynrunner_manager_local::oom::{classify_disconnect, OomWatcher};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_manager_local::worker::WorkerEvent;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

/// Same window as the LocalManager path — 500ms covers ~10 samples
/// worth of correlation tolerance at the production 50ms sample
/// cadence between kernel `oom_kill` counter increment and the
/// worker pipe-EOF observation.
const KERNEL_OOM_CORRELATION_WINDOW: Duration = Duration::from_millis(500);

use crate::primary::PrimaryCommand;

use super::super::wire::timestamp_now;
use super::super::SecondaryCoordinator;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// `command_rx` threads the operational-loop's command-channel
    /// receiver into the cascade so a callback-issued `spawn_tasks`
    /// applies inline before the next `drain_empty_active_phases`
    /// poll. Off-loop callers pass `&mut None`.
    pub(in crate::secondary) async fn handle_worker_event(
        &mut self,
        event: WorkerEvent<I>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
        factory: &mut impl WorkerFactory<M>,
        oom_watcher: &OomWatcher,
    ) -> Result<Option<WorkerId>, String> {
        match event {
            WorkerEvent::TaskCompleted {
                worker_id,
                result,
                binary,
                ..
            } => {
                // Test-only: track tasks run on this secondary's own
                // workers. See `local_tasks_run` doc on
                // `SecondaryCoordinator`.
                #[cfg(test)]
                {
                    self.local_tasks_run += 1;
                }
                // Reclaim protocol state from the spawned poll task
                self.pool.workers[worker_id as usize].reclaim_protocol().await;
                self.pool.workers[worker_id as usize].clear_task();

                // Find the file hash for this worker's task
                let file_hash = self
                    .active_tasks
                    .iter()
                    .find(|&(_, &wid)| wid == worker_id)
                    .map(|(hash, _)| hash.clone());
                let log_task_hash = file_hash.clone();

                if let Some(hash) = file_hash {
                    self.active_tasks.remove(&hash);
                    // `completed_tasks` is the "saw it terminate" set;
                    // the primary's dispatch path uses it to
                    // avoid redispatching tasks the cluster has
                    // already finished. For Recoverable failures we
                    // intend to retry, so the hash must NOT land here
                    // — otherwise `handle_primary_task_request` would
                    // filter the re-injected binary out via its
                    // `completed_tasks` retain, and retry silently
                    // becomes a no-op. Mirrors the pre-existing
                    // dispatch.rs::TaskFailed forward and peer.rs::
                    // TaskFailed wire paths, both of which already
                    // skip `completed_tasks` insertion for
                    // Recoverable. The terminal-failure / success
                    // branches still insert below.
                    let recoverable_failure = !result.success
                        && result
                            .error_type
                            .as_ref()
                            .is_some_and(|e| matches!(
                                e,
                                dynrunner_core::ErrorType::Recoverable
                            ));
                    if !recoverable_failure {
                        self.completed_tasks.insert(hash.clone());
                    }

                    if result.success {
                        // Drive the primary's phase machine if
                        // this node is acting as one and dispatched
                        // the task — a no-op otherwise. Mid-run
                        // firing is what unblocks chained phases in
                        // the primary pool.
                        self.note_primary_item_completed(&hash, command_rx).await;
                        // Report completion to the current primary
                        // (whichever node currently holds authority).
                        let msg = DistributedMessage::TaskComplete {
                            sender_id: self.config.secondary_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: self.config.secondary_id.clone(),
                            worker_id,
                            task_hash: hash.clone(),
                            result_data: None,
                        };
                        self.send_to_current_primary(msg.clone()).await?;
                        let _ = self.peer_transport.broadcast(msg).await;
                    } else {
                        // Reuse the upstream classification from the
                        // worker protocol; default to NonRecoverable
                        // only when the worker layer didn't tag the
                        // result (genuine "unknown" — distinct from
                        // the disconnect path which now reports
                        // Recoverable explicitly via worker.rs /
                        // protocol-manager-worker).
                        let error_type = result
                            .error_type
                            .clone()
                            .unwrap_or(ErrorType::NonRecoverable);
                        // Failure-aware variant: Recoverable failures
                        // land in `primary_failed` for the
                        // retry pass. Phase-machine in-flight
                        // bookkeeping is identical to the success
                        // case (decrement + cascade).
                        self.note_primary_item_failed(&hash, &error_type, command_rx).await;
                        // Synchronous drain-check (see peer.rs for
                        // rationale): immediately re-inject if this
                        // was the last in-flight task and there's
                        // retry budget left.
                        self.primary_drain_check_and_retry(factory).await;
                        // Report error to the current primary.
                        let msg = DistributedMessage::TaskFailed {
                            sender_id: self.config.secondary_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: self.config.secondary_id.clone(),
                            worker_id,
                            task_hash: hash.clone(),
                            error_type,
                            error_message: result
                                .error_message
                                .unwrap_or_else(|| "Unknown error".into()),
                        };
                        self.send_to_current_primary(msg.clone()).await?;
                        let _ = self.peer_transport.broadcast(msg).await;
                    }

                    // Request next task for this worker
                    self.request_task_for_worker(worker_id, factory).await?;
                }

                // Operator-facing INFO: did the worker finish a
                // task, and did it succeed? The `secondary_id` is
                // redundant in per-secondary slurm_*.out files
                // (the file IS the secondary). Per-task identity
                // (task_id / phase / task_type) moves to the
                // sibling DEBUG so the operator log stays terse
                // while diagnostic context is preserved.
                tracing::info!(
                    worker_id,
                    task_hash = ?log_task_hash,
                    success = result.success,
                    "task done"
                );
                tracing::debug!(
                    secondary = %self.config.secondary_id,
                    worker_id,
                    task_id = ?binary.as_ref().and_then(|b| b.task_id.as_deref()),
                    phase = ?binary.as_ref().map(|b| b.phase_id.to_string()),
                    task_type = ?binary.as_ref().map(|b| b.type_id.to_string()),
                    task_hash = ?log_task_hash,
                    success = result.success,
                    "task done: identity"
                );

                Ok(None)
            }
            WorkerEvent::Disconnected {
                worker_id,
                result,
                binary,
            } => {
                // Reap the subprocess BEFORE reclaim_protocol so the
                // exit status rides the same log line as the
                // disconnect. `try_reap_exit` is non-blocking; `None`
                // means PID was untracked, kernel hasn't reaped yet
                // (SIGCHLD race), or the child was already reaped by
                // another path. See WorkerHandle::try_reap_exit for
                // the full set of None conditions.
                let exit_status = self.pool.workers[worker_id as usize].try_reap_exit();

                // Reclaim protocol state from the spawned poll task
                self.pool.workers[worker_id as usize].reclaim_protocol().await;
                self.pool.workers[worker_id as usize].clear_task();

                tracing::warn!(
                    worker_id,
                    error = ?result.error_message,
                    exit_status = exit_status.as_ref().map(|s| s.to_string()),
                    "worker disconnected"
                );

                // Find and report the task as failed
                let file_hash = self
                    .active_tasks
                    .iter()
                    .find(|&(_, &wid)| wid == worker_id)
                    .map(|(hash, _)| hash.clone());

                if let Some(hash) = file_hash {
                    self.active_tasks.remove(&hash);

                    // Discriminate two Disconnected-event shapes:
                    //
                    //   A. Worker explicitly reported a real
                    //      task failure on the wire — Response::
                    //      Error(NonRecoverable, msg). The
                    //      communication SUCCEEDED; the message
                    //      WAS delivered. The task ran (or at
                    //      least the worker attempted it) and
                    //      reported a non-recoverable failure.
                    //      → forward as TaskFailed(NonRecoverable)
                    //        so the primary records the real
                    //        failure (consumes retry budget if
                    //        applicable; surfaces in fail_final
                    //        per the outcome-class breakdown).
                    //
                    //   B. Pure transport-level disconnect — no
                    //      wire-level error response, just an
                    //      EOF on the manager's read end (the
                    //      protocol layer at `state.rs:152`
                    //      synthesises `Recoverable +
                    //      "transport disconnected"` for this
                    //      case; `worker.rs:154` synthesises
                    //      `Recoverable + "Disconnected before
                    //      Ready"` for the pre-Ready variant).
                    //      The communication FAILED; the task
                    //      may not have run at all.
                    //      → forward as backpressure-shaped
                    //        TaskFailed (Recoverable + the
                    //        marker the primary's
                    //        `is_backpressure` predicate
                    //        recognises) so the primary REQUEUES
                    //        the task at the pool without
                    //        consuming retry budget. The next
                    //        TaskRequest from the respawned
                    //        worker (or another peer) picks it
                    //        back up.
                    //
                    // Discriminator is `result.error_type`:
                    // NonRecoverable → real failure (A); anything
                    // else (Recoverable + transport-disconnected
                    // synthesis) → comm failure (B).
                    //
                    // Before discrimination, reclassify using the
                    // exit-status + OOM watcher: a SIGKILL with a
                    // recent kernel `oom_kill` increment upgrades
                    // the synthesised `Recoverable` to
                    // `ResourceExhausted(memory)`; SIGSEGV / SIGABRT
                    // / SIGBUS / SIGFPE / SIGILL downgrade to
                    // `NonRecoverable`. See [`classify_disconnect`].
                    let kernel_oom_recent =
                        oom_watcher.kernel_oom_recent(KERNEL_OOM_CORRELATION_WINDOW);
                    let error_type = classify_disconnect(
                        result
                            .error_type
                            .clone()
                            .unwrap_or(ErrorType::Recoverable),
                        exit_status.as_ref(),
                        kernel_oom_recent,
                    );
                    let is_comm_failure =
                        matches!(error_type, ErrorType::Recoverable);

                    let (wire_error_type, wire_error_message) = if is_comm_failure {
                        (
                            ErrorType::Recoverable,
                            "worker pipe broken; respawning".to_string(),
                        )
                    } else {
                        (
                            error_type,
                            result
                                .error_message
                                .unwrap_or_else(|| "Worker disconnected".into()),
                        )
                    };

                    let msg = DistributedMessage::TaskFailed {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        worker_id,
                        task_hash: hash,
                        error_type: wire_error_type,
                        error_message: wire_error_message,
                    };
                    let _ = self.send_to_current_primary(msg.clone()).await;
                    let _ = self.peer_transport.broadcast(msg).await;
                }

                let _ = binary; // binary info already reported

                // Signal that this worker needs restart
                Ok(Some(worker_id))
            }
            WorkerEvent::PhaseUpdate {
                worker_id,
                phase_name,
            } => {
                tracing::debug!(worker_id, phase = %phase_name, "phase update");
                Ok(None)
            }
            WorkerEvent::Keepalive { worker_id } => {
                tracing::trace!(worker_id, "worker keepalive");
                Ok(None)
            }
            WorkerEvent::Ready { worker_id } => {
                tracing::debug!(worker_id, "worker ready");
                Ok(None)
            }
        }
    }
}
