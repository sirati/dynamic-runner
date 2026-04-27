use std::time::Instant;

use db_comm_api_base::{
    BinaryInfo, ErrorType, Identifier, ResourceKind, ResourceMap, TaskResult, WorkerId,
};
use db_manager_runner_comm::ManagerEndpoint;

use crate::monitor::{ProcStatmMonitor, ResourceMonitor};
use db_manager_runner_comm::state::{
    AssignResult, PollResult, Processing, RunnerProtocol, RunnerProtocolState, WaitReadyResult,
};
use db_scheduler_api::WorkerBudgetInfo;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Events produced by a worker that the manager reacts to.
#[derive(Debug)]
pub enum WorkerEvent<I: Identifier> {
    Ready {
        worker_id: WorkerId,
    },
    TaskCompleted {
        worker_id: WorkerId,
        result: TaskResult,
        /// Opaque task-specific payload (the bytes after `done:` on the wire).
        /// `None` if the worker sent a bare `done`.
        result_data: Option<Vec<u8>>,
        binary: Option<BinaryInfo<I>>,
        estimated_resources: ResourceMap,
    },
    Disconnected {
        worker_id: WorkerId,
        result: TaskResult,
        binary: Option<BinaryInfo<I>>,
    },
    PhaseUpdate {
        worker_id: WorkerId,
        phase_name: String,
    },
    Keepalive {
        worker_id: WorkerId,
    },
}

/// Manager-side handle for one worker.
///
/// Wraps the ZST protocol state machine plus per-worker metadata used by the
/// scheduler (budget, current task, opportunistic flag, etc.).
///
/// When a task is assigned, the protocol is moved into a spawned background
/// task that reads from the transport and sends `WorkerEvent`s to a shared
/// channel. This avoids head-of-line blocking when polling multiple workers.
pub struct WorkerHandle<M: ManagerEndpoint, I: Identifier> {
    pub worker_id: WorkerId,
    pub reserved_budgets: ResourceMap,
    pub estimated_resources: ResourceMap,
    pub current_binary: Option<BinaryInfo<I>>,
    pub opportunistic: bool,
    pub has_initial_assignment: bool,
    pub idle: bool,
    pub actual_usage: ResourceMap,
    pub assignment_failure_count: u32,
    pub pid: Option<u32>,
    /// Current processing phase name (set by PhaseUpdate messages).
    pub phase: Option<String>,
    /// Timestamp of the last keepalive or phase update.
    pub last_keepalive: Option<Instant>,
    /// When the worker entered its current phase. Reset on PhaseUpdate.
    pub phase_started_at: Option<Instant>,
    /// Index of the next stuck-worker interval to fire from
    /// `LocalManagerConfig::phase_status_log_intervals`.
    pub phase_status_log_idx: usize,
    protocol: RunnerProtocolState<M>,
    /// Shared channel for sending worker events to the manager.
    event_tx: mpsc::UnboundedSender<WorkerEvent<I>>,
    /// Handle to the background poll task (set while Processing).
    poll_task: Option<JoinHandle<RunnerProtocolState<M>>>,
}

impl<M: ManagerEndpoint + 'static, I: Identifier> WorkerHandle<M, I> {
    pub fn new(
        worker_id: WorkerId,
        transport: M,
        event_tx: mpsc::UnboundedSender<WorkerEvent<I>>,
    ) -> Self {
        let waiting = RunnerProtocol::connect(transport);
        Self {
            worker_id,
            reserved_budgets: ResourceMap::new(),
            estimated_resources: ResourceMap::new(),
            current_binary: None,
            opportunistic: false,
            has_initial_assignment: false,
            idle: false,
            actual_usage: ResourceMap::new(),
            assignment_failure_count: 0,
            pid: None,
            phase: None,
            last_keepalive: None,
            phase_started_at: None,
            phase_status_log_idx: 0,
            protocol: RunnerProtocolState::WaitingForReady(waiting),
            event_tx,
            poll_task: None,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.protocol.is_idle() || self.protocol.is_processing()
    }

    pub fn is_idle_state(&self) -> bool {
        self.protocol.is_idle()
    }

    pub fn is_processing(&self) -> bool {
        // Transitioning means the protocol is in a spawned poll task
        self.protocol.is_processing() || self.poll_task.is_some()
    }

    pub fn is_stopped(&self) -> bool {
        self.protocol.is_stopped()
    }

    /// Build a snapshot for the scheduler.
    pub fn budget_info(&self) -> WorkerBudgetInfo<I> {
        WorkerBudgetInfo {
            worker_id: self.worker_id,
            reserved_budgets: self.reserved_budgets.clone(),
            actual_usage: self.actual_usage.clone(),
            is_idle: self.idle && self.current_binary.is_none(),
            is_opportunistic: self.opportunistic,
            has_initial_assignment: self.has_initial_assignment,
            current_task: self.current_binary.clone(),
            estimated_usage: self.estimated_resources.clone(),
        }
    }

    /// Try to advance from WaitingForReady → Idle.
    pub async fn poll_ready(&mut self) -> Option<WorkerEvent<I>> {
        let waiting = self.protocol.take_waiting()?;
        match waiting.wait_ready().await {
            WaitReadyResult::Ready(idle) => {
                self.protocol = RunnerProtocolState::Idle(idle);
                self.idle = true;
                Some(WorkerEvent::Ready {
                    worker_id: self.worker_id,
                })
            }
            WaitReadyResult::NotYet(w) => {
                self.protocol = RunnerProtocolState::WaitingForReady(w);
                None
            }
            WaitReadyResult::Disconnected(s) => {
                self.protocol = RunnerProtocolState::Stopped(s);
                Some(WorkerEvent::Disconnected {
                    worker_id: self.worker_id,
                    result: TaskResult::error(
                        ErrorType::NonRecoverable,
                        "Disconnected before Ready".into(),
                    ),
                    binary: None,
                })
            }
        }
    }

    /// Assign a task to this worker. Transitions Idle → Processing.
    ///
    /// Spawns a background task that reads from the transport and sends
    /// `WorkerEvent`s to the shared event channel. The manager receives
    /// events for all workers from a single channel without blocking.
    pub async fn assign_task(
        &mut self,
        binary: BinaryInfo<I>,
        estimated_resources: ResourceMap,
        opportunistic: bool,
    ) -> Result<(), String> {
        let idle = self
            .protocol
            .take_idle()
            .ok_or_else(|| "worker not in Idle state".to_string())?;

        let relative_path = binary.path.to_string_lossy().into_owned();
        match idle.assign_task(relative_path).await {
            AssignResult::Assigned(processing) => {
                // Spawn a background task that polls the worker protocol.
                let worker_id = self.worker_id;
                let binary_clone = binary.clone();
                let tx = self.event_tx.clone();

                let est_clone = estimated_resources.clone();
                let handle = tokio::task::spawn_local(async move {
                    Self::poll_loop(processing, worker_id, binary_clone, est_clone, tx).await
                });

                self.poll_task = Some(handle);
                // Protocol is now owned by the spawned task; mark as Transitioning
                self.protocol = RunnerProtocolState::Transitioning;
                self.current_binary = Some(binary);
                self.estimated_resources = estimated_resources;
                self.opportunistic = opportunistic;
                self.has_initial_assignment = true;
                self.idle = false;
                self.assignment_failure_count = 0;
                Ok(())
            }
            AssignResult::SendFailed { error, protocol } => {
                self.protocol = RunnerProtocolState::Stopped(protocol);
                Err(error)
            }
        }
    }

    /// Background poll loop: reads responses from the transport, sends events
    /// to the shared channel, returns the final protocol state.
    async fn poll_loop(
        mut processing: RunnerProtocol<Processing, M>,
        worker_id: WorkerId,
        binary: BinaryInfo<I>,
        estimated_resources: ResourceMap,
        tx: mpsc::UnboundedSender<WorkerEvent<I>>,
    ) -> RunnerProtocolState<M> {
        loop {
            match processing.poll_status().await {
                PollResult::Completed { result, result_data, protocol } => {
                    let _ = tx.send(WorkerEvent::TaskCompleted {
                        worker_id,
                        result,
                        result_data,
                        binary: Some(binary),
                        estimated_resources,
                    });
                    return RunnerProtocolState::Idle(protocol);
                }
                PollResult::StillRunning {
                    protocol,
                    phase_update,
                    got_keepalive,
                } => {
                    processing = protocol;
                    if let Some(phase) = phase_update {
                        let _ = tx.send(WorkerEvent::PhaseUpdate {
                            worker_id,
                            phase_name: phase,
                        });
                    } else if got_keepalive {
                        let _ = tx.send(WorkerEvent::Keepalive { worker_id });
                    }
                    // Loop to read the next response
                }
                PollResult::Disconnected { result, protocol } => {
                    let _ = tx.send(WorkerEvent::Disconnected {
                        worker_id,
                        result,
                        binary: Some(binary),
                    });
                    return RunnerProtocolState::Stopped(protocol);
                }
            }
        }
    }

    /// Reclaim the protocol state from the background poll task after a
    /// terminal event (TaskCompleted or Disconnected) has been received.
    ///
    /// Must be called after receiving a terminal WorkerEvent for this worker.
    pub async fn reclaim_protocol(&mut self) {
        if let Some(handle) = self.poll_task.take() {
            match handle.await {
                Ok(state) => {
                    self.protocol = state;
                }
                Err(e) => {
                    tracing::error!(
                        worker_id = self.worker_id,
                        error = %e,
                        "poll task panicked"
                    );
                    // Can't recover the transport — mark as stopped with a
                    // placeholder. The manager should restart this worker.
                    self.protocol = RunnerProtocolState::Unconnected;
                }
            }
        }
    }

    /// Send Stop and transition to Stopped.
    pub async fn stop(&mut self) {
        if let Some(idle) = self.protocol.take_idle() {
            let stopped = idle.stop().await;
            self.protocol = RunnerProtocolState::Stopped(stopped);
        }
    }

    /// Clear current task metadata (after completion or OOM kill).
    pub fn clear_task(&mut self) {
        self.current_binary = None;
        self.estimated_resources = ResourceMap::new();
        self.idle = true;
        self.phase = None;
        self.last_keepalive = None;
        self.phase_started_at = None;
        self.phase_status_log_idx = 0;
    }

    /// Mark this worker as OOM-killed: clear task, mark opportunistic.
    pub fn mark_oom_killed(&mut self) {
        self.current_binary = None;
        self.estimated_resources = ResourceMap::new();
        self.opportunistic = true;
    }

    /// Update actual resource usage by reading /proc/[pid]/statm (Linux only).
    pub fn update_resource_usage(&mut self) {
        self.actual_usage = ProcStatmMonitor.measure(self.pid);
    }
}
