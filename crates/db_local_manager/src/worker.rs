use db_comm_api_base::{
    BinaryInfo, ErrorType, Identifier, ManagerEndpoint, MemoryBytes, TaskResult, WorkerId,
};
use db_manager_runner_comm::state::{
    AssignResult, PollResult, RunnerProtocol, RunnerProtocolState, WaitReadyResult,
};
use db_scheduler_api::WorkerBudgetInfo;

/// Events produced by a worker that the manager reacts to.
#[derive(Debug)]
pub enum WorkerEvent<I: Identifier> {
    Ready {
        worker_id: WorkerId,
    },
    TaskCompleted {
        worker_id: WorkerId,
        result: TaskResult,
        binary: Option<BinaryInfo<I>>,
        estimated_memory: MemoryBytes,
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
pub struct WorkerHandle<M: ManagerEndpoint, I: Identifier> {
    pub worker_id: WorkerId,
    pub reserved_budget: MemoryBytes,
    pub estimated_memory: MemoryBytes,
    pub current_binary: Option<BinaryInfo<I>>,
    pub opportunistic: bool,
    pub has_initial_assignment: bool,
    pub idle: bool,
    pub actual_memory_usage: MemoryBytes,
    pub assignment_failure_count: u32,
    protocol: RunnerProtocolState<M>,
}

impl<M: ManagerEndpoint, I: Identifier> WorkerHandle<M, I> {
    pub fn new(worker_id: WorkerId, transport: M) -> Self {
        let waiting = RunnerProtocol::connect(transport);
        Self {
            worker_id,
            reserved_budget: 0,
            estimated_memory: 0,
            current_binary: None,
            opportunistic: false,
            has_initial_assignment: false,
            idle: false,
            actual_memory_usage: 0,
            assignment_failure_count: 0,
            protocol: RunnerProtocolState::WaitingForReady(waiting),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.protocol.is_idle() || self.protocol.is_processing()
    }

    pub fn is_idle_state(&self) -> bool {
        self.protocol.is_idle()
    }

    pub fn is_processing(&self) -> bool {
        self.protocol.is_processing()
    }

    pub fn is_stopped(&self) -> bool {
        self.protocol.is_stopped()
    }

    /// Build a snapshot for the scheduler.
    pub fn budget_info(&self) -> WorkerBudgetInfo<I> {
        WorkerBudgetInfo {
            worker_id: self.worker_id,
            reserved_budget: self.reserved_budget,
            actual_memory_usage: self.actual_memory_usage,
            is_idle: self.idle && self.current_binary.is_none(),
            is_opportunistic: self.opportunistic,
            has_initial_assignment: self.has_initial_assignment,
            current_task: self.current_binary.clone(),
            estimated_memory: self.estimated_memory,
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
    pub async fn assign_task(
        &mut self,
        binary: BinaryInfo<I>,
        estimated_memory: MemoryBytes,
        opportunistic: bool,
    ) -> Result<(), String> {
        let idle = self
            .protocol
            .take_idle()
            .ok_or_else(|| "worker not in Idle state".to_string())?;

        let relative_path = binary.path.to_string_lossy().into_owned();
        match idle.assign_task(relative_path).await {
            AssignResult::Assigned(processing) => {
                self.protocol = RunnerProtocolState::Processing(processing);
                self.current_binary = Some(binary);
                self.estimated_memory = estimated_memory;
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

    /// Poll for task completion. Returns events if any.
    pub async fn poll_status(&mut self) -> Option<WorkerEvent<I>> {
        let processing = self.protocol.take_processing()?;
        match processing.poll_status().await {
            PollResult::Completed {
                result,
                protocol,
                phase_updates,
            } => {
                self.protocol = RunnerProtocolState::Idle(protocol);
                for phase in &phase_updates {
                    tracing::debug!(
                        worker_id = self.worker_id,
                        phase = %phase,
                        "phase update"
                    );
                }
                let binary = self.current_binary.clone();
                let estimated_memory = self.estimated_memory;
                self.clear_task();
                Some(WorkerEvent::TaskCompleted {
                    worker_id: self.worker_id,
                    result,
                    binary,
                    estimated_memory,
                })
            }
            PollResult::StillRunning {
                protocol,
                phase_updates,
                got_keepalive,
            } => {
                self.protocol = RunnerProtocolState::Processing(protocol);
                if let Some(phase) = phase_updates.into_iter().last() {
                    return Some(WorkerEvent::PhaseUpdate {
                        worker_id: self.worker_id,
                        phase_name: phase,
                    });
                }
                if got_keepalive {
                    return Some(WorkerEvent::Keepalive {
                        worker_id: self.worker_id,
                    });
                }
                None
            }
            PollResult::Disconnected { result, protocol } => {
                self.protocol = RunnerProtocolState::Stopped(protocol);
                let binary = self.current_binary.clone();
                self.clear_task();
                Some(WorkerEvent::Disconnected {
                    worker_id: self.worker_id,
                    result,
                    binary,
                })
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
        self.estimated_memory = 0;
        self.idle = true;
    }

    /// Mark this worker as OOM-killed: clear task, mark opportunistic.
    pub fn mark_oom_killed(&mut self) {
        self.current_binary = None;
        self.estimated_memory = 0;
        self.opportunistic = true;
    }
}
