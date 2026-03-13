use db_comm_api_base::{BinaryInfo, Identifier, MemoryBytes, WorkerId};

/// Processing phases that the manager cycles through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingPhase {
    InitialAssignment,
    MainPhase,
    RetryPhase,
    OomPhase,
    UnassignedPhase,
    Complete,
}

/// A worker's scheduling state (visible to the scheduler for decisions).
#[derive(Debug, Clone)]
pub struct WorkerBudgetInfo<I: Identifier> {
    pub worker_id: WorkerId,
    pub reserved_budget: MemoryBytes,
    pub actual_memory_usage: MemoryBytes,
    pub is_idle: bool,
    pub is_opportunistic: bool,
    pub has_initial_assignment: bool,
    pub current_task: Option<BinaryInfo<I>>,
    pub estimated_memory: MemoryBytes,
}

/// Decision made by the scheduler about one assignment.
#[derive(Debug)]
pub enum AssignmentDecision {
    /// Assign this binary to this worker with this estimated memory.
    /// The `binary_index` is the index into the pending list that was chosen.
    Assign {
        worker_id: WorkerId,
        binary_index: usize,
        estimated_memory: MemoryBytes,
        opportunistic: bool,
    },
    /// No suitable task found for this worker right now.
    NoFit,
    /// No more pending tasks at all.
    NoPendingTasks,
}

/// Decision about OOM killing.
#[derive(Debug)]
pub enum OomDecision {
    /// Kill this worker (it is the victim).
    Kill { worker_id: WorkerId, reason: String },
    /// No action needed.
    NoAction,
}

/// Abstract memory estimation function, provided by the task definition.
pub trait MemoryEstimator {
    fn estimate_memory(&self, binary_size: u64) -> MemoryBytes;
}

/// The scheduler trait — stateless decisions based on current state snapshot.
///
/// The scheduler does not own any state. All needed information is passed
/// as parameters, making it trivially testable and composable.
///
/// Generic over `I` (identifier type) so it works with any task definition.
pub trait Scheduler<I: Identifier> {
    /// Calculate initial budget for a worker given its index.
    fn initial_budget(&self, worker_index: u32, max_memory: MemoryBytes) -> MemoryBytes;

    /// Called during the initial assignment phase for one worker.
    fn assign_initial(
        &self,
        worker: &WorkerBudgetInfo<I>,
        pending: &[BinaryInfo<I>],
        total_assigned_memory: MemoryBytes,
        max_memory: MemoryBytes,
        estimator: &dyn MemoryEstimator,
    ) -> AssignmentDecision;

    /// Called during the normal phase for one idle worker.
    fn assign_normal(
        &self,
        worker: &WorkerBudgetInfo<I>,
        all_workers: &[WorkerBudgetInfo<I>],
        pending: &[BinaryInfo<I>],
        max_memory: MemoryBytes,
        estimator: &dyn MemoryEstimator,
        retry_attempt: bool,
    ) -> AssignmentDecision;

    /// Check memory pressure and decide whether to kill a worker.
    fn check_oom(
        &self,
        workers: &[WorkerBudgetInfo<I>],
        max_memory: MemoryBytes,
        in_oom_phase: bool,
    ) -> OomDecision;
}
