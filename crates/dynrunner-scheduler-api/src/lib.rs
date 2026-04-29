use dynrunner_core::{TaskInfo, Identifier, ResourceMap, WorkerId};

/// Processing phases that the manager cycles through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingPhase {
    InitialAssignment,
    MainPhase,
    RetryPhase,
    ResourcePressurePhase,
    UnassignedPhase,
    Complete,
}

/// A worker's scheduling state (visible to the scheduler for decisions).
#[derive(Debug, Clone)]
pub struct WorkerBudgetInfo<I: Identifier> {
    pub worker_id: WorkerId,
    pub reserved_budgets: ResourceMap,
    pub actual_usage: ResourceMap,
    pub is_idle: bool,
    pub is_opportunistic: bool,
    pub has_initial_assignment: bool,
    pub current_task: Option<TaskInfo<I>>,
    pub estimated_usage: ResourceMap,
}

/// Decision made by the scheduler about one assignment.
#[derive(Debug)]
pub enum AssignmentDecision {
    Assign {
        worker_id: WorkerId,
        binary_index: usize,
        estimated_usage: ResourceMap,
        opportunistic: bool,
    },
    NoFit,
    NoPendingTasks,
}

/// Decision about resource pressure killing.
#[derive(Debug)]
pub enum ResourcePressureDecision {
    Kill { worker_id: WorkerId, reason: String },
    NoAction,
}

/// Abstract resource estimation function, provided by the task definition.
pub trait ResourceEstimator {
    fn estimate(&self, binary_size: u64) -> ResourceMap;
}

/// The scheduler trait — stateless decisions based on current state snapshot.
///
/// The scheduler does not own any state. All needed information is passed
/// as parameters, making it trivially testable and composable.
///
/// Generic over `I` (identifier type) so it works with any task definition.
pub trait Scheduler<I: Identifier> {
    /// Calculate initial budget for a worker given its index.
    fn initial_budget(&self, worker_index: u32, max_resources: &ResourceMap) -> ResourceMap;

    /// Called during the initial assignment phase for one worker.
    fn assign_initial(
        &self,
        worker: &WorkerBudgetInfo<I>,
        pending: &[TaskInfo<I>],
        total_assigned: &ResourceMap,
        max_resources: &ResourceMap,
        estimator: &dyn ResourceEstimator,
    ) -> AssignmentDecision;

    /// Called during the normal phase for one idle worker.
    fn assign_normal(
        &self,
        worker: &WorkerBudgetInfo<I>,
        all_workers: &[WorkerBudgetInfo<I>],
        pending: &[TaskInfo<I>],
        max_resources: &ResourceMap,
        estimator: &dyn ResourceEstimator,
        retry_attempt: bool,
    ) -> AssignmentDecision;

    /// Check resource pressure and decide whether to kill a worker.
    fn check_resource_pressure(
        &self,
        workers: &[WorkerBudgetInfo<I>],
        max_resources: &ResourceMap,
        in_pressure_phase: bool,
    ) -> ResourcePressureDecision;
}
