use dynrunner_core::{Identifier, ResourceMap, TaskInfo, WorkerId};

pub mod pending_pool;
pub use pending_pool::{
    BucketKey, IngestPartition, PendingPool, PendingPoolError, PhaseState, ReservationKey,
    ViewSelection, WorkerView,
};

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

/// Why the scheduler chose to kill a specific worker.
///
/// Discriminates "no-fault" preemption (the worker was running an
/// opportunistic task, or its actual usage is still below its reserved
/// budget when the system tipped into overall pressure) from "at-fault"
/// OOM kills where the worker really did exceed what it had been
/// promised. Downstream callers route the displaced task differently:
/// no-fault → silent requeue to the pool front; at-fault → counts
/// against the task's retry budget and reports as `ResourceExhausted`.
///
/// All four variants are inputs to the scheduling decision the moment
/// the victim is picked; no extra plumbing is required because the
/// classifier has the worker's `is_opportunistic`, `actual_usage`, and
/// `reserved_budgets` already in scope via [`WorkerBudgetInfo`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillReason {
    /// Median opportunistic victim picked under pressure. The worker
    /// signed up for being killable when it was assigned an
    /// opportunistic task; the task itself is innocent.
    NoFaultMemoryStealing,
    /// Smallest active worker picked under pressure, AND its actual
    /// usage stayed under its reserved budget. The worker was not the
    /// cause of pressure (another worker overshot, or external load
    /// drove the cgroup up); the task is innocent.
    NoFaultUnderBudget,
    /// Smallest active worker picked under pressure, and its actual
    /// usage is at or above the budget that was reserved for it. The
    /// task overshot its estimate; counts against retry budget.
    OomOverBudget,
    /// Pressure exceeded the effective cap, no opportunistic candidate
    /// was available, and the picked smallest-active is the only
    /// option. Counts as at-fault for retry-budget accounting (this is
    /// the same as `OomOverBudget` from the wire's perspective; kept
    /// distinct so logging and metrics can tell the operator that no
    /// safer victim existed).
    OomLastResort,
}

impl KillReason {
    /// True for the two no-fault variants — caller should silently
    /// requeue the displaced task without consuming retry budget.
    pub fn is_no_fault(self) -> bool {
        matches!(
            self,
            KillReason::NoFaultMemoryStealing | KillReason::NoFaultUnderBudget
        )
    }

    /// Short label suitable for log fields. The legacy free-form
    /// `tracing::warn!(reason = %reason, ...)` line in the pool keeps
    /// reading a stable string by going through this.
    pub fn as_str(self) -> &'static str {
        match self {
            KillReason::NoFaultMemoryStealing => "no_fault_memory_stealing",
            KillReason::NoFaultUnderBudget => "no_fault_under_budget",
            KillReason::OomOverBudget => "oom_over_budget",
            KillReason::OomLastResort => "oom_last_resort",
        }
    }
}

impl std::fmt::Display for KillReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Decision about resource pressure killing.
#[derive(Debug)]
pub enum ResourcePressureDecision {
    Kill {
        worker_id: WorkerId,
        reason: KillReason,
    },
    NoAction,
}

/// Abstract resource estimation function, provided by the task definition.
///
/// Generic over `I` (identifier type) so the implementation can dispatch
/// on `task.type_id`, read fields from `task.payload`, etc.
pub trait ResourceEstimator<I: Identifier> {
    /// Memory budget the worker should reserve before running this item.
    /// Receives the full `TaskInfo` so the implementation can dispatch
    /// on `task.type_id`, read fields from `task.payload`, etc.
    fn estimate(&self, task: &TaskInfo<I>) -> ResourceMap;
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
    ///
    /// `pending` is an ordered list of BORROWED candidates (the shape a
    /// clone-free [`WorkerView`] exposes); the scheduler answers with a
    /// positional `binary_index` into it.
    fn assign_initial(
        &self,
        worker: &WorkerBudgetInfo<I>,
        pending: &[&TaskInfo<I>],
        total_assigned: &ResourceMap,
        max_resources: &ResourceMap,
        estimator: &dyn ResourceEstimator<I>,
    ) -> AssignmentDecision;

    /// Called during the normal phase for one idle worker.
    ///
    /// `pending` is an ordered list of BORROWED candidates (the shape a
    /// clone-free [`WorkerView`] exposes); the scheduler answers with a
    /// positional `binary_index` into it.
    fn assign_normal(
        &self,
        worker: &WorkerBudgetInfo<I>,
        all_workers: &[WorkerBudgetInfo<I>],
        pending: &[&TaskInfo<I>],
        max_resources: &ResourceMap,
        estimator: &dyn ResourceEstimator<I>,
        retry_attempt: bool,
    ) -> AssignmentDecision;

    /// Check resource pressure and decide whether to kill a worker.
    ///
    /// `in_pressure_phase` is the manager-side authority on "the
    /// cluster has crossed an OOM-pressure boundary and active
    /// preempt is now warranted". Implementations MUST short-circuit
    /// to `ResourcePressureDecision::NoAction` when the flag is
    /// `false` — outside an explicit pressure phase, no kill should
    /// fire even if `actual_usage` overshoots `effective_max` (an
    /// overshoot without a pressure-phase entry is the manager's
    /// concern, not the scheduler's). The flag is set by the
    /// manager-side phase machine (see
    /// `dynrunner-manager-local::manager::phases`) when the
    /// pre-existing in-flight backlog should be drained before any
    /// new task is dispatched.
    fn check_resource_pressure(
        &self,
        workers: &[WorkerBudgetInfo<I>],
        max_resources: &ResourceMap,
        in_pressure_phase: bool,
    ) -> ResourcePressureDecision;
}
