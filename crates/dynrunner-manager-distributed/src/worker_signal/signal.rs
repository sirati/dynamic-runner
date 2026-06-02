//! Worker-management signal value type.
//!
//! Single concern: the shape of the values that flow from phase/task
//! management to worker management WITHOUT phase/task management calling
//! worker management directly. The phase/task side `emit_worker_mgmt`s a
//! signal onto the bus; the worker-management side drains a coalesced
//! batch of these from inside its operational `select!`. The bus is the
//! only synchronization crossing — neither side holds a reference to the
//! other.
//!
//! The signal is opaque to the apply path / emit side: it only states
//! that something happened that worker management may need to react to
//! (tasks became available, a phase started that needs workers, the run
//! should be failed). The drain side owns the policy for what to do with
//! each signal; the emit side knows nothing about worker management's
//! internals.

use dynrunner_core::PhaseId;

/// One worker-management signal. Emitted by phase/task management onto
/// the bus installed via
/// [`crate::cluster_state::ClusterState::install_worker_mgmt_sender`];
/// drained as a coalesced batch by
/// [`super::drain_worker_signal_batch`] from inside worker
/// management's operational `select!`.
///
/// Each variant carries the full payload worker management needs to act
/// on the signal, captured at emit time so the drain side reads a
/// consistent value without re-acquiring any of the emit side's borrows.
/// Variants are coalesced under the idle-window batching rule in
/// [`super::pipeline`] — a burst becomes one batch carrying every signal
/// in arrival order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerMgmtSignal {
    /// One or more tasks became available to dispatch. Worker
    /// management re-checks whether any parked/idle worker can now be
    /// assigned. Carries no payload — the recheck reads the current
    /// task view itself.
    TasksAdded,
    /// A phase started and needs at least `min` workers to make
    /// progress. Worker management uses this to drive scale-up toward
    /// the phase's floor.
    PhaseStartedNeedsWorkers {
        /// The phase that started.
        phase: PhaseId,
        /// Minimum worker count the phase needs to make progress.
        min: usize,
    },
    /// The run should be failed. Worker management tears down workers
    /// and propagates the failure. `reason` is a human-readable cause
    /// captured at emit time.
    RunShouldFail {
        /// Human-readable failure cause.
        reason: String,
    },
}
