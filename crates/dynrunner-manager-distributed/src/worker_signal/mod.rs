//! Worker-management signal bus module.
//!
//! Single concern: a decoupling signal channel that lets phase/task
//! management signal worker management WITHOUT calling it directly.
//! Two submodules:
//!
//! - [`signal`] defines the [`WorkerMgmtSignal`] value type that flows
//!   from phase/task management (the emit side) to worker management
//!   (the drain side). One variant per kind of thing worker management
//!   may need to react to (`TasksAdded`, `PhaseStartedNeedsWorkers`,
//!   `RunShouldFail`, `PolicyFatalExit`).
//! - [`pipeline`] is the batched drain helper worker management's
//!   operational `select!` awaits. It coalesces a burst of signals with
//!   a 50ms idle window and yields one [`WorkerSignalBatch`] carrying
//!   every signal in arrival order.
//!
//! The module boundary mirrors `fulfillability_matcher`: the emit side
//! NEVER invokes worker management directly — it only `tx.send()`s a
//! signal onto the bus (via
//! [`crate::cluster_state::ClusterState::emit_worker_mgmt`], installed
//! with `install_worker_mgmt_sender`). Worker management's reaction runs
//! strictly off the emit path (its operational loop's `select!` arm), so
//! a slow worker-management reaction cannot stall the emit side.

pub mod pipeline;
pub mod signal;

pub use pipeline::{
    WORKER_SIGNAL_BATCH_IDLE_WINDOW, WorkerSignalBatch, drain_worker_signal_batch,
    try_collect_worker_signal_batch,
};
pub use signal::WorkerMgmtSignal;
