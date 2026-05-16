//! [`WorkerEvent`] — the message a worker emits to the manager.
//!
//! All worker→manager state transitions surface as one of the
//! variants here; the manager loop in [`crate::manager`] dispatches
//! on the tag.

use dynrunner_core::{Identifier, ResourceMap, TaskInfo, TaskResult, WorkerId};

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
        binary: Option<TaskInfo<I>>,
        estimated_resources: ResourceMap,
    },
    Disconnected {
        worker_id: WorkerId,
        result: TaskResult,
        binary: Option<TaskInfo<I>>,
    },
    PhaseUpdate {
        worker_id: WorkerId,
        phase_name: String,
    },
    Keepalive {
        worker_id: WorkerId,
    },
}
