//! [`WorkerEvent`] — the message a worker emits to the manager.
//!
//! All worker→manager state transitions surface as one of the
//! variants here; the manager loop in [`crate::manager`] dispatches
//! on the tag.

use dynrunner_core::{Identifier, ResourceMap, TaskInfo, TaskResult, WorkerId};

/// Events produced by a worker that the manager reacts to.
///
/// Every variant carries the `generation` of the subprocess that
/// produced it (captured when the emitting poll task was spawned, so it
/// is immutable for that task's lifetime). A consumer that tracks the
/// slot's CURRENT generation can drop any event whose generation is
/// stale — see [`WorkerEvent::generation`] and the slot-replacement
/// rationale on [`crate::worker::WorkerHandle::generation`].
#[derive(Debug)]
pub enum WorkerEvent<I: Identifier> {
    Ready {
        worker_id: WorkerId,
        generation: u64,
    },
    TaskCompleted {
        worker_id: WorkerId,
        generation: u64,
        result: TaskResult,
        /// Opaque task-specific payload (the bytes after `done:` on the wire).
        /// `None` if the worker sent a bare `done`.
        result_data: Option<Vec<u8>>,
        binary: Option<TaskInfo<I>>,
        estimated_resources: ResourceMap,
    },
    Disconnected {
        worker_id: WorkerId,
        generation: u64,
        result: TaskResult,
        binary: Option<TaskInfo<I>>,
    },
    PhaseUpdate {
        worker_id: WorkerId,
        generation: u64,
        phase_name: String,
    },
    Keepalive {
        worker_id: WorkerId,
        generation: u64,
    },
    /// Worker → manager consumer custom message (a non-terminal
    /// `Response::Custom` observed mid-task). `topic` is the
    /// consumer routing key; `data` the opaque payload (≤
    /// `dynrunner_protocol_manager_worker::CUSTOM_MESSAGE_MAX_BYTES`,
    /// enforced at the worker API). Generation-stamped like every
    /// other event so a buffered custom from a replaced subprocess is
    /// dropped by the stale-event gate instead of reaching the
    /// consumer listener.
    CustomMessage {
        worker_id: WorkerId,
        generation: u64,
        topic: String,
        data: Vec<u8>,
    },
}

impl<I: Identifier> WorkerEvent<I> {
    /// The slot the event was produced for.
    pub fn worker_id(&self) -> WorkerId {
        match self {
            WorkerEvent::Ready { worker_id, .. }
            | WorkerEvent::TaskCompleted { worker_id, .. }
            | WorkerEvent::Disconnected { worker_id, .. }
            | WorkerEvent::PhaseUpdate { worker_id, .. }
            | WorkerEvent::Keepalive { worker_id, .. }
            | WorkerEvent::CustomMessage { worker_id, .. } => *worker_id,
        }
    }

    /// The subprocess generation that produced the event. Compared
    /// against the slot's live generation to reject stale-generation
    /// events (a buffered terminal a respawn could not retract).
    pub fn generation(&self) -> u64 {
        match self {
            WorkerEvent::Ready { generation, .. }
            | WorkerEvent::TaskCompleted { generation, .. }
            | WorkerEvent::Disconnected { generation, .. }
            | WorkerEvent::PhaseUpdate { generation, .. }
            | WorkerEvent::Keepalive { generation, .. }
            | WorkerEvent::CustomMessage { generation, .. } => *generation,
        }
    }
}
