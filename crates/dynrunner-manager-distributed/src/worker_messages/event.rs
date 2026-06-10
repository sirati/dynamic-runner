//! Worker custom-message event type.
//!
//! Why this exists: a worker streams consumer custom messages
//! (`Response::Custom`, e.g. descriptor batches / progress pings)
//! mid-task; the secondary's worker-event bridge surfaces each one to
//! the consumer's `worker_message_listener` hook. The hook runs
//! consumer Python, so it must fire OFF the operational loop — the
//! bridge enqueues this event onto the dispatcher mpsc instead of
//! invoking listeners inline.
//!
//! The single concern of this module is the *shape* of the event the
//! worker-event bridge enqueues onto the dispatcher mpsc; no emission
//! logic, no consumer logic lives here.
//!
//! Symmetric to [`crate::task_completed::event::TaskCompletedEvent`]
//! in dispatch shape; it differs in the trigger (a node-local worker
//! wire frame, not a CRDT apply). Deliberately NODE-LOCAL — never
//! CRDT-resident: no primary decision consumes worker→secondary
//! customs, so replicating them would violate the
//! no-observer-only-CRDT law. The lawful replicated twin is the
//! secondary→primary custom-message channel (feature 5), a separate
//! concern.

use dynrunner_core::WorkerId;

/// One consumer custom message received from a worker on this node's
/// own pool (a non-terminal `Response::Custom` frame).
///
/// Field semantics:
/// - `worker_id`: the pool slot whose subprocess sent the message.
///   The stale-generation gate already ran at the worker-event
///   bridge — a buffered custom from a replaced subprocess never
///   reaches this event.
/// - `type_id`: the `TypeId` of the task the worker was running when
///   it sent the message (the consumer's routing context: which
///   worker *kind* is talking). Empty string when no task identity
///   was resolvable (defensive; the emitting worker is mid-task on
///   every production path).
/// - `topic`: the consumer routing key from `Task.send_message` —
///   the framework never interprets it.
/// - `data`: the opaque payload (≤
///   `dynrunner_protocol_manager_worker::CUSTOM_MESSAGE_MAX_BYTES`,
///   enforced at the worker API and at the reply chokepoints).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerCustomMessage {
    pub worker_id: WorkerId,
    pub type_id: String,
    pub topic: String,
    pub data: Vec<u8>,
}
