//! Secondary control-plane ingress commands.
//!
//! Single concern: the typed command vocabulary external surfaces
//! (today: the PyO3 `SecondaryHandle` handed to the consumer's
//! `worker_message_listener`) use to ask THIS secondary's operational
//! loop to act on its own workers. The dispatch-decoupling law
//! applies: listener code never touches the pool directly — it queues
//! a command here and the `process_tasks` select drains it on the
//! loop's own thread, where the pool lives.
//!
//! The channel is per-coordinator, minted in
//! `SecondaryCoordinator::new`; clone senders via
//! [`super::SecondaryCoordinator::secondary_control_sender`].

use dynrunner_core::WorkerId;

/// One externally-issued command against this secondary's own worker
/// pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecondaryControlCommand {
    /// Queue a `Command::Custom { topic, data }` to the given worker
    /// slot (the consumer reply direction of the worker↔secondary
    /// custom-message channel). Legal whatever the slot's protocol
    /// state: an Idle slot flushes inline, a Processing slot's poll
    /// task drains it onto the full-duplex socket mid-task, a
    /// Transitioning slot queues until the next flush point — see
    /// `dynrunner_manager_local::worker::WorkerHandle::send_custom`.
    ///
    /// `data` must be ≤
    /// `dynrunner_protocol_manager_worker::CUSTOM_MESSAGE_MAX_BYTES`;
    /// the API call site (`SecondaryHandle.send_to_worker`) rejects
    /// oversize payloads with a `ValueError` naming size + limit, and
    /// the pool chokepoint re-checks defensively.
    SendToWorker {
        worker_id: WorkerId,
        topic: String,
        data: Vec<u8>,
    },
}
