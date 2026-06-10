//! Secondary control-plane ingress commands.
//!
//! Single concern: the typed command vocabulary external surfaces
//! (today: the PyO3 `SecondaryHandle` handed to the consumer's
//! `worker_message_listener`) use to ask THIS secondary's operational
//! loop to act through the secondary's control plane — its own
//! workers, or the mesh leg to the primary. The dispatch-decoupling
//! law applies: listener code never touches the pool or the mesh
//! directly — it queues a command here and the `process_tasks` select
//! drains it on the loop's own thread, where the pool and the
//! coordinator live.
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

    /// Relay one consumer custom message to whoever currently holds
    /// the primary role, via this secondary's
    /// `send_custom_to_primary` seam (which owns the `msg_seq`
    /// idempotency stamp and hands the frame to the `send_to_primary`
    /// chokepoint — retention/replay for the important class, no-route
    /// absorb for the droppable class).
    ///
    /// `data` must be ≤
    /// `dynrunner_protocol_primary_secondary::CUSTOM_MESSAGE_MAX_BYTES`;
    /// the API call site (`SecondaryHandle.send_to_primary`) rejects
    /// oversize payloads with a `ValueError` naming size + limit, and
    /// the seam re-checks defensively.
    SendToPrimary {
        topic: String,
        data: Vec<u8>,
        important: bool,
    },
}
