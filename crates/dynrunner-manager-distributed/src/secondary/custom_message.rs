//! Secondary-side send seam for consumer custom messages (F5).
//!
//! Single concern: turn a consumer `(topic, data, important)` request
//! into a well-formed [`DistributedMessage::CustomMessage`] — size-gate
//! the payload, stamp the per-origin `msg_seq` idempotency key — and
//! hand it to the `send_to_primary` chokepoint, which owns EVERYTHING
//! downstream (the `delivery_seq` stamp, the #352 retention/replay for
//! the important class, the no-route absorb for the droppable class).
//! This seam owns NO retention/route knowledge and the chokepoint owns
//! NO custom-message shape knowledge.
//!
//! Callers: the consumer-facing `SecondaryHandle.send_to_primary`
//! (PyO3) reaches this through the secondary's operational loop — a
//! queued `SecondaryControlCommand::SendToPrimary` drained by the
//! loop's control arm (`processing/process_tasks.rs`), per the
//! dispatch-decoupling law.

use dynrunner_core::Identifier;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{CUSTOM_MESSAGE_MAX_BYTES, DistributedMessage};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::SecondaryCoordinator;
use super::wire::timestamp_now;

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Send one consumer custom message to whoever currently holds the
    /// primary role (F5).
    ///
    /// * Size gate: `data` over [`CUSTOM_MESSAGE_MAX_BYTES`] is rejected
    ///   HERE, before framing, naming size + limit (the
    ///   `publish_string` precedent) — the Python API surfaces it as a
    ///   `ValueError`.
    /// * Idempotency key: stamps the next per-origin `msg_seq`
    ///   (monotonic from 1) on IMPORTANT messages ONLY; with this
    ///   secondary's id it dedups transport replays at the primary's
    ///   CRDT inbox. Droppables are UNSEQUENCED (`msg_seq = 0`, a
    ///   sentinel the primary's droppable path never reads): a
    ///   droppable is legitimately lost on no-route/failover, so it
    ///   must never occupy a slot in the identity space the
    ///   terminal-ordering gate counts — otherwise a lost droppable
    ///   would leave a PERMANENT gap below a terminal's
    ///   `msgs_posted_through` stamp and wedge that gate forever. The
    ///   important-only counter keeps the per-origin space DENSE, which
    ///   is also what makes the CRDT's contiguous-prefix watermark
    ///   compaction exact (every transient gap is an in-flight
    ///   important that WILL arrive).
    /// * Delivery class: `important = false` is fire-and-forget through
    ///   the chokepoint (at-most-once, lost on no-route/failover by
    ///   design); `important = true` is `delivery_seq`-stamped and
    ///   retained by the chokepoint until the primary's `TerminalAck`
    ///   confirms the landing (at-least-once; replays re-resolve
    ///   `Destination::Primary`, so a failover mid-flight re-lands at
    ///   the NEW primary).
    ///
    /// The `Err` surfaces ONLY the size rejection: route-level outcomes
    /// are the chokepoint's concern (a no-route is absorbed there into
    /// the retention/probe machinery, never bubbled).
    pub(crate) async fn send_custom_to_primary(
        &mut self,
        topic: String,
        data: Vec<u8>,
        important: bool,
    ) -> Result<(), String> {
        if data.len() > CUSTOM_MESSAGE_MAX_BYTES {
            return Err(format!(
                "custom message data is {} bytes; the limit is {} bytes \
                 (CUSTOM_MESSAGE_MAX_BYTES)",
                data.len(),
                CUSTOM_MESSAGE_MAX_BYTES
            ));
        }
        // IMPORTANT-only sequencing — see the doc above: droppables are
        // unsequenced (0) so the gate-counted identity space stays
        // dense and a lost-by-design droppable can never be awaited.
        let msg_seq = if important {
            let seq = self.next_custom_msg_seq;
            self.next_custom_msg_seq += 1;
            seq
        } else {
            0
        };
        let msg = DistributedMessage::CustomMessage {
            target: None,
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            origin_secondary_id: self.config.secondary_id.clone(),
            msg_seq,
            topic,
            data,
            important,
            // Stamped at the send_to_primary chokepoint (#352),
            // important-only.
            delivery_seq: None,
        };
        // The chokepoint absorbs a no-route into `Ok(())` (retaining the
        // important class for replay; dropping the droppable class by
        // contract), so this only errors on a genuinely-fatal send class
        // — none exists today.
        self.send_to_primary(msg).await
    }
}
