//! Terminal-ordering gate: task terminals never overtake the IMPORTANT
//! custom messages their task causally sent first.
//!
//! Single concern: WHEN a wire task terminal (`TaskComplete` /
//! `TaskFailed`) may be processed, relative to its origin's replicated
//! custom-message inbox. The terminal HANDLERS (`task/complete.rs`,
//! `task/failed.rs`) own what processing means; the custom-message
//! dispatch decision (`custom_message.rs`) owns how the inbox resolves;
//! the CRDT watermark (`cluster_state/apply_custom.rs`) owns what
//! "resolved" is. This module composes those APIs and owns ONLY the
//! defer/admit decision plus the parking lot.
//!
//! # The gate (the asm-dataset run_20260611_005220 race)
//!
//! A task that streams important custom messages and then exits races
//! its own terminal: the messages ride the secondary's control-plane
//! drain while the terminal rides the worker-event arm, and the mesh's
//! redundant terminal forwarding (#50) can reorder the two even when
//! the origin sequenced them. Phase-end derives from terminals, so an
//! overtaking terminal lets `on_phase_end` fire before the consumer's
//! handler saw the messages the phase's own task sent.
//!
//! The origin therefore stamps every terminal with its causal
//! watermark — `msgs_posted_through`, the highest important `msg_seq`
//! it had stamped when the terminal first left (see the secondary's
//! `send_to_primary` chokepoint) — and THIS gate defers the terminal
//! until the origin's replicated terminal watermark
//! ([`crate::cluster_state::ClusterState::custom_terminal_watermark`])
//! covers the stamp. The important `msg_seq` space is dense per origin
//! (droppables are unsequenced — they can never be awaited), so
//! `watermark >= stamp` proves every important message the consumer
//! handed over before the terminal is Handled/Failed-RESOLVED — both
//! terminals open the gate, so a raising handler surfaces through the
//! consumer's own phase-end barrier instead of wedging the run.
//!
//! # Liveness (the gate can never wedge)
//!
//! * The awaited messages are confirmable (#352): retained and
//!   replayed by the origin until the landing is acked, across
//!   failover — while the origin lives, they ARRIVE, the ingest
//!   trigger resolves them, and the same pass releases the terminal.
//! * A DEAD origin's unsent messages died with its retention buffer,
//!   so the gate consults the replicated membership: a non-alive
//!   origin's terminals are admitted unconditionally (lost-with-origin
//!   is the droppable-loss class — the consumer's phase-end barrier
//!   reports the gap). The release pass runs on the heartbeat-cadence
//!   dispatch backstop, which is the same cadence death declarations
//!   ride, so a removal opens the gate within one tick.
//! * An unstamped terminal (pre-field sender) makes no causal claim
//!   and is never deferred.
//!
//! # Re-check cadence (no busy-wait)
//!
//! [`PrimaryCoordinator::release_gated_terminals`] runs at the tail of
//! EVERY [`PrimaryCoordinator::dispatch_unhandled_custom_messages`]
//! pass — the only place the watermark advances on a live primary —
//! so all three dispatch triggers (ingest, heartbeat backstop,
//! promotion replay) are release triggers with no per-site wiring. A
//! watermark advanced by a snapshot restore-merge (anti-entropy pull)
//! is picked up by the next heartbeat backstop pass.
//!
//! # Failover
//!
//! The stamp rides the confirmable report, so a replay re-lands it —
//! stamp intact (sticky, like `delivery_seq`) — at the NEW primary,
//! whose hydrated CRDT carries the merged watermark; the origin's
//! retention re-delivers any unresolved importants there too, and the
//! promotion-replay dispatch resolves inherited `Unhandled` residue.
//! A terminal parked on a DYING primary is acked-but-unprocessed and
//! is lost with it — the standard lost-terminal reconciliation
//! (inherited-slot requeue / re-dispatch) recovers the task, the same
//! class as a death between ack and apply.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{DistributedMessage, MessageType};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use super::PrimaryCoordinator;
use super::command_channel::PrimaryCommand;
use crate::cluster_state::PeerMembership;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Ingest seam for BOTH wire task terminals: admit through the
    /// ordering gate or park. `dispatch_message` routes its
    /// `TaskComplete` / `TaskFailed` arms here; the release pass
    /// re-routes parked frames through the same admit path, so
    /// processing semantics are identical either way.
    ///
    /// The #352 ack already ran (acked-per-landing, before the
    /// handlers); parking after the ack widens the existing
    /// ack-then-die-before-apply window — see the module doc's
    /// failover section for why that degrades safely.
    pub(super) async fn ingest_task_terminal(
        &mut self,
        msg: DistributedMessage<I>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        if !self.terminal_gate_admits(&msg) {
            tracing::debug!(
                origin = ?msg.delivery_reporter(),
                task_hash = ?msg.task_hash(),
                stamp = ?msg.msgs_posted_through(),
                parked = self.gated_terminals.len() + 1,
                "task terminal deferred: its origin's important custom \
                 messages through the stamp are not yet resolved (the \
                 messages are retained/in flight and re-checked on the \
                 dispatch cadence)"
            );
            self.gated_terminals.push_back(msg);
            return;
        }
        self.process_admitted_terminal(msg, command_rx).await;
    }

    /// THE defer/admit decision. Admit iff the frame makes no causal
    /// claim (unstamped / legacy), its origin is not a live member
    /// (lost-with-origin — never awaitable), or the origin's replicated
    /// terminal watermark covers the stamp.
    fn terminal_gate_admits(&self, msg: &DistributedMessage<I>) -> bool {
        let Some(stamp) = msg.msgs_posted_through() else {
            return true;
        };
        let Some(origin) = msg.delivery_reporter() else {
            return true;
        };
        if self.cluster_state.peer_membership(origin) != PeerMembership::AliveMember {
            return true;
        }
        self.cluster_state
            .custom_terminal_watermark(origin)
            .unwrap_or(0)
            >= stamp
    }

    /// Release pass: process every parked terminal whose gate now
    /// admits, preserving arrival order (per-origin stamps are
    /// monotonic, so a blocked earlier terminal implies its same-origin
    /// successors are blocked too — the order-preserving scan can never
    /// invert same-origin terminals). Cheap no-op while nothing is
    /// parked (the steady-state hot path). Runs at the tail of every
    /// custom-message dispatch pass — see the module doc's cadence
    /// section.
    pub(super) async fn release_gated_terminals(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        if self.gated_terminals.is_empty() {
            return;
        }
        let mut still_gated = std::collections::VecDeque::new();
        while let Some(msg) = self.gated_terminals.pop_front() {
            if self.terminal_gate_admits(&msg) {
                tracing::debug!(
                    origin = ?msg.delivery_reporter(),
                    task_hash = ?msg.task_hash(),
                    stamp = ?msg.msgs_posted_through(),
                    "deferred task terminal released: its causal custom \
                     messages are resolved (or its origin left the \
                     membership)"
                );
                self.process_admitted_terminal(msg, command_rx).await;
            } else {
                still_gated.push_back(msg);
            }
        }
        self.gated_terminals = still_gated;
    }

    /// Route one ADMITTED terminal to its handler — the single routing
    /// point both the ingest seam and the release pass share, so a
    /// released frame is processed exactly as a never-parked one.
    async fn process_admitted_terminal(
        &mut self,
        msg: DistributedMessage<I>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        match msg.msg_type() {
            MessageType::TaskComplete => self.handle_task_complete(msg, command_rx).await,
            MessageType::TaskFailed => self.handle_task_failed(msg, command_rx).await,
            // Unreachable by construction: only the two terminal arms
            // route here. A new caller with a different shape is a
            // programming error surfaced loudly in debug builds.
            other => debug_assert!(false, "non-terminal frame in the terminal gate: {other:?}"),
        }
    }
}
