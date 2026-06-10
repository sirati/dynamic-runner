//! Primary-side custom-message concern (F5): the `CustomMessage` ingest
//! arm and the handler-DISPATCH decision over the replicated inbox.
//!
//! Single concern: WHO invokes the consumer's `custom_message_handler`,
//! in WHAT order, and with WHAT poison policy. The replicated facts it
//! reads/originates (`custom_messages` + the `CustomMessagePosted` /
//! `CustomMessageHandled` mutations) are owned by
//! `cluster_state/apply_custom.rs`; the at-least-once transport leg is
//! owned by the secondary's retention chokepoint; the ack echo is the
//! generic `ack_delivery_report` in `connect.rs`. This module never
//! touches any of those internals — it composes their APIs.
//!
//! # The dispatch decision (per the F5 design)
//!
//! * DROPPABLE (`important = false`): dispatch the handler directly on
//!   ingest, at-most-once — a raise is WARNed and the message is gone
//!   (lost on failover by design; never CRDT-resident).
//! * IMPORTANT: the ingest originates `CustomMessagePosted` (idempotent
//!   under transport replays — the `(origin, seq)` vacant-insert NoOps a
//!   duplicate), then runs the decision over EVERY `Unhandled` entry in
//!   `(origin, seq)` order: a clean handler return originates
//!   `CustomMessageHandled`; a raise leaves the entry `Unhandled`, WARNs,
//!   and retries on a later dispatch trigger with exponential backoff —
//!   after [`CUSTOM_HANDLER_POISON_CAP`] consecutive raises the message
//!   is latched `Handled` anyway with a structured ERROR (an
//!   always-raising handler must not wedge the inbox; fork F5-c). Strike
//!   counts are node-local — a failover resets them, fail-safe (the new
//!   primary's handler may well succeed).
//! * PER-ORIGIN ORDER: within one origin, a not-yet-handleable entry
//!   (backoff pending, or a fresh raise) BLOCKS its successors — seq
//!   `n+1` is never handled before seq `n` resolves (handled or
//!   poison-capped). Origins are independent.
//!
//! # Dispatch triggers
//!
//! 1. The ingest arm (`handle_custom_message`) — the live path.
//! 2. The promotion replay: `run_pipeline`'s operational arm calls
//!    [`PrimaryCoordinator::dispatch_unhandled_custom_messages`] after
//!    hydrate, so a primary that died between landing and handling has
//!    its `Unhandled` residue re-dispatched on the promoted primary.
//! 3. The heartbeat tick — the persistent-interval retry driver for
//!    backoff-deferred entries (a per-iteration sleep arm would be
//!    reset by load; the interval fires under load by construction).
//!
//! Every trigger passes the live `command_rx` so the handler's
//! in-runtime `PrimaryHandle` commands (the streamed-spawn site's
//! `spawn_tasks`) drain inline through the SAME
//! `drain_callback_queued_commands` chokepoint `on_phase_end` uses —
//! the spawn lands BEFORE `CustomMessageHandled` is originated (the
//! hook-mutations-before-the-fact ordering rule), so a death in between
//! re-handles on the next primary and the spawn dedup absorbs the
//! replay (fail-SAFE).

use std::collections::HashSet;
use std::time::{Duration, Instant};

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use super::PrimaryCoordinator;
use super::command_channel::PrimaryCommand;

/// Consecutive handler raises after which an important message is
/// latched `Handled` anyway, with a structured ERROR (fork F5-c: an
/// always-raising handler must not wedge the per-origin inbox forever).
pub(crate) const CUSTOM_HANDLER_POISON_CAP: u32 = 5;

/// Default backoff base for handler-raise retries: retry `n` (1-based)
/// becomes due `base × 2^(n-1)` after the raise. With the 5-strike cap
/// the full poison schedule spans ~15s of deferral.
pub(crate) const CUSTOM_HANDLER_BACKOFF_BASE: Duration = Duration::from_secs(1);

/// Node-local per-message raise bookkeeping (see the module doc and the
/// `custom_handler_strikes` field on the coordinator).
#[derive(Debug)]
pub(crate) struct CustomHandlerBackoff {
    /// Consecutive raises observed for this `(origin, seq)`.
    pub(super) strikes: u32,
    /// The earliest instant the next retry may run.
    pub(super) next_due: Instant,
}

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Ingest arm for a [`DistributedMessage::CustomMessage`] landing.
    ///
    /// The generic ack echo already ran in `dispatch_message` (every
    /// `delivery_seq`-stamped landing is acked BEFORE the handlers,
    /// including dedup-dropped duplicates — the #352 law), so this arm
    /// owns only the class routing:
    ///   * droppable → direct handler dispatch, done;
    ///   * important → originate `CustomMessagePosted` (a transport
    ///     replay NoOps on the `(origin, seq)` key), then run the
    ///     handler-dispatch decision over the whole `Unhandled` inbox.
    pub(super) async fn handle_custom_message(
        &mut self,
        msg: DistributedMessage<I>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        let DistributedMessage::CustomMessage {
            origin_secondary_id,
            msg_seq,
            topic,
            data,
            important,
            ..
        } = msg
        else {
            return;
        };
        if !important {
            // DROPPABLE: at-most-once direct dispatch. A raise loses the
            // message by contract (WARNed inside the invoke); no CRDT, no
            // retention, no retry.
            let _ = self.invoke_custom_handler(&origin_secondary_id, &topic, &data, false);
            // The handler may have queued in-runtime PrimaryHandle
            // commands (spawn_tasks et al.) — drain them inline exactly
            // like the phase cascade does after `on_phase_end`.
            self.drain_callback_queued_commands(command_rx).await;
            return;
        }
        tracing::debug!(
            origin = %origin_secondary_id,
            msg_seq,
            topic = %topic,
            bytes = data.len(),
            "important custom message landed; posting to the replicated inbox"
        );
        // Originate the replicated `Unhandled` fact FIRST — the landing
        // must be CRDT-resident before any handler runs, so a death
        // between landing and handling leaves the entry `Unhandled` in
        // every replica (the promotion replay's input). A duplicate
        // landing NoOps here (vacant-insert) and was already re-acked.
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::CustomMessagePosted {
            origin: origin_secondary_id,
            seq: msg_seq,
            topic,
            data,
        }])
        .await;
        self.dispatch_unhandled_custom_messages(command_rx).await;
    }

    /// THE handler-dispatch decision (F5): walk every `Unhandled` inbox
    /// entry in `(origin, seq)` order and resolve each — clean handler
    /// return → originate `CustomMessageHandled`; raise → strike +
    /// backoff, poison-cap after [`CUSTOM_HANDLER_POISON_CAP`]; within
    /// one origin an unresolved entry blocks its successors (per-origin
    /// send order is the consumer contract). Idempotent + cheap when the
    /// inbox has no `Unhandled` entries (the steady-state hot path).
    ///
    /// Called from all three dispatch triggers (ingest, promotion
    /// replay, heartbeat retry tick) — see the module doc.
    pub(crate) async fn dispatch_unhandled_custom_messages(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        let unhandled = self.cluster_state.unhandled_custom_messages();
        if unhandled.is_empty() {
            return;
        }
        let now = Instant::now();
        // Origins whose in-order head could not be resolved this pass:
        // their later seqs are skipped to preserve per-origin order.
        let mut blocked_origins: HashSet<String> = HashSet::new();
        for (origin, seq, topic, data) in unhandled {
            if blocked_origins.contains(&origin) {
                continue;
            }
            let key = (origin.clone(), seq);
            if let Some(backoff) = self.custom_handler_strikes.get(&key)
                && backoff.next_due > now
            {
                // Raised before and the backoff window is still open:
                // not due yet — and nothing AFTER it in this origin may
                // overtake it.
                blocked_origins.insert(origin);
                continue;
            }
            match self.invoke_custom_handler(&origin, &topic, &data, true) {
                Ok(()) => {
                    self.custom_handler_strikes.remove(&key);
                    // Drain the handler's in-runtime commands BEFORE
                    // originating the Handled latch: the hook's
                    // injection mutations must precede the fact on the
                    // wire (the `PhaseEnded` ordering rule) so a death
                    // in between re-handles on the next primary and the
                    // deterministic re-spawn is absorbed by the spawn
                    // dedup — the fail-SAFE side.
                    self.drain_callback_queued_commands(command_rx).await;
                    self.apply_and_broadcast_cluster_mutations(vec![
                        ClusterMutation::CustomMessageHandled {
                            origin: origin.clone(),
                            seq,
                        },
                    ])
                    .await;
                }
                Err(reason) => {
                    // The handler may have queued commands before
                    // raising — never strand them.
                    self.drain_callback_queued_commands(command_rx).await;
                    let entry = self
                        .custom_handler_strikes
                        .entry(key.clone())
                        .or_insert(CustomHandlerBackoff {
                            strikes: 0,
                            next_due: now,
                        });
                    entry.strikes += 1;
                    let strikes = entry.strikes;
                    if strikes >= CUSTOM_HANDLER_POISON_CAP {
                        // POISON CAP (F5-c): latch it Handled anyway —
                        // an always-raising handler must not wedge the
                        // origin's inbox — and surface the loss as a
                        // structured ERROR the operator can act on.
                        self.custom_handler_strikes.remove(&key);
                        tracing::error!(
                            origin = %origin,
                            msg_seq = seq,
                            topic = %topic,
                            strikes,
                            error = %reason,
                            "custom_message_handler raised {strikes} consecutive \
                             times for this message; poison cap reached — marking \
                             it Handled UNCONSUMED (the payload is dropped)"
                        );
                        self.apply_and_broadcast_cluster_mutations(vec![
                            ClusterMutation::CustomMessageHandled {
                                origin: origin.clone(),
                                seq,
                            },
                        ])
                        .await;
                        // The poison latch resolves this seq, so the
                        // origin's successors may proceed this pass.
                    } else {
                        // Exponential backoff: retry n becomes due
                        // base × 2^(n-1) after this raise.
                        let delay = self
                            .custom_handler_backoff_base
                            .saturating_mul(1u32 << (strikes - 1).min(16));
                        entry.next_due = now + delay;
                        tracing::warn!(
                            origin = %origin,
                            msg_seq = seq,
                            topic = %topic,
                            strikes,
                            retry_in_secs = delay.as_secs_f64(),
                            error = %reason,
                            "custom_message_handler raised; message stays \
                             Unhandled and will be retried with backoff"
                        );
                        blocked_origins.insert(origin);
                    }
                }
            }
        }
    }

    /// Invoke the consumer hook for ONE message. `None` hook = the
    /// consumer exposes no `custom_message_handler`: the message is
    /// consumed unhandled with a WARN (returning `Ok` so an important
    /// message latches `Handled` — a hook-less consumer must not grow
    /// the replicated inbox unboundedly, and every peer runs the same
    /// `TaskDefinition`, so no failover asymmetry exists).
    fn invoke_custom_handler(
        &mut self,
        origin: &str,
        topic: &str,
        data: &[u8],
        important: bool,
    ) -> Result<(), String> {
        match self.on_custom_message.as_mut() {
            Some(cb) => cb(origin, topic, data, important),
            None => {
                tracing::warn!(
                    origin = %origin,
                    topic = %topic,
                    important,
                    "custom message arrived but the TaskDefinition exposes no \
                     custom_message_handler; consuming it unhandled"
                );
                Ok(())
            }
        }
    }
}
