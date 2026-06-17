//! Inbound `Relay` / `RelayBackoff` dispatch helpers for `Router<I>`.
//!
//! Apply the forward-step decision (deliver direct, forward via new
//! relay envelope, or back-off to predecessor on dead-end) and handle
//! the inbound `RelayBackoff` look-up + retry/propagate/drop path.
//! These are the only `Router<I>` methods that BOTH mutate state AND
//! perform outbound dispatch — kept here so the public-API and the
//! observe helpers stay free of the destructure-then-rebuild noise.

use std::collections::HashMap;

use dynrunner_core::Identifier;

use crate::messages::DistributedMessage;
use crate::relay::channel::OutboundChannel;
use crate::relay::router::dispatcher::Router;
use crate::relay::router::log_rate::RelayWarnKind;
use crate::relay::router::state::{
    Clocks, MSG_DROPPED_AT_ORIGINATOR, RELAY_LOG_TARGET, blacklist_for,
};
use crate::relay::{BackoffDecision, RouteDecision, handle_backoff};

impl<I: Identifier> Router<I> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_forward_decision<C: OutboundChannel<I>>(
        &mut self,
        decision: RouteDecision<I>,
        sender_id: String,
        relay_id: u64,
        target_id: String,
        path: Vec<String>,
        connections: &mut HashMap<String, C>,
        clocks: Clocks,
    ) {
        match decision {
            RouteDecision::Direct(inner_unwrapped) => {
                let send_res = connections
                    .get(&target_id)
                    .map(|chan| chan.dispatch(inner_unwrapped));
                match send_res {
                    Some(Ok(())) => {}
                    Some(Err(_)) => {
                        connections.remove(&target_id);
                        if let Some(suppressed) = self.warn_gate.admit(
                            RelayWarnKind::DirectForwardClosed,
                            &target_id,
                            clocks.now,
                        ) {
                            tracing::warn!(
                                target: RELAY_LOG_TARGET,
                                target_peer = %target_id,
                                suppressed_repeats = suppressed,
                                "relay forward failed: target connection closed"
                            );
                        }
                    }
                    None => {
                        if let Some(suppressed) = self.warn_gate.admit(
                            RelayWarnKind::DirectForwardMissing,
                            &target_id,
                            clocks.now,
                        ) {
                            tracing::warn!(
                                target: RELAY_LOG_TARGET,
                                target_peer = %target_id,
                                suppressed_repeats = suppressed,
                                "relay forward target unexpectedly missing"
                            );
                        }
                    }
                }
            }
            RouteDecision::Relay {
                via,
                wrapped,
                bookkeeping,
            } => {
                let send_res = connections.get(&via).map(|chan| chan.dispatch(wrapped));
                match send_res {
                    Some(Ok(())) => {
                        self.outgoing_relays
                            .insert((sender_id.clone(), relay_id), bookkeeping);
                    }
                    Some(Err(_)) => {
                        connections.remove(&via);
                        if let Some(suppressed) = self.warn_gate.admit(
                            RelayWarnKind::RelayForwardClosed,
                            &target_id,
                            clocks.now,
                        ) {
                            tracing::warn!(
                                target: RELAY_LOG_TARGET,
                                target_peer = %target_id,
                                next = %via,
                                suppressed_repeats = suppressed,
                                "relay forward failed: forwarder connection closed"
                            );
                        }
                    }
                    None => {
                        if let Some(suppressed) = self.warn_gate.admit(
                            RelayWarnKind::RelayForwardMissing,
                            &target_id,
                            clocks.now,
                        ) {
                            tracing::warn!(
                                target: RELAY_LOG_TARGET,
                                target_peer = %target_id,
                                next = %via,
                                suppressed_repeats = suppressed,
                                "relay forward target unexpectedly missing"
                            );
                        }
                    }
                }
            }
            RouteDecision::NoRoute => {
                if let Some(predecessor) = path.last() {
                    let backoff = DistributedMessage::RelayBackoff {
                        target: None,
                        sender_id: self.self_id.clone(),
                        timestamp: clocks.wire,
                        original_sender: sender_id.clone(),
                        relay_id,
                    };
                    let backoff_failed = match connections.get(predecessor) {
                        Some(chan) => chan.dispatch(backoff).is_err(),
                        None => true,
                    };
                    if backoff_failed {
                        // Discriminate closed-vs-missing for the message, but
                        // throttle each kind per `(kind, target)`.
                        let (kind, msg) = if connections.contains_key(predecessor) {
                            (
                                RelayWarnKind::BackoffPredecessorClosed,
                                "relay backoff send failed: predecessor connection closed",
                            )
                        } else {
                            (
                                RelayWarnKind::BackoffPredecessorMissing,
                                "relay backoff send failed: predecessor not in connections",
                            )
                        };
                        let predecessor = predecessor.clone();
                        if let Some(suppressed) =
                            self.warn_gate.admit(kind, &target_id, clocks.now)
                        {
                            tracing::warn!(
                                target: RELAY_LOG_TARGET,
                                target_peer = %target_id,
                                predecessor = %predecessor,
                                suppressed_repeats = suppressed,
                                "{msg}"
                            );
                        }
                    }
                } else if let Some(suppressed) =
                    self.warn_gate
                        .admit(RelayWarnKind::BackoffEmptyPath, &target_id, clocks.now)
                {
                    tracing::warn!(
                        target: RELAY_LOG_TARGET,
                        target_peer = %target_id,
                        suppressed_repeats = suppressed,
                        "dropping relay: empty path on dead-end (no predecessor to back off to)"
                    );
                }
            }
        }
    }

    /// Handle an inbound `RelayBackoff` for `(original_sender,
    /// relay_id)`: look up our state, record the failed forwarder in
    /// the per-target blacklist for future relays, ask the routing
    /// helper what to do, and apply.
    pub(super) fn handle_inbound_backoff<C: OutboundChannel<I>>(
        &mut self,
        original_sender: String,
        relay_id: u64,
        failed_via: String,
        connections: &mut HashMap<String, C>,
        clocks: Clocks,
    ) {
        let key = (original_sender.clone(), relay_id);
        // Own the target up front and release the `&mut` borrow: the
        // per-target WARN gate (`self.warn_gate`) is a second mutable
        // borrow of `self`, so it cannot coexist with a live `&mut
        // self.outgoing_relays` entry. `handle_backoff` re-fetches its
        // `&mut state` below, after the gate work is done.
        let target = match self.outgoing_relays.get(&key) {
            Some(s) => s.target.clone(),
            None => {
                // Stale or duplicate backoff — our state was already
                // pruned (TTL) or we never had this relay. Silent.
                return;
            }
        };
        // Record per-target so subsequent relays don't keep paying
        // the same dead-end. Keyed by (target, forwarder) so the
        // failure of `failed_via` to deliver to `target`
        // doesn't blacklist it for any other destination.
        self.failed_forwarders
            .insert((target.clone(), failed_via.clone()), clocks.now);
        let blacklist = blacklist_for(&self.failed_forwarders, &target, clocks.now);
        // Routability state transition (the silent-branch rule): if THIS
        // blacklist entry exhausted the last candidate path to the
        // target, the link just flipped no-route — the condition the
        // egress no-route gate and the death-evidence reads key on via
        // `has_route`. Name it once at the flip; on a FLAPPING link the
        // per-target gate keeps it to one WARN per window (naming the
        // suppressed repeats) instead of one per flip. The restore side
        // is narrated by the existing "peer direct link restored" /
        // blacklist-TTL expiry (entries age out silently into a re-try).
        if !crate::relay::route_exists(connections, &self.self_id, &target, &blacklist)
            && let Some(suppressed) =
                self.warn_gate
                    .admit(RelayWarnKind::PeerUnroutable, &target, clocks.now)
        {
            tracing::warn!(
                target: RELAY_LOG_TARGET,
                target_peer = %target,
                exhausted_forwarder = %failed_via,
                suppressed_repeats = suppressed,
                "peer unroutable: no direct link and every connected \
                 forwarder is blacklisted for it — sends will no-route \
                 until a path or the blacklist TTL recovers"
            );
        }
        let state = self
            .outgoing_relays
            .get_mut(&key)
            .expect("relay state present: guarded above, never removed in between");
        let decision = handle_backoff(
            state,
            connections,
            &self.self_id,
            relay_id,
            &failed_via,
            clocks.wire,
            &blacklist,
        );
        match decision {
            BackoffDecision::Retry { via, wrapped } => {
                let send_res = connections.get(&via).map(|chan| chan.dispatch(wrapped));
                match send_res {
                    Some(Ok(())) => {}
                    Some(Err(_)) | None => {
                        connections.remove(&via);
                        if let Some(suppressed) =
                            self.warn_gate
                                .admit(RelayWarnKind::RetrySendFailed, &target, clocks.now)
                        {
                            tracing::warn!(
                                target: RELAY_LOG_TARGET,
                                target_peer = %target,
                                next = %via,
                                suppressed_repeats = suppressed,
                                "relay retry send failed; further retries will fire on next backoff"
                            );
                        }
                    }
                }
            }
            BackoffDecision::PropagateBackoff { to, msg } => {
                let send_res = connections.get(&to).map(|chan| chan.dispatch(msg));
                self.outgoing_relays.remove(&key);
                match send_res {
                    Some(Ok(())) => {}
                    Some(Err(_)) | None => {
                        if let Some(suppressed) = self.warn_gate.admit(
                            RelayWarnKind::BackoffPropagationFailed,
                            &target,
                            clocks.now,
                        ) {
                            tracing::warn!(
                                target: RELAY_LOG_TARGET,
                                target_peer = %target,
                                predecessor = %to,
                                suppressed_repeats = suppressed,
                                "relay backoff propagation failed: predecessor connection closed"
                            );
                        }
                    }
                }
            }
            BackoffDecision::Drop => {
                self.outgoing_relays.remove(&key);
                tracing::warn!(
                    target: RELAY_LOG_TARGET,
                    target_peer = %target,
                    relay_id,
                    original_sender = %original_sender,
                    "{MSG_DROPPED_AT_ORIGINATOR}"
                );
            }
        }
    }
}
