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
                        tracing::warn!(
                            target: RELAY_LOG_TARGET,
                            target_peer = %target_id,
                            "relay forward failed: target connection closed"
                        );
                    }
                    None => {
                        tracing::warn!(
                            target: RELAY_LOG_TARGET,
                            target_peer = %target_id,
                            "relay forward target unexpectedly missing"
                        );
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
                        tracing::warn!(
                            target: RELAY_LOG_TARGET,
                            target_peer = %target_id,
                            next = %via,
                            "relay forward failed: forwarder connection closed"
                        );
                    }
                    None => {
                        tracing::warn!(
                            target: RELAY_LOG_TARGET,
                            target_peer = %target_id,
                            next = %via,
                            "relay forward target unexpectedly missing"
                        );
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
                    if let Some(chan) = connections.get(predecessor) {
                        if chan.dispatch(backoff).is_err() {
                            tracing::warn!(
                                target: RELAY_LOG_TARGET,
                                target_peer = %target_id,
                                predecessor = %predecessor,
                                "relay backoff send failed: predecessor connection closed"
                            );
                        }
                    } else {
                        tracing::warn!(
                            target: RELAY_LOG_TARGET,
                            target_peer = %target_id,
                            predecessor = %predecessor,
                            "relay backoff send failed: predecessor not in connections"
                        );
                    }
                } else {
                    tracing::warn!(
                        target: RELAY_LOG_TARGET,
                        target_peer = %target_id,
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
        let state = match self.outgoing_relays.get_mut(&key) {
            Some(s) => s,
            None => {
                // Stale or duplicate backoff — our state was already
                // pruned (TTL) or we never had this relay. Silent.
                return;
            }
        };
        // Record per-target so subsequent relays don't keep paying
        // the same dead-end. Keyed by (target, forwarder) so the
        // failure of `failed_via` to deliver to `state.target`
        // doesn't blacklist it for any other destination.
        self.failed_forwarders
            .insert((state.target.clone(), failed_via.clone()), clocks.now);
        let blacklist = blacklist_for(&self.failed_forwarders, &state.target, clocks.now);
        // Routability state transition (the silent-branch rule): if THIS
        // blacklist entry exhausted the last candidate path to the
        // target, the link just flipped no-route — the condition the
        // egress no-route gate and the death-evidence reads key on via
        // `has_route`. Name it once at the flip; the restore side is
        // narrated by the existing "peer direct link restored" /
        // blacklist-TTL expiry (entries age out silently into a re-try).
        if !crate::relay::route_exists(connections, &self.self_id, &state.target, &blacklist) {
            tracing::warn!(
                target: RELAY_LOG_TARGET,
                target_peer = %state.target,
                exhausted_forwarder = %failed_via,
                "peer unroutable: no direct link and every connected \
                 forwarder is blacklisted for it — sends will no-route \
                 until a path or the blacklist TTL recovers"
            );
        }
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
                        tracing::warn!(
                            target: RELAY_LOG_TARGET,
                            target_peer = %state.target,
                            next = %via,
                            "relay retry send failed; further retries will fire on next backoff"
                        );
                    }
                }
            }
            BackoffDecision::PropagateBackoff { to, msg } => {
                let target_for_log = state.target.clone();
                let send_res = connections.get(&to).map(|chan| chan.dispatch(msg));
                self.outgoing_relays.remove(&key);
                match send_res {
                    Some(Ok(())) => {}
                    Some(Err(_)) | None => {
                        tracing::warn!(
                            target: RELAY_LOG_TARGET,
                            target_peer = %target_for_log,
                            predecessor = %to,
                            "relay backoff propagation failed: predecessor connection closed"
                        );
                    }
                }
            }
            BackoffDecision::Drop => {
                let target_for_log = state.target.clone();
                self.outgoing_relays.remove(&key);
                tracing::warn!(
                    target: RELAY_LOG_TARGET,
                    target_peer = %target_for_log,
                    relay_id,
                    original_sender = %original_sender,
                    "{MSG_DROPPED_AT_ORIGINATOR}"
                );
            }
        }
    }
}
