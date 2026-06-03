//! Per-peer route observation methods for `Router<I>`.
//!
//! Sender-side ([`Router::observe_direct`], [`Router::observe_relay`])
//! and receiver-side ([`Router::observe_relay_recv`]) state-transition
//! tracking. Drives the transition log AND the redial-cooldown gate
//! that signals when the transport should kick a background dial. Pure
//! state mutation — no outbound I/O.

use std::time::Instant;

use dynrunner_core::Identifier;

use crate::relay::router::dispatcher::Router;
use crate::relay::router::state::{
    MSG_DIRECT_RESTORED, MSG_RELAY_ENGAGED, PeerRouteState, REDIAL_COOLDOWN, RELAY_LOG_TARGET,
    RouteVia,
};

impl<I: Identifier> Router<I> {
    /// Observe a Direct outcome for `target`. Updates `route_state`
    /// (logging the transition if it changed) but DOES NOT touch
    /// `last_observed_relay_at` — the cooldown gate is only driven
    /// by Relay outcomes.
    pub(super) fn observe_direct(&mut self, target: &str) {
        let prev = self.route_state.get(target).cloned();
        let new_via = RouteVia::Direct;
        match prev.as_ref().map(|s| &s.via) {
            None => {}
            Some(RouteVia::Direct) => {}
            Some(RouteVia::Relay { forwarder }) => {
                tracing::info!(
                    target: RELAY_LOG_TARGET,
                    target_peer = %target,
                    from = %forwarder,
                    "{MSG_DIRECT_RESTORED}"
                );
            }
        }
        // Keep last_observed_relay_at — Direct doesn't touch it.
        let last_observed_relay_at = prev.and_then(|s| s.last_observed_relay_at);
        self.route_state.insert(
            target.to_string(),
            PeerRouteState {
                via: new_via,
                last_observed_relay_at,
            },
        );
    }

    /// Observe a Relay outcome for `target` via forwarder `forwarder`
    /// at time `now`. Updates `route_state`, logs the transition if it
    /// changed, and applies the redial-cooldown gate to determine
    /// whether to emit a `redial_target`. Returns `Some(target)` iff
    /// the cooldown tripped on this observation.
    pub(super) fn observe_relay(
        &mut self,
        target: &str,
        forwarder: &str,
        now: Instant,
    ) -> Option<String> {
        let prev = self.route_state.get(target).cloned();
        let new_via = RouteVia::Relay {
            forwarder: forwarder.to_string(),
        };
        // Transition log fires only on a STATE CHANGE — the first
        // observation of a peer's route is silent (matches the
        // pre-Router behavior of `observe_transition` in
        // relay/mod.rs, where `(None, _) => {}` is the silent arm).
        match prev.as_ref().map(|s| &s.via) {
            None => {}
            Some(RouteVia::Direct) => {
                tracing::warn!(
                    target: RELAY_LOG_TARGET,
                    target_peer = %target,
                    relay = %forwarder,
                    "{MSG_RELAY_ENGAGED}"
                );
            }
            Some(RouteVia::Relay { forwarder: old }) if old != forwarder => {
                tracing::info!(
                    target: RELAY_LOG_TARGET,
                    target_peer = %target,
                    from = %old,
                    to = %forwarder,
                    "peer relay forwarder changed"
                );
            }
            Some(RouteVia::Relay { .. }) => {}
        }
        let prev_observed = prev.as_ref().and_then(|s| s.last_observed_relay_at);
        let trip = match prev_observed {
            None => true,
            Some(t) => now.duration_since(t) >= REDIAL_COOLDOWN,
        };
        let redial_target = if trip { Some(target.to_string()) } else { None };
        self.route_state.insert(
            target.to_string(),
            PeerRouteState {
                via: new_via,
                last_observed_relay_at: Some(now),
            },
        );
        redial_target
    }

    /// Receiver-side relay observation: a `Relay` envelope addressed
    /// to us arrived from `peer`. Bumps `last_observed_relay_at[peer]`
    /// and applies the redial-cooldown gate, but does NOT touch
    /// `route_state[peer].via` — `via` is OUR outbound route, only
    /// knowable from our send-side experience. Their→us being broken
    /// does not imply ours→them is broken (asymmetric partitions are
    /// possible in principle), so this path stays silent on logging
    /// and lets the next outgoing send observe the true outbound route.
    pub(super) fn observe_relay_recv(&mut self, peer: &str, now: Instant) -> Option<String> {
        let prev = self.route_state.get(peer).cloned();
        let prev_observed = prev.as_ref().and_then(|s| s.last_observed_relay_at);
        let trip = match prev_observed {
            None => true,
            Some(t) => now.duration_since(t) >= REDIAL_COOLDOWN,
        };
        let redial_target = if trip { Some(peer.to_string()) } else { None };
        let via = prev
            .as_ref()
            .map(|s| s.via.clone())
            .unwrap_or(RouteVia::Direct);
        self.route_state.insert(
            peer.to_string(),
            PeerRouteState {
                via,
                last_observed_relay_at: Some(now),
            },
        );
        redial_target
    }
}
