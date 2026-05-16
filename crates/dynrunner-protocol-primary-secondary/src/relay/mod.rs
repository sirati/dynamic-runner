//! Routing helpers for peer-to-peer relay-through-peer fallback.
//!
//! When the direct A↔B link is unreachable, A picks a deterministic
//! forwarder C (lowest-id peer in its direct connections, excluding
//! the target itself) and wraps the message in
//! [`DistributedMessage::Relay`] so C can forward to B. C unwraps if
//! it's the target, sends the inner message direct if it has a path
//! to the target, or forwards via another non-`path` peer otherwise.
//!
//! ## Backoff with backtracking
//!
//! Forwarders that exhaust their candidates send a
//! [`DistributedMessage::RelayBackoff`] back to their predecessor
//! (the last entry in `path` from their view): "your relay
//! `relay_id` is undeliverable through me; try another peer of
//! yours." The predecessor marks the failed forwarder tried, picks
//! the next-lowest-id reachable peer that's not in `path` and not
//! already tried, and re-sends. If the predecessor's candidates also
//! exhaust, it propagates the backoff one step further back. The
//! originator drops with a final warn when its own candidates run
//! out — that's the only place the relay can be authoritatively
//! given up on.
//!
//! Identification: each outgoing relay has a `relay_id` (originator's
//! monotonic counter) plus the `sender_id` field already present on
//! every wire message. The cluster-wide key is the pair
//! `(original_sender, relay_id)`, so independent originators
//! starting at counter 0 don't collide.
//!
//! Loop prevention: `path` records every peer the message has
//! visited. Forwarders MUST exclude `path ∪ {target, self} ∪ tried`
//! when picking a candidate — by construction the same peer never
//! receives the same relay twice.
//!
//! Concerns kept on this side of the boundary:
//! 1. **Deterministic forwarder choice** ([`pick_relay`]) — by
//!    lowest id, the same node makes the same decision every tick
//!    so two senders don't oscillate between forwarders.
//! 2. **Pure routing decisions** ([`route_send`], [`forward_step`],
//!    [`handle_backoff`]) — each takes the full state needed to
//!    decide and returns what to do, never touches I/O. Transports
//!    apply the decisions.
//!
//! State-transition observation (direct→relay, relay→direct, etc.)
//! and the only relay-path logging now live inside [`Router`]
//! (`relay/router/`). Transports never call into this module to
//! decide — they delegate to [`Router::send_to_peer`] /
//! [`Router::process_inbound`] / [`Router::process_inbound_sync`],
//! which call these helpers internally.
//!
//! Submodule layout:
//!   - [`decisions`] — pure data types (`RouteDecision`,
//!     `OutgoingRelay`, `BackoffDecision`).
//!   - [`forwarding`] — the pure routing primitives
//!     (`pick_relay`, `route_send`, `forward_step`,
//!     `handle_backoff`).
//!   - [`channel`] — the [`OutboundChannel`] trait the dispatcher
//!     uses to ship decisions through the transport.
//!   - [`router`] — the `Router<I>` dispatcher that applies the
//!     decisions, observes route transitions, and gates the redial
//!     signal.
//!   - [`testing`] — `#[cfg(any(test, feature = "test-utils"))]`
//!     helpers used by tests in this crate AND by cross-crate
//!     mesh-partition tests in `dynrunner-transport-channel`.
//!
//! [`Router`]: router::Router

pub mod channel;
pub mod decisions;
pub mod forwarding;
pub mod router;
pub mod testing;

#[cfg(test)]
mod forwarding_tests;

pub use channel::OutboundChannel;
pub use decisions::{BackoffDecision, OutgoingRelay, RouteDecision};
pub use forwarding::{forward_step, handle_backoff, pick_relay, route_send};
pub use router::{
    Clocks, InboundOutcome, PeerRouteState, RouteVia, Router, RoutingError, SendOutcome,
    MSG_DIRECT_RESTORED, MSG_DROPPED_AT_ORIGINATOR, MSG_RELAY_ENGAGED, REDIAL_COOLDOWN,
    RELAY_LOG_TARGET,
};
