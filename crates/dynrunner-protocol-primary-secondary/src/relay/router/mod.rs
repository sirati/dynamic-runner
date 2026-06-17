//! Peer-mesh routing dispatcher.
//!
//! Split-file layout:
//!   - [`state`] — types (`Clocks`, `PeerRouteState`, `RouteVia`,
//!     `SendOutcome`, `InboundOutcome`, `RoutingError`), TTL constants,
//!     and pure prune helpers.
//!   - [`dispatcher`] — `Router<I>` struct + public API
//!     (`new`/`prune`/`send_to_peer`/`process_inbound`/
//!     `process_inbound_sync`/`route_state`).
//!   - [`observe`] — private per-peer route observation impls
//!     (`observe_direct`/`observe_relay`/`observe_relay_recv`) that
//!     drive the transition log and redial-cooldown gate.
//!   - [`inbound`] — private inbound-side helpers
//!     (`apply_forward_decision`/`handle_inbound_backoff`) — the only
//!     `Router` methods that BOTH mutate state AND dispatch outbound.
//!   - [`log_rate`] — per-`(kind, target)` WARN rate-limit gate so a
//!     flapping/dead peer never storms the operator stream with one WARN
//!     per flip / per message.
//!
//! See the per-submodule docs for the design rationale of each split.

pub(crate) mod dispatcher;
pub(crate) mod filter;
pub(crate) mod inbound;
pub(crate) mod log_rate;
pub(crate) mod observe;
pub(crate) mod state;

#[cfg(test)]
mod tests;

pub use dispatcher::Router;
pub use filter::{InboundFilter, Verdict};
pub use state::{
    Clocks, InboundOutcome, MSG_DIRECT_RESTORED, MSG_DROPPED_AT_ORIGINATOR, MSG_RELAY_ENGAGED,
    PeerRouteState, REDIAL_COOLDOWN, RELAY_LOG_TARGET, RouteVia, RoutingError, SendOutcome,
};
