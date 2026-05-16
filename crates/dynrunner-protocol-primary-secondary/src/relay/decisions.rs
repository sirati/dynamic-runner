//! Routing decisions and per-relay state shared by the routing
//! helpers and the `Router` dispatcher.
//!
//! Pure data types — no I/O, no transport interaction. Owned by
//! [`forwarding`] (which produces them) and consumed by
//! [`router`](super::router::dispatcher) (which applies them).

use std::collections::HashSet;
use std::time::Instant;

use crate::messages::DistributedMessage;

/// A routing decision for one outbound `send_to_peer(target)` call.
#[derive(Debug)]
pub enum RouteDecision<I> {
    /// Send directly to the target.
    Direct(DistributedMessage<I>),
    /// Wrap in a `Relay` envelope and send to `via` instead.
    Relay {
        via: String,
        wrapped: DistributedMessage<I>,
        /// The state to record in `outgoing_relays` so a future
        /// `RelayBackoff` for this `relay_id` can retry.
        bookkeeping: OutgoingRelay<I>,
    },
    /// No path — neither direct nor any forwarder available.
    NoRoute,
}

/// One outbound relay attempt, kept per `(original_sender, relay_id)`
/// in the transport's routing state until the backoff chain
/// exhausts or a TTL prunes it. Stores everything needed to retry
/// without consulting the application again.
#[derive(Debug, Clone)]
pub struct OutgoingRelay<I> {
    /// Final destination of the relay (not us).
    pub target: String,
    /// The peer we received this relay from, if we're a forwarder.
    /// `None` for the originator.
    pub predecessor: Option<String>,
    /// The path we ourselves embed when sending — `[self]` for the
    /// originator, `path_received_from_predecessor + [self]` for a
    /// forwarder. Never modified after creation; backoff retries
    /// re-use the same path.
    pub path_at_send: Vec<String>,
    /// Forwarders we've already tried for this relay. New picks must
    /// avoid this set.
    pub tried: HashSet<String>,
    /// The application-layer message we're delivering.
    pub inner: Box<DistributedMessage<I>>,
    /// Original sender id from the wire envelope (so retries
    /// preserve the field exactly).
    pub original_sender: String,
    /// Original timestamp from the wire envelope. Used for both
    /// re-send fidelity and TTL-based GC of stale state.
    pub original_timestamp: f64,
    /// When this state was last touched (created or refreshed by a
    /// retry). Drives the TTL sweep — entries older than the TTL
    /// are pruned without action. Monotonic `Instant` so the TTL
    /// arithmetic is unaffected by wall-clock jumps; cross-machine
    /// correlation uses [`OutgoingRelay::original_timestamp`] instead.
    pub last_used_at: Instant,
}

/// What the transport should do on receiving a `RelayBackoff` from a
/// peer in our `tried` set.
#[derive(Debug)]
pub enum BackoffDecision<I> {
    /// Send a fresh `Relay` to `via`, re-using the existing
    /// `bookkeeping` state which the transport must re-store under
    /// the same key.
    Retry {
        via: String,
        wrapped: DistributedMessage<I>,
    },
    /// Send a `RelayBackoff` to `to` (our own predecessor), and drop
    /// our local state for this relay.
    PropagateBackoff {
        to: String,
        msg: DistributedMessage<I>,
    },
    /// We're the originator and have no candidates left — drop with
    /// a warn. Local state is already exhausted; transport drops the
    /// entry.
    Drop,
}
