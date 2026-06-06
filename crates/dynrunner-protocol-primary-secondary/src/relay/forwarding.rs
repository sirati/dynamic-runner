//! Pure routing primitives — `pick_relay`, `route_send`,
//! `forward_step`, `handle_backoff`.
//!
//! Each function takes the full state it needs to decide and returns a
//! [`RouteDecision`] or [`BackoffDecision`] without touching I/O. The
//! `Router` dispatcher (in `relay/router/`) applies the decisions
//! against its connection map.
//!
//! See the [`super`] module docs for the relay-with-backtracking
//! protocol design (deterministic forwarder choice, loop prevention
//! via `path`, originator-only drop semantics).

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use dynrunner_core::Identifier;

use crate::messages::DistributedMessage;
use crate::relay::decisions::{BackoffDecision, OutgoingRelay, RouteDecision};

/// Pick the forwarder: lowest-id peer in `connections` not in
/// `exclude`. Deterministic so concurrent senders don't oscillate
/// between forwarders. Pass at minimum the target id; forwarders also
/// pass every peer already in the relay's `path` plus their own id
/// plus everything in `tried`.
pub fn pick_relay<'a, V>(
    connections: &HashMap<String, V>,
    exclude: impl IntoIterator<Item = &'a str>,
) -> Option<String> {
    let excluded: HashSet<&str> = exclude.into_iter().collect();
    connections
        .keys()
        .filter(|k| !excluded.contains(k.as_str()))
        .min()
        .cloned()
}

/// Build the routing decision for a fresh `send_to_peer(target,
/// msg)` call by the originator.
///
/// `relay_id` must be a fresh value from the originator's monotonic
/// counter; it's embedded in the envelope so a future
/// `RelayBackoff` can be correlated. The cluster-wide identity of
/// the relay is `(my_peer_id, relay_id)` — no two outgoing relays
/// from the same originator share an id.
///
/// `blacklist` is the set of peers we know are unreliable forwarders
/// **for this specific target** (recently bounced a relay back).
/// The transport maintains it across messages so subsequent relays
/// don't keep paying the dead-end cost; entries expire after a
/// transport-controlled TTL so a direct link re-established in the
/// meantime doesn't stay shadowed forever. Pass an empty set when
/// no cross-relay history applies.
pub fn route_send<I: Identifier, V>(
    connections: &HashMap<String, V>,
    my_peer_id: &str,
    target: &str,
    relay_id: u64,
    msg: DistributedMessage<I>,
    timestamp: f64,
    blacklist: &HashSet<String>,
) -> RouteDecision<I> {
    if connections.contains_key(target) {
        return RouteDecision::Direct(msg);
    }
    // Forwarder candidates exclude target, self, and any peer the
    // transport has marked as a known-bad forwarder for this target
    // within the TTL window. The originator's `path` is `[self]`, so
    // a future picked forwarder F sees self already in path and
    // won't try to bounce back through us.
    let mut excluded: HashSet<&str> = HashSet::new();
    excluded.insert(target);
    excluded.insert(my_peer_id);
    for b in blacklist {
        excluded.insert(b.as_str());
    }
    let via = match pick_relay(connections, excluded.iter().copied()) {
        Some(v) => v,
        None => return RouteDecision::NoRoute,
    };
    let inner = Box::new(msg);
    let path_at_send = vec![my_peer_id.to_string()];
    let mut tried = HashSet::new();
    tried.insert(via.clone());
    let bookkeeping = OutgoingRelay {
        target: target.to_string(),
        predecessor: None,
        path_at_send: path_at_send.clone(),
        tried,
        inner: inner.clone(),
        original_sender: my_peer_id.to_string(),
        original_timestamp: timestamp,
        last_used_at: Instant::now(),
    };
    let wrapped = DistributedMessage::Relay {
        target: None,
        sender_id: my_peer_id.to_string(),
        timestamp,
        target_id: target.to_string(),
        relay_id,
        path: path_at_send,
        inner,
    };
    RouteDecision::Relay {
        via,
        wrapped,
        bookkeeping,
    }
}

/// Compute the next forwarding step for an inbound `Relay` we've
/// determined isn't for us. Returns the decision plus the forwarder
/// state to record for future backoff handling.
///
/// The returned [`RouteDecision`] is one of:
/// - [`RouteDecision::Direct`] — we have a direct path to the
///   target; deliver the unwrapped inner straight to it (no further
///   relay envelope).
/// - [`RouteDecision::Relay`] — pick a non-`path` forwarder; the
///   bookkeeping records `predecessor = path.last()` so a backoff
///   we receive later knows where to propagate.
/// - [`RouteDecision::NoRoute`] — every candidate is in `path` (or
///   we have no connections); the caller sends a `RelayBackoff` to
///   `path.last()` and records nothing.
///
/// `blacklist` skips peers the transport knows have recently bounced
/// a relay for this target. Same semantics as in [`route_send`].
///
/// The argument list reflects the `DistributedMessage::Relay`
/// envelope fields plus routing context; bundling into a struct
/// would just shift the destructure-rebundle one step out.
#[allow(clippy::too_many_arguments)]
pub fn forward_step<I: Identifier, V>(
    connections: &HashMap<String, V>,
    my_peer_id: &str,
    target: &str,
    relay_id: u64,
    path: &[String],
    timestamp: f64,
    sender_id: &str,
    inner: Box<DistributedMessage<I>>,
    blacklist: &HashSet<String>,
) -> RouteDecision<I> {
    if connections.contains_key(target) {
        return RouteDecision::Direct(*inner);
    }
    let mut excluded: HashSet<&str> = path.iter().map(|s| s.as_str()).collect();
    excluded.insert(target);
    excluded.insert(my_peer_id);
    for b in blacklist {
        excluded.insert(b.as_str());
    }
    let candidate = match pick_relay(connections, excluded.iter().copied()) {
        Some(c) => c,
        None => return RouteDecision::NoRoute,
    };
    let mut new_path = path.to_vec();
    new_path.push(my_peer_id.to_string());
    let predecessor = path.last().cloned();
    let mut tried = HashSet::new();
    tried.insert(candidate.clone());
    let bookkeeping = OutgoingRelay {
        target: target.to_string(),
        predecessor,
        path_at_send: new_path.clone(),
        tried,
        inner: inner.clone(),
        original_sender: sender_id.to_string(),
        original_timestamp: timestamp,
        last_used_at: Instant::now(),
    };
    let wrapped = DistributedMessage::Relay {
        target: None,
        sender_id: sender_id.to_string(),
        timestamp,
        target_id: target.to_string(),
        relay_id,
        path: new_path,
        inner,
    };
    RouteDecision::Relay {
        via: candidate,
        wrapped,
        bookkeeping,
    }
}

/// Decide what to do when a `RelayBackoff` arrives for one of our
/// outbound relays. `state` is mutated in place: `failed_via` is
/// added to `tried`, and on a successful retry `last_used_at` is
/// refreshed.
///
/// `relay_id` is the outgoing relay's id (caller looked it up via
/// `(state.original_sender, relay_id)`); it's passed back into the
/// new wrapped message and the propagated backoff so the upstream
/// peer can correlate.
///
/// `blacklist` excludes peers the transport knows have recently
/// bounced a relay for this same target — same semantics as
/// [`route_send`] / [`forward_step`].
pub fn handle_backoff<I: Identifier, V>(
    state: &mut OutgoingRelay<I>,
    connections: &HashMap<String, V>,
    my_peer_id: &str,
    relay_id: u64,
    failed_via: &str,
    backoff_timestamp: f64,
    blacklist: &HashSet<String>,
) -> BackoffDecision<I> {
    state.tried.insert(failed_via.to_string());
    let mut excluded: HashSet<&str> = state.path_at_send.iter().map(|s| s.as_str()).collect();
    excluded.insert(state.target.as_str());
    excluded.insert(my_peer_id);
    for t in &state.tried {
        excluded.insert(t.as_str());
    }
    for b in blacklist {
        excluded.insert(b.as_str());
    }
    if let Some(via) = pick_relay(connections, excluded.iter().copied()) {
        state.tried.insert(via.clone());
        state.last_used_at = Instant::now();
        let wrapped = DistributedMessage::Relay {
            target: None,
            sender_id: state.original_sender.clone(),
            timestamp: state.original_timestamp,
            target_id: state.target.clone(),
            relay_id,
            path: state.path_at_send.clone(),
            inner: state.inner.clone(),
        };
        return BackoffDecision::Retry { via, wrapped };
    }
    match &state.predecessor {
        Some(pred) => {
            let msg = DistributedMessage::RelayBackoff {
                target: None,
                sender_id: my_peer_id.to_string(),
                timestamp: backoff_timestamp,
                original_sender: state.original_sender.clone(),
                relay_id,
            };
            BackoffDecision::PropagateBackoff {
                to: pred.clone(),
                msg,
            }
        }
        None => BackoffDecision::Drop,
    }
}
