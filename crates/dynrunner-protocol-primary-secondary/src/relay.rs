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
//! 2. **State-transition observation** ([`observe_transition`]) —
//!    the only logging on the relay path: direct→relay, relay→relay,
//!    relay→direct. Successful sends in steady state are silent.
//! 3. **Pure routing decisions** ([`route_send`], [`forward_step`],
//!    [`handle_backoff`]) — each takes the full state needed to
//!    decide and returns what to do, never touches I/O. Transports
//!    apply the decisions.
//!
//! The mechanics of *applying* a routing decision (mpsc fan-out,
//! clone-on-send, error mapping) live in each [`PeerTransport`] impl —
//! routing policy is shared, transport plumbing is not.

use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

use crate::messages::DistributedMessage;
use dynrunner_core::Identifier;

/// Per-target observed route — drives the transition log so steady-state
/// sends don't generate noise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteState {
    Direct,
    Relay { via: String },
}

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
    /// are pruned without action.
    pub last_used_at: SystemTime,
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
pub fn route_send<I: Identifier, V>(
    connections: &HashMap<String, V>,
    my_peer_id: &str,
    target: &str,
    relay_id: u64,
    msg: DistributedMessage<I>,
    timestamp: f64,
) -> RouteDecision<I> {
    if connections.contains_key(target) {
        return RouteDecision::Direct(msg);
    }
    // Forwarder candidates exclude target and self. The originator's
    // `path` is `[self]`, so a future picked forwarder F sees self
    // already in path and won't try to bounce back through us.
    let via = match pick_relay(connections, [target, my_peer_id]) {
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
        last_used_at: SystemTime::now(),
    };
    let wrapped = DistributedMessage::Relay {
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
pub fn forward_step<I: Identifier, V>(
    connections: &HashMap<String, V>,
    my_peer_id: &str,
    target: &str,
    relay_id: u64,
    path: &[String],
    timestamp: f64,
    sender_id: &str,
    inner: Box<DistributedMessage<I>>,
) -> RouteDecision<I> {
    if connections.contains_key(target) {
        return RouteDecision::Direct(*inner);
    }
    let mut excluded: HashSet<&str> = path.iter().map(|s| s.as_str()).collect();
    excluded.insert(target);
    excluded.insert(my_peer_id);
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
        last_used_at: SystemTime::now(),
    };
    let wrapped = DistributedMessage::Relay {
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
pub fn handle_backoff<I: Identifier, V>(
    state: &mut OutgoingRelay<I>,
    connections: &HashMap<String, V>,
    my_peer_id: &str,
    relay_id: u64,
    failed_via: &str,
    backoff_timestamp: f64,
) -> BackoffDecision<I> {
    state.tried.insert(failed_via.to_string());
    let mut excluded: HashSet<&str> = state
        .path_at_send
        .iter()
        .map(|s| s.as_str())
        .collect();
    excluded.insert(state.target.as_str());
    excluded.insert(my_peer_id);
    for t in &state.tried {
        excluded.insert(t.as_str());
    }
    if let Some(via) = pick_relay(connections, excluded.iter().copied()) {
        state.tried.insert(via.clone());
        state.last_used_at = SystemTime::now();
        let wrapped = DistributedMessage::Relay {
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

/// Update the per-target route state and emit a transition log only on
/// state change. `new_via` is the actual peer the transport is about
/// to send to; `target` is the logical destination. Equal means
/// direct; differ means relay.
///
/// Returns the post-transition state for callers that want to assert
/// in tests (the live caller is fine to discard it).
pub fn observe_transition(
    state: &mut HashMap<String, RouteState>,
    target: &str,
    new_via: &str,
) -> RouteState {
    let new_state = if new_via == target {
        RouteState::Direct
    } else {
        RouteState::Relay {
            via: new_via.to_string(),
        }
    };
    let prev = state.get(target).cloned();
    match (&prev, &new_state) {
        (None, _) => {}
        (Some(a), b) if a == b => {}
        (Some(RouteState::Direct), RouteState::Relay { via }) => {
            tracing::warn!(
                target = %target,
                relay = %via,
                "peer relay engaged: direct link unreachable, forwarding via peer"
            );
        }
        (Some(RouteState::Relay { via: old }), RouteState::Relay { via: new })
            if old != new =>
        {
            tracing::info!(
                target = %target,
                from = %old,
                to = %new,
                "peer relay forwarder changed"
            );
        }
        (Some(RouteState::Relay { via: old }), RouteState::Direct) => {
            tracing::info!(
                target = %target,
                from = %old,
                "peer direct link restored"
            );
        }
        _ => {}
    }
    state.insert(target.to_string(), new_state.clone());
    new_state
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conns(ids: &[&str]) -> HashMap<String, ()> {
        ids.iter().map(|s| (s.to_string(), ())).collect()
    }

    fn keepalive(sender: &str) -> DistributedMessage<()> {
        DistributedMessage::Keepalive {
            sender_id: sender.into(),
            timestamp: 1.0,
            secondary_id: sender.into(),
            active_workers: 0,
        }
    }

    // ── pick_relay ──

    #[test]
    fn pick_relay_returns_lowest_excluding_target() {
        let c = conns(&["c", "a", "b"]);
        assert_eq!(pick_relay(&c, ["b"]).as_deref(), Some("a"));
        assert_eq!(pick_relay(&c, ["a"]).as_deref(), Some("b"));
    }

    #[test]
    fn pick_relay_excludes_multiple() {
        let c = conns(&["a", "b", "c", "d"]);
        assert_eq!(pick_relay(&c, ["b", "a"]).as_deref(), Some("c"));
    }

    #[test]
    fn pick_relay_no_other_peers() {
        let c = conns(&["b"]);
        assert_eq!(pick_relay(&c, ["b"]), None);
    }

    #[test]
    fn pick_relay_empty() {
        let c: HashMap<String, ()> = HashMap::new();
        assert_eq!(pick_relay(&c, ["x"]), None);
    }

    // ── route_send ──

    #[test]
    fn route_send_direct_when_target_reachable() {
        let c = conns(&["a", "b", "c"]);
        let d = route_send(&c, "a", "b", 1, keepalive("a"), 1.0);
        assert!(matches!(d, RouteDecision::Direct(_)));
    }

    #[test]
    fn route_send_relays_via_lowest_when_target_unreachable() {
        let c = conns(&["c", "d"]);
        let d = route_send(&c, "a", "b", 7, keepalive("a"), 1.0);
        match d {
            RouteDecision::Relay {
                via,
                wrapped,
                bookkeeping,
            } => {
                assert_eq!(via, "c");
                if let DistributedMessage::Relay {
                    target_id,
                    relay_id,
                    path,
                    inner,
                    ..
                } = wrapped
                {
                    assert_eq!(target_id, "b");
                    assert_eq!(relay_id, 7);
                    assert_eq!(path, vec!["a".to_string()]);
                    assert!(matches!(*inner, DistributedMessage::Keepalive { .. }));
                } else {
                    panic!("not a relay");
                }
                assert_eq!(bookkeeping.target, "b");
                assert!(bookkeeping.predecessor.is_none());
                assert_eq!(bookkeeping.path_at_send, vec!["a".to_string()]);
                assert!(bookkeeping.tried.contains("c"));
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn route_send_no_route_when_alone_and_target_missing() {
        let c: HashMap<String, ()> = HashMap::new();
        let d = route_send(&c, "a", "b", 0, keepalive("a"), 1.0);
        assert!(matches!(d, RouteDecision::NoRoute));
    }

    // ── forward_step ──

    #[test]
    fn forward_step_unwraps_when_target_directly_reachable() {
        let c = conns(&["a", "b"]);
        let path = vec!["a".to_string()];
        let inner = Box::new(keepalive("a"));
        let d = forward_step::<(), _>(&c, "c", "b", 1, &path, 1.0, "a", inner);
        match d {
            RouteDecision::Direct(m) => {
                assert!(matches!(m, DistributedMessage::Keepalive { .. }));
            }
            other => panic!("expected Direct: {:?}", other),
        }
    }

    #[test]
    fn forward_step_picks_next_lowest_excluding_path() {
        let c = conns(&["a", "b", "d"]);
        let path = vec!["a".to_string()];
        let inner = Box::new(keepalive("a"));
        let d = forward_step::<(), _>(&c, "c", "z", 5, &path, 1.0, "a", inner);
        match d {
            RouteDecision::Relay {
                via,
                wrapped,
                bookkeeping,
            } => {
                assert_eq!(via, "b");
                if let DistributedMessage::Relay {
                    path: new_path,
                    relay_id,
                    ..
                } = wrapped
                {
                    assert_eq!(new_path, vec!["a".to_string(), "c".to_string()]);
                    assert_eq!(relay_id, 5);
                }
                assert_eq!(bookkeeping.predecessor.as_deref(), Some("a"));
                assert_eq!(bookkeeping.path_at_send, vec!["a".to_string(), "c".to_string()]);
            }
            other => panic!("expected Relay: {:?}", other),
        }
    }

    #[test]
    fn forward_step_no_route_when_only_path_peers_remain() {
        let c = conns(&["a", "b"]);
        let path = vec!["a".to_string(), "b".to_string()];
        let inner = Box::new(keepalive("a"));
        let d = forward_step::<(), _>(&c, "c", "z", 1, &path, 1.0, "a", inner);
        assert!(matches!(d, RouteDecision::NoRoute));
    }

    // ── handle_backoff ──

    fn outgoing_originator() -> OutgoingRelay<()> {
        // Originator a sent to b with relay_id 1 via c.
        let mut tried = HashSet::new();
        tried.insert("c".to_string());
        OutgoingRelay {
            target: "b".to_string(),
            predecessor: None,
            path_at_send: vec!["a".to_string()],
            tried,
            inner: Box::new(keepalive("a")),
            original_sender: "a".to_string(),
            original_timestamp: 1.0,
            last_used_at: SystemTime::now(),
        }
    }

    fn outgoing_forwarder() -> OutgoingRelay<()> {
        // Forwarder c received from a, forwarded to d.
        let mut tried = HashSet::new();
        tried.insert("d".to_string());
        OutgoingRelay {
            target: "z".to_string(),
            predecessor: Some("a".to_string()),
            path_at_send: vec!["a".to_string(), "c".to_string()],
            tried,
            inner: Box::new(keepalive("a")),
            original_sender: "a".to_string(),
            original_timestamp: 1.0,
            last_used_at: SystemTime::now(),
        }
    }

    #[test]
    fn handle_backoff_retries_with_next_lowest() {
        // Originator a, candidates {c, d, e}. Already tried c. Backoff
        // says c failed → try d (next lowest excluding tried + path
        // + target + self).
        let mut state = outgoing_originator();
        let connections = conns(&["c", "d", "e"]);
        let decision = handle_backoff(&mut state, &connections, "a", 1, "c", 2.0);
        match decision {
            BackoffDecision::Retry { via, wrapped } => {
                assert_eq!(via, "d");
                if let DistributedMessage::Relay { relay_id, path, .. } = wrapped {
                    assert_eq!(relay_id, 1);
                    assert_eq!(path, vec!["a".to_string()]);
                } else {
                    panic!("not a Relay");
                }
                assert!(state.tried.contains("d"));
            }
            other => panic!("expected Retry: {:?}", other),
        }
    }

    #[test]
    fn handle_backoff_propagates_when_forwarder_exhausted() {
        // Forwarder c with path [a, c], target z, only connection d
        // (which already failed). No remaining candidates; propagate
        // backoff to predecessor a.
        let mut state = outgoing_forwarder();
        let connections = conns(&["a", "d"]);
        let decision = handle_backoff(&mut state, &connections, "c", 9, "d", 3.0);
        match decision {
            BackoffDecision::PropagateBackoff { to, msg } => {
                assert_eq!(to, "a");
                if let DistributedMessage::RelayBackoff {
                    sender_id,
                    relay_id,
                    original_sender,
                    ..
                } = msg
                {
                    assert_eq!(sender_id, "c");
                    assert_eq!(relay_id, 9);
                    assert_eq!(original_sender, "a");
                } else {
                    panic!("not a RelayBackoff");
                }
            }
            other => panic!("expected PropagateBackoff: {:?}", other),
        }
    }

    #[test]
    fn handle_backoff_drops_when_originator_exhausted() {
        // Originator a, only candidate was c which failed; no other
        // connections — drop with no propagation possible.
        let mut state = outgoing_originator();
        let connections = conns(&["c"]); // only c, which is tried
        let decision = handle_backoff(&mut state, &connections, "a", 1, "c", 2.0);
        assert!(matches!(decision, BackoffDecision::Drop));
    }

    #[test]
    fn handle_backoff_skips_path_peers_when_picking_retry() {
        // Forwarder c, path [a, c], target z. Connections {a, b, d}
        // — d failed (tried), a is in path. Only b is eligible.
        let mut state = outgoing_forwarder();
        let connections = conns(&["a", "b", "d"]);
        let decision = handle_backoff(&mut state, &connections, "c", 9, "d", 3.0);
        match decision {
            BackoffDecision::Retry { via, .. } => {
                assert_eq!(via, "b", "must skip a (in path) and d (tried)");
            }
            other => panic!("expected Retry: {:?}", other),
        }
    }

    // ── observe_transition ──

    #[test]
    fn observe_transition_no_log_on_steady_direct() {
        let mut state = HashMap::new();
        let s1 = observe_transition(&mut state, "b", "b");
        let s2 = observe_transition(&mut state, "b", "b");
        assert_eq!(s1, RouteState::Direct);
        assert_eq!(s2, RouteState::Direct);
    }

    #[test]
    fn observe_transition_direct_to_relay() {
        let mut state = HashMap::new();
        observe_transition(&mut state, "b", "b");
        let s = observe_transition(&mut state, "b", "a");
        assert_eq!(s, RouteState::Relay { via: "a".into() });
    }

    #[test]
    fn observe_transition_relay_to_direct() {
        let mut state = HashMap::new();
        observe_transition(&mut state, "b", "a");
        let s = observe_transition(&mut state, "b", "b");
        assert_eq!(s, RouteState::Direct);
    }

    #[test]
    fn observe_transition_relay_path_changed() {
        let mut state = HashMap::new();
        observe_transition(&mut state, "b", "a");
        let s = observe_transition(&mut state, "b", "c");
        assert_eq!(s, RouteState::Relay { via: "c".into() });
    }
}
