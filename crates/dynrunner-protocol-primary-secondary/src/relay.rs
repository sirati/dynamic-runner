//! Routing helpers for peer-to-peer relay-through-peer fallback.
//!
//! When the direct A↔B link is unreachable, A picks a deterministic
//! forwarder C (lowest peer-id from its remaining direct connections,
//! excluding the target itself) and wraps the message in
//! [`DistributedMessage::Relay`] so C can forward to B. C unwraps if
//! it's the target, or attempts one direct forward to `target_id` if
//! not.
//!
//! ## Loop prevention
//!
//! The `Relay` envelope carries a `path: Vec<String>` recording every
//! peer it has visited (original sender at index 0, each forwarder
//! appended in order). Forwarders MUST pick a candidate that is not
//! in `path`, not equal to the target, and not equal to themselves —
//! that exclusion alone makes loops impossible because a candidate
//! never receives a relay it has already touched. If no candidate
//! satisfies the exclusion, the message is dropped with a warn
//! (deferred work: a stateful "ask previous to choose another"
//! backtracking pass).
//!
//! Concerns kept on this side of the boundary:
//! 1. **Deterministic forwarder choice** ([`pick_relay`]) — by lowest
//!    id, the same node makes the same decision every tick so two
//!    senders don't oscillate between forwarders. Callers can pass
//!    `exclude` to skip the target itself plus any sender they want
//!    to keep out of consideration (e.g. for hop2 sanity, though
//!    hop_count alone already prevents loops).
//! 2. **State-transition observation** ([`observe_transition`]) — the
//!    only logging on the relay path: direct→relay, relay→relay,
//!    relay→direct. Successful sends in steady state are silent.
//!
//! The mechanics of *applying* a routing decision (mpsc fan-out,
//! clone-on-send, error mapping) live in each [`PeerTransport`] impl —
//! routing policy is shared, transport plumbing is not.

use std::collections::HashMap;

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
    },
    /// No path — neither direct nor any forwarder available.
    NoRoute,
}

/// Pick the forwarder: lowest-id peer in `connections` not in
/// `exclude`. Deterministic so concurrent senders don't oscillate
/// between forwarders. Pass at minimum the target id; forwarders also
/// pass every peer already in the relay's `path` plus their own id
/// so the chosen candidate is guaranteed not to have seen the
/// message before.
pub fn pick_relay<'a, V>(
    connections: &HashMap<String, V>,
    exclude: impl IntoIterator<Item = &'a str>,
) -> Option<String> {
    let excluded: std::collections::HashSet<&str> = exclude.into_iter().collect();
    connections
        .keys()
        .filter(|k| !excluded.contains(k.as_str()))
        .min()
        .cloned()
}

/// Build the routing decision for a `send_to_peer(target, msg)` call.
/// `connections` is the set of currently-reachable direct peers
/// (anything whose entry the transport will accept a write on).
///
/// `my_peer_id` populates the relay envelope's `sender_id`; the inner
/// message's own `sender_id` is preserved.
pub fn route_send<I: Identifier, V>(
    connections: &HashMap<String, V>,
    my_peer_id: &str,
    target: &str,
    msg: DistributedMessage<I>,
    timestamp: f64,
) -> RouteDecision<I> {
    if connections.contains_key(target) {
        return RouteDecision::Direct(msg);
    }
    // Exclude target and self from forwarder candidates. Self is
    // normally not in `connections` (a peer doesn't dial itself), but
    // the exclusion keeps the helper correct under any setup where
    // the caller's connection map happens to list its own id (test
    // fixtures, future fan-out variants). The originator's path is
    // [self], so a forwarder later excludes self automatically via
    // the path check too — both gates close the loop.
    match pick_relay(connections, [target, my_peer_id]) {
        Some(via) => {
            let wrapped = DistributedMessage::Relay {
                sender_id: my_peer_id.to_string(),
                timestamp,
                target_id: target.to_string(),
                path: vec![my_peer_id.to_string()],
                inner: Box::new(msg),
            };
            RouteDecision::Relay { via, wrapped }
        }
        None => RouteDecision::NoRoute,
    }
}

/// Compute the next forwarding step for an inbound `Relay` we've
/// determined isn't for us. Returns the peer to send to plus the
/// updated message (with self appended to `path`), or `None` when
/// there is no candidate that hasn't already seen this relay.
///
/// `connections` is the forwarder's direct-peer set; `my_peer_id` is
/// the forwarder's own id; `path` is the relay's current path; the
/// returned message keeps every other field untouched.
pub fn forward_step<I: Identifier, V>(
    connections: &HashMap<String, V>,
    my_peer_id: &str,
    target: &str,
    path: &[String],
    timestamp: f64,
    sender_id: &str,
    inner: Box<DistributedMessage<I>>,
) -> Option<(String, DistributedMessage<I>)> {
    if connections.contains_key(target) {
        // Direct hop wins — we deliver the inner unwrapped to the
        // target ourselves rather than wrapping it in another Relay.
        return Some((target.to_string(), *inner));
    }
    // Build the exclusion set: everything in path (so we don't loop
    // back through earlier forwarders), plus the target (we'd send
    // direct above if reachable), plus self.
    let mut excluded: std::collections::HashSet<&str> = path
        .iter()
        .map(|s| s.as_str())
        .collect();
    excluded.insert(target);
    excluded.insert(my_peer_id);
    let candidate = connections
        .keys()
        .filter(|k| !excluded.contains(k.as_str()))
        .min()
        .cloned()?;
    let mut new_path = path.to_vec();
    new_path.push(my_peer_id.to_string());
    let forwarded = DistributedMessage::Relay {
        sender_id: sender_id.to_string(),
        timestamp,
        target_id: target.to_string(),
        path: new_path,
        inner,
    };
    Some((candidate, forwarded))
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

    #[test]
    fn pick_relay_returns_lowest_excluding_target() {
        let c = conns(&["c", "a", "b"]);
        assert_eq!(pick_relay(&c, ["b"]).as_deref(), Some("a"));
        assert_eq!(pick_relay(&c, ["a"]).as_deref(), Some("b"));
    }

    #[test]
    fn pick_relay_excludes_multiple() {
        // sender='a' forwarder excludes both target and sender so the
        // path can never bounce back to the origin.
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

    fn keepalive(sender: &str) -> DistributedMessage<()> {
        DistributedMessage::Keepalive {
            sender_id: sender.into(),
            timestamp: 1.0,
            secondary_id: sender.into(),
            active_workers: 0,
        }
    }

    #[test]
    fn route_send_direct_when_target_reachable() {
        let c = conns(&["a", "b", "c"]);
        let d = route_send(&c, "a", "b", keepalive("a"), 1.0);
        assert!(matches!(d, RouteDecision::Direct(_)));
    }

    #[test]
    fn route_send_relays_via_lowest_when_target_unreachable() {
        // Originator "a" has direct connections {c, d}. Target "b"
        // is unreachable directly, so route_send wraps the keepalive
        // and picks the lowest non-{target, self} forwarder = "c".
        let c = conns(&["c", "d"]);
        let d = route_send(&c, "a", "b", keepalive("a"), 1.0);
        match d {
            RouteDecision::Relay { via, wrapped } => {
                assert_eq!(via, "c");
                if let DistributedMessage::Relay {
                    target_id,
                    path,
                    inner,
                    ..
                } = wrapped
                {
                    assert_eq!(target_id, "b");
                    assert_eq!(path, vec!["a".to_string()]);
                    assert!(matches!(*inner, DistributedMessage::Keepalive { .. }));
                } else {
                    panic!("not a relay");
                }
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn route_send_no_route_when_alone_and_target_missing() {
        // Originator "a" with no direct connections at all — neither
        // target nor a forwarder is reachable.
        let c: HashMap<String, ()> = HashMap::new();
        let d = route_send(&c, "a", "b", keepalive("a"), 1.0);
        assert!(matches!(d, RouteDecision::NoRoute));
    }

    #[test]
    fn forward_step_unwraps_when_target_directly_reachable() {
        // Forwarder "c" sees a relay for target "b" and "b" is in
        // c's direct connections — return ("b", inner_unwrapped).
        let c = conns(&["a", "b"]);
        let path = vec!["a".to_string()];
        let inner = Box::new(keepalive("a"));
        let step = forward_step::<(), _>(&c, "c", "b", &path, 1.0, "a", inner)
            .expect("forwarder must deliver direct");
        assert_eq!(step.0, "b");
        assert!(matches!(step.1, DistributedMessage::Keepalive { .. }));
    }

    #[test]
    fn forward_step_picks_next_lowest_excluding_path() {
        // Forwarder "c" doesn't have direct to "z". Connections are
        // {a, b, d}; path is [a]; target "z". Exclude path + target
        // + self → candidates {b, d} (a in path, c is self, z is
        // target). Lowest = "b".
        let c = conns(&["a", "b", "d"]);
        let path = vec!["a".to_string()];
        let inner = Box::new(keepalive("a"));
        let step = forward_step::<(), _>(&c, "c", "z", &path, 1.0, "a", inner)
            .expect("must find next hop");
        assert_eq!(step.0, "b");
        if let DistributedMessage::Relay { path: new_path, .. } = step.1 {
            assert_eq!(new_path, vec!["a".to_string(), "c".to_string()]);
        } else {
            panic!("forward_step must wrap in Relay when not direct");
        }
    }

    #[test]
    fn forward_step_returns_none_when_only_path_peers_remain() {
        // Forwarder "c" only has connections to peers already in the
        // path — no candidate can be picked without re-visiting.
        let c = conns(&["a", "b"]);
        let path = vec!["a".to_string(), "b".to_string()];
        let inner = Box::new(keepalive("a"));
        let step = forward_step::<(), _>(&c, "c", "z", &path, 1.0, "a", inner);
        assert!(step.is_none(), "must drop on dead-end");
    }

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
