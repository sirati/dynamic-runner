//! Unit tests for the pure routing primitives in `forwarding.rs`.
//!
//! Router-level integration tests (sender-side dispatch, inbound
//! relay/backoff handling) live alongside the dispatcher in
//! `relay/router/tests/`. The split mirrors the design: this file
//! pins the decision functions, that file pins the dispatcher applying
//! the decisions.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use super::*;
use crate::messages::{DistributedMessage, KeepaliveRole};

fn conns(ids: &[&str]) -> HashMap<String, ()> {
    ids.iter().map(|s| (s.to_string(), ())).collect()
}

fn keepalive(sender: &str) -> DistributedMessage<()> {
    DistributedMessage::Keepalive {
        target: None,
        sender_id: sender.into(),
        timestamp: 1.0,
        secondary_id: sender.into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

fn empty_blacklist() -> HashSet<String> {
    HashSet::new()
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
    let d = route_send(&c, "a", "b", 1, keepalive("a"), 1.0, &empty_blacklist());
    assert!(matches!(d, RouteDecision::Direct(_)));
}

#[test]
fn route_send_relays_via_lowest_when_target_unreachable() {
    let c = conns(&["c", "d"]);
    let d = route_send(&c, "a", "b", 7, keepalive("a"), 1.0, &empty_blacklist());
    match d {
        RouteDecision::Relay {
            via,
            wrapped,
            bookkeeping,
        } => {
            assert_eq!(via, "c");
            if let DistributedMessage::Relay {
                target: None,
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
                assert!(matches!(*inner, DistributedMessage::Keepalive {
    target: None, .. }));
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
    let d = route_send(&c, "a", "b", 0, keepalive("a"), 1.0, &empty_blacklist());
    assert!(matches!(d, RouteDecision::NoRoute));
}

#[test]
fn route_send_blacklist_skips_known_bad_forwarder() {
    // Connections {b, c, d}. Lowest non-target is b but b is
    // blacklisted for target z; pick must skip to c.
    let c = conns(&["b", "c", "d"]);
    let mut blacklist = HashSet::new();
    blacklist.insert("b".to_string());
    let d = route_send(&c, "a", "z", 0, keepalive("a"), 1.0, &blacklist);
    match d {
        RouteDecision::Relay { via, .. } => {
            assert_eq!(via, "c", "must skip b (blacklisted) and go to next-lowest");
        }
        other => panic!("expected Relay: {:?}", other),
    }
}

// ── forward_step ──

#[test]
fn forward_step_unwraps_when_target_directly_reachable() {
    let c = conns(&["a", "b"]);
    let path = vec!["a".to_string()];
    let inner = Box::new(keepalive("a"));
    let d = forward_step::<(), _>(&c, "c", "b", 1, &path, 1.0, "a", inner, &empty_blacklist());
    match d {
        RouteDecision::Direct(m) => {
            assert!(matches!(m, DistributedMessage::Keepalive {
    target: None, .. }));
        }
        other => panic!("expected Direct: {:?}", other),
    }
}

#[test]
fn forward_step_picks_next_lowest_excluding_path() {
    let c = conns(&["a", "b", "d"]);
    let path = vec!["a".to_string()];
    let inner = Box::new(keepalive("a"));
    let d = forward_step::<(), _>(&c, "c", "z", 5, &path, 1.0, "a", inner, &empty_blacklist());
    match d {
        RouteDecision::Relay {
            via,
            wrapped,
            bookkeeping,
        } => {
            assert_eq!(via, "b");
            if let DistributedMessage::Relay {
                target: None,
                path: new_path,
                relay_id,
                ..
            } = wrapped
            {
                assert_eq!(new_path, vec!["a".to_string(), "c".to_string()]);
                assert_eq!(relay_id, 5);
            }
            assert_eq!(bookkeeping.predecessor.as_deref(), Some("a"));
            assert_eq!(
                bookkeeping.path_at_send,
                vec!["a".to_string(), "c".to_string()]
            );
        }
        other => panic!("expected Relay: {:?}", other),
    }
}

#[test]
fn forward_step_no_route_when_only_path_peers_remain() {
    let c = conns(&["a", "b"]);
    let path = vec!["a".to_string(), "b".to_string()];
    let inner = Box::new(keepalive("a"));
    let d = forward_step::<(), _>(&c, "c", "z", 1, &path, 1.0, "a", inner, &empty_blacklist());
    assert!(matches!(d, RouteDecision::NoRoute));
}

#[test]
fn forward_step_blacklist_skips_known_bad_for_same_target() {
    // Forwarder c with path [a], target z. Connections {a, b, d}.
    // b would be lowest pick, but b is blacklisted for target z;
    // must skip to d. (a is in path; c is self.)
    let c = conns(&["a", "b", "d"]);
    let mut blacklist = HashSet::new();
    blacklist.insert("b".to_string());
    let path = vec!["a".to_string()];
    let inner = Box::new(keepalive("a"));
    let d = forward_step::<(), _>(&c, "c", "z", 5, &path, 1.0, "a", inner, &blacklist);
    match d {
        RouteDecision::Relay { via, .. } => assert_eq!(via, "d"),
        other => panic!("expected Relay via d: {:?}", other),
    }
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
        last_used_at: Instant::now(),
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
        last_used_at: Instant::now(),
    }
}

#[test]
fn handle_backoff_retries_with_next_lowest() {
    // Originator a, candidates {c, d, e}. Already tried c. Backoff
    // says c failed → try d (next lowest excluding tried + path
    // + target + self).
    let mut state = outgoing_originator();
    let connections = conns(&["c", "d", "e"]);
    let decision = handle_backoff(
        &mut state,
        &connections,
        "a",
        1,
        "c",
        2.0,
        &empty_blacklist(),
    );
    match decision {
        BackoffDecision::Retry { via, wrapped } => {
            assert_eq!(via, "d");
            if let DistributedMessage::Relay {
    target: None, relay_id, path, .. } = wrapped {
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
    let decision = handle_backoff(
        &mut state,
        &connections,
        "c",
        9,
        "d",
        3.0,
        &empty_blacklist(),
    );
    match decision {
        BackoffDecision::PropagateBackoff { to, msg } => {
            assert_eq!(to, "a");
            if let DistributedMessage::RelayBackoff {
                target: None,
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
    let decision = handle_backoff(
        &mut state,
        &connections,
        "a",
        1,
        "c",
        2.0,
        &empty_blacklist(),
    );
    assert!(matches!(decision, BackoffDecision::Drop));
}

#[test]
fn handle_backoff_skips_path_peers_when_picking_retry() {
    // Forwarder c, path [a, c], target z. Connections {a, b, d}
    // — d failed (tried), a is in path. Only b is eligible.
    let mut state = outgoing_forwarder();
    let connections = conns(&["a", "b", "d"]);
    let decision = handle_backoff(
        &mut state,
        &connections,
        "c",
        9,
        "d",
        3.0,
        &empty_blacklist(),
    );
    match decision {
        BackoffDecision::Retry { via, .. } => {
            assert_eq!(via, "b", "must skip a (in path) and d (tried)");
        }
        other => panic!("expected Retry: {:?}", other),
    }
}

#[test]
fn handle_backoff_blacklist_skips_known_bad_during_retry() {
    // Forwarder c retries after d's backoff. Connections include
    // {a, b, d, e}. a is in path. d is tried. b is blacklisted
    // for the target. Must skip to e.
    let mut state = outgoing_forwarder();
    let connections = conns(&["a", "b", "d", "e"]);
    let mut blacklist = HashSet::new();
    blacklist.insert("b".to_string());
    let decision = handle_backoff(&mut state, &connections, "c", 9, "d", 3.0, &blacklist);
    match decision {
        BackoffDecision::Retry { via, .. } => {
            assert_eq!(via, "e", "must skip a (path), d (tried), b (blacklisted)");
        }
        other => panic!("expected Retry via e: {:?}", other),
    }
}
