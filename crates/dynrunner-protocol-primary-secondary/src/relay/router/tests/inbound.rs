use std::time::{Duration, Instant};

use super::*;
use crate::messages::DistributedMessage;
use crate::relay::testing::new_log;

// ── process_inbound: forwarder path ──

#[test]
fn process_inbound_forwards_relay_via_next_hop() {
    // Forwarder c sees a Relay from a targeted at z. c has direct
    // links to {a, b, d}; pick the lowest non-{path,target,self} = b.
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["a", "b", "d"], &log);
    let mut router = Router::<()>::new("c".into());
    let inbound = DistributedMessage::Relay {
        sender_id: "a".into(),
        timestamp: 1.0,
        target_id: "z".into(),
        relay_id: 7,
        path: vec!["a".into()],
        inner: Box::new(keepalive("a")),
    };
    let outcome = router.process_inbound(inbound, &mut conns, clocks_at(Instant::now(), 1.0));
    assert!(matches!(
        outcome,
        InboundOutcome::Handled {
            redial_target: None
        }
    ));
    let entries = log.borrow();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].addressee, "b");
    match &entries[0].msg {
        DistributedMessage::Relay {
            relay_id,
            target_id,
            path,
            sender_id,
            ..
        } => {
            assert_eq!(*relay_id, 7);
            assert_eq!(target_id, "z");
            assert_eq!(sender_id, "a");
            assert_eq!(path, &vec!["a".to_string(), "c".to_string()]);
        }
        other => panic!("expected forwarded Relay: {other:?}"),
    }
    // Forwarder bookkeeping recorded for backoff.
    assert!(router.outgoing_relays.contains_key(&("a".to_string(), 7)));
}

// ── process_inbound: receiver-side relay observation ──

#[test]
fn process_inbound_relay_for_self_emits_redial() {
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["b"], &log);
    let mut router = Router::<()>::new("a".into());
    // d sends a Relay envelope targeted at a, immediately
    // forwarded by b (path=[d, b], from a's view).
    let inbound = DistributedMessage::Relay {
        sender_id: "d".into(),
        timestamp: 1.0,
        target_id: "a".into(),
        relay_id: 3,
        path: vec!["d".into(), "b".into()],
        inner: Box::new(keepalive("d")),
    };
    let now = Instant::now();
    let outcome = router.process_inbound(inbound, &mut conns, clocks_at(now, 1.0));
    match outcome {
        InboundOutcome::Deliver { msg, redial_target } => {
            assert!(matches!(&*msg, DistributedMessage::Keepalive { .. }));
            assert_eq!(redial_target.as_deref(), Some("d"));
        }
        other => panic!("expected Deliver with redial target d: {other:?}"),
    }
    // Receiver-side observation must have written
    // last_observed_relay_at against the originator.
    assert_eq!(
        router
            .route_state
            .get("d")
            .and_then(|s| s.last_observed_relay_at),
        Some(now)
    );
    // No outbound dispatch — we delivered, didn't forward.
    assert!(log.borrow().is_empty());
}

#[test]
fn process_inbound_relay_for_self_preserves_existing_direct_via() {
    // A1.M1 regression: receiver-side relay observation must NOT
    // overwrite route_state[sender].via if we already observed a
    // Direct route to the sender. Asymmetric partitions are
    // possible in principle (their→us broken, ours→them works);
    // the next outbound send must NOT log a spurious
    // direct→relay warn.
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["b", "d"], &log);
    let mut router = Router::<()>::new("a".into());
    // Establish a Direct route to d via a real send.
    let _ = router
        .send_to_peer(
            "d",
            keepalive("a"),
            &mut conns,
            clocks_at(Instant::now(), 1.0),
        )
        .unwrap();
    match &router.route_state.get("d").expect("route_state for d").via {
        RouteVia::Direct => {}
        other => panic!("expected Direct, got {other:?}"),
    }
    // Now d sends a relay envelope addressed to us via b.
    let inbound = DistributedMessage::Relay {
        sender_id: "d".into(),
        timestamp: 1.0,
        target_id: "a".into(),
        relay_id: 3,
        path: vec!["d".into(), "b".into()],
        inner: Box::new(keepalive("d")),
    };
    let _ = router.process_inbound(inbound, &mut conns, clocks_at(Instant::now(), 2.0));
    // via must remain Direct: their inbound being relayed says
    // nothing about our outbound.
    match &router.route_state.get("d").expect("route_state for d").via {
        RouteVia::Direct => {}
        other => panic!("via should remain Direct after recv-relay-for-self, got {other:?}"),
    }
    assert!(
        router
            .route_state
            .get("d")
            .and_then(|s| s.last_observed_relay_at)
            .is_some()
    );
}

#[test]
fn process_inbound_relay_for_self_redial_suppressed_within_cooldown() {
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["b"], &log);
    let mut router = Router::<()>::new("a".into());
    let t0 = Instant::now();
    let envelope = || DistributedMessage::Relay {
        sender_id: "d".into(),
        timestamp: 1.0,
        target_id: "a".into(),
        relay_id: 3,
        path: vec!["d".into(), "b".into()],
        inner: Box::new(keepalive("d")),
    };
    let _ = router.process_inbound(envelope(), &mut conns, clocks_at(t0, 1.0));
    let outcome = router.process_inbound(
        envelope(),
        &mut conns,
        clocks_at(t0 + Duration::from_secs(5), 2.0),
    );
    match outcome {
        InboundOutcome::Deliver { redial_target, .. } => {
            assert!(redial_target.is_none(), "second observation suppressed");
        }
        other => panic!("expected Deliver: {other:?}"),
    }
}

// ── process_inbound: backoff retry & propagate ──

#[test]
fn process_inbound_backoff_retries_via_next_candidate() {
    // Originator a sent to d via b (relay_id 0). Backoff arrives;
    // a retries via c.
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["b", "c"], &log);
    let mut router = Router::<()>::new("a".into());
    let _ = router
        .send_to_peer(
            "d",
            keepalive("a"),
            &mut conns,
            clocks_at(Instant::now(), 1.0),
        )
        .unwrap();
    log.borrow_mut().clear();
    let backoff = DistributedMessage::RelayBackoff {
        sender_id: "b".into(),
        timestamp: 2.0,
        original_sender: "a".into(),
        relay_id: 0,
    };
    let outcome = router.process_inbound(backoff, &mut conns, clocks_at(Instant::now(), 2.0));
    assert!(matches!(
        outcome,
        InboundOutcome::Handled {
            redial_target: None
        }
    ));
    let entries = log.borrow();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].addressee, "c", "retry went to next candidate");
    assert!(matches!(entries[0].msg, DistributedMessage::Relay { .. }));
    // Failed_via b is now blacklisted for target d.
    assert!(
        router
            .failed_forwarders
            .contains_key(&("d".to_string(), "b".to_string()))
    );
}

#[test]
fn process_inbound_backoff_propagates_when_forwarder_exhausted() {
    // Forwarder c received a relay from a for target z; c picked
    // d. Now d's backoff returns and c has no other candidates
    // (a is in path, c is self, d is tried). c propagates
    // backoff to predecessor a.
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["a", "d"], &log);
    let mut router = Router::<()>::new("c".into());
    let inbound = DistributedMessage::Relay {
        sender_id: "a".into(),
        timestamp: 1.0,
        target_id: "z".into(),
        relay_id: 9,
        path: vec!["a".into()],
        inner: Box::new(keepalive("a")),
    };
    let _ = router.process_inbound(inbound, &mut conns, clocks_at(Instant::now(), 1.0));
    log.borrow_mut().clear();
    let backoff = DistributedMessage::RelayBackoff {
        sender_id: "d".into(),
        timestamp: 2.0,
        original_sender: "a".into(),
        relay_id: 9,
    };
    let outcome = router.process_inbound(backoff, &mut conns, clocks_at(Instant::now(), 2.0));
    assert!(matches!(
        outcome,
        InboundOutcome::Handled {
            redial_target: None
        }
    ));
    let entries = log.borrow();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].addressee, "a",
        "backoff propagated to predecessor"
    );
    match &entries[0].msg {
        DistributedMessage::RelayBackoff {
            sender_id,
            relay_id,
            original_sender,
            ..
        } => {
            assert_eq!(sender_id, "c");
            assert_eq!(*relay_id, 9);
            assert_eq!(original_sender, "a");
        }
        other => panic!("expected RelayBackoff: {other:?}"),
    }
    // Local state for the relay we propagated must be removed.
    assert!(!router.outgoing_relays.contains_key(&("a".to_string(), 9)));
}

#[test]
fn process_inbound_backoff_drops_when_originator_exhausted() {
    // Originator a sent to d via b (only candidate). Backoff
    // returns; no other candidates → drop. No further dispatch,
    // local state cleared.
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["b"], &log);
    let mut router = Router::<()>::new("a".into());
    let _ = router
        .send_to_peer(
            "d",
            keepalive("a"),
            &mut conns,
            clocks_at(Instant::now(), 1.0),
        )
        .unwrap();
    log.borrow_mut().clear();
    let backoff = DistributedMessage::RelayBackoff {
        sender_id: "b".into(),
        timestamp: 2.0,
        original_sender: "a".into(),
        relay_id: 0,
    };
    let _ = router.process_inbound(backoff, &mut conns, clocks_at(Instant::now(), 2.0));
    assert!(log.borrow().is_empty(), "originator drop emits nothing");
    assert!(!router.outgoing_relays.contains_key(&("a".to_string(), 0)));
}

// ── process_inbound: non-routing pass-through ──

#[test]
fn process_inbound_non_routing_delivers() {
    let log = new_log::<()>();
    let mut conns: HashMap<String, RecordingChannel<()>> = HashMap::new();
    let mut router = Router::<()>::new("a".into());
    let outcome =
        router.process_inbound(keepalive("b"), &mut conns, clocks_at(Instant::now(), 1.0));
    match outcome {
        InboundOutcome::Deliver { msg, redial_target } => {
            assert!(matches!(&*msg, DistributedMessage::Keepalive { .. }));
            assert!(redial_target.is_none());
        }
        other => panic!("expected Deliver: {other:?}"),
    }
    assert!(log.borrow().is_empty());
}
