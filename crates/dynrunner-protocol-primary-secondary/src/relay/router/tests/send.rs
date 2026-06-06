use std::time::{Duration, Instant};

use super::*;
use crate::messages::DistributedMessage;
use crate::relay::testing::new_log;

// ── send_to_peer ──

#[test]
fn send_to_peer_direct_when_target_reachable() {
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["b", "c"], &log);
    let mut router = Router::<()>::new("a".into());
    let outcome = router
        .send_to_peer(
            "b",
            keepalive("a"),
            &mut conns,
            clocks_at(Instant::now(), 1.0),
        )
        .expect("send ok");
    assert_eq!(outcome, SendOutcome::Direct);
    let entries = log.borrow();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].addressee, "b");
    assert!(matches!(
        entries[0].msg,
        DistributedMessage::Keepalive { target: None, .. }
    ));
    // Direct path must NOT have set last_observed_relay_at.
    assert!(
        router
            .route_state
            .get("b")
            .and_then(|s| s.last_observed_relay_at)
            .is_none()
    );
}

#[test]
fn send_to_peer_relays_via_lowest_and_emits_redial_on_first_observation() {
    let log = new_log::<()>();
    // Target d not in our connections; b is the lowest non-self.
    let mut conns = conns_with_log(&["b", "c"], &log);
    let mut router = Router::<()>::new("a".into());
    let now = Instant::now();
    let outcome = router
        .send_to_peer("d", keepalive("a"), &mut conns, clocks_at(now, 1.0))
        .expect("send ok");
    match outcome {
        SendOutcome::Relayed {
            forwarder,
            redial_target,
        } => {
            assert_eq!(forwarder, "b");
            assert_eq!(redial_target.as_deref(), Some("d"));
        }
        other => panic!("expected Relayed: {other:?}"),
    }
    let entries = log.borrow();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].addressee, "b", "envelope went to forwarder");
    match &entries[0].msg {
        DistributedMessage::Relay {
            target_id,
            sender_id,
            relay_id,
            path,
            ..
        } => {
            assert_eq!(target_id, "d");
            assert_eq!(sender_id, "a");
            assert_eq!(*relay_id, 0);
            assert_eq!(path, &vec!["a".to_string()]);
        }
        other => panic!("expected Relay envelope: {other:?}"),
    }
    assert_eq!(
        router
            .route_state
            .get("d")
            .expect("route_state populated for relay target")
            .last_observed_relay_at,
        Some(now),
        "last_observed_relay_at recorded"
    );
}

#[test]
fn send_to_peer_no_route_when_alone() {
    let log = new_log::<()>();
    let mut conns: HashMap<String, RecordingChannel<()>> = HashMap::new();
    let mut router = Router::<()>::new("a".into());
    let outcome = router
        .send_to_peer(
            "b",
            keepalive("a"),
            &mut conns,
            clocks_at(Instant::now(), 1.0),
        )
        .expect("send ok");
    assert!(matches!(outcome, SendOutcome::NoRoute));
    assert!(log.borrow().is_empty());
    let _ = log;
}

#[test]
fn send_to_peer_dispatch_failure_drops_channel() {
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["b"], &log);
    // Simulate b's pipe being dead.
    conns.get("b").unwrap().disable();
    let mut router = Router::<()>::new("a".into());
    let err = router
        .send_to_peer(
            "b",
            keepalive("a"),
            &mut conns,
            clocks_at(Instant::now(), 1.0),
        )
        .expect_err("dispatch failure");
    assert!(matches!(err, RoutingError::DispatchFailed { .. }));
    assert!(!conns.contains_key("b"), "dead channel evicted from map");
}

// ── redial cooldown gate ──

#[test]
fn relay_redial_signal_suppressed_within_cooldown() {
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["b"], &log);
    let mut router = Router::<()>::new("a".into());
    let t0 = Instant::now();
    // First relay observation trips the gate.
    let out1 = router
        .send_to_peer("d", keepalive("a"), &mut conns, clocks_at(t0, 1.0))
        .unwrap();
    assert!(matches!(out1, SendOutcome::Relayed { redial_target: Some(ref id), .. } if id == "d"));
    // Second observation 5s later → gate suppresses.
    let out2 = router
        .send_to_peer(
            "d",
            keepalive("a"),
            &mut conns,
            clocks_at(t0 + Duration::from_secs(5), 2.0),
        )
        .unwrap();
    assert!(
        matches!(
            out2,
            SendOutcome::Relayed {
                redial_target: None,
                ..
            }
        ),
        "second relay within cooldown emits no redial: {out2:?}"
    );
}

#[test]
fn relay_redial_signal_re_fires_after_cooldown() {
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["b"], &log);
    let mut router = Router::<()>::new("a".into());
    let t0 = Instant::now();
    let _ = router
        .send_to_peer("d", keepalive("a"), &mut conns, clocks_at(t0, 1.0))
        .unwrap();
    // Past cooldown → fresh signal.
    let out = router
        .send_to_peer(
            "d",
            keepalive("a"),
            &mut conns,
            clocks_at(t0 + REDIAL_COOLDOWN + Duration::from_secs(1), 2.0),
        )
        .unwrap();
    assert!(matches!(out, SendOutcome::Relayed { redial_target: Some(ref id), .. } if id == "d"));
}
