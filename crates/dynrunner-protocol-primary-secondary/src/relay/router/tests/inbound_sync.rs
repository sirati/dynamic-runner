use std::time::Instant;

use super::*;
use crate::messages::DistributedMessage;

// ── process_inbound_sync ──

#[test]
fn process_inbound_sync_delivers_relay_for_self_and_emits_redial() {
    // A3.M1: sync path now mirrors the async path for
    // Relay-for-self — receiver-side bookkeeping is pure state
    // mutation (no outbound dispatch), so the sync constraint
    // does not exclude it. A consumer driving recv via try_recv
    // only must NOT silently lose the redial safety net.
    let mut router = Router::<()>::new("a".into());
    let inbound = DistributedMessage::Relay {
        target: None,
        sender_id: "d".into(),
        timestamp: 1.0,
        target_id: "a".into(),
        relay_id: 3,
        path: vec!["d".into(), "b".into()],
        inner: Box::new(keepalive("d")),
    };
    let now = Instant::now();
    let outcome = router.process_inbound_sync(inbound, clocks_at(now, 1.0));
    match outcome {
        InboundOutcome::Deliver { msg, redial_target } => {
            assert!(matches!(
                &*msg,
                DistributedMessage::Keepalive { target: None, .. }
            ));
            assert_eq!(redial_target.as_deref(), Some("d"));
        }
        other => panic!("expected Deliver: {other:?}"),
    }
    assert_eq!(
        router
            .route_state
            .get("d")
            .and_then(|s| s.last_observed_relay_at),
        Some(now)
    );
}

#[test]
fn process_inbound_sync_drops_relay_for_others() {
    let mut router = Router::<()>::new("a".into());
    let inbound = DistributedMessage::Relay {
        target: None,
        sender_id: "d".into(),
        timestamp: 1.0,
        target_id: "z".into(),
        relay_id: 3,
        path: vec!["d".into()],
        inner: Box::new(keepalive("d")),
    };
    let outcome = router.process_inbound_sync(inbound, clocks_at(Instant::now(), 1.0));
    assert!(matches!(
        outcome,
        InboundOutcome::Handled {
            redial_target: None
        }
    ));
}

#[test]
fn process_inbound_sync_drops_backoff() {
    let mut router = Router::<()>::new("a".into());
    let inbound = DistributedMessage::RelayBackoff {
        target: None,
        sender_id: "b".into(),
        timestamp: 1.0,
        original_sender: "a".into(),
        relay_id: 0,
    };
    let outcome = router.process_inbound_sync(inbound, clocks_at(Instant::now(), 1.0));
    assert!(matches!(
        outcome,
        InboundOutcome::Handled {
            redial_target: None
        }
    ));
}
