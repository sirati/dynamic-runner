use std::time::{Duration, Instant};

use super::*;
use crate::messages::DistributedMessage;
use crate::relay::router::state::{BLACKLIST_TTL, RELAY_STATE_TTL};
use crate::relay::testing::new_log;

// ── prune ──

#[test]
fn prune_evicts_stale_outgoing_relays() {
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["b"], &log);
    let mut router = Router::<()>::new("a".into());
    let t0 = Instant::now();
    let _ = router
        .send_to_peer("d", keepalive("a"), &mut conns, clocks_at(t0, 1.0))
        .unwrap();
    assert!(router.outgoing_relays.contains_key(&("a".to_string(), 0)));
    // Past TTL.
    router.prune(t0 + RELAY_STATE_TTL + Duration::from_secs(1));
    assert!(!router.outgoing_relays.contains_key(&("a".to_string(), 0)));
}

#[test]
fn prune_evicts_stale_blacklist() {
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["b", "c"], &log);
    let mut router = Router::<()>::new("a".into());
    let t0 = Instant::now();
    let _ = router
        .send_to_peer("d", keepalive("a"), &mut conns, clocks_at(t0, 1.0))
        .unwrap();
    // Backoff inserts blacklist entry under (target=d, peer=b).
    let backoff = DistributedMessage::RelayBackoff {
        target: None,
        sender_id: "b".into(),
        timestamp: 2.0,
        original_sender: "a".into(),
        relay_id: 0,
    };
    let _ = router.process_inbound(backoff, &mut conns, clocks_at(t0, 2.0));
    assert!(
        router
            .failed_forwarders
            .contains_key(&("d".to_string(), "b".to_string()))
    );
    // Past blacklist TTL.
    router.prune(t0 + BLACKLIST_TTL + Duration::from_secs(1));
    assert!(
        !router
            .failed_forwarders
            .contains_key(&("d".to_string(), "b".to_string()))
    );
}
