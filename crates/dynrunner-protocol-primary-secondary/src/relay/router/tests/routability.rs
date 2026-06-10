//! `Router::has_route` / `Router::unroutable_ids` — the deliverability
//! predicate behind the egress no-route gate and the published
//! membership projection (BUG 3.3: one state owner for "can frames
//! reach this peer", shared with what `send_to_peer` actually does).

use std::time::{Duration, Instant};

use super::*;
use crate::relay::router::state::BLACKLIST_TTL;
use crate::relay::testing::new_log;

/// Direct connection ⇒ routable; absent-with-forwarder ⇒ routable
/// (relay candidate); absent-with-NO-other-connection ⇒ unroutable.
#[test]
fn has_route_direct_relay_and_empty() {
    let log = new_log::<()>();
    let router = Router::<()>::new("a".into());
    let now = Instant::now();

    let conns = conns_with_log(&["b"], &log);
    assert!(router.has_route("b", &conns, now), "direct");
    assert!(
        router.has_route("d", &conns, now),
        "no direct, but b is a relay candidate"
    );

    let empty: HashMap<String, crate::relay::testing::RecordingChannel<()>> = HashMap::new();
    assert!(
        !router.has_route("d", &empty, now),
        "no connections at all: nothing routable — exactly the state in \
         which no inbound can arrive either (the coherence invariant)"
    );

    // The only connection being the target itself is direct (routable),
    // AND that same connection is a legitimate forwarder candidate for
    // a DIFFERENT target (`route_send` would relay e-bound frames via
    // d) — `has_route` mirrors the send decision exactly.
    let only_d = conns_with_log(&["d"], &log);
    assert!(router.has_route("d", &only_d, now));
    assert!(router.has_route("e", &only_d, now));
}

/// Blacklist saturation flips `has_route` false and surfaces the target
/// in `unroutable_ids`; the TTL expiry restores both — the post-bounce
/// steady state of a genuinely dead peer, and its recovery window.
#[test]
fn blacklist_saturation_makes_target_unroutable_until_ttl() {
    let log = new_log::<()>();
    let mut router = Router::<()>::new("a".into());
    let now = Instant::now();
    let conns = conns_with_log(&["b", "c"], &log);

    // Partial blacklist: c still forwards toward d.
    router.blacklist_forwarder_for_test("d", "b");
    assert!(router.has_route("d", &conns, now));
    assert!(router.unroutable_ids(&conns, now).is_empty());

    // Saturated: every connected forwarder bounced for d.
    router.blacklist_forwarder_for_test("d", "c");
    assert!(!router.has_route("d", &conns, now));
    assert_eq!(router.unroutable_ids(&conns, now), vec!["d".to_string()]);

    // A direct link to d trumps the blacklist (the blacklist names
    // FORWARDERS for d, never d itself).
    let with_d = conns_with_log(&["b", "c", "d"], &log);
    assert!(router.has_route("d", &with_d, now));
    assert!(router.unroutable_ids(&with_d, now).is_empty());

    // TTL expiry: the bounced forwarders are re-tried, d is routable
    // again — a recovered path never stays shadowed forever.
    let later = now + BLACKLIST_TTL + Duration::from_secs(1);
    assert!(router.has_route("d", &conns, later));
    assert!(router.unroutable_ids(&conns, later).is_empty());
}

/// `has_route` mirrors `send_to_peer`'s real decision: a saturated
/// blacklist makes the send return `NoRoute`, and `has_route` reads
/// `false` for exactly that state — the two can never disagree about
/// one link.
#[test]
fn has_route_agrees_with_send_outcome() {
    let log = new_log::<()>();
    let mut router = Router::<()>::new("a".into());
    let now = Instant::now();
    let mut conns = conns_with_log(&["b"], &log);

    router.blacklist_forwarder_for_test("d", "b");
    assert!(!router.has_route("d", &conns, now));
    let outcome = router
        .send_to_peer("d", keepalive("a"), &mut conns, clocks_at(now, 1.0))
        .expect("send ok");
    assert_eq!(outcome, SendOutcome::NoRoute);
}
