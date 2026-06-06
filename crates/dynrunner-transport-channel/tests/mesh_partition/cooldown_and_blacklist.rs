//! Scenarios 7-9: cooldown gate (scenario 7), receiver-side relay
//! observation (scenario 8), and blacklist persistence across sends
//! (scenario 9). All three drive `Router` directly with synthesized
//! [`Clocks`] values because the transport's `now_clocks()` shim
//! reads `std::time::Instant::now()` unconditionally and
//! `tokio::time::pause` cannot affect it.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::{
    Clocks, DistributedMessage, InboundOutcome, PeerTransport, REDIAL_COOLDOWN, RouteVia, Router,
    SendOutcome,
};
use tokio::sync::mpsc;

use crate::helpers::{
    TestId, build_mesh, keepalive, pump_until, pump_until_received, pump_until_with_deadline,
};

// ── Scenario 7 ──
//
// Cooldown gate: bypasses the transport because Clocks::now is
// std::time::Instant — the channel transport's `now_clocks()` shim
// reads it directly and tokio::time::pause cannot affect it. We
// drive `Router::send_to_peer` with synthesized Clocks values to
// exercise the same code path the transport delegates to.

#[tokio::test]
async fn redial_signal_fires_on_first_send_then_silent_within_cooldown() {
    let mut router = Router::<TestId>::new("a".to_string());
    // Fresh outbox-style map: B is a direct neighbor; D is not.
    // Sender receivers are leaked here on purpose — we are testing
    // the cooldown gate, not delivery. (`route_send`/`forward_step`
    // key off the map's `contains_key`; the dispatch on the same
    // path will return Ok(()) for any non-closed sender.)
    let (b_tx, _b_rx) = mpsc::unbounded_channel::<DistributedMessage<TestId>>();
    let mut conns: HashMap<String, mpsc::UnboundedSender<DistributedMessage<TestId>>> =
        HashMap::new();
    conns.insert("b".to_string(), b_tx);

    let t0 = Instant::now();
    // First send → first observation → cooldown gate trips.
    let out1 = router
        .send_to_peer(
            "d",
            keepalive("a"),
            &mut conns,
            Clocks { now: t0, wire: 1.0 },
        )
        .expect("send ok");
    match out1 {
        SendOutcome::Relayed {
            redial_target,
            forwarder,
        } => {
            assert_eq!(forwarder, "b");
            assert_eq!(
                redial_target.as_deref(),
                Some("d"),
                "first observation trips the gate"
            );
        }
        other => panic!("expected Relayed: {other:?}"),
    }
    // 5s later → still within cooldown → suppressed.
    let out2 = router
        .send_to_peer(
            "d",
            keepalive("a"),
            &mut conns,
            Clocks {
                now: t0 + Duration::from_secs(5),
                wire: 2.0,
            },
        )
        .expect("send ok");
    assert!(
        matches!(
            out2,
            SendOutcome::Relayed {
                redial_target: None,
                ..
            }
        ),
        "within cooldown: redial suppressed: {out2:?}"
    );
    // 35s further (total 40s past t0; last observation was at t0+5s,
    // so we are 35s past the last observation → past 30s cooldown).
    let out3 = router
        .send_to_peer(
            "d",
            keepalive("a"),
            &mut conns,
            Clocks {
                now: t0 + Duration::from_secs(40),
                wire: 3.0,
            },
        )
        .expect("send ok");
    match out3 {
        SendOutcome::Relayed { redial_target, .. } => {
            assert_eq!(
                redial_target.as_deref(),
                Some("d"),
                "past cooldown: gate re-trips"
            );
        }
        other => panic!("expected Relayed: {other:?}"),
    }
    // Sanity: the cooldown constant is exactly what the test math
    // assumes (30s). A change in the constant should fail this
    // assertion BEFORE the timing assertions go subtly wrong.
    assert_eq!(REDIAL_COOLDOWN, Duration::from_secs(30));
}

// ── Scenario 8 ──

/// Receiver-side observation: a `Relay` envelope addressed to us
/// records `last_observed_relay_at` against the original sender and
/// (subject to the cooldown gate) emits a redial signal in
/// `InboundOutcome::Deliver`. We assert via `route_state()` that the
/// timestamp was bumped to the synthesized clock's `now`.
#[tokio::test]
async fn receiver_side_relay_observation_triggers_redial() {
    let mut router = Router::<TestId>::new("a".to_string());
    // A has B as a direct neighbor. D reaches A by relaying
    // through B. The inbound message is a Relay envelope where
    // target_id == "a" (us). The receiver-side branch in
    // `process_inbound` then bumps `last_observed_relay_at` against
    // the original sender (D) and emits a redial.
    let (b_tx, _b_rx) = mpsc::unbounded_channel::<DistributedMessage<TestId>>();
    let mut conns: HashMap<String, mpsc::UnboundedSender<DistributedMessage<TestId>>> =
        HashMap::new();
    conns.insert("b".to_string(), b_tx);

    let now = Instant::now();
    let inbound = DistributedMessage::Relay {
        target: None,
        sender_id: "d".into(),
        timestamp: 1.0,
        target_id: "a".into(),
        relay_id: 7,
        path: vec!["d".into(), "b".into()],
        inner: Box::new(keepalive("d")),
    };
    let outcome = router.process_inbound(inbound, &mut conns, Clocks { now, wire: 1.0 });
    match outcome {
        InboundOutcome::Deliver { msg, redial_target } => {
            assert!(matches!(
                &*msg,
                DistributedMessage::Keepalive { target: None, .. }
            ));
            assert_eq!(
                redial_target.as_deref(),
                Some("d"),
                "first receiver-side observation trips the gate"
            );
        }
        other => panic!("expected Deliver with redial=d: {other:?}"),
    }
    // Route-state assertion: D's entry has last_observed_relay_at
    // bumped, but `via` is NOT overwritten — receiver-side
    // observation says nothing about OUR outbound route to D, so
    // `via` remains the default (Direct) until our next outgoing
    // send overwrites it accurately. This guards against the
    // A1.M1 spurious-direct→relay-warn bug.
    let route_state = router.route_state();
    let d_state = route_state
        .get("d")
        .expect("d must be present in route_state after receiver-side observation");
    assert_eq!(
        d_state.last_observed_relay_at,
        Some(now),
        "last_observed_relay_at bumped to clock's now"
    );
    assert_eq!(
        d_state.via,
        RouteVia::Direct,
        "via is NOT overwritten by receiver-side observation \
         (their inbound being relayed says nothing about our outbound)"
    );
}

// ── Scenario 9 ──

/// Blacklist persistence end-to-end. Today the
/// `route_send_blacklist_skips_known_bad_forwarder` unit test on the
/// pure helper proves the blacklist is consulted *if* it's present,
/// but no integration test proves the blacklist actually persists
/// across `Router::send_to_peer` calls — i.e. that
/// `failed_forwarders` is real owned state on the Router and not a
/// per-call scratch map.
///
/// `{A↔B, A↔C, C↔D}`. First A→D: A picks B (lex-lowest of A's
/// candidates); B's only neighbor is A (in `path`), so `forward_step`
/// returns `NoRoute` and B emits `RelayBackoff` to A; A's
/// `handle_inbound_backoff` records `failed_forwarders[("d", "b")]`
/// then retries via C; C delivers direct.
///
/// Note the missing B↔C link versus scenario 5: with B↔C present,
/// B forwards via C *itself* and A never receives a backoff —
/// `failed_forwarders` would never get populated and the test
/// could not distinguish blacklist-skip from lex-ordering.
///
/// Second A→D: B is still lex-lowest in `connections`, but the
/// blacklist for D excludes it, so `route_send` must pick C
/// directly without another backoff round-trip.
#[tokio::test]
async fn blacklist_persists_across_sends() {
    let mut mesh = build_mesh(&["a", "b", "c", "d"], &[("a", "b"), ("a", "c"), ("c", "d")]);
    // First send: A picks B; B backs off; A retries via C; C delivers.
    mesh.get_mut("a")
        .unwrap()
        .send_to_peer("d", keepalive("a"))
        .await
        .expect("send ok");
    let mut delivered: HashMap<String, Vec<DistributedMessage<TestId>>> = HashMap::new();
    let ok = pump_until_received(&mut mesh, &mut delivered, "d").await;
    assert!(
        ok,
        "d did not receive 1st within deadline; delivered={delivered:?}"
    );
    match &mesh.get("a").unwrap().last_outcome {
        Some(SendOutcome::Relayed { forwarder, .. }) => {
            assert_eq!(forwarder, "b", "first pick is lex-lowest B");
        }
        other => panic!("expected first Relayed via b: {other:?}"),
    }
    // Pump a bit more so the backoff cascade fully quiesces and
    // A's `handle_inbound_backoff` records (d, b) in
    // `failed_forwarders`. `pump_until_received` returns as soon as
    // D has 1 msg; the backoff handling on A may still be a poll
    // away on the same loop iteration.
    let _ = pump_until_with_deadline(
        &mut mesh,
        &mut delivered,
        Instant::now() + Duration::from_millis(100),
        |_| false,
    )
    .await;
    // Second send: B is blacklisted for D so route_send must skip
    // it and pick C even though B is still lex-lowest in
    // `connections`.
    mesh.get_mut("a")
        .unwrap()
        .send_to_peer("d", keepalive("a"))
        .await
        .expect("send ok");
    match &mesh.get("a").unwrap().last_outcome {
        Some(SendOutcome::Relayed { forwarder, .. }) => {
            assert_eq!(
                forwarder, "c",
                "blacklist must skip B even though B is lex-lowest in connections"
            );
        }
        other => panic!("expected second Relayed via c: {other:?}"),
    }
    let ok = pump_until(&mut mesh, &mut delivered, |d| {
        d.get("d").map(|v| v.len() >= 2).unwrap_or(false)
    })
    .await;
    assert!(
        ok,
        "d did not receive 2nd within deadline; delivered={delivered:?}"
    );
}
