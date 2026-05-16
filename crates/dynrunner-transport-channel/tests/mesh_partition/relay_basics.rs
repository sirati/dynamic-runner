//! Scenarios 1-6: transport-level relay routing through a live
//! pump driver. Scenarios 1, 2 cover initial relay selection
//! (lex-lowest pick + alternate after partition); 3 covers heal;
//! 4 covers exhausted candidate set (dead end + originator drop
//! log); 5 covers multi-hop delivery; 6 covers mid-relay sever.
//!
//! All scenarios drive transports' `recv_peer` via the shared
//! `pump_until*` helpers. Cooldown semantics are not in scope here
//! — see `cooldown_and_blacklist.rs` for the timestamp-driven
//! scenarios that bypass the transport.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerTransport, SendOutcome, MSG_DROPPED_AT_ORIGINATOR,
};
use tokio::sync::mpsc;

#[allow(unused_imports)]
use crate::helpers::{
    build_mesh, keepalive, pump_until, pump_until_received, pump_until_with_deadline,
    with_relay_log_capture, CapturedRecord, TestId,
};

// ── Scenario 1 ──

/// `{A↔B, A↔C, B↔C, B↔D, C↔D}` (no A↔D). Originator A picks the
/// lexicographically lowest non-self forwarder (`B`). Cooldown gate
/// trips on the first observation, so `last_outcome.redial_target`
/// is `Some("D")`.
#[tokio::test]
async fn a_to_d_via_lowest_id_relay() {
    let mut mesh = build_mesh(
        &["a", "b", "c", "d"],
        &[
            ("a", "b"),
            ("a", "c"),
            ("b", "c"),
            ("b", "d"),
            ("c", "d"),
        ],
    );
    mesh.get_mut("a")
        .unwrap()
        .send_to_peer("d", keepalive("a"))
        .await
        .expect("send ok");
    let mut delivered: HashMap<String, Vec<DistributedMessage<TestId>>> = HashMap::new();
    let ok = pump_until_received(&mut mesh, &mut delivered, "d").await;
    assert!(ok, "d did not receive within deadline; delivered={delivered:?}");
    let outcome = mesh.get("a").unwrap().last_outcome.clone();
    match outcome {
        Some(SendOutcome::Relayed {
            forwarder,
            redial_target,
        }) => {
            assert_eq!(forwarder, "b", "lexicographically first eligible forwarder");
            assert_eq!(
                redial_target.as_deref(),
                Some("d"),
                "first observation trips the cooldown gate"
            );
        }
        other => panic!("expected Relayed via b: {other:?}"),
    }
}

// ── Scenario 2 ──

/// Same starting adjacency as #1 but A loses its direct B link.
/// Routing must fall back to the next-lowest eligible forwarder
/// (`C`).
#[tokio::test]
async fn partition_forces_alternate_relay() {
    let mut mesh = build_mesh(
        &["a", "b", "c", "d"],
        &[
            ("a", "b"),
            ("a", "c"),
            ("b", "c"),
            ("b", "d"),
            ("c", "d"),
        ],
    );
    mesh.get_mut("a").unwrap().disconnect_from("b");
    mesh.get_mut("a")
        .unwrap()
        .send_to_peer("d", keepalive("a"))
        .await
        .expect("send ok");
    let mut delivered: HashMap<String, Vec<DistributedMessage<TestId>>> = HashMap::new();
    let ok = pump_until_received(&mut mesh, &mut delivered, "d").await;
    assert!(ok, "d did not receive within deadline; delivered={delivered:?}");
    let outcome = mesh.get("a").unwrap().last_outcome.clone();
    match outcome {
        Some(SendOutcome::Relayed { forwarder, .. }) => {
            assert_eq!(forwarder, "c", "B unreachable → fall back to C");
        }
        other => panic!("expected Relayed via c: {other:?}"),
    }
}

// ── Scenario 3 ──

/// Heal: start without A↔D so first send relays. Then `connect_to`
/// fresh sender pairs as the "healed" direct link — we don't care
/// about delivery on the heal path, only that the next
/// `route_send` decision picks `Direct` because the outbox now
/// contains the target id.
#[tokio::test]
async fn heal_restores_direct_then_silent() {
    let mut mesh = build_mesh(
        &["a", "b", "c", "d"],
        &[("a", "b"), ("a", "c"), ("b", "c"), ("b", "d"), ("c", "d")],
    );
    // First send: A↔D unreachable, must relay.
    mesh.get_mut("a")
        .unwrap()
        .send_to_peer("d", keepalive("a"))
        .await
        .expect("send ok");
    let mut delivered: HashMap<String, Vec<DistributedMessage<TestId>>> = HashMap::new();
    pump_until_received(&mut mesh, &mut delivered, "d").await;
    assert!(
        matches!(
            mesh.get("a").unwrap().last_outcome,
            Some(SendOutcome::Relayed { .. })
        ),
        "first send should relay"
    );
    // Heal: synthesize fresh sender pairs and graft them onto A's
    // outbox-for-D and D's outbox-for-A. The receivers are kept
    // alive by the leading-underscore bindings (the underscore
    // suppresses the unused-var warning, it does not drop). They
    // MUST stay alive: dispatching through a closed receiver would
    // Err and the Router would evict the entry from `connections`,
    // forcing the next send back through relay routing — defeating
    // the test.
    let (a_to_d_tx, _a_to_d_rx) = mpsc::unbounded_channel();
    let (d_to_a_tx, _d_to_a_rx) = mpsc::unbounded_channel();
    mesh.get_mut("a").unwrap().connect_to("d".to_string(), a_to_d_tx);
    mesh.get_mut("d").unwrap().connect_to("a".to_string(), d_to_a_tx);
    // Second send: must take Direct.
    mesh.get_mut("a")
        .unwrap()
        .send_to_peer("d", keepalive("a"))
        .await
        .expect("send ok");
    assert_eq!(
        mesh.get("a").unwrap().last_outcome,
        Some(SendOutcome::Direct),
        "post-heal send must be Direct"
    );
    // Third send: still Direct (steady-state).
    mesh.get_mut("a")
        .unwrap()
        .send_to_peer("d", keepalive("a"))
        .await
        .expect("send ok");
    assert_eq!(
        mesh.get("a").unwrap().last_outcome,
        Some(SendOutcome::Direct),
        "steady-state Direct"
    );
}

// ── Scenario 4 ──

/// `{A↔B, A↔C, B↔C}`; D has no neighbors at all. A picks B; B's
/// outbox lacks D and lacks any non-{A, B} alternative for D after
/// excluding path+self → emits RelayBackoff back to A. A then picks
/// C; same dead-end. A's tried set exhausts and the relay drops.
/// Assertions: D never receives a message; the originator-drop
/// log fires exactly once.
#[tokio::test]
async fn dead_end_propagates_backoff_to_originator() {
    let ((), captured) = with_relay_log_capture(async {
        let mut mesh = build_mesh(
            &["a", "b", "c", "d"],
            &[("a", "b"), ("a", "c"), ("b", "c")],
        );
        // D exists in the mesh by virtue of being in `peer_ids`;
        // with no adjacency entry referencing it, its outgoing map
        // is empty and no other transport has a sender into its
        // inbox. The relay must dead-end.
        mesh.get_mut("a")
            .unwrap()
            .send_to_peer("d", keepalive("a"))
            .await
            .expect("send ok");
        let mut delivered: HashMap<String, Vec<DistributedMessage<TestId>>> =
            HashMap::new();
        // Pump until the backoff cascade quiesces. There's no
        // positive done-condition (we're asserting absence), so we
        // use a short dedicated deadline rather than the 5s abort
        // guard. 250ms is ~50x the per-transport recv slice and
        // easily fits the 4-step cascade (A→B, B→A backoff, A→C,
        // C→A backoff, A drop).
        let _ = pump_until_with_deadline(
            &mut mesh,
            &mut delivered,
            Instant::now() + Duration::from_millis(250),
            |_| false,
        )
        .await;
        assert!(
            delivered.get("d").map(|v| v.is_empty()).unwrap_or(true),
            "d must not receive any message; delivered={delivered:?}"
        );
        // First-attempt outcome was Relayed (via B); the dead-end
        // unfolds via subsequent inbound RelayBackoff handling.
        assert!(
            matches!(
                mesh.get("a").unwrap().last_outcome,
                Some(SendOutcome::Relayed { .. })
            ),
            "first attempt was Relayed before backoff cascade"
        );
    })
    .await;
    // The originator-drop log fires exactly once when A's tried set
    // exhausts. Tied to the same constant the runtime emits so a
    // future rename of the message does not silently invalidate
    // this assertion.
    let drop_events: Vec<&CapturedRecord> = captured
        .iter()
        .filter(|e| e.message.contains(MSG_DROPPED_AT_ORIGINATOR))
        .collect();
    assert_eq!(
        drop_events.len(),
        1,
        "expected exactly one originator-drop log; got {drop_events:#?}; \
         full trace: {captured:#?}"
    );
}

// ── Scenario 5 ──

/// `{A↔B, A↔C, B↔C, C↔D}`. A picks B; B has no D-direct so it picks
/// C (lowest non-{A=path, B=self}); C delivers D directly. End-to-
/// end multi-hop forward succeeds.
#[tokio::test]
async fn backoff_retries_through_alternate_then_succeeds() {
    let mut mesh = build_mesh(
        &["a", "b", "c", "d"],
        &[("a", "b"), ("a", "c"), ("b", "c"), ("c", "d")],
    );
    mesh.get_mut("a")
        .unwrap()
        .send_to_peer("d", keepalive("a"))
        .await
        .expect("send ok");
    let mut delivered: HashMap<String, Vec<DistributedMessage<TestId>>> = HashMap::new();
    let ok = pump_until_received(&mut mesh, &mut delivered, "d").await;
    assert!(ok, "d did not receive within deadline; delivered={delivered:?}");
    // Originator picked B as first forwarder.
    match &mesh.get("a").unwrap().last_outcome {
        Some(SendOutcome::Relayed { forwarder, .. }) => {
            assert_eq!(forwarder, "b");
        }
        other => panic!("expected Relayed via b: {other:?}"),
    }
}

// ── Scenario 6 ──

/// `{A↔B, A↔C, B↔C, B↔D, C↔D}` then mid-relay we sever B↔D so when
/// the relay arrives at B, B has no D-direct any more and forwards
/// via C (the only non-{A, B-self, D-target} candidate left).
#[tokio::test]
async fn forwarder_picks_alternate_when_direct_link_severed_pre_send() {
    let mut mesh = build_mesh(
        &["a", "b", "c", "d"],
        &[
            ("a", "b"),
            ("a", "c"),
            ("b", "c"),
            ("b", "d"),
            ("c", "d"),
        ],
    );
    // Sever B's direct link to D before A's send. (The plan calls
    // this "mid-relay"; with the synchronous send semantics of the
    // channel transport, the only deterministic place to do it is
    // before A's outbound dispatch — A still picks B because A
    // doesn't know about B↔D. B then sees its outbox lacks D and
    // forwards via C.)
    mesh.get_mut("b").unwrap().disconnect_from("d");
    mesh.get_mut("a")
        .unwrap()
        .send_to_peer("d", keepalive("a"))
        .await
        .expect("send ok");
    let mut delivered: HashMap<String, Vec<DistributedMessage<TestId>>> = HashMap::new();
    let ok = pump_until_received(&mut mesh, &mut delivered, "d").await;
    assert!(ok, "d did not receive within deadline; delivered={delivered:?}");
    // A still picked B as first forwarder.
    match &mesh.get("a").unwrap().last_outcome {
        Some(SendOutcome::Relayed { forwarder, .. }) => {
            assert_eq!(forwarder, "b");
        }
        other => panic!("expected Relayed via b: {other:?}"),
    }
}
