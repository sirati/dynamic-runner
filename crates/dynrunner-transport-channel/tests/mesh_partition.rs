//! Mesh-partition end-to-end scenarios for the relay-routing
//! state machine ([`Router`]).
//!
//! ## Driver
//!
//! Sequential cooperative pump. We never use `tokio::join!` or
//! `select!` over multiple transports' `recv_peer`: that attempts
//! overlapping `&mut self` borrows on transport state. Instead we
//! round-robin call `tokio::time::timeout(short, recv_peer)` on
//! each transport. `recv_peer` is the only path that actually
//! forwards `Relay` envelopes (the sync `try_recv_peer` drops them
//! with a warn — see `Router::process_inbound_sync`). If the
//! timeout fires while `recv_peer` is awaiting an empty inbox the
//! future is cancellation-safe (only `await` point is
//! `mpsc::UnboundedReceiver::recv` and `process_inbound` is
//! synchronous). The pump terminates on either (a) the per-test
//! watch closure succeeding or (b) a 5s wall-clock deadline. The
//! wall-clock deadline is independent of any `tokio::time::pause()`
//! virtual clock — a paused-clock test still aborts on a real bug.
//!
//! The pump captures the delivered messages into a per-peer
//! `Vec<DistributedMessage>` and lets the watch closure inspect
//! those records.
//!
//! ## Cooldown-gate scenarios (#7, #8)
//!
//! [`Clocks::now`] is a `std::time::Instant`. `tokio::time::pause`
//! does **not** affect `std::time::Instant::now()`, so we can't drive
//! the cooldown gate via a paused tokio clock through the transport
//! — the transport's `now_clocks()` shim reads the real monotonic
//! clock unconditionally. To exercise the cooldown gate
//! deterministically these scenarios bypass the transport and drive
//! the [`Router`] directly with synthesized [`Clocks`] values. The
//! same `Router::send_to_peer` / `Router::process_inbound` code path
//! the transport delegates to is exercised; the only thing we skip
//! is the trivial `now_clocks()` wrapper.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::{
    Clocks, DistributedMessage, InboundOutcome, PeerTransport, RouteVia, Router, SendOutcome,
    MSG_DROPPED_AT_ORIGINATOR, REDIAL_COOLDOWN,
};
use dynrunner_transport_channel::{
    peer_mesh_with_adjacency, ChannelPeerTransport,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::Registry;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

/// Convert a `&[&str]` of peer ids into the owned-`String` shape the
/// public mesh constructor wants.
fn ids(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

/// Convert a `&[(&str, &str)]` adjacency list into the owned-`String`
/// pair shape the public mesh constructor wants.
fn links(items: &[(&str, &str)]) -> Vec<(String, String)> {
    items
        .iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}

/// Build a partial mesh keyed by peer-id for clean borrow-by-key
/// access in the pump driver. The construction order is the order
/// the caller passed in `peer_ids`; we re-key into a `HashMap` after
/// the fact since `peer_mesh_with_adjacency` returns a `Vec`.
fn build_mesh(
    peer_ids: &[&str],
    link_pairs: &[(&str, &str)],
) -> HashMap<String, ChannelPeerTransport<TestId>> {
    let id_vec = ids(peer_ids);
    let link_vec = links(link_pairs);
    let transports = peer_mesh_with_adjacency::<TestId>(&id_vec, &link_vec);
    id_vec.into_iter().zip(transports).collect()
}

/// Sample payload — a `Keepalive` is the cheapest non-routing
/// `DistributedMessage` variant and round-trips identically through
/// every relay decision.
fn keepalive(sender: &str) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        sender_id: sender.to_string(),
        timestamp: 1.0,
        secondary_id: sender.to_string(),
        active_workers: 0,
    }
}

/// One captured `tracing` event from the relay log target. The
/// `target` field is redundant given the layer pre-filters on
/// target == `"dynrunner_relay"`, but keeping it keeps the trace
/// shape symmetric with `dynrunner-transport-quic`'s
/// `CapturedEvent` and future-proofs scenarios that want to relax
/// the pre-filter.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct CapturedRecord {
    target: String,
    message: String,
}

/// Field visitor that pulls the formatted message body out of an
/// `Event`. Modeled on the same shape used in
/// `dynrunner-transport-quic/src/peer/tests.rs` — the `tracing`
/// macros encode the message as a field named `message`.
struct MessageVisitor(String);

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.0 = value.to_string();
        }
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

/// `tracing-subscriber` layer that appends every relay-target event
/// it sees to a shared buffer. We pre-filter on the layer side
/// (target == `"dynrunner_relay"`) so unrelated events from other
/// crates' instrumentation don't dilute the trace.
struct RelayCaptureLayer {
    records: Arc<Mutex<Vec<CapturedRecord>>>,
}

impl<S: tracing::Subscriber> Layer<S> for RelayCaptureLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let target = event.metadata().target().to_string();
        if target != "dynrunner_relay" {
            return;
        }
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        if let Ok(mut buf) = self.records.lock() {
            buf.push(CapturedRecord {
                target,
                message: visitor.0,
            });
        }
    }
}

/// Run `body` with a thread-local `tracing` subscriber that captures
/// every `dynrunner_relay`-target event. Returns `(body_output,
/// captured_records)`. Use this in scenarios that need to assert on
/// the relay-path log trace (e.g. the originator-drop log in
/// scenario 4).
///
/// Caveat: `set_default` installs the subscriber thread-locally. The
/// scenarios in this file run under `#[tokio::test]`'s default
/// `current_thread` runtime and do not `tokio::spawn` to other
/// threads, so every event surfaces through this layer. A future
/// scenario that crosses thread boundaries would need
/// `set_global_default` (and serial-execution gating against other
/// tests).
async fn with_relay_log_capture<F, T>(body: F) -> (T, Vec<CapturedRecord>)
where
    F: std::future::Future<Output = T>,
{
    let records: Arc<Mutex<Vec<CapturedRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = RelayCaptureLayer {
        records: records.clone(),
    };
    let subscriber = Registry::default().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);
    let result = body.await;
    let captured = records.lock().unwrap().clone();
    (result, captured)
}

/// Pump every transport's `recv_peer` round-robin (with a tiny
/// per-call timeout) until `done` returns `true` or the deadline
/// expires. Returns `true` iff `done` succeeded before the deadline.
///
/// Captured deliveries are appended to `delivered` keyed by recipient
/// peer-id so the closure can inspect them. The transports map is
/// borrowed exclusively for the duration of each iteration; no two
/// `recv_peer` calls overlap.
async fn pump_until_with_deadline<F>(
    transports: &mut HashMap<String, ChannelPeerTransport<TestId>>,
    delivered: &mut HashMap<String, Vec<DistributedMessage<TestId>>>,
    deadline: Instant,
    mut done: F,
) -> bool
where
    F: FnMut(&HashMap<String, Vec<DistributedMessage<TestId>>>) -> bool,
{
    // Per-transport recv timeout. Tiny so a quiescent inbox doesn't
    // hold up the round-robin; the outer wall-clock deadline still
    // bounds total time. `recv_peer`'s sole `.await` point is on
    // `mpsc::UnboundedReceiver::recv()` which is cancellation-safe,
    // so dropping the future on timeout leaves the receiver intact.
    let recv_slice = Duration::from_millis(5);
    // Iterate by sorted key so the trace is reproducible across
    // platforms — the standard library's HashMap iteration is
    // randomized, and the relay invariants we assert depend on
    // forwarder-id ordering rather than iteration order, so this is
    // a no-op for correctness; it just stabilises the trace order.
    let keys: Vec<String> = {
        let mut ks: Vec<String> = transports.keys().cloned().collect();
        ks.sort();
        ks
    };
    loop {
        let mut progressed = false;
        for k in &keys {
            if let Some(t) = transports.get_mut(k) {
                // Async `recv_peer` is the only path that forwards
                // Relay envelopes (Router::process_inbound, vs the
                // sync `try_recv_peer` which drops them).
                match tokio::time::timeout(recv_slice, t.recv_peer()).await {
                    Ok(Some(msg)) => {
                        delivered.entry(k.clone()).or_default().push(msg);
                        progressed = true;
                    }
                    Ok(None) => {
                        // Inbox closed — peer gone. Skip on
                        // subsequent iterations by leaving the
                        // entry alone; HashMap removal would race
                        // with the borrow.
                    }
                    Err(_) => {
                        // Timeout: empty inbox or only routing
                        // envelopes that resolved without
                        // delivering. Move on.
                    }
                }
            }
        }
        if done(delivered) {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        if !progressed {
            tokio::task::yield_now().await;
        }
    }
}

/// Pump with the standard 5s wall-clock abort deadline.
async fn pump_until<F>(
    transports: &mut HashMap<String, ChannelPeerTransport<TestId>>,
    delivered: &mut HashMap<String, Vec<DistributedMessage<TestId>>>,
    done: F,
) -> bool
where
    F: FnMut(&HashMap<String, Vec<DistributedMessage<TestId>>>) -> bool,
{
    pump_until_with_deadline(
        transports,
        delivered,
        Instant::now() + Duration::from_secs(5),
        done,
    )
    .await
}

/// Convenience: pump until the named recipient has at least one
/// delivered message.
async fn pump_until_received(
    transports: &mut HashMap<String, ChannelPeerTransport<TestId>>,
    delivered: &mut HashMap<String, Vec<DistributedMessage<TestId>>>,
    target: &str,
) -> bool {
    let target = target.to_string();
    pump_until(transports, delivered, |d| {
        d.get(&target).map(|v| !v.is_empty()).unwrap_or(false)
    })
    .await
}

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
        .send_to_peer("d", keepalive("a"), &mut conns, Clocks { now: t0, wire: 1.0 })
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
        sender_id: "d".into(),
        timestamp: 1.0,
        target_id: "a".into(),
        relay_id: 7,
        path: vec!["d".into(), "b".into()],
        inner: Box::new(keepalive("d")),
    };
    let outcome = router.process_inbound(inbound, &mut conns, Clocks { now, wire: 1.0 });
    match outcome {
        InboundOutcome::Deliver {
            msg,
            redial_target,
        } => {
            assert!(matches!(msg, DistributedMessage::Keepalive { .. }));
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
    let mut mesh = build_mesh(
        &["a", "b", "c", "d"],
        &[("a", "b"), ("a", "c"), ("c", "d")],
    );
    // First send: A picks B; B backs off; A retries via C; C delivers.
    mesh.get_mut("a")
        .unwrap()
        .send_to_peer("d", keepalive("a"))
        .await
        .expect("send ok");
    let mut delivered: HashMap<String, Vec<DistributedMessage<TestId>>> = HashMap::new();
    let ok = pump_until_received(&mut mesh, &mut delivered, "d").await;
    assert!(ok, "d did not receive 1st within deadline; delivered={delivered:?}");
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
    assert!(ok, "d did not receive 2nd within deadline; delivered={delivered:?}");
}
