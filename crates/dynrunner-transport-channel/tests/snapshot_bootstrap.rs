//! Snapshot-bootstrap end-to-end scenario for [`PeerTransport::join_running_cluster`].
//!
//! Pins Step 8 of the transport-unification refactor: a fresh
//! observer / late-joiner uses `join_running_cluster(seed)` to dial a
//! known peer set, send `RequestClusterSnapshot`, and receive a
//! `ClusterSnapshot` reply carrying the serialized
//! `ClusterStateSnapshot<I>` payload.
//!
//! ## Why this lives in the channel-transport crate
//!
//! The trait-level `join_running_cluster` is a default-impl on
//! [`PeerTransport`] (see `dynrunner-protocol-primary-secondary`); the
//! channel-transport crate is the lowest layer that gets a working
//! impl out-of-the-box (the QUIC impl inherits the same default).
//! Putting the test here keeps the dependency graph clean — no
//! manager-distributed dev-dep, no cycle. The receiver side is
//! simulated by a hand-rolled responder that pumps the channel
//! transport's `recv_peer`, recognises `RequestClusterSnapshot`, and
//! replies with a synthetic `ClusterSnapshot` whose `snapshot_json` is
//! the test's pre-baked JSON. This mirrors the production responder
//! at `crates/dynrunner-manager-distributed/src/secondary/dispatch.rs:402-450`
//! without pulling in the full secondary coordinator (which would
//! drag the run lifecycle, election state machine, and ResourceEstimator
//! into a transport-layer test — wrong layer).
//!
//! ## Architectural assertion
//!
//! The pre-baked snapshot JSON includes both task entries AND a
//! `capabilities` map — the joiner asserts both round-trip. This pins
//! the role-capability 2P-set (C6) wiring through the snapshot frame:
//! without the capability roster carried through the snapshot, a fresh
//! joiner would have empty `role_table.observers` /
//! `role_table.can_be_primary` projections between snapshot-restore and
//! the next live PeerInfo broadcast, and its election filter
//! (`secondary::election::lowest_alive` skips observers) would briefly
//! mis-promote an observer candidate.

use std::collections::HashMap;
use std::time::Duration;

use dynrunner_protocol_primary_secondary::{
    DEFAULT_JOIN_TIMEOUT, Destination, DistributedMessage, JoinError, KeepaliveRole,
    PeerConnectionInfo, PeerId, PeerTransport, timestamp_now,
};
use dynrunner_transport_channel::{ChannelPeerTransport, peer_mesh};
use serde::{Deserialize, Serialize};

/// In-test identifier — same shape as `mesh_partition.rs`'s `TestId`.
/// The wire-frame JSON uses string identifiers anyway, so the
/// snapshot's JSON shape is independent of the concrete `I`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

/// Synthetic `ClusterStateSnapshot<TestId>` matching the on-wire shape
/// produced by `dynrunner_manager_distributed::ClusterState::snapshot`.
///
/// This test fixture intentionally does NOT depend on
/// `dynrunner-manager-distributed` — that would inject a dev-dep
/// cycle (manager-distributed already depends on this crate). We
/// hand-roll a serde-compatible struct whose JSON encoding is
/// byte-identical to `ClusterStateSnapshot<TestId>::serialize`. The
/// snapshot tests in `cluster_state.rs` pin the round-trip on the
/// production side; this test pins the transport-layer plumbing
/// against the same wire shape.
///
/// The `capabilities` field mirrors the real
/// `capabilities: HashMap<String, CapabilityEntry>` 2P-set the
/// production snapshot serializes (C6 — the SINGLE source of
/// `is_observer` / `can_be_primary`, which replaced the old projected
/// `observers` field). [`SyntheticCapability`] reproduces
/// `CapabilityEntry`'s EXACT externally-tagged serde shape verbatim (see
/// its doc), so the bytes this fixture round-trips are the bytes
/// production emits — not a parallel shape that would give false
/// confidence the transport carries the real role data.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SyntheticSnapshot {
    tasks: HashMap<String, serde_json::Value>,
    current_primary: Option<String>,
    primary_epoch: u64,
    phase_deps: HashMap<String, Vec<String>>,
    #[serde(default)]
    capabilities: HashMap<String, SyntheticCapability>,
}

/// Byte-for-byte mirror of `CapabilityEntry`'s serde shape
/// (`cluster_state/types.rs`). `CapabilityEntry` is a plain
/// `#[derive(Serialize, Deserialize)]` enum with no `serde(tag)` /
/// `rename` / `deny_unknown_fields`, so serde's DEFAULT externally-tagged
/// representation applies. `Advertised { is_observer, can_be_primary,
/// cap_version }` encodes as
/// `{"Advertised":{"is_observer":<bool>,"can_be_primary":<bool>,"cap_version":{"primary_epoch":<u64>,"seq":<u32>}}}`;
/// `Departed` (a unit variant) encodes as the bare string `"Departed"`.
///
/// Because the encoding is STRUCTURAL, the variant + field names here MUST
/// match `CapabilityEntry`'s exactly for the bytes to be identical — which
/// is the whole point of this fixture (mirror the real encoder's bytes, do
/// not invent a parallel shape). `cap_version` mirrors `TaskVersion`'s
/// `{primary_epoch, seq}` serde shape via [`SyntheticVersion`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum SyntheticCapability {
    Advertised {
        is_observer: bool,
        can_be_primary: bool,
        cap_version: SyntheticVersion,
    },
    Departed,
}

/// Mirror of `TaskVersion`'s serde shape (`core/types/version.rs`):
/// `{ "primary_epoch": <u64>, "seq": <u32> }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
struct SyntheticVersion {
    primary_epoch: u64,
    seq: u32,
}

/// Pre-bake a snapshot payload the responder will echo. Two task
/// hashes, a known primary epoch, and one capability entry: an
/// `Advertised { is_observer: true, .. }` for the late-joining observer
/// peer. The shape is deliberately minimal — the assertion target is
/// `tasks` + `capabilities` round-trip; the live merge-rule tests live in
/// `cluster_state.rs`.
fn make_synthetic_snapshot() -> SyntheticSnapshot {
    SyntheticSnapshot {
        tasks: [
            ("task-1".to_string(), serde_json::json!({"Pending": {}})),
            ("task-2".to_string(), serde_json::json!({"Pending": {}})),
        ]
        .into_iter()
        .collect(),
        current_primary: Some("primary-peer".to_string()),
        primary_epoch: 7,
        phase_deps: HashMap::new(),
        capabilities: [(
            "observer-peer".to_string(),
            SyntheticCapability::Advertised {
                is_observer: true,
                can_be_primary: false,
                cap_version: SyntheticVersion {
                    primary_epoch: 7,
                    seq: 1,
                },
            },
        )]
        .into_iter()
        .collect(),
    }
}

/// Synchronously process whatever is in `responder`'s inbox; reply to
/// each `RequestClusterSnapshot` with the canned snapshot. Caller
/// drives this between joiner sends/recvs — we don't `spawn` to keep
/// the test single-task and deterministic.
async fn responder_pump(
    responder: &mut ChannelPeerTransport<TestId>,
    responder_id: &str,
    snapshot_json: &str,
) {
    // Drain everything currently visible without blocking. The
    // `recv_peer` future is cancel-safe (its only `.await` point is
    // the unbounded receiver), so wrapping in a tiny timeout to
    // detect "nothing pending" is the same shape `mesh_partition.rs`
    // uses.
    loop {
        let next = tokio::time::timeout(Duration::from_millis(5), responder.recv_peer()).await;
        match next {
            Err(_) => return, // timeout = inbox quiescent
            Ok(None) => return,
            Ok(Some(DistributedMessage::RequestClusterSnapshot {
                target: None,
                sender_id,
                ..
            })) => {
                let reply: DistributedMessage<TestId> = DistributedMessage::ClusterSnapshot {
                    target: None,
                    sender_id: responder_id.to_string(),
                    timestamp: timestamp_now(),
                    snapshot_json: snapshot_json.to_string(),
                };
                // The unicast reply goes back to the joiner via its id
                // (carried in the request's sender_id). Mirrors the
                // dispatch.rs receiver path exactly.
                let _ = responder.send_to_peer(&sender_id, reply).await;
            }
            Ok(Some(_other)) => {
                // Non-request frames are silently dropped — the
                // responder fixture is a one-trick simulator. Live
                // dispatch.rs handles many more variants.
            }
        }
    }
}

/// End-to-end: 4-node channel mesh — three "cluster" peers
/// (primary-peer, observer-peer, regular-peer) plus a joiner. The
/// joiner calls `join_running_cluster(seed)` where seed lists all
/// three existing peers; one of them (primary-peer) responds with the
/// canned snapshot.
///
/// Assertions:
/// 1. `join_running_cluster` returns `Ok(snapshot_json)` within the
///    bootstrap timeout.
/// 2. The returned JSON deserializes back into `SyntheticSnapshot` —
///    proves the wire frame round-tripped exactly.
/// 3. `snapshot.tasks` matches the canned set — proves the task
///    ledger survives the snapshot RPC.
/// 4. `snapshot.capabilities` matches the canned 2P-set — proves the
///    role-capability roster (C6) survives the snapshot roundtrip (the
///    `Advertised { is_observer: true, .. }` entry for the observer peer
///    round-trips byte-identically to `CapabilityEntry`'s wire shape).
#[tokio::test]
async fn join_running_cluster_returns_snapshot_with_capabilities() {
    let peer_ids: Vec<String> = vec![
        "joiner".into(),
        "primary-peer".into(),
        "observer-peer".into(),
        "regular-peer".into(),
    ];
    let mut transports: Vec<ChannelPeerTransport<TestId>> = peer_mesh::<TestId>(&peer_ids);

    // Pop the joiner off the front; the rest are the responding peers.
    // peer_mesh returns transports in input order so index 0 is joiner.
    let mut joiner = transports.remove(0);
    let mut primary = transports.remove(0); // was index 1
    let mut observer = transports.remove(0); // was index 2
    let mut regular = transports.remove(0); // was index 3

    let canned = make_synthetic_snapshot();
    let canned_json = serde_json::to_string(&canned).expect("synthetic snapshot serializes");

    // Seed lists all three live peers. Real PeerConnectionInfo cert
    // / port fields are irrelevant for the channel transport (its
    // `connect_to_peers` is a no-op) — fill with empty strings to
    // satisfy the struct shape.
    let seed: Vec<PeerConnectionInfo> = ["primary-peer", "observer-peer", "regular-peer"]
        .iter()
        .map(|id| PeerConnectionInfo {
            secondary_id: (*id).into(),
            cert: String::new(),
            ipv4: None,
            ipv6: None,
            port: 0,
            is_observer: *id == "observer-peer",
            liveness_port: None,
        })
        .collect();

    // Drive the joiner's `join_running_cluster` concurrently with a
    // responder pump on `primary` (the snapshot answerer). The other
    // two peers don't respond — first-success-wins on the joiner side
    // (it iterates seed in order; the first peer to accept the
    // unicast send is the chosen responder).
    // `is_observer = true` + `can_be_primary = false`: this scenario is
    // the fresh observer late-joiner described in the module doc (an
    // observer is never primary-capable).
    let join_fut = joiner.join_running_cluster(&seed, DEFAULT_JOIN_TIMEOUT, true, false);
    tokio::pin!(join_fut);

    // The join future immediately sends out a RequestClusterSnapshot
    // and then blocks on recv. We drive responders in a loop until
    // join_fut resolves. The channel transport is synchronous-fast
    // (mpsc), so a single responder cycle should be enough; the
    // wall-clock bound here is generous for CI noise.
    let join_result = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            tokio::select! {
                biased;
                result = &mut join_fut => break result,
                _ = responder_pump(&mut primary, "primary-peer", &canned_json) => {
                    // Responder ran a pass; loop back to give the
                    // joiner a chance to deliver. Non-target peers
                    // also get a pump pass below so any stray frames
                    // they receive get processed.
                }
                _ = responder_pump(&mut observer, "observer-peer", &canned_json) => {}
                _ = responder_pump(&mut regular, "regular-peer", &canned_json) => {}
            }
        }
    })
    .await
    .expect("test deadline: join_running_cluster did not resolve within 2s");

    let snapshot_jsons = match join_result {
        Ok(s) => s,
        Err(e) => panic!("join_running_cluster failed: {e}"),
    };

    // Multi-responder bootstrap returns a non-empty Vec; only
    // primary-peer answers here, so exactly one payload comes back.
    assert!(
        !snapshot_jsons.is_empty(),
        "join_running_cluster must return at least one snapshot on Ok"
    );
    let parsed: SyntheticSnapshot =
        serde_json::from_str(&snapshot_jsons[0]).expect("returned snapshot_json round-trips");

    // Task ledger survives the RPC.
    assert_eq!(
        parsed
            .tasks
            .keys()
            .collect::<std::collections::HashSet<_>>(),
        ["task-1".to_string(), "task-2".to_string()]
            .iter()
            .collect()
    );

    // C6 contract: the role-capability 2P-set survives the snapshot
    // roundtrip. Without this, the joiner's election filter would briefly
    // promote an observer in the gap between snapshot-restore and the next
    // live PeerInfo broadcast. The decoded entry must be the exact
    // `Advertised { is_observer: true, .. }` the fixture baked — proving
    // the transport carried `CapabilityEntry`'s real wire bytes (the
    // structured capability state), not just a key presence.
    let expected_capabilities: HashMap<String, SyntheticCapability> = [(
        "observer-peer".to_string(),
        SyntheticCapability::Advertised {
            is_observer: true,
            can_be_primary: false,
            cap_version: SyntheticVersion {
                primary_epoch: 7,
                seq: 1,
            },
        },
    )]
    .into_iter()
    .collect();
    assert_eq!(parsed.capabilities, expected_capabilities);

    // Primary-epoch carries through (canonical authority for the
    // joiner's role-table on restore).
    assert_eq!(parsed.primary_epoch, 7);
    assert_eq!(parsed.current_primary.as_deref(), Some("primary-peer"));
}

/// Multi-responder bootstrap: the joiner fans `RequestClusterSnapshot`
/// to ALL seeds and collects EVERY responder's snapshot (not just the
/// first). This is the completeness fix — the first reachable seed may
/// hold an INCOMPLETE roster, so a single reply could bootstrap from a
/// partial snapshot. Two responders answer with DISTINCT payloads (one
/// incomplete: {task-A}; one complete: {task-A, task-B}); the joiner
/// returns both, and the caller-side union (decode each + restore) heals
/// to {task-A, task-B}. The union is modelled here at the wire level
/// (this transport-layer test can't depend on the manager-distributed
/// `restore` lattice — that round-trip is pinned in `cluster_state.rs`).
#[tokio::test]
async fn join_running_cluster_collects_all_responders_for_union() {
    let peer_ids: Vec<String> = vec![
        "joiner".into(),
        "incomplete-peer".into(),
        "complete-peer".into(),
    ];
    let mut transports: Vec<ChannelPeerTransport<TestId>> = peer_mesh::<TestId>(&peer_ids);
    let mut joiner = transports.remove(0);
    let mut incomplete = transports.remove(0);
    let mut complete = transports.remove(0);

    // Incomplete responder: only task-A. Complete responder: task-A +
    // task-B. Their union is the full ledger.
    let incomplete_snap = SyntheticSnapshot {
        tasks: [("task-A".to_string(), serde_json::json!({"Pending": {}}))]
            .into_iter()
            .collect(),
        current_primary: None,
        primary_epoch: 1,
        phase_deps: HashMap::new(),
        capabilities: Default::default(),
    };
    let complete_snap = SyntheticSnapshot {
        tasks: [
            ("task-A".to_string(), serde_json::json!({"Pending": {}})),
            ("task-B".to_string(), serde_json::json!({"Pending": {}})),
        ]
        .into_iter()
        .collect(),
        current_primary: Some("primary-peer".to_string()),
        primary_epoch: 5,
        phase_deps: HashMap::new(),
        capabilities: Default::default(),
    };
    let incomplete_json = serde_json::to_string(&incomplete_snap).unwrap();
    let complete_json = serde_json::to_string(&complete_snap).unwrap();

    let seed: Vec<PeerConnectionInfo> = ["incomplete-peer", "complete-peer"]
        .iter()
        .map(|id| PeerConnectionInfo {
            secondary_id: (*id).into(),
            cert: String::new(),
            ipv4: None,
            ipv6: None,
            port: 0,
            is_observer: false,
            liveness_port: None,
        })
        .collect();

    let join_fut = joiner.join_running_cluster(&seed, DEFAULT_JOIN_TIMEOUT, false, true);
    tokio::pin!(join_fut);

    let join_result = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            tokio::select! {
                biased;
                result = &mut join_fut => break result,
                _ = responder_pump(&mut incomplete, "incomplete-peer", &incomplete_json) => {}
                _ = responder_pump(&mut complete, "complete-peer", &complete_json) => {}
            }
        }
    })
    .await
    .expect("test deadline: join_running_cluster did not resolve within 2s");

    let snapshot_jsons = match join_result {
        Ok(s) => s,
        Err(e) => panic!("join_running_cluster failed: {e}"),
    };

    // BOTH responders' snapshots are collected (the multi-responder
    // contract — first-success-wins would have returned exactly one).
    assert_eq!(
        snapshot_jsons.len(),
        2,
        "both responders' snapshots must be collected, got {}",
        snapshot_jsons.len()
    );

    // The union of the returned payloads' task sets is the complete
    // ledger — proving an incomplete responder is healed by a complete
    // one (the idempotent-lattice union the caller performs via restore).
    let mut union: std::collections::HashSet<String> = std::collections::HashSet::new();
    for json in &snapshot_jsons {
        let parsed: SyntheticSnapshot =
            serde_json::from_str(json).expect("each returned snapshot round-trips");
        union.extend(parsed.tasks.keys().cloned());
    }
    assert_eq!(
        union,
        ["task-A".to_string(), "task-B".to_string()]
            .into_iter()
            .collect(),
        "the union of all responders' snapshots heals to the complete ledger"
    );
}

/// Edge case: when the seed list contains only the joiner itself,
/// there's no candidate to send to and `join_running_cluster` must
/// surface `NoReachablePeer` rather than time out silently.
#[tokio::test]
async fn join_running_cluster_empty_seed_errors_fast() {
    let peer_ids: Vec<String> = vec!["joiner".into(), "other".into()];
    let mut transports = peer_mesh::<TestId>(&peer_ids);
    let mut joiner = transports.remove(0);

    // Seed with only the joiner's own id — no valid candidates.
    let seed: Vec<PeerConnectionInfo> = vec![PeerConnectionInfo {
        secondary_id: "joiner".into(),
        cert: String::new(),
        ipv4: None,
        ipv6: None,
        port: 0,
        is_observer: false,
        liveness_port: None,
    }];

    // Short timeout: with no candidates the connect-loop's
    // peer_count > 0 gate still passes (the channel mesh pre-wires
    // "other"), but the send loop finds no non-self id and surfaces
    // SendFailed. Either error is acceptable — the contract is "fail
    // fast, don't burn the full bootstrap budget".
    let timeout = Duration::from_millis(500);
    // `is_observer = false`: a joining worker (the common case); the
    // role is irrelevant here since no request is ever sent.
    let result = joiner
        .join_running_cluster(&seed, timeout, false, false)
        .await;
    match result {
        Err(JoinError::SendFailed(_)) | Err(JoinError::NoReachablePeer) => {}
        other => panic!("expected SendFailed or NoReachablePeer, got {other:?}"),
    }
}

/// Pump one cluster peer through a PRIMARY-PROMOTION window. Each pass
/// first pushes realistic gossip at the joiner (a stamped broadcast
/// `Keepalive` + `ClusterMutation` — exactly the frame traffic the
/// production joiner's legs carried throughout its bootstrap window),
/// then drains the inbox:
///
/// - a `RequestClusterSnapshot` received BEFORE `promoted_at` is
///   DROPPED — the promotion window: the responder seat is churning
///   (mid coordinator-swap slot loss / reply legs not yet established),
///   so the joiner's first-shot request dies silently;
/// - after `promoted_at`, the peer holding `snapshot_json = Some(..)`
///   is the newly-seated responder and answers with a
///   production-shaped, role-stamped reply
///   (`Some(Destination::Observer(<joiner>))` — the
///   `anti_entropy::reply_destination` stamp); peers with `None` keep
///   gossiping and never answer.
async fn promotion_window_pump(
    transport: &mut ChannelPeerTransport<TestId>,
    id: &str,
    snapshot_json: Option<&str>,
    promoted_at: tokio::time::Instant,
) {
    let keepalive: DistributedMessage<TestId> = DistributedMessage::Keepalive {
        target: Some(Destination::All),
        sender_id: id.to_string(),
        timestamp: timestamp_now(),
        secondary_id: id.to_string(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    };
    let _ = transport.send_to_peer("joiner", keepalive).await;
    let mutation: DistributedMessage<TestId> = DistributedMessage::ClusterMutation {
        target: Some(Destination::All),
        sender_id: id.to_string(),
        timestamp: timestamp_now(),
        mutations: Vec::new(),
    };
    let _ = transport.send_to_peer("joiner", mutation).await;
    loop {
        let next = tokio::time::timeout(Duration::from_millis(5), transport.recv_peer()).await;
        match next {
            Err(_) => return, // inbox quiescent
            Ok(None) => return,
            Ok(Some(DistributedMessage::RequestClusterSnapshot { sender_id, .. })) => {
                let seated = tokio::time::Instant::now() >= promoted_at;
                if let (true, Some(json)) = (seated, snapshot_json) {
                    let reply: DistributedMessage<TestId> = DistributedMessage::ClusterSnapshot {
                        target: Some(Destination::Observer(PeerId::from(sender_id.clone()))),
                        sender_id: id.to_string(),
                        timestamp: timestamp_now(),
                        snapshot_json: json.to_string(),
                    };
                    let _ = transport.send_to_peer(&sender_id, reply).await;
                }
                // In-window requests (and post-window requests to a
                // non-responder) are dropped — one-shot loss.
            }
            Ok(Some(_other)) => {}
        }
    }
}

/// Promotion-window replay (asm-tokenizer test-env forensics): an
/// observer late-joiner bootstraps while NO primary is seated. Its
/// snapshot-request fan-out lands inside the promotion window and every
/// first-shot request is lost; broadcast gossip (`ClusterMutation` /
/// `Keepalive`) keeps arriving on the joiner's legs the whole time —
/// the bootstrap window is NOT a silent channel. The promotion
/// completes INSIDE the bootstrap budget.
///
/// Contract under test: the bootstrap RE-REQUESTS the snapshot on a
/// cadence until its deadline, so a re-request reaching the
/// newly-seated responder heals the join within the SAME bootstrap
/// budget. A one-shot request protocol fails this test by sitting out
/// the rest of the budget dropping gossip and dying
/// `JoinError::Timeout` — the production observer late-joiner FATAL.
#[tokio::test]
async fn join_rerequests_until_a_promotion_window_closes() {
    let peer_ids: Vec<String> = vec![
        "joiner".into(),
        "promoting-peer".into(),
        "secondary-1".into(),
        "secondary-2".into(),
    ];
    let mut transports: Vec<ChannelPeerTransport<TestId>> = peer_mesh::<TestId>(&peer_ids);
    let mut joiner = transports.remove(0);
    let mut promoting = transports.remove(0);
    let mut sec1 = transports.remove(0);
    let mut sec2 = transports.remove(0);

    let canned = make_synthetic_snapshot();
    let canned_json = serde_json::to_string(&canned).expect("synthetic snapshot serializes");

    let seed: Vec<PeerConnectionInfo> = ["promoting-peer", "secondary-1", "secondary-2"]
        .iter()
        .map(|id| PeerConnectionInfo {
            secondary_id: (*id).into(),
            cert: String::new(),
            ipv4: None,
            ipv6: None,
            port: 0,
            is_observer: false,
            liveness_port: None,
        })
        .collect();

    // The promotion seats 600ms in — well inside the 2s bootstrap
    // budget, mirroring the production timeline (dial at :54, the
    // PrimaryChanged released ~6s later, ~4s of budget left).
    let promoted_at = tokio::time::Instant::now() + Duration::from_millis(600);

    let join_fut = joiner.join_running_cluster(&seed, Duration::from_secs(2), true, false);
    tokio::pin!(join_fut);

    let join_result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                biased;
                result = &mut join_fut => break result,
                _ = promotion_window_pump(&mut promoting, "promoting-peer", Some(&canned_json), promoted_at) => {}
                _ = promotion_window_pump(&mut sec1, "secondary-1", None, promoted_at) => {}
                _ = promotion_window_pump(&mut sec2, "secondary-2", None, promoted_at) => {}
            }
        }
    })
    .await
    .expect("test deadline: join_running_cluster did not resolve within 5s");

    let snapshot_jsons = match join_result {
        Ok(s) => s,
        Err(e) => panic!(
            "bootstrap must re-request and heal once the promotion completes \
             within its own budget; instead it failed: {e}"
        ),
    };
    assert!(
        !snapshot_jsons.is_empty(),
        "the re-request reaching the newly-seated responder must yield a snapshot"
    );
    let parsed: SyntheticSnapshot =
        serde_json::from_str(&snapshot_jsons[0]).expect("returned snapshot_json round-trips");
    assert_eq!(parsed.primary_epoch, 7);
    assert_eq!(parsed.current_primary.as_deref(), Some("primary-peer"));
}
