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
//! The pre-baked snapshot JSON includes both task entries AND an
//! `observers` field — the joiner asserts both round-trip. This pins
//! Step 7's "observers as first-class cluster facts" wiring through
//! the snapshot frame: without observers carried through the
//! snapshot, a fresh joiner would have an empty `role_table.observers`
//! between snapshot-restore and the next live PeerInfo broadcast, and
//! its election filter (`secondary::election::lowest_alive` skips
//! observers) would briefly mis-promote an observer candidate.

use std::collections::HashMap;
use std::time::Duration;

use dynrunner_protocol_primary_secondary::{
    DEFAULT_JOIN_TIMEOUT, DistributedMessage, JoinError, PeerConnectionInfo, PeerTransport,
    timestamp_now,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SyntheticSnapshot {
    tasks: HashMap<String, serde_json::Value>,
    current_primary: Option<String>,
    primary_epoch: u64,
    phase_deps: HashMap<String, Vec<String>>,
    #[serde(default)]
    observers: std::collections::HashSet<String>,
}

/// Pre-bake a snapshot payload the responder will echo. Two task
/// hashes, a known primary epoch, and two observer ids. The shape is
/// deliberately minimal — the assertion target is `tasks` + `observers`
/// round-trip; the live merge-rule tests live in `cluster_state.rs`.
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
        observers: ["observer-peer".to_string()].into_iter().collect(),
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
            Ok(Some(DistributedMessage::RequestClusterSnapshot { sender_id, .. })) => {
                let reply: DistributedMessage<TestId> = DistributedMessage::ClusterSnapshot {
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
/// 4. `snapshot.observers` matches the canned set — proves Step 7's
///    "observers as first-class cluster facts" wiring survives the
///    snapshot roundtrip (this is the new contract added in Step 8).
#[tokio::test]
async fn join_running_cluster_returns_snapshot_with_observers() {
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

    let snapshot_json = match join_result {
        Ok(s) => s,
        Err(e) => panic!("join_running_cluster failed: {e}"),
    };

    let parsed: SyntheticSnapshot =
        serde_json::from_str(&snapshot_json).expect("returned snapshot_json round-trips");

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

    // Step 7 + 8 contract: observers survive the snapshot roundtrip.
    // Without this, the joiner's election filter would briefly
    // promote an observer in the gap between snapshot-restore and
    // the next live PeerInfo broadcast.
    assert_eq!(
        parsed.observers,
        ["observer-peer".to_string()].into_iter().collect()
    );

    // Primary-epoch carries through (canonical authority for the
    // joiner's role-table on restore).
    assert_eq!(parsed.primary_epoch, 7);
    assert_eq!(parsed.current_primary.as_deref(), Some("primary-peer"));
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
    }];

    // Short timeout: with no candidates the connect-loop's
    // peer_count > 0 gate still passes (the channel mesh pre-wires
    // "other"), but the send loop finds no non-self id and surfaces
    // SendFailed. Either error is acceptable — the contract is "fail
    // fast, don't burn the full bootstrap budget".
    let timeout = Duration::from_millis(500);
    // `is_observer = false`: a joining worker (the common case); the
    // role is irrelevant here since no request is ever sent.
    let result = joiner.join_running_cluster(&seed, timeout, false, false).await;
    match result {
        Err(JoinError::SendFailed(_)) | Err(JoinError::NoReachablePeer) => {}
        other => panic!("expected SendFailed or NoReachablePeer, got {other:?}"),
    }
}
