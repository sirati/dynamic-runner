//! `connect_to_peers` sweep-summary dispositions (#362) under full-mesh
//! dialing.
//!
//! Full-mesh: EVERY node dials EVERY not-yet-connected peer, regardless
//! of id ordering — the lower-id-dials asymmetry (and the silent-zero-
//! dial #362 shape it produced on the highest-id node) is gone. These
//! tests pin that every node spawns a dial per listed peer, the
//! already-connected / self skips, the dropped-peer detection on
//! authoritative-list replacement, and that the reconnect dial-failure
//! summary now always names this node as the dialer.

use std::sync::{Arc, Mutex};

use super::super::reconnect::DIAL_SUMMARY_THRESHOLD;
use super::super::{DialSweepSummary, PeerNetwork};
use super::TestId;
use super::log_capture::{CaptureLayer, CapturedEvent};
use dynrunner_protocol_primary_secondary::PeerConnectionInfo;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

fn pinfo(id: &str) -> PeerConnectionInfo {
    PeerConnectionInfo {
        secondary_id: id.into(),
        cert: String::new(),
        // Unroutable test address: dial tasks spawned against it just
        // fail in the background after the test ends; the assertions
        // here only read the sweep summary, never await the dials.
        ipv4: Some("10.255.255.254".into()),
        ipv6: None,
        port: 59124,
        is_observer: false,
        liveness_port: None,
        slurm_job_id: None,
    }
}

/// HIGHEST-id node under full mesh: it now DIALS every listed peer
/// (regardless of id ordering) — the previously-broken case where the
/// highest-id node spawned ZERO dials and its mesh hung on the siblings'
/// inbound dials (the #362 shape). A lower-id peer is dialed, where
/// before it would have parked awaiting-inbound.
#[tokio::test(flavor = "current_thread")]
async fn highest_id_sweep_dials_every_lower_id_peer() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("peer-z", None).await.unwrap();
            let summary = net.connect_to_peers_inner(&[pinfo("peer-a"), pinfo("peer-b")]);
            assert_eq!(
                summary,
                DialSweepSummary {
                    listed: 2,
                    spawned: 2,
                    already_connected: 0,
                    dropped_from_list: vec![],
                },
                "full-mesh: the highest-id node must dial BOTH lower-id peers"
            );
            assert_eq!(net.peer_dial_info.len(), 2);
        })
        .await;
}

/// LOWEST-id node: it dials every listed peer too (always did, and still
/// does under full mesh).
#[tokio::test(flavor = "current_thread")]
async fn lowest_id_sweep_spawns_a_dial_per_peer() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("peer-a", None).await.unwrap();
            let summary = net.connect_to_peers_inner(&[pinfo("peer-b"), pinfo("peer-z")]);
            assert_eq!(summary.listed, 2);
            assert_eq!(summary.spawned, 2);
        })
        .await;
}

/// Self is skipped entirely (not listed); an already-connected peer is
/// counted but not re-dialed.
#[tokio::test(flavor = "current_thread")]
async fn sweep_skips_self_and_counts_already_connected() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("peer-a", None).await.unwrap();
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            net.connections.insert("peer-b".to_string(), tx);
            let summary =
                net.connect_to_peers_inner(&[pinfo("peer-a"), pinfo("peer-b"), pinfo("peer-c")]);
            assert_eq!(summary.listed, 2, "self must not be listed");
            assert_eq!(summary.already_connected, 1);
            assert_eq!(summary.spawned, 1);
        })
        .await;
}

/// Authoritative-list replacement that DROPS a previously-tracked peer
/// must surface the dropped id — losing a peer's dial info silently
/// kills its redial tracking forever, which is operator-significant
/// (e.g. a freshly-promoted primary broadcasting a partial roster).
#[tokio::test(flavor = "current_thread")]
async fn sweep_names_peers_dropped_from_the_authoritative_list() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("peer-z", None).await.unwrap();
            net.connect_to_peers_inner(&[pinfo("peer-a"), pinfo("peer-b")]);
            let summary = net.connect_to_peers_inner(&[pinfo("peer-b")]);
            assert_eq!(summary.dropped_from_list, vec!["peer-a".to_string()]);
            assert_eq!(
                net.peer_dial_info.len(),
                1,
                "replacement semantics unchanged: the dropped peer's dial info is gone"
            );
            // A list identical to the cache drops nothing.
            let summary = net.connect_to_peers_inner(&[pinfo("peer-b")]);
            assert!(summary.dropped_from_list.is_empty());
        })
        .await;
}

/// Full-mesh: a higher-id node's reconnect dial-failure summary now names
/// THIS node as the dialer ("peer unreachable; dialing address") even for
/// a lower-id peer — because under full mesh it dials everyone. The old
/// "this node NEVER dials it" awaiting-inbound summary is gone.
#[tokio::test(flavor = "current_thread")]
async fn higher_id_summary_names_self_as_dialer() {
    let records: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = CaptureLayer {
        records: records.clone(),
    };
    let subscriber = Registry::default().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Self sorts HIGHER than the tracked peer — under full mesh it
            // STILL dials it.
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("peer-z", None).await.unwrap();
            let info = pinfo("peer-a");
            net.peer_dial_info.insert(info.secondary_id.clone(), info);

            for _ in 0..DIAL_SUMMARY_THRESHOLD {
                net.process_reconnect_tick();
            }

            let captured = records.lock().unwrap().clone();
            let summaries: Vec<&CapturedEvent> = captured
                .iter()
                .filter(|e| e.message.contains("peer unreachable; dialing address"))
                .collect();
            assert_eq!(
                summaries.len(),
                1,
                "exactly one dialing-side summary at the threshold; got {captured:#?}"
            );
            // The old higher-id-only awaiting-inbound summary must NOT fire.
            assert!(
                !captured
                    .iter()
                    .any(|e| e.message.contains("peer leg missing")),
                "full-mesh: no awaiting-inbound summary may fire; got {captured:#?}"
            );
        })
        .await;
}
