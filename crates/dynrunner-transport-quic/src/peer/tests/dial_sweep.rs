//! `connect_to_peers` sweep-summary dispositions (#362).
//!
//! The production silence: a member logged "received peer list,
//! kicking off peer dials peers=2" and then NOTHING, ever — because it
//! was the lexicographically-HIGHEST id in the fleet, so the
//! lower-id-dials rule made it spawn ZERO dials (each skip logged only
//! at DEBUG) and its entire mesh depended on the siblings' inbound
//! dials. These tests pin the [`DialSweepSummary`] dispositions that
//! now narrate that shape at operator level, the dropped-peer
//! detection on authoritative-list replacement, and the higher-id
//! side's truthful "this node never dials it" reconnect summary.

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

/// HIGHEST-id node: every listed peer sorts lower, so the sweep spawns
/// ZERO dials and names every peer as awaiting-inbound — the exact
/// structural shape behind the #362 "dials die 4-for-4 with zero
/// trace" member. Pre-summary, this outcome was invisible.
#[tokio::test(flavor = "current_thread")]
async fn highest_id_sweep_spawns_zero_dials_and_names_awaiting_inbound() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("peer-z", None).await.unwrap();
            let summary = net.connect_to_peers_inner(&[pinfo("peer-a"), pinfo("peer-b")]);
            assert_eq!(
                summary,
                DialSweepSummary {
                    listed: 2,
                    spawned: 0,
                    already_connected: 0,
                    awaiting_inbound: vec!["peer-a".into(), "peer-b".into()],
                    dropped_from_list: vec![],
                },
                "the highest-id node must spawn no dials and name both \
                 peers as awaiting-inbound"
            );
            // The dial info is still cached for both (redial signals +
            // reconnect tracking need it even on the non-dialing side).
            assert_eq!(net.peer_dial_info.len(), 2);
        })
        .await;
}

/// LOWEST-id node: it owns the dial side for every listed peer.
#[tokio::test(flavor = "current_thread")]
async fn lowest_id_sweep_spawns_a_dial_per_peer() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("peer-a", None).await.unwrap();
            let summary = net.connect_to_peers_inner(&[pinfo("peer-b"), pinfo("peer-z")]);
            assert_eq!(summary.listed, 2);
            assert_eq!(summary.spawned, 2);
            assert!(summary.awaiting_inbound.is_empty());
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

/// Higher-id side's reconnect summary must NOT claim to be dialing
/// (it never does — lower-id-dials rule); it must instead point the
/// operator at the peer's own dial logs / this node's advertised
/// address. Mirror of `dial_failure_summary_fires_at_threshold_with_addr`,
/// with self ABOVE the tracked peer.
#[tokio::test(flavor = "current_thread")]
async fn higher_id_summary_names_awaiting_inbound_not_dialing() {
    let records: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = CaptureLayer {
        records: records.clone(),
    };
    let subscriber = Registry::default().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Self sorts HIGHER than the tracked peer ⇒ this node never
            // dials it.
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("peer-z", None).await.unwrap();
            let info = pinfo("peer-a");
            net.peer_dial_info.insert(info.secondary_id.clone(), info);

            for _ in 0..DIAL_SUMMARY_THRESHOLD {
                net.process_reconnect_tick();
            }

            let captured = records.lock().unwrap().clone();
            let summaries: Vec<&CapturedEvent> = captured
                .iter()
                .filter(|e| e.message.contains("peer leg missing"))
                .collect();
            assert_eq!(
                summaries.len(),
                1,
                "exactly one awaiting-inbound summary at the threshold; got {captured:#?}"
            );
            assert!(
                summaries[0].message.contains("NEVER dials"),
                "the summary must say this node never dials the peer; got {:?}",
                summaries[0].message
            );
            // And no event may claim this node is DIALING the peer —
            // that was the pre-fix lie ("peer unreachable; dialing
            // address") on the higher-id side.
            assert!(
                !captured
                    .iter()
                    .any(|e| e.message.contains("peer unreachable; dialing address")),
                "higher-id side must not emit the dialing-side summary; got {captured:#?}"
            );
        })
        .await;
}
