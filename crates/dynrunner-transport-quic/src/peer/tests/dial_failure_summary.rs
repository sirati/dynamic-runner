//! `dial_failure_summary_*` — the operator-visible per-peer
//! dial-failure summary fired from `process_reconnect_tick`.
//!
//! Observability-only: the summary carries the peer id, the address
//! being dialed, and the consecutive-failed-dial count, throttled to
//! the `DIAL_SUMMARY_THRESHOLD` / `DIAL_SUMMARY_RECURRENCE` boundaries
//! so it never floods. These tests pin (a) that the WARN carries the
//! resolved dial address (the datum the missing-`%addr` incident
//! needed), and (b) the throttle boundary — silent before the
//! threshold, one WARN at it.

use std::sync::{Arc, Mutex};

use super::super::PeerNetwork;
use super::super::reconnect::DIAL_SUMMARY_THRESHOLD;
use super::TestId;
use super::log_capture::{CaptureLayer, CapturedEvent};
use dynrunner_protocol_primary_secondary::PeerConnectionInfo;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

/// The dialed-address token the summary must surface. A
/// distinctive non-routable address so the assertion can't match
/// incidentally on some other log line's address.
const UNROUTABLE_IPV4: &str = "10.255.255.254";
const UNROUTABLE_PORT: u16 = 59123;

/// Count of `process_reconnect_tick` calls whose emitted summary
/// WARNs (target=this crate, mentioning the unroutable address) we
/// observe — paired with the captured records for content checks.
fn summary_events(records: &Arc<Mutex<Vec<CapturedEvent>>>) -> Vec<CapturedEvent> {
    // The dialed address rides as a structured `addr=` field (the
    // `%addr` in the WARN), so match on the captured field string, not
    // the format-string message.
    records
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.fields.contains(UNROUTABLE_IPV4))
        .cloned()
        .collect()
}

/// Build a peer (NOT self, lexicographically higher than "peer-a" so
/// the lower-id-dials rule lets "peer-a" own the dial) with an
/// unroutable address, registered in `peer_dial_info` but absent from
/// `connections` — the exact "tracked, perpetually failing to dial"
/// shape `process_reconnect_tick` summarises.
fn unreachable_peer_info() -> PeerConnectionInfo {
    PeerConnectionInfo {
        secondary_id: "peer-z".into(),
        cert: String::new(),
        ipv4: Some(UNROUTABLE_IPV4.into()),
        ipv6: None,
        port: UNROUTABLE_PORT,
        is_observer: false,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn dial_failure_summary_fires_at_threshold_with_addr() {
    let records: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = CaptureLayer {
        records: records.clone(),
    };
    let subscriber = Registry::default().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_a: PeerNetwork<TestId> = PeerNetwork::start("peer-a").await.unwrap();
            // Register the unreachable peer's dial info, but never
            // connect it — so every reconnect tick observes it as
            // disconnected and re-dials (which fails silently against
            // the unroutable address).
            let info = unreachable_peer_info();
            peer_a
                .peer_dial_info
                .insert(info.secondary_id.clone(), info);

            // Ticks before the threshold: the summary must stay SILENT.
            // (The first tick also emits the one-shot "peer disconnect
            // observed" WARN from the tracker, which does NOT mention
            // the dialed address — our filter ignores it.)
            for _ in 0..(DIAL_SUMMARY_THRESHOLD - 1) {
                peer_a.process_reconnect_tick();
            }
            assert!(
                summary_events(&records).is_empty(),
                "dial-failure summary must be silent before the threshold; got {:#?}",
                summary_events(&records)
            );

            // The threshold tick: exactly one address-carrying summary.
            peer_a.process_reconnect_tick();
            let events = summary_events(&records);
            assert_eq!(
                events.len(),
                1,
                "exactly one summary at the threshold; got {events:#?}"
            );
            let fields = &events[0].fields;
            // The WARN must carry the dialed socket address (ip:port)
            // and the consecutive-failure count — the two operator
            // diagnostics — as structured fields.
            assert!(
                fields.contains(&format!("{UNROUTABLE_IPV4}:{UNROUTABLE_PORT}")),
                "summary must carry the dialed socket addr; got fields {fields:?}"
            );
            assert!(
                fields.contains(&format!(
                    "consecutive_failed_dials={DIAL_SUMMARY_THRESHOLD}"
                )),
                "summary must carry the consecutive-failure count; got fields {fields:?}"
            );
            // Must come from this crate's transport target, not a
            // third-party one.
            assert!(
                events[0].target.starts_with("dynrunner_transport_quic"),
                "summary target should be this transport crate; got {:?}",
                events[0].target
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn dial_failure_summary_not_emitted_when_peer_connects() {
    // Revert-guard on the "tracked AND not connected" precondition: a
    // peer that is present in `connections` is observed as reconnected
    // every tick, never accrues failed-dial count, and so never
    // summarises — even past where the threshold would otherwise fire.
    let records: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = CaptureLayer {
        records: records.clone(),
    };
    let subscriber = Registry::default().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_a: PeerNetwork<TestId> = PeerNetwork::start("peer-a").await.unwrap();
            let info = unreachable_peer_info();
            let peer_id = info.secondary_id.clone();
            peer_a.peer_dial_info.insert(peer_id.clone(), info);
            // Inject a live connection entry: a dummy channel so the
            // peer reads as connected. (The receiver is dropped, but
            // `process_reconnect_tick` never sends on it — it only
            // checks membership.)
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            peer_a.connections.insert(peer_id.clone(), tx);

            for _ in 0..(DIAL_SUMMARY_THRESHOLD + 2) {
                peer_a.process_reconnect_tick();
            }
            assert!(
                summary_events(&records).is_empty(),
                "a connected peer must never summarise a dial failure; got {:#?}",
                summary_events(&records)
            );
        })
        .await;
}
