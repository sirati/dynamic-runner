//! Dial-side cert contract: a seed entry that CARRIES the peer's cert
//! PEM authenticates over QUIC (the late-joiner-with-credentials
//! shape); a cert-less entry falls back to WSS AND the no-valid-cert
//! WARN names WHY in its `reasons=` field (pre-fix the field was
//! empty — the run_20260611_200548 production symptom gave the
//! operator no clue the seed simply had no cert to pin).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::super::PeerNetwork;
use super::super::dial::{DialAttempt, dial_peer};
use super::super::util::PeerConnection;
use super::TestId;
use super::log_capture::{CaptureLayer, CapturedEvent};
use dynrunner_protocol_primary_secondary::PeerConnectionInfo;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

/// Seed entry for the target network, with the given cert field.
fn seed_entry(port: u16, cert: &str) -> PeerConnectionInfo {
    PeerConnectionInfo {
        secondary_id: "peer-target".into(),
        cert: cert.into(),
        ipv4: Some("127.0.0.1".into()),
        ipv6: None,
        port,
        is_observer: false,
        liveness_port: None,
    }
}

/// A joiner whose seed entry carries the peer's cert PEM dials QUIC
/// with a VALID pinned cert — the QUIC leg connects (no WSS fallback).
/// This is the late-joiner-with-credentials contract: the same
/// `PeerConnectionInfo` that previously came cert-less from `.info`
/// records, now overlaid with the persisted cert, upgrades the dial.
#[tokio::test(flavor = "current_thread")]
async fn dial_with_pinned_cert_connects_quic() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let target: PeerNetwork<TestId> =
                PeerNetwork::start("peer-target", None).await.unwrap();
            let info = seed_entry(target.port(), target.cert_pem());

            let conn = dial_peer("peer-target", &info, DialAttempt::Initial)
                .await
                .expect("dial with a valid pinned cert must connect");
            assert!(
                matches!(conn, PeerConnection::Quic(_)),
                "a seed entry carrying the peer's cert must connect via QUIC, not WSS"
            );
        })
        .await;
}

/// A cert-less seed entry still connects (WSS fallback STAYS), and the
/// `no valid cert for peer` WARN carries a NON-EMPTY `reasons=` field
/// naming the absent cert. Replays the production WARN shape
/// (`reasons=` blank) and pins the fix.
#[tokio::test(flavor = "current_thread")]
async fn dial_certless_falls_back_to_wss_with_named_reason() {
    let records: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = CaptureLayer {
        records: records.clone(),
    };
    let subscriber = Registry::default().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let target: PeerNetwork<TestId> =
                PeerNetwork::start("peer-target", None).await.unwrap();
            let info = seed_entry(target.port(), "");

            let conn = tokio::time::timeout(
                Duration::from_secs(15),
                dial_peer("peer-target", &info, DialAttempt::Initial),
            )
            .await
            .expect("cert-less dial must not consume the QUIC race budget")
            .expect("cert-less dial must still connect over WSS");
            assert!(
                matches!(conn, PeerConnection::Wss(_)),
                "without a cert the dial must skip QUIC and connect WSS"
            );
        })
        .await;

    let no_cert_warns: Vec<CapturedEvent> = records
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.message.contains("no valid cert for peer"))
        .cloned()
        .collect();
    assert_eq!(
        no_cert_warns.len(),
        1,
        "exactly one no-valid-cert WARN for the one initial dial"
    );
    let warn = &no_cert_warns[0];
    assert_eq!(warn.level, tracing::Level::WARN);
    assert!(
        warn.fields.contains("reasons=") && warn.fields.contains("carries no certificate"),
        "the WARN's reasons field must name the absent cert (pre-fix it was empty): {}",
        warn.fields
    );
}
