//! The "all secondaries connected" bring-up milestone
//! (`wait_for_connections`) must emit on the IMPORTANT target and
//! DISCRIMINATE the two loop-exit paths an operator watching
//! `--important-stdio-only` has to tell apart:
//!
//! 1. FULL fleet — every requested secondary welcomed + cert-exchanged.
//!    The line reports the full `k/n` count.
//! 2. QUORUM proceed — the straggler window expired with `k<n`; the
//!    missing secondaries are dropped and the run continues at reduced
//!    parallelism. The line reports `k/n` AND says it is proceeding at
//!    quorum, so the operator is not left guessing whether the fleet is
//!    whole.
//!
//! Owner spec (#418): "it must log when all secondaries have connected to
//! it" — and the quorum-proceed variant must be distinguishable from a
//! full connect.
//!
//! REVERT-CHECK: collapse the two arms back to the single
//! `"all secondaries connected"` line (pre-#418 shape) and the quorum
//! assertion below fails RED — the operator can no longer tell a partial
//! fleet from a whole one.

use super::*;

use crate::test_capture::{IMPORTANT_TARGET, TargetCapture};
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

/// Drive `wait_for_connections` to completion with `welcomed` secondaries
/// fully cert-exchanged out of `requested` expected, returning the
/// IMPORTANT-target lines the wait emitted. A `welcomed < requested` run
/// exercises the quorum-proceed timeout arm (short `connect_timeout`); a
/// `welcomed == requested` run breaks immediately on the full-fleet check.
async fn connect_and_capture(requested: u32, welcomed: u32) -> Vec<String> {
    // One outbox so the assembly beacon's broadcast has a live receiver;
    // the inbound is held open but never fed (we populate `self.secondaries`
    // directly via the handshake handlers below, not over the wire).
    let (sec_tx, _sec_rx) = tokio_mpsc::unbounded_channel();
    let (_inbound_hold, inbound_rx) = tokio_mpsc::unbounded_channel();
    let mut outgoing: HashMap<String, tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>> =
        HashMap::new();
    outgoing.insert("sec-0".to_string(), sec_tx);
    let transport = ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, inbound_rx);

    let config = PrimaryConfig {
        num_secondaries: requested,
        // A short window so the quorum-proceed arm fires promptly under
        // the paused clock; the full-fleet path never reaches it.
        connect_timeout: Duration::from_secs(30),
        ..test_primary_config()
    };
    let (mut primary, _mesh) = build_test_primary(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    // Bring `welcomed` secondaries to cert-exchanged by feeding the two
    // handshake frames directly — `is_at_least_cert_exchanged` then counts
    // them, exactly as a wire-delivered handshake would.
    for i in 0..welcomed {
        let id = format!("secondary-{i}");
        primary
            .handle_welcome(DistributedMessage::SecondaryWelcome {
                target: None,
                sender_id: id.clone(),
                timestamp: 0.0,
                secondary_id: id.clone(),
                resources: vec![dynrunner_core::ResourceAmount {
                    kind: dynrunner_core::ResourceKind::memory(),
                    amount: 1024 * 1024 * 1024,
                }],
                worker_count: 1,
                hostname: "test".into(),
                is_observer: false,
                can_be_primary: true,
            })
            .await;
        primary
            .handle_cert_exchange(DistributedMessage::CertExchange {
                target: None,
                sender_id: id.clone(),
                timestamp: 0.0,
                secondary_id: id.clone(),
                public_cert_pem: "cert".into(),
                ipv4_address: Some("127.0.0.1".into()),
                ipv6_address: None,
                quic_port: 4000 + i as u16,
                liveness_port: Some(5000 + i as u16),
            })
            .await;
    }

    let capture = TargetCapture::for_target(IMPORTANT_TARGET);
    let subscriber = Registry::default().with(capture.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let mut no_commands = None;
    primary
        .wait_for_connections(&mut no_commands)
        .await
        .expect("a >=1-welcome connect must resolve Ok");

    capture
        .events()
        .into_iter()
        .map(|e| e.event.message)
        .collect()
}

/// FULL fleet: all requested secondaries welcomed → exactly one IMPORTANT
/// milestone reporting the full `n/n` count, NOT the quorum phrasing.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn full_fleet_connect_emits_n_of_n_milestone() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let msgs = connect_and_capture(3, 3).await;
            let milestone: Vec<&String> = msgs
                .iter()
                .filter(|m| m.contains("secondaries connected") || m.contains("connected"))
                .collect();
            assert!(
                milestone
                    .iter()
                    .any(|m| m.contains("all secondaries connected") && m.contains("3/3")),
                "the full-fleet bring-up milestone must report n/n on the \
                 IMPORTANT target; saw: {msgs:?}"
            );
            assert!(
                !milestone.iter().any(|m| m.contains("quorum")),
                "a full fleet must NOT claim a quorum proceed: {msgs:?}"
            );
        })
        .await;
}

/// QUORUM proceed: fewer welcomed than requested → the window expires, the
/// missing secondaries are dropped, and the IMPORTANT milestone reports
/// `k/n` AND that it is proceeding at quorum (the operator-distinguishing
/// signal #418 mandates).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn quorum_proceed_emits_k_of_n_with_quorum_phrasing() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let msgs = connect_and_capture(3, 2).await;
            assert!(
                msgs.iter()
                    .any(|m| m.contains("quorum") && m.contains("2/3")),
                "the quorum-proceed bring-up milestone must report k/n AND \
                 name the quorum proceed on the IMPORTANT target; saw: {msgs:?}"
            );
        })
        .await;
}
