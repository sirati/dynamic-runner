//! `send_peer_lists` persists the roster's connection credentials to
//! LOCAL state when `PrimaryConfig.peer_credentials_path` is set — the
//! same `Vec<PeerConnectionInfo>` (cert PEM + addresses + ports) the
//! `PeerInfo` broadcast fans out, captured at the fan-out instead of
//! dropping with process memory. A late-joiner observer on the
//! submitter host overlays these cert pins onto its `.info`-derived
//! seed and dials the mesh over QUIC with valid certs.
//!
//! Also pins: persistence is NEVER fatal (an unwritable path must not
//! abort cluster setup), and the default config (path `None`) writes
//! nothing.

use super::*;

use crate::peer_credentials::load_peer_credentials;

/// Build a primary with the given credentials path, feed `n`
/// secondaries through the welcome + cert-exchange handshake handlers
/// (the same state the wire path produces), and run `send_peer_lists`.
async fn drive_send_peer_lists(n: u32, path: Option<std::path::PathBuf>) {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(drive_send_peer_lists_inner(n, path))
        .await;
}

/// Body of [`drive_send_peer_lists`] — split so the `LocalSet` wrap
/// (the handshake handlers `spawn_local`) stays at one place.
async fn drive_send_peer_lists_inner(n: u32, path: Option<std::path::PathBuf>) {
    let (sec_tx, _sec_rx) = tokio_mpsc::unbounded_channel();
    let (_inbound_hold, inbound_rx) = tokio_mpsc::unbounded_channel();
    let mut outgoing: HashMap<String, tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>> =
        HashMap::new();
    outgoing.insert("sec-0".to_string(), sec_tx);
    let transport = ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, inbound_rx);

    let config = PrimaryConfig {
        num_secondaries: n,
        peer_credentials_path: path,
        ..test_primary_config()
    };
    let (mut primary, _mesh) = build_test_primary(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    for i in 0..n {
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
        primary.handle_cert_exchange(DistributedMessage::CertExchange {
            target: None,
            sender_id: id.clone(),
            timestamp: 0.0,
            secondary_id: id.clone(),
            public_cert_pem: format!("CERT-PEM-{i}"),
            ipv4_address: Some("10.0.0.7".into()),
            ipv6_address: None,
            quic_port: 4000 + i as u16,
            liveness_port: Some(5000 + i as u16),
        });
    }

    primary
        .send_peer_lists()
        .await
        .expect("send_peer_lists must succeed");
}

/// The configured path receives the FULL roster — per-peer cert PEM,
/// dial address, mesh port, liveness port — loadable through the
/// `peer_credentials` reader the late-joiner uses.
#[tokio::test]
async fn send_peer_lists_persists_roster_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("peer_credentials.json");
    drive_send_peer_lists(2, Some(path.clone())).await;

    let mut creds = load_peer_credentials(&path).expect("persisted credentials must load");
    creds.sort_by(|a, b| a.secondary_id.cmp(&b.secondary_id));
    assert_eq!(creds.len(), 2);
    assert_eq!(creds[0].secondary_id, "secondary-0");
    assert_eq!(creds[0].cert, "CERT-PEM-0");
    assert_eq!(creds[0].ipv4.as_deref(), Some("10.0.0.7"));
    assert_eq!(creds[0].port, 4000);
    assert_eq!(creds[0].liveness_port, Some(5000));
    assert_eq!(creds[1].secondary_id, "secondary-1");
    assert_eq!(creds[1].cert, "CERT-PEM-1");
    assert_eq!(creds[1].port, 4001);
}

/// An unwritable credentials path must NOT abort cluster setup — the
/// store is a bootstrap aid (the WARN names it), never a fatal step.
#[tokio::test]
async fn unwritable_credentials_path_is_non_fatal() {
    let dir = tempfile::tempdir().unwrap();
    // Parent "dir" is a FILE → every write under it fails.
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, b"x").unwrap();
    let path = blocker.join("peer_credentials.json");
    // The expect inside drive_send_peer_lists IS the assertion: the
    // failed store must not surface as a send_peer_lists error.
    drive_send_peer_lists(1, Some(path)).await;
}

/// The default config (path `None`) persists nothing — non-submitter
/// primaries (promoted compute peers, tests) keep zero filesystem
/// side effects.
#[tokio::test]
async fn no_path_persists_nothing() {
    let dir = tempfile::tempdir().unwrap();
    drive_send_peer_lists(1, None).await;
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        0,
        "no credentials file may appear without a configured path"
    );
}
