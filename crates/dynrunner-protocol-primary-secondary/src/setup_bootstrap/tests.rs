//! Sanity tests for the lossless `SetupBootstrapMessage` <->
//! `DistributedMessage` round-trip and the structural invariant that
//! non-setup variants fail the `TryFrom` boundary.

use super::*;
use crate::messages::{DistributedMessage, KeepaliveRole, PeerConnectionInfo};

// Sanity that the bidirectional conversion is lossless and that
// any non-setup variant fails the TryFrom — this is the
// structural invariant the whole module rests on.

#[test]
fn welcome_roundtrip() {
    let original: SetupBootstrapMessage = SetupBootstrapMessage::SecondaryWelcome {
        sender_id: "s0".into(),
        timestamp: 1.0,
        secondary_id: "s0".into(),
        resources: Vec::new(),
        worker_count: 2,
        hostname: "host".into(),
        is_observer: false,
    };
    let wire: DistributedMessage<()> = original.clone().into();
    let back: SetupBootstrapMessage = wire.try_into().expect("setup variant");
    match (original, back) {
        (
            SetupBootstrapMessage::SecondaryWelcome {
                secondary_id: a,
                worker_count: aw,
                is_observer: ai,
                ..
            },
            SetupBootstrapMessage::SecondaryWelcome {
                secondary_id: b,
                worker_count: bw,
                is_observer: bi,
                ..
            },
        ) => {
            assert_eq!(a, b);
            assert_eq!(aw, bw);
            assert_eq!(ai, bi);
        }
        _ => panic!("variant changed across roundtrip"),
    }
}

#[test]
fn cert_exchange_roundtrip() {
    let original: SetupBootstrapMessage = SetupBootstrapMessage::CertExchange {
        sender_id: "s0".into(),
        timestamp: 2.0,
        secondary_id: "s0".into(),
        public_cert_pem: "PEM".into(),
        ipv4_address: Some("10.0.0.1".into()),
        ipv6_address: None,
        quic_port: 4242,
    };
    let wire: DistributedMessage<()> = original.into();
    match wire {
        DistributedMessage::CertExchange {
            public_cert_pem,
            ipv4_address,
            ipv6_address,
            quic_port,
            ..
        } => {
            assert_eq!(public_cert_pem, "PEM");
            assert_eq!(ipv4_address.as_deref(), Some("10.0.0.1"));
            assert!(ipv6_address.is_none());
            assert_eq!(quic_port, 4242);
        }
        _ => panic!("converted to wrong wire variant"),
    }
}

#[test]
fn peer_info_roundtrip() {
    let original: SetupBootstrapMessage = SetupBootstrapMessage::PeerInfo {
        sender_id: "primary".into(),
        timestamp: 3.0,
        peers: vec![PeerConnectionInfo {
            secondary_id: "s0".into(),
            cert: "PEM".into(),
            ipv4: None,
            ipv6: None,
            port: 0,
            is_observer: false,
        }],
    };
    let wire: DistributedMessage<()> = original.into();
    let back: SetupBootstrapMessage = wire.try_into().expect("setup variant");
    match back {
        SetupBootstrapMessage::PeerInfo { peers, .. } => {
            assert_eq!(peers.len(), 1);
            assert_eq!(peers[0].secondary_id, "s0");
        }
        _ => panic!("variant changed across roundtrip"),
    }
}

#[test]
fn non_setup_variant_rejected() {
    // Any non-setup variant must Err out at the TryFrom boundary.
    // Pick `Keepalive` — the canonical runtime frame and the one
    // most likely to race a setup-phase recv in practice.
    let runtime: DistributedMessage<()> = DistributedMessage::Keepalive {
        sender_id: "s0".into(),
        timestamp: 4.0,
        secondary_id: "s0".into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    };
    let result: Result<SetupBootstrapMessage, DistributedMessage<()>> = runtime.try_into();
    assert!(
        result.is_err(),
        "Keepalive must not convert to SetupBootstrapMessage"
    );
}
