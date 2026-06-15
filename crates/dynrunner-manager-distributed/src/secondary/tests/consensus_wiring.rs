//! #556 secondary-side mesh-consensus WIRE-UP tests (Layer 4).
//!
//! The FSM-level state-transition coverage lives in
//! `crate::secondary::consensus::tests`; this file covers the COORDINATOR
//! wiring — that the dispatch arms route inbound frames into the FSM and
//! that the FSM's emitted frames leave the egress correctly stamped +
//! addressed.

#![cfg(test)]

use super::super::test_helpers::{election_config, make_secondary_recording};
use dynrunner_protocol_primary_secondary::DistributedMessage;

/// An inbound `PeerProbe` whose `probed_id` matches this secondary's own
/// id is answered with a `PeerProbeAck` to the prober. The ack carries
/// the same `consensus_id` and names the prober (the wire contract).
#[tokio::test(flavor = "current_thread")]
async fn inbound_peer_probe_emits_matching_probe_ack() {
    let (mut sec, log) = make_secondary_recording(election_config("sec-0"), 1);
    sec.enter_operational_for_test();
    sec.set_bootstrap_primary_id("setup".to_string());

    sec.handle_consensus_probe("peer-0", 42, "sec-0").await;
    sec.drain_egress().await;

    let recorded = log.borrow();
    let acks: Vec<_> = recorded
        .iter()
        .filter_map(|m| match m {
            DistributedMessage::PeerProbeAck {
                consensus_id,
                prober_id,
                ..
            } => Some((*consensus_id, prober_id.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        acks.len(),
        1,
        "exactly one PeerProbeAck must be emitted for a matching probe; got {recorded:?}"
    );
    assert_eq!(acks[0].0, 42, "consensus_id must echo the inbound probe");
    assert_eq!(
        acks[0].1, "peer-0",
        "prober_id must name the original prober"
    );
}

/// An inbound `PeerProbe` whose `probed_id` does NOT match this
/// secondary's own id is dropped silently — the FSM's stateless
/// addressee filter (`apply_probe_request`). No ack is emitted.
#[tokio::test(flavor = "current_thread")]
async fn inbound_peer_probe_addressed_to_other_is_dropped() {
    let (mut sec, log) = make_secondary_recording(election_config("sec-0"), 1);
    sec.enter_operational_for_test();
    sec.set_bootstrap_primary_id("setup".to_string());

    sec.handle_consensus_probe("peer-0", 42, "some-other-peer")
        .await;
    sec.drain_egress().await;

    let recorded = log.borrow();
    let ack_count = recorded
        .iter()
        .filter(|m| matches!(m, DistributedMessage::PeerProbeAck { .. }))
        .count();
    assert_eq!(
        ack_count, 0,
        "misrouted probe (probed_id != self) must NOT emit any ack; got {recorded:?}"
    );
}
