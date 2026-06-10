//! Link-state coherence over a relay-capable mesh (BUG 3.3,
//! run_20260610_221140): the no-route-for-sends reader and the
//! recovered-for-liveness reader must share ONE state owner — the
//! transport's deliverability (`has_route`) — so a direct-wire-only
//! outage with a live relay path can neither drop primary-bound frames
//! at the egress nor arm the death-evidence legs, while a GENUINELY
//! unroutable primary still arms both.
//!
//! The production signature these pin: sec-2's direct wire to the
//! primary died while relays kept delivering INBOUND primary frames;
//! the old `has_peer` egress gate no-routed every OUTBOUND send (each
//! arming the failover-health probe) and the old `is_mesh_member`
//! death-evidence read declared the primary departed — so the node
//! flip-flopped "primary death suspected → election recovered: primary
//! message resumed" for the whole outage, and its TaskRequests /
//! terminal reports never even reached the (reachable) primary.

#![cfg(test)]

use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, KeepaliveRole, PrimaryChangeReason,
};

use super::super::test_helpers::{election_config, make_secondary_membership_relay};

/// The primary whose DIRECT wire dies.
const PRIMARY_ID: &str = "secondary-0";
/// A live peer — the relay path during the outage.
const PEER_1: &str = "secondary-1";
/// THIS node.
const SELF_ID: &str = "secondary-2";

fn probe_msg() -> DistributedMessage<super::super::test_helpers::TestId> {
    DistributedMessage::Keepalive {
        target: None,
        sender_id: SELF_ID.into(),
        timestamp: 0.0,
        secondary_id: SELF_ID.into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

/// Direct wire to the primary down, relay path alive: primary-bound
/// sends must FLOW (queued for the relay, no probe arming) and the
/// membership-departure death evidence must stay silent — the link has
/// ONE owner (deliverability), so "no-route for sends" can no longer
/// coexist with "recovered for liveness" (the flip-flop).
#[tokio::test(flavor = "current_thread")]
async fn direct_outage_with_live_relay_neither_no_routes_nor_reads_departed() {
    let (mut sec, connected, _unroutable) = make_secondary_membership_relay(
        election_config(SELF_ID),
        vec![PeerId::from(PRIMARY_ID), PeerId::from(PEER_1)],
    );
    sec.enter_operational_for_test();
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: PRIMARY_ID.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    sec.publish_membership();
    sec.record_primary_message();

    // The DIRECT wire dies; PEER_1 stays connected (the relay path).
    connected
        .borrow_mut()
        .retain(|m| m != &PeerId::from(PRIMARY_ID));
    sec.publish_membership();

    // Outbound: the send is QUEUED (the transport relays it), not
    // dropped at the egress; the failover-health probe stays un-armed.
    assert!(
        sec.send_to_primary(probe_msg()).await.is_ok(),
        "primary-bound send must flow during a direct-only outage"
    );
    assert!(
        !sec.op_mut().primary_link.is_link_failing(),
        "a relay-covered link must NOT anchor the failover-health \
         window (the old has_peer gate armed it on every send — the \
         flip-flop's 'death suspected' half)"
    );

    // Death evidence: a relay-reachable primary has NOT departed.
    assert!(
        !sec.primary_departed_membership(),
        "leg (C) must stay silent while the transport can still \
         deliver to the primary via relay"
    );
}

/// The same topology with the primary GENUINELY unroutable (every
/// forwarder blacklisted — the transport's post-bounce steady state):
/// the probe arms on the send and the membership-departure evidence
/// fires — honest fast failover is preserved.
#[tokio::test(flavor = "current_thread")]
async fn unroutable_primary_arms_probe_and_reads_departed() {
    let (mut sec, connected, unroutable) = make_secondary_membership_relay(
        election_config(SELF_ID),
        vec![PeerId::from(PRIMARY_ID), PeerId::from(PEER_1)],
    );
    sec.enter_operational_for_test();
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: PRIMARY_ID.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    sec.publish_membership();
    sec.record_primary_message();

    // The primary dies for real: direct gone AND relays have bounced
    // off every forwarder (the transport decided it unroutable).
    connected
        .borrow_mut()
        .retain(|m| m != &PeerId::from(PRIMARY_ID));
    unroutable.borrow_mut().push(PeerId::from(PRIMARY_ID));
    sec.publish_membership();

    // Outbound: absorbed no-route (failover-B) + the probe arms.
    assert!(
        sec.send_to_primary(probe_msg()).await.is_ok(),
        "no-route is absorbed into Ok (failover signal, not a run abort)"
    );
    assert!(
        sec.op_mut().primary_link.is_link_failing(),
        "a genuinely unroutable primary must anchor the failover-health \
         window on the first no-route send"
    );

    // Death evidence: no path by any route ⇒ departed.
    assert!(
        sec.primary_departed_membership(),
        "leg (C) must fire once the transport cannot deliver by ANY path"
    );
}
