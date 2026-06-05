//! The secondary's `Destination::All` egress also reaches a co-located
//! primary's inbound (CH2) — the symmetric broadcast-loopback leg.
//!
//! On a `--source-already-staged` SLURM run the SAME node that becomes the
//! co-located primary is the designated discoverer. It runs discovery and
//! originates a `PhaseDepsSet + N×TaskAdded` batch as `Destination::All`,
//! plus periodic `Secondary` keepalives as `Destination::All`. The
//! transport's `broadcast` fans these to REMOTE peers only — a node has no
//! self-connection — so a self-originated broadcast never reaches a
//! co-located `PrimaryCoordinator` sharing the host through the normal
//! mesh-member receipt path. The co-located primary's inbound (CH2) is fed
//! ONLY by the secondary's ingress demux of frames RECEIVED from remote
//! peers, which does not loop the secondary's own outbound broadcasts back.
//!
//! Pre-fix the 229 discovered tasks therefore never reached the co-located
//! primary's separate `cluster_state`: it stayed empty, `run_complete_check`
//! tripped instantly (the `0+0 >= 0` counter exit, `setup_pending` already
//! flipped false by the empty ledger on the legacy path / never armed), and
//! the run reported `total = 0` — the silent zero-run. The self keepalive
//! likewise never reached the co-located primary, which could falsely
//! declare its own co-located secondary dead.
//!
//! The fix adds the missing egress leg at the secondary's `send_to`
//! `SendTarget::Broadcast` arm — the SINGLE general home for every
//! `Destination::All` frame — the exact symmetric counterpart of the
//! primary's existing broadcast-loopback leg. These tests drive that arm
//! directly and assert EVERY self-originated `Destination::All` frame
//! (TaskAdded batch + Keepalive) lands on CH2, and that the carried batch
//! seeds a `cluster_state` to a non-zero `task_count` (so the primary's
//! counter exit no longer false-fires) while the carried keepalive is the
//! exact `Secondary`/self frame the primary credits via
//! `record_keepalive(sender_id)`.

#![cfg(test)]

use crate::cluster_state::ClusterState;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, KeepaliveRole, MessageType,
};
use dynrunner_scheduler::ResourceStealingScheduler;
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;

use super::super::test_helpers::{
    FixedEstimator, TestId, election_config, make_secondary, seed_member, set_current_primary,
};
use super::super::{SecondaryConfig, SecondaryCoordinator};
use super::processing::make_binary;

/// A `Destination::All` `ClusterMutation` batch shaped exactly like the
/// setup-discovery ingest (`PhaseDepsSet + N×TaskAdded`).
fn discovery_batch(n: usize) -> DistributedMessage<TestId> {
    let mut mutations: Vec<ClusterMutation<TestId>> = Vec::with_capacity(n + 1);
    let mut deps = std::collections::HashMap::new();
    deps.insert(dynrunner_core::PhaseId::from("default"), vec![]);
    mutations.push(ClusterMutation::PhaseDepsSet { deps });
    for i in 0..n {
        let task = make_binary(&format!("item-{i}"), 1);
        mutations.push(ClusterMutation::TaskAdded {
            hash: dynrunner_core::compute_task_hash(&task),
            task,
        });
    }
    DistributedMessage::ClusterMutation {
        sender_id: "sec-0".into(),
        timestamp: 0.0,
        mutations,
    }
}

/// A `Secondary`-role keepalive shaped exactly like `send_keepalive`'s
/// frame for `sec-0`.
fn self_keepalive() -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        sender_id: "sec-0".into(),
        timestamp: 0.0,
        secondary_id: "sec-0".into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

/// Build a co-located-authority secondary: `sec-0` is the sole alive
/// `can_be_primary` non-observer member AND the recognized
/// `current_primary` (so `current_primary() == self`), with a registered
/// co-located primary inbound (CH2). Returns the coordinator + the CH2
/// receiver the co-located `PrimaryCoordinator`'s `recv_peer` drains.
#[allow(clippy::type_complexity)]
fn colocated_authority(
    config: SecondaryConfig,
) -> (
    SecondaryCoordinator<
        super::super::test_helpers::TestTransport<super::super::test_helpers::NoPeers>,
        dynrunner_transport_channel::ChannelManagerEnd,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) {
    let mut secondary = make_secondary(config);
    seed_member(&mut secondary, "sec-0", true, false);
    set_current_primary(&mut secondary, "sec-0");
    let (ch2_tx, ch2_rx) = tokio_mpsc::unbounded_channel();
    secondary.register_colocated_primary_inbound(ch2_tx);
    (secondary, ch2_rx)
}

/// PRE-FIX: the co-located primary's CH2 inbound stays empty — the
/// self-originated `Destination::All` batch reaches REMOTE peers only and
/// the primary's `cluster_state` is never seeded (`task_count == 0` →
/// `run_complete_check` false-fires → `total = 0`).
/// POST-FIX: the egress leg loops the SAME frame into CH2; applying it to a
/// fresh `cluster_state` (the exact type + apply path the primary uses)
/// yields `task_count == N`, so the primary's counter exit no longer trips
/// at entry.
#[tokio::test(flavor = "current_thread")]
async fn self_originated_taskadded_batch_reaches_colocated_primary_inbound() {
    let _ = tracing_subscriber::fmt::try_init();
    let mut config = election_config("sec-0");
    config.keepalive_interval = Duration::from_secs(60);
    let (mut secondary, mut ch2_rx) = colocated_authority(config);

    // The exact egress the designated-discoverer originates after running
    // discovery: a `PhaseDepsSet + 2×TaskAdded` batch as `Destination::All`.
    secondary
        .send_to(Destination::All, discovery_batch(2))
        .await
        .expect("Destination::All send must not error");

    // POST-FIX: the batch is on CH2 (the co-located primary's inbound).
    let looped = ch2_rx
        .try_recv()
        .expect("self-originated Destination::All TaskAdded batch must reach the co-located primary inbound (CH2)");
    assert_eq!(
        looped.msg_type(),
        MessageType::ClusterMutation,
        "the looped-back frame must be the ClusterMutation batch verbatim",
    );

    // Faithful downstream effect: apply the looped batch through the SAME
    // `ClusterState::apply` the co-located primary's `handle_cluster_mutation`
    // runs. The primary's `cluster_state` reflects the tasks — `task_count`
    // is N (not 0), so `setup_pending` (`required_setup_on_promote &&
    // task_count == 0`) is false and `run_complete_check`'s `0+0 >= 0`
    // counter exit no longer false-fires at entry.
    let mut primary_cluster_state: ClusterState<TestId> = ClusterState::new();
    let DistributedMessage::ClusterMutation { mutations, .. } = looped else {
        panic!("expected ClusterMutation");
    };
    for m in mutations {
        primary_cluster_state.apply(m);
    }
    assert_eq!(
        primary_cluster_state.task_count(),
        2,
        "the co-located primary's cluster_state must reflect the discovered tasks (total = N, not 0)",
    );
    assert!(
        !primary_cluster_state.run_complete(),
        "the seeded ledger must not be flagged run-complete at entry",
    );

    // The leg is general (not TaskAdded-special-cased): the SAME arm also
    // loops the `Secondary` keepalive `send_keepalive` originates as
    // `Destination::All`. The primary's `dispatch_message` credits it via
    // `record_keepalive(sender_id())` into `secondary_keepalives[self]`.
    secondary
        .send_to(Destination::All, self_keepalive())
        .await
        .expect("Destination::All keepalive send must not error");
    let looped_ka = ch2_rx
        .try_recv()
        .expect("self-originated Destination::All keepalive must also reach the co-located primary inbound (CH2)");
    match looped_ka {
        DistributedMessage::Keepalive {
            secondary_id,
            emitter_role,
            ..
        } => {
            assert_eq!(
                emitter_role,
                KeepaliveRole::Secondary,
                "the looped keepalive must be the Secondary-role frame the primary credits",
            );
            assert_eq!(
                secondary_id, "sec-0",
                "the keepalive must credit secondary_keepalives[self] — sender is the co-located self",
            );
        }
        other => panic!("expected a looped-back Keepalive, got {other:?}"),
    }

    // Exactly the two self-originated frames were looped — no duplication.
    assert!(
        ch2_rx.try_recv().is_err(),
        "only the two self-originated Destination::All frames must reach CH2",
    );
}

/// The leg is correctly GATED: a node that is NOT the current primary must
/// not loopback its `Destination::All` frames to a (stale) co-located
/// primary inbound, even when one is registered. This guards against an
/// over-broad leg that would double-deliver on a demoted / non-authority
/// node whose co-located primary is gone.
#[tokio::test(flavor = "current_thread")]
async fn destination_all_not_looped_when_not_current_primary() {
    let _ = tracing_subscriber::fmt::try_init();
    let mut config = election_config("sec-0");
    config.keepalive_interval = Duration::from_secs(60);
    let mut secondary = make_secondary(config);

    // Register a co-located primary inbound but DO NOT make `sec-0` the
    // current primary — a remote node holds authority.
    seed_member(&mut secondary, "sec-0", true, false);
    seed_member(&mut secondary, "primary", true, false);
    set_current_primary(&mut secondary, "primary");
    let (ch2_tx, mut ch2_rx) = tokio_mpsc::unbounded_channel();
    secondary.register_colocated_primary_inbound(ch2_tx);

    secondary
        .send_to(Destination::All, self_keepalive())
        .await
        .expect("Destination::All send must not error");

    assert!(
        ch2_rx.try_recv().is_err(),
        "a non-current-primary node must NOT loopback Destination::All frames to a co-located \
         primary inbound (the loopback leg is gated on current_primary() == self)",
    );
}

/// No co-located primary composed (`colocated_primary_inbound_tx` is
/// `None`, every non-pyo3 path): the leg is inert and the broadcast goes
/// out the transport unchanged — no panic, no loopback. This is the default
/// production secondary shape.
#[tokio::test(flavor = "current_thread")]
async fn destination_all_inert_without_colocated_primary() {
    let _ = tracing_subscriber::fmt::try_init();
    let mut config = election_config("sec-0");
    config.keepalive_interval = Duration::from_secs(60);
    let mut secondary = make_secondary(config);
    seed_member(&mut secondary, "sec-0", true, false);
    set_current_primary(&mut secondary, "sec-0");
    // No `register_colocated_primary_inbound` — the leg must be inert.

    secondary
        .send_to(Destination::All, self_keepalive())
        .await
        .expect("Destination::All send must not error without a co-located primary");
}
