//! #420 face (c) — re-election from the FORMED-MESH setup phase.
//!
//! PRODUCTION REPLAY (asm-dataset LMU mesh-always, ~14:29): the promoted
//! primary hit a fatal task-graph error during bring-up and never aborted
//! the run (face (a)). The 11 secondaries sat in `wait_for_setup` with a
//! FULLY FORMED mesh (peer lists received, dial sweeps run) while the primary
//! was unreachable — and NOBODY elected a replacement. Each died one-by-one
//! at the 600s unconfigured deadline ("setup deadline elapsed despite peers
//! reachable — primary unresponsive").
//!
//! THE FIX: once a setup-phase secondary's primary-silence accumulates past
//! HALF the unconfigured deadline with a FORMED mesh, it ARMS the SAME
//! failover election the operational loop runs (`run_election_tick` +
//! `fire_local_promotion`), via a transient `setup_election` holder whose
//! quorum `peer_keepalives` denominator is SEEDED from the replicated
//! membership (the bootstrap-evidence pattern). A survivor promotes; the rest
//! defer + receive `PrimaryChanged` + continue setup against the new primary.
//! The election runs WITHOUT leaving the setup wait (only the winner promotes
//! out, via the PromotionSignal), so the losers stay where the elected
//! primary's re-sent setup trio completes them.
//!
//! Pins (driving the setup-election driver methods directly — the same
//! methods `wait_for_setup`'s select arm calls):
//!   * SILENT primary + formed mesh + silence ≥ threshold → election ARMS
//!     (membership-seeded quorum) and the lex-lowest survivor reaches
//!     `Promoted` + fires the `PromotionSignal`;
//!   * SLOW-but-live primary (silence < threshold) → NO election arms (the
//!     negative case — a setup-phase election must not fight a live primary);
//!   * never-meshed (degraded) secondary → NO election arms (split-brain
//!     guard: a lone never-meshed node must die, not self-promote).

#![cfg(test)]

use std::time::Duration;

use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::{ClusterMutation, PrimaryChangeReason};

use super::super::election::ElectionState;
use super::super::test_helpers::{
    MembershipControlPeer, SecondaryHarness, election_config, make_secondary_membership,
};

const PRIMARY_ID: &str = "secondary-0"; // the silent (dead-during-bring-up) primary
const SELF_ID: &str = "secondary-1"; // THIS node — the lex-lowest survivor
const PEER_2: &str = "secondary-2";
const PEER_3: &str = "secondary-3";

const KEEPALIVE: Duration = Duration::from_millis(50);

/// One advertised-memory `ResourceAmount` vec (the live welcome shape).
fn mem(bytes: u64) -> Vec<dynrunner_core::ResourceAmount> {
    vec![dynrunner_core::ResourceAmount {
        kind: dynrunner_core::ResourceKind::memory(),
        amount: bytes,
    }]
}

/// Seed an alive worker-secondary into this node's replicated `cluster_state`
/// — a `PeerJoined` + a `SecondaryCapacity { worker_count > 0 }` — so it
/// appears in `alive_secondary_members()` (the setup-election quorum seed
/// source).
fn seed_member(sec: &mut SecondaryHarness<MembershipControlPeer>, id: &str) {
    sec.cluster_state.apply(ClusterMutation::PeerJoined {
        peer_id: id.into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: Default::default(),
        member_gen: 0,
    });
    sec.cluster_state.apply(ClusterMutation::SecondaryCapacity {
        secondary: id.into(),
        worker_count: 2,
        resources: mem(8 * 1024 * 1024 * 1024),
    });
}

/// A `secondary-1` STILL IN SETUP (NOT operational), with a FORMED mesh of
/// {primary, peer-2, peer-3}, the replicated membership seeded for all three,
/// and `current_primary = secondary-0`. The primary then goes (and stays)
/// silent: it is a member of the transport mesh but emits nothing — exactly
/// the production shape (a primary that started bring-up, hit the fatal, and
/// went dark without departing). NO `enter_operational_for_test`: the
/// lifecycle stays in its setup variant throughout.
fn setup_secondary_formed_mesh() -> (
    SecondaryHarness<MembershipControlPeer>,
    std::rc::Rc<std::cell::RefCell<Vec<PeerId>>>,
) {
    let (mut sec, members) = make_secondary_membership(
        election_config(SELF_ID),
        vec![
            PeerId::from(PRIMARY_ID),
            PeerId::from(PEER_2),
            PeerId::from(PEER_3),
        ],
    );
    // The mesh DID form (this incident's mesh was fully formed) — NOT the
    // never-meshed split-brain case.
    sec.mesh.degraded = false;
    // The replicated membership the setup-election quorum seed reads.
    seed_member(&mut sec, PRIMARY_ID);
    seed_member(&mut sec, PEER_2);
    seed_member(&mut sec, PEER_3);
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: PRIMARY_ID.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    sec.publish_membership();
    (sec, members)
}

/// THE FIX: a setup-phase secondary whose primary has been silent past the
/// election threshold, with a formed mesh, ARMS the failover election
/// (membership-seeded), gathers quorum, self-leads (lex-lowest), collects
/// peer confirms, and reaches `Promoted` — firing the `PromotionSignal` so
/// the Node builds the replacement primary. Pre-fix NO election existed in
/// setup at all: the node would have died at the unconfigured deadline.
#[tokio::test(flavor = "current_thread")]
async fn setup_phase_silent_primary_arms_election_and_lowest_survivor_promotes() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _members) = setup_secondary_formed_mesh();

            // Precondition: NOT operational (still in setup), no election armed.
            assert!(
                sec.election_state().is_none(),
                "precondition: a setup secondary holds no election before arming"
            );

            // Silence past the threshold (half the 600s unconfigured deadline
            // = 300s) with a formed mesh ⇒ ARM. The membership seed yields the
            // two non-primary live members (peer-2, peer-3) as the quorum
            // denominator.
            sec.maybe_arm_setup_election(Duration::from_secs(300));
            assert!(
                sec.setup_election.is_some(),
                "primary-silence past the threshold with a formed mesh must ARM \
                 a setup-phase election"
            );
            assert_eq!(
                sec.live_peer_ids()
                    .cloned()
                    .collect::<std::collections::HashSet<_>>(),
                std::collections::HashSet::from([PEER_2.to_string(), PEER_3.to_string()]),
                "the membership-seeded quorum denominator is the two non-primary \
                 live members (the current primary is role-excluded)"
            );

            // Tick 1: the silence legs ((B) backstop — primary_last_seen
            // backdated to now-300s) arm Suspecting + broadcast a TimeoutQuery.
            sec.drive_setup_election_tick().await;
            assert!(
                matches!(sec.election_state(), Some(ElectionState::Suspecting { .. })),
                "tick 1 must arm Suspecting; got {:?}",
                sec.election_state().map(std::mem::discriminant)
            );

            // The peers agree the primary is silent (last saw it well past the
            // death deadline).
            sec.record_timeout_response(PEER_2.into(), Some(10.0));
            sec.record_timeout_response(PEER_3.into(), Some(10.0));

            // Gather window, then tally: quorum met (self + 2 ≥
            // failover_quorum(2)=2), self is lex-lowest ⇒ self-lead → Candidate.
            tokio::time::sleep(KEEPALIVE + Duration::from_millis(5)).await;
            sec.drive_setup_election_tick().await;
            assert!(
                matches!(
                    sec.election_state(),
                    Some(ElectionState::Candidate { .. } | ElectionState::Promoted)
                ),
                "after quorum the lex-lowest survivor must campaign as Candidate; \
                 got {:?}",
                sec.election_state().map(std::mem::discriminant)
            );

            // Peer confirms cross quorum → Promoted + PromotionSignal fired.
            let _ = sec
                .handle_setup_election_frame(
                    dynrunner_protocol_primary_secondary::DistributedMessage::PromotionConfirm {
                        target: None,
                        sender_id: PEER_2.into(),
                        timestamp: 0.0,
                        new_primary_id: SELF_ID.into(),
                        vote_round: 1,
                    },
                )
                .await;
            sec.handle_setup_election_frame(
                dynrunner_protocol_primary_secondary::DistributedMessage::PromotionConfirm {
                    target: None,
                    sender_id: PEER_3.into(),
                    timestamp: 0.0,
                    new_primary_id: SELF_ID.into(),
                    vote_round: 1,
                },
            )
            .await;

            // The win named THIS node the primary in the replicated ledger
            // (fire_local_promotion's local apply) and fired the PromotionSignal
            // — the Node's promotion arm builds the replacement primary off it.
            assert_eq!(
                sec.cluster_state.current_primary(),
                Some(SELF_ID),
                "the setup-phase election winner must name itself the primary"
            );
            assert!(
                sec.promotion_rx.try_recv().is_ok(),
                "winning the setup-phase election must fire the PromotionSignal \
                 (→ the Node builds the snapshot-seeded replacement primary)"
            );
        })
        .await;
}

/// NEGATIVE: a SLOW-but-LIVE primary (silence BELOW the threshold — its
/// setup-liveness frames keep re-arming the deadline) must NOT arm a
/// setup-phase election. A premature election would fight a primary that is
/// merely assembling slowly (the asm-dataset LMU fleet-death class this whole
/// machinery exists to avoid recurring on the WRONG side).
#[tokio::test(flavor = "current_thread")]
async fn setup_phase_slow_live_primary_does_not_arm_election() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _members) = setup_secondary_formed_mesh();

            // Silence WELL BELOW the threshold (half of 600s = 300s): a
            // slow-but-live primary keeps re-arming the deadline, so `silent_for`
            // never reaches the threshold.
            sec.maybe_arm_setup_election(Duration::from_secs(15));
            assert!(
                sec.setup_election.is_none(),
                "a primary silent for LESS than the election threshold (a slow \
                 but live primary still re-arming the deadline) must NOT arm a \
                 setup-phase election — the election must not fight a live primary"
            );
            // A drive tick is a no-op with no election armed.
            sec.drive_setup_election_tick().await;
            assert!(sec.election_state().is_none());
        })
        .await;
}

/// NEGATIVE: a NEVER-MESHED (degraded) setup secondary must NOT arm a
/// setup-phase election even past the silence threshold — a lone node with no
/// peers to gather quorum from must die at the deadline, not self-promote
/// (the split-brain guard, mirroring the operational `mesh_degraded` bail).
#[tokio::test(flavor = "current_thread")]
async fn setup_phase_never_meshed_does_not_arm_election() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _members) = setup_secondary_formed_mesh();
            // The mesh NEVER formed (zero peers ever meshed).
            sec.mesh.degraded = true;

            sec.maybe_arm_setup_election(Duration::from_secs(300));
            assert!(
                sec.setup_election.is_none(),
                "a never-meshed (degraded) secondary must NOT arm a setup-phase \
                 election — it has no peers to elect with; it must die at the \
                 deadline, never self-promote (split-brain safety)"
            );
        })
        .await;
}

/// #423 NEGATIVE: under a lagged OWN tick (CPU starvation), the setup-phase
/// election arm must NOT arm even past the silence threshold. The
/// `silent_for` the arm reads is the setup-deadline-anchor age; when THIS
/// node's runtime froze, that age reflects OUR stall (we could not process
/// the primary's setup frames), not the primary's silence. `wait_for_setup`'s
/// select arm feeds the SAME shared `own_tick_health` authority once per tick
/// and gates `maybe_arm_setup_election` on `!starved` — exactly the sequence
/// this test replays. (Once the node recovers a healthy tick, a still-silent
/// primary arms on the next on-cadence tick — covered by the headline arm
/// test above.)
#[tokio::test(flavor = "current_thread")]
async fn setup_phase_lagged_own_tick_defers_arm() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _members) = setup_secondary_formed_mesh();

            // Replay the select arm: observe the tick (a 10s inter-tick gap ≫
            // the 150ms = 3× 50ms-cadence starvation threshold), then gate the
            // arm on the verdict — the precise `wait_for_setup` sequence.
            let now = std::time::Instant::now();
            assert!(!sec
                .own_tick_health
                .observe_tick(now - Duration::from_secs(10)));
            let starved = sec.own_tick_health.observe_tick(now);
            assert!(starved, "a 10s inter-tick gap must be judged starved");

            if !starved {
                sec.maybe_arm_setup_election(Duration::from_secs(300));
            }
            assert!(
                sec.setup_election.is_none(),
                "a lagged own tick must DEFER the setup-phase election arm — the \
                 measured primary silence reflects OUR stall, not the primary's"
            );
        })
        .await;
}

/// LOSER CONTRACT: a setup-phase candidate that observes a PEER win the
/// election (a `PrimaryChanged` naming the peer) must DROP its transient
/// `setup_election` holder and STAY in setup (it never went operational), so
/// the new primary's re-sent setup trio completes its handshake. Pins
/// `reset_election_to_normal`'s setup branch (the holder is cleared, not left
/// dangling, and the lifecycle stays its setup variant).
#[tokio::test(flavor = "current_thread")]
async fn setup_phase_loser_drops_election_and_stays_in_setup() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _members) = setup_secondary_formed_mesh();
            sec.maybe_arm_setup_election(Duration::from_secs(300));
            assert!(sec.setup_election.is_some(), "armed for the test");

            // A PEER wins and broadcasts its `PrimaryChanged` — applied through
            // the same hook `wait_for_setup`'s ClusterMutation arm uses.
            sec.apply_cluster_mutations(vec![ClusterMutation::PrimaryChanged {
                new: PEER_2.into(),
                epoch: 2,
                reason: PrimaryChangeReason::Election,
            }]);

            assert!(
                sec.setup_election.is_none(),
                "a peer-won PrimaryChanged must DROP this loser's transient \
                 setup-election holder (the loser contract)"
            );
            assert_eq!(
                sec.cluster_state.current_primary(),
                Some(PEER_2),
                "the loser re-points current_primary onto the winner and \
                 continues its setup wait against it"
            );
            // The PromotionSignal must NOT have fired for this node (it lost).
            assert!(
                sec.promotion_rx.try_recv().is_err(),
                "a setup-phase LOSER must not fire its own PromotionSignal"
            );
        })
        .await;
}
