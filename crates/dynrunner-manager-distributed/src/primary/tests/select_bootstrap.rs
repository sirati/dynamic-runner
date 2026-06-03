//! Unit tests for the bootstrap primary-selection policy
//! ([`PrimaryCoordinator::select_bootstrap_primary`]).
//!
//! The selector is a pure `&self` CRDT accessor: candidate =
//! `alive_secondary_members` (alive AND worker_count>0 ⇒ observers
//! excluded by capacity) ∩ `mesh_ready_secondaries` ∩
//! `transport.has_peer` − `role_table().observers`; result = the
//! deterministic `min()` by id, or `None` when the set is empty. These
//! tests seed each predicate via its real apply/record path
//! (`SecondaryCapacity` + `PeerJoined` mutations, `handle_mesh_ready`,
//! and the channel transport's membership-keyed `has_peer`) and assert
//! the policy bites at every boundary.

use super::*;
use dynrunner_protocol_primary_secondary::PeerId;

/// Build a submitter coordinator whose mesh transport confirms exactly
/// the secondaries `sec-0 .. sec-{confirmed_peers-1}` (the
/// `ChannelPeerTransport` keys `has_peer` off its `outgoing` membership,
/// which `setup_test` populates with those ids). Any id outside that
/// range is an unconfirmed peer (`has_peer == false`).
fn coordinator_with_confirmed_peers(
    confirmed_peers: u32,
) -> PrimaryCoordinator<ChannelPeerTransport<TestId>, ResourceStealingScheduler, FixedEstimator, TestId>
{
    let (transport, _ends) = setup_test(confirmed_peers);
    PrimaryCoordinator::new(
        PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: confirmed_peers.max(1),
            ..Default::default()
        },
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    )
}

/// Seed one secondary into the coordinator's replicated `cluster_state`:
/// `PeerJoined` (→ Alive, and into `observers` when `is_observer`) plus
/// `SecondaryCapacity` (→ the capacity record `alive_secondary_members`
/// reads). `worker_count == 0` is the structural observer shape.
fn seed_secondary(
    coordinator: &mut PrimaryCoordinator<
        ChannelPeerTransport<TestId>,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
    id: &str,
    worker_count: u32,
    is_observer: bool,
) {
    let state = coordinator.cluster_state_mut_for_test();
    let _ = state.apply(ClusterMutation::PeerJoined {
        peer_id: id.to_string(),
        is_observer,
        can_be_primary: false,
    });
    let _ = state.apply(ClusterMutation::SecondaryCapacity {
        secondary: id.to_string(),
        worker_count,
        resources: vec![],
    });
}

/// Record a secondary's `MeshReady` so it lands in
/// `mesh_ready_secondaries` (the real `handle_mesh_ready` path).
fn mark_mesh_ready(
    coordinator: &mut PrimaryCoordinator<
        ChannelPeerTransport<TestId>,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
    id: &str,
) {
    coordinator.handle_mesh_ready(DistributedMessage::MeshReady {
        sender_id: id.to_string(),
        timestamp: 0.0,
        secondary_id: id.to_string(),
        peer_count: 0,
    });
}

/// Three alive, non-observer, mesh-ready, confirmed worker-secondaries
/// (seeded out of id order) → the selector returns the deterministic
/// lowest id.
#[test]
fn returns_lowest_id_among_eligible() {
    let mut coordinator = coordinator_with_confirmed_peers(3);
    for id in ["sec-2", "sec-0", "sec-1"] {
        seed_secondary(&mut coordinator, id, 4, false);
        mark_mesh_ready(&mut coordinator, id);
    }

    assert_eq!(
        coordinator.select_bootstrap_primary(),
        Some(PeerId::from("sec-0")),
        "the deterministic lowest-id eligible peer must be chosen"
    );
}

/// No secondaries at all (degenerate single-node / submitter-only) →
/// `None`, so the caller stays primary.
#[test]
fn none_when_no_secondaries() {
    let coordinator = coordinator_with_confirmed_peers(0);
    assert_eq!(
        coordinator.select_bootstrap_primary(),
        None,
        "an empty fleet has no hand-off candidate"
    );
}

/// Every secondary is an observer (worker_count == 0, in the observer
/// set) → no non-observer candidate → `None`.
#[test]
fn none_when_all_observers() {
    let mut coordinator = coordinator_with_confirmed_peers(2);
    for id in ["sec-0", "sec-1"] {
        seed_secondary(&mut coordinator, id, 0, true);
        mark_mesh_ready(&mut coordinator, id);
    }

    assert_eq!(
        coordinator.select_bootstrap_primary(),
        None,
        "an all-observer fleet has no primary-capable candidate"
    );
}

/// An observer (worker_count == 0) is excluded by capacity even though
/// it is alive, mesh-ready, and a confirmed peer — the real
/// worker-secondary is chosen over the lower-id observer.
#[test]
fn excludes_capacity_observer() {
    let mut coordinator = coordinator_with_confirmed_peers(2);
    // sec-0 is an observer (no workers); sec-1 is a real worker-secondary.
    seed_secondary(&mut coordinator, "sec-0", 0, true);
    mark_mesh_ready(&mut coordinator, "sec-0");
    seed_secondary(&mut coordinator, "sec-1", 4, false);
    mark_mesh_ready(&mut coordinator, "sec-1");

    assert_eq!(
        coordinator.select_bootstrap_primary(),
        Some(PeerId::from("sec-1")),
        "an observer (worker_count==0) must never be selected, even at a lower id"
    );
}

/// A peer that is alive, has workers, and is mesh-ready but is NOT a
/// confirmed transport member (`has_peer == false`) is excluded; the
/// lower-id confirmed peer would otherwise lose to it.
#[test]
fn excludes_unconfirmed_peer() {
    // Only `sec-0` is a confirmed transport member; `sec-99` is not.
    let mut coordinator = coordinator_with_confirmed_peers(1);
    seed_secondary(&mut coordinator, "sec-0", 4, false);
    mark_mesh_ready(&mut coordinator, "sec-0");
    // `sec-99` is alive + has workers + mesh-ready, but `has_peer` is
    // false for it (not in the transport's `outgoing`), and its id sorts
    // ABOVE sec-0 so its exclusion is what keeps sec-0 the answer.
    seed_secondary(&mut coordinator, "sec-99", 4, false);
    mark_mesh_ready(&mut coordinator, "sec-99");

    assert_eq!(
        coordinator.select_bootstrap_primary(),
        Some(PeerId::from("sec-0")),
        "an unconfirmed (!has_peer) candidate must be excluded"
    );

    // And with sec-0 removed from the eligible set, the unconfirmed
    // sec-99 must NOT step in — the result collapses to None.
    let mut only_unconfirmed = coordinator_with_confirmed_peers(0);
    seed_secondary(&mut only_unconfirmed, "sec-99", 4, false);
    mark_mesh_ready(&mut only_unconfirmed, "sec-99");
    assert_eq!(
        only_unconfirmed.select_bootstrap_primary(),
        None,
        "the sole candidate being unconfirmed leaves no eligible peer"
    );
}

/// A peer that has workers (so it survives the capacity filter) but is
/// nonetheless listed in `role_table().observers` is excluded by the
/// defensive observer cut — the explicit `- observers` term, not the
/// capacity filter, is what bites here.
#[test]
fn excludes_observer_by_role_table_even_with_workers() {
    let mut coordinator = coordinator_with_confirmed_peers(2);
    // sec-0: an inconsistent record — worker_count>0 AND flagged observer.
    seed_secondary(&mut coordinator, "sec-0", 4, true);
    mark_mesh_ready(&mut coordinator, "sec-0");
    // sec-1: a clean worker-secondary.
    seed_secondary(&mut coordinator, "sec-1", 4, false);
    mark_mesh_ready(&mut coordinator, "sec-1");

    assert!(
        coordinator
            .cluster_state_for_test()
            .role_table()
            .observers
            .contains("sec-0"),
        "fixture precondition: sec-0 is in the observer set despite having workers"
    );
    assert_eq!(
        coordinator.select_bootstrap_primary(),
        Some(PeerId::from("sec-1")),
        "the defensive role_table().observers cut must exclude sec-0 \
         despite worker_count>0"
    );
}

/// A mesh-ready, confirmed, non-observer worker-secondary that has NOT
/// reported `MeshReady` is excluded by the `mesh_ready_secondaries`
/// intersection.
#[test]
fn excludes_not_mesh_ready_peer() {
    let mut coordinator = coordinator_with_confirmed_peers(2);
    // sec-0 is fully eligible EXCEPT it never reported MeshReady.
    seed_secondary(&mut coordinator, "sec-0", 4, false);
    // sec-1 is fully eligible.
    seed_secondary(&mut coordinator, "sec-1", 4, false);
    mark_mesh_ready(&mut coordinator, "sec-1");

    assert_eq!(
        coordinator.select_bootstrap_primary(),
        Some(PeerId::from("sec-1")),
        "a peer absent from mesh_ready_secondaries must be excluded \
         even at a lower id"
    );
}
