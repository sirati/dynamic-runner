//! Unit tests for the submitter's bootstrap hand-off relocation
//! decision ([`PrimaryCoordinator::relocate_primary_to`]) and the
//! relinquished-case result getters.
//!
//! These exercise the manager-layer mechanism directly (the pyo3
//! coordinator runtimes can't be unit-tested — libpython linking), over
//! a `ChannelPeerTransport` whose `has_peer` / `peer_count` reflect real
//! `outgoing` membership and whose broadcasts fan out to per-secondary
//! receivers a test can drain.

use super::*;
use dynrunner_core::ErrorType;
use dynrunner_protocol_primary_secondary::PeerId;

use crate::primary::lifecycle::RelocationOutcome;
use crate::primary::wire::compute_task_hash;

/// Build a submitter coordinator whose mesh transport confirms exactly
/// the secondaries `sec-0 .. sec-{confirmed_peers-1}` (the
/// `ChannelPeerTransport` keys `has_peer` / `peer_count` off its
/// `outgoing` membership, which `setup_test` populates with those ids),
/// returning the coordinator and the per-secondary end handles so a test
/// can drain the broadcasts the submitter fans out.
#[allow(clippy::type_complexity)]
fn coordinator_with_confirmed_peers(
    confirmed_peers: u32,
) -> (
    PrimaryCoordinator<ChannelPeerTransport<TestId>, ResourceStealingScheduler, FixedEstimator, TestId>,
    Vec<(
        String,
        tokio::sync::mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio::sync::mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
) {
    let (transport, ends) = setup_test(confirmed_peers);
    let coordinator = PrimaryCoordinator::new(
        PrimaryConfig {
            num_secondaries: confirmed_peers.max(1),
            // Short so the dead-fleet grace fires fast in the
            // peer-count==0 observer test.
            fleet_dead_timeout: std::time::Duration::from_millis(50),
            ..Default::default()
        },
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    (coordinator, ends)
}

/// Collect every `PrimaryChanged { new, epoch }` the submitter fanned
/// out to the given secondary-end receiver.
fn drain_primary_changes(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::ClusterMutation { mutations, .. } = msg {
            for m in mutations {
                if let ClusterMutation::PrimaryChanged { new, epoch, .. } = m {
                    out.push((new, epoch));
                }
            }
        }
    }
    out
}

/// `relocate_primary_to(chosen)` on a confirmed, non-observer candidate:
/// originates `PrimaryChanged { chosen, epoch+1 }` over the mesh, steps
/// the submitter down (drops its own `Role::Primary` via the local apply),
/// and — crucially — does NOT call `activate_local_primary` (so it never
/// sets `primary_id = self`).
#[tokio::test(flavor = "current_thread")]
async fn relocate_originates_primary_changed_and_does_not_pin_self() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, mut ends) = coordinator_with_confirmed_peers(1);

            // Pre-state: the submitter is the bootstrap primary WITHOUT a
            // self-announce — the relocate path does no submitter epoch-1
            // self-announce, so `primary_epoch` starts at 0 (the bootstrap
            // pin is not a `PrimaryChanged`).
            assert_eq!(coordinator.cluster_state_for_test().primary_epoch(), 0);

            let outcome = coordinator
                .relocate_primary_to(PeerId::from("sec-0"))
                .await
                .expect("relocate succeeds");
            assert_eq!(
                outcome,
                RelocationOutcome::Relocated,
                "a confirmed, non-observer candidate must relocate, not fall back"
            );

            // Wire announce: exactly one PrimaryChanged naming the CHOSEN
            // peer at epoch 1 (= primary_epoch()+1 = 0+1) fanned out over
            // the mesh.
            let (_id, rx, _tx) = &mut ends[0];
            assert_eq!(
                drain_primary_changes(rx),
                vec![("sec-0".to_string(), 1)],
                "relocate must broadcast PrimaryChanged{{ chosen, epoch+1 }}"
            );

            // The submitter applied its own broadcast: current_primary is
            // now the chosen peer at epoch 1 (it dropped Role::Primary).
            assert_eq!(
                coordinator.cluster_state_for_test().current_primary(),
                Some("sec-0"),
                "the submitter's own apply must repoint current_primary to the chosen peer"
            );
            assert_eq!(coordinator.cluster_state_for_test().primary_epoch(), 1);

            // The bootstrap pin is GONE: relocate must NOT set
            // primary_id = self (that is `activate_local_primary`'s job,
            // which the relocate path must never call).
            assert_eq!(
                coordinator.primary_id, None,
                "relocate_primary_to must NOT set primary_id = self \
                 (it does not call activate_local_primary)"
            );
        })
        .await;
}

/// Vanished-candidate fallback: when the chosen peer is NOT a confirmed
/// mesh peer at origination time (`has_peer == false`), relocate falls
/// back to `activate_local_primary` — the submitter STAYS primary
/// (`primary_id = self`, current_primary = self) and signals the caller
/// to run the normal operational path.
#[tokio::test(flavor = "current_thread")]
async fn relocate_falls_back_to_local_on_vanished_candidate() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Zero confirmed peers → has_peer("sec-0") is false.
            let (mut coordinator, _ends) = coordinator_with_confirmed_peers(0);

            let outcome = coordinator
                .relocate_primary_to(PeerId::from("sec-0"))
                .await
                .expect("relocate succeeds via fallback");
            assert_eq!(
                outcome,
                RelocationOutcome::FellBackToLocal,
                "an unconfirmed candidate must fall back to local primary"
            );

            // Fell back to activate_local_primary: the submitter stayed
            // primary (pinned its own id, named itself current_primary).
            assert_eq!(
                coordinator.primary_id,
                Some("primary".to_string()),
                "the fallback must activate the submitter as the local primary"
            );
            assert_eq!(
                coordinator.cluster_state_for_test().current_primary(),
                Some("primary"),
                "the fallback must leave the submitter as current_primary"
            );
        })
        .await;
}

/// A confirmed candidate that has nonetheless landed in
/// `role_table().observers` between selection and origination is rejected
/// by the defensive observer cut → fall back to local primary.
#[tokio::test(flavor = "current_thread")]
async fn relocate_falls_back_when_candidate_became_observer() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _ends) = coordinator_with_confirmed_peers(1);
            // sec-0 IS a confirmed mesh peer (has_peer true) but joined as
            // an observer, so it is in role_table().observers.
            coordinator
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-0".into(),
                    is_observer: true,
                    can_be_primary: false,
                });
            assert!(
                coordinator
                    .cluster_state_for_test()
                    .role_table()
                    .observers
                    .contains("sec-0"),
                "fixture precondition: sec-0 is an observer"
            );

            let outcome = coordinator
                .relocate_primary_to(PeerId::from("sec-0"))
                .await
                .expect("relocate succeeds via fallback");
            assert_eq!(
                outcome,
                RelocationOutcome::FellBackToLocal,
                "a candidate in role_table().observers must fall back to local primary"
            );
            assert_eq!(
                coordinator.primary_id,
                Some("primary".to_string()),
                "the observer-cut fallback must activate the submitter as local primary"
            );
        })
        .await;
}

/// E4b plumbing: in the relinquished case the result getters the PyO3
/// boundary reads (`completed` / `failed`) come from the replicated
/// ledger (`cluster_state.outcome_counts()`), NOT the now-empty local
/// pool / per-node sets. Seed terminal task states directly into the
/// replicated `cluster_state` (the only source an observer has — its pool
/// is empty) and assert the counts come back off the ledger.
#[tokio::test(flavor = "current_thread")]
async fn relinquished_result_getters_read_replicated_ledger() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _ends) = coordinator_with_confirmed_peers(1);

            // The observer's local pool / completed_tasks / failed_tasks
            // sets are empty (it relinquished authority and never
            // dispatched). The replicated ledger is the source of truth.
            assert!(
                coordinator.completed_tasks.is_empty() && coordinator.failed_tasks.is_empty(),
                "fixture precondition: the relinquished node's local sets are empty"
            );

            // Two succeeded, one failed — replicated into cluster_state
            // exactly as the chosen primary's broadcasts would feed an
            // observer's mirror.
            let ok_a = make_binary("ok-a", 10);
            let ok_b = make_binary("ok-b", 20);
            let bad = make_binary("bad", 30);
            for b in [&ok_a, &ok_b, &bad] {
                coordinator
                    .cluster_state_mut_for_test()
                    .apply(ClusterMutation::TaskAdded {
                        hash: compute_task_hash(b),
                        task: b.clone(),
                    });
            }
            for b in [&ok_a, &ok_b] {
                coordinator
                    .cluster_state_mut_for_test()
                    .apply(ClusterMutation::TaskCompleted {
                        hash: compute_task_hash(b),
                        result_data: None,
                    });
            }
            coordinator
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::TaskFailed {
                    hash: compute_task_hash(&bad),
                    kind: ErrorType::NonRecoverable,
                    error: "boom".into(),
                });

            // The getters the PyO3 boundary reads (run.rs:497-498) route
            // through `cluster_state.outcome_counts()`, so they report the
            // replicated tally even though the local sets are empty.
            assert_eq!(
                coordinator.completed_count(),
                2,
                "completed_count must read the replicated ledger, not the empty local set"
            );
            assert_eq!(
                coordinator.failed_count(),
                1,
                "failed_count must read the replicated ledger, not the empty local set"
            );
        })
        .await;
}
