//! Backpressure-bounce requeue coherence — the CRDT half of the bounce
//! classification (`primary/task/failed.rs`, the `is_backpressure_shaped`
//! arm).
//!
//! A backpressure-shaped `TaskFailed` ("No idle worker available" et al.)
//! is a requeue signal, not a terminal: the primary frees the slot +
//! ledger and returns the binary to the pool. Every OTHER requeue path
//! (`recover_inflight_for_dead_secondary`, `reconcile_inherited_slot`)
//! originates `ClusterMutation::TaskRequeued` in lockstep with the local
//! pool requeue, so the replicated `InFlight → Pending` transition
//! travels with the recovery. The bounce arm historically did NOT: the
//! local copy sat queued in the pool while every replica still recorded
//! the task `InFlight` on the bounced worker — fail-safe (a failover's
//! hydrate routes the stale `InFlight` to the in-flight ledger and the
//! reconciliation probe eventually denies it) but incoherent, and the
//! stale fact strands the task for the probe window on any failover that
//! lands inside it.

use super::*;

use crate::primary::wire::compute_task_hash;
use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind};

/// One advertised-memory resource amount (bytes), the live welcome shape.
fn mem(bytes: u64) -> Vec<ResourceAmount> {
    vec![ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// The member's `MeshReady` confirmation — without it the proactive
/// dispatch recheck withholds (the mesh-confirmation gate).
fn mesh_ready_from(secondary_id: &str) -> DistributedMessage<TestId> {
    DistributedMessage::MeshReady {
        target: None,
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        peer_count: 1,
    }
}

/// The secondary router's generic capacity bounce, mirrored VERBATIM
/// (`secondary/dispatch/router.rs`, the no-idle-worker arm): a
/// `TaskFailed` whose `error_message` is the recognised backpressure
/// marker.
fn backpressure_bounce(secondary_id: &str, worker_id: u32, hash: &str) -> DistributedMessage<TestId> {
    DistributedMessage::TaskFailed {
        target: None,
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        worker_id,
        task_hash: hash.into(),
        error_type: dynrunner_core::ErrorType::Recoverable,
        error_message: "No idle worker available".into(),
        delivery_seq: None,
        msgs_posted_through: None,
    }
}

/// A bounce-classified `TaskFailed` must originate `TaskRequeued` so the
/// replicated ledger mirrors the local pool requeue (`InFlight →
/// Pending`). FAILS pre-fix: the arm requeued locally only, leaving the
/// CRDT `InFlight` while the copy sat queued in the pool.
#[tokio::test(flavor = "current_thread")]
async fn bounce_requeue_originates_task_requeued() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let task = make_binary("bounced", 100);
            let hash = compute_task_hash(&task);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(PhaseId::from("default"), vec![])]),
                });
                cs.apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-0".into(),
                    is_observer: false,
                    can_be_primary: true,
                    cap_version: Default::default(),
                    member_gen: 0,
                });
                cs.apply(ClusterMutation::SecondaryCapacity {
                    secondary: "sec-0".into(),
                    worker_count: 1,
                    resources: mem(8 * 1024 * 1024 * 1024),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: hash.clone(),
                    task,
                });
            }
            primary
                .hydrate_from_cluster_state()
                .expect("test fixture: composed task graph is valid");
            primary.handle_mesh_ready(mesh_ready_from("sec-0"));

            // Live dispatch: commits the slot + ledger and originates the
            // replicated `Pending → InFlight` transition.
            primary
                .dispatch_to_idle_workers(true)
                .await
                .expect("dispatch recheck");
            settle_pump().await;
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&hash),
                    Some(crate::cluster_state::TaskState::InFlight { .. })
                ),
                "dispatch originated the replicated InFlight; got {:?}",
                primary.cluster_state_for_test().task_state(&hash)
            );

            // The capacity bounce: the secondary could not place the task.
            primary
                .handle_task_failed(backpressure_bounce("sec-0", 0, &hash), &mut None)
                .await;

            // Local recovery (pre-existing behaviour): pool requeue, ledger
            // freed, no retry budget burned.
            assert_eq!(
                primary.pool().iter().count(),
                1,
                "the bounced task is requeued into the pool"
            );
            assert!(
                primary.in_flight.is_empty(),
                "the bounce freed the in-flight ledger entry"
            );
            assert!(
                !primary.failed_tasks.contains_key(&hash),
                "a bounce is not a terminal: no retry budget consumed"
            );

            // THE coherence post-condition: the replicated ledger mirrors
            // the requeue. FAILS pre-fix — the CRDT stayed `InFlight` on
            // the bounced worker while the copy sat queued in the pool.
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&hash),
                    Some(crate::cluster_state::TaskState::Pending { .. })
                ),
                "the bounce requeue must originate TaskRequeued so the \
                 replicated state returns to Pending in lockstep with the \
                 pool requeue; got {:?}",
                primary.cluster_state_for_test().task_state(&hash)
            );
        })
        .await;
}
