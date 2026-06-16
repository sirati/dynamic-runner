//! #497 affine-deferral PRIMARY-HANDLER round trip — the never-wired-handler
//! loop the fix closes.
//!
//! Production sequence replayed:
//!   1. A work task `B` is assigned to an affine secondary the normal way:
//!      CRDT `InFlight` + tracked in `self.in_flight` + its worker slot
//!      `Assigned`.
//!   2. The secondary finds `B` depends on a SecondaryAffine gate that is
//!      `AffineReady` but not yet locally imported, so it DEFERS `B` (parks
//!      it in `affine_running`, NOT `active_tasks`) and reports
//!      `TaskQueuedAfterLocalDependency`.
//!   3. THE FIX: the primary's handler originates `QueuedAfterLocalDependencySet`
//!      (CRDT `InFlight → QueuedAfterLocalDependency`) AND removes `B` from
//!      `self.in_flight` — so the reconciliation probe, whose view is built
//!      SOLELY from `self.in_flight`, never sees `B` and cannot loop on it.
//!   4. The import completes; the secondary self-dispatches `B` and reports
//!      `LocalDependencyReleased`. The primary re-originates the EXISTING
//!      `TaskAssigned` (`→ InFlight`) and re-enters `B` into `self.in_flight`.
//!
//! REVERT-CHECK: on trunk (no handler) the reports hit the `dispatch_message`
//! catch-all, so `B` stays `InFlight` AND in `self.in_flight` forever — the
//! probe poll below would still surface `B` as probeable. The post-defer
//! `poll` assertion (no probe fires for `B`) is the load-bearing pin.

use super::*;

use crate::primary::reconciliation_probe::ReconciliationProber;
use crate::primary::wire::compute_task_hash;
use std::time::{Duration, Instant};

/// Build a 1-secondary primary and seat work task `B` as `InFlight` on
/// sec-0's worker exactly as a live dispatch would: the worker slot
/// `Assigned`, the CRDT `InFlight` fact, and the `self.in_flight` ledger
/// entry. Returns the primary, the mesh keepalive, and `B`'s hash.
async fn primary_with_inflight_dependent() -> (
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    PrimaryMeshKeepalive,
    String,
) {
    let (transport, _ends) = setup_test(1);
    let (mut primary, mesh) = build_test_primary(
        test_primary_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let task = make_binary("affine-dependent-B", 100);
    let hash = compute_task_hash(&task);
    // Seed the CRDT entry (TaskAdded → InFlight) the live dispatch wrote.
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::TaskAdded {
            hash: hash.clone(),
            task: task.clone(),
        });
    }
    // Seat B on sec-0's worker (slot Assigned + ledger entry + type slot),
    // then mirror the CRDT InFlight fact the originate would have written.
    let staged = primary.stage_in_flight_for_test("sec-0".into(), 0, task.clone());
    assert_eq!(staged, hash);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::TaskAssigned {
            hash: hash.clone(),
            secondary: "sec-0".into(),
            worker: 0,
            version: Default::default(),
            attempt: 0,
        });
    }
    assert!(
        primary.in_flight_for_test().contains_key(&hash),
        "fixture precondition: B is tracked InFlight in the ledger"
    );
    (primary, mesh, hash)
}

fn queued_report(secondary: &str, task_hash: &str) -> DistributedMessage<TestId> {
    DistributedMessage::TaskQueuedAfterLocalDependency {
        target: None,
        sender_id: secondary.into(),
        timestamp: 0.0,
        secondary_id: secondary.into(),
        task_hash: task_hash.into(),
        affine_hash: "affine-gate-I".into(),
    }
}

fn released_report(secondary: &str, task_hash: &str, worker_id: u32) -> DistributedMessage<TestId> {
    DistributedMessage::LocalDependencyReleased {
        target: None,
        sender_id: secondary.into(),
        timestamp: 0.0,
        secondary_id: secondary.into(),
        task_hash: task_hash.into(),
        worker_id,
    }
}

/// Count the `QueuedAfterLocalDependencySet` mutations carried by the
/// `ClusterMutation` frames drained from a secondary end, and the number of
/// distinct `ClusterMutation` FRAMES that carried at least one. Returns
/// `(frames, mutations)`. Mesh keepalives / digests are a different
/// `DistributedMessage` variant, so they are ignored.
fn count_queued_set_broadcasts(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> (usize, usize) {
    let mut frames = 0;
    let mut mutations = 0;
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::ClusterMutation { mutations: ms, .. } = msg {
            let n = ms
                .iter()
                .filter(|m| {
                    matches!(m, ClusterMutation::QueuedAfterLocalDependencySet { .. })
                })
                .count();
            if n > 0 {
                frames += 1;
                mutations += n;
            }
        }
    }
    (frames, mutations)
}

/// COALESCING (Commit 3): a contiguous run of N `TaskQueuedAfterLocalDependency`
/// reports dispatched through `dispatch_inbox_batch_coalescing_deferrals`
/// produces exactly ONE `ClusterMutation` broadcast carrying all N rank-drops
/// — not N broadcasts. The build_compilers affine burst (S × M reports) is
/// thereby O(1) broadcasts per inbox drain instead of O(N), so it no longer
/// floods ingest and trips the self-starvation false-election.
#[tokio::test(flavor = "current_thread")]
async fn deferral_burst_coalesces_into_one_broadcast() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Seat THREE distinct work tasks as InFlight on sec-0 (the
            // build_compilers burst shape — many dependents parked behind one
            // import), exactly as a live dispatch would.
            let mut hashes = Vec::new();
            for (slot, name) in ["dep-A", "dep-B", "dep-C"].iter().enumerate() {
                let task = make_binary(name, 100);
                let hash = compute_task_hash(&task);
                {
                    let cs = primary.cluster_state_mut_for_test();
                    cs.apply(ClusterMutation::TaskAdded {
                        hash: hash.clone(),
                        task: task.clone(),
                    });
                }
                let staged =
                    primary.stage_in_flight_for_test("sec-0".into(), slot as u32, task.clone());
                assert_eq!(staged, hash);
                {
                    let cs = primary.cluster_state_mut_for_test();
                    cs.apply(ClusterMutation::TaskAssigned {
                        hash: hash.clone(),
                        secondary: "sec-0".into(),
                        worker: slot as u32,
                        version: Default::default(),
                        attempt: 0,
                    });
                }
                hashes.push(hash);
            }

            // Drain any setup/keepalive frames already queued so the count
            // below reflects only the deferral burst's broadcasts.
            settle_pump().await;
            let (_, rx, _) = &mut ends[0];
            while rx.try_recv().is_ok() {}

            // The burst: three TaskQueuedAfterLocalDependency reports in one
            // inbox-drain batch, dispatched through the coalescing path.
            let batch: Vec<DistributedMessage<TestId>> = hashes
                .iter()
                .map(|h| queued_report("sec-0", h))
                .collect();
            primary
                .dispatch_inbox_batch_coalescing_deferrals(batch, &mut None)
                .await
                .expect("coalescing dispatch succeeds");

            // All three parked dependents are dropped from the ledger (the
            // per-report local effect is unchanged by coalescing).
            for h in &hashes {
                assert!(
                    !primary.in_flight_for_test().contains_key(h),
                    "each parked dependent must be dropped from the in-flight ledger"
                );
            }

            settle_pump().await;
            let (_, rx, _) = &mut ends[0];
            let (frames, mutations) = count_queued_set_broadcasts(rx);
            assert_eq!(
                frames, 1,
                "the deferral burst must coalesce into exactly ONE ClusterMutation \
                 broadcast frame (got {frames}); O(1) per inbox drain, not O(N)"
            );
            assert_eq!(
                mutations, 3,
                "the single coalesced frame must carry all three rank-drops (got \
                 {mutations})"
            );
        })
        .await;
}

/// THE round trip: defer parks B (CRDT QueuedAfterLocalDependency + dropped
/// from the in-flight ledger so the probe cannot loop), release re-seats it
/// (CRDT InFlight + re-entered in the ledger).
#[tokio::test(flavor = "current_thread")]
async fn defer_drops_from_in_flight_then_release_re_enters() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _mesh, hash) = primary_with_inflight_dependent().await;

            // (defer) The secondary's TaskQueuedAfterLocalDependency report.
            primary
                .handle_task_queued_after_local_dependency(queued_report("sec-0", &hash))
                .await;

            // CRDT: B is now QueuedAfterLocalDependency on sec-0.
            match primary.cluster_state_for_test().task_state(&hash) {
                Some(crate::cluster_state::TaskState::QueuedAfterLocalDependency {
                    secondary,
                    ..
                }) => assert_eq!(secondary, "sec-0"),
                other => panic!("expected QueuedAfterLocalDependency, got {other:?}"),
            }
            // Ledger: B is GONE — the load-bearing step. The reconciliation
            // probe builds its view from `self.in_flight`, so a poll past the
            // deadline now fires NO probe for B (on trunk it still would).
            assert!(
                !primary.in_flight_for_test().contains_key(&hash),
                "the defer handler must drop B from the in-flight ledger so \
                 the reconciliation probe never loops on the parked dependent"
            );
            let view: Vec<(&str, &str)> = primary
                .in_flight_for_test()
                .iter()
                .map(|(h, e)| (h.as_str(), e.secondary_id.as_str()))
                .collect();
            assert!(
                view.is_empty(),
                "the probe view (built from self.in_flight) must not contain B"
            );
            // Drive a real prober past the deadline against this empty view:
            // no probe fires (the loop the fix breaks would re-probe B here).
            let mut prober = ReconciliationProber::new(
                Duration::from_secs(600),
                Duration::from_secs(15),
                Duration::from_secs(3600),
            );
            let t0 = Instant::now();
            let _ = prober.poll(t0, &view);
            let tick = prober.poll(t0 + Duration::from_secs(601), &view);
            assert!(
                tick.probes.is_empty(),
                "no reconciliation probe may fire for a parked dependent"
            );

            // (release) The secondary's LocalDependencyReleased report.
            primary
                .handle_local_dependency_released(released_report("sec-0", &hash, 0))
                .await;

            // CRDT: B is InFlight again on sec-0/worker 0.
            match primary.cluster_state_for_test().task_state(&hash) {
                Some(crate::cluster_state::TaskState::InFlight {
                    secondary, worker, ..
                }) => {
                    assert_eq!(secondary, "sec-0");
                    assert_eq!(*worker, 0);
                }
                other => panic!("expected InFlight after release, got {other:?}"),
            }
            // Ledger: B is tracked again, so the probe + death seam cover it.
            let entry = primary
                .in_flight_for_test()
                .get(&hash)
                .expect("B must be re-entered into the in-flight ledger on release");
            assert_eq!(entry.secondary_id, "sec-0");
            assert_eq!(entry.local_worker_id, Some(0));
        })
        .await;
}
