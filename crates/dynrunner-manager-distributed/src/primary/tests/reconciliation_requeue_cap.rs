//! #497 backstop — the reconciliation-requeue CAP, coordinator level.
//!
//! Derived from the production looper (a4af7457): a work task whose holder
//! repeatedly denies it (`held = false`) is requeued through the
//! backpressure-shaped path every `task_reconciliation_timeout`, forever,
//! re-originating `InFlight` and leaking the coordinator — because the task
//! can NEVER register a holder (a never-wired report, an affine dependent
//! unfulfillable on every secondary). The cap bounds that loop: after
//! `max_reconciliation_requeues` consecutive LOSS requeues with no genuine
//! progress, the task is routed to a NonRecoverable terminal instead of
//! requeued, so the run fails fast with a diagnostic.
//!
//! Pinned here:
//!   * the loop TERMINATES — N consecutive losses reach the NonRecoverable
//!     terminal (`failed_tasks` records it), it is NOT requeued again;
//!   * each loss BELOW the cap is the unchanged backpressure-shaped requeue
//!     (dispatchable again, no retry-budget burn);
//!   * a task that makes genuine PROGRESS (a real terminal lands) RESETS the
//!     counter — a later fresh loss starts counting from zero, never poisoned
//!     by stale losses.

use std::time::{Duration, Instant};

use dynrunner_core::PhaseId;
use dynrunner_scheduler_api::PendingPool;

use super::*;

/// Build a 1-secondary primary with a default-phase pool and a SMALL cap so
/// the loop is cheap to drive to the terminal.
fn primary_with_cap(
    cap: u32,
) -> (
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    PrimaryMeshKeepalive,
) {
    let (transport, _ends) = setup_test(1);
    let (mut primary, mesh) = build_test_primary(
        PrimaryConfig {
            max_reconciliation_requeues: cap,
            ..test_primary_config()
        },
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let phase = PhaseId::from("default");
    let pool = PendingPool::<TestId>::new([phase], HashMap::new()).expect("default-phase pool");
    primary.pending = Some(pool);
    (primary, mesh)
}

/// Seat `task` as InFlight on sec-0/worker-0 and prime the prober so a probe
/// for it is OUTSTANDING against sec-0 (the state a `TaskHoldResponse`
/// verdict is adjudicated in). `clock` is a shared MONOTONIC virtual clock
/// advanced across cycles so each poll lands past the prober's poll-cadence
/// throttle (the prober's deadlines are stored `Instant`s — a fresh
/// `Instant::now()` per cycle would not be monotone vs the prober's
/// `next_poll`). Returns the hash.
fn stage_and_prime_probe(
    primary: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    task: TaskInfo<TestId>,
    clock: &mut Instant,
) -> String {
    let hash = primary.stage_in_flight_for_test("sec-0".into(), 0, task);
    let timeout = primary.config.task_reconciliation_timeout;
    // First sight arms the deadline at `clock + timeout`.
    let _ = primary.recon_prober.poll(*clock, &[(hash.as_str(), "sec-0")]);
    // Advance past the deadline: the outstanding probe fires.
    *clock += timeout + Duration::from_secs(1);
    let fired = primary
        .recon_prober
        .poll(*clock, &[(hash.as_str(), "sec-0")])
        .probes;
    assert_eq!(fired.len(), 1, "deadline elapsed: one probe outstanding");
    // Advance again so the NEXT cycle's first-sight poll is strictly later
    // than this cycle's (monotonic vs the prober's internal `next_poll`).
    *clock += timeout + Duration::from_secs(1);
    hash
}

fn deny(responder: &str, task_hash: &str) -> DistributedMessage<TestId> {
    DistributedMessage::TaskHoldResponse {
        target: None,
        sender_id: responder.into(),
        timestamp: 0.0,
        task_hash: task_hash.into(),
        held: false,
    }
}

/// N consecutive LOST cycles with no progress: the first N-1 requeue
/// (backpressure-shaped, no retry burn); the Nth fails NonRecoverable
/// instead of requeueing — the loop terminates.
#[tokio::test(flavor = "current_thread")]
async fn n_consecutive_losses_reach_nonrecoverable_terminal() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            const CAP: u32 = 3;
            let (mut primary, _mesh) = primary_with_cap(CAP);
            let task = make_binary("never-registers", 100);
            let mut clock = Instant::now();

            // Drive CAP-1 lost cycles: each is a backpressure requeue.
            for cycle in 1..CAP {
                let hash = stage_and_prime_probe(&mut primary, task.clone(), &mut clock);
                primary
                    .dispatch_message(deny("sec-0", &hash), &mut None)
                    .await
                    .unwrap();
                assert!(
                    !primary.in_flight.contains_key(&hash),
                    "cycle {cycle}: the lost entry is dropped"
                );
                assert!(
                    primary.failed_tasks.is_empty(),
                    "cycle {cycle} (< cap): backpressure requeue burns no retry budget"
                );
                assert!(
                    primary.pool().has_queued_dispatchable(),
                    "cycle {cycle} (< cap): the task is requeued (dispatchable)"
                );
                // The re-dispatch puts it back; simulate by re-staging next
                // loop iteration (the scheduler would re-assign it to sec-0).
                // Clear the pool's queued copy so the re-stage is the sole
                // InFlight record (mirrors take_selected on re-dispatch).
                let _ = primary.pool_mut().drain_queued();
            }

            // The CAP-th loss: the cap trips → NonRecoverable terminal, NOT
            // a requeue. The loop is bounded.
            let hash = stage_and_prime_probe(&mut primary, task.clone(), &mut clock);
            primary
                .dispatch_message(deny("sec-0", &hash), &mut None)
                .await
                .unwrap();
            assert_eq!(
                primary.failed_tasks.get(&hash),
                Some(&dynrunner_core::ErrorType::NonRecoverable),
                "the CAP-th consecutive loss must fail the task NonRecoverable"
            );
            assert!(
                !primary.pool().has_queued_dispatchable(),
                "a capped task is NOT requeued — the loop terminates"
            );
        })
        .await;
}

/// Genuine PROGRESS resets the per-task counter so the cap can never poison
/// a task whose terminal is merely slow. The SAME hash accumulates CAP-1
/// losses, then a real TaskComplete clears the counter; a subsequent loss on
/// that hash is cycle 1 again (NOT the capped NonRecoverable terminal it
/// would be if the pre-progress losses had persisted). The `failed_tasks`
/// ledger is the load-bearing oracle: a capped loss records NonRecoverable
/// there, a sub-cap loss records nothing (backpressure-shaped).
#[tokio::test(flavor = "current_thread")]
async fn genuine_progress_resets_the_counter() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            const CAP: u32 = 3;
            let (mut primary, _mesh) = primary_with_cap(CAP);
            let task = make_binary("slow-but-real", 100);
            let mut clock = Instant::now();

            // CAP-1 lost cycles on this hash (counter now at CAP-1 — ONE more
            // loss without progress would trip the cap).
            for _ in 1..CAP {
                let hash = stage_and_prime_probe(&mut primary, task.clone(), &mut clock);
                primary
                    .dispatch_message(deny("sec-0", &hash), &mut None)
                    .await
                    .unwrap();
                let _ = primary.pool_mut().drain_queued();
            }

            // Genuine progress on the SAME hash: a real terminal lands and
            // clears the loss counter.
            let hash = stage_and_prime_probe(&mut primary, task.clone(), &mut clock);
            primary
                .dispatch_message(
                    DistributedMessage::TaskComplete {
                        target: None,
                        sender_id: "sec-0".into(),
                        timestamp: 0.0,
                        secondary_id: "sec-0".into(),
                        worker_id: 0,
                        task_hash: hash.clone(),
                        result_data: None,
                        delivery_seq: None,
                        msgs_posted_through: None,
                    },
                    &mut None,
                )
                .await
                .unwrap();
            assert!(
                primary.completed_tasks.contains(&hash),
                "the genuine terminal landed"
            );

            // A loss on the SAME hash AFTER progress: had the pre-progress
            // CAP-1 losses persisted, this (the CAP-th) would trip the
            // NonRecoverable terminal. Because progress RESET the counter it
            // is cycle 1 — a backpressure-shaped requeue, so `failed_tasks`
            // stays empty (no NonRecoverable record).
            let hash = stage_and_prime_probe(&mut primary, task.clone(), &mut clock);
            primary
                .dispatch_message(deny("sec-0", &hash), &mut None)
                .await
                .unwrap();
            assert!(
                primary.failed_tasks.is_empty(),
                "progress RESET the counter: the post-progress loss is cycle 1 \
                 (backpressure requeue), NOT the capped NonRecoverable terminal \
                 it would be if the pre-progress losses had persisted"
            );
        })
        .await;
}
