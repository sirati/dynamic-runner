//! #308 — the per-task reconciliation probe, coordinator-level.
//!
//! The pure timing unit (per-task persistent deadlines, fires-under-
//! load, re-arm semantics, response windows, holder-change voiding) is
//! unit-tested in `primary::reconciliation_probe`. This family covers
//! the COORDINATOR wiring around it:
//!   * the NOT-HELD verdict routes through the backpressure-shaped
//!     `handle_task_failed` path — slot freed, ledger entry dropped,
//!     task requeued, NO retry budget burned;
//!   * the HELD verdict leaves the assignment completely untouched;
//!   * a denial from a NON-holder (stale responder) is ignored;
//!   * end-to-end: a live-but-amnesiac secondary that swallowed its
//!     work is probed, denies, and the work is recovered + completed
//!     by re-dispatch — the run converges instead of wedging.

use std::time::Instant;

use dynrunner_core::PhaseId;
use dynrunner_scheduler_api::PendingPool;

use super::*;

/// Stage one in-flight task on `sec-0`/worker-0 (the real
/// `commit_assignment` lifecycle) with a pool installed, and prime the
/// prober (virtual clock) so a probe for the task is OUTSTANDING
/// against `sec-0` — the state in which a `TaskHoldResponse` verdict
/// is adjudicated.
fn stage_probed_task(
    primary: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
) -> String {
    let phase = PhaseId::from("default");
    let pool = PendingPool::<TestId>::new([phase], HashMap::new()).expect("default-phase pool");
    primary.pending = Some(pool);

    let task = make_binary("probed", 100);
    let hash = primary.stage_in_flight_for_test("sec-0".into(), 0, task);

    // Prime the prober with explicit instants: first sight arms the
    // deadline; a poll past it emits the probe (outstanding).
    let timeout = primary.config.task_reconciliation_timeout;
    let t0 = Instant::now();
    assert!(
        primary
            .recon_prober
            .poll(t0, &[(hash.as_str(), "sec-0")])
            .is_empty(),
        "first sight must arm, not fire"
    );
    let fired = primary
        .recon_prober
        .poll(t0 + timeout, &[(hash.as_str(), "sec-0")]);
    assert_eq!(fired.len(), 1, "deadline elapsed: exactly one probe fires");
    hash
}

fn hold_response(responder: &str, task_hash: &str, held: bool) -> DistributedMessage<TestId> {
    DistributedMessage::TaskHoldResponse {
        target: None,
        sender_id: responder.into(),
        timestamp: 0.0,
        task_hash: task_hash.into(),
        held,
    }
}

/// The NOT-HELD verdict: the holder's denial frees the slot, drops the
/// in-flight ledger entry, requeues the task into the pool
/// (dispatchable again), and does NOT consume retry budget (the task
/// never failed anywhere — `failed_tasks` stays empty). This is the
/// backpressure-shaped path, reached through the real
/// `dispatch_message` ingest.
#[tokio::test(flavor = "current_thread")]
async fn not_held_verdict_requeues_without_burning_retry_budget() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let hash = stage_probed_task(&mut primary);
            assert!(primary.in_flight.contains_key(&hash));
            assert!(primary.slot_holds_hash_for_test("sec-0", 0, &hash));

            primary
                .dispatch_message(hold_response("sec-0", &hash, false), &mut None)
                .await
                .unwrap();

            assert!(
                !primary.in_flight.contains_key(&hash),
                "the lost task's in-flight ledger entry must be dropped"
            );
            assert!(
                !primary.slot_holds_hash_for_test("sec-0", 0, &hash),
                "the holding slot must be freed back to Idle"
            );
            assert!(
                primary.pool().has_queued_dispatchable(),
                "the lost task must be REQUEUED (dispatchable again), not dropped"
            );
            assert!(
                primary.failed_tasks.is_empty(),
                "a probe-verdict loss is backpressure-shaped: it must not \
                 consume the task's retry budget"
            );
            assert!(
                !primary.completed_tasks.contains(&hash),
                "the task is pending again, never terminal"
            );
        })
        .await;
}

/// The HELD verdict (a long build): the assignment is left completely
/// untouched — slot still assigned, ledger entry still present, pool
/// still empty, no failure recorded. Zero false fires by construction.
#[tokio::test(flavor = "current_thread")]
async fn held_verdict_leaves_assignment_untouched() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let hash = stage_probed_task(&mut primary);

            primary
                .dispatch_message(hold_response("sec-0", &hash, true), &mut None)
                .await
                .unwrap();

            assert!(
                primary.in_flight.contains_key(&hash),
                "held: the in-flight entry stays"
            );
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &hash),
                "held: the slot stays assigned"
            );
            assert!(
                !primary.pool().has_queued_dispatchable(),
                "held: nothing is requeued"
            );
            assert!(primary.failed_tasks.is_empty(), "held: nothing failed");
        })
        .await;
}

/// A DENIAL from a node that is not the probed holder (a stale answer
/// from a previous holder, a misdirected frame, an answer to a prior
/// primary's probe) must never fail the live assignment.
#[tokio::test(flavor = "current_thread")]
async fn denial_from_non_holder_is_ignored() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(2);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let hash = stage_probed_task(&mut primary);

            // sec-1 was never the probed holder; its denial is noise.
            primary
                .dispatch_message(hold_response("sec-1", &hash, false), &mut None)
                .await
                .unwrap();

            assert!(
                primary.in_flight.contains_key(&hash),
                "a non-holder's denial must not drop the in-flight entry"
            );
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &hash),
                "a non-holder's denial must not free the slot"
            );
            assert!(primary.failed_tasks.is_empty());
            assert!(!primary.pool().has_queued_dispatchable());
        })
        .await;
}

/// End-to-end through the real operational loop: sec-0 is LIVE but
/// AMNESIAC — it swallows its initially-assigned work (no terminal,
/// ever) and answers the reconciliation probe with `held = false`;
/// sec-1 is a normal completing secondary. With a short
/// `task_reconciliation_timeout` the probe fires, the denial requeues
/// the swallowed tasks, re-dispatch lands them on a worker that
/// actually runs them (post-amnesia sec-0 or sec-1 — wherever the
/// scheduler places them), and the run CONVERGES with every task
/// completed and zero failures. Without the probe this run wedges
/// forever: the holder is alive (its probe answers keep its keepalive
/// fresh), so the silence machinery never declares it dead, and no
/// terminal ever arrives for the swallowed tasks.
#[tokio::test(flavor = "current_thread")]
async fn lost_tasks_are_probed_denied_requeued_and_recovered_e2e() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut secondary_ends) = setup_test(2);

            let config = PrimaryConfig {
                num_secondaries: 2,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                // Short probe deadline so the e2e exercises the real
                // fire → query → denial → requeue → re-dispatch chain
                // inside test time (the prober's 1s poll cadence is the
                // effective floor).
                task_reconciliation_timeout: Duration::from_millis(100),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..4)
                .map(|i| make_binary(&format!("bin_{i}"), 100))
                .collect();
            let hashes: Vec<String> = binaries
                .iter()
                .map(crate::primary::wire::compute_task_hash)
                .collect();

            // sec-0: the amnesiac (swallows its initial batch, denies
            // holding); sec-1: a normal completing secondary.
            let (id0, rx0, tx0) = secondary_ends.remove(0);
            tokio::task::spawn_local(fake_amnesiac_secondary(
                id0,
                1,
                1024 * 1024 * 1024,
                rx0,
                tx0,
            ));
            let (id1, rx1, tx1) = secondary_ends.remove(0);
            tokio::task::spawn_local(fake_secondary(id1, 2, 1024 * 1024 * 1024, rx1, tx1));

            {
                let (deps, ops, ope) = noop_phase_args();
                seed_operational_ledger(&mut primary, binaries, deps);
                primary
                    .run(SeedSource::PromotionSnapshot, ops, ope)
                    .await
                    .unwrap()
            };

            assert_eq!(
                primary.completed_count(),
                4,
                "every task — including the swallowed batch — must be \
                 recovered by the probe verdict and completed somewhere"
            );
            assert_eq!(
                primary.failed_count(),
                0,
                "probe-verdict recovery is backpressure-shaped: no task \
                 ends up failed"
            );
            for hash in &hashes {
                assert!(
                    primary.completed_tasks.contains(hash),
                    "task {hash} must be terminal-completed"
                );
            }
        })
        .await;
}
