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
//!     by re-dispatch — the run converges instead of wedging;
//!   * the per-sweep LOG AGGREGATION: one sweep emits exactly ONE INFO
//!     line (counts + not-held/no-answer hashes inline), per-task
//!     launch/verdict detail rides DEBUG, and a verdict landing after
//!     the aggregate was emitted is a DEBUG correction only.

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
            .probes
            .is_empty(),
        "first sight must arm, not fire"
    );
    let fired = primary
        .recon_prober
        .poll(t0 + timeout, &[(hash.as_str(), "sec-0")])
        .probes;
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
            // The amnesiac reports a DEGRADED mesh (peer_count=0) — the
            // channel fixture wires only primary↔secondary legs, so it sees
            // no siblings. Inject the FORMED-mesh report it would emit on a
            // real QUIC mesh so the run clears the mesh-formation deadline
            // and reaches the probe→requeue→recover chain under test (see
            // `inject_mesh_ready_for`). sec-1 (`fake_secondary`) already
            // reports a formed mesh.
            let id0_for_mesh = id0.clone();
            inject_mesh_ready_for(&tx0, std::slice::from_ref(&id0_for_mesh));
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
                    .run(SeedSource::PromotionSnapshot { kind: crate::process::BootstrapKind::Failover }, ops, ope)
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

// ---------------------------------------------------------------------
// Per-sweep log aggregation (one INFO line per sweep, not per task).
// ---------------------------------------------------------------------

/// Capture every event the probe module emits (any level), scoped to
/// this thread — and, on a `current_thread` runtime, therefore to the
/// whole test. See `test_capture::TargetCapture` for why an
/// always-interest install is safe to hold across `.await`s.
fn probe_log_capture() -> (
    crate::test_capture::TargetCapture,
    tracing::subscriber::DefaultGuard,
) {
    use tracing_subscriber::layer::SubscriberExt;
    let capture = crate::test_capture::TargetCapture::for_target(
        crate::primary::reconciliation_probe::LOG_TARGET,
    );
    let subscriber = tracing_subscriber::Registry::default().with(capture.clone());
    let guard = tracing::subscriber::set_default(subscriber);
    (capture, guard)
}

/// The probe config the log-shape tests run under: a 50ms
/// reconciliation deadline so a BACKDATED first sight is already past
/// due when the real wall-clock tick runs.
fn aggregation_test_config() -> PrimaryConfig {
    PrimaryConfig {
        task_reconciliation_timeout: Duration::from_millis(50),
        ..test_primary_config()
    }
}

/// Install a pool, stage `n` in-flight tasks on `sec-0`, and arm the
/// prober IN THE PAST, so the next REAL `reconciliation_probe_tick()`
/// (which reads the wall clock) is past both the poll cadence and
/// every task's deadline — the whole sweep then launches through the
/// real tick, which is where the per-task launch logging lives.
fn stage_backdated_sweep(
    primary: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    n: u32,
) -> Vec<String> {
    let phase = PhaseId::from("default");
    let pool = PendingPool::<TestId>::new([phase], HashMap::new()).expect("default-phase pool");
    primary.pending = Some(pool);
    let hashes: Vec<String> = (0..n)
        .map(|i| {
            let task = make_binary(&format!("sweep_{i}"), 100);
            primary.stage_in_flight_for_test("sec-0".into(), i, task)
        })
        .collect();
    // Backdate first sight by 2s: with the 50ms deadline and the 1s
    // prober poll cadence, the real tick is due AND every task is past
    // its deadline.
    let t0 = Instant::now() - Duration::from_secs(2);
    let view: Vec<(&str, &str)> = hashes.iter().map(|h| (h.as_str(), "sec-0")).collect();
    let armed = primary.recon_prober.poll(t0, &view);
    assert!(armed.probes.is_empty(), "first sight arms, never fires");
    hashes
}

/// One sweep, mixed verdicts: N=3 probes launch (per-task lines at
/// DEBUG — the per-task INFO flood is gone), one holder confirms, one
/// denies, one never answers. Exactly ONE INFO line is emitted for the
/// sweep — at the next due tick, since the straggler kept the cohort
/// open — carrying the H/L/S tallies with the not-held and no-answer
/// hashes attributable inline.
#[tokio::test(flavor = "current_thread")]
async fn sweep_emits_one_info_aggregate_with_mixed_verdicts() {
    let (capture, _guard) = probe_log_capture();
    let mut hashes: Vec<String> = Vec::new();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                aggregation_test_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            hashes = stage_backdated_sweep(&mut primary, 3);
            // The real tick launches the whole sweep (one cohort).
            primary.reconciliation_probe_tick().await;

            primary
                .dispatch_message(hold_response("sec-0", &hashes[0], true), &mut None)
                .await
                .unwrap();
            primary
                .dispatch_message(hold_response("sec-0", &hashes[1], false), &mut None)
                .await
                .unwrap();
            // hashes[2] never answers: the cohort stays open, so the
            // next due tick (one 1s poll cadence later) flushes it.
            tokio::time::sleep(Duration::from_millis(1100)).await;
            primary.reconciliation_probe_tick().await;
        })
        .await;

    let events = capture.events();
    let infos: Vec<_> = events
        .iter()
        .filter(|e| e.level == tracing::Level::INFO)
        .collect();
    assert_eq!(
        infos.len(),
        1,
        "exactly ONE INFO line per sweep — the aggregate: {events:#?}"
    );
    let line = &infos[0].event.message;
    assert!(
        line.contains("3 tasks past the reconciliation deadline"),
        "{line}"
    );
    assert!(line.contains("1 holders confirmed (re-armed)"), "{line}");
    assert!(line.contains("1 not held (failed + requeued)"), "{line}");
    assert!(
        line.contains("1 no answer (left to the silence machinery)"),
        "{line}"
    );
    let fields = &infos[0].event.fields;
    assert!(
        fields["not_held_tasks"].contains(&hashes[1]),
        "the state-changing (not held) hash stays attributable at INFO: {fields:?}"
    );
    assert!(
        fields["no_answer_tasks"].contains(&hashes[2]),
        "the unresolved hash stays attributable at INFO: {fields:?}"
    );
    // The per-task detail still exists — at DEBUG.
    assert!(
        events.iter().any(|e| e.level == tracing::Level::DEBUG
            && e.event.message.contains("probing its holder")),
        "per-task launch detail rides DEBUG: {events:#?}"
    );
    assert!(
        events.iter().any(|e| e.level == tracing::Level::DEBUG
            && e.event.message.contains("NOT HELD")),
        "per-task verdict detail rides DEBUG: {events:#?}"
    );
}

/// All-held sweep: the LAST verdict completes the cohort and the one
/// INFO aggregate is emitted immediately — no waiting for the next
/// tick, zero-count categories rendered, hash fields empty ("-").
#[tokio::test(flavor = "current_thread")]
async fn all_held_sweep_emits_one_info_on_cohort_completion() {
    let (capture, _guard) = probe_log_capture();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                aggregation_test_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let hashes = stage_backdated_sweep(&mut primary, 2);
            primary.reconciliation_probe_tick().await;
            for hash in &hashes {
                primary
                    .dispatch_message(hold_response("sec-0", hash, true), &mut None)
                    .await
                    .unwrap();
            }
            // NO second tick: completion emits eagerly.
        })
        .await;

    let events = capture.events();
    let infos: Vec<_> = events
        .iter()
        .filter(|e| e.level == tracing::Level::INFO)
        .collect();
    assert_eq!(
        infos.len(),
        1,
        "the cohort-completing verdict emits the one INFO aggregate: {events:#?}"
    );
    let line = &infos[0].event.message;
    assert!(
        line.contains("2 tasks past the reconciliation deadline"),
        "{line}"
    );
    assert!(line.contains("2 holders confirmed (re-armed)"), "{line}");
    assert!(line.contains("0 not held (failed + requeued)"), "{line}");
    assert!(
        line.contains("0 no answer (left to the silence machinery)"),
        "{line}"
    );
    assert_eq!(infos[0].event.fields["not_held_tasks"], "-");
    assert_eq!(infos[0].event.fields["no_answer_tasks"], "-");
}

/// A verdict that arrives AFTER its sweep's aggregate was emitted (it
/// was counted there as no-answer) logs a DEBUG correction only —
/// never a second INFO line.
#[tokio::test(flavor = "current_thread")]
async fn late_verdict_after_cohort_emission_logs_debug_only() {
    let (capture, _guard) = probe_log_capture();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                aggregation_test_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let hashes = stage_backdated_sweep(&mut primary, 1);
            primary.reconciliation_probe_tick().await;
            // No answer by the next due tick: the aggregate goes out
            // with the task counted as no-answer.
            tokio::time::sleep(Duration::from_millis(1100)).await;
            primary.reconciliation_probe_tick().await;
            // The verdict lands after the aggregate (the response
            // window — keepalive-derived, 15s here — is still open, so
            // the probe SEMANTICS still re-arm; only the logging is a
            // late correction).
            primary
                .dispatch_message(hold_response("sec-0", &hashes[0], true), &mut None)
                .await
                .unwrap();
        })
        .await;

    let events = capture.events();
    let infos: Vec<_> = events
        .iter()
        .filter(|e| e.level == tracing::Level::INFO)
        .collect();
    assert_eq!(infos.len(), 1, "still exactly one INFO: {events:#?}");
    assert!(
        infos[0]
            .event
            .message
            .contains("1 no answer (left to the silence machinery)"),
        "{}",
        infos[0].event.message
    );
    let verdicts: Vec<_> = events
        .iter()
        .filter(|e| e.event.message.contains("HELD"))
        .collect();
    assert_eq!(verdicts.len(), 1, "{events:#?}");
    assert_eq!(
        verdicts[0].level,
        tracing::Level::DEBUG,
        "a late verdict logs DEBUG only: {events:#?}"
    );
    assert_eq!(
        verdicts[0]
            .event
            .fields
            .get("late_after_sweep_summary")
            .map(String::as_str),
        Some("true"),
        "the correction is marked late: {events:#?}"
    );
}
