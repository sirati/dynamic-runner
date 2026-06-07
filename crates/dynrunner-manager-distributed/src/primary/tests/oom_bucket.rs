//! OOM-retry-bucket dispatch-shape coverage.
//!
//! Single concern: pin the OOM-bucket throughput optimization (user
//! spec 2026-05-17) end-to-end at the unit-test boundary. The four
//! invariants that fall under this file:
//!
//!   1. Memory-DESC pairing: candidates sorted by estimator-memory
//!      DESC; secondaries snapshotted by node-memory DESC; per-task
//!      `preferred_secondaries` written cyclically through the
//!      snapshot. Biggest task → biggest secondary.
//!   2. Single-worker mask: while the OOM bucket is active, only
//!      worker_0 of each secondary is eligible for dispatch.
//!   3. Flag lifecycle: `single_worker_mode` is `true` while the
//!      OOM bucket is reinjecting and `false` outside (including
//!      after the bucket settles with no candidates left or with
//!      its budget exhausted).
//!   4. 5-min stuck-worker watchdog is disabled during the bucket
//!      (single-worker memory-pressed retries can legitimately
//!      exceed the watchdog window).
//!
//! Tests #1, #2, #3 exercise the retry-bucket primitive directly
//! against a coordinator state seeded by hand (no transport
//! traffic) — this keeps each invariant testable in isolation
//! without re-spinning the full distributed pipeline. Test #4
//! pins the watchdog gate by inspecting `failed_tasks` after a
//! synthetic OOM-bucket window.

use std::collections::HashMap;

use dynrunner_core::{
    ErrorType, PhaseId, ResourceAmount, ResourceKind, ResourceMap, SoftPreferredSecondaries,
    TaskInfo, TypeId,
};
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator};

use super::*;
use crate::primary::retry_bucket::BucketKind;
use crate::state::{SecondaryConnection, SecondaryConnectionState};
use crate::worker_signal::WorkerMgmtSignal;

/// Estimator that returns `task.size` bytes as the memory cost. The
/// OOM bucket sorts retries by estimator-memory DESC, so the size
/// field doubles as the per-task memory budget for these tests. No
/// non-memory resources advertised.
#[derive(Clone)]
struct SizeEqualsMemoryEstimator;

impl ResourceEstimator<TestId> for SizeEqualsMemoryEstimator {
    fn estimate(&self, task: &TaskInfo<TestId>) -> ResourceMap {
        ResourceMap::from([(ResourceKind::memory(), task.size)])
    }
}

/// Build a `TaskInfo<TestId>` with explicit phase + size. The size
/// is the estimator's per-task memory cost (see
/// [`SizeEqualsMemoryEstimator`]).
fn phased_task(name: &str, phase: &str, size: u64) -> TaskInfo<TestId> {
    TaskInfo {
        path: std::path::PathBuf::from(format!("/tmp/{name}")),
        size,
        identifier: TestId(name.into()),
        phase_id: PhaseId::from(phase),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: name.into(),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        resolved_path: None,
    }
}

/// Register `secondary_id` as `Operational` with `memory_bytes`
/// advertised RAM and `num_workers` workers. Mirrors
/// `register_operational_secondary` in `preferred_secondaries.rs`
/// but parameterised on memory so the memory-DESC accessor has
/// distinguishable inputs. Also seeds the per-secondary
/// [`RemoteWorkerState`] entries the dispatch pipeline iterates.
fn register_secondary_with_workers(
    primary: &mut PrimaryCoordinator<ResourceStealingScheduler, SizeEqualsMemoryEstimator, TestId>,
    secondary_id: &str,
    memory_bytes: u64,
    num_workers: u32,
) {
    let conn = SecondaryConnection::new(secondary_id.into())
        .receive_welcome(
            num_workers,
            vec![ResourceAmount {
                kind: ResourceKind::memory(),
                amount: memory_bytes,
            }],
            "host".into(),
            0,
            None,
            false,
            false,
        )
        .receive_cert_exchange(String::new(), None, None, 0)
        .begin_peer_discovery()
        .peers_ready()
        .assignments_sent();
    primary.secondaries.insert(
        secondary_id.into(),
        SecondaryConnectionState::Operational(conn),
    );
    let next_global = primary.workers.len() as u32;
    for local in 0..num_workers {
        primary.register_idle_worker_for_test(
            secondary_id.into(),
            next_global + local,
            ResourceMap::from([(ResourceKind::memory(), memory_bytes)]),
        );
    }
}

/// Shared minimal config tuned for the OOM-bucket unit tests: a
/// generous OOM budget so the bucket can actually run, no retry
/// pacing pressure, no setup-promote gate.
fn oom_bucket_test_config(oom_retry_max_passes: u32) -> PrimaryConfig {
    PrimaryConfig {
        num_secondaries: 0,
        connect_timeout: std::time::Duration::from_secs(5),
        peer_timeout: std::time::Duration::from_secs(5),
        oom_retry_max_passes,
        ..test_primary_config()
    }
}

/// Build a coordinator pre-seeded with a default-phase pool +
/// `tasks` in `all_binaries`. Pool is initialised empty (drained
/// state); the OOM-bucket reinject is what populates it for each
/// of these tests.
fn make_primed_primary(
    transport: ChannelPeerTransport<TestId>,
    oom_retry_max_passes: u32,
    tasks: Vec<TaskInfo<TestId>>,
) -> (
    PrimaryCoordinator<ResourceStealingScheduler, SizeEqualsMemoryEstimator, TestId>,
    PrimaryMeshKeepalive,
) {
    let (mut primary, mesh) = build_test_primary(
        oom_bucket_test_config(oom_retry_max_passes),
        transport,
        ResourceStealingScheduler::memory(),
        SizeEqualsMemoryEstimator,
    );
    let phase = PhaseId::from("default");
    let pool = PendingPool::<TestId>::new([phase.clone()], std::collections::HashMap::new())
        .expect("default-phase pool");
    primary.pending = Some(pool);
    primary.total_tasks = tasks.len();
    primary.all_binaries = tasks;
    (primary, mesh)
}

/// Mark every task in `all_binaries` as failed `ResourceExhausted(memory)`
/// — the failure class the OOM bucket pulls from.
fn mark_all_failed_oom(
    primary: &mut PrimaryCoordinator<ResourceStealingScheduler, SizeEqualsMemoryEstimator, TestId>,
) {
    for binary in &primary.all_binaries {
        let hash = crate::primary::wire::compute_task_hash(binary);
        primary
            .failed_tasks
            .insert(hash, ErrorType::ResourceExhausted(ResourceKind::memory()));
    }
}

/// Test #1: OOM bucket dispatches each retry to a secondary in
/// memory-DESC order (biggest task → biggest secondary), and only
/// worker_0 of each secondary serves the retry.
///
/// Setup: 4 tasks each failed OOM (sizes 100/80/60/40 → memory
/// estimates via [`SizeEqualsMemoryEstimator`]). 4 secondaries with
/// descending memory budgets (1024/800/600/400). Each secondary has
/// 2 workers.
///
/// We register the per-secondary outgoing channels so the bucket's
/// `dispatch_to_idle_workers` kickstart actually emits
/// `TaskAssignment` messages on the wire. The assertions inspect
/// the wire to verify (a) each task went to its memory-DESC paired
/// secondary and (b) every assignment landed on `worker_id == 0`
/// of its secondary (single-worker mask).
#[tokio::test(flavor = "current_thread")]
async fn oom_bucket_dispatches_tasks_to_secondaries_memory_desc() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Build a transport with per-secondary outgoing channels
            // registered. Each secondary has a receiver we drain after
            // the bucket runs to observe the actual dispatch shape.
            // `_incoming_tx_keepalive` is held only to keep the
            // incoming receiver from observing a close — this test
            // never sends inbound traffic, but the operational shape
            // expects the channel to stay open across the bucket call.
            let (_incoming_tx_keepalive, incoming_rx) =
                tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            let mut outgoing: HashMap<
                String,
                tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
            > = HashMap::new();
            let mut sec_receivers: HashMap<
                String,
                tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
            > = HashMap::new();
            for sec_id in ["sec-1024", "sec-800", "sec-600", "sec-400"] {
                let (tx, rx) = tokio_mpsc::unbounded_channel();
                outgoing.insert(sec_id.into(), tx);
                sec_receivers.insert(sec_id.into(), rx);
            }
            let transport =
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);

            let tasks = vec![
                phased_task("t_small", "default", 40),
                phased_task("t_large", "default", 100),
                phased_task("t_medium", "default", 60),
                phased_task("t_big", "default", 80),
            ];
            let (mut primary, _mesh) =
                make_primed_primary(transport, /* oom_passes */ 1, tasks);

            // 4 secondaries with descending memory + 2 workers each.
            // Names chosen so the lexicographic tie-break is
            // observable but never load-bearing (no two memories
            // tie).
            register_secondary_with_workers(&mut primary, "sec-1024", 1024, 2);
            register_secondary_with_workers(&mut primary, "sec-800", 800, 2);
            register_secondary_with_workers(&mut primary, "sec-600", 600, 2);
            register_secondary_with_workers(&mut primary, "sec-400", 400, 2);

            mark_all_failed_oom(&mut primary);

            // Install the worker-management bus so the OOM bucket's
            // post-reinject `TasksAdded` emit lands on a receiver we can
            // drain. Dispatch is now DEFERRED to the worker-management
            // recheck (the dispatch-decoupling law) rather than fired
            // synchronously inside `try_run_phase_retry_bucket`; this
            // test drives the recheck explicitly below so it observes
            // the same wire shape via the deferred path.
            let (wm_tx, mut wm_rx) = tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
            primary
                .cluster_state_mut_for_test()
                .install_worker_mgmt_sender(wm_tx);

            // Pre-flight: `secondaries_sorted_by_memory_desc` returns
            // the canonical ordering the bucket uses to pair tasks.
            let ordered = primary.secondaries_sorted_by_memory_desc();
            assert_eq!(
                ordered,
                vec![
                    "sec-1024".to_string(),
                    "sec-800".to_string(),
                    "sec-600".to_string(),
                    "sec-400".to_string(),
                ],
                "secondaries_sorted_by_memory_desc must return DESC by memory"
            );

            let mut command_rx = None;
            let phase = PhaseId::from("default");
            let reinjected = primary
                .try_run_phase_retry_bucket(&phase, BucketKind::Oom, &mut command_rx)
                .await
                .expect("OOM bucket call must succeed");
            assert!(
                reinjected,
                "OOM bucket must reinject when candidates + budget available"
            );
            assert!(
                primary.single_worker_mode(),
                "single_worker_mode must be true while the OOM bucket is active"
            );

            // Deferred-recheck contract: the bucket emitted a
            // `TasksAdded` rather than dispatching inline. Drain the
            // coalesced batch and run the worker-management reaction
            // synchronously — this is exactly what the operational
            // loop's worker-management `select!` arm does, minus the
            // 50ms idle window (driven here directly for determinism).
            let batch = crate::worker_signal::drain_worker_signal_batch(
                &mut wm_rx,
                std::time::Duration::from_millis(50),
            )
            .await
            .expect("OOM-bucket reinject must emit a TasksAdded batch");
            assert!(
                batch.signals.contains(&WorkerMgmtSignal::TasksAdded),
                "the bucket's emit must carry a TasksAdded; got {:?}",
                batch.signals
            );
            primary.react_to_worker_signal_batch(batch).await;
            // Let the pump drain the queued TaskAssignments onto the wire
            // before reading them (egress is QUEUED — M4).
            settle_pump().await;

            // Drain each secondary's outgoing channel: every
            // `TaskAssignment` carries the worker id + task id, which
            // together pin the dispatch shape.
            let mut assignments: HashMap<String, Vec<(String, u32)>> = HashMap::new();
            for (sec_id, mut rx) in sec_receivers {
                let mut got: Vec<(String, u32)> = Vec::new();
                while let Ok(msg) = rx.try_recv() {
                    if let DistributedMessage::TaskAssignment {
                        target: _,
                        worker_id,
                        binary_info,
                        ..
                    } = msg
                    {
                        got.push((binary_info.task_id, worker_id));
                    }
                }
                assignments.insert(sec_id, got);
            }

            // Memory-DESC ↔ size-DESC pairing.
            for (sec_id, expected_task_id) in [
                ("sec-1024", "t_large"),
                ("sec-800", "t_big"),
                ("sec-600", "t_medium"),
                ("sec-400", "t_small"),
            ] {
                let got = assignments
                    .get(sec_id)
                    .unwrap_or_else(|| panic!("secondary {sec_id} must receive an assignment"));
                assert_eq!(
                    got.len(),
                    1,
                    "secondary {sec_id} must receive exactly one OOM-bucket assignment; got {got:?}"
                );
                let (task_id, worker_id) = &got[0];
                assert_eq!(
                    task_id, expected_task_id,
                    "secondary {sec_id} must receive its memory-DESC pair {expected_task_id}; \
                     got task {task_id}"
                );
                assert_eq!(
                    *worker_id, 0,
                    "OOM-bucket assignment to {sec_id} must land on worker 0 \
                     (single-worker mask); got worker {worker_id} task {task_id}"
                );
            }

            // Single-worker mask predicate: for each secondary,
            // worker 0 is dispatch-eligible; worker 1 is NOT.
            // (Some workers are now `is_idle = false` after the
            // kickstart, but the masking predicate is independent
            // of idle-state.)
            for worker_idx in 0..primary.workers.len() {
                let local = primary.local_worker_id_in_secondary(worker_idx);
                let expect_skip = local != 0;
                assert_eq!(
                    primary.should_skip_worker_for_dispatch(worker_idx, false),
                    expect_skip,
                    "worker idx {worker_idx} masking mismatch (expect_skip={expect_skip})"
                );
            }
        })
        .await;
}

/// Test #2: outside the OOM bucket, the normal-pass dispatch
/// pipeline applies neither the single-worker mask nor the strict
/// preferred-secondaries gate. Mixed assignments (some with
/// `preferred_secondaries`, some without) all surface in every
/// worker's view; the masking predicate returns `false` for every
/// worker.
#[tokio::test(flavor = "current_thread")]
async fn normal_pass_unmasked_when_oom_bucket_inactive() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(0);
            // No OOM failures pre-loaded → no bucket fire path.
            let tasks = vec![
                phased_task("t_no_pref", "default", 50),
                phased_task("t_pinned_a", "default", 30),
            ];
            let (mut primary, _mesh) =
                make_primed_primary(transport, /* oom_passes */ 1, tasks);
            register_secondary_with_workers(&mut primary, "sec-A", 1024, 2);
            register_secondary_with_workers(&mut primary, "sec-B", 512, 2);
            // Pin one task to sec-A through the construction path
            // a real consumer would use (TaskInfo built with the
            // preference; the OOM bucket has not run, so this is
            // user-supplied input, not bucket-written).
            primary.all_binaries[1].preferred_secondaries =
                SoftPreferredSecondaries::new(vec!["sec-A".into()]);
            // Seed the pool with both tasks. Clone out first so the
            // `pool_mut()` mutable borrow doesn't overlap with the
            // immutable read of `all_binaries`.
            let seeded = primary.all_binaries.clone();
            primary.pool_mut().extend(seeded).expect("valid extend");

            // Coordinator never entered OOM-bucket mode: flag is
            // off, mask is off everywhere, view contains every task
            // for every worker.
            assert!(
                !primary.single_worker_mode(),
                "single_worker_mode must be false outside the OOM bucket"
            );
            for worker_idx in 0..primary.workers.len() {
                assert!(
                    !primary.should_skip_worker_for_dispatch(worker_idx, false),
                    "worker idx {worker_idx} must be dispatch-eligible \
                     outside the OOM bucket"
                );
                let view = primary.dispatch_view_for_worker(worker_idx);
                assert_eq!(
                    view.as_slice().len(),
                    2,
                    "outside the OOM bucket every worker must see all queued tasks; \
                     worker idx {worker_idx} saw {} items",
                    view.as_slice().len()
                );
            }
        })
        .await;
}

/// Test #3 (flag lifecycle): `single_worker_mode` is false at
/// construction; true after `try_run_phase_retry_bucket(Oom, ...)`
/// reinjects; false again after the bucket exhausts its budget.
#[tokio::test(flavor = "current_thread")]
async fn flag_lifecycle_tracks_oom_bucket_pass() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(0);
            let tasks = vec![phased_task("t_oom", "default", 50)];
            // Budget = 1 so the second OOM-bucket pass exhausts and
            // the flag clears on that return.
            let (mut primary, _mesh) =
                make_primed_primary(transport, /* oom_passes */ 1, tasks);
            register_secondary_with_workers(&mut primary, "sec-A", 1024, 2);

            // Construction baseline.
            assert!(
                !primary.single_worker_mode(),
                "single_worker_mode must default to false at construction"
            );

            mark_all_failed_oom(&mut primary);

            // First OOM-bucket pass: reinjects the failure.
            let mut command_rx = None;
            let phase = PhaseId::from("default");
            let reinjected = primary
                .try_run_phase_retry_bucket(&phase, BucketKind::Oom, &mut command_rx)
                .await
                .expect("first OOM bucket pass must succeed");
            assert!(reinjected, "first pass must reinject");
            assert!(
                primary.single_worker_mode(),
                "single_worker_mode must be true while OOM bucket is active"
            );

            // Simulate the retry failing OOM again — `failed_tasks`
            // re-populated for the same task. The bucket call below
            // will see used (1) >= cap (1) and exhaust.
            mark_all_failed_oom(&mut primary);
            let reinjected_again = primary
                .try_run_phase_retry_bucket(&phase, BucketKind::Oom, &mut command_rx)
                .await
                .expect("second OOM bucket pass must succeed");
            assert!(
                !reinjected_again,
                "budget-exhausted bucket must return Ok(false)"
            );
            assert!(
                !primary.single_worker_mode(),
                "single_worker_mode must clear when the OOM bucket exhausts"
            );
        })
        .await;
}

/// Test #3b: the no-candidates exit path also clears the flag. If a
/// caller pre-sets the flag and then runs the OOM bucket with no
/// matching failures, the bucket short-circuits and lifts the gate
/// — otherwise a stale `true` would silently cap normal-pass
/// throughput after the bucket settled.
#[tokio::test(flavor = "current_thread")]
async fn flag_clears_on_oom_bucket_with_no_candidates() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(0);
            let tasks = vec![phased_task("t_clean", "default", 50)];
            let (mut primary, _mesh) =
                make_primed_primary(transport, /* oom_passes */ 1, tasks);
            register_secondary_with_workers(&mut primary, "sec-A", 1024, 2);

            // No `failed_tasks` entries: the OOM bucket finds no
            // candidates. Pre-set the flag to prove that even the
            // "settled clean" path resets it.
            primary.single_worker_mode = true;

            let mut command_rx = None;
            let phase = PhaseId::from("default");
            let reinjected = primary
                .try_run_phase_retry_bucket(&phase, BucketKind::Oom, &mut command_rx)
                .await
                .expect("empty-bucket pass must succeed");
            assert!(!reinjected, "empty-bucket pass must return Ok(false)");
            assert!(
                !primary.single_worker_mode(),
                "single_worker_mode must clear when the OOM bucket finds no candidates"
            );
        })
        .await;
}

/// Test #4: 5-min stuck-worker watchdog is gated by
/// `single_worker_mode`. The operational-loop arm `if
/// !self.single_worker_mode()` parks the watchdog future
/// indefinitely while the OOM bucket runs; this test pins the
/// gate at the coordinator-state level (the operational-loop arm
/// reads `self.single_worker_mode()` directly).
///
/// Approach: prove the gate's READ contract — the arm-condition
/// evaluates the same way the watchdog sees it. We can't
/// reasonably run the 5-min timer in a unit test; the
/// integration coverage in production is the
/// `operational_loop.rs:_ = sleep(300s), if !self.single_worker_mode()`
/// arm guard which is structurally tied to this flag.
#[tokio::test(flavor = "current_thread")]
async fn watchdog_arm_is_gated_by_single_worker_mode() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(0);
            let tasks = vec![phased_task("t_synth", "default", 50)];
            let (mut primary, _mesh) =
                make_primed_primary(transport, /* oom_passes */ 1, tasks);
            register_secondary_with_workers(&mut primary, "sec-A", 1024, 1);

            // Pre-bucket: watchdog arm-condition is enabled.
            assert!(
                !primary.single_worker_mode(),
                "pre-bucket: watchdog arm enabled (flag false)"
            );

            // Enter OOM bucket: arm-condition disables watchdog.
            mark_all_failed_oom(&mut primary);
            let mut command_rx = None;
            let phase = PhaseId::from("default");
            primary
                .try_run_phase_retry_bucket(&phase, BucketKind::Oom, &mut command_rx)
                .await
                .expect("OOM bucket must succeed");
            assert!(
                primary.single_worker_mode(),
                "during OOM bucket: watchdog arm disabled (flag true)"
            );
            // The arm guard's negation in `operational_loop.rs`
            // (`if !self.single_worker_mode()`) is the only
            // call-site consumer — so the flag's read here is the
            // exact signal the watchdog would see.

            // Settle the bucket: arm-condition re-enables.
            mark_all_failed_oom(&mut primary);
            primary
                .try_run_phase_retry_bucket(&phase, BucketKind::Oom, &mut command_rx)
                .await
                .expect("budget-exhaust pass must succeed");
            assert!(
                !primary.single_worker_mode(),
                "post-bucket: watchdog arm re-enabled (flag false)"
            );
        })
        .await;
}
