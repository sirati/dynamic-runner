//! P2 setup-task EXECUTOR coverage: the primary-side selector
//! (`dispatch_setup_tasks`), on-primary self-exec, off-primary assignment +
//! terminal ingest (`handle_setup_terminal`), and the end-to-end
//! "setup succeeds → dependent Work task becomes dispatchable" path.
//!
//! Deterministic — the worker-management reaction is driven directly
//! (exactly what the operational loop's arm does), the wire is read off the
//! per-secondary channel ends, and `SetupTerminal` reports are fed straight
//! into the ingest. No operational loop raced against a wall clock.

use super::*;

use dynrunner_core::{PhaseId, TaskDep, TaskKind, TypeId};

use crate::primary::wire::compute_task_hash;
use crate::worker_signal::{WorkerMgmtSignal, WorkerSignalBatch};

type TestPrimary = PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>;

/// A `Setup`-kind task on phase `work` with an optional executor affinity.
fn setup_task(name: &str, affinity: Option<&str>) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from("work");
    t.type_id = TypeId::from("default");
    t.kind = TaskKind::Setup;
    t.setup_affinity = affinity.map(str::to_string);
    t
}

/// A `Setup`-kind UPLOAD task (#336 P1): carries an upload-file ref so its
/// in-process executor performs the upload via the registered action.
fn upload_setup_task(name: &str, affinity: Option<&str>, source: &str) -> TaskInfo<TestId> {
    let mut t = setup_task(name, affinity);
    t.upload_file = Some(Box::new(dynrunner_core::UploadFileRef {
        source: std::path::PathBuf::from(source),
        dest: None,
    }));
    t
}

/// A stub `UploadAction` for the end-to-end primary tests: counts calls and
/// returns a fixed result. The trait is `Send + Sync` (so a real
/// `Arc<dyn UploadAction>` survives relocation), so the stub uses an atomic
/// counter rather than a `RefCell`.
struct StubUploader {
    calls: std::sync::atomic::AtomicUsize,
    result: Result<(), crate::upload_action::UploadError>,
}

impl StubUploader {
    fn ok() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            calls: std::sync::atomic::AtomicUsize::new(0),
            result: Ok(()),
        })
    }
    fn permanent(reason: &str) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            calls: std::sync::atomic::AtomicUsize::new(0),
            result: Err(crate::upload_action::UploadError::Permanent(reason.into())),
        })
    }
    fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait(?Send)]
impl crate::upload_action::UploadAction for StubUploader {
    async fn upload(
        &self,
        _file: &dynrunner_core::UploadFileRef,
    ) -> Result<(), crate::upload_action::UploadError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.result.clone()
    }
}

/// A `Work` task on phase `work` depending on the named setup task (same
/// phase) — the build task gated on a setup task.
fn dependent_work(name: &str, dep_setup_id: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from("work");
    t.type_id = TypeId::from("default");
    t.task_depends_on = vec![TaskDep {
        task_id: dep_setup_id.into(),
        phase_id: PhaseId::from("work"),
        inherit_outputs: false,
    }];
    t
}

/// A single-secondary primary on phase `work` whose CRDT carries `tasks`,
/// hydrated into a pool. Returns the primary + the live channel ends. No
/// worker is registered by default (a setup task never needs one); callers
/// add one only when they also exercise dependent WORK dispatch.
#[allow(clippy::type_complexity)]
fn primary_with_tasks(
    tasks: Vec<TaskInfo<TestId>>,
) -> (
    TestPrimary,
    Vec<(
        String,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
    PrimaryMeshKeepalive,
) {
    let (transport, ends) = setup_test(1);
    let (mut primary, mesh) = build_test_primary(
        PrimaryConfig::default(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    {
        let cs = primary.cluster_state_mut_for_test();
        for task in &tasks {
            cs.apply(ClusterMutation::TaskAdded {
                hash: compute_task_hash(task),
                task: task.clone(),
                def_id: None,
            });
        }
    }
    primary
        .hydrate_from_cluster_state()
        .expect("test fixture: task set is valid");
    (primary, ends, mesh)
}

/// Mark `member` a live replicated cluster member (so the selector treats
/// its affinity as routable).
fn mark_alive(primary: &mut TestPrimary, member: &str) {
    primary.cluster_state_mut_for_test().apply(ClusterMutation::PeerJoined {
        peer_id: member.into(),
        is_observer: false,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 0,
    });
}

/// Drain every `SetupAssignment` (secondary_id, task_hash) off a receiver.
fn drained_setup_assignments(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::SetupAssignment {
            secondary_id,
            task_hash,
            ..
        } = msg
        {
            out.push((secondary_id, task_hash));
        }
    }
    out
}

fn one_tasks_added_batch() -> WorkerSignalBatch {
    WorkerSignalBatch {
        signals: vec![WorkerMgmtSignal::TasksAdded],
    }
}

/// SELECTOR — routes a setup task to its CONNECTED off-primary affinity
/// member: the directed `SetupAssignment` reaches that member and the task
/// is committed `InFlight` (death-seam coverage).
#[tokio::test(flavor = "current_thread")]
async fn selector_routes_setup_to_connected_off_primary_member() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let setup = setup_task("setup-a", Some("sec-0"));
            let setup_hash = compute_task_hash(&setup);
            let (mut primary, mut ends, _mesh) = primary_with_tasks(vec![setup]);
            mark_alive(&mut primary, "sec-0");

            // Drive the worker-management reaction (the operational loop's
            // arm) — its TasksAdded branch services setup dispatch.
            primary
                .react_to_worker_signal_batch(one_tasks_added_batch(), &mut None)
                .await;
            settle_pump().await;

            // The directed SetupAssignment reached sec-0 …
            let assignments = drained_setup_assignments(&mut ends[0].1);
            assert_eq!(
                assignments,
                vec![("sec-0".to_string(), setup_hash.clone())],
                "the setup task is assigned to its connected affinity member"
            );
            // … the task is committed InFlight on the primary (death-seam
            // ledger) and is no longer queued in the pool.
            assert!(
                primary.in_flight.contains_key(&setup_hash),
                "the off-primary setup task is committed to the in-flight ledger"
            );
            assert!(
                !primary.pool().iter().any(|t| t.task_id == "setup-a"),
                "the routed setup task left the pool"
            );
        })
        .await;
}

/// SELECTOR — SKIPS a setup task whose affinity member is ABSENT (not a
/// live member): it stays queued, no assignment is sent, nothing committed.
#[tokio::test(flavor = "current_thread")]
async fn selector_skips_setup_whose_affinity_is_absent() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let setup = setup_task("setup-a", Some("ghost-member"));
            let setup_hash = compute_task_hash(&setup);
            // ghost-member is never marked alive.
            let (mut primary, mut ends, _mesh) = primary_with_tasks(vec![setup]);

            primary
                .react_to_worker_signal_batch(one_tasks_added_batch(), &mut None)
                .await;
            settle_pump().await;

            assert!(
                drained_setup_assignments(&mut ends[0].1).is_empty(),
                "no assignment is sent for an absent affinity member"
            );
            assert!(
                !primary.in_flight.contains_key(&setup_hash),
                "nothing is committed in flight for an unroutable setup task"
            );
            assert!(
                primary.pool().iter().any(|t| t.task_id == "setup-a"),
                "the setup task stays queued, holding its phase open, for a later pass"
            );
        })
        .await;
}

/// ON-PRIMARY self-exec — a setup task whose affinity is the primary itself
/// is executed in-process (no wire frame) and its `SetupCompleted` terminal
/// lands in the CRDT. No worker is involved.
#[tokio::test(flavor = "current_thread")]
async fn primary_affinity_setup_self_execs_to_setup_completed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Affinity == the primary's own id ("setup") → self-exec.
            let setup = setup_task("setup-a", Some("setup"));
            let setup_hash = compute_task_hash(&setup);
            let (mut primary, mut ends, _mesh) = primary_with_tasks(vec![setup]);

            primary
                .react_to_worker_signal_batch(one_tasks_added_batch(), &mut None)
                .await;
            settle_pump().await;

            // No wire frame — self-exec is in-process.
            assert!(
                drained_setup_assignments(&mut ends[0].1).is_empty(),
                "a primary-affinity setup task is run in-process, not sent on the wire"
            );
            // The success terminal is in the CRDT (separate setup bucket).
            assert!(
                matches!(
                    primary.cluster_state.task_state(&setup_hash),
                    Some(crate::cluster_state::TaskState::SetupCompleted { .. })
                ),
                "the self-executed setup task is terminal SetupCompleted"
            );
            assert_eq!(
                primary.cluster_state.outcome_counts().setup_succeeded,
                1,
                "the succeeded setup task counts in the separate setup bucket"
            );
            assert_eq!(
                primary.cluster_state.outcome_counts().succeeded,
                0,
                "and NOT in the worker-work succeeded bucket"
            );
        })
        .await;
}

/// `None` affinity defaults to the primary (self-exec) — a setup task with
/// no declared affinity runs on the primary.
#[tokio::test(flavor = "current_thread")]
async fn unset_affinity_defaults_to_primary_self_exec() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let setup = setup_task("setup-a", None);
            let setup_hash = compute_task_hash(&setup);
            let (mut primary, _ends, _mesh) = primary_with_tasks(vec![setup]);

            primary
                .react_to_worker_signal_batch(one_tasks_added_batch(), &mut None)
                .await;
            settle_pump().await;

            assert!(
                matches!(
                    primary.cluster_state.task_state(&setup_hash),
                    Some(crate::cluster_state::TaskState::SetupCompleted { .. })
                ),
                "an unset affinity defaults to the primary and self-execs to SetupCompleted"
            );
        })
        .await;
}

/// OFF-PRIMARY terminal ingest — a `SetupTerminal { success: true }` report
/// from the executor member originates `SetupCompleted` and frees the
/// in-flight ledger entry.
#[tokio::test(flavor = "current_thread")]
async fn off_primary_setup_terminal_success_settles_setup_completed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let setup = setup_task("setup-a", Some("sec-0"));
            let setup_hash = compute_task_hash(&setup);
            let (mut primary, mut ends, _mesh) = primary_with_tasks(vec![setup]);
            mark_alive(&mut primary, "sec-0");
            // Assign it off-primary (commits InFlight).
            primary
                .react_to_worker_signal_batch(one_tasks_added_batch(), &mut None)
                .await;
            settle_pump().await;
            let _ = drained_setup_assignments(&mut ends[0].1);
            assert!(primary.in_flight.contains_key(&setup_hash));

            // The executor reports success.
            let report = DistributedMessage::SetupTerminal {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 0.0,
                secondary_id: "sec-0".into(),
                task_hash: setup_hash.clone(),
                success: true,
                error_message: String::new(),
            };
            primary.handle_setup_terminal(report, &mut None).await;

            assert!(
                matches!(
                    primary.cluster_state.task_state(&setup_hash),
                    Some(crate::cluster_state::TaskState::SetupCompleted { .. })
                ),
                "the success report settles SetupCompleted in the CRDT"
            );
            assert!(
                !primary.in_flight.contains_key(&setup_hash),
                "the in-flight ledger entry is freed on the terminal"
            );
        })
        .await;
}

/// OFF-PRIMARY terminal ingest — a `SetupTerminal { success: false }` report
/// drives the EXISTING `TaskFailed { NonRecoverable }` terminal (shared with
/// the death seam) and cascades to a dependent Work task.
#[tokio::test(flavor = "current_thread")]
async fn off_primary_setup_terminal_failure_fails_nonrecoverable_and_cascades() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let setup = setup_task("setup-a", Some("sec-0"));
            let setup_hash = compute_task_hash(&setup);
            // The dependent is an initial-graph task (Pending in the CRDT,
            // pool-blocked on the setup prereq). On the setup failure the
            // primary's permanent-failure cascade (`on_item_failed_permanent`)
            // drops it from the pool and records it failed — the same cascade
            // a non-recoverable WORKER terminal drives.
            let build = dependent_work("build", "setup-a");
            let (mut primary, mut ends, _mesh) = primary_with_tasks(vec![setup, build]);
            mark_alive(&mut primary, "sec-0");
            primary
                .react_to_worker_signal_batch(one_tasks_added_batch(), &mut None)
                .await;
            settle_pump().await;
            let _ = drained_setup_assignments(&mut ends[0].1);

            // The executor reports failure.
            let report = DistributedMessage::SetupTerminal {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 0.0,
                secondary_id: "sec-0".into(),
                task_hash: setup_hash.clone(),
                success: false,
                error_message: "build action failed".into(),
            };
            primary.handle_setup_terminal(report, &mut None).await;

            // The setup task is terminally Failed(NonRecoverable) — the SAME
            // terminal the executor-death seam drives.
            assert!(
                matches!(
                    primary.cluster_state.task_state(&setup_hash),
                    Some(crate::cluster_state::TaskState::Failed {
                        kind: dynrunner_core::ErrorType::NonRecoverable,
                        ..
                    })
                ),
                "a failed setup task is terminal Failed(NonRecoverable)"
            );
            // Its dependent build task cascade-failed: the permanent-failure
            // cascade dropped it from the dispatch pool (it can never run
            // without its setup prereq). This is the SAME `on_item_failed_permanent`
            // cascade a non-recoverable worker terminal drives — the setup
            // failure reuses it verbatim.
            assert!(
                !primary.pool().iter().any(|t| t.task_id == "build"),
                "the dependent build task is dropped from the pool by the cascade \
                 (its setup prereq failed non-recoverably)"
            );
            // The failed setup task itself is accounted in the CRDT failure
            // count (the dependent's CRDT terminal materializes later, on the
            // run's finalize/spawn path — out of scope for this seam test).
            assert!(
                primary.failed_count() >= 1,
                "the failed setup task is accounted in the run's failure count"
            );
        })
        .await;
}

/// INTEGRATION — a setup task assigned to its affinity member executes
/// IN-PROCESS (no worker), its `SetupCompleted` terminal lands in the CRDT
/// via the off-primary report, and the dependent Work task auto-resumes
/// to dispatchable (Pending). The end-to-end primitive: build tasks gate on
/// a setup task and unblock the moment it succeeds.
#[tokio::test(flavor = "current_thread")]
async fn setup_success_unblocks_dependent_work_task_end_to_end() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let setup = setup_task("setup-a", Some("sec-0"));
            let setup_hash = compute_task_hash(&setup);
            // Seed only the setup task initially; SPAWN the dependent at
            // runtime (the `TasksSpawned` classifier lands it CRDT-`Blocked`
            // on the still-Pending setup task — the path the SetupCompleted
            // apply arm's `resume_blocked_on` unblocks).
            let (mut primary, mut ends, _mesh) = primary_with_tasks(vec![setup]);
            mark_alive(&mut primary, "sec-0");
            let build = dependent_work("build", "setup-a");
            let build_hash = compute_task_hash(&build);
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::TasksSpawned { tasks: vec![build] });

            // The spawned dependent is CRDT-Blocked on the Pending setup task.
            assert!(
                matches!(
                    primary.cluster_state.task_state(&build_hash),
                    Some(crate::cluster_state::TaskState::Blocked { .. })
                ),
                "the spawned dependent starts Blocked on the not-yet-done setup task"
            );

            // Selector routes the setup task to its in-process executor.
            primary
                .react_to_worker_signal_batch(one_tasks_added_batch(), &mut None)
                .await;
            settle_pump().await;
            let assignments = drained_setup_assignments(&mut ends[0].1);
            assert_eq!(assignments.len(), 1, "the setup task is routed to sec-0");
            // No worker was ever registered — the executor is in-process.

            // The member executes in-process and reports success.
            let report = DistributedMessage::SetupTerminal {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 0.0,
                secondary_id: "sec-0".into(),
                task_hash: setup_hash.clone(),
                success: true,
                error_message: String::new(),
            };
            primary.handle_setup_terminal(report, &mut None).await;

            // Terminal landed in the CRDT …
            assert!(matches!(
                primary.cluster_state.task_state(&setup_hash),
                Some(crate::cluster_state::TaskState::SetupCompleted { .. })
            ));
            // … and the dependent build task auto-resumed to dispatchable.
            assert!(
                matches!(
                    primary.cluster_state.task_state(&build_hash),
                    Some(crate::cluster_state::TaskState::Pending { .. })
                ),
                "the dependent Work task becomes dispatchable once its setup prereq succeeds"
            );
            // It is back in the live pool too (re-injected by the resume).
            assert!(
                primary.pool().iter().any(|t| t.task_id == "build"),
                "the resumed dependent re-enters the dispatch pool"
            );
        })
        .await;
}

// ── #336 P1: the upload-action end-to-end primary path ─────────────────

/// UPLOAD ACTION (on-primary self-exec) — a file-setup-task whose affinity
/// is the primary fires the registered upload callback, settles
/// `SetupCompleted`, and its dependent Work task becomes dispatchable. The
/// gating/overlap fall out of the #489 dep model (mirrors
/// `setup_success_unblocks_dependent_work_task_end_to_end`, but the action
/// is a real upload, not the no-op).
#[tokio::test(flavor = "current_thread")]
async fn upload_setup_task_fires_callback_then_unblocks_dependent() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Affinity == the primary's own id ("setup") → self-exec.
            let upload = upload_setup_task("up-a", Some("setup"), "/src/libfoo.a");
            let upload_hash = compute_task_hash(&upload);
            let (mut primary, _ends, _mesh) = primary_with_tasks(vec![upload]);
            let uploader = StubUploader::ok();
            primary.set_upload_action(uploader.clone());

            // The dependent build task is spawned Blocked on the upload.
            let build = dependent_work("build", "up-a");
            let build_hash = compute_task_hash(&build);
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::TasksSpawned { tasks: vec![build] });

            primary
                .react_to_worker_signal_batch(one_tasks_added_batch(), &mut None)
                .await;
            settle_pump().await;

            // The upload callback fired exactly once for this task …
            assert_eq!(
                uploader.calls(),
                1,
                "the upload action is invoked for the file-setup-task"
            );
            // … the setup task settled SetupCompleted (separate bucket) …
            assert!(
                matches!(
                    primary.cluster_state.task_state(&upload_hash),
                    Some(crate::cluster_state::TaskState::SetupCompleted { .. })
                ),
                "a successful upload settles SetupCompleted"
            );
            assert_eq!(primary.cluster_state.outcome_counts().setup_succeeded, 1);
            // … and the dependent Work task auto-resumed to dispatchable.
            assert!(
                matches!(
                    primary.cluster_state.task_state(&build_hash),
                    Some(crate::cluster_state::TaskState::Pending { .. })
                ),
                "the dependent work task unblocks once its upload setup task succeeds"
            );
        })
        .await;
}

/// UPLOAD ACTION permanent failure — a permanent transfer failure drives the
/// EXISTING `TaskFailed { NonRecoverable }` terminal (distinct from the
/// no-op gate's death semantics) and cascades to the dependent Work task.
#[tokio::test(flavor = "current_thread")]
async fn upload_setup_task_permanent_failure_cascades() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upload = upload_setup_task("up-b", Some("setup"), "/src/missing.a");
            let upload_hash = compute_task_hash(&upload);
            let build = dependent_work("build", "up-b");
            let (mut primary, _ends, _mesh) = primary_with_tasks(vec![upload, build]);
            let uploader = StubUploader::permanent("source missing on submitter");
            primary.set_upload_action(uploader.clone());

            primary
                .react_to_worker_signal_batch(one_tasks_added_batch(), &mut None)
                .await;
            settle_pump().await;

            assert_eq!(uploader.calls(), 1, "the permanent failure is not retried");
            // The upload setup task is terminal Failed(NonRecoverable).
            assert!(
                matches!(
                    primary.cluster_state.task_state(&upload_hash),
                    Some(crate::cluster_state::TaskState::Failed {
                        kind: dynrunner_core::ErrorType::NonRecoverable,
                        ..
                    })
                ),
                "a permanent upload failure is terminal Failed(NonRecoverable)"
            );
            // Its dependent build cascade-failed (dropped from the pool).
            assert!(
                !primary.pool().iter().any(|t| t.task_id == "build"),
                "the dependent build is dropped by the permanent-failure cascade"
            );
        })
        .await;
}

// ── #336 P3: priority-ordered (min-by-rank) setup-upload routing ───────────

/// A `Work` build on `phase`/`affinity` depending on `dep` — same shape as
/// [`dependent_work`] but with an explicit affinity so the dependent's
/// dispatch-rank class (typed vs free-pool) is controllable.
fn dependent_build(name: &str, phase: &str, affinity: Option<&str>, dep: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from(phase);
    t.type_id = TypeId::from("default");
    t.affinity_id = affinity.map(dynrunner_core::AffinityId::from);
    t.task_depends_on = vec![TaskDep {
        task_id: dep.into(),
        phase_id: PhaseId::from(phase),
        inherit_outputs: false,
    }];
    t
}

/// A `SecondaryAffine` import gate on `phase` depending on `dep` (#497).
fn affine_import(name: &str, phase: &str, dep: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from(phase);
    t.type_id = TypeId::from("default");
    t.kind = TaskKind::SecondaryAffine;
    t.task_depends_on = vec![TaskDep {
        task_id: dep.into(),
        phase_id: PhaseId::from(phase),
        inherit_outputs: false,
    }];
    t
}

/// PRIORITY ROUTING (vs FIFO) — two routable upload setup tasks A and B (both
/// affine to one connected member, so both route over the wire in PICK order).
/// A is seeded FIRST (FIFO would route A→B) but B feeds a dispatch-imminent
/// (typed-affinity, Active phase) build while A feeds a free-pool build, so B
/// out-ranks A and routes BEFORE it (min-by-rank).
#[tokio::test(flavor = "current_thread")]
async fn routes_higher_ranked_upload_before_lower_ranked_fifo_earlier() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // A queued first (FIFO would pick A first). Both affine to sec-0.
            let up_a = setup_task("up-a", Some("sec-0"));
            let up_b = setup_task("up-b", Some("sec-0"));
            let a_hash = compute_task_hash(&up_a);
            let b_hash = compute_task_hash(&up_b);
            let (mut primary, mut ends, _mesh) = primary_with_tasks(vec![up_a, up_b]);
            mark_alive(&mut primary, "sec-0");

            // Seed the dependents directly into the pool's blocked map (they
            // never dispatch — they only feed the rank). B's build is typed
            // (affinity "x") → class_tier 0; A's build is free-pool → 1. Both
            // on the Active "work" phase.
            primary
                .pool_mut()
                .extend(vec![
                    dependent_build("b-build", "work", Some("x"), "up-b"),
                    dependent_build("a-build", "work", None, "up-a"),
                ])
                .expect("valid extend");

            // Drive the setup-dispatch pass — it drains BOTH routable uploads
            // in PICK order onto sec-0's channel.
            primary
                .react_to_worker_signal_batch(one_tasks_added_batch(), &mut None)
                .await;
            settle_pump().await;

            let assignments = drained_setup_assignments(&mut ends[0].1);
            let order: Vec<&String> = assignments.iter().map(|(_, h)| h).collect();
            assert_eq!(
                order,
                vec![&b_hash, &a_hash],
                "B (typed-affinity dependent) routes BEFORE A (free-pool dependent), \
                 NOT the FIFO A-then-B order"
            );
        })
        .await;
}

/// TRANSITIVE-THROUGH-AFFINE (#497) — B's build is gated on B's upload only
/// through a `SecondaryAffine` import gate (upload → import → build); the rank
/// walk recurses through the import to the build, so B (whose transitive build
/// is Active+typed) still out-ranks A (whose direct build is free-pool). Routes
/// B before A.
#[tokio::test(flavor = "current_thread")]
async fn routes_upload_by_transitive_through_affine_import() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let up_a = setup_task("up-a", Some("sec-0"));
            let up_b = setup_task("up-b", Some("sec-0"));
            let a_hash = compute_task_hash(&up_a);
            let b_hash = compute_task_hash(&up_b);
            let (mut primary, mut ends, _mesh) = primary_with_tasks(vec![up_a, up_b]);
            mark_alive(&mut primary, "sec-0");

            // up-b → import (SecondaryAffine) → b-build (Work, typed, Active).
            // up-a → a-build (Work, free-pool, Active) directly.
            primary
                .pool_mut()
                .extend(vec![
                    affine_import("import", "work", "up-b"),
                    dependent_build("b-build", "work", Some("x"), "import"),
                    dependent_build("a-build", "work", None, "up-a"),
                ])
                .expect("valid extend");

            primary
                .react_to_worker_signal_batch(one_tasks_added_batch(), &mut None)
                .await;
            settle_pump().await;

            let assignments = drained_setup_assignments(&mut ends[0].1);
            let order: Vec<&String> = assignments.iter().map(|(_, h)| h).collect();
            assert_eq!(
                order,
                vec![&b_hash, &a_hash],
                "B routes before A even though B's build is reachable only TRANSITIVELY \
                 through the affine import gate"
            );
        })
        .await;
}

/// DEP-MODEL INVARIANT — reordering only ROUTES; it never strands a dependent.
/// After the higher-ranked upload routes FIRST and reports SetupCompleted, its
/// dependent unblocks (per-file SetupCompleted → resume) to dispatchable; the
/// lower-ranked upload is still routed (not dropped) and its dependent still
/// unblocks on ITS completion. Pure ordering: the mutation kind + per-file
/// granularity are unchanged.
#[tokio::test(flavor = "current_thread")]
async fn reorder_routes_both_and_strands_no_dependent() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Both affine to the primary itself → self-exec synchronously, so
            // each SetupCompleted (and its dependent resume) settles in-pass.
            // up-b's dependent is typed (ranks first); up-a's is free-pool.
            let up_a = setup_task("up-a", Some("setup"));
            let up_b = setup_task("up-b", Some("setup"));
            let a_hash = compute_task_hash(&up_a);
            let b_hash = compute_task_hash(&up_b);
            let (mut primary, _ends, _mesh) = primary_with_tasks(vec![up_a, up_b]);

            // The dependents, spawned CRDT-Blocked on their uploads (the live
            // resume surface the per-file SetupCompleted apply unblocks).
            let b_build = dependent_build("b-build", "work", Some("x"), "up-b");
            let a_build = dependent_build("a-build", "work", None, "up-a");
            let b_build_hash = compute_task_hash(&b_build);
            let a_build_hash = compute_task_hash(&a_build);
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::TasksSpawned {
                    tasks: vec![b_build, a_build],
                });

            // Drive the dispatch pass: both uploads self-exec to SetupCompleted.
            primary
                .react_to_worker_signal_batch(one_tasks_added_batch(), &mut None)
                .await;
            settle_pump().await;

            // BOTH uploads routed (self-exec'd to SetupCompleted) — neither
            // dropped by the reorder. The mutation kind is the UNCHANGED
            // SetupCompleted (the success terminal), per-file, for each.
            for h in [&a_hash, &b_hash] {
                assert!(
                    matches!(
                        primary.cluster_state.task_state(h),
                        Some(crate::cluster_state::TaskState::SetupCompleted { .. })
                    ),
                    "every routed upload settles the UNCHANGED SetupCompleted terminal"
                );
            }
            // And NEITHER dependent is stranded — each unblocked to dispatchable
            // (Pending) once ITS upload completed.
            for h in [&a_build_hash, &b_build_hash] {
                assert!(
                    matches!(
                        primary.cluster_state.task_state(h),
                        Some(crate::cluster_state::TaskState::Pending { .. })
                    ),
                    "each dependent unblocks to dispatchable after its upload — none stranded"
                );
            }
        })
        .await;
}
