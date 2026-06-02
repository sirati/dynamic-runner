use std::collections::HashMap;
use std::time::Duration;

use dynrunner_core::ErrorType;
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_scheduler_api::PendingPool;
use dynrunner_transport_channel::ChannelPeerTransport;
use tokio::sync::oneshot;

use crate::cluster_state::TaskState;
use crate::primary::command_channel::{
    handle_primary_command, PrimaryCommand,
};
use crate::primary::test_helpers::{
    make_binary, setup_test, FixedEstimator, RecordingPeer, TestId,
};
use crate::primary::wire::compute_task_hash;
use crate::primary::{PrimaryConfig, PrimaryCoordinator};
use crate::state::{SecondaryConnection, SecondaryConnectionState};

/// Stand-alone fixture matching the shape used by
/// `command_channel::tests::make_coordinator`: a `PrimaryCoordinator`
/// over an in-process channel transport stub with zero connected
/// secondaries, suitable for driving `run_retry_passes` /
/// `apply_reinject_task` directly without a full operational loop.
fn make_coordinator(
    retry_max_passes: u32,
) -> PrimaryCoordinator<
    ChannelPeerTransport<TestId>,
    ResourceStealingScheduler,
    FixedEstimator,
    TestId,
> {
    let (transport, _secondary_ends) = setup_test(0);
    let config = PrimaryConfig {
        node_id: "primary".into(),
        num_secondaries: 0,
        connect_timeout: Duration::from_secs(1),
        peer_timeout: Duration::from_secs(1),
        keepalive_interval: Duration::from_millis(100),
        keepalive_miss_threshold: 3,
        source_pre_staged_root: None,
        uses_file_based_items: false,
        required_setup_on_promote: false,
        max_concurrent_per_type: HashMap::new(),
        retry_max_passes,
        oom_retry_max_passes: retry_max_passes,
        fleet_dead_timeout: Duration::from_secs(1),
        mesh_ready_timeout: Duration::from_secs(1),
        mass_death_grace: Duration::from_secs(1),
        mass_death_min_count: 2,
        source_dir: None,
        unfulfillable_reinject_max_per_task: None,
        setup_promote_deadline: std::time::Duration::from_secs(600),
    };
    PrimaryCoordinator::new(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    )
}

/// Seed `coordinator.pending` with a default-phase pool that owns
/// the supplied binary's phase. Required so `pool.reinject(binary)`
/// has a phase entry to flip back to Active.
fn install_pool_for_phase(
    coordinator: &mut PrimaryCoordinator<
        ChannelPeerTransport<TestId>,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
    binary: &dynrunner_core::TaskInfo<TestId>,
) {
    let mut phase_set = std::collections::HashSet::new();
    phase_set.insert(binary.phase_id.clone());
    coordinator.pending = Some(
        PendingPool::<TestId>::new(phase_set, HashMap::new())
            .expect("pool init"),
    );
    coordinator
        .phase_completed
        .insert(binary.phase_id.clone(), 0);
    coordinator
        .phase_failed
        .insert(binary.phase_id.clone(), 0);
}

/// Build a `PrimaryCoordinator` whose SINGLE transport is a
/// [`RecordingPeer`], returning the coordinator, the shared broadcast
/// log, and the secondary-end handles from `setup_test`. Post-collapse
/// the keepalive emits over the one `Tr` transport, so the recorder IS
/// that transport — every keepalive lands in the shared log. The
/// `RecordingPeer`'s `recv_peer()` parks forever, so the
/// `wait_for_mesh_ready` select's heartbeat-tick arm is what fires
/// (the secondary-end handles are returned for callers that want to
/// keep them, but the recorder's parked recv no longer depends on
/// them). The `setup_test` channels stand in for those ends.
///
/// `keepalive_interval` is short and `mesh_ready_timeout` is a few
/// keepalive intervals so the pre-operational keepalive arm has room to
/// tick at least once before the wait times out — the emission-lifetime
/// invariant under test. Both tests drive `tokio::time` paused.
#[allow(clippy::type_complexity)]
fn make_recording_coordinator(
    num_secondaries: u32,
    keepalive_interval: Duration,
    mesh_ready_timeout: Duration,
) -> (
    PrimaryCoordinator<
        RecordingPeer<TestId>,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
    std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>,
    Vec<(
        String,
        tokio::sync::mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio::sync::mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
) {
    let (_transport, secondary_ends) = setup_test(num_secondaries);
    let config = PrimaryConfig {
        node_id: "primary".into(),
        num_secondaries,
        connect_timeout: Duration::from_secs(1),
        peer_timeout: Duration::from_secs(1),
        keepalive_interval,
        keepalive_miss_threshold: 3,
        source_pre_staged_root: None,
        uses_file_based_items: false,
        required_setup_on_promote: false,
        max_concurrent_per_type: HashMap::new(),
        retry_max_passes: 0,
        oom_retry_max_passes: 0,
        fleet_dead_timeout: Duration::from_secs(1),
        mesh_ready_timeout,
        mass_death_grace: Duration::from_secs(1),
        mass_death_min_count: 2,
        source_dir: None,
        unfulfillable_reinject_max_per_task: None,
        setup_promote_deadline: Duration::from_secs(600),
    };
    let recorder = RecordingPeer::<TestId>::new();
    let log = recorder.log_handle();
    let coordinator = PrimaryCoordinator::new(
        config,
        recorder,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    (coordinator, log, secondary_ends)
}

/// Register a secondary in the primary's routable set so the keepalive
/// emitter does not early-return on an empty fleet. The pre-welcome
/// `AwaitingWelcome` state is enough for the emission assertion: the
/// emitter only checks `secondaries.is_empty()` and reads
/// `self.config.node_id` + `self.workers` — it does not depend on the
/// connection's typestate.
fn seed_secondary(
    coordinator: &mut PrimaryCoordinator<
        RecordingPeer<TestId>,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
    secondary_id: &str,
) {
    coordinator.secondaries.insert(
        secondary_id.into(),
        SecondaryConnectionState::AwaitingWelcome(SecondaryConnection::new(
            secondary_id.into(),
        )),
    );
}

/// Count the `Keepalive` messages in a recorded broadcast log.
fn count_keepalives(log: &std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>) -> usize {
    log.borrow()
        .iter()
        .filter(|m| matches!(m, DistributedMessage::Keepalive { .. }))
        .count()
}

/// Emission-lifetime invariant (sub-fix A), convergence point:
/// `activate_local_primary` is the single point both bootstrap and
/// failover reach when this node asserts primary authority. It MUST emit
/// one keepalive so a just-promoted/just-bootstrapped primary is not
/// silent over the authority↔worker link until the operational loop's
/// first `heartbeat_tick` fires. Asserts the emission CALL was issued
/// over the peer transport (delivery is not asserted — for the parked
/// failover primary the transport is a no-op until A-M2 swaps it).
#[tokio::test(flavor = "current_thread")]
async fn activate_local_primary_emits_a_keepalive() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, log, _ends) = make_recording_coordinator(
                1,
                Duration::from_millis(100),
                Duration::from_secs(1),
            );
            seed_secondary(&mut coordinator, "sec-0");

            assert_eq!(
                count_keepalives(&log),
                0,
                "no keepalive should have been emitted before activation"
            );

            coordinator
                .activate_local_primary()
                .await
                .expect("activation succeeds");

            assert_eq!(
                count_keepalives(&log),
                1,
                "activate_local_primary must emit exactly one keepalive at the \
                 authority convergence point"
            );
        })
        .await;
}

/// Initial-setup-done / first-operational important event: the single
/// bootstrap+failover convergence point (`activate_local_primary`) emits
/// exactly one "co-located primary activated" line on the importance
/// marker target so the dual-sink can surface it on stdio under
/// `--important-stdio-only`.
#[tokio::test(flavor = "current_thread")]
async fn activate_local_primary_emits_initial_setup_done_important_event() {
    use crate::test_capture::{important_only, ImportantCapture};
    use tracing::subscriber::set_default;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::{Layer, Registry};

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _log, _ends) = make_recording_coordinator(
                1,
                Duration::from_millis(100),
                Duration::from_secs(1),
            );
            seed_secondary(&mut coordinator, "sec-0");

            let capture = ImportantCapture::default();
            let subscriber =
                Registry::default().with(capture.clone().with_filter(important_only()));
            // `set_default` holds the subscriber across the `.await`
            // inside `activate_local_primary` (current-thread runtime).
            let _guard = set_default(subscriber);

            coordinator
                .activate_local_primary()
                .await
                .expect("activation succeeds");

            let msgs = capture.messages();
            assert_eq!(
                msgs.len(),
                1,
                "exactly one initial-setup-done important event: {msgs:?}"
            );
            assert!(
                msgs[0].contains("activated as sole authority"),
                "{msgs:?}"
            );
        })
        .await;
}

/// Emission-lifetime invariant (sub-fix A), pre-operational window: the
/// bootstrap region (`perform_initial_assignment → wait_for_mesh_ready →
/// activate_local_primary → operational_loop`) can outlast the
/// secondary's primary-silence deadline while the mesh forms, yet only
/// the operational loop used to tick keepalives. `wait_for_mesh_ready`
/// must tick the SAME emitter so liveness is asserted across the wait.
///
/// Driven with paused time: one secondary is in the routable set but NOT
/// in `mesh_ready_secondaries`, so the wait enters its loop and blocks
/// until the mesh-ready timeout. Holding the secondary-end senders keeps
/// `transport.recv()` pending, so the heartbeat-tick arm is what fires.
/// Asserts at least one keepalive was emitted before the timeout returns.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn wait_for_mesh_ready_ticks_keepalive() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // keepalive every 100ms, mesh-ready timeout at 350ms → at
            // least three keepalive ticks fit before the wait gives up.
            let (mut coordinator, log, _ends) = make_recording_coordinator(
                1,
                Duration::from_millis(100),
                Duration::from_millis(350),
            );
            seed_secondary(&mut coordinator, "sec-0");

            // `sec-0` never reports MeshReady → the wait blocks on its
            // select! until the mesh-ready deadline elapses, ticking the
            // pre-operational keepalive arm in the meantime.
            let mut no_cmd_rx: Option<
                tokio::sync::mpsc::Receiver<PrimaryCommand<TestId>>,
            > = None;
            coordinator
                .wait_for_mesh_ready(&mut no_cmd_rx)
                .await
                .expect("wait returns on the mesh-ready timeout");

            assert!(
                count_keepalives(&log) >= 1,
                "wait_for_mesh_ready must tick the keepalive emitter across the \
                 pre-operational window; got {}",
                count_keepalives(&log)
            );
        })
        .await;
}

/// The per-phase retry bucket primitive must NOT reinject entries
/// whose `ErrorType` is `Unfulfillable { .. }`. Those are the
/// operator-resolvable failure class — `TaskState::Unfulfillable` in
/// the CRDT — and reinjection is reserved for the explicit
/// `PrimaryCommand::ReinjectTask` path. The bucket's partition
/// predicate (`BucketKind::Recoverable`) only matches
/// `ErrorType::Recoverable`; everything else stays in `failed_tasks`.
///
/// This was historically owned by the post-pipeline `run_retry_passes`
/// (now a no-op). The semantic moved into
/// [`PrimaryCoordinator::try_run_phase_retry_bucket`] with the 2026-05-17
/// per-phase redesign; the assertions are unchanged because the
/// failure-bucket partition has not.
#[tokio::test(flavor = "current_thread")]
async fn retry_bucket_skips_unfulfillable_failures() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let mut coordinator = make_coordinator(/* retry_max_passes = */ 3);

        // Seed two binaries in the same phase: one Unfulfillable, one
        // Recoverable. `all_binaries` is the lookup table the bucket
        // primitive uses to map hashes back to dispatchable
        // `TaskInfo`s.
        let unfulfillable_bin = make_binary("operator-only", 50);
        let recoverable_bin = make_binary("retriable", 40);
        let unfulfillable_hash = compute_task_hash(&unfulfillable_bin);
        let recoverable_hash = compute_task_hash(&recoverable_bin);
        coordinator.all_binaries =
            vec![unfulfillable_bin.clone(), recoverable_bin.clone()];

        install_pool_for_phase(&mut coordinator, &unfulfillable_bin);

        coordinator.failed_tasks.insert(
            unfulfillable_hash.clone(),
            ErrorType::Unfulfillable {
                reason: "missing toolchain".to_string().into(),
            },
        );
        coordinator
            .failed_tasks
            .insert(recoverable_hash.clone(), ErrorType::Recoverable);

        // Drive the Recoverable bucket directly. The bucket now EMITS a
        // `TasksAdded` instead of dispatching inline; with no worker-
        // management receiver installed the emit is a silent no-op, but
        // the partition + reinject step happens unconditionally.
        let phase = unfulfillable_bin.phase_id.clone();
        let mut no_cmd_rx: Option<tokio::sync::mpsc::Receiver<PrimaryCommand<TestId>>> =
            None;
        let reinjected = coordinator
            .try_run_phase_retry_bucket(
                &phase,
                crate::primary::retry_bucket::BucketKind::Recoverable,
                &mut no_cmd_rx,
            )
            .await
            .expect("retry bucket runs cleanly");
        assert!(reinjected, "Recoverable failure should reinject");

        // Retriable entry was drained from the failed-set into the
        // pool; the Unfulfillable entry stayed because the
        // Recoverable bucket's predicate doesn't match it.
        assert!(
            !coordinator.failed_tasks.contains_key(&recoverable_hash),
            "retry bucket should drain Recoverable entries from \
             failed_tasks before reinjecting"
        );
        match coordinator.failed_tasks.get(&unfulfillable_hash) {
            Some(ErrorType::Unfulfillable { reason }) => {
                assert_eq!(reason.as_ref(), "missing toolchain");
            }
            other => panic!(
                "Unfulfillable entry must remain in failed_tasks; \
                 got {other:?}"
            ),
        }
    }).await;
}

/// Pin the cleanup invariant ReinjectTask depends on: the local
/// `failed_tasks` HashMap entry for a hash is removed when
/// `apply_reinject_task` transitions the CRDT from
/// `TaskState::Unfulfillable` back to `Pending`. Without this,
/// the operational loop's `completed + failed >= total` exit
/// would trip on a hash that's been re-armed for dispatch — and
/// any subsequent `run_retry_passes` pass would see a stale entry
/// claiming the task still owes a retry.
#[tokio::test(flavor = "current_thread")]
async fn reinject_clears_failed_tasks_entry_for_hash() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let mut coordinator = make_coordinator(/* retry_max_passes = */ 0);

        let binary = make_binary("op-resolvable", 50);
        let hash = compute_task_hash(&binary);
        install_pool_for_phase(&mut coordinator, &binary);
        coordinator.all_binaries = vec![binary.clone()];

        // Pre-state: worker reported Unfulfillable. The CRDT
        // lands in `TaskState::Unfulfillable`; the local
        // `failed_tasks` mirror records the same kind.
        coordinator.cluster_state.apply(
            dynrunner_protocol_primary_secondary::ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: binary.clone(),
            },
        );
        coordinator.cluster_state.apply(
            dynrunner_protocol_primary_secondary::ClusterMutation::TaskFailed {
                hash: hash.clone(),
                kind: ErrorType::Unfulfillable {
                    reason: "missing toolchain".to_string().into(),
                },
                error: "unfulfillable".into(),
            },
        );
        coordinator.failed_tasks.insert(
            hash.clone(),
            ErrorType::Unfulfillable {
                reason: "missing toolchain".to_string().into(),
            },
        );

        // Operator dispatches the reinject command.
        let (reply_tx, reply_rx) = oneshot::channel();
        handle_primary_command(
            &mut coordinator,
            PrimaryCommand::ReinjectTask {
                hash: hash.clone(),
                reply: reply_tx,
            },
            &mut None,
        )
        .await;
        assert!(
            reply_rx.await.unwrap().is_ok(),
            "reinject should accept Unfulfillable entry"
        );

        // Post-state: the local `failed_tasks` mirror no longer
        // claims this hash failed — the operational loop's
        // exit-counter sees the entry as in-flight / pending
        // again, matching the CRDT's transition to Pending.
        assert!(
            !coordinator.failed_tasks.contains_key(&hash),
            "reinject must clear failed_tasks[hash]"
        );
        assert!(matches!(
            coordinator.cluster_state.task_state(&hash),
            Some(TaskState::Pending { .. })
        ));
    }).await;
}

/// Full round-trip: Unfulfillable failure → operator reinjects →
/// task fails again as Recoverable → next `run_retry_passes` pass
/// picks the hash up exactly like any other Recoverable failure.
/// Pins that the per-task retry channel is independent of the
/// per-task ReinjectTask channel — burning the operator's
/// `unfulfillable_reinject_remaining` ticket does not consume
/// any of the run-wide pass-counter budget, and the new
/// `ErrorType::Recoverable` entry rides the retry pass with a
/// fresh ledger ErrorType (no Unfulfillable carry-over).
#[tokio::test(flavor = "current_thread")]
async fn unfulfillable_reinjected_task_can_use_retry_pass() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let mut coordinator = make_coordinator(/* retry_max_passes = */ 2);

        let binary = make_binary("round-trip", 50);
        let hash = compute_task_hash(&binary);
        install_pool_for_phase(&mut coordinator, &binary);
        coordinator.all_binaries = vec![binary.clone()];

        // Step 1: Unfulfillable failure observed.
        coordinator.cluster_state.apply(
            dynrunner_protocol_primary_secondary::ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: binary.clone(),
            },
        );
        coordinator.cluster_state.apply(
            dynrunner_protocol_primary_secondary::ClusterMutation::TaskFailed {
                hash: hash.clone(),
                kind: ErrorType::Unfulfillable {
                    reason: "missing toolchain".to_string().into(),
                },
                error: "unfulfillable".into(),
            },
        );
        coordinator.failed_tasks.insert(
            hash.clone(),
            ErrorType::Unfulfillable {
                reason: "missing toolchain".to_string().into(),
            },
        );

        // Step 2: operator reinjects.
        let (reply_tx, reply_rx) = oneshot::channel();
        handle_primary_command(
            &mut coordinator,
            PrimaryCommand::ReinjectTask {
                hash: hash.clone(),
                reply: reply_tx,
            },
            &mut None,
        )
        .await;
        assert!(reply_rx.await.unwrap().is_ok());
        assert!(!coordinator.failed_tasks.contains_key(&hash));

        // Step 3: task re-runs and fails Recoverably this time
        // (the operator's resource provisioning worked, but the
        // re-attempted execution hit a generic transient error).
        // The CRDT-side state machine takes Pending → Failed{
        // Recoverable } via the Pending arm of TaskFailed.
        coordinator.cluster_state.apply(
            dynrunner_protocol_primary_secondary::ClusterMutation::TaskFailed {
                hash: hash.clone(),
                kind: ErrorType::Recoverable,
                error: "transient".into(),
            },
        );
        coordinator
            .failed_tasks
            .insert(hash.clone(), ErrorType::Recoverable);

        // Step 4: per-phase Recoverable bucket drains the entry
        // into the pool. The fresh Recoverable kind — not the
        // carried Unfulfillable — is what determines bucket
        // eligibility.
        let mut no_cmd_rx: Option<tokio::sync::mpsc::Receiver<PrimaryCommand<TestId>>> =
            None;
        let reinjected = coordinator
            .try_run_phase_retry_bucket(
                &binary.phase_id,
                crate::primary::retry_bucket::BucketKind::Recoverable,
                &mut no_cmd_rx,
            )
            .await
            .expect("retry bucket runs cleanly");
        assert!(reinjected, "Recoverable failure should reinject");

        // The hash was drained (no zero-worker re-failure can
        // re-populate it), confirming the retry bucket picked it up.
        assert!(
            !coordinator.failed_tasks.contains_key(&hash),
            "Recoverable failure on a previously-reinjected hash \
             must still be retry-bucket-eligible"
        );
    }).await;
}
