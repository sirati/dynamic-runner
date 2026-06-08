use std::collections::HashMap;
use std::time::Duration;

use dynrunner_core::ErrorType;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage, RemovalCause};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_scheduler_api::PendingPool;
use tokio::sync::oneshot;

use crate::cluster_state::TaskState;
use crate::primary::command_channel::{PrimaryCommand, handle_primary_command};
use crate::primary::test_helpers::{
    FixedEstimator, PrimaryMeshKeepalive, RecordingPeer, TestId, build_test_primary, make_binary,
    setup_test,
};
use crate::primary::wire::compute_task_hash;
use crate::primary::{PrimaryConfig, PrimaryCoordinator};
use crate::process::{LocalRole, Mesh};
use crate::state::{SecondaryConnection, SecondaryConnectionState};
use dynrunner_protocol_primary_secondary::address::PeerId;

/// Keeps the spawned mesh-pump + demote sender + pump control handle alive
/// for a [`PrimaryCoordinator`] built over a [`RecordingPeer`]. These
/// keepalive / announce-emission tests assert on what the coordinator
/// broadcasts, which only reaches the recorder once the production mesh-pump
/// (`crate::process::pump::run_pump`) drains the queued egress onto the
/// transport — so the fixture spawns that pump exactly as `build_test_primary`
/// does. The pump task OWNS the slot `Arc`; this guard holds the control
/// handle (so the pump's control arm stays open) and aborts the pump on drop.
struct RecordingMeshKeepalive {
    _demote_tx: tokio::sync::mpsc::UnboundedSender<()>,
    _control: crate::process::MeshControlHandle<TestId>,
    pump: tokio::task::JoinHandle<()>,
}

impl Drop for RecordingMeshKeepalive {
    fn drop(&mut self) {
        self.pump.abort();
    }
}

/// Stand-alone fixture matching the shape used by
/// `command_channel::tests::make_coordinator`: a `PrimaryCoordinator`
/// over an in-process channel transport stub with zero connected
/// secondaries, suitable for driving `run_retry_passes` /
/// `apply_reinject_task` directly without a full operational loop.
fn make_coordinator(
    retry_max_passes: u32,
) -> (
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    PrimaryMeshKeepalive,
) {
    let (transport, _secondary_ends) = setup_test(0);
    let config = PrimaryConfig {
        node_id: "setup".into(),
        num_secondaries: 0,
        connect_timeout: Duration::from_secs(1),
        peer_timeout: Duration::from_secs(1),
        keepalive_interval: Duration::from_millis(100),
        keepalive_miss_threshold: 3,
        source_pre_staged_root: None,
        uses_file_based_items: false,
        max_concurrent_per_type: HashMap::new(),
        retry_max_passes,
        oom_retry_max_passes: retry_max_passes,
        fleet_dead_timeout: Duration::from_secs(1),
        mesh_ready_timeout: Duration::from_secs(1),
        source_dir: None,
        unfulfillable_reinject_max_per_task: None,
        ..PrimaryConfig::default()
    };
    build_test_primary(
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
    coordinator: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    binary: &dynrunner_core::TaskInfo<TestId>,
) {
    let mut phase_set = std::collections::HashSet::new();
    phase_set.insert(binary.phase_id.clone());
    coordinator.pending =
        Some(PendingPool::<TestId>::new(phase_set, HashMap::new()).expect("pool init"));
    // Per-phase EVENT tallies are the replicated CRDT `phase_event_tallies`
    // now (F4); a never-incremented phase reads 0 via the accessor, so no
    // pre-seed-to-0 is needed.
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
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>,
    Vec<(
        String,
        tokio::sync::mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio::sync::mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
    RecordingMeshKeepalive,
) {
    let (_transport, secondary_ends) = setup_test(num_secondaries);
    let config = PrimaryConfig {
        node_id: "setup".into(),
        num_secondaries,
        connect_timeout: Duration::from_secs(1),
        peer_timeout: Duration::from_secs(1),
        keepalive_interval,
        keepalive_miss_threshold: 3,
        source_pre_staged_root: None,
        uses_file_based_items: false,
        max_concurrent_per_type: HashMap::new(),
        retry_max_passes: 0,
        oom_retry_max_passes: 0,
        fleet_dead_timeout: Duration::from_secs(1),
        mesh_ready_timeout,
        source_dir: None,
        unfulfillable_reinject_max_per_task: None,
        ..PrimaryConfig::default()
    };
    let recorder = RecordingPeer::<TestId>::new();
    let log = recorder.log_handle();
    // Mint the mesh trio over the recording transport, then spawn the
    // PRODUCTION mesh-pump over the mesh — exactly as `build_test_primary`
    // does. The pump drains the coordinator's QUEUED egress (M4) onto the
    // recorder, so a `client.send` (keepalive / PrimaryChanged announce) lands
    // in the shared log once the pump is scheduled; the tests `settle_pump()`
    // before reading the log. MUST be called inside a `LocalSet` (the pump is
    // `spawn_local`'d), which both callers are.
    let mut mesh = Mesh::new(recorder);
    let (slot, client, inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from(config.node_id.as_str()));
    let (demote_tx, demote_rx) = tokio::sync::mpsc::unbounded_channel();
    let coordinator = PrimaryCoordinator::new(
        config,
        client,
        inbox,
        demote_rx,
        crate::primary::RelocationPolicy::StayLocal,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    // Publish live membership before the pump spawns (the pump republishes
    // every cycle thereafter), then hand the mesh to the production pump. The
    // pump task OWNS the slot `Arc` for its lifetime, mirroring the node.
    mesh.publish_membership();
    let (control, control_rx) = crate::process::pump::control_channel::<TestId>();
    let pump = tokio::task::spawn_local(async move {
        let _slot = slot;
        crate::process::pump::run_pump(mesh, control_rx).await;
    });
    (
        coordinator,
        log,
        secondary_ends,
        RecordingMeshKeepalive {
            _demote_tx: demote_tx,
            _control: control,
            pump,
        },
    )
}

/// Register a secondary in the primary's routable set so the keepalive
/// emitter does not early-return on an empty fleet. The pre-welcome
/// `AwaitingWelcome` state is enough for the emission assertion: the
/// emitter only checks `secondaries.is_empty()` and reads
/// `self.config.node_id` + `self.workers` — it does not depend on the
/// connection's typestate.
fn seed_secondary(
    coordinator: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    secondary_id: &str,
) {
    coordinator.secondaries.insert(
        secondary_id.into(),
        SecondaryConnectionState::AwaitingWelcome(SecondaryConnection::new(secondary_id.into())),
    );
    // Model a fully-connected secondary: a real welcome originates BOTH the
    // transport-handle entry above AND the replicated `SecondaryCapacity`
    // record. The latter is what `known_secondaries()` reads — the
    // CRDT-derived roster the V5 `wait_for_mesh_ready` `expected` set (and
    // the assignment roster read) consult.
    coordinator
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::SecondaryCapacity {
            secondary: secondary_id.into(),
            worker_count: 1,
            resources: vec![dynrunner_core::ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: 1024 * 1024 * 1024,
            }],
        });
}

/// Count the `Keepalive` messages in a recorded broadcast log.
fn count_keepalives(
    log: &std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>,
) -> usize {
    log.borrow()
        .iter()
        .filter(|m| matches!(m, DistributedMessage::Keepalive { target: _, .. }))
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
            let (mut coordinator, log, _ends, _mesh) =
                make_recording_coordinator(1, Duration::from_millis(100), Duration::from_secs(1));
            // Seed sec-0 into the local `secondaries` map so the keepalive
            // emitter has a roster to fan to (`broadcast_primary_keepalive`
            // early-returns on an empty roster).
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

            // The keepalive is a QUEUED mesh send; settle the production pump
            // so it drains onto the recorder before reading the log.
            crate::primary::tests::settle_pump().await;
            assert_eq!(
                count_keepalives(&log),
                1,
                "activate_local_primary must emit exactly one keepalive at the \
                 authority convergence point"
            );
        })
        .await;
}

/// Uniform primary announcement: the single bootstrap+failover
/// convergence point (`activate_local_primary`) originates
/// `ClusterMutation::PrimaryChanged { new = self }` over the one mesh, so
/// `current_primary()` resolves to this host cluster-wide. The replicated
/// apply drives the primary-changed important-event hook (registered at
/// construction), so the LLM-wake milestone is emitted uniformly on the
/// genuine holder transition — no hand-written "sole authority" line.
///
/// Asserts BOTH the wire announce (the broadcast `PrimaryChanged{new:
/// "primary", epoch: 1}` in the recorded log — `epoch: 1` is the
/// non-default value that proves the originator ran, not a zeroed
/// default) AND the single uniform "primary changed" important event.
#[tokio::test(flavor = "current_thread")]
async fn activate_local_primary_announces_primary_changed() {
    use dynrunner_protocol_primary_secondary::cluster_mutation::ClusterMutation;

    use crate::test_capture::{ImportantCapture, important_only};
    use tracing::subscriber::set_default;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::{Layer, Registry};

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, log, _ends, _mesh) =
                make_recording_coordinator(1, Duration::from_millis(100), Duration::from_secs(1));
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

            // The PrimaryChanged announce is a QUEUED mesh send; settle the
            // production pump so it drains onto the recorder before reading
            // the log. The important-event hook fires on the LOCAL apply
            // (synchronous, inside activate), so it is already captured.
            crate::primary::tests::settle_pump().await;

            // Wire announce: exactly one broadcast carries a
            // `PrimaryChanged { new = "primary", epoch = 1 }`. `epoch: 1`
            // (= primary_epoch() + 1 on the bootstrap cluster_state) is
            // the non-default value confirming the originator computed it
            // rather than shipping a zeroed default.
            let primary_changes: Vec<(String, u64)> = log
                .borrow()
                .iter()
                .filter_map(|m| match m {
                    DistributedMessage::ClusterMutation { mutations, .. } => {
                        Some(mutations.clone())
                    }
                    _ => None,
                })
                .flatten()
                .filter_map(|mutation| match mutation {
                    ClusterMutation::PrimaryChanged { new, epoch, .. } => Some((new, epoch)),
                    _ => None,
                })
                .collect();
            assert_eq!(
                primary_changes,
                vec![("setup".to_string(), 1)],
                "activate_local_primary must broadcast exactly one \
                 PrimaryChanged naming self at epoch 1"
            );

            // Uniform milestone: the replicated apply fires the
            // primary-changed important-event hook exactly once on the
            // genuine None → "primary" holder transition.
            let msgs = capture.messages();
            assert_eq!(
                msgs.len(),
                1,
                "exactly one primary-changed important event: {msgs:?}"
            );
            assert!(msgs[0].contains("primary changed"), "{msgs:?}");
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
            let (mut coordinator, log, _ends, _mesh) = make_recording_coordinator(
                1,
                Duration::from_millis(100),
                Duration::from_millis(350),
            );
            seed_secondary(&mut coordinator, "sec-0");

            // `sec-0` never reports MeshReady → the wait blocks on its
            // select! until the mesh-ready deadline elapses, ticking the
            // pre-operational keepalive arm in the meantime.
            let mut no_cmd_rx: Option<tokio::sync::mpsc::Receiver<PrimaryCommand<TestId>>> = None;
            coordinator
                .wait_for_mesh_ready(&mut no_cmd_rx)
                .await
                .expect("wait returns on the mesh-ready timeout");

            // The keepalives the wait ticked are QUEUED mesh sends; settle the
            // production pump so any still on the egress queue reach the
            // recorder before the count is read.
            crate::primary::tests::settle_pump().await;
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
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator(/* retry_max_passes = */ 3);

            // Seed two binaries in the same phase: one Unfulfillable, one
            // Recoverable. `all_binaries` is the lookup table the bucket
            // primitive uses to map hashes back to dispatchable
            // `TaskInfo`s.
            let unfulfillable_bin = make_binary("operator-only", 50);
            let recoverable_bin = make_binary("retriable", 40);
            let unfulfillable_hash = compute_task_hash(&unfulfillable_bin);
            let recoverable_hash = compute_task_hash(&recoverable_bin);
            coordinator.all_binaries = vec![unfulfillable_bin.clone(), recoverable_bin.clone()];

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
            let mut no_cmd_rx: Option<tokio::sync::mpsc::Receiver<PrimaryCommand<TestId>>> = None;
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
        })
        .await;
}

/// Issue #189 regression: an `Unfulfillable` task sitting in
/// `failed_tasks` must NOT be drained/reinjected by — nor charge —
/// the actual-execution-error retry-pass buckets.
///
/// `Unfulfillable` means a task's dependencies aren't available — it
/// is NOT an execution failure. Its reinjection is reserved for the
/// operator/matcher `PrimaryCommand::ReinjectTask` channel (with its
/// OWN budget, `unfulfillable_reinject_max_per_task`). If the
/// execution-error buckets (`Recoverable` / OOM) instead treated an
/// `Unfulfillable` entry as a candidate, a never-resolving
/// Unfulfillable task would be reinjected, re-fail as Unfulfillable,
/// land back in `failed_tasks`, and be reinjected again on the next
/// drain — each reinject burning one execution-error retry pass.
/// That churn would EXHAUST the `Recoverable` budget a genuinely
/// transient failure needs, wrongly giving up on it. issue #189
/// (asm-dataset-reported) is exactly this conflation.
///
/// Scenario: ONE phase owning BOTH an `Unfulfillable` task and a
/// `Recoverable` task, `retry_max_passes = 2`, BOTH present in
/// `failed_tasks` when the execution-error bucket runs (the
/// worker-reported shape — neither has been operator-reinjected). We
/// run the `Recoverable` bucket repeatedly with the Unfulfillable
/// entry persistently re-seeded into `failed_tasks` (modelling a
/// never-resolving dependency), and assert:
///   * the Unfulfillable entry is NEVER drained from `failed_tasks` by
///     the execution-error bucket (it is not a bucket candidate), and
///   * the Recoverable task still receives its FULL `retry_max_passes`
///     reinjections — the budget tally is charged purely by the
///     Recoverable candidate, with ZERO consumed by the persistent
///     Unfulfillable entry.
///
/// This pins the budget-accounting invariant the partition test
/// (`retry_bucket_skips_unfulfillable_failures`) does not: that test
/// runs the bucket once; this one keeps the Unfulfillable entry in
/// `failed_tasks` across the whole budget so a conflated predicate
/// would visibly churn-exhaust the Recoverable budget early.
#[tokio::test(flavor = "current_thread")]
async fn unfulfillable_entry_does_not_consume_execution_error_retry_budget() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            const RETRY_MAX_PASSES: u32 = 2;
            let (mut coordinator, _mesh) = make_coordinator(RETRY_MAX_PASSES);

            // Both tasks live in the SAME phase ("default") so a conflated
            // budget would be visibly shared.
            let unfulfillable_bin = make_binary("deps-missing", 50);
            let recoverable_bin = make_binary("transient-fail", 40);
            let unfulfillable_hash = compute_task_hash(&unfulfillable_bin);
            let recoverable_hash = compute_task_hash(&recoverable_bin);
            let phase = unfulfillable_bin.phase_id.clone();
            assert_eq!(
                phase, recoverable_bin.phase_id,
                "both tasks must share a phase for the budget-sharing check"
            );

            install_pool_for_phase(&mut coordinator, &unfulfillable_bin);
            coordinator.all_binaries = vec![unfulfillable_bin.clone(), recoverable_bin.clone()];

            // Seed the CRDT: the Unfulfillable task in its discrete state.
            for bin in [&unfulfillable_bin, &recoverable_bin] {
                coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                    hash: compute_task_hash(bin),
                    task: bin.clone(),
                });
            }

            // The Unfulfillable entry — re-seeded before every bucket run
            // to model a dependency that never becomes available. If the
            // execution-error bucket treated it as a candidate, this
            // persistent entry would be reinjected (and charge a pass)
            // every time the budget allowed.
            let reseed_unfulfillable = |c: &mut PrimaryCoordinator<_, _, _>| {
                c.failed_tasks.insert(
                    unfulfillable_hash.clone(),
                    ErrorType::Unfulfillable {
                        reason: "missing toolchain".to_string().into(),
                    },
                );
            };

            // No execution-error pass charged before the bucket runs.
            reseed_unfulfillable(&mut coordinator);
            assert_eq!(
                coordinator.retry_passes_used_for_test(),
                0,
                "no retry pass should be charged before the bucket runs"
            );

            // Run the Recoverable execution-error bucket to exhaustion.
            // The Recoverable task re-fails after each reinject; the
            // Unfulfillable entry stays present the entire time.
            let mut no_cmd_rx: Option<tokio::sync::mpsc::Receiver<PrimaryCommand<TestId>>> = None;
            for pass in 1..=RETRY_MAX_PASSES {
                reseed_unfulfillable(&mut coordinator);
                coordinator
                    .failed_tasks
                    .insert(recoverable_hash.clone(), ErrorType::Recoverable);

                let reinjected = coordinator
                    .try_run_phase_retry_bucket(
                        &phase,
                        crate::primary::retry_bucket::BucketKind::Recoverable,
                        &mut no_cmd_rx,
                    )
                    .await
                    .expect("retry bucket runs cleanly");
                assert!(
                    reinjected,
                    "Recoverable task must get its full retry budget; \
                     pass {pass} of {RETRY_MAX_PASSES} should reinject"
                );

                // The execution-error bucket must NEVER touch the
                // Unfulfillable entry — it is not a candidate, so it stays
                // in `failed_tasks` for the operator/matcher channel.
                assert!(
                    matches!(
                        coordinator.failed_tasks.get(&unfulfillable_hash),
                        Some(ErrorType::Unfulfillable { .. })
                    ),
                    "Unfulfillable entry must remain in failed_tasks after \
                     the Recoverable bucket runs (pass {pass}); the \
                     execution-error bucket must not drain it"
                );
            }

            // The Recoverable bucket charged EXACTLY `retry_max_passes`
            // passes. With a conflated predicate the persistent
            // Unfulfillable entry would have been reinjected alongside the
            // Recoverable task each pass — but the pass TALLY is per
            // bucket-run, so the decisive signal is below: with the budget
            // now spent, a Recoverable failure must still be deniable
            // (budget exhausted), AND the next probe must show the
            // Unfulfillable entry was never the reason a pass was spent.
            assert_eq!(
                coordinator.retry_passes_used_for_test(),
                RETRY_MAX_PASSES,
                "the execution-error retry budget must be charged purely \
                 by the Recoverable bucket"
            );

            // Budget exhausted: a further Recoverable failure does NOT
            // reinject. Crucially, the Unfulfillable entry is STILL
            // present — proving it was carried untouched across the entire
            // budget rather than being churned through the execution-error
            // channel (which a conflated predicate would have done,
            // draining it on the first pass that had budget).
            reseed_unfulfillable(&mut coordinator);
            coordinator
                .failed_tasks
                .insert(recoverable_hash.clone(), ErrorType::Recoverable);
            let post_exhaustion = coordinator
                .try_run_phase_retry_bucket(
                    &phase,
                    crate::primary::retry_bucket::BucketKind::Recoverable,
                    &mut no_cmd_rx,
                )
                .await
                .expect("retry bucket runs cleanly");
            assert!(
                !post_exhaustion,
                "Recoverable budget should be exhausted after exactly \
                 retry_max_passes passes"
            );
            assert!(
                matches!(
                    coordinator.failed_tasks.get(&unfulfillable_hash),
                    Some(ErrorType::Unfulfillable { .. })
                ),
                "Unfulfillable entry must survive in failed_tasks for the \
                 operator/matcher reinject channel — never consumed by the \
                 execution-error retry budget"
            );
        })
        .await;
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
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator(/* retry_max_passes = */ 0);

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
                    attempt: 0,
                    hash: hash.clone(),
                    kind: ErrorType::Unfulfillable {
                        reason: "missing toolchain".to_string().into(),
                    },
                    error: "unfulfillable".into(),
                    version: Default::default(),
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
        })
        .await;
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
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator(/* retry_max_passes = */ 2);

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
                    attempt: 0,
                    hash: hash.clone(),
                    kind: ErrorType::Unfulfillable {
                        reason: "missing toolchain".to_string().into(),
                    },
                    error: "unfulfillable".into(),
                    version: Default::default(),
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
                    attempt: 0,
                    hash: hash.clone(),
                    kind: ErrorType::Recoverable,
                    error: "transient".into(),
                    version: Default::default(),
                },
            );
            coordinator
                .failed_tasks
                .insert(hash.clone(), ErrorType::Recoverable);

            // Step 4: per-phase Recoverable bucket drains the entry
            // into the pool. The fresh Recoverable kind — not the
            // carried Unfulfillable — is what determines bucket
            // eligibility.
            let mut no_cmd_rx: Option<tokio::sync::mpsc::Receiver<PrimaryCommand<TestId>>> = None;
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
        })
        .await;
}

// ── Fleet-dead arming on `alive_remote_secondary_count` ──────────────
//
// These tests pin the honest-liveness arming quantity: the primary arms
// fleet-dead when the count of alive worker-secondaries OTHER than the
// host it recognizes as primary reaches zero (with a non-empty pool).
// The condition reads `cluster_state.alive_remote_secondary_count()`, so
// the fixtures seed the replicated `cluster_state` via the real apply
// path (`PeerJoined` → Alive, `SecondaryCapacity` → the capacity record
// `alive_secondary_members` reads, `PrimaryChanged` → `current_primary`,
// `PeerRemoved` → Dead) rather than touching the primary-local
// `secondaries` map (which the OLD `secondaries.is_empty()` condition
// keyed off — exactly the field that left a primary running its own
// secondary unable to ever arm).

/// Seed ONE worker-secondary into the coordinator's replicated
/// `cluster_state`: `PeerJoined` (→ Alive) + `SecondaryCapacity` (→ the
/// capacity record `alive_secondary_members` reads). A non-zero
/// `worker_count` is the positive "has a secondary" signal the count
/// filters on.
fn seed_cluster_secondary(
    coordinator: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    id: &str,
    worker_count: u32,
) {
    let state = coordinator.cluster_state_mut_for_test();
    let _ = state.apply(ClusterMutation::PeerJoined {
        peer_id: id.to_string(),
        is_observer: false,
        can_be_primary: false,
        cap_version: Default::default(),
    });
    let _ = state.apply(ClusterMutation::SecondaryCapacity {
        secondary: id.to_string(),
        worker_count,
        resources: vec![],
    });
}

/// Name `id` the recognized primary in the replicated `cluster_state`
/// (epoch 1, above the bootstrap epoch 0 so the LWW apply installs it).
fn set_current_primary(
    coordinator: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    id: &str,
) {
    let _ = coordinator
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::PrimaryChanged {
            new: id.to_string(),
            epoch: 1,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        });
}

/// Mark a previously-seeded secondary dead (`PeerRemoved` → `Dead`), so
/// `is_peer_alive` reads false and it drops out of
/// `alive_secondary_members`.
fn kill_cluster_secondary(
    coordinator: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    id: &str,
) {
    let _ = coordinator
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::PeerRemoved {
            id: id.to_string(),
            cause: RemovalCause::KeepaliveMiss,
        });
}

/// Prime the coordinator with `count` queued binaries in a default phase
/// plus the run-level counters the fleet-dead drain / stranded
/// accounting reads. Mirrors the priming in `tests::stranded`.
fn prime_pool_with_queued(
    coordinator: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    count: usize,
) {
    let phase = dynrunner_core::PhaseId::from("default");
    let mut pool =
        PendingPool::<TestId>::new([phase.clone()], HashMap::new()).expect("default-phase pool");
    let binaries: Vec<dynrunner_core::TaskInfo<TestId>> = (0..count)
        .map(|i| make_binary(&format!("bin_{i}"), 50 + i as u64 * 10))
        .collect();
    pool.extend(binaries.clone()).expect("valid extend");
    coordinator.pending = Some(pool);
    coordinator.all_binaries = binaries.clone();
    coordinator.total_tasks = binaries.len();
}

/// Build a coordinator with a `fleet_dead_timeout` of `timeout` over a
/// channel transport with zero pre-registered peers.
fn make_fleet_coordinator(
    timeout: Duration,
) -> (
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    PrimaryMeshKeepalive,
) {
    let (transport, _ends) = setup_test(0);
    build_test_primary(
        PrimaryConfig {
            node_id: "setup".into(),
            num_secondaries: 0,
            fleet_dead_timeout: timeout,
            ..Default::default()
        },
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    )
}

/// (a) A primary running its own secondary, partitioned from EVERY
/// remote worker-secondary, arms fleet-dead and strands — even though its
/// OWN secondary is still alive in the cluster ledger. This is the
/// split-brain-safety invariant: the primary's own secondary (whose id IS
/// `current_primary`) must NOT keep it alive, because a freshly-elected
/// primary may already be running the real cluster. The OLD
/// `secondaries.is_empty()` condition could never trip here (the own
/// secondary lives in the primary-local map), so the run hung; the
/// count-based condition arms correctly.
#[tokio::test(flavor = "current_thread")]
async fn primary_strands_when_only_own_secondary_alive() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Zero timeout so the very first loop iteration's
            // `elapsed >= fleet_dead_timeout` predicate trips.
            let (mut primary, _mesh) = make_fleet_coordinator(Duration::ZERO);

            // The host advertises BOTH a worker-secondary under its own id
            // ("primary") AND is the recognized primary.
            seed_cluster_secondary(&mut primary, "setup", 4);
            set_current_primary(&mut primary, "setup");
            // A remote secondary that has since died (partition).
            seed_cluster_secondary(&mut primary, "sec-0", 4);
            kill_cluster_secondary(&mut primary, "sec-0");

            // Fixture preconditions: the OWN secondary IS an alive
            // worker-secondary, but the REMOTE count is zero (the own
            // entry is excluded by the `id != current_primary` filter).
            let state = primary.cluster_state_for_test();
            assert!(
                state.alive_secondary_members().any(|id| id == "setup"),
                "the own secondary must be an alive worker-secondary"
            );
            assert_eq!(
                state.alive_remote_secondary_count(),
                0,
                "every REMOTE worker-secondary is gone; only the own \
                 secondary remains, which the filter excludes"
            );

            prime_pool_with_queued(&mut primary, 3);

            primary
                .operational_loop()
                .await
                .expect("operational_loop returns Ok on the fleet-dead exit");

            // Armed + stranded: pool drained, nothing classified failed
            // (never dispatched), so run-level accounting strands all.
            assert!(
                primary.pool().is_empty(),
                "the primary must arm fleet-dead and drain the pool \
                 despite its own secondary being alive"
            );
            assert!(
                primary.failed_tasks.is_empty(),
                "never-dispatched tasks must NOT be classified failed"
            );
            let stranded = primary
                .total_tasks
                .saturating_sub(primary.completed_tasks.len() + primary.failed_tasks.len());
            assert_eq!(
                stranded, primary.total_tasks,
                "every un-dispatched binary surfaces as stranded"
            );
        })
        .await;
}

/// (b) A healthy multi-node fleet (two alive REMOTE worker-secondaries)
/// must NOT arm fleet-dead: the remote count is > 0, so the run keeps
/// waiting for work. Driven under paused time with a bounded wait —
/// holding a transport inbound sender so `recv_peer` parks; the
/// operational loop must still be running (not have taken any exit) when
/// the bound elapses.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn healthy_fleet_does_not_arm_fleet_dead() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Keep an inbound sender alive so `recv_peer` parks (does not
            // return None → transport-closed) and the loop genuinely
            // blocks rather than exiting on a closed transport.
            let (transport, secondary_ends) = setup_test(1);
            let _inbound_keepalive = secondary_ends; // hold the incoming_tx clone
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig {
                    node_id: "setup".into(),
                    num_secondaries: 2,
                    // Long enough that, were the arm reachable, it would
                    // fire well after the bounded wait below — but it must
                    // never arm because the remote count is > 0.
                    fleet_dead_timeout: Duration::from_secs(60),
                    keepalive_interval: Duration::from_secs(3600),
                    ..Default::default()
                },
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Two alive REMOTE worker-secondaries; the submitter "primary"
            // is the recognized primary (and is NOT itself a secondary).
            set_current_primary(&mut primary, "setup");
            seed_cluster_secondary(&mut primary, "sec-0", 4);
            seed_cluster_secondary(&mut primary, "sec-1", 4);
            assert_eq!(
                primary
                    .cluster_state_for_test()
                    .alive_remote_secondary_count(),
                2,
                "two alive remote worker-secondaries must be counted"
            );

            prime_pool_with_queued(&mut primary, 3);

            // Run the loop under a bounded paused-time wait. With a
            // healthy fleet no exit condition trips, so the loop is still
            // pending when the bound elapses → `timeout` returns Err. A
            // premature `Ok(..)` would mean the loop took the fleet-dead
            // (or some other) exit — the regression this guards against.
            let outcome =
                tokio::time::timeout(Duration::from_secs(5), primary.operational_loop()).await;
            assert!(
                outcome.is_err(),
                "healthy fleet (remote count > 0) must NOT arm fleet-dead; the \
                 operational loop must still be running, not exited"
            );
        })
        .await;
}

/// (c) A submitter primary (no own secondary) whose remote-only fleet has
/// entirely died arms fleet-dead and strands. The submitter is the
/// recognized primary but is NOT a worker-secondary, so the
/// `id != current_primary` filter is a no-op here: the count is simply
/// "all alive worker-secondaries", which is zero once every remote
/// secondary is dead.
#[tokio::test(flavor = "current_thread")]
async fn submitter_primary_strands_when_remote_fleet_gone() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _mesh) = make_fleet_coordinator(Duration::ZERO);

            // Submitter primary: recognized primary, NO own secondary
            // capacity. Two remote secondaries that have both died.
            set_current_primary(&mut primary, "setup");
            for id in ["sec-0", "sec-1"] {
                seed_cluster_secondary(&mut primary, id, 4);
                kill_cluster_secondary(&mut primary, id);
            }

            let state = primary.cluster_state_for_test();
            assert!(
                state.alive_secondary_members().next().is_none(),
                "every worker-secondary is dead"
            );
            assert_eq!(
                state.alive_remote_secondary_count(),
                0,
                "submitter primary: filter is a no-op, count == all alive \
                 worker-secondaries == 0"
            );

            prime_pool_with_queued(&mut primary, 3);

            primary
                .operational_loop()
                .await
                .expect("operational_loop returns Ok on the fleet-dead exit");

            assert!(
                primary.pool().is_empty(),
                "submitter primary must arm fleet-dead and drain the pool"
            );
            assert!(
                primary.failed_tasks.is_empty(),
                "never-dispatched tasks must NOT be classified failed"
            );
            let stranded = primary
                .total_tasks
                .saturating_sub(primary.completed_tasks.len() + primary.failed_tasks.len());
            assert_eq!(
                stranded, primary.total_tasks,
                "every un-dispatched binary surfaces as stranded"
            );
        })
        .await;
}
