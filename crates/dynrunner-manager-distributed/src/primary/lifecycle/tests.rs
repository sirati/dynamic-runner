use std::collections::HashMap;
use std::time::Duration;

use dynrunner_core::ErrorType;
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_scheduler_api::PendingPool;
use dynrunner_transport_channel::ChannelSecondaryTransportEnd;
use tokio::sync::oneshot;

use crate::cluster_state::TaskState;
use crate::primary::command_channel::{
    handle_primary_command, PrimaryCommand,
};
use crate::primary::test_helpers::{
    make_binary, setup_test, FixedEstimator, NoPeers, TestId,
};
use crate::primary::wire::compute_task_hash;
use crate::primary::{PrimaryConfig, PrimaryCoordinator};

/// Stand-alone fixture matching the shape used by
/// `command_channel::tests::make_coordinator`: a `PrimaryCoordinator`
/// over an in-process channel transport stub with zero connected
/// secondaries, suitable for driving `run_retry_passes` /
/// `apply_reinject_task` directly without a full operational loop.
fn make_coordinator(
    retry_max_passes: u32,
) -> PrimaryCoordinator<
    ChannelSecondaryTransportEnd<TestId>,
    NoPeers,
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
        fleet_dead_timeout: Duration::from_secs(1),
        mesh_ready_timeout: Duration::from_secs(1),
        mass_death_grace: Duration::from_secs(1),
        mass_death_min_count: 2,
        source_dir: None,
        unfulfillable_reinject_max_per_task: None,
    };
    PrimaryCoordinator::new(
        config,
        transport,
        NoPeers,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    )
}

/// Seed `coordinator.pending` with a default-phase pool that owns
/// the supplied binary's phase. Required so `pool.reinject(binary)`
/// has a phase entry to flip back to Active.
fn install_pool_for_phase(
    coordinator: &mut PrimaryCoordinator<
        ChannelSecondaryTransportEnd<TestId>,
        NoPeers,
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

/// `run_retry_passes` must NOT reinject entries whose
/// `ErrorType` is `Unfulfillable { .. }`. Those are the operator-
/// resolvable failure class — `TaskState::Unfulfillable` in the
/// CRDT — and reinjection is reserved for the explicit
/// `PrimaryCommand::ReinjectTask` path. Pre-fix, the snapshot
/// `mem::take(&mut self.failed_tasks)` drained EVERY entry
/// (including Unfulfillable) into the pool, sidestepping the
/// per-task `unfulfillable_reinject_max_per_task` budget that
/// gates the operator path. This test pins the partition.
#[tokio::test(flavor = "current_thread")]
async fn retry_pass_skips_unfulfillable_failures() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let mut coordinator = make_coordinator(/* retry_max_passes = */ 3);

        // Seed two binaries: one Unfulfillable, one Recoverable.
        // `all_binaries` is the lookup table `run_retry_passes`
        // uses to map hashes back to dispatchable `TaskInfo`s.
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

        // No connected secondaries + no in-flight workers ⇒ the
        // operational loop run inside the retry pass returns
        // immediately (counter check trips at
        // `0 + len(failed) >= 0` for total_tasks=0 with
        // active_workers=0). That's enough to observe the
        // partition behaviour: the recoverable entry is drained
        // into the pool, the Unfulfillable entry stays in
        // `failed_tasks`.
        coordinator.run_retry_passes().await.unwrap();

        // Retriable entry was drained and reinjected (would land
        // back in `failed_tasks` only if the operational loop
        // observed another failure — with zero workers no such
        // observation can happen).
        assert!(
            !coordinator.failed_tasks.contains_key(&recoverable_hash),
            "retry pass should drain Recoverable entries from \
             failed_tasks before reinjecting"
        );

        // Unfulfillable entry stayed — and kept its ErrorType so
        // end-of-run accounting still classifies it correctly.
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

        // Step 4: retry-pass drains the Recoverable entry into
        // the pool. The fresh Recoverable kind — not the carried
        // Unfulfillable — is what determines retry-pass
        // eligibility.
        coordinator.run_retry_passes().await.unwrap();

        // The hash was drained (no zero-worker re-failure can
        // re-populate it), confirming the retry pass picked it up.
        assert!(
            !coordinator.failed_tasks.contains_key(&hash),
            "Recoverable failure on a previously-reinjected hash \
             must still be retry-pass-eligible"
        );
    }).await;
}
