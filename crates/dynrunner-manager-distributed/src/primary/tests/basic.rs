//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;


#[tokio::test(flavor = "current_thread")]
async fn single_secondary_processes_all_tasks() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, secondary_ends) = setup_test(1);

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
                    keepalive_interval: Duration::from_secs(5),
                    keepalive_miss_threshold: 3,
                    source_pre_staged_root: None,
                    uses_file_based_items: true,
                    required_setup_on_promote: false,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
            setup_promote_deadline: std::time::Duration::from_secs(600),
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries = vec![
            make_binary("a", 50),
            make_binary("b", 60),
            make_binary("c", 70),
        ];

        for (id, rx, tx) in secondary_ends {
            tokio::task::spawn_local(fake_secondary(
                id,
                2,
                1024 * 1024 * 1024,
                rx,
                tx,
            ));
        }

        { let (deps, ops, ope) = noop_phase_args(); primary.run(binaries, deps, ops, ope).await.unwrap() };

        assert_eq!(primary.completed_count(), 3);
        assert_eq!(primary.failed_count(), 0);
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn two_secondaries_distribute_work() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, secondary_ends) = setup_test(2);

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 2,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
                    keepalive_interval: Duration::from_secs(5),
                    keepalive_miss_threshold: 3,
                    source_pre_staged_root: None,
                    uses_file_based_items: true,
                    required_setup_on_promote: false,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
            setup_promote_deadline: std::time::Duration::from_secs(600),
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries: Vec<TaskInfo<TestId>> = (0..6)
            .map(|i| make_binary(&format!("bin_{i}"), 100))
            .collect();

        for (id, rx, tx) in secondary_ends {
            tokio::task::spawn_local(fake_secondary(
                id,
                2,
                1024 * 1024 * 1024,
                rx,
                tx,
            ));
        }

        { let (deps, ops, ope) = noop_phase_args(); primary.run(binaries, deps, ops, ope).await.unwrap() };

        assert_eq!(primary.completed_count(), 6);
        assert_eq!(primary.failed_count(), 0);
    }).await;
}

/// Regression: when there are more secondaries than initial-assignable
/// items, the secondaries that DON'T get any work must still receive an
/// InitialAssignment message (with empty zip_files / workers_ready /
/// staged_files). Otherwise their `wait_for_setup` waits forever for
/// the third gating message and the run stalls until heartbeat declares
/// them dead. Caught in the field on a 4-secondary run with a single
/// phase-3 item — three secondaries hung in setup, primary killed them
/// 15s later, work proceeded only on the lucky 4th.
///
/// Setup: 2 real secondaries with workers, 1 binary. Pre-fix only
/// secondary 0 receives InitialAssignment; secondary 1 hangs in
/// `wait_for_setup`. Post-fix both reach `process_tasks` and the
/// run completes.
#[tokio::test(flavor = "current_thread")]
async fn empty_batch_secondary_still_reaches_process_tasks() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let max_res = dynrunner_core::ResourceMap::from(
            [(dynrunner_core::ResourceKind::memory(), 2 * 1024 * 1024 * 1024u64)]
        );
        let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
        let mut outgoing = HashMap::new();
        let mut sec_handles = Vec::new();

        for i in 0..2u32 {
            let secondary_id = format!("sec-{i}");
            let (pri_to_sec_tx, sec_to_pri_rx, handle) =
                spawn_real_secondary(secondary_id.clone(), 2, max_res.clone());
            outgoing.insert(secondary_id, pri_to_sec_tx);
            sec_handles.push(handle);

            let tx = incoming_tx.clone();
            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if tx.send(msg).is_err() {
                        break;
                    }
                }
            });
        }
        drop(incoming_tx);

        let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 2,
            connect_timeout: Duration::from_secs(10),
            peer_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            required_setup_on_promote: false,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
            setup_promote_deadline: std::time::Duration::from_secs(600),
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // ONE binary for TWO secondaries — initial assignment will
        // dispatch it to whichever secondary's worker the scheduler
        // picks first; the other gets `assigned=0` and must still
        // receive a (possibly empty) InitialAssignment to escape
        // wait_for_setup.
        let binaries = vec![make_binary("only", 50)];

        // The pre-fix bug doesn't prevent primary.run() from
        // returning — secondary 0 completes the binary, pool drains,
        // primary exits. But secondary 1 is wedged in
        // wait_for_setup and never reaches process_tasks, so its
        // `completed_count()` would never observe the cluster-wide
        // forward (the value stays at 0 instead of 1). That
        // discrepancy is the test signal.
        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        let completed = primary.completed_count();
        let failed = primary.failed_count();
        drop(primary);

        let mut per_sec_completed = Vec::new();
        for handle in sec_handles {
            per_sec_completed.push(handle.await.unwrap());
        }

        assert_eq!(completed, 1);
        assert_eq!(failed, 0);
        // Both secondaries must have reached process_tasks; the
        // cluster-wide TaskComplete forward registers in each
        // secondary's `completed_tasks` set. Pre-fix the
        // empty-batch secondary is stuck in wait_for_setup and its
        // count stays at 0.
        for (i, count) in per_sec_completed.iter().enumerate() {
            assert!(
                *count >= 1,
                "secondary {i} should have observed the cluster's 1 \
                 completion (entered process_tasks); saw {count}"
            );
        }
    }).await;
}

/// Live distribution past the initial assignment, primary side: 1 secondary
/// with 2 workers, 20 binaries. The initial assignment can cover at most
/// 2 binaries (one per worker); the operational loop is responsible for
/// the remaining 18+. Pins the live-flow path that the legacy Python
/// never managed to get right.
#[tokio::test(flavor = "current_thread")]
async fn live_distribution_continues_past_initial_batch() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, secondary_ends) = setup_test(1);

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
                    uses_file_based_items: true,
                    required_setup_on_promote: false,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
            setup_promote_deadline: std::time::Duration::from_secs(600),
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries: Vec<TaskInfo<TestId>> = (0..20)
            .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
            .collect();

        for (id, rx, tx) in secondary_ends {
            tokio::task::spawn_local(fake_secondary(
                id,
                2,
                1024 * 1024 * 1024,
                rx,
                tx,
            ));
        }

        { let (deps, ops, ope) = noop_phase_args(); primary.run(binaries, deps, ops, ope).await.unwrap() };

        // All 20 must complete; ≥ 18 went via the operational TaskRequest
        // → TaskAssignment loop (one secondary × 2 workers = 2 initial).
        assert_eq!(primary.completed_count(), 20);
        assert_eq!(primary.failed_count(), 0);
    }).await;
}
