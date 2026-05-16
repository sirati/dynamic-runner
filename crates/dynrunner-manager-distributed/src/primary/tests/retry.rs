//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;


/// Regression: post-demotion the local primary's `run_retry_passes`
/// is a no-op (the primary owns retry). This test pins the
/// primary side equivalent: a Recoverable failure observed by
/// the primary's own worker should land in
/// `primary_failed`, the synchronous drain-check should
/// re-inject into `primary_pending` once the in-flight ledger empties,
/// and the next dispatch cycle should rerun the task. A task that
/// succeeds on the retry leaves `primary_failed` empty.
///
/// Why the assertions probe the primary (not the local primary):
/// the local primary's `operational_loop` exits the moment its
/// counter check satisfies `completed + failed >= total`, which fires
/// on the FIRST failure observed via the wire forward — well before
/// the primary's keepalive-tick / synchronous drain-check
/// delivers the retry success. Once the local primary returns from
/// `run()`, its `completed_tasks` / `failed_tasks` snapshots are
/// frozen and don't reflect any later retry outcome. The
/// primary's `completed_tasks` is the post-demotion source of
/// truth for cluster-wide completion accounting; the local primary's
/// counters are a forwarding cache that's deliberately stale at this
/// point.
///
/// Setup: 1 binary "ok" (50 bytes) + 1 binary "flaky" (40 bytes), 1
/// real secondary, 1 worker. The pool sorts size-DESC so "ok" is
/// initial-assigned first; "flaky" stays queued and falls into the
/// primary's `primary_pending` post-promotion. The
/// `FlakyWorkerFactory` is parameterised to fail "flaky" exactly once
/// on its first attempt (Recoverable), and to succeed every other
/// task / subsequent attempt. After the first failure the
/// primary re-injects "flaky" into its own pool and the worker
/// picks it up again via the steady-state `request_task_for_worker`
/// path; the second attempt succeeds. End state on the primary
/// side: 2 completions, 0 residual failures, 1 retry pass consumed.
#[tokio::test(flavor = "current_thread")]
async fn recoverable_failure_succeeds_on_retry_pass() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let max_res = dynrunner_core::ResourceMap::from(
            [(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024u64)]
        );
        // Quota=1 for "flaky" (relative_path = "/tmp/flaky"): fail
        // attempt 1 with Recoverable, succeed from attempt 2 onwards.
        // "ok" is unlisted → quota=0 → succeeds on attempt 1.
        let mut quotas = HashMap::new();
        quotas.insert("/tmp/flaky".to_string(), 1u32);
        let flaky = super::test_helpers::FlakyWorkerFactory::with_quotas(quotas);

        let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
            spawn_real_secondary_flaky(
                "sec-0".into(),
                /* num_workers = */ 1,
                max_res,
                flaky,
                /* retry_max_passes = */ 1,
            );

        // Wire the channel pair into the primary's transport.
        let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
        let mut outgoing = HashMap::new();
        outgoing.insert("sec-0".to_string(), pri_to_sec_tx);
        tokio::task::spawn_local(async move {
            let mut rx = sec_to_pri_rx;
            while let Some(msg) = rx.recv().await {
                if incoming_tx.send(msg).is_err() {
                    break;
                }
            }
        });
        let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(10),
            peer_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_millis(50),
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
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Two binaries: "ok" (50 bytes, sorts first under size-DESC
        // → initial-assigned to worker 0) and "flaky" (40 bytes,
        // stays queued → on promotion the new primary's
        // `populate_primary_from_cluster_state` rebuilds
        // `primary_pending` from the replicated cluster ledger).
        let binaries = vec![
            make_binary("ok", 50),
            make_binary("flaky", 40),
        ];

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        // Drop primary to close the secondary's primary_transport;
        // the primary's `process_tasks` exits on transport
        // close + zero peers (single-secondary case). By the time
        // `primary.run()` returns the primary has already
        // observed the retry-success TaskComplete on its own
        // worker-event channel and incremented its
        // `completed_tasks` count to 2 — the local primary's exit
        // happens AFTER the primary's bookkeeping is final.
        drop(primary);

        let (completed, failed_residual, passes_used) =
            sec_handle.await.unwrap();

        // Both binaries reached terminal success on the
        // primary's view: "ok" succeeded first attempt, "flaky"
        // succeeded on retry. No residual permanent failures.
        // Exactly one retry pass was consumed.
        assert_eq!(completed, 2, "primary should report 2 completions");
        assert_eq!(
            failed_residual, 0,
            "primary's failed ledger should be empty after retry success"
        );
        assert_eq!(
            passes_used, 1,
            "exactly one retry pass should have been consumed"
        );
    }).await;
}

/// Companion to `recoverable_failure_succeeds_on_retry_pass`: a task
/// that fails Recoverably on EVERY attempt (main pass + every retry
/// pass) ends up permanently in `primary_failed`, and the
/// retry budget reaches `config.retry_max_passes`. Pins the
/// budget-exhaustion side of the primary retry pass — without
/// this guard the drain-check could re-inject in an unbounded loop.
///
/// Setup: same shape as the success test (1 binary "ok" + 1 binary
/// "doomed", 1 worker, 1 secondary). `FlakyWorkerFactory` is told to
/// fail "doomed" `u32::MAX` times — i.e. always — so both the main
/// dispatch and the single retry attempt return Recoverable. End
/// state on the primary side: 1 completion ("ok"), 1
/// permanent failure ("doomed"), 1 retry pass consumed (=
/// `retry_max_passes`).
#[tokio::test(flavor = "current_thread")]
async fn recoverable_failure_exhausts_retry_budget_and_becomes_permanent() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let max_res = dynrunner_core::ResourceMap::from(
            [(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024u64)]
        );
        // Quota = u32::MAX so "doomed" never succeeds across any
        // number of attempts. With `retry_max_passes = 1`, the
        // primary tries: main pass (fail #1) → retry pass
        // (fail #2) → budget exhausted, "doomed" stays in
        // `primary_failed`.
        let mut quotas = HashMap::new();
        quotas.insert("/tmp/doomed".to_string(), u32::MAX);
        let flaky = super::test_helpers::FlakyWorkerFactory::with_quotas(quotas);

        let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
            spawn_real_secondary_flaky(
                "sec-0".into(),
                1,
                max_res,
                flaky,
                /* retry_max_passes = */ 1,
            );

        let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
        let mut outgoing = HashMap::new();
        outgoing.insert("sec-0".to_string(), pri_to_sec_tx);
        tokio::task::spawn_local(async move {
            let mut rx = sec_to_pri_rx;
            while let Some(msg) = rx.recv().await {
                if incoming_tx.send(msg).is_err() {
                    break;
                }
            }
        });
        let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(10),
            peer_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_millis(50),
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
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // "ok" sorts first (size 50 > 40) → initial-assigned →
        // succeeds. "doomed" stays in pool → reaches primary's
        // `primary_pending` post-promotion → dispatched via
        // `handle_primary_task_request` → fails Recoverably → drain-
        // check re-injects → fails again → budget exhausted →
        // permanent.
        let binaries = vec![
            make_binary("ok", 50),
            make_binary("doomed", 40),
        ];

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        // Drop primary so the primary's transport closes and
        // its `process_tasks` exits. By that point the
        // primary has fully consumed its retry budget on
        // "doomed".
        drop(primary);

        let (succeeded, failed_residual, passes_used) =
            sec_handle.await.unwrap();

        // `secondary.completed_count()` is the size of the
        // `completed_tasks` set, which after the latest fix only
        // tracks tasks that reached non-Recoverable termination
        // (success or terminal failure). Recoverable failures —
        // whether retried-to-success, retried-to-Recoverable-again,
        // or budget-exhausted-still-Recoverable — stay out of the
        // set so the primary's dispatch retain doesn't filter
        // them out from a future re-injection. Here "ok" succeeded
        // and is in the set; "doomed" was Recoverable on every
        // attempt and isn't.
        assert_eq!(
            succeeded, 1,
            "only the unconditionally-succeeding binary should land in completed_tasks"
        );
        // The retry-specific bookkeeping is the assertion that
        // matters for this regression: "doomed" still sits in the
        // permanent-failure ledger after the budget was consumed.
        assert_eq!(
            failed_residual, 1,
            "exhausted retry budget should leave 1 entry in primary_failed"
        );
        assert_eq!(
            passes_used, 1,
            "retry budget should be fully consumed"
        );
    }).await;
}

/// Companion: a task that fails BOTH the main pass and the retry
/// pass stays permanently in `failed_tasks` — `retry_max_passes=1`
/// means one retry, no third chance.
#[tokio::test(flavor = "current_thread")]
async fn recoverable_failure_twice_becomes_permanent() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, mut secondary_ends) = setup_test(1);

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
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries = vec![make_binary("doomed", 50)];

        let (id, rx, tx) = secondary_ends.remove(0);
        tokio::task::spawn_local(async move {
            let mut rx = rx;
            tx.send(DistributedMessage::SecondaryWelcome {
                sender_id: id.clone(), timestamp: 0.0,
                secondary_id: id.clone(),
                resources: vec![dynrunner_core::ResourceAmount {
                    kind: dynrunner_core::ResourceKind::memory(),
                    amount: 1024 * 1024 * 1024,
                }],
                worker_count: 1,
                hostname: "test".into(),
                is_observer: false,
            }).unwrap();
            tx.send(DistributedMessage::CertExchange {
                sender_id: id.clone(), timestamp: 0.0,
                secondary_id: id.clone(),
                public_cert_pem: "FAKE".into(),
                ipv4_address: Some("127.0.0.1".into()),
                ipv6_address: None,
                quic_port: 5000,
            }).unwrap();
            // Fail every attempt — both main and retry pass. Issue a
            // TaskRequest after each failure so primary's operational
            // loop has a chance to dispatch the re-injected retry
            // task; otherwise the pool would sit with the task and
            // never drain.
            while let Some(msg) = rx.recv().await {
                let hash_opt = match &msg {
                    DistributedMessage::InitialAssignment { zip_files, .. } => zip_files
                        .first()
                        .and_then(|z| z.binaries.first())
                        .map(|e| e.hash.clone()),
                    DistributedMessage::TaskAssignment { file_hash, .. } => {
                        Some(file_hash.clone())
                    }
                    _ => None,
                };
                if let Some(h) = hash_opt {
                    tx.send(DistributedMessage::TaskFailed {
                        sender_id: id.clone(), timestamp: 0.0,
                        secondary_id: id.clone(),
                        worker_id: 0,
                        task_hash: h,
                        error_type: dynrunner_core::ErrorType::Recoverable,
                        error_message: "always fails".into(),
                    }).unwrap();
                    tx.send(DistributedMessage::TaskRequest {
                        sender_id: id.clone(), timestamp: 0.0,
                        secondary_id: id.clone(),
                        worker_id: 0,
                        available_resources: vec![dynrunner_core::ResourceAmount {
                            kind: dynrunner_core::ResourceKind::memory(),
                            amount: 1024 * 1024 * 1024,
                        }],
                    }).unwrap();
                }
            }
        });

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        // Main pass fails, retry pass fails again → permanent.
        assert_eq!(primary.completed_count(), 0);
        assert_eq!(primary.failed_count(), 1);
    }).await;
}

/// `retry_max_passes = 0` disables the retry loop entirely: a task
/// that fails Recoverable in the main pass becomes permanently failed
/// without a second attempt. Pins the budget knob's lower bound so
/// consumers that opt into "fail-fast" behaviour (e.g. CI smoke runs
/// where a single Recoverable signals a real bug rather than a flake)
/// get the contract they ask for.
#[tokio::test(flavor = "current_thread")]
async fn retry_max_passes_zero_disables_retry() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, mut secondary_ends) = setup_test(1);

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
            retry_max_passes: 0,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries = vec![make_binary("doomed", 50)];

        let (id, rx, tx) = secondary_ends.remove(0);
        // Mirror the structure of `recoverable_failure_twice_becomes_permanent`:
        // fail every attempt with Recoverable. With retry_max_passes=0
        // the for-loop in run_retry_passes never iterates, so the
        // single main-pass failure is final.
        tokio::task::spawn_local(async move {
            let mut rx = rx;
            tx.send(DistributedMessage::SecondaryWelcome {
                sender_id: id.clone(), timestamp: 0.0,
                secondary_id: id.clone(),
                resources: vec![dynrunner_core::ResourceAmount {
                    kind: dynrunner_core::ResourceKind::memory(),
                    amount: 1024 * 1024 * 1024,
                }],
                worker_count: 1,
                hostname: "test".into(),
                is_observer: false,
            }).unwrap();
            tx.send(DistributedMessage::CertExchange {
                sender_id: id.clone(), timestamp: 0.0,
                secondary_id: id.clone(),
                public_cert_pem: "FAKE".into(),
                ipv4_address: Some("127.0.0.1".into()),
                ipv6_address: None,
                quic_port: 5000,
            }).unwrap();
            while let Some(msg) = rx.recv().await {
                let hash_opt = match &msg {
                    DistributedMessage::InitialAssignment { zip_files, .. } => zip_files
                        .first()
                        .and_then(|z| z.binaries.first())
                        .map(|e| e.hash.clone()),
                    DistributedMessage::TaskAssignment { file_hash, .. } => {
                        Some(file_hash.clone())
                    }
                    _ => None,
                };
                if let Some(h) = hash_opt {
                    tx.send(DistributedMessage::TaskFailed {
                        sender_id: id.clone(), timestamp: 0.0,
                        secondary_id: id.clone(),
                        worker_id: 0,
                        task_hash: h,
                        error_type: dynrunner_core::ErrorType::Recoverable,
                        error_message: "always fails".into(),
                    }).unwrap();
                    tx.send(DistributedMessage::TaskRequest {
                        sender_id: id.clone(), timestamp: 0.0,
                        secondary_id: id.clone(),
                        worker_id: 0,
                        available_resources: vec![dynrunner_core::ResourceAmount {
                            kind: dynrunner_core::ResourceKind::memory(),
                            amount: 1024 * 1024 * 1024,
                        }],
                    }).unwrap();
                }
            }
        });

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        // Main pass fails once; retry loop is skipped entirely
        // because budget is 0 → permanent failure with no retry.
        assert_eq!(primary.completed_count(), 0);
        assert_eq!(primary.failed_count(), 1);
    }).await;
}
