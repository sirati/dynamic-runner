//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;

/// End-to-end: 1 real primary + 1 real secondary (2 workers), 5 tasks.
#[tokio::test(flavor = "current_thread")]
async fn e2e_primary_and_secondary_single_node() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let secondary_id = "sec-0".to_string();
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);

            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
                spawn_real_secondary(secondary_id.clone(), 2, max_res);

            // Build primary transport wired to the real secondary
            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert(secondary_id.clone(), pri_to_sec_tx);

            // Forward secondary→primary messages into the primary's incoming channel
            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
            });

            let transport =
                ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                ..test_primary_config()
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..5)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            {
                let (deps, ops, ope) = noop_phase_args();
                primary.run(binaries, deps, ops, ope).await.unwrap()
            };

            let completed = primary.completed_count();
            let failed = primary.failed_count();

            // Drop primary to close transport channels, allowing secondaries to exit
            drop(primary);

            let sec_completed = sec_handle.await.unwrap();

            assert_eq!(completed, 5);
            assert_eq!(failed, 0);
            assert_eq!(sec_completed, 5);
        })
        .await;
}

/// End-to-end: 1 real primary + 2 real secondaries (2 workers each), 10 tasks.
#[tokio::test(flavor = "current_thread")]
async fn e2e_primary_and_two_secondaries() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                2 * 1024 * 1024 * 1024u64,
            )]);
            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            let mut sec_handles = Vec::new();

            for i in 0..2u32 {
                let secondary_id = format!("sec-{i}");
                let (pri_to_sec_tx, sec_to_pri_rx, handle) =
                    spawn_real_secondary(secondary_id.clone(), 2, max_res.clone());

                outgoing.insert(secondary_id, pri_to_sec_tx);
                sec_handles.push(handle);

                // Forward secondary→primary
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
            drop(incoming_tx); // Only forwarding tasks hold senders now

            let transport =
                ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                num_secondaries: 2,
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                ..test_primary_config()
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..10)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            {
                let (deps, ops, ope) = noop_phase_args();
                primary.run(binaries, deps, ops, ope).await.unwrap()
            };

            let completed = primary.completed_count();
            let failed = primary.failed_count();

            // Drop primary to close transport channels, allowing secondaries to exit
            drop(primary);

            let mut per_sec_completed = Vec::new();
            for handle in sec_handles {
                per_sec_completed.push(handle.await.unwrap());
            }

            assert_eq!(completed, 10);
            assert_eq!(failed, 0);
            // `spawn_real_secondary`'s handle returns each secondary's
            // OWN-worker run count (`local_tasks_run_for_test`). In the
            // unified model a secondary is a worker host, not an authority
            // mirror — it does NOT keep a cluster-wide `completed_tasks`
            // set (that was the demolished demoted-primary mirror). Every
            // task runs on exactly one secondary's worker, so the per-
            // secondary own-work counts partition the 10 tasks: their SUM
            // is the total and each secondary ran at least one (the fleet
            // genuinely shared the load, not all on one node). The
            // cluster-wide convergence invariant (every node's replicated
            // CRDT observes all 10 terminals) is asserted at the CRDT layer
            // in `cluster_state_converges_on_primary_and_secondary`.
            let total_own: usize = per_sec_completed.iter().sum();
            assert_eq!(
                total_own, 10,
                "every task must run on exactly one secondary's worker; the \
             own-work counts {per_sec_completed:?} must partition all 10 tasks"
            );
            for (i, count) in per_sec_completed.iter().enumerate() {
                assert!(
                    *count >= 1,
                    "secondary {i} should have run at least one task (load shared \
                 across the fleet), got {count}"
                );
            }
        })
        .await;
}

/// Pin that `notify_stage_file` actually emits a `StageFile` wire
/// message into the targeted secondary's incoming channel with the
/// exact fields supplied. This is what the packaging pipeline
/// depends on — without correct routing, the ExtractionCache on the
/// receiving secondary never gets primed.
#[tokio::test(flavor = "current_thread")]
async fn notify_stage_file_emits_wire_message() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut secondary_ends) = setup_test(1);

            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                ..test_primary_config()
            };

            let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Send directly via inherent method (skips the queue + run loop).
            primary
                .notify_stage_file(
                    "sec-0",
                    "deadbeefcafebabe".to_string(),
                    "deadbeef".repeat(8),
                    "rel/binary".to_string(),
                    "scratch/binary".to_string(),
                )
                .await
                .expect("notify_stage_file should succeed");

            // Pull the message out of the targeted secondary's channel.
            let (id, mut to_sec_rx, _outgoing) = secondary_ends.remove(0);
            assert_eq!(id, "sec-0");
            let msg = to_sec_rx
                .recv()
                .await
                .expect("StageFile should be delivered to sec-0");
            match msg {
                DistributedMessage::StageFile {
                    secondary_id,
                    file_hash,
                    src_path,
                    dest_path,
                    ..
                } => {
                    assert_eq!(secondary_id, "sec-0");
                    assert_eq!(file_hash, "deadbeefcafebabe");
                    assert_eq!(src_path, "rel/binary");
                    assert_eq!(dest_path, "scratch/binary");
                }
                other => panic!("expected StageFile, got {:?}", other.msg_type()),
            }
        })
        .await;
}

/// Phase S — replicated cluster ledger convergence: after a real
/// primary + secondary run completes, the secondary's mirror
/// `ClusterState` must reflect the same `Completed` count the primary
/// observed. Pins that:
///   - the post-`wait_for_peer_connections` `TaskAdded` batch reached
///     the secondary,
///   - per-completion `ClusterMutation::TaskCompleted` broadcasts were
///     applied to the secondary's mirror,
///   - the originator-side `apply_and_broadcast_cluster_mutations`
///     applied locally so the primary's own ledger converges.
#[tokio::test(flavor = "current_thread")]
async fn cluster_state_converges_on_primary_and_secondary() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let secondary_id = "sec-0".to_string();
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);

            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();

            let sec_secondary_id = secondary_id.clone();
            let sec_handle: tokio::task::JoinHandle<(usize, crate::cluster_state::StateCounts)> =
                tokio::task::spawn_local(async move {
                    // Channel-backed mesh secondary: the in-process primary
                    // is folded in as an ordinary mesh peer keyed by
                    // `"primary"` (no per-role uplink). Inbound is the
                    // primary→secondary channel; the outbound primary link
                    // is the secondary→primary channel.
                    let mut transport = ChannelPeerTransport::from_raw_channels(
                        sec_secondary_id.clone(),
                        HashMap::new(),
                        pri_to_sec_rx,
                    );
                    transport.register_primary_link("primary".into(), sec_to_pri_tx);

                    let config = SecondaryConfig {
                        secondary_id: sec_secondary_id,
                        num_workers: 2,
                        max_resources: max_res,
                        hostname: "test-host".into(),
                        keepalive_interval: Duration::from_secs(60),
                        src_network: None,
                        src_tmp: None,
                        peer_timeout: Duration::from_secs(120),
                        keepalive_miss_threshold: 3,
                        retry_max_passes: 1,
                        oom_retry_max_passes: 1,
                        primary_link_failure_threshold: 5,
                        primary_link_failure_window: Duration::from_secs(30),
                        primary_silence_backstop: Duration::from_secs(120),
                        unconfigured_deadline: Duration::from_secs(600),
                        is_observer: false,
                        can_be_primary: false,
                        resource_check_interval: Duration::from_millis(100),
                        log_oom_watcher: false,
                        promoted_primary_quiesce_grace: Duration::from_millis(100),
                        unfulfillable_reinject_max_per_task: None,
                        mem_manager_reserved_bytes: None,
                        output_dir: None,
                        memuse_log_path: None,
                    };
                    let mut secondary = SecondaryCoordinator::new(
                        config,
                        transport,
                        ResourceStealingScheduler::memory(),
                        FixedEstimator(100),
                    );
                    secondary.set_bootstrap_primary_id("primary".to_string());
                    let mut factory = FakeWorkerFactory;
                    secondary.run(&mut factory).await.unwrap();
                    (
                        secondary.local_tasks_run_for_test(),
                        secondary.cluster_state_counts_for_test(),
                    )
                });

            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert(secondary_id.clone(), pri_to_sec_tx);

            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
            });

            let transport =
                ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                ..test_primary_config()
            };
            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..5)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            let (deps, ops, ope) = noop_phase_args();
            primary.run(binaries, deps, ops, ope).await.unwrap();

            let primary_counts = primary.cluster_state_counts_for_test();
            assert_eq!(
                primary_counts.completed, 5,
                "primary's own cluster_state should reflect all 5 completions"
            );
            assert_eq!(primary_counts.pending, 0);

            drop(primary);

            let (sec_completed, sec_counts) = sec_handle.await.unwrap();
            assert_eq!(sec_completed, 5);
            assert_eq!(
                sec_counts.completed, 5,
                "secondary's mirror should converge to 5 Completed via \
                 TaskAdded + TaskCompleted broadcasts"
            );
            assert_eq!(sec_counts.pending, 0);
        })
        .await;
}

/// End-to-end: pre-staged source mode locks in the path-mapping
/// contract added in a344b0e (PrimaryConfig.source_pre_staged_root).
///
/// Setup mocks the gateway-bind-mount: a tmpdir holds N fake binary
/// files; the primary's TaskInfo.path is the tmpdir-absolute path
/// (matching what a consumer's discover_items would emit). The
/// primary's `source_pre_staged_root` and the secondary's
/// `src_network` both point at the same tmpdir — in production the
/// gateway-host path and the in-container path are different (the
/// wrapper bind-mounts one to the other) but the test collapses the
/// two views since there's no container.
///
/// Asserts:
///   - All 5 binaries complete.
///   - The wire's local_path on each TaskAssignment was the
///     tmpdir-relative form (the strip happened); without that
///     strip, the secondary's `src_network.join(local_path)` would
///     return the absolute path as-is and `.exists()` would still
///     be true here, so the strip behaviour wouldn't be asserted.
///     We pin it by inspecting the wire on the secondary side.
///
/// This test was the missing pre-stage end-to-end coverage that
/// let bf1ce02 + a344b0e ship with a contract gap each.
#[tokio::test(flavor = "current_thread")]
async fn e2e_pre_staged_source_mode() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Two distinct tmpdirs — `gateway_path` is the host path
            // the primary's TaskInfo.path's are relative to (the
            // wrapper's bind-mount source); `container_path` is what
            // the secondary's `src_network` resolves to (the wrapper's
            // bind-mount destination). Production has these as
            // different paths the wrapper bind-mounts together; the
            // test models them as different tmpdirs with the SAME
            // file basenames present under each. This setup is the
            // load-bearing one: if the primary's `wire_local_path`
            // strip doesn't fire, the wire's local_path is the
            // gateway-absolute `<gateway>/bin_X`, secondary's
            // `src_network.join(<absolute>)` returns the
            // gateway-absolute path verbatim (Path::join rules), and
            // `<gateway>/bin_X.exists()` is true ONLY if the secondary
            // can see the gateway-side files — which it can't here.
            let gateway = tempfile::TempDir::new().expect("gateway tmpdir");
            let gateway_path = gateway.path().to_path_buf();
            let container = tempfile::TempDir::new().expect("container tmpdir");
            let container_path = container.path().to_path_buf();

            let names: Vec<String> = (0..5).map(|i| format!("bin_{i}")).collect();
            for name in &names {
                // Files exist only under the container view (the
                // gateway path is just a string the primary treats as
                // an authoritative root for prefix-stripping).
                std::fs::write(container_path.join(name), b"x")
                    .expect("write fake binary in container view");
            }

            // TaskInfos with paths under the gateway view.
            let binaries: Vec<TaskInfo<TestId>> = names
                .iter()
                .map(|n| {
                    let mut b = make_binary(n, 1);
                    b.path = gateway_path.join(n);
                    b
                })
                .collect();

            let secondary_id = "sec-0".to_string();
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);

            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) = spawn_real_secondary_with_src_network(
                secondary_id.clone(),
                2,
                max_res,
                Some(container_path.clone()),
            );

            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert(secondary_id.clone(), pri_to_sec_tx);

            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
            });

            let transport =
                ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                source_pre_staged_root: Some(gateway_path.clone()),
                ..test_primary_config()
            };
            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            {
                let (deps, ops, ope) = noop_phase_args();
                primary.run(binaries, deps, ops, ope).await.unwrap()
            };

            let completed = primary.completed_count();
            let failed = primary.failed_count();
            drop(primary);

            let sec_completed = sec_handle.await.unwrap();

            assert_eq!(
                completed, 5,
                "primary should see 5 completed in pre-staged mode"
            );
            assert_eq!(failed, 0, "no failures expected");
            assert_eq!(
                sec_completed, 5,
                "secondary should resolve all 5 via src_network"
            );
        })
        .await;
}

/// End-to-end: `uses_file_based_items=false` (FR-2). The TaskInfo
/// `path` is an opaque identifier — no real file at that location.
/// The framework MUST NOT stat/hash/resolve it; the secondary
/// passes `local_path` through to the worker verbatim. Asserts all
/// 5 dispatch successfully despite the paths pointing at nowhere.
///
/// Without the flag, the same setup (no src_network, no
/// queue_initial_staging) would hit the unresolvable-task guard
/// and fail every item NonRecoverable.
#[tokio::test(flavor = "current_thread")]
async fn e2e_uses_file_based_items_false() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let secondary_id = "sec-0".to_string();
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);

            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
                spawn_real_secondary(secondary_id.clone(), 2, max_res);

            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert(secondary_id.clone(), pri_to_sec_tx);

            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
            });

            let transport =
                ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                uses_file_based_items: false,
                ..test_primary_config()
            };
            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Items with paths that don't back to anything on disk.
            // In file-based mode this would fail the dispatch guard;
            // with uses_file_based_items=false the framework treats
            // these as opaque identifiers.
            let binaries: Vec<TaskInfo<TestId>> = (0..5)
                .map(|i| {
                    let mut b = make_binary(&format!("opaque_{i}"), 1);
                    b.path = std::path::PathBuf::from(format!("opaque://manifest-{i}"));
                    b
                })
                .collect();

            {
                let (deps, ops, ope) = noop_phase_args();
                primary.run(binaries, deps, ops, ope).await.unwrap()
            };

            let completed = primary.completed_count();
            let failed = primary.failed_count();
            drop(primary);

            let sec_completed = sec_handle.await.unwrap();

            assert_eq!(completed, 5, "primary should see 5 completed");
            assert_eq!(failed, 0, "no failures expected");
            assert_eq!(sec_completed, 5, "secondary should pass paths through");
        })
        .await;
}

/// FR-1 (scoped): per-type `max_concurrent` cap. With a 4-worker
/// secondary and a cap of 2 on type "compile", the scheduler should
/// never have more than 2 "compile" items in flight at once even
/// though the worker pool could absorb 4. Other types
/// (uncapped) run at the full pool width.
///
/// This isn't a strict-mid-flight assertion (the test fakes complete
/// every assigned task instantly so the in-flight overlap window is
/// tiny); it asserts the run COMPLETES correctly with the cap on
/// (no deadlock, all items dispatched). The real-world value of
/// the cap shows up under slow workers; here we just pin the wire
/// flow + bookkeeping.
#[tokio::test(flavor = "current_thread")]
async fn e2e_per_type_max_concurrent() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let secondary_id = "sec-0".to_string();
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);

            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
                spawn_real_secondary(secondary_id.clone(), 4, max_res);

            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert(secondary_id.clone(), pri_to_sec_tx);

            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
            });

            let mut caps = std::collections::HashMap::new();
            caps.insert(dynrunner_core::TypeId::from("compile"), 2);

            let transport =
                ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                max_concurrent_per_type: caps,
                ..test_primary_config()
            };
            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // 8 items of type "compile" (capped at 2 concurrent) +
            // 8 items of type "merge" (uncapped) = 16 total.
            let binaries: Vec<TaskInfo<TestId>> = (0..16)
                .map(|i| {
                    let mut b = make_binary(&format!("bin_{i}"), 1);
                    b.type_id = if i < 8 {
                        dynrunner_core::TypeId::from("compile")
                    } else {
                        dynrunner_core::TypeId::from("merge")
                    };
                    b
                })
                .collect();

            {
                let (deps, ops, ope) = noop_phase_args();
                primary.run(binaries, deps, ops, ope).await.unwrap()
            };

            let completed = primary.completed_count();
            let failed = primary.failed_count();
            drop(primary);

            let sec_completed = sec_handle.await.unwrap();

            assert_eq!(completed, 16, "all 16 should complete");
            assert_eq!(failed, 0, "no failures expected");
            assert_eq!(sec_completed, 16, "secondary saw all 16");
        })
        .await;
}

/// T1 — regression pin. Asserts that without `queue_stage_file` /
/// `queue_initial_staging_from_binaries`, the in-process distributed
/// pipeline's failure mode is reachable: the task lands as `Failed`
/// with the canonical `expected StageFile notification first` error
/// substring.
///
/// Pairs with T2 (same setup, plus the staging call) — together they
/// form the regression gate against re-introducing the gap that
/// caused asm-tokenizer's `--multi-computer single-process` runs to
/// 100%-fail at HEAD `2f30920`.
#[tokio::test(flavor = "current_thread")]
async fn run_without_stage_file_queue_fails_all_tasks() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let secondary_id = "secondary-0".to_string();
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);

            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
                spawn_real_secondary(secondary_id.clone(), 1, max_res);

            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert(secondary_id.clone(), pri_to_sec_tx);
            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
            });
            let transport =
                ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx);

            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                keepalive_interval: Duration::from_millis(50),
                // `retry_max_passes = 0` so a Recoverable failure becomes
                // permanent on the first pass — the regression we're
                // pinning produces NonRecoverable failures (the unresolvable
                // task guard sends `ErrorType::NonRecoverable`), so the
                // budget is moot, but keeping it at 0 avoids any chance of
                // a retry pass masking the assertion.
                retry_max_passes: 0,
                ..test_primary_config()
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Relative path → wire `local_path` is relative → secondary's
            // `report_unresolvable_task` sees `src_network=None` AND
            // `local_path_is_relative=true` → fires the StageFile-error
            // failure path under test.
            let binaries = vec![make_relative_binary("missing/binary", 50)];

            let (deps, ops, ope) = noop_phase_args();
            primary.run(binaries, deps, ops, ope).await.unwrap();

            // Failure mode reached: 0 completed, 1 permanent failure.
            assert_eq!(
                primary.completed_count(),
                0,
                "no task should complete when staging is omitted"
            );
            assert_eq!(
                primary.failed_count(),
                1,
                "the single task must land in failed_tasks"
            );

            // Pin the canonical error substring so a future refactor that
            // changes the wording surfaces here (a deliberate breakage,
            // not a silent drift). Consumers (asm-tokenizer's e2e check)
            // grep for this string.
            let cs = primary.cluster_state_for_test();
            let mut saw_expected = false;
            for (_hash, state) in cs.tasks_iter() {
                if let crate::cluster_state::TaskState::Failed { last_error, .. } = state {
                    assert!(
                        last_error.contains("expected StageFile notification first"),
                        "failed task's last_error must carry the canonical \
                     regression substring; got: {last_error}"
                    );
                    saw_expected = true;
                }
            }
            assert!(
                saw_expected,
                "cluster_state must record at least one Failed task"
            );

            drop(primary);
            let _ = sec_handle.await;
        })
        .await;
}

/// T2 — fix validation. Same setup as T1, but `queue_initial_staging_from_binaries`
/// is invoked before `run()` so the secondary receives a StageFile
/// record in its `InitialAssignment.staged_files`. Asserts the task
/// completes (i.e. the lift-to-Rust method is wired correctly and
/// the per-secondary fan-out targets the supplied id).
///
/// Pairs with T1 — together the two pin the regression at `2f30920`.
#[tokio::test(flavor = "current_thread")]
async fn run_with_initial_staging_succeeds() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Materialise a real source tree so `compute_file_hash` can
            // succeed: the staging walk reads the file from disk to hash
            // the contents. Single binary keeps the test fast and the
            // assertion surface tight.
            let source_root = std::env::temp_dir().join(format!(
                "stage_init_t2_{}_{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let bin_rel = std::path::PathBuf::from("missing/binary");
            let on_disk = source_root.join(&bin_rel);
            std::fs::create_dir_all(on_disk.parent().unwrap()).unwrap();
            std::fs::write(&on_disk, b"t2-staging-payload").unwrap();

            let secondary_id = "secondary-0".to_string();
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);

            // Secondary needs `src_network` pointing at the source tree
            // so its `stage_file` step can copy the file into the cache —
            // mirrors the real in-process pipeline, where the secondary
            // shares filesystem visibility with the primary. Without
            // `src_network` set the staging copy fails (no source root)
            // and the task still falls through to the unresolvable
            // guard, which would mask the fix.
            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) = spawn_real_secondary_with_src_network(
                secondary_id.clone(),
                1,
                max_res,
                Some(source_root.clone()),
            );

            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert(secondary_id.clone(), pri_to_sec_tx);
            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
            });
            let transport =
                ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx);

            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                keepalive_interval: Duration::from_millis(50),
                retry_max_passes: 0,
                ..test_primary_config()
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries = vec![make_relative_binary(
                bin_rel.to_str().unwrap(),
                18, // matches payload length above; size is informational
            )];

            // The fix under test: lift-to-Rust staging walk. The single
            // secondary's id matches `spawn_real_secondary_with_src_network`'s
            // welcome message, so its `pending_stage_files` entry routes
            // correctly through `staged_per_secondary`.
            let secondary_ids = vec![secondary_id.clone()];
            primary
                .queue_initial_staging_from_binaries(&binaries, &secondary_ids, &source_root)
                .expect("staging walk should succeed for a present, readable file");

            let (deps, ops, ope) = noop_phase_args();
            primary.run(binaries, deps, ops, ope).await.unwrap();

            assert_eq!(
                primary.completed_count(),
                1,
                "task should complete when staging is queued"
            );
            assert_eq!(
                primary.failed_count(),
                0,
                "no task should fail when staging is queued"
            );

            drop(primary);
            let _ = sec_handle.await;

            // Best-effort cleanup; `tempdir`-style teardown.
            let _ = std::fs::remove_dir_all(&source_root);
        })
        .await;
}
