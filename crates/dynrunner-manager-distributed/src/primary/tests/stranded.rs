//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;

/// Stranded-task accounting: a "happy path" run where every binary
/// reaches a terminal completion must report `stranded_count() == 0`.
///
/// Pin: the new counter must not leak a stale residue from a previous
/// run, must be reset at `run()` start, and must agree with
/// `total - completed - failed` on the success arm. Without this
/// guard a refactor that forgot to reset `stranded_count` between
/// runs would silently turn every clean run into a `RunError::ClusterCollapsed`.
#[tokio::test(flavor = "current_thread")]
async fn stranded_count_is_zero_on_clean_run() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
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
                oom_retry_max_passes: 1,
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
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries = vec![
                make_binary("a", 50),
                make_binary("b", 60),
                make_binary("c", 70),
            ];

            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary(id, 2, 1024 * 1024 * 1024, rx, tx));
            }

            let (deps, ops, ope) = noop_phase_args();
            primary.run(binaries, deps, ops, ope).await.unwrap();

            assert_eq!(primary.completed_count(), 3);
            assert_eq!(primary.failed_count(), 0);
            assert_eq!(
                primary.stranded_count(),
                0,
                "clean-run stranded must be zero (total - completed - failed)"
            );
        })
        .await;
}

/// Mid-handshake disconnect helper: the fake sends Welcome + Cert +
/// MeshReady, then immediately drops the channel that lets it talk
/// back to the primary the moment it sees its first inbound message
/// (which in practice will be `PeerInfo`, the next message after
/// the primary completes its half of the handshake). The drop closes
/// the primary's mpsc receiver, surfacing as `recv() -> None` inside
/// the operational loop — i.e. the cluster-collapse failure mode the
/// stranded-tracking patch is designed to detect.
///
/// Co-located with the test that uses it because the shape ("die
/// after handshake, before the run loop can dispatch a single task")
/// is specific to the cluster-collapse regression and not general
/// enough to merit promotion to `test_helpers.rs`.
async fn fake_secondary_dies_post_mesh_ready(
    secondary_id: String,
    num_workers: u32,
    ram_bytes: u64,
    mut incoming_from_primary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    outgoing_to_primary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    outgoing_to_primary
        .send(DistributedMessage::SecondaryWelcome {
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            resources: vec![dynrunner_core::ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: ram_bytes,
            }],
            worker_count: num_workers,
            hostname: "test-host".into(),
            is_observer: false,
            can_be_primary: false,
        })
        .unwrap();
    outgoing_to_primary
        .send(DistributedMessage::CertExchange {
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            public_cert_pem: "FAKE_CERT".into(),
            ipv4_address: Some("127.0.0.1".into()),
            ipv6_address: None,
            quic_port: 5000,
        })
        .unwrap();
    outgoing_to_primary
        .send(DistributedMessage::MeshReady {
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            peer_count: 0,
        })
        .unwrap();

    // Wait for any one inbound message — at this point the
    // handshake on the primary side is past the wait_for_connections
    // phase and we're either in peer_setup or initial_assignment.
    // Drop the outbound channel by letting it go out of scope so the
    // primary's `recv()` returns `None` once every fake has dropped
    // its clone. Any further inbound messages are simply discarded
    // by closing the receiver.
    let _ = incoming_from_primary.recv().await;
    drop(outgoing_to_primary);
}

/// Thread-local tracing buffer: captures every event emitted on the
/// current thread for the lifetime of the returned guard. Used by
/// the cluster-collapse test to pin the diagnostic log line without
/// touching the process-global subscriber that other tests in this
/// binary set via `tracing_subscriber::fmt::try_init`.
///
/// `current_thread` tokio flavour + `LocalSet` keep every spawned
/// fake-secondary on the same thread as the test future, so a
/// `set_default()` thread-local subscriber is reached by every
/// `tracing::error!` site that the `run()` flow hits.
fn capture_logs_thread_local() -> (
    std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    tracing::dispatcher::DefaultGuard,
) {
    use std::sync::{Arc, Mutex};
    #[derive(Clone)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedWriter {
        type Writer = SharedWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    let buf = Arc::new(Mutex::new(Vec::new()));
    let writer = SharedWriter(buf.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_writer(writer)
        .with_max_level(tracing::Level::ERROR)
        .with_ansi(false)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (buf, guard)
}

/// T-stranded-on-cluster-collapse: when the secondaries die fatally
/// after handshake but before any task is dispatched, `run()` must
/// return `RunError::ClusterCollapsed` carrying the per-category
/// counts; the post-call accounting must satisfy
/// `completed + failed + stranded == total`; and the diagnostic log
/// line must fire so consumers grepping for "tasks left unassigned
/// because cluster routing collapsed" see it on every collapse.
///
/// Pre-fix: `run()` returned `Ok(())` with completed=0 / failed=0 /
/// total=N, hiding the `total - 0 - 0 = N` un-dispatched tasks; CI
/// scripts checking exit code saw green when the run had collapsed.
#[tokio::test(flavor = "current_thread")]
async fn stranded_on_cluster_collapse_returns_err_with_counts() {
    // Install the thread-local log capture before any awaits so the
    // diagnostic emitted from inside `primary.run().await` is recorded.
    // The guard scopes the subscriber to the current thread for as
    // long as it lives — dropped at the end of the test, leaving the
    // process-global subscriber (if any) untouched.
    let (log_buf, _log_guard) = capture_logs_thread_local();

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
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
                oom_retry_max_passes: 1,
                // Long fleet_dead so the operational loop's exit happens
                // via "transport closed" (recv → None), not via the
                // fleet-dead timer push-to-failed path. Keeps this test
                // focused on the stranded-on-recv-None arm; a separate
                // future test could pin the fleet-dead arm independently.
                fleet_dead_timeout: std::time::Duration::from_secs(600),
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
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..6)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();
            let total = binaries.len();

            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary_dies_post_mesh_ready(
                    id,
                    /* num_workers = */ 1,
                    1024 * 1024 * 1024,
                    rx,
                    tx,
                ));
            }

            let (deps, ops, ope) = noop_phase_args();
            let outcome = primary.run(binaries, deps, ops, ope).await;

            match outcome {
                Err(RunError::ClusterCollapsed { stranded, outcome }) => {
                    assert!(
                        stranded > 0,
                        "stranded must be positive on cluster collapse"
                    );
                    assert_eq!(
                        outcome.total_terminal() + stranded,
                        total,
                        "succeeded + fail_retry + fail_oom + fail_final + stranded must equal total"
                    );
                    assert_eq!(outcome.succeeded, primary.completed_count());
                    assert_eq!(
                        outcome.fail_retry + outcome.fail_oom + outcome.fail_final,
                        primary.failed_count(),
                        "per-class fail buckets must sum to total failed_tasks count"
                    );
                    assert_eq!(stranded, primary.stranded_count());
                }
                other => panic!(
                    "expected RunError::ClusterCollapsed, got {other:?} (counters: \
                 succeeded={} failed={} stranded={} total={})",
                    primary.completed_count(),
                    primary.failed_count(),
                    primary.stranded_count(),
                    total,
                ),
            }

            // Diagnostic log line must have fired so consumers grepping
            // for the substring see it. Log emission happens inside
            // `PrimaryCoordinator::run`, which is awaited directly in
            // the test scope — the thread-local subscriber installed by
            // `capture_logs_thread_local` records every error-level event
            // from the same thread.
            let captured = String::from_utf8_lossy(&log_buf.lock().unwrap()).into_owned();
            assert!(
                captured.contains("tasks left unassigned because cluster routing collapsed"),
                "diagnostic 'tasks left unassigned because cluster routing collapsed' must \
             fire on the cluster-collapse arm so ops scripts can detect it; captured \
             error-level logs:\n{captured}"
            );
        })
        .await;
}

/// Pin Fix-#21 contract: when the operational loop's
/// `fleet_dead_timeout` arm fires with queued tasks in the pool, the
/// drained binaries must be classified as `stranded` — not `failed`.
/// They were never dispatched, no secondary attempted them, no worker
/// reported a failure, so the only honest category is "couldn't be
/// tried". Pre-fix the arm pushed each pending task's hash into
/// `failed_tasks`, conflating worker-reported failure with
/// never-dispatched and (worse) burning the retry budget on tasks
/// that hadn't actually failed.
///
/// We drive the operational loop directly (bypassing `run()`'s setup
/// phases) so the fleet-dead arm fires from a primed state: empty
/// secondaries map + non-empty pool + tight timeout. This isolates
/// the pre/post fix semantic delta to a single observable assertion
/// (`failed_tasks.is_empty() && pool drained`) — pre-fix the arm
/// would land every drained binary in `failed_tasks`; post-fix the
/// arm leaves `failed_tasks` empty and the binaries flow into the
/// run-level `stranded` category by way of the `total - completed -
/// failed` accounting in `run()`.
#[tokio::test(flavor = "current_thread")]
async fn fleet_dead_timeout_pending_become_stranded_not_failed() {
    use dynrunner_scheduler_api::PendingPool;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(0);
            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: 0,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                keepalive_interval: Duration::from_secs(60),
                keepalive_miss_threshold: 3,
                source_pre_staged_root: None,
                uses_file_based_items: true,
                required_setup_on_promote: false,
                max_concurrent_per_type: std::collections::HashMap::new(),
                retry_max_passes: 0,
                oom_retry_max_passes: 1,
                // Zero timeout so the very first loop iteration's
                // `elapsed >= fleet_dead_timeout` predicate trips, no
                // wall-clock wait needed in the test.
                fleet_dead_timeout: std::time::Duration::ZERO,
                mesh_ready_timeout: std::time::Duration::from_secs(5),
                mass_death_grace: std::time::Duration::ZERO,
                mass_death_min_count: 2,
                source_dir: None,
                unfulfillable_reinject_max_per_task: None,
                setup_promote_deadline: std::time::Duration::from_secs(600),
            };
            let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Prime: pool with three queued binaries, empty secondaries
            // map (the fleet-dead predicate is `secondaries.is_empty() &&
            // !pool.is_empty()`), `total_tasks` set so the run-level
            // accounting can later compute `stranded = total -
            // completed - failed`.
            let phase = dynrunner_core::PhaseId::from("default");
            let mut pool =
                PendingPool::<TestId>::new([phase.clone()], std::collections::HashMap::new())
                    .expect("default-phase pool");
            let binaries: Vec<TaskInfo<TestId>> = (0..3)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();
            pool.extend(binaries.clone()).expect("valid extend");
            primary.pending = Some(pool);
            primary.phase_completed.insert(phase.clone(), 0);
            primary.phase_failed.insert(phase, 0);
            primary.all_binaries = binaries.clone();
            primary.total_tasks = binaries.len();

            // No workers, no secondaries — fleet-dead arm fires
            // immediately on entry to the operational loop.
            primary
                .operational_loop()
                .await
                .expect("operational_loop must return Ok on the fleet-dead exit path");

            // Pool must be drained so the loop terminates (both pre and
            // post fix).
            assert!(
                primary.pool().is_empty(),
                "fleet-dead arm must drain the queued pool"
            );
            // Fix-#21 contract: pre-fix this set was populated with the
            // drained binaries' hashes; post-fix it stays empty so the
            // `total - completed - failed` accounting downstream
            // classifies them as stranded.
            assert!(
                primary.failed_tasks.is_empty(),
                "fleet-dead pending must NOT be classified as failed; pre-fix \
             arm pushed pending hashes into failed_tasks, conflating \
             never-dispatched with worker-reported failure (got {:?})",
                primary.failed_tasks
            );
            assert!(
                primary.completed_tasks.is_empty(),
                "fleet-dead with un-dispatched tasks must report no completions"
            );

            // Drive the run-level accounting that `run()` would do post-
            // operational-loop, end-to-end-equivalent. With failed and
            // completed both empty, every binary lands in the stranded
            // bucket — exactly the category Fix-#21 surfaces.
            let total = primary.total_tasks;
            let completed = primary.completed_tasks.len();
            let failed = primary.failed_tasks.len();
            let stranded = total.saturating_sub(completed + failed);
            assert_eq!(
                stranded, total,
                "every un-dispatched binary must surface as stranded \
             (completed={completed} failed={failed} total={total})"
            );
        })
        .await;
}

/// Pin: `drain_pending_messages` processes any `TaskComplete` /
/// `TaskFailed` messages still queued in the inbound transport when
/// it's invoked, updating `completed_tasks` / `failed_tasks` exactly
/// as the operational loop's `recv → dispatch_message` pipeline does.
/// This is the helper the post-loop drain step in `run()` calls before
/// computing the stranded count, closing the window where the
/// pre-fix accounting saw pre-drain counters and false-positived clean
/// runs into `RunError::ClusterCollapsed` (counted successful
/// completions as `stranded`).
///
/// Construction: we drive the helper directly (no `run()` lifecycle)
/// by pre-loading TaskComplete messages into the shared incoming
/// channel and asserting the helper drains them. The drain helper
/// reuses `dispatch_message`, which calls `handle_task_complete`,
/// which inserts into `completed_tasks` regardless of whether a
/// matching worker exists — so we don't have to plumb a fake worker
/// to exercise the counter update.
#[tokio::test(flavor = "current_thread")]
async fn drain_pending_messages_updates_completed_set() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
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
                oom_retry_max_passes: 1,
                fleet_dead_timeout: std::time::Duration::from_secs(30),
                mesh_ready_timeout: std::time::Duration::from_secs(5),
                mass_death_grace: std::time::Duration::ZERO,
                mass_death_min_count: 2,
                source_dir: None,
                unfulfillable_reinject_max_per_task: None,
                setup_promote_deadline: std::time::Duration::from_secs(600),
            };
            let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Inject three TaskComplete messages from the fake secondary's
            // outbound clone (which is what `secondary_ends[i].2` is — the
            // shared inbound side from the primary's perspective). Closing
            // the sender at the end ensures `transport.recv()` will yield
            // `Some` for each queued message and then `None`, exercising
            // the drain helper's "process until empty" path through both
            // arms of the recv result.
            let (sec_id, _to_sec_rx, incoming_tx) = secondary_ends.into_iter().next().unwrap();
            for hash in ["hash-a", "hash-b", "hash-c"] {
                incoming_tx
                    .send(DistributedMessage::TaskComplete {
                        sender_id: sec_id.clone(),
                        timestamp: 0.0,
                        secondary_id: sec_id.clone(),
                        worker_id: 0,
                        task_hash: hash.into(),
                        result_data: None,
                    })
                    .unwrap();
            }
            // Drop the sender so the recv channel will eventually yield
            // `None`. The drain helper should treat that as "transport
            // closed → drain complete" and break.
            drop(incoming_tx);

            primary
                .drain_pending_messages(Duration::from_millis(500))
                .await
                .expect("drain must succeed on healthy transport");

            // Post-drain the per-hash completed-set has every drained
            // TaskComplete's hash. We pin the HashSet contents directly
            // (rather than `completed_count()`) because the test injects
            // synthesized TaskComplete messages without prior TaskAdded
            // mutations — `cluster_state.apply(TaskCompleted)` NoOps on
            // a non-existent ledger entry (CRDT precondition). The drain's
            // load-bearing job here is to consume the messages through
            // `handle_task_complete` (line 58: `completed_tasks.insert`),
            // not to converge the CRDT mirror; the post-fix
            // `completed_count()` reads through the CRDT and would NOT
            // observe these synthetic completes.
            assert_eq!(
                primary.completed_tasks.len(),
                3,
                "drain must have processed all three queued TaskComplete messages \
             (per-hash set populated by handle_task_complete's direct insert)"
            );
            for hash in ["hash-a", "hash-b", "hash-c"] {
                assert!(
                    primary.completed_tasks.contains(hash),
                    "completed_tasks must contain {hash} after drain"
                );
            }
        })
        .await;
}

/// Pin Fix-#23 contract end-to-end: a happy-path run with multiple
/// secondaries where every dispatched task succeeds must not surface
/// any task as `stranded`. Pre-fix the accounting in `run()` ran
/// before any post-loop drain of in-flight TaskComplete messages,
/// flipping clean runs into `RunError::ClusterCollapsed`. Post-fix
/// the drain step processes whatever was still in transit before the
/// stranded computation, so completed runs report
/// `completed=N / failed=0 / stranded=0` and `run()` returns `Ok(())`.
///
/// Two secondaries here (vs the N=1 fixture in
/// `stranded_count_is_zero_on_clean_run`) widens the surface to the
/// multi-secondary code paths — completion-forwarding, per-secondary
/// worker bookkeeping — that the e2e scenario in #19 surfaced.
#[tokio::test(flavor = "current_thread")]
async fn clean_run_does_not_false_positive_stranded() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
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
                oom_retry_max_passes: 1,
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
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..4)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();
            let total = binaries.len();

            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary(
                    id,
                    /* num_workers = */ 2,
                    1024 * 1024 * 1024,
                    rx,
                    tx,
                ));
            }

            let (deps, ops, ope) = noop_phase_args();
            primary
                .run(binaries, deps, ops, ope)
                .await
                .expect("clean multi-secondary run must return Ok");

            assert_eq!(
                primary.completed_count(),
                total,
                "every binary must report completed on a clean run"
            );
            assert_eq!(
                primary.failed_count(),
                0,
                "no binary should land in failed on a clean run"
            );
            assert_eq!(
                primary.stranded_count(),
                0,
                "no binary should be stranded on a clean run — \
             pre-fix the accounting ran before pending TaskCompletes \
             drained, false-positiving successful tasks as stranded"
            );
        })
        .await;
}
