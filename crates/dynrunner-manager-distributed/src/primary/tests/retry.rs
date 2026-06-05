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
    local
        .run_until(async {
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);
            // Quota=1 for "flaky" (relative_path = "/tmp/flaky"): fail
            // attempt 1 with Recoverable, succeed from attempt 2 onwards.
            // "ok" is unlisted → quota=0 → succeeds on attempt 1.
            let mut quotas = HashMap::new();
            quotas.insert("/tmp/flaky".to_string(), 1u32);
            let flaky = super::test_helpers::FlakyWorkerFactory::with_quotas(quotas);

            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) = spawn_real_secondary_flaky(
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
            let transport =
                ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx);

            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                keepalive_interval: Duration::from_millis(50),
                ..test_primary_config()
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Two binaries: "ok" (50 bytes, sorts first under size-DESC
            // → initial-assigned to worker 0) and "flaky" (40 bytes,
            // stays queued → on promotion the new primary's
            // `populate_primary_from_cluster_state` rebuilds
            // `primary_pending` from the replicated cluster ledger).
            let binaries = vec![make_binary("ok", 50), make_binary("flaky", 40)];

            let (deps, ops, ope) = noop_phase_args();
            primary.run(binaries, deps, ops, ope).await.unwrap();

            // The authoritative retry cascade lives on the PRIMARY (the
            // secondary is a pure flaky-worker driver / reporter). Read the
            // primary's final counters BEFORE dropping it.
            let completed = primary.completed_count();
            let failed_residual = primary.failed_count();
            let passes_used = primary.retry_passes_used_for_test();

            // Drop primary to close the secondary's uplink; the secondary
            // drains down and the join handle resolves.
            drop(primary);
            let _ = sec_handle.await;

            // Both binaries reached terminal success: "ok" succeeded first
            // attempt, "flaky" succeeded on retry. No residual permanent
            // failures. Exactly one retry pass was consumed.
            assert_eq!(completed, 2, "primary should report 2 completions");
            assert_eq!(
                failed_residual, 0,
                "primary's failed ledger should be empty after retry success"
            );
            assert_eq!(
                passes_used, 1,
                "exactly one retry pass should have been consumed"
            );
        })
        .await;
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
    local
        .run_until(async {
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);
            // Quota = u32::MAX so "doomed" never succeeds across any
            // number of attempts. With `retry_max_passes = 1`, the
            // primary tries: main pass (fail #1) → retry pass
            // (fail #2) → budget exhausted, "doomed" stays in
            // `primary_failed`.
            let mut quotas = HashMap::new();
            quotas.insert("/tmp/doomed".to_string(), u32::MAX);
            let flaky = super::test_helpers::FlakyWorkerFactory::with_quotas(quotas);

            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) = spawn_real_secondary_flaky(
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
            let transport =
                ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx);

            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                keepalive_interval: Duration::from_millis(50),
                ..test_primary_config()
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // "ok" sorts first (size 50 > 40) → initial-assigned →
            // succeeds. "doomed" stays in pool → reaches primary's
            // `primary_pending` post-promotion → dispatched via
            // `handle_primary_task_request` → fails Recoverably → drain-
            // check re-injects → fails again → budget exhausted →
            // permanent.
            let binaries = vec![make_binary("ok", 50), make_binary("doomed", 40)];

            let (deps, ops, ope) = noop_phase_args();
            primary.run(binaries, deps, ops, ope).await.unwrap();

            // Read the PRIMARY's authoritative counters before drop: by
            // this point the primary has fully consumed its retry budget on
            // "doomed".
            let succeeded = primary.completed_count();
            let failed_residual = primary.failed_count();
            let passes_used = primary.retry_passes_used_for_test();

            drop(primary);
            let _ = sec_handle.await;

            // "ok" succeeded; "doomed" was Recoverable on every attempt and
            // ends up in the primary's terminal failure ledger after the
            // budget was consumed.
            assert_eq!(
                succeeded, 1,
                "only the unconditionally-succeeding binary should be counted complete"
            );
            // The retry-specific bookkeeping is the assertion that matters
            // for this regression: "doomed" still sits in the
            // permanent-failure ledger after the budget was consumed.
            assert_eq!(
                failed_residual, 1,
                "exhausted retry budget should leave 1 terminal failure on the primary"
            );
            assert_eq!(passes_used, 1, "retry budget should be fully consumed");
        })
        .await;
}

/// Companion: a task that fails BOTH the main pass and the retry
/// pass stays permanently in `failed_tasks` — `retry_max_passes=1`
/// means one retry, no third chance.
#[tokio::test(flavor = "current_thread")]
async fn recoverable_failure_twice_becomes_permanent() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut secondary_ends) = setup_test(1);

            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                ..test_primary_config()
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries = vec![make_binary("doomed", 50)];

            let (id, rx, tx) = secondary_ends.remove(0);
            tokio::task::spawn_local(async move {
                let mut rx = rx;
                tx.send(DistributedMessage::SecondaryWelcome {
                    sender_id: id.clone(),
                    timestamp: 0.0,
                    secondary_id: id.clone(),
                    resources: vec![dynrunner_core::ResourceAmount {
                        kind: dynrunner_core::ResourceKind::memory(),
                        amount: 1024 * 1024 * 1024,
                    }],
                    worker_count: 1,
                    hostname: "test".into(),
                    is_observer: false,
                    can_be_primary: false,
                })
                .unwrap();
                tx.send(DistributedMessage::CertExchange {
                    sender_id: id.clone(),
                    timestamp: 0.0,
                    secondary_id: id.clone(),
                    public_cert_pem: "FAKE".into(),
                    ipv4_address: Some("127.0.0.1".into()),
                    ipv6_address: None,
                    quic_port: 5000,
                })
                .unwrap();
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
                            sender_id: id.clone(),
                            timestamp: 0.0,
                            secondary_id: id.clone(),
                            worker_id: 0,
                            task_hash: h,
                            error_type: dynrunner_core::ErrorType::Recoverable,
                            error_message: "always fails".into(),
                        })
                        .unwrap();
                        tx.send(DistributedMessage::TaskRequest {
                            sender_id: id.clone(),
                            timestamp: 0.0,
                            secondary_id: id.clone(),
                            worker_id: 0,
                            available_resources: vec![dynrunner_core::ResourceAmount {
                                kind: dynrunner_core::ResourceKind::memory(),
                                amount: 1024 * 1024 * 1024,
                            }],
                        })
                        .unwrap();
                    }
                }
            });

            let (deps, ops, ope) = noop_phase_args();
            primary.run(binaries, deps, ops, ope).await.unwrap();

            // Main pass fails, retry pass fails again → permanent.
            assert_eq!(primary.completed_count(), 0);
            assert_eq!(primary.failed_count(), 1);
        })
        .await;
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
    local
        .run_until(async {
            let (transport, mut secondary_ends) = setup_test(1);

            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                retry_max_passes: 0,
                ..test_primary_config()
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
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
                    sender_id: id.clone(),
                    timestamp: 0.0,
                    secondary_id: id.clone(),
                    resources: vec![dynrunner_core::ResourceAmount {
                        kind: dynrunner_core::ResourceKind::memory(),
                        amount: 1024 * 1024 * 1024,
                    }],
                    worker_count: 1,
                    hostname: "test".into(),
                    is_observer: false,
                    can_be_primary: false,
                })
                .unwrap();
                tx.send(DistributedMessage::CertExchange {
                    sender_id: id.clone(),
                    timestamp: 0.0,
                    secondary_id: id.clone(),
                    public_cert_pem: "FAKE".into(),
                    ipv4_address: Some("127.0.0.1".into()),
                    ipv6_address: None,
                    quic_port: 5000,
                })
                .unwrap();
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
                            sender_id: id.clone(),
                            timestamp: 0.0,
                            secondary_id: id.clone(),
                            worker_id: 0,
                            task_hash: h,
                            error_type: dynrunner_core::ErrorType::Recoverable,
                            error_message: "always fails".into(),
                        })
                        .unwrap();
                        tx.send(DistributedMessage::TaskRequest {
                            sender_id: id.clone(),
                            timestamp: 0.0,
                            secondary_id: id.clone(),
                            worker_id: 0,
                            available_resources: vec![dynrunner_core::ResourceAmount {
                                kind: dynrunner_core::ResourceKind::memory(),
                                amount: 1024 * 1024 * 1024,
                            }],
                        })
                        .unwrap();
                    }
                }
            });

            let (deps, ops, ope) = noop_phase_args();
            primary.run(binaries, deps, ops, ope).await.unwrap();

            // Main pass fails once; retry loop is skipped entirely
            // because budget is 0 → permanent failure with no retry.
            assert_eq!(primary.completed_count(), 0);
            assert_eq!(primary.failed_count(), 1);
        })
        .await;
}

/// LMU-regression target: the OOM-bucket budget is independent of
/// the Recoverable-bucket budget, and a phase whose only outstanding
/// failures are `ResourceExhausted(memory)` must reach `Done` once
/// the OOM bucket is exhausted (here disabled with
/// `oom_retry_max_passes = 0`). Pre-redesign the post-pipeline
/// retry pass + binary "Unfulfillable vs everything else" partition
/// would have wedged the phase: `on_phase_end` had already fired
/// against a stale view, retry would reinject and re-fail, then the
/// surviving fail_oom set stayed in `failed_tasks` while the
/// counter exit kept `total - completed - failed = 0` but the
/// phase-state machine drifted out of sync.
///
/// Asserts BOTH (a) the per-class outcome partition is exactly
/// `0 completed / 1 fail_oom / 0 fail_retry / 0 fail_final`, AND
/// (b) `on_phase_end` fires for the phase with the right counts.
#[tokio::test(flavor = "current_thread")]
async fn oom_failure_with_zero_retries_still_advances_phase() {
    use std::sync::{Arc, Mutex};
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut secondary_ends) = setup_test(1);

            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                oom_retry_max_passes: 0,
                mesh_ready_timeout: std::time::Duration::from_millis(500),
                ..test_primary_config()
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries = vec![make_binary("doomed", 50)];

            let (id, rx, tx) = secondary_ends.remove(0);
            tokio::task::spawn_local(async move {
                let mut rx = rx;
                tx.send(DistributedMessage::SecondaryWelcome {
                    sender_id: id.clone(),
                    timestamp: 0.0,
                    secondary_id: id.clone(),
                    resources: vec![dynrunner_core::ResourceAmount {
                        kind: dynrunner_core::ResourceKind::memory(),
                        amount: 1024 * 1024 * 1024,
                    }],
                    worker_count: 1,
                    hostname: "test".into(),
                    is_observer: false,
                    can_be_primary: false,
                })
                .unwrap();
                tx.send(DistributedMessage::CertExchange {
                    sender_id: id.clone(),
                    timestamp: 0.0,
                    secondary_id: id.clone(),
                    public_cert_pem: "FAKE".into(),
                    ipv4_address: Some("127.0.0.1".into()),
                    ipv6_address: None,
                    quic_port: 5000,
                })
                .unwrap();
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
                            sender_id: id.clone(),
                            timestamp: 0.0,
                            secondary_id: id.clone(),
                            worker_id: 0,
                            task_hash: h,
                            error_type: dynrunner_core::ErrorType::ResourceExhausted(
                                dynrunner_core::ResourceKind::memory(),
                            ),
                            error_message: "over budget".into(),
                        })
                        .unwrap();
                        tx.send(DistributedMessage::TaskRequest {
                            sender_id: id.clone(),
                            timestamp: 0.0,
                            secondary_id: id.clone(),
                            worker_id: 0,
                            available_resources: vec![dynrunner_core::ResourceAmount {
                                kind: dynrunner_core::ResourceKind::memory(),
                                amount: 1024 * 1024 * 1024,
                            }],
                        })
                        .unwrap();
                    }
                }
            });

            let recorded_ends: Arc<Mutex<Vec<(String, u32, u32)>>> =
                Arc::new(Mutex::new(Vec::new()));
            let ends_cb = recorded_ends.clone();
            let on_end: crate::primary::OnPhaseEnd =
                Box::new(move |p: &dynrunner_core::PhaseId, c: u32, f: u32| {
                    ends_cb.lock().unwrap().push((p.to_string(), c, f));
                });
            let on_start: crate::primary::OnPhaseStart = Box::new(|_| {});

            // Bound the test so a wedged phase surfaces as a timeout, not
            // a hang. Mesh-ready collapses fast (500ms above);
            // post-promotion the secondary's quiesce grace contributes
            // ~2s, hence the 10s budget.
            let run_fut = primary.run(binaries, HashMap::new(), on_start, on_end);
            match tokio::time::timeout(Duration::from_secs(10), run_fut).await {
                Ok(res) => res.unwrap(),
                Err(_) => panic!(
                    "LMU regression: run() did not return within 10s with \
                 oom_retry_max_passes=0; phase wedged on ResourceExhausted(memory) \
                 failure"
                ),
            }

            // Per-class outcome partition: 1 fail_oom; everything else 0.
            assert_eq!(primary.completed_count(), 0);
            assert_eq!(primary.failed_count(), 1);
            let outcome = primary.outcome_summary();
            assert_eq!(outcome.fail_oom, 1, "OOM failure must classify as fail_oom");
            assert_eq!(outcome.fail_retry, 0);
            assert_eq!(outcome.fail_final, 0);

            // on_phase_end fired exactly once for the default phase
            // with the right counts.
            let ends = recorded_ends.lock().unwrap().clone();
            assert_eq!(
                ends.len(),
                1,
                "on_phase_end must fire exactly once even with OOM failure; got {ends:?}"
            );
            let (phase, completed, failed) = &ends[0];
            assert_eq!(phase, "default");
            assert_eq!(*completed, 0);
            assert_eq!(*failed, 1);
        })
        .await;
}

/// Per-phase Recoverable bucket runs the retry pass at the
/// phase-drain edge BEFORE `on_phase_end` fires. Setup: one task
/// fails Recoverably once, then succeeds. End state: cluster
/// reports 1 completion, 0 failures — the bucket reinjected at the
/// drain edge, the retry succeeded, the per-phase counter cleared
/// `failed_tasks`.
///
/// Counterpart to `recoverable_failure_succeeds_on_retry_pass`,
/// which exercises the same shape from the secondary's primary-
/// path retry. This test pins the EXIT contract — `completed_count`
/// and `failed_count` reflect the post-retry state by the time
/// `run()` returns — so a regression that loses the retry-success
/// observation between the operational-loop exit and the run-level
/// accounting surfaces here.
#[tokio::test(flavor = "current_thread")]
async fn recoverable_bucket_runs_within_phase_drain_edge() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);
            let mut quotas = HashMap::new();
            quotas.insert("/tmp/flaky".to_string(), 1u32);
            let flaky = super::test_helpers::FlakyWorkerFactory::with_quotas(quotas);

            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) = spawn_real_secondary_flaky(
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
            let transport =
                ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx);

            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                keepalive_interval: Duration::from_millis(50),
                mesh_ready_timeout: std::time::Duration::from_millis(500),
                ..test_primary_config()
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries = vec![make_binary("flaky", 50)];
            let (deps, ops, ope) = noop_phase_args();
            primary.run(binaries, deps, ops, ope).await.unwrap();

            // 1 completion, 0 residual failures: the per-phase bucket
            // reinjected the Recoverable failure inside the same `run()`
            // and the retry succeeded.
            assert_eq!(primary.completed_count(), 1);
            assert_eq!(primary.failed_count(), 0);

            drop(primary);
            let _ = sec_handle.await;
        })
        .await;
}

/// Sequential phase advance: phase B (depends on A) does NOT become
/// Active until A's retry buckets exhaust. A's only task fails
/// `ResourceExhausted(memory)` with `oom_retry_max_passes = 0`, so
/// the OOM bucket immediately exhausts and `on_phase_end(A)` fires;
/// `on_phase_start(B)` must NOT precede `on_phase_end(A)`.
///
/// Pins the "next phase depends on previous phase being done"
/// invariant from the 2026-05-17 user spec.
#[tokio::test(flavor = "current_thread")]
async fn sequential_phase_advance_after_oom_bucket_exhausts() {
    use dynrunner_core::{PhaseId, SoftPreferredSecondaries, TypeId};
    use std::sync::{Arc, Mutex};

    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut secondary_ends) = setup_test(1);

            let mut phase_deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            phase_deps.insert(PhaseId::from("B"), vec![PhaseId::from("A")]);

            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                keepalive_interval: Duration::from_millis(50),
                oom_retry_max_passes: 0,
                mesh_ready_timeout: std::time::Duration::from_millis(500),
                ..test_primary_config()
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Phase A: one task that fails OOM. Phase B: one task that
            // succeeds. The fake secondary fails phase-A tasks with OOM
            // and accepts phase-B tasks.
            fn phased(name: &str, phase: &str) -> TaskInfo<TestId> {
                TaskInfo {
                    path: std::path::PathBuf::from(format!("/tmp/{name}")),
                    size: 50,
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
            let binaries = vec![phased("a_task", "A"), phased("b_task", "B")];

            // Fake secondary: fail "a_task" (OOM), succeed "b_task".
            let (id, rx, tx) = secondary_ends.remove(0);
            tokio::task::spawn_local(async move {
                let mut rx = rx;
                tx.send(DistributedMessage::SecondaryWelcome {
                    sender_id: id.clone(),
                    timestamp: 0.0,
                    secondary_id: id.clone(),
                    resources: vec![dynrunner_core::ResourceAmount {
                        kind: dynrunner_core::ResourceKind::memory(),
                        amount: 1024 * 1024 * 1024,
                    }],
                    worker_count: 1,
                    hostname: "test".into(),
                    is_observer: false,
                    can_be_primary: false,
                })
                .unwrap();
                tx.send(DistributedMessage::CertExchange {
                    sender_id: id.clone(),
                    timestamp: 0.0,
                    secondary_id: id.clone(),
                    public_cert_pem: "FAKE".into(),
                    ipv4_address: Some("127.0.0.1".into()),
                    ipv6_address: None,
                    quic_port: 5000,
                })
                .unwrap();
                // For each assignment: send a `TaskFailed` with OOM if
                // path contains "a_task", or a `TaskComplete` for
                // "b_task". Then ask for the next task.
                while let Some(msg) = rx.recv().await {
                    let assignment = match &msg {
                        DistributedMessage::InitialAssignment { zip_files, .. } => zip_files
                            .first()
                            .and_then(|z| z.binaries.first())
                            .map(|e| (e.hash.clone(), e.binary_info.task_id.clone())),
                        DistributedMessage::TaskAssignment {
                            file_hash,
                            binary_info,
                            ..
                        } => Some((file_hash.clone(), binary_info.task_id.clone())),
                        _ => None,
                    };
                    if let Some((h, task_id)) = assignment {
                        if task_id == "a_task" {
                            tx.send(DistributedMessage::TaskFailed {
                                sender_id: id.clone(),
                                timestamp: 0.0,
                                secondary_id: id.clone(),
                                worker_id: 0,
                                task_hash: h,
                                error_type: dynrunner_core::ErrorType::ResourceExhausted(
                                    dynrunner_core::ResourceKind::memory(),
                                ),
                                error_message: "over budget".into(),
                            })
                            .unwrap();
                        } else {
                            tx.send(DistributedMessage::TaskComplete {
                                sender_id: id.clone(),
                                timestamp: 0.0,
                                secondary_id: id.clone(),
                                worker_id: 0,
                                task_hash: h,
                                result_data: None,
                            })
                            .unwrap();
                        }
                        tx.send(DistributedMessage::TaskRequest {
                            sender_id: id.clone(),
                            timestamp: 0.0,
                            secondary_id: id.clone(),
                            worker_id: 0,
                            available_resources: vec![dynrunner_core::ResourceAmount {
                                kind: dynrunner_core::ResourceKind::memory(),
                                amount: 1024 * 1024 * 1024,
                            }],
                        })
                        .unwrap();
                    }
                }
            });

            // Record ordered phase events.
            #[derive(Clone, Debug)]
            enum Ev {
                Start(String),
                End(String),
            }
            let log: Arc<Mutex<Vec<Ev>>> = Arc::new(Mutex::new(Vec::new()));
            let log_starts = log.clone();
            let on_start: crate::primary::OnPhaseStart = Box::new(move |p: &PhaseId| {
                log_starts.lock().unwrap().push(Ev::Start(p.to_string()));
            });
            let log_ends = log.clone();
            let on_end: crate::primary::OnPhaseEnd = Box::new(move |p: &PhaseId, _, _| {
                log_ends.lock().unwrap().push(Ev::End(p.to_string()));
            });

            let run_fut = primary.run(binaries, phase_deps, on_start, on_end);
            match tokio::time::timeout(Duration::from_secs(10), run_fut).await {
                Ok(res) => res.unwrap(),
                Err(_) => panic!(
                    "sequential-phase-advance: run() did not return within 10s; \
                 phase A may have wedged"
                ),
            }

            let events = log.lock().unwrap().clone();
            // Find indices of Start(B) and End(A). Start(B) must come
            // AFTER End(A).
            let mut start_b: Option<usize> = None;
            let mut end_a: Option<usize> = None;
            for (i, ev) in events.iter().enumerate() {
                match ev {
                    Ev::Start(p) if p == "B" => start_b = Some(i),
                    Ev::End(p) if p == "A" => end_a = Some(i),
                    _ => {}
                }
            }
            let end_a =
                end_a.expect("on_phase_end(A) must fire even after OOM failure; events: see log");
            let start_b =
                start_b.expect("on_phase_start(B) must fire after A is Done; events: see log");
            assert!(
                end_a < start_b,
                "on_phase_start(B) must NOT precede on_phase_end(A); \
             end_a={end_a} start_b={start_b} events={events:?}"
            );
        })
        .await;
}
