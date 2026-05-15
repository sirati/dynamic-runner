//! Tests for the primary coordinator. Fixtures live in
//! `super::test_helpers`; this file holds the test scenarios.

use super::test_helpers::{
    fake_secondary, fake_secondary_with_addrs, make_binary, make_relative_binary, setup_test,
    FakeWorkerFactory, FixedEstimator, NoPeers, TestId,
};
use super::*;
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::{
    ChannelPrimaryTransportEnd, ChannelSecondaryTransportEnd,
};
use crate::secondary::{SecondaryConfig, SecondaryCoordinator};
use std::collections::HashMap;
use tokio::sync::mpsc as tokio_mpsc;

/// Phase 4b: tests that don't care about phase lifecycle pass an empty
/// dep map and no-op closures. Centralised here so individual tests
/// stay focused on the wire-flow they actually exercise.
fn noop_phase_args() -> (
    HashMap<dynrunner_core::PhaseId, Vec<dynrunner_core::PhaseId>>,
    OnPhaseStart,
    OnPhaseEnd,
) {
    (HashMap::new(), Box::new(|_| {}), Box::new(|_, _, _| {}))
}


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

/// Regression for the asm-tokenizer LMU CIP Tier-3 2-of-235 hang.
///
/// Scenario (setup-promote + retry-success):
///   - Local submitter is the demoted primary
///     (`required_setup_on_promote = true`) — its `operational_loop`
///     is gated off the counter-based exit by the `partial_view` guard
///     and can ONLY exit via the authoritative
///     `cluster_state.run_complete()` signal.
///   - The promoted secondary discovers a small task graph including
///     one binary that fails Recoverably on its first attempt and
///     succeeds on its retry-pass redispatch.
///   - All other binaries succeed on the first attempt.
///
/// Pre-fix shape of the bug:
///   1. The CRDT `apply(TaskCompleted)` arm short-circuited to NoOp when
///      the target hash was already in `TaskState::Failed { .. }`,
///      leaving the retry-succeeded task stuck in the ledger as
///      `Failed { Recoverable }`. `outcome_counts()` then reported
///      `succeeded = N-1, fail_retry = 1` even though every task had
///      ultimately succeeded.
///   2. The promoted secondary's RunComplete-broadcast trigger required
///      `primary_disconnected`, but the demoted local primary was
///      sitting in operational_loop waiting for RunComplete and never
///      disconnected first — circular wait, deadlock. The demoted's
///      `run()` would hang for the SLURM job's full wall-clock budget
///      (asm-tokenizer LMU saw the 1200s harness kill).
///
/// Post-fix invariants pinned here:
///   (A) `cluster_state.outcome_counts().succeeded == total` —
///       retry-success transitions Failed → Completed in the CRDT.
///   (B) `cluster_state.outcome_counts().fail_retry == 0` — no task
///       is stuck reporting as recoverable-failed after its retry
///       succeeded.
///   (C) `cluster_state.run_complete()` is set on the demoted primary
///       — the natural-quiesce broadcast on the promoted secondary
///       (independent of `primary_disconnected`) drove the demoted's
///       exit cue.
///   (D) `primary.run()` returns `Ok(())` within a bounded wait — no
///       hang.
///
/// Test rig:
///   - `required_setup_on_promote = true` so the demoted local sits in
///     partial-view mode (`total_tasks = 0` until a `TaskAdded` arrives;
///     counter exit gated by `partial_view`).
///   - A driver task spawns the real secondary on a local-set task,
///     calls `run_until_setup_or_done`, observes `SetupPending`, calls
///     `ingest_setup_discovery` with three binaries (one of which is
///     `flaky` — quota=1 on `FlakyWorkerFactory`), then re-enters
///     `run_until_setup_or_done` until it returns `Done`. This mirrors
///     the PyO3 secondary wrapper's contract.
///   - Bounded `tokio::time::timeout` around `primary.run()` distinguishes
///     "natural cue fired" from "fell back to transport-closed exit on
///     timeout"; the test fails loudly on the latter.
#[tokio::test(flavor = "current_thread")]
async fn setup_promote_run_with_retry_success_completes_via_runcomplete() {
    use crate::secondary::RunOutcome;

    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let max_res = dynrunner_core::ResourceMap::from(
            [(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024u64)]
        );
        // Quota=1 for "flaky": fail attempt 1 (Recoverable), succeed
        // from attempt 2 onwards. The other two binaries have no quota
        // entry → succeed on attempt 1.
        let mut quotas = HashMap::new();
        quotas.insert("/tmp/flaky".to_string(), 1u32);
        let flaky = super::test_helpers::FlakyWorkerFactory::with_quotas(quotas);

        let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
        let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();

        // Three binaries: discovered by the secondary inside its
        // setup-promote yield. `make_binary` builds `/tmp/<name>`
        // paths so the FlakyWorkerFactory's relative-path quota key
        // matches.
        let discovered = vec![
            make_binary("ok-1", 50),
            make_binary("flaky", 40),
            make_binary("ok-2", 30),
        ];
        let total = discovered.len();
        let phase_deps: HashMap<dynrunner_core::PhaseId, Vec<dynrunner_core::PhaseId>>
            = HashMap::new();

        // Drive the secondary: run_until_setup_or_done → on
        // SetupPending, call ingest_setup_discovery with the three
        // discovered binaries, then re-enter until Done. This mirrors
        // the PyO3 secondary wrapper's two-call contract (the only
        // production caller that drives setup-promote).
        let discovered_for_secondary = discovered.clone();
        let phase_deps_for_secondary = phase_deps.clone();
        let sec_handle = tokio::task::spawn_local(async move {
            let transport = ChannelPrimaryTransportEnd {
                tx: sec_to_pri_tx,
                rx: pri_to_sec_rx,
            };
            let config = SecondaryConfig {
                secondary_id: "sec-0".into(),
                num_workers: 2,
                max_resources: max_res,
                hostname: "test-host".into(),
                // Tight keepalive so the natural-quiesce branch ticks
                // promptly once primary_pending + primary_in_flight +
                // active_tasks all drain — the assertion budget is 10s
                // and we don't want CI flake from a slow heartbeat.
                keepalive_interval: Duration::from_millis(50),
                src_network: None,
                src_tmp: None,
                peer_timeout: Duration::from_secs(120),
                keepalive_miss_threshold: 3,
                retry_max_passes: 1,
                primary_link_failure_threshold: 5,
                primary_link_failure_window: Duration::from_secs(30),
                setup_deadline: Duration::from_secs(60),
                is_observer: false,
            };
            let mut secondary = SecondaryCoordinator::new(
                config,
                transport,
                NoPeers,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let mut factory = flaky;
            loop {
                match secondary.run_until_setup_or_done(&mut factory).await {
                    Ok(RunOutcome::Done) => break,
                    Ok(RunOutcome::SetupPending) => {
                        secondary
                            .ingest_setup_discovery(
                                discovered_for_secondary.clone(),
                                phase_deps_for_secondary.clone(),
                            )
                            .await
                            .expect("ingest_setup_discovery succeeds");
                        // Re-enter; the next iteration's
                        // process_tasks sees setup_pending cleared and
                        // the hydrated pool.
                    }
                    Err(e) => panic!("secondary.run_until_setup_or_done: {e}"),
                }
            }
            (
                secondary.completed_count(),
                secondary.primary_failed_count_for_test(),
                secondary.primary_retry_passes_used_for_test(),
            )
        });

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
            // Pass binary paths through without hash-verifying against
            // a staged source tree — the test fixtures don't materialise
            // real files at `/tmp/<name>` and the resolver's existence
            // check would drop the dispatch. The FlakyWorkerFactory
            // doesn't read the file either, so passthrough is fine.
            uses_file_based_items: false,
            // Setup-promote mode: the LMU CIP path. Demoted local
            // primary skips `seed_cluster_state` + `perform_initial_assignment`;
            // total_tasks = 0 at run start; counter-based exit gated
            // by `partial_view` (demoted && required_setup_on_promote).
            required_setup_on_promote: true,
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

        // Setup-promote contract: primary.run() is called with an
        // EMPTY binaries vector (the submitter has no local view of
        // the corpus — `--source-already-staged` mode). The promoted
        // secondary owns discovery + ledger seed via
        // `ingest_setup_discovery`, driven from the spawn_local task
        // above.
        let (deps, ops, ope) = noop_phase_args();
        let run_outcome = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            primary.run(vec![], deps, ops, ope),
        )
        .await;

        match run_outcome {
            Ok(Ok(())) => {
                // Invariant (D): clean Ok return. Distinguishes from
                // (a) the pre-Fix-A NoOp on Failed → Completed which
                // would have left cluster_state stuck reporting one
                // task as fail_retry indefinitely, and (b) the pre-
                // Fix-B circular-wait deadlock where the demoted
                // primary's `cluster_state.run_complete()` exit cue
                // never fired because the promoted secondary's
                // RunComplete broadcast required `primary_disconnected`.
            }
            Ok(Err(e)) => panic!(
                "primary.run() returned an error on a clean setup-promote \
                 retry-success scenario: {e}"
            ),
            Err(_elapsed) => panic!(
                "primary.run() did not return within 10s — pre-fix \
                 deadlock regression. The demoted local primary's \
                 partial-view operational_loop is waiting for \
                 RunComplete, but the promoted secondary never \
                 broadcast it because its RunComplete trigger was \
                 gated on `primary_disconnected` (which the demoted \
                 local never satisfied since it's stuck waiting for \
                 RunComplete)."
            ),
        }

        let outcome = primary.outcome_summary();
        let cluster_state_counts = primary.cluster_state_counts_for_test();

        // Invariant (A): every binary, including the retry-succeeded
        // `flaky`, lands in the `succeeded` partition. Pre-Fix-A the
        // CRDT NoOp on Failed → Completed left `flaky` stuck as
        // `Failed { Recoverable }` and `outcome.succeeded` plateaued
        // at `total - 1`.
        assert_eq!(
            outcome.succeeded, total,
            "outcome.succeeded must equal total ({total}) — retry-succeeded \
             tasks must transition Failed → Completed in cluster_state \
             (Fix A). Got outcome={outcome:?}, cluster_state_counts={cluster_state_counts:?}"
        );

        // Invariant (B): the retry-success has emptied the
        // `fail_retry` partition. The same CRDT transition that
        // populates `succeeded` correctly clears `fail_retry` — pre-
        // fix this stayed pinned at 1 indefinitely.
        assert_eq!(
            outcome.fail_retry, 0,
            "outcome.fail_retry must be 0 after every retry has either \
             succeeded or exhausted budget — pre-fix CRDT left the \
             retry-succeeded task stuck as Failed{{Recoverable}}. \
             Got outcome={outcome:?}"
        );
        assert_eq!(outcome.fail_oom, 0, "no OOM failures in this scenario");
        assert_eq!(outcome.fail_final, 0, "no permanent failures in this scenario");

        // Invariant (C): RunComplete actually fired on the demoted
        // local's mirror. Pre-Fix-B the demoted local sat in
        // operational_loop waiting for `cluster_state.run_complete()`,
        // which only flips on a received `ClusterMutation::RunComplete`;
        // the promoted secondary's broadcast was gated on
        // `primary_disconnected` and never fired in the alive-demoted
        // scenario.
        assert!(
            primary.cluster_state_for_test().run_complete(),
            "cluster_state.run_complete() must be true after primary.run() \
             returns — the promoted secondary's natural-quiesce branch \
             must have broadcast ClusterMutation::RunComplete (Fix B). \
             Pre-fix this stayed false and primary.run() would only \
             return via the 10s timeout fallback (caught above)."
        );

        // Stranded must be zero: every task reached a terminal state.
        assert_eq!(
            primary.stranded_count(), 0,
            "no task should be stranded on a clean retry-success run"
        );

        drop(primary);

        let (completed, failed_residual, passes_used) =
            sec_handle.await.unwrap();
        assert_eq!(
            completed, total,
            "secondary's `completed_tasks` (the per-hash terminal set) \
             must cover every binary"
        );
        assert_eq!(
            failed_residual, 0,
            "primary_failed should be empty after retry-success"
        );
        assert_eq!(
            passes_used, 1,
            "exactly one retry pass should have been consumed"
        );
    }).await;
}

/// Regression for the asm-tokenizer Tier-2 hang (post-`a78c89c`):
/// multi-secondary setup-promote + all-success natural quiesce
/// must broadcast `RunComplete` and let the run terminate, even when
/// the chosen secondary's task workload happens to all dispatch
/// to its own workers (peer secondaries stay idle).
///
/// Shape mirrors the 1-secondary `setup_promote_run_with_retry_success_completes_via_runcomplete`
/// test but with FOUR real secondaries on a real peer mesh:
///
///   - 4 SecondaryCoordinators wired via `peer_mesh` (all-to-all
///     `ChannelPeerTransport`) so each one has 3 real peers.
///   - The submitter's PrimaryCoordinator is in setup-promote mode
///     (`required_setup_on_promote = true`). It demotes itself and
///     hands authority to the first secondary in its `secondaries`
///     map ordering.
///   - Only the chosen / promoted secondary drives discovery via
///     `run_until_setup_or_done` → `ingest_setup_discovery`.
///     The other three run plain `run()` (which loops on
///     `run_until_setup_or_done` internally and never observes
///     `SetupPending` because PromotePrimary targets a peer, not
///     them).
///   - 5 binaries, all succeed on first attempt (no retries).
///     Workers on the chosen secondary are fast enough that in
///     production-shaped runs the entire workload dispatches to
///     its own pool before peers send TaskRequests; the test pins
///     this end-state regardless.
///
/// Pre-fix Tier-2 symptom: after the last TaskComplete the promoted
/// secondary's natural-quiesce branch fails to fire RunComplete,
/// `primary.run()` hangs past the bounded timeout, the assertion
/// trips.
///
/// Post-fix invariants:
///   (A) `primary.run()` returns `Ok(())` within 10s.
///   (B) `primary.cluster_state.run_complete()` is true.
///   (C) Every binary terminates as `Completed` (`outcome.succeeded
///       == total`, no `fail_retry` / `fail_oom` / `fail_final`).
#[tokio::test(flavor = "current_thread")]
async fn setup_promote_multi_secondary_natural_quiesce_completes_via_runcomplete() {
    use crate::secondary::RunOutcome;
    use dynrunner_transport_channel::{peer_mesh, ChannelPeerTransport};

    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        const N_SECONDARIES: usize = 4;
        let secondary_ids: Vec<String> =
            (0..N_SECONDARIES).map(|i| format!("sec-{i}")).collect();

        // Build the peer mesh up front so every secondary's
        // `ChannelPeerTransport` already has the full adjacency
        // populated. `peer_mesh` returns one transport per id in
        // input order; we pop them off into the per-secondary spawn
        // sites below.
        let mut peer_transports: Vec<ChannelPeerTransport<TestId>> =
            peer_mesh(&secondary_ids);

        // 5 binaries — small enough that the chosen secondary's
        // own two workers cover the entire workload, mirroring the
        // production scenario where `--jobs 4` * 2 workers >> 20
        // tasks and a fast secondary grabs everything before
        // peer-TaskRequest backoff cycles.
        let discovered: Vec<TaskInfo<TestId>> = (0..5)
            .map(|i| make_binary(&format!("bin-{i}"), 50 + i * 10))
            .collect();
        let total = discovered.len();
        let phase_deps: HashMap<dynrunner_core::PhaseId, Vec<dynrunner_core::PhaseId>>
            = HashMap::new();

        // Per-secondary primary-side channel pairs.
        let mut pri_to_sec_txs: HashMap<String, _> = HashMap::new();
        let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
        let mut sec_handles: Vec<tokio::task::JoinHandle<(String, usize)>> = Vec::new();

        // The "chosen" secondary id is whichever `secondaries.keys().next()`
        // picks first inside `lifecycle.rs::promote_primary` — HashMap
        // iteration order. We don't try to predict it; every spawned
        // secondary is prepared to drive the setup-pending yield.
        for (idx, secondary_id) in secondary_ids.iter().enumerate() {
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            pri_to_sec_txs.insert(secondary_id.clone(), pri_to_sec_tx);

            // Wire the per-secondary upstream into the primary's
            // aggregated incoming channel.
            let tx = incoming_tx.clone();
            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if tx.send(msg).is_err() {
                        break;
                    }
                }
            });

            let peer_transport = peer_transports.remove(0);
            let discovered_local = discovered.clone();
            let phase_deps_local = phase_deps.clone();
            let secondary_id_local = secondary_id.clone();
            let max_res = dynrunner_core::ResourceMap::from(
                [(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024u64)]
            );

            let handle = tokio::task::spawn_local(async move {
                let transport = ChannelPrimaryTransportEnd {
                    tx: sec_to_pri_tx,
                    rx: pri_to_sec_rx,
                };
                let config = SecondaryConfig {
                    secondary_id: secondary_id_local.clone(),
                    num_workers: 2,
                    max_resources: max_res,
                    hostname: "test-host".into(),
                    // Tight keepalive so the natural-quiesce branch
                    // ticks promptly once everything drains.
                    keepalive_interval: Duration::from_millis(50),
                    src_network: None,
                    src_tmp: None,
                    peer_timeout: Duration::from_secs(120),
                    keepalive_miss_threshold: 3,
                    retry_max_passes: 1,
                    primary_link_failure_threshold: 5,
                    primary_link_failure_window: Duration::from_secs(30),
                    setup_deadline: Duration::from_secs(60),
                    is_observer: false,
                };
                let mut secondary = SecondaryCoordinator::new(
                    config,
                    transport,
                    peer_transport,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );
                let mut factory = FakeWorkerFactory;
                // Every secondary runs the same wrapper-style loop.
                // Only the chosen one will observe SetupPending; the
                // others fall through `run_until_setup_or_done` →
                // `Done` after the run completes.
                loop {
                    match secondary.run_until_setup_or_done(&mut factory).await {
                        Ok(RunOutcome::Done) => break,
                        Ok(RunOutcome::SetupPending) => {
                            secondary
                                .ingest_setup_discovery(
                                    discovered_local.clone(),
                                    phase_deps_local.clone(),
                                )
                                .await
                                .expect("ingest_setup_discovery succeeds");
                        }
                        Err(e) => panic!("sec-{idx}.run_until_setup_or_done: {e}"),
                    }
                }
                (secondary_id_local, secondary.completed_count())
            });
            sec_handles.push(handle);
        }
        drop(incoming_tx);

        let transport = ChannelSecondaryTransportEnd {
            outgoing: pri_to_sec_txs,
            incoming_rx,
        };
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: N_SECONDARIES as u32,
            connect_timeout: Duration::from_secs(10),
            peer_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_millis(50),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            // FakeWorkerFactory doesn't read the task file; passthrough
            // matches the existing setup-promote test fixture.
            uses_file_based_items: false,
            required_setup_on_promote: true,
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

        let (deps, ops, ope) = noop_phase_args();
        let run_outcome = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            primary.run(vec![], deps, ops, ope),
        )
        .await;

        match run_outcome {
            Ok(Ok(())) => {}
            Ok(Err(e)) => panic!(
                "primary.run() returned an error on a multi-secondary \
                 setup-promote happy-path: {e}"
            ),
            Err(_elapsed) => panic!(
                "primary.run() did not return within 10s — Tier-2 \
                 regression: the promoted secondary's natural-quiesce \
                 branch failed to broadcast RunComplete in the \
                 multi-secondary case, leaving the demoted primary's \
                 partial-view operational_loop waiting forever."
            ),
        }

        // Invariant (B): RunComplete actually fired on the demoted
        // local's cluster_state mirror.
        assert!(
            primary.cluster_state_for_test().run_complete(),
            "cluster_state.run_complete() must be true after the \
             promoted secondary's natural-quiesce branch fires the \
             RunComplete broadcast in a multi-secondary mesh."
        );

        // Invariant (C): every binary terminates as Completed.
        let outcome = primary.outcome_summary();
        assert_eq!(
            outcome.succeeded, total,
            "outcome.succeeded must equal total ({total}) — every \
             binary should reach the Completed terminal state on \
             a clean multi-secondary natural-quiesce run. \
             Got outcome={outcome:?}"
        );
        assert_eq!(outcome.fail_retry, 0);
        assert_eq!(outcome.fail_oom, 0);
        assert_eq!(outcome.fail_final, 0);
        assert_eq!(primary.stranded_count(), 0);

        drop(primary);

        // Every secondary must exit Done (not panic, not hang).
        for handle in sec_handles {
            let (sid, completed) = handle.await.unwrap();
            // The cluster-wide completed_tasks set on every secondary
            // covers the full task list — that's the failover-
            // survivability contract from `cluster_state_converges_*`.
            // Multi-secondary peers receive the broadcast TaskCompleted
            // via primary_transport (from the demoted local's
            // re-broadcast) AND the originator's TaskComplete via the
            // peer mesh, so every secondary's `completed_tasks` set
            // grows to `total`.
            assert!(
                completed >= total,
                "secondary {sid} should have observed all {total} \
                 completions (cluster-wide failover-survivability view); \
                 got {completed}"
            );
        }
    }).await;
}

/// Regression for the asm-tokenizer Tier-2 RunComplete-writer-flush race
/// (post-`cd729fe`).
///
/// Scenario: same multi-secondary natural-quiesce shape as
/// `setup_promote_multi_secondary_natural_quiesce_completes_via_runcomplete`,
/// but with each secondary's `primary_transport` wrapped in a
/// `BufferedPrimaryTransport` that mimics the production
/// `NetworkClient`'s bridge pattern:
///
///   - Outgoing messages enter an internal mpsc; a `spawn_local` writer
///     task drains the mpsc to the inner channel transport with a
///     deliberate `yield_now().await` per message so the runtime has
///     a chance to drop the wrapper before the message is forwarded.
///   - `Drop` aborts the writer task — identical to
///     `BridgedConnection::Drop` in
///     `dynrunner-transport-quic::network::client`.
///
/// Pre-fix wire-flow (race):
///   1. Promoted secondary's natural-quiesce branch enqueues
///      `ClusterMutation::RunComplete` via
///      `primary_transport.send.await`. The send returns as soon as
///      the wrapper's mpsc enqueue succeeds — the writer task has not
///      yet picked it up.
///   2. The exit-check on the SAME loop iteration sees
///      `cluster_state.run_complete() == true` (the local
///      `cluster_state.apply` flipped the flag before broadcast) and
///      breaks out of `process_tasks`.
///   3. `process_tasks` returns; the spawn_local task running the
///      secondary completes; `SecondaryCoordinator` drops;
///      `BufferedPrimaryTransport` drops; writer task aborts before
///      forwarding the RunComplete.
///   4. The workstation primary never observes RunComplete; the
///      demoted-local operational loop's partial-CRDT-view guard
///      keeps it waiting; `primary.run()` hangs past the timeout.
///
/// Post-fix invariant: after the broadcast, the natural-quiesce branch
/// awaits `primary_transport.flush()` (bounded by `FLUSH_DEADLINE`).
/// The flush rendezvous round-trips through the writer task; the
/// writer therefore drains the RunComplete to the inner channel before
/// signalling the oneshot, after which the secondary is free to exit
/// and the workstation primary observes RunComplete normally.
#[tokio::test(flavor = "current_thread")]
async fn promoted_secondary_flushes_primary_transport_before_natural_quiesce_exit() {
    use crate::secondary::RunOutcome;
    use dynrunner_core::{MessageReceiver, MessageSender};
    use dynrunner_protocol_primary_secondary::DistributedMessage;
    use dynrunner_transport_channel::{peer_mesh, ChannelPeerTransport, ChannelPrimaryTransportEnd};
    use tokio::sync::oneshot;

    // Outgoing-channel payload mirroring the production
    // `NetworkClient`'s Outgoing enum: messages and flush markers
    // share one FIFO, so a flush only fires AFTER every preceding
    // message has been forwarded.
    enum BufOut {
        Msg(DistributedMessage<TestId>),
        Flush(oneshot::Sender<()>),
    }

    /// Buffered wrapper around an inner primary transport that
    /// forwards via a `spawn_local` writer task, with `Drop`
    /// aborting the writer. Mimics
    /// `dynrunner-transport-quic::network::client::BridgedConnection`.
    struct BufferedPrimaryTransport {
        outgoing_tx: tokio_mpsc::UnboundedSender<BufOut>,
        // Inner recv is decoupled from the writer — we just
        // forward the receive side directly. The race lives on
        // the SEND path only.
        rx: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        writer: tokio::task::JoinHandle<()>,
    }

    impl Drop for BufferedPrimaryTransport {
        fn drop(&mut self) {
            // Identical to BridgedConnection::Drop — abort the
            // writer task immediately. Any messages still in the
            // mpsc are silently lost. This is the very behaviour
            // the production fix exists to defend against.
            self.writer.abort();
        }
    }

    impl MessageSender<DistributedMessage<TestId>> for BufferedPrimaryTransport {
        async fn send(
            &mut self,
            msg: DistributedMessage<TestId>,
        ) -> Result<(), String> {
            self.outgoing_tx
                .send(BufOut::Msg(msg))
                .map_err(|_| "buffered transport writer task exited".to_string())
        }

        async fn flush(&mut self) -> Result<(), String> {
            let (tx, rx) = oneshot::channel();
            self.outgoing_tx
                .send(BufOut::Flush(tx))
                .map_err(|_| "buffered transport writer task exited".to_string())?;
            rx.await
                .map_err(|_| "buffered transport writer task exited before flush ack".to_string())
        }
    }

    impl MessageReceiver<DistributedMessage<TestId>> for BufferedPrimaryTransport {
        async fn recv(&mut self) -> Option<DistributedMessage<TestId>> {
            self.rx.recv().await
        }
    }

    fn wrap_buffered(
        inner: ChannelPrimaryTransportEnd<TestId>,
    ) -> BufferedPrimaryTransport {
        let ChannelPrimaryTransportEnd {
            tx: inner_tx,
            rx: inner_rx,
        } = inner;
        let (outgoing_tx, mut outgoing_rx) = tokio_mpsc::unbounded_channel::<BufOut>();
        let writer = tokio::task::spawn_local(async move {
            while let Some(item) = outgoing_rx.recv().await {
                match item {
                    BufOut::Msg(msg) => {
                        // Sleep BEFORE forwarding to make the race
                        // deterministic in `current_thread`-flavour
                        // tests: a plain `yield_now` would race with
                        // whatever order the scheduler picks tasks
                        // in, but a timer puts us back on the queue
                        // strictly after a wall-clock delay, giving
                        // the secondary's exit path time to drop
                        // the wrapper (and abort us) BEFORE the
                        // forward fires — exactly the production
                        // SSH-tunnel-latency shape on the
                        // RunComplete-after-natural-quiesce path.
                        //
                        // Without flush(): drop happens during the
                        // sleep, the abort kills this task, the
                        // inner_tx.send below never runs, the
                        // workstation primary never receives the
                        // message. With flush(): the secondary
                        // awaits its flush rendezvous before
                        // returning, so we observe the Flush marker
                        // AFTER all preceding Msg writes have
                        // forwarded (FIFO contract).
                        tokio::time::sleep(
                            std::time::Duration::from_millis(50),
                        )
                        .await;
                        if inner_tx.send(msg).is_err() {
                            break;
                        }
                    }
                    BufOut::Flush(ack) => {
                        let _ = ack.send(());
                    }
                }
            }
        });
        BufferedPrimaryTransport {
            outgoing_tx,
            rx: inner_rx,
            writer,
        }
    }

    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        const N_SECONDARIES: usize = 4;
        let secondary_ids: Vec<String> =
            (0..N_SECONDARIES).map(|i| format!("sec-{i}")).collect();

        let mut peer_transports: Vec<ChannelPeerTransport<TestId>> =
            peer_mesh(&secondary_ids);

        let discovered: Vec<TaskInfo<TestId>> = (0..5)
            .map(|i| make_binary(&format!("bin-{i}"), 50 + i * 10))
            .collect();
        let total = discovered.len();
        let phase_deps: HashMap<dynrunner_core::PhaseId, Vec<dynrunner_core::PhaseId>>
            = HashMap::new();

        let mut pri_to_sec_txs: HashMap<String, _> = HashMap::new();
        let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
        // Keep an extra incoming_tx clone alive in the test scope for the
        // entire run. Production's `NetworkServer` does NOT report its
        // receive end as closed merely because one WSS connection drops —
        // it keeps the underlying mpsc open for the lifetime of the
        // primary process. The default channel-transport test fixture
        // (which `drop(incoming_tx)` after spawning forwarders) collapses
        // the receive end the moment every secondary exits, masking the
        // RunComplete-writer-flush race because the primary's
        // operational loop exits via the "transport closed" arm instead
        // of waiting on a `cluster_state.run_complete()` mirror update.
        // Pinning the tx here makes the fixture model match production:
        // the primary can only exit via the RunComplete branch, so the
        // race is observable.
        let _incoming_tx_pin = incoming_tx.clone();
        let mut sec_handles: Vec<tokio::task::JoinHandle<(String, usize)>> = Vec::new();

        for (idx, secondary_id) in secondary_ids.iter().enumerate() {
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            pri_to_sec_txs.insert(secondary_id.clone(), pri_to_sec_tx);

            let tx = incoming_tx.clone();
            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if tx.send(msg).is_err() {
                        break;
                    }
                }
            });

            let peer_transport = peer_transports.remove(0);
            let discovered_local = discovered.clone();
            let phase_deps_local = phase_deps.clone();
            let secondary_id_local = secondary_id.clone();
            let max_res = dynrunner_core::ResourceMap::from(
                [(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024u64)]
            );

            let handle = tokio::task::spawn_local(async move {
                let inner = ChannelPrimaryTransportEnd {
                    tx: sec_to_pri_tx,
                    rx: pri_to_sec_rx,
                };
                // Buffered wrap — every secondary's primary_transport
                // now has a writer-task / Drop-aborts shape, so the
                // RunComplete-writer-flush race is observable here.
                let transport = wrap_buffered(inner);
                let config = SecondaryConfig {
                    secondary_id: secondary_id_local.clone(),
                    num_workers: 2,
                    max_resources: max_res,
                    hostname: "test-host".into(),
                    keepalive_interval: std::time::Duration::from_millis(50),
                    src_network: None,
                    src_tmp: None,
                    peer_timeout: std::time::Duration::from_secs(120),
                    keepalive_miss_threshold: 3,
                    retry_max_passes: 1,
                    primary_link_failure_threshold: 5,
                    primary_link_failure_window: std::time::Duration::from_secs(30),
                    setup_deadline: std::time::Duration::from_secs(60),
                    is_observer: false,
                };
                let mut secondary = SecondaryCoordinator::new(
                    config,
                    transport,
                    peer_transport,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );
                let mut factory = FakeWorkerFactory;
                loop {
                    match secondary.run_until_setup_or_done(&mut factory).await {
                        Ok(RunOutcome::Done) => break,
                        Ok(RunOutcome::SetupPending) => {
                            secondary
                                .ingest_setup_discovery(
                                    discovered_local.clone(),
                                    phase_deps_local.clone(),
                                )
                                .await
                                .expect("ingest_setup_discovery succeeds");
                        }
                        Err(e) => panic!("sec-{idx}.run_until_setup_or_done: {e}"),
                    }
                }
                (secondary_id_local, secondary.completed_count())
            });
            sec_handles.push(handle);
        }
        drop(incoming_tx);

        let transport = ChannelSecondaryTransportEnd {
            outgoing: pri_to_sec_txs,
            incoming_rx,
        };
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: N_SECONDARIES as u32,
            connect_timeout: std::time::Duration::from_secs(10),
            peer_timeout: std::time::Duration::from_secs(10),
            keepalive_interval: std::time::Duration::from_millis(50),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: false,
            required_setup_on_promote: true,
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

        let (deps, ops, ope) = noop_phase_args();
        let run_outcome = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            primary.run(vec![], deps, ops, ope),
        )
        .await;

        match run_outcome {
            Ok(Ok(())) => {}
            Ok(Err(e)) => panic!(
                "primary.run() returned an error on the buffered \
                 multi-secondary natural-quiesce flow: {e}"
            ),
            Err(_elapsed) => panic!(
                "primary.run() did not return within 10s — \
                 RunComplete-writer-flush race regressed: the \
                 promoted secondary exited before its buffered \
                 primary_transport's writer task forwarded \
                 RunComplete, the workstation primary never \
                 observed run_complete=true on its CRDT mirror, \
                 and the demoted-local operational loop's \
                 partial-view guard held it indefinitely. \
                 Expected: secondary awaits primary_transport.flush() \
                 after broadcast, writer drains, primary observes \
                 RunComplete, primary returns Ok(())."
            ),
        }

        // The primary's `cluster_state` mirror must observe
        // `run_complete()` — the direct assertion that the
        // RunComplete actually crossed `primary_transport` and
        // wasn't lost in the writer-task abort.
        assert!(
            primary.cluster_state_for_test().run_complete(),
            "primary.cluster_state.run_complete() must be true: \
             the promoted secondary's flush() must rendezvous with \
             its primary_transport writer task BEFORE \
             process_tasks returns, so the RunComplete reaches \
             this primary's CRDT mirror across the buffered \
             writer-task hop."
        );

        let outcome = primary.outcome_summary();
        assert_eq!(
            outcome.succeeded, total,
            "every binary should reach Completed across the \
             buffered-transport natural-quiesce run; outcome={outcome:?}"
        );

        drop(primary);
        for handle in sec_handles {
            let _ = handle.await;
        }
    }).await;
}

/// Regression for the asm-tokenizer `--jobs 4` 20/0/0/0 dispatch bias.
///
/// Scenario: multi-secondary setup-promote on a real peer mesh,
/// keepalive interval set to the production default (5s) so peers'
/// `repoll_idle_workers` doesn't auto-retry inside the test's wall-
/// clock budget.
///
/// Pre-fix shape of the race:
///   1. Every secondary's process-tasks entry for-loop sends an
///      initial `TaskRequest` for each idle worker. At that point
///      `primary_link.current_primary() == None`, so requests route via
///      `primary_transport.send` to the still-live demoted local.
///   2. The demoted local skips local-assign (`!self.demoted` gate in
///      `primary/task.rs::handle_task_request`) and tries to relay via
///      `peer_transport.send(Address::Role(Primary), msg)`. The
///      role-table cache is empty pre-PromotePrimary; the relay drops.
///   3. `note_request_sent` already bumped each worker's backoff
///      window. The next attempt only fires on `repoll_idle_workers`,
///      called on the keepalive tick (5s).
///   4. Meanwhile the chosen secondary's PromotePrimary lands; it
///      runs discovery, hydrates `primary_pending`, and its own two
///      workers self-assign synchronously in the entry for-loop.
///      With FakeWorker (instant) the entire workload burns through
///      before any peer's 5s repoll fires → peers' `local_tasks_run
///      == 0` post-run, the promoted node ran every task.
///
/// Post-fix invariant: `on_primary_changed` (in the `PromotePrimary`
/// arm of `dispatch.rs`) now calls `repoll_idle_workers` immediately
/// after resetting backoff + installing the new routing target — every
/// idle worker re-issues against the freshly-identified primary inside
/// the same dispatch tick, no 5s wait. With 4 secondaries × 2 workers
/// = 8 worker slots and 20 binaries, every secondary should run a
/// non-zero share.
///
/// Test rig: identical to
/// `setup_promote_multi_secondary_natural_quiesce_completes_via_runcomplete`
/// but bumps `keepalive_interval` to 5s (production default) and
/// `total` to 20 (matching the production recipe's binary count).
/// Asserts every secondary's `local_tasks_run_for_test() > 0` AND
/// the cluster as a whole completes (no hang).
#[tokio::test(flavor = "current_thread")]
async fn setup_promote_multi_secondary_distributes_to_idle_peers_on_promote() {
    use crate::secondary::RunOutcome;
    use dynrunner_transport_channel::{peer_mesh, ChannelPeerTransport};

    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        const N_SECONDARIES: usize = 4;
        let secondary_ids: Vec<String> =
            (0..N_SECONDARIES).map(|i| format!("sec-{i}")).collect();

        let mut peer_transports: Vec<ChannelPeerTransport<TestId>> =
            peer_mesh(&secondary_ids);

        // 20 binaries — matches the asm-tokenizer Tier-2 recipe's
        // `--name-regex minigzipsh --platform x64 --compiler gcc`
        // post-filter count exactly. Enough work that even
        // FakeWorker instant-complete leaves plenty of dispatch
        // opportunities for peers if their workers can re-poll.
        let discovered: Vec<TaskInfo<TestId>> = (0..20)
            .map(|i| make_binary(&format!("bin-{i}"), 50 + i * 5))
            .collect();
        let total = discovered.len();
        let phase_deps: HashMap<dynrunner_core::PhaseId, Vec<dynrunner_core::PhaseId>>
            = HashMap::new();

        let mut pri_to_sec_txs: HashMap<String, _> = HashMap::new();
        let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
        // Per-secondary result: (secondary_id, completed_count,
        // local_tasks_run). `local_tasks_run` is the assertion
        // surface for this test.
        let mut sec_handles: Vec<
            tokio::task::JoinHandle<(String, usize, usize)>,
        > = Vec::new();

        for (idx, secondary_id) in secondary_ids.iter().enumerate() {
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            pri_to_sec_txs.insert(secondary_id.clone(), pri_to_sec_tx);

            let tx = incoming_tx.clone();
            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if tx.send(msg).is_err() {
                        break;
                    }
                }
            });

            let peer_transport = peer_transports.remove(0);
            let discovered_local = discovered.clone();
            let phase_deps_local = phase_deps.clone();
            let secondary_id_local = secondary_id.clone();
            let max_res = dynrunner_core::ResourceMap::from(
                [(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024u64)]
            );

            let handle = tokio::task::spawn_local(async move {
                let transport = ChannelPrimaryTransportEnd {
                    tx: sec_to_pri_tx,
                    rx: pri_to_sec_rx,
                };
                let config = SecondaryConfig {
                    secondary_id: secondary_id_local.clone(),
                    num_workers: 2,
                    max_resources: max_res,
                    hostname: "test-host".into(),
                    // Production-default keepalive: the 5s window that
                    // makes the pre-fix bias visible. Tighter values
                    // (50ms) would mask the bug because the periodic
                    // repoll would catch up within the test's wall-
                    // clock budget.
                    keepalive_interval: Duration::from_secs(5),
                    src_network: None,
                    src_tmp: None,
                    peer_timeout: Duration::from_secs(120),
                    keepalive_miss_threshold: 3,
                    retry_max_passes: 1,
                    primary_link_failure_threshold: 5,
                    primary_link_failure_window: Duration::from_secs(30),
                    setup_deadline: Duration::from_secs(60),
                    is_observer: false,
                };
                let mut secondary = SecondaryCoordinator::new(
                    config,
                    transport,
                    peer_transport,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );
                let mut factory = FakeWorkerFactory;
                loop {
                    match secondary.run_until_setup_or_done(&mut factory).await {
                        Ok(RunOutcome::Done) => break,
                        Ok(RunOutcome::SetupPending) => {
                            secondary
                                .ingest_setup_discovery(
                                    discovered_local.clone(),
                                    phase_deps_local.clone(),
                                )
                                .await
                                .expect("ingest_setup_discovery succeeds");
                        }
                        Err(e) => panic!("sec-{idx}.run_until_setup_or_done: {e}"),
                    }
                }
                (
                    secondary_id_local,
                    secondary.completed_count(),
                    secondary.local_tasks_run_for_test(),
                )
            });
            sec_handles.push(handle);
        }
        drop(incoming_tx);

        let transport = ChannelSecondaryTransportEnd {
            outgoing: pri_to_sec_txs,
            incoming_rx,
        };
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: N_SECONDARIES as u32,
            connect_timeout: Duration::from_secs(10),
            peer_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: false,
            required_setup_on_promote: true,
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

        let (deps, ops, ope) = noop_phase_args();
        // Generous timeout — 5s keepalive interval means the
        // pre-fix path would only re-poll on the *next* tick after
        // the role-table-empty drop; the test should still complete
        // well under 30s post-fix because there's no need to wait
        // for keepalive ticks at all.
        let run_outcome = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            primary.run(vec![], deps, ops, ope),
        )
        .await;

        match run_outcome {
            Ok(Ok(())) => {}
            Ok(Err(e)) => panic!("primary.run() failed: {e}"),
            Err(_elapsed) => panic!(
                "primary.run() did not return within 30s — \
                 something other than the dispatch bias is wrong"
            ),
        }

        assert!(
            primary.cluster_state_for_test().run_complete(),
            "cluster_state.run_complete() must be true"
        );
        let outcome = primary.outcome_summary();
        assert_eq!(
            outcome.succeeded, total,
            "all {total} tasks must complete; got outcome={outcome:?}"
        );

        drop(primary);

        // The load-bearing assertion: every secondary's OWN worker
        // pool ran at least one task. Pre-fix the promoted secondary's
        // pool consumed everything before peers got a second TaskRequest
        // chance on the 5s keepalive; the 3 idle peers had
        // `local_tasks_run == 0`.
        let mut per_sec: Vec<(String, usize)> = Vec::new();
        for handle in sec_handles {
            let (sid, _cluster_seen, local_run) = handle.await.unwrap();
            per_sec.push((sid, local_run));
        }
        let sum_local: usize = per_sec.iter().map(|(_, n)| *n).sum();
        assert_eq!(
            sum_local, total,
            "sum of per-secondary local_tasks_run must equal total \
             ({total}); got per_sec={per_sec:?}"
        );
        for (sid, local_run) in &per_sec {
            assert!(
                *local_run > 0,
                "secondary {sid} ran zero tasks — peer-repoll-on-\
                 PromotePrimary fix regressed; per-secondary \
                 distribution = {per_sec:?}"
            );
        }
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


// ── End-to-end tests: real Primary + real Secondary with workers ──


/// Wire up a real SecondaryCoordinator as a tokio task, connected to the
/// primary via channels. Returns the secondary's channel ends that should
/// be plugged into the primary's ChannelTransport.
fn spawn_real_secondary(
    secondary_id: String,
    num_workers: u32,
    max_resources: dynrunner_core::ResourceMap,
) -> (
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,  // primary→secondary
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>, // secondary→primary
    tokio::task::JoinHandle<usize>,                    // returns completed count
) {
    spawn_real_secondary_with_src_network(secondary_id, num_workers, max_resources, None)
}

fn spawn_real_secondary_with_src_network(
    secondary_id: String,
    num_workers: u32,
    max_resources: dynrunner_core::ResourceMap,
    src_network: Option<std::path::PathBuf>,
) -> (
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    tokio::task::JoinHandle<usize>,
) {
    // primary→secondary channel
    let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
    // secondary→primary channel
    let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();

    let handle = tokio::task::spawn_local(async move {
        let transport = ChannelPrimaryTransportEnd {
            tx: sec_to_pri_tx,
            rx: pri_to_sec_rx,
        };
        let config = SecondaryConfig {
            secondary_id,
            num_workers,
            max_resources,
            hostname: "test-host".into(),
            keepalive_interval: Duration::from_secs(60),
            src_network,
            src_tmp: None,
            peer_timeout: Duration::from_secs(120),
                keepalive_miss_threshold: 3,
            retry_max_passes: 1,
            primary_link_failure_threshold: 5,
            primary_link_failure_window: Duration::from_secs(30),
            setup_deadline: Duration::from_secs(60),
            is_observer: false,
        };
        let mut secondary = SecondaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        let mut factory = FakeWorkerFactory;
        secondary.run(&mut factory).await.unwrap();
        secondary.completed_count()
    });

    (pri_to_sec_tx, sec_to_pri_rx, handle)
}

/// Variant of `spawn_real_secondary` that drives a `FlakyWorkerFactory`
/// and threads `retry_max_passes` into `SecondaryConfig` so the
/// primary's retry pass is governed by the same knob the live
/// primary uses. Returns the
/// `(completed_count, primary_failed_count, retry_passes_used)`
/// triple the primary side ended up with — that's the assertion
/// surface for the post-demotion retry tests, since the local primary's
/// `failed_count()` is a stale forwarding cache once the operational
/// loop's exit condition fires (see `recoverable_failure_succeeds_on_retry_pass`).
///
/// `flaky` is cloned (its `Rc<RefCell<HashMap>>` is shared) so the test
/// caller can also inspect the per-task attempt counts after the run.
fn spawn_real_secondary_flaky(
    secondary_id: String,
    num_workers: u32,
    max_resources: dynrunner_core::ResourceMap,
    flaky: super::test_helpers::FlakyWorkerFactory,
    retry_max_passes: u32,
) -> (
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    tokio::task::JoinHandle<(usize, usize, u32)>,
) {
    let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
    let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();

    let handle = tokio::task::spawn_local(async move {
        let transport = ChannelPrimaryTransportEnd {
            tx: sec_to_pri_tx,
            rx: pri_to_sec_rx,
        };
        let config = SecondaryConfig {
            secondary_id,
            num_workers,
            max_resources,
            hostname: "test-host".into(),
            // Tight keepalive so the keepalive-tick backstop fires
            // quickly enough that tests don't hit the default 60s
            // wait if any code path needs the periodic drain-check
            // (the synchronous one in `note_primary_item_failed` is
            // the primary trigger — this is just defensive).
            keepalive_interval: Duration::from_millis(50),
            src_network: None,
            src_tmp: None,
            peer_timeout: Duration::from_secs(120),
            keepalive_miss_threshold: 3,
            retry_max_passes,
            primary_link_failure_threshold: 5,
            primary_link_failure_window: Duration::from_secs(30),
            setup_deadline: Duration::from_secs(60),
            is_observer: false,
        };
        let mut secondary = SecondaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        let mut factory = flaky;
        secondary.run(&mut factory).await.unwrap();
        (
            secondary.completed_count(),
            secondary.primary_failed_count_for_test(),
            secondary.primary_retry_passes_used_for_test(),
        )
    });

    (pri_to_sec_tx, sec_to_pri_rx, handle)
}

/// End-to-end: 1 real primary + 1 real secondary (2 workers), 5 tasks.
#[tokio::test(flavor = "current_thread")]
async fn e2e_primary_and_secondary_single_node() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let secondary_id = "sec-0".to_string();
        let max_res = dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024u64)]);

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

        let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
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
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries: Vec<TaskInfo<TestId>> = (0..5)
            .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
            .collect();

        { let (deps, ops, ope) = noop_phase_args(); primary.run(binaries, deps, ops, ope).await.unwrap() };

        let completed = primary.completed_count();
        let failed = primary.failed_count();

        // Drop primary to close transport channels, allowing secondaries to exit
        drop(primary);

        let sec_completed = sec_handle.await.unwrap();

        assert_eq!(completed, 5);
        assert_eq!(failed, 0);
        assert_eq!(sec_completed, 5);
    }).await;
}

/// End-to-end: 1 real primary + 2 real secondaries (2 workers each), 10 tasks.
#[tokio::test(flavor = "current_thread")]
async fn e2e_primary_and_two_secondaries() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let max_res = dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), 2 * 1024 * 1024 * 1024u64)]);
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
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries: Vec<TaskInfo<TestId>> = (0..10)
            .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
            .collect();

        { let (deps, ops, ope) = noop_phase_args(); primary.run(binaries, deps, ops, ope).await.unwrap() };

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
        // After the failover-survivability fix, every secondary's
        // `completed_tasks` reflects the CLUSTER view (own work +
        // peer broadcasts + primary-side forwards) so it can serve
        // as a promoted-primary on local-death without
        // re-dispatching done items. Each secondary therefore sees
        // all 10 completions, not just its own ~5. Asserting the
        // cluster-wide invariant directly: every secondary's set
        // has at least the total — anything less is a missed
        // forward that would cause a re-dispatch on failover.
        for (i, count) in per_sec_completed.iter().enumerate() {
            assert!(
                *count >= 10,
                "secondary {i} should have observed all 10 completions \
                 (cluster-wide view for failover survivability), got {count}"
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

/// Pin that `notify_stage_file` actually emits a `StageFile` wire
/// message into the targeted secondary's incoming channel with the
/// exact fields supplied. This is what the packaging pipeline
/// depends on — without correct routing, the ExtractionCache on the
/// receiving secondary never gets primed.
#[tokio::test(flavor = "current_thread")]
async fn notify_stage_file_emits_wire_message() {
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

        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
            PrimaryCoordinator::new(
                config,
                transport,
                NoPeers,
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
    }).await;
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

            let sec_handle = tokio::task::spawn_local(async move {
                let transport = ChannelPrimaryTransportEnd {
                    tx: sec_to_pri_tx,
                    rx: pri_to_sec_rx,
                };
                let config = SecondaryConfig {
                    secondary_id: "sec-0".into(),
                    num_workers: 2,
                    max_resources: max_res,
                    hostname: "test-host".into(),
                    keepalive_interval: Duration::from_secs(60),
                    src_network: None,
                    src_tmp: None,
                    peer_timeout: Duration::from_secs(120),
                    keepalive_miss_threshold: 3,
                    retry_max_passes: 1,
                    primary_link_failure_threshold: 5,
                    primary_link_failure_window: Duration::from_secs(30),
                    setup_deadline: Duration::from_secs(60),
                    is_observer: false,
                };
                let mut secondary = SecondaryCoordinator::new(
                    config,
                    transport,
                    NoPeers,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );
                let mut factory = FakeWorkerFactory;
                secondary.run(&mut factory).await.unwrap();
                (
                    secondary.completed_count(),
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

            let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: 1,
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
            };
            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                NoPeers,
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

            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
                spawn_real_secondary_with_src_network(
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

            let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: 1,
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                keepalive_interval: Duration::from_secs(5),
                keepalive_miss_threshold: 3,
                source_pre_staged_root: Some(gateway_path.clone()),
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

            {
                let (deps, ops, ope) = noop_phase_args();
                primary.run(binaries, deps, ops, ope).await.unwrap()
            };

            let completed = primary.completed_count();
            let failed = primary.failed_count();
            drop(primary);

            let sec_completed = sec_handle.await.unwrap();

            assert_eq!(completed, 5, "primary should see 5 completed in pre-staged mode");
            assert_eq!(failed, 0, "no failures expected");
            assert_eq!(sec_completed, 5, "secondary should resolve all 5 via src_network");
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

            let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: 1,
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                keepalive_interval: Duration::from_secs(5),
                keepalive_miss_threshold: 3,
                source_pre_staged_root: None,
                uses_file_based_items: false,
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

            let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: 1,
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                keepalive_interval: Duration::from_secs(5),
                keepalive_miss_threshold: 3,
                source_pre_staged_root: None,
                uses_file_based_items: true,
                required_setup_on_promote: false,
                max_concurrent_per_type: caps,
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

/// Pin the wire-strip behaviour directly: PrimaryConfig::wire_local_path
/// returns the absolute path verbatim outside pre-staged mode and the
/// relative-to-root form inside it. Paths that don't sit under the
/// root pass through unchanged (the secondary then surfaces the
/// mismatch as NonRecoverable).
#[test]
fn wire_local_path_strips_pre_staged_prefix() {
    let mut cfg = PrimaryConfig::default();

    let mut bin = make_binary("x", 0);
    bin.path = std::path::PathBuf::from("/srv/data/bin_0");

    // Off → verbatim.
    assert_eq!(cfg.wire_local_path(&bin), "/srv/data/bin_0");

    // On with matching prefix (abs-under-src) → relative tail.
    cfg.source_pre_staged_root = Some(std::path::PathBuf::from("/srv/data"));
    assert_eq!(cfg.wire_local_path(&bin), "bin_0");

    // On with mismatching prefix (abs-out-of-tree) → verbatim
    // (consumer misconfig is surfaced downstream by
    // `resolve_pre_staged` returning None, not silently re-routed).
    cfg.source_pre_staged_root = Some(std::path::PathBuf::from("/other/prefix"));
    assert_eq!(cfg.wire_local_path(&bin), "/srv/data/bin_0");

    // On with a relative `binary.path` (rel-under-src — the post-
    // Bug-B wire-id shape consumers emit). Resolving the relative
    // path against the prestaged root and re-stripping yields the
    // original relative form verbatim, which is exactly what
    // `secondary.src_network.join(<wire>)` expects. Pre-fix the
    // relative path silently fell through the strip-prefix Err arm
    // and shipped as-is — the value happened to be correct, but
    // for the wrong reason; this test pins the explicit round-trip.
    cfg.source_pre_staged_root = Some(std::path::PathBuf::from("/srv/data"));
    bin.path = std::path::PathBuf::from("bin_0");
    assert_eq!(cfg.wire_local_path(&bin), "bin_0");

    bin.path = std::path::PathBuf::from("nested/bin_1");
    assert_eq!(cfg.wire_local_path(&bin), "nested/bin_1");
}

/// Multi-secondary mesh-ready gate: the primary must NOT issue
/// `PromotePrimary` until every connected secondary has reported
/// `MeshReady`. Pre-fix the promotion fired ~750µs after cert-
/// exchange completed; the promoted secondary then became
/// authoritative against a still-forming peer mesh, and every
/// pre-mesh-formation peer-broadcast routed into the void for up
/// to 30s. This test pins the new ordering: wire `PromotePrimary`
/// arrives at every fake secondary AFTER all of them have sent
/// their own `MeshReady`. Implementation uses a per-secondary
/// `tokio::sync::oneshot` to gate the MeshReady send so the test
/// can drive the order deterministically.
#[tokio::test(flavor = "current_thread")]
async fn promote_primary_held_until_every_secondary_reports_mesh_ready() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            const N_SECONDARIES: u32 = 3;
            let (transport, secondary_ends) = setup_test(N_SECONDARIES);

            // Per-secondary oneshot triggers. Test drives them in
            // order to enforce: the primary doesn't fire
            // PromotePrimary until ALL three have flipped.
            let mut mesh_triggers: Vec<tokio::sync::oneshot::Sender<()>> = Vec::new();
            // Per-secondary observation: did this secondary see
            // PromotePrimary BEFORE it was allowed to send
            // MeshReady? (true = bug present)
            let mut promote_seen_pre_mesh_observers: Vec<
                tokio::sync::oneshot::Receiver<bool>,
            > = Vec::new();

            for (id, rx, tx) in secondary_ends {
                let (mesh_tx, mesh_rx) = tokio::sync::oneshot::channel::<()>();
                let (obs_tx, obs_rx) = tokio::sync::oneshot::channel::<bool>();
                mesh_triggers.push(mesh_tx);
                promote_seen_pre_mesh_observers.push(obs_rx);
                tokio::task::spawn_local(gated_mesh_secondary(
                    id,
                    2,
                    1024 * 1024 * 1024,
                    rx,
                    tx,
                    mesh_rx,
                    obs_tx,
                ));
            }

            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: N_SECONDARIES,
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
                // Generous timeout so the test can fire triggers
                // sequentially without racing the deadline.
                mesh_ready_timeout: std::time::Duration::from_secs(10),
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

            let binaries: Vec<TaskInfo<TestId>> = (0..6)
                .map(|i| make_binary(&format!("bin_{i}"), 100))
                .collect();

            // Drive the primary's coordination pipeline on a child
            // task so the test body can release MeshReady triggers
            // in sequence and observe the gate.
            let primary_handle = tokio::task::spawn_local(async move {
                let (deps, ops, ope) = noop_phase_args();
                primary.run(binaries, deps, ops, ope).await.unwrap();
                primary.completed_count()
            });

            // Release MeshReady triggers one at a time. Between
            // each release, yield enough times for the primary's
            // wait loop to observe the freshly-arrived
            // MeshReady. The primary must NOT have advanced past
            // `wait_for_mesh_ready` until all three triggers have
            // fired — otherwise the per-secondary "did I see
            // PromotePrimary before being allowed to MeshReady?"
            // observer would have reported true for some of them.
            for trigger in mesh_triggers {
                trigger.send(()).expect("trigger send");
                // Yield repeatedly so the primary task gets a
                // chance to dequeue & process the MeshReady. A
                // single `yield_now` isn't enough on a
                // current_thread runtime when the primary is
                // mid-message, so spam it.
                for _ in 0..16 {
                    tokio::task::yield_now().await;
                }
            }

            // Collect the per-secondary observations. None of
            // them should have seen PromotePrimary before being
            // allowed to send MeshReady.
            for (i, obs) in promote_seen_pre_mesh_observers.into_iter().enumerate() {
                let saw = obs.await.expect("observer recv");
                assert!(
                    !saw,
                    "secondary {i} observed PromotePrimary BEFORE its own \
                     MeshReady was allowed to send — primary's \
                     wait_for_mesh_ready step is not gating PromotePrimary"
                );
            }

            let completed = primary_handle.await.unwrap();
            assert_eq!(completed, 6, "all 6 tasks should complete");
        })
        .await;
}

/// Fake secondary that defers `MeshReady` until the test fires
/// `mesh_trigger`. Reports via `observer` whether it saw
/// `PromotePrimary` arrive before its `MeshReady` was permitted to
/// send (true = bug). Otherwise behaves like `fake_secondary`.
async fn gated_mesh_secondary(
    secondary_id: String,
    num_workers: u32,
    ram_bytes: u64,
    mut incoming_from_primary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    outgoing_to_primary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    mesh_trigger: tokio::sync::oneshot::Receiver<()>,
    observer: tokio::sync::oneshot::Sender<bool>,
) {
    use dynrunner_protocol_primary_secondary::MessageType;

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

    // Race: receive the trigger to send MeshReady against
    // observing PromotePrimary on the inbound path. If
    // PromotePrimary arrives first, the gate failed.
    let mut mesh_trigger_opt = Some(mesh_trigger);
    let mut observer_opt = Some(observer);
    let mut mesh_sent = false;
    let mut promote_seen_pre_mesh = false;

    loop {
        // While we're still pre-MeshReady, race the trigger
        // against an inbound PromotePrimary. After MeshReady has
        // been sent, the trigger arm is removed and we fall back
        // to a normal recv loop.
        if !mesh_sent {
            let trigger = mesh_trigger_opt.as_mut().unwrap();
            tokio::select! {
                _ = trigger => {
                    outgoing_to_primary
                        .send(DistributedMessage::MeshReady {
                            sender_id: secondary_id.clone(),
                            timestamp: 0.0,
                            secondary_id: secondary_id.clone(),
                            peer_count: 0,
                        })
                        .unwrap();
                    mesh_sent = true;
                    mesh_trigger_opt = None;
                    if let Some(obs) = observer_opt.take() {
                        let _ = obs.send(promote_seen_pre_mesh);
                    }
                }
                msg = incoming_from_primary.recv() => match msg {
                    Some(m) => {
                        if matches!(m.msg_type(), MessageType::PromotePrimary) {
                            promote_seen_pre_mesh = true;
                        }
                        handle_inbound_for_gated_secondary(
                            &secondary_id,
                            &outgoing_to_primary,
                            ram_bytes,
                            m,
                        );
                    }
                    None => break,
                },
            }
        } else {
            match incoming_from_primary.recv().await {
                Some(m) => handle_inbound_for_gated_secondary(
                    &secondary_id,
                    &outgoing_to_primary,
                    ram_bytes,
                    m,
                ),
                None => break,
            }
        }
    }
}

fn handle_inbound_for_gated_secondary(
    secondary_id: &str,
    outgoing: &tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    ram_bytes: u64,
    msg: DistributedMessage<TestId>,
) {
    match msg {
        DistributedMessage::PeerInfo { .. } => {}
        DistributedMessage::InitialAssignment {
            zip_files,
            workers_ready,
            ..
        } => {
            // Pair each binary with the worker the primary's
            // `assign_initial` placed it on (positional alignment of
            // `workers_ready[i]` and `zip_files[0].binaries[i]` is
            // `perform_initial_assignment`'s contract). Always
            // emitting `worker_id=0` worked pre-demotion because the
            // primary's kickstart re-dispatch eventually cleared
            // every worker's `current_task` regardless of which one
            // a TaskComplete was attributed to. Post-demotion the
            // primary stops dispatching after `PromotePrimary`, so a
            // mis-attributed TaskComplete leaves the OTHER worker
            // permanently mid-dispatch and `active_workers > 0`
            // forever — operational_loop never terminates.
            let entries: Vec<_> = zip_files
                .iter()
                .flat_map(|zf| zf.binaries.iter())
                .collect();
            for (idx, entry) in entries.iter().enumerate() {
                let worker_id = workers_ready
                    .get(idx)
                    .map(|w| w.worker_id)
                    .unwrap_or(0);
                let _ = outgoing.send(DistributedMessage::TaskComplete {
                    sender_id: secondary_id.into(),
                    timestamp: 0.0,
                    secondary_id: secondary_id.into(),
                    worker_id,
                    task_hash: entry.hash.clone(),
                    result_data: None,
                });
                let _ = outgoing.send(DistributedMessage::TaskRequest {
                    sender_id: secondary_id.into(),
                    timestamp: 0.0,
                    secondary_id: secondary_id.into(),
                    worker_id,
                    available_resources: vec![dynrunner_core::ResourceAmount {
                        kind: dynrunner_core::ResourceKind::memory(),
                        amount: ram_bytes,
                    }],
                });
            }
        }
        DistributedMessage::TransferComplete { .. } => {}
        DistributedMessage::TaskAssignment { file_hash, .. } => {
            let _ = outgoing.send(DistributedMessage::TaskComplete {
                sender_id: secondary_id.into(),
                timestamp: 0.0,
                secondary_id: secondary_id.into(),
                worker_id: 0,
                task_hash: file_hash,
                result_data: None,
            });
            let _ = outgoing.send(DistributedMessage::TaskRequest {
                sender_id: secondary_id.into(),
                timestamp: 0.0,
                secondary_id: secondary_id.into(),
                worker_id: 0,
                available_resources: vec![dynrunner_core::ResourceAmount {
                    kind: dynrunner_core::ResourceKind::memory(),
                    amount: ram_bytes,
                }],
            });
        }
        _ => {}
    }
}

/// End-to-end pin for the "peer ipv4/ipv6 addresses reach the dialer"
/// plumbing: spin up a primary against two channel-transport
/// secondaries, have each advertise BOTH families in CertExchange, and
/// inspect the `PeerInfo` broadcast that lands at one of them. The
/// peers vector must carry the OTHER secondary's ipv4 AND ipv6 — pre-
/// fix `peer_setup::send_peer_lists` hardcoded `ipv6: None`, which
/// produced empty happy-eyeballs candidate sets on dual-stack hosts
/// where ipv4 was administratively blocked between compute nodes.
///
/// The test snoops `PeerInfo` by intercepting the second secondary's
/// inbound channel: a forwarder task drains the channel, copies any
/// `PeerInfo` into a `oneshot` for assertion, then forwards every
/// message to the real fake-secondary task so the lifecycle
/// (PeerInfo → InitialAssignment → TaskAssignment → TaskComplete)
/// completes and `primary.run` returns.
#[tokio::test(flavor = "current_thread")]
async fn peer_info_broadcast_carries_both_ipv4_and_ipv6() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, mut secondary_ends) = setup_test(2);

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
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries = vec![make_binary("a", 50)];

        // Two secondaries, each advertising a distinct ipv4 + ipv6.
        // sec-0 → (10.0.0.1, 2001:db8::1)
        // sec-1 → (10.0.0.2, 2001:db8::2)
        // The assertion below pulls the PeerInfo sec-1 receives and
        // looks up sec-0's entry — that's the entry whose addresses
        // were in flight through `handle_cert_exchange` →
        // `SecondaryConnectionState` → `send_peer_lists`.
        let addrs: Vec<(String, String)> = vec![
            ("10.0.0.1".into(), "2001:db8::1".into()),
            ("10.0.0.2".into(), "2001:db8::2".into()),
        ];

        // Snoop the second secondary's primary→secondary channel: a
        // forwarder task copies any `PeerInfo` into a oneshot before
        // re-forwarding every message to the actual fake-secondary
        // task. Without the forward step, the fake never sees
        // InitialAssignment / TransferComplete and `primary.run`
        // hangs on `wait_for_peer_connections` budgeting → timeout.
        let (peer_info_tx, peer_info_rx) = tokio::sync::oneshot::channel();
        let mut peer_info_tx = Some(peer_info_tx);

        // Pull sec-1 out first so we can wrap its inbound channel.
        // `secondary_ends` is ordered sec-0, sec-1.
        let (sec1_id, sec1_inbound, sec1_outbound) = secondary_ends.remove(1);
        let (sec0_id, sec0_inbound, sec0_outbound) = secondary_ends.remove(0);

        // sec-0: vanilla fake_secondary_with_addrs.
        let (sec0_ipv4, sec0_ipv6) = addrs[0].clone();
        tokio::task::spawn_local(fake_secondary_with_addrs(
            sec0_id.clone(),
            1,
            1024 * 1024 * 1024,
            Some(sec0_ipv4),
            Some(sec0_ipv6),
            sec0_inbound,
            sec0_outbound,
        ));

        // sec-1: forwarder + fake.
        let (sec1_inner_tx, sec1_inner_rx) = tokio_mpsc::unbounded_channel();
        let (sec1_ipv4, sec1_ipv6) = addrs[1].clone();
        tokio::task::spawn_local(fake_secondary_with_addrs(
            sec1_id.clone(),
            1,
            1024 * 1024 * 1024,
            Some(sec1_ipv4),
            Some(sec1_ipv6),
            sec1_inner_rx,
            sec1_outbound,
        ));

        tokio::task::spawn_local(async move {
            let mut rx = sec1_inbound;
            while let Some(msg) = rx.recv().await {
                if let DistributedMessage::PeerInfo { peers, .. } = &msg {
                    if let Some(tx) = peer_info_tx.take() {
                        let _ = tx.send(peers.clone());
                    }
                }
                if sec1_inner_tx.send(msg).is_err() {
                    break;
                }
            }
        });

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        let peers = peer_info_rx.await.expect("PeerInfo never delivered");

        let sec0_peer = peers
            .iter()
            .find(|p| p.secondary_id == "sec-0")
            .expect("sec-0 missing from PeerInfo");
        assert_eq!(
            sec0_peer.ipv4.as_deref(),
            Some("10.0.0.1"),
            "primary dropped ipv4 from peer broadcast"
        );
        assert_eq!(
            sec0_peer.ipv6.as_deref(),
            Some("2001:db8::1"),
            "primary dropped ipv6 from peer broadcast — happy-eyeballs \
             dialer would race only ipv4 candidates and fail on \
             clusters where ipv4 is administratively blocked between \
             compute nodes"
        );
    })
    .await;
}

/// Regression: `promote_primary` flips `self.demoted` to true
/// and from that point `dispatch_to_idle_workers` is a no-op on the
/// scheduler — i.e. the local primary stops handing out work as
/// soon as it has handed authority off to the primary.
///
/// Without this contract the local primary and the promoted secondary
/// would both run dispatch in parallel against the same pool, racing
/// for workers and creating duplicate assignments / inconsistent
/// ledger state. See `demoted` doc on `PrimaryCoordinator` for the
/// full rationale.
#[tokio::test(flavor = "current_thread")]
async fn promote_primary_demotes_local_and_disables_dispatch() {
    use crate::state::{SecondaryConnection, SecondaryConnectionState};
    use dynrunner_scheduler_api::PendingPool;

    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, _ends) = setup_test(1);
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
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Pre-conditions: a registered secondary, a single idle
        // virtual worker bound to it, and a pool with one queued
        // binary that `dispatch_to_idle_workers` would otherwise
        // pick up. We bypass `run()` because we want to drive
        // `promote_primary` and `dispatch_to_idle_workers`
        // in isolation.
        let phase = dynrunner_core::PhaseId::from("default");
        let mut pool = PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
        .expect("default-phase pool");
        let bin = make_binary("solo", 50);
        pool.extend([bin.clone()]).expect("valid extend");
        primary.pending = Some(pool);
        primary.phase_completed.insert(phase.clone(), 0);
        primary.phase_failed.insert(phase, 0);
        primary.all_binaries = vec![bin];
        primary.total_tasks = 1;

        let conn = SecondaryConnection::new("sec-0".into())
            .receive_welcome(1, vec![], "host".into(), 0, None, false)
            .receive_cert_exchange(String::new(), None, None, 0)
            .begin_peer_discovery()
            .peers_ready()
            .assignments_sent();
        primary.secondaries.insert(
            "sec-0".into(),
            SecondaryConnectionState::Operational(conn),
        );
        primary.workers.push(RemoteWorkerState {
            worker_id: 0,
            secondary_id: "sec-0".into(),
            resource_budgets: dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]),
            current_task: None,
            estimated_resources: dynrunner_core::ResourceMap::new(),
            is_idle: true,
        });

        assert!(!primary.demoted, "fresh primary is not demoted");

        // Promote: should set `demoted = true` and emit a
        // `PromotePrimary` to the secondary (we don't observe the
        // wire here; the demotion flag is the contract under test).
        primary.promote_primary().await.unwrap();
        assert!(primary.demoted, "promote_primary must demote local");
        assert_eq!(
            primary.primary_id.as_deref(),
            Some("sec-0"),
            "promote_primary records the routing target"
        );

        // The pool still has its queued binary; the worker is
        // still idle. Pre-fix `dispatch_to_idle_workers` would
        // happily take the binary from the pool and assign it.
        // Post-fix it must early-return without touching pool
        // state — since the primary now owns dispatch.
        let pool_len_before = primary.pool().len();
        let view_before = primary.pool().view_for_worker(0).len();
        assert_eq!(pool_len_before, 1);
        assert_eq!(view_before, 1);
        assert!(primary.workers[0].is_idle);
        assert!(primary.workers[0].current_task.is_none());

        primary.dispatch_to_idle_workers().await.unwrap();

        assert_eq!(
            primary.pool().len(),
            pool_len_before,
            "dispatch_to_idle_workers must not take from pool when demoted"
        );
        assert!(
            primary.workers[0].is_idle,
            "worker must remain idle when local primary is demoted"
        );
        assert!(
            primary.workers[0].current_task.is_none(),
            "worker must not be assigned a task when local primary is demoted"
        );
    }).await;
}

/// Regression: a demoted primary that receives a `TaskRequest` from any
/// secondary must NOT relay it to `self.primary_id` via `transport.send_to`.
/// The promoted peer's outgoing channel on the demoted side is the
/// server-side writer the new primary no longer drains in its post-flip
/// role, so the next `send_to` after promotion fails with `channel closed`.
/// Pre-fix the `?` on that send escalated to `run()`'s error path and the
/// demoted submitter process exited within two keepalive intervals of
/// promotion — operator-facing tokenizer setup-promote regression
/// (04:48:38 promote → 04:48:48 "primary coordinator failed").
///
/// Per `feedback_mesh_independent_of_role_and_membership.md`, transport-
/// level channel-closed to a promoted peer must NOT be fatal: the peer
/// mesh stays as-is and the requesting secondary will re-route to peer-
/// transport once it applies the `PromotePrimary` broadcast we sent.
/// Dropping the relayed TaskRequest is benign on the demoted side; the
/// secondary retries on its next backoff tick.
///
/// Setup mirrors `promote_primary_demotes_local_and_disables_dispatch`
/// but adds the failure injection: after `promote_primary` flips
/// `demoted = true`, we drop the receiver end of the promoted peer's
/// outgoing channel so the next `transport.send_to(promoted_id, ..)`
/// surfaces `channel closed`. Then we feed a synthesized TaskRequest
/// through `dispatch_message`; pre-fix this returns Err and would
/// torpedo the demoted submitter's `run()`. Post-fix it returns Ok and
/// a subsequent `ClusterMutation::RunComplete` continues to apply on
/// the demoted primary's mirror — the failover/run-done path is intact.
#[tokio::test(flavor = "current_thread")]
async fn demoted_primary_suppresses_taskrequest_relay_after_promotion() {
    use crate::state::{SecondaryConnection, SecondaryConnectionState};
    use dynrunner_scheduler_api::PendingPool;

    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        // Single-secondary fixture; `setup_test(1)` registers `sec-0` in
        // the outgoing map. Hold the receiver in `_ends` and explicitly
        // drop it below to trigger the channel-closed condition on the
        // primary's `send_to(sec-0, ..)`.
        let (transport, mut ends) = setup_test(1);
        // `required_setup_on_promote=true` is the setup-promote mode
        // that exercises the demoted-submitter path in production
        // (matches the tokenizer trace's PrimaryConfig). The local
        // primary skips initial assignment and lives only as an
        // observer post-promotion.
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            required_setup_on_promote: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
        };
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Install an empty default-phase pool so `dispatch_message`
        // doesn't trip on a missing pool when it threads through
        // unrelated handlers. The setup-promote submitter starts
        // with an empty pool by design.
        let phase = dynrunner_core::PhaseId::from("default");
        let pool = PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
        .expect("default-phase pool");
        primary.pending = Some(pool);
        primary.phase_completed.insert(phase.clone(), 0);
        primary.phase_failed.insert(phase, 0);

        // Register `sec-0` at Operational state — same handshake
        // chain the production code drives. No workers are pushed:
        // setup-promote mode skips `perform_initial_assignment`, so
        // the demoted primary's `workers` list stays empty.
        let conn = SecondaryConnection::new("sec-0".into())
            .receive_welcome(1, vec![], "host".into(), 0, None, false)
            .receive_cert_exchange(String::new(), None, None, 0)
            .begin_peer_discovery()
            .peers_ready()
            .assignments_sent();
        primary.secondaries.insert(
            "sec-0".into(),
            SecondaryConnectionState::Operational(conn),
        );

        // Promote `sec-0` — sets `self.demoted = true` and
        // `self.primary_id = Some("sec-0")`. Mirrors what the
        // operational path does after `wait_for_mesh_ready`.
        primary.promote_primary().await.unwrap();
        assert!(primary.demoted, "promote_primary must demote local");
        assert_eq!(primary.primary_id.as_deref(), Some("sec-0"));

        // Failure injection: drop the receiver end of `sec-0`'s
        // outgoing channel. The next `transport.send_to(sec-0, ..)`
        // will return `Err("channel closed")` — exactly the wire-
        // level condition the tokenizer trace surfaced.
        let (sec_id, _drop_rx, _tx) = ends.remove(0);
        assert_eq!(sec_id, "sec-0");
        // `_drop_rx` goes out of scope here; the unbounded mpsc's
        // SendError surfaces "channel closed" as the Display.

        // Feed a TaskRequest as if it arrived from `sec-0` —
        // `handle_task_request` would try to relay it to
        // `primary_id` (= `sec-0`) and hit the closed channel.
        let request = DistributedMessage::TaskRequest {
            sender_id: "sec-0".into(),
            timestamp: 0.0,
            secondary_id: "sec-0".into(),
            worker_id: 0,
            available_resources: vec![dynrunner_core::ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: 1024 * 1024 * 1024,
            }],
        };
        let result = primary.dispatch_message(request).await;
        assert!(
            result.is_ok(),
            "dispatch_message on a demoted primary must not propagate \
             channel-closed from a relay attempt to the promoted peer; \
             pre-fix this returned Err and killed the demoted submitter \
             within two keepalive intervals of promotion. Got: {:?}",
            result.err()
        );

        // The failover/run-done path is intact: a
        // `ClusterMutation::RunComplete` from the promoted peer still
        // applies on the demoted primary's mirror so the operational
        // loop's exit cue fires. Without this, our fix would have
        // broken the run-done signaling and we'd just trade one hang
        // for another.
        let run_complete = DistributedMessage::ClusterMutation {
            sender_id: "sec-0".into(),
            timestamp: 0.0,
            mutations: vec![
                dynrunner_protocol_primary_secondary::ClusterMutation::<TestId>::RunComplete,
            ],
        };
        primary
            .dispatch_message(run_complete)
            .await
            .expect("ClusterMutation::RunComplete must still apply on demoted primary");
        assert!(
            primary.cluster_state_for_test().run_complete(),
            "RunComplete signal must reach cluster_state mirror post-fix; \
             confirms the demoted primary remains a functioning observer \
             after a relay-suppressed TaskRequest"
        );
    })
    .await;
}

// ── Backlog L2: load-aware dispatch ordering ──

fn make_remote_worker(
    worker_id: u32,
    secondary_id: &str,
    busy: bool,
) -> RemoteWorkerState<TestId> {
    RemoteWorkerState {
        worker_id,
        secondary_id: secondary_id.into(),
        resource_budgets: dynrunner_core::ResourceMap::new(),
        current_task: if busy { Some(make_binary("placeholder", 0)) } else { None },
        estimated_resources: dynrunner_core::ResourceMap::new(),
        is_idle: !busy,
    }
}

#[test]
fn dispatch_order_equal_load_preserves_worker_id_order() {
    let workers = vec![
        make_remote_worker(0, "A", false),
        make_remote_worker(1, "A", false),
        make_remote_worker(2, "B", false),
        make_remote_worker(3, "B", false),
    ];
    let order = super::lifecycle::dispatch_order(&workers);
    assert_eq!(order, vec![0, 1, 2, 3]);
}

#[test]
fn dispatch_order_prefers_less_loaded_secondary() {
    // A has 2 busy + 2 idle (load 2). B has 0 busy + 2 idle (load 0).
    // B's idle workers must come before A's even though A's worker_ids
    // are lower — the pre-fix iteration order would have given A first
    // dibs on tail-of-phase items.
    let workers = vec![
        make_remote_worker(0, "A", true),
        make_remote_worker(1, "A", true),
        make_remote_worker(2, "A", false),
        make_remote_worker(3, "A", false),
        make_remote_worker(4, "B", false),
        make_remote_worker(5, "B", false),
    ];
    let order = super::lifecycle::dispatch_order(&workers);
    assert_eq!(order, vec![4, 5, 2, 3]);
}

#[test]
fn dispatch_order_excludes_busy_workers() {
    let workers = vec![
        make_remote_worker(0, "A", true),
        make_remote_worker(1, "A", false),
        make_remote_worker(2, "B", true),
        make_remote_worker(3, "B", false),
    ];
    let order = super::lifecycle::dispatch_order(&workers);
    assert_eq!(order, vec![1, 3]);
}

#[test]
fn dispatch_order_empty_workers() {
    let workers: Vec<RemoteWorkerState<TestId>> = vec![];
    let order = super::lifecycle::dispatch_order(&workers);
    assert!(order.is_empty());
}

#[test]
fn dispatch_order_no_idle_workers() {
    let workers = vec![
        make_remote_worker(0, "A", true),
        make_remote_worker(1, "B", true),
    ];
    let order = super::lifecycle::dispatch_order(&workers);
    assert!(order.is_empty());
}

// ── Regression gate: in-process distributed pipeline must queue
// initial staging entries before `run()`. Without them, every task's
// `local_path` arrives at the secondary unstaged and dispatch's
// `report_unresolvable_task` rejects it with "expected StageFile
// notification first". The pair below pins:
//   T1: the failure mode is reachable when staging is omitted.
//   T2: calling `queue_initial_staging_from_binaries` clears it.
//
// Setup is deliberately minimal: 1 binary with a relative `path`
// (so `local_path_is_relative=true` triggers the unresolvable-task
// guard), 1 real secondary with `src_network=None` (so the guard's
// `src_network.is_some()` clause stays false too — the relative-path
// branch does the work), 1 worker (sufficient to dispatch the single
// binary).
//
// We use a real `SecondaryCoordinator` (not a `fake_secondary`) so
// the wire path that produces the regression error string is
// exercised end-to-end on the secondary side, not just simulated.

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
    local.run_until(async {
        let secondary_id = "secondary-0".to_string();
        let max_res = dynrunner_core::ResourceMap::from(
            [(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024u64)]
        );

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
            // `retry_max_passes = 0` so a Recoverable failure becomes
            // permanent on the first pass — the regression we're
            // pinning produces NonRecoverable failures (the unresolvable
            // task guard sends `ErrorType::NonRecoverable`), so the
            // budget is moot, but keeping it at 0 avoids any chance of
            // a retry pass masking the assertion.
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
    }).await;
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
    local.run_until(async {
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
        let max_res = dynrunner_core::ResourceMap::from(
            [(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024u64)]
        );

        // Secondary needs `src_network` pointing at the source tree
        // so its `stage_file` step can copy the file into the cache —
        // mirrors the real in-process pipeline, where the secondary
        // shares filesystem visibility with the primary. Without
        // `src_network` set the staging copy fails (no source root)
        // and the task still falls through to the unresolvable
        // guard, which would mask the fix.
        let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
            spawn_real_secondary_with_src_network(
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
            .queue_initial_staging_from_binaries(
                &binaries,
                &secondary_ids,
                &source_root,
            )
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
    }).await;
}

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

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        assert_eq!(primary.completed_count(), 3);
        assert_eq!(primary.failed_count(), 0);
        assert_eq!(
            primary.stranded_count(),
            0,
            "clean-run stranded must be zero (total - completed - failed)"
        );
    }).await;
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
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
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
                assert!(stranded > 0, "stranded must be positive on cluster collapse");
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
    }).await;
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
    local.run_until(async {
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
            // Zero timeout so the very first loop iteration's
            // `elapsed >= fleet_dead_timeout` predicate trips, no
            // wall-clock wait needed in the test.
            fleet_dead_timeout: std::time::Duration::ZERO,
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
        };
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Prime: pool with three queued binaries, empty secondaries
        // map (the fleet-dead predicate is `secondaries.is_empty() &&
        // !pool.is_empty()`), `total_tasks` set so the run-level
        // accounting can later compute `stranded = total -
        // completed - failed`.
        let phase = dynrunner_core::PhaseId::from("default");
        let mut pool = PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
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
        };
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
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

        assert_eq!(
            primary.completed_count(),
            3,
            "drain must have processed all three queued TaskComplete messages"
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
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
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

// ── Demoted-primary ClusterMutation arm: regression gate against the
// asm-dataset-nix R2 / T3 1200s hang.
//
// Pre-fix the primary-side `dispatch_message` had no arm for
// `MessageType::ClusterMutation` — every ClusterMutation broadcast
// addressed at the demoted local primary fell through the catch-all,
// leaving its replicated `cluster_state` mirror frozen at the
// pre-promotion view and the per-task accounting (`completed_tasks` /
// `failed_tasks`, the two sets the operational loop's exit-counter
// check reads) blind to cross-secondary completions on the new primary's
// pool. The loop sat forever; the local-primary process never exited;
// the asm-dataset-nix e2e harness killed it at the 1200s deadline.
//
// The three tests below pin:
//   T-A: a synthetic `ClusterMutation::TaskCompleted` arriving via
//        `dispatch_message` lands in `completed_tasks` (the unit
//        contract — without this `completed + failed >= total` cannot
//        trip on a demoted primary).
//   T-B: an end-to-end run where the local primary is demoted and the
//        promoted secondary's RunComplete signal must land on the
//        demoted primary's `cluster_state.run_complete()` and break
//        the operational loop within bounded wait — same window as the
//        existing 500ms RunComplete settle in `run()`.
//   T-C: an explicit ClusterMutation::RunComplete delivered via the
//        demoted primary's transport must drive the same exit cleanly
//        even when no task accounting is in play (the
//        `cluster_state.run_complete()` exit fires standalone).

/// T-A — unit contract. Drive `dispatch_message` directly with a
/// synthesized `DistributedMessage::ClusterMutation` carrying a
/// `TaskCompleted` mutation; assert `completed_tasks` grows. Failed
/// pre-fix because the dispatch-message catch-all silently dropped
/// every ClusterMutation arrival on the primary side; succeeds post-fix
/// because the new arm threads the mutation through both the local
/// `cluster_state` mirror and the accounting sets the operational
/// loop's exit-counter check reads.
#[tokio::test(flavor = "current_thread")]
async fn demoted_primary_applies_cluster_mutation_taskcompleted() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, _ends) = setup_test(1);
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
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Pre-state: empty completed_tasks. Post-fix the
        // ClusterMutation arm grows it from any TaskCompleted /
        // TaskFailed mutation, regardless of whether the hash also
        // appears in cluster_state's CRDT (which has its own
        // happens-before constraint requiring TaskAdded first — that
        // path is exercised by the e2e tests, not this unit one).
        // The accounting sets are the load-bearing surface for the
        // operational loop's exit-counter check, so they're what we
        // pin here.
        assert!(primary.completed_tasks.is_empty());
        assert!(primary.failed_tasks.is_empty());

        // Seed cluster_state with TaskAdded so the subsequent
        // TaskCompleted apply isn't a NoOp (the CRDT requires the
        // entry to exist before transitioning state). Without the
        // seed the cluster_state assertion below would be unreachable
        // even on a correct fix.
        let bin = make_binary("demoted-arm-task", 100);
        let hash = super::wire::compute_task_hash(&bin);
        let seed_msg = DistributedMessage::ClusterMutation {
            sender_id: "sec-promoted".into(),
            timestamp: 0.0,
            mutations: vec![
                dynrunner_protocol_primary_secondary::ClusterMutation::<TestId>::TaskAdded {
                    hash: hash.clone(),
                    task: bin,
                },
            ],
        };
        primary
            .dispatch_message(seed_msg)
            .await
            .expect("seed TaskAdded must dispatch");

        let msg = DistributedMessage::ClusterMutation {
            sender_id: "sec-promoted".into(),
            timestamp: 0.0,
            mutations: vec![dynrunner_protocol_primary_secondary::ClusterMutation::<
                TestId,
            >::TaskCompleted {
                hash: hash.clone(),
            }],
        };
        primary
            .dispatch_message(msg)
            .await
            .expect("dispatch_message must accept a ClusterMutation");

        assert!(
            primary.completed_tasks.contains(&hash),
            "ClusterMutation::TaskCompleted must mirror into completed_tasks; \
             without this the demoted primary's `completed + failed >= total` \
             exit-counter check never trips on cross-secondary completions"
        );

        // The cluster_state mirror also reflects the mutation — the
        // CRDT lattice is the source of truth for the primary's view
        // of the run, even post-demotion. Verifies the apply is on
        // the same code path the secondary's
        // `apply_cluster_mutations` uses.
        let cs_counts = primary.cluster_state_for_test().counts();
        assert_eq!(
            cs_counts.completed, 1,
            "cluster_state must record 1 Completed entry after the mutation"
        );
    }).await;
}

/// T-B — end-to-end. A demoted primary plus a real secondary (acting
/// as the promoted primary) drive the run; the secondary fires
/// `ClusterMutation::RunComplete` once its primary view drains, and
/// the demoted primary's operational loop must observe the signal and
/// exit. The wait is bounded by the timeout below — pre-fix the run
/// never returns and the test would hang until killed by the harness;
/// post-fix the wait closes well within 1s in-process.
///
/// We don't drive a full failover sequence (PromotePrimary handshake,
/// election, etc.) — that surface is covered by the existing failover
/// tests. Here the contract under test is narrower: assuming a
/// promoted secondary has emitted the RunComplete signal AND the
/// signal lands on the demoted primary's transport, does the demoted
/// primary's loop break? We construct that exact wire shape via the
/// single-secondary primary fixture and the secondary's existing
/// "promoted primary done; broadcasting RunComplete" path
/// (processing.rs).
///
/// `demoted=true` is forced via `promote_primary` before `run()` so
/// the operational loop runs in observer mode — exactly what
/// asm-dataset-nix's R2 trace reports for the local primary.
#[tokio::test(flavor = "current_thread")]
async fn demoted_primary_exits_on_run_complete_broadcast() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        // setup_test(1) yields a primary-side transport plus one
        // secondary "end" (id, primary→sec rx, sec→primary tx).
        // The sec→primary tx is the channel we use to deliver
        // synthetic wire messages — exactly the shape a promoted
        // secondary's loopback would produce on the demoted
        // primary's transport.
        let (transport, secondary_ends) = setup_test(1);
        let (_sec_id, _to_sec_rx, incoming_tx) =
            secondary_ends.into_iter().next().unwrap();

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_millis(100),
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
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Bypass `run()`: drive `operational_loop` in isolation with a
        // pre-loaded ClusterMutation::RunComplete arriving on the
        // transport. Same wire shape the promoted secondary's
        // `processing.rs` produces when its primary view drains.
        let phase = dynrunner_core::PhaseId::from("default");
        let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
        .expect("default-phase pool");
        primary.pending = Some(pool);
        primary.phase_completed.insert(phase.clone(), 0);
        primary.phase_failed.insert(phase, 0);
        // total_tasks = 1 with no completion mirrors the asm-dataset-nix
        // R2 starvation: the counter check `completed + failed >=
        // total` is unreachable from this state, so only the
        // RunComplete-driven exit can break the loop. Pre-fix this
        // test would hang inside `operational_loop` indefinitely.
        primary.total_tasks = 1;
        primary.demoted = true;

        // Inject the RunComplete signal on the transport. The recv
        // tick inside operational_loop must dispatch it, the new
        // ClusterMutation arm must apply it, and the new run_complete
        // exit must break the loop.
        incoming_tx
            .send(DistributedMessage::ClusterMutation {
                sender_id: "sec-promoted".into(),
                timestamp: 0.0,
                mutations: vec![
                    dynrunner_protocol_primary_secondary::ClusterMutation::<TestId>::RunComplete,
                ],
            })
            .unwrap();
        // Drop the sender so the loop's recv yields `None` after the
        // queued message, exercising the transport-closed branch as a
        // hard backstop. The post-fix exit MUST come from the
        // run_complete check (cluster_state.run_complete() == true),
        // not from the transport-closed break — assert below
        // distinguishes the two paths.
        drop(incoming_tx);

        // Bounded wait: pre-fix the loop was unbounded. Post-fix the
        // mutation arrives in <1ms, the apply is synchronous, and the
        // next loop iteration's run_complete check breaks. 5s ceiling
        // for CI flake tolerance.
        let exit = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            primary.operational_loop(),
        )
        .await;

        match exit {
            Ok(Ok(())) => {
                assert!(
                    primary.cluster_state_for_test().run_complete(),
                    "cluster_state must record run_complete after the mutation; \
                     if this fails the loop exited via the transport-closed \
                     fallback, not the run_complete check under test"
                );
            }
            Ok(Err(e)) => panic!("operational_loop returned Err on RunComplete: {e}"),
            Err(_) => panic!(
                "operational_loop did not exit within 5s — the demoted \
                 primary's RunComplete-driven exit is broken (pre-fix \
                 hang regression)"
            ),
        }
    }).await;
}

/// T-C — end-to-end happy path. A demoted primary + 2 fake secondaries,
/// where one is the promoted primary draining its replicated pool. Pre-
/// fix the local primary's operational loop sat forever waiting for a
/// counter tick that never came; post-fix the RunComplete signal
/// (delivered via the new primary_transport.send loopback in
/// secondary/processing.rs) lands on the demoted primary's transport,
/// the new ClusterMutation arm applies it, and the run_complete exit
/// closes the loop within bounded wait.
///
/// This wires the same delivery path asm-dataset-nix R2 / T3 exercises
/// in production: the new primary's `processing.rs` RunComplete site
/// fanning out to peers AND back to the demoted primary's transport.
/// Without the primary_transport.send addition this test would still
/// hang post-fix.
#[tokio::test(flavor = "current_thread")]
async fn demoted_primary_exits_on_clean_completion() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, secondary_ends) = setup_test(1);
        let (_sec_id, _to_sec_rx, incoming_tx) =
            secondary_ends.into_iter().next().unwrap();

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
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
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Pre-state: pool with no items, two pre-mirrored completions,
        // total_tasks set to a value the counter check cannot reach
        // from the existing completions alone — so only the
        // run_complete-driven exit can break the loop. demoted=true
        // puts the loop in observer mode (matches asm-dataset-nix R2:
        // local primary already handed off authority to the promoted
        // secondary).
        let phase = dynrunner_core::PhaseId::from("default");
        let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
        .expect("default-phase pool");
        primary.pending = Some(pool);
        primary.phase_completed.insert(phase.clone(), 0);
        primary.phase_failed.insert(phase, 0);
        primary.total_tasks = 3; // counter-check unreachable
        primary.completed_tasks.insert("h-already-done-1".into());
        primary.completed_tasks.insert("h-already-done-2".into());
        primary.demoted = true;

        // Inject the ClusterMutation::RunComplete on the transport
        // exactly the way the new primary's
        // `processing.rs::primary_transport.send` loopback delivers it
        // post-fix. Pre-fix this delivery path doesn't exist (the
        // RunComplete only went out via peer_transport, which the
        // demoted primary isn't on); even with delivery, pre-fix
        // there's no `MessageType::ClusterMutation` arm to consume it.
        incoming_tx
            .send(DistributedMessage::ClusterMutation {
                sender_id: "sec-promoted".into(),
                timestamp: 0.0,
                mutations: vec![
                    dynrunner_protocol_primary_secondary::ClusterMutation::<TestId>::RunComplete,
                ],
            })
            .unwrap();
        // Hold the sender open: the loop's run_complete exit must fire
        // on its OWN, not via the transport-closed fallback. Asserting
        // on `cluster_state.run_complete()` after the loop returns
        // distinguishes the two paths.
        let _hold = incoming_tx;

        // Bounded wait. Pre-fix the loop was unbounded — the
        // asm-dataset-nix harness killed the local primary at 1200s.
        // Post-fix the run_complete check fires within one heartbeat
        // tick of the mutation arriving (50ms keepalive_interval here
        // means at most ~100ms before the next select! cycle picks up
        // the message). 5s ceiling for CI flake tolerance.
        let exit = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            primary.operational_loop(),
        )
        .await;

        match exit {
            Ok(Ok(())) => {
                assert!(
                    primary.cluster_state_for_test().run_complete(),
                    "cluster_state.run_complete() must be set after the \
                     RunComplete-driven exit fired (distinguishes from a \
                     stale transport-closed break)"
                );
            }
            Ok(Err(e)) => panic!("operational_loop returned Err: {e}"),
            Err(_) => panic!(
                "operational_loop did not exit within 5s on a clean \
                 RunComplete signal — the demoted primary's exit path \
                 is broken (asm-dataset-nix R2 / T3 1200s hang \
                 regression)"
            ),
        }
    }).await;
}

// ── Step 6 — demoted primary ingests ClusterMutation broadcasts via
// `peer_transport.recv_peer()`. Pins bug-class #1 (asm-tokenizer
// "succeeded=0 + 235 CSVs landed") and #79 (chain-gate reading stale
// 0/0/0 because the demoted local's accounting was blind to cross-
// secondary completions on the new primary's pool).
//
// Pre-Step-6 the demoted primary's run-loop only read from
// `self.transport.recv()` (the legacy `SecondaryTransport` channel),
// which closes for the promoted secondary post-PromotePrimary. The
// new primary's broadcasts via the mesh landed in
// `peer_transport.recv_peer()`'s queue but went unread; per-task
// accounting (`completed_tasks`) and the cluster-state mirror's
// `run_complete()` flag stayed frozen at the pre-promotion view.
//
// Step 6 adds the `peer_transport.recv_peer()` arm to the operational
// loop's `select!` (forwarding to the same `dispatch_message` the
// legacy arm uses; the CRDT `apply` + the HashSet inserts make
// duplicate delivery a no-op). It also relaxes the
// transport-closed→break gate when `peer_transport.peer_count() > 0`,
// so the demoted local stays in the loop as long as the mesh is alive.
//
// The two tests below pin:
//   T-D: a ClusterMutation::TaskCompleted arriving via the tunneled
//        peer view (production wiring: per-secondary forwarder taps
//        each inbound frame into the peer queue) lands in the
//        demoted primary's `completed_tasks` and `cluster_state`.
//        Closes bug #79's "chain-gate reading stale 0/0/0".
//   T-E: when the legacy transport is closed but the tunneled peer
//        view still has connections, the loop stays alive on the
//        peer arm — does NOT trip the historical transport-closed
//        break. RunComplete delivered via the peer arm cleanly exits.

/// T-D — unit contract via TunneledPeerTransport. Build a primary
/// wired with a `TunneledPeerTransport` (the same setup
/// `crates/dynrunner-pyo3/src/managers/distributed.rs` uses in
/// production post-Step-5b). Stage a `ClusterMutation::TaskAdded` then
/// `ClusterMutation::TaskCompleted` into the per-secondary forwarder's
/// inbound-tap (the production wire shape: the legacy `transport`
/// recv-side clones each frame into `inbound_tap`). Run
/// `operational_loop` briefly; assert the demoted primary's
/// `completed_tasks` grows and `cluster_state.run_complete()` fires
/// once a RunComplete mutation rides the same path.
#[tokio::test(flavor = "current_thread")]
async fn step6_demoted_primary_observes_cluster_mutation_via_recv_peer_arm() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    use dynrunner_transport_tunnel::TunneledPeerTransport;

    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        // Build the tunneled peer view first so the per-secondary
        // forwarder below can tap inbound frames into the peer queue
        // — same wiring shape as the production in-process
        // distributed PyO3 path (`distributed.rs::fwd_tap`).
        let (peer_transport, shared_outgoing, inbound_tap) =
            TunneledPeerTransport::<TestId>::new("primary".into());

        // Legacy transport with one secondary registered in BOTH the
        // legacy outgoing HashMap AND the shared writer table —
        // exactly how production registers a secondary post-Step-5b.
        let (incoming_tx, incoming_rx) =
            tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
        let (pri_to_sec_tx, _pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
        shared_outgoing
            .borrow_mut()
            .insert("sec-promoted".into(), pri_to_sec_tx.clone());
        let mut outgoing = HashMap::new();
        outgoing.insert("sec-promoted".to_string(), pri_to_sec_tx);
        let transport = ChannelSecondaryTransportEnd {
            outgoing,
            incoming_rx,
        };

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
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
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            peer_transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Demoted-observer state: the loop will run in observer mode,
        // mirroring the post-promotion submitter-host primary in the
        // production scenario. `total_tasks=1` makes the counter-
        // based exit unreachable from completed=0, so only the
        // `cluster_state.run_complete()` exit can break the loop.
        let phase = dynrunner_core::PhaseId::from("default");
        let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
        .expect("default-phase pool");
        primary.pending = Some(pool);
        primary.phase_completed.insert(phase.clone(), 0);
        primary.phase_failed.insert(phase, 0);
        // Seed `total_tasks=2` BEFORE entering the loop. Without
        // this the first top-of-loop counter check trips immediately
        // (`0 + 0 >= 0`) and the loop exits before reading any
        // mutation. The two staged TaskAdded mutations only refresh
        // total_tasks on the first dispatch — by that time the
        // pre-loop check has already broken out. The TaskAdded
        // mutations remain useful (they let cluster_state.apply
        // accept the subsequent TaskCompleted, which requires the
        // entry to exist per CRDT-happens-before).
        //
        // With `total_tasks = 2` and ONE TaskCompleted (`completed +
        // failed = 1 < 2`), the counter-based exit stays
        // unreachable; only the RunComplete-driven exit can break
        // the loop. This pins BOTH halves of the regression:
        //   (a) the new arm dispatched the TaskCompleted mutation
        //       (completed_tasks grew — bug #79's chain-gate fix);
        //   (b) the same arm later dispatched the RunComplete
        //       (cluster_state.run_complete() flipped — bug class
        //       #1's demoted-primary exit-cue fix).
        primary.total_tasks = 2;
        primary.demoted = true;

        // Stage the mutations into the peer view's inbound queue
        // (NOT the legacy transport's `incoming_tx`). This is the
        // exact path the production forwarder feeds: a frame
        // arriving on the SSH tunnel gets cloned into `inbound_tap`
        // first, then forwarded to the legacy `incoming_tx`. Here
        // we only feed the peer view, to prove the new `select!`
        // arm IS the one applying the mutation (any leakage via the
        // legacy arm would mask the regression).
        let bin_a = make_binary("step6-arm-task-a", 100);
        let hash_a = super::wire::compute_task_hash(&bin_a);
        let bin_b = make_binary("step6-arm-task-b", 100);
        let hash_b = super::wire::compute_task_hash(&bin_b);
        inbound_tap
            .send(DistributedMessage::ClusterMutation {
                sender_id: "sec-promoted".into(),
                timestamp: 0.0,
                mutations: vec![
                    ClusterMutation::<TestId>::TaskAdded {
                        hash: hash_a.clone(),
                        task: bin_a,
                    },
                    ClusterMutation::<TestId>::TaskAdded {
                        hash: hash_b.clone(),
                        task: bin_b,
                    },
                ],
            })
            .expect("tap accepts TaskAdded batch");
        inbound_tap
            .send(DistributedMessage::ClusterMutation {
                sender_id: "sec-promoted".into(),
                timestamp: 0.0,
                mutations: vec![ClusterMutation::<TestId>::TaskCompleted {
                    hash: hash_a.clone(),
                }],
            })
            .expect("tap accepts TaskCompleted");
        inbound_tap
            .send(DistributedMessage::ClusterMutation {
                sender_id: "sec-promoted".into(),
                timestamp: 0.0,
                mutations: vec![ClusterMutation::<TestId>::RunComplete],
            })
            .expect("tap accepts RunComplete");
        // Hold the legacy incoming channel OPEN — we want the
        // peer_transport arm to be the one driving exit, not the
        // legacy-transport-closed fallback. Asserting on
        // `cluster_state.run_complete()` post-loop distinguishes
        // the two paths.
        let _hold_legacy = incoming_tx;
        // Drop the tap clone we used to send so the peer view's
        // recv eventually yields None after draining (loop's
        // run_complete exit fires first; this is just sanity).
        drop(inbound_tap);

        // Bounded wait. Pre-Step-6 the loop is unbounded (the
        // mutations on the peer view are never read). Post-Step-6
        // the new arm dispatches each mutation through
        // `dispatch_message`, the CRDT apply updates the mirror,
        // and the top-of-loop `cluster_state.run_complete()` check
        // breaks within microseconds. 5s ceiling for CI flake
        // tolerance.
        let exit = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            primary.operational_loop(),
        )
        .await;

        match exit {
            Ok(Ok(())) => {
                assert!(
                    primary.completed_tasks.contains(&hash_a),
                    "ClusterMutation::TaskCompleted received via the new \
                     `peer_transport.recv_peer()` arm must populate \
                     `completed_tasks`; this is the fix for bug #79 \
                     (chain-gate reading stale 0/0/0 because the demoted \
                     primary's accounting was blind to cross-secondary \
                     completions)"
                );
                assert!(
                    !primary.completed_tasks.contains(&hash_b),
                    "hash_b was never TaskCompleted; presence here would \
                     indicate accounting drift independent of the arm \
                     fix under test"
                );
                assert!(
                    primary.cluster_state_for_test().run_complete(),
                    "cluster_state.run_complete() must be set after \
                     the RunComplete mutation rides the peer arm; \
                     distinguishes from a stale transport-closed exit \
                     and from the counter-based exit (counter is \
                     unreachable here: 1 < 2)"
                );
            }
            Ok(Err(e)) => panic!("operational_loop returned Err: {e}"),
            Err(_) => panic!(
                "operational_loop did not exit within 5s — the \
                 `peer_transport.recv_peer()` arm is missing or not \
                 forwarding to dispatch_message (Step 6 regression)"
            ),
        }
    }).await;
}

/// T-E — transport-closed gate relaxation. When the legacy
/// `transport.recv()` returns None (the demoted-primary case: per-
/// secondary writer-task exits post-PromotePrimary) but the tunneled
/// peer view still has connected peers, the operational loop must NOT
/// fall through the historical "transport closed → break" path.
/// Otherwise the demoted local exits prematurely, takes the local-
/// primary process down, and the asm-tokenizer "succeeded=0 + 235
/// CSVs landed" symptom returns.
///
/// Setup: drop the legacy `incoming_tx` BEFORE entering the loop so
/// `transport.recv()` resolves None immediately. The peer view stays
/// connected (one secondary in `shared_outgoing`, `peer_count() == 1`).
/// A RunComplete mutation arrives ONLY via the peer-tap; the new arm
/// must drive the run_complete exit.
#[tokio::test(flavor = "current_thread")]
async fn step6_demoted_primary_stays_alive_when_legacy_transport_closes_but_peer_mesh_alive() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    use dynrunner_transport_tunnel::TunneledPeerTransport;

    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (peer_transport, shared_outgoing, inbound_tap) =
            TunneledPeerTransport::<TestId>::new("primary".into());

        let (incoming_tx, incoming_rx) =
            tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
        let (pri_to_sec_tx, _pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
        shared_outgoing
            .borrow_mut()
            .insert("sec-promoted".into(), pri_to_sec_tx.clone());
        let mut outgoing = HashMap::new();
        outgoing.insert("sec-promoted".to_string(), pri_to_sec_tx);
        let transport = ChannelSecondaryTransportEnd {
            outgoing,
            incoming_rx,
        };

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
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
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            peer_transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let phase = dynrunner_core::PhaseId::from("default");
        let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
        .expect("default-phase pool");
        primary.pending = Some(pool);
        primary.phase_completed.insert(phase.clone(), 0);
        primary.phase_failed.insert(phase, 0);
        primary.total_tasks = 1;
        primary.demoted = true;

        // Close the legacy channel BEFORE entering the loop — the
        // very first `transport.recv()` poll returns None. With
        // peer_count() > 0 the loop must `continue` past the break,
        // gate the legacy arm off, and await the peer arm for the
        // RunComplete signal.
        drop(incoming_tx);

        // RunComplete rides ONLY the peer view's inbound-tap.
        inbound_tap
            .send(DistributedMessage::ClusterMutation {
                sender_id: "sec-promoted".into(),
                timestamp: 0.0,
                mutations: vec![ClusterMutation::<TestId>::RunComplete],
            })
            .expect("tap accepts RunComplete");

        let exit = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            primary.operational_loop(),
        )
        .await;

        match exit {
            Ok(Ok(())) => {
                assert!(
                    primary.cluster_state_for_test().run_complete(),
                    "cluster_state.run_complete() must be set: the exit \
                     fired via the peer arm's RunComplete, not via the \
                     transport-closed break (which would re-introduce \
                     bug class #1)"
                );
            }
            Ok(Err(e)) => panic!("operational_loop returned Err: {e}"),
            Err(_) => panic!(
                "operational_loop did not exit within 5s — the peer-arm \
                 RunComplete path is broken or the transport-closed gate \
                 is not relaxed (Step 6 regression)"
            ),
        }
    }).await;
}

/// T-#33: initial assignment is round-robin across secondaries AND
/// secondary iteration order is deterministic (sorted by name).
///
/// Setup: 3 secondaries × 1 worker × 3 binaries. With contiguous-
/// per-secondary order (pre-fix) the assignment was still
/// one-per-secondary in this exact-fit case, but the SECONDARY-ID
/// ORDER of which-secondary-got-which-binary was HashMap-random.
/// Post-fix the binaries land in sec-0, sec-1, sec-2 order.
///
/// More important regression case: tasks ≪ total_workers. With
/// pre-fix (contiguous), 3 secondaries × 2 workers × 3 tasks would
/// have given the first secondary 2 tasks and one other secondary
/// 1 task — the third got nothing. Post-fix all three each receive
/// exactly 1. We exercise that exact case here to pin the actual
/// behaviour change, not just the determinism gain.
#[tokio::test(flavor = "current_thread")]
async fn initial_assignment_is_round_robin_and_name_sorted() {
    use std::sync::Arc;
    use std::sync::Mutex;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(3);

            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: 3,
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

            // 3 tasks, 3 secondaries × 2 workers = 6 worker slots.
            // The pre-fix contiguous-per-secondary order would have
            // given two secondaries all 3 tasks and one secondary 0.
            // Post-fix every secondary gets exactly 1.
            let binaries = vec![
                make_binary("a", 50),
                make_binary("b", 50),
                make_binary("c", 50),
            ];

            // Per-secondary initial-assignment count, captured by
            // intercepting each secondary's primary→secondary channel.
            // Forwarder counts InitialAssignment binaries before
            // re-forwarding every message to the real fake-secondary,
            // so the lifecycle still completes via TaskComplete +
            // TaskRequest cycles.
            let counts: Arc<Mutex<std::collections::BTreeMap<String, usize>>> =
                Arc::new(Mutex::new(std::collections::BTreeMap::new()));

            for (id, sec_inbound, sec_outbound) in secondary_ends {
                let (inner_tx, inner_rx) = tokio_mpsc::unbounded_channel();
                let counts_for_secondary = Arc::clone(&counts);
                let id_for_forwarder = id.clone();
                tokio::task::spawn_local(async move {
                    let mut rx = sec_inbound;
                    while let Some(msg) = rx.recv().await {
                        if let DistributedMessage::InitialAssignment {
                            zip_files, ..
                        } = &msg
                        {
                            let n: usize =
                                zip_files.iter().map(|zf| zf.binaries.len()).sum();
                            counts_for_secondary
                                .lock()
                                .unwrap()
                                .insert(id_for_forwarder.clone(), n);
                        }
                        if inner_tx.send(msg).is_err() {
                            break;
                        }
                    }
                });

                tokio::task::spawn_local(fake_secondary(
                    id,
                    2,
                    1024 * 1024 * 1024,
                    inner_rx,
                    sec_outbound,
                ));
            }

            let (deps, ops, ope) = noop_phase_args();
            primary.run(binaries, deps, ops, ope).await.unwrap();

            assert_eq!(primary.completed_count(), 3);
            assert_eq!(primary.failed_count(), 0);

            // Each of the 3 secondaries must have received exactly 1
            // binary in its InitialAssignment. Pre-fix the
            // contiguous-per-secondary layout produced something like
            // {sec-X: 2, sec-Y: 1, sec-Z: 0} where X/Y/Z were
            // HashMap-random; the secondary that got 0 then had to
            // wait for the operational TaskRequest cycle to receive
            // any work at all.
            let final_counts = counts.lock().unwrap().clone();
            assert_eq!(
                final_counts.len(),
                3,
                "every secondary must receive an InitialAssignment \
                 (even an empty one) so wait_for_setup unblocks; \
                 captured: {:?}",
                final_counts
            );
            for sid in &["sec-0", "sec-1", "sec-2"] {
                let n = final_counts
                    .get(*sid)
                    .copied()
                    .expect("expected secondary missing from captured InitialAssignment");
                assert_eq!(
                    n, 1,
                    "{sid} expected exactly 1 initial-assignment binary, \
                     got {n}. Pre-fix this would fail because contiguous-\
                     per-secondary ordering plus HashMap-random iteration \
                     order gave 2 tasks to one secondary and 0 to another. \
                     Captured: {:?}",
                    final_counts
                );
            }
        })
        .await;
}

// ── setup-promote operational-loop exit gate ──
//
// Regression pair pinning the `setup_pending` exit-check gate. In
// setup-promote mode (`required_setup_on_promote = true`) the submitter
// primary skips `seed_cluster_state`, so it enters the operational loop
// with `total_tasks = 0`. Pre-fix the counter-based exit check
// (`completed + failed >= total_tasks && active_workers == 0`) tripped
// at `0 + 0 >= 0` on the very first loop iteration — BEFORE the
// promoted setup-secondary had a chance to run discovery and broadcast
// its first `ClusterMutation::TaskAdded`. The submitter exited with
// `total=0` (the "primary finished succeeded=0 fail_retry=0 fail_oom=0
// fail_final=0 total=0" log line) and tore down the run before any task
// could dispatch.
//
// Post-fix: `setup_pending = config.required_setup_on_promote` at
// startup gates BOTH the counter-based exit (line ~193) AND the pool-
// drained exit (line ~206) in `operational_loop`. The flag is cleared
// when the first `TaskAdded` or `RunComplete` mutation arrives via
// `mirror_mutation_to_accounting`, re-enabling the historical exit
// checks for the rest of the run. Pre-seeded bootstrap
// (`required_setup_on_promote = false`) starts the flag at `false`, so
// existing behaviour is unchanged — pinned by the second test below.

/// T1 — setup-promote: operational loop does NOT exit at the first
/// tick when `setup_pending = true` and `total_tasks = 0`, even though
/// the counter check `0 + 0 >= 0` is satisfied. After a `TaskAdded`
/// mutation arrives via the mirror path the flag clears, `total_tasks`
/// refreshes to 1, and a subsequent `TaskCompleted` lets the counter
/// check fire cleanly. Pre-fix this test would observe the loop exit
/// before the TaskAdded message was even consumed off the transport.
#[tokio::test(flavor = "current_thread")]
async fn setup_pending_blocks_immediate_exit_then_proceeds_on_task_added() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, secondary_ends) = setup_test(1);
        let (_sec_id, _to_sec_rx, incoming_tx) =
            secondary_ends.into_iter().next().unwrap();

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_millis(50),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            // Setup-promote intent: the submitter has deferred
            // discovery + ledger seed to the promoted secondary, so
            // `total_tasks` starts at 0 and the operational loop must
            // wait for the secondary's TaskAdded broadcast.
            required_setup_on_promote: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
        };
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Sanity: `PrimaryCoordinator::new` must initialise
        // `setup_pending` from the config (the field's invariant). If
        // this fails the rest of the test's reasoning is wrong.
        assert!(
            primary.setup_pending,
            "setup_pending must be initialised from config.required_setup_on_promote at construction"
        );

        // Mirror what `run()` would set up: empty pool, default phase
        // tracked, no binaries, `total_tasks = 0`. demoted=false: this
        // test pins the `setup_pending` gate on the !partial_view
        // counter exit path. With `required_setup_on_promote = true
        // && demoted = true` the `partial_view` gate
        // (lifecycle.rs `let partial_view = self.demoted &&
        // self.config.required_setup_on_promote`) would suppress
        // the counter exit entirely, making the test hang — and
        // the partial-view race is covered separately by
        // `demoted_primary_ignores_partial_crdt_view_waits_for_run_complete`.
        // `self.secondaries` is empty in this synthetic setup, so
        // `process_heartbeat_tick` walks empty hashmaps and is a
        // no-op even on the !demoted path; no race.
        let phase = dynrunner_core::PhaseId::from("default");
        let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
        .expect("default-phase pool");
        primary.pending = Some(pool);
        primary.phase_completed.insert(phase.clone(), 0);
        primary.phase_failed.insert(phase, 0);
        primary.total_tasks = 0;
        primary.demoted = false;

        // Pre-load the transport: a TaskAdded mutation followed by a
        // TaskCompleted for the same hash. The loop's first iteration
        // MUST NOT exit (setup_pending blocks the counter check at
        // `0+0 >= 0`); on the recv tick it consumes the TaskAdded,
        // which (a) clears `setup_pending` via the mirror path and
        // (b) refreshes `total_tasks` from `cluster_state.task_count()`
        // = 1. On the next iteration the counter check is `0+0 >= 1`
        // = false, so the loop stays alive. The TaskCompleted then
        // arrives, advancing `completed_tasks` to 1; the iteration
        // after that observes `1+0 >= 1 && active_workers == 0` and
        // exits "all tasks completed or failed".
        let bin = make_binary("setup-discovered-task", 100);
        let hash = super::wire::compute_task_hash(&bin);
        incoming_tx
            .send(DistributedMessage::ClusterMutation {
                sender_id: "sec-promoted".into(),
                timestamp: 0.0,
                mutations: vec![ClusterMutation::<TestId>::TaskAdded {
                    hash: hash.clone(),
                    task: bin.clone(),
                }],
            })
            .unwrap();
        incoming_tx
            .send(DistributedMessage::ClusterMutation {
                sender_id: "sec-promoted".into(),
                timestamp: 0.0,
                mutations: vec![ClusterMutation::<TestId>::TaskCompleted {
                    hash: hash.clone(),
                }],
            })
            .unwrap();
        // Hold the sender open so the loop's exit MUST come from the
        // counter check, not the transport-closed fallback. Asserting
        // on `setup_pending == false` post-exit pins that the
        // TaskAdded mirror path actually cleared the gate.
        let _hold = incoming_tx;

        // Bounded wait. Pre-fix the loop exits on iteration 1 (the
        // counter check fires at `0+0 >= 0` before any recv runs).
        // Post-fix the loop must process both mutations before the
        // counter check trips; that completes in single-digit ms on
        // an in-process channel transport. 5s ceiling for CI flake
        // tolerance — matches the existing
        // `demoted_primary_exits_on_run_complete_broadcast` test.
        let exit = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            primary.operational_loop(),
        )
        .await;

        match exit {
            Ok(Ok(())) => {
                // Pin the post-fix invariants:
                // (1) `setup_pending` cleared by the TaskAdded mirror.
                assert!(
                    !primary.setup_pending,
                    "setup_pending must be cleared by the TaskAdded mirror; \
                     if this fails the gate never lifted and the loop \
                     exited via some other branch we did not intend"
                );
                // (2) `total_tasks` refreshed from cluster_state to 1.
                assert_eq!(
                    primary.total_tasks, 1,
                    "total_tasks must refresh from cluster_state.task_count() \
                     after the TaskAdded batch applies"
                );
                // (3) The TaskCompleted mirror landed.
                assert!(
                    primary.completed_tasks.contains(&hash),
                    "completed_tasks must include the hash from the second \
                     mirrored ClusterMutation::TaskCompleted"
                );
            }
            Ok(Err(e)) => panic!(
                "operational_loop returned Err in setup-promote scenario: {e}"
            ),
            Err(_) => panic!(
                "operational_loop did not exit within 5s after the \
                 TaskAdded + TaskCompleted mirrored mutations — the \
                 setup_pending gate may be stuck, or the counter check \
                 is not re-enabling on the cleared flag"
            ),
        }
    }).await;
}

/// T2 — pre-seeded bootstrap exit semantics unchanged: with
/// `required_setup_on_promote = false`, `setup_pending` starts at
/// `false` and the counter-based exit at line ~193 of
/// `operational_loop` fires immediately when
/// `completed + failed >= total_tasks && active_workers == 0`. Pins
/// that the gate added in T1 is a strict superset of historical
/// behaviour — no regression on the path where `seed_cluster_state`
/// ran locally and `total_tasks` was non-zero at startup.
#[tokio::test(flavor = "current_thread")]
async fn pre_seeded_counter_exit_unchanged() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, secondary_ends) = setup_test(1);
        let (_sec_id, _to_sec_rx, _incoming_tx) =
            secondary_ends.into_iter().next().unwrap();

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_millis(50),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            // Pre-seeded bootstrap: `seed_cluster_state` ran locally, so
            // `total_tasks` is set by `run()` from `binaries.len()`
            // and the counter-based exit must fire on the very first
            // iteration once completions cover the total.
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
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Pin invariant: pre-seeded path leaves `setup_pending = false`.
        assert!(
            !primary.setup_pending,
            "setup_pending must default to false when required_setup_on_promote = false"
        );

        // Pre-seeded mid-run state: 2 tasks total, both already in the
        // completed set (mirrors what would normally arrive via
        // TaskComplete handlers). No active workers. The counter
        // check on the first iteration is `2+0 >= 2 && 0 == 0` —
        // must trip immediately.
        let phase = dynrunner_core::PhaseId::from("default");
        let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
        .expect("default-phase pool");
        primary.pending = Some(pool);
        primary.phase_completed.insert(phase.clone(), 0);
        primary.phase_failed.insert(phase, 0);
        primary.total_tasks = 2;
        primary.completed_tasks.insert("h-legacy-1".into());
        primary.completed_tasks.insert("h-legacy-2".into());
        primary.demoted = true; // observer mode, no heartbeat side-effects

        // Bounded wait. The counter-check exit should fire on
        // iteration 1 of the loop — well under 1s. A 5s ceiling is
        // overkill but stays consistent with the other operational-
        // loop tests in this file.
        let exit = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            primary.operational_loop(),
        )
        .await;

        match exit {
            Ok(Ok(())) => {
                // Exit path pinning: still on the pre-seeded counter-based
                // exit. `setup_pending` stayed false the entire time
                // (no TaskAdded / RunComplete arrived to clear it),
                // and `cluster_state.run_complete()` was never set.
                assert!(
                    !primary.setup_pending,
                    "pre-seeded bootstrap must not flip setup_pending true at any point"
                );
                assert!(
                    !primary.cluster_state_for_test().run_complete(),
                    "pre-seeded bootstrap exit must be via the counter check, \
                     not via the cluster_state.run_complete() branch"
                );
            }
            Ok(Err(e)) => panic!(
                "operational_loop returned Err in pre-seeded bootstrap scenario: {e}"
            ),
            Err(_) => panic!(
                "pre-seeded bootstrap operational_loop did not exit within 5s \
                 despite the counter check `2+0 >= 2 && active_workers == 0` \
                 being satisfied on the first iteration — regression on the \
                 historical exit semantics"
            ),
        }
    }).await;
}

/// T3 — demoted-primary partial-CRDT-view race. The asm-tokenizer LMU
/// CIP `--jobs 15` bug: the setup-promoted secondary discovers 235
/// items and broadcasts a stream of TaskAdded + interleaved
/// TaskCompleted mutations over the SSH-tunneled QUIC mesh. The demoted
/// local primary's view evolves through partial states where
/// `total_tasks` (refreshed from `cluster_state.task_count()` after
/// each TaskAdded batch) and `completed_tasks.len()` BOTH advance — but
/// briefly align (e.g. 50 Added arrive, then 50 Completed arrive
/// before the next Added batch). At that instant `completed + failed
/// >= total_tasks && active_workers == 0` is true, even though the
/// authoritative primary is still mid-run with 185 unaccounted-for
/// items.
///
/// Pre-fix: the counter-based exit at the top of `operational_loop`
/// trips on that partial view, the demoted primary exits with `total=N
/// succeeded=N`, and the local dispatcher reads that as run-done,
/// chains to Phase 2, and tears down Phase 1's tunnels — killing the
/// actively-running Phase 1 on the secondaries.
///
/// Post-fix: the counter-based exit (and the parallel pool-drained
/// exit) is gated behind `!(self.demoted && self.config.required_setup_on_promote)`
/// — the local view is treated as partial (and unreliable) whenever
/// the demoted submitter never ran `seed_cluster_state`. In that
/// regime the demoted-primary loop has exactly one exit cue:
/// `cluster_state.run_complete() && active_workers == 0` — the
/// authoritative "every task accounted for" assertion the new primary
/// broadcasts as its last act. Legacy demoted primaries (the local
/// always demotes post-PromotePrimary in every distributed run, see
/// `lifecycle.rs::self.demoted = true`) keep the counter exit
/// because their `total_tasks` was pre-seeded and is stable.
///
/// This test stages the partial-view race directly: TaskAdded for 2
/// items, TaskCompleted for both. Pre-fix the loop exits immediately
/// after the second TaskCompleted dispatches (`2+0 >= 2`). Post-fix
/// the loop stays alive for the bounded poll window because no
/// RunComplete has arrived. The second half then injects RunComplete
/// to prove the loop CAN still exit when the authoritative signal
/// lands — distinguishing "exit gate fixed" from "loop wedged".
#[tokio::test(flavor = "current_thread")]
async fn demoted_primary_ignores_partial_crdt_view_waits_for_run_complete() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;

    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, secondary_ends) = setup_test(1);
        let (_sec_id, _to_sec_rx, incoming_tx) =
            secondary_ends.into_iter().next().unwrap();

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_millis(50),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            // Setup-promote: this primary deferred discovery + ledger
            // seed to the promoted secondary. `setup_pending` starts
            // true; the first TaskAdded will clear it. Pre-fix that
            // unblocked the counter exit — exactly the bug under test.
            required_setup_on_promote: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
        };
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let phase = dynrunner_core::PhaseId::from("default");
        let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
        .expect("default-phase pool");
        primary.pending = Some(pool);
        primary.phase_completed.insert(phase.clone(), 0);
        primary.phase_failed.insert(phase, 0);
        primary.total_tasks = 0;
        // Local submitter is already demoted (PromotePrimary broadcast
        // happened during `complete_handshake_and_assignment` per
        // `lifecycle.rs::self.demoted = true` post-PromotePrimary).
        primary.demoted = true;

        // Stage the partial-CRDT-view race: two TaskAdded then two
        // TaskCompleted for the same hashes. Pre-fix progression:
        //   iter 1: setup_pending=true → counter exit blocked. Recv
        //           TaskAdded batch → mirror clears setup_pending,
        //           cluster_state.task_count = 2, total_tasks = 2.
        //   iter 2: counter check `0+0 >= 2` → false. Recv first
        //           TaskCompleted → completed_tasks.len() = 1.
        //   iter 3: counter check `1+0 >= 2` → false. Recv second
        //           TaskCompleted → completed_tasks.len() = 2.
        //   iter 4: counter check `2+0 >= 2 && active_workers == 0`
        //           → **PRE-FIX EXITS HERE**. This is the asm-
        //           tokenizer LMU bug.
        //
        // Post-fix iter 4: counter check is `partial_view`-gated
        // (demoted=true && required_setup_on_promote=true → true)
        // → never tested. cluster_state.run_complete() is still
        // false (no RunComplete arrived yet). Loop stays alive.
        let bin_a = make_binary("lmu-task-a", 100);
        let hash_a = super::wire::compute_task_hash(&bin_a);
        let bin_b = make_binary("lmu-task-b", 100);
        let hash_b = super::wire::compute_task_hash(&bin_b);
        incoming_tx
            .send(DistributedMessage::ClusterMutation {
                sender_id: "sec-promoted".into(),
                timestamp: 0.0,
                mutations: vec![
                    ClusterMutation::<TestId>::TaskAdded {
                        hash: hash_a.clone(),
                        task: bin_a,
                    },
                    ClusterMutation::<TestId>::TaskAdded {
                        hash: hash_b.clone(),
                        task: bin_b,
                    },
                ],
            })
            .unwrap();
        incoming_tx
            .send(DistributedMessage::ClusterMutation {
                sender_id: "sec-promoted".into(),
                timestamp: 0.0,
                mutations: vec![ClusterMutation::<TestId>::TaskCompleted {
                    hash: hash_a.clone(),
                }],
            })
            .unwrap();
        incoming_tx
            .send(DistributedMessage::ClusterMutation {
                sender_id: "sec-promoted".into(),
                timestamp: 0.0,
                mutations: vec![ClusterMutation::<TestId>::TaskCompleted {
                    hash: hash_b.clone(),
                }],
            })
            .unwrap();

        // Phase A: poll the loop for 1s and assert it does NOT exit.
        // Pre-fix the loop would have exited within milliseconds of
        // the second TaskCompleted being dispatched. Post-fix it must
        // stay alive — no RunComplete has been broadcast yet, and the
        // authoritative primary at the other end is still mid-run.
        let phase_a = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            primary.operational_loop(),
        )
        .await;
        match phase_a {
            Ok(Ok(())) => panic!(
                "demoted-primary operational_loop exited within 1s on \
                 the partial-CRDT-view race (TaskAdded x2 + \
                 TaskCompleted x2 with total_tasks refreshed to 2 \
                 and completed_tasks.len() == 2). This is the \
                 asm-tokenizer LMU CIP `--jobs 15` regression — the \
                 counter-based exit must be `partial_view`-gated \
                 (demoted && required_setup_on_promote)."
            ),
            Ok(Err(e)) => panic!(
                "demoted-primary operational_loop returned Err in \
                 partial-view scenario: {e}"
            ),
            Err(_) => {
                // Timeout = loop still alive = correct.
                // Pin the intermediate state: setup_pending cleared,
                // total_tasks refreshed to 2, both tasks completed.
                // If any of these don't hold, the test isn't actually
                // exercising the racy state and the "didn't exit"
                // result is meaningless.
                assert!(
                    !primary.setup_pending,
                    "TaskAdded mirror must have cleared setup_pending; \
                     if not, the loop stayed alive only because the \
                     setup_pending gate was still active — not what \
                     this test is pinning"
                );
                assert_eq!(
                    primary.total_tasks, 2,
                    "total_tasks must have refreshed from \
                     cluster_state.task_count() = 2"
                );
                assert_eq!(
                    primary.completed_tasks.len(),
                    2,
                    "both TaskCompleted mirrors must have landed; \
                     completed.len() < 2 means the loop didn't \
                     actually reach the racy state"
                );
                assert!(
                    !primary.cluster_state_for_test().run_complete(),
                    "cluster_state.run_complete() must still be false; \
                     a stray RunComplete here would invalidate the \
                     test premise"
                );
            }
        }

        // Phase B: inject RunComplete and assert the loop NOW exits
        // promptly. Distinguishes "demoted exit gate fixed" (correct)
        // from "loop wedged forever" (would also pass Phase A but for
        // the wrong reason).
        incoming_tx
            .send(DistributedMessage::ClusterMutation {
                sender_id: "sec-promoted".into(),
                timestamp: 0.0,
                mutations: vec![ClusterMutation::<TestId>::RunComplete],
            })
            .unwrap();
        let _hold = incoming_tx;

        let phase_b = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            primary.operational_loop(),
        )
        .await;
        match phase_b {
            Ok(Ok(())) => {
                assert!(
                    primary.cluster_state_for_test().run_complete(),
                    "cluster_state.run_complete() must be set on \
                     exit; otherwise the loop exited via the \
                     transport-closed fallback (sender held open \
                     above to prevent that path) or some other arm"
                );
            }
            Ok(Err(e)) => panic!(
                "demoted-primary operational_loop returned Err on \
                 RunComplete: {e}"
            ),
            Err(_) => panic!(
                "demoted-primary operational_loop did not exit within \
                 5s after RunComplete was injected — the run_complete \
                 exit arm is broken, or the new `partial_view` gate \
                 also accidentally suppressed the run_complete exit"
            ),
        }
    }).await;
}
