//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;


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
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
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
                    resource_check_interval: Duration::from_millis(100),
                    log_oom_watcher: false,
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
        // Box keeps `BufOut` small: `DistributedMessage<TestId>` is
        // ~360B while `Flush` is one oneshot::Sender — boxing the
        // heavy variant stops the entire enum from carrying the
        // worst-case payload through the writer's mpsc.
        Msg(Box<DistributedMessage<TestId>>),
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
                .send(BufOut::Msg(Box::new(msg)))
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
                        if inner_tx.send(*msg).is_err() {
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
                    resource_check_interval: std::time::Duration::from_millis(100),
                    log_oom_watcher: false,
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
                    resource_check_interval: Duration::from_millis(100),
                    log_oom_watcher: false,
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
        let hash = crate::primary::wire::compute_task_hash(&bin);
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
