//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;


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
        let hash = crate::primary::wire::compute_task_hash(&bin);
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

/// `completed_count()` / `failed_count()` read from the CRDT-replicated
/// `cluster_state.outcome_counts()`, not from the local `completed_tasks`
/// / `failed_tasks` HashSets.
///
/// Concrete bug class this pins (#88 follow-up): on a demoted observer
/// the cross-secondary-completion mirror hop
/// (`mirror_mutation_to_accounting`) can be bypassed in production —
/// `cluster_state` converges via the CRDT broadcast but the local
/// HashSet stays empty. Pre-fix the dispatcher's PyO3-facing
/// `succeeded=N` stdout read `completed_count() = completed_tasks.len()`
/// and reported 0 for runs that genuinely completed. The terminal log
/// line was already migrated to `cluster_state.outcome_counts()` (Step
/// 11 / commit 37d450d); this test pins the same migration at the
/// `completed_count()` / `failed_count()` accessors.
///
/// The test artificially drives the divergence by directly populating
/// `cluster_state` (via the same `ClusterMutation` apply path the wire
/// uses) while leaving `completed_tasks` empty. A correct accessor
/// reads CRDT-authoritative; a pre-fix accessor would read the empty
/// HashSet and report 0.
#[tokio::test(flavor = "current_thread")]
async fn completed_and_failed_count_read_from_cluster_state_not_local_hashset() {
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

        // Seed cluster_state directly with two Completed entries +
        // one Failed { Recoverable }. We DELIBERATELY do NOT touch
        // `completed_tasks` or `failed_tasks` — that's the divergence
        // the production bypass produces. A pre-fix accessor would
        // return `completed_tasks.len() == 0`; the post-fix accessor
        // routes through `cluster_state.outcome_counts()` and returns
        // the CRDT count.
        for i in 0..2 {
            let bin = make_binary(&format!("done-{i}"), 100);
            let hash = crate::primary::wire::compute_task_hash(&bin);
            primary.cluster_state.apply(
                dynrunner_protocol_primary_secondary::ClusterMutation::<TestId>::TaskAdded {
                    hash: hash.clone(),
                    task: bin,
                },
            );
            primary.cluster_state.apply(
                dynrunner_protocol_primary_secondary::ClusterMutation::<TestId>::TaskCompleted {
                    hash,
                },
            );
        }
        let failing_bin = make_binary("flaky", 100);
        let failing_hash = crate::primary::wire::compute_task_hash(&failing_bin);
        primary.cluster_state.apply(
            dynrunner_protocol_primary_secondary::ClusterMutation::<TestId>::TaskAdded {
                hash: failing_hash.clone(),
                task: failing_bin,
            },
        );
        primary.cluster_state.apply(
            dynrunner_protocol_primary_secondary::ClusterMutation::<TestId>::TaskFailed {
                hash: failing_hash,
                kind: dynrunner_core::ErrorType::Recoverable,
                error: "synthetic recoverable failure".into(),
            },
        );

        // Pre-condition: HashSets are empty (we never touched them).
        assert!(
            primary.completed_tasks.is_empty(),
            "test setup: completed_tasks must be empty to simulate \
             the production mirror-bypass divergence"
        );
        assert!(
            primary.failed_tasks.is_empty(),
            "test setup: failed_tasks must be empty to simulate \
             the production mirror-bypass divergence"
        );

        // Post-fix: the accessors read CRDT-authoritative counts even
        // when the local HashSets are empty. Pre-fix these returned
        // `0` / `0` because they read from `completed_tasks.len()` /
        // `failed_tasks.len()`.
        assert_eq!(
            primary.completed_count(),
            2,
            "completed_count() must read the CRDT's succeeded partition \
             (2 TaskCompleted applied above), not the empty local \
             completed_tasks HashSet"
        );
        assert_eq!(
            primary.failed_count(),
            1,
            "failed_count() must read the sum of CRDT failure buckets \
             (1 TaskFailed Recoverable -> fail_retry == 1), not the empty \
             local failed_tasks HashSet"
        );

        // Sanity: outcome_summary() and the new accessors agree (the
        // accessors are thin views over the same outcome_counts()).
        let outcome = primary.outcome_summary();
        assert_eq!(
            primary.completed_count(),
            outcome.succeeded,
            "completed_count() must equal outcome_summary().succeeded — \
             both route through cluster_state.outcome_counts()"
        );
        assert_eq!(
            primary.failed_count(),
            outcome.fail_retry + outcome.fail_oom + outcome.fail_final,
            "failed_count() must equal the sum of outcome_summary()'s \
             failure buckets — same CRDT source"
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
        let hash_a = crate::primary::wire::compute_task_hash(&bin_a);
        let bin_b = make_binary("step6-arm-task-b", 100);
        let hash_b = crate::primary::wire::compute_task_hash(&bin_b);
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
        let hash_a = crate::primary::wire::compute_task_hash(&bin_a);
        let bin_b = make_binary("lmu-task-b", 100);
        let hash_b = crate::primary::wire::compute_task_hash(&bin_b);
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
