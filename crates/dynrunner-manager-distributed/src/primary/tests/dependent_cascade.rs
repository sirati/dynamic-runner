//! Unfulfillable-dependents termination: a dependency graph that can no
//! longer complete must produce a loud terminal, not an infinite wait.
//!
//! The pinned production sequence (distributed-local-subprocess e2e,
//! 2026-06-10): task `a` fails terminally (`NonRecoverable`, the
//! "not pre-staged" guard) while task `b` declares `task_depends_on=[a]`.
//! `b` sits in the pool's blocked map; the phase's drain gate counted it
//! as live work, so the phase never reached the drain edge, the per-phase
//! retry buckets never got to decline, the permanent-failure cascade never
//! ran, and the operational loop's run-completion checks
//! (`completed + failed >= total`, pool-drained) never tripped — the run
//! hung forever after the aggregated NonRecoverable failure report.
//!
//! The fix routes the wire-terminal failure into the pool as a
//! soft (retry-pending) failure, lets the drain gate discount blocked
//! items that are doomed by same-phase soft-failed / final-failed
//! prereqs, and — once the phase's retry buckets decline at the drain
//! edge — finalizes the soft failures, cascade-failing every transitive
//! dependent with the canonical `upstream-failed` shape so the run
//! accounting completes and the terminal fires.

use dynrunner_core::{PhaseId, TaskDep};

use super::*;

/// Commit-1 (Defect 1): a `NonRecoverable` Work task with NO dependents
/// must terminalize to `fail_final` IMMEDIATELY — it must NOT be routed to
/// the soft / retry-pending path (NonRecoverable can never be a retry
/// candidate, so a soft marker would only delay its terminalization to the
/// drain edge). The run reaches a clean terminal with `succeeded=0`,
/// `fail_final=1`, and `retry_passes_used==0` (no pass is burned on a
/// permanently-failed task), and NO hang.
#[tokio::test(flavor = "current_thread")]
async fn nonrecoverable_no_deps_fails_final_immediately() {
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
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                keepalive_interval: Duration::from_millis(50),
                // A generous retry budget proves the NonRecoverable is NOT a
                // candidate: if it were mis-routed to the soft path it could
                // burn a pass; the assertion below requires it stays 0.
                retry_max_passes: 3,
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // Relative path + no staging on the secondary → the
            // unresolvable-task guard fails it NonRecoverable.
            let a = make_relative_binary("missing/only", 50);
            let (deps, ops, ope) = noop_phase_args();
            seed_operational_ledger(&mut primary, vec![a], deps);
            tokio::time::timeout(
                Duration::from_secs(30),
                primary.run(
                    SeedSource::PromotionSnapshot {
                        kind: crate::process::BootstrapKind::Failover,
                    },
                    ops,
                    ope,
                ),
            )
            .await
            .expect("a NonRecoverable no-dep task must terminalize, not hang")
            .unwrap();

            assert_eq!(primary.completed_count(), 0, "nothing succeeds");
            assert_eq!(
                primary.failed_count(),
                1,
                "the NonRecoverable task is fail_final"
            );
            assert_eq!(
                primary.retry_passes_used_for_test(),
                0,
                "a NonRecoverable failure must NEVER consume a retry pass"
            );

            drop(primary);
            let _ = sec_handle.await;
        })
        .await;
}

/// Commit-2 (Defect 2): a PERPETUAL-recoverable prereq (fails every
/// attempt; `retry_max_passes = 1`) with a dependent must, after the one
/// pass EXHAUSTS, finalize (soft → permanent) and cascade-fail the
/// dependent — reaching a clean terminal, NOT a hang and NOT a wholesale
/// abort. The prereq counts as `fail_retry` (it exhausted its bucket) and
/// the dependent as `fail_final` (cascade); the run exits within the
/// bound.
#[tokio::test(flavor = "current_thread")]
async fn perpetual_recoverable_prereq_exhausts_then_cascades_dependent() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);
            // "root" fails on EVERY attempt (quota = u32::MAX), Recoverable.
            let mut quotas = HashMap::new();
            quotas.insert("/tmp/root".to_string(), u32::MAX);
            let flaky = super::test_helpers::FlakyWorkerFactory::with_quotas(quotas);
            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) = spawn_real_secondary_flaky(
                "sec-0".into(),
                /* num_workers = */ 1,
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
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                keepalive_interval: Duration::from_millis(50),
                retry_max_passes: 1,
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // "root" (perpetual-recoverable) + "child" depends on it.
            let root = make_binary("root", 50);
            let mut child = make_binary("child", 40);
            child.task_depends_on = vec![TaskDep {
                task_id: root.task_id.clone(),
                phase_id: PhaseId::from("default"),
                inherit_outputs: false,
                def_id: None,
            }];
            let (deps, ops, ope) = noop_phase_args();
            seed_operational_ledger(&mut primary, vec![root, child], deps);
            tokio::time::timeout(
                Duration::from_secs(30),
                primary.run(
                    SeedSource::PromotionSnapshot {
                        kind: crate::process::BootstrapKind::Failover,
                    },
                    ops,
                    ope,
                ),
            )
            .await
            .expect(
                "after the retry budget exhausts, the soft prereq must \
                 finalize and cascade its dependent — a timeout is the hang",
            )
            .unwrap();

            assert_eq!(primary.completed_count(), 0, "nothing succeeds");
            assert_eq!(
                primary.failed_count(),
                2,
                "root (exhausted) AND child (cascade) are both accounted"
            );
            assert_eq!(
                primary.retry_passes_used_for_test(),
                1,
                "exactly one Recoverable retry pass consumed before exhaustion"
            );

            drop(primary);
            let _ = sec_handle.await;
        })
        .await;
}

/// `b` depends on `a`; `a` fails NonRecoverable on dispatch (the
/// unresolvable-task "expected StageFile notification first" guard —
/// the exact production failure class). The run MUST terminate within
/// the timeout with BOTH tasks accounted as failed: `a` with its own
/// error, `b` cascade-failed as `upstream-failed`.
///
/// Pre-fix this test TIMES OUT: `b` stays blocked forever and no
/// run-completion check ever trips (the e2e hang, bounded here).
#[tokio::test(flavor = "current_thread")]
async fn terminally_failed_dep_cascades_dependents_and_run_exits() {
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
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);

            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                keepalive_interval: Duration::from_millis(50),
                // No retry budget: the first drain edge's bucket pass
                // declines and the soft failure finalizes immediately.
                // (The failure under test is NonRecoverable — never a
                // bucket candidate — so the budget is moot; 0 keeps the
                // test deterministic against future bucket changes.)
                retry_max_passes: 0,
                ..test_primary_config()
            };

            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // `a`: relative path + no src_network on the secondary + no
            // staging queued → the secondary's unresolvable-task guard
            // fails it NonRecoverable ("expected StageFile notification
            // first") — the production failure class verbatim.
            let a = make_relative_binary("missing/dep_root", 50);
            // `b`: blocked on `a` via the per-task dep edge. Never
            // dispatchable once `a` is terminally failed.
            let mut b = make_relative_binary("missing/dependent", 50);
            b.task_depends_on = vec![TaskDep {
                task_id: a.task_id.clone(),
                phase_id: PhaseId::from("default"),
                inherit_outputs: false,
                def_id: None,
            }];
            let binaries = vec![a, b];

            let (deps, ops, ope) = noop_phase_args();
            seed_operational_ledger(&mut primary, binaries, deps);
            // BOUNDED: pre-fix the run never exits (the production hang);
            // the timeout converts that into a loud test failure.
            tokio::time::timeout(
                Duration::from_secs(30),
                primary.run(SeedSource::PromotionSnapshot { kind: crate::process::BootstrapKind::Failover }, ops, ope),
            )
            .await
            .expect(
                "run must terminate once the dependency graph can no longer \
                 complete (terminally-failed prereq + blocked dependent) — \
                 a timeout here is the production hang",
            )
            .unwrap();

            assert_eq!(primary.completed_count(), 0, "nothing can complete");
            assert_eq!(
                primary.failed_count(),
                2,
                "both the root failure AND its unfulfillable dependent must \
                 be accounted as failed (the dependent via the cascade)"
            );

            // The dependent's CRDT terminal carries the canonical
            // upstream-failed shape so operators can attribute it.
            let cs = primary.cluster_state_for_test();
            let mut saw_upstream_failed = false;
            for (_hash, state) in cs.tasks_iter() {
                if let crate::cluster_state::TaskState::Failed {
                    last_error, ..
                } = state
                    && state.def().task_id == "missing/dependent"
                {
                    assert!(
                        last_error.contains("upstream-failed"),
                        "cascaded dependent must carry the canonical \
                         upstream-failed error; got: {last_error}"
                    );
                    saw_upstream_failed = true;
                }
            }
            assert!(
                saw_upstream_failed,
                "the blocked dependent must reach a Failed terminal in the \
                 cluster ledger (no silent strand)"
            );

            drop(primary);
            let _ = sec_handle.await;
        })
        .await;
}
