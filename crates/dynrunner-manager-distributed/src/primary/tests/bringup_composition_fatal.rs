//! #420 face (a) — a fatal task-graph error at the authoritative pool
//! composition during bring-up MUST become a run-level fatal: latch +
//! broadcast the `RunAborted` terminal verdict and surface a typed
//! `RunError`, never the pre-fix ERROR-and-continue that left the primary
//! with an empty pool and the fleet dying one-by-one on setup deadlines.
//!
//! PRODUCTION REPLAY (asm-dataset LMU mesh-always, ~14:29): a promoted
//! primary ran mode-2 discovery, then at composition logged
//!   ERROR "post-composition: invalid task graph in cluster_state; primary
//!   will start with no pending tasks error=duplicate task_id
//!   `g2o-…O0__0babd3b0` in pool"
//! …and that was its LAST line — no exit, no verdict. The dup was a
//! duplicate `(phase_id, task_id)` whose two entries hash DIFFERENTLY
//! (`compute_task_hash` folds `(phase_id, path, identifier)`, NOT `task_id`),
//! so both land in the CRDT (the `TaskAdded` NoOp never collapses them) and
//! only `PendingPool::extend`'s task_id-uniqueness check surfaces the
//! collision — at composition, inside `hydrate_from_cluster_state`.
//!
//! Contract under fix:
//!   * `hydrate_from_cluster_state` SURFACES the graph error as `Err`
//!     instead of swallowing it (`pending = None` + ERROR + continue);
//!   * `discover_on_promotion` routes that `Err` through
//!     `abort_run_on_invalid_composition` — latch + broadcast `RunAborted`
//!     (carrying the offending task_id in the reason), then return the
//!     typed `RunError::InvalidComposedGraph`;
//!   * the typed error is NOT the swallow-eligible `Other` (the PyO3
//!     boundary RAISES it), and its Display names the duplicate task_id (the
//!     operator's one-line diagnosis).

use super::*;

use dynrunner_protocol_primary_secondary::ClusterMutation;

/// A discovered batch carrying a DUPLICATE `(phase_id, task_id)` identity
/// (the production shape): two tasks with the SAME `task_id` but DISTINCT
/// content, so they hash differently and BOTH seed into the CRDT — the
/// collision is invisible to the hash-keyed dedup and surfaces only at the
/// composition `extend`.
fn duplicate_task_id_discovery_batch() -> Vec<TaskInfo<TestId>> {
    // Distinct content (identifier `a` vs `b` ⇒ distinct hash) but the SAME
    // task_id — exactly the production `g2o-…` duplicate shape.
    let mut t1 = make_binary("content-a", 100);
    t1.task_id = "g2o-git-libg2o_types_slam3d_addons.so/x86_64/gcc/11.1.0/O0__0babd3b0".into();
    let mut t2 = make_binary("content-b", 100);
    t2.task_id = "g2o-git-libg2o_types_slam3d_addons.so/x86_64/gcc/11.1.0/O0__0babd3b0".into();
    // Sanity: the two entries hash differently, so the CRDT keeps both
    // (the dup is NOT collapsed by the content hash — the whole point).
    assert_ne!(
        crate::primary::wire::compute_task_hash(&t1),
        crate::primary::wire::compute_task_hash(&t2),
        "fixture: the duplicate-task_id pair must hash DISTINCTLY (so both \
         seed the ledger and only extend surfaces the collision)"
    );
    vec![t1, t2]
}

/// THE REPLAY: `discover_on_promotion` over a duplicate-task_id batch must
/// (1) latch + broadcast the `RunAborted` verdict naming the dup task_id, and
/// (2) return the typed `RunError::InvalidComposedGraph` (NOT the swallow-
/// eligible `Other`, NOT a silent empty pool).
///
/// RED against the pre-fix code: hydrate logged the ERROR, set `pending =
/// None`, and `return`ed `()`; `discover_on_promotion` then returned `Ok(())`
/// with NO verdict and NO typed error — the run never aborted.
#[tokio::test(flavor = "current_thread")]
async fn discover_on_promotion_duplicate_task_id_aborts_run_with_verdict() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // The relocated/pre-staged primary owes discovery.
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::DiscoveryDebtDeclared);
            let fires = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
            primary.register_setup_discovery(fixed_discovery(
                duplicate_task_id_discovery_batch(),
                HashMap::new(),
                fires.clone(),
            ));

            let err = primary.discover_on_promotion().await.expect_err(
                "a discovery batch with a duplicate (phase_id, task_id) must \
                     ABORT the run at composition, never resolve Ok with an empty \
                     pool (the production ERROR-and-continue defect)",
            );

            // (1) The typed error is the structured composition fatal, NOT the
            //     swallow-eligible `Other` (the PyO3 boundary RAISES it).
            assert!(
                !matches!(err, crate::primary::RunError::Other(_)),
                "the composition fatal surfaced as the swallow-eligible \
                 RunError::Other — the PyO3 boundary maps that to exit 0, the \
                 exact false-not-shutdown this fix prevents: {err}"
            );
            assert!(
                matches!(err, crate::primary::RunError::InvalidComposedGraph { .. }),
                "expected the structured InvalidComposedGraph fatal, got: {err:?}"
            );

            // (2) The error's Display NAMES the offending task_id (the
            //     operator's one-line diagnosis — the production trace's value).
            let shown = err.to_string();
            assert!(
                shown.contains("g2o-git-libg2o_types_slam3d_addons.so"),
                "the InvalidComposedGraph Display must name the duplicate \
                 task_id (operators diagnose from that one line); got: {shown}"
            );

            // (3) The terminal verdict is LATCHED + present in the replicated
            //     ledger, carrying the dup task_id — so every secondary's
            //     setup-wait run-terminal gate (setup.rs loop head) and the
            //     observer see the TRUE reason. (RED pre-fix: no verdict was
            //     ever authored.)
            let reason = primary.cluster_state_for_test().run_aborted().expect(
                "the composition fatal must latch RunAborted so the fleet \
                     exits on the verdict instead of dying on setup deadlines",
            );
            assert!(
                reason.contains("g2o-git-libg2o_types_slam3d_addons.so"),
                "the latched RunAborted reason must name the duplicate identity; \
                 got: {reason}"
            );

            // The pool is empty (hydrate set it None on the Err) — the run is
            // aborting, so nothing is dispatchable.
            assert!(
                primary.pending.is_none(),
                "an invalid-composition abort must leave no pending pool"
            );
        })
        .await;
}

/// The verdict is BROADCAST to the connected fleet, not merely latched
/// locally: a connected secondary's inbound channel receives the
/// `RunAborted` mutation. This is the half that makes a secondary's
/// setup-wait exit on the verdict (its loop-head `run_aborted()` check)
/// instead of dying on its unconfigured deadline — the production fleet
/// never heard it because the verdict was never authored.
#[tokio::test(flavor = "current_thread")]
async fn invalid_composition_verdict_is_broadcast_to_the_fleet() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(1);
            let (_sec_id, mut to_sec_rx, _incoming_tx) = secondary_ends.into_iter().next().unwrap();
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::DiscoveryDebtDeclared);
            let fires = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
            primary.register_setup_discovery(fixed_discovery(
                duplicate_task_id_discovery_batch(),
                HashMap::new(),
                fires.clone(),
            ));

            let _ = primary.discover_on_promotion().await.expect_err("aborts");
            // Let the mesh-pump drain the egress queue onto the wire.
            settle_pump().await;

            let mut saw_abort = false;
            while let Ok(msg) = to_sec_rx.try_recv() {
                if let DistributedMessage::ClusterMutation { mutations, .. } = msg {
                    for m in mutations {
                        if let ClusterMutation::RunAborted { reason, .. } = m {
                            assert!(
                                reason.contains("g2o-git-libg2o_types_slam3d_addons.so"),
                                "the broadcast RunAborted must carry the dup \
                                 task_id reason; got: {reason}"
                            );
                            saw_abort = true;
                        }
                    }
                }
            }
            assert!(
                saw_abort,
                "the invalid-composition abort must BROADCAST RunAborted to the \
                 connected fleet (the production verdict the secondaries never \
                 heard) — not merely latch it locally"
            );
        })
        .await;
}

// ── #420 face (b): the primary MUST log its exit on EVERY path ───────────
//
// No log line proved the production primary ever exited (its last line was the
// composition ERROR). The single chokepoint `run_pipeline` (wrapping
// `run_pipeline_inner`, the one place both `run` and `run_consuming` flow
// through) now emits a GUARANTEED "primary exiting: ..." line — INFO on a
// clean Ok, ERROR with the `RunError` reason on an Err. Asserted via
// `TargetCapture` on the coordinator module target on BOTH paths.

use crate::test_capture::TargetCapture;

/// Install a `TargetCapture` on the coordinator module target (the run-loop
/// exit line's home), scoped to this thread (and so, on a current_thread
/// runtime, to the whole test). Always-interest, safe to hold across `.await`.
fn coordinator_log_capture() -> (TargetCapture, tracing::subscriber::DefaultGuard) {
    use tracing_subscriber::layer::SubscriberExt;
    let capture = TargetCapture::for_target(crate::primary::coordinator::LOG_TARGET);
    let subscriber = tracing_subscriber::Registry::default().with(capture.clone());
    let guard = tracing::subscriber::set_default(subscriber);
    (capture, guard)
}

/// The exit line on the ERROR path: a `run(PromotionSnapshot)` whose inherited
/// ledger ALREADY carries a duplicate `(phase_id, task_id)` identity aborts at
/// the pre-loop hydrate (`run_pipeline`'s composition gate, BEFORE
/// `wait_for_connections` — so no fleet is needed and the run returns
/// promptly). The single chokepoint must emit the ERROR "primary exiting"
/// line naming the failure, and `run` must surface the typed
/// `InvalidComposedGraph`.
///
/// This also pins face (a)'s OTHER routing site: the `run_pipeline` pre-loop
/// hydrate (not just `discover_on_promotion`) routes a composition `Err`
/// through `abort_run_on_invalid_composition`.
#[tokio::test(flavor = "current_thread")]
async fn primary_logs_error_exit_on_composition_fatal() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (capture, _guard) = coordinator_log_capture();
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // Seed the inherited ledger with a duplicate-task_id pair (the
            // production shape: distinct content ⇒ distinct hash, same
            // task_id ⇒ both seeded, collision only at composition).
            {
                let cs = primary.cluster_state_mut_for_test();
                for t in duplicate_task_id_discovery_batch() {
                    cs.apply(ClusterMutation::TaskAdded {
                        hash: crate::primary::wire::compute_task_hash(&t),
                        task: t,
                        def_id: None,
                    });
                }
            }

            let (deps, ops, ope) = noop_phase_args();
            let _ = deps;
            let err = primary
                .run(
                    SeedSource::PromotionSnapshot {
                        kind: crate::process::BootstrapKind::Failover,
                    },
                    ops,
                    ope,
                )
                .await
                .expect_err("the inherited dup-task_id ledger must abort the run");
            assert!(
                matches!(err, crate::primary::RunError::InvalidComposedGraph { .. }),
                "run_pipeline's pre-loop hydrate must route the composition Err \
                 through the abort path; got {err:?}"
            );

            // The single chokepoint emitted the ERROR exit line.
            let events = capture.events();
            let exit_err = events.iter().find(|e| {
                e.event.message.contains("primary exiting") && e.level == tracing::Level::ERROR
            });
            assert!(
                exit_err.is_some(),
                "every primary run-loop exit must emit a 'primary exiting' line; \
                 an Err exit must be at ERROR. captured: {:?}",
                events
                    .iter()
                    .map(|e| (e.level, &e.event.message))
                    .collect::<Vec<_>>()
            );
        })
        .await;
}

/// The exit line on the OK path: a clean `run(PromotionSnapshot)` over an
/// empty inherited ledger (zero tasks ⇒ the operational loop's counter exit
/// fires immediately) against a connected compute secondary must emit the
/// INFO "primary exiting: run loop returned cleanly" line — the same single
/// chokepoint, the success arm.
#[tokio::test(flavor = "current_thread")]
async fn primary_logs_clean_exit_on_success() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (capture, _guard) = coordinator_log_capture();
            let (transport, secondary_ends) = setup_test(1);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                ..test_primary_config()
            };
            let (primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // A connected compute secondary so `wait_for_connections` proceeds;
            // the PromotionSnapshot ledger is EMPTY, so the operational loop's
            // counter exit (`0 + 0 >= 0`) fires and the run completes cleanly.
            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary(id, 2, 1024 * 1024 * 1024, rx, tx));
            }

            let (_deps, ops, ope) = noop_phase_args();
            let outcome = tokio::time::timeout(
                Duration::from_secs(10),
                primary.run_consuming(
                    SeedSource::PromotionSnapshot {
                        kind: crate::process::BootstrapKind::Failover,
                    },
                    ops,
                    ope,
                ),
            )
            .await
            .expect("the empty PromotionSnapshot run must complete promptly");
            assert!(
                matches!(
                    outcome,
                    Ok(crate::primary::PrimaryRunOutcome::Local { result: Ok(()), .. })
                ),
                "an empty PromotionSnapshot run must complete cleanly; got {outcome:?}"
            );

            let events = capture.events();
            let exit_ok = events.iter().find(|e| {
                e.event.message.contains("primary exiting")
                    && e.event.message.contains("cleanly")
                    && e.level == tracing::Level::INFO
            });
            assert!(
                exit_ok.is_some(),
                "a clean primary exit must emit the INFO 'primary exiting: run \
                 loop returned cleanly' line. captured: {:?}",
                events
                    .iter()
                    .map(|e| (e.level, &e.event.message))
                    .collect::<Vec<_>>()
            );
        })
        .await;
}
