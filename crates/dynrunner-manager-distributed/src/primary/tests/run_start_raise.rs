//! Honest `on_run_start`: a consumer run-start hook that RAISES on the
//! PROMOTED-primary path must surface as a run-level FATAL failure
//! (`RunError::FatalPolicyExit`), not the old log-and-continue
//! false-green (run_20260611_221215: a promoted primary absorbed a
//! raising `on_run_start` and finished as "Ok").
//!
//! Mechanism under test (Rust side): the pyo3 promotion recipe fires
//! `on_run_start` synchronously BEFORE `run_consuming` and, on a raise,
//! records the reason via
//! [`PrimaryCoordinator::record_pre_run_hook_abort`]; `run_pipeline`
//! reads it at the post-connection abort gate (`fire_pre_run_hook_abort`),
//! broadcasts the replicated `RunAborted` verdict (so the fleet stops),
//! and returns `RunError::FatalPolicyExit`. These tests stand in for the
//! pyo3 recipe by recording the directive directly — the pyo3-side proof
//! that a raising `on_run_start` reaches `record_pre_run_hook_abort`
//! (instead of being swallowed) lives in `dynrunner-pyo3`'s
//! `relocated_primary_tests`. Here the concern is the manager-distributed
//! contract: a recorded pre-run hook abort → `FatalPolicyExit` +
//! `RunAborted` broadcast; no record → clean completion (mirroring
//! `phase_end_raise`'s split exactly — the cold-start path is unchanged
//! and already fatals via `?`-propagation before `run()`).

use super::*;

/// 1 real primary + 1 real secondary, 5 single-phase tasks. The promoted
/// primary's `on_run_start` "raised" (the directive is recorded — the
/// Rust analog of the pyo3 recipe catching a Python exception). The run
/// MUST surface `RunError::FatalPolicyExit` AND broadcast `RunAborted`,
/// never a clean `Ok`/`RunComplete`.
#[tokio::test(flavor = "current_thread")]
async fn on_run_start_raise_surfaces_fatal_policy_exit() {
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
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // The promoted primary's `on_run_start` raised: the pyo3 recipe
            // records the directive onto the coordinator BEFORE `run`. Done
            // directly here (the Rust analog of catching the Python exception).
            primary.record_pre_run_hook_abort(
                "TaskDefinition.on_run_start raised: SystemExit: 2".to_string(),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..5)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            let on_start: OnPhaseStart = Box::new(|_p: &dynrunner_core::PhaseId| {});
            let on_end: OnPhaseEnd = Box::new(|_p, _c, _f, _outputs| {});

            // Operational primary (mesh-always): seed the inherited ledger +
            // run as `PromotionSnapshot` (the promoted-primary path — a
            // `ColdStart` would relocate away before reaching the abort gate).
            seed_operational_ledger(&mut primary, binaries, HashMap::new());
            let result = primary
                .run(SeedSource::PromotionSnapshot { kind: crate::process::BootstrapKind::Failover }, on_start, on_end)
                .await;

            // The terminal RUN VERDICT: the pre-run hook abort must broadcast
            // the honest `RunAborted` (the failure twin of `RunComplete`)
            // before exiting, so the fleet stops on the replicated verdict
            // rather than idling into its own deadline. The primary's own
            // local apply is the faithful observable for WHAT was broadcast.
            let state = primary.cluster_state_for_test();
            let abort_reason = state
                .run_aborted()
                .unwrap_or_else(|| {
                    panic!(
                        "a pre-run hook abort must broadcast RunAborted \
                         (run_aborted() = Some); run_complete()={}",
                        state.run_complete()
                    )
                })
                .to_string();
            assert!(
                !state.run_complete(),
                "a pre-run hook abort must NOT latch RunComplete — the fleet \
                 would narrate a false success (the run_20260611_221215 smell)"
            );
            assert!(
                abort_reason.contains("on_run_start"),
                "the abort reason must carry the on_run_start raise text, \
                 got: {abort_reason}"
            );

            // Fleet-teardown half: the verdict landed on the REAL secondary's
            // CRDT mirror, so its loop exits on its own instead of idling into
            // a timeout. A hung handle here is the absorb-and-continue defect.
            let sec_exit = tokio::time::timeout(Duration::from_secs(10), sec_handle).await;
            assert!(
                sec_exit.is_ok(),
                "the RunAborted verdict must tear the secondary down on its \
                 own; it idled past the 10s budget"
            );
            drop(primary);

            match result {
                Err(RunError::FatalPolicyExit { reason }) => {
                    assert!(
                        reason.contains("on_run_start"),
                        "FatalPolicyExit reason should name the raised on_run_start \
                         hook; got: {reason}"
                    );
                }
                other => panic!(
                    "an on_run_start hook that raised on the promoted primary must \
                     surface RunError::FatalPolicyExit, not log-and-continue; \
                     got {other:?}"
                ),
            }
        })
        .await;
}

/// Control: a promoted primary whose `on_run_start` did NOT raise (no
/// directive recorded) completes cleanly — the fatal path is gated
/// strictly on a recorded raise, not on the gate's mere presence. Pins
/// that the cold-start / non-raising promotion path is untouched.
#[tokio::test(flavor = "current_thread")]
async fn non_raising_on_run_start_completes_cleanly() {
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
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // No `record_pre_run_hook_abort`: the hook did not raise.

            let binaries: Vec<TaskInfo<TestId>> = (0..5)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            let on_start: OnPhaseStart = Box::new(|_p: &dynrunner_core::PhaseId| {});
            let on_end: OnPhaseEnd = Box::new(|_p, _c, _f, _outputs| {});

            seed_operational_ledger(&mut primary, binaries, HashMap::new());
            let result = primary
                .run(SeedSource::PromotionSnapshot { kind: crate::process::BootstrapKind::Failover }, on_start, on_end)
                .await;

            let completed = primary.completed_count();
            let failed = primary.failed_count();
            let state = primary.cluster_state_for_test();
            assert!(
                state.run_aborted().is_none(),
                "a clean run must not broadcast RunAborted, got: {:?}",
                state.run_aborted()
            );
            assert!(state.run_complete(), "a clean run must latch RunComplete");
            drop(primary);
            let _ = sec_handle.await;

            assert!(
                result.is_ok(),
                "a non-raising on_run_start must not fail the run; got {result:?}"
            );
            assert_eq!(completed, 5, "all tasks complete on the clean path");
            assert_eq!(failed, 0, "no failures on the clean path");
        })
        .await;
}
