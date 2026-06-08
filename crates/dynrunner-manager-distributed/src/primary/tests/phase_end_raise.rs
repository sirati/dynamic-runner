//! Honest `on_phase_end` (Phase 6c): a consumer phase-end hook that
//! RAISES must surface as a run-level FATAL failure
//! (`RunError::FatalPolicyExit`), not the old warn-and-continue
//! false-green.
//!
//! Mechanism under test (Rust side): the `on_phase_end` closure records
//! a raise into a shared [`PhaseHookRaiseLatch`] the coordinator was
//! wired with (`set_phase_hook_raise_latch`); the phase cascade reads
//! the latch immediately after firing the hook and emits
//! [`WorkerMgmtSignal::PolicyFatalExit`] onto the decoupled
//! worker-management bus, which the operational loop's worker arm maps
//! to a `RunError::FatalPolicyExit` break outcome. The `()` callback
//! return is unchanged.
//!
//! These tests stand in for the pyo3 `make_on_phase_end` closure by
//! recording into the latch directly — the pyo3-side proof that BOTH the
//! modern-signature and legacy-signature raise arms route through the
//! SAME `record` lives in `dynrunner-pyo3`'s `lifecycle` test module
//! (the modern/legacy distinction is a pyo3 kwarg-binding concern). Here
//! the concern is the manager-distributed contract: a recorded raise →
//! `FatalPolicyExit`; no raise → clean completion.

use super::*;

/// 1 real primary + 1 real secondary, 5 single-phase tasks. The
/// `on_phase_end` hook RECORDS a raise into the wired latch (the Rust
/// analog of the pyo3 closure observing a Python exception). The run
/// MUST surface `RunError::FatalPolicyExit`, never a clean `Ok`.
#[tokio::test(flavor = "current_thread")]
async fn on_phase_end_raise_surfaces_fatal_policy_exit() {
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

            // Wire the SAME latch into the coordinator AND the closure —
            // exactly as the pyo3 real-primary path does.
            let raise_latch = PhaseHookRaiseLatch::new();
            primary.set_phase_hook_raise_latch(raise_latch.clone());

            let binaries: Vec<TaskInfo<TestId>> = (0..5)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            let on_start: OnPhaseStart = Box::new(|_p: &dynrunner_core::PhaseId| {});
            // The hook "raises": record into the latch (what the pyo3
            // closure does on a caught Python exception). `()` return.
            let hook_latch = raise_latch.clone();
            let on_end: OnPhaseEnd = Box::new(move |_p, _c, _f, _outputs| {
                hook_latch.record("synthetic on_phase_end raise".to_string());
            });

            // Operational primary (mesh-always): seed the inherited ledger +
            // run as `PromotionSnapshot` (a `ColdStart` would relocate away,
            // never reaching the on_phase_end raise this test asserts).
            seed_operational_ledger(&mut primary, binaries, HashMap::new());
            let result = primary
                .run(SeedSource::PromotionSnapshot, on_start, on_end)
                .await;

            // The contract under test is the RUN RESULT. The
            // worker-mgmt-fail early-return surfaces the fatal outcome
            // BEFORE the terminal `RunComplete` broadcast (same as the
            // pre-existing `RunShouldFail`→`Other` path), so the
            // straggler secondary is not signalled to exit here — abort
            // its task rather than awaiting it (the production cluster
            // teardown is a separate concern, out of 6c scope).
            drop(primary);
            sec_handle.abort();

            match result {
                Err(RunError::FatalPolicyExit { reason }) => {
                    assert!(
                        reason.contains("on_phase_end") && reason.contains("raised"),
                        "FatalPolicyExit reason should name the raised on_phase_end hook; \
                         got: {reason}"
                    );
                }
                other => panic!(
                    "an on_phase_end hook that raised must surface \
                     RunError::FatalPolicyExit, not warn-and-continue; got {other:?}"
                ),
            }
        })
        .await;
}

/// Control: an `on_phase_end` hook that does NOT raise (never records
/// into the latch) leaves the run unaffected — it completes cleanly.
/// Proves the fatal path is gated strictly on a recorded raise, not a
/// mere wiring of the latch.
#[tokio::test(flavor = "current_thread")]
async fn non_raising_on_phase_end_completes_cleanly() {
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

            // Latch wired, but the hook never records into it.
            let raise_latch = PhaseHookRaiseLatch::new();
            primary.set_phase_hook_raise_latch(raise_latch);

            let binaries: Vec<TaskInfo<TestId>> = (0..5)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            let on_start: OnPhaseStart = Box::new(|_p: &dynrunner_core::PhaseId| {});
            let on_end: OnPhaseEnd = Box::new(|_p, _c, _f, _outputs| {});

            // Operational primary (mesh-always): seed the inherited ledger +
            // run as `PromotionSnapshot` (a `ColdStart` would relocate away).
            seed_operational_ledger(&mut primary, binaries, HashMap::new());
            let result = primary
                .run(SeedSource::PromotionSnapshot, on_start, on_end)
                .await;

            let completed = primary.completed_count();
            let failed = primary.failed_count();
            drop(primary);
            let _ = sec_handle.await;

            assert!(
                result.is_ok(),
                "a non-raising on_phase_end must not fail the run; got {result:?}"
            );
            assert_eq!(completed, 5, "all tasks complete on the clean path");
            assert_eq!(failed, 0, "no failures on the clean path");
        })
        .await;
}
