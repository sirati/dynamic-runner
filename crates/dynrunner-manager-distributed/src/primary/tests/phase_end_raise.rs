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
                .run(SeedSource::PromotionSnapshot { kind: crate::process::BootstrapKind::Failover }, on_start, on_end)
                .await;

            // #313 — the terminal RUN VERDICT. The worker-mgmt-fail
            // early-return must broadcast the honest `RunAborted` (the
            // failure twin of `RunComplete`) before exiting. Pre-fix it
            // broadcast NOTHING: the primary just vanished, the straggler
            // secondary idled into its own timeouts (this test had to
            // `abort()` it), and the observer reported nothing. The
            // primary's own local apply is the faithful observable for
            // WHAT was broadcast.
            let state = primary.cluster_state_for_test();
            let abort_reason = state
                .run_aborted()
                .unwrap_or_else(|| {
                    panic!(
                        "a fatal-policy exit must broadcast RunAborted \
                         (run_aborted() = Some); run_complete()={}",
                        state.run_complete()
                    )
                })
                .to_string();
            assert!(
                !state.run_complete(),
                "a fatal-policy exit must NOT latch RunComplete — the fleet \
                 would narrate a false success"
            );
            assert!(
                abort_reason.contains("on_phase_end"),
                "the abort reason must carry the FatalPolicyExit render naming \
                 the raised hook, got: {abort_reason}"
            );

            // Fleet-teardown half (#313): the verdict landed on the REAL
            // secondary's CRDT mirror, so its `process_tasks` loop exits on
            // its own (`SecondaryTerminal::Aborted` → non-zero at the PyO3
            // boundary) instead of idling into a timeout. A hung handle
            // here is the pre-fix defect.
            let sec_exit = tokio::time::timeout(Duration::from_secs(10), sec_handle).await;
            assert!(
                sec_exit.is_ok(),
                "the RunAborted verdict must tear the secondary down on its \
                 own; it idled past the 10s budget (the pre-#313 defect)"
            );
            drop(primary);

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

/// Shared two-phase fixture for the synchronous dispatch-freeze tests:
/// phase `p1` holds one task in flight on `(sec-0, 0)`; dependent phase
/// `p2` holds one Pending task. The `on_phase_end` hook records a raise
/// into the wired latch iff `raises`. Driving the p1 terminal through
/// `dispatch_message` runs the REAL cascade (hook fire → latch read →
/// run-fail emit) and frees the worker — the post-raise dispatch
/// surface is then directly assertable.
async fn drive_phase_end_with_two_phase_ledger(
    raises: bool,
) -> (
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    PrimaryMeshKeepalive,
) {
    let (transport, _ends) = setup_test(1);
    let (mut primary, mesh) = build_test_primary(
        PrimaryConfig::default(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    let mut p1_task = make_binary("p1_task", 100);
    p1_task.phase_id = dynrunner_core::PhaseId::from("p1");
    let p1_hash = crate::primary::wire::compute_task_hash(&p1_task);
    let mut p2_task = make_binary("p2_task", 100);
    p2_task.phase_id = dynrunner_core::PhaseId::from("p2");
    let p2_hash = crate::primary::wire::compute_task_hash(&p2_task);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(
                dynrunner_core::PhaseId::from("p2"),
                vec![dynrunner_core::PhaseId::from("p1")],
            )]),
        });
        cs.apply(ClusterMutation::PeerJoined {
            peer_id: "sec-0".into(),
            is_observer: false,
            can_be_primary: true,
            cap_version: Default::default(),
            member_gen: 0,
        });
        cs.apply(ClusterMutation::SecondaryCapacity {
            secondary: "sec-0".into(),
            worker_count: 1,
            resources: vec![dynrunner_core::ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: 8 * 1024 * 1024 * 1024,
            }],
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: p1_hash.clone(),
            task: p1_task,
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: p2_hash,
            task: p2_task,
        });
        cs.apply(ClusterMutation::TaskAssigned {
            attempt: 0,
            hash: p1_hash.clone(),
            secondary: "sec-0".into(),
            worker: 0,
            version: Default::default(),
        });
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");
    // The mesh-readiness dispatch gate would veto sec-0 otherwise.
    primary.handle_mesh_ready(DistributedMessage::MeshReady {
        target: None,
        sender_id: "sec-0".into(),
        timestamp: 0.0,
        secondary_id: "sec-0".into(),
        peer_count: 1,
    });

    let raise_latch = PhaseHookRaiseLatch::new();
    primary.set_phase_hook_raise_latch(raise_latch.clone());
    let hook_latch = raise_latch.clone();
    primary.register_phase_lifecycle_callbacks(
        Box::new(|_p| {}),
        Box::new(move |p, _c, _f, _outputs| {
            if raises && p.as_str() == "p1" {
                hook_latch.record("synthetic on_phase_end raise".to_string());
            }
        }),
    );

    // The p1 terminal: the cascade fires on_phase_end(p1) inline.
    primary
        .dispatch_message(
            DistributedMessage::TaskComplete {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 0.0,
                secondary_id: "sec-0".into(),
                worker_id: 0,
                task_hash: p1_hash,
                result_data: None,
                delivery_seq: Some(1),
                msgs_posted_through: None,
            },
            &mut None,
        )
        .await
        .unwrap();
    assert!(
        primary.slot_is_idle_for_test("sec-0", 0),
        "fixture: the p1 terminal freed the worker — the post-phase-end \
         dispatch surface is live"
    );
    (primary, mesh)
}

/// THE SMELL (run_20260611_005220's second half): after the
/// `on_phase_end` raise emitted the run-should-fail signal, the
/// production run STILL started the next phase and assigned 6 tasks
/// before the asynchronously-consumed signal took effect. The raise
/// must SYNCHRONOUSLY latch a dispatch freeze through the SAME
/// dispatch-view step-0 seam the graceful abort uses — by the time the
/// emit returns, EVERY dispatch path's view is empty, so no next-phase
/// assignment can escape the raise→abort window.
#[tokio::test(flavor = "current_thread")]
async fn on_phase_end_raise_synchronously_freezes_dispatch() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _mesh) = drive_phase_end_with_two_phase_ledger(true).await;
            let view = primary.dispatch_view_for_worker(0, false);
            assert!(
                view.is_empty(),
                "the on_phase_end raise must SYNCHRONOUSLY empty the \
                 dispatch view (step-0 freeze) — a non-empty view is the \
                 production window where 6 next-phase tasks were assigned \
                 after the raise"
            );
            // The full dispatch pass agrees: nothing is assigned.
            primary.dispatch_to_idle_workers(true).await.ok();
            assert!(
                primary.slot_is_idle_for_test("sec-0", 0),
                "no post-raise assignment may reach a worker"
            );
        })
        .await;
}

/// Control: a non-raising `on_phase_end` leaves dispatch unfrozen — the
/// dependent phase's work is visible to the very next dispatch view
/// (the freeze is gated strictly on the run-fail emit, not on the
/// phase-end edge itself).
#[tokio::test(flavor = "current_thread")]
async fn non_raising_phase_end_leaves_dispatch_live() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (primary, _mesh) = drive_phase_end_with_two_phase_ledger(false).await;
            let view = primary.dispatch_view_for_worker(0, false);
            assert!(
                !view.is_empty(),
                "without a raise the dependent phase's task must be \
                 dispatchable (freeze not latched spuriously)"
            );
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
                .run(SeedSource::PromotionSnapshot { kind: crate::process::BootstrapKind::Failover }, on_start, on_end)
                .await;

            let completed = primary.completed_count();
            let failed = primary.failed_count();
            // Verdict control (#313): the clean path latches the SUCCESS
            // terminal, never the failure twin — pins that the abort
            // broadcast is gated strictly on the fatal latch.
            let state = primary.cluster_state_for_test();
            assert!(
                state.run_aborted().is_none(),
                "a clean run must not broadcast RunAborted, got: {:?}",
                state.run_aborted()
            );
            assert!(
                state.run_complete(),
                "a clean run must latch RunComplete"
            );
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
