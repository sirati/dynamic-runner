//! #3b run-wide invalidation is a TERMINAL run abort (asm-dataset
//! run_20260611_112116): the duplicate-identity verdict must be latched
//! as the replicated `RunAborted { reason }` BEFORE the ledger is wiped,
//! and the phase machine must never re-derive "phase ended" from the
//! wiped ledger.
//!
//! Production trace replayed here: dep_graph's hook streamed 67097
//! tasks; ONE runtime spawn batch carried a duplicate identity →
//! `invalidate_all_pending` wiped every not-yet-terminal task — but
//! authored NO verdict ("cluster continues"). The phase-end barrier then
//! fired against the invalidated ledger, the consumer hook (correctly,
//! from its view) raised "handoff incomplete: … (spawned=0)", and THAT
//! raise became the `RunAborted` reason the secondaries read — a false
//! narrative that buried the true invalid-task verdict.
//!
//! Contract under fix:
//!   * the invalidation latches + broadcasts `RunAborted` with the
//!     duplicate-identity reason FIRST (before the `TaskFailed` wipe);
//!   * `process_phase_lifecycle` never fires `on_phase_end` once the
//!     run-terminal verdict is latched (hooks never see wiped state);
//!   * the verdict latch is first-writer-wins end-to-end — a later
//!     abort attempt (e.g. the finalize tail's worker-mgmt broadcast)
//!     never overwrites the reason (`apply` NoOp + wire filter);
//!   * the invalidation synchronously freezes dispatch (the same
//!     step-0 seam as the on_phase_end-raise emit).

use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Two-phase ledger fixture (mirrors `phase_end_raise.rs`): phase `p1`
/// holds one task IN FLIGHT on `(sec-0, 0)`; dependent phase `p2` holds
/// one Pending task. The `on_phase_end` hook RAISES via the wired latch
/// (the Rust analog of the consumer's "handoff incomplete" raise) and
/// counts its fires, so the test can assert the cascade never fires it
/// against an invalidated ledger.
async fn two_phase_primary_with_counting_raise_hook() -> (
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    PrimaryMeshKeepalive,
    Arc<AtomicUsize>,
    String,
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
            def_id: None,
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: p2_hash,
            task: p2_task,
            def_id: None,
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
    let fires = Arc::new(AtomicUsize::new(0));
    let fires_in_hook = Arc::clone(&fires);
    primary.register_phase_lifecycle_callbacks(
        Box::new(|_p| {}),
        Box::new(move |_p, _c, _f, _outputs| {
            fires_in_hook.fetch_add(1, Ordering::SeqCst);
            // The consumer-side view of an invalidated handoff: the
            // summary tally is gone, so the hook raises.
            hook_latch.record("handoff incomplete: no summary message received (spawned=0)".into());
        }),
    );

    (primary, mesh, fires, p1_hash)
}

/// Drive the #3b edge through the REAL command handler (a runtime
/// `SpawnTasks` whose batch carries the same identity twice).
async fn spawn_duplicate_batch(
    primary: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
) {
    let mut dup = make_binary("dup_task", 100);
    dup.phase_id = dynrunner_core::PhaseId::from("p1");
    dup.task_id = "dup_id".into();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    crate::primary::command_channel::handle_primary_command(
        primary,
        PrimaryCommand::SpawnTasks {
            tasks: vec![dup.clone(), dup],
            reply: reply_tx,
        },
        &mut None,
    )
    .await;
    let errors = reply_rx
        .await
        .expect("reply oneshot closed")
        .expect("spawn_tasks itself succeeds (per-task failures are not vec-level)");
    assert!(
        errors
            .iter()
            .any(|(_, e)| matches!(e, SpawnError::DuplicateInBatch(_))),
        "fixture: the batch must trip the #3b within-batch duplicate, got {errors:?}"
    );
}

/// THE REPLAY (reason conflation + post-wipe hook fire). The #3b
/// invalidation must latch the duplicate-identity `RunAborted` verdict
/// BEFORE wiping, the phase cascade must never fire `on_phase_end`
/// against the wiped ledger (no raise → no false reason), and a later
/// abort attempt must not overwrite the latched reason.
#[tokio::test(flavor = "current_thread")]
async fn invalidation_latches_verdict_first_and_suppresses_phase_hooks() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _mesh, fires, p1_hash) =
                two_phase_primary_with_counting_raise_hook().await;

            spawn_duplicate_batch(&mut primary).await;

            // (1) Verdict latched FIRST: the replicated reason is the
            // duplicate-identity verdict, present the moment the
            // invalidation returns — BEFORE any hook can run.
            let reason = primary
                .cluster_state_for_test()
                .run_aborted()
                .expect(
                    "the #3b invalidation must latch RunAborted{duplicate-identity} \
                     BEFORE wiping the ledger (the missing latch is the production \
                     reason-conflation defect)",
                )
                .to_string();
            assert!(
                reason.contains("duplicate task identity"),
                "the latched reason must name the duplicate-identity verdict, got: {reason}"
            );

            // (2) The racing in-flight terminal (a secondary's report
            // crossing the abort) drives the REAL cascade. With every
            // p1/p2 task invalidated, the pre-fix cascade fired
            // on_phase_end against the wiped ledger; under fix the
            // run-terminal gate suppresses it.
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
            assert_eq!(
                fires.load(Ordering::SeqCst),
                0,
                "on_phase_end must NEVER fire once the run-terminal verdict is \
                 latched — a post-invalidation fire runs the consumer hook against \
                 wiped state (the production 'spawned=0' raise)"
            );

            // (3) First-writer-wins end-to-end: the finalize tail's later
            // abort attempt (the worker-mgmt broadcast carrying a hook-raise
            // render) must not overwrite the latched verdict.
            primary
                .broadcast_terminal_verdict(crate::primary::lifecycle::TerminalVerdict::Aborted(
                    "on_phase_end hook for phase p1 raised: handoff incomplete: \
                     no summary message received (spawned=0)"
                        .into(),
                ))
                .await;
            assert_eq!(
                primary.cluster_state_for_test().run_aborted(),
                Some(reason.as_str()),
                "a second RunAborted must be a NoOp — the first (true) reason wins"
            );

            // (4) The invalidation synchronously froze dispatch: nothing is
            // assignable in the invalidation→break window (same step-0 seam
            // as the on_phase_end-raise emit).
            let view = primary.dispatch_view_for_worker(0, false);
            assert!(
                view.is_empty(),
                "the invalidation must synchronously empty the dispatch view \
                 (step-0 freeze) — a non-empty view is the post-abort assignment \
                 window"
            );
        })
        .await;
}
