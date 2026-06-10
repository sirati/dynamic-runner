//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;

use dynrunner_protocol_primary_secondary::DiscoveryDebt;

use crate::primary::wire::compute_task_hash;

// `fixed_discovery` lives in `tests::mod` (shared with the `stranded`
// Owed-seed collapse regression); picked up here via `use super::*`.

// ────────────────────────────────────────────────────────────────────────
// V6: originate_relocated_seed + discover_on_promotion + the DiscoveryDebt
// re-gating (run_complete_check / empty-phase cascade / process_phase_lifecycle).
// ────────────────────────────────────────────────────────────────────────

/// `originate_relocated_seed` stages ONLY the phase graph + the discovery-debt
/// marker (NO tasks), sets `all_binaries` empty, and ratchets
/// `discovery_debt` `Undeclared → Owed` on the LOCAL apply. The staged frames
/// are the post-connection broadcast (shipped by `broadcast_cold_seed`).
#[tokio::test(flavor = "current_thread")]
async fn originate_relocated_seed_declares_debt_and_seeds_phase_graph_only() {
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

            assert_eq!(
                primary.cluster_state_for_test().discovery_debt(),
                DiscoveryDebt::Undeclared,
                "a fresh CRDT is Undeclared (bottom)"
            );

            let mut deps = HashMap::new();
            deps.insert(
                dynrunner_core::PhaseId::from("ship"),
                vec![dynrunner_core::PhaseId::from("build")],
            );
            primary.originate_relocated_seed(deps.clone());

            // Local apply ratcheted to Owed; NO tasks seeded; phase graph set.
            assert_eq!(
                primary.cluster_state_for_test().discovery_debt(),
                DiscoveryDebt::Owed,
                "the relocated seed must declare debt (Undeclared → Owed)"
            );
            assert_eq!(
                primary.cluster_state_for_test().task_count(),
                0,
                "the relocated seed must seed NO tasks (discovery runs later)"
            );
            assert_eq!(
                primary.cluster_state_for_test().phase_deps(),
                &deps,
                "the relocated seed must replicate the phase graph"
            );
        })
        .await;
}

/// `discover_on_promotion` is a NO-OP when the CRDT is `Undeclared` (cold
/// mode-1 / legacy): the gate short-circuits and the registered policy is
/// NEVER consulted. The NO-REDO invariant at the driver level.
#[tokio::test(flavor = "current_thread")]
async fn discover_on_promotion_noop_when_undeclared() {
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
            let fires = std::rc::Rc::new(std::cell::Cell::new(0u32));
            primary.register_setup_discovery(fixed_discovery(
                vec![make_binary("x", 100)],
                HashMap::new(),
                fires.clone(),
            ));

            primary
                .discover_on_promotion()
                .await
                .expect("Undeclared → no-op Ok");

            assert_eq!(fires.get(), 0, "the policy must NOT run when Undeclared");
            assert_eq!(
                primary.cluster_state_for_test().task_count(),
                0,
                "no tasks seeded on the no-op path"
            );
            assert_eq!(
                primary.cluster_state_for_test().discovery_debt(),
                DiscoveryDebt::Undeclared,
                "the marker stays Undeclared on the no-op path"
            );
        })
        .await;
}

/// `discover_on_promotion` is a NO-OP when the CRDT is already `Settled` (a
/// re-promotion AFTER a prior origination completed). The NO-REDO invariant:
/// a populated-CRDT promoted primary does NOT re-discover.
#[tokio::test(flavor = "current_thread")]
async fn discover_on_promotion_noop_when_settled() {
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
            // A prior origination already settled (Owed then Settled).
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::DiscoveryDebtDeclared);
                cs.apply(ClusterMutation::DiscoverySettled);
            }
            let fires = std::rc::Rc::new(std::cell::Cell::new(0u32));
            primary.register_setup_discovery(fixed_discovery(
                vec![make_binary("x", 100)],
                HashMap::new(),
                fires.clone(),
            ));

            primary
                .discover_on_promotion()
                .await
                .expect("Settled → no-op Ok");

            assert_eq!(fires.get(), 0, "the policy must NOT run when Settled");
            assert_eq!(
                primary.cluster_state_for_test().task_count(),
                0,
                "no tasks seeded on the no-op path (no re-discovery)"
            );
        })
        .await;
}

/// `discover_on_promotion` on `Owed` + a NON-empty corpus: runs the policy
/// ONCE, originates `PhaseDepsSet` + one `TaskAdded` per binary +
/// `DiscoverySettled` (ratcheting `Owed → Settled`), and hydrates the pool.
/// No `RunComplete` on the non-empty arm.
#[tokio::test(flavor = "current_thread")]
async fn discover_on_promotion_owed_nonempty_seeds_tasks_and_settles() {
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
            primary.cluster_state_mut_for_test().apply(ClusterMutation::DiscoveryDebtDeclared);

            let t1 = make_binary("disc-1", 100);
            let t2 = make_binary("disc-2", 100);
            let h1 = compute_task_hash(&t1);
            let h2 = compute_task_hash(&t2);
            let fires = std::rc::Rc::new(std::cell::Cell::new(0u32));
            primary.register_setup_discovery(fixed_discovery(
                vec![t1, t2],
                HashMap::new(),
                fires.clone(),
            ));

            primary
                .discover_on_promotion()
                .await
                .expect("Owed + non-empty → Ok");

            assert_eq!(fires.get(), 1, "the policy runs EXACTLY once");
            assert_eq!(
                primary.cluster_state_for_test().discovery_debt(),
                DiscoveryDebt::Settled,
                "discovery must settle (Owed → Settled) atomically with the seed"
            );
            assert_eq!(
                primary.cluster_state_for_test().task_count(),
                2,
                "both discovered tasks must be seeded as TaskAdded"
            );
            assert!(
                primary.cluster_state_for_test().task_state(&h1).is_some()
                    && primary.cluster_state_for_test().task_state(&h2).is_some(),
                "both discovered hashes must be present in the ledger"
            );
            assert!(
                !primary.cluster_state_for_test().run_complete(),
                "the NON-empty arm must NOT originate RunComplete"
            );
            // hydrate built the pool from the seeded tasks.
            assert_eq!(
                primary.total_tasks, 2,
                "hydrate must set total_tasks from the seeded ledger"
            );
        })
        .await;
}

/// `discover_on_promotion` on `Owed` + an EMPTY corpus: originates
/// `DiscoverySettled` and NO run-terminal. POST-FIX the empty corpus finalizes
/// through the SAME counter machinery the all-skipped / mode-1 paths use — the
/// seam settles the debt + re-hydrates (`total_tasks == 0`, `completed` empty),
/// and the operational loop's `0 + 0 >= 0` counter exit fires once the debt is
/// no longer `Owed`. No single-phase-view `RunComplete` is originated, so a
/// phase-chaining consumer that injects its real first phase via `on_phase_end`
/// is not contradicted.
#[tokio::test(flavor = "current_thread")]
async fn discover_on_promotion_owed_empty_settles_and_counter_finalizes() {
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
            primary.cluster_state_mut_for_test().apply(ClusterMutation::DiscoveryDebtDeclared);

            let fires = std::rc::Rc::new(std::cell::Cell::new(0u32));
            primary.register_setup_discovery(fixed_discovery(
                Vec::new(),
                HashMap::new(),
                fires.clone(),
            ));

            primary
                .discover_on_promotion()
                .await
                .expect("Owed + empty → Ok");

            assert_eq!(fires.get(), 1, "the policy runs exactly once");
            assert_eq!(
                primary.cluster_state_for_test().discovery_debt(),
                DiscoveryDebt::Settled,
                "empty discovery must still settle the debt"
            );
            assert_eq!(
                primary.cluster_state_for_test().task_count(),
                0,
                "an empty corpus seeds no tasks"
            );
            // POST-FIX: NO explicit RunComplete — the empty corpus finalizes via
            // the counter, identical to the all-skipped / mode-1 paths.
            assert!(
                !primary.cluster_state_for_test().run_complete(),
                "the empty arm must NOT originate a single-phase-view RunComplete; \
                 it finalizes through the counter exit"
            );
            assert_eq!(
                primary.total_tasks, 0,
                "hydrate sets total_tasks from the (empty) seeded ledger"
            );
            // ANTI-HANG GUARD: with the debt now Settled the empty-corpus
            // counter exit (`0 + 0 >= 0`, no active workers) fires.
            assert!(
                primary.run_complete_check(),
                "once the empty discovery settles, the zero-task counter exit fires \
                 — deleting the explicit RunComplete does not strand the empty corpus"
            );
        })
        .await;
}

/// R5 end-to-end marker thread (discovery seam): a discovered item carrying
/// the `skipped_already_done` marker must land in the ledger as a TERMINAL
/// `SkippedAlreadyDone`, while an UNMARKED sibling lands `Pending`. This is
/// the plumbing-e2e guard — per-layer unit tests can each pass while one
/// layer silently drops the bit, so this asserts the bit survives the WHOLE
/// thread from the discovery batch through `skip_transitions` into the ledger
/// state.
#[tokio::test(flavor = "current_thread")]
async fn discover_on_promotion_marked_item_lands_skipped_unmarked_lands_pending() {
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
            primary.cluster_state_mut_for_test().apply(ClusterMutation::DiscoveryDebtDeclared);

            let skipped = make_binary("already-done", 100);
            let to_run = make_binary("needs-run", 100);
            let skipped_hash = compute_task_hash(&skipped);
            let to_run_hash = compute_task_hash(&to_run);
            let fires = std::rc::Rc::new(std::cell::Cell::new(0u32));
            // The marked item rides through the SetupDiscoveryFn as
            // `(task, true)` — exactly the shape `extract_binaries` yields
            // for a Python item with `skipped_already_done=True`.
            primary.register_setup_discovery(fixed_discovery_marked(
                vec![(skipped, true), (to_run, false)],
                HashMap::new(),
                fires.clone(),
            ));

            primary
                .discover_on_promotion()
                .await
                .expect("Owed + non-empty (mixed marked) → Ok");

            // Both items are real tasks of the phase — task_count counts them.
            assert_eq!(
                primary.cluster_state_for_test().task_count(),
                2,
                "EVERY discovered item is seeded (the marked one is a real \
                 task with a terminal state, not dropped)"
            );
            // The marked item is TERMINAL SkippedAlreadyDone — never dispatched.
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&skipped_hash),
                    Some(crate::cluster_state::TaskState::SkippedAlreadyDone { .. })
                ),
                "the marked item must land SkippedAlreadyDone (NOT Pending / \
                 dispatched); got {:?}",
                primary.cluster_state_for_test().task_state(&skipped_hash)
            );
            // The unmarked sibling is a normal Pending task.
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&to_run_hash),
                    Some(crate::cluster_state::TaskState::Pending { .. })
                ),
                "the unmarked item must land Pending; got {:?}",
                primary.cluster_state_for_test().task_state(&to_run_hash)
            );
            // One real to-run task remains, so the run is NOT complete.
            assert!(
                !primary.cluster_state_for_test().run_complete(),
                "a corpus with ANY to-run work must NOT originate RunComplete"
            );
        })
        .await;
}

/// `discover_on_promotion` on `Owed` + a 100%-already-done corpus: every item
/// is marked skipped. POST-FIX the seam originates NO run-terminal — it
/// finalizes through the SAME counter machinery mode-1 uses. The skips land as
/// terminal `SkippedAlreadyDone` ledger entries, debt Settles, and the seam's
/// own trailing `hydrate_from_cluster_state` projects every skip into
/// `completed_tasks` (with `total_tasks` from the ledger) so the operational
/// loop's `completed + failed >= total_tasks` counter exit can finalize the
/// run WITHOUT a single-phase-view `RunComplete` that a phase-chaining
/// consumer's later `on_phase_end` injection would contradict.
///
/// The `run_complete_check()` assertion at the tail is the ANTI-HANG guard: it
/// proves the deleted explicit `RunComplete` is not load-bearing — the counter
/// genuinely accounts for the post-hydrate skips, so removing the shortcut
/// trades the premature complete for a clean counter finalize, not a hang.
#[tokio::test(flavor = "current_thread")]
async fn discover_on_promotion_all_skipped_settles_and_counter_finalizes() {
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
            primary.cluster_state_mut_for_test().apply(ClusterMutation::DiscoveryDebtDeclared);

            let s1 = make_binary("done-1", 100);
            let s2 = make_binary("done-2", 100);
            let h1 = compute_task_hash(&s1);
            let h2 = compute_task_hash(&s2);
            let fires = std::rc::Rc::new(std::cell::Cell::new(0u32));
            primary.register_setup_discovery(fixed_discovery_marked(
                vec![(s1, true), (s2, true)],
                HashMap::new(),
                fires.clone(),
            ));

            primary
                .discover_on_promotion()
                .await
                .expect("Owed + all-skipped → Ok");

            assert_eq!(
                primary.cluster_state_for_test().discovery_debt(),
                DiscoveryDebt::Settled,
                "discovery must settle even when every item is skipped"
            );
            // Both items are seeded as real tasks, both terminal.
            assert_eq!(
                primary.cluster_state_for_test().task_count(),
                2,
                "all-skipped corpus still seeds every item as a real task"
            );
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&h1),
                    Some(crate::cluster_state::TaskState::SkippedAlreadyDone { .. })
                ) && matches!(
                    primary.cluster_state_for_test().task_state(&h2),
                    Some(crate::cluster_state::TaskState::SkippedAlreadyDone { .. })
                ),
                "every item in an all-skipped corpus is terminal SkippedAlreadyDone"
            );
            // POST-FIX: the seam originates NO `RunComplete` of its own — a
            // single-phase discovery view must NOT decide the run is terminal.
            assert!(
                !primary.cluster_state_for_test().run_complete(),
                "the seam must NOT originate a single-phase-view RunComplete; the \
                 all-skipped corpus finalizes through the counter exit, like mode-1"
            );
            // The seam's trailing hydrate projected BOTH skips into the
            // completed set (the part that closes the counter gap the deleted
            // explicit RunComplete used to cover).
            assert!(
                primary.completed_tasks.contains(&h1)
                    && primary.completed_tasks.contains(&h2),
                "hydrate must project every SkippedAlreadyDone into completed_tasks \
                 so the counter exit accounts for them; completed_tasks = {:?}",
                primary.completed_tasks
            );
            assert_eq!(
                primary.total_tasks, 2,
                "hydrate sets total_tasks from the seeded ledger"
            );
            // ANTI-HANG GUARD: the counter exit fires (`2 + 0 >= 2` with no
            // active workers and debt Settled). Without the post-hydrate skip
            // projection this would be false → the run would hang, trading a
            // premature complete for a deadlock.
            assert!(
                primary.run_complete_check(),
                "an all-skipped mode-2 corpus must finalize via the counter exit — \
                 the post-hydrate skip projection makes `completed >= total` true, \
                 so deleting the explicit RunComplete does NOT introduce a hang"
            );
        })
        .await;
}

/// PREMATURE-COMPLETE REGRESSION (the consumer-reproduced bug): a phase-chaining
/// mode-2 corpus where discovery returns ZERO to-run items but a DEPENDENT phase
/// is declared in `phase_deps`. The deleted `to_run == 0 → RunComplete` shortcut
/// would have originated a STICKY `RunComplete` from this single-phase discovery
/// view — wrong, because the consumer injects the dependent phase's real work
/// later via `on_phase_end`, and the sticky latch made the observer exit
/// ("run complete: 0 succeeded") while secondaries still worked and the cascade
/// ran the next phase in an already-"complete" state.
///
/// POST-FIX the seam originates NO `RunComplete`: zero to-run items at discovery
/// time is NOT a run-terminal when a later phase is still pending injection. The
/// dependent phase here (`phase2 → phase1`) models that later-injected work — at
/// the discovery seam it is an empty declared phase, NOT a completion signal.
#[tokio::test(flavor = "current_thread")]
async fn discover_on_promotion_zero_to_run_with_dependent_phase_no_run_complete() {
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
            primary.cluster_state_mut_for_test().apply(ClusterMutation::DiscoveryDebtDeclared);

            // The phase-chaining shape: `phase2` depends on `phase1`. Discovery
            // resolves only `phase1` (all-skipped, zero to-run); `phase2`'s real
            // work is the consumer's later `on_phase_end` injection. At the
            // discovery seam `phase2` is an empty declared phase.
            let mut deps = HashMap::new();
            deps.insert(
                dynrunner_core::PhaseId::from("phase2"),
                vec![dynrunner_core::PhaseId::from("phase1")],
            );

            let mut s1 = make_binary("p1-done-1", 100);
            s1.phase_id = dynrunner_core::PhaseId::from("phase1");
            let mut s2 = make_binary("p1-done-2", 100);
            s2.phase_id = dynrunner_core::PhaseId::from("phase1");

            let fires = std::rc::Rc::new(std::cell::Cell::new(0u32));
            primary.register_setup_discovery(fixed_discovery_marked(
                vec![(s1, true), (s2, true)],
                deps,
                fires.clone(),
            ));

            primary
                .discover_on_promotion()
                .await
                .expect("Owed + zero-to-run + dependent phase → Ok");

            assert_eq!(
                primary.cluster_state_for_test().discovery_debt(),
                DiscoveryDebt::Settled,
                "discovery settles even with zero to-run items"
            );
            // THE REGRESSION ASSERTION: no single-phase-view RunComplete.
            assert!(
                !primary.cluster_state_for_test().run_complete(),
                "zero to-run items at discovery time MUST NOT originate RunComplete \
                 when a dependent phase is declared — the consumer injects its real \
                 work via on_phase_end and the sticky latch would prematurely \
                 complete the run (the consumer-reproduced bug)"
            );
            // The seam grew NO `RunComplete` mutation in the seed batch — the
            // ledger flag is the load-bearing observable, and it is unset.
            assert!(
                !primary.cluster_state_for_test().run_complete(),
                "the discovery seam must not grow a RunComplete latch in any form"
            );
        })
        .await;
}

/// R5 end-to-end marker thread (cold-seed seam): the same marker contract on
/// the `originate_cold_seed` path. A marked item lands terminal
/// `SkippedAlreadyDone`; an unmarked sibling lands `Pending`. The cold-seed
/// path needs NO explicit RunComplete (hydrate seeds the skip into
/// `completed_tasks`, so the operational loop's counter exit trips) — this
/// test pins the per-task ledger landing, the part the seed seam owns.
#[test]
fn cold_seed_marked_item_lands_skipped_unmarked_lands_pending() {
    let (transport, _ends) = setup_test(1);
    let (mut primary, _mesh) = build_test_primary(
        test_primary_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    let skipped = make_binary("cs-already-done", 100);
    let to_run = make_binary("cs-needs-run", 100);
    let skipped_hash = compute_task_hash(&skipped);
    let to_run_hash = compute_task_hash(&to_run);

    primary
        .originate_cold_seed(vec![(skipped, true), (to_run, false)], HashMap::new())
        .expect("mixed marked cold seed");
    primary.hydrate_from_cluster_state();

    assert_eq!(
        primary.cluster_state_for_test().task_count(),
        2,
        "every cold-seeded item is a real task (marked or not)"
    );
    assert!(
        matches!(
            primary.cluster_state_for_test().task_state(&skipped_hash),
            Some(crate::cluster_state::TaskState::SkippedAlreadyDone { .. })
        ),
        "the marked cold-seed item must land SkippedAlreadyDone; got {:?}",
        primary.cluster_state_for_test().task_state(&skipped_hash)
    );
    assert!(
        matches!(
            primary.cluster_state_for_test().task_state(&to_run_hash),
            Some(crate::cluster_state::TaskState::Pending { .. })
        ),
        "the unmarked cold-seed item must land Pending; got {:?}",
        primary.cluster_state_for_test().task_state(&to_run_hash)
    );
    // hydrate seeds the skip into the completed projection, so the
    // counter-exit denominator accounts for it (no explicit RunComplete on
    // this path).
    assert!(
        primary.completed_tasks.contains(&skipped_hash),
        "the skipped hash must be in the completed projection so the \
         operational loop's counter exit accounts for it"
    );
}

/// `discover_on_promotion` on `Owed` with NO policy registered is a hard
/// `RunError` — a primary that owes discovery MUST carry the policy; silently
/// stranding would never exit (the counter arm is gated on `Owed`).
#[tokio::test(flavor = "current_thread")]
async fn discover_on_promotion_owed_without_policy_is_hard_error() {
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
            primary.cluster_state_mut_for_test().apply(ClusterMutation::DiscoveryDebtDeclared);
            // No register_setup_discovery.

            let r = primary.discover_on_promotion().await;
            assert!(
                matches!(r, Err(crate::primary::RunError::Other(_))),
                "Owed but no policy must hard-fail, not silently strand; got {r:?}"
            );
        })
        .await;
}

// `discover_on_promotion_noop_on_relocating_submitter` is DELETED: its premise
// — a setup peer that REACHES `discover_on_promotion` while owing debt — no
// longer exists. Under mesh-always the setup peer relocates in
// `run_pipeline`'s `BootstrapRole::SetupPeer` arm BEFORE `discover_on_promotion`,
// so a policyless owing setup peer never reaches the driver (the relocate
// TARGET, a `PromotionSnapshot`, does the discovery). The real end-to-end
// pre-staged-relocate behaviour is covered by
// `relocated_seed_setup_peer_relocates_and_target_discovers_and_completes`
// below (a TRUE `Node::run` relocate: the target discovers + completes).

/// `run_complete_check` gated on `discovery_debt() == Owed` (V6): a zero-task
/// CRDT that declares debt must NOT trip the counter exit (`0+0 >= 0`) — the
/// driver hasn't seeded yet. After `DiscoverySettled` lands, the same
/// zero-task CRDT trips the exit (an empty corpus legitimately completes).
#[test]
fn run_complete_check_gated_on_discovery_owed() {
    let (transport, _ends) = setup_test(1);
    let (mut primary, _mesh) = build_test_primary(
        test_primary_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    // Empty pool, zero tasks, no active workers: the counter arm would
    // otherwise trip `0+0 >= 0 && active==0`.
    let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
        [dynrunner_core::PhaseId::from("default")],
        HashMap::new(),
    )
    .expect("default-phase pool");
    primary.pending = Some(pool);
    primary.total_tasks = 0;

    // Declare debt → the exit must be SUPPRESSED.
    primary
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::DiscoveryDebtDeclared);
    assert!(
        !primary.run_complete_check(),
        "while discovery is Owed the counter/pool-drain exits must be suppressed"
    );

    // Settle → the empty-corpus run legitimately completes via the counter.
    primary
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::DiscoverySettled);
    assert!(
        primary.run_complete_check(),
        "once discovery Settles, the zero-task counter exit fires (empty corpus done)"
    );
}

/// Ported `setup_pending_suppresses_initial_phase_cascade_until_task_added`
/// (re-expressed on the V6 marker): while `DiscoveryDebtDeclared` holds the
/// CRDT `Owed`, `process_phase_lifecycle` is a defence-in-depth NO-OP — no
/// `on_phase_end` fires for the transiently-empty declared phases. After the
/// driver's seed batch (incl. `DiscoverySettled`) lands, the cascade resumes
/// and narrates the seeded phases. Drives the marker, not
/// `required_setup_on_promote` + a bare `TaskAdded`.
#[tokio::test(flavor = "current_thread")]
async fn discovery_owed_suppresses_phase_cascade_until_settled() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use std::sync::Arc;
            use std::sync::atomic::{AtomicU32, Ordering};
            // `OnPhaseEnd` is `+ Send`, so the counter must be `Send` — Arc/atomic.
            let phase_end_fires = Arc::new(AtomicU32::new(0));
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let fires_for_cb = phase_end_fires.clone();
            primary.register_phase_lifecycle_callbacks(
                Box::new(|_| {}),
                Box::new(move |_, _, _, _| {
                    fires_for_cb.fetch_add(1, Ordering::SeqCst);
                }),
            );

            // A declared-but-empty phase graph + Owed: the relocated-seed
            // shape (PhaseDepsSet + DiscoveryDebtDeclared, no tasks).
            let mut deps = HashMap::new();
            deps.insert(dynrunner_core::PhaseId::from("build"), vec![]);
            primary.originate_relocated_seed(deps);
            primary.hydrate_from_cluster_state();

            // While Owed, the cascade is a defence-in-depth no-op: NO
            // on_phase_end fires for the transiently-empty "build" phase.
            let mut rx = None;
            primary.process_phase_lifecycle(&mut rx).await;
            assert_eq!(
                phase_end_fires.load(Ordering::SeqCst),
                0,
                "while discovery is Owed no on_phase_end may fire (the phases \
                 are only transiently-empty, awaiting the discovery seed)"
            );

            // The driver's seed batch lands (the discovered tasks for "build"
            // + DiscoverySettled): debt flips to Settled.
            let fires2 = std::rc::Rc::new(std::cell::Cell::new(0u32));
            let mut t = make_binary("build-1", 100);
            t.phase_id = dynrunner_core::PhaseId::from("build");
            primary.register_setup_discovery(fixed_discovery(
                vec![t],
                {
                    let mut d = HashMap::new();
                    d.insert(dynrunner_core::PhaseId::from("build"), vec![]);
                    d
                },
                fires2.clone(),
            ));
            primary
                .discover_on_promotion()
                .await
                .expect("seed batch incl. DiscoverySettled");
            assert_eq!(
                primary.cluster_state_for_test().discovery_debt(),
                DiscoveryDebt::Settled,
                "the driver's seed batch settles the debt"
            );
            // Now the gate is open — the cascade resumes normal operation
            // (the "build" phase holds a real Pending task, so it does not
            // false-drain; no spurious empty on_phase_end).
            primary.process_phase_lifecycle(&mut rx).await;
            assert_eq!(
                phase_end_fires.load(Ordering::SeqCst),
                0,
                "a phase holding real discovered work must NOT fire a spurious \
                 empty on_phase_end after settle"
            );
        })
        .await;
}

/// Pre-seeded bootstrap exit semantics: the counter-based exit at the top
/// of `operational_loop` fires immediately when
/// `completed + failed >= total_tasks && active_workers == 0`. Pins the
/// cold path where `seed_cluster_state` ran locally and `total_tasks` was
/// non-zero at startup.
#[tokio::test(flavor = "current_thread")]
async fn pre_seeded_counter_exit_unchanged() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(1);
            let (_sec_id, _to_sec_rx, _incoming_tx) = secondary_ends.into_iter().next().unwrap();

            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                keepalive_interval: Duration::from_millis(50),
                // Pre-seeded bootstrap: `seed_cluster_state` ran locally, so
                // `total_tasks` is set by `run()` from `binaries.len()`
                // and the counter-based exit must fire on the very first
                // iteration once completions cover the total.
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
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
            primary.total_tasks = 2;
            primary.completed_tasks.insert("h-legacy-1".into());
            primary.completed_tasks.insert("h-legacy-2".into());

            // Bounded wait. The counter-check exit should fire on
            // iteration 1 of the loop — well under 1s. A 5s ceiling is
            // overkill but stays consistent with the other operational-
            // loop tests.
            let exit = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                primary.operational_loop(),
            )
            .await;

            match exit {
                Ok(Ok(())) => {
                    // Exit path pinning: the pre-seeded counter-based exit
                    // fired, not the `cluster_state.run_complete()` branch.
                    assert!(
                        !primary.cluster_state_for_test().run_complete(),
                        "pre-seeded bootstrap exit must be via the counter check, \
                     not via the cluster_state.run_complete() branch"
                    );
                }
                Ok(Err(e)) => {
                    panic!("operational_loop returned Err in pre-seeded bootstrap scenario: {e}")
                }
                Err(_) => panic!(
                    "pre-seeded bootstrap operational_loop did not exit within 5s \
                 despite the counter check `2+0 >= 2 && active_workers == 0` \
                 being satisfied on the first iteration — regression on the \
                 historical exit semantics"
                ),
            }
        })
        .await;
}

// ────────────────────────────────────────────────────────────────────────
// Bootstrap-relocation: select_relocation_target + relocate_primary_to +
// the SeedSource-keyed bootstrap role (mesh-always — the setup peer ALWAYS
// relocates the primary onto a compute peer; the promoted destination runs
// the operational loop in place).
// ────────────────────────────────────────────────────────────────────────

/// One advertised-memory `ResourceAmount` vec (the live welcome shape).
fn relocate_mem(bytes: u64) -> Vec<dynrunner_core::ResourceAmount> {
    vec![dynrunner_core::ResourceAmount {
        kind: dynrunner_core::ResourceKind::memory(),
        amount: bytes,
    }]
}

/// Seed a cluster member into the primary's `cluster_state`: an Alive
/// `PeerJoined { is_observer, can_be_primary }` plus a `SecondaryCapacity {
/// worker_count }`. A peer is eligible for relocation iff it is alive AND has
/// worker_count > 0 AND `can_be_primary` AND is NOT an observer — exactly what
/// `select_relocation_target` filters on (observers carry worker_count == 0
/// structurally, so they are excluded by both the worker filter and the
/// explicit observers filter).
fn seed_member<S, E>(
    primary: &mut crate::primary::PrimaryCoordinator<S, E, TestId>,
    id: &str,
    worker_count: u32,
    is_observer: bool,
    can_be_primary: bool,
) where
    S: dynrunner_scheduler_api::Scheduler<TestId>,
    E: dynrunner_scheduler_api::ResourceEstimator<TestId>,
{
    let cs = primary.cluster_state_mut_for_test();
    cs.apply(ClusterMutation::PeerJoined {
        peer_id: id.into(),
        is_observer,
        can_be_primary,
        cap_version: Default::default(),
        member_gen: 0,
    });
    cs.apply(ClusterMutation::SecondaryCapacity {
        secondary: id.into(),
        worker_count,
        resources: relocate_mem(8 * 1024 * 1024 * 1024),
    });
}

/// Selection picks the LOWEST-id member of `alive ∩ can_be_primary −
/// observers`. Seed: sec-0 (observer → excluded), sec-1 (eligible), sec-2
/// (eligible, higher id), sec-3 (can_be_primary=false → excluded). The min
/// eligible is `sec-1` (NOT sec-0, which is an observer despite being
/// lowest-id).
#[tokio::test(flavor = "current_thread")]
async fn select_relocation_target_picks_lowest_eligible_compute_peer() {
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
            // sec-0: lowest id but an OBSERVER → excluded.
            seed_member(&mut primary, "sec-0", 0, true, false);
            // sec-1: eligible (alive, workers, can_be_primary, not observer).
            seed_member(&mut primary, "sec-1", 2, false, true);
            // sec-2: eligible but a higher id than sec-1.
            seed_member(&mut primary, "sec-2", 2, false, true);
            // sec-3: workers but can_be_primary=false → excluded.
            seed_member(&mut primary, "sec-3", 2, false, false);

            assert_eq!(
                primary.select_relocation_target(crate::primary::lifecycle::RelocationPolicy::LowestId).as_deref(),
                Some("sec-1"),
                "must pick the LOWEST-id eligible compute peer — sec-0 is an \
                 observer (excluded), sec-3 lacks can_be_primary (excluded), so \
                 sec-1 (< sec-2) wins"
            );
        })
        .await;
}

/// No eligible compute peer → `None` (the `SetupPeer` bootstrap arm maps this
/// to a hard `NoRelocationTarget` error). Seed only an observer and a
/// non-can_be_primary worker — neither is promotable.
#[tokio::test(flavor = "current_thread")]
async fn select_relocation_target_none_when_no_eligible_peer() {
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
            seed_member(&mut primary, "obs-0", 0, true, false);
            seed_member(&mut primary, "sec-0", 2, false, false);
            assert_eq!(
                primary.select_relocation_target(crate::primary::lifecycle::RelocationPolicy::LowestId),
                None,
                "an observer + a can_be_primary=false worker leave NO promotable \
                 compute peer; selection must be None so the bootstrap errors \
                 rather than silently staying setup-primary"
            );
        })
        .await;
}

/// Self is excluded from the candidate set even when it advertises a
/// worker-secondary capability under its own id with `can_be_primary`. Seed
/// the primary's own id as an eligible-looking member and one OTHER eligible
/// peer; selection must skip self and pick the other peer.
#[tokio::test(flavor = "current_thread")]
async fn select_relocation_target_excludes_self() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let config = test_primary_config();
            let own_id = config.node_id.clone();
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // The submitter's own id advertised (defensively) as a
            // worker-secondary that would otherwise be eligible.
            seed_member(&mut primary, &own_id, 2, false, true);
            seed_member(&mut primary, "sec-9", 2, false, true);
            assert_eq!(
                primary.select_relocation_target(crate::primary::lifecycle::RelocationPolicy::LowestId).as_deref(),
                Some("sec-9"),
                "selection must exclude this primary's OWN id even when it \
                 advertises an eligible-looking worker-secondary capability — \
                 the submitter must never relocate the role to itself"
            );
        })
        .await;
}

/// `relocate_primary_to` originates `PrimaryChanged { new=chosen,
/// reason=Transferred, epoch=primary_epoch()+1 }`, advances the LOCAL
/// `current_primary` to the chosen peer, and does NOT set `primary_id` to
/// self (this host is stepping DOWN, not asserting authority). The broadcast
/// frame reaches the connected secondary verbatim.
#[tokio::test(flavor = "current_thread")]
async fn relocate_primary_to_originates_transferred_to_chosen_not_self() {
    use dynrunner_protocol_primary_secondary::PrimaryChangeReason;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(1);
            let (_sec_id, mut to_sec_rx, _incoming_tx) =
                secondary_ends.into_iter().next().unwrap();
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // The chosen peer is the lowest-id eligible compute peer.
            seed_member(&mut primary, "sec-0", 2, false, true);
            let chosen = primary
                .select_relocation_target(crate::primary::lifecycle::RelocationPolicy::LowestId)
                .expect("an eligible compute peer was seeded");
            assert_eq!(chosen, "sec-0");

            let epoch_before = primary.cluster_state_for_test().primary_epoch();
            primary.relocate_primary_to(chosen.clone()).await;
            // Let the mesh-pump drain the egress queue onto the wire so the
            // broadcast frame lands on the secondary's inbound channel.
            settle_pump().await;

            // (1) LOCAL apply named the CHOSEN peer the primary (not self).
            assert_eq!(
                primary.cluster_state_for_test().current_primary(),
                Some("sec-0"),
                "the local apply of the Transferred PrimaryChanged must advance \
                 current_primary to the chosen peer"
            );
            // (2) `primary_id` is NOT set to self — this host is stepping down.
            assert_ne!(
                primary.primary_id.as_deref(),
                Some(primary.config.node_id.as_str()),
                "relocate must NOT set primary_id=self; the setup is handing the \
                 role away, not asserting it (that is activate_local_primary's job)"
            );
            assert_eq!(
                primary.primary_id, None,
                "relocate leaves primary_id unset — only activate_local_primary \
                 (the stay-local arm) sets it to self"
            );

            // (3) The broadcast frame is the Transferred PrimaryChanged at
            // epoch+1, naming the chosen peer.
            let mut saw_transfer = false;
            while let Ok(msg) = to_sec_rx.try_recv() {
                if let DistributedMessage::ClusterMutation { mutations, .. } = msg {
                    for m in mutations {
                        if let ClusterMutation::PrimaryChanged { new, epoch, reason } = m {
                            assert_eq!(new, "sec-0", "PrimaryChanged must name the chosen peer");
                            assert_eq!(
                                epoch,
                                epoch_before + 1,
                                "relocate epoch must be primary_epoch()+1 (strictly supersede)"
                            );
                            assert!(
                                matches!(reason, PrimaryChangeReason::Transferred),
                                "the relocate reason must be Transferred, not Election"
                            );
                            saw_transfer = true;
                        }
                    }
                }
            }
            assert!(
                saw_transfer,
                "relocate_primary_to must broadcast a PrimaryChanged{{Transferred}} \
                 frame to the connected fleet"
            );
        })
        .await;
}

/// EPOCH SINGLE-BUMP (relocate→promotion is ONE transition): when the chosen
/// peer promotes, its `seed_from_promotion_snapshot` has ALREADY restored
/// `current_primary = self` at the epoch the submitter's relocate committed
/// (E). `originate_primary_changed` (via `activate_local_primary`) must then
/// RE-ASSERT at E — NOT bump to E+1 — so the cluster sees exactly one epoch
/// transition for the holder, never a double-announce that would pin a
/// disconnected submitter-observer at E while the cluster ran at E+1.
///
/// Drive it at the unit level: seed `current_primary = self` at a non-zero
/// epoch E (the post-restore state of a relocate target), then call
/// `originate_primary_changed` and assert the epoch is UNCHANGED. The
/// complementary genuine-first-assertion case (no prior `current_primary` ⇒
/// bump to 1) is covered by `bootstrap_tail_activates_local_primary`.
#[tokio::test(flavor = "current_thread")]
async fn promote_reasserts_at_inherited_epoch_single_bump() {
    use dynrunner_protocol_primary_secondary::PrimaryChangeReason;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let config = test_primary_config();
            let own_id = config.node_id.clone();
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Simulate `seed_from_promotion_snapshot`'s restored state: the
            // upstream relocate already named THIS host the primary at epoch E.
            const E: u64 = 7;
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::PrimaryChanged {
                    new: own_id.clone(),
                    epoch: E,
                    reason: PrimaryChangeReason::Transferred,
                });
            assert_eq!(
                primary.cluster_state_for_test().primary_epoch(),
                E,
                "precondition: the restore committed epoch E naming self",
            );
            assert_eq!(
                primary.cluster_state_for_test().current_primary(),
                Some(own_id.as_str()),
                "precondition: the restore named self the primary",
            );

            // The promoted primary's self-announce: it must RE-ASSERT at E,
            // not bump to E+1 (the double-announce SMELL the fix closes).
            primary.originate_primary_changed().await;

            assert_eq!(
                primary.cluster_state_for_test().primary_epoch(),
                E,
                "SINGLE bump: a promoted primary whose snapshot already names it \
                 must re-assert at the inherited epoch E, NOT bump to E+1 — the \
                 relocate→promotion is one epoch transition, not two",
            );
            assert_eq!(
                primary.cluster_state_for_test().current_primary(),
                Some(own_id.as_str()),
                "the re-assert keeps this host the primary",
            );
        })
        .await;
}

/// Race tolerance (F6): a concurrent failover election at the SAME `epoch+1`
/// can win the equal-epoch lex tiebreak against this primary's
/// `relocate_primary_to { chosen }`, so the converged `current_primary` is the
/// lex-LOWER winner, NOT `chosen` — `relocate_primary_to` must NOT assert its
/// target won. Both originations independently picked `primary_epoch()+1` from
/// the SAME starting epoch (the concurrency), so they collide at one epoch and
/// the lex tiebreak decides. Drive it order-independently: relocate toward the
/// lex-HIGHER `sec-9` (it originates + applies at epoch E = primary_epoch()+1),
/// then apply the concurrent election naming the lex-LOWER `sec-0` at that SAME
/// epoch E. The CRDT register-adopt rule (`primary_register_adopt`,
/// equal-epoch → lex-lower wins) converges on `sec-0` regardless of which apply
/// lands first.
#[tokio::test(flavor = "current_thread")]
async fn relocate_primary_to_tolerates_concurrent_lex_lower_winner() {
    use dynrunner_protocol_primary_secondary::PrimaryChangeReason;

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
            seed_member(&mut primary, "sec-0", 2, false, true);
            seed_member(&mut primary, "sec-9", 2, false, true);

            // The relocate originates at E = primary_epoch()+1 and names the
            // lex-HIGHER sec-9. Capture E AFTER the relocate so it reflects the
            // epoch the relocate used.
            let epoch_before = primary.cluster_state_for_test().primary_epoch();
            primary.relocate_primary_to("sec-9".into()).await;
            let collide_epoch = epoch_before + 1;
            assert_eq!(
                primary.cluster_state_for_test().primary_epoch(),
                collide_epoch,
                "the relocate must originate at primary_epoch()+1"
            );
            assert_eq!(
                primary.cluster_state_for_test().current_primary(),
                Some("sec-9"),
                "before the concurrent election lands, the relocate named sec-9"
            );

            // The concurrent failover election names the lex-LOWER sec-0 at the
            // SAME epoch E. The equal-epoch lex tiebreak (sec-0 < sec-9) wins,
            // so sec-0 overwrites sec-9 — convergent, and relocate_primary_to
            // adds no logic forcing sec-9 to stay.
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::PrimaryChanged {
                    new: "sec-0".into(),
                    epoch: collide_epoch,
                    reason: PrimaryChangeReason::Election,
                });
            assert_eq!(
                primary.cluster_state_for_test().current_primary(),
                Some("sec-0"),
                "the equal-epoch lex tiebreak converges on the lex-lower winner \
                 sec-0; relocate must tolerate a DIFFERENT successor than its \
                 target (the Transferred reason is advisory, not asserted)"
            );
        })
        .await;
}

/// The OPERATIONAL bootstrap tail (`bootstrap_tail_dispatch`) activates THIS
/// node as the local primary (`activate_local_primary` →
/// `run_operational_and_finalize`): a primary seeded pre-complete with zero
/// tasks drives the tail to a clean `Ok(())` and asserts itself the local
/// primary (`primary_id == self`, `current_primary == self`). Reached ONLY on
/// the `BootstrapRole::PromotedDestination` arm (a `PromotionSnapshot`); the
/// relocate is NOT here (it fired in `run_pipeline`'s `SetupPeer` arm), so the
/// tail never relocates even with an eligible compute peer present.
#[tokio::test(flavor = "current_thread")]
async fn bootstrap_tail_activates_local_primary() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let config = test_primary_config();
            let own_id = config.node_id.clone();
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // An eligible compute peer IS present — the operational tail must
            // still NOT relocate to it (relocation is the SetupPeer arm's job,
            // which this tail is not).
            seed_member(&mut primary, "sec-0", 2, false, true);

            // Drive only the operational bootstrap tail directly (no full run):
            // the seed leaves `self.total_tasks == 0` (read LIVE by the tail),
            // so the operational loop's counter exit fires immediately.
            let exit = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                primary.bootstrap_tail_dispatch(),
            )
            .await;
            assert!(
                matches!(exit, Ok(Ok(()))),
                "the operational bootstrap tail must run the in-place tail to a \
                 clean Ok(()); got {exit:?}"
            );
            assert_eq!(
                primary.primary_id.as_deref(),
                Some(own_id.as_str()),
                "the operational tail must activate THIS node as the local \
                 primary (primary_id == self)"
            );
            assert_eq!(
                primary.cluster_state_for_test().current_primary(),
                Some(own_id.as_str()),
                "activate_local_primary must name self the primary"
            );
        })
        .await;
}

/// A SETUP PEER (a `ColdStart` seed ⇒ `BootstrapRole::SetupPeer`) with an EMPTY
/// candidate set is a hard `RunError::NoRelocationTarget` (mesh-always: the
/// setup peer must never stay primary). Drive the FULL `run_consuming` against
/// a CONNECTED secondary that is NOT promotion-eligible (`fake_secondary`
/// advertises `can_be_primary:false`): `wait_for_connections` + mesh formation
/// succeed (so the pipeline reaches the `SetupPeer` arm), but
/// `select_relocation_target(crate::primary::lifecycle::RelocationPolicy::LowestId)` finds no eligible compute peer ⇒ the run errors
/// rather than silently staying local.
#[tokio::test(flavor = "current_thread")]
async fn setup_peer_empty_candidate_set_is_no_relocation_target() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // One CONNECTED but non-eligible secondary (fake_secondary welcomes
            // with can_be_primary:false), so mesh formation completes and the
            // pipeline reaches the SetupPeer relocate branch — but the
            // candidate set is empty (the only peer cannot be primary).
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
            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary(id, 2, 1024 * 1024 * 1024, rx, tx));
            }

            let (deps, ops, ope) = noop_phase_args();
            let exit = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                primary.run_consuming(
                    SeedSource::ColdStart { binaries: vec![], phase_deps: deps },
                    ops,
                    ope,
                ),
            )
            .await
            .expect("the SetupPeer run must return promptly on the empty-candidate path");
            // The SetupPeer branch returns `Err(NoRelocationTarget)` from the
            // pipeline; `run_consuming`'s non-demoted arm wraps it as
            // `PrimaryRunOutcome::Local { result: Err(..) }` (the pipeline
            // COMPLETED with an error — it never demoted, because the relocate
            // never fired).
            assert!(
                matches!(
                    exit,
                    Ok(crate::primary::PrimaryRunOutcome::Local {
                        result: Err(crate::primary::RunError::NoRelocationTarget),
                        ..
                    })
                ),
                "a setup peer with no eligible compute peer must surface \
                 RunError::NoRelocationTarget, never silently stay local; got {exit:?}"
            );
        })
        .await;
}

/// BUG C regression — the setup peer relocates WITHOUT gating on the
/// secondary OPERATIONAL `MeshReady` signal (a circular deadlock).
///
/// The compute secondary here ([`fake_secondary_transport_only_no_meshready`])
/// is TRANSPORT-CONNECTED (welcome + cert-exchange) and an eligible relocation
/// target (`can_be_primary: true`), but it NEVER emits `MeshReady` — it models
/// a secondary still in `wait_for_setup`, which only goes operational (and
/// thus only emits `MeshReady`) after it receives an `InitialAssignment`. Under
/// mesh-always the setup peer never sends one (it relocates the role away), so
/// `MeshReady` is structurally unreachable on this path.
///
/// `mesh_ready_timeout` is set ABSURDLY HIGH (1 hour): the pre-fix code's
/// unconditional `wait_for_mesh_ready` would block on the never-arriving
/// `MeshReady` for the full hour, so the tight outer `timeout(5s)` would trip
/// and FAIL the test. With the fix the setup peer relocates off the transport-
/// connected fleet immediately — mesh-readiness is a transport fact, decoupled
/// from any secondary's operational state — and the run returns
/// `PrimaryRunOutcome::Relocated` in milliseconds, well inside the 5s budget.
///
/// REVERT CHECK: re-instating the pre-branch `self.wait_for_mesh_ready(..)?`
/// call regresses this test (the 5s `timeout` fires before the 1h mesh-ready
/// deadline → `expect` panics).
#[tokio::test(flavor = "current_thread")]
async fn setup_peer_relocates_without_gating_on_secondary_operational_meshready() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // One transport-connected, eligible-but-NOT-operational secondary.
            let (transport, secondary_ends) = setup_test(1);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                // Absurdly high: if the relocate were (wrongly) gated on the
                // operational `MeshReady`, it would block here for an hour. The
                // 5s outer timeout below is the deadlock detector.
                mesh_ready_timeout: Duration::from_secs(3600),
                ..test_primary_config()
            };
            let (primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary_transport_only_no_meshready(
                    id,
                    2,
                    1024 * 1024 * 1024,
                    rx,
                    tx,
                ));
            }

            let (deps, ops, ope) = noop_phase_args();
            let exit = tokio::time::timeout(
                Duration::from_secs(5),
                primary.run_consuming(
                    SeedSource::ColdStart { binaries: vec![], phase_deps: deps },
                    ops,
                    ope,
                ),
            )
            .await
            .expect(
                "the setup peer must relocate WITHOUT waiting on the secondary's \
                 operational MeshReady — a 5s relocate that overruns means the \
                 circular wait_for_mesh_ready deadlock is back (BUG C)",
            );

            // The setup peer handed the role to the connected compute peer:
            // `run_consuming`'s demote arm wins and returns `Relocated` — proof
            // the relocate fired off TRANSPORT connectivity alone, never the
            // operational MeshReady the fake withheld.
            assert!(
                matches!(exit, Ok(crate::primary::PrimaryRunOutcome::Relocated { .. })),
                "a transport-connected eligible compute peer must let the setup \
                 peer RELOCATE (PrimaryRunOutcome::Relocated) even though no \
                 MeshReady was ever sent; got {exit:?}"
            );
        })
        .await;
}
