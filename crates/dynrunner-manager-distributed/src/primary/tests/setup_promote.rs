//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;

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
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_millis(50),
            // Setup-promote intent: the submitter has deferred
            // discovery + ledger seed to the promoted secondary, so
            // `total_tasks` starts at 0 and the operational loop must
            // wait for the secondary's TaskAdded broadcast.
            required_setup_on_promote: true,
            ..test_primary_config()
        };
        let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Sanity: `PrimaryCoordinator::new` must initialise
        // `setup_pending` from the config (the field's invariant). If
        // this fails the rest of the test's reasoning is wrong.
        assert!(
            primary.setup_pending(),
            "setup_pending must be initialised from config.required_setup_on_promote at construction"
        );

        // Mirror what `run()` would set up: empty pool, default phase
        // tracked, no binaries, `total_tasks = 0`. This pins the
        // `run_complete_check` counter exit being gated by the
        // CRDT-derived `setup_pending()` predicate
        // (`required_setup_on_promote && cluster_state.task_count() == 0`)
        // — while the gate holds, the `0+0 >= 0` counter trip is
        // suppressed so the loop does not declare the run done before
        // discovery seeds the ledger. `self.secondaries` is empty in
        // this synthetic setup, so `process_heartbeat_tick` walks empty
        // hashmaps and is a no-op; no death-eval race.
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
        // Regression guard: the seed `TaskAdded` for a setup-discovered
        // task must be keyed with the wire-canonical recipe, which folds
        // `phase_id` into the hash. A prior secondary-side seed helper
        // hashed only `path + identifier`; for any phase-bearing task
        // that key DIVERGED from `compute_task_hash`, so every
        // assignment/completion the promoted primary later originated
        // (keyed by `compute_task_hash`) missed the ledger entry and the
        // CRDT row stayed Pending forever. Pin that the canonical key is
        // sensitive to `phase_id` — a path+identifier-only hash would
        // collide a different-phase task here and would NOT match this
        // value, which is the divergence the bug shipped on.
        let bare_path_identifier_hash = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut h = DefaultHasher::new();
            bin.path.hash(&mut h);
            bin.identifier.hash(&mut h);
            format!("{:016x}", h.finish())
        };
        assert_ne!(
            hash, bare_path_identifier_hash,
            "the canonical seed key must fold phase_id in; a path+\
             identifier-only key is the drifted recipe that stranded \
             setup-discovered tasks in the CRDT ledger",
        );
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
        // The completion arrives as a `TaskComplete` wire report — the
        // shape the co-located secondary's worker uses to report to the
        // authoritative primary in the composed model. The authority's
        // `handle_task_complete` inserts the hash into `completed_tasks`
        // (the set the operational-loop counter exit reads) and
        // broadcasts the CRDT `TaskCompleted`. The hash is not locally
        // in-flight (the TaskAdded above mirrored into `cluster_state`,
        // not the pool), so `free_slot_on_terminal` no-ops on the slot
        // and the per-phase cascade is skipped — only the
        // `completed_tasks` insert + the counter exit matter here.
        incoming_tx
            .send(DistributedMessage::TaskComplete {
                sender_id: "sec-promoted".into(),
                timestamp: 0.0,
                secondary_id: "sec-promoted".into(),
                worker_id: 0,
                task_hash: hash.clone(),
                result_data: None,
            })
            .unwrap();
        // Hold the sender open so the loop's exit MUST come from the
        // counter check, not the transport-closed fallback. Asserting
        // on `setup_pending == false` post-exit pins that the
        // TaskAdded mirror path actually cleared the gate.
        let _hold = incoming_tx;

        // Bounded wait. Pre-fix the loop exits on iteration 1 (the
        // counter check fires at `0+0 >= 0` before any recv runs).
        // Post-fix the loop must process both wire messages before the
        // counter check trips; that completes in single-digit ms on
        // an in-process channel transport. 5s ceiling for CI flake
        // tolerance.
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
                    !primary.setup_pending(),
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
                // iteration once completions cover the total. The default
                // `required_setup_on_promote = false` is exactly this path.
                ..test_primary_config()
            };
            let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Pin invariant: pre-seeded path leaves `setup_pending = false`.
            assert!(
                !primary.setup_pending(),
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
                        !primary.setup_pending(),
                        "pre-seeded bootstrap must not flip setup_pending true at any point"
                    );
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

/// T3 — setup-promote: the initial-empty-phase cascade does NOT fire
/// `on_phase_end` while `setup_pending = true`, and phases remain
/// `Active` (not `Drained`). After a `TaskAdded` mutation flips
/// `setup_pending` to `false`, a subsequent cascade legitimately
/// drains the still-empty phases and fires `on_phase_end(.., 0, 0)`.
///
/// Pre-fix shape of the bug: a setup-defer submitter enters `run()`
/// with `binaries = []` (no items to discover locally) so every
/// declared phase is `Active` with zero items as a TRANSIENT
/// pre-discovery state. The pre-loop cascade
/// (`drain_empty_active_phases` + `process_phase_lifecycle`) fires
/// `on_phase_end(.., 0, 0)` for every phase before the promoted
/// secondary has had a chance to broadcast its first `TaskAdded`.
/// In asm-tokenizer's full-pipeline mode the consumer's
/// `on_phase_end` callback walks the just-finished phase's output
/// tree to spawn next-phase items; firing it on phases whose outputs
/// don't yet exist surfaces as `OSError: No such file or directory`,
/// crashes through the catch-all "TaskDefinition.on_phase_end raised;
/// continuing" log, and leaves the run with `total = 0` work after
/// all 15 SLURM jobs spawn and exit clean.
///
/// Fix: the cascade gates on `!self.setup_pending` at both the
/// pre-loop call site (`coordinator.rs:1257` area, the explicit
/// drain + cascade pair) and at the top of `process_phase_lifecycle`
/// (defence-in-depth for every other caller). While the gate is up
/// neither side-effect runs — phases stay `Active`, no callback
/// fires, no `drained_pending` accumulates. The latch clears via
/// the `TaskAdded` / `TasksSpawned` / `RunComplete` mirror in
/// `mirror_mutation_to_accounting`, after which the SAME cascade
/// shape (drain + process) legitimately fires `on_phase_end` on
/// the now-truly-empty phases.
///
/// Test rig: builds a `PrimaryCoordinator` directly (no operational
/// loop, no wire), seeds a 2-phase pool, attaches an `on_phase_end`
/// callback that records every fire, and calls the cascade pair
/// twice — once with `setup_pending = true`, once after a
/// `TaskAdded` mutation has cleared the latch. Asserts on (a) the
/// callback fire-counts pre- and post-clear, (b) the `phase_state`
/// reading on the pool (Active before, Done after), and (c) the
/// latch transition itself.
#[tokio::test(flavor = "current_thread")]
async fn setup_pending_suppresses_initial_phase_cascade_until_task_added() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, _secondary_ends) = setup_test(1);

        let config = PrimaryConfig {
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_millis(50),
            // Setup-promote intent: the gate's invariant is keyed off
            // this. With `false` the gate is always satisfied and the
            // bug cannot manifest — that case is covered by the
            // pre-seeded-bootstrap regression above.
            required_setup_on_promote: true,
            ..test_primary_config()
        };
        let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        // Sanity: setup_pending must be initialised from the config.
        // If this fails the rest of the test's reasoning is wrong.
        assert!(
            primary.setup_pending(),
            "setup_pending must be initialised from config.required_setup_on_promote at construction"
        );

        // Two declared phases (no deps between them; both start
        // `Active`). Mirrors the asm-tokenizer full-pipeline shape
        // where `phase_deps` registers e.g. `tokenize` and
        // `unify_vocab` as separate top-level phases. Both start with
        // zero items — the promoted secondary will later seed items
        // via wire-received TaskAdded, but at this point the local
        // pool is empty.
        let phase_a = dynrunner_core::PhaseId::from("tokenize");
        let phase_b = dynrunner_core::PhaseId::from("unify_vocab");
        let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
            [phase_a.clone(), phase_b.clone()],
            std::collections::HashMap::new(),
        )
        .expect("two-phase pool");
        primary.pending = Some(pool);
        primary.phase_completed.insert(phase_a.clone(), 0);
        primary.phase_completed.insert(phase_b.clone(), 0);
        primary.phase_failed.insert(phase_a.clone(), 0);
        primary.phase_failed.insert(phase_b.clone(), 0);
        primary.total_tasks = 0;

        // Record every on_phase_end invocation in a shared ledger
        // the test can inspect after each cascade attempt.
        // Arc<Mutex<...>> not Rc<RefCell<...>> because OnPhaseEnd is
        // `Send`-bounded (see `primary/config.rs::OnPhaseEnd =
        // Box<dyn FnMut(...) + Send>`).
        let calls: std::sync::Arc<std::sync::Mutex<Vec<(String, u32, u32)>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let calls_inner = calls.clone();
        primary.on_phase_end = Some(Box::new(move |p, c, f| {
            calls_inner
                .lock()
                .expect("poisoned")
                .push((p.as_str().to_string(), c, f));
        }));

        // -------- Phase 1: cascade-while-setup-pending --------
        // Exercise the cascade GATE on `process_phase_lifecycle`
        // directly: pre-populate `drained_pending` by calling
        // `drain_empty_active_phases` UNCONDITIONALLY (mimicking the
        // pre-fix production flow where the call-site gate did not
        // exist), then invoke the cascade. With the fix, the cascade
        // early-returns and the queued drained-phase entries sit
        // untouched in `drained_pending` — no `on_phase_end` fires,
        // no `mark_phase_done` runs. Without the fix, the cascade
        // would walk the queue and fire `on_phase_end(.., 0, 0)`
        // for each phase + flip them to Done.
        //
        // This pins the DEFENCE-IN-DEPTH guard inside
        // `process_phase_lifecycle` independently of the
        // call-site gate at `coordinator.rs:1257`. A future
        // refactor that drops the call-site gate but leaves the
        // cascade gate intact still passes this test; the
        // production pre-loop drain at 1257 is conditional on
        // `!self.setup_pending` purely to avoid the
        // `Active → Drained` pool-state flip (the cascade-level
        // gate alone would leave a stale queue of drained phases
        // that fire all at once whenever the latch clears, which
        // is exactly the post-clear scenario Phase 2 below pins).
        primary.pool_mut().drain_empty_active_phases();
        primary.process_phase_lifecycle(&mut None).await;

        // Assertion (1): no on_phase_end fires while setup is pending.
        // This is the load-bearing assertion against unfixed code —
        // pre-fix, the cascade walks the queued drained_pending and
        // fires two callbacks here (one per phase).
        {
            let recorded = calls.lock().expect("poisoned");
            assert!(
                recorded.is_empty(),
                "on_phase_end must NOT fire while setup_pending=true \
                 even when drained_pending is non-empty; observed \
                 calls = {:?}",
                *recorded
            );
        }
        // Assertion (2): phases sit at `Drained` (the drain DID
        // mark them, since we called it unconditionally in this
        // test) but have NOT reached `Done` — `mark_phase_done`
        // only runs inside the cascade after `on_phase_end` fires,
        // and the cascade early-returned. Pre-fix, the phases
        // would be `Done` at this point.
        for phase in [&phase_a, &phase_b] {
            assert_eq!(
                primary.pool().phase_state(phase),
                Some(dynrunner_scheduler_api::PhaseState::Drained),
                "phase {phase} must sit at Drained (drained but not \
                 marked Done) while setup_pending=true; the cascade \
                 gate must suppress mark_phase_done together with the \
                 on_phase_end fire"
            );
        }

        // -------- Transition: apply a TaskAdded mutation --------
        // The mirror path (`mirror_mutation_to_accounting`) flips
        // `setup_pending = false` on TaskAdded / TasksSpawned /
        // RunComplete. We synthesise the mutation locally and route
        // it through `handle_cluster_mutation` — the same chokepoint
        // the operational loop uses when a TaskAdded arrives off the
        // wire from the promoted secondary. Using a task in
        // `phase_a` so the post-apply ledger has at least one entry;
        // `phase_b` stays empty to pin "still-empty phases fire
        // on_phase_end legitimately post-discovery".
        let bin = TaskInfo {
            path: std::path::PathBuf::from("/tmp/discovered"),
            size: 100,
            identifier: TestId("discovered".into()),
            phase_id: phase_a.clone(),
            type_id: dynrunner_core::TypeId::from("default"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: "task-discovered".into(),
            task_depends_on: vec![],
            preferred_secondaries: dynrunner_core::SoftPreferredSecondaries::default(),
            preferred_version: Default::default(),
            resolved_path: None,
        };
        let hash = crate::primary::wire::compute_task_hash(&bin);
        primary
            .handle_cluster_mutation(DistributedMessage::ClusterMutation {
                sender_id: "sec-promoted".into(),
                timestamp: 0.0,
                mutations: vec![ClusterMutation::TaskAdded {
                    hash: hash.clone(),
                    task: bin.clone(),
                }],
            })
            .await;

        // Pin the mid-test invariants: the mirror path cleared the
        // latch and refreshed total_tasks.
        assert!(
            !primary.setup_pending(),
            "setup_pending must be cleared by the TaskAdded mirror; \
             if this fails the latch is stuck and the rest of the test \
             reasoning collapses"
        );
        assert_eq!(
            primary.total_tasks, 1,
            "total_tasks must refresh from cluster_state.task_count() \
             after the TaskAdded batch applies (mirror path's \
             post-apply refresh in handle_cluster_mutation)"
        );

        // -------- Phase 2: cascade-after-setup-cleared --------
        // Re-invoke `process_phase_lifecycle`. The `drained_pending`
        // queue Phase 1's drain populated is STILL pending (the
        // early-return suppressed both the callback fire AND the
        // mark_phase_done step, so poll_drain_transitions never ran
        // to consume the queue). With the gate cleared, the cascade
        // now consumes the queue and fires `on_phase_end(.., 0, 0)`
        // per phase, then marks each Done. This pins the
        // post-discovery behaviour: the gate is a strict superset of
        // the historical semantics; once cleared, the cascade
        // exhibits the same shape a legacy bootstrap pre-loop
        // cascade would have.
        //
        // Note: cluster_state now holds 1 task in phase_a, but the
        // LOCAL pool is still empty for both phases — TaskAdded
        // mirrors into cluster_state, not the local pool (see the
        // `if self.pending.is_some() { reinject }` arm in
        // handle_cluster_mutation: it gates on TasksSpawned, not
        // TaskAdded). The locally-empty phases are therefore the
        // right cascade subject — the shape a setup-defer authority
        // sees while discovery has seeded the CRDT but not the local
        // pool.
        primary.process_phase_lifecycle(&mut None).await;

        // Assertion (3): on_phase_end fires exactly once per declared
        // phase, with (completed=0, failed=0). Order is not pinned
        // (the cascade walks `drained_pending` whose ordering is
        // implementation-defined); we sort-and-compare to keep the
        // assertion deterministic.
        {
            let mut recorded = calls.lock().expect("poisoned").clone();
            recorded.sort();
            assert_eq!(
                recorded,
                vec![
                    ("tokenize".to_string(), 0, 0),
                    ("unify_vocab".to_string(), 0, 0),
                ],
                "post-setup_pending cascade must fire on_phase_end once \
                 per declared phase with (completed=0, failed=0); \
                 observed calls = {recorded:?}"
            );
        }
        // Assertion (4): the pool has fully cascaded — both phases
        // reached Done (mark_phase_done ran post-on_phase_end).
        for phase in [&phase_a, &phase_b] {
            assert_eq!(
                primary.pool().phase_state(phase),
                Some(dynrunner_scheduler_api::PhaseState::Done),
                "phase {phase} must reach Done after the post-clear cascade"
            );
        }
    }).await;
}

/// Regression for the `--source-already-staged` long-hang class: a
/// setup-defer authority whose discovery feed never seeds the ledger.
///
/// Scenario (setup-promote, discovery never lands):
///   - The authority is in setup-defer mode
///     (`required_setup_on_promote = true`), so `setup_pending()` is true
///     and `total_tasks = 0` at operational-loop entry.
///   - The discovery feed never broadcasts TaskAdded / TasksSpawned /
///     RunComplete (e.g. the `discover_items` Python callback hung, or
///     the discovering node died before its first broadcast).
///   - While `setup_pending()` holds, the counter exit and the
///     pool-drain exit in `run_complete_check` are both suppressed (the
///     `0+0 >= 0` trip would otherwise declare the run done before any
///     task exists). With `secondaries` empty the fleet-dead timer never
///     starts either. Without a backstop this is an unbounded hang.
///
/// Post-fix invariants pinned here (the restored setup-promote-deadline
/// arm in `operational_loop`):
///   (A) `operational_loop` returns `Ok(())` (the arm exits via `break`,
///       never via Err — Err would propagate through `?` and lose the
///       diagnostic Duration).
///   (B) `setup_deadline_outcome` is `Some(elapsed)` with
///       `elapsed >= deadline` and `elapsed < deadline + slack`. The
///       outer `run_pipeline` then surfaces this as
///       `RunError::SetupDeadlineExpired { elapsed }`; tested via the
///       Display impl below.
///   (C) `setup_pending()` was still true at fire time (defensive: the
///       arm re-checks the gate at fire time and treats a cleared gate
///       as a no-op — pinning that the exit was driven by genuine
///       deadline expiry, not a race where a TaskAdded landed
///       concurrently and the loop exited via the counter check).
///   (D) The deadline arm DOES NOT fire when the gate clears in time.
///       Covered by the sibling
///       `setup_deadline_does_not_fire_when_taskadded_arrives_in_time`
///       test (clean exit through the counter path well before the
///       deadline).
///
/// Test rig:
///   - Short `setup_promote_deadline` (200ms) so the test completes in
///     well under 1s on every test runner. A `tokio::time::timeout`
///     wraps the call with a 5s ceiling so a stuck loop fails loudly
///     (rather than hanging the test runner).
///   - No transport activity: the channel transport's incoming queue
///     stays empty so no message arm can resolve.
#[tokio::test(flavor = "current_thread")]
async fn setup_deadline_fires_when_promoted_secondary_silent() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(1);
            // Keep the secondary end alive so transport.recv() doesn't
            // return None (which would exit via the both-channels-closed
            // fallback, not the deadline arm we're testing).
            let (_sec_id, _to_sec_rx, _incoming_tx) = secondary_ends.into_iter().next().unwrap();

            let deadline = Duration::from_millis(200);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                keepalive_interval: Duration::from_millis(50),
                // Setup-promote intent: `setup_pending` starts true and
                // there is no TaskAdded / TasksSpawned / RunComplete
                // coming. The new deadline arm is the only exit cue.
                required_setup_on_promote: true,
                // The arm under test. 200ms is comfortably above the
                // tokio timer resolution (1ms) so the elapsed-> Duration
                // check below has room without flake-prone tight bounds.
                setup_promote_deadline: deadline,
                ..test_primary_config()
            };
            let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Sanity: latch was initialised from
            // `required_setup_on_promote`. If this fails the rest of the
            // test's reasoning collapses.
            assert!(
                primary.setup_pending(),
                "setup_pending must be initialised from \
             config.required_setup_on_promote at construction"
            );

            // Mirror what `run()` would set up before the operational
            // loop entry: empty pool, default phase tracked, no binaries,
            // `total_tasks = 0`. The CRDT-derived `setup_pending()` gate
            // suppresses the counter exit; this isolates the
            // deadline arm as the ONLY non-trivial exit cue (the counter
            // / pool-drain / run_complete / fleet_dead / transport-closed
            // arms are all structurally unavailable: total_tasks=0 is
            // gated by setup_pending, secondaries={} is the test rig's
            // synthetic state, the channels stay open).
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

            // Outer ceiling: a stuck operational loop should fail the
            // test loudly rather than hang the runner. 5s is 25× the
            // deadline so a healthy run finishes well within budget.
            let start = std::time::Instant::now();
            let exit = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                primary.operational_loop(),
            )
            .await;
            let elapsed = start.elapsed();

            match exit {
                Ok(Ok(())) => {
                    // (A) Loop returned Ok via `break`, not via Err.
                    // (B) The deadline arm set the outcome field with a
                    //     plausible elapsed value.
                    let outcome = primary
                        .setup_deadline_outcome
                        .expect("setup_deadline_outcome must be Some after deadline-driven exit");
                    assert!(
                        outcome >= deadline,
                        "elapsed ({outcome:?}) must be at least the deadline ({deadline:?}) — \
                     the arm should not fire EARLY"
                    );
                    // Slack for scheduler jitter: 500ms above the
                    // deadline is generous on a hot test runner and tight
                    // enough that a real hang would blow through.
                    assert!(
                        outcome < deadline + Duration::from_millis(500),
                        "elapsed ({outcome:?}) must be within deadline+500ms ({:?}) — \
                     a substantially-later fire suggests the arm is being \
                     out-raced by another arm that's letting iterations \
                     leak past the timer",
                        deadline + Duration::from_millis(500)
                    );
                    // (C) The latch stayed up — no TaskAdded came in.
                    assert!(
                        primary.setup_pending(),
                        "setup_pending must remain true after a deadline-driven \
                     exit; if this fails the test rig leaked a TaskAdded and \
                     the run actually exited via the counter path, which \
                     defeats the regression's purpose"
                    );
                    // Outer wall-clock sanity: the test itself completed
                    // close to the deadline (the loop didn't hang on
                    // some other arm).
                    assert!(
                        elapsed < Duration::from_secs(2),
                        "outer wall-clock ({elapsed:?}) should be much less than the 5s \
                     ceiling — a stuck loop would hit the ceiling"
                    );
                }
                Ok(Err(e)) => panic!(
                    "operational_loop returned Err: {e} (expected Ok with \
                 setup_deadline_outcome set)"
                ),
                Err(_) => panic!(
                    "operational_loop did not exit within 5s — the deadline arm \
                 ({deadline:?}) is not firing. Either the arm condition is \
                 wrong, the sleep_until isn't waking, or another arm is \
                 raced ahead and disabled the deadline incorrectly."
                ),
            }
        })
        .await;
}

/// Companion to `setup_deadline_fires_when_promoted_secondary_silent`:
/// pin that the arm IS DISABLED when `setup_pending` clears before the
/// deadline. A TaskAdded mutation arrives ~50ms into the wait;
/// `setup_pending` flips false via the mirror path; the deadline arm's
/// `if self.setup_pending && !setup_promote_deadline_consumed` guard
/// fails on the next select! re-entry, so the arm parks. The loop then
/// exits via the natural counter path (total_tasks=1, completed=1) —
/// NOT via the deadline arm.
///
/// Pre-fix shape (if the arm were unconditional): the sleep_until
/// would continue ticking after the latch cleared and false-fire at
/// deadline, returning Err with a spurious deadline-expiry on a
/// completed run. Post-fix: `setup_deadline_outcome` stays `None`.
#[tokio::test(flavor = "current_thread")]
async fn setup_deadline_does_not_fire_when_taskadded_arrives_in_time() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(1);
            let (_sec_id, _to_sec_rx, incoming_tx) = secondary_ends.into_iter().next().unwrap();

            let deadline = Duration::from_millis(500);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                keepalive_interval: Duration::from_millis(50),
                required_setup_on_promote: true,
                setup_promote_deadline: deadline,
                ..test_primary_config()
            };
            let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
                config,
                transport,
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

            // Pre-queue a TaskAdded + TaskCompleted that mirror the
            // shape of the existing
            // `setup_pending_blocks_immediate_exit_then_proceeds_on_task_added`
            // test. The TaskAdded clears the latch + refreshes
            // total_tasks to 1; the TaskCompleted lets the counter exit
            // fire at `1+0 >= 1 && active_workers == 0`.
            //
            // We deliberately enqueue both messages BEFORE entering the
            // operational loop — the transport arm drains them
            // immediately, well before the 500ms deadline. The deadline
            // arm should observe the cleared latch on its
            // arm-condition re-evaluation and park.
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
            // Completion as a `TaskComplete` wire report — the composed
            // authority's `handle_task_complete` populates `completed_tasks`
            // (counter-exit input) and broadcasts the CRDT mutation. See the
            // matching note in
            // `setup_pending_blocks_immediate_exit_then_proceeds_on_task_added`.
            incoming_tx
                .send(DistributedMessage::TaskComplete {
                    sender_id: "sec-promoted".into(),
                    timestamp: 0.0,
                    secondary_id: "sec-promoted".into(),
                    worker_id: 0,
                    task_hash: hash.clone(),
                    result_data: None,
                })
                .unwrap();
            // Hold the sender so the channel doesn't close (which would
            // exit via the transport-closed fallback rather than the
            // counter exit we want to observe).
            let _hold = incoming_tx;

            let exit = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                primary.operational_loop(),
            )
            .await;

            match exit {
                Ok(Ok(())) => {
                    // Latch was cleared by the TaskAdded mirror — the
                    // arm-condition's `self.setup_pending` test should
                    // have flipped false long before the deadline.
                    assert!(
                        !primary.setup_pending(),
                        "setup_pending must be cleared by the TaskAdded mirror"
                    );
                    // The deadline arm did NOT set its outcome — the
                    // exit was via the counter path.
                    assert!(
                        primary.setup_deadline_outcome.is_none(),
                        "setup_deadline_outcome must be None when the run \
                     completes via the counter path before the deadline; \
                     a Some(...) here means the deadline arm fired \
                     spuriously after the latch cleared"
                    );
                    // Sanity: the run produced the expected outcome.
                    assert_eq!(primary.total_tasks, 1);
                    assert!(primary.completed_tasks.contains(&hash));
                }
                Ok(Err(e)) => panic!("operational_loop returned Err: {e}"),
                Err(_) => panic!(
                    "operational_loop did not exit within 5s — the counter \
                 exit is not firing after the latch clears, or the deadline \
                 arm is somehow blocking the natural exit path"
                ),
            }
        })
        .await;
}
