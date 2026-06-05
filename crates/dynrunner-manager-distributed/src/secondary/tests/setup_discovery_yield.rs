//! Setup-discovery `SetupPending` yield: the pre-staged-mode producer.
//!
//! In pre-staged mode the authority defers task discovery to the
//! corpus-mounting secondaries (it sends an empty `InitialAssignment {
//! pre_staged_mode: true }` rather than seeding the ledger). The
//! secondary's `process_tasks` loop yields `RunOutcome::SetupPending`
//! when [`SecondaryCoordinator::setup_discovery_pending`] is true so the
//! PyO3 wrapper can run Python's `task.discover_items` and feed the
//! result back via [`SecondaryCoordinator::ingest_setup_discovery`].
//!
//! These tests pin the DISCRIMINATOR + the FIRE-ONCE latch at the
//! manager-method level — synchronous predicate assertions, no
//! wall-clock racing of the unbounded `process_tasks` loop (which the
//! plan forbids). The full loop-yields-SetupPending behaviour is
//! e2e-validated in R5c via the pyo3 wrapper, which is the only caller
//! that acts on the yield.

#![cfg(test)]

use std::collections::HashMap;

use dynrunner_core::PhaseId;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};

use super::super::test_helpers::{election_config, make_secondary_recording};
use super::processing::make_binary;

/// Baseline: a non-pre-staged secondary NEVER yields. `pre_staged_mode`
/// is the gate's first axis; legacy / failover runs leave it false.
#[tokio::test(flavor = "current_thread")]
async fn non_pre_staged_never_pending() {
    let (mut sec, _log) = make_secondary_recording(election_config("sec-a"), 0);
    // `pre_staged_mode` / `setup_discovery_done` are carried on the typed
    // lifecycle's `Configuring`/`Operational` state; the predicate is
    // evaluated against the configured state, so land Operational first
    // (matching the production point where the discriminator is read).
    sec.enter_operational_for_test();
    assert!(
        !sec.setup_discovery_pending(),
        "default (non-pre-staged) secondary must never be setup-discovery pending",
    );
}

/// Pre-staged + empty ledger + not-yet-discovered → the yield fires.
#[tokio::test(flavor = "current_thread")]
async fn pre_staged_empty_ledger_is_pending() {
    let (mut sec, _log) = make_secondary_recording(election_config("sec-a"), 0);
    sec.enter_operational_for_test();
    sec.set_pre_staged_mode(true);
    assert!(
        sec.setup_discovery_pending(),
        "pre-staged mode with an empty ledger must be setup-discovery pending",
    );
}

/// `ingest_setup_discovery` with discovered items: broadcasts
/// `PhaseDepsSet + TaskAdded`, seeds the local ledger (so the predicate
/// self-clears on the "ledger non-empty" axis) AND latches the
/// fire-once guard. The predicate is false afterward.
#[tokio::test(flavor = "current_thread")]
async fn ingest_with_items_clears_pending_and_broadcasts() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(election_config("sec-a"), 1);
            sec.enter_operational_for_test();
            sec.set_pre_staged_mode(true);
            assert!(sec.setup_discovery_pending());

            let mut deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            deps.insert(PhaseId::from("default"), vec![]);
            sec.ingest_setup_discovery(
                vec![make_binary("item-0", 1), make_binary("item-1", 1)],
                deps,
            )
            .await
            .expect("ingest must succeed");

            // Ledger seeded → predicate false on the count axis.
            assert!(
                !sec.setup_discovery_pending(),
                "a non-empty ledger must clear the setup-discovery pending state",
            );

            // The broadcast carried PhaseDepsSet + one TaskAdded per item.
            let log = log.borrow();
            let mutations: Vec<&ClusterMutation<_>> = log
                .iter()
                .filter_map(|m| match m {
                    DistributedMessage::ClusterMutation { mutations, .. } => Some(mutations),
                    _ => None,
                })
                .flatten()
                .collect();
            assert!(
                mutations
                    .iter()
                    .any(|m| matches!(m, ClusterMutation::PhaseDepsSet { .. })),
                "ingest must broadcast PhaseDepsSet",
            );
            let task_added = mutations
                .iter()
                .filter(|m| matches!(m, ClusterMutation::TaskAdded { .. }))
                .count();
            assert_eq!(task_added, 2, "one TaskAdded per discovered item");
        })
        .await;
}

/// Empty discovery (every item already complete under a `--skip-existing`
/// filter): the ledger stays EMPTY, so the count-axis self-clear does
/// NOT fire — the FIRE-ONCE latch is the load-bearing guard preventing a
/// re-yield. `ingest_setup_discovery` sets the latch unconditionally and
/// also broadcasts `RunComplete`. The predicate is false afterward
/// despite the empty ledger.
#[tokio::test(flavor = "current_thread")]
async fn empty_discovery_latches_without_reyield() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(election_config("sec-a"), 1);
            sec.enter_operational_for_test();
            sec.set_pre_staged_mode(true);
            assert!(sec.setup_discovery_pending());

            let mut deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            deps.insert(PhaseId::from("default"), vec![]);
            // Zero discovered items.
            sec.ingest_setup_discovery(vec![], deps)
                .await
                .expect("empty ingest must succeed");

            // Ledger is STILL empty (no TaskAdded), but the latch is set,
            // so the predicate must be false — the loop will not re-yield.
            assert!(
                !sec.setup_discovery_pending(),
                "the fire-once latch must suppress re-yield on an empty discovery",
            );

            // Empty discovery broadcasts RunComplete so peers' exit arms fire.
            assert!(
                log.borrow().iter().any(|m| matches!(
                    m,
                    DistributedMessage::ClusterMutation { mutations, .. }
                        if mutations.iter().any(|mu| matches!(mu, ClusterMutation::RunComplete))
                )),
                "empty discovery must broadcast RunComplete",
            );
        })
        .await;
}

/// Wire-shape mirror: the secondary's `ingest_setup_discovery` seed key
/// for a phase-bearing task MUST equal the key the promoted primary
/// derives at assignment/completion time. Both sides are
/// [`dynrunner_core::compute_task_hash`] — the single canonical recipe
/// that folds `phase_id` into the hash. This asserts the keys match by
/// mirroring the OTHER side's recipe (the primary's
/// `compute_task_hash`), not by round-tripping the secondary's own seed.
///
/// The historical defect seeded with a path+identifier-only hash that
/// dropped `phase_id`; for any phase-bearing task that key diverged from
/// `compute_task_hash`, so this assertion would have caught it.
#[tokio::test(flavor = "current_thread")]
async fn seed_key_mirrors_primary_assignment_key() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _log) = make_secondary_recording(election_config("sec-a"), 1);
            sec.enter_operational_for_test();
            sec.set_pre_staged_mode(true);

            let bin = make_binary("phase-bearing-item", 1);
            // The non-default phase_id is the load-bearing differentiator:
            // a recipe that drops it would collide every phase and miss
            // the primary's assignment key. `make_binary` already sets a
            // non-empty phase_id; pin it explicitly here so the test's
            // premise (phase-bearing task) is self-evident.
            assert!(
                !bin.phase_id.as_str().is_empty(),
                "the seed task must carry a phase_id for this mirror to mean anything",
            );

            let mut deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            deps.insert(bin.phase_id.clone(), vec![]);
            sec.ingest_setup_discovery(vec![bin.clone()], deps)
                .await
                .expect("ingest must succeed");

            // The primary's assignment/completion paths key on
            // `compute_task_hash` (see `lifecycle/dispatch.rs`,
            // `task/request.rs`, `task/complete.rs`). The seed must land
            // under that EXACT key for the ledger entry to be found.
            let primary_key = dynrunner_core::compute_task_hash(&bin);
            assert!(
                sec.cluster_state.task_state(&primary_key).is_some(),
                "the seed `TaskAdded` must be keyed by `compute_task_hash` \
                 (the primary's assignment key); a divergent seed recipe \
                 leaves the ledger entry unreachable by every later \
                 assignment/completion mutation",
            );

            // Mirror-recipe divergence guard: a path+identifier-only hash
            // (the drifted recipe) is NOT the seed key for a phase-bearing
            // task — proving the canonical key is phase-sensitive.
            let bare_path_identifier_hash = {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut h = DefaultHasher::new();
                bin.path.hash(&mut h);
                bin.identifier.hash(&mut h);
                format!("{:016x}", h.finish())
            };
            assert_ne!(
                primary_key, bare_path_identifier_hash,
                "the canonical recipe must fold phase_id in; the bare \
                 path+identifier hash is the drifted recipe the bug shipped",
            );
            assert!(
                sec.cluster_state.task_state(&bare_path_identifier_hash).is_none(),
                "the ledger must NOT carry an entry under the bare \
                 path+identifier key — if it did, the seed used the \
                 drifted recipe and would strand the task",
            );
        })
        .await;
}

/// End-to-end CRDT-terminal regression: seed via the REAL
/// `ingest_setup_discovery` path, then apply a cross-process-style
/// `TaskAssigned` + `TaskCompleted` keyed (as the promoted primary keys
/// them) by `compute_task_hash`. The replicated `outcome_counts()` MUST
/// reach `succeeded == total` with zero stranded (`counts().pending ==
/// 0`).
///
/// Fails-before semantics: pre-fix the seed keyed the ledger by a
/// path+identifier-only hash that dropped `phase_id`. The
/// `compute_task_hash`-keyed `TaskCompleted` then found no matching
/// entry (`apply` returns NoOp), so the CRDT row stayed `Pending`
/// forever — `outcome_counts().succeeded == 0` and `pending == total`
/// ("cluster routing collapsed"). Post-fix both sides share the
/// canonical recipe, the completion lands, and the row reaches
/// `Completed`. The `seed_key_mirrors_primary_assignment_key` test above
/// pins the keys-match precondition that makes this terminal reachable.
#[tokio::test(flavor = "current_thread")]
async fn setup_discovered_tasks_reach_crdt_terminal_on_completion() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _log) = make_secondary_recording(election_config("sec-a"), 1);
            sec.enter_operational_for_test();
            sec.set_pre_staged_mode(true);

            let binaries = vec![
                make_binary("disc-0", 1),
                make_binary("disc-1", 1),
                make_binary("disc-2", 1),
                make_binary("disc-3", 1),
            ];
            let total = binaries.len();

            let mut deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            deps.insert(PhaseId::from("default"), vec![]);
            sec.ingest_setup_discovery(binaries.clone(), deps)
                .await
                .expect("ingest must succeed");

            // Before any completion: every discovered task sits Pending
            // in the replicated ledger, none terminal.
            assert_eq!(sec.cluster_state.outcome_counts().succeeded, 0);
            assert_eq!(sec.cluster_state.counts().pending, total);

            // Drive each task to terminal the way the promoted primary
            // does it on the replicated CRDT: assign (Pending -> InFlight)
            // then complete (InFlight -> Completed), keyed by
            // `compute_task_hash` — the SAME recipe the assignment +
            // completion wire paths use. This is the cross-process
            // mutation the demolished band-aid keyed differently from the
            // seed.
            for bin in &binaries {
                let key = dynrunner_core::compute_task_hash(bin);
                sec.cluster_state.apply(ClusterMutation::<_>::TaskAssigned {
                    hash: key.clone(),
                    secondary: "sec-promoted".into(),
                    worker: 0u32,
                });
                sec.cluster_state.apply(ClusterMutation::<_>::TaskCompleted {
                    hash: key,
                    result_data: None,
                });
            }

            // CRDT-authoritative terminal: every task succeeded, none
            // stranded. This is the read the demoted primary / observer
            // terminal log line consumes (`outcome_counts`), NOT a
            // per-node HashSet counter.
            let outcome = sec.cluster_state.outcome_counts();
            assert_eq!(
                outcome.succeeded, total,
                "every setup-discovered task must reach Completed in the \
                 replicated ledger; succeeded < total means the completion \
                 mutation missed the seed entry (divergent hash recipe)",
            );
            assert_eq!(
                sec.cluster_state.counts().pending, 0,
                "no task may be stranded Pending after every completion \
                 lands; a non-zero count is the 'cluster routing \
                 collapsed' symptom",
            );
            assert_eq!(
                outcome.fail_retry + outcome.fail_oom + outcome.fail_final,
                0,
                "no task may be classified as failed in this happy path",
            );
        })
        .await;
}
