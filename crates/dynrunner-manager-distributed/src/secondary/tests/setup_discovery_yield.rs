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
    let (sec, _log) = make_secondary_recording(election_config("sec-a"), 0);
    assert!(
        !sec.setup_discovery_pending(),
        "default (non-pre-staged) secondary must never be setup-discovery pending",
    );
}

/// Pre-staged + empty ledger + not-yet-discovered → the yield fires.
#[tokio::test(flavor = "current_thread")]
async fn pre_staged_empty_ledger_is_pending() {
    let (mut sec, _log) = make_secondary_recording(election_config("sec-a"), 0);
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
