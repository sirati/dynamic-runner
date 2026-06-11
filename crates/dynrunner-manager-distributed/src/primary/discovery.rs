//! Mode-2 discover-on-promotion driver (V6 / mesh-always Phase 5b).
//!
//! Single concern: on a primary whose CRDT declares discovery `Owed`, run
//! the consumer's `discover_items` POLICY once and originate its result
//! (PhaseDepsSet + TaskAdded* + DiscoverySettled) into the replicated
//! ledger BEFORE the first run-complete check. This is the CRDT-pure
//! replacement for the deleted secondary-defer-discovery path: the empty
//! CRDT + the `Owed` marker IS the "awaiting seed" state, and the
//! compute-peer (relocated) primary — or an in-process
//! `--source-already-staged` local primary — runs discovery itself rather
//! than feeding it from a secondary.
//!
//! Module boundary: the driver lives on the [`PrimaryCoordinator`] (the
//! authority that owns origination), takes a consumer-supplied discovery
//! POLICY closure ([`crate::discovery::SetupDiscovery`], registered via
//! `register_setup_discovery`), and originates through the EXISTING
//! `apply_and_broadcast_cluster_mutations` pipeline + the single
//! `hydrate_from_cluster_state` pool builder. Callers (the pyo3 recipe)
//! see: "register a discovery policy + phase graph, same as you registered
//! phase callbacks." The driver NEVER touches the consumer `on_run_start`
//! lifecycle hook (once-and-fatal, fired by the run wrapper) — it runs only
//! the `discover_items` query.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DiscoveryDebt};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::primary::wire::compute_task_hash;
use crate::primary::{PrimaryCoordinator, RunError};

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Mode-2 discover-on-promotion. Fires at most once, ONLY when the CRDT
    /// declares discovery `Owed`. Runs the consumer's `discover_items`
    /// policy off the runtime thread (the closure already does the
    /// `spawn_blocking` GIL excursion — §14/§15), then originates ONE batch:
    /// `PhaseDepsSet` + one `TaskAdded` per discovered binary +
    /// one `TaskSkippedAlreadyDone` per discovery-marked already-done item +
    /// `DiscoverySettled`, through the canonical broadcast/apply pipeline.
    ///
    /// Originates NO run-terminal of its own. An empty corpus, a
    /// 100%-already-done corpus, and a corpus with live work ALL finalize
    /// through the SAME machinery mode-1 (`originate_cold_seed`) uses: the
    /// seam ends with [`Self::hydrate_from_cluster_state`] (line below), which
    /// projects EVERY seeded terminal (`SkippedAlreadyDone` included) into the
    /// `completed_tasks` set and sets `total_tasks` from the ledger — so the
    /// operational loop's counter exit (`completed + failed >= total_tasks`)
    /// trips for an all-skipped / empty corpus exactly as for a fully-completed
    /// run, and a corpus with to-run work finalizes when that work terminates.
    /// A run-terminal originated HERE, from a single-phase discovery view, was
    /// premature for a phase-chaining consumer: zero to-run items at discovery
    /// time does NOT mean the run is complete (later phases are injected via
    /// `on_phase_end`), and the sticky `RunComplete` latch made the observer
    /// exit while secondaries still worked and the cascade ran the next phase.
    ///
    /// Idempotent + failover-safe: gated on `discovery_debt() == Owed`,
    /// which a completed prior origination ratcheted to `Settled` (and which
    /// a re-promoted node inherits via the snapshot's sticky-monotone join),
    /// so a re-promotion after discovery completed is a NO-OP. `TaskAdded` is
    /// NoOp-on-duplicate, so even a re-run after a partial broadcast
    /// converges.
    ///
    /// Inert on every non-debt primary: a cold mode-1 / legacy run never
    /// declares debt (`discovery_debt() == Undeclared`), so the gate
    /// short-circuits and no policy is consulted.
    ///
    /// Reached ONLY by the OPERATIONAL primary (the
    /// `BootstrapRole::PromotedDestination` arm — a
    /// [`crate::process::SeedSource::PromotionSnapshot`]): the relocate TARGET,
    /// which inherits the `Owed` marker via its snapshot AND carries the
    /// registered discovery policy (via the promote recipe / the in-process
    /// `--source-already-staged` registration). The SETUP PEER never reaches
    /// this function — it relocates in `run_pipeline`'s `SetupPeer` arm BEFORE
    /// discover, so a setup peer that owes debt without a corpus or policy
    /// hands the `Owed` marker on untouched (the relocate happens before this
    /// point). The structural relocate-before-discover ordering — NOT a policy
    /// gate — is what keeps a policyless setup peer off the hard-error branch
    /// below.
    ///
    /// The driver ends with [`Self::hydrate_from_cluster_state`] — the SOLE
    /// pool builder — so THIS primary's pool holds the discovered tasks
    /// without duplicating the receive-path rebuild
    /// (`apply_and_broadcast_cluster_mutations` grows `cluster_state` but does
    /// NOT rebuild the pool).
    pub(crate) async fn discover_on_promotion(&mut self) -> Result<(), RunError> {
        if self.cluster_state.discovery_debt() != DiscoveryDebt::Owed {
            // Not a relocated / pre-staged primary, or discovery already
            // settled (re-promotion after a prior origination). NO-OP.
            return Ok(());
        }
        let Some(mut sd) = self.setup_discovery.take() else {
            // `Owed` but no policy registered = a programmer error: a primary
            // that owes discovery MUST carry the discovery policy. Reachable
            // ONLY by a `PromotionSnapshot` primary (the setup peer relocated
            // before this point), and a `PromotionSnapshot` that owes debt MUST
            // carry the policy via its recipe — so this fires only on a genuine
            // recipe-construction bug. Hard-fail rather than silently strand
            // (which run_complete_check would never exit — the counter arm is
            // gated on `Owed`).
            return Err(RunError::Other(
                "discovery_debt is Owed but no discovery policy was registered \
                 on this primary (the relocated / pre-staged recipe must \
                 register_setup_discovery before run)"
                    .into(),
            ));
        };

        // Run the consumer's discovery query off-runtime (the closure
        // `.await`s its own spawn_blocking GIL handle). `Err` aborts the run.
        let binaries = (sd.discover)().await.map_err(RunError::Other)?;

        // ONE atomic batch so "the tasks are now in the CRDT" and "debt
        // settled" land together on the wire — no window where a peer sees
        // `Settled` without the tasks or vice versa. PhaseDepsSet first (so
        // hydrate has the dep graph), then one TaskAdded per binary, then
        // DiscoverySettled (ratchets `Owed → Settled`).
        let mut batch: Vec<ClusterMutation<I>> = Vec::with_capacity(binaries.len() + 2);
        batch.push(ClusterMutation::PhaseDepsSet {
            deps: sd.phase_deps.clone(),
        });
        for (task, _skipped) in &binaries {
            batch.push(ClusterMutation::TaskAdded {
                hash: compute_task_hash(task),
                task: task.clone(),
            });
        }
        // Discovery already-done partition: after EVERY discovered item is
        // seeded `Pending` by the `TaskAdded` fan-out (so `task_count` ==
        // all items), materialise the marked subset terminal
        // `SkippedAlreadyDone`. One shared helper with `originate_cold_seed`
        // — no duplicated partition logic.
        batch.extend(self.skip_transitions(&binaries));
        batch.push(ClusterMutation::DiscoverySettled);
        self.apply_and_broadcast_cluster_mutations(batch).await;

        // `all_binaries` is a pure derived cache of the CRDT task universe;
        // hydrate rebuilds it from `tasks_iter()` below, so we do NOT set it
        // here (single builder).
        //
        // NO run-terminal is originated here. The all-skipped / empty corpus
        // finalizes through the SAME counter machinery mode-1 uses: hydrate
        // (below) runs AFTER the skip batch landed in `cluster_state`, so its
        // projection seeds every `SkippedAlreadyDone` into `completed_tasks`
        // and sets `total_tasks` from the ledger — the operational loop's
        // `completed + failed >= total_tasks` exit then trips for an
        // all-skipped corpus exactly as a fully-completed run, with no
        // single-phase-view run-terminal that a phase-chaining consumer's
        // later `on_phase_end` injection would contradict.

        // Build THIS primary's pool / total_tasks / rosters from the
        // now-seeded CRDT. The SOLE pool builder (idempotent); reused here
        // rather than duplicating the `task::mutation` discovery-rebuild. Runs
        // AFTER the skip batch above, so its `SkippedAlreadyDone → completed`
        // projection (hydrate.rs) accounts for every skip in the counter exit
        // — closing the only window where the deleted explicit `RunComplete`
        // was load-bearing (an all-skipped corpus whose skips fire NO
        // completion event on the live path).
        //
        // A composition failure here (a discovered batch carrying a duplicate
        // `(phase_id, task_id)` identity, a missing dep, or a cycle) is a
        // run-fatal during bring-up — the asm-dataset LMU run_~1429 defect.
        // Route it through the SAME terminal-verdict path the #3a/#3b
        // duplicate aborts use (`abort_run_on_invalid_composition`): latch +
        // broadcast `RunAborted` so the fleet exits on the verdict (not on
        // its setup deadline) and surface the typed `RunError`. Pre-fix
        // hydrate swallowed this (ERROR + empty pool), so the run never
        // aborted and the fleet died slowly on deadlines.
        if let Err(e) = self.hydrate_from_cluster_state() {
            return Err(self.abort_run_on_invalid_composition(e).await);
        }
        Ok(())
    }
}
