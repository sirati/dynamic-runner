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
use crate::primary::{PrimaryCoordinator, RelocationPolicy, RunError};

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Mode-2 discover-on-promotion. Fires at most once, ONLY when the CRDT
    /// declares discovery `Owed`. Runs the consumer's `discover_items`
    /// policy off the runtime thread (the closure already does the
    /// `spawn_blocking` GIL excursion — §14/§15), then originates ONE batch:
    /// `PhaseDepsSet` + one `TaskAdded` per discovered binary +
    /// `DiscoverySettled`, through the canonical broadcast/apply pipeline.
    /// On an empty discovery it additionally originates `RunComplete` (the
    /// empty-corpus terminal — no `TaskCompleted` will ever drive the
    /// counter finalize; the precedent the deleted `ingest_setup_discovery`
    /// set for the empty-discovery happy path).
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
    /// short-circuits and no policy is consulted. It is ALSO inert on a
    /// [`RelocationPolicy::RelocateToComputePeer`] primary even when it owes
    /// debt: that primary is the mode-2 bootstrap submitter, which seeded the
    /// `Owed` marker only to HAND it to the compute peer it relocates to (the
    /// submitter has no corpus + no discovery policy). Discovery is owed by the
    /// primary that STAYS — the relocate TARGET (built `StayLocal` by the
    /// promotion recipe, which inherits the `Owed` marker via its snapshot and
    /// carries the registered discovery policy), or the in-process `StayLocal`
    /// local primary. So the driver fires only on a `StayLocal` primary that
    /// owes debt; a relocating submitter hands the marker on untouched.
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
        if self.relocation_policy == RelocationPolicy::RelocateToComputePeer {
            // The mode-2 bootstrap submitter: it seeded `Owed` only to relocate
            // the role (and the marker) to a compute peer that owns the corpus
            // and the discovery policy. The submitter never discovers — the
            // `StayLocal` relocate target does, on the same `Owed` marker it
            // inherits. NO-OP here (the relocate fires at the bootstrap tail).
            return Ok(());
        }
        let Some(mut sd) = self.setup_discovery.take() else {
            // `Owed` but no policy registered = a programmer error: a primary
            // that owes discovery MUST carry the discovery policy. Hard-fail
            // rather than silently strand (which run_complete_check would
            // never exit — the counter arm is gated on `Owed`).
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
        for b in &binaries {
            batch.push(ClusterMutation::TaskAdded {
                hash: compute_task_hash(b),
                task: b.clone(),
            });
        }
        batch.push(ClusterMutation::DiscoverySettled);
        self.apply_and_broadcast_cluster_mutations(batch).await;

        // `all_binaries` is a pure derived cache of the CRDT task universe;
        // hydrate rebuilds it from `tasks_iter()` below, so we do NOT set it
        // here (single builder).
        if binaries.is_empty() {
            // Empty corpus: no `TaskCompleted` will ever fire, so the
            // counter-based finalize cannot drive the run terminal —
            // originate the terminal directly (the deleted
            // `ingest_setup_discovery` empty-discovery precedent).
            self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::RunComplete])
                .await;
        }

        // Build THIS primary's pool / total_tasks / rosters from the
        // now-seeded CRDT. The SOLE pool builder (idempotent); reused here
        // rather than duplicating the `task::mutation` discovery-rebuild.
        self.hydrate_from_cluster_state();
        Ok(())
    }
}
