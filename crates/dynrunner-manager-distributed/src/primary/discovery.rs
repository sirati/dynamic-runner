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
    /// START the mode-2 discovery future (the WHERE-it-runs split, Part 1).
    ///
    /// Gated identically to [`Self::discover_on_promotion`]: a NO-OP unless the
    /// CRDT declares `DiscoveryDebt::Owed`; the same "Owed but no policy = hard
    /// error" branch (the relocated / pre-staged recipe must
    /// `register_setup_discovery`). On `Owed` + a registered policy it MOVES the
    /// policy out of [`Self::setup_discovery`], fires it ONCE
    /// (`(sd.discover)()`) to obtain the started future, and parks that future
    /// (with the carried `phase_deps`) on [`Self::discovery_in_flight`] for the
    /// operational loop's discovery `select!` arm to poll CONCURRENTLY with
    /// every sibling arm. Idempotent: the `setup_discovery.take()` IS the
    /// fire-once latch alongside the `Owed` gate.
    ///
    /// This does NOT await the future — that is the whole point of the
    /// concurrent-arm fix: the ~6min collect-all polls as an arm while the
    /// primary stays app-alive + services secondary setup. The post-resolve
    /// seed/hydrate lives in [`Self::finish_discovery`].
    pub(crate) fn start_discovery_if_owed(&mut self) -> Result<(), RunError> {
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
        // Fire the policy ONCE to start the (collect-all) future; carry the
        // phase graph across the await so the post-resolve seed has it without
        // re-consulting the (now-dropped) policy.
        let future = (sd.discover)();
        self.discovery_in_flight = Some(crate::discovery::InFlightDiscovery {
            future,
            phase_deps: sd.phase_deps,
        });
        Ok(())
    }

    /// RESOLVE the mode-2 discovery (the post-await seed/hydrate half).
    ///
    /// Consumes the discovered `binaries` (the future's `Ok`) plus the carried
    /// `phase_deps`, and originates ONE atomic ledger batch + re-hydrates,
    /// exactly as the sequential driver did post-await. Flips `Owed → Settled`
    /// (the `DiscoverySettled` mutation in the batch). Called by the operational
    /// loop's discovery `select!` arm on the future's resolution, and by the
    /// sequential [`Self::discover_on_promotion`] composition.
    ///
    /// Originates NO run-terminal of its own — an empty / all-skipped corpus
    /// finalizes through the counter machinery once the trailing re-hydrate
    /// projects the skips into `completed_tasks`, exactly as mode-1.
    pub(crate) async fn finish_discovery(
        &mut self,
        binaries: Vec<(dynrunner_core::TaskInfo<I>, bool)>,
        phase_deps: std::collections::HashMap<
            dynrunner_core::PhaseId,
            Vec<dynrunner_core::PhaseId>,
        >,
    ) -> Result<(), RunError> {
        // Framework flagged staging (#489 P3): augment the discovered batch
        // with per-file PRE-SUCCEEDED setup tasks + the work tasks' `TaskDep`
        // gates, exactly as `originate_cold_seed` does. This is the mode-2
        // `--source-already-staged` path — the corpus is discovered POST-
        // relocate, so the SAME augmentation transform runs HERE on the
        // discovered batch (the transform is shared, like `skip_transitions`).
        // A no-op (identity) when the flag is off.
        let crate::primary::StagingAugmentation {
            batch: binaries,
            pre_succeeded: staging_pre_succeeded,
        } = crate::primary::augment_batch_for_staging(binaries, self.config.staging_strategy);

        // ONE atomic batch so "the tasks are now in the CRDT" and "debt
        // settled" land together on the wire — no window where a peer sees
        // `Settled` without the tasks or vice versa. PhaseDepsSet first (so
        // hydrate has the dep graph), then one TaskAdded per binary, then
        // DiscoverySettled (ratchets `Owed → Settled`).
        let mut batch: Vec<ClusterMutation<I>> = Vec::with_capacity(binaries.len() + 2);
        batch.push(ClusterMutation::PhaseDepsSet {
            deps: phase_deps.clone(),
        });
        for (task, _skipped) in &binaries {
            batch.push(ClusterMutation::TaskAdded {
                hash: compute_task_hash(task),
                task: task.clone(),
                // The originate stamp pass (`broadcast::stamp_def_ids`)
                // allocates the primary-owned, CRDT-agreed def id before
                // broadcast; `None` here is the un-stamped seed.
                def_id: None,
            });
        }
        // Discovery already-done partition: after EVERY discovered item is
        // seeded `Pending` by the `TaskAdded` fan-out (so `task_count` ==
        // all items), materialise the marked subset terminal
        // `SkippedAlreadyDone`. One shared helper with `originate_cold_seed`
        // — no duplicated partition logic.
        batch.extend(self.skip_transitions(&binaries));
        // Framework flagged staging (#489 P3): transition each pre-staged
        // file's setup task `Pending → SetupCompleted` — the SAME shared
        // helper the cold seed uses, so the pre-succeeded seeding has one
        // owner on both originators. Empty when the flag is off.
        batch.extend(self.setup_completed_transitions(&staging_pre_succeeded));
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

    /// Mode-2 discover-on-promotion — the SEQUENTIAL composition of
    /// [`Self::start_discovery_if_owed`] + awaiting the started future +
    /// [`Self::finish_discovery`]. Fires at most once, ONLY when the CRDT
    /// declares discovery `Owed`. Runs the consumer's `discover_items`
    /// policy off the runtime thread (the closure already does the
    /// `spawn_blocking` GIL excursion — §14/§15), then originates ONE batch:
    /// `PhaseDepsSet` + one `TaskAdded` per discovered binary +
    /// one `TaskSkippedAlreadyDone` per discovery-marked already-done item +
    /// `DiscoverySettled`, through the canonical broadcast/apply pipeline.
    ///
    /// # Sequential vs concurrent
    ///
    /// This composition AWAITS the discovery future inline — the legacy
    /// shape. The OPERATIONAL primary no longer reaches this method: its
    /// pre-loop runs [`Self::start_discovery_if_owed`] (which parks the future
    /// on `self.discovery_in_flight`) and the operational `select!`'s discovery
    /// arm awaits it CONCURRENTLY, then calls [`Self::finish_discovery`] on
    /// resolve — so the primary stays app-alive + services secondary setup
    /// during the ~6min collect-all. This sequential wrapper is retained as the
    /// single-call test surface (the unit tests that assert the end-to-end
    /// seed/settle/hydrate of one discovery pass) and as the in-one-step
    /// composition for any future caller that genuinely wants to block on
    /// discovery; the WHAT it produces is byte-for-byte identical to the
    /// concurrent path (same `start` + same `finish`), only the WHERE differs.
    ///
    /// Originates NO run-terminal of its own. An empty corpus, a
    /// 100%-already-done corpus, and a corpus with live work ALL finalize
    /// through the SAME machinery mode-1 (`originate_cold_seed`) uses: the
    /// seam ends with [`Self::hydrate_from_cluster_state`], which
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
    /// The SETUP PEER never reaches the driver — it relocates in
    /// `run_pipeline`'s `SetupPeer` arm BEFORE discover, so a setup peer that
    /// owes debt without a corpus or policy hands the `Owed` marker on
    /// untouched (the relocate happens before this point). The structural
    /// relocate-before-discover ordering — NOT a policy gate — is what keeps a
    /// policyless setup peer off the hard-error branch in
    /// [`Self::start_discovery_if_owed`].
    ///
    /// `#[cfg(test)]`: the OPERATIONAL primary reaches discovery through the
    /// concurrent op-loop arm (`start_discovery_if_owed` + the arm +
    /// `finish_discovery_arm`), NOT this sequential await — so the only live
    /// callers are the unit tests that assert one discovery pass end-to-end.
    #[cfg(test)]
    pub(crate) async fn discover_on_promotion(&mut self) -> Result<(), RunError> {
        self.start_discovery_if_owed()?;
        // `None` ⇒ the gate short-circuited (not Owed): a NO-OP, same as the
        // pre-split early-return. `Some` ⇒ Owed + policy fired: await the
        // started future inline and seed.
        let Some(in_flight) = self.discovery_in_flight.take() else {
            return Ok(());
        };
        let binaries = in_flight.future.await.map_err(RunError::Other)?;
        self.finish_discovery(binaries, in_flight.phase_deps).await
    }

    /// ON-RESOLVE body of the operational loop's discovery `select!` arm: the
    /// post-await tail that used to run SEQUENTIALLY after
    /// `discover_on_promotion` in the bring-up pre-loop, now run CONCURRENTLY
    /// when the parked discovery future resolves DURING the operational loop.
    ///
    /// `binaries` is the future's resolved batch; `phase_deps` the graph carried
    /// alongside it on [`crate::discovery::InFlightDiscovery`]. The arm has
    /// already `take`n `discovery_in_flight` (the fire-once latch) before
    /// calling this, so it cannot re-enter.
    ///
    /// Ordering (the load-bearing sequence the pre-loop preserved):
    ///   1. [`Self::finish_discovery`] — seed `PhaseDepsSet + TaskAdded* +
    ///      DiscoverySettled` + re-hydrate the pool (flips `Owed → Settled`, so
    ///      every gate keyed on `discovery_owed()` — `run_complete_check`,
    ///      `process_phase_lifecycle`, the pre-loop cascade — now opens);
    ///   2. [`Self::fire_initial_phase_starts`] — narrate + worker-demand for
    ///      the now-populated initial phases (deferred from the pre-loop, where
    ///      the phases were still phantom-empty);
    ///   3. the empty-phase cascade (`drain_empty_active_phases` +
    ///      `process_phase_lifecycle`) — same fire-before-cascade coupling as
    ///      the pre-loop, so a trivially-empty initial phase cascades `Done` and
    ///      unblocks its work-bearing dependents, and a consumer `on_phase_end`
    ///      hook can lazily inject the next phase inline;
    ///   4. EMIT `TasksAdded` onto the decoupled worker-management bus so the
    ///      operational loop's worker-mgmt arm runs `dispatch_to_idle_workers`
    ///      over the now-seeded pool — the discovery arm states "work exists"
    ///      and never dispatches directly (the dispatch-decoupling law). By the
    ///      time discovery settles the fleet has long since confirmed `MeshReady`
    ///      (it formed concurrently during the ~6min collect-all), so the
    ///      operational interleaved dispatch (`dispatch_order` least-projected-
    ///      load) distributes the corpus across the confirmed members — the
    ///      cold-formation reservation window the pre-loop `seed_bringup_
    ///      reservation` opens is moot here (no member is still forming).
    ///
    /// On a `finish_discovery` composition error (duplicate identity / missing
    /// dep / cycle) the typed `RunError` propagates to the arm, which surfaces
    /// it as the loop's `Err` exactly as the sequential driver did — the abort
    /// verdict was already latched + broadcast inside `finish_discovery`.
    pub(crate) async fn finish_discovery_arm(
        &mut self,
        binaries: Vec<(dynrunner_core::TaskInfo<I>, bool)>,
        phase_deps: std::collections::HashMap<
            dynrunner_core::PhaseId,
            Vec<dynrunner_core::PhaseId>,
        >,
        command_rx: &mut Option<
            tokio::sync::mpsc::Receiver<crate::primary::command_channel::PrimaryCommand<I>>,
        >,
    ) -> Result<(), RunError> {
        self.finish_discovery(binaries, phase_deps).await?;
        // Segment A (the initial-phase cascade) — the SAME fire-before-cascade
        // triple the non-discovery pre-loop runs, shared via one owner so the
        // coupling lives in ONE place. Runs AFTER finish_discovery flipped
        // `Owed → Settled`, so the discovery-owed gates (process_phase_lifecycle,
        // run_complete_check) are lifted and it narrates + drains the now-
        // populated ledger, not phantom-empty phases.
        self.run_initial_phase_cascade(command_rx).await;
        // The seeded pool now holds dispatchable work, but no worker will send a
        // fresh TaskRequest unprompted (they parked "no work" while the ledger
        // was empty). EMIT onto the decoupled worker-management bus so the
        // operational loop's worker-mgmt arm coalesces it into one batched
        // `dispatch_to_idle_workers` recheck over every confirmed idle worker —
        // identical to the task-backoff arm's re-dispatch trigger.
        self.cluster_state
            .emit_worker_mgmt(crate::worker_signal::WorkerMgmtSignal::TasksAdded);
        Ok(())
    }
}
