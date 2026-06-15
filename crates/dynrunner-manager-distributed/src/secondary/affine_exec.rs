//! Secondary-side RUN-ONCE-PER-SECONDARY executor for a SecondaryAffine
//! IMPORT a work task gates on (#497 P4 — the headline correctness phase).
//!
//! ## The one concern
//! Run a SecondaryAffine task `I`'s per-secondary IMPORT AT MOST ONCE on this
//! node, gating EVERY work task on this node that depends on `I` behind that
//! single run. The secondary holds NO CRDT authority for this: the import +
//! the run-once latch are NODE-LOCAL (`OperationalState::affine_done` /
//! `affine_running`, never replicated); the only CRDT-visible effects are the
//! frames this executor REPORTS to the primary
//! ([`DistributedMessage::TaskQueuedAfterLocalDependency`] when a work task is
//! queued behind the import, [`DistributedMessage::LocalDependencyReleased`]
//! when it releases, [`DistributedMessage::TaskFailed`] when the import
//! fails). The primary ORIGINATES the authoritative CRDT mutation off each
//! frame (the work-split law) — this executor never writes the CRDT.
//!
//! ## The run-once invariant (the headline — owner-emphasized)
//! Multiple work tasks on the SAME secondary depending on the SAME
//! not-yet-imported `I` must run `I` EXACTLY ONCE; ALL of them enter
//! `QueuedAfterLocalDependency`. The latch is the PRESENCE of `I`'s hash in
//! `affine_running`:
//!   * 1st dependent → inserts `affine_running[I] = vec![dep]`, reports the
//!     queued state, spawns the ONE import.
//!   * 2nd..Nth dependent → APPENDS to the existing vec, reports the queued
//!     state, starts NO second import.
//!   * import `Ok` → drains the vec, inserts `I` into `affine_done`, sends one
//!     `LocalDependencyReleased` per queued dependent (→ each `B` `InFlight`).
//!   * import `Err` → sends one `TaskFailed{class}` per queued dependent
//!     (re-routable per #495), does NOT set `affine_done` (a later assignment
//!     / another secondary retries its OWN import — the done set is never
//!     poisoned).
//!
//! ## Module boundary (CLAUDE.md design-first)
//!   * Owner: the secondary. The seam it crosses is the EXISTING dispatch
//!     router (Phase 5), which calls [`Self::ensure_affine_import`] for a work
//!     task whose SecondaryAffine dependency is locally unmet — a one-line
//!     delegate; no run-once / queue logic in the router.
//!   * Execution body: the role-agnostic [`execute_affine_with_action`] core
//!     (read the task, run the action with a bounded transient retry, classify
//!     the outcome). The wrapper adds ONLY the secondary-specific parts:
//!     resolving `I`'s `TaskInfo` from the local replicated `cluster_state`,
//!     and routing each queued dependent's release / failure to the primary.
//!   * What callers see: `ensure_affine_import(affine_hash, dep)` →
//!     [`AffineGateOutcome`]. The router never learns the run-once latch, the
//!     queue, or the import.

use dynrunner_core::{Identifier, TaskInfo};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::wire::timestamp_now;
use super::{AffineGateOutcome, PendingAffineDependent, SecondaryCoordinator};
use crate::affine_action::{IMPORT_OUTER_RETRIES, ImportActionHandle, ImportError};

/// Completion event delivered back to the secondary's operational `select!`
/// loop when an OFF-LOOP affine import finishes (#497 P5).
///
/// The import itself (`nix-store --import` of GBs) is driven on a detached
/// `spawn_local` task off a [`AffineGateOutcome::StartedRun`], NOT inline on
/// the coordinator loop — running it inline would hold the loop across the
/// whole multi-GB import await and starve every sibling arm (the starvation
/// class #476-#478). The spawned task computes the classified
/// [`AffineOutcome`] and sends it back through the coordinator-held
/// `affine_import_tx` channel; the loop's [`Self::complete_affine_import`]
/// handler then runs the on-loop release (drain the queued dependents, set
/// `affine_done`, dispatch each released `B` onto its worker). This MIRRORS
/// the worker-completion mechanism: each worker monitor task pushes a
/// `WorkerEvent` through the pool's `event_tx` and the pool arm receives it —
/// the import drive is the same off-loop-compute / on-loop-apply split.
#[derive(Debug)]
pub(crate) struct AffineImportComplete {
    /// The SecondaryAffine task hash whose single per-secondary import just
    /// finished — the key into `affine_running` the on-loop handler drains.
    pub(in crate::secondary) affine_hash: String,
    /// The classified import result. `Success` releases every queued
    /// dependent (`LocalDependencyReleased` + on-worker dispatch) and marks
    /// the hash `affine_done`; `Failure` fails each dependent with the #495
    /// class and leaves the done set untouched (no poison).
    pub(in crate::secondary) outcome: AffineOutcome,
}

/// The classified result of running a SecondaryAffine import in-process, after
/// the bounded transient retry. Mirrors
/// [`crate::setup_exec::SetupOutcome`]: the role wrapper maps `Success` onto
/// the release path (one `LocalDependencyReleased` per queued dependent) and
/// `Failure` onto the per-dependent `TaskFailed` path, carrying the #495
/// failure class so the authority can re-route (Recoverable) or cascade
/// (NonRecoverable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::secondary) enum AffineOutcome {
    /// The import completed cleanly — the affine hash is now locally done.
    Success,
    /// The import failed; carries the #495 class to stamp on each queued
    /// dependent's `TaskFailed` terminal and the operator-facing reason.
    Failure {
        error_type: dynrunner_core::ErrorType,
        reason: String,
    },
}

/// The ASYNC execution path the secondary's run-once executor calls for the
/// single per-secondary import of a SecondaryAffine task — #497 P4's role-
/// agnostic core, mirroring [`crate::setup_exec::execute_setup_with_upload`].
///
/// Dispatch by the registered action:
///   * An `ImportAction` IS registered ⇒ run the import (with a bounded OUTER
///     retry on a `Transient` the provider could not absorb — see
///     [`IMPORT_OUTER_RETRIES`]; any per-step retry lives in the provider).
///     Provider `Ok` ⇒ `Success`; `Recoverable`/`NonRecoverable` ⇒ `Failure`
///     with that class; `Transient` exhausted ⇒ `Failure { Recoverable }`
///     (the import could not complete on THIS node, so re-route per #495).
///   * NO action is registered ⇒ `Failure { NonRecoverable }` (the executor
///     was asked to import but has no importer — a wiring error, surfaced
///     loudly rather than silently succeeded; a work task WITHOUT an affine
///     dependency never reaches here).
pub(in crate::secondary) async fn execute_affine_with_action<I>(
    task: &TaskInfo<I>,
    action: &ImportActionHandle<I>,
) -> AffineOutcome
where
    I: Identifier,
{
    let Some(importer) = action.as_ref() else {
        return AffineOutcome::Failure {
            error_type: dynrunner_core::ErrorType::NonRecoverable,
            reason: format!(
                "work task gates on SecondaryAffine import '{}' but no import \
                 action is registered on this secondary — wiring error",
                task.task_id,
            ),
        };
    };
    // Bounded OUTER retry: the provider owns any per-step transient retry, so
    // this only re-attempts a WHOLE-action transient the provider could not
    // absorb. A Recoverable / NonRecoverable failure short-circuits (no
    // retry) — its class is preserved.
    let mut last_transient: Option<String> = None;
    for attempt in 0..=IMPORT_OUTER_RETRIES {
        match importer.import(task).await {
            Ok(()) => return AffineOutcome::Success,
            Err(e) if e.is_transient() => {
                // Per-attempt RETRY-LOOP decision noise (trace, not info): the
                // standalone, operator-meaningful event is the FINAL outcome
                // of the bounded retry (the Success arm above, or the
                // Recoverable exhaustion fall-through below) — both lifted to
                // info on the release seam. A mid-loop transient that the
                // outer retry absorbs is decision-internal observability.
                tracing::trace!(
                    target: "dynrunner_affine",
                    task_id = %task.task_id,
                    attempt,
                    reason = %e.reason(),
                    "secondary-affine import hit a transient failure; re-attempting"
                );
                last_transient = Some(e.reason().to_string());
            }
            Err(e) => {
                return AffineOutcome::Failure {
                    error_type: e.error_type(),
                    reason: format!(
                        "secondary-affine import of '{}' failed: {}",
                        task.task_id,
                        e.reason(),
                    ),
                };
            }
        }
    }
    // Transient exhaustion folds into a Recoverable work-task failure: the
    // import could not complete on THIS node, but another may import cleanly.
    AffineOutcome::Failure {
        error_type: ImportError::Transient(String::new()).error_type(),
        reason: format!(
            "secondary-affine import of '{}' failed after {} transient attempts: {}",
            task.task_id,
            IMPORT_OUTER_RETRIES + 1,
            last_transient.unwrap_or_else(|| "unknown transient fault".to_string()),
        ),
    }
}

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Consult the OPTIONAL per-(gate,node) satisfied probe (#537) and, on a
    /// `Satisfied` verdict, SEED `affine_done` so this dependent (and every
    /// subsequent dependent for the rest of the run) short-circuits the
    /// run-once executor entirely. Returns `true` iff the caller should now
    /// take the `AlreadyDone` path (no queue, no report, no spawn_local).
    ///
    /// Five-way decision (the only path that calls the consumer probe is the
    /// cache-miss / cache-expired branch):
    ///   * No probe handle registered (the default) ⇒ `false`. Today's
    ///     behaviour bit-for-bit; the executor runs the unchanged
    ///     `ImportAction` path.
    ///   * Gate body NOT resolvable on this node (the #509 sync race —
    ///     `TaskAdded` outran the assignment) ⇒ `false`. Probing an
    ///     unresolved gate is meaningless; the unchanged path delivers
    ///     today's Recoverable absent verdict from `drive_affine_import`.
    ///   * Cache hit `ProbeOutcome::Satisfied` ⇒ seed `affine_done` +
    ///     `true`. (Defensive: a `Satisfied` verdict ALSO populated
    ///     `affine_done`, so the caller would have short-circuited on the
    ///     outer `affine_done.contains` check; this branch is reached only
    ///     if someone cleared `affine_done` between calls.)
    ///   * Cache hit fresh `NotSatisfied` / `Errored` ⇒ `false`. The probe
    ///     was consulted recently and said "not yet"; respect the TTL and
    ///     skip the call to keep probe-call frequency bounded on a
    ///     thousand-dependents-per-gate fleet.
    ///   * Cache miss / expired ⇒ CALL the probe under `catch_unwind` (an
    ///     `Errored` verdict on panic; the Python bridge converts an
    ///     exception into `false` plus a warn log already, so a panic here
    ///     is the rare native-bug case). Cache the verdict; a `Satisfied`
    ///     also seeds `affine_done`.
    ///
    /// SYNC by design (the trait's `is_satisfied` is sync): the whole
    /// short-circuit's value proposition is avoiding async / spawn_local
    /// scaffolding, so the probe consult must stay on the operational loop
    /// without a second seam. The probe contract caps the call cost at an
    /// FS stat.
    fn try_short_circuit_via_probe(&mut self, affine_hash: &str) -> bool {
        // Cache hit on a fresh verdict — never call the probe.
        let now = std::time::Instant::now();
        if let Some(cached) = self
            .lifecycle
            .operational_ref()
            .and_then(|op| op.affine_probe_cache.get(affine_hash))
            .cloned()
            && cached.is_fresh(now)
        {
            if matches!(cached, crate::affine_satisfied::ProbeOutcome::Satisfied) {
                self.op_mut().affine_done.insert(affine_hash.to_string());
                return true;
            }
            return false;
        }
        // No probe registered — the overwhelming case for consumers that
        // never opted in; today's behaviour bit-for-bit.
        let Some(probe) = self.affine_satisfied_probe.clone() else {
            return false;
        };
        // Gate body NOT resolvable: do not probe. The #509 absent-gate
        // verdict (delivered later by `drive_affine_import`) is the right
        // re-route signal — probing an unresolved gate would have nothing
        // to ask about.
        let Some(task) = self.cluster_state.affine_gate_task(affine_hash) else {
            return false;
        };
        // Call the probe under `catch_unwind` so a panicking native probe
        // is classified `Errored` (cached briefly so a persistently-faulty
        // probe is not hammered) rather than propagating up and tearing
        // down the dispatch loop. `AssertUnwindSafe` is sound: the probe's
        // `&self` (Arc) and the borrowed `&TaskInfo` are not mutated by
        // the unwind.
        let verdict = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            probe.is_satisfied(&task)
        }));
        let outcome = match verdict {
            Ok(true) => crate::affine_satisfied::ProbeOutcome::Satisfied,
            Ok(false) => crate::affine_satisfied::ProbeOutcome::NotSatisfied { cached_at: now },
            Err(_panic) => {
                tracing::warn!(
                    target: "dynrunner_affine",
                    affine_hash = %affine_hash,
                    "affine satisfied probe panicked; classified Errored and \
                     falling through to the import path (probe cached briefly \
                     to avoid hammering a persistently-faulty probe)"
                );
                crate::affine_satisfied::ProbeOutcome::Errored { cached_at: now }
            }
        };
        let satisfied =
            matches!(outcome, crate::affine_satisfied::ProbeOutcome::Satisfied);
        self.op_mut()
            .affine_probe_cache
            .insert(affine_hash.to_string(), outcome);
        if satisfied {
            // Standalone lifecycle event: a FRESH probe verdict identified
            // this node as the gate's producer — `affine_done` is seeded and
            // every subsequent dependent for the rest of the run short-circuits
            // through the outer `affine_done.contains` check (the probe is NOT
            // re-consulted, the cache-hit `Satisfied` path stays silent — the
            // verdict is `Satisfied` forever). Fires at most ONCE per
            // (node, gate) by construction.
            tracing::info!(
                target: "dynrunner_affine",
                affine_hash = %affine_hash,
                "satisfied probe identified this node as gate producer; \
                 seeding affine_done (the import scaffolding is skipped)"
            );
            self.op_mut().affine_done.insert(affine_hash.to_string());
        }
        satisfied
    }

    /// Gate a work task `B` on its locally-unmet SecondaryAffine dependency
    /// `I` (#497 P4 — the run-once entry point the dispatch router calls).
    ///
    /// Partitions on the node-local sets:
    ///   * `affine_hash ∈ affine_done` → [`AffineGateOutcome::AlreadyDone`]:
    ///     the import already ran on this node; the caller releases `B`
    ///     straight to `InFlight` (no queue, no report, no import).
    ///   * `affine_hash ∈ affine_running` → APPEND `dependent` to the existing
    ///     queue, report `TaskQueuedAfterLocalDependency` for `B`, start NO
    ///     second import → [`AffineGateOutcome::QueuedBehindRun`].
    ///   * otherwise (FIRST dependent) → insert `affine_running[hash] =
    ///     vec![dependent]`, report `TaskQueuedAfterLocalDependency` for `B`,
    ///     run EXACTLY ONE import → [`AffineGateOutcome::StartedRun`].
    ///
    /// The PRESENCE of the hash key in `affine_running` is the run-once latch:
    /// only the first call (which finds neither set populated) returns
    /// [`AffineGateOutcome::StartedRun`], TELLING the caller it is the one
    /// that must now drive the single import ([`Self::drive_affine_import`] in
    /// production, off the loop; [`Self::run_affine_import_once`] inline in the
    /// executor-level tests); every concurrent / later dependent on the SAME
    /// hash finds the key present and only queues
    /// ([`AffineGateOutcome::QueuedBehindRun`]).
    ///
    /// ## Why the gate is SYNCHRONOUS (does not await the import)
    /// This method performs ONLY the synchronous run-once decision (check the
    /// node-local sets, append the dependent, report the queued state) and
    /// returns WITHOUT running the import. Driving the import here would hold
    /// `&mut self` across the (blocking) import await, so the 2nd..Nth
    /// dependent assignments could not be processed and queued WHILE the
    /// single import is in flight — exactly the run-once-under-concurrency
    /// invariant. The caller drives the import off a `StartedRun` (in
    /// production, OFF the coordinator loop via [`Self::drive_affine_import`]),
    /// decoupled from the queue-and-report gate, so ALL N dependents enter
    /// `QueuedAfterLocalDependency` behind the ONE run.
    // Reached via the dispatch router's TaskAssignment arm
    // ([`Self::try_gate_on_affine_import`], #497 P5) for a work task whose
    // SecondaryAffine dependency is locally unmet; also driven directly by the
    // executor-level Phase-4 tests with a stub ImportAction.
    pub(in crate::secondary) async fn ensure_affine_import(
        &mut self,
        affine_hash: String,
        dependent: PendingAffineDependent<I>,
    ) -> Result<AffineGateOutcome, String> {
        // Already locally imported: release straight to InFlight. No queue, no
        // report — the caller (router) re-emits the standard assignment. A
        // `Satisfied` verdict from the #537 satisfied-probe ALSO populates
        // this set, so the second..Nth dependent on a producer-resolved gate
        // short-circuits HERE without re-consulting the probe (and the cache
        // never even loads), exactly like a previously-imported gate.
        if self.op_mut().affine_done.contains(&affine_hash) {
            return Ok(AffineGateOutcome::AlreadyDone);
        }

        // #537 — consult the OPTIONAL per-(gate,node) satisfied probe BEFORE
        // touching the run-once latch. A `Satisfied` verdict means this node
        // is the PRODUCER (the consumer's `build_common_dep` already left the
        // closure valid in the local store): SEED `affine_done` so EVERY
        // subsequent dependent — for the rest of the run — short-circuits on
        // the `affine_done.contains` check above, AND return `AlreadyDone`
        // for THIS dependent (no queue, no report, no spawn_local — zero
        // executor scaffolding on the wire). Anything but `Satisfied`
        // (no probe registered / probe says no / probe raised / gate body
        // not yet resolvable for the #509 sync race) falls through to the
        // unchanged run-once path below.
        if self.try_short_circuit_via_probe(&affine_hash) {
            return Ok(AffineGateOutcome::AlreadyDone);
        }

        // Whether THIS call is the first dependent (it must drive the single
        // import) or a 2nd..Nth (it only queues). The vacancy of the
        // `affine_running` key is the discriminator — checked + claimed under
        // one borrow so two near-simultaneous calls cannot both claim "first".
        let is_first = !self.op_mut().affine_running.contains_key(&affine_hash);
        let work_hash = dependent.work_hash.clone();
        self.op_mut()
            .affine_running
            .entry(affine_hash.clone())
            .or_default()
            .push(dependent);

        // Every queued dependent (first or not) reports the CRDT-visible
        // queued state, so the primary/observer SEE `B` waiting on the local
        // import (never silently stuck mid-InFlight). The primary originates
        // `QueuedAfterLocalDependencySet` off this frame. Clone the hash so it
        // remains available for the standalone lifecycle emit below.
        self.report_queued_after_local_dependency(work_hash, affine_hash.clone())
            .await?;

        // The import itself is driven by the caller off a `StartedRun` (the
        // synchronous-gate rationale above): exactly one dependent per (node,
        // affine hash) sees the key vacant.
        if is_first {
            // Standalone lifecycle event: this node just kicked off its single
            // per-secondary import for the gate (`StartedRun` fires exactly
            // ONCE per (node, gate) by virtue of the `affine_running` latch).
            // Pairs with the `complete_affine_import` "finished" emit below
            // and the primary's `AffineReady` emission for end-to-end gate-
            // lifecycle visibility.
            tracing::info!(
                target: "dynrunner_affine",
                affine_hash = %affine_hash,
                "secondary-affine import started (first dependent on this node)"
            );
            Ok(AffineGateOutcome::StartedRun)
        } else {
            Ok(AffineGateOutcome::QueuedBehindRun)
        }
    }

    /// Run the SINGLE per-secondary import for `affine_hash` and drain every
    /// dependent queued behind it (#497 P4). Called EXACTLY ONCE per import,
    /// from the first-dependent branch of [`Self::ensure_affine_import`].
    ///
    /// On `Success`: mark the hash locally-done, take the queued dependents,
    /// and send one `LocalDependencyReleased` per dependent (→ the primary
    /// originates `TaskAssigned` → `B` `InFlight`). On `Failure`: send one
    /// `TaskFailed{class}` per dependent (re-routable per #495) and do NOT set
    /// `affine_done` — a later assignment / another secondary retries its OWN
    /// import (the done set is never poisoned).
    ///
    /// ## Inline vs off-loop (the #497 P5 split)
    /// This method drives the import INLINE (`.await` on the caller's task)
    /// and is the EXECUTOR-LEVEL test driver (Phase-4 tests, with an in-process
    /// stub importer). The PRODUCTION dispatch path does NOT call it: the
    /// router's gate ([`Self::try_gate_on_affine_import`]) spawns the import
    /// OFF the coordinator loop ([`Self::drive_affine_import`]) so the multi-GB
    /// import never blocks the loop, and the completion arm calls
    /// [`Self::complete_affine_import`] — the SAME release body this method
    /// delegates to. Keeping the resolve-then-import-then-release shape in one
    /// place lets the inline-test and off-loop-production paths share the exact
    /// release logic (no divergence). Test-only because production never
    /// imports inline.
    #[cfg(test)]
    pub(in crate::secondary) async fn run_affine_import_once(
        &mut self,
        affine_hash: String,
        factory: &mut impl dynrunner_manager_local::WorkerFactory<M>,
    ) -> Result<(), String> {
        // Resolve `I`'s TaskInfo over the FULL LOGICAL ledger (fat OR the
        // spilled settled record) — the resolution twin of the spill-safe
        // gate DETECTION (`unmet_local_affine_dep`). CLONE it out so the
        // `cluster_state` borrow ends before the async import runs against
        // `&self.import_action`. A hash in NEITHER half is the #509 sync race
        // (TaskAdded not yet synced): fail every queued dependent
        // RECOVERABLY (re-routable per #495) rather than dropping the build.
        let task = self.cluster_state.affine_gate_task(&affine_hash);

        let outcome = match task {
            Some(task) => execute_affine_with_action(&task, &self.import_action).await,
            None => Self::affine_gate_absent_failure(&affine_hash),
        };

        self.complete_affine_import(affine_hash, outcome, factory)
            .await
    }

    /// The TRANSIENT verdict used when a SecondaryAffine gate's body cannot
    /// be resolved over the FULL LOGICAL ledger at import time — the gate is
    /// in NEITHER the fat map NOR the settled index, so the build's
    /// ASSIGNMENT frame outran the gate's `TaskAdded` CRDT propagation to
    /// this node (#509). This is a transient SYNC RACE, NOT a permanent
    /// fault: a `Recoverable` verdict re-routes each queued dependent per
    /// #495 (`report_deferred_task_failed` → the primary re-injects into the
    /// pool, BOUNDED by the per-phase `retry_max_passes` budget), so on
    /// re-assignment — once the gate's `TaskAdded` has synced — the build is
    /// gated behind the import and runs. A NonRecoverable here was the #509
    /// bug: the primary does NOT re-route a NonRecoverable, so a build lost
    /// the race and was permanently dropped. A gate that genuinely NEVER
    /// appears fails-final after the budget, never silently lost and never
    /// looped — the SAME Recoverable-vs-NonRecoverable shape as
    /// [`Self::report_unresolvable_task`] (#495).
    ///
    /// A SPILLED gate is NOT absent — its body is read back from the spill
    /// file by [`crate::cluster_state::ClusterState::affine_gate_task`], so
    /// the import proceeds normally and never reaches this verdict. Shared by
    /// the inline ([`Self::run_affine_import_once`]) and off-loop
    /// ([`Self::drive_affine_import`]) drives so the absent-gate verdict has
    /// one definition.
    pub(in crate::secondary) fn affine_gate_absent_failure(affine_hash: &str) -> AffineOutcome {
        AffineOutcome::Failure {
            error_type: dynrunner_core::ErrorType::Recoverable,
            reason: format!(
                "SecondaryAffine gate '{affine_hash}' not yet synced to the \
                 local ledger (assignment outran its TaskAdded) — re-routing \
                 its queued dependents (Recoverable) so they retry once the \
                 gate's TaskAdded arrives",
            ),
        }
    }

    /// Drain every dependent queued behind `affine_hash`'s single import and
    /// apply the classified `outcome` (#497 P4 release body / P5 completion
    /// handler). Runs ON the coordinator loop — both the inline
    /// [`Self::run_affine_import_once`] and the off-loop completion arm reach
    /// the run-once release through here, so the two drives share one body.
    ///
    /// On `Success`: mark the hash locally-done (so any FUTURE dependent
    /// releases immediately), then per dependent BOTH (a) report
    /// `LocalDependencyReleased` (→ the primary originates the EXISTING
    /// `TaskAssigned` → `B` `InFlight`, pinning the same `(secondary,
    /// worker)`) AND (b) dispatch `B` onto its worker NOW
    /// ([`Self::dispatch_released_affine_dependent`]) — the deferred
    /// assignment the gate withheld. On `Failure`: fail each dependent with
    /// the import's #495 class (re-routable per #495) and leave `affine_done`
    /// untouched (no poison) so a fresh `ensure_affine_import` starts a new
    /// single run.
    pub(in crate::secondary) async fn complete_affine_import(
        &mut self,
        affine_hash: String,
        outcome: AffineOutcome,
        factory: &mut impl dynrunner_manager_local::WorkerFactory<M>,
    ) -> Result<(), String> {
        // Drain the queued dependents for THIS hash, clearing the run-once
        // latch. On success the hash also enters `affine_done` so any FUTURE
        // dependent releases immediately; on failure the done set is left
        // untouched (no poison) so a fresh `ensure_affine_import` starts a new
        // single run.
        let dependents = self
            .op_mut()
            .affine_running
            .remove(&affine_hash)
            .unwrap_or_default();
        if matches!(outcome, AffineOutcome::Success) {
            self.op_mut().affine_done.insert(affine_hash.clone());
        }

        // Standalone lifecycle event: the single per-secondary import for this
        // gate just finished and the run-once latch is being drained — releases
        // every queued dependent (Success) or fails them with the #495 class
        // (Failure). Fires exactly ONCE per (node, gate, import attempt); pairs
        // with the `ensure_affine_import` "started" emit above.
        match &outcome {
            AffineOutcome::Success => tracing::info!(
                target: "dynrunner_affine",
                affine_hash = %affine_hash,
                queued_dependents = dependents.len(),
                "secondary-affine import completed; releasing queued dependents"
            ),
            AffineOutcome::Failure { error_type, reason } => tracing::info!(
                target: "dynrunner_affine",
                affine_hash = %affine_hash,
                queued_dependents = dependents.len(),
                error_type = ?error_type,
                reason = %reason,
                "secondary-affine import failed; failing queued dependents (re-routable per #495 on Recoverable)"
            ),
        }

        for dependent in dependents {
            match &outcome {
                AffineOutcome::Success => {
                    // Release `B`: the primary originates the EXISTING
                    // `TaskAssigned` (→ `B` `InFlight`) off this frame — NOT a
                    // second InFlight originator — pinning the same
                    // `(secondary, worker)` pair this node chose.
                    self.report_local_dependency_released(
                        dependent.work_hash.clone(),
                        dependent.worker_id,
                    )
                    .await?;
                    // Actually dispatch `B` onto its worker now: the gate
                    // withheld the assignment when `B` queued; the import is
                    // done, so the deferred dispatch runs here (mirrors the
                    // `pending_first_bind` post-Ready dispatch).
                    self.dispatch_released_affine_dependent(dependent, factory)
                        .await?;
                }
                AffineOutcome::Failure { error_type, reason } => {
                    // Fail `B` with the import's #495 class, reusing the
                    // worker-terminal `TaskFailed` report path. Recoverable ⇒
                    // re-routable to a secondary that runs its OWN import.
                    self.report_deferred_task_failed(
                        dependent.worker_id,
                        &dependent.work_hash,
                        error_type.clone(),
                        reason.clone(),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    /// Report a work task `B` as QUEUED behind this node's local SecondaryAffine
    /// import (#497 P4) — the secondary half of the queued state (the primary
    /// originates `QueuedAfterLocalDependencySet`). Reuses the same
    /// `send_to_primary` chokepoint every primary-bound report uses.
    async fn report_queued_after_local_dependency(
        &mut self,
        task_hash: String,
        affine_hash: String,
    ) -> Result<(), String> {
        let report = DistributedMessage::TaskQueuedAfterLocalDependency {
            target: None,
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            task_hash,
            affine_hash,
        };
        self.send_to_primary(report).await
    }

    /// Report that this node's local SecondaryAffine import for the task queued
    /// as `task_hash` is DONE — release it (#497 P4). The primary originates
    /// the EXISTING `TaskAssigned` off this frame, pinning the same
    /// `(secondary, worker)` pair this node chose.
    async fn report_local_dependency_released(
        &mut self,
        task_hash: String,
        worker_id: dynrunner_core::WorkerId,
    ) -> Result<(), String> {
        let report = DistributedMessage::LocalDependencyReleased {
            target: None,
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            task_hash,
            worker_id,
        };
        self.send_to_primary(report).await
    }

    /// The hash of the FIRST of `binary`'s dependencies whose ledger entry is
    /// a SecondaryAffine gate that has RESOLVED (`AffineReady`) but whose
    /// per-secondary import this node has NOT yet run (`∉ affine_done`) — the
    /// locally-unmet affine dependency that gates `B`'s assignment (#497 P5).
    /// `None` ⇒ `B` has no unmet local affine dep, so the normal worker-
    /// dispatch path runs unchanged.
    ///
    /// Resolution mirrors the primary-side gate detector
    /// ([`crate::cluster_state::ClusterState::task_hash_for_dep`]): each
    /// `TaskDep` `(phase_id, task_id)` resolves to the dep's content hash,
    /// then the dep must be a RESOLVED SecondaryAffine import gate per
    /// [`crate::cluster_state::ClusterState::is_affine_ready_gate`]. A dep that is not a
    /// SecondaryAffine gate, not yet `AffineReady`, or already locally
    /// imported is skipped. Keyed on the NODE-LOCAL `affine_done` set, so
    /// EVERY worker on this node gates on the SAME single import (the
    /// run-once latch lives in `affine_running`).
    ///
    /// Gate DETECTION is over the FULL logical ledger (fat OR the slim
    /// settled index), NOT the live-only `task_state` read it superseded:
    /// `AffineReady` is the gate's join fixed-point and is SETTLE-ELIGIBLE,
    /// so once it spills, `task_state` returns `None` and the live-only
    /// check went blind — a build dispatched onto a not-yet-imported node
    /// AFTER the spill would SKIP the import and fail (#497 P5 hole). The
    /// settled index keeps the `SettledClass::AffineReady` fact (no disk
    /// read), so the gate stays visible through the spill and the late-join
    /// snapshot restore alike.
    ///
    /// Read-only over the replicated `cluster_state` + the node-local
    /// `affine_done`; returns an OWNED hash so the `cluster_state` borrow ends
    /// at the call site.
    pub(in crate::secondary) fn unmet_local_affine_dep(
        &self,
        binary: &TaskInfo<I>,
    ) -> Option<String> {
        binary.task_depends_on.iter().find_map(|dep| {
            let dep_hash = self
                .cluster_state
                .task_hash_for_dep(&dep.phase_id, dep.task_id.as_str())?;
            // The dep must be the resolved SecondaryAffine gate `AffineReady`,
            // answered over fat OR settled state so a spilled gate stays
            // visible. A SecondaryAffine task only ever sits Blocked / Pending
            // (not yet ready — `B` would not have been dispatched) or
            // AffineReady (ready), so this isolates exactly the
            // ready-but-not-locally-imported gate.
            let is_ready_affine_gate = self.cluster_state.is_affine_ready_gate(dep_hash);
            // `affine_done` lives on `OperationalState`; this read is only
            // reached on the operational dispatch path, so a `None` here (not
            // yet Operational) correctly reads as "not locally imported".
            let locally_done = self
                .lifecycle
                .operational_ref()
                .is_some_and(|op| op.affine_done.contains(dep_hash));
            (is_ready_affine_gate && !locally_done).then(|| dep_hash.to_string())
        })
    }

    /// The dispatch-router intercept (#497 P5): gate a work task `B`'s
    /// assignment on its locally-unmet SecondaryAffine import. Returns `true`
    /// when `B` was GATED (queued behind the import — the caller must NOT
    /// proceed to worker dispatch) and `false` when `B` has no unmet local
    /// affine dep (the caller runs the UNCHANGED dispatch path).
    ///
    /// On a locally-unmet dep, builds the [`PendingAffineDependent`] (carrying
    /// everything the release dispatch needs) and runs the SYNCHRONOUS run-once
    /// claim-or-queue ([`Self::ensure_affine_import`]):
    ///   * `AlreadyDone` → the import already ran on this node; `B` is NOT
    ///     gated (returns `false`) so the caller dispatches it immediately.
    ///   * `QueuedBehindRun` → `B` queued behind the in-flight import; no
    ///     second import. Returns `true` (gated).
    ///   * `StartedRun` → `B` is the first dependent; drive the SINGLE import
    ///     OFF the coordinator loop ([`Self::drive_affine_import`]) and return
    ///     `true` (gated). The import never blocks the loop.
    ///
    /// The router never learns the run-once latch, the queue, or the import —
    /// this is the one seam it crosses.
    pub(in crate::secondary) async fn try_gate_on_affine_import(
        &mut self,
        worker_id: dynrunner_core::WorkerId,
        binary: &TaskInfo<I>,
        estimated: &dynrunner_core::ResourceMap,
        predecessor_outputs: &std::collections::BTreeMap<String, dynrunner_core::TaskOutputs>,
        work_hash: &str,
    ) -> Result<bool, String> {
        let Some(affine_hash) = self.unmet_local_affine_dep(binary) else {
            return Ok(false);
        };
        let dependent = PendingAffineDependent {
            work_hash: work_hash.to_string(),
            worker_id,
            binary: binary.clone(),
            estimated: estimated.clone(),
            predecessor_outputs: predecessor_outputs.clone(),
        };
        match self.ensure_affine_import(affine_hash.clone(), dependent).await? {
            // The import already ran on this node — `B` is not gated; the
            // caller dispatches it on the normal path immediately.
            AffineGateOutcome::AlreadyDone => Ok(false),
            // `B` queued behind the single in-flight import — no second run.
            AffineGateOutcome::QueuedBehindRun => Ok(true),
            // `B` is the FIRST dependent: drive the ONE import off the loop.
            AffineGateOutcome::StartedRun => {
                self.drive_affine_import(affine_hash);
                Ok(true)
            }
        }
    }

    /// Drive the SINGLE per-secondary import for `affine_hash` OFF the
    /// coordinator loop (#497 P5 — the load-bearing decoupling). The import
    /// (`nix-store --import` of GBs) is run on a detached [`spawn_local`] task
    /// so it never blocks the operational `select!` loop — running it inline
    /// would hold the loop across the whole multi-GB await and starve every
    /// sibling arm (the starvation class #476-#478 fixed). The spawned task
    /// computes the classified [`AffineOutcome`] and sends an
    /// [`AffineImportComplete`] back through `affine_import_tx`; the loop's
    /// completion arm ([`Self::complete_affine_import`]) then runs the on-loop
    /// release. This MIRRORS the worker-completion mechanism (a monitor task
    /// pushes a `WorkerEvent` through the pool's `event_tx`; the pool arm
    /// receives it).
    ///
    /// Task resolution happens HERE (on the loop, reading `cluster_state`) so
    /// the cloned `TaskInfo` + the cloned `Arc<dyn ImportAction>` (both
    /// moveable) cross into the spawned task without holding any borrow. The
    /// gate body is resolved over the FULL LOGICAL ledger
    /// ([`crate::cluster_state::ClusterState::affine_gate_task`] — fat OR the
    /// spilled settled record), mirroring the spill-safe gate DETECTION, so a
    /// gate that resolved-then-SPILLED still imports (the fat-only read went
    /// blind). A gate in NEITHER half is the #509 sync race (TaskAdded not yet
    /// synced) delivered as a RECOVERABLE completion (no spawn — there is
    /// nothing yet to import), so its queued dependents are RE-ROUTED (per
    /// #495) by the SAME release body, retrying once the gate syncs.
    ///
    /// [`spawn_local`]: tokio::task::spawn_local
    pub(in crate::secondary) fn drive_affine_import(&mut self, affine_hash: String) {
        let task = self.cluster_state.affine_gate_task(&affine_hash);
        let tx = self.affine_import_tx.clone();

        match task {
            Some(task) => {
                // The action handle is `Option<Arc<dyn ImportAction>>` — the
                // `Arc` is Send+Sync (moveable); the import FUTURE is `?Send`,
                // so the detached task is `spawn_local` (the secondary runs
                // LocalSet-bound, like every other secondary off-loop task).
                let action = self.import_action.clone();
                tokio::task::spawn_local(async move {
                    let outcome = execute_affine_with_action(&task, &action).await;
                    // The receiver lives on the coordinator for the whole
                    // operational span; a send error only after teardown.
                    let _ = tx.send(AffineImportComplete {
                        affine_hash,
                        outcome,
                    });
                });
            }
            None => {
                // Gate body in NEITHER the fat map nor the settled index —
                // the #509 sync race. No body to import yet; deliver the
                // RECOVERABLE absent verdict straight to the completion arm,
                // which re-routes the queued dependents (per #495) so they
                // retry once the gate's TaskAdded syncs.
                //
                // Diagnosability (#514): log the gate content-hash this node is
                // LOOKING FOR but cannot resolve. Pairs with the emission-side
                // `gate_content_hash` log on the AffineReady transition: an
                // operator greps both and disambiguates an ABSENT gate (no
                // emission line ever fired for this hash) from a HASH-MISMATCH
                // (the emission fired for a DIFFERENT hash) — different
                // remediations.
                tracing::warn!(
                    target: "dynrunner_affine",
                    looking_for_gate_content_hash = %affine_hash,
                    "SecondaryAffine gate body NOT resolvable on this node (in \
                     neither the fat ledger nor the settled index); delivering a \
                     RECOVERABLE absent verdict so its queued dependents re-route"
                );
                let _ = tx.send(AffineImportComplete {
                    outcome: Self::affine_gate_absent_failure(&affine_hash),
                    affine_hash,
                });
            }
        }
    }

    /// Dispatch a released SecondaryAffine dependent `B` onto its worker now
    /// that the per-secondary import has finished (#497 P5). The gate withheld
    /// `B`'s worker binding when it queued (the intercept runs BEFORE worker
    /// selection); on the import's success the release re-enters the SAME
    /// selection + per-type-ensure + assign path every normal assignment uses
    /// ([`Self::assign_resolved_task`]) — no duplicated worker-binding logic.
    ///
    /// Re-checks the run-aborted gate first (mirroring the `pending_first_bind`
    /// post-Ready dispatch): once the replicated `RunAborted` verdict is
    /// latched, this continuation must NOT bind the deferred task — the stash
    /// is dropped (the authority exits on the same verdict; there is no run
    /// left to requeue into) and this loop's own tail exits on the same latch.
    ///
    /// `factory` is threaded from the operational loop (which owns it) so the
    /// per-type subprocess (re)spawn `assign_resolved_task` drives reaches the
    /// real factory.
    pub(in crate::secondary) async fn dispatch_released_affine_dependent(
        &mut self,
        dependent: PendingAffineDependent<I>,
        factory: &mut impl dynrunner_manager_local::WorkerFactory<M>,
    ) -> Result<(), String> {
        if let Some(reason) = self.cluster_state.run_aborted() {
            tracing::info!(
                worker_id = dependent.worker_id,
                task_hash = %dependent.work_hash,
                reason = %reason,
                "released affine dependent NOT dispatched: the replicated \
                 run-terminal verdict is latched; dropping the deferred task \
                 (this node exits on the same latch)"
            );
            return Ok(());
        }
        let PendingAffineDependent {
            work_hash,
            worker_id,
            binary,
            estimated,
            predecessor_outputs,
        } = dependent;
        self.assign_resolved_task(
            worker_id,
            binary,
            estimated,
            predecessor_outputs,
            work_hash,
            factory,
        )
        .await
    }
}
