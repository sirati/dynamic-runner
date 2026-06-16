//! Primary-side RESOLUTION of dependency-satisfied `TaskKind::SecondaryAffine`
//! gates — the WITH-dep firing surface (#506) the seed originator (#502) and
//! the live spawn/resume delta originator both structurally miss.
//!
//! ## The one concern
//! Drive the affine-gate `Pending → AffineReady` resolution from the AUTHORITY
//! side for a gate whose dependency completes AFTER seed: pick each queued
//! `SecondaryAffine` gate whose own deps are now all terminal, originate its
//! `AffineReady` terminal, and unblock its dependents. A gate is invisible to
//! every worker-dispatch path (it is never worker-assignable, like a `Setup`
//! task); this module is the ONLY thing that moves a WITH-dep gate out of
//! `Pending` once its dep completes post-seed.
//!
//! ## Why this surface is REQUIRED (the #506 gap)
//! The affine originator has exactly two PRE-#506 firing surfaces, and a
//! with-dep gate whose dep completes after seed rides NEITHER:
//!   * the SEED scan (`originate_cold_seed` / `discover_on_promotion`'s
//!     `affine_ready_mutations_for_ledger`, the #502 fix) runs ONCE at seed —
//!     it correctly SKIPS a with-dep gate (its dep is not yet terminal);
//!   * the live DELTA surface (`apply_and_broadcast_cluster_mutations`'s
//!     `became_pending`) fires ONLY on a CRDT `Blocked → Pending` resume. But
//!     the seed path classifies EVERY `TaskAdded` task `Pending` (dep-blocking
//!     lives in the POOL's blocked-map, not CRDT `Blocked`), so a with-dep
//!     gate is born CRDT-`Pending` and `resume_blocked_on(<its dep>)` finds
//!     nothing when the dep completes — `became_pending` is empty, the
//!     originator never re-fires, and the gate sits `Pending` forever (a gate
//!     is not worker-assignable, so the pool never dispatches it) wedging its
//!     dependents Blocked → DEADLOCK.
//!
//! The MISSING surface is the gate's DEPENDENCY completing. The pool's
//! existing dep-resolution authority already computes it: when the dep
//! completes, `on_item_finished` walks `dependents_of[<dep>]` and unblocks the
//! gate from `blocked` into a dispatch bucket. This pass observes that result
//! — a gate now QUEUED (`pool().iter()` yields only bucket items, never
//! blocked ones) is exactly "a gate whose deps the pool just satisfied" — and
//! routes it to the EXISTING `AffineReady` originator. No re-scan of the
//! ledger (the #504 O(n²) trap), no parallel affine-specific dep-scan beside
//! the pool's: the pool already did the dep resolution, this reads it.
//!
//! ## Module boundary (CLAUDE.md design-first)
//!   * Owner: the primary. The single seam it crosses is the existing
//!     worker-management reaction (`react_to_worker_signal_batch` calls
//!     [`Self::resolve_dependency_satisfied_affine_gates`] right after the
//!     worker recheck + setup dispatch — a gate entering a bucket emits the
//!     same `TasksAdded` a new work/setup task does).
//!   * API the caller sees: ONE one-line delegate — "resolve
//!     dependency-satisfied affine gates". The caller learns nothing about
//!     gate internals; no `if kind == SecondaryAffine` in the loop.
//!   * Detection owner is REUSED, not reinvented: the gate's READY condition
//!     is decided by `cluster_state`'s `affine_ready_mutations_for` (the same
//!     detector the seed + delta surfaces use); the dependent unblock is the
//!     pool's `on_gate_resolved` (the same walk `on_item_finished` uses).

use dynrunner_core::{Identifier, PhaseId};
use dynrunner_protocol_primary_secondary::ClusterMutation;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::PrimaryCoordinator;
use crate::primary::command_channel::PrimaryCommand;
use crate::primary::wire::compute_task_hash;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Resolve every queued `SecondaryAffine` gate whose own deps are now all
    /// terminal: originate its `AffineReady` terminal and unblock its
    /// dependents.
    ///
    /// Called from the worker-management reaction's `TasksAdded` branch — the
    /// same recheck that dispatches work + setup tasks. A gate whose dep just
    /// completed was unblocked `blocked → bucket` by the pool's
    /// `on_item_finished` dep walk on that completion, which emits
    /// `TasksAdded`; this pass then drains the now-queued gate. A gate whose
    /// deps are NOT yet all terminal is left queued and re-evaluated on the
    /// next `TasksAdded` (it cannot be dispatched to a worker either way, so
    /// leaving it is harmless — it never wedges a worker slot).
    ///
    /// Coalesced + idempotent: the loop drains every currently-resolvable
    /// gate; one is resolved at most once (it is removed from the pool on
    /// resolution). Re-running with nothing resolvable is a cheap no-op.
    pub(crate) async fn resolve_dependency_satisfied_affine_gates(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        // Two-phase per iteration to keep the `cluster_state` (detection) borrow
        // and the pool (take) borrow disjoint, mirroring `dispatch_setup_tasks`:
        // resolve the READY gate's hash against the detection owner FIRST, then
        // take it by hash.
        while let Some(hash) = self.pool_ref_resolvable_affine_gate() {
            // Remove exactly that gate from the pool by hash. A gate is never
            // worker-assignable, so it can only ever sit queued; taking it is
            // the ONLY consumer of a queued gate (no worker dispatch competes).
            let Some(gate) = self
                .pool_mut()
                .take_first_match(|t| compute_task_hash(t) == hash)
            else {
                // Raced away (e.g. a concurrent drain). Stop the pass; the next
                // `TasksAdded` re-evaluates.
                break;
            };
            self.resolve_affine_gate(gate, command_rx).await;
        }
    }

    /// Find the FIRST queued gate that is a dependency-resolved
    /// `SecondaryAffine` gate per the SINGLE detection owner
    /// (`affine_ready_mutations_for`), returning its content hash. `None` when
    /// no queued gate is currently resolvable.
    ///
    /// Reuses the cluster_state detector rather than re-deciding readiness here
    /// (one detection owner): the pool's queued view feeds candidate hashes,
    /// the detector filters to the `Pending` gates whose deps are all terminal.
    /// `pool().iter()` yields only QUEUED (bucket) items — a gate appears only
    /// once the pool's dep walk has unblocked it — so a still-blocked gate is
    /// never a candidate. Reads the pool's queued view and `cluster_state` in
    /// ONE `&self` borrow so the caller can then take by hash without a borrow
    /// conflict.
    fn pool_ref_resolvable_affine_gate(&self) -> Option<String> {
        let gate_hashes = self
            .pool()
            .iter()
            .filter(|t| t.kind.is_secondary_affine())
            .map(compute_task_hash);
        self.cluster_state
            .affine_ready_mutations_for(gate_hashes)
            .into_iter()
            .find_map(|m| match m {
                ClusterMutation::AffineReady { hash } => Some(hash),
                _ => None,
            })
    }

    /// Originate the authoritative `AffineReady` terminal for a resolved gate,
    /// then run the pool's dependent-unblock + phase cascade.
    ///
    /// `AffineReady` (READY-not-EXECUTED): the primary NEVER runs the gate.
    /// Origination rides the EXISTING `apply_and_broadcast_cluster_mutations`
    /// path (so the terminal replicates to every secondary and the CRDT
    /// `resume_blocked_on(<gate>)` resumes any dependent that WAS CRDT-`Blocked`
    /// — the chained-gate case where a downstream gate was spawned blocked).
    /// Then `on_gate_resolved` runs the pool side: it records the gate
    /// completed and unblocks every POOL-blocked dependent (the build tasks,
    /// which were seeded CRDT-`Pending` and so are not CRDT-`Blocked` — only
    /// the pool's blocked-map holds them), moving them `blocked → queued`
    /// dispatchable. NO in-flight decrement (a gate is never in-flight — see
    /// `PendingPool::on_gate_resolved`), and the phase drain transition runs so
    /// the gate's phase progresses now that its inert gate item is gone.
    ///
    /// A freshly-unblocked dependent that is ITSELF an affine gate (a chained
    /// import) lands queued in this same pass and the caller's loop drains it
    /// on its next iteration. The unblocked builds become dispatchable; the
    /// per-completion `TasksAdded` that drove this pass re-runs the worker
    /// recheck after it, and any subsequent completion re-emits `TasksAdded`,
    /// so the builds are picked up by the standard dispatch recheck.
    async fn resolve_affine_gate(
        &mut self,
        gate: std::sync::Arc<dynrunner_core::TaskInfo<I>>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        let hash = compute_task_hash(&gate);
        let phase: PhaseId = gate.phase_id.clone();
        let task_id = gate.task_id.clone();
        tracing::info!(
            task_hash = %hash,
            phase = %phase,
            "affine gate dependency-satisfied post-seed; resolving to AffineReady"
        );
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::AffineReady {
            hash: hash.clone(),
        }])
        .await;
        // Pool side: unblock the gate's dependents WITHOUT an in-flight
        // decrement (the gate was never dispatched), then run the phase
        // cascade — the SAME drain edge + hooks a worker/setup completion runs.
        self.pool_mut().on_gate_resolved(&phase, &task_id);
        self.process_phase_lifecycle(command_rx).await;
    }
}
