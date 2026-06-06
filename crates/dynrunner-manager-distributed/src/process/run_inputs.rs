//! The per-role run inputs [`super::Node::run`] consumes.
//!
//! # Concern
//!
//! ONE concern: carry into `Node::run` everything the live roles' run loops
//! need that the [`super::Node`] struct (mesh + role entries + lifecycle
//! channels) does not already hold — the primary's pipeline args, the
//! secondary's `WorkerFactory`, and the recipe for BUILDING a promoted
//! primary on a promotion signal.
//!
//! # Why a builder for the promoted primary (the boundary)
//!
//! The [`super::Node`] must NOT know how to construct a scheduler / estimator
//! / `PrimaryConfig` (those are the caller's — pyo3's — concern). So a
//! promotion does not have the node build a primary from raw parts; the
//! caller supplies a [`PromotedPrimaryBuilder`] closure that, given the mesh
//! ends + the demote receiver + the promoting host's converged snapshot,
//! returns a fully-built, snapshot-seeded `PrimaryCoordinator` PLUS the
//! pipeline args its `run` needs. The node only orchestrates — it registers
//! the slot, calls the builder, and spawns the returned coordinator. This
//! keeps the node ignorant of scheduler/estimator construction while owning
//! the lifecycle (SUPREME-LAW #3 & #7: the secondary signals, the node
//! builds — never the secondary).

use std::collections::HashMap;

use dynrunner_core::{Identifier, PhaseId, TaskInfo};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc;

use crate::primary::{OnPhaseEnd, OnPhaseStart, PrimaryCoordinator};
use crate::process::{MeshClient, RoleInbox};

/// The pipeline args a primary's `run` / `run_consuming` consumes.
///
/// Single-use (the `on_phase_*` closures are `Box<dyn FnMut>`, not `Clone`):
/// a bootstrap primary consumes one set; a promoted primary gets a FRESH set
/// from its [`PromotedPrimaryBuilder`].
pub struct PrimaryRunArgs<I: Identifier> {
    /// The task binaries the pipeline dispatches.
    pub binaries: Vec<TaskInfo<I>>,
    /// The phase dependency graph.
    pub phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    /// Per-phase-start narration hook.
    pub on_phase_start: OnPhaseStart,
    /// Per-phase-end narration hook.
    pub on_phase_end: OnPhaseEnd,
}

/// The inputs of one promotion build: the snapshot-seeded
/// `PrimaryCoordinator` the node will `run`, plus its pipeline args.
pub struct PromotedPrimary<Sched, Est, I>
where
    Sched: Scheduler<I>,
    Est: ResourceEstimator<I>,
    I: Identifier,
{
    /// The freshly-built, snapshot-seeded primary. The builder has ALREADY
    /// called `seed_from_promotion_snapshot` and any pre-run registration
    /// (panik, listeners); the node registers the demote hook + runs it.
    pub coordinator: PrimaryCoordinator<Sched, Est, I>,
    /// Fresh pipeline args for this promoted primary's `run`.
    pub run_args: PrimaryRunArgs<I>,
}

/// The caller-supplied recipe for building a promoted primary.
///
/// Invoked by [`super::Node::run`] on a [`super::PromotionSignal`], handed
/// the just-minted mesh ends, the demote receiver the node owns, AND the
/// promoting host's converged `cluster_state` snapshot (carried ON the
/// signal — captured atomically at the promotion-fire instant by the
/// secondary). Returns the built + seeded primary and its run args.
///
/// The node supplies the snapshot but stays ignorant of how to TURN it into a
/// seeded coordinator: the builder calls
/// `PrimaryCoordinator::seed_from_promotion_snapshot(snapshot)` itself,
/// because only the caller (pyo3) knows the scheduler / estimator /
/// `PrimaryConfig` to construct the coordinator around it. The node only
/// registers the slot + spawns the returned coordinator (SUPREME-LAW #3: the
/// node builds via the recipe, the secondary never builds). Threading the
/// snapshot through the signal (rather than a shared-mutable cell the builder
/// reads out-of-band) keeps the seed coherent with its trigger and owned
/// (`Send`). `FnMut` (not `FnOnce`) only so the type is a plain boxed closure;
/// a node promotes at most once per lifetime.
pub type PromotedPrimaryBuilder<Sched, Est, I> = Box<
    dyn FnMut(
        MeshClient<I>,
        RoleInbox<I>,
        mpsc::UnboundedReceiver<()>,
        crate::cluster_state::ClusterStateSnapshot<I>,
    ) -> PromotedPrimary<Sched, Est, I>,
>;

/// Everything [`super::Node::run`] consumes beyond the [`super::Node`]
/// struct itself.
///
/// Each field is `Option` because a node hosts only the roles it was
/// composed with: a submitter node carries `primary_run_args` (its bootstrap
/// primary) and no `secondary_factory`; a compute node carries a
/// `secondary_factory` (+ a `promote` builder so it can become primary) and
/// no `primary_run_args`.
pub struct NodeRunInputs<F, Sched, Est, I>
where
    Sched: Scheduler<I>,
    Est: ResourceEstimator<I>,
    I: Identifier,
{
    /// The bootstrap primary's pipeline args, iff a primary `RoleEntry` is
    /// live at composition (the submitter). `None` on a compute node.
    pub primary_run_args: Option<PrimaryRunArgs<I>>,
    /// The send end of the bootstrap primary's BUG-6 demote channel. The
    /// caller mints the channel, passes the RECEIVER to
    /// `PrimaryCoordinator::new(.., demote_rx, ..)` (B-PRIMARY's constructor),
    /// and hands the SENDER here so the node registers the role-change hook
    /// that feeds it (`register_demote_on_displaced`). `None` on a node with
    /// no bootstrap primary OR one that supplies an inert demote_rx.
    pub primary_demote_tx: Option<mpsc::UnboundedSender<()>>,
    /// The secondary's `WorkerFactory`, iff a secondary `RoleEntry` is live
    /// (every compute node + the submitter's co-bootstrap secondary, if any).
    /// `F: WorkerFactory<Mgr>` — the factory type is distinct from the
    /// secondary's `ManagerEndpoint` it produces.
    pub secondary_factory: Option<F>,
    /// The recipe to build a promoted primary on a [`super::PromotionSignal`].
    /// `None` on a node that can never be primary (no promotion path).
    pub promote: Option<PromotedPrimaryBuilder<Sched, Est, I>>,
}

impl<F, Sched, Est, I> Default for NodeRunInputs<F, Sched, Est, I>
where
    Sched: Scheduler<I>,
    Est: ResourceEstimator<I>,
    I: Identifier,
{
    fn default() -> Self {
        Self {
            primary_run_args: None,
            primary_demote_tx: None,
            secondary_factory: None,
            promote: None,
        }
    }
}
