//! Promotion-build path for [`super::Node::run`].
//!
//! # Concern
//!
//! ONE concern: turn a [`super::super::PromotionSignal`] into a running
//! snapshot-seeded primary (SUPREME-LAW #3 & #7 — the secondary SIGNALS,
//! the node BUILDS). Mints the Primary trio through the pump so the slot +
//! the primary's `secondary_keepalives` seeding land BEFORE its first
//! heartbeat tick (BUG-4), wires a FRESH BUG-6 demote channel, calls the
//! caller's builder (which snapshot-seeds the primary), and spawns it.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::{mpsc, oneshot};

use super::super::node::PromotionSignal;
use super::super::pump::MeshControlHandle;
use super::super::role::LocalRole;
use super::super::run_inputs::PrimaryRunArgs;
use super::coordinator_host::{CoordinatorThread, spawn_primary};
use crate::primary::{PrimaryCoordinator, PrimaryRunOutcome};

/// Build, seed, register, and spawn a promoted primary on a promotion
/// signal (SUPREME-LAW #3 & #7 — the secondary SIGNALLED; the NODE builds).
///
/// The node MINTS the Primary trio (register through the pump, so the slot +
/// the primary's `secondary_keepalives` seeding land BEFORE its first
/// heartbeat tick — BUG-4 — because the build + spawn run synchronously here
/// before the primary's run loop awaits), wires a FRESH demote channel
/// (BUG-6: the promoted primary demotes itself on any later self→other flip),
/// calls the caller's builder (which snapshot-seeds the primary itself), and
/// spawns it. Returns the outcome receiver + the slot `Arc` (the node holds
/// the latter as the teardown lever). `None` if the promotion cannot proceed
/// (no builder, or the pump is gone).
pub(super) async fn self_build_promoted_primary<I, Sched, Est>(
    signal: PromotionSignal<I>,
    promote: &mut Option<super::super::run_inputs::PromotedPrimaryBuilder<Sched, Est, I>>,
    control: &MeshControlHandle<I>,
    own_peer_id: &PeerId,
    // Whether to isolate the promoted primary on its OWN thread (a real-network
    // node, where it co-resides in-process with a live secondary) vs keep it on
    // the node's shared `LocalSet` (the in-process `--multi-computer local`
    // node). Sourced by the node from `MeshHost::runs_on_dedicated_thread`.
    dedicated: bool,
) -> Option<(oneshot::Receiver<PrimaryRunOutcome<I>>, CoordinatorThread)>
where
    I: Identifier + 'static,
    Sched: Scheduler<I> + Clone + Send + 'static,
    Est: ResourceEstimator<I> + Clone + Send + 'static,
{
    let builder = promote.as_mut()?;
    // Register the Primary slot + mint its trio through the pump.
    let (slot, client, inbox) = control
        .register(LocalRole::Primary, own_peer_id.clone())
        .await?;

    // BUG-6 demote channel: the node owns `demote_tx` (fed by the role-change
    // hook), the promoted primary owns `demote_rx` (its `run_consuming`
    // relocates on it). Minted here so the hook and the receiver pair.
    let (demote_tx, demote_rx) = mpsc::unbounded_channel();

    // Which promotion path this is, derived from the signal's
    // `PrimaryChangeReason` (Transferred relocate vs Election failover) —
    // captured before `signal.snapshot` is moved. The builder stamps it onto
    // `SeedSource::PromotionSnapshot { kind }` so the bring-up reservation
    // opens ONLY on a `BootstrapRelocation`.
    let bootstrap_kind = super::super::BootstrapKind::from(signal.reason);

    // The caller's recipe builds + snapshot-seeds the primary from the
    // converged `cluster_state` the secondary captured ON the signal at the
    // promotion-fire instant. The node only threads the snapshot (fat
    // entries) AND the settled-CRDT base (join-fixed-point slice, inherited
    // from the promoting host's spill index without replay) AND the bootstrap
    // kind through — the builder owns `adopt_settled_base` +
    // `seed_from_promotion_snapshot` + coordinator construction
    // (scheduler/estimator are the caller's concern).
    let mut built = builder(
        client,
        inbox,
        demote_rx,
        signal.snapshot,
        signal.settled_base,
        bootstrap_kind,
    );
    built.coordinator.register_demote_on_displaced(demote_tx);

    let (tx, rx) = oneshot::channel();
    let coord_thread =
        spawn_primary_with(built.coordinator, built.run_args, control, tx, dedicated);
    // Hold the slot `Arc` for the primary's lifetime — dropping it is the
    // role-teardown lever (the mesh `Weak` then stops upgrading). Park it in a
    // detached task on the NODE's `LocalSet` (NOT the primary's dedicated
    // thread): the slot belongs to the mesh (the pump delivers loopback frames
    // through it), so it must outlive on the node's runtime, independent of
    // where the primary coordinator's loop executes.
    tokio::task::spawn_local(async move {
        let _slot = slot;
        std::future::pending::<()>().await;
    });
    Some((rx, coord_thread))
}

/// Spawn a primary's `run_consuming` via the coordinator-host executor, sending
/// the outcome back. The BUG-6 demote hook is registered by the caller BEFORE
/// this (bootstrap path) or inside the promotion build, so the consuming run can
/// already race its demote receiver.
///
/// `dedicated` selects the executor flavor (see
/// [`super::coordinator_host::spawn_primary`]): `true` isolates the primary loop
/// on its own thread (real-network node, co-located with a live secondary);
/// `false` keeps it on the node's shared `LocalSet` (in-process node). The
/// boundary is channel-shaped either way (mesh ends + the outcome `oneshot`
/// cross runtimes natively), so the node's `select!` loop is unchanged; the
/// returned [`CoordinatorThread`] is the node's teardown lever. `control` is not
/// needed (the executor reaches the mesh only through the coordinator's
/// already-wired `MeshClient` / `RoleInbox`), kept in the signature for
/// call-site symmetry.
pub(super) fn spawn_primary_with<I, Sched, Est>(
    coordinator: PrimaryCoordinator<Sched, Est, I>,
    args: PrimaryRunArgs<I>,
    control: &MeshControlHandle<I>,
    done: oneshot::Sender<PrimaryRunOutcome<I>>,
    dedicated: bool,
) -> CoordinatorThread
where
    I: Identifier + 'static,
    Sched: Scheduler<I> + Clone + Send + 'static,
    Est: ResourceEstimator<I> + Clone + Send + 'static,
{
    let _ = control;
    spawn_primary(coordinator, args, done, dedicated)
}
