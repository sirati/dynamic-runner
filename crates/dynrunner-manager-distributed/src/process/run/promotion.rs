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
) -> Option<oneshot::Receiver<PrimaryRunOutcome<I>>>
where
    I: Identifier + 'static,
    Sched: Scheduler<I> + Clone + 'static,
    Est: ResourceEstimator<I> + Clone + 'static,
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

    // The caller's recipe builds + snapshot-seeds the primary from the
    // converged `cluster_state` the secondary captured ON the signal at the
    // promotion-fire instant. The node only threads the snapshot (fat
    // entries) AND the settled-CRDT base (join-fixed-point slice, inherited
    // from the promoting host's spill index without replay) through — the
    // builder owns `adopt_settled_base` + `seed_from_promotion_snapshot` +
    // coordinator construction (scheduler/estimator are the caller's concern).
    let mut built = builder(
        client,
        inbox,
        demote_rx,
        signal.snapshot,
        signal.settled_base,
    );
    built.coordinator.register_demote_on_displaced(demote_tx);

    let (tx, rx) = oneshot::channel();
    spawn_primary_with(built.coordinator, built.run_args, control, tx);
    // Hold the slot `Arc` for the primary's lifetime — dropping it is the
    // role-teardown lever (the mesh `Weak` then stops upgrading). Park it in a
    // detached task so it lives as long as the run.
    tokio::task::spawn_local(async move {
        let _slot = slot;
        std::future::pending::<()>().await;
    });
    Some(rx)
}

/// Spawn a primary's `run_consuming`, sending the outcome back. The BUG-6
/// demote hook is registered by the caller BEFORE this (bootstrap path) or
/// inside the promotion build, so the consuming run can already race its
/// demote receiver.
pub(super) fn spawn_primary_with<I, Sched, Est>(
    coordinator: PrimaryCoordinator<Sched, Est, I>,
    args: PrimaryRunArgs<I>,
    control: &MeshControlHandle<I>,
    done: oneshot::Sender<PrimaryRunOutcome<I>>,
) where
    I: Identifier + 'static,
    Sched: Scheduler<I> + Clone + 'static,
    Est: ResourceEstimator<I> + Clone + 'static,
{
    let _ = control;
    tokio::task::spawn_local(async move {
        let PrimaryRunArgs {
            seed,
            on_phase_start,
            on_phase_end,
        } = args;
        match coordinator
            .run_consuming(seed, on_phase_start, on_phase_end)
            .await
        {
            Ok(outcome) => {
                let _ = done.send(outcome);
            }
            Err(e) => {
                let _ = done.send(PrimaryRunOutcome::Local {
                    result: Err(e),
                    completed: 0,
                    failed: 0,
                    stranded: 0,
                });
            }
        }
    });
}
