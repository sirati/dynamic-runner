//! Submitter-primary→observer swap for [`super::Node::run`].
//!
//! # Concern
//!
//! ONE concern: relocate a demoted submitter-primary into a standalone
//! observer by retagging the slot IN PLACE through the pump (H5 — the same
//! `Arc<RoleSlot>` / inbound channel survives, so a primary-facing frame in
//! flight at the retag is drained by the observer inbox and applied
//! idempotently), and the shared standalone-observer spawn used by BOTH the
//! relocate-swap and the cold-join path.

use dynrunner_core::Identifier;

use super::super::pump::MeshControlHandle;
use super::super::role::LocalRole;
use super::outcome::ObserverJoinHandle;
use crate::observer::{ObserverCoordinator, ObserverHandoff};

/// Submitter-primary→observer swap (H5): retag the slot in place through the
/// pump (primary→observer, preserving the stable channel), build the observer
/// from the handoff, and spawn it. A free fn (not a method) because the node
/// `self` was destructured into the run loop's locals.
pub(super) fn swap_primary_to_observer<I>(
    control: &MeshControlHandle<I>,
    handoff: Box<ObserverHandoff<I>>,
) -> ObserverJoinHandle
where
    I: Identifier + 'static,
{
    control.retag(LocalRole::Primary, LocalRole::Observer);
    let observer = ObserverCoordinator::from_handoff(*handoff);
    // The retagged slot (same Arc the primary held) stays alive via the former
    // primary's parked slot-holder task, so no slot is passed here.
    spawn_observer(observer, None)
}

/// Spawn an observer's standalone `run`, optionally holding a slot `Arc` for
/// the run's lifetime so the mesh `Weak` keeps upgrading (ingress liveness).
/// The cold-join path passes its freshly-registered slot; the relocate-swap
/// path passes `None` (the retagged slot is already held by the former
/// primary's parked slot-holder task).
pub(super) fn spawn_observer<I>(
    mut observer: ObserverCoordinator<I>,
    slot: Option<std::sync::Arc<crate::process::RoleSlot<I>>>,
) -> ObserverJoinHandle
where
    I: Identifier + 'static,
{
    tokio::task::spawn_local(async move {
        let _slot = slot;
        // Carry BOTH the run disposition AND the converged completion count
        // out of the task. The count is read off the coordinator AFTER `run`
        // returns (the converged ledger) but BEFORE the task drops the
        // coordinator — once the task ends the coordinator is gone, so the
        // count must travel back here, not be re-sourced by the caller.
        let run_result = observer.run().await;
        let completed = observer.completed_count();
        (run_result, completed)
    })
}
