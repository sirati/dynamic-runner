//! Observer-side announcer wire-up.
//!
//! # Concern
//!
//! Single concern: bundle the channel construction, role-change-hook
//! registration, post-restore initial trigger fire, and the
//! `Arc<AtomicU64>` epoch-mirror hand-off that the observer's
//! resource-holdings announcer task needs to participate in the
//! `SecondaryCoordinator`'s lifecycle.
//!
//! # Module boundary
//!
//! - **Upward** to the caller (the PyO3 observer dispatcher in
//!   `dynrunner-pyo3`): one orchestration function
//!   [`attach_observer_announcer`] returns an [`AnnouncerHandle`] the
//!   caller spawns later (after `restore_from_snapshot_and_skip_setup`
//!   has fired the initial trigger by way of the role-change hook
//!   firing from inside `cluster_state.restore`).
//! - **Downward** to [`crate::observer::announcer`]: hands the
//!   announcer task its three inputs — `rx`, `holdings`, the epoch
//!   mirror — and stores the `tx` for the role-change hook to fire.
//! - **Sideways** to [`crate::SecondaryCoordinator`]: uses the public
//!   surface only (`register_role_change_hook`, `primary_epoch_mirror`).
//!
//! The lifecycle module does NOT spawn the announcer task or pick the
//! [`crate::observer::announcer::AnnouncerSender`] impl. Spawning lives
//! at the call site because the sender impl is environment-specific —
//! the production wiring needs E1's
//! `ClusterMutation::PeerResourceHoldingsUpdated` variant + a
//! cross-task outbox to the run loop's `peer_transport`; tests
//! substitute a fake. Keeping the sender choice outside the lifecycle
//! module avoids a generic-soup signature here.
//!
//! # Wire-up sequence (caller view)
//!
//! ```ignore
//! let handle = attach_observer_announcer(&mut secondary, holdings, "observer-x".into());
//! secondary.restore_from_snapshot_and_skip_setup(snapshot);
//! // Snapshot apply called fire_role_change_hooks → handle.tx.try_send(AnnounceTrigger)
//! // happened. Spawn the task; it drains the queued trigger first.
//! tokio::task::spawn_local(run_observer_announcer(
//!     handle.rx,
//!     handle.holdings,
//!     handle.peer_id,
//!     sender,
//!     handle.primary_epoch_mirror,
//! ));
//! ```
//!
//! The two-step (attach, then spawn) shape mirrors the existing
//! peer-lifecycle dispatcher pattern: `register_lifecycle_listener`
//! happens at construction, `spawn_local(run_peer_lifecycle_dispatcher)`
//! happens inside the run loop. See `secondary/mod.rs`'s
//! `run_until_setup_or_done` for the prior art.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use tokio::sync::mpsc;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::RoleTable;

use super::announcer::AnnounceTrigger;
use crate::ClusterState;

/// Bundle of inputs the caller threads into
/// [`crate::observer::announcer::run_observer_announcer`] after
/// `attach_observer_announcer` returns. Carries:
///
/// - `rx`: the trigger receiver. Already wired to the role-change
///   hook + the initial post-restore fire path.
/// - `holdings`: the observer's static resource set, moved in.
/// - `peer_id`: the announcer's `peer_id` field of the outbound
///   payload — equal to the secondary's `secondary_id`.
/// - `primary_epoch_mirror`: lock-free epoch reader; the announcer
///   stamps every send with `mirror.load(Acquire)`.
///
/// The caller's responsibility (the PyO3 observer dispatcher today)
/// is to construct an `AnnouncerSender` impl and then `spawn_local`
/// the announcer task with these inputs.
pub struct AnnouncerHandle {
    pub rx: mpsc::Receiver<AnnounceTrigger>,
    pub holdings: HashSet<String>,
    pub peer_id: String,
    pub primary_epoch_mirror: Arc<AtomicU64>,
}

/// Channel capacity for the announcer's trigger queue.
///
/// Sized to 8 to absorb a flap burst (epoch 5→6→7→8 within a few ms)
/// without dropping intermediate triggers while the announcer is mid-
/// send. Each trigger is a unit value, so the memory footprint is
/// negligible; the cap exists to guarantee `try_send` from the
/// (synchronous) role-change hook can never block. Per the
/// "back-pressure isn't load-bearing" rule, drops on full are silent —
/// the next successful fire (or the still-pending coalesced fire)
/// will carry the latest epoch off the mirror.
const ANNOUNCE_CHANNEL_CAPACITY: usize = 8;

/// Wire up the observer-side announcer onto `secondary`. Returns the
/// inputs the caller subsequently spawns the announcer task with.
///
/// Side effects on `secondary`:
///   - One [`crate::RoleChangeHook`] is registered. The hook fires
///     `tx.try_send(AnnounceTrigger).ok()` synchronously from inside
///     `apply`/`restore`'s `fire_role_change_hooks`. The `.ok()`
///     drops a full-channel error — see [`ANNOUNCE_CHANNEL_CAPACITY`]
///     rationale.
///
/// What this function deliberately does NOT do:
///   - It does not spawn the announcer task. The caller picks the
///     [`crate::observer::announcer::AnnouncerSender`] impl + runtime.
///   - It does not fire an initial trigger directly. The initial
///     trigger is the role-change hook firing from inside
///     `restore_from_snapshot_and_skip_setup` once the caller invokes
///     it post-attach — `cluster_state.restore`'s `primary_epoch >
///     local` branch updates the mirror and calls
///     `fire_role_change_hooks`, which runs our hook, which pushes
///     the trigger. No code path here needs to duplicate that
///     stimulus.
///
/// # Why bound on `&mut ClusterState`, not `&mut SecondaryCoordinator`
///
/// `SecondaryCoordinator` is generic over six type parameters
/// (`PT, P, M, S, E, I`); adding an observer-specific method to its
/// impl block forces all six on this concern even though the
/// announcer cares about exactly one (`I` for the wire payload).
/// Binding on the concrete `ClusterState<I>` (which is what the
/// announcer touches) keeps the boundary minimal — the registrar
/// trait + the epoch-mirror accessor are the only surface used.
/// Callers holding a `SecondaryCoordinator` pass
/// `&mut secondary.cluster_state` (or, if outside the secondary
/// module's `pub(super)` boundary, use the forwarding helpers
/// `register_role_change_hook` + `primary_epoch_mirror` on the
/// coordinator and call `attach_observer_announcer_with_pieces`
/// below instead).
pub fn attach_observer_announcer<I>(
    cluster_state: &mut ClusterState<I>,
    holdings: HashSet<String>,
    peer_id: String,
) -> AnnouncerHandle
where
    I: Identifier,
{
    let (tx, rx) = mpsc::channel::<AnnounceTrigger>(ANNOUNCE_CHANNEL_CAPACITY);
    // The closure captures the sender end; it fires on EVERY role-
    // change hook invocation (PrimaryChanged + observer-set churn
    // both fire `fire_role_change_hooks`). The announcer doesn't
    // care about which sub-event drove the fire — the trigger is
    // semantically "primary or observer-set may have changed,
    // re-announce holdings"; over-announcing on observer churn is
    // harmless (the apply rule's epoch monotonicity discards
    // duplicates at the receiver).
    //
    // `try_send` is the right shape here: `send` is async (the hook
    // closure is sync `Fn`), and `blocking_send` would deadlock the
    // apply loop on a backed-up announcer. Dropping on full preserves
    // the apply path's non-blocking contract.
    let trigger_tx = tx.clone();
    let hook: Box<dyn Fn(&RoleTable) + Send + Sync + 'static> =
        Box::new(move |_table: &RoleTable| {
            // `.ok()` discards both the full-channel error and the
            // closed-channel error — neither is recoverable from
            // inside the hook closure, and the announcer task's exit
            // (which is the only path to a closed channel) means
            // we're shutting down anyway.
            let _ = trigger_tx.try_send(AnnounceTrigger);
        });
    use crate::RoleChangeHookRegistrar;
    cluster_state.register_role_change_hook(hook);
    AnnouncerHandle {
        rx,
        holdings,
        peer_id,
        primary_epoch_mirror: cluster_state.primary_epoch_mirror(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_core::RunnerIdentifier;
    use dynrunner_protocol_primary_secondary::cluster_mutation::ClusterMutation;
    use std::sync::atomic::Ordering;

    /// End-to-end check on the wire-up: a `PrimaryChanged` mutation
    /// applied to the registered `ClusterState` fires the hook,
    /// which `try_send`s an `AnnounceTrigger` onto `handle.rx`, and
    /// `handle.primary_epoch_mirror` carries the post-mutation epoch.
    ///
    /// Pins the contract the announcer task depends on: triggers
    /// arrive on every `PrimaryChanged`, and the mirror is observable
    /// at the moment the hook fires (so an announcer that reads the
    /// mirror on first wake sees the new epoch).
    #[test]
    fn attach_observer_announcer_fires_on_primary_changed() {
        let mut state = ClusterState::<RunnerIdentifier>::new();
        let mut handle = attach_observer_announcer(
            &mut state,
            HashSet::from(["/nix/store/abc".to_string()]),
            "observer-1".into(),
        );

        // Sanity: no trigger before any role change.
        assert!(handle.rx.try_recv().is_err());
        assert_eq!(handle.primary_epoch_mirror.load(Ordering::Acquire), 0);

        let outcome = state.apply(ClusterMutation::PrimaryChanged {
            new: "secondary-7".into(),
            epoch: 5,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        });
        assert_eq!(outcome, crate::cluster_state::ApplyOutcome::Applied);

        // Hook fired → trigger queued.
        assert!(
            handle.rx.try_recv().is_ok(),
            "hook must push a trigger on PrimaryChanged",
        );
        // Mirror is current.
        assert_eq!(handle.primary_epoch_mirror.load(Ordering::Acquire), 5);
        // Handle threads the static observer identity through.
        assert_eq!(handle.peer_id, "observer-1");
        assert_eq!(
            handle.holdings,
            HashSet::from(["/nix/store/abc".to_string()]),
        );
    }
}
