//! [`RoleHolderView`] — a coordinator-published, mesh-read view of which
//! PEER currently holds the (id-less) Primary role, for the INGRESS
//! relay decision.
//!
//! # Concern
//!
//! ONE concern: carry the ROUTING-holder fact ("the Primary role lives
//! on peer X") from the replicated role table the coordinators own into
//! the [`super::Mesh`]'s ingress demux, so a directed `Primary` frame
//! that lands at a process with no live Primary slot can be RELAYED
//! toward the holder instead of dying in a local fan/hold (the
//! run_20260612_045106 zombie: a respawned secondary's
//! `SecondaryWelcome` — and every other Primary-addressed frame kind —
//! black-holed at the relocated-away setup process).
//!
//! # Direction (the mirror of [`crate::process::MembershipView`])
//!
//! `MembershipView` is pump-published, coordinator-read. This view is
//! the REVERSE bridge: the COORDINATORS write (each one's
//! `ClusterState` role-change hook publishes `role_table.primary` —
//! see [`attach_primary_recognition`]) and the MESH reads at ingress.
//! The mesh never classifies frame content and never re-derives role
//! state; it consumes the published projection only.
//!
//! # Recognition vs routing (owner principle)
//!
//! Recognition-identity (whose frames a coordinator ACCEPTS as the
//! primary: `current_primary ?? bootstrap_primary_id`) is decoupled
//! from the ROUTING-holder this view carries (the role table's
//! `primary`). The ingress relay is a ROUTING decision, so it reads
//! the role-table fact — never a bootstrap fallback, which is exactly
//! the stale belief that mis-delivered the frame here in the first
//! place. A cold view (no `PrimaryChanged` applied yet) answers `None`
//! and the mesh falls back to its documented fan/hold default.
//!
//! # Staleness / divergence contract
//!
//! Writes are epoch-gated (the role-table epoch rides each publish), so
//! an older mirror on a multi-coordinator process can never regress a
//! newer holder fact. ACROSS processes the views may transiently
//! diverge (CRDT convergence in flight); the mesh's relay loop-guard
//! (the relay ring in `routing.rs`) bounds the worst case — a frame
//! bouncing between stale views comes to rest in a hold/fan at the
//! first revisited process, never cycling.

use std::sync::{Arc, Mutex};

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::address::PeerId;

use crate::cluster_state::ClusterState;

/// The published holder fact: the Primary role's host peer-id (or
/// `None` while unknown), with the role-table epoch it was read at.
#[derive(Debug, Clone, Default)]
struct PrimaryHolder {
    holder: Option<PeerId>,
    epoch: u64,
}

/// A cloneable, coordinator-published view of which peer hosts the
/// Primary role, read by the mesh ingress relay.
///
/// Every clone shares one cell. The coordinators hold the write side
/// (via the role-change hook [`attach_primary_recognition`] registers);
/// the [`super::Mesh`] reads [`RoleHolderView::primary`] at ingress.
#[derive(Clone, Default)]
pub struct RoleHolderView {
    inner: Arc<Mutex<PrimaryHolder>>,
}

impl RoleHolderView {
    /// A fresh view knowing no holder until the first publish.
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish the Primary holder read off a role table at `epoch`.
    ///
    /// Epoch-gated last-writer-wins: a publish at an OLDER epoch than
    /// the stored fact is a no-op, so a behind mirror on a
    /// multi-coordinator process can never regress the view. Same-epoch
    /// publishes apply (the mirrors carry the same fact there).
    pub fn publish_primary(&self, holder: Option<&str>, epoch: u64) {
        let mut guard = self.inner.lock().expect("role holder view poisoned");
        if epoch < guard.epoch {
            return;
        }
        guard.epoch = epoch;
        guard.holder = holder.map(PeerId::from);
    }

    /// The last-published Primary holder, if any coordinator on this
    /// process has recognized one.
    pub fn primary(&self) -> Option<PeerId> {
        self.inner
            .lock()
            .expect("role holder view poisoned")
            .holder
            .clone()
    }
}

/// Attach the recognition→routing publish onto a coordinator's
/// `cluster_state`: seed `view` from the CURRENT role table (the state
/// may already be converged — a relocation handoff or a
/// snapshot-seeded promotion), then register a role-change hook so
/// every later `PrimaryChanged` apply / snapshot heal republishes.
///
/// The single wiring point every coordinator constructor calls (the
/// same attach-at-construction shape as
/// [`crate::observer::attach_observer_announcer`]); the coordinators
/// know nothing about the mesh's relay decision and the mesh knows
/// nothing about the CRDT — this hook is the entire boundary.
pub fn attach_primary_recognition<I: Identifier>(
    cluster_state: &mut ClusterState<I>,
    view: RoleHolderView,
) {
    use dynrunner_protocol_primary_secondary::RoleChangeHookRegistrar;
    let epoch_mirror = cluster_state.primary_epoch_mirror();
    // Seed from the possibly-already-warm table: the hook only fires on
    // FUTURE mutations, and a handoff/promotion-seeded state has the
    // holder fact already applied.
    view.publish_primary(
        cluster_state.role_table().primary.as_deref(),
        epoch_mirror.load(std::sync::atomic::Ordering::Acquire),
    );
    cluster_state.register_role_change_hook(Box::new(move |table| {
        view.publish_primary(
            table.primary.as_deref(),
            epoch_mirror.load(std::sync::atomic::Ordering::Acquire),
        );
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_core::RunnerIdentifier;
    use dynrunner_protocol_primary_secondary::cluster_mutation::ClusterMutation;

    /// A fresh view knows no holder; a publish moves it; clones share
    /// the same cell.
    #[test]
    fn publish_is_observed_by_clones() {
        let view = RoleHolderView::new();
        assert_eq!(view.primary(), None);

        let reader = view.clone();
        view.publish_primary(Some("secondary-0"), 1);
        assert_eq!(reader.primary(), Some(PeerId::from("secondary-0")));
    }

    /// Epoch gating: an older-epoch publish never regresses the stored
    /// fact; a same-or-newer epoch applies (including clearing back to
    /// `None` at a newer epoch).
    #[test]
    fn publish_is_epoch_gated() {
        let view = RoleHolderView::new();
        view.publish_primary(Some("promoted"), 3);
        // A behind mirror (epoch 1) cannot regress the holder.
        view.publish_primary(Some("stale"), 1);
        assert_eq!(view.primary(), Some(PeerId::from("promoted")));
        // A same-epoch publish applies (mirrors at the same generation).
        view.publish_primary(Some("promoted"), 3);
        assert_eq!(view.primary(), Some(PeerId::from("promoted")));
        // A newer epoch may move (or clear) the fact.
        view.publish_primary(None, 4);
        assert_eq!(view.primary(), None);
    }

    /// `attach_primary_recognition` seeds the view from an
    /// already-warm role table AND republishes on every later
    /// `PrimaryChanged` apply — the end-to-end wiring the coordinator
    /// constructors rely on (mirrors
    /// `attach_observer_announcer_fires_on_primary_changed`).
    #[test]
    fn attach_seeds_then_tracks_primary_changed() {
        let mut state = ClusterState::<RunnerIdentifier>::new();
        // Warm the state BEFORE attaching (the handoff/promotion shape).
        let outcome = state.apply(ClusterMutation::PrimaryChanged {
            new: "secondary-0".into(),
            epoch: 1,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        });
        assert_eq!(outcome, crate::cluster_state::ApplyOutcome::Applied);

        let view = RoleHolderView::new();
        attach_primary_recognition(&mut state, view.clone());
        assert_eq!(
            view.primary(),
            Some(PeerId::from("secondary-0")),
            "attach must SEED from the already-converged table"
        );

        // A later identity advance republishes through the hook.
        let outcome = state.apply(ClusterMutation::PrimaryChanged {
            new: "secondary-2".into(),
            epoch: 2,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        });
        assert_eq!(outcome, crate::cluster_state::ApplyOutcome::Applied);
        assert_eq!(view.primary(), Some(PeerId::from("secondary-2")));
    }
}
