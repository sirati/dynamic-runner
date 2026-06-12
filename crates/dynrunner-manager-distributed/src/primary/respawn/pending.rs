//! The pending-replacement table and its derived "awaiting a join" gate.
//!
//! # Single concern
//!
//! Own the node-local map of minted replacement id → removed member it
//! replaces, and keep ONE derived fact in sync with it: is the respawn
//! pipeline currently awaiting any replacement's `PeerJoined`? That fact
//! is the gate the [`super::listener`] consults to decide whether an
//! `Added` lifecycle event is respawn-relevant at all.
//!
//! # Why the gate exists
//!
//! `Removed` lifecycle events are ALWAYS respawn-relevant (a death is a
//! spawn trigger). `Added` events are respawn-relevant ONLY while a
//! replacement is pending — that is the sole window in which
//! [`super::handler::PrimaryCoordinator::reconcile_replacements_on_join`]
//! can do work (claim a joined replacement, or revoke a squatter when the
//! original re-admits). With no replacement pending, every `Added` event
//! is a guaranteed two-probe no-op. On a busy mesh those joins arrive
//! continuously, so an ungated respawn arm woke (and inflated its
//! `respawn_request` arm-stat) once per membership join with nothing to
//! do — the run_20260612_035105 busy-arm face. Gating `Added` delivery on
//! this flag lets the arm genuinely park (`Pending`) whenever the pipeline
//! is idle, so each surviving wake is real respawn work.
//!
//! # Why a derived flag and not a `len()` probe at the listener
//!
//! The listener runs on the lifecycle-dispatcher task, not the operational
//! loop, so it cannot borrow the coordinator's `pending_replacements`. The
//! flag is a shared [`AtomicBool`] the table updates on every mutation, so
//! the listener reads the current truth with one relaxed load and never
//! touches coordinator-owned state. The flag is a pure FUNCTION of the
//! map's emptiness (`!is_empty()`), re-derived after every insert/remove —
//! there is no second source of truth to drift.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Minted replacement id → the removed member it replaces, plus the
/// derived "awaiting a join" gate kept in lockstep with the map's
/// emptiness. Every mutation routes through this type's methods so the
/// gate can never drift from the map.
#[derive(Default)]
pub(crate) struct PendingReplacements {
    table: HashMap<String, String>,
    /// `true` iff `table` is non-empty. Shared with the respawn listener
    /// (a cheap `Arc` clone) so it can gate `Added`-event delivery without
    /// reaching into coordinator state.
    awaiting_join: Arc<AtomicBool>,
}

impl PendingReplacements {
    /// A shared handle to the "awaiting a join" gate for the listener.
    /// Reads `true` exactly while at least one replacement is pending.
    pub(crate) fn awaiting_join_gate(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.awaiting_join)
    }

    /// Record a freshly-minted replacement (`new_id`) and the member it
    /// replaces (`original_id`). Re-derives the gate.
    pub(crate) fn insert(&mut self, new_id: String, original_id: String) {
        self.table.insert(new_id, original_id);
        self.sync_gate();
    }

    /// Drop the entry for `new_id` (the minted replacement), returning the
    /// `original_id` it replaced if present. Re-derives the gate.
    pub(crate) fn remove(&mut self, new_id: &str) -> Option<String> {
        let removed = self.table.remove(new_id);
        self.sync_gate();
        removed
    }

    /// The minted replacement ids whose `original_id` is `original` — the
    /// squatters to revoke when `original` re-admits.
    pub(crate) fn replacements_of(&self, original: &str) -> Vec<String> {
        self.table
            .iter()
            .filter(|(_, original_id)| original_id.as_str() == original)
            .map(|(new_id, _)| new_id.clone())
            .collect()
    }

    /// Re-derive the gate from the map's emptiness — the single place the
    /// flag is ever written, so it cannot diverge from the table.
    fn sync_gate(&self) {
        self.awaiting_join
            .store(!self.table.is_empty(), Ordering::Relaxed);
    }
}

// Read-only inspection accessors — the operational handlers act through
// `insert` / `remove` / `replacements_of`; these surface table state for
// assertions and any future read site without exposing mutation that could
// bypass the gate.
impl PendingReplacements {
    /// The `original_id` recorded for a minted replacement id, if pending.
    #[cfg(test)]
    pub(crate) fn get(&self, new_id: &str) -> Option<&String> {
        self.table.get(new_id)
    }

    /// Whether `new_id` is currently pending.
    #[cfg(test)]
    pub(crate) fn contains_key(&self, new_id: &str) -> bool {
        self.table.contains_key(new_id)
    }

    /// Whether no replacement is pending (the gate reads `false`).
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// The original ids of every pending replacement.
    #[cfg(test)]
    pub(crate) fn values(&self) -> impl Iterator<Item = &String> {
        self.table.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The gate tracks the map's emptiness across the full insert →
    /// claim/revoke lifecycle, and a shared handle observes every change.
    #[test]
    fn gate_tracks_table_emptiness() {
        let mut pending = PendingReplacements::default();
        let gate = pending.awaiting_join_gate();
        assert!(!gate.load(Ordering::Relaxed), "idle starts un-gated");

        pending.insert("secondary-4".into(), "secondary-3".into());
        assert!(gate.load(Ordering::Relaxed), "a pending replacement gates on");

        pending.insert("secondary-5".into(), "secondary-2".into());
        assert!(gate.load(Ordering::Relaxed), "still on with two pending");

        assert_eq!(
            pending.remove("secondary-4").as_deref(),
            Some("secondary-3")
        );
        assert!(
            gate.load(Ordering::Relaxed),
            "still on while one remains pending"
        );

        assert_eq!(
            pending.remove("secondary-5").as_deref(),
            Some("secondary-2")
        );
        assert!(
            !gate.load(Ordering::Relaxed),
            "the last claim returns to idle (un-gated)"
        );
    }

    /// `replacements_of` returns every minted id pending for one original
    /// (the squatter set) and nothing for an unrelated original.
    #[test]
    fn replacements_of_returns_the_squatter_set() {
        let mut pending = PendingReplacements::default();
        pending.insert("secondary-4".into(), "secondary-3".into());
        pending.insert("secondary-5".into(), "secondary-3".into());
        pending.insert("secondary-6".into(), "secondary-2".into());

        let mut squatters = pending.replacements_of("secondary-3");
        squatters.sort();
        assert_eq!(squatters, vec!["secondary-4", "secondary-5"]);

        assert!(pending.replacements_of("secondary-9").is_empty());
    }
}
