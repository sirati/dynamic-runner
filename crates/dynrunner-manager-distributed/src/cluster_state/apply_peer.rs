//! Peer-lifecycle apply rules.
//!
//! Single concern: the three `ClusterMutation` arms that mutate
//! per-peer state — `PeerJoined`, `PeerRemoved`, and
//! `PeerResourceHoldingsUpdated`. Each enforces a sticky-per-id
//! invariant (a `Dead` id is terminal-locked against further
//! `PeerJoined` / `PeerRemoved`) or an epoch-supersede rule (the
//! resource-holdings announce is dropped if its epoch is older than
//! the local `primary_epoch`). The role-table observer projection
//! and the peer-lifecycle dispatcher fan-out are wired here; the
//! central `apply` dispatch in sibling `apply.rs` delegates the
//! three arms to these methods.

use std::collections::HashSet;

use dynrunner_core::{Identifier, ResourceAmount};
use dynrunner_protocol_primary_secondary::{RemovalCause, SecondaryCapacityRecord};

use super::types::{PeerEntry, PeerState};
use super::{ApplyOutcome, ClusterState};
use crate::peer_lifecycle::PeerLifecycleEvent;

impl<I: Identifier> ClusterState<I> {
    /// Apply a `ClusterMutation::PeerJoined`.
    ///
    /// Sticky-per-id removal wins: if the id is currently `Dead` in
    /// `peer_state`, the broadcast is logged at `warn` and dropped
    /// (NoOp). Otherwise the entry is brought to `Alive` (insert or
    /// in-place ratchet of `is_observer` upward; the observer flag
    /// never regresses true→false via `PeerJoined`, only the matching
    /// `PeerRemoved` can clear it). The `RoleTable.observers`
    /// projection is updated in lockstep and role-change hooks fire
    /// when the set actually changes. A `PeerLifecycleEvent::Added`
    /// is emitted on every state-changing apply; pure-idempotent
    /// re-deliveries return NoOp and emit nothing.
    ///
    /// `can_be_primary` rides the same join the exact way `is_observer`
    /// does: when `true` the id is inserted into
    /// `RoleTable.can_be_primary` (the explicit per-peer capability set).
    /// The insert is independent of the observer projection and of
    /// liveness — capability is its own first-class fact. The set change
    /// participates in the same "state-changing apply" accounting as the
    /// observer set, so a join that advertises capability without
    /// touching the observer set is still `Applied`.
    pub(super) fn apply_peer_joined(
        &mut self,
        peer_id: String,
        is_observer: bool,
        can_be_primary: bool,
    ) -> ApplyOutcome {
        match self.peer_state.get(&peer_id) {
            Some(entry) if entry.state == PeerState::Dead => {
                tracing::warn!(
                    target: "dynrunner_cluster_state",
                    peer_id = %peer_id,
                    "PeerJoined for dead id ignored",
                );
                return ApplyOutcome::NoOp;
            }
            _ => {}
        }
        // Capability projection: insert when the join advertises it.
        // `HashSet::insert` returns `true` only on a genuine widening,
        // so a re-advertised capability is idempotent. Recorded before
        // the per-id `peer_state` match so the role-change hook (fired
        // on any set change below) observes the post-mutation set.
        let capability_set_changed =
            can_be_primary && self.role_table.can_be_primary.insert(peer_id.clone());
        let (entry_was_new, observer_set_changed) = match self.peer_state.get_mut(&peer_id) {
            None => {
                self.peer_state.insert(
                    peer_id.clone(),
                    PeerEntry {
                        state: PeerState::Alive,
                        pubkey: None,
                        endpoint: None,
                        is_observer,
                    },
                );
                let observer_set_changed =
                    is_observer && self.role_table.observers.insert(peer_id.clone());
                (true, observer_set_changed)
            }
            Some(entry) => {
                // Ratchet the observer flag upward only. Stale flip-
                // back broadcasts (`is_observer = false` for an
                // already-observed peer) must not regress the
                // projection — only `PeerRemoved` clears observer
                // status.
                if is_observer && !entry.is_observer {
                    entry.is_observer = true;
                    let inserted = self.role_table.observers.insert(peer_id.clone());
                    (false, inserted)
                } else {
                    (false, false)
                }
            }
        };
        if observer_set_changed || capability_set_changed {
            self.fire_role_change_hooks();
        }
        if !entry_was_new && !observer_set_changed && !capability_set_changed {
            return ApplyOutcome::NoOp;
        }
        self.emit_lifecycle_event(PeerLifecycleEvent::Added {
            id: peer_id,
            is_observer,
        });
        ApplyOutcome::Applied
    }

    /// Apply a `ClusterMutation::SetCanBePrimary`.
    ///
    /// Runtime client update of a peer's explicit primary-capability:
    /// `true` widens `RoleTable.can_be_primary`, `false` removes the id.
    /// Idempotent — re-applying the current value (a no-op set
    /// operation) returns `NoOp` and fires no hook. A genuine change
    /// fires the role-change hooks so any registered write-through
    /// cache stays coherent, mirroring the observer-set update path.
    ///
    /// Capability is decoupled from membership/liveness: this rule does
    /// NOT gate on `peer_state` (a client may pre-arm or revoke a peer's
    /// capability around its join), and it never touches the observer
    /// projection. It is the dedicated steady-state writer for the
    /// capability set the way `PeerResourceHoldingsUpdated` is for
    /// holdings.
    pub(super) fn apply_set_can_be_primary(
        &mut self,
        peer_id: String,
        can_be_primary: bool,
    ) -> ApplyOutcome {
        let changed = if can_be_primary {
            self.role_table.can_be_primary.insert(peer_id)
        } else {
            self.role_table.can_be_primary.remove(&peer_id)
        };
        if changed {
            self.fire_role_change_hooks();
            ApplyOutcome::Applied
        } else {
            ApplyOutcome::NoOp
        }
    }

    /// Apply a `ClusterMutation::PeerRemoved`.
    ///
    /// Sticky-per-id: once `peer_state[id]` is `Dead`, any further
    /// `PeerRemoved` for the same id is a silent NoOp. An `Absent`
    /// id is inserted as `Dead` so the entry blocks any late
    /// out-of-order `PeerJoined` for the same id. Observers lose
    /// their projection on removal; role-change hooks fire when the
    /// set actually shrinks. A `PeerLifecycleEvent::Removed` is
    /// emitted on every state-changing apply.
    ///
    /// The primary-capability projection is cleared on removal too — the
    /// exact twin of the observer projection. A dead id never resurrects
    /// (sticky-per-id), so dropping it from `RoleTable.can_be_primary`
    /// keeps the capability set free of ids that can no longer host a
    /// primary.
    pub(super) fn apply_peer_removed(&mut self, id: String, cause: RemovalCause) -> ApplyOutcome {
        if let Some(entry) = self.peer_state.get(&id)
            && entry.state == PeerState::Dead
        {
            return ApplyOutcome::NoOp;
        }
        // Capability projection: a removed peer can no longer host the
        // primary. Drop it regardless of whether it was an observer.
        self.role_table.can_be_primary.remove(&id);
        let observer_set_changed = match self.peer_state.get_mut(&id) {
            None => {
                self.peer_state.insert(
                    id.clone(),
                    PeerEntry {
                        state: PeerState::Dead,
                        pubkey: None,
                        endpoint: None,
                        is_observer: false,
                    },
                );
                false
            }
            Some(entry) => {
                entry.state = PeerState::Dead;
                let was_observer = entry.is_observer;
                entry.is_observer = false;
                was_observer && self.role_table.observers.remove(&id)
            }
        };
        if observer_set_changed {
            self.fire_role_change_hooks();
        }
        self.emit_lifecycle_event(PeerLifecycleEvent::Removed { id, cause });
        ApplyOutcome::Applied
    }

    /// Apply a `ClusterMutation::PeerResourceHoldingsUpdated`.
    ///
    /// Supersede-old-pending semantics on `epoch`: an announce whose
    /// `epoch` is strictly older than the current `primary_epoch` is
    /// dropped as stale (the announcing peer hadn't yet learned of
    /// the current primary when it sent). Same-or-newer epoch is
    /// accepted — the announce is about per-peer holdings, not
    /// about primary identity, and a peer that already learned of a
    /// newer primary before its announce reached us is still
    /// authoritative about its own holdings.
    ///
    /// Replace-if-changed: the incoming `Vec<String>` is collected
    /// into a `HashSet<String>` (so duplicate strings inside a
    /// single announce collapse) and compared against the stored
    /// set for the same `peer_id`. Unchanged → NoOp; changed (or
    /// first-time insertion) → replace and return `Applied`.
    ///
    /// No `peer_state` liveness gate today: a `PeerResourceHoldingsUpdated`
    /// for a peer the CRDT has never seen `PeerJoined` for is
    /// accepted (the announce IS evidence the peer is alive enough
    /// to send); for a peer marked `Dead` in `peer_state` the
    /// announce is still recorded but downstream consumers reading
    /// `peer_state` alongside `peer_holdings` can apply their own
    /// liveness filter. The CRDT layer's only contract is "store
    /// what was announced under the supersede-by-epoch rule"; a
    /// liveness filter belongs to the consumer policy.
    pub(super) fn apply_peer_resource_holdings_updated(
        &mut self,
        peer_id: String,
        holdings: Vec<String>,
        epoch: u64,
    ) -> ApplyOutcome {
        if epoch < self.primary_epoch {
            return ApplyOutcome::NoOp;
        }
        let incoming: HashSet<String> = holdings.into_iter().collect();
        match self.peer_holdings.get(&peer_id) {
            Some(existing) if existing == &incoming => ApplyOutcome::NoOp,
            _ => {
                self.peer_holdings.insert(peer_id, incoming);
                ApplyOutcome::Applied
            }
        }
    }

    /// Apply a `ClusterMutation::SecondaryCapacity`.
    ///
    /// Set-once: the first apply for a given `secondary` records its
    /// static capacity (worker-slot count + advertised resources);
    /// every subsequent apply for the same id is an idempotent NoOp.
    /// Capacity is static for the secondary's lifetime in the run, so
    /// re-application (snapshot replay, redundant peer-forwarding, the
    /// idempotent `PeerJoined` re-emit from `send_peer_lists`) must not
    /// clobber the first-recorded value — mirroring the static-config
    /// shape of the `PhaseDepsSet` arm, keyed per-secondary.
    pub(super) fn apply_secondary_capacity(
        &mut self,
        secondary: String,
        worker_count: u32,
        resources: Vec<ResourceAmount>,
    ) -> ApplyOutcome {
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.secondary_capacities.entry(secondary)
        {
            e.insert(SecondaryCapacityRecord {
                worker_count,
                resources,
            });
            ApplyOutcome::Applied
        } else {
            ApplyOutcome::NoOp
        }
    }
}
