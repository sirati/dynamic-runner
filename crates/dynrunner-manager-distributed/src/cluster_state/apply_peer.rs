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

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::RemovalCause;

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
    pub(super) fn apply_peer_joined(&mut self, peer_id: String, is_observer: bool) -> ApplyOutcome {
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
        if observer_set_changed {
            self.fire_role_change_hooks();
        }
        if !entry_was_new && !observer_set_changed {
            return ApplyOutcome::NoOp;
        }
        self.emit_lifecycle_event(PeerLifecycleEvent::Added {
            id: peer_id,
            is_observer,
        });
        ApplyOutcome::Applied
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
    pub(super) fn apply_peer_removed(&mut self, id: String, cause: RemovalCause) -> ApplyOutcome {
        if let Some(entry) = self.peer_state.get(&id)
            && entry.state == PeerState::Dead
        {
            return ApplyOutcome::NoOp;
        }
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
}
