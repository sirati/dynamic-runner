//! Peer-lifecycle apply rules.
//!
//! Single concern: the `ClusterMutation` arms that mutate per-peer state —
//! `PeerJoined`, `SetCanBePrimary`, `PeerRemoved`, and
//! `PeerResourceHoldingsUpdated`. Each enforces a sticky-per-id
//! invariant (a `Dead` id is terminal-locked against further
//! `PeerJoined` / `PeerRemoved`) or an epoch-supersede rule (the
//! resource-holdings announce is dropped if its epoch is older than
//! the local `primary_epoch`).
//!
//! Role-capability convergence (C6): the `is_observer` / `can_be_primary`
//! capabilities live in EXACTLY ONE replicated place — the `capabilities`
//! 2P-set — merged here via `merge_capability`. The `RoleTable.observers`
//! / `RoleTable.can_be_primary` sets are materialized by the single
//! `reproject_roles` helper (capability × local-alive), the SOLE producer
//! of both sets — no per-arm role-set surgery. `reproject_roles` is the
//! ONE place liveness composes with capability, and it is a LOCAL read
//! (never replicated). The peer-lifecycle dispatcher fan-out is wired
//! here; the central `apply` dispatch in sibling `apply.rs` delegates the
//! arms to these methods.

use std::collections::HashSet;

use dynrunner_core::{Identifier, ResourceAmount, TaskVersion};
use dynrunner_protocol_primary_secondary::{RemovalCause, SecondaryCapacityRecord};

use super::merge::merge_capability;
use super::types::{CapabilityEntry, PeerEntry, PeerState};
use super::{ApplyOutcome, ClusterState};
use crate::peer_lifecycle::PeerLifecycleEvent;

impl<I: Identifier> ClusterState<I> {
    /// Rebuild BOTH `RoleTable.observers` and `RoleTable.can_be_primary`
    /// from the `capabilities` 2P-set composed with the LOCAL `peer_state`
    /// alive bit, then fire the role-change hooks (C6). The SOLE producer
    /// of both role sets — every apply / restore path that touches EITHER
    /// `capabilities` OR `peer_state` liveness calls this, so there is no
    /// per-arm role-set insert/remove bookkeeping (no duplicated logic).
    ///
    /// This is the ONE place liveness composes with capability, and it is
    /// a LOCAL read — never replicated. A `Departed`-tombstoned or
    /// `Dead`/never-seen id projects OUT of both sets for free
    /// (`Advertised` ∧ `Alive`); a capability the node converged via the
    /// 2P-set projects IN the moment the node also holds the peer Alive.
    ///
    /// Fires the hooks UNCONDITIONALLY after rebuilding: the production
    /// registrant (the transport write-through `RoleTable` cache) needs to
    /// observe the post-projection table whenever a role-bearing mutation
    /// applied. The `Applied`/`NoOp` accounting at each call site already
    /// gates whether a mutation reached this helper at all, so a NoOp
    /// re-delivery never calls it.
    pub(super) fn reproject_roles(&mut self) {
        let mut observers = HashSet::new();
        let mut can_be_primary = HashSet::new();
        for (id, entry) in &self.capabilities {
            let CapabilityEntry::Advertised {
                is_observer,
                can_be_primary: cbp,
                ..
            } = entry
            else {
                // Departed tombstone projects out of both sets.
                continue;
            };
            let alive = self
                .peer_state
                .get(id)
                .is_some_and(|e| e.state == PeerState::Alive);
            if !alive {
                continue;
            }
            if *is_observer {
                observers.insert(id.clone());
            }
            if *cbp {
                can_be_primary.insert(id.clone());
            }
        }
        self.role_table.observers = observers;
        self.role_table.can_be_primary = can_be_primary;
        self.fire_role_change_hooks();
    }

    /// Merge one `Advertised` capability for `peer_id` into the 2P-set via
    /// `merge_capability` and return whether the stored entry actually
    /// changed (the `Applied` signal — it gates re-broadcast and the
    /// digest fold). A `Departed` tombstone absorbs the advertise (no
    /// change), so a capability advertise for a removed id is inert.
    fn merge_advertised_capability(
        &mut self,
        peer_id: &str,
        is_observer: bool,
        can_be_primary: bool,
        cap_version: TaskVersion,
    ) -> bool {
        let incoming = CapabilityEntry::Advertised {
            is_observer,
            can_be_primary,
            cap_version,
        };
        let merged = match self.capabilities.get(peer_id) {
            Some(local) => merge_capability(local, &incoming),
            None => incoming,
        };
        let changed = self.capabilities.get(peer_id) != Some(&merged);
        if changed {
            self.capabilities.insert(peer_id.to_string(), merged);
        }
        changed
    }

    /// Apply a `ClusterMutation::PeerJoined`.
    ///
    /// Sticky-per-id removal wins: if the id is currently `Dead` in
    /// `peer_state`, the broadcast is logged at `warn` and dropped
    /// (NoOp). Otherwise the entry is brought to `Alive` and the join's
    /// `(is_observer, can_be_primary)` advertisement is merged into the
    /// `capabilities` 2P-set (cap_version stamped at origination); a
    /// `Departed` tombstone absorbs it (a removed id never resurrects its
    /// capability). The `RoleTable` sets are rebuilt by `reproject_roles`
    /// whenever liveness or capability changed, firing the role-change
    /// hooks. A `PeerLifecycleEvent::Added` is emitted on every
    /// state-changing apply; pure-idempotent re-deliveries return NoOp.
    pub(super) fn apply_peer_joined(
        &mut self,
        peer_id: String,
        is_observer: bool,
        can_be_primary: bool,
        cap_version: TaskVersion,
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
        // Liveness: insert a fresh Alive entry if first-seen. (An already-
        // Alive entry is unchanged — the liveness bit is idempotent.)
        let entry_was_new = match self.peer_state.get(&peer_id) {
            None => {
                self.peer_state.insert(
                    peer_id.clone(),
                    PeerEntry {
                        state: PeerState::Alive,
                        pubkey: None,
                        endpoint: None,
                    },
                );
                true
            }
            Some(_) => false,
        };
        // Capability: merge the advertisement into the 2P-set.
        let capability_changed =
            self.merge_advertised_capability(&peer_id, is_observer, can_be_primary, cap_version);
        if entry_was_new || capability_changed {
            // Either liveness or capability changed → rebuild the role
            // projections (and fire hooks) from the post-mutation state.
            self.reproject_roles();
        } else {
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
    /// Runtime client update of a peer's explicit primary-capability,
    /// merged into the `capabilities` 2P-set (the higher `cap_version`
    /// wins, so a newer `false` beats an older `true`). Idempotent —
    /// re-applying a value that does not change the merged entry returns
    /// `NoOp`. A genuine capability change rebuilds the role projections
    /// (`reproject_roles`, firing the hooks).
    ///
    /// Capability is decoupled from membership/liveness at the APPLY
    /// level: this rule does NOT gate on `peer_state` (a client may
    /// pre-arm or revoke a peer's capability around its join). The
    /// liveness AND is applied only at READ-projection time — a pre-armed
    /// capability for a not-yet-joined peer is held in the 2P-set and
    /// projects into `RoleTable.can_be_primary` once the peer is Alive.
    pub(super) fn apply_set_can_be_primary(
        &mut self,
        peer_id: String,
        can_be_primary: bool,
        cap_version: TaskVersion,
    ) -> ApplyOutcome {
        // Preserve the observed observer bit (capability is an upward
        // ratchet on `is_observer`; this mutation only sets cbp). If the
        // peer has no capability entry yet, default observer = false.
        let is_observer = match self.capabilities.get(&peer_id) {
            Some(CapabilityEntry::Advertised { is_observer, .. }) => *is_observer,
            _ => false,
        };
        let changed =
            self.merge_advertised_capability(&peer_id, is_observer, can_be_primary, cap_version);
        if changed {
            self.reproject_roles();
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
    /// out-of-order `PeerJoined` for the same id. A `Departed` tombstone
    /// is written into the `capabilities` 2P-set (genuine departure
    /// dominates any earlier `Advertised`), and `reproject_roles` drops
    /// the id from both role sets for free (Departed/Dead projects out).
    /// A `PeerLifecycleEvent::Removed` is emitted on every state-changing
    /// apply.
    pub(super) fn apply_peer_removed(&mut self, id: String, cause: RemovalCause) -> ApplyOutcome {
        if let Some(entry) = self.peer_state.get(&id)
            && entry.state == PeerState::Dead
        {
            return ApplyOutcome::NoOp;
        }
        // Liveness: mark Dead (sticky) / insert a Dead entry if absent.
        match self.peer_state.get_mut(&id) {
            None => {
                self.peer_state.insert(
                    id.clone(),
                    PeerEntry {
                        state: PeerState::Dead,
                        pubkey: None,
                        endpoint: None,
                    },
                );
            }
            Some(entry) => {
                entry.state = PeerState::Dead;
            }
        }
        // Capability: write the 2P-set Departed tombstone (dominates any
        // Advertised; `merge_capability` keeps it sticky on re-merge).
        self.capabilities
            .insert(id.clone(), CapabilityEntry::Departed);
        // Rebuild the role projections (and fire hooks) — the Departed +
        // Dead id projects out of both sets.
        self.reproject_roles();
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
