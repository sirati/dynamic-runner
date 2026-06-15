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
use std::time::Duration;

use dynrunner_core::{Identifier, ResourceAmount, TaskVersion};
use dynrunner_protocol_primary_secondary::{
    RemovalCause, SecondaryCapacityRecord, SecondaryResourceSampleRecord,
};

use super::merge::merge_capability;
use super::types::{CapabilityEntry, PeerEntry, PeerState};
use super::{ApplyOutcome, ClusterState};
use crate::peer_lifecycle::PeerLifecycleEvent;
use crate::warn_throttle::WarnThrottle;

/// Minimum spacing between two "PeerJoined for dead id" WARNs FOR THE SAME
/// PEER (#416). A removed-but-alive peer re-applies the same non-advancing
/// `PeerJoined` on every authenticated frame until its transport leg
/// re-admits; an untrottled WARN spammed for 45+ min in
/// run_20260611_123632. A minute-cadence per-peer gate keeps the
/// re-admission stall narrated (one line + a suppressed count) without one
/// line per frame.
const DEAD_REJOIN_WARN_INTERVAL: Duration = Duration::from_secs(60);

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
    /// digest fold). A SAME-generation `Departed` tombstone absorbs the
    /// advertise (no change), so a stale capability advertise for a
    /// removed id is inert; a strictly-higher-generation advertise (a
    /// re-admission) supersedes the tombstone through the generation-
    /// first merge.
    fn merge_advertised_capability(
        &mut self,
        peer_id: &str,
        is_observer: bool,
        can_be_primary: bool,
        cap_version: TaskVersion,
        member_gen: u64,
    ) -> bool {
        let incoming = CapabilityEntry::Advertised {
            is_observer,
            can_be_primary,
            cap_version,
            member_gen,
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
    /// Sticky-per-GENERATION removal wins: if the id is currently `Dead`
    /// in `peer_state` and the join's `member_gen` is NOT strictly above
    /// the entry's generation, the broadcast is logged at `warn` and
    /// dropped (NoOp) — the original sticky rule, scoped to one
    /// membership incarnation, so a late/reordered stale `PeerJoined`
    /// still cannot resurrect an authoritative removal. A join whose
    /// `member_gen` IS strictly above a `Dead` entry's generation
    /// RE-ADMITS the id (removal at gen N, rejoin at gen N+1 — the
    /// primary's frame-ingest re-admission seam is the sole originator
    /// of the bump): the entry returns to `Alive` at the new generation
    /// and the join's advertisement supersedes the capability tombstone
    /// through the generation-first `merge_capability`. Otherwise the
    /// entry is brought to `Alive` and the join's `(is_observer,
    /// can_be_primary)` advertisement is merged into the `capabilities`
    /// 2P-set (cap_version stamped at origination). The `RoleTable` sets
    /// are rebuilt by `reproject_roles` whenever liveness or capability
    /// changed, firing the role-change hooks. A
    /// `PeerLifecycleEvent::Added` is emitted on every state-changing
    /// apply; pure-idempotent re-deliveries return NoOp.
    pub(super) fn apply_peer_joined(
        &mut self,
        peer_id: String,
        is_observer: bool,
        can_be_primary: bool,
        cap_version: TaskVersion,
        member_gen: u64,
    ) -> ApplyOutcome {
        match self.peer_state.get(&peer_id) {
            Some(entry) if entry.state == PeerState::Dead && member_gen <= entry.member_gen => {
                // SLURM-AUTHORITATIVE TIEBREAK (#546): the sticky-removal
                // may have been a false-positive (local view declared the
                // peer dead while slurm still has the original's job
                // running — the run_20260615_112332 face). Consult the
                // off-loop authority snapshot; if it shows the peer
                // ALIVE, REVERSE the dead-mark and re-admit at the
                // EXISTING entry_gen (no generation bump — this is
                // correction-of-our-error, not a new incarnation,
                // matching the #540 phase-may-be-empty symmetry).
                //
                // Fail-closed on Unknown / stale snapshot / no snapshot
                // wired: keep the sticky-removal. Without positive
                // evidence of life we don't reverse the declaration.
                use crate::authority_snapshot::PeerLifeState;
                let alive_via_authority = self
                    .authority_snapshot
                    .as_ref()
                    .is_some_and(|s| matches!(s.peer_life(&peer_id), PeerLifeState::Alive));

                if alive_via_authority {
                    tracing::warn!(
                        target: "dynrunner_cluster_state",
                        peer_id = %peer_id,
                        entry_gen = entry.member_gen,
                        join_gen = member_gen,
                        "PeerJoined for dead id at non-advancing gen — slurm-authoritative \
                         evidence shows the peer's job is ALIVE; REVERSING the sticky-removal \
                         (the prior dead-declaration was a false-positive from local \
                         deafness — see #543/#544/#546)",
                    );
                    // Fall through to the liveness-update path below.
                    // The Dead-entry branch there handles the state flip
                    // and (per the gen-unchanged branch) keeps the
                    // existing entry_gen on a reversal.
                } else {
                    // Per-peer throttle (#416): a removed-but-alive peer
                    // re-applies this same non-advancing join on EVERY
                    // frame until its transport leg re-admits — emit once
                    // per peer per minute, carrying the suppressed count,
                    // never one line per frame.
                    let entry_gen = entry.member_gen;
                    if let Some(suppressed) = self
                        .dead_rejoin_warn
                        .entry(peer_id.clone())
                        .or_insert_with(|| WarnThrottle::new(DEAD_REJOIN_WARN_INTERVAL))
                        .permit()
                    {
                        tracing::warn!(
                            target: "dynrunner_cluster_state",
                            peer_id = %peer_id,
                            entry_gen = entry_gen,
                            join_gen = member_gen,
                            suppressed_since_last_warn = suppressed,
                            "PeerJoined for dead id at a non-advancing generation ignored \
                             (sticky removal within the membership incarnation)",
                        );
                    }
                    return ApplyOutcome::NoOp;
                }
            }
            _ => {}
        }
        // Liveness: insert a fresh Alive entry if first-seen; RE-ADMIT a
        // Dead entry whose generation the join strictly advances; adopt a
        // strictly-higher generation onto an already-Alive entry (a
        // re-admission echo whose removal this node never observed). An
        // already-Alive entry at the same-or-higher generation is
        // unchanged — the liveness bit is idempotent.
        let liveness_changed = match self.peer_state.get_mut(&peer_id) {
            None => {
                self.peer_state.insert(
                    peer_id.clone(),
                    PeerEntry {
                        state: PeerState::Alive,
                        member_gen,
                        pubkey: None,
                        endpoint: None,
                    },
                );
                true
            }
            Some(entry) if entry.state == PeerState::Dead => {
                // Two cases reach here:
                // (a) STANDARD RE-ADMISSION: member_gen > entry.member_gen
                //     (the frame-ingest seam advanced the gen on this
                //     dead-but-alive sender's frame). Adopt the new gen.
                // (b) STICKY-REMOVAL REVERSAL via the slurm-authoritative
                //     tiebreak (#546): member_gen <= entry.member_gen,
                //     authority said Alive. Keep the existing entry_gen
                //     (this is not a new incarnation — it is
                //     correction-of-our-error).
                if member_gen > entry.member_gen {
                    tracing::warn!(
                        target: "dynrunner_cluster_state",
                        peer_id = %peer_id,
                        from_gen = entry.member_gen,
                        to_gen = member_gen,
                        "re-admitting removed peer at an advanced membership \
                         generation (its authenticated frames prove it alive)",
                    );
                    entry.member_gen = member_gen;
                }
                entry.state = PeerState::Alive;
                true
            }
            Some(entry) => {
                let advanced = member_gen > entry.member_gen;
                if advanced {
                    entry.member_gen = member_gen;
                }
                advanced
            }
        };
        // Capability: merge the advertisement into the 2P-set (generation-
        // first, so a re-admission supersedes the Departed tombstone).
        let capability_changed = self.merge_advertised_capability(
            &peer_id,
            is_observer,
            can_be_primary,
            cap_version,
            member_gen,
        );
        if liveness_changed || capability_changed {
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
        // The update targets the CURRENT membership incarnation: merge at
        // the existing capability entry's generation (falling back to the
        // liveness entry's, then 0) so the generation-first merge lands in
        // the same-generation `Advertised` fold where `cap_version`
        // arbitrates — a lower stamped generation would be dropped
        // outright, a higher one would clobber the incarnation key.
        let member_gen = self
            .capabilities
            .get(&peer_id)
            .map(super::merge::capability_member_gen)
            .or_else(|| self.peer_state.get(&peer_id).map(|e| e.member_gen))
            .unwrap_or(0);
        let changed = self.merge_advertised_capability(
            &peer_id,
            is_observer,
            can_be_primary,
            cap_version,
            member_gen,
        );
        if changed {
            self.reproject_roles();
            ApplyOutcome::Applied
        } else {
            ApplyOutcome::NoOp
        }
    }

    /// Apply a `ClusterMutation::PeerRemoved`.
    ///
    /// Sticky-per-GENERATION: once `peer_state[id]` is `Dead` at
    /// generation N, any further `PeerRemoved` for the same id at a
    /// generation `<= N` is a silent NoOp — and so is a removal whose
    /// generation is strictly BELOW an `Alive` entry's (a stale removal
    /// of an already-superseded membership incarnation must not re-bury
    /// the re-admitted live peer). A removal at the `Alive` entry's own
    /// generation kills that incarnation (the authoritative removal); a
    /// removal at a generation this node has never seen join is applied
    /// at that generation so it still blocks the late out-of-order
    /// `PeerJoined` of the same incarnation. An absent id is inserted as
    /// `Dead` for the same reason. A `Departed` tombstone PRESERVING the
    /// advertisement current at departure is merged into the
    /// `capabilities` 2P-set at the removal's generation (it dominates
    /// the same-generation `Advertised`; a later re-admission's
    /// higher-generation advertise supersedes it), and `reproject_roles`
    /// drops the id from both role sets for free (Departed/Dead projects
    /// out). A `PeerLifecycleEvent::Removed` is emitted on every
    /// state-changing apply.
    pub(super) fn apply_peer_removed(
        &mut self,
        id: String,
        cause: RemovalCause,
        member_gen: u64,
    ) -> ApplyOutcome {
        if let Some(entry) = self.peer_state.get(&id) {
            if entry.state == PeerState::Dead && member_gen <= entry.member_gen {
                return ApplyOutcome::NoOp;
            }
            if entry.state == PeerState::Alive && member_gen < entry.member_gen {
                // Stale removal of a superseded incarnation: the peer was
                // already re-admitted at a higher generation, so this
                // removal lost. Name the drop (silent-branch rule) — a
                // swallowed removal is a membership decision.
                tracing::info!(
                    target: "dynrunner_cluster_state",
                    peer_id = %id,
                    entry_gen = entry.member_gen,
                    removal_gen = member_gen,
                    "stale PeerRemoved for a superseded membership \
                     incarnation ignored (peer was re-admitted at a higher \
                     generation)",
                );
                return ApplyOutcome::NoOp;
            }
        }
        // Liveness: mark Dead (sticky within the incarnation) / insert a
        // Dead entry if absent. The entry's generation adopts the
        // removal's when higher (a removal observed before its join).
        match self.peer_state.get_mut(&id) {
            None => {
                self.peer_state.insert(
                    id.clone(),
                    PeerEntry {
                        state: PeerState::Dead,
                        member_gen,
                        pubkey: None,
                        endpoint: None,
                    },
                );
            }
            Some(entry) => {
                entry.state = PeerState::Dead;
                entry.member_gen = entry.member_gen.max(member_gen);
            }
        }
        // Capability: merge the 2P-set Departed tombstone at the removal's
        // generation, PRESERVING the advertisement current at departure so
        // a re-admission can restore the exact capability. Routed through
        // `merge_capability` so the generation-first rule arbitrates (a
        // higher-generation Advertised already present keeps winning).
        let (is_observer, can_be_primary, cap_version) = match self.capabilities.get(&id) {
            Some(CapabilityEntry::Advertised {
                is_observer,
                can_be_primary,
                cap_version,
                ..
            })
            | Some(CapabilityEntry::Departed {
                is_observer,
                can_be_primary,
                cap_version,
                ..
            }) => (*is_observer, *can_be_primary, *cap_version),
            None => (false, false, TaskVersion::default()),
        };
        let tombstone = CapabilityEntry::Departed {
            member_gen,
            is_observer,
            can_be_primary,
            cap_version,
        };
        let merged = match self.capabilities.get(&id) {
            Some(local) => super::merge::merge_capability(local, &tombstone),
            None => tombstone,
        };
        self.capabilities.insert(id.clone(), merged);
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

    /// Apply a `ClusterMutation::SecondaryResourceSample` (#575).
    ///
    /// LWW per `secondary` on `(member_gen, emitted_at_ms)`: the
    /// incoming record wins iff its stamp is strictly greater than
    /// the local entry's stamp (or the local has no entry). Equal or
    /// older stamps are NoOp.
    ///
    /// The two-component stamp's discipline: a respawned-member's
    /// fresh aggregate carries a higher `member_gen` and therefore
    /// strictly dominates whatever stale record the dead incarnation
    /// left in the map (so the observer's projection switches to the
    /// new incarnation's numbers on the next emit, not after the dead
    /// record falls out of some TTL). Within one membership the
    /// `emitted_at_ms` clock breaks ties — that is the steady-state
    /// 5-minute monotone advance.
    ///
    /// Snapshot replay / redundant re-broadcast / anti-entropy heal:
    /// each carries the same `(member_gen, emitted_at_ms)` stamp as
    /// the originating apply, so a duplicate landing finds an
    /// already-equal local stamp and NoOps — idempotent under at-
    /// least-once delivery (matching the broader CRDT contract).
    pub(super) fn apply_secondary_resource_sample(
        &mut self,
        secondary: String,
        record: SecondaryResourceSampleRecord,
    ) -> ApplyOutcome {
        let incoming = (record.member_gen, record.emitted_at_ms);
        match self.latest_resource_samples.entry(secondary) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(record);
                ApplyOutcome::Applied
            }
            std::collections::hash_map::Entry::Occupied(mut e) => {
                let local = e.get();
                let local_stamp = (local.member_gen, local.emitted_at_ms);
                if incoming > local_stamp {
                    e.insert(record);
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::NoOp
                }
            }
        }
    }
}
