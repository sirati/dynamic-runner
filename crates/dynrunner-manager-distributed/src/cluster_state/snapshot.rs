//! Snapshot type and CRDT-merge restore.
//!
//! Single concern: a serializable view of the entire `ClusterState`
//! (the `ClusterStateSnapshot<I>` type), the `snapshot()` deep-clone
//! capture, and the lattice-merge `restore()` that the snapshot RPC
//! callers (late joiner, reconnect) apply against local state.
//! Idempotent under repeated and overlapping snapshots per the
//! per-field merge rules documented on `ClusterStateSnapshot`.

use std::collections::{HashMap, HashSet};

use dynrunner_core::{Identifier, PhaseId, TaskInfo, TaskOutputs};
use dynrunner_protocol_primary_secondary::{RunMilestoneKind, SecondaryCapacityRecord};
use serde::{Deserialize, Serialize};

use super::ClusterState;
use super::TaskState;
use super::merge::merge_capability;
use super::types::{CapabilityEntry, PeerEntry, PeerState};

/// Serializable snapshot of an entire `ClusterState`. Used by the
/// snapshot RPC (`RequestClusterSnapshot` → `ClusterSnapshot`) so a
/// late-joining or reconnecting node can bootstrap its replicated
/// ledger from any peer.
///
/// Merge semantics on the receiver side (see `ClusterState::restore`):
///
/// - Per task: routed through the SHARED `merge_task_state` join (the
///   SAME canonical `task_join_key` order `apply` and the digest use, so
///   apply == restore == digest by construction). Band-first
///   (any terminal beats any non-terminal regardless of version), then
///   within the terminal band `{Failed, Unfulfillable} < Completed <
///   InvalidTask` (D-T) with the per-task version + payload content hash
///   settling same-rank divergence, and within the non-terminal band the
///   version arbitrates before rank (C3). A winning terminal emits its
///   `TaskCompletedEvent` and folds co-present outputs, exactly-once
///   (re-restore yields a NoOp).
/// - `(current_primary, primary_epoch)`: higher epoch wins.
/// - `phase_deps`: CRD-3 deterministic content-hash merge. Replaced if
///   local is empty (first-bootstrap); if both non-empty and the
///   canonical (order-independent) content hash DIFFERS, the LOWER
///   content-hash graph is adopted so two replicas that diverged across a
///   partition reconcile to the SAME graph regardless of pull order.
/// - `capabilities`: per-id 2P-set merge via `merge_capability` (C6 —
///   the SINGLE replicated source of `is_observer`/`can_be_primary`).
///   Monotone: a `Departed` tombstone sticks, `Advertised` ratchets
///   `is_observer` up and follows the higher `cap_version` for
///   `can_be_primary`. After merging, the `RoleTable.observers` /
///   `RoleTable.can_be_primary` projections are rebuilt from
///   `capability × local-alive` (`reproject_roles`). Because the 2P-set
///   IS snapshot-healable, the digest folds it (detect-WITH-heal) — a
///   flagged capability divergence converges in one pull.
///   `#[serde(default)]` keeps wire compat with a pre-field sender (a
///   missing field decodes as an empty map).
/// - `peer_holdings`: replaced if local is empty, otherwise kept —
///   same first-bootstrap-only contract as `observers`. The live
///   `PeerResourceHoldingsUpdated` apply path is the steady-state
///   writer; the snapshot field exists so a late-joiner sees
///   current per-peer holdings before any live announce arrives.
/// - `task_outputs`: per-key first-write-wins. Each entry is set
///   exactly once by the originating `TaskCompleted` apply arm, so
///   a snapshot's entry and a live-applied entry for the same
///   `task_id` carry the same value — the merge inserts a snapshot
///   entry only when the local map has no entry for that key. This
///   matches the `tasks` lattice's monotonic-terminal-wins shape
///   projected onto the cache's single-write-per-key semantics.
/// - `secondary_capacities`: per-secondary first-write-wins. Each
///   entry is set exactly once by the originating `SecondaryCapacity`
///   apply arm (set-once; capacity is static for the run), so a
///   snapshot's entry and a live-applied entry for the same secondary
///   carry the same value — the merge inserts a snapshot entry only
///   when the local map has no entry for that key. Same monotonic-
///   insertion shape as `task_outputs`. Carried so a freshly-promoted
///   primary and late-joining observers hold the full per-secondary
///   roster on snapshot-restore, before any live `SecondaryCapacity`
///   broadcast reaches them.
/// - `alive_members`: Dead-wins / sticky-removal merge, mirroring the
///   `PeerJoined`/`PeerRemoved` apply rules in `apply_peer.rs`. Each
///   incoming alive id is inserted as a fresh `Alive` `PeerEntry`
///   ONLY IF the local `peer_state` has no entry for it — never
///   overwriting a local `Dead` (sticky removal wins, exactly as
///   `apply_peer_joined` drops a join for a `Dead` id). The set carries
///   only the `Alive` ids; absence of an id is NOT read as `Dead` (a
///   local-only `Dead` entry stays Dead, a never-seen id stays absent).
///   Idempotent + order-insensitive: re-restoring inserts nothing new.
///   Carried so a bootstrap-relocated / freshly-promoted primary seeded
///   purely from a snapshot reconstructs the alive-membership ledger
///   (`is_peer_alive` → `alive_secondary_members` →
///   `alive_remote_secondary_count`) the moment it restores — without
///   it `peer_state` stays empty and the count is a false zero from
///   tick 0. The inserted entry holds ONLY liveness; the role
///   capabilities ride the separate `capabilities` 2P-set (C6) and the
///   `RoleTable` projections are rebuilt from `capability × alive` after
///   both merge.
/// - `run_complete` / `run_aborted`: sticky-monotonic merge, mirroring
///   the `RunComplete`/`RunAborted` apply arms. `run_complete` ratchets
///   `false → true` only (never regresses); `run_aborted` latches the
///   first `Some(reason)` and never overwrites an already-`Some` local
///   value. Carried so a node seeded from a snapshot learns the run is
///   already over / aborted without waiting for a re-broadcast.
/// - `run_milestones` (A7): grow-only set UNION merge — every incoming
///   `(kind, phase)` is inserted, never removed (mirroring the
///   `RunMilestone` apply arm's `HashSet::insert`). Idempotent +
///   order-insensitive: re-restoring inserts nothing new, and the union
///   converges regardless of (live-broadcast, snapshot) arrival order.
///   Carried so a late-joining / freshly-promoted observer holds the full
///   reached-milestone set the moment it restores — the set IS
///   snapshot-healable (detect-WITH-heal via the digest fold).
///
/// These rules make `restore` an idempotent CRDT merge — applying the
/// same snapshot twice is a no-op, applying overlapping snapshots
/// converges to the same state regardless of order.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(serialize = "I: Serialize", deserialize = "I: for<'a> Deserialize<'a>",))]
pub struct ClusterStateSnapshot<I> {
    pub tasks: HashMap<String, TaskState<I>>,
    pub current_primary: Option<String>,
    pub primary_epoch: u64,
    pub phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    /// Replicated role-capability 2P-set (C6) — the SINGLE source of
    /// `is_observer` / `can_be_primary`, carried so a late-joiner /
    /// reconnecting node converges the full capability roster (including
    /// `Departed` tombstones) the moment it restores. Merged per-id via
    /// `merge_capability` on `restore` (monotone — Departed sticks,
    /// Advertised ratchets), then the `RoleTable.observers` /
    /// `RoleTable.can_be_primary` projections are rebuilt from
    /// `capability × local-alive` (`reproject_roles`). This is the
    /// failover-safe heal channel for capability: a promoted primary
    /// inherits the full capability roster from the snapshot it restores
    /// at promotion, so a capability divergence converges in one
    /// anti-entropy round regardless of who is primary.
    ///
    /// `#[serde(default)]` keeps wire compat with a pre-field sender
    /// (missing field decodes as an empty map, the conservative pre-field
    /// shape — a node with no capability info projects empty role sets,
    /// which the live `PeerJoined` broadcasts then populate).
    #[serde(default)]
    pub capabilities: HashMap<String, CapabilityEntry>,
    /// Replicated per-peer holdings map. Carried so a late-joiner
    /// sees the current set of opaque resource strings each peer
    /// announces before any live `PeerResourceHoldingsUpdated`
    /// broadcast arrives. Replaced on `restore` when local is
    /// empty; otherwise kept (the live apply path is the steady-
    /// state writer; the snapshot is authoritative for first-
    /// bootstrap only — same shape as `observers` and `phase_deps`).
    /// `#[serde(default)]` keeps wire compat with senders running an
    /// older crate (missing field deserializes as an empty map,
    /// identical to the pre-variant shape).
    #[serde(default)]
    pub peer_holdings: HashMap<String, HashSet<String>>,
    /// Replicated keyed-output cache (one entry per task that has
    /// reached `Completed` with a non-empty `result_data` payload),
    /// keyed by the wire-canonical content hash (which folds in
    /// `phase_id`, so same-`task_id`-different-phase outputs never
    /// collide). Carried so a late-joiner can resolve a dependent's
    /// predecessor outputs immediately on snapshot-restore, before the
    /// next live `TaskCompleted` broadcast reaches it — symmetric with
    /// how `tasks` carries terminal task states for the same reason.
    ///
    /// Merge rule on `restore`: per-key first-write-wins. Each entry
    /// is set exactly once (by the originating `TaskCompleted` apply
    /// arm; duplicate `TaskCompleted`s NoOp before reaching the
    /// populate helper), so a snapshot entry and a live-applied
    /// entry for the same hash carry the same value — the merge
    /// keeps whichever landed first and ignores the duplicate. This
    /// matches the `tasks` lattice's "terminal wins; among terminals,
    /// local wins" rule projected onto the cache's monotonic
    /// insertion semantics.
    ///
    /// `#[serde(default)]` keeps wire compat with pre-feature senders
    /// (missing field deserializes as an empty map, identical to the
    /// pre-cache shape).
    #[serde(default)]
    pub task_outputs: HashMap<String, TaskOutputs>,
    /// Replicated per-secondary static capacity (worker-slot count +
    /// advertised resources, one entry per secondary the
    /// `SecondaryCapacity` apply arm has recorded). Carried so a
    /// freshly-promoted primary and late-joining observers reconstruct
    /// the full worker roster immediately on snapshot-restore — without
    /// it a promoted primary starts `alive_worker_count() == 0` and
    /// cannot dispatch.
    ///
    /// Merge rule on `restore`: per-secondary first-write-wins. Each
    /// entry is set exactly once (the `SecondaryCapacity` apply is
    /// set-once; capacity is static for the run), so a snapshot entry
    /// and a live-applied entry for the same secondary carry the same
    /// value — the merge keeps whichever landed first and ignores the
    /// duplicate. Same monotonic-insertion shape as `task_outputs`.
    ///
    /// `#[serde(default)]` keeps wire compat with pre-feature senders
    /// (missing field deserializes as an empty map, identical to the
    /// pre-feature shape).
    #[serde(default)]
    pub secondary_capacities: HashMap<String, SecondaryCapacityRecord>,
    /// Replicated grow-only set of reached run-milestones (A7), one
    /// `(RunMilestoneKind, PhaseId)` element per phase-task-spawning /
    /// retry-pass-start marker the promoted primary has originated.
    /// Carried so a late-joining / freshly-promoted observer holds the
    /// full reached-milestone set immediately on snapshot-restore, before
    /// any live `RunMilestone` broadcast reaches it.
    ///
    /// Merge rule on `restore`: grow-only set UNION (insert every incoming
    /// element; never remove), mirroring the `RunMilestone` apply arm.
    /// Idempotent + order-insensitive — the union is the same regardless
    /// of (live-broadcast, snapshot) arrival order.
    ///
    /// `#[serde(default)]` keeps wire compat with pre-feature senders
    /// (missing field deserializes as an empty set, identical to the
    /// pre-feature shape).
    #[serde(default)]
    pub run_milestones: HashSet<(RunMilestoneKind, PhaseId)>,
    /// Projected alive-membership set: the ids whose `peer_state` entry
    /// is `Alive`. A PROJECTION of the module-private `peer_state` map
    /// (only `HashSet<String>` crosses the wire — `PeerEntry`/`PeerState`
    /// stay module-private), carried exactly the way `observers` /
    /// `can_be_primary` project `RoleTable` subsets.
    ///
    /// Merge rule on `restore`: Dead-wins / sticky-removal (see the
    /// type-level `alive_members` doc). Only `Alive` ids are carried;
    /// `Dead` is sticky-local, so the absence of an id must NOT be read
    /// as `Dead`. `#[serde(default)]` keeps wire compat with senders
    /// that predate the field (missing field decodes as an empty set,
    /// the pre-field shape).
    #[serde(default)]
    pub alive_members: HashSet<String>,
    /// Sticky-monotonic run-completion flag (the replicated `RunComplete`
    /// latch). Merge rule on `restore`: ratchets `false → true` only,
    /// never regresses — mirroring the `RunComplete` apply arm.
    /// `#[serde(default)]` keeps wire compat with pre-field senders
    /// (missing field decodes as `false`, the pre-field shape).
    #[serde(default)]
    pub run_complete: bool,
    /// Sticky-monotonic run-abort latch (the replicated `RunAborted`
    /// reason). Merge rule on `restore`: the first `Some(reason)` wins
    /// and never overwrites an already-`Some` local value — mirroring
    /// the `RunAborted` apply arm. `#[serde(default)]` keeps wire compat
    /// with pre-field senders (missing field decodes as `None`, the
    /// pre-field shape).
    #[serde(default)]
    pub run_aborted: Option<String>,
}

/// Migration shim (snapshot-ONLY): fill the enclosing task's phase into
/// every legacy un-phased dep in the snapshot. A dep that already names
/// its phase (every dep a current binary emits) is left untouched, so
/// this is a no-op for non-legacy snapshots. Operates in place on the
/// decoded snapshot before the lattice merge so the restored ledger
/// carries fully-explicit `(phase_id, task_id)` deps.
fn migrate_unphased_deps<I>(snap: &mut ClusterStateSnapshot<I>) {
    for state in snap.tasks.values_mut() {
        let task = state.task_mut();
        let enclosing = task.phase_id.clone();
        for dep in &mut task.task_depends_on {
            dep.fill_phase(&enclosing);
        }
    }
}

impl<I: Identifier> ClusterState<I> {
    /// Take a snapshot of the whole state. The snapshot is a deep
    /// clone — applying further mutations to `self` after this call
    /// does not affect the returned snapshot. Used as the response
    /// payload to `RequestClusterSnapshot`.
    pub fn snapshot(&self) -> ClusterStateSnapshot<I> {
        // Exhaustive destructure (NO `..` rest pattern) — the structural
        // completeness guard. Every `ClusterState` field is NAMED here,
        // so adding a future field is a COMPILE ERROR at this site until
        // the developer explicitly classifies it transfer-vs-node-local.
        // This is the only mechanism that catches a silently-omitted
        // replicated field (the bug this exists to prevent).
        let ClusterState {
            // ── replicated (transferred) ──
            tasks,
            current_primary,
            primary_epoch,
            phase_deps,
            run_complete,
            run_aborted,
            role_table: _role_table,
            peer_state,
            capabilities,
            peer_holdings,
            task_outputs,
            secondary_capacities,
            run_milestones,
            // ── node-local: not replicated ──
            // Atomic mirror is derived from `primary_epoch`; restore
            // re-stores it from the merged epoch (see `restore`).
            primary_epoch_mirror: _primary_epoch_mirror,
            // node-local: not replicated (transport write-through cache
            // re-registers on the restoring replica).
            role_change_hooks: _role_change_hooks,
            // node-local: not replicated (dispatcher mpsc senders belong
            // to the owning node's coordinator tasks).
            lifecycle_tx: _lifecycle_tx,
            matcher_trigger_tx: _matcher_trigger_tx,
            worker_mgmt_tx: _worker_mgmt_tx,
            task_completed_tx: _task_completed_tx,
            // node-local: the originator's per-hash version counter is not
            // part of the converged ledger (each replica mints its own on
            // origination; a restoring replica cold-starts it).
            task_seq: _task_seq,
        } = self;
        ClusterStateSnapshot {
            tasks: tasks.clone(),
            current_primary: current_primary.clone(),
            primary_epoch: *primary_epoch,
            phase_deps: phase_deps.clone(),
            // Carry the replicated role-capability 2P-set (the SINGLE
            // source of `is_observer`/`can_be_primary`) through the
            // snapshot so a late-joiner / promoted primary converges the
            // full capability roster — including `Departed` tombstones —
            // and rebuilds its `RoleTable` projections from it on restore.
            capabilities: capabilities.clone(),
            // Per-peer holdings — same first-bootstrap-only
            // contract as `observers` (replaced on restore when
            // local is empty, otherwise kept).
            peer_holdings: peer_holdings.clone(),
            // Replicated keyed-output cache — carried so a late-joiner
            // can resolve a dependent's predecessor outputs without
            // waiting for the prereq's `TaskCompleted` to retransmit.
            task_outputs: task_outputs.clone(),
            // Per-secondary static capacity — carried so a freshly-
            // promoted primary and late-joining observers reconstruct
            // the full worker roster on snapshot-restore.
            secondary_capacities: secondary_capacities.clone(),
            // Grow-only reached-milestone set (A7) — carried so a late-
            // joining / freshly-promoted observer holds the full set the
            // moment it restores, and the narrator projects it off the
            // converged ledger.
            run_milestones: run_milestones.clone(),
            // Project the alive-membership set out of `peer_state`:
            // ONLY ids whose entry is `Alive` (Dead is sticky-local;
            // absence must not be read as Dead). `PeerEntry`/`PeerState`
            // stay module-private — only the `HashSet<String>` crosses
            // the wire, the same projection shape as `observers`.
            alive_members: peer_state
                .iter()
                .filter(|(_, entry)| entry.state == PeerState::Alive)
                .map(|(id, _)| id.clone())
                .collect(),
            // Sticky-monotonic run latches — carried so a node seeded
            // from a snapshot learns the run is already over / aborted.
            run_complete: *run_complete,
            run_aborted: run_aborted.clone(),
        }
    }

    /// Merge a snapshot into local state per the CRDT lattice
    /// described on `ClusterStateSnapshot`. Idempotent: applying the
    /// same snapshot twice produces the same state as applying it
    /// once; applying overlapping snapshots converges regardless of
    /// order.
    ///
    /// Why merge (not replace): a node may have already applied
    /// live broadcasts before the snapshot RPC response arrives —
    /// for example, peer B's `TaskCompleted` reaches the joiner
    /// before peer A's snapshot does. Replacing would lose B's
    /// mutation; merging keeps the strictly stronger of (local,
    /// snapshot) per the lattice and stays correct under arbitrary
    /// interleaving of live broadcasts and snapshot delivery.
    /// Convenience wrapper around [`Self::restore_collecting_resumed`]
    /// for callers that do NOT own a dispatch pool — every non-pool
    /// caller (secondary, observer, late-joiner) keeps calling this
    /// unchanged. It allocates a throwaway `resumed` buffer and discards
    /// it. Mirrors the existing `apply` / `apply_with_resumed_blocked`
    /// split exactly (one canonical opt-in pattern, no new shape for the
    /// callers that don't consume the resumed surface).
    pub fn restore(&mut self, snap: ClusterStateSnapshot<I>) {
        let mut _resumed_scratch: Vec<TaskInfo<I>> = Vec::new();
        self.restore_collecting_resumed(snap, &mut _resumed_scratch);
    }

    /// Merge a snapshot AND surface the cross-task `Blocked → Pending`
    /// auto-resumes (TS-2 on the restore path) into `resumed` for the
    /// caller to re-inject into its live `PendingPool`. Only the
    /// pool-owning primary path reads `resumed`; the convenience
    /// [`Self::restore`] discards it.
    ///
    /// Each per-task `(hash, incoming)` routes through the SHARED
    /// [`Self::merge_task_state`] join (the SAME order apply uses), so the
    /// restore lattice is no longer a second hand-rolled rank: a terminal
    /// that wins the join emits its `TaskCompletedEvent` via the same
    /// sink as apply (TS-5, exactly-once — a re-restore yields `NoOp`
    /// because the key no longer dominates), records co-present outputs
    /// (TS-3), and resumes blocked dependents (TS-2).
    pub fn restore_collecting_resumed(
        &mut self,
        mut snap: ClusterStateSnapshot<I>,
        resumed: &mut Vec<TaskInfo<I>>,
    ) {
        // Migration shim (snapshot-ONLY): a legacy snapshot predates the
        // `(phase_id, task_id)` dep identity, so its deps decode with the
        // migration sentinel (empty `PhaseId`). Inject the enclosing
        // task's phase into every un-phased dep before merging. A new
        // dep always names its phase, so this is a no-op for any
        // snapshot produced by a current binary — the shim touches only
        // legacy entries and is never a runtime default. The enclosing
        // task's phase is the unambiguous source for a legacy dep
        // because a legacy snapshot only ever expressed same-phase deps
        // implicitly.
        migrate_unphased_deps(&mut snap);
        // Exhaustive destructure (NO `..` rest pattern) — the SYMMETRIC
        // structural completeness guard, mirroring `snapshot()`. Every
        // `ClusterStateSnapshot` field is NAMED here, so adding a future
        // snapshot field is a COMPILE ERROR at this site until the
        // developer explicitly classifies it restore-vs-skip. This closes
        // the round-trip: `snapshot()` guards "a new `ClusterState` field
        // must be classified for serialize"; this guard catches "a new
        // snapshot field silently ignored on restore". Each binding below
        // is consumed by the merge that previously read `snap.<field>`;
        // the transformation is a faithful rename (`snap.X` → `X`), not a
        // logic change.
        let ClusterStateSnapshot {
            tasks,
            current_primary,
            primary_epoch,
            phase_deps,
            capabilities,
            peer_holdings,
            task_outputs,
            secondary_capacities,
            run_milestones,
            alive_members,
            run_complete,
            run_aborted,
        } = snap;
        // Per-task restore now routes through the SHARED `merge_task_state`
        // join — the SAME order apply uses, so apply == restore by
        // construction (no second hand-rolled rank). The co-present output
        // is read from the snapshot's content-hash-keyed `task_outputs`
        // cache (F16/C7 — keyed by content hash, NOT task_id) so a
        // newly-completed restore folds its outputs into the same merge.
        // `merge_task_state`'s first-write-wins `record_task_outputs_value`
        // means a winning terminal does not clobber a local output entry a
        // live broadcast already populated. The `Applied { event }` emit
        // rides the SAME `emit_task_completed_event` sink as apply (TS-5),
        // and a re-restore yields `NoOp` (the key no longer dominates) so
        // the event fires exactly once.
        for (hash, incoming) in tasks {
            let co_present_outputs = task_outputs.get(&hash).cloned();
            if let super::merge::MergeOutcome::Applied {
                event: Some(ev), ..
            } = self.merge_task_state(&hash, incoming, co_present_outputs, resumed)
            {
                self.emit_task_completed_event(ev);
            }
        }
        // Primary register: CRD-2/D-P adopt rule, applied IDENTICALLY to
        // the live `PrimaryChanged` apply arm (`primary_register_adopt`):
        // higher epoch wins; equal epoch → lex-lower id wins. The
        // equal-epoch tie-break heals a same-epoch identity split BOTH
        // ways in one round (each replica pulls the other, both adopt the
        // lower id), where the prior strict-`>` gate kept local and never
        // converged.
        if super::merge::primary_register_adopt(
            self.primary_epoch,
            self.current_primary.as_deref(),
            primary_epoch,
            // `primary_register_adopt`'s `inc_id` is only consulted at
            // equal epoch; a `None` snapshot primary at a higher epoch
            // still adopts (epoch dominates), and a `None` at equal epoch
            // never wins (a `None` inc loses to any `Some` local). Use an
            // empty-str sentinel when the snapshot carries no primary —
            // safe because at a higher epoch the id is not read, and at
            // equal epoch a real local `Some` id is never lex-greater than
            // the empty string, so it never wrongly adopts a `None`.
            current_primary.as_deref().unwrap_or(""),
        ) && current_primary.is_some()
        {
            self.primary_epoch = primary_epoch;
            // Mirror update on the snapshot-merge path mirrors the live
            // `PrimaryChanged` apply rule — same `Release` ordering, same
            // pre-`fire_role_change_hooks` write — so a late-joiner's
            // announcer wakes from the restore-time trigger and reads the
            // restored epoch, not the cold-start 0.
            self.primary_epoch_mirror
                .store(primary_epoch, std::sync::atomic::Ordering::Release);
            self.current_primary = current_primary.clone();
            // Keep the replicated `RoleTable` in lockstep with
            // `current_primary` even when the new value lands via
            // the snapshot-merge path (late joiner / reconnect),
            // not just via live `PrimaryChanged` mutations. The
            // role-change hook fires AFTER the table update so any
            // registered write-through cache stays coherent with
            // the post-merge state.
            self.role_table.primary = current_primary;
            self.fire_role_change_hooks();
        }
        // Phase deps: CRD-3/D-G deterministic content-hash merge. Adopt if
        // local is empty (first-bootstrap); if BOTH are non-empty and the
        // canonical (order-independent) content hash DIFFERS, adopt the
        // LOWER content-hash graph so two replicas that diverged across a
        // partition reconcile to the SAME graph regardless of pull order.
        // (Apply's `PhaseDepsSet` arm flags the divergence loudly; this is
        // the separate reconciliation layer — both share the one
        // `canonical_phase_deps_hash` helper.)
        if self.phase_deps.is_empty() {
            self.phase_deps = phase_deps;
        } else if !phase_deps.is_empty() {
            let local_hash = super::merge::canonical_phase_deps_hash(&self.phase_deps);
            let inc_hash = super::merge::canonical_phase_deps_hash(&phase_deps);
            if inc_hash < local_hash {
                self.phase_deps = phase_deps;
            }
        }
        // Capabilities: per-id 2P-set merge (C6). Monotone — `Departed`
        // sticks, `Advertised` ratchets `is_observer` and follows the
        // higher `cap_version` for `can_be_primary`. After merging, the
        // `RoleTable.observers` / `RoleTable.can_be_primary` projections
        // are rebuilt from `capability × local-alive` (`reproject_roles`,
        // below, after the alive-membership merge so the alive bit is
        // current). The 2P-set IS snapshot-healable, so a capability
        // divergence the digest flagged converges here in one pull.
        let mut capabilities_changed = false;
        for (id, incoming) in capabilities {
            let merged = match self.capabilities.get(&id) {
                Some(local) => merge_capability(local, &incoming),
                None => incoming,
            };
            if self.capabilities.get(&id) != Some(&merged) {
                self.capabilities.insert(id, merged);
                capabilities_changed = true;
            }
        }
        // Peer-holdings map: same first-bootstrap-only contract
        // as `observers` and `phase_deps`. The live
        // `PeerResourceHoldingsUpdated` apply path is the steady-
        // state writer; the snapshot field is authoritative only
        // before any live announce reaches this replica. No hook
        // fire here: holdings-change hooks (wired by the sibling
        // E3 subtask via the lifecycle dispatcher mpsc) are
        // per-peer-announce signals, not snapshot-bootstrap signals.
        if self.peer_holdings.is_empty() && !peer_holdings.is_empty() {
            self.peer_holdings = peer_holdings;
        }
        // Keyed-output cache merge: per-key first-write-wins. Each
        // `TaskCompleted` apply for a given hash records exactly one
        // entry (duplicate `TaskCompleted`s NoOp before reaching the
        // populate helper), so a snapshot's entry and a live-applied
        // entry for the same hash carry the same value — keeping
        // the local entry when present and inserting the snapshot's
        // entry when missing converges every replica to the same map
        // regardless of (live-broadcast, snapshot) arrival order. The
        // `entry().or_insert(_)` shape is the CRDT-coherent choice;
        // a blanket replace would clobber legitimately-applied local
        // entries when the snapshot interleaves with live broadcasts.
        for (hash, outputs) in task_outputs {
            self.task_outputs.entry(hash).or_insert(outputs);
        }
        // Per-secondary capacity merge: per-secondary first-write-wins,
        // identical shape to `task_outputs`. The `SecondaryCapacity`
        // apply is set-once, so a snapshot entry and a live-applied
        // entry for the same secondary carry the same value — keeping
        // the local entry when present and inserting the snapshot's
        // entry when missing converges every replica to the same map
        // regardless of (live-broadcast, snapshot) arrival order. A
        // blanket replace would clobber a legitimately-applied local
        // entry when the snapshot interleaves with live broadcasts.
        for (secondary, record) in secondary_capacities {
            self.secondary_capacities.entry(secondary).or_insert(record);
        }
        // Run-milestone set merge (A7): grow-only set UNION. Insert every
        // incoming `(kind, phase)`; the set never shrinks, so the union
        // converges regardless of (live-broadcast, snapshot) arrival order
        // and a re-restore inserts nothing new (idempotent). Mirrors the
        // `RunMilestone` apply arm's `HashSet::insert` — apply == restore
        // by construction.
        self.run_milestones.extend(run_milestones);
        // Alive-membership merge: Dead-wins / sticky-removal, mirroring
        // the `PeerJoined`/`PeerRemoved` apply rules in `apply_peer.rs`.
        // For each incoming alive id insert a fresh `Alive` entry ONLY
        // IF the local `peer_state` has no entry for it — never
        // overwriting a local `Dead` (sticky removal wins, exactly as
        // `apply_peer_joined` drops a join for a `Dead` id, and as a
        // never-restore on an already-`Alive` local entry is a no-op).
        // The `Entry::Vacant` guard makes this idempotent + order-
        // insensitive. The entry holds ONLY liveness (C6); the role
        // capabilities ride the separate `capabilities` 2P-set merged
        // above, and the `RoleTable` projections are rebuilt from
        // `capability × alive` by `reproject_roles` below.
        let mut alive_changed = false;
        for id in alive_members {
            if let std::collections::hash_map::Entry::Vacant(e) = self.peer_state.entry(id) {
                e.insert(PeerEntry {
                    state: PeerState::Alive,
                    pubkey: None,
                    endpoint: None,
                });
                alive_changed = true;
            }
        }
        // Rebuild the role projections from the post-merge capability
        // 2P-set + the post-merge alive bit, firing the role-change hooks,
        // whenever EITHER changed. The SOLE producer of the `RoleTable`
        // role sets, identical to the live apply path — so a snapshot-
        // restored node and a live-applied node converge to the same
        // projections.
        if capabilities_changed || alive_changed {
            self.reproject_roles();
        }
        // Run latches: sticky-monotonic, mirroring the `RunComplete` /
        // `RunAborted` apply arms. `run_complete` ratchets false→true
        // only (`|=` never regresses true→false); `run_aborted` latches
        // the first `Some` and never overwrites an already-`Some` local
        // value (`get_or_insert` is a no-op when already `Some`).
        self.run_complete |= run_complete;
        if self.run_aborted.is_none()
            && let Some(reason) = run_aborted
        {
            self.run_aborted = Some(reason);
        }
    }
}
