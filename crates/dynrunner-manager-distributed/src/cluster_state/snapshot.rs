//! Snapshot type and CRDT-merge restore.
//!
//! Single concern: a serializable view of the entire `ClusterState`
//! (the `ClusterStateSnapshot<I>` type), the `snapshot()` deep-clone
//! capture, and the lattice-merge `restore()` that the snapshot RPC
//! callers (late joiner, reconnect) apply against local state.
//! Idempotent under repeated and overlapping snapshots per the
//! per-field merge rules documented on `ClusterStateSnapshot`.

use std::collections::{HashMap, HashSet};

use dynrunner_core::{Identifier, PhaseId, TaskOutputs};
use dynrunner_protocol_primary_secondary::SecondaryCapacityRecord;
use serde::{Deserialize, Serialize};

use super::ClusterState;
use super::TaskState;

/// Serializable snapshot of an entire `ClusterState`. Used by the
/// snapshot RPC (`RequestClusterSnapshot` → `ClusterSnapshot`) so a
/// late-joining or reconnecting node can bootstrap its replicated
/// ledger from any peer.
///
/// Merge semantics on the receiver side (see `ClusterState::restore`):
///
/// - Per task: terminal states (`Completed` / `Failed`) win over
///   non-terminal; among non-terminals, `InFlight` wins over `Pending`.
///   When local and incoming are both terminal, local wins (first-seen
///   terminal is canonical, mirroring the live `apply` rules).
/// - `(current_primary, primary_epoch)`: higher epoch wins.
/// - `phase_deps`: replaced if local is empty, otherwise kept (the
///   graph is static for the run's lifetime).
/// - `observers`: replaced if local is empty, otherwise kept. The
///   live mutation path (`ClusterMutation::PeerJoined { is_observer
///   = true }` broadcasts) inserts into the set with set semantics,
///   so a snapshot is authoritative for the late-joiner's first-
///   bootstrap and inert thereafter. A broader merge rule (union,
///   or epoch-tagged replace) would be over-engineering today —
///   subsequent `PeerJoined` broadcasts converge any divergence
///   between snapshot-restored and live-applied observers via the
///   apply rule's idempotent insert. `#[serde(default)]` keeps wire
///   compat with pre-Step-8 senders (snapshots from a peer running
///   an older crate omit the field; deserialize defaults to an
///   empty set, identical to the pre-Step-8 shape).
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
    /// Replicated observer set (Step 7's `RoleTable.observers`). A
    /// late-joiner needs this immediately to apply the election
    /// filter (`secondary::election::lowest_alive` skips observers);
    /// the live PeerInfo broadcast that arrives shortly after will
    /// supersede it, but in the gap between snapshot-restore and
    /// the next PeerInfo broadcast, the joiner would otherwise
    /// promote an observer candidate.
    #[serde(default)]
    pub observers: HashSet<String>,
    /// Replicated primary-capability set (`RoleTable.can_be_primary`).
    /// Carried so a late-joiner / reconnecting node sees which peers may
    /// host the primary the moment it restores a snapshot — before any
    /// live `PeerJoined { can_be_primary }` / `SetCanBePrimary` mutation
    /// reaches it. Replaced on `restore` when local is empty; otherwise
    /// kept — the same first-bootstrap-only contract as `observers`
    /// (the live `PeerJoined`/`SetCanBePrimary` apply path is the
    /// steady-state writer; subsequent broadcasts converge any
    /// divergence via the idempotent set ops). `#[serde(default)]`
    /// keeps wire compat with a peer that predates the field (missing
    /// field decodes as an empty set, the conservative pre-field shape).
    #[serde(default)]
    pub can_be_primary: HashSet<String>,
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

fn task_state_rank<I>(s: &TaskState<I>) -> u8 {
    match s {
        // Pending and Blocked are both non-dispatching states; Blocked
        // ranks above Pending because it carries the cascade prereq
        // identity (`on`) — a snapshot's Blocked should not be silently
        // overwritten by a stale peer's Pending observation.
        TaskState::Pending { .. } => 0,
        TaskState::Blocked { .. } => 1,
        // An active dispatch (InFlight) supersedes cascade-paused
        // observers — if any peer saw the worker pick the task up, that
        // happens-after the cascade decision.
        TaskState::InFlight { .. } => 2,
        // All terminals share the strongest rank. Convergence among
        // terminals follows the per-arm rules in `apply` (Completed
        // never regresses; Failed/Unfulfillable/InvalidTask lock out
        // incoming TaskFailed for their own hash).
        TaskState::Completed { .. }
        | TaskState::Failed { .. }
        | TaskState::Unfulfillable { .. }
        | TaskState::InvalidTask { .. } => 3,
    }
}

impl<I: Identifier> ClusterState<I> {
    /// Take a snapshot of the whole state. The snapshot is a deep
    /// clone — applying further mutations to `self` after this call
    /// does not affect the returned snapshot. Used as the response
    /// payload to `RequestClusterSnapshot`.
    pub fn snapshot(&self) -> ClusterStateSnapshot<I> {
        ClusterStateSnapshot {
            tasks: self.tasks.clone(),
            current_primary: self.current_primary.clone(),
            primary_epoch: self.primary_epoch,
            phase_deps: self.phase_deps.clone(),
            // Carry the replicated observer set through the snapshot
            // so a late-joiner can populate `RoleTable.observers`
            // before any `PeerJoined` mutation arrives. The set is
            // the same one the `PeerJoined { is_observer = true }`
            // apply rule writes; the snapshot is authoritative for
            // first-bootstrap and inert thereafter.
            observers: self.role_table.observers.clone(),
            // Replicated primary-capability set — same first-bootstrap-
            // only contract as `observers`. Carried so a late-joiner
            // sees which peers may host the primary on snapshot-restore.
            can_be_primary: self.role_table.can_be_primary.clone(),
            // Per-peer holdings — same first-bootstrap-only
            // contract as `observers` (replaced on restore when
            // local is empty, otherwise kept).
            peer_holdings: self.peer_holdings.clone(),
            // Replicated keyed-output cache — carried so a late-joiner
            // can resolve a dependent's predecessor outputs without
            // waiting for the prereq's `TaskCompleted` to retransmit.
            task_outputs: self.task_outputs.clone(),
            // Per-secondary static capacity — carried so a freshly-
            // promoted primary and late-joining observers reconstruct
            // the full worker roster on snapshot-restore.
            secondary_capacities: self.secondary_capacities.clone(),
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
    pub fn restore(&mut self, mut snap: ClusterStateSnapshot<I>) {
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
        for (hash, incoming) in snap.tasks {
            match self.tasks.get(&hash) {
                None => {
                    self.tasks.insert(hash, incoming);
                }
                Some(local) => {
                    if task_state_rank(&incoming) > task_state_rank(local) {
                        self.tasks.insert(hash, incoming);
                    }
                }
            }
        }
        if snap.primary_epoch > self.primary_epoch {
            self.primary_epoch = snap.primary_epoch;
            // Mirror update on the snapshot-merge path mirrors the live
            // `PrimaryChanged` apply rule — same `Release` ordering, same
            // pre-`fire_role_change_hooks` write — so a late-joiner's
            // announcer wakes from the restore-time trigger and reads the
            // restored epoch, not the cold-start 0.
            self.primary_epoch_mirror
                .store(snap.primary_epoch, std::sync::atomic::Ordering::Release);
            self.current_primary = snap.current_primary.clone();
            // Keep the replicated `RoleTable` in lockstep with
            // `current_primary` even when the new value lands via
            // the snapshot-merge path (late joiner / reconnect),
            // not just via live `PrimaryChanged` mutations. The
            // role-change hook fires AFTER the table update so any
            // registered write-through cache stays coherent with
            // the post-merge state.
            self.role_table.primary = snap.current_primary;
            self.fire_role_change_hooks();
        }
        if self.phase_deps.is_empty() {
            self.phase_deps = snap.phase_deps;
        }
        // Observer set: replace if local is empty (first-bootstrap
        // case), otherwise keep local. The live `PeerJoined` apply
        // path is the steady-state writer (set-semantics insert);
        // this branch only fires on the late-joiner's very first
        // restore, before any `PeerJoined` mutation arrives. Firing
        // the role-change hooks when the set actually changes keeps
        // the transport's write-through cache coherent on the
        // snapshot path the same way `PeerJoined` does on the live
        // path.
        if self.role_table.observers.is_empty() && !snap.observers.is_empty() {
            self.role_table.observers = snap.observers;
            self.fire_role_change_hooks();
        }
        // Primary-capability set: replace if local is empty (first-
        // bootstrap), otherwise keep local — the same contract as
        // `observers`. The live `PeerJoined { can_be_primary }` /
        // `SetCanBePrimary` apply path is the steady-state writer; this
        // branch only fires on the very first restore before any such
        // mutation arrives. Fire the role-change hooks on a genuine
        // change so the write-through cache stays coherent on the
        // snapshot path the same way the live apply does.
        if self.role_table.can_be_primary.is_empty() && !snap.can_be_primary.is_empty() {
            self.role_table.can_be_primary = snap.can_be_primary;
            self.fire_role_change_hooks();
        }
        // Peer-holdings map: same first-bootstrap-only contract
        // as `observers` and `phase_deps`. The live
        // `PeerResourceHoldingsUpdated` apply path is the steady-
        // state writer; the snapshot field is authoritative only
        // before any live announce reaches this replica. No hook
        // fire here: holdings-change hooks (wired by the sibling
        // E3 subtask via the lifecycle dispatcher mpsc) are
        // per-peer-announce signals, not snapshot-bootstrap signals.
        if self.peer_holdings.is_empty() && !snap.peer_holdings.is_empty() {
            self.peer_holdings = snap.peer_holdings;
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
        for (hash, outputs) in snap.task_outputs {
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
        for (secondary, record) in snap.secondary_capacities {
            self.secondary_capacities.entry(secondary).or_insert(record);
        }
    }
}
