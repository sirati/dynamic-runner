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
///
/// These rules make `restore` an idempotent CRDT merge — applying the
/// same snapshot twice is a no-op, applying overlapping snapshots
/// converges to the same state regardless of order.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "I: Serialize",
    deserialize = "I: for<'a> Deserialize<'a>",
))]
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
    /// Replicated panik latch. Carried so a late-joining observer
    /// learns of an in-progress emergency shutdown immediately on
    /// snapshot-restore, before the next `PanikRequested` broadcast
    /// reaches it. The latch is sticky monotonic: once true on any
    /// replica it stays true forever, and `restore` honors that —
    /// a snapshot with `panik_active = true` sets local state's
    /// flag (and reason/source if they are still `None`); a
    /// snapshot with `panik_active = false` is a NoOp against a
    /// local `true`. `#[serde(default)]` keeps wire compat with
    /// pre-feature senders (deserialize defaults to `false`).
    #[serde(default)]
    pub panik_active: bool,
    /// First-applying panik reason from the originator's broadcast.
    /// `Some(_)` iff `panik_active`. See [`Self::panik_active`].
    /// `#[serde(default)]` for wire compat.
    #[serde(default)]
    pub panik_reason: Option<String>,
    /// Peer that originated the first-applying panik. `Some(_)` iff
    /// `panik_active`. Forensic-only; no apply rule consults it.
    /// `#[serde(default)]` for wire compat.
    #[serde(default)]
    pub panik_source: Option<String>,
    /// Replicated keyed-output cache (one entry per task that has
    /// reached `Completed` with a non-empty `result_data` payload).
    /// Carried so a late-joiner can resolve a dependent's predecessor
    /// outputs immediately on snapshot-restore, before the next live
    /// `TaskCompleted` broadcast reaches it — symmetric with how
    /// `tasks` carries terminal task states for the same reason.
    ///
    /// Merge rule on `restore`: per-key first-write-wins. Each entry
    /// is set exactly once (by the originating `TaskCompleted` apply
    /// arm; duplicate `TaskCompleted`s NoOp before reaching the
    /// populate helper), so a snapshot entry and a live-applied
    /// entry for the same `task_id` carry the same value — the merge
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
        // never regresses; Failed/Unfulfillable lock out incoming
        // TaskFailed for their own hash; Cancelled is preserved by
        // `TaskFailed` and only superseded by `TaskCompleted`).
        TaskState::Completed { .. }
        | TaskState::Failed { .. }
        | TaskState::Unfulfillable { .. }
        | TaskState::Cancelled { .. } => 3,
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
            // Per-peer holdings — same first-bootstrap-only
            // contract as `observers` (replaced on restore when
            // local is empty, otherwise kept).
            peer_holdings: self.peer_holdings.clone(),
            // Sticky panik latch — carried so a late-joiner sees
            // the in-progress emergency-stop immediately, in
            // addition to via any live `PanikRequested` broadcast.
            panik_active: self.panik_active,
            panik_reason: self.panik_reason.clone(),
            panik_source: self.panik_source.clone(),
            // Replicated keyed-output cache — carried so a late-joiner
            // can resolve a dependent's predecessor outputs without
            // waiting for the prereq's `TaskCompleted` to retransmit.
            task_outputs: self.task_outputs.clone(),
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
    pub fn restore(&mut self, snap: ClusterStateSnapshot<I>) {
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
        // Sticky panik latch: monotonic-true. Snapshot with
        // `panik_active = true` always sets local; local `true` never
        // regresses to `false` even if the incoming snapshot omits it
        // (#[serde(default)] = false case). Reason / source land
        // alongside the flag the first time the latch flips to true.
        if snap.panik_active && !self.panik_active {
            self.panik_active = true;
            self.panik_reason = snap.panik_reason;
            self.panik_source = snap.panik_source;
        }
        // Keyed-output cache merge: per-key first-write-wins. Each
        // `TaskCompleted` apply for a given hash records exactly one
        // entry (duplicate `TaskCompleted`s NoOp before reaching the
        // populate helper), so a snapshot's entry and a live-applied
        // entry for the same `task_id` carry the same value — keeping
        // the local entry when present and inserting the snapshot's
        // entry when missing converges every replica to the same map
        // regardless of (live-broadcast, snapshot) arrival order. The
        // `entry().or_insert(_)` shape is the CRDT-coherent choice;
        // a blanket replace would clobber legitimately-applied local
        // entries when the snapshot interleaves with live broadcasts.
        for (task_id, outputs) in snap.task_outputs {
            self.task_outputs.entry(task_id).or_insert(outputs);
        }
    }
}
