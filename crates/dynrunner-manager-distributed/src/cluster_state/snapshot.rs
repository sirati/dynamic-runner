//! Snapshot type and CRDT-merge restore.
//!
//! Single concern: a serializable view of the entire `ClusterState`
//! (the `ClusterStateSnapshot<I>` type), the `snapshot()` deep-clone
//! capture, and the lattice-merge `restore()` that the snapshot RPC
//! callers (late joiner, reconnect) apply against local state.
//! Idempotent under repeated and overlapping snapshots per the
//! per-field merge rules documented on `ClusterStateSnapshot`.

use std::collections::{HashMap, HashSet};

use dynrunner_core::{Identifier, PhaseId, TaskInfo, TaskOutputs, TerminalOutcomeCounts};
use dynrunner_protocol_primary_secondary::{DiscoveryDebt, SecondaryCapacityRecord};
use serde::{Deserialize, Serialize};

use crate::primary::retry_bucket::BucketKind;

use super::ClusterState;
use super::TaskState;
use super::grow_max::{merge_grow_max, merge_grow_set};
use super::merge::merge_capability;
use super::types::{
    CapabilityEntry, CustomMsgState, PeerEntry, PeerState, PhaseTally, RespawnEventRecord,
};

/// Wire adapter for the TUPLE-keyed grow-only-MAX maps (F4
/// `phase_event_tallies`, P3 `retry_passes_used`): the snapshot rides the
/// wire as JSON (`DistributedMessage::ClusterSnapshot { snapshot_json }`),
/// and serde_json REJECTS a map whose key is not a string — so a plain
/// `HashMap<(K1, K2), u32>` field serialized fine while EMPTY and errored
/// ("key must be a string") the moment it held an entry, making every
/// snapshot responder silently warn-and-DROP its reply (the late-joiner /
/// anti-entropy heal path) once a tally existed. Encoding the map as a
/// `Vec<((K1, K2), u32)>` pair list keeps the in-memory type unchanged and
/// makes the wire shape key-type-agnostic. Entry order is not significant
/// (the restore merge is per-key MAX, order-independent); `#[serde(
/// default)]` on the fields still covers a pre-field sender.
mod tuple_keyed_map {
    use std::collections::HashMap;
    use std::hash::Hash;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(super) fn serialize<K, V, S>(map: &HashMap<K, V>, ser: S) -> Result<S::Ok, S::Error>
    where
        K: Serialize,
        V: Serialize,
        S: Serializer,
    {
        ser.collect_seq(map.iter())
    }

    pub(super) fn deserialize<'de, K, V, D>(de: D) -> Result<HashMap<K, V>, D::Error>
    where
        K: Deserialize<'de> + Eq + Hash,
        V: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        let pairs: Vec<(K, V)> = Vec::deserialize(de)?;
        Ok(pairs.into_iter().collect())
    }
}

/// Serializable snapshot of an entire `ClusterState`. Used by the
/// snapshot RPC (`RequestSnapshotStream` → `SnapshotStreamPackage`) so a
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
///   `alive_worker_secondary_count`) the moment it restores — without
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
/// - `discovery_debt`: sticky-monotonic merge, join = `max` over the
///   three-state lattice `Undeclared ⊑ Owed ⊑ Settled`. A replica only
///   moves UP: a `Settled` snapshot ratchets any lower local to `Settled`;
///   an `Owed` snapshot ratchets a local `Undeclared → Owed` but never
///   overwrites a local `Settled`; an `Undeclared` snapshot loses to both.
///   Carried so a promoted primary inherits "discovery already done" (and
///   does NOT re-run discovery on failover) AND so a replica that missed
///   the live `Declared` broadcast still learns `Owed` via the pull.
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
    /// Replicated `may_be_empty` phase set (same static-graph lifecycle as
    /// `phase_deps`): carried so a late-joiner / promoted node restores the
    /// consumer's empty-drain opt-out and its proceed-or-fail policy matches
    /// the live primary's. `#[serde(default)]` so a snapshot from a peer
    /// predating this field restores as "no phase opted out" — wire-safe.
    #[serde(default)]
    pub phase_may_be_empty: std::collections::HashSet<PhaseId>,
    /// Replicated `barrier=False` phase set (same static-graph lifecycle as
    /// `phase_deps`): carried so a late-joiner / promoted node restores the
    /// consumer's pipelined-edge opt-in and its `apply_spawn_tasks` barrier
    /// interlock matches the live primary's. `#[serde(default)]` so a
    /// snapshot from a peer predating this field restores as "every phase
    /// is barrier=True" — wire-safe.
    #[serde(default)]
    pub phase_no_barrier: std::collections::HashSet<PhaseId>,
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
    /// Membership-incarnation generations of the ALIVE members (the
    /// re-admission lattice) — `id → member_gen` for exactly the ids in
    /// `alive_members`. Carried so a snapshot pull can RE-ADMIT a peer
    /// the puller still holds `Dead` at a LOWER generation (the
    /// snapshotting node observed the generation-advancing `PeerJoined`
    /// the puller missed): without the generation the Dead-wins merge
    /// would keep the stale tombstone forever. Merge rule on `restore`:
    /// per-id, an incoming alive id at a STRICTLY higher generation than
    /// the local entry re-admits/advances it; at or below, the local
    /// entry (Dead-wins within an incarnation) is kept. An id absent
    /// from this map (a pre-field snapshot) defaults to generation 0 —
    /// exactly the pre-generation vacant-insert-only semantics.
    /// `#[serde(default)]` keeps wire compat with pre-field senders.
    #[serde(default)]
    pub member_generations: HashMap<String, u64>,
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
    /// The terminal verdict's FINALIZED per-class outcome counts (the
    /// replicated `terminal_outcome` payload carried atomically on
    /// `RunComplete`/`RunAborted`). Merge rule on `restore`: the first
    /// `Some` wins and never overwrites an already-`Some` local value —
    /// mirroring the `run_aborted` latch — so a behind replica pulling a
    /// snapshot receives the counts ALONGSIDE the run latch (the same atomic
    /// carriage the live broadcast gives). `#[serde(default)]` keeps wire
    /// compat with pre-field senders (missing decodes as `None`).
    #[serde(default)]
    pub terminal_outcome: Option<TerminalOutcomeCounts>,
    /// Sticky-monotonic graceful-abort latch (the replicated
    /// `GracefulAbortRequested` dispatch freeze). Merge rule on `restore`:
    /// ratchets `false → true` only, never regresses — mirroring the
    /// `GracefulAbortRequested` apply arm — so a failover-promoted primary
    /// restoring a frozen snapshot INHERITS the freeze (the no-redo law).
    /// `#[serde(default)]` keeps wire compat with pre-field senders
    /// (missing field decodes as `false`, the pre-field shape).
    #[serde(default)]
    pub graceful_abort_requested: bool,
    /// Replicated per-peer wind-down directive set (#467) — grow-only SET
    /// of `(secondary_id, member_gen)` pairs, the per-incarnation sibling
    /// of `graceful_abort_requested`. Carried so a failover-promoted
    /// primary INHERITS every in-flight wind-down directive and the
    /// directed secondary still stands down at quiescence after the
    /// primary relocates (the no-redo law, per-peer). Merge rule on
    /// `restore`: union (a grow-only set never regresses). `#[serde(default)]`
    /// keeps wire compat with pre-field senders (missing field decodes as
    /// an empty set, the pre-field shape). Tuple element, so the wire shape
    /// is a list of `[secondary_id, member_gen]` pairs (serde handles a
    /// `HashSet` of tuples as a JSON array of 2-element arrays).
    #[serde(default)]
    pub wind_down_requested: HashSet<(String, u64)>,
    /// Sticky-monotonic discovery-debt latch (the replicated discovery
    /// lattice). Merge rule on `restore`: join = `max` over
    /// `Undeclared ⊑ Owed ⊑ Settled` (a replica only moves UP; a `Settled`
    /// snapshot wins, an `Owed` snapshot ratchets a local `Undeclared` but
    /// never overwrites a local `Settled`). `#[serde(default)]` keeps wire
    /// compat with pre-field senders (missing field decodes as `Undeclared`
    /// = the never-declared BOTTOM = the conservative never-owed shape,
    /// which loses to any peer's higher state, so a legacy snapshot never
    /// drags a declared run down).
    #[serde(default)]
    pub discovery_debt: DiscoveryDebt,
    /// Replicated per-phase EVENT tallies (F4) — grow-only MAX of a monotone
    /// event count keyed by `(PhaseId, PhaseTally)`. Carried so a promoted
    /// primary inherits the per-phase completed/failed EVENT counts (a fail
    /// → reinject → succeed task contributed to BOTH) and reports the SAME
    /// `on_phase_end` numbers. Merge rule on `restore`: per-key grow-only
    /// MAX (a stale peer can never resurrect a lower count). `#[serde(default)]`
    /// keeps wire compat with a pre-field sender (missing field decodes as an
    /// empty map, the accessor's `unwrap_or(0)` covers it). Tuple-keyed, so
    /// the wire shape is the `tuple_keyed_map` pair list (serde_json rejects
    /// non-string map keys).
    #[serde(default, with = "tuple_keyed_map")]
    pub phase_event_tallies: HashMap<(PhaseId, PhaseTally), u32>,
    /// Replicated per-(phase, bucket) retry-pass USED counter (P3) —
    /// grow-only MAX of a monotone used count. Carried so a promoted primary
    /// inherits the retry budget already consumed (the budget is NOT
    /// re-granted on failover). Merge rule on `restore`: per-key grow-only
    /// MAX. `#[serde(default)]` keeps wire compat with a pre-field sender.
    /// Tuple-keyed, so the wire shape is the `tuple_keyed_map` pair list
    /// (serde_json rejects non-string map keys).
    #[serde(default, with = "tuple_keyed_map")]
    pub retry_passes_used: HashMap<(PhaseId, BucketKind), u32>,
    /// Replicated per-hash unfulfillable-reinject USED counter (P3) —
    /// grow-only MAX of a monotone used count. Carried so a promoted primary
    /// inherits the reinject budget already consumed (the budget is NOT
    /// re-granted on failover; `remaining = cap − used` is derived locally).
    /// Merge rule on `restore`: per-key grow-only MAX. `#[serde(default)]`
    /// keeps wire compat with a pre-field sender.
    #[serde(default)]
    pub unfulfillable_reinject_used: HashMap<String, u32>,
    /// Replicated respawn ledger (F7) — grow-only SET keyed by `new_id`,
    /// value `RespawnEventRecord`. Carried so a promoted primary inherits
    /// the full respawn ledger and the admission budget + cooldown are NOT
    /// re-granted on failover. Merge rule on `restore`: union-by-key (a
    /// `new_id` is globally unique per event and its value is written once,
    /// so shared keys never diverge). `#[serde(default)]` keeps wire compat
    /// with a pre-field sender (missing field decodes as an empty map).
    #[serde(default)]
    pub respawn_events: HashMap<String, RespawnEventRecord>,
    /// Replicated respawn-policy CAPS — the run-constant
    /// `--respawn-policy=on-secondary-death` knobs the budget admission
    /// gate compares the `respawn_events` SPEND against. Carried so a
    /// promoted primary re-arms the respawn DECISION at hydrate (the
    /// sibling ledger above carries the spend; without the caps a
    /// relocated primary could never re-enable the pipeline). Merge rule
    /// on `restore`: first-write-wins (adopt only when local is `None` —
    /// the policy is set once per run, mirroring `phase_may_be_empty`).
    /// `#[serde(default)]` keeps wire compat with a pre-field sender
    /// (missing field decodes as `None` — "respawn off", the
    /// conservative shape).
    #[serde(default)]
    pub respawn_policy: Option<super::types::ReplicatedRespawnPolicy>,
    /// Replicated "phase ended" facts (#343) — grow-only SET of the phases
    /// whose `on_phase_end` edge completed. Carried so a promoted primary
    /// inherits exactly which phases already fired their hook: present →
    /// seeded `Done` without re-firing (#326); absent → the phase flows
    /// through the live cascade and fires for the first time (the
    /// freshly-discovered all-skipped phase). Merge rule on `restore`:
    /// set UNION (grow-only; a stale peer's snapshot can never un-end a
    /// phase). `#[serde(default)]` keeps wire compat with a pre-field
    /// sender (missing field decodes as the empty set — "no hook is known
    /// to have fired", the conservative replay-the-edge shape).
    #[serde(default)]
    pub phases_ended: HashSet<PhaseId>,
    /// Replicated custom-message inbox (F5) — the IMPORTANT
    /// secondary→primary consumer messages, keyed by `(origin, seq)`.
    /// Carried so a promoted primary inherits every `Unhandled` entry
    /// and its hydrate replays them to the local handler (the
    /// failover-safety the feature exists for), and so a late-joiner's
    /// mirror converges. Merge rule on `restore`: per-key sticky-latch
    /// join (`Unhandled ⊑ Handled` — a `Handled` wins; an `Unhandled`
    /// vacant-inserts; watermark-subsumed keys are skipped), then the
    /// per-origin watermark compaction prunes newly-complete prefixes.
    /// Tuple-keyed, so the wire shape is the `tuple_keyed_map` pair
    /// list (the #358 lesson — serde_json REJECTS non-string map keys,
    /// and a plain map field would serialize fine while empty then
    /// error on the first real entry, silently dropping every snapshot
    /// reply). `#[serde(default)]` keeps wire compat with a pre-field
    /// sender (missing field decodes as an empty inbox).
    #[serde(default, with = "tuple_keyed_map")]
    pub custom_messages: HashMap<(String, u64), CustomMsgState>,
    /// Per-origin contiguous-prefix handled watermark (F5 compaction).
    /// Merge rule on `restore`: per-origin grow-only MAX, then every
    /// local entry the merged watermark subsumes is pruned (a peer's
    /// higher watermark PROVES those seqs were handled cluster-wide).
    /// `#[serde(default)]` keeps wire compat with a pre-field sender.
    #[serde(default)]
    pub custom_terminal_watermarks: HashMap<String, u64>,
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

impl<I> Default for ClusterStateSnapshot<I> {
    /// The EMPTY partial snapshot — every field at its merge-neutral
    /// value, so `restore()` of a default is a complete no-op. The
    /// snapshot STREAM builds its partials from this base (a task-batch
    /// package is `default + tasks + task_outputs`; the tail package is
    /// `default + phase_event_tallies`), which is exactly why each
    /// package can route through the ONE `restore` lattice unchanged.
    /// Hand-written (not derived) so no spurious `I: Default` bound is
    /// added — every field is a container/latch whose `Default` is
    /// `I`-independent.
    fn default() -> Self {
        Self {
            tasks: HashMap::new(),
            current_primary: None,
            primary_epoch: 0,
            phase_deps: HashMap::new(),
            phase_may_be_empty: HashSet::new(),
            phase_no_barrier: HashSet::new(),
            capabilities: HashMap::new(),
            peer_holdings: HashMap::new(),
            task_outputs: HashMap::new(),
            secondary_capacities: HashMap::new(),
            alive_members: HashSet::new(),
            member_generations: HashMap::new(),
            run_complete: false,
            run_aborted: None,
            terminal_outcome: None,
            graceful_abort_requested: false,
            wind_down_requested: HashSet::new(),
            discovery_debt: DiscoveryDebt::default(),
            phase_event_tallies: HashMap::new(),
            retry_passes_used: HashMap::new(),
            unfulfillable_reinject_used: HashMap::new(),
            respawn_events: HashMap::new(),
            respawn_policy: None,
            phases_ended: HashSet::new(),
            custom_messages: HashMap::new(),
            custom_terminal_watermarks: HashMap::new(),
        }
    }
}

impl<I: Identifier> ClusterState<I> {
    /// Take a snapshot of the whole state. The snapshot is a deep
    /// clone — applying further mutations to `self` after this call
    /// does not affect the returned snapshot. Composed from the stream
    /// partitions (head ∪ task entries ∪ tail) so the field
    /// classification lives ONCE, in [`Self::stream_head`]'s exhaustive
    /// destructure. Used by the snapshot-stream plan's full-equality
    /// tests and any in-memory capture; the WIRE transfer is the
    /// package stream (see `cluster_state::stream`), never one
    /// monolithic serialization of this value.
    ///
    /// FAT entries only: a SETTLED (spilled) entry's body lives in the
    /// node-local spill file and is deliberately NOT rehydrated here —
    /// the stream serves settled entries per-key from the file, and the
    /// promotion capture (the one production caller) pairs this with
    /// the settled-base handover (`settled_base_clone`), which IS the
    /// settled half of the seed.
    pub fn snapshot(&self) -> ClusterStateSnapshot<I> {
        let mut snap = self.stream_head();
        snap.tasks = self.tasks.clone();
        snap.task_outputs = self.task_outputs.clone();
        snap.phase_event_tallies = self.phase_event_tallies.clone();
        snap
    }

    /// The HEAD partition of the snapshot stream: every replicated
    /// field EXCEPT the three the stream carries separately —
    /// `tasks` + `task_outputs` (the O(ledger) bulk, shipped as
    /// byte-bounded task-batch packages in canonical sorted-key order)
    /// and `phase_event_tallies` (the join-BUMPED grow-max map, shipped
    /// in the TAIL package: the #358 states-before-fields order rule
    /// holds per-STREAM too — a tally import must never precede the
    /// task states whose events it counts, see
    /// `cluster_state::stream`).
    ///
    /// Shipped FIRST so a joiner learns the control-plane facts
    /// (current primary, membership, run latches, capabilities) before
    /// the bulk transfer runs.
    pub(crate) fn stream_head(&self) -> ClusterStateSnapshot<I> {
        // Exhaustive destructure (NO `..` rest pattern) — the structural
        // completeness guard. Every `ClusterState` field is NAMED here,
        // so adding a future field is a COMPILE ERROR at this site until
        // the developer explicitly classifies it head / task-batch /
        // tail / node-local. This is the only mechanism that catches a
        // silently-omitted replicated field (the bug this exists to
        // prevent); `snapshot()` composes from this same site, so the
        // full capture and the stream partitions can never diverge.
        let ClusterState {
            // ── task-batch partition: carried by the stream's task
            // packages (and re-attached by `snapshot()`); NOT part of
            // the head ──
            tasks: _tasks,
            current_primary,
            primary_epoch,
            phase_deps,
            phase_may_be_empty,
            phase_no_barrier,
            run_complete,
            run_aborted,
            terminal_outcome,
            graceful_abort_requested,
            wind_down_requested,
            discovery_debt,
            role_table: _role_table,
            peer_state,
            capabilities,
            peer_holdings,
            // ── task-batch partition: each task's keyed outputs ride in
            // the SAME package as the task (the restore join reads the
            // co-present entry) ──
            task_outputs: _task_outputs,
            secondary_capacities,
            // ── tail partition: the join-BUMPED grow-max map (F4) must
            // arrive AFTER every task state it counts (#358 order rule
            // projected onto the stream) ──
            phase_event_tallies: _phase_event_tallies,
            // Replicated grow-only-MAX maps (P3) — NOT join-bumped
            // (originator-API-only writers), so import order vs the task
            // merge is free and they ride the head.
            retry_passes_used,
            unfulfillable_reinject_used,
            // Replicated grow-only SET (F7).
            respawn_events,
            // Replicated run-constant respawn caps.
            respawn_policy,
            // Replicated grow-only SET (#343).
            phases_ended,
            // Replicated custom-message inbox + watermarks (F5).
            custom_messages,
            custom_terminal_watermarks,
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
            task_state_change_tx: _task_state_change_tx,
            // node-local: the originator's per-hash version counter is not
            // part of the converged ledger (each replica mints its own on
            // origination; a restoring replica cold-starts it).
            task_seq: _task_seq,
            // node-local: the dead-rejoin WARN throttle is a per-node log
            // gate (#416) — each replica throttles its own log stream, so
            // it never crosses the wire.
            dead_rejoin_warn: _dead_rejoin_warn,
            // node-local: the digest memo + its fold counter are pure
            // derivations of the replicated fields (the memo IS the digest
            // of this very snapshot's content), so they carry no signal of
            // their own and never cross the wire.
            digest_cache: _digest_cache,
            digest_fold_count: _digest_fold_count,
            // node-local: the range-fold memo is a pure derivation of the
            // replicated `tasks` + `settled` (it IS the range fold of this
            // snapshot's content), so it carries no signal of its own and
            // never crosses the wire — a restoring replica maintains its own.
            range_fold_memo: _range_fold_memo,
            // ── task-batch partition, file-served ──: a settled entry is
            // a `tasks` ledger entry whose fat body lives in the spill
            // file; the snapshot STREAM serves it per-key from the file
            // (`settled_record`) inside the same task-batch packages.
            // `snapshot()` (the in-memory capture) carries fat entries
            // only — its production caller (the promotion signal) pairs
            // it with the settled-base handover (`settled_base_clone`).
            settled: _settled,
            // node-local: slurm-authoritative life-state snapshot consumed
            // by the apply-path sticky-removal reversibility tiebreak
            // (#546) — a pure runtime handle the restoring replica re-wires
            // on its own deployment side, never crosses the wire.
            authority_snapshot: _authority_snapshot,
        } = self;
        ClusterStateSnapshot {
            // Task-batch partition — empty in the head; the stream's
            // task packages carry the entries (and `snapshot()`
            // re-attaches the full maps for in-memory captures).
            tasks: HashMap::new(),
            current_primary: current_primary.clone(),
            primary_epoch: *primary_epoch,
            phase_deps: phase_deps.clone(),
            // Replicated static phase-graph metadata — carried so a
            // promoted / late-joining node restores the consumer's
            // empty-drain opt-out (same contract as `phase_deps`).
            phase_may_be_empty: phase_may_be_empty.clone(),
            // Replicated `barrier=False` opt-in set — carried so a
            // promoted / late-joining node restores the consumer's
            // pipelined-edge opt-in (same contract as `phase_may_be_empty`).
            phase_no_barrier: phase_no_barrier.clone(),
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
            // Task-batch partition — empty in the head; each output
            // entry rides the SAME task package as its task so the
            // restore join reads the co-present entry.
            task_outputs: HashMap::new(),
            // Per-secondary static capacity — carried so a freshly-
            // promoted primary and late-joining observers reconstruct
            // the full worker roster on snapshot-restore.
            secondary_capacities: secondary_capacities.clone(),
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
            // Pair each ALIVE member with its membership-incarnation
            // generation so a restoring node can RE-ADMIT a peer it
            // still holds `Dead` at a lower generation (it missed the
            // generation-advancing re-admission `PeerJoined`).
            member_generations: peer_state
                .iter()
                .filter(|(_, entry)| entry.state == PeerState::Alive)
                .map(|(id, entry)| (id.clone(), entry.member_gen))
                .collect(),
            // Sticky-monotonic run latches — carried so a node seeded
            // from a snapshot learns the run is already over / aborted.
            run_complete: *run_complete,
            run_aborted: run_aborted.clone(),
            // The verdict's finalized counts — carried with the run latches
            // so a node seeded from a snapshot narrates the SAME
            // authoritative terminal partition (atomic latch+counts carriage,
            // here via the snapshot rather than the live mutation).
            terminal_outcome: *terminal_outcome,
            // Sticky-monotonic graceful-abort latch — carried so a
            // failover-promoted primary inherits the dispatch freeze.
            graceful_abort_requested: *graceful_abort_requested,
            // Grow-only per-peer wind-down directive set (#467) — carried
            // so a failover-promoted primary inherits every in-flight
            // wind-down and the directed secondary still stands down after
            // the relocation.
            wind_down_requested: wind_down_requested.clone(),
            // Sticky-monotonic discovery-debt latch — carried so a promoted
            // primary inherits "discovery already settled" and does NOT
            // re-run discovery on failover.
            discovery_debt: *discovery_debt,
            // Tail partition — empty in the head: the join-bumped F4
            // tally map must arrive AFTER every task state it counts
            // (#358 order rule projected onto the stream).
            phase_event_tallies: HashMap::new(),
            // Replicated grow-only-MAX maps (P3) — carried so a promoted
            // primary inherits the retry / reinject used-budgets via
            // max-merge on restore. NOT join-bumped, so head-safe.
            retry_passes_used: retry_passes_used.clone(),
            unfulfillable_reinject_used: unfulfillable_reinject_used.clone(),
            // Replicated grow-only SET (F7) — carried so a promoted primary
            // inherits the respawn ledger via union-merge on restore (the
            // admission budget + cooldown survive failover).
            respawn_events: respawn_events.clone(),
            // Replicated run-constant respawn caps — carried so a promoted
            // primary re-arms the respawn decision at hydrate (first-write-
            // wins on restore).
            respawn_policy: *respawn_policy,
            // Replicated grow-only SET (#343) — carried so a promoted
            // primary inherits which phases already fired `on_phase_end`
            // (the no-redo decision input) via union-merge on restore.
            phases_ended: phases_ended.clone(),
            // Replicated custom-message inbox + watermarks (F5) — carried
            // so a promoted primary inherits every `Unhandled` entry for
            // the hydrate replay, and the compaction state converges.
            custom_messages: custom_messages.clone(),
            custom_terminal_watermarks: custom_terminal_watermarks.clone(),
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
        // The restore chokepoint: this entry (and the per-task
        // `merge_task_state` join + grow-max field merges it runs below) is
        // the only path a snapshot merge changes a digest-folded field, so
        // clear the memo once here. See `invalidate_digest_cache`.
        self.invalidate_digest_cache();
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
            phase_may_be_empty,
            phase_no_barrier,
            capabilities,
            peer_holdings,
            task_outputs,
            secondary_capacities,
            alive_members,
            member_generations,
            run_complete,
            run_aborted,
            terminal_outcome,
            graceful_abort_requested,
            wind_down_requested,
            discovery_debt,
            phase_event_tallies,
            retry_passes_used,
            unfulfillable_reinject_used,
            respawn_events,
            respawn_policy,
            phases_ended,
            custom_messages,
            custom_terminal_watermarks,
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
        // #520: a restore-delivered transition is a CRDT change the observer
        // narrates exactly like a live one — same merge seam, same
        // exactly-once contract (a re-restore NoOps). The narration event is
        // emitted by the shared `set_task_state` write path inside
        // `merge_task_state`, so narration is PATH-INDEPENDENT by construction:
        // a TaskCompleted/Assigned that arrives only via snapshot (its live
        // broadcast dropped) narrates through the same single write path as the
        // live one. This loop emits only the terminal-completion event the
        // join pre-builds for the (separate) task-completed channel.
        for (hash, incoming) in tasks {
            let co_present_outputs = task_outputs.get(&hash).cloned();
            if let super::merge::MergeOutcome::Applied { event: Some(ev), .. } =
                self.merge_task_state(&hash, incoming, co_present_outputs, resumed)
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
        // `may_be_empty` set: same static-graph lifecycle as `phase_deps` —
        // adopt on first-bootstrap (local empty). It is the consumer's
        // set-once declaration, so a non-empty local is already the run's
        // graph; a divergent incoming set is the same contract violation
        // `phase_deps` guards, and keeping local is the conservative
        // first-write-wins choice (the empty-drain policy fails loud on the
        // safe side if a gate's opt-out were ever dropped).
        if self.phase_may_be_empty.is_empty() {
            self.phase_may_be_empty = phase_may_be_empty;
        }
        // `no_barrier` set: same static-graph lifecycle as `phase_deps` —
        // adopt on first-bootstrap (local empty). The consumer's
        // `PhaseSpec.barrier=False` declaration is set-once at run start
        // (the topology fact), so a non-empty local already encodes the
        // run's pipelined-edge opt-in; a divergent incoming set keeps the
        // local for the same conservative first-write-wins reasoning as
        // `phase_may_be_empty`.
        if self.phase_no_barrier.is_empty() {
            self.phase_no_barrier = phase_no_barrier;
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
        // Alive-membership merge: Dead-wins WITHIN one membership
        // incarnation / generation-advances re-admit, mirroring the
        // `PeerJoined`/`PeerRemoved` apply rules in `apply_peer.rs`.
        // For each incoming alive id (its incarnation generation read
        // from `member_generations`, defaulting 0 for a pre-field
        // snapshot — the pre-generation semantics):
        //   - no local entry → insert a fresh `Alive` at that generation;
        //   - local entry at a STRICTLY LOWER generation → re-admit /
        //     advance it (the snapshotting node observed the generation-
        //     advancing `PeerJoined` this node missed — keeping the
        //     stale `Dead` would bury the re-admitted live peer forever);
        //   - local entry at the same-or-higher generation → keep it
        //     (sticky removal wins within the incarnation, exactly as
        //     `apply_peer_joined` drops a non-advancing join for a
        //     `Dead` id; an already-`Alive` local entry is a no-op).
        // Idempotent + order-insensitive (the generation pick is a max).
        // The entry holds ONLY liveness (C6); the role capabilities ride
        // the separate `capabilities` 2P-set merged above, and the
        // `RoleTable` projections are rebuilt from `capability × alive`
        // by `reproject_roles` below.
        let mut alive_changed = false;
        for id in alive_members {
            let incoming_gen = member_generations.get(&id).copied().unwrap_or(0);
            match self.peer_state.entry(id) {
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(PeerEntry {
                        state: PeerState::Alive,
                        member_gen: incoming_gen,
                        pubkey: None,
                        endpoint: None,
                    });
                    alive_changed = true;
                }
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    let entry = e.get_mut();
                    if incoming_gen > entry.member_gen {
                        entry.state = PeerState::Alive;
                        entry.member_gen = incoming_gen;
                        alive_changed = true;
                    }
                }
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
        // Verdict-count payload: first-`Some`-wins, the same latch rule as
        // `run_aborted` — a behind replica seeded from a snapshot receives
        // the authoritative terminal counts ALONGSIDE the run latch (the
        // snapshot-borne twin of the live mutation's atomic latch+counts),
        // and an already-latched local value is never overwritten.
        if self.terminal_outcome.is_none()
            && let Some(counts) = terminal_outcome
        {
            self.terminal_outcome = Some(counts);
        }
        // Graceful-abort latch: the same false→true ratchet as
        // `run_complete` — a promoted primary restoring a frozen snapshot
        // inherits the dispatch freeze and refuses to schedule (no-redo).
        self.graceful_abort_requested |= graceful_abort_requested;
        // Per-peer wind-down directive set (#467): plain set UNION (the
        // grow-only join the directive declares) so a promoted primary
        // inherits every in-flight wind-down and a stale snapshot can
        // never un-request one. Idempotent + order-insensitive (set
        // insert), the same shape as `phases_ended` below.
        self.wind_down_requested.extend(wind_down_requested);
        // Discovery-debt latch: sticky-monotonic join = `max` over the
        // total order `Undeclared ⊑ Owed ⊑ Settled` (the derived `Ord`).
        // A replica only moves UP: an incoming `Settled` ratchets a local
        // `Owed`/`Undeclared → Settled`; an incoming `Owed` ratchets a local
        // `Undeclared → Owed` but NEVER overwrites a local `Settled`; an
        // incoming `Undeclared` loses to both. So a promoted primary that
        // restored a `Settled` snapshot inherits "discovery done" and does
        // not re-run it, while a replica that missed the live `Declared`
        // broadcast still learns `Owed` via the snapshot pull (the case the
        // single-bool digest could not carry — see `digest()` / `is_behind`).
        self.discovery_debt = self.discovery_debt.max(discovery_debt);
        // Grow-only-MAX maps (F4 + P3): per-key MAX merge so a promoted
        // primary inherits the per-phase event tallies and the retry /
        // reinject used-budgets, and a stale peer's snapshot can never
        // resurrect a lower count (max never decreases). Converges under
        // max regardless of (live-broadcast, snapshot) arrival order — the
        // exact property that makes the run-start `clear()` unnecessary AND
        // safe. The merge rule is spelled once in `grow_max::merge_grow_max`.
        //
        // ORDER IS LOAD-BEARING for F4 (#358): this field merge must run
        // AFTER the per-task `merge_task_state` loop at the top of this fn.
        // The join bumps the tally on each winning terminal transition; a
        // snapshot's tally count covers exactly the events its own task
        // states reflect, so merging states FIRST lets each in-snapshot
        // transition bump once and the `max` here then aliases (never adds)
        // those same events. Field-first would max-import the count and
        // THEN bump again for the same snapshot's transitions = overshoot.
        merge_grow_max(&mut self.phase_event_tallies, phase_event_tallies);
        merge_grow_max(&mut self.retry_passes_used, retry_passes_used);
        merge_grow_max(
            &mut self.unfulfillable_reinject_used,
            unfulfillable_reinject_used,
        );
        // Grow-only SET (F7): union-by-key merge so a promoted primary
        // inherits the full respawn ledger and a stale peer's snapshot can
        // never remove an event (the budget + cooldown survive failover).
        // A `new_id` is globally unique per event and its value is written
        // exactly once, so shared keys never diverge — union-by-key is
        // correct + idempotent. The merge rule is spelled once in
        // `grow_max::merge_grow_set`.
        merge_grow_set(&mut self.respawn_events, respawn_events);
        // Respawn-policy caps: same static-config lifecycle as
        // `phase_may_be_empty` — adopt on first-bootstrap (local `None`).
        // The policy is set once per run by the submitter's seed, so a
        // non-`None` local is already the run's policy; first-write-wins
        // keeps a divergent (contract-violating) incoming value from
        // flapping the budget gate.
        if self.respawn_policy.is_none() {
            self.respawn_policy = respawn_policy;
        }
        // Grow-only SET (#343): plain set UNION so a promoted primary
        // inherits which phases already fired `on_phase_end` and a stale
        // peer's snapshot can never un-end a phase. Idempotent +
        // order-insensitive (set insert), the OR-join the fact declares.
        self.phases_ended.extend(phases_ended);
        // F5 custom-message inbox: per-origin watermark grow-MAX first
        // (a peer's higher watermark PROVES every subsumed seq was
        // handled cluster-wide — prune the newly-covered local entries),
        // then the per-key sticky-latch join over the incoming entries
        // (`Handled` wins over `Unhandled`; an absent key vacant-inserts;
        // watermark-covered keys are skipped), then the apply-side
        // compaction re-runs per touched origin so a restore that
        // completes a handled prefix compacts exactly like the live
        // apply path would (apply == restore by construction).
        let watermark_origins: Vec<String> = custom_terminal_watermarks.keys().cloned().collect();
        merge_grow_max(
            &mut self.custom_terminal_watermarks,
            custom_terminal_watermarks,
        );
        for origin in &watermark_origins {
            self.prune_below_custom_watermark(origin);
        }
        let mut touched_origins: HashSet<String> = HashSet::new();
        for ((origin, seq), incoming) in custom_messages {
            if self
                .custom_terminal_watermarks
                .get(&origin)
                .is_some_and(|w| seq <= *w)
            {
                continue;
            }
            match self.custom_messages.entry((origin.clone(), seq)) {
                std::collections::hash_map::Entry::Vacant(e) => {
                    if matches!(incoming, CustomMsgState::Handled) {
                        touched_origins.insert(origin);
                    }
                    e.insert(incoming);
                }
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    if matches!(incoming, CustomMsgState::Handled)
                        && matches!(e.get(), CustomMsgState::Unhandled { .. })
                    {
                        e.insert(CustomMsgState::Handled);
                        touched_origins.insert(origin);
                    }
                }
            }
        }
        for origin in &touched_origins {
            self.compact_custom_watermark(origin);
        }
    }
}
