//! Originator-side apply-locally + broadcast filter and the
//! transport-facing role-change hook registration boundary.
//!
//! Single concern: the wire-shape facade between the CRDT apply
//! pass and the transport-layer broadcast. Two items:
//!
//! - The `RoleChangeHookRegistrar` impl, which lets a transport
//!   register its write-through `RoleTable` cache against the
//!   authoritative ledger.
//! - `apply_locally_for_broadcast` + its `AppliedBatch` output,
//!   the canonical place where the two originator paths (live
//!   primary, promoted secondary) apply a mutation batch locally
//!   and filter to the `Applied` subset so the wire doesn't
//!   amplify under peer-forward redundancy.

use std::sync::Arc;

use dynrunner_core::{Identifier, TaskInfo, TaskVersion};
use dynrunner_protocol_primary_secondary::{ClusterMutation, RoleChangeHookRegistrar, RoleTable};

use super::{ApplyOutcome, ClusterState};

impl<I: Identifier> ClusterState<I> {
    /// Mint the next monotone `TaskVersion` for `hash` (the originator's
    /// half of the D-V stamp). Bumps the node-local per-hash `task_seq`
    /// counter and pairs it with the current `primary_epoch`. The FIRST
    /// stamp for a hash is `seq = 1` (not 0) so even at `primary_epoch ==
    /// 0` (a pre-failover / local-bootstrap run that never minted an
    /// epoch) the first stamped version `(0, 1)` strictly EXCEEDS the
    /// `(0, 0)` default that a fresh state field (`TaskInfo.preferred_version`,
    /// the cold-start non-terminal version) and a legacy pre-field sender
    /// both carry — so a stamped update always wins its first apply.
    /// Total order is lexicographic `(primary_epoch, seq)`, so each
    /// subsequent stamp for the same key strictly exceeds the prior, and
    /// a post-promotion stamp (higher `primary_epoch`) exceeds every
    /// pre-promotion version regardless of the local `seq` cold-start.
    ///
    /// The key is a task content hash for per-task versions and a peer id
    /// for capability `cap_version`s (C6); the per-key monotone counter is
    /// generic over the key string — only monotonicity-per-key matters.
    pub(super) fn next_task_version(&mut self, hash: &str) -> TaskVersion {
        let seq = self
            .task_seq
            .entry(hash.to_string())
            .and_modify(|s| *s += 1)
            .or_insert(1);
        TaskVersion {
            primary_epoch: self.primary_epoch,
            seq: *seq,
        }
    }
}

/// Stamp the originator's `TaskVersion` onto every version-bearing
/// mutation in `mutations` (the per-task `version` AND the capability
/// `cap_version`) AND the per-task retry `attempt` onto the three
/// COPY-CURRENT variants (F2 / C-1), BEFORE the apply+filter loop (B3).
/// The ONE choke point both originator paths route through, so a
/// forgotten stamp at any `failed.rs`/`handler.rs`/`mutations.rs`/
/// `coordinator.rs` origination site is impossible — those sites build
/// the mutation with `version: Default::default()` (or `cap_version:
/// Default::default()`, or `attempt: 0`) and this pass overwrites it.
///
/// Attempt-stamping at the choke point (C-1, NOT per-origination-site):
/// `TaskAssigned`/`TaskCompleted`/`TaskFailed` are COPY-CURRENT — they
/// build a candidate state from the task's existing ledger entry — so the
/// choke point stamps each with the task's CURRENT `attempt` (read via
/// `task_state(hash).attempt()`, EXACTLY as `version` is stamped via
/// `next_task_version`). At broadcast time the task is already at
/// `attempt: n+1` if a `TaskRetried` reset applied earlier, so the copy-
/// current variants pick it up automatically. Stamping it here — instead
/// of at each of the N origination sites — keeps the logic in ONE place
/// and makes a missed-site `attempt:0`-for-a-retried-task hazard (which
/// would lose the join and reintroduce the lost work) impossible. ONLY
/// `TaskRetried` (the n→n+1 INCREMENT, not a copy) carries an originator-
/// computed `attempt` and is NOT attempt-stamped here.
///
/// Compile guard (B3): the match is EXHAUSTIVE over `ClusterMutation`.
/// The version-bearing variants are matched by DESTRUCTURING their
/// `version` binding (so the field is named and written); a NEW
/// version-bearing variant added without a stamp arm here is a COMPILE
/// ERROR against the enum's exhaustiveness (it would have to be added to
/// one arm or the other). The `_`-equivalent arm lists the genuinely
/// version-less variants EXPLICITLY (no `..` rest) — the invariant a
/// reviewer enforces is that it NEVER silently swallows a
/// `version`-bearing variant. A forgotten stamp would otherwise degrade
/// silently to `(0,0)` = the losing value.
fn stamp_versions<I: Identifier>(
    state: &mut ClusterState<I>,
    mutations: &mut [ClusterMutation<I>],
) {
    for m in mutations.iter_mut() {
        match m {
            // COPY-CURRENT variants: stamp BOTH the minted `version` AND the
            // task's CURRENT `attempt` (C-1). The attempt read is `&self`
            // and the version mint is `&mut self`; reading the attempt into
            // a local FIRST keeps the borrows disjoint. A task absent from
            // the ledger (an out-of-order copy-current ahead of its
            // `TaskAdded`) reads attempt 0 — the apply arm NoOps it anyway
            // (no local entry), so the stamped 0 never lands.
            ClusterMutation::TaskAssigned {
                hash,
                version,
                attempt,
                ..
            }
            | ClusterMutation::TaskFailed {
                hash,
                version,
                attempt,
                ..
            } => {
                *attempt = state.task_state(hash).map_or(0, |s| s.attempt());
                *version = state.next_task_version(hash);
            }
            ClusterMutation::TaskPreferredSecondariesUpdated { hash, version, .. }
            | ClusterMutation::TaskReinjected { hash, version }
            | ClusterMutation::TaskRequeued { hash, version }
            | ClusterMutation::TaskRetried { hash, version, .. } => {
                *version = state.next_task_version(hash);
            }
            // `TaskCompleted` is version-LESS (idempotent + rank-dominant)
            // but DOES carry the F2 `attempt`: stamp it from the task's
            // current generation so the completion preserves the attempt it
            // completed under.
            ClusterMutation::TaskCompleted { hash, attempt, .. } => {
                *attempt = state.task_state(hash).map_or(0, |s| s.attempt());
            }
            // Capability mutations carry the SAME monotone stamp keyed by
            // PEER id (C6): the `cap_version` arbitrates a `can_be_primary`
            // flip-back so a missed `SetCanBePrimary(false)` heals. Keyed
            // by `peer_id` (not a task hash), but the per-key
            // `next_task_version` counter is generic over the key string.
            ClusterMutation::PeerJoined {
                peer_id,
                cap_version,
                ..
            }
            | ClusterMutation::SetCanBePrimary {
                peer_id,
                cap_version,
                ..
            } => {
                *cap_version = state.next_task_version(peer_id);
            }
            // The genuinely version-LESS, attempt-LESS variants, listed
            // EXPLICITLY (B3 invariant: this arm must NEVER swallow a
            // version-bearing OR attempt-bearing variant). `TaskAdded`
            // (vacant-insert at the cold attempt 0), `TaskBlocked`
            // (cascade-pause keyed on `on`; its attempt is preserved from
            // the Pending source in the apply arm, not stamped here), and
            // every non-versioned non-task mutation. (`TaskCompleted` is
            // attempt-stamped above.)
            ClusterMutation::TaskAdded { .. }
            | ClusterMutation::TaskBlocked { .. }
            | ClusterMutation::PrimaryChanged { .. }
            | ClusterMutation::PhaseDepsSet { .. }
            | ClusterMutation::RunComplete
            | ClusterMutation::RunAborted { .. }
            | ClusterMutation::PeerRemoved { .. }
            | ClusterMutation::PeerResourceHoldingsUpdated { .. }
            | ClusterMutation::SecondaryCapacity { .. }
            | ClusterMutation::TasksSpawned { .. } => {}
        }
    }
}

/// `ClusterState` is the authoritative role-table owner; transports
/// register their write-through cache through this boundary trait.
///
/// The implementation appends to the internal `Vec<RoleChangeHook>`;
/// hooks accumulate across calls and are fired (in registration
/// order) by `apply` whenever a mutation actually changes the table.
/// Today the only registrant is the `PeerTransport` write-through
/// cache, one per node.
impl<I: Identifier> RoleChangeHookRegistrar for ClusterState<I> {
    fn register_role_change_hook(&mut self, hook: Box<dyn Fn(&RoleTable) + Send + Sync + 'static>) {
        self.role_change_hooks.push(Arc::from(hook));
    }
}

/// Output of [`apply_locally_for_broadcast`]: the wire subset to
/// broadcast plus every `TaskInfo<I>` that was just auto-resumed
/// from `Blocked → Pending` by a `TaskCompleted` mutation in this
/// batch (see [`ClusterState::apply_with_resumed_blocked`]).
///
/// Originator-side callers must re-inject `resumed_for_dispatch`
/// into their live `PendingPool` (the cascade-paused entries were
/// dropped from the pool by the earlier `on_item_failed_permanent`
/// call; only the CRDT auto-resume kept them addressable). The
/// promoted-secondary originator path's pool seeds Blocked items
/// from the CRDT at promotion time and tracks them via the pool's
/// own `task_depends_on` graph, so its caller may silently discard
/// the list.
#[derive(Debug)]
pub(crate) struct AppliedBatch<I: Identifier> {
    pub applied: Vec<ClusterMutation<I>>,
    pub resumed_for_dispatch: Vec<TaskInfo<I>>,
}

/// Apply each mutation to `state` locally and return the subset that
/// actually changed state (`ApplyOutcome::Applied`) plus every
/// `TaskInfo<I>` the apply pass auto-resumed from `Blocked` to
/// `Pending`. `NoOp` mutations are dropped from the wire batch —
/// under the CRDT's idempotency contract a re-application against the
/// post-state is silent, and re-broadcasting a NoOp would amplify
/// under peer-forward redundancy (every peer forwarding observed
/// terminal events to the primary would turn one TaskComplete into N
/// re-broadcasts = N² messages).
///
/// Single concern: apply-locally + filter to applied (+ surface the
/// resumed-for-dispatch list). The broadcast step and the pool
/// re-injection step are both caller-specific (the authority primary
/// re-injects resumed dependents into its dispatch pool; the secondary
/// originator holds no pool and discards the resumed list — the
/// same-peer authority drives dispatch off the same mutation), so they
/// stay at the call sites. This free function is the canonical place to
/// perform the apply+filter so the two originator paths can't drift on
/// the filter semantics.
///
/// Callers:
///   - `primary::lifecycle::apply_and_broadcast_cluster_mutations`
///     (the authority primary's originator path).
///   - `secondary::origination::apply_and_broadcast_mutations` (the
///     secondary-side originator path, used by the panik self-departure
///     announcement).
pub(crate) fn apply_locally_for_broadcast<I: Identifier>(
    state: &mut ClusterState<I>,
    mut mutations: Vec<ClusterMutation<I>>,
) -> AppliedBatch<I> {
    // Version-stamp pass (B3): the SINGLE origination choke point stamps
    // the monotone `TaskVersion` onto every version-bearing mutation
    // BEFORE the apply+filter loop, so a forgotten stamp at any
    // origination call site is impossible. The applied subset re-broadcast
    // below carries the stamped versions.
    stamp_versions(state, &mut mutations);
    let mut applied: Vec<ClusterMutation<I>> = Vec::with_capacity(mutations.len());
    let mut resumed_for_dispatch: Vec<TaskInfo<I>> = Vec::new();
    // The originator paths (live primary's `apply_spawn_tasks`,
    // promoted-secondary's `apply_spawn_tasks`) already walk the
    // post-apply CRDT via `task_state(&hash)` lookups to reinject
    // freshly-Pending entries; the apply rule's
    // `newly_pending_from_spawn` surface targets the receive-side
    // callers (`apply_cluster_mutations`) instead, so we accept the
    // surface here and discard. The scratch buffer is allocated once
    // and reused across the batch.
    let mut _newly_pending_scratch: Vec<TaskInfo<I>> = Vec::new();
    for m in mutations {
        let outcome = state.apply_with_resumed_blocked(
            m.clone(),
            &mut resumed_for_dispatch,
            &mut _newly_pending_scratch,
        );
        if outcome == ApplyOutcome::Applied {
            applied.push(m);
        }
    }
    AppliedBatch {
        applied,
        resumed_for_dispatch,
    }
}
