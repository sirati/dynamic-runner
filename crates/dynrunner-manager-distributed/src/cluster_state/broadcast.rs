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

use dynrunner_core::{Identifier, TaskInfo};
use dynrunner_protocol_primary_secondary::{ClusterMutation, RoleChangeHookRegistrar, RoleTable};

use super::{ApplyOutcome, ClusterState};

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
/// co-located authority drives dispatch off the same mutation), so they
/// stay at the call sites. This free function is the canonical place to
/// perform the apply+filter so the two originator paths can't drift on
/// the filter semantics.
///
/// Callers:
///   - `primary::lifecycle::apply_and_broadcast_cluster_mutations`
///     (the authority primary's originator path).
///   - `secondary::origination::apply_and_broadcast_mutations` (the
///     secondary-side originator path, used by `ingest_setup_discovery`
///     to seed the ledger with the discovery-time `TaskAdded` batch +
///     `PhaseDepsSet`, and by the panik self-departure announcement).
pub(crate) fn apply_locally_for_broadcast<I: Identifier>(
    state: &mut ClusterState<I>,
    mutations: Vec<ClusterMutation<I>>,
) -> AppliedBatch<I> {
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
