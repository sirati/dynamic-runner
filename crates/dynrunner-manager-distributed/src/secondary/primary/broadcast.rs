//! Originator-side cluster-mutation broadcast and setup-discovery
//! ingest.
//!
//! Single concern: produce a batch of `ClusterMutation`s on this
//! (promoted-secondary or natural-quiesce) node, run the
//! `apply_locally_for_broadcast` apply-first filter, and fan out the
//! `Applied` subset over both the peer mesh and the
//! demoted-submitter link. `ingest_setup_discovery` is the wrapper
//! entry-point that turns the wrapper-driven discovery result into
//! the canonical `PhaseDepsSet + N×TaskAdded` batch through the same
//! broadcast helper.

use std::collections::HashMap;

use dynrunner_core::{Identifier, MessageReceiver, MessageSender, PhaseId, TaskInfo};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PeerTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::wire::timestamp_now;
use super::super::SecondaryCoordinator;
use super::task_file_hash;
use crate::cluster_state::apply_locally_for_broadcast;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{

    /// Originator-side apply + broadcast for a batch of
    /// `ClusterMutation`s the promoted-secondary is producing (not
    /// receiving). Mirrors the live primary's
    /// `apply_and_broadcast_cluster_mutations` — local apply runs first
    /// via the shared `apply_locally_for_broadcast` helper (so the
    /// `Applied`-vs-`NoOp` filter semantics stay identical), then the
    /// applied subset fans out over BOTH the peer mesh and the demoted-
    /// submitter link.
    ///
    /// Fan-out shape: `peer_transport.broadcast` reaches every
    /// surviving secondary in the cluster; `primary_transport.send`
    /// reaches the demoted local submitter (which sits on the other end
    /// of this node's secondary→primary channel — not a peer, mute-
    /// routing is asymmetric). Without the demoted-submitter loopback
    /// the submitter's `mirror_mutation_to_accounting` never observes
    /// our `TaskAdded` batch and its `total_tasks` accounting stays at
    /// 0, tripping the exit-counter check the moment it sees 0+0>=0.
    ///
    /// Errors are best-effort: per-peer `peer_transport` failure
    /// surfaces as a single error string (the trait's contract); a
    /// dropped `primary_transport.send` means the submitter already
    /// exited and we're past the point where the loopback matters.
    /// CRDT idempotency makes a missed mutation recoverable from the
    /// next snapshot RPC; we never block the originator on universal
    /// delivery.
    ///
    /// Single concern: "originate a batch on the promoted-secondary
    /// side". Receiver-side application of mutations OBSERVED on the
    /// wire goes through `apply_cluster_mutations` (in dispatch.rs)
    /// instead.
    pub(in crate::secondary) async fn apply_and_broadcast_mutations(
        &mut self,
        mutations: Vec<ClusterMutation<I>>,
    ) -> Result<(), String> {
        if mutations.is_empty() {
            return Ok(());
        }
        // `apply_locally_for_broadcast` also surfaces auto-resumed
        // Blocked dependents (see the live primary's mirror site).
        // The promoted secondary's `primary_pending` pool was seeded
        // from CRDT at promotion time (`populate_primary_from_cluster_state`)
        // so any Blocked entries already exist in the pool as
        // `task_depends_on`-tracked items — the pool's own dep
        // machinery will dispatch them when the prereq's
        // `on_item_finished` fires through the normal task-event
        // path. Re-injecting the resumed clones here would
        // duplicate them in the pool's buckets, so we deliberately
        // discard the list on this path. (Unfulfillable-cascade
        // sequences originated post-promotion would also leave the
        // dependents in the pool's `blocked` map for the same
        // dep-machinery to unblock; the live primary's mirror site
        // is the only one whose pool actually loses the items via
        // `on_item_failed_permanent`.)
        let batch = apply_locally_for_broadcast(&mut self.cluster_state, mutations);
        let crate::cluster_state::AppliedBatch {
            applied,
            resumed_for_dispatch: _,
        } = batch;
        if applied.is_empty() {
            return Ok(());
        }
        let msg = DistributedMessage::ClusterMutation {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            mutations: applied,
        };
        // Fan out to both transports. Order matters only for symmetry
        // with `processing.rs`'s RunComplete fan-out (peer-first there;
        // mirror it here so future maintainers see one pattern). Errors
        // are logged but not propagated — see method doc.
        if let Err(e) = self.primary_transport.send(msg.clone()).await {
            tracing::warn!(
                error = %e,
                "ClusterMutation send to demoted submitter failed (submitter likely exited)"
            );
        }
        if let Err(e) = self.peer_transport.broadcast(msg).await {
            tracing::warn!(
                error = %e,
                "ClusterMutation peer broadcast failed"
            );
        }
        Ok(())
    }

    /// Ingest the result of Python's `task.discover_items` after a
    /// `PromotePrimary { required_setup: true }` promotion.
    ///
    /// Pre-condition: `setup_pending == true` (set by the
    /// `PromotePrimary` handler in `dispatch.rs` for the setup-promote
    /// reason). The outer process-tasks loop yielded back to the PyO3
    /// wrapper, which ran `task.discover_items` against the locally
    /// bind-mounted staged source filesystem and is now feeding the
    /// result back into the Rust core.
    ///
    /// Sequence:
    ///   1. Build the mutation batch: one `PhaseDepsSet` carrying the
    ///      task graph's static phase dependency map (so every
    ///      receiver's `cluster_state.phase_deps()` is populated
    ///      before any post-promotion hydration consults it), then one
    ///      `TaskAdded` per discovered binary.
    ///   2. Originate the batch via `apply_and_broadcast_mutations` —
    ///      applies locally to `cluster_state` and fans out to peers +
    ///      the demoted submitter. The submitter's
    ///      `mirror_mutation_to_accounting` updates its `total_tasks`
    ///      counter as the `TaskAdded` mutations arrive, so its
    ///      exit-counter check sees a non-zero target instead of
    ///      tripping at 0+0>=0.
    ///   3. Clear `setup_pending` so the outer loop's next iteration
    ///      doesn't yield again.
    ///   4. Hydrate `primary_pending` from the now-populated
    ///      `cluster_state`. This is the same call the pre-seeded
    ///      (`required_setup_on_promote = false`) path makes in the
    ///      `PromotePrimary` handler; here it runs after
    ///      `setup_pending` clears so operational dispatch can begin
    ///      on the next tick.
    ///
    /// Idempotency: `ClusterMutation::TaskAdded` is no-op-on-duplicate
    /// (the CRDT silently drops it via the `apply` filter), and
    /// `apply_locally_for_broadcast` filters NoOp mutations out before
    /// broadcast, so a duplicate `ingest_setup_discovery` call (e.g.
    /// from a wrapper retry on transport hiccup) doesn't re-broadcast
    /// the same batch.
    pub async fn ingest_setup_discovery(
        &mut self,
        binaries: Vec<TaskInfo<I>>,
        phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    ) -> Result<(), String> {
        let mut mutations: Vec<ClusterMutation<I>> = Vec::with_capacity(binaries.len() + 1);
        mutations.push(ClusterMutation::PhaseDepsSet { deps: phase_deps });
        for b in &binaries {
            mutations.push(ClusterMutation::TaskAdded {
                hash: task_file_hash(b),
                task: b.clone(),
            });
        }
        let task_count = binaries.len();
        self.apply_and_broadcast_mutations(mutations).await?;
        self.setup_pending = false;
        self.populate_primary_from_cluster_state();
        tracing::info!(
            tasks = task_count,
            "ingested setup-discovery; primary pool hydrated"
        );
        // Empty-discovery happy path: when discovery surfaces zero
        // items (e.g. every binary's output already exists under a
        // `--skip-existing` filter), the pool is drained from
        // inception and there will never be a `TaskCompleted` to
        // trigger the normal counter-driven RunComplete broadcast.
        // Originate RunComplete directly so every peer's exit arm
        // observes the same authoritative terminal signal.
        if task_count == 0 {
            self.apply_and_broadcast_mutations(vec![ClusterMutation::RunComplete])
                .await?;
            tracing::info!(
                "empty-discovery: RunComplete broadcast — no tasks to run"
            );
        }
        Ok(())
    }
}
