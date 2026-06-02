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

use dynrunner_core::{BoundedString, Identifier, MessageReceiver, MessageSender, PhaseId, TaskInfo};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PeerTransport, RemovalCause,
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

    /// Apply a `ClusterMutation::TaskCompleted` LOCALLY on this
    /// secondary's `cluster_state` without broadcasting.
    ///
    /// Single concern: synchronise the local CRDT mirror for a
    /// completion this node is about to act on in the SAME await frame
    /// (the `note_primary_item_completed` → `on_item_finished` cascade
    /// releases a dependent in `primary_pending`, and the immediate
    /// follow-up `request_task_for_worker(...).await` on a still-idle
    /// worker dispatches into `handle_primary_task_request`, which
    /// reads `predecessor_outputs` from `self.cluster_state` via
    /// [`crate::primary::task::predecessor_outputs::gather_predecessor_outputs`]).
    ///
    /// Without this synchronisation, the dispatch reads an empty
    /// `task_outputs` for the just-completed prerequisite: the
    /// canonical `ClusterMutation::TaskCompleted` originator on this
    /// completion path is the demoted-local primary, which only
    /// applies + broadcasts after the loopback
    /// `DistributedMessage::TaskComplete` arrives over its
    /// `primary_transport` channel and is dequeued by its own
    /// operational loop — strictly later than the same-frame self-
    /// assign on this node.
    ///
    /// Broadcast is intentionally NOT performed here: the
    /// demoted-local primary remains the single broadcast originator
    /// for completion mutations on the live-primary path, and the
    /// wire-side fan-out continues to flow through its
    /// `apply_and_broadcast_cluster_mutations`. The CRDT's idempotent
    /// `TaskCompleted` arm makes the demoted primary's later apply +
    /// broadcast → receive-side apply on this node a NoOp; the
    /// invariant "one originator per mutation" is preserved.
    ///
    /// Gated on `is_primary`: off the primary path this node forwards
    /// the completion via `send_to_current_primary` and does NOT
    /// dispatch in the same await frame, so the same-frame race does
    /// not exist and the local apply is unnecessary (and would
    /// duplicate the receive-side apply that the inbound broadcast
    /// triggers).
    ///
    /// Called from the two completion-receive sites that may dispatch
    /// in the same await frame on the promoted-secondary side:
    ///   - own-worker completion in
    ///     `secondary/processing/worker_event.rs`'s `TaskCompleted` arm
    ///   - peer-observed completion in
    ///     `secondary/peer/message_handler.rs`'s `TaskComplete` arm
    pub(in crate::secondary) fn apply_task_completed_locally_if_primary(
        &mut self,
        task_hash: String,
        result_data: Option<Vec<u8>>,
    ) {
        if !self.is_primary {
            return;
        }
        self.cluster_state.apply(ClusterMutation::TaskCompleted {
            hash: task_hash,
            result_data,
        });
    }

    /// React to a panik-watcher signal on this secondary.
    ///
    /// Single concern: turn the watcher's panik event into the side
    /// effects the emergency-stop contract requires. Behaviour branches
    /// on the watcher-documented source predicate
    /// [`crate::panik_watcher::is_sigterm_signal`] applied to
    /// `matched_path` — file-source vs SIGTERM-source carry different
    /// cluster semantics:
    ///
    /// **File source** (`matched_path` is a real filesystem path,
    /// matched by [`crate::panik_watcher::PanikWatcherConfig::paths`]):
    /// this node is leaving the mesh. All three steps fire:
    ///   1. Announce departure: originate a self-authored
    ///      `ClusterMutation::PeerRemoved { id: <self>, cause:
    ///      SelfDeparture(reason) }` — applied locally and fanned out
    ///      to every peer via [`Self::apply_and_broadcast_mutations`].
    ///      Peers LOG the departure and mark this node Dead. This is
    ///      observability only: it does NOT cancel cluster work or
    ///      terminate the run on peers, and the mesh stays free to
    ///      continue / re-elect.
    ///   2. Take down every worker pgid this secondary owns via
    ///      [`dynrunner_manager_local::pool::WorkerPool::kill_all_workers_with_grace`].
    ///   3. Surface the matched path + reason to the caller for
    ///      [`crate::secondary::RunOutcome::PanikShutdown`] (this
    ///      node's own local exit).
    ///
    /// **SIGTERM source** (`matched_path` is the documented sentinel
    /// [`crate::panik_watcher::SIGTERM_SENTINEL_PATH`], fired by the
    /// watcher's SIGTERM arm when
    /// [`crate::panik_watcher::PanikWatcherConfig::listen_for_sigterm`]
    /// is enabled): per-host SIGTERM (e.g. SLURM time-limit /
    /// `scancel` forwarded as `podman exec <c> kill -TERM <pid>`).
    /// This host's SIGTERM is a purely local event — no mesh
    /// announcement is broadcast; only steps 2 and 3 fire (local-only
    /// teardown + exit). A delayed/missed-SIGTERM peer remains free to
    /// re-elect and continue the run.
    ///
    /// Kill primitive uses negative-pgid `kill(-pgid, ...)` so worker
    /// descendants (subprocess pools, container exec children, etc.)
    /// are taken down too. The supplied `kill_grace` is the same
    /// window the SubprocessWorkerFactory uses for
    /// `terminate_children` — workers that installed a SIGTERM
    /// handler get a chance to exit cleanly before the escalation.
    ///
    /// Side effects are best-effort: broadcast errors (file-source
    /// only) are logged but never propagated (the panik-react path
    /// must always finish even if the peer mesh is degraded — the
    /// local kill + exit are the load-bearing terminal steps). Kill
    /// timing is bounded by the supplied `grace`.
    pub(in crate::secondary) async fn handle_panik_signal(
        &mut self,
        matched_path: std::path::PathBuf,
        kill_grace: std::time::Duration,
    ) -> (std::path::PathBuf, String) {
        // Source dispatch on the watcher's documented predicate.
        // File-source matched_path's reason is preserved verbatim for
        // downstream log-parser compatibility ("panik file: <path>");
        // SIGTERM-source uses a distinct reason that does not conflate
        // "file" with the source type.
        let is_sigterm = crate::panik_watcher::is_sigterm_signal(&matched_path);
        let reason = if is_sigterm {
            "panik SIGTERM (per-host)".to_string()
        } else {
            format!("panik file: {}", matched_path.display())
        };
        if is_sigterm {
            tracing::warn!(
                secondary = %self.config.secondary_id,
                matched_path = %matched_path.display(),
                "SIGTERM panik signal observed; local-only worker teardown \
                 (no cluster broadcast — mesh remains free to re-elect)"
            );
        } else {
            tracing::warn!(
                secondary = %self.config.secondary_id,
                matched_path = %matched_path.display(),
                "panik file observed; announcing self-departure and \
                 tearing down workers"
            );
            // Self-authored departure announcement: peers LOG it and
            // mark this node Dead (observability only). It does NOT
            // cancel cluster work or terminate the run on peers — the
            // mesh stays free to continue / re-elect. `BoundedString::from`
            // truncates at the 1 KiB cap `SelfDeparture` carries.
            let mutation = ClusterMutation::PeerRemoved {
                id: self.config.secondary_id.clone(),
                cause: RemovalCause::SelfDeparture(BoundedString::from(reason.clone())),
            };
            if let Err(e) = self.apply_and_broadcast_mutations(vec![mutation]).await {
                tracing::warn!(
                    error = %e,
                    "panik self-departure apply+broadcast failed; \
                     proceeding with local worker teardown anyway"
                );
            }
        }
        // Drain the per-task memprofile sampler BEFORE the bulk
        // kill drops the worker pool's `SubcgroupHandle`s (which
        // best-effort rmdirs the leaf cgroups the sampler reads
        // from). Mirrors the same ordering invariant the clean
        // teardown path uses — see `shutdown_sampler_if_present`.
        // Sampler shutdown is bounded by its own command-channel
        // drain so it can't extend the panik path's wallclock
        // budget meaningfully.
        self.shutdown_sampler_if_present().await;
        // Tear down every worker pgid with the SIGTERM → grace →
        // SIGKILL ladder. Owned by the pool concern; coordinator
        // just calls. Runs on both source paths — local teardown is
        // unconditional.
        self.pool.kill_all_workers_with_grace(kill_grace).await;
        (matched_path, reason)
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
