
use dynrunner_core::{Identifier, TaskInfo};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PeerTransport,
    SecondaryTransport,
};
use dynrunner_scheduler_api::{
    ResourceEstimator, Scheduler,
};

use crate::primary::PrimaryCoordinator;


impl<T: SecondaryTransport<I>, P: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, P, S, E, I> {

    /// Apply a replicated `DistributedMessage::ClusterMutation` batch.
    ///
    /// Single concern: keep the demoted primary's CRDT mirror — and the
    /// accounting sets the operational loop's exit-counter check
    /// reads — converged with the cluster's view, even when the live
    /// primary authority has handed off to a promoted secondary.
    ///
    /// Pre-fix the primary's `dispatch_message` had no arm for
    /// `MessageType::ClusterMutation`: any mutation broadcast addressed
    /// at the demoted primary fell through the catch-all. The
    /// operational loop's `completed + failed >= total` exit check
    /// reads `self.completed_tasks` / `self.failed_tasks`, which on a
    /// demoted primary are fed only by direct `TaskComplete` /
    /// `TaskFailed` forwards reaching the local primary's transport.
    /// Cross-secondary completions on the new primary's pool never
    /// arrived as direct forwards (the new primary doesn't loopback
    /// peer-observed completions to the demoted primary's transport),
    /// so the counter never reached the total and the run loop sat
    /// forever — the asm-dataset-nix R2 / T3 hang.
    ///
    /// Mirrors `secondary::dispatch::apply_cluster_mutations` in shape
    /// (idempotent fan-out over a `Vec<ClusterMutation>`); diverges in
    /// that the primary additionally maintains `completed_tasks` /
    /// `failed_tasks` because those are the sets the lifecycle
    /// exit-counter reads. The CRDT idempotency on `cluster_state`
    /// makes repeated apply safe; `HashSet::insert` is idempotent on
    /// the accounting side.
    pub(crate) async fn handle_cluster_mutation(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::ClusterMutation { mutations, .. } = msg {
            // Note whether any TaskAdded rides in this batch BEFORE
            // moving the mutations into apply. Only `TaskAdded` grows
            // the cluster ledger; refreshing `total_tasks` for any
            // other variant would be a wasted read on the hot terminal-
            // mutation path (TaskCompleted / TaskFailed).
            let has_task_added = mutations.iter().any(|m| {
                matches!(
                    m,
                    ClusterMutation::TaskAdded { .. }
                        | ClusterMutation::TasksSpawned { .. }
                )
            });
            // Collect any PeerJoined ids riding in the batch BEFORE
            // moving the mutations into apply. After the batch
            // applies, each joined id may have resolved a previously-
            // unknown entry in some task's `preferred_secondaries`,
            // so we forget its dedup state and re-run validation.
            // Same shape as `has_task_added`: snapshot pre-apply,
            // act post-apply.
            let joined_peer_ids: Vec<String> = mutations
                .iter()
                .filter_map(|m| match m {
                    ClusterMutation::PeerJoined { peer_id, .. } => {
                        Some(peer_id.clone())
                    }
                    _ => None,
                })
                .collect();
            // Snapshot the per-task list inside every TasksSpawned
            // mutation so we can mirror accounting AFTER apply (the
            // apply rule decides which task lands in Pending /
            // Blocked / Failed based on dependency resolution; the
            // pre-apply mirror can't see that classification).
            let spawned_task_batches: Vec<Vec<TaskInfo<I>>> = mutations
                .iter()
                .filter_map(|m| match m {
                    ClusterMutation::TasksSpawned { tasks } => Some(tasks.clone()),
                    _ => None,
                })
                .collect();
            // Receive-side pool growth surface: every `TasksSpawned`
            // entry the apply rule classifies as freshly `Pending`
            // (no deps, or all deps already `Completed`) is cloned
            // into `newly_pending`. A primary that still locally owns
            // a pool (`self.pending.is_some()`) reinjects each entry
            // so the pool stays coherent with the CRDT ledger across
            // wire-received batches.
            //
            // This matters for the re-promotion path: a demoted
            // primary applying a promoted-secondary's TasksSpawned
            // broadcast keeps the post-spawn tasks dispatchable in
            // its local pool, so a later re-election (the demoted
            // primary becomes live again) finds the pool already
            // aligned with the cluster's view. Without it, re-
            // election would resurrect the pool from its pre-spawn
            // snapshot and the post-spawn tasks would never
            // dispatch on this node.
            //
            // `resumed` is not consumed here for the same reason the
            // promoted-secondary's receive path discards it — the
            // pool's own dep machinery dispatches Blocked entries on
            // the prereq's `on_item_finished` event.
            let mut resumed: Vec<TaskInfo<I>> = Vec::new();
            let mut newly_pending: Vec<TaskInfo<I>> = Vec::new();
            for m in mutations {
                self.mirror_mutation_to_accounting(&m);
                self.cluster_state.apply_with_resumed_blocked(
                    m,
                    &mut resumed,
                    &mut newly_pending,
                );
            }
            if self.pending.is_some() {
                for task in newly_pending {
                    tracing::debug!(
                        phase = %task.phase_id,
                        task_id = ?task.task_id,
                        "pool: reinject freshly-Pending task from \
                         wire-received TasksSpawned"
                    );
                    self.pool_mut().reinject(task);
                }
            }
            for batch in &spawned_task_batches {
                self.mirror_tasks_spawned_post_apply(batch);
            }
            if !joined_peer_ids.is_empty() {
                // A peer that just joined may have resolved a
                // previously-unknown `preferred_secondaries` id; drop
                // each joined id from the warn dedup set so the next
                // validation cycle re-evaluates from scratch, then
                // walk every replicated task in `cluster_state` (the
                // authoritative post-apply view — on a demoted /
                // setup-promoted primary `all_binaries` is empty and
                // `cluster_state.iter_all()` is the only source).
                for id in &joined_peer_ids {
                    self.preferred_secondaries_validator.forget(id);
                }
                let known: std::collections::HashSet<&str> =
                    self.secondaries.keys().map(|s| s.as_str()).collect();
                let tasks: Vec<TaskInfo<I>> = self
                    .cluster_state
                    .iter_all()
                    .map(|(_, t)| t.clone())
                    .collect();
                self.preferred_secondaries_validator
                    .validate(tasks.iter(), &known);
            }
            if has_task_added {
                // Refresh from the post-apply CRDT view. In setup-defer
                // mode the demoted submitter starts with
                // `total_tasks = 0` (no local `all_binaries`, no
                // `seed_cluster_state` ever ran on this node) and only
                // learns the run's task count from the promoted
                // secondary's broadcast `TaskAdded` batch; without
                // this refresh the operational-loop exit check
                // (`completed + failed >= total_tasks`) trips at
                // `0 + 0 >= 0` the moment the demoted primary enters
                // the loop — before the chosen secondary has
                // broadcast its first TaskAdded — and the local
                // primary exits prematurely, taking the whole run
                // down with it.
                //
                // Legacy bootstrap path: `total_tasks` was set in
                // `run()` from `binaries.len()` before any mutation is
                // mirrored. The local `seed_cluster_state` batch
                // applies to the same count, so `task_count()`
                // converges to the same value the field already
                // holds — the refresh is a no-op write. Idempotent
                // against duplicate-Add via the CRDT's presence
                // semantics: re-applying a TaskAdded for a hash
                // already in the ledger leaves `task_count`
                // unchanged.
                self.total_tasks = self.cluster_state.task_count();
            }
        }
    }

    /// Update `completed_tasks` / `failed_tasks` (the sets the
    /// operational loop reads for its exit-counter check) from a
    /// single `ClusterMutation`. Non-terminal mutations
    /// (`TaskAdded`, `TaskAssigned`, `PrimaryChanged`, `PhaseDepsSet`,
    /// `RunComplete`) leave both sets untouched: `TaskAdded` /
    /// `TaskAssigned` describe non-terminal lifecycle states the
    /// counter check ignores, `PrimaryChanged` / `PhaseDepsSet` are
    /// orthogonal, and `RunComplete` flows through `cluster_state`'s
    /// own `run_complete` flag which the loop reads separately.
    /// (`total_tasks` is refreshed by the caller — see
    /// `handle_cluster_mutation` — after the batch applies, not per
    /// mutation here.)
    ///
    /// Terminal mutations preserve the same single-bucket invariant
    /// `handle_task_complete` / `handle_task_failed` enforce: a hash
    /// sits in exactly one of {completed, failed} at a time. A late
    /// `TaskCompleted` after a `TaskFailed` removes from `failed_tasks`
    /// and inserts into `completed_tasks` (success supersedes
    /// recoverable failure, mirroring the live-primary behaviour).
    /// `TaskFailed` for a hash already in `completed_tasks` does NOT
    /// regress — the cluster_state apply will NoOp the mutation by
    /// terminal-locked-out semantics, and we mirror that here by
    /// skipping the failed-set insert when the hash is already
    /// completed.
    fn mirror_mutation_to_accounting(&mut self, m: &ClusterMutation<I>) {
        match m {
            ClusterMutation::TaskCompleted { hash, .. } => {
                self.failed_tasks.remove(hash);
                self.completed_tasks.insert(hash.clone());
            }
            ClusterMutation::TaskFailed { hash, kind, .. } => {
                if !self.completed_tasks.contains(hash) {
                    self.failed_tasks.insert(hash.clone(), kind.clone());
                }
            }
            // `TaskAdded` proves the chosen secondary has run discovery
            // and seeded at least one task; `RunComplete` proves it
            // legitimately finished (including the zero-items case).
            // Either is the signal that the operational-loop exit-check
            // can safely re-enable on a demoted setup-promote submitter
            // — see the `setup_pending` field doc on
            // `PrimaryCoordinator`. Idempotent flip (a no-op on the
            // legacy path where the field is already `false`).
            ClusterMutation::TaskAdded { .. } | ClusterMutation::RunComplete => {
                self.setup_pending = false;
            }
            ClusterMutation::TaskReinjected { hash } => {
                // External-control reinject moves the entry off
                // `Failed` in the CRDT; the per-pass `failed_tasks`
                // ledger must mirror so the operational-loop exit
                // check (`completed + failed >= total`) doesn't trip
                // on a hash that's been re-armed for dispatch.
                self.failed_tasks.remove(hash);
            }
            ClusterMutation::TaskAssigned { .. }
            | ClusterMutation::PrimaryChanged { .. }
            | ClusterMutation::PhaseDepsSet { .. }
            | ClusterMutation::TaskPreferredSecondariesUpdated { .. }
            | ClusterMutation::PeerJoined { .. }
            | ClusterMutation::PeerRemoved { .. }
            | ClusterMutation::PeerResourceHoldingsUpdated { .. }
            | ClusterMutation::TaskBlocked { .. } => {
                // Routing / role / membership hints with no impact on
                // terminal-state accounting. `TaskBlocked` is a
                // cascade-pause notice — the dependent is dormant,
                // not failed, so the per-pass `failed_tasks` ledger
                // does not record it; the originating Unfulfillable
                // task's `TaskFailed` arm above is the only entry to
                // the local-fail-counter pipeline for the cascade
                // root. `PeerResourceHoldingsUpdated` is a generic
                // CRDT replication of opaque-string per-peer
                // holdings — orthogonal to task accounting.
            }
            ClusterMutation::TasksSpawned { .. } => {
                // Mirrors `TaskAdded`: a TasksSpawned batch grows the
                // ledger with brand-new entries. Setup-pending flips
                // off for the same reason TaskAdded does — the
                // batch is evidence the promoted secondary is
                // actively building out the task graph. Failed
                // (cascade-fail) and accounting-relevant entries are
                // mirrored in the post-apply walk in
                // `handle_cluster_mutation` (the
                // `mirror_tasks_spawned_post_apply` step there),
                // because at this point the apply hasn't yet run and
                // the per-task landing state isn't known.
                self.setup_pending = false;
            }
            ClusterMutation::PanikRequested { .. } => {
                // Operator-initiated emergency stop. The apply rule
                // transitions every non-terminal entry to
                // `TaskState::Cancelled` in one sweep; the per-pass
                // accounting mirror is updated in the post-apply walk
                // (`mirror_panik_post_apply` in
                // `handle_cluster_mutation`) where every affected
                // hash is known. At this point the apply hasn't yet
                // run, so we record nothing here — same shape as
                // `TasksSpawned` above. The post-apply walk inserts
                // every cancelled hash into the `failed_tasks`
                // accounting bucket (alongside its terminal counter)
                // so the operational-loop exit check sees the
                // run as fully accounted-for.
            }
        }
    }

    /// Post-apply accounting mirror for a `ClusterMutation::TasksSpawned`
    /// batch arriving from a remote originator. Each task in the
    /// batch lands in one of three relevant states (Pending,
    /// Blocked, Failed); we walk every input task and record the
    /// Failed entries in `failed_tasks` so the operational-loop's
    /// exit-counter check converges.
    ///
    /// Pending / Blocked entries contribute nothing here: they're
    /// non-terminal. The hash recomputation is the same wire-
    /// canonical primitive the apply rule uses, so the keys line up
    /// with `cluster_state.tasks`.
    pub(crate) fn mirror_tasks_spawned_post_apply(
        &mut self,
        tasks: &[TaskInfo<I>],
    ) {
        for task in tasks {
            let hash = crate::primary::wire::compute_task_hash(task);
            if let Some(crate::cluster_state::TaskState::Failed { kind, .. }) =
                self.cluster_state.task_state(&hash)
                && !self.completed_tasks.contains(&hash)
            {
                self.failed_tasks.insert(hash, kind.clone());
            }
        }
    }

}
