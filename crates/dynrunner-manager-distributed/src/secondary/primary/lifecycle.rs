//! Phase-lifecycle bookkeeping for the promoted-secondary's primary
//! pool.
//!
//! Single concern: drive `Drained` phases through the per-phase
//! retry-bucket cascade → `on_phase_end` → `mark_phase_done` →
//! newly-Active phases through `on_phase_start`, mirroring
//! `PrimaryCoordinator::process_phase_lifecycle` so a setup-promoted
//! secondary that owns the live pool fires the same retry semantics
//! and lifecycle hooks the demoted primary would have. The
//! fire-site is the only addition; the cascade-drain primitive
//! itself stays the free-function `cascade_drain_done`
//! (callback-silent, used by `populate_primary_from_cluster_state`
//! whose semantics must NOT refire `on_phase_end` for items that
//! completed pre-promotion).
//!
//! Module boundary:
//!   * Owns: the `Option<OnPhaseStart>` / `Option<OnPhaseEnd>`
//!     invocation semantics on `SecondaryCoordinator`, the
//!     per-phase counter bookkeeping (`primary_phase_completed`,
//!     `primary_phase_failed`, `primary_phase_started_emitted`),
//!     and the per-side candidate-build + kickstart steps that
//!     wrap the shared retry-bucket core.
//!   * Does NOT own: the pool primitives themselves
//!     (`poll_drain_transitions` / `mark_phase_done` /
//!     `drain_empty_active_phases` / `active_phases`) — those live in
//!     `dynrunner-scheduler-api`'s `PendingPool` and are invoked
//!     verbatim here. The retry-bucket core
//!     (`try_phase_retry_bucket_core`) lives in
//!     `primary/retry_bucket.rs` and is reused 1:1 with the live
//!     primary path.

use dynrunner_core::{Identifier, MessageReceiver, MessageSender, PhaseId, TaskInfo};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::retry_bucket::{
    try_phase_retry_bucket_core, BucketKind,
};
use crate::primary::PrimaryCommand;

use super::super::SecondaryCoordinator;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Fire `on_phase_start` for every phase the primary pool
    /// currently reports as `Active` that we haven't notified yet.
    /// Idempotent: re-running visits only newly-active phases.
    /// Mirrors `PrimaryCoordinator::fire_initial_phase_starts`.
    ///
    /// No-op when the pool is unset (pre-promotion or hydrate failure)
    /// or when no `on_phase_start` callback was registered; the
    /// `primary_phase_started_emitted` set still tracks observed
    /// phases either way so a later callback registration cannot
    /// double-fire for the same phase.
    pub(in crate::secondary) fn fire_primary_phase_starts(&mut self) {
        let Some(pool) = self.primary_pending.as_ref() else {
            return;
        };
        let active: Vec<PhaseId> = pool.active_phases();
        for p in active {
            if self.primary_phase_started_emitted.insert(p.clone())
                && let Some(cb) = self.on_phase_start.as_mut()
            {
                cb(&p);
            }
        }
    }

    /// Drive `Drained` phases through `on_phase_end` →
    /// `mark_phase_done` → newly-Active phases through
    /// `on_phase_start`. Called from
    /// `note_primary_item_completed`/`note_primary_item_failed` after
    /// the per-phase counters are bumped and `pool.on_item_finished`
    /// has run. Mirrors `PrimaryCoordinator::process_phase_lifecycle`
    /// 1:1 so the consumer-observable semantics are identical
    /// regardless of which node currently owns the primary pool.
    ///
    /// No-op when `primary_pending` is `None`. The callback `Option`
    /// guards each fire-site so a coordinator without a registered
    /// hook walks the cascade silently (preserving the pool's state
    /// machine transitions while skipping the user-callback work).
    ///
    /// `command_rx` carries the operational-loop's command-channel
    /// receiver (the `take`n local; see
    /// `secondary/processing/process_tasks.rs:122`). After each
    /// cascade iteration's `on_phase_end` fires, we drain any commands
    /// the user callback queued via the in-runtime `PrimaryHandle`
    /// path (e.g. `spawn_tasks(next_phase_items)`) and dispatch each
    /// through the existing `handle_secondary_command` chokepoint
    /// BEFORE the next `drain_empty_active_phases` poll. Mirrors the
    /// primary's drain step 1:1; same false-empty-successor bug,
    /// same fix shape.
    ///
    /// Pre-loop / off-loop callers (e.g. tests, or any
    /// `fail_permanent` path running outside `process_tasks`) pass
    /// `&mut None`.
    ///
    /// No setup-pending gate (deliberately): the primary-side mirror
    /// (`PrimaryCoordinator::process_phase_lifecycle`) early-returns
    /// while `setup_pending = true` because the demoted submitter
    /// enters `run()` with `total_tasks = 0` and the chosen secondary
    /// has not yet broadcast `TaskAdded` — firing
    /// `on_phase_end(.., 0, 0)` there would be spurious. The
    /// promoted-secondary path is structurally different: by the time
    /// `primary_pending` is `Some` (set inside
    /// `populate_primary_from_cluster_state` in `primary/hydrate.rs`),
    /// the setup-promoted secondary has ALREADY (a) run Python
    /// discovery on its bind-mounted source filesystem and
    /// (b) applied `PhaseDepsSet` + `N×TaskAdded` mutations to its
    /// own `cluster_state` via `ingest_setup_discovery`. The hydrate
    /// step reads those items out of `cluster_state` and `extend()`s
    /// them into the new pool, so `primary_pending`'s phases reflect
    /// the post-discovery item population from frame zero. A cascade
    /// firing here observes the legitimate phase state, not a
    /// pre-discovery transient. The non-promote-setup paths
    /// (pre-seeded bootstrap, failover-election) reach this point
    /// with `cluster_state` already populated from the live primary's
    /// broadcasts, so the same invariant holds — no transient
    /// empty-phase window exists for the secondary to gate against.
    /// Per-(phase, bucket) retry-bucket primitive for the
    /// promoted-secondary's primary path. Mirrors
    /// `PrimaryCoordinator::try_run_phase_retry_bucket` 1:1 by
    /// delegating to the shared core in
    /// `primary/retry_bucket.rs`. The two paths differ only in:
    ///
    ///   1. Candidate-build source — the secondary's
    ///      `primary_failed` ledger keys hashes to
    ///      `FailedTaskEntry { binary, error_type }` so each
    ///      entry already carries the `TaskInfo` needed for
    ///      reinjection. The live primary cross-references its
    ///      `all_binaries` snapshot against `failed_tasks` (which
    ///      stores `ErrorType` only).
    ///   2. Kickstart — the secondary calls `repoll_idle_workers`
    ///      against its OWN worker pool; peer secondaries
    ///      self-recover on their own keepalive tick via the same
    ///      method.
    ///
    /// Returns `true` iff at least one task was reinjected. On
    /// `true` the caller (`process_primary_phase_lifecycle`)
    /// skips `on_phase_end` + `mark_phase_done` for this phase
    /// because the pool has flipped `Drained → Active` again via
    /// `PendingPool::reinject`; the next `poll_drain_transitions`
    /// will revisit the phase only after the freshly-reinjected
    /// items terminate.
    ///
    /// `command_rx` is threaded for symmetry with the live-primary
    /// path; the current call path does not consume commands
    /// inside the bucket (no callback fires from inside the
    /// budget+reinject step), but the parameter keeps the
    /// signature aligned with future cross-cut paths (e.g. a
    /// `FailPermanent` re-entering via `apply_fail_permanent`
    /// would need the same dispatch chokepoint).
    async fn try_run_primary_phase_retry_bucket(
        &mut self,
        phase: &PhaseId,
        kind: BucketKind,
        _command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) -> bool {
        let Some(pool) = self.primary_pending.as_mut() else {
            return false;
        };
        // Walk `primary_failed` directly: each entry already
        // carries the `TaskInfo` (the secondary's mirror keeps the
        // binary on-entry; the live primary keeps a separate
        // snapshot). Filter by `(phase, kind)` exactly the way the
        // live primary does. The ledger key here is the wire
        // `file_hash` — identical to `compute_task_hash(binary)`
        // in production (the worker and primary share the same
        // recipe; see `primary/wire.rs::compute_task_hash` and
        // `secondary/primary/mod.rs::task_file_hash`).
        let candidates: Vec<TaskInfo<I>> = self
            .primary_failed
            .iter()
            .filter(|(_, e)| e.binary.phase_id == *phase)
            .filter(|(_, e)| kind.matches(&e.error_type))
            .map(|(_, e)| e.binary.clone())
            .collect();
        let cap = kind.max_passes_secondary(&self.config);
        let primary_failed = &mut self.primary_failed;
        try_phase_retry_bucket_core(
            phase,
            kind,
            candidates,
            pool,
            &mut self.primary_retry_passes_used,
            cap,
            |h| {
                primary_failed.remove(h);
            },
        )
    }

    pub(in crate::secondary) async fn process_primary_phase_lifecycle(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        loop {
            let drained: Vec<PhaseId> = match self.primary_pending.as_mut() {
                Some(pool) => pool.poll_drain_transitions(),
                None => return,
            };
            if drained.is_empty() {
                break;
            }
            for p in &drained {
                // Per-phase retry-bucket cascade — runs BEFORE
                // `on_phase_end` so phase B (which depends on A)
                // doesn't activate until phase A's retry buckets
                // are exhausted. Mirrors the live-primary's
                // `process_phase_lifecycle` 1:1 — see
                // `crate::primary::retry_bucket` for the partition
                // and counter semantics.
                //
                // Recoverable bucket first; OOM bucket second.
                // Same ordering rationale as the live primary:
                // Recoverable retries that succeed leave nothing
                // for the OOM bucket to find; OOM running after
                // Recoverable settles keeps any future
                // single-worker / memory-DESC dispatch modifier
                // scoped to actually-over-budget tasks.
                //
                // On `true`: the bucket reinjected at least one
                // task; the phase has flipped Drained → Active
                // and the next iteration's `poll_drain_transitions`
                // will not return it until the new items terminate.
                // Skip `on_phase_end` and `mark_phase_done` for
                // this phase on this iteration.
                if self
                    .try_run_primary_phase_retry_bucket(
                        p,
                        BucketKind::Recoverable,
                        command_rx,
                    )
                    .await
                {
                    continue;
                }
                if self
                    .try_run_primary_phase_retry_bucket(
                        p,
                        BucketKind::Oom,
                        command_rx,
                    )
                    .await
                {
                    continue;
                }
                let completed = self
                    .primary_phase_completed
                    .get(p)
                    .copied()
                    .unwrap_or(0);
                let failed = self.primary_phase_failed.get(p).copied().unwrap_or(0);
                if let Some(cb) = self.on_phase_end.as_mut() {
                    cb(p, completed, failed);
                }
                // Drain queued commands one-at-a-time so each
                // `try_recv` borrow releases before the dispatch
                // re-borrows `command_rx` (the recursive cascade
                // fired by e.g. `apply_fail_permanent` needs
                // `&mut command_rx` to drain its own post-callback
                // queue). `Box::pin` breaks the async-recursion cycle
                // (cascade → handle_secondary_command →
                // apply_fail_permanent → note_primary_item_failed →
                // cascade). See primary-side mirror in
                // `primary/coordinator.rs::process_phase_lifecycle`
                // for the load-bearing-property rationale.
                loop {
                    let cmd = match command_rx.as_mut() {
                        Some(rx) => rx.try_recv().ok(),
                        None => None,
                    };
                    let Some(cmd) = cmd else { break };
                    Box::pin(
                        crate::secondary::command_channel::handle_secondary_command(
                            self, cmd, command_rx,
                        ),
                    )
                    .await;
                }
                if let Some(pool) = self.primary_pending.as_mut() {
                    pool.mark_phase_done(p);
                }
            }
            // mark_phase_done may have flipped Blocked → Active for
            // dependents; emit on_phase_start for them.
            self.fire_primary_phase_starts();
            // Newly-Active dependents may themselves be empty (a phase
            // chain like 0→1→2→3 with all items in phase 3 cascades
            // through this branch on every iteration). Re-drain so the
            // next poll_drain_transitions catches them and the loop
            // continues; without this the cascade stops one phase
            // short and items in the final phase never dispatch.
            if let Some(pool) = self.primary_pending.as_mut() {
                pool.drain_empty_active_phases();
            }
        }
    }
}
