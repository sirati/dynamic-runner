//! Originator-side cluster-mutation broadcast, panik self-departure,
//! and setup-discovery ingest.
//!
//! Single concern: originate cluster-state changes from THIS node —
//! produce a batch of `ClusterMutation`s, run the
//! `apply_locally_for_broadcast` apply-first filter, and fan the
//! `Applied` subset out to the mesh. `ingest_setup_discovery` is the
//! wrapper entry-point that turns the wrapper-driven discovery result
//! into the canonical `PhaseDepsSet + N×TaskAdded` batch through the
//! same broadcast helper; `handle_panik_signal` originates the
//! self-departure announcement (file source) and tears down local
//! workers on the Phase-0B emergency-stop path.
//!
//! Relocated here from the removed `secondary/primary/*` mirror: these
//! three functions are NOT mirror logic (they originate cluster state
//! on the live-secondary side and serve the pyo3 + panik surfaces), so
//! they survive the mirror demolition. The two free pool helpers
//! `task_file_hash` and `cascade_drain_done` also relocate here (they
//! were free functions in the removed `secondary/primary/mod.rs`); the
//! symmetric primary-side hydration re-uses `cascade_drain_done`.

use std::collections::HashMap;

use dynrunner_core::{BoundedString, Identifier, PhaseId, TaskInfo};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, PeerTransport, RemovalCause,
};
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator, Scheduler};

use super::SecondaryCoordinator;
use super::wire::timestamp_now;
use crate::cluster_state::apply_locally_for_broadcast;

/// Stable hash of a `TaskInfo`'s path+identifier, matching the wire
/// `file_hash` shape used elsewhere in the secondary. Pulled out as a
/// free function so the originating paths agree on the key space
/// without duplicating the hashing recipe.
///
/// Relocated faithfully from the removed `secondary/primary/mod.rs`.
pub(super) fn task_file_hash<I: Identifier>(item: &TaskInfo<I>) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    item.path.hash(&mut h);
    item.identifier.hash(&mut h);
    format!("{:016x}", h.finish())
}

/// Run the phase-lifecycle drain cascade on a pool until quiescent.
/// Each iteration:
///   1. `drain_empty_active_phases` — moves any Active phase whose
///      `(queued, in_flight) == (0, 0)` to Drained, queues it for
///      `poll_drain_transitions`.
///   2. `poll_drain_transitions` — returns and clears the
///      drained-pending list.
///   3. `mark_phase_done` — flips Drained → Done, may unblock
///      dependent phases (Blocked → Active).
///
/// The loop terminates when no new drains surface (the next
/// `drain_empty_active_phases` finds nothing to transition AND
/// `poll_drain_transitions` returns empty).
///
/// `pub(crate)` so the symmetric primary-side hydration
/// (`crate::primary::hydrate`) reuses the identical drain cascade
/// rather than re-deriving the loop — single source of truth for
/// "drain a freshly-seeded pool to quiescence".
///
/// Relocated faithfully from the removed `secondary/primary/mod.rs`.
/// The sole caller is the primary-side `hydrate_from_cluster_state`,
/// reached from the composed primary's seeded resume (failover
/// activation).
pub(crate) fn cascade_drain_done<I: Identifier>(pool: &mut PendingPool<I>) {
    loop {
        pool.drain_empty_active_phases();
        let drained = pool.poll_drain_transitions();
        if drained.is_empty() {
            break;
        }
        for p in &drained {
            pool.mark_phase_done(p);
        }
    }
}

impl<Tr, M, S, E, I> SecondaryCoordinator<Tr, M, S, E, I>
where
    Tr: PeerTransport<I>,
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
    /// applied subset fans out to the mesh.
    ///
    /// Fan-out shape: ONE `send_to(Destination::All)` mesh broadcast
    /// reaches every mesh member. This node IS the authority here
    /// (promoted-secondary originating its own mutations), so a single
    /// mesh broadcast is the authoritative propagation — the demoted
    /// node (and any observer) receives it because it is itself a mesh
    /// member. There is no separate demoted-submitter uplink leg: the
    /// old dual fan-out (`primary_transport.send` + `peer_transport.broadcast`)
    /// was the pre-unification shape where the demoted submitter sat on
    /// a non-peer channel; the unified mesh now carries it.
    ///
    /// Errors are best-effort: a broadcast failure surfaces as a single
    /// error string (the trait's contract) and is logged, not
    /// propagated. CRDT idempotency makes a missed mutation recoverable
    /// from the next snapshot RPC; we never block the originator on
    /// universal delivery.
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
        // Blocked dependents. The secondary holds NO dispatch pool —
        // it is never the authority — so the resumed list has no local
        // consumer: dispatch of resumed dependents is the
        // `PrimaryCoordinator`'s concern, driven on the authority's own
        // pool when it applies the same mutation. We deliberately
        // discard the list here; the CRDT mirror is the only local
        // effect.
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
        // ONE mesh broadcast — every mesh member (peers, the co-located
        // authority, any observer) receives it. Errors are logged, not
        // propagated (see method doc).
        if let Err(e) = self.send_to(Destination::All, msg).await {
            tracing::warn!(
                error = %e,
                "ClusterMutation mesh broadcast failed"
            );
        }
        Ok(())
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
    ///   3. Surface the matched path + reason to the caller, which records
    ///      the [`crate::secondary::SecondaryTerminal::Panik`] lifecycle
    ///      terminal (this node's own local exit).
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
        // unconditional. The pool lives in `Configuring`/`Operational`;
        // a panik before the pool was spawned (no `Configuring` reached)
        // has no workers to kill — `pool_mut()` is `None` there.
        if let Some(pool) = self.lifecycle.pool_mut() {
            pool.kill_all_workers_with_grace(kill_grace).await;
        }
        (matched_path, reason)
    }

    /// Ingest the result of Python's `task.discover_items` after a
    /// setup-defer promotion (the chosen peer received an
    /// `InitialAssignment { pre_staged_mode: true }` and yielded
    /// `SetupPending`).
    ///
    /// The outer process-tasks loop yielded back to the PyO3 wrapper,
    /// which ran `task.discover_items` against the locally bind-mounted
    /// staged source filesystem and is now feeding the result back into
    /// the Rust core.
    ///
    /// Sequence:
    ///   1. Build the mutation batch: one `PhaseDepsSet` carrying the
    ///      task graph's static phase dependency map (so every
    ///      receiver's `cluster_state.phase_deps()` is populated
    ///      before any post-promotion hydration consults it), then one
    ///      `TaskAdded` per discovered binary.
    ///   2. Originate the batch via `apply_and_broadcast_mutations` —
    ///      applies locally to `cluster_state` and fans out to the mesh
    ///      (every member, including the co-located authority, receives
    ///      it). This is legitimate originator-side cluster-state
    ///      production from the node that ran discovery; the secondary
    ///      is the producer of the discovery result, not an authority
    ///      over dispatch.
    ///
    /// Idempotency: `ClusterMutation::TaskAdded` is no-op-on-duplicate
    /// (the CRDT silently drops it via the `apply` filter), and
    /// `apply_locally_for_broadcast` filters NoOp mutations out before
    /// broadcast, so a duplicate `ingest_setup_discovery` call (e.g.
    /// from a wrapper retry on transport hiccup) doesn't re-broadcast
    /// the same batch.
    ///
    /// Feed to the composed authoritative primary: the `TaskAdded`
    /// broadcast reaches the co-located primary as any mesh member would
    /// receive it (the discovering node and the authority are both mesh
    /// members). The primary's `handle_cluster_mutation` applies the
    /// batch to its `cluster_state`, refreshes `total_tasks` from the
    /// now-populated ledger, and the CRDT-derived `setup_pending()` gate
    /// flips false — re-enabling the run-complete exits the gate had
    /// suppressed. No separate loopback hydration call is needed: the
    /// mesh broadcast IS the feed (the pre-demolition `setup_pending =
    /// false` + `populate_primary_from_cluster_state()` steps lived on
    /// the secondary's deleted authority mirror; the composed primary
    /// reaches the same state reactively off the replicated ledger).
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
        // Latch the one-shot so `setup_discovery_pending()` (the
        // `process_tasks` yield discriminator) never fires again on this
        // node — set unconditionally so the empty-discovery path (which
        // leaves the ledger empty) does not re-yield on re-entry. The
        // latch now lives in `OperationalState`: `ingest_setup_discovery`
        // is called by the wrapper AFTER `process_tasks` yielded
        // `SetupPending`, which only happens from the operational loop, so
        // the lifecycle is `Operational` here. See the
        // `OperationalState::setup_discovery_done` doc.
        self.op_mut().setup_discovery_done = true;
        tracing::info!(
            tasks = task_count,
            "ingested setup-discovery; broadcast PhaseDepsSet + TaskAdded batch"
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
            tracing::info!("empty-discovery: RunComplete broadcast — no tasks to run");
        }
        Ok(())
    }
}
