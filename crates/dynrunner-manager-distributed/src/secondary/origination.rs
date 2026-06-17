//! Originator-side cluster-mutation broadcast + panik self-departure.
//!
//! Single concern: originate cluster-state changes from THIS node —
//! produce a batch of `ClusterMutation`s, run the
//! `apply_locally_for_broadcast` apply-first filter, and fan the
//! `Applied` subset out to the mesh. `handle_panik_signal` originates the
//! self-departure announcement (file source) and tears down local
//! workers on the Phase-0B emergency-stop path.
//!
//! Relocated here from the removed `secondary/primary/*` mirror: these
//! functions are NOT mirror logic (they originate cluster state on the
//! live-secondary side and serve the pyo3 + panik surfaces), so they
//! survive the mirror demolition. The free pool helper
//! `cascade_drain_done` also relocates here (it was a free function in
//! the removed `secondary/primary/mod.rs`); the symmetric primary-side
//! hydration re-uses it.

use dynrunner_core::{BoundedString, Identifier};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, RemovalCause,
};
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator, Scheduler};

use super::SecondaryCoordinator;
use super::wire::timestamp_now;
use crate::cluster_state::apply_locally_for_broadcast;

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
// R4 SEAM: the only caller is the primary-side hydrate_from_cluster_state
#[allow(dead_code)] // TODO(R4): reached via hydrate_from_cluster_state (P4 composition)
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

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
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
        // The secondary originator (the panik self-departure announcement)
        // is never the authority, so neither the resumed-dispatch list nor
        // the `became_pending` affine-ready surface has a local consumer —
        // gate ready-resolution is the `PrimaryCoordinator`'s concern,
        // driven on the authority's pool when it applies the same mutation.
        // Discard both; the CRDT mirror is the only local effect.
        let crate::cluster_state::AppliedBatch { applied, .. } = batch;
        if applied.is_empty() {
            return Ok(());
        }
        let msg = DistributedMessage::ClusterMutation {
            target: None,
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            mutations: applied,
        };
        // ONE mesh broadcast — every mesh member (peers, the same-peer
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

    /// Announce this secondary's DELIBERATE graceful-abort drain departure:
    /// a self-authored `ClusterMutation::PeerRemoved { id: <self>, cause:
    /// SelfDeparture("graceful abort: local work drained") }`, applied
    /// locally and fanned out — the SAME graceful-leave path the
    /// file-source panik uses ([`Self::handle_panik_signal`] step 1), so
    /// peers LOG the departure and mark this node Dead-deliberately instead
    /// of the keepalive watchdog later declaring an unexplained death.
    /// Observability + membership only: it does NOT cancel cluster work,
    /// does NOT terminate the run on peers, and never touches the failover
    /// machinery (elections key on PRIMARY silence, which a departing
    /// worker-secondary never is — the co-resident-primary case is excluded
    /// by the caller's drain gate). On the primary the resulting
    /// lifecycle `Removed` event's respawn request is suppressed by the
    /// graceful-abort admission gate, so no replacement is spawned for a
    /// drain departure. Best-effort: a broadcast failure is logged and the
    /// local exit proceeds (mirroring the panik departure).
    pub(in crate::secondary) async fn announce_graceful_drain_departure(&mut self) {
        self.announce_self_departure("graceful abort: local work drained".to_string())
            .await;
    }

    /// Originate + fan out an AUTHORITATIVE self-departure carrying the
    /// supplied exit `reason`: a self-authored `ClusterMutation::PeerRemoved
    /// { id: <self>, cause: SelfDeparture(reason), member_gen: <self's
    /// current incarnation> }`, applied locally and broadcast to every
    /// known mesh endpoint via [`Self::apply_and_broadcast_mutations`].
    ///
    /// Single source of truth for "this node is leaving the mesh; mark it
    /// Dead-deliberately and stop dialing it". The graceful-drain departure,
    /// the panik file-source departure, and the setup-timeout abort all
    /// route through here — they differ ONLY in the reason string, so the
    /// `PeerRemoved` shape + member_gen stamp + apply-then-broadcast +
    /// best-effort error handling live in ONE place.
    ///
    /// Best-effort: a broadcast failure is logged with the reason and the
    /// caller proceeds with its exit anyway (the node is leaving regardless;
    /// peers' keepalive/setup watchdogs reap the membership entry as the
    /// fallback). Observability + membership only: it does NOT cancel
    /// cluster work, terminate the run on peers, or touch failover.
    pub(in crate::secondary) async fn announce_self_departure(&mut self, reason: String) {
        let mutation = ClusterMutation::PeerRemoved {
            id: self.config.secondary_id.clone(),
            cause: RemovalCause::SelfDeparture(BoundedString::from(reason.clone())),
            // Kills THIS node's current membership incarnation.
            member_gen: self.cluster_state.peer_member_gen(&self.config.secondary_id),
        };
        if let Err(e) = self.apply_and_broadcast_mutations(vec![mutation]).await {
            tracing::warn!(
                error = %e,
                reason = %reason,
                "self-departure apply+broadcast failed; exiting anyway \
                 (peers' keepalive/setup watchdogs reap the membership entry \
                 as the fallback)"
            );
        }
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
        sender_pid: Option<u32>,
        kill_grace: std::time::Duration,
    ) -> (std::path::PathBuf, String) {
        // Source dispatch on the watcher's documented predicate. The
        // canonical source-attributed reason is owned by `panik_watcher`:
        // a file-source reason is preserved verbatim ("panik file: <path>")
        // for downstream log-parser compatibility; a SIGTERM-source reason
        // NAMES the sender pid — carried verbatim into
        // `SecondaryTerminal::Panik.reason` and every downstream terminal
        // log — so the operator sees "the HOST killed this secondary"
        // instead of hunting an invalid-task monitor that never fired.
        let is_sigterm = crate::panik_watcher::is_sigterm_signal(&matched_path);
        let reason = crate::panik_watcher::panik_reason(&matched_path, sender_pid);
        if is_sigterm {
            // `sender_pid` is the load-bearing diagnostic: who sent the
            // SIGTERM. `Some(0)` = kernel-originated (OOM-killer);
            // `Some(pid)` names the sender (slurmstepd on SLURM
            // TIMEOUT/scancel, the wrapper/shutdown-manager, etc.);
            // `None` = sender capture was unavailable.
            tracing::error!(
                secondary = %self.config.secondary_id,
                matched_path = %matched_path.display(),
                sender_pid = ?sender_pid,
                "SIGTERM panik from pid={sender}; local-only worker teardown \
                 (no cluster broadcast — mesh remains free to re-elect)",
                sender = sender_pid
                    .map(|p| p.to_string())
                    .unwrap_or_else(|| "<unknown>".to_string()),
            );
        } else {
            tracing::error!(
                secondary = %self.config.secondary_id,
                matched_path = %matched_path.display(),
                "panik file observed; announcing self-departure and \
                 tearing down workers"
            );
            // Self-authored departure announcement: peers LOG it and
            // mark this node Dead (observability only). It does NOT
            // cancel cluster work or terminate the run on peers — the
            // mesh stays free to continue / re-elect. The shared helper
            // truncates the reason at the 1 KiB `SelfDeparture` cap, then
            // proceeds with the local worker teardown below even on a
            // broadcast failure.
            self.announce_self_departure(reason.clone()).await;
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

}
