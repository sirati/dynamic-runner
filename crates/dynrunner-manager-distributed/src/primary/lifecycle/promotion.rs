use std::collections::HashSet;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    PeerTransport, SecondaryTransport,
};
use dynrunner_scheduler_api::{
    ResourceEstimator, Scheduler,
};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::PrimaryCoordinator;
use crate::primary::command_channel::PrimaryCommand;



impl<T: SecondaryTransport<I>, P: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, P, S, E, I> {

    /// Block on every connected secondary reporting `MeshReady`
    /// before letting `promote_primary` fire. The 750µs gap
    /// between "all secondaries cert-exchanged" and the previous
    /// promotion call left the promoted secondary
    /// authoritative against a still-forming peer mesh — every
    /// pre-mesh-formation message went into the void for the
    /// 30s peer-dial budget. Closing the gap means waiting until
    /// each secondary has signalled its mesh has settled (mesh
    /// formed, watchdog elapsed, or no peers were expected for
    /// single-secondary).
    ///
    /// Bounded by `config.mesh_ready_timeout` (default 60s):
    /// stragglers past the deadline log a warning and the run
    /// proceeds anyway. A buggy secondary that never emits
    /// `MeshReady` must not be able to deadlock the entire
    /// dispatch pipeline; the post-promotion paths are all
    /// already failure-tolerant against an absent peer.
    ///
    /// Cancellation safety: `transport.recv` is the cancel-safe
    /// mpsc bridge; `sleep_until` is one-shot cancel-safe per
    /// tokio docs. The `select!` here mirrors the same shape
    /// `wait_for_connections` uses one phase up.
    pub(crate) async fn wait_for_mesh_ready(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) -> Result<(), String> {
        // The expected set is the live-secondaries set captured
        // AT this moment (post-quorum, post-cert-exchange). It is
        // not `config.num_secondaries` because the connect phase
        // may have dropped no-show secondaries on its own
        // timeout — we only wait for who's actually here.
        let expected: HashSet<String> = self.secondaries.keys().cloned().collect();
        if expected.is_empty() {
            tracing::debug!("no secondaries connected; skipping wait_for_mesh_ready");
            return Ok(());
        }

        // Fast path: messages may have already arrived before this
        // step ran (the welcome/cert-exchange/peer-info loop above
        // is event-driven and a fast secondary can emit MeshReady
        // before we enter the wait).
        if expected.is_subset(&self.mesh_ready_secondaries) {
            tracing::info!(
                count = expected.len(),
                "all secondaries reported MeshReady before wait step"
            );
            return Ok(());
        }

        let deadline = tokio::time::Instant::now() + self.config.mesh_ready_timeout;
        tracing::info!(
            expected = expected.len(),
            already_reported = self.mesh_ready_secondaries.len(),
            timeout_s = self.config.mesh_ready_timeout.as_secs_f64(),
            "waiting for peer-mesh formation across secondary fleet before \
             promoting primary"
        );

        // Pre-operational keepalive cadence. The bootstrap region
        // (`perform_initial_assignment` → here → `activate_local_primary`
        // → `operational_loop`) can outlast the secondary's
        // primary-silence deadline (`keepalive_interval *
        // keepalive_miss_threshold`) while waiting for the mesh to form,
        // yet the operational loop — the only place keepalives ticked —
        // hasn't started. Tick the SAME emitter here so liveness is
        // asserted across the whole pre-operational window. Same shape
        // as `operational_loop`'s `heartbeat_tick`: the immediate first
        // tick is skipped (secondaries may not have sent their own first
        // keepalive yet), the cadence is `keepalive_interval`.
        let mut heartbeat_tick = tokio::time::interval(self.config.keepalive_interval);
        heartbeat_tick.tick().await;

        loop {
            if expected.is_subset(&self.mesh_ready_secondaries) {
                tracing::info!(
                    count = expected.len(),
                    "all secondaries reported MeshReady; releasing PromotePrimary"
                );
                return Ok(());
            }

            tokio::select! {
                msg = self.transport.recv() => {
                    match msg {
                        // Pre-operational-loop site. See
                        // `wait_for_connections` for the matching
                        // rationale: thread `command_rx` through so an
                        // `on_phase_end` callback fired by a
                        // TaskComplete arriving during this wait can
                        // queue `SpawnTasks` and have it applied
                        // inline, refreshing `total_tasks` BEFORE
                        // `operational_loop`'s entry-time exit check
                        // sees the post-spawn ledger.
                        Some(m) => self.dispatch_message(m, command_rx).await?,
                        None => return Err("transport closed during wait_for_mesh_ready".into()),
                    }
                }
                _ = heartbeat_tick.tick() => {
                    // Reuse the heartbeat module's sole emitter so the
                    // pre-operational window asserts liveness on the
                    // same cadence the operational loop uses. No
                    // spawned task, no second send path — one keepalive
                    // origination point shared across the lifecycle.
                    self.broadcast_primary_keepalive().await;
                }
                _ = tokio::time::sleep_until(deadline) => {
                    let missing: Vec<String> = expected
                        .difference(&self.mesh_ready_secondaries)
                        .cloned()
                        .collect();
                    tracing::warn!(
                        missing = ?missing,
                        reported = self.mesh_ready_secondaries.len(),
                        expected = expected.len(),
                        timeout_s = self.config.mesh_ready_timeout.as_secs_f64(),
                        "mesh-ready timeout: some secondaries never reported \
                         MeshReady; proceeding with PromotePrimary anyway. The \
                         promoted secondary may briefly route into a \
                         partially-formed peer mesh until those secondaries \
                         finish (or fail) their dials."
                    );
                    return Ok(());
                }
            }
        }
    }

    /// Activate THIS node's co-located primary as the authoritative
    /// primary. The single composition mechanism both handoff sides
    /// converge on (the brief's `activate_local_primary`): bootstrap
    /// (the run pipeline reaches its operational loop) and failover (the
    /// election's terminal `Promoted` transition) both call this.
    ///
    /// # Why this does NOT broadcast a remote `PromotePrimary`
    ///
    /// In the unified model every node runs one `PrimaryCoordinator` +
    /// one `SecondaryCoordinator`; the authority is the node the
    /// secondaries already dialled (their `UnifiedSecondaryTransport`
    /// uplink). A secondary routes `Address::Role(Role::Primary)` to its
    /// uplink while its role cache is COLD — which is exactly "the
    /// original primary I dialled". Broadcasting `PromotePrimary { new =
    /// <some secondary id> }` here was the LEGACY submitter→secondary
    /// hand-off: it re-pointed every secondary's role cache at the named
    /// node, so the chosen secondary's own `Role::Primary` resolved to
    /// LOOPBACK — and with the secondary-internal primary mirror now
    /// demolished, that loopback had no primary to receive the
    /// secondary's own keepalives / completions / requests. They looped
    /// back and died, the authority saw the secondary go silent, and the
    /// fleet-dead timeout stranded the run. The composed authority IS the
    /// original primary, so no role re-point is needed: leaving every
    /// secondary's cache cold keeps `Role::Primary` routed to the uplink
    /// (this node).
    ///
    /// The genuine FAILOVER re-point — a *new* node taking over from a
    /// dead original primary — is owned by the election winner's
    /// authoritative `PrimaryChanged` apply (broadcast on the
    /// `PromotePrimary` it emits after winning, in
    /// `record_promotion_confirm`'s terminal action), which the
    /// surviving secondaries' role-change hook applies to re-point
    /// `Role::Primary` from the dead uplink to the winner's mesh peer.
    /// That is a distinct concern from this bootstrap activation.
    ///
    /// `primary_id` is set to this node's own id for the heartbeat
    /// requeue path's "did the primary just die?" check — which can
    /// never match a secondary id, so the standalone authority never
    /// self-clears the pointer.
    ///
    /// # Seeded resume (failover activation)
    ///
    /// Two activation shapes converge here:
    ///
    ///   * **Bootstrap**: `run_pipeline` already built the pool from the
    ///     `binaries` argument and set `total_tasks` before reaching this
    ///     call. Nothing to seed — the local pool IS the source of truth.
    ///   * **Failover resume**: a node whose co-located primary was
    ///     PARKED (it never ran `run_pipeline`'s pool-build) activates
    ///     after the election. Its pool is unseeded but the continuously-
    ///     replicated `cluster_state` already holds the full ledger. It
    ///     must rebuild its `PendingPool` + unified `in_flight` ledger +
    ///     `completed_tasks` from that CRDT (`hydrate_from_cluster_state`)
    ///     so dispatch resumes seeded, BYPASSING the connect / mesh-ready
    ///     handshake (it inherited a formed mesh). The discriminator is
    ///     "the CRDT holds tasks the local pool does not reflect"
    ///     (`total_tasks == 0 && cluster_state.task_count() > 0`): true
    ///     only on the parked-then-activated path, false for bootstrap
    ///     (where `total_tasks` was set from `binaries`) and for the
    ///     setup-defer authority (whose CRDT is still empty at activation
    ///     — discovery seeds it later via the feed).
    pub(crate) async fn activate_local_primary(&mut self) -> Result<(), String> {
        self.primary_id = Some(self.config.node_id.clone());

        // Seeded-resume hydration. See the doc above for the
        // discriminator's rationale; the bootstrap and setup-defer paths
        // both leave this false, so this is reached ONLY by a parked
        // co-located primary activating into an already-replicated
        // ledger.
        if self.total_tasks == 0 && self.cluster_state.task_count() > 0 {
            tracing::info!(
                node = %self.config.node_id,
                crdt_tasks = self.cluster_state.task_count(),
                "co-located primary activating on a replicated ledger; \
                 hydrating pool + in-flight ledger from cluster_state"
            );
            self.hydrate_from_cluster_state();
        }

        // Initial-setup-done / first-operational milestone (LLM-wake
        // important event): the single bootstrap+failover convergence
        // point at which this node asserts primary authority, reached
        // immediately before the operational loop. Emitted at the
        // importance target so the dual-sink surfaces it on stdio under
        // `--important-stdio-only`.
        tracing::info!(
            target: crate::primary::important_events::IMPORTANT_TARGET,
            node = %self.config.node_id,
            "co-located primary activated as sole authority; secondaries route \
             Role::Primary to their uplink (this node) — no remote PromotePrimary"
        );

        // Liveness spans authority, not the operational phase. This is
        // the single bootstrap+failover convergence point (the brief's
        // `activate_local_primary`), so emit one keepalive here the
        // moment authority is asserted — before `operational_loop`'s
        // `heartbeat_tick` is even constructed. Without this a just-
        // promoted/just-bootstrapped primary is silent over the
        // authority↔worker link until the first operational tick fires,
        // and a secondary that stopped seeing keepalives during the
        // handoff window would trip `primary_silent` and re-elect.
        // Reuses the heartbeat module's sole emitter (no spawned task,
        // no second send path) so there remains exactly one keepalive
        // origination point.
        self.broadcast_primary_keepalive().await;

        Ok(())
    }

}
