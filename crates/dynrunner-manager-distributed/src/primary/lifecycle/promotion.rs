use std::collections::HashSet;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::PeerTransport;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::PrimaryCoordinator;
use crate::primary::command_channel::PrimaryCommand;

impl<Tr: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier>
    PrimaryCoordinator<Tr, S, E, I>
{
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
    /// Cancellation safety: `transport.recv_peer` is the cancel-safe
    /// unified inbound demux; `sleep_until` is one-shot cancel-safe per
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
                msg = self.transport.recv_peer() => {
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
    /// # Uniform primary announcement
    ///
    /// Every primary — bootstrap and promoted alike — originates
    /// `ClusterMutation::PrimaryChanged { new = self, epoch }` here, so
    /// `current_primary()` resolves to this host uniformly cluster-wide
    /// through the one peer mesh. The submitter is now a routable mesh
    /// peer (registered in every replica's `connections` under its
    /// host-id), so warming the `Role::Primary` cache to its id routes
    /// correctly — there is no longer a "sole authority" special case
    /// that suppresses the announce. Epoch is `primary_epoch() + 1`, so
    /// on failover the announce strictly supersedes the prior primary
    /// identity via epoch-LWW; the re-announce names the same holder the
    /// election winner's `PromotePrimary` already installed, so the
    /// primary-changed important-event hook (which fires only on a
    /// genuine holder transition) stays silent.
    ///
    /// `primary_id` is set to this node's own id for the heartbeat
    /// requeue path's "did the primary just die?" check — which can
    /// never match a secondary id, so the authority never self-clears
    /// the pointer.
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

        // Uniform primary announcement: originate `PrimaryChanged { new
        // = self }` so `current_primary()` resolves to this host
        // cluster-wide through the one mesh. This is THE single
        // bootstrap+failover convergence point at which this node asserts
        // primary authority; the announce warms the `Role::Primary` cache
        // to this now-routable mesh peer (replacing the old "sole
        // authority" suppression). `primary_epoch() + 1` strictly
        // supersedes the prior identity via epoch-LWW. The replicated
        // `PrimaryChanged` apply drives the primary-changed important
        // event (registered as a role-change hook at construction), so
        // the LLM-wake milestone is emitted uniformly on every primary
        // transition without a hand-written per-call-site log line.
        self.originate_primary_changed().await;

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
