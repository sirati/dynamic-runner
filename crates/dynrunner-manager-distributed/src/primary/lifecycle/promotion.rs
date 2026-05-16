use std::collections::HashSet;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PeerTransport,
    SecondaryTransport,
};
use dynrunner_scheduler_api::{
    ResourceEstimator, Scheduler,
};

use crate::primary::PrimaryCoordinator;
use crate::primary::wire::timestamp_now;



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
    pub(crate) async fn wait_for_mesh_ready(&mut self) -> Result<(), String> {
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
                        Some(m) => self.dispatch_message(m).await?,
                        None => return Err("transport closed during wait_for_mesh_ready".into()),
                    }
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

    pub(crate) async fn promote_primary(&mut self) -> Result<(), String> {
        if let Some(first_id) = self.secondaries.keys().next().cloned() {
            self.primary_id = Some(first_id.clone());
            // Monotonic per-promotion epoch carried on the wire and
            // fed into `ClusterState::PrimaryChanged`'s last-writer-
            // wins resolver. Starting from the local mirror's current
            // epoch + 1 is sufficient at the bootstrap promotion
            // (epoch starts at 0 cluster-wide); the failover election
            // protocol's own `round` will supersede this when it
            // re-elects.
            let new_epoch = self.cluster_state.primary_epoch() + 1;
            tracing::info!(primary = %first_id, epoch = new_epoch, "promoting secondary to primary");

            // Apply locally so the originator's mirror flips atomically
            // with the broadcast, not after the broadcast round-trips
            // back to us. Lower-epoch races no-op against the higher
            // epoch we just installed.
            self.cluster_state.apply(ClusterMutation::PrimaryChanged {
                new: first_id.clone(),
                epoch: new_epoch,
            });

            let msg = DistributedMessage::<I>::PromotePrimary {
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                new_primary_id: first_id.clone(),
                epoch: new_epoch,
                // Bootstrap-promote discriminator: when this primary
                // skipped `seed_cluster_state` + `perform_initial_assignment`
                // (setup-defer mode driven by `--source-already-staged`,
                // i.e. `required_setup_on_promote = true`), the chosen
                // secondary needs to know it's the one doing discovery
                // + ledger seed after promotion. The election/failover
                // sites in `secondary/election.rs` unconditionally
                // pass `false` because, by election time, the local
                // ledger is already non-empty (seeded either by the
                // original setup-defer secondary or by a pre-seeded
                // submitter — `required_setup_on_promote = false`,
                // a fully production-supported path), so re-running
                // discovery would double-seed.
                required_setup: self.config.required_setup_on_promote,
            };
            // Broadcast to every secondary, not unicast to the elected
            // node: every secondary needs the role-change to update
            // its `primary_link` routing target and clear its per-
            // worker backoff so idle workers re-issue at the new
            // primary on their next tick (otherwise the new primary's
            // peers stay quiet for one stale-window). The pre-Phase-P
            // unicast was Bug 2 in the trace at `feb1052` — only the
            // elected node logged the role change because only it
            // received the message.
            if let Err(failures) = self.transport.broadcast(msg).await {
                for (secondary_id, error) in &failures {
                    tracing::warn!(
                        secondary = %secondary_id,
                        error = %error,
                        "PromotePrimary broadcast delivery failed"
                    );
                }
            }

            // Hand-off complete: the local primary stops being
            // authoritative the moment `PromotePrimary` is on the
            // wire. We stay alive (transport open, message loop
            // still runs) so completion forwards keep
            // `completed_tasks` accurate for the run-done counter
            // check in `operational_loop`, but we no longer
            // dispatch, kickstart, or drive heartbeat-based
            // requeue — the promoted secondary owns all of that.
            // Without this, the local primary and the promoted
            // secondary both act as primaries simultaneously and
            // their parallel dispatch paths race for the same
            // workers. See `demoted` doc on `PrimaryCoordinator`.
            self.demoted = true;
            // The demoted local primary is NOT an observer — observers
            // are first-class members of `RoleTable.observers` (Step 7,
            // Decision G) with `is_observer=true` set at startup. A
            // demoted primary stays a regular member; it just no
            // longer drives dispatch. Prior log wording conflated the
            // two concepts.
            tracing::info!(
                primary = %first_id,
                "local primary demoted; promoted secondary is sole authoritative primary"
            );
        }
        Ok(())
    }

}
