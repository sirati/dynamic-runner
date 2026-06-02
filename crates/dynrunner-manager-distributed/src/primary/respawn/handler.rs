//! Operational-loop arms for the respawn pipeline. The impl block here
//! decorates `PrimaryCoordinator` with `dispatch_respawn_request` (called
//! on the respawn-request select arm) and `handle_respawn_join` (called
//! when the `JoinSet<RespawnOutcome>` yields a finished task).

use std::sync::Arc;

use super::types::{
    push_event, RespawnDecision, RespawnEvent, RespawnOutcome, RespawnRequest,
    SecondarySpawnSpec,
};


// ── Operational-loop entry points ─────────────────────────────────
//
// Single concern: the coordinator-side handlers the operational
// `select!` arms delegate to. The arms themselves live in
// `primary::lifecycle::operational_loop`; this `impl` block is the
// only place that mutates `PrimaryCoordinator`'s respawn fields
// from inside the loop. Keeping the bodies here (rather than inline
// in `lifecycle.rs`) co-locates them with the budget logic / the
// types they consume — a future maintainer reading respawn.rs sees
// the end-to-end pipeline without cross-file hopping.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

impl<Tr, S, E, I> crate::primary::PrimaryCoordinator<Tr, S, E, I>
where
    Tr: PeerTransport<I>,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    /// Handle one [`RespawnRequest`] drained off the respawn-request
    /// channel. Consults the budget against the live `respawn_events`
    /// ring, mints a fresh secondary id on accept, builds the
    /// [`SecondarySpawnSpec`], and spawns the future onto
    /// `respawn_tasks`. Rejections emit the
    /// `respawn_budget_exhausted` structured log event and a
    /// budget-rejection record on the ring so downstream forensics
    /// can see why a death didn't lead to a respawn.
    pub(crate) fn dispatch_respawn_request(&mut self, request: RespawnRequest) {
        let (spawner, budget) = match (
            self.respawn_spawner.as_ref(),
            self.respawn_budget.as_ref(),
        ) {
            (Some(s), Some(b)) => (Arc::clone(s), b.clone()),
            // Defensive: the listener is only registered when
            // `enable_respawn` was called, which always installs
            // both fields. If we reach here without them, the
            // policy is disabled and the request is a no-op.
            _ => {
                tracing::warn!(
                    target: "dynrunner_respawn",
                    peer_id = %request.original_id,
                    "respawn request received but policy is disabled; dropping",
                );
                return;
            }
        };

        let now = std::time::SystemTime::now();
        let decision = budget.should_respawn(
            &request.original_id,
            &self.respawn_events,
            now,
        );
        match decision {
            RespawnDecision::Accept => {}
            RespawnDecision::RejectFamilyBudget
            | RespawnDecision::RejectTotalBudget
            | RespawnDecision::RejectCooldown => {
                tracing::warn!(
                    target: "dynrunner_respawn",
                    peer_id = %request.original_id,
                    cause = ?request.cause,
                    decision = ?decision,
                    max_per_secondary = budget.max_per_secondary,
                    max_total = budget.max_total,
                    cooldown_s = budget.cooldown.as_secs_f64(),
                    event = "respawn_budget_exhausted",
                    "respawn budget rejected request; not spawning replacement",
                );
                return;
            }
        }

        let new_id = self.mint_secondary_id();
        let spec = SecondarySpawnSpec {
            new_secondary_id: new_id.clone(),
            primary_endpoint: self.respawn_primary_endpoint.clone(),
            primary_pubkey_pem: self.respawn_primary_pubkey_pem.clone(),
        };

        // Record the attempt on the ring NOW — before the spawn
        // future resolves — so budget consultation for any
        // immediately-following request in the same `select!` tick
        // already sees this entry. Without this, a tight burst of
        // peer deaths could each independently consult an empty
        // ring and all pass the cap.
        push_event(
            &mut self.respawn_events,
            RespawnEvent {
                original_id: request.original_id.clone(),
                new_id: new_id.clone(),
                cause: request.cause.clone(),
                at: now,
            },
        );
        tracing::info!(
            target: "dynrunner_respawn",
            original_id = %request.original_id,
            new_id = %new_id,
            cause = ?request.cause,
            event = "respawn_attempted",
            "spawning replacement secondary",
        );

        let original_id = request.original_id;
        let cause = request.cause;
        self.respawn_tasks.spawn_local(async move {
            let result = spawner.spawn(spec).await.map_err(|e| e.to_string());
            RespawnOutcome {
                original_id,
                new_id,
                cause,
                result,
            }
        });
    }

    /// Handle one completed (or join-cancelled) entry off the
    /// `respawn_tasks` JoinSet. Logs structured `respawn_succeeded`
    /// / `respawn_failed` / `respawn_join_failed` events; ring-buffer
    /// bookkeeping happens at dispatch time (see
    /// [`Self::dispatch_respawn_request`]) so a successful spawn that
    /// races against a join failure still leaves the family-count
    /// invariant intact.
    pub(crate) fn handle_respawn_join(
        &mut self,
        outcome: Option<Result<RespawnOutcome, tokio::task::JoinError>>,
    ) {
        match outcome {
            None => {
                // Empty JoinSet — the `select!` arm parks on
                // `pending()` while empty, so this branch is only
                // reachable through a race window (an `abort()`
                // between the empty check and the poll). No-op.
            }
            Some(Ok(outcome)) => match &outcome.result {
                Ok(()) => {
                    tracing::info!(
                        target: "dynrunner_respawn",
                        original_id = %outcome.original_id,
                        new_id = %outcome.new_id,
                        cause = ?outcome.cause,
                        event = "respawn_succeeded",
                        "spawner completed; awaiting PeerJoined for new secondary",
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        target: "dynrunner_respawn",
                        original_id = %outcome.original_id,
                        new_id = %outcome.new_id,
                        cause = ?outcome.cause,
                        error = %err,
                        event = "respawn_failed",
                        "spawner returned an error; replacement not running",
                    );
                }
            },
            Some(Err(join_err)) => {
                tracing::warn!(
                    target: "dynrunner_respawn",
                    error = %join_err,
                    event = "respawn_join_failed",
                    "respawn task panicked or was aborted; spawn outcome unknown",
                );
            }
        }
    }

    /// Drain in-flight respawn tasks at operational-loop shutdown.
    /// Aborts every outstanding future via [`JoinSet::shutdown`],
    /// then logs a structured summary so operators can see whether
    /// any respawn was in flight when the run ended. Any
    /// already-started spawn that did not complete is logged
    /// as a possible orphan — for SLURM mode this is where a
    /// follow-on `scancel` would belong; today we log loudly.
    pub(crate) async fn drain_respawn_tasks(&mut self) {
        if self.respawn_tasks.is_empty() {
            return;
        }
        let in_flight = self.respawn_tasks.len();
        tracing::info!(
            target: "dynrunner_respawn",
            in_flight,
            event = "respawn_drain_starting",
            "draining outstanding respawn tasks at shutdown",
        );
        self.respawn_tasks.shutdown().await;
        tracing::warn!(
            target: "dynrunner_respawn",
            aborted = in_flight,
            event = "respawn_drain_complete",
            "respawn tasks aborted; any successfully-spawned-but-unregistered \
             secondary may require manual scancel/cleanup",
        );
    }
}
