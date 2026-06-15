//! Operational-loop arms for the respawn pipeline. The impl block here
//! decorates `PrimaryCoordinator` with `dispatch_respawn_lifecycle`
//! (called on the respawn lifecycle select arm; routes `Removed` into
//! `dispatch_respawn_request`; `Added` events are dropped here as a
//! no-op — see the doc on `dispatch_respawn_lifecycle` for why) and
//! `handle_respawn_join` (called when the `JoinSet<RespawnOutcome>`
//! yields a finished task).

use std::sync::Arc;

use super::types::{RespawnDecision, RespawnOutcome, RespawnRequest, SecondarySpawnSpec};

use crate::cluster_state::RespawnEventRecord;
use crate::peer_lifecycle::PeerLifecycleEvent;

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
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

impl<S, E, I> crate::primary::PrimaryCoordinator<S, E, I>
where
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    /// Route one [`PeerLifecycleEvent`] drained off the respawn
    /// lifecycle channel.
    ///
    /// Only `Removed` is acted on: each removal is translated into a
    /// [`RespawnRequest`] and dispatched. The historical `Added`
    /// reconciliation (re-admitted-original → revoke-still-pending
    /// replacement) has been removed in favour of the slurm-authoritative
    /// quantity gate in [`Self::dispatch_respawn_request`]: that gate
    /// refuses respawn whenever slurm itself reports the fleet at-or-above
    /// initial count, so the redundant-replacement scenario is unreachable
    /// to begin with. Over-allocation that slips through (a small race
    /// between the local death-declaration and the next probe) is
    /// structurally tolerated — at-least-once execution is the precedent
    /// (`feedback_at_least_once_execution_deliberate`). The listener
    /// continues to drop `Added` events for the respawn arm; if one ever
    /// arrives here it is a no-op.
    pub(crate) fn dispatch_respawn_lifecycle(&mut self, event: PeerLifecycleEvent) {
        if let PeerLifecycleEvent::Removed { id, cause } = event {
            self.dispatch_respawn_request(RespawnRequest {
                original_id: id,
                cause,
            });
        }
    }

    /// Handle one [`RespawnRequest`] drained off the respawn-request
    /// channel. Consults the budget against the REPLICATED respawn ledger
    /// (`cluster_state.respawn_events()`), mints a fresh secondary id on
    /// accept, builds the [`SecondarySpawnSpec`], and spawns the future
    /// onto `respawn_tasks`. Rejections emit the `respawn_budget_exhausted`
    /// structured log event but record NOTHING on the ledger (the ledger
    /// holds accepted events only — the budget consults `len()` for the
    /// total cap, so a rejection must not inflate it). A disabled policy
    /// (`respawn_budget == None`) early-returns BEFORE any ledger write, so
    /// the replicated set is never touched when respawn is off.
    pub(crate) fn dispatch_respawn_request(&mut self, request: RespawnRequest) {
        // Deliberate-self-departure admission gate: a `SelfDeparture` is a
        // node leaving the mesh ON PURPOSE (a panik-file teardown, a
        // graceful-abort drain exit, or a #467 per-peer wind-down) — it is
        // NEVER an unexpected death, so it must never be "replaced" by a
        // respawn. Checked FIRST (before the fleet-wide graceful-abort gate
        // and the budget) because the cause alone settles it regardless of
        // any other cluster state: the per-peer wind-down departure carries
        // NO global freeze (the rest of the run continues), so the
        // graceful-abort gate below would NOT catch it, and the
        // re-admitted-original `is_peer_alive` gate is about a DIFFERENT id
        // (the original, not the departing replacement). Without this guard
        // a #467 wind-down would self-defeat: wind down → respawn →
        // re-seat → wind down. A cause-based suppression is the general,
        // correct rule and subsumes the graceful-abort gate's purpose for
        // self-departures.
        if matches!(
            request.cause,
            dynrunner_protocol_primary_secondary::RemovalCause::SelfDeparture(_)
        ) {
            tracing::info!(
                target: "dynrunner_respawn",
                peer_id = %request.original_id,
                cause = ?request.cause,
                event = "respawn_suppressed_self_departure",
                "secondary departed deliberately (self-departure); not \
                 spawning a replacement (a deliberate departure is never an \
                 unexpected death — covers panik teardown, graceful-abort \
                 drain, and per-peer wind-down)",
            );
            return;
        }
        // Graceful-abort admission gate: under the replicated
        // `graceful_abort_requested` freeze the fleet is draining DOWN by
        // design — every secondary departure (the drain self-departures
        // especially) is deliberate, so no replacement may ever be spawned.
        // Checked BEFORE the budget so a drain departure never consumes
        // ledger budget either. A primary decision consuming the CRDT fact,
        // sibling to the dispatch-view freeze.
        if self.cluster_state.graceful_abort_requested() {
            tracing::info!(
                target: "dynrunner_respawn",
                peer_id = %request.original_id,
                cause = ?request.cause,
                event = "respawn_suppressed_graceful_abort",
                "graceful abort active; not spawning a replacement for a \
                 departing secondary (the fleet is draining down)",
            );
            return;
        }
        // Re-admission heal gate: the removal that enqueued this request
        // may have been FALSE — the frame-ingest re-admission seam flips
        // a removed-but-provably-alive member back to `Alive` (at the
        // next membership generation) while the request still sits queued
        // on the channel. A replacement for a peer that is alive again is
        // pure waste (and a budget spend on a non-death), so the queued
        // stage is canceled HERE, at the dispatch decision point — the
        // single place a queued request becomes a spawn. Checked BEFORE
        // the budget consult, so a canceled request never writes the
        // replicated ledger (the budget "refund" is structural: the spend
        // only happens on accept, below). The LAUNCHED stage has its own
        // counterpart: an accepted respawn comes up under a freshly-
        // minted `secondary-N` id (never the dead peer's), so it cannot
        // duplicate the re-admitted identity. Over-allocation that
        // slips through (a small race between the local death-declaration
        // and the next authoritative probe) is structurally tolerated —
        // see `feedback_at_least_once_execution_deliberate`.
        if self.cluster_state.is_peer_alive(&request.original_id) {
            tracing::info!(
                target: "dynrunner_respawn",
                peer_id = %request.original_id,
                cause = ?request.cause,
                event = "respawn_canceled_readmitted",
                "peer was re-admitted (alive again) after the removal that \
                 requested this respawn; canceling the queued replacement \
                 (no budget consumed)",
            );
            return;
        }
        // SLURM-AUTHORITATIVE QUANTITY GATE (#543, rule 2):
        // "if we are respawning only do so if there are less slurm jobs
        //  active/queued than when started".
        //
        // The local view of "X is dead" can be false-positive when the
        // coordinator loop wedges and heartbeats appear silent. The
        // slurm-authoritative snapshot is the tiebreak: only fire respawn
        // when slurm itself agrees the fleet is below initial count.
        //
        // Fail-closed on Unknown (no probe answer / stale snapshot): refuse
        // to fire. Without authoritative evidence, the conservative
        // direction is don't-spawn.
        //
        // THE LAG IS A FEATURE: when 10 secondaries are declared dead and
        // 10 respawn requests arrive in the same tick, the snapshot still
        // shows their jobs Alive (probe hasn't re-run). Every dispatch
        // sees count==initial, refuses to fire. Only after slurm confirms
        // Gone does respawn fire. This is the structural mechanism by
        // which local-deafness false-deaths produce no respawn cascade.
        // DO NOT remove the lag.
        let initial_count = self.config.num_secondaries as usize;
        match self.authority_snapshot.secondary_active_or_queued_count() {
            Some(current) if current >= initial_count => {
                tracing::warn!(
                    target: "dynrunner_respawn",
                    peer_id = %request.original_id,
                    cause = ?request.cause,
                    initial_count,
                    current,
                    event = "respawn_suppressed_quantity_gate",
                    "slurm-authoritative count of active/queued secondaries is \
                     at or above initial count; not firing respawn (the \
                     declaration was likely a false-positive from local \
                     deafness — see #543/#544)",
                );
                return;
            }
            None => {
                tracing::warn!(
                    target: "dynrunner_respawn",
                    peer_id = %request.original_id,
                    cause = ?request.cause,
                    initial_count,
                    event = "respawn_suppressed_authority_unknown",
                    "slurm-authoritative count is Unknown (stale snapshot / \
                     probe failure); fail-closed: refusing respawn until \
                     evidence lands",
                );
                return;
            }
            Some(_) => { /* current < initial_count: proceed */ }
        }
        let (spawner, budget) = match (self.respawn_spawner.as_ref(), self.respawn_budget.as_ref())
        {
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
            self.cluster_state.respawn_events(),
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
        // Carry the DEAD member's id so the provider can resolve its SLURM
        // node from SLURM's own vocabulary (job id → squeue/sacct) and
        // exclude it, keeping the replacement off a NODE_FAIL/faulty node.
        // The primary is provider-agnostic — it never resolves the node
        // itself (a non-SLURM provider has no node to exclude); it just
        // names who died and lets the provider decide. Best-effort: an
        // unresolvable id places the replacement unconstrained.
        let spec = SecondarySpawnSpec {
            new_secondary_id: new_id.clone(),
            primary_endpoint: self.respawn_primary_endpoint.clone(),
            primary_pubkey_pem: self.respawn_primary_pubkey_pem.clone(),
            dead_member_id: Some(request.original_id.clone()),
        };

        // Record the accepted event on the REPLICATED ledger NOW —
        // before the spawn future resolves — so budget consultation for
        // any immediately-following request in the same `select!` tick
        // already sees this entry. Without this, a tight burst of peer
        // deaths could each independently consult an empty ledger and all
        // pass the cap. Keyed by the freshly-minted `new_id` (globally
        // unique), so the union-by-key merge never collides; the value
        // carries the chain root + cause + timestamp the budget reads. A
        // promoted primary inherits this via snapshot/AE, so the budget +
        // cooldown survive failover (F7).
        self.cluster_state.record_respawn_event(
            new_id.clone(),
            RespawnEventRecord {
                original_id: request.original_id.clone(),
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
