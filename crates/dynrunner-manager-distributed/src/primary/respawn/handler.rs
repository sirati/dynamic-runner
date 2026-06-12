//! Operational-loop arms for the respawn pipeline. The impl block here
//! decorates `PrimaryCoordinator` with `dispatch_respawn_lifecycle`
//! (called on the respawn lifecycle select arm; routes `Removed` into
//! `dispatch_respawn_request` and `Added` into the pending-replacement
//! reconciliation) and `handle_respawn_join` (called when the
//! `JoinSet<RespawnOutcome>` yields a finished task).

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
    /// lifecycle channel. `Removed` is a death — translated into a
    /// [`RespawnRequest`] and dispatched. `Added` is a join —
    /// reconciled against the pending-replacement bookkeeping (the
    /// joiner is either a pending replacement claiming its place, the
    /// re-admitted original whose still-pending replacement is now a
    /// squatter, or — the common case — neither).
    pub(crate) fn dispatch_respawn_lifecycle(&mut self, event: PeerLifecycleEvent) {
        match event {
            PeerLifecycleEvent::Removed { id, cause } => {
                self.dispatch_respawn_request(RespawnRequest {
                    original_id: id,
                    cause,
                });
            }
            PeerLifecycleEvent::Added { id, .. } => {
                self.reconcile_replacements_on_join(&id);
            }
        }
    }

    /// Reconcile the pending-replacement bookkeeping against a peer
    /// that just joined the replicated membership.
    ///
    /// Two matches are possible (both cheap map probes; the common
    /// no-match join costs one lookup + one scan over a map bounded by
    /// `RespawnBudget::max_total`):
    ///
    /// 1. `joined_id` IS a pending replacement → it welcomed and is
    ///    the legitimate occupant (it joins under its freshly-minted
    ///    `secondary-N` id, never the dead member's, so there is no
    ///    identity conflict). Clear its entry. If its original is
    ///    re-admitted LATER, nothing is revoked: a welcomed member is
    ///    ordinary fleet capacity, and killing it would only re-enter
    ///    the removal→respawn churn it was spawned to resolve.
    ///
    /// 2. `joined_id` is the ORIGINAL of one or more pending
    ///    replacements → the re-admission edge (the frame-ingest seam
    ///    proved the removed member alive and bumped its membership
    ///    generation). Every still-pending replacement for it is a
    ///    resource squatter: revoke it through the provider port
    ///    (best-effort `scancel` for SLURM). The revoke runs detached
    ///    on the LocalSet — same orphan-safety shape as the
    ///    provider's own spawn internals — because the loop arm must
    ///    not await a gateway round-trip. A revoke transport failure
    ///    is logged loudly; the job id stays on the provider's
    ///    `job_ids` ledger, so the run-teardown `cleanup()` sweep
    ///    still scancels it (no re-admission retry sweep exists).
    ///
    /// Ordering between the two cases is the lifecycle channel's apply
    /// order — both `PeerJoined` applies flow through the SAME
    /// dispatcher → channel → this method, so the
    /// replacement-welcomed-first and original-re-admitted-first
    /// interleavings resolve deterministically.
    fn reconcile_replacements_on_join(&mut self, joined_id: &str) {
        // Case 1: the joiner is a pending replacement — it claimed its
        // place; the bookkeeping entry is done.
        if let Some(original_id) = self.pending_replacements.remove(joined_id) {
            tracing::info!(
                target: "dynrunner_respawn",
                original_id = %original_id,
                new_id = %joined_id,
                event = "respawn_replacement_joined",
                "replacement secondary joined the membership; it is the \
                 legitimate occupant (no revocation possible for it from \
                 here on)",
            );
            return;
        }
        // Case 2: the joiner is the original of pending replacement(s)
        // — the re-admission edge. Revoke every squatter.
        let squatters: Vec<String> = self
            .pending_replacements
            .iter()
            .filter(|(_, original_id)| original_id.as_str() == joined_id)
            .map(|(new_id, _)| new_id.clone())
            .collect();
        if squatters.is_empty() {
            return;
        }
        // Defensive mirror of `dispatch_respawn_request`: entries only
        // exist when `enable_respawn` installed the spawner, so this
        // cannot fire outside a logic error.
        let Some(spawner) = self.respawn_spawner.as_ref().map(Arc::clone) else {
            tracing::warn!(
                target: "dynrunner_respawn",
                peer_id = %joined_id,
                "pending replacements exist but the respawn policy is \
                 disabled; cannot revoke",
            );
            return;
        };
        for new_id in squatters {
            self.pending_replacements.remove(&new_id);
            tracing::info!(
                target: "dynrunner_respawn",
                original_id = %joined_id,
                new_id = %new_id,
                event = "respawn_replacement_revoked",
                "member re-admitted while its replacement was still \
                 pending; revoking the redundant replacement",
            );
            let spawner = Arc::clone(&spawner);
            tokio::task::spawn_local(async move {
                if let Err(e) = spawner.revoke(&new_id).await {
                    tracing::warn!(
                        target: "dynrunner_respawn",
                        new_id = %new_id,
                        error = %e,
                        event = "respawn_revoke_failed",
                        "could not revoke the redundant replacement (provider \
                         backend unreachable); its job id remains on the \
                         provider's ledger, so run-teardown cleanup() will \
                         scancel it — if the run aborts before teardown, a \
                         manual scancel may be needed",
                    );
                }
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
        // duplicate the re-admitted identity — but until it JOINS it is
        // a revocable resource squatter, tracked in
        // `pending_replacements` and reconciled by
        // `reconcile_replacements_on_join` when either party's
        // `PeerJoined` lands.
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
        // The dead member's node, recorded from its welcome and surviving
        // its removal (`secondary_nodes`). When known, the spawner excludes
        // it so the replacement never re-inherits a NODE_FAIL/faulty node;
        // when unknown the replacement places unconstrained (best-effort).
        let exclude_node = self.secondary_nodes.get(&request.original_id).cloned();
        let spec = SecondarySpawnSpec {
            new_secondary_id: new_id.clone(),
            primary_endpoint: self.respawn_primary_endpoint.clone(),
            primary_pubkey_pem: self.respawn_primary_pubkey_pem.clone(),
            exclude_node,
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
        // Track the replacement as pending-until-join so a later
        // re-admission of the original can revoke it (see
        // `reconcile_replacements_on_join`). Inserted at accept time —
        // BEFORE the spawn future runs — so a re-admission landing in
        // the submission window is still observed; the provider's
        // `revoke` contract absorbs the not-yet-submitted race.
        self.pending_replacements
            .insert(new_id.clone(), request.original_id.clone());
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
