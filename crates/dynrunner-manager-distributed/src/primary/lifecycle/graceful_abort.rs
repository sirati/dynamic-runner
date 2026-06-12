//! The primary side of the observer-requested GRACEFUL abort.
//!
//! # Single concern
//!
//! Drive the graceful-abort protocol on the authoritative primary:
//!
//! 1. **Request → latch.** An observer's typed
//!    [`DistributedMessage::GracefulAbortRequest`] (the ONE management
//!    command a zero-authority observer may send) is turned into the
//!    replicated [`ClusterMutation::GracefulAbortRequested`] sticky latch
//!    via the canonical `apply_and_broadcast_cluster_mutations` path
//!    ([`PrimaryCoordinator::handle_graceful_abort_request`]).
//! 2. **The drain decision.** Once per operational-loop iteration,
//!    [`PrimaryCoordinator::graceful_abort_tick`] evaluates the replicated
//!    occupancy facts and either (a) breaks the loop when the WHOLE fleet
//!    has drained (no `InFlight` anywhere → the finalize tail broadcasts
//!    `RunComplete` and surfaces the graceful-abort verdict), (b) RELOCATES
//!    the primary role to the busiest secondary when THIS node's own work
//!    has drained while other secondaries still run
//!    ([`RelocationPolicy::MostActiveWorkers`] over the one shared
//!    selector, riding the existing `PrimaryChanged { Transferred }` +
//!    promotion-snapshot relocation mechanism), or (c) keeps looping.
//! 3. **The respawn gate.** Under the latch a departed secondary must not
//!    be replaced ([`PrimaryCoordinator::dispatch_respawn_request`] consults
//!    the same fact) — the fleet is draining DOWN by design.
//!
//! The dispatch FREEZE itself does NOT live here: it is step 0 of the one
//! dispatch-view pipeline (`PrimaryCoordinator::dispatch_view_for_worker`),
//! the single seam every path from the ready pool to a worker constructs
//! its view through.
//!
//! Every decision input is CRDT-derived (`graceful_abort_requested`,
//! `inflight_count_for_secondary`, `current_primary`), so a
//! failover-promoted primary inherits the same facts via its snapshot and
//! re-derives the same decisions (the no-redo law).

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::primary::PrimaryCoordinator;

use super::promotion::RelocationPolicy;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// React to an observer's `GracefulAbortRequest` frame: delegate to
    /// [`Self::initiate_graceful_abort`] with the requesting node's id.
    /// The wire frame is the zero-authority observer's ONE management
    /// command; a primary that receives the operator's SIGUSR2 directly
    /// drives the SAME initiation via its signal arm (it IS the abort
    /// authority — no wire request needed).
    pub(crate) async fn handle_graceful_abort_request(&mut self, msg: DistributedMessage<I>) {
        let DistributedMessage::GracefulAbortRequest { sender_id, .. } = msg else {
            return;
        };
        self.initiate_graceful_abort(&sender_id).await;
    }

    /// THE single graceful-abort initiation: originate the replicated
    /// [`ClusterMutation::GracefulAbortRequested`] sticky latch (apply
    /// locally + broadcast fleet-wide) and emit the operator-facing
    /// important event. `requested_by` names the originator (an observer's
    /// `sender_id` for a wire request, this primary's own node id for a
    /// SIGUSR2 self-trigger) and rides the milestone log only.
    ///
    /// Idempotent: a re-sent request against an already-latched freeze is a
    /// silent NoOp (the apply filter drops it off the wire too), so operator
    /// re-triggering / at-least-once delivery / a SIGUSR2 racing a wire
    /// request never re-announces.
    pub(crate) async fn initiate_graceful_abort(&mut self, requested_by: &str) {
        if self.cluster_state.graceful_abort_requested() {
            tracing::debug!(
                requested_by = %requested_by,
                "graceful-abort request received but the freeze is already latched; NoOp"
            );
            return;
        }
        // Operator-wake milestone: the run is now deliberately winding
        // down. Emitted at the importance target so `--important-stdio`
        // surfaces it on the primary host too (the observer narrates its
        // own copy off the replicated latch).
        tracing::warn!(
            target: super::super::important_events::IMPORTANT_TARGET,
            requested_by = %requested_by,
            "graceful abort requested — dispatch frozen; running tasks will \
             complete, each secondary tears down as it drains, and the run \
             ends with the graceful-abort verdict"
        );
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::GracefulAbortRequested])
            .await;
    }

    /// The per-iteration graceful-abort drain decision. Returns `true` iff
    /// the operational loop should BREAK this iteration (the whole fleet
    /// has drained — route into the finalize tail, which broadcasts
    /// `RunComplete` and surfaces the graceful-abort verdict).
    ///
    /// Decision ladder (all inputs replicated — see the module doc):
    ///
    /// 1. Not latched → `false` (the steady-state no-op; one bool read).
    /// 2. Not the recognized primary (this node already relocated the role
    ///    away and is awaiting its demote cancellation) → `false`: a
    ///    stepped-down node must never drive the terminal.
    /// 3. ZERO `InFlight` entries cluster-wide → `true` (full drain; the
    ///    deliberately-unscheduled pool residue is the finalize tail's
    ///    graceful-verdict accounting, never a strand).
    /// 4. THIS node's own co-resident secondary has drained while ≥1 other
    ///    secondary still runs work → relocate the primary role to the
    ///    busiest eligible secondary
    ///    ([`RelocationPolicy::MostActiveWorkers`]) and return `false`:
    ///    the local apply of `PrimaryChanged { Transferred }` fires the
    ///    demote hook, `run_consuming`'s demote arm cancels this pipeline
    ///    into the standalone-observer handoff, and the chosen peer's
    ///    `PromotionSignal` builds the snapshot-seeded promoted primary —
    ///    the EXISTING relocation mechanism end to end. No eligible busy
    ///    target (e.g. none advertises `can_be_primary`) → stay put and
    ///    keep draining in place.
    pub(crate) async fn graceful_abort_tick(&mut self) -> bool {
        if !self.cluster_state.graceful_abort_requested() {
            return false;
        }
        // (2) Only the recognized primary acts. After a relocate the local
        // apply has already re-pointed `current_primary` at the chosen
        // peer, so this also one-shots the relocate (no re-fire while the
        // demote cancellation is in flight).
        let own_id = self.config.node_id.as_str();
        if self.cluster_state.current_primary() != Some(own_id) {
            return false;
        }
        // (3) Full drain: no replicated `InFlight` anywhere. Dead
        // secondaries' in-flight entries leave this set via the existing
        // `TaskRequeued` recovery, so a mid-drain death converges here too.
        if self.cluster_state.counts().in_flight == 0 {
            tracing::info!(
                target: super::super::important_events::IMPORTANT_TARGET,
                "graceful abort: fleet drained (no in-flight work anywhere); \
                 finalizing with the graceful-abort verdict"
            );
            return true;
        }
        // (4) Own node drained, remote work continues → relocate to the
        // busiest eligible secondary. `inflight_count_for_secondary(own)`
        // covers the co-resident secondary (primary and its secondary
        // share the node id).
        if self.cluster_state.inflight_count_for_secondary(own_id) == 0
            && let Some(chosen) =
                self.select_relocation_target(RelocationPolicy::MostActiveWorkers)
        {
            tracing::info!(
                target: super::super::important_events::IMPORTANT_TARGET,
                chosen = %chosen,
                "graceful abort: this primary's node has drained while \
                 other secondaries still run work; relocating the primary \
                 role to the busiest secondary"
            );
            self.relocate_primary_to(chosen).await;
        }
        // (Own node drained but no eligible busy target: stay put and
        // drain in place — the full-drain arm above terminates the run
        // once the remote work finishes.)
        false
    }
}
