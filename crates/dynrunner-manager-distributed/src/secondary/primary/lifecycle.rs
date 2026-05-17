//! Phase-lifecycle bookkeeping for the promoted-secondary's primary
//! pool.
//!
//! Single concern: drive `Drained` phases through `on_phase_end` →
//! `mark_phase_done` → newly-Active phases through `on_phase_start`,
//! mirroring `PrimaryCoordinator::process_phase_lifecycle` so a
//! setup-promoted secondary that owns the live pool fires the same
//! lifecycle hooks the demoted primary would have. The fire-site is
//! the only addition; the cascade-drain primitive itself stays the
//! free-function `cascade_drain_done` (callback-silent, used by
//! `populate_primary_from_cluster_state` whose semantics must NOT
//! refire `on_phase_end` for items that completed pre-promotion).
//!
//! Module boundary:
//!   * Owns: the `Option<OnPhaseStart>` / `Option<OnPhaseEnd>`
//!     invocation semantics on `SecondaryCoordinator` and the
//!     per-phase counter bookkeeping (`primary_phase_completed`,
//!     `primary_phase_failed`, `primary_phase_started_emitted`).
//!   * Does NOT own: the pool primitives themselves
//!     (`poll_drain_transitions` / `mark_phase_done` /
//!     `drain_empty_active_phases` / `active_phases`) — those live in
//!     `dynrunner-scheduler-api`'s `PendingPool` and are invoked
//!     verbatim here.

use dynrunner_core::{Identifier, MessageReceiver, MessageSender, PhaseId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

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
    pub(in crate::secondary) fn process_primary_phase_lifecycle(&mut self) {
        loop {
            let drained: Vec<PhaseId> = match self.primary_pending.as_mut() {
                Some(pool) => pool.poll_drain_transitions(),
                None => return,
            };
            if drained.is_empty() {
                break;
            }
            for p in &drained {
                let completed = self
                    .primary_phase_completed
                    .get(p)
                    .copied()
                    .unwrap_or(0);
                let failed = self.primary_phase_failed.get(p).copied().unwrap_or(0);
                if let Some(cb) = self.on_phase_end.as_mut() {
                    cb(p, completed, failed);
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
