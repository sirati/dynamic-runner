//! Already-held coherence handling — the authority half of the
//! duplicate-assignment recognition (the post-failover assign loop).
//!
//! A secondary that receives a `TaskAssignment` for a hash it is
//! ALREADY EXECUTING answers with a `TaskFailed` frame carrying
//! [`crate::secondary::TASK_ALREADY_HELD_WIRE_MESSAGE`] and the REAL
//! holding worker id (see the emitter arm in
//! `secondary/dispatch/router.rs`). That frame is a COHERENCE REPORT,
//! not a terminal and not backpressure: the work never left the
//! holder. `handle_task_failed` recognises the marker FIRST (before
//! its dedup gate, before `free_slot_on_terminal`) and routes here.

use dynrunner_core::Identifier;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::primary::PrimaryCoordinator;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// React to a holder's "already running this hash" report.
    ///
    /// CASE 1 — the hash is in the in-flight ledger (the common shape:
    /// the report answers THIS coordinator's own duplicate dispatch):
    /// the optimistic dispatch commit (slot `Assigned` + ledger entry +
    /// replicated `InFlight`) is retroactively the CORRECT holder
    /// record, so nothing is touched. The eventual real terminal
    /// settles slot/ledger/accounting by hash through
    /// `free_slot_on_terminal` exactly like any live dispatch. The
    /// report's `worker_id` (the holder's REAL worker) is diagnostics
    /// only: even when it differs from the slot this coordinator
    /// picked, terminals resolve the holder from the LEDGER entry, so
    /// the commit stays settle-coherent without a slot relocation (and
    /// relocating onto a slot that may hold another hash would break
    /// the slot-hash settle invariant).
    ///
    /// CASE 2 — the hash is NOT in the ledger: the report raced a
    /// recovery requeue (a false-dead sweep dropped the entry between
    /// the dispatch and this landing) or a terminal that already
    /// settled it. NO action is the safe and convergent handling:
    ///   * still queued → the next dispatch recheck re-offers it, the
    ///     holder re-answers already-held, and that commit sticks as
    ///     CASE 1 — one extra round trip, no loop, no state surgery;
    ///   * already terminal → there is nothing left to reconcile.
    ///
    /// Pre-fix this frame fell through to the TERMINAL-failure arm:
    /// retry budget burned, a false replicated `TaskFailed` originated
    /// for a still-running task, and a queued copy reclaimed as failed.
    ///
    /// Deliberately NO backpressure window and NO requeue in either
    /// case: an already-held answer is not a capacity signal, and a
    /// requeue is exactly the loop this report exists to break.
    ///
    /// CASE 1b (#518) — the ledger's holder is a DIFFERENT member than the
    /// reporter: the hash is executing on BOTH (a cross-member duplicate,
    /// the requeued copy from a false-dead recovery). The reporter is
    /// AUTHORITATIVELY running it (it told us so — the worker is the source
    /// of truth), so this no longer TOLERATES the double-exec: it routes
    /// through the SAME [`Self::reconcile_authoritative_holder`] primitive
    /// the #518 re-admission roster uses — re-seat the ledger onto the
    /// reporter and WITHDRAW the duplicate copy from the ledger's member.
    /// One dedup concern, two triggers (the re-admission roster pull and
    /// this already-held report).
    pub(crate) async fn note_task_already_held(
        &mut self,
        secondary_id: &str,
        worker_id: u32,
        task_hash: &str,
    ) {
        let Some(entry) = self.in_flight.get(task_hash) else {
            tracing::debug!(
                secondary = %secondary_id,
                holding_worker_id = worker_id,
                task_hash = %task_hash,
                "already-held report for an un-tracked hash (it raced a \
                 recovery requeue or a terminal); no action — a queued copy \
                 re-converges on the next dispatch recheck"
            );
            return;
        };
        if entry.secondary_id == secondary_id {
            tracing::info!(
                secondary = %secondary_id,
                holding_worker_id = worker_id,
                task_hash = %task_hash,
                "holder confirmed it is already running the dispatched \
                 task (duplicate assignment after an in-flight-fact \
                 loss); keeping it in flight on the holder — the real \
                 terminal settles it"
            );
            return;
        }
        // CASE 1b (#518): cross-member duplicate. The reporter is the
        // authoritative holder (it IS running the hash); reconcile + cancel
        // the duplicate copy on the ledger's member through the one dedup
        // primitive instead of leaving both to run.
        tracing::warn!(
            reporting_secondary = %secondary_id,
            ledger_secondary = %entry.secondary_id,
            task_hash = %task_hash,
            "already-held report from a DIFFERENT member than the ledger's \
             holder: the hash is executing on both (a duplicate dispatch \
             crossed members). The reporter is the authoritative holder — \
             reconciling the ledger onto it and withdrawing the duplicate \
             copy (#518)"
        );
        self.reconcile_authoritative_holder(secondary_id, worker_id, task_hash)
            .await;
    }
}
