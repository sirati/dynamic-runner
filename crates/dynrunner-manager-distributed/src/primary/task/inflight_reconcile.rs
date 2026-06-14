//! Cross-member duplicate dedup (#518) — the authority half of the
//! re-admission worker-source-of-truth reconciliation.
//!
//! # The gap this closes
//!
//! When a LIVE member A is falsely declared dead (its keepalive lagged
//! under CPU load), the primary REQUEUES A's in-flight tasks and
//! re-dispatches them to OTHER live members. A is NOT told to stop
//! (`readmission.rs`) and keeps running its tasks; the requeued copy runs
//! on a DIFFERENT member B that genuinely is not running it — so #517's
//! same-worker bounce never fires (B's slot was idle) and the SAME task
//! executes on BOTH A and B (cross-member double-execution).
//!
//! # The reconciliation (the owner's principle, verbatim)
//!
//! A is the source of truth for what ITS workers run. On re-admission the
//! primary PULLS A's actual in-flight roster (`RequestInFlightRoster` →
//! `InFlightRoster`, read off A's own `active_tasks`). For each hash A
//! reports running, the primary recognises A as the AUTHORITATIVE holder
//! and reconciles: it re-seats its in-flight ledger to A and WITHDRAWS
//! the requeued duplicate copy from the other member B (`WithdrawTask`).
//!
//! # Authority rule
//!
//! The ORIGINAL member (A — already executing) WINS over the requeued
//! copy (B). The withdraw stands the LOSER (B) down, never the
//! re-admitted original — matching #467's seated-replacement wind-down
//! (the copy was spawned only to cover a death that turned out false) and
//! avoiding re-running work already in progress.
//!
//! # What the withdraw can and cannot do
//!
//! There is NO per-task mid-run abort on the secondary, so `WithdrawTask`
//! cleanly drops a copy B has NOT yet started (still queued / pre-bind). A
//! copy already executing on B is left in place; its eventual terminal is
//! absorbed by the primary's existing hash-keyed terminal dedup
//! (`completed_tasks` / `failed_tasks`), so double-execution can still
//! waste compute in that narrow window but NEVER corrupts accounting. The
//! re-seat + withdraw closes the steady-state double-run.
//!
//! # Shared with the already-held cross-member arm
//!
//! [`PrimaryCoordinator::reconcile_authoritative_holder`] is the single
//! "this member is the authoritative holder → withdraw the duplicate"
//! primitive. The `already_held` handler's cross-member arm (a holder
//! reports it is already running a hash the ledger attributes to a
//! DIFFERENT member) routes through the SAME primitive instead of
//! tolerating the double-exec — one dedup concern, two triggers
//! (re-admission roster pull + already-held report).

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    Destination, DistributedMessage, InFlightRosterEntry, PeerId,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::primary::wire::timestamp_now;
use crate::primary::{PrimaryCoordinator, SlotProvenance};

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Pull a just-re-admitted member's ACTUAL in-flight roster (#518): a
    /// directed [`DistributedMessage::RequestInFlightRoster`]. Called from
    /// the re-admission seam (`readmission.rs`) the moment a falsely-removed
    /// member is re-admitted — the member is the source of truth for what
    /// its workers run, and the primary must learn that to dedup the
    /// duplicates it requeued onto other members. Best-effort: a lost
    /// request leaves the duplicate to the terminal-dedup backstop and the
    /// member's next re-contact does not re-trigger (it is alive now), so
    /// the request rides the reliable directed-secondary edge.
    pub(in crate::primary) async fn request_inflight_roster(&mut self, member_id: &str) {
        let msg = DistributedMessage::<I>::RequestInFlightRoster {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
        };
        if let Err(e) = self
            .send_to(
                Destination::Secondary(PeerId::from(member_id.to_string())),
                msg,
            )
            .await
        {
            tracing::warn!(
                member = %member_id,
                error = %e,
                "re-admission in-flight roster request send failed; the \
                 cross-member duplicate (if any) is left to the terminal-dedup \
                 backstop"
            );
        }
    }

    /// React to a re-admitted member's `InFlightRoster` answer (#518): for
    /// every hash the member reports its workers ACTUALLY running, recognise
    /// the member as the authoritative holder and dedup any requeued copy on
    /// another member via [`Self::reconcile_authoritative_holder`].
    ///
    /// `member_gen` STALENESS GATE: a roster that crossed a re-removal in
    /// flight (the member was re-declared dead between sending the roster and
    /// this landing) is stale — its live `peer_member_gen` has advanced past
    /// the reported generation, so the member is no longer the authoritative
    /// holder. Ignore the whole roster in that case rather than re-seating
    /// the ledger onto a member the cluster has since removed again.
    pub(in crate::primary) async fn handle_inflight_roster(&mut self, msg: DistributedMessage<I>) {
        let DistributedMessage::InFlightRoster {
            secondary_id,
            member_gen,
            entries,
            ..
        } = msg
        else {
            return;
        };

        let live_gen = self.cluster_state.peer_member_gen(&secondary_id);
        if member_gen != live_gen {
            tracing::warn!(
                member = %secondary_id,
                reported_gen = member_gen,
                live_gen,
                "ignoring a STALE in-flight roster: the reporter's membership \
                 generation advanced (a re-removal crossed the roster in \
                 flight); it is no longer the authoritative holder"
            );
            return;
        }
        // Skip a member that is not currently a live member (re-removed
        // after the gen read above is impossible on the single-writer loop,
        // but a never-readmitted id reporting unsolicited is rejected here).
        if !self.cluster_state.is_peer_alive(&secondary_id) {
            tracing::warn!(
                member = %secondary_id,
                "ignoring an in-flight roster from a non-live member"
            );
            return;
        }

        tracing::info!(
            member = %secondary_id,
            member_gen,
            reported = entries.len(),
            "re-admitted member reported its actual in-flight roster; \
             reconciling each authoritatively-held task and withdrawing any \
             cross-member duplicate"
        );

        for entry in entries {
            let InFlightRosterEntry {
                hash, worker_id, ..
            } = entry;
            self.reconcile_authoritative_holder(&secondary_id, worker_id, &hash)
                .await;
        }
    }

    /// THE single cross-member dedup primitive (#518): `authoritative` is
    /// the member that is ACTUALLY running `task_hash` on its worker
    /// `auth_worker_id` (it told us so — the source of truth). If this
    /// primary's in-flight ledger attributes the SAME hash to a DIFFERENT
    /// member, that is the requeued duplicate: re-seat the ledger onto the
    /// authoritative holder and WITHDRAW the duplicate copy from the other
    /// member.
    ///
    /// Three cases on the ledger entry for `task_hash`:
    ///
    /// 1. holder == `authoritative` already — the ledger is correct
    ///    (re-dispatch happened to land back on the original, or the
    ///    re-admission re-seat already ran): NO action.
    /// 2. holder == a DIFFERENT live member B — the requeued duplicate. The
    ///    loser is B: free B's slot model, RE-SEAT the ledger entry onto the
    ///    authoritative holder (Inherited provenance — the occupancy is the
    ///    member's report, not a dispatch this primary originated, mirroring
    ///    the #517 incumbent re-seat), reconcile the authoritative member's
    ///    `(secondary, worker_id)` slot to hold the hash, and send
    ///    `WithdrawTask` to B.
    /// 3. NO ledger entry — the hash is either terminal (settled) or still
    ///    Pending (requeued, not yet re-dispatched). NO action: a terminal
    ///    must never be re-seated (re-running completed work), and a still-
    ///    Pending copy is not an ACTIVE double-run (no other member is
    ///    running it yet). Re-seating a Pending copy would need the full
    ///    task body the roster does not carry; the load-bearing double-RUN
    ///    case is (2), and a Pending copy that later double-dispatches is the
    ///    rare residual the terminal-dedup absorbs. Logged for forensics.
    ///
    /// The re-seat reads the full task body from the existing ledger entry
    /// (the duplicate's own record), so the roster's structured identity is
    /// not needed here — the ledger entry IS the task.
    pub(super) async fn reconcile_authoritative_holder(
        &mut self,
        authoritative: &str,
        auth_worker_id: u32,
        task_hash: &str,
    ) {
        let Some(entry) = self.in_flight.get(task_hash) else {
            tracing::debug!(
                authoritative = %authoritative,
                task_hash = %task_hash,
                "authoritative holder reports a hash absent from the in-flight \
                 ledger (terminal-settled or still pending); no dedup needed — \
                 no other member is running it"
            );
            return;
        };
        let ledger_holder = entry.secondary_id.clone();
        if ledger_holder == authoritative {
            tracing::debug!(
                authoritative = %authoritative,
                task_hash = %task_hash,
                "in-flight ledger already attributes the task to the \
                 authoritative holder; no cross-member duplicate"
            );
            return;
        }

        // (2) Cross-member duplicate. `ledger_holder` (B) is the requeued
        // loser; `authoritative` (A) keeps it. Snapshot B's holding worker
        // id (for the withdraw) and the task body (to re-seat the ledger and
        // the authoritative slot) BEFORE mutating.
        let loser = ledger_holder;
        let loser_worker = entry.local_worker_id;
        let phase = entry.phase.clone();
        let task = entry.task.clone();

        tracing::warn!(
            authoritative = %authoritative,
            auth_worker_id,
            duplicate_member = %loser,
            task_hash = %task_hash,
            "cross-member DUPLICATE detected: the task is authoritatively \
             running on the re-admitted original; re-seating the ledger onto \
             it and withdrawing the requeued copy (NOT failing it — the \
             requeue-inverse)"
        );

        // Free the LOSER's slot model so the primary stops believing B holds
        // this hash (B is being told to withdraw it). Resolve B's stable
        // `(secondary, worker_id)` to the live Vec slot and vacate it iff it
        // still holds this hash. The ledger entry is re-seated below, NOT
        // dropped — the authoritative holder owns it now.
        if let Some(b_worker) = loser_worker
            && let Some(b_idx) = self.worker_idx_for(&loser, b_worker)
        {
            let holds_this = self.workers[b_idx]
                .held_task()
                .is_some_and(|t| t.identifier == task.identifier);
            if holds_this {
                self.workers[b_idx].vacate();
            }
        }

        // RE-SEAT the ledger entry onto the authoritative holder: update the
        // holder + worker so the eventual terminal (from A) settles it by
        // hash on the correct member. The type slot stays reserved across
        // the re-seat (the task is still in flight, just on a different
        // member), and the phase in-flight counter is unchanged (still one
        // in-flight instance) — so we mutate the entry in place rather than
        // remove+reinsert.
        if let Some(e) = self.in_flight.get_mut(task_hash) {
            e.secondary_id = authoritative.to_string();
            e.local_worker_id = Some(auth_worker_id);
        }

        // Reconcile the AUTHORITATIVE member's slot to hold the hash with
        // Inherited provenance (the occupancy is A's report, reconciled
        // later by A's own terminal — exactly the #517 incumbent re-seat /
        // failover-resume occupancy crossing). Only when the slot resolves
        // AND is currently Idle in the model; a slot already holding this
        // hash is already correct, and a slot holding a different live hash
        // is left to its own terminal path.
        if let Some(a_idx) = self.worker_idx_for(authoritative, auth_worker_id)
            && self.workers[a_idx].is_idle()
        {
            let estimated = self.estimator.estimate(&task);
            let _assigned = self.workers[a_idx].assign(
                task_hash.to_string(),
                task,
                estimated,
                SlotProvenance::Inherited,
            );
        }

        let _ = phase;

        // WITHDRAW the duplicate copy from the loser. NOT a `TaskFailed`
        // (no terminal accounting, no retry-budget burn) — the requeue-
        // inverse, like the #517 bounce. The loser drops a not-yet-started
        // copy; a copy already executing is left to the terminal-dedup.
        let withdraw_worker = loser_worker.unwrap_or(0);
        let msg = DistributedMessage::<I>::WithdrawTask {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: loser.clone(),
            worker_id: withdraw_worker,
            task_hash: task_hash.to_string(),
        };
        if let Err(e) = self
            .send_to(Destination::Secondary(PeerId::from(loser.clone())), msg)
            .await
        {
            tracing::warn!(
                duplicate_member = %loser,
                task_hash = %task_hash,
                error = %e,
                "WithdrawTask delivery to the duplicate-holder failed; its \
                 eventual terminal is absorbed by the hash-keyed terminal \
                 dedup (no accounting corruption)"
            );
        }
    }
}
