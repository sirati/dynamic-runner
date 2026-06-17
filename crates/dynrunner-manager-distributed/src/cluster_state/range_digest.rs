//! Range-scoped (Merkle-lite) projection of the task ledger — the P1
//! "resume from last good state" DELTA on top of the scalar
//! [`StateDigest`](dynrunner_protocol_primary_secondary::StateDigest) task
//! fold.
//!
//! # Single concern
//!
//! Build a per-bucket
//! [`RangeDigest`](dynrunner_protocol_primary_secondary::RangeDigest) of the
//! task ledger: split the keyspace into [`RANGE_COUNT`] fixed hash-prefix
//! buckets ([`super::keyspace::range_index`]) and, per bucket, count the
//! entries + XOR-fold the SAME per-entry term the scalar
//! [`super::digest`] folds into `tasks_hash`. A behind node compares its
//! buckets to a peer's ([`RangeDigest::divergent_ranges`]) and pulls ONLY
//! the divergent ones.
//!
//! This is a PURE PROJECTION — read-only over the same `tasks` + `settled`
//! the digest reads, no mutation, no merge logic. It is a sibling to
//! [`super::digest`] (the scalar fold) and bound by the SAME two ledger
//! halves: the in-memory `tasks` (folded via
//! [`super::keyspace::task_digest_term`]) and the spilled `settled` entries
//! (folded via their persisted `digest_contribution`, the identical term
//! stamped at spill-commit). Folding both keeps the cross-bucket sum equal
//! to the scalar `tasks_hash` across the fat/settled split.
//!
//! # The two correctness invariants (pinned by the tests below)
//!
//! - `XOR(range-folds) == StateDigest::tasks_hash` and
//!   `sum(counts) == StateDigest::tasks_count` — by construction (every
//!   entry's term lands in exactly one bucket; XOR is associative +
//!   commutative). The `range_digest_folds_match_scalar` test pins it.
//! - A one-task change moves exactly one bucket (the changed key's), so a
//!   delta pull re-streams ~one bucket. The `one_task_change_isolates_to_
//!   one_range` test pins it.
//!
//! # Incremental memo (#492 P2 — built)
//!
//! [`ClusterState::tasks_range_digest`] is served from the node-local
//! [`super::range_fold_memo::RangeFoldMemo`], maintained INCREMENTALLY at the
//! task-state mutation sites (the seam this module + [`super::keyspace`]
//! documented): each mutation XORs the per-entry term keyed by `range_index`
//! out/in, so the read is O(buckets), not the O(ledger) fold a probe storm
//! would otherwise run on every inbound `PullProbe` (the #504 oploop wedge).
//! The one-pass fold remains as the `#[cfg(test)]`
//! [`ClusterState::fresh_tasks_range_digest`] the differential invariant test
//! recomputes against.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::RangeDigest;

use super::ClusterState;
use super::TaskState;
use super::keyspace::task_digest_term;
#[cfg(test)]
use super::keyspace::range_index;

impl<I: Identifier> ClusterState<I> {
    // ─── Incremental memo maintenance — the single task-state write path ───
    //
    // EVERY task-state write routes through the ONE `set_task_state` setter
    // below. It is the post-decision write primitive: it does NOT decide
    // WHICH state wins (the merge-join's LWW/rank join, the rank-drop arms'
    // variant preconditions, and the create sites' vacancy checks all decide
    // that BEFORE calling), it only WRITES an already-decided state and, in
    // the one place, maintains the two coupled side-tables a write must touch:
    //   * the range-fold memo (XOR the old per-entry term out + the new term
    //     in, via the SHARED `keyspace::task_digest_term` — the SAME term
    //     `digest()` folds into `tasks_hash`, so memo and scalar fold can
    //     never disagree), and
    //   * the #520 narration event (built from the POST-write state's
    //     classification, emitted on the observer channel — a silent no-op
    //     when nobody narrates).
    // A NEW transition arm calls `set_task_state` and gets memo maintenance +
    // narration FOR FREE — it cannot forget either. The memo stays equal to a
    // fresh fold by construction (the `range_digest_memo_matches_fresh_fold`
    // invariant), and the narration fires exactly-once per genuine write (the
    // callers route the setter ONLY on their real-transition branch; a NoOp /
    // dominated arm short-circuits before calling it).

    /// The ONE task-state write path. Writes `new` into the `hash` slot and,
    /// in the correct order, maintains the range-fold memo + emits the #520
    /// narration event:
    ///
    /// 1. Capture the PRE-write per-entry range term while the old state is
    ///    still in the slot (`None` if the slot was vacant — a logical
    ///    CREATE; `Some` if occupied — a state CHANGE under a fixed key).
    /// 2. Insert `new`.
    /// 3. Range-fold memo: a `None` old term is a CREATE (`add` — bumps the
    ///    bucket count); a `Some` is a CHANGE (`swap` old→new — count
    ///    conserved). The memo ops are pure XOR on the fold array keyed by
    ///    `range_index(hash)` and never read `self.tasks`, so doing them after
    ///    the insert is bit-identical to doing them before.
    /// 4. #520 emit: build the narration event from the POST-write state's
    ///    `to_state_change` classification, with the holder = the post-write
    ///    state's OWN holder, falling back to `fallback_holder` only when the
    ///    post-write state names none. The merge join passes the PRE-merge
    ///    holder there so a terminal that superseded an `InFlight` (whose own
    ///    holder is `None`) narrates the "completed/failed ON which worker"
    ///    answer the post-write terminal no longer carries; a non-merge write
    ///    passes `None`, so the holder is exactly the post-write state's own
    ///    (bit-identical to the pre-refactor `emit_task_state_change_for`).
    ///    The post-state-FIRST precedence reproduces the merge join's
    ///    `incoming.holder().or(prior_holder)` exactly (an assignment's own
    ///    new `InFlight` holder wins; a terminal falls back to the prior). A
    ///    silent no-op when no observer installed the channel.
    ///
    /// This is the POST-decision write: callers run their own join /
    /// precondition / vacancy decision FIRST and call this only on a genuine
    /// write, so the emit fires exactly-once per winning transition and the
    /// NoOp arms stay narration-silent.
    pub(super) fn set_task_state(
        &mut self,
        hash: &str,
        new: TaskState<I>,
        fallback_holder: Option<(String, dynrunner_core::WorkerId)>,
    ) {
        let old_term = self
            .tasks
            .get(hash)
            .map(|old| task_digest_term(hash, old));
        // #520 FROM-state: the human tag of the slot's PRE-write occupant,
        // captured under the same immutable borrow that reads `old_term`,
        // BEFORE the move-in overwrites the slot. `None` for a logical
        // CREATE (vacant slot — a spawn-time first write), where there is
        // no prior state to name. Skipped (along with the whole event
        // build below) when no observer is narrating, but read here while
        // the borrow is cheap and the slot still holds the old state.
        let from_state: Option<&'static str> = self
            .tasks
            .get(hash)
            .filter(|_| self.task_state_change_tx.is_some())
            .map(|old| old.state_tag());
        // Capture the OLD slot's outcome bucket (if it held a terminal) under
        // the same immutable borrow, before the move-in overwrites it. The
        // post-insert `outcome_tally.swap` decrements it and increments the
        // new state's bucket — the SOLE incremental maintenance of the
        // outcome partition `outcome_counts()` reads in O(1).
        let old_outcome_bucket = self
            .tasks
            .get(hash)
            .and_then(super::outcome_tally::outcome_bucket_of);
        // Capture the OLD slot's `Blocked.on` (if any) so the post-insert
        // `blocked_by` reverse-index maintenance below removes `hash` from
        // the old prereq's dependent set. Read here under the immutable
        // `tasks.get(hash)` borrow so the index ops do not race the insert.
        let old_blocked_on: Option<String> = self
            .tasks
            .get(hash)
            .and_then(|old| match old {
                TaskState::Blocked { on, .. } => Some(on.clone()),
                _ => None,
            });
        // Capture the NEW slot's `Blocked.on` (if any) under the move-in's
        // borrow so the post-insert add does not need to re-read the slot.
        let new_blocked_on: Option<String> = match &new {
            TaskState::Blocked { on, .. } => Some(on.clone()),
            _ => None,
        };
        let new_term = task_digest_term(hash, &new);
        // Capture the NEW slot's outcome bucket (if terminal) under the
        // move-in's borrow so the post-insert tally swap needs no re-read.
        let new_outcome_bucket = super::outcome_tally::outcome_bucket_of(&new);
        self.tasks.insert(hash.to_string(), new);
        match old_term {
            Some(old) => self.range_fold_memo.swap(hash, old, new_term),
            None => self.range_fold_memo.add(hash, new_term),
        }
        // Outcome tally: decrement the OLD terminal bucket (if any) and
        // increment the NEW (if any). A CREATE (old `None`) increments; a
        // terminal→non-terminal (reinject / reset) decrements; a terminal→
        // different-terminal adjusts both. Pure scalar adjust on the partition
        // keyed by nothing but the buckets, so doing it after the insert is
        // bit-identical to doing it before (it never reads `self.tasks`).
        self.outcome_tally
            .swap(old_outcome_bucket, new_outcome_bucket);
        // `blocked_by` reverse-index maintenance (#547). REMOVE first, ADD
        // second — both ops keyed by the prereq hash the dependent waits on,
        // so a Blocked→Blocked rewrite onto a DIFFERENT prereq correctly
        // re-buckets (remove from the old key, add to the new). A
        // Blocked→non-Blocked transition removes (resume); a non-Blocked→
        // Blocked transition adds (cascade/spawn into Blocked); a same-`on`
        // Blocked→Blocked rewrite is a no-op (the entry already names the
        // hash). Borrow by ref so we can consult both at the second arm.
        let same_on = old_blocked_on.as_deref() == new_blocked_on.as_deref();
        if let Some(old_on) = old_blocked_on.as_deref()
            && !same_on
            && let Some(set) = self.blocked_by.get_mut(old_on)
        {
            set.remove(hash);
            if set.is_empty() {
                self.blocked_by.remove(old_on);
            }
        }
        if let Some(new_on) = new_blocked_on
            && !same_on
        {
            self.blocked_by
                .entry(new_on)
                .or_default()
                .insert(hash.to_string());
        }
        // #520: build + emit the narration event from the POST-write state.
        // Skip the read+build entirely when no observer is narrating (the
        // common case on primary/secondary). The holder is the post-write
        // state's own, falling back to `fallback_holder` (the merge join's
        // PRE-merge holder for a terminal) only when the post-write state
        // names none — the SAME `incoming.holder().or(prior_holder)`
        // precedence the merge join used inline.
        if self.task_state_change_tx.is_none() {
            return;
        }
        if let Some(state) = self.tasks.get(hash) {
            let event = crate::task_state_change::TaskStateChangeEvent {
                task_id: state.def().task_id.clone(),
                change: state.to_state_change(),
                holder: state.holder().or(fallback_holder),
                // The PRE-write occupant's tag (`None` on a CREATE) +
                // the POST-write state's CRDT transaction coordinates —
                // the from→to transition + the correlator the merge join
                // arbitrated on. Both read off the SAME shared write seam,
                // path-independent across apply / restore / rank-drop.
                from: from_state,
                txn: state.txn_id(),
                // The build site carries no narration-source concern: the
                // write seam is CRDT-path-independent. The emit chokepoint
                // (`emit_task_state_change_event`) AUTHORITATIVELY stamps
                // this from the scoped restore marker — `LiveBroadcast`
                // here is the default it overwrites under restore scope.
                source: crate::task_state_change::NarrationSource::LiveBroadcast,
            };
            self.emit_task_state_change_event(event);
        }
    }

    /// The memo-aware in-place state rewrite the authoritative-rank-drop arms
    /// share (the 8 `*state = ...` sites): a presence-guarded wrapper over the
    /// single [`Self::set_task_state`] write path. Returns `false` (a NoOp,
    /// no write) when the hash is absent — the same absent-slot NoOp the arms'
    /// bare `get_mut` took. The arms keep their own variant-precondition match
    /// (which extracts task/attempt) BEFORE calling this; this funnels the
    /// common rewrite tail through the ONE memo-maintaining + narrating seam
    /// so no `*state =` site can silently skip the memo or the emit. The eight
    /// arms (Reinjected / Requeued / Retried / Blocked / SkippedAlreadyDone /
    /// SetupCompleted / AffineReady / QueuedAfterLocalDependencySet) each
    /// transition to a state whose narration holder is its own (always `None`
    /// for these target states), so no `fallback_holder` is needed.
    pub(super) fn rewrite_task_state(&mut self, hash: &str, new: TaskState<I>) -> bool {
        if !self.tasks.contains_key(hash) {
            return false;
        }
        self.set_task_state(hash, new, None);
        true
    }

    /// Build the range-scoped [`RangeDigest`] of the task ledger: per
    /// hash-prefix bucket, the entry count + the XOR-fold of the per-entry
    /// term. The cross-bucket fold reconstructs the scalar
    /// `StateDigest::tasks_hash` exactly (and the cross-bucket count sum the
    /// scalar `tasks_count`) — the invariant the delta's correctness rests
    /// on.
    ///
    /// O(buckets): served from the incrementally-maintained
    /// [`super::range_fold_memo::RangeFoldMemo`] (a cheap array clone), NOT a
    /// per-call O(ledger) fold. The memo is XOR-maintained over the LOGICAL
    /// ledger (fat `tasks` ∪ spilled `settled`) at every task-state mutation,
    /// so a probe storm at a 66k-task phase START reads it instantly instead
    /// of folding 66k entries per inbound probe (#504). A SETTLED entry stays
    /// counted in the memo — a spill MOVES the entry's term between the fat
    /// and settled halves the logical fold sums over, never changing it.
    ///
    /// Returns a `Box`: a `RangeDigest` is ~3 KiB, and every consumer (the
    /// probe reply, the candidate list, the pull directive) keeps it boxed to
    /// stay off the by-value stack-move paths through the hot dispatch loop,
    /// so building it on the heap from the start avoids a transient 3 KiB
    /// stack copy at the read site.
    pub fn tasks_range_digest(&self) -> Box<RangeDigest> {
        self.range_fold_memo.to_range_digest()
    }

    /// Test seam: the un-memoized O(ledger) one-pass fold the incremental
    /// [`Self::tasks_range_digest`] memo must always equal. The differential
    /// invariant pin (`range_digest_memo_matches_fresh_fold`) asserts the
    /// memo-served digest equals this fresh fold after every mutation; a
    /// missed XOR site at any mutation path makes them diverge.
    ///
    /// Read-only over the LOGICAL ledger (fat `tasks` ∪ spilled `settled`),
    /// the same universe `digest()` / `snapshot()` / the stream plan iterate.
    /// A SETTLED entry folds its persisted `digest_contribution` (the term
    /// stamped at spill-commit, identical to a live fold) into its bucket.
    #[cfg(test)]
    pub(crate) fn fresh_tasks_range_digest(&self) -> Box<RangeDigest> {
        // Count this full O(ledger) fold for the bound-per-iteration test
        // (mirroring `digest.rs`'s `digest_fold_count`): the PRODUCTION
        // `tasks_range_digest` read serves the memo and NEVER calls this, so
        // a probe storm can be proved to run ZERO full re-folds. A process-
        // wide thread-local keeps the counter test-scoped (no struct field,
        // no exhaustive-guard churn) — the tests run single-threaded per case.
        RANGE_FRESH_FOLD_COUNT.with(|c| c.set(c.get() + 1));
        let mut digest = Box::new(RangeDigest::default());
        // Fat (in-memory) entries: fold the per-entry term into the key's
        // bucket — the SAME term `digest()`'s live loop folds into
        // `tasks_hash`.
        for (key, state) in &self.tasks {
            let r = range_index(key);
            digest.counts[r] = digest.counts[r].saturating_add(1);
            digest.folds[r] ^= task_digest_term(key, state);
        }
        // Settled (spilled) entries: fold their persisted contribution (the
        // identical term, moved into the settled accumulator at spill-commit)
        // into the key's bucket. Settled bodies live on disk; the term is the
        // only thing the fold needs, so no per-entry file read here.
        for (key, term) in self.settled.digest_contributions() {
            let r = range_index(key);
            digest.counts[r] = digest.counts[r].saturating_add(1);
            digest.folds[r] ^= term;
        }
        digest
    }
}

#[cfg(test)]
thread_local! {
    /// Process-wide (per-thread) count of full O(ledger) range folds run via
    /// [`ClusterState::fresh_tasks_range_digest`]. The bound-per-iteration
    /// test reads it to prove the PRODUCTION `tasks_range_digest` read serves
    /// the memo and runs ZERO full re-folds under a probe burst.
    static RANGE_FRESH_FOLD_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
impl<I: Identifier> ClusterState<I> {
    /// Test seam: snapshot the full-range-fold counter (the count of O(ledger)
    /// folds run so far on this thread).
    pub(crate) fn range_fresh_fold_count() -> u64 {
        RANGE_FRESH_FOLD_COUNT.with(|c| c.get())
    }

    /// Test seam: reset the counter so a test isolates its own probe-burst
    /// window (the counter is thread-local and tests run single-threaded per
    /// case, but a reset makes the assertion robust to prior reads in-case).
    pub(crate) fn reset_range_fresh_fold_count() {
        RANGE_FRESH_FOLD_COUNT.with(|c| c.set(0));
    }
}
