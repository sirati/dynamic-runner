//! The canonical convergence comparators that `apply`, `restore`, and
//! `digest` SHARE.
//!
//! Single concern: the ONE place the per-task CRDT join order is spelled.
//! Before this module the join was three independent hand-written
//! encodings (apply's per-arm lattice, restore's `task_state_rank`,
//! digest's `task_state_rank`) that could — and did — disagree. This
//! module owns the single ranker (`task_join_key`), the single dominance
//! comparator (`task_join_key_dominates`), and the single
//! side-effect-bearing join (`merge_task_state`); apply's monotone arms,
//! restore's merge loop, and the digest fold all derive their order from
//! here.
//!
//! Sectioned by field — the rule for the NEXT comparator is "it lives
//! here, under its own field section":
//!   * `// === per-task join ===` — `task_join_key`,
//!     `task_join_key_dominates`, `hashable_join_key`, `MergeOutcome`,
//!     `merge_task_state`.
//!   * `// === primary register ===` — `primary_register_adopt`.
//!   * `// === phase_deps ===` — `canonical_phase_deps_hash`.
//!   * `// === capabilities ===` — `merge_capability`,
//!     `capability_fold` (the 2P-set merge + its digest projection).
//!
//! The resets-are-not-joins boundary (DRAWN ONCE here): `merge_task_state`
//! is the MONOTONE join. Authoritative rank-DROP resets
//! (`TaskReinjected`/`TaskRequeued`) and the cascade-pause (`TaskBlocked`)
//! are DIFFERENT concerns and keep their own explicit-precondition arms in
//! `apply.rs` — they do NOT route through this join (a monotone dominance
//! comparator would reject a rank-drop). A reset still bumps the version
//! it stamps onto the new `Pending` so the C3 resurrection is closed, but
//! that stamp is the originator's concern (`broadcast.rs`), not a join.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use dynrunner_core::{Identifier, PhaseId, TaskOutputs, TaskVersion};

use super::ClusterState;
use super::TaskState;
use super::types::{
    CapabilityEntry, FailedLikeRank, JoinBand, NonTerminalRank, PhaseTally, TaskJoinKey,
    TerminalRank,
};
use crate::task_completed::TaskCompletedEvent;
use crate::task_state_change::TaskStateChangeEvent;

// === per-task join ===

/// Hash one hashable value to a `u64` via the standard default hasher.
/// Process-stable; the digest is only compared between peers running the
/// SAME binary, so cross-build stability is not required — only
/// determinism + order-independence within the run.
fn hash_one<H: Hash>(value: H) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Content hash of a terminal payload — `(discriminant, error/reason,
/// last_error)` — so two divergent failure records at an equal
/// `(terminal_rank, version)` compare/fold differently (C4). NON-OPTIONAL
/// for terminals (this is what closes the equal-version divergent-payload
/// sub-case the version alone, commonly `(epoch, 0)` for a single
/// primary, cannot). `0` for non-terminals.
///
/// Delegates per-variant to the field-level `*_payload_hash` helpers
/// below so the settled-spill PROBE keys (built from raw mutation
/// fields, without a `TaskState` in hand) hash the identical tuples —
/// one spelling, no drift.
fn terminal_payload_hash<I>(state: &TaskState<I>) -> u64 {
    match state {
        TaskState::Completed { .. } => completed_payload_hash(),
        TaskState::Failed {
            kind, last_error, ..
        } => failed_payload_hash(&kind.wire_value(), last_error),
        TaskState::Unfulfillable {
            reason, last_error, ..
        } => unfulfillable_payload_hash(reason, last_error),
        TaskState::InvalidTask {
            reason, last_error, ..
        } => invalid_task_payload_hash(reason, last_error),
        // Fixed discriminant tag; a skip carries no error payload, so the
        // content hash is the constant `4u8` (the terminal_rank already
        // separates it as the weakest terminal — two replicas holding the
        // skip for the same hash share this constant and idempotent-NoOp).
        TaskState::SkippedAlreadyDone { .. } => skipped_payload_hash(),
        // Fixed discriminant tag; a succeeded setup task carries no error
        // payload (the terminal_rank already separates it) — two replicas
        // holding the SetupCompleted for the same hash share this constant
        // and idempotent-NoOp.
        TaskState::SetupCompleted { .. } => setup_completed_payload_hash(),
        // Fixed discriminant tag; a SecondaryAffine gate carries no error
        // payload (the terminal_rank already separates it) — two replicas
        // holding the AffineReady for the same hash share this constant and
        // idempotent-NoOp.
        TaskState::AffineReady { .. } => affine_ready_payload_hash(),
        // Non-terminals (incl. the non-terminal `QueuedAfterLocalDependency`)
        // carry no terminal payload hash.
        TaskState::Pending { .. }
        | TaskState::InFlight { .. }
        | TaskState::QueuedAfterLocalDependency { .. }
        | TaskState::Blocked { .. } => 0,
    }
}

// Field-level payload-hash helpers: the per-variant tuples hashed by
// `terminal_payload_hash`, exposed so a PROBE key built from raw
// mutation fields (the settled-spill consult path, which has no
// `TaskState` to rank) folds byte-identically. `String` and `&str`
// hash identically (std `Hash` for `String` delegates to `str`), so a
// probe over the wire fields equals the state-borne hash.
fn completed_payload_hash() -> u64 {
    hash_one(0u8)
}
fn failed_payload_hash(kind_wire: &str, last_error: &str) -> u64 {
    hash_one((1u8, kind_wire, last_error))
}
fn unfulfillable_payload_hash(reason: &str, last_error: &str) -> u64 {
    hash_one((2u8, reason, last_error))
}
fn invalid_task_payload_hash(reason: &str, last_error: &str) -> u64 {
    hash_one((3u8, reason, last_error))
}
fn skipped_payload_hash() -> u64 {
    hash_one(4u8)
}
fn setup_completed_payload_hash() -> u64 {
    hash_one(5u8)
}
fn affine_ready_payload_hash() -> u64 {
    hash_one(6u8)
}

/// Build the ONE canonical convergence key for a task state (§2.2). The
/// single ranker — replaces both deleted `task_state_rank` fns. The
/// returned [`TaskJoinKey`] is ordered (via its derived `Ord`) so that
/// lexicographic comparison IS the convergence order: band first, then
/// (within the non-terminal band) version before rank (C3), and (within
/// the terminal band) terminal-rank (D-T) before version and payload
/// hash.
pub(super) fn task_join_key<I>(state: &TaskState<I>) -> TaskJoinKey {
    match state {
        TaskState::Pending {
            version, attempt, ..
        } => key_pending(*attempt, *version),
        TaskState::InFlight {
            version, attempt, ..
        } => key_in_flight(*attempt, *version),
        TaskState::Blocked { attempt, .. } => TaskJoinKey {
            attempt: *attempt,
            band: JoinBand::Blocked,
            terminal_rank: TerminalRank::FailedLike,
            version: TaskVersion::default(),
            nonterminal_rank: NonTerminalRank::Pending,
            failedlike: FailedLikeRank::Failed,
            payload_content_hash: 0,
        },
        TaskState::Completed { attempt, .. } => key_completed(*attempt),
        TaskState::Failed {
            version, attempt, ..
        } => key_failed(*attempt, *version, terminal_payload_hash(state)),
        TaskState::Unfulfillable {
            version, attempt, ..
        } => key_unfulfillable(*attempt, *version, terminal_payload_hash(state)),
        TaskState::InvalidTask {
            version, attempt, ..
        } => key_invalid_task(*attempt, *version, terminal_payload_hash(state)),
        TaskState::SkippedAlreadyDone { attempt, .. } => key_skipped(*attempt),
        TaskState::SetupCompleted { attempt, .. } => key_setup_completed(*attempt),
        TaskState::AffineReady { attempt, .. } => key_affine_ready(*attempt),
        TaskState::QueuedAfterLocalDependency {
            version, attempt, ..
        } => key_queued_after_local_dependency(*attempt, *version),
    }
}

// Per-variant key constructors: the ONE spelling of each variant's
// `TaskJoinKey` shape, shared by `task_join_key` (ranking a held
// `TaskState`) and the settled-spill PROBE keys (ranking an incoming
// mutation against a settled — fat-body-evicted — entry, where no
// candidate `TaskState` can be built because the `TaskInfo` is on
// disk). A probe built here is BY CONSTRUCTION identical to the key
// the candidate state would rank to.

pub(super) fn key_pending(attempt: u32, version: TaskVersion) -> TaskJoinKey {
    TaskJoinKey {
        attempt,
        band: JoinBand::NonTerminal,
        terminal_rank: TerminalRank::FailedLike,
        version,
        nonterminal_rank: NonTerminalRank::Pending,
        failedlike: FailedLikeRank::Failed,
        payload_content_hash: 0,
    }
}

pub(super) fn key_in_flight(attempt: u32, version: TaskVersion) -> TaskJoinKey {
    TaskJoinKey {
        attempt,
        band: JoinBand::NonTerminal,
        terminal_rank: TerminalRank::FailedLike,
        version,
        nonterminal_rank: NonTerminalRank::InFlight,
        failedlike: FailedLikeRank::Failed,
        payload_content_hash: 0,
    }
}

pub(super) fn key_completed(attempt: u32) -> TaskJoinKey {
    TaskJoinKey {
        attempt,
        band: JoinBand::Terminal,
        terminal_rank: TerminalRank::Completed,
        // Completed carries no version; the terminal rank already
        // separates it from FailedLike below and InvalidTask above.
        version: TaskVersion::default(),
        nonterminal_rank: NonTerminalRank::Pending,
        failedlike: FailedLikeRank::Failed,
        payload_content_hash: completed_payload_hash(),
    }
}

pub(super) fn key_failed(attempt: u32, version: TaskVersion, payload_hash: u64) -> TaskJoinKey {
    TaskJoinKey {
        attempt,
        band: JoinBand::Terminal,
        terminal_rank: TerminalRank::FailedLike,
        version,
        nonterminal_rank: NonTerminalRank::Pending,
        failedlike: FailedLikeRank::Failed,
        payload_content_hash: payload_hash,
    }
}

pub(super) fn key_unfulfillable(
    attempt: u32,
    version: TaskVersion,
    payload_hash: u64,
) -> TaskJoinKey {
    TaskJoinKey {
        attempt,
        band: JoinBand::Terminal,
        terminal_rank: TerminalRank::FailedLike,
        version,
        nonterminal_rank: NonTerminalRank::Pending,
        failedlike: FailedLikeRank::Unfulfillable,
        payload_content_hash: payload_hash,
    }
}

pub(super) fn key_invalid_task(
    attempt: u32,
    version: TaskVersion,
    payload_hash: u64,
) -> TaskJoinKey {
    TaskJoinKey {
        attempt,
        band: JoinBand::Terminal,
        terminal_rank: TerminalRank::InvalidTask,
        version,
        nonterminal_rank: NonTerminalRank::Pending,
        failedlike: FailedLikeRank::Failed,
        payload_content_hash: payload_hash,
    }
}

pub(super) fn key_skipped(attempt: u32) -> TaskJoinKey {
    TaskJoinKey {
        attempt,
        band: JoinBand::Terminal,
        // The WEAKEST terminal rank: a real terminal for the same hash
        // always wins the join over the spawn-time skip. A skip carries
        // no version; the terminal rank already places it below every
        // other terminal.
        terminal_rank: TerminalRank::SkippedAlreadyDone,
        version: TaskVersion::default(),
        nonterminal_rank: NonTerminalRank::Pending,
        failedlike: FailedLikeRank::Failed,
        payload_content_hash: skipped_payload_hash(),
    }
}

pub(super) fn key_setup_completed(attempt: u32) -> TaskJoinKey {
    TaskJoinKey {
        attempt,
        band: JoinBand::Terminal,
        // A non-competing success-like terminal: a setup-kind task's hash
        // is only ever originated terminal by its in-process executor (it
        // is never worker-dispatched), so no real worker outcome competes
        // for the same hash. It carries no version; the terminal rank
        // places it as the second-weakest terminal (above the spawn-time
        // skip, below every WORK terminal) purely for a total order.
        terminal_rank: TerminalRank::SetupCompleted,
        version: TaskVersion::default(),
        nonterminal_rank: NonTerminalRank::Pending,
        failedlike: FailedLikeRank::Failed,
        payload_content_hash: setup_completed_payload_hash(),
    }
}

pub(super) fn key_affine_ready(attempt: u32) -> TaskJoinKey {
    TaskJoinKey {
        attempt,
        band: JoinBand::Terminal,
        // A non-competing success-like terminal: a SecondaryAffine gate's
        // hash is only ever originated terminal by the primary's ready-
        // resolution hook (it is NEVER worker-dispatched and never
        // re-failed), so no real worker outcome competes for the same hash.
        // It carries no version; the terminal rank places it just above
        // `SetupCompleted` purely for a total order.
        terminal_rank: TerminalRank::AffineReady,
        version: TaskVersion::default(),
        nonterminal_rank: NonTerminalRank::Pending,
        failedlike: FailedLikeRank::Failed,
        payload_content_hash: affine_ready_payload_hash(),
    }
}

pub(super) fn key_queued_after_local_dependency(
    attempt: u32,
    version: TaskVersion,
) -> TaskJoinKey {
    TaskJoinKey {
        attempt,
        band: JoinBand::NonTerminal,
        terminal_rank: TerminalRank::SkippedAlreadyDone,
        version,
        // The non-terminal sub-rank between `Pending` and `InFlight`: a
        // queued task is more committed than a bare Pending but less than a
        // running InFlight, so the release `TaskAssigned` (minting a
        // strictly-higher version) dominates it and a stale redelivery never
        // resurrects an InFlight over it.
        nonterminal_rank: NonTerminalRank::QueuedAfterLocalDependency,
        failedlike: FailedLikeRank::Failed,
        payload_content_hash: 0,
    }
}

/// The PROBE key for an incoming `TaskFailed { kind, error, version,
/// attempt }` mutation: mirrors the apply arm's `ErrorType → discrete
/// variant` translation (Unfulfillable / InvalidTask demultiplex) so the
/// settled consult ranks the mutation exactly as the candidate state the
/// arm would build.
pub(super) fn probe_key_for_failed_mutation(
    kind: &dynrunner_core::ErrorType,
    error: &str,
    version: TaskVersion,
    attempt: u32,
) -> TaskJoinKey {
    match kind {
        dynrunner_core::ErrorType::Unfulfillable { reason } => key_unfulfillable(
            attempt,
            version,
            unfulfillable_payload_hash(reason.as_str(), error),
        ),
        dynrunner_core::ErrorType::InvalidTask { reason } => key_invalid_task(
            attempt,
            version,
            invalid_task_payload_hash(reason.as_str(), error),
        ),
        other => key_failed(attempt, version, failed_payload_hash(&other.wire_value(), error)),
    }
}

/// Does `incoming` WIN the join against `local`? Compares the two keys
/// lexicographically; `incoming` wins iff strictly greater. On a TOTAL
/// tie the payloads are equal by construction (the same logical update
/// redelivered), so `incoming` does NOT win — the idempotent NoOp. This
/// is the SINGLE source of the order; apply, restore, and digest all
/// derive from it.
pub(super) fn task_join_key_dominates(incoming: &TaskJoinKey, local: &TaskJoinKey) -> bool {
    incoming > local
}

/// A hashable `u64` projection of a state's join key, for the digest
/// fold. Derives from the SAME `task_join_key`, so a divergence the merge
/// would heal is one the digest can see (and vice versa).
pub(super) fn hashable_join_key<I>(state: &TaskState<I>) -> u64 {
    let k = task_join_key(state);
    hash_one((
        // `attempt` is prepended (F2) so the digest fold sees a retry
        // reset even at equal band/version: `Failed { attempt: n }` and
        // `Pending { attempt: n+1 }` produce different `tasks_hash`es, so
        // `field_behind` detects the divergence and the heal pulls the
        // higher-attempt state.
        k.attempt,
        k.band as u8,
        k.terminal_rank as u8,
        k.version,
        k.nonterminal_rank as u8,
        k.failedlike as u8,
        k.payload_content_hash,
    ))
}

/// What [`ClusterState::merge_task_state`] did, with exactly the info a
/// caller needs to run side-effects exactly-once. Returned by the ONE
/// join fn; apply's monotone arms, restore's loop, and (read-only) the
/// digest fold consume it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum MergeOutcome {
    /// `incoming` did not win (idempotent / dominated / out-of-order).
    /// No side-effects.
    NoOp,
    /// `incoming` won; carries the post-merge transition flags + the
    /// pre-built event a caller emits.
    Applied {
        /// Post-merge is `Completed` AND pre-merge was NOT `Completed`.
        /// Drives `resume_blocked_on` (TS-2), `record_task_outputs`
        /// (TS-3), and the success event (TS-5).
        newly_completed: bool,
        /// A NON-SUCCESS terminal WON THE JOIN (B1): post-merge is
        /// `Failed`/`Unfulfillable`/`InvalidTask` AND `incoming` strictly
        /// won the dominance compare. Fires on a higher-version
        /// re-failure too (preserving today's re-failure emit cadence)
        /// and is a NoOp (no emit) for an idempotent same-version
        /// re-delivery.
        failure_won: bool,
        /// Pre-built terminal event (`None` for a non-terminal
        /// transition); built once here via `to_completed_event` so apply
        /// and restore emit byte-identical events.
        event: Option<TaskCompletedEvent>,
        /// Pre-built #520 per-transition narration event — ALWAYS `Some`
        /// on a win (every winning transition, terminal or not, is a
        /// narration-worthy CRDT change). Built here from the POST-merge
        /// state's classification + the holder (post-merge for an
        /// assignment, the PRE-merge holder captured below for a terminal
        /// that superseded an `InFlight`), so apply and restore narrate
        /// byte-identically and path-independently. The caller does the
        /// actual `emit_task_state_change_event` (the emit sink is the
        /// caller's concern, exactly like `event`). `Box`ed to keep the
        /// `Applied` variant from dominating `MergeOutcome`'s size
        /// (`large_enum_variant`): every `merge_task_state` return moves
        /// this enum, and the narration event (which carries the holder
        /// strings + the fail reason/last_error) is the largest field.
        state_change_event: Box<TaskStateChangeEvent>,
    },
}

impl<I: Identifier> ClusterState<I> {
    /// The ONE side-effect-bearing per-task join. Every monotone task
    /// transition — apply's `TaskCompleted`/`TaskFailed`/`TaskAssigned`
    /// arms, restore's per-`(hash, incoming)` loop — routes through here
    /// so the supersede precedence is spelled exactly once.
    ///
    /// `incoming_outputs` is the co-present `TaskOutputs` for this hash
    /// (the apply path passes the decoded `result_data`; restore passes
    /// the snapshot's content-hash-keyed cache entry). `resumed` collects
    /// the cross-task `Blocked → Pending` auto-resumes for the caller
    /// (opt-in: the primary's pool-owning path reads it, others discard a
    /// scratch buffer) — see the `restore` / `apply` opt-in splits.
    ///
    /// Side-effects (only on a WIN):
    ///   * `newly_completed` → `record_task_outputs` (TS-3, first-write-
    ///     wins) + `resume_blocked_on` (TS-2, collected into `resumed`).
    ///   * the terminal event is BUILT here from the POST-merge state via
    ///     `to_completed_event` so apply and restore emit identical bytes;
    ///     the CALLER does the actual `emit_task_completed_event` (the
    ///     emit sink is the caller's concern).
    pub(super) fn merge_task_state(
        &mut self,
        hash: &str,
        incoming: TaskState<I>,
        incoming_outputs: Option<TaskOutputs>,
        resumed: &mut Vec<dynrunner_core::TaskInfo<I>>,
    ) -> MergeOutcome {
        // 0. Settled consult: a hash absent from the fat map may be a
        // SETTLED entry (fat body evicted to the spill file, slim index
        // retained). The stored join key answers the dominance question
        // without touching the disk: the common late-duplicate /
        // re-restore NoOps right here; the lattice-allowed (but
        // practically-unreachable for a join fixed-point) dominating
        // incoming REHYDRATES the fat body back into `tasks` first, so
        // the normal occupied-slot path below runs with full fidelity
        // (`was_completed`, tally bump, event build all see the true
        // pre-merge state).
        if !self.tasks.contains_key(hash)
            && self.settled_contains(hash)
            && !self.unsettle_if_dominated(hash, &task_join_key(&incoming))
        {
            return MergeOutcome::NoOp;
        }
        // 1+2. Look up; on a present slot compare keys and bail on a NoOp.
        // Capture the PRE-merge per-entry range term while we hold the old
        // state, so the memo update below can XOR it OUT (the new term goes IN
        // after the insert). `None` (the entry never existed — not fat, not
        // settled) makes this insert a logical CREATE, so no old term to
        // remove. After an `unsettle_if_dominated` rehydrate above the entry
        // is fat here, so its old term is captured and the rehydrate's count
        // (added back by unsettle moving it fat) is correctly conserved.
        let old_range_term = self
            .tasks
            .get(hash)
            .map(|local| super::keyspace::task_digest_term(hash, local));
        let was_completed = match self.tasks.get(hash) {
            None => false,
            Some(local) => {
                let local_key = task_join_key(local);
                let inc_key = task_join_key(&incoming);
                if !task_join_key_dominates(&inc_key, &local_key) {
                    return MergeOutcome::NoOp;
                }
                matches!(local, TaskState::Completed { .. })
            }
        };
        // The PRE-merge holder (`InFlight`/`QueuedAfterLocalDependency`
        // secondary+worker), captured while the old state is still in the
        // slot — for the #520 narration event. A terminal (Completed /
        // Failed) carries no holder of its own, so "task X completed/failed
        // ON which worker" is exactly the holder of the `InFlight` this
        // terminal superseded. `None` if the prior state named no holder
        // (e.g. a Pending → InFlight assignment, whose holder is on the
        // POST-merge state instead).
        let prior_holder = self.tasks.get(hash).and_then(TaskState::holder);
        // Replace the slot with the winning incoming state.
        let post_is_completed = matches!(incoming, TaskState::Completed { .. });
        let post_is_failure_terminal = matches!(
            incoming,
            TaskState::Failed { .. }
                | TaskState::Unfulfillable { .. }
                | TaskState::InvalidTask { .. }
        );
        // Build the event from the POST-merge state BEFORE the move so the
        // projection reads the winning state's fields.
        let newly_completed = post_is_completed && !was_completed;
        // `failure_won` = a non-success terminal won the dominance compare
        // (B1). We are here only because the incoming strictly won, so any
        // non-success terminal incoming qualifies.
        let failure_won = post_is_failure_terminal;
        let event = if newly_completed || failure_won {
            incoming.to_completed_event(hash)
        } else {
            None
        };
        // #520 narration event — built once here from the POST-merge state's
        // classification, ALWAYS on a win (every winning transition is a
        // narration-worthy change). The holder is the POST-merge state's own
        // (an assignment's new `InFlight` holder) when it names one, else the
        // PRE-merge `prior_holder` (a terminal that superseded an `InFlight`
        // — "completed/failed ON which worker"). Built BEFORE `incoming` is
        // moved into the map.
        let state_change_event = Box::new(TaskStateChangeEvent {
            task_id: incoming.task().task_id.clone(),
            change: incoming.to_state_change(),
            holder: incoming.holder().or(prior_holder),
        });
        // F4 per-phase EVENT tally bump (#358) — the SINGLE owner of the
        // bump, exactly here because this join is the one place a terminal
        // OBSERVATION lands on ANY node (originator apply-locally, mirror
        // apply of the per-completion broadcast, and snapshot restore all
        // route through this fn). Bumping with the replicated event itself
        // keeps every mirror's tally exact in REAL TIME — pre-fix the bump
        // lived in the live primary's `note_item_*` and rode only the
        // snapshot/anti-entropy field merge, so a failover winner's
        // `on_phase_end` read a tally lagging its own completed task
        // states by up to one anti-entropy round.
        //
        // Convergence (never double-counts, never overshoots):
        //   * a given winning join key wins AT MOST ONCE per node (an
        //     idempotent redelivery / re-restore NoOps above), so each
        //     locally-observed event bumps exactly once;
        //   * the snapshot's `phase_event_tallies` field max-merges AFTER
        //     restore's per-task merge loop (states-before-fields in
        //     `restore_collecting_resumed` — load-bearing order), so a
        //     count the field merge imports always covers state
        //     transitions already merged, never transitions still to come
        //     in the same snapshot — `max(local_bumps, snapshot_count)`
        //     is then exactly the union coverage, not a double-count;
        //   * a node that missed intermediate events (e.g. restored
        //     `Failed { attempt: 1 }` straight over `Pending`, skipping
        //     the attempt-0 failure) transiently UNDERSHOOTS and the
        //     grow-MAX field merge heals it from the originator's exact
        //     count.
        //
        // EVENT-shaped, mirroring the join's own cadence: `newly_completed`
        // fires at most once per hash; `failure_won` fires per winning
        // failure-terminal observation (a higher-attempt re-failure counts
        // again — B1 re-failure cadence), so a fail → retry → succeed task
        // increments BOTH Failed and Completed. A `SkippedAlreadyDone` is a
        // terminal LEDGER state, not a completion EVENT — no bump (it never
        // routes here as `Completed`/failure-terminal).
        if newly_completed || failure_won {
            let kind = if newly_completed {
                PhaseTally::Completed
            } else {
                PhaseTally::Failed
            };
            let key = (incoming.task().phase_id.clone(), kind);
            let next = self.phase_event_tally_for(&key) + 1;
            self.record_phase_event_tally(key, next);
        }
        // Range-fold memo: XOR the OLD term out (if the slot was occupied)
        // and the NEW winning term in. A `None` old term is a logical CREATE
        // (count bumps); a `Some` is a state CHANGE under a fixed key (count
        // conserved). Done as raw memo ops (not the `range_memo_*` bridges)
        // because the winning `incoming` is moved into the map on the very
        // next line — capture its term first.
        let new_range_term = super::keyspace::task_digest_term(hash, &incoming);
        match old_range_term {
            Some(old) => self.range_fold_memo.swap(hash, old, new_range_term),
            None => self.range_fold_memo.add(hash, new_range_term),
        }
        self.tasks.insert(hash.to_string(), incoming);
        // 4. Newly-completed cross-task side-effects.
        if newly_completed {
            self.record_task_outputs_value(hash, incoming_outputs);
            let just_resumed = self.resume_blocked_on(hash);
            resumed.extend(just_resumed);
        }
        MergeOutcome::Applied {
            newly_completed,
            failure_won,
            event,
            state_change_event,
        }
    }
}

// === primary register ===

/// Equal-epoch LWW adopt rule for the `current_primary` register (D-P /
/// CRD-2), consumed by BOTH apply's `PrimaryChanged` arm and restore's
/// primary branch (wave B wires those callers). Returns `true` iff the
/// incoming `(epoch, id)` should be adopted:
///   * `inc_epoch > local_epoch`, OR
///   * equal epoch AND the incoming id is lexicographically LOWER (a
///     `None` local always loses to a `Some` at equal epoch).
///
/// The lex-lower tie-break matches the election's `lowest_alive` `.min()`
/// convention, so the CRDT register agrees with the leader the election
/// would pick — and BOTH replicas of an equal-epoch split converge to the
/// same id in one round.
pub(super) fn primary_register_adopt(
    local_epoch: u64,
    local_id: Option<&str>,
    inc_epoch: u64,
    inc_id: &str,
) -> bool {
    if inc_epoch > local_epoch {
        return true;
    }
    if inc_epoch < local_epoch {
        return false;
    }
    // Equal epoch: lower id wins; a None local always loses to a Some.
    match local_id {
        None => true,
        Some(local) => inc_id < local,
    }
}

// === phase_deps ===

/// Order-independent canonical hash of the static phase-dependency graph
/// (CRD-3 / D-G), consumed by BOTH restore's deterministic merge and the
/// digest (wave B wires those callers). Sorts the phases and each dep
/// list before folding so two replicas with the same graph in different
/// insertion order produce the same hash, and a divergent-but-equal-count
/// graph produces a DIFFERENT hash (which the count-only digest line
/// could not see).
pub(super) fn canonical_phase_deps_hash(deps: &HashMap<PhaseId, Vec<PhaseId>>) -> u64 {
    let mut entries: Vec<(&PhaseId, Vec<&PhaseId>)> = deps
        .iter()
        .map(|(phase, dep_list)| {
            let mut sorted: Vec<&PhaseId> = dep_list.iter().collect();
            sorted.sort();
            (phase, sorted)
        })
        .collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    hash_one(&entries)
}

// === capabilities ===

/// 2P-set merge of one peer's role capability (C6), GENERATION-FIRST
/// (the re-admission lattice). The SINGLE place the capability lattice
/// join order is spelled, consumed by apply's peer arms (against the
/// local entry) and restore's per-id loop:
///   * `member_gen` dominates FIRST: the strictly-higher-generation
///     entry wins outright, whichever variant it is — a re-admitted
///     member's gen-(N+1) `Advertised` beats the gen-N `Departed`
///     tombstone on EVERY merge path (apply, snapshot restore, digest
///     heal), so a stale replica's snapshot can never re-bury a
///     re-admitted peer's capability; symmetrically a gen-(N+1)
///     tombstone beats a stale gen-N advertise.
///   * AT EQUAL GENERATION (one membership incarnation), the original
///     rules: `Departed ∨ _ = Departed` (the tombstone dominates — a
///     departure cannot be undone by a stale same-generation
///     advertise); `Advertised ∨ Advertised = Advertised { is_observer:
///     a || b (upward ratchet — an observer never un-observes),
///     can_be_primary: <bit of the higher cap_version> (a newer
///     `SetCanBePrimary(false)` beats an older `true`), cap_version:
///     max(a, b) }`. Two same-generation tombstones fold their
///     PRESERVED advertisements by the same `Advertised` rule so
///     divergent tombstones converge deterministically.
///
/// Commutative / associative / idempotent: the generation pick is a
/// max, `Departed` absorbs within a generation, and the advertisement
/// fold is field-wise OR + a max-versioned pick — all order-
/// independent. Returns the merged entry so the caller writes it back
/// into the `capabilities` map.
pub(super) fn merge_capability(
    local: &CapabilityEntry,
    incoming: &CapabilityEntry,
) -> CapabilityEntry {
    // Generation dominates first: the strictly-newer membership
    // incarnation's entry wins outright; equal generations fall through
    // to the within-incarnation rules.
    match capability_member_gen(local).cmp(&capability_member_gen(incoming)) {
        std::cmp::Ordering::Less => return incoming.clone(),
        std::cmp::Ordering::Greater => return local.clone(),
        std::cmp::Ordering::Equal => {}
    }
    match (local, incoming) {
        // Same generation, both tombstones: fold the PRESERVED
        // advertisements (same rule as the Advertised arm below) so two
        // divergent tombstones converge deterministically.
        (
            CapabilityEntry::Departed {
                member_gen,
                is_observer: lo,
                can_be_primary: lc,
                cap_version: lv,
            },
            CapabilityEntry::Departed {
                is_observer: io,
                can_be_primary: ic,
                cap_version: iv,
                ..
            },
        ) => {
            let (is_observer, can_be_primary, cap_version) =
                fold_advertisement(*lo, *lc, *lv, *io, *ic, *iv);
            CapabilityEntry::Departed {
                member_gen: *member_gen,
                is_observer,
                can_be_primary,
                cap_version,
            }
        }
        // Same generation, exactly one tombstone: it absorbs (genuine
        // departure dominates within one membership incarnation).
        (dep @ CapabilityEntry::Departed { .. }, CapabilityEntry::Advertised { .. })
        | (CapabilityEntry::Advertised { .. }, dep @ CapabilityEntry::Departed { .. }) => {
            dep.clone()
        }
        (
            CapabilityEntry::Advertised {
                is_observer: lo,
                can_be_primary: lc,
                cap_version: lv,
                member_gen,
            },
            CapabilityEntry::Advertised {
                is_observer: io,
                can_be_primary: ic,
                cap_version: iv,
                ..
            },
        ) => {
            let (is_observer, can_be_primary, cap_version) =
                fold_advertisement(*lo, *lc, *lv, *io, *ic, *iv);
            CapabilityEntry::Advertised {
                is_observer,
                can_be_primary,
                cap_version,
                member_gen: *member_gen,
            }
        }
    }
}

/// The membership-incarnation generation a capability entry carries —
/// the FIRST-dominating key of [`merge_capability`], read uniformly off
/// either variant.
pub(super) fn capability_member_gen(entry: &CapabilityEntry) -> u64 {
    match entry {
        CapabilityEntry::Advertised { member_gen, .. }
        | CapabilityEntry::Departed { member_gen, .. } => *member_gen,
    }
}

/// The same-generation advertisement fold — the pre-generation
/// `Advertised ∨ Advertised` rule, shared by the live-advertise arm and
/// the tombstone-payload arm of [`merge_capability`] so the two cannot
/// drift: `is_observer` ratchets up (OR — an observer never un-observes,
/// so it needs no version); `can_be_primary` follows the higher
/// `cap_version` (a TOTAL tie keeps `local` — idempotent on the same
/// advertisement redelivered); `cap_version` is the max.
fn fold_advertisement(
    lo: bool,
    lc: bool,
    lv: TaskVersion,
    io: bool,
    ic: bool,
    iv: TaskVersion,
) -> (bool, bool, TaskVersion) {
    let can_be_primary = if iv > lv { ic } else { lc };
    (lo || io, can_be_primary, lv.max(iv))
}

/// Order-independent digest fold over the `capabilities` 2P-set (C6 — the
/// snapshot-healable CRDT, so folding it is detect-WITH-heal, not the R2
/// no-op pull loop). Per-entry hash of `(id, is_observer, can_be_primary,
/// cap_version, is_departed, member_gen)`; the caller XOR-folds these so
/// the result is invariant under iteration order. A `Departed` tombstone
/// folds with its distinct `is_departed` flag AND its preserved
/// advertisement so a node that converged the tombstone differs from one
/// that still holds the `Advertised`, and two divergent same-generation
/// tombstones differ until the merge converges them. `member_gen` is in
/// the fold so a re-admitted (generation-advanced) entry differs from a
/// stale lower-generation one — the heal pull then converges the stale
/// replica through the generation-first [`merge_capability`].
pub(super) fn capability_fold(id: &str, entry: &CapabilityEntry) -> u64 {
    match entry {
        CapabilityEntry::Advertised {
            is_observer,
            can_be_primary,
            cap_version,
            member_gen,
        } => hash_one((
            id,
            *is_observer,
            *can_be_primary,
            *cap_version,
            false,
            *member_gen,
        )),
        CapabilityEntry::Departed {
            member_gen,
            is_observer,
            can_be_primary,
            cap_version,
        } => hash_one((
            id,
            *is_observer,
            *can_be_primary,
            *cap_version,
            true,
            *member_gen,
        )),
    }
}
