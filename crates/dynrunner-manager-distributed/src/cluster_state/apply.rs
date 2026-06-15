//! The CRDT `apply` rule — the central dispatch over `ClusterMutation`.
//!
//! Single concern: routing each `ClusterMutation<I>` variant to its
//! per-arm transition logic. The peer-lifecycle arms (`PeerJoined`,
//! `PeerRemoved`, `PeerResourceHoldingsUpdated`) delegate to
//! `apply_peer`; the `TasksSpawned` batch arm delegates to
//! `apply_tasks`; the simpler per-hash transitions (`TaskAdded`,
//! `TaskAssigned`, `TaskCompleted`, `TaskFailed`, `PrimaryChanged`,
//! `PhaseDepsSet`, `RunComplete`, `TaskReinjected`, `TaskRequeued`,
//! `TaskBlocked`, `TaskPreferredSecondariesUpdated`) live inline here.
//!
//! Two entry points: the receiver-side `apply` convenience wrapper
//! that discards the auto-resumed list AND the freshly-Pending
//! TasksSpawned surface, and the originator-/receiver-side
//! `apply_with_resumed_blocked` that surfaces (a) auto-resumed
//! `TaskInfo`s for re-injection into the live `PendingPool` and (b)
//! freshly-Pending entries from a `TasksSpawned` batch so a receive-
//! side caller that locally owns a dispatch pool can grow it.

use dynrunner_core::{ErrorType, Identifier, SoftPreferredSecondaries, TaskInfo};
use dynrunner_protocol_primary_secondary::{ClusterMutation, DiscoveryDebt};

use super::merge::MergeOutcome;
use super::{ApplyOutcome, ClusterState, TaskState};

impl<I: Identifier> ClusterState<I> {
    /// Convenience wrapper around [`Self::apply_with_resumed_blocked`]
    /// for callers that don't care which `Blocked` dependents the
    /// apply unblocked OR which freshly-Pending tasks rode in on a
    /// `TasksSpawned` batch.
    ///
    /// Pool-owning callers (live primary's
    /// `apply_and_broadcast_cluster_mutations`, the promoted
    /// secondary's `apply_and_broadcast_mutations`, AND the wire-
    /// receive paths that locally own a dispatch pool) must use
    /// `apply_with_resumed_blocked` so the resumed `TaskInfo`s can be
    /// re-injected into the live `PendingPool` AND so wire-received
    /// `TasksSpawned` mutations grow the pool to match the ledger.
    /// Their original pool entries were dropped by the cascade-fail
    /// in `pool.on_item_failed_permanent` (resumed) or never existed
    /// on this node (wire-received TasksSpawned), and only the CRDT
    /// side has kept them addressable since.
    pub fn apply(&mut self, m: ClusterMutation<I>) -> ApplyOutcome {
        let mut _resumed_scratch: Vec<TaskInfo<I>> = Vec::new();
        let mut _newly_pending_scratch: Vec<TaskInfo<I>> = Vec::new();
        self.apply_with_resumed_blocked(m, &mut _resumed_scratch, &mut _newly_pending_scratch)
    }

    /// Apply a single `ClusterMutation<I>` and, in addition to the
    /// usual [`ApplyOutcome`], surface two derived-view buffers:
    ///
    /// * `resumed` — clones of every `TaskInfo<I>` auto-resumed from
    ///   `Blocked → Pending` by a `TaskCompleted` arm (see
    ///   [`Self::resume_blocked_on`]). Pre-fix the cascade-pause
    ///   primitive dropped the dependent's pool entry; the resumed
    ///   surface lets originator-side callers re-introduce it for
    ///   dispatch.
    /// * `newly_pending_from_spawn` — clones of every `TaskInfo<I>`
    ///   inside a `TasksSpawned` batch whose post-classify state is
    ///   `Pending` (no deps, or all deps already `Completed`).
    ///   Receive-side callers that locally own a dispatch pool use
    ///   this surface to grow the pool so it stays coherent with
    ///   the CRDT ledger. Duplicate-hash entries that NoOp on the
    ///   ledger are NOT surfaced (pool growth is idempotent under
    ///   snapshot retransmission). Cascade-Failed and Blocked entries
    ///   are not surfaced because they don't enter the pool.
    ///
    /// Both buffers are out-parameters rather than part of the return
    /// type: most apply variants append zero items, and the receive-
    /// side callers (which are the only ones that read the list)
    /// prefer to accumulate across a whole mutation batch into a
    /// single buffer before deciding what to do with it.
    pub fn apply_with_resumed_blocked(
        &mut self,
        m: ClusterMutation<I>,
        resumed: &mut Vec<TaskInfo<I>>,
        newly_pending_from_spawn: &mut Vec<TaskInfo<I>>,
    ) -> ApplyOutcome {
        // The apply chokepoint: this entry (and the delegated apply_peer /
        // apply_tasks / apply_custom / merge arms it dispatches to) is the
        // ONLY path a `ClusterMutation` changes a digest-folded field, so
        // clearing the memo once here covers every arm. Unconditional (a
        // NoOp arm still clears) — see `invalidate_digest_cache`.
        self.invalidate_digest_cache();
        match m {
            ClusterMutation::TaskAdded { hash, task } => {
                // Occupied means occupied in the LOGICAL ledger: a SETTLED
                // entry (fat body spilled to disk) is still this hash's
                // ledger entry, and a re-delivered TaskAdded must NoOp
                // against it exactly as against a fat occupied slot — a
                // vacant-insert here would resurrect a terminal to Pending.
                if self.settled_contains(&hash) {
                    return ApplyOutcome::NoOp;
                }
                if self.tasks.contains_key(&hash) {
                    ApplyOutcome::NoOp
                } else {
                    // A vacant slot in the LOGICAL ledger (the `settled_contains`
                    // guard above already excluded a spilled terminal): a
                    // logical CREATE. The shared `set_task_state` write path
                    // inserts, maintains the range-fold memo (`add` — a vacant
                    // slot has no old term, so the bucket count bumps), and
                    // emits the #520 "changed state to pending" narration event
                    // from the post-write state (holder `None` — a spawn-time
                    // Pending names none). No `fallback_holder` (this arm never
                    // supersedes an `InFlight`).
                    let state = TaskState::Pending {
                        task,
                        version: Default::default(),
                        // Brand-new task: the cold generation (F2).
                        attempt: 0,
                    };
                    self.set_task_state(&hash, state, None);
                    ApplyOutcome::Applied
                }
            }
            ClusterMutation::TaskAssigned {
                hash,
                secondary,
                worker,
                version,
                attempt,
            } => {
                // The arm owns the mutation→state translation (its
                // legitimate concern): the assignment carries no TaskInfo,
                // so the candidate `InFlight` reuses the local entry's
                // task. The join then decides whether it wins — under C3 a
                // stale (pre-reset) assignment LOSES to a higher-version
                // requeue/reinject reset within the non-terminal band, so
                // a dead-secondary assignment is never resurrected. The
                // `attempt` (F2, stamped at the choke point from the task's
                // CURRENT generation) is the TOP of the join key, so an
                // assignment for the retried generation out-ranks the
                // `TaskRetried` reset and a stale lower-attempt assignment
                // loses.
                //
                // The lookup consults the settled index via a PROBE key
                // (built from the mutation's scalar fields by the same
                // constructors `task_join_key` uses): a dominated settled
                // entry NoOps right here; a dominating assignment (the
                // out-of-order post-retry re-assignment) rehydrates the fat
                // body first so the candidate carries the true `TaskInfo`.
                let probe = super::merge::key_in_flight(attempt, version);
                let Some(state) = self.task_entry_unsettling(&hash, &probe) else {
                    return ApplyOutcome::NoOp;
                };
                let task = state.task().clone();
                let incoming = TaskState::InFlight {
                    task,
                    secondary,
                    worker,
                    version,
                    attempt,
                };
                self.apply_merge(&hash, incoming, None, resumed)
            }
            ClusterMutation::TaskCompleted {
                hash,
                result_data,
                attempt,
            } => {
                // The arm owns the mutation→state translation: the
                // completion carries no TaskInfo, so the candidate
                // `Completed` reuses the local entry's task. The join then
                // decides — `Completed` is terminal-rank-dominant over
                // `{Failed, Unfulfillable}` and all non-terminals, but
                // LOSES to a local `InvalidTask` (D-T: InvalidTask is the
                // unique TOP), which preserves the InvalidTask lockout. The
                // retry-success supersession (`Failed → Completed`) and the
                // newly-completed side-effects (output cache, auto-resume,
                // event) are all owned by `merge_task_state`. The `attempt`
                // (F2) is carried onto the `Completed` so the completion
                // preserves the generation it completed under.
                //
                // Settled consult via probe key: a late duplicate against a
                // settled terminal NoOps; a genuinely dominating completion
                // (e.g. the retry-pass success superseding a settled
                // Failed-final in a failover race) rehydrates first.
                let probe = super::merge::key_completed(attempt);
                let Some(state) = self.task_entry_unsettling(&hash, &probe) else {
                    return ApplyOutcome::NoOp;
                };
                let task = state.task().clone();
                let outputs = Self::decode_done_payload_outputs(result_data);
                let incoming = TaskState::Completed { task, attempt };
                self.apply_merge(&hash, incoming, outputs, resumed)
            }
            ClusterMutation::TaskFailed {
                hash,
                kind,
                error,
                version,
                attempt,
            } => {
                // The arm owns the mutation→state translation (error class
                // → discrete variant); the join owns the supersede
                // precedence. The candidate terminal carries BOTH the
                // typed body (`reason`) AND the wire message (`last_error`)
                // so a restore-path emit reconstructs the identical
                // `last_error` (TS-4). The join then decides:
                //   * incoming `InvalidTask` SUPERSEDES a local `Completed`
                //     (D-T flip — InvalidTask is the unique TOP);
                //   * a local terminal of equal-or-higher join key locks
                //     out the incoming (e.g. local `InvalidTask` NoOps an
                //     incoming generic `Failed`; an equal-version
                //     `Unfulfillable` NoOps an incoming generic `Failed`);
                //   * a higher-version re-failure WINS (B1 re-failure emit
                //     cadence), an idempotent same-version re-delivery
                //     NoOps (no double-count).
                //
                // Settled consult via probe key (mirrors the arm's own
                // ErrorType → discrete-variant translation): a late
                // duplicate against a settled terminal NoOps; a dominating
                // failure (an incoming InvalidTask over a settled Completed
                // — the D-T flip — or a higher-version re-failure over a
                // settled Failed-final) rehydrates first.
                let probe =
                    super::merge::probe_key_for_failed_mutation(&kind, &error, version, attempt);
                let Some(state) = self.task_entry_unsettling(&hash, &probe) else {
                    return ApplyOutcome::NoOp;
                };
                let task = state.task().clone();
                let incoming = match kind {
                    ErrorType::Unfulfillable { reason } => TaskState::Unfulfillable {
                        task,
                        reason: reason.to_string(),
                        last_error: error,
                        version,
                        attempt,
                    },
                    ErrorType::InvalidTask { reason } => TaskState::InvalidTask {
                        task,
                        reason: reason.to_string(),
                        last_error: error,
                        version,
                        attempt,
                    },
                    other => TaskState::Failed {
                        task,
                        kind: other,
                        last_error: error,
                        version,
                        attempt,
                    },
                };
                self.apply_merge(&hash, incoming, None, resumed)
            }
            // `reason` is advisory routing metadata only; the register
            // adopt rule ("higher epoch wins; equal epoch → lex-lower id
            // wins") is `reason`-blind. CRD-2/D-P: the equal-epoch
            // tie-break (lower id wins, applied identically here and in
            // restore via `primary_register_adopt`) heals the permanent
            // equal-epoch identity split two concurrent `PrimaryChanged`
            // originations would otherwise create — and it agrees with the
            // election's `lowest_alive` `.min()` leader, so the CRDT
            // register names the same primary the election would.
            ClusterMutation::PrimaryChanged { new, epoch, .. } => {
                if !super::merge::primary_register_adopt(
                    self.primary_epoch,
                    self.current_primary.as_deref(),
                    epoch,
                    &new,
                ) {
                    return ApplyOutcome::NoOp;
                }
                self.current_primary = Some(new.clone());
                self.primary_epoch = epoch;
                // Keep the lock-free epoch mirror in lockstep with the
                // field so off-`apply` readers (the observer
                // resource-holdings announcer) see the post-mutation
                // value when their hook is fired below. `Release`
                // pairs with the announcer's `Acquire` load. Writing
                // BEFORE `fire_role_change_hooks` ensures any hook
                // observer that synchronously reads the mirror
                // observes the new value.
                self.primary_epoch_mirror
                    .store(epoch, std::sync::atomic::Ordering::Release);
                // Replicated `RoleTable` mutation: kept in lockstep
                // with `current_primary` so the transport-layer
                // write-through cache (Step 2) observes a coherent
                // snapshot. Hook fires AFTER the field update, so
                // registrants see the post-mutation value.
                self.role_table.primary = Some(new);
                self.fire_role_change_hooks();
                ApplyOutcome::Applied
            }
            ClusterMutation::PhaseDepsSet { deps } => {
                if !self.phase_deps.is_empty() {
                    // Static config: re-application is silent on the SAME
                    // graph (and on a degenerate EMPTY re-set, which is not
                    // a second origination — just a redundant no-op). CRD-3
                    // detection layer: a re-application with a non-empty
                    // DIVERGENT graph is a contract violation (the phase
                    // graph is set-once per run; a genuine second
                    // origination means two primaries minted different
                    // graphs). Flag it LOUDLY (a live cluster never
                    // wedges — the deterministic content-hash reconcile
                    // runs in `restore`, the separate reconciliation
                    // layer; detection here, reconciliation there, sharing
                    // the one `canonical_phase_deps_hash` helper).
                    if !deps.is_empty()
                        && super::merge::canonical_phase_deps_hash(&self.phase_deps)
                            != super::merge::canonical_phase_deps_hash(&deps)
                    {
                        tracing::error!(
                            target: "dynrunner_cluster_state",
                            "PhaseDepsSet re-applied with a DIVERGENT graph — \
                             the per-run phase-dependency graph is set-once; a \
                             second origination with different deps is a contract \
                             violation. Keeping the local graph; anti-entropy \
                             restore reconciles deterministically (lower \
                             content-hash wins)."
                        );
                        debug_assert!(false, "PhaseDepsSet re-applied with a divergent graph");
                    }
                    return ApplyOutcome::NoOp;
                }
                self.phase_deps = deps;
                ApplyOutcome::Applied
            }
            ClusterMutation::PhaseMayBeEmptySet { phases } => {
                // Static config, set-once (mirrors `PhaseDepsSet`): a
                // re-application once the local set is seeded is a no-op
                // (idempotent re-origination / at-least-once replication).
                // An empty incoming set on the common no-opt-out run is the
                // degenerate seed — applying it is harmless (the set stays
                // empty) and keeps the "primary always pairs this with
                // PhaseDepsSet" origination uniform.
                if !self.phase_may_be_empty.is_empty() {
                    return ApplyOutcome::NoOp;
                }
                if phases.is_empty() {
                    return ApplyOutcome::NoOp;
                }
                self.phase_may_be_empty = phases.into_iter().collect();
                ApplyOutcome::Applied
            }
            ClusterMutation::PhaseNoBarrierSet { phases } => {
                // Static config, set-once (mirrors `PhaseMayBeEmptySet`):
                // the no-barrier set is run-constant — the consumer's
                // declared per-phase `PhaseSpec.barrier=False` opt-in is
                // a topology fact that does not change mid-run. A
                // re-application once the local set is seeded is a no-op
                // (idempotent re-origination / at-least-once replication).
                // An empty incoming set on the common all-barriers run is
                // the degenerate seed — applying it is harmless (the set
                // stays empty).
                if !self.phase_no_barrier.is_empty() {
                    return ApplyOutcome::NoOp;
                }
                if phases.is_empty() {
                    return ApplyOutcome::NoOp;
                }
                self.phase_no_barrier = phases.into_iter().collect();
                ApplyOutcome::Applied
            }
            ClusterMutation::RespawnPolicySet {
                max_per_secondary,
                max_total,
                cooldown_ms,
            } => {
                // Static config, set-once (mirrors `PhaseMayBeEmptySet`):
                // the respawn caps are run-constant — originated once by
                // the submitter primary in the seed batch — so a
                // re-application once the local policy is seeded is a
                // no-op (idempotent re-origination / at-least-once
                // replication). First-write-wins; there is no un-set.
                if self.respawn_policy.is_some() {
                    return ApplyOutcome::NoOp;
                }
                self.respawn_policy = Some(super::types::ReplicatedRespawnPolicy {
                    max_per_secondary,
                    max_total,
                    cooldown_ms,
                });
                ApplyOutcome::Applied
            }
            ClusterMutation::RunComplete { counts } => {
                if self.run_complete {
                    return ApplyOutcome::NoOp;
                }
                self.run_complete = true;
                // Latch the verdict's carried counts set-once together with
                // the flag — the atomic latch+counts carriage (same mutation),
                // so observing the verdict means its authoritative counts are
                // in hand with no separate per-task convergence to wait on.
                // First-writer-wins, mirroring `run_aborted` below: a later
                // duplicate verdict NoOps above before ever reaching here, so
                // the guard is belt-and-suspenders for an interleaved abort.
                if self.terminal_outcome.is_none() {
                    self.terminal_outcome = Some(counts);
                }
                ApplyOutcome::Applied
            }
            ClusterMutation::RunAborted { reason, counts } => {
                // Sticky monotonic: the FIRST abort reason wins. A
                // re-applied / duplicate `RunAborted` (at-least-once
                // delivery, a snapshot re-broadcast, or a second abort
                // attempt with a DIFFERENT reason — e.g. the finalize
                // tail's worker-mgmt render after the #3b invalidation
                // already latched the duplicate-identity verdict) is a
                // NoOp so the reason and the latched flag never churn.
                // Mirror of the `RunComplete` arm above — the failure
                // twin. The drop is logged so a swallowed second reason
                // stays diagnosable.
                if let Some(latched) = &self.run_aborted {
                    tracing::debug!(
                        latched = %latched,
                        dropped = %reason,
                        "RunAborted reason already latched \
                         (first-writer-wins); dropping the later abort \
                         reason"
                    );
                    return ApplyOutcome::NoOp;
                }
                self.run_aborted = Some(reason);
                // Latch the carried counts set-once with the abort reason —
                // the verdict's COUNT payload, same atomic carriage as the
                // `RunComplete` arm. First-writer-wins (the `run_aborted`
                // guard above already short-circuits a duplicate).
                if self.terminal_outcome.is_none() {
                    self.terminal_outcome = Some(counts);
                }
                ApplyOutcome::Applied
            }
            ClusterMutation::GracefulAbortRequested => {
                // Sticky monotonic dispatch-freeze latch: `false → true`
                // exactly once; a re-applied / duplicate request (operator
                // re-trigger, at-least-once delivery, snapshot re-broadcast)
                // is a NoOp. Mirror of the `RunComplete` arm above — the
                // graceful sibling of the two run latches.
                if self.graceful_abort_requested {
                    return ApplyOutcome::NoOp;
                }
                self.graceful_abort_requested = true;
                ApplyOutcome::Applied
            }
            ClusterMutation::WindDownRequested {
                secondary_id,
                member_gen,
            } => {
                // Grow-only set-insert of the `(secondary_id, member_gen)`
                // wind-down directive — the per-incarnation sibling of the
                // global `GracefulAbortRequested` latch. A re-applied /
                // duplicate directive (at-least-once delivery, snapshot
                // re-broadcast, AE catch-up) is a NoOp once the pair is
                // recorded; a directive for a DIFFERENT incarnation of the
                // same id (higher `member_gen`) is its own distinct pair, so
                // a stale lower-generation directive can never re-target a
                // re-seated id. The carried generation IS the arbiter (no
                // version stamp), mirroring the `PeerRemoved` per-incarnation
                // rule.
                if self.wind_down_requested.insert((secondary_id, member_gen)) {
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::NoOp
                }
            }
            ClusterMutation::DiscoveryDebtDeclared => {
                // Declare the per-run discovery debt: `Undeclared → Owed`.
                // Sticky-monotone: the declare is Applied ONLY from the
                // lattice BOTTOM (`Undeclared`). From `Owed` (already
                // declared) or `Settled` (already done) it is a NoOp — the
                // monotonicity lives HERE (in the apply rule), not on the
                // wire, so a reordered/redelivered `Declared` that arrives
                // AFTER `Settled` can NEVER drag the run back down. This is
                // exactly the case the distinct `Undeclared` bottom exists to
                // disambiguate from a cold replica's first declare.
                if self.discovery_debt != DiscoveryDebt::Undeclared {
                    return ApplyOutcome::NoOp;
                }
                self.discovery_debt = DiscoveryDebt::Owed;
                ApplyOutcome::Applied
            }
            ClusterMutation::DiscoverySettled => {
                // Ratchet the per-run discovery debt to `Settled` (the
                // lattice TOP): `Applied` iff it CHANGED the local value,
                // else a NoOp. Once `Settled` it never reverts, so a
                // duplicate / re-broadcast `DiscoverySettled` is idempotent.
                // (Settling directly from `Undeclared` is also valid — the
                // join only moves UP.)
                if self.discovery_debt == DiscoveryDebt::Settled {
                    return ApplyOutcome::NoOp;
                }
                self.discovery_debt = DiscoveryDebt::Settled;
                ApplyOutcome::Applied
            }
            ClusterMutation::TaskReinjected { hash, version } => {
                // External-control reinjection moves a
                // `Unfulfillable { .. }` entry back to `Pending`. Any
                // other state is a NoOp so out-of-order delivery and
                // post-completion re-applies can't regress the ledger.
                //
                // Tightened from the pre-variant matcher
                // (`Failed { NonRecoverable, .. }`) in lockstep with
                // `apply_reinject_task` in the command channel: the
                // operator-resolvable-failure class now has its own
                // discrete state, so the apply rule rejects anything
                // outside it.
                //
                // A reset is an authoritative rank-DROP (NOT a monotone
                // join), so it keeps its explicit precondition and does
                // NOT route through `merge_task_state`. It writes the
                // primary-stamped `version` (strictly higher than the
                // pre-reset state's, via the monotone choke point) onto the
                // new `Pending` so a late stale assignment cannot
                // resurrect (C3).
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                // Preserve the source attempt (F2): a reinject is a
                // within-generation rank-DROP, not a new retry attempt, so
                // the attempt is unchanged — the same generation continues
                // (mirrors `TaskRequeued`). `attempt` is read BEFORE the
                // `task.clone()` move so both come off the same borrow.
                let (task, attempt) = match state {
                    TaskState::Unfulfillable { task, attempt, .. } => (task.clone(), *attempt),
                    _ => return ApplyOutcome::NoOp,
                };
                // Memo-maintaining in-place rewrite (XOR old term out, new in;
                // count conserved). The precondition borrow above has ended.
                self.rewrite_task_state(
                    &hash,
                    TaskState::Pending {
                        task,
                        version,
                        attempt,
                    },
                );
                ApplyOutcome::Applied
            }
            ClusterMutation::TaskRequeued { hash, version } => {
                // Dead-secondary recovery moves an `InFlight { .. }` OR a
                // `QueuedAfterLocalDependency { .. }` entry back to `Pending`
                // so the live primary re-dispatches it (and a post-failover
                // hydrate routes it into the pool rather than the in-flight
                // ledger). BOTH are active assignments to the dead secondary:
                // an `InFlight` task it was running, and a
                // `QueuedAfterLocalDependency` task it had committed to but
                // was holding behind a local import (#497) — when that
                // secondary dies, the queued task is just as lost as the
                // running one and re-routes to another secondary per #495.
                // The death seam (`recover_inflight_for_dead_secondary`)
                // emits ONE `TaskRequeued` for both source states (the work
                // task stays in the primary's `in_flight` ledger throughout
                // the queued window), so this arm is the single requeue
                // originator — no second InFlight-originator is invented.
                //
                // Any other state is a NoOp:
                //   * a terminal (`Completed` / `Failed` /
                //     `Unfulfillable` / `InvalidTask`) that arrived
                //     first wins — a worker outcome that raced the
                //     death observation must not be resurrected;
                //   * `Pending` is idempotent under at-least-once
                //     delivery;
                //   * `Blocked` is a cascade-pause, not a dispatched
                //     task — there is nothing in flight to requeue.
                // The preserved `TaskInfo` is moved into the new
                // `Pending` so re-dispatch carries the same binary.
                // Authoritative rank-DROP (see TaskReinjected): kept
                // outside the join with its explicit precondition; writes
                // the stamped `version` onto the new `Pending` so a
                // redelivered stale `TaskAssigned` (lower version) cannot
                // resurrect the dead-secondary assignment (C3).
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                // Preserve the source attempt (F2): a requeue is a
                // within-generation rank-DROP (the C3 non-regression note
                // pins this — a requeue does NOT bump attempt; the same
                // dispatch generation re-dispatches, so version still
                // arbitrates a redelivered stale `TaskAssigned`).
                let (task, attempt) = match state {
                    TaskState::InFlight { task, attempt, .. }
                    | TaskState::QueuedAfterLocalDependency { task, attempt, .. } => {
                        (task.clone(), *attempt)
                    }
                    _ => return ApplyOutcome::NoOp,
                };
                // Memo-maintaining in-place rewrite (XOR old term out, new in).
                self.rewrite_task_state(
                    &hash,
                    TaskState::Pending {
                        task,
                        version,
                        attempt,
                    },
                );
                ApplyOutcome::Applied
            }
            // The F2 retry reset: `Failed { attempt: n } → Pending {
            // attempt: n+1 }`. Sibling to `TaskRequeued`/`TaskReinjected`
            // (an authoritative rank-DROP that does NOT route through the
            // monotone `merge_task_state` join — a dominance comparator
            // would reject a band-crossing drop). The F2-β gate is
            // `Failed`-ONLY: any other source state is a NoOp, so the reset
            // cannot resurrect a `Completed`/`InvalidTask`/`Unfulfillable`/
            // `InFlight`/`Pending`/`Blocked` task, and re-application is
            // silent (the source is no longer `Failed`). The `attempt` was
            // computed by the originator (`old.attempt + 1`, via the
            // `Failed`-only read-side gate); the apply rule trusts it and
            // writes it onto the new `Pending`. The bumped `attempt` is the
            // TOP of the join key, so this reset out-ranks the prior
            // `Failed { attempt: n }` across EVERY merge path including
            // `restore`/anti-entropy — the orphan-revert the naive (no-
            // attempt) `Failed → Pending` arm suffers is structurally
            // impossible. The stamped `version` rides onto the `Pending`
            // too so a late stale assignment within the new generation
            // cannot resurrect (C3, preserved one level down).
            ClusterMutation::TaskRetried {
                hash,
                attempt,
                version,
            } => {
                // Settled consult via probe key: the bumped `attempt` is
                // the TOP of the join key, so a reset against a settled
                // `Failed`-final REHYDRATES it (attempt n+1 dominates
                // attempt n across the band boundary) and the `Failed`-only
                // gate below then applies exactly as on a fat entry. A
                // reset whose probe does NOT dominate the settled entry is
                // a stale redelivery and NoOps. (The retry buckets never
                // TARGET a settle-eligible kind — Recoverable/OOM stay fat
                // — so this arm's rehydrate is the defensive lattice path,
                // not a steady-state cost.)
                let probe = super::merge::key_pending(attempt, version);
                if self.task_entry_unsettling(&hash, &probe).is_none() {
                    return ApplyOutcome::NoOp;
                }
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                let task = match state {
                    TaskState::Failed { task, .. } => task.clone(),
                    _ => return ApplyOutcome::NoOp,
                };
                // Memo-maintaining in-place rewrite. After the rehydrate above
                // the entry is fat (and stayed counted in the memo as the
                // settled entry), so this swap conserves its count.
                self.rewrite_task_state(
                    &hash,
                    TaskState::Pending {
                        task,
                        version,
                        attempt,
                    },
                );
                ApplyOutcome::Applied
            }
            ClusterMutation::TaskBlocked { hash, on } => {
                // Cascade-paused dependent: transition Pending →
                // Blocked, preserving the TaskInfo so auto-resume can
                // re-dispatch the same binary. Idempotent under
                // re-application against a `Blocked` entry whose `on`
                // matches; mismatched `on` (peer A says blocked-on-X,
                // peer B has blocked-on-Y) keeps local — the first
                // observed cascade root wins. Terminal states
                // (Completed, Failed, Unfulfillable) and an active
                // dispatch (InFlight) lock out the cascade decision:
                // a late TaskBlocked must not regress a worker's
                // observed outcome.
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                // The winning transition's new state (built from the cloned
                // precondition), or `None` for a NoOp arm. The `state` borrow
                // ends with the match; the memo-maintaining rewrite runs after.
                let new_state = match state {
                    TaskState::Completed { .. }
                    | TaskState::Failed { .. }
                    | TaskState::Unfulfillable { .. }
                    | TaskState::InvalidTask { .. }
                    // A skip is terminal: it locks out a late cascade-pause
                    // exactly like the other terminals (a TaskBlocked must
                    // not regress an already-done item to Blocked). A
                    // succeeded setup task is terminal for the same reason,
                    // and so is a resolved SecondaryAffine gate (AffineReady).
                    | TaskState::SkippedAlreadyDone { .. }
                    | TaskState::SetupCompleted { .. }
                    | TaskState::AffineReady { .. }
                    // An active assignment locks out a late cascade-pause:
                    // `InFlight` and `QueuedAfterLocalDependency` are both a
                    // worker's observed assignment that a stale TaskBlocked
                    // must not regress.
                    | TaskState::QueuedAfterLocalDependency { .. }
                    | TaskState::InFlight { .. } => None,
                    TaskState::Blocked { .. } => {
                        // Already blocked: idempotent on a matching `on`,
                        // and the first observed cascade root wins on a
                        // divergent one — a re-cascade against the same
                        // dependent is silent either way.
                        None
                    }
                    TaskState::Pending { task, attempt, .. } => {
                        // Preserve the generation across the cascade-pause
                        // (F2): a Blocked dependent re-dispatches at the
                        // same attempt it was Pending under.
                        let attempt = *attempt;
                        let task = task.clone();
                        Some(TaskState::Blocked { task, on, attempt })
                    }
                };
                match new_state {
                    Some(new) => {
                        self.rewrite_task_state(&hash, new);
                        ApplyOutcome::Applied
                    }
                    None => ApplyOutcome::NoOp,
                }
            }
            ClusterMutation::PhaseEnded { phase } => {
                // Grow-only per-phase "end edge completed" fact (#343):
                // set-insert, join = OR. `Applied` iff the phase was not
                // yet recorded; a re-applied / redelivered `PhaseEnded` is
                // a NoOp (idempotent under at-least-once delivery), and no
                // transition ever removes a phase (sticky — the no-redo
                // decision must never regress to re-firing a hook that
                // already fired, #326).
                if self.phases_ended.insert(phase) {
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::NoOp
                }
            }
            ClusterMutation::TaskSkippedAlreadyDone { hash } => {
                // Discovery-time skip: materialize the ledger entry DIRECTLY
                // terminal. Authoritative spawn-time transition (like the
                // resets, NOT a monotone join), so it keeps its explicit
                // precondition arm and does NOT route through
                // `merge_task_state`. Gate on `Pending` ONLY: an in-flight
                // assignment or a real terminal (the weakest-terminal lockout)
                // wins, so a late/out-of-order skip can never overwrite real
                // progress. Idempotent — a re-applied skip against an
                // already-`SkippedAlreadyDone` entry is the `_ => NoOp` arm.
                // The `attempt` is preserved from the `Pending` source.
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                let new_state = match state {
                    TaskState::Pending { task, attempt, .. } => {
                        let (task, attempt) = (task.clone(), *attempt);
                        Some(TaskState::SkippedAlreadyDone { task, attempt })
                    }
                    _ => None,
                };
                match new_state {
                    Some(new) => {
                        // Memo-maintaining in-place rewrite (the borrow ended).
                        self.rewrite_task_state(&hash, new);
                        ApplyOutcome::Applied
                    }
                    None => ApplyOutcome::NoOp,
                }
            }
            ClusterMutation::SetupCompleted { hash } => {
                // A setup task SUCCEEDED in its in-process executor.
                // Authoritative in-process transition (like
                // `TaskSkippedAlreadyDone`, NOT a monotone join), so it keeps
                // its explicit precondition arm and does NOT route through
                // `merge_task_state`. Gate on `InFlight` (the executor was
                // assigned via the standard `TaskAssigned` `Pending →
                // InFlight`) OR `Pending` (a not-yet-assigned originate, or a
                // failover-replayed seed): a real terminal or `Blocked` state
                // is the weakest-terminal lockout, so a late/out-of-order
                // setup success can never overwrite real progress. Idempotent
                // — a re-applied success against an already-`SetupCompleted`
                // entry is the `_ => NoOp` arm. The `attempt` is preserved
                // from the source.
                //
                // Unlike `TaskSkippedAlreadyDone`, a setup SUCCESS must
                // auto-resume its `Blocked` dependents: a build task gated on
                // this setup task (`TaskDep`) sits `Blocked { on: <hash> }`
                // and unblocks the moment the setup task succeeds (the
                // setup-task primitive's overlapping-dependent design). Reuse
                // the SAME `resume_blocked_on` cascade the `TaskCompleted` arm
                // runs — the single owner of the Blocked → Pending resume —
                // and surface the resumed tasks on the SAME `resumed` sink the
                // originator-side caller drains into its live pool.
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                let new_state = match state {
                    TaskState::InFlight { task, attempt, .. }
                    | TaskState::Pending { task, attempt, .. } => {
                        let (task, attempt) = (task.clone(), *attempt);
                        Some(TaskState::SetupCompleted { task, attempt })
                    }
                    _ => None,
                };
                match new_state {
                    Some(new) => {
                        // Memo-maintaining in-place rewrite, THEN the cascade
                        // resume (itself memo-maintained on the resume path).
                        self.rewrite_task_state(&hash, new);
                        let just_resumed = self.resume_blocked_on(&hash);
                        resumed.extend(just_resumed);
                        ApplyOutcome::Applied
                    }
                    None => ApplyOutcome::NoOp,
                }
            }
            ClusterMutation::AffineReady { hash } => {
                // A SecondaryAffine gate `I` became dependency-SATISFIED (all
                // its own deps resolved while `Pending`). READY-not-EXECUTED:
                // the primary NEVER runs the gate — it just transitions the
                // ledger entry to the terminal `AffineReady` and unblocks the
                // gate's dependents. Authoritative spawn-time transition
                // (like `TaskSkippedAlreadyDone` / `SetupCompleted`, NOT a
                // monotone join), so it keeps an explicit precondition arm.
                //
                // Gate on `Pending` ONLY (the load-bearing tightening vs.
                // `SetupCompleted`, which also accepts `InFlight`): a gate is
                // NEVER worker-dispatched, so it can never legitimately be
                // `InFlight` — the only state from which it goes ready is the
                // `Pending` it was born / resumed into. Any non-`Pending`
                // state — a real terminal (the weakest-terminal lockout) or
                // an idempotent re-application against an already-`AffineReady`
                // entry — is the `_ => NoOp` arm.
                //
                // Like `SetupCompleted`, the ready transition auto-resumes
                // every `Blocked { on: <this hash> }` dependent back to
                // `Pending` via the SAME `resume_blocked_on` cascade (the
                // single owner of the Blocked → Pending resume): a build task
                // gated on the gate unblocks the moment the gate is ready.
                // The `attempt` is preserved from the `Pending` source.
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                let new_state = match state {
                    TaskState::Pending { task, attempt, .. } => {
                        // Diagnosability (#514): on the ACTUAL `Pending →
                        // AffineReady` transition (this `Pending` arm) — the one
                        // edge every emission surface (seed / live delta /
                        // post-seed dispatch) converges through — log the gate's
                        // CONTENT-hash + identity. An idempotent re-apply against
                        // an already-`AffineReady` entry takes the `_ => None`
                        // arm below and is a silent NoOp, so anti-entropy /
                        // snapshot-restore / gossip re-delivery never re-log
                        // (no convergence-point spam). The consumption side logs
                        // the hash it is LOOKING FOR when a gate cannot be
                        // resolved, so an operator greps both and tells an ABSENT
                        // gate (this line never fired for the hash) from a
                        // HASH-MISMATCH (it fired for a DIFFERENT hash) — they
                        // need different remediation.
                        tracing::info!(
                            gate_content_hash = %hash,
                            phase = %task.phase_id,
                            task_id = %task.task_id,
                            "SecondaryAffine gate EMITTED AffineReady (resolved \
                             to READY-not-EXECUTED)"
                        );
                        let (task, attempt) = (task.clone(), *attempt);
                        Some(TaskState::AffineReady { task, attempt })
                    }
                    _ => None,
                };
                match new_state {
                    Some(new) => {
                        // Memo-maintaining in-place rewrite, THEN the cascade
                        // resume (itself memo-maintained on the resume path).
                        self.rewrite_task_state(&hash, new);
                        let just_resumed = self.resume_blocked_on(&hash);
                        resumed.extend(just_resumed);
                        ApplyOutcome::Applied
                    }
                    None => ApplyOutcome::NoOp,
                }
            }
            ClusterMutation::QueuedAfterLocalDependencySet { hash, secondary } => {
                // Work task `B` (assigned to `secondary`) is now WAITING on
                // that secondary's LOCAL SecondaryAffine import (#497). The
                // secondary REPORTED (its `TaskQueuedAfterLocalDependency`
                // frame), the primary ORIGINATES this transition (the
                // work-split law). Authoritative rank-DROP `InFlight |
                // Pending → QueuedAfterLocalDependency` (an active assignment
                // is parked behind a local dep), so it keeps an explicit
                // precondition arm and does NOT route through the monotone
                // join (the join would reject the `InFlight → Queued` rank
                // drop). The `TaskInfo`, `version`, and `attempt` are
                // PRESERVED from the source so the eventual release
                // `TaskAssigned` (minting a strictly-higher version) cleanly
                // dominates this queued entry and a stale redelivery never
                // resurrects an `InFlight` over it.
                //
                // Gate on `InFlight` (the standard just-assigned source) OR
                // `Pending` (a deferred-assignment race where the report
                // outran the local `TaskAssigned` apply): any other state — a
                // terminal that already settled `B`, or an idempotent
                // re-application against an already-queued entry — is the `_
                // => NoOp` arm.
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                let (task, version, attempt) = match state {
                    TaskState::InFlight {
                        task,
                        version,
                        attempt,
                        ..
                    }
                    | TaskState::Pending {
                        task,
                        version,
                        attempt,
                        ..
                    } => (task.clone(), *version, *attempt),
                    _ => return ApplyOutcome::NoOp,
                };
                // Memo-maintaining in-place rewrite (the borrow ended).
                self.rewrite_task_state(
                    &hash,
                    TaskState::QueuedAfterLocalDependency {
                        task,
                        secondary,
                        version,
                        attempt,
                    },
                );
                ApplyOutcome::Applied
            }
            ClusterMutation::TaskPreferredSecondariesUpdated {
                hash,
                secondaries,
                version,
            } => {
                // Preferred metadata is a `TaskInfo`-level concern (R4),
                // NOT a state transition: it mutates `preferred_secondaries`
                // in place on EVERY variant under a fixed ledger key. It
                // does NOT route through `merge_task_state` (which keys on
                // the variant-level join); instead it keeps the higher
                // `preferred_version` so two concurrent updates converge
                // regardless of the task's state.
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                let task = state.task_mut();
                if version <= task.preferred_version {
                    // A stale (or idempotent re-delivered) update loses to
                    // the already-recorded higher-or-equal version.
                    return ApplyOutcome::NoOp;
                }
                task.preferred_secondaries = SoftPreferredSecondaries::new(secondaries);
                task.preferred_version = version;
                ApplyOutcome::Applied
            }
            ClusterMutation::PeerJoined {
                peer_id,
                is_observer,
                can_be_primary,
                cap_version,
                member_gen,
            } => self.apply_peer_joined(peer_id, is_observer, can_be_primary, cap_version, member_gen),
            ClusterMutation::SetCanBePrimary {
                peer_id,
                can_be_primary,
                cap_version,
            } => self.apply_set_can_be_primary(peer_id, can_be_primary, cap_version),
            ClusterMutation::PeerRemoved { id, cause, member_gen } => {
                self.apply_peer_removed(id, cause, member_gen)
            }
            ClusterMutation::PeerResourceHoldingsUpdated {
                peer_id,
                holdings,
                epoch,
            } => self.apply_peer_resource_holdings_updated(peer_id, holdings, epoch),
            ClusterMutation::SecondaryCapacity {
                secondary,
                worker_count,
                resources,
            } => self.apply_secondary_capacity(secondary, worker_count, resources),
            ClusterMutation::SecondaryResourceSample { secondary, record } => {
                self.apply_secondary_resource_sample(secondary, record)
            }
            ClusterMutation::TasksSpawned { tasks } => {
                self.apply_tasks_spawned(tasks, newly_pending_from_spawn)
            }
            ClusterMutation::CustomMessagePosted {
                origin,
                seq,
                topic,
                data,
            } => self.apply_custom_message_posted(origin, seq, topic, data),
            ClusterMutation::CustomMessageHandled { origin, seq } => {
                self.apply_custom_message_handled(origin, seq)
            }
            ClusterMutation::CustomMessageFailed {
                origin,
                seq,
                reason,
            } => self.apply_custom_message_failed(origin, seq, reason),
        }
    }

    /// Apply-side adapter over the shared [`Self::merge_task_state`] join:
    /// run the join, emit the pre-built terminal completion event on a win
    /// (the emit SINK is the caller's concern — `merge_task_state` only BUILDS
    /// that event so apply and restore emit byte-identical bytes), and map the
    /// [`MergeOutcome`] onto the [`ApplyOutcome`] the apply arms return. The
    /// #520 narration event is NOT emitted here — `merge_task_state` writes
    /// the winning state through the shared `set_task_state` path, which emits
    /// it (the forget-proof single write path). One seam so the monotone arms
    /// never re-spell the supersede precedence nor the emit.
    fn apply_merge(
        &mut self,
        hash: &str,
        incoming: TaskState<I>,
        incoming_outputs: Option<dynrunner_core::TaskOutputs>,
        resumed: &mut Vec<TaskInfo<I>>,
    ) -> ApplyOutcome {
        match self.merge_task_state(hash, incoming, incoming_outputs, resumed) {
            MergeOutcome::NoOp => ApplyOutcome::NoOp,
            MergeOutcome::Applied { event, .. } => {
                if let Some(ev) = event {
                    self.emit_task_completed_event(ev);
                }
                // #520: the winning transition's narration event is emitted by
                // the shared `set_task_state` write path inside
                // `merge_task_state` (the forget-proof single write path), so
                // this seam emits only the terminal-completion event above.
                ApplyOutcome::Applied
            }
        }
    }
}
