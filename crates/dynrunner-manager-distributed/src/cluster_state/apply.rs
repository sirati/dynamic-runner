//! The CRDT `apply` rule — the central dispatch over `ClusterMutation`.
//!
//! Single concern: routing each `ClusterMutation<I>` variant to its
//! per-arm transition logic. The peer-lifecycle arms (`PeerJoined`,
//! `PeerRemoved`, `PeerResourceHoldingsUpdated`) delegate to
//! `apply_peer`; the `TasksSpawned` batch arm delegates to
//! `apply_tasks`; the simpler per-hash transitions (`TaskAdded`,
//! `TaskAssigned`, `TaskCompleted`, `TaskFailed`, `PrimaryChanged`,
//! `PhaseDepsSet`, `RunComplete`, `TaskReinjected`, `TaskBlocked`,
//! `TaskPreferredSecondariesUpdated`) live inline here.
//!
//! Two entry points: the receiver-side `apply` convenience wrapper
//! that discards the auto-resumed list AND the freshly-Pending
//! TasksSpawned surface, and the originator-/receiver-side
//! `apply_with_resumed_blocked` that surfaces (a) auto-resumed
//! `TaskInfo`s for re-injection into the live `PendingPool` and (b)
//! freshly-Pending entries from a `TasksSpawned` batch so a receive-
//! side caller that locally owns a dispatch pool can grow it.

use dynrunner_core::{ErrorType, Identifier, SoftPreferredSecondaries, TaskInfo};
use dynrunner_protocol_primary_secondary::ClusterMutation;

use super::{ApplyOutcome, ClusterState, TaskState};
use crate::task_completed::TaskCompletedEvent;

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
        self.apply_with_resumed_blocked(
            m,
            &mut _resumed_scratch,
            &mut _newly_pending_scratch,
        )
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
        match m {
            ClusterMutation::TaskAdded { hash, task } => {
                if let std::collections::hash_map::Entry::Vacant(e) =
                    self.tasks.entry(hash)
                {
                    e.insert(TaskState::Pending { task });
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::NoOp
                }
            }
            ClusterMutation::TaskAssigned {
                hash,
                secondary,
                worker,
            } => {
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                let task = match state {
                    TaskState::Pending { task } => task.clone(),
                    _ => return ApplyOutcome::NoOp,
                };
                *state = TaskState::InFlight {
                    task,
                    secondary,
                    worker,
                };
                ApplyOutcome::Applied
            }
            ClusterMutation::TaskCompleted { hash, result_data } => {
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                let task = match state {
                    // Idempotent dedup on a redundant TaskCompleted (the
                    // same hash arrives twice via peer-forwarding
                    // redundancy or snapshot replay).
                    TaskState::Completed { .. } => return ApplyOutcome::NoOp,
                    // Retry-success supersedes a prior Recoverable
                    // failure: the retry pass re-injects the binary,
                    // a worker picks it up, and the next TaskCompleted
                    // for the same hash legitimately transitions
                    // Failed → Completed. Pre-fix this branch NoOp'd,
                    // leaving the ledger stuck at `Failed { Recoverable }`
                    // even though the task ultimately succeeded — so
                    // `outcome_counts().succeeded` undercounted the
                    // retry-successes and the run-done logic that reads
                    // it never saw the cluster reach "all terminal as
                    // succeeded". The HashSet-side bookkeeping in
                    // `primary/task.rs::handle_task_complete` already
                    // implements this same supersession; this arm
                    // brings the CRDT into agreement so cross-node
                    // mirrors converge to the right terminal state.
                    //
                    // Commutativity: if peer A observes
                    // (TaskFailed, TaskCompleted) for the same hash and
                    // peer B observes (TaskCompleted, TaskFailed), both
                    // converge to `Completed` — A applies Failed then
                    // transitions to Completed here; B applies Completed
                    // then NoOps the late TaskFailed (the Completed
                    // arm in `TaskFailed` below). Success is the
                    // strongest terminal regardless of arrival order;
                    // the prior `attempts` / `last_error` are dropped
                    // because the cluster's authoritative outcome for
                    // this hash is now success.
                    //
                    // `Unfulfillable` and `Blocked` both yield the same
                    // way: if the run somehow reaches Completed for the
                    // hash (worker raced ahead of the cascade decision,
                    // or external resolver dispatched the binary out-of-
                    // band), success is still the strongest terminal.
                    TaskState::Failed { task, .. }
                    | TaskState::Unfulfillable { task, .. }
                    | TaskState::Blocked { task, .. } => task.clone(),
                    TaskState::Pending { task } | TaskState::InFlight { task, .. } => task.clone(),
                };
                // Snapshot the `task_id` for the dispatcher event
                // BEFORE the move into `TaskState::Completed`; both
                // halves consume distinct fields of the same `task`
                // value, but the dispatcher event lives strictly off
                // the apply path (mpsc enqueue) so capturing the id
                // before the state transition keeps the apply rule
                // observably equivalent to the pre-event-emit
                // baseline.
                let event_task_id = task.task_id.clone();
                *state = TaskState::Completed { task };
                // Cache the wire mutation's `result_data` payload under
                // the completing task's `task_id` so dispatch-time
                // dependent resolution can attach predecessor outputs
                // without re-decoding the wire bytes. Helper lives in
                // `apply_tasks.rs` alongside other task-related apply
                // helpers (`resume_blocked_on`). No-op for `None`
                // payloads (worker did not publish outputs) and for
                // anonymous tasks (no `task_id` to key under).
                self.record_task_outputs(&hash, result_data);
                // Auto-resume: any `Blocked { on: <this hash>, .. }`
                // dependent transitions back to `Pending` so the next
                // dispatch tick on the live primary picks it up. Event-
                // driven (apply-rule-local) rather than retry-pass
                // wall-clock; the same broadcast that converges this
                // hash to Completed converges every blocked dependent
                // to Pending on the same apply call across every
                // replica.
                let just_resumed = self.resume_blocked_on(&hash);
                resumed.extend(just_resumed);
                // Best-effort, non-blocking dispatcher fan-out. See
                // [`Self::emit_task_completed_event`] for the CCD-9
                // contract (apply path never invokes a listener
                // directly).
                self.emit_task_completed_event(TaskCompletedEvent {
                    task_id: event_task_id,
                    task_hash: hash,
                    success: true,
                    error_kind: None,
                });
                ApplyOutcome::Applied
            }
            ClusterMutation::TaskFailed { hash, kind, error } => {
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                // Capture task_id + wire-stable error_kind for the
                // dispatcher event. The id snapshot lives outside the
                // arms below so the emit at the bottom of the match
                // sees a uniform binding regardless of which arm
                // applied. The arms that NoOp return early and never
                // reach the emit; the two "Applied" arms set this to
                // `Some(...)` before exiting via the bottom emit.
                // `task.task_id` is non-optional per the framework's
                // boundary contract; the captured payload is `String`.
                let mut emit_payload: Option<(String, String)> = None;
                let outcome = match state {
                    // Strongest terminals lock out incoming TaskFailed.
                    // `Completed` never regresses; `Unfulfillable` is a
                    // stable reinjectable terminal — a late generic
                    // worker-originated TaskFailed must not regress it.
                    TaskState::Completed { .. }
                    | TaskState::Unfulfillable { .. } => ApplyOutcome::NoOp,
                    TaskState::Failed {
                        task,
                        kind: k,
                        last_error,
                        attempts,
                    } => {
                        emit_payload = Some((task.task_id.clone(), kind.wire_value()));
                        *attempts += 1;
                        *k = kind;
                        *last_error = error;
                        ApplyOutcome::Applied
                    }
                    // Non-terminal states transition based on the
                    // error class: `Unfulfillable` routes to the
                    // discrete state so downstream matcher / reinject
                    // logic can dispatch on the discriminant; every
                    // other `ErrorType` lands in the generic `Failed`
                    // bucket preserving the legacy attempts/last_error
                    // shape.
                    TaskState::Pending { task }
                    | TaskState::InFlight { task, .. }
                    | TaskState::Blocked { task, .. } => {
                        let task = task.clone();
                        emit_payload = Some((task.task_id.clone(), kind.wire_value()));
                        *state = match kind {
                            ErrorType::Unfulfillable { reason } => {
                                TaskState::Unfulfillable {
                                    task,
                                    reason: reason.to_string(),
                                }
                            }
                            other => TaskState::Failed {
                                task,
                                kind: other,
                                last_error: error,
                                attempts: 1,
                            },
                        };
                        ApplyOutcome::Applied
                    }
                };
                // Best-effort, non-blocking dispatcher fan-out — only
                // when the apply actually advanced state (the
                // strongest-terminal NoOp arms above leave
                // `emit_payload = None`). See
                // [`Self::emit_task_completed_event`] for the CCD-9
                // contract.
                if let Some((task_id, error_kind)) = emit_payload {
                    self.emit_task_completed_event(TaskCompletedEvent {
                        task_id,
                        task_hash: hash,
                        success: false,
                        error_kind: Some(error_kind),
                    });
                }
                outcome
            }
            ClusterMutation::PrimaryChanged { new, epoch } => {
                if epoch < self.primary_epoch {
                    return ApplyOutcome::NoOp;
                }
                if epoch == self.primary_epoch && self.current_primary.as_deref() == Some(&new) {
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
                    // Static config: re-application is silent.
                    return ApplyOutcome::NoOp;
                }
                self.phase_deps = deps;
                ApplyOutcome::Applied
            }
            ClusterMutation::RunComplete => {
                if self.run_complete {
                    return ApplyOutcome::NoOp;
                }
                self.run_complete = true;
                ApplyOutcome::Applied
            }
            ClusterMutation::TaskReinjected { hash } => {
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
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                let task = match state {
                    TaskState::Unfulfillable { task, .. } => task.clone(),
                    _ => return ApplyOutcome::NoOp,
                };
                *state = TaskState::Pending { task };
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
                match state {
                    TaskState::Completed { .. }
                    | TaskState::Failed { .. }
                    | TaskState::Unfulfillable { .. }
                    | TaskState::InFlight { .. } => ApplyOutcome::NoOp,
                    TaskState::Blocked { on: existing_on, .. } => {
                        if existing_on == &on {
                            ApplyOutcome::NoOp
                        } else {
                            // First observed cascade root wins; a
                            // divergent re-cascade against the same
                            // dependent is silent.
                            ApplyOutcome::NoOp
                        }
                    }
                    TaskState::Pending { task } => {
                        let task = task.clone();
                        *state = TaskState::Blocked { task, on };
                        ApplyOutcome::Applied
                    }
                }
            }
            ClusterMutation::TaskPreferredSecondariesUpdated { hash, secondaries } => {
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                let task = match state {
                    TaskState::Pending { task }
                    | TaskState::InFlight { task, .. }
                    | TaskState::Completed { task }
                    | TaskState::Failed { task, .. }
                    | TaskState::Unfulfillable { task, .. }
                    | TaskState::Blocked { task, .. } => task,
                };
                task.preferred_secondaries = SoftPreferredSecondaries::new(secondaries);
                ApplyOutcome::Applied
            }
            ClusterMutation::PeerJoined {
                peer_id,
                is_observer,
            } => self.apply_peer_joined(peer_id, is_observer),
            ClusterMutation::PeerRemoved { id, cause } => {
                self.apply_peer_removed(id, cause)
            }
            ClusterMutation::PeerResourceHoldingsUpdated {
                peer_id,
                holdings,
                epoch,
            } => self.apply_peer_resource_holdings_updated(peer_id, holdings, epoch),
            ClusterMutation::TasksSpawned { tasks } => {
                self.apply_tasks_spawned(tasks, newly_pending_from_spawn)
            }
        }
    }
}
