//! Cross-thread / cross-async-runtime ingress for "from outside the
//! operational loop, please apply this mutation to the running primary".
//!
//! Single concern: a typed, reply-bearing command channel whose receiver
//! is read inside the primary's operational-loop `select!` and whose
//! sender is cloned out to consumers (PyO3 `PrimaryHandle`, future
//! Rust-side control-plane callers). Each command carries a
//! `oneshot::Sender<Result<...>>` so the caller can block / await the
//! handler's outcome and surface success / failure synchronously.
//!
//! Module boundary:
//!   * Owns the `PrimaryCommand<I>` enum and the handler entry
//!     `handle_primary_command`. The handlers themselves dispatch back
//!     into `PrimaryCoordinator` methods (`apply_fail_permanent`,
//!     `apply_reinject_task`, `apply_update_preferred_secondaries`) so
//!     each mutation's *implementation* stays co-located with the rest
//!     of the coordinator's state-machine semantics.
//!   * The operational loop's `select!` arm (`lifecycle.rs`) calls
//!     `handle_primary_command(self, cmd).await` — single line, no
//!     per-variant logic at the call site.
//!
//! What callers see (Python and Rust):
//!   * `mpsc::Sender<PrimaryCommand<I>>` — clone, build a command +
//!     `oneshot::channel()`, `send().await`, then `await` the reply.
//!   * `oneshot::Sender::send`-side error paths on the handler side
//!     are non-fatal: a dropped `reply` receiver just means the caller
//!     stopped caring (e.g. timed out, panicked). No coordinator state
//!     change rolls back on `reply.send(...)` failing.
//!
//! Capacity: the inbound channel is bounded (`COMMAND_CHANNEL_CAPACITY`)
//! so a runaway caller can't OOM the primary. Backpressure surfaces to
//! the sender side as a slow `send().await`; the handler-side reply
//! oneshot is the per-command flow-control signal.
//!
//! # Wire / CRDT effects
//!
//! Each handler routes through the same
//! `apply_and_broadcast_cluster_mutations` primitive the rest of the
//! coordinator uses, so the live primary's local apply and the cluster-
//! wide CRDT broadcast happen together. Variants:
//!   * `FailPermanent` — drives `pending_pool::on_item_failed_permanent`
//!     (with the cascade-to-dependents semantics that primitive owns)
//!     and broadcasts `TaskFailed { kind: NonRecoverable, .. }`.
//!   * `ReinjectTask` — accepts only entries whose CRDT state is the
//!     discrete `TaskState::Unfulfillable { .. }` variant (the
//!     operator-resolvable-failure class); flips the local pool's
//!     Unfulfillable → re-injected, broadcasts `TaskReinjected{hash}`,
//!     and decrements the per-task budget
//!     `unfulfillable_reinject_remaining[hash]` (initialised from
//!     `PrimaryConfig::unfulfillable_reinject_max_per_task`; `None`
//!     means unbounded). Budget exhaustion is a structured-log event,
//!     never a panic.
//!   * `UpdatePreferredSecondaries` — broadcasts
//!     `TaskPreferredSecondariesUpdated{hash, secondaries}` so every
//!     node's mirror sees the same update. Local `TaskInfo`-side
//!     storage of the field lands in Phase 4; the command-variant +
//!     reply path are in place today so the PyO3 surface can ship.

use dynrunner_core::{ErrorType, Identifier};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, PeerTransport, SecondaryTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::oneshot;

use crate::cluster_state::TaskState;
use super::PrimaryCoordinator;

/// Bounded capacity for the command channel. Sized so a noisy caller
/// can't OOM the primary while still giving multi-command batches
/// (e.g. a control-plane that emits N `UpdatePreferredSecondaries`
/// commands in a tight loop) some slack before backpressure kicks in.
pub const COMMAND_CHANNEL_CAPACITY: usize = 256;

/// One in-flight command on the `PrimaryHandle` → coordinator channel.
///
/// No `I: Identifier` parameter: every variant addresses a task by
/// its content hash (`String`) — the same wire-canonical key the rest
/// of the CRDT path uses — so the channel doesn't have to be typed by
/// the task identifier. Cheap to construct; the `reply` oneshot is the
/// per-call flow-control signal.
pub enum PrimaryCommand {
    /// Apply `pending_pool::on_item_failed_permanent` + cascade for the
    /// named hash and broadcast `ClusterMutation::TaskFailed{
    /// NonRecoverable, error }`.
    ///
    /// Error path: unknown hash → `Err(...)`. The handler's `reply`
    /// carries the result; the coordinator's own state is unchanged on
    /// the error arm.
    FailPermanent {
        hash: String,
        error: ErrorType,
        reason: String,
        reply: oneshot::Sender<Result<(), String>>,
    },

    /// External-control reinjection: accept iff the named hash is in
    /// `TaskState::Unfulfillable { .. }` (the discrete state for the
    /// operator-resolvable-failure class) and there's at least one
    /// reinjection ticket left in `unfulfillable_reinject_remaining[hash]`.
    /// On accept, transition Unfulfillable→Pending and broadcast
    /// `ClusterMutation::TaskReinjected{ hash }`. On budget exhaustion,
    /// emit the `unfulfillable_reinject_budget_exhausted` structured
    /// log event and return `Err` to the caller — the local state
    /// stays `Unfulfillable` (no regression).
    ReinjectTask {
        hash: String,
        reply: oneshot::Sender<Result<(), String>>,
    },

    /// External-control update of the named task's preferred-secondaries
    /// list. Broadcasts `ClusterMutation::TaskPreferredSecondariesUpdated`
    /// so every node's mirror picks up the new preference list; the
    /// Phase-4 `TaskInfo.preferred_secondaries` storage owns the
    /// in-memory side once it lands.
    UpdatePreferredSecondaries {
        hash: String,
        secondaries: Vec<String>,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

/// Dispatch one received command to its handler. Single line at the
/// `select!` call site keeps the operational-loop's match arm
/// transport-shape-pure.
pub(super) async fn handle_primary_command<T, P, S, E, I>(
    coordinator: &mut PrimaryCoordinator<T, P, S, E, I>,
    command: PrimaryCommand,
) where
    T: SecondaryTransport<I>,
    P: PeerTransport<I>,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    match command {
        PrimaryCommand::FailPermanent {
            hash,
            error,
            reason,
            reply,
        } => {
            let result = coordinator
                .apply_fail_permanent(hash, error, reason)
                .await;
            let _ = reply.send(result);
        }
        PrimaryCommand::ReinjectTask { hash, reply } => {
            let result = coordinator.apply_reinject_task(hash).await;
            let _ = reply.send(result);
        }
        PrimaryCommand::UpdatePreferredSecondaries {
            hash,
            secondaries,
            reply,
        } => {
            let result = coordinator
                .apply_update_preferred_secondaries(hash, secondaries)
                .await;
            let _ = reply.send(result);
        }
    }
}

impl<T, P, S, E, I> PrimaryCoordinator<T, P, S, E, I>
where
    T: SecondaryTransport<I>,
    P: PeerTransport<I>,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    /// Resolve a task hash through the CRDT ledger and return
    /// `(phase_id, task_id)` for the pool's bookkeeping. The CRDT is
    /// the single authoritative source for the post-failure metadata
    /// the pool needs; the local `pending_pool` doesn't itself index
    /// by hash.
    pub(super) fn task_meta_for_hash(
        &self,
        hash: &str,
    ) -> Option<(dynrunner_core::PhaseId, Option<String>)> {
        let state = self.cluster_state.task_state(hash)?;
        let task = match state {
            TaskState::Pending { task }
            | TaskState::InFlight { task, .. }
            | TaskState::Completed { task }
            | TaskState::Failed { task, .. }
            | TaskState::Unfulfillable { task, .. }
            | TaskState::Blocked { task, .. } => task,
        };
        Some((task.phase_id.clone(), task.task_id.clone()))
    }

    /// Handler for `PrimaryCommand::FailPermanent`. Wraps the existing
    /// `pending_pool::on_item_failed_permanent` primitive so the
    /// cascade-to-dependents semantics that primitive owns also apply
    /// to externally-requested failures, then broadcasts the
    /// `TaskFailed` mutation so every node mirrors the terminal state.
    ///
    /// Cascade routing splits on `error`:
    /// * `ErrorType::Unfulfillable { .. }` — dependents are broadcast
    ///   as `ClusterMutation::TaskBlocked { hash, on: <root> }`, so
    ///   the CRDT mirrors land in `TaskState::Blocked { on, task }`
    ///   on every replica. The matching `TaskCompleted` apply arm
    ///   auto-resumes them to `Pending` when the prereq later
    ///   completes via the reinject + re-run path. Dependents are
    ///   NOT recorded in the local per-pass `failed_tasks` ledger —
    ///   they're cascade-paused, not failed.
    /// * Any other `ErrorType` — dependents are recorded in the local
    ///   `failed_tasks` ledger with the same error (the legacy shape
    ///   a worker-driven cascade-fail produces).
    ///
    /// TODO: when a prereq's TaskCompleted auto-resumes its Blocked
    /// dependents in the CRDT, the live primary's pool must re-add
    /// those binaries (the pool's `on_item_failed_permanent` cascade
    /// already removed them from the blocked-set). Wire this through
    /// the auto-resume apply path so the pool stays coherent with
    /// the CRDT. Today the CRDT side is correct; pool-side dispatch
    /// of auto-resumed binaries needs the integration.
    pub(super) async fn apply_fail_permanent(
        &mut self,
        hash: String,
        error: ErrorType,
        reason: String,
    ) -> Result<(), String> {
        let Some((phase_id, task_id)) = self.task_meta_for_hash(&hash) else {
            return Err(format!(
                "fail_permanent: unknown task hash {hash}"
            ));
        };
        // Record the failure in the local per-pass ledger so the
        // operational loop's accounting + the per-phase counters match
        // the wire-side state. Mirrors `handle_task_failed`'s
        // `failed_tasks.insert(...)` step (the same in-memory side-
        // effect a worker-originated failure would have).
        self.failed_tasks.insert(hash.clone(), error.clone());

        // Cascade-to-dependents via the pool primitive. The returned
        // list is the dependents that the pool just gave up on; how
        // the caller observes them depends on the error class
        // (cascade-pause for Unfulfillable, cascade-fail otherwise).
        let cascaded_blocks: Vec<(String, String)> = if let Some(id) = task_id.as_deref() {
            let cascaded = self
                .pool_mut()
                .on_item_failed_permanent(&phase_id, id);
            let is_unfulfillable = matches!(error, ErrorType::Unfulfillable { .. });
            let mut blocks = Vec::new();
            for cascaded_binary in &cascaded {
                let cascaded_hash =
                    super::wire::compute_task_hash(cascaded_binary);
                if is_unfulfillable {
                    blocks.push((cascaded_hash, hash.clone()));
                } else {
                    self.failed_tasks
                        .insert(cascaded_hash, error.clone());
                }
            }
            blocks
        } else {
            Vec::new()
        };

        // Phase + lifecycle bookkeeping. Must run AFTER the pool
        // mutation so `process_phase_lifecycle` observes the post-
        // cascade pool state.
        self.note_item_failed(&phase_id, task_id.as_deref());

        // Broadcast the terminal state for the originating task plus
        // any cascade-paused dependents (Unfulfillable case only).
        // The CRDT-applied broadcast is the single source of truth
        // for every observer; ordering the originating TaskFailed
        // first means receivers see the prereq's Unfulfillable state
        // before the dependents' Blocked state — the cascade root is
        // visible whenever a dependent's `on` field is consulted.
        let mut mutations: Vec<ClusterMutation<I>> = Vec::with_capacity(
            1 + cascaded_blocks.len(),
        );
        mutations.push(ClusterMutation::TaskFailed {
            hash,
            kind: error,
            error: reason,
        });
        for (dep_hash, on_hash) in cascaded_blocks {
            mutations.push(ClusterMutation::TaskBlocked {
                hash: dep_hash,
                on: on_hash,
            });
        }
        self.apply_and_broadcast_cluster_mutations(mutations).await;
        Ok(())
    }

    /// Handler for `PrimaryCommand::ReinjectTask`. Accepts only entries
    /// whose CRDT state is the discrete `TaskState::Unfulfillable { .. }`
    /// — the operator-resolvable-failure class. Decrements the per-task
    /// budget; on exhaustion the local state stays `Unfulfillable` and
    /// the caller receives `Err`.
    pub(super) async fn apply_reinject_task(
        &mut self,
        hash: String,
    ) -> Result<(), String> {
        // Inspect CRDT state first — the local pool isn't indexed by
        // hash, and the discrete-variant gate has to read the
        // authoritative ledger.
        let binary = match self.cluster_state.task_state(&hash) {
            Some(TaskState::Unfulfillable { task, .. }) => task.clone(),
            Some(_) => {
                return Err(format!(
                    "reinject_task: hash {hash} not in Unfulfillable state"
                ));
            }
            None => {
                return Err(format!(
                    "reinject_task: unknown task hash {hash}"
                ));
            }
        };

        // Budget check. None == unbounded (the bypass branch);
        // `Some(0)` means "exhausted, refuse"; `Some(n>0)` decrements
        // and proceeds. The map is initialised lazily — first reinject
        // for a hash seeds the counter from the configured cap.
        let max = self.config.unfulfillable_reinject_max_per_task;
        if let Some(cap) = max {
            let remaining = self
                .unfulfillable_reinject_remaining
                .entry(hash.clone())
                .or_insert(cap);
            if *remaining == 0 {
                tracing::warn!(
                    task_hash = %hash,
                    cap,
                    event = "unfulfillable_reinject_budget_exhausted",
                    "reinject budget exhausted for task; staying Failed"
                );
                return Err(format!(
                    "reinject_task: budget exhausted for hash {hash} \
                     (cap={cap})"
                ));
            }
            *remaining -= 1;
        }

        // Local pool reinject: same primitive the retry-pass code path
        // uses. Re-injecting flips Drained/Done phase state back to
        // Active for this binary's phase, putting the item back into
        // the bucket head so the next dispatch tick picks it up.
        self.failed_tasks.remove(&hash);
        self.pool_mut().reinject(binary);

        // Broadcast so every node's CRDT mirror moves the entry off
        // `Failed` synchronously.
        self.apply_and_broadcast_cluster_mutations(vec![
            ClusterMutation::TaskReinjected { hash },
        ])
        .await;
        Ok(())
    }

    /// Handler for `PrimaryCommand::UpdatePreferredSecondaries`.
    /// Broadcasts the per-task preferred-secondaries update so every
    /// node's CRDT mirror sees the new preference list; the Phase-4
    /// `TaskInfo.preferred_secondaries` storage owns the in-memory
    /// side once it lands.
    pub(super) async fn apply_update_preferred_secondaries(
        &mut self,
        hash: String,
        secondaries: Vec<String>,
    ) -> Result<(), String> {
        if self.cluster_state.task_state(&hash).is_none() {
            return Err(format!(
                "update_preferred_secondaries: unknown task hash {hash}"
            ));
        }
        // TODO(phase-4): also update the in-memory
        // `TaskInfo.preferred_secondaries` on the live primary's pool
        // entry once the field exists; today only the CRDT side moves.
        self.apply_and_broadcast_cluster_mutations(vec![
            ClusterMutation::TaskPreferredSecondariesUpdated {
                hash,
                secondaries,
            },
        ])
        .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    use dynrunner_core::ErrorType;
    use dynrunner_scheduler::ResourceStealingScheduler;
    use tokio::sync::oneshot;

    use crate::primary::test_helpers::{
        make_binary, setup_test, FixedEstimator, NoPeers, TestId,
    };
    use crate::primary::wire::compute_task_hash;
    use crate::primary::PrimaryConfig;
    use crate::primary::PrimaryCoordinator;
    use crate::cluster_state::TaskState;
    use dynrunner_transport_channel::ChannelSecondaryTransportEnd;

    /// Build a `PrimaryCoordinator` against the in-process channel
    /// transport stub used by the rest of the primary tests. We
    /// don't drive a full run; the tests below call the command-
    /// channel handlers directly to assert per-command semantics
    /// without coupling to the operational loop's exit conditions.
    fn make_coordinator() -> PrimaryCoordinator<
        ChannelSecondaryTransportEnd<TestId>,
        NoPeers,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    > {
        let (transport, _secondary_ends) = setup_test(0);
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 0,
            connect_timeout: Duration::from_secs(1),
            peer_timeout: Duration::from_secs(1),
            keepalive_interval: Duration::from_millis(100),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: false,
            required_setup_on_promote: false,
            max_concurrent_per_type: HashMap::new(),
            retry_max_passes: 0,
            fleet_dead_timeout: Duration::from_secs(1),
            mesh_ready_timeout: Duration::from_secs(1),
            mass_death_grace: Duration::from_secs(1),
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
        };
        PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        )
    }

    /// End-to-end: send `FailPermanent`, observe the local
    /// `failed_tasks` ledger updates and the reply oneshot fires.
    #[tokio::test(flavor = "current_thread")]
    async fn fail_permanent_via_channel() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            // Seed a single Pending task into cluster_state so the
            // hash-to-meta lookup succeeds. Also pre-initialise the
            // pool so `pool.on_item_failed_permanent` has a phase
            // to discount in_flight against.
            let binary = make_binary("a", 100);
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(
                ClusterMutation::TaskAdded {
                    hash: hash.clone(),
                    task: binary.clone(),
                },
            );
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(binary.phase_id.clone());
            coordinator.pending = Some(
                dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                    .expect("pool init"),
            );
            for p in coordinator.pool().active_phases() {
                coordinator.phase_completed.insert(p.clone(), 0);
                coordinator.phase_failed.insert(p.clone(), 0);
            }

            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::FailPermanent {
                    hash: hash.clone(),
                    error: ErrorType::NonRecoverable,
                    reason: "test".into(),
                    reply: reply_tx,
                },
            )
            .await;
            let reply = reply_rx.await.expect("reply oneshot closed");
            assert!(reply.is_ok(), "fail_permanent should accept: {reply:?}");
            assert!(
                coordinator.failed_tasks.contains_key(&hash),
                "failed_tasks should include the hash"
            );
            // CRDT mirror reflects the Failed terminal state.
            match coordinator.cluster_state.task_state(&hash) {
                Some(TaskState::Failed { kind, .. }) => {
                    assert_eq!(*kind, ErrorType::NonRecoverable);
                }
                other => panic!("expected Failed, got {other:?}"),
            }
        }).await;
    }

    /// `FailPermanent` on an unknown hash returns Err and leaves
    /// coordinator state untouched.
    #[tokio::test(flavor = "current_thread")]
    async fn fail_permanent_unknown_hash() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::FailPermanent {
                    hash: "nonexistent".into(),
                    error: ErrorType::NonRecoverable,
                    reason: "test".into(),
                    reply: reply_tx,
                },
            )
            .await;
            let reply = reply_rx.await.expect("reply oneshot closed");
            assert!(reply.is_err(), "unknown hash should error");
            assert!(coordinator.failed_tasks.is_empty());
        }).await;
    }

    /// `ReinjectTask` accepts on `TaskState::Unfulfillable { .. }` and
    /// budget exhaustion locks further reinjects out without
    /// regressing the ledger.
    #[tokio::test(flavor = "current_thread")]
    async fn reinject_task_budget_exhaustion() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            coordinator
                .set_unfulfillable_reinject_max_per_task(Some(1));
            let binary = make_binary("a", 100);
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: binary.clone(),
            });
            coordinator.cluster_state.apply(ClusterMutation::TaskFailed {
                hash: hash.clone(),
                kind: ErrorType::Unfulfillable {
                    reason: "missing toolchain".to_string().into(),
                },
                error: "unfulfillable".into(),
            });
            // Pool init: reinject requires the phase to exist.
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(binary.phase_id.clone());
            coordinator.pending = Some(
                dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                    .expect("pool init"),
            );

            // First reinject — accepts.
            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::ReinjectTask {
                    hash: hash.clone(),
                    reply: reply_tx,
                },
            )
            .await;
            assert!(reply_rx.await.unwrap().is_ok(), "first reinject accepts");
            // CRDT mirror moved off Unfulfillable.
            assert!(matches!(
                coordinator.cluster_state.task_state(&hash),
                Some(TaskState::Pending { .. })
            ));

            // Re-set to Unfulfillable and try again — budget should be exhausted.
            coordinator.cluster_state.apply(ClusterMutation::TaskFailed {
                hash: hash.clone(),
                kind: ErrorType::Unfulfillable {
                    reason: "still missing".to_string().into(),
                },
                error: "unfulfillable again".into(),
            });
            let (reply_tx2, reply_rx2) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::ReinjectTask {
                    hash: hash.clone(),
                    reply: reply_tx2,
                },
            )
            .await;
            let r2 = reply_rx2.await.unwrap();
            assert!(r2.is_err(), "second reinject should hit budget cap");
            // Ledger stays Unfulfillable.
            assert!(matches!(
                coordinator.cluster_state.task_state(&hash),
                Some(TaskState::Unfulfillable { .. })
            ));
        }).await;
    }

    /// `UpdatePreferredSecondaries` is the simplest path — it only
    /// broadcasts the CRDT mutation; with no consumer wired today,
    /// the success case is "the call returns Ok and the CRDT
    /// applies".
    #[tokio::test(flavor = "current_thread")]
    async fn update_preferred_secondaries_smoke() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            let binary = make_binary("a", 100);
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: binary.clone(),
            });
            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::UpdatePreferredSecondaries {
                    hash: hash.clone(),
                    secondaries: vec!["sec-1".into(), "sec-2".into()],
                    reply: reply_tx,
                },
            )
            .await;
            assert!(reply_rx.await.unwrap().is_ok());
        }).await;
    }

    /// End-to-end through the cross-thread channel: send commands via
    /// the public `command_sender()` and consume them through the
    /// `PrimaryCommand` arm. Exercises the same code path the PyO3
    /// `PrimaryHandle` uses.
    #[tokio::test(flavor = "current_thread")]
    async fn command_channel_end_to_end() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            let binary = make_binary("a", 100);
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(
                ClusterMutation::TaskAdded {
                    hash: hash.clone(),
                    task: binary.clone(),
                },
            );
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(binary.phase_id.clone());
            coordinator.pending = Some(
                dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                    .expect("pool init"),
            );
            for p in coordinator.pool().active_phases() {
                coordinator.phase_completed.insert(p.clone(), 0);
                coordinator.phase_failed.insert(p.clone(), 0);
            }
            let sender = coordinator.command_sender();
            let (reply_tx, reply_rx) = oneshot::channel();
            // Send through the channel (the same `tokio::sync::mpsc`
            // the PyO3 handle uses), then drain the receiver once.
            sender
                .send(PrimaryCommand::FailPermanent {
                    hash: hash.clone(),
                    error: ErrorType::NonRecoverable,
                    reason: "via channel".into(),
                    reply: reply_tx,
                })
                .await
                .expect("send into command channel");
            // Mimic the operational loop: take the receiver, recv one
            // command, dispatch it.
            let mut rx = coordinator.command_rx.take().expect("rx present");
            let command = rx.recv().await.expect("first command");
            super::handle_primary_command(&mut coordinator, command).await;
            coordinator.command_rx = Some(rx);
            let reply = reply_rx.await.expect("reply oneshot closed");
            assert!(reply.is_ok(), "{reply:?}");
            assert!(coordinator.failed_tasks.contains_key(&hash));
        }).await;
    }

    /// `FailPermanent` with `ErrorType::Unfulfillable` routes the
    /// cascade to a `TaskBlocked` broadcast for each dependent — the
    /// dependent's CRDT entry lands in `TaskState::Blocked { on, .. }`
    /// rather than `TaskState::Failed`, so the auto-resume path can
    /// recover when the prereq is reinjected. Pins the cascade
    /// dispatch in `apply_fail_permanent`.
    #[tokio::test(flavor = "current_thread")]
    async fn fail_permanent_unfulfillable_blocks_dependents() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();

            // Prereq carries an explicit task_id so the pool can wire
            // the dep-cascade reverse-index.
            let mut prereq = make_binary("prereq", 100);
            prereq.task_id = Some("prereq_id".into());
            let prereq_hash = compute_task_hash(&prereq);

            // Dependent declares task_depends_on for the cascade walk.
            let mut dep = make_binary("dep", 100);
            dep.task_id = Some("dep_id".into());
            dep.task_depends_on = vec!["prereq_id".into()];
            let dep_hash = compute_task_hash(&dep);

            // Seed CRDT for both.
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: prereq_hash.clone(),
                task: prereq.clone(),
            });
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: dep_hash.clone(),
                task: dep.clone(),
            });

            // Pool seeded with both phases + the items so the cascade
            // primitive has dependents to walk.
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(prereq.phase_id.clone());
            let mut pool = dynrunner_scheduler_api::PendingPool::new(
                phase_set,
                HashMap::new(),
            )
            .expect("pool init");
            pool.extend(vec![prereq.clone(), dep.clone()])
                .expect("pool extend");
            coordinator.pending = Some(pool);
            for p in coordinator.pool().active_phases() {
                coordinator.phase_completed.insert(p.clone(), 0);
                coordinator.phase_failed.insert(p.clone(), 0);
            }
            // Mark the prereq in flight so on_item_failed_permanent's
            // in_flight bookkeeping doesn't saturate.
            coordinator.pool_mut().mark_in_flight(&prereq.phase_id);

            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::FailPermanent {
                    hash: prereq_hash.clone(),
                    error: ErrorType::Unfulfillable {
                        reason: "missing toolchain".to_string().into(),
                    },
                    reason: "no peer holds the resource".into(),
                    reply: reply_tx,
                },
            )
            .await;
            let reply = reply_rx.await.expect("reply oneshot closed");
            assert!(reply.is_ok(), "fail_permanent should accept: {reply:?}");

            // Prereq lands in the discrete Unfulfillable variant.
            assert!(matches!(
                coordinator.cluster_state.task_state(&prereq_hash),
                Some(TaskState::Unfulfillable { .. })
            ));
            // Dependent lands in Blocked-on-prereq via the cascade
            // broadcast (NOT in Failed).
            match coordinator.cluster_state.task_state(&dep_hash) {
                Some(TaskState::Blocked { on, .. }) => {
                    assert_eq!(on, &prereq_hash);
                }
                other => panic!("expected Blocked, got {other:?}"),
            }
            // Dependent is NOT in the local failed_tasks ledger — it's
            // cascade-paused, not failed.
            assert!(
                !coordinator.failed_tasks.contains_key(&dep_hash),
                "blocked dependent must not be in failed_tasks"
            );
        }).await;
    }
}
