//! Replicated cluster ledger.
//!
//! Single concern: every node holds a continuously-coherent view of the
//! cluster's task ledger and the current primary identity, maintained by
//! applying CRDT-style mutations broadcast across the mesh.
//!
//! The dispatcher (primary) is the only node that *originates* `TaskAdded`
//! and `TaskAssigned` mutations; every node — primary included — applies
//! every mutation that flows through. `TaskCompleted` / `TaskFailed` are
//! originated by whichever node observes the worker outcome (typically
//! the secondary that owns the worker), and `PrimaryChanged` by the
//! election protocol.
//!
//! Idempotency-by-precondition: each mutation describes the state it
//! applies against, and re-application against the post-state is a
//! `NoOp`. This makes out-of-order delivery and at-least-once delivery
//! safe: terminal states (`Completed` / `Failed`) lock out non-terminal
//! transitions, so a `TaskCompleted` that lands before the matching
//! `TaskAssigned` correctly leaves the entry terminal even when the
//! late `TaskAssigned` arrives next.

use std::collections::HashMap;

use dynrunner_core::{ErrorType, Identifier, PhaseId, TaskInfo, WorkerId};
use dynrunner_protocol_primary_secondary::ClusterMutation;
use serde::{Deserialize, Serialize};

/// Per-task state in the replicated ledger.
///
/// Every variant carries the `TaskInfo`. Pre-Phase-B terminal variants
/// dropped it on the assumption that nothing downstream needed it; that
/// assumption broke for the post-promotion hydration path, which needs
/// the `task_id` of completed tasks to seed `PendingPool::mark_tasks_completed`
/// so surviving variants' `task_depends_on` references resolve correctly.
/// Keeping `TaskInfo` everywhere costs O(N) extra clones but removes the
/// need for a parallel completed-task-id ledger; the alternative would
/// reintroduce duplicated state across `cluster_state` and a side cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "I: Serialize",
    deserialize = "I: for<'a> Deserialize<'a>",
))]
pub enum TaskState<I> {
    Pending {
        task: TaskInfo<I>,
    },
    InFlight {
        task: TaskInfo<I>,
        secondary: String,
        worker: WorkerId,
    },
    Completed {
        task: TaskInfo<I>,
    },
    Failed {
        task: TaskInfo<I>,
        kind: ErrorType,
        last_error: String,
        attempts: u32,
    },
}

/// Outcome of `ClusterState::apply`. `NoOp` is the normal silent-merge
/// case (duplicate, late delivery, terminal-locked); not an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    NoOp,
}

/// Counts of tasks per top-level state. For tests / metrics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StateCounts {
    pub pending: usize,
    pub in_flight: usize,
    pub completed: usize,
    pub failed: usize,
}

/// The replicated cluster-state CRDT.
#[derive(Debug, Clone)]
pub struct ClusterState<I> {
    tasks: HashMap<String, TaskState<I>>,
    current_primary: Option<String>,
    primary_epoch: u64,
    /// Per-run static phase dependency graph. Set once at run start
    /// via `ClusterMutation::PhaseDepsSet` (originated by the primary,
    /// applied on every node) and never overwritten — the deps are
    /// derived from the consumer's `TaskDefinition` declaration and
    /// don't change for the duration of a run.
    phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
}

impl<I> Default for ClusterState<I> {
    fn default() -> Self {
        Self {
            tasks: HashMap::new(),
            current_primary: None,
            primary_epoch: 0,
            phase_deps: HashMap::new(),
        }
    }
}

/// Serializable snapshot of an entire `ClusterState`. Used by the
/// snapshot RPC (`RequestClusterSnapshot` → `ClusterSnapshot`) so a
/// late-joining or reconnecting node can bootstrap its replicated
/// ledger from any peer.
///
/// Merge semantics on the receiver side (see `ClusterState::restore`):
///
/// - Per task: terminal states (`Completed` / `Failed`) win over
///   non-terminal; among non-terminals, `InFlight` wins over `Pending`.
///   When local and incoming are both terminal, local wins (first-seen
///   terminal is canonical, mirroring the live `apply` rules).
/// - `(current_primary, primary_epoch)`: higher epoch wins.
/// - `phase_deps`: replaced if local is empty, otherwise kept (the
///   graph is static for the run's lifetime).
///
/// These rules make `restore` an idempotent CRDT merge — applying the
/// same snapshot twice is a no-op, applying overlapping snapshots
/// converges to the same state regardless of order.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "I: Serialize",
    deserialize = "I: for<'a> Deserialize<'a>",
))]
pub struct ClusterStateSnapshot<I> {
    pub tasks: HashMap<String, TaskState<I>>,
    pub current_primary: Option<String>,
    pub primary_epoch: u64,
    pub phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
}

fn task_state_rank<I>(s: &TaskState<I>) -> u8 {
    match s {
        TaskState::Pending { .. } => 0,
        TaskState::InFlight { .. } => 1,
        TaskState::Completed { .. } | TaskState::Failed { .. } => 2,
    }
}

impl<I: Identifier> ClusterState<I> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    pub fn task_state(&self, hash: &str) -> Option<&TaskState<I>> {
        self.tasks.get(hash)
    }

    /// Iterator over `(&hash, &TaskState)` for every entry in the
    /// ledger. Used by post-promotion hydration that needs to make
    /// state-dependent decisions per task (Pending → into pool;
    /// terminal → contribute task_id to completed-deps seed;
    /// InFlight → skip).
    pub fn tasks_iter(&self) -> impl Iterator<Item = (&String, &TaskState<I>)> {
        self.tasks.iter()
    }

    pub fn iter_pending(&self) -> impl Iterator<Item = (&String, &TaskInfo<I>)> {
        self.tasks.iter().filter_map(|(h, s)| match s {
            TaskState::Pending { task } => Some((h, task)),
            _ => None,
        })
    }

    pub fn iter_in_flight(&self) -> impl Iterator<Item = (&String, &str, WorkerId)> {
        self.tasks.iter().filter_map(|(h, s)| match s {
            TaskState::InFlight {
                secondary, worker, ..
            } => Some((h, secondary.as_str(), *worker)),
            _ => None,
        })
    }

    pub fn counts(&self) -> StateCounts {
        let mut c = StateCounts::default();
        for s in self.tasks.values() {
            match s {
                TaskState::Pending { .. } => c.pending += 1,
                TaskState::InFlight { .. } => c.in_flight += 1,
                TaskState::Completed { .. } => c.completed += 1,
                TaskState::Failed { .. } => c.failed += 1,
            }
        }
        c
    }

    /// Iterator over `(task_hash, &TaskInfo)` for every entry in the
    /// ledger, regardless of state. Used by the post-promotion
    /// hydration path to seed `mark_tasks_completed` with the
    /// task_ids of terminal tasks (so surviving Pending tasks'
    /// `task_depends_on` references resolve).
    pub fn iter_all(&self) -> impl Iterator<Item = (&String, &TaskInfo<I>)> {
        self.tasks.iter().map(|(h, s)| {
            let t = match s {
                TaskState::Pending { task }
                | TaskState::InFlight { task, .. }
                | TaskState::Completed { task }
                | TaskState::Failed { task, .. } => task,
            };
            (h, t)
        })
    }

    /// Iterator over `(task_hash, &TaskInfo)` for terminal entries
    /// (`Completed` or `Failed`).
    pub fn iter_terminal(&self) -> impl Iterator<Item = (&String, &TaskInfo<I>)> {
        self.tasks.iter().filter_map(|(h, s)| match s {
            TaskState::Completed { task } | TaskState::Failed { task, .. } => Some((h, task)),
            _ => None,
        })
    }

    pub fn current_primary(&self) -> Option<&str> {
        self.current_primary.as_deref()
    }

    pub fn primary_epoch(&self) -> u64 {
        self.primary_epoch
    }

    pub fn phase_deps(&self) -> &HashMap<PhaseId, Vec<PhaseId>> {
        &self.phase_deps
    }

    /// Take a snapshot of the whole state. The snapshot is a deep
    /// clone — applying further mutations to `self` after this call
    /// does not affect the returned snapshot. Used as the response
    /// payload to `RequestClusterSnapshot`.
    pub fn snapshot(&self) -> ClusterStateSnapshot<I> {
        ClusterStateSnapshot {
            tasks: self.tasks.clone(),
            current_primary: self.current_primary.clone(),
            primary_epoch: self.primary_epoch,
            phase_deps: self.phase_deps.clone(),
        }
    }

    /// Merge a snapshot into local state per the CRDT lattice
    /// described on `ClusterStateSnapshot`. Idempotent: applying the
    /// same snapshot twice produces the same state as applying it
    /// once; applying overlapping snapshots converges regardless of
    /// order.
    ///
    /// Why merge (not replace): a node may have already applied
    /// live broadcasts before the snapshot RPC response arrives —
    /// for example, peer B's `TaskCompleted` reaches the joiner
    /// before peer A's snapshot does. Replacing would lose B's
    /// mutation; merging keeps the strictly stronger of (local,
    /// snapshot) per the lattice and stays correct under arbitrary
    /// interleaving of live broadcasts and snapshot delivery.
    pub fn restore(&mut self, snap: ClusterStateSnapshot<I>) {
        for (hash, incoming) in snap.tasks {
            match self.tasks.get(&hash) {
                None => {
                    self.tasks.insert(hash, incoming);
                }
                Some(local) => {
                    if task_state_rank(&incoming) > task_state_rank(local) {
                        self.tasks.insert(hash, incoming);
                    }
                }
            }
        }
        if snap.primary_epoch > self.primary_epoch {
            self.primary_epoch = snap.primary_epoch;
            self.current_primary = snap.current_primary;
        }
        if self.phase_deps.is_empty() {
            self.phase_deps = snap.phase_deps;
        }
    }

    pub fn apply(&mut self, m: ClusterMutation<I>) -> ApplyOutcome {
        match m {
            ClusterMutation::TaskAdded { hash, task } => {
                if self.tasks.contains_key(&hash) {
                    ApplyOutcome::NoOp
                } else {
                    self.tasks.insert(hash, TaskState::Pending { task });
                    ApplyOutcome::Applied
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
            ClusterMutation::TaskCompleted { hash } => {
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                let task = match state {
                    TaskState::Completed { .. } | TaskState::Failed { .. } => {
                        return ApplyOutcome::NoOp;
                    }
                    TaskState::Pending { task } | TaskState::InFlight { task, .. } => task.clone(),
                };
                *state = TaskState::Completed { task };
                ApplyOutcome::Applied
            }
            ClusterMutation::TaskFailed { hash, kind, error } => {
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                match state {
                    TaskState::Completed { .. } => ApplyOutcome::NoOp,
                    TaskState::Failed {
                        kind: k,
                        last_error,
                        attempts,
                        ..
                    } => {
                        *attempts += 1;
                        *k = kind;
                        *last_error = error;
                        ApplyOutcome::Applied
                    }
                    TaskState::Pending { task } | TaskState::InFlight { task, .. } => {
                        let task = task.clone();
                        *state = TaskState::Failed {
                            task,
                            kind,
                            last_error: error,
                            attempts: 1,
                        };
                        ApplyOutcome::Applied
                    }
                }
            }
            ClusterMutation::PrimaryChanged { new, epoch } => {
                if epoch < self.primary_epoch {
                    return ApplyOutcome::NoOp;
                }
                if epoch == self.primary_epoch && self.current_primary.as_deref() == Some(&new) {
                    return ApplyOutcome::NoOp;
                }
                self.current_primary = Some(new);
                self.primary_epoch = epoch;
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_core::{PhaseId, RunnerIdentifier, TypeId};
    use std::path::PathBuf;

    fn mk_task(name: &str) -> TaskInfo<RunnerIdentifier> {
        TaskInfo {
            path: PathBuf::from(format!("/tasks/{name}")),
            size: 0,
            identifier: RunnerIdentifier::from(name),
            phase_id: PhaseId::from("p0"),
            type_id: TypeId::from("t0"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: Some(name.into()),
            task_depends_on: Vec::new(),
            resolved_path: None,
        }
    }

    #[test]
    fn task_added_idempotent() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        let t = mk_task("a");
        assert_eq!(
            s.apply(ClusterMutation::TaskAdded {
                hash: "h".into(),
                task: t.clone(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            s.apply(ClusterMutation::TaskAdded {
                hash: "h".into(),
                task: t,
            }),
            ApplyOutcome::NoOp
        );
        assert_eq!(s.task_count(), 1);
        assert_eq!(s.counts().pending, 1);
    }

    #[test]
    fn assigned_late_after_completed_is_noop() {
        // Demonstrates terminal-states-win: TaskCompleted lands first
        // (out-of-order delivery), the late TaskAssigned must NOT
        // resurrect the entry to InFlight.
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("a"),
        });
        assert_eq!(
            s.apply(ClusterMutation::TaskCompleted { hash: "h".into() }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            s.apply(ClusterMutation::TaskAssigned {
                hash: "h".into(),
                secondary: "s1".into(),
                worker: 0,
            }),
            ApplyOutcome::NoOp
        );
        assert!(matches!(s.task_state("h"), Some(TaskState::Completed { .. })));
    }

    #[test]
    fn duplicate_completed_is_noop() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("a"),
        });
        s.apply(ClusterMutation::TaskCompleted { hash: "h".into() });
        assert_eq!(
            s.apply(ClusterMutation::TaskCompleted { hash: "h".into() }),
            ApplyOutcome::NoOp
        );
    }

    #[test]
    fn failed_then_completed_is_noop_completed_locked_out() {
        // Once a node observes Failed for a task, a later TaskCompleted
        // for the same hash must not flip it back to success.
        // Symmetric to the Completed-locks-out-Failed direction.
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("a"),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::Recoverable,
            error: "x".into(),
        });
        assert_eq!(
            s.apply(ClusterMutation::TaskCompleted { hash: "h".into() }),
            ApplyOutcome::NoOp
        );
        assert!(matches!(s.task_state("h"), Some(TaskState::Failed { .. })));
    }

    #[test]
    fn failed_attempts_counter_increments() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("a"),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::Recoverable,
            error: "first".into(),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::NonRecoverable,
            error: "second".into(),
        });
        match s.task_state("h") {
            Some(TaskState::Failed {
                kind,
                last_error,
                attempts,
                ..
            }) => {
                assert_eq!(*attempts, 2);
                assert_eq!(*kind, ErrorType::NonRecoverable);
                assert_eq!(last_error, "second");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn primary_changed_higher_epoch_wins() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::PrimaryChanged {
            new: "a".into(),
            epoch: 1,
        });
        // Lower epoch is rejected.
        assert_eq!(
            s.apply(ClusterMutation::PrimaryChanged {
                new: "b".into(),
                epoch: 0,
            }),
            ApplyOutcome::NoOp
        );
        assert_eq!(s.current_primary(), Some("a"));
        // Higher epoch wins.
        s.apply(ClusterMutation::PrimaryChanged {
            new: "c".into(),
            epoch: 5,
        });
        assert_eq!(s.current_primary(), Some("c"));
        assert_eq!(s.primary_epoch(), 5);
    }

    #[test]
    fn iter_pending_only_returns_pending() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "p".into(),
            task: mk_task("p"),
        });
        s.apply(ClusterMutation::TaskAdded {
            hash: "i".into(),
            task: mk_task("i"),
        });
        s.apply(ClusterMutation::TaskAssigned {
            hash: "i".into(),
            secondary: "s".into(),
            worker: 0,
        });
        s.apply(ClusterMutation::TaskAdded {
            hash: "c".into(),
            task: mk_task("c"),
        });
        s.apply(ClusterMutation::TaskCompleted { hash: "c".into() });

        let mut pending: Vec<&str> = s.iter_pending().map(|(h, _)| h.as_str()).collect();
        pending.sort();
        assert_eq!(pending, vec!["p"]);

        let in_flight: Vec<&str> = s.iter_in_flight().map(|(h, _, _)| h.as_str()).collect();
        assert_eq!(in_flight, vec!["i"]);
    }

    /// Convergence under the realistic reordering: `TaskAdded` always
    /// happens-before `TaskAssigned` and `TaskCompleted` (the primary
    /// originates `TaskAdded` before issuing the assignment, which is
    /// itself a strict prerequisite of the worker completing). Within
    /// that constraint, `TaskCompleted` can race ahead of the matching
    /// `TaskAssigned` at a third-party receiver — both orderings must
    /// converge to `Completed`.
    #[test]
    fn convergence_completed_can_race_assigned() {
        let added = ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("a"),
        };
        let assigned = ClusterMutation::TaskAssigned {
            hash: "h".into(),
            secondary: "s".into(),
            worker: 0,
        };
        let completed: ClusterMutation<RunnerIdentifier> =
            ClusterMutation::TaskCompleted { hash: "h".into() };

        let mut a = ClusterState::<RunnerIdentifier>::new();
        a.apply(added.clone());
        a.apply(assigned.clone());
        a.apply(completed.clone());

        let mut b = ClusterState::<RunnerIdentifier>::new();
        b.apply(added);
        b.apply(completed);
        b.apply(assigned);

        assert!(matches!(a.task_state("h"), Some(TaskState::Completed { .. })));
        assert!(matches!(b.task_state("h"), Some(TaskState::Completed { .. })));
    }

    /// Convergence under duplicates: applying every mutation twice
    /// in interleaved order produces the same final state as
    /// applying each once in their natural order.
    #[test]
    fn convergence_under_duplicates() {
        let muts: Vec<ClusterMutation<RunnerIdentifier>> = vec![
            ClusterMutation::TaskAdded {
                hash: "h1".into(),
                task: mk_task("h1"),
            },
            ClusterMutation::TaskAdded {
                hash: "h2".into(),
                task: mk_task("h2"),
            },
            ClusterMutation::TaskAssigned {
                hash: "h1".into(),
                secondary: "s".into(),
                worker: 0,
            },
            ClusterMutation::TaskCompleted { hash: "h1".into() },
            ClusterMutation::TaskFailed {
                hash: "h2".into(),
                kind: ErrorType::Recoverable,
                error: "boom".into(),
            },
        ];
        let mut once = ClusterState::<RunnerIdentifier>::new();
        for m in muts.iter().cloned() {
            once.apply(m);
        }
        let mut twice = ClusterState::<RunnerIdentifier>::new();
        for m in muts.iter().cloned() {
            twice.apply(m.clone());
            twice.apply(m);
        }
        assert_eq!(once.counts(), twice.counts());
        assert!(matches!(once.task_state("h1"), Some(TaskState::Completed { .. })));
        assert!(matches!(twice.task_state("h1"), Some(TaskState::Completed { .. })));
        // Failed got applied twice; second TaskFailed bumps attempts.
        match twice.task_state("h2") {
            Some(TaskState::Failed { attempts, .. }) => assert_eq!(*attempts, 2),
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn phase_deps_set_then_re_set_is_noop() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        let deps: HashMap<PhaseId, Vec<PhaseId>> =
            [(PhaseId::from("p1"), vec![PhaseId::from("p0")])]
                .into_iter()
                .collect();
        assert_eq!(
            s.apply(ClusterMutation::PhaseDepsSet { deps: deps.clone() }),
            ApplyOutcome::Applied
        );
        assert_eq!(s.phase_deps(), &deps);
        // Re-application is silent — the per-run graph is static.
        let other: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
        assert_eq!(
            s.apply(ClusterMutation::PhaseDepsSet { deps: other }),
            ApplyOutcome::NoOp
        );
        assert_eq!(s.phase_deps(), &deps);
    }

    #[test]
    fn snapshot_round_trip_preserves_state() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "p".into(),
            task: mk_task("p"),
        });
        s.apply(ClusterMutation::TaskAdded {
            hash: "i".into(),
            task: mk_task("i"),
        });
        s.apply(ClusterMutation::TaskAssigned {
            hash: "i".into(),
            secondary: "s1".into(),
            worker: 7,
        });
        s.apply(ClusterMutation::TaskAdded {
            hash: "c".into(),
            task: mk_task("c"),
        });
        s.apply(ClusterMutation::TaskCompleted { hash: "c".into() });
        s.apply(ClusterMutation::PrimaryChanged {
            new: "s1".into(),
            epoch: 3,
        });
        let deps: HashMap<PhaseId, Vec<PhaseId>> =
            [(PhaseId::from("p1"), vec![PhaseId::from("p0")])]
                .into_iter()
                .collect();
        s.apply(ClusterMutation::PhaseDepsSet { deps: deps.clone() });

        let snap = s.snapshot();
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        joiner.restore(snap);

        assert_eq!(joiner.counts(), s.counts());
        assert_eq!(joiner.current_primary(), Some("s1"));
        assert_eq!(joiner.primary_epoch(), 3);
        assert_eq!(joiner.phase_deps(), &deps);
        assert!(matches!(
            joiner.task_state("p"),
            Some(TaskState::Pending { .. })
        ));
        assert!(matches!(
            joiner.task_state("i"),
            Some(TaskState::InFlight { .. })
        ));
        assert!(matches!(
            joiner.task_state("c"),
            Some(TaskState::Completed { .. })
        ));
    }

    #[test]
    fn restore_lattice_merge_preserves_local_terminal() {
        // Joiner has already observed TaskCompleted via a live broadcast
        // before the snapshot RPC response arrives. The snapshot's
        // weaker InFlight state must NOT override the local terminal.
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        joiner.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("h"),
        });
        joiner.apply(ClusterMutation::TaskCompleted { hash: "h".into() });

        let mut peer = ClusterState::<RunnerIdentifier>::new();
        peer.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("h"),
        });
        peer.apply(ClusterMutation::TaskAssigned {
            hash: "h".into(),
            secondary: "s".into(),
            worker: 0,
        });

        joiner.restore(peer.snapshot());
        assert!(matches!(
            joiner.task_state("h"),
            Some(TaskState::Completed { .. })
        ));
    }

    #[test]
    fn restore_lattice_merge_promotes_pending_to_in_flight() {
        // Joiner has only seen TaskAdded; snapshot has the InFlight
        // entry. The stronger lattice element (InFlight) wins.
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        joiner.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("h"),
        });

        let mut peer = ClusterState::<RunnerIdentifier>::new();
        peer.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("h"),
        });
        peer.apply(ClusterMutation::TaskAssigned {
            hash: "h".into(),
            secondary: "s".into(),
            worker: 4,
        });

        joiner.restore(peer.snapshot());
        match joiner.task_state("h") {
            Some(TaskState::InFlight { worker, .. }) => assert_eq!(*worker, 4),
            other => panic!("expected InFlight, got {other:?}"),
        }
    }

    #[test]
    fn restore_higher_epoch_wins_for_primary() {
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        joiner.apply(ClusterMutation::PrimaryChanged {
            new: "old".into(),
            epoch: 1,
        });
        let mut peer = ClusterState::<RunnerIdentifier>::new();
        peer.apply(ClusterMutation::PrimaryChanged {
            new: "new".into(),
            epoch: 5,
        });
        joiner.restore(peer.snapshot());
        assert_eq!(joiner.current_primary(), Some("new"));
        assert_eq!(joiner.primary_epoch(), 5);

        // Reverse direction: a stale snapshot must not regress epoch.
        let mut stale_peer = ClusterState::<RunnerIdentifier>::new();
        stale_peer.apply(ClusterMutation::PrimaryChanged {
            new: "ancient".into(),
            epoch: 2,
        });
        joiner.restore(stale_peer.snapshot());
        assert_eq!(joiner.current_primary(), Some("new"));
        assert_eq!(joiner.primary_epoch(), 5);
    }

    #[test]
    fn restore_idempotent_under_double_apply() {
        let mut peer = ClusterState::<RunnerIdentifier>::new();
        peer.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("h"),
        });
        peer.apply(ClusterMutation::TaskCompleted { hash: "h".into() });

        let snap = peer.snapshot();
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        joiner.restore(snap.clone());
        let counts_once = joiner.counts();
        joiner.restore(snap);
        assert_eq!(joiner.counts(), counts_once);
    }
}
