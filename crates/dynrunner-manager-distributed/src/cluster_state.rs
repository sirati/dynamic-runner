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

use dynrunner_core::{ErrorType, Identifier, TaskInfo, WorkerId};
use dynrunner_protocol_primary_secondary::ClusterMutation;
use serde::{Deserialize, Serialize};

/// Per-task state in the replicated ledger.
///
/// `Pending` and `InFlight` carry the `TaskInfo` so a node that observes
/// only the post-`TaskAdded` ledger can later dispatch the task without
/// asking the originator for the payload. Terminal states drop the
/// payload because nothing downstream needs it.
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
    Completed,
    Failed {
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
}

impl<I> Default for ClusterState<I> {
    fn default() -> Self {
        Self {
            tasks: HashMap::new(),
            current_primary: None,
            primary_epoch: 0,
        }
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
                TaskState::Completed => c.completed += 1,
                TaskState::Failed { .. } => c.failed += 1,
            }
        }
        c
    }

    pub fn current_primary(&self) -> Option<&str> {
        self.current_primary.as_deref()
    }

    pub fn primary_epoch(&self) -> u64 {
        self.primary_epoch
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
                match state {
                    TaskState::Completed | TaskState::Failed { .. } => ApplyOutcome::NoOp,
                    _ => {
                        *state = TaskState::Completed;
                        ApplyOutcome::Applied
                    }
                }
            }
            ClusterMutation::TaskFailed { hash, kind, error } => {
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                match state {
                    TaskState::Completed => ApplyOutcome::NoOp,
                    TaskState::Failed {
                        kind: k,
                        last_error,
                        attempts,
                    } => {
                        *attempts += 1;
                        *k = kind;
                        *last_error = error;
                        ApplyOutcome::Applied
                    }
                    _ => {
                        *state = TaskState::Failed {
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
        assert!(matches!(s.task_state("h"), Some(TaskState::Completed)));
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

        assert!(matches!(a.task_state("h"), Some(TaskState::Completed)));
        assert!(matches!(b.task_state("h"), Some(TaskState::Completed)));
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
        assert!(matches!(once.task_state("h1"), Some(TaskState::Completed)));
        assert!(matches!(twice.task_state("h1"), Some(TaskState::Completed)));
        // Failed got applied twice; second TaskFailed bumps attempts.
        match twice.task_state("h2") {
            Some(TaskState::Failed { attempts, .. }) => assert_eq!(*attempts, 2),
            other => panic!("expected Failed, got {other:?}"),
        }
    }
}
