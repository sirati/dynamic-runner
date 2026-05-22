//! Dispatch-time predecessor-output gathering.
//!
//! Single concern: given a `TaskInfo` about to be dispatched, assemble
//! the `BTreeMap<task_id, TaskOutputs>` that rides on its
//! `DistributedMessage::TaskAssignment.predecessor_outputs` field.
//!
//! Read-only over the replicated [`ClusterState`] — no mutation, no
//! panic on missing predecessor entries. Both primary dispatch
//! construction sites (`primary/lifecycle/dispatch.rs` and
//! `primary/task/request.rs`) call into this helper so the assembly
//! shape is identical regardless of which dispatch path fires.
//!
//! Contract per the keyed-outputs feature plan:
//!   * For every direct entry in `task.task_depends_on`, the result
//!     contains a key for that dep's `task_id`. The value is the
//!     dep's cached outputs if recorded, otherwise an empty
//!     [`TaskOutputs`] so dependents that hard-require a key see a
//!     stable "present-but-empty" shape rather than an absent entry.
//!   * For each direct dep with `inherit_outputs == true`, the
//!     transitive ancestry reachable through that dep's OWN
//!     `task_depends_on` edges is also included. Ancestors that
//!     produced no outputs likewise yield an empty `TaskOutputs`.
//!     Ancestry-walk traversal does not re-consult `inherit_outputs`
//!     on inner edges — the flag on the first edge selects between
//!     "direct only" and "transitive closure of the predecessor's
//!     ancestry".
//!   * A direct dep whose `inherit_outputs == false` contributes only
//!     its own key; its predecessors are not walked from this site
//!     (they will reach the assembled map only if some other direct
//!     edge from `task` reaches them).
//!
//! Cycle defence: the ancestry walk carries a visited set keyed by
//! `task_id`. The scheduler's BFS spawn-time check rejects cyclic
//! dep graphs before they reach this code path, but defensive coding
//! keeps a malformed snapshot (or a future regression in the
//! validator) from looping the dispatch path.

use std::collections::{BTreeMap, HashSet, VecDeque};

use dynrunner_core::{Identifier, TaskInfo, TaskOutputs};

use crate::cluster_state::ClusterState;

/// Assemble the predecessor-output map for `task`. See module doc for
/// the per-edge contract.
///
/// Returned map is owned; callers wire it directly into the wire
/// message. Empty map if `task.task_depends_on` is empty.
pub(in crate::primary) fn gather_predecessor_outputs<I: Identifier>(
    state: &ClusterState<I>,
    task: &TaskInfo<I>,
) -> BTreeMap<String, TaskOutputs> {
    let mut result: BTreeMap<String, TaskOutputs> = BTreeMap::new();

    for dep in &task.task_depends_on {
        insert_outputs_for(state, &dep.task_id, &mut result);
        if dep.inherit_outputs {
            walk_ancestry(state, &dep.task_id, &mut result);
        }
    }

    result
}

/// Insert the cached outputs for `task_id` into `result`. If the cache
/// has no entry, insert an empty [`TaskOutputs`] so the dependent sees
/// a present-but-empty key (the contract documented on the module).
///
/// Idempotent on `result`: a second insert for the same `task_id` is
/// a no-op (the first write wins) — important because the transitive
/// walk and the direct-dep loop can both reach the same ancestor
/// through different paths.
fn insert_outputs_for<I: Identifier>(
    state: &ClusterState<I>,
    task_id: &str,
    result: &mut BTreeMap<String, TaskOutputs>,
) {
    if result.contains_key(task_id) {
        return;
    }
    let outputs = state
        .outputs_for(task_id)
        .cloned()
        .unwrap_or_default();
    result.insert(task_id.to_string(), outputs);
}

/// Breadth-first traversal of the ancestry rooted at `root_id`,
/// inserting each ancestor's outputs (or empty) into `result`.
///
/// The visited set is a cycle defence — the scheduler's BFS check
/// at spawn time forbids cyclic dep graphs, but a malformed
/// snapshot or a future regression in the validator would otherwise
/// loop this walk forever. Re-using `result.keys()` as the visited
/// set is not enough on its own because the walk inserts the root
/// only on the first visit; the dedicated `HashSet` guards the
/// queue against re-enqueueing across siblings.
fn walk_ancestry<I: Identifier>(
    state: &ClusterState<I>,
    root_id: &str,
    result: &mut BTreeMap<String, TaskOutputs>,
) {
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    visited.insert(root_id.to_string());
    queue.push_back(root_id.to_string());

    while let Some(current_id) = queue.pop_front() {
        let Some(current_task) = find_task_info_by_id(state, &current_id) else {
            // Unknown task_id — pre-apply spawn validation rejects
            // unknown deps, but a snapshot containing only a subset
            // of the run's tasks (late-joiner partial restore) can
            // reach this branch. Skip silently: the direct-dep
            // loop already emitted a present-but-empty key for any
            // missing direct ancestor, and missing transitives are
            // a strictly weaker concern than missing direct deps.
            continue;
        };
        for dep in &current_task.task_depends_on {
            if visited.insert(dep.task_id.clone()) {
                insert_outputs_for(state, &dep.task_id, result);
                queue.push_back(dep.task_id.clone());
            }
        }
    }
}

/// Linear-scan `state.tasks.values()` for the entry whose `task_id ==
/// Some(task_id)`. Returns `None` if no entry carries that id.
///
/// O(n) over the ledger; acceptable here because the ancestry walk
/// fires only at dispatch time (not the hot path) and per-task chains
/// are short. The CRDT does not maintain a `task_id → hash` reverse
/// index by design — the `outputs_for` accessor already keys by
/// `task_id` so the cached-output lookup is O(1); only the secondary
/// hop "predecessor's own dep list" needs a `TaskInfo` borrow, and
/// adding a replicated reverse index would be a larger refactor
/// (PhaseDepsSet / TaskAdded apply paths must agree).
fn find_task_info_by_id<'a, I: Identifier>(
    state: &'a ClusterState<I>,
    task_id: &str,
) -> Option<&'a TaskInfo<I>> {
    state
        .iter_all()
        .find_map(|(_, task)| (task.task_id.as_deref() == Some(task_id)).then_some(task))
}

#[cfg(test)]
mod tests {
    //! Pin the gather behaviour against the four documented shapes:
    //! direct dep with outputs, direct dep without outputs, transitive
    //! inheritance through one edge, inherit_outputs=false stops the
    //! walk. Plus a cycle-defence smoke test against a hypothetical
    //! self-loop (the validator forbids it but defensive coding lives
    //! here too).

    use super::*;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use dynrunner_core::{
        PhaseId, ResultValue, RunnerIdentifier, SoftPreferredSecondaries, TaskDep, TaskInfo,
        TaskOutputs, TypeId,
    };
    use dynrunner_protocol_primary_secondary::ClusterMutation;

    use crate::cluster_state::ClusterState;

    fn mk_task(name: &str, deps: Vec<TaskDep>) -> TaskInfo<RunnerIdentifier> {
        TaskInfo {
            path: PathBuf::from(format!("/tasks/{name}")),
            size: 0,
            identifier: RunnerIdentifier::from(name),
            phase_id: PhaseId::from("p0"),
            type_id: TypeId::from("t0"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: Some(name.into()),
            task_depends_on: deps,
            preferred_secondaries: SoftPreferredSecondaries::default(),
            resolved_path: None,
        }
    }

    fn outputs_with(key: &str, value: &str) -> TaskOutputs {
        let mut m: BTreeMap<String, ResultValue> = BTreeMap::new();
        m.insert(key.to_string(), ResultValue::Inline(value.to_string()));
        TaskOutputs(m)
    }

    /// Build a `ClusterState` with `task` added and (if `outputs` is
    /// `Some`) the matching `TaskCompleted` applied to populate the
    /// outputs cache. Uses the task_id as the hash for simplicity —
    /// the cache keys by `task_id`, so the hash only needs to be
    /// unique within the ledger.
    fn seed(
        state: &mut ClusterState<RunnerIdentifier>,
        task: TaskInfo<RunnerIdentifier>,
        outputs: Option<TaskOutputs>,
    ) {
        let hash = task
            .task_id
            .clone()
            .unwrap_or_else(|| format!("anon-{:p}", &task));
        state.apply(ClusterMutation::TaskAdded {
            hash: hash.clone(),
            task,
        });
        if let Some(o) = outputs {
            let bytes = serde_json::to_vec(&o).expect("serialise outputs");
            state.apply(ClusterMutation::TaskCompleted {
                hash,
                result_data: Some(bytes),
            });
        }
    }

    #[test]
    fn direct_dep_no_inherit_returns_only_predecessor_outputs() {
        // A → B, no inheritance. The dispatch-time gather for B sees
        // exactly {"A": A's outputs}.
        let mut state = ClusterState::<RunnerIdentifier>::new();
        let a = mk_task("A", Vec::new());
        let b = mk_task(
            "B",
            vec![TaskDep {
                task_id: "A".into(),
                inherit_outputs: false,
            }],
        );
        let a_outputs = outputs_with("nonce", "xyz");
        seed(&mut state, a, Some(a_outputs.clone()));

        let assembled = gather_predecessor_outputs(&state, &b);
        assert_eq!(assembled.len(), 1);
        assert_eq!(assembled.get("A"), Some(&a_outputs));
    }

    #[test]
    fn direct_dep_with_no_recorded_outputs_yields_empty_taskoutputs() {
        // A is added but has not completed with outputs. B still
        // receives a present-but-empty key for "A" — the contract
        // lets dependents index by predecessor id without a None
        // check.
        let mut state = ClusterState::<RunnerIdentifier>::new();
        let a = mk_task("A", Vec::new());
        let b = mk_task(
            "B",
            vec![TaskDep {
                task_id: "A".into(),
                inherit_outputs: false,
            }],
        );
        seed(&mut state, a, None);

        let assembled = gather_predecessor_outputs(&state, &b);
        assert_eq!(assembled.len(), 1);
        assert_eq!(assembled.get("A"), Some(&TaskOutputs::default()));
    }

    #[test]
    fn inherit_outputs_pulls_transitive_ancestor() {
        // A → B → C with C→B carrying inherit_outputs=true. The
        // assembled map for C includes both B (direct) and A
        // (transitive via B's own task_depends_on).
        let mut state = ClusterState::<RunnerIdentifier>::new();
        let a = mk_task("A", Vec::new());
        let b = mk_task(
            "B",
            vec![TaskDep {
                task_id: "A".into(),
                inherit_outputs: false,
            }],
        );
        let c = mk_task(
            "C",
            vec![TaskDep {
                task_id: "B".into(),
                inherit_outputs: true,
            }],
        );
        let a_outputs = outputs_with("x", "1");
        let b_outputs = outputs_with("y", "2");
        seed(&mut state, a, Some(a_outputs.clone()));
        seed(&mut state, b, Some(b_outputs.clone()));

        let assembled = gather_predecessor_outputs(&state, &c);
        assert_eq!(assembled.len(), 2);
        assert_eq!(assembled.get("B"), Some(&b_outputs));
        assert_eq!(assembled.get("A"), Some(&a_outputs));
    }

    #[test]
    fn inherit_outputs_false_blocks_transitive_walk() {
        // Same A → B → C graph but C→B carries inherit_outputs=false.
        // C sees only {"B": B's outputs}; A's outputs are NOT
        // attached because the inheritance flag gates the walk.
        let mut state = ClusterState::<RunnerIdentifier>::new();
        let a = mk_task("A", Vec::new());
        let b = mk_task(
            "B",
            vec![TaskDep {
                task_id: "A".into(),
                inherit_outputs: false,
            }],
        );
        let c = mk_task(
            "C",
            vec![TaskDep {
                task_id: "B".into(),
                inherit_outputs: false,
            }],
        );
        seed(&mut state, a, Some(outputs_with("x", "1")));
        let b_outputs = outputs_with("y", "2");
        seed(&mut state, b, Some(b_outputs.clone()));

        let assembled = gather_predecessor_outputs(&state, &c);
        assert_eq!(assembled.len(), 1);
        assert_eq!(assembled.get("B"), Some(&b_outputs));
        assert!(!assembled.contains_key("A"));
    }

    #[test]
    fn self_referential_dep_with_inherit_does_not_infinite_loop() {
        // Cycle defence: an A → A self-edge with inherit_outputs=true
        // exercises the visited-set guard. The validator forbids this
        // graph at spawn time, but a malformed snapshot or a future
        // validator regression could expose it; the defensive walk
        // must terminate and emit exactly one entry for "A".
        let mut state = ClusterState::<RunnerIdentifier>::new();
        let a = mk_task(
            "A",
            vec![TaskDep {
                task_id: "A".into(),
                inherit_outputs: true,
            }],
        );
        let a_outputs = outputs_with("k", "v");
        seed(&mut state, a.clone(), Some(a_outputs.clone()));

        let assembled = gather_predecessor_outputs(&state, &a);
        assert_eq!(assembled.len(), 1);
        assert_eq!(assembled.get("A"), Some(&a_outputs));
    }
}
