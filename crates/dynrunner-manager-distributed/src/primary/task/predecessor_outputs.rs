//! Dispatch-time predecessor-output gathering — distributed-mode wrapper.
//!
//! Single concern: bind the generic [`dynrunner_core::gather_predecessor_outputs`]
//! free function to the [`ClusterState`] storage shape. Both primary
//! dispatch construction sites (`primary/lifecycle/dispatch.rs` and
//! `primary/task/request.rs`) call into this wrapper so the assembled
//! map's shape matches the local-mode dispatch path (which binds the
//! same core helper to its per-manager cache).
//!
//! The gather contract (direct deps emit a present-but-empty key when
//! no outputs were recorded; `inherit_outputs=true` widens to the
//! transitive ancestry; cycle defence via a visited set) lives in
//! [`dynrunner_core::output_gather`]. This file is purely the
//! [`ClusterState`]-shaped binding.

use std::collections::BTreeMap;

use dynrunner_core::{
    gather_predecessor_outputs as core_gather, Identifier, TaskInfo, TaskOutputs,
};

use crate::cluster_state::ClusterState;

/// Assemble the predecessor-output map for `task` by binding the core
/// helper to `state`'s replicated `task_outputs` cache and ledger
/// view. Returned map is owned; callers wire it directly into the
/// wire message.
pub(crate) fn gather_predecessor_outputs<I: Identifier>(
    state: &ClusterState<I>,
    task: &TaskInfo<I>,
) -> BTreeMap<String, TaskOutputs> {
    core_gather(
        &task.task_depends_on,
        // Cached outputs lookup: O(1) via the CRDT's task_id-keyed cache.
        |task_id| state.outputs_for(task_id).cloned(),
        // Deps-of lookup: linear scan over `state.iter_all()`. The
        // CRDT does not maintain a `task_id → hash` reverse index by
        // design; the ancestry walk fires only at dispatch time (not
        // the hot path) and per-task chains are short, so the O(n)
        // scan is acceptable. Adding a replicated reverse index
        // would be a larger refactor (PhaseDepsSet / TaskAdded apply
        // paths must agree).
        |task_id| find_task_info_by_id(state, task_id).map(|t| t.task_depends_on.clone()),
    )
}

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
    //! Pin the [`ClusterState`]-bound wrapper against the same four
    //! documented shapes the core helper's unit tests pin, but
    //! through the full CRDT apply path so the binding glue
    //! (`outputs_for`, `iter_all`) is exercised end-to-end.

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
