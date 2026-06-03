//! Dispatch-time predecessor-output gathering.
//!
//! Single concern: given a task's direct dependency list, assemble the
//! `BTreeMap<task_id, TaskOutputs>` that rides on the wire-message
//! handed to the dependent's worker.
//!
//! Pure free function over two caller-supplied lookups (cached outputs
//! by `task_id`, deps-of-task by `task_id`). The helper does NOT know
//! about [`ClusterState`], the local manager's cache, or any specific
//! storage shape — it consumes whatever map-like view the caller
//! exposes through the closures. Both distributed-mode (primary,
//! reading from the replicated `task_outputs` cache) and local-mode
//! (`manager-local`, reading from its per-manager
//! `task_outputs_cache`) call into this helper so the assembly shape
//! is identical regardless of which dispatch path fires.
//!
//! Contract per the keyed-outputs feature plan:
//!   * For every direct entry in the supplied `task_deps`, the result
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

use crate::types::{PhaseId, TaskDep, TaskOutputs};

/// Assemble the predecessor-output map for a task whose direct deps are
/// `task_deps`. See module doc for the per-edge contract.
///
/// The two closures decouple the helper from any specific storage.
/// Both key on a dependency's FULL `(phase_id, task_id)` identity — the
/// same `task_id` in two different phases is a distinct predecessor, so
/// resolution must carry the phase:
///
/// * `outputs_for(phase_id, task_id)` — returns the cached outputs for
///   that identity (owned/cloned). `None` means "no outputs recorded";
///   the helper converts that into the present-but-empty contract entry.
/// * `deps_of(phase_id, task_id)` — returns the named ancestor's OWN
///   `task_depends_on` for the transitive walk. `None` means "task
///   not known to this caller"; the walk skips silently (the
///   direct-dep loop has already emitted the present-but-empty key,
///   so a missing transitive entry is strictly weaker than a missing
///   direct dep).
///
/// Returned map is keyed by the predecessor's `task_id` (the
/// consumer-visible handle a worker indexes its predecessor outputs
/// by). Owned; callers wire it directly into the wire message. Empty
/// map if `task_deps` is empty.
pub fn gather_predecessor_outputs<F1, F2>(
    task_deps: &[TaskDep],
    outputs_for: F1,
    deps_of: F2,
) -> BTreeMap<String, TaskOutputs>
where
    F1: Fn(&PhaseId, &str) -> Option<TaskOutputs>,
    F2: Fn(&PhaseId, &str) -> Option<Vec<TaskDep>>,
{
    let mut result: BTreeMap<String, TaskOutputs> = BTreeMap::new();

    for dep in task_deps {
        insert_outputs_for(&outputs_for, &dep.phase_id, &dep.task_id, &mut result);
        if dep.inherit_outputs {
            walk_ancestry(&outputs_for, &deps_of, &dep.phase_id, &dep.task_id, &mut result);
        }
    }

    result
}

/// Insert the cached outputs for `task_id` into `result`. If the lookup
/// returns `None`, insert an empty [`TaskOutputs`] so the dependent
/// sees a present-but-empty key (the contract documented on the
/// module).
///
/// Idempotent on `result`: a second insert for the same `task_id` is
/// a no-op (the first write wins) — important because the transitive
/// walk and the direct-dep loop can both reach the same ancestor
/// through different paths.
fn insert_outputs_for<F1>(
    outputs_for: &F1,
    phase_id: &PhaseId,
    task_id: &str,
    result: &mut BTreeMap<String, TaskOutputs>,
) where
    F1: Fn(&PhaseId, &str) -> Option<TaskOutputs>,
{
    if result.contains_key(task_id) {
        return;
    }
    let outputs = outputs_for(phase_id, task_id).unwrap_or_default();
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
fn walk_ancestry<F1, F2>(
    outputs_for: &F1,
    deps_of: &F2,
    root_phase: &PhaseId,
    root_id: &str,
    result: &mut BTreeMap<String, TaskOutputs>,
) where
    F1: Fn(&PhaseId, &str) -> Option<TaskOutputs>,
    F2: Fn(&PhaseId, &str) -> Option<Vec<TaskDep>>,
{
    // The visited set is keyed by the full `(phase_id, task_id)`
    // identity: the same `task_id` in two different phases is a
    // distinct ancestor, so de-duping on task_id alone would drop a
    // legitimate cross-phase ancestor.
    let mut visited: HashSet<(PhaseId, String)> = HashSet::new();
    let mut queue: VecDeque<(PhaseId, String)> = VecDeque::new();
    visited.insert((root_phase.clone(), root_id.to_string()));
    queue.push_back((root_phase.clone(), root_id.to_string()));

    while let Some((current_phase, current_id)) = queue.pop_front() {
        let Some(current_deps) = deps_of(&current_phase, &current_id) else {
            // Unknown `(phase_id, task_id)` — pre-apply spawn
            // validation rejects unknown deps, but a snapshot
            // containing only a subset of the run's tasks (late-joiner
            // partial restore) can reach this branch. Skip silently:
            // the direct-dep loop already emitted a present-but-empty
            // key for any missing direct ancestor, and missing
            // transitives are a strictly weaker concern than missing
            // direct deps.
            continue;
        };
        for dep in &current_deps {
            if visited.insert((dep.phase_id.clone(), dep.task_id.clone())) {
                insert_outputs_for(outputs_for, &dep.phase_id, &dep.task_id, result);
                queue.push_back((dep.phase_id.clone(), dep.task_id.clone()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Pin the gather behaviour against the four documented shapes:
    //! direct dep with outputs, direct dep without outputs, transitive
    //! inheritance through one edge, inherit_outputs=false stops the
    //! walk. Plus a cycle-defence smoke test against a hypothetical
    //! self-loop (the validator forbids it but defensive coding lives
    //! here too).
    //!
    //! The closures stand in for any caller-shaped storage (ClusterState
    //! in distributed mode, per-manager HashMap in local mode); the
    //! tests use a pair of HashMaps for clarity.

    use super::*;
    use crate::types::{PhaseId, ResultValue, TaskDep, TaskOutputs};
    use std::collections::{BTreeMap, HashMap};

    fn outputs_with(key: &str, value: &str) -> TaskOutputs {
        let mut m: BTreeMap<String, ResultValue> = BTreeMap::new();
        m.insert(key.to_string(), ResultValue::Inline(value.to_string()));
        TaskOutputs(m)
    }

    /// A single-phase dep for the intra-phase gather shapes. Cross-phase
    /// resolution is exercised by the cluster-state-bound wrapper tests.
    fn dep(task_id: &str, inherit_outputs: bool) -> TaskDep {
        TaskDep {
            task_id: task_id.into(),
            phase_id: PhaseId::from("p"),
            inherit_outputs,
        }
    }

    type OutputsLookup = Box<dyn Fn(&PhaseId, &str) -> Option<TaskOutputs>>;
    type DepsLookup = Box<dyn Fn(&PhaseId, &str) -> Option<Vec<TaskDep>>>;

    /// Build the two lookups from a pair of HashMaps keyed by the full
    /// `(phase_id, task_id)` identity. The closures returned own clones
    /// of the maps so the test bodies stay concise.
    fn lookups(
        outputs: HashMap<(PhaseId, String), TaskOutputs>,
        deps: HashMap<(PhaseId, String), Vec<TaskDep>>,
    ) -> (OutputsLookup, DepsLookup) {
        let outputs_for: OutputsLookup = Box::new(move |phase: &PhaseId, task_id: &str| {
            outputs.get(&(phase.clone(), task_id.to_string())).cloned()
        });
        let deps_of: DepsLookup = Box::new(move |phase: &PhaseId, task_id: &str| {
            deps.get(&(phase.clone(), task_id.to_string())).cloned()
        });
        (outputs_for, deps_of)
    }

    fn key(task_id: &str) -> (PhaseId, String) {
        (PhaseId::from("p"), task_id.to_string())
    }

    #[test]
    fn direct_dep_no_inherit_returns_only_predecessor_outputs() {
        // A → B, no inheritance. The dispatch-time gather for B sees
        // exactly {"A": A's outputs}.
        let a_outputs = outputs_with("nonce", "xyz");
        let mut outputs = HashMap::new();
        outputs.insert(key("A"), a_outputs.clone());
        let deps = HashMap::new();
        let (outputs_for, deps_of) = lookups(outputs, deps);

        let b_deps = vec![dep("A", false)];
        let assembled = gather_predecessor_outputs(&b_deps, outputs_for, deps_of);
        assert_eq!(assembled.len(), 1);
        assert_eq!(assembled.get("A"), Some(&a_outputs));
    }

    #[test]
    fn direct_dep_with_no_recorded_outputs_yields_empty_taskoutputs() {
        // A is referenced but has not recorded outputs. B still
        // receives a present-but-empty key for "A" — the contract
        // lets dependents index by predecessor id without a None
        // check.
        let outputs = HashMap::new();
        let deps = HashMap::new();
        let (outputs_for, deps_of) = lookups(outputs, deps);

        let b_deps = vec![dep("A", false)];
        let assembled = gather_predecessor_outputs(&b_deps, outputs_for, deps_of);
        assert_eq!(assembled.len(), 1);
        assert_eq!(assembled.get("A"), Some(&TaskOutputs::default()));
    }

    #[test]
    fn inherit_outputs_pulls_transitive_ancestor() {
        // A → B → C with C→B carrying inherit_outputs=true. The
        // assembled map for C includes both B (direct) and A
        // (transitive via B's own task_depends_on).
        let a_outputs = outputs_with("x", "1");
        let b_outputs = outputs_with("y", "2");
        let mut outputs = HashMap::new();
        outputs.insert(key("A"), a_outputs.clone());
        outputs.insert(key("B"), b_outputs.clone());
        let mut deps = HashMap::new();
        deps.insert(
            key("B"),
            vec![dep("A", false)],
        );
        let (outputs_for, deps_of) = lookups(outputs, deps);

        let c_deps = vec![dep("B", true)];
        let assembled = gather_predecessor_outputs(&c_deps, outputs_for, deps_of);
        assert_eq!(assembled.len(), 2);
        assert_eq!(assembled.get("B"), Some(&b_outputs));
        assert_eq!(assembled.get("A"), Some(&a_outputs));
    }

    #[test]
    fn inherit_outputs_false_blocks_transitive_walk() {
        // Same A → B → C graph but C→B carries inherit_outputs=false.
        // C sees only {"B": B's outputs}; A's outputs are NOT
        // attached because the inheritance flag gates the walk.
        let b_outputs = outputs_with("y", "2");
        let mut outputs = HashMap::new();
        outputs.insert(key("A"), outputs_with("x", "1"));
        outputs.insert(key("B"), b_outputs.clone());
        let mut deps = HashMap::new();
        deps.insert(
            key("B"),
            vec![dep("A", false)],
        );
        let (outputs_for, deps_of) = lookups(outputs, deps);

        let c_deps = vec![dep("B", false)];
        let assembled = gather_predecessor_outputs(&c_deps, outputs_for, deps_of);
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
        let a_outputs = outputs_with("k", "v");
        let mut outputs = HashMap::new();
        outputs.insert(key("A"), a_outputs.clone());
        let mut deps = HashMap::new();
        deps.insert(
            key("A"),
            vec![dep("A", true)],
        );
        let (outputs_for, deps_of) = lookups(outputs, deps);

        let a_deps = vec![dep("A", true)];
        let assembled = gather_predecessor_outputs(&a_deps, outputs_for, deps_of);
        assert_eq!(assembled.len(), 1);
        assert_eq!(assembled.get("A"), Some(&a_outputs));
    }
}
