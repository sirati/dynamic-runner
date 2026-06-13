//! Wire-canonical task content hash.
//!
//! Single concern: deterministic `String` hash recipe used as the key
//! in every cluster ledger, retry bucket, command-channel address
//! space, and pool predicate. Hoisted to `dynrunner-core` so both the
//! distributed primary path and the local manager path (which also
//! processes `PrimaryCommand` via its own command channel) compute the
//! same value without crossing a manager → manager dependency edge.
//!
//! Module boundary:
//!   * Owns: the canonical hash recipe + the `String` shape every
//!     consumer treats as opaque.
//!   * Does NOT own: any per-backend bookkeeping. Callers store their
//!     own `HashMap<String, _>` keyed on the returned value.
//!
//! Stability contract: the recipe (`{phase_id, path, identifier}`
//! hashed via `std::collections::hash_map::DefaultHasher`, formatted
//! as a 16-char lowercase hex string) is observable on the wire (CRDT
//! ledger keys, `TaskHash` Python bytes). Any change here is a breaking
//! wire change for live runs that mix old + new binaries.
//!
//! Identity: a task's full identity is `(phase_id, task_id)`. The hash
//! folds `phase_id` into the recipe so the SAME `(path, identifier)`
//! declared in two different phases produces two DISTINCT hashes — the
//! phase is a first-class differentiator, not an implicit same-phase
//! default. `task_id` is not folded in directly because it is a
//! consumer-supplied label over the same `(path, identifier)` content;
//! the content + phase pair is the wire-canonical identity.

use crate::{Identifier, TaskInfo};

/// Compute the wire-canonical content hash for a task. Deterministic
/// across runs given identical `(phase_id, path, identifier)` inputs;
/// opaque to callers (treat as an arbitrary string id).
///
/// The same `(path, identifier)` in two different phases hashes to two
/// distinct values — `phase_id` is folded into the recipe so the full
/// `(phase_id, task_id)` task identity is reflected in the wire key.
///
/// Used as the key in (a) the CRDT cluster ledger, (b) every primary
/// command-channel address (FailPermanent, ReinjectTask,
/// UpdatePreferredSecondaries, SpawnTasks duplicate detection), and
/// (c) the local manager's `task_by_hash` mirror that backs the same
/// command channel on the local backend.
pub fn compute_task_hash<I: Identifier>(binary: &TaskInfo<I>) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    binary.phase_id.hash(&mut hasher);
    binary.path.hash(&mut hasher);
    binary.identifier.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AffinityId, PhaseId, SoftPreferredSecondaries, TaskDep, TaskInfo, TypeId};
    use std::path::PathBuf;
    use std::sync::Arc;

    fn mk(phase: &str, content: &str) -> TaskInfo<Arc<str>> {
        TaskInfo {
            path: PathBuf::from(content),
            size: 1,
            identifier: Arc::<str>::from(content),
            phase_id: PhaseId::from(phase),
            type_id: TypeId::from("t"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: "shared-id".into(),
            task_depends_on: Vec::<TaskDep>::new(),
            kind: Default::default(),
            preferred_secondaries: SoftPreferredSecondaries::default(),
            preferred_version: Default::default(),
            resolved_path: None,
        }
    }

    #[test]
    fn same_content_distinct_phase_hashes_differently() {
        // The task identity is `(phase_id, task_id)`: the same content +
        // task_id in two different phases must hash to distinct values.
        let a = mk("phase-A", "/bin/x");
        let b = mk("phase-B", "/bin/x");
        assert_ne!(
            compute_task_hash(&a),
            compute_task_hash(&b),
            "same content in two phases must hash distinctly"
        );
    }

    #[test]
    fn same_content_same_phase_is_stable() {
        // Within one phase the hash is deterministic over identical
        // `(phase, path, identifier)` inputs (the `AffinityId` is not
        // part of the recipe).
        let mut a = mk("phase-A", "/bin/x");
        let b = mk("phase-A", "/bin/x");
        assert_eq!(compute_task_hash(&a), compute_task_hash(&b));
        // Mutating a non-recipe field does not change the hash.
        a.affinity_id = Some(AffinityId::from("aff"));
        assert_eq!(compute_task_hash(&a), compute_task_hash(&b));
    }
}
