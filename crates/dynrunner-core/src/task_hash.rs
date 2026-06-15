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
//! Stability contract: the recipe (`{phase_id, type_id, task_id, path,
//! identifier}` hashed via `std::collections::hash_map::DefaultHasher`,
//! formatted as a 16-char lowercase hex string) is observable on the
//! wire (CRDT ledger keys, `TaskHash` Python bytes). Any change here is
//! a breaking wire change for live runs that mix old + new binaries.
//!
//! Identity: a task's full identity is `(phase_id, task_id)` — and
//! `task_id` is part of the consumer-supplied CONTEXT that
//! distinguishes the unit of work. The hash folds every identity
//! component into the recipe so the receiver-side ledger
//! (`HashMap<task_hash, TaskState<I>>`) and every command-channel
//! variant addressing tasks by hash (`FailPermanent`, `ReinjectTask`,
//! `UpdatePreferredSecondaries`, the `SpawnTasks` within-batch dedup)
//! distinguish two tasks that share `(path, identifier)` in the same
//! phase but differ in `(type_id, task_id)` — the canonical
//! TWO-TASK-TYPES-OVER-ONE-INPUT shape (e.g. a `build_index` phase
//! that emits a `realized_lengths` and a `sorted_index` task over the
//! same binary). Folding `phase_id` keeps the same `(path, identifier,
//! task_id)` declared in two different phases as DISTINCT tasks; the
//! phase remains a first-class differentiator.
//!
//! Wire-format change (#590): the previous recipe omitted `type_id` and
//! `task_id`, which silently collapsed those distinct same-phase tasks
//! to one ledger key. A consumer that pre-staged artifacts against the
//! old hash MUST recompute against the new recipe — the old hash was
//! underspecified for any phase with two task types over the same
//! `(path, identifier)`.

use crate::{Identifier, TaskInfo};

/// Compute the wire-canonical task hash. Deterministic across runs
/// given identical `(phase_id, type_id, task_id, path, identifier)`
/// inputs; opaque to callers (treat as an arbitrary string id).
///
/// The recipe folds every component of the task's identity
/// (`phase_id`, `type_id`, `task_id`) together with its content
/// (`path`, `identifier`) so two tasks that share content but differ
/// in any identity component produce distinct hashes. The canonical
/// case is a phase with two task types running over the same input
/// (e.g. `build_index`'s `realized_lengths` + `sorted_index` over one
/// binary): identical `(path, identifier)`, distinct `(type_id,
/// task_id)`, distinct hashes — distinct entries in the cluster ledger
/// and the within-batch dedup set.
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
    binary.type_id.hash(&mut hasher);
    binary.task_id.hash(&mut hasher);
    binary.path.hash(&mut hasher);
    binary.identifier.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        AffinityId, PhaseId, SoftPreferredSecondaries, TaskDep, TaskInfo, TaskKind, TypeId,
    };
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
            setup_affinity: None,
            preferred_secondaries: SoftPreferredSecondaries::default(),
            preferred_version: Default::default(),
            upload_file: None,
            required_files: None,
            resolved_path: None,
        }
    }

    #[test]
    fn same_content_distinct_phase_hashes_differently() {
        // The task identity folds `phase_id`: the same content +
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
    fn same_content_same_phase_same_identity_is_stable() {
        // Within one phase, with identical `(type_id, task_id, path,
        // identifier)`, the hash is deterministic. Non-recipe fields
        // (e.g. `affinity_id`) do not affect the hash.
        let mut a = mk("phase-A", "/bin/x");
        let b = mk("phase-A", "/bin/x");
        assert_eq!(compute_task_hash(&a), compute_task_hash(&b));
        // Mutating a non-recipe field does not change the hash.
        a.affinity_id = Some(AffinityId::from("aff"));
        assert_eq!(compute_task_hash(&a), compute_task_hash(&b));
    }

    #[test]
    fn secondary_affine_kind_excluded_from_hash() {
        // The hash recipe is `{phase_id, type_id, task_id, path,
        // identifier}` — `kind` is not folded in, so changing the kind
        // never changes the ledger key. Two tasks identical but for
        // kind ∈ {Work, SecondaryAffine} must produce EQUAL hashes
        // (same key in the cluster ledger), exactly as for `Setup`.
        let work = mk("phase-A", "/bin/x");
        let mut affine = mk("phase-A", "/bin/x");
        affine.kind = TaskKind::SecondaryAffine;
        assert_eq!(
            compute_task_hash(&work),
            compute_task_hash(&affine),
            "kind is not part of the hash recipe"
        );
    }

    /// #590 regression: the canonical two-task-types-over-one-input
    /// shape — same phase, same `(path, identifier)`, distinct
    /// `(type_id, task_id)` (e.g. `build_index`'s `realized_lengths`
    /// and `sorted_index` over one binary) — must hash distinctly so
    /// the receiver-side ledger (`HashMap<task_hash, TaskState<I>>`)
    /// holds BOTH entries and the within-batch dedup set distinguishes
    /// them.
    ///
    /// Pre-fix this asserted EQUAL hashes (the bug). The inverted
    /// assertion locks in the corrected recipe.
    #[test]
    fn same_content_same_phase_distinct_type_id_hashes_differently() {
        let mut rlen = mk("index", "nping");
        let mut sidx = mk("index", "nping");
        rlen.type_id = TypeId::from("realized_lengths");
        rlen.task_id = "rlen:nping".into();
        sidx.type_id = TypeId::from("sorted_index");
        sidx.task_id = "sidx:nping".into();
        assert_ne!(
            compute_task_hash(&rlen),
            compute_task_hash(&sidx),
            "distinct (type_id, task_id) over identical content must hash distinctly \
             (pre-#590 the hash dropped these into the same ledger slot)"
        );
    }

    /// #590 regression: distinct `task_id` alone (same `type_id`,
    /// same content) must also hash distinctly. The consumer-supplied
    /// `task_id` is part of the task's identity context, not a label
    /// over the same content.
    #[test]
    fn same_content_distinct_task_id_alone_hashes_differently() {
        let mut a = mk("phase-A", "/bin/x");
        let mut b = mk("phase-A", "/bin/x");
        a.task_id = "first".into();
        b.task_id = "second".into();
        assert_ne!(
            compute_task_hash(&a),
            compute_task_hash(&b),
            "distinct task_id over identical content must hash distinctly"
        );
    }

    /// #590 regression: distinct `type_id` alone (same `task_id`,
    /// same content) must also hash distinctly. Pins that BOTH
    /// identity components contribute independently.
    #[test]
    fn same_content_distinct_type_id_alone_hashes_differently() {
        let mut a = mk("phase-A", "/bin/x");
        let mut b = mk("phase-A", "/bin/x");
        a.type_id = TypeId::from("ta");
        b.type_id = TypeId::from("tb");
        assert_ne!(
            compute_task_hash(&a),
            compute_task_hash(&b),
            "distinct type_id over identical content must hash distinctly"
        );
    }

    /// #590 wire-format change: ONE-LINE doc pin so a future reader
    /// who lands here understands the hash recipe is post-#590. The
    /// pre-#590 hash recipe `{phase_id, path, identifier}` was
    /// underspecified — folding `(type_id, task_id)` is the
    /// correctness fix, not a wire-stability regression. Pre-staged
    /// artifacts that relied on the old recipe MUST recompute. This
    /// test is a CONSTRUCTION-time assertion: if the recipe ever
    /// regresses to the pre-#590 shape, this test fires.
    #[test]
    fn post_590_recipe_distinguishes_all_identity_components() {
        // Baseline: a fully-distinct task.
        let base = mk("phase-A", "/bin/x");
        // Mutating ANY recipe component changes the hash.
        let mut variant_phase = base.clone();
        variant_phase.phase_id = PhaseId::from("phase-B");
        let mut variant_type = base.clone();
        variant_type.type_id = TypeId::from("t2");
        let mut variant_task = base.clone();
        variant_task.task_id = "other".into();
        let mut variant_path = base.clone();
        variant_path.path = PathBuf::from("/bin/y");
        let mut variant_ident = base.clone();
        variant_ident.identifier = Arc::<str>::from("/bin/y");
        let baseline_hash = compute_task_hash(&base);
        assert_ne!(baseline_hash, compute_task_hash(&variant_phase));
        assert_ne!(baseline_hash, compute_task_hash(&variant_type));
        assert_ne!(baseline_hash, compute_task_hash(&variant_task));
        assert_ne!(baseline_hash, compute_task_hash(&variant_path));
        assert_ne!(baseline_hash, compute_task_hash(&variant_ident));
    }
}
