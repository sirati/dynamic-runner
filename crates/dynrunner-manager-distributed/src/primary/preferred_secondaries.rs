//! Soft per-task secondary preferences: predicate builder + validator.
//!
//! Single concern of this module: turn `TaskInfo::preferred_secondaries`
//! into two consumer-facing primitives that the rest of the primary
//! coordinator wires in without learning the soft-preference semantics
//! itself.
//!
//! * [`apply_preferred_secondaries_predicate`] — builds a
//!   `Fn(&TaskInfo<I>) -> Ordering` keyed on a specific `secondary_id`.
//!   Passed through `PendingPool::view_for_worker`'s
//!   `preference_predicate` slot so the existing four-class scheduler
//!   ordering (pin / typed / free / co-pin) keeps its class boundaries
//!   intact and the predicate only re-orders within each class. The
//!   helper is independent of any coordinator state — caller owns the
//!   `secondary_id` choice; we just produce the closure.
//!
//! * [`PreferredSecondariesValidator`] — bookkeeping for "this task
//!   names a secondary id the cluster has never heard of". Single
//!   responsibility: dedup WARN logs so an operator who configured a
//!   typoed id sees the message ONCE per offending id rather than once
//!   per task per validation cycle. The validator does not own the
//!   known-secondaries set nor the task list; the caller supplies both
//!   per `validate` call and decides when to invoke (post-seed,
//!   post-PeerJoined apply, etc.). On PeerJoined the caller invokes
//!   [`PreferredSecondariesValidator::forget`] for the joined id so
//!   the next validation cycle treats it as fresh: if the id is now
//!   actually known, no warn will fire; if some other task still
//!   references an unknown id, the dedup state for THAT id is
//!   unaffected.
//!
//! The two primitives intentionally live side-by-side: both concern
//! `TaskInfo::preferred_secondaries` exclusively, share zero state,
//! and are the entire surface the rest of `primary/` needs to honour
//! soft preferences. Adding a sibling preference dimension in the
//! future (e.g. "preferred type families") would land as a parallel
//! module rather than as additions to either of these two types.

use std::cmp::Ordering;
use std::collections::HashSet;

use dynrunner_core::{Identifier, TaskInfo};

/// Build a [`view_for_worker`]-compatible preference predicate that
/// ranks tasks lower (sorts first within their class) when their
/// `preferred_secondaries` list contains `secondary_id`.
///
/// The predicate returns `Ordering::Less` for "preferred for this
/// secondary" and `Ordering::Equal` otherwise — no `Greater` branch,
/// because the within-class sort is stable and equal-keyed items
/// preserve their construction-time FIFO order. The intended call site
/// is `PendingPool::view_for_worker(global_wid, Some(&pred))` AFTER
/// `cap_filter_view` has already dropped over-cap items: caps are a
/// hard constraint, preferences are a tie-break.
///
/// [`view_for_worker`]: dynrunner_scheduler_api::PendingPool::view_for_worker
pub fn apply_preferred_secondaries_predicate<'a, I: Identifier>(
    secondary_id: &'a str,
) -> impl Fn(&TaskInfo<I>) -> Ordering + 'a {
    move |task| {
        if task
            .preferred_secondaries
            .as_slice()
            .iter()
            .any(|s| s.as_str() == secondary_id)
        {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }
}

/// Tracks which "preferred secondary id is not in the known-set"
/// warnings have already been emitted so repeat validation cycles
/// don't spam the log.
///
/// The validator owns only its dedup set; every per-call input
/// (known-secondaries set, task iterator) is supplied by the caller
/// per [`Self::validate`] invocation. Callers invoke `validate` once
/// after the initial cluster seed and once per peer-membership change
/// that could resolve a previously-unknown id (paired with a
/// [`Self::forget`] for the changed id).
#[derive(Debug, Default)]
pub struct PreferredSecondariesValidator {
    /// Ids we've already emitted a warn for. A second validation cycle
    /// that still finds the id unresolved is silenced; an explicit
    /// `forget` clears the entry so a fresh warn can fire if the id
    /// remains unknown after a join that did not resolve it.
    warned: HashSet<String>,
}

impl PreferredSecondariesValidator {
    /// Construct an empty validator. The dedup set starts empty so the
    /// very first `validate` call emits warns for every offending id.
    pub fn new() -> Self {
        Self::default()
    }

    /// Walk `tasks` and emit one structured `warn` log per unique
    /// preferred-secondary id that is not in `known`. Re-validation
    /// cycles silence ids the validator has already warned about; a
    /// caller that wants a previously-warned id reconsidered must call
    /// [`Self::forget`] first.
    ///
    /// Log shape:
    /// ```text
    /// target: "dynrunner_preferred_secondaries"
    /// event: "unknown_preferred_secondary"
    /// secondary_id: <offending id>
    /// known_count: <known.len()>
    /// ```
    /// Operators searching `event=unknown_preferred_secondary` get a
    /// stable token across releases.
    pub fn validate<'a, I, T, K>(&mut self, tasks: T, known: &HashSet<K>)
    where
        I: Identifier + 'a,
        T: IntoIterator<Item = &'a TaskInfo<I>>,
        K: std::borrow::Borrow<str> + std::hash::Hash + Eq,
    {
        // Re-build the known-set as `&str` keys for the membership
        // check so callers can hand us either `&HashSet<String>` or
        // `&HashSet<&str>` without forcing an extra allocation here.
        // `Borrow<str>` covers both shapes.
        for task in tasks {
            for id in task.preferred_secondaries.as_slice() {
                if known.iter().any(|k| k.borrow() == id.as_str()) {
                    continue;
                }
                if self.warned.insert(id.clone()) {
                    let known_count = known.len();
                    tracing::warn!(
                        target: "dynrunner_preferred_secondaries",
                        event = "unknown_preferred_secondary",
                        secondary_id = %id,
                        known_count,
                        "task names a preferred secondary id that is not in the known-secondaries set"
                    );
                }
            }
        }
    }

    /// Drop `id` from the dedup set. Used when a peer-membership event
    /// makes an id potentially resolvable — the next `validate` cycle
    /// will re-evaluate from scratch for this id and either keep silent
    /// (now known) or emit a fresh warn (still unknown, e.g. because
    /// the apply-rule fanout outpaced the actual peer registration).
    pub fn forget(&mut self, id: &str) {
        self.warned.remove(id);
    }

    /// Snapshot of the currently-suppressed-by-dedup id set. Test-only
    /// inspection surface; production code uses `validate` + `forget`.
    #[cfg(test)]
    pub(crate) fn warned_snapshot(&self) -> HashSet<String> {
        self.warned.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_core::{AffinityId, PhaseId, SoftPreferredSecondaries, TypeId};
    use dynrunner_scheduler_api::PendingPool;

    /// Minimal `TaskInfo<()>` fixture. Lives next to the validator
    /// tests rather than reusing `primary/test_helpers.rs::make_binary`
    /// (which is a `TaskInfo<TestId>` shaped for the in-process
    /// distributed pipeline) so the unit tests here stay independent
    /// of the heavier test harness.
    fn task_with_prefs(name: &str, prefs: &[&str]) -> TaskInfo<()> {
        TaskInfo {
            path: std::path::PathBuf::from(format!("/tmp/{name}")),
            size: 1,
            identifier: (),
            phase_id: PhaseId::from("P"),
            type_id: TypeId::from("T"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: None,
            task_depends_on: vec![],
            preferred_secondaries: SoftPreferredSecondaries::new(
                prefs.iter().map(|s| (*s).to_string()).collect(),
            ),
            resolved_path: None,
        }
    }

    /// Predicate maps every task whose `preferred_secondaries` lists
    /// the supplied id to `Less` and every other task to `Equal`.
    #[test]
    fn predicate_marks_preferred_tasks_less() {
        let pred = apply_preferred_secondaries_predicate::<()>("secondary-2");
        let prefers = task_with_prefs("a", &["secondary-2"]);
        let other = task_with_prefs("b", &["secondary-7"]);
        let empty = task_with_prefs("c", &[]);
        assert_eq!(pred(&prefers), Ordering::Less);
        assert_eq!(pred(&other), Ordering::Equal);
        assert_eq!(pred(&empty), Ordering::Equal);
    }

    /// `validate` emits exactly once per unknown id even when many
    /// tasks reference the same id. `forget` clears the entry so a
    /// subsequent validate cycle treats the id as fresh.
    #[test]
    fn validate_dedups_unknown_id_until_forget() {
        let mut v = PreferredSecondariesValidator::new();
        let tasks = [
            task_with_prefs("a", &["secondary-unknown"]),
            task_with_prefs("b", &["secondary-unknown"]),
        ];
        let known: HashSet<String> = HashSet::new();
        v.validate(tasks.iter(), &known);
        assert!(v.warned_snapshot().contains("secondary-unknown"));
        assert_eq!(v.warned_snapshot().len(), 1);

        // Second cycle: same input — still tracked, still no fresh add.
        v.validate(tasks.iter(), &known);
        assert_eq!(v.warned_snapshot().len(), 1);

        // Forget then re-validate: tracked again from scratch.
        v.forget("secondary-unknown");
        assert!(v.warned_snapshot().is_empty());
        v.validate(tasks.iter(), &known);
        assert!(v.warned_snapshot().contains("secondary-unknown"));
    }

    /// Ids present in the known-set are never warned.
    #[test]
    fn validate_silent_when_id_in_known_set() {
        let mut v = PreferredSecondariesValidator::new();
        let tasks = [task_with_prefs("a", &["secondary-known"])];
        let known: HashSet<String> = [String::from("secondary-known")]
            .into_iter()
            .collect();
        v.validate(tasks.iter(), &known);
        assert!(v.warned_snapshot().is_empty());
    }

    /// Fixture: same `t()` shape as the scheduler-api pending-pool
    /// tests so the priority-class ordering assertions read against
    /// the same mental model — phase / type / affinity / size /
    /// preferred_secondaries.
    fn t(
        phase: &str,
        ty: &str,
        affinity: &str,
        size: u64,
        prefs: &[&str],
    ) -> TaskInfo<()> {
        TaskInfo {
            path: std::path::PathBuf::from(format!(
                "/tmp/{phase}_{ty}_{affinity}_{size}"
            )),
            size,
            identifier: (),
            phase_id: PhaseId::from(phase),
            type_id: TypeId::from(ty),
            affinity_id: if affinity.is_empty() {
                None
            } else {
                Some(AffinityId::from(affinity))
            },
            payload: serde_json::Value::Null,
            task_id: None,
            task_depends_on: vec![],
            preferred_secondaries: SoftPreferredSecondaries::new(
                prefs.iter().map(|s| (*s).to_string()).collect(),
            ),
            resolved_path: None,
        }
    }

    /// Predicate plugged into `view_for_worker` re-orders within a
    /// priority class without ever lifting a lower-class task over a
    /// higher-class one. Mirrors the scheduler-api
    /// `view_for_worker_predicate_sorts_within_priority_class` shape
    /// but exercises the manager-distributed-side helper end-to-end:
    /// build the predicate from a secondary id, feed it through
    /// `view_for_worker`, observe the four-class boundaries hold.
    #[test]
    fn preferred_secondaries_predicate_promotes_preferred_within_class() {
        let phases = vec![PhaseId::from("P")];
        let mut pool = PendingPool::<()>::new(
            phases,
            std::collections::HashMap::new(),
        )
        .expect("valid graph");
        pool.extend([
            // alpha bucket — will become worker 1's pin after the first pop.
            //
            // Two pinned-class items: the FIRST has no preference;
            // the SECOND prefers `secondary-target`. With the
            // predicate active, the second must overtake the first
            // INSIDE the pin class.
            t("P", "T", "alpha", 100, &[]),
            t("P", "T", "alpha", 200, &["secondary-target"]),
            // beta bucket — unpinned typed; a preferred item must
            // still sit BEHIND every pinned item even though the
            // predicate marks it `Less`. The class boundary wins.
            t("P", "T", "beta", 1, &["secondary-target"]),
            t("P", "T", "beta", 2, &[]),
            // free pool.
            t("P", "T", "", 50, &[]),
        ])
        .expect("valid extend");
        // Worker 1 grabs alpha first so alpha becomes its pin.
        let _ = pool.pop_for_worker(1).unwrap();

        let pred = apply_preferred_secondaries_predicate::<()>("secondary-target");
        let view = pool.view_for_worker(1, Some(&pred));
        let sizes: Vec<u64> = view.as_slice().iter().map(|t| t.size).collect();
        // Expected:
        //   class 0 (pin = alpha): [200]              — only one item remains
        //                                               after the pop; size 200
        //                                               happens to be the
        //                                               preferred one too.
        //   class 1 (typed = beta): [1, 2]            — preferred sorts first
        //                                               WITHIN the class; size
        //                                               1 (preferred) precedes
        //                                               size 2 (non-preferred).
        //   class 2 (free):         [50]
        //   class 3 (co-pin):       []
        assert_eq!(
            sizes,
            vec![200, 1, 2, 50],
            "preferred task must sort first within its class without breaking \
             the class boundary"
        );
    }

    /// Tasks without any `preferred_secondaries` and a predicate that
    /// always returns `Equal` for them produce the same item order
    /// that `view_for_worker(.., None)` would have built. The stable
    /// sort preserves the construction-time FIFO order across the
    /// `Equal` keys.
    #[test]
    fn preferred_secondaries_predicate_empty_list_is_noop() {
        let phases = vec![PhaseId::from("P")];
        let mut pool = PendingPool::<()>::new(
            phases,
            std::collections::HashMap::new(),
        )
        .expect("valid graph");
        pool.extend([
            t("P", "T", "alpha", 1, &[]),
            t("P", "T", "alpha", 2, &[]),
            t("P", "T", "beta", 3, &[]),
            t("P", "T", "", 4, &[]),
        ])
        .expect("valid extend");
        let _ = pool.pop_for_worker(1).unwrap();

        let pred = apply_preferred_secondaries_predicate::<()>("secondary-any");
        let with_pred: Vec<u64> = pool
            .view_for_worker(1, Some(&pred))
            .as_slice()
            .iter()
            .map(|t| t.size)
            .collect();
        let without_pred: Vec<u64> = pool
            .view_for_worker(1, None)
            .as_slice()
            .iter()
            .map(|t| t.size)
            .collect();
        assert_eq!(
            with_pred, without_pred,
            "empty preferred_secondaries means the predicate is a no-op"
        );
        assert_eq!(with_pred, vec![2, 3, 4]);
    }
}
