//! Scheduling-seam tests for the setup-task primitive (P1 seam (a)).
//!
//! A `TaskKind::Setup` task is NEVER worker-assignable: it must be
//! invisible to every worker-dispatch read path (`view_for_worker`,
//! `pop_for_worker`) while still occupying its bucket (so it holds its
//! phase open until its in-process executor — a later phase — consumes
//! it). A `TaskKind::Work` task is unaffected.

use super::{phase, pool_with, setup_t, t};

/// A `Setup` task in an Active phase is NEVER exposed to a worker view,
/// and a worker `pop_for_worker` against it returns `None` — yet it is
/// still QUEUED (it holds the phase open). This is the scheduling seam:
/// the kind, not a separate flag, excludes it from worker dispatch.
#[test]
fn setup_task_never_appears_in_worker_view() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([setup_t("P", "T", "", 10)]).expect("valid extend");

    // It IS in the pool (queued, holding the phase open) ...
    assert_eq!(p.len(), 1, "the setup task is queued (holds the phase open)");
    // ... but invisible to a worker view ...
    let view = p.view_for_worker(1, None);
    assert!(
        view.is_empty(),
        "a Setup task must never appear in a worker dispatch view"
    );
    // ... and a worker pop finds nothing dispatchable.
    assert!(
        p.pop_for_worker(1).is_none(),
        "a Setup task must never be popped for a worker"
    );
    // Nothing went in-flight (nothing was dispatched).
    assert_eq!(p.in_flight(&phase("P")), 0);
}

/// A `Work` task sharing the phase with a `Setup` task IS dispatched
/// normally; only the `Setup` task is held back. Proves the exclusion
/// is keyed on the kind, not a blanket phase suppression.
#[test]
fn work_task_dispatches_while_setup_task_held_back() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([setup_t("P", "T", "", 10), t("P", "T", "", 20)])
        .expect("valid extend");

    // The view shows exactly the one Work task.
    let view = p.view_for_worker(1, None);
    assert_eq!(
        view.len(),
        1,
        "only the Work task is worker-visible; the Setup task is held back"
    );
    assert_eq!(view.as_slice()[0].size, 20, "the visible task is the Work one");

    // Popping for a worker yields the Work task and nothing else.
    let popped = p.pop_for_worker(1).expect("the Work task is dispatchable");
    assert_eq!(popped.size, 20);
    assert!(
        p.pop_for_worker(2).is_none(),
        "after the Work task is taken, only the (non-dispatchable) Setup task remains"
    );
    // The setup task is still queued (1 in-flight Work + 1 queued Setup).
    assert_eq!(p.len(), 2, "setup task still queued, work task now in-flight");
    assert_eq!(p.in_flight(&phase("P")), 1);
}
