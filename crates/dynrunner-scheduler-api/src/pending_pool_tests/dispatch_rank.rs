//! `dependent_dispatch_rank` tests (#336 P3a): the would-be dispatch
//! standing of the WORK tasks gated (transitively) on a setup (upload)
//! task's id — the ordering key the primary uses to route the upload whose
//! dependents are most dispatch-imminent FIRST (instead of FIFO).
//!
//! The rank derives from the SAME phase-state + affinity reads the dispatch
//! view uses (no soft-pin logic duplicated) and walks the pool's ONE
//! dependency reverse-index `dependents_of`, recursing through non-Work
//! pass-through nodes (a #497 `SecondaryAffine` import gate between an upload
//! and its builds) to the Work leaves. Mirrors the `query_predicates`
//! fixture style.

use dynrunner_core::{PhaseId, TaskDep, TaskInfo, TaskKind};

use super::{DispatchRank, pool_with, t};

/// Build a `TaskInfo<()>` with an explicit id, kind, affinity, and same-phase
/// deps so a dependent can be parked in the task-level `blocked` map and
/// classified typed-vs-free-pool. An empty affinity → free-pool sentinel.
fn task(
    phase: &str,
    affinity: &str,
    id: &str,
    kind: TaskKind,
    deps: &[&str],
) -> TaskInfo<()> {
    let mut item = t(phase, "T", affinity, 1);
    item.task_id = id.to_string();
    item.kind = kind;
    item.task_depends_on = deps
        .iter()
        .map(|d| TaskDep {
            task_id: d.to_string(),
            phase_id: PhaseId::from(phase),
            inherit_outputs: false,
            def_id: None,
        })
        .collect();
    item
}

/// An upload setup task with the given id (no deps — it is the dep ROOT).
fn upload(phase: &str, id: &str) -> TaskInfo<()> {
    task(phase, "", id, TaskKind::Setup, &[])
}

/// A Work build task on `phase`/`affinity` depending on `dep`.
fn build(phase: &str, affinity: &str, id: &str, dep: &str) -> TaskInfo<()> {
    task(phase, affinity, id, TaskKind::Work, &[dep])
}

/// 1. No dependent known yet (the upload's id has no `dependents_of` entry)
///    ⇒ `None`. The caller maps this to `DispatchRank::WORST` (route last).
#[test]
fn rank_none_when_no_dependent_known() {
    let mut p = pool_with(&["P"], &[]);
    // Only the upload itself is in the pool; nothing depends on it.
    p.extend([upload("P", "up")]).expect("valid extend");
    assert_eq!(
        p.dependent_dispatch_rank("up"),
        None,
        "an upload with no known dependent has no rank (routes last)"
    );
}

/// 2. The rank reflects the BEST (min) dependent: an upload feeding a
///    typed-affinity build outranks one feeding a free-pool build, all in an
///    Active phase. (typed class_tier 0 < free-pool class_tier 1.)
#[test]
fn rank_reflects_best_dependent() {
    let mut p = pool_with(&["P"], &[]);
    // up_typed → a typed (affinity "x") build; up_free → a free-pool build.
    p.extend([
        upload("P", "up_typed"),
        upload("P", "up_free"),
        build("P", "x", "b_typed", "up_typed"),
        build("P", "", "b_free", "up_free"),
    ])
    .expect("valid extend");

    let typed = p
        .dependent_dispatch_rank("up_typed")
        .expect("has a dependent");
    let free = p
        .dependent_dispatch_rank("up_free")
        .expect("has a dependent");
    assert_eq!(typed.class_tier, 0, "typed dependent → class_tier 0");
    assert_eq!(free.class_tier, 1, "free-pool dependent → class_tier 1");
    assert!(
        typed < free,
        "the upload feeding a typed build ranks BETTER (min) than the free-pool one"
    );
}

/// 3. The asm-dataset GROUP case: a shared upload feeding MANY builds outranks
///    a single-dependent upload at the SAME (phase, class) tier, via the
///    negated-dependent-count tiebreak.
#[test]
fn shared_upload_outranks_single_by_dependent_count() {
    let mut p = pool_with(&["P"], &[]);
    // group_common feeds 3 free-pool builds; delta feeds 1 free-pool build.
    p.extend([
        upload("P", "group_common"),
        upload("P", "delta"),
        build("P", "", "g1", "group_common"),
        build("P", "", "g2", "group_common"),
        build("P", "", "g3", "group_common"),
        build("P", "", "d1", "delta"),
    ])
    .expect("valid extend");

    let group = p
        .dependent_dispatch_rank("group_common")
        .expect("has dependents");
    let delta = p.dependent_dispatch_rank("delta").expect("has a dependent");
    assert_eq!(
        (group.phase_tier, group.class_tier),
        (delta.phase_tier, delta.class_tier),
        "both are equal (phase, class) tier"
    );
    assert_eq!(group.neg_dependent_count, -3, "group feeds 3 builds");
    assert_eq!(delta.neg_dependent_count, -1, "delta feeds 1 build");
    assert!(
        group < delta,
        "the shared group_common upload routes ahead of the single-dependent delta"
    );
}

/// 4. A dependent in a Blocked (not-yet-Active) phase ranks WORSE than one in
///    an Active phase: phase_tier 2 (will-activate) vs 0 (active now).
#[test]
fn dependent_in_blocked_phase_ranks_worse_than_active() {
    // Phase "Q" depends on phase "P" → Q starts Blocked.
    let mut p = pool_with(&["P", "Q"], &[("Q", &["P"])]);
    // An upload on P feeding a build on the Active phase P; another upload on
    // Q feeding a build on the Blocked phase Q.
    p.extend([
        upload("P", "up_active"),
        upload("Q", "up_blocked"),
        build("P", "", "b_active", "up_active"),
        build("Q", "", "b_blocked", "up_blocked"),
    ])
    .expect("valid extend");

    let active = p
        .dependent_dispatch_rank("up_active")
        .expect("has a dependent");
    let blocked = p
        .dependent_dispatch_rank("up_blocked")
        .expect("has a dependent");
    assert_eq!(active.phase_tier, 0, "Active-phase dependent → phase_tier 0");
    assert_eq!(
        blocked.phase_tier, 2,
        "Blocked-phase (will-activate) dependent → phase_tier 2"
    );
    assert!(
        active < blocked,
        "an upload feeding an Active-phase build routes ahead of one feeding a Blocked-phase build"
    );
}

/// TRANSITIVE-THROUGH-AFFINE (#497): upload → I (SecondaryAffine import gate)
/// → B (Work build). The import is a pass-through node — it never dispatches
/// to a worker — so the rank walk recurses THROUGH it to the build leaf, and
/// the upload's rank reflects the BUILD's standing, not the gate's.
#[test]
fn rank_walks_transitively_through_affine_import_to_build() {
    let mut p = pool_with(&["P"], &[]);
    // upload (Setup) → import (SecondaryAffine, dep upload) → build (Work, dep import).
    p.extend([
        upload("P", "upload"),
        task("P", "x", "import", TaskKind::SecondaryAffine, &["upload"]),
        build("P", "x", "build", "import"),
    ])
    .expect("valid extend");

    let rank = p
        .dependent_dispatch_rank("upload")
        .expect("a Work leaf is reachable through the import gate");
    // The reachable leaf is the typed (affinity "x") Active-phase BUILD.
    assert_eq!(rank.phase_tier, 0, "the build's phase is Active");
    assert_eq!(rank.class_tier, 0, "the build is typed (affinity x)");
    assert_eq!(
        rank.neg_dependent_count, -1,
        "exactly one Work leaf is reachable (the import gate itself does not score)"
    );
}

/// TRANSITIVE ordering: an upload that transitively feeds a dispatch-imminent
/// (Active, typed) build THROUGH an affine import routes BEFORE an upload that
/// directly feeds a late (Blocked-phase) build — proving the transitive walk
/// is what determines the order, not the direct-dependent shape.
#[test]
fn transitive_imminent_build_outranks_direct_late_build() {
    // "Late" depends on "Early" → Late starts Blocked.
    let mut p = pool_with(&["Early", "Late"], &[("Late", &["Early"])]);
    // up_A → import (affine) → build_early (Active, typed): dispatch-imminent.
    // up_B → build_late (Blocked phase): late.
    p.extend([
        upload("Early", "up_A"),
        task("Early", "x", "import", TaskKind::SecondaryAffine, &["up_A"]),
        build("Early", "x", "build_early", "import"),
        upload("Late", "up_B"),
        build("Late", "", "build_late", "up_B"),
    ])
    .expect("valid extend");

    let a = p
        .dependent_dispatch_rank("up_A")
        .expect("reaches build_early through the import");
    let b = p.dependent_dispatch_rank("up_B").expect("feeds build_late");
    assert!(
        a < b,
        "up_A (transitively feeding the imminent Active build) routes BEFORE up_B \
         (feeding the late Blocked-phase build): {a:?} vs {b:?}"
    );
}

/// `DispatchRank::WORST` sorts strictly AFTER any real rank — the contract the
/// P3b picker relies on so an unranked (no-known-dependent) upload routes last.
#[test]
fn worst_sentinel_sorts_after_any_real_rank() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([upload("P", "up"), build("P", "", "b", "up")])
        .expect("valid extend");
    let real = p.dependent_dispatch_rank("up").expect("has a dependent");
    assert!(
        real < DispatchRank::WORST,
        "a real rank is always better (sooner) than the WORST sentinel"
    );
}
