//! Tests for the AF-id affine state layer: affine-id allocation/agreement,
//! the per-secondary bitvector cell apply + LWW merge, the
//! snapshot/digest/restore round-trip, and the failover gen-floor resume.

use super::*;
use crate::cluster_state::SecondaryCellId;
use dynrunner_core::TaskKind;
use dynrunner_protocol_primary_secondary::SecondaryCell;

/// A `TaskKind::SecondaryAffine` task fixture (twin of `mk_task`).
fn mk_affine_task(name: &str) -> TaskInfo<RunnerIdentifier> {
    let mut t = mk_task(name);
    t.kind = TaskKind::SecondaryAffine;
    t
}

/// A `TaskKind::SecondaryAffine` task in a chosen phase (twin of `mk_task_in`).
fn mk_affine_task_in(name: &str, phase: &str) -> TaskInfo<RunnerIdentifier> {
    let mut t = mk_affine_task(name);
    t.phase_id = PhaseId::from(phase);
    t
}

/// A `Work` task in a chosen phase (twin of `mk_task`, phase-parameterized).
fn mk_work_task_in(name: &str, phase: &str) -> TaskInfo<RunnerIdentifier> {
    let mut t = mk_task(name);
    t.phase_id = PhaseId::from(phase);
    t
}

/// Originating a `SecondaryAffine` `TaskAdded` through the broadcast choke
/// point reserves an affine-id, INJECTS a paired `SecondaryCellRegistered`,
/// and binds the def's content hash to that affine-id. A NON-affine task gets
/// NO affine-id (the bitvector tracks only the affine subset — dense).
#[test]
fn originate_secondary_affine_allocates_and_registers_affine_id() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let batch = vec![
        ClusterMutation::TaskAdded {
            hash: "h-affine".into(),
            task: mk_affine_task("imp"),
            def_id: None,
        },
        ClusterMutation::TaskAdded {
            hash: "h-work".into(),
            task: mk_task("work"),
            def_id: None,
        },
    ];
    let applied = crate::cluster_state::apply_locally_for_broadcast(&mut a, batch);
    // The two TaskAdded + the ONE injected registration for the affine task.
    let registrations = applied
        .applied
        .iter()
        .filter(|m| matches!(m, ClusterMutation::SecondaryCellRegistered { .. }))
        .count();
    assert_eq!(registrations, 1, "exactly one affine registration injected");
    // The affine task's hash is bound to an affine-id; the work task is not.
    assert!(a.affine_id_for_hash("h-affine").is_some());
    assert!(a.affine_id_for_hash("h-work").is_none());
}

/// Two replicas applying the SAME originated batch (the injected registration
/// included) bind the affine def's content to the SAME affine-id — the
/// CRDT-agreement the wire-carried id exists for, regardless of arrival order.
#[test]
fn two_replicas_agree_on_affine_id() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let batch = vec![
        ClusterMutation::TaskAdded {
            hash: "h0".into(),
            task: mk_affine_task("i0"),
            def_id: None,
        },
        ClusterMutation::TaskAdded {
            hash: "h1".into(),
            task: mk_affine_task("i1"),
            def_id: None,
        },
    ];
    let applied = crate::cluster_state::apply_locally_for_broadcast(&mut a, batch);
    let id_a0 = a.affine_id_for_hash("h0").unwrap();
    let id_a1 = a.affine_id_for_hash("h1").unwrap();
    assert_ne!(id_a0, id_a1, "distinct affine defs ⇒ distinct affine-ids");

    // Node B receives the stamped broadcast in REVERSE order and still agrees.
    let mut b = ClusterState::<RunnerIdentifier>::new();
    for m in applied.applied.into_iter().rev() {
        b.apply(m);
    }
    assert_eq!(b.affine_id_for_hash("h0"), Some(id_a0));
    assert_eq!(b.affine_id_for_hash("h1"), Some(id_a1));
}

/// A cell mutation applied through the broadcast choke point gets its
/// `generation` stamped (monotone) and lands as the named cell value; a stale
/// (lower-generation) re-apply NoOps (idempotent / LWW).
#[test]
fn cell_apply_is_lww_and_idempotent() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Seed an affine-id.
    let batch = vec![ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_affine_task("i"),
        def_id: None,
    }];
    crate::cluster_state::apply_locally_for_broadcast(&mut s, batch);
    let aid = s.affine_id_for_hash("h").unwrap();

    // Originate Queued then Finished through the choke point (monotone gens).
    let cells = vec![
        ClusterMutation::SecondaryCellQueued {
            secondary: "s1".into(),
            cell_id: aid.0,
            generation: 0, // stamped at the choke point
        },
        ClusterMutation::SecondaryCellFinished {
            secondary: "s1".into(),
            cell_id: aid.0,
            generation: 0,
        },
    ];
    crate::cluster_state::apply_locally_for_broadcast(&mut s, cells);
    assert_eq!(s.affine_state("s1", aid), SecondaryCell::Done);

    // A STALE Failed at generation 1 (below the stamped Finished) is a NoOp.
    assert_eq!(
        s.apply(ClusterMutation::SecondaryCellFailed {
            secondary: "s1".into(),
            cell_id: aid.0,
            generation: 1,
        }),
        ApplyOutcome::NoOp
    );
    assert_eq!(s.affine_state("s1", aid), SecondaryCell::Done);
}

/// The load-bearing convergence case: two replicas that diverge on a cell
/// (one Queued, the other the steal's Unqueued at a higher generation) MERGE
/// to the SAME value via snapshot restore, regardless of direction — the LWW
/// reset WINS (a value max-join would never converge here).
#[test]
fn steal_reset_converges_via_snapshot_restore() {
    // Replica A: the affine def is Queued on s1 at generation 5.
    let mut a = ClusterState::<RunnerIdentifier>::new();
    a.apply(ClusterMutation::SecondaryCellRegistered {
        hash: "h".into(),
        cell_id: 0,
    });
    a.apply(ClusterMutation::SecondaryCellQueued {
        secondary: "s1".into(),
        cell_id: 0,
        generation: 5,
    });
    // Replica B: the idle-steal moved the unit away, resetting s1 to NotDone
    // at generation 6 (strictly greater than the Queued it undoes).
    let mut b = ClusterState::<RunnerIdentifier>::new();
    b.apply(ClusterMutation::SecondaryCellRegistered {
        hash: "h".into(),
        cell_id: 0,
    });
    b.apply(ClusterMutation::SecondaryCellUnqueued {
        secondary: "s1".into(),
        cell_id: 0,
        generation: 6,
    });

    // A restores B's snapshot AND B restores A's: both converge to NotDone
    // (the higher-generation reset wins both ways).
    let mut a_into_b = a.clone();
    a_into_b.restore(b.snapshot());
    let mut b_into_a = b.clone();
    b_into_a.restore(a.snapshot());
    assert_eq!(a_into_b.affine_state("s1", SecondaryCellId(0)), SecondaryCell::NotDone);
    assert_eq!(b_into_a.affine_state("s1", SecondaryCellId(0)), SecondaryCell::NotDone);
}

/// The digest DETECTS an affine-cell divergence (count-OR-hash), and a
/// snapshot restore HEALS it — detect-WITH-heal. After restore the two
/// replicas' digests match.
#[test]
fn digest_detects_and_restore_heals_affine_divergence() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    a.apply(ClusterMutation::SecondaryCellFinished {
        secondary: "s1".into(),
        cell_id: 0,
        generation: 3,
    });
    let b = ClusterState::<RunnerIdentifier>::new();
    // B holds no affine cells → B is behind A (A holds a cell B lacks).
    assert!(b.digest().is_behind(&a.digest()));
    assert!(!a.digest().is_behind(&b.digest()));

    // Restore heals: B pulls A's snapshot and converges.
    let mut b = b;
    b.restore(a.snapshot());
    assert_eq!(b.affine_state("s1", SecondaryCellId(0)), SecondaryCell::Done);
    assert!(!b.digest().is_behind(&a.digest()));
    assert!(!a.digest().is_behind(&b.digest()));
}

/// The affine-id↔hash BINDING survives snapshot → restore into a FRESH replica
/// (the relocation / failover handoff). The snapshot does NOT carry the def
/// store's affine registry; it is rebuilt per-task from each restored def's
/// INLINE `affine_id`. So the originating side must STAMP the id onto the def
/// (`intern_affine_at`) and the restoring side must RE-ANCHOR it
/// (`register_restored_def`). Without both, a snapshot-promoted primary
/// resolves `affine_id_for_hash == None`, the dependent's affine deps read
/// empty, the per-secondary import is never placed, and the run stalls with the
/// import phase held open (the secondary-affine --mode local stall).
#[test]
fn affine_id_binding_survives_snapshot_restore_into_fresh_replica() {
    // Originate the affine def through the broadcast choke (reserves the id,
    // injects + applies `SecondaryCellRegistered`, and stamps the def).
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let batch = vec![ClusterMutation::TaskAdded {
        hash: "h-affine".into(),
        task: mk_affine_task("imp"),
        def_id: None,
    }];
    crate::cluster_state::apply_locally_for_broadcast(&mut a, batch);
    let bound = a
        .affine_id_for_hash("h-affine")
        .expect("origin binds the affine-id");

    // A FRESH replica (no def store, no affine registry) restores A's snapshot
    // — the relocation/failover handoff shape (the registry is NOT in the
    // snapshot; it must rebuild from the restored def's inline `affine_id`).
    let mut fresh = ClusterState::<RunnerIdentifier>::new();
    assert!(
        fresh.affine_id_for_hash("h-affine").is_none(),
        "precondition: the fresh replica knows nothing yet"
    );
    fresh.restore(a.snapshot());

    assert_eq!(
        fresh.affine_id_for_hash("h-affine"),
        Some(bound),
        "the restored replica must re-anchor the affine-id from the def's \
         inline field — else affine placement finds no affine deps and the \
         import never dispatches"
    );
}

// ── phase_rollups SecondaryAffine exclusion (rollup-side twin of #642) ──
//
// `phase_rollups().has_live` is the phase-end blocker `phase_can_proceed`
// vetoes on. A `SecondaryAffine` task's global `TaskState` can stay
// non-terminal indefinitely (its real "completion" is the per-secondary
// bitvector, not a global terminal), so counting it as live would pin
// `has_live = true` forever and the phase would never end after its WORK
// terminalized — the matrix_eval→dependency_graph stall. The fix excludes
// `SecondaryAffine` from `has_live` (NOT from `has_any`), matching the pool's
// `queued_count` via the SAME `counts_for_phase_drain` predicate.

/// matrix_eval-SHAPE: a phase with one NON-terminal `SecondaryAffine` BARRIER
/// import + two TERMINAL `Work` tasks reads `has_live == false` (the affine is
/// excluded), so `phase_can_proceed`'s `has_any && !has_live` arm fires.
///
/// REVERT-CONFIRM (pre-fix): the affine's `Pending` `TaskState` set
/// `has_live = true` and `phase_can_proceed` vetoed `PhaseEnded` forever — the
/// observed live stall.
#[test]
fn phase_rollup_excludes_nonterminal_affine_barrier_from_has_live() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // 1 affine BARRIER (stays Pending — never gets a worker TaskComplete) +
    // 2 work tasks, all in phase `me`.
    s.apply(ClusterMutation::TaskAdded {
        hash: "barrier".into(),
        task: mk_affine_task_in("barrier", "me"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "w0".into(),
        task: mk_work_task_in("w0", "me"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "w1".into(),
        task: mk_work_task_in("w1", "me"),
        def_id: None,
    });
    // Both WORK tasks terminalize; the affine barrier stays Pending.
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "w0".into(),
        result_data: None,
    });
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "w1".into(),
        result_data: None,
    });

    let rollups = s.phase_rollups();
    let me = PhaseId::from("me");
    let r = rollups.get(&me).expect("phase present");
    assert!(r.has_any, "the phase owns tasks (affine kept in has_any)");
    assert!(
        !r.has_live,
        "a non-terminal SecondaryAffine barrier must NOT count as live work \
         (rollup-side twin of the pool's queued_count exclusion); pre-fix it \
         pinned has_live=true and stalled the phase boundary"
    );
}

/// BUILD-SHAPE (proves GLOBAL scope): a phase with MANY (5) NON-terminal
/// `SecondaryAffine` import gates + a TERMINAL `Work` task reads
/// `has_live == false`. The rollup loop builds every phase's entry, so the
/// exclusion is global by construction — not a one-phase special case.
///
/// REVERT-CONFIRM (pre-fix): the 5 `Pending` import gates kept
/// `has_live = true` after the build work terminalized — the re-stall at
/// end-of-BUILD the consumer's ~323 import gates would hit.
#[test]
fn phase_rollup_excludes_many_affine_gates_global_scope() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for i in 0..5 {
        let id = format!("gate{i}");
        s.apply(ClusterMutation::TaskAdded {
            hash: id.clone(),
            task: mk_affine_task_in(&id, "build"),
            def_id: None,
        });
    }
    s.apply(ClusterMutation::TaskAdded {
        hash: "build_variant".into(),
        task: mk_work_task_in("build_variant", "build"),
        def_id: None,
    });
    // The build work terminalizes; all 5 import gates stay Pending (NotDone).
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "build_variant".into(),
        result_data: None,
    });

    let rollups = s.phase_rollups();
    let build = PhaseId::from("build");
    let r = rollups.get(&build).expect("phase present");
    assert!(r.has_any, "the phase owns tasks");
    assert!(
        !r.has_live,
        "MANY non-terminal SecondaryAffine gates must ALL be excluded from \
         has_live — the exclusion is global (the rollup loop builds every \
         phase's entry), not a single-phase special case"
    );
}

/// DEPENDENCY-GATING PRESERVED: excluding the affine from the phase COUNT does
/// NOT touch the dependency edge. A `Work` task `Blocked` on the affine import
/// stays `Blocked` (its `TaskDep` is intact) — the count change is orthogonal
/// to the dependency mechanism that gates the work's dispatch. Because that
/// `Blocked` work task is a NON-affine LIVE task, the phase correctly still
/// reads `has_live == true` (it is NOT prematurely proceeded).
#[test]
fn affine_exclusion_preserves_dependent_blocked_edge() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "imp".into(),
        task: mk_affine_task_in("imp", "p"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "dep".into(),
        task: mk_work_task_in("dep", "p"),
        def_id: None,
    });
    // The dependent work is Blocked on the affine import — the dependency edge.
    s.apply(ClusterMutation::TaskBlocked {
        hash: "dep".into(),
        on: "imp".into(),
    });

    // The dependency edge is intact: the work is still Blocked (not dispatched).
    assert!(
        matches!(s.task_state("dep"), Some(TaskState::Blocked { .. })),
        "the affine count change must NOT touch the TaskDep edge — the work \
         still waits on the import"
    );
    // And the phase reads has_live=true because of the BLOCKED WORK (a live
    // non-affine task), NOT because of the affine — proving the exclusion is
    // surgical to the affine, not a blanket suppression.
    let rollups = s.phase_rollups();
    let p = PhaseId::from("p");
    let r = rollups.get(&p).expect("phase present");
    assert!(
        r.has_any && r.has_live,
        "a live Blocked WORK dependent keeps the phase live (the affine \
         exclusion does not prematurely proceed it)"
    );
}

/// REGRESSION: a phase with a genuine NON-affine non-terminal `Work` task still
/// reads `has_live == true` — the exclusion is scoped to `SecondaryAffine` and
/// does NOT let an ordinary live phase proceed prematurely.
#[test]
fn phase_rollup_genuine_nonaffine_live_still_holds_phase() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "affine".into(),
        task: mk_affine_task_in("affine", "p"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "live_work".into(),
        task: mk_work_task_in("live_work", "p"),
        def_id: None,
    });
    // The work stays Pending (live); the affine stays Pending too.
    let rollups = s.phase_rollups();
    let p = PhaseId::from("p");
    let r = rollups.get(&p).expect("phase present");
    assert!(
        r.has_any && r.has_live,
        "a genuine non-terminal WORK task must keep has_live=true — the \
         exclusion must not over-fire and proceed a still-live phase"
    );
}

/// #617 AFFINE-ONLY PHASE: a phase whose ONLY content is `SecondaryAffine`
/// tasks keeps `has_any == true` (the affine is a task the phase genuinely
/// owns). Once the affine's first per-secondary run records its global terminal
/// (the `TaskCompleted` the manager originates on first run, complete.rs), the
/// phase reads `has_any && !has_live` and `phase_can_proceed`'s primary arm
/// fires — the SAME path a work phase takes. Keeping the affine in `has_any`
/// (rather than excluding it from both) is what preserves this: excluding it
/// from `has_any` would make an affine-only phase read `has_any == false`,
/// dropping it out of the proceed arm AND the narrator's start/complete edges.
///
/// Pre-terminal (the affine's global state is still Pending) the phase reads
/// `has_any && !has_live`? NO — `has_live` is false (affine excluded), so the
/// rollup gate would PASS early. That is SAFE here precisely because this gate
/// is consulted ONLY after the POOL surfaces the phase as drained, and the
/// pool's separate `phase_has_live_affine_prereq` guard holds the affine-only
/// phase open until that first-run terminal (#617's premature-drain guard) —
/// the pool, not the rollup, owns the affine HOLD.
#[test]
fn phase_rollup_affine_only_phase_keeps_has_any() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "only".into(),
        task: mk_affine_task_in("only", "affine_only"),
        def_id: None,
    });

    // Before the affine's global terminal: has_any TRUE (the phase owns the
    // affine), has_live FALSE (the affine is excluded from has_live).
    {
        let rollups = s.phase_rollups();
        let ph = PhaseId::from("affine_only");
        let r = rollups.get(&ph).expect("affine-only phase present");
        assert!(
            r.has_any,
            "an affine-only phase must keep has_any=true so phase_can_proceed's \
             primary arm and the narrator's start/complete edges fire for it"
        );
        assert!(!r.has_live, "the affine is excluded from has_live");
    }

    // After the affine's first-run global terminal (the manager's first-run
    // TaskCompleted): the phase reads has_any && !has_live — the proceed arm.
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "only".into(),
        result_data: None,
    });
    let rollups = s.phase_rollups();
    let ph = PhaseId::from("affine_only");
    let r = rollups.get(&ph).expect("affine-only phase present");
    assert!(
        r.has_any && !r.has_live,
        "post-terminal affine-only phase takes the has_any && !has_live arm"
    );
}

/// A promoted primary RESUMES the cell-generation stamp counter PAST every
/// inherited cell generation at the `PrimaryChanged` epoch advance, so a
/// later originated cell write out-stamps the inherited cells (the LWW total
/// order survives failover).
#[test]
fn failover_resumes_cell_generation_past_inherited() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Inherit a cell at a high generation (e.g. restored from a prior epoch).
    s.apply(ClusterMutation::SecondaryCellQueued {
        secondary: "s1".into(),
        cell_id: 0,
        generation: 100,
    });
    // Promotion: a new primary epoch advances → the gen-floor resume fires.
    s.apply(ClusterMutation::PrimaryChanged {
        new: "node-self".into(),
        epoch: 1,
        reason: Default::default(),
    });
    // A newly originated cell write through the choke point must out-stamp the
    // inherited generation-100 cell, so it WINS the LWW (lands as Done).
    let cells = vec![ClusterMutation::SecondaryCellFinished {
        secondary: "s1".into(),
        cell_id: 0,
        generation: 0, // stamped at the choke point, resumed past 100
    }];
    crate::cluster_state::apply_locally_for_broadcast(&mut s, cells);
    assert_eq!(s.affine_state("s1", SecondaryCellId(0)), SecondaryCell::Done);
}
