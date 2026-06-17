//! Tests for the AF-id affine state layer: affine-id allocation/agreement,
//! the per-secondary bitvector cell apply + LWW merge, the
//! snapshot/digest/restore round-trip, and the failover gen-floor resume.

use super::*;
use crate::cluster_state::AffineId;
use dynrunner_core::TaskKind;
use dynrunner_protocol_primary_secondary::AffineCell;

/// A `TaskKind::SecondaryAffine` task fixture (twin of `mk_task`).
fn mk_affine_task(name: &str) -> TaskInfo<RunnerIdentifier> {
    let mut t = mk_task(name);
    t.kind = TaskKind::SecondaryAffine;
    t
}

/// Originating a `SecondaryAffine` `TaskAdded` through the broadcast choke
/// point reserves an affine-id, INJECTS a paired `SecondaryAffineRegistered`,
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
        .filter(|m| matches!(m, ClusterMutation::SecondaryAffineRegistered { .. }))
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
        ClusterMutation::SecondaryAffineQueued {
            secondary: "s1".into(),
            affine_id: aid.0,
            generation: 0, // stamped at the choke point
        },
        ClusterMutation::SecondaryAffineFinished {
            secondary: "s1".into(),
            affine_id: aid.0,
            generation: 0,
        },
    ];
    crate::cluster_state::apply_locally_for_broadcast(&mut s, cells);
    assert_eq!(s.affine_state("s1", aid), AffineCell::Done);

    // A STALE Failed at generation 1 (below the stamped Finished) is a NoOp.
    assert_eq!(
        s.apply(ClusterMutation::SecondaryAffineFailed {
            secondary: "s1".into(),
            affine_id: aid.0,
            generation: 1,
        }),
        ApplyOutcome::NoOp
    );
    assert_eq!(s.affine_state("s1", aid), AffineCell::Done);
}

/// The load-bearing convergence case: two replicas that diverge on a cell
/// (one Queued, the other the steal's Unqueued at a higher generation) MERGE
/// to the SAME value via snapshot restore, regardless of direction — the LWW
/// reset WINS (a value max-join would never converge here).
#[test]
fn steal_reset_converges_via_snapshot_restore() {
    // Replica A: the affine def is Queued on s1 at generation 5.
    let mut a = ClusterState::<RunnerIdentifier>::new();
    a.apply(ClusterMutation::SecondaryAffineRegistered {
        hash: "h".into(),
        affine_id: 0,
    });
    a.apply(ClusterMutation::SecondaryAffineQueued {
        secondary: "s1".into(),
        affine_id: 0,
        generation: 5,
    });
    // Replica B: the idle-steal moved the unit away, resetting s1 to NotDone
    // at generation 6 (strictly greater than the Queued it undoes).
    let mut b = ClusterState::<RunnerIdentifier>::new();
    b.apply(ClusterMutation::SecondaryAffineRegistered {
        hash: "h".into(),
        affine_id: 0,
    });
    b.apply(ClusterMutation::SecondaryAffineUnqueued {
        secondary: "s1".into(),
        affine_id: 0,
        generation: 6,
    });

    // A restores B's snapshot AND B restores A's: both converge to NotDone
    // (the higher-generation reset wins both ways).
    let mut a_into_b = a.clone();
    a_into_b.restore(b.snapshot());
    let mut b_into_a = b.clone();
    b_into_a.restore(a.snapshot());
    assert_eq!(a_into_b.affine_state("s1", AffineId(0)), AffineCell::NotDone);
    assert_eq!(b_into_a.affine_state("s1", AffineId(0)), AffineCell::NotDone);
}

/// The digest DETECTS an affine-cell divergence (count-OR-hash), and a
/// snapshot restore HEALS it — detect-WITH-heal. After restore the two
/// replicas' digests match.
#[test]
fn digest_detects_and_restore_heals_affine_divergence() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    a.apply(ClusterMutation::SecondaryAffineFinished {
        secondary: "s1".into(),
        affine_id: 0,
        generation: 3,
    });
    let b = ClusterState::<RunnerIdentifier>::new();
    // B holds no affine cells → B is behind A (A holds a cell B lacks).
    assert!(b.digest().is_behind(&a.digest()));
    assert!(!a.digest().is_behind(&b.digest()));

    // Restore heals: B pulls A's snapshot and converges.
    let mut b = b;
    b.restore(a.snapshot());
    assert_eq!(b.affine_state("s1", AffineId(0)), AffineCell::Done);
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
    // injects + applies `SecondaryAffineRegistered`, and stamps the def).
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

/// A promoted primary RESUMES the cell-generation stamp counter PAST every
/// inherited cell generation at the `PrimaryChanged` epoch advance, so a
/// later originated cell write out-stamps the inherited cells (the LWW total
/// order survives failover).
#[test]
fn failover_resumes_cell_generation_past_inherited() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Inherit a cell at a high generation (e.g. restored from a prior epoch).
    s.apply(ClusterMutation::SecondaryAffineQueued {
        secondary: "s1".into(),
        affine_id: 0,
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
    let cells = vec![ClusterMutation::SecondaryAffineFinished {
        secondary: "s1".into(),
        affine_id: 0,
        generation: 0, // stamped at the choke point, resumed past 100
    }];
    crate::cluster_state::apply_locally_for_broadcast(&mut s, cells);
    assert_eq!(s.affine_state("s1", AffineId(0)), AffineCell::Done);
}
