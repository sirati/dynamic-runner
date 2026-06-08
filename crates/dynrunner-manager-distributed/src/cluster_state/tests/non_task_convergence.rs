//! Property / order-independence tests for the NON-task convergence
//! comparators: the `current_primary` register (CRD-2/D-P), the
//! `phase_deps` deterministic content-hash merge (CRD-3/D-G), the
//! `task_outputs` value-divergence detection (AE-5/C7), and the
//! role-capability 2P-set failover heal (C6).
//!
//! Single concern: pin that apply == restore == digest for the non-task
//! ledger fields, including the equal-epoch primary tie-break, the
//! partition-reconcile phase-deps merge, and the capability lattice's
//! snapshot-healable convergence across a failover.

use super::super::CapabilityEntry;
use super::*;

// ── CRD-2 / D-P: current_primary equal-epoch register ──

/// At EQUAL epoch the lexicographically-LOWER `current_primary` id wins —
/// applied identically in apply and restore (`primary_register_adopt`), so
/// two replicas that each minted `PrimaryChanged{self, N}` concurrently
/// converge to the SAME id in one round (the prior arrival-LWW left a
/// permanent equal-epoch identity split).
#[test]
fn primary_equal_epoch_lower_id_wins() {
    // Apply path: a local higher-id primary at epoch 5, then an incoming
    // LOWER-id primary at the SAME epoch wins.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::PrimaryChanged {
        new: "sec-z".into(),
        epoch: 5,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    assert_eq!(s.current_primary(), Some("sec-z"));
    let won = s.apply(ClusterMutation::PrimaryChanged {
        new: "sec-a".into(),
        epoch: 5,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    assert_eq!(won, ApplyOutcome::Applied);
    assert_eq!(
        s.current_primary(),
        Some("sec-a"),
        "equal-epoch lower id (sec-a) must win over higher id (sec-z)"
    );
    // A higher-id at equal epoch does NOT regress the register.
    let lost = s.apply(ClusterMutation::PrimaryChanged {
        new: "sec-z".into(),
        epoch: 5,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    assert_eq!(lost, ApplyOutcome::NoOp);
    assert_eq!(s.current_primary(), Some("sec-a"));
}

/// Equal-epoch apply is COMMUTATIVE: applying `{sec-a, sec-z}` in either
/// order converges to the lex-lower `sec-a`. The order-independence the
/// register must satisfy for two concurrent originations to heal.
#[test]
fn apply_equal_epoch_commutes() {
    let mk = || {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::PrimaryChanged {
            new: "seed".into(),
            epoch: 5,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        });
        s
    };
    let mut ab = mk();
    ab.apply(ClusterMutation::PrimaryChanged {
        new: "sec-a".into(),
        epoch: 5,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    ab.apply(ClusterMutation::PrimaryChanged {
        new: "sec-z".into(),
        epoch: 5,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    let mut ba = mk();
    ba.apply(ClusterMutation::PrimaryChanged {
        new: "sec-z".into(),
        epoch: 5,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    ba.apply(ClusterMutation::PrimaryChanged {
        new: "sec-a".into(),
        epoch: 5,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    assert_eq!(ab.current_primary(), Some("sec-a"));
    assert_eq!(ba.current_primary(), Some("sec-a"));
    assert_eq!(ab.current_primary(), ba.current_primary());
}

/// CRD-2/D-P bilateral one-round convergence (C5): two replicas at the
/// SAME epoch with DIVERGENT `current_primary` each detect the split
/// (`current_primary_hash` differs) and BOTH pull; restore's deterministic
/// lower-id-wins converges both to `sec-a` in one round, and a second
/// round pulls nothing (quiesce).
#[test]
fn bilateral_equal_epoch_convergence_one_round() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let mut b = ClusterState::<RunnerIdentifier>::new();
    a.apply(ClusterMutation::PrimaryChanged {
        new: "sec-a".into(),
        epoch: 9,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    b.apply(ClusterMutation::PrimaryChanged {
        new: "sec-z".into(),
        epoch: 9,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    // Equal epoch, different identity → BOTH sides detect (bilateral).
    assert_eq!(a.digest().primary_epoch, b.digest().primary_epoch);
    assert!(a.digest().is_behind(&b.digest()));
    assert!(b.digest().is_behind(&a.digest()));

    // One round: each pulls the other's snapshot. Deterministic lower-id-
    // wins → both converge to sec-a regardless of pull order.
    let snap_a = a.snapshot();
    let snap_b = b.snapshot();
    a.restore(snap_b);
    b.restore(snap_a);
    assert_eq!(a.current_primary(), Some("sec-a"));
    assert_eq!(b.current_primary(), Some("sec-a"));

    // Second round pulls nothing (quiesce).
    assert_eq!(a.digest(), b.digest());
    assert!(!a.digest().is_behind(&b.digest()));
    assert!(!b.digest().is_behind(&a.digest()));
}

// ── CRD-2 / D-P: relocate-vs-failover compose convergence (Phase 6b) ──

/// Phase 6b — same apply path, reason-BLIND. Relocation
/// (`PrimaryChanged{Transferred}`) and failover (`PrimaryChanged{Election}`)
/// move `current_primary` through the IDENTICAL epoch-LWW
/// `primary_register_adopt` rule: the `reason` is advisory routing metadata
/// only and is never read by the adopt rule. So at any fixed `(epoch, id)`,
/// SWAPPING the reason cannot change which id the register holds — the
/// epoch-LWW + equal-epoch lex tiebreak alone decide. Drive a representative
/// matrix (lower-then-higher, higher-then-lower, higher-epoch override) on
/// two replicas that differ ONLY in the reason stamped on each mutation; the
/// two replicas must end on the byte-identical primary register every time.
#[test]
fn relocate_and_failover_share_reason_blind_apply_path() {
    use dynrunner_protocol_primary_secondary::PrimaryChangeReason::{Election, Transferred};

    // One mutation as (id, epoch); the reason is supplied per-replica so the
    // ONLY difference between the two replicas is the advisory reason field.
    let script: &[(&str, u64)] = &[
        ("sec-z", 5), // initial primary at epoch 5
        ("sec-a", 5), // equal-epoch lex-lower → adopts
        ("sec-z", 5), // equal-epoch lex-higher → NoOp (no regress)
        ("sec-m", 6), // higher epoch → overrides regardless of id ordering
        ("sec-a", 6), // equal-epoch lex-lower than sec-m → adopts
    ];

    // Replica T stamps EVERY mutation `Transferred` (the relocate reason);
    // replica E stamps EVERY mutation `Election` (the failover reason).
    let run = |reason| {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        let mut outcomes = Vec::new();
        for (id, epoch) in script {
            outcomes.push(s.apply(ClusterMutation::PrimaryChanged {
                new: (*id).into(),
                epoch: *epoch,
                reason,
            }));
        }
        (s.current_primary().map(str::to_owned), s.primary_epoch(), outcomes)
    };
    let (prim_t, epoch_t, outcomes_t) = run(Transferred);
    let (prim_e, epoch_e, outcomes_e) = run(Election);

    // The per-step Applied/NoOp decisions are identical: the adopt rule never
    // consulted the reason, so the relocate-reason and failover-reason
    // replicas took the SAME branch at every step.
    assert_eq!(
        outcomes_t, outcomes_e,
        "swapping the PrimaryChanged reason must not change any apply outcome \
         (the epoch-LWW adopt rule is reason-blind)"
    );
    // And they converge on the byte-identical register: lex-lower sec-a at the
    // highest epoch 6.
    assert_eq!(prim_t.as_deref(), Some("sec-a"));
    assert_eq!(prim_t, prim_e, "relocate and failover reasons name the same primary");
    assert_eq!(epoch_t, 6);
    assert_eq!(epoch_t, epoch_e, "relocate and failover reasons settle the same epoch");
}

/// Phase 6b — heal after a mid-run PARTITION. Two partitions each elect at the
/// SAME new epoch E+1 with DIFFERENT ids, via DIFFERENT reasons (one relocate
/// `Transferred`, one failover `Election`). While partitioned, each replica's
/// register holds its own pick — a transient equal-epoch split. On heal,
/// anti-entropy detects the divergence BILATERALLY (`is_behind` both ways at
/// equal epoch with divergent identity), each side pulls the other's snapshot,
/// and `restore`'s reason-blind `primary_register_adopt` converges BOTH onto
/// the lex-lower id — the same id the election's `lowest_alive` `.min()` leader
/// would name — in a single round, with no permanent split-brain. Exercises
/// the real merge path (`digest`/`is_behind`/`snapshot`/`restore`), not a
/// hand-asserted tiebreak.
#[test]
fn partition_relocate_vs_failover_heals_to_one_primary() {
    use dynrunner_protocol_primary_secondary::PrimaryChangeReason::{Election, Transferred};

    // Both partitions start from a shared pre-partition primary at epoch 7,
    // so they each independently mint epoch 7+1 = 8 (the concurrency that
    // collides at one epoch).
    let mut p_reloc = ClusterState::<RunnerIdentifier>::new();
    let mut p_fail = ClusterState::<RunnerIdentifier>::new();
    for s in [&mut p_reloc, &mut p_fail] {
        s.apply(ClusterMutation::PrimaryChanged {
            new: "sec-old".into(),
            epoch: 7,
            reason: Election,
        });
    }

    // Partition A: a relocate hands authority to the lex-HIGHER `sec-9` at
    // epoch 8 (Transferred — names a peer that is not the originator).
    p_reloc.apply(ClusterMutation::PrimaryChanged {
        new: "sec-9".into(),
        epoch: 8,
        reason: Transferred,
    });
    // Partition B: a failover election self-names the lex-LOWER `sec-0` at the
    // SAME epoch 8 (Election — the self-announce shape).
    p_fail.apply(ClusterMutation::PrimaryChanged {
        new: "sec-0".into(),
        epoch: 8,
        reason: Election,
    });
    assert_eq!(p_reloc.current_primary(), Some("sec-9"));
    assert_eq!(p_fail.current_primary(), Some("sec-0"));

    // Heal: equal epoch (8 == 8), divergent identity → BOTH sides detect the
    // split, so anti-entropy is bilateral (each pulls the other).
    assert_eq!(p_reloc.digest().primary_epoch, p_fail.digest().primary_epoch);
    assert!(p_reloc.digest().is_behind(&p_fail.digest()));
    assert!(p_fail.digest().is_behind(&p_reloc.digest()));

    // One anti-entropy round: each restores the other's snapshot. The
    // reason-blind adopt rule converges BOTH on the lex-lower `sec-0`
    // regardless of pull order (the relocate's Transferred does NOT pin sec-9).
    let snap_reloc = p_reloc.snapshot();
    let snap_fail = p_fail.snapshot();
    p_reloc.restore(snap_fail);
    p_fail.restore(snap_reloc);
    assert_eq!(
        p_reloc.current_primary(),
        Some("sec-0"),
        "the relocate partition heals onto the lex-lower failover winner \
         (Transferred is advisory; the equal-epoch lex tiebreak decides)"
    );
    assert_eq!(p_fail.current_primary(), Some("sec-0"));
    assert_eq!(
        p_reloc.current_primary(),
        p_fail.current_primary(),
        "both partitions converge on ONE primary — no permanent split-brain"
    );

    // Quiesce: a second round pulls nothing (the lattice is at a fixpoint).
    assert_eq!(p_reloc.digest(), p_fail.digest());
    assert!(!p_reloc.digest().is_behind(&p_fail.digest()));
    assert!(!p_fail.digest().is_behind(&p_reloc.digest()));
}

// ── CRD-3 / D-G: phase_deps deterministic merge ──

fn deps(pairs: &[(&str, &[&str])]) -> HashMap<PhaseId, Vec<PhaseId>> {
    pairs
        .iter()
        .map(|(p, ds)| {
            (
                PhaseId::from(*p),
                ds.iter().map(|d| PhaseId::from(*d)).collect(),
            )
        })
        .collect()
}

/// Two replicas that diverged on the (set-once-but-genuinely-diverged)
/// phase graph reconcile DETERMINISTICALLY on `restore`: the LOWER
/// content-hash graph wins, applied the same way regardless of pull
/// direction, so both converge to the SAME graph in one round.
#[test]
fn phase_deps_divergent_merge_deterministic() {
    let g1 = deps(&[("p1", &["p0"])]);
    let g2 = deps(&[("p1", &["p0", "px"])]);

    let mut a = ClusterState::<RunnerIdentifier>::new();
    let mut b = ClusterState::<RunnerIdentifier>::new();
    a.apply(ClusterMutation::PhaseDepsSet { deps: g1.clone() });
    b.apply(ClusterMutation::PhaseDepsSet { deps: g2.clone() });

    // Compute the deterministic winner (lower canonical content hash).
    let h1 = super::super::merge::canonical_phase_deps_hash(&g1);
    let h2 = super::super::merge::canonical_phase_deps_hash(&g2);
    let winner = if h1 < h2 { &g1 } else { &g2 };

    // Each pulls the other. Both must end on `winner`, regardless of order.
    let snap_a = a.snapshot();
    let snap_b = b.snapshot();
    a.restore(snap_b);
    b.restore(snap_a);
    assert_eq!(a.phase_deps(), winner);
    assert_eq!(b.phase_deps(), winner);
    assert_eq!(a.phase_deps(), b.phase_deps());
}

/// The digest DETECTS a phase-deps divergence even at EQUAL count (R5 —
/// the count-only line could not see a divergent-but-equal-count graph).
/// Both graphs have one phase entry (count == 1) but different dep lists,
/// so the count is equal and only the `phase_deps_hash` separates them.
#[test]
fn digest_detects_phase_deps_divergence() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let mut b = ClusterState::<RunnerIdentifier>::new();
    a.apply(ClusterMutation::PhaseDepsSet {
        deps: deps(&[("p1", &["p0"])]),
    });
    b.apply(ClusterMutation::PhaseDepsSet {
        deps: deps(&[("p1", &["p0", "px"])]),
    });
    // Equal count, divergent hash → both detect (count-OR-hash, R5).
    assert_eq!(a.digest().phase_deps_count, b.digest().phase_deps_count);
    assert_ne!(a.digest().phase_deps_hash, b.digest().phase_deps_hash);
    assert!(a.digest().is_behind(&b.digest()));
    assert!(b.digest().is_behind(&a.digest()));
}

// ── AE-5 / C7: task_outputs value divergence ──

/// A `task_outputs` VALUE divergence at an equal KEY is DETECTED (AE-5 —
/// the fold is KEY+VALUE, not key-only), and first-write-wins holds on
/// both apply and restore (C7) so a snapshot output never clobbers a
/// locally-populated entry.
#[test]
fn task_outputs_value_divergence_detected() {
    use dynrunner_core::{ResultValue, TaskOutputs};
    use std::collections::BTreeMap;

    let mk_outputs = |v: &str| {
        let mut m: BTreeMap<String, ResultValue> = BTreeMap::new();
        m.insert("k".into(), ResultValue::Inline(v.into()));
        TaskOutputs(m)
    };
    let done_payload = |outputs: &TaskOutputs| -> Vec<u8> {
        // Wire-shape DonePayload { outputs, .. }; the cache populate helper
        // decodes the inner `outputs`. Use the value-only JSON the decoder
        // accepts (unknown fields are dropped — no deny_unknown_fields).
        serde_json::to_vec(&serde_json::json!({ "outputs": outputs })).unwrap()
    };

    let mut a = ClusterState::<RunnerIdentifier>::new();
    let mut b = ClusterState::<RunnerIdentifier>::new();
    for s in [&mut a, &mut b] {
        s.apply(ClusterMutation::TaskAdded {
            hash: "t".into(),
            task: mk_task("t"),
        });
    }
    // Same key "t", DIFFERENT output values on the two replicas.
    a.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "t".into(),
        result_data: Some(done_payload(&mk_outputs("alpha"))),
    });
    b.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "t".into(),
        result_data: Some(done_payload(&mk_outputs("beta"))),
    });

    // Equal output-cache count, divergent VALUE → divergent fold → detected.
    assert_eq!(a.digest().task_outputs_count, b.digest().task_outputs_count);
    assert_ne!(a.digest().task_outputs_hash, b.digest().task_outputs_hash);
    assert!(a.digest().is_behind(&b.digest()));

    // First-write-wins on restore: `a` already holds "alpha"; restoring
    // `b`'s snapshot (carrying "beta") must NOT clobber the local entry.
    let before = a.outputs_for(&PhaseId::from("p0"), "t").cloned();
    a.restore(b.snapshot());
    let after = a.outputs_for(&PhaseId::from("p0"), "t").cloned();
    assert_eq!(
        before, after,
        "restore must NOT clobber a locally-populated output entry (first-write-wins, C7)"
    );
}

// ── C6: role-capability 2P-set failover heal ──

/// A node that MISSED a `PeerRemoved` + `SetCanBePrimary(false)` during a
/// failover converges the capability across the failover via the
/// snapshot-healable 2P-set — independent of its OWN liveness view of the
/// departed peer, and even though the snapshot source (a "promoted
/// primary") holds NO node-local Dead knowledge of the peer.
#[test]
fn peer_removed_role_heals_across_failover() {
    // `n` is the node that missed the events: it saw obs-1 join as a
    // primary-capable peer and never learned it departed / was revoked.
    let mut n = ClusterState::<RunnerIdentifier>::new();
    n.apply(ClusterMutation::PeerJoined {
        peer_id: "obs-1".into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: TaskVersion {
            primary_epoch: 1,
            seq: 1,
        },
    });
    assert!(n.can_be_primary("obs-1"));

    // `promoted` is a freshly-promoted primary that holds the CONVERGED
    // capability 2P-set: it saw obs-1's `can_be_primary` REVOKED (a
    // higher-cap_version `false`) and then obs-1 DEPARTED (a `Departed`
    // tombstone). Critically it has NO node-local Dead knowledge in
    // `peer_state` beyond what the tombstone implies — it was built purely
    // from the capability mutations (a promoted node inheriting the
    // capability lattice without the prior primary's liveness view).
    let mut promoted = ClusterState::<RunnerIdentifier>::new();
    promoted.apply(ClusterMutation::PeerJoined {
        peer_id: "obs-1".into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: TaskVersion {
            primary_epoch: 1,
            seq: 1,
        },
    });
    promoted.apply(ClusterMutation::SetCanBePrimary {
        peer_id: "obs-1".into(),
        can_be_primary: false,
        cap_version: TaskVersion {
            primary_epoch: 2,
            seq: 1,
        },
    });
    promoted.apply(ClusterMutation::PeerRemoved {
        id: "obs-1".into(),
        cause: RemovalCause::KeepaliveMiss,
    });
    assert!(!promoted.can_be_primary("obs-1"));

    // One anti-entropy round: `n`'s digest flags the `capabilities_hash`
    // divergence (the Departed tombstone + revoked cbp) → pull → restore
    // merges the 2P-set → reproject drops obs-1 from can_be_primary.
    assert!(n.digest().is_behind(&promoted.digest()));
    n.restore(promoted.snapshot());

    // `n` converged the capability INDEPENDENT of its own liveness view:
    // it still holds obs-1 Alive locally (restore never buries), but the
    // capability tombstone dominates so obs-1 projects OUT of both role
    // sets — matching the promoted primary's `can_be_primary` after
    // convergence, even though the promoted primary had no live Dead bit.
    assert!(
        n.is_peer_alive("obs-1"),
        "restore must not bury n's locally-Alive peer (honest-liveness)"
    );
    assert!(
        !n.can_be_primary("obs-1"),
        "the Departed capability tombstone heals can_be_primary across the failover"
    );
    assert_eq!(
        n.role_table().can_be_primary,
        promoted.role_table().can_be_primary,
        "n's can_be_primary projection matches the promoted primary's after convergence"
    );
    assert!(!n.role_table().observers.contains("obs-1"));
}

// ── C6: merge_capability lattice order-independence (DIRECT) ──

/// Build an `Advertised` capability entry tersely.
fn adv(is_observer: bool, can_be_primary: bool, epoch: u64, seq: u32) -> CapabilityEntry {
    CapabilityEntry::Advertised {
        is_observer,
        can_be_primary,
        cap_version: TaskVersion {
            primary_epoch: epoch,
            seq,
        },
    }
}

/// `merge_capability` is a CRDT join: COMMUTATIVE, ASSOCIATIVE, and
/// IDEMPOTENT. The existing `merge_is_total_and_commutative` pins only the
/// per-TASK join; this pins the role-capability lattice's order-
/// independence DIRECTLY (it was previously covered only indirectly via
/// the failover-heal test). A representative cross-product over `{Departed,
/// Advertised(is_observer, can_be_primary, cap_version)}` at several
/// versions exercises every arm: the `Departed` absorbing element, the
/// `is_observer` OR-ratchet, the `can_be_primary`-follows-higher-version
/// rule, and `max(cap_version)`.
///
/// CRITICAL invariant the variant set honors (so the order-independence
/// claim is over the lattice as PRODUCTION produces it, not a fictional
/// one): `can_be_primary` is a function of `cap_version`. The primary
/// stamps a STRICTLY HIGHER `cap_version` on every `SetCanBePrimary`
/// (`broadcast.rs`), so two `Advertised` entries at the SAME `cap_version`
/// ALWAYS carry the SAME `can_be_primary` bit. `merge_capability`'s tie
/// rule (equal version → keep local's `can_be_primary`, the strict
/// `iv > lv` gate) is commutative ONLY under that invariant — a
/// hand-built equal-version pair with DIVERGENT `can_be_primary` (which
/// production cannot mint) would expose the tie's local-bias and is
/// excluded by construction. `is_observer` stays free across the set (it
/// is a version-independent OR-ratchet).
#[test]
fn merge_capability_is_commutative_associative_idempotent() {
    use super::super::merge::merge_capability;

    // `can_be_primary` derived from the version so two same-version entries
    // never disagree on it (the production invariant). `is_observer` is the
    // free dimension. Versions span (0,0) < (1,0) < (2,0) < (2,5) so the
    // higher-version `can_be_primary`-pick and `max(version)` arms fire.
    let cbp_of = |epoch: u64, seq: u32| (epoch + u64::from(seq)) % 2 == 1;
    let mut variants = vec![CapabilityEntry::Departed];
    for (epoch, seq) in [(0u64, 0u32), (1, 0), (2, 0), (2, 5)] {
        for is_observer in [false, true] {
            variants.push(adv(is_observer, cbp_of(epoch, seq), epoch, seq));
        }
    }

    // Idempotence: merge(x, x) == x for every variant.
    for x in &variants {
        assert_eq!(
            merge_capability(x, x),
            x.clone(),
            "merge_capability is not idempotent for {x:?}"
        );
    }

    // Commutativity: merge(a, b) == merge(b, a) for every ordered pair.
    for a in &variants {
        for b in &variants {
            assert_eq!(
                merge_capability(a, b),
                merge_capability(b, a),
                "merge_capability({a:?}, {b:?}) != merge_capability({b:?}, {a:?})"
            );
        }
    }

    // Associativity: merge(merge(a, b), c) == merge(a, merge(b, c)).
    for a in &variants {
        for b in &variants {
            for c in &variants {
                let left = merge_capability(&merge_capability(a, b), c);
                let right = merge_capability(a, &merge_capability(b, c));
                assert_eq!(
                    left, right,
                    "merge_capability is not associative for ({a:?}, {b:?}, {c:?})"
                );
            }
        }
    }
}

/// Pin the EXACT lattice outcomes `merge_capability` produces (read off
/// `merge.rs::merge_capability`), arm by arm, so a future edit that
/// silently changes a rule is caught — not just order-independence.
#[test]
fn merge_capability_pins_each_lattice_arm() {
    use super::super::merge::merge_capability;

    // 1. Departed is an absorbing element on BOTH sides.
    assert_eq!(
        merge_capability(&CapabilityEntry::Departed, &adv(true, true, 9, 9)),
        CapabilityEntry::Departed,
        "Departed ∨ Advertised = Departed"
    );
    assert_eq!(
        merge_capability(&adv(true, true, 9, 9), &CapabilityEntry::Departed),
        CapabilityEntry::Departed,
        "Advertised ∨ Departed = Departed"
    );
    assert_eq!(
        merge_capability(&CapabilityEntry::Departed, &CapabilityEntry::Departed),
        CapabilityEntry::Departed,
    );

    // 2. is_observer is a pure upward OR-ratchet (version-independent).
    //    A `false` observer at a HIGHER version never un-observes a `true`
    //    at a lower version.
    assert_eq!(
        merge_capability(&adv(true, false, 1, 0), &adv(false, false, 5, 0)),
        adv(true, false, 5, 0),
        "is_observer ratchets up (OR) and never regresses, even at a higher incoming version"
    );

    // 3. can_be_primary follows the HIGHER cap_version: a newer
    //    `can_be_primary=false` beats an older `true`.
    assert_eq!(
        merge_capability(&adv(false, true, 1, 0), &adv(false, false, 2, 0)),
        adv(false, false, 2, 0),
        "can_be_primary follows the higher cap_version (newer false wins)"
    );
    //    ...and an older `false` does NOT beat a newer `true`.
    assert_eq!(
        merge_capability(&adv(false, false, 1, 0), &adv(false, true, 2, 0)),
        adv(false, true, 2, 0),
        "can_be_primary follows the higher cap_version (newer true wins)"
    );

    // 4. Equal cap_version: can_be_primary keeps LOCAL (idempotent — the
    //    same advertisement redelivered; the `iv > lv` gate is strict).
    assert_eq!(
        merge_capability(&adv(false, true, 2, 0), &adv(false, false, 2, 0)),
        adv(false, true, 2, 0),
        "at EQUAL cap_version, can_be_primary keeps local (strict `iv > lv` gate)"
    );

    // 5. cap_version is the max of the two (lexicographic on (epoch, seq)).
    assert_eq!(
        merge_capability(&adv(false, false, 2, 3), &adv(false, false, 2, 9)),
        adv(false, false, 2, 9),
        "cap_version = max((epoch, seq))"
    );
}
