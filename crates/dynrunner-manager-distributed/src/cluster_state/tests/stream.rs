//! Tests for the snapshot-STREAM partition policy + payload codec
//! (`cluster_state::stream`) — the incremental replacement of the
//! monolithic snapshot-JSON transfer.
//!
//! Pins, in order: (a) full-stream restore == monolithic `snapshot()`
//! restore (state identity — the wire model changed, the converged
//! state must not); (b) the partition rules (head carries the
//! control-plane facts and NO bulk/tallies; batches ride canonical
//! sorted order with co-keyed outputs; the tail carries the tallies);
//! (c) the #358-per-stream tally rule — capture-at-start + ship-last —
//! under a mid-stream completion delivered BOTH through a later
//! package window and through live gossip (the permanent-overshoot
//! hazard the partition exists to prevent); (d) resume semantics
//! (fresh-from-cursor omits the tail; reposition keeps the capture);
//! (e) the byte budget; (f) production-shape end-to-end digest
//! equality with bounded packages.

use super::*;
use crate::cluster_state::stream::{PACKAGE_BYTE_BUDGET, PACKAGE_MAX_TASKS};
use crate::cluster_state::{
    SnapshotStreamPlan, StreamPackage, decode_stream_payload, encode_stream_payload,
};

fn add_task(s: &mut ClusterState<RunnerIdentifier>, name: &str) {
    s.apply(ClusterMutation::TaskAdded {
        hash: name.to_string(),
        task: mk_task(name),
        def_id: None,
    });
}

fn complete_task(s: &mut ClusterState<RunnerIdentifier>, name: &str) -> ClusterMutation<RunnerIdentifier> {
    let m = ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: name.to_string(),
        result_data: None,
    };
    s.apply(m.clone());
    m
}

/// A donor with every partition populated: tasks (some terminal, so the
/// F4 tallies are non-zero), a primary register, membership, and a
/// custom message — the shape every cross-partition pin below reads.
fn donor() -> ClusterState<RunnerIdentifier> {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::PrimaryChanged {
        new: "prim-1".into(),
        epoch: 3,
        reason: Default::default(),
    });
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "sec-1".into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: Default::default(),
        member_gen: 0,
    });
    for i in 0..40 {
        add_task(&mut s, &format!("t{i:03}"));
    }
    for i in 0..7 {
        complete_task(&mut s, &format!("t{i:03}"));
    }
    s
}

/// Drive a full plan to completion, returning every built package.
fn drain_plan(
    plan: &mut SnapshotStreamPlan,
    state: &ClusterState<RunnerIdentifier>,
) -> Vec<StreamPackage> {
    let mut out = Vec::new();
    while let Some(built) = plan.next_package(state) {
        out.push(built.expect("package encodes"));
    }
    assert!(plan.complete(), "drained plan must report complete");
    out
}

fn restore_all(joiner: &mut ClusterState<RunnerIdentifier>, packages: &[StreamPackage]) {
    for p in packages {
        let snap = decode_stream_payload::<RunnerIdentifier>(&p.payload).expect("payload decodes");
        joiner.restore(snap);
    }
}

/// (a) State identity: a joiner restored from the full package stream is
/// digest-equal to one restored from the monolithic `snapshot()` — and
/// to the donor itself (the comparison anti-entropy quiesces on).
#[test]
fn full_stream_restores_to_the_same_state_as_the_monolithic_snapshot() {
    let s = donor();
    let mut plan = SnapshotStreamPlan::new(&s, None, &[]);
    let packages = drain_plan(&mut plan, &s);
    assert!(
        packages.last().expect("non-empty").done,
        "final package carries done"
    );

    let mut via_stream = ClusterState::<RunnerIdentifier>::new();
    restore_all(&mut via_stream, &packages);

    let mut via_monolith = ClusterState::<RunnerIdentifier>::new();
    via_monolith.restore(s.snapshot());

    assert_eq!(via_stream.digest(), via_monolith.digest());
    assert_eq!(via_stream.digest(), s.digest());
    assert_eq!(via_stream.task_count(), s.task_count());
}

/// (b) Partition rules: the head is bulk-free and tally-free but carries
/// the control-plane facts; batches ascend the canonical key order with
/// ascending cursors and co-keyed outputs; ONLY the tail carries the
/// tally map.
#[test]
fn head_carries_control_plane_batches_carry_bulk_tail_carries_tallies() {
    let mut s = donor();
    // Give one completed task an output entry so the co-keying is
    // observable.
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "t039".into(),
        result_data: Some(vec![1, 2, 3]),
    });
    let mut plan = SnapshotStreamPlan::new(&s, None, &[]);
    let packages = drain_plan(&mut plan, &s);
    assert!(packages.len() >= 3, "head + >=1 batch + tail");

    let head = decode_stream_payload::<RunnerIdentifier>(&packages[0].payload).unwrap();
    assert!(head.tasks.is_empty(), "head must not carry the task bulk");
    assert!(head.task_outputs.is_empty());
    assert!(
        head.phase_event_tallies.is_empty(),
        "the join-bumped tally map must NOT ride the head (#358 order rule)"
    );
    assert_eq!(head.current_primary.as_deref(), Some("prim-1"));
    assert_eq!(head.primary_epoch, 3);
    assert!(head.capabilities.contains_key("sec-1"));
    assert!(packages[0].cursor.is_none());

    let tail = decode_stream_payload::<RunnerIdentifier>(&packages.last().unwrap().payload).unwrap();
    assert!(tail.tasks.is_empty());
    assert!(
        !tail.phase_event_tallies.is_empty(),
        "the tail must carry the tally map"
    );

    // Middle packages: canonical ascending order, ascending cursors,
    // outputs co-keyed with their tasks, and no tallies anywhere but
    // the tail.
    let mut last_cursor = String::new();
    let mut seen_keys: Vec<String> = Vec::new();
    for p in &packages[1..packages.len() - 1] {
        let part = decode_stream_payload::<RunnerIdentifier>(&p.payload).unwrap();
        assert!(part.phase_event_tallies.is_empty());
        for k in part.task_outputs.keys() {
            assert!(
                part.tasks.contains_key(k),
                "an output entry must ride the same package as its task"
            );
        }
        let mut keys: Vec<String> = part.tasks.keys().cloned().collect();
        keys.sort();
        let cursor = p.cursor.clone().expect("task batches carry a cursor");
        assert!(cursor > last_cursor, "cursors ascend");
        assert_eq!(
            keys.last(),
            Some(&cursor),
            "the cursor is the batch's highest key"
        );
        last_cursor = cursor;
        seen_keys.extend(keys);
    }
    let mut expected: Vec<String> = s.tasks.keys().cloned().collect();
    expected.sort();
    assert_eq!(seen_keys, expected, "batches cover the ledger in order");
}

/// (c) THE tally-overshoot pin (#358 projected onto the stream), in the
/// interleaving that actually bites: a task completes at the donor
/// AFTER its batch already shipped (the batch carried it non-terminal),
/// the TAIL ships next, and the completion's live gossip reaches the
/// joiner only AFTER the tail import. With capture-at-START the
/// imported tally cannot count the late event, so the joiner's
/// subsequent bump lands exactly once and both replicas agree. A tally
/// captured at tail-BUILD time counts the event whose state the stream
/// never delivered — the joiner imports it AND bumps on the late
/// gossip, and grow-max freezes the double-count in permanently
/// (spreading it: the overshot replica looks "ahead" to every digest).
#[test]
fn mid_stream_completion_never_overshoots_the_tally() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for i in 0..6 {
        add_task(&mut s, &format!("t{i}"));
    }
    complete_task(&mut s, "t0");

    let mut plan = SnapshotStreamPlan::new(&s, None, &[]);
    let mut joiner = ClusterState::<RunnerIdentifier>::new();

    // Head + every task batch ship first (t5 rides its batch as
    // Pending). The fixture is small enough for one batch; emit until
    // only the tail remains.
    let head = plan.next_package(&s).unwrap().unwrap();
    joiner.restore(decode_stream_payload::<RunnerIdentifier>(&head.payload).unwrap());
    loop {
        let pkg = plan
            .next_package(&s)
            .expect("tail still pending")
            .expect("package encodes");
        let snap = decode_stream_payload::<RunnerIdentifier>(&pkg.payload).unwrap();
        let is_tail_shape = snap.tasks.is_empty();
        assert!(
            !is_tail_shape,
            "fixture expects task batches before the tail"
        );
        joiner.restore(snap);
        if pkg.cursor.as_deref() == Some("t5") {
            break;
        }
    }

    // AFTER its batch shipped: t5 completes at the donor. The donor's
    // live tally now counts an event the stream's STATES never carried.
    let live = complete_task(&mut s, "t5");

    // The tail ships next (capture-at-start: WITHOUT t5's event).
    let rest = drain_plan(&mut plan, &s);
    restore_all(&mut joiner, &rest);

    // The completion's live gossip reaches the joiner LAST — after the
    // tally import. The join bumps exactly once for the winning
    // transition; an import that already counted the event would now
    // freeze a permanent double-count into the grow-max lattice.
    joiner.apply(live);

    let key = (PhaseId::from("p0"), PhaseTally::Completed);
    assert_eq!(
        joiner.phase_event_tally_for(&key),
        s.phase_event_tally_for(&key),
        "joiner tally must equal donor tally (no double count, no overshoot)"
    );
    assert_eq!(joiner.digest(), s.digest(), "fully converged");
}

/// (d) Resume: a FRESH plan built from a cursor ships only the keys
/// strictly after it and OMITS the tail (no provably-safe capture
/// exists); a repositioned ALIVE plan keeps its capture and re-sends
/// head + remainder + tail.
#[test]
fn resume_semantics_fresh_omits_tail_reposition_keeps_capture() {
    let s = donor();
    let mut all_keys: Vec<String> = s.tasks.keys().cloned().collect();
    all_keys.sort();
    let cursor = &all_keys[24];

    // Fresh-from-cursor plan (the responder lost the stream).
    let mut fresh = SnapshotStreamPlan::new(&s, Some(cursor), &[]);
    let packages = drain_plan(&mut fresh, &s);
    let mut shipped: Vec<String> = Vec::new();
    for p in &packages {
        let part = decode_stream_payload::<RunnerIdentifier>(&p.payload).unwrap();
        assert!(
            part.phase_event_tallies.is_empty(),
            "a fresh-from-cursor stream must omit the tail tallies"
        );
        shipped.extend(part.tasks.keys().cloned());
    }
    shipped.sort();
    assert_eq!(
        shipped,
        all_keys[25..].to_vec(),
        "only keys strictly after the cursor ship on a resume"
    );
    assert!(packages.last().unwrap().done);

    // Reposition of a still-alive plan: capture kept, tail still ships.
    let mut alive = SnapshotStreamPlan::new(&s, None, &[]);
    // Ship head + first batch, then "the requester re-requests".
    let _head = alive.next_package(&s).unwrap().unwrap();
    let first_batch = alive.next_package(&s).unwrap().unwrap();
    alive.reposition(first_batch.cursor.as_deref());
    let resumed = drain_plan(&mut alive, &s);
    let tail =
        decode_stream_payload::<RunnerIdentifier>(&resumed.last().unwrap().payload).unwrap();
    assert!(
        !tail.phase_event_tallies.is_empty(),
        "a repositioned alive plan keeps its tally capture"
    );
}

/// (e) Byte budget: a ledger of large entries splits into multiple
/// packages, each raw-CBOR payload within budget + one entry's slack,
/// and a joiner restored from them still converges.
#[test]
fn task_batches_respect_the_byte_budget() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for i in 0..24 {
        let name = format!("big{i:02}");
        let mut t = mk_task(&name);
        // ~512 KiB payload per task ⇒ ~4 tasks per 2 MiB budget.
        t.payload = serde_json::json!({ "blob": "x".repeat(512 * 1024) });
        s.apply(ClusterMutation::TaskAdded {
            hash: name.clone(),
            task: t,
            def_id: None,
        });
    }
    let mut plan = SnapshotStreamPlan::new(&s, None, &[]);
    let packages = drain_plan(&mut plan, &s);
    // head + >=4 batches (24 × 512KiB ≈ 12 MiB over a 2 MiB budget).
    assert!(
        packages.len() >= 5,
        "expected multiple bounded batches, got {}",
        packages.len()
    );
    use base64::Engine as _;
    for p in &packages {
        let raw = base64::engine::general_purpose::STANDARD
            .decode(&p.payload)
            .unwrap();
        assert!(
            raw.len() < PACKAGE_BYTE_BUDGET + 600 * 1024,
            "raw package must stay within budget + one entry slack, got {}",
            raw.len()
        );
    }
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    restore_all(&mut joiner, &packages);
    assert_eq!(joiner.digest(), s.digest());
}

/// A key the plan captured but whose entry no longer exists at
/// batch-build time is skipped (the plan holds keys, not entries) while
/// the cursor still advances past it.
#[test]
fn vanished_keys_are_skipped_at_build_time() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for i in 0..4 {
        add_task(&mut s, &format!("t{i}"));
    }
    let mut plan = SnapshotStreamPlan::new(&s, None, &[]);
    // Build the packages against a DIFFERENT state missing t2 — the
    // direct seam for "entry vanished since capture".
    let mut shrunk = ClusterState::<RunnerIdentifier>::new();
    for i in [0usize, 1, 3] {
        add_task(&mut shrunk, &format!("t{i}"));
    }
    let packages = drain_plan(&mut plan, &shrunk);
    let mut shipped: Vec<String> = Vec::new();
    for p in &packages {
        let part = decode_stream_payload::<RunnerIdentifier>(&p.payload).unwrap();
        shipped.extend(part.tasks.keys().cloned());
    }
    shipped.sort();
    assert_eq!(shipped, vec!["t0", "t1", "t3"]);
    assert!(packages.last().unwrap().done);
}

/// Payload codec round-trip: encode → decode is the identity on the
/// partial snapshot (CBOR handles the tuple-keyed maps through the same
/// serde adapters the JSON wire used).
#[test]
fn payload_codec_round_trips() {
    let s = donor();
    let snap = s.snapshot();
    let encoded = encode_stream_payload(&snap).unwrap();
    let decoded: ClusterStateSnapshot<RunnerIdentifier> =
        decode_stream_payload(&encoded).unwrap();
    let mut a = ClusterState::<RunnerIdentifier>::new();
    a.restore(snap);
    let mut b = ClusterState::<RunnerIdentifier>::new();
    b.restore(decoded);
    assert_eq!(a.digest(), b.digest());
}

/// (f) Production-shape chain: a ledger in the asm-dataset shape
/// (~1.5 KB serialized per task) streams as MANY bounded packages —
/// none anywhere near the 96 MiB wire cap that forced the old
/// monolith through FrameChunk — and the union of packages restores a
/// fresh joiner to FULL CRDT equality (digest equality — the same
/// comparison anti-entropy quiesces on), WHILE live mutations land
/// concurrently at both ends.
#[test]
fn production_shaped_ledger_streams_bounded_and_converges_under_live_gossip() {
    const TASKS: usize = 20_000;
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for i in 0..TASKS {
        let name = format!("crate-{i:06}");
        let mut t = mk_task(&name);
        t.payload = serde_json::json!({
            "source": format!("/network/corpus/shard-{:03}/{}.zip", i % 512, name),
            "blob": "x".repeat(1100),
            "index": i,
        });
        s.apply(ClusterMutation::TaskAdded {
            hash: name,
            task: t,
            def_id: None,
        });
    }
    for i in 0..500 {
        complete_task(&mut s, &format!("crate-{i:06}"));
    }

    let mut plan = SnapshotStreamPlan::new(&s, None, &[]);
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    let mut packages = 0usize;
    let mut max_raw = 0usize;
    use base64::Engine as _;
    let mut i = 600usize;
    while let Some(built) = plan.next_package(&s) {
        let p = built.unwrap();
        packages += 1;
        max_raw = max_raw.max(
            base64::engine::general_purpose::STANDARD
                .decode(&p.payload)
                .unwrap()
                .len(),
        );
        joiner.restore(decode_stream_payload::<RunnerIdentifier>(&p.payload).unwrap());
        // Live gossip lands between packages at BOTH replicas — the
        // no-consistent-cut property under real interleaving.
        if i < 650 {
            let live = complete_task(&mut s, &format!("crate-{i:06}"));
            joiner.apply(live);
            i += 1;
        }
    }
    assert!(
        packages >= 10,
        "a ~30 MB ledger must stream as many bounded packages, got {packages}"
    );
    assert!(
        max_raw < 4 * 1024 * 1024,
        "every package stays in the 1–4 MiB band, max was {max_raw}"
    );
    assert!(max_raw < PACKAGE_BYTE_BUDGET + 64 * 1024);
    let _ = PACKAGE_MAX_TASKS; // bound exists; tiny-task ledgers pin it above
    assert_eq!(
        joiner.digest(),
        s.digest(),
        "the streamed joiner must converge to digest equality under live gossip"
    );
    assert_eq!(joiner.task_count(), s.task_count());
}

// ─────────────────────────────────────────────────────────────────────
// P1 range-scoped delta: the range-digest-narrowed stream is EQUIVALENT to
// a full snapshot merge (delta-merge ≡ full-merge), and a one-task change
// narrows the streamed key-set to one range.
// ─────────────────────────────────────────────────────────────────────

/// Tiny deterministic splitmix64 — no `rand` dependency. Drives the
/// property-style random divergences below reproducibly.
struct SplitMix(u64);
impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// Run a complete RANGE-SCOPED pull from `donor` onto `behind`: compute the
/// divergent buckets (the production `RangeDigest::divergent_ranges` path),
/// stream ONLY those buckets through the REAL `SnapshotStreamPlan` (with the
/// requester's resume cursor = None for a clean range pull), and `restore`
/// each package. Returns the streamed task KEY set, so a caller can assert
/// the streamed keys ⊆ the divergent ranges.
fn pull_divergent_ranges(
    behind: &mut ClusterState<RunnerIdentifier>,
    donor: &ClusterState<RunnerIdentifier>,
) -> std::collections::HashSet<String> {
    let behind_rd = behind.tasks_range_digest();
    let donor_rd = donor.tasks_range_digest();
    let divergent = behind_rd.divergent_ranges(&donor_rd);

    // The responder builds a plan filtered to exactly the divergent ranges
    // (resume_after None — a fresh range pull) and streams it.
    let mut plan = SnapshotStreamPlan::new(donor, None, &divergent);
    let mut streamed_keys = std::collections::HashSet::new();
    while let Some(built) = plan.next_package(donor) {
        let p = built.expect("range package encodes");
        let snap = decode_stream_payload::<RunnerIdentifier>(&p.payload).expect("decodes");
        for k in snap.tasks.keys() {
            streamed_keys.insert(k.clone());
        }
        behind.restore(snap);
    }
    streamed_keys
}

/// HEADLINE DIFFERENTIAL TEST (delta-merge ≡ full-merge): for many random
/// divergences between two ledgers, the state reached by applying the
/// RANGE-SCOPED delta equals the state reached by a FULL snapshot merge.
/// This is the whole-point correctness pin — a wrong range split would
/// silently LOSE entries on the delta, so the two converged states would
/// differ. Property-style over 40 random divergence shapes (different key
/// universes, different terminal-state overlaps, different missing slices).
#[test]
fn range_delta_merge_equals_full_merge_property() {
    let mut rng = SplitMix(0xD1E7_C0DE_1234_5678);
    for trial in 0..40 {
        let n: usize = 60 + (rng.next() % 240) as usize; // 60..300 keys

        // The AHEAD donor: a ledger where a random slice is terminalized.
        let mut donor = ClusterState::<RunnerIdentifier>::new();
        for i in 0..n {
            add_task(&mut donor, &format!("k{trial:02}-{i:04}"));
        }
        for i in 0..n {
            if rng.next().is_multiple_of(3) {
                complete_task(&mut donor, &format!("k{trial:02}-{i:04}"));
            }
        }

        // The BEHIND replica: starts from a RANDOM SUBSET of the donor's
        // keys at possibly-older states — the divergence the delta heals.
        // Built by adding a random subset and completing an even smaller
        // random subset, so it is missing keys AND lagging on some states.
        let mut behind = ClusterState::<RunnerIdentifier>::new();
        for i in 0..n {
            if !rng.next().is_multiple_of(4) {
                // ~75% of keys present (some missing entirely)
                add_task(&mut behind, &format!("k{trial:02}-{i:04}"));
                if rng.next().is_multiple_of(5) {
                    // a few of those completed (most lag at Pending)
                    complete_task(&mut behind, &format!("k{trial:02}-{i:04}"));
                }
            }
        }

        // Reference: a FULL snapshot merge of the donor onto a clone of the
        // behind replica.
        let mut via_full = ClusterState::<RunnerIdentifier>::new();
        via_full.restore(behind.snapshot());
        via_full.restore(donor.snapshot());

        // The behind replica WAS task-behind the full merge before the
        // delta (the delta has real work to do — otherwise the test is
        // vacuous). The reference is `via_full` (the lattice JOIN of behind
        // ∪ donor), NOT the bare donor: a CRDT merge can leave the result
        // ABOVE the donor alone, because `behind` may hold keys/states the
        // donor lacks. The correct delta target is the join.
        let behind_was_behind = behind.digest().tasks_hash != via_full.digest().tasks_hash
            || behind.digest().tasks_count != via_full.digest().tasks_count;

        // Under test: the RANGE-SCOPED delta merge.
        let mut via_delta = ClusterState::<RunnerIdentifier>::new();
        via_delta.restore(behind.snapshot());
        let streamed = pull_divergent_ranges(&mut via_delta, &donor);

        // (1) THE HEADLINE — delta-merge ≡ full-merge: applying the
        // RANGE-SCOPED delta reaches the SAME converged task ledger as a FULL
        // snapshot merge. A wrong range split (a key bucketed differently on
        // the two sides, or a divergent bucket the requester failed to ask
        // for) would silently LOSE an entry, so the two task folds would
        // differ. Equal fold + equal count over 40 random divergences is the
        // proof the delta is faithful.
        assert_eq!(
            via_delta.digest().tasks_hash,
            via_full.digest().tasks_hash,
            "trial {trial}: delta-merge tasks_hash must equal full-merge \
             (a wrong range split would lose entries)"
        );
        assert_eq!(
            via_delta.digest().tasks_count,
            via_full.digest().tasks_count,
            "trial {trial}: delta-merge task count must equal full-merge"
        );
        // (2) non-vacuity: the delta actually closed a real gap (the behind
        // replica was task-behind the full merge, and now equals it). Skips
        // the rare trial where the random behind already matched the join.
        if behind_was_behind {
            assert_ne!(
                behind.digest().tasks_hash,
                via_delta.digest().tasks_hash,
                "trial {trial}: the delta must have CHANGED the behind ledger \
                 (it had a real gap to close)"
            );
        }
        // NOTE: the whole-digest `is_behind` is deliberately NOT asserted —
        // a range-scoped pull omits the tail (`phase_event_tallies`, the F4
        // grow-max map the donor bumped on completions), which heals through
        // the anti-entropy digest on the next pull, exactly as the resume
        // case documents. P1 is a TASK delta; the tallies are out of its
        // scope by the capture-at-start overshoot rule.
        // (3) the streamed keys are ALL within the divergent ranges (the
        // narrowing actually happened): every streamed key's bucket was in
        // the divergent set the requester asked for.
        let pre = {
            let mut b = ClusterState::<RunnerIdentifier>::new();
            b.restore(behind.snapshot());
            b
        };
        let divergent: std::collections::HashSet<u16> = pre
            .tasks_range_digest()
            .divergent_ranges(&donor.tasks_range_digest())
            .into_iter()
            .collect();
        for k in &streamed {
            let bucket = crate::cluster_state::range_index_for_test(k) as u16;
            assert!(
                divergent.contains(&bucket),
                "trial {trial}: streamed key {k} is in bucket {bucket}, which \
                 was NOT in the divergent set — the delta streamed an \
                 unrequested range"
            );
        }
    }
}

/// A ONE-TASK change re-pulls ONLY its range: the streamed key-set ⊆ the
/// single divergent bucket (and is non-empty — the changed key DID stream).
/// This is the storm-killer payoff at the stream grain — a one-task churn
/// transfers ~one bucket, not all keys.
#[test]
fn one_task_change_streams_only_its_range() {
    // Two converged replicas.
    let mut donor = ClusterState::<RunnerIdentifier>::new();
    for i in 0..400 {
        add_task(&mut donor, &format!("z-{i:04}"));
    }
    let mut behind = ClusterState::<RunnerIdentifier>::new();
    behind.restore(donor.snapshot());
    assert!(
        behind.tasks_range_digest().divergent_ranges(&donor.tasks_range_digest()).is_empty(),
        "precondition: the two replicas are converged (no divergent range)"
    );

    // ONE task advances on the donor.
    donor.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "z-0257".into(),
        result_data: None,
    });
    let changed_bucket = crate::cluster_state::range_index_for_test("z-0257") as u16;

    let streamed = pull_divergent_ranges(&mut behind, &donor);

    // The streamed keys are all in the one changed bucket, and the changed
    // key itself streamed.
    assert!(streamed.contains("z-0257"), "the changed key must stream");
    for k in &streamed {
        assert_eq!(
            crate::cluster_state::range_index_for_test(k) as u16,
            changed_bucket,
            "every streamed key must be in the single divergent bucket"
        );
    }
    // And the delta fully converged the TASK LEDGER (the tail/tallies heal
    // via anti-entropy — a range pull is a task delta, see the property
    // test's note).
    assert_eq!(behind.digest().tasks_hash, donor.digest().tasks_hash);
    assert_eq!(behind.digest().tasks_count, donor.digest().tasks_count);
}
