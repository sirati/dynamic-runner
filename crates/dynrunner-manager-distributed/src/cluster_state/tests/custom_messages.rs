//! F5 custom-message inbox CRDT semantics: the `(origin, seq)`-keyed
//! sticky lattice (`absent ⊑ Unhandled ⊑ {Handled, Failed}`, join =
//! Handled-wins), the payload-drop tombstones, the per-origin
//! contiguous-prefix terminal watermark compaction (covering BOTH
//! terminals), and the snapshot round-trip THROUGH the wire JSON (the
//! #358 tuple-keyed-map lesson — a tuple-keyed snapshot field that
//! serializes fine empty must still survive serde_json with entries).

use super::*;
use crate::cluster_state::CustomMsgState;

fn posted(origin: &str, seq: u64, topic: &str, data: &[u8]) -> ClusterMutation<RunnerIdentifier> {
    ClusterMutation::CustomMessagePosted {
        origin: origin.into(),
        seq,
        topic: topic.into(),
        data: data.to_vec(),
    }
}

fn handled(origin: &str, seq: u64) -> ClusterMutation<RunnerIdentifier> {
    ClusterMutation::CustomMessageHandled {
        origin: origin.into(),
        seq,
    }
}

fn failed(origin: &str, seq: u64) -> ClusterMutation<RunnerIdentifier> {
    ClusterMutation::CustomMessageFailed {
        origin: origin.into(),
        seq,
    }
}

/// Posted inserts `Unhandled`; a duplicate Posted (a transport replay's
/// re-post) NoOps and never clobbers the payload.
#[test]
fn posted_vacant_inserts_unhandled_and_duplicate_noops() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(s.apply(posted("sec-1", 1, "t", b"v1")), ApplyOutcome::Applied);
    assert_eq!(s.apply(posted("sec-1", 1, "t", b"v2")), ApplyOutcome::NoOp);
    assert_eq!(
        s.custom_message_state("sec-1", 1),
        Some(CustomMsgState::Unhandled {
            topic: "t".into(),
            data: b"v1".to_vec()
        })
    );
    assert_eq!(
        s.unhandled_custom_messages(),
        vec![("sec-1".to_string(), 1, "t".to_string(), b"v1".to_vec())]
    );
}

/// The lattice converges across ALL FOUR Posted/Handled arrival orders
/// — including `Handled` BEFORE its `Posted` (a latch on an absent key)
/// — and the payload is dropped at `Handled`.
#[test]
fn lattice_converges_in_all_arrival_orders_and_drops_payload() {
    // Use seq 2 with seq 1 left unposted so the watermark CANNOT
    // compact — the converged value is the in-map tombstone itself,
    // which makes the four orders directly comparable.
    let orders: [&[&str]; 4] = [
        &["posted", "handled"],
        &["handled", "posted"],
        &["posted", "handled", "posted"],
        &["handled", "posted", "handled"],
    ];
    for order in orders {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        for step in order {
            match *step {
                "posted" => {
                    s.apply(posted("sec-1", 2, "t", b"payload"));
                }
                "handled" => {
                    s.apply(handled("sec-1", 2));
                }
                _ => unreachable!(),
            }
        }
        assert_eq!(
            s.custom_message_state("sec-1", 2),
            Some(CustomMsgState::Handled),
            "order {order:?} must converge to the Handled latch"
        );
        assert!(
            s.unhandled_custom_messages().is_empty(),
            "order {order:?}: no Unhandled residue (payload dropped)"
        );
        // No compaction happened (seq 1 is a gap), so the tombstone is
        // physically present.
        assert_eq!(s.custom_message_count(), 1, "order {order:?}");
        assert_eq!(s.custom_terminal_watermark("sec-1"), None);
    }
}

/// Handled-before-Posted: the latch wins regardless of order — the late
/// `Posted` NoOps against the latched entry and never resurrects an
/// `Unhandled` (the DiscoveryDebt sticky-latch precedent).
#[test]
fn handled_latch_locks_out_a_late_posted() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(s.apply(handled("sec-1", 2)), ApplyOutcome::Applied);
    assert_eq!(s.apply(posted("sec-1", 2, "t", b"late")), ApplyOutcome::NoOp);
    assert_eq!(
        s.custom_message_state("sec-1", 2),
        Some(CustomMsgState::Handled)
    );
    // Idempotent re-handle.
    assert_eq!(s.apply(handled("sec-1", 2)), ApplyOutcome::NoOp);
}

/// Watermark compaction: handling the contiguous prefix 1..=3 physically
/// prunes the tombstones, records `watermark = 3`, and a re-applied
/// `Posted { seq <= 3 }` (an at-least-once replay arriving after
/// compaction) is a NoOp by watermark check. A gap stops the walk.
#[test]
fn watermark_compacts_contiguous_handled_prefix_and_subsumes_replays() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for seq in 1..=4 {
        s.apply(posted("sec-1", seq, "t", b"x"));
    }
    // Handle 1, 3, 4 — the prefix stops at the unhandled 2.
    s.apply(handled("sec-1", 1));
    s.apply(handled("sec-1", 3));
    s.apply(handled("sec-1", 4));
    assert_eq!(s.custom_terminal_watermark("sec-1"), Some(1));
    // 3 live entries remain (2 Unhandled + the 3,4 tombstones the gap
    // protects); the seq-1 tombstone is pruned.
    assert_eq!(s.custom_message_count(), 3);

    // Handling 2 closes the gap: the walk consumes 2,3,4 in one pass.
    s.apply(handled("sec-1", 2));
    assert_eq!(s.custom_terminal_watermark("sec-1"), Some(4));
    assert_eq!(s.custom_message_count(), 0);

    // A replayed Posted at-or-below the watermark is subsumed (NoOp) —
    // it must NOT resurrect an Unhandled entry.
    for seq in 1..=4 {
        assert_eq!(
            s.apply(posted("sec-1", seq, "t", b"replay")),
            ApplyOutcome::NoOp,
            "seq {seq} is watermark-subsumed"
        );
        // And reads as Handled (the watermark IS its record).
        assert_eq!(
            s.custom_message_state("sec-1", seq),
            Some(CustomMsgState::Handled)
        );
    }
    // A replayed Handled below the watermark is likewise a NoOp.
    assert_eq!(s.apply(handled("sec-1", 2)), ApplyOutcome::NoOp);
}

/// Origins are independent: one origin's compaction never touches
/// another's entries, and the dispatch read surface sorts by
/// `(origin, seq)`.
#[test]
fn origins_are_independent_and_read_surface_is_sorted() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(posted("sec-2", 2, "t", b"b2"));
    s.apply(posted("sec-1", 1, "t", b"a1"));
    s.apply(posted("sec-2", 1, "t", b"b1"));
    s.apply(handled("sec-1", 1));
    assert_eq!(s.custom_terminal_watermark("sec-1"), Some(1));
    assert_eq!(s.custom_terminal_watermark("sec-2"), None);
    assert_eq!(
        s.unhandled_custom_messages(),
        vec![
            ("sec-2".to_string(), 1, "t".to_string(), b"b1".to_vec()),
            ("sec-2".to_string(), 2, "t".to_string(), b"b2".to_vec()),
        ]
    );
}

/// `Failed` is a sticky terminal latch exactly like `Handled`: it
/// converges across all arrival orders (including Failed-before-Posted
/// on an absent key), drops the payload, locks out a late `Posted`,
/// never surfaces on the dispatch read surface, and is idempotent.
#[test]
fn failed_latch_converges_in_all_arrival_orders_and_drops_payload() {
    // seq 2 with seq 1 unposted — the gap blocks compaction so the
    // converged value is the in-map tombstone itself.
    let orders: [&[&str]; 4] = [
        &["posted", "failed"],
        &["failed", "posted"],
        &["posted", "failed", "posted"],
        &["failed", "posted", "failed"],
    ];
    for order in orders {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        for step in order {
            match *step {
                "posted" => {
                    s.apply(posted("sec-1", 2, "t", b"payload"));
                }
                "failed" => {
                    s.apply(failed("sec-1", 2));
                }
                _ => unreachable!(),
            }
        }
        assert_eq!(
            s.custom_message_state("sec-1", 2),
            Some(CustomMsgState::Failed),
            "order {order:?} must converge to the Failed latch"
        );
        assert!(
            s.unhandled_custom_messages().is_empty(),
            "order {order:?}: Failed is terminal — never replayed"
        );
        assert_eq!(s.custom_message_count(), 1, "order {order:?}");
        assert_eq!(s.custom_terminal_watermark("sec-1"), None);
    }
}

/// The deterministic Handled-wins join for the THEORETICAL
/// Handled-vs-Failed conflict (the primary only ever originates ONE
/// terminal per message): both application orders converge to
/// `Handled` — `Failed → Handled` is Applied, `Handled → Failed` is a
/// NoOp.
#[test]
fn handled_vs_failed_join_is_handled_wins_in_both_orders() {
    // Order 1: Failed first, Handled second → flips to Handled.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(posted("sec-1", 2, "t", b"x"));
    assert_eq!(s.apply(failed("sec-1", 2)), ApplyOutcome::Applied);
    assert_eq!(s.apply(handled("sec-1", 2)), ApplyOutcome::Applied);
    assert_eq!(
        s.custom_message_state("sec-1", 2),
        Some(CustomMsgState::Handled)
    );

    // Order 2: Handled first, Failed second → Failed NoOps.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(posted("sec-1", 2, "t", b"x"));
    assert_eq!(s.apply(handled("sec-1", 2)), ApplyOutcome::Applied);
    assert_eq!(s.apply(failed("sec-1", 2)), ApplyOutcome::NoOp);
    assert_eq!(
        s.custom_message_state("sec-1", 2),
        Some(CustomMsgState::Handled)
    );

    // Idempotent re-fail on a Failed entry.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(failed("sec-1", 2));
    assert_eq!(s.apply(failed("sec-1", 2)), ApplyOutcome::NoOp);
}

/// The terminal watermark compacts over BOTH tombstone kinds: a mixed
/// Handled/Failed contiguous prefix prunes in one walk, replays at or
/// below the watermark are subsumed for all three mutations, and the
/// label is erased (a compacted Failed reads Handled — the watermark
/// IS its record).
#[test]
fn watermark_compacts_over_failed_tombstones_and_subsumes_replays() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for seq in 1..=3 {
        s.apply(posted("sec-1", seq, "t", b"x"));
    }
    s.apply(handled("sec-1", 1));
    s.apply(failed("sec-1", 2));
    s.apply(handled("sec-1", 3));
    assert_eq!(
        s.custom_terminal_watermark("sec-1"),
        Some(3),
        "the walk consumes Handled and Failed tombstones alike"
    );
    assert_eq!(s.custom_message_count(), 0);

    // Subsumed replays NoOp for every mutation kind…
    assert_eq!(s.apply(posted("sec-1", 2, "t", b"replay")), ApplyOutcome::NoOp);
    assert_eq!(s.apply(failed("sec-1", 2)), ApplyOutcome::NoOp);
    assert_eq!(s.apply(handled("sec-1", 2)), ApplyOutcome::NoOp);
    // …and the compacted key reads as terminal (label erased).
    assert_eq!(
        s.custom_message_state("sec-1", 2),
        Some(CustomMsgState::Handled)
    );
}

/// A snapshot carrying an uncompacted `Failed` tombstone survives the
/// wire payload codec and restores: the tombstone lands on the cold
/// replica, stays off the dispatch read surface, and the digest folds it
/// distinctly from `Unhandled` AND from `Handled` (anti-entropy sees a
/// lagging replica in both directions).
#[test]
fn failed_survives_stream_payload_and_digest_distinguishes_it() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(posted("sec-1", 2, "t", b"boom"));
    s.apply(failed("sec-1", 2)); // seq-1 gap → stays resident

    let payload = crate::cluster_state::encode_stream_payload(&s.snapshot())
        .expect("Failed tombstone serializes");
    let decoded: ClusterStateSnapshot<RunnerIdentifier> =
        crate::cluster_state::decode_stream_payload(&payload).expect("payload decodes");
    let mut cold = ClusterState::<RunnerIdentifier>::new();
    cold.restore(decoded);
    assert_eq!(
        cold.custom_message_state("sec-1", 2),
        Some(CustomMsgState::Failed)
    );
    assert!(cold.unhandled_custom_messages().is_empty());

    // Digest: Unhandled-vs-Failed at equal count diverges the fold.
    let mut behind = ClusterState::<RunnerIdentifier>::new();
    behind.apply(posted("sec-1", 2, "t", b"boom"));
    assert!(
        behind.digest().is_behind(&s.digest()),
        "Unhandled-vs-Failed at equal count must diverge the fold"
    );
    // And Failed-vs-Handled diverges too (the labels fold differently).
    let mut other = ClusterState::<RunnerIdentifier>::new();
    other.apply(posted("sec-1", 2, "t", b"boom"));
    other.apply(handled("sec-1", 2));
    assert!(
        other.digest().is_behind(&s.digest()) || s.digest().is_behind(&other.digest()),
        "Failed and Handled must not fold identically"
    );
}

/// THE #358 serde lesson, applied to the F5 field: a snapshot with a
/// NON-EMPTY tuple-keyed `custom_messages` map must survive the ACTUAL
/// wire encoding (the snapshot-stream payload codec — the pair-list
/// adapter keeps tuple keys representable in any serde format).
/// Round-trips through the codec and restore-merges into a cold
/// replica.
#[test]
fn snapshot_with_nonempty_custom_messages_survives_wire_codec_and_restores() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(posted("sec-1", 1, "t", b"one"));
    s.apply(posted("sec-1", 2, "batch", b"two"));
    s.apply(handled("sec-1", 1)); // compacts → watermark 1
    assert_eq!(s.custom_terminal_watermark("sec-1"), Some(1));

    // THE wire leg: SnapshotStreamPackage carries base64-CBOR payloads.
    let payload = crate::cluster_state::encode_stream_payload(&s.snapshot()).expect(
        "a non-empty tuple-keyed custom_messages map must serialize \
         through the wire payload codec (the pair-list adapter)",
    );
    let decoded: ClusterStateSnapshot<RunnerIdentifier> =
        crate::cluster_state::decode_stream_payload(&payload).expect("payload decodes");

    let mut cold = ClusterState::<RunnerIdentifier>::new();
    cold.restore(decoded);
    assert_eq!(cold.custom_terminal_watermark("sec-1"), Some(1));
    assert_eq!(
        cold.custom_message_state("sec-1", 1),
        Some(CustomMsgState::Handled),
        "watermark-subsumed seq reads Handled on the restored replica"
    );
    assert_eq!(
        cold.unhandled_custom_messages(),
        vec![("sec-1".to_string(), 2, "batch".to_string(), b"two".to_vec())]
    );

    // Idempotent: re-restoring the same snapshot changes nothing.
    let again: ClusterStateSnapshot<RunnerIdentifier> =
        crate::cluster_state::decode_stream_payload(&payload).expect("payload decodes twice");
    cold.restore(again);
    assert_eq!(cold.custom_message_count(), 1);
}

/// Restore merge semantics: an incoming `Handled` latches over a local
/// `Unhandled`; an incoming higher watermark prunes the local entries it
/// subsumes; a local `Handled` is never regressed by an incoming
/// `Unhandled`.
#[test]
fn restore_merges_latch_and_watermark_over_local_state() {
    // Replica A: handled everything up to 2 (compacted).
    let mut a = ClusterState::<RunnerIdentifier>::new();
    a.apply(posted("sec-1", 1, "t", b"x"));
    a.apply(posted("sec-1", 2, "t", b"y"));
    a.apply(handled("sec-1", 1));
    a.apply(handled("sec-1", 2));
    assert_eq!(a.custom_terminal_watermark("sec-1"), Some(2));

    // Replica B: lagging — both still Unhandled, plus a 3 A hasn't seen.
    let mut b = ClusterState::<RunnerIdentifier>::new();
    b.apply(posted("sec-1", 1, "t", b"x"));
    b.apply(posted("sec-1", 2, "t", b"y"));
    b.apply(posted("sec-1", 3, "t", b"z"));

    // B pulls A: the watermark prunes B's stale 1..=2; 3 stays Unhandled.
    b.restore(a.snapshot());
    assert_eq!(b.custom_terminal_watermark("sec-1"), Some(2));
    assert_eq!(
        b.unhandled_custom_messages(),
        vec![("sec-1".to_string(), 3, "t".to_string(), b"z".to_vec())]
    );

    // A pulls B: gains the Unhandled 3; its own handled state is never
    // regressed.
    a.restore(b.snapshot());
    assert_eq!(a.custom_terminal_watermark("sec-1"), Some(2));
    assert_eq!(
        a.unhandled_custom_messages(),
        vec![("sec-1".to_string(), 3, "t".to_string(), b"z".to_vec())]
    );

    // Convergence: both replicas now digest identically on the F5 fields.
    let (da, db) = (a.digest(), b.digest());
    assert_eq!(da.custom_messages_count, db.custom_messages_count);
    assert_eq!(da.custom_messages_hash, db.custom_messages_hash);
    assert_eq!(
        da.custom_terminal_watermarks_hash,
        db.custom_terminal_watermarks_hash
    );
}

/// Anti-entropy detection: a replica that misses an inbox entry — or
/// holds it `Unhandled` while the peer latched `Handled` (equal count,
/// divergent fold) — is behind; converged replicas quiesce.
#[test]
fn digest_detects_inbox_and_watermark_divergence() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let mut b = ClusterState::<RunnerIdentifier>::new();
    a.apply(posted("sec-1", 2, "t", b"x"));
    assert!(b.digest().is_behind(&a.digest()), "missing entry → behind");
    b.apply(posted("sec-1", 2, "t", b"x"));
    assert!(!b.digest().is_behind(&a.digest()), "converged → quiesce");
    assert!(!a.digest().is_behind(&b.digest()));

    // Equal count, divergent state fold (Unhandled vs Handled latch).
    a.apply(handled("sec-1", 2)); // seq 1 gap → no compaction, count equal
    assert!(
        b.digest().is_behind(&a.digest()),
        "Unhandled-vs-Handled at equal count must diverge the fold"
    );

    // Watermark divergence: A compacts fully; B (still uncompacted)
    // must detect the peer's watermark.
    a.apply(posted("sec-1", 1, "t", b"w"));
    a.apply(handled("sec-1", 1));
    assert_eq!(a.custom_terminal_watermark("sec-1"), Some(2));
    assert!(
        b.digest().is_behind(&a.digest()),
        "a peer watermark this replica lacks → behind"
    );
    b.restore(a.snapshot());
    assert!(!b.digest().is_behind(&a.digest()), "healed → quiesce");
}
