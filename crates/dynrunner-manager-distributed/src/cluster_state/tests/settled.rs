//! Tests for the settled-CRDT disk spill (`cluster_state::settled`).
//!
//! Covers the owner-mandated behaviour changes:
//!   * settlement eviction — a completed task's fat body leaves memory,
//!     and the fat-body-free readers (counts, outcome, dep resolution,
//!     keyed-output, terminal view) still serve it via the slim index;
//!   * digest differential under spill — digest with spill enabled ==
//!     a fresh full fold of the logical (unspilled) state, across a
//!     mixed mutation+settle sequence;
//!   * stream-from-file byte/semantic equivalence — a joiner bootstrapped
//!     from a SPILLED responder converges to the identical ledger;
//!   * the late-duplicate NoOp + the lattice-escape rehydrate against a
//!     settled entry;
//!   * the settle predicate (retry-eligible Failed stays fat);
//!   * promotion hydrate from the settled base (no replay);
//!   * memory arithmetic — N settled entries' resident index bytes are a
//!     small fraction of the resident fat bytes.

use super::*;
use crate::cluster_state::SettledStore;
use dynrunner_core::DonePayload;

/// A non-empty `DonePayload` wire body so a completed task records real
/// outputs (the keyed-output reader path).
fn done_with_output(key: &str, value: &str) -> Vec<u8> {
    let mut m = std::collections::BTreeMap::new();
    m.insert(
        key.to_string(),
        dynrunner_core::ResultValue::Inline(value.to_string()),
    );
    let payload = DonePayload {
        outputs: dynrunner_core::TaskOutputs(m),
    };
    serde_json::to_vec(&payload).expect("encode DonePayload")
}

/// Apply a `PrimaryChanged { new, epoch }` — the failover/promotion seam
/// that fires the def-id resume floor (L6a). `reason` is apply-blind.
fn apply_primary_changed(s: &mut ClusterState<RunnerIdentifier>, new: &str, epoch: u64) {
    s.apply(ClusterMutation::PrimaryChanged {
        new: new.into(),
        epoch,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::default(),
    });
}

/// Build a state with one completed task carrying an output, spill it,
/// and return the state + the spill-file tempdir (kept alive).
fn spilled_completed_state() -> (ClusterState<RunnerIdentifier>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "done".into(),
        task: mk_task("done"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "done".into(),
        result_data: Some(done_with_output("k", "v")),
    });
    let evicted = s.test_spill_all(&dir.path().join("spill.cbor"));
    assert_eq!(evicted, 1, "the completed task must settle + evict");
    (s, dir)
}

/// A completed task's fat body leaves memory once it settles, but every
/// fat-body-free reader still serves it via the slim index.
#[test]
fn settlement_evicts_fat_body_but_lookups_serve_via_index() {
    let (s, _dir) = spilled_completed_state();

    // Fat map no longer holds it; the logical ledger + settled index do.
    assert!(
        s.task_state("done").is_none(),
        "fat body must be evicted (task_state is fat-only)"
    );
    assert!(s.settled_contains("done"), "settled index holds the entry");
    assert!(s.contains_task("done"), "logical ledger still has it");
    assert_eq!(s.tasks_in_memory(), 0, "no fat entry resident");
    assert_eq!(s.task_count(), 1, "one logical entry total");

    // counts() / outcome_counts() fold the settled class into the same
    // buckets a fat Completed would feed.
    assert_eq!(s.counts().completed, 1);
    assert_eq!(s.outcome_counts().succeeded, 1);

    // Dep resolution + keyed-output lookups serve via the index + the
    // settled-disk fallback (the output payload was EVICTED from the
    // resident map at commit-spill — `outputs_for` reads it off disk).
    let p0 = PhaseId::from("p0");
    assert_eq!(s.task_hash_for_dep(&p0, "done"), Some("done"));
    let outs = s.outputs_for(&p0, "done").expect("output served via index");
    assert!(outs.0.contains_key("k"));

    // phase rollups: the phase HAS a task and has no live work.
    let rollups = s.phase_rollups();
    let r = rollups.get(&p0).expect("phase present");
    assert!(r.has_any && !r.has_live);
}

/// digest() with spill enabled == a fresh full fold of the identical
/// logical state held fully in memory — across a mixed sequence of
/// completed / final-failed / skipped / live tasks where exactly the
/// terminal slice spills.
#[test]
fn digest_under_spill_equals_full_fold() {
    let dir = tempfile::tempdir().expect("tempdir");

    let build = |spill: bool| {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        // A completed task with output.
        s.apply(ClusterMutation::TaskAdded {
            hash: "c".into(),
            task: mk_task("c"),
            def_id: None,
        });
        s.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: "c".into(),
            result_data: Some(done_with_output("k", "v")),
        });
        // A NonRecoverable (final) failure — settles.
        s.apply(ClusterMutation::TaskAdded {
            hash: "f".into(),
            task: mk_task("f"),
            def_id: None,
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "f".into(),
            kind: dynrunner_core::ErrorType::NonRecoverable,
            error: "boom".into(),
            version: Default::default(),
            attempt: 0,
        });
        // A skipped task — settles.
        s.apply(ClusterMutation::TaskAdded {
            hash: "sk".into(),
            task: mk_task("sk"),
            def_id: None,
        });
        s.apply(ClusterMutation::TaskSkippedAlreadyDone { hash: "sk".into() });
        // A pending task — stays fat (live).
        s.apply(ClusterMutation::TaskAdded {
            hash: "p".into(),
            task: mk_task("p"),
            def_id: None,
        });
        if spill {
            let n = s.test_spill_all(&dir.path().join("spill.cbor"));
            assert_eq!(n, 3, "the three terminals settle; pending stays fat");
        }
        s
    };

    let full = build(false);
    let spilled = build(true);

    // The spilled replica still holds exactly one fat (pending) entry.
    assert_eq!(spilled.tasks_in_memory(), 1);
    assert_eq!(spilled.settled_store().len(), 3);
    // Byte-identical digest despite the eviction.
    assert_eq!(
        spilled.digest(),
        full.digest(),
        "spill must not perturb the digest fold"
    );
    // And the memoized read matches a fresh full fold of the spilled
    // state (the #480 differential extended to spill).
    assert_eq!(spilled.digest(), spilled.fresh_digest_fold());
    // #492: spill is RANGE-memo-neutral too — the incrementally-maintained
    // range memo (untouched by commit_spill) still equals a fresh fold over
    // the fat ∪ settled split, and reconstructs the scalar tasks_hash. A
    // spill that wrongly removed the term from the memo, or a fold that
    // dropped the settled half, would fail here.
    let rmemo = spilled.tasks_range_digest();
    let rfresh = spilled.fresh_tasks_range_digest();
    assert_eq!(
        (rmemo.folds, rmemo.counts),
        (rfresh.folds, rfresh.counts),
        "the range memo must equal a fresh fold across the fat/settled split \
         (spill is range-memo-neutral)"
    );
    assert_eq!(
        rmemo.folds.iter().fold(0u64, |a, f| a ^ f),
        spilled.digest().tasks_hash,
        "XOR(range memo) must reconstruct tasks_hash after a spill"
    );
}

/// A joiner bootstrapped from a SPILLED responder (its terminal slice
/// served per-key from the spill file) converges to the IDENTICAL
/// ledger as a joiner bootstrapped from the same state held fully fat —
/// the #481 convergence test extended to spill.
#[test]
fn joiner_from_spilled_responder_converges_identically() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Two equivalent donors: one fully fat, one with its terminals spilled.
    let mut fat_donor = ClusterState::<RunnerIdentifier>::new();
    for name in ["a", "b", "c"] {
        fat_donor.apply(ClusterMutation::TaskAdded {
            hash: name.into(),
            task: mk_task(name),
            def_id: None,
        });
    }
    fat_donor.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "a".into(),
        result_data: Some(done_with_output("k", "v")),
    });
    fat_donor.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "b".into(),
        result_data: None,
    });
    let mut spilled_donor = fat_donor.clone();
    let n = spilled_donor.test_spill_all(&dir.path().join("spill.cbor"));
    assert_eq!(n, 2, "the two completed tasks spill; c stays pending");

    // Bootstrap a joiner from EACH via the snapshot-stream package
    // sequence (the production responder path), then compare.
    let stream_into = |donor: &ClusterState<RunnerIdentifier>| {
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        for frame in crate::snapshot_stream::stream_frames_for_test(donor, "donor", "s1") {
            if let dynrunner_protocol_primary_secondary::DistributedMessage::SnapshotStreamPackage {
                payload,
                ..
            } = frame
            {
                let snap = crate::cluster_state::decode_stream_payload::<RunnerIdentifier>(&payload)
                    .expect("decode package");
                joiner.restore(snap);
            }
        }
        joiner
    };

    let from_fat = stream_into(&fat_donor);
    let from_spilled = stream_into(&spilled_donor);

    // Both joiners converge to the donor's logical ledger — same digest,
    // same counts, same per-task identity.
    assert_eq!(from_fat.digest(), fat_donor.digest());
    assert_eq!(
        from_spilled.digest(),
        fat_donor.digest(),
        "a joiner from the SPILLED responder converges to the identical ledger"
    );
    assert_eq!(from_spilled.counts(), from_fat.counts());
    // The completed task's output rode the package (read from the file)
    // and resolves on the joiner.
    let p0 = PhaseId::from("p0");
    assert!(from_spilled.outputs_for(&p0, "a").is_some());
}

/// A late-duplicate `TaskCompleted` for a settled hash NoOps via the
/// settled consult (no rehydrate, no double-count); a genuinely
/// dominating mutation (an `InvalidTask` over a settled `Completed` —
/// the D-T flip) REHYDRATES the fat body and applies.
#[test]
fn settled_consult_noops_duplicate_and_rehydrates_dominator() {
    let (mut s, _dir) = spilled_completed_state();

    // Late duplicate completion: NoOp, entry stays settled.
    let out = s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "done".into(),
        result_data: None,
    });
    assert_eq!(out, crate::cluster_state::ApplyOutcome::NoOp);
    assert!(s.settled_contains("done"), "duplicate must not unsettle");
    assert!(s.task_state("done").is_none());

    // A dominating InvalidTask (the unique terminal TOP) rehydrates +
    // applies — the lattice escape hatch.
    let out = s.apply(ClusterMutation::TaskFailed {
        hash: "done".into(),
        kind: dynrunner_core::ErrorType::InvalidTask {
            reason: dynrunner_core::BoundedString::from("bad".to_string()),
        },
        error: "invalid".into(),
        version: Default::default(),
        attempt: 0,
    });
    assert_eq!(out, crate::cluster_state::ApplyOutcome::Applied);
    assert!(
        !s.settled_contains("done"),
        "the dominator rehydrated the entry back to fat"
    );
    assert!(matches!(
        s.task_state("done"),
        Some(TaskState::InvalidTask { .. })
    ));
    // counts reflect the flip with no leftover settled double-count.
    assert_eq!(s.counts().completed, 0);
    assert_eq!(s.counts().invalid_task, 1);
    assert_eq!(s.task_count(), 1);
}

/// The settle predicate: a retry-eligible `Failed` (Recoverable / OOM)
/// stays FAT — a `TaskRetried` reset can still reach it.
#[test]
fn retry_eligible_failure_stays_fat() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "r".into(),
        task: mk_task("r"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "r".into(),
        kind: dynrunner_core::ErrorType::Recoverable,
        error: "retry me".into(),
        version: Default::default(),
        attempt: 0,
    });
    let evicted = s.test_spill_all(&dir.path().join("spill.cbor"));
    assert_eq!(evicted, 0, "a Recoverable failure must NOT settle");
    assert!(matches!(s.task_state("r"), Some(TaskState::Failed { .. })));
}

/// Promotion hydrate from a settled base: a freshly-promoted replica
/// adopts the donor's settled store (index + read fds) WITHOUT replaying
/// fat bodies, and the inherited terminal still serves every reader.
#[test]
fn promotion_adopts_settled_base_without_replay() {
    let (donor, _dir) = spilled_completed_state();
    let base: SettledStore = donor.settled_base_clone();

    // A fresh replica installs the base, then restores the donor's fat
    // snapshot over it (the disjoint fat half — empty here).
    let mut promoted = ClusterState::<RunnerIdentifier>::new();
    promoted.install_settled_base(base);
    promoted.restore(donor.snapshot());

    // The settled terminal is inherited (read through the SHARED file fd)
    // with no fat residency.
    assert!(promoted.settled_contains("done"));
    assert_eq!(promoted.tasks_in_memory(), 0);
    assert_eq!(promoted.counts().completed, 1);
    // The digest matches the donor's exactly — same logical ledger.
    assert_eq!(promoted.digest(), donor.digest());
    // The record reads back through the inherited fd.
    let (state, outputs) = promoted.settled_record("done").expect("record reads");
    assert!(matches!(state, TaskState::Completed { .. }));
    assert!(outputs.is_some());
}

/// Memory arithmetic: at N settled entries the resident index bytes are
/// a small fraction of the resident fat bytes — the structural win the
/// spill exists for. Uses the counting/size seam, not RSS. Tasks carry a
/// PRODUCTION-SHAPED fat payload (the opaque per-item `payload` JSON the
/// real 46.5k-task ledger carries on every `TaskInfo`) so the proxy for
/// resident fat bytes reflects the real footprint, not a near-empty stub.
#[test]
fn settled_index_bytes_far_below_fat_bytes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut s = ClusterState::<RunnerIdentifier>::new();
    const N: usize = 2000;
    // A realistic fat body: a non-trivial opaque payload + a longer path,
    // mirroring the production `TaskInfo` whose resident cost is the whole
    // point of the spill (the slim index drops ALL of it).
    let fat_task = |name: &str| {
        let mut t = mk_task(name);
        t.path = std::path::PathBuf::from(format!(
            "/very/long/staged/source/tree/path/segment/{name}/binary.tar"
        ));
        // ~2 KB opaque payload — the production `TaskInfo.payload` shape
        // (consumer metadata) the slim index does NOT retain. The real
        // 46.5k-task ledger's multi-GB footprint is exactly this fat
        // per-item body × N; the slim index keeps only id/phase/deps.
        t.payload = serde_json::json!({
            "toolchain": "clang-17-x86_64-unknown-linux-gnu",
            "flags": ["-O2", "-march=native", "-flto", "-fno-omit-frame-pointer"],
            "variant": format!("variant-of-{name}"),
            "blob": "x".repeat(2048),
        });
        t
    };
    // The fat resident footprint proxy: sum of each task's encoded size
    // (the same fields that occupy the in-memory map). Captured BEFORE the
    // spill, while the entries are still fat.
    let mut fat_bytes_resident = 0usize;
    for i in 0..N {
        let name = format!("task-{i:05}");
        let task = fat_task(&name);
        let mut scratch = Vec::new();
        ciborium::into_writer(&task, &mut scratch).expect("encode fat task");
        fat_bytes_resident += scratch.len();
        s.apply(ClusterMutation::TaskAdded {
            hash: name.clone(),
            task,
            def_id: None,
        });
        s.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: name,
            result_data: None,
        });
    }
    assert_eq!(s.tasks_in_memory(), N);

    let evicted = s.test_spill_all(&dir.path().join("spill.cbor"));
    assert_eq!(evicted, N);
    assert_eq!(s.tasks_in_memory(), 0, "every completed task evicted");

    let index_bytes = s.settled_store().approx_index_bytes();
    // The fat bodies moved to disk; the resident residue is the slim
    // index. The structural claim the spill exists for: the resident
    // index is a SMALL FRACTION of what the fat bodies cost in RAM.
    assert!(
        index_bytes * 8 < fat_bytes_resident,
        "resident index ({index_bytes} B) must be << the fat-body resident \
         footprint ({fat_bytes_resident} B) the spill moved to disk"
    );
    // Per-entry the slim index is bounded + small (no opaque payload, no
    // long path, no identifier — just id/phase/class/key/location).
    let per_entry = index_bytes / N;
    assert!(
        per_entry < 512,
        "per-entry index ({per_entry} B) must stay slim"
    );
}

/// Concurrent read-while-write hammer: a reader streaming committed
/// records (its OWN fd, positional `pread`, never reading past the
/// published committed offset) NEVER observes a torn record while the
/// writer appends batch after batch and publishes the offset only after
/// each flush. The one-writer / lock-free-reader protocol.
#[test]
fn concurrent_reader_never_sees_torn_record() {
    use std::io::Write as _;
    use std::os::unix::fs::FileExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("hammer.cbor");
    // Truncate-create, then a writer fd + a reader fd (each its own).
    std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .expect("create");
    let mut write_fd = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("write fd");
    let read_fd = std::fs::File::open(&path).expect("read fd");
    let committed = Arc::new(AtomicU64::new(0));
    // Highest committed offset the reader has fully consumed (its decode
    // cursor at the end of its latest pass). The writer waits on this to
    // know the reader has caught up to the final committed offset before
    // it stops — making the "reader observed records" liveness a
    // synchronisation handshake rather than a CPU-scheduling race.
    let read_progress = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    // Reader thread: repeatedly walk every committed record from offset 0
    // up to the published committed offset, decoding each. A torn read
    // (reading bytes past a half-written record) would surface as a CBOR
    // decode error or a length-prefix overrun — assert neither ever
    // happens.
    let reader = {
        let read_fd = read_fd;
        let committed = Arc::clone(&committed);
        let read_progress = Arc::clone(&read_progress);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut reads = 0u64;
            while !stop.load(Ordering::Acquire) {
                let limit = committed.load(Ordering::Acquire);
                let mut off = 0u64;
                while off + 4 <= limit {
                    let mut len_buf = [0u8; 4];
                    read_fd.read_exact_at(&mut len_buf, off).expect("len pread");
                    let body_len = u32::from_le_bytes(len_buf) as u64;
                    let rec_end = off + 4 + body_len;
                    // The published offset is record-aligned (it advances
                    // only by whole flushed records), so a body that would
                    // run past `limit` means we mis-parsed a length — i.e.
                    // a torn read. It must NEVER happen.
                    assert!(
                        rec_end <= limit,
                        "torn record: body claims to end at {rec_end} past committed {limit}"
                    );
                    let mut body = vec![0u8; body_len as usize];
                    read_fd.read_exact_at(&mut body, off + 4).expect("body pread");
                    let _rec: SettledRecordForTest = ciborium::from_reader(body.as_slice())
                        .expect("committed record must decode (never torn)");
                    off = rec_end;
                    reads += 1;
                }
                // Publish how far this pass consumed so the writer can
                // observe the reader catching up. (Release pairs with the
                // writer's Acquire load below.)
                read_progress.store(off, Ordering::Release);
            }
            reads
        })
    };

    // Writer: 200 batches of small records, flush + publish after each.
    let mut offset = 0u64;
    for batch in 0..200u32 {
        let mut buf = Vec::new();
        for i in 0..8u32 {
            let body = SettledRecordForTest {
                hash: format!("b{batch}-{i}"),
                marker: (batch as u64) << 8 | i as u64,
            };
            let mut body_buf = Vec::new();
            ciborium::into_writer(&body, &mut body_buf).expect("encode");
            buf.extend_from_slice(&(body_buf.len() as u32).to_le_bytes());
            buf.extend_from_slice(&body_buf);
        }
        write_fd.write_all(&buf).expect("append batch");
        write_fd.flush().expect("flush");
        offset += buf.len() as u64;
        // Publish ONLY after the flush — the reader may now reach these
        // bytes. (Release pairs with the reader's Acquire load.)
        committed.store(offset, Ordering::Release);
        // Yield so the reader interleaves mid-stream.
        std::thread::yield_now();
    }
    // Wait for the reader to consume up to the final committed offset
    // before stopping it. This guarantees the reader walked the whole
    // committed stream concurrently with the writes (so the torn-read
    // invariants were exercised), and removes the prior CPU-scheduling
    // race where a starved reader could observe nothing before stop.
    //
    // The wait is BOUNDED on a no-progress deadline: a starved reader
    // (busy host) keeps making progress and just takes longer, so the
    // deadline resets on every advance. Only a reader that is genuinely
    // STUCK — e.g. a real regression panics/stalls the reader thread
    // before it reaches the final offset — fails to advance, and then
    // this fails FAST with a clear message rather than hanging the suite.
    let no_progress_bound = std::time::Duration::from_secs(30);
    let mut last_progress = read_progress.load(Ordering::Acquire);
    let mut deadline = std::time::Instant::now() + no_progress_bound;
    while last_progress < offset {
        std::thread::yield_now();
        let now = read_progress.load(Ordering::Acquire);
        if now > last_progress {
            last_progress = now;
            deadline = std::time::Instant::now() + no_progress_bound;
        } else if std::time::Instant::now() >= deadline {
            panic!(
                "reader stalled at offset {last_progress} (final committed {offset}) for \
                 {no_progress_bound:?} — reader thread is stuck (regression?), not just slow"
            );
        }
    }
    stop.store(true, Ordering::Release);
    let reads = reader.join().expect("reader thread");
    // The reader observed many committed records and decoded every one —
    // never a torn read.
    assert!(reads > 0, "reader must have observed committed records");
}

/// The owner's hard requirement, pinned: after N completed tasks with
/// inline outputs settle/spill, the RESIDENT output map is EMPTY (the
/// O(completed-tasks) accumulation is gone), yet every output is still
/// readable via the storage-agnostic accessor (decoded off disk).
///
/// FAIL-on-trunk: pre-fix `commit_spill` never evicted `task_outputs`, so
/// the resident map held all N payloads forever.
#[test]
fn settled_outputs_leave_resident_map_but_stay_readable_via_disk() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut s = ClusterState::<RunnerIdentifier>::new();
    const N: usize = 64;
    for i in 0..N {
        let name = format!("t{i:03}");
        s.apply(ClusterMutation::TaskAdded {
            hash: name.clone(),
            task: mk_task(&name),
            def_id: None,
        });
        s.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: name.clone(),
            result_data: Some(done_with_output("k", &format!("v{i}"))),
        });
    }
    // Pre-spill: every payload is resident.
    assert_eq!(s.task_outputs_resident_len(), N, "all N outputs resident pre-spill");

    let evicted = s.test_spill_all(&dir.path().join("spill.cbor"));
    assert_eq!(evicted, N, "all N completed tasks settle");

    // The hard requirement: ZERO resident output payloads — the
    // accumulation the bug created is gone.
    assert_eq!(
        s.task_outputs_resident_len(),
        0,
        "settled outputs must leave the resident map (zero accumulation)"
    );
    // …yet the LOGICAL output count is intact (resident ∪ settled) and
    // every output reads back via the accessor (decoded off disk).
    assert_eq!(s.digest().task_outputs_count, N as u64, "logical count unchanged");
    let p0 = PhaseId::from("p0");
    for i in 0..N {
        let outs = s
            .outputs_for(&p0, &format!("t{i:03}"))
            .expect("settled output readable from disk");
        assert_eq!(
            outs.0.get("k"),
            Some(&dynrunner_core::ResultValue::Inline(format!("v{i}")))
        );
    }
}

/// `phase_task_outputs` (the `on_phase_end` hook's reader) serves a
/// SPILLED phase's outputs from disk after eviction — the brief's test
/// #3.
///
/// FAIL-on-trunk: pre-fix the settled branch read the resident map, which
/// would be fine pre-eviction; post-eviction it would return an empty map
/// without the disk fallback.
#[test]
fn phase_task_outputs_serves_spilled_phase_from_disk() {
    let (s, _dir) = spilled_completed_state();
    let p0 = PhaseId::from("p0");
    // Resident map evicted, but the phase gather still finds the output.
    assert_eq!(s.task_outputs_resident_len(), 0, "evicted at settle");
    let gathered = s.phase_task_outputs(&p0);
    let outs = gathered.get("done").expect("phase output present (from disk)");
    assert_eq!(
        outs.0.get("k"),
        Some(&dynrunner_core::ResultValue::Inline("v".to_string()))
    );
}

/// `unsettle_if_dominated` REHYDRATES a settled entry's outputs from the
/// spill record — they are NOT silently dropped — the brief's test #4.
/// A dominating `InvalidTask` over a settled `Completed { outputs }`
/// un-settles; the rehydrated entry's output must be retrievable, and the
/// digest's logical output term stays coherent across the round-trip.
///
/// FAIL-on-trunk is N/A (the eviction is new); this pins that the new
/// eviction's INVERSE faithfully restores the evicted payload — a fix
/// that evicted but forgot to rehydrate would drop the output here.
#[test]
fn unsettle_rehydrates_evicted_outputs() {
    let (mut s, _dir) = spilled_completed_state();
    let p0 = PhaseId::from("p0");
    assert_eq!(s.task_outputs_resident_len(), 0, "evicted at settle");
    // Logical output present (settled half).
    assert_eq!(s.digest().task_outputs_count, 1);

    // A dominating mutation that does NOT itself carry the prior output:
    // a same-attempt completion is a NoOp, so drive the lattice escape
    // with an InvalidTask (the unique terminal TOP, D-T flip) — it
    // rehydrates the settled Completed first. The rehydrated state then
    // loses the dominance compare? No: InvalidTask dominates Completed, so
    // after rehydrate the slot becomes InvalidTask. The OUTPUT, however,
    // was reinstated into the resident map by the rehydrate BEFORE the
    // overwrite — so it must survive the rehydrate moment. We assert the
    // rehydrate-time reinstatement directly via the lower-level path: a
    // RECOVERABLE-failure dominator would keep it fat without erasing
    // outputs, but Completed→Recoverable does not dominate. Use the
    // public observable: after the InvalidTask flip, the resident map
    // re-held the output during rehydrate, then the slot flipped; the
    // output entry (first-write-wins, never cleared) remains resident.
    let out = s.apply(ClusterMutation::TaskFailed {
        hash: "done".into(),
        kind: dynrunner_core::ErrorType::InvalidTask {
            reason: dynrunner_core::BoundedString::from("bad".to_string()),
        },
        error: "invalid".into(),
        version: Default::default(),
        attempt: 0,
    });
    assert_eq!(out, crate::cluster_state::ApplyOutcome::Applied);
    assert!(!s.settled_contains("done"), "dominator un-settled the entry");
    // The evicted output was rehydrated into the resident map (NOT dropped).
    assert_eq!(
        s.task_outputs_resident_len(),
        1,
        "unsettle must rehydrate the evicted output payload"
    );
    let outs = s.outputs_for(&p0, "done").expect("rehydrated output readable");
    assert_eq!(
        outs.0.get("k"),
        Some(&dynrunner_core::ResultValue::Inline("v".to_string()))
    );
    // Digest output term stays coherent (one logical output, now resident).
    assert_eq!(s.digest().task_outputs_count, 1);
    assert_eq!(s.digest(), s.fresh_digest_fold());
}

/// Replication round-trips a SPILLED responder's evicted outputs into a
/// joiner (per-key off the spill file, via the snapshot-stream packages
/// — the production bootstrap path; `snapshot()` carries FAT entries
/// only), and a re-stream onto a replica that has ALREADY settled the
/// same keys does NOT re-bloat the resident map or double-count the
/// digest — the brief's test #5.
#[test]
fn stream_round_trips_spilled_outputs_no_rebloat() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Donor: two completed-with-output tasks, both spilled (outputs on disk).
    let mut donor = ClusterState::<RunnerIdentifier>::new();
    for (h, v) in [("a", "va"), ("b", "vb")] {
        donor.apply(ClusterMutation::TaskAdded {
            hash: h.into(),
            task: mk_task(h),
            def_id: None,
        });
        donor.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: h.into(),
            result_data: Some(done_with_output("k", v)),
        });
    }
    let n = donor.test_spill_all(&dir.path().join("donor.cbor"));
    assert_eq!(n, 2);
    assert_eq!(donor.task_outputs_resident_len(), 0, "donor evicted");

    // Bootstrap a fresh joiner from the donor via the snapshot-stream
    // package sequence — the settled outputs are decoded per-key off the
    // donor's spill file and ride each task batch (the production path).
    let stream_into = |joiner: &mut ClusterState<RunnerIdentifier>| {
        for frame in crate::snapshot_stream::stream_frames_for_test(&donor, "donor", "s1") {
            if let dynrunner_protocol_primary_secondary::DistributedMessage::SnapshotStreamPackage {
                payload,
                ..
            } = frame
            {
                let snap = crate::cluster_state::decode_stream_payload::<RunnerIdentifier>(&payload)
                    .expect("decode package");
                joiner.restore(snap);
            }
        }
    };

    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    stream_into(&mut joiner);
    let p0 = PhaseId::from("p0");
    for (h, v) in [("a", "va"), ("b", "vb")] {
        let outs = joiner.outputs_for(&p0, h).expect("output round-tripped");
        assert_eq!(
            outs.0.get("k"),
            Some(&dynrunner_core::ResultValue::Inline(v.to_string()))
        );
    }
    assert_eq!(joiner.digest(), donor.digest(), "joiner converges to donor");

    // No-rebloat: stream the donor onto a replica that has ALREADY settled
    // the same keys (its outputs are on its OWN disk). The restore
    // output-merge must SKIP the settled keys — no resident re-bloat, no
    // digest double-count.
    let mut already_settled = ClusterState::<RunnerIdentifier>::new();
    for (h, v) in [("a", "va"), ("b", "vb")] {
        already_settled.apply(ClusterMutation::TaskAdded {
            hash: h.into(),
            task: mk_task(h),
            def_id: None,
        });
        already_settled.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: h.into(),
            result_data: Some(done_with_output("k", v)),
        });
    }
    let n2 = already_settled.test_spill_all(&dir.path().join("settled.cbor"));
    assert_eq!(n2, 2);
    assert_eq!(already_settled.task_outputs_resident_len(), 0);
    let before = already_settled.digest();
    stream_into(&mut already_settled);
    assert_eq!(
        already_settled.task_outputs_resident_len(),
        0,
        "restore must NOT re-bloat the resident map for already-settled keys"
    );
    assert_eq!(
        already_settled.digest().task_outputs_count,
        2,
        "no output double-count after restoring onto a settled replica"
    );
    assert_eq!(
        before, already_settled.digest(),
        "restoring an equal stream onto a settled replica is digest-neutral"
    );
}

/// A minimal length-prefixed CBOR record mirroring the spill framing,
/// used by the concurrency hammer (the production `SettledRecord` is
/// generic over `I` + carries a `TaskState`; this test only needs the
/// framing + decode-or-fail behaviour).
#[derive(serde::Serialize, serde::Deserialize)]
struct SettledRecordForTest {
    hash: String,
    marker: u64,
}

/// Writer-failure degrade: an injected IO error on the batch write
/// leaves every entry FAT (never evicted), advances no committed offset,
/// and surfaces the error — the run continues fat-but-correct. Exercises
/// `write_spill_batch`'s error path directly (the driver's
/// `complete`-on-`Err` arm latches `degraded`).
#[test]
fn writer_failure_keeps_entries_fat() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "c".into(),
        task: mk_task("c"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "c".into(),
        result_data: None,
    });
    // Collect a real batch (the entry is settle-eligible). We must attach
    // a segment so `collect_spill_batch` yields candidates.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("degrade.cbor");
    std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .expect("create");
    let read_fd = std::fs::File::open(&path).expect("read fd");
    let committed = std::sync::Arc::new(AtomicU64::new(0));
    let segment = s.attach_spill_segment(std::sync::Arc::new(read_fd), std::sync::Arc::clone(&committed));
    let batch = s.collect_spill_batch(usize::MAX);
    assert_eq!(batch.len(), 1);

    // Inject the IO error: a READ-ONLY file fd fails on write_all.
    let mut ro_fd = std::fs::File::open(&path).expect("ro fd");
    let result = crate::cluster_state::write_spill_batch(&mut ro_fd, 0, &committed, segment, batch);
    assert!(result.is_err(), "writing to a read-only fd must fail");

    // No commit applied → the entry is STILL fat, the committed offset is
    // still 0, and the ledger is intact (fat-but-correct).
    assert_eq!(committed.load(Ordering::Acquire), 0);
    assert!(
        matches!(s.task_state("c"), Some(TaskState::Completed { .. })),
        "the entry must remain fat after a failed write"
    );
    assert!(!s.settled_contains("c"));
    assert_eq!(s.counts().completed, 1);
}

/// `fat_task_breakdown` honestly splits the fat (in-memory, un-spilled)
/// task map by the SAME `settle_eligible` predicate the spiller gates on:
/// settle-eligible terminals (a Completed held in memory — spill-lag) vs.
/// non-terminal live work (Pending + InFlight — a liveness backlog). The
/// two parts must sum to `tasks_in_memory()`.
#[test]
fn fat_task_breakdown_splits_eligible_from_live() {
    let mut s = ClusterState::<RunnerIdentifier>::new();

    // One settle-ELIGIBLE entry held FAT: Completed but never spilled, so
    // its fat body is still resident in `self.tasks`.
    s.apply(ClusterMutation::TaskAdded {
        hash: "done".into(),
        task: mk_task("done"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "done".into(),
        result_data: None,
    });

    // One NON-terminal Pending entry (TaskAdded leaves it Pending).
    s.apply(ClusterMutation::TaskAdded {
        hash: "pending".into(),
        task: mk_task("pending"),
        def_id: None,
    });

    // One NON-terminal InFlight entry (TaskAssigned drives Pending -> InFlight).
    s.apply(ClusterMutation::TaskAdded {
        hash: "inflight".into(),
        task: mk_task("inflight"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskAssigned {
        attempt: 0,
        hash: "inflight".into(),
        secondary: "s1".into(),
        worker: 0,
        version: Default::default(),
    });

    // Sanity: all three are resident fat (no spill ran).
    assert_eq!(s.tasks_in_memory(), 3, "all three entries are fat");
    assert!(matches!(
        s.task_state("done"),
        Some(TaskState::Completed { .. })
    ));
    assert!(matches!(
        s.task_state("pending"),
        Some(TaskState::Pending { .. })
    ));
    assert!(matches!(
        s.task_state("inflight"),
        Some(TaskState::InFlight { .. })
    ));

    let (eligible, not_eligible) = s.fat_task_breakdown();
    assert_eq!(eligible, 1, "only the Completed terminal is settle-eligible");
    assert_eq!(not_eligible, 2, "Pending + InFlight are the live backlog");
    assert_eq!(
        eligible + not_eligible,
        s.tasks_in_memory(),
        "the split must sum to the fat total"
    );
}

// ── L6a: failover-stable def-ids over (in-memory ∪ settled) ──

/// Build a donor with one task carrying the WIRE def-id `def_id`, complete
/// + spill it so its SETTLED record carries that real id. Returns the donor
/// + the spill-file tempdir (kept alive for the shared read fd).
fn donor_with_settled_wire_def(
    hash: &str,
    def_id: u32,
) -> (ClusterState<RunnerIdentifier>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut donor = ClusterState::<RunnerIdentifier>::new();
    // A WIRE-AGREED def-id (`Some`) routes through `intern_at`, which STAMPS
    // it onto the def — so the settled record (and its slim index entry)
    // carries the real id, not the node-local `UNBOUND` sentinel.
    donor.apply(ClusterMutation::TaskAdded {
        hash: hash.into(),
        task: mk_task(hash),
        def_id: Some(def_id),
    });
    donor.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: hash.into(),
        result_data: Some(done_with_output("k", "v")),
    });
    let evicted = donor.test_spill_all(&dir.path().join("spill.cbor"));
    assert_eq!(evicted, 1, "the completed task must settle + evict");
    (donor, dir)
}

/// A settled record's WIRE def-id is captured into its slim index entry at
/// commit-spill (no disk read) and surfaced by `SettledStore::max_def_id`;
/// a node-local (`UNBOUND`) settled def is EXCLUDED from the max.
#[test]
fn settled_store_surfaces_wire_def_id_and_excludes_unbound() {
    // Wire id 7 → counted.
    let (donor, _dir) = donor_with_settled_wire_def("done", 7);
    assert_eq!(
        donor.settled_store().max_def_id(),
        Some(7),
        "the settled record's stamped wire def-id is surfaced"
    );

    // A node-local (`def_id: None`) settled def carries `UNBOUND` and is
    // excluded — folding the `u32::MAX` sentinel would slam the allocator
    // to the top of the id space.
    let (node_local, _dir2) = spilled_completed_state();
    assert_eq!(
        node_local.settled_store().max_def_id(),
        None,
        "an UNBOUND (node-local) settled def is excluded from the max"
    );
}

/// THE leaf invariant: a promoted primary that inherits tasks — some
/// IN-MEMORY (snapshot-restored, so their def-ids re-anchor the in-memory
/// store) and some SETTLED (inherited via the settled base, NOT in the
/// snapshot's def store) — resumes its def-id allocator PAST the max of
/// BOTH halves, so a newly-minted def-id collides with NEITHER.
#[test]
fn promotion_resumes_def_alloc_past_settled_and_in_memory_max() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Donor: a SETTLED task at wire id 7 (the high inherited id lives only
    // on the settled side) + an IN-MEMORY task at wire id 3 (a smaller id
    // that re-anchors the in-memory store on restore).
    let mut donor = ClusterState::<RunnerIdentifier>::new();
    donor.apply(ClusterMutation::TaskAdded {
        hash: "settled".into(),
        task: mk_task("settled"),
        def_id: Some(7),
    });
    donor.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "settled".into(),
        result_data: Some(done_with_output("k", "v")),
    });
    donor.apply(ClusterMutation::TaskAdded {
        hash: "live".into(),
        task: mk_task("live"),
        def_id: Some(3),
    });
    let evicted = donor.test_spill_all(&dir.path().join("spill.cbor"));
    assert_eq!(evicted, 1, "only the completed 'settled' task spills");
    assert_eq!(donor.settled_store().max_def_id(), Some(7));

    // Promote: install the settled base, then restore the disjoint fat
    // snapshot over it (the live 'live' task at id 3).
    let base = donor.settled_base_clone();
    let mut promoted = ClusterState::<RunnerIdentifier>::new();
    promoted.install_settled_base(base);
    promoted.restore(donor.snapshot());

    // BEFORE the promotion seam: the in-memory store re-anchored only the
    // RESTORED def (id 3 → floor 4); the SETTLED id 7 is INVISIBLE to the
    // in-memory store (the snapshot ships defs by value, but a settled
    // entry's fat body — and its def — is NOT in the snapshot's task
    // batch). This is the exact gap L6a closes.
    assert_eq!(
        promoted.def_alloc_floor_for_test(),
        4,
        "pre-resume floor reflects only the restored in-memory max"
    );
    assert!(
        promoted
            .resolve_def_for_test(crate::cluster_state::TaskDefId(7))
            .is_none(),
        "the settled def-id is not in the in-memory store — the gap"
    );

    // The promotion seam: a freshly-promoted primary originates
    // `PrimaryChanged { new = self, epoch + 1 }`, whose apply fires the
    // def-id resume floor over (in-memory ∪ settled).
    apply_primary_changed(&mut promoted, "promoted-self", 1);

    // AFTER: the allocator resumed PAST max(in-memory 3, settled 7) ⇒ 8.
    assert_eq!(
        promoted.def_alloc_floor_for_test(),
        8,
        "the resume floor includes the settled id (7) + 1"
    );

    // A newly-allocated def-id does NOT collide with the settled id 7 nor
    // the in-memory id 3 — it mints 8.
    let fresh = promoted.allocate_def_id("brand-new");
    assert_eq!(
        fresh,
        crate::cluster_state::TaskDefId(8),
        "the next def-id is past every inherited id (in-memory AND settled)"
    );
}

/// Cross-epoch (producer_backstop shape): two primary epochs minting from
/// INDEPENDENT allocators must not produce a LIVE collision. Epoch-1
/// settled id 5 is inherited by an epoch-2 promoted primary, whose resume
/// floor re-anchors past it — so the epoch-2 allocator never re-mints id 5
/// for a different task, and the would-be cross-epoch collision is gone.
#[test]
fn cross_epoch_promotion_does_not_reuse_settled_def_id() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Epoch-1 primary: settles a task at wire id 5, then a separate epoch-2
    // primary is promoted off its inherited state.
    let mut epoch1 = ClusterState::<RunnerIdentifier>::new();
    apply_primary_changed(&mut epoch1, "epoch1-primary", 1);
    epoch1.apply(ClusterMutation::TaskAdded {
        hash: "e1-settled".into(),
        task: mk_task("e1-settled"),
        def_id: Some(5),
    });
    epoch1.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "e1-settled".into(),
        result_data: Some(done_with_output("k", "v")),
    });
    let evicted = epoch1.test_spill_all(&dir.path().join("spill.cbor"));
    assert_eq!(evicted, 1);
    assert_eq!(epoch1.settled_store().max_def_id(), Some(5));

    // Epoch-2 promoted primary inherits the settled base + snapshot, then
    // asserts authority at epoch 2 — the resume floor re-anchors past the
    // inherited settled id 5.
    let mut epoch2 = ClusterState::<RunnerIdentifier>::new();
    epoch2.install_settled_base(epoch1.settled_base_clone());
    epoch2.restore(epoch1.snapshot());
    apply_primary_changed(&mut epoch2, "epoch2-primary", 2);

    // The epoch-2 allocator mints id 6 next — it does NOT alias the epoch-1
    // settled id 5. So even though the two epochs minted from independent
    // allocators, the inherited-max resume forecloses the live collision.
    let fresh = epoch2.allocate_def_id("e2-new");
    assert_eq!(
        fresh,
        crate::cluster_state::TaskDefId(6),
        "epoch-2's allocator resumed past the epoch-1 settled id (no reuse)"
    );
    assert_ne!(
        fresh,
        crate::cluster_state::TaskDefId(5),
        "the cross-epoch collision on the settled id is foreclosed"
    );
}
