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

/// Build a state with one completed task carrying an output, spill it,
/// and return the state + the spill-file tempdir (kept alive).
fn spilled_completed_state() -> (ClusterState<RunnerIdentifier>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "done".into(),
        task: mk_task("done"),
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
    // (never-evicted) output map.
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
        });
        s.apply(ClusterMutation::TaskSkippedAlreadyDone { hash: "sk".into() });
        // A pending task — stays fat (live).
        s.apply(ClusterMutation::TaskAdded {
            hash: "p".into(),
            task: mk_task("p"),
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
    let stop = Arc::new(AtomicBool::new(false));

    // Reader thread: repeatedly walk every committed record from offset 0
    // up to the published committed offset, decoding each. A torn read
    // (reading bytes past a half-written record) would surface as a CBOR
    // decode error or a length-prefix overrun — assert neither ever
    // happens.
    let reader = {
        let read_fd = read_fd;
        let committed = Arc::clone(&committed);
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
    stop.store(true, Ordering::Release);
    let reads = reader.join().expect("reader thread");
    // The reader observed many committed records and decoded every one —
    // never a torn read.
    assert!(reads > 0, "reader must have observed committed records");
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
