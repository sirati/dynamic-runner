//! Tests for the serialize-once snapshot-JSON cache (#367's CPU half)
//! and the production-shaped oversize-ledger restore chain.
//!
//! The wire half of #367 (an oversize `ClusterSnapshot` frame crossing
//! real QUIC/WSS legs as chunked transfer, byte-identical) is pinned in
//! `dynrunner-transport-quic`'s `oversize_snapshot_chunking` tests;
//! here we pin (a) that the responder edge serializes ONCE per state
//! generation and invalidates on any digest-covered change, and (b)
//! that a production-shaped >cap ledger's `snapshot_json()` bytes
//! restore a fresh joiner to FULL CRDT equality (digest equality) —
//! stitching the chain: the bytes the transport delivers verbatim are
//! exactly the bytes that converge the requester.

use super::*;

/// A production-shaped task: the asm-dataset 67k-task ledger averaged
/// ~1.5 KB of serialized `TaskState` per task (≈100–116 MB / 67k), so
/// the synthetic payload pads each task to roughly that footprint.
fn mk_production_shaped_task(i: usize) -> TaskInfo<RunnerIdentifier> {
    let name = format!("crate-{i:06}-extraction-unit");
    let mut t = mk_task(&name);
    // ~1.2 KB payload + path/id/phase strings lands the per-entry
    // serialized size near the production ~1.5 KB/task shape.
    t.payload = serde_json::json!({
        "source": format!("/network/corpus/shard-{:03}/{}.zip", i % 512, name),
        "blob": "x".repeat(1100),
        "index": i,
    });
    t
}

/// Serialize-once: two reads at the same state generation return the
/// SAME allocation; any digest-covered mutation invalidates the cache
/// and the refreshed bytes reflect the new state.
#[test]
fn snapshot_json_serializes_once_per_generation() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for i in 0..32 {
        s.apply(ClusterMutation::TaskAdded {
            hash: format!("t{i}"),
            task: mk_task(&format!("t{i}")),
        });
    }
    let first = s.snapshot_json().unwrap();
    let second = s.snapshot_json().unwrap();
    assert!(
        Arc::ptr_eq(&first, &second),
        "an unchanged ledger must serve the cached serialization"
    );

    // A digest-covered change (a task completing) must invalidate.
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "t0".into(),
        result_data: None,
    });
    let third = s.snapshot_json().unwrap();
    assert!(
        !Arc::ptr_eq(&second, &third),
        "a digest-covered mutation must invalidate the cache"
    );
    // The refreshed bytes restore the NEW fact.
    let snap: ClusterStateSnapshot<RunnerIdentifier> = serde_json::from_str(&third).unwrap();
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(snap);
    assert!(matches!(
        joiner.task_state("t0"),
        Some(TaskState::Completed { .. })
    ));
    // And the cache re-stabilises at the new generation.
    let fourth = s.snapshot_json().unwrap();
    assert!(Arc::ptr_eq(&third, &fourth));
}

/// Production-shape chain: a ledger big enough that its snapshot JSON
/// exceeds the 96 MiB wire cap (the run_20260611_115429 trigger)
/// serializes through the cache, reports the per-task size datum, and
/// its bytes restore a fresh joiner to FULL CRDT equality (digest
/// equality — the same comparison anti-entropy quiesces on).
#[test]
fn oversize_production_shaped_ledger_restores_to_digest_equality() {
    const TASKS: usize = 70_000;
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for i in 0..TASKS {
        s.apply(ClusterMutation::TaskAdded {
            hash: format!("crate-{i:06}"),
            task: mk_production_shaped_task(i),
        });
    }
    // Sprinkle non-Pending states so the equality covers the lattice,
    // not just the bulk-load shape.
    for i in 0..1000 {
        s.apply(ClusterMutation::TaskAssigned {
            attempt: 0,
            hash: format!("crate-{i:06}"),
            secondary: "s1".into(),
            worker: (i % 24) as u32,
            version: Default::default(),
        });
    }
    for i in 0..500 {
        s.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: format!("crate-{i:06}"),
            result_data: None,
        });
    }

    let json = s.snapshot_json().unwrap();
    let per_task = json.len() / TASKS;
    println!(
        "snapshot_json: {} bytes over {TASKS} tasks = {per_task} bytes/task",
        json.len()
    );
    assert!(
        json.len() > dynrunner_transport_quic::MAX_WIRE_FRAME_BYTES,
        "the ledger must exceed the wire cap to model the production trigger \
         (got {} bytes; raise the payload pad if the wire shape shrank)",
        json.len()
    );

    let snap: ClusterStateSnapshot<RunnerIdentifier> = serde_json::from_str(&json).unwrap();
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(snap);
    assert_eq!(
        joiner.digest(),
        s.digest(),
        "the restored replica must be digest-equal to the responder \
         (anti-entropy quiesces on exactly this comparison)"
    );
    assert_eq!(joiner.task_count(), s.task_count());
}
