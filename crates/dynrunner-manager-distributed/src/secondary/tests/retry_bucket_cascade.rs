//! Per-phase retry-bucket cascade tests for the promoted-secondary's
//! primary path.
//!
//! Pins the symmetric Recoverable + OOM bucket behaviour the live
//! primary owns at the phase-drain edge (see
//! `crates/dynrunner-manager-distributed/src/primary/retry_bucket.rs`
//! and the live-primary test in `primary/tests/retry.rs`):
//!
//!   1. A Recoverable failure observed via `note_primary_item_failed`
//!      lands in `primary_failed`, fires the cascade, and the bucket
//!      re-injects the binary into `primary_pending` (using the
//!      Recoverable budget).
//!   2. A `ResourceExhausted(memory)` failure lands in
//!      `primary_failed` and the OOM bucket re-injects it (using the
//!      OOM budget).
//!   3. LMU regression: with `oom_retry_max_passes = 0` an OOM
//!      failure does NOT re-inject; the phase advances to Done and
//!      `on_phase_end` fires with the per-class counts.
//!   4. `NonRecoverable` failures stay in `primary_failed` (no
//!      bucket matches them) and the phase advances to Done.
//!   5. Demoted-then-promoted scenario: a secondary that inherits a
//!      pre-populated `primary_failed` ledger (with both Recoverable
//!      and OOM entries) drives both buckets through the cascade on
//!      the first promoted drain.
//!
//! Test fixture mirrors `phase_lifecycle_callback.rs`: a
//! hand-built `PendingPool` + `primary_in_flight` entry, no tokio
//! `select!`, no wire messages — the goal is to pin the cascade's
//! retry-bucket semantics, not the full operational loop.
//!
//! # Non-duplication invariant
//!
//! Both this file's secondary tests and the live-primary tests in
//! `primary/tests/retry.rs` exercise the SAME core
//! ([`crate::primary::retry_bucket::try_phase_retry_bucket_core`]).
//! The two callers differ only in:
//!
//!   - Candidate-build (`primary_failed` walk vs. `all_binaries`
//!     cross-reference against `failed_tasks`).
//!   - Kickstart (`repoll_idle_workers` vs.
//!     `dispatch_to_idle_workers`).
//!
//! The partition predicate, the per-(phase, bucket) counter
//! semantics, the budget check, and the pool-reinject step are
//! single-source. A future PR that re-introduces a parallel
//! partition or counter recipe on either side regresses CLAUDE.md's
//! "no duplicated logic" rule; the test in (3) below pins the
//! key behavioural surface (OOM-budget-zero with a residual
//! failure still advancing the phase) which is the exact LMU
//! regression scenario the consumer hit.

#![cfg(test)]

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use dynrunner_core::{
    ErrorType, PhaseId, ResourceKind, SoftPreferredSecondaries, TaskInfo, TypeId,
};
use dynrunner_scheduler_api::{PendingPool, PhaseState};

use crate::primary::wire::compute_task_hash;

use super::super::test_helpers::{election_config, make_secondary, TestId};
use super::super::{FailedTaskEntry, PrimaryInFlightItem};

fn make_binary(name: &str, phase: &str) -> TaskInfo<TestId> {
    TaskInfo {
        path: PathBuf::from(format!("/tmp/{name}")),
        size: 100,
        identifier: TestId(name.into()),
        phase_id: PhaseId::from(phase),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: format!("task-{name}"),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    }
}

fn make_in_flight(name: &str, phase: &str) -> PrimaryInFlightItem<TestId> {
    PrimaryInFlightItem {
        phase_id: PhaseId::from(phase),
        target_secondary_id: "self".into(),
        binary: make_binary(name, phase),
    }
}

/// Build a one-phase pool with N in-flight items so `on_item_finished`
/// can decrement the phase down to 0.
fn one_phase_pool_with_items(phase: &PhaseId, names: &[&str]) -> PendingPool<TestId> {
    let mut phase_ids = HashSet::new();
    phase_ids.insert(phase.clone());
    let mut pool = PendingPool::<TestId>::new(phase_ids, HashMap::new())
        .expect("graph valid");
    let in_flight: Vec<(String, PhaseId)> = names
        .iter()
        .map(|n| (format!("task-{n}"), phase.clone()))
        .collect();
    pool.mark_tasks_in_flight(in_flight);
    pool
}

/// (1) Recoverable failure at phase-drain edge: the per-phase
/// Recoverable bucket re-injects the failed task; `on_phase_end`
/// does NOT fire (the bucket flipped the phase Drained → Active).
/// Asserts the bucket-pass counter ticked to 1 for
/// `(phase, Recoverable)` and the binary is back in the pool.
#[tokio::test(flavor = "current_thread")]
async fn recoverable_failure_drives_bucket_reinjection_at_phase_drain_edge() {
    let phase = PhaseId::from("phase-a");
    let mut cfg = election_config("sec-0");
    cfg.retry_max_passes = 1;
    cfg.oom_retry_max_passes = 0;
    let mut sec = make_secondary(cfg);
    sec.primary_pending = Some(one_phase_pool_with_items(&phase, &["a"]));
    // The retry-bucket uses `compute_task_hash` to key the
    // ledger; prime `primary_in_flight` under the canonical hash
    // so `note_primary_item_failed` → `primary_failed` insert
    // → bucket-side `primary_failed.remove` form a closed key
    // loop. Mirrors the production wire shape (the worker
    // reports a TaskFailed carrying the same canonical hash).
    let in_flight_a = make_in_flight("a", "phase-a");
    let hash_a = compute_task_hash(&in_flight_a.binary);
    sec.primary_in_flight.insert(hash_a.clone(), in_flight_a);

    let calls: Arc<Mutex<Vec<(String, u32, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_inner = calls.clone();
    sec.register_phase_lifecycle_callbacks(
        Box::new(|_: &PhaseId| {}),
        Box::new(move |p: &PhaseId, c: u32, f: u32| {
            calls_inner
                .lock()
                .expect("poisoned")
                .push((p.as_str().to_string(), c, f));
        }),
    );

    sec.note_primary_item_failed(&hash_a, &ErrorType::Recoverable, &mut None)
        .await;

    // The bucket re-injected — `on_phase_end` must NOT have fired.
    let recorded = calls.lock().expect("poisoned");
    assert!(
        recorded.is_empty(),
        "Recoverable bucket re-inject must suppress on_phase_end on this drain edge"
    );
    // Counter ticked.
    assert_eq!(
        sec.primary_retry_passes_used_for_test(),
        1,
        "Recoverable bucket consumed one pass for this phase"
    );
    // Ledger drained — the bucket removed the entry after reinject.
    assert_eq!(
        sec.primary_failed_count_for_test(),
        0,
        "successful reinject drains the failed ledger"
    );
    // Pool flipped back to Active for this phase via reinject.
    assert_eq!(
        sec.primary_pending
            .as_ref()
            .expect("pool present")
            .phase_state(&phase),
        Some(PhaseState::Active),
        "reinject flips the phase Drained → Active"
    );
}

/// (2) OOM failure at phase-drain edge: with
/// `oom_retry_max_passes = 1`, the OOM bucket re-injects the
/// `ResourceExhausted(memory)` failure. Mirrors test (1) for the
/// OOM channel.
#[tokio::test(flavor = "current_thread")]
async fn oom_failure_drives_oom_bucket_reinjection_at_phase_drain_edge() {
    let phase = PhaseId::from("phase-a");
    let mut cfg = election_config("sec-0");
    cfg.retry_max_passes = 0;
    cfg.oom_retry_max_passes = 1;
    let mut sec = make_secondary(cfg);
    sec.primary_pending = Some(one_phase_pool_with_items(&phase, &["a"]));
    let in_flight_a = make_in_flight("a", "phase-a");
    let hash_a = compute_task_hash(&in_flight_a.binary);
    sec.primary_in_flight.insert(hash_a.clone(), in_flight_a);

    let calls: Arc<Mutex<Vec<(String, u32, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_inner = calls.clone();
    sec.register_phase_lifecycle_callbacks(
        Box::new(|_: &PhaseId| {}),
        Box::new(move |p: &PhaseId, c: u32, f: u32| {
            calls_inner
                .lock()
                .expect("poisoned")
                .push((p.as_str().to_string(), c, f));
        }),
    );

    sec.note_primary_item_failed(
        &hash_a,
        &ErrorType::ResourceExhausted(ResourceKind::memory()),
        &mut None,
    )
    .await;

    assert!(
        calls.lock().expect("poisoned").is_empty(),
        "OOM bucket re-inject must suppress on_phase_end"
    );
    assert_eq!(
        sec.primary_retry_passes_used_for_test(),
        1,
        "OOM bucket consumed one pass for this phase"
    );
    assert_eq!(
        sec.primary_failed_count_for_test(),
        0,
        "successful OOM reinject drains the failed ledger"
    );
}

/// (3) LMU regression on the secondary path. Mirrors the live-
/// primary's `oom_failure_with_zero_retries_still_advances_phase`:
/// a single OOM failure with `oom_retry_max_passes = 0` does NOT
/// wedge the phase. The Recoverable bucket finds nothing, the OOM
/// bucket finds a candidate but the budget is exhausted at 0/0, the
/// cascade falls through to `on_phase_end`, and the phase advances
/// to Done.
///
/// Pre-refactor (legacy `primary_drain_check_and_retry`) this case
/// was impossible to express because OOM never landed in
/// `primary_failed` to begin with (only Recoverable was stored).
/// Post-refactor the bucket-budget-zero gate makes it explicit:
/// failures that match no bucket OR fail the budget pass through
/// to phase-advance.
#[tokio::test(flavor = "current_thread")]
async fn oom_failure_with_zero_retries_advances_phase_on_secondary() {
    let phase = PhaseId::from("phase-a");
    let mut cfg = election_config("sec-0");
    cfg.retry_max_passes = 0;
    cfg.oom_retry_max_passes = 0;
    let mut sec = make_secondary(cfg);
    sec.primary_pending = Some(one_phase_pool_with_items(&phase, &["a"]));
    let in_flight_a = make_in_flight("a", "phase-a");
    let hash_a = compute_task_hash(&in_flight_a.binary);
    sec.primary_in_flight.insert(hash_a.clone(), in_flight_a);

    let calls: Arc<Mutex<Vec<(String, u32, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_inner = calls.clone();
    sec.register_phase_lifecycle_callbacks(
        Box::new(|_: &PhaseId| {}),
        Box::new(move |p: &PhaseId, c: u32, f: u32| {
            calls_inner
                .lock()
                .expect("poisoned")
                .push((p.as_str().to_string(), c, f));
        }),
    );

    sec.note_primary_item_failed(
        &hash_a,
        &ErrorType::ResourceExhausted(ResourceKind::memory()),
        &mut None,
    )
    .await;

    let recorded = calls.lock().expect("poisoned");
    assert_eq!(
        recorded.len(),
        1,
        "OOM budget = 0 must still advance the phase; got {recorded:?}"
    );
    assert_eq!(
        recorded[0],
        ("phase-a".to_string(), 0, 1),
        "on_phase_end receives (completed=0, failed=1)"
    );
    assert_eq!(
        sec.primary_pending
            .as_ref()
            .expect("pool present")
            .phase_state(&phase),
        Some(PhaseState::Done),
        "phase advances to Done despite the residual OOM failure"
    );
    // The failed entry SURVIVES — it's terminal now.
    assert_eq!(
        sec.primary_failed_count_for_test(),
        1,
        "OOM failure stays in primary_failed as fail_final after budget exhaustion"
    );
}

/// (4) `NonRecoverable` failures: no bucket matches them, so they
/// never re-inject; phase advances directly.
#[tokio::test(flavor = "current_thread")]
async fn nonrecoverable_failure_advances_phase_directly() {
    let phase = PhaseId::from("phase-a");
    let mut cfg = election_config("sec-0");
    // Even with the budgets non-zero, NonRecoverable matches
    // neither bucket — partition predicate excludes it. The
    // cascade must fall through to `on_phase_end` on the first
    // drain edge.
    cfg.retry_max_passes = 5;
    cfg.oom_retry_max_passes = 5;
    let mut sec = make_secondary(cfg);
    sec.primary_pending = Some(one_phase_pool_with_items(&phase, &["a"]));
    let in_flight_a = make_in_flight("a", "phase-a");
    let hash_a = compute_task_hash(&in_flight_a.binary);
    sec.primary_in_flight.insert(hash_a.clone(), in_flight_a);

    let calls: Arc<Mutex<Vec<(String, u32, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_inner = calls.clone();
    sec.register_phase_lifecycle_callbacks(
        Box::new(|_: &PhaseId| {}),
        Box::new(move |p: &PhaseId, c: u32, f: u32| {
            calls_inner
                .lock()
                .expect("poisoned")
                .push((p.as_str().to_string(), c, f));
        }),
    );

    sec.note_primary_item_failed(&hash_a, &ErrorType::NonRecoverable, &mut None)
        .await;

    let recorded = calls.lock().expect("poisoned");
    assert_eq!(
        recorded.len(),
        1,
        "NonRecoverable failure must NOT trigger a bucket re-inject"
    );
    assert_eq!(
        sec.primary_retry_passes_used_for_test(),
        0,
        "no bucket pass consumed for a non-bucket-matching error class"
    );
    assert_eq!(
        sec.primary_pending
            .as_ref()
            .expect("pool present")
            .phase_state(&phase),
        Some(PhaseState::Done),
    );
    assert_eq!(
        sec.primary_failed_count_for_test(),
        1,
        "NonRecoverable stays in primary_failed as fail_final"
    );
}

/// (5) Demoted-then-promoted scenario: a secondary acting as primary
/// inherits a pre-populated `primary_failed` ledger containing one
/// Recoverable + one OOM entry across two items of a phase whose
/// pool has just drained (the two items were in-flight on the
/// formerly-live primary, both came back as failures of different
/// classes, and the secondary was promoted before the drain edge
/// ran). On the first cascade pass:
///   * Recoverable bucket re-injects its candidate first.
///   * Because reinject flipped the phase Drained → Active, the
///     OOM bucket does NOT run on THIS iteration.
///   * `on_phase_end` does NOT fire (the phase is no longer
///     drained).
///
/// Pre-refactor (single-pass `primary_drain_check_and_retry`) only
/// the Recoverable class was stored, so the OOM half of this
/// scenario was structurally unrepresentable on the secondary path.
/// Post-refactor both classes mirror correctly.
#[tokio::test(flavor = "current_thread")]
async fn demoted_then_promoted_inherits_recoverable_and_oom_ledger() {
    let phase = PhaseId::from("phase-a");
    let mut cfg = election_config("sec-0");
    cfg.retry_max_passes = 1;
    cfg.oom_retry_max_passes = 1;
    let mut sec = make_secondary(cfg);
    sec.is_primary = true;
    // Pool starts EMPTY (both items already completed-as-failure
    // pre-promotion). The phase's pool state reflects this:
    // mark both task ids as in-flight and decrement them down so
    // the next `poll_drain_transitions` returns this phase.
    let mut pool = one_phase_pool_with_items(&phase, &["a", "b"]);
    pool.on_item_finished(&phase, Some("task-a"));
    pool.on_item_finished(&phase, Some("task-b"));
    sec.primary_pending = Some(pool);

    // Seed the inherited ledger directly — same shape
    // `note_primary_item_failed` produces on the worker-event path.
    // The retry-bucket keys by `compute_task_hash(binary)`, so seed
    // under that canonical hash (production parity: the worker's
    // wire `file_hash` IS this value).
    let bin_a = make_binary("a", "phase-a");
    let bin_b = make_binary("b", "phase-a");
    let hash_a = compute_task_hash(&bin_a);
    let hash_b = compute_task_hash(&bin_b);
    sec.primary_failed.insert(
        hash_a.clone(),
        FailedTaskEntry {
            binary: bin_a.clone(),
            error_type: ErrorType::Recoverable,
        },
    );
    sec.primary_failed.insert(
        hash_b.clone(),
        FailedTaskEntry {
            binary: bin_b.clone(),
            error_type: ErrorType::ResourceExhausted(ResourceKind::memory()),
        },
    );

    let calls: Arc<Mutex<Vec<(String, u32, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_inner = calls.clone();
    sec.register_phase_lifecycle_callbacks(
        Box::new(|_: &PhaseId| {}),
        Box::new(move |p: &PhaseId, c: u32, f: u32| {
            calls_inner
                .lock()
                .expect("poisoned")
                .push((p.as_str().to_string(), c, f));
        }),
    );

    // Drive the cascade directly; the in-flight markers were
    // decremented above so `poll_drain_transitions` will return
    // this phase on the next call.
    sec.process_primary_phase_lifecycle(&mut None).await;

    // The Recoverable bucket fired first; its reinject suppressed
    // both `on_phase_end` AND the OOM bucket on this iteration.
    let recorded = calls.lock().expect("poisoned");
    assert!(
        recorded.is_empty(),
        "Recoverable reinject suppresses on_phase_end on first drain edge; got {recorded:?}"
    );
    // Per-(phase, bucket) counters: Recoverable ticked, OOM did not.
    assert_eq!(
        sec.primary_retry_passes_used_for_test(),
        1,
        "Recoverable bucket consumed one pass; OOM bucket waits for the next drain edge"
    );
    // The Recoverable entry was drained from the ledger; the OOM
    // entry survives until the next drain edge runs the OOM bucket.
    assert!(
        !sec.primary_failed.contains_key(&hash_a),
        "Recoverable entry removed after reinject"
    );
    assert!(
        sec.primary_failed.contains_key(&hash_b),
        "OOM entry stays in primary_failed until its bucket runs"
    );
    // Pool flipped to Active via reinject of the Recoverable item.
    assert_eq!(
        sec.primary_pending
            .as_ref()
            .expect("pool present")
            .phase_state(&phase),
        Some(PhaseState::Active),
    );
}

/// (6) Budget exhaustion: a Recoverable failure that re-fails
/// after the bucket has already consumed its 1-pass budget stays
/// in `primary_failed` permanently. Phase advances to Done with
/// `on_phase_end(0, 1)`.
#[tokio::test(flavor = "current_thread")]
async fn recoverable_budget_exhaustion_lets_phase_advance() {
    let phase = PhaseId::from("phase-a");
    let mut cfg = election_config("sec-0");
    cfg.retry_max_passes = 1;
    cfg.oom_retry_max_passes = 0;
    let mut sec = make_secondary(cfg);
    sec.primary_pending = Some(one_phase_pool_with_items(&phase, &["a"]));
    let in_flight_a = make_in_flight("a", "phase-a");
    let hash_a = compute_task_hash(&in_flight_a.binary);
    sec.primary_in_flight.insert(hash_a.clone(), in_flight_a);

    let calls: Arc<Mutex<Vec<(String, u32, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_inner = calls.clone();
    sec.register_phase_lifecycle_callbacks(
        Box::new(|_: &PhaseId| {}),
        Box::new(move |p: &PhaseId, c: u32, f: u32| {
            calls_inner
                .lock()
                .expect("poisoned")
                .push((p.as_str().to_string(), c, f));
        }),
    );

    // Pass 1: failure → bucket reinjects → counter at 1.
    sec.note_primary_item_failed(&hash_a, &ErrorType::Recoverable, &mut None)
        .await;
    assert!(calls.lock().expect("poisoned").is_empty());
    assert_eq!(sec.primary_retry_passes_used_for_test(), 1);
    assert_eq!(
        sec.primary_pending
            .as_ref()
            .expect("pool present")
            .phase_state(&phase),
        Some(PhaseState::Active),
    );

    // Simulate the reinjected item being dispatched again to a
    // worker. In production `handle_primary_task_request`
    // (a) pops the queued item via `take_first_match` and
    // (b) calls `pool.mark_in_flight`. Mirror both so the pool's
    // (queued, in_flight) goes from (1, 0) → (0, 1); without (a)
    // the next `on_item_finished` would leave a queued item
    // behind and the phase would never re-drain.
    if let Some(pool) = sec.primary_pending.as_mut() {
        let _popped = pool.take_first_match(|b| b.phase_id == phase);
        pool.mark_in_flight(&phase);
    }
    sec.primary_in_flight
        .insert(hash_a.clone(), make_in_flight("a", "phase-a"));

    // Pass 2: failure → bucket budget exhausted → on_phase_end
    // fires with (completed=0, failed=2 — both arrivals bumped the
    // per-phase failure counter).
    sec.note_primary_item_failed(&hash_a, &ErrorType::Recoverable, &mut None)
        .await;

    let recorded = calls.lock().expect("poisoned");
    assert_eq!(
        recorded.len(),
        1,
        "budget-exhausted retry must let on_phase_end fire"
    );
    assert_eq!(recorded[0], ("phase-a".to_string(), 0, 2));
    assert_eq!(
        sec.primary_pending
            .as_ref()
            .expect("pool present")
            .phase_state(&phase),
        Some(PhaseState::Done),
    );
    assert!(
        sec.primary_failed.contains_key(&hash_a),
        "budget-exhausted Recoverable stays in primary_failed as fail_final"
    );
}
