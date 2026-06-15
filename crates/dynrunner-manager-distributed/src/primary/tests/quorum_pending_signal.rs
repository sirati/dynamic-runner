//! Unit tests for the setup-quorum PENDING(Resources) observability signal
//! (#565/#572).
//!
//! # Single concern
//! `AuthorityUpdaterHandle::publish` emits exactly ONE important INFO line the
//! FIRST time a probe result carries `pending_resources > 0`; it stays silent
//! when `pending_resources == 0` or when no emit config is wired.
//!
//! # Why probe-publish, NOT wait_for_connections
//! The original (#565) emit was gated at `wait_for_connections` start, reading
//! `pending_resources_count()` synchronously. The first probe tick fires
//! immediately but only STARTS the async squeue subprocess — the count stays
//! at 0 until that subprocess returns (10ms–100ms+ in practice). The
//! synchronous read near-always beats the probe, silently skipping the emit.
//! #572 moves the emit to `AuthorityUpdaterHandle::publish` — the sole point
//! where fresh probe data is installed — so the INFO line fires the moment
//! squeue data first arrives, regardless of `wait_for_connections` timing.
//!
//! # Test strategy
//! Tests drive `AuthorityUpdaterHandle::publish` directly (no full primary
//! needed). A `TargetCapture` layer asserts event count and field values.
//! The `StaticSnapshot`-based helper used by the removed wait-start tests is
//! no longer applicable to the new emit site, so the test helper is
//! replaced with a publish-level harness.

use std::collections::HashMap;

use tracing::subscriber::set_default;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::Registry;

use crate::authority_snapshot::OffLoopAuthoritySnapshot;
use crate::test_capture::{IMPORTANT_TARGET, TargetCapture};

/// Build a handle with emit config, drive `publish()` with the given
/// `pending_resources`, and return all events captured on the importance
/// target. A 30s probe interval is standard (staleness gating is irrelevant
/// for publish-path tests — the latch is checked before the map is stored).
fn run_publish_with_pending(
    pending_resources: usize,
    partition: Option<&str>,
    num_secondaries: usize,
) -> Vec<crate::test_capture::LeveledEvent> {
    let probe_interval = std::time::Duration::from_secs(30);
    let snapshot = OffLoopAuthoritySnapshot::new(probe_interval);
    let handle = snapshot
        .updater_handle()
        .with_emit_config(partition.map(str::to_owned), num_secondaries);

    let capture = TargetCapture::for_target(IMPORTANT_TARGET);
    let subscriber = Registry::default().with(capture.clone());
    let _guard = set_default(subscriber);

    handle.publish(HashMap::new(), pending_resources);

    capture.events()
}

// ── POSITIVE: first publish with k>0 emits exactly one important INFO ────────

/// `pending_resources = 2` on the first publish emits exactly ONE INFO event
/// on the importance target, naming the pending count and the partition.
#[test]
fn pending_resources_emits_one_important_info_on_first_publish() {
    let events = run_publish_with_pending(2, Some("gpu-partition"), 5);

    let important: Vec<_> = events
        .iter()
        .filter(|e| e.level == tracing::Level::INFO)
        .collect();
    assert_eq!(
        important.len(),
        1,
        "exactly ONE important INFO must fire when pending_resources=2; \
         got {} events: {events:?}",
        important.len(),
    );
    let ev = &important[0].event;

    assert_eq!(
        ev.fields.get("pending_resources").map(String::as_str),
        Some("2"),
        "pending_resources field must be 2; fields: {:?}",
        ev.fields
    );
    assert!(
        ev.fields
            .get("partition")
            .is_some_and(|v| v.contains("gpu-partition")),
        "partition field must name the configured partition; fields: {:?}",
        ev.fields
    );
    assert!(
        ev.message.contains("PENDING(Resources)"),
        "message must mention PENDING(Resources); got {:?}",
        ev.message
    );
}

// ── NEGATIVE: zero pending_resources ─────────────────────────────────────────

/// `pending_resources = 0` on every publish must NOT produce any important
/// event — the partition has capacity for all jobs.
#[test]
fn zero_pending_resources_emits_no_important_event() {
    let events = run_publish_with_pending(0, None, 4);
    assert!(
        events.is_empty(),
        "zero pending_resources must NOT produce any important event; \
         got {events:?}",
    );
}

// ── LATCH: second publish with k>0 must NOT re-emit ──────────────────────────

/// A second `publish()` call with `pending_resources > 0` on the SAME handle
/// must NOT emit a duplicate INFO line — the latch is fire-once per lineage.
#[test]
fn pending_resources_latch_prevents_duplicate_emit() {
    let probe_interval = std::time::Duration::from_secs(30);
    let snapshot = OffLoopAuthoritySnapshot::new(probe_interval);
    let handle = snapshot
        .updater_handle()
        .with_emit_config(Some("gpu-partition".into()), 5);

    let capture = TargetCapture::for_target(IMPORTANT_TARGET);
    let subscriber = Registry::default().with(capture.clone());
    let _guard = set_default(subscriber);

    // First publish: k=2 → emit fires.
    handle.publish(HashMap::new(), 2);
    // Second publish: k=3 (still > 0) → latch held, no re-emit.
    handle.publish(HashMap::new(), 3);

    let important: Vec<_> = capture
        .events()
        .into_iter()
        .filter(|e| e.level == tracing::Level::INFO)
        .collect();
    assert_eq!(
        important.len(),
        1,
        "latch must prevent a second emit; got {} events: {important:?}",
        important.len(),
    );
}

// ── LATE-ARRIVING PENDING: empty-first then k>0 ──────────────────────────────

/// The race scenario that defeated #565: the first probe returns
/// `pending_resources = 0` (jobs just submitted, squeue not yet showing them
/// as PD(Resources)), then a later probe returns `pending_resources = 3`.
/// The emit must fire on the SECOND publish — the first was silent.
///
/// This is the primary correctness check for #572: the probe-publish path
/// handles late-arriving PENDING(Resources) counts that were 0 at
/// wait_for_connections start.
#[test]
fn late_arriving_pending_resources_emits_on_first_nonzero_publish() {
    let probe_interval = std::time::Duration::from_secs(30);
    let snapshot = OffLoopAuthoritySnapshot::new(probe_interval);
    let handle = snapshot
        .updater_handle()
        .with_emit_config(Some("gpu".into()), 4);

    let capture = TargetCapture::for_target(IMPORTANT_TARGET);
    let subscriber = Registry::default().with(capture.clone());
    let _guard = set_default(subscriber);

    // First publish: k=0 (jobs not yet showing as PD(Resources)) → silent.
    handle.publish(HashMap::new(), 0);
    assert!(
        capture.events().is_empty(),
        "first publish with k=0 must be silent; got {:?}",
        capture.events()
    );

    // Second publish: k=3 (partition capacity exhausted) → emit fires.
    handle.publish(HashMap::new(), 3);

    let important: Vec<_> = capture
        .events()
        .into_iter()
        .filter(|e| e.level == tracing::Level::INFO)
        .collect();
    assert_eq!(
        important.len(),
        1,
        "emit must fire on the first nonzero publish; got {} events: {important:?}",
        important.len(),
    );
    assert_eq!(
        important[0].event.fields.get("pending_resources").map(String::as_str),
        Some("3"),
        "pending_resources field must be 3; fields: {:?}",
        important[0].event.fields,
    );
}

// ── NO EMIT CONFIG: handle without context stays silent ──────────────────────

/// A handle that was NOT configured with `with_emit_config` must stay silent
/// even when `pending_resources > 0`. Non-SLURM / test-stub callers that
/// never call `with_emit_config` must not emit spurious lines.
#[test]
fn no_emit_config_stays_silent() {
    let probe_interval = std::time::Duration::from_secs(30);
    let snapshot = OffLoopAuthoritySnapshot::new(probe_interval);
    // Deliberately NO `with_emit_config` call.
    let handle = snapshot.updater_handle();

    let capture = TargetCapture::for_target(IMPORTANT_TARGET);
    let subscriber = Registry::default().with(capture.clone());
    let _guard = set_default(subscriber);

    handle.publish(HashMap::new(), 5);

    assert!(
        capture.events().is_empty(),
        "a handle without emit config must produce no important events; \
         got {:?}",
        capture.events()
    );
}
