//! Unit tests for the setup-quorum PENDING(Resources) observability signal
//! (#565).
//!
//! # Single concern
//! `wait_for_connections` emits exactly ONE important INFO line at wait-start
//! when the SLURM-authoritative snapshot reports `k > 0`
//! PENDING(Resources) jobs; it stays silent when `k = 0` or the snapshot
//! is absent.
//!
//! # Test strategy
//! We install a `StaticSnapshot` with a controlled `pending_resources` value,
//! then run `wait_for_connections` with a 1ms `connect_timeout` so it exits
//! immediately via the quorum-proceed timeout arm (no real secondaries
//! connect). The `TargetCapture` (safe to hold across `.await` on a
//! current-thread runtime) asserts the correct event count and field values.

use std::sync::Arc;
use std::time::Duration;

use tracing::subscriber::set_default;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::Registry;

use crate::authority_snapshot::test_helpers::StaticSnapshot;
use crate::test_capture::{IMPORTANT_TARGET, TargetCapture};

use super::*;

/// Helper: build a primary with `num_secondaries = 2`, a 1ms connect timeout
/// (so `wait_for_connections` exits instantly), and a `StaticSnapshot` with
/// the given `pending_resources` count.
///
/// Runs `wait_for_connections` and returns the events captured on the
/// importance target.
async fn run_quorum_wait_with_pending(
    pending_resources: Option<usize>,
    partition: Option<String>,
) -> Vec<crate::test_capture::LeveledEvent> {
    let (transport, _ends) = setup_test(2);
    let config = PrimaryConfig {
        num_secondaries: 2,
        // 1 ms: the wait exits immediately via the quorum-proceed timeout arm.
        connect_timeout: Duration::from_millis(1),
        slurm_partition: partition,
        ..test_primary_config()
    };
    let (mut primary, _mesh) = build_test_primary(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    primary.set_authority_snapshot(Arc::new(StaticSnapshot {
        map: Default::default(),
        count: None,
        pending_resources,
    }));

    let capture = TargetCapture::for_target(IMPORTANT_TARGET);
    let subscriber = Registry::default().with(capture.clone());
    let _guard = set_default(subscriber);

    // Drive the wait. We pass `None` as the command_rx (no commands
    // expected for this narrow unit).
    let _ = primary.wait_for_connections(&mut None).await;

    capture.events()
}

/// POSITIVE: `pending_resources = Some(2)` emits exactly one INFO event on
/// the importance target naming the pending count, partition, deadline, and
/// suggested job count.
#[tokio::test(flavor = "current_thread")]
async fn pending_resources_emits_one_important_info_at_wait_start() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let events =
                run_quorum_wait_with_pending(Some(2), Some("gpu-partition".into())).await;

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

            // Fields: pending_resources, partition, deadline_secs present.
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
        })
        .await;
}

/// NEGATIVE: `pending_resources = Some(0)` — no PENDING jobs — must produce
/// NO important events at all (only the normal wait-start non-important INFO
/// is allowed).
#[tokio::test(flavor = "current_thread")]
async fn zero_pending_resources_emits_no_important_event() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let events = run_quorum_wait_with_pending(Some(0), None).await;
            assert!(
                events.is_empty(),
                "zero pending_resources must NOT produce any important event; \
                 got {events:?}",
            );
        })
        .await;
}

/// NEGATIVE: `pending_resources = None` (snapshot absent / stale) — no
/// important event.
#[tokio::test(flavor = "current_thread")]
async fn absent_snapshot_emits_no_important_event() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let events = run_quorum_wait_with_pending(None, None).await;
            assert!(
                events.is_empty(),
                "absent snapshot (None) must NOT produce any important event; \
                 got {events:?}",
            );
        })
        .await;
}
