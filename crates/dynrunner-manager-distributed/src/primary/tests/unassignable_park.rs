//! Tests for the unassignable-`TaskRequest` DROP-PATH log shape
//! (`handle_task_request`, the `!assigned` arm).
//!
//! The drop path splits by sub-cause:
//!   * a request naming a KNOWN roster slot that finds no dispatchable
//!     work is the PARK case — rolled up through
//!     `coordinator::unassignable_park_warn` (one line per interval naming
//!     the parked-worker count + the suppressed re-request count), because
//!     at scale every idle worker re-requests once per backoff tick;
//!   * a request naming NO roster slot keeps its individual line (rare,
//!     orthogonal membership/liveness signal).
//!
//! Captured with a per-test scoped, unfiltered `TargetCapture` on the
//! request module's target — `Interest::always`, safe to hold across the
//! `.await` in `handle_task_request` (the same pattern the
//! illegal-assignment log-shape test uses). `start_paused` time drives the
//! `WarnThrottle` interval deterministically.

use super::*;

use dynrunner_core::{ResourceMap, ResourceKind};

use crate::primary::task::UNASSIGNABLE_PARK_WARN_INTERVAL;

const REQUEST_TARGET: &str = "dynrunner_manager_distributed::primary::task::request";

/// Empty-pool one-secondary primary with `idle_workers` idle slots on
/// `sec-0` (local worker ids `0..idle_workers`). Every `TaskRequest`
/// against such a slot parks (nothing in the pool fits).
#[allow(clippy::type_complexity)]
fn primary_with_idle_workers_empty_pool(
    idle_workers: u32,
) -> (
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    Vec<(
        String,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
    PrimaryMeshKeepalive,
) {
    let (transport, ends) = setup_test(1);
    let (mut primary, mesh) = build_test_primary(
        PrimaryConfig::default(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let phase = dynrunner_core::PhaseId::from("work");
    let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new([phase], HashMap::new())
        .expect("work-phase pool");
    primary.pending = Some(pool);
    for w in 0..idle_workers {
        primary.register_idle_worker_for_test(
            "sec-0".into(),
            w,
            ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)]),
        );
    }
    (primary, ends, mesh)
}

/// A `TaskRequest` from `sec-0`'s worker `worker_id` (a known roster slot
/// once registered).
fn park_request(worker_id: u32) -> DistributedMessage<TestId> {
    DistributedMessage::TaskRequest {
        target: None,
        sender_id: "sec-0".into(),
        timestamp: 0.0,
        secondary_id: "sec-0".into(),
        worker_id,
        available_resources: vec![dynrunner_core::ResourceAmount {
            kind: ResourceKind::memory(),
            amount: 1024 * 1024 * 1024u64,
        }],
    }
}

/// PARK case: N park-drops inside one throttle window emit EXACTLY ONE
/// rolled-up line carrying `suppressed_re_requests = N - 1` and the live
/// parked-worker count; past the interval the next park re-emits.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn park_drops_roll_up_into_one_line_with_suppressed_count() {
    let log = crate::test_capture::TargetCapture::for_target(REQUEST_TARGET);
    let _guard = {
        use tracing_subscriber::layer::SubscriberExt;
        tracing::subscriber::set_default(tracing_subscriber::Registry::default().with(log.clone()))
    };
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // 3 idle workers, empty pool — every request parks.
            let (mut primary, _ends, _mesh) = primary_with_idle_workers_empty_pool(3);

            // Five park-drops back-to-back inside the same window.
            for _ in 0..5 {
                primary
                    .handle_task_request(park_request(0), &mut None)
                    .await
                    .expect("park drop succeeds");
            }

            let park_lines: Vec<_> = log
                .events()
                .into_iter()
                .filter(|e| e.event.fields.contains_key("parked_workers"))
                .collect();
            assert_eq!(
                park_lines.len(),
                1,
                "five park-drops in one window emit exactly one rolled-up line"
            );
            let fields = &park_lines[0].event.fields;
            // The FIRST occurrence always emits, so it carries 0 suppressed;
            // the next four are suppressed + counted, surfacing on the NEXT
            // emitted line (after the interval) below.
            assert_eq!(
                fields.get("suppressed_re_requests").map(String::as_str),
                Some("0"),
                "the first occurrence emits immediately with a zero suppressed count"
            );
            assert_eq!(
                fields.get("parked_workers").map(String::as_str),
                Some("3"),
                "the parked-worker count is the live idle count, computed on the permitted tick"
            );

            // Past the interval the next park re-emits, NAMING the four
            // re-requests suppressed in between.
            tokio::time::advance(UNASSIGNABLE_PARK_WARN_INTERVAL + std::time::Duration::from_secs(1))
                .await;
            primary
                .handle_task_request(park_request(0), &mut None)
                .await
                .expect("park drop succeeds");
            let park_lines: Vec<_> = log
                .events()
                .into_iter()
                .filter(|e| e.event.fields.contains_key("parked_workers"))
                .collect();
            assert_eq!(
                park_lines.len(),
                2,
                "past the interval the throttle re-emits a second rolled-up line"
            );
            assert_eq!(
                park_lines[1]
                    .event
                    .fields
                    .get("suppressed_re_requests")
                    .map(String::as_str),
                Some("4"),
                "the second emit names the four re-requests suppressed in the interval"
            );
        })
        .await;
}

/// NO-ROSTER-SLOT case: a request naming an unknown worker keeps its
/// INDIVIDUAL line and never feeds the park throttle (orthogonal cause).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn no_roster_slot_keeps_individual_line() {
    let log = crate::test_capture::TargetCapture::for_target(REQUEST_TARGET);
    let _guard = {
        use tracing_subscriber::layer::SubscriberExt;
        tracing::subscriber::set_default(tracing_subscriber::Registry::default().with(log.clone()))
    };
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // No registered workers → any request names no roster slot.
            let (mut primary, _ends, _mesh) = primary_with_idle_workers_empty_pool(0);

            for _ in 0..3 {
                primary
                    .handle_task_request(park_request(99), &mut None)
                    .await
                    .expect("no-slot drop succeeds");
            }

            let events = log.events();
            let no_slot_lines = events
                .iter()
                .filter(|e| e.event.message.contains("no roster slot"))
                .count();
            assert_eq!(
                no_slot_lines, 3,
                "each no-roster-slot drop keeps its own individual line (not throttled)"
            );
            assert!(
                events
                    .iter()
                    .all(|e| !e.event.fields.contains_key("parked_workers")),
                "the no-roster-slot cause never feeds the park throttle"
            );
        })
        .await;
}
