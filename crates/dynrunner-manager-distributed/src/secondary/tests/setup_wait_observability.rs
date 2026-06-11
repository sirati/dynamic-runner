//! Owner-spec (#404) secondary startup observability: the
//! instructions-wait narration schedule and its 10-minute structured
//! abort.
//!
//! The spec: a secondary logs when it is READY to receive instructions
//! from setup, then — while none arrive — marks the wait at 30s, 1m and
//! 5m (exactly that escalating schedule, not a periodic heartbeat), and
//! at 10m it aborts loudly. The schedule shares the give-up policy's
//! clock (the re-armable `setup_deadline`), and the abort surfaces as
//! the STRUCTURED `RunError::BringUpFailed` — the secondary-side twin of
//! the primary's zero-welcome bring-up fatal — never a silent cold-exit
//! or a generic policy-exit.
//!
//! Three pins:
//!
//! - `wait_marks_fire_under_constant_beacon_load`: the marks fire at
//!   30s/1m/5m — and ONLY those — while the ~20s anti-entropy digest
//!   tick and the handshake-retry arm churn the same `select!` (the
//!   watchdog-needs-a-fires-under-load law: a per-iteration sleep arm
//!   would be reset by every sibling fire and never mark anything).
//!   The READY line is also pinned: exactly one, at wait entry.
//!
//! - `arrival_at_90s_resets_the_schedule_no_5m_mark`: primary evidence
//!   at t=90s ends the original wait window — its 5m mark never fires;
//!   the narration re-escalates from 30s anchored at the arrival (the
//!   same re-anchor the deadline's `extend()` performs, read off the
//!   ONE shared cell).
//!
//! - `expiry_is_the_structured_bring_up_fatal`: the deadline expiry
//!   records the TYPED `SecondaryTerminal::BringUpFailed` (assert the
//!   typed shape, not just the log) — the seam `process::run::outcome`
//!   maps to `RunError::BringUpFailed` so the PyO3 boundary raises
//!   non-zero with the bring-up story.

#![cfg(test)]

use super::super::test_helpers::{
    FakeWorkerFactory, TestId, channel_mesh_to_primary, make_secondary_channel,
    start_secondary_pump,
};
use super::super::*;
use crate::cluster_state::ClusterState;
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;

fn observability_config(secondary_id: &str, horizon: Duration) -> SecondaryConfig {
    SecondaryConfig {
        secondary_id: secondary_id.into(),
        num_workers: 1,
        max_resources: dynrunner_core::ResourceMap::from([(
            dynrunner_core::ResourceKind::memory(),
            1024 * 1024 * 1024,
        )]),
        hostname: "test-host".into(),
        keepalive_interval: Duration::from_millis(50),
        src_network: None,
        src_tmp: None,
        peer_timeout: Duration::from_secs(120),
        keepalive_miss_threshold: 3,
        retry_max_passes: 1,
        oom_retry_max_passes: 1,
        primary_link_failure_threshold: 5,
        primary_link_failure_window: Duration::from_secs(30),
        primary_silence_backstop: Duration::from_secs(120),
        unconfigured_deadline: horizon,
        can_be_primary: true,
        resource_check_interval: Duration::from_millis(100),
        log_oom_watcher: false,
        promoted_primary_quiesce_grace: Duration::from_millis(100),
        unfulfillable_reinject_max_per_task: None,
        mem_manager_reserved_bytes: None,
        output_dir: None,
        memuse_log_path: None,
        forwarded_argv: Vec::new(),
    }
}

/// Capture every event the setup module emits, scoped to this thread
/// (and, on a `current_thread` runtime, therefore to the whole test).
/// See `test_capture::TargetCapture` for why an always-interest install
/// is safe to hold across `.await`s.
fn setup_log_capture() -> (
    crate::test_capture::TargetCapture,
    tracing::subscriber::DefaultGuard,
) {
    use tracing_subscriber::layer::SubscriberExt;
    let capture =
        crate::test_capture::TargetCapture::for_target(crate::secondary::setup::LOG_TARGET);
    let subscriber = tracing_subscriber::Registry::default().with(capture.clone());
    let guard = tracing::subscriber::set_default(subscriber);
    (capture, guard)
}

/// The `waited_secs` values of every wait-mark event captured so far,
/// in emission order.
fn captured_mark_secs(capture: &crate::test_capture::TargetCapture) -> Vec<String> {
    capture
        .events()
        .iter()
        .filter(|e| e.event.message.contains("still waiting for instructions"))
        .map(|e| {
            e.event
                .fields
                .get("waited_secs")
                .cloned()
                .unwrap_or_default()
        })
        .collect()
}

/// SILENT primary, busy select: marks at exactly 30s/1m/5m of waiting
/// while the anti-entropy beacon (~20s jittered) and the handshake
/// retry arm fire dozens of times in between — sibling activity must
/// never reset the schedule, and no extra marks may appear.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn wait_marks_fire_under_constant_beacon_load() {
    let (capture, _guard) = setup_log_capture();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, _sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (_pri_to_sec_hold, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let config = observability_config("sec-marks-load", Duration::from_secs(600));
            let unified =
                channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut harness = make_secondary_channel(config, unified);
            harness.set_bootstrap_primary_id("setup".to_string());
            let (mut secondary, _guard_pump) = start_secondary_pump(harness);

            let mut factory = FakeWorkerFactory;
            tokio::select! {
                res = secondary.run_until_setup_or_done(&mut factory) => panic!(
                    "the wait must still be inside its 600s horizon at t=400s, got {res:?}"
                ),
                _ = tokio::time::sleep(Duration::from_secs(400)) => {}
            }

            // The escalating schedule, exactly: 30s, 1m, 5m — nothing
            // else fired by t=400s despite ~20 beacon ticks of select
            // churn (a sibling-reset schedule would have fired NOTHING;
            // a periodic one would have fired more).
            assert_eq!(
                captured_mark_secs(&capture),
                vec!["30", "60", "300"],
                "the owner schedule is 30s/1m/5m, fired exactly once each \
                 under constant sibling-arm load"
            );
            // Sanity that the load was real: the digest beacon ticked
            // many times inside the same select.
            let beacon_ticks = capture
                .events()
                .iter()
                .filter(|e| e.event.message.contains("anti-entropy digest broadcast"))
                .count();
            assert!(
                beacon_ticks >= 10,
                "expected the ~20s digest tick to churn the select \
                 (saw {beacon_ticks} ticks) — without load this test \
                 proves nothing about fires-under-load"
            );
            // READY (owner-spec line 2): exactly one readiness line, at
            // wait entry.
            let ready_lines = capture
                .events()
                .iter()
                .filter(|e| {
                    e.event
                        .message
                        .contains("ready to receive instructions from setup")
                })
                .count();
            assert_eq!(ready_lines, 1, "exactly one READY line at wait entry");
        })
        .await;
}

/// Primary evidence at t=90s ends the original wait window: its 5m mark
/// never fires (no `waited_secs=300` by t=380s even though 380 > 300),
/// and the narration re-escalates from 30s anchored at the arrival —
/// the marks and the give-up deadline move together because they read
/// the ONE shared cell.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn arrival_at_90s_resets_the_schedule_no_5m_mark() {
    let (capture, _guard) = setup_log_capture();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, _sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let config = observability_config("sec-marks-arrival", Duration::from_secs(600));
            let unified =
                channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut harness = make_secondary_channel(config, unified);
            harness.set_bootstrap_primary_id("setup".to_string());
            let (mut secondary, _guard_pump) = start_secondary_pump(harness);

            // The arrival: one frame from the PRIMARY ("setup", the
            // bootstrap-dialled id) at t=90s — the same digest shape the
            // assembling primary's setup-liveness beacon sends. It
            // extends the deadline (note_setup_primary_liveness), which
            // IS the schedule's re-anchor.
            let driver = async {
                tokio::time::sleep(Duration::from_secs(90)).await;
                pri_to_sec_tx
                    .send(DistributedMessage::StateDigest {
                        target: None,
                        sender_id: "setup".into(),
                        timestamp: 0.0,
                        digest: ClusterState::<TestId>::new().digest(),
                    })
                    .expect("inbound open");
                tokio::time::sleep(Duration::from_secs(290)).await; // t=380
            };

            let mut factory = FakeWorkerFactory;
            tokio::select! {
                res = secondary.run_until_setup_or_done(&mut factory) => panic!(
                    "the wait must still be pending at t=380s (the evidence \
                     extended the 600s horizon), got {res:?}"
                ),
                () = driver => {}
            }

            let marks = captured_mark_secs(&capture);
            assert!(
                !marks.iter().any(|m| m == "300"),
                "the ORIGINAL window's 5m mark must never fire once \
                 instructions/evidence arrived at t=90s (got {marks:?})"
            );
            assert_eq!(
                marks,
                vec!["30", "60", "30", "60"],
                "the wait narration re-escalates from 30s anchored at the \
                 t=90s arrival (new-window marks at t=120/150); the new \
                 window's own 5m mark is not due until t=390"
            );
        })
        .await;
}

/// The 10-minute point of the owner schedule is the abort: the deadline
/// expiry records the TYPED `SecondaryTerminal::BringUpFailed` terminal
/// (the shape `process::run::outcome` maps to the structured
/// `RunError::BringUpFailed` raise) — asserted on the type, not the log.
///
/// REVERT-CHECK: pre-fix the expiry returned a bare `Err(String)` with
/// NO terminal recorded, which the node-outcome mapping types as the
/// generic `FatalPolicyExit` (whose Display blames "a run-loop policy"
/// for what is a bring-up failure).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn expiry_is_the_structured_bring_up_fatal() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, _sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (_pri_to_sec_hold, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            // Short horizon so the paused-clock test observes the expiry
            // fast; the production default (600s) IS the spec's 10m.
            let config = observability_config("sec-bringup-fatal", Duration::from_secs(10));
            let unified =
                channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut harness = make_secondary_channel(config, unified);
            harness.set_bootstrap_primary_id("setup".to_string());
            let (mut secondary, _guard_pump) = start_secondary_pump(harness);

            let mut factory = FakeWorkerFactory;
            let err = tokio::time::timeout(
                Duration::from_secs(30),
                secondary.run_until_setup_or_done(&mut factory),
            )
            .await
            .expect("the 10s horizon must expire well inside 30s")
            .expect_err("a silent primary must expire the setup wait as an Err");
            assert!(
                err.contains("setup deadline") && err.contains("elapsed"),
                "the expiry Err carries the deadline diagnosis: {err}"
            );

            // The decisive seam: the lifecycle recorded the TYPED
            // bring-up terminal carrying the SAME reason, so the node
            // outcome surfaces `RunError::BringUpFailed` (the boundary
            // raises), never the FatalPolicyExit misattribution.
            match secondary.terminal() {
                Some(SecondaryTerminal::BringUpFailed { reason }) => {
                    assert_eq!(
                        reason, err,
                        "the typed terminal and the propagated Err must \
                         carry the same diagnosis"
                    );
                }
                other => panic!(
                    "the setup-wait expiry must record the TYPED \
                     SecondaryTerminal::BringUpFailed terminal \
                     (the structured-fatal seam); got {other:?}"
                ),
            }
        })
        .await;
}
