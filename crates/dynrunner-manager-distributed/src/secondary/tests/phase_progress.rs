//! Secondary per-worker phase-progress observability (#534 wiring).
//!
//! The distributed secondary now mirrors the single-node LocalManager's
//! per-worker phase-progress LOGGING: a `PhaseUpdate` from one of its
//! OWN workers is RECORDED on the slot (through the shared
//! [`dynrunner_manager_local::WorkerPool::note_phase_update`] seam)
//! instead of being discarded, and the secondary's OOM-sweep arm fires
//! the shared [`dynrunner_manager_local::WorkerPool::report_stuck_workers`]
//! reporter so a worker held quiet in one phase past the configured
//! interval surfaces an escalating WARN.
//!
//! OBSERVABILITY ONLY: these tests assert the WARN is EMITTED. They do
//! NOT (and must not) assert any force-fail / timeout / kill — the
//! secondary's userland-kill path stays gated off; this fix adds the
//! visibility half only.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::Subscriber;
use tracing::subscriber::with_default;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;

use super::super::test_helpers::{FakeWorkerFactory, make_secondary};
use super::generation_gate::{one_worker_config, test_oom_watcher};
use dynrunner_manager_local::WorkerEvent;

/// Capture WARN-level events whose message is the stuck-worker phase
/// line. An unfiltered (always-interest) layer is safe to hold across
/// `.await`s on a current-thread runtime — see `test_capture::TargetCapture`
/// for the interest-cache rationale — but here we only install it around
/// the SYNCHRONOUS `report_stuck_workers` call anyway.
#[derive(Clone, Default)]
struct StuckWarnCapture {
    lines: Arc<Mutex<Vec<String>>>,
}

impl StuckWarnCapture {
    fn stuck_count(&self) -> usize {
        self.lines
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.contains("in the same phase for"))
            .count()
    }
}

impl<S> Layer<S> for StuckWarnCapture
where
    S: Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }
        #[derive(Default)]
        struct MsgVisitor(String);
        impl tracing::field::Visit for MsgVisitor {
            fn record_debug(
                &mut self,
                field: &tracing::field::Field,
                value: &dyn std::fmt::Debug,
            ) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut v = MsgVisitor::default();
        event.record(&mut v);
        self.lines.lock().unwrap().push(v.0);
    }
}

/// End-to-end secondary wiring: a `PhaseUpdate` from this secondary's
/// own worker is RECORDED on the slot via `handle_worker_event` (no
/// longer discarded), and the shared `report_stuck_workers` reporter —
/// the one the secondary's OOM-sweep arm fires — then emits the
/// escalating phase-progress WARN for a worker held past the interval.
///
/// Drives the report with a zero-length interval so the held phase is
/// "past" immediately, without a real long sleep: the test pins that the
/// recorded phase + the reporter are wired, not the wall-clock duration
/// (the duration arithmetic itself is pinned at the pool seam in
/// `pool::stuck_worker_report_tests`).
#[tokio::test(flavor = "current_thread")]
async fn secondary_phase_update_drives_stuck_worker_warn() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut secondary = make_secondary(one_worker_config("sec-1"));
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            let current_gen = secondary.pool_mut().workers[0].generation;
            let oom = test_oom_watcher();

            // The worker reports it entered a phase. Pre-#534 the
            // secondary DISCARDED this (debug!+Ok(None)); now it records
            // it on the slot through the shared seam.
            secondary
                .handle_worker_event(
                    WorkerEvent::PhaseUpdate {
                        worker_id: 0,
                        generation: current_gen,
                        phase_name: "ghidra-analysis".into(),
                    },
                    &oom,
                    &mut FakeWorkerFactory,
                )
                .await
                .unwrap();

            // The phase must now be recorded on the slot (the discard is
            // gone): the reporter reads `phase` + `phase_started_at`.
            assert_eq!(
                secondary.pool_mut().workers[0].phase.as_deref(),
                Some("ghidra-analysis"),
                "PhaseUpdate must be RECORDED on the slot, not discarded"
            );
            assert!(
                secondary.pool_mut().workers[0].phase_started_at.is_some(),
                "recording a phase must stamp phase_started_at for the reporter"
            );

            // Fire the SAME shared reporter the OOM-sweep arm fires,
            // under a WARN capture. A zero interval makes the held phase
            // immediately past, so the escalating WARN must fire.
            let capture = StuckWarnCapture::default();
            let subscriber = tracing_subscriber::registry().with(capture.clone());
            with_default(subscriber, || {
                secondary
                    .pool_mut()
                    .report_stuck_workers(&[Duration::from_millis(0)]);
            });

            assert_eq!(
                capture.stuck_count(),
                1,
                "the secondary must emit the per-worker phase-progress WARN \
                 for a worker held in a phase past the interval"
            );
        })
        .await;
}

/// A `Keepalive` from the secondary's own worker is RECORDED through the
/// shared seam (no longer discarded as a bare `trace!`): it refreshes the
/// slot's `last_keepalive` clock. Pins the keepalive-wiring half — the
/// liveness signal the reporter + timeout machinery read.
#[tokio::test(flavor = "current_thread")]
async fn secondary_keepalive_refreshes_last_keepalive() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut secondary = make_secondary(one_worker_config("sec-1"));
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // Freshly-initialised slot has never been seen.
            assert!(
                secondary.pool_mut().workers[0].last_keepalive.is_none(),
                "a fresh slot has no recorded keepalive"
            );

            let current_gen = secondary.pool_mut().workers[0].generation;
            let oom = test_oom_watcher();
            secondary
                .handle_worker_event(
                    WorkerEvent::Keepalive {
                        worker_id: 0,
                        generation: current_gen,
                    },
                    &oom,
                    &mut FakeWorkerFactory,
                )
                .await
                .unwrap();

            assert!(
                secondary.pool_mut().workers[0].last_keepalive.is_some(),
                "a Keepalive must refresh the slot's last_keepalive (no longer discarded)"
            );
        })
        .await;
}
