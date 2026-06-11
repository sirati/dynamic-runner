//! Production replay (asm-tokenizer test-env run_20260611_131736): a
//! welcome-timeout with ZERO connected secondaries must be a run-level
//! FATAL, never a success-shaped outcome.
//!
//! In that run the submitter primary's `wait_for_connections` window
//! expired at +60s with 0/4 welcomes (sequential ~50-90s container
//! loads made welcoming physically impossible), the process logged
//! ERROR "primary node run failed" — and then printed the normal
//! accounting teardown ("Completed: 0 / Failed: 0 / Stranded: 0") and
//! exited rc=0. Mechanism: the timeout site returned a bare
//! `Err(String)`, which `From<String>` typed as `RunError::Other` — the
//! ONE swallow-eligible variant the PyO3 boundary deliberately maps to
//! exit 0.
//!
//! This test replays the observed sequence (n=4 expected, no welcome
//! ever arrives, window expires) through the real `run` entry and pins
//! the error TYPE: a zero-welcome bring-up failure must surface as the
//! structured [`RunError::BringUpFailed`] — never the swallow-eligible
//! `Other`.
//!
//! REVERT-CHECK: pre-fix the timeout arm returned `Err(format!(...))`
//! (→ `Other`), and this test failed RED on the not-`Other` assertion
//! with the verbatim production message ("timeout waiting for
//! secondaries: 0/4 sent SecondaryWelcome").

use super::*;

/// A submitter primary expecting 4 secondaries, none of which ever
/// welcomes, must resolve `run` to the structured bring-up fatal once
/// its quorum-proceed window expires.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn zero_welcome_timeout_is_structured_bring_up_fatal() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Four registered secondary outboxes (the production --jobs 4
            // shape); the rx halves are held open so beacon broadcasts
            // succeed, but NO welcome is ever fed into the primary's
            // inbound — the secondaries are still loading their images
            // when the window expires.
            let mut outgoing: HashMap<
                String,
                tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
            > = HashMap::new();
            let mut held_rx = Vec::new();
            for i in 0..4 {
                let (tx, rx) = tokio_mpsc::unbounded_channel();
                outgoing.insert(format!("sec-{i}"), tx);
                held_rx.push(rx);
            }
            let (_inbound_hold, inbound_rx) = tokio_mpsc::unbounded_channel();
            let transport =
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, inbound_rx);

            let config = PrimaryConfig {
                num_secondaries: 4,
                connect_timeout: Duration::from_secs(60),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // The production run was a cold-start submitter: seed a real
            // (small) corpus so the replay enters `run` the same way.
            let (deps, ops, ope) = noop_phase_args();
            let binaries = vec![(make_binary("a", 50), false)];
            let err = primary
                .run(
                    SeedSource::ColdStart {
                        binaries,
                        phase_deps: deps,
                    },
                    ops,
                    ope,
                )
                .await
                .expect_err(
                    "a 0/4-welcome bring-up timeout must fail the run, \
                     never resolve Ok",
                );

            // The decisive seam: `RunError::Other` is BY CONTRACT the one
            // swallow-eligible variant (the PyO3 boundary maps it to the
            // accounting summary + exit 0 — exactly the production rc=0
            // mask). A bring-up failure must be a structured variant the
            // boundary raises on.
            assert!(
                !matches!(err, RunError::Other(_)),
                "the zero-welcome bring-up timeout surfaced as the \
                 swallow-eligible RunError::Other — the PyO3 boundary maps \
                 that to 'Completed: 0 / Failed: 0 / Stranded: 0' + rc=0 \
                 (the run_20260611_131736 false-green): {err}"
            );
            assert!(
                matches!(
                    err,
                    RunError::BringUpFailed { ref reason }
                        if reason.contains("0/4 sent SecondaryWelcome")
                ),
                "expected the structured bring-up fatal naming the 0/4 \
                 welcome count, got: {err:?}"
            );
        })
        .await;
}
