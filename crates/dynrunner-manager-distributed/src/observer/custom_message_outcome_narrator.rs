//! The observer's F5 custom-message OUTCOME narrator (#570).
//!
//! # Single concern
//!
//! Turn each [`crate::custom_message_outcome::CustomMessageOutcomeEvent`]
//! the F5 apply rule fires into ONE operator wake-line on the
//! target picked by the consumer's `is_high_volume` flag (#583/#587)
//! via [`dynrunner_core::high_volume_target`]:
//!   - `Handled` → INFO "custom message handled: from {origin} (seq {seq})";
//!   - `Failed { reason }` → ERROR "custom message handler FAILED
//!     (raised): from {origin} (seq {seq}) — {reason} — result discarded,
//!     no task mutations applied".
//!
//! `is_high_volume=False` (the default, low-fanout consumer) ⇒
//! [`dynrunner_core::IMPORTANT_TARGET`] (wake-worthy on every sink);
//! `is_high_volume=True` (asm-dataset's per-spawn-batch
//! `dep_graph_spawn` and similar) ⇒
//! [`dynrunner_core::OBSERVER_TASK_TARGET`] (suppressed from stdio
//! under `--important-stdio-only`; the rate-limited aggregator rollup
//! line is the wake signal).
//!
//! It owns NO state — every narrated field rides the event, which the
//! F5 apply rule built BEFORE the per-origin watermark compactor erased
//! the Handled/Failed label (so the post-compaction state cannot tell
//! the two terminals apart, but this event-driven path carries the
//! truth — the #568/#570 boundary).
//!
//! # Module boundary
//!
//! The observer's run loop owns the channel receiver (the same shape as
//! [`crate::observer::task_narrator`]'s receiver — a tokio mpsc on the
//! `select!` loop); this module owns only the event→line projection.
//!
//! # De-dup with [`crate::observer::task_narrator`] / [`crate::run_narrator`]
//!
//! - [`crate::observer::task_narrator::ObserverTaskNarrator`] narrates
//!   per-TASK transitions (assign / complete / fail / non-terminal), via
//!   the `TaskStateChangeEvent` channel. This narrator is its F5 sibling
//!   — a different apply seam (the custom-message inbox, not
//!   `merge_task_state`), a different event type, a different channel —
//!   so the two never double-emit.
//! - [`crate::run_narrator::RunNarrator`] narrates phase / setup /
//!   retry-pass / failover / `CustomMessagePosted` landing-edge lines
//!   (#508/#513/#333/#568) but is DELIBERATELY silent on the F5
//!   TERMINALS (`CustomMessageHandled` / `CustomMessageFailed`) — the
//!   `custom_message_terminals_are_silent_in_state_narrator` pin in
//!   `run_narrator.rs` documents that silence as the #570 hand-off. This
//!   narrator is exactly the event-driven follow-up that pin points to.

use dynrunner_core::narrate_routed;

use crate::custom_message_outcome::{CustomMessageOutcome, CustomMessageOutcomeEvent};

/// Per-custom-message-terminal operator narrator. Holds NO state —
/// every field rides the event, so each call is a pure projection. The
/// observer's run loop drains the channel and calls
/// [`Self::narrate_live`] per event.
#[derive(Debug, Default)]
pub(crate) struct ObserverCustomMessageOutcomeNarrator;

impl ObserverCustomMessageOutcomeNarrator {
    /// Narrate ONE custom-message terminal apply. Emits a single line
    /// at the spec-fixed level (INFO for Handled, ERROR for Failed).
    /// Returns whether a line was emitted — the caller's wake-stream
    /// piggyback seam (a narrated terminal is a wake-stream HOST,
    /// exactly like
    /// [`crate::observer::task_narrator::ObserverTaskNarrator::narrate_live`]'s
    /// return). Today every event narrates, so the return is always
    /// `true`; the bool is kept for shape-parity with the sibling
    /// narrator so the caller's `if narrated { flush_after_host() }`
    /// idiom uniforms across both arms.
    pub(crate) fn narrate_live(&self, event: &CustomMessageOutcomeEvent) -> bool {
        let origin = &event.origin;
        let seq = event.seq;
        // Operator-narration volume class (#583/#587). The consumer
        // chose this class at send time on
        // `SecondaryHandle.send_to_primary`; the apply rules captured
        // it from the `Unhandled` entry BEFORE the terminal latch
        // dropped the payload, so the Handled / Failed wake line
        // routes to the SAME target as the originating Posted line
        // (no narration-target divergence within a single message).
        // The wake signal in the high-volume mode is the sibling
        // `observer::failure_response::custom_message_activity`
        // aggregator emitting "custom message activity (aggregated,
        // last 60s)" on `IMPORTANT_TARGET`. The `narrate_routed!`
        // macro owns the runtime target branch.
        match &event.outcome {
            CustomMessageOutcome::Handled => {
                narrate_routed!(
                    info,
                    event.is_high_volume,
                    origin = %origin,
                    seq = seq,
                    "custom message handled: from {origin} (seq {seq})",
                );
            }
            CustomMessageOutcome::Failed { reason } => {
                narrate_routed!(
                    error,
                    event.is_high_volume,
                    origin = %origin,
                    seq = seq,
                    reason = %reason,
                    "custom message handler FAILED (raised): from {origin} \
                     (seq {seq}) — {reason} — result discarded, no task \
                     mutations applied",
                );
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_capture::TargetCapture;
    use dynrunner_core::{IMPORTANT_TARGET, OBSERVER_TASK_TARGET};
    use tracing::subscriber::with_default;
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;

    /// Drive `body` with a [`TargetCapture`] installed on a chosen
    /// `target` so the emitted lines + their level are recorded.
    /// Mirrors `run_narrator.rs::tests::capture`'s shape, but uses
    /// `TargetCapture` (level-preserving) rather than
    /// `ImportantCapture` (level-erased) because this narrator's
    /// invariant is "Handled → INFO, Failed → ERROR".
    fn capture_with_level(
        target: &'static str,
        body: impl FnOnce(),
    ) -> Vec<crate::test_capture::LeveledEvent> {
        let cap = TargetCapture::for_target(target);
        let subscriber = Registry::default().with(cap.clone());
        with_default(subscriber, body);
        cap.events()
    }

    #[test]
    fn handled_narrates_info_with_origin_and_seq() {
        let events = capture_with_level(IMPORTANT_TARGET, || {
            let narrator = ObserverCustomMessageOutcomeNarrator;
            assert!(narrator.narrate_live(&CustomMessageOutcomeEvent {
                origin: "sec-a".into(),
                seq: 7,
                outcome: CustomMessageOutcome::Handled,
                is_high_volume: false,
            }));
        });
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.level, tracing::Level::INFO);
        assert_eq!(
            e.event.message, "custom message handled: from sec-a (seq 7)",
            "Handled emits the spec-fixed INFO line verbatim",
        );
    }

    #[test]
    fn failed_narrates_error_with_verbatim_reason() {
        let events = capture_with_level(IMPORTANT_TARGET, || {
            let narrator = ObserverCustomMessageOutcomeNarrator;
            assert!(narrator.narrate_live(&CustomMessageOutcomeEvent {
                origin: "sec-b".into(),
                seq: 3,
                outcome: CustomMessageOutcome::Failed {
                    reason: "boom in handler stage 2".into(),
                },
                is_high_volume: false,
            }));
        });
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.level, tracing::Level::ERROR);
        assert_eq!(
            e.event.message,
            "custom message handler FAILED (raised): from sec-b (seq 3) \
             — boom in handler stage 2 — result discarded, no task mutations applied",
            "Failed emits the spec-fixed ERROR line with verbatim reason",
        );
    }

    /// An empty reason (a legacy-encoded `CustomMessageFailed` frame
    /// decoded via the wire field's `#[serde(default,
    /// skip_serializing_if = "String::is_empty")]`) STILL narrates a
    /// failure ERROR — just with an empty reason in the rendered text.
    /// The legacy path is degraded (no verbatim reason), never silent.
    #[test]
    fn failed_with_empty_reason_still_narrates_error() {
        let events = capture_with_level(IMPORTANT_TARGET, || {
            let narrator = ObserverCustomMessageOutcomeNarrator;
            narrator.narrate_live(&CustomMessageOutcomeEvent {
                origin: "sec-c".into(),
                seq: 1,
                outcome: CustomMessageOutcome::Failed { reason: String::new() },
                is_high_volume: false,
            });
        });
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, tracing::Level::ERROR);
        assert!(
            events[0].event.message.starts_with("custom message handler FAILED"),
            "empty reason still narrates ERROR: {events:?}",
        );
    }

    /// T1 (#583): a Handled event whose `is_high_volume=true` narrates
    /// the spec-fixed INFO line, but the emit goes to
    /// `OBSERVER_TASK_TARGET` — NOT `IMPORTANT_TARGET` — so the
    /// `--important-stdio-only` stdio gate suppresses the per-event
    /// line. The wake signal at scale is the aggregator rollup, not
    /// this line.
    #[test]
    fn handled_high_volume_routes_to_observer_task_target_not_important() {
        let on_observer = capture_with_level(OBSERVER_TASK_TARGET, || {
            let narrator = ObserverCustomMessageOutcomeNarrator;
            narrator.narrate_live(&CustomMessageOutcomeEvent {
                origin: "sec-a".into(),
                seq: 11,
                outcome: CustomMessageOutcome::Handled,
                is_high_volume: true,
            });
        });
        assert_eq!(on_observer.len(), 1, "high-volume Handled lands on OBSERVER_TASK_TARGET");
        assert_eq!(on_observer[0].level, tracing::Level::INFO);
        let on_important = capture_with_level(IMPORTANT_TARGET, || {
            let narrator = ObserverCustomMessageOutcomeNarrator;
            narrator.narrate_live(&CustomMessageOutcomeEvent {
                origin: "sec-a".into(),
                seq: 12,
                outcome: CustomMessageOutcome::Handled,
                is_high_volume: true,
            });
        });
        assert!(
            on_important.is_empty(),
            "high-volume Handled MUST NOT emit on IMPORTANT_TARGET: {on_important:?}",
        );
    }

    /// T3 (#587 sibling for custom-message Failed): the Failed ERROR
    /// arm honors the same routing — a consumer-flagged high-volume
    /// terminal failure goes to `OBSERVER_TASK_TARGET`, not
    /// `IMPORTANT_TARGET`. The wake signal is the aggregator rollup.
    #[test]
    fn failed_high_volume_routes_to_observer_task_target_not_important() {
        let on_observer = capture_with_level(OBSERVER_TASK_TARGET, || {
            let narrator = ObserverCustomMessageOutcomeNarrator;
            narrator.narrate_live(&CustomMessageOutcomeEvent {
                origin: "sec-b".into(),
                seq: 5,
                outcome: CustomMessageOutcome::Failed { reason: "boom".into() },
                is_high_volume: true,
            });
        });
        assert_eq!(on_observer.len(), 1);
        assert_eq!(on_observer[0].level, tracing::Level::ERROR);
        let on_important = capture_with_level(IMPORTANT_TARGET, || {
            let narrator = ObserverCustomMessageOutcomeNarrator;
            narrator.narrate_live(&CustomMessageOutcomeEvent {
                origin: "sec-b".into(),
                seq: 6,
                outcome: CustomMessageOutcome::Failed { reason: "boom".into() },
                is_high_volume: true,
            });
        });
        assert!(
            on_important.is_empty(),
            "high-volume Failed MUST NOT emit on IMPORTANT_TARGET: {on_important:?}",
        );
    }

    /// T2 (#583): a Handled event with `is_high_volume=false` (the
    /// default — low-fanout consumer) stays on `IMPORTANT_TARGET`
    /// (the regression guard — the new flag must not flip normal
    /// custom-message routing).
    #[test]
    fn handled_low_volume_stays_on_important_target() {
        let on_important = capture_with_level(IMPORTANT_TARGET, || {
            let narrator = ObserverCustomMessageOutcomeNarrator;
            narrator.narrate_live(&CustomMessageOutcomeEvent {
                origin: "sec-c".into(),
                seq: 1,
                outcome: CustomMessageOutcome::Handled,
                is_high_volume: false,
            });
        });
        assert_eq!(on_important.len(), 1);
        let on_observer = capture_with_level(OBSERVER_TASK_TARGET, || {
            let narrator = ObserverCustomMessageOutcomeNarrator;
            narrator.narrate_live(&CustomMessageOutcomeEvent {
                origin: "sec-c".into(),
                seq: 2,
                outcome: CustomMessageOutcome::Handled,
                is_high_volume: false,
            });
        });
        assert!(
            on_observer.is_empty(),
            "low-volume Handled MUST NOT emit on OBSERVER_TASK_TARGET: {on_observer:?}",
        );
    }
}
