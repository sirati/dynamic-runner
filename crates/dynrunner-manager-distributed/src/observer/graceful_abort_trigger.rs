//! The operator's SIGUSR2 graceful-abort trigger.
//!
//! # Single concern
//!
//! Own the process lifecycle of the SIGUSR2 operator channel: install the
//! handler (which takes the signal off its default terminate-the-process
//! disposition), latch deliveries, and hand them to the ONE consumer — the
//! observer run loop's graceful-abort arm. There is exactly one
//! `SignalKind::user_defined2()` stream per process, owned here; nobody
//! else may create one (two listeners on one signal would race the latch).
//!
//! # Why arming is split from consumption
//!
//! The handler used to be installed inside the observer run loop, so every
//! moment between process start and the observer seating left SIGUSR2 on
//! its kernel default — terminate. A late-joiner that received the
//! CLI-documented graceful-abort signal during its bootstrap window (after
//! transport bring-up, before `join_running_cluster` returned) died
//! instantly with zero narration. [`GracefulAbortTrigger::arm`] at process
//! entry makes that window survivable: the tokio signal stream LATCHES a
//! delivery received while nobody is consuming, so a pre-seat request is
//! picked up by the run loop's first poll exactly like a post-seat one.
//!
//! # Module boundary
//!
//! * [`GracefulAbortTrigger::arm`] — create the trigger (and install the
//!   handler) as early as a tokio runtime exists, BEFORE any bootstrap
//!   step that can block. Requires an active runtime with IO/signal
//!   drivers enabled.
//! * `ObserverCoordinator::set_graceful_abort_trigger` — inject the
//!   pre-armed trigger; an un-injected coordinator arms at run start (the
//!   behaviour every non-late-joiner path keeps).
//! * [`GracefulAbortTrigger::recv`] — the run loop's cancel-safe
//!   consumption arm.
//! * [`GracefulAbortTrigger::report_undelivered`] — the failed-bootstrap
//!   exit path: narrate a latched-but-undeliverable abort intent on the
//!   operator wake stream so it is never silently lost.

use dynrunner_core::IMPORTANT_TARGET;

/// The single owner of the process's SIGUSR2 stream. See the module
/// header for the arm-early / consume-late split.
pub struct GracefulAbortTrigger {
    /// `None` after a registration failure (exotic runtimes — degrades to
    /// a parked arm; an embedding driver can still call
    /// `request_graceful_abort` directly) or once the stream closed.
    stream: Option<tokio::signal::unix::Signal>,
}

impl GracefulAbortTrigger {
    /// Install the SIGUSR2 handler NOW and return the trigger that owns
    /// the stream. From this call on, a SIGUSR2 cannot kill the process:
    /// a delivery with no active `recv` is latched by the stream and
    /// consumed by the first poll.
    ///
    /// Registration failure (exotic runtimes) degrades to a parked arm —
    /// the embedding driver can still call `request_graceful_abort`
    /// directly.
    pub fn arm() -> Self {
        let stream =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined2())
                .map_err(|e| {
                    tracing::warn!(
                        error = %e,
                        "SIGUSR2 graceful-abort trigger could not be \
                         registered; the signal channel is disabled for \
                         this observer"
                    );
                })
                .ok();
        Self { stream }
    }

    /// Await the next graceful-abort delivery. `Some(())` per delivery
    /// (coalesced by the kernel/tokio while unconsumed); `None` exactly
    /// once if the signal stream closes, after which — and on a trigger
    /// whose registration failed — this parks forever (never a hot loop).
    ///
    /// Cancel-safety: `Signal::recv` is cancel-safe (tokio docs); a
    /// sibling select arm winning drops and rebuilds the recv future
    /// without losing a queued signal. The close-latch below only runs
    /// after a completed recv, so cancellation cannot corrupt state.
    pub async fn recv(&mut self) -> Option<()> {
        match self.stream.as_mut() {
            Some(stream) => {
                let sig = stream.recv().await;
                if sig.is_none() {
                    // Stream closed: park every later call.
                    self.stream = None;
                }
                sig
            }
            None => std::future::pending().await,
        }
    }

    /// Non-blocking probe-and-consume of a latched delivery. Used only by
    /// the failed-bootstrap exit path (the latch is consumed — fine,
    /// the process is exiting).
    fn take_buffered(&mut self) -> bool {
        let Some(stream) = self.stream.as_mut() else {
            return false;
        };
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        matches!(
            stream.poll_recv(&mut cx),
            std::task::Poll::Ready(Some(()))
        )
    }

    /// Failed-bootstrap exit path: if a graceful abort was latched while
    /// the trigger was still waiting for its consumer (the bootstrap
    /// never seated the observer), narrate it on the operator wake
    /// stream so the intent is never silently lost. Returns whether a
    /// latched intent was found (and narrated). Consumes the trigger —
    /// the process is exiting.
    ///
    /// Async because of the driver flush: a signal delivered moments ago
    /// may still sit in the signal driver's wake pipe (the OS handler
    /// ran, but the driver only propagates into the stream when the
    /// runtime parks). One short sleep parks the runtime so the driver
    /// flushes before the probe — best-effort hardening of an
    /// already-failed exit path.
    pub async fn report_undelivered(mut self) -> bool {
        if self.stream.is_some() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let buffered = self.take_buffered();
        if buffered {
            emit_undelivered_abort_narration();
        }
        buffered
    }
}

/// The single emit site for the undeliverable-abort narration — split out
/// so the importance-channel test asserts the exact message through the
/// shared capture layer without needing a real signal.
pub(crate) fn emit_undelivered_abort_narration() {
    tracing::warn!(
        target: IMPORTANT_TARGET,
        "graceful abort was requested during bootstrap; bootstrap did not \
         complete; exiting without delivery"
    );
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::test_capture::capture_important;

    /// The undeliverable-abort narration rides the importance marker with
    /// the exact operator-readable message.
    #[test]
    fn undelivered_narration_is_important_and_exact() {
        let events = capture_important(emit_undelivered_abort_narration);
        assert_eq!(events.len(), 1, "exactly one important event");
        assert_eq!(
            events[0].message,
            "graceful abort was requested during bootstrap; bootstrap did \
             not complete; exiting without delivery"
        );
    }

    /// A trigger whose registration failed parks `recv` forever (never a
    /// hot loop, never an error) — the degraded-arm contract the run
    /// loop's select relies on.
    #[tokio::test]
    async fn disabled_trigger_parks_recv() {
        let mut trigger = GracefulAbortTrigger { stream: None };
        let parked =
            tokio::time::timeout(Duration::from_millis(50), trigger.recv()).await;
        assert!(parked.is_err(), "recv on a disabled trigger must park");
    }

    /// A disabled trigger has nothing buffered: the failed-bootstrap exit
    /// narrates nothing.
    #[tokio::test]
    async fn disabled_trigger_reports_nothing_undelivered() {
        let trigger = GracefulAbortTrigger { stream: None };
        assert!(!trigger.report_undelivered().await);
    }
}
