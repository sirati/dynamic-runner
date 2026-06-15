//! The operator's SIGUSR1 force-print trigger (observer-only).
//!
//! # Single concern
//!
//! Own the process lifecycle of the SIGUSR1 operator channel: install the
//! handler (which takes the signal off its default terminate-the-process
//! disposition), latch deliveries, and hand them to the ONE active
//! observer reporter task. There is exactly one
//! `SignalKind::user_defined1()` stream per process, owned here; nobody
//! else may create one (two listeners on one signal would race the latch).
//!
//! # Why this exists
//!
//! The 10-minute periodic stats report skips a tick when the only counters
//! that moved since the last announcement are `succeeded` /
//! `fail_{retry,oom,final}` (routine throughput). The 1-hour safety net
//! still prints, but an operator who wants a status read NOW — e.g. while
//! debugging a hang — needs an off-cadence trigger. SIGUSR1 against the
//! observer process force-prints the FULL snapshot (every field, including
//! unchanged ones) on the importance channel and advances the
//! last-announced baseline like a normal emission.
//!
//! # Why arming is split from consumption
//!
//! The observer's reporter task is spawned only after the bootstrap
//! rendezvous + cluster-state restore. A SIGUSR1 received during that
//! window would land on the kernel's default disposition (terminate) and
//! kill the late-joiner with zero narration. [`ForcePrintTrigger::arm`] at
//! process entry installs the handler immediately, so the tokio signal
//! stream LATCHES a pre-seat delivery; the reporter's first poll after
//! seating consumes it exactly like a post-seat one.
//!
//! # Module boundary
//!
//! * [`ForcePrintTrigger::arm`] — create the trigger (and install the
//!   handler) as early as a tokio runtime exists, BEFORE any bootstrap
//!   step that can block. Requires an active runtime with IO/signal
//!   drivers enabled.
//! * `ObserverCoordinator::set_force_print_trigger` — inject the
//!   pre-armed trigger into the run loop that will consume it. An
//!   un-injected observer arms at run start (the cold-join behaviour).
//! * [`ForcePrintTrigger::recv`] — the run loop's cancel-safe
//!   consumption arm.
//!
//! # Why observer-only (no primary / secondary consumer)
//!
//! The framework also documents `kill -USR1` against a SECONDARY for an
//! all-thread Python frame dump via `faulthandler.register(SIGUSR1, …)`
//! — that handler is installed by every dispatch route through
//! `python/dynamic_runner/run.py::dispatch`. The observer's dispatch path
//! SKIPS that registration (`register_sigusr1=False`), so the signal is
//! free for THIS trigger on observer processes only. Sending SIGUSR1 to a
//! primary/secondary still dumps frames as before; only the observer
//! repurposes it to a force-print.

use dynrunner_core::IMPORTANT_TARGET;

/// The single owner of the process's SIGUSR1 stream (observer mode). See
/// the module header for the arm-early / consume-late split and for why
/// this trigger is observer-only.
pub struct ForcePrintTrigger {
    /// `None` after a registration failure (exotic runtimes — degrades to
    /// a parked arm; the reporter still drives the periodic cadences) or
    /// once the stream closed.
    stream: Option<tokio::signal::unix::Signal>,
}

impl ForcePrintTrigger {
    /// Install the SIGUSR1 handler NOW and return the trigger that owns
    /// the stream. From this call on, a SIGUSR1 cannot kill the process:
    /// a delivery with no active `recv` is latched by the stream and
    /// consumed by the first poll.
    ///
    /// Registration failure (exotic runtimes) degrades to a parked arm —
    /// the periodic cadences still run; the operator simply has no
    /// off-grid force-print channel.
    pub fn arm() -> Self {
        let stream =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())
                .map_err(|e| {
                    tracing::warn!(
                        target: IMPORTANT_TARGET,
                        error = %e,
                        "SIGUSR1 force-print trigger could not be \
                         registered; the signal channel is disabled for \
                         this observer"
                    );
                })
                .ok();
        Self { stream }
    }

    /// Await the next force-print delivery. `Some(())` per delivery
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
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// A trigger whose registration failed parks `recv` forever (never a
    /// hot loop, never an error) — the degraded-arm contract the run
    /// loop's select relies on.
    #[tokio::test]
    async fn disabled_trigger_parks_recv() {
        let mut trigger = ForcePrintTrigger { stream: None };
        let parked =
            tokio::time::timeout(Duration::from_millis(50), trigger.recv()).await;
        assert!(parked.is_err(), "recv on a disabled trigger must park");
    }
}
