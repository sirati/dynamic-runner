//! Observer lost-visibility reporter.
//!
//! # Single concern
//!
//! ONE concern: track whether the zero-authority observer currently has
//! visibility into the run (any peer reachable, the named primary not
//! silent), emit operator-facing reports when that visibility is LOST,
//! recurs while still lost, or is REGAINED, AND tell the coordinator when
//! a reconnect ATTEMPT is due (the same ~60s cadence as the recurrence
//! report — one clock, one concern). It NEVER decides an exit — a
//! lost-visibility observer keeps observing, reporting + retrying, until
//! it OBSERVES the primary's run-terminal via the CRDT.
//!
//! This module owns the report/retry CADENCE (the lost-since clock + the
//! "is an attempt due" decision); it does NOT own the reconnect MECHANISM
//! — the coordinator acts on the returned [`RetryDirective`] by triggering
//! the [`super::reconnect::TunnelReconnector`] port (which the provider
//! layer rebuilds the `-R` tunnel behind). The split keeps the cadence
//! state in one place and the ssh-rebuild concern out of this state
//! machine entirely.
//!
//! # Why this exists (the BUG-B sever)
//!
//! Before this, the observer's run loop treated `peer_count() == 0` for
//! `fleet_dead_timeout` (§5) and a named-primary silence past
//! `peer_timeout` (§6) as a STRAND that returned `Err(ClusterCollapsed)`
//! — which the node-run outcome mapped to a RUN-FAILING terminal. But the
//! observer carries ZERO authority over the job: its OWN loss of transport
//! view says "I (observer) lost visibility", NOT "the cluster died". The
//! compute mesh (primary + secondaries, directly meshed) keeps running
//! autonomously. So visibility loss can never be the run's verdict. This
//! reporter REPLACES the §5/§6 strand-exit: it reports lost + retries, the
//! observer never self-strands.
//!
//! # Why the observer must DRIVE the reconnect (the `-R` rebuild)
//!
//! The relocated submitter→observer inherits the submitter's
//! [`dynrunner_transport_tunnel::TunneledPeerTransport`]: the compute peers
//! DIAL the submitter over per-secondary `ssh -R` reverse tunnels, the
//! submitter never dials out, and that transport has NO QUIC reconnect
//! ticker (the ticker lives only on the secondary/late-joiner
//! `PeerNetwork`). So when a `-R` tunnel drops (an external ssh blip /
//! `ServerAliveCountMax` exhaustion — there is no auto-reconnect on the ssh
//! side), NOTHING re-establishes the link on its own: the compute peer's
//! redial can't punch through a dead `-R`, and the observer's transport
//! cannot dial. The observer MUST actively trigger a `-R` rebuild. This
//! reporter therefore tells the coordinator when an attempt is due (the
//! returned [`RetryDirective`]); the coordinator fires the
//! [`super::reconnect::TunnelReconnector`] port.
//!
//! # Boundary
//!
//! The coordinator owns the liveness inputs (its `MembershipView`
//! peer-count + its `primary_last_seen` clock against the CRDT-named
//! primary) and the run-loop tick that drives the recurrence cadence. Each
//! top-of-loop it hands this reporter the CURRENT visibility verdict; the
//! reporter owns only the report-state machine (lost-since clock, last
//! report instant, the operator-facing emits) and returns whether a
//! reconnect attempt is due this iteration. It does NOT name a transport,
//! drive a dial, or know about ssh — the coordinator acts on the returned
//! directive by triggering the reconnect port.

use std::time::{Duration, Instant};

use dynrunner_core::IMPORTANT_TARGET;

/// How long to wait between repeated "still disconnected, still retrying"
/// reports while visibility stays lost. The owner directive: "if all
/// connection is lost they do not shut down, they report that connection
/// is lost and try reconnecting after a minute."
const REPORT_RECURRENCE: Duration = Duration::from_secs(60);

/// The current visibility the coordinator observes each loop iteration.
///
/// `Visible` means the observer can see the run (a peer is reachable and,
/// if a primary is named, it is not silent past the threshold). `Lost`
/// carries a human reason for the operator log (which signal dropped).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Visibility {
    /// The observer currently sees the run.
    Visible,
    /// The observer currently cannot see the run; `reason` names which
    /// signal dropped (fleet empty / named-primary silent) for the log.
    Lost { reason: String },
}

/// What the coordinator should do this iteration, returned from
/// [`LostVisibilityReporter::observe`]. The reporter owns the cadence
/// (lost-since clock + the ~60s recurrence), the coordinator owns the
/// action (trigger the reconnect port). NEVER an exit — visibility loss
/// is not a run verdict (BUG-B).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDirective {
    /// Visibility is fine (or just regained) — nothing to do.
    Idle,
    /// Visibility is currently lost AND a reconnect attempt is due this
    /// iteration (the first lost loop, then once per recurrence interval).
    /// The coordinator should trigger the [`super::reconnect::TunnelReconnector`]
    /// with its current roster. The same cadence as the recurrence report,
    /// so a single clock drives both "remind the operator" and "retry the
    /// tunnel".
    ReconnectDue,
    /// Visibility is currently lost but no attempt is due yet (the
    /// inter-recurrence wait) — keep observing, do not re-fire the rebuild.
    WaitingToRetry,
}

/// Report-state machine for the observer's connection visibility.
///
/// Single writer (the coordinator's run loop, single-threaded LocalSet),
/// so no synchronisation. It owns ONLY the report cadence; it carries no
/// transport, no exit decision, no authority.
#[derive(Debug, Default)]
pub struct LostVisibilityReporter {
    /// `Some(t)` while visibility is currently lost — the instant the
    /// CURRENT loss episode began. `None` while visible.
    lost_since: Option<Instant>,
    /// The last instant a "connection lost / still retrying" report was
    /// emitted, so recurrence fires at most once per [`REPORT_RECURRENCE`].
    last_report: Option<Instant>,
}

impl LostVisibilityReporter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed the current visibility verdict and learn what to do this
    /// iteration. On the FIRST loop where visibility is lost, emit the
    /// "connection lost — observer continues passively, retrying reconnect"
    /// report AND return [`RetryDirective::ReconnectDue`]; while still
    /// lost, re-emit + signal a fresh attempt at most once per
    /// [`REPORT_RECURRENCE`] (otherwise [`RetryDirective::WaitingToRetry`]);
    /// on the loop visibility is REGAINED, emit a "reconnected" report,
    /// clear the loss state, and return [`RetryDirective::Idle`].
    ///
    /// NEVER returns an exit signal — a lost-visibility observer keeps
    /// observing. This is the entire BUG-B contract: the observer reports,
    /// retries the tunnel rebuild, and does not reap the run. The single
    /// `due` decision drives BOTH the operator report and the reconnect
    /// attempt, so they share one ~60s clock.
    pub fn observe(&mut self, visibility: &Visibility) -> RetryDirective {
        match visibility {
            Visibility::Visible => {
                if let Some(since) = self.lost_since.take() {
                    tracing::warn!(
                        target: IMPORTANT_TARGET,
                        lost_secs = since.elapsed().as_secs(),
                        "observer reconnected — visibility regained; resuming passive \
                         observation of the run"
                    );
                }
                self.last_report = None;
                RetryDirective::Idle
            }
            Visibility::Lost { reason } => {
                let now = Instant::now();
                let first = self.lost_since.is_none();
                self.lost_since.get_or_insert(now);
                let due = first
                    || self
                        .last_report
                        .is_none_or(|last| now.duration_since(last) >= REPORT_RECURRENCE);
                if due {
                    let since = self.lost_since.expect("set above");
                    tracing::warn!(
                        target: IMPORTANT_TARGET,
                        %reason,
                        lost_secs = since.elapsed().as_secs(),
                        "observer lost connection to the run — this does NOT mean the cluster \
                         died (the compute mesh runs autonomously over its direct links); the \
                         observer carries zero authority and stays a passive monitor, rebuilding \
                         its tunnel + retrying reconnect (~60s cadence). The run's verdict comes \
                         from the primary, never from the observer's own view."
                    );
                    self.last_report = Some(now);
                    RetryDirective::ReconnectDue
                } else {
                    RetryDirective::WaitingToRetry
                }
            }
        }
    }

    /// Whether the reporter currently considers visibility lost. Test/
    /// diagnostic accessor — not part of any exit decision.
    #[cfg(test)]
    pub fn is_lost(&self) -> bool {
        self.lost_since.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lost(reason: &str) -> Visibility {
        Visibility::Lost {
            reason: reason.to_string(),
        }
    }

    #[test]
    fn observe_never_signals_exit_while_lost() {
        // The contract is that `observe` returns a `RetryDirective` (Idle /
        // ReconnectDue / WaitingToRetry) — there is NO variant by which a
        // lost-visibility observer is told to exit. This test pins the
        // type-level guarantee: feeding repeated losses only updates
        // internal report state + asks for reconnects, never a terminal.
        let mut r = LostVisibilityReporter::new();
        let d = r.observe(&lost("fleet empty"));
        assert!(r.is_lost());
        assert_eq!(
            d,
            RetryDirective::ReconnectDue,
            "the first lost loop must request a reconnect attempt"
        );
        // The immediately-following lost loop is inside the recurrence
        // window, so no fresh attempt is due — but the observer keeps
        // observing (no exit variant exists).
        let d2 = r.observe(&lost("fleet empty"));
        assert!(r.is_lost(), "still lost — observer keeps observing, no exit");
        assert_eq!(
            d2,
            RetryDirective::WaitingToRetry,
            "a second lost loop within the recurrence window waits, does not re-fire"
        );
    }

    #[test]
    fn first_loss_requests_reconnect() {
        // The owner directive: a lost observer must TRY to rebuild its
        // tunnel + reconnect. The first lost observation surfaces the
        // ReconnectDue directive the coordinator acts on (trigger the
        // TunnelReconnector). This is the unit pin of "the observer drives
        // the reconnect", independent of the wall-clock recurrence.
        let mut r = LostVisibilityReporter::new();
        assert_eq!(
            r.observe(&lost("no reachable peer")),
            RetryDirective::ReconnectDue,
        );
    }

    #[test]
    fn regaining_visibility_clears_loss_state() {
        let mut r = LostVisibilityReporter::new();
        assert_eq!(
            r.observe(&lost("primary silent")),
            RetryDirective::ReconnectDue
        );
        assert!(r.is_lost());
        assert_eq!(r.observe(&Visibility::Visible), RetryDirective::Idle);
        assert!(!r.is_lost(), "regained visibility clears the loss episode");
    }

    #[test]
    fn visible_throughout_stays_clear() {
        let mut r = LostVisibilityReporter::new();
        assert_eq!(r.observe(&Visibility::Visible), RetryDirective::Idle);
        assert_eq!(r.observe(&Visibility::Visible), RetryDirective::Idle);
        assert!(!r.is_lost());
    }
}
