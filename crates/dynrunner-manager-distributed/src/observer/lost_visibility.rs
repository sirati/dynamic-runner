//! Observer lost-visibility reporter.
//!
//! # Single concern
//!
//! ONE concern: track whether the zero-authority observer currently has
//! visibility into the run (any peer reachable, the named primary not
//! silent) and emit operator-facing reports when that visibility is LOST,
//! recurs while still lost, or is REGAINED. It NEVER decides an exit — a
//! lost-visibility observer keeps observing (the existing transport
//! reconnect ticker redials the wire underneath it), reporting that it is
//! retrying, until it OBSERVES the primary's run-terminal via the CRDT.
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
//! autonomously; the by-design `-R` setup-tunnel drop after relocation
//! must cause no hiccup. So visibility loss can never be the run's verdict.
//! This reporter REPLACES the §5/§6 strand-exit: it reports lost + retries,
//! the observer never self-strands.
//!
//! # Boundary
//!
//! The coordinator owns the liveness inputs (its `MembershipView`
//! peer-count + its `primary_last_seen` clock against the CRDT-named
//! primary) and the run-loop tick that drives the recurrence cadence. Each
//! top-of-loop it hands this reporter the CURRENT visibility verdict; the
//! reporter owns only the report-state machine (lost-since clock, last
//! report instant, the operator-facing emits). The actual wire reconnect
//! is the transport's role-blind concern (`transport-quic` reconnect
//! ticker); this reporter narrates that retry, it does not drive the dial.

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

    /// Feed the current visibility verdict. On the FIRST loop where
    /// visibility is lost, emit the "connection lost — observer continues
    /// passively, retrying reconnect" report; while still lost, re-emit at
    /// most once per [`REPORT_RECURRENCE`]; on the loop visibility is
    /// REGAINED, emit a "reconnected" report and clear the loss state.
    ///
    /// NEVER returns an exit signal — a lost-visibility observer keeps
    /// observing. This is the entire BUG-B contract: the observer reports
    /// and retries, it does not reap the run.
    pub fn observe(&mut self, visibility: &Visibility) {
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
                         observer carries zero authority and stays a passive monitor, retrying \
                         reconnect (~60s cadence). The run's verdict comes from the primary, \
                         never from the observer's own view."
                    );
                    self.last_report = Some(now);
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
        // The contract is purely that `observe` returns `()` — there is NO
        // path by which a lost-visibility observer is told to exit. This
        // test pins the type-level guarantee: feeding repeated losses only
        // updates internal report state, never yields a terminal.
        let mut r = LostVisibilityReporter::new();
        r.observe(&lost("fleet empty"));
        assert!(r.is_lost());
        r.observe(&lost("fleet empty"));
        assert!(r.is_lost(), "still lost — observer keeps observing, no exit");
    }

    #[test]
    fn regaining_visibility_clears_loss_state() {
        let mut r = LostVisibilityReporter::new();
        r.observe(&lost("primary silent"));
        assert!(r.is_lost());
        r.observe(&Visibility::Visible);
        assert!(!r.is_lost(), "regained visibility clears the loss episode");
    }

    #[test]
    fn visible_throughout_stays_clear() {
        let mut r = LostVisibilityReporter::new();
        r.observe(&Visibility::Visible);
        r.observe(&Visibility::Visible);
        assert!(!r.is_lost());
    }
}
