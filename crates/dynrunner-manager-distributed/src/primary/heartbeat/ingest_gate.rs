//! [`IngestEdgeGate`] — the decider-health guard for staleness-based
//! removals: defer while the node's own ingest path is provably
//! backlogged.
//!
//! # Concern
//!
//! ONE question, answered once per heartbeat sweep: "is THIS node
//! moving inbound frames from its transport to its delivery edge right
//! now?" If not, every staleness reading the sweep consumes is suspect
//! — a silent-looking peer's keepalives may sit unattributed in the
//! same backed-up queue — so the sweep must author no removal.
//!
//! # The signal (arrival-vs-drained pending persistence)
//!
//! The transport publishes two per-peer clocks bracketing its inbound
//! queue ([`IngestEdges`]): ARRIVAL (stamped by the connection read
//! loops, which keep running while the consumer starves) and DRAINED
//! (stamped as `recv_peer` pulls frames back out). For a peer whose
//! `arrival > drained`, an undrained frame provably sits in the queue.
//! One pending frame is normal (frames are always momentarily in
//! flight on a busy mesh); the gate therefore keys on PERSISTENCE:
//! a peer stays "pending" across sweeps only while its drained clock
//! does not advance — any drain progress for that peer clears it (the
//! pump is moving its frames; staleness readings are at most one
//! queue-transit old, which the arrival-clock union already covers).
//! The gate trips when some peer has been continuously
//! pending-without-drain-progress longer than the caller's threshold
//! (the same `SWEEP_STARVATION_TICK_MULTIPLE` × keepalive-interval
//! budget the sweep's own tick-lag guard uses — far below the hard
//! death backstop, so a healthy node never feels it).
//!
//! # Relation to the sibling guards (defense-in-depth layers)
//!
//! - the TICK-LAG guard (`local_sweep_starved`) catches a starved
//!   OPERATIONAL loop — the sweep itself ran late;
//! - the ARRIVAL-CLOCK union in `collect_heartbeat_report` keeps
//!   silence ages honest for peers whose frames the read loops could
//!   attribute;
//! - THIS gate catches the residue: a sweep running on cadence while
//!   the MESH PUMP lags, where a buried peer's frames may be in the
//!   backlog without an attributable arrival stamp yet.
//!
//! A permanently-dead pump therefore defers removals indefinitely —
//! deliberately: a node that cannot ingest must not adjudicate other
//! nodes' liveness (the honest-liveness law), and the throttled WARN
//! names the condition for the operator every minute.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::IngestEdges;

use crate::warn_throttle::WarnThrottle;

/// Minimum spacing between two deferral WARNs. The gate re-detects the
/// same backlog on every heartbeat sweep (5s cadence in production); a
/// minute-cadence WARN with a suppressed count keeps the outage
/// narrated without one line per tick.
const DEFER_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// Sweep-side tracker of arrival-vs-drained pending persistence, plus
/// the deferral verdict + its operator narration. Owned by the
/// `PrimaryCoordinator`; fed once per heartbeat sweep via
/// [`IngestEdgeGate::observe`]; consulted between sweeps (the
/// dispatch-altitude silent-set read) via [`IngestEdgeGate::deferring`].
pub(in crate::primary) struct IngestEdgeGate {
    /// Each peer's drained stamp as of the previous sweep — the
    /// progress baseline: a newer stamp this sweep means the pump moved
    /// that peer's frames since.
    prev_drained: HashMap<String, Instant>,
    /// When each currently-pending peer's pending-without-drain-progress
    /// streak started. Inserted on first pending observation, removed
    /// on drain progress or queue-empty.
    pending_since: HashMap<String, Instant>,
    /// The current verdict: `Some(longest pending streak)` while the
    /// gate defers staleness-based removals, `None` while healthy.
    /// Refreshed by [`Self::observe`]; at most one sweep stale for the
    /// between-sweeps readers.
    deferral_lag: Option<Duration>,
    /// Throttle for the deferral WARN.
    warn: WarnThrottle,
}

impl IngestEdgeGate {
    pub(in crate::primary) fn new() -> Self {
        Self {
            prev_drained: HashMap::new(),
            pending_since: HashMap::new(),
            deferral_lag: None,
            warn: WarnThrottle::new(DEFER_WARN_INTERVAL),
        }
    }

    /// Fold one sweep's edge-clock sample into the tracker and return
    /// the verdict: `Some(lag)` iff some peer's frames have sat
    /// undrained, without any drain progress for that peer, for longer
    /// than `pending_threshold` — the sweep must then author no
    /// staleness-based removal. Logs the deferral (throttled WARN
    /// naming the lag and the threshold) and the recovery (one INFO per
    /// outage), so neither branch is silent.
    pub(in crate::primary) fn observe(
        &mut self,
        edges: &IngestEdges,
        now: Instant,
        pending_threshold: Duration,
    ) -> Option<Duration> {
        let mut longest: Option<Duration> = None;
        for (peer, arrived) in edges.arrival.snapshot() {
            let drained = edges.drained.last_seen(&peer);
            let pending = drained.is_none_or(|d| arrived > d);
            let drain_advanced = match (self.prev_drained.get(&peer), drained) {
                (Some(prev), Some(d)) => d > *prev,
                (None, Some(_)) => true,
                _ => false,
            };
            if let Some(d) = drained {
                self.prev_drained.insert(peer.clone(), d);
            }
            if pending && !drain_advanced {
                let since = *self.pending_since.entry(peer).or_insert(now);
                let age = now.saturating_duration_since(since);
                if longest.is_none_or(|l| age > l) {
                    longest = Some(age);
                }
            } else {
                self.pending_since.remove(&peer);
            }
        }

        let verdict = longest.filter(|age| *age > pending_threshold);
        match (self.deferral_lag.take(), verdict) {
            (_, Some(lag)) => {
                if let Some(suppressed) = self.warn.permit() {
                    tracing::warn!(
                        pending_for_s = lag.as_secs_f64(),
                        threshold_s = pending_threshold.as_secs_f64(),
                        suppressed_since_last_warn = suppressed,
                        "ingest path backlogged: frames arrived at the \
                         transport but have not been drained toward delivery \
                         for the named duration — every staleness reading is \
                         suspect, so dead-peer declarations are DEFERRED \
                         until the backlog moves (decider-health gate)"
                    );
                }
            }
            (Some(prev), None) => {
                tracing::info!(
                    was_pending_for_s = prev.as_secs_f64(),
                    "ingest path healthy again (backlog drained); \
                     staleness-based dead-peer declarations resume"
                );
            }
            (None, None) => {}
        }
        self.deferral_lag = verdict;
        verdict
    }

    /// The verdict of the most recent sweep, for staleness consumers
    /// running between sweeps (the dispatch-altitude silent-set read):
    /// `Some` while the gate defers. At most one sweep stale — the same
    /// staleness class those consumers already accept from the
    /// keepalive clocks themselves.
    pub(in crate::primary) fn deferring(&self) -> Option<Duration> {
        self.deferral_lag
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const THRESHOLD: Duration = Duration::from_millis(150);

    /// Drive `observe` with synthetic sweep times over a hand-built
    /// edge pair: a pending frame must persist past the threshold to
    /// trip the gate, and the verdict carries the streak age.
    #[test]
    fn pending_persistence_trips_gate_after_threshold() {
        let mut gate = IngestEdgeGate::new();
        let edges = IngestEdges::new();
        let t0 = Instant::now();

        // Empty clocks: healthy.
        assert_eq!(gate.observe(&edges, t0, THRESHOLD), None);

        // A frame from sec-a arrives, never drained: first observation
        // starts the streak (age 0 — under threshold).
        edges.arrival.record("sec-a");
        assert_eq!(gate.observe(&edges, t0, THRESHOLD), None);
        // Persisting under the threshold: still healthy.
        assert_eq!(
            gate.observe(&edges, t0 + Duration::from_millis(100), THRESHOLD),
            None
        );
        // Persisting past it: deferred, verdict = streak age.
        let lag = gate
            .observe(&edges, t0 + Duration::from_millis(200), THRESHOLD)
            .expect("pending past the threshold defers");
        assert_eq!(lag, Duration::from_millis(200));
        assert!(gate.deferring().is_some(), "verdict visible between sweeps");
    }

    /// Drain progress for the pending peer clears its streak — a busy
    /// healthy mesh (frames always momentarily in flight, but the pump
    /// moving) never trips the gate.
    #[test]
    fn drain_progress_clears_the_streak() {
        let mut gate = IngestEdgeGate::new();
        let edges = IngestEdges::new();
        let t0 = Instant::now();

        edges.arrival.record("sec-a");
        assert_eq!(gate.observe(&edges, t0, THRESHOLD), None);

        // The pump drains it (drained advances)...
        edges.drained.record("sec-a");
        // ...and a NEW frame arrives right after (still arrival > drained
        // at the sweep instant — the busy-mesh shape).
        std::thread::sleep(Duration::from_millis(2));
        edges.arrival.record("sec-a");
        // Drain progress since the last sweep clears the streak even
        // though a frame is momentarily pending.
        assert_eq!(
            gate.observe(&edges, t0 + Duration::from_millis(200), THRESHOLD),
            None
        );
        // The fresh streak only matures if the pump now STOPS: not yet...
        assert_eq!(
            gate.observe(&edges, t0 + Duration::from_millis(300), THRESHOLD),
            None
        );
        // ...but past the threshold of no further drain progress
        // (streak restarted at the t0+300 sweep) it trips.
        assert!(
            gate.observe(&edges, t0 + Duration::from_millis(460), THRESHOLD)
                .is_some()
        );
    }

    /// A fully-drained queue (drained >= arrival) is healthy and also
    /// ENDS a deferral — the recovery transition resets the verdict.
    #[test]
    fn drained_queue_recovers_the_gate() {
        let mut gate = IngestEdgeGate::new();
        let edges = IngestEdges::new();
        let t0 = Instant::now();

        edges.arrival.record("sec-a");
        gate.observe(&edges, t0, THRESHOLD);
        assert!(
            gate.observe(&edges, t0 + Duration::from_millis(200), THRESHOLD)
                .is_some()
        );

        // The pump catches up.
        edges.drained.record("sec-a");
        assert_eq!(
            gate.observe(&edges, t0 + Duration::from_millis(260), THRESHOLD),
            None
        );
        assert_eq!(gate.deferring(), None);
    }
}
