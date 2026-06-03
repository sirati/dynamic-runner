//! Peer reconnect-attempt tracker.
//!
//! When a peer drops out of `PeerNetwork.connections` (broadcast
//! send failure, outgoing-handler task exit, or any other path
//! that observes the connection as dead), the reconnect tracker
//! records the disconnect and the periodic reconnect-tick task
//! issues a redial every [`RECONNECT_TICK`] until the peer is back.
//!
//! Operator-facing WARN logs fire at the [`MILESTONES`] thresholds
//! after the disconnect — 1m, 5m, 10m, 20m — then every
//! [`MILESTONE_RECURRENCE`] thereafter. Each log carries the
//! attempt count since the disconnect was first observed, so a
//! long-disconnected peer stays visible in `tail -f` without
//! spamming the log file with per-attempt lines.
//!
//! Disconnect-and-immediate-reconnect (a transient blip caught
//! and cleared inside one tick) emits a single `peer reconnected`
//! INFO and no WARNs — the milestones never trip on a fast heal.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Cadence at which the reconnect ticker fires. Each tick, every
/// tracked peer gets one redial attempt. 5 seconds: fast enough
/// that a transient network blip heals well within an operator's
/// "what's going on" attention window, slow enough that a
/// permanently dead peer doesn't churn the dial path.
pub(super) const RECONNECT_TICK: Duration = Duration::from_secs(5);

/// Initial milestone schedule. After each crossing, one WARN
/// fires; past the last entry, the schedule continues at
/// [`MILESTONE_RECURRENCE`] from `disconnect_at`.
///
/// The schedule is absolute (measured from disconnect), not
/// relative to the last log. A peer dead for 1h sees WARNs at
/// 1m, 5m, 10m, 20m, 40m, 60m — fixed wall-clock points the
/// operator can predict.
pub(super) const MILESTONES: &[Duration] = &[
    Duration::from_secs(60),      // 1 minute
    Duration::from_secs(5 * 60),  // 5 minutes
    Duration::from_secs(10 * 60), // 10 minutes
    Duration::from_secs(20 * 60), // 20 minutes
];

/// Post-`MILESTONES` recurrence interval. Aligned with the last
/// `MILESTONES` entry (20m) so the recurring cadence is "every
/// 20m after the 20m mark" — 40m, 60m, 80m, ...
pub(super) const MILESTONE_RECURRENCE: Duration = Duration::from_secs(20 * 60);

#[derive(Debug)]
pub(super) struct PeerReconnectState {
    /// Wall-time when the disconnect was first observed. All
    /// milestone arithmetic measures elapsed from here.
    pub(super) disconnect_at: Instant,
    /// Redial attempts issued since `disconnect_at`. Incremented
    /// once per [`RECONNECT_TICK`] tick.
    pub(super) attempts: u32,
    /// Index into [`MILESTONES`] of the next threshold to log on,
    /// OR — once past the end — the position in the
    /// [`MILESTONE_RECURRENCE`] schedule (idx ==
    /// `MILESTONES.len() + N` ⇒ N+1 recurrences emitted).
    pub(super) next_milestone_idx: usize,
}

/// Per-peer reconnect state machine. Owned by `PeerNetwork`; the
/// only writer is the network's main task (LocalSet-bound) so no
/// synchronisation is needed. Visibility is `pub(super)` so the
/// owning struct field can be `pub(super)` without a privacy
/// warning, but the tracker stays an implementation detail of
/// the `peer` submodule — callers reach it only through
/// `PeerNetwork`'s methods.
pub(crate) struct ReconnectTracker {
    state: HashMap<String, PeerReconnectState>,
}

impl ReconnectTracker {
    pub fn new() -> Self {
        Self {
            state: HashMap::new(),
        }
    }

    /// Returns `true` iff this is the first observation of the
    /// disconnect (no prior entry). Callers may use the return
    /// value to fire an immediate redial without waiting for the
    /// next tick — the goal is fast heal on transient drops, the
    /// 5s tick is the steady-state retry pulse.
    pub fn observe_disconnect(&mut self, peer_id: &str) -> bool {
        if self.state.contains_key(peer_id) {
            return false;
        }
        self.state.insert(
            peer_id.to_string(),
            PeerReconnectState {
                disconnect_at: Instant::now(),
                attempts: 0,
                next_milestone_idx: 0,
            },
        );
        tracing::warn!(
            peer = %peer_id,
            "peer disconnect observed; reconnect ticker engaged (5s cadence)"
        );
        true
    }

    /// Clear tracker state for `peer_id` and emit an INFO with the
    /// attempt count + elapsed time. Idempotent on absence.
    pub fn observe_reconnect(&mut self, peer_id: &str) {
        if let Some(state) = self.state.remove(peer_id) {
            tracing::info!(
                peer = %peer_id,
                attempts = state.attempts,
                elapsed_secs = state.disconnect_at.elapsed().as_secs(),
                "peer reconnected"
            );
        }
    }

    /// One tick of the reconnect loop. Bumps attempt counters,
    /// emits any milestone WARNs that just tripped, and returns
    /// the list of peer ids the caller should redial.
    pub fn tick(&mut self) -> Vec<String> {
        let now = Instant::now();
        let mut peers = Vec::with_capacity(self.state.len());
        for (peer_id, state) in &mut self.state {
            state.attempts += 1;
            peers.push(peer_id.clone());

            let elapsed = now.duration_since(state.disconnect_at);

            // Initial milestone phase: trip every entry of
            // MILESTONES whose threshold has been crossed since
            // the last tick. `while` rather than `if` so a long
            // sleep / first tick after a multi-minute backlog
            // logs every missed milestone in order, not just the
            // most-recent one.
            while state.next_milestone_idx < MILESTONES.len() {
                let threshold = MILESTONES[state.next_milestone_idx];
                if elapsed >= threshold {
                    tracing::warn!(
                        peer = %peer_id,
                        attempts = state.attempts,
                        elapsed_secs = elapsed.as_secs(),
                        threshold_secs = threshold.as_secs(),
                        "peer reconnect still failing"
                    );
                    state.next_milestone_idx += 1;
                } else {
                    break;
                }
            }

            // Recurrence phase: emit at
            // `last_milestone + (k+1) * MILESTONE_RECURRENCE`
            // for k = 0, 1, 2, … so a peer dead for an hour
            // already sees logs at 1m / 5m / 10m / 20m / 40m /
            // 60m, all measured from `disconnect_at`.
            //
            // `while` rather than `if` so a first tick that
            // arrives after a multi-recurrence-interval backlog
            // (e.g. tracker was created on a long-running
            // primary that the operator restarted; first tick
            // catches up over the whole disconnect window) logs
            // every missed recurrence in order, not just one.
            while state.next_milestone_idx >= MILESTONES.len() {
                let last_milestone = *MILESTONES.last().expect("MILESTONES non-empty");
                let recurrence_count = (state.next_milestone_idx - MILESTONES.len()) as u32;
                let next_log_at = last_milestone + MILESTONE_RECURRENCE * (recurrence_count + 1);
                if elapsed >= next_log_at {
                    tracing::warn!(
                        peer = %peer_id,
                        attempts = state.attempts,
                        elapsed_secs = elapsed.as_secs(),
                        "peer reconnect still failing (recurring)"
                    );
                    state.next_milestone_idx += 1;
                } else {
                    break;
                }
            }
        }
        peers
    }

    /// Tracker size. Used in tests + the existing `peer_count`
    /// neighbourhood for diagnostics — not part of any operator
    /// log contract.
    #[allow(dead_code)]
    pub fn tracked_count(&self) -> usize {
        self.state.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observe_disconnect_idempotent() {
        let mut t = ReconnectTracker::new();
        assert!(t.observe_disconnect("peer-1"));
        assert!(!t.observe_disconnect("peer-1"));
        assert_eq!(t.tracked_count(), 1);
    }

    #[test]
    fn observe_reconnect_clears() {
        let mut t = ReconnectTracker::new();
        t.observe_disconnect("peer-1");
        assert_eq!(t.tracked_count(), 1);
        t.observe_reconnect("peer-1");
        assert_eq!(t.tracked_count(), 0);
    }

    #[test]
    fn observe_reconnect_idempotent_on_absence() {
        let mut t = ReconnectTracker::new();
        // No entry for "peer-1"; observe_reconnect must be a NoOp,
        // not a panic.
        t.observe_reconnect("peer-1");
        assert_eq!(t.tracked_count(), 0);
    }

    #[test]
    fn tick_lists_all_tracked_peers() {
        let mut t = ReconnectTracker::new();
        t.observe_disconnect("peer-a");
        t.observe_disconnect("peer-b");
        let peers = t.tick();
        assert_eq!(peers.len(), 2);
        assert!(peers.contains(&"peer-a".to_string()));
        assert!(peers.contains(&"peer-b".to_string()));
    }

    #[test]
    fn tick_bumps_attempts() {
        let mut t = ReconnectTracker::new();
        t.observe_disconnect("peer-1");
        t.tick();
        t.tick();
        t.tick();
        let state = t.state.get("peer-1").expect("peer tracked");
        assert_eq!(state.attempts, 3);
    }

    #[test]
    fn milestone_indices_advance_past_thresholds() {
        // Manually construct a state with a disconnect_at far in
        // the past so all milestones trip in one tick. Verifies
        // the "while" loop in tick() — a long sleep should log
        // every missed milestone in order, not just the most
        // recent one.
        let mut t = ReconnectTracker::new();
        let one_hour_ago = Instant::now() - Duration::from_secs(60 * 60);
        t.state.insert(
            "stale-peer".to_string(),
            PeerReconnectState {
                disconnect_at: one_hour_ago,
                attempts: 0,
                next_milestone_idx: 0,
            },
        );
        t.tick();
        let state = t.state.get("stale-peer").expect("peer tracked");
        // After one tick over the 1h-disconnect: idx should have
        // moved past every entry of MILESTONES (4) and the
        // recurrence phase should have advanced (40m, 60m → 2
        // more crossings) → idx >= 6.
        assert!(
            state.next_milestone_idx >= 6,
            "expected idx >= 6 after 1h disconnect, got {}",
            state.next_milestone_idx
        );
    }
}
