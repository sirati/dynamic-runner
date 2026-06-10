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

/// Consecutive-failed-dial count at which the FIRST address-carrying
/// dial-failure summary fires (see [`PeerReconnectState::dial_summary_due`]).
/// At [`RECONNECT_TICK`] cadence (5s) this is ~15s — past the one-or-two-
/// tick window a transient blip heals in, so a flapped peer stays silent
/// (mirrors the milestone schedule's "no WARN on a fast heal" intent)
/// while a genuinely-unreachable peer surfaces its dialed address once,
/// promptly.
pub(super) const DIAL_SUMMARY_THRESHOLD: u32 = 3;

/// After the first summary at [`DIAL_SUMMARY_THRESHOLD`], the
/// address-carrying summary recurs every this-many further consecutive
/// failed dials. At [`RECONNECT_TICK`] cadence (5s) that is ~60s — the
/// operator keeps seeing "peer X unreachable, dialing addr Y" roughly
/// once a minute, never per-tick.
pub(super) const DIAL_SUMMARY_RECURRENCE: u32 = 12;

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
    /// Once-per-outage latch for the broadcast-miss WARN (#363).
    /// `false` until the first [`ReconnectTracker::first_broadcast_miss`]
    /// call of this outage flips it; cleared implicitly when
    /// [`ReconnectTracker::observe_reconnect`] removes the whole entry,
    /// so a FRESH outage re-arms the warn.
    pub(super) broadcast_miss_warned: bool,
}

impl PeerReconnectState {
    /// Count-based throttle gate for the address-carrying dial-failure
    /// summary. Returns `true` on exactly the consecutive-attempt counts
    /// `{THRESHOLD, THRESHOLD + RECURRENCE, THRESHOLD + 2·RECURRENCE, …}`
    /// and `false` everywhere else — so the caller emits the summary once
    /// at the threshold and then once per recurrence window, never on
    /// every tick.
    ///
    /// This throttle is intentionally COUNT-based (consecutive failed
    /// dials), orthogonal to the TIME-based [`MILESTONES`] WARNs in
    /// [`ReconnectTracker::tick`]: the milestone line answers "how long
    /// has this peer been gone", the summary line answers "what address
    /// are we even dialing" — the operator-facing datum the missing-
    /// `%addr` incident needed. The tracker owns this gate (the count);
    /// the address itself is resolved by the transport caller from its
    /// own `peer_dial_info`, so the timing tracker never learns about
    /// dial addresses.
    fn dial_summary_due(&self) -> bool {
        match self.attempts.checked_sub(DIAL_SUMMARY_THRESHOLD) {
            None => false,                                // below the first threshold
            Some(0) => true,                             // exactly the first threshold
            Some(over) => over % DIAL_SUMMARY_RECURRENCE == 0, // recurrence windows
        }
    }
}

/// A peer whose consecutive failed-dial count just crossed a
/// [`PeerReconnectState::dial_summary_due`] boundary this tick. Carries
/// only what the timing tracker owns — the peer id and the running
/// attempt count. The transport caller pairs this with the dialed
/// address (from its `peer_dial_info`) to emit the operator WARN; the
/// tracker itself never touches dial-address state.
pub(crate) struct DialSummary {
    pub(super) peer_id: String,
    pub(super) attempts: u32,
}

/// Result of one [`ReconnectTracker::tick`]: the peers to redial this
/// tick (unchanged contract) plus any address-carrying dial-failure
/// summaries whose count-throttle boundary was crossed. Bundling both in
/// one return keeps the tracker the single owner of per-peer attempt
/// state — the caller never re-derives the count.
pub(crate) struct TickOutcome {
    pub(super) to_dial: Vec<String>,
    pub(super) dial_summaries: Vec<DialSummary>,
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
                broadcast_miss_warned: false,
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
    /// the [`TickOutcome`]: the peer ids the caller should redial
    /// plus any address-carrying dial-failure summaries whose
    /// count-throttle boundary was crossed this tick.
    pub fn tick(&mut self) -> TickOutcome {
        let now = Instant::now();
        let mut peers = Vec::with_capacity(self.state.len());
        let mut dial_summaries = Vec::new();
        for (peer_id, state) in &mut self.state {
            state.attempts += 1;
            peers.push(peer_id.clone());

            // Count-based dial-failure summary gate (orthogonal to the
            // time-based milestones below): when the consecutive-failure
            // count hits the throttle boundary, hand the peer + count
            // back to the caller, which owns the dialed address.
            if state.dial_summary_due() {
                dial_summaries.push(DialSummary {
                    peer_id: peer_id.clone(),
                    attempts: state.attempts,
                });
            }

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
        TickOutcome {
            to_dial: peers,
            dial_summaries,
        }
    }

    /// Consecutive redial attempts issued for `peer_id` since its
    /// disconnect was first observed; `None` when the peer is not
    /// tracked (connected, or never disconnected). Read by the dial
    /// path so each spawned redial's narration carries its attempt
    /// number — the tracker stays the single owner of the count.
    pub fn attempts_for(&self, peer_id: &str) -> Option<u32> {
        self.state.get(peer_id).map(|s| s.attempts)
    }

    /// Once-per-outage gate for the broadcast-miss WARN (#363): returns
    /// `true` exactly once per tracked outage — the first call for a
    /// peer whose disconnect is currently tracked flips the per-outage
    /// latch; every later call returns `false` until
    /// [`Self::observe_reconnect`] clears the entry (a fresh outage
    /// re-arms the gate). The tracker owns this latch because it owns
    /// the outage LIFECYCLE — the latch's reset point (the heal) is the
    /// entry's removal, so the two can never drift.
    ///
    /// An UNTRACKED peer returns `false`: a known-but-not-yet-tracked
    /// peer is in the mesh-forming dial window (≤ one [`RECONNECT_TICK`]
    /// until the tick reconciliation tracks it), and staying silent
    /// there mirrors the milestone schedule's "no WARN on a fast heal"
    /// intent — only a peer the tracker already considers disconnected
    /// earns the broadcast-miss line.
    pub fn first_broadcast_miss(&mut self, peer_id: &str) -> bool {
        match self.state.get_mut(peer_id) {
            Some(state) if !state.broadcast_miss_warned => {
                state.broadcast_miss_warned = true;
                true
            }
            _ => false,
        }
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
        let peers = t.tick().to_dial;
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
    fn dial_summary_fires_at_threshold_not_before() {
        // The address-carrying dial-failure summary is count-throttled:
        // SILENT for the first THRESHOLD-1 consecutive failed dials
        // (suppresses a transient blip that heals in one or two ticks),
        // then fires EXACTLY on the THRESHOLD-th tick — the boundary.
        let mut t = ReconnectTracker::new();
        t.observe_disconnect("peer-1");

        // Ticks 1..THRESHOLD-1: no summary yet.
        for n in 1..DIAL_SUMMARY_THRESHOLD {
            let out = t.tick();
            assert!(
                out.dial_summaries.is_empty(),
                "summary must stay silent before the threshold (tick {n})"
            );
        }

        // Tick == THRESHOLD: exactly one summary for peer-1, carrying the
        // running consecutive-failure count.
        let out = t.tick();
        assert_eq!(
            out.dial_summaries.len(),
            1,
            "summary must fire on the THRESHOLD-th consecutive failed dial"
        );
        assert_eq!(out.dial_summaries[0].peer_id, "peer-1");
        assert_eq!(out.dial_summaries[0].attempts, DIAL_SUMMARY_THRESHOLD);
    }

    #[test]
    fn dial_summary_throttles_to_recurrence_not_every_tick() {
        // Past the first threshold the summary must recur only once per
        // RECURRENCE window — NOT on every tick. Revert-check for the
        // throttle: counting summaries across a long failing window must
        // be small (1 + #recurrences), not one-per-tick.
        let mut t = ReconnectTracker::new();
        t.observe_disconnect("peer-1");

        let total_ticks = DIAL_SUMMARY_THRESHOLD + 2 * DIAL_SUMMARY_RECURRENCE;
        let mut summary_ticks = Vec::new();
        for n in 1..=total_ticks {
            if !t.tick().dial_summaries.is_empty() {
                summary_ticks.push(n);
            }
        }

        // Exactly the boundary set {THRESHOLD, THRESHOLD+RECURRENCE,
        // THRESHOLD+2·RECURRENCE} — three emissions across a window that
        // saw `total_ticks` ticks. A removed throttle would fire on
        // every tick (total_ticks emissions).
        assert_eq!(
            summary_ticks,
            vec![
                DIAL_SUMMARY_THRESHOLD,
                DIAL_SUMMARY_THRESHOLD + DIAL_SUMMARY_RECURRENCE,
                DIAL_SUMMARY_THRESHOLD + 2 * DIAL_SUMMARY_RECURRENCE,
            ],
            "summary must fire only at threshold + recurrence boundaries, \
             not every tick"
        );
        assert!(
            summary_ticks.len() < total_ticks as usize,
            "throttle must suppress the vast majority of ticks"
        );
    }

    #[test]
    fn dial_summary_clears_on_reconnect() {
        // A heal before the threshold leaves no pending summary, and a
        // fresh disconnect restarts the count from zero — the summary
        // never fires for a peer that flapped quickly.
        let mut t = ReconnectTracker::new();
        t.observe_disconnect("peer-1");
        t.tick(); // attempt 1
        t.observe_reconnect("peer-1");
        assert_eq!(t.tracked_count(), 0);

        // Re-disconnect: the count starts over, so the first tick after
        // is attempt 1 again — still below the threshold, still silent.
        t.observe_disconnect("peer-1");
        let out = t.tick();
        assert!(out.dial_summaries.is_empty());
        let state = t.state.get("peer-1").expect("peer tracked");
        assert_eq!(state.attempts, 1);
    }

    #[test]
    fn first_broadcast_miss_false_for_untracked_peer() {
        // A peer the tracker does not consider disconnected (connected,
        // or in the mesh-forming window before the first reconciling
        // tick) must NOT earn a broadcast-miss warn.
        let mut t = ReconnectTracker::new();
        assert!(!t.first_broadcast_miss("peer-1"));
    }

    #[test]
    fn first_broadcast_miss_fires_once_per_outage() {
        // The gate is once-per-outage: true on the first call of a
        // tracked outage, false on every subsequent call — a
        // persistently-down peer must not warn on every broadcast.
        let mut t = ReconnectTracker::new();
        t.observe_disconnect("peer-1");
        assert!(t.first_broadcast_miss("peer-1"));
        assert!(!t.first_broadcast_miss("peer-1"));
        assert!(!t.first_broadcast_miss("peer-1"));
    }

    #[test]
    fn first_broadcast_miss_rearms_on_fresh_outage() {
        // The heal removes the entry (and with it the latch), so a NEW
        // outage earns a new warn.
        let mut t = ReconnectTracker::new();
        t.observe_disconnect("peer-1");
        assert!(t.first_broadcast_miss("peer-1"));
        t.observe_reconnect("peer-1");
        assert!(!t.first_broadcast_miss("peer-1"), "healed peer is silent");
        t.observe_disconnect("peer-1");
        assert!(
            t.first_broadcast_miss("peer-1"),
            "a fresh outage re-arms the once-per-outage gate"
        );
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
                broadcast_miss_warned: false,
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
