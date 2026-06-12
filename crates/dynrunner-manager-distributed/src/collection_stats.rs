//! Periodic collection-stats line — accumulation visibility for the
//! unbounded-by-design collections a coordinator owns.
//!
//! # Single concern
//!
//! Decide — purely, testably — WHEN the periodic structured stats line
//! fires and WHETHER it escalates to WARN, given one snapshot of the
//! watched collection sizes. The coordinator owns gathering the
//! snapshot (each number through its owning structure's read surface)
//! and emitting the line; this module owns only the cadence + the
//! threshold policy, the same split as
//! [`crate::primary::custom_message::CustomBacklogMonitor`] and
//! [`crate::oploop_instrumentation`].
//!
//! # Why (the 22 GB cold-swap class)
//!
//! Several collections are deliberately unbounded: the replicated
//! custom-message inbox MIRROR (every replica retains `Unhandled`
//! payloads until the Handled fact compacts them — a stalled watermark
//! grows payload-bearing state on every mirror), the confirmable-report
//! replay buffer (one retained frame clone per unacked terminal /
//! important custom message — an ack outage grows it per task), and the
//! role-inbox channel (a starved loop accumulates every inbound frame).
//! Each is append-rate-bounded only by production event rates, and each
//! is COLD once written (appended, never traversed until its consumer
//! event) — exactly the shape that surfaces months later as an
//! unexplained multi-GB mostly-swapped process. The stats line turns
//! that growth into a greppable trend in the production logs at
//! megabyte scale instead of a post-mortem at the 22 GB scale.
//!
//! # Cadence
//!
//! One line per [`COLLECTION_STATS_INTERVAL`] per coordinator (INFO;
//! WARN when any threshold is breached). The deadline is PERSISTENT
//! state seeded at construction, so the keepalive-arm driver firing
//! more often than the interval never bunches emissions, and a missed
//! window (a stalled loop) emits on the first observation after it.

use std::time::{Duration, Instant};

use crate::cluster_state::CustomInboxStats;

/// Cadence of the periodic collection-stats line. Matches the oploop
/// arm-stats interval: one compact line per 2 minutes per coordinator
/// is noise-free at any run length, and a leak at the observed
/// production rate (~2–3 GB/h) is unmistakable across a handful of
/// lines.
pub(crate) const COLLECTION_STATS_INTERVAL: Duration = Duration::from_secs(120);

/// `Unhandled` custom-inbox entries past which the mirror is presumed
/// stalled (steady state is ~zero: every Handled/Failed apply
/// compacts). Replica-side twin of the primary's keep-up monitor.
pub(crate) const CUSTOM_UNHANDLED_WARN: usize = 512;

/// Retained custom-payload bytes past which the mirror WARNs even at a
/// low entry count (few entries × near-cap 100 KiB payloads).
pub(crate) const CUSTOM_PAYLOAD_BYTES_WARN: usize = 32 * 1024 * 1024;

/// Retained confirmable-report replay entries past which the ack path
/// is presumed broken (steady state is the in-flight unacked window —
/// tens at most; each entry holds a full frame clone, results
/// included).
pub(crate) const REPLAY_BUFFER_WARN: usize = 256;

/// Role-inbox frames past which the operational loop is presumed
/// starved (a drained loop holds ~zero queued frames).
pub(crate) const INBOX_DEPTH_WARN: usize = 4096;

/// One observation of the watched collection sizes, gathered by the
/// coordinator from each structure's own read surface.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CollectionStatsSnapshot {
    /// The replicated custom-message inbox mirror
    /// (`ClusterState::custom_inbox_stats`).
    pub(crate) custom_inbox: CustomInboxStats,
    /// Retained confirmable reports awaiting ack/replay
    /// (`pending_report_replays.len()`).
    pub(crate) replay_buffered: usize,
    /// Age of the oldest retained report in seconds (0 when empty).
    pub(crate) replay_oldest_secs: u64,
    /// Frames queued in the role inbox (`RoleInbox::depth`).
    pub(crate) inbox_depth: usize,
}

impl CollectionStatsSnapshot {
    /// The names of every breached threshold, in a stable order — the
    /// WARN escalation input (empty = healthy INFO line).
    pub(crate) fn breaches(&self) -> Vec<&'static str> {
        let mut out = Vec::new();
        if self.custom_inbox.unhandled >= CUSTOM_UNHANDLED_WARN {
            out.push("custom_unhandled");
        }
        if self.custom_inbox.payload_bytes >= CUSTOM_PAYLOAD_BYTES_WARN {
            out.push("custom_payload_bytes");
        }
        if self.replay_buffered >= REPLAY_BUFFER_WARN {
            out.push("replay_buffered");
        }
        if self.inbox_depth >= INBOX_DEPTH_WARN {
            out.push("inbox_depth");
        }
        out
    }
}

/// The cadence half: persistent next-due deadline over the interval.
#[derive(Debug)]
pub(crate) struct CollectionStatsEmitter {
    next_due: Instant,
}

impl CollectionStatsEmitter {
    /// Seeded one full interval out, mirroring the oploop stats line:
    /// the first line lands after the run has something to say.
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            next_due: now + COLLECTION_STATS_INTERVAL,
        }
    }

    /// Is a line due at `now`? Cheap (one compare) so the driver arm
    /// may call it every tick; the SNAPSHOT gather (an O(live-entries)
    /// inbox fold) runs only when this returns true.
    pub(crate) fn due(&self, now: Instant) -> bool {
        now >= self.next_due
    }

    /// Consume the due window: re-arm the deadline a full interval from
    /// `now` (await-before-resleep — a stalled span emits once on the
    /// first observation after it, never a burst of catch-ups).
    pub(crate) fn mark_emitted(&mut self, now: Instant) {
        self.next_due = now + COLLECTION_STATS_INTERVAL;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emitter_fires_once_per_interval() {
        let t0 = Instant::now();
        let mut emitter = CollectionStatsEmitter::new(t0);
        assert!(!emitter.due(t0), "seeded one interval out");
        assert!(!emitter.due(t0 + COLLECTION_STATS_INTERVAL / 2));
        let t1 = t0 + COLLECTION_STATS_INTERVAL;
        assert!(emitter.due(t1));
        emitter.mark_emitted(t1);
        assert!(!emitter.due(t1 + Duration::from_secs(1)), "re-armed");
        assert!(emitter.due(t1 + COLLECTION_STATS_INTERVAL));
    }

    #[test]
    fn stalled_span_emits_once_not_a_burst() {
        let t0 = Instant::now();
        let mut emitter = CollectionStatsEmitter::new(t0);
        // Ten intervals of silence (a stalled loop), then one
        // observation: due once, and the re-arm is a FULL interval from
        // the emission instant.
        let late = t0 + COLLECTION_STATS_INTERVAL * 10;
        assert!(emitter.due(late));
        emitter.mark_emitted(late);
        assert!(!emitter.due(late + Duration::from_secs(1)));
        assert!(emitter.due(late + COLLECTION_STATS_INTERVAL));
    }

    #[test]
    fn healthy_snapshot_has_no_breaches() {
        let snap = CollectionStatsSnapshot::default();
        assert!(snap.breaches().is_empty());
    }

    #[test]
    fn each_threshold_breaches_independently() {
        let snap = CollectionStatsSnapshot {
            custom_inbox: CustomInboxStats {
                unhandled: CUSTOM_UNHANDLED_WARN,
                terminal: 0,
                payload_bytes: 0,
            },
            ..Default::default()
        };
        assert_eq!(snap.breaches(), vec!["custom_unhandled"]);

        let snap = CollectionStatsSnapshot {
            custom_inbox: CustomInboxStats {
                unhandled: 1,
                terminal: 0,
                payload_bytes: CUSTOM_PAYLOAD_BYTES_WARN,
            },
            ..Default::default()
        };
        assert_eq!(snap.breaches(), vec!["custom_payload_bytes"]);

        let snap = CollectionStatsSnapshot {
            replay_buffered: REPLAY_BUFFER_WARN,
            ..Default::default()
        };
        assert_eq!(snap.breaches(), vec!["replay_buffered"]);

        let snap = CollectionStatsSnapshot {
            inbox_depth: INBOX_DEPTH_WARN,
            ..Default::default()
        };
        assert_eq!(snap.breaches(), vec!["inbox_depth"]);
    }

    #[test]
    fn all_breaches_listed_together() {
        let snap = CollectionStatsSnapshot {
            custom_inbox: CustomInboxStats {
                unhandled: CUSTOM_UNHANDLED_WARN,
                terminal: 7,
                payload_bytes: CUSTOM_PAYLOAD_BYTES_WARN,
            },
            replay_buffered: REPLAY_BUFFER_WARN,
            replay_oldest_secs: 10,
            inbox_depth: INBOX_DEPTH_WARN,
        };
        assert_eq!(
            snap.breaches(),
            vec![
                "custom_unhandled",
                "custom_payload_bytes",
                "replay_buffered",
                "inbox_depth"
            ]
        );
    }
}
