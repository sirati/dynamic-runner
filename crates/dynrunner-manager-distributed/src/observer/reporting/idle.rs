//! Idle-secondary detector.
//!
//! # Single concern
//!
//! Per owner-decision C-4's idle-secondary alert: a secondary that has
//! held ZERO in-flight tasks for a continuous minute WHILE the queue
//! has ready-to-execute work is stalled and worth an LLM wake. This
//! module owns that decision and its de-duplication gate; it emits
//! NOTHING itself (it returns the set of secondaries to alert on this
//! tick) so the tracing sink stays the caller's concern.
//!
//! # Known-secondary set is accumulated from observed in-flight
//!
//! A zero-authority observer has no clean CRDT roster accessor: the
//! `RoleTable` carries only `primary` + `observers`, and the `peer_state`
//! roster is module-private to `cluster_state`. The one CRDT fact that
//! names a working secondary is `TaskState::InFlight { secondary }` —
//! but a secondary whose tasks have all completed no longer appears
//! there (terminal states drop the executing-secondary field). So this
//! detector ACCUMULATES every secondary it has ever seen carry an
//! in-flight task into `known`, and tracks each one's idleness over
//! that accumulated set. "Idle" is therefore well-defined relative to
//! "has been observed doing work" — which is exactly the population the
//! alert is about (a secondary that ran work and then went quiet while
//! work remains). The TOTAL-secondary denominator the occupancy stats
//! want is a separate, deferred Part-D concern (it needs the replicated
//! capacity record); this detector deliberately does not depend on it.
//!
//! # Gate semantics
//!
//! For each known secondary the detector holds one [`SecondaryGate`]:
//!   * `idle_since`: instant it last transitioned to zero in-flight
//!     (cleared the moment it picks up any task again),
//!   * `alerted`: whether the one-shot alert has already fired for the
//!     current idle spell.
//!
//! An alert fires once when a secondary has been idle ≥ `idle_threshold`
//! AND there is ready work; it does NOT repeat. Receiving any task
//! clears `idle_since` and re-arms `alerted`, so a later idle spell can
//! alert again.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::stats::StatsSnapshot;

/// Per-secondary idle bookkeeping.
#[derive(Debug, Clone)]
struct SecondaryGate {
    /// `Some(instant)` while the secondary has zero in-flight tasks,
    /// recording when the idle spell began; `None` while it has ≥1.
    idle_since: Option<Instant>,
    /// Whether the one-shot alert already fired for the CURRENT idle
    /// spell. Reset to `false` whenever the secondary picks up a task.
    alerted: bool,
}

/// The idle-secondary detector. Holds the accumulated known-secondary
/// set and each secondary's gate. `idle_threshold` is injected (one
/// minute in production) so tests can drive it under a paused clock.
#[derive(Debug)]
pub struct IdleDetector {
    gates: HashMap<String, SecondaryGate>,
    idle_threshold: Duration,
}

impl IdleDetector {
    pub fn new(idle_threshold: Duration) -> Self {
        Self {
            gates: HashMap::new(),
            idle_threshold,
        }
    }

    /// Fold one snapshot into the gates at instant `now` and return the
    /// secondaries that should alert on THIS tick (each at most once per
    /// idle spell). `now` is injected so the cadence is deterministic
    /// under `tokio::time::pause`.
    ///
    /// Returns secondary ids in sorted order so the caller's emission
    /// (and the tests) are deterministic.
    pub fn tick(&mut self, snapshot: &StatsSnapshot, now: Instant) -> Vec<String> {
        let ready_work = snapshot.ready_in_queue > 0;

        // 1. Learn any newly-observed secondaries (accumulate the
        //    roster) and update every gate's idle/busy transition.
        //    Iterating over the union of (already-known) ∪ (currently
        //    in-flight) keeps a secondary that has fallen out of the
        //    in-flight map (all its tasks finished) under observation.
        let observed: Vec<String> = self
            .gates
            .keys()
            .cloned()
            .chain(snapshot.per_secondary_in_flight.keys().cloned())
            .collect();
        for id in observed {
            let in_flight = snapshot
                .per_secondary_in_flight
                .get(&id)
                .copied()
                .unwrap_or(0);
            let gate = self.gates.entry(id).or_insert(SecondaryGate {
                idle_since: None,
                alerted: false,
            });
            if in_flight > 0 {
                // Busy: clear the idle spell and re-arm the one-shot.
                gate.idle_since = None;
                gate.alerted = false;
            } else if gate.idle_since.is_none() {
                // First tick of a fresh idle spell: stamp its start.
                gate.idle_since = Some(now);
            }
        }

        // 2. Decide which gates fire. Only when there is ready work to
        //    run does an idle secondary read as a stall; with an empty
        //    queue an idle secondary is correctly idle (nothing to do)
        //    and must not alert.
        if !ready_work {
            return Vec::new();
        }
        let mut firing: Vec<String> = Vec::new();
        for (id, gate) in self.gates.iter_mut() {
            if gate.alerted {
                continue;
            }
            if let Some(since) = gate.idle_since
                && now.duration_since(since) >= self.idle_threshold
            {
                gate.alerted = true;
                firing.push(id.clone());
            }
        }
        firing.sort();
        firing
    }
}
