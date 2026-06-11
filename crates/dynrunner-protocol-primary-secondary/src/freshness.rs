//! Per-peer freshness clocks + the transport ingest-edge instruments.
//!
//! # Concern
//!
//! ONE mechanism, three uses: a shared cell recording "the last `Instant`
//! a frame from peer X passed THIS measuring point". [`FreshnessClock`]
//! is the cell; [`IngestEdges`] is the pair of cells a transport mounts
//! on the two ends of its inbound queue (producer side = ARRIVAL,
//! consumer side = DRAINED); [`InboundTap`] is the recording sender that
//! IS the producer-side mount.
//!
//! # Why the measuring point matters (the ingest-edge law)
//!
//! Liveness clocks measure the PEER's silence only if they are fed at
//! the earliest point a peer's frame is attributable on this node — the
//! transport's connection read loops. Any clock fed further downstream
//! (the mesh pump's delivery, the coordinator's dispatch) inflates under
//! LOCAL backlog: the run_20260611_115429 face had every member's
//! keepalives ARRIVING at the primary's transport while the mesh pump —
//! starved by a snapshot-flooded egress — never moved them to the
//! delivery edge, so the downstream clocks aged and the primary removed
//! live members for an hour. The arrival-side clock here is written by
//! the per-connection reader tasks, which keep running while the pump
//! starves — it stays honest exactly when the downstream clocks lie.
//!
//! # Attribution honesty (the unidentified window)
//!
//! A frame is recorded ONLY once it has fully decoded — attribution is
//! the envelope's `sender_id`, available from the first complete frame
//! onward (including the accept-side identification frame itself).
//! Partially-buffered bytes are unattributable and deliberately refresh
//! NOTHING: the clock may under-report (conservative — a peer looks at
//! most one frame staler than it is), never over-report.
//!
//! # Freshness, not membership
//!
//! These cells record ONLY positive evidence (a frame passed at time
//! `t`); they never remove entries. Staleness and eviction are the
//! reader's concern.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use dynrunner_core::Identifier;
use tokio::sync::mpsc;

use crate::DistributedMessage;

/// A cloneable handle to one per-peer freshness cell.
///
/// Every clone shares one cell; writers record on their own tasks and a
/// detached reader samples on its own cadence. Keying on a monotonic
/// `Instant` makes the freshness immune to a coordinated host
/// suspend/resume.
#[derive(Clone, Default)]
pub struct FreshnessClock {
    inner: Arc<Mutex<HashMap<String, Instant>>>,
}

impl FreshnessClock {
    /// A fresh cell with no observations yet.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Record that a frame from `node_id` just passed this measuring
    /// point (local receipt `Instant`).
    pub fn record(&self, node_id: &str) {
        self.inner
            .lock()
            .expect("freshness clock poisoned")
            .insert(node_id.to_string(), Instant::now());
    }

    /// The most recent recorded `Instant` for `node_id`, or `None` if
    /// no frame from it was ever observed here. The reader compares
    /// `now - last_seen` against its own staleness threshold.
    pub fn last_seen(&self, node_id: &str) -> Option<Instant> {
        self.inner
            .lock()
            .expect("freshness clock poisoned")
            .get(node_id)
            .copied()
    }

    /// Snapshot every `(peer, last_seen)` observation. For readers that
    /// must scan the whole cell (the ingest-health gate compares the
    /// arrival and drained edges per peer) rather than probe one id.
    pub fn snapshot(&self) -> Vec<(String, Instant)> {
        self.inner
            .lock()
            .expect("freshness clock poisoned")
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }
}

/// The two freshness clocks bracketing a transport's inbound queue.
///
/// - [`IngestEdges::arrival`] — written on the PRODUCER side (the
///   per-connection reader tasks, via [`InboundTap`]) the moment a
///   decoded frame enters the transport's inbound channel.
/// - [`IngestEdges::drained`] — written on the CONSUMER side (the
///   transport's `recv_peer`/`try_recv_peer` pull) the moment the same
///   frame leaves that channel toward routing/delivery.
///
/// Both record the SAME frame stream under the SAME key (the envelope
/// `sender_id` — relay envelopes key the forwarding hop on both sides,
/// so the pair never desyncs on routing semantics). `arrival` newer
/// than `drained` for a peer therefore proves an undrained frame from
/// it sits in the queue — the decider-side ingest-health signal the
/// removal gate consumes.
#[derive(Clone, Default)]
pub struct IngestEdges {
    /// Producer-side clock: frame entered the inbound queue.
    pub arrival: FreshnessClock,
    /// Consumer-side clock: frame was pulled out of the inbound queue.
    pub drained: FreshnessClock,
}

impl IngestEdges {
    /// A fresh pair with no observations on either edge.
    pub fn new() -> Self {
        Self::default()
    }
}

/// The recording sender every inbound-frame producer pushes through —
/// the transport's single inbound fan-in, with the arrival-edge clock
/// mounted on it.
///
/// Wraps the transport's inbound `mpsc::UnboundedSender` so EVERY
/// producer (QUIC/WSS reader pumps, accept-side identification frames,
/// in-process forwarders, bootstrap-wire forwarders) records the
/// sender's arrival by construction — no per-call-site discipline. The
/// frame is recorded BEFORE the channel send: receipt at this node is a
/// fact regardless of whether the consumer half still listens.
pub struct InboundTap<I: Identifier> {
    tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    arrival: FreshnessClock,
}

// Manual impl: `#[derive(Clone)]` would needlessly bound `I: Clone`.
impl<I: Identifier> Clone for InboundTap<I> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            arrival: self.arrival.clone(),
        }
    }
}

impl<I: Identifier> InboundTap<I> {
    /// Mount `arrival` (a transport's [`IngestEdges::arrival`] clone)
    /// onto the inbound sender `tx`.
    pub fn new(tx: mpsc::UnboundedSender<DistributedMessage<I>>, arrival: FreshnessClock) -> Self {
        Self { tx, arrival }
    }

    /// A tap whose recordings nobody reads — for inbound legs that have
    /// no liveness consumer (e.g. the worker-side uplink client). Keeps
    /// the producer plumbing uniform without an `Option` at every pump.
    pub fn untracked(tx: mpsc::UnboundedSender<DistributedMessage<I>>) -> Self {
        Self::new(tx, FreshnessClock::new())
    }

    /// Record the frame's sender on the arrival clock, then push the
    /// frame into the inbound channel. `Err` iff the consumer half is
    /// gone (the transport is tearing down) — the frame is then
    /// unrecoverable (its only consumer no longer exists), so the error
    /// is the unit [`InboundClosed`] rather than the frame-carrying
    /// `SendError` (every producer only branches on `is_err`).
    pub fn send(&self, msg: DistributedMessage<I>) -> Result<(), InboundClosed> {
        self.arrival.record(msg.sender_id());
        self.tx.send(msg).map_err(|_| InboundClosed)
    }
}

/// [`InboundTap::send`]'s error: the inbound channel's consumer half is
/// gone (transport teardown). Carries nothing — the frame's only
/// consumer no longer exists, so there is nothing useful to hand back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InboundClosed;

impl std::fmt::Display for InboundClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("inbound channel closed (transport tearing down)")
    }
}

impl std::error::Error for InboundClosed {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KeepaliveRole;
    use std::time::Duration;

    #[test]
    fn record_observed_by_clones() {
        let v = FreshnessClock::new();
        let reader = v.clone();
        assert_eq!(reader.last_seen("secondary-0"), None);
        v.record("secondary-0");
        let first = reader.last_seen("secondary-0").expect("recorded");
        // A later record refreshes the same node's entry to a newer instant.
        std::thread::sleep(Duration::from_millis(2));
        v.record("secondary-0");
        let second = reader.last_seen("secondary-0").expect("refreshed");
        assert!(second >= first, "the entry advances on a fresh frame");
        // A different node is tracked independently.
        assert_eq!(reader.last_seen("secondary-1"), None);
        v.record("secondary-1");
        assert!(reader.last_seen("secondary-1").is_some());
        // The snapshot enumerates both observations.
        let snap = reader.snapshot();
        assert_eq!(snap.len(), 2);
        assert!(snap.iter().any(|(id, _)| id == "secondary-0"));
        assert!(snap.iter().any(|(id, _)| id == "secondary-1"));
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    struct TestId(String);

    fn keepalive(sender: &str) -> DistributedMessage<TestId> {
        DistributedMessage::Keepalive {
            target: None,
            sender_id: sender.to_string(),
            timestamp: 0.0,
            secondary_id: sender.to_string(),
            active_workers: 0,
            emitter_role: KeepaliveRole::Secondary,
        }
    }

    /// The tap records the envelope sender on the arrival clock and
    /// forwards the frame; a closed consumer errs the send but the
    /// arrival fact is still recorded (receipt happened regardless).
    #[test]
    fn tap_records_arrival_then_forwards() {
        let edges = IngestEdges::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let tap = InboundTap::new(tx, edges.arrival.clone());

        assert_eq!(edges.arrival.last_seen("sec-a"), None);
        tap.send(keepalive("sec-a")).expect("consumer alive");
        assert!(edges.arrival.last_seen("sec-a").is_some());
        let got = rx.try_recv().expect("frame forwarded");
        assert_eq!(got.sender_id(), "sec-a");
        // The drained edge is untouched by the tap — it belongs to the
        // consumer side.
        assert_eq!(edges.drained.last_seen("sec-a"), None);

        drop(rx);
        assert!(tap.send(keepalive("sec-b")).is_err());
        assert!(
            edges.arrival.last_seen("sec-b").is_some(),
            "receipt at this node is a fact even when the consumer is gone"
        );
    }
}
