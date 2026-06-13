//! Snapshot-stream DRIVERS: the responder-side stream scheduler and the
//! requester-side progress tracker, role-agnostic (primary, secondary
//! router, observer all hold one of each — exactly the anti_entropy.rs
//! precedent: the policy lives here ONCE, each role owns only its
//! `send_to` edge and its loop arm).
//!
//! # Responder ([`SnapshotStreamResponder`])
//!
//! One `RequestSnapshotStream` creates (or repositions) a per-stream
//! [`SnapshotStreamPlan`] and enqueues a wake token on an internal
//! channel. The coordinator's select loop gains ONE arm: await
//! [`SnapshotStreamResponder::next_wake`], then call
//! [`SnapshotStreamResponder::emit_next`] and send the returned frame.
//! `emit_next` builds exactly ONE bounded package from LIVE state and
//! re-enqueues the token if the stream has more — the self-enqueueing
//! shape that yields the loop to every other arm between packages. No
//! borrow is held across a yield; no ledger copy is frozen (only the
//! sorted key list + the small tally capture, see
//! `cluster_state::stream`).
//!
//! Concurrent streams (several joiners at once) are just several
//! entries in the stream map, each self-enqueueing independently; the
//! per-wakeup unit of work stays one package. The map is bounded
//! ([`MAX_ACTIVE_STREAMS`], WARN + refuse beyond) and idle streams
//! expire ([`STREAM_IDLE_TTL`]) so a vanished requester cannot pin
//! state — its leg death also surfaces as a send error, which the
//! coordinator reports via [`SnapshotStreamResponder::abort_stream`].
//!
//! Duplicate wake tokens are benign by design: a token for a finished /
//! aborted stream finds no entry (`emit_next` returns `None`), and a
//! reposition's extra token only lets the stream advance one package
//! sooner. The token channel is unbounded but its population is bounded
//! by (streams × in-flight tokens per stream ≤ 2).
//!
//! # Requester tracker ([`InboundSnapshotStreams`])
//!
//! Tracks per-responder inbound progress (stream id, the canonical-key
//! cursor off each package frame, the done latch) so the pull paths —
//! the digest-reactive pull, the timer-driven recovery pull, and the
//! bootstrap re-request — RESUME an interrupted stream (same stream id,
//! `resume_after` = last cursor) instead of restarting a 100 MB
//! transfer from package 0 (the resume-from-cursor shape of the old
//! re-request cadence).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{Destination, DistributedMessage};

use crate::anti_entropy::reply_destination;
use crate::cluster_state::{ClusterState, SnapshotStreamPlan};

/// Cap on concurrently-active outbound streams per responder. 16 covers
/// every realistic burst (a whole fleet re-bootstrapping through one
/// responder); beyond it a new request is refused with a WARN — the
/// requester's pull cadence retries and other responders answer too.
pub(crate) const MAX_ACTIVE_STREAMS: usize = 16;

/// Idle TTL for an outbound stream: a stream that neither accepted a
/// resume nor emitted a package for this long is dropped (the requester
/// vanished or lost interest; a live requester re-requests and resumes
/// from its cursor). Generous — covers a slow consumer draining a
/// backlog — while still bounding abandoned-state lifetime.
pub(crate) const STREAM_IDLE_TTL: Duration = Duration::from_secs(120);

struct OutboundStream {
    requester: String,
    requester_is_observer: bool,
    plan: SnapshotStreamPlan,
    seq: u64,
    last_activity: Instant,
}

/// Responder-side driver: the stream map + the self-wake channel. One
/// per coordinator (any role serves pulls — `cluster_state` is
/// replicated).
pub struct SnapshotStreamResponder {
    node_id: String,
    streams: HashMap<String, OutboundStream>,
    wake_tx: tokio::sync::mpsc::UnboundedSender<String>,
    wake_rx: tokio::sync::mpsc::UnboundedReceiver<String>,
}

impl SnapshotStreamResponder {
    pub fn new(node_id: &str) -> Self {
        let (wake_tx, wake_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            node_id: node_id.to_string(),
            streams: HashMap::new(),
            wake_tx,
            wake_rx,
        }
    }

    /// Handle one `RequestSnapshotStream`: create the plan (or
    /// reposition the still-alive one the requester is resuming) and
    /// enqueue the stream's wake token. Capture cost on the loop: one
    /// sorted key list + the small tally clone — never a ledger copy.
    pub fn accept_request<I: Identifier>(
        &mut self,
        state: &ClusterState<I>,
        requester: &str,
        requester_is_observer: bool,
        stream_id: &str,
        resume_after: Option<&str>,
        task_ranges: &[u16],
    ) {
        self.expire_idle();
        if let Some(existing) = self.streams.get_mut(stream_id) {
            // Same-stream resume: keep the original plan (and its
            // tally capture — provably safe, see the plan's doc),
            // skip forward to the requester's cursor. The plan's range
            // filter was fixed at creation (the requester's divergent set is
            // a property of THAT pull); a resume re-request only advances the
            // cursor, never re-scopes the ranges.
            existing.plan.reposition(resume_after);
            existing.last_activity = Instant::now();
            let _ = self.wake_tx.send(stream_id.to_string());
            return;
        }
        if self.streams.len() >= MAX_ACTIVE_STREAMS {
            tracing::warn!(
                stream_id,
                requester,
                active = self.streams.len(),
                "snapshot-stream request refused: responder at max active \
                 streams (the requester's pull cadence retries; other \
                 responders can serve it meanwhile)"
            );
            return;
        }
        tracing::info!(
            stream_id,
            requester,
            resume = resume_after.is_some(),
            tasks = state.task_count(),
            "snapshot stream opened"
        );
        self.streams.insert(
            stream_id.to_string(),
            OutboundStream {
                requester: requester.to_string(),
                requester_is_observer,
                plan: SnapshotStreamPlan::new(state, resume_after, task_ranges),
                seq: 0,
                last_activity: Instant::now(),
            },
        );
        let _ = self.wake_tx.send(stream_id.to_string());
    }

    /// The select-loop arm's future: the next stream wake token.
    /// Cancel-safe (a single mpsc recv — the token is consumed only by
    /// the poll that completes). Never resolves while no stream has
    /// pending work (tokens exist only when work exists), so the arm
    /// parks exactly like every other quiescent arm.
    pub async fn next_wake(&mut self) -> String {
        self.wake_rx
            .recv()
            .await
            .expect("wake channel cannot close: self holds the sender")
    }

    /// Build + return the woken stream's next package frame (and the
    /// destination typed off the requester's declared role). Exactly
    /// ONE bounded package per call; re-enqueues the token when the
    /// stream has more. `None` when the token was stale (stream
    /// finished / aborted / expired) — the caller simply continues.
    pub fn emit_next<I: Identifier>(
        &mut self,
        stream_id: &str,
        state: &ClusterState<I>,
        timestamp: f64,
    ) -> Option<(Destination, DistributedMessage<I>)> {
        let stream = self.streams.get_mut(stream_id)?;
        let built = match stream.plan.next_package(state) {
            None => {
                self.streams.remove(stream_id);
                return None;
            }
            Some(Err(e)) => {
                // Loud-and-drop: a payload that cannot encode cannot be
                // healed by retrying the same state; the requester's
                // pull cadence re-requests (possibly against a mutated,
                // encodable ledger) — same WARN-shape as the old
                // serialize-failure path.
                tracing::warn!(
                    stream_id,
                    error = %e,
                    "snapshot-stream package build failed; dropping stream"
                );
                self.streams.remove(stream_id);
                return None;
            }
            Some(Ok(pkg)) => pkg,
        };
        stream.seq += 1;
        stream.last_activity = Instant::now();
        let frame = DistributedMessage::SnapshotStreamPackage {
            target: None,
            sender_id: self.node_id.clone(),
            timestamp,
            stream_id: stream_id.to_string(),
            seq: stream.seq - 1,
            cursor: built.cursor,
            payload: built.payload,
            done: built.done,
        };
        let dst = reply_destination(&stream.requester, stream.requester_is_observer);
        if built.done {
            tracing::info!(
                stream_id,
                requester = %stream.requester,
                packages = stream.seq,
                "snapshot stream complete"
            );
            self.streams.remove(stream_id);
        } else {
            // Self-enqueue: one package per loop wakeup, every other
            // arm gets the loop in between.
            let _ = self.wake_tx.send(stream_id.to_string());
        }
        Some((dst, frame))
    }

    /// Drop a stream whose package send failed (dead leg) and return the
    /// `(requester_id, requester_is_observer)` of the aborted stream, so the
    /// caller can send the requester a `PullFail` (the pull-model
    /// indirect-delivery fallback: a chunk that could not be DELIVERED over
    /// the direct leg signals the requester to fall to its next pull target,
    /// the one frame routed via the relay-toward-the-role-holder path).
    /// `None` when the stream id was unknown (already done/expired) — there
    /// is no requester to notify. The requester also resumes from its cursor
    /// on a plain re-request, so the `PullFail` is an OPTIMISTIC fast-path
    /// fallback, not the sole recovery.
    pub fn abort_stream(&mut self, stream_id: &str) -> Option<(String, bool)> {
        let removed = self.streams.remove(stream_id)?;
        tracing::debug!(stream_id, "snapshot stream aborted (send failure)");
        Some((removed.requester, removed.requester_is_observer))
    }

    fn expire_idle(&mut self) {
        self.streams.retain(|stream_id, s| {
            let live = s.last_activity.elapsed() < STREAM_IDLE_TTL;
            if !live {
                tracing::debug!(
                    stream_id,
                    requester = %s.requester,
                    "snapshot stream expired idle (requester vanished or resumed elsewhere)"
                );
            }
            live
        });
    }

    /// Test hook: number of live outbound streams.
    #[cfg(test)]
    pub(crate) fn active_streams(&self) -> usize {
        self.streams.len()
    }

    /// Test hook: drain one pending wake token without awaiting (the
    /// loop-less unit tests drive the responder synchronously).
    #[cfg(test)]
    pub(crate) fn try_next_wake(&mut self) -> Option<String> {
        self.wake_rx.try_recv().ok()
    }
}

#[derive(Debug, Clone)]
struct InboundProgress {
    stream_id: String,
    cursor: Option<String>,
    done: bool,
    /// Packages successfully applied off this stream (decode failures
    /// never count — see `note_package`'s caller contract).
    received: u64,
    /// Last applied-package instant; gates [`InboundSnapshotStreams::
    /// mid_transfer`] so a DEAD stream cannot hold a terminal exit
    /// hostage forever.
    last_progress: Instant,
}

/// Requester-side per-responder stream progress: mints stream ids,
/// records each package's cursor/done off the frame fields (the payload
/// stays opaque to the tracker), and answers "what should the next pull
/// to responder R carry" — a fresh id, or the interrupted stream's id +
/// `resume_after` cursor.
pub struct InboundSnapshotStreams {
    node_id: String,
    mint: u64,
    by_responder: HashMap<String, InboundProgress>,
}

impl InboundSnapshotStreams {
    pub fn new(node_id: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            mint: 0,
            by_responder: HashMap::new(),
        }
    }

    /// The `(stream_id, resume_after)` pair for the next pull to
    /// `responder`: resumes the tracked incomplete stream, or mints a
    /// fresh id (`<node-id>/<counter>` — unique without an RNG: node
    /// ids are cluster-unique, the counter is per-tracker).
    pub fn request_params(&mut self, responder: &str) -> (String, Option<String>) {
        if let Some(p) = self.by_responder.get(responder)
            && !p.done
        {
            return (p.stream_id.clone(), p.cursor.clone());
        }
        let stream_id = format!("{}/{}", self.node_id, self.mint);
        self.mint += 1;
        self.by_responder.insert(
            responder.to_string(),
            InboundProgress {
                stream_id: stream_id.clone(),
                cursor: None,
                done: false,
                received: 0,
                last_progress: Instant::now(),
            },
        );
        (stream_id, None)
    }

    /// Record one received package's progress facts (call AFTER the
    /// payload merged so a decode failure never advances the cursor).
    /// Packages for a stream this tracker did not mint (a stale
    /// attempt's late packages) are ignored — their payload still
    /// merged idempotently, they just carry no progress signal.
    pub fn note_package(
        &mut self,
        responder: &str,
        stream_id: &str,
        cursor: Option<&str>,
        done: bool,
    ) {
        if let Some(p) = self.by_responder.get_mut(responder)
            && p.stream_id == stream_id
        {
            if let Some(c) = cursor {
                // Monotone: an out-of-order/duplicate package can only
                // trail; never regress the resume point.
                if p.cursor.as_deref().is_none_or(|prev| c > prev) {
                    p.cursor = Some(c.to_string());
                }
            }
            p.done |= done;
            p.received += 1;
            p.last_progress = Instant::now();
        }
    }

    /// Whether a PARTIALLY-APPLIED inbound stream is still live: some
    /// stream has delivered at least one package, is not `done`, and
    /// made progress within `idle_ttl`. The terminal-exit gate on the
    /// reporting roles reads this so a run-terminal latched by a
    /// stream's HEAD package does not exit the loop with a half-merged
    /// mirror (the head lands first by design; exiting on it would
    /// report a complete run with zero counts). Bounded by `idle_ttl` —
    /// a stream whose responder died mid-transfer stops holding the
    /// exit once it stalls past the TTL (the double-fault edge: run
    /// complete AND the only responder gone), and a request that was
    /// NEVER answered holds nothing (received == 0 — there is no
    /// partial merge to wait out).
    pub fn mid_transfer(&self, idle_ttl: Duration) -> bool {
        self.by_responder
            .values()
            .any(|p| !p.done && p.received > 0 && p.last_progress.elapsed() < idle_ttl)
    }
}

/// Test support: the full package-frame sequence a responder would emit
/// for `donor` (fresh stream, no resume), built synchronously. Lets a
/// scripted test peer answer a snapshot pull exactly like a production
/// responder — same plan, same codec, same frame fields — without
/// standing up a responder loop.
#[cfg(test)]
pub(crate) fn stream_frames_for_test<I: Identifier>(
    donor: &ClusterState<I>,
    responder_id: &str,
    stream_id: &str,
) -> Vec<DistributedMessage<I>> {
    let mut plan = SnapshotStreamPlan::new(donor, None, &[]);
    let mut out = Vec::new();
    while let Some(built) = plan.next_package(donor) {
        let p = built.expect("test snapshot package encodes");
        out.push(DistributedMessage::SnapshotStreamPackage {
            target: None,
            sender_id: responder_id.to_string(),
            timestamp: 0.0,
            stream_id: stream_id.to_string(),
            seq: out.len() as u64,
            cursor: p.cursor,
            payload: p.payload,
            done: p.done,
        });
    }
    out
}

#[cfg(test)]
mod tests;
