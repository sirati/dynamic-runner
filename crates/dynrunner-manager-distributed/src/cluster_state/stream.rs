//! Snapshot-STREAM partition policy + package payload codec.
//!
//! Single concern: HOW one replica's `ClusterState` is cut into a
//! sequence of bounded, independently-mergeable package payloads (and
//! how a payload is encoded/decoded for the wire). The stream replaces
//! the monolithic snapshot-JSON transfer: a production 67k-task ledger
//! serialized to ~100 MB in ONE synchronous `serde_json::to_string` on
//! the coordinator's `current_thread` runtime (0.5–2 s of solid CPU per
//! pull, and the serialize-once cache never hit mid-phase because every
//! completion invalidated it). With the stream, a responder produces
//! ONE bounded package per loop wakeup — no frozen ledger copy, no
//! borrow across yields, no `spawn_blocking`.
//!
//! # Why no consistent cut is needed
//!
//! The CRDT join (`ClusterState::restore`) is idempotent, commutative
//! and monotone, so each package is a valid PARTIAL snapshot on its
//! own: the receiver merges packages as they arrive, interleaved with
//! live mutation broadcasts, and anything that mutated behind the
//! stream's cursor converges through gossip / the anti-entropy cadence.
//! Every payload is a partial [`ClusterStateSnapshot`] routed through
//! the ONE existing `restore` lattice — there is no second merge path.
//!
//! # The three partitions
//!
//! - **head** (first package): every replicated field EXCEPT the task
//!   bulk and the join-bumped tally map — see
//!   [`ClusterState::stream_head`]. Small (control-plane facts), sent
//!   first so a joiner learns membership/primary/run-latches before the
//!   bulk transfer runs.
//! - **task batches**: `tasks` + the co-keyed `task_outputs` entries,
//!   in CANONICAL sorted-key order (the total order every replica
//!   shares — which is what makes the resume cursor responder-agnostic),
//!   each batch bounded by [`PACKAGE_BYTE_BUDGET`].
//! - **tail** (last package): `phase_event_tallies` — the ONE grow-max
//!   map the task-merge join BUMPS (#358). The order rule
//!   "states-before-fields" holds per-STREAM: a tally import must never
//!   precede the task states whose events it counts, or the receiver
//!   bumps the same event again after importing it and the grow-max
//!   lattice freezes the overshoot in permanently (and spreads it: an
//!   overshot replica looks "ahead" to every digest comparison).
//!
//! # Tally capture-at-start
//!
//! The tail's tally map is CLONED when the plan is created, not when
//! the tail is sent. Task batches are built lazily from LIVE state, so
//! a batch can carry a NEWER state than plan-creation time — that is
//! safe (the receiver bumps for the newer terminal it merges, and the
//! older captured tally aliases below it under `max`). The reverse —
//! a tally captured AFTER a batch was sent, counting a terminal event
//! whose state the stream already shipped in its OLDER form — is the
//! permanent-overshoot hazard above. Capture-at-start makes
//! `tally ⊑ events(states delivered)` by ledger monotonicity.
//!
//! A plan created WITH a resume cursor for a stream the responder no
//! longer holds (`SnapshotStreamPlan::new` with `resume_after: Some`)
//! OMITS the tail entirely: the entries before the cursor were shipped
//! by an earlier, possibly-older stream, so no capture time is provably
//! safe. The receiver's tallies then converge through the anti-entropy
//! digest (which folds them) on the next pull — rare, and strictly
//! better than risking a permanent overshoot. A resume that finds the
//! ORIGINAL stream still alive keeps its original capture (see
//! `snapshot_stream::SnapshotStreamResponder`).

use std::collections::HashMap;

use base64::Engine as _;
use dynrunner_core::{Identifier, PhaseId};

use super::types::PhaseTally;
use super::{ClusterState, ClusterStateSnapshot};

/// Soft byte budget for one task-batch package's RAW CBOR payload
/// (pre-base64). Mid of the design band (1–4 MiB): large enough that a
/// 100 MB ledger streams in ~50 packages, small enough that building +
/// encoding one package is a bounded ~ms unit of loop work. Soft: a
/// single task entry larger than the whole budget ships in a package
/// of its own (entries are never split), and the wire's chunking layer
/// covers the pathological single-entry-over-96MiB case.
pub(crate) const PACKAGE_BYTE_BUDGET: usize = 2 * 1024 * 1024;

/// Secondary count bound per task-batch package, so a ledger of tiny
/// tasks still produces packages a receiver merges in bounded work.
pub(crate) const PACKAGE_MAX_TASKS: usize = 4096;

/// Encode one partial snapshot as a wire payload: CBOR (compact,
/// escape-free — the reason the stream is not JSON) wrapped in base64
/// (the wire envelope is JSON, which has no raw-bytes representation).
pub fn encode_stream_payload<I: Identifier>(
    snap: &ClusterStateSnapshot<I>,
) -> Result<String, String> {
    let mut buf = Vec::new();
    ciborium::into_writer(snap, &mut buf)
        .map_err(|e| format!("snapshot-stream payload CBOR encode: {e}"))?;
    Ok(base64::engine::general_purpose::STANDARD.encode(buf))
}

/// Decode one wire payload back into the partial snapshot. The inverse
/// of [`encode_stream_payload`]; every receiver routes the result
/// through `ClusterState::restore` (the one merge path).
pub fn decode_stream_payload<I: Identifier>(
    payload: &str,
) -> Result<ClusterStateSnapshot<I>, String> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(payload)
        .map_err(|e| format!("snapshot-stream payload base64 decode: {e}"))?;
    ciborium::from_reader(bytes.as_slice())
        .map_err(|e| format!("snapshot-stream payload CBOR decode: {e}"))
}

/// One built package, ready for the responder to wrap in a
/// `SnapshotStreamPackage` frame.
pub struct StreamPackage {
    /// Base64-wrapped CBOR of the partial snapshot.
    pub payload: String,
    /// The canonical-order cursor after this package: the highest task
    /// key it advanced PAST (including keys skipped because they
    /// vanished from the live map). `None` for the head/tail packages.
    pub cursor: Option<String>,
    /// `true` when this is the stream's final package.
    pub done: bool,
}

/// The responder-side per-stream iteration state: the sorted key list
/// captured at stream start, the position cursor, and the tally capture
/// for the tail. Owns NO wire or scheduling concern (that is
/// `snapshot_stream::SnapshotStreamResponder`); this is purely "what
/// does the next package contain".
pub struct SnapshotStreamPlan {
    /// Canonical iteration order: the task keys present at plan
    /// creation, SORTED (the total order every replica shares).
    /// ~1 MB for a 67k-key ledger — one O(n log n) sort per stream.
    /// Tasks added after capture ride live gossip / the next pull;
    /// tasks that vanish are skipped at batch-build time.
    keys: Vec<String>,
    /// Next index into `keys` to ship.
    pos: usize,
    head_sent: bool,
    /// Tail capture (see the module doc's capture-at-start rule).
    /// `None` on a fresh-from-cursor resume plan — the tail is omitted
    /// and tallies heal via anti-entropy.
    tallies: Option<HashMap<(PhaseId, PhaseTally), u32>>,
    tail_sent: bool,
}

impl SnapshotStreamPlan {
    /// Capture a plan over the CURRENT ledger. `resume_after: Some(k)`
    /// builds a fresh-resume plan: only keys strictly after `k`, and NO
    /// tail capture (see the module doc).
    pub fn new<I: Identifier>(state: &ClusterState<I>, resume_after: Option<&str>) -> Self {
        // The key UNIVERSE is the LOGICAL ledger: fat in-memory keys ∪
        // settled (spilled) keys — a settled entry is still this
        // replica's ledger entry, served per-key from the spill file at
        // batch-build time. The union keeps the canonical sorted-key
        // order (and with it the responder-agnostic resume cursor)
        // identical to the all-in-memory iteration.
        let all_keys = || {
            state
                .tasks
                .keys()
                .chain(state.settled_entries().map(|(k, _)| k))
        };
        let mut keys: Vec<String> = match resume_after {
            None => all_keys().cloned().collect(),
            Some(cursor) => all_keys()
                .filter(|k| k.as_str() > cursor)
                .cloned()
                .collect(),
        };
        keys.sort_unstable();
        Self {
            keys,
            pos: 0,
            head_sent: false,
            // An EMPTY capture needs no tail package at all (a max-merge
            // of an empty map is a no-op) — `done` then rides the last
            // batch (or the head of an empty ledger).
            tallies: resume_after
                .is_none()
                .then(|| state.phase_event_tallies.clone())
                .filter(|t| !t.is_empty()),
            tail_sent: false,
        }
    }

    /// Re-position an ALIVE plan for a resume re-request: skip forward
    /// to the first key strictly after `resume_after` (never backward —
    /// the requester's cursor can only trail ours, and re-shipping is
    /// merely redundant, not wrong) and re-arm the head (it is small,
    /// and re-sending it refreshes the control-plane facts). The tail
    /// capture is KEPT: every key before the new position was delivered
    /// by THIS stream (possibly the pre-drop attempt), all at states no
    /// older than the capture, so the capture-at-start safety argument
    /// still holds.
    pub fn reposition(&mut self, resume_after: Option<&str>) {
        if let Some(cursor) = resume_after {
            let next = self.keys.partition_point(|k| k.as_str() <= cursor);
            self.pos = self.pos.max(next);
        }
        self.head_sent = false;
        self.tail_sent = false;
    }

    /// Whether every package of this plan has been emitted.
    pub fn complete(&self) -> bool {
        self.head_sent && self.pos >= self.keys.len() && (self.tail_sent || self.tallies.is_none())
    }

    /// Build the next package: head → task batches → tail. Returns
    /// `None` once complete. One call is one bounded unit of loop work
    /// (the responder schedules exactly one per wakeup).
    pub fn next_package<I: Identifier>(
        &mut self,
        state: &ClusterState<I>,
    ) -> Option<Result<StreamPackage, String>> {
        if self.complete() {
            return None;
        }
        if !self.head_sent {
            self.head_sent = true;
            let head = state.stream_head();
            return Some(encode_stream_payload(&head).map(|payload| StreamPackage {
                payload,
                cursor: None,
                done: self.complete(),
            }));
        }
        if self.pos < self.keys.len() {
            let mut batch = ClusterStateSnapshot::<I>::default();
            let mut raw_bytes = 0usize;
            let mut cursor: Option<String> = None;
            while self.pos < self.keys.len()
                && batch.tasks.len() < PACKAGE_MAX_TASKS
                && (batch.tasks.is_empty() || raw_bytes < PACKAGE_BYTE_BUDGET)
            {
                let key = &self.keys[self.pos];
                self.pos += 1;
                cursor = Some(key.clone());
                // Resolve the entry wherever its body lives: the fat map,
                // or — for a SETTLED key — the spill file, decoded
                // per-entry (`settled_record`: its own read fd, never past
                // the committed offset; transient — no fat residency). A
                // key that vanished from BOTH (or an unreadable record) is
                // skipped exactly as before: the plan holds keys, not
                // entries, and the receiver converges via anti-entropy.
                let (task_state, settled_outputs) = match state.tasks.get(key) {
                    Some(s) => (s.clone(), None),
                    None => match state.settled_record(key) {
                        Some((s, outputs)) => (s, outputs),
                        None => continue,
                    },
                };
                // Size the entry by encoding it once into a scratch
                // buffer — bounded per-batch work, and the only way to
                // get an exact bound without a second full encode of a
                // speculative batch.
                let mut scratch = Vec::new();
                if let Err(e) = ciborium::into_writer(&task_state, &mut scratch) {
                    return Some(Err(format!("snapshot-stream task entry encode: {e}")));
                }
                raw_bytes += scratch.len() + key.len() + 16;
                batch.tasks.insert(key.clone(), task_state);
                // The co-keyed output entry rides the SAME package so
                // the restore join reads it co-present (TS-3). The
                // in-memory map is the one hot source; the record's
                // embedded copy is the fallback shape (equal by
                // construction — first-write-wins on both ends).
                if let Some(outputs) = state
                    .task_outputs
                    .get(key)
                    .cloned()
                    .or(settled_outputs)
                {
                    raw_bytes += key.len() + 64;
                    batch.task_outputs.insert(key.clone(), outputs);
                }
            }
            return Some(encode_stream_payload(&batch).map(|payload| StreamPackage {
                payload,
                cursor,
                done: self.complete(),
            }));
        }
        // Tail: the captured tally map (`complete()` returned false and
        // both earlier partitions are exhausted, so `tallies` is Some).
        self.tail_sent = true;
        let tail = ClusterStateSnapshot::<I> {
            phase_event_tallies: self
                .tallies
                .clone()
                .expect("tail reached only when a capture exists"),
            ..Default::default()
        };
        Some(encode_stream_payload(&tail).map(|payload| StreamPackage {
            payload,
            cursor: None,
            done: true,
        }))
    }
}
