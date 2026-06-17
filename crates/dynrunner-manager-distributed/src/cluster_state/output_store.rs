//! Always-on node-local output store: the durable disk home a completed
//! task's [`TaskOutputs`] payload lands in AT THE INSTANT OF COMPLETION,
//! so the payload is NEVER even transiently retained in a long-lived
//! resident map (the owner's hard zero-residence requirement).
//!
//! Single concern: WHERE a completed task's output payload physically
//! lives from completion onward, and the digest TERM + COUNT it
//! contributes — independent of the task's settle status and independent
//! of whether THIS node ever reads outputs back.
//!
//! # Decoupled from the settled-spill driver
//!
//! This store is opened at CONSTRUCTION (the coordinator's
//! [`crate::settled_spill::SettledSpillDriver::start`] attaches its
//! segment before the first apply), not lazily on the first settle sweep.
//! The settled-spill driver moves task STATE fixed-points to disk on a
//! cadence; this store moves the OUTPUT payload to disk synchronously on
//! the `TaskCompleted` apply (write-through-then-drop). The two are
//! orthogonal storage backends.
//!
//! # Every role persists (zero residence on EVERY role)
//!
//! EVERY role — primary, observer, AND plain secondary — persists its
//! outputs to node-local disk (`retains_payload = true` at the one
//! production attach seam) and folds the SAME digest term, so zero
//! payload bytes are ever resident and every node converges the SAME
//! outputs digest. The digest ACCUMULATOR (`outputs_hash_acc` +
//! `outputs_count`) is folded at apply on every node because the output
//! VALUE is in hand at the `TaskCompleted` apply on all of them; the
//! digest is over the LOGICAL outputs (which arrive via the replicated
//! `TaskCompleted`), so the term is identical on every role.
//!
//! Persisting on every role is what makes promotion loss-free: a
//! promoted primary inherits FAT outputs by snapshot + SETTLED outputs
//! by `settled_base`, and its own completions are already on its disk
//! store, so it never lacks an output it must read back
//! (`gather_predecessor_outputs` at dispatch, `phase_task_outputs` at
//! `on_phase_end`, the snapshot-stream output-serve).
//!
//! # Degrade / no-disk-home fallback
//!
//! When no disk segment is attached (a bare `ClusterState::new()` test
//! fixture, or a node whose work dir was unavailable at driver start) the
//! node cannot persist the payload, so the apply path falls back to
//! RESIDENT retention in `task_outputs` — reads still work, and the
//! digest folds the resident value the legacy way. Zero residence is
//! achieved exactly when a disk home exists (always, in production); the
//! fallback keeps correctness universal.
//!
//! # Record framing / concurrency
//!
//! One record = `u32-LE length prefix` + ciborium CBOR of
//! [`OutputRecord`] `{ hash, outputs }`. Appended + flushed synchronously
//! at apply (no `spawn_blocking`: the owner forbids the transient
//! residence a deferred write implies); the committed offset is published
//! (`Release`) after the flush, and readers hold their own positionless
//! `pread` fd and never read past it — the same one-writer / lock-free-
//! readers protocol as the settled spill. The file is scratch (truncate
//! on open, never reused across respawns).

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dynrunner_core::{Identifier, TaskOutputs};
use serde::{Deserialize, Serialize};

use crate::warn_throttle::WarnThrottle;

/// Hash one hashable value — the same default-hasher fold `digest.rs` /
/// `merge.rs` / `settled.rs` use (process-stable; cross-build stability
/// not required). The output digest TERM is `hash_one((hash, &outputs))`,
/// byte-identical to the term the resident-map fold and the settled-spill
/// fold compute, so a node that persists, a node that retains resident,
/// and a node that only folds (a secondary) all contribute the SAME term.
fn hash_one<H: std::hash::Hash>(value: H) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Bytes of the `u32`-LE length prefix in front of every record body.
const RECORD_PREFIX_LEN: usize = 4;

/// The on-disk record: one completed task's keyed outputs, keyed by the
/// task's wire content hash. Serialized as CBOR behind a length prefix.
#[derive(Serialize, Deserialize)]
struct OutputRecord {
    #[allow(dead_code)]
    hash: String,
    outputs: TaskOutputs,
}

/// Slim in-memory index entry: where one task's output record lives on
/// disk. Indexed once the payload is persisted (a disk home is attached);
/// in the no-disk-home fallback the store indexes nothing and reads serve
/// from the resident map.
#[derive(Debug, Clone)]
struct OutputEntry {
    offset: u64,
    len: u32,
    /// The `(hash, outputs)` digest term this entry contributed to
    /// `outputs_hash_acc`, stored so a value-preserving move could
    /// subtract exactly what was added.
    #[allow(dead_code)]
    digest_contribution: u64,
}

/// The append fd + published committed offset + a shared read fd. Present
/// once the driver attaches a segment at construction; `None` keeps the
/// store in resident-fallback mode.
struct OutputSegment {
    write_file: File,
    write_offset: u64,
    committed: Arc<AtomicU64>,
    read_file: Arc<File>,
}

/// The always-on output store: optional disk segment + slim index + the
/// always-on output-digest accumulator. Owned by `ClusterState` as a
/// NODE-LOCAL backend for replicated output data (the accumulator is a
/// pure derivation of the replicated `TaskCompleted` stream; the file is
/// node-local scratch).
///
/// Manual [`Default`] (the decode-skip throttle below carries no
/// `Default`): every other field is `Default`; the throttle is built
/// with its fixed interval.
pub(crate) struct OutputStore {
    /// `Some` once a disk home is attached; `None` keeps the store in
    /// resident-fallback mode (reads served from `task_outputs`).
    segment: Option<OutputSegment>,
    /// `hash → on-disk location`, populated once a disk home is attached.
    /// Empty in the no-disk-home fallback (reads serve from the resident
    /// map) and before a segment attaches.
    index: HashMap<String, OutputEntry>,
    /// The set of hashes whose `(hash, outputs)` term has been folded
    /// into the accumulator — the first-FOLD-wins dedup guard, on EVERY
    /// node (a no-disk-home node has no `index` to dedup against, but the
    /// accumulator must still count each output exactly once). Holds only
    /// the small hash STRING, NOT the payload, so it does not violate
    /// payload zero-residence.
    folded: HashSet<String>,
    /// XOR accumulator over `(hash, outputs)` terms folded at apply on
    /// EVERY node (disk or not) — the always-on half of
    /// the `task_outputs_hash` fold. `digest()` seeds the resident fold
    /// with this (XOR associativity), the always-on twin of the settled
    /// store's `task_outputs_hash_acc`.
    outputs_hash_acc: u64,
    /// Count of outputs folded into `outputs_hash_acc` — the always-on
    /// half of `task_outputs_count`.
    outputs_count: u64,
    /// Latched on the first write error: payload persistence stops for
    /// the process lifetime (fat-but-correct: the apply path falls back
    /// to resident retention so reads still work).
    degraded: bool,
    /// Read-path log-hygiene: an UNDECODABLE record body (an old on-disk
    /// format inherited across the promotion seam from a prior-version
    /// donor's segment, say) is SKIPPED — the index entry's framing
    /// (`offset`/`len`) is intact, so the read locates exactly past it and
    /// every other record still decodes. Without throttling, an adopted
    /// old-format segment re-emits one ERROR per record on every output
    /// read (`outputs_for_hash` consults this store FIRST) — per-record
    /// spam for the process lifetime. This gate rolls those into ONE WARN
    /// per interval naming the skipped count. Interior-mutable (`Mutex`)
    /// so the read path stays `&self`; node-local runtime state, reset on
    /// clone. A STRUCTURAL fault (read past the committed offset, or an IO
    /// error) stays a loud un-throttled ERROR — it is distinct from an
    /// old-format body and signals real incoherence/corruption.
    decode_skip_warn: Mutex<WarnThrottle>,
}

/// Roll up undecodable-record skips into at most one WARN this often.
/// Matched to the settled spill's [`crate::cluster_state::settled`]
/// decode-skip throttle (a stuck fault must surface, but not once per
/// record).
const DECODE_SKIP_WARN_INTERVAL: Duration = Duration::from_secs(60);

impl Default for OutputStore {
    fn default() -> Self {
        Self {
            segment: None,
            index: HashMap::new(),
            folded: HashSet::new(),
            outputs_hash_acc: 0,
            outputs_count: 0,
            degraded: false,
            decode_skip_warn: Mutex::new(WarnThrottle::new(DECODE_SKIP_WARN_INTERVAL)),
        }
    }
}

impl std::fmt::Debug for OutputStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutputStore")
            .field("has_segment", &self.segment.is_some())
            .field("index", &self.index.len())
            .field("outputs_count", &self.outputs_count)
            .field("degraded", &self.degraded)
            .finish()
    }
}

/// `Clone` is a read-only clone (matches `SettledStore`): the index +
/// shared read fd carry over so a cloned replica keeps serving output
/// reads; the WRITER affiliation (the append fd) is node-local runtime
/// state and the clone is marked degraded (one-writer rule: a clone never
/// appends to the source's file). Required so the promotion-handover
/// value stays `Clone`.
impl Clone for OutputStore {
    fn clone(&self) -> Self {
        Self {
            segment: self.segment.as_ref().map(|seg| OutputSegment {
                // The clone never appends (degraded below); the write fd
                // is a read-fd placeholder kept only to preserve the
                // struct shape — reads use `read_file`.
                write_file: seg
                    .read_file
                    .try_clone()
                    .expect("clone read fd as write placeholder"),
                write_offset: seg.committed.load(Ordering::Acquire),
                committed: Arc::clone(&seg.committed),
                read_file: Arc::clone(&seg.read_file),
            }),
            index: self.index.clone(),
            folded: self.folded.clone(),
            outputs_hash_acc: self.outputs_hash_acc,
            outputs_count: self.outputs_count,
            degraded: true,
            // Node-local runtime state (like the writer affiliation): the
            // clone gets a fresh throttle, so its first decode-skip on this
            // replica emits immediately rather than inheriting the donor's
            // suppression window.
            decode_skip_warn: Mutex::new(WarnThrottle::new(DECODE_SKIP_WARN_INTERVAL)),
        }
    }
}

impl OutputStore {
    /// Attach the disk segment (read fd + committed-offset cell + the
    /// writer's append fd). Called once by the spill driver at
    /// construction; until then the store is in resident-fallback mode.
    pub(crate) fn attach_segment(
        &mut self,
        write_file: File,
        read_file: Arc<File>,
        committed: Arc<AtomicU64>,
    ) {
        self.segment = Some(OutputSegment {
            write_file,
            write_offset: 0,
            committed,
            read_file,
        });
        self.degraded = false;
    }

    /// Whether a disk home is attached AND able to persist payloads (not
    /// degraded). When `false` the apply path must retain the payload
    /// resident so reads still work.
    pub(crate) fn can_persist(&self) -> bool {
        !self.degraded && self.segment.is_some()
    }

    /// Whether a disk segment has been attached at all (the driver ran its
    /// construction attach). When `false` the store is in pure
    /// resident-fallback mode — a bare `ClusterState::new()` fixture, or
    /// before the coordinator's driver attaches. A node with no attach
    /// must retain outputs RESIDENT (the safe legacy behavior).
    pub(crate) fn is_attached(&self) -> bool {
        self.segment.is_some()
    }

    pub(super) fn outputs_hash_acc(&self) -> u64 {
        self.outputs_hash_acc
    }

    pub(super) fn outputs_count(&self) -> u64 {
        self.outputs_count
    }

    /// Total committed bytes (observability).
    pub(crate) fn committed_bytes(&self) -> u64 {
        self.segment
            .as_ref()
            .map(|s| s.committed.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    /// Number of indexed (disk-resident) output records.
    pub(crate) fn index_len(&self) -> usize {
        self.index.len()
    }

    /// Fold one completed task's output into the always-on digest
    /// accumulator (every node, regardless of persistence) and return the
    /// per-output digest term. First-fold-wins: a hash already folded
    /// (an idempotent re-record) returns `None` and does NOT double-fold.
    fn fold_term(&mut self, hash: &str, outputs: &TaskOutputs) -> Option<u64> {
        if !self.folded.insert(hash.to_string()) {
            // Already folded (an idempotent re-record — e.g. the snapshot
            // `task_outputs` restore loop after the `tasks` loop already
            // recorded the same hash). Do NOT double-fold.
            return None;
        }
        let term = hash_one((hash, outputs));
        self.outputs_hash_acc ^= term;
        self.outputs_count += 1;
        Some(term)
    }

    /// Persist one completed task's output to disk + index it. Only a node
    /// with a writable segment reaches here (the caller gates on
    /// [`Self::can_persist`]). Appends the record, flushes, publishes
    /// the new committed offset (`Release`), and indexes its location.
    /// Returns `Ok(())` on success, `Err` on IO/encode failure (the
    /// caller degrades and retains resident). An already-indexed hash is a
    /// no-op (idempotent re-record).
    fn persist(&mut self, hash: &str, outputs: &TaskOutputs, term: u64) -> std::io::Result<()> {
        if self.index.contains_key(hash) {
            return Ok(());
        }
        let Some(seg) = self.segment.as_mut() else {
            return Ok(());
        };
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&[0u8; RECORD_PREFIX_LEN]);
        let record = OutputRecord {
            hash: hash.to_string(),
            outputs: outputs.clone(),
        };
        ciborium::into_writer(&record, &mut buf)
            .map_err(|e| std::io::Error::other(format!("output record encode: {e}")))?;
        let body_len = (buf.len() - RECORD_PREFIX_LEN) as u32;
        buf[..RECORD_PREFIX_LEN].copy_from_slice(&body_len.to_le_bytes());
        let offset = seg.write_offset;
        seg.write_file.write_all(&buf)?;
        // Durable in the page cache (a scratch spill, not a durability
        // file — no fsync); publish so readers may now reach the record.
        seg.write_file.flush()?;
        let len = buf.len() as u32;
        seg.write_offset += u64::from(len);
        seg.committed.store(seg.write_offset, Ordering::Release);
        self.index.insert(
            hash.to_string(),
            OutputEntry {
                offset,
                len,
                digest_contribution: term,
            },
        );
        Ok(())
    }

    /// Mark the writer degraded (a write failed): payload persistence
    /// stops process-wide; the apply path falls back to resident
    /// retention. The digest accumulator already holds this output's term
    /// (folded before the persist attempt), so convergence is unaffected.
    fn degrade(&mut self) {
        self.degraded = true;
    }

    /// Read + decode one task's persisted outputs, or `None` when the
    /// hash is not indexed (no disk home, or a task that published
    /// nothing) or the record is unreadable (logged; the caller treats it
    /// as no-output, exactly as a vanished key). Never reads past the
    /// committed offset.
    fn read_outputs(&self, hash: &str) -> Option<TaskOutputs> {
        let entry = self.index.get(hash)?;
        self.read_at(entry.offset, entry.len)
    }

    /// Locate + decode the output record at `(offset, len)` — the
    /// coordinate-keyed core [`Self::read_outputs`] (and the decode-skip
    /// test seam) share, so the locate/range-check/decode path and its log
    /// shape are spelled ONCE. Mirrors the settled spill's `read_at`:
    ///
    /// * a STRUCTURAL fault (read past the committed offset, or an IO
    ///   error) is loud, un-throttled ERROR — real incoherence/corruption;
    /// * an UNDECODABLE body (an old/foreign on-disk format) is SKIPPED
    ///   (`None`) and rolled into the throttled WARN — the index entry's
    ///   framing is intact, so the read located it exactly and every other
    ///   record still decodes; an adopted old-format segment must not spam
    ///   one ERROR per record on every read.
    fn read_at(&self, offset: u64, len: u32) -> Option<TaskOutputs> {
        let seg = self.segment.as_ref()?;
        let committed = seg.committed.load(Ordering::Acquire);
        let end = offset.checked_add(u64::from(len))?;
        if end > committed {
            tracing::error!(
                offset,
                len,
                committed,
                "output-store read past committed offset refused (index/commit incoherence)"
            );
            return None;
        }
        let mut buf = vec![0u8; len as usize];
        if let Err(e) = read_exact_at(&seg.read_file, &mut buf, offset) {
            tracing::error!(error = %e, offset, "output-store record read failed");
            return None;
        }
        let body = buf.get(RECORD_PREFIX_LEN..)?;
        match ciborium::from_reader::<OutputRecord, _>(body) {
            Ok(rec) => Some(rec.outputs),
            Err(e) => {
                // An UNDECODABLE body is an old/foreign on-disk format, NOT
                // corruption of THIS store's framing: the index carried the
                // record's exact `(offset, len)`, so the read already
                // located past it and every other record still decodes.
                // SKIP it (return `None`) and roll the skip into the
                // throttled WARN so an adopted old-format segment does not
                // spam one ERROR per record on every read.
                self.note_decode_skip(&e);
                None
            }
        }
    }

    /// Account one skipped undecodable record and, at most once per
    /// [`DECODE_SKIP_WARN_INTERVAL`], emit a rolled-up WARN naming how many
    /// were suppressed since the last emit. Distinct from the loud
    /// un-throttled ERRORs the structural-fault arms emit (read past
    /// committed, IO error) — an old-format body is expected-and-skipped, a
    /// torn/short read is real incoherence.
    fn note_decode_skip(&self, error: &dyn std::fmt::Display) {
        // A poisoned throttle lock (a prior panic mid-emit) must not gate
        // the read; recover the guard and proceed.
        let mut throttle = self
            .decode_skip_warn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(suppressed) = throttle.permit() {
            tracing::warn!(
                error = %error,
                also_skipped_since_last = suppressed,
                "skipped undecodable output-store record(s) (old/foreign on-disk \
                 format); the decodable records still load — set is not corrupted"
            );
        }
    }

    /// Test-only: drive the coordinate-keyed decode at `(offset, len)`
    /// exactly as the production read does (same range check, same log
    /// shape, same decode-skip throttle). `Some` for a decodable record,
    /// `None` for an undecodable/old-format one (skipped + counted into the
    /// throttled WARN).
    #[cfg(test)]
    pub(crate) fn read_at_for_test(&self, offset: u64, len: u32) -> Option<TaskOutputs> {
        self.read_at(offset, len)
    }

    /// Test-only: register a read-only segment over `file` whose published
    /// committed offset is `committed_to` (the bytes a reader may reach).
    /// Mirrors the production read segment without a writer, so a test can
    /// hand-place a mix of decodable + undecodable records and drive the
    /// decode path. The write fd is a read-fd placeholder (the test never
    /// appends through this store).
    #[cfg(test)]
    fn attach_read_segment_for_test(&mut self, file: Arc<File>, committed_to: u64) {
        let write_placeholder = file
            .try_clone()
            .expect("clone read fd as write placeholder");
        self.segment = Some(OutputSegment {
            write_file: write_placeholder,
            write_offset: committed_to,
            committed: Arc::new(AtomicU64::new(committed_to)),
            read_file: file,
        });
        self.degraded = true;
    }
}

/// Positional read of the whole buffer at `offset` (Unix `pread`).
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

impl<I: Identifier> super::ClusterState<I> {
    /// Attach the always-on output store's disk segment (driver
    /// construction seam). Every role persists its outputs to node-local
    /// disk.
    pub(crate) fn attach_output_segment(
        &mut self,
        write_file: File,
        read_file: Arc<File>,
        committed: Arc<AtomicU64>,
    ) {
        self.output_store
            .attach_segment(write_file, read_file, committed);
    }

    /// Borrow the output store (driver / observability seam).
    pub(crate) fn output_store(&self) -> &OutputStore {
        &self.output_store
    }

    /// Read one task's outputs from the always-on disk store, if this
    /// node persisted them. The disk leg of the storage-agnostic
    /// [`Self::outputs_for_hash`] read.
    pub(super) fn output_store_read(&self, hash: &str) -> Option<TaskOutputs> {
        self.output_store.read_outputs(hash)
    }

    /// The write-through-then-DROP output write seam, invoked from
    /// `record_task_outputs_value` on a newly-completed task's outputs.
    /// On EVERY node it folds the digest term into the always-on
    /// accumulator (so every role converges the same `task_outputs_hash`).
    /// Then:
    ///
    /// * a node with a writable disk home PERSISTS the payload to disk and
    ///   indexes it — the payload NEVER enters the resident `task_outputs`
    ///   map (zero residence);
    /// * a node with NO disk home (a bare test state / a degraded writer)
    ///   RETAINS the payload resident as the correctness fallback (reads
    ///   still work; the digest's resident fold would double-count, so the
    ///   accumulator term is REVERSED for the resident leg — the resident
    ///   map owns the term in that case).
    ///
    /// First-write-wins: a hash already accounted (idempotent re-record)
    /// is a no-op, matching the resident map's `or_insert` CRDT
    /// discipline.
    ///
    /// Returns `true` if the payload was stored zero-residence (folded +
    /// persisted); `false` when the caller must retain the payload
    /// RESIDENT (the no-disk-home fallback) — the caller then `or_insert`s
    /// it into `task_outputs`.
    pub(super) fn output_store_record(&mut self, hash: &str, outputs: &TaskOutputs) -> bool {
        // Fold the always-on accumulator on every node (idempotent: a
        // re-record returns None and double-folds nothing).
        let Some(term) = self.output_store.fold_term(hash, outputs) else {
            // Already accounted (idempotent re-record). Drop the payload;
            // first-write already settled the slot.
            return true;
        };
        if !self.output_store.is_attached() {
            // No disk home declared (a bare fixture / pre-attach): retain
            // RESIDENT (the safe legacy behavior). Reverse the accumulator
            // fold so the resident `digest()` leg owns the term (counted
            // exactly once).
            self.output_store.outputs_hash_acc ^= term;
            self.output_store.outputs_count -= 1;
            return false;
        }
        if self.output_store.can_persist() {
            match self.output_store.persist(hash, outputs, term) {
                Ok(()) => return true,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "output-store write failed; payload persistence DISABLED \
                         for this process — retaining completed outputs in memory \
                         (fat-but-correct)"
                    );
                    self.output_store.degrade();
                    // Fall through to the resident-retention fallback.
                }
            }
        }
        // No (usable) disk home: the resident map must own both the
        // payload AND its digest term. Reverse the accumulator
        // fold (the resident `digest()` leg re-adds the identical
        // `hash_one((key, value))` term) so the term is counted exactly
        // once. The caller retains the payload resident.
        self.output_store.outputs_hash_acc ^= term;
        self.output_store.outputs_count -= 1;
        false
    }

    /// Test-only: attach an always-on output segment over a freshly-
    /// truncated file at `path`. Mirrors the production
    /// [`crate::settled_spill::SettledSpillDriver::start`] attach so a test
    /// drives the write-through-then-drop path end-to-end without the
    /// coordinator.
    #[cfg(test)]
    pub(crate) fn attach_output_segment_for_test(&mut self, path: &std::path::Path) {
        let write_fd = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .expect("test output file open");
        let read_fd = File::open(path).expect("test output read fd");
        let committed = Arc::new(AtomicU64::new(0));
        self.attach_output_segment(write_fd, Arc::new(read_fd), committed);
    }

    /// Test-only: register a read-only output segment over `file` whose
    /// published committed offset is `committed_to`. The decode-skip
    /// throttle seam: a test hand-places a mix of decodable + undecodable
    /// records and drives [`OutputStore::read_at_for_test`] over them.
    #[cfg(test)]
    pub(crate) fn attach_output_read_segment_for_test(
        &mut self,
        file: Arc<File>,
        committed_to: u64,
    ) {
        self.output_store
            .attach_read_segment_for_test(file, committed_to);
    }
}
