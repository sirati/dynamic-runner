//! Settled-entry disk spill: node-local storage backend for the JOIN
//! FIXED-POINT slice of the replicated task ledger.
//!
//! Single concern: WHERE a settled task's fat body lives (the
//! append-only spill file + the slim in-memory index) and the algebra
//! that keeps the `ClusterState` reads/merges/digest byte-identical to
//! the all-in-memory state. WHEN batches are written (the cadence, the
//! `spawn_blocking` write, the degrade policy) is the
//! [`crate::settled_spill`] driver's concern.
//!
//! # Settlement criterion (formal)
//!
//! An entry is SETTLED when it is a join fixed-point: merging any
//! REACHABLE input yields itself. Derived against the `TaskJoinKey`
//! lattice + the originator gates:
//!
//! * `Completed` — the strongest reachable terminal. `InvalidTask`
//!   out-ranks it within an attempt (D-T), but an `InvalidTask` is only
//!   originated at injection-validation time, before dispatch — a task
//!   that completed was dispatched, so no InvalidTask origination
//!   exists for its hash. A higher attempt is only minted by
//!   `TaskRetried`, whose F2-β gate is `Failed`-only.
//! * `SkippedAlreadyDone` — the WEAKEST terminal by rank, but a skip is
//!   never dispatched, so no real outcome is ever originated for it.
//! * `InvalidTask` — the unique TOP within an attempt; non-reinjectable,
//!   never retried (no bucket targets it).
//! * `Failed` with a kind NO retry bucket matches
//!   ([`BucketKind::matches`] is the ONE policy source): `Recoverable`
//!   (the retry bucket) and `ResourceExhausted("memory")` (the OOM
//!   bucket) stay FAT — a `TaskRetried` reset reaches them; every other
//!   kind is final.
//!
//! NOT settled: `Pending` / `InFlight` / `Blocked` (live), and
//! `Unfulfillable` (reinjectable via `TaskReinjected`).
//!
//! The criterion is an OPTIMIZATION, not a correctness assumption: the
//! settled consult in `merge_task_state` / the apply-arm probes compare
//! the stored join key and REHYDRATE the fat body from the file when a
//! lattice-allowed dominating input arrives anyway (the
//! fixed-point-violation escape hatch), so convergence is unconditional.
//!
//! # Record framing
//!
//! One record = `u32-LE length prefix` + ciborium CBOR of
//! [`SettledRecord`] `{ hash, state, outputs }` — one settled entry,
//! self-contained (the co-keyed `TaskOutputs` ride inside so a record
//! is the complete settled fact). Deliberately NOT byte-spliceable into
//! a `#481` snapshot-stream package: a package payload is ONE CBOR
//! `ClusterStateSnapshot` document whose `tasks` / `task_outputs` maps
//! are serde-encoded containers with their own headers — splicing
//! per-entry fragments would mean hand-rolled CBOR container assembly
//! outside serde (fragile, bypasses the one codec). The stream instead
//! DECODES a record from the file and re-encodes the package
//! (`ClusterState::settled_record`): the memory win holds (no resident
//! fat body — the decode is transient per package) and the wire format
//! is unchanged, so receivers and resume cursors are untouched.
//!
//! # Concurrency protocol (one writer, lock-free readers)
//!
//! Settled candidates are collected ON-LOOP as clones
//! ([`ClusterState::collect_spill_batch`] — the fat entry STAYS in the
//! map while the write is in flight, so every on-loop reader keeps
//! seeing it), written in one blocking batch
//! ([`write_spill_batch`], run inside `spawn_blocking` by the driver),
//! and only AFTER the flush is the committed offset published
//! (`Release` store) and the receipt applied on-loop
//! ([`ClusterState::commit_spill`] — evict + index). Readers hold their
//! own `Arc<File>` (positionless `pread`) and never read past the
//! committed offset, so no torn record is ever observable and no lock
//! exists anywhere on the path. An entry that advanced between collect
//! and commit fails the receipt's join-key check and is simply skipped
//! (its on-disk record is dead bytes; the advanced state re-settles
//! later if it reaches a new fixed-point).
//!
//! # Digest algebra
//!
//! Each committed entry's XOR contribution `hash_one((key,
//! hashable_join_key(state)))` moves from the live fold into the
//! settled accumulator — `digest()` folds `acc ⊕ fold(fat)`, which is
//! BYTE-IDENTICAL to the full fold by XOR associativity (settle and
//! unsettle are value-preserving moves; the differential test pins it).
//!
//! # Crash / restart
//!
//! The file is scratch: created with truncate on open, never reused by
//! a respawned process (replicated state rebuilds via the bootstrap
//! stream and the fresh process re-settles as entries arrive).

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dynrunner_core::{ErrorType, Identifier, PhaseId, TaskDep, TaskOutputs};
use serde::{Deserialize, Serialize};

use crate::primary::retry_bucket::BucketKind;

use super::merge::{hashable_join_key, task_join_key, task_join_key_dominates};
use super::types::TaskJoinKey;
use super::{ClusterState, TaskDefId, TaskState};

/// Hash one hashable value — the same default-hasher fold `digest.rs` /
/// `merge.rs` use (process-stable; cross-build stability not required).
fn hash_one<H: std::hash::Hash>(value: H) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Compact terminal classification of a settled entry — exactly the
/// projection every fat-body-free reader needs (outcome buckets, dep
/// cascade classification, hydrate seeds). `FailedFinal` carries the
/// `ErrorType` verbatim so the hydrate-time `failed_tasks` ledger and
/// the `fail_oom`-vs-`fail_final` bucket split stay faithful.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SettledClass {
    Completed,
    SkippedAlreadyDone,
    SetupCompleted,
    /// A SecondaryAffine gate that became dependency-satisfied (the
    /// READY-not-EXECUTED terminal). Like `SetupCompleted` it RESOLVES a
    /// dependent's `TaskDep` (a build gated on the gate unblocks) but, like
    /// the skip, it is an INERT terminal counted in neither the success nor
    /// any failure bucket — its own `affine_ready` slot.
    AffineReady,
    InvalidTask,
    FailedFinal(ErrorType),
}

/// Slim in-memory index entry for one settled task: identity +
/// classification + join key + file location. Retained fields, each
/// justified by a production reader:
///
/// * `task_id` / `phase_id` — dep resolution (`task_hash_for_dep`),
///   hydrate seeds, per-phase rollups/partitions, observer stats.
/// * `task_depends_on` — the dispatch-time `inherit_outputs` ancestor
///   walk (`predecessor_outputs`) reads a COMPLETED predecessor's dep
///   edges; typically empty or a few edges.
/// * `class` — outcome buckets, the CRDT-terminal settle path's
///   classification, hydrate's per-class seeds.
/// * `join_key` — the merge dominance consult + the broadcast
///   choke-point's attempt stamp.
/// * `digest_contribution` — the entry's XOR term, stored so unsettle
///   subtracts exactly what commit added (XOR is self-inverse).
/// * `segment` / `offset` / `len` — the record's location for the
///   stream-from-file responder and the rehydrate escape hatch.
///
/// The co-keyed output VALUES dep-resolution / keyed-output /
/// skip-existing lookups need are EVICTED from the resident
/// `task_outputs` map at commit-spill (their authoritative copy rides
/// the spill record): an inline `ResultValue` is up to 16 MiB, so at
/// scale the keyed outputs — not just the per-task `TaskInfo` body —
/// are a dominant resident cost that must not accumulate O(completed
/// tasks). Their digest term moves into `task_outputs_hash_acc` (the
/// output twin of `tasks_hash_acc`), and every reader resolves them
/// through [`super::ClusterState::outputs_for_hash`] (resident map →
/// settled-disk fallback), so callers never know where the payload
/// physically lives.
#[derive(Debug, Clone)]
pub(crate) struct SettledEntry {
    pub(crate) task_id: String,
    pub(crate) phase_id: PhaseId,
    pub(crate) task_depends_on: Vec<TaskDep>,
    pub(crate) class: SettledClass,
    pub(super) join_key: TaskJoinKey,
    /// The self-describing [`TaskDefId`] this settled record's def carries,
    /// captured at commit-spill from `state.def().def_id` (the def is in
    /// hand there — no disk read). Surfaced in-memory so a promoted
    /// primary's failover def-id resume floor includes the ids of tasks
    /// that have already SETTLED (their defs left `definitions` only by
    /// content-Arc, but the snapshot ships them by value separately, so the
    /// in-memory store's `next_id` alone does NOT cover a settled id once
    /// the base is installed over a fresh store). A wire-agreed id (the
    /// production path) is a real slot; a node-local intern carries
    /// [`TaskDefId::UNBOUND`] and is excluded from the resume max (a
    /// node-local id is intra-node only — it never aliases across the
    /// failover seam). See [`SettledStore::max_def_id`].
    def_id: TaskDefId,
    digest_contribution: u64,
    /// The `(key, TaskOutputs)` digest term this entry's EVICTED output
    /// payload contributed to `task_outputs_hash_acc` at commit-spill,
    /// or `None` when the entry had no resident output payload (it
    /// published nothing, so it never sat in `task_outputs` and
    /// contributes to neither the count nor the fold). Stored so unsettle
    /// subtracts exactly what commit added (XOR is self-inverse) and
    /// decrements the settled-outputs count by exactly one when present.
    outputs_digest_contribution: Option<u64>,
    pub(crate) segment: u32,
    pub(crate) offset: u64,
    pub(crate) len: u32,
}

impl SettledEntry {
    /// The retry-attempt generation the settled state carried — read by
    /// the broadcast choke point's attempt stamp.
    pub(crate) fn attempt(&self) -> u32 {
        self.join_key.attempt
    }

    /// Rough resident size of this index entry (the memory-pin seam —
    /// a counting estimate, not an allocator measurement).
    fn approx_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.task_id.capacity()
            + self.phase_id.as_str().len()
            + self
                .task_depends_on
                .iter()
                .map(|d| {
                    std::mem::size_of::<TaskDep>()
                        + d.task_id.capacity()
                        + d.phase_id.as_str().len()
                })
                .sum::<usize>()
            + match &self.class {
                SettledClass::FailedFinal(k) => std::mem::size_of_val(k),
                _ => 0,
            }
    }
}

/// One spill-file segment: a read fd (`Arc<File>` — positionless
/// `pread`, shareable across clones / the promoted-primary handover)
/// plus the published committed offset readers must not read past.
#[derive(Clone)]
pub(crate) struct SettledSegment {
    file: Arc<File>,
    committed: Arc<AtomicU64>,
}

/// The settled store: slim index + segment table + the settled half of
/// the digest's tasks fold. Owned by `ClusterState` as a NODE-LOCAL
/// storage backend for replicated data: the index/accumulator are pure
/// derivations of replicated entries (like the digest memo), the file
/// is node-local scratch.
///
/// `pub` (not `pub(crate)`) because it crosses the promotion seam as a
/// [`crate::process::PromotionSignal`] field / builder parameter — the
/// pyo3 recipe threads it into the promoted primary opaquely
/// (`adopt_settled_base`); every method stays `pub(crate)`.
#[derive(Default)]
pub struct SettledStore {
    index: HashMap<String, SettledEntry>,
    segments: Vec<SettledSegment>,
    /// XOR accumulator over settled `(key, hashable_join_key)` terms —
    /// `digest()` folds `acc ⊕ fold(fat)`.
    tasks_hash_acc: u64,
    /// XOR accumulator over settled `(key, TaskOutputs)` terms — the
    /// output twin of `tasks_hash_acc`. A settled task's output payload
    /// leaves the resident `task_outputs` map (it lives in the spill
    /// record), so its `task_outputs_hash` term moves here at
    /// commit-spill and `digest()` folds `acc ⊕ fold(resident)` exactly
    /// as it does for the task fold. Only entries that actually had a
    /// resident output payload contribute (a settled task that published
    /// nothing adds nothing — the resident map held no entry, so neither
    /// the count nor the fold moves).
    task_outputs_hash_acc: u64,
    /// Count of settled entries whose evicted output payload is folded
    /// into `task_outputs_hash_acc` — the settled half of
    /// `task_outputs_count`. Equal to the number of settled keys that
    /// had a resident `task_outputs` entry at commit time.
    settled_outputs_count: u64,
    /// The segment THIS instance's writer appends to; `None` on a
    /// read-only adopted base (a clone, or before a writer attaches).
    own_segment: Option<u32>,
    /// Total records committed through this store (observability).
    records_committed: u64,
    /// Running estimate of resident index bytes (the memory-pin seam).
    approx_index_bytes: usize,
}

/// `Clone` IS the read-only clone ([`SettledStore::clone_read_only`]):
/// the index + shared read fds carry over; the writer affiliation is
/// dropped (one-writer rule). Required so the `PromotionSignal` that
/// carries a settled base stays `Clone` for test fixtures.
impl Clone for SettledStore {
    fn clone(&self) -> Self {
        self.clone_read_only()
    }
}

impl std::fmt::Debug for SettledStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SettledStore")
            .field("index", &self.index.len())
            .field("segments", &self.segments.len())
            .field("own_segment", &self.own_segment)
            .field("records_committed", &self.records_committed)
            .finish()
    }
}

impl SettledStore {
    /// An EMPTY settled base — no index entries, no segments. The
    /// merge-neutral handover value: a `PromotedPrimaryBuilder` (or a
    /// test fixture) with no settled slice to inherit installs this, and
    /// the subsequent snapshot restore seeds a fully-fat ledger. `pub`
    /// for the same reason as the type: it crosses the promotion seam.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Read-only clone of this store: the index + accumulator are
    /// replicated-data derivations (a cloned replica keeps serving
    /// settled reads through the shared `Arc<File>` segments), the
    /// WRITER affiliation is node-local runtime state and is dropped
    /// (one-writer rule: the clone never appends to the source's file;
    /// its own driver may attach a fresh segment).
    pub(crate) fn clone_read_only(&self) -> Self {
        Self {
            index: self.index.clone(),
            segments: self.segments.clone(),
            tasks_hash_acc: self.tasks_hash_acc,
            task_outputs_hash_acc: self.task_outputs_hash_acc,
            settled_outputs_count: self.settled_outputs_count,
            own_segment: None,
            records_committed: self.records_committed,
            approx_index_bytes: self.approx_index_bytes,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.index.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub(crate) fn contains(&self, hash: &str) -> bool {
        self.index.contains_key(hash)
    }

    pub(crate) fn get(&self, hash: &str) -> Option<&SettledEntry> {
        self.index.get(hash)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&String, &SettledEntry)> {
        self.index.iter()
    }

    pub(super) fn tasks_hash_acc(&self) -> u64 {
        self.tasks_hash_acc
    }

    /// The settled half of the `task_outputs_hash` fold — `digest()`
    /// seeds the resident fold with this (XOR associativity makes
    /// `acc ⊕ fold(resident)` equal the full logical fold), the output
    /// twin of [`Self::tasks_hash_acc`].
    pub(super) fn task_outputs_hash_acc(&self) -> u64 {
        self.task_outputs_hash_acc
    }

    /// Count of settled entries whose evicted output payload the digest
    /// must add to `task_outputs_count` (the settled half of the output
    /// cache count).
    pub(super) fn settled_outputs_count(&self) -> u64 {
        self.settled_outputs_count
    }

    /// The maximum WIRE-AGREED [`TaskDefId`] held by any settled record,
    /// or `None` when no settled record carries one. The settled half of a
    /// promoted primary's failover def-id resume floor: a settled record's
    /// def left the in-memory `definitions` store (the snapshot ships defs
    /// by value separately from the settled base, so a fresh store seeded
    /// by `install_settled_base` + snapshot-restore does NOT re-anchor a
    /// settled id), yet a promoted primary must still resume its allocator
    /// PAST it so a new task never re-mints a settled task's id (the
    /// failover-aliasing CL-A2 forbids — the L5 def-id dep-ref prerequisite).
    ///
    /// [`TaskDefId::UNBOUND`] entries (node-local interns — intra-node only,
    /// legitimately divergent across replicas) are EXCLUDED: a node-local id
    /// never aliases across the failover seam, and folding the sentinel
    /// `u32::MAX` would wrongly slam the allocator to the top of the space.
    pub(crate) fn max_def_id(&self) -> Option<u32> {
        self.index
            .values()
            .map(|e| e.def_id)
            .filter(|&id| id != TaskDefId::UNBOUND)
            .map(|id| id.0)
            .max()
    }

    /// Per-settled-entry `(key, digest_contribution)` pairs — the persisted
    /// per-entry fold term each settled entry contributes to `tasks_hash`
    /// (`digest_contribution` was stamped at spill-commit as the SAME
    /// `hash_one((key, hashable_join_key))` the live fold uses). The P1
    /// range-digest projection buckets these by `range_index(key)` so the
    /// settled half of the ledger folds into the right ranges, keeping the
    /// `XOR(range-folds) == tasks_hash` invariant whole across the
    /// fat/settled split. Keeps `digest_contribution` private to this
    /// module (the store owns the term's lifecycle); callers see only the
    /// `(key, term)` pair, never the entry internals.
    ///
    /// Test-only now (#492): the production [`super::ClusterState::tasks_range_digest`]
    /// reads the incrementally-maintained `RangeFoldMemo` instead of folding
    /// the settled half on every probe; this one-pass settled fold survives as
    /// the `#[cfg(test)] fresh_tasks_range_digest` the differential memo
    /// invariant recomputes against. The settled-entry term is XOR-maintained
    /// into the memo at the same logical-create/change sites the fat entries
    /// are (a spill is memo-neutral), so the memo stays whole across the split.
    #[cfg(test)]
    pub(super) fn digest_contributions(&self) -> impl Iterator<Item = (&str, u64)> {
        self.index
            .iter()
            .map(|(key, entry)| (key.as_str(), entry.digest_contribution))
    }

    pub(crate) fn records_committed(&self) -> u64 {
        self.records_committed
    }

    pub(crate) fn approx_index_bytes(&self) -> usize {
        self.approx_index_bytes
    }

    /// Whether a writer segment is attached (the sweep collects only
    /// when there is somewhere durable to put the batch).
    pub(crate) fn has_writer(&self) -> bool {
        self.own_segment.is_some()
    }

    /// The writer segment's id, if one is attached (the driver stamps
    /// it onto receipts).
    pub(crate) fn writer_segment_id(&self) -> Option<u32> {
        self.own_segment
    }

    /// The writer segment's committed-offset cell, if one is attached
    /// (the driver re-arms its writer half around it after each batch).
    pub(crate) fn writer_committed_cell(&self) -> Option<Arc<AtomicU64>> {
        self.own_segment
            .map(|id| Arc::clone(&self.segments[id as usize].committed))
    }

    /// Total committed bytes across every segment backing this store
    /// (observability: the spill-file footprint settled reads draw on).
    pub(crate) fn committed_bytes(&self) -> u64 {
        self.segments
            .iter()
            .map(|s| s.committed.load(Ordering::Acquire))
            .sum()
    }

    /// Attach a segment (read fd + committed-offset cell) and make it
    /// this store's writer target. Returns the segment id the driver
    /// stamps onto its receipts.
    pub(crate) fn attach_writer_segment(
        &mut self,
        file: Arc<File>,
        committed: Arc<AtomicU64>,
    ) -> u32 {
        let id = self.segments.len() as u32;
        self.segments.push(SettledSegment { file, committed });
        self.own_segment = Some(id);
        id
    }

    /// Read + decode one settled record. `None` on any failure (an
    /// out-of-committed-range read, an IO error, a decode error) — the
    /// caller treats the entry as unreadable and degrades loudly at its
    /// own seam. Never reads past the segment's committed offset.
    fn read_record<I: Identifier>(&self, entry: &SettledEntry) -> Option<SettledRecord<I>> {
        let segment = self.segments.get(entry.segment as usize)?;
        let committed = segment.committed.load(Ordering::Acquire);
        let end = entry.offset.checked_add(u64::from(entry.len))?;
        if end > committed {
            // Index entries are only minted at commit (post-flush), so
            // this is structurally unreachable for a coherent store;
            // refuse rather than risk a torn read.
            tracing::error!(
                offset = entry.offset,
                len = entry.len,
                committed,
                "settled-spill read past committed offset refused (index/commit incoherence)"
            );
            return None;
        }
        let mut buf = vec![0u8; entry.len as usize];
        if let Err(e) = read_exact_at(&segment.file, &mut buf, entry.offset) {
            tracing::error!(
                error = %e,
                offset = entry.offset,
                len = entry.len,
                "settled-spill record read failed"
            );
            return None;
        }
        // Strip the length prefix; the body is the CBOR record.
        let body = buf.get(RECORD_PREFIX_LEN..)?;
        match ciborium::from_reader(body) {
            Ok(rec) => Some(rec),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    offset = entry.offset,
                    "settled-spill record decode failed"
                );
                None
            }
        }
    }
}

/// Positional read of the whole buffer at `offset` (Unix `pread` —
/// `&File` is enough; no seek, so concurrent readers never interfere).
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

/// Bytes of the `u32`-LE length prefix in front of every record body.
const RECORD_PREFIX_LEN: usize = 4;

/// The on-disk record: one settled entry, self-contained (state + the
/// co-keyed outputs). Serialized as CBOR behind a length prefix.
#[derive(Serialize, Deserialize)]
#[serde(bound(serialize = "I: Serialize", deserialize = "I: for<'a> Deserialize<'a>",))]
pub(crate) struct SettledRecord<I> {
    pub(crate) hash: String,
    pub(crate) state: TaskState<I>,
    pub(crate) outputs: Option<TaskOutputs>,
}

/// One collected spill candidate: a CLONE of the fat entry (the map
/// keeps the original until commit) plus the join key the commit-time
/// staleness check compares.
pub(crate) struct SpillCandidate<I> {
    pub(crate) hash: String,
    pub(crate) state: TaskState<I>,
    pub(crate) outputs: Option<TaskOutputs>,
    // The join key the commit-time staleness check compares against —
    // `pub(super)` so the `TaskJoinKey` (cluster_state-internal) never
    // leaks past the module; the driver only moves candidates through.
    pub(super) join_key: TaskJoinKey,
}

/// The receipt a completed blocking write hands back on-loop: where
/// each record landed, stamped with the join key it was collected at.
pub(crate) struct SpillReceipt {
    pub(crate) segment: u32,
    pub(crate) written: Vec<WrittenRecord>,
}

pub(crate) struct WrittenRecord {
    pub(crate) hash: String,
    // `pub(super)` — same `TaskJoinKey`-containment rationale as
    // `SpillCandidate::join_key`.
    pub(super) join_key: TaskJoinKey,
    pub(crate) offset: u64,
    pub(crate) len: u32,
}

/// Append `batch` to `file` (the writer's OWN fd, positioned at
/// `start_offset`), flush, then publish the new committed offset
/// (`Release` — pairs with readers' `Acquire`). Pure blocking IO +
/// encode; the driver runs it inside `spawn_blocking`. On error the
/// committed offset is NOT advanced (whatever partial bytes landed
/// past it are invisible to every reader) and the caller degrades.
pub(crate) fn write_spill_batch<I: Identifier>(
    file: &mut File,
    start_offset: u64,
    committed: &AtomicU64,
    segment: u32,
    batch: Vec<SpillCandidate<I>>,
) -> std::io::Result<(SpillReceipt, u64)> {
    let mut written = Vec::with_capacity(batch.len());
    let mut buf: Vec<u8> = Vec::new();
    let mut offset = start_offset;
    for cand in batch {
        buf.clear();
        // Reserve the length prefix, encode the body behind it, then
        // back-fill the prefix.
        buf.extend_from_slice(&[0u8; RECORD_PREFIX_LEN]);
        let record = SettledRecord {
            hash: cand.hash.clone(),
            state: cand.state,
            outputs: cand.outputs,
        };
        ciborium::into_writer(&record, &mut buf)
            .map_err(|e| std::io::Error::other(format!("settled record encode: {e}")))?;
        let body_len = (buf.len() - RECORD_PREFIX_LEN) as u32;
        buf[..RECORD_PREFIX_LEN].copy_from_slice(&body_len.to_le_bytes());
        file.write_all(&buf)?;
        let len = buf.len() as u32;
        written.push(WrittenRecord {
            hash: cand.hash,
            join_key: cand.join_key,
            offset,
            len,
        });
        offset += u64::from(len);
    }
    file.flush()?;
    // Durable in the page cache (a spill, not a durability file — no
    // fsync); publish so readers may now reach these records.
    committed.store(offset, Ordering::Release);
    Ok((SpillReceipt { segment, written }, offset))
}

/// Is `state` a join fixed-point (see the module doc)? The `Failed`
/// arm derives finality from [`BucketKind::matches`] — the ONE retry
/// policy source — so the settle predicate can never drift from what
/// the retry buckets actually target.
pub(crate) fn settle_eligible<I>(state: &TaskState<I>) -> bool {
    match state {
        // `SetupCompleted` is a join fixed-point: a setup task's terminal
        // is originated once by its in-process executor and is never
        // retried or re-failed (no retry bucket targets it, no reset
        // resurrects it), so it is settle-eligible exactly like the other
        // success-like terminals.
        TaskState::Completed { .. }
        | TaskState::SkippedAlreadyDone { .. }
        | TaskState::SetupCompleted { .. }
        // `AffineReady` is a join fixed-point: a SecondaryAffine gate's
        // terminal is originated once by the primary's ready-resolution hook
        // and is never retried or re-failed (no retry bucket targets it, no
        // reset resurrects it), so it is settle-eligible exactly like the
        // other success-like terminals.
        | TaskState::AffineReady { .. }
        | TaskState::InvalidTask { .. } => true,
        TaskState::Failed { kind, .. } => {
            !BucketKind::Recoverable.matches(kind) && !BucketKind::Oom.matches(kind)
        }
        // `QueuedAfterLocalDependency` is non-terminal (an active assignment
        // awaiting its secondary's local import), so it is NOT settle-eligible
        // exactly like `InFlight`.
        TaskState::Pending { .. }
        | TaskState::InFlight { .. }
        | TaskState::QueuedAfterLocalDependency { .. }
        | TaskState::Blocked { .. }
        | TaskState::Unfulfillable { .. } => false,
    }
}

/// Project a settle-eligible state onto its [`SettledClass`]. `None`
/// for a non-eligible state (callers gate on [`settle_eligible`]).
fn settled_class_of<I>(state: &TaskState<I>) -> Option<SettledClass> {
    match state {
        TaskState::Completed { .. } => Some(SettledClass::Completed),
        TaskState::SkippedAlreadyDone { .. } => Some(SettledClass::SkippedAlreadyDone),
        TaskState::SetupCompleted { .. } => Some(SettledClass::SetupCompleted),
        TaskState::AffineReady { .. } => Some(SettledClass::AffineReady),
        TaskState::InvalidTask { .. } => Some(SettledClass::InvalidTask),
        TaskState::Failed { kind, .. } if settle_eligible(state) => {
            Some(SettledClass::FailedFinal(kind.clone()))
        }
        _ => None,
    }
}

impl<I: Identifier> ClusterState<I> {
    /// Whether `hash` is in the settled index (fat body on disk).
    pub(crate) fn settled_contains(&self, hash: &str) -> bool {
        self.settled.contains(hash)
    }

    /// Borrow `hash`'s settled index entry, if settled.
    pub(crate) fn settled_entry(&self, hash: &str) -> Option<&SettledEntry> {
        self.settled.get(hash)
    }

    /// Iterate the settled index: `(hash, slim entry)` for every entry
    /// whose fat body lives on disk. The union of this and the fat
    /// iterators is the full logical ledger.
    pub(crate) fn settled_entries(&self) -> impl Iterator<Item = (&String, &SettledEntry)> {
        self.settled.iter()
    }

    /// Borrow the settled store (driver / observability seam).
    pub(crate) fn settled_store(&self) -> &SettledStore {
        &self.settled
    }

    /// Attach the spill writer's segment (read fd + committed offset)
    /// and return its id. Called once by the spill driver at enable
    /// time; until then the store is empty/read-only and nothing
    /// settles.
    pub(crate) fn attach_spill_segment(&mut self, file: Arc<File>, committed: Arc<AtomicU64>) -> u32 {
        self.settled.attach_writer_segment(file, committed)
    }

    /// Adopt a read-only settled base (promotion handover / the Clone
    /// path): the donor's index + segments + accumulator become this
    /// replica's settled view. Only legal while this state holds NO
    /// settled entries of its own (the promoted primary installs the
    /// base BEFORE restoring the donor snapshot); a non-empty local
    /// store is a caller bug and panics in debug builds.
    pub(crate) fn install_settled_base(&mut self, base: SettledStore) {
        debug_assert!(
            self.settled.is_empty(),
            "install_settled_base on a state that already settled entries"
        );
        // Keep an already-attached writer segment functional: re-attach
        // it after the base's segments. (Today the promoted-primary
        // path installs the base before any writer attaches, so this is
        // a defensive re-append, exercised by unit tests only.)
        let own = self.settled.own_segment.map(|id| {
            let seg = self.settled.segments[id as usize].clone();
            (seg.file, seg.committed)
        });
        self.settled = base;
        self.settled.own_segment = None;
        if let Some((file, committed)) = own {
            self.settled.attach_writer_segment(file, committed);
        }
    }

    /// Clone this state's settled store read-only (the promotion
    /// handover capture — see [`Self::install_settled_base`]).
    pub(crate) fn settled_base_clone(&self) -> SettledStore {
        self.settled.clone_read_only()
    }

    /// Collect up to `max_entries` settle-eligible fat entries as spill
    /// candidates (CLONES — the fat map is untouched until
    /// [`Self::commit_spill`]). Returns an empty batch when no writer
    /// segment is attached. The scan is over the FAT map only (already-
    /// settled entries are not in it), self-healing by construction:
    /// anything missed this sweep is collected on the next.
    pub(crate) fn collect_spill_batch(&self, max_entries: usize) -> Vec<SpillCandidate<I>> {
        if !self.settled.has_writer() {
            return Vec::new();
        }
        let mut out = Vec::new();
        for (hash, state) in &self.tasks {
            if out.len() >= max_entries {
                break;
            }
            if !settle_eligible(state) {
                continue;
            }
            out.push(SpillCandidate {
                hash: hash.clone(),
                state: state.clone(),
                outputs: self.task_outputs.get(hash).cloned(),
                join_key: task_join_key(state),
            });
        }
        out
    }

    /// Apply a durable write receipt: for every record whose fat entry
    /// STILL ranks at the collected join key, evict the fat body and
    /// insert the slim index entry; an entry that advanced mid-write is
    /// skipped (its record is dead bytes — it re-settles later if it
    /// reaches a new fixed-point). Returns the number of entries
    /// evicted.
    ///
    /// Digest-value-preserving: each eviction XORs the entry's term
    /// into the settled accumulator at the same instant it leaves the
    /// live fold, so `digest()` is unchanged — the memo is deliberately
    /// NOT invalidated.
    pub(crate) fn commit_spill(&mut self, receipt: SpillReceipt) -> usize {
        let mut evicted = 0usize;
        for rec in receipt.written {
            let Some(state) = self.tasks.get(&rec.hash) else {
                continue;
            };
            if task_join_key(state) != rec.join_key {
                // Advanced between collect and commit (the lattice-
                // allowed escape) — keep it fat.
                continue;
            }
            let Some(class) = settled_class_of(state) else {
                continue;
            };
            let def = state.def();
            // Capture every value that borrows `state`/`def` (the `self.tasks`
            // borrow) UP FRONT, so the whole-`self` `resolve_dep_refs` call
            // below does not overlap that borrow. The dep REFS are cheap
            // `Copy` clones; resolving them to string deps needs the def store
            // (`&self`), done AFTER the `state` borrow ends.
            let task_id = def.task_id.clone();
            let phase_id = def.phase_id.clone();
            let captured_def_id = def.def_id;
            let dep_refs = def.task_depends_on.clone();
            let digest_contribution = hash_one((&rec.hash, hashable_join_key(state)));
            // Evict the resident output payload: its co-keyed copy already
            // rode the spill record (`collect_spill_batch` cloned it into
            // the `SettledRecord`), so the disk copy is authoritative and
            // the resident one is pure accumulation. Its `task_outputs_hash`
            // term moves into the settled accumulator at the SAME instant it
            // leaves the resident fold (XOR associativity keeps `digest()`
            // byte-identical — the output twin of the `tasks_hash_acc`
            // move). A task that published nothing held no resident entry,
            // so it contributes nothing to either the count or the fold.
            let outputs_digest_contribution = self
                .task_outputs
                .remove(&rec.hash)
                .map(|outputs| hash_one((&rec.hash, &outputs)));
            // L5: resolve the fat def's compact `TaskDepRef`s → string deps
            // at commit-spill (the def store is in hand here), so the slim
            // settled index keeps the resolved identity its `task_deps_for_identity`
            // reader hands the transitive-ancestor walk — no def-store
            // round-trip on the (common) spilled-predecessor read path.
            let task_depends_on = self.resolve_dep_refs(&dep_refs);
            let entry = SettledEntry {
                task_id,
                phase_id,
                task_depends_on,
                class,
                join_key: rec.join_key,
                // Self-describing def-id captured here (the def is in hand —
                // no disk read) so the failover resume floor can include
                // settled ids without re-reading every spill record.
                def_id: captured_def_id,
                digest_contribution,
                outputs_digest_contribution,
                segment: receipt.segment,
                offset: rec.offset,
                len: rec.len,
            };
            self.settled.tasks_hash_acc ^= entry.digest_contribution;
            if let Some(term) = entry.outputs_digest_contribution {
                self.settled.task_outputs_hash_acc ^= term;
                self.settled.settled_outputs_count += 1;
            }
            self.settled.approx_index_bytes += entry.approx_bytes();
            self.settled.records_committed += 1;
            self.settled.index.insert(rec.hash.clone(), entry);
            self.tasks.remove(&rec.hash);
            // Range-fold memo: a spill is memo-NEUTRAL. The entry stays a
            // LOGICAL ledger entry (its term moves from the fat `tasks` half
            // into the settled half the range fold sums over, identical
            // value), so its bucket fold + count are unchanged — exactly as
            // this spill leaves `tasks_hash` unchanged (the term is XORed out
            // of the live fold and into `tasks_hash_acc` above). No memo touch.
            evicted += 1;
        }
        evicted
    }

    /// The fixed-point-violation escape hatch: if `incoming_key`
    /// strictly dominates `hash`'s settled join key, rehydrate the fat
    /// body from the spill file back into `tasks` (removing the index
    /// entry and subtracting its digest term) and return `true` — the
    /// caller then proceeds exactly as if the entry had always been
    /// fat. `false` when the settled entry dominates (the common late
    /// duplicate → NoOp) or the record is unreadable (logged loudly;
    /// refusing keeps the settled fact — other replicas still hold the
    /// data, anti-entropy converges them).
    pub(super) fn unsettle_if_dominated(&mut self, hash: &str, incoming_key: &TaskJoinKey) -> bool {
        let Some(entry) = self.settled.get(hash) else {
            return false;
        };
        if !task_join_key_dominates(incoming_key, &entry.join_key) {
            return false;
        }
        let Some(record) = self.settled.read_record::<I>(entry) else {
            tracing::error!(
                hash,
                "settled entry dominated by an incoming mutation but its spill \
                 record is unreadable; keeping the settled fact (convergence \
                 for this hash defers to anti-entropy against other replicas)"
            );
            return false;
        };
        tracing::debug!(
            hash,
            "settled entry rehydrated: an incoming mutation dominates its \
             join fixed-point (lattice escape hatch)"
        );
        let entry = self
            .settled
            .index
            .remove(hash)
            .expect("checked present above");
        self.settled.tasks_hash_acc ^= entry.digest_contribution;
        // Reverse the output eviction: the payload left the resident map at
        // commit-spill (its term moved into `task_outputs_hash_acc`), so
        // un-settling must XOR that term back out, drop the settled-outputs
        // count, and REHYDRATE the payload from the spill record into the
        // resident map — otherwise the outputs are silently dropped. The
        // record's embedded `outputs` is the authoritative copy
        // (`collect_spill_batch` wrote it; equal-by-construction to what was
        // evicted, so the XOR-out is the exact inverse of the commit XOR-in).
        // First-write-wins (`or_insert`) matches the resident map's CRDT
        // discipline: a concurrent live broadcast may have already
        // re-populated the slot.
        if let Some(term) = entry.outputs_digest_contribution {
            self.settled.task_outputs_hash_acc ^= term;
            self.settled.settled_outputs_count =
                self.settled.settled_outputs_count.saturating_sub(1);
            if let Some(outputs) = record.outputs.clone() {
                self.task_outputs.entry(hash.to_string()).or_insert(outputs);
            }
        }
        self.settled.approx_index_bytes = self
            .settled
            .approx_index_bytes
            .saturating_sub(entry.approx_bytes());
        // Range-fold memo: an unsettle is memo-NEUTRAL — the inverse of a
        // spill. The entry was already a LOGICAL ledger entry counted in the
        // memo (as the settled term, which equals the rehydrated fat term);
        // moving it from the settled half back to fat changes neither its
        // bucket fold nor its count, exactly as it leaves `tasks_hash`
        // unchanged (the term is XORed out of `tasks_hash_acc` above). No memo
        // touch. A subsequent dominating merge then swaps to the winning term.
        debug_assert!(!matches!(record.state, TaskState::Blocked { .. }), "settle_eligible() should exclude Blocked — if this fires, the reverse-index at the set_task_state seam needs to be updated for this unsettle path");
        self.tasks.insert(hash.to_string(), record.state);
        true
    }

    /// Storage-agnostic keyed-output read: a completed task's
    /// [`TaskOutputs`] for `hash`, WHEREVER its payload lives — the
    /// resident `task_outputs` map (a fat / not-yet-spilled entry) takes
    /// precedence, falling back to the settled spill record's embedded
    /// copy (a SETTLED entry whose payload was evicted at commit-spill).
    /// `None` when the task published no outputs, or when a settled
    /// record is unreadable (logged at the read seam; the caller treats
    /// it as no-output, exactly as a vanished key). This is the ONE seam
    /// every output reader routes through, so no caller knows whether a
    /// completed task's outputs are in RAM or on disk.
    ///
    /// Owned clone (the resident borrow could not outlive the transient
    /// disk decode, and callers — dispatch assembler, `on_phase_end`,
    /// the stream builder — all need an owned value across an apply or a
    /// callback boundary anyway).
    pub(crate) fn outputs_for_hash(&self, hash: &str) -> Option<TaskOutputs> {
        if let Some(outputs) = self.task_outputs.get(hash) {
            return Some(outputs.clone());
        }
        // Not resident. Steady state: the payload was write-through-then-
        // dropped to the ALWAYS-ON output store at completion (the
        // zero-residence home a reader persists). Fall back to the legacy
        // settled spill record (the no-disk-home path that went resident
        // then spilled at settle), then to unknown / output-less.
        if let Some(outputs) = self.output_store_read(hash) {
            return Some(outputs);
        }
        self.settled_record(hash).and_then(|(_, outputs)| outputs)
    }

    /// Read + decode `hash`'s settled record from the spill file — the
    /// stream-from-file responder's per-entry read. `None` when the
    /// hash is not settled or the record is unreadable (the package
    /// build then skips it exactly like a vanished key; the receiver
    /// converges through anti-entropy).
    pub(crate) fn settled_record(&self, hash: &str) -> Option<(TaskState<I>, Option<TaskOutputs>)> {
        let entry = self.settled.get(hash)?;
        let record = self.settled.read_record::<I>(entry)?;
        Some((record.state, record.outputs))
    }

    /// Apply-arm lookup that consults the settled index: a fat entry is
    /// returned as-is; a settled entry whose stored key `incoming_key`
    /// strictly dominates is rehydrated first; a dominating settled
    /// entry (the common late duplicate) returns `None` — exactly the
    /// NoOp the arm's absent-slot path takes.
    pub(super) fn task_entry_unsettling(
        &mut self,
        hash: &str,
        incoming_key: &TaskJoinKey,
    ) -> Option<&TaskState<I>> {
        if !self.tasks.contains_key(hash) {
            self.unsettle_if_dominated(hash, incoming_key);
        }
        self.tasks.get(hash)
    }

    /// Test-only: the number of output payloads RESIDENT in the
    /// `task_outputs` map (the memory-pin seam for the zero-accumulation
    /// claim — a settled task's payload must have left it for the spill
    /// file). Distinct from the LOGICAL output count (resident ∪ settled),
    /// which the digest's `task_outputs_count` reports.
    #[cfg(test)]
    pub(crate) fn task_outputs_resident_len(&self) -> usize {
        self.task_outputs.len()
    }

    /// Test-only: drop any attached spill writer by replacing the settled
    /// store with a fresh EMPTY (writer-less) one. Only legal while NO entry
    /// has settled (asserted) — its sole use is to clear the writer a
    /// coordinator's production spill driver attached at construction (to a
    /// process-shared, role-named file) so a test can re-attach its OWN
    /// per-test writer over a unique path via [`Self::test_spill_all`],
    /// isolated from every other coordinator the parallel test run builds.
    #[cfg(test)]
    pub(crate) fn detach_spill_writer_for_test(&mut self) {
        debug_assert!(
            self.settled.is_empty(),
            "detach_spill_writer_for_test before any entry has settled"
        );
        self.settled = SettledStore::empty();
    }

    /// Test-only: attach a writer segment over a freshly-truncated spill
    /// file at `path` (the same shape the production driver opens), then
    /// synchronously run ONE full collect → blocking write+flush →
    /// commit sweep. Returns the count of entries evicted. The committed
    /// offset is a per-call cell so a reader opened against `path` sees
    /// exactly the flushed bytes. Drives the protocol end-to-end without
    /// the tokio driver.
    #[cfg(test)]
    pub(crate) fn test_spill_all(&mut self, path: &std::path::Path) -> usize {
        if !self.settled.has_writer() {
            // Truncate-create the file (drop the write handle immediately —
            // the sweep below reopens its own append fd), then attach a
            // dedicated read fd as the segment.
            std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(path)
                .expect("test spill file open");
            let read_fd = File::open(path).expect("test spill read fd");
            let committed = Arc::new(AtomicU64::new(0));
            self.attach_spill_segment(Arc::new(read_fd), committed);
        }
        let segment = self.settled.writer_segment_id().expect("writer attached");
        let committed = self
            .settled
            .writer_committed_cell()
            .expect("writer attached");
        let start = committed.load(Ordering::Acquire);
        let batch = self.collect_spill_batch(usize::MAX);
        if batch.is_empty() {
            return 0;
        }
        let mut write_fd = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("test spill reopen");
        use std::io::Seek as _;
        write_fd
            .seek(std::io::SeekFrom::Start(start))
            .expect("seek to append offset");
        let (receipt, _new_offset) =
            write_spill_batch(&mut write_fd, start, &committed, segment, batch)
                .expect("test spill write");
        self.commit_spill(receipt)
    }
}
