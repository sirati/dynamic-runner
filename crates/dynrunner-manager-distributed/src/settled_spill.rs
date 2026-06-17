//! Settled-spill DRIVER: the per-coordinator scheduler that turns the
//! `cluster_state::settled` storage primitives into a running spill.
//!
//! Single concern: WHEN settled entries move to disk — the sweep
//! cadence, the one-in-flight `spawn_blocking` write, the
//! durable-then-commit ordering, the degrade-on-IO-error policy, and
//! the stats-cadence observability line. WHAT settles / HOW records
//! are framed / HOW reads stay torn-free is `cluster_state::settled`'s
//! concern. Role-agnostic, exactly the `anti_entropy.rs` /
//! `snapshot_stream.rs` precedent: the policy lives here ONCE; the
//! primary, secondary, and observer loops each own one `select!` arm
//! (`event = driver.next_event()` → `driver.handle(event, &mut
//! cluster_state)`).
//!
//! # The sweep (owner-mandated OOM-sweep shape)
//!
//! collect → blocking write+flush → publish:
//!
//! 1. On a cadence tick, with no write in flight, CLONE up to a batch
//!    of settle-eligible fat entries off the loop
//!    ([`ClusterState::collect_spill_batch`] — the fat map keeps the
//!    originals, so every reader stays coherent while the write runs).
//! 2. `spawn_blocking` appends + flushes the batch and publishes the
//!    committed offset ([`cluster_state::write_spill_batch`]).
//! 3. The completion lands back on the loop as a [`SpillEvent`]; the
//!    receipt is applied ([`ClusterState::commit_spill`]) — only NOW do
//!    the fat bodies leave memory.
//!
//! # Degrade loudly, never corrupt
//!
//! A write failure (disk full, IO error) STOPS spilling for the rest of
//! the process: the collected entries simply stay fat (they were never
//! evicted), a throttled WARN names the error, and the run continues
//! fat-but-correct. The committed offset was not advanced past the last
//! good flush, so readers can never observe the torn tail.
//!
//! # File lifecycle
//!
//! One file per coordinator instance, `settled_CRDT.<role>.cbor` inside
//! the resolved work dir, created with TRUNCATE on open (the file is
//! scratch — a respawned process rebuilds replicated state via the
//! bootstrap stream and re-settles; it never reuses a predecessor's
//! file). The `<role>` suffix keeps the one-writer rule when a host
//! process runs several coordinators (a promoted primary next to its
//! worker-secondary): each instance writes its OWN file; inherited
//! settled bases are read through their own `Arc<File>` segments.
//!
//! # Work dir resolution
//!
//! `DYNRUNNER_WORK_DIR` (the wrapper mounts a scratch volume at
//! `/app/work-tmp` and exports it) when set, else a per-process dir
//! under `std::env::temp_dir()` — so the spill works identically under
//! SLURM, local subprocess, in-process, and test-fixture modes.

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use dynrunner_core::Identifier;

use crate::cluster_state::{ClusterState, SpillReceipt, write_spill_batch};

/// Env var the wrapper (and any operator) sets to place the spill; the
/// wrapper exports the in-container scratch work mount here.
pub const WORK_DIR_ENV: &str = "DYNRUNNER_WORK_DIR";

/// Sweep cadence: how often the driver checks the fat map for newly
/// settled entries. Latency-insensitive (a settled entry is correct
/// wherever it lives) — the cadence only bounds how long fat bodies
/// linger before eviction.
const SWEEP_INTERVAL: Duration = Duration::from_secs(5);

/// Max entries per write batch — bounds both the on-loop clone cost of
/// one collect and the transient double-residency (clone + original)
/// while a write is in flight.
const BATCH_MAX_ENTRIES: usize = 4096;

/// Emit the observability line roughly this often (in sweep ticks).
const STATS_EVERY_TICKS: u32 = 24;

/// Resolve the per-process work dir: `DYNRUNNER_WORK_DIR` when set
/// (the wrapper's scratch mount), else `temp_dir()/dynrunner-work-<pid>`
/// (every non-SLURM mode). Creates the dir; `Err` carries the reason
/// the caller degrades on (no spill — never fatal to the run).
pub fn resolve_work_dir() -> std::io::Result<PathBuf> {
    let dir = match std::env::var_os(WORK_DIR_ENV) {
        Some(v) if !v.is_empty() => PathBuf::from(v),
        _ => std::env::temp_dir().join(format!("dynrunner-work-{}", std::process::id())),
    };
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// What one loop wakeup of the driver means.
pub enum SpillEvent {
    /// Cadence tick: sweep + maybe kick a write.
    Sweep,
    /// A blocking write finished (the writer file rides back with it).
    WriteDone(Box<WriteDone>),
}

pub struct WriteDone {
    outcome: std::io::Result<(SpillReceipt, u64)>,
    file: File,
}

/// The writer half: the append fd + offset, present while idle; taken
/// (moved into the `spawn_blocking` closure) while a write is in
/// flight.
struct SpillWriter {
    file: File,
    offset: u64,
    committed: Arc<AtomicU64>,
    segment: u32,
}

/// Per-coordinator settled-spill driver. Construct with
/// [`SettledSpillDriver::start`] (attaches the segment to the
/// coordinator's `ClusterState`); add ONE loop arm:
///
/// ```ignore
/// event = self.settled_spill.next_event() => {
///     self.settled_spill.handle(event, &mut self.cluster_state);
/// }
/// ```
pub struct SettledSpillDriver {
    /// Lazily built on the first [`Self::next_event`] poll — `tokio::time::interval`
    /// panics outside a runtime, and the synchronous coordinator/test
    /// constructors call [`Self::start`] / [`Self::disabled`] before any
    /// runtime is necessarily entered. Deferring the build to the first
    /// async poll (where a runtime is guaranteed) keeps construction
    /// runtime-free without leaking the cadence concern to any caller.
    interval: Option<tokio::time::Interval>,
    done_tx: tokio::sync::mpsc::UnboundedSender<WriteDone>,
    done_rx: tokio::sync::mpsc::UnboundedReceiver<WriteDone>,
    /// `Some` while idle; `None` while a write is in flight or after a
    /// degrade.
    writer: Option<SpillWriter>,
    /// Latched on the first write error: spilling stops for the
    /// process lifetime (fat-but-correct).
    degraded: bool,
    /// Sweep ticks since the last stats line.
    ticks_since_stats: u32,
    /// Diagnostic identity for the log lines.
    role: &'static str,
}

impl SettledSpillDriver {
    /// Open `settled_CRDT.<role>.cbor` (truncate) under the resolved
    /// work dir, attach its read segment to `state`, and return the
    /// running driver. On ANY setup failure the driver comes up
    /// DISABLED (a WARN names the reason; the run proceeds fat) — the
    /// spill is an optimization and must never gate a run.
    pub fn start<I: Identifier>(role: &'static str, state: &mut ClusterState<I>) -> Self {
        let (done_tx, done_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut driver = Self {
            interval: None,
            done_tx,
            done_rx,
            writer: None,
            degraded: false,
            ticks_since_stats: 0,
            role,
        };
        match Self::open_spill_file(role) {
            Ok((path, write_fd, read_fd)) => {
                let committed = Arc::new(AtomicU64::new(0));
                let segment = state.attach_spill_segment(Arc::new(read_fd), Arc::clone(&committed));
                driver.writer = Some(SpillWriter {
                    file: write_fd,
                    offset: 0,
                    committed,
                    segment,
                });
                tracing::info!(role, path = %path.display(), "settled-CRDT spill enabled");
            }
            Err(e) => {
                driver.degraded = true;
                tracing::warn!(
                    role,
                    error = %e,
                    "settled-CRDT spill disabled (work dir / spill file \
                     unavailable); run continues with the full ledger in memory"
                );
            }
        }
        // The ALWAYS-ON output store rides the SAME construction seam (the
        // driver is the per-coordinator place a node-local scratch file is
        // opened), but is INDEPENDENT of the settle sweep: a completed
        // task's output is write-through-then-dropped to this file at
        // apply, not on the settle cadence. Every role persists the payload
        // to disk (`retains_payload = true`) — zero MEMORY residence on
        // every node, and promotion-safe (a node never loses an output from
        // memory because it never held it). On any open failure the store
        // stays in resident-fallback (a WARN names it; the run proceeds
        // fat-but-correct — the spill is an optimization, never a gate).
        match Self::open_output_file(role) {
            Ok((path, write_fd, read_fd)) => {
                let committed = Arc::new(AtomicU64::new(0));
                state.attach_output_segment(write_fd, Arc::new(read_fd), committed, true);
                tracing::info!(role, path = %path.display(), "always-on output store enabled");
            }
            Err(e) => {
                tracing::warn!(
                    role,
                    error = %e,
                    "always-on output store disabled (work dir / file \
                     unavailable); completed outputs retained in memory \
                     (fat-but-correct)"
                );
            }
        }
        driver
    }

    /// A driver that never spills (unit fixtures that want the arm shape
    /// without touching the filesystem).
    pub fn disabled(role: &'static str) -> Self {
        let (done_tx, done_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            interval: None,
            done_tx,
            done_rx,
            writer: None,
            degraded: true,
            ticks_since_stats: 0,
            role,
        }
    }

    fn open_spill_file(role: &str) -> std::io::Result<(PathBuf, File, File)> {
        Self::open_scratch_file(&format!("settled_CRDT.{role}.cbor"))
    }

    /// Open the always-on output store's scratch file (`outputs.<role>.cbor`),
    /// a SEPARATE file from the settled spill — the two are orthogonal
    /// backends (state fixed-points vs output payloads).
    fn open_output_file(role: &str) -> std::io::Result<(PathBuf, File, File)> {
        Self::open_scratch_file(&format!("outputs.{role}.cbor"))
    }

    /// Open one scratch file under the resolved work dir, returning its
    /// path + an append write fd + a separate read fd (the one-writer /
    /// lock-free-readers protocol). Truncate on open: the file is scratch,
    /// never reused across respawns (crash/restart rebuilds via the
    /// bootstrap stream). Shared by both the settled spill and the output
    /// store so the open shape is spelled once.
    fn open_scratch_file(name: &str) -> std::io::Result<(PathBuf, File, File)> {
        let dir = resolve_work_dir()?;
        let path = dir.join(name);
        let write_fd = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        // Readers hold their OWN fd (positionless pread).
        let read_fd = File::open(&path)?;
        Ok((path, write_fd, read_fd))
    }

    /// The loop arm's future: the next sweep tick or write completion.
    /// Cancel-safe (an `Interval` tick and an mpsc recv both are); never
    /// resolves into busy-spin — ticks are bounded by the cadence and
    /// completions by in-flight writes (≤1).
    pub async fn next_event(&mut self) -> SpillEvent {
        // Build the cadence on first poll (runtime guaranteed here), then
        // split the borrows so the `tick()` and `recv()` futures poll
        // together. The `interval` helper returns the field as `&mut
        // Interval`; bind it + the receiver to disjoint locals before the
        // `select!` so neither future re-borrows `self`.
        let interval = self.interval.get_or_insert_with(|| {
            let mut interval = tokio::time::interval(SWEEP_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval
        });
        let done_rx = &mut self.done_rx;
        tokio::select! {
            _ = interval.tick() => SpillEvent::Sweep,
            done = done_rx.recv() => {
                SpillEvent::WriteDone(Box::new(done.expect(
                    "done channel cannot close: self holds the sender",
                )))
            }
        }
    }

    /// Apply one event against the coordinator's `ClusterState`.
    pub fn handle<I: Identifier>(&mut self, event: SpillEvent, state: &mut ClusterState<I>) {
        match event {
            SpillEvent::Sweep => self.sweep(state),
            SpillEvent::WriteDone(done) => self.complete(*done, state),
        }
    }

    fn sweep<I: Identifier>(&mut self, state: &mut ClusterState<I>) {
        self.maybe_emit_stats(state);
        if self.degraded {
            return;
        }
        // One write in flight at a time (the writer fd is moved into
        // the blocking task); a sweep that finds it absent just waits
        // for the completion to re-arm.
        let Some(writer) = self.writer.take() else {
            return;
        };
        let batch = state.collect_spill_batch(BATCH_MAX_ENTRIES);
        if batch.is_empty() {
            self.writer = Some(writer);
            return;
        }
        let SpillWriter {
            mut file,
            offset,
            committed,
            segment,
        } = writer;
        let done_tx = self.done_tx.clone();
        // collect → blocking write+flush → publish (the OOM-sweep
        // pattern): the loop keeps running every other arm while the
        // batch encodes + writes off-thread.
        tokio::task::spawn_blocking(move || {
            let outcome = write_spill_batch(&mut file, offset, &committed, segment, batch);
            // The receiver outliving us is the steady state; a dropped
            // receiver means the coordinator wound down mid-write — the
            // write itself is moot then.
            let _ = done_tx.send(WriteDone { outcome, file });
        });
    }

    fn complete<I: Identifier>(&mut self, done: WriteDone, state: &mut ClusterState<I>) {
        match done.outcome {
            Ok((receipt, new_offset)) => {
                let written = receipt.written.len();
                let evicted = state.commit_spill(receipt);
                tracing::debug!(
                    role = self.role,
                    written,
                    evicted,
                    file_bytes = new_offset,
                    "settled-CRDT spill batch committed"
                );
                // Re-arm the writer at the new append offset. The
                // committed cell + segment id live in the state's
                // segment table; rebuild the writer half around the
                // returned fd.
                if let Some(w) = self.writer.as_mut() {
                    // Unreachable (one write in flight), kept total.
                    w.offset = new_offset;
                } else {
                    let committed = state
                        .settled_store()
                        .writer_committed_cell()
                        .expect("segment attached at start");
                    let segment = state
                        .settled_store()
                        .writer_segment_id()
                        .expect("segment attached at start");
                    self.writer = Some(SpillWriter {
                        file: done.file,
                        offset: new_offset,
                        committed,
                        segment,
                    });
                }
            }
            Err(e) => {
                // Degrade loudly: stop spilling, keep everything fat,
                // never corrupt. The committed offset was not advanced,
                // so the torn tail is invisible to every reader.
                self.degraded = true;
                self.writer = None;
                tracing::warn!(
                    role = self.role,
                    error = %e,
                    "settled-CRDT spill write failed; spilling DISABLED for \
                     this process — run continues with settled entries in \
                     memory (fat-but-correct)"
                );
            }
        }
    }

    fn maybe_emit_stats<I: Identifier>(&mut self, state: &ClusterState<I>) {
        self.ticks_since_stats += 1;
        if self.ticks_since_stats < STATS_EVERY_TICKS {
            return;
        }
        self.ticks_since_stats = 0;
        let store = state.settled_store();
        if store.is_empty() && self.degraded {
            return;
        }
        let (unsettled_eligible, unsettled_live) = state.fat_task_breakdown();
        let outputs = state.output_store();
        tracing::trace!(
            role = self.role,
            spill_file_bytes = store.committed_bytes(),
            records_written = store.records_committed(),
            index_entries = store.len(),
            index_bytes_approx = store.approx_index_bytes(),
            in_memory_unsettled = state.tasks_in_memory(),
            unsettled_settle_eligible = unsettled_eligible,
            unsettled_not_terminal = unsettled_live,
            output_store_file_bytes = outputs.committed_bytes(),
            output_store_records = outputs.index_len(),
            degraded = self.degraded,
            "settled-CRDT spill stats"
        );
    }
}
