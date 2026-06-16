//! Size-aware ring writer: the inner `Write` of a file sink's non-blocking
//! appender, bounding the on-disk volume of a forensic-complete TRACE log
//! while always retaining the failure-proximate TAIL.
//!
//! Single concern: cap the bytes a single role-log file (`setup.log`,
//! `primary.log`, …) may occupy on disk, by maintaining a fixed-size RING of
//! rotated segments behind a LIVE segment that keeps the stable base filename.
//! Nothing about routing, formatting or non-blocking buffering lives here —
//! this struct is purely the byte-counting + rotate-on-threshold writer that
//! [`super::non_blocking_file`] wraps.
//!
//! WHY a custom ring and not `tracing-appender`'s `RollingFileAppender`:
//! that crate rotates by TIME (or `max_log_files` count over time-rotated
//! files) only — it has no size trigger — and, decisively, its rotation
//! CHANGES the active filename (a date suffix). The framework's role routing
//! and the Python `_fault_dumps` dump-path derivation both key off the STABLE
//! base filename (`setup.log` etc.), so a name-changing rotation would break
//! both. This ring keeps the LIVE segment at the base path always; only the
//! older, pruned-away segments carry the numeric `.1`…`.K` suffix.
//!
//! WHY it slots UNDER the non-blocking appender: `NonBlocking`'s single drain
//! thread owns the inner writer, so the ring's `rename`/`unlink` syscalls run
//! OFF the async runtime — no oploop impact and no fd contention (the same
//! drain-thread that already owns the write performs the rotation).
//!
//! ## Ring geometry (cap math)
//!
//! The ring keeps the LIVE segment plus up to [`RETAINED_SEGMENTS`] (`K`)
//! rotated segments, each of at most `segment_bytes` bytes. Total on-disk is
//! therefore bounded by `(K + 1) * segment_bytes`. Given a caller-supplied
//! `max_bytes` cap we derive `segment_bytes = max_bytes / (K + 1)` so the
//! total stays at or below the cap (integer division only rounds DOWN, so the
//! true bound `(K + 1) * (max_bytes / (K + 1)) <= max_bytes` holds — the cap
//! is never exceeded, at worst slightly under-shot). A floor of one byte
//! guards a pathologically tiny cap so a segment can always make forward
//! progress (the live segment is permitted to exceed `segment_bytes` by AT
//! MOST one `write()` call's length, since the threshold is checked before
//! each write and a single buffered line is written whole).
//!
//! ## Rotation order (crash-tolerance)
//!
//! On reaching the threshold the live segment is rotated out and a fresh empty
//! live segment is opened, OLDEST-FIRST so no rename ever clobbers a segment
//! that has not yet been shifted:
//!
//!   1. unlink `base.K` (the oldest retained segment falls out of the ring),
//!   2. rename `base.{K-1} -> base.K`, …, `base.1 -> base.2` (shift up),
//!   3. rename `base -> base.1` (the just-filled live segment becomes newest
//!      retained),
//!   4. reopen a fresh empty `base` as the new live segment.
//!
//! This is RENAME-then-REOPEN, never truncate-in-place: a hard abort at any
//! step leaves the live data either still at `base` (before step 3) or at
//! `base.1` (after step 3) — it is never lost to a truncation. The LIVE
//! (newest) segment is the only one never pruned, so the failure-proximate
//! tail is always on disk.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Number of rotated segments retained behind the live segment (`K`). Total
/// on-disk is `(K + 1) * segment_bytes` (the `+1` is the live segment). Small
/// so the cap divides into a handful of reasonably-sized segments rather than
/// many tiny ones: with `K = 4` the cap splits into 5 segments, so the default
/// 2 GiB cap gives ~409 MiB segments — coarse enough that rotation is rare on a
/// normal run, fine enough that a capped scale run keeps a multi-segment tail.
const RETAINED_SEGMENTS: u64 = 4;

/// A size-bounded ring over a base log path. Implements [`Write`] by appending
/// to a LIVE file at the base path, counting bytes, and rotating the ring once
/// the live segment would exceed `segment_bytes` — keeping total on-disk at or
/// below the configured cap while never pruning the newest (tail) segment.
///
/// Constructed via [`RingWriter::new`]. The opt-out (unbounded) path does NOT
/// use this type at all — see [`super::non_blocking_file`], which passes the
/// bare append-create file when `max_bytes == 0`, preserving exact prior
/// behaviour.
pub(crate) struct RingWriter {
    /// Stable base path — the live segment always lives here.
    base: PathBuf,
    /// The currently-open live segment (always the file at `base`).
    live: File,
    /// Bytes written to the current live segment so far.
    live_len: u64,
    /// Per-segment byte budget; the live segment rotates once a write would
    /// carry it past this. Derived from the cap (see module docs).
    segment_bytes: u64,
}

impl RingWriter {
    /// Open a ring over `base_path` bounded to roughly `max_bytes` total on
    /// disk. `max_bytes` must be non-zero (the `== 0` unbounded opt-out is
    /// handled by the caller, which never constructs a `RingWriter`); the
    /// per-segment budget is `max_bytes / (K + 1)` floored at one byte.
    ///
    /// The live segment is opened append-create (so a resumed run never
    /// truncates a prior run's tail), and its starting length is read back
    /// from the file's current size so the byte counter is correct across a
    /// process restart that re-opens a partially-filled live segment.
    pub(crate) fn new(base_path: PathBuf, max_bytes: u64) -> io::Result<Self> {
        let segment_bytes = (max_bytes / (RETAINED_SEGMENTS + 1)).max(1);
        let live = open_append_create(&base_path)?;
        // Resume-correct: a re-opened live segment may already hold bytes from
        // a prior run (append-create never truncates), so seed the counter
        // from the on-disk size rather than assuming an empty file.
        let live_len = live.metadata()?.len();
        Ok(Self {
            base: base_path,
            live,
            live_len,
            segment_bytes,
        })
    }

    /// The path of the `n`th rotated segment (`base.n`, `1 <= n <= K`).
    fn segment_path(&self, n: u64) -> PathBuf {
        let mut name = self.base.as_os_str().to_owned();
        name.push(format!(".{n}"));
        PathBuf::from(name)
    }

    /// Rotate the ring: retire the full live segment into `base.1`, shift the
    /// retained segments up (dropping the oldest), and reopen a fresh empty
    /// live segment at the base path. Oldest-first + rename-then-reopen, so a
    /// hard abort mid-rotation never loses the live data (see module docs).
    fn rotate(&mut self) -> io::Result<()> {
        // 1. Drop the oldest retained segment out of the ring. `remove_file`
        //    on a missing path is the only non-fatal case (the ring may not be
        //    full yet), so ignore NotFound and surface anything else.
        let oldest = self.segment_path(RETAINED_SEGMENTS);
        match fs::remove_file(&oldest) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        // 2. Shift the retained segments up: base.{K-1} -> base.K, …,
        //    base.1 -> base.2. Descending so a rename never clobbers a
        //    not-yet-shifted segment. A missing source is fine (ring not full).
        for n in (1..RETAINED_SEGMENTS).rev() {
            let from = self.segment_path(n);
            let to = self.segment_path(n + 1);
            match fs::rename(&from, &to) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        // 3. The just-filled live segment becomes the newest retained segment.
        //    Until step 4 reopens it the live data lives at base.1 — never
        //    truncated, so a crash here keeps the tail.
        fs::rename(&self.base, self.segment_path(1))?;
        // 4. Reopen a fresh empty live segment at the stable base path.
        self.live = open_append_create(&self.base)?;
        self.live_len = 0;
        Ok(())
    }
}

impl Write for RingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Rotate BEFORE writing when the live segment is already at/over the
        // per-segment budget, so each segment holds at most `segment_bytes`
        // plus at most one buffered line (the non-blocking appender hands us
        // whole lines). Checking before the write — rather than after —
        // guarantees we never grow a segment unboundedly within one call.
        if self.live_len >= self.segment_bytes {
            self.rotate()?;
        }
        let n = self.live.write(buf)?;
        self.live_len += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.live.flush()
    }
}

/// Open a file append-create, materialising the parent directory first.
/// Mirrors [`super::open_append_create`]'s create-parent + append semantics
/// but returns the `io::Result` rather than panicking, because rotation runs
/// on the appender's drain thread where an `io::Error` is the right currency
/// (the `Write` impl propagates it) rather than a process abort.
fn open_append_create(path: &Path) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    OpenOptions::new().create(true).append(true).open(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    /// Read a segment file's bytes, or empty if it does not exist.
    fn read_or_empty(path: &Path) -> Vec<u8> {
        let mut v = Vec::new();
        if let Ok(mut f) = File::open(path) {
            f.read_to_end(&mut v).unwrap();
        }
        v
    }

    /// A write that does NOT cross the per-segment budget leaves the live
    /// segment at the base path and creates no rotated segment.
    #[test]
    fn under_budget_does_not_rotate() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("setup.log");
        // cap 50 → segment_bytes = 50 / 5 = 10.
        let mut w = RingWriter::new(base.clone(), 50).unwrap();
        w.write_all(b"12345").unwrap();
        w.flush().unwrap();
        assert_eq!(read_or_empty(&base), b"12345");
        let dot1 = PathBuf::from(format!("{}.1", base.display()));
        assert!(!dot1.exists(), "rotated segment created under budget");
    }

    /// Writing past the per-segment budget rotates: the filled segment moves
    /// to `.1` and a fresh live segment holds the most-recent bytes (tail).
    #[test]
    fn over_segment_bytes_rotates_and_keeps_tail_live() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("setup.log");
        // cap 50 → segment_bytes = 10. First write fills to 12 (>= 10).
        let mut w = RingWriter::new(base.clone(), 50).unwrap();
        w.write_all(b"AAAAAAAAAAAA").unwrap(); // 12 bytes → live_len 12 >= 10
        w.write_all(b"BBBB").unwrap(); // triggers rotate, then writes to fresh live
        w.flush().unwrap();

        let dot1 = PathBuf::from(format!("{}.1", base.display()));
        assert!(dot1.exists(), "rotation did not create .1 segment");
        assert_eq!(read_or_empty(&dot1), b"AAAAAAAAAAAA", "filled segment lost");
        // The LIVE base file holds the most-recent bytes (the tail).
        assert_eq!(read_or_empty(&base), b"BBBB", "tail not in live segment");
    }

    /// Writing well past the cap prunes old segments: total retained files are
    /// bounded to live + K, `.{K+1}` never exists, and the live base file
    /// holds the most-recent write (the failure-proximate tail).
    #[test]
    fn over_cap_prunes_old_segments_total_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("setup.log");
        // cap 50 → segment_bytes = 10, K = 4 retained → 5 segments total.
        let mut w = RingWriter::new(base.clone(), 50).unwrap();
        // Write 10 distinct 12-byte chunks → 10 rotations, far past the ring.
        let mut last = Vec::new();
        for i in 0..10u8 {
            let chunk = vec![b'0' + i; 12];
            w.write_all(&chunk).unwrap();
            last = chunk;
        }
        w.flush().unwrap();

        // `.{K+1}` (= .5) must NEVER exist — the ring is bounded.
        let overflow = PathBuf::from(format!("{}.{}", base.display(), RETAINED_SEGMENTS + 1));
        assert!(
            !overflow.exists(),
            "segment beyond K retained still on disk: {overflow:?}"
        );

        // Total retained files: live (base) + at most K rotated (.1..=.K).
        let mut present = 0u64;
        if base.exists() {
            present += 1;
        }
        for n in 1..=RETAINED_SEGMENTS {
            if PathBuf::from(format!("{}.{n}", base.display())).exists() {
                present += 1;
            }
        }
        assert!(
            present <= RETAINED_SEGMENTS + 1,
            "more than live + K segments retained: {present}"
        );

        // The live base file holds the most-recent bytes — the tail survives.
        // The final 12-byte write lands after a rotation (live_len was >= 10),
        // so the live segment contains exactly the last chunk.
        assert_eq!(read_or_empty(&base), last, "most-recent tail not in live");
    }

    /// Re-opening an existing (non-empty) live segment seeds the byte counter
    /// from its on-disk size, so a resumed run does not under-count and
    /// over-fill the segment before its first rotation.
    #[test]
    fn reopen_seeds_live_len_from_existing_size() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("setup.log");
        {
            let mut w = RingWriter::new(base.clone(), 50).unwrap();
            w.write_all(b"AAAAA").unwrap(); // 5 bytes, under segment_bytes (10)
            w.flush().unwrap();
        }
        // Re-open: live_len must resume at 5, so a 6-byte write crosses 10 and
        // the FOLLOWING write rotates (proving the counter was not reset to 0).
        let mut w = RingWriter::new(base.clone(), 50).unwrap();
        w.write_all(b"BBBBBB").unwrap(); // live_len 5 -> 11 (no rotate before)
        w.write_all(b"C").unwrap(); // 11 >= 10 → rotate, C lands in fresh live
        w.flush().unwrap();
        let dot1 = PathBuf::from(format!("{}.1", base.display()));
        assert!(dot1.exists(), "resumed counter failed to trigger rotation");
        assert_eq!(read_or_empty(&dot1), b"AAAAABBBBBB");
        assert_eq!(read_or_empty(&base), b"C");
    }
}
