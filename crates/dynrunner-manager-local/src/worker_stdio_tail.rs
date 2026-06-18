//! Bounded read of a worker subprocess's captured stdout/stderr tail,
//! plus the one place that splices it onto a failure's `error_message`.
//!
//! Single concern: turn "the last bytes a dead/failed worker wrote to its
//! capture file" into a bounded `String`, and present it uniformly on the
//! failure-report text channel. Both the local-manager and distributed-
//! secondary failure paths funnel through [`append_stdio_tail`] so the tail's
//! format (header marker, truncation) lives in exactly one place — no
//! per-call-site duplication.
//!
//! WHY this lives here and not at the factory: the *capture mechanism*
//! (which file, how stdio is redirected) is the [`crate::WorkerFactory`]'s
//! concern — only it knows where a worker's stdout/stderr went. But the
//! *bounded-read primitive* and the *failure-text splice* are task-agnostic
//! and shared by every caller, so they sit next to the trait that exposes
//! `worker_stdio_tail`, not inside any one factory implementation. A factory
//! that captures stdio (the subprocess factory) builds its tail by handing a
//! path to [`read_file_tail`]; a factory that captures nothing inherits the
//! trait's `None` default and no caller is any the wiser.

use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// Default upper bound on the captured-stdio tail spliced onto a failed
/// task's `error_message`. 16 KiB is large enough to carry a full Python
/// traceback plus the surrounding diagnostic prints, and small enough that
/// it never bloats the replicated CRDT failure record (the `error_message`
/// rides the existing `TaskFailed` wire field all the way to the consumer's
/// `FailedTask.error_message`). Deliberately a constant, not config: a
/// failure tail is a diagnostic, and a uniform bound keeps the wire-cost of
/// a failure storm predictable.
pub const DEFAULT_STDIO_TAIL_BYTES: u64 = 16 * 1024;

/// Marker that precedes a spliced stdio tail in an `error_message`, so a
/// consumer can find (and a human can recognise) the worker-process output
/// appended after the framework's own failure text.
const STDIO_TAIL_HEADER: &str = "\n\n--- worker stdout/stderr tail ---\n";

/// Note prepended to the tail when the source file was larger than the read
/// bound, so the reader knows earlier output was elided.
const TRUNCATION_NOTE: &str = "[...truncated, showing last bytes...]\n";

/// Read the last `max_bytes` bytes of the file at `path` as a UTF-8 (lossy)
/// string. Returns `None` when the path does not exist, cannot be opened or
/// read, or the file is empty — capturing a tail is strictly best-effort
/// diagnostics and must never turn into an error on the failure path.
///
/// When the file is larger than `max_bytes`, the read starts at
/// `len - max_bytes` and the returned string is prefixed with
/// [`TRUNCATION_NOTE`] so the caller/consumer can tell output was elided.
/// The seek-to-tail keeps the read O(max_bytes) regardless of how large the
/// worker's log grew (a long-running worker can accumulate megabytes of
/// per-task output before it dies).
pub fn read_file_tail(path: &Path, max_bytes: u64) -> Option<String> {
    let mut file = std::fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    if len == 0 {
        return None;
    }
    let (start, truncated) = if len > max_bytes {
        (len - max_bytes, true)
    } else {
        (0, false)
    };
    file.seek(SeekFrom::Start(start)).ok()?;
    // `start..len` is at most `max_bytes`, so the read is bounded.
    let mut buf = Vec::with_capacity((len - start) as usize);
    file.take(max_bytes).read_to_end(&mut buf).ok()?;
    if buf.is_empty() {
        return None;
    }
    let body = String::from_utf8_lossy(&buf);
    if truncated {
        Some(format!("{TRUNCATION_NOTE}{body}"))
    } else {
        Some(body.into_owned())
    }
}

/// Splice an optional captured-stdio `tail` onto a failure `error_message`,
/// behind a recognisable header marker. `None`/empty tail returns the
/// message unchanged, so a caller can pass the factory's
/// `worker_stdio_tail(..)` result straight through with no per-site branch.
///
/// This is the SINGLE owner of the tail's presentation on the failure-text
/// channel: every failure-report site (local raise/disconnect, distributed
/// raise/disconnect) calls it, so the header and join shape can never drift
/// between paths.
pub fn append_stdio_tail(error_message: String, tail: Option<String>) -> String {
    match tail {
        Some(t) if !t.is_empty() => format!("{error_message}{STDIO_TAIL_HEADER}{t}"),
        _ => error_message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// A file shorter than the bound is returned whole, with no truncation
    /// note.
    #[test]
    fn read_file_tail_returns_whole_short_file() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "Traceback (most recent call last):\nValueError: boom\n").unwrap();
        let tail = read_file_tail(f.path(), DEFAULT_STDIO_TAIL_BYTES).unwrap();
        assert!(tail.contains("ValueError: boom"));
        assert!(
            !tail.contains("truncated"),
            "short file must not be marked truncated: {tail:?}"
        );
    }

    /// A file larger than the bound is read from the TAIL: the last bytes
    /// are present, earlier bytes are elided, the read is bounded to
    /// roughly `max_bytes` (plus the truncation note), and the truncation
    /// note is prepended.
    #[test]
    fn read_file_tail_bounds_large_file_to_last_bytes() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        // 10 KiB of filler the bound must drop, then a 200-byte marker the
        // bound must keep.
        let filler = "A".repeat(10 * 1024);
        write!(f, "{filler}").unwrap();
        let marker = "Z".repeat(200);
        write!(f, "{marker}").unwrap();
        f.flush().unwrap();

        let max = 1024;
        let tail = read_file_tail(f.path(), max).unwrap();
        // The tail end (the marker) survives.
        assert!(tail.contains(&marker), "tail must keep the last bytes");
        // The head (filler) is dropped — the bound is honoured.
        assert!(
            !tail.contains(&"A".repeat(2 * 1024)),
            "tail must drop the head of an oversized file"
        );
        assert!(tail.starts_with(TRUNCATION_NOTE), "oversized read must note truncation");
        // Read stays bounded: at most `max` bytes of body + the note.
        assert!(
            tail.len() as u64 <= max + TRUNCATION_NOTE.len() as u64,
            "tail length {} exceeds bound {} + note",
            tail.len(),
            max
        );
    }

    /// Empty and missing files yield `None` (best-effort: nothing to show
    /// is not an error).
    #[test]
    fn read_file_tail_none_on_empty_or_missing() {
        let f = tempfile::NamedTempFile::new().unwrap();
        assert!(read_file_tail(f.path(), DEFAULT_STDIO_TAIL_BYTES).is_none());
        assert!(read_file_tail(Path::new("/no/such/file/xyzzy"), DEFAULT_STDIO_TAIL_BYTES).is_none());
    }

    /// `append_stdio_tail` splices behind the header when a tail is present
    /// and is a pass-through on `None`/empty — so a call site never needs
    /// its own branch.
    #[test]
    fn append_stdio_tail_splices_only_when_present() {
        let base = "task raised ValueError".to_string();
        let with = append_stdio_tail(base.clone(), Some("native fault\n".into()));
        assert!(with.starts_with("task raised ValueError"));
        assert!(with.contains("worker stdout/stderr tail"));
        assert!(with.contains("native fault"));

        assert_eq!(append_stdio_tail(base.clone(), None), base);
        assert_eq!(append_stdio_tail(base.clone(), Some(String::new())), base);
    }
}
