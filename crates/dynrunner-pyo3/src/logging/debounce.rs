//! Debounced operator-stdio writer for `--important-stdio-only`.
//!
//! Single concern: when importance mode is active, the operator-facing stdio
//! sink does not write each event straight to stdout. Instead every line
//! accumulates in a byte buffer and is flushed as ONE wake event under a
//! quiet-edge + max-delay debounce policy. This is the WRITER layer of the
//! importance-mode stdio sink — the emit sites ([`super::IMPORTANT_TARGET`]
//! call sites in the observer, run-narrator and reporter) and the gate filter
//! ([`super::important_stdio_filter`]) stay completely unaware: they still emit
//! / pass events exactly as before, and the only thing that changes is which
//! `MakeWriter` [`super::stdio_layer`] is handed.
//!
//! ## Why
//!
//! Each flush to the real stdout is ONE wake event for the LLM operator
//! reading the stream. A burst — a stats grid plus a piggybacked reconnect
//! note plus a few narrator lines, all emitted within milliseconds — must
//! coalesce into a SINGLE wake event rather than N partial reads. (Owner spec,
//! task #412: "output only flushes after 500ms of no further output, at most
//! delay a flush by 5 seconds … best to impl this via a buffer rather than
//! relying on flush".)
//!
//! ## Policy
//!
//!   * [`QUIET_EDGE`] (500 ms): flush once this long elapses with NO new write
//!     — the trailing edge of a burst.
//!   * [`MAX_DELAY`] (5 s): never delay a buffered line longer than this past
//!     the OLDEST unflushed byte, so a continuously-talking run is not starved
//!     of wake events forever.
//!
//! ## Flush guarantees
//!
//! The buffer must NEVER eat a line on a crash/exit. Three independent hooks
//! cover every terminal path:
//!
//!   1. The background flusher thread fires the quiet-edge / max-delay flushes
//!      during normal operation.
//!   2. A `libc::atexit` handler (registered once at construction) flushes on
//!      EVERY `std::process::exit` — including the `exit(137)` / `exit(1)`
//!      fatal paths in the manager run loops, which run C `atexit` handlers but
//!      NOT Rust destructors.
//!   3. [`flush_now`], exposed to the Python fatal-surfacing path
//!      (`logging_setup._flush_all_logging`) via the `flush_important_stdio`
//!      pyfunction, flushes synchronously and immediately the moment a fatal
//!      error is surfaced — so the diagnosable line is on the wire before the
//!      process tears down, not merely at the atexit backstop.
//!
//! `Drop` on the writer state also flushes, covering scoped teardown (tests
//! and any future non-global installation).

use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use tracing_subscriber::fmt::MakeWriter;

/// Quiet-edge debounce window: flush once this long passes with no further
/// write. The trailing edge of a burst — a stats grid, a piggybacked
/// reconnect note, and narrator lines emitted within milliseconds collapse
/// into one wake event for the LLM operator. (Owner spec #412: "only flushes
/// after 500ms of no further output".)
pub(crate) const QUIET_EDGE: Duration = Duration::from_millis(500);

/// Max-delay bound: never hold a buffered line longer than this past the
/// OLDEST unflushed byte, even if output keeps arriving and the quiet edge
/// never lands. Bounds starvation so a continuously-talking run still produces
/// periodic wake events. (Owner spec #412: "at most delay a flush by 5
/// seconds".)
pub(crate) const MAX_DELAY: Duration = Duration::from_secs(5);

/// The pure debounce decision: given the current instant and the two
/// timestamps the buffer tracks, decide whether the buffer must flush now,
/// wait until a deadline, or sit idle (empty).
///
/// Single concern: the timing policy, with NO threads, locks, or I/O — so it
/// is exhaustively unit-testable with plain `Duration` arithmetic. The driver
/// thread ([`DebounceState::run_flusher`]) consumes this to decide its next
/// timed wait, and the math lives in exactly one place.
#[derive(Debug, PartialEq, Eq)]
enum FlushDecision {
    /// Buffer is empty — nothing to flush, park until woken by a write.
    Idle,
    /// At least one of the two bounds is satisfied at `now` — flush.
    FlushNow,
    /// Buffer is non-empty but neither bound is met yet — wait this long, then
    /// re-decide (a later write can only push the quiet edge further out, never
    /// closer, and cannot move the max-delay deadline, so re-deciding on wake
    /// is correct).
    WaitFor(Duration),
}

/// Decide the flush action for a buffer whose oldest unflushed byte arrived at
/// `oldest_unflushed` and whose most recent write was at `last_write`
/// (both `None` iff the buffer is empty).
///
/// Flush when EITHER bound is reached — quiet edge
/// (`now - last_write >= QUIET_EDGE`) or max delay
/// (`now - oldest_unflushed >= MAX_DELAY`); otherwise wait until the EARLIER of
/// the two pending deadlines.
fn flush_decision(
    now: Instant,
    oldest_unflushed: Option<Instant>,
    last_write: Option<Instant>,
) -> FlushDecision {
    let (Some(oldest), Some(last)) = (oldest_unflushed, last_write) else {
        return FlushDecision::Idle;
    };
    // Remaining time on each bound. `checked_sub` is `None` once the elapsed
    // time has PASSED the window; treat a zero remainder (elapsed == window,
    // exactly AT the deadline) as reached too — otherwise the driver would
    // `WaitFor(0)` and busy-spin instead of flushing.
    let quiet_remaining = QUIET_EDGE
        .checked_sub(now.saturating_duration_since(last))
        .filter(|d| !d.is_zero());
    let max_remaining = MAX_DELAY
        .checked_sub(now.saturating_duration_since(oldest))
        .filter(|d| !d.is_zero());
    match (quiet_remaining, max_remaining) {
        // Either bound reached (`None` once elapsed >= the window).
        (None, _) | (_, None) => FlushDecision::FlushNow,
        // Both pending: wait until the EARLIER deadline, then re-decide.
        (Some(q), Some(m)) => FlushDecision::WaitFor(q.min(m)),
    }
}

/// Shared mutable buffer + timestamps, guarded by one mutex, with a condvar the
/// driver thread waits on. The sink is boxed behind [`Write`] so tests inject
/// an in-memory buffer in place of the real stdout.
struct Inner {
    buf: Vec<u8>,
    /// When the oldest unflushed byte was written (drives [`MAX_DELAY`]).
    oldest_unflushed: Option<Instant>,
    /// When the most recent byte was written (drives [`QUIET_EDGE`]).
    last_write: Option<Instant>,
    /// The real destination the coalesced bytes are flushed to.
    sink: Box<dyn Write + Send>,
}

impl Inner {
    /// Write the whole buffer to the sink and clear the timestamps. Best-effort
    /// on the I/O: a broken stdout pipe must not poison the mutex or panic the
    /// flusher thread (the operator may have closed the reader), so a write
    /// error is swallowed but the buffer is still drained so it cannot grow
    /// unbounded.
    fn flush_locked(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        let _ = self.sink.write_all(&self.buf);
        let _ = self.sink.flush();
        self.buf.clear();
        self.oldest_unflushed = None;
        self.last_write = None;
    }
}

/// The debounce engine: shared state plus the condvar the driver waits on and
/// the shutdown flag. Owns the buffer's lifecycle; the [`DebouncedWriter`]
/// handed to the tracing layer is a thin newtype over an `Arc<DebounceState>`.
pub(crate) struct DebounceState {
    inner: Mutex<Inner>,
    /// Woken on every write (so the driver re-evaluates its deadline) and on
    /// shutdown.
    cvar: Condvar,
    /// Set on `Drop` to tell the driver thread to flush-and-exit.
    shutdown: AtomicBool,
}

impl DebounceState {
    fn new(sink: Box<dyn Write + Send>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                buf: Vec::new(),
                oldest_unflushed: None,
                last_write: None,
                sink,
            }),
            cvar: Condvar::new(),
            shutdown: AtomicBool::new(false),
        })
    }

    /// Append `bytes` to the buffer, stamp the write instants, and wake the
    /// driver so it (re)computes its deadline. Called synchronously from the
    /// tracing `fmt` layer's writer on whatever thread emitted the event —
    /// sync OR async context — so it must never block on anything but the
    /// short buffer mutex (no `.await`, no I/O here).
    fn push(&self, bytes: &[u8]) {
        let now = Instant::now();
        let mut inner = self.inner.lock().expect("debounce buffer mutex poisoned");
        if inner.oldest_unflushed.is_none() {
            inner.oldest_unflushed = Some(now);
        }
        inner.last_write = Some(now);
        inner.buf.extend_from_slice(bytes);
        drop(inner);
        self.cvar.notify_one();
    }

    /// Flush the buffer immediately and synchronously. The fatal-surfacing
    /// hook and the `atexit` handler call this so a diagnosable line is on the
    /// wire before teardown, independent of the driver thread's schedule.
    fn flush_now(&self) {
        let mut inner = self.inner.lock().expect("debounce buffer mutex poisoned");
        inner.flush_locked();
    }

    /// The driver loop: wait on the condvar for the next deadline (or a write /
    /// shutdown wake), then flush when [`flush_decision`] says so. Runs on a
    /// dedicated `std::thread` because the writer is invoked from non-async
    /// contexts too — a tokio timer would not cover those, whereas a condvar
    /// timed wait is context-agnostic.
    fn run_flusher(self: &Arc<Self>) {
        let mut inner = self.inner.lock().expect("debounce buffer mutex poisoned");
        loop {
            if self.shutdown.load(Ordering::Acquire) {
                inner.flush_locked();
                return;
            }
            match flush_decision(Instant::now(), inner.oldest_unflushed, inner.last_write) {
                FlushDecision::FlushNow => {
                    inner.flush_locked();
                    // Re-loop: now Idle, so we park on the condvar below.
                }
                FlushDecision::Idle => {
                    // Empty buffer: park until a write or shutdown wakes us.
                    inner = self
                        .cvar
                        .wait(inner)
                        .expect("debounce buffer mutex poisoned");
                }
                FlushDecision::WaitFor(d) => {
                    // A pending deadline: timed-wait so a later write can wake
                    // us early to re-evaluate (it can only extend the quiet
                    // edge), and we still wake at the deadline to flush.
                    let (guard, _timeout) = self
                        .cvar
                        .wait_timeout(inner, d)
                        .expect("debounce buffer mutex poisoned");
                    inner = guard;
                }
            }
        }
    }
}

impl Drop for DebounceState {
    fn drop(&mut self) {
        // Scoped teardown (tests, any future non-global install): flush what's
        // left so a dropped writer never eats buffered lines. The driver thread
        // holds its own `Arc` clone, so this `Drop` runs only once every handle
        // is gone — at which point flushing the buffer directly is sound (no
        // concurrent driver). The process-global install never hits this (the
        // subscriber lives for the whole process); `atexit` covers that path.
        if let Ok(inner) = self.inner.get_mut() {
            inner.flush_locked();
        }
    }
}

/// Process-global handle to the live debounce engine, set once when the
/// importance-mode stdio sink is installed. The `atexit` handler and the
/// `flush_important_stdio` pyfunction read it to flush without threading a
/// handle through every call site. `None` until the importance-mode sink is
/// installed (or in normal mode), so both hooks are no-ops off importance mode.
static GLOBAL: OnceLock<Arc<DebounceState>> = OnceLock::new();

/// The `MakeWriter` handed to [`super::stdio_layer`] in importance mode. A
/// thin clone-able handle over the shared engine; each `make_writer` yields a
/// guard whose `write` pushes into the buffer (the tracing fmt layer writes a
/// whole formatted line per event, so a push is a whole-line append).
#[derive(Clone)]
pub(crate) struct DebouncedWriter(Arc<DebounceState>);

/// The per-event writer guard the fmt layer obtains from
/// [`DebouncedWriter::make_writer`]. `write` appends to the shared buffer;
/// `flush` is a no-op because flushing is owned by the debounce policy (the
/// driver thread / atexit / fatal hook), NOT by the per-event fmt flush — the
/// whole point is to decouple wake events from per-line flushes.
pub(crate) struct DebouncedGuard(Arc<DebounceState>);

impl Write for DebouncedGuard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // WHOLE-LINE INTEGRITY: tracing's fmt layer formats each event into one
        // string and emits it with a single `write_all`; consuming the whole
        // slice in one `push` (returning `buf.len()`) makes `write_all` do
        // exactly ONE locked `push` per event. The flusher can therefore only
        // ever acquire the buffer mutex BETWEEN events, never mid-event, so a
        // flush never splits a partial line.
        self.0.push(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // Deliberately a no-op: the per-event fmt flush must NOT force a wake.
        // Flushing is the debounce policy's job (quiet-edge / max-delay /
        // fatal-path / atexit), never the emitter's.
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for DebouncedWriter {
    type Writer = DebouncedGuard;
    fn make_writer(&'a self) -> Self::Writer {
        DebouncedGuard(self.0.clone())
    }
}

/// Build the importance-mode debounced stdout writer: spawn the driver thread,
/// register the `atexit` flush backstop, publish the global flush handle, and
/// return the `MakeWriter` for [`super::stdio_layer`]. The sink is the real
/// stdout (locked per flush). Installed by [`super::build_layers`] ONLY when
/// importance mode is on; the normal path keeps the unbuffered `io::stdout`.
pub(crate) fn install_debounced_stdout() -> DebouncedWriter {
    install_with_sink(Box::new(io::stdout()))
}

/// Inner constructor over an arbitrary sink so tests inject an in-memory
/// buffer. Spawns the driver thread and registers the atexit hook against the
/// global handle.
fn install_with_sink(sink: Box<dyn Write + Send>) -> DebouncedWriter {
    let state = DebounceState::new(sink);
    // The driver thread holds its own clone, so the state outlives every
    // `DebouncedWriter`/guard until the thread exits on shutdown.
    let driver_state = Arc::clone(&state);
    std::thread::Builder::new()
        .name("important-stdio-debounce".into())
        .spawn(move || driver_state.run_flusher())
        .expect("failed to spawn important-stdio debounce flusher thread");

    // Publish the global handle for the atexit backstop and the fatal-path
    // `flush_now`. `set` fails only if already set (a second importance-mode
    // install — impossible given the subscriber is installed once), in which
    // case the first handle remains authoritative; ignore.
    let _ = GLOBAL.set(Arc::clone(&state));
    register_atexit_flush();

    DebouncedWriter(state)
}

/// Flush the global importance-mode buffer if one is installed; no-op
/// otherwise. The synchronous fatal-path / atexit entry point.
pub(crate) fn flush_now() {
    if let Some(state) = GLOBAL.get() {
        state.flush_now();
    }
}

/// Register the C `atexit` handler that flushes the global buffer on every
/// `std::process::exit` (the manager run loops' `exit(137)` / `exit(1)` fatal
/// paths run C atexit handlers but NOT Rust `Drop`). Idempotent across the
/// process: registered once, guarded by a `OnceLock`, so a re-entrant install
/// cannot stack duplicate handlers.
fn register_atexit_flush() {
    static REGISTERED: OnceLock<()> = OnceLock::new();
    REGISTERED.get_or_init(|| {
        extern "C" fn atexit_flush() {
            flush_now();
        }
        // SAFETY: `libc::atexit` takes an `extern "C" fn` with no arguments and
        // no return; `atexit_flush` matches that ABI exactly and only touches
        // the `OnceLock`-guarded global (no captured state, no unwinding across
        // the FFI boundary — `flush_now` cannot panic on the broken-pipe path
        // because `flush_locked` swallows I/O errors).
        unsafe {
            libc::atexit(atexit_flush);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// A `Write` sink over a shared in-memory buffer so a test reads back
    /// exactly what (and when) the engine flushed.
    #[derive(Clone, Default)]
    struct SharedSink(Arc<Mutex<Vec<u8>>>);

    impl SharedSink {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl Write for SharedSink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    // ---- Pure decision-logic tests (deterministic, no threads/wall clock) ----

    #[test]
    fn empty_buffer_is_idle() {
        assert_eq!(flush_decision(Instant::now(), None, None), FlushDecision::Idle);
    }

    #[test]
    fn quiet_edge_reached_flushes_now() {
        let now = Instant::now();
        let last = now - QUIET_EDGE; // exactly the window → reached
        let oldest = now - QUIET_EDGE;
        assert_eq!(flush_decision(now, Some(oldest), Some(last)), FlushDecision::FlushNow);
    }

    #[test]
    fn max_delay_reached_flushes_now_even_with_recent_write() {
        let now = Instant::now();
        // A write 1ms ago (quiet edge NOT met) but the oldest byte is 5s old.
        let last = now - Duration::from_millis(1);
        let oldest = now - MAX_DELAY;
        assert_eq!(
            flush_decision(now, Some(oldest), Some(last)),
            FlushDecision::FlushNow,
            "max-delay bound must fire even while writes keep arriving"
        );
    }

    #[test]
    fn neither_bound_met_waits_for_earlier_deadline() {
        let now = Instant::now();
        // last write 100ms ago → 400ms left on the quiet edge.
        let last = now - Duration::from_millis(100);
        // oldest 200ms ago → 4800ms left on the max delay.
        let oldest = now - Duration::from_millis(200);
        // The earlier deadline is the quiet edge.
        assert_eq!(
            flush_decision(now, Some(oldest), Some(last)),
            FlushDecision::WaitFor(QUIET_EDGE - Duration::from_millis(100))
        );
    }

    #[test]
    fn near_max_delay_waits_for_max_delay_when_it_is_earlier() {
        let now = Instant::now();
        // Continuous writes: last write 10ms ago (490ms of quiet edge left),
        // but the oldest byte is 4.9s old (only 100ms of max delay left).
        let last = now - Duration::from_millis(10);
        let oldest = now - (MAX_DELAY - Duration::from_millis(100));
        assert_eq!(
            flush_decision(now, Some(oldest), Some(last)),
            FlushDecision::WaitFor(Duration::from_millis(100)),
            "must wait for the EARLIER (max-delay) deadline under continuous writes"
        );
    }

    // ---- End-to-end tests through the threaded writer (real time, generous) ----

    /// Poll the sink until it is non-empty or the deadline passes; returns the
    /// elapsed time to first content (or the full wait on timeout).
    fn wait_for_content(sink: &SharedSink, within: Duration) -> Option<Duration> {
        let start = Instant::now();
        while start.elapsed() < within {
            if !sink.contents().is_empty() {
                return Some(start.elapsed());
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        None
    }

    #[test]
    fn burst_of_writes_coalesces_into_one_flush_after_quiet_edge() {
        // (a) 5 writes within ~100ms → ONE flush after ~500ms quiet, all 5
        // lines together, and nothing on the wire before the quiet edge.
        let sink = SharedSink::default();
        let writer = install_with_sink(Box::new(sink.clone()));
        for i in 0..5 {
            writer.make_writer().write_all(format!("line-{i}\n").as_bytes()).unwrap();
            std::thread::sleep(Duration::from_millis(20));
        }
        // Still buffered well before the quiet edge elapses.
        assert!(
            sink.contents().is_empty(),
            "burst leaked to stdout before the quiet edge: {:?}",
            sink.contents()
        );
        // Flushes once the quiet edge passes (generous upper bound for the
        // poll/scheduler jitter).
        let elapsed = wait_for_content(&sink, Duration::from_millis(1500))
            .expect("burst never flushed after the quiet edge");
        assert!(
            elapsed >= QUIET_EDGE - Duration::from_millis(100),
            "flushed too early ({elapsed:?}) — quiet edge not honoured"
        );
        let out = sink.contents();
        for i in 0..5 {
            assert!(out.contains(&format!("line-{i}")), "missing line-{i} in {out:?}");
        }
        drop(writer);
    }

    #[test]
    fn continuous_writes_flush_by_max_delay_not_starved() {
        // (b) writes every 200ms for ~6s → the quiet edge never lands, but a
        // flush MUST happen by the max-delay bound (~5s), never starved.
        let sink = SharedSink::default();
        let writer = install_with_sink(Box::new(sink.clone()));
        let stop = Arc::new(AtomicBool::new(false));
        let stop_w = Arc::clone(&stop);
        let w = writer.clone();
        let feeder = std::thread::spawn(move || {
            let mut i = 0;
            while !stop_w.load(Ordering::Acquire) {
                w.make_writer().write_all(format!("c-{i}\n").as_bytes()).unwrap();
                i += 1;
                std::thread::sleep(Duration::from_millis(200));
            }
        });
        // First flush must arrive by the max-delay bound (+ generous jitter),
        // well before the 6s the feeder would otherwise run.
        let elapsed = wait_for_content(&sink, MAX_DELAY + Duration::from_millis(1500))
            .expect("continuous writes were starved — no max-delay flush");
        assert!(
            elapsed >= MAX_DELAY - Duration::from_millis(500),
            "flushed before the max-delay bound ({elapsed:?}) despite continuous \
             writes resetting the quiet edge"
        );
        stop.store(true, Ordering::Release);
        feeder.join().unwrap();
        drop(writer);
    }

    #[test]
    fn single_line_flushes_after_quiet_edge() {
        // (c) one line → flushed at ~the quiet edge.
        let sink = SharedSink::default();
        let writer = install_with_sink(Box::new(sink.clone()));
        writer.make_writer().write_all(b"solo\n").unwrap();
        assert!(sink.contents().is_empty(), "single line flushed before the quiet edge");
        let elapsed = wait_for_content(&sink, Duration::from_millis(1500))
            .expect("single line never flushed");
        assert!(
            elapsed >= QUIET_EDGE - Duration::from_millis(100),
            "single line flushed too early: {elapsed:?}"
        );
        assert!(sink.contents().contains("solo"));
        drop(writer);
    }

    #[test]
    fn drop_flushes_remaining_buffer() {
        // (d) dropping the writer (scoped teardown) flushes what's buffered —
        // a dropped writer must not eat lines.
        let sink = SharedSink::default();
        let writer = install_with_sink(Box::new(sink.clone()));
        writer.make_writer().write_all(b"pending-on-drop\n").unwrap();
        // Drop BEFORE the quiet edge would have flushed it.
        assert!(sink.contents().is_empty(), "flushed before drop");
        drop(writer);
        // The driver thread observes shutdown and flushes; give it a moment.
        let flushed = wait_for_content(&sink, Duration::from_millis(1000))
            .expect("drop did not flush the remaining buffer");
        let _ = flushed;
        assert!(
            sink.contents().contains("pending-on-drop"),
            "drop ate the buffered line: {:?}",
            sink.contents()
        );
    }

    #[test]
    fn flush_now_drains_immediately() {
        // The fatal-path / atexit hook flushes synchronously, without waiting
        // for the quiet edge. Driven directly on the state so it does not
        // depend on the process-global handle (which a sibling test may own).
        let sink = SharedSink::default();
        let state = DebounceState::new(Box::new(sink.clone()));
        state.push(b"fatal-line\n");
        assert!(sink.contents().is_empty(), "pushed line flushed without a flush call");
        state.flush_now();
        assert!(
            sink.contents().contains("fatal-line"),
            "flush_now did not drain the buffer: {:?}",
            sink.contents()
        );
    }
}
