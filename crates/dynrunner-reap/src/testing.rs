//! Test doubles for the reap crate's [`crate::process_probe::ProcessProbe`]
//! and [`crate::clock::Clock`] traits. Lives in the library crate so this
//! crate's own tests AND both consumers' tests (the wrapper's in-band reap
//! tests, the shutdown-manager's poll-loop tests) share ONE implementation
//! — duplicating a process-probe mock would violate the single-concern /
//! no-duplication rules.
//!
//! These types compile in release builds too, but LTO+strip eliminates
//! them from the production binaries because neither `main` references
//! them.

use crate::clock::Clock;
use crate::process_probe::ProcessProbe;
use std::cell::RefCell;
use std::time::Duration;

/// Non-blocking clock. Records every sleep duration and never blocks.
/// Optionally fires a one-shot caller-supplied callback on the Nth sleep
/// so a test can simulate "an external event arrives partway through a
/// polling loop" (e.g. a signal flag flips) WITHOUT coupling this crate
/// to any consumer's flag type — the callback closure carries that
/// coupling on the consumer side.
/// A one-shot callback the [`FakeClock`] fires on the Nth sleep, paired
/// with that 1-based tick index. The closure carries any consumer-specific
/// coupling (e.g. setting a shutdown flag) so this crate stays decoupled.
type SleepCallback = (usize, Box<dyn FnMut()>);

#[derive(Default)]
pub struct FakeClock {
    sleeps: RefCell<Vec<Duration>>,
    callback_on: RefCell<Option<SleepCallback>>,
    count: RefCell<usize>,
}

impl FakeClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every sleep duration recorded, in order.
    pub fn sleeps(&self) -> Vec<Duration> {
        self.sleeps.borrow().clone()
    }

    /// On the Nth call to `sleep` (1-based), invoke `callback`. The
    /// consumer uses this to flip its own state (e.g. set a shutdown
    /// flag) mid-loop without raising a real signal — the closure owns
    /// the consumer-specific type so this crate stays decoupled.
    pub fn run_on_sleep(&self, n: usize, callback: Box<dyn FnMut()>) {
        *self.callback_on.borrow_mut() = Some((n, callback));
    }
}

impl Clock for FakeClock {
    fn sleep(&self, dur: Duration) {
        self.sleeps.borrow_mut().push(dur);
        let mut n = self.count.borrow_mut();
        *n += 1;
        let target_now = *n;
        let mut slot = self.callback_on.borrow_mut();
        if let Some((target, cb)) = slot.as_mut() {
            if *target == target_now {
                cb();
            }
        }
    }
}

/// Sentinel "this is still the original workload" start-time value used
/// by the reap-path mock constructors. Any value distinct from this one
/// (or `None`) models the process being gone or the PID reused.
pub const MOCK_WORKLOAD_START: u64 = 4242_4242;

/// Programmable [`ProcessProbe`]. Each `is_alive` call consumes one
/// scripted boolean; past the end of the script the probe sticks at the
/// final value (saturating) — this matches realistic semantics (once a
/// process has died, it stays dead).
#[derive(Default)]
pub struct MockProcessProbe {
    script: RefCell<Vec<bool>>,
    /// Saturating `is_alive` value once `script` is drained. Defaults to
    /// the last popped value, falling back to `false` when nothing was
    /// ever scripted.
    last: RefCell<bool>,
    calls: RefCell<u32>,
    /// Every `(pid, signal)` delivered through `signal`, in order, so
    /// reap tests can assert "SIGTERM then SIGKILL to the captured PID"
    /// without a real PID space.
    signals_sent: RefCell<Vec<(u32, i32)>>,
    /// Scripted `start_time` returns, consumed in order (saturating at
    /// the final value once drained). This is the SECOND observable
    /// channel — distinct from `script`/`is_alive` — and it drives the
    /// reap path's identity guard: the caller captures element 0 at
    /// PID-capture time, and the reap re-checks via the trait-default
    /// `is_same_process`, so a later element that differs from the
    /// captured value models a PID-reuse / process-gone event without a
    /// real `/proc`.
    start_time_script: RefCell<Vec<Option<u64>>>,
    /// Saturating value once `start_time_script` drains. Defaults to the
    /// last scripted value, falling back to `None`.
    start_time_last: RefCell<Option<u64>>,
}

impl MockProcessProbe {
    /// Construct a probe with a scripted sequence of `is_alive` returns.
    /// After the script is drained the most recent value is returned on
    /// every subsequent call. The `start_time` channel is left at its
    /// default (`None`, saturating) — `script` is for liveness-monitor
    /// tests; reap tests use [`MockProcessProbe::reap`] /
    /// [`MockProcessProbe::reap_start_times`].
    pub fn script(values: Vec<bool>) -> Self {
        let saturating = values.last().copied().unwrap_or(false);
        Self {
            script: RefCell::new(values),
            last: RefCell::new(saturating),
            ..Default::default()
        }
    }

    /// Reap-path probe whose identity-aware liveness mirrors a liveness
    /// intent. Element 0 of the intent is the PID-capture sighting; for
    /// the reap to have a captured start time at all it must be `true`.
    /// Each `true` maps to `Some(MOCK_WORKLOAD_START)` (still the SAME
    /// process) and each `false` to `None` (process gone). The capture
    /// call and every `is_same_process` re-check then consume this one
    /// `start_time` channel, so the trait-default identity comparison —
    /// not a mock-side override — decides alive-vs-gone.
    pub fn reap(intent: Vec<bool>) -> Self {
        let start_times = intent
            .into_iter()
            .map(|alive| alive.then_some(MOCK_WORKLOAD_START))
            .collect();
        Self::reap_start_times(start_times)
    }

    /// Reap-path probe driven by a raw `start_time` script. The caller's
    /// capture call consumes element 0; each `is_same_process` re-check
    /// consumes the next. A later element differing from the captured
    /// value models kernel PID reuse (same PID, new process); `None`
    /// models the process having exited. Saturates at the final value
    /// once drained.
    pub fn reap_start_times(start_times: Vec<Option<u64>>) -> Self {
        let saturating = start_times.last().copied().unwrap_or(None);
        Self {
            start_time_script: RefCell::new(start_times),
            start_time_last: RefCell::new(saturating),
            ..Default::default()
        }
    }

    /// Probe that always reports the process as alive — the test default
    /// for paths that do not exercise the liveness-monitor branch. Its
    /// `start_time` channel also saturates at a fixed value, so on the
    /// reap path it reports the process as the SAME live process forever
    /// (the "orphan never dies" case).
    pub fn always_alive() -> Self {
        Self {
            last: RefCell::new(true),
            start_time_last: RefCell::new(Some(MOCK_WORKLOAD_START)),
            ..Default::default()
        }
    }

    /// Probe that always reports the process as dead. Its `start_time`
    /// channel saturates at `None` (no process), the default.
    pub fn always_dead() -> Self {
        Self {
            last: RefCell::new(false),
            ..Default::default()
        }
    }

    /// Number of times the loop has asked us — useful for asserting the
    /// probe was consulted (or NOT consulted, in inertness tests).
    pub fn calls(&self) -> u32 {
        *self.calls.borrow()
    }

    /// Every `(pid, signal)` delivered, in order. Reap tests assert the
    /// captured PID was signalled SIGTERM, then SIGKILL on survival.
    pub fn signals_sent(&self) -> Vec<(u32, i32)> {
        self.signals_sent.borrow().clone()
    }
}

impl ProcessProbe for MockProcessProbe {
    fn is_alive(&self, _pid: u32) -> bool {
        *self.calls.borrow_mut() += 1;
        let mut script = self.script.borrow_mut();
        match script.is_empty() {
            true => *self.last.borrow(),
            false => {
                let v = script.remove(0);
                *self.last.borrow_mut() = v;
                v
            }
        }
    }

    fn signal(&self, pid: u32, signal: i32) -> bool {
        // Record the delivery; liveness is driven independently by the
        // `start_time` sequence so a test can model "signal accepted but
        // process survives" vs "process dies after the signal".
        self.signals_sent.borrow_mut().push((pid, signal));
        true
    }

    fn start_time(&self, _pid: u32) -> Option<u64> {
        // Second observable channel, distinct from `is_alive`. The
        // trait-default `is_same_process` consumes this, so the reap
        // path's identity comparison is exercised for real; the mock
        // never reimplements the comparison.
        let mut script = self.start_time_script.borrow_mut();
        match script.is_empty() {
            true => *self.start_time_last.borrow(),
            false => {
                let v = script.remove(0);
                *self.start_time_last.borrow_mut() = v;
                v
            }
        }
    }
}
