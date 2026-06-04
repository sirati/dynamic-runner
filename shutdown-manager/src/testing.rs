//! Test doubles for the [`PodmanBackend`] trait and the [`Clock`]
//! trait. Lives in the library crate so both unit-tests (inside
//! modules) and integration tests (in `tests/`) can share one
//! implementation — duplicating it would violate the project's
//! single-concern / no-duplication rules.
//!
//! These types are present in release builds too, but LTO+strip
//! eliminates them from the production binary because `main.rs` never
//! references them.

use crate::clock::Clock;
use crate::podman::PodmanBackend;
use crate::process_probe::ProcessProbe;
use crate::shutdown_flag::ShutdownFlag;
use std::cell::RefCell;
use std::path::Path;
use std::time::Duration;

/// Programmable backend. Each method has a *script* of return values
/// (consumed in order); past the end of the script the method returns
/// a safe default (false / None). Every call is recorded for later
/// assertion.
#[derive(Default)]
pub struct MockBackend {
    calls: RefCell<Vec<String>>,
    exists_script: RefCell<Vec<bool>>,
    pgrep_script: RefCell<Vec<Option<u32>>>,
    workload_pid_script: RefCell<Vec<Option<u32>>>,
    remove_script: RefCell<Vec<bool>>,
    rm_all_called: RefCell<bool>,
}

impl MockBackend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn script_exists(&self, results: Vec<bool>) {
        *self.exists_script.borrow_mut() = results;
    }
    pub fn script_pgrep(&self, results: Vec<Option<u32>>) {
        *self.pgrep_script.borrow_mut() = results;
    }
    pub fn script_workload_pid(&self, results: Vec<Option<u32>>) {
        *self.workload_pid_script.borrow_mut() = results;
    }
    pub fn script_remove(&self, results: Vec<bool>) {
        *self.remove_script.borrow_mut() = results;
    }

    pub fn calls(&self) -> Vec<String> {
        self.calls.borrow().clone()
    }
    pub fn rm_all_called(&self) -> bool {
        *self.rm_all_called.borrow()
    }

    fn record(&self, s: impl Into<String>) {
        self.calls.borrow_mut().push(s.into());
    }
    fn pop_bool(slot: &RefCell<Vec<bool>>) -> bool {
        let mut v = slot.borrow_mut();
        match v.is_empty() {
            true => false,
            false => v.remove(0),
        }
    }
    fn pop_pgrep(slot: &RefCell<Vec<Option<u32>>>) -> Option<u32> {
        let mut v = slot.borrow_mut();
        match v.is_empty() {
            true => None,
            false => v.remove(0),
        }
    }
}

impl PodmanBackend for MockBackend {
    fn container_exists(&self, name: &str) -> bool {
        let r = Self::pop_bool(&self.exists_script);
        self.record(format!("container_exists({}) -> {}", name, r));
        r
    }
    fn exec_signal(&self, name: &str, pid: u32, signal: &str) -> bool {
        self.record(format!("exec_signal({}, {}, {})", name, pid, signal));
        true
    }
    fn exec_pgrep_first_child(&self, name: &str) -> Option<u32> {
        let r = Self::pop_pgrep(&self.pgrep_script);
        self.record(format!("exec_pgrep_first_child({}) -> {:?}", name, r));
        r
    }
    fn workload_pid(&self, name: &str) -> Option<u32> {
        let r = Self::pop_pgrep(&self.workload_pid_script);
        self.record(format!("workload_pid({}) -> {:?}", name, r));
        r
    }
    fn kill_pid1(&self, name: &str, signal: &str) -> bool {
        self.record(format!("kill_pid1({}, {})", name, signal));
        true
    }
    fn stop(&self, name: &str, grace_secs: u32) -> bool {
        self.record(format!("stop({}, {})", name, grace_secs));
        true
    }
    fn rm_all(&self) -> bool {
        *self.rm_all_called.borrow_mut() = true;
        self.record("rm_all".to_string());
        true
    }
    fn remove_tmp_tree(&self, path: &Path) -> Result<(), String> {
        let r = Self::pop_bool(&self.remove_script);
        self.record(format!("remove_tmp_tree({}) -> {}", path.display(), r));
        match r {
            true => Ok(()),
            false => Err("mock-failure".to_string()),
        }
    }
}

/// Non-blocking clock. Records every sleep duration, optionally fires
/// a one-shot flag-setter on the Nth sleep so tests can simulate "a
/// signal arrives partway through the polling loop".
#[derive(Default)]
pub struct FakeClock {
    sleeps: RefCell<Vec<Duration>>,
    set_flag_on: RefCell<Option<(usize, ShutdownFlag)>>,
    count: RefCell<usize>,
}

impl FakeClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sleeps(&self) -> Vec<Duration> {
        self.sleeps.borrow().clone()
    }

    /// On the Nth call to `sleep` (1-based), set `flag`. Used to model
    /// SIGTERM/SIGCONT arrival without raising a real signal.
    pub fn set_on_sleep(&self, n: usize, flag: ShutdownFlag) {
        *self.set_flag_on.borrow_mut() = Some((n, flag));
    }
}

impl Clock for FakeClock {
    fn sleep(&self, dur: Duration) {
        self.sleeps.borrow_mut().push(dur);
        let mut n = self.count.borrow_mut();
        *n += 1;
        let trigger = self.set_flag_on.borrow().clone();
        match trigger {
            Some((target, flag)) if target == *n => flag.set_for_test(),
            _ => {}
        }
    }
}

/// Programmable [`ProcessProbe`]. Each `is_alive` call consumes one
/// scripted boolean; past the end of the script the probe sticks at
/// the final value (saturating) — this matches realistic semantics
/// (once the wrapper has died, it stays dead).
#[derive(Default)]
pub struct MockProcessProbe {
    script: RefCell<Vec<bool>>,
    /// Saturating value once `script` is drained. Defaults to the
    /// last popped value, falling back to `false` when nothing was
    /// ever scripted.
    last: RefCell<bool>,
    calls: RefCell<u32>,
    /// Every `(pid, signal)` delivered through `signal`, in order,
    /// so reap tests can assert "SIGTERM then SIGKILL to the captured
    /// PID" without a real PID space.
    signals_sent: RefCell<Vec<(u32, i32)>>,
    /// Scripted `start_time` returns, consumed in order (saturating at
    /// the final value once drained). This is the SECOND observable
    /// channel — distinct from `script`/`is_alive` (which feeds the
    /// wrapper-monitor) — and it drives the reap path's identity guard:
    /// the poll loop captures element 0 at PID-capture time, and the
    /// reap re-checks via the trait-default `is_same_process`, so a
    /// later element that differs from the captured value models a
    /// PID-reuse / process-gone event without a real `/proc`.
    start_time_script: RefCell<Vec<Option<u64>>>,
    /// Saturating value once `start_time_script` drains. Defaults to
    /// the last scripted value, falling back to `None`.
    start_time_last: RefCell<Option<u64>>,
}

/// Sentinel "this is still the original workload" start-time value used
/// by the reap-path mock constructors. Any value distinct from this one
/// (or `None`) models the process being gone or the PID reused.
pub const MOCK_WORKLOAD_START: u64 = 4242_4242;

impl MockProcessProbe {
    /// Construct a probe with a scripted sequence of `is_alive`
    /// returns (the wrapper-monitor channel). After the script is
    /// drained the most recent value is returned on every subsequent
    /// call. The `start_time` channel is left at its default (`None`,
    /// saturating) — `script` is for wrapper-monitor tests; reap tests
    /// use [`MockProcessProbe::reap`] / [`MockProcessProbe::reap_start_times`].
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

    /// Reap-path probe driven by a raw `start_time` script. The poll
    /// loop's capture call consumes element 0; each `is_same_process`
    /// re-check consumes the next. A later element differing from the
    /// captured value models kernel PID reuse (same PID, new process);
    /// `None` models the process having exited. Saturates at the final
    /// value once drained.
    pub fn reap_start_times(start_times: Vec<Option<u64>>) -> Self {
        let saturating = start_times.last().copied().unwrap_or(None);
        Self {
            start_time_script: RefCell::new(start_times),
            start_time_last: RefCell::new(saturating),
            ..Default::default()
        }
    }

    /// Probe that always reports the wrapper as alive — the test
    /// default for paths that do not exercise the wrapper-monitor
    /// branch. Its `start_time` channel also saturates at a fixed
    /// value, so on the reap path it reports the workload as the SAME
    /// live process forever (the "orphan never dies" case).
    pub fn always_alive() -> Self {
        Self {
            last: RefCell::new(true),
            start_time_last: RefCell::new(Some(MOCK_WORKLOAD_START)),
            ..Default::default()
        }
    }

    /// Probe that always reports the wrapper as dead — used to
    /// drive the SIGNAL_SHUTDOWN branch from the wrapper-monitor. Its
    /// `start_time` channel saturates at `None` (no process), the
    /// default.
    pub fn always_dead() -> Self {
        Self {
            last: RefCell::new(false),
            ..Default::default()
        }
    }

    /// Number of times the loop has asked us — useful for asserting
    /// the probe was consulted (or NOT consulted, in the `pid=None`
    /// inertness test).
    pub fn calls(&self) -> u32 {
        *self.calls.borrow()
    }

    /// Every `(pid, signal)` delivered, in order. Reap tests assert
    /// the captured PID was signalled SIGTERM, then SIGKILL on
    /// survival.
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
        // `script`/`last` sequence so a test can model "signal accepted
        // but process survives" vs "process dies after the signal".
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
