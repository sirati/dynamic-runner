//! Test doubles for the [`PodmanBackend`] trait and a flag-coupled
//! [`Clock`] fixture for the poll loop. Lives in the library crate so
//! both unit-tests (inside modules) and integration tests (in `tests/`)
//! can share one implementation — duplicating it would violate the
//! project's single-concern / no-duplication rules.
//!
//! The process-probe mock + the `MOCK_WORKLOAD_START` sentinel live in
//! the shared `dynrunner-reap` crate (so the wrapper and this manager
//! drive their reap tests with ONE mock); they are re-exported here so
//! the manager's tests keep importing `crate::testing::MockProcessProbe`.
//!
//! These types are present in release builds too, but LTO+strip
//! eliminates them from the production binary because `main.rs` never
//! references them.

use crate::clock::Clock;
use crate::podman::PodmanBackend;
use crate::shutdown_flag::ShutdownFlag;
use std::cell::RefCell;
use std::path::Path;
use std::time::Duration;

// Re-export the shared process-probe mock so the manager's tests keep
// their historical `crate::testing::{MockProcessProbe, MOCK_WORKLOAD_START}`
// import path after the lift to the reap crate.
pub use dynrunner_reap::testing::{MockProcessProbe, MOCK_WORKLOAD_START};

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
    conmon_pid_script: RefCell<Vec<Option<u32>>>,
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
    pub fn script_conmon_pid(&self, results: Vec<Option<u32>>) {
        *self.conmon_pid_script.borrow_mut() = results;
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
    fn conmon_pid(&self, name: &str) -> Option<u32> {
        let r = Self::pop_pgrep(&self.conmon_pid_script);
        self.record(format!("conmon_pid({}) -> {:?}", name, r));
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
