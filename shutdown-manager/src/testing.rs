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
use std::path::{Path, PathBuf};
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
    find_files_script: RefCell<Vec<Result<Vec<PathBuf>, String>>>,
    find_dirs_script: RefCell<Vec<Result<Vec<PathBuf>, String>>>,
    unlink_script: RefCell<Vec<Result<(), String>>>,
    rmdir_script: RefCell<Vec<Result<(), String>>>,
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

    /// Scripted return values for `unshare_find_files`. Each call
    /// consumes one slot; past the script the method returns
    /// `Ok(Vec::new())` (empty enumeration) as a safe default.
    pub fn script_find_files(&self, results: Vec<Result<Vec<PathBuf>, String>>) {
        *self.find_files_script.borrow_mut() = results;
    }
    /// Scripted return values for `unshare_find_dirs`. Same shape as
    /// `script_find_files`.
    pub fn script_find_dirs(&self, results: Vec<Result<Vec<PathBuf>, String>>) {
        *self.find_dirs_script.borrow_mut() = results;
    }
    /// Scripted return values for `unshare_unlink` — one per file the
    /// cleanup walk attempts to unlink. Past the script the method
    /// returns `Ok(())` (best-case default).
    pub fn script_unlink(&self, results: Vec<Result<(), String>>) {
        *self.unlink_script.borrow_mut() = results;
    }
    /// Scripted return values for `unshare_rmdir` — one per directory
    /// the walk attempts to rmdir.
    pub fn script_rmdir(&self, results: Vec<Result<(), String>>) {
        *self.rmdir_script.borrow_mut() = results;
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
    /// Pop next find-result; default to `Ok(empty)` so tests that
    /// don't script enumeration get a no-op walk.
    fn pop_find(
        slot: &RefCell<Vec<Result<Vec<PathBuf>, String>>>,
    ) -> Result<Vec<PathBuf>, String> {
        let mut v = slot.borrow_mut();
        match v.is_empty() {
            true => Ok(Vec::new()),
            false => v.remove(0),
        }
    }
    /// Pop next unlink/rmdir result; default `Ok(())`.
    fn pop_unit(slot: &RefCell<Vec<Result<(), String>>>) -> Result<(), String> {
        let mut v = slot.borrow_mut();
        match v.is_empty() {
            true => Ok(()),
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
    fn unshare_find_files(&self, root: &Path) -> Result<Vec<PathBuf>, String> {
        let r = Self::pop_find(&self.find_files_script);
        let summary = match &r {
            Ok(v) => format!("Ok({} entries)", v.len()),
            Err(e) => format!("Err({})", e),
        };
        self.record(format!(
            "unshare_find_files({}) -> {}",
            root.display(),
            summary
        ));
        r
    }
    fn unshare_find_dirs(&self, root: &Path) -> Result<Vec<PathBuf>, String> {
        let r = Self::pop_find(&self.find_dirs_script);
        let summary = match &r {
            Ok(v) => format!("Ok({} entries)", v.len()),
            Err(e) => format!("Err({})", e),
        };
        self.record(format!(
            "unshare_find_dirs({}) -> {}",
            root.display(),
            summary
        ));
        r
    }
    fn unshare_unlink(&self, file: &Path) -> Result<(), String> {
        let r = Self::pop_unit(&self.unlink_script);
        let summary = match &r {
            Ok(()) => "Ok".to_string(),
            Err(e) => format!("Err({})", e),
        };
        self.record(format!("unshare_unlink({}) -> {}", file.display(), summary));
        r
    }
    fn unshare_rmdir(&self, dir: &Path) -> Result<(), String> {
        let r = Self::pop_unit(&self.rmdir_script);
        let summary = match &r {
            Ok(()) => "Ok".to_string(),
            Err(e) => format!("Err({})", e),
        };
        self.record(format!("unshare_rmdir({}) -> {}", dir.display(), summary));
        r
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
}

impl MockProcessProbe {
    /// Construct a probe with a scripted sequence of `is_alive`
    /// returns. After the script is drained the most recent value
    /// is returned on every subsequent call.
    pub fn script(values: Vec<bool>) -> Self {
        let saturating = values.last().copied().unwrap_or(false);
        Self {
            script: RefCell::new(values),
            last: RefCell::new(saturating),
            calls: RefCell::new(0),
        }
    }

    /// Probe that always reports the wrapper as alive — the test
    /// default for paths that do not exercise the wrapper-monitor
    /// branch.
    pub fn always_alive() -> Self {
        Self {
            script: RefCell::new(Vec::new()),
            last: RefCell::new(true),
            calls: RefCell::new(0),
        }
    }

    /// Probe that always reports the wrapper as dead — used to
    /// drive the SIGNAL_SHUTDOWN branch from the wrapper-monitor.
    pub fn always_dead() -> Self {
        Self {
            script: RefCell::new(Vec::new()),
            last: RefCell::new(false),
            calls: RefCell::new(0),
        }
    }

    /// Number of times the loop has asked us — useful for asserting
    /// the probe was consulted (or NOT consulted, in the `pid=None`
    /// inertness test).
    pub fn calls(&self) -> u32 {
        *self.calls.borrow()
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
}
