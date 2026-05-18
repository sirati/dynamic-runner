//! Single concern: filesystem cleanup at the end of the shutdown
//! sequence — `/tmp/asm-XXX` directory and the PID file.
//!
//! The `/tmp` removal is non-trivial: rootless podman places its
//! layer/storage directories under `<tmp_prefix>/storage` with
//! subuid-owned files that the host UID cannot unlink directly. The
//! mechanism is `podman ... unshare`, which re-enters the user
//! namespace where those subuids are local and readable.
//!
//! WALK DESIGN: cleanup owns the order; backend owns the per-entry
//! primitive. There is no `rm -rf`-equivalent primitive (neither
//! podman-side `rm -rf` nor host-side `fs::remove_dir_all`). The
//! four-stage walk is:
//!
//!   1. `unshare_find_files(root)` → list every file under root.
//!   2. for each: `unshare_unlink(file)` — one at a time.
//!   3. `unshare_find_dirs(root)` → list every dir under root,
//!      leaf-first (find `-depth`), INCLUDING root itself.
//!   4. for each: `unshare_rmdir(dir)` — one at a time.
//!
//! Single-entry-per-op is load-bearing: a recursive primitive (with
//! `-r`/`-rf` or `remove_dir_all`) can cascade-delete an unintended
//! subtree if path computation is wrong; the per-entry walk fails
//! loudly (or harmlessly) on a path that doesn't exist instead.
//! There is NO host-side fallback — if `podman unshare` cannot enter
//! the userns, cleanup logs the specific failure and exits leaving
//! the tree for an operator to inspect. Losing `/tmp` debris is
//! strictly less bad than `rm -rf`-ing the wrong path.

use crate::podman::PodmanBackend;
use std::fs;
use std::io;
use std::path::Path;

/// Run the full filesystem-cleanup phase.
///
/// `log` is invoked once per step with a human-readable message; main
/// wires it to `eprintln!` with the shared prefix, tests record
/// messages instead.
pub fn final_cleanup<B: PodmanBackend, L: FnMut(&str)>(
    backend: &B,
    tmp_prefix: &Path,
    pid_file: &Path,
    log: L,
) {
    let mut log = log;
    remove_tmp_prefix(backend, tmp_prefix, &mut log);
    remove_pid_file(pid_file, &mut log);
}

/// Four-stage walk: list files via `podman unshare find -type f`,
/// unlink each via `podman unshare rm`, list dirs leaf-first via
/// `podman unshare find -depth -type d`, rmdir each via `podman
/// unshare rmdir`. Per-entry failures are logged and the walk
/// continues (so a single stuck inode does not block the rest of
/// the prefix's teardown). Enumeration failures (stage 1 or 3)
/// abort cleanup with a log line; there is intentionally no
/// host-side fallback.
fn remove_tmp_prefix<B: PodmanBackend, L: FnMut(&str)>(
    backend: &B,
    tmp_prefix: &Path,
    log: &mut L,
) {
    match tmp_prefix.exists() {
        false => {
            log(&format!(
                "tmp-prefix already gone: {}",
                tmp_prefix.display()
            ));
            return;
        }
        true => {}
    }

    // Stage 1: enumerate files under root.
    let files = match backend.unshare_find_files(tmp_prefix) {
        Ok(v) => v,
        Err(stderr) => {
            log(&format!(
                "podman unshare find -type f failed; cleanup aborted (no host-side fallback by design). stderr: {}",
                stderr
            ));
            return;
        }
    };
    log(&format!(
        "planning per-file unlink: {} files under {}",
        files.len(),
        tmp_prefix.display()
    ));

    // Stage 2: unlink each file one at a time, continuing past
    // per-entry failures (one stuck inode must not block the rest).
    let mut files_failed: u32 = 0;
    for f in &files {
        match backend.unshare_unlink(f) {
            Ok(()) => {}
            Err(stderr) => {
                files_failed += 1;
                log(&format!("unlink failed for {}: {}", f.display(), stderr));
            }
        }
    }

    // Stage 3: enumerate dirs leaf-first. `find -depth` ensures
    // children precede parents — required for rmdir's emptiness
    // contract. The list includes `tmp_prefix` itself.
    let dirs = match backend.unshare_find_dirs(tmp_prefix) {
        Ok(v) => v,
        Err(stderr) => {
            log(&format!(
                "podman unshare find -type d failed; rmdir phase skipped. stderr: {}",
                stderr
            ));
            return;
        }
    };
    log(&format!(
        "planning per-dir rmdir: {} dirs under {} (leaf-first)",
        dirs.len(),
        tmp_prefix.display()
    ));

    // Stage 4: rmdir each dir one at a time.
    let mut dirs_failed: u32 = 0;
    for d in &dirs {
        match backend.unshare_rmdir(d) {
            Ok(()) => {}
            Err(stderr) => {
                dirs_failed += 1;
                log(&format!("rmdir failed for {}: {}", d.display(), stderr));
            }
        }
    }

    log(&format!(
        "cleanup walk complete: attempted {} unlink and {} rmdir ops",
        files.len(),
        dirs.len()
    ));
    match (files_failed, dirs_failed) {
        (0, 0) => {}
        (f, d) => log(&format!(
            "WARNING: {} file-unlink and {} dir-rmdir failures — tmp-prefix may not be fully removed",
            f, d
        )),
    }
}

/// Unlink the PID file, ignoring missing-file errors.
fn remove_pid_file<L: FnMut(&str)>(pid_file: &Path, log: &mut L) {
    match fs::remove_file(pid_file) {
        Ok(()) => log(&format!("pid-file removed: {}", pid_file.display())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            log(&format!("pid-file already gone: {}", pid_file.display()));
        }
        Err(e) => log(&format!(
            "pid-file unlink failed for {}: {}",
            pid_file.display(),
            e
        )),
    }
}

/// Write our own PID to `pid_file` at startup. Errors are returned so
/// main can decide whether to proceed (it does — losing the PID file
/// is not fatal, but worth logging).
pub fn write_pid_file(pid_file: &Path) -> io::Result<()> {
    let pid = std::process::id();
    fs::write(pid_file, format!("{}\n", pid))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::path::PathBuf;

    /// Programmable backend for the cleanup walk. Each of the four
    /// new primitives consumes one slot from its scripted queue;
    /// past the queue the primitive returns `Ok(empty)` /`Ok(())`
    /// as a safe default. Every call is recorded in `calls` for
    /// post-test inspection.
    struct FakeBackend {
        find_files: RefCell<Vec<Result<Vec<PathBuf>, String>>>,
        find_dirs: RefCell<Vec<Result<Vec<PathBuf>, String>>>,
        unlink_results: RefCell<Vec<Result<(), String>>>,
        rmdir_results: RefCell<Vec<Result<(), String>>>,
        calls: RefCell<Vec<String>>,
    }

    impl FakeBackend {
        fn new() -> Self {
            Self {
                find_files: RefCell::new(Vec::new()),
                find_dirs: RefCell::new(Vec::new()),
                unlink_results: RefCell::new(Vec::new()),
                rmdir_results: RefCell::new(Vec::new()),
                calls: RefCell::new(Vec::new()),
            }
        }
        fn script_find_files(mut self, r: Result<Vec<PathBuf>, String>) -> Self {
            self.find_files.get_mut().push(r);
            self
        }
        fn script_find_dirs(mut self, r: Result<Vec<PathBuf>, String>) -> Self {
            self.find_dirs.get_mut().push(r);
            self
        }
        fn script_unlinks(mut self, results: Vec<Result<(), String>>) -> Self {
            *self.unlink_results.get_mut() = results;
            self
        }
        fn script_rmdirs(mut self, results: Vec<Result<(), String>>) -> Self {
            *self.rmdir_results.get_mut() = results;
            self
        }
        fn pop_find(slot: &RefCell<Vec<Result<Vec<PathBuf>, String>>>) -> Result<Vec<PathBuf>, String> {
            let mut v = slot.borrow_mut();
            match v.is_empty() {
                true => Ok(Vec::new()),
                false => v.remove(0),
            }
        }
        fn pop_unit(slot: &RefCell<Vec<Result<(), String>>>) -> Result<(), String> {
            let mut v = slot.borrow_mut();
            match v.is_empty() {
                true => Ok(()),
                false => v.remove(0),
            }
        }
    }

    impl PodmanBackend for FakeBackend {
        fn container_exists(&self, _name: &str) -> bool {
            unreachable!("cleanup tests don't poll container_exists")
        }
        fn exec_signal(&self, _: &str, _: u32, _: &str) -> bool {
            unreachable!()
        }
        fn exec_pgrep_first_child(&self, _: &str) -> Option<u32> {
            unreachable!()
        }
        fn kill_pid1(&self, _: &str, _: &str) -> bool {
            unreachable!()
        }
        fn stop(&self, _: &str, _: u32) -> bool {
            unreachable!()
        }
        fn rm_all(&self) -> bool {
            unreachable!()
        }
        fn unshare_find_files(&self, root: &Path) -> Result<Vec<PathBuf>, String> {
            self.calls
                .borrow_mut()
                .push(format!("unshare_find_files({})", root.display()));
            Self::pop_find(&self.find_files)
        }
        fn unshare_find_dirs(&self, root: &Path) -> Result<Vec<PathBuf>, String> {
            self.calls
                .borrow_mut()
                .push(format!("unshare_find_dirs({})", root.display()));
            Self::pop_find(&self.find_dirs)
        }
        fn unshare_unlink(&self, file: &Path) -> Result<(), String> {
            self.calls
                .borrow_mut()
                .push(format!("unshare_unlink({})", file.display()));
            Self::pop_unit(&self.unlink_results)
        }
        fn unshare_rmdir(&self, dir: &Path) -> Result<(), String> {
            self.calls
                .borrow_mut()
                .push(format!("unshare_rmdir({})", dir.display()));
            Self::pop_unit(&self.rmdir_results)
        }
    }

    #[test]
    fn pid_file_write_and_remove_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("p.pid");
        write_pid_file(&pid_path).unwrap();
        assert!(pid_path.exists());
        let mut logs: Vec<String> = Vec::new();
        remove_pid_file(&pid_path, &mut |m| logs.push(m.to_string()));
        assert!(!pid_path.exists());
        assert!(
            logs.iter().any(|l| l.contains("pid-file removed")),
            "logs: {:?}",
            logs
        );
    }

    #[test]
    fn pid_file_missing_is_silent_success() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("nope.pid");
        let mut logs: Vec<String> = Vec::new();
        remove_pid_file(&pid_path, &mut |m| logs.push(m.to_string()));
        assert!(
            logs.iter().any(|l| l.contains("already gone")),
            "logs: {:?}",
            logs
        );
    }

    #[test]
    fn tmp_prefix_absent_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("does-not-exist");
        let backend = FakeBackend::new();
        let mut logs: Vec<String> = Vec::new();
        remove_tmp_prefix(&backend, &tmp, &mut |m| logs.push(m.to_string()));
        assert!(
            logs.iter().any(|l| l.contains("already gone")),
            "logs: {:?}",
            logs
        );
        assert!(
            backend.calls.borrow().is_empty(),
            "backend must not be called when prefix is absent; calls: {:?}",
            backend.calls.borrow()
        );
    }

    /// Happy-path walk: two files, then the leaf subdir and the
    /// tmp_prefix itself. Asserts the order is files-first, dirs
    /// leaf-first, and that the backend received exactly the
    /// expected per-entry primitives in order.
    #[test]
    fn tmp_prefix_present_walks_per_file() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("asm-xxx");
        fs::create_dir(&tmp).unwrap();
        let file1 = tmp.join("file1");
        let file2 = tmp.join("file2");
        let subdir = tmp.join("sub");
        let backend = FakeBackend::new()
            .script_find_files(Ok(vec![file1.clone(), file2.clone()]))
            .script_find_dirs(Ok(vec![subdir.clone(), tmp.clone()]));
        let mut logs: Vec<String> = Vec::new();
        remove_tmp_prefix(&backend, &tmp, &mut |m| logs.push(m.to_string()));

        let calls = backend.calls.borrow();
        let expected: Vec<String> = vec![
            format!("unshare_find_files({})", tmp.display()),
            format!("unshare_unlink({})", file1.display()),
            format!("unshare_unlink({})", file2.display()),
            format!("unshare_find_dirs({})", tmp.display()),
            format!("unshare_rmdir({})", subdir.display()),
            format!("unshare_rmdir({})", tmp.display()),
        ];
        assert_eq!(
            *calls, expected,
            "per-entry walk call sequence mismatch"
        );
        assert!(
            logs.iter().any(|l| l.contains("planning per-file unlink: 2 files")),
            "logs: {:?}",
            logs
        );
        assert!(
            logs.iter().any(|l| l.contains("planning per-dir rmdir: 2 dirs")),
            "logs: {:?}",
            logs
        );
        assert!(
            logs.iter().any(|l| l.contains("cleanup walk complete")),
            "logs: {:?}",
            logs
        );
    }

    /// Stage-1 enumeration failure: cleanup aborts before any
    /// unlink or rmdir is attempted; the stderr is captured into
    /// the log so the operator can diagnose the userns-entry
    /// failure. NO host-side fallback fires (by design — the
    /// previous `fs::remove_dir_all` was removed because it is the
    /// same recursive-remove anti-pattern as `rm -rf`).
    #[test]
    fn find_files_failure_aborts_walk() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("asm-xxx");
        fs::create_dir(&tmp).unwrap();
        let backend =
            FakeBackend::new().script_find_files(Err("subuid mapping not found".to_string()));
        let mut logs: Vec<String> = Vec::new();
        remove_tmp_prefix(&backend, &tmp, &mut |m| logs.push(m.to_string()));

        let calls = backend.calls.borrow();
        assert_eq!(
            calls.len(),
            1,
            "only the find-files probe must run; got: {:?}",
            calls
        );
        assert!(
            calls[0].starts_with("unshare_find_files("),
            "calls: {:?}",
            calls
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("unshare_unlink(")),
            "no unlink ops on enumeration failure; calls: {:?}",
            calls
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("unshare_rmdir(")),
            "no rmdir ops on enumeration failure; calls: {:?}",
            calls
        );
        assert!(
            logs.iter()
                .any(|l| l.contains("podman unshare find -type f failed")),
            "logs: {:?}",
            logs
        );
        // tmp_prefix is still on-disk; the walk left it intact (no
        // host-side fallback). This is the load-bearing assertion:
        // a recursive-remove fallback would have wiped it.
        assert!(
            tmp.exists(),
            "no host-side fallback: tmp-prefix must remain on disk on enumeration failure"
        );
    }

    /// Enumeration failure logs the captured stderr verbatim into
    /// the diagnostic line (operator-facing).
    #[test]
    fn unshare_failure_logs_stderr_in_message() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("asm-xxx");
        fs::create_dir(&tmp).unwrap();
        let backend = FakeBackend::new().script_find_files(Err(
            "subuid mapping not found in /etc/subuid".to_string(),
        ));
        let mut logs: Vec<String> = Vec::new();
        remove_tmp_prefix(&backend, &tmp, &mut |m| logs.push(m.to_string()));
        assert!(
            logs.iter().any(|l| l.contains(
                "stderr: subuid mapping not found in /etc/subuid"
            )),
            "logs: {:?}",
            logs
        );
    }

    /// A per-file unlink failure must NOT short-circuit the walk:
    /// the remaining files are still attempted and the dir-rmdir
    /// phase still runs. The end-of-walk WARNING summary mentions
    /// the failed file count.
    #[test]
    fn per_file_unlink_failure_continues_walk() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("asm-xxx");
        fs::create_dir(&tmp).unwrap();
        let f1 = tmp.join("f1");
        let f2 = tmp.join("f2");
        let f3 = tmp.join("f3");
        let backend = FakeBackend::new()
            .script_find_files(Ok(vec![f1.clone(), f2.clone(), f3.clone()]))
            // Middle file fails; first and third succeed.
            .script_unlinks(vec![
                Ok(()),
                Err("EBUSY: file is open".to_string()),
                Ok(()),
            ])
            .script_find_dirs(Ok(vec![tmp.clone()]));
        let mut logs: Vec<String> = Vec::new();
        remove_tmp_prefix(&backend, &tmp, &mut |m| logs.push(m.to_string()));

        let calls = backend.calls.borrow();
        // All three unlinks were attempted (per-file failure does
        // not abort the walk).
        let unlink_calls: Vec<&String> = calls
            .iter()
            .filter(|c| c.starts_with("unshare_unlink("))
            .collect();
        assert_eq!(
            unlink_calls.len(),
            3,
            "all three unlinks must be attempted; calls: {:?}",
            calls
        );
        // The rmdir phase still ran.
        assert!(
            calls.iter().any(|c| c.starts_with("unshare_rmdir(")),
            "rmdir phase must run after per-file failure; calls: {:?}",
            calls
        );
        // Failure was logged.
        assert!(
            logs.iter()
                .any(|l| l.contains("unlink failed for") && l.contains("EBUSY")),
            "logs: {:?}",
            logs
        );
        // End-of-walk summary mentions failure count.
        assert!(
            logs.iter()
                .any(|l| l.contains("WARNING: 1 file-unlink and 0 dir-rmdir failures")),
            "logs: {:?}",
            logs
        );
    }

    /// Symmetric: per-dir rmdir failure logs the specific
    /// diagnostic but does not abort the rest of the rmdir phase.
    /// Typical real-world cause is ENOTEMPTY (another process
    /// re-populated the dir between stage-3 enumeration and stage-4
    /// rmdir, or the planner's stage-2 left an unlink failure
    /// behind).
    #[test]
    fn rmdir_phase_continues_on_per_dir_failure() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("asm-xxx");
        fs::create_dir(&tmp).unwrap();
        let d1 = tmp.join("d1");
        let d2 = tmp.join("d2");
        let backend = FakeBackend::new()
            .script_find_files(Ok(Vec::new()))
            .script_find_dirs(Ok(vec![d1.clone(), d2.clone(), tmp.clone()]))
            // Middle dir fails; first and root succeed.
            .script_rmdirs(vec![
                Ok(()),
                Err("ENOTEMPTY".to_string()),
                Ok(()),
            ]);
        let mut logs: Vec<String> = Vec::new();
        remove_tmp_prefix(&backend, &tmp, &mut |m| logs.push(m.to_string()));

        let calls = backend.calls.borrow();
        let rmdir_calls: Vec<&String> = calls
            .iter()
            .filter(|c| c.starts_with("unshare_rmdir("))
            .collect();
        assert_eq!(
            rmdir_calls.len(),
            3,
            "all three rmdirs must be attempted; calls: {:?}",
            calls
        );
        assert!(
            logs.iter()
                .any(|l| l.contains("rmdir failed for") && l.contains("ENOTEMPTY")),
            "logs: {:?}",
            logs
        );
        assert!(
            logs.iter()
                .any(|l| l.contains("WARNING: 0 file-unlink and 1 dir-rmdir failures")),
            "logs: {:?}",
            logs
        );
    }

    /// Stage-3 enumeration failure (after stage 1+2 ran fine):
    /// the per-file unlinks already happened, but the rmdir phase
    /// is skipped and the failure is logged. No host-side
    /// fallback fires.
    #[test]
    fn find_dirs_failure_skips_rmdir_phase() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("asm-xxx");
        fs::create_dir(&tmp).unwrap();
        let f1 = tmp.join("f1");
        let backend = FakeBackend::new()
            .script_find_files(Ok(vec![f1.clone()]))
            .script_find_dirs(Err("podman unshare exec'd into bad userns".to_string()));
        let mut logs: Vec<String> = Vec::new();
        remove_tmp_prefix(&backend, &tmp, &mut |m| logs.push(m.to_string()));

        let calls = backend.calls.borrow();
        // Stage 1+2 ran (find_files + one unlink), stage 3 ran
        // (find_dirs returned Err), stage 4 was skipped.
        assert!(
            calls.iter().any(|c| c.starts_with("unshare_unlink(")),
            "stage-2 unlink must have run; calls: {:?}",
            calls
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("unshare_rmdir(")),
            "stage-4 rmdir must NOT run on stage-3 enumeration failure; calls: {:?}",
            calls
        );
        assert!(
            logs.iter()
                .any(|l| l.contains("podman unshare find -type d failed")),
            "logs: {:?}",
            logs
        );
    }
}
