//! Single concern: filesystem cleanup at the end of the shutdown
//! sequence — `/tmp/asm-XXX` directory and the PID file.
//!
//! The tmp-prefix is removed via host `<rm> <validated-abs-path>
//! -rf`. The path is canonicalized AND validated by
//! [`crate::podman::validate_safe_tmp_path`] before any exec runs
//! (strictly under `/tmp/`, no `/home/`, character whitelist, no
//! symlink escape). Validation is the safety; no `podman unshare`
//! wrapping is involved.
//!
//! The previous `podman unshare`-wrapped invocation failed with
//! `EBUSY` on `<prefix>/storage/overlay` because the unshare's
//! storage driver (initialized via `--root=<prefix>/storage`) held
//! an internal lock on its own root dir — a podman-internal busy
//! state, not a kernel mount. Empirically (asm-tokenizer
//! 2026-05-18 12:20): `findmnt` shows zero kernel mounts under
//! the cleanup target; `ls -la` shows only kruppb-owned regular
//! dirs that the rootless-podman storage driver pre-creates as
//! metadata. Host rm clears the tree instantly. The argv-shape
//! validation gate is what protects against catastrophic path
//! bugs, not the absent unshare wrapping.
//!
//! On rm failure the manager logs the captured stderr and leaves
//! the tree on disk for operator inspection. No fallback — losing
//! `/tmp/asm-*` debris is strictly less bad than a recursive
//! removal whose target the validation could not vet.

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

/// Host `<rm> <validated-abs-path> -rf`. On failure the captured
/// stderr (including argv and exit status) is logged and the tree
/// is left on disk for operator inspection. Intentionally NO
/// fallback — the only path that gets touched is the one
/// [`crate::podman::validate_safe_tmp_path`] approved.
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
        }
        true => {
            log(&format!(
                "removing tmp-prefix: {}",
                tmp_prefix.display()
            ));
            match backend.remove_tmp_tree(tmp_prefix) {
                Ok(()) => log("tmp-prefix removed"),
                Err(stderr) => log(&format!(
                    "rm failed; tmp-prefix left on disk for operator inspection. stderr: {}",
                    stderr
                )),
            }
        }
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

    struct FakeBackend {
        remove_result: Result<(), String>,
        calls: RefCell<Vec<String>>,
    }

    impl FakeBackend {
        fn new(remove_ok: bool) -> Self {
            Self {
                remove_result: match remove_ok {
                    true => Ok(()),
                    false => Err("mock-failure".to_string()),
                },
                calls: RefCell::new(Vec::new()),
            }
        }

        fn with_stderr(stderr: &str) -> Self {
            Self {
                remove_result: Err(stderr.to_string()),
                calls: RefCell::new(Vec::new()),
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
        fn remove_tmp_tree(&self, p: &Path) -> Result<(), String> {
            self.calls
                .borrow_mut()
                .push(format!("remove_tmp_tree({})", p.display()));
            self.remove_result.clone()
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
        let backend = FakeBackend::new(true);
        let mut logs: Vec<String> = Vec::new();
        remove_tmp_prefix(&backend, &tmp, &mut |m| logs.push(m.to_string()));
        assert!(
            logs.iter().any(|l| l.contains("already gone")),
            "logs: {:?}",
            logs
        );
        assert!(
            backend.calls.borrow().is_empty(),
            "backend should not be called when prefix is absent"
        );
    }

    #[test]
    fn tmp_prefix_present_invokes_remove() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("asm-xxx");
        fs::create_dir(&tmp).unwrap();
        let backend = FakeBackend::new(true);
        let mut logs: Vec<String> = Vec::new();
        remove_tmp_prefix(&backend, &tmp, &mut |m| logs.push(m.to_string()));
        assert_eq!(
            backend.calls.borrow().len(),
            1,
            "expected one remove_tmp_tree call, got {:?}",
            backend.calls.borrow()
        );
        assert!(
            backend.calls.borrow()[0].contains("remove_tmp_tree"),
            "calls: {:?}",
            backend.calls.borrow()
        );
        assert!(
            logs.iter().any(|l| l.contains("tmp-prefix removed")),
            "logs: {:?}",
            logs
        );
    }

    #[test]
    fn remove_failure_logs_stderr_in_message() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("asm-xxx");
        fs::create_dir(&tmp).unwrap();
        let backend = FakeBackend::with_stderr("subuid mapping not found");
        let mut logs: Vec<String> = Vec::new();
        remove_tmp_prefix(&backend, &tmp, &mut |m| logs.push(m.to_string()));
        assert!(
            logs.iter()
                .any(|l| l.contains("stderr: subuid mapping not found")),
            "logs: {:?}",
            logs
        );
    }

    /// On `remove_tmp_tree` failure the tree is intentionally left
    /// on disk — no host-side fallback. The log line captures the
    /// stderr AND states that the tree was left in place, so the
    /// operator knows where to look.
    #[test]
    fn remove_failure_leaves_tree_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("asm-xxx");
        fs::create_dir(&tmp).unwrap();
        let inner = tmp.join("file");
        fs::write(&inner, b"x").unwrap();
        let backend = FakeBackend::new(false); // remove_tmp_tree returns Err
        let mut logs: Vec<String> = Vec::new();
        remove_tmp_prefix(&backend, &tmp, &mut |m| logs.push(m.to_string()));
        // Tree must STILL be on disk: the load-bearing safety
        // property after dropping the host-fallback. A
        // host-recursive remove can never improve on a userns-aware
        // invocation; leaving debris is strictly safer.
        assert!(
            tmp.exists(),
            "tmp-prefix must remain on disk when rm fails; no fallback"
        );
        assert!(
            inner.exists(),
            "inner file must still be present; no host fallback ran"
        );
        assert!(
            logs.iter()
                .any(|l| l.contains("tmp-prefix left on disk for operator inspection")),
            "logs: {:?}",
            logs
        );
    }
}
