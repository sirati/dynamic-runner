//! Single concern: filesystem cleanup at the end of the shutdown
//! sequence — `/tmp/asm-XXX` directory and the PID file.
//!
//! The tmp-prefix is removed via `podman unshare <rm>
//! <validated-abs-path> -rf` (no `--root`/`--runroot`). The path
//! is canonicalized AND validated by
//! [`crate::podman::validate_safe_tmp_path`] before any exec runs
//! (strictly under `/tmp/`, no `/home/`, character whitelist, no
//! symlink escape). Validation is the safety; the `podman unshare`
//! wrap is for permission.
//!
//! Two orthogonal failure modes shaped the current design:
//!
//!   1. `podman --root=X --runroot=Y unshare rm X -rf` (a70d3bf,
//!      62f3ffb) failed with `EBUSY: Device or resource busy` on
//!      `X/storage/overlay`. The unshare's storage driver,
//!      initialized via `--root=X`, holds an internal lock on its
//!      own root directory — a podman-internal busy state, not a
//!      kernel mount (asm-tokenizer 2026-05-18 12:20: `findmnt`
//!      showed zero kernel mounts).
//!
//!   2. Plain host `rm <path> -rf` (def6d7a) failed with EACCES
//!      on rootless-podman overlay content. The files themselves
//!      are kruppb-owned, but their parent directories follow the
//!      nix-store immutable pattern — mode `r-xr-xr-x`, no write
//!      bit. POSIX `unlinkat(2)` needs write permission on the
//!      *parent*, which host kruppb lacks. (asm-tokenizer
//!      2026-05-18 12:37: directly observed on libtsan.so under
//!      `<prefix>/storage/overlay/.../diff/nix/store/.../lib/`.)
//!
//! The current shape resolves both: `podman unshare` (without
//! `--root`/`--runroot`) gives kruppb uid-0 inside the user
//! namespace, which bypasses the dir-write-bit via
//! `CAP_DAC_OVERRIDE`; the absence of `--root`/`--runroot` means
//! no storage driver is initialized on the path being deleted,
//! so no busy-lock.
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
/// `preserve_scratch` reflects the reaper's "leave the orphan
/// inspectable" decision: when a known workload PID survived the reap
/// the podman handle is intentionally left intact (see
/// [`crate::poll_loop::signal_shutdown`]), and removing the scratch
/// tree out from under that still-live orphan would undercut the
/// inspectable intent and delete files the process may still hold open.
/// So when `preserve_scratch` is set the tmp tree is LEFT on disk; the
/// PID file is still removed unconditionally (the reaper process is
/// exiting, so its own pid-file is stale regardless).
///
/// `log` is invoked once per step with a human-readable message; main
/// wires it to `eprintln!` with the shared prefix, tests record
/// messages instead.
pub fn final_cleanup<B: PodmanBackend, L: FnMut(&str)>(
    backend: &B,
    tmp_prefix: &Path,
    pid_file: &Path,
    preserve_scratch: bool,
    log: L,
) {
    let mut log = log;
    match preserve_scratch {
        true => log(&format!(
            "orphan survived the reap; leaving scratch tree {} on disk for inspection",
            tmp_prefix.display()
        )),
        false => remove_tmp_prefix(backend, tmp_prefix, &mut log),
    }
    remove_pid_file(pid_file, &mut log);
}

/// `podman unshare <rm> <validated-abs-path> -rf` (no `--root`,
/// no `--runroot`). On failure the captured stderr (including
/// argv and exit status) is logged and the tree is left on disk
/// for operator inspection. Intentionally NO fallback — the only
/// path that gets touched is the one
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
        fn workload_pid(&self, _: &str) -> Option<u32> {
            unreachable!("cleanup tests don't capture the workload PID")
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

    /// When the reaper left a live orphan, `final_cleanup` must LEAVE
    /// the scratch tree on disk (so the orphan stays inspectable) while
    /// still removing the stale pid-file. `remove_tmp_tree` must NOT be
    /// called — the `unreachable!()` on the backend proves it.
    #[test]
    fn final_cleanup_preserves_scratch_when_orphan_survives() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("asm-xxx");
        fs::create_dir(&tmp).unwrap();
        let inner = tmp.join("file");
        fs::write(&inner, b"x").unwrap();
        let pid_path = dir.path().join("p.pid");
        write_pid_file(&pid_path).unwrap();
        // remove_tmp_tree must never be invoked; the bare new(true)
        // backend records calls but the preserve gate short-circuits
        // before reaching it.
        let backend = FakeBackend::new(true);
        let mut logs: Vec<String> = Vec::new();
        final_cleanup(&backend, &tmp, &pid_path, true, |m| logs.push(m.to_string()));
        assert!(tmp.exists(), "scratch tree must be preserved for an orphan");
        assert!(inner.exists(), "scratch contents must be preserved");
        assert!(
            backend.calls.borrow().is_empty(),
            "remove_tmp_tree must NOT run when scratch is preserved; calls: {:?}",
            backend.calls.borrow()
        );
        assert!(!pid_path.exists(), "stale pid-file is still removed");
        assert!(
            logs.iter().any(|l| l.contains("leaving scratch tree")),
            "logs must note the scratch tree was left for inspection: {:?}",
            logs
        );
    }

    /// On the normal (no-orphan) path `final_cleanup` removes both the
    /// scratch tree and the pid-file, exactly as before the gate.
    #[test]
    fn final_cleanup_removes_scratch_when_no_orphan() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("asm-xxx");
        fs::create_dir(&tmp).unwrap();
        let pid_path = dir.path().join("p.pid");
        write_pid_file(&pid_path).unwrap();
        let backend = FakeBackend::new(true);
        let mut logs: Vec<String> = Vec::new();
        final_cleanup(&backend, &tmp, &pid_path, false, |m| logs.push(m.to_string()));
        assert_eq!(
            backend.calls.borrow().len(),
            1,
            "remove_tmp_tree must run once on the no-orphan path; calls: {:?}",
            backend.calls.borrow()
        );
        assert!(
            backend.calls.borrow()[0].contains("remove_tmp_tree"),
            "calls: {:?}",
            backend.calls.borrow()
        );
        assert!(!pid_path.exists(), "pid-file removed on the no-orphan path");
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
