//! Single concern: filesystem cleanup at the end of the shutdown
//! sequence — `/tmp/asm-XXX` directory and the PID file.
//!
//! The `/tmp` removal is non-trivial: rootless podman places its
//! layer/storage directories under `<tmp_prefix>/storage` with
//! subuid-owned files that the host UID cannot unlink directly. The
//! correct primitive is `podman unshare rm -rf <prefix>`, which
//! re-enters the user namespace where those subuids are local. If
//! podman itself is gone or the unshare fails, we fall back to plain
//! `rm -rf` — that succeeds only for the no-subuid case (e.g. the
//! container never started), but losing some `/tmp` debris is not
//! worth crashing the shutdown manager.

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

/// `podman unshare rm -rf` with a plain-`rm -rf` fallback.
///
/// The plain fallback is a separate function so it can be reused by
/// tests that don't have a real podman, and so its behaviour is one
/// concern at a time.
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
                "removing tmp-prefix via podman unshare: {}",
                tmp_prefix.display()
            ));
            match backend.unshare_remove(tmp_prefix) {
                true => log("tmp-prefix removed via unshare"),
                false => {
                    log("podman unshare rm failed; falling back to host rm -rf");
                    fallback_remove(tmp_prefix, log);
                }
            }
        }
    }
}

/// Host-UID `rm -rf` via std::fs::remove_dir_all (no spawn, smaller
/// binary). Best-effort: failures are logged, not propagated.
fn fallback_remove<L: FnMut(&str)>(path: &Path, log: &mut L) {
    let res = match path.is_dir() {
        true => fs::remove_dir_all(path),
        false => fs::remove_file(path),
    };
    match res {
        Ok(()) => log(&format!("fallback removal succeeded: {}", path.display())),
        Err(e) => log(&format!(
            "fallback removal failed for {}: {}",
            path.display(),
            e
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

    struct FakeBackend {
        unshare_ok: bool,
        calls: RefCell<Vec<String>>,
    }

    impl FakeBackend {
        fn new(unshare_ok: bool) -> Self {
            Self {
                unshare_ok,
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
        fn unshare_remove(&self, p: &Path) -> bool {
            self.calls
                .borrow_mut()
                .push(format!("unshare_remove({})", p.display()));
            self.unshare_ok
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
    fn tmp_prefix_present_invokes_unshare() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("asm-xxx");
        fs::create_dir(&tmp).unwrap();
        let backend = FakeBackend::new(true);
        let mut logs: Vec<String> = Vec::new();
        remove_tmp_prefix(&backend, &tmp, &mut |m| logs.push(m.to_string()));
        assert_eq!(
            backend.calls.borrow().len(),
            1,
            "expected one unshare call, got {:?}",
            backend.calls.borrow()
        );
        assert!(
            backend.calls.borrow()[0].contains("unshare_remove"),
            "calls: {:?}",
            backend.calls.borrow()
        );
        assert!(
            logs.iter().any(|l| l.contains("removed via unshare")),
            "logs: {:?}",
            logs
        );
    }

    #[test]
    fn unshare_failure_triggers_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("asm-xxx");
        fs::create_dir(&tmp).unwrap();
        let inner = tmp.join("file");
        fs::write(&inner, b"x").unwrap();
        let backend = FakeBackend::new(false); // unshare returns false
        let mut logs: Vec<String> = Vec::new();
        remove_tmp_prefix(&backend, &tmp, &mut |m| logs.push(m.to_string()));
        // The fallback host-rm should succeed because we own everything.
        assert!(!tmp.exists(), "fallback should have removed tmp");
        assert!(
            logs.iter().any(|l| l.contains("fallback removal succeeded")),
            "logs: {:?}",
            logs
        );
    }
}
