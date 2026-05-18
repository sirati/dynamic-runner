//! Single concern: invoking the `podman` CLI behind a trait so the
//! state machine in `poll_loop` is generic over the backend.
//!
//! All real-world invocations go through the same root/runroot/
//! cgroup-manager prefix; the production impl owns that prefix once.
//! Tests use [`MockBackend`] in `tests/common`, never spawning real
//! podman processes.
//!
//! Errors are intentionally collapsed to `bool` at the trait surface
//! for the *signalling* methods (`kill_pid1`, `stop`, `rm_all`, ...):
//! every caller in this binary treats those failures as "best effort,
//! move on". The exception is [`PodmanBackend::unshare_remove`], whose
//! caller (`cleanup::remove_tmp_prefix`) needs the captured stderr in
//! the manager's log to diagnose why `/tmp/asm-*` cleanup fails — it
//! therefore returns `Result<(), String>` with stderr/argv/exit packed
//! into the error string.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// Backend abstraction. Production: [`RealPodman`]. Tests: a mock that
/// records calls in order.
pub trait PodmanBackend {
    /// `podman container exists <NAME>` — true iff exit status 0.
    fn container_exists(&self, name: &str) -> bool;

    /// `podman exec <NAME> kill -<SIGNAL> <pid>` — true iff exit 0.
    /// Used to signal the secondary process inside the container by its
    /// pid-1 child PID.
    fn exec_signal(&self, name: &str, pid: u32, signal: &str) -> bool;

    /// `podman exec <NAME> pgrep -P 1 -o` — return the oldest child of
    /// PID 1 inside the container, or `None` if pgrep finds nothing
    /// (or the exec fails — caller treats both alike, see state-machine
    /// commentary).
    fn exec_pgrep_first_child(&self, name: &str) -> Option<u32>;

    /// `podman kill --signal <SIGNAL> <NAME>` — signals pid 1 of the
    /// container itself. Belt-and-suspenders for the case the user
    /// process never spawned a child, or pgrep missed it.
    fn kill_pid1(&self, name: &str, signal: &str) -> bool;

    /// `podman stop -t <grace_secs> <NAME>` — graceful stop, falling
    /// back to SIGKILL after `grace_secs`.
    fn stop(&self, name: &str, grace_secs: u32) -> bool;

    /// `podman rm -af` — remove all containers under this storage
    /// root, releasing layer references. Idempotent.
    fn rm_all(&self) -> bool;

    /// `podman unshare rm -rf <path>` — drop into the user-namespace
    /// where the storage subuids are owned and remove a directory
    /// tree. Required because subuid-owned files under `<tmp>/storage`
    /// are not unlinkable by the host UID alone.
    ///
    /// On failure returns `Err(stderr)` where the string carries the
    /// captured stderr plus the argv and exit status — this is the
    /// only podman call whose failure we actively diagnose, since
    /// `/tmp/asm-*` directories silently piling up on workers is a
    /// real recurring symptom and the next repro must tell us why.
    fn unshare_remove(&self, path: &Path) -> Result<(), String>;
}

/// Production backend. Holds the podman binary path AND the
/// storage/runroot prefix so callers do not have to know about
/// either.
///
/// `podman_path` is an explicit input (not a hard-coded `"podman"`)
/// because the manager runs inside a systemd-user-service unit whose
/// `PATH` does NOT inherit the parent shell's PATH — on NixOS workers
/// `podman` lives at `/run/current-system/sw/bin/podman`, which is
/// not on the default user-systemd PATH and would ENOENT under
/// `Command::new("podman").spawn()` (asm-tokenizer 2026-05-18). The
/// wrapper script resolves `command -v podman` once at render time
/// and passes the absolute path via `--podman-path`.
#[derive(Debug, Clone)]
pub struct RealPodman {
    podman_path: PathBuf,
    storage_root: PathBuf,
    runroot: PathBuf,
}

impl RealPodman {
    pub fn new(podman_path: PathBuf, storage_root: PathBuf, runroot: PathBuf) -> Self {
        Self {
            podman_path,
            storage_root,
            runroot,
        }
    }

    /// Build a `podman` invocation pre-loaded with the common prefix:
    /// `--root <storage_root> --runroot <runroot> --cgroup-manager=cgroupfs`.
    /// All subcommands flow through this helper to keep the prefix
    /// in exactly one place.
    fn cmd(&self) -> Command {
        let mut c = Command::new(&self.podman_path);
        c.arg("--root")
            .arg(&self.storage_root)
            .arg("--runroot")
            .arg(&self.runroot)
            .arg("--cgroup-manager=cgroupfs");
        c
    }

    /// Run a command swallowing all output, returning exit-0 as bool.
    fn run_silent(mut cmd: Command) -> bool {
        cmd.stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        matches!(cmd.status(), Ok(s) if s.success())
    }

    /// Run a command capturing stderr (stdout/stdin still nulled).
    /// `Ok(())` on exit-0; `Err(diag)` otherwise, where `diag` packs
    /// argv (debug-formatted — may include shell-unsafe chars, fine
    /// for a log line but not for replay), exit status, and the
    /// captured stderr decoded best-effort as UTF-8 via
    /// `String::from_utf8_lossy`.
    fn run_capture_stderr(mut cmd: Command) -> Result<(), String> {
        cmd.stdin(Stdio::null()).stdout(Stdio::null());
        let argv = format!("{:?}", cmd);
        match cmd.output() {
            Ok(out) => match out.status.success() {
                true => Ok(()),
                false => Err(format!(
                    "argv: {}; exit={}; stderr: {}",
                    argv,
                    out.status,
                    String::from_utf8_lossy(&out.stderr).trim_end()
                )),
            },
            Err(e) => Err(format!("argv: {}; spawn-error: {}", argv, e)),
        }
    }
}

impl PodmanBackend for RealPodman {
    fn container_exists(&self, name: &str) -> bool {
        let mut c = self.cmd();
        c.arg("container").arg("exists").arg(name);
        Self::run_silent(c)
    }

    fn exec_signal(&self, name: &str, pid: u32, signal: &str) -> bool {
        let mut c = self.cmd();
        c.arg("exec")
            .arg(name)
            .arg("kill")
            .arg(format!("-{}", signal))
            .arg(pid.to_string());
        Self::run_silent(c)
    }

    fn exec_pgrep_first_child(&self, name: &str) -> Option<u32> {
        let mut c = self.cmd();
        c.arg("exec")
            .arg(name)
            .arg("pgrep")
            .arg("-P")
            .arg("1")
            .arg("-o")
            .stdin(Stdio::null())
            .stderr(Stdio::null());
        let out = c.output().ok()?;
        match out.status.success() {
            false => None,
            true => {
                let text = String::from_utf8(out.stdout).ok()?;
                text.trim().lines().next()?.trim().parse::<u32>().ok()
            }
        }
    }

    fn kill_pid1(&self, name: &str, signal: &str) -> bool {
        let mut c = self.cmd();
        c.arg("kill").arg("--signal").arg(signal).arg(name);
        Self::run_silent(c)
    }

    fn stop(&self, name: &str, grace_secs: u32) -> bool {
        let mut c = self.cmd();
        c.arg("stop")
            .arg("-t")
            .arg(grace_secs.to_string())
            .arg(name);
        Self::run_silent(c)
    }

    fn rm_all(&self) -> bool {
        let mut c = self.cmd();
        c.arg("rm").arg("-af");
        Self::run_silent(c)
    }

    fn unshare_remove(&self, path: &Path) -> Result<(), String> {
        let mut c = self.cmd();
        c.arg("unshare").arg("rm").arg("-rf").arg(path);
        Self::run_capture_stderr(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke: builder records podman_path/storage/runroot in the
    /// command vector (we can't easily inspect a `Command` post-hoc
    /// without spawning, so this is more a constructor sanity-check
    /// than a behaviour test — behaviour is exercised via the mock
    /// in `tests/common`).
    #[test]
    fn real_backend_constructs() {
        let b = RealPodman::new(
            PathBuf::from("/nix/store/x/bin/podman"),
            PathBuf::from("/r"),
            PathBuf::from("/rr"),
        );
        assert_eq!(b.podman_path, Path::new("/nix/store/x/bin/podman"));
        assert_eq!(b.storage_root, Path::new("/r"));
        assert_eq!(b.runroot, Path::new("/rr"));
    }
}
