//! Single concern: invoking the `podman` CLI behind a trait so the
//! state machine in `poll_loop` is generic over the backend.
//!
//! All real-world invocations go through the same root/runroot/
//! cgroup-manager prefix; the production impl owns that prefix once.
//! Tests use [`MockBackend`] in `tests/common`, never spawning real
//! podman processes.
//!
//! Errors are intentionally collapsed to `bool` at the trait surface:
//! every caller in this binary treats podman failure as "best effort,
//! move on" — surfacing typed error info upward would only be ignored
//! and inflate the binary.

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
    fn unshare_remove(&self, path: &Path) -> bool;
}

/// Production backend. Holds the storage/runroot prefix so callers do
/// not have to know about it.
#[derive(Debug, Clone)]
pub struct RealPodman {
    storage_root: PathBuf,
    runroot: PathBuf,
}

impl RealPodman {
    pub fn new(storage_root: PathBuf, runroot: PathBuf) -> Self {
        Self {
            storage_root,
            runroot,
        }
    }

    /// Build a `podman` invocation pre-loaded with the common prefix:
    /// `--root <storage_root> --runroot <runroot> --cgroup-manager=cgroupfs`.
    /// All subcommands flow through this helper to keep the prefix
    /// in exactly one place.
    fn cmd(&self) -> Command {
        let mut c = Command::new("podman");
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

    fn unshare_remove(&self, path: &Path) -> bool {
        let mut c = self.cmd();
        c.arg("unshare").arg("rm").arg("-rf").arg(path);
        Self::run_silent(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke: builder records storage/runroot in the command vector
    /// (we can't easily inspect a `Command` post-hoc without spawning,
    /// so this is more a constructor sanity-check than a behaviour
    /// test — behaviour is exercised via the mock in `tests/common`).
    #[test]
    fn real_backend_constructs() {
        let b = RealPodman::new(PathBuf::from("/r"), PathBuf::from("/rr"));
        assert_eq!(b.storage_root, Path::new("/r"));
        assert_eq!(b.runroot, Path::new("/rr"));
    }
}
