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
//! move on". The cleanup-walk primitives — `unshare_find_files`,
//! `unshare_find_dirs`, `unshare_unlink`, `unshare_rmdir` — are the
//! exception and return `Result<_, String>` with captured stderr so
//! `cleanup::remove_tmp_prefix` can log the specific failure for each
//! entry without aborting the whole walk.
//!
//! The walk-primitives DELIBERATELY do not expose any recursive flag
//! (no `-r`, no `-rf`, no `-depth -delete`). Recursion is owned by
//! `cleanup` — listing first, removing one entry per call — so a
//! caller-side bug in the path computation can never cascade into a
//! `rm -rf` of an unintended subtree. There is no host-side fallback;
//! if `podman unshare` cannot enter the userns the cleanup logs the
//! failure and exits leaving the tree in place for an operator to
//! diagnose.

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

    /// `podman unshare <find-path> <root> -mindepth 1 -type f
    /// -print0` — list every FILE under `root` (one entry per
    /// returned `PathBuf`). The cleanup walk's stage-1 plans
    /// per-file unlink from this list.
    ///
    /// `-mindepth 1` excludes `root` itself (which is a directory and
    /// belongs to the stage-3 dir list). `-print0` emits NUL-separated
    /// output so paths containing newlines (rare under `/tmp/asm-*`
    /// but possible) parse unambiguously.
    ///
    /// On non-zero exit returns `Err(diag)` where `diag` packs the
    /// argv, exit status, and captured stderr — the cleanup-walk
    /// caller logs this and aborts cleanup (rather than fall back
    /// to a recursive host-side `remove_dir_all`, which would
    /// defeat the entire single-entry-per-op design).
    fn unshare_find_files(&self, root: &Path) -> Result<Vec<PathBuf>, String>;

    /// `podman unshare <find-path> <root> -depth -type d -print0` —
    /// list every DIRECTORY under `root` (INCLUDING `root` itself),
    /// leaf-first.
    ///
    /// `-depth` is load-bearing: rmdir requires emptiness, so the
    /// walk's stage-4 must process children before their parents.
    /// `-print0` for the same NUL-safety reason as `unshare_find_files`.
    ///
    /// `Err` on non-zero exit, same diagnostic shape as
    /// `unshare_find_files`.
    fn unshare_find_dirs(&self, root: &Path) -> Result<Vec<PathBuf>, String>;

    /// `podman unshare <rm-path> -- <file>` — unlink ONE file. No
    /// `-r`, no `-f`. Single-entry.
    ///
    /// `Err(stderr-diag)` on non-zero exit. The walk continues past
    /// per-file failures (so a single stuck inode doesn't block the
    /// rest of the prefix from being torn down); the per-file
    /// diagnostic is logged.
    fn unshare_unlink(&self, file: &Path) -> Result<(), String>;

    /// `podman unshare <rmdir-path> -- <dir>` — remove ONE empty
    /// directory. Single-entry.
    ///
    /// `Err` typically signals `ENOTEMPTY`/`EBUSY` (the walk's
    /// planner left children, or another process re-populated the
    /// dir between list-time and rmdir-time). The walk continues
    /// past per-dir failures with the failure count summarised at
    /// end-of-walk.
    fn unshare_rmdir(&self, dir: &Path) -> Result<(), String>;
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
/// `Command::new("podman").spawn()`. `rm_path`, `rmdir_path`, and
/// `find_path` follow the same convention — the wrapper resolves
/// `command -v rm|rmdir|find` once at render time so the per-entry
/// cleanup primitives have absolute paths to invoke under `podman
/// unshare`.
#[derive(Debug, Clone)]
pub struct RealPodman {
    podman_path: PathBuf,
    rm_path: PathBuf,
    rmdir_path: PathBuf,
    find_path: PathBuf,
    storage_root: PathBuf,
    runroot: PathBuf,
}

impl RealPodman {
    pub fn new(
        podman_path: PathBuf,
        rm_path: PathBuf,
        rmdir_path: PathBuf,
        find_path: PathBuf,
        storage_root: PathBuf,
        runroot: PathBuf,
    ) -> Self {
        Self {
            podman_path,
            rm_path,
            rmdir_path,
            find_path,
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

    /// Run a command capturing stdout AND stderr. `Ok(stdout-bytes)`
    /// on exit-0; `Err(stderr-diag)` otherwise. Used by the
    /// `unshare_find_*` primitives, which need the NUL-separated
    /// stdout payload while still surfacing exit-code+stderr on
    /// failure.
    fn run_capture_stdout(mut cmd: Command) -> Result<Vec<u8>, String> {
        cmd.stdin(Stdio::null());
        let argv = format!("{:?}", cmd);
        match cmd.output() {
            Ok(out) => match out.status.success() {
                true => Ok(out.stdout),
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

/// Parse a `-print0` payload (NUL-separated paths, trailing NUL
/// included for non-empty output) into a `Vec<PathBuf>`. Empty
/// elements (between adjacent NULs, or the trailing-NUL terminator)
/// are filtered. UTF-8 decoded best-effort via `String::from_utf8_lossy`
/// — paths under `/tmp/asm-*` are ASCII-safe by construction (random
/// suffix + container-runtime-generated subdirs); a stray non-UTF-8
/// byte would corrupt that one path to `U+FFFD` rather than failing
/// the walk.
fn split_print0(bytes: &[u8]) -> Vec<PathBuf> {
    bytes
        .split(|&b| b == 0)
        .filter(|slice| !slice.is_empty())
        .map(|slice| PathBuf::from(String::from_utf8_lossy(slice).into_owned()))
        .collect()
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

    fn unshare_find_files(&self, root: &Path) -> Result<Vec<PathBuf>, String> {
        let mut c = self.cmd();
        c.arg("unshare")
            .arg(&self.find_path)
            .arg(root)
            .arg("-mindepth")
            .arg("1")
            .arg("-type")
            .arg("f")
            .arg("-print0");
        Self::run_capture_stdout(c).map(|bytes| split_print0(&bytes))
    }

    fn unshare_find_dirs(&self, root: &Path) -> Result<Vec<PathBuf>, String> {
        let mut c = self.cmd();
        c.arg("unshare")
            .arg(&self.find_path)
            .arg(root)
            .arg("-depth")
            .arg("-type")
            .arg("d")
            .arg("-print0");
        Self::run_capture_stdout(c).map(|bytes| split_print0(&bytes))
    }

    fn unshare_unlink(&self, file: &Path) -> Result<(), String> {
        let mut c = self.cmd();
        c.arg("unshare").arg(&self.rm_path).arg("--").arg(file);
        Self::run_capture_stderr(c)
    }

    fn unshare_rmdir(&self, dir: &Path) -> Result<(), String> {
        let mut c = self.cmd();
        c.arg("unshare").arg(&self.rmdir_path).arg("--").arg(dir);
        Self::run_capture_stderr(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke: builder records every CLI-path field on the struct (we
    /// can't easily inspect a `Command` post-hoc without spawning, so
    /// this is more a constructor sanity-check than a behaviour test
    /// — behaviour is exercised via the mock in `tests/common`).
    #[test]
    fn real_backend_constructs() {
        let b = RealPodman::new(
            PathBuf::from("/nix/store/x/bin/podman"),
            PathBuf::from("/nix/store/x/bin/rm"),
            PathBuf::from("/nix/store/x/bin/rmdir"),
            PathBuf::from("/nix/store/x/bin/find"),
            PathBuf::from("/r"),
            PathBuf::from("/rr"),
        );
        assert_eq!(b.podman_path, Path::new("/nix/store/x/bin/podman"));
        assert_eq!(b.rm_path, Path::new("/nix/store/x/bin/rm"));
        assert_eq!(b.rmdir_path, Path::new("/nix/store/x/bin/rmdir"));
        assert_eq!(b.find_path, Path::new("/nix/store/x/bin/find"));
        assert_eq!(b.storage_root, Path::new("/r"));
        assert_eq!(b.runroot, Path::new("/rr"));
    }

    /// `split_print0` consumes the canonical `find -print0` payload
    /// shape: each entry terminated by NUL, including the final entry.
    /// Empty slices between adjacent NULs (or after the trailing NUL)
    /// must be filtered — `find -print0` never emits an empty path
    /// itself, but the trailing NUL produces an empty trailing slice
    /// from `split`.
    #[test]
    fn split_print0_parses_canonical_payload() {
        let payload = b"/tmp/asm-XXX/a\0/tmp/asm-XXX/b\0/tmp/asm-XXX/c\0";
        let parsed = split_print0(payload);
        assert_eq!(
            parsed,
            vec![
                PathBuf::from("/tmp/asm-XXX/a"),
                PathBuf::from("/tmp/asm-XXX/b"),
                PathBuf::from("/tmp/asm-XXX/c"),
            ]
        );
    }

    /// Empty `find` output (no matching entries under the root) parses
    /// to an empty Vec. The cleanup walk relies on this — an empty
    /// stage-1 list yields zero unlink calls and proceeds straight to
    /// stage-3.
    #[test]
    fn split_print0_handles_empty_payload() {
        assert!(split_print0(b"").is_empty());
        assert!(split_print0(b"\0").is_empty());
        assert!(split_print0(b"\0\0\0").is_empty());
    }
}
