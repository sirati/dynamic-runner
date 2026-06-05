//! Single concern: spawn the out-of-cgroup shutdown manager
//! (generate.rs:214-296): `systemd-run --user --unit` service mode with a
//! `setsid -f` fallback, same argv as the bash.
//!
//! XDG_RUNTIME_DIR invariant (Phase 2): `systemd_user_runtime_dir` is the
//! canonical per-uid value (`$XDG_RUNTIME_DIR` or `/run/user/<euid>`). It is
//! only ever applied as a per-`Command` env override on the `systemd-run`
//! child (mirroring the bash `XDG_RUNTIME_DIR=... systemd-run` prefix). The
//! wrapper process must NOT globally clobber its own `XDG_RUNTIME_DIR`; the
//! podman child is given `XDG_RUNTIME_DIR=<layout.podman_run>` only on its own
//! `Command` env. Keeping both as per-child overrides is what keeps this value
//! correct for the bus probe / systemd-run while podman still sees its
//! storage-cookie path.

use crate::bin_resolve::{on_path, ResolvedBins};
use crate::dirs::Layout;
use dynrunner_slurm_wrapper_config::WrapperConfig;
use std::fs::OpenOptions;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

/// Which cgroup-escape primitive actually started the manager — the
/// teardown forward (`teardown.rs`) picks the matching signal path.
#[derive(Debug, Clone)]
pub enum ShutdownMode {
    /// `systemd-run --user --unit=<unit>` (cgroup escape OK).
    Systemd { unit: String },
    /// `setsid -f` fallback; pid captured from the manager's pid-file.
    Setsid { pid: u32 },
    /// Neither primitive available, or the config had no binary path.
    None,
}

/// Shared manager argv (generate.rs:246-254 / :267-275). Pure so the golden
/// test can assert the exact token vector without spawning.
fn manager_args(layout: &Layout, bins: &ResolvedBins, wrapper_pid: u32) -> Vec<String> {
    vec![
        "--container-name".to_string(),
        layout.container_name.clone(),
        "--storage-root".to_string(),
        layout.podman_storage.display().to_string(),
        "--runroot".to_string(),
        layout.podman_run.display().to_string(),
        "--tmp-prefix".to_string(),
        layout.rndtmp.display().to_string(),
        "--pid-file".to_string(),
        layout.shutdown_pid_file.display().to_string(),
        "--wrapper-pid".to_string(),
        wrapper_pid.to_string(),
        "--log-file".to_string(),
        layout.shutdown_log_path.display().to_string(),
        "--podman-path".to_string(),
        bins.podman.clone(),
        "--rm-path".to_string(),
        bins.rm.clone(),
        // HOST side of the reaper-panik sentinel. The reaper writes this
        // file as a graceful last resort when its direct PID-reap cannot
        // confirm the workload dead; it appears at the mirrored container
        // path (`podman_run.rs` mounts `<log_tmp>:/app/log-tmp` and injects
        // the matching in-container `--panik-file`), so the secondary's
        // own panik watcher sees it and shuts down gracefully. Derived
        // from `layout.log_tmp` so host and container sides share one
        // source of truth.
        "--panik-file".to_string(),
        layout.reaper_panik_host_path().display().to_string(),
    ]
}

/// `systemd-run` argv (everything after the `systemd-run` program), mirroring
/// generate.rs:239-254. Pure for golden testing.
fn systemd_run_args(layout: &Layout, bin: &Path, manager_args: &[String]) -> Vec<String> {
    let mut args = vec![
        "--user".to_string(),
        "--quiet".to_string(),
        format!("--unit={}", layout.shutdown_unit_name),
        "--property=Restart=no".to_string(),
        "--property=PrivateTmp=false".to_string(),
        "--property=StandardError=journal".to_string(),
        "--".to_string(),
        bin.display().to_string(),
    ];
    args.extend(manager_args.iter().cloned());
    args
}

/// `setsid` argv (everything after the `setsid` program), mirroring
/// generate.rs:266-275: `-f <bin> <manager_args...>`. Pure for golden testing.
fn setsid_args(bin: &Path, manager_args: &[String]) -> Vec<String> {
    let mut args = vec!["-f".to_string(), bin.display().to_string()];
    args.extend(manager_args.iter().cloned());
    args
}

/// Canonical per-uid systemd runtime dir (generate.rs:333-341): `$XDG_RUNTIME_DIR`
/// if set, else `/run/user/<euid>`. Captured WITHOUT mutating the wrapper's own
/// environment — see the module-level XDG_RUNTIME_DIR invariant.
fn systemd_user_runtime_dir() -> PathBuf {
    match std::env::var_os("XDG_RUNTIME_DIR") {
        Some(v) if !v.is_empty() => PathBuf::from(v),
        _ => PathBuf::from(format!("/run/user/{}", nix::unistd::geteuid())),
    }
}

/// Open `<shutdown_log_path>` in append mode for use as a child's stderr/stdout
/// (mirrors the bash `2>>"$SHUTDOWN_LOG_PATH"` / `>>... 2>&1`).
fn append_log(path: &Path) -> std::io::Result<std::fs::File> {
    OpenOptions::new().create(true).append(true).open(path)
}

/// Spawn the manager and return the mode it started in. `wrapper_pid` is
/// passed as `--wrapper-pid`. When `cfg.shutdown_manager_bin_path` is
/// `None`, returns `ShutdownMode::None` without spawning.
pub fn spawn(
    cfg: &WrapperConfig,
    layout: &Layout,
    bins: &ResolvedBins,
    wrapper_pid: u32,
) -> ShutdownMode {
    let Some(bin) = cfg.shutdown_manager_bin_path.as_deref() else {
        return ShutdownMode::None;
    };

    let manager = manager_args(layout, bins, wrapper_pid);
    let runtime_dir = systemd_user_runtime_dir();
    let log_path = layout.shutdown_log_path.as_path();

    // --- Service mode (preferred): systemd-run --user --unit (cgroup escape).
    let bus_socket = runtime_dir.join("systemd/private");
    let bus_ok = std::fs::metadata(&bus_socket)
        .map(|m| {
            use std::os::unix::fs::FileTypeExt;
            m.file_type().is_socket()
        })
        .unwrap_or(false);
    if bus_ok && on_path("systemd-run") {
        match try_systemd_run(layout, bin, &manager, &runtime_dir, log_path) {
            Some(mode) => return mode,
            None => { /* non-zero exit: warned inside; fall through */ }
        }
    }

    // --- setsid fallback (cgroup escape DISABLED).
    if on_path("setsid") {
        return run_setsid(layout, bin, &manager, log_path);
    }

    eprintln!(
        "ERROR: neither systemd-run --user --unit (bus probe failed or \
         registration failed) nor setsid available; orphan-container cleanup \
         DISABLED on signal"
    );
    ShutdownMode::None
}

/// Run `systemd-run` synchronously (it blocks until registration). `Some` on
/// success, `None` (with a WARNING logged) on non-zero / spawn failure so the
/// caller falls through to setsid.
fn try_systemd_run(
    layout: &Layout,
    bin: &Path,
    manager: &[String],
    runtime_dir: &Path,
    log_path: &Path,
) -> Option<ShutdownMode> {
    let stderr = match append_log(log_path) {
        Ok(f) => Stdio::from(f),
        Err(e) => {
            eprintln!(
                "WARNING: systemd-run --user --unit failed (cannot open log \
                 {}: {e}); falling back to setsid -- cgroup escape DISABLED",
                log_path.display()
            );
            return None;
        }
    };
    let mut cmd = Command::new("systemd-run");
    cmd.args(systemd_run_args(layout, bin, manager))
        .env("XDG_RUNTIME_DIR", runtime_dir)
        .stderr(stderr);
    // Reset the inherited blocked signal mask before exec so the manager
    // (and the unit systemd spawns) start with normal signal disposition —
    // applied to the systemd-run invocation regardless of whether systemd
    // re-derives the unit's mask, per the C2 safety note.
    // SAFETY: child_pre_exec runs only an async-signal-safe sigprocmask.
    unsafe {
        cmd.pre_exec(crate::signals::child_pre_exec());
    }
    let status = cmd.status();
    match status {
        Ok(s) if s.success() => {
            println!(
                "Spawned shutdown manager in unit {} (cgroup escape via \
                 user.slice service)",
                layout.shutdown_unit_name
            );
            Some(ShutdownMode::Systemd {
                unit: layout.shutdown_unit_name.clone(),
            })
        }
        Ok(s) => {
            eprintln!(
                "WARNING: systemd-run --user --unit failed (exit={}); falling \
                 back to setsid -- cgroup escape DISABLED",
                s.code()
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "signal".to_string())
            );
            None
        }
        Err(e) => {
            eprintln!(
                "WARNING: systemd-run --user --unit failed (exit={e}); falling \
                 back to setsid -- cgroup escape DISABLED"
            );
            None
        }
    }
}

/// `setsid -f <bin> <manager_args>` with stdin from /dev/null and stdout+stderr
/// appended to the log; then poll the manager's pid-file (50 * 100ms).
fn run_setsid(layout: &Layout, bin: &Path, manager: &[String], log_path: &Path) -> ShutdownMode {
    eprintln!(
        "WARNING: shutdown manager running under setsid -- cgroup escape \
         DISABLED; SLURM TIMEOUT will reap the manager before cleanup."
    );

    let devnull = match OpenOptions::new().read(true).open("/dev/null") {
        Ok(f) => Stdio::from(f),
        Err(_) => Stdio::null(),
    };
    let (out, err) = match (append_log(log_path), append_log(log_path)) {
        (Ok(o), Ok(e)) => (Stdio::from(o), Stdio::from(e)),
        _ => (Stdio::null(), Stdio::null()),
    };
    // setsid -f detaches and returns immediately; we ignore its handle and rely
    // on the manager's pid-file for the pid (mirrors the bash, which does the
    // same poll rather than trusting setsid's exit pid).
    let mut cmd = Command::new("setsid");
    cmd.args(setsid_args(bin, manager))
        .stdin(devnull)
        .stdout(out)
        .stderr(err);
    // Reset the inherited blocked signal mask before exec (C2 safety note:
    // applied to the setsid invocation regardless of session inheritance).
    // SAFETY: child_pre_exec runs only an async-signal-safe sigprocmask.
    unsafe {
        cmd.pre_exec(crate::signals::child_pre_exec());
    }
    let _ = cmd.status();

    let pid_file = layout.shutdown_pid_file.as_path();
    for _ in 0..50 {
        if pid_file.is_file() {
            if let Some(pid) = std::fs::read_to_string(pid_file)
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok())
            {
                println!(
                    "Spawned shutdown manager (setsid pid={pid}); cgroup escape \
                     unavailable"
                );
                return ShutdownMode::Setsid { pid };
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    eprintln!(
        "ERROR: setsid-spawned shutdown manager did not write pid-file within \
         5s -- wrapper cleanup forward will be a no-op"
    );
    ShutdownMode::None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout() -> Layout {
        Layout {
            rndtmp: PathBuf::from("/tmp/asm-abc123"),
            container_name: "asm-abc123-7".to_string(),
            src_tmp: PathBuf::from("/tmp/asm-abc123/src"),
            out_tmp: PathBuf::from("/tmp/asm-abc123/out"),
            log_tmp: PathBuf::from("/tmp/asm-abc123/log"),
            podman_storage: PathBuf::from("/tmp/asm-abc123/storage"),
            podman_run: PathBuf::from("/tmp/asm-abc123/run"),
            socket_dir: PathBuf::from("/tmp/asm-abc123/sockets"),
            cmd_socket: PathBuf::from("/tmp/asm-abc123/sockets/cmd.sock"),
            shutdown_unit_name: "dynrunner-shutdown-abc123".to_string(),
            // Persistent per-secondary log dir on the network share; NOT
            // under the /tmp scratch tree the manager deletes on teardown.
            shutdown_log_dir: PathBuf::from("/net/log/7"),
            shutdown_log_path: PathBuf::from("/net/log/7/shutdown-manager.log"),
            shutdown_pid_file: PathBuf::from("/tmp/asm-abc123/shutdown-manager.pid"),
            local_image: PathBuf::from("/tmp/asm-abc123/image.tar"),
            image_cache_root: PathBuf::from("/tmp/asm-imgcache"),
        }
    }

    fn bins() -> ResolvedBins {
        ResolvedBins {
            podman: "/run/current-system/sw/bin/podman".to_string(),
            rm: "/run/current-system/sw/bin/rm".to_string(),
        }
    }

    #[test]
    fn manager_args_golden() {
        let got = manager_args(&layout(), &bins(), 4242);
        let expected = vec![
            "--container-name",
            "asm-abc123-7",
            "--storage-root",
            "/tmp/asm-abc123/storage",
            "--runroot",
            "/tmp/asm-abc123/run",
            "--tmp-prefix",
            "/tmp/asm-abc123",
            "--pid-file",
            "/tmp/asm-abc123/shutdown-manager.pid",
            "--wrapper-pid",
            "4242",
            "--log-file",
            "/net/log/7/shutdown-manager.log",
            "--podman-path",
            "/run/current-system/sw/bin/podman",
            "--rm-path",
            "/run/current-system/sw/bin/rm",
            "--panik-file",
            "/tmp/asm-abc123/log/.dynrunner-reaper.panik",
        ];
        assert_eq!(got, expected);
    }

    #[test]
    fn systemd_run_args_golden() {
        let l = layout();
        let bin = PathBuf::from("/opt/dynrunner-slurm-shutdown");
        let manager = manager_args(&l, &bins(), 4242);
        let got = systemd_run_args(&l, &bin, &manager);
        let expected = vec![
            "--user",
            "--quiet",
            "--unit=dynrunner-shutdown-abc123",
            "--property=Restart=no",
            "--property=PrivateTmp=false",
            "--property=StandardError=journal",
            "--",
            "/opt/dynrunner-slurm-shutdown",
            "--container-name",
            "asm-abc123-7",
            "--storage-root",
            "/tmp/asm-abc123/storage",
            "--runroot",
            "/tmp/asm-abc123/run",
            "--tmp-prefix",
            "/tmp/asm-abc123",
            "--pid-file",
            "/tmp/asm-abc123/shutdown-manager.pid",
            "--wrapper-pid",
            "4242",
            "--log-file",
            "/net/log/7/shutdown-manager.log",
            "--podman-path",
            "/run/current-system/sw/bin/podman",
            "--rm-path",
            "/run/current-system/sw/bin/rm",
            "--panik-file",
            "/tmp/asm-abc123/log/.dynrunner-reaper.panik",
        ];
        assert_eq!(got, expected);
    }

    #[test]
    fn setsid_args_golden() {
        let bin = PathBuf::from("/opt/dynrunner-slurm-shutdown");
        let manager = manager_args(&layout(), &bins(), 4242);
        let got = setsid_args(&bin, &manager);
        let expected = vec![
            "-f",
            "/opt/dynrunner-slurm-shutdown",
            "--container-name",
            "asm-abc123-7",
            "--storage-root",
            "/tmp/asm-abc123/storage",
            "--runroot",
            "/tmp/asm-abc123/run",
            "--tmp-prefix",
            "/tmp/asm-abc123",
            "--pid-file",
            "/tmp/asm-abc123/shutdown-manager.pid",
            "--wrapper-pid",
            "4242",
            "--log-file",
            "/net/log/7/shutdown-manager.log",
            "--podman-path",
            "/run/current-system/sw/bin/podman",
            "--rm-path",
            "/run/current-system/sw/bin/rm",
            "--panik-file",
            "/tmp/asm-abc123/log/.dynrunner-reaper.panik",
        ];
        assert_eq!(got, expected);
    }
}
