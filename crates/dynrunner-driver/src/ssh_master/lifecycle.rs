//! `SshMaster::spawn` and `SshMaster::adopt` constructors. Both
//! produce a fully-initialised `SshMaster`; spawn-master owns the
//! daemon's lifetime, adopt-master does not. Locked design point
//! (a-bis) governs the type-internal branching downstream
//! (`disconnect`, `Drop`).

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use crate::config::SshConfig;
use crate::error::SshMasterError;
use crate::ssh_target::SshTarget;

use super::SshMaster;
use super::argv::{
    build_auth_flags, build_base_args, build_master_argv, generate_master_control_path,
    validate_control_path_len,
};
use super::probe::{probe_master_pid, probe_master_pid_with_timeout};
use super::watcher::spawn_master_watcher;

impl SshMaster {
    /// Construct by spawning `ssh -M -N`. Sync (per locked design
    /// point (i): no async-spawn variant).
    ///
    /// Steps:
    ///   1. Generate `/tmp/dynrunner-m-<pid>-<seq>.sock` (validated
    ///      under 108 bytes per locked point (e)).
    ///   2. Fork the launcher.
    ///   3. Poll for the control socket to appear (10s deadline).
    ///   4. Probe daemon PID via `ssh -O check`.
    ///   5. Reap the launcher zombie.
    ///   6. Spawn the watcher std::thread.
    pub fn spawn(config: SshConfig) -> Result<Self, SshMasterError> {
        let target = config.target.clone();
        tracing::info!(target = %target, "spawning SSH master");

        // Stable control socket under /tmp. We deliberately do NOT
        // use `tempfile::TempDir`: TempDir's Drop unlinks the parent
        // dir on `SshMaster` Drop, racing with the master's own
        // socket cleanup and stranding a master with no path back
        // for `ssh -O exit` (bug (g) regression class).
        let cp_str = generate_master_control_path();
        let cp = PathBuf::from(&cp_str);
        // Defence in depth: the path generator is bounded by
        // construction (PID + seq fits well under 108), but pin the
        // check so a future edit that lengthens the prefix surfaces
        // as a typed error rather than a silent ssh bind failure.
        validate_control_path_len(&cp)?;
        // Pre-flight: if a stale socket from a prior crashed instance
        // happens to collide (PID + sequence makes this near-
        // impossible, but be defensive), clear it so ssh can bind.
        let _ = std::fs::remove_file(&cp);

        let auth_flags = build_auth_flags(&config);
        let base_args = build_base_args(config.port, &auth_flags);

        // Direct std::process::Command spawn of `ssh -M -N` — no
        // `-f`, no `setsid` indirection. NB the spawned process is
        // the *launcher*: with `ControlPersist=yes` (which we pin
        // in master_only_options), OpenSSH always forks a daemon
        // child at end-of-handshake and the launcher exits 0 within
        // ~120ms. The daemon, reparented to systemd --user / init,
        // is the actual long-lived master. We discover its PID via
        // `ssh -O check` once the control socket appears (below)
        // and use *that* PID as the lifetime anchor.
        let argv = build_master_argv(
            &base_args,
            &cp_str,
            &config.forwarded_ports,
            target.as_str(),
        );
        let mut cmd = Command::new("ssh");
        cmd.args(&argv);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::null());

        let mut launcher = cmd.spawn().map_err(SshMasterError::spawn_failed)?;
        let launcher_pid = launcher.id();

        // Wait for the control socket to appear. 10s timeout — the
        // SSH handshake usually completes in <500ms on a healthy link.
        // While waiting, also poll the launcher: if it exited with
        // *non-zero* status before the socket appeared, the handshake
        // failed — surface as `HandshakeRefused`. A *zero* exit
        // before the socket appears is benign (the daemon child
        // created the socket but a small ordering window let the
        // launcher's exit_group beat the directory entry showing up).
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut handshake_failure = false;
        while !cp.exists() {
            if let Ok(Some(status)) = launcher.try_wait()
                && !status.success()
            {
                handshake_failure = true;
                break;
            }
            if Instant::now() >= deadline {
                let _ = launcher.kill();
                let _ = launcher.wait();
                return Err(SshMasterError::ControlSocketTimeout);
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        if handshake_failure {
            let _ = launcher.wait();
            return Err(SshMasterError::HandshakeRefused);
        }

        // Discover the daemon PID via `ssh -O check`. Output shape:
        //   stdout: "Master running (pid=<N>)\n"
        // Exit status non-zero means the control socket exists but
        // doesn't respond — a real fault, not an interim handshake
        // state (we already waited for the socket to appear).
        let daemon_pid = match probe_master_pid(&cp_str, target.as_str(), &base_args) {
            Ok(pid) => pid,
            Err(e) => {
                // Best-effort cleanup so a probe failure doesn't
                // leak the launcher / daemon. The launcher will exit
                // on its own; we *don't* know the daemon PID, so
                // fall back to `ssh -O exit` which goes via the
                // socket and lands at the daemon.
                let mut exit_cmd = Command::new("ssh");
                for arg in &base_args {
                    exit_cmd.arg(arg);
                }
                exit_cmd.args(["-O", "exit", "-o", &format!("ControlPath={cp_str}")]);
                exit_cmd.arg(target.as_str());
                exit_cmd.stdin(std::process::Stdio::null());
                exit_cmd.stdout(std::process::Stdio::null());
                exit_cmd.stderr(std::process::Stdio::null());
                let _ = exit_cmd.status();
                return Err(e);
            }
        };

        // Reap the launcher zombie ASAP. Reading exit status is
        // bookkeeping-only; we don't gate on it. On the rare path
        // where the launcher hasn't exited yet (control socket showed
        // up faster than the launcher's exit_group, possible on
        // loaded systems) `wait()` blocks until it does — typically
        // single-digit ms.
        let _ = launcher.wait();

        let invalidated = Arc::new(AtomicBool::new(false));
        let watcher_cancel = Arc::new(AtomicBool::new(false));
        let watcher_thread =
            spawn_master_watcher(daemon_pid, Arc::clone(&watcher_cancel), Arc::clone(&invalidated));

        tracing::info!(
            ?launcher_pid,
            daemon_pid,
            target = %target,
            "SSH master spawned"
        );

        Ok(Self {
            target,
            port: config.port,
            auth_flags,
            control_path: cp,
            daemon_pid: Some(daemon_pid),
            last_known_pid: Some(daemon_pid),
            invalidated,
            watcher_cancel,
            watcher_thread: Some(watcher_thread),
            forwarded_ports: config.forwarded_ports,
            is_spawned: true,
            spawn_timestamp: Instant::now(),
            test_kill_hook: None,
        })
    }

    /// Adopt an externally-spawned master pointed-at by `path`. Per
    /// locked design point (k): three fail-fast checks at
    /// construction time.
    ///
    ///   1. `path.as_os_str().len() < 108` — the kernel
    ///      `sockaddr_un.sun_path` cap. Beyond this, ssh silently
    ///      fails to connect.
    ///   2. `path` exists and is a socket file (stat check).
    ///   3. `ssh -O check` on the path returns
    ///      `Master running (pid=N)`. **Minimal flags only** — no
    ///      `-i`, no `-F` from any caller-provided config — because
    ///      the master responds via the unix socket regardless. The
    ///      host arg is structurally required but ignored at the
    ///      master layer; we pass `target` verbatim.
    ///
    /// On any failure: [`SshMasterError::MasterAdoptFailed`] with a
    /// `reason` describing which check tripped.
    pub fn adopt(path: PathBuf, target: SshTarget) -> Result<Self, SshMasterError> {
        // (1) length check.
        validate_control_path_len(&path)?;
        // (2) stat check: must exist AND be a socket. We use
        // `std::fs::metadata` (not `try_exists`) because it surfaces
        // the file type via `FileType::is_socket` (cfg(unix)), which
        // distinguishes "file is a regular file the operator pointed
        // us at by accident" from "file is a unix socket".
        match std::fs::metadata(&path) {
            Err(e) => {
                return Err(SshMasterError::adopt_failed(
                    path,
                    format!("control path not accessible: {e}"),
                ));
            }
            Ok(md) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::FileTypeExt;
                    if !md.file_type().is_socket() {
                        return Err(SshMasterError::adopt_failed(
                            path,
                            "control path is not a socket file",
                        ));
                    }
                }
                #[cfg(not(unix))]
                {
                    let _ = md;
                }
            }
        }
        // (3) `ssh -O check` with MINIMAL flags. Master responds via
        // the unix socket; the host arg is ignored at the master
        // layer. Avoids host/identity mismatch issues. We add a
        // hard 3s timeout because `ssh -O check` against a socket
        // that has a listener but isn't an SSH multiplex master
        // hangs indefinitely waiting for a response — we observed
        // this with `std::os::unix::net::UnixListener::bind`-only
        // sockets in the integration tests. The timeout converts
        // that hang into a typed `MasterAdoptFailed`.
        let cp_str = path.to_string_lossy().into_owned();
        let probed_pid = probe_master_pid_with_timeout(
            &cp_str,
            target.as_str(),
            &[],
            Duration::from_secs(3),
        )
        .map_err(|e| {
            SshMasterError::adopt_failed(
                path.clone(),
                format!("ssh -O check rejected adoption: {e}"),
            )
        })?;

        tracing::info!(
            target = %target,
            control_path = ?path,
            probed_pid,
            "adopted external SSH master"
        );

        // Adopt-master invariants:
        // - `is_spawned = false` → Drop is a no-op.
        // - `daemon_pid = None` → we do not track liveness on a
        //   master we don't own (the upstream driver owns it).
        // - `last_known_pid = Some(probed_pid)` → `master_pid()`
        //   returns the value we observed at adoption.
        // - No watcher thread.
        // - `auth_flags` empty: per locked point (k), the master
        //   responds via unix socket and accepts no per-call auth
        //   from us. The session layer holds its own auth.
        Ok(Self {
            target,
            port: 22, // unused for adopt-master; session layer brings its own port
            auth_flags: Vec::new(),
            control_path: path,
            daemon_pid: None,
            last_known_pid: Some(probed_pid),
            invalidated: Arc::new(AtomicBool::new(false)),
            watcher_cancel: Arc::new(AtomicBool::new(false)),
            watcher_thread: None,
            forwarded_ports: Vec::new(),
            is_spawned: false,
            spawn_timestamp: Instant::now(),
            test_kill_hook: None,
        })
    }

}
