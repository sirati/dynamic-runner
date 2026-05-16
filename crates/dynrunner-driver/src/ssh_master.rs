//! [`SshMaster`] — owns a long-lived OpenSSH `ControlPersist` master
//! daemon's lifetime.
//!
//! # Single concern
//!
//! Spawn or adopt the master, hand back its control socket path,
//! observe its liveness, and (for spawn-master) tear it down on
//! [`Drop`] or [`SshMaster::disconnect`]. **No command execution**:
//! `execute_command` / `transfer_file` / `scp` / `sftp` belong to
//! the [`crate::session::Session`] type, which composes over an
//! [`SshMaster`] reference. Two-type split per locked design point
//! (a) — future transports (mosh / aws-ssm / kubectl-exec) will
//! implement the same session trait without touching master
//! lifecycle.
//!
//! If you find yourself adding `fn execute_command()` / `fn scp()`
//! / `fn ssh()` as an `impl SshMaster`, STOP. It belongs on the
//! session layer.
//!
//! # Lifetime model (carried over verbatim from the original
//! `dynrunner-gateway::ssh::SshGateway`)
//!
//! OpenSSH with `ControlPersist=yes` *always* forks-and-detaches at
//! the end of the handshake: the `ssh -M -N` process we spawn (the
//! "launcher") exits 0 within ~120ms via `exit_group(0)`; a daemon
//! child becomes the persistent master, reparented to `systemd
//! --user` (or PID 1 / init), in a different session. The daemon is
//! the process that responds to control-socket commands (`ssh -O
//! exit` / `ssh -O check` / per-channel ssh+scp invocations) — *not*
//! the launcher. Tracking the launcher's `Child` as the master
//! lifetime anchor was the bug behind a silent regression of bug
//! (g): `Child::kill()` and `kill_on_drop(true)` operate on the
//! launcher zombie, so dropping the master without an explicit
//! teardown leaked the daemon.
//!
//! Post-fix: we discover the daemon PID via `ssh -O check` after the
//! control socket appears, reap the launcher zombie immediately, and
//! hold the daemon PID as the lifetime anchor. Drop sends SIGTERM
//! (then SIGKILL after a brief grace) to the daemon directly via
//! `nix::sys::signal::kill`. The watcher polls `kill(daemon_pid, 0)`
//! so the "master died" log fires for actual daemon death.
//!
//! # Drop-on-abort contract (locked design point (g))
//!
//! Drop runs only on graceful Rust-side drop. Process abort
//! (SIGKILL, segfault, OOM-kill) leaves the daemon alive; the 18h
//! ServerAlive ladder (`ServerAliveInterval=60 ×
//! ServerAliveCountMax=1080`) is the only floor in that case.
//! Consumers needing crash-safe daemon cleanup should rely on the
//! OpenSSH-side keepalive contract, not on `Drop`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::config::SshConfig;
use crate::error::{KillLadder, SshMasterError};
use crate::ssh_target::SshTarget;

/// Owns the lifetime of an OpenSSH `ControlPersist` master daemon.
///
/// Two construction paths per locked design point (a-bis):
/// - [`SshMaster::spawn`] — we run `ssh -M -N`, discover the daemon
///   PID, and own it (Drop → SIGTERM/SIGKILL ladder).
/// - [`SshMaster::adopt`] — an upstream driver pre-spawned a master
///   we point at via its control socket. Drop is a **no-op** — the
///   daemon is not ours to kill — and `disconnect()` does
///   per-forward `ssh -O cancel` cleanup, NOT termination.
///
/// `master_pid()` returns the daemon PID (the long-lived
/// `ControlPersist` process), never the short-lived launcher zombie.
/// After watcher-observed invalidation, `master_pid()` returns the
/// *last known* PID (`Some(pid)`, not `None`) — see locked design
/// point (h.1) and the field doc on [`Self::invalidated`].
///
/// `Debug` is intentionally a hand-written non-derive impl below so
/// the `test_kill_hook` field (a closure with no `Debug`) doesn't
/// block the impl, and so error / log output never leaks the closure
/// (which is internal-test-only).
pub struct SshMaster {
    /// `user@host` (or `host`) target arg used by every ssh
    /// subprocess. Stored so error variants can include it without
    /// having to keep the full [`SshConfig`] around (which would
    /// risk leaking identity-file paths into telemetry).
    target: SshTarget,
    /// `-p <port>` arg, or 22 for the default. The session layer
    /// threads it into per-call ssh/scp invocations; storing on the
    /// master simplifies the Session API.
    port: u16,
    /// Per-instance auth flags (`-i`, `IdentitiesOnly`,
    /// `IdentityAgent`, `-F`). Only *we* use these on `ssh -O
    /// check`/`ssh -O exit`/`ssh -O cancel` — the session layer
    /// holds its own copy of equivalent flags. We retain them
    /// because the `disconnect()` path on adopt-master needs to
    /// authenticate the per-forward cleanup commands, and we don't
    /// want callers to re-derive them on every call.
    auth_flags: Vec<String>,
    /// Path to the Unix-domain control socket. For `spawn()` this is
    /// generated under /tmp; for `adopt()` it's the operator-supplied
    /// path. Either way, every per-channel ssh subprocess threads it
    /// via `-o ControlPath=<this>`.
    control_path: PathBuf,
    /// PID of the persistent `ControlPersist` daemon. `None` for
    /// `adopt()`-constructed instances (we *can* probe `ssh -O check`
    /// at construction time, but we don't track the daemon for
    /// teardown — Drop is a no-op for adopt-master). For `spawn()`-
    /// constructed instances, populated at the end of construction
    /// and cleared by [`Drop`] / [`SshMaster::disconnect`] right
    /// before tearing the daemon down so a second teardown call is a
    /// fast no-op.
    daemon_pid: Option<u32>,
    /// **Last-known** daemon PID, preserved across watcher-observed
    /// invalidation. Per locked point (h.1): `master_pid()` returns
    /// this value, not `None`, after the watcher fires — surfacing
    /// the "was alive, not is alive" semantics. Cleared only on
    /// successful `disconnect()` (where the death is *expected*).
    last_known_pid: Option<u32>,
    /// `true` once the watcher has observed daemon death OR the
    /// kill-ladder has run to completion. Operations after this
    /// surface [`SshMasterError::MasterDied`]; `disconnect()` and
    /// `Drop` post-invalidation are no-ops (locked points (h.2),
    /// (h.3)).
    invalidated: Arc<AtomicBool>,
    /// Cancellation flag for the master-watcher std::thread. Set by
    /// `disconnect()` *before* tearing the daemon down so the
    /// *expected* exit doesn't surface as "died unexpectedly", and
    /// by `Drop` for the same reason on the panic / forgot-to-
    /// disconnect path. The watcher thread observes this on each 1s
    /// poll and exits silently when set.
    watcher_cancel: Arc<AtomicBool>,
    /// Handle to the master-watcher std::thread. The thread is
    /// deliberately *not* a tokio task: under PyO3+Python the calling
    /// runtime is short-lived (the wrapper builds a fresh
    /// current-thread runtime per call and drops it on return), so a
    /// tokio-spawned watcher would be cancelled the moment the call
    /// returned. A `std::thread` outlives any per-call runtime and
    /// only exits on `watcher_cancel` or daemon death.
    watcher_thread: Option<std::thread::JoinHandle<()>>,
    /// `(local_port, remote_port)` pairs registered via the spawn
    /// argv (or, for `adopt()`, via runtime `ssh -O forward`). On
    /// `disconnect()` of an adopt-master we issue `ssh -O cancel
    /// -R 0.0.0.0:<remote>:localhost:<local>` per entry — partial
    /// cleanup, not termination, per locked design point (b).
    forwarded_ports: Vec<(u16, u16)>,
    /// `true` if this instance was constructed via `spawn()` and
    /// therefore owns the daemon. `false` for `adopt()`. Drives the
    /// Drop / disconnect() branch selection — locked points (a-bis),
    /// (b), (h.3).
    is_spawned: bool,
    /// When the master was spawned (or, for adopt, when adoption
    /// succeeded). Used by [`SshMasterError::MasterDied`] to expose
    /// the "spawn → death" interval to post-mortem tooling.
    spawn_timestamp: Instant,
    /// **Hook** used only by tests to inject a faked kill-ladder
    /// outcome without actually sending signals. Production code
    /// always uses [`terminate_daemon_blocking`]; tests that
    /// pin the panic-in-Drop prohibition (locked point (j)) set
    /// this to a closure that returns `Err(UnkillableMaster)`. The
    /// field is `Option<...>` rather than a generic so SshMaster's
    /// type signature stays unchanged in production.
    ///
    /// Always-compiled (not `cfg(test)`-gated) so integration tests
    /// in `tests/` can reach it via [`SshMaster::install_test_kill_hook`].
    /// Marked `#[doc(hidden)]` on its setter to keep it out of the
    /// public docs surface — the API is internal-test-only.
    test_kill_hook: Option<TestKillHook>,
}

type TestKillHook = Box<dyn Fn(u32, &SshTarget) -> Result<(), SshMasterError> + Send + Sync>;

impl std::fmt::Debug for SshMaster {
    /// Hand-written `Debug` impl: the `test_kill_hook` field stores
    /// a closure (no `Debug`), so we can't `derive(Debug)`. The impl
    /// also intentionally surfaces only the operator-relevant state
    /// — target, control path, daemon PID, invalidation, spawn-vs-
    /// adopt — never the auth flags or the test hook closure.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SshMaster")
            .field("target", &self.target)
            .field("control_path", &self.control_path)
            .field("port", &self.port)
            .field("daemon_pid", &self.daemon_pid)
            .field("last_known_pid", &self.last_known_pid)
            .field("is_spawned", &self.is_spawned)
            .field("invalidated", &self.invalidated.load(Ordering::SeqCst))
            .field("forwarded_ports", &self.forwarded_ports)
            .finish_non_exhaustive()
    }
}

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

    /// PID of the daemon master (the long-lived `ControlPersist`
    /// process). After watcher-observed invalidation, returns the
    /// last-known PID per locked point (h.1) — the semantic is
    /// **was alive, not is alive**.
    pub fn master_pid(&self) -> Option<u32> {
        self.last_known_pid
    }

    /// Path to the control socket. Threaded into `-o
    /// ControlPath=<this>` by the session layer on every per-call
    /// ssh/scp invocation.
    pub fn control_path(&self) -> &Path {
        &self.control_path
    }

    /// The `user@host` target. Borrowed for telemetry and for
    /// session-layer ssh subprocess composition.
    pub fn target(&self) -> &SshTarget {
        &self.target
    }

    /// Port. The session layer threads it into `ssh -p` / `scp -P`.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Auth flags pinned at construction time. The session layer
    /// reuses these so per-channel ssh/scp invocations carry the
    /// same identity contract as the master spawn. Empty for
    /// adopt-master (the upstream driver owns the master's auth;
    /// the session layer brings its own).
    pub fn auth_flags(&self) -> &[String] {
        &self.auth_flags
    }

    /// Forwarded ports registered at spawn (or via runtime
    /// registrations on adopt-master). Borrowed for diagnostics and
    /// for the disconnect-time per-forward `ssh -O cancel` cleanup
    /// on adopt-master.
    pub fn forwarded_ports(&self) -> &[(u16, u16)] {
        &self.forwarded_ports
    }

    /// `true` if constructed via [`Self::spawn`].
    pub fn is_spawned(&self) -> bool {
        self.is_spawned
    }

    /// `true` if the watcher has observed daemon death or the kill
    /// ladder has run to completion. Operations against the master
    /// after invalidation surface [`SshMasterError::MasterDied`];
    /// `disconnect()` and `Drop` post-invalidation are no-ops.
    pub fn is_invalidated(&self) -> bool {
        self.invalidated.load(Ordering::SeqCst)
    }

    /// Register a runtime-added reverse forward on an adopt-master.
    /// Issues `ssh -O forward -R 0.0.0.0:<remote>:localhost:<local>`
    /// against the control socket and, on success, records the pair
    /// in `forwarded_ports` so the `disconnect()`-time `ssh -O
    /// cancel` finds it.
    ///
    /// Spawn-master forwards are baked into the spawn argv; calling
    /// `add_forward` on a spawn-master would issue a duplicate
    /// registration and is rejected with [`SshMasterError::Other`].
    pub fn add_forward(
        &mut self,
        local_port: u16,
        remote_port: u16,
    ) -> Result<(), SshMasterError> {
        if self.is_spawned {
            return Err(SshMasterError::Other(
                "add_forward is only valid on adopt-master; \
                 spawn-master forwards are baked into the spawn argv via SshConfig"
                    .into(),
            ));
        }
        if self.is_invalidated() {
            return Err(self.master_died_err());
        }
        let cp_str = self.control_path.to_string_lossy().into_owned();
        let mut cmd = Command::new("ssh");
        for arg in &self.auth_flags {
            cmd.arg(arg);
        }
        cmd.args([
            "-O",
            "forward",
            "-o",
            &format!("ControlPath={cp_str}"),
            "-R",
            &format!("0.0.0.0:{remote_port}:localhost:{local_port}"),
        ]);
        cmd.arg(self.target.as_str());
        let out = cmd
            .output()
            .map_err(|e| SshMasterError::Other(format!("ssh -O forward spawn: {e}")))?;
        if !out.status.success() {
            return Err(SshMasterError::Other(format!(
                "ssh -O forward {remote_port}:localhost:{local_port} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        self.forwarded_ports.push((local_port, remote_port));
        Ok(())
    }

    /// Tear the master down (spawn-master) or release per-forward
    /// state (adopt-master).
    ///
    /// **Spawn-master**: cancel the watcher, run `ssh -O exit`
    /// (polite), then SIGTERM/SIGKILL ladder via `nix::kill`.
    /// Surfaces [`SshMasterError::UnkillableMaster`] if even SIGKILL
    /// doesn't take.
    ///
    /// **Adopt-master**: per [`Self::forwarded_ports`] entry, run
    /// `ssh -O cancel -R 0.0.0.0:<remote>:localhost:<local>`. The
    /// master itself is *not* killed — it belongs to the upstream
    /// driver. Per locked design point (b): partial cleanup, not
    /// termination.
    ///
    /// Idempotent. Calling after a successful disconnect (or after
    /// watcher-observed invalidation) is a no-op (locked point
    /// (h.2)).
    pub fn disconnect(&mut self) -> Result<(), SshMasterError> {
        // Post-invalidation: no-op (h.2). Cancel the watcher first
        // in case Drop runs after this — keeps the cancel/teardown
        // ordering invariant from getting violated.
        if self.is_invalidated() {
            self.watcher_cancel.store(true, Ordering::SeqCst);
            if let Some(t) = self.watcher_thread.take() {
                let _ = t.join();
            }
            return Ok(());
        }

        if self.is_spawned {
            self.disconnect_spawn_master()
        } else {
            self.disconnect_adopt_master()
        }
    }

    fn disconnect_spawn_master(&mut self) -> Result<(), SshMasterError> {
        // Signal the watcher to exit silently. Anything from this
        // point forward is an *expected* teardown; the observer must
        // not log it as unexpected. The watcher thread observes the
        // flag on its next 1s poll and returns. We join the thread
        // *after* the daemon is down so it has its expected exit
        // condition (either flag-set or daemon-gone, whichever wins).
        self.watcher_cancel.store(true, Ordering::SeqCst);

        // Politely ask the master to exit via `ssh -O exit` first,
        // which cleans up the control socket and reverse forwards in
        // an orderly fashion. The control-socket request lands at
        // the daemon (the launcher is long gone by now).
        if let Some(cp_str) = self.control_path.to_str() {
            let mut cmd = Command::new("ssh");
            for arg in &self.auth_flags {
                cmd.arg(arg);
            }
            if self.port != 22 {
                cmd.args(["-p", &self.port.to_string()]);
            }
            cmd.args([
                "-O",
                "exit",
                "-o",
                &format!("ControlPath={cp_str}"),
            ]);
            cmd.arg(self.target.as_str());
            cmd.stdin(std::process::Stdio::null());
            cmd.stdout(std::process::Stdio::null());
            cmd.stderr(std::process::Stdio::null());
            let _ = cmd.status();
        }

        // Wait for the daemon to actually exit, with SIGTERM/SIGKILL
        // fallback. Bounded at ~250ms total.
        let result = if let Some(pid) = self.daemon_pid.take() {
            self.run_kill_ladder(pid)
        } else {
            Ok(())
        };

        // Join the watcher thread (sync — it observes the cancel
        // flag on its next 1s poll). This is bounded at ~1s.
        if let Some(t) = self.watcher_thread.take() {
            let _ = t.join();
        }

        // Mark invalidated regardless of ladder outcome: post-
        // disconnect, the master is gone (or so unkillable that
        // further attempts won't help). Either way operations after
        // this should not target the daemon.
        self.invalidated.store(true, Ordering::SeqCst);
        // On clean teardown, last_known_pid is no longer interesting
        // — clear it so `master_pid()` returns None for the "we
        // explicitly tore it down" case (distinguishing from the
        // watcher-observed-death case where last_known_pid is still
        // Some).
        if result.is_ok() {
            self.last_known_pid = None;
        }

        result
    }

    fn disconnect_adopt_master(&mut self) -> Result<(), SshMasterError> {
        // Per locked point (b): adopt-master `disconnect()` runs
        // `ssh -O cancel -R …` per forwarded_ports entry. We do NOT
        // kill the daemon. We do NOT issue `ssh -O exit`.
        let cp_str = self.control_path.to_string_lossy().into_owned();
        let mut last_err: Option<SshMasterError> = None;
        for &(local_port, remote_port) in &self.forwarded_ports {
            let mut cmd = Command::new("ssh");
            for arg in &self.auth_flags {
                cmd.arg(arg);
            }
            cmd.args([
                "-O",
                "cancel",
                "-o",
                &format!("ControlPath={cp_str}"),
                "-R",
                &format!("0.0.0.0:{remote_port}:localhost:{local_port}"),
            ]);
            cmd.arg(self.target.as_str());
            cmd.stdin(std::process::Stdio::null());
            cmd.stdout(std::process::Stdio::null());
            cmd.stderr(std::process::Stdio::null());
            match cmd.output() {
                Ok(o) if o.status.success() => continue,
                Ok(o) => {
                    last_err = Some(SshMasterError::Other(format!(
                        "ssh -O cancel -R {remote_port}:localhost:{local_port} \
                         exited {}: {}",
                        o.status.code().unwrap_or(-1),
                        String::from_utf8_lossy(&o.stderr).trim()
                    )));
                }
                Err(e) => {
                    last_err = Some(SshMasterError::Other(format!(
                        "ssh -O cancel spawn: {e}"
                    )));
                }
            }
        }
        // Whether or not the cancels succeeded, mark this handle as
        // invalidated so further calls are no-ops.
        self.invalidated.store(true, Ordering::SeqCst);
        match last_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn master_died_err(&self) -> SshMasterError {
        SshMasterError::MasterDied {
            target: self.target.clone(),
            last_known_pid: self.last_known_pid,
            spawn_timestamp: self.spawn_timestamp,
            observation_timestamp: Instant::now(),
        }
    }

    /// Production: real SIGTERM→SIGKILL ladder via [`terminate_daemon_blocking`].
    /// Tests: dispatch via `test_kill_hook` if set, so the
    /// panic-in-Drop prohibition test can fake an UnkillableMaster
    /// outcome without sending real signals.
    #[inline]
    fn run_kill_ladder(&self, pid: u32) -> Result<(), SshMasterError> {
        if let Some(hook) = &self.test_kill_hook {
            return hook(pid, &self.target);
        }
        terminate_daemon_blocking(pid, &self.target)
    }
}

impl Drop for SshMaster {
    /// Per locked design point (b):
    /// - **spawn-master**: SIGTERM→SIGKILL ladder. Per (j),
    ///   `tracing::error!` on UnkillableMaster but **no panic** —
    ///   double-panic = process abort.
    /// - **adopt-master**: no-op. The daemon is not ours to kill.
    /// - **post-invalidation**: no-op (locked point (h.3)).
    fn drop(&mut self) {
        // adopt-master + post-invalidation: nothing to do beyond
        // joining the (possibly-already-cancelled) watcher.
        if !self.is_spawned || self.is_invalidated() {
            self.watcher_cancel.store(true, Ordering::SeqCst);
            if let Some(t) = self.watcher_thread.take() {
                let _ = t.join();
            }
            return;
        }

        // spawn-master: full teardown.
        self.watcher_cancel.store(true, Ordering::SeqCst);
        if let Some(pid) = self.daemon_pid.take() {
            // Per locked point (j): never panic in Drop. Log on
            // UnkillableMaster and continue.
            if let Err(e) = self.run_kill_ladder(pid) {
                tracing::error!(
                    target = %self.target,
                    error = %e,
                    "SSH master Drop: terminate ladder did not complete cleanly; \
                     leaking daemon (per locked-design panic-in-Drop prohibition)"
                );
            }
        }
        if let Some(t) = self.watcher_thread.take() {
            // Best-effort join. The thread sees the cancel flag on
            // its next 1s poll tick. We swallow `JoinError` because
            // panicking in Drop would mask the underlying error.
            let _ = t.join();
        }
        self.invalidated.store(true, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------
// Internal helpers — single concern: ssh argv construction, control
// path generation, daemon-PID probing, watcher thread, sync teardown
// ladder. Only `pub(crate)` items are intended for the Session layer
// (which builds its own ssh argv on top of `auth_flags()` and the
// public `port()` accessor).
// ---------------------------------------------------------------------

/// Master-only `-o` flags. Pinned here (not in operator-owned
/// ssh_config) because they're the framework's lifetime contract for
/// the master process: liveness-floor + log-noise-suppression.
/// `ServerAliveInterval=60 × ServerAliveCountMax=1080 = 18h`
/// keepalive-probe budget (locked design point (f)). NOT overridable
/// — operators wanting different keepalive use `adopt()` with their
/// own master.
fn master_only_options() -> &'static [&'static str] {
    &[
        "-o", "ControlMaster=auto",
        "-o", "ControlPersist=yes",
        "-o", "ServerAliveInterval=60",
        "-o", "ServerAliveCountMax=1080",
        "-o", "TCPKeepAlive=yes",
        "-o", "LogLevel=ERROR",
    ]
}

/// Build the auth flags (`-i`, `-F`, `IdentitiesOnly=yes`,
/// `IdentityAgent=none`) for both the master spawn and per-channel
/// ssh subprocesses. Order is part of the contract — see the
/// pre-extraction `dynrunner-gateway::ssh::SshGateway::auth_options`
/// docstring for the agent-leakage rationale.
fn build_auth_flags(config: &SshConfig) -> Vec<String> {
    let mut opts = Vec::new();
    if let Some(identity) = &config.identity_file {
        opts.extend([
            "-i".to_string(),
            identity.to_string_lossy().into_owned(),
            "-o".to_string(),
            "IdentitiesOnly=yes".to_string(),
            "-o".to_string(),
            "IdentityAgent=none".to_string(),
        ]);
    }
    if let Some(config_file) = &config.config_file {
        opts.extend([
            "-F".to_string(),
            config_file.to_string_lossy().into_owned(),
        ]);
    }
    opts
}

/// Construct the always-on argv prefix for ssh subprocesses
/// (`-p` plus auth flags). The master spawn appends `-M -N -o
/// ControlPath=… <master_only_options> [-R …] <target>` after this;
/// per-call ssh/scp invocations from the session layer compose
/// differently.
fn build_base_args(port: u16, auth_flags: &[String]) -> Vec<String> {
    let mut args = Vec::new();
    if port != 22 {
        args.push("-p".into());
        args.push(port.to_string());
    }
    args.extend_from_slice(auth_flags);
    args
}

/// Build the argv (excluding `ssh` itself) for the master spawn.
///
/// Pure function — pulled out so the contract (specifically the 18h
/// ServerAlive floor) is unit-testable without a live sshd.
pub(crate) fn build_master_argv(
    base_args: &[String],
    control_path: &str,
    forwarded_ports: &[(u16, u16)],
    target: &str,
) -> Vec<String> {
    let mut argv: Vec<String> = Vec::new();
    argv.extend(base_args.iter().cloned());
    argv.push("-M".into());
    argv.push("-N".into());
    argv.push("-o".into());
    argv.push(format!("ControlPath={control_path}"));
    for opt in master_only_options() {
        argv.push((*opt).into());
    }
    for &(local_port, remote_port) in forwarded_ports {
        argv.push("-R".into());
        argv.push(format!("0.0.0.0:{remote_port}:localhost:{local_port}"));
    }
    argv.push(target.into());
    argv
}

/// Per-process monotonic suffix for control-socket paths. Combined
/// with PID, gives a per-instance unique path under /tmp without
/// pulling in a `rand` dep. Acquire-Release isn't needed; only the
/// uniqueness matters, not memory ordering with other state.
static CONTROL_PATH_SEQ: AtomicU64 = AtomicU64::new(0);

/// Generate a master control socket path under `/tmp`.
///
/// Format: `/tmp/dynrunner-m-<pid>-<seq>.sock`. Stays well below the
/// 108-byte `sockaddr_un.sun_path` cap even with 7-digit PIDs and
/// 19-digit sequence numbers (locked point (e)).
pub(crate) fn generate_master_control_path() -> String {
    let pid = std::process::id();
    let seq = CONTROL_PATH_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("/tmp/dynrunner-m-{pid}-{seq}.sock")
}

/// Validate that a control-socket path fits in `sockaddr_un.sun_path`.
/// The kernel cap is 108 bytes including the NUL terminator. We
/// require strictly < 108 to leave room for the NUL.
fn validate_control_path_len(p: &Path) -> Result<(), SshMasterError> {
    use std::os::unix::ffi::OsStrExt;
    let len = p.as_os_str().as_bytes().len();
    if len < 108 {
        return Ok(());
    }
    Err(SshMasterError::adopt_failed(
        p.to_path_buf(),
        format!("control path is {len} bytes; sockaddr_un.sun_path cap is 108"),
    ))
}

/// Run `ssh -O check` over the control socket and return the daemon
/// PID. Errors surface as [`SshMasterError::MasterPidProbeFailed`].
pub(crate) fn probe_master_pid(
    control_path: &str,
    target: &str,
    base_args: &[String],
) -> Result<u32, SshMasterError> {
    let mut cmd = Command::new("ssh");
    for arg in base_args {
        cmd.arg(arg);
    }
    cmd.args([
        "-O",
        "check",
        "-o",
        &format!("ControlPath={control_path}"),
    ]);
    cmd.arg(target);

    let output = cmd
        .output()
        .map_err(|_| SshMasterError::MasterPidProbeFailed)?;
    if !output.status.success() {
        return Err(SshMasterError::MasterPidProbeFailed);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    parse_master_pid(&stdout)
        .or_else(|| parse_master_pid(&stderr))
        .ok_or(SshMasterError::MasterPidProbeFailed)
}

/// Same as [`probe_master_pid`] but with a hard timeout: if the
/// `ssh -O check` subprocess hasn't returned within `timeout`, we
/// SIGKILL it and surface [`SshMasterError::MasterPidProbeFailed`].
///
/// Why this exists: `ssh -O check` against a socket that has a
/// listener but doesn't speak the SSH multiplex protocol hangs
/// indefinitely. We observed this in the
/// `adopt_rejects_stale_socket_no_master_behind_it` integration test
/// (a Rust `UnixListener::bind` with no accept loop). The fast-path
/// (a real master responding to the probe) returns in <1ms; the
/// slow-path is a bug-class signal we want to convert into a typed
/// error, not a hang.
///
/// Implementation: spawn the subprocess and poll its `try_wait`
/// status at 50ms cadence, sending SIGKILL on deadline. Sync (we're
/// already in a sync path) and dependency-free.
fn probe_master_pid_with_timeout(
    control_path: &str,
    target: &str,
    base_args: &[String],
    timeout: Duration,
) -> Result<u32, SshMasterError> {
    let mut cmd = Command::new("ssh");
    for arg in base_args {
        cmd.arg(arg);
    }
    cmd.args([
        "-O",
        "check",
        "-o",
        &format!("ControlPath={control_path}"),
    ]);
    cmd.arg(target);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = cmd
        .spawn()
        .map_err(|_| SshMasterError::MasterPidProbeFailed)?;

    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                // Subprocess exited. Read its output.
                let out = child.wait_with_output().unwrap_or_else(|_| {
                    std::process::Output {
                        status,
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                    }
                });
                if !out.status.success() {
                    return Err(SshMasterError::MasterPidProbeFailed);
                }
                let stdout = String::from_utf8_lossy(&out.stdout);
                let stderr = String::from_utf8_lossy(&out.stderr);
                return parse_master_pid(&stdout)
                    .or_else(|| parse_master_pid(&stderr))
                    .ok_or(SshMasterError::MasterPidProbeFailed);
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    // Hung subprocess — kill and surface as probe failure.
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(SshMasterError::MasterPidProbeFailed);
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => return Err(SshMasterError::MasterPidProbeFailed),
        }
    }
}

/// Parse `Master running (pid=<N>)` out of `ssh -O check` output.
fn parse_master_pid(s: &str) -> Option<u32> {
    let marker = "Master running (pid=";
    let rest = s.find(marker).map(|i| &s[i + marker.len()..])?;
    let pid_str: String =
        rest.chars().take_while(char::is_ascii_digit).collect();
    if pid_str.is_empty() {
        return None;
    }
    pid_str.parse().ok()
}

/// Spawn the master-watcher thread. Returns the join handle. Polls
/// `kill(daemon_pid, 0)` once per second; on ESRCH, sets
/// `invalidated` and emits a `tracing::error!`.
fn spawn_master_watcher(
    daemon_pid: u32,
    cancel: Arc<AtomicBool>,
    invalidated: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(format!("dynrunner-ssh-master-watch-{daemon_pid}"))
        .spawn(move || master_watcher_loop(daemon_pid, cancel, invalidated))
        .expect("failed to spawn ssh-master-watch thread")
}

fn master_watcher_loop(daemon_pid: u32, cancel: Arc<AtomicBool>, invalidated: Arc<AtomicBool>) {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    let pid = Pid::from_raw(daemon_pid as i32);
    let tick = Duration::from_secs(1);
    loop {
        std::thread::sleep(tick);
        if cancel.load(Ordering::SeqCst) {
            return;
        }
        match kill(pid, None) {
            Ok(()) => continue,
            Err(Errno::ESRCH) => {
                invalidated.store(true, Ordering::SeqCst);
                tracing::error!(daemon_pid, "SSH master exited unexpectedly");
                return;
            }
            Err(e) => {
                // EPERM in particular means the daemon is alive but
                // not owned by us (PID reuse onto another user's
                // process — rare, but possible). Treat any other
                // errno as "stop probing" so we don't spin forever
                // on a structural fault.
                tracing::warn!(
                    daemon_pid,
                    error = %e,
                    "SSH master kill(pid,0) probe failed; stopping observer"
                );
                return;
            }
        }
    }
}

/// Sync daemon-teardown ladder: SIGTERM → 200ms grace → SIGKILL
/// → 50ms settle.
///
/// Returns `Err(SshMasterError::UnkillableMaster)` only when even
/// SIGKILL did not result in ESRCH within the post-SIGKILL settle
/// window. This is the only path through which the unkillable
/// condition is surfaced — Drop logs (per locked point (j)) and
/// `disconnect()` returns the variant.
///
/// Sync (not async) for two reasons:
///   1. Drop is sync — async-ifying would require holding a runtime
///      handle on the master, which leaks runtime ownership into
///      the master type.
///   2. The polite `ssh -O exit` already had its sync chance up the
///      stack; this is the fallback ladder, where blocking for at
///      most ~250ms is cheap.
fn terminate_daemon_blocking(daemon_pid: u32, target: &SshTarget) -> Result<(), SshMasterError> {
    use nix::errno::Errno;
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let pid = Pid::from_raw(daemon_pid as i32);

    // Fast-path: already gone (e.g. `ssh -O exit` worked, or this is
    // a second teardown call after disconnect()).
    if matches!(kill(pid, None), Err(Errno::ESRCH)) {
        return Ok(());
    }

    // SIGTERM, then poll until ESRCH or grace expires.
    if let Err(e) = kill(pid, Signal::SIGTERM)
        && !matches!(e, Errno::ESRCH)
    {
        tracing::warn!(
            daemon_pid,
            error = %e,
            "SIGTERM to SSH master daemon failed"
        );
    }
    let grace = Instant::now() + Duration::from_millis(200);
    let poll = Duration::from_millis(20);
    loop {
        if matches!(kill(pid, None), Err(Errno::ESRCH)) {
            return Ok(());
        }
        if Instant::now() >= grace {
            break;
        }
        std::thread::sleep(poll);
    }

    // Grace expired — SIGKILL. Reaping isn't ours to do (the daemon
    // is reparented to systemd --user / init), so we just signal
    // and poll once more to confirm.
    if let Err(e) = kill(pid, Signal::SIGKILL)
        && !matches!(e, Errno::ESRCH)
    {
        tracing::warn!(
            daemon_pid,
            error = %e,
            "SIGKILL to SSH master daemon failed"
        );
    }
    // Brief post-SIGKILL settle. We don't loop indefinitely:
    // SIGKILL is un-ignorable, and a process surviving SIGKILL is
    // an unrecoverable kernel-level fault we don't want to spin on.
    std::thread::sleep(Duration::from_millis(50));
    if matches!(kill(pid, None), Err(Errno::ESRCH)) {
        return Ok(());
    }
    Err(SshMasterError::UnkillableMaster {
        target: target.clone(),
        last_known_pid: daemon_pid,
        kill_ladder_reached: KillLadder::SigkillButPidStillExists,
    })
}

// ---------------------------------------------------------------------
// Test-only API: install a fake kill ladder for the panic-in-Drop
// prohibition test (locked point (j)). The hook is always compiled
// (not `cfg(test)`-gated) so integration tests in `tests/` — which
// link against the crate as an external dep — can reach it. Marked
// `#[doc(hidden)]` to keep it out of the public docs surface; the
// API is internal-test-only.
// ---------------------------------------------------------------------

impl SshMaster {
    /// Install a closure that will be invoked instead of
    /// [`terminate_daemon_blocking`] from `disconnect_spawn_master`
    /// and `Drop`. **Internal test API only** — production code
    /// must never call this. Used by the
    /// `drop_does_not_panic_on_unkillable_master` integration test
    /// to inject UnkillableMaster outcomes without sending real
    /// signals.
    ///
    /// Naming + `#[doc(hidden)]` are the visibility contract: the
    /// symbol exists for testing but does not surface in rustdoc.
    /// A future feature-flag gate (`__test-hooks`) is the cleaner
    /// long-term home, but adding it requires a workspace-wide
    /// feature wiring change that's out of scope for the extraction
    /// commit.
    #[doc(hidden)]
    pub fn install_test_kill_hook(
        &mut self,
        hook: impl Fn(u32, &SshTarget) -> Result<(), SshMasterError> + Send + Sync + 'static,
    ) {
        self.test_kill_hook = Some(Box::new(hook));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// T1 (carried over from the original gateway tests): pin the
    /// 18h ServerAlive floor in the master spawn argv.
    /// Regression-pin for the anti-leak floor: any change to this
    /// must surface in code review and not be a silent override.
    #[test]
    fn master_argv_pins_18h_serveralive_floor() {
        let argv = build_master_argv(
            &Vec::new(),
            "/tmp/dynrunner-m-test.sock",
            &[],
            "user@host",
        );
        let has_pair = |a: &str, b: &str| {
            argv.windows(2).any(|w| w[0] == a && w[1] == b)
        };
        assert!(
            has_pair("-o", "ServerAliveInterval=60"),
            "missing `-o ServerAliveInterval=60`; argv={argv:?}"
        );
        assert!(
            has_pair("-o", "ServerAliveCountMax=1080"),
            "missing `-o ServerAliveCountMax=1080`; argv={argv:?}"
        );
        assert_eq!(60u64 * 1080, 64_800);
    }

    #[test]
    fn master_argv_includes_master_mode_flags() {
        let argv = build_master_argv(
            &Vec::new(),
            "/tmp/dynrunner-m-test.sock",
            &[],
            "user@host",
        );
        assert!(argv.contains(&"-M".to_string()));
        assert!(argv.contains(&"-N".to_string()));
        assert!(
            !argv.contains(&"-f".to_string()),
            "master argv must not contain `-f` (auth-failure masking); argv={argv:?}"
        );
    }

    #[test]
    fn master_argv_threads_control_path_and_target() {
        let argv = build_master_argv(
            &["-p".to_string(), "2222".to_string()],
            "/tmp/dynrunner-m-42-7.sock",
            &[(1234, 5678)],
            "alice@host",
        );
        assert_eq!(argv.first().map(String::as_str), Some("-p"));
        assert_eq!(argv.get(1).map(String::as_str), Some("2222"));
        assert!(argv.contains(&"ControlPath=/tmp/dynrunner-m-42-7.sock".to_string()));
        assert!(argv.contains(&"-R".to_string()));
        assert!(argv.contains(&"0.0.0.0:5678:localhost:1234".to_string()));
        assert_eq!(argv.last().map(String::as_str), Some("alice@host"));
    }

    #[test]
    fn control_path_fits_sockaddr_un() {
        let cp = generate_master_control_path();
        assert!(
            cp.len() < 108,
            "control path is {} bytes ({:?}) — must stay under 108",
            cp.len(),
            cp,
        );
        assert!(cp.starts_with("/tmp/dynrunner-m-"));
        assert!(cp.ends_with(".sock"));
    }

    #[test]
    fn control_path_unique_across_calls() {
        let a = generate_master_control_path();
        let b = generate_master_control_path();
        assert_ne!(a, b);
    }

    #[test]
    fn control_path_pessimistic_pid_sequence_still_fits() {
        let synthetic = format!("/tmp/dynrunner-m-{}-{}.sock", 9_999_999u32, u64::MAX);
        assert!(
            synthetic.len() < 108,
            "even worst-case PID/seq path is {} bytes ({:?})",
            synthetic.len(),
            synthetic,
        );
    }

    #[test]
    fn parse_master_pid_extracts_from_canonical_output() {
        assert_eq!(parse_master_pid("Master running (pid=12345)\n"), Some(12345));
    }

    #[test]
    fn parse_master_pid_handles_leading_whitespace_and_extra_lines() {
        let out = "  Master running (pid=42)\nsomething else\n";
        assert_eq!(parse_master_pid(out), Some(42));
    }

    #[test]
    fn parse_master_pid_returns_none_when_marker_absent() {
        assert_eq!(parse_master_pid("Stop listening request sent.\n"), None);
        assert_eq!(parse_master_pid("Master running\n"), None);
        assert_eq!(parse_master_pid(""), None);
    }

    #[test]
    fn parse_master_pid_returns_none_on_non_numeric_pid() {
        assert_eq!(parse_master_pid("Master running (pid=abc)"), None);
    }

    #[test]
    fn parse_master_pid_rejects_overflow() {
        assert_eq!(
            parse_master_pid("Master running (pid=99999999999999)"),
            None
        );
    }
}
