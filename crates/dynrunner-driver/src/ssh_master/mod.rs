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
//! # Lifetime model
//!
//! OpenSSH with `ControlPersist=yes` *always* forks-and-detaches at
//! the end of the handshake: the `ssh -M -N` process we spawn (the
//! "launcher") exits 0 within ~120ms; a daemon child becomes the
//! persistent master, reparented to `systemd --user` (or PID 1 /
//! init), in a different session. The daemon is the process that
//! responds to control-socket commands (`ssh -O exit` /
//! `ssh -O check` / per-channel ssh+scp invocations) — *not* the
//! launcher. Tracking the launcher's `Child` as the master lifetime
//! anchor was the bug behind a silent regression of bug (g):
//! `Child::kill()` and `kill_on_drop(true)` operate on the launcher
//! zombie, so dropping the master without an explicit teardown
//! leaked the daemon.
//!
//! Post-fix: we discover the daemon PID via `ssh -O check` after
//! the control socket appears, reap the launcher zombie immediately,
//! and hold the daemon PID as the lifetime anchor. Drop sends
//! SIGTERM (then SIGKILL after a brief grace) to the daemon
//! directly via `nix::sys::signal::kill`. The watcher polls
//! `kill(daemon_pid, 0)` so the "master died" log fires for actual
//! daemon death.
//!
//! # Drop-on-abort contract (locked design point (g))
//!
//! Drop runs only on graceful Rust-side drop. Process abort
//! (SIGKILL, segfault, OOM-kill) leaves the daemon alive; the 18h
//! ServerAlive ladder (`ServerAliveInterval=60 ×
//! ServerAliveCountMax=1080`) is the only floor in that case.
//! Consumers needing crash-safe daemon cleanup should rely on the
//! OpenSSH-side keepalive contract, not on `Drop`.
//!
//! # Layout
//!
//! Sub-modules:
//! - [`lifecycle`]: `spawn` + `adopt` constructors.
//! - [`teardown`]: `disconnect` + per-branch helpers + `add_forward`.
//! - [`argv`]: argv builders + control-path utilities.
//! - [`probe`]: daemon-PID discovery via `ssh -O check`.
//! - [`watcher`]: master-watcher thread + `terminate_daemon_blocking`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::error::SshMasterError;
use crate::ssh_target::SshTarget;

pub(crate) mod argv;
pub(crate) mod lifecycle;
pub(crate) mod probe;
pub(crate) mod teardown;
pub(crate) mod watcher;

// `terminate_daemon_blocking` stays `pub(super)` in `watcher` so the
// kill-ladder remains internal to the `ssh_master` module tree — only
// `run_kill_ladder` above is allowed to invoke it, gated by the
// `test_kill_hook` substitution path.
use watcher::terminate_daemon_blocking;

#[cfg(test)]
mod tests;

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
    /// having to keep the full `SshConfig` around (which would risk
    /// leaking identity-file paths into telemetry).
    pub(super) target: SshTarget,
    /// `-p <port>` arg, or 22 for the default. The session layer
    /// threads it into per-call ssh/scp invocations; storing on the
    /// master simplifies the Session API.
    pub(super) port: u16,
    /// Per-instance auth flags (`-i`, `IdentitiesOnly`,
    /// `IdentityAgent`, `-F`). Only *we* use these on `ssh -O
    /// check`/`ssh -O exit`/`ssh -O cancel` — the session layer
    /// holds its own copy of equivalent flags.
    pub(super) auth_flags: Vec<String>,
    /// Path to the Unix-domain control socket.
    pub(super) control_path: PathBuf,
    /// PID of the persistent `ControlPersist` daemon. `None` for
    /// `adopt()`-constructed instances.
    pub(super) daemon_pid: Option<u32>,
    /// **Last-known** daemon PID, preserved across watcher-observed
    /// invalidation (locked point (h.1): `master_pid()` returns this
    /// value, not `None`, after the watcher fires).
    pub(super) last_known_pid: Option<u32>,
    /// `true` once the watcher has observed daemon death OR the
    /// kill-ladder has run to completion (locked points (h.2), (h.3)).
    pub(super) invalidated: Arc<AtomicBool>,
    /// Cancellation flag for the master-watcher std::thread.
    pub(super) watcher_cancel: Arc<AtomicBool>,
    /// Handle to the master-watcher std::thread. The thread is
    /// deliberately *not* a tokio task — see crate docs.
    pub(super) watcher_thread: Option<std::thread::JoinHandle<()>>,
    /// `(local_port, remote_port)` pairs registered via the spawn
    /// argv or runtime `ssh -O forward`. On `disconnect()` of an
    /// adopt-master we issue `ssh -O cancel -R 0.0.0.0:<remote>:
    /// localhost:<local>` per entry — partial cleanup, not
    /// termination, per locked design point (b).
    pub(super) forwarded_ports: Vec<(u16, u16)>,
    /// `true` if this instance was constructed via `spawn()` and
    /// therefore owns the daemon. `false` for `adopt()`.
    pub(super) is_spawned: bool,
    /// When the master was spawned (or, for adopt, when adoption
    /// succeeded).
    pub(super) spawn_timestamp: Instant,
    /// **Hook** used only by tests to inject a faked kill-ladder
    /// outcome without actually sending signals.
    pub(super) test_kill_hook: Option<TestKillHook>,
}

pub(super) type TestKillHook =
    Box<dyn Fn(u32, &SshTarget) -> Result<(), SshMasterError> + Send + Sync>;

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

    /// Forwarded port pairs `(local, remote)` registered at spawn or
    /// later via `add_forward`. Telemetry/diagnostic surface.
    pub fn forwarded_ports(&self) -> &[(u16, u16)] {
        &self.forwarded_ports
    }

    /// `true` if this master was constructed via `spawn()` (owns
    /// the daemon). `false` for `adopt()` (the daemon is not ours).
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

    /// Internal: build a `MasterDied` error stamped with the current
    /// observation timestamp.
    pub(super) fn master_died_err(&self) -> SshMasterError {
        SshMasterError::MasterDied {
            target: self.target.clone(),
            last_known_pid: self.last_known_pid,
            spawn_timestamp: self.spawn_timestamp,
            observation_timestamp: Instant::now(),
        }
    }

    /// Internal: production runs the real SIGTERM→SIGKILL ladder via
    /// [`terminate_daemon_blocking`]; tests can swap in
    /// `test_kill_hook` to simulate an UnkillableMaster outcome
    /// without sending real signals.
    #[inline]
    pub(super) fn run_kill_ladder(&self, pid: u32) -> Result<(), SshMasterError> {
        if let Some(hook) = &self.test_kill_hook {
            return hook(pid, &self.target);
        }
        terminate_daemon_blocking(pid, &self.target)
    }

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
    #[doc(hidden)]
    pub fn install_test_kill_hook(
        &mut self,
        hook: impl Fn(u32, &SshTarget) -> Result<(), SshMasterError> + Send + Sync + 'static,
    ) {
        self.test_kill_hook = Some(Box::new(hook));
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
