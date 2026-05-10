//! Public error type for [`crate::ssh_master::SshMaster`].
//!
//! Single concern: typed failure modes for SSH master lifecycle. Per
//! the design call's locked point (c), this enum exhausts the
//! observable failure set across `spawn()`, `adopt()`, `disconnect()`,
//! and `Drop`. Per peer guidance on payload shape, every variant that
//! mentions the remote target carries an [`SshTarget`] (newtype over
//! the user@host[:port] form) — never a serialised [`crate::SshConfig`]
//! — so error formatting cannot leak identity-file or
//! agent-socket paths into telemetry.
//!
//! Variants:
//! - `SpawnFailed { source }` — `ssh -M -N` could not be invoked at
//!   all (e.g. `ssh` binary missing). Wraps the underlying I/O error.
//! - `ControlSocketTimeout` — the launcher started but the control
//!   socket never appeared within the configured deadline (10s).
//! - `HandshakeRefused` — the launcher exited with non-zero status
//!   before the control socket appeared (auth / network refusal).
//! - `MasterPidProbeFailed` — `ssh -O check` against a freshly-bound
//!   socket either exited non-zero or produced output without the
//!   `Master running (pid=N)` marker.
//! - `MasterDied { target, last_known_pid, spawn_timestamp,
//!   observation_timestamp }` — the watcher observed the daemon PID
//!   gone via `kill(pid, 0) == ESRCH`. Operations after invalidation
//!   surface this variant. Carries the timestamps so post-mortem
//!   tooling can correlate against external death-cause signals
//!   (e.g. an OOM-kill in dmesg).
//! - `UnkillableMaster { target, last_known_pid, kill_ladder_reached }`
//!   — the SIGTERM→SIGKILL ladder ran to completion without observing
//!   ESRCH. Surfaces from `disconnect()` (per locked point (j),
//!   panic-in-Drop is prohibited; Drop only logs).
//! - `MasterAdoptFailed { control_path, reason }` — the three
//!   construction-time fail-fast checks of `adopt()` rejected the
//!   path (length >= 108 / not a socket / `ssh -O check` did not
//!   answer).
//! - `Other(String)` — escape hatch for unanticipated failure modes;
//!   we surface the message verbatim and accept the tradeoff that
//!   string-matching on it is brittle.

use std::path::PathBuf;
use std::time::Instant;

use crate::ssh_target::SshTarget;

/// Step of the SIGTERM→SIGKILL ladder reached when an
/// [`SshMasterError::UnkillableMaster`] was raised. Lets the caller
/// distinguish "we gave up on SIGTERM" (operator can retry SIGKILL
/// out-of-band) from "even SIGKILL did not stick" (kernel-level
/// fault — the daemon must be reaped by init).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillLadder {
    /// SIGTERM was sent but the grace window expired before the
    /// daemon exited. Variant exists for completeness — in practice,
    /// `disconnect()` always escalates to SIGKILL on grace expiry, so
    /// this is the path you reach mid-ladder, not the final state.
    SigtermSent,
    /// SIGTERM grace expired, SIGKILL was sent, and we observed ESRCH
    /// shortly after — i.e. the kill ladder *succeeded*, just at the
    /// SIGKILL step rather than the SIGTERM step. Only surfaces from
    /// telemetry / tests; not a failure state on its own.
    SigkillSent,
    /// SIGKILL was sent and the post-SIGKILL existence probe still
    /// returned `Ok(())` (process alive, not ESRCH). This is the
    /// "kernel won't let us go any harder" state — the operator must
    /// reap the daemon out-of-band.
    SigkillButPidStillExists,
}

#[derive(Debug, thiserror::Error)]
pub enum SshMasterError {
    /// `ssh -M -N` could not be spawned. `source` is the underlying
    /// `std::io::Error` from the launcher subprocess invocation.
    #[error("failed to spawn SSH master launcher: {source}")]
    SpawnFailed {
        #[source]
        source: std::io::Error,
    },

    /// The launcher started but the control socket never appeared
    /// within the configured deadline. Caused by an unresponsive
    /// remote sshd, a network black hole, or an authentication
    /// prompt the launcher is hanging on.
    #[error(
        "SSH master timed out establishing control socket. \
         Pass --ssh-config <path> for ssh_config(5) overrides if a \
         host-key / agent / identity directive needs adjusting."
    )]
    ControlSocketTimeout,

    /// The launcher exited with non-zero status before the control
    /// socket appeared. Almost always an authentication or network
    /// refusal at the sshd handshake. The launcher's stderr is
    /// captured upstream where invocation context is available.
    #[error(
        "SSH master handshake refused (launcher exited non-zero). \
         Pass --ssh-config <path> for ssh_config(5) overrides if a \
         host-key / agent / identity directive needs adjusting."
    )]
    HandshakeRefused,

    /// `ssh -O check` against a freshly-bound socket either exited
    /// non-zero or produced output that didn't include
    /// `Master running (pid=N)`. Either the OpenSSH version changed
    /// the format, or the master crashed between socket-bind and
    /// our probe.
    #[error("SSH master PID probe failed: ssh -O check did not return a parseable PID")]
    MasterPidProbeFailed,

    /// The watcher observed the daemon PID gone via
    /// `kill(pid, 0) == ESRCH`. The master was alive at some point
    /// (we successfully spawned/adopted it) and is no longer; its
    /// last known PID is preserved for diagnostics. Operations
    /// against the master after invalidation surface this variant.
    #[error(
        "SSH master to {target} died unexpectedly \
         (last_known_pid={last_known_pid:?})"
    )]
    MasterDied {
        target: SshTarget,
        /// PID the master had at the most recent `master_pid()` /
        /// post-spawn discovery. `Option` because adopt() may have
        /// been constructed without a successful PID probe (in which
        /// case this variant is not how the failure surfaces — fail-
        /// fast in `adopt()` raises `MasterAdoptFailed` before any
        /// PID is recorded).
        last_known_pid: Option<u32>,
        /// When the master was spawned (or adopted-and-probed). Lets
        /// post-mortem tooling correlate against external kill-
        /// reason signals (dmesg / journalctl) by timestamp.
        spawn_timestamp: Instant,
        /// When the watcher first observed the death. The gap to
        /// `spawn_timestamp` is the master's effective lifetime.
        observation_timestamp: Instant,
    },

    /// The SIGTERM→SIGKILL ladder ran to completion without observing
    /// ESRCH. Per locked point (j), this is the only path through
    /// which the unkillable-master condition surfaces — Drop never
    /// panics; it logs via `tracing::error!` and returns. Operators
    /// see the variant only via `disconnect()`.
    #[error(
        "SSH master to {target} survived SIGKILL \
         (last_known_pid={last_known_pid}, \
          kill_ladder_reached={kill_ladder_reached:?})"
    )]
    UnkillableMaster {
        target: SshTarget,
        last_known_pid: u32,
        kill_ladder_reached: KillLadder,
    },

    /// `adopt()` rejected the supplied path at construction time.
    /// `reason` describes which of the three checks failed
    /// (path length, not-a-socket, or `ssh -O check` non-response).
    #[error("failed to adopt SSH master at {control_path:?}: {reason}")]
    MasterAdoptFailed {
        control_path: PathBuf,
        reason: String,
    },

    /// Escape hatch. String-matching on this is brittle; prefer
    /// adding a typed variant when a recurring failure mode emerges.
    #[error("{0}")]
    Other(String),
}

impl SshMasterError {
    /// Helper: wrap a `std::io::Error` from the launcher spawn.
    /// Slightly nicer than the [`From`] impl because it makes the
    /// callsite read as "spawn failed" rather than "io::Error became
    /// SpawnFailed".
    pub(crate) fn spawn_failed(source: std::io::Error) -> Self {
        Self::SpawnFailed { source }
    }

    /// Helper: wrap an `adopt()`-time rejection.
    pub(crate) fn adopt_failed(control_path: PathBuf, reason: impl Into<String>) -> Self {
        Self::MasterAdoptFailed {
            control_path,
            reason: reason.into(),
        }
    }
}
