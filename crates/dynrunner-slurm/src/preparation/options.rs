//! Configuration types for the SLURM preparation phase:
//! [`InfoFileReader`] (the connection-info-file bridge trait that lets
//! the caller plug in a `Gateway`-flavoured reader without binding to
//! a concrete gateway impl), [`PreparationOptions`] (the data the
//! pipeline reads to drive ssh-tunnel setup), and [`PrepError`]
//! (every error variant the establishment loop can surface).

use std::future::Future;
use std::time::Duration;

use super::policy::EstablishmentPolicy;

/// Single-method bridge for reading the contents of a connection-info
/// file on the gateway. Returns the raw stdout of `cat <path>` (with
/// trailing newline) on success, or `None` if the file does not yet
/// exist (return code != 0 or empty stdout). This matches the polling
/// shape — the watcher distinguishes "not yet" from "done" by stdout
/// presence, not by error.
pub trait InfoFileReader: Clone + 'static {
    fn read(
        &self,
        path: String,
    ) -> impl Future<Output = Result<Option<String>, PrepError>> + 'static;
}

/// Configuration for a [`SlurmPreparation`] instance.
///
/// Mirrors the fields the Python implementation pulls off the gateway
/// (`host`, `user`, `port`, `auth_options()`) plus the deployment-
/// derived `extra_port_forwards`. Holding these as plain data keeps
/// the preparation crate independent of any specific gateway impl.
#[derive(Debug, Clone)]
pub struct PreparationOptions {
    /// `<base_log_dir>/<run_id>` — info files live in
    /// `<run_log_dir>/connection_info/<secondary_id>.info`.
    pub run_log_dir: String,
    /// Hostname (or LB alias) compute nodes use to ProxyJump back
    /// through. Used verbatim — no `hostname -f` substitution.
    pub gateway_host: String,
    pub gateway_user: Option<String>,
    pub gateway_port: u16,
    /// Output of the gateway's `auth_options()` — `-i <key>
    /// IdentitiesOnly=yes IdentityAgent=none -F <config>` chain.
    /// Empty when neither `--ssh-identity-file` nor `--ssh-config`
    /// was supplied by the user.
    pub auth_options: Vec<String>,
    /// `(local_port, gateway_port)` pairs that fan out as additional
    /// `-R gateway_port:localhost:local_port` on every per-compute
    /// reverse tunnel. Mirrors `TaskDeploymentSpec.extra_port_forwards`.
    pub extra_port_forwards: Vec<(u16, u16)>,
    /// Total deadline for all secondaries to report ready. Default
    /// 600s matches the legacy Python timeout.
    pub setup_timeout: Duration,
    /// Watcher polling cadence; default 2s matches Python.
    pub poll_interval: Duration,
    /// Policy for the establishment-phase rate-limiter + retry. See
    /// [`EstablishmentPolicy`] for field meanings; default values are
    /// safe for the LMU CIP gateway load-balancer (max 4 concurrent
    /// handshakes, 3 attempts at [0, 5s, 15s] backoff, 90s per-tunnel
    /// wall-clock cap).
    pub establishment: EstablishmentPolicy,
    /// Optional OpenSSH `LogLevel` for the per-secondary `ssh -N -R`
    /// reverse-tunnel CHILD processes (diagnostic primitive, #415 face
    /// (a)). `None` keeps OpenSSH's default (`INFO`, banner-only on these
    /// children); `Some("DEBUG1".into())` makes each tunnel child emit the
    /// rekey / `channel ... forwarding` / mux lines a wire-drop
    /// investigation needs — without which the tunnel-child stderr is
    /// banner-only and a fleet-wide-drop cause (sshd rekey, MaxSessions
    /// channel refusal) is invisible. Threaded as config (read once from
    /// the env at the pyo3 boundary) so the argv builder stays pure.
    pub tunnel_child_log_level: Option<String>,
}

impl PreparationOptions {
    pub fn new(
        run_log_dir: String,
        gateway_host: String,
        gateway_user: Option<String>,
        gateway_port: u16,
        auth_options: Vec<String>,
        extra_port_forwards: Vec<(u16, u16)>,
    ) -> Self {
        Self {
            run_log_dir,
            gateway_host,
            gateway_user,
            gateway_port,
            auth_options,
            extra_port_forwards,
            setup_timeout: Duration::from_secs(600),
            poll_interval: Duration::from_secs(2),
            establishment: EstablishmentPolicy::default(),
            tunnel_child_log_level: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PrepError {
    #[error("timeout waiting for secondary connection info: {ready}/{total} ready")]
    Timeout { ready: usize, total: usize },
    #[error("connection-info read failed for {secondary_id}: {message}")]
    InfoRead {
        secondary_id: String,
        message: String,
    },
    #[error("malformed connection-info for {secondary_id}: {message}")]
    InfoParse {
        secondary_id: String,
        message: String,
    },
    #[error("ssh tunnel for {secondary_id} failed to establish (rc={rc:?}): {stderr}")]
    TunnelFailed {
        secondary_id: String,
        rc: Option<i32>,
        stderr: String,
    },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("watcher task panicked: {0}")]
    WatcherPanic(String),
    #[error("watcher result lost: {0}")]
    WatcherLost(String),
}
