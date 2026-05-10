//! Configuration types passed into [`crate::SshMaster::spawn`].
//!
//! Single concern: describe the inputs to `ssh -M -N` without
//! coupling to higher-level types in `dynrunner-gateway`. The shape
//! mirrors `dynrunner-gateway::config::SshConfig` deliberately (the
//! gateway will eventually adopt this crate's [`SshConfig`] in a
//! follow-up); duplicating the type for now keeps the dependency
//! direction one-way:
//!
//!   `dynrunner-driver`  ←  `dynrunner-gateway`
//!
//! never the other way around. The migration plan is to delete the
//! gateway-side struct once the gateway calls into the driver
//! internally.

use std::path::PathBuf;

/// Inputs to `ssh -M -N`. Forwarded ports are passed at construction
/// time so the master spawn argv can include `-R` clauses up-front,
/// avoiding the runtime `ssh -O forward` round-trip.
#[derive(Debug, Clone)]
pub struct SshConfig {
    /// `-p <port>` if non-22.
    pub port: u16,
    /// Final positional target arg (`user@host` or `host`).
    pub target: crate::ssh_target::SshTarget,
    /// `-i <path>`. When set, the master spawn also emits
    /// `IdentitiesOnly=yes` and `IdentityAgent=none` to enforce the
    /// "no agent leakage" contract the framework's auth chain
    /// depends on.
    pub identity_file: Option<PathBuf>,
    /// `-F <path>`. Operator-supplied ssh_config(5) overrides.
    pub config_file: Option<PathBuf>,
    /// `(local_port, remote_port)` pairs — emitted as
    /// `-R 0.0.0.0:<remote>:localhost:<local>` on the master spawn
    /// argv. Order is preserved.
    pub forwarded_ports: Vec<(u16, u16)>,
}

impl SshConfig {
    /// Convenience builder for the common case of a single forwarded
    /// port (or none). Tests and downstream callers without
    /// port-forwarding needs use this path.
    pub fn new(target: impl Into<crate::ssh_target::SshTarget>, port: u16) -> Self {
        Self {
            port,
            target: target.into(),
            identity_file: None,
            config_file: None,
            forwarded_ports: Vec::new(),
        }
    }

    /// Set `-i <path>`.
    pub fn with_identity_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.identity_file = Some(path.into());
        self
    }

    /// Set `-F <path>`.
    pub fn with_config_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.config_file = Some(path.into());
        self
    }

    /// Append a reverse-forward pair.
    pub fn with_forward(mut self, local_port: u16, remote_port: u16) -> Self {
        self.forwarded_ports.push((local_port, remote_port));
        self
    }
}
