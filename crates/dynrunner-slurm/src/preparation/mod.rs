//! SLURM preparation phase: SSH reverse-tunnel lifecycle.
//!
//! Owned concern: spawning and tearing down `ssh -N -R` subprocesses
//! that bridge each compute node back to the primary's QUIC port. The
//! preparation watches per-secondary connection-info files (URI form,
//! one line `<scheme>://<host>:<port>` produced by the wrapper script)
//! through a caller-supplied [`InfoFileReader`], and once a secondary
//! reports its hostname + tunnel port, opens the matching SSH
//! ProxyJump tunnel.
//!
//! Async shape: each per-secondary watcher runs as an independent
//! `tokio::task::spawn_local` task, communicating its outcome through
//! a `oneshot::Sender`. The coordinator gathers all receivers under a
//! single outer `tokio::time::timeout` (default 600s) — this avoids
//! the cancel-safety hazard of putting a `recv` arm inside a `select!`
//! that also drives a timer (see
//! `crates/dynrunner-manager-distributed/src/secondary/setup.rs:76-96`
//! for the canonical cautionary tale). On timeout the coordinator
//! [`AbortHandle::abort`]s outstanding watchers; cleanup() walks the
//! shared subprocess vector populated by watchers and terminates any
//! ssh -R that escaped the deadline.
//!
//! No gateway abstraction is bound here at the type level: the Python
//! bridge needs to call back into a Python `gateway.execute_command()`
//! to read the info files, and a callback / trait-with-single-method
//! is the minimum surface for that. The auth-options chain
//! (`-i`/`IdentitiesOnly`/`IdentityAgent=none` / `-F config`) is
//! passed in as a `Vec<String>` from the caller — single source of
//! truth lives on the gateway, not duplicated here.
//!
//! Layout:
//! - [`options`] — `InfoFileReader`, `PreparationOptions`, `PrepError`.
//! - [`policy`] — `EstablishmentPolicy` (rate-limiter + retry + per-
//!   tunnel wall-clock cap).
//! - [`pipeline`] — `SlurmPreparation` (the shared per-cohort state +
//!   `setup_ssh_tunnels`, `establish_one_tunnel`, `cleanup`).
//! - [`establish`] — the per-tunnel watcher / retry / backoff loop
//!   shared by the cohort and respawn paths.
//! - [`store`] — the `TunnelStore` commit/drain seam: the append-only
//!   `SharedTunnelVec` (cohort + respawn) and the per-secondary
//!   `PerSecondaryTunnelRegistry` (observer-reconnect liveness gate).
//! - [`ssh`] — low-level argv construction, `Command::spawn`, liveness
//!   verification, and SIGTERM/SIGKILL teardown.
//! - [`io`] — `read_peer_info_file` (late-joiner bootstrap entry
//!   point) + test-only `parse_connection_uri` shim.
//! - [`tests`] — module-internal tests, split per concern into
//!   sub-files (`uri_parsing`, `pipeline`, `ssh`, `establish`,
//!   `respawn`).
//!
//! [`pipeline`] sits just above the 300-line target because the
//! `SlurmPreparation` impl is one cohesive concern (cohort setup +
//! single-respawn establishment + cleanup all share interior-mutable
//! state); splitting it would just shuffle the lock-acquisition
//! boilerplate without splitting concerns.

mod establish;
mod io;
mod options;
mod pipeline;
mod policy;
mod ssh;
mod store;
#[cfg(test)]
mod tests;

pub use io::read_peer_info_file;
pub use options::{InfoFileReader, PrepError, PreparationOptions};
pub use pipeline::SlurmPreparation;
pub use policy::EstablishmentPolicy;
