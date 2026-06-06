//! Cold-start run-config FETCH driver: dial the bootstrap primary, ask
//! it for the cluster-wide `forwarded_argv`, hand it back to the Python
//! bootstrap shim to splice onto `sys.argv` before `run()`.
//!
//! # Concern
//!
//! Single PyO3 entry point — `#[pyfunction] fetch_run_config` — for the
//! framework bootstrap shim's pre-`run()` fetch leg. A freshly-spawned
//! secondary (or a respawn) holds the bootstrap argv but not the
//! consumer's run-config; it dials the primary, pulls the run-config over
//! the mesh, and exits non-zero on failure (respawn-eligible).
//!
//! The flow is one straight line:
//!
//! 1. Resolve `primary_url` to a `SocketAddr` (same `tcp://` / `ws://` /
//!    `wss://` / bare-`host:port` acceptance as the secondary dispatcher).
//! 2. Dial the bootstrap primary via
//!    [`crate::managers::transport_factory::dial_secondary_mesh`] — the
//!    real WSS-retry loop, the peer-overlay selection, and the bootstrap-
//!    wire fold (`register_primary_link`) that makes the primary an
//!    ordinary mesh member. Dial budget ≥ the unconfigured-deadline (audit
//!    D4): a still-starting primary must be waited out, NOT the 10s
//!    rendezvous budget.
//! 3. Drive [`dynrunner_protocol_primary_secondary::PeerTransport::fetch_run_config`]
//!    on the folded mesh — an UNWELCOMED `RequestRunConfig` → `RunConfig`
//!    round-trip that returns the `forwarded_argv`. No `SecondaryWelcome`
//!    / cert exchange (audit D2): the real join happens later inside the
//!    spliced `run()`.
//! 4. Return the argv to Python. On any failure (dial exhausted, no reply
//!    within budget) raise a `RuntimeError` — the shim exits non-zero.
//!
//! # Module boundary
//!
//! This module owns ONLY the driver glue. The dial + retry + overlay +
//! wire-fold lives in `transport_factory`; the fetch RPC rendezvous lives
//! in the protocol crate's `fetch_run_config` trait method. This wrapper
//! resolves the URL, calls the factory, drives the trait method on a
//! tokio `LocalSet`, and maps the result to a `PyResult`.
//!
//! # File split
//!
//! `mod.rs` owns the Python-facing `#[pyfunction]` signature (the API
//! boundary); `run.rs` owns the backend-opaque dial + fetch
//! implementation (`drive_fetch_run_config`).

use pyo3::prelude::*;

use crate::config::distributed::DistributedConfig;

mod run;

/// Fetch the cluster-wide `forwarded_argv` from the bootstrap primary
/// over the mesh.
///
/// `primary_url` is the bootstrap primary's dial endpoint (the same value
/// a normal secondary receives); `secondary_id` is this node's logical id
/// — BOTH the CN baked into its mesh cert AND the unicast return address
/// stamped on the `RequestRunConfig` so the primary's reply routes back.
/// `distributed_config` supplies the dial budget (the unconfigured-
/// deadline), the retry delay, and the peer-overlay selection; omitted, a
/// default config is used (600s deadline).
///
/// Returns the run-config token list the shim splices onto `sys.argv`.
/// Raises `RuntimeError` on dial exhaustion or fetch timeout (the shim
/// exits non-zero and is respawn-eligible).
#[pyfunction]
#[pyo3(signature = (primary_url, secondary_id, distributed_config = None))]
pub(crate) fn fetch_run_config(
    py: Python<'_>,
    primary_url: String,
    secondary_id: String,
    distributed_config: Option<DistributedConfig>,
) -> PyResult<Vec<String>> {
    let distributed_config = distributed_config.unwrap_or_default();
    run::drive_fetch_run_config(py, primary_url, secondary_id, distributed_config)
}
