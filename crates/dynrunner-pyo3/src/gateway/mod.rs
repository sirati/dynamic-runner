//! PyO3 adapters for the [`dynrunner_gateway`] crate.
//!
//! Each backend (local, ssh) gets its own `Rust*Gateway` pyclass that
//! wraps the Rust `Gateway` impl behind a synchronous Python API. The
//! sync surface mirrors the `dynamic_runner.packaging.gateway.Gateway`
//! Protocol so the Python thin-shim modules can delegate 1:1 without
//! reshaping arguments.

pub mod local;
pub mod ssh;

use dynrunner_gateway::traits::GatewayError;
use pyo3::PyErr;

/// Map a [`GatewayError`] to the Python exception type that callers
/// historically observed from the equivalent Python gateway. Keeping the
/// mapping centralised means every backend reports failures uniformly,
/// and the Python thin-shims do not need to know about Rust error
/// variants.
pub(crate) fn gateway_error_to_py(err: GatewayError) -> PyErr {
    match err {
        GatewayError::NotConnected => {
            pyo3::exceptions::PyRuntimeError::new_err("Gateway not connected")
        }
        GatewayError::CommandFailed(msg) => pyo3::exceptions::PyRuntimeError::new_err(msg),
        // Pre-migration Python gateways raised `RuntimeError` for copy
        // failures (`File copy failed: ...` locally, `SCP failed: ...`
        // / `SCP download failed: ...` over ssh). `Io` (mkdir in
        // `create_directory`, etc.) keeps mapping to `OSError` since
        // those callsites observed the underlying `OSError` directly.
        GatewayError::CopyFailed(msg) => pyo3::exceptions::PyRuntimeError::new_err(msg),
        GatewayError::Io(e) => pyo3::exceptions::PyOSError::new_err(e.to_string()),
        GatewayError::Other(msg) => pyo3::exceptions::PyRuntimeError::new_err(msg),
    }
}
