//! PyO3 bindings for the [`dynrunner_gateway`] crate.
//!
//! Each backend (local, ssh) gets its own `Rust*Gateway` pyclass that
//! wraps the Rust `Gateway` impl behind a synchronous Python API. The
//! sync surface mirrors the `dynamic_runner.packaging.gateway.Gateway`
//! Protocol so the Python thin-shim modules can delegate 1:1 without
//! reshaping arguments.

pub(crate) mod local;

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
        GatewayError::CommandFailed(msg) => {
            pyo3::exceptions::PyRuntimeError::new_err(msg)
        }
        GatewayError::TransferFailed(msg) => {
            pyo3::exceptions::PyRuntimeError::new_err(msg)
        }
        GatewayError::Io(e) => pyo3::exceptions::PyOSError::new_err(e.to_string()),
        GatewayError::Other(msg) => pyo3::exceptions::PyRuntimeError::new_err(msg),
    }
}
