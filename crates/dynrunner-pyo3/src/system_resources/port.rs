//! `pick_free_port` — bind/unbind helper for picking a free TCP port.

use pyo3::prelude::*;

/// Bind to TCP port 0, read the OS-assigned port, drop the
/// listener. The caller (e.g. SLURM packaging pipeline) re-binds
/// the same port via the Rust primary coordinator after setting
/// up SSH `-R` forwarding to it; the temp listener is just to
/// claim a free port number.
#[pyfunction]
pub(crate) fn pick_free_port() -> PyResult<u16> {
    let listener = std::net::TcpListener::bind("0.0.0.0:0").map_err(|e| {
        pyo3::exceptions::PyOSError::new_err(format!("pick_free_port: bind failed: {e}"))
    })?;
    let port = listener
        .local_addr()
        .map_err(|e| {
            pyo3::exceptions::PyOSError::new_err(format!(
                "pick_free_port: local_addr failed: {e}"
            ))
        })?
        .port();
    drop(listener);
    Ok(port)
}
