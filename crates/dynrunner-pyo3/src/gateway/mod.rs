//! Python adapters for the [`dynrunner_gateway`] backends.
//!
//! Each submodule wraps one concrete `Gateway` impl as a PyO3
//! `#[pyclass]` so Python orchestration code can drive it through the
//! same surface as the original pure-Python gateways. The Python side
//! becomes a thin shim that delegates every method to the wrapper.

pub mod ssh;
