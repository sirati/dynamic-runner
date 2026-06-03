//! The cross-crate "important" (LLM-wake-worthy) tracing marker.
//!
//! Single concern: own the ONE string that marks a tracing event as
//! important. Every emit site (`tracing::info!(target: IMPORTANT_TARGET,
//! …)`) and the `dynrunner-pyo3` stdio gate (`important_stdio_filter`)
//! key on this single const, so the emit target and the filter can never
//! diverge. The Python side mirrors it with the child logger
//! `dynamic_runner.important`.

/// Tracing target marking an event as "important" (LLM-wake-worthy).
///
/// Fixed cross-crate contract, defined here once and re-exported from the
/// crate root. Events emitted at this target reach stdio even when the
/// `dynrunner-pyo3` dual-sink runs in importance mode
/// (`--important-stdio-only`); the full log always keeps everything.
pub const IMPORTANT_TARGET: &str = "dynrunner_important";
