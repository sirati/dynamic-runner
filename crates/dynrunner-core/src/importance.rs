//! The cross-crate "important" (LLM-wake-worthy) tracing marker.
//!
//! Single concern: own the ONE string that marks a tracing event as
//! important. Every emit site (`tracing::info!(target: IMPORTANT_TARGET,
//! …)`) and the `dynrunner-pyo3` stdio gate (`important_stdio_filter`)
//! key on this single const, so the emit target and the filter can never
//! diverge. The Python side mirrors it with the child logger
//! `dynamic_runner.important`.
//!
//! The sibling [`OBSERVER_TASK_TARGET`] carries the per-task narration
//! that is operator-visible in the default (non-importance) stdio mode
//! but suppressed FROM the importance-mode stdio sink. The full log
//! sinks are target-agnostic and record both targets unconditionally
//! (TRACE forensic record).

/// Tracing target marking an event as "important" (LLM-wake-worthy).
///
/// Fixed cross-crate contract, defined here once and re-exported from the
/// crate root. Events emitted at this target reach stdio even when the
/// `dynrunner-pyo3` dual-sink runs in importance mode
/// (`--important-stdio-only`); the full log always keeps everything.
pub const IMPORTANT_TARGET: &str = "dynrunner_important";

/// Tracing target for the observer's per-task INFO narration arms.
///
/// Per-task assign/complete/state-change lines are operator-visible in
/// the default stdio mode but must NOT flood the importance-mode stdio
/// sink (a 46k-task build phase would emit tens of thousands of lines
/// past `--important-stdio-only`'s wake-worthy contract). They emit at
/// this dedicated target so the `dynrunner-pyo3` importance gate
/// (`important_stdio_filter`, an allow-list keyed on
/// [`IMPORTANT_TARGET`]) rejects them from stdio under the flag, while
/// the default non-bridge stdio gate admits them. The full log sinks
/// stay target-agnostic and record every line at TRACE.
///
/// Per-task FAILURE narration (terminal ERROR, recoverable WARN, oom
/// WARN) stays on [`IMPORTANT_TARGET`] — those are wake-worthy and must
/// reach stdio under the flag.
pub const OBSERVER_TASK_TARGET: &str = "dynrunner_observer_task";
