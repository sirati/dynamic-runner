//! The cross-crate "important" (LLM-wake-worthy) tracing marker.
//!
//! Single concern: own the ONE string that marks a tracing event as
//! important. Every emit site (`tracing::info!(target: IMPORTANT_TARGET,
//! …)`) and the `dynrunner-pyo3` stdio gate (`important_stdio_filter`)
//! key on this single const, so the emit target and the filter can never
//! diverge. The Python side mirrors it with the child logger
//! `dynamic_runner.important`.
//!
//! The sibling [`OBSERVER_TASK_TARGET`] carries the HIGH-VOLUME
//! narration (per-task assign / complete / fail / state-change AND
//! consumer-flagged custom-message Posted / Handled / Failed) that is
//! operator-visible in the default (non-importance) stdio mode but
//! suppressed FROM the importance-mode stdio sink — the rate-limited
//! aggregator rollups on [`IMPORTANT_TARGET`] are the wake signal for
//! the suppressed streams. The full log sinks are target-agnostic and
//! record both targets unconditionally (TRACE forensic record). See
//! [`high_volume_target`] for the one-line mapping every emit site
//! routes through (#583/#587).

/// Tracing target marking an event as "important" (LLM-wake-worthy).
///
/// Fixed cross-crate contract, defined here once and re-exported from the
/// crate root. Events emitted at this target reach stdio even when the
/// `dynrunner-pyo3` dual-sink runs in importance mode
/// (`--important-stdio-only`); the full log always keeps everything.
pub const IMPORTANT_TARGET: &str = "dynrunner_important";

/// Tracing target for HIGH-VOLUME narration arms (#583/#587).
///
/// High-volume narration is operator-visible in the default stdio mode
/// but must NOT flood the importance-mode stdio sink (a 46k-task build
/// phase would emit tens of thousands of lines past
/// `--important-stdio-only`'s wake-worthy contract; an asm-dataset
/// untuned-packages run fires thousands of per-task terminal-failure
/// ERRORs; a per-spawn-batch `dep_graph_spawn` custom-message stream
/// fires 621 batches × 2 lines = ~1242 lines per run). These arms emit
/// at this dedicated target so the `dynrunner-pyo3` importance gate
/// (`important_stdio_filter`, an allow-list keyed on
/// [`IMPORTANT_TARGET`]) rejects them from stdio under the flag,
/// while the default non-bridge stdio gate admits them. The full log
/// sinks stay target-agnostic and record every line at TRACE.
///
/// Members of the high-volume class today:
///   - Per-task narration (assign / complete / state-change INFO,
///     recoverable WARN, oom WARN, terminal-failure ERROR — #573 and
///     #587).
///   - Consumer-flagged custom-message narration (Posted / Handled /
///     Failed; consumer sets `is_high_volume=True` at
///     `SecondaryHandle.send_to_primary` — #583).
///
/// Wake signal on `IMPORTANT_TARGET`:
///   - Per-task failures: the rate-limited `ErrorAggregationPolicy`
///     rollup (`observer::failure_response::aggregation`) emits the
///     per-window "task failures (aggregated, last 60s): …" line.
///   - Custom-message activity: the sibling
///     `observer::failure_response::custom_message_activity` rollup
///     emits the per-window "custom message activity (aggregated,
///     last 60s): N posts, K handled, J failed" line.
pub const OBSERVER_TASK_TARGET: &str = "dynrunner_observer_task";

/// Choose the tracing target for an emit whose volume class is fixed
/// by a per-narration-kind boolean — a `const fn` so a STATIC volume
/// class (the framework-classified per-task narrators that pass
/// `true` literally) compiles to the right constant target at the
/// emit site. The two targets stay the file-private cross-crate
/// strings above; no narrator quotes the literal target name.
///
/// `true` ⇒ [`OBSERVER_TASK_TARGET`] (suppressed under
/// `--important-stdio-only`; full log captures it at TRACE);
/// `false` ⇒ [`IMPORTANT_TARGET`] (wake-worthy on every sink).
///
/// For DYNAMIC volume classes (consumer-flagged custom messages where
/// the boolean rides the event) the `tracing::*!` macros require a
/// `&'static str` literal-or-const for `target:`, which a runtime
/// `event.is_high_volume` cannot satisfy. Use the sibling
/// [`crate::narrate_routed!`] macro: it owns the one-time
/// target-branch so every site stays a single call (the macro itself
/// is the SINGLE OWNER of the `is_high_volume → target` mapping for
/// runtime-decided cases — narrators never spell the if).
#[inline]
pub const fn high_volume_target(is_high_volume: bool) -> &'static str {
    if is_high_volume {
        OBSERVER_TASK_TARGET
    } else {
        IMPORTANT_TARGET
    }
}

/// Emit a tracing event whose target is picked by a RUNTIME
/// `is_high_volume` boolean (#583/#587). The `tracing::*!` macros
/// require `target:` to be const-evaluable, so a dynamic per-event
/// flag (custom-message Posted / Handled / Failed) cannot be handed
/// to one emit — this macro owns the one-time target branch so
/// narration sites stay one call per level/arm and the
/// `is_high_volume → target` mapping has ONE owner.
///
/// Shape: `narrate_routed!(<level>, <is_high_volume_expr>,
/// <fields…>, <fmt>, <fmt_args…>);` — identical to a
/// `tracing::<level>!(target: …, fields…, fmt, fmt_args…)` modulo
/// the leading volume-class boolean.
///
/// Level token may be one of `info`, `warn`, `error`, `debug`,
/// `trace` (the standard tracing levels). The macro never spells the
/// target string; the underlying targets stay the
/// [`IMPORTANT_TARGET`] / [`OBSERVER_TASK_TARGET`] constants.
#[macro_export]
macro_rules! narrate_routed {
    ($level:ident, $is_high_volume:expr, $($rest:tt)*) => {{
        if $is_high_volume {
            ::tracing::$level!(target: $crate::OBSERVER_TASK_TARGET, $($rest)*);
        } else {
            ::tracing::$level!(target: $crate::IMPORTANT_TARGET, $($rest)*);
        }
    }};
}
