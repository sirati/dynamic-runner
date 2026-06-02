//! Periodic-report formatting: delta computation + the inclusion rule.
//!
//! # Single concern
//!
//! Turn a current [`StatsSnapshot`] and the previous-announcement
//! snapshot into the report body text, applying owner-decision C-3's
//! rules:
//!
//! * SUCCESS and each per-failure-type line render `{total}(+{Δ})`,
//!   where Δ is the increase since the last announcement.
//! * A metric is INCLUDED only if it is currently `> 0` AND it CHANGED
//!   since the last announcement; otherwise it is omitted.
//! * If ANY metric was omitted *because it was unchanged* (as opposed
//!   to being zero), the report ends with the line
//!   "Omitted unchanged stats.".
//!
//! The function is pure (snapshot in, `Option<String>` out) so the
//! delta/inclusion rules are unit-testable without a clock, a CRDT, or
//! the tracing sink.

use super::stats::StatsSnapshot;

/// One metric's render decision. Carries the human label, the current
/// total, and the delta versus the previous announcement.
struct MetricLine {
    label: &'static str,
    total: usize,
    delta: usize,
    /// `true` for the success / per-failure-type lines that render the
    /// `{total}(+{Δ})` shape; `false` for the in-flight / waiting /
    /// blocked / ready lines that render a bare `{total}` (they are
    /// level gauges, not monotone counters, so a parenthetical delta
    /// reads as noise).
    show_delta: bool,
}

/// Outcome of rendering a single metric.
enum Rendered {
    /// Render this line.
    Include(String),
    /// Omit because the value is currently zero. Does NOT trigger the
    /// footer.
    OmitZero,
    /// Omit because the value is unchanged since the last announcement.
    /// Triggers the "Omitted unchanged stats." footer.
    OmitUnchanged,
}

impl MetricLine {
    fn render(&self) -> Rendered {
        if self.total == 0 {
            return Rendered::OmitZero;
        }
        if self.delta == 0 {
            return Rendered::OmitUnchanged;
        }
        let body = if self.show_delta {
            format!("{}: {}(+{})", self.label, self.total, self.delta)
        } else {
            format!("{}: {}", self.label, self.total)
        };
        Rendered::Include(body)
    }
}

/// Saturating delta `cur - prev` (a metric should never decrease in
/// steady state, but a CRDT reorder or a reinject could momentarily
/// lower a level gauge; clamp at 0 rather than underflow-panic).
fn delta(cur: usize, prev: usize) -> usize {
    cur.saturating_sub(prev)
}

/// Build the periodic-report body, or `None` if there is nothing
/// worth waking an LLM for (every metric omitted as zero AND nothing
/// was omitted-because-unchanged). A `Some` body is the multi-line
/// text the caller emits on the importance channel.
///
/// `prev` is the snapshot at the LAST announcement (not the last tick):
/// callers advance `prev` only when they actually emit, so an
/// all-omitted tick does not reset the delta baseline.
pub fn render_report(cur: &StatsSnapshot, prev: &StatsSnapshot) -> Option<String> {
    let lines = [
        MetricLine {
            label: "succeeded",
            total: cur.succeeded,
            delta: delta(cur.succeeded, prev.succeeded),
            show_delta: true,
        },
        MetricLine {
            label: "failed (retry)",
            total: cur.fail_retry,
            delta: delta(cur.fail_retry, prev.fail_retry),
            show_delta: true,
        },
        MetricLine {
            label: "failed (oom)",
            total: cur.fail_oom,
            delta: delta(cur.fail_oom, prev.fail_oom),
            show_delta: true,
        },
        MetricLine {
            label: "failed (final)",
            total: cur.fail_final,
            delta: delta(cur.fail_final, prev.fail_final),
            show_delta: true,
        },
        MetricLine {
            label: "unfulfillable",
            total: cur.unfulfillable,
            delta: delta(cur.unfulfillable, prev.unfulfillable),
            show_delta: true,
        },
        MetricLine {
            label: "in-flight",
            total: cur.in_flight,
            delta: delta(cur.in_flight, prev.in_flight),
            show_delta: false,
        },
        MetricLine {
            label: "waiting on deps",
            total: cur.waiting_on_deps,
            delta: delta(cur.waiting_on_deps, prev.waiting_on_deps),
            show_delta: false,
        },
        MetricLine {
            label: "blocked (upstream unfulfillable)",
            total: cur.blocked,
            delta: delta(cur.blocked, prev.blocked),
            show_delta: false,
        },
        MetricLine {
            label: "ready in queue",
            total: cur.ready_in_queue,
            delta: delta(cur.ready_in_queue, prev.ready_in_queue),
            show_delta: false,
        },
    ];

    let mut body: Vec<String> = Vec::new();
    let mut any_omitted_unchanged = false;
    for line in &lines {
        match line.render() {
            Rendered::Include(s) => body.push(s),
            Rendered::OmitZero => {}
            Rendered::OmitUnchanged => any_omitted_unchanged = true,
        }
    }

    // Nothing INCLUDED → there is nothing wake-worthy this period (every
    // nonzero metric was unchanged, or every metric was zero). Stay
    // silent: a footer-only "Omitted unchanged stats." report would
    // wake an LLM to announce that nothing changed, which defeats the
    // whole point of the importance channel. The footer is meaningful
    // ONLY as a suffix to at least one real, changed line.
    if body.is_empty() {
        return None;
    }

    if any_omitted_unchanged {
        body.push("Omitted unchanged stats.".to_string());
    }

    Some(body.join("\n"))
}
