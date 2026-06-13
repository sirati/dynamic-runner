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

/// The render shape of a metric — counter, gauge, or occupancy ratio.
/// Each shape owns its own "currently present", "changed since the last
/// announcement", and body rendering, so the inclusion rule
/// ([`MetricLine::render`]) is ONE uniform path (present-AND-changed)
/// rather than per-line `if` branches.
enum MetricShape {
    /// A monotone counter rendered `{total}(+{Δ})` — the success /
    /// per-failure-type lines. Present iff `total > 0`; changed iff
    /// `delta > 0`.
    Counter { total: usize, delta: usize },
    /// A level gauge rendered as a bare `{total}` — the in-flight /
    /// waiting / blocked / ready lines (a parenthetical delta on a
    /// gauge reads as noise). Present iff `total > 0`; changed iff
    /// `delta > 0`.
    Gauge { total: usize, delta: usize },
    /// An occupancy ratio rendered `{busy}/{total}`. Per the spec the
    /// inclusion rule is: "currently >0" = `busy > 0`; "changed" =
    /// either `busy` OR `total` differs from the last announcement.
    Ratio {
        busy: usize,
        total: usize,
        prev_busy: usize,
        prev_total: usize,
    },
}

/// One metric's render decision. Carries the human label and the shape
/// that drives its inclusion + body.
struct MetricLine {
    label: &'static str,
    shape: MetricShape,
}

/// Outcome of rendering a single metric.
enum Rendered {
    /// Render this line.
    Include(String),
    /// Omit because the metric is not currently present (its numerator
    /// is zero). Does NOT trigger the footer.
    OmitZero,
    /// Omit because the value is unchanged since the last announcement.
    /// Triggers the "Omitted unchanged stats." footer.
    OmitUnchanged,
}

impl MetricShape {
    /// `true` while the metric is worth reporting at all (numerator
    /// `> 0`): counter/gauge `total`, ratio `busy`.
    fn present(&self) -> bool {
        match self {
            MetricShape::Counter { total, .. } | MetricShape::Gauge { total, .. } => *total > 0,
            MetricShape::Ratio { busy, .. } => *busy > 0,
        }
    }

    /// `true` if the metric moved since the last announcement: a
    /// counter/gauge delta, or either component of a ratio.
    fn changed(&self) -> bool {
        match self {
            MetricShape::Counter { delta, .. } | MetricShape::Gauge { delta, .. } => *delta > 0,
            MetricShape::Ratio {
                busy,
                total,
                prev_busy,
                prev_total,
            } => busy != prev_busy || total != prev_total,
        }
    }

    fn body(&self, label: &str) -> String {
        match self {
            MetricShape::Counter { total, delta } => format!("{label}: {total}(+{delta})"),
            MetricShape::Gauge { total, .. } => format!("{label}: {total}"),
            MetricShape::Ratio { busy, total, .. } => format!("{label}: {busy}/{total}"),
        }
    }
}

impl MetricLine {
    fn render(&self) -> Rendered {
        if !self.shape.present() {
            return Rendered::OmitZero;
        }
        if !self.shape.changed() {
            return Rendered::OmitUnchanged;
        }
        Rendered::Include(self.shape.body(self.label))
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
            shape: MetricShape::Counter {
                total: cur.succeeded,
                delta: delta(cur.succeeded, prev.succeeded),
            },
        },
        MetricLine {
            label: "setup",
            shape: MetricShape::Counter {
                total: cur.setup_succeeded,
                delta: delta(cur.setup_succeeded, prev.setup_succeeded),
            },
        },
        MetricLine {
            label: "failed (retry)",
            shape: MetricShape::Counter {
                total: cur.fail_retry,
                delta: delta(cur.fail_retry, prev.fail_retry),
            },
        },
        MetricLine {
            label: "failed (oom)",
            shape: MetricShape::Counter {
                total: cur.fail_oom,
                delta: delta(cur.fail_oom, prev.fail_oom),
            },
        },
        MetricLine {
            label: "failed (final)",
            shape: MetricShape::Counter {
                total: cur.fail_final,
                delta: delta(cur.fail_final, prev.fail_final),
            },
        },
        MetricLine {
            label: "unfulfillable",
            shape: MetricShape::Counter {
                total: cur.unfulfillable,
                delta: delta(cur.unfulfillable, prev.unfulfillable),
            },
        },
        MetricLine {
            label: "invalid_task",
            shape: MetricShape::Counter {
                total: cur.invalid_task,
                delta: delta(cur.invalid_task, prev.invalid_task),
            },
        },
        MetricLine {
            label: "in-flight",
            shape: MetricShape::Gauge {
                total: cur.in_flight,
                delta: delta(cur.in_flight, prev.in_flight),
            },
        },
        MetricLine {
            label: "waiting on deps",
            shape: MetricShape::Gauge {
                total: cur.waiting_on_deps,
                delta: delta(cur.waiting_on_deps, prev.waiting_on_deps),
            },
        },
        MetricLine {
            label: "blocked (upstream unfulfillable)",
            shape: MetricShape::Gauge {
                total: cur.blocked,
                delta: delta(cur.blocked, prev.blocked),
            },
        },
        MetricLine {
            label: "ready in queue",
            shape: MetricShape::Gauge {
                total: cur.ready_in_queue,
                delta: delta(cur.ready_in_queue, prev.ready_in_queue),
            },
        },
        MetricLine {
            label: "busy secondaries",
            shape: MetricShape::Ratio {
                busy: cur.busy_secondaries,
                total: cur.total_secondaries,
                prev_busy: prev.busy_secondaries,
                prev_total: prev.total_secondaries,
            },
        },
        MetricLine {
            label: "busy workers",
            shape: MetricShape::Ratio {
                busy: cur.busy_workers,
                total: cur.total_workers,
                prev_busy: prev.busy_workers,
                prev_total: prev.total_workers,
            },
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
