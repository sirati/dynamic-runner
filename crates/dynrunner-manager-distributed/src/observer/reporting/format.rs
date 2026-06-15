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

/// Per-field last-PRINTED baseline for the #575 resource-stat averages
/// — kept ALONGSIDE the snapshot-shaped `last_announced` baseline (the
/// existing 13 fields advance the whole-snapshot baseline atomically).
/// A resource line that was OMITTED (None, or within the 25%
/// threshold, or a sibling line tripped the print but THIS one didn't)
/// must NOT advance — the next emit decides inclusion against the
/// SAME prior value the operator last saw. Held by the
/// [`super::reporter::Reporter`]; the render path here both reads it
/// (for the threshold predicate) and PRODUCES the next baseline
/// (returned alongside the report body) so the reporter advances
/// per-field on actual emission.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceBaseline {
    pub mem_p10_bytes: Option<u64>,
    pub mem_p30_bytes: Option<u64>,
    pub mem_p50_bytes: Option<u64>,
    pub mem_p70_bytes: Option<u64>,
    pub mem_p90_bytes: Option<u64>,
    pub mem_avg_bytes: Option<u64>,
    pub total_free_memory_bytes: Option<u64>,
    pub total_swap_used_bytes: Option<u64>,
    pub total_free_swap_bytes: Option<u64>,
    pub cpu_utilization_milli: Option<u32>,
}

/// Relative-change inclusion threshold for the #575 resource lines —
/// owner's spec: include the line when the averaged value moved more
/// than 25% from the last-printed value. Special-cased zero-baseline
/// (always include the first non-zero, see `MetricShape::changed`).
const RESOURCE_THRESHOLD: f64 = 0.25;

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
    /// A #575 resource-stat averaged value rendered as the formatted
    /// bytes/percent gauge with no parenthetical delta (the value IS
    /// itself a rolling mean — the operator wants the level, not the
    /// jitter). Inclusion: present iff `value.is_some()`; changed iff
    /// `prev_printed` is `None` OR the relative move from `prev_printed`
    /// exceeds `RESOURCE_THRESHOLD` (=25%). The zero-baseline path
    /// (`prev_printed = Some(0)`) reports `|v - 0| / max(0, 1) = v`,
    /// so any positive `value` exceeds the threshold and the first
    /// non-zero is always included.
    ResourceAvg {
        value: Option<u64>,
        prev_printed: Option<u64>,
        unit: ResourceUnit,
    },
}

/// How to format a `ResourceAvg`'s body — owns the byte→human or
/// milli-percent→percent presentation. Single seam so a new resource
/// field plugs in by picking a unit, never by adding a new format
/// branch.
#[derive(Debug, Clone, Copy)]
enum ResourceUnit {
    /// Bytes, rendered through the standard `B / KiB / MiB / GiB / TiB`
    /// auto-scale.
    Bytes,
    /// Milli-percent (100_000 = 100%), rendered as `XX.YY%`.
    Percent,
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
    /// `> 0`): counter/gauge `total`, ratio `busy`, resource value
    /// is `Some`.
    fn present(&self) -> bool {
        match self {
            MetricShape::Counter { total, .. } | MetricShape::Gauge { total, .. } => *total > 0,
            MetricShape::Ratio { busy, .. } => *busy > 0,
            MetricShape::ResourceAvg { value, .. } => value.is_some(),
        }
    }

    /// `true` if the metric moved since the last announcement: a
    /// counter/gauge delta, or either component of a ratio, or a
    /// resource-avg crossing the 25% threshold against the last
    /// PRINTED value.
    fn changed(&self) -> bool {
        match self {
            MetricShape::Counter { delta, .. } | MetricShape::Gauge { delta, .. } => *delta > 0,
            MetricShape::Ratio {
                busy,
                total,
                prev_busy,
                prev_total,
            } => busy != prev_busy || total != prev_total,
            MetricShape::ResourceAvg {
                value,
                prev_printed,
                ..
            } => match (value, prev_printed) {
                (None, _) => false,
                (Some(_), None) => true,
                (Some(v), Some(p)) => {
                    let prev_floor = (*p as f64).max(1.0);
                    let rel = ((*v as f64) - (*p as f64)).abs() / prev_floor;
                    rel > RESOURCE_THRESHOLD
                }
            },
        }
    }

    fn body(&self, label: &str) -> String {
        match self {
            MetricShape::Counter { total, delta } => format!("{label}: {total}(+{delta})"),
            MetricShape::Gauge { total, .. } => format!("{label}: {total}"),
            MetricShape::Ratio { busy, total, .. } => format!("{label}: {busy}/{total}"),
            MetricShape::ResourceAvg { value, unit, .. } => match (value, unit) {
                (Some(v), ResourceUnit::Bytes) => format!("{label}: {}", format_bytes(*v)),
                (Some(v), ResourceUnit::Percent) => format!("{label}: {}", format_milli_percent(*v)),
                (None, _) => format!("{label}: -"),
            },
        }
    }

    /// Helper used by the per-field baseline advance: project the
    /// inner `Option<u64>` value of a `ResourceAvg` shape, or `None`
    /// for any other shape (unreachable under the format.rs use
    /// site — only resource lines route through `set`).
    fn resource_value_for_baseline(&self) -> Option<u64> {
        match self {
            MetricShape::ResourceAvg { value, .. } => *value,
            _ => None,
        }
    }
}

/// Human-readable bytes formatter — picks the largest power-of-1024
/// unit `<= value` and renders to two decimal places. Matches the
/// standard SI-binary convention (KiB, MiB, GiB, TiB).
fn format_bytes(v: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;
    const TIB: u64 = GIB * 1024;
    if v >= TIB {
        format!("{:.2} TiB", v as f64 / TIB as f64)
    } else if v >= GIB {
        format!("{:.2} GiB", v as f64 / GIB as f64)
    } else if v >= MIB {
        format!("{:.2} MiB", v as f64 / MIB as f64)
    } else if v >= KIB {
        format!("{:.2} KiB", v as f64 / KIB as f64)
    } else {
        format!("{v} B")
    }
}

/// Milli-percent → percent string (`100_000` = 100.00%).
fn format_milli_percent(v: u64) -> String {
    format!("{:.2}%", v as f64 / 1000.0)
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

/// Outcome of one [`render_report`] call: an optional body PLUS the
/// per-field #575 resource baseline to advance into. The reporter
/// commits the baseline ONLY when it actually emits, so an
/// all-omitted tick (body is `None`) leaves the prev_printed
/// baseline untouched — the next emit decides inclusion against the
/// same prior value the operator last saw.
pub struct RenderReport {
    pub body: Option<String>,
    pub next_resource_baseline: ResourceBaseline,
}

/// Build the periodic-report body, or `None` if there is nothing
/// worth waking an LLM for (every metric omitted as zero AND nothing
/// was omitted-because-unchanged). A `Some` body is the multi-line
/// text the caller emits on the importance channel.
///
/// `prev` is the snapshot at the LAST announcement (not the last tick):
/// callers advance `prev` only when they actually emit, so an
/// all-omitted tick does not reset the delta baseline.
///
/// `resource_prev` is the per-field #575 resource baseline (the
/// last-PRINTED value per resource field — never the last tick).
/// The returned [`RenderReport::next_resource_baseline`] advances
/// per-field on actual emission (only the lines this body included
/// move their baseline) so a sibling-only print does not silently
/// reset the per-field gate the operator is relying on.
pub fn render_report(
    cur: &StatsSnapshot,
    prev: &StatsSnapshot,
    resource_prev: &ResourceBaseline,
) -> RenderReport {
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

    // #575 resource-stat lines — per-field 25% threshold against the
    // last-PRINTED baseline (not the last-announced snapshot). The
    // order pairs with [`ResourceFieldKind`] one-to-one so the
    // per-field baseline advance below can route an "Include" outcome
    // back to the right baseline slot.
    let resource_lines: [(MetricLine, ResourceFieldKind); 10] = [
        (
            MetricLine {
                label: "mem P10 (workers, avg per secondary)",
                shape: MetricShape::ResourceAvg {
                    value: cur.avg_mem_p10_bytes,
                    prev_printed: resource_prev.mem_p10_bytes,
                    unit: ResourceUnit::Bytes,
                },
            },
            ResourceFieldKind::MemP10,
        ),
        (
            MetricLine {
                label: "mem P30 (workers, avg per secondary)",
                shape: MetricShape::ResourceAvg {
                    value: cur.avg_mem_p30_bytes,
                    prev_printed: resource_prev.mem_p30_bytes,
                    unit: ResourceUnit::Bytes,
                },
            },
            ResourceFieldKind::MemP30,
        ),
        (
            MetricLine {
                label: "mem P50 (workers, avg per secondary)",
                shape: MetricShape::ResourceAvg {
                    value: cur.avg_mem_p50_bytes,
                    prev_printed: resource_prev.mem_p50_bytes,
                    unit: ResourceUnit::Bytes,
                },
            },
            ResourceFieldKind::MemP50,
        ),
        (
            MetricLine {
                label: "mem P70 (workers, avg per secondary)",
                shape: MetricShape::ResourceAvg {
                    value: cur.avg_mem_p70_bytes,
                    prev_printed: resource_prev.mem_p70_bytes,
                    unit: ResourceUnit::Bytes,
                },
            },
            ResourceFieldKind::MemP70,
        ),
        (
            MetricLine {
                label: "mem P90 (workers, avg per secondary)",
                shape: MetricShape::ResourceAvg {
                    value: cur.avg_mem_p90_bytes,
                    prev_printed: resource_prev.mem_p90_bytes,
                    unit: ResourceUnit::Bytes,
                },
            },
            ResourceFieldKind::MemP90,
        ),
        (
            MetricLine {
                label: "mem avg (workers, avg per secondary)",
                shape: MetricShape::ResourceAvg {
                    value: cur.avg_mem_avg_bytes,
                    prev_printed: resource_prev.mem_avg_bytes,
                    unit: ResourceUnit::Bytes,
                },
            },
            ResourceFieldKind::MemAvg,
        ),
        (
            MetricLine {
                label: "free host memory (avg per secondary)",
                shape: MetricShape::ResourceAvg {
                    value: cur.avg_total_free_memory_bytes,
                    prev_printed: resource_prev.total_free_memory_bytes,
                    unit: ResourceUnit::Bytes,
                },
            },
            ResourceFieldKind::TotalFreeMemory,
        ),
        (
            MetricLine {
                label: "host swap used (avg per secondary)",
                shape: MetricShape::ResourceAvg {
                    value: cur.avg_total_swap_used_bytes,
                    prev_printed: resource_prev.total_swap_used_bytes,
                    unit: ResourceUnit::Bytes,
                },
            },
            ResourceFieldKind::TotalSwapUsed,
        ),
        (
            MetricLine {
                label: "free host swap (avg per secondary)",
                shape: MetricShape::ResourceAvg {
                    value: cur.avg_total_free_swap_bytes,
                    prev_printed: resource_prev.total_free_swap_bytes,
                    unit: ResourceUnit::Bytes,
                },
            },
            ResourceFieldKind::TotalFreeSwap,
        ),
        (
            MetricLine {
                label: "host CPU utilization (avg per secondary)",
                shape: MetricShape::ResourceAvg {
                    value: cur.avg_cpu_utilization_milli.map(|v| v as u64),
                    prev_printed: resource_prev.cpu_utilization_milli.map(|v| v as u64),
                    unit: ResourceUnit::Percent,
                },
            },
            ResourceFieldKind::CpuUtilization,
        ),
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
    // Resource lines + per-field baseline advance. The baseline starts
    // at the prior value (the no-regress contract: an omitted line
    // never updates its slot) and ratchets the slot only when this
    // emit actually INCLUDED that line. NOTE the resource lines are
    // NOT counted toward `any_omitted_unchanged` — a resource-only
    // unchanged tick must stay silent (no footer suffix); the
    // footer's wake-worthiness contract belongs to the existing 13
    // operational metrics.
    let mut next_resource_baseline = *resource_prev;
    for (line, kind) in &resource_lines {
        if let Rendered::Include(s) = line.render() {
            body.push(s);
            next_resource_baseline.set(*kind, line.shape.resource_value_for_baseline());
        }
    }

    // Nothing INCLUDED → there is nothing wake-worthy this period (every
    // nonzero metric was unchanged, or every metric was zero). Stay
    // silent: a footer-only "Omitted unchanged stats." report would
    // wake an LLM to announce that nothing changed, which defeats the
    // whole point of the importance channel. The footer is meaningful
    // ONLY as a suffix to at least one real, changed line.
    if body.is_empty() {
        return RenderReport {
            body: None,
            next_resource_baseline: *resource_prev,
        };
    }

    if any_omitted_unchanged {
        body.push("Omitted unchanged stats.".to_string());
    }

    RenderReport {
        body: Some(body.join("\n")),
        next_resource_baseline,
    }
}

/// Discriminant for the per-field [`ResourceBaseline`] ratchet — pairs
/// one-to-one with the resource lines in `render_report`. Lives in
/// this file because the routing is local to the format/inclusion seam.
#[derive(Debug, Clone, Copy)]
enum ResourceFieldKind {
    MemP10,
    MemP30,
    MemP50,
    MemP70,
    MemP90,
    MemAvg,
    TotalFreeMemory,
    TotalSwapUsed,
    TotalFreeSwap,
    CpuUtilization,
}

impl ResourceBaseline {
    /// Write `value` into the field named by `kind`. The CPU field
    /// downcasts from `Option<u64>` to `Option<u32>` saturating.
    fn set(&mut self, kind: ResourceFieldKind, value: Option<u64>) {
        match kind {
            ResourceFieldKind::MemP10 => self.mem_p10_bytes = value,
            ResourceFieldKind::MemP30 => self.mem_p30_bytes = value,
            ResourceFieldKind::MemP50 => self.mem_p50_bytes = value,
            ResourceFieldKind::MemP70 => self.mem_p70_bytes = value,
            ResourceFieldKind::MemP90 => self.mem_p90_bytes = value,
            ResourceFieldKind::MemAvg => self.mem_avg_bytes = value,
            ResourceFieldKind::TotalFreeMemory => self.total_free_memory_bytes = value,
            ResourceFieldKind::TotalSwapUsed => self.total_swap_used_bytes = value,
            ResourceFieldKind::TotalFreeSwap => self.total_free_swap_bytes = value,
            ResourceFieldKind::CpuUtilization => {
                self.cpu_utilization_milli =
                    value.map(|v| v.min(u32::MAX as u64) as u32);
            }
        }
    }
}
