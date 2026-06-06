//! The compact, human-readable per-role-file line format.
//!
//! Single concern: own the ONE line shape the per-role full-log files
//! (`primary.log` / `secondary.log`) and the submitter's single full-log
//! `File` sink emit. The shape, verbatim:
//!
//! ```text
//! 13:14:30 INFO P-secondary-0  <message> k=v ...
//! ```
//!
//! i.e. `{h:mm:ss local} {LEVEL} {ROLE}-{id}  {message} {event k=v fields}`:
//!
//!   * local-timezone `HH:MM:SS` (no date, no microseconds, no UTC `Z`),
//!   * the level,
//!   * the role prefix `P-<node>` (primary) / `S-<id>` (secondary), derived
//!     GENERICALLY from the role span — see [`ROLE_PREFIXES`],
//!   * two spaces, then the event message and any structured `k=v` fields,
//!   * NO event target (`module::path:`), NO `dynrunner_role_*{…}:` span
//!     prefix, NO span-field dump.
//!
//! The role prefix is the ONLY thing that varies per role, and it is read
//! off the run future's role span via one table ([`ROLE_PREFIXES`]) — there
//! is no per-role code branch in the formatter. A companion layer
//! ([`RoleTagLayer`]) recognises the role span at creation, records a typed
//! [`RoleTag`] into the span's extensions, and the formatter reads it back
//! off the event scope. The two role files share one formatter instance;
//! each narrows by role span via the existing scope-gated filter (see
//! `role_full_layer`), so the format itself is role-agnostic.
//!
//! Local offset is taken from [`chrono::Local`] for the same multithreaded
//! soundness reason the important-stdio `LocalHhMm` timer documents: the
//! `time` crate's local-offset path refuses to compute the offset in a
//! multithreaded process and silently falls back to UTC. By the time the
//! runner installs logging the process is already multithreaded, so only the
//! libc `localtime_r` path (`chrono::Local`) yields correct local time.

use std::fmt;

use chrono::Local;
use tracing::field::{Field, Visit};
use tracing::span::Attributes;
use tracing::{Event, Id, Metadata, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

use dynrunner_core::{PRIMARY_ROLE_SPAN, SECONDARY_ROLE_SPAN};

/// One entry per role: the span NAME that tags a coordinator's run future,
/// the single-letter prefix it maps to, and the span field that carries the
/// human-readable id (primary tags `node = …`, secondary tags `id = …`).
///
/// This is the ONLY place a role is mapped to its line prefix; adding a
/// future role is one new entry here, never a new branch in [`RoleTagLayer`]
/// or [`CompactRoleFormat`]. Both the recogniser and the formatter route
/// through this table, so the prefix can never diverge from the routing key.
const ROLE_PREFIXES: &[RolePrefix] = &[
    RolePrefix {
        span_name: PRIMARY_ROLE_SPAN,
        letter: 'P',
        id_field: "node",
    },
    RolePrefix {
        span_name: SECONDARY_ROLE_SPAN,
        letter: 'S',
        id_field: "id",
    },
];

/// A role-span → line-prefix mapping entry. See [`ROLE_PREFIXES`].
struct RolePrefix {
    /// The role span name (a `dynrunner_core` role-span const).
    span_name: &'static str,
    /// The single-letter prefix (`P` / `S`).
    letter: char,
    /// The span field whose value is the human-readable id.
    id_field: &'static str,
}

/// The role attribution recorded once on a role span at creation and read
/// back by the formatter: the prefix letter plus the resolved id value
/// (e.g. `P` + `secondary-0`). Stored as a typed span extension so the
/// formatter reads a structured value, never a parse of `FormattedFields`.
#[derive(Clone)]
struct RoleTag {
    letter: char,
    id: String,
}

impl RoleTag {
    /// `{letter}-{id}` — the role prefix token, e.g. `P-secondary-0`.
    fn render(&self) -> String {
        format!("{}-{}", self.letter, self.id)
    }
}

/// Field visitor that pulls one named field's value out of a span's
/// `Attributes` as a plain string (no surrounding quotes / `field=`). Used
/// by [`RoleTagLayer`] to read the role span's id field at creation.
struct IdFieldVisitor<'a> {
    wanted: &'a str,
    found: Option<String>,
}

impl Visit for IdFieldVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == self.wanted {
            self.found = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == self.wanted && self.found.is_none() {
            // The coordinators record the id via the `%` display adapter,
            // which `tracing` lowers to a `record_debug` of the `Display`
            // output; strip the surrounding `"` only if a `Debug`-quoted
            // string slipped through.
            let rendered = format!("{value:?}");
            self.found = Some(rendered.trim_matches('"').to_string());
        }
    }
}

/// Companion layer that recognises a role span at creation and records its
/// [`RoleTag`] into the span's extensions, so the [`CompactRoleFormat`]
/// formatter can read a typed role prefix off the event scope.
///
/// Single concern: role recognition + attribution. It never filters or
/// formats — `enabled`/`event_enabled` stay default-true so it never strips
/// a span from any sibling layer's scope. Recognition is table-driven
/// ([`ROLE_PREFIXES`]); a span whose name is not a role span gets no tag.
pub(crate) struct RoleTagLayer;

impl<S> Layer<S> for RoleTagLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(role) = ROLE_PREFIXES
            .iter()
            .find(|r| r.span_name == attrs.metadata().name())
        else {
            return;
        };
        let mut visitor = IdFieldVisitor {
            wanted: role.id_field,
            found: None,
        };
        attrs.record(&mut visitor);
        let Some(span) = ctx.span(id) else { return };
        span.extensions_mut().insert(RoleTag {
            letter: role.letter,
            id: visitor.found.unwrap_or_else(|| "?".to_string()),
        });
    }
}

/// Resolve the role prefix token for the in-context event by walking its
/// span scope for the nearest [`RoleTag`]. Returns `None` for an event
/// emitted outside any role span (the per-role file layers gate those out,
/// so this is the defensive path only).
fn role_token<S, N>(ctx: &FmtContext<'_, S, N>) -> Option<String>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    ctx.event_scope()?
        .find_map(|span| span.extensions().get::<RoleTag>().map(|tag| tag.render()))
}

/// The compact per-role-file event formatter. Emits
/// `{h:mm:ss local} {LEVEL} {role}-{id}  {message} {k=v}` with no target,
/// no span-name prefix, and no span-field dump. See the module docs.
pub(crate) struct CompactRoleFormat;

impl<S, N> FormatEvent<S, N> for CompactRoleFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta: &Metadata<'_> = event.metadata();

        // Local-timezone `HH:MM:SS` — no date, no micros, no UTC `Z`.
        write!(writer, "{} ", Local::now().format("%H:%M:%S"))?;

        // Level, then the role prefix token. The role file layers gate
        // events to one role span, so the token is present in practice; the
        // `?` fallback keeps an out-of-span event readable rather than
        // panicking.
        write!(writer, "{} ", meta.level())?;
        write!(writer, "{}  ", role_token(ctx).as_deref().unwrap_or("?-?"))?;

        // Message + event `k=v` fields via the configured field formatter —
        // no target, no span-field dump.
        ctx.field_format().format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use tracing::subscriber::with_default;
    use tracing_subscriber::Registry;
    use tracing_subscriber::filter::LevelFilter;
    use tracing_subscriber::fmt::MakeWriter;
    use tracing_subscriber::layer::SubscriberExt;

    /// A `MakeWriter` over a shared in-memory buffer so a test can read back
    /// exactly what the formatter emitted.
    #[derive(Clone, Default)]
    struct BufWriter(Arc<Mutex<Vec<u8>>>);

    impl BufWriter {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    struct BufGuard(Arc<Mutex<Vec<u8>>>);
    impl Write for BufGuard {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> MakeWriter<'a> for BufWriter {
        type Writer = BufGuard;
        fn make_writer(&'a self) -> Self::Writer {
            BufGuard(self.0.clone())
        }
    }

    /// Drive the compact formatter under the production `RoleTagLayer` over
    /// an in-memory buffer, emit one event inside the given role span, and
    /// return the captured line. Mirrors the production composition:
    /// `RoleTagLayer` records the tag, the compact-format fmt layer reads it.
    fn capture_role_line(span: impl FnOnce() -> tracing::Span) -> String {
        let buf = BufWriter::default();
        // Mirror the production file layers: compact format + ANSI off (these
        // are persisted files, never a terminal).
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_writer(buf.clone())
            .event_format(CompactRoleFormat)
            .with_ansi(false);
        // Bound the level so this scoped subscriber's `max_level_hint` cannot
        // raise the process-global `MAX_LEVEL` static above the sibling
        // logging tests' expectation (they assert a DEBUG ceiling); an
        // unbounded fmt layer hints TRACE, which pollutes parallel reads of
        // `LevelFilter::current()`.
        let subscriber = Registry::default()
            .with(LevelFilter::INFO)
            .with(RoleTagLayer)
            .with(fmt_layer);
        with_default(subscriber, || {
            span().in_scope(|| tracing::info!(attempt = 3, "task dispatched"));
        });
        buf.contents()
    }

    #[test]
    fn primary_line_is_compact_no_target_with_role_prefix() {
        use chrono::Local;

        let before = Local::now();
        let out = capture_role_line(|| {
            tracing::info_span!(PRIMARY_ROLE_SPAN, kind = "primary", node = "secondary-0")
        });
        let after = Local::now();

        let line = out
            .lines()
            .find(|l| l.contains("task dispatched"))
            .unwrap_or_else(|| panic!("line missing: {out:?}"));

        // No target / span-name prefix anywhere on the line.
        assert!(
            !line.contains("dynrunner"),
            "compact line still carries a target / span-name prefix: {line:?}"
        );
        assert!(
            !line.contains("kind="),
            "compact line dumped the role span fields: {line:?}"
        );

        let mut parts = line.split_whitespace();
        let ts = parts.next().expect("timestamp token");
        let level = parts.next().expect("level token");
        let role = parts.next().expect("role token");

        // `HH:MM:SS` local clock — no date / `T` / `Z` / micros.
        for noise in ['T', 'Z'] {
            assert!(!ts.contains(noise), "timestamp {ts:?} carries `{noise}`");
        }
        let hms: Vec<&str> = ts.split(':').collect();
        assert_eq!(hms.len(), 3, "timestamp not `HH:MM:SS`: {ts:?}");
        assert!(
            hms.iter()
                .all(|p| p.len() == 2 && p.bytes().all(|b| b.is_ascii_digit())),
            "timestamp not two-digit `HH:MM:SS`: {ts:?}"
        );
        // Local, not UTC: the stamp's `HH:MM` matches the local clock.
        let expected: Vec<String> = [before, after]
            .iter()
            .map(|t| t.format("%H:%M").to_string())
            .collect();
        assert!(
            expected.iter().any(|e| ts.starts_with(e.as_str())),
            "timestamp {ts:?} is not the local clock {expected:?}"
        );

        assert_eq!(level, "INFO", "level token misplaced: {line:?}");
        assert_eq!(role, "P-secondary-0", "primary role prefix wrong: {line:?}");

        // Message and event k=v field survive after the role prefix.
        assert!(
            line.contains("task dispatched"),
            "message missing: {line:?}"
        );
        assert!(line.contains("attempt=3"), "event field dropped: {line:?}");
    }

    #[test]
    fn secondary_line_derives_s_prefix_from_id_field() {
        let out = capture_role_line(|| {
            tracing::info_span!(SECONDARY_ROLE_SPAN, kind = "secondary", id = "sec-7")
        });
        let line = out
            .lines()
            .find(|l| l.contains("task dispatched"))
            .unwrap_or_else(|| panic!("line missing: {out:?}"));
        let role = line.split_whitespace().nth(2).expect("role token");
        assert_eq!(role, "S-sec-7", "secondary role prefix wrong: {line:?}");
    }

    #[test]
    fn role_id_recorded_via_display_adapter_has_no_quotes() {
        // The coordinators record the id with the `%` Display adapter
        // (`node = %self.config.node_id`), which `tracing` lowers to a
        // `record_debug` whose `{:?}` forwards to `Display`. The token must
        // carry the bare id, not a `Debug`-quoted string.
        let node = String::from("primary-host-2");
        let out = capture_role_line(
            || tracing::info_span!(PRIMARY_ROLE_SPAN, kind = "primary", node = %node),
        );
        let line = out
            .lines()
            .find(|l| l.contains("task dispatched"))
            .unwrap_or_else(|| panic!("line missing: {out:?}"));
        let role = line.split_whitespace().nth(2).expect("role token");
        assert_eq!(
            role, "P-primary-host-2",
            "display-adapter id mangled: {line:?}"
        );
    }

    #[test]
    fn event_outside_any_role_span_falls_back_to_unknown_prefix() {
        // Defensive path: the per-role file layers gate these out in
        // production, but the formatter must stay panic-free off-span.
        let buf = BufWriter::default();
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_writer(buf.clone())
            .event_format(CompactRoleFormat)
            .with_ansi(false);
        let subscriber = Registry::default()
            .with(LevelFilter::INFO)
            .with(RoleTagLayer)
            .with(fmt_layer);
        with_default(subscriber, || tracing::info!("orphan"));
        let line = buf.contents();
        assert!(
            line.contains("?-?"),
            "off-span fallback prefix missing: {line:?}"
        );
        assert!(
            line.contains("orphan"),
            "off-span message dropped: {line:?}"
        );
    }
}
