//! Tracing-subscriber wiring for the native extension.
//!
//! Single concern: build the process-wide tracing subscriber. Two sinks
//! compose in one [`Registry`]:
//!
//!   * a **full sink** that records every event (subject only to the
//!     verbosity ceiling — a [`LevelFilter`] resolved from the parsed
//!     `--debug` flag, never from `RUST_LOG`/any env), and
//!   * a **stdio sink** that, when *importance mode* is active, passes
//!     ONLY events whose tracing target is [`IMPORTANT_TARGET`]; when
//!     inactive it behaves exactly like the historical single `fmt`
//!     layer (everything to stdout).
//!
//! The gate is one target-keyed *layer filter* ([`important_stdio_filter`]),
//! never a per-call-site `if`. Emitting at `target: "dynrunner_important"`
//! is therefore the only thing a call site needs to know.
//!
//! Selection is passed in EXPLICITLY by the Python side after argparse
//! (`init_logging(...)` — see [`crate::logging::py_init_logging`]), never
//! read from the environment. The knobs the Python CLI surface owns:
//!
//!   * `important_stdio_only` — enables importance mode.
//!   * `full_log_file` — optional explicit path for a single full-log file
//!     (the submitter's `--important-stdio-only` sink). When set, the full
//!     sink writes there (so stdout can be gated without losing the full
//!     log).
//!   * `full_log_dir` — optional per-node directory anchored on the
//!     gateway-shared `--log-dir` mount. When set, the framework's own
//!     runner log is persisted under it, SPLIT BY ROLE: primary-role
//!     events to `<dir>/primary.log`, secondary-role events to
//!     `<dir>/secondary.log`. So the log of a relocated same-host primary
//!     is isolated from its host secondary's, and both land host-readably.
//!     The role is read off the run future's role span (see
//!     [`role_full_layer`] and `dynrunner_core::role_span`), never a
//!     per-call-site branch. Takes precedence over `full_log_file`: the
//!     per-node mount is the durable sink the spawn paths forward as a CLI
//!     arg, the single-file knob is the submitter-only path. When neither
//!     is set, the full log stays on stdout and shell/sbatch redirection
//!     captures it, preserving today's single-stream behaviour.
//!   * `debug` — the parsed `--debug` flag. Raises the verbosity ceiling
//!     from the historical `info` to `debug`, so a `--debug` run produces
//!     DEBUG lines in the full/per-role files (the secondary's
//!     `secondary.log`), not just at the Python root logger. It is the only
//!     verbosity knob — there is no `RUST_LOG` read. The ceiling is applied
//!     ONCE as a global subscriber-level filter on the registry (see
//!     [`init_with`]), not per-sink: every sink layer narrows only by
//!     target/role, so the process-wide `max_level_hint` authoritatively
//!     reflects `--debug` (the per-sink-filter shape left it implicit — see
//!     [`init_with`]).
//!
//! The subscriber is NOT installed at `_native` import — installing it
//! there forced the config to be read from the environment before argparse
//! could run. Instead [`py_init_logging`] installs it explicitly after the
//! Python CLI has parsed the flags. Until that call lands there is no
//! global subscriber, so any framework event emitted in the pre-init
//! window is dropped (the framework drives no run loop in that window, so
//! nothing operator-relevant is lost).

mod compact_format;

use std::fs::OpenOptions;
use std::io;
use std::path::PathBuf;

use chrono::Local;
use compact_format::{CompactRoleFormat, RoleTagLayer};
use pyo3::prelude::*;
use tracing::{Event, Metadata};
use tracing_subscriber::filter::{FilterFn, LevelFilter};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::layer::{Context, Filter, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

/// Tracing target that marks an event as "important" (LLM-wake-worthy).
/// Events emitted at this target reach stdio even in importance mode.
///
/// Re-exported from the single cross-crate source of truth
/// ([`dynrunner_core::IMPORTANT_TARGET`]); the Python side mirrors it with
/// the child logger `dynamic_runner.important`. Keying the stdio filter
/// ([`important_stdio_filter`]) on this same const is what guarantees the
/// emit target and the gate can never diverge.
pub(crate) use dynrunner_core::IMPORTANT_TARGET;

/// Filename for primary-role events under the per-node full-log dir: every
/// event a primary coordinator's run future emits (it carries the
/// [`PRIMARY_ROLE_SPAN`]). Separate from [`SECONDARY_LOG_FILENAME`] so a
/// relocated same-host primary's log is isolated from its host
/// secondary's in the one-process promoted case.
const PRIMARY_LOG_FILENAME: &str = "primary.log";

/// Filename for secondary-role events under the per-node full-log dir:
/// every event a secondary coordinator's run future emits (it carries the
/// [`SECONDARY_ROLE_SPAN`]).
const SECONDARY_LOG_FILENAME: &str = "secondary.log";

/// Cross-crate role-span names, the routing keys for the two per-role
/// full-log layers. Defined once in `dynrunner-core`; the coordinators
/// enter spans of these names at their run entry, this layer reads the
/// names off the event scope. See [`role_full_layer`].
use dynrunner_core::{PRIMARY_ROLE_SPAN, SECONDARY_ROLE_SPAN};

/// Where the full (everything) sink writes.
#[derive(Debug)]
pub(crate) enum FullSink {
    /// Share stdout with the stdio sink. Used when no full-log file is
    /// configured; preserves the historical single-stream behaviour.
    Stdout,
    /// A single dedicated file (the submitter's explicit `full_log_file`
    /// path). Lets stdout be gated without losing the full log; one
    /// unfiltered layer, the only role on the bare submitter is its own
    /// primary.
    File(PathBuf),
    /// A per-node directory (the `full_log_dir` mount path for this node).
    /// The verbose sink splits by role: primary-span events to
    /// `<dir>/primary.log`, secondary-span events to `<dir>/secondary.log`.
    /// In the one-process promoted case the relocated same-host primary
    /// and its host secondary are distinct `spawn_local` tasks carrying
    /// distinct role spans, so their events land in distinct files.
    PerNodeDir(PathBuf),
}

/// Resolved logging mode, built from explicit parameters the Python CLI
/// surface passes to [`py_init_logging`] after argparse.
#[derive(Debug)]
pub(crate) struct LogConfig {
    /// Whether the stdio sink is gated to the important target only.
    pub(crate) important_stdio_only: bool,
    /// Destination of the full sink.
    pub(crate) full_sink: FullSink,
    /// Verbosity ceiling for every sink, resolved from the `--debug` flag:
    /// `DEBUG` when debug is on, else the default `INFO`.
    pub(crate) level: LevelFilter,
}

impl LogConfig {
    /// Build the mode from explicit parameters. The per-node `full_log_dir`
    /// wins over the single `full_log_file`: it is the durable sink the
    /// spawn paths forward (as a `--full-log-dir` CLI arg) so every
    /// container persists its runner log split by role, whereas
    /// `full_log_file` is the submitter-only `--important-stdio-only` path.
    /// Neither set → stdout (historical single-stream). Whitespace-only
    /// strings are treated as unset so an empty CLI value collapses cleanly.
    ///
    /// `debug` raises the verbosity ceiling on EVERY sink to `DEBUG`; off it
    /// stays at the historical `INFO`. The level is parametric (from the
    /// parsed `--debug` flag), never read from `RUST_LOG`/any env — matching
    /// the explicit-parameter design of the rest of this module.
    pub(crate) fn new(
        important_stdio_only: bool,
        full_log_file: Option<String>,
        full_log_dir: Option<String>,
        debug: bool,
    ) -> Self {
        let trimmed = |s: Option<String>| s.filter(|v| !v.trim().is_empty());
        let full_sink = match trimmed(full_log_dir) {
            Some(dir) => FullSink::PerNodeDir(PathBuf::from(dir)),
            None => match trimmed(full_log_file) {
                Some(path) => FullSink::File(PathBuf::from(path)),
                None => FullSink::Stdout,
            },
        };
        Self {
            important_stdio_only,
            full_sink,
            level: level_filter(debug),
        }
    }
}

/// The verbosity ceiling for every layer, resolved from the explicit
/// `--debug` flag: `DEBUG` when on, else the historical `INFO`. This is the
/// single place verbosity is decided — parametric, never read from
/// `RUST_LOG`/any env, consistent with this module's explicit-parameter
/// design. A fresh `LevelFilter` is handed to each layer because filters are
/// consumed on layer attachment.
fn level_filter(debug: bool) -> LevelFilter {
    if debug {
        LevelFilter::DEBUG
    } else {
        LevelFilter::INFO
    }
}

/// The single layer-level gate for the stdio sink. Passes an event iff it
/// targets [`IMPORTANT_TARGET`]. This is the ONLY place importance is
/// decided; call sites just choose their target.
pub(crate) fn important_stdio_filter() -> FilterFn<fn(&Metadata<'_>) -> bool> {
    fn predicate(meta: &Metadata<'_>) -> bool {
        meta.target() == IMPORTANT_TARGET
    }
    FilterFn::new(predicate as fn(&Metadata<'_>) -> bool)
}

/// Build the full (everything) `fmt` layer over `writer`. Unfiltered at the
/// layer level: the verbosity ceiling is a SINGLE global subscriber-level
/// filter applied once on the registry (see [`init_with`]), so this layer
/// owns only the "record everything that passes the global ceiling" concern.
/// Generic over the writer so tests can inject an in-memory buffer.
///
/// The line shape is the compact, human-readable per-role format
/// ([`CompactRoleFormat`]) — the same shape the role-split files emit, so the
/// submitter's single `full_log_file` reads identically. The submitter's
/// only role is its own primary, so the role token resolves to `P-<node>`.
///
/// Returned as a boxed layer so the two-layer set has one uniform type
/// regardless of the writer concretes.
pub(crate) fn full_layer<S, W>(make_writer: W) -> Box<dyn Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    tracing_subscriber::fmt::layer()
        .with_writer(make_writer)
        .event_format(CompactRoleFormat)
        // Plain text for the persisted full-log file (see `role_full_layer`).
        .with_ansi(false)
        .boxed()
}

/// The single layer-level gate for a per-role full-log file: admit an
/// event iff one of the spans in its scope is the role span named
/// `role_span_name`. This is the ONLY place a role is decided for routing;
/// call sites just emit, and the role span their run future entered (see
/// `dynrunner_core::role_span`) carries the attribution.
///
/// The role is read off the span NAME (intrinsic metadata, always present
/// in the event scope), so no field-value-recording layer is needed. Only
/// `event_enabled` is overridden — `enabled` stays default-true so the
/// role span is never disabled for either role layer, keeping the scope
/// intact for the other layer to read.
struct RoleFilter {
    role_span_name: &'static str,
}

impl<S> Filter<S> for RoleFilter
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, _meta: &Metadata<'_>, _cx: &Context<'_, S>) -> bool {
        // Verbosity is owned by the sibling env-filter; role routing is a
        // per-event scope decision made in `event_enabled`. Never disable a
        // span here (that would strip the role span from the other role's
        // scope).
        true
    }

    fn event_enabled(&self, event: &Event<'_>, cx: &Context<'_, S>) -> bool {
        cx.event_scope(event)
            .into_iter()
            .flatten()
            .any(|span| span.name() == self.role_span_name)
    }
}

/// Build a per-role full (everything) `fmt` layer over `writer`, scope-gated
/// to the role span named `role_span_name`. Used by the per-node-dir sink to
/// split `primary.log` / `secondary.log`. The verbosity ceiling is the
/// single global subscriber-level filter (see [`init_with`]); this layer
/// owns only the ROLE-routing concern, so a `--debug` run reaches the
/// per-role files via the global ceiling, never a per-layer level filter.
///
/// The line shape is the compact, human-readable per-role format
/// ([`CompactRoleFormat`]): `{h:mm:ss local} {LEVEL} {P|S}-{id}  {message}`
/// with no target, no `dynrunner_role_*{…}:` span prefix, and no span-field
/// dump. The role prefix is read off the [`RoleTagLayer`]-recorded tag, so
/// the format itself is role-agnostic — both role files share one formatter.
fn role_full_layer<S, W>(
    make_writer: W,
    role_span_name: &'static str,
) -> Box<dyn Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    tracing_subscriber::fmt::layer()
        .with_writer(make_writer)
        .event_format(CompactRoleFormat)
        // Plain text: these are persisted files, never a terminal, so the
        // default `fmt` ANSI colour/dim escapes around the level/fields would
        // corrupt the host-readable log (same reason the important-stdio sink
        // sets `with_ansi(false)`).
        .with_ansi(false)
        .with_filter(RoleFilter { role_span_name })
        .boxed()
}

/// Local-timezone `HH:MM` timer for the operator-facing important-stdio
/// sink. Replaces the default RFC3339-UTC stamp with a compact local
/// clock (e.g. a 19:07Z event prints `21:07` at UTC+2).
///
/// Local offset comes from [`chrono::Local`], which reads it through libc
/// `localtime_r` and is therefore **thread-safe**. This is the deliberate
/// reason for not using `tracing_subscriber`'s built-in local timer (the
/// `time` crate's `UtcOffset::current_local_offset`): that one *refuses*
/// to compute the offset in a multithreaded process for soundness and
/// silently falls back to UTC — which is exactly the bug this fixes. By
/// the time the runner installs logging the process is already
/// multithreaded, so only the libc path yields correct local time.
#[derive(Clone, Copy)]
struct LocalHhMm;

impl FormatTime for LocalHhMm {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", Local::now().format("%H:%M"))
    }
}

/// Build the stdio `fmt` layer over `writer`. Target-gated to
/// [`IMPORTANT_TARGET`] when `important_only`; otherwise ungated. The
/// verbosity ceiling is the single global subscriber-level filter (see
/// [`init_with`]), so this layer owns only the IMPORTANCE-gate concern —
/// never a per-layer level filter.
///
/// In importance mode the layer is also reformatted for operators: a
/// compact local-time [`LocalHhMm`] stamp and no event target (so the
/// `dynrunner_important:` prefix is dropped). The field order is the
/// fmt default — time, level, message, fields — which is already what's
/// wanted, so only the timer and target are overridden. The non-important
/// path and the full sink ([`full_layer`]) keep the verbose default format
/// for debugging.
pub(crate) fn stdio_layer<S, W>(
    make_writer: W,
    important_only: bool,
) -> Box<dyn Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    if important_only {
        tracing_subscriber::fmt::layer()
            .with_writer(make_writer)
            .with_timer(LocalHhMm)
            .with_target(false)
            // Operator-facing plain text: no ANSI dim/colour escapes around
            // the timestamp/level (the default `fmt` layer emits them, which
            // would wrap `21:07` as `\e[2m21:07\e[0m` — noise the owner's
            // target line forbids, and corruption when this sink is captured
            // to a file / sbatch log rather than a terminal).
            .with_ansi(false)
            .with_filter(important_stdio_filter())
            .boxed()
    } else {
        tracing_subscriber::fmt::layer()
            .with_writer(make_writer)
            .boxed()
    }
}

/// Open a full-log file append-create, materialising its parent directory
/// first. The per-node mount subdir (`<mount>/<secondary_id>`) need not
/// exist yet when logging installs — the spawn paths inject the path but
/// the framework composes the per-node tree lazily — so create the parent
/// before opening. Append-create survives the read-once-at-import /
/// file-not-yet-existing case and never truncates a prior run's record.
fn open_append_create(path: &std::path::Path) -> std::fs::File {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .unwrap_or_else(|e| panic!("failed to create full-log dir {}: {e}", parent.display()));
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|e| panic!("failed to open full-log file {}: {e}", path.display()))
}

/// Assemble the layer set for `config`.
///
/// The full sink composes per `FullSink`: `Stdout` adds no file layer (the
/// stdio layer alone is the full stream); `File` adds one unfiltered
/// verbose file layer (the submitter's explicit path); `PerNodeDir` adds
/// TWO role-routed verbose file layers (`primary.log` / `secondary.log`),
/// each gated on the run future's role span. The stdio layer is always
/// present (mode off → ungated stdout; mode on → only the important target
/// to stdout). This is one rule per sink shape, not a per-event branch.
///
/// None of these layers carry a verbosity filter: the `--debug`-resolved
/// ceiling is applied ONCE as a global subscriber-level filter on the
/// registry (see [`init_with`]), so it authoritatively drives the process
/// `max_level_hint` and every layer only narrows further by target/role.
fn build_layers<S>(config: &LogConfig) -> Vec<Box<dyn Layer<S> + Send + Sync>>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    let mut layers: Vec<Box<dyn Layer<S> + Send + Sync>> = Vec::new();
    match &config.full_sink {
        FullSink::Stdout => {}
        FullSink::File(path) => {
            layers.push(full_layer(open_append_create(path)));
        }
        FullSink::PerNodeDir(dir) => {
            layers.push(role_full_layer(
                open_append_create(&dir.join(PRIMARY_LOG_FILENAME)),
                PRIMARY_ROLE_SPAN,
            ));
            layers.push(role_full_layer(
                open_append_create(&dir.join(SECONDARY_LOG_FILENAME)),
                SECONDARY_ROLE_SPAN,
            ));
        }
    }
    layers.push(stdio_layer(io::stdout, config.important_stdio_only));
    layers
}

/// Install the process-wide subscriber for `config`. Idempotent and
/// non-fatal: a second call (or a pre-existing global subscriber) is a
/// no-op, so a secondary that re-enters `init_logging` after a respawn or
/// a consumer that calls `cli_main` then `run` cannot panic.
///
/// The `--debug`-resolved verbosity ceiling (`config.level`) is installed as
/// a SINGLE global subscriber-level filter on the registry — `LevelFilter`
/// implements `Layer`, so `registry().with(level)` is a subscriber-wide
/// gate, not a per-layer one. This is the one place verbosity is decided, so
/// the process-wide `max_level_hint` is authoritatively the level (a
/// `--debug` run reports `DEBUG`), and the per-sink layers
/// ([`build_layers`]) only narrow further by target/role. The previous
/// per-layer `.with_filter(level)` shape left the ceiling implicit: a layer
/// whose OUTERMOST per-layer filter is the role/importance gate reports
/// `None` for its `max_level_hint`, so the registry's global hint never
/// reflected `--debug` (it worked only because `tracing`'s static default is
/// `TRACE`). Hoisting the level to the registry makes the ceiling explicit
/// and robust.
pub(crate) fn init_with(config: &LogConfig) {
    // The [`RoleTagLayer`] recognises each coordinator's role span at
    // creation and records the `{P|S}-{id}` attribution the per-role/full
    // file formatter ([`CompactRoleFormat`]) reads back. It owns only
    // recognition (no filter, no format), so it is attached once globally —
    // the tag lands in the shared per-span extensions every sink layer can
    // read. Harmless when the full sink is stdout-only (no compact-format
    // layer reads the tag), so it is unconditional.
    let _ = tracing_subscriber::registry()
        .with(config.level)
        .with(RoleTagLayer)
        .with(build_layers(config))
        .try_init();
}

/// Install the process-wide tracing subscriber from EXPLICIT parameters the
/// Python CLI surface passes after argparse. This is the single, deferred
/// init point — the `_native` pymodule init no longer installs logging, so
/// the config is chosen by parsed flags rather than read from the
/// environment at import.
///
/// Single concern: translate the Python-side logging knobs into a
/// [`LogConfig`] and install it. `important_stdio_only` arms the stdio gate;
/// `full_log_file` / `full_log_dir` choose the full sink (dir wins — see
/// [`LogConfig::new`]); `debug` raises every sink's verbosity ceiling to
/// `DEBUG` (the parsed `--debug` flag), so a `--debug` run produces DEBUG
/// lines in the per-role/full sinks — not just at the Python root logger.
#[pyfunction]
#[pyo3(name = "init_logging", signature = (
    important_stdio_only = false,
    full_log_file = None,
    full_log_dir = None,
    debug = false,
))]
pub(crate) fn py_init_logging(
    important_stdio_only: bool,
    full_log_file: Option<String>,
    full_log_dir: Option<String>,
    debug: bool,
) {
    let config = LogConfig::new(important_stdio_only, full_log_file, full_log_dir, debug);
    init_with(&config);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use tracing::subscriber::with_default;
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;

    /// A `MakeWriter` over a shared in-memory buffer so a test can read back
    /// exactly what a layer emitted.
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

    /// Drive a full + stdio layer set over in-memory buffers, emit one
    /// important and one normal event, and return (full, stdio) contents.
    /// Verbosity stays at the historical `INFO` default for the
    /// importance-mode/format tests.
    fn run_capture(important_only: bool) -> (String, String) {
        let full_buf = BufWriter::default();
        let stdio_buf = BufWriter::default();
        // Compose via a `Vec<Box<dyn Layer>>` exactly as production does:
        // `Vec<L>` implements `Layer<S>` uniformly, so the two boxed layers
        // attach in one `.with(...)`.
        let layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = vec![
            full_layer::<Registry, _>(full_buf.clone()),
            stdio_layer::<Registry, _>(stdio_buf.clone(), important_only),
        ];
        // The verbosity ceiling is a single global subscriber-level filter
        // (one `LevelFilter` as a registry-wide gate), exactly as production
        // composes it in `init_with`; the importance-mode/format tests run at
        // the historical `INFO` ceiling. Attached last so the boxed
        // `Layer<Registry>` set keeps its base-subscriber type — the gate
        // and global hint are order-independent for a global level filter.
        let subscriber = Registry::default().with(layers).with(LevelFilter::INFO);
        with_default(subscriber, || {
            tracing::info!(target: IMPORTANT_TARGET, "wake-the-llm");
            tracing::info!(target: "dynrunner_normal", "routine-chatter");
        });
        (full_buf.contents(), stdio_buf.contents())
    }

    #[test]
    fn importance_mode_gates_stdio_to_important_target_only() {
        let (full, stdio) = run_capture(true);

        // The full sink records EVERYTHING.
        assert!(
            full.contains("wake-the-llm"),
            "full sink missing important event: {full}"
        );
        assert!(
            full.contains("routine-chatter"),
            "full sink missing normal event: {full}"
        );

        // The stdio sink passes ONLY the important target.
        assert!(
            stdio.contains("wake-the-llm"),
            "stdio missing important event: {stdio}"
        );
        assert!(
            !stdio.contains("routine-chatter"),
            "stdio leaked a normal event in importance mode: {stdio}"
        );
    }

    #[test]
    fn importance_stdio_line_is_compact_local_hhmm_no_target() {
        use chrono::{Local, Timelike, Utc};

        // Bracket the emit with local-clock samples so the assertion is
        // robust across a minute boundary (the line stamps to one of the
        // local minutes observed during the emit window).
        let before = Local::now();
        let (full, stdio) = run_capture(true);
        let after = Local::now();

        let line = stdio
            .lines()
            .find(|l| l.contains("wake-the-llm"))
            .unwrap_or_else(|| panic!("important line missing from stdio: {stdio}"));

        // Plain operator text: no ANSI dim/colour escape sequences.
        assert!(
            !line.contains('\u{1b}'),
            "important-stdio line still carries ANSI escapes: {line:?}"
        );

        // Target is dropped: no `dynrunner_important:` prefix/noise.
        assert!(
            !line.contains(IMPORTANT_TARGET),
            "important-stdio line still carries the target: {line:?}"
        );
        assert!(
            !line.contains("dynrunner_important"),
            "important-stdio line still names the important target: {line:?}"
        );

        // Shape: `HH:MM LEVEL message ...` — leading local clock, then the
        // default field order (time, level, message, fields). The default
        // `fmt` format right-pads the level into a 5-char field, so the
        // separator after the timestamp is run-length whitespace; split on
        // whitespace runs rather than single spaces.
        let mut parts = line.split_whitespace();
        let ts = parts.next().expect("timestamp token");
        let level = parts.next().expect("level token");
        let rest = parts.next().expect("message token");

        // The leading timestamp token carries no RFC3339/date/seconds/UTC
        // noise: no `T` separator, no `Z`, no date `-`, no seconds `.`
        // (the message itself may legitimately contain `-`, so this is
        // scoped to the timestamp token, not the whole line).
        for noise in ['T', 'Z', '-', '.'] {
            assert!(
                !ts.contains(noise),
                "timestamp {ts:?} still carries `{noise}` (date/RFC3339/seconds/UTC noise)"
            );
        }

        assert_eq!(ts.len(), 5, "timestamp is not `HH:MM`: {ts:?}");
        let (hh, mm) = ts.split_once(':').expect("`HH:MM` colon");
        assert!(
            hh.len() == 2 && hh.bytes().all(|b| b.is_ascii_digit()),
            "hour is not two digits: {ts:?}"
        );
        assert!(
            mm.len() == 2 && mm.bytes().all(|b| b.is_ascii_digit()),
            "minute is not two digits: {ts:?}"
        );
        assert_eq!(level, "INFO", "level token not where expected: {line:?}");
        assert!(
            rest.starts_with("wake-the-llm"),
            "message not directly after level: {line:?}"
        );

        // The stamp is LOCAL time: it must equal one of the local `HH:MM`
        // values observed across the emit window (handles a minute roll).
        let expected: Vec<String> = [before, after]
            .iter()
            .map(|t| t.format("%H:%M").to_string())
            .collect();
        assert!(
            expected.iter().any(|e| e == ts),
            "timestamp {ts:?} is not the local clock {expected:?}"
        );

        // And it is genuinely LOCAL, not UTC: whenever this box runs at a
        // whole-hour offset from UTC the printed hour must differ from the
        // UTC hour (the original bug printed UTC). When the box *is* UTC
        // (offset 0) this is vacuously skipped — the shape checks above
        // still pin the format.
        let local_now = Local::now();
        let utc_now = Utc::now();
        if local_now.hour() != utc_now.hour() {
            let utc_ts = utc_now.format("%H:%M").to_string();
            assert_ne!(
                ts, utc_ts,
                "timestamp matches UTC {utc_ts:?}, not local — \
                 multithreaded fallback-to-UTC regressed: {line:?}"
            );
        }

        // The full sink now uses the SAME compact per-role format as the
        // role-split files: no target (`module::path:`), no RFC3339-UTC `Z`.
        let full_line = full
            .lines()
            .find(|l| l.contains("wake-the-llm"))
            .unwrap_or_else(|| panic!("important line missing from full sink: {full}"));
        assert!(
            !full_line.contains(IMPORTANT_TARGET),
            "full sink still carries the target — compact format regressed: {full_line:?}"
        );
        assert!(
            !full_line.contains('Z'),
            "full sink still carries the RFC3339-UTC stamp — compact format regressed: {full_line:?}"
        );
    }

    #[test]
    fn inactive_mode_sends_everything_to_stdio() {
        let (full, stdio) = run_capture(false);

        assert!(full.contains("wake-the-llm") && full.contains("routine-chatter"));
        // No gate: stdio behaves exactly as today — everything passes.
        assert!(
            stdio.contains("wake-the-llm"),
            "stdio missing important event: {stdio}"
        );
        assert!(
            stdio.contains("routine-chatter"),
            "stdio dropped a normal event with the gate off: {stdio}"
        );
    }

    /// Drive a full layer under a GLOBAL `level` ceiling over an in-memory
    /// buffer, emit one INFO and one DEBUG event, and return the captured
    /// contents. The verbosity ceiling is the only variable under test, and
    /// it is the single global subscriber-level filter — exactly how
    /// production (`init_with`) applies it.
    fn run_capture_level(level: LevelFilter) -> String {
        let full_buf = BufWriter::default();
        let layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> =
            vec![full_layer::<Registry, _>(full_buf.clone())];
        // Global level gate attached last (order-independent for a global
        // filter); the boxed `Layer<Registry>` set keeps its base type.
        let subscriber = Registry::default().with(layers).with(level);
        with_default(subscriber, || {
            tracing::info!("an-info-line");
            tracing::debug!("a-debug-line");
        });
        full_buf.contents()
    }

    #[test]
    fn debug_flag_resolves_to_debug_level_else_info() {
        // The `--debug` flag is the only verbosity knob: on → DEBUG ceiling,
        // off → the historical INFO. Parametric, never from `RUST_LOG`.
        assert_eq!(level_filter(true), LevelFilter::DEBUG);
        assert_eq!(level_filter(false), LevelFilter::INFO);

        // And it lands on the config every sink reads its level from.
        let cfg = LogConfig::new(false, None, None, true);
        assert_eq!(cfg.level, LevelFilter::DEBUG);
        let cfg = LogConfig::new(false, None, None, false);
        assert_eq!(cfg.level, LevelFilter::INFO);
    }

    #[test]
    fn debug_true_emits_debug_lines_false_drops_them() {
        // End-to-end through the real layer builder: with debug on, a DEBUG
        // event reaches the sink (this is the `secondary.log` fix); with it
        // off, the INFO ceiling drops it. The INFO line passes either way.
        let debug_on = run_capture_level(level_filter(true));
        assert!(
            debug_on.contains("an-info-line"),
            "INFO line missing under debug: {debug_on}"
        );
        assert!(
            debug_on.contains("a-debug-line"),
            "DEBUG line missing under --debug — verbosity ceiling not raised: {debug_on}"
        );

        let debug_off = run_capture_level(level_filter(false));
        assert!(
            debug_off.contains("an-info-line"),
            "INFO line missing under default level: {debug_off}"
        );
        assert!(
            !debug_off.contains("a-debug-line"),
            "DEBUG line leaked at the default INFO ceiling: {debug_off}"
        );
    }

    #[test]
    fn config_dir_wins_over_file_and_whitespace_is_unset() {
        // `LogConfig::new` is the param contract the Python CLI feeds:
        // the per-node dir takes precedence over the single file, and a
        // whitespace-only value collapses to "unset" so an empty CLI value
        // is treated the same as an omitted one.
        let cfg = LogConfig::new(true, Some("/x/full.log".into()), Some("/x/dir".into()), false);
        assert!(matches!(cfg.full_sink, FullSink::PerNodeDir(_)));
        assert!(cfg.important_stdio_only);

        let cfg = LogConfig::new(false, Some("/x/full.log".into()), None, false);
        assert!(matches!(cfg.full_sink, FullSink::File(_)));

        let cfg = LogConfig::new(false, Some("   ".into()), Some("\t".into()), false);
        assert!(
            matches!(cfg.full_sink, FullSink::Stdout),
            "whitespace-only knobs must collapse to the stdout single-stream"
        );
    }

    #[test]
    fn no_full_log_file_yields_a_single_stdout_stream() {
        // Default config (no file, gate off) must produce exactly one
        // layer — the historical single stdout stream, no duplication.
        let config = LogConfig::new(false, None, None, false);
        let layers = build_layers::<Registry>(&config);
        assert_eq!(
            layers.len(),
            1,
            "expected a single stdout layer when no full-log file is set"
        );
    }

    #[test]
    fn single_full_log_file_adds_one_unfiltered_layer() {
        // The submitter's explicit `full_log_file` path: one verbose file
        // layer + the stdio layer. No role split (the bare submitter's
        // only role is its own primary).
        let dir = tempfile::tempdir().unwrap();
        let config = LogConfig::new(
            true,
            Some(dir.path().join("full.log").display().to_string()),
            None,
            false,
        );
        let layers = build_layers::<Registry>(&config);
        assert_eq!(layers.len(), 2, "expected full-file layer + stdio layer");
    }

    #[test]
    fn per_node_dir_adds_two_role_layers_and_creates_missing_dir() {
        // The per-node mount subdir is composed lazily, so the dir may not
        // exist when logging installs. Building the per-node-dir layers must
        // materialise it and open BOTH role files (append-create), plus the
        // stdio layer — three layers total.
        let dir = tempfile::tempdir().unwrap();
        let node_dir = dir.path().join("sec-0");
        assert!(!node_dir.exists(), "precondition: per-node dir absent");
        let config = LogConfig::new(false, None, Some(node_dir.display().to_string()), false);
        let layers = build_layers::<Registry>(&config);
        assert_eq!(
            layers.len(),
            3,
            "expected primary.log + secondary.log + stdio layers"
        );
        assert!(
            node_dir.join(PRIMARY_LOG_FILENAME).exists(),
            "primary.log not created under fresh per-node dir"
        );
        assert!(
            node_dir.join(SECONDARY_LOG_FILENAME).exists(),
            "secondary.log not created under fresh per-node dir"
        );
    }

    #[test]
    fn role_span_routes_events_to_the_matching_role_file() {
        // The two per-role layers are scope-gated on the role span name:
        // a primary-span event lands ONLY in primary.log, a secondary-span
        // event ONLY in secondary.log, and an event under no role span lands
        // in neither. This is the one-process promoted-case isolation.
        let primary_buf = BufWriter::default();
        let secondary_buf = BufWriter::default();
        let layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = vec![
            role_full_layer::<Registry, _>(primary_buf.clone(), PRIMARY_ROLE_SPAN),
            role_full_layer::<Registry, _>(secondary_buf.clone(), SECONDARY_ROLE_SPAN),
        ];
        let subscriber = Registry::default().with(layers).with(LevelFilter::INFO);
        with_default(subscriber, || {
            tracing::info_span!(PRIMARY_ROLE_SPAN, kind = "primary")
                .in_scope(|| tracing::info!("primary-event"));
            tracing::info_span!(SECONDARY_ROLE_SPAN, kind = "secondary")
                .in_scope(|| tracing::info!("secondary-event"));
            // No role span in scope: routed to neither role file.
            tracing::info!("orphan-event");
        });

        let primary = primary_buf.contents();
        let secondary = secondary_buf.contents();

        assert!(
            primary.contains("primary-event"),
            "primary.log missing the primary-span event: {primary}"
        );
        assert!(
            !primary.contains("secondary-event"),
            "primary.log leaked a secondary-span event: {primary}"
        );
        assert!(
            !primary.contains("orphan-event"),
            "primary.log leaked an unattributed event: {primary}"
        );

        assert!(
            secondary.contains("secondary-event"),
            "secondary.log missing the secondary-span event: {secondary}"
        );
        assert!(
            !secondary.contains("primary-event"),
            "secondary.log leaked a primary-span event: {secondary}"
        );
        assert!(
            !secondary.contains("orphan-event"),
            "secondary.log leaked an unattributed event: {secondary}"
        );
    }

    #[test]
    fn role_routing_attributes_nested_child_span_events() {
        // Events emitted from a child span nested under the role span still
        // route by role — the filter walks the whole event scope, not just
        // the innermost span (the run future enters one role span and emits
        // through whatever inner spans it may open).
        let primary_buf = BufWriter::default();
        let secondary_buf = BufWriter::default();
        let layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = vec![
            role_full_layer::<Registry, _>(primary_buf.clone(), PRIMARY_ROLE_SPAN),
            role_full_layer::<Registry, _>(secondary_buf.clone(), SECONDARY_ROLE_SPAN),
        ];
        let subscriber = Registry::default().with(layers).with(LevelFilter::INFO);
        with_default(subscriber, || {
            tracing::info_span!(PRIMARY_ROLE_SPAN, kind = "primary").in_scope(|| {
                tracing::info_span!("phase", n = 1)
                    .in_scope(|| tracing::info!("nested-primary-event"));
            });
        });
        assert!(
            primary_buf.contents().contains("nested-primary-event"),
            "nested primary event did not route to primary.log"
        );
        assert!(
            !secondary_buf.contents().contains("nested-primary-event"),
            "nested primary event leaked to secondary.log"
        );
    }

    /// REPRODUCTION (the gap the single-filter unit test missed): the EXACT
    /// cluster layer stack — `important_stdio_only` + per-node-dir (so the
    /// two nested `role_full_layer`s) + DEBUG level — must let a `debug!`
    /// emitted INSIDE the secondary role span land in `secondary.log`.
    ///
    /// This drives the production `build_layers` (not the single-filter
    /// `full_layer`), so it exercises the nested
    /// `.with_filter(level).with_filter(RoleFilter/important)` shape the
    /// cluster uses. The on-cluster symptom is ZERO debug lines anywhere.
    #[test]
    fn cluster_stack_debug_reaches_secondary_log() {
        use tracing::level_filters::LevelFilter as CurrentLevel;

        let dir = tempfile::tempdir().unwrap();
        let node_dir = dir.path().join("secondary-0");
        let config = LogConfig::new(
            true, // important_stdio_only — the cluster flag
            None,
            Some(node_dir.display().to_string()), // PerNodeDir → role_full_layers
            true,                                  // --debug → DEBUG level
        );
        // Compose EXACTLY as `init_with`: the level is one global subscriber-
        // level filter, the per-sink layers only narrow by target/role. The
        // layers are generic over the post-level subscriber type (inferred),
        // matching `init_with`'s `registry().with(level).with(build_layers())`.
        let subscriber = Registry::default().with(config.level).with(build_layers(&config));

        with_default(subscriber, || {
            // The verbosity ceiling is now an explicit GLOBAL filter, so the
            // process max-level hint authoritatively reflects `--debug`
            // (DEBUG) rather than relying on `tracing`'s `TRACE` default —
            // the robustness this hoist buys.
            let hint = CurrentLevel::current();
            assert_eq!(
                hint,
                CurrentLevel::DEBUG,
                "global max-level hint should be DEBUG under --debug, got {hint:?}"
            );

            tracing::info_span!(SECONDARY_ROLE_SPAN, kind = "secondary")
                .in_scope(|| tracing::debug!("cluster-debug-line"));

            let secondary = std::fs::read_to_string(node_dir.join(SECONDARY_LOG_FILENAME))
                .expect("secondary.log should exist");
            assert!(
                secondary.contains("cluster-debug-line"),
                "DEBUG event did not reach secondary.log under the cluster \
                 stack — global max-level hint is {hint:?} (expected DEBUG). \
                 secondary.log contents: {secondary:?}"
            );
        });
    }

    /// CURATION-PRESERVED: raising the ceiling to DEBUG must NOT widen the
    /// important-stdio gate. A non-important DEBUG event must stay out of the
    /// important-stdio sink; only target-tagged events reach it.
    #[test]
    fn debug_does_not_widen_important_stdio_gate() {
        let stdio_buf = BufWriter::default();
        let layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> =
            vec![stdio_layer::<Registry, _>(stdio_buf.clone(), true)];
        // DEBUG ceiling applied globally (as production does); the important-
        // stdio gate is the layer's only narrowing — raising the level must
        // not widen it. Global gate attached last (order-independent).
        let subscriber = Registry::default().with(layers).with(LevelFilter::DEBUG);
        with_default(subscriber, || {
            tracing::debug!(target: IMPORTANT_TARGET, "important-debug");
            tracing::debug!(target: "dynrunner_normal", "routine-debug");
        });
        let stdio = stdio_buf.contents();
        assert!(
            stdio.contains("important-debug"),
            "important DEBUG event missing from important-stdio sink: {stdio}"
        );
        assert!(
            !stdio.contains("routine-debug"),
            "non-important DEBUG event leaked into the important-stdio sink \
             — raising the level widened the gate: {stdio}"
        );
    }

    /// GLOBAL-INSTALL REPRODUCTION: the cluster installs the subscriber
    /// process-globally via `init_with` (`try_init` → `set_global_default`),
    /// NOT the scoped `with_default` the other tests use. `set_global_default`
    /// sets the process-wide `MAX_LEVEL` static from the subscriber's
    /// `max_level_hint()`; `with_default` sets a per-thread one. This test
    /// pins that the GLOBAL path also lets a secondary-span `debug!` reach
    /// `secondary.log` under the cluster stack.
    ///
    /// `#[ignore]` because it mutates the process-global subscriber (a
    /// one-shot per process): run explicitly, alone, with
    /// `--ignored --exact logging::tests::cluster_stack_debug_reaches_secondary_log_global`.
    #[test]
    #[ignore = "installs the process-global subscriber; run alone"]
    fn cluster_stack_debug_reaches_secondary_log_global() {
        use tracing::level_filters::LevelFilter as CurrentLevel;

        let dir = tempfile::tempdir().unwrap();
        let node_dir = dir.path().join("secondary-0");
        let config = LogConfig::new(true, None, Some(node_dir.display().to_string()), true);

        // The REAL global install the cluster runs (`try_init`).
        init_with(&config);

        // The process-wide ceiling now authoritatively reflects `--debug`
        // (the global registry-level filter), not `tracing`'s TRACE default.
        let hint = CurrentLevel::current();
        assert_eq!(
            hint,
            CurrentLevel::DEBUG,
            "process max-level hint should be DEBUG under --debug, got {hint:?}"
        );
        tracing::info_span!(SECONDARY_ROLE_SPAN, kind = "secondary")
            .in_scope(|| tracing::debug!("cluster-debug-line-global"));

        let secondary = std::fs::read_to_string(node_dir.join(SECONDARY_LOG_FILENAME))
            .expect("secondary.log should exist");
        assert!(
            secondary.contains("cluster-debug-line-global"),
            "DEBUG event did not reach secondary.log under the GLOBAL cluster \
             install — process-wide max-level hint is {hint:?} (expected to \
             admit DEBUG). secondary.log contents: {secondary:?}"
        );
    }

    /// SUBMITTER-STACK PROBE: the importance-mode submitter stack
    /// (`important_stdio_only=true` + single-file `File` full sink, DEBUG
    /// level) must let a DEBUG event reach the `File` full sink — the
    /// `dynrunner-full.log` the submitter inspects. This stack nests
    /// `full_layer` (single `.with_filter(level)`) alongside the
    /// `stdio_layer` important branch (`.with_filter(level).with_filter(
    /// important_stdio_filter)`); pins that the mixed nesting does not
    /// collapse the global hint and suppress the submitter's own DEBUG.
    #[test]
    #[ignore = "installs the process-global subscriber; run alone"]
    fn submitter_importance_stack_debug_reaches_full_file_global() {
        let dir = tempfile::tempdir().unwrap();
        let full_file = dir.path().join("dynrunner-full.log");
        let config = LogConfig::new(
            true,                                          // important_stdio_only
            Some(full_file.display().to_string()),         // File full sink
            None,
            true,                                          // --debug
        );
        init_with(&config);

        tracing::debug!("submitter-debug-line");

        let full = std::fs::read_to_string(&full_file).expect("full log file should exist");
        assert!(
            full.contains("submitter-debug-line"),
            "DEBUG event did not reach the submitter's full-log file under the \
             importance-mode stack: {full:?}"
        );
    }
}
