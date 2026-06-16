//! Tracing-subscriber wiring for the native extension.
//!
//! Single concern: build the process-wide tracing subscriber. Two sinks
//! compose in one [`Registry`], each carrying its OWN level filter — the
//! file sink is the FORENSIC-COMPLETE record (TRACE), the stdio sink is
//! the OPERATOR-RELEVANT view (`config.level`, i.e. INFO or DEBUG):
//!
//!   * a **full sink** (file or stdout-fallback) that records every event
//!     at TRACE: every peer's file log is the forensic-complete record of
//!     its run, so per-call-site `tracing::trace!(...)` emits land in
//!     `primary.log` / `secondary.log` / `observer.log` regardless of the
//!     `--debug` flag (the flag drives stdio verbosity only). The only
//!     narrowing each file layer applies is its role/scope routing.
//!   * a **stdio sink** that, when *importance mode* is active, passes
//!     ONLY events whose tracing target is [`IMPORTANT_TARGET`]; when
//!     inactive it behaves exactly like the historical single `fmt`
//!     layer (everything to stdout). Bounded by `config.level` (INFO or
//!     DEBUG from `--debug`) so the operator-facing stream is unchanged
//!     by the file sink's TRACE ceiling — `--debug` widens stdio, file
//!     verbosity is the contract, not a knob.
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
//!     `<dir>/secondary.log`, observer-role events to `<dir>/observer.log`,
//!     and events under NO role span — the submitter's preparation phase
//!     and any other un-attributed emit — to `<dir>/setup.log` (the
//!     catch-all complement; see [`SETUP_LOG_FILENAME`]).
//!     So the log of a relocated same-host primary (and the observer it
//!     becomes after handing its role to a compute peer) is isolated from
//!     its host secondary's, and all land host-readably. The submitter
//!     passes a per-setup `full_log_dir` too, so its bootstrap-primary
//!     events (`primary.log`) and post-relocation observer events
//!     (`observer.log`) are captured independently of the operator-facing
//!     `--important-stdio-only` view.
//!     The role is read off the run future's role span (see
//!     [`role_full_layer`] and `dynrunner_core::role_span`), never a
//!     per-call-site branch. Takes precedence over `full_log_file`: the
//!     per-node mount is the durable sink the spawn paths forward as a CLI
//!     arg, the single-file knob is the submitter-only path. When neither
//!     is set, the full log stays on stdout and shell/sbatch redirection
//!     captures it, preserving today's single-stream behaviour.
//!   * `debug` — the parsed `--debug` flag. Raises the STDIO sink's
//!     verbosity ceiling from the historical `INFO` to `DEBUG`, so a
//!     `--debug` run shows DEBUG lines on the operator-facing stream. The
//!     file sink is FORENSIC-COMPLETE at TRACE independently of this knob
//!     (the file-level contract is not operator-tunable — every event a
//!     peer emits is on the durable record, including TRACE), so a `trace!`
//!     emit always lands in `primary.log` / `secondary.log` / `observer.log`
//!     whether or not `--debug` is set. It is the only stdio verbosity knob
//!     — there is no `RUST_LOG` read. The ceiling is applied PER LAYER (the
//!     stdio layer carries `config.level`; each file layer carries
//!     `LevelFilter::TRACE` for the forensic-complete contract) rather than
//!     as one global registry-level filter, because the two sinks have
//!     genuinely different ceilings by design — the process-wide
//!     `max_level_hint` is therefore the max across attached layers
//!     (TRACE whenever any file sink is attached).
//!
//! The subscriber is NOT installed at `_native` import — installing it
//! there forced the config to be read from the environment before argparse
//! could run. Instead [`py_init_logging`] installs it explicitly after the
//! Python CLI has parsed the flags. Until that call lands there is no
//! global subscriber, so any framework event emitted in the pre-init
//! window is dropped (the framework drives no run loop in that window, so
//! nothing operator-relevant is lost).

mod compact_format;
mod debounce;
mod ring_writer;

use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::sync::OnceLock;

use chrono::Local;
use compact_format::{CompactRoleFormat, RoleTagLayer};
use pyo3::prelude::*;
use tracing::{Event, Metadata};
use tracing_appender::non_blocking::{NonBlocking, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::Layer;
use tracing_subscriber::filter::{FilterExt, FilterFn, LevelFilter};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::layer::{Context, Filter, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

/// Tracing target that marks an event as "important" (LLM-wake-worthy).
/// Events emitted at this target reach stdio even in importance mode.
///
/// Re-exported from the single cross-crate source of truth
/// ([`dynrunner_core::IMPORTANT_TARGET`]); the Python side mirrors it with
/// the child logger `dynamic_runner.important`. Keying the stdio filter
/// ([`important_stdio_filter`]) on this same const is what guarantees the
/// emit target and the gate can never diverge.
pub(crate) use dynrunner_core::IMPORTANT_TARGET;

/// Tracing target carrying CONSUMER Python log records forwarded into Rust
/// tracing by [`py_log`] (the `dynamic_runner` Python→tracing bridge).
///
/// A forwarded record inherits the emitting thread's role span (the consumer
/// hook runs inside the run future's role scope — `on_phase_end` on the
/// run-loop thread, `discover_items` on a role-span-entered blocking thread),
/// so it routes to the matching per-role full-log file
/// (`primary.log` / `secondary.log` / `observer.log`) through the SAME
/// [`RoleFilter`] every framework event uses — no per-role branch.
///
/// It is a REGULAR (non-important) target: the operator-facing stdio sink
/// already carries Python chatter through the Python root console handler, so
/// the bridged copy is gated OUT of the stdio sink ([`stdio_layer`]) to avoid
/// a double line, while the full/per-role file sinks (which record everything)
/// keep it. This is the durable per-role Python sink the relocated primary /
/// observer lacked on SLURM.
pub(crate) const BRIDGE_TARGET: &str = "dynrunner_py_bridge";

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

/// Filename for observer-role events under the per-node full-log dir:
/// every event an observer coordinator's run future emits (it carries the
/// [`OBSERVER_ROLE_SPAN`]). Separate so a relocated submitter — which
/// steps down from primary to a standalone observer on the SAME host as
/// the compute peer it promoted — keeps its post-relocation log isolated
/// from that peer's `primary.log` (and from any host `secondary.log`),
/// keeping the relocated submitter debuggable.
const OBSERVER_LOG_FILENAME: &str = "observer.log";

/// Filename for events under NO role span — the per-node dir's
/// catch-all complement of the three role files. The role files are
/// scope-GATED, so without this complement every event emitted outside
/// a coordinator's run future reached no file at all: on the submitter
/// that silently dropped the ENTIRE preparation phase (tunnel build,
/// linger enable/restore, binary staging) from the durable record —
/// and under `--important-stdio-only` from stdout too (proven
/// on-cluster: zero linger-enable lines anywhere, krater 2026-06-10).
/// "setup" because pre-role events are bootstrap/preparation by
/// nature; the same file also catches any later unattributed emit, so
/// the four files are jointly LOSSLESS (every event lands in exactly
/// one).
const SETUP_LOG_FILENAME: &str = "setup.log";

/// Cross-crate role-span names, the routing keys for the per-role
/// full-log layers. Defined once in `dynrunner-core`; the coordinators
/// enter spans of these names at their run entry, this layer reads the
/// names off the event scope. See [`role_full_layer`].
use dynrunner_core::{OBSERVER_ROLE_SPAN, PRIMARY_ROLE_SPAN, SECONDARY_ROLE_SPAN};

/// All role-span names, in one place, so the catch-all complement
/// ([`UnattributedFilter`]) and the per-role gates can never disagree
/// about what counts as "attributed".
const ROLE_SPAN_NAMES: [&str; 3] = [PRIMARY_ROLE_SPAN, SECONDARY_ROLE_SPAN, OBSERVER_ROLE_SPAN];

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
    /// Verbosity ceiling for the STDIO sink only, resolved from the
    /// `--debug` flag: `DEBUG` when debug is on, else the default `INFO`.
    /// The file sink is forensic-complete at TRACE independently of this
    /// (see [`role_full_layer`] / [`full_layer`] / [`unattributed_full_layer`]),
    /// so the field name reflects the only knob `--debug` actually drives —
    /// the operator-facing stream.
    pub(crate) level: LevelFilter,
    /// Per-role-file on-disk byte cap for the forensic-complete file sinks.
    /// `0` means UNBOUNDED — the exact prior (#585) behaviour: each file sink
    /// is a bare append-create file with no rotation. When non-zero each file
    /// sink's inner writer is a [`ring_writer::RingWriter`] that bounds the
    /// file's on-disk volume to roughly this many bytes while retaining the
    /// failure-proximate TAIL (the live segment plus a fixed ring of older
    /// segments). Default [`DEFAULT_FULL_LOG_MAX_BYTES`]; `--full-log-max-bytes`
    /// overrides, `0` opts back out to unbounded.
    pub(crate) full_log_max_bytes: u64,
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
    /// `debug` raises the STDIO sink's verbosity ceiling to `DEBUG`; off it
    /// stays at the historical `INFO`. The file sink is forensic-complete
    /// at TRACE regardless of this flag (every peer's file log records
    /// EVERY event — operator stream verbosity and forensic record are two
    /// different concerns owned by two different per-layer filters). The
    /// level is parametric (from the parsed `--debug` flag), never read
    /// from `RUST_LOG`/any env — matching the explicit-parameter design of
    /// the rest of this module.
    pub(crate) fn new(
        important_stdio_only: bool,
        full_log_file: Option<String>,
        full_log_dir: Option<String>,
        debug: bool,
        full_log_max_bytes: u64,
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
            full_log_max_bytes,
        }
    }
}

/// Default per-role-file on-disk cap for the forensic-complete TRACE file
/// sinks: ~2 GiB. Bounds #585's forensic-complete file log to a bounded TAIL
/// (the live segment plus a small ring of older segments) by default, so a
/// long/large run cannot fill the disk — at 46k-task×15-node scale #585 wrote
/// 118 GB/38 min (21 GB on one node's `setup.log`) and filled the 477 GB quota
/// → exit-126 fleet death; at this cap that `setup.log` rings at ~2 GiB
/// instead. A NORMAL run's role files are well under 2 GiB, so they never
/// rotate and read identically to today. Opt back out to the exact unbounded
/// #585 behaviour with `--full-log-max-bytes 0`.
const DEFAULT_FULL_LOG_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// The STDIO-sink verbosity ceiling, resolved from the explicit `--debug`
/// flag: `DEBUG` when on, else the historical `INFO`. This is the single
/// place STDIO verbosity is decided — parametric, never read from
/// `RUST_LOG`/any env, consistent with this module's explicit-parameter
/// design. The file sink ignores this knob and runs at TRACE for the
/// forensic-complete contract (the file-level ceiling is fixed by the
/// invariant, not the operator). A fresh `LevelFilter` is handed to each
/// layer because filters are consumed on layer attachment.
fn level_filter(debug: bool) -> LevelFilter {
    if debug {
        LevelFilter::DEBUG
    } else {
        LevelFilter::INFO
    }
}

/// The FILE-sink verbosity ceiling — fixed at TRACE by the framework's
/// forensic-complete contract: every peer's file log records EVERY event
/// it emits, including `tracing::trace!(...)` decision narration, so the
/// durable per-role record never silently elides a TRACE emit. Not
/// parametric — there is no operator knob to lower it (the operator-facing
/// knob is `--debug`, which is the STDIO ceiling only). A fresh
/// `LevelFilter` is handed to each file layer because filters are
/// consumed on layer attachment.
fn file_level_filter() -> LevelFilter {
    LevelFilter::TRACE
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

/// Build the full (everything) `fmt` layer over `writer`. Carries
/// [`file_level_filter`] (TRACE) so this file sink is forensic-complete —
/// the framework's contract is that every peer's file log records EVERY
/// event, including TRACE-level decision narration, regardless of `--debug`
/// (which is the STDIO ceiling, owned per [`stdio_layer`]). Generic over
/// the writer so tests can inject an in-memory buffer.
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
        .with_filter(file_level_filter())
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
/// split `primary.log` / `secondary.log` / `observer.log`. Carries TWO
/// per-layer filters composed by `Filter::and`: [`file_level_filter`] (TRACE,
/// the forensic-complete file-sink ceiling) AND a [`RoleFilter`] for span
/// routing. The TRACE ceiling is the file-sink contract — every event a
/// role's run future emits lands in its per-role log, including `trace!`,
/// regardless of `--debug` (which only widens stdio).
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
        // File-sink ceiling: TRACE (forensic-complete contract). Composed
        // with the role-routing filter so the layer narrows on BOTH the
        // level (≤ TRACE — admit all) AND the role-span scope. Order is
        // immaterial — `Filter::and` short-circuits on either rejection.
        .with_filter(file_level_filter().and(RoleFilter { role_span_name }))
        .boxed()
}

/// The complement gate of the per-role files: admit an event iff NO span
/// in its scope is a role span ([`ROLE_SPAN_NAMES`]). Together with the
/// three [`RoleFilter`]-gated layers this partitions the event stream —
/// every event lands in exactly one per-node file. Same `enabled`-stays-
/// true discipline as [`RoleFilter`] so spans are never disabled for the
/// sibling layers.
struct UnattributedFilter;

impl<S> Filter<S> for UnattributedFilter
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, _meta: &Metadata<'_>, _cx: &Context<'_, S>) -> bool {
        true
    }

    fn event_enabled(&self, event: &Event<'_>, cx: &Context<'_, S>) -> bool {
        !cx.event_scope(event)
            .into_iter()
            .flatten()
            .any(|span| ROLE_SPAN_NAMES.contains(&span.name()))
    }
}

/// Build the catch-all full `fmt` layer over `writer` for events under NO
/// role span — the per-node dir's `setup.log` (see [`SETUP_LOG_FILENAME`]
/// for why it must exist). Same compact format, ANSI policy AND file-sink
/// TRACE ceiling as the role files; the only difference is the complement
/// gate. The TRACE ceiling means the pre-role preparation phase's TRACE
/// emits land in setup.log too — forensic-complete, same contract as the
/// per-role files.
fn unattributed_full_layer<S, W>(make_writer: W) -> Box<dyn Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    tracing_subscriber::fmt::layer()
        .with_writer(make_writer)
        .event_format(CompactRoleFormat)
        .with_ansi(false)
        .with_filter(file_level_filter().and(UnattributedFilter))
        .boxed()
}

/// Local-timezone `HH:MM±hhmm` timer for the operator-facing
/// important-stdio sink. Replaces the default RFC3339-UTC stamp with a
/// compact local clock, suffixed with the UTC offset (e.g. a 19:07Z
/// event prints `21:07+0200` at UTC+2). The offset suffix disambiguates
/// the operator-facing local stamps against the compute peers' UTC full
/// logs — pairing the two records (a primary's 09:14 UTC line with the
/// observer's 11:14 local line) is otherwise guesswork.
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
        write!(w, "{}", Local::now().format("%H:%M%z"))
    }
}

/// The single layer-level gate for the ungated stdio sink: pass every event
/// EXCEPT the Python→tracing bridge target ([`BRIDGE_TARGET`]). A bridged
/// consumer record is already on stdout via the Python root console handler,
/// so admitting it here too would double the line; the full/per-role file
/// sinks (which carry everything) are the bridge's destination. This is the
/// ONLY place the bridge target is excluded from stdout; the importance
/// branch excludes it implicitly (it admits ONLY [`IMPORTANT_TARGET`]).
fn non_bridge_stdio_filter() -> FilterFn<fn(&Metadata<'_>) -> bool> {
    fn predicate(meta: &Metadata<'_>) -> bool {
        meta.target() != BRIDGE_TARGET
    }
    FilterFn::new(predicate as fn(&Metadata<'_>) -> bool)
}

/// Build the stdio `fmt` layer over `writer`. Target-gated to
/// [`IMPORTANT_TARGET`] when `important_only`; otherwise admits everything
/// EXCEPT the Python→tracing bridge target (see [`non_bridge_stdio_filter`]).
/// The stdio verbosity ceiling (`stdio_level`, the parsed `--debug` resolved
/// by [`level_filter`]) is attached HERE as a per-layer filter — sibling
/// file layers carry their own (TRACE, forensic-complete) ceiling, so the
/// operator stream's INFO/DEBUG bound is decoupled from the file sinks'
/// TRACE bound. This layer owns BOTH narrowings: the importance/bridge
/// gate AND the stdio verbosity ceiling.
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
    stdio_level: LevelFilter,
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
            // Compose the stdio verbosity ceiling with the importance gate:
            // both must pass. The file sinks' TRACE ceiling sits on sibling
            // layers and does NOT widen this stream — `--debug` is the only
            // operator-facing knob.
            .with_filter(stdio_level.and(important_stdio_filter()))
            .boxed()
    } else {
        tracing_subscriber::fmt::layer()
            .with_writer(make_writer)
            // Drop the bridged consumer-Python copy: it is already on stdout
            // via the Python root console handler (see logging_setup.py); the
            // bridge's durable destination is the per-role/full file sinks.
            // AND the stdio verbosity ceiling (`stdio_level`, INFO/DEBUG from
            // `--debug`) — the file sinks' TRACE does NOT widen stdout.
            .with_filter(stdio_level.and(non_bridge_stdio_filter()))
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

/// Bounded buffer depth (in lines) for the non-blocking file appender. Once
/// the drain thread falls this far behind a slow sink, the LOSSY policy
/// DROPS further lines (and `tracing-appender` emits its own dropped-count
/// line) rather than blocking the emitter. Generous so the forensic-complete
/// TRACE record stays best-effort-whole under a transient NFS stall, yet
/// bounded so a sustained stall cannot grow memory without limit — the whole
/// point is that runtime threads NEVER block on the high-volume sink.
const FILE_APPENDER_BUFFERED_LINES: usize = 100_000;

/// Wrap a file in the NON-BLOCKING appender: a dedicated drain thread owns the
/// write behind a bounded ([`FILE_APPENDER_BUFFERED_LINES`]) buffer, so the
/// runtime threads that emit a TRACE line hand it off and return instead of
/// stalling on the (NFS) write — and only the SINGLE drain thread touches the
/// fd, so the `fdget_pos`/`f_pos` serialization across N emitter threads
/// disappears.
///
/// LOSSY by design: under a slow sink the appender DROPS lines past the
/// buffer (emitting its own dropped-count marker) rather than applying
/// backpressure to the emitters. The forensic-complete TRACE record is thus
/// best-effort — whole under normal sink throughput, sampled under a sustained
/// stall — which is the correct tradeoff against wedging the runtime (the
/// 46k-scale `folio_wait_bit_common` freeze). The faulthandler frame dump is
/// a SEPARATE fd, so a hard crash's stack is preserved regardless of what the
/// appender buffer held.
///
/// The returned [`WorkerGuard`] MUST be held for the process lifetime: its
/// `Drop` flushes the buffer, which is what preserves the fatal-flush
/// guarantee on a CLEAN exit (see [`init_with`]).
///
/// Generic over the inner writer (`W: Write + Send + 'static`, which is all
/// `NonBlockingBuilder::finish` requires) so the high-volume file sink can be
/// EITHER a bare append-create `std::fs::File` (the unbounded `max_bytes == 0`
/// opt-out — exact prior behaviour) OR a size-bounded [`ring_writer::RingWriter`]
/// (the default bounded TAIL). The ring slots HERE, under the non-blocking
/// appender, so its rotate `rename`/`unlink` syscalls run on the SAME single
/// drain thread that owns the write — off the async runtime, no fd contention.
fn non_blocking_file<W: Write + Send + 'static>(writer: W) -> (NonBlocking, WorkerGuard) {
    NonBlockingBuilder::default()
        .lossy(true)
        .buffered_lines_limit(FILE_APPENDER_BUFFERED_LINES)
        .finish(writer)
}

/// Process-lifetime parking lot for the file appenders' [`WorkerGuard`]s.
///
/// A non-blocking appender's drain thread is kept alive — and its buffer
/// flushed on a clean exit — only while its `WorkerGuard` is held. The guards
/// are a process-lifetime concern (the subscriber is installed once globally),
/// so they live in this `OnceLock`, mirroring the module's other
/// process-global ([`debounce`]'s `GLOBAL`). [`init_with`] parks them here at
/// the install seam; the `OnceLock` is never dropped before process exit, so a
/// clean exit runs each guard's `Drop` (flush) while a hard abort/panic may
/// lose the buffer's last lines — the documented, accepted tradeoff.
static FILE_APPENDER_GUARDS: OnceLock<Vec<WorkerGuard>> = OnceLock::new();

/// Assemble the layer set for `config`.
///
/// The full sink composes per `FullSink`: `Stdout` adds no file layer (the
/// stdio layer alone is the full stream); `File` adds one unfiltered
/// verbose file layer (the submitter's explicit path); `PerNodeDir` adds
/// THREE role-routed verbose file layers (`primary.log` / `secondary.log` /
/// `observer.log`), each gated on the run future's role span — so a
/// relocated submitter's post-relocation observer events land in
/// `observer.log`, isolated from the promoted peer's `primary.log` — PLUS
/// the catch-all complement (`setup.log`) for events under no role span,
/// so the per-node record is jointly lossless (the role trio alone
/// silently dropped the submitter's whole preparation phase). The
/// stdio layer is always present (mode off → ungated stdout; mode on →
/// only the important target to stdout). This is one rule per sink shape,
/// not a per-event branch.
///
/// Each layer carries its OWN verbosity ceiling: file layers run at TRACE
/// (forensic-complete contract, [`file_level_filter`]); the stdio layer
/// runs at `config.level` (INFO/DEBUG from `--debug`). The two ceilings are
/// genuinely different by design — the operator stream and the durable
/// record are different concerns — so they are NOT collapsed into a global
/// subscriber filter; the process-wide `max_level_hint` is therefore the
/// max across attached layers (TRACE whenever any file sink is attached).
fn build_layers<S>(config: &LogConfig) -> (Vec<Box<dyn Layer<S> + Send + Sync>>, Vec<WorkerGuard>)
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    let mut layers: Vec<Box<dyn Layer<S> + Send + Sync>> = Vec::new();
    // The non-blocking file appenders' drain-thread guards, returned to the
    // install seam ([`init_with`]) which owns their process-lifetime parking.
    // Each `WorkerGuard` keeps its file's single drain thread alive and
    // flushes its buffer on `Drop`.
    let mut guards: Vec<WorkerGuard> = Vec::new();
    // Open the high-volume sink's inner writer AND wrap it non-blocking,
    // stashing the guard. ONE helper so every file sink (the submitter's single
    // file AND every per-node role/setup file) goes through the same
    // drain-thread path — no per-sink special-casing.
    //
    // The inner writer is chosen ONCE by the cap, not per-sink: when
    // `full_log_max_bytes == 0` it is the bare append-create file (the exact
    // unbounded #585 behaviour — opt-out); otherwise it is a size-bounded
    // [`ring_writer::RingWriter`] that caps each file's on-disk volume to
    // roughly the cap while keeping the failure-proximate TAIL. The ring's
    // rotate syscalls run on the appender's single drain thread (see
    // [`non_blocking_file`]). A RingWriter open failure is fatal the same way
    // `open_append_create` is (the durable record must exist), so it panics
    // with the path for a diagnosable bring-up failure.
    let max_bytes = config.full_log_max_bytes;
    let mut nb = |path: PathBuf| -> NonBlocking {
        let (writer, guard) = if max_bytes == 0 {
            non_blocking_file(open_append_create(&path))
        } else {
            let ring = ring_writer::RingWriter::new(path.clone(), max_bytes)
                .unwrap_or_else(|e| panic!("failed to open ring log file {}: {e}", path.display()));
            non_blocking_file(ring)
        };
        guards.push(guard);
        writer
    };
    match &config.full_sink {
        FullSink::Stdout => {}
        FullSink::File(path) => {
            layers.push(full_layer(nb(path.clone())));
        }
        FullSink::PerNodeDir(dir) => {
            layers.push(role_full_layer(
                nb(dir.join(PRIMARY_LOG_FILENAME)),
                PRIMARY_ROLE_SPAN,
            ));
            layers.push(role_full_layer(
                nb(dir.join(SECONDARY_LOG_FILENAME)),
                SECONDARY_ROLE_SPAN,
            ));
            layers.push(role_full_layer(
                nb(dir.join(OBSERVER_LOG_FILENAME)),
                OBSERVER_ROLE_SPAN,
            ));
            // The complement: events under NO role span (the submitter's
            // entire preparation phase — tunnel build, linger enable,
            // binary staging — plus any other pre-/un-attributed emit).
            // Without it the role-gated trio silently DROPPED those from
            // the durable record (see SETUP_LOG_FILENAME).
            layers.push(unattributed_full_layer(nb(dir.join(SETUP_LOG_FILENAME))));
        }
    }
    // The stdio sink's writer differs by mode. In importance mode each flush
    // to the real stdout is ONE wake event for the LLM operator, so the writer
    // is a debouncing buffer ([`debounce`]) that coalesces bursts into a single
    // flush (500ms quiet edge / 5s max delay) — the gate filter and emit sites
    // stay unaware; only the `MakeWriter` changes. Normal mode keeps the
    // historical unbuffered, line-buffered `io::stdout`.
    //
    // The stdio sink stays SYNCHRONOUS (no non-blocking wrap): it is the
    // LOW-VOLUME operator-relevant stream (`config.level`, INFO/DEBUG — not
    // the TRACE firehose), so it neither drives the NFS write wall nor needs a
    // drain thread, and keeping it synchronous preserves immediate operator
    // visibility AND a synchronous fatal line on the `--important-stdio-only`
    // path (the debounced buffer there is flushed explicitly by
    // [`py_flush_important_stdio`] on the fatal path). Only the high-volume
    // FILE sinks above are made non-blocking.
    if config.important_stdio_only {
        layers.push(stdio_layer(
            debounce::install_debounced_stdout(),
            true,
            config.level,
        ));
    } else {
        layers.push(stdio_layer(io::stdout, false, config.level));
    }
    (layers, guards)
}

/// Install the process-wide subscriber for `config`. Idempotent and
/// non-fatal: a second call (or a pre-existing global subscriber) is a
/// no-op, so a secondary that re-enters `init_logging` after a respawn or
/// a consumer that calls `cli_main` then `run` cannot panic.
///
/// Verbosity is owned PER LAYER, not by the registry: each file sink layer
/// carries [`file_level_filter`] (TRACE, the forensic-complete contract)
/// and the stdio sink layer carries `config.level` (INFO/DEBUG from
/// `--debug`). The two ceilings differ by design — the durable record is
/// not bound by the operator stream — so a global registry-level filter
/// would conflate them and silently truncate the file sink. The process-
/// wide `max_level_hint` is therefore the max across attached layers
/// (TRACE whenever any file sink is attached), which is exactly what the
/// runtime needs to evaluate `tracing::trace!(...)` call sites without
/// pre-filter elision.
pub(crate) fn init_with(config: &LogConfig) {
    // The [`RoleTagLayer`] recognises each coordinator's role span at
    // creation and records the `{P|S}-{id}` attribution the per-role/full
    // file formatter ([`CompactRoleFormat`]) reads back. It owns only
    // recognition (no filter, no format), so it is attached once globally —
    // the tag lands in the shared per-span extensions every sink layer can
    // read. Harmless when the full sink is stdout-only (no compact-format
    // layer reads the tag), so it is unconditional.
    let (layers, guards) = build_layers(config);
    let installed = tracing_subscriber::registry()
        .with(RoleTagLayer)
        .with(layers)
        .try_init()
        .is_ok();
    // Park the non-blocking file appenders' drain-thread guards for the
    // process lifetime ONLY when THIS call installed the subscriber. Holding
    // each `WorkerGuard` keeps its drain thread alive and — crucially —
    // flushes its buffer on `Drop`, so a CLEAN process exit (which runs the
    // `OnceLock`'s drop) preserves the fatal-flush guarantee for the file
    // sinks. A hard abort/panic may lose the buffer's last lines, but the
    // separate faulthandler frame dump still preserves the stack — the
    // documented, accepted tradeoff.
    //
    // If `try_init` was a no-op (a second `init_with`, or a pre-existing
    // global subscriber — the idempotency contract), the layers were NOT
    // attached, so their appenders will never receive an event; we DROP the
    // guards here, flush-and-joining their freshly-spawned drain threads
    // rather than leaking them. `OnceLock::set` is therefore reached at most
    // once (the first successful install), matching the single global
    // subscriber.
    if installed {
        // First successful install: park. `set` cannot already be occupied
        // (only this branch ever sets it, and `try_init` succeeds once).
        let _ = FILE_APPENDER_GUARDS.set(guards);
    } else {
        drop(guards);
    }
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
/// `full_log_max_bytes` caps each forensic-complete file sink's on-disk volume
/// (the bounded TAIL); `None` applies the native [`DEFAULT_FULL_LOG_MAX_BYTES`]
/// default, `Some(0)` opts out to the unbounded #585 behaviour — so the bounded
/// default is owned in ONE place (this crate), not duplicated on the Python side.
#[pyfunction]
#[pyo3(name = "init_logging", signature = (
    important_stdio_only = false,
    full_log_file = None,
    full_log_dir = None,
    debug = false,
    full_log_max_bytes = None,
))]
pub(crate) fn py_init_logging(
    important_stdio_only: bool,
    full_log_file: Option<String>,
    full_log_dir: Option<String>,
    debug: bool,
    full_log_max_bytes: Option<u64>,
) {
    let config = LogConfig::new(
        important_stdio_only,
        full_log_file,
        full_log_dir,
        debug,
        full_log_max_bytes.unwrap_or(DEFAULT_FULL_LOG_MAX_BYTES),
    );
    init_with(&config);
}

/// Forward ONE consumer Python log record into Rust tracing so it lands in
/// the correct per-role full-log file via the existing role-span routing.
///
/// Single concern: emit a `tracing::event!` at [`BRIDGE_TARGET`] carrying the
/// record's message, mapping the Python level NAME to the tracing level. The
/// event inherits the CURRENT thread's role span (the consumer hook runs
/// inside the run future's role scope), so the per-role [`RoleFilter`] routes
/// it to `primary.log` / `secondary.log` / `observer.log` with NO role tag
/// passed across the boundary — role attribution lives entirely in the span,
/// where the run loop owns it. The Python side (a `logging.Handler` installed
/// by `logging_setup.setup_logging`) is the only caller.
///
/// `tracing::event!` needs a compile-time-constant level, so the runtime
/// level name selects one of the five const-level emits. An unrecognised name
/// degrades to `INFO` (the record is never dropped — a forwarded record is a
/// real consumer log line). The `target` is fixed to [`BRIDGE_TARGET`]; the
/// `record_target` (the Python logger NAME) rides along as a structured field
/// so the per-role file still attributes which consumer logger spoke, without
/// the bridge target ever varying (the gate/routing keys must not drift).
#[pyfunction]
#[pyo3(name = "py_log", signature = (level, record_target, message))]
pub(crate) fn py_log(level: &str, record_target: &str, message: &str) {
    match level {
        "CRITICAL" | "ERROR" => tracing::event!(
            target: BRIDGE_TARGET,
            tracing::Level::ERROR,
            logger = record_target,
            "{message}"
        ),
        "WARNING" | "WARN" => tracing::event!(
            target: BRIDGE_TARGET,
            tracing::Level::WARN,
            logger = record_target,
            "{message}"
        ),
        "DEBUG" => tracing::event!(
            target: BRIDGE_TARGET,
            tracing::Level::DEBUG,
            logger = record_target,
            "{message}"
        ),
        "TRACE" | "NOTSET" => tracing::event!(
            target: BRIDGE_TARGET,
            tracing::Level::TRACE,
            logger = record_target,
            "{message}"
        ),
        // "INFO" and any unrecognised name: a forwarded record is a real
        // consumer log line, so never drop it — default to INFO.
        _ => tracing::event!(
            target: BRIDGE_TARGET,
            tracing::Level::INFO,
            logger = record_target,
            "{message}"
        ),
    }
}

/// Forward ONE fatal Python error message to the IMPORTANT target so it
/// reaches stdio even under `--important-stdio-only`, and the full sink as
/// always.
///
/// Single concern: be the Python end of the FATAL-error surfacing path. A
/// fatal/uncaught error (the process is about to exit non-zero) MUST always
/// be diagnosable, so it is emitted at [`IMPORTANT_TARGET`] — the one target
/// the importance-mode stdio gate ([`important_stdio_filter`]) admits — at the
/// ERROR level. This is deliberately SEPARATE from the regular Python→tracing
/// bridge ([`py_log`], which emits at [`BRIDGE_TARGET`] and is gated OUT of the
/// important-stdio sink): the bridge stays importance-agnostic; only this
/// dedicated primitive routes to the important target. The Python side
/// (`logging_setup.surface_fatal_errors`) is the only caller, on the
/// exit-non-zero path.
///
/// Durability of the fatal line: the STDIO sink is synchronous (unbuffered
/// `std::fs::File` for the single-file full sink's stdout-fallback, or
/// line-buffered `io::stdout`), so the operator-facing fatal line is on the
/// wire immediately — the `--important-stdio-only` debounced buffer is flushed
/// explicitly by [`py_flush_important_stdio`] on the fatal path. The FILE
/// sinks are now NON-BLOCKING (a drain thread behind a bounded buffer, see
/// [`non_blocking_file`]), so the file copy of this fatal line is flushed on a
/// CLEAN exit by the [`WorkerGuard`]s' `Drop` ([`init_with`] / the
/// [`FILE_APPENDER_GUARDS`] parking lot). Under a hard abort/panic the file
/// buffer's last lines may be lost — but the operator stdio copy is already on
/// the wire and the separate faulthandler frame dump preserves the stack, so
/// the fatal error stays diagnosable regardless.
///
/// `level` selects the tracing level the event is emitted at. It defaults to
/// `"ERROR"` so the existing fatal-surfacing caller
/// (`logging_setup.surface_fatal_errors`) is unchanged. Non-fatal Python
/// callers (the bring-up milestone emits) pass `"INFO"` so a routine
/// milestone is not recorded as an error in the full log — the importance
/// gate is target-only (level-independent), so every level still reaches the
/// `--important-stdio-only` operator stream. `tracing::event!` needs a
/// compile-time-constant level, so the runtime name selects one of the
/// const-level emits; an unrecognised name degrades to INFO (a milestone is
/// a real line, never dropped).
#[pyfunction]
#[pyo3(name = "py_log_important", signature = (message, level = "ERROR"))]
pub(crate) fn py_log_important(message: &str, level: &str) {
    match level {
        "CRITICAL" | "ERROR" => {
            tracing::event!(target: IMPORTANT_TARGET, tracing::Level::ERROR, "{message}")
        }
        "WARNING" | "WARN" => {
            tracing::event!(target: IMPORTANT_TARGET, tracing::Level::WARN, "{message}")
        }
        "DEBUG" => tracing::event!(target: IMPORTANT_TARGET, tracing::Level::DEBUG, "{message}"),
        "TRACE" | "NOTSET" => {
            tracing::event!(target: IMPORTANT_TARGET, tracing::Level::TRACE, "{message}")
        }
        // "INFO" and any unrecognised name: a milestone is a real line, so
        // never drop it — default to INFO.
        _ => tracing::event!(target: IMPORTANT_TARGET, tracing::Level::INFO, "{message}"),
    }
}

/// Flush the importance-mode debounced stdio buffer immediately, if installed;
/// a no-op in normal mode (no buffer) and when importance mode is off.
///
/// Single concern: be the Python end of the FATAL-path buffer flush. Under
/// `--important-stdio-only` the operator-stdio sink buffers lines and flushes
/// them as one wake event on a 500ms-quiet / 5s-max-delay debounce
/// ([`debounce`]). On a fatal exit the surfacing path
/// (`logging_setup._flush_all_logging`) must put the just-emitted
/// [`py_log_important`] line on the wire SYNCHRONOUSLY rather than wait for the
/// debounce timer or the `atexit` backstop — so it calls this after the
/// important emit. Off importance mode there is no buffer and this is inert, so
/// the Python side calls it unconditionally with no mode knowledge.
#[pyfunction]
#[pyo3(name = "flush_important_stdio")]
pub(crate) fn py_flush_important_stdio() {
    debounce::flush_now();
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
    /// Verbosity is per-layer as production composes it: the file layer
    /// admits TRACE (forensic-complete), the stdio layer is bounded by the
    /// historical `INFO` for the importance-mode/format tests.
    fn run_capture(important_only: bool) -> (String, String) {
        let full_buf = BufWriter::default();
        let stdio_buf = BufWriter::default();
        // Compose via a `Vec<Box<dyn Layer>>` exactly as production does:
        // `Vec<L>` implements `Layer<S>` uniformly, so the two boxed layers
        // attach in one `.with(...)`.
        let layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = vec![
            full_layer::<Registry, _>(full_buf.clone()),
            stdio_layer::<Registry, _>(stdio_buf.clone(), important_only, LevelFilter::INFO),
        ];
        // No global registry-level filter: each sink layer carries its own
        // ceiling (TRACE for file, `stdio_level` for stdio), exactly how
        // production composes it in `init_with`. Stacking a global INFO
        // here would cap the file sink to INFO and defeat the forensic-
        // complete contract.
        let subscriber = Registry::default().with(layers);
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
        // noise: no `T` separator, no `Z`, no seconds `.` (the `-` of a
        // negative UTC offset is legitimate, so the date-dash check is
        // scoped to the clock half below).
        for noise in ['T', 'Z', '.'] {
            assert!(
                !ts.contains(noise),
                "timestamp {ts:?} still carries `{noise}` (RFC3339/seconds/UTC noise)"
            );
        }

        // Shape: `HH:MM±hhmm` — the compact local clock plus the UTC
        // offset suffix that disambiguates the operator-facing local
        // stamps against the compute peers' UTC full logs.
        assert_eq!(ts.len(), 10, "timestamp is not `HH:MM±hhmm`: {ts:?}");
        let (clock, offset) = ts.split_at(5);
        assert!(
            !clock.contains('-'),
            "clock half {clock:?} carries a date dash"
        );
        let (hh, mm) = clock.split_once(':').expect("`HH:MM` colon");
        assert!(
            hh.len() == 2 && hh.bytes().all(|b| b.is_ascii_digit()),
            "hour is not two digits: {ts:?}"
        );
        assert!(
            mm.len() == 2 && mm.bytes().all(|b| b.is_ascii_digit()),
            "minute is not two digits: {ts:?}"
        );
        assert!(
            (offset.starts_with('+') || offset.starts_with('-'))
                && offset[1..].bytes().all(|b| b.is_ascii_digit()),
            "offset is not `±hhmm`: {ts:?}"
        );
        assert_eq!(level, "INFO", "level token not where expected: {line:?}");
        assert!(
            rest.starts_with("wake-the-llm"),
            "message not directly after level: {line:?}"
        );

        // The stamp is LOCAL time: it must equal one of the local
        // `HH:MM±hhmm` values observed across the emit window (handles a
        // minute roll).
        let expected: Vec<String> = [before, after]
            .iter()
            .map(|t| t.format("%H:%M%z").to_string())
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
                clock, utc_ts,
                "clock half matches UTC {utc_ts:?}, not local — \
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

    /// Drive the same layer set as [`run_capture`] but emit on the
    /// observer per-task target — the non-wake-worthy channel for the
    /// #520 per-task INFO narration. Returns `(full, stdio)` so the
    /// caller can assert routing under both modes.
    fn run_capture_observer_task(important_only: bool) -> (String, String) {
        use dynrunner_core::OBSERVER_TASK_TARGET;

        let full_buf = BufWriter::default();
        let stdio_buf = BufWriter::default();
        let layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = vec![
            full_layer::<Registry, _>(full_buf.clone()),
            stdio_layer::<Registry, _>(stdio_buf.clone(), important_only, LevelFilter::INFO),
        ];
        // Per-layer ceilings — no global registry filter (file = TRACE,
        // stdio = INFO above).
        let subscriber = Registry::default().with(layers);
        with_default(subscriber, || {
            tracing::info!(
                target: OBSERVER_TASK_TARGET,
                "task t42 assigned to sec-a-3",
            );
        });
        (full_buf.contents(), stdio_buf.contents())
    }

    /// Regression for #573: per-task observer narration (target
    /// [`dynrunner_core::OBSERVER_TASK_TARGET`]) is SUPPRESSED FROM
    /// stdio when `--important-stdio-only` is set, while the full
    /// sink still records it. The 46k-task build phase would
    /// otherwise flood the operator's wake stream with tens of
    /// thousands of non-wake lines.
    #[test]
    fn importance_mode_suppresses_observer_task_target_from_stdio_but_full_keeps_it() {
        let (full, stdio) = run_capture_observer_task(true);

        assert!(
            full.contains("task t42 assigned to sec-a-3"),
            "full sink lost the per-task line: {full}"
        );
        assert!(
            !stdio.contains("task t42"),
            "per-task narration LEAKED to stdio under --important-stdio-only: {stdio}"
        );
    }

    /// In default mode the per-task observer narration IS visible on
    /// stdio — operators running without `--important-stdio-only`
    /// still see the live per-task stream (#520's default-mode
    /// contract).
    #[test]
    fn default_mode_admits_observer_task_target_on_stdio() {
        let (full, stdio) = run_capture_observer_task(false);

        assert!(full.contains("task t42 assigned to sec-a-3"));
        assert!(
            stdio.contains("task t42 assigned to sec-a-3"),
            "default-mode stdio dropped per-task narration: {stdio}"
        );
    }

    /// Drive a full + stdio layer set under the per-layer ceilings
    /// (file = TRACE, stdio = `stdio_level`) over in-memory buffers, emit
    /// one INFO + one DEBUG + one TRACE event, and return (full, stdio)
    /// contents. The stdio ceiling is the only variable under test — the
    /// file ceiling is fixed at TRACE by the forensic-complete contract.
    fn run_capture_level(stdio_level: LevelFilter) -> (String, String) {
        let full_buf = BufWriter::default();
        let stdio_buf = BufWriter::default();
        let layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = vec![
            full_layer::<Registry, _>(full_buf.clone()),
            stdio_layer::<Registry, _>(stdio_buf.clone(), false, stdio_level),
        ];
        // Per-layer ceilings — no global registry filter.
        let subscriber = Registry::default().with(layers);
        with_default(subscriber, || {
            tracing::info!("an-info-line");
            tracing::debug!("a-debug-line");
            tracing::trace!("a-trace-line");
        });
        (full_buf.contents(), stdio_buf.contents())
    }

    #[test]
    fn debug_flag_resolves_to_debug_level_else_info() {
        // The `--debug` flag is the only STDIO verbosity knob: on → DEBUG
        // ceiling, off → the historical INFO. Parametric, never from
        // `RUST_LOG`. The file ceiling is TRACE regardless (forensic-
        // complete contract).
        assert_eq!(level_filter(true), LevelFilter::DEBUG);
        assert_eq!(level_filter(false), LevelFilter::INFO);

        // And it lands on the config the stdio layer reads its level from.
        let cfg = LogConfig::new(false, None, None, true, 0);
        assert_eq!(cfg.level, LevelFilter::DEBUG);
        let cfg = LogConfig::new(false, None, None, false, 0);
        assert_eq!(cfg.level, LevelFilter::INFO);

        // The file ceiling is fixed at TRACE — not parametric.
        assert_eq!(file_level_filter(), LevelFilter::TRACE);
    }

    /// The file sink is FORENSIC-COMPLETE: DEBUG and TRACE events land in
    /// it regardless of `--debug`. This is the #585 invariant — primary.log
    /// / secondary.log / observer.log capture EVERY event a peer emits.
    #[test]
    fn file_sink_records_trace_and_debug_regardless_of_debug_flag() {
        // --debug OFF: file still records TRACE + DEBUG (the contract).
        let (full_off, _stdio_off) = run_capture_level(level_filter(false));
        assert!(
            full_off.contains("an-info-line"),
            "INFO line missing from file under --debug-off: {full_off}"
        );
        assert!(
            full_off.contains("a-debug-line"),
            "DEBUG line missing from file under --debug-off — the #585 \
             forensic-complete contract regressed: {full_off}"
        );
        assert!(
            full_off.contains("a-trace-line"),
            "TRACE line missing from file under --debug-off — the #585 \
             forensic-complete contract regressed: {full_off}"
        );

        // --debug ON: file still records TRACE + DEBUG (same contract).
        let (full_on, _stdio_on) = run_capture_level(level_filter(true));
        assert!(
            full_on.contains("an-info-line"),
            "INFO line missing from file under --debug-on: {full_on}"
        );
        assert!(
            full_on.contains("a-debug-line"),
            "DEBUG line missing from file under --debug-on: {full_on}"
        );
        assert!(
            full_on.contains("a-trace-line"),
            "TRACE line missing from file under --debug-on: {full_on}"
        );
    }

    /// The stdio sink admits DEBUG only when `--debug` is set, and NEVER
    /// admits TRACE — the operator stream stays operator-relevant. This is
    /// the dual contract to `file_sink_records_trace_and_debug_*`: file is
    /// forensic-complete, stdio is operator-relevant, decoupled.
    #[test]
    fn stdio_sink_caps_at_info_or_debug_per_debug_flag_never_trace() {
        // --debug OFF: stdio capped at INFO (historical default).
        let (_full_off, stdio_off) = run_capture_level(level_filter(false));
        assert!(
            stdio_off.contains("an-info-line"),
            "INFO line missing from stdio under default: {stdio_off}"
        );
        assert!(
            !stdio_off.contains("a-debug-line"),
            "DEBUG line leaked to stdio at the default INFO ceiling: {stdio_off}"
        );
        assert!(
            !stdio_off.contains("a-trace-line"),
            "TRACE line leaked to stdio at the default INFO ceiling: {stdio_off}"
        );

        // --debug ON: stdio admits DEBUG but still NEVER TRACE.
        let (_full_on, stdio_on) = run_capture_level(level_filter(true));
        assert!(
            stdio_on.contains("an-info-line"),
            "INFO line missing from stdio under --debug: {stdio_on}"
        );
        assert!(
            stdio_on.contains("a-debug-line"),
            "DEBUG line missing from stdio under --debug — operator ceiling \
             not raised: {stdio_on}"
        );
        assert!(
            !stdio_on.contains("a-trace-line"),
            "TRACE line leaked to stdio under --debug — the file-sink TRACE \
             contract bled into the operator stream: {stdio_on}"
        );
    }

    #[test]
    fn config_dir_wins_over_file_and_whitespace_is_unset() {
        // `LogConfig::new` is the param contract the Python CLI feeds:
        // the per-node dir takes precedence over the single file, and a
        // whitespace-only value collapses to "unset" so an empty CLI value
        // is treated the same as an omitted one.
        let cfg = LogConfig::new(
            true,
            Some("/x/full.log".into()),
            Some("/x/dir".into()),
            false,
            0,
        );
        assert!(matches!(cfg.full_sink, FullSink::PerNodeDir(_)));
        assert!(cfg.important_stdio_only);

        let cfg = LogConfig::new(false, Some("/x/full.log".into()), None, false, 0);
        assert!(matches!(cfg.full_sink, FullSink::File(_)));

        let cfg = LogConfig::new(false, Some("   ".into()), Some("\t".into()), false, 0);
        assert!(
            matches!(cfg.full_sink, FullSink::Stdout),
            "whitespace-only knobs must collapse to the stdout single-stream"
        );
    }

    #[test]
    fn no_full_log_file_yields_a_single_stdout_stream() {
        // Default config (no file, gate off) must produce exactly one
        // layer — the historical single stdout stream, no duplication.
        let config = LogConfig::new(false, None, None, false, 0);
        let (layers, _guards) = build_layers::<Registry>(&config);
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
            0,
        );
        let (layers, _guards) = build_layers::<Registry>(&config);
        assert_eq!(layers.len(), 2, "expected full-file layer + stdio layer");
    }

    #[test]
    fn per_node_dir_adds_four_file_layers_and_creates_missing_dir() {
        // The per-node mount subdir is composed lazily, so the dir may not
        // exist when logging installs. Building the per-node-dir layers must
        // materialise it and open all THREE role files plus the catch-all
        // setup.log (append-create), plus the stdio layer — five layers
        // total.
        let dir = tempfile::tempdir().unwrap();
        let node_dir = dir.path().join("sec-0");
        assert!(!node_dir.exists(), "precondition: per-node dir absent");
        let config = LogConfig::new(false, None, Some(node_dir.display().to_string()), false, 0);
        let (layers, _guards) = build_layers::<Registry>(&config);
        assert_eq!(
            layers.len(),
            5,
            "expected primary.log + secondary.log + observer.log + setup.log + stdio layers"
        );
        assert!(
            node_dir.join(PRIMARY_LOG_FILENAME).exists(),
            "primary.log not created under fresh per-node dir"
        );
        assert!(
            node_dir.join(SECONDARY_LOG_FILENAME).exists(),
            "secondary.log not created under fresh per-node dir"
        );
        assert!(
            node_dir.join(OBSERVER_LOG_FILENAME).exists(),
            "observer.log not created under fresh per-node dir"
        );
        assert!(
            node_dir.join(SETUP_LOG_FILENAME).exists(),
            "setup.log not created under fresh per-node dir"
        );
    }

    /// REPRODUCTION (the on-cluster observability gap): under the
    /// SUBMITTER's production stack — per-node-dir full sink +
    /// `--important-stdio-only` — an event emitted OUTSIDE any role span
    /// (exactly what the whole preparation pipeline emits: linger
    /// enable/restore, tunnel build, binary staging) must land in the
    /// per-node `setup.log`. Before the catch-all complement existed it
    /// reached NO sink at all: the three role files are scope-gated and
    /// the importance-mode stdio gate dropped it from stdout — zero hits
    /// anywhere (krater 2026-06-10). Also pins the partition: the
    /// pre-role event stays OUT of the role files, and a role-scoped
    /// event stays OUT of setup.log.
    #[test]
    fn preparation_events_outside_role_spans_land_in_setup_log() {
        let dir = tempfile::tempdir().unwrap();
        let node_dir = dir.path().join("setup");
        let config = LogConfig::new(
            true, // --important-stdio-only: the consumer's flag
            None,
            Some(node_dir.display().to_string()),
            false,
            0,
        );
        let (layers, guards) = build_layers::<Registry>(&config);
        // No global level gate — each layer in `build_layers` carries its
        // own (file = TRACE, stdio = `config.level`).
        let subscriber = Registry::default().with(layers);

        with_default(subscriber, || {
            // The preparation pipeline emits with NO role span entered.
            tracing::info!("enabling logind linger for the run user");
            // A role-scoped event for the partition assertion.
            tracing::info_span!(PRIMARY_ROLE_SPAN, kind = "primary")
                .in_scope(|| tracing::info!("primary-scoped-event"));
        });

        // The file sinks are non-blocking: dropping the guards flushes their
        // buffers AND joins the drain threads, so the on-disk files are
        // complete before we read them back.
        drop(guards);

        let setup = std::fs::read_to_string(node_dir.join(SETUP_LOG_FILENAME))
            .expect("setup.log should exist");
        assert!(
            setup.contains("enabling logind linger"),
            "preparation-phase event missing from setup.log — the \
             observability gap regressed: {setup:?}"
        );
        assert!(
            !setup.contains("primary-scoped-event"),
            "role-scoped event leaked into setup.log — the partition broke: {setup:?}"
        );

        for role_file in [
            PRIMARY_LOG_FILENAME,
            SECONDARY_LOG_FILENAME,
            OBSERVER_LOG_FILENAME,
        ] {
            let contents = std::fs::read_to_string(node_dir.join(role_file))
                .unwrap_or_else(|_| panic!("{role_file} should exist"));
            assert!(
                !contents.contains("enabling logind linger"),
                "pre-role event leaked into {role_file}: {contents:?}"
            );
        }
    }

    #[test]
    fn observer_span_routes_events_to_observer_log() {
        // The observer role layer is scope-gated on OBSERVER_ROLE_SPAN: an
        // observer-span event lands ONLY in observer.log, isolated from
        // primary.log / secondary.log. This is the relocated-submitter
        // debuggability fix — a setup that handed its primary role to a
        // compute peer keeps logging under its own observer role span.
        let primary_buf = BufWriter::default();
        let secondary_buf = BufWriter::default();
        let observer_buf = BufWriter::default();
        let layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = vec![
            role_full_layer::<Registry, _>(primary_buf.clone(), PRIMARY_ROLE_SPAN),
            role_full_layer::<Registry, _>(secondary_buf.clone(), SECONDARY_ROLE_SPAN),
            role_full_layer::<Registry, _>(observer_buf.clone(), OBSERVER_ROLE_SPAN),
        ];
        // Per-layer ceilings — file layers carry TRACE already.
        let subscriber = Registry::default().with(layers);
        with_default(subscriber, || {
            tracing::info_span!(OBSERVER_ROLE_SPAN, kind = "observer")
                .in_scope(|| tracing::info!("observer-event"));
        });

        assert!(
            observer_buf.contents().contains("observer-event"),
            "observer.log missing the observer-span event: {}",
            observer_buf.contents()
        );
        assert!(
            !primary_buf.contents().contains("observer-event"),
            "primary.log leaked an observer-span event: {}",
            primary_buf.contents()
        );
        assert!(
            !secondary_buf.contents().contains("observer-event"),
            "secondary.log leaked an observer-span event: {}",
            secondary_buf.contents()
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
        // Per-layer ceilings (file = TRACE).
        let subscriber = Registry::default().with(layers);
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
        // Per-layer ceilings (file = TRACE).
        let subscriber = Registry::default().with(layers);
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

    /// REPRODUCTION (#585): the EXACT cluster layer stack —
    /// `important_stdio_only` + per-node-dir (so the three nested
    /// `role_full_layer`s plus the catch-all) — must let a TRACE-level event
    /// emitted INSIDE the secondary role span land in `secondary.log`
    /// regardless of `--debug` (the forensic-complete file-sink contract).
    /// Same for DEBUG with `--debug` off — file is not bounded by stdio's
    /// `--debug` knob. Drives the production `build_layers` so the per-layer
    /// TRACE ceiling on every file sink is exercised end-to-end.
    #[test]
    fn cluster_stack_trace_and_debug_reach_secondary_log_without_debug_flag() {
        use tracing::level_filters::LevelFilter as CurrentLevel;

        let dir = tempfile::tempdir().unwrap();
        let node_dir = dir.path().join("secondary-0");
        // CRUCIAL: `--debug` is OFF here. Under the OLD global-ceiling shape
        // a DEBUG/TRACE emit would be elided; the #585 fix is that file sinks
        // carry their own TRACE ceiling and ignore `--debug`.
        let config = LogConfig::new(
            true,                                 // important_stdio_only — the cluster flag
            None,
            Some(node_dir.display().to_string()), // PerNodeDir → role_full_layers
            false,                                // --debug OFF
            // DEFAULT bounded cap (not 0): exercises the RingWriter inner-writer
            // path under the non-blocking appender end-to-end — the tiny writes
            // here never rotate, so forensic-completeness/routing through the
            // ring is what this asserts (the ring is transparent under the cap).
            DEFAULT_FULL_LOG_MAX_BYTES,
        );
        // Compose EXACTLY as `init_with`: per-layer ceilings, no global
        // registry-level filter.
        let (layers, guards) = build_layers(&config);
        let subscriber = Registry::default().with(layers);

        with_default(subscriber, || {
            // The process max-level hint is the max across attached layers;
            // the file layers carry TRACE, so it is TRACE — call sites for
            // `tracing::trace!(...)` are evaluated rather than pre-filtered.
            let hint = CurrentLevel::current();
            assert_eq!(
                hint,
                CurrentLevel::TRACE,
                "max-level hint should be TRACE under the file-sink \
                 forensic-complete contract, got {hint:?}"
            );

            tracing::info_span!(SECONDARY_ROLE_SPAN, kind = "secondary").in_scope(|| {
                tracing::debug!("cluster-debug-line");
                tracing::trace!("cluster-trace-line");
            });
        });

        // Non-blocking file sinks: flush + join the drain threads before
        // reading the on-disk file.
        drop(guards);

        let secondary = std::fs::read_to_string(node_dir.join(SECONDARY_LOG_FILENAME))
            .expect("secondary.log should exist");
        assert!(
            secondary.contains("cluster-debug-line"),
            "DEBUG event did not reach secondary.log under the cluster \
             stack with --debug OFF — file-sink forensic-complete \
             contract regressed (#585). secondary.log: {secondary:?}"
        );
        assert!(
            secondary.contains("cluster-trace-line"),
            "TRACE event did not reach secondary.log under the cluster \
             stack — the #585 violation: {secondary:?}"
        );
    }

    /// SMELL E (the bridge): a consumer Python record forwarded through
    /// [`py_log`] from INSIDE a non-submitter role span lands in that role's
    /// per-role full-log file — the durable per-role Python sink the relocated
    /// primary / observer lacked on SLURM. Driven over the PRODUCTION
    /// `build_layers` PerNodeDir + ungated-stdio stack (the relocated primary's
    /// `--full-log-dir` + non-importance compute-node config), so it exercises
    /// the real role routing and the stdio bridge-exclusion together.
    ///
    /// Asserts BOTH halves of the role attribution: the record enters the
    /// matching per-role file (`primary.log` here — a relocated primary is a
    /// non-submitter role), AND is excluded from the other role files and from
    /// the ungated stdout (it is already on stdout via the Python console
    /// handler; admitting the bridge copy would double the line).
    #[test]
    fn py_log_bridge_routes_consumer_record_to_role_file() {
        let dir = tempfile::tempdir().unwrap();
        let node_dir = dir.path().join("compute-0");
        // The compute-node relocated-primary config: per-node-dir full sink,
        // importance OFF (it is NOT forwarded to non-submitter roles).
        let config = LogConfig::new(false, None, Some(node_dir.display().to_string()), false, 0);

        let stdio_buf = BufWriter::default();
        // Mirror production `build_layers` for PerNodeDir (the three role files)
        // but route the always-present stdio layer to an in-memory buffer so the
        // bridge-exclusion is observable. Same shapes, same filters.
        let layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = vec![
            role_full_layer(
                open_append_create(&node_dir.join(PRIMARY_LOG_FILENAME)),
                PRIMARY_ROLE_SPAN,
            ),
            role_full_layer(
                open_append_create(&node_dir.join(SECONDARY_LOG_FILENAME)),
                SECONDARY_ROLE_SPAN,
            ),
            role_full_layer(
                open_append_create(&node_dir.join(OBSERVER_LOG_FILENAME)),
                OBSERVER_ROLE_SPAN,
            ),
            stdio_layer::<Registry, _>(stdio_buf.clone(), config.important_stdio_only, config.level),
        ];
        // No global level filter — per-layer ceilings (file = TRACE; stdio
        // = `config.level`). Mirrors production `init_with`.
        let subscriber = Registry::default().with(layers);

        with_default(subscriber, || {
            // The consumer hook runs INSIDE the run future's role span (here
            // PRIMARY, as on a relocated primary). The bridge forwards the
            // Python record via `py_log`, which emits at `BRIDGE_TARGET`; the
            // role span on the current thread is what routes it.
            tracing::info_span!(PRIMARY_ROLE_SPAN, kind = "primary", node = "compute-0")
                .in_scope(|| {
                    py_log("INFO", "consumer.task", "phase complete: 7 done");
                });
        });

        let primary = std::fs::read_to_string(node_dir.join(PRIMARY_LOG_FILENAME))
            .expect("primary.log should exist");
        let secondary = std::fs::read_to_string(node_dir.join(SECONDARY_LOG_FILENAME))
            .expect("secondary.log should exist");
        let observer = std::fs::read_to_string(node_dir.join(OBSERVER_LOG_FILENAME))
            .expect("observer.log should exist");

        assert!(
            primary.contains("phase complete: 7 done"),
            "bridged consumer record missing from the relocated primary's \
             primary.log: {primary:?}"
        );
        assert!(
            !secondary.contains("phase complete: 7 done"),
            "bridged consumer record leaked into secondary.log: {secondary:?}"
        );
        assert!(
            !observer.contains("phase complete: 7 done"),
            "bridged consumer record leaked into observer.log: {observer:?}"
        );
        // The bridged copy is excluded from the ungated stdio sink (it is on
        // stdout via the Python console handler; no double line).
        assert!(
            !stdio_buf.contents().contains("phase complete: 7 done"),
            "bridged consumer record doubled onto stdout: {}",
            stdio_buf.contents()
        );
    }

    /// REVERT-CHECK for the role-attribution half of the bridge: without the
    /// emitting thread carrying the role span (the `spawn_blocking` detach the
    /// `build_setup_discovery_fn` span-propagation restores), a `py_log` record
    /// reaches NO per-role file. This is what made the GAP invisible on
    /// SLURM — a relocated primary's off-thread `discover_items` logging fell
    /// through every per-role filter. Drop the `discover_items` span re-entry
    /// (i.e. remove the role span from the current thread) and this test shows
    /// the record vanishing from primary.log; that is the failure the fix
    /// prevents.
    #[test]
    fn py_log_outside_role_span_reaches_no_role_file() {
        let dir = tempfile::tempdir().unwrap();
        let node_dir = dir.path().join("compute-0");
        let config = LogConfig::new(false, None, Some(node_dir.display().to_string()), false, 0);
        let (layers, guards) = build_layers::<Registry>(&config);
        // No global level filter — per-layer ceilings.
        let subscriber = Registry::default().with(layers);

        with_default(subscriber, || {
            // No role span entered (the detached blocking thread): the emit has
            // no role scope to route by.
            py_log("INFO", "consumer.task", "orphan-bridge-record");
        });

        // Non-blocking file sinks: flush + join before reading the files.
        drop(guards);

        for role_file in [
            PRIMARY_LOG_FILENAME,
            SECONDARY_LOG_FILENAME,
            OBSERVER_LOG_FILENAME,
        ] {
            let contents = std::fs::read_to_string(node_dir.join(role_file))
                .unwrap_or_else(|_| panic!("{role_file} should exist"));
            assert!(
                !contents.contains("orphan-bridge-record"),
                "a role-span-less bridge record reached {role_file} — the \
                 role routing is not scope-gated: {contents:?}"
            );
        }

        // ...but the record is NOT lost: the catch-all complement keeps it
        // durable in setup.log (jointly-lossless per-node record).
        let setup = std::fs::read_to_string(node_dir.join(SETUP_LOG_FILENAME))
            .expect("setup.log should exist");
        assert!(
            setup.contains("orphan-bridge-record"),
            "role-span-less bridge record missing from the setup.log \
             catch-all: {setup:?}"
        );
    }

    /// The `py_log` level NAME maps to the tracing level: it feeds the
    /// real verbosity gates rather than being a flat INFO emit. Asserted on
    /// the STDIO sink (the only one with an INFO/DEBUG-bounded ceiling) — a
    /// Python `WARNING` passes the stdio INFO ceiling and a `DEBUG` is
    /// dropped from stdio. The FILE sink admits both regardless (forensic-
    /// complete contract), so the file-sink half of the assertion is the
    /// dual: a DEBUG bridge record IS preserved in the per-role log.
    #[test]
    fn py_log_level_name_maps_to_tracing_level() {
        let dir = tempfile::tempdir().unwrap();
        let node_dir = dir.path().join("compute-0");
        // Stdio at INFO (debug off); file at TRACE (forensic contract).
        let config = LogConfig::new(false, None, Some(node_dir.display().to_string()), false, 0);

        // We need to observe stdio too — substitute an in-memory writer for
        // the always-present stdio layer so the bridge-exclusion + level
        // gate are both visible. The file layers come straight from the
        // production `build_layers` path (PerNodeDir → role files +
        // setup.log).
        let stdio_buf = BufWriter::default();
        let mut layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = vec![
            role_full_layer(
                open_append_create(&node_dir.join(PRIMARY_LOG_FILENAME)),
                PRIMARY_ROLE_SPAN,
            ),
            role_full_layer(
                open_append_create(&node_dir.join(SECONDARY_LOG_FILENAME)),
                SECONDARY_ROLE_SPAN,
            ),
            role_full_layer(
                open_append_create(&node_dir.join(OBSERVER_LOG_FILENAME)),
                OBSERVER_ROLE_SPAN,
            ),
            unattributed_full_layer(open_append_create(&node_dir.join(SETUP_LOG_FILENAME))),
        ];
        layers.push(stdio_layer::<Registry, _>(
            stdio_buf.clone(),
            config.important_stdio_only,
            config.level,
        ));
        let subscriber = Registry::default().with(layers);

        with_default(subscriber, || {
            tracing::info_span!(PRIMARY_ROLE_SPAN, kind = "primary", node = "c0").in_scope(|| {
                py_log("WARNING", "consumer.task", "warn-passes-info-ceiling");
                py_log("DEBUG", "consumer.task", "debug-passes-file-but-not-stdio");
            });
        });

        let primary = std::fs::read_to_string(node_dir.join(PRIMARY_LOG_FILENAME))
            .expect("primary.log should exist");
        let stdio = stdio_buf.contents();

        // FILE sink (TRACE ceiling, forensic-complete): both bridged records
        // land in primary.log, confirming the level mapping is real (not a
        // flat-INFO emit) AND that the file ceiling admits DEBUG.
        assert!(
            primary.contains("warn-passes-info-ceiling"),
            "WARNING-level bridge record missing from primary.log: {primary:?}"
        );
        assert!(
            primary.contains("debug-passes-file-but-not-stdio"),
            "DEBUG-level bridge record missing from primary.log — the file \
             sink should be forensic-complete: {primary:?}"
        );

        // STDIO sink: the bridge is gated out (already on Python console);
        // confirm explicitly so the test pins both halves.
        assert!(
            !stdio.contains("warn-passes-info-ceiling"),
            "bridged record leaked onto stdout (bridge-exclusion broke): {stdio:?}"
        );
        assert!(
            !stdio.contains("debug-passes-file-but-not-stdio"),
            "bridged record leaked onto stdout: {stdio:?}"
        );
    }

    /// CURATION-PRESERVED: raising the ceiling to DEBUG must NOT widen the
    /// important-stdio gate. A non-important DEBUG event must stay out of the
    /// important-stdio sink; only target-tagged events reach it.
    #[test]
    fn debug_does_not_widen_important_stdio_gate() {
        let stdio_buf = BufWriter::default();
        // DEBUG stdio ceiling carried by the layer itself (no global filter);
        // the important-stdio gate is the other half of the layer's filter —
        // raising the level must not widen the target gate.
        let layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = vec![stdio_layer::<Registry, _>(
            stdio_buf.clone(),
            true,
            LevelFilter::DEBUG,
        )];
        let subscriber = Registry::default().with(layers);
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

    /// FATAL-PATH GUARANTEE: a fatal Python error forwarded through
    /// [`py_log_important`] reaches the important-stdio sink AND the full sink
    /// under `--important-stdio-only`, whereas the regular Python→tracing
    /// bridge ([`py_log`]) is gated OUT of the important-stdio sink. This is
    /// the diagnosability invariant the on-cluster defect violated: a fatal
    /// dispatch error logged through the bridge vanished from stdio under
    /// importance mode. Driven over the production importance-mode stack (the
    /// stdio gate + a `File` full sink) so both halves are observed together.
    #[test]
    fn fatal_important_emit_reaches_stdio_and_full_under_importance_mode() {
        let dir = tempfile::tempdir().unwrap();
        let full_file = dir.path().join("dynrunner-full.log");
        let stdio_buf = BufWriter::default();
        // Importance-mode stdio gate + a `File` full sink, exactly as the
        // submitter's `--important-stdio-only --full-log-file=...` stack.
        let layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = vec![
            full_layer::<Registry, _>(open_append_create(&full_file)),
            stdio_layer::<Registry, _>(stdio_buf.clone(), true, LevelFilter::INFO),
        ];
        // No global filter — per-layer ceilings (file = TRACE, stdio = INFO).
        let subscriber = Registry::default().with(layers);
        with_default(subscriber, || {
            // The fatal-error primitive — emits at the IMPORTANT target.
            py_log_important("SLURM dispatch failed: boom", "ERROR");
            // A regular bridged Python record — emits at BRIDGE_TARGET.
            py_log("ERROR", "consumer.cli", "routine-bridge-error");
        });

        let full = std::fs::read_to_string(&full_file).expect("full log should exist");
        let stdio = stdio_buf.contents();

        // The fatal message MUST surface on the gated stdio (it is the whole
        // point — a hard failure must always be diagnosable).
        assert!(
            stdio.contains("SLURM dispatch failed: boom"),
            "fatal error did not reach the important-stdio sink under \
             importance mode — the defect: {stdio:?}"
        );
        // And it is captured in the full log.
        assert!(
            full.contains("SLURM dispatch failed: boom"),
            "fatal error missing from the full sink: {full:?}"
        );
        // A regular bridged record stays gated OUT of the important-stdio sink
        // (the importance filter is NOT weakened for non-fatal logs)...
        assert!(
            !stdio.contains("routine-bridge-error"),
            "the importance gate leaked a non-fatal bridge record onto stdio: {stdio:?}"
        );
        // ...while still landing in the full log.
        assert!(
            full.contains("routine-bridge-error"),
            "non-fatal bridge record missing from the full sink: {full:?}"
        );
    }

    /// Poll a file until it contains `needle` or the timeout elapses, then
    /// return its full contents. The GLOBAL-install tests below install via
    /// [`init_with`], which parks the non-blocking file appenders' guards in
    /// the process-global [`FILE_APPENDER_GUARDS`] — they are never dropped
    /// during the test, so the drain thread's flush is asynchronous. Polling
    /// (rather than dropping a guard the test does not own) is the honest
    /// assertion: the event DOES land, just on the drain thread's schedule.
    fn read_until_contains(path: &std::path::Path, needle: &str) -> String {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let contents = std::fs::read_to_string(path).unwrap_or_default();
            if contents.contains(needle) || std::time::Instant::now() >= deadline {
                return contents;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }

    /// GLOBAL-INSTALL REPRODUCTION (#585): the cluster installs the
    /// subscriber process-globally via `init_with` (`try_init` →
    /// `set_global_default`), NOT the scoped `with_default` the other tests
    /// use. `set_global_default` sets the process-wide `MAX_LEVEL` static
    /// from the subscriber's `max_level_hint()`; `with_default` sets a
    /// per-thread one. This test pins that the GLOBAL path lets a
    /// secondary-span `trace!` AND `debug!` reach `secondary.log` even with
    /// `--debug` OFF — the file-sink forensic-complete contract owned
    /// PER-LAYER (not by the operator-facing `--debug` knob).
    ///
    /// `#[ignore]` because it mutates the process-global subscriber (a
    /// one-shot per process): run explicitly, alone, with
    /// `--ignored --exact logging::tests::cluster_stack_trace_reaches_secondary_log_global`.
    #[test]
    #[ignore = "installs the process-global subscriber; run alone"]
    fn cluster_stack_trace_reaches_secondary_log_global() {
        use tracing::level_filters::LevelFilter as CurrentLevel;

        let dir = tempfile::tempdir().unwrap();
        let node_dir = dir.path().join("secondary-0");
        // --debug OFF — the file sink must still admit DEBUG + TRACE.
        let config = LogConfig::new(true, None, Some(node_dir.display().to_string()), false, 0);

        // The REAL global install the cluster runs (`try_init`).
        init_with(&config);

        // The process-wide ceiling now reflects the MAX across all attached
        // layers — the file layers carry TRACE, so the static hint is TRACE
        // (call sites for `tracing::trace!(...)` are evaluated rather than
        // pre-filtered).
        let hint = CurrentLevel::current();
        assert_eq!(
            hint,
            CurrentLevel::TRACE,
            "process max-level hint should be TRACE under the file-sink \
             forensic-complete contract, got {hint:?}"
        );
        tracing::info_span!(SECONDARY_ROLE_SPAN, kind = "secondary").in_scope(|| {
            tracing::debug!("cluster-debug-line-global");
            tracing::trace!("cluster-trace-line-global");
        });

        let secondary = read_until_contains(
            &node_dir.join(SECONDARY_LOG_FILENAME),
            "cluster-trace-line-global",
        );
        assert!(
            secondary.contains("cluster-debug-line-global"),
            "DEBUG event did not reach secondary.log under the GLOBAL cluster \
             install with --debug OFF — process-wide max-level hint is \
             {hint:?} (expected TRACE). secondary.log: {secondary:?}"
        );
        assert!(
            secondary.contains("cluster-trace-line-global"),
            "TRACE event did not reach secondary.log under the GLOBAL cluster \
             install — the #585 violation: {secondary:?}"
        );
    }

    /// SUBMITTER-STACK PROBE (#585): the importance-mode submitter stack
    /// (`important_stdio_only=true` + single-file `File` full sink) must
    /// let a DEBUG and TRACE event reach the `File` full sink — the
    /// `dynrunner-full.log` the submitter inspects — even with `--debug`
    /// OFF. The file sink is forensic-complete (TRACE) regardless of the
    /// operator-facing `--debug` knob; the stdio sink stays operator-
    /// relevant. This stack pairs `full_layer` (per-layer TRACE filter)
    /// alongside the `stdio_layer` important branch (per-layer
    /// `stdio_level`-AND-importance filter); pins that the per-layer
    /// ceilings do not collide.
    #[test]
    #[ignore = "installs the process-global subscriber; run alone"]
    fn submitter_importance_stack_trace_reaches_full_file_global() {
        let dir = tempfile::tempdir().unwrap();
        let full_file = dir.path().join("dynrunner-full.log");
        // --debug OFF — file sink must still record DEBUG + TRACE.
        let config = LogConfig::new(
            true,                                  // important_stdio_only
            Some(full_file.display().to_string()), // File full sink
            None,
            false, // --debug OFF
            0,
        );
        init_with(&config);

        tracing::debug!("submitter-debug-line");
        tracing::trace!("submitter-trace-line");

        let full = read_until_contains(&full_file, "submitter-trace-line");
        assert!(
            full.contains("submitter-debug-line"),
            "DEBUG event did not reach the submitter's full-log file under the \
             importance-mode stack with --debug OFF — file-sink contract \
             regressed: {full:?}"
        );
        assert!(
            full.contains("submitter-trace-line"),
            "TRACE event did not reach the submitter's full-log file — the \
             #585 forensic-complete contract regressed: {full:?}"
        );
    }
}
