//! Tracing-subscriber wiring for the native extension.
//!
//! Single concern: build the process-wide tracing subscriber. Two sinks
//! compose in one [`Registry`]:
//!
//!   * a **full sink** that records every event (subject only to the
//!     verbosity [`EnvFilter`]), and
//!   * a **stdio sink** that, when *importance mode* is active, passes
//!     ONLY events whose tracing target is [`IMPORTANT_TARGET`]; when
//!     inactive it behaves exactly like the historical single `fmt`
//!     layer (everything to stdout).
//!
//! The gate is one target-keyed *layer filter* ([`important_stdio_filter`]),
//! never a per-call-site `if`. Emitting at `target: "dynrunner_important"`
//! is therefore the only thing a call site needs to know.
//!
//! Selection is read ONCE from the environment at [`init`] time, a fixed
//! contract shared with the Python side (which sets the variables before
//! the first `_native` import):
//!
//!   * [`IMPORTANT_STDIO_ONLY_ENV`] — truthy enables importance mode.
//!   * [`FULL_LOG_FILE_ENV`] — optional path; when set, the full sink
//!     writes there (so stdout can be gated without losing the full log).
//!     When unset, the full log stays on stdout and shell/sbatch
//!     redirection captures it, preserving today's single-stream
//!     behaviour.

use std::fs::OpenOptions;
use std::io;
use std::path::PathBuf;

use chrono::Local;
use tracing::Metadata;
use tracing_subscriber::filter::FilterFn;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

/// Tracing target that marks an event as "important" (LLM-wake-worthy).
/// Events emitted at this target reach stdio even in importance mode.
///
/// Re-exported from the single cross-crate source of truth
/// ([`dynrunner_core::IMPORTANT_TARGET`]); the Python side mirrors it with
/// the child logger `dynamic_runner.important`. Keying the stdio filter
/// ([`important_stdio_filter`]) on this same const is what guarantees the
/// emit target and the gate can never diverge.
pub(crate) use dynrunner_core::IMPORTANT_TARGET;

/// Environment variable selecting importance mode. Truthy = on. Read once
/// at [`init`]; set by Python before the first `_native` import.
pub(crate) const IMPORTANT_STDIO_ONLY_ENV: &str = "DYNRUNNER_IMPORTANT_STDIO_ONLY";

/// Optional destination for the full (unfiltered) sink. When set, the full
/// log is written here instead of stdout, so stdout can carry only the
/// important events without losing the full record.
pub(crate) const FULL_LOG_FILE_ENV: &str = "DYNRUNNER_FULL_LOG_FILE";

/// Where the full (everything) sink writes.
#[derive(Debug)]
pub(crate) enum FullSink {
    /// Share stdout with the stdio sink. Used when no full-log file is
    /// configured; preserves the historical single-stream behaviour.
    Stdout,
    /// A dedicated file. Lets stdout be gated without losing the full log.
    File(PathBuf),
}

/// Resolved logging mode, read once from the environment.
#[derive(Debug)]
pub(crate) struct LogConfig {
    /// Whether the stdio sink is gated to the important target only.
    pub(crate) important_stdio_only: bool,
    /// Destination of the full sink.
    pub(crate) full_sink: FullSink,
}

impl LogConfig {
    /// Read the mode from the process environment. Truthiness mirrors the
    /// common shell convention used on the Python side.
    pub(crate) fn from_env() -> Self {
        let important_stdio_only = std::env::var(IMPORTANT_STDIO_ONLY_ENV)
            .map(|v| is_truthy(&v))
            .unwrap_or(false);
        let full_sink = match std::env::var(FULL_LOG_FILE_ENV) {
            Ok(path) if !path.trim().is_empty() => FullSink::File(PathBuf::from(path)),
            _ => FullSink::Stdout,
        };
        Self {
            important_stdio_only,
            full_sink,
        }
    }
}

/// Truthy test shared with the Python side: `1`, `true`, `yes`, `on`
/// (case-insensitive). Everything else — including unset — is false.
fn is_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
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

/// Build an [`EnvFilter`] from `RUST_LOG`/default-env, falling back to
/// `info`. A fresh instance is built per layer because `EnvFilter` is not
/// `Clone` across layer attachment.
fn env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Build the full (everything) `fmt` layer over `writer`, filtered only by
/// verbosity. Generic over the writer so tests can inject an in-memory
/// buffer.
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
        .with_filter(env_filter())
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

/// Build the stdio `fmt` layer over `writer`. Always verbosity-filtered;
/// additionally target-gated to [`IMPORTANT_TARGET`] when `important_only`.
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
            .with_filter(env_filter())
            .with_filter(important_stdio_filter())
            .boxed()
    } else {
        tracing_subscriber::fmt::layer()
            .with_writer(make_writer)
            .with_filter(env_filter())
            .boxed()
    }
}

/// Assemble the two-layer set for `config`.
///
/// The full file layer exists *iff* a full-log file is configured; with no
/// file the stdio layer alone is the full stream (mode off → ungated → the
/// historical single stdout; mode on → only the important target to stdout
/// while the operator-supplied file would carry the rest). This is one
/// rule, not a per-event branch.
fn build_layers<S>(config: &LogConfig) -> Vec<Box<dyn Layer<S> + Send + Sync>>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    let mut layers: Vec<Box<dyn Layer<S> + Send + Sync>> = Vec::new();
    if let FullSink::File(path) = &config.full_sink {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap_or_else(|e| panic!("failed to open full-log file {}: {e}", path.display()));
        layers.push(full_layer(file));
    }
    layers.push(stdio_layer(io::stdout, config.important_stdio_only));
    layers
}

/// Install the process-wide subscriber from the environment. Idempotent and
/// non-fatal: a second call (or a pre-existing global subscriber) is a no-op.
pub(crate) fn init() {
    let config = LogConfig::from_env();
    let _ = tracing_subscriber::registry()
        .with(build_layers(&config))
        .try_init();
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

        // The full sink keeps the verbose default format for debugging:
        // the same event carries the target and an RFC3339-UTC instant.
        let full_line = full
            .lines()
            .find(|l| l.contains("wake-the-llm"))
            .unwrap_or_else(|| panic!("important line missing from full sink: {full}"));
        assert!(
            full_line.contains(IMPORTANT_TARGET),
            "full sink dropped the target — verbose format regressed: {full_line:?}"
        );
        assert!(
            full_line.contains('Z'),
            "full sink dropped the RFC3339-UTC stamp — verbose format regressed: {full_line:?}"
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

    #[test]
    fn truthy_parsing_matches_shared_contract() {
        for v in ["1", "true", "TRUE", "Yes", "on", " on "] {
            assert!(is_truthy(v), "expected truthy: {v:?}");
        }
        for v in ["0", "false", "no", "off", "", "maybe"] {
            assert!(!is_truthy(v), "expected falsy: {v:?}");
        }
    }

    #[test]
    fn no_full_log_file_yields_a_single_stdout_stream() {
        // Default config (no file, gate off) must produce exactly one
        // layer — the historical single stdout stream, no duplication.
        let config = LogConfig {
            important_stdio_only: false,
            full_sink: FullSink::Stdout,
        };
        let layers = build_layers::<Registry>(&config);
        assert_eq!(
            layers.len(),
            1,
            "expected a single stdout layer when no full-log file is set"
        );
    }

    #[test]
    fn full_log_file_adds_a_second_layer() {
        let dir = tempfile::tempdir().unwrap();
        let config = LogConfig {
            important_stdio_only: true,
            full_sink: FullSink::File(dir.path().join("full.log")),
        };
        let layers = build_layers::<Registry>(&config);
        assert_eq!(layers.len(), 2, "expected full-file layer + stdio layer");
    }
}
