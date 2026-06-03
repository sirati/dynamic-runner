//! Test-only capture of "important" (LLM-wake) tracing events.
//!
//! Single concern: a reusable `tracing_subscriber::Layer` that records
//! every event whose target is the importance marker — its `message`
//! plus its other fields (debug-formatted) — so a deterministic test
//! can assert an important event fired exactly once at its trigger,
//! that nothing else leaks onto the marker, and that a discriminating
//! field (e.g. `bucket`) carries the expected value. Shared across the
//! modules that emit important events (`primary::important_events`,
//! `primary::retry_bucket`, …) so the capture layer is defined once
//! rather than re-derived per test file.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tracing::Metadata;
use tracing_subscriber::filter::FilterFn;
use tracing_subscriber::layer::{Context, Layer};

/// The importance marker target — the single canonical const from
/// `dynrunner_core`, re-exported here so the capture layer + filter
/// reference it by name without re-deriving the literal (and without
/// reaching into any emitter module's `pub(crate)` const). Equal by
/// contract to every emitter's `IMPORTANT_TARGET` and to `dynrunner-pyo3`'s
/// `logging::IMPORTANT_TARGET`.
pub(crate) use dynrunner_core::IMPORTANT_TARGET;

/// One captured important event.
#[derive(Clone, Debug, Default)]
pub(crate) struct CapturedEvent {
    /// The `message` field, debug-formatted (empty if absent).
    pub(crate) message: String,
    /// Every non-`message` field, name → debug-formatted value.
    pub(crate) fields: HashMap<String, String>,
}

/// Records every important event into a shared buffer.
#[derive(Clone, Default)]
pub(crate) struct ImportantCapture(Arc<Mutex<Vec<CapturedEvent>>>);

impl ImportantCapture {
    /// Snapshot of every captured event, in emission order.
    pub(crate) fn events(&self) -> Vec<CapturedEvent> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Snapshot of just the captured messages, in emission order.
    pub(crate) fn messages(&self) -> Vec<String> {
        self.events().into_iter().map(|e| e.message).collect()
    }
}

impl<S> Layer<S> for ImportantCapture
where
    S: tracing::Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        struct EventVisitor<'a>(&'a mut CapturedEvent);
        impl tracing::field::Visit for EventVisitor<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                let rendered = format!("{value:?}");
                if field.name() == "message" {
                    self.0.message = rendered;
                } else {
                    self.0.fields.insert(field.name().to_string(), rendered);
                }
            }
        }
        let mut captured = CapturedEvent::default();
        event.record(&mut EventVisitor(&mut captured));
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(captured);
    }
}

/// A layer filter passing only events on the importance marker target —
/// pair with [`ImportantCapture`] via `.with_filter(...)`.
pub(crate) fn important_only() -> FilterFn<fn(&Metadata<'_>) -> bool> {
    fn predicate(meta: &Metadata<'_>) -> bool {
        meta.target() == IMPORTANT_TARGET
    }
    FilterFn::new(predicate as fn(&Metadata<'_>) -> bool)
}
