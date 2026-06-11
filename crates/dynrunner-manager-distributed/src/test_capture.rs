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

/// One captured event together with its level — the record of the
/// general (non-importance) [`TargetCapture`].
#[derive(Clone, Debug)]
pub(crate) struct LeveledEvent {
    pub(crate) level: tracing::Level,
    pub(crate) event: CapturedEvent,
}

/// Debug-render an event's `message` + remaining fields — the one
/// shared visitor behind every capture layer in this module.
fn render_event(event: &tracing::Event<'_>) -> CapturedEvent {
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
    captured
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
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(render_event(event));
    }
}

/// Records every event emitted on ONE exact tracing target, at any
/// level, with the level preserved — so a log-shape test can assert
/// "exactly one INFO, everything else DEBUG". The target
/// discrimination happens INSIDE `on_event` rather than via a layer
/// filter: a filtered install caches per-callsite `Interest` in the
/// PROCESS-GLOBAL table and can poison sibling tests' callsites (see
/// `capture_important` and `staging`'s `WarnCapture`); an unfiltered
/// layer caches `Interest::always`, which never suppresses anyone.
/// Unlike `capture_important`'s synchronous-body constraint, an
/// always-interest install is also safe to hold (via
/// `tracing::subscriber::set_default`) across `.await`s on a
/// current-thread runtime: while this dispatcher is registered, every
/// interest-cache rebuild yields at least `sometimes`, so no emission
/// can be cached away from it.
#[derive(Clone)]
pub(crate) struct TargetCapture {
    target: &'static str,
    events: Arc<Mutex<Vec<LeveledEvent>>>,
}

impl TargetCapture {
    pub(crate) fn for_target(target: &'static str) -> Self {
        Self {
            target,
            events: Arc::default(),
        }
    }

    /// Snapshot of every captured event, in emission order.
    pub(crate) fn events(&self) -> Vec<LeveledEvent> {
        self.events.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl<S> Layer<S> for TargetCapture
where
    S: tracing::Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != self.target {
            return;
        }
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(LeveledEvent {
                level: *event.metadata().level(),
                event: render_event(event),
            });
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

/// Run `body` with an [`ImportantCapture`] installed as the thread-local
/// default subscriber (filtered to the importance marker), returning every
/// important event `body` emitted, in order.
///
/// `body` must produce its important events SYNCHRONOUSLY: the capture is
/// reliable only when there is no `.await` between installing the subscriber
/// and the emission. `tracing` caches a per-callsite `Interest` in a
/// PROCESS-GLOBAL table; that cache is concurrently re-poisoned by sibling
/// tests that install a `fmt::try_init` global subscriber (which has no
/// interest in [`IMPORTANT_TARGET`], so a callsite it touches first caches
/// `Interest::never`). The `with_default` install registers THIS subscriber
/// and rebuilds the interest cache immediately before `body` runs, and a
/// synchronous, yield-free `body` leaves no window for a concurrent
/// re-poison before its emissions — so the capture is deterministic. A
/// subscriber held across an `.await` (e.g. `set_default` around an async
/// `run()`) exposes that window and flakes; drive the emission synchronously
/// instead. This is the single owning helper for the capture-construction
/// the importance tests share.
pub(crate) fn capture_important(body: impl FnOnce()) -> Vec<CapturedEvent> {
    use tracing_subscriber::Layer;
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;

    let capture = ImportantCapture::default();
    let subscriber = Registry::default().with(capture.clone().with_filter(important_only()));
    tracing::subscriber::with_default(subscriber, body);
    capture.events()
}
