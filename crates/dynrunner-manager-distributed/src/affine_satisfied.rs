//! The OPTIONAL per-(gate,node) "already satisfied" probe (#537).
//!
//! ## The one concern
//! ONE seam: "ask the consumer whether THIS node already holds the product of
//! a [`TaskKind::SecondaryAffine`](dynrunner_core::TaskKind) gate, BEFORE
//! dispatching the gate body to a worker subprocess (#577)". A `true`
//! verdict short-circuits the entire run-once executor — no worker
//! dispatch, no `QueuedAfterLocalDependency` / `LocalDependencyReleased`
//! frames on the wire — exactly as if the gate body had already run on
//! this node (the gate's hash enters `affine_done`; the dispatch router's
//! `AlreadyDone` branch releases the dependent work task on the unchanged
//! path). A `false` verdict (or NO probe registered) falls through to the
//! gate-body-on-worker dispatch path bit-for-bit.
//!
//! ## Why a SEPARATE port (not a sentinel return from the gate body)
//! The consumer's gate-body worker handler can ALREADY mark a gate
//! locally-done by returning success without doing real work. What this
//! port adds is "the framework SKIPS the worker dispatch entirely" — the
//! gate-body path today still threads a worker-subprocess dispatch (a
//! per-type subprocess spawn + assignment frame), the run-once latch
//! bookkeeping, and a pair of CRDT-visible frames per dependent. On the
//! PRODUCING node (where the consumer's local logic already left the
//! product valid in the local store) that whole scaffolding is wasted on
//! every dependent of every gate. A distinct PROBE port — consulted BEFORE
//! `ensure_affine_import` touches the run-once latch — is the right level:
//! the framework asks "do you already have this?", and on YES treats the
//! gate exactly like a previously-imported one. A sentinel return from the
//! gate body cannot avoid the dispatch, so the scaffolding still fires.
//!
//! ## Why SYNC
//! The probe is a "do I already have this path locally" check — a single
//! local filesystem stat at most. An async probe would force a second
//! off-loop seam ([`tokio::task::spawn_local`] + completion channel),
//! defeating the whole point of the short-circuit (avoid scheduler work).
//! The trait stays object-safe with the generic on the TRAIT (not the
//! method). The probe is held as `Arc<dyn AffineSatisfiedProbe<I>>` so it
//! survives the relocation handoff onto the observer tail.
//!
//! ## Failure semantics
//! A probe that PANICS / RAISES (Python) is treated as
//! [`ProbeOutcome::Errored`] and CACHED with a short expiry — the dependent
//! falls through to today's import path each time, but the probe is not
//! hammered at high frequency. NEVER POISONS `affine_done`: only an actual
//! `Satisfied` verdict marks the gate locally-done. This matches the import
//! action's "never poison the done set" rule.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dynrunner_core::{Identifier, TaskInfo};

/// The classified verdict of one probe call, cached on the secondary's
/// [`crate::secondary::lifecycle::Operational`] state so a high-volume run
/// (thousands of dependents per gate) never calls the probe more than
/// strictly necessary. Held per gate-hash; the cache lives on the
/// node-local operational state alongside `affine_done` / `affine_running`.
#[derive(Debug, Clone)]
pub enum ProbeOutcome {
    /// The probe reported the gate's product is LOCALLY PRESENT on this
    /// node. Caches FOREVER for the run: the consumer would never
    /// un-publish a locally-imported store path on a healthy run (and if
    /// they did, the dependent work task would fail with its own #495
    /// class — the same shape `affine_done` already tolerates). On the
    /// first `Satisfied` verdict the executor inserts the gate's hash into
    /// `affine_done` immediately, so subsequent dependents short-circuit
    /// through the existing `affine_done.contains` check and never even
    /// consult the cache.
    Satisfied,
    /// The probe reported the gate's product is NOT yet present on this
    /// node. Cached with [`PROBE_NEGATIVE_CACHE_TTL`] expiry so a
    /// mid-run-published gate (e.g. a dynamic-graph-discovered common_dep
    /// the consumer ships during a later phase) re-probes after the TTL
    /// elapses, but a FIXED-graph batch of N dependents in flight at the
    /// same time only probes ONCE per gate per TTL window.
    NotSatisfied { cached_at: Instant },
    /// The probe RAISED — treat as NotSatisfied for routing (today's import
    /// path runs) but with a shorter expiry so a flaky probe does not
    /// hammer the consumer callable, AND so a CORRECTABLE error
    /// (recompilation between attempts, e.g. an `ImportError` typo) is
    /// re-probed quickly. The dependent falls through to today's behaviour
    /// each time the cache misses — never poisons `affine_done`.
    Errored { cached_at: Instant },
}

/// Time-to-live for a [`ProbeOutcome::NotSatisfied`] cache entry. After this
/// has elapsed the next dependent's gate-resolution re-probes — covers the
/// mid-run-publish case where a consumer ships a previously-not-present
/// closure during a later phase. Chosen conservatively: long enough that a
/// stable batch's N dependents share ONE probe call per gate; short enough
/// that a dynamic graph publishes are noticed within a phase boundary.
pub const PROBE_NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(30);

/// Time-to-live for a [`ProbeOutcome::Errored`] cache entry. SHORTER than
/// `PROBE_NEGATIVE_CACHE_TTL` so a transiently-failing probe is re-tried
/// promptly once the cause clears (e.g. a momentary FS error inside
/// `is_path_locally_present`), but still bounded so a persistently-erroring
/// probe is not called at dispatch frequency.
pub const PROBE_ERROR_CACHE_TTL: Duration = Duration::from_secs(5);

impl ProbeOutcome {
    /// Whether THIS cached verdict is STILL FRESH at `now`. A `Satisfied`
    /// verdict never expires (the locally-present-store-path invariant
    /// persists for the run); `NotSatisfied` expires after
    /// [`PROBE_NEGATIVE_CACHE_TTL`]; `Errored` expires after
    /// [`PROBE_ERROR_CACHE_TTL`]. Once expired, the executor re-probes.
    pub fn is_fresh(&self, now: Instant) -> bool {
        match self {
            ProbeOutcome::Satisfied => true,
            ProbeOutcome::NotSatisfied { cached_at } => {
                now.saturating_duration_since(*cached_at) < PROBE_NEGATIVE_CACHE_TTL
            }
            ProbeOutcome::Errored { cached_at } => {
                now.saturating_duration_since(*cached_at) < PROBE_ERROR_CACHE_TTL
            }
        }
    }
}

/// Port the secondary's run-once affine executor crosses to ASK the consumer
/// whether the gate's product is already locally present on this node
/// (#537). Consulted BEFORE the run-once latch in
/// [`crate::secondary::SecondaryCoordinator::ensure_affine_import`]; a
/// `Satisfied` verdict short-circuits the whole executor (the dependent
/// dispatches on the `AlreadyDone` path).
///
/// Single concern: "does THIS node already hold the product?". The
/// implementation does NOT perform any import work — that is done by the
/// gate body running in a worker subprocess (#577) for nodes that answer
/// "not yet". A probe that PANICS / RAISES is classified
/// [`ProbeOutcome::Errored`] (the dependent falls through to the gate
/// body's worker dispatch path); the probe NEVER poisons `affine_done`.
pub trait AffineSatisfiedProbe<I: Identifier>: Send + Sync {
    /// Report whether `task`'s product is already locally present on THIS
    /// node. `true` ⇒ the executor inserts `task`'s hash into
    /// `affine_done` and the dependent work task dispatches on the
    /// unchanged `AlreadyDone` path; `false` ⇒ today's import path runs.
    /// Must be a fast local check (FS stat at most); SYNC by design
    /// because an async second seam would defeat the short-circuit.
    fn is_satisfied(&self, task: &TaskInfo<I>) -> bool;
}

/// A registered probe handle a secondary holds. `None` (the default ⇒ the
/// overwhelming case for consumers that never register one) leaves the
/// executor with today's behaviour bit-for-bit: every gate is dispatched
/// to a worker subprocess (#577) for its body to run. `Some` enables the
/// per-node short-circuit.
pub type AffineSatisfiedProbeHandle<I> = Option<Arc<dyn AffineSatisfiedProbe<I>>>;
