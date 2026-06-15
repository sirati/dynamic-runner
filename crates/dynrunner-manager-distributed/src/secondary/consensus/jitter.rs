//! Jitter-source seam for the secondary-side mesh-consensus FSM.
//!
//! The probe-fan-out scheduler ([`super::fsm::SecondaryConsensusFsm`])
//! offsets each per-target next-fire instant by `PROBE_BASE_PERIOD ±
//! jitter_ms` so a broadcast `SuspectPeers` that opens the round on N
//! secondaries simultaneously does NOT produce a synchronized N-way
//! probe storm against the suspected peer.
//!
//! The FSM never calls into stdlib `rand` / system entropy directly —
//! it consults a [`JitterSource`] trait object injected at construction.
//! Production uses [`XorshiftJitter`], a small stdlib-only deterministic
//! PRNG seeded from `(self_id, creation Instant)`; tests inject
//! [`FixedJitter`] for byte-exact deadline assertions.
//!
//! Cryptographic randomness would be overkill: the jitter exists purely
//! to desynchronize probe storms, not to defend against an adversary
//! predicting probe times.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Instant;

use super::PROBE_JITTER_MS;

/// Per-probe-fire jitter source. Each call returns a fresh sample in
/// the closed interval `-PROBE_JITTER_MS..=PROBE_JITTER_MS` (in
/// milliseconds), which the FSM ADDS to [`super::PROBE_BASE_PERIOD`] to
/// compute the next-fire instant for one target.
///
/// `&mut self` so impls can carry per-instance state (the production
/// xorshift carries a seed; the test fixed-value carries the constant
/// it returns). The FSM owns the source by value; trait objects are
/// supported via `Box<dyn JitterSource>` if a future caller needs
/// runtime polymorphism.
pub trait JitterSource: Send + 'static {
    /// Returns a fresh jitter sample in milliseconds, clamped to
    /// `-PROBE_JITTER_MS..=PROBE_JITTER_MS` by contract on the impl.
    /// Callers MAY assume the returned value respects the bound (it is
    /// not re-clamped by the FSM); a defective impl that returns a
    /// larger value would only de-synchronize probes further, not
    /// corrupt the protocol.
    fn next_ms(&mut self) -> i32;
}

/// Production jitter source: a small stdlib-only xorshift PRNG seeded
/// from `(self_id, creation Instant)`. Avoids pulling `rand` into the
/// dependency graph for what is structurally a probe-storm
/// desynchronizer.
///
/// Cycle length is 2^64 − 1 and statistical quality is far beyond what
/// the use case requires (each call goes through one xorshift step then
/// a modulo into the `[-PROBE_JITTER_MS, PROBE_JITTER_MS]` window). The
/// per-instance seed plus the creation-time entropy means two
/// secondaries on the same node-id (which should never happen, but the
/// test environment occasionally constructs lookalike-id fixtures)
/// still see distinct streams.
#[derive(Debug, Clone)]
pub struct XorshiftJitter {
    state: u64,
}

impl XorshiftJitter {
    /// Construct seeded from a peer-id hash mixed with the supplied
    /// creation instant. Production callers pass their own
    /// `self_id` and `Instant::now()`; tests should NOT use this and
    /// should reach for [`FixedJitter`] instead.
    pub fn new(self_id: &str, created_at: Instant) -> Self {
        let mut hasher = DefaultHasher::new();
        self_id.hash(&mut hasher);
        // `Instant` is not directly hashable in a portable way; mix in
        // its elapsed-since-process-start nanosecond count via the
        // standard duration_since(any-fixed-reference) trick. We use
        // `created_at - created_at` as the zero reference and instead
        // pull entropy from `created_at`'s memory representation, which
        // does change run-to-run. A debug-formatted instant carries the
        // tick count on every supported platform.
        format!("{created_at:?}").hash(&mut hasher);
        let seed = hasher.finish();
        // Guard against a zero seed — xorshift converges to zero from
        // zero. The DefaultHasher with non-empty inputs essentially
        // never returns 0, but we belt-and-braces it.
        Self {
            state: if seed == 0 { 0x9E3779B97F4A7C15 } else { seed },
        }
    }

    /// One xorshift step (xorshift64*: shifts 13/7/17). Returns the
    /// updated state; the FSM reads it via [`Self::next_ms`].
    fn step(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
}

impl JitterSource for XorshiftJitter {
    fn next_ms(&mut self) -> i32 {
        // Symmetric window: `[-PROBE_JITTER_MS, PROBE_JITTER_MS]` is
        // `2 * PROBE_JITTER_MS + 1` distinct integer values. We sample
        // a u64 step and modulo into that range, then shift down to
        // recenter on zero.
        let span = (2 * PROBE_JITTER_MS as u64) + 1;
        let raw = self.step() % span;
        raw as i32 - PROBE_JITTER_MS
    }
}

/// Test-only jitter source: every call returns the constant supplied at
/// construction. The FSM uses it to make probe-deadline assertions
/// byte-exact across CI runs.
///
/// Carries a single `i32`; the caller is responsible for choosing a
/// value inside `-PROBE_JITTER_MS..=PROBE_JITTER_MS` if they want the
/// production contract to hold (out-of-band values are accepted and
/// faithfully returned, which is occasionally useful for boundary
/// testing).
#[derive(Debug, Clone, Copy)]
pub struct FixedJitter(pub i32);

impl JitterSource for FixedJitter {
    fn next_ms(&mut self) -> i32 {
        self.0
    }
}
