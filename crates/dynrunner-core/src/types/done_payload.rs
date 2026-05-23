//! Wire shape of the Python worker's `DoneResponse.result_data` body.
//!
//! Single source of truth for the JSON wrapper the worker's
//! `_encode_done_payload` (in `python/dynamic_runner/worker/runtime.py`)
//! emits. The Python encoder writes a body of the form
//!
//! ```json
//! {"warnings": N,
//!  "filtered": M,
//!  "outputs": { key → {"kind": ..., "value": ...} }}
//! ```
//!
//! where the `outputs` field is the PRODUCING TASK's own keyed
//! outputs (the same shape [`TaskOutputs`] deserialises from), NOT a
//! `dep_id`-nested map — the producer only knows about its own keys.
//! The dispatcher later gathers per-dependent maps from the framework
//! cache to build a consumer's `predecessor_outputs`.
//!
//! Each top-level key is OMITTED when its value would be zero/empty.
//! When all three are absent the encoder returns `None` (byte-
//! identical to the pre-feature legacy wire shape); a successful
//! decode of a fully-empty payload is therefore not reachable from
//! production traffic.
//!
//! Decoder-side concern: only the `outputs` sub-object is consumed by
//! the cluster-state populate paths (distributed + local manager).
//! `warnings` and `filtered` are worker-local diagnostics — they ride
//! the wire so a consumer that opts in can read them, but the
//! framework's own cache machinery ignores them. Serde's default
//! "unknown fields are dropped" behaviour (NO `deny_unknown_fields`)
//! handles the silent skip without per-field plumbing.
//!
//! Placement rationale: this is the cross-language wire contract for
//! `result_data`. Both `dynrunner-manager-distributed` (CRDT apply
//! path) and `dynrunner-manager-local` (in-process cache populate)
//! decode the bytes and need the same definition; the `types` module
//! is the natural owner because the `outputs` field type is already
//! [`TaskOutputs`] (re-exported from `types::outputs`). Duplicating
//! the struct in each consumer crate would violate the "no duplicated
//! logic" rule — both call sites must use this single re-export.

use serde::{Deserialize, Serialize};

use super::outputs::TaskOutputs;

/// Mirror of the Python worker's `_encode_done_payload` body shape.
///
/// Counters (`warnings`, `filtered`) are not consumed by the
/// framework's keyed-outputs cache — only the `outputs` field is.
/// The struct uses `#[derive(Default)]` so a decode failure on
/// upstream paths can fall back to "empty outputs" without an
/// explicit constructor (matches the warn-and-store-empty contract
/// the call sites enforce today).
///
/// `#[serde(default)]` on `outputs` is load-bearing: the encoder
/// OMITS `outputs` when the accumulator is empty (a counters-only
/// payload), so a deserialize of `{"warnings": 2}` must yield
/// `DonePayload { outputs: TaskOutputs::default() }`, not a
/// missing-field error.
///
/// Note: this struct is intentionally NOT `Serialize`. The
/// Rust-side cache machinery never writes the wrapper — only the
/// Python worker produces it. Tests that construct the wire bytes
/// for round-trip coverage use `serde_json::json!(...)` literals
/// that mirror the encoder's output verbatim.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct DonePayload {
    #[serde(default)]
    pub outputs: TaskOutputs,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ResultValue;

    /// Full encoder shape — `warnings`, `filtered`, and the keyed
    /// `outputs` map. Pins that the decoder extracts the `outputs`
    /// sub-object correctly when all three top-level keys are present.
    /// This is the shape consumers see when a task uses both the
    /// `WorkerOutput` counters and the keyed-publish API in the same
    /// handler.
    #[test]
    fn decodes_full_python_wrapper_shape() {
        let wire = serde_json::to_vec(&serde_json::json!({
            "warnings": 2,
            "filtered": 1,
            "outputs": {
                "nonce": {"kind": "inline", "value": "xyz"},
                "artifact": {"kind": "file", "value": "/app/out/foo.tar"},
            }
        }))
        .expect("encode wrapper");

        let body: DonePayload = serde_json::from_slice(&wire).expect("decode");
        assert_eq!(
            body.outputs.0.get("nonce"),
            Some(&ResultValue::Inline("xyz".to_string()))
        );
        assert_eq!(
            body.outputs.0.get("artifact"),
            Some(&ResultValue::File("/app/out/foo.tar".to_string()))
        );
        assert_eq!(body.outputs.0.len(), 2);
    }

    /// Outputs-only payload (no counters): the encoder omits the
    /// `warnings`/`filtered` keys when both are zero. Decoder must
    /// still extract the `outputs` map.
    #[test]
    fn decodes_outputs_only_wrapper_shape() {
        let wire = serde_json::to_vec(&serde_json::json!({
            "outputs": {
                "k": {"kind": "inline", "value": "v"},
            }
        }))
        .expect("encode wrapper");

        let body: DonePayload = serde_json::from_slice(&wire).expect("decode");
        assert_eq!(
            body.outputs.0.get("k"),
            Some(&ResultValue::Inline("v".to_string()))
        );
    }

    /// Counters-only payload (no keyed outputs): the encoder omits
    /// the `outputs` key when the accumulator is empty. Decoder must
    /// produce an empty `TaskOutputs` via the `#[serde(default)]`
    /// attribute on the field, NOT a missing-field error.
    #[test]
    fn decodes_counters_only_wrapper_shape() {
        let wire = serde_json::to_vec(&serde_json::json!({
            "warnings": 3,
            "filtered": 5,
        }))
        .expect("encode wrapper");

        let body: DonePayload = serde_json::from_slice(&wire).expect("decode");
        assert!(body.outputs.0.is_empty());
    }

    /// Garbage bytes are a decode error — the call-site convention
    /// is to `unwrap_or_default()` (or pattern-match and warn) so
    /// dependents see an empty cache entry rather than a panic. This
    /// test pins that the error is reported, not silently swallowed
    /// at the struct level.
    #[test]
    fn malformed_bytes_yield_decode_error() {
        let garbage: &[u8] = b"not-json-at-all";
        let result: Result<DonePayload, _> = serde_json::from_slice(garbage);
        assert!(result.is_err());
    }
}
