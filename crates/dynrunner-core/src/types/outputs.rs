//! Keyed task output values.
//!
//! Producers commit a [`TaskOutputs`] map alongside their `Done` response;
//! the dispatcher attaches each consumer's predecessor outputs to its
//! task assignment so the consumer reads them verbatim from its `Task`
//! object. The framework never inspects keys or values — they round-trip
//! through serde-JSON only.
//!
//! Soft caps are advisory: oversize values still propagate, but
//! [`check_soft_caps`] emits a `tracing::warn!` once per overflow class
//! so the operator notices accumulator bloat before it dominates the
//! `result_data` wire.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Map of consumer-chosen string keys to the producer's published values.
///
/// Keys are deliberately stable-sorted (`BTreeMap`) so the wire bytes are
/// deterministic for a given accumulator content — useful for diff-based
/// replay tests and for the CRDT-replicated `TaskCompleted` mutation.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TaskOutputs(pub BTreeMap<String, ResultValue>);

/// One published value. Either inlined into the wire JSON or a path
/// pointing at a post-publish destination on the shared mount the
/// consumer already reads from.
///
/// Adjacent tagging (`{"kind": ..., "value": ...}`) is the load-bearing
/// shape: serde rejects internally-tagged newtype variants that wrap a
/// non-map payload (`String` is not a map), and the consuming Python
/// side reads `result["kind"]` and `result["value"]` directly. Keep this
/// attribute exactly as-is — switching to `tag = "kind"` alone breaks
/// `serde_json::to_string` at runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ResultValue {
    /// Inline string value. Soft-cap warns at >64 KiB per value
    /// and >1 MiB per task; the value still propagates.
    Inline(String),
    /// Post-publish destination path (same shared mount the consumer
    /// reads — equivalent to the `dst` arg to `Task.publish`).
    File(String),
}

/// Per-value inline soft cap. Above this size, [`check_soft_caps`] emits
/// a warn for the offending value (still propagates).
pub const INLINE_VALUE_SOFT_CAP_BYTES: usize = 64 * 1024;

/// Per-task total inline soft cap (sum of all `Inline` byte lengths).
/// Above this, [`check_soft_caps`] emits a separate warn (still propagates).
pub const PER_TASK_INLINE_SOFT_CAP_BYTES: usize = 1024 * 1024;

/// Inspect `outputs` against the inline soft caps and emit at most one
/// `tracing::warn!` per overflow class (per-value, per-task total). The
/// helper has no failure mode — values always propagate regardless.
///
/// Called from the worker-side commit path; lives here so the data
/// module owns its own policy.
pub fn check_soft_caps(outputs: &TaskOutputs, task_id: &str) {
    let mut total: usize = 0;
    let mut per_value_warned = false;
    let mut largest_offender: Option<(&str, usize)> = None;

    for (key, value) in outputs.0.iter() {
        let ResultValue::Inline(s) = value else {
            continue;
        };
        let len = s.len();
        total = total.saturating_add(len);

        let over = len > INLINE_VALUE_SOFT_CAP_BYTES;
        let larger_than_prev = largest_offender.is_none_or(|(_, prev)| len > prev);
        if over && larger_than_prev {
            largest_offender = Some((key.as_str(), len));
        }
        per_value_warned |= over;
    }

    if let Some((key, len)) = largest_offender.filter(|_| per_value_warned) {
        tracing::warn!(
            task_id = %task_id,
            key = %key,
            len_bytes = len,
            cap_bytes = INLINE_VALUE_SOFT_CAP_BYTES,
            "TaskOutputs inline value exceeds per-value soft cap"
        );
    }

    if total > PER_TASK_INLINE_SOFT_CAP_BYTES {
        tracing::warn!(
            task_id = %task_id,
            total_bytes = total,
            cap_bytes = PER_TASK_INLINE_SOFT_CAP_BYTES,
            "TaskOutputs total inline payload exceeds per-task soft cap"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_outputs_roundtrip_inline_and_file() {
        let mut map: BTreeMap<String, ResultValue> = BTreeMap::new();
        map.insert("nonce".to_string(), ResultValue::Inline("xyz".to_string()));
        map.insert(
            "artifact".to_string(),
            ResultValue::File("/app/out-network/build/foo.tar".to_string()),
        );
        let outputs = TaskOutputs(map);

        let json = serde_json::to_string(&outputs).expect("serialise");
        let parsed: TaskOutputs = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(outputs, parsed);
    }

    #[test]
    fn result_value_inline_serde_shape() {
        // Adjacent tagging: {"kind": "inline", "value": "hello"}. The
        // Python consumer reads `result["kind"]` and `result["value"]`
        // directly — keep both keys locked.
        let v = ResultValue::Inline("hello".to_string());
        let json = serde_json::to_value(&v).expect("to_value");
        assert_eq!(json["kind"], "inline");
        assert_eq!(json["value"], "hello");
    }

    #[test]
    fn result_value_file_serde_shape() {
        let v = ResultValue::File("/tmp/x".to_string());
        let json = serde_json::to_value(&v).expect("to_value");
        assert_eq!(json["kind"], "file");
        assert_eq!(json["value"], "/tmp/x");
    }

    #[test]
    fn check_soft_caps_per_value_overflow_does_not_panic() {
        let mut map: BTreeMap<String, ResultValue> = BTreeMap::new();
        // 64 KiB + 1 — trips the per-value cap.
        map.insert(
            "big".to_string(),
            ResultValue::Inline("x".repeat(INLINE_VALUE_SOFT_CAP_BYTES + 1)),
        );
        let outputs = TaskOutputs(map);
        // No panic; helper has no return — soft caps are advisory.
        check_soft_caps(&outputs, "task-under-test");
    }

    #[test]
    fn check_soft_caps_per_task_total_overflow_does_not_panic() {
        let mut map: BTreeMap<String, ResultValue> = BTreeMap::new();
        // 33 entries of 32 KiB each = 1056 KiB total, over 1 MiB cap;
        // each individual value is under the 64 KiB per-value cap.
        for i in 0..33 {
            map.insert(
                format!("k{}", i),
                ResultValue::Inline("x".repeat(32 * 1024)),
            );
        }
        let outputs = TaskOutputs(map);
        check_soft_caps(&outputs, "task-under-test");
    }

    #[test]
    fn check_soft_caps_under_caps_is_silent() {
        let mut map: BTreeMap<String, ResultValue> = BTreeMap::new();
        map.insert("k".to_string(), ResultValue::Inline("small".to_string()));
        let outputs = TaskOutputs(map);
        check_soft_caps(&outputs, "task-under-test");
    }

    #[test]
    fn check_soft_caps_ignores_file_values() {
        let mut map: BTreeMap<String, ResultValue> = BTreeMap::new();
        // A `File` whose path-string happens to be huge does not count
        // against the inline caps — the path is a pointer, not the payload.
        map.insert(
            "f".to_string(),
            ResultValue::File("x".repeat(PER_TASK_INLINE_SOFT_CAP_BYTES + 1)),
        );
        let outputs = TaskOutputs(map);
        check_soft_caps(&outputs, "task-under-test");
    }
}
