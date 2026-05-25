use std::collections::BTreeMap;

use serde::Serialize;

/// One memprofile sample for one worker at one tick.
///
/// `memory_stat` carries the cgroup-v2 `memory.stat` file verbatim
/// (key/value pairs); the kernel adds keys across versions and we
/// preserve all of them rather than picking a fixed subset. The
/// top-level mirrors of `memory_current` / `swap_current` exist for
/// `jq`-style ergonomics — consumers shouldn't need to index into
/// the nested map for the two hottest fields.
///
/// `BTreeMap` (not `HashMap`) so serde emits the stat keys in
/// alphabetical order — gives deterministic output for diffing,
/// hashing, and snapshot tests.
#[derive(Debug, Clone, Serialize)]
pub struct Sample {
    /// Wall-clock nanoseconds since UNIX_EPOCH at the moment the
    /// sampler ticked. UTC; for cross-log correlation.
    pub t_ns: u64,
    /// Monotonic nanoseconds since the task was assigned to the
    /// worker. Sourced from `Instant::elapsed()`; safe across NTP
    /// adjustments.
    pub t_rel_ns: u64,
    /// Worker that owns the cgroup this sample was read from.
    /// Redundant with the filename (`worker-N`) but inlined so
    /// consumers can join across files without parsing filenames.
    pub worker_id: u32,
    /// `memory.current` for the worker's leaf cgroup, bytes.
    pub memory_current: u64,
    /// `memory.swap.current` for the worker's leaf cgroup, bytes.
    pub swap_current: u64,
    /// Verbatim parse of `memory.stat`; alphabetically ordered.
    pub memory_stat: BTreeMap<String, u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `Sample` whose `memory_stat` is inserted out of
    /// alphabetical order. `BTreeMap`'s ordering invariant means
    /// serde must still emit the keys alphabetically — this is the
    /// deterministic-output contract callers (frame decoder, diff
    /// tooling, snapshot tests) depend on.
    #[test]
    fn serializes_with_alphabetic_memory_stat_keys() {
        let mut memory_stat = BTreeMap::new();
        memory_stat.insert("c".to_string(), 3);
        memory_stat.insert("a".to_string(), 1);
        memory_stat.insert("b".to_string(), 2);

        let sample = Sample {
            t_ns: 1_747_900_000_000_000_000,
            t_rel_ns: 1_234_567_890,
            worker_id: 3,
            memory_current: 5_368_709_120,
            swap_current: 0,
            memory_stat,
        };

        let json = serde_json::to_string(&sample).expect("serialize");
        assert!(
            json.contains(r#""memory_stat":{"a":1,"b":2,"c":3}"#),
            "expected alphabetic memory_stat ordering in JSON: {json}"
        );
    }

    /// Documents that struct declaration order is the JSON wire
    /// order: `t_ns` first, `memory_stat` last. Frame-decoded logs
    /// then read predictably (hot scalars up front, the nested map
    /// trailing).
    #[test]
    fn top_level_field_order_matches_struct() {
        let sample = Sample {
            t_ns: 42,
            t_rel_ns: 7,
            worker_id: 1,
            memory_current: 100,
            swap_current: 0,
            memory_stat: BTreeMap::new(),
        };

        let json = serde_json::to_string(&sample).expect("serialize");

        // First field after the opening brace must be t_ns.
        assert!(
            json.starts_with(r#"{"t_ns""#),
            "expected JSON to start with t_ns: {json}"
        );

        // memory_stat must be the trailing field: its key appears
        // after every other field name, and the value (here an
        // empty object) immediately precedes the closing brace.
        let memory_stat_pos = json
            .find(r#""memory_stat""#)
            .expect("memory_stat key present");
        for other in [
            r#""t_ns""#,
            r#""t_rel_ns""#,
            r#""worker_id""#,
            r#""memory_current""#,
            r#""swap_current""#,
        ] {
            let pos = json.find(other).expect("field present");
            assert!(
                pos < memory_stat_pos,
                "expected {other} before memory_stat in {json}"
            );
        }
        assert!(
            json.ends_with(r#""memory_stat":{}}"#),
            "expected memory_stat as the trailing field: {json}"
        );
    }
}
