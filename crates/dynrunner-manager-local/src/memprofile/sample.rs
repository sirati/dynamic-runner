//! Per-sample serializable type.
//!
//! SIBLING-DEP: this is a minimal stub introduced by the writer
//! work so the writer's public API (`write_sample_as_frame(&Sample)`)
//! has a concrete type to point at. The full struct (top-level
//! convenience keys + `memory_stat: BTreeMap<String, u64>`) lands
//! with the sample work; the coordinator reconciles on merge by
//! keeping the richer definition. The serde shape (one JSON object
//! per sample) is what the writer commits to on disk, so any sibling
//! redefinition must remain `#[derive(Serialize)]`.

use std::collections::BTreeMap;

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Sample {
    pub t_ns: u64,
    pub t_rel_ns: u64,
    pub worker_id: u32,
    pub memory_current: u64,
    pub swap_current: u64,
    pub memory_stat: BTreeMap<String, u64>,
}
