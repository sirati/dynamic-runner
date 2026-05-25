//! 1-Hz per-task memory profiler.
//!
//! Reads each active worker's cgroup memory stats once per second and
//! appends one zstd-framed JSONL sample per task to a per-task file
//! under `{output_dir}/memprofile/`. Frame-per-sample lets a hard
//! manager death lose at most one sample (consumers truncate at the
//! last complete frame).
//!
//! Build-up in phases B, C, D — this file currently only declares
//! submodules so the crate compiles while siblings land.

pub mod cgroup_reader;
pub mod config;
pub mod error;
pub mod sample;
pub mod writer;

#[cfg(test)]
mod tests;
