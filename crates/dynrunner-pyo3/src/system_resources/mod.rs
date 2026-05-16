//! Runtime resource detection + CLI-spec parsing exposed to Python.
//!
//! The framework historically did `multiprocessing.cpu_count()` and
//! `/proc/meminfo` parsing in Python (`python/dynamic_runner/system_resources.py`)
//! purely so it could compute integers it then handed straight back
//! to a `_rs.LocalManagerConfig(...)` / `_rs.SecondaryConfig(...)`
//! call. Same pattern as the psutil refactor: no Python-exclusive
//! content, runs once per dispatch — moved to Rust so the Python
//! layer is thin glue, not a `/proc/meminfo` parser.
//!
//! Exposed PyO3 functions:
//!   - `detect_logical_cpu_count() -> int`
//!   - `detect_total_memory_bytes() -> int`
//!   - `detect_available_memory_bytes() -> int`
//!   - `parse_cores(spec: str) -> int`
//!   - `parse_memory(spec: str) -> int`
//!   - `pick_free_port() -> int`
//!
//! Submodules carve the file by concern: `detection` owns the
//! `detect_*` family + the cgroup v2 walking that makes them
//! container-aware; `parse` owns the CLI-spec parsing for the
//! `--cores` / `--memory` flags (which compose on top of
//! `detection`); `port` owns the small TCP free-port helper.

mod detection;
mod parse;
mod port;

pub(crate) use detection::{detect_logical_cpu_count, detect_total_memory_bytes};
pub(crate) use parse::{parse_cores, parse_memory};
pub(crate) use port::pick_free_port;
