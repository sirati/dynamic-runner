//! Single concern: container memory-cap probe (generate.rs:540-569).
//! MIN(host MemTotal - 2GiB, cgroup memory.max). Phase 1 (1D) fills body.

/// RAM cap in bytes, or `None` when both probes are empty (=> caller
/// omits the `--memory` flag). The caller renders
/// `--memory=<n> --memory-swap=-1` when `Some`.
pub fn detect_memory_cap() -> Option<u64> {
    todo!("1D: min(host MemTotal-2GiB, cgroup memory.max)")
}
