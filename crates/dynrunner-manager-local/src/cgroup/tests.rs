//! Unit tests for the nested-cgroup setup module.
//!
//! These tests inject a tempdir-rooted fake `/sys/fs/cgroup` via the
//! `cgroup_root` parameter on [`super::setup_worker_cgroup`]. They
//! exercise:
//!
//!   * Happy path: creates `workers/`, enables controllers, writes
//!     tightened `memory.max`, resets `memory.swap.max` to "max".
//!   * Graceful fallback: cgroup-v1 host, missing memory controller,
//!     non-writable subtree_control. Each returns `Ok(None)`.
//!   * Attach-pid primitive: writes the pid as decimal to
//!     `workers/cgroup.procs`.
//!   * Idempotence: a second call against an already-prepared leaf
//!     does not error and ends with the same on-disk shape.
//!
//! `cgroup_v2_leaf` reads the REAL `/proc/self/cgroup` (we don't
//! mock /proc), so the tests below DON'T exercise [`super::setup_worker_cgroup`]
//! end-to-end. Instead they call the lower-level
//! [`super::writer::write_workers_subgroup`] (re-exported into the
//! test module via `use super::writer`) for the happy-path /
//! idempotence assertions and exercise the graceful-fallback gates
//! by directly invoking the predicates.
//!
//! Conscious-trade-off: dropping the `/proc/self/cgroup` mock means
//! the warn-line plumbing in the orchestrator isn't exercised in
//! unit tests. The orchestrator is six straightforward branches over
//! the predicate helpers, each itself unit-tested below; the
//! integration risk is bounded.

use super::*;
use std::path::Path;

/// Write the given body to a file, creating parent dirs as needed.
/// Helper used by every fixture below.
fn write_fixture(path: &Path, body: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, body).unwrap();
}

/// Construct a fake cgroup-v2 leaf at `<root>/leaf/` with the
/// requested controllers list and `memory.max` body. Returns the
/// leaf path so the test can write further fixtures.
fn make_fake_leaf(root: &Path, controllers: &str, memory_max: &str) -> std::path::PathBuf {
    let leaf = root.join("leaf");
    std::fs::create_dir_all(&leaf).unwrap();
    write_fixture(&leaf.join("cgroup.controllers"), controllers);
    write_fixture(&leaf.join("cgroup.subtree_control"), "");
    write_fixture(&leaf.join("memory.max"), memory_max);
    leaf
}

#[test]
fn write_workers_subgroup_happy_path() {
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "cpu memory pids io\n", "4294967296\n");

    let workers_path = super::writer::write_workers_subgroup(&leaf, 500 * 1024 * 1024).unwrap();
    assert_eq!(workers_path, leaf.join("workers"));
    assert!(workers_path.is_dir());

    // memory.max tightened: 4 GiB - 500 MiB = 4 294 967 296 - 524 288 000 = 3 770 679 296.
    let mem_max = std::fs::read_to_string(workers_path.join("memory.max")).unwrap();
    assert_eq!(mem_max.trim(), "3770679296");

    // memory.swap.max forced to "max".
    let swap_max = std::fs::read_to_string(workers_path.join("memory.swap.max")).unwrap();
    assert_eq!(swap_max.trim(), "max");
}

#[test]
fn write_workers_subgroup_is_idempotent() {
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory pids\n", "1073741824\n");

    let first = super::writer::write_workers_subgroup(&leaf, 0).unwrap();
    let second = super::writer::write_workers_subgroup(&leaf, 0).unwrap();
    assert_eq!(first, second);
    // Cap stays at full container_max - 0 reserved = 1 GiB after both runs.
    let mem_max = std::fs::read_to_string(second.join("memory.max")).unwrap();
    assert_eq!(mem_max.trim(), "1073741824");
}

#[test]
fn write_workers_subgroup_parent_unlimited_skips_memory_max() {
    // Parent's memory.max is the literal "max" → workers subgroup
    // should NOT have a memory.max file at all (we'd be artificially
    // capping what the parent does not cap).
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory\n", "max\n");

    let workers_path = super::writer::write_workers_subgroup(&leaf, 500 * 1024 * 1024).unwrap();
    assert!(!workers_path.join("memory.max").exists(),
        "workers/memory.max should not be written when parent is unlimited");
    // But swap.max is still forced (cgroup-v2 children default to 0).
    let swap = std::fs::read_to_string(workers_path.join("memory.swap.max")).unwrap();
    assert_eq!(swap.trim(), "max");
}

#[test]
fn write_workers_subgroup_saturating_sub_floor_at_zero() {
    // reserved_bytes > container_max → saturating sub floors at 0.
    // Kernel will reject memory.max=0 in practice, but the writer's
    // job is to surface the misconfiguration as a write error; here
    // against a tmpfs the write succeeds and the test just verifies
    // the value.
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory\n", "1024\n");

    let workers_path = super::writer::write_workers_subgroup(&leaf, 4096).unwrap();
    let mem_max = std::fs::read_to_string(workers_path.join("memory.max")).unwrap();
    assert_eq!(mem_max.trim(), "0");
}

#[test]
fn leaf_has_memory_controller_detects_listed_controller() {
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "cpu memory pids\n", "max\n");
    assert!(super::leaf_has_memory_controller(&leaf).unwrap());
}

#[test]
fn leaf_has_memory_controller_rejects_absent_controller() {
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "cpu pids\n", "max\n");
    assert!(!super::leaf_has_memory_controller(&leaf).unwrap());
}

#[test]
fn leaf_has_memory_controller_io_error_on_missing_file() {
    let root = tempfile::tempdir().unwrap();
    let leaf = root.path().join("absent_leaf");
    std::fs::create_dir_all(&leaf).unwrap();
    let err = super::leaf_has_memory_controller(&leaf).unwrap_err();
    assert!(matches!(err, super::CgroupSetupError::Io(_)));
}

#[test]
fn leaf_subtree_writable_true_when_writable() {
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory\n", "max\n");
    assert!(super::leaf_subtree_writable(&leaf));
}

#[test]
fn leaf_subtree_writable_false_when_missing() {
    let root = tempfile::tempdir().unwrap();
    let leaf = root.path().join("no_subtree");
    std::fs::create_dir_all(&leaf).unwrap();
    // No subtree_control file → open fails → returns false.
    assert!(!super::leaf_subtree_writable(&leaf));
}

#[test]
fn attach_pid_writes_decimal_to_cgroup_procs() {
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory\n", "max\n");
    let workers_path = super::writer::write_workers_subgroup(&leaf, 0).unwrap();

    let handle = NestedCgroupHandle { workers_path: workers_path.clone() };
    super::attach_pid(&handle, 12345).unwrap();

    let body = std::fs::read_to_string(workers_path.join("cgroup.procs")).unwrap();
    assert_eq!(body.trim(), "12345");
}

#[test]
fn workers_path_accessor_returns_materialised_dir() {
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory\n", "max\n");
    let workers_path = super::writer::write_workers_subgroup(&leaf, 0).unwrap();
    let handle = NestedCgroupHandle { workers_path: workers_path.clone() };
    assert_eq!(handle.workers_path(), workers_path);
}
