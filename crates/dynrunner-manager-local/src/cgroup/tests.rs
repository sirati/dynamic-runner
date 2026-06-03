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
//!   * Per-worker leaf attach: `SubcgroupHandle::attach_pid` writes
//!     the pid as decimal to `<worker-N>/cgroup.procs` (the old
//!     `workers/cgroup.procs` path is forbidden once subtree_control
//!     is enabled on `workers/`).
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
    assert!(
        !workers_path.join("memory.max").exists(),
        "workers/memory.max should not be written when parent is unlimited"
    );
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
fn workers_path_accessor_returns_materialised_dir() {
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory\n", "max\n");
    let workers_path = super::writer::write_workers_subgroup(&leaf, 0).unwrap();
    let handle = NestedCgroupHandle {
        workers_path: workers_path.clone(),
    };
    assert_eq!(handle.workers_path(), workers_path);
}

#[test]
fn write_workers_subgroup_enables_subtree_control_on_workers() {
    // After init on a clean leaf (no pids in workers/cgroup.procs),
    // workers/cgroup.subtree_control must be written (the kernel
    // pseudo-file accumulates each `+controller` write into a single
    // displayed list, e.g. "memory pids"). Our tempdir fake is a
    // regular file under tmpfs, so each `std::fs::write` truncates
    // and only the LAST controller written ("+pids" in CONTROLLERS
    // order) survives. We assert (a) the file exists, (b) it is
    // non-empty (the writer DID attempt to enable subtree_control),
    // (c) it contains the last token from the writer's controller
    // list. The integration-level "both controllers landed" check is
    // out of unit-test reach without a real cgroup-v2 host; covered
    // by the integration smoke (Phase F).
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory pids\n", "1073741824\n");

    let workers_path = super::writer::write_workers_subgroup(&leaf, 0).unwrap();

    let subtree_path = workers_path.join("cgroup.subtree_control");
    assert!(
        subtree_path.exists(),
        "workers/cgroup.subtree_control should be created"
    );
    let subtree = std::fs::read_to_string(&subtree_path).unwrap();
    assert!(
        !subtree.is_empty(),
        "workers/cgroup.subtree_control should have been written; got empty"
    );
    // Under the fake's truncate-on-write semantics only the last
    // token from CONTROLLERS ("+pids") survives.
    assert!(
        subtree.contains("pids"),
        "expected last controller write ('+pids') to be present; got: {subtree:?}"
    );
}

#[test]
fn workers_with_existing_pids_skips_subtree_control() {
    // LegacyFlat upgrade case: a previous run left pids attached to
    // workers/cgroup.procs (flat layout). Enabling subtree_control on
    // workers/ would fail with EBUSY at the kernel level. The writer
    // detects the non-empty procs file and SKIPS the workers/
    // subtree_control writes, returning successfully so the caller
    // can continue running in flat mode this run.
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory pids\n", "1073741824\n");
    let workers_path = leaf.join("workers");
    std::fs::create_dir_all(&workers_path).unwrap();
    write_fixture(&workers_path.join("cgroup.procs"), "12345\n");
    // Pre-create the subtree_control file (empty) so we can later
    // assert it stayed empty — the test fake is a regular file, not
    // a kernel pseudo-file that auto-creates on first read.
    write_fixture(&workers_path.join("cgroup.subtree_control"), "");

    let returned = super::writer::write_workers_subgroup(&leaf, 0).unwrap();
    assert_eq!(returned, workers_path);

    // workers/subtree_control must be untouched (still empty fixture).
    let subtree = std::fs::read_to_string(workers_path.join("cgroup.subtree_control")).unwrap();
    assert_eq!(
        subtree, "",
        "subtree_control should stay empty in LegacyFlat fallback; got: {subtree:?}"
    );
    // The legacy procs file still contains the original pid.
    let procs = std::fs::read_to_string(workers_path.join("cgroup.procs")).unwrap();
    assert_eq!(procs.trim(), "12345");
}

#[test]
fn self_move_into_secondary_when_leaf_has_only_self_pid() {
    // Bare `systemd-run --user --scope` case: the secondary is the
    // ONLY pid in the leaf. The writer must move us into a
    // `<leaf>/secondary/` sub-cgroup so the subsequent
    // subtree_control write on the leaf doesn't trip the cgroup-v2
    // "no internal processes" rule.
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory pids\n", "1073741824\n");
    let self_pid = std::process::id();
    write_fixture(&leaf.join("cgroup.procs"), &format!("{self_pid}\n"));

    let workers_path = super::writer::write_workers_subgroup(&leaf, 0).unwrap();
    assert_eq!(workers_path, leaf.join("workers"));

    let secondary_dir = leaf.join("secondary");
    assert!(
        secondary_dir.is_dir(),
        "expected <leaf>/secondary/ to be created by the self-move"
    );
    let moved_procs = std::fs::read_to_string(secondary_dir.join("cgroup.procs")).unwrap();
    assert_eq!(
        moved_procs.trim(),
        self_pid.to_string(),
        "<leaf>/secondary/cgroup.procs should contain our pid post-move"
    );
}

#[test]
fn self_move_is_noop_when_leaf_procs_is_empty() {
    // Container case: the runtime nested us in a sub-cgroup already,
    // so the leaf's cgroup.procs is empty. No self-move needed; the
    // subtree_control write below works directly.
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory pids\n", "1073741824\n");
    write_fixture(&leaf.join("cgroup.procs"), "");

    super::writer::write_workers_subgroup(&leaf, 0).unwrap();

    assert!(
        !leaf.join("secondary").exists(),
        "no self-move expected when leaf already has no pids"
    );
}

#[test]
fn self_move_skipped_when_leaf_has_foreign_pids() {
    // Mixed-pid case: leaf contains a pid we don't own (e.g. some
    // sibling tool the operator left running). We can't safely move
    // foreign processes, so the writer warns and the subsequent
    // subtree_control write proceeds — and would fail under a real
    // kernel; under the tempdir fake it silently succeeds because
    // we're writing to a regular file.
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory pids\n", "1073741824\n");
    let self_pid = std::process::id();
    write_fixture(
        &leaf.join("cgroup.procs"),
        &format!("{self_pid}\n99999999\n"),
    );

    super::writer::write_workers_subgroup(&leaf, 0).unwrap();

    assert!(
        !leaf.join("secondary").exists(),
        "self-move must not happen when foreign pids are present"
    );
}

#[test]
fn self_move_is_idempotent() {
    // Re-running the writer must not error or double-write — once
    // self-moved, the leaf's cgroup.procs is empty (on a real kernel
    // it would be; in the tempdir we manually clear it to simulate
    // the post-move state).
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory pids\n", "1073741824\n");
    let self_pid = std::process::id();
    write_fixture(&leaf.join("cgroup.procs"), &format!("{self_pid}\n"));

    super::writer::write_workers_subgroup(&leaf, 0).unwrap();
    // Simulate the kernel's post-move state: leaf cgroup.procs empty.
    write_fixture(&leaf.join("cgroup.procs"), "");

    // Second call must succeed (mkdir is idempotent, no-op branch).
    super::writer::write_workers_subgroup(&leaf, 0).unwrap();
    assert!(leaf.join("secondary").is_dir());
}

#[test]
fn prepare_worker_subgroup_creates_leaf_with_swap_max() {
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory pids\n", "max\n");
    let workers_path = super::writer::write_workers_subgroup(&leaf, 0).unwrap();
    let handle = NestedCgroupHandle {
        workers_path: workers_path.clone(),
    };

    let sub = super::prepare_worker_subgroup(&handle, 3).unwrap();

    let expected = workers_path.join("worker-3");
    assert_eq!(sub.cgroup_dir(), expected);
    assert!(expected.is_dir(), "per-worker leaf should be a directory");
    let swap = std::fs::read_to_string(expected.join("memory.swap.max")).unwrap();
    assert_eq!(swap.trim(), "max");
    // Intentional: NO memory.max on per-worker leaf (observability
    // only; aggregate cap lives on the parent workers/).
    assert!(
        !expected.join("memory.max").exists(),
        "per-worker memory.max must NOT be written"
    );

    // Prevent Drop from rmdir'ing during the test (we want the
    // assertions above to be visible at scope-exit).
    std::mem::forget(sub);
}

#[test]
fn prepare_worker_subgroup_is_idempotent() {
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory pids\n", "max\n");
    let workers_path = super::writer::write_workers_subgroup(&leaf, 0).unwrap();
    let handle = NestedCgroupHandle { workers_path };

    let first = super::prepare_worker_subgroup(&handle, 7).unwrap();
    let first_path = first.cgroup_dir().to_path_buf();
    std::mem::forget(first);

    let second = super::prepare_worker_subgroup(&handle, 7).unwrap();
    assert_eq!(second.cgroup_dir(), first_path);
    std::mem::forget(second);
}

#[test]
fn subcgroup_attach_pid_writes_decimal_to_leaf_procs() {
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory pids\n", "max\n");
    let workers_path = super::writer::write_workers_subgroup(&leaf, 0).unwrap();
    let handle = NestedCgroupHandle {
        workers_path: workers_path.clone(),
    };
    let sub = super::prepare_worker_subgroup(&handle, 42).unwrap();
    let leaf_dir = sub.cgroup_dir().to_path_buf();

    sub.attach_pid(99887).unwrap();

    let body = std::fs::read_to_string(leaf_dir.join("cgroup.procs")).unwrap();
    assert_eq!(body.trim(), "99887");
    // Cleanup: drop the handle; the leaf still has the pid fixture
    // so Drop will hit the ENOTEMPTY warn path. That's exercised in
    // a dedicated test below — here we only care about the write.
}

#[test]
fn subcgroup_handle_drop_rmdirs_empty_leaf() {
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory pids\n", "max\n");
    let workers_path = super::writer::write_workers_subgroup(&leaf, 0).unwrap();
    let handle = NestedCgroupHandle {
        workers_path: workers_path.clone(),
    };
    let sub = super::prepare_worker_subgroup(&handle, 5).unwrap();
    let leaf_dir = sub.cgroup_dir().to_path_buf();
    assert!(leaf_dir.is_dir());

    // The fixture wrote memory.swap.max as a regular file; rmdir
    // would fail with ENOTEMPTY against that. To exercise the "empty
    // leaf" path we have to remove that file first. This mirrors the
    // real-world case where the kernel auto-removes pseudo-files
    // when the cgroup is empty (no procs, no controllers configured
    // on a leaf about to be rmdir'd).
    std::fs::remove_file(leaf_dir.join("memory.swap.max")).unwrap();

    drop(sub);

    assert!(!leaf_dir.exists(), "empty leaf should be rmdir'd by Drop");
}

#[test]
fn subcgroup_handle_drop_swallows_nonempty() {
    // The per-worker leaf contains the fixture's memory.swap.max
    // regular file (and potentially a cgroup.procs we write below).
    // rmdir against a non-empty dir returns ENOTEMPTY; Drop must
    // swallow it without panicking and leave the directory intact
    // for an operator to find.
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory pids\n", "max\n");
    let workers_path = super::writer::write_workers_subgroup(&leaf, 0).unwrap();
    let handle = NestedCgroupHandle { workers_path };
    let sub = super::prepare_worker_subgroup(&handle, 11).unwrap();
    let leaf_dir = sub.cgroup_dir().to_path_buf();
    // Simulate an attached pid by writing cgroup.procs explicitly.
    write_fixture(&leaf_dir.join("cgroup.procs"), "31337\n");

    drop(sub); // must not panic.

    // Directory survived (rmdir refused; warn line was emitted but
    // unobserved in this test — log capture isn't worth the
    // dependency churn for one line).
    assert!(leaf_dir.is_dir(), "non-empty leaf should remain after Drop");
}

#[test]
fn subcgroup_handle_drop_silent_on_already_gone() {
    // Race: another teardown path (or the kernel auto-removal on
    // last-pid-exit) removed the leaf before Drop ran. ENOENT must
    // be silent — no panic, no warn line.
    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory pids\n", "max\n");
    let workers_path = super::writer::write_workers_subgroup(&leaf, 0).unwrap();
    let handle = NestedCgroupHandle { workers_path };
    let sub = super::prepare_worker_subgroup(&handle, 13).unwrap();
    let leaf_dir = sub.cgroup_dir().to_path_buf();

    // Remove fixture + dir before Drop. We have to nuke the
    // memory.swap.max child first because the test fixture is a
    // regular file under a regular directory.
    std::fs::remove_file(leaf_dir.join("memory.swap.max")).unwrap();
    std::fs::remove_dir(&leaf_dir).unwrap();

    drop(sub); // must not panic.
    assert!(!leaf_dir.exists());
}
