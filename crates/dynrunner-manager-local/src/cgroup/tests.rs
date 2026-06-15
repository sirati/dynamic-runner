//! Unit tests for the nested-cgroup setup module.
//!
//! These tests inject a tempdir-rooted fake `/sys/fs/cgroup` via the
//! `cgroup_root` parameter on [`super::setup_worker_cgroup`]. They
//! exercise:
//!
//!   * Happy path: creates `workers/`, enables controllers, writes
//!     tightened `memory.max`, caps `memory.swap.max` to "0" (best-effort).
//!   * Graceful fallback: cgroup-v1 host, missing memory controller,
//!     non-writable subtree_control, and any write-phase error. Each
//!     returns `None` (the orchestrator is infallible).
//!   * Per-worker leaf attach: `SubcgroupHandle::attach_pid` writes
//!     the pid as decimal to `<worker-N>/cgroup.procs` (the old
//!     `workers/cgroup.procs` path is forbidden once subtree_control
//!     is enabled on `workers/`).
//!   * Idempotence: a second call against an already-prepared leaf
//!     does not error and ends with the same on-disk shape.
//!
//! `cgroup_v2_leaf` reads the REAL `/proc/self/cgroup` (we don't
//! mock /proc), so most tests below DON'T exercise [`super::setup_worker_cgroup`]
//! end-to-end. Instead they call the lower-level
//! [`super::writer::write_workers_subgroup`] (re-exported into the
//! test module via `use super::writer`) for the happy-path /
//! idempotence assertions and exercise the graceful-fallback gates
//! by directly invoking the predicates.
//!
//! Exception: the degrade-on-error tests
//! (`setup_worker_cgroup_degrades_on_permission_denied`,
//! `setup_worker_cgroup_degrades_on_missing_controllers_file`) DO
//! drive the public orchestrator end-to-end. `cgroup_v2_leaf` joins
//! the real `/proc/self/cgroup` relative path onto the injected
//! root, so a fixture materialised at exactly
//! `<tempdir>/<real-rel-path>` is what the orchestrator resolves —
//! no /proc mock needed. They skip (eprintln + return) on hosts
//! without a cgroup-v2 line or when running as a user that chmod
//! cannot lock out (root). Errnos a tempdir filesystem cannot
//! produce (EOPNOTSUPP, EBUSY) are pinned against the policy seam
//! [`super::degrade_setup_failure_to_flat`] directly.

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

    // memory.swap.max capped to "0" — workers must not swap (a
    // swapping worker is a death spiral the watcher reads as relief).
    let swap_max = std::fs::read_to_string(workers_path.join("memory.swap.max")).unwrap();
    assert_eq!(swap_max.trim(), "0");
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
    // But swap.max is still capped to zero — the no-swap policy is
    // independent of whether the parent has a concrete RAM cap.
    let swap = std::fs::read_to_string(workers_path.join("memory.swap.max")).unwrap();
    assert_eq!(swap.trim(), "0");
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
    assert!(matches!(err, super::CgroupSetupError::Io { .. }));
    // The Display must name the operation and path — it's the only
    // diagnostic that reaches the operator via the degrade warn line.
    let rendered = err.to_string();
    assert!(
        rendered.contains("read") && rendered.contains("cgroup.controllers"),
        "Io Display must carry op + path; got: {rendered}"
    );
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

/// Materialise a fixture leaf at the path the REAL
/// `/proc/self/cgroup` relative path resolves to under the injected
/// `root`, so [`super::setup_worker_cgroup`] (whose first step is
/// `cgroup_v2_leaf(root)`) finds it end-to-end. Returns `None` on
/// hosts without a cgroup-v2 (`0::`) line, in which case the caller
/// should skip.
fn make_fake_leaf_at_real_rel_path(root: &Path) -> Option<std::path::PathBuf> {
    let leaf = super::cgroup_v2_leaf(root)?;
    std::fs::create_dir_all(&leaf).unwrap();
    Some(leaf)
}

#[test]
fn setup_worker_cgroup_degrades_on_permission_denied() {
    // The #371 repro shape: every PROBE passes (memory controller
    // listed, subtree_control file opens for write — file perms allow
    // it even under a chmod-555 dir), but the WRITE phase hits a real
    // kernel EACCES (here: `create_dir_all(<leaf>/workers)` against a
    // read-only directory). The orchestrator must degrade to the flat
    // layout (`None`) exactly like the probe-stage conditions — NOT
    // abort the run the way the pre-fix code did with "nested workers
    // cgroup setup failed: cgroup I/O error: Permission denied".
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().unwrap();
    let Some(leaf) = make_fake_leaf_at_real_rel_path(root.path()) else {
        eprintln!(
            "skipping setup_worker_cgroup_degrades_on_permission_denied: \
             no cgroup-v2 line in /proc/self/cgroup"
        );
        return;
    };
    write_fixture(&leaf.join("cgroup.controllers"), "cpu memory pids\n");
    write_fixture(&leaf.join("cgroup.subtree_control"), "");
    write_fixture(&leaf.join("memory.max"), "1073741824\n");

    // Lock the leaf directory: r-x only, so mkdir inside it returns
    // EACCES. (Probe files inside stay openable — directory +x allows
    // traversal and the files' own modes allow the write-open.)
    std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o555)).unwrap();
    // Root (or CAP_DAC_OVERRIDE) bypasses directory modes; the EACCES
    // this test depends on never fires there. Detect and skip.
    if std::fs::create_dir(leaf.join(".dac-override-probe")).is_ok() {
        std::fs::remove_dir(leaf.join(".dac-override-probe")).unwrap();
        std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o755)).unwrap();
        eprintln!(
            "skipping setup_worker_cgroup_degrades_on_permission_denied: \
             chmod 555 does not lock this user out (root / CAP_DAC_OVERRIDE)"
        );
        return;
    }

    let handle = super::setup_worker_cgroup(root.path(), 0);

    // Restore perms BEFORE asserting so the tempdir cleans up even on
    // assertion failure.
    std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o755)).unwrap();

    assert!(
        handle.is_none(),
        "write-phase EACCES must degrade to the flat layout (None handle)"
    );
    assert!(
        !leaf.join("workers").exists(),
        "no workers/ subgroup should have been materialised"
    );
}

#[test]
fn setup_worker_cgroup_degrades_on_missing_controllers_file() {
    // The round-2 #371 contract: a NON-permission errno (here ENOENT
    // from a leaf missing `cgroup.controllers`) must ALSO degrade to
    // the flat layout. Pre-fix this was classified "genuinely fatal"
    // and aborted the run — exactly the errno whack-a-mole the
    // consumer then hit with EOPNOTSUPP/EBUSY on a plain desktop
    // session. The flat layout serves every run, so no setup failure
    // is worth aborting over.
    let root = tempfile::tempdir().unwrap();
    let Some(_leaf) = make_fake_leaf_at_real_rel_path(root.path()) else {
        eprintln!(
            "skipping setup_worker_cgroup_degrades_on_missing_controllers_file: \
             no cgroup-v2 line in /proc/self/cgroup"
        );
        return;
    };
    // No fixtures: the bare directory lacks cgroup.controllers.

    let handle = super::setup_worker_cgroup(root.path(), 0);
    assert!(
        handle.is_none(),
        "missing cgroup.controllers (ENOENT) must degrade to the flat layout"
    );
}

/// Construct an `Io`-shaped setup error around the given raw OS errno,
/// as the write phase would surface it.
fn io_setup_error(raw_os_errno: i32) -> CgroupSetupError {
    CgroupSetupError::Io {
        op: "write",
        path: "/sys/fs/cgroup/leaf/cgroup.subtree_control".into(),
        source: std::io::Error::from_raw_os_error(raw_os_errno),
    }
}

#[test]
fn any_setup_error_degrades_to_flat() {
    // The degrade policy seam: EVERY `Err` shape maps to `None`
    // (flat layout), with NO errno family enumeration. The errnos
    // pinned here are the ones field reports actually produced —
    // EACCES (round 1), EOPNOTSUPP and EBUSY (round 2, subtree
    // _control writes on a plain desktop session) — plus a generic
    // I/O error standing in for "whatever the kernel says next".
    // A tempdir filesystem cannot fabricate EOPNOTSUPP/EBUSY, so
    // these drive the policy seam directly rather than the
    // orchestrator end-to-end (the e2e degrade paths are covered by
    // the two tests above).
    const EACCES: i32 = 13;
    const EBUSY: i32 = 16;
    const EOPNOTSUPP: i32 = 95;
    for errno in [EACCES, EBUSY, EOPNOTSUPP] {
        assert!(
            super::degrade_setup_failure_to_flat(Err(io_setup_error(errno))).is_none(),
            "os error {errno} must degrade to the flat layout, not stay fatal"
        );
    }
    assert!(
        super::degrade_setup_failure_to_flat(Err(CgroupSetupError::Io {
            op: "read",
            path: "/sys/fs/cgroup/leaf/memory.max".into(),
            source: std::io::Error::other("transient sysfs failure"),
        }))
        .is_none(),
        "a generic io::Error must degrade to the flat layout"
    );

    // Pass-through: `Ok` outcomes are untouched in both directions.
    let handle = NestedCgroupHandle::from_workers_path_for_test("/fake/workers".into());
    assert!(super::degrade_setup_failure_to_flat(Ok(Some(handle))).is_some());
    assert!(super::degrade_setup_failure_to_flat(Ok(None)).is_none());
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
    assert_eq!(swap.trim(), "0");
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

/// Capture every event at target `cgroup_swap_cap`, recording its
/// level, so the swap-cap tolerance tests can assert the "info once
/// per process, debug per occurrence" contract.
#[derive(Clone, Default)]
struct SwapCapLogCapture {
    events: std::sync::Arc<std::sync::Mutex<Vec<tracing::Level>>>,
}

impl SwapCapLogCapture {
    fn count_at(&self, level: tracing::Level) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|l| **l == level)
            .count()
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SwapCapLogCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() != "cgroup_swap_cap" {
            return;
        }
        self.events.lock().unwrap().push(*event.metadata().level());
    }
}

/// The swap-cap write tolerance contract, on BOTH write sites
/// (workers/ subgroup and per-worker leaf): a host where the
/// `memory.swap.max` write fails (EOPNOTSUPP / EBUSY in rootless
/// podman; simulated here with an EISDIR fixture — the policy
/// deliberately ignores the errno family) must NOT degrade the
/// nested setup. The orchestration succeeds, `memory.max` is still
/// tightened, the per-worker leaf is still created, and the failure
/// logs once at warn (operator-visible: workers can swap on this
/// host, and a swapping worker thrashes instead of OOM-killing —
/// the userland swap-driven kill in `dynrunner-scheduler` is the
/// backstop) plus per-occurrence at debug.
///
/// Deliberately ONE test: these are the only call sites in the
/// binary that drive `cap_swap_best_effort` down its failure branch,
/// so keeping them in a single (sequential) test makes the info/debug
/// counts exact. Split across parallel tests, the first hit on the
/// failure-branch tracing callsites can race their registration from
/// a subscriber-less sibling thread (events silently dropped) and
/// the Once is consumed by whichever test runs first.
#[test]
fn swap_cap_write_failure_is_non_fatal_and_logged_once() {
    use tracing_subscriber::layer::SubscriberExt;

    let root = tempfile::tempdir().unwrap();
    let leaf = make_fake_leaf(root.path(), "memory pids\n", "1073741824\n");
    // Pre-create memory.swap.max as a DIRECTORY so fs::write fails.
    std::fs::create_dir_all(leaf.join("workers").join("memory.swap.max")).unwrap();

    let capture = SwapCapLogCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    tracing::subscriber::with_default(subscriber, || {
        // Two setup passes, both hitting the unwritable swap file.
        let first = super::writer::write_workers_subgroup(&leaf, 0)
            .expect("swap-cap write failure must not fail the setup");
        let second = super::writer::write_workers_subgroup(&leaf, 0)
            .expect("swap-cap write failure must stay non-fatal on re-run");
        assert_eq!(first, second);
        // The rest of the setup still happened: memory.max written.
        let mem_max = std::fs::read_to_string(first.join("memory.max")).unwrap();
        assert_eq!(mem_max.trim(), "1073741824");

        // Same tolerance on the per-worker leaf: pre-create its
        // memory.swap.max as a directory; leaf creation must still
        // succeed so the spawn proceeds.
        std::fs::create_dir_all(first.join("worker-9").join("memory.swap.max")).unwrap();
        let handle = NestedCgroupHandle {
            workers_path: first,
        };
        let sub = super::prepare_worker_subgroup(&handle, 9)
            .expect("per-worker swap-cap failure must not fail leaf creation");
        assert!(sub.cgroup_dir().is_dir());
        std::mem::forget(sub);
    });

    // Exactly one operator-visible warn line (Once-gated across the
    // three failures above) and one debug trace per occurrence.
    // The level was raised from info to warn so the failure pages
    // operator dashboards: a workers tree where the swap cap fails
    // can be made to swap, and the framework's userland kill is the
    // only backstop.
    assert_eq!(
        capture.count_at(tracing::Level::WARN),
        1,
        "the operator-visible warn line is Once-gated"
    );
    assert_eq!(
        capture.count_at(tracing::Level::DEBUG),
        3,
        "each swap-cap write failure must leave a debug trace"
    );
}
