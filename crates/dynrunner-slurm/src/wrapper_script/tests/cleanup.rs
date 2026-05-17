//! Tests for the wrapper-script's cleanup trap. Single concern:
//! pin the shape of the signal-trap body when
//! `shutdown_manager_bin_path = None` (legacy CMD_RELAY-only
//! teardown, no /tmp cleanup). The `Some(path)` shape is covered
//! by `tests::shutdown_manager::cleanup_trap_forwards_to_scope_when_enabled`.
//!
//! In-script `podman unshare rm -rf $RNDTMP` + per-file
//! `rm -f $LOCAL_IMAGE` were moved out of the wrapper into the
//! out-of-cgroup `dynrunner-slurm-shutdown` binary. The wrapper's
//! signal trap now only nudges the manager (SIGCONT wake) and
//! tears down the CMD_RELAY FIFO that lives in the wrapper's own
//! process group.

use crate::config::SlurmConfig;
use crate::wrapper_script::generate_wrapper_script;

use super::standard_cfg;

/// With `shutdown_manager_bin_path = None` (the legacy default
/// for unmigrated callers) the cleanup trap reduces to the
/// CMD_RELAY-only teardown: no `rm -rf`, no `podman unshare`,
/// no `systemctl --user kill` forward. The caller has opted
/// in to "no /tmp cleanup on SLURM-induced termination" by
/// leaving the field None.
#[test]
fn cleanup_trap_minimal_when_disabled() {
    let config = SlurmConfig::default();
    let script = generate_wrapper_script(&standard_cfg(&config, &[]));

    // Extract the cleanup() function body so the substring checks
    // are scoped to it (not the pre-flight block, which still uses
    // `podman rm -af` etc. for its own concern).
    let body = extract_cleanup_body(&script);
    assert!(
        !body.contains("podman unshare"),
        "cleanup trap must not contain `podman unshare` (moved to shutdown-manager binary); body was:\n{body}"
    );
    assert!(
        !body.contains("rm -rf"),
        "cleanup trap must not contain `rm -rf` (moved to shutdown-manager binary); body was:\n{body}"
    );
    assert!(
        !body.contains("rm -f -- \"$LOCAL_IMAGE\""),
        "cleanup trap must not unlink $LOCAL_IMAGE per-file (moved to shutdown-manager binary); body was:\n{body}"
    );
    assert!(
        !body.contains("systemctl"),
        "cleanup trap must not contain a systemctl forward when shutdown_manager_bin_path=None; body was:\n{body}"
    );

    // The CMD_RELAY teardown is the only operation left.
    assert!(
        body.contains("kill -TERM \"$CMD_RELAY_PID\""),
        "cleanup trap must still tear down CMD_RELAY; body was:\n{body}"
    );
    assert!(
        body.contains("wait \"$CMD_RELAY_PID\""),
        "cleanup trap must still wait for CMD_RELAY exit; body was:\n{body}"
    );

    // Trap targets are unchanged — SLURM-induced termination
    // signals still hit cleanup().
    assert!(script.contains("trap cleanup EXIT TERM HUP INT"));
}

/// Locate the body between `cleanup() {` and the matching close
/// brace. Used to scope substring checks to the trap body rather
/// than to the whole script.
fn extract_cleanup_body(script: &str) -> &str {
    let start = script
        .find("cleanup() {")
        .expect("cleanup() function must be defined")
        + "cleanup() {".len();
    // The cleanup body ends at the matching close brace at column 1.
    // Looking for "\n}\n" finds the line containing just `}`.
    let rest = &script[start..];
    let end = rest
        .find("\n}\n")
        .expect("cleanup() body must close with a `}` line");
    &rest[..end]
}
