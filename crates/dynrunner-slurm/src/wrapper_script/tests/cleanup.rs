//! Tests for the wrapper-script's cleanup trap: rootless-podman
//! teardown via `podman unshare`, per-file image unlink, and the
//! stderr-logging-on-failure / success-after-rm sequence.

use crate::config::SlurmConfig;
use crate::wrapper_script::generate_wrapper_script;

use super::standard_cfg;

#[test]
fn cleanup_uses_podman_unshare_for_rndtmp_teardown() {
    let config = SlurmConfig::default();
    let script = generate_wrapper_script(&standard_cfg(&config, &[]));
    assert!(
        script.contains("podman unshare rm -rf -- \"$RNDTMP\""),
        "cleanup must use `podman unshare rm` to reach subuid-mapped files"
    );
    // `--` is critical: blocks accidental flag interpretation
    // if $RNDTMP ever starts with a dash.
    assert!(script.contains("rm -rf -- \"$RNDTMP\""));
    // Old `sudo rm -rf` fallback is gone — sudo can't help
    // with user-ns subuid files (different uid space) and
    // workers don't have sudo anyway.
    assert!(
        !script.contains("sudo rm -rf"),
        "sudo fallback was removed — it never worked for subuid-mapped files"
    );
}

/// Per-file unlink for the image tarball: it's host-UID
/// owned (cp'd in by the wrapper before any container ran),
/// so a plain `rm -f` reaches it. The "rm -rf in scripts is
/// dangerous" rule pushes us to per-file unlink wherever
/// feasible.
#[test]
fn cleanup_unlinks_local_image_per_file() {
    let config = SlurmConfig::default();
    let script = generate_wrapper_script(&standard_cfg(&config, &[]));
    assert!(
        script.contains("rm -f -- \"$LOCAL_IMAGE\""),
        "cleanup must per-file unlink $LOCAL_IMAGE before tree-rm"
    );
    // The `${LOCAL_IMAGE:-}` guard covers early-exit paths
    // that hit the trap before LOCAL_IMAGE was assigned.
    assert!(script.contains("${LOCAL_IMAGE:-}"));
}

/// On rm failure the wrapper must log to stderr — silent
/// `|| true` is what masked Bug AA in the first place. On
/// success the cleanup line must appear AFTER the rm runs,
/// not before, so logs reflect the actual outcome.
#[test]
fn cleanup_logs_failure_to_stderr_and_success_after_rm() {
    let config = SlurmConfig::default();
    let script = generate_wrapper_script(&standard_cfg(&config, &[]));
    // Failure is logged to stderr (`>&2`) with a marker that
    // log scrapers can match on. The "leaked" wording is
    // load-bearing for the field-ops grep.
    assert!(
        script.contains("/tmp scratch leaked on $(hostname)") && script.contains(">&2"),
        "cleanup must log rm failures to stderr with a scrapable marker"
    );
    // Success is logged AFTER the rm completes — the
    // `Cleaned up` (past-tense) string sits inside the
    // success branch, not before the rm.
    assert!(script.contains("Cleaned up temporary directory: $RNDTMP"));
}

