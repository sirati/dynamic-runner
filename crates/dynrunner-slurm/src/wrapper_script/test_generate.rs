//! Image-validation wrapper: [`TestWrapperScriptConfig`] and
//! [`generate_test_wrapper_script`]. A stripped-down variant of the
//! secondary wrapper that exercises only the image-load path —
//! emitted by the `e2e-image-smoke` style of sbatch job. No secondary
//! container is started; the script verifies the docker-archive tar
//! exists, runs `podman load`, then tears down. Sibling of
//! [`generate`](super::generate).

use super::quote::rand_hex8;

/// Configuration for the image-validation test wrapper.
///
/// Mirrors the input shape of [`generate_test_wrapper_script`] —
/// the test wrapper exercises only the image-load path, so it needs
/// far fewer knobs than the full secondary wrapper.
pub struct TestWrapperScriptConfig<'a> {
    /// Absolute (already tilde-expanded) path to the docker-archive
    /// tar on the gateway.
    pub image_path: &'a str,
    pub image_name: &'a str,
    pub image_tag: &'a str,
    pub image_tar_basename: &'a str,
    pub load_command: &'a str,
    /// In-container entrypoint to test via `--help`.
    pub container_command: &'a str,
}

/// Generate the bash wrapper script used by `slurm-validate-image`.
///
/// The test wrapper copies the image to /tmp, loads it into a
/// fresh podman storage root, lists the image, and runs the
/// container's `--help` to confirm the entrypoint is intact. It
/// shares the SLURM-induced-signal trap pattern with the full
/// wrapper (commit 485629c) so test-job termination doesn't leak
/// /tmp/asm-test-XXXX scratch dirs.
pub fn generate_test_wrapper_script(cfg: &TestWrapperScriptConfig<'_>) -> String {
    let rnd_suffix = rand_hex8();
    let rndtmp = format!("/tmp/asm-test-{rnd_suffix}");
    let podman_storage = format!("{rndtmp}/storage");
    let podman_run = format!("{rndtmp}/run");
    let image_ref = format!("{}:{}", cfg.image_name, cfg.image_tag);

    format!(
        r##"#!/usr/bin/env bash
set -e

echo "=================================================="
echo "SLURM Test Job - Docker Image Validation"
echo "Node: $(hostname)"
echo "Job ID: $SLURM_JOB_ID"
echo "Time: $(date)"
echo "=================================================="
echo ""

RNDTMP="{rndtmp}"
echo "Creating temporary directory: $RNDTMP"
mkdir -p "$RNDTMP"

cleanup() {{
    echo ""
    echo "Cleaning up temporary directory: $RNDTMP"
    # Per-file unlink of the image tarball before the tree rm —
    # tarball is host-UID owned (cp'd in by this wrapper), so
    # plain rm reaches it without entering the user-namespace.
    # `${{LOCAL_IMAGE:-}}` guard covers early-exit paths that hit
    # the trap before LOCAL_IMAGE was assigned. See the secondary
    # wrapper's cleanup() for the rationale on per-file vs tree.
    if [ -n "${{LOCAL_IMAGE:-}}" ] && [ -e "$LOCAL_IMAGE" ]; then
        rm -f -- "$LOCAL_IMAGE" 2>/dev/null \
            || echo "WARNING: failed to unlink $LOCAL_IMAGE" >&2
    fi
    # `podman unshare rm -rf` is the only mechanism that reaches
    # the subuid-mapped files rootless `podman load` writes into
    # $RNDTMP/storage. Plain rm fallback keeps the wrapper safe
    # on hosts without podman. `--` blocks accidental flag
    # interpretation. Result logged AFTER the rm so a leak is
    # never silent.
    if podman unshare rm -rf -- "$RNDTMP" 2>/dev/null \
        || rm -rf -- "$RNDTMP" 2>/dev/null; then
        echo "Cleaned up temporary directory: $RNDTMP"
    else
        echo "ERROR: failed to clean up $RNDTMP — /tmp scratch leaked on $(hostname)" >&2
    fi
}}
# Also cleanup on SLURM-induced signals: SIGTERM is sent by sbatch
# at time-limit / scancel, SIGHUP by an ssh disconnect, SIGINT by
# Ctrl+C from interactive jobs. Without these, the trap fires only
# on graceful exit and SLURM-killed jobs leak /tmp/asm-XXXX dirs
# until the node's /tmp fills (observed in the field on multi-day
# clusters). EXIT alone misses every non-graceful termination.
trap cleanup EXIT TERM HUP INT

PODMAN_STORAGE="{podman_storage}"
PODMAN_RUN="{podman_run}"
mkdir -p "$PODMAN_STORAGE" "$PODMAN_RUN"
chmod 700 "$PODMAN_STORAGE" "$PODMAN_RUN"
export XDG_RUNTIME_DIR="$PODMAN_RUN"
echo "Podman storage: $PODMAN_STORAGE"
echo "Podman run root: $PODMAN_RUN"
echo "XDG_RUNTIME_DIR: $XDG_RUNTIME_DIR"
echo ""

echo "Copying image to local /tmp..."
LOCAL_IMAGE="$RNDTMP/{image_tar_basename}"
echo "  Source: {image_path}"
cp "{image_path}" "$LOCAL_IMAGE"
echo "  Size: $(du -h "$LOCAL_IMAGE" | cut -f1)"
echo ""

echo "Loading image..."
{load_command}
echo ""

echo "Verifying image is loaded..."
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" --cgroup-manager=cgroupfs images | grep {image_name} || echo "WARNING: Image not found in listing"
echo ""

echo "Testing secondary entrypoint ({container_command} --help)..."
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" --cgroup-manager=cgroupfs run --rm {image_ref} {container_command} --help | head -5
echo ""

echo "=================================================="
echo "Test Job Completed Successfully"
echo "Time: $(date)"
echo "=================================================="
"##,
        image_tar_basename = cfg.image_tar_basename,
        image_path = cfg.image_path,
        load_command = cfg.load_command,
        image_name = cfg.image_name,
        container_command = cfg.container_command,
    )
}
