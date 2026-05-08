#!/usr/bin/env bash
# Bring up the slurm-test-env cluster on the local podman engine.
#
# Single concern: turn the two image tarballs (built by `nix build` or
# pre-injected via $SLURM_TEST_ENV_*_IMAGE by the flake wrapper) into a
# running set of containers — one gateway and N workers — with the shared
# /home bind mount wired in.
#
# Image lifecycle: tarballs are imported with INSTANCE_ID-scoped tags on
# `up.sh` and removed by `down.sh`. Nothing remains in podman storage
# after teardown; only the simulated /home persists on the host.
#
# No secret management: munge keys are baked into the images via a fixed
# insecure dev key (see modules/slurm-cluster.nix). User pubkeys land on
# the shared /home through provision-user.sh — the shared mount itself is
# the distribution mechanism.

set -euo pipefail

# --- Locate config -----------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SLURM_TEST_ENV_ENV_FILE:-${SCRIPT_DIR}/env.sh}"
# shellcheck disable=SC1090
source "$ENV_FILE"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

printf '\n=== slurm-test-env :: bringing up cluster ===\n'
print_layout

# --- Resolve image tarballs --------------------------------------------------
#
# Two flows:
#   - $SLURM_TEST_ENV_*_IMAGE pre-set (flake-wrapped run) → these are the
#     build-output directories;
#   - unset → run nix build on the sibling flake to materialize them.
#
# .rules requires --max-jobs 6 --cores 4 to avoid OOM on this host.

resolve_image() {
  local var_name="$1" attr="$2"
  local value="${!var_name:-}"
  if [[ -n "$value" ]]; then
    printf '%s' "$value"
    return
  fi
  printf 'Building %s via nix...\n' "$attr" >&2
  nix build \
    --no-link \
    --print-out-paths \
    --max-jobs 6 \
    --cores 4 \
    "${SCRIPT_DIR}/..#${attr}"
}

# nixpkgs' `virtualisation/docker-image.nix` produces a flat NixOS tarball
# at `<out>/tarball/nixos-system-*.tar.xz` (NOT an OCI layered image). It
# must be loaded via `podman import` and started with `/init` as the
# entrypoint; `podman load` would not work here.
locate_tarball() {
  local out_dir="$1"
  if [[ -f "$out_dir" ]]; then
    printf '%s' "$out_dir"
    return
  fi
  local found
  found="$(find "$out_dir" -name 'nixos-system-*.tar.xz' -type f 2>/dev/null | head -n1)"
  if [[ -z "$found" ]]; then
    printf 'Could not locate nixos-system-*.tar.xz under %q\n' "$out_dir" >&2
    exit 1
  fi
  printf '%s' "$found"
}

gateway_out="$(resolve_image SLURM_TEST_ENV_GATEWAY_IMAGE gateway-image)"
worker_out="$(resolve_image SLURM_TEST_ENV_WORKER_IMAGE worker-image)"
gateway_tar="$(locate_tarball "$gateway_out")"
worker_tar="$(locate_tarball "$worker_out")"

# --- Shared host-side mount points -------------------------------------------
#
# Only /home is shared across the cluster — the simulated network drive
# every container sees at /home. Each container also gets its own
# writable /tmp (gateway via --tmpfs, workers via per-container host
# bind-mount, see below); nothing else is shared between containers.

mkdir -p "$HOME_SHARE"

# --- Network -----------------------------------------------------------------
#
# A user-defined podman network gives the containers internal DNS — so
# slurm.conf can name them by their cluster-internal hostnames — and
# default NAT to the host so workers reach the public internet. Only the
# gateway publishes a host port; workers stay on the internal segment.

if ! podman network exists "$NETWORK"; then
  printf 'Creating cluster network...\n'
  podman network create "$NETWORK" >/dev/null
fi

# --- Import images with instance-scoped tags --------------------------------

printf 'Importing images...\n'
podman import "$gateway_tar" "$GATEWAY_IMAGE_TAG" >/dev/null
podman import "$worker_tar" "$WORKER_IMAGE_TAG" >/dev/null

# --- Refuse to overwrite a running cluster -----------------------------------
#
# The error message is intentionally generic — it identifies the problem
# (instance is already up) without revealing the internal naming scheme.

abort_if_running() {
  local name="$1"
  if podman container exists "$name"; then
    printf 'Cluster instance is already running; run down.sh first.\n' >&2
    exit 1
  fi
}
abort_if_running "$GATEWAY_NAME"
for i in $(seq 1 "$WORKER_COUNT"); do
  abort_if_running "$(worker_container_name "$i")"
done

# --- Run gateway + workers ---------------------------------------------------
#
# Per-node run flags and the common-flags array live in deploy/lib.sh —
# the same helpers are reused by reboot-node.sh, so the canonical run
# command has a single source of truth.

printf 'Starting gateway...\n'
start_gateway

printf 'Starting %d worker(s)...\n' "$WORKER_COUNT"
mkdir -p "$WORKER_TMP_BASE"
for i in $(seq 1 "$WORKER_COUNT"); do
  start_worker "$i"
done

cat <<EOF

=== slurm-test-env :: cluster up ===
EOF
print_layout

cat <<EOF
Provision a user (host-side, with the same INSTANCE_ID you set for up.sh):
  ./scripts/provision-user.sh <username> <pubkey-file>
  # or, when running from the flake:
  nix run .#provision-user -- <username> <pubkey-file>

Then submit slurm jobs from the gateway:
  ssh -p ${SSH_PORT} -i <matching-private-key> <username>@localhost
  srun --partition=debug -N1 hostname
  sinfo

EOF
