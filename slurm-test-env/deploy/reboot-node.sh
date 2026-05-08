#!/usr/bin/env bash
# Restart one cluster node in place.
#
# Single concern: replace one container (gateway or a specific worker)
# with a fresh instance, reusing the existing image tags and shared
# state. Every other node keeps running. Useful when one slurmd has
# wedged badly enough that `scontrol update ... State=RESUME` alone
# won't bring the node back.
#
# Usage:
#   ./deploy/reboot-node.sh <node-hostname>
#
#   ./deploy/reboot-node.sh slurm-worker1
#   ./deploy/reboot-node.sh slurm-gateway
#
# The argument is the cluster-internal hostname (what `sinfo` /
# `scontrol show node` print) — that's the identifier the operator
# already has in hand when diagnosing a sick node.
#
# Preserved across reboot:
#   - shared /home (always; bind-mounted from the host)
#   - imported image tags (no re-import — that's `down.sh` / `up.sh`
#     territory; reboot reuses what `up.sh` loaded)
#   - podman network and the other nodes' DNS aliases
#
# Wiped on reboot (workers only):
#   - the per-worker host-side /tmp scratch dir, matching real-reboot
#     semantics (tmpfs is lost on reboot) and avoiding stale state
#     that may itself be why slurmd wedged.
#
# slurmctld does not auto-resume a node it previously marked DOWN, so
# after a worker reboot the script runs `scontrol update State=RESUME`
# directly inside the gateway container. No operator step needed.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SLURM_TEST_ENV_ENV_FILE:-${SCRIPT_DIR}/env.sh}"
# shellcheck disable=SC1090
source "$ENV_FILE"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

usage() {
  cat <<EOF
usage: reboot-node.sh <node-hostname>

Restart one node in place. Examples:
  ./deploy/reboot-node.sh slurm-worker1
  ./deploy/reboot-node.sh slurm-gateway

The shared /home and other nodes are not touched.
EOF
}

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 64
fi

case "$1" in
  -h|--help) usage; exit 0 ;;
esac

# --- Resolve target ----------------------------------------------------------

target_hostname="$1"
target_kind=
target_container=
target_index=

case "$target_hostname" in
  "$GATEWAY_HOSTNAME")
    target_kind=gateway
    target_container="$GATEWAY_NAME"
    ;;
  "${WORKER_HOSTNAME_PREFIX}"*)
    target_index="${target_hostname#"$WORKER_HOSTNAME_PREFIX"}"
    if [[ ! "$target_index" =~ ^[0-9]+$ ]]; then
      printf 'Invalid worker hostname: %q (expected %s<N>).\n' \
        "$target_hostname" "$WORKER_HOSTNAME_PREFIX" >&2
      exit 64
    fi
    if (( target_index < 1 || target_index > WORKER_COUNT )); then
      printf 'Worker index %s is outside this instance (WORKER_COUNT=%s).\n' \
        "$target_index" "$WORKER_COUNT" >&2
      exit 64
    fi
    target_kind=worker
    target_container="$(worker_container_name "$target_index")"
    ;;
  *)
    printf 'Unknown node hostname: %q\n' "$target_hostname" >&2
    printf 'Expected %s or %s<N>.\n' \
      "$GATEWAY_HOSTNAME" "$WORKER_HOSTNAME_PREFIX" >&2
    exit 64
    ;;
esac

# --- Verify the cluster has been brought up ---------------------------------
#
# A reboot makes no sense if no cluster is running — the image tag may
# not even exist in podman storage. The image tags are populated by
# up.sh's import step and removed by down.sh, so checking the relevant
# tag is a precise proxy for "has up.sh run for this instance".

case "$target_kind" in
  gateway) image_tag="$GATEWAY_IMAGE_TAG" ;;
  worker)  image_tag="$WORKER_IMAGE_TAG" ;;
esac

if ! podman image exists "$image_tag"; then
  printf 'Image %q is not loaded — run ./deploy/up.sh first.\n' \
    "$image_tag" >&2
  exit 1
fi

# --- Reboot ------------------------------------------------------------------

printf 'Stopping %s (%s)...\n' "$target_hostname" "$target_kind"
remove_node_container "$target_container"

if [[ "$target_kind" == worker ]]; then
  # `podman unshare` is required because nested rootless podman writes
  # files owned by mapped subuids the host operator can't unlink
  # directly — same constraint that down.sh hits on $WORKER_TMP_BASE.
  w_tmp="$(worker_tmp_dir "$target_index")"
  if [[ -d "$w_tmp" ]]; then
    podman unshare rm -rf -- "$w_tmp"
  fi
fi

printf 'Starting %s...\n' "$target_hostname"
case "$target_kind" in
  gateway) start_gateway ;;
  worker)  start_worker "$target_index" ;;
esac

# --- Notify slurmctld of a worker restart -----------------------------------
#
# slurmctld does not auto-resume a node it previously marked DOWN — even
# once slurmd checks in, the controller keeps the DOWN flag until told
# otherwise. Run `scontrol update State=RESUME` directly inside the
# gateway container so the operator does not have to. Skipped on
# gateway reboot (the gateway *is* slurmctld; workers reconnect on
# their own when it comes back).
#
# `podman exec` runs as root in the gateway container, which has the
# munge key needed to authenticate to slurmctld locally — no ssh /
# user-key dance.
if [[ "$target_kind" == worker ]]; then
  printf 'Resuming %s in slurmctld...\n' "$target_hostname"
  if ! podman exec "$GATEWAY_NAME" \
       scontrol update "NodeName=${target_hostname}" State=RESUME; then
    cat >&2 <<EOF

slurmctld notification failed — the worker container is running but
slurmctld may still mark it DOWN. Once the gateway is reachable:

  podman exec ${GATEWAY_NAME} scontrol update NodeName=${target_hostname} State=RESUME

EOF
    exit 1
  fi
fi

cat <<EOF

=== slurm-test-env :: ${target_hostname} restarted ===

EOF
