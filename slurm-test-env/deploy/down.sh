#!/usr/bin/env bash
# Tear down the slurm-test-env cluster.
#
# Stops and removes every globally-scoped resource for this instance —
# containers, podman network, instance-scoped image tags, and the UID
# allocation lock file — leaving the host's podman storage with no trace
# of this run. The simulated /home (= $HOME_SHARE) is preserved so the
# operator can inspect test output post-mortem; pass --purge to wipe it.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SLURM_TEST_ENV_ENV_FILE:-${SCRIPT_DIR}/env.sh}"
# shellcheck disable=SC1090
source "$ENV_FILE"

purge=0
for arg in "$@"; do
  case "$arg" in
    --purge) purge=1 ;;
    -h|--help)
      cat <<EOF
usage: down.sh [--purge]

Stops the cluster. The simulated /home is preserved at
${HOME_SHARE} for post-test inspection.

  --purge   also wipe the simulated /home.
EOF
      exit 0
      ;;
    *)
      printf 'Unknown argument: %q\n' "$arg" >&2
      exit 64
      ;;
  esac
done

# All teardown helpers below are intentionally silent — they remove
# internal podman objects whose names should never surface to the user.
# Only the final summary block prints anything, and it shows just the
# host-visible /home path (the one persistent artifact).

remove_container() {
  local name="$1"
  if podman container exists "$name"; then
    podman rm -f "$name" >/dev/null
  fi
}

remove_image() {
  local tag="$1"
  if podman image exists "$tag"; then
    podman image rm "$tag" >/dev/null
  fi
}

printf 'Stopping cluster...\n'

# --- Containers --------------------------------------------------------------

remove_container "$GATEWAY_NAME"
for i in $(seq 1 "$WORKER_COUNT"); do
  remove_container "$(worker_container_name "$i")"
done

# --- Network + images --------------------------------------------------------
#
# Always cleared on `down` so a stopped cluster leaves nothing behind in
# podman storage. up.sh recreates the network and reimports the images on
# next invocation — the sole persistent artifact is $HOME_SHARE below.

if podman network exists "$NETWORK"; then
  podman network rm "$NETWORK" >/dev/null
fi

remove_image "$GATEWAY_IMAGE_TAG"
remove_image "$WORKER_IMAGE_TAG"

# --- UID-allocation lock file ------------------------------------------------
#
# Pure coordination state — the actual UID assignments live as ownership
# bits on $HOME_SHARE/<user>/, which provision-user.sh re-derives by
# scanning. Safe to drop unconditionally.

rm -f -- "${STATE_DIR}/uid.lock"

# --- Optional state wipe -----------------------------------------------------

if (( purge )); then
  if [[ -d "$STATE_DIR" ]]; then
    # Files under $HOME_SHARE (and any other in-container-written dir)
    # are owned by user-namespace-mapped subuids that the operator can
    # neither chown nor unlink directly. `podman unshare` enters the
    # mapping so the unlink succeeds for those subuid-owned files.
    podman unshare rm -rf -- "$STATE_DIR"
  fi
  cat <<EOF

=== slurm-test-env :: cluster down (purged) ===

  simulated /home:    ${HOME_SHARE}    (removed)

EOF
else
  cat <<EOF

=== slurm-test-env :: cluster down ===

  simulated /home:    ${HOME_SHARE}    (preserved for inspection)

Pass --purge to also wipe the simulated /home.

EOF
fi
