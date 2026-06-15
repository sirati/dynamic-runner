# shellcheck shell=bash
# Shared lifecycle helpers for slurm-test-env nodes.
#
# Single concern: launch a configured container (gateway or worker)
# with the canonical run flags. Both `up.sh` (whole-cluster bring-up)
# and `reboot-node.sh` (single-node restart) consume these helpers, so
# the run-flag set has exactly one source of truth — adding a new
# mount, label, or capability is a single-file edit here.
#
# Prerequisites: env.sh must be sourced before this file. The guards
# below fail loudly if a caller forgets.

: "${NETWORK:?NETWORK unset; source deploy/env.sh before lib.sh}"
: "${HOME_SHARE:?HOME_SHARE unset; source deploy/env.sh before lib.sh}"

# Common flags every node container shares.
#
# --systemd=always:  PID1 is /init (NixOS systemd); podman wires up
#                    /sys/fs/cgroup, /run, signal forwarding accordingly.
# --privileged:      required for systemd cgroup management + slurm
#                    cgroup tracking + nested podman on workers.
#                    Acceptable: explicit local test harness, not a
#                    virtualization boundary.
# --entrypoint /init: imported tarball carries no metadata, so the
#                    entrypoint is declared at run time.
NODE_COMMON_FLAGS=(
  --network "$NETWORK"
  --systemd=always
  --privileged
  --entrypoint /init
  -v "${HOME_SHARE}:/home:rw"
)

# Remove a container if present, in a netavark-safe order. Silent —
# names are internal.
#
# Sequence is stop → network disconnect → rm -f. The ordering matters
# because `podman rm -f` on a still-attached container makes netavark
# tear down the bridge attachment as part of removal, and if the
# container's network namespace is in a bad state (e.g. cpuset-wedged
# processes inside it — see #560 / #561) that teardown trips
# setns(2)=EIO and globally wedges aardvark-dns on the host. Doing
# the disconnect first runs the netavark teardown while podman's
# bookkeeping is still consistent and aardvark records are cleanly
# released; the subsequent `rm -f` then has no network teardown left
# to do. The `stop` is a defensive front-end: it lets PID 1 reap its
# children and exit cleanly on the common path, reducing how often
# the bad-ns code path is reached at all.
#
# Every step is `|| true`-guarded so set -euo pipefail doesn't trip
# on an already-stopped container, an already-disconnected network,
# or a container that races to exit between `exists` and `rm`.
remove_node_container() {
  local name="$1"
  if podman container exists "$name"; then
    podman stop -t 10 "$name" >/dev/null 2>&1 || true
    podman network disconnect "$NETWORK" "$name" >/dev/null 2>&1 || true
    podman rm -f "$name" >/dev/null 2>&1 || true
  fi
}

# `podman exec` doesn't run a login shell, so the container PATH is
# the image's minimal default — NixOS keeps its real binaries under
# /run/current-system/sw/bin and that's not on the default PATH.
# pexec()/pexec_i() inject that prefix so bare command names (id,
# getent, useradd, scontrol, install, ...) resolve. Used by both
# host-side host-to-container scripts (reboot-node.sh, scripts/
# provision-user.sh) — keep it here so the PATH list has exactly
# one source of truth.
pexec() {
  podman exec --env PATH=/run/current-system/sw/bin:/usr/bin:/bin "$@"
}
pexec_i() {
  podman exec -i --env PATH=/run/current-system/sw/bin:/usr/bin:/bin "$@"
}

# Start the gateway container.
#
# --name           globally visible container name (instance-suffixed).
# --hostname       cluster-internal name; matches slurm.conf.
# --network-alias  cluster-internal DNS — distinct from --name so two
#                  instances can both alias their gateways to
#                  "slurm-gateway" inside their own networks.
start_gateway() {
  podman run -d \
    --name "$GATEWAY_NAME" \
    --hostname "$GATEWAY_HOSTNAME" \
    --network-alias "$GATEWAY_HOSTNAME" \
    --memory "$GATEWAY_MEMORY" \
    --cpus "$GATEWAY_CPUS" \
    --publish "${SSH_PORT}:22" \
    --tmpfs "/tmp:rw,nosuid,size=512m" \
    "${NODE_COMMON_FLAGS[@]}" \
    "$GATEWAY_IMAGE_TAG" >/dev/null
}

# Start worker N (1..$WORKER_COUNT). Each worker's /tmp is a host-side
# bind-mount under $WORKER_TMP_BASE — multi-GB worker scratch (image
# tarballs, etc.) would otherwise blow an in-container tmpfs cap.
start_worker() {
  local i="$1"
  local c_name h_name w_tmp
  c_name="$(worker_container_name "$i")"
  h_name="$(worker_hostname "$i")"
  w_tmp="$(worker_tmp_dir "$i")"
  mkdir -p "$w_tmp"
  # World-writable + sticky bit so any uid inside the container (root,
  # slurm users, nested-podman subuids) can write — matches tmpfs
  # semantics.
  chmod 1777 "$w_tmp"
  # --memory-swap=-1 makes the host's full swap available to the
  # worker on top of its $WORKER_MEMORY RAM cap. Podman's default
  # when --memory is set but --memory-swap is unset is 2 × --memory
  # (so 4 GiB worker would silently get 4 GiB swap regardless of
  # what swap the host has); -1 instead grants unlimited swap, so a
  # workload that briefly spikes past 4 GiB swaps gracefully on a
  # swap-equipped host instead of cgroup-OOMing. The RAM cap is
  # still 4 GiB — workloads that genuinely need more RAM still
  # surface the over-allocation, just as swap pressure rather than
  # immediate kill.
  podman run -d \
    --name "$c_name" \
    --hostname "$h_name" \
    --network-alias "$h_name" \
    --memory "$WORKER_MEMORY" \
    --memory-swap=-1 \
    --cpus "$WORKER_CPUS" \
    --pids-limit "$WORKER_PIDS_LIMIT" \
    -v "${w_tmp}:/tmp:rw" \
    "${NODE_COMMON_FLAGS[@]}" \
    "$WORKER_IMAGE_TAG" >/dev/null
}
