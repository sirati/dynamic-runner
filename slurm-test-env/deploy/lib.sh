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

# Remove a container if present. Silent — names are internal.
remove_node_container() {
  local name="$1"
  if podman container exists "$name"; then
    podman rm -f "$name" >/dev/null
  fi
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
  podman run -d \
    --name "$c_name" \
    --hostname "$h_name" \
    --network-alias "$h_name" \
    --memory "$WORKER_MEMORY" \
    --cpus "$WORKER_CPUS" \
    -v "${w_tmp}:/tmp:rw" \
    "${NODE_COMMON_FLAGS[@]}" \
    "$WORKER_IMAGE_TAG" >/dev/null
}
