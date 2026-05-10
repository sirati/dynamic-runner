#!/usr/bin/env bash
# Host-side cluster user provisioner for slurm-test-env.
#
# Usage:  provision-user.sh <username> <pubkey-file>
#
# Single concern: ensure a POSIX user exists on every cluster container with
# a consistent UID/GID, and authorize an SSH pubkey for that user on the
# shared /home mount so they can log into the gateway (and any worker).
#
# Why this script lives on the *host* and not in a container:
#   - the operator already controls the host; running `podman exec` from
#     here is auth-free, whereas running it from inside a container would
#     require some auth mechanism we explicitly avoid.
#   - all per-container ops (groupadd/useradd/install pubkey) are issued
#     via `podman exec`, which authenticates by container ownership.
#
# Why UIDs must be consistent across containers:
#   - the shared /home is a plain bind mount; the kernel records files by
#     numeric UID/GID, so a mismatch surfaces as permission errors.
#   - slurmd on workers spawns jobs under the submitting user's UID; that
#     UID must resolve to the same name as on the gateway.
#   - real NFS-backed clusters have the same constraint; reproduced here.

set -euo pipefail

# --- Argument parsing --------------------------------------------------------

if [[ $# -ne 2 ]]; then
  cat >&2 <<EOF
usage: $0 <username> <pubkey-file>

  username     POSIX-compatible name, [a-z_][a-z0-9_-]{0,31}
  pubkey-file  path to an OpenSSH public key file (will be appended to the
               user's ~/.ssh/authorized_keys on the shared /home mount)

The user is created idempotently on the gateway and on every worker
container with a consistent UID/GID. The pubkey is appended; calling this
script repeatedly with different keys grants the same user multiple keys.
EOF
  exit 64
fi

username="$1"
pubkey_file="$2"

if [[ ! "$username" =~ ^[a-z_][a-z0-9_-]{0,31}$ ]]; then
  printf 'Invalid username %q (allowed: [a-z_][a-z0-9_-]{0,31}).\n' "$username" >&2
  exit 65
fi

if [[ ! -r "$pubkey_file" ]]; then
  printf 'Pubkey file %q is not readable.\n' "$pubkey_file" >&2
  exit 66
fi

pubkey_content="$(cat -- "$pubkey_file")"
if [[ -z "$pubkey_content" ]]; then
  printf 'Pubkey file %q is empty.\n' "$pubkey_file" >&2
  exit 66
fi

# --- Locate config -----------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SLURM_TEST_ENV_ENV_FILE:-${SCRIPT_DIR}/../deploy/env.sh}"
# shellcheck disable=SC1090
source "$ENV_FILE"
# pexec()/pexec_i() live in lib.sh — see deploy/up.sh for the
# SLURM_TEST_ENV_LIB_SH env-var-with-fallback rationale.
LIB_SH="${SLURM_TEST_ENV_LIB_SH:-${SCRIPT_DIR}/../deploy/lib.sh}"
# shellcheck disable=SC1090
source "$LIB_SH"

# --- UID assignment ----------------------------------------------------------
#
# The cluster's in-container /etc/passwd is the source of truth for UID
# allocation: the scan target is `getent passwd` inside the gateway
# container, NOT a host-side counter file or a host-side stat of the
# shared /home. Two reasons:
#
#   1. Podman on this host runs rootless. Numeric UIDs in the bind-mounted
#      /home are user-namespace-translated subuids, not the container-
#      visible cluster UIDs. A host-side stat would scan the wrong number
#      space; reading /etc/passwd inside the container gives us the
#      unmapped (cluster) UID directly.
#   2. /etc/passwd already exists and is updated atomically by useradd —
#      we don't need a parallel counter file that could drift out of sync.
#
# Cross-down/up persistence: /etc/passwd dies with the container, but the
# shared /home survives. We write a `.cluster_uid` marker file inside
# each provisioned home (plain text, world-readable, no UID translation
# needed) so a re-provision after `down` reuses the same UID and avoids
# orphaning the operator's previously-stored files.
#
# flock guards against concurrent operator invocations. UID_BASE keeps us
# clear of system UIDs.

UID_LOCK="${STATE_DIR}/uid.lock"
# Cluster UIDs live in a fixed window above the system range and below
# the subuid pool that hosts nested rootless podman.
#   * UID_BASE keeps us clear of OS users (0-999) and any allocations
#     under 10000.
#   * UID_CEILING stops short of the subuid pool. Without the ceiling
#     the scan would pick up the high-end `nobody` user (65534) and
#     hand out 65535 — the kernel's invalid-UID sentinel — and would
#     also collide with the subuid pool.
UID_BASE=10000
UID_CEILING=20000

# Shared subuid pool — see modules/common.nix for the full layout
# rationale. SUBUID_BASE..SUBUID_BASE+SUBUID_COUNT-1 must stay within
# the container's user namespace (max uid 65535) and must not overlap
# the cluster UID range (UID_BASE..UID_CEILING).
SUBUID_BASE=20000
SUBUID_COUNT=45536

mkdir -p "$STATE_DIR"

# Compose the full cluster-node roster from env.sh shape.
all_nodes=("$GATEWAY_NAME")
for i in $(seq 1 "$WORKER_COUNT"); do
  all_nodes+=("$(worker_container_name "$i")")
done

# Verify cluster is up before we start mutating anything.
for c in "${all_nodes[@]}"; do
  if ! podman container exists "$c"; then
    printf 'Cluster is not running; bring it up first with up.sh.\n' >&2
    exit 70
  fi
done

# Determine whether the user already exists somewhere — and if so, what UID
# we must reuse. Three sources of truth, checked in order:
#   1. live cluster nodes (most authoritative when the cluster is up and
#      consistent)
#   2. the .cluster_uid marker on the persistent shared /home (survives
#      down/up cycles without --purge — its contents ARE the cluster UID,
#      written in plain text precisely so it can be read from the host
#      without any UID-namespace translation)
#   3. fresh allocation: scan /etc/passwd in the gateway, take max+1
existing_uid=""
for c in "${all_nodes[@]}"; do
  if uid_in_c="$(pexec "$c" id -u "$username" 2>/dev/null)"; then
    if [[ -n "$existing_uid" && "$existing_uid" != "$uid_in_c" ]]; then
      printf 'FATAL: user %q has uid=%s on some nodes and uid=%s on others.\n' \
        "$username" "$existing_uid" "$uid_in_c" >&2
      printf 'The cluster is in an inconsistent state; manual cleanup required.\n' >&2
      exit 71
    fi
    existing_uid="$uid_in_c"
  fi
done

if [[ -z "$existing_uid" ]]; then
  marker="${HOME_SHARE}/${username}/.cluster_uid"
  if [[ -f "$marker" ]]; then
    existing_uid="$(cat -- "$marker")"
    printf 'Found existing /home/%s with cluster_uid=%s; reusing.\n' \
      "$username" "$existing_uid"
  fi
fi

if [[ -n "$existing_uid" ]]; then
  cluster_uid="$existing_uid"
  cluster_gid="$cluster_uid"
  printf 'User %q already exists with uid=%s; reconciling missing nodes.\n' \
    "$username" "$cluster_uid"
else
  exec 9>"$UID_LOCK"
  flock 9
  # Scan the gateway's /etc/passwd for the highest cluster UID in use;
  # the next free one is max+1. The gateway is authoritative because
  # every fresh provisioning registers there first (inside this same
  # flock, below) — so concurrent provisioners always see each other's
  # claims when they scan.
  max_uid=$(
    pexec "$GATEWAY_NAME" getent passwd \
      | awk -F: -v lo="$UID_BASE" -v hi="$UID_CEILING" \
          '$3 >= lo && $3 < hi { print $3 }' \
      | sort -n | tail -1
  )
  max_uid="${max_uid:-$((UID_BASE - 1))}"
  cluster_uid=$((max_uid + 1))
  cluster_gid="$cluster_uid"
  # Atomically claim by registering in the gateway's /etc/passwd inside
  # the flock. groupadd may fail if the GID is already in use by an
  # unrelated group (extremely unlikely in this UID range); let it
  # surface as an error.
  pexec "$GATEWAY_NAME" groupadd --gid "$cluster_gid" "$username"
  pexec "$GATEWAY_NAME" useradd \
    --uid "$cluster_uid" \
    --gid "$cluster_gid" \
    --no-create-home \
    --home-dir "/home/${username}" \
    --shell /run/current-system/sw/bin/bash \
    "$username"
  flock -u 9
  exec 9>&-
  printf 'Allocated uid=%s for user %q.\n' "$cluster_uid" "$username"
fi

# --- Per-node useradd --------------------------------------------------------
#
# Always --no-create-home: the home directory lives on the shared /home
# mount and is materialized below (in-container, by install -d, so the
# user-namespace mapping records the right ownership). Doing
# --create-home in N containers would race on the shared mount anyway.
#
# For freshly-allocated UIDs the gateway useradd happened inside the
# flock above; this loop covers every other node. For reused UIDs the
# loop covers every node — `id -u` short-circuits if already present.
# Loop is silent — progress would just leak internal node names (scope-
# private). Failures still surface naturally via set -e.

for c in "${all_nodes[@]}"; do
  if ! pexec "$c" id -u "$username" >/dev/null 2>&1; then
    pexec "$c" groupadd --gid "$cluster_gid" "$username"
    pexec "$c" useradd \
      --uid "$cluster_uid" \
      --gid "$cluster_gid" \
      --no-create-home \
      --home-dir "/home/${username}" \
      --shell /run/current-system/sw/bin/bash \
      "$username"
  fi
  # Subuid/subgid: shared pool, written identically on every node.
  # SUB_UID_COUNT=0 in modules/common.nix disables useradd's auto-
  # allocation (which would pick the default 100000+ range, outside
  # the container's 65536-uid namespace), so this is the sole writer
  # of /etc/subuid + /etc/subgid for cluster users. Idempotent: any
  # prior entry for the same user is removed first, then re-added,
  # keeping the layout stable across re-provisionings.
  pexec_i "$c" /run/current-system/sw/bin/bash -c "
    set -eu
    /run/current-system/sw/bin/sed -i '/^${username}:/d' /etc/subuid /etc/subgid
    printf '%s:%s:%s\n' '${username}' '${SUBUID_BASE}' '${SUBUID_COUNT}' >> /etc/subuid
    printf '%s:%s:%s\n' '${username}' '${SUBUID_BASE}' '${SUBUID_COUNT}' >> /etc/subgid
  "
done

# --- Materialize home + marker + authorized_keys (gateway only — /home is shared)
#
# All filesystem ops on /home are issued in-container (via podman exec)
# rather than on the host. Reason: host-side ops under rootless podman
# would write files owned by the operator's host UID, which the in-
# container view sees as user-namespace-translated to a wrong number;
# in-container ops use the native cluster UID space and the kernel
# records the user-namespace-mapped subuid on the host, which is the
# coherent encoding.
#
# Single node (gateway) chosen arbitrarily — the filesystem is shared.
#
# The .cluster_uid marker is written every time (idempotent overwrite
# with the same value) so a `down`/`up` cycle followed by re-provisioning
# the same user reuses the same UID — preventing orphaned files on the
# persistent shared /home.

home_dir="/home/${username}"
ssh_dir="${home_dir}/.ssh"
auth_keys="${ssh_dir}/authorized_keys"
marker_in="${home_dir}/.cluster_uid"

pexec "$GATEWAY_NAME" install -d -m 0755 -o "$username" -g "$username" "$home_dir"
pexec "$GATEWAY_NAME" install -d -m 0700 -o "$username" -g "$username" "$ssh_dir"
pexec_i "$GATEWAY_NAME" /run/current-system/sw/bin/bash -c "
  set -eu
  printf '%s\n' '${cluster_uid}' > '${marker_in}'
  chown '${username}:${username}' '${marker_in}'
  chmod 0644 '${marker_in}'
"

# Append each pubkey line only if not already present (line-by-line dedup,
# so a repeated invocation with the same file is a no-op and the file
# does not grow unboundedly on retries; multi-key files are also handled
# correctly because each line is matched independently).
pexec_i "$GATEWAY_NAME" \
  /run/current-system/sw/bin/bash -c "
    set -eu
    auth='${auth_keys}'
    touch \"\$auth\"
    chown '${username}':'${username}' \"\$auth\"
    chmod 0600 \"\$auth\"
    added=0
    while IFS= read -r line; do
      if [[ -z \"\$line\" ]]; then
        continue
      fi
      if ! /run/current-system/sw/bin/grep -qxF -- \"\$line\" \"\$auth\"; then
        printf '%s\n' \"\$line\" >> \"\$auth\"
        added=\$((added + 1))
      fi
    done
    echo \"  authorized_keys: +\${added} new key(s)\"
  " <<<"$pubkey_content"

cat <<EOF

User ${username} provisioned across the cluster (uid=${cluster_uid}).
SSH login: ssh -p ${SSH_PORT} -i <matching-private-key> ${username}@localhost
EOF
