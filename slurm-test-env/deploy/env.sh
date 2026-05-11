# shellcheck shell=bash
# Deploy-time configuration for slurm-test-env.
#
# Sourced by up.sh, down.sh, and scripts/provision-user.sh. Override any
# value by exporting it before invocation:
#
#   INSTANCE_ID=ci-job-42 SSH_PORT=2299 ./deploy/up.sh
#
# Two concurrent clusters MUST differ on:
#   - INSTANCE_ID  (suffix on container names, podman network, image tags,
#                   state dir — i.e. every globally-scoped resource)
#   - SSH_PORT     (host TCP listen port)
# Everything else can stay at defaults.

# --- Instance identity (REQUIRED) -------------------------------------------
#
# The instance id is the suffix on every globally-scoped name. Cluster-
# internal names (hostnames, network aliases, slurm.conf entries) stay
# stable across instances — those live inside each instance's own podman
# network namespace and never collide there.
#
# This is intentionally not given a default value: silently sharing an id
# between concurrent test runs would corrupt their shared /home and
# scramble UIDs. The operator must pick one explicitly.

if [[ -z "${INSTANCE_ID:-}" ]]; then
  cat >&2 <<'EOF'
INSTANCE_ID is not set.

This identifier scopes every globally-visible resource (podman containers,
network, image tags, host state dir) so that multiple slurm-test-env
clusters can run concurrently on the same host without colliding. Pick any
short alphanumeric tag — it is local to your host:

  INSTANCE_ID=alpha SSH_PORT=2222 ./up.sh
  INSTANCE_ID=beta  SSH_PORT=2322 ./up.sh
EOF
  return 1 2>/dev/null || exit 1
fi

if [[ ! "$INSTANCE_ID" =~ ^[a-zA-Z0-9_-]+$ ]]; then
  printf 'Invalid INSTANCE_ID %q (must match [a-zA-Z0-9_-]+).\n' "$INSTANCE_ID" >&2
  return 1 2>/dev/null || exit 1
fi

# --- Cluster shape -----------------------------------------------------------

: "${WORKER_COUNT:=4}"

# Cluster-internal names. Match the slurm.conf NodeName/ControlMachine
# entries baked into modules/slurm-cluster.nix; do NOT change without
# rebuilding the images.
GATEWAY_HOSTNAME="slurm-gateway"
WORKER_HOSTNAME_PREFIX="slurm-worker"

# Globally-scoped names — all carry the INSTANCE_ID suffix so concurrent
# instances do not collide on the host's podman engine.
GATEWAY_NAME="${GATEWAY_HOSTNAME}-${INSTANCE_ID}"
NETWORK="slurm-test-net-${INSTANCE_ID}"
GATEWAY_IMAGE_TAG="slurm-test-env-${INSTANCE_ID}-gateway:latest"
WORKER_IMAGE_TAG="slurm-test-env-${INSTANCE_ID}-worker:latest"

# Naming helpers consumed by up/down/provision scripts. Defined here so
# the naming scheme has exactly one source of truth — adding e.g. a port
# offset, a different separator, or a new node role is a single-file edit.

worker_hostname()       { printf '%s%s' "$WORKER_HOSTNAME_PREFIX" "$1"; }
worker_container_name() { printf '%s%s-%s' "$WORKER_HOSTNAME_PREFIX" "$1" "$INSTANCE_ID"; }
worker_tmp_dir()        { printf '%s/%s' "$WORKER_TMP_BASE" "$(worker_hostname "$1")"; }

# --- Host-side filesystem layout --------------------------------------------
#
# Per-instance tree:
#
#   $STATE_DIR/                    persistent root for this instance
#     home/  (= $HOME_SHARE)       the simulated shared drive seen as /home
#     uid.lock                     flock target during user provisioning
#
# `down.sh` leaves $STATE_DIR in place; the simulated /home is what the
# operator will want to inspect for test output, and re-creating it is a
# deliberate manual step (rm -rf under `podman unshare` if needed).

: "${STATE_BASE_DIR:=${HOME}/.local/state/slurm-test-env}"
STATE_DIR="${STATE_BASE_DIR}/${INSTANCE_ID}"
HOME_SHARE="${STATE_DIR}/home"

# Per-worker /tmp lives on the host's real /tmp instead of an in-container
# tmpfs: image tarballs (multi-GB) and other worker scratch can otherwise
# blow a tmpfs's size cap. The base dir is per-instance so concurrent
# clusters don't share scratch; per-worker subdirs are created in up.sh
# and removed wholesale in down.sh.
: "${WORKER_TMP_BASE:=${TMPDIR:-/tmp}/slurm-test-env-${INSTANCE_ID}}"

# --- Host SSH port ----------------------------------------------------------
#
# MUST be unique per concurrent instance. There is no auto-default that
# can pick a free port without races; the operator chooses.

: "${SSH_PORT:=2222}"

# --- Per-container resource caps --------------------------------------------

: "${GATEWAY_MEMORY:=1g}"
: "${GATEWAY_CPUS:=2}"
: "${WORKER_MEMORY:=4g}"
: "${WORKER_CPUS:=2}"

# Cgroup pids.max for worker containers. Podman's default --pids-limit
# for rootless containers is 2048, which a fork-heavy nix build
# (configure + parallel make + cc + ld) blows past container-wide
# regardless of how the per-job slurm cgroup is set up — observed end-
# to-end on ds-test 2026-05-11 (run_20260511_135927) where slurmd
# survived (post-2bf8410) but variant builds hit `Broken pipe` /
# `unexpected end-of-file` because the nix-build subprocess tree hit
# the 2048 ceiling. 32768 matches the framework's per-process nproc
# ceiling (--ulimit nproc=32768, task #20) — same scale at the cgroup
# layer for symmetry, large enough that realistic workloads don't
# trip it. Gateway containers are not affected (slurmctld + sshd
# don't fork much), so no GATEWAY_PIDS_LIMIT — added only when a
# concrete failure surfaces.
: "${WORKER_PIDS_LIMIT:=32768}"

# --- User-facing summary ----------------------------------------------------
#
# Deliberately shows only what an operator of a real slurm cluster would
# care about: where the shared /home is on the host (for post-test
# inspection), and how to ssh in. Everything else — container names,
# network, image tags, state-dir layout — is implementation detail and
# is intentionally hidden. Users of this harness should never need to
# learn the internal naming scheme.

print_layout() {
  cat <<EOF

  simulated /home:    ${HOME_SHARE}
  ssh login:          ssh -p ${SSH_PORT} <user>@localhost

EOF
}
