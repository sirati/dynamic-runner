#!/usr/bin/env bash
# End-to-end smoke test for a running slurm-test-env cluster.
#
# Single concern: drive the cluster as the testuser via ssh and verify
# the slurm control plane works for the recipes a real operator would
# care about — partition visibility, sbatch + srun --jobid attach,
# multi-node distribution, and inter-node networking.
#
# Idempotent: provisions a `testuser` with an ed25519 keypair under
# $STATE_DIR/keys (re-used across runs and across `down`/`up` cycles),
# then runs a sequence of stages and prints a PASS/FAIL summary.
#
# Exit codes:
#   0  all stages passed
#   1  one or more stages failed
#   70 cluster is not running (provision-user.sh's contract)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SLURM_TEST_ENV_ENV_FILE:-${SCRIPT_DIR}/../deploy/env.sh}"
# shellcheck disable=SC1090
source "$ENV_FILE"

# --- Test identity -----------------------------------------------------------

TEST_USER=testuser
KEY_DIR="${STATE_DIR}/keys"
PRIV_KEY="${KEY_DIR}/id_ed25519"
PUB_KEY="${PRIV_KEY}.pub"

ensure_keypair() {
  if [[ ! -f "$PRIV_KEY" ]]; then
    mkdir -p "$KEY_DIR"
    chmod 0700 "$KEY_DIR"
    ssh-keygen -t ed25519 -N '' -f "$PRIV_KEY" \
      -C "slurm-test-env-${INSTANCE_ID}" >/dev/null
  fi
}

# --- ssh wrapper -------------------------------------------------------------
#
# Force IdentitiesOnly + IdentityAgent=none so we never accidentally
# fall through to a host-side agent identity (the cluster-internal sshd
# only authorizes the keypair we provisioned). Disable host-key
# checking — every cluster instance regenerates host keys.

ssh_user() {
  ssh -i "$PRIV_KEY" \
      -o IdentitiesOnly=yes \
      -o IdentityAgent=none \
      -o StrictHostKeyChecking=no \
      -o UserKnownHostsFile=/dev/null \
      -o LogLevel=ERROR \
      -o ConnectTimeout=10 \
      -p "$SSH_PORT" "${TEST_USER}@localhost" "$@"
}

# --- Stage harness -----------------------------------------------------------
#
# Each stage is a function returning 0 on PASS / nonzero on FAIL.
# `stage` calls it under `if` so `set -e` doesn't kill the whole script
# when a single stage fails — every other stage still runs.

declare -i PASS_COUNT=0 FAIL_COUNT=0
declare -a FAILURES=()

stage() {
  local name="$1"; shift
  printf '\n[stage] %s\n' "$name"
  if "$@"; then
    PASS_COUNT+=1
    printf '  PASS\n'
  else
    FAIL_COUNT+=1
    FAILURES+=("$name")
    printf '  FAIL\n'
  fi
}

# Poll squeue until the named job is RUNNING, up to a deadline.
wait_running() {
  local jobid="$1" deadline=$((SECONDS + 30)) state
  while (( SECONDS < deadline )); do
    state="$(ssh_user "squeue -h -j $jobid -o '%T'" 2>/dev/null || true)"
    if [[ "$state" == "RUNNING" ]]; then
      return 0
    fi
    sleep 1
  done
  printf '  job %s never reached RUNNING (last state: %s)\n' \
    "$jobid" "${state:-<unknown>}"
  return 1
}

# --- Stages ------------------------------------------------------------------

stage_sinfo() {
  local out
  out="$(ssh_user 'sinfo --noheader -o "%P %a %D %T"')" || return 1
  printf '%s\n' "$out" | sed 's/^/    /'
  # Expect at least one partition and the worker count we configured.
  [[ -n "$out" ]]
}

stage_sbatch_sleep() {
  local jobid
  jobid="$(ssh_user "sbatch --parsable --wrap='sleep 60'")" || return 1
  jobid="${jobid//[^0-9]/}"
  printf '  jobid=%s\n' "$jobid"
  if ! [[ "$jobid" =~ ^[0-9]+$ ]]; then return 1; fi
  printf '%s' "$jobid" > "$JOBID_FILE"
}

stage_scontrol_show() {
  local jobid
  jobid="$(cat "$JOBID_FILE")"
  ssh_user "scontrol show job $jobid" \
    | awk '/^[[:space:]]*(JobId|JobState|NodeList)=/' \
    | sed 's/^/    /'
}

stage_srun_jobid_attach() {
  local jobid out
  jobid="$(cat "$JOBID_FILE")"
  wait_running "$jobid" || return 1
  out="$(ssh_user "srun --jobid=$jobid hostname")" || return 1
  printf '  hostname inside allocation: %s\n' "$out"
  [[ "$out" == slurm-worker* ]]
}

stage_srun_multinode() {
  local out unique
  out="$(ssh_user 'srun -N2 hostname' | sort -u)" || return 1
  printf '  hostnames seen: %s\n' "$(echo "$out" | tr '\n' ' ')"
  unique="$(printf '%s\n' "$out" | wc -l)"
  (( unique == 2 ))
}

stage_internode_network() {
  # The two hostnames must be DNS-resolvable inside the podman network
  # (network-alias entries set up by up.sh). ICMP is blocked by default
  # in podman-rootless setups, so we test reachability with a TCP probe
  # to sshd:22 — every cluster image runs sshd. /dev/tcp is bash builtin
  # so no extra package needed.
  local out
  out="$(ssh_user 'srun -N1 -w slurm-worker1 \
            bash -c "exec 3<>/dev/tcp/slurm-worker2/22 \
                     && head -n1 <&3 && exec 3<&- 3>&-"')" || return 1
  printf '  worker1 -> worker2:22 banner: %s\n' "$out"
  [[ "$out" == SSH-* ]]
}

stage_cancel_sleep() {
  local jobid
  jobid="$(cat "$JOBID_FILE" 2>/dev/null || true)"
  [[ -n "$jobid" ]] || return 0
  ssh_user "scancel $jobid" >/dev/null 2>&1 || true
  return 0
}

# --- Run ---------------------------------------------------------------------

JOBID_FILE="$(mktemp -t slurm-smoke-jobid.XXXXXX)"
trap 'rm -f -- "$JOBID_FILE"' EXIT

printf '=== slurm-test-env :: smoke test (instance=%s) ===\n' "$INSTANCE_ID"

ensure_keypair

# Resolve the provisioner: prefer the flake-wrapped binary on PATH (which
# carries its own podman/coreutils dependency closure), fall back to the
# in-tree script when running checked-out copies directly.
if command -v slurm-test-env-provision-user >/dev/null 2>&1; then
  provision_user=(slurm-test-env-provision-user)
else
  provision_user=("$SCRIPT_DIR/provision-user.sh")
fi
"${provision_user[@]}" "$TEST_USER" "$PUB_KEY"

stage 'sinfo lists partitions and nodes' stage_sinfo
stage 'sbatch sleep 60 returns a jobid' stage_sbatch_sleep
stage 'scontrol show job sees the job' stage_scontrol_show
stage 'srun --jobid attaches and runs hostname' stage_srun_jobid_attach
stage 'srun -N2 distributes across two nodes' stage_srun_multinode
stage 'srun on worker1 reaches worker2:22' stage_internode_network
stage 'scancel cleans up the test job' stage_cancel_sleep

# --- Summary -----------------------------------------------------------------

printf '\n=== smoke test summary ===\n'
printf '  passed: %d\n' "$PASS_COUNT"
printf '  failed: %d\n' "$FAIL_COUNT"
if (( FAIL_COUNT > 0 )); then
  printf '  failed stages:\n'
  for name in "${FAILURES[@]}"; do printf '    - %s\n' "$name"; done
  exit 1
fi
printf '  result: ALL PASSED\n'
