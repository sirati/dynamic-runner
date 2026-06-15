#!/usr/bin/env bash
# End-to-end assertion for PhaseSpec.barrier=False (#540).
#
# Single concern: dispatch the 3-phase barrier-false synthetic workload
# against a RUNNING slurm-test-env cluster, then grep the captured
# primary log for the temporal ordering that proves the barrier feature
# works end-to-end (phase_b dispatches mid-phase_a; phase_c waits for
# phase_b drain; no SpawnError::BarrierViolation).
#
# Cluster lifecycle is OUT OF SCOPE — the operator brings the cluster
# up before this test runs (matches the smoke-test.sh contract).
#
# Exit codes (matches the brief-e2e-common-rules.md contract):
#   0  all assertions passed
#   1  at least one assertion failed (or dispatch failed)
#   70 cluster sshd not reachable on $SSH_PORT
#   2  argparse / setup error

set -euo pipefail
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SLURM_TEST_ENV_ENV_FILE:-${SCRIPT_DIR}/../deploy/env.sh}"
# shellcheck disable=SC1090
source "$ENV_FILE"

# --- Resolve the host-side dynamic_runner repo root --------------------------
#
# The driver runs ``python -m tests.e2e.test_consumer_barrier_540`` —
# the framework + consumer code lives in the repo, NOT in the cluster.
# Two resolution paths:
#
#   (1) explicit ``$DYNRUNNER_REPO_ROOT`` env var (orchestrator / CI
#       sets this). Preferred — works from a nix-store-installed
#       wrapper that cannot see the repo via its own path.
#   (2) walk up from the script's directory to a tree carrying
#       ``python/dynamic_runner/__init__.py``. Lets a developer
#       invoke the in-tree script directly without exporting env.
#
# Path-walk fallback is intentional: the brief specifies the script
# lives under ``slurm-test-env/scripts/`` — two parents up is the
# repo. The Nix-store-installed wrapper case is covered by (1).

resolve_repo_root() {
  if [[ -n "${DYNRUNNER_REPO_ROOT:-}" ]]; then
    printf '%s' "$DYNRUNNER_REPO_ROOT"
    return
  fi
  local candidate="${SCRIPT_DIR}/../.."
  candidate="$(cd "$candidate" && pwd)"
  if [[ -f "${candidate}/python/dynamic_runner/__init__.py" ]]; then
    printf '%s' "$candidate"
    return
  fi
  printf 'Could not resolve repo root: set DYNRUNNER_REPO_ROOT.\n' >&2
  return 1
}

REPO_ROOT="$(resolve_repo_root)" || exit 2

# --- Resolve the driver script -----------------------------------------------
#
# Two install layouts:
#
#   (a) flake-installed: the wrapper sits next to the driver in
#       ``$out/bin``, so we can find it by basename via $PATH or
#       sibling-of-self.
#   (b) in-tree: the driver lives next to this script.
#
# Try the sibling first, fall back to PATH lookup.

resolve_driver() {
  local sibling="${SCRIPT_DIR}/test-540-driver.py"
  if [[ -f "$sibling" ]]; then
    printf '%s' "$sibling"
    return
  fi
  if command -v slurm-test-env-test-540-driver >/dev/null 2>&1; then
    printf '%s' "$(command -v slurm-test-env-test-540-driver)"
    return
  fi
  printf 'Could not resolve test-540 driver script.\n' >&2
  return 1
}

DRIVER="$(resolve_driver)" || exit 2

# --- Cluster reachability gate -----------------------------------------------
#
# Match smoke-test.sh's contract: 70 if the cluster isn't running.
# A 2s TCP probe is enough — if sshd takes longer than that to respond,
# the operator has bigger problems than a barrier test.

probe_sshd() {
  exec 3<>/dev/tcp/localhost/"$SSH_PORT" 2>/dev/null
  local rc=$?
  exec 3<&- 3>&- 2>/dev/null || true
  return $rc
}
if ! probe_sshd; then
  printf '[fatal] slurm-test-env cluster sshd not reachable on localhost:%s\n' \
    "$SSH_PORT" >&2
  exit 70
fi

# --- Pick a log destination --------------------------------------------------
#
# Under $STATE_DIR so the artifact survives the script's process tree
# and lands next to the keypair / state the wrapper inspects on
# failure. The driver also captures structured assertion output to
# stdout/stderr — the log file holds the raw dispatch tee.

LOG_FILE="${DYNRUNNER_TEST540_LOG:-${STATE_DIR}/test540/dispatch.log}"
mkdir -p "$(dirname "$LOG_FILE")"

# --- SSH keypair + user provisioning (mirrors smoke-test.sh) -----------------
#
# The dispatcher SSHes into the cluster as ``testuser``. We provision
# the keypair + authorized_keys here (NOT in the driver) for the same
# reason smoke-test.sh does: the provisioner is itself a flake-wrapped
# binary, and shelling it from a Python subprocess would mean carrying
# the same ``nix run`` /  ``$PATH`` resolution logic in two places.
#
# Keypair lives under ``${STATE_DIR}/test540/keys/`` — our own subdir
# so we don't collide with smoke-test.sh's ``${STATE_DIR}/keys/``.

TEST_USER=testuser
KEY_DIR="${STATE_DIR}/test540/keys"
PRIV_KEY="${KEY_DIR}/id_ed25519"
PUB_KEY="${PRIV_KEY}.pub"

if [[ ! -f "$PRIV_KEY" ]]; then
  mkdir -p "$KEY_DIR"
  chmod 0700 "$KEY_DIR"
  ssh-keygen -t ed25519 -N '' -f "$PRIV_KEY" \
    -C "slurm-test-env-test540-${INSTANCE_ID}" >/dev/null
fi

# Same resolver as smoke-test.sh: prefer the flake-wrapped binary on
# PATH (carries its own podman/coreutils closure), fall back to in-tree.
if command -v slurm-test-env-provision-user >/dev/null 2>&1; then
  provision_user=(slurm-test-env-provision-user)
else
  provision_user=("${SCRIPT_DIR}/provision-user.sh")
fi
"${provision_user[@]}" "$TEST_USER" "$PUB_KEY"

# --- Hand off to the driver --------------------------------------------------
#
# Python (from $PATH — the flake wrapping baked it into the closure)
# runs the driver with the env it pulled from env.sh + the resolved
# repo root. Every config knob is an argparse arg on the driver so a
# manual run can override one knob without re-exporting env.

printf '=== slurm-test-env :: test-540 PhaseSpec.barrier=False (instance=%s) ===\n' \
  "$INSTANCE_ID"
printf '  repo root:   %s\n' "$REPO_ROOT"
printf '  driver:      %s\n' "$DRIVER"
printf '  private key: %s\n' "$PRIV_KEY"
printf '  log file:    %s\n' "$LOG_FILE"
printf '\n'

exec python3 "$DRIVER" \
  --repo-root "$REPO_ROOT" \
  --state-dir "$STATE_DIR" \
  --instance-id "$INSTANCE_ID" \
  --ssh-port "$SSH_PORT" \
  --ssh-user "$TEST_USER" \
  --ssh-private-key "$PRIV_KEY" \
  --workers "$WORKER_COUNT" \
  --log-file "$LOG_FILE"
