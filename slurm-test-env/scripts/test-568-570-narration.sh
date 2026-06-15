#!/usr/bin/env bash
# End-to-end test for the #568/#570 narration arms.
#
# Single concern: drive the three feasible narration arms against a
# RUNNING slurm-test-env cluster and assert the verbatim emit strings
# reach the operator's --important-stdio-only stream. The actual
# assertions live in the per-arm e2e scenarios under
# ``tests/e2e/scenarios/narration_*.py``; this script just orchestrates
# the per-scenario dispatch via ``run_e2e.py`` and aggregates results.
#
# Arms covered (all SLURM-mode, --important-stdio-only gated):
#   #568 DiscoveryDebt Owed/Settled   → narration-discovery-debt
#   #568 CustomMessagePosted          → narration-custom-message-handled
#                                       (Posted always precedes Handled,
#                                        so the Handled scenario asserts
#                                        BOTH lines from a single run)
#   #570 CustomMessage Handled        → narration-custom-message-handled
#   #570 CustomMessage Failed         → narration-custom-message-failed
#
# Arm DEFERRED (documented at exit, not asserted):
#   #568 WindDownRequested WARN — requires a replacement-secondary
#   respawn-then-rescind sequence that the slurm-test-env cluster has no
#   single-flag CLI for. The state-mutation
#   (``ClusterMutation::WindDownRequested``) IS unit-tested at the
#   crate level (``run_narrator.rs::wind_down_requested_narrates_once_per_pair``),
#   so the emit shape itself is covered; what is NOT covered here is the
#   end-to-end "real cluster fires the mutation" path. A future addition
#   would either (a) drive a slurm-authoritative reversibility scenario
#   that the framework's election layer produces a WindDownRequested on,
#   or (b) add a debug-only ``--inject-wind-down-requested`` knob the
#   scenario can flip. Both are framework changes — out of scope for an
#   e2e test branch.
#
# Cluster lifecycle is SHARED: this script takes the cluster as given
# (running, provisioned). If the cluster is not running we exit 70 —
# matches ``smoke-test.sh``'s contract so a parallel orchestrator can
# notice and bring it up.
#
# Exit codes:
#   0  all FEASIBLE narration arms passed
#   1  at least one feasible arm failed
#   70 cluster not running (provision-user.sh's contract)

set -euo pipefail
set -o pipefail  # belt-and-braces: any pipeline failure must surface

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SLURM_TEST_ENV_ENV_FILE:-${SCRIPT_DIR}/../deploy/env.sh}"
# shellcheck disable=SC1090
source "$ENV_FILE"

# --- repo / driver layout ----------------------------------------------------
#
# The script lives under ``slurm-test-env/scripts/`` inside the canonical
# dynamic_runner checkout. The e2e driver (``tests/e2e/run_e2e.py``) is two
# directories up. ``DYNRUNNER_REPO_ROOT`` is the explicit override for
# operators running the wheel-installed wrapper from elsewhere — when set,
# we use it verbatim; otherwise we walk up from this script's location.

REPO_ROOT="${DYNRUNNER_REPO_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
DRIVER="${REPO_ROOT}/tests/e2e/run_e2e.py"

if [[ ! -f "$DRIVER" ]]; then
  cat >&2 <<EOF
[test-568-570-narration] ERROR: e2e driver not found at:
  $DRIVER

Either run this script from a dynamic_runner repo checkout (default), or
export DYNRUNNER_REPO_ROOT to point at a checkout containing tests/e2e/.
EOF
  exit 2
fi

# --- cluster running check ---------------------------------------------------
#
# Match smoke-test.sh's exit-70 contract: if the gateway's SSH port is not
# accepting connections, the cluster is not up. We use the same ``bash
# /dev/tcp`` probe the e2e driver's ``_cluster.is_cluster_running`` does
# (avoids requiring nc/ncat in the slurm-test-env image's path).

cluster_alive() {
  exec 3<>"/dev/tcp/127.0.0.1/${SSH_PORT}" 2>/dev/null && \
    { exec 3<&- 3>&-; return 0; }
  return 1
}

if ! cluster_alive; then
  printf '[test-568-570-narration] cluster not running on ssh port %s — exit 70\n' \
    "$SSH_PORT" >&2
  exit 70
fi

# --- per-arm runner ----------------------------------------------------------
#
# Each arm is one ``run_e2e.py --scenario <name>`` invocation. The driver
# handles per-scenario tmpdir, per-plan dispatcher subprocess, log
# capture, and exit code: 0 on assertions pass, 1 on failure, 124 on
# timeout. We collect each arm's result and report a summary.

declare -i ARMS_PASS=0 ARMS_FAIL=0
declare -a ARM_FAILURES=()

run_arm() {
  local name="$1"
  printf '\n[arm] %s\n' "$name"
  if python3 "$DRIVER" \
      --scenario "$name" \
      --mode slurm \
      --instance-id "$INSTANCE_ID" \
      --ssh-port "$SSH_PORT"; then
    ARMS_PASS+=1
    printf '  PASS: %s\n' "$name"
  else
    ARMS_FAIL+=1
    ARM_FAILURES+=("$name")
    printf '  FAIL: %s\n' "$name"
  fi
}

printf '=== slurm-test-env :: #568/#570 narration arms (instance=%s) ===\n' \
  "$INSTANCE_ID"

# DiscoveryDebt Owed + Settled (#568) — drives mode-2 (--source-already-
# staged) under --important-stdio-only and greps both INFO lines.
run_arm 'narration-discovery-debt'

# CustomMessagePosted (#568) + CustomMessageOutcome Handled (#570) —
# drives a worker that posts a custom message handled cleanly by the
# primary; asserts BOTH the Posted INFO and the Handled INFO emits on
# the operator stream (Posted always precedes Handled at the emit site,
# so a single run covers both arms).
run_arm 'narration-custom-message-handled'

# CustomMessageOutcome Failed (#570) — drives the same wire path but
# with a primary handler that raises; asserts the FAILED-header AND
# the verbatim exception reason ride through to the operator stream.
run_arm 'narration-custom-message-failed'

# --- Summary -----------------------------------------------------------------

printf '\n=== narration arms summary ===\n'
printf '  passed: %d\n' "$ARMS_PASS"
printf '  failed: %d\n' "$ARMS_FAIL"
if (( ARMS_FAIL > 0 )); then
  printf '  failed arms:\n'
  for name in "${ARM_FAILURES[@]}"; do printf '    - %s\n' "$name"; done
fi

cat <<'EOF'

  DEFERRED (not asserted by this script):
    - #568 WindDownRequested WARN per (secondary, member_gen):
      requires a replacement-secondary respawn-then-rescind sequence
      the slurm-test-env cluster has no single-flag CLI to drive. The
      emit shape itself is unit-tested at the crate level
      (run_narrator.rs::wind_down_requested_narrates_once_per_pair); the
      end-to-end "real cluster fires the mutation" path is not covered.

EOF

if (( ARMS_FAIL > 0 )); then
  exit 1
fi
printf '  result: ALL FEASIBLE ARMS PASSED\n'
