#!/usr/bin/env bash
# End-to-end test for the #563 four-seam composition contract.
#
# Single concern: drive the existing ``tests/e2e/run_e2e.py`` harness
# against the canonical ``fatal-abort`` scenario, which exercises the
# four #563 seams (run_pipeline broadcasts RunAborted on every fatal
# Err, election arming consults the run-terminal latch,
# bootstrap_tail_dispatch adopts the latch pre-loop, narrator
# suppresses failover/peer-lost narration under the latch). This
# wrapper is the slurm-test-env flake's ``test-563-fatal-abort`` app
# entry — a thin glue layer between ``nix run`` and the Python
# harness. All actual ASSERTIONS live in
# ``tests/e2e/scenarios/fatal_abort.py`` (the modular API every
# scenario exposes); this wrapper owns only the env-loading +
# repo-root resolution + exit-code mapping.
#
# Cluster lifecycle is the orchestrator's concern (``nix run .#up`` /
# ``nix run .#down``). This script asserts the cluster is RUNNING via
# the env file's INSTANCE_ID / SSH_PORT and bails with exit 70 (the
# slurm-test-env contract) if it is not — matching smoke-test.sh's
# pre-check shape.
#
# Exit codes:
#   0  the four-seam contract holds end-to-end
#   1  at least one seam regressed (see harness stderr for the
#      offending failure strings, each names its seam)
#  70  cluster not running (the smoke-test.sh contract)
#   2  argparse / setup error (driven by run_e2e.py)
# 124  driver hit its overall timeout (the pre-#563 wedge re-asserting
#      itself — the dispatcher's observer never read the RunAborted
#      verdict and run_e2e.py timed out)

set -euo pipefail
# Pipefail in inner shells too (the merge-chain pattern from
# MEMORY.md::feedback_pipefail_in_merge_chains).
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ENV_FILE: same env-with-fallback pattern as smoke-test.sh. Under
# ``nix run`` the wrapper sets SLURM_TEST_ENV_ENV_FILE to the staged
# share/ copy; bare ``bash scripts/test-563-fatal-abort.sh`` falls
# back to the in-tree ``deploy/env.sh`` next to this script.
ENV_FILE="${SLURM_TEST_ENV_ENV_FILE:-${SCRIPT_DIR}/../deploy/env.sh}"
# shellcheck disable=SC1090
source "$ENV_FILE"

# --- Repo-root resolution ----------------------------------------------------
#
# The Python harness lives at ``$REPO_ROOT/tests/e2e/run_e2e.py``.
# Resolution order:
#   1. ``DYNRUNNER_REPO_ROOT`` env var — the orchestrator's explicit
#      pin (e.g. when ``nix run`` is invoked from outside the repo).
#   2. ``git rev-parse --show-toplevel`` from $PWD — the natural
#      operator-from-repo-root invocation.
#   3. ``${SCRIPT_DIR}/../..`` — the bare-bash-from-checkout fallback;
#      works when the script runs from the in-tree path
#      (``slurm-test-env/scripts/``) but NOT when wrapped into nix's
#      $out/bin (the wrapper sees ``$out/bin``, NOT the repo).
#
# Validates that ``tests/e2e/run_e2e.py`` exists under the resolved
# root and fails loud if not — saves a Python ``ModuleNotFoundError``
# whose traceback would obscure the real config issue.

resolve_repo_root() {
  if [[ -n "${DYNRUNNER_REPO_ROOT:-}" ]]; then
    printf '%s' "$DYNRUNNER_REPO_ROOT"
    return 0
  fi
  if root="$(git -C "$PWD" rev-parse --show-toplevel 2>/dev/null)"; then
    printf '%s' "$root"
    return 0
  fi
  printf '%s' "$(cd "${SCRIPT_DIR}/../.." && pwd)"
}

REPO_ROOT="$(resolve_repo_root)"
HARNESS="${REPO_ROOT}/tests/e2e/run_e2e.py"

if [[ ! -f "$HARNESS" ]]; then
  printf 'test-563-fatal-abort: cannot find tests/e2e/run_e2e.py under %q\n' \
    "$REPO_ROOT" >&2
  printf '  Set DYNRUNNER_REPO_ROOT to the dynamic_runner repo root,\n' >&2
  printf '  or invoke this script from within the repo via `nix run .#test-563-fatal-abort`.\n' >&2
  exit 2
fi

# --- Cluster-running pre-check ----------------------------------------------
#
# smoke-test.sh delegates this to provision-user.sh's exit-70 contract.
# We replicate the lightest check: a TCP probe to $SSH_PORT on
# localhost — the slurm-test-env gateway publishes ssh there when
# ``up.sh`` succeeds. Using a host-only probe (NOT a podman/container
# lookup) keeps the rule from MEMORY.md::feedback_no_host_wide_kills:
# we never reach into another tenant's container namespace, only our
# own forwarded SSH port.
if ! (exec 3<>"/dev/tcp/localhost/${SSH_PORT}") 2>/dev/null; then
  printf 'test-563-fatal-abort: gateway SSH port %s not reachable; \n' "$SSH_PORT" >&2
  printf '  is the slurm-test-env cluster up? Try `nix run .#up`.\n' >&2
  exit 70
fi
exec 3>&-

# --- Harness invocation ------------------------------------------------------
#
# Forward INSTANCE_ID + SSH_PORT to run_e2e.py through its CLI flags
# (it also reads them from the environment via DEFAULT_INSTANCE_ID /
# DEFAULT_SSH_PORT, but explicit is robust against an env-var typo).
# ``--keep-cluster`` is the default but we name it for legibility:
# this script asserts ONE seam contract and must never tear down the
# shared cluster on success — that's the cluster-lifecycle-is-shared
# rule the brief states explicitly.
#
# ``--workers`` defaults to the cluster's worker count if set in the
# env (slurm-test-env exposes ``WORKERS`` via lib.sh); otherwise
# run_e2e.py's own default applies. We don't override it: this
# scenario's fatal-Err repro is independent of the worker count — one
# secondary is enough to exercise all four seams.

cd "$REPO_ROOT"

# ``python3`` over a bare ``python`` for the explicit-major-version
# guarantee (nix's PATH-wrapped script may or may not bind ``python``
# at all, depending on whether the consumer baked it into the
# packaging). The framework itself is Python-version-pinned via its
# pyproject.toml; we just need a 3.x.
PYTHON="${DYNRUNNER_PYTHON:-python3}"

# Forward any additional argv (e.g. ``--mode in-process`` for fast
# local iteration, ``--keep-tmp`` for post-mortem). Defaults pin the
# SLURM mode that is the brief's explicit ask.
exec "$PYTHON" -m tests.e2e.run_e2e \
  --scenario fatal-abort \
  --mode slurm \
  --instance-id "${INSTANCE_ID}" \
  --ssh-port "${SSH_PORT}" \
  --keep-cluster \
  "$@"
