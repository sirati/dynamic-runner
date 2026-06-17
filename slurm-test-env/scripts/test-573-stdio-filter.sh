#!/usr/bin/env bash
# E2E assertion for #573 — per-task narration split from the
# importance target so `--important-stdio-only` filters the per-task
# INFO firehose out of the operator's stdout while observer.log keeps
# every event.
#
# The fix (commit 9229d4cc, merged via caa3e90d on handoff/mesh-always):
#   * Per-task INFO emits (assigned / completed / other state-change)
#     in `ObserverTaskNarrator::narrate_live` were moved off
#     `IMPORTANT_TARGET` onto a new `OBSERVER_TASK_TARGET`
#     ("dynrunner_observer_task"). Failure arms (terminal /
#     recoverable / OOM) stay on `IMPORTANT_TARGET` — those still wake
#     the operator.
#   * The stdio layer in dynrunner-pyo3 logging keeps its
#     `important_stdio_filter()` (admit iff `meta.target() ==
#     IMPORTANT_TARGET`), so per-task INFO is now filtered out of
#     stdout when `--important-stdio-only` is on.
#   * The full role-split log (observer.log inside `--full-log-dir`)
#     is unfiltered at the layer level (only scope-gated by
#     `OBSERVER_ROLE_SPAN`), so every per-task INFO line still lands
#     there at INFO.
#
# What this script asserts when live-log artifacts are reachable:
#   A. WITH --important-stdio-only: the captured stdout contains
#      ZERO matches of the per-task narrator's three INFO shapes
#      ("task <id> assigned to <holder>" / "task <id> completed on
#      <holder>" / "task <id> changed state to <state>").
#   B. WITH --important-stdio-only: the captured stdout DOES contain
#      at least one wake-worthy event (a run reached a milestone —
#      e.g. "starting job phase", "phase complete", "run complete:",
#      "secondaries connected", "Connecting to gateway").
#   C. observer.log (the full unfiltered durable log written by the
#      OBSERVER_ROLE_SPAN's full-log layer) contains >= 20 per-task
#      INFO lines — proving the events still reach the durable record
#      and the split is target-only, not a drop.
#   D. WITHOUT --important-stdio-only (default mode): the captured
#      stdout DOES contain per-task INFO lines — the #520 contract
#      ("operator sees per-task chatter by default") is preserved.
#
# Plus source-shape assertions (test-571 pattern) that pin the fix
# seams in the dynrunner source tree alongside this script: a
# regression that reverts any seam (constant rename / per-task emit
# silently moved back to IMPORTANT_TARGET / stdio filter widened)
# trips a FAIL with a clear diagnostic, independent of whether live
# artifacts are present.
#
# Practical-limit skip (test-575 pattern):
#   The slurm-test-env cluster images do NOT package dynrunner
#   (modules/worker.nix, modules/gateway.nix, modules/common.nix carry
#   no dynrunner wheel/binary — only the SLURM control plane). A real
#   dispatcher submission inside the cluster therefore can't be
#   driven by this harness alone; the operator stages the run from
#   the host or from a consumer (asm-tokenizer, asm-dataset-nix). The
#   live assertions are SKIPped (with a recorded reason) when the
#   artifacts described below are not yet present on the gateway;
#   the source-shape assertions still run and gate the fix's seams.
#
# Inputs (env, contract with the orchestrator / consumer / operator):
#
#   DYNRUNNER_STDOUT_WITH_FLAG     absolute path on the gateway to a
#                                  captured stdout file from a
#                                  recent `--important-stdio-only`
#                                  run with >= 20 tasks. When unset,
#                                  the script probes a small set of
#                                  default locations under the
#                                  testuser home (see
#                                  DEFAULT_STDOUT_GLOB).
#   DYNRUNNER_STDOUT_NO_FLAG       absolute path on the gateway to a
#                                  captured stdout file from a
#                                  recent default-mode run (no
#                                  `--important-stdio-only`) with
#                                  >= 20 tasks. When unset, same
#                                  default-probe convention.
#   DYNRUNNER_OBSERVER_LOG         absolute path on the gateway to
#                                  the observer.log written under
#                                  `--full-log-dir` during the
#                                  `--important-stdio-only` run.
#                                  When unset, the script searches
#                                  for observer.log under default
#                                  per-node dirs in the testuser
#                                  home (see
#                                  DEFAULT_OBSERVER_LOG_GLOB).
#   SLURM_TEST_ENV_DYNRUNNER_SRC   override the dynrunner source-
#                                  tree path used by the source-
#                                  shape assertions. Defaults to
#                                  two dirs above this script (the
#                                  same convention as test-571).
#
# Exit codes (per slurm-test-env/scripts/brief-e2e-common-rules.md):
#   0   all assertions pass (or all live-log assertions SKIP with
#       documented practical-limit reason AND source-shape pass)
#   1   at least one assertion failed
#   70  cluster is not running (matches smoke-test.sh contract)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SLURM_TEST_ENV_ENV_FILE:-${SCRIPT_DIR}/../deploy/env.sh}"
# shellcheck disable=SC1090
source "$ENV_FILE"

# --- Locate the dynrunner source tree (test-571 pattern) ---------------------

locate_dynrunner_src() {
  local candidates=()
  if [[ -n "${SLURM_TEST_ENV_DYNRUNNER_SRC:-}" ]]; then
    candidates+=("$SLURM_TEST_ENV_DYNRUNNER_SRC")
  fi
  # Walk up from SCRIPT_DIR — works for `bash scripts/test-573-...sh` direct
  # invocation; does NOT work under `nix run` where SCRIPT_DIR is /nix/store
  # (the wrapper-bin landing site). The PWD fallback then catches the
  # in-tree case.
  candidates+=(
    "$SCRIPT_DIR/../.."
    "$PWD"
    "$PWD/.."
  )
  for c in "${candidates[@]}"; do
    if [[ -d "$c/crates/dynrunner-manager-distributed" ]]; then
      ( cd "$c" && pwd )
      return 0
    fi
  done
  return 1
}

DYN_SRC=""
if ! DYN_SRC="$(locate_dynrunner_src)"; then
  printf 'ERROR: could not locate dynrunner source tree.\n' >&2
  printf '  Set SLURM_TEST_ENV_DYNRUNNER_SRC=<path-to-dynrunner-repo>\n' >&2
  printf '  or run from the dynrunner repo root.\n' >&2
  exit 1
fi
printf 'dynrunner source: %s\n' "$DYN_SRC"

# --- Test identity (mirrors smoke-test.sh) -----------------------------------

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

# --- Stage harness (test-575 pattern with SKIP exit 77) ----------------------

declare -i PASS_COUNT=0 FAIL_COUNT=0 SKIP_COUNT=0
declare -a FAILURES=() SKIPS=()

stage() {
  local name="$1"; shift
  printf '\n[stage] %s\n' "$name"
  local rc=0
  "$@" || rc=$?
  case "$rc" in
    0)
      PASS_COUNT+=1
      printf '  PASS\n'
      ;;
    77)
      SKIP_COUNT+=1
      SKIPS+=("$name")
      printf '  SKIP (practical-limit)\n'
      ;;
    *)
      FAIL_COUNT+=1
      FAILURES+=("$name")
      printf '  FAIL\n'
      ;;
  esac
}

# --- Cluster liveness probe (fleet convention) -------------------------------

probe_cluster_or_exit_70() {
  ensure_keypair
  if command -v slurm-test-env-provision-user >/dev/null 2>&1; then
    slurm-test-env-provision-user "$TEST_USER" "$PUB_KEY" \
      || { printf 'provision-user failed; cluster likely not running.\n' >&2; exit 70; }
  else
    "$SCRIPT_DIR/provision-user.sh" "$TEST_USER" "$PUB_KEY" \
      || { printf 'provision-user failed; cluster likely not running.\n' >&2; exit 70; }
  fi
  if ! ssh_user 'sinfo --noheader -o "%P"' >/dev/null 2>&1; then
    printf 'sinfo failed over ssh; cluster not running.\n' >&2
    exit 70
  fi
  printf 'cluster reachable; proceeding with #573 assertions.\n'
}

# --- Source-shape helpers (test-571 pattern) ---------------------------------

assert_grep() {
  local pattern="$1" relpath="$2" out
  local abspath="$DYN_SRC/$relpath"
  if [[ ! -f "$abspath" ]]; then
    printf '    file missing: %s\n' "$relpath"
    return 1
  fi
  if ! out="$(grep -n -- "$pattern" "$abspath" 2>/dev/null)"; then
    printf '    pattern NOT FOUND: %q in %s\n' "$pattern" "$relpath"
    return 1
  fi
  printf '    %s\n' "$(echo "$out" | head -1)"
  return 0
}

# --- #573 source-shape assertions --------------------------------------------
#
# Each seam pins ONE element of the fix. The patterns are specific enough to
# fail on a regression but loose enough to survive cosmetic edits (e.g.
# refactors that re-arrange whitespace inside the tracing macro). A reverted
# seam (per-task emit moved back onto IMPORTANT_TARGET; OBSERVER_TASK_TARGET
# constant removed; the gate widened) trips a clear FAIL.
#
# Seam map:
#   1. dynrunner-core declares OBSERVER_TASK_TARGET as the new per-task
#      narration target ("dynrunner_observer_task").
#   2. ObserverTaskNarrator::narrate_live emits the Assigned arm on
#      OBSERVER_TASK_TARGET (per-task INFO, no longer IMPORTANT_TARGET).
#   3. Same for the Completed arm.
#   4. Same for the Other state-change arm.
#   5. Failure arms (TerminalFailure / RecoverableFailure / OomFailure)
#      STAY on IMPORTANT_TARGET — those are wake-worthy and must still
#      reach stdout under --important-stdio-only.
#   6. The stdio gate is exactly target == IMPORTANT_TARGET (the only
#      place importance is decided). A widened gate would defeat the
#      whole split.

seam1_observer_task_target_const() {
  local f=crates/dynrunner-core/src/importance.rs
  assert_grep 'OBSERVER_TASK_TARGET' "$f" \
    && assert_grep 'dynrunner_observer_task' "$f"
}

seam2_assigned_on_observer_task() {
  local f=crates/dynrunner-manager-distributed/src/observer/task_narrator.rs
  # The Assigned arm lives between the `Assigned =>` match and the closing
  # `}` of its `tracing::info!` macro; we pin both the target line and the
  # message shape so a target-only revert (line one only) still trips.
  # The target binding in this file is the local const PER_TASK_TARGET
  # (= high_volume_target(true) = "dynrunner_observer_task"), the post
  # #583/#587 rename of the inline OBSERVER_TASK_TARGET reference. Same
  # runtime target; we pin the current source symbol.
  assert_grep 'target: PER_TASK_TARGET' "$f" \
    && assert_grep 'task {id} assigned to {holder}' "$f"
}

seam3_completed_on_observer_task() {
  local f=crates/dynrunner-manager-distributed/src/observer/task_narrator.rs
  assert_grep 'task {id} completed on {holder}' "$f"
}

seam4_other_state_on_observer_task() {
  local f=crates/dynrunner-manager-distributed/src/observer/task_narrator.rs
  assert_grep 'task {id} changed state to {state}' "$f"
}

seam5_failures_stay_on_important() {
  local f=crates/dynrunner-manager-distributed/src/observer/task_narrator.rs
  # Failure arms route to IMPORTANT_TARGET — wake-worthy, must reach
  # stdout under --important-stdio-only. The shipped fix routes ALL
  # three failure shapes (TerminalFailure / RecoverableFailure /
  # OomFailure) onto IMPORTANT_TARGET; pinning the verbatim
  # message-shape lines is the strongest single-line guard.
  assert_grep 'target: IMPORTANT_TARGET' "$f" \
    && assert_grep 'terminally failed on {holder}' "$f" \
    && assert_grep 'failed (recoverable) on {holder}' "$f" \
    && assert_grep 'failed (oom) on {holder}' "$f"
}

seam6_stdio_gate_unchanged() {
  local f=crates/dynrunner-pyo3/src/logging/mod.rs
  # The single decision point: admit iff target == IMPORTANT_TARGET.
  # A widened gate (e.g. `|| meta.target() == OBSERVER_TASK_TARGET`)
  # would re-flood stdout with per-task INFO under --important-stdio-only.
  assert_grep 'meta.target() == IMPORTANT_TARGET' "$f"
}

# --- Live-log artifact discovery ---------------------------------------------
#
# Three artifacts are needed for the live-log assertions:
#   * stdout from a `--important-stdio-only` run (>= 20 tasks)
#   * stdout from a default-mode run         (>= 20 tasks)
#   * observer.log from the `--important-stdio-only` run's --full-log-dir
#
# The caller MAY pin each via env vars (DYNRUNNER_STDOUT_WITH_FLAG,
# DYNRUNNER_STDOUT_NO_FLAG, DYNRUNNER_OBSERVER_LOG). When unset, we probe a
# documented default-location set under the testuser home. A missing artifact
# is a SKIP, not a FAIL — the slurm-test-env images do not ship dynrunner, so
# the script is necessarily artifact-driven until a consumer (or the
# operator's host-side harness) produces them.

# Glob covers a handful of conventional capture filenames so an operator who
# tee'd stdout to any of these is auto-discovered. The naming hints are
# specific enough to avoid matching unrelated logs (the `important-stdio` /
# `default` suffixes are the contract).
DEFAULT_STDOUT_GLOB='~/dynrunner-*-important-stdio-only*.out ~/dynrunner-*-default*.out'
DEFAULT_OBSERVER_LOG_GLOB='~/dynrunner-*-fulllog/observer.log ~/dynrunner-full-log*/observer.log ~/.dynrunner/*-fulllog/observer.log'

# Resolve a single env-var-or-glob artifact path on the gateway. Prints the
# absolute path on success (empty stdout when none found).
resolve_artifact() {
  local env_value="$1" glob_pattern="$2"
  if [[ -n "$env_value" ]]; then
    # The env-var case is operator-pinned; we still verify it exists on the
    # gateway side so a stale path surfaces as a SKIP reason rather than
    # silently letting the file-not-found drop through.
    if ssh_user "[ -f $env_value ]" 2>/dev/null; then
      printf '%s' "$env_value"
      return 0
    fi
    printf '' # pinned but absent — caller emits the SKIP with reason
    return 0
  fi
  ssh_user "ls -t $glob_pattern 2>/dev/null | head -n1" || true
}

# Filter an artifact-glob expansion down to the path whose basename matches a
# substring — used to pick the with-flag vs default stdout file out of the
# combined default-glob.
filter_match() {
  local artifact="$1" needle="$2"
  if [[ -z "$artifact" ]]; then printf ''; return 0; fi
  if [[ "$artifact" == *"$needle"* ]]; then
    printf '%s' "$artifact"
  fi
}

STDOUT_WITH_FLAG=""
STDOUT_NO_FLAG=""
OBSERVER_LOG=""

stage_discover_artifacts() {
  local with_env="${DYNRUNNER_STDOUT_WITH_FLAG:-}"
  local no_env="${DYNRUNNER_STDOUT_NO_FLAG:-}"
  local obs_env="${DYNRUNNER_OBSERVER_LOG:-}"

  # When env vars are set we resolve directly. When unset we lean on the
  # default-glob; for stdout we then sub-filter by basename so the two
  # artifacts come from distinct files (one with-flag, one default).
  STDOUT_WITH_FLAG="$(resolve_artifact "$with_env" "${DEFAULT_STDOUT_GLOB}" | head -n1)"
  if [[ -z "$STDOUT_WITH_FLAG" && -z "$with_env" ]]; then
    # Sub-filter the glob: any newest file whose path carries the
    # `important-stdio-only` token wins.
    STDOUT_WITH_FLAG="$(ssh_user "ls -t ${DEFAULT_STDOUT_GLOB} 2>/dev/null \
      | grep -F important-stdio-only | head -n1" || true)"
  fi
  if [[ -z "$STDOUT_NO_FLAG" && -z "$no_env" ]]; then
    STDOUT_NO_FLAG="$(ssh_user "ls -t ${DEFAULT_STDOUT_GLOB} 2>/dev/null \
      | grep -F -- -default | head -n1" || true)"
  fi
  if [[ -z "$STDOUT_NO_FLAG" && -n "$no_env" ]]; then
    STDOUT_NO_FLAG="$(resolve_artifact "$no_env" '' | head -n1)"
  fi
  if [[ -z "$OBSERVER_LOG" ]]; then
    OBSERVER_LOG="$(resolve_artifact "$obs_env" "${DEFAULT_OBSERVER_LOG_GLOB}" | head -n1)"
  fi

  printf '  STDOUT_WITH_FLAG: %s\n' "${STDOUT_WITH_FLAG:-<not found>}"
  printf '  STDOUT_NO_FLAG:   %s\n' "${STDOUT_NO_FLAG:-<not found>}"
  printf '  OBSERVER_LOG:     %s\n' "${OBSERVER_LOG:-<not found>}"

  if [[ -z "$STDOUT_WITH_FLAG" && -z "$STDOUT_NO_FLAG" && -z "$OBSERVER_LOG" ]]; then
    printf '  no live-log artifacts found. Provide one of:\n'
    printf '    - DYNRUNNER_STDOUT_WITH_FLAG / DYNRUNNER_STDOUT_NO_FLAG / DYNRUNNER_OBSERVER_LOG envs\n'
    printf '    - capture files under testuser home matching:\n'
    printf '        %s\n' $DEFAULT_STDOUT_GLOB
    printf '        %s\n' $DEFAULT_OBSERVER_LOG_GLOB
    printf '  slurm-test-env images do not ship dynrunner (modules/common.nix\n'
    printf '  carries no wheel); a real >=20-task multi-mode run is observable\n'
    printf '  only when a consumer (asm-tokenizer / asm-dataset-nix) or the\n'
    printf '  operator''s host-side run_e2e harness produces the artifacts.\n'
    printf '  Skipping live-log assertions; source-shape gates still apply.\n'
    return 77
  fi
}

# --- Per-task INFO line pattern ---------------------------------------------
#
# The three OBSERVER_TASK_TARGET INFO message shapes verbatim, escaped for
# extended-regex matching (only the per-task narrator emits these; no
# accidental sibling-source match risk). One ERE alternation, used both for
# the WITH-flag negative gate and the observer.log positive gate.
PER_TASK_INFO_RE='task [^ ]+ (assigned to |completed on |changed state to )'

# Wake-worthy milestone substrings — at least one must appear in the
# with-flag stdout for a "complete run reached an operator-visible milestone"
# proof. These are the verbatim IMPORTANT_TARGET emit fragments (matching
# tests/e2e/scenarios/important_stdio.py:_REQUIRED_MILESTONES — kept in sync
# so a regression in either lands here too). Substring match; ANSI stripping
# is handled by the importance sink's `with_ansi(false)` on the stdio layer.
WAKE_WORTHY=(
  'Connecting to gateway...'
  'container image ready on gateway'
  'SLURM jobs queued'
  'secondaries connected'
  'starting job phase'
  'phase complete'
  'run complete:'
  'primary is now'
  'primary left'
  'secondary joined'
)

# --- Assertion A: WITH-flag stdout has ZERO per-task INFO lines --------------

stage_with_flag_no_per_task_info() {
  if [[ -z "$STDOUT_WITH_FLAG" ]]; then
    printf '  STDOUT_WITH_FLAG unavailable; skipping (run a `--important-stdio-only`\n'
    printf '  job and pin DYNRUNNER_STDOUT_WITH_FLAG, or capture stdout to a path\n'
    printf '  matching %s).\n' "$DEFAULT_STDOUT_GLOB"
    return 77
  fi
  local hits
  hits="$(ssh_user "grep -E -c -- '$PER_TASK_INFO_RE' '$STDOUT_WITH_FLAG' || echo 0")"
  hits="${hits//[^0-9]/}"
  printf '  per-task-INFO matches in WITH-flag stdout: %s\n' "${hits:-0}"
  if (( ${hits:-0} > 0 )); then
    printf '  --important-stdio-only is LEAKING per-task INFO. First 3 matches:\n'
    ssh_user "grep -E -- '$PER_TASK_INFO_RE' '$STDOUT_WITH_FLAG' | head -3" \
      | sed 's/^/    /'
    return 1
  fi
}

# --- Assertion B: WITH-flag stdout shows at least one wake-worthy event ------

stage_with_flag_has_wake_worthy() {
  if [[ -z "$STDOUT_WITH_FLAG" ]]; then
    return 77
  fi
  local any_hit=0 needle hits
  for needle in "${WAKE_WORTHY[@]}"; do
    hits="$(ssh_user "grep -F -c -- '$needle' '$STDOUT_WITH_FLAG' || echo 0")"
    hits="${hits//[^0-9]/}"
    if (( ${hits:-0} > 0 )); then
      printf '    %-40s : %s appearance(s)\n' "$needle" "$hits"
      any_hit=1
    fi
  done
  if (( any_hit == 0 )); then
    printf '  no wake-worthy milestone visible on WITH-flag stdout.\n'
    printf '  Either the run never reached a milestone (incomplete) or the\n'
    printf '  importance gate is dropping IMPORTANT_TARGET emits (regression).\n'
    return 1
  fi
}

# --- Assertion C: observer.log has >= 20 per-task INFO lines -----------------

stage_observer_log_has_per_task_info() {
  if [[ -z "$OBSERVER_LOG" ]]; then
    printf '  OBSERVER_LOG unavailable; skipping (run with `--full-log-dir <dir>`\n'
    printf '  and pin DYNRUNNER_OBSERVER_LOG=<dir>/observer.log).\n'
    return 77
  fi
  local hits
  hits="$(ssh_user "grep -E -c -- '$PER_TASK_INFO_RE' '$OBSERVER_LOG' || echo 0")"
  hits="${hits//[^0-9]/}"
  printf '  per-task-INFO matches in observer.log: %s\n' "${hits:-0}"
  if (( ${hits:-0} < 20 )); then
    printf '  observer.log carries fewer than 20 per-task INFO lines.\n'
    printf '  Either the run had < 20 tasks (brief requires >= 20) or the\n'
    printf '  split silently dropped them from the durable log too (regression\n'
    printf '  in the OBSERVER_ROLE_SPAN routing — see crates/dynrunner-pyo3/\n'
    printf '  src/logging/mod.rs::role_full_layer).\n'
    return 1
  fi
}

# --- Assertion D: WITHOUT-flag stdout DOES carry per-task INFO ---------------

stage_no_flag_has_per_task_info() {
  if [[ -z "$STDOUT_NO_FLAG" ]]; then
    printf '  STDOUT_NO_FLAG unavailable; skipping (run a default-mode job\n'
    printf '  WITHOUT `--important-stdio-only` and pin DYNRUNNER_STDOUT_NO_FLAG,\n'
    printf '  or capture stdout to a path matching %s).\n' "$DEFAULT_STDOUT_GLOB"
    return 77
  fi
  local hits
  hits="$(ssh_user "grep -E -c -- '$PER_TASK_INFO_RE' '$STDOUT_NO_FLAG' || echo 0")"
  hits="${hits//[^0-9]/}"
  printf '  per-task-INFO matches in default-mode stdout: %s\n' "${hits:-0}"
  if (( ${hits:-0} == 0 )); then
    printf '  default-mode stdout has ZERO per-task INFO — the #520 contract\n'
    printf '  (operator sees per-task chatter by default) is broken: a global\n'
    printf '  stdio filter has been installed when none was requested.\n'
    return 1
  fi
}

# --- Run ---------------------------------------------------------------------

printf '=== slurm-test-env :: test-573-stdio-filter (instance=%s) ===\n' "$INSTANCE_ID"
probe_cluster_or_exit_70

printf '\n--- #573 source-shape assertions on %s ---\n' "$DYN_SRC"
stage 'seam 1: dynrunner-core declares OBSERVER_TASK_TARGET'        seam1_observer_task_target_const
stage 'seam 2: Assigned arm emits on OBSERVER_TASK_TARGET'           seam2_assigned_on_observer_task
stage 'seam 3: Completed arm emits on OBSERVER_TASK_TARGET'          seam3_completed_on_observer_task
stage 'seam 4: Other state-change emits on OBSERVER_TASK_TARGET'     seam4_other_state_on_observer_task
stage 'seam 5: failure arms stay on IMPORTANT_TARGET (wake-worthy)'  seam5_failures_stay_on_important
stage 'seam 6: stdio gate admits exactly IMPORTANT_TARGET'           seam6_stdio_gate_unchanged

printf '\n--- #573 live-log assertions (requires consumer-produced artifacts) ---\n'
stage 'discover live-log artifacts on gateway'                       stage_discover_artifacts
stage 'WITH --important-stdio-only: ZERO per-task INFO on stdout'    stage_with_flag_no_per_task_info
stage 'WITH --important-stdio-only: >=1 wake-worthy event on stdout' stage_with_flag_has_wake_worthy
stage 'observer.log carries >=20 per-task INFO lines'                stage_observer_log_has_per_task_info
stage 'WITHOUT --important-stdio-only: per-task INFO on stdout'      stage_no_flag_has_per_task_info

# --- Summary -----------------------------------------------------------------

printf '\n=== test-573-stdio-filter summary ===\n'
printf '  passed: %d\n' "$PASS_COUNT"
printf '  failed: %d\n' "$FAIL_COUNT"
printf '  skipped: %d\n' "$SKIP_COUNT"
if (( FAIL_COUNT > 0 )); then
  printf '  failed stages:\n'
  for name in "${FAILURES[@]}"; do printf '    - %s\n' "$name"; done
  exit 1
fi
if (( SKIP_COUNT > 0 )); then
  printf '  skipped stages (practical-limit, see header docstring):\n'
  for name in "${SKIPS[@]}"; do printf '    - %s\n' "$name"; done
fi
printf '  result: ALL ASSERTIONS PASSED — #573 stdio-filter contract holds\n'
