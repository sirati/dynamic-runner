#!/usr/bin/env bash
# E2E assertion: #543 respawn-never-scancels invariant.
#
# Single concern: prove that when a secondary dies mid-run, the primary's
# respawn path is ADDITIVE-ONLY — it submits a fresh SLURM job (new
# job_id) under the same consumer-prefix naming, and NEVER scancels the
# dead slot. The dead slot is left to SLURM's job-timeout / natural exit;
# the primary issues zero scancel calls for the failed secondary.
#
# Inputs (env, contract with the orchestrator)
# --------------------------------------------
#   DYNRUNNER_DISPATCH_CMD   shell command (run as testuser on the
#                            gateway) that launches a multi-secondary
#                            dynamic_runner SLURM run in the background;
#                            must spawn at least 2 secondaries and run
#                            long enough for kill + respawn observation
#                            (target: >= 90s of in-flight work).
#   DYNRUNNER_PRIMARY_LOG    absolute path on the gateway to primary.log
#                            written by the dispatch.
#   DYNRUNNER_JOB_PREFIX     consumer job-name prefix passed to
#                            dynamic_runner (e.g. "asm-2026"); the script
#                            asserts respawned jobs preserve this prefix.
#
# Exit codes (per slurm-test-env e2e contract)
#   0   all assertions pass
#   1   at least one assertion failed
#   2   prerequisite env var unset (script cannot run)
#   70  cluster is not running
#
# This script intentionally does NOT bring the cluster up/down — it
# consumes a running cluster, the same lifecycle contract as
# smoke-test.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SLURM_TEST_ENV_ENV_FILE:-${SCRIPT_DIR}/../deploy/env.sh}"
# shellcheck disable=SC1090
source "$ENV_FILE"

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

# --- Stage harness (same shape as smoke-test.sh) -----------------------------

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

# --- Prereq check ------------------------------------------------------------

require_env() {
  local var="$1"
  if [[ -z "${!var:-}" ]]; then
    printf 'error: env var %s is required (see header docstring)\n' "$var" >&2
    exit 2
  fi
}

# --- Snapshots & helpers -----------------------------------------------------
#
# squeue snapshot file format (one job per line): "<job_id> <name>".
# Keeps the parsing trivial and immune to squeue column-width changes
# (we set the format string ourselves with %i %j).

snapshot_jobs() {
  local out="$1"
  ssh_user "squeue --me --noheader -o '%i %j'" > "$out"
}

# Extract just the job_id column from a snapshot.
ids_from() {
  awk '{print $1}' "$1" | sort -u
}

# Extract the job name for a given job id.
name_for() {
  local snapshot="$1" jobid="$2"
  awk -v id="$jobid" '$1==id {print $2}' "$snapshot"
}

# Poll until the gateway-visible secondary count reaches the expected
# floor — or fail after a deadline. Used to wait for steady-state before
# capturing the initial snapshot.
wait_for_secondary_count() {
  local floor="$1" deadline=$((SECONDS + 120)) count
  while (( SECONDS < deadline )); do
    count=$(ssh_user "squeue --me --noheader -o '%i'" | wc -l)
    if (( count >= floor )); then
      return 0
    fi
    sleep 2
  done
  printf '  never reached >=%d secondary jobs (last=%d)\n' "$floor" "${count:-0}"
  return 1
}

# --- Stages ------------------------------------------------------------------

# 1. Launch the dispatch in the background on the gateway. The caller's
#    command is responsible for spawning the multi-secondary run and
#    must write primary.log to $DYNRUNNER_PRIMARY_LOG.
stage_launch_dispatch() {
  ssh_user "nohup bash -c '$DYNRUNNER_DISPATCH_CMD' \
            >/tmp/dynrunner-test543-dispatch.out \
            2>/tmp/dynrunner-test543-dispatch.err </dev/null &" \
    || return 1
  printf '  dispatched (cmd=%q)\n' "$DYNRUNNER_DISPATCH_CMD"
}

# 2. Wait for the framework to submit its secondaries and record the
#    initial job_id set.
stage_capture_initial() {
  wait_for_secondary_count 2 || return 1
  snapshot_jobs "$INITIAL_SNAPSHOT"
  local initial_count
  initial_count=$(wc -l < "$INITIAL_SNAPSHOT")
  printf '  initial secondaries: %d\n' "$initial_count"
  cat "$INITIAL_SNAPSHOT" | sed 's/^/    /'
  (( initial_count >= 2 ))
}

# 3. Pick one of the initial secondaries and kill its process tree on
#    the worker node. We SIGKILL the secondary process (not scancel) so
#    the kill bypasses SLURM's job-state machinery and forces the
#    framework to notice the death via mesh/heartbeat — which is the
#    exact path that respawn-never-scancels gates.
stage_kill_one_secondary() {
  local victim_id victim_name node
  victim_id=$(awk 'NR==1{print $1}' "$INITIAL_SNAPSHOT")
  victim_name=$(name_for "$INITIAL_SNAPSHOT" "$victim_id")
  printf '%s' "$victim_id" > "$VICTIM_ID_FILE"
  printf '%s' "$victim_name" > "$VICTIM_NAME_FILE"

  node=$(ssh_user "squeue -h -j $victim_id -o '%N'" | tr -d '[:space:]')
  if [[ -z "$node" ]]; then
    printf '  victim %s has no NodeList yet — not RUNNING?\n' "$victim_id"
    return 1
  fi
  printf '  victim: jobid=%s name=%s node=%s\n' \
    "$victim_id" "$victim_name" "$node"

  # SIGKILL via srun --jobid attach so the kill runs inside the
  # victim's allocation and reaches the secondary process tree. We
  # match by --job-name (jobid was passed as --job-name to slurm by
  # the framework, see respawn/spawner.rs).
  ssh_user "srun --jobid=$victim_id --overlap bash -lc \
            'pkill -KILL -f dynrunner-secondary || \
             pkill -KILL -f dynamic_runner || true'" \
    >/dev/null 2>&1 || true

  printf '  pkill -KILL issued inside allocation\n'
}

# 4. Watch for the slurm-authoritative count to drop, then for a NEW
#    job_id to appear (additive). Deadline tracks the brief's "before
#    SLURM timeout" window — we just need to see the new id arrive.
stage_observe_respawn() {
  local deadline=$((SECONDS + 180))
  local initial_ids new_ids appeared cur_snap
  initial_ids=$(ids_from "$INITIAL_SNAPSHOT")
  cur_snap="${TMPDIR_TEST}/cur.snap"

  while (( SECONDS < deadline )); do
    snapshot_jobs "$cur_snap"
    appeared=$(comm -13 \
                 <(echo "$initial_ids") \
                 <(ids_from "$cur_snap"))
    if [[ -n "$appeared" ]]; then
      printf '%s\n' "$appeared" > "$NEW_IDS_FILE"
      cp "$cur_snap" "$POST_SNAPSHOT"
      printf '  new job_id(s) seen: %s\n' "$(echo "$appeared" | tr '\n' ' ')"
      return 0
    fi
    sleep 3
  done
  printf '  no new job_id appeared within %ds\n' 180
  return 1
}

# 5. ADDITIVE invariant: the dead victim's job_id MUST still be present
#    in the slurm view after respawn — i.e. respawn added, it did not
#    replace. (The dead slot stays until SLURM timeout / natural exit.)
stage_assert_additive() {
  local victim_id present
  victim_id=$(cat "$VICTIM_ID_FILE")
  present=$(awk -v id="$victim_id" '$1==id' "$POST_SNAPSHOT")
  if [[ -z "$present" ]]; then
    printf '  victim %s vanished from squeue — respawn REPLACED, not added\n' \
      "$victim_id"
    return 1
  fi
  printf '  victim %s still in squeue post-respawn — additive confirmed\n' \
    "$victim_id"
}

# 6. PREFIX-PRESERVATION invariant: respawned job's name shares the
#    consumer prefix with the victim. spawner.rs uses
#    "<consumer_prefix>-<secondary_id>" so the prefix matches the
#    everything-before-the-last-`-` of the original.
stage_assert_prefix_preserved() {
  local new_id new_name prefix
  new_id=$(head -n1 "$NEW_IDS_FILE")
  new_name=$(name_for "$POST_SNAPSHOT" "$new_id")
  prefix="$DYNRUNNER_JOB_PREFIX"
  printf '  new job: id=%s name=%s expected-prefix=%s\n' \
    "$new_id" "$new_name" "$prefix"
  [[ "$new_name" == "${prefix}"* ]]
}

# 7. ZERO-SCANCEL invariant from the primary side: no scancel call by
#    the primary user for ANY of the initial secondary ids. We check
#    BOTH paths the brief calls out:
#
#   (a) primary.log contains no "scancel" mention attributable to the
#       respawn pipeline. Legitimate scancels (teardown, fatal-abort,
#       setup-abort) all live in dynrunner-slurm::job_manager::lifecycle
#       and emit through cancel_job / cancel_all_jobs sites. Those
#       belong to phases the test never reaches (we kill mid-run, the
#       dispatch is still in active steady-state). So a respawn-window
#       grep is "any scancel line at all" — none should appear.
#   (b) sacct shows the victim was NOT cancelled by the test user.
#       Anything other than CANCELLED-by-<user> for the victim id
#       passes (e.g. FAILED, NODE_FAIL, TIMEOUT, RUNNING-still). The
#       point: it wasn't the primary that scancelled.
stage_assert_zero_scancel_primary_log() {
  local victim_id hits
  victim_id=$(cat "$VICTIM_ID_FILE")
  hits=$(ssh_user "grep -c -F 'scancel' '$DYNRUNNER_PRIMARY_LOG'" \
           || echo 0)
  hits=${hits//[^0-9]/}
  printf '  primary.log scancel mentions: %s\n' "$hits"
  (( hits == 0 ))
}

stage_assert_no_cancel_by_user_in_sacct() {
  local victim_id state user
  victim_id=$(cat "$VICTIM_ID_FILE")
  # sacct -X = job-step-collapsed; -P pipe-separated; -n no header.
  # Field order matches -o.
  local row
  row=$(ssh_user "sacct -j $victim_id -X -P -n -o State,User" \
          | head -n1)
  state=$(printf '%s' "$row" | awk -F'|' '{print $1}' | awk '{print $1}')
  user=$(printf  '%s' "$row" | awk -F'|' '{print $2}')
  printf '  victim sacct: state=%s user=%s\n' "${state:-<empty>}" "${user:-<empty>}"
  # FAIL only if the victim is CANCELLED by testuser (the primary's
  # user). Any other terminal/in-progress state passes.
  if [[ "$state" == CANCELLED* && "$user" == "$TEST_USER" ]]; then
    printf '  victim was CANCELLED by primary user — invariant violated\n'
    return 1
  fi
}

# 8. Cleanup: tear down the dispatch and its surviving jobs. This is
#    OUR scancel (the test driver), not the primary's — outside the
#    invariant. Best-effort.
stage_cleanup() {
  ssh_user "squeue --me --noheader -o '%i' | xargs -r scancel" \
    >/dev/null 2>&1 || true
  return 0
}

# --- Prereqs -----------------------------------------------------------------

require_env DYNRUNNER_DISPATCH_CMD
require_env DYNRUNNER_PRIMARY_LOG
require_env DYNRUNNER_JOB_PREFIX

# --- Run ---------------------------------------------------------------------

TMPDIR_TEST="$(mktemp -d -t slurm-test543.XXXXXX)"
INITIAL_SNAPSHOT="${TMPDIR_TEST}/initial.snap"
POST_SNAPSHOT="${TMPDIR_TEST}/post.snap"
VICTIM_ID_FILE="${TMPDIR_TEST}/victim.id"
VICTIM_NAME_FILE="${TMPDIR_TEST}/victim.name"
NEW_IDS_FILE="${TMPDIR_TEST}/new.ids"
trap 'rm -rf -- "$TMPDIR_TEST"' EXIT

printf '=== slurm-test-env :: test-543-no-scancel (instance=%s) ===\n' "$INSTANCE_ID"

ensure_keypair

if command -v slurm-test-env-provision-user >/dev/null 2>&1; then
  provision_user=(slurm-test-env-provision-user)
else
  provision_user=("$SCRIPT_DIR/provision-user.sh")
fi
"${provision_user[@]}" "$TEST_USER" "$PUB_KEY"

stage 'launch multi-secondary dispatch'           stage_launch_dispatch
stage 'capture initial secondary job_id set'      stage_capture_initial
stage 'SIGKILL one secondary inside its alloc'    stage_kill_one_secondary
stage 'observe new (additive) respawn job_id'     stage_observe_respawn
stage 'additive: victim slot still in squeue'     stage_assert_additive
stage 'respawn name preserves consumer prefix'    stage_assert_prefix_preserved
stage 'zero scancel mentions in primary.log'      stage_assert_zero_scancel_primary_log
stage 'victim not CANCELLED by primary user'      stage_assert_no_cancel_by_user_in_sacct
stage 'cleanup: scancel surviving test jobs'      stage_cleanup

# --- Summary -----------------------------------------------------------------

printf '\n=== test-543-no-scancel summary ===\n'
printf '  passed: %d\n' "$PASS_COUNT"
printf '  failed: %d\n' "$FAIL_COUNT"
if (( FAIL_COUNT > 0 )); then
  printf '  failed stages:\n'
  for name in "${FAILURES[@]}"; do printf '    - %s\n' "$name"; done
  exit 1
fi
printf '  result: ALL PASSED — #543 invariant holds\n'
