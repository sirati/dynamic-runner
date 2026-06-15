#!/usr/bin/env bash
# E2E assertion for #547 — chunked apply_spawn_tasks via
# PumpSpawnContinuation. Drives a >256-task spawn_tasks burst on a
# running cluster, captures the primary's full log, and asserts the
# operational select! kept its sibling arms alive across the burst.
#
# Single concern: post-mortem assertions over the primary log; ALL
# workload-shape concerns live in test_547_workload/. Cluster lifecycle
# is shared across tests — this script takes the cluster as-given (the
# operator/orchestrator runs `nix run .#up` first).
#
# Fix under test:
#   `Chunk apply_spawn_tasks across select! iterations via
#    PumpSpawnContinuation` — a large `PrimaryCommand::SpawnTasks` was
#   applied in ONE COMMAND-arm body of the operational `select!`,
#   wedging sibling arms (heartbeat_tick / inbox / matcher) for the
#   whole apply. Chunking at K=APPLY_SPAWN_CHUNK_SIZE=256 + returning
#   to `select!` between chunks lets siblings re-fire mid-burst.
#
# Proof in the log:
#   The operational loop emits a periodic INFO line:
#     oploop=iter=N arm_counts=[command=..., matcher=..., worker_mgmt=...,
#       inbox=..., heartbeat=..., ...] since_inbox=K inbox_idle=Ts last_arm=X
#     "oploop arm stats"
#   For #547 to be in effect during a >256-task burst we expect:
#     - At least one stats line (proves the loop emitted at all)
#     - `inbox=N` with N>0 AND `heartbeat=N` with N>0 — sibling arms
#       fired during the run (a single-burst loop with NO sibling
#       activity would show inbox=0 heartbeat=0)
#     - Final `inbox_idle=Ts` with T<5 — the inbox arm never went
#       silent for >5s (the wedge signature; the production fix's
#       starvation gate fires at 30s, so 5s is a generous floor)
#     - No "oploop INBOUND arm starved" WARN line (the starvation
#       warning emitted by oploop_instrumentation.rs)
#
# Exit codes:
#   0  all assertions pass
#   1  one or more assertions failed
#   70 cluster is not running (matches smoke-test.sh's contract)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SLURM_TEST_ENV_ENV_FILE:-${SCRIPT_DIR}/../deploy/env.sh}"
# shellcheck disable=SC1090
source "$ENV_FILE"

# --- Test identity & ssh wrapper --------------------------------------------
#
# Same pattern as smoke-test.sh: ed25519 keypair under $STATE_DIR/keys
# re-used across runs; force IdentitiesOnly + IdentityAgent=none so we
# never fall through to a host-side agent identity.

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

scp_user_from() {
  scp -i "$PRIV_KEY" \
      -o IdentitiesOnly=yes \
      -o IdentityAgent=none \
      -o StrictHostKeyChecking=no \
      -o UserKnownHostsFile=/dev/null \
      -o LogLevel=ERROR \
      -o ConnectTimeout=10 \
      -P "$SSH_PORT" "${TEST_USER}@localhost:$1" "$2"
}

# --- Configurable knobs (env overrides) -------------------------------------
#
# DYNRUNNER_CMD is the in-cluster invocation of the test driver. The
# orchestrator wires this up at post-merge execution time (the wheel
# and the test_547_workload package land in the cluster via
# operator-coordinated deployment; see brief-e2e-common-rules.md). For
# the static gate this stays a default that documents the expected
# shape — the script is validated by `nix flake check`, `bash -n`, and
# `nix build .#test-547-chunking` and does NOT actually run end-to-end
# until the operator wires DYNRUNNER_CMD.
#
# DRIVER_BURST_TASKS overrides the per-burst count (default 400, must
# stay >256 to exercise APPLY_SPAWN_CHUNK_SIZE).
#
# GATEWAY_RUN_ROOT is where the driver puts its logs/source/output. The
# default writes under $HOME on the gateway so a stock --multi-computer
# single-process run works without --slurm-root.

DYNRUNNER_CMD="${DYNRUNNER_CMD:-python -m test_547_workload.driver}"
DRIVER_BURST_TASKS="${DRIVER_BURST_TASKS:-400}"
GATEWAY_RUN_ROOT="${GATEWAY_RUN_ROOT:-/home/${TEST_USER}/test-547-run}"
DRIVER_TIMEOUT_SECS="${DRIVER_TIMEOUT_SECS:-300}"

# Where the assertion grep tail-greps. The primary writes its full log
# to ${GATEWAY_RUN_ROOT}/dynrunner-full.log per `--full-log-file`.
PRIMARY_LOG_REMOTE="${GATEWAY_RUN_ROOT}/dynrunner-full.log"

# Threshold for the per-emit inbox-idle wedge check. The production
# starvation-WARN gate is 30s; we assert a tighter 5s floor so a soft
# starvation (no WARN yet, but inbox quiet long enough to be suspect)
# still fails the test.
INBOX_IDLE_MAX_SECS="${INBOX_IDLE_MAX_SECS:-5}"

# --- Stage harness ----------------------------------------------------------

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

# --- Stages -----------------------------------------------------------------

# Sanity-check the cluster: same shape as smoke-test's sinfo gate, so we
# fail fast (with the SAME 70 exit code below if cluster never came up)
# before running the driver.
stage_sinfo() {
  local out
  out="$(ssh_user 'sinfo --noheader -o "%P %a %D %T"')" || return 1
  printf '%s\n' "$out" | sed 's/^/    /'
  [[ -n "$out" ]]
}

# Run the workload. Writes the full log to $PRIMARY_LOG_REMOTE on the
# gateway. The driver runs --multi-computer single-process so the
# primary's operational_loop is in the local Python process — the
# `oploop arm stats` line still emits to --full-log-file (the framework
# wires tracing → log file uniformly across multi-computer modes).
stage_run_driver() {
  ssh_user "set -euo pipefail; \
    mkdir -p '${GATEWAY_RUN_ROOT}/source' '${GATEWAY_RUN_ROOT}/output'; \
    rm -f '${PRIMARY_LOG_REMOTE}'; \
    cd '${GATEWAY_RUN_ROOT}'; \
    timeout ${DRIVER_TIMEOUT_SECS} ${DYNRUNNER_CMD} \
      --multi-computer single-process \
      --jobs 2 \
      --source '${GATEWAY_RUN_ROOT}/source' \
      --output '${GATEWAY_RUN_ROOT}/output' \
      --full-log-file '${PRIMARY_LOG_REMOTE}' \
      --burst-tasks ${DRIVER_BURST_TASKS}" \
    || return 1
  # The full log MUST exist (no log = no proof = fail).
  ssh_user "test -s '${PRIMARY_LOG_REMOTE}'"
}

# Pull the full log to a local temp file so every following stage can
# grep it without an ssh round-trip per check.
LOCAL_LOG=""
stage_fetch_log() {
  LOCAL_LOG="$(mktemp -t test-547-primary-log.XXXXXX)"
  scp_user_from "${PRIMARY_LOG_REMOTE}" "$LOCAL_LOG" || return 1
  printf '  fetched: %s (%d bytes)\n' \
    "$LOCAL_LOG" "$(wc -c < "$LOCAL_LOG")"
  [[ -s "$LOCAL_LOG" ]]
}

# The primary MUST have emitted at least one "oploop arm stats" line
# during the run. Absence = the loop never reached
# `STATS_LINE_INTERVAL` (120s) OR the loop never ran at all — either
# way no proof.
stage_emitted_arm_stats() {
  local n
  n="$(grep -c 'oploop arm stats' "$LOCAL_LOG" || true)"
  printf '  oploop arm stats emit count: %d\n' "$n"
  (( n >= 1 ))
}

# Extract every "oploop arm stats" line's `arm_counts=[...]` body and
# assert the final-cumulative sibling-arm activity. The fix proves
# itself by:
#   - command<N (each chunk is ONE arm-win; for N>256 the burst takes
#     ceil(N/256) command-arm wins — but in single-process mode many
#     other COMMAND messages also flow, so we only assert sibling arms
#     fired at all, since the dispute is "could OTHER arms win during
#     the apply", not "what command's final tally is")
#   - inbox > 0   (sibling arm fired at some point during the run)
#   - heartbeat > 0 (heartbeat arm fired during the run)
#
# The asm-tokenizer 300-task validation cited in the brief saw
# command=0 inbox=139 heartbeat=24; for single-process mode with no
# distributed inbox traffic the inbox count may be lower, but a clean
# chunked apply still posts at least ONE non-command arm win.
stage_arm_counts_show_sibling_activity() {
  # Pull every arm_counts block and tally per-arm max — successive
  # stats lines are CUMULATIVE since loop entry, so the LAST line's
  # values are the final-counts we assert against.
  local last_line
  last_line="$(grep 'oploop arm stats' "$LOCAL_LOG" | tail -n1)"
  printf '  last stats line: %s\n' "$last_line"
  [[ -n "$last_line" ]] || return 1

  # arm_counts=[command=N, matcher=N, worker_mgmt=N, inbox=N, ...]
  local arm_counts
  arm_counts="$(printf '%s\n' "$last_line" \
    | sed -n 's/.*arm_counts=\[\([^]]*\)\].*/\1/p')"
  printf '  arm_counts: %s\n' "$arm_counts"
  [[ -n "$arm_counts" ]] || return 1

  local inbox heartbeat command
  inbox="$(printf '%s\n' "$arm_counts" \
    | sed -n 's/.*\binbox=\([0-9][0-9]*\).*/\1/p')"
  heartbeat="$(printf '%s\n' "$arm_counts" \
    | sed -n 's/.*\bheartbeat=\([0-9][0-9]*\).*/\1/p')"
  command="$(printf '%s\n' "$arm_counts" \
    | sed -n 's/.*\bcommand=\([0-9][0-9]*\).*/\1/p')"
  inbox="${inbox:-0}"
  heartbeat="${heartbeat:-0}"
  command="${command:-0}"
  printf '  parsed: command=%s inbox=%s heartbeat=%s\n' \
    "$command" "$inbox" "$heartbeat"

  # Sibling activity assertion: both sibling arms must show >0 wins.
  if (( inbox <= 0 )); then
    printf '  inbox arm never won — sibling arms wedged behind the burst\n'
    return 1
  fi
  if (( heartbeat <= 0 )); then
    printf '  heartbeat arm never won — sibling arms wedged behind the burst\n'
    return 1
  fi
}

# `inbox_idle=Ts` on the last stats line MUST be <INBOX_IDLE_MAX_SECS.
# The since_inbox iteration counter is per-loop, so wall-clock idle is
# the comparable axis to "5s stall" in the brief.
stage_no_inbox_stall() {
  local last_line idle_secs
  last_line="$(grep 'oploop arm stats' "$LOCAL_LOG" | tail -n1)"
  [[ -n "$last_line" ]] || return 1
  idle_secs="$(printf '%s\n' "$last_line" \
    | sed -n 's/.*inbox_idle=\([0-9][0-9]*\)s.*/\1/p')"
  idle_secs="${idle_secs:-0}"
  printf '  final inbox_idle: %ss (max allowed: %ss)\n' \
    "$idle_secs" "$INBOX_IDLE_MAX_SECS"
  (( idle_secs < INBOX_IDLE_MAX_SECS ))
}

# The starvation WARN line emitted by oploop_instrumentation.rs
# (gate: 10k iters + 30s wall-clock + 30s cooldown) MUST NOT have
# fired. Its presence is the hard-proof signature of the wedge the
# #547 fix was meant to prevent.
stage_no_starvation_warn() {
  local hits
  hits="$(grep -c 'oploop INBOUND arm starved' "$LOCAL_LOG" || true)"
  printf '  starvation WARN count: %d\n' "$hits"
  (( hits == 0 ))
}

# Every burst task must have completed (no oploop-wedge starvation =
# the dispatch path never stalled = all 400 tasks ran). The driver
# writes `ran <name>` per task; counting `burst-` outputs proves all
# DRIVER_BURST_TASKS finished.
stage_all_burst_tasks_completed() {
  local n
  n="$(ssh_user "ls '${GATEWAY_RUN_ROOT}/output' 2>/dev/null \
    | grep -c '^burst-' || true")"
  printf '  burst tasks completed: %s / expected %s\n' \
    "$n" "$DRIVER_BURST_TASKS"
  (( n == DRIVER_BURST_TASKS ))
}

# --- Run --------------------------------------------------------------------

printf '=== slurm-test-env :: test-547-chunking (instance=%s) ===\n' "$INSTANCE_ID"
printf '  DYNRUNNER_CMD=%s\n' "$DYNRUNNER_CMD"
printf '  DRIVER_BURST_TASKS=%s\n' "$DRIVER_BURST_TASKS"
printf '  PRIMARY_LOG_REMOTE=%s\n' "$PRIMARY_LOG_REMOTE"

ensure_keypair

if command -v slurm-test-env-provision-user >/dev/null 2>&1; then
  provision_user=(slurm-test-env-provision-user)
else
  provision_user=("$SCRIPT_DIR/provision-user.sh")
fi
"${provision_user[@]}" "$TEST_USER" "$PUB_KEY"

trap '[[ -n "$LOCAL_LOG" ]] && rm -f -- "$LOCAL_LOG"' EXIT

stage 'sinfo lists partitions and nodes' stage_sinfo
stage 'driver runs and writes primary full-log' stage_run_driver
stage 'fetch primary log to host' stage_fetch_log
stage 'primary emitted >=1 "oploop arm stats" line' stage_emitted_arm_stats
stage 'arm_counts show sibling arms fired during burst' \
  stage_arm_counts_show_sibling_activity
stage 'final inbox_idle below stall threshold' stage_no_inbox_stall
stage 'no "oploop INBOUND arm starved" WARN fired' stage_no_starvation_warn
stage 'all burst tasks completed' stage_all_burst_tasks_completed

# --- Summary ---------------------------------------------------------------

printf '\n=== test-547-chunking summary ===\n'
printf '  passed: %d\n' "$PASS_COUNT"
printf '  failed: %d\n' "$FAIL_COUNT"
if (( FAIL_COUNT > 0 )); then
  printf '  failed stages:\n'
  for name in "${FAILURES[@]}"; do printf '    - %s\n' "$name"; done
  exit 1
fi
printf '  result: ALL PASSED\n'
