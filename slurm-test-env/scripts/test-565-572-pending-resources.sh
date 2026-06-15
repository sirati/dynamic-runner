#!/usr/bin/env bash
# E2E assertion for #565 + #572: PENDING(Resources) unschedulable-quorum
# early-signal.
#
# Single concern: drive a dynamic_runner run on the slurm-test-env cluster
# with `--jobs N` larger than the partition's effective capacity, capture
# the observer's --important-stdio-only stdout, and assert that the
# PENDING(Resources) INFO line fires (within ~60s of the first probe
# cycle landing after wait_for_connections starts) AND that the run
# proceeds at (N - pending) / N once the setup deadline elapses.
#
# What the fix ships (see crates/dynrunner-manager-distributed/src/
# authority_snapshot.rs::publish, lines 271-310):
#
#   tracing::info!(
#       target: dynrunner_core::IMPORTANT_TARGET,
#       pending_resources,
#       requested = expected,
#       partition = partition_label,
#       "setup-quorum is waiting for {expected} secondaries; \
#        however slurm queue shows {pending_resources} job(s) in \
#        PENDING(Resources) on partition {partition_label} — those \
#        may NEVER schedule on this partition's capacity. \
#        Run will proceed at {} / {expected} after the setup \
#        deadline elapses. To avoid the wait, lower --jobs to {} \
#        or use a partition with more capacity.",
#       expected.saturating_sub(pending_resources),
#       expected.saturating_sub(pending_resources),
#   );
#
# #572 moved this emit from wait_for_connections entry (which raced the
# first squeue probe at ~0s) to the snapshot owner's probe-publish path,
# so the signal latches on the first probe round that returns
# pending_resources > 0 (~30s after wait_for_connections starts on the
# default 30s probe cadence).
#
# Cluster shape assumed (slurm-test-env defaults):
#   * partition = "debug" (Default=YES, see modules/slurm-cluster.nix)
#   * 4 worker nodes, CPUs=2 each (env.sh WORKER_COUNT=4)
#   * Total slot count = 8 CPUs across 4 nodes
#
# Test recipe: submit with `--jobs 5 --slurm-cpus-per-task 2
# --slurm-partition debug`. Each secondary asks for 2 CPUs on one node,
# saturating that node. 4 secondaries fit (one per worker); the 5th
# parks in PENDING(Resources) on capacity. After the setup deadline
# fires, the run proceeds at 4/5.
#
# Exit codes (smoke-test.sh contract):
#   0  all assertions pass
#   1  at least one assertion failed
#   70 cluster prereqs not met (cluster down OR launch cmd unset)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SLURM_TEST_ENV_ENV_FILE:-${SCRIPT_DIR}/../deploy/env.sh}"
# shellcheck disable=SC1090
source "$ENV_FILE"

# --- Test identity ----------------------------------------------------------
# Mirror smoke-test.sh's keypair scheme: per-instance, persistent under
# $STATE_DIR/keys, re-used across runs and down/up cycles.

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

# --- ssh wrapper ------------------------------------------------------------
# Identical knobs to smoke-test.sh's ssh_user — see that script for the
# rationale (IdentitiesOnly, no host-key checking, no agent leak). The
# long-running launch in `run_dynrunner` cannot reach a bash function
# through `timeout`, so it inlines the same flags under `bash -c`; this
# helper covers the short reachability check in `preflight`.

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

# --- Preflight --------------------------------------------------------------
# The cluster lifecycle is SHARED across e2e tests (orchestrator brings
# it up via `nix run .#up`; we don't touch it). Validate the two things
# this test owns: the cluster is reachable as `testuser`, and the
# orchestrator has supplied a launch command for dynamic_runner on the
# gateway.
#
# DYNRUNNER_TEST_LAUNCH_CMD: the remote command (as a single string)
# that, when invoked via ssh and given the test's CLI tail args, runs
# a dynamic_runner job. The orchestrator sets this so this test script
# does NOT need to know how dynamic_runner was staged (wheel under
# /home/<user>, nix-built venv on the gateway, etc.). Exit 70 if unset
# — matches the smoke-test.sh "cluster not running" contract for
# environment prereqs we cannot fix from inside the test.

preflight() {
  ensure_keypair
  # Resolve the user-provisioner the same way smoke-test.sh does. We
  # don't strictly need to re-provision (smoke-test would have, and the
  # orchestrator runs that first), but doing it idempotently shields us
  # from a fresh /home wipe between runs.
  if command -v slurm-test-env-provision-user >/dev/null 2>&1; then
    provision_user=(slurm-test-env-provision-user)
  else
    provision_user=("$SCRIPT_DIR/provision-user.sh")
  fi
  if ! "${provision_user[@]}" "$TEST_USER" "$PUB_KEY" >/dev/null 2>&1; then
    printf 'preflight: provision-user failed; cluster likely not running.\n' >&2
    exit 70
  fi
  if ! ssh_user true >/dev/null 2>&1; then
    printf 'preflight: ssh to %s@localhost:%s failed; cluster not reachable.\n' \
      "$TEST_USER" "$SSH_PORT" >&2
    exit 70
  fi
  if [[ -z "${DYNRUNNER_TEST_LAUNCH_CMD:-}" ]]; then
    printf 'preflight: DYNRUNNER_TEST_LAUNCH_CMD is unset.\n' >&2
    printf '  This test needs a remote command that launches a dynamic_runner\n' >&2
    printf '  run on the gateway. The orchestrator (which built/staged the\n' >&2
    printf '  wheel) owns this seam — set DYNRUNNER_TEST_LAUNCH_CMD to a\n' >&2
    printf '  command line such as:\n' >&2
    printf '    "cd ~/run && PYTHONPATH=/home/testuser/dynrunner python3 -m tests.e2e.test_consumer"\n' >&2
    printf '  This test will append the test-specific CLI tail args.\n' >&2
    exit 70
  fi
}

# --- Test parameters --------------------------------------------------------
# Match the assumptions in the script header: 4 capacity slots, 5
# requested, 1 PENDING(Resources). The CPU shape (2 per task on 2-CPU
# nodes) is what makes one secondary monopolise one node and forces the
# 5th into PENDING(Resources). Keep PARTITION the test-env default
# (modules/slurm-cluster.nix `Default=YES`); the emit reports the actual
# partition name, so changing this here must mirror to the assertion.

PARTITION="debug"
REQUESTED_JOBS=5
CPUS_PER_TASK=2
EXPECTED_PENDING=1
EXPECTED_RUNNING=$((REQUESTED_JOBS - EXPECTED_PENDING))  # 4

# The emit fires on the FIRST probe round that observes pending > 0.
# Probe cadence is 30s (drive_rust.rs::authority_probe_interval). Allow
# two cycles + slack to absorb job-submission latency and the initial
# slurmctld settle.
EMIT_WAIT_DEADLINE=90

# Cap the whole run. The orchestrator's launch cmd should target a NOP
# task that completes well within this once 4/5 secondaries are up. If
# the run does not finish by RUN_DEADLINE we treat it as a failure
# (likely a stuck setup-quorum, which is the bug the fix prevents).
RUN_DEADLINE=180

# --- Logging ----------------------------------------------------------------
# Tee the launch command's combined stdout+stderr into a per-run log file
# so assertions can grep it after the run terminates. The path is
# deterministic per-instance so the operator can re-inspect after a fail
# — but we wipe it at the top of each run to avoid stale lines causing
# false-pass on a re-run.

LOG_DIR="${STATE_DIR}/test-565-572"
LOG_FILE="${LOG_DIR}/observer-stdio.log"

prepare_log() {
  mkdir -p "$LOG_DIR"
  : > "$LOG_FILE"
}

# --- Run --------------------------------------------------------------------
#
# Launch dynamic_runner on the gateway via ssh under a hard wall-clock
# timeout, tee the combined stream into $LOG_FILE for assertion-time
# grep, AND stream it live to the operator's terminal.
#
# `timeout` exec()s argv[0] directly so a bash function can't be its
# target — we inline the ssh invocation under `bash -c` instead, with
# positional args carrying the per-instance ssh knobs (matches
# ssh_user_stream's flag set above; kept in one place via the local
# `cli_tail` + `remote_cmd` construction).
#
# `--important-stdio-only` is the seam under test: the PENDING signal
# must surface here, NOT only in the full log. `--jobs 5 --slurm-partition
# debug --slurm-cpus-per-task 2` produces the over-capacity shape (one
# secondary per 2-CPU node saturates that node; 4 fit, 5th pends).

run_dynrunner() {
  local cli_tail
  cli_tail="--multi-computer slurm --important-stdio-only \
--jobs ${REQUESTED_JOBS} --slurm-partition ${PARTITION} \
--slurm-cpus-per-task ${CPUS_PER_TASK}"

  local remote_cmd
  remote_cmd="${DYNRUNNER_TEST_LAUNCH_CMD} ${cli_tail}"

  set +e
  timeout --kill-after=10 "$RUN_DEADLINE" bash -c '
    ssh -i "$1" \
        -o IdentitiesOnly=yes \
        -o IdentityAgent=none \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR \
        -o ConnectTimeout=10 \
        -o ServerAliveInterval=15 \
        -p "$2" "$3@localhost" "$4"
  ' _ "$PRIV_KEY" "$SSH_PORT" "$TEST_USER" "$remote_cmd" 2>&1 \
    | tee "$LOG_FILE"
  RUN_EXIT="${PIPESTATUS[0]}"
  set -e
}

# --- Assertions -------------------------------------------------------------
#
# Each `assert_*` returns 0 / nonzero so the `stage` harness from
# smoke-test.sh can run them all and accumulate a summary instead of
# bailing on the first failure.

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

# Verify the PENDING-emit landed at all. We grep the message-prefix
# substring (no timestamp, no struct-field positioning) so a future
# log-format change inside the tracing layer doesn't false-fail us; the
# message text itself is the contract.
assert_pending_emit_text() {
  if grep -F -q "setup-quorum is waiting for ${REQUESTED_JOBS} secondaries" \
       "$LOG_FILE"; then
    return 0
  fi
  printf '  log does NOT contain "setup-quorum is waiting for %s secondaries"\n' \
    "$REQUESTED_JOBS"
  return 1
}

assert_partition_named() {
  if grep -F -q "PENDING(Resources) on partition ${PARTITION}" "$LOG_FILE"; then
    return 0
  fi
  printf '  log does NOT contain "PENDING(Resources) on partition %s"\n' \
    "$PARTITION"
  return 1
}

assert_lower_jobs_suggestion() {
  if grep -F -q "lower --jobs to ${EXPECTED_RUNNING}" "$LOG_FILE"; then
    return 0
  fi
  printf '  log does NOT contain "lower --jobs to %s"\n' \
    "$EXPECTED_RUNNING"
  return 1
}

assert_proceed_at_running_over_requested() {
  if grep -F -q "proceed at ${EXPECTED_RUNNING} / ${REQUESTED_JOBS}" \
       "$LOG_FILE"; then
    return 0
  fi
  printf '  log does NOT contain "proceed at %s / %s"\n' \
    "$EXPECTED_RUNNING" "$REQUESTED_JOBS"
  return 1
}

# Tracing structured fields land in the line as `field=value` (no quotes
# for ints, quotes optional for strings — we use a regex that accepts
# both forms so a future fmt-layer change does not flake us).
assert_field_pending_resources() {
  if grep -E -q "pending_resources=${EXPECTED_PENDING}\b" "$LOG_FILE"; then
    return 0
  fi
  printf '  log does NOT contain field "pending_resources=%s"\n' \
    "$EXPECTED_PENDING"
  return 1
}

assert_field_requested() {
  if grep -E -q "requested=${REQUESTED_JOBS}\b" "$LOG_FILE"; then
    return 0
  fi
  printf '  log does NOT contain field "requested=%s"\n' "$REQUESTED_JOBS"
  return 1
}

assert_field_partition() {
  if grep -E -q "partition=\"?${PARTITION}\"?(\s|$)" "$LOG_FILE"; then
    return 0
  fi
  printf '  log does NOT contain field "partition=%s"\n' "$PARTITION"
  return 1
}

# Timing assertion: the emit must land within EMIT_WAIT_DEADLINE seconds
# of run start. We can't directly time the emit in this script (the run
# is a single blocking ssh), but we CAN verify it landed BEFORE any
# "all secondaries connected" / "phase 0 starting" line — i.e. it
# preceded the post-setup-deadline phase progression. Skip this check
# if the run was killed by the outer timeout (RUN_EXIT == 124).
assert_emit_preceded_run_progress() {
  if (( RUN_EXIT == 124 )); then
    printf '  run hit RUN_DEADLINE (%ss); cannot validate ordering\n' \
      "$RUN_DEADLINE"
    return 1
  fi
  local pending_line phase_line
  pending_line="$(grep -n -F "setup-quorum is waiting for " "$LOG_FILE" \
    | head -n1 | cut -d: -f1 || true)"
  # Any post-quorum phase signal: "phase" / "secondaries connected" /
  # the proceed message itself moving from pending to a runtime line.
  phase_line="$(grep -n -E "(phase|secondaries connected|run starting|RunStarted)" \
    "$LOG_FILE" | head -n1 | cut -d: -f1 || true)"
  if [[ -z "$pending_line" ]]; then
    printf '  PENDING emit line not present; cannot order\n'
    return 1
  fi
  if [[ -z "$phase_line" ]]; then
    # No subsequent phase line is fine — many short NOP runs print only
    # the proceed message; treat absence as inconclusive-pass.
    return 0
  fi
  if (( pending_line < phase_line )); then
    return 0
  fi
  printf '  PENDING emit (line %s) did NOT precede phase progress (line %s)\n' \
    "$pending_line" "$phase_line"
  return 1
}

# The run should EXIT CLEANLY once the setup deadline fires — i.e. the
# fix must not turn a quorum shortfall into a fatal error. (Exit code 0
# from the launch command; the NOP task body the orchestrator selects
# must itself be a no-op success.)
assert_run_completed_cleanly() {
  if (( RUN_EXIT == 0 )); then
    return 0
  fi
  if (( RUN_EXIT == 124 )); then
    printf '  run was killed by RUN_DEADLINE (%ss) — likely stuck quorum\n' \
      "$RUN_DEADLINE"
    return 1
  fi
  printf '  launch cmd exited non-zero (%s)\n' "$RUN_EXIT"
  return 1
}

# --- Drive ------------------------------------------------------------------

printf '=== slurm-test-env :: PENDING(Resources) e2e (#565+#572) (instance=%s) ===\n' \
  "$INSTANCE_ID"

preflight
prepare_log

printf '\nlaunching dynamic_runner: --jobs %s --slurm-partition %s --slurm-cpus-per-task %s\n' \
  "$REQUESTED_JOBS" "$PARTITION" "$CPUS_PER_TASK"
printf 'capturing observer stdout to: %s\n' "$LOG_FILE"

run_dynrunner

stage 'PENDING line names the requested secondary count'  assert_pending_emit_text
stage 'PENDING line names the partition'                   assert_partition_named
stage 'PENDING line suggests lowering --jobs'              assert_lower_jobs_suggestion
stage 'PENDING line announces 4/5 reduced quorum'          assert_proceed_at_running_over_requested
stage 'structured field pending_resources=1'               assert_field_pending_resources
stage 'structured field requested=5'                       assert_field_requested
stage 'structured field partition=debug'                   assert_field_partition
stage 'PENDING emit precedes any post-quorum progress'     assert_emit_preceded_run_progress
stage 'run completes cleanly at reduced quorum'            assert_run_completed_cleanly

printf '\n=== test summary ===\n'
printf '  passed: %d\n' "$PASS_COUNT"
printf '  failed: %d\n' "$FAIL_COUNT"
if (( FAIL_COUNT > 0 )); then
  printf '  failed stages:\n'
  for name in "${FAILURES[@]}"; do printf '    - %s\n' "$name"; done
  printf '  log captured at: %s\n' "$LOG_FILE"
  exit 1
fi
printf '  result: ALL PASSED\n'
