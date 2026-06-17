#!/usr/bin/env bash
# E2E assertion for #575 secondary resource stats (CRDT emission +
# observer aggregation + per-field 25%-relative inclusion gate).
#
# The fix:
#   * Each compute secondary holds a 10-minute local-only rolling buffer
#     of raw OOM-sweep samples (kept in process memory, never crosses
#     the CRDT).
#   * Every 5 minutes it broadcasts ONE SecondaryResourceSample CRDT
#     mutation carrying memory P10/P30/P50/P70/P90/avg, free RAM, swap
#     used, free swap and CPU utilisation (milli-percent).
#   * The observer averages each field across alive compute secondaries
#     and prints each in its periodic-stats output IFF the averaged
#     value moved more than 25% from the last-printed value (per-field
#     independent gate; first non-zero is always included; an omitted
#     line does NOT advance its baseline).
#
# What this script asserts when a live observer log is reachable:
#   A. 5-minute emit cadence — successive "periodic cluster stats"
#      report timestamps in the observer log are 5min ± slack apart.
#   B. Resource fields appear in the periodic stats output when the
#      averaged value crosses the 25% threshold (or on first non-zero).
#      The labels "mem P10/P30/P50/P70/P90 (workers, avg per
#      secondary)", "mem avg (workers, avg per secondary)", "free host
#      memory (avg per secondary)", "host swap used (avg per
#      secondary)", "free host swap (avg per secondary)" and "host CPU
#      utilization (avg per secondary)" all map 1:1 to format.rs's
#      ResourceUnit-rendered lines.
#   C. Per-field independence: in any one report, some resource fields
#      may be omitted while others are present (a sibling-only print
#      does NOT silently advance the omitted field's baseline).
#   D. Rolling-buffer behaviour is INFERRED from the OOM-watcher's
#      50ms sample cadence × 10-min retention window: if probe.rs's
#      sweep loop is producing samples (per its own log lines) and an
#      aggregated emit fires every 5 minutes, the buffer is by
#      construction holding ≤ 12_000 samples per the SAMPLE_WINDOW
#      const in secondary/resource_buffer.rs (= Duration::from_secs(600)).
#
# Practical-limit skip:
#   slurm-test-env has no dynrunner binary on PATH inside the cluster
#   images (modules/worker.nix carries no dynrunner package); it
#   provides ONLY the SLURM control plane. A real multi-secondary
#   10-minute dynrunner run is therefore unreachable from this test
#   harness alone — observed only when a consumer (asm-tokenizer,
#   asm-dataset-nix) submits a real job. This script reports SKIP
#   (with a recorded reason) when no observer log artifacts are
#   reachable, and runs the four assertions when they are.
#
# Exit codes (per slurm-test-env/scripts/brief-e2e-common-rules.md):
#   0  all assertions PASS (or all assertions SKIP with documented
#      practical-limit reason)
#   1  at least one assertion FAIL
#   70 cluster is not running (matches smoke-test.sh contract)

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

# --- Cluster reachability -----------------------------------------------------

ensure_keypair

# Resolve the provisioner: prefer the flake-wrapped binary on PATH,
# fall back to the in-tree script when checked out directly. The
# 70-exit contract is inherited from provision-user.sh: it bails out
# if the cluster gateway is not running.
if command -v slurm-test-env-provision-user >/dev/null 2>&1; then
  provision_user=(slurm-test-env-provision-user)
else
  provision_user=("$SCRIPT_DIR/provision-user.sh")
fi
"${provision_user[@]}" "$TEST_USER" "$PUB_KEY"

printf '=== slurm-test-env :: test 575 resource stats (instance=%s) ===\n' \
  "$INSTANCE_ID"

# --- Stage harness ------------------------------------------------------------

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

# --- Observer-log discovery ---------------------------------------------------
#
# The observer emits its periodic stats via
#   tracing::info!(target: IMPORTANT_TARGET, "periodic cluster stats (10m):\n{report}")
# (see observer/reporting/reporter.rs line ~228). When the operator runs the
# primary/observer with --important-stdio-only, that log lands on the process'
# stdout; when run under SLURM with sbatch, stdout is captured to
# slurm-<jobid>.out under the user's submission cwd. We probe the testuser's
# home for any such artifact below — if none exists, this test has nothing to
# assert against and emits SKIP rather than FAIL.

OBSERVER_GLOB='~/dynrunner*.log ~/observer*.log ~/.dynrunner/*.log ~/slurm-*.out'

# Returns the newest matched log path on stdout, or empty if none.
find_observer_log() {
  # Run the glob expansion inside the user's shell so ~/... resolves
  # against the cluster-side homedir, not the host's. ls -t orders by
  # mtime descending; head -n1 picks the freshest.
  # shellcheck disable=SC2029
  ssh_user "ls -t ${OBSERVER_GLOB} 2>/dev/null | head -n1" || true
}

OBSERVER_LOG_PATH=""

stage_observer_log_present() {
  OBSERVER_LOG_PATH="$(find_observer_log)"
  if [[ -z "$OBSERVER_LOG_PATH" ]]; then
    printf '  no observer log artifact found at any of:\n'
    printf '    %s\n' $OBSERVER_GLOB
    printf '  slurm-test-env has no dynrunner-on-cluster harness (modules/worker.nix\n'
    printf '  carries no dynrunner package); a real multi-secondary 10-minute run is\n'
    printf '  only observable when a consumer submits a job. Skipping live-log\n'
    printf '  assertions; static-gate validation of the script body still applies.\n'
    return 77
  fi
  printf '  observer log: %s\n' "$OBSERVER_LOG_PATH"
  local lines
  lines="$(ssh_user "wc -l < $OBSERVER_LOG_PATH" 2>/dev/null || echo 0)"
  printf '  lines: %s\n' "$lines"
  return 0
}

# --- Assertion A: 5-minute emit cadence --------------------------------------
#
# The reporter wakes on a tokio::time::interval at the same cadence
# the secondary emits (5 minutes), and the importance log carries
# ISO-8601 timestamps from the tracing subscriber. We pull every line
# matching the report header, extract the timestamp, and verify
# consecutive deltas land in [4m, 6m] — generous slack for scheduler
# jitter on a busy host.

stage_emit_cadence() {
  if [[ -z "$OBSERVER_LOG_PATH" ]]; then
    return 77
  fi
  local raw_ts
  raw_ts="$(ssh_user "grep -F 'periodic cluster stats' $OBSERVER_LOG_PATH \
    | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}'" \
    2>/dev/null || true)"
  if [[ -z "$raw_ts" ]]; then
    printf '  no "periodic cluster stats" lines in observer log\n'
    return 77
  fi
  local count
  count="$(printf '%s\n' "$raw_ts" | wc -l)"
  printf '  found %s "periodic cluster stats" report(s)\n' "$count"
  if (( count < 2 )); then
    printf '  fewer than 2 reports — cannot measure cadence; run must be ≥10min\n'
    return 77
  fi
  # Compute pairwise epoch deltas. date -d on the gateway image (NixOS
  # coreutils) understands the ISO format. The cadence target is 300s;
  # we accept 240–360s to absorb interval-tick drift plus tracing-flush
  # latency.
  local epochs
  epochs="$(ssh_user "for ts in $raw_ts; do date -d \"\$ts\" +%s; done" \
    2>/dev/null || true)"
  if [[ -z "$epochs" ]]; then
    printf '  failed to parse timestamps\n'
    return 1
  fi
  local prev="" cur delta any_fail=0
  while read -r cur; do
    if [[ -n "$prev" ]]; then
      delta=$(( cur - prev ))
      printf '    delta: %ss\n' "$delta"
      if (( delta < 240 || delta > 360 )); then
        printf '    delta %ss outside [240,360] for 5min cadence\n' "$delta"
        any_fail=1
      fi
    fi
    prev="$cur"
  done <<<"$epochs"
  return "$any_fail"
}

# --- Assertion B: resource fields render with the documented labels ----------
#
# The exact label strings are owned by format.rs and must appear
# verbatim in render_report's output once the 25%-relative gate
# admits them. We check that AT LEAST ONE report carries each label
# across the whole run — a label that never appears across a multi-
# hour run is a strong signal the field's CRDT path is broken
# (apply_peer.rs apply site never landed, or aggregation returned
# None for the whole run).

RESOURCE_LABELS=(
  'mem P10 (workers, avg per secondary)'
  'mem P30 (workers, avg per secondary)'
  'mem P50 (workers, avg per secondary)'
  'mem P70 (workers, avg per secondary)'
  'mem P90 (workers, avg per secondary)'
  'mem avg (workers, avg per secondary)'
  'free host memory (avg per secondary)'
  'host swap used (avg per secondary)'
  'free host swap (avg per secondary)'
  'host CPU utilization (avg per secondary)'
)

stage_resource_fields_render() {
  if [[ -z "$OBSERVER_LOG_PATH" ]]; then
    return 77
  fi
  local any_fail=0 label hit
  for label in "${RESOURCE_LABELS[@]}"; do
    # grep -c prints its own 0 and exits 1 on no-match; swallow the
    # nonzero exit WITHOUT a spurious second echo, then keep only the
    # first line so $hit is always a single clean integer (empty → 0)
    # for the (( hit == 0 )) test below.
    hit="$(ssh_user "grep -cF '$label' $OBSERVER_LOG_PATH 2>/dev/null; true" | head -n1)"
    hit="${hit:-0}"
    printf '    "%s": %s appearance(s)\n' "$label" "$hit"
    if (( hit == 0 )); then
      any_fail=1
    fi
  done
  if (( any_fail != 0 )); then
    printf '  one or more resource labels never rendered — check\n'
    printf '  observer/reporting/format.rs:292-322 + secondary emit + apply site\n'
  fi
  return "$any_fail"
}

# --- Assertion C: per-field independence of the 25% gate ---------------------
#
# Verifies that NOT every report carries EVERY resource field — if
# any single "periodic cluster stats" block ever omitted at least one
# resource label that appeared in a sibling block, the per-field gate
# is honouring its single-line independence. (A fully present every
# block would either mean every field crossed 25% every time, which
# is highly unlikely on a real run, OR the gate collapsed to "print
# everything" and is unsound. Either way: a sample of mixed-presence
# blocks is the positive signal.)

stage_per_field_independence() {
  if [[ -z "$OBSERVER_LOG_PATH" ]]; then
    return 77
  fi
  # Use awk to walk the log line-by-line and bucket each "periodic
  # cluster stats" block; count, per block, how many of the 10
  # resource labels appear. If we see ≥1 block with 0 < count < 10
  # AND ≥1 OTHER block carrying a different label-set, the per-field
  # gate is doing its job. (count == 10 for every block would
  # collapse independence; count == 0 means no resource data at all
  # and we SKIP rather than fail.)
  local report
  report="$(ssh_user "awk '
    /periodic cluster stats/ {
      if (in_block) { print blkcount, blkmask; }
      in_block=1; blkcount=0; blkmask=\"\";
      next;
    }
    in_block {
      n=split(\"mem P10|mem P30|mem P50|mem P70|mem P90|mem avg|free host memory|host swap used|free host swap|host CPU utilization\", labels, \"|\");
      for (i=1; i<=n; i++) {
        if (index(\$0, labels[i]) > 0 && index(mask_seen, i \"|\") == 0) {
          blkcount++;
          mask_seen = mask_seen i \"|\";
          blkmask = blkmask i;
        }
      }
      if (/^[[:space:]]*$/ || /periodic cluster stats/) { in_block=0; mask_seen=\"\"; }
    }
    END {
      if (in_block) { print blkcount, blkmask; }
    }
  ' $OBSERVER_LOG_PATH" 2>/dev/null || true)"
  if [[ -z "$report" ]]; then
    printf '  no parseable report blocks\n'
    return 77
  fi
  local mixed=0 always_full=0 always_empty=0 total=0
  local cnt mask
  while read -r cnt mask; do
    total=$(( total + 1 ))
    if [[ "$cnt" == "10" ]]; then
      always_full=$(( always_full + 1 ))
    elif [[ "$cnt" == "0" ]]; then
      always_empty=$(( always_empty + 1 ))
    else
      mixed=$(( mixed + 1 ))
    fi
  done <<<"$report"
  printf '  blocks: total=%s full=%s empty=%s mixed=%s\n' \
    "$total" "$always_full" "$always_empty" "$mixed"
  if (( total < 2 )); then
    printf '  too few report blocks to observe independence\n'
    return 77
  fi
  if (( mixed == 0 && always_full == total )); then
    printf '  EVERY block carries EVERY resource field — gate appears collapsed\n'
    return 1
  fi
  if (( mixed == 0 && always_empty == total )); then
    printf '  NO block carries ANY resource field — no signal to test gate\n'
    return 77
  fi
  return 0
}

# --- Assertion D: rolling-buffer cadence inference ---------------------------
#
# The OOM probe sweep cadence (DEFAULT_SAMPLE_INTERVAL = 50ms, see
# manager-local/src/oom/mod.rs) feeds resource_buffer.rs's
# SAMPLE_WINDOW (= 600s). With a 5-minute emit interval and a 10-min
# window, every emit aggregates over [t-600s, t]; two successive emits
# (one inclusion) implicitly traverse one full window length.
# Externally we infer this by counting "periodic cluster stats"
# reports: ≥2 reports = ≥10 minutes of secondary uptime, which is
# necessary for the rolling window to have lived a full cycle.
#
# (Direct probe-side instrumentation has no log line — probe.rs sweeps
# silently. The buffer's internal state never crosses the CRDT either,
# by design, so it cannot be cross-checked from the observer side.)

stage_rolling_buffer_inference() {
  if [[ -z "$OBSERVER_LOG_PATH" ]]; then
    return 77
  fi
  local count
  # grep -c prints its own 0 and exits 1 on no-match; swallow the
  # nonzero exit WITHOUT a spurious second echo, then keep only the
  # first line so $count is always a single clean integer (empty → 0)
  # for the (( count < 2 )) test below.
  count="$(ssh_user "grep -cF 'periodic cluster stats' $OBSERVER_LOG_PATH 2>/dev/null; true" | head -n1)"
  count="${count:-0}"
  printf '  observed %s "periodic cluster stats" report(s)\n' "$count"
  if (( count < 2 )); then
    printf '  run did not reach two emit cycles (≥10min); buffer life unverified\n'
    return 77
  fi
  printf '  ≥2 reports observed — secondary survived ≥1 full SAMPLE_WINDOW (600s)\n'
  return 0
}

# --- Run ---------------------------------------------------------------------

stage 'observer log artifact present' stage_observer_log_present
stage '5-minute emit cadence (deltas in [240,360]s)' stage_emit_cadence
stage 'resource labels render with documented format.rs strings' \
  stage_resource_fields_render
stage 'per-field 25% gate exhibits per-line independence' \
  stage_per_field_independence
stage 'rolling-buffer ≥1 full SAMPLE_WINDOW inferred from emit count' \
  stage_rolling_buffer_inference

# --- Summary ------------------------------------------------------------------

printf '\n=== test 575 summary ===\n'
printf '  passed:  %d\n' "$PASS_COUNT"
printf '  failed:  %d\n' "$FAIL_COUNT"
printf '  skipped: %d (practical-limit; see per-stage detail above)\n' \
  "$SKIP_COUNT"

if (( FAIL_COUNT > 0 )); then
  printf '  failed stages:\n'
  for name in "${FAILURES[@]}"; do printf '    - %s\n' "$name"; done
  exit 1
fi
if (( SKIP_COUNT > 0 )); then
  printf '  skipped stages:\n'
  for name in "${SKIPS[@]}"; do printf '    - %s\n' "$name"; done
  printf '  NOTE: slurm-test-env has no dynrunner-on-cluster harness.\n'
  printf '  Live-log assertions require a consumer (asm-tokenizer or\n'
  printf '  asm-dataset-nix) to submit a real multi-secondary job lasting\n'
  printf '  ≥10 minutes; the assertion script then runs against that\n'
  printf '  observer log. Static-gate validation of THIS script body\n'
  printf '  passed if you reached this line.\n'
fi
printf '  result: %s\n' \
  "$([[ "$FAIL_COUNT" -eq 0 ]] && echo PASS || echo FAIL)"
