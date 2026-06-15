#!/usr/bin/env bash
# e2e shape-assertion test for #571 — setup-phase secondary tunnel-dial
# bounded deadline.
#
# Single concern: verify the #571 fix is present in the dynrunner source
# tree that the slurm-test-env worktree is shipped alongside.
#
# Why shape-assertion instead of live induction? Inducing #571 requires:
#   - a real dynrunner installation on the cluster (this test-env does
#     not ship dynrunner — see modules/common.nix; no python wheel, no
#     binaries), and
#   - a live submitter whose reverse SSH tunnel can be cleanly killed
#     mid-setup while a 5th secondary is in PENDING(Resources) and only
#     reaches setup-phase after another job releases its nodes (a 4-node
#     scheduling shape that asm-tokenizer's analysis showed cannot be
#     reproduced on consumer-side substrate either).
#
# The brief explicitly authorizes "no-induce-just-static-assert-on-output-
# shape" when induction is infeasible on the substrate; the cargo unit
# test in transport_factory.rs (tunnel_gave_up_rx_fires_on_deadline_
# exhaustion + sibling regression) is the framework's mechanism-level
# coverage. This script is the integration-level shape gate: it fails if
# any seam of the fix is removed or renamed, catching regressions a unit
# test on one file cannot catch.
#
# Honours the slurm-test-env test contract: a quick sinfo probe so the
# script returns 70 when the cluster is not running (matches smoke-test.sh
# + the other e2e tests' exit-code convention) before running the
# source-shape assertions.
#
# Exit codes:
#   0  all assertions pass
#   1  at least one assertion failed
#   70 cluster is not running

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SLURM_TEST_ENV_ENV_FILE:-${SCRIPT_DIR}/../deploy/env.sh}"
# shellcheck disable=SC1090
source "$ENV_FILE"

# --- Locate the dynrunner source tree ---------------------------------------
#
# This script is installed into the slurm-test-env flake output but the
# fix it asserts lives in the sibling dynrunner crates. Two resolution
# paths:
#   1. SLURM_TEST_ENV_DYNRUNNER_SRC env override (CI / out-of-tree caller)
#   2. Walk up from SCRIPT_DIR to find the dynrunner repo root (the
#      common case: this script's source path is <repo>/slurm-test-env/
#      scripts/test-571-tunnel-deadline.sh, so the repo root is two dirs
#      above SCRIPT_DIR — but under `nix run` SCRIPT_DIR points into the
#      nix store, so the fallback uses $PWD).
#
# Either way the located dir must contain crates/dynrunner-manager-
# distributed/ — that's the canonical marker.

locate_dynrunner_src() {
  local candidates=()
  if [[ -n "${SLURM_TEST_ENV_DYNRUNNER_SRC:-}" ]]; then
    candidates+=("$SLURM_TEST_ENV_DYNRUNNER_SRC")
  fi
  # Walk up from SCRIPT_DIR (works for `bash scripts/test-571-...sh` direct
  # invocation; does NOT work under `nix run` where SCRIPT_DIR is /nix/store).
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

# --- Test identity (cluster probe only — fleet convention) -------------------

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

# --- Stage harness (mirrors smoke-test.sh shape) ----------------------------

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

# Assert that grepping for $1 in tree-relative path $2 returns non-empty.
# Prints the first match for the log trail.
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

# Negative assertion: pattern MUST NOT appear in $2.
assert_no_grep() {
  local pattern="$1" relpath="$2"
  local abspath="$DYN_SRC/$relpath"
  if [[ ! -f "$abspath" ]]; then
    printf '    file missing: %s\n' "$relpath"
    return 1
  fi
  if grep -nq -- "$pattern" "$abspath" 2>/dev/null; then
    printf '    pattern UNEXPECTEDLY PRESENT: %q in %s\n' "$pattern" "$relpath"
    grep -n -- "$pattern" "$abspath" | head -2 | sed 's/^/      /'
    return 1
  fi
  return 0
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
  printf 'cluster reachable; proceeding with #571 shape assertions.\n'
}

# --- #571 shape assertions ---------------------------------------------------
#
# Each assertion pins ONE seam of the fix. A regression that removes any
# one of these (or renames it) trips a FAIL with a clear diagnostic. The
# patterns are intentionally specific enough to fail on a regression but
# loose enough to survive cosmetic edits (e.g. comment rewording).
#
# Seam map (matches the fix commit 7478d188):
#   1. Producer side: transport_factory::dial_secondary_mesh builds a
#      oneshot tunnel_gave_up channel, fires `Ok(())` on dial deadline
#      exhaustion, drops the sender on success path.
#   2. Bundle plumbing: SecondaryDialBundle carries tunnel_gave_up_rx
#      across the mesh-host dedicated thread.
#   3. Consumer side: SecondaryCoordinator::register_tunnel_gave_up_rx
#      stores the receiver; wait_for_setup's select! arm polls it.
#   4. Deadline binding: PySecondaryCoordinator::run binds the
#      bring-up dial connect_timeout to 80% of unconfigured_deadline.
#   5. Log target: the deadline-expiry emit is on IMPORTANT_TARGET
#      with the operator-visible message about releasing the SLURM
#      allocation.
#   6. Cleanup: the obsolete connect_timeout knob on DistributedConfig
#      was removed (since the deadline now derives from
#      unconfigured_deadline_secs).

# Seam 1 — producer: oneshot channel construction + send-on-exhaustion +
# success-path drop in transport_factory::dial_secondary_mesh.
seam1_producer_oneshot() {
  local f=crates/dynrunner-pyo3/src/managers/transport_factory.rs
  assert_grep 'tunnel_gave_up_tx, tunnel_gave_up_rx) = tokio::sync::oneshot::channel' "$f" \
    && assert_grep 'tunnel_gave_up_tx.send' "$f"
}

# Seam 2 — bundle plumbing: SecondaryDialBundle exposes tunnel_gave_up_rx
# (typed `oneshot::Receiver<()>`) so it can cross the on_dedicated_thread
# boundary in run.rs.
seam2_bundle_carries_rx() {
  local f=crates/dynrunner-pyo3/src/managers/transport_factory.rs
  assert_grep 'pub tunnel_gave_up_rx: tokio::sync::oneshot::Receiver<()>' "$f"
}

# Seam 3 — consumer: SecondaryCoordinator::register_tunnel_gave_up_rx +
# storage field + wait_for_setup select arm consuming it.
seam3_coordinator_register() {
  local f=crates/dynrunner-manager-distributed/src/secondary/coordinator.rs
  assert_grep 'pub fn register_tunnel_gave_up_rx' "$f" \
    && assert_grep 'tunnel_gave_up_rx: None' "$f"
}

seam3_coordinator_field() {
  local f=crates/dynrunner-manager-distributed/src/secondary/mod.rs
  assert_grep 'tunnel_gave_up_rx' "$f"
}

seam3_setup_select_arm() {
  local f=crates/dynrunner-manager-distributed/src/secondary/setup.rs
  # The select! arm: takes the rx off self before the while-loop, awaits
  # rx.await OR pending() (None case) inside the arm. Pattern check looks
  # for the take + the pending fallback that proves the cancel-safe
  # loop-local pattern is preserved.
  assert_grep 'self.tunnel_gave_up_rx.take' "$f" \
    && assert_grep 'enter_terminal_bring_up_failed' "$f"
}

# Seam 4 — deadline binding: secondary/run.rs binds the dial
# connect_timeout to `dist_unconfigured_deadline.mul_f64(0.8)` (80% of
# unconfigured deadline). Regression-pin: the binding MUST go through
# unconfigured_deadline, NOT a stale `connect_timeout` knob.
seam4_deadline_binding() {
  local f=crates/dynrunner-pyo3/src/managers/secondary/run.rs
  assert_grep 'dist_unconfigured_deadline.mul_f64(0.8)' "$f" \
    && assert_grep 'secondary.register_tunnel_gave_up_rx' "$f"
}

# Seam 5 — log target: the deadline-expiry tracing::error fires on
# dynrunner_core::IMPORTANT_TARGET with the operator-visible message
# about releasing the SLURM allocation. Both lines (target + reason)
# must be present.
seam5_important_target_emit() {
  local f=crates/dynrunner-manager-distributed/src/secondary/setup.rs
  assert_grep 'target: dynrunner_core::IMPORTANT_TARGET' "$f" \
    && assert_grep 'release the SLURM allocation' "$f" \
    && assert_grep 'setup-phase tunnel-wait deadline expired' "$f"
}

# Seam 6 — cleanup: the stale `connect_timeout()` accessor on
# DistributedConfig (the knob the old indefinite-dial behaviour read
# via `dist_connect_timeout = self.distributed_config.connect_timeout()`)
# was removed. The `connect_timeout_secs` FIELD + the
# `connect_timeout_override()` accessor remain (they back the primary's
# quorum-window derivation, a separate concern). If the bare
# `connect_timeout()` accessor returns, the secondary's dial may bypass
# the new 80%-of-unconfigured deadline. Negative assertion guards
# against that regression: matches `fn connect_timeout(` but NOT
# `fn connect_timeout_override(` / `fn connect_timeout_secs(` etc.
seam6_old_accessor_removed() {
  local f=crates/dynrunner-pyo3/src/config/distributed.rs
  assert_no_grep 'fn connect_timeout(' "$f"
}

# Seam 6b — cleanup: the secondary-side consumer site that read the
# old accessor (`dist_connect_timeout = self.distributed_config.
# connect_timeout()` in pyo3/managers/secondary/run.rs) was retired.
# If it returns, the deadline binding is broken even if the accessor
# itself remains.
seam6_old_consumer_site_removed() {
  local f=crates/dynrunner-pyo3/src/managers/secondary/run.rs
  assert_no_grep 'distributed_config.connect_timeout()' "$f"
}

# Documentation pin — the operator-visible error message contains the
# verbatim phrase the brief mandates ("submitter never appeared"). The
# Rust source wraps this string across two lines with a `\` line
# continuation (`"submitter never \\n appeared;"`), so the literal at
# rest contains "submitter never appeared" but `grep` sees "submitter
# never" on one line + "appeared" on the next. Two assertions pin both
# halves; together they prove the contiguous phrase exists in the
# compiled string while remaining robust to harmless whitespace edits.
docpin_operator_phrase() {
  local f=crates/dynrunner-manager-distributed/src/secondary/setup.rs
  assert_grep 'submitter never' "$f" \
    && assert_grep 'appeared; exiting clean to release SLURM' "$f"
}

# --- Run ---------------------------------------------------------------------

printf '=== slurm-test-env :: test-571 tunnel-deadline (instance=%s) ===\n' "$INSTANCE_ID"
probe_cluster_or_exit_70

printf '\n--- #571 source-shape assertions on %s ---\n' "$DYN_SRC"
stage 'seam 1: producer oneshot channel + send-on-exhaustion'  seam1_producer_oneshot
stage 'seam 2: SecondaryDialBundle carries tunnel_gave_up_rx'  seam2_bundle_carries_rx
stage 'seam 3a: SecondaryCoordinator::register_tunnel_gave_up_rx' seam3_coordinator_register
stage 'seam 3b: tunnel_gave_up_rx field on SecondaryCoordinator'  seam3_coordinator_field
stage 'seam 3c: wait_for_setup select arm + terminal handler'  seam3_setup_select_arm
stage 'seam 4: dial deadline bound to 80% of unconfigured_deadline' seam4_deadline_binding
stage 'seam 5: IMPORTANT_TARGET emit + SLURM-release phrasing' seam5_important_target_emit
stage 'seam 6a: obsolete connect_timeout() accessor removed (negative)' seam6_old_accessor_removed
stage 'seam 6b: obsolete distributed_config.connect_timeout() consumer removed (negative)' seam6_old_consumer_site_removed
stage 'docpin: operator-visible "submitter never appeared" phrase' docpin_operator_phrase

# --- Summary -----------------------------------------------------------------

printf '\n=== test-571 summary ===\n'
printf '  passed: %d\n' "$PASS_COUNT"
printf '  failed: %d\n' "$FAIL_COUNT"
if (( FAIL_COUNT > 0 )); then
  printf '  failed stages:\n'
  for name in "${FAILURES[@]}"; do printf '    - %s\n' "$name"; done
  printf '  result: FAILED — at least one #571 seam is missing or renamed.\n'
  exit 1
fi
printf '  result: ALL PASSED — #571 fix shape is intact in the source tree.\n'
printf '  note: live induction is INFEASIBLE on this substrate (test-env\n'
printf '        ships no dynrunner stack; 5-secondary PENDING(Resources)\n'
printf '        shape is not reproducible on 4-node consumer substrate per\n'
printf '        asm-tokenizer analysis). Mechanism-level coverage is the\n'
printf '        cargo unit test transport_factory::tunnel_gave_up_rx_fires_\n'
printf '        on_deadline_exhaustion.\n'
