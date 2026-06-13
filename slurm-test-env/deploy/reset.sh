#!/usr/bin/env bash
# Reset the slurm-test-env state to a fresh post-provision baseline
# WITHOUT tearing the cluster down (so no slow image rebuild on the next
# run — use `down` for that). Safe whether the cluster is up or down.
#
# Wipes the two bind-mounted scratch surfaces an operator wants clean
# between runs:
#   - the shared /home (= $HOME_SHARE): every user's run artifacts (job
#     output, logs, caches, framework state) are removed, leaving ONLY
#     each user's home directory itself plus their SSH access (.ssh/) and
#     their cluster-UID marker (.cluster_uid). So a provisioned user can
#     still log in and keeps their UID — no re-provision needed — but
#     starts from an empty home. Stray top-level entries in /home are
#     removed too.
#   - the per-worker /tmp (= $WORKER_TMP_BASE/<worker>): contents cleared,
#     the bind-mount target dirs themselves kept so a running cluster's
#     mounts stay valid.
#
# `podman unshare` is required for both surfaces: nested rootless podman
# writes files owned by mapped subuids the host operator can't unlink
# directly (the same reason down.sh wipes /tmp under unshare).
#
# Why .cluster_uid is preserved (not "only the pubkey"): provision-user
# relies on it to re-derive a user's cluster UID across down/up cycles;
# dropping it would orphan the very files this reset just cleaned the next
# time the user is re-provisioned. It is part of the user's provisioned
# identity, not run output.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SLURM_TEST_ENV_ENV_FILE:-${SCRIPT_DIR}/env.sh}"
# shellcheck disable=SC1090
source "$ENV_FILE"

for arg in "$@"; do
  case "$arg" in
    -h|--help)
      cat <<EOF
usage: reset.sh

Clears run state to a fresh baseline WITHOUT stopping the cluster:
  - ${HOME_SHARE}
      each user's home is emptied except .ssh/ (SSH access) and
      .cluster_uid (stable UID); stray top-level entries are removed.
  - ${WORKER_TMP_BASE}/<worker>
      per-worker /tmp contents cleared (the dirs are kept).

Use \`down\` to stop the cluster entirely. Safe whether up or down.
EOF
      exit 0
      ;;
    *)
      printf 'Unknown argument: %q\n' "$arg" >&2
      exit 64
      ;;
  esac
done

# --- Safety: validate the derived rm targets before any removal -------------
#
# env.sh already guards INSTANCE_ID (non-empty, [a-zA-Z0-9_-]+), but the
# derived wipe targets are asserted to match their expected per-instance
# shape before a single file is touched — a malformed override of
# STATE_BASE_DIR / TMPDIR must never let a wipe escape the instance tree.

if [[ -z "${HOME_SHARE:-}" || "$HOME_SHARE" != */"${INSTANCE_ID}"/home ]]; then
  printf 'Refusing to reset: HOME_SHARE=%q does not match */%s/home.\n' \
    "${HOME_SHARE:-}" "$INSTANCE_ID" >&2
  exit 70
fi
if [[ -z "${WORKER_TMP_BASE:-}" || "$WORKER_TMP_BASE" != *"slurm-test-env-${INSTANCE_ID}" ]]; then
  printf 'Refusing to reset: WORKER_TMP_BASE=%q does not match *slurm-test-env-%s.\n' \
    "${WORKER_TMP_BASE:-}" "$INSTANCE_ID" >&2
  exit 70
fi

printf 'Resetting state for instance %s...\n' "$INSTANCE_ID"

# --- Shared /home + per-worker /tmp reset (one unshare) ----------------------
#
# Both surfaces hold subuid-mapped files, so the wipe runs inside a single
# `podman unshare` (the operator's user namespace, where those subuids are
# unlinkable — the same mechanism down.sh uses for /tmp). The program is
# deliberately conservative: per-entry iteration with an explicit, audited
# preserve set, so a glob or quoting slip cannot widen the deletion.
#
# PRESERVED in each user home: .ssh (SSH access) and .cluster_uid (stable
# cluster UID). Everything else under each home, every stray top-level
# entry directly in /home, and all per-worker /tmp contents are removed.
# The home directories and the per-worker /tmp dirs themselves are KEPT
# (the latter are live bind-mount targets while the cluster is up).

podman unshare bash -c '
  set -eu
  home_share="$1"
  worker_tmp_base="$2"

  if [ -d "$home_share" ]; then
    for entry in "$home_share"/* "$home_share"/.[!.]* "$home_share"/..?*; do
      [ -e "$entry" ] || continue
      if [ -d "$entry" ] && [ ! -L "$entry" ]; then
        # A user home: empty it except the SSH dir and the UID marker.
        find "$entry" -mindepth 1 -maxdepth 1 \
          ! -name .ssh ! -name .cluster_uid \
          -exec rm -rf -- {} +
      else
        # Stray top-level file/symlink directly under /home.
        rm -rf -- "$entry"
      fi
    done
  fi

  if [ -d "$worker_tmp_base" ]; then
    for wt in "$worker_tmp_base"/*/; do
      [ -d "$wt" ] || continue
      find "$wt" -mindepth 1 -delete
    done
    # Stray non-dir entries directly under the tmp base.
    for f in "$worker_tmp_base"/* "$worker_tmp_base"/.[!.]* "$worker_tmp_base"/..?*; do
      [ -e "$f" ] || continue
      if [ -d "$f" ] && [ ! -L "$f" ]; then
        continue
      fi
      rm -rf -- "$f"
    done
  fi
' _ "$HOME_SHARE" "$WORKER_TMP_BASE"

cat <<EOF

=== slurm-test-env :: state reset ===

  simulated /home:    ${HOME_SHARE}    (emptied; users + SSH access kept)
  per-worker /tmp:    ${WORKER_TMP_BASE}    (cleared)

EOF
