//! [`generate_wrapper_script`]: the canonical secondary-mode wrapper
//! generator. ~670 lines of sequentially-built bash heredocs spanning
//! scratch-dir setup, podman storage, FIFO command-relay,
//! conmon-watchdog fallback, image load, container run, and cleanup
//! traps. Splitting further would fracture the linearly-constructed
//! bash payload (each section depends on shell variables defined
//! upstream); the file sits above the 300-line target because the
//! script body it emits is itself one cohesive bash program. See
//! [`super`] for the higher-level rationale.

use super::config::{ConnectionMode, WrapperScriptConfig, WRAPPER_SRC_NETWORK_CONTAINER_PATH};
use super::quote::{bash_quote, rand_hex8};

/// Generate the bash wrapper script for a SLURM job.
///
/// The script sets up scratch /tmp dirs, podman storage, the FIFO
/// command relay, the conmon-watchdog fallback, loads the docker
/// image, and runs the container in the requested connection mode.
pub fn generate_wrapper_script(cfg: &WrapperScriptConfig<'_>) -> String {
    let rnd_suffix = rand_hex8();
    let rndtmp = format!("/tmp/asm-{rnd_suffix}");
    let container_name = format!("asm-{rnd_suffix}-{}", cfg.secondary_id);

    let src_tmp = format!("{rndtmp}/src");
    let out_tmp = format!("{rndtmp}/out");
    let log_tmp = format!("{rndtmp}/log");
    let podman_storage = format!("{rndtmp}/storage");
    let podman_run = format!("{rndtmp}/run");
    let socket_dir = format!("{rndtmp}/sockets");
    let cmd_socket = format!("{socket_dir}/cmd.sock");

    let srcbins_network = cfg
        .srcbins_mount_source
        .map(String::from)
        .unwrap_or_else(|| cfg.slurm_config.src_bins_path());
    let output_network = cfg
        .output_dir
        .map(String::from)
        .unwrap_or_else(|| cfg.slurm_config.output_path());
    let log_network = cfg
        .run_log_dir
        .map(String::from)
        .unwrap_or_else(|| cfg.slurm_config.log_path());

    // Optional dynrunner-network volume/env block. When absent the
    // strings are empty and collapse cleanly inside the podman-run
    // continuation lines.
    let (dynrunner_volume_block, dynrunner_env_block, dynrunner_echo_block) =
        match cfg.dynrunner_network_dir {
            Some(dir) => (
                format!("    -v \"{dir}:/app/dynrunner-network\" \\\n"),
                "    -e DYNRUNNER_NETWORK=\"/app/dynrunner-network\" \\\n".to_string(),
                format!("echo \"    {dir} -> /app/dynrunner-network\""),
            ),
            None => (String::new(), String::new(), "true".to_string()),
        };

    // Bash-quote each consumer-supplied flag so values containing
    // spaces or shell-metacharacters survive intact, then render as
    // one continuation line per arg so the resulting `podman run`
    // block keeps the same readable shape regardless of how many
    // flags the consumer passes. Empty slice → empty string, which
    // collapses cleanly between the env+volume block and the
    // image-tag line.
    let extra_run_args_block: String = cfg
        .extra_run_args
        .iter()
        .map(|arg| format!("    {} \\\n", bash_quote(arg)))
        .collect();

    let mut script = format!(
        r##"#!/usr/bin/env bash
set -e

echo "=================================================="
echo "SLURM Secondary Job Starting"
echo "Node: $(hostname)"
echo "Job ID: $SLURM_JOB_ID"
echo "Time: $(date)"
echo "=================================================="

RNDTMP="{rndtmp}"
echo "Creating temporary directory: $RNDTMP"
mkdir -p "$RNDTMP"
mkdir -p "{src_tmp}" "{out_tmp}" "{log_tmp}" "{socket_dir}"

cleanup() {{
    # Terminate the command-relay subshell and WAIT for it to exit
    # before removing its FIFO. Without `wait`, kill is racy with
    # the rm-rf below — and the relay loop is designed to exit 1
    # with a loud diagnostic if its FIFO disappears unexpectedly
    # (so a careless ops mistake gets noticed instead of silently
    # neutering the secondary). During intentional cleanup we don't
    # want that diagnostic; we want the subshell killed cleanly via
    # SIGTERM before the FIFO vanishes.
    # `${{CMD_RELAY_PID:-}}` guard handles early-failure paths where
    # the relay was never started.
    if [ -n "${{CMD_RELAY_PID:-}}" ]; then
        kill -TERM "$CMD_RELAY_PID" 2>/dev/null || true
        wait "$CMD_RELAY_PID" 2>/dev/null || true
    fi
    echo "Cleaning up temporary directory: $RNDTMP"
    # Per-file unlink for the image tarball: it's host-UID owned
    # (cp'd in by this wrapper before any container ran), so a
    # plain `rm -f` reaches it without entering the user-namespace.
    # Guarded with `${{LOCAL_IMAGE:-}}` because the trap is installed
    # before LOCAL_IMAGE gets assigned, so early-failure paths
    # (port probe, etc.) reach cleanup with the variable unset.
    # `--` stops accidental flag interpretation. The "rm -rf in
    # scripts is dangerous" rule pushes us toward per-file unlink
    # wherever feasible — recursive tree-rm is reserved for
    # $RNDTMP itself, where there's no other mechanism.
    if [ -n "${{LOCAL_IMAGE:-}}" ] && [ -e "$LOCAL_IMAGE" ]; then
        rm -f -- "$LOCAL_IMAGE" 2>/dev/null \
            || echo "WARNING: failed to unlink $LOCAL_IMAGE" >&2
    fi
    # Tmp-tree teardown via `podman unshare`: rootless podman
    # writes files into $RNDTMP/storage owned by mapped subuids
    # the host operator's UID can't unlink directly (this leaked
    # ~3.2 GB per run on slurm-test-env workers — bug AA in the
    # field log). `podman unshare` enters the user-namespace where
    # those files are reachable. Plain `rm -rf` is a defensive
    # fallback for the no-podman case; shouldn't fire on real
    # workers but keeps the wrapper safe to dry-run on a host
    # without podman installed. `--` guards against $RNDTMP ever
    # starting with a dash. Result is logged after the rm
    # completes so a silent leak is impossible to miss in logs.
    if podman unshare rm -rf -- "$RNDTMP" 2>/dev/null \
        || rm -rf -- "$RNDTMP" 2>/dev/null; then
        echo "Cleaned up temporary directory: $RNDTMP"
    else
        echo "ERROR: failed to clean up $RNDTMP — /tmp scratch leaked on $(hostname)" >&2
    fi
}}
# Also cleanup on SLURM-induced signals: SIGTERM is sent by sbatch
# at time-limit / scancel, SIGHUP by an ssh disconnect, SIGINT by
# Ctrl+C from interactive jobs. Without these, the trap fires only
# on graceful exit and SLURM-killed jobs leak /tmp/asm-XXXX dirs
# until the node's /tmp fills (observed in the field on multi-day
# clusters). EXIT alone misses every non-graceful termination.
trap cleanup EXIT TERM HUP INT

PODMAN_STORAGE="{podman_storage}"
PODMAN_RUN="{podman_run}"
mkdir -p "$PODMAN_STORAGE" "$PODMAN_RUN"
chmod 700 "$PODMAN_STORAGE" "$PODMAN_RUN"
export XDG_RUNTIME_DIR="$PODMAN_RUN"
echo "Podman storage: $PODMAN_STORAGE"
echo "Podman run root: $PODMAN_RUN"
echo "XDG_RUNTIME_DIR: $XDG_RUNTIME_DIR"
echo ""

# ============================================================================
# Pre-flight: graceful-stop any podman containers left running under the
# current user from prior dispatches on this compute node.
# ============================================================================
#
# Why this exists: ungraceful SLURM job termination (preemption, time-limit
# SIGKILL after the script's TERM trap missed, node reboot) leaves the
# wrapper's per-job ``$PODMAN_STORAGE`` (under /tmp/asm-XXXX/storage) on
# disk with its container still running, supervised by an orphan conmon
# process that no parent will ever reap. Field-observed pattern
# (asm-tokenizer 2026-05-16): 16 orphan containers on a 40-node cluster,
# one alive 7+ hours, actively writing into the network output volume
# alongside live dispatches and corrupting data. Default-storage
# ``podman ps`` does NOT see these — they're in orphan per-job
# ``$PODMAN_STORAGE`` roots ``podman`` was never told about. Recovery
# required a 1.167 TiB manual sweep across all 40 nodes (host-side
# ``find /tmp -name 'asm-*'`` + per-orphan ``podman --root X --runroot Y
# stop+rm`` + ``unshare`` mode rewrites for the rootless-subuid layers).
#
# What this does: enumerate every ``/tmp/*/storage`` directory owned by
# this user (the wrapper's per-job storage shape), graceful-stop running
# containers there, then ``podman rm -af`` so the orphan exited
# containers no longer hold storage layers. Also scans the user-default
# rootless storage for symmetry — same operation, no harm if empty.
# Skipped via ``DYNRUNNER_DISABLE_PREFLIGHT_PODMAN=1`` for the rare
# operator who needs to keep prior containers running (mid-job
# diagnostics).
#
# Why graceful (-t 10) rather than ``podman kill``: per user spec
# (``--oom-pressure-threshold`` PR thread on 2026-05-17). 10s grace
# lets the orphan's process tree flush bind-mount writes and final
# logs before the SIGKILL fallback.
#
# Why the current job's own ``$PODMAN_STORAGE`` is harmless to scan:
# it was just ``mkdir -p``'d empty above. ``podman ps`` returns nothing
# there; ``podman rm -af`` is a no-op.
if [ "${{DYNRUNNER_DISABLE_PREFLIGHT_PODMAN:-0}}" = "1" ]; then
    echo "Pre-flight podman cleanup: skipped (DYNRUNNER_DISABLE_PREFLIGHT_PODMAN=1)"
else
    echo "Pre-flight: scanning for leftover podman containers..."
    preflight_found=0
    # Phase 1: orphan per-job storage roots under /tmp/.
    for orphan_storage in /tmp/*/storage; do
        [ -d "$orphan_storage" ] || continue
        [ -O "$orphan_storage" ] || continue
        orphan_runroot="${{orphan_storage%/storage}}/run"
        # Running containers: graceful stop with 10s grace.
        orphan_running=$(podman --root "$orphan_storage" --runroot "$orphan_runroot" --cgroup-manager=cgroupfs ps -q 2>/dev/null || true)
        if [ -n "$orphan_running" ]; then
            preflight_found=1
            echo "Pre-flight: stopping containers in $orphan_storage: $orphan_running"
            podman --root "$orphan_storage" --runroot "$orphan_runroot" --cgroup-manager=cgroupfs stop -t 10 $orphan_running 2>/dev/null || true
        fi
        # All containers (including stopped/exited): remove to release
        # storage layers. The peer-documented leak: exited containers
        # held the network-output bind mount open even after the
        # process died; only ``rm`` releases those references.
        podman --root "$orphan_storage" --runroot "$orphan_runroot" --cgroup-manager=cgroupfs rm -af 2>/dev/null || true
    done
    # Phase 2: user-default rootless storage. Same operations; covers
    # operators who run ad-hoc ``podman`` without ``--root``.
    default_running=$(podman ps -q 2>/dev/null || true)
    if [ -n "$default_running" ]; then
        preflight_found=1
        echo "Pre-flight: stopping containers in default storage: $default_running"
        podman stop -t 10 $default_running 2>/dev/null || true
    fi
    podman rm -af 2>/dev/null || true
    if [ "$preflight_found" = "1" ]; then
        echo "Pre-flight: cleaned up leftover containers"
    else
        echo "Pre-flight: no leftover containers"
    fi
fi
echo ""

# Cap container memory at MIN(host MemTotal - 2GiB, wrapper-cgroup-memory-max)
# so a runaway worker hits a graceful container-OOM (just kills the
# worker process) instead of a host kernel-OOM that wedges the cgroup
# and leaves zombie SLURM jobs stuck COMPLETING.
#
# Two probes feed the cap:
#   1. /proc/meminfo:MemTotal — node-wide physical RAM. Always shows
#      the HOST's MemTotal even from inside a container/cgroup;
#      represents the upper bound on what the kernel can give us.
#   2. /sys/fs/cgroup/memory.max — the wrapper's own cgroup v2 memory
#      cap. SLURM with TaskPlugin=task/cgroup sets this per-job;
#      podman's own slurm-worker container also caps the slurmd
#      process tree (`WORKER_MEMORY` knob on slurm-test-env).
#      The literal string "max" means "no cap at this level"; any
#      numeric value is the active cap in bytes.
#
# Taking the min ensures we never tell podman the secondary container
# can have more RAM than its parent cgroup actually permits. Pre-fix
# the wrapper only consulted /proc/meminfo and on slurm-test-env
# (NodeRAM=96GiB but WORKER_MEMORY=4GiB) advertised `--memory=94GiB`
# to podman; inside the secondary container `/sys/fs/cgroup/memory.max`
# then reflected 94 GiB, the framework's `detect_total_memory_bytes`
# read that as the budget, workers allocated 90+ GiB each, and the
# kernel-enforced 4 GiB outer cap OOM-killed them on first nix-build
# fork burst — surfacing as `Broken pipe (os error 32)` on the
# nix-daemon socket (asm-dataset-nix T3 at 3c5f105). The min-with-
# cgroup-max fix closes this.
#
# --memory-swap=-1 (unlimited swap on top of the RAM cap) is the
# explicit user policy: a worker that overshoots its RAM budget
# should get swapped instead of OOM-killed. Reasoning: workers
# that "waste" memory in bursts can recover under pressure
# (swap-thrash is slow but observable) rather than dying
# abruptly. The RAM cap (--memory) still bounds in-core usage
# from the kernel's perspective; --memory-swap=-1 just unbounds
# the swap component of that ceiling so the container's effective
# memory.max becomes max(RAM cap + host swap, RAM cap). Without
# this, podman's default `--memory-swap=2*--memory` (or
# `--memory-swap=--memory` if we set it equal) causes the
# kernel cgroup-OOM-killer to fire as soon as actual RAM usage
# crosses the cap — losing the worker's progress and potentially
# triggering the bilateral-OOM-kill-by-cgroup pattern asm-dataset-nix
# diagnosed at afd1654. Slurm-test-env-owner's 929a7b9 does the
# parallel change on the outer worker container; this commit
# does it on the framework's inner secondary container.
#
# Falls back to no cap when both probes yield empty (absurdly
# small node and no cgroup cap — implausible on cluster).
MEM_BYTES_NODE=$(awk '/MemTotal:/{{val = $2*1024 - 2*1024*1024*1024; if (val > 0) print val; else print ""}}' /proc/meminfo)
MEM_BYTES_CGROUP=$(cat /sys/fs/cgroup/memory.max 2>/dev/null || echo "")
case "$MEM_BYTES_CGROUP" in
    ""|max) MEM_BYTES_CGROUP="" ;;
    *[!0-9]*) MEM_BYTES_CGROUP="" ;;  # defend against unexpected shapes
esac
if [ -n "${{MEM_BYTES_NODE}}" ] && [ -n "${{MEM_BYTES_CGROUP}}" ]; then
    if [ "${{MEM_BYTES_NODE}}" -lt "${{MEM_BYTES_CGROUP}}" ]; then
        MEM_BYTES="${{MEM_BYTES_NODE}}"
        MEM_SOURCE="host MemTotal - 2GiB (tighter than cgroup ${{MEM_BYTES_CGROUP}})"
    else
        MEM_BYTES="${{MEM_BYTES_CGROUP}}"
        MEM_SOURCE="wrapper cgroup memory.max (tighter than host-MemTotal-2GiB ${{MEM_BYTES_NODE}})"
    fi
elif [ -n "${{MEM_BYTES_NODE}}" ]; then
    MEM_BYTES="${{MEM_BYTES_NODE}}"
    MEM_SOURCE="host MemTotal - 2GiB (no cgroup cap detected)"
elif [ -n "${{MEM_BYTES_CGROUP}}" ]; then
    MEM_BYTES="${{MEM_BYTES_CGROUP}}"
    MEM_SOURCE="wrapper cgroup memory.max (host-MemTotal probe failed)"
else
    MEM_BYTES=""
fi
if [ -n "${{MEM_BYTES}}" ]; then
    MEM_FLAGS="--memory=${{MEM_BYTES}} --memory-swap=-1"
    echo "Container memory cap: ${{MEM_BYTES}} bytes RAM + unlimited swap (${{MEM_SOURCE}})"
else
    MEM_FLAGS=""
    echo "Container memory cap: disabled (host-MemTotal and cgroup probes both empty)"
fi
echo ""

# Resolve the compute node's peer-routable IPs so the secondary
# advertises addresses other cluster nodes can actually dial. The
# container runs with `--network host` so it shares this node's
# network namespace, but `hostname -I` in there still returns
# *every* configured non-loopback address — and on Krater-class
# nodes the first one is often a CNI bridge / podman-internal
# subnet (10.x.x.x) that's not routed off-host. Resolving the
# node's FQDN through NSS picks the canonical cluster address that
# slurmd, ssh, and DNS all agree on. Empty values are tolerated by
# the Rust env-hint reader (see network::detect_ipv4); a probe
# failure simply falls back to the legacy `hostname -I` first-token.
SLURM_NODE_NAME="${{SLURMD_NODENAME:-$(hostname -f)}}"
PRIMARY_NODE_IPV4=$(getent ahostsv4 "$SLURM_NODE_NAME" 2>/dev/null | awk '{{print $1; exit}}')
PRIMARY_NODE_IPV6=$(getent ahostsv6 "$SLURM_NODE_NAME" 2>/dev/null | awk '$1 ~ /:/ {{print $1; exit}}')
echo "Peer-routable IPv4: ${{PRIMARY_NODE_IPV4:-<unresolved, will fall back to hostname -I>}}"
echo "Peer-routable IPv6: ${{PRIMARY_NODE_IPV6:-<unresolved, will fall back to hostname -I or skip>}}"
echo ""
"##
    );

    // Connection-mode-specific port allocation
    match &cfg.connection {
        ConnectionMode::Reverse { connection_info_dir } => {
            let sid = cfg.secondary_id;
            let is_observer = if cfg.is_observer { "true" } else { "false" };
            script.push_str(&format!(
                r##"
echo "Finding free ports on compute node..."
TUNNEL_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('', 0)); print(s.getsockname()[1]); s.close()")
QUIC_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('', 0)); print(s.getsockname()[1]); s.close()")
echo "Using tunnel port: $TUNNEL_PORT"
echo "Using QUIC port: $QUIC_PORT"

HOSTNAME=$(hostname -f)
mkdir -p "{connection_info_dir}"
# Peer-info file format (dynrunner-slurm/src/peer_info.rs):
#   Line 1 — legacy `<scheme>://<host>:<port>` URI for the SSH
#            reverse-tunnel target (back-compat: a v1 reader that
#            only knows about line 1 keeps working unchanged).
#   Lines 2+ — `key=value` envelope (v2). Consumed by a late-joining
#              observer's bootstrap reader (`peer_info::parse`) which
#              needs more than just the tunnel host:port to dial
#              the peer mesh.
#
# `cert_pem_b64` is intentionally omitted here: the cert is generated
# inside the secondary container at startup (during CertExchange),
# not at wrapper-render time. The secondary itself rewrites this
# file post-cert via the `dynrunner_slurm::peer_info::Builder` API
# (Step 8 of the transport-unification refactor).
{{
    printf 'tcp://%s:%s\n' "$HOSTNAME" "$TUNNEL_PORT"
    printf 'version=2\n'
    printf 'secondary_id=%s\n' '{sid}'
    if [ -n "${{PRIMARY_NODE_IPV4}}" ]; then printf 'ipv4=%s\n' "$PRIMARY_NODE_IPV4"; fi
    if [ -n "${{PRIMARY_NODE_IPV6}}" ]; then printf 'ipv6=%s\n' "$PRIMARY_NODE_IPV6"; fi
    printf 'quic_port=%s\n' "$QUIC_PORT"
    printf 'is_observer={is_observer}\n'
}} > "{connection_info_dir}/{sid}.info"
echo "Connection info written to: {connection_info_dir}/{sid}.info"
"##
            ));
        }
        ConnectionMode::Standard { .. } => {
            script.push_str(
                r##"
echo "Finding free port for QUIC server..."
QUIC_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('', 0)); print(s.getsockname()[1]); s.close()")
echo "Using QUIC port: $QUIC_PORT"
"##,
            );
        }
    }

    // FIFO command relay + image load.
    script.push_str(&format!(
        r##"
echo "Starting command relay service..."
SOCKET_COUNTER=0
{{
    rm -f "{cmd_socket}" "{cmd_socket}.response"
    mkfifo "{cmd_socket}"
    mkfifo "{cmd_socket}.response"
    while true; do
        if read -r CMD < "{cmd_socket}"; then
            if [ -n "$CMD" ]; then
                SOCKET_COUNTER=$((SOCKET_COUNTER + 1))
                OUTPUT_SOCK="{socket_dir}/output_${{SOCKET_COUNTER}}.sock"
                EXIT_SOCK="{socket_dir}/exit_${{SOCKET_COUNTER}}.sock"
                SIGNAL_SOCK="{socket_dir}/signal_${{SOCKET_COUNTER}}.sock"
                mkfifo "$OUTPUT_SOCK" "$EXIT_SOCK" "$SIGNAL_SOCK"
                {{
                    eval "$CMD" > "$OUTPUT_SOCK" 2>&1
                    EXIT_CODE=$?
                    rm -f "$OUTPUT_SOCK"
                    echo "$EXIT_CODE" > "$EXIT_SOCK"
                    rm -f "$EXIT_SOCK"
                }} &
                CMD_PID=$!
                {{
                    if read -r SIGNAL < "$SIGNAL_SOCK"; then
                        if [ -n "$SIGNAL" ]; then
                            kill -$SIGNAL $CMD_PID 2>/dev/null || true
                        fi
                    fi
                    rm -f "$SIGNAL_SOCK"
                }} &
                echo "output_${{SOCKET_COUNTER}}.sock,exit_${{SOCKET_COUNTER}}.sock,signal_${{SOCKET_COUNTER}}.sock,$CMD_PID" > "{cmd_socket}.response"
            fi
        elif [ ! -p "{cmd_socket}" ]; then
            # FIFO disappeared with no SIGTERM from cleanup() — that's
            # corrupt state (external rm, filesystem eviction, etc.),
            # not a normal lifecycle event. Bail loud so the failure is
            # diagnosable instead of silently neutering the secondary.
            # During intentional cleanup, the trap's kill+wait sequence
            # exits this subshell via signal before the FIFO vanishes,
            # so this branch only fires on genuine unexpected loss.
            echo "ERROR: command relay FIFO {cmd_socket} disappeared unexpectedly; secondary cannot continue." >&2
            exit 1
        fi
    done
}} &
CMD_RELAY_PID=$!

echo "Copying image to local temp directory..."
LOCAL_IMAGE="$RNDTMP/{image_tar_basename}"
cp "{image_path}" "$LOCAL_IMAGE"
echo "Image copied to: $LOCAL_IMAGE"

echo "Loading image into container runtime..."
# Wrap the load command in an explicit failure check so the abort
# surfaces as a clear marker on STDOUT (the .out file consumers
# check first), not just an opaque set-e exit between the
# "Loading…" line and the cleanup trap. The container runtime's
# own stderr still ends up in the .err file as before.
if ! {load_command}; then
    echo "ERROR: image load failed; secondary cannot start. See the .err file for the runtime's diagnostic."
    echo "ERROR: image load failed; secondary cannot start." >&2
    exit 1
fi
echo "Image loaded successfully"

CONTAINER_NAME="{container_name}"

# Detached fallback teardown for the conmon-double-fork-escapes-
# cgroup case observed in the field: when SLURM proctrack/cgroup
# either isn't in use or doesn't track the container monitor's
# detached pid, scancel/timeout/SIGTERM doesn't propagate into
# the container — conmon and its children survive both the
# wrapper's death and the SLURM job's termination, leaking
# storage and worker processes on the compute node.
#
# Two trigger conditions must BOTH hold before any kill action:
#
#   1. SLURM job state is no longer RUNNING. Probed via
#      `squeue -j $JOBID -h -o "%T"` — verbose state code per
#      `squeue(1)`. A job leaving RUNNING means slurmctld has
#      decided to terminate it (COMPLETING after time limit /
#      scancel, FAILED, TIMEOUT, CANCELLED) or the job has
#      already left the queue (empty stdout). Crucially, a job
#      in COMPLETING (CG) state — slurmctld trying to clean up
#      a stuck cgroup, often because the container itself is
#      blocking cleanup — is the case this watchdog exists for.
#      Polling for state, not presence, catches that case;
#      presence-polling would miss it because the job is still
#      in the queue.
#
#   2. Container is still alive. If the dispatcher exited
#      cleanly with the workload, the container is gone and
#      there is nothing to tear down.
#
# Teardown is graceful: SIGTERM first, then up to 60s for the
# dispatcher to flush in-flight task state, peer disconnects,
# and exit; only if the container is still alive after 60s does
# the watchdog escalate to SIGKILL. This preserves the
# dispatcher's ability to surface partial results when the job
# is terminated mid-run rather than discarding them.
#
# The watchdog never issues `podman rm`. The container is
# started with `podman run --rm`, so a clean exit (whether from
# SIGTERM, SIGKILL, or natural workload completion) auto-removes
# it. If a container somehow survives SIGKILL that is a
# runtime/kernel issue worth surfacing in slurm_*.out, not
# papering over with `rm -f`.
#
# Debounce: two consecutive non-running observations at 5s
# interval (10s confirm) before triggering. slurmctld can
# return transient state inconsistencies during RPC stalls or
# accounting flushes; the debounce rides those out. squeue
# command failures (rc!=0) are skipped entirely — they
# indicate slurmctld is unreachable, not that the job ended.
#
# Watchdog actions log to the wrapper's stdout via fd 3 (the
# wrapper redirects its own stdout to the .out file before the
# watchdog spawns; the watchdog inherits fd 3 → same .out
# file). Operators grepping `slurm_*.out` for "WATCHDOG:" can
# attribute container teardown to the watchdog post-hoc.
#
# Detached via `setsid -f` so it survives wrapper exit and
# (where possible) cgroup teardown of the wrapper's pidtree.
# If proctrack/cgroup is in fact tracking the watchdog too,
# the watchdog dies alongside the rest — but in that case the
# container is also dead, so there's nothing to clean up.
#
# Skipped when SLURM_JOB_ID is empty (running outside SLURM):
# squeue would never find a matching job so the watchdog could
# never exit cleanly.
#
# Operator escape hatch: setting `DYNRUNNER_DISABLE_TEARDOWN_WATCHDOG=1`
# in the wrapper's environment skips spawning the watchdog
# entirely. Two use cases:
#   - **A/B diagnostic** when an operator suspects the watchdog
#     is the source of a phantom SIGTERM (the kill target is
#     PID 1 of the container, but operators inspecting a
#     cross-cluster signal source may want to rule it out
#     definitively).
#   - **Healthy proctrack/cgroup clusters** where SLURM's own
#     cgroup teardown reliably reaps conmon's double-forked
#     containers and the watchdog is redundant. Pre-2026-04-29
#     watchdog's only purpose was the conmon-escape-cgroup
#     fallback; on a cluster where proctrack/cgroup catches
#     that case (confirmed working on slurm-test-env per
#     2bf8410), the watchdog is belt-and-suspenders only.
#
# Default behaviour (env var unset or set to "0") is
# unchanged — watchdog spawns and runs the state-aware
# polling.
if [ -n "${{SLURM_JOB_ID:-}}" ] \
   && [ "${{DYNRUNNER_DISABLE_TEARDOWN_WATCHDOG:-0}}" != "1" ]; then
    # Duplicate the wrapper's stdout to fd 3 so the detached
    # watchdog can log its actions to the .out file even
    # after the wrapper exits and its main stdout chain may be
    # torn down (setsid'd subshell's stdout goes to /dev/null
    # via the spawn line). The exec is bash-local; the
    # subshell inherits open fds.
    exec 3>&1
    setsid -f bash -c '
        job_id="$1"
        cname="$2"
        storage="$3"
        runroot="$4"
        # State-poll cadence and debounce. 5s sleep × 2-strike
        # confirm = ~10s to act after a real state change, which
        # is fast enough on the operator side and long enough to
        # ride out slurmctld inconsistencies.
        state_poll_seconds=5
        state_threshold=2
        nonrunning_count=0
        last_state="<unknown>"
        while true; do
            sleep "$state_poll_seconds"
            # %T renders the verbose job state (RUNNING, COMPLETING,
            # FAILED, TIMEOUT, CANCELLED, PREEMPTED, ...). Empty
            # stdout means the job has left the queue entirely.
            # squeue rc!=0 indicates slurmctld is unreachable, not
            # that the job ended — skip those ticks.
            if out=$(squeue -j "$job_id" -h -o "%T" 2>/dev/null); then
                state=$(printf "%s" "$out" | head -n1)
                if [ "$state" = "RUNNING" ] || [ "$state" = "R" ]; then
                    nonrunning_count=0
                else
                    nonrunning_count=$((nonrunning_count + 1))
                    if [ "$nonrunning_count" -ge "$state_threshold" ]; then
                        last_state="${{state:-<empty>}}"
                        break
                    fi
                fi
            fi
        done

        # Container already gone — workload finished, nothing
        # to do.
        if ! podman --root "$storage" --runroot "$runroot" --cgroup-manager=cgroupfs container exists "$cname" 2>/dev/null; then
            exit 0
        fi

        # Phase 2: graceful SIGTERM. The dispatcher inside gets a
        # chance to flush in-flight state, signal peers, and exit
        # cleanly before we escalate. The container was started
        # with `podman run --rm`, so a clean exit auto-removes
        # the container; the watchdog never issues `podman rm`.
        grace_seconds=60
        echo "WATCHDOG: job $job_id state=$last_state; sending SIGTERM to container $cname (${{grace_seconds}}s grace before SIGKILL)" >&3 2>/dev/null || true
        podman --root "$storage" --runroot "$runroot" --cgroup-manager=cgroupfs kill --signal TERM "$cname" 2>/dev/null

        # Phase 3: wait up to grace_seconds for graceful exit.
        # Poll once a second so we exit promptly when the
        # dispatcher finishes. `container exists` returning non-
        # zero means `--rm` has already reaped the container,
        # which is the only termination path we care about.
        elapsed=0
        while [ "$elapsed" -lt "$grace_seconds" ]; do
            sleep 1
            elapsed=$((elapsed + 1))
            if ! podman --root "$storage" --runroot "$runroot" --cgroup-manager=cgroupfs container exists "$cname" 2>/dev/null; then
                echo "WATCHDOG: container $cname exited gracefully after ${{elapsed}}s" >&3 2>/dev/null || true
                exit 0
            fi
        done

        # Phase 4: still alive after grace — escalate to SIGKILL.
        # SIGKILL cannot be trapped; the kernel terminates pid 1
        # in the container namespace and --rm reaps the container.
        # No `podman rm`: if a container somehow survives SIGKILL
        # that is a runtime/kernel issue worth surfacing loudly,
        # not papering over with rm -f.
        # (Comments here must avoid ASCII apostrophes because this
        # whole block runs inside a bash -c single-quoted string.)
        echo "WATCHDOG: container $cname did not exit within ${{grace_seconds}}s of SIGTERM; force-killing (SIGKILL)" >&3 2>/dev/null || true
        podman --root "$storage" --runroot "$runroot" --cgroup-manager=cgroupfs kill --signal KILL "$cname" 2>/dev/null
    ' watchdog "$SLURM_JOB_ID" "$CONTAINER_NAME" "$PODMAN_STORAGE" "$PODMAN_RUN" \
        </dev/null >/dev/null 2>&1
    echo "Spawned podman teardown watchdog (poll=5s debounce=2 grace=60s)"
elif [ "${{DYNRUNNER_DISABLE_TEARDOWN_WATCHDOG:-0}}" = "1" ]; then
    echo "Skipped podman teardown watchdog (DYNRUNNER_DISABLE_TEARDOWN_WATCHDOG=1)"
fi

echo "Starting Docker container..."
echo "  Volumes:"
echo "    {src_tmp} -> /app/src-tmp"
echo "    {out_tmp} -> /app/out-tmp"
echo "    {log_tmp} -> /app/log-tmp"
echo "    {srcbins_network} -> /app/src-network (ro)"
echo "    {output_network} -> /app/out-network"
echo "    {log_network} -> /app/log-network"
{dynrunner_echo_block}
echo "    {socket_dir} -> /app/sockets"
echo "  Secondary ID: {secondary_id}"
"##,
        image_tar_basename = cfg.image_tar_basename,
        image_path = cfg.image_path,
        load_command = cfg.load_command,
        secondary_id = cfg.secondary_id,
    ));

    // Mode-specific bits: banner echo lines and the `--secondary <url>`
    // argument. The podman-run block itself (volumes, env, framework
    // flags) is identical between modes — rendered once below.
    let image_ref = format!("{}:{}", cfg.image_name, cfg.image_tag);
    let sid = cfg.secondary_id;
    let container_command = cfg.container_command;
    let cores_spec = cfg.cores_spec;
    let max_memory_spec = cfg.max_memory_spec;
    // Container-internal bind-mount path for the staged-source drive.
    // Forwarded as `--src-network={path}` so the secondary's argparse
    // stores it on `args.src_network` and the dispatcher hands it to
    // `SecondaryConfig(src_network=...)` without relying on the auto-
    // detect path-exists check (which silently falls back to `None`
    // when the bind-mount appears late, the path is inaccessible to
    // the user, or any other transient filesystem-visibility issue).
    let src_network_path = WRAPPER_SRC_NETWORK_CONTAINER_PATH;
    // Bash-quote each forwarded user argv token and prefix with a
    // leading space so the joined block splices cleanly after the
    // framework's `--src-network={path}` argument. Empty slice
    // collapses to "" and the rendered line remains identical to the
    // pre-forwarding shape — callers that pass no forwarded_argv see
    // no diff in the rendered wrapper. `bash_quote` matches Python's
    // `shlex.quote` semantics (safe chars verbatim, everything else
    // single-quoted with `'\''` escaping for embedded apostrophes),
    // so values containing spaces, glob chars, or shell metacharacters
    // round-trip intact through the bash interpreter.
    let forwarded_argv_block: String = cfg
        .forwarded_argv
        .iter()
        .map(|arg| format!(" {}", bash_quote(arg)))
        .collect();
    let (mode_banner, secondary_url) = match &cfg.connection {
        ConnectionMode::Reverse { .. } => (
            "echo \"  Mode: SSH ProxyJump (primary tunnels to secondary via gateway)\""
                .to_string(),
            "tcp://localhost:$TUNNEL_PORT".to_string(),
        ),
        ConnectionMode::Standard {
            gateway_host,
            gateway_port,
        } => (
            format!(
                "echo \"  Gateway: {gateway_host}:{gateway_port}\"\n\
                 echo \"  Mode: Standard (secondary connects to primary via gateway)\""
            ),
            format!("tcp://{gateway_host}:{gateway_port}"),
        ),
    };

    script.push_str(&format!(
        r##"{mode_banner}
echo ""

# `--pull=never`: if the local `podman load` was incomplete (image
# layers missing from the load), podman's default behaviour is to
# silently fall through to a registry pull and try docker.io —
# which on most institutional clusters returns "access denied"
# only after a multi-minute timeout, by which point the
# dispatcher has already given up with `timeout waiting for
# secondaries`. `--pull=never` makes that class of incomplete-load
# fail loud-and-fast with a clear "image not in local storage"
# error instead.
#
# `--ulimit nproc=32768:32768` overrides the host-side RLIMIT_NPROC
# that podman would otherwise propagate into the container. Without
# this, fork-heavy in-container workloads (autotools `./configure`,
# parallel gcc/clang, JVM thread spawn) hit `EAGAIN: Resource
# temporarily unavailable` whenever the SLURM job's inherited
# per-user nproc cap (or podman's `containers.conf` default) lands
# below the workload's peak fork count — independently of the
# `--pids-limit` cgroup ceiling, which only constrains pids.max
# inside the container's cgroup. Sibling fix to the `--pids-limit`
# default (commit 9b3dce0): same class of pre-2026-05 per-consumer
# rediscovery tax. 32768 = 2× the per-container `--pids-limit`
# (so two concurrent containers on one node can each hit their
# cgroup ceiling without bumping into the per-user nproc cap)
# and ½ of 65536 (the kernel's typical max), leaving operator
# headroom. Override path is the same as `--pids-limit`: pass
# `--ulimit nproc=<N>:<N>` via `TaskDeploymentSpec.extra_run_args`;
# podman applies the LAST occurrence. NOTE: this cannot raise the
# SLURM cgroup's pids.max — that's operator policy. Documented in
# `docs/MIGRATION_2026_05_PYTHON_TO_RUST.md`.
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" --cgroup-manager=cgroupfs run --rm \
    --name "$CONTAINER_NAME" \
    --pull=never \
    --network host \
    --pids-limit=16384 \
    --ulimit nproc=32768:32768 \
    ${{MEM_FLAGS}} \
    -e PRIMARY_NODE_IPV4="$PRIMARY_NODE_IPV4" \
    -e PRIMARY_NODE_IPV6="$PRIMARY_NODE_IPV6" \
{dynrunner_env_block}    -v "{src_tmp}:/app/src-tmp" \
    -v "{out_tmp}:/app/out-tmp" \
    -v "{log_tmp}:/app/log-tmp" \
    -v "{srcbins_network}:/app/src-network:ro" \
    -v "{output_network}:/app/out-network" \
    -v "{log_network}:/app/log-network" \
{dynrunner_volume_block}    -v "{socket_dir}:/app/sockets" \
{extra_run_args_block}    {image_ref} \
    {container_command} --secondary {secondary_url} --secondary-id {sid} --secondary-quic-port $QUIC_PORT --cores={cores_spec} --max-memory={max_memory_spec} --src-network={src_network_path} --log-dir=/app/log-network{forwarded_argv_block}"##
    ));

    script.push_str(
        r#"
CONTAINER_EXIT_CODE=$?
echo "Container exited with code: $CONTAINER_EXIT_CODE"
kill $CMD_RELAY_PID 2>/dev/null || true

echo "=================================================="
echo "Job completed"
echo "Time: $(date)"
echo "=================================================="

exit $CONTAINER_EXIT_CODE
"#,
    );

    script
}
