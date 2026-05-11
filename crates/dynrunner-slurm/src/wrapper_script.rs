//! SLURM wrapper-script generator (single source of truth).
//!
//! Single concern: render the bash wrapper that runs on a SLURM
//! compute node. This is the canonical generator; the Python
//! `dynamic_runner.packaging.job_manager` module thin-shims into it
//! via the PyO3 binding (see `crates/dynrunner-pyo3/src/slurm/`).
//!
//! Inputs are **fully-resolved strings** by the caller: tilde
//! expansion against the gateway's remote home, image-tar basename,
//! load command (`podman load < ...` template with substitutions
//! already done — except the `$VAR` references which the bash
//! interpreter resolves), etc. The generator does no path-resolution
//! of its own; the Python caller's only job is to pre-resolve those
//! strings from its objects (`PodmanImageMetadata`, `PodmanPackaging`,
//! `SlurmConfig`, `TaskDeploymentSpec`).

use crate::config::SlurmConfig;

/// Configuration for generating a SLURM wrapper script.
pub struct WrapperScriptConfig<'a> {
    pub slurm_config: &'a SlurmConfig,
    /// Absolute (already tilde-expanded) path to the docker-archive
    /// tar on the gateway.
    pub image_path: &'a str,
    /// Identifier of the secondary that will run inside the container.
    pub secondary_id: &'a str,
    /// Container image name (e.g. `asm-tokenizer`).
    pub image_name: &'a str,
    /// Container image tag (e.g. `latest`).
    pub image_tag: &'a str,
    /// Basename of the docker-archive tar on the compute node's local
    /// /tmp copy. Mirrors `TaskDeploymentSpec.image_tar_basename`
    /// (typically `<image_name>.tar`).
    pub image_tar_basename: &'a str,
    /// Bash snippet that loads the image into podman storage. The
    /// caller pre-substitutes `$LOCAL_IMAGE`, `$PODMAN_STORAGE`,
    /// `$PODMAN_RUN`; the generator emits this verbatim inside the
    /// `if ! { ... }` failure-marker block.
    pub load_command: &'a str,
    /// In-container entrypoint and its args after `--secondary` URL,
    /// `--secondary-id`, `--secondary-quic-port`, and `--cores` are
    /// appended. For the typical case this is the consumer's
    /// `TaskDeploymentSpec.secondary_module`.
    pub container_command: &'a str,
    /// CLI `--cores` spec (verbatim string: `"0"`, `"N"`, `"+N"`,
    /// `"-N"`) forwarded to the secondary subprocess inside the
    /// container. Each secondary parses this locally against its
    /// own container's detected CPU count via `parse_cores`,
    /// preserving the per-machine semantic. The framework's
    /// `PySecondaryConfig.__new__` auto-detect (which reads the
    /// host's `available_parallelism` from inside a cgroup-CPU-
    /// quota'd container and returns the host CPU count, not the
    /// SLURM cgroup's quota) is then suppressed because the
    /// secondary's argparse parses `--cores` and explicitly
    /// populates `num_workers`. Symmetric with the
    /// `--multi-computer local` fix in `spawn_secondary.py`
    /// (commit 38a0c30 / task #26).
    pub cores_spec: &'a str,
    /// CLI `--max-memory` spec (verbatim string: `"16G"`, `"4G"`,
    /// `"-2G"`, `"+1G"`, …) forwarded to the secondary subprocess
    /// inside the container. Each secondary parses this locally via
    /// `parse_memory` against its OWN host's `/proc/meminfo:MemTotal`
    /// (or cgroup-v2 memory.max if a cap applies), preserving the
    /// per-machine semantic in heterogeneous SLURM clusters. Without
    /// forwarding, the secondary's argparse default (`"-2G"` =
    /// host_memory - 2 GiB) and PySecondaryConfig's auto-detect
    /// (`detect_total_memory_bytes`) read the host's full RAM from
    /// inside the cgroup-memory-quota'd container (asm-dataset-nix
    /// observed `budget_mb=92030` for a worker in a 4 GiB-capped
    /// container at 3c5f105) — workers then think they have 90+ GiB
    /// each and over-allocate. Symmetric with `cores_spec`. The
    /// `--multi-computer local` path INTENTIONALLY does NOT forward
    /// memory (single-host shared RAM = double-counting); this
    /// SLURM-only forward is correct because SLURM secondaries are
    /// each on a different host with their own RAM budget.
    pub max_memory_spec: &'a str,
    /// Connection-mode-specific config (gateway/standard vs reverse).
    pub connection: ConnectionMode<'a>,
    /// Optional override for the run-log directory used as the
    /// `/app/log-network` mount source. Falls back to
    /// `slurm_config.log_path()` when None.
    pub run_log_dir: Option<&'a str>,
    /// Optional bind-mount source for the framework's filesystem
    /// control-plane (mounted at `/app/dynrunner-network` and exposed
    /// via `DYNRUNNER_NETWORK` env in the container). When None the
    /// volume and env are omitted entirely. Mirrors
    /// `TaskDeploymentSpec.dynrunner_network_dir`.
    pub dynrunner_network_dir: Option<&'a str>,
    /// Bind-mount source for the cluster-wide src-bins network mount
    /// (typically `slurm_config.get_srcbins_mount_source()` from the
    /// Python side; pre-tilde-expanded). When None the generator
    /// defaults to `slurm_config.src_bins_path()` for back-compat.
    pub srcbins_mount_source: Option<&'a str>,
    /// Bind-mount source for the cluster-wide output mount. When
    /// None defaults to `slurm_config.output_path()`.
    pub output_dir: Option<&'a str>,
    /// Consumer-supplied additional flags to interpolate into the
    /// `podman run` invocation BEFORE the `{image_name}:{image_tag}`
    /// argument and AFTER the framework's own flags. Each entry is
    /// bash-quoted by the generator (callers MUST NOT pre-quote).
    /// Mirrors `TaskDeploymentSpec.extra_run_args`.
    pub extra_run_args: &'a [String],
}

/// How the secondary connects to the primary.
pub enum ConnectionMode<'a> {
    /// Secondary connects to primary via gateway host:port.
    Standard {
        gateway_host: &'a str,
        gateway_port: u16,
    },
    /// Primary tunnels to secondary via ProxyJump; secondary writes
    /// connection info into `connection_info_dir` for the primary
    /// to pick up.
    Reverse {
        connection_info_dir: &'a str,
    },
}

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

# Cap container memory at MIN(NodeRAM - 2GiB, wrapper-cgroup-memory-max)
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
        MEM_SOURCE="NodeRAM - 2GiB (tighter than cgroup ${{MEM_BYTES_CGROUP}})"
    else
        MEM_BYTES="${{MEM_BYTES_CGROUP}}"
        MEM_SOURCE="wrapper cgroup memory.max (tighter than NodeRAM-2GiB ${{MEM_BYTES_NODE}})"
    fi
elif [ -n "${{MEM_BYTES_NODE}}" ]; then
    MEM_BYTES="${{MEM_BYTES_NODE}}"
    MEM_SOURCE="NodeRAM - 2GiB (no cgroup cap detected)"
elif [ -n "${{MEM_BYTES_CGROUP}}" ]; then
    MEM_BYTES="${{MEM_BYTES_CGROUP}}"
    MEM_SOURCE="wrapper cgroup memory.max (NodeRAM probe failed)"
else
    MEM_BYTES=""
fi
if [ -n "${{MEM_BYTES}}" ]; then
    MEM_FLAGS="--memory=${{MEM_BYTES}} --memory-swap=-1"
    echo "Container memory cap: ${{MEM_BYTES}} bytes RAM + unlimited swap (${{MEM_SOURCE}})"
else
    MEM_FLAGS=""
    echo "Container memory cap: disabled (NodeRAM and cgroup probes both empty)"
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
            script.push_str(&format!(
                r##"
echo "Finding free ports on compute node..."
TUNNEL_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('', 0)); print(s.getsockname()[1]); s.close()")
QUIC_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('', 0)); print(s.getsockname()[1]); s.close()")
echo "Using tunnel port: $TUNNEL_PORT"
echo "Using QUIC port: $QUIC_PORT"

HOSTNAME=$(hostname -f)
mkdir -p "{connection_info_dir}"
# Wire format: single-line `<scheme>://<host>:<port>` URI parsed by
# the Rust-side `parse_connection_uri` in dynrunner-slurm/preparation.
# Aligns with the framework's `Primary URL: tcp://...` convention and
# leaves room for future extension (path/query) without re-spinning
# the parser.
printf 'tcp://%s:%s\n' "$HOSTNAME" "$TUNNEL_PORT" > "{connection_info_dir}/{sid}.info"
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
# Watchdog polls `squeue -j $SLURM_JOB_ID` once a second; when
# the job is gone (squeue exit 0 + empty stdout — distinct from
# transient query failure which exits nonzero), it issues
# `podman kill` then `podman rm -f` on this wrapper's container
# by name. Detached via `setsid -f` so it survives wrapper exit
# and (where possible) cgroup teardown of the wrapper's pidtree.
#
# Debounced (task #40): a SINGLE rc=0-with-empty observation is
# NOT sufficient to fire the kill. slurmctld can transiently
# return rc=0 with empty output during RPC stalls, accounting
# flushes, or task/cgroup view churn under workload bursts.
# Interpreting any one such observation as "job is gone"
# falsely kills a container whose SLURM job is still running.
# Require KILL_THRESHOLD consecutive empty observations
# (default 3 → 3s debounce at 1s sleep) before firing. The
# tradeoff is false-kill latency vs true-kill reliability;
# 3s is short enough that operators don't notice the extra
# wait on actual teardown but long enough to ride out
# slurmctld blips. asm-dataset-nix at run_20260511_190700
# hit the undebounced shape: workers died mid-build with no
# obvious kill source on the container.
#
# The kill event is logged to the wrapper's stdout via fd 3
# (the wrapper redirects its own stdout to the .out file
# before the watchdog spawns; the watchdog inherits fd 3 →
# same .out file). Operators grepping `slurm_*.out` for
# "WATCHDOG kill" can confirm whether the watchdog killed a
# container post-hoc.
#
# If proctrack/cgroup is in fact tracking the watchdog too, the
# watchdog dies alongside the rest — but in that case the
# container is also dead, so there's nothing to clean up. Either
# branch leaves the system in a clean state.
#
# Skipped when SLURM_JOB_ID is empty (running outside SLURM):
# squeue would never find a matching job so the watchdog could
# never exit cleanly.
if [ -n "${{SLURM_JOB_ID:-}}" ]; then
    # Duplicate the wrapper's stdout to fd 3 so the detached
    # watchdog can log its kill-trigger to the .out file even
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
        kill_threshold=3
        empty_count=0
        while true; do
            sleep 1
            if out=$(squeue -j "$job_id" -h -o "%i" 2>/dev/null); then
                if [ -z "$out" ]; then
                    empty_count=$((empty_count + 1))
                    if [ "$empty_count" -ge "$kill_threshold" ]; then
                        break
                    fi
                else
                    empty_count=0
                fi
            fi
        done
        echo "WATCHDOG kill: $kill_threshold consecutive empty squeue observations for job $job_id; killing container $cname" >&3 2>/dev/null || true
        podman --root "$storage" --runroot "$runroot" kill --signal TERM "$cname" 2>/dev/null
        sleep 5
        podman --root "$storage" --runroot "$runroot" rm -f "$cname" 2>/dev/null
    ' watchdog "$SLURM_JOB_ID" "$CONTAINER_NAME" "$PODMAN_STORAGE" "$PODMAN_RUN" \
        </dev/null >/dev/null 2>&1
    echo "Spawned podman teardown watchdog for SLURM job $SLURM_JOB_ID (container $CONTAINER_NAME), kill-threshold=3"
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
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" run --rm \
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
    {container_command} --secondary {secondary_url} --secondary-id {sid} --secondary-quic-port $QUIC_PORT --cores={cores_spec} --max-memory={max_memory_spec}"##
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

/// Configuration for the image-validation test wrapper.
///
/// Mirrors the input shape of [`generate_test_wrapper_script`] —
/// the test wrapper exercises only the image-load path, so it needs
/// far fewer knobs than the full secondary wrapper.
pub struct TestWrapperScriptConfig<'a> {
    /// Absolute (already tilde-expanded) path to the docker-archive
    /// tar on the gateway.
    pub image_path: &'a str,
    pub image_name: &'a str,
    pub image_tag: &'a str,
    pub image_tar_basename: &'a str,
    pub load_command: &'a str,
    /// In-container entrypoint to test via `--help`.
    pub container_command: &'a str,
}

/// Generate the bash wrapper script used by `slurm-validate-image`.
///
/// The test wrapper copies the image to /tmp, loads it into a
/// fresh podman storage root, lists the image, and runs the
/// container's `--help` to confirm the entrypoint is intact. It
/// shares the SLURM-induced-signal trap pattern with the full
/// wrapper (commit 485629c) so test-job termination doesn't leak
/// /tmp/asm-test-XXXX scratch dirs.
pub fn generate_test_wrapper_script(cfg: &TestWrapperScriptConfig<'_>) -> String {
    let rnd_suffix = rand_hex8();
    let rndtmp = format!("/tmp/asm-test-{rnd_suffix}");
    let podman_storage = format!("{rndtmp}/storage");
    let podman_run = format!("{rndtmp}/run");

    format!(
        r##"#!/usr/bin/env bash
set -e

echo "=================================================="
echo "SLURM Test Job - Docker Image Validation"
echo "Node: $(hostname)"
echo "Job ID: $SLURM_JOB_ID"
echo "Time: $(date)"
echo "=================================================="
echo ""

RNDTMP="{rndtmp}"
echo "Creating temporary directory: $RNDTMP"
mkdir -p "$RNDTMP"

cleanup() {{
    echo ""
    echo "Cleaning up temporary directory: $RNDTMP"
    # Per-file unlink of the image tarball before the tree rm —
    # tarball is host-UID owned (cp'd in by this wrapper), so
    # plain rm reaches it without entering the user-namespace.
    # `${{LOCAL_IMAGE:-}}` guard covers early-exit paths that hit
    # the trap before LOCAL_IMAGE was assigned. See the secondary
    # wrapper's cleanup() for the rationale on per-file vs tree.
    if [ -n "${{LOCAL_IMAGE:-}}" ] && [ -e "$LOCAL_IMAGE" ]; then
        rm -f -- "$LOCAL_IMAGE" 2>/dev/null \
            || echo "WARNING: failed to unlink $LOCAL_IMAGE" >&2
    fi
    # `podman unshare rm -rf` is the only mechanism that reaches
    # the subuid-mapped files rootless `podman load` writes into
    # $RNDTMP/storage. Plain rm fallback keeps the wrapper safe
    # on hosts without podman. `--` blocks accidental flag
    # interpretation. Result logged AFTER the rm so a leak is
    # never silent.
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

echo "Copying image to local /tmp..."
LOCAL_IMAGE="$RNDTMP/{image_tar_basename}"
echo "  Source: {image_path}"
cp "{image_path}" "$LOCAL_IMAGE"
echo "  Size: $(du -h "$LOCAL_IMAGE" | cut -f1)"
echo ""

echo "Loading image..."
{load_command}
echo ""

echo "Verifying image is loaded..."
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" images | grep {image_name} || echo "WARNING: Image not found in listing"
echo ""

echo "Testing secondary entrypoint ({container_command} --help)..."
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" run --rm {image_ref} {container_command} --help | head -5
echo ""

echo "=================================================="
echo "Test Job Completed Successfully"
echo "Time: $(date)"
echo "=================================================="
"##,
        image_tar_basename = cfg.image_tar_basename,
        image_path = cfg.image_path,
        load_command = cfg.load_command,
        image_name = cfg.image_name,
        container_command = cfg.container_command,
        image_ref = format!("{}:{}", cfg.image_name, cfg.image_tag),
    )
}

/// Bash-quote a string the way Python's `shlex.quote` does:
/// safe chars (`[A-Za-z0-9@%+=:,./_-]`) and non-empty input pass
/// through verbatim; everything else is wrapped in single quotes
/// with internal `'` replaced by `'\''`. The empty string becomes
/// `''` to avoid silent collapse on the bash side.
fn bash_quote(s: &str) -> String {
    if s.is_empty() {
        return "''".to_string();
    }
    let safe = s
        .bytes()
        .all(|b| matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'@' | b'%' | b'+' | b'=' | b':' | b',' | b'.' | b'/' | b'_' | b'-'));
    if safe {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

/// 8-hex-char random suffix using `/dev/urandom` (4 bytes of
/// entropy). Mirrors Python's `secrets.token_hex(4)`. Falls back
/// to a hash of the system time if /dev/urandom is unreadable
/// (extremely unlikely on Linux).
fn rand_hex8() -> String {
    use std::io::Read;
    let mut buf = [0u8; 4];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        if f.read_exact(&mut buf).is_ok() {
            return format!(
                "{:02x}{:02x}{:02x}{:02x}",
                buf[0], buf[1], buf[2], buf[3]
            );
        }
    }
    // Fallback: hash of nanoseconds-since-epoch — not cryptographic
    // but identical entropy semantics for the suffix's purpose
    // (avoid two parallel jobs sharing /tmp/asm-XXXX).
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut h = DefaultHasher::new();
    h.write_u128(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    format!("{:08x}", h.finish() as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn standard_cfg<'a>(
        slurm_config: &'a SlurmConfig,
        extra_run_args: &'a [String],
    ) -> WrapperScriptConfig<'a> {
        WrapperScriptConfig {
            slurm_config,
            image_path: "/images/test.tar",
            secondary_id: "sec-01",
            image_name: "test-app",
            image_tag: "latest",
            image_tar_basename: "test-app.tar",
            load_command: "podman --root \"$PODMAN_STORAGE\" --runroot \"$PODMAN_RUN\" load < \"$LOCAL_IMAGE\"",
            container_command: "dynamic_batch_tokenizer",
            cores_spec: "0",
            max_memory_spec: "-2G",
            connection: ConnectionMode::Standard {
                gateway_host: "gateway.example.com",
                gateway_port: 9000,
            },
            run_log_dir: None,
            dynrunner_network_dir: None,
            srcbins_mount_source: None,
            output_dir: None,
            extra_run_args,
        }
    }

    #[test]
    fn standard_mode_script_forwards_cores_spec() {
        // Task #29: each SLURM secondary's container_command MUST
        // receive `--cores <spec>` as a verbatim argument so the
        // secondary subprocess inside the cgroup-CPU-quota'd
        // container resolves the per-machine core count via
        // `parse_cores` instead of falling back to
        // `available_parallelism` (which inside the SLURM container
        // reads the host's CPU count, not the cgroup quota — the
        // foot-gun this fix closes). Pre-fix this argv suffix was
        // entirely absent and asm-dataset-nix observed
        // `secondary starting workers=32` even with `--cores 2` on
        // the dispatcher.
        let config = SlurmConfig::default();
        let mut cfg = standard_cfg(&config, &[]);
        cfg.cores_spec = "-2";
        let script = generate_wrapper_script(&cfg);
        // `=` syntax mandatory (task #32): `--cores -2` confuses
        // argparse on the secondary because `-2` matches argparse's
        // "looks like a flag" heuristic and the option-with-required-
        // value rejects it. `--cores=-2` always treats RHS as literal
        // value regardless of leading dash.
        assert!(
            script.contains("--cores=-2"),
            "wrapper script must forward `--cores=-2` (not `--cores -2`) \
             to secondary so argparse parses the leading-dash value as \
             a value, not a flag; render did not contain it"
        );
        // The `--cores` flag MUST appear AFTER `--secondary-quic-port`
        // (the argv-build order matches the regular CLI order):
        // assert position to catch regressions that move the flag
        // somewhere broken (e.g. into the podman-run flags block
        // instead of the container_command suffix).
        let port_idx = script
            .find("--secondary-quic-port")
            .expect("--secondary-quic-port must be present");
        let cores_idx = script
            .find("--cores=")
            .expect("--cores= must be present");
        assert!(
            cores_idx > port_idx,
            "--cores must appear after --secondary-quic-port in the secondary's \
             argv (currently at byte {cores_idx}, port at {port_idx})"
        );
    }

    #[test]
    fn standard_mode_script_forwards_max_memory_spec() {
        // Task #30: each SLURM secondary's container_command MUST
        // receive `--max-memory <spec>` symmetrically with `--cores`.
        // Without forwarding, the secondary inside the cgroup-memory-
        // quota'd container falls through to its argparse default
        // (`-2G` = HOST_MemTotal - 2G as seen via /proc/meminfo).
        // Inside a 4 GiB-capped container, /proc/meminfo still shows
        // the host's full RAM, so the framework computes 90+ GiB
        // worker budgets and workers OOM-die under the outer
        // cgroup's actual cap (asm-dataset-nix observed
        // `worker_id=0 budget_mb=92030` inside WORKER_MEMORY=4g).
        //
        // Defends the explicit-forward contract: dispatcher value
        // reaches the wrapper, wrapper emits the argv suffix.
        let config = SlurmConfig::default();
        let mut cfg = standard_cfg(&config, &[]);
        cfg.max_memory_spec = "3G";
        let script = generate_wrapper_script(&cfg);
        assert!(
            script.contains("--max-memory=3G"),
            "wrapper script must forward `--max-memory=3G` to secondary; \
             render did not contain it"
        );
        // Also test the negative-prefix case explicitly — this is the
        // exact value that caused the original argparse-collision
        // (asm-dataset-nix T3 at 57d7ee8 with default `-2G`).
        cfg.max_memory_spec = "-2G";
        let script_negative = generate_wrapper_script(&cfg);
        assert!(
            script_negative.contains("--max-memory=-2G"),
            "wrapper script must use `=` syntax for negative-offset memory \
             specs (task #32 argparse-collision fix); render did not contain it"
        );
        // `--max-memory` MUST land AFTER `--cores` (argv-build order).
        let cores_idx = script
            .find("--cores=")
            .expect("--cores= must be present");
        let mem_idx = script
            .find("--max-memory=")
            .expect("--max-memory= must be present");
        assert!(
            mem_idx > cores_idx,
            "--max-memory must appear after --cores in the secondary's argv \
             (currently at byte {mem_idx}, cores at {cores_idx})"
        );
    }

    #[test]
    fn standard_mode_script_contains_gateway() {
        let config = SlurmConfig::default();
        let script = generate_wrapper_script(&standard_cfg(&config, &[]));
        assert!(script.contains("gateway.example.com:9000"));
        assert!(script.contains("--secondary-id sec-01"));
        assert!(script.contains("mkfifo"));
        assert!(!script.contains("TUNNEL_PORT"));
        assert!(script.contains("test-app.tar"));
        assert!(script.contains("dynamic_batch_tokenizer --secondary"));
        // Host-IP probe + env plumbing (the bug fix this guards):
        // without these the container's `hostname -I` advertises a
        // non-routable bridge IP and peers can't dial it.
        assert!(script.contains("getent ahostsv4"));
        assert!(script.contains("PRIMARY_NODE_IPV4="));
        assert!(script.contains("-e PRIMARY_NODE_IPV4="));
        assert!(script.contains("-e PRIMARY_NODE_IPV6="));
        // `--pull=never`, `--pids-limit=16384`, and
        // `--ulimit nproc=32768:32768` are framework defaults the
        // wrapper must always emit (commits 48288f7, 9b3dce0, and
        // the nproc framework-default sibling).
        assert!(script.contains("--pull=never"));
        assert!(script.contains("--pids-limit=16384"));
        assert!(script.contains("--ulimit nproc=32768:32768"));
        // Cleanup trap covers SLURM-induced signals (commit 485629c).
        assert!(script.contains("trap cleanup EXIT TERM HUP INT"));
        // Watchdog block (commit a12f84a + #40 debounce).
        assert!(script.contains("setsid -f bash -c"));
        assert!(script.contains("podman teardown watchdog"));
        // #40: debounce — kill_threshold + empty_count machinery
        // must be present, preventing a single transient squeue
        // rc=0+empty observation from killing a still-running
        // container. Pin both the literal threshold and the kill-
        // trigger log line so future edits don't accidentally
        // revert to the single-observation fire path.
        assert!(
            script.contains("kill_threshold=3"),
            "watchdog must declare kill_threshold=3 (debounce \
             count); render did not contain it"
        );
        assert!(
            script.contains("empty_count=$((empty_count + 1))"),
            "watchdog must increment empty_count on each rc=0+empty \
             observation, NOT break immediately; render did not \
             contain the increment"
        );
        assert!(
            script.contains("WATCHDOG kill:"),
            "watchdog must log a kill-trigger line so post-hoc \
             diagnosis can attribute container deaths; render did \
             not contain the log"
        );
        // The kill-threshold value is also surfaced in the spawn-
        // confirmation echo so operators see the configured
        // threshold without reading the bash.
        assert!(
            script.contains("kill-threshold=3"),
            "spawn-confirmation echo must surface the kill-threshold \
             value; render did not contain it"
        );
        // Memory-cap block: both probes (NodeRAM + wrapper cgroup
        // memory.max) must be present so the min() logic engages on
        // any cluster where SLURM imposes a per-job cap tighter than
        // NodeRAM-2GiB. The renaming from MEM_BYTES to MEM_BYTES_NODE
        // in #31 is intentional — the new shape composes two probes
        // before settling on MEM_BYTES.
        assert!(script.contains("MEM_BYTES_NODE=$(awk"));
        assert!(script.contains("MEM_BYTES_CGROUP="));
        assert!(script.contains("/sys/fs/cgroup/memory.max"));
        assert!(script.contains("${MEM_FLAGS}"));
        // User-policy regression pin: `--memory-swap=-1` (unlimited
        // swap on top of the RAM cap) per explicit instruction.
        // Defends against accidental revert to `--memory-swap=<RAM>`
        // (which would re-introduce immediate cgroup-OOM on RAM
        // overshoot) or to `--memory-swap=<2x RAM>` (podman's
        // unset-flag default — same OOM-on-overshoot behaviour
        // because the swap component is bounded). The string match
        // is exact: `--memory-swap=-1` not `--memory-swap=$<var>`.
        assert!(
            script.contains("--memory-swap=-1"),
            "wrapper must emit `--memory-swap=-1` so workers swap \
             instead of getting cgroup-OOM-killed under RAM pressure; \
             render did not contain it"
        );
        // And the RAM cap must still apply — --memory=<bytes> is
        // load-bearing for the kernel's in-core ceiling.
        assert!(
            script.contains("--memory=${MEM_BYTES}"),
            "wrapper must still emit --memory=<bytes> alongside the \
             unlimited-swap flag — RAM cap is independent of swap cap"
        );
        // FIFO loud-error elif (commit 179afd9).
        assert!(script.contains("disappeared unexpectedly"));
        // Image-load loud-failure marker (commit 733559c).
        assert!(script.contains("ERROR: image load failed"));
        // Container name flow (asm- prefix per L1.7 wire reconciliation).
        assert!(script.contains("--name \"$CONTAINER_NAME\""));
        assert!(script.contains("/tmp/asm-"));
    }

    #[test]
    fn reverse_mode_script_contains_tunnel_port() {
        let config = SlurmConfig::default();
        let extra: [String; 0] = [];
        let cfg = WrapperScriptConfig {
            slurm_config: &config,
            image_path: "/images/test.tar",
            secondary_id: "sec-02",
            image_name: "test-app",
            image_tag: "latest",
            image_tar_basename: "test-app.tar",
            load_command: "podman --root \"$PODMAN_STORAGE\" --runroot \"$PODMAN_RUN\" load < \"$LOCAL_IMAGE\"",
            container_command: "my_runner",
            cores_spec: "0",
            max_memory_spec: "-2G",
            connection: ConnectionMode::Reverse {
                connection_info_dir: "/logs/connection_info",
            },
            run_log_dir: Some("/logs/run_001"),
            dynrunner_network_dir: None,
            srcbins_mount_source: None,
            output_dir: None,
            extra_run_args: &extra,
        };
        let script = generate_wrapper_script(&cfg);
        assert!(script.contains("TUNNEL_PORT"));
        assert!(script.contains("sec-02.info"));
        assert!(script.contains("localhost:$TUNNEL_PORT"));
        assert!(script.contains("my_runner --secondary"));
        // Reverse-mode connection-info file is the post-L1.7 URI
        // wire format: a single `<scheme>://<host>:<port>\n` line
        // parsed by `parse_connection_uri` on the primary side.
        assert!(script.contains(r#"printf 'tcp://%s:%s\n' "$HOSTNAME" "$TUNNEL_PORT""#));
        // The legacy `key=value` shape must not reappear — guards
        // against an accidental revert that the URI parser would
        // reject at runtime.
        assert!(!script.contains("hostname=$HOSTNAME"));
        assert!(!script.contains("tunnel_port=$TUNNEL_PORT"));
    }

    #[test]
    fn dynrunner_network_dir_emits_volume_and_env() {
        let config = SlurmConfig::default();
        let extra: [String; 0] = [];
        let cfg = WrapperScriptConfig {
            dynrunner_network_dir: Some("/host/dynrunner"),
            extra_run_args: &extra,
            ..standard_cfg(&config, &[])
        };
        let script = generate_wrapper_script(&cfg);
        assert!(script.contains("/host/dynrunner:/app/dynrunner-network"));
        assert!(script.contains("-e DYNRUNNER_NETWORK=\"/app/dynrunner-network\""));
    }

    #[test]
    fn extra_run_args_are_bash_quoted_and_appear_before_image_ref() {
        let config = SlurmConfig::default();
        let extras = vec!["--ulimit=nofile=65536".to_string(), "--shm-size=2g".to_string()];
        let cfg = standard_cfg(&config, &extras);
        let script = generate_wrapper_script(&cfg);
        for flag in &extras {
            assert!(
                script.contains(flag),
                "expected extra_run_args entry {flag:?} to appear in rendered script"
            );
        }
        let image_idx = script.find("test-app:latest").expect("image ref present");
        let extra_idx = script.find("--ulimit=nofile=65536").expect("extra arg present");
        assert!(
            extra_idx < image_idx,
            "extra_run_args must precede the image ref; podman parses left-to-right"
        );
    }

    #[test]
    fn extra_run_args_with_metacharacters_are_quoted() {
        let config = SlurmConfig::default();
        let extras = vec!["--annotation=hello world".to_string()];
        let cfg = standard_cfg(&config, &extras);
        let script = generate_wrapper_script(&cfg);
        // The space forces single-quoting.
        assert!(script.contains("'--annotation=hello world'"));
    }

    /// Consumer-supplied `--ulimit nproc=N:N` via `extra_run_args`
    /// must land AFTER the framework default in the rendered
    /// invocation so podman's last-wins flag parsing applies the
    /// consumer's value. Mirrors the pids-limit override semantic
    /// (commit 9b3dce0) — same rule, sibling concern.
    #[test]
    fn consumer_nproc_ulimit_lands_after_framework_default() {
        let config = SlurmConfig::default();
        let consumer_value = "--ulimit=nproc=65536:65536".to_string();
        let extras = vec![consumer_value.clone()];
        let cfg = standard_cfg(&config, &extras);
        let script = generate_wrapper_script(&cfg);

        let default_idx = script
            .find("--ulimit nproc=32768:32768")
            .expect("framework default `--ulimit nproc=32768:32768` must be present");
        let consumer_idx = script
            .find(consumer_value.as_str())
            .expect("consumer-supplied nproc override must be rendered");
        assert!(
            default_idx < consumer_idx,
            "consumer-supplied nproc override must follow the framework default \
             so podman's last-wins parsing applies the consumer's value; \
             got default at {default_idx} and consumer at {consumer_idx}"
        );
    }

    #[test]
    fn test_wrapper_traps_termination_signals() {
        let script = generate_test_wrapper_script(&test_wrapper_cfg());
        assert!(script.contains("trap cleanup EXIT TERM HUP INT"));
        assert!(script.contains("/tmp/asm-test-"));
        assert!(script.contains("test-app.tar"));
        assert!(script.contains("my_runner --help"));
    }

    /// Render the wrapper in both connection modes and pipe through
    /// `bash -n` to catch any quoting/escape regression that compiles
    /// fine but produces a syntactically broken script — the kind of
    /// failure that would only surface on a SLURM compute node, miles
    /// from the developer's terminal. Guarded on `bash` being on
    /// $PATH; on stripped CI sandboxes the test silently no-ops.
    #[test]
    fn rendered_scripts_pass_bash_syntax_check() {
        let bash = match std::process::Command::new("bash")
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
        {
            Ok(s) if s.success() => "bash",
            _ => return,
        };

        let config = SlurmConfig::default();
        let standard = generate_wrapper_script(&standard_cfg(&config, &[]));
        let reverse = generate_wrapper_script(&WrapperScriptConfig {
            connection: ConnectionMode::Reverse {
                connection_info_dir: "/logs/connection_info",
            },
            ..standard_cfg(&config, &[])
        });
        let test_wrapper = generate_test_wrapper_script(&TestWrapperScriptConfig {
            image_path: "/images/test.tar",
            image_name: "test-app",
            image_tag: "latest",
            image_tar_basename: "test-app.tar",
            load_command: "podman --root \"$PODMAN_STORAGE\" --runroot \"$PODMAN_RUN\" load < \"$LOCAL_IMAGE\"",
            container_command: "my_runner",
        });

        for (label, script) in [
            ("standard", standard.as_str()),
            ("reverse", reverse.as_str()),
            ("test-wrapper", test_wrapper.as_str()),
        ] {
            use std::io::Write;
            let mut child = std::process::Command::new(bash)
                .args(["-n", "/dev/stdin"])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("spawn bash");
            child
                .stdin
                .as_mut()
                .unwrap()
                .write_all(script.as_bytes())
                .unwrap();
            let out = child.wait_with_output().expect("wait bash");
            assert!(
                out.status.success(),
                "bash -n rejected the {label}-mode wrapper:\nSTDERR:\n{}\n--- script ---\n{}",
                String::from_utf8_lossy(&out.stderr),
                script,
            );
        }
    }

    #[test]
    fn bash_quote_examples() {
        assert_eq!(bash_quote("hello"), "hello");
        assert_eq!(bash_quote(""), "''");
        assert_eq!(bash_quote("a b"), "'a b'");
        assert_eq!(bash_quote("it's"), "'it'\\''s'");
        assert_eq!(bash_quote("--pids-limit=16384"), "--pids-limit=16384");
    }

    /// Bug AA in the field log: rootless `podman load` writes
    /// $RNDTMP/storage files owned by mapped subuids that the
    /// host operator's UID can't unlink. The pre-fix cleanup
    /// silently swallowed the failure and leaked ~3.2 GB per
    /// run. The fix routes the tree-rm through `podman unshare`
    /// so it runs inside the user-namespace where those files
    /// are reachable, with a plain-rm fallback for hosts
    /// without podman.
    #[test]
    fn cleanup_uses_podman_unshare_for_rndtmp_teardown() {
        let config = SlurmConfig::default();
        let script = generate_wrapper_script(&standard_cfg(&config, &[]));
        assert!(
            script.contains("podman unshare rm -rf -- \"$RNDTMP\""),
            "cleanup must use `podman unshare rm` to reach subuid-mapped files"
        );
        // `--` is critical: blocks accidental flag interpretation
        // if $RNDTMP ever starts with a dash.
        assert!(script.contains("rm -rf -- \"$RNDTMP\""));
        // Old `sudo rm -rf` fallback is gone — sudo can't help
        // with user-ns subuid files (different uid space) and
        // workers don't have sudo anyway.
        assert!(
            !script.contains("sudo rm -rf"),
            "sudo fallback was removed — it never worked for subuid-mapped files"
        );
    }

    /// Per-file unlink for the image tarball: it's host-UID
    /// owned (cp'd in by the wrapper before any container ran),
    /// so a plain `rm -f` reaches it. The "rm -rf in scripts is
    /// dangerous" rule pushes us to per-file unlink wherever
    /// feasible.
    #[test]
    fn cleanup_unlinks_local_image_per_file() {
        let config = SlurmConfig::default();
        let script = generate_wrapper_script(&standard_cfg(&config, &[]));
        assert!(
            script.contains("rm -f -- \"$LOCAL_IMAGE\""),
            "cleanup must per-file unlink $LOCAL_IMAGE before tree-rm"
        );
        // The `${LOCAL_IMAGE:-}` guard covers early-exit paths
        // that hit the trap before LOCAL_IMAGE was assigned.
        assert!(script.contains("${LOCAL_IMAGE:-}"));
    }

    /// On rm failure the wrapper must log to stderr — silent
    /// `|| true` is what masked Bug AA in the first place. On
    /// success the cleanup line must appear AFTER the rm runs,
    /// not before, so logs reflect the actual outcome.
    #[test]
    fn cleanup_logs_failure_to_stderr_and_success_after_rm() {
        let config = SlurmConfig::default();
        let script = generate_wrapper_script(&standard_cfg(&config, &[]));
        // Failure is logged to stderr (`>&2`) with a marker that
        // log scrapers can match on. The "leaked" wording is
        // load-bearing for the field-ops grep.
        assert!(
            script.contains("/tmp scratch leaked on $(hostname)") && script.contains(">&2"),
            "cleanup must log rm failures to stderr with a scrapable marker"
        );
        // Success is logged AFTER the rm completes — the
        // `Cleaned up` (past-tense) string sits inside the
        // success branch, not before the rm.
        assert!(script.contains("Cleaned up temporary directory: $RNDTMP"));
    }

    /// Shared test fixture for the image-validation wrapper.
    /// Mirrors the shape used by `test_wrapper_traps_termination_signals`
    /// so all test-wrapper assertions render against the same input.
    fn test_wrapper_cfg() -> TestWrapperScriptConfig<'static> {
        TestWrapperScriptConfig {
            image_path: "/images/test.tar",
            image_name: "test-app",
            image_tag: "latest",
            image_tar_basename: "test-app.tar",
            load_command: "podman --root \"$PODMAN_STORAGE\" --runroot \"$PODMAN_RUN\" load < \"$LOCAL_IMAGE\"",
            container_command: "my_runner",
        }
    }

    /// Same fix applies to the image-validation wrapper: it
    /// also runs `podman load` into $RNDTMP/storage and so
    /// produces the same subuid-mapped tree.
    #[test]
    fn test_wrapper_cleanup_uses_podman_unshare() {
        let script = generate_test_wrapper_script(&test_wrapper_cfg());
        assert!(script.contains("podman unshare rm -rf -- \"$RNDTMP\""));
        assert!(script.contains("rm -f -- \"$LOCAL_IMAGE\""));
        assert!(!script.contains("sudo rm -rf"));
    }

    /// Render both wrappers and run `bash -n` on each to catch
    /// quoting / brace / heredoc regressions at unit-test time.
    /// Without this guard a misplaced `{` or unbalanced quote
    /// only surfaces on the compute node, where diagnosis costs
    /// a SLURM round-trip.
    #[test]
    fn rendered_wrapper_passes_bash_syntax_check() {
        use std::io::Write;
        use std::process::Command;

        // Skip cleanly if `bash` isn't on PATH (e.g. a
        // stripped-down CI image). Letting the test fail there
        // would force every consumer of the crate to install
        // bash before they can run `cargo test`, which isn't
        // what this guard is meant to enforce.
        if Command::new("bash").arg("--version").output().is_err() {
            eprintln!("skipping: bash not available on PATH");
            return;
        }

        let config = SlurmConfig::default();
        let secondary = generate_wrapper_script(&standard_cfg(&config, &[]));
        let test_wrapper = generate_test_wrapper_script(&test_wrapper_cfg());

        for (label, script) in [("secondary", secondary), ("test", test_wrapper)] {
            let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
            tmp.write_all(script.as_bytes()).expect("write script");
            let path = tmp.into_temp_path();
            let out = Command::new("bash")
                .arg("-n")
                .arg(&path)
                .output()
                .expect("spawn bash -n");
            assert!(
                out.status.success(),
                "{label} wrapper failed `bash -n`:\nstdout={}\nstderr={}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr),
            );
        }
    }
}
