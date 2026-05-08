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
    /// `--secondary-id`, `--secondary-quic-port` are appended. For
    /// the typical case this is the consumer's
    /// `TaskDeploymentSpec.secondary_module`.
    pub container_command: &'a str,
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
    rm -rf "$RNDTMP" 2>/dev/null || sudo rm -rf "$RNDTMP" 2>/dev/null || true
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

# Cap container memory at NodeRAM - 2GiB so a runaway worker hits a
# graceful container-OOM (just kills the worker process) instead of a
# host kernel-OOM that wedges the cgroup and leaves zombie SLURM jobs
# stuck COMPLETING. Probed at wrapper-execution time on the compute
# node — node RAM is not known at submit time and may differ from the
# primary. --memory-swap is set equal to --memory so podman cannot
# silently swap-thrash under memory pressure. Falls back to no cap on
# absurdly small nodes (<2GiB MemTotal, implausible on cluster).
MEM_BYTES=$(awk '/MemTotal:/{{val = $2*1024 - 2*1024*1024*1024; if (val > 0) print val; else print ""}}' /proc/meminfo)
if [ -n "${{MEM_BYTES}}" ]; then
    MEM_FLAGS="--memory=${{MEM_BYTES}} --memory-swap=${{MEM_BYTES}}"
    echo "Container memory cap: ${{MEM_BYTES}} bytes (NodeRAM - 2GiB)"
else
    MEM_FLAGS=""
    echo "Container memory cap: disabled (MemTotal probe yielded non-positive headroom)"
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
# If proctrack/cgroup is in fact tracking the watchdog too, the
# watchdog dies alongside the rest — but in that case the
# container is also dead, so there's nothing to clean up. Either
# branch leaves the system in a clean state.
#
# Skipped when SLURM_JOB_ID is empty (running outside SLURM):
# squeue would never find a matching job so the watchdog could
# never exit cleanly.
if [ -n "${{SLURM_JOB_ID:-}}" ]; then
    setsid -f bash -c '
        job_id="$1"
        cname="$2"
        storage="$3"
        runroot="$4"
        while true; do
            sleep 1
            if out=$(squeue -j "$job_id" -h -o "%i" 2>/dev/null); then
                [ -n "$out" ] || break
            fi
        done
        podman --root "$storage" --runroot "$runroot" kill --signal TERM "$cname" 2>/dev/null
        sleep 5
        podman --root "$storage" --runroot "$runroot" rm -f "$cname" 2>/dev/null
    ' watchdog "$SLURM_JOB_ID" "$CONTAINER_NAME" "$PODMAN_STORAGE" "$PODMAN_RUN" \
        </dev/null >/dev/null 2>&1
    echo "Spawned podman teardown watchdog for SLURM job $SLURM_JOB_ID (container $CONTAINER_NAME)"
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
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" run --rm \
    --name "$CONTAINER_NAME" \
    --pull=never \
    --network host \
    --pids-limit=16384 \
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
    {container_command} --secondary {secondary_url} --secondary-id {sid} --secondary-quic-port $QUIC_PORT"##
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
    rm -rf "$RNDTMP" 2>/dev/null || sudo rm -rf "$RNDTMP" 2>/dev/null || true
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
        // `--pull=never` and `--pids-limit=16384` are framework
        // defaults the wrapper must always emit (commits 48288f7
        // and the pids-limit framework-default).
        assert!(script.contains("--pull=never"));
        assert!(script.contains("--pids-limit=16384"));
        // Cleanup trap covers SLURM-induced signals (commit 485629c).
        assert!(script.contains("trap cleanup EXIT TERM HUP INT"));
        // Watchdog block (commit a12f84a).
        assert!(script.contains("setsid -f bash -c"));
        assert!(script.contains("podman teardown watchdog"));
        // Memory-cap block.
        assert!(script.contains("MEM_BYTES=$(awk"));
        assert!(script.contains("${MEM_FLAGS}"));
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

    #[test]
    fn test_wrapper_traps_termination_signals() {
        let cfg = TestWrapperScriptConfig {
            image_path: "/images/test.tar",
            image_name: "test-app",
            image_tag: "latest",
            image_tar_basename: "test-app.tar",
            load_command: "podman --root \"$PODMAN_STORAGE\" --runroot \"$PODMAN_RUN\" load < \"$LOCAL_IMAGE\"",
            container_command: "my_runner",
        };
        let script = generate_test_wrapper_script(&cfg);
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
}
