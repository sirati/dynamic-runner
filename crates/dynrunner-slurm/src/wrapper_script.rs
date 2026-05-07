use crate::config::SlurmConfig;

// NOTE (parity gap): the Python wrapper generator in
// `dynamic_runner.packaging.job_manager.SlurmJobManager.generate_wrapper_script`
// is the production code path used by every SLURM dispatch today; this
// Rust generator is exported but unused. The Python side has been
// extended with a `TaskDeploymentSpec.extra_run_args: tuple[str, ...]`
// hook that interpolates consumer-supplied flags (e.g.
// `--pids-limit=16384`) into the `podman run` invocation BEFORE the
// `{image_name}:{image_tag}` argument. This Rust template intentionally
// does NOT mirror that hook yet — the consumer-facing API crosses
// only through the Python `TaskDeploymentSpec`, so adding it here would
// be dead code until the production path migrates. If/when this
// generator is wired into production, mirror the field on
// `WrapperScriptConfig` and inject it at the same position in both
// `Standard` and `Reverse` arms below.

/// Configuration for generating a SLURM wrapper script.
pub struct WrapperScriptConfig<'a> {
    pub slurm_config: &'a SlurmConfig,
    pub image_path: &'a str,
    pub secondary_id: &'a str,
    pub image_name: &'a str,
    pub image_tag: &'a str,
    pub load_command: &'a str,
    pub container_command: &'a str,
    pub connection: ConnectionMode<'a>,
    pub run_log_dir: Option<&'a str>,
}

/// How the secondary connects to the primary.
pub enum ConnectionMode<'a> {
    /// Secondary connects to primary via gateway host:port.
    Standard {
        gateway_host: &'a str,
        gateway_port: u16,
    },
    /// Primary tunnels to secondary via ProxyJump; secondary writes connection info.
    Reverse {
        connection_info_dir: &'a str,
    },
}

/// Generate a bash wrapper script for a SLURM job.
///
/// The script sets up temp dirs, Podman storage, a FIFO command relay,
/// loads the Docker image, and runs the container in the appropriate
/// connection mode.
pub fn generate_wrapper_script(cfg: &WrapperScriptConfig<'_>) -> String {
    let rnd_suffix = format!("{:08x}", rand_u32());
    let rndtmp = format!("/tmp/db-{rnd_suffix}");

    let src_tmp = format!("{rndtmp}/src");
    let out_tmp = format!("{rndtmp}/out");
    let log_tmp = format!("{rndtmp}/log");
    let podman_storage = format!("{rndtmp}/storage");
    let podman_run = format!("{rndtmp}/run");
    let socket_dir = format!("{rndtmp}/sockets");
    let cmd_socket = format!("{socket_dir}/cmd.sock");

    let srcbins_network = cfg.slurm_config.src_bins_path();
    let output_network = cfg.slurm_config.output_path();
    let log_network = cfg
        .run_log_dir
        .map(String::from)
        .unwrap_or_else(|| cfg.slurm_config.log_path());

    let mut script = format!(
        r##"#!/usr/bin/env bash
set -e

echo "=================================================="
echo "SLURM Secondary Job Starting"
echo "Node: $(hostname)"
echo "Job ID: $SLURM_JOB_ID"
echo "Time: $(date)"
echo "=================================================="

# Create temporary directories
RNDTMP="{rndtmp}"
mkdir -p "$RNDTMP"
mkdir -p "{src_tmp}"
mkdir -p "{out_tmp}"
mkdir -p "{log_tmp}"
mkdir -p "{socket_dir}"

cleanup() {{
    rm -rf "$RNDTMP" 2>/dev/null || sudo rm -rf "$RNDTMP" 2>/dev/null || true
}}
# Also cleanup on SLURM-induced signals: SIGTERM is sent by sbatch
# at time-limit / scancel, SIGHUP by an ssh disconnect, SIGINT by
# Ctrl+C from interactive jobs. Without these, the trap fires only
# on graceful exit and SLURM-killed jobs leak /tmp/asm-XXXX dirs
# until the node's /tmp fills (observed in the field on multi-day
# clusters). EXIT alone misses every non-graceful termination.
trap cleanup EXIT TERM HUP INT

# Setup Podman environment for SLURM
PODMAN_STORAGE="{podman_storage}"
PODMAN_RUN="{podman_run}"
mkdir -p "$PODMAN_STORAGE" "$PODMAN_RUN"
chmod 700 "$PODMAN_STORAGE" "$PODMAN_RUN"
export XDG_RUNTIME_DIR="$PODMAN_RUN"

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
"##
    );

    // Connection-mode-specific port allocation
    match &cfg.connection {
        ConnectionMode::Reverse { connection_info_dir } => {
            let sid = cfg.secondary_id;
            script.push_str(&format!(
                r##"
# Find two free ports: one for SSH tunnel, one for QUIC server
TUNNEL_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('', 0)); print(s.getsockname()[1]); s.close()")
QUIC_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('', 0)); print(s.getsockname()[1]); s.close()")

# Write connection info for primary
HOSTNAME=$(hostname -f)
mkdir -p "{connection_info_dir}"
echo "{sid},$HOSTNAME,$TUNNEL_PORT" > "{connection_info_dir}/{sid}.info"
"##
            ));
        }
        ConnectionMode::Standard { .. } => {
            script.push_str(
                r##"
# Find a free port for QUIC server
QUIC_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('', 0)); print(s.getsockname()[1]); s.close()")
"##,
            );
        }
    }

    // FIFO command relay
    script.push_str(&format!(
        r##"
# Start command relay service in background
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
                mkfifo "$OUTPUT_SOCK"
                mkfifo "$EXIT_SOCK"
                mkfifo "$SIGNAL_SOCK"

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
        fi
    done
}} &
CMD_RELAY_PID=$!

# Copy Docker image to local /tmp for faster loading.
LOCAL_IMAGE="$RNDTMP/{image_name}-docker.tar"
cp "{image_path}" "$LOCAL_IMAGE"

# Load Docker image with Podman. The cp above can land a corrupt
# tarball on flaky local FS / transient I/O — we've observed
# `gzip: invalid checksum` on 1-of-N nodes with a known-good
# gateway tarball. Without retry, that one node's failure cascades
# into the whole dispatch timing out at `connect_timeout` (10
# minutes default) waiting for the now-dead secondary. Retry once
# with a fresh cp — if a transit-corruption flake repeats twice
# the underlying issue is usually deeper than transient and
# should fail loud, but a single retry catches the common case.
if ! {load_command}; then
    echo "podman load failed; re-copying image and retrying once" >&2
    rm -f "$LOCAL_IMAGE"
    cp "{image_path}" "$LOCAL_IMAGE"
    {load_command}
fi
"##,
        image_name = cfg.image_name,
        image_path = cfg.image_path,
        load_command = cfg.load_command,
    ));

    // Volume mounts (common to both modes)
    let volumes = format!(
        r#"-v "{src_tmp}:/app/src-tmp" \
    -v "{out_tmp}:/app/out-tmp" \
    -v "{log_tmp}:/app/log-tmp" \
    -v "{srcbins_network}:/app/src-network:ro" \
    -v "{output_network}:/app/out-network" \
    -v "{log_network}:/app/log-network" \
    -v "{socket_dir}:/app/sockets""#
    );

    // Env-var hints (common to both modes). The host-IP probe earlier
    // in the script populates these shell vars; podman forwards them
    // into the container where `network::detect_ipv{4,6}` consumes
    // them in preference to `hostname -I`. Empty values are tolerated
    // by the reader (see network.rs) so a probe miss falls back to
    // legacy detection without poisoning the chain.
    let env_flags = r#"-e PRIMARY_NODE_IPV4="$PRIMARY_NODE_IPV4" \
    -e PRIMARY_NODE_IPV6="$PRIMARY_NODE_IPV6""#;

    let image_ref = format!("{}:{}", cfg.image_name, cfg.image_tag);
    let sid = cfg.secondary_id;
    let container_command = cfg.container_command;

    match &cfg.connection {
        ConnectionMode::Reverse { .. } => {
            script.push_str(&format!(
                r##"
# Run container - reverse mode (primary tunnels to secondary via gateway).
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
    --pull=never \
    --network host \
    {env_flags} \
    {volumes} \
    {image_ref} \
    {container_command} --secondary tcp://localhost:$TUNNEL_PORT --secondary-id {sid} --secondary-quic-port $QUIC_PORT
"##
            ));
        }
        ConnectionMode::Standard {
            gateway_host,
            gateway_port,
        } => {
            script.push_str(&format!(
                r##"
# Run container - standard mode (secondary connects to primary via gateway).
# `--pull=never`: see the reverse-mode block above for the
# rationale; same incomplete-load → registry-fallback pitfall.
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" run --rm \
    --pull=never \
    --network host \
    {env_flags} \
    {volumes} \
    {image_ref} \
    {container_command} --secondary tcp://{gateway_host}:{gateway_port} --secondary-id {sid} --secondary-quic-port $QUIC_PORT
"##
            ));
        }
    }

    script.push_str(
        r#"
CONTAINER_EXIT_CODE=$?

# Kill command relay service
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

/// Simple deterministic-enough random u32 using std (no extra dep).
fn rand_u32() -> u32 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let s = RandomState::new();
    let mut h = s.build_hasher();
    h.write_u8(0);
    h.finish() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_mode_script_contains_gateway() {
        let config = SlurmConfig::default();
        let cfg = WrapperScriptConfig {
            slurm_config: &config,
            image_path: "/images/test.tar",
            secondary_id: "sec-01",
            image_name: "test-app",
            image_tag: "latest",
            load_command: "podman --root $PODMAN_STORAGE --runroot $PODMAN_RUN load -i $LOCAL_IMAGE",
            container_command: "dynamic_batch_tokenizer",
            connection: ConnectionMode::Standard {
                gateway_host: "gateway.example.com",
                gateway_port: 9000,
            },
            run_log_dir: None,
        };
        let script = generate_wrapper_script(&cfg);
        assert!(script.contains("gateway.example.com:9000"));
        assert!(script.contains("--secondary-id sec-01"));
        assert!(script.contains("mkfifo"));
        assert!(!script.contains("TUNNEL_PORT"));
        assert!(script.contains("test-app-docker.tar"));
        assert!(script.contains("dynamic_batch_tokenizer --secondary"));
        // Host-IP probe + env plumbing (the bug fix this guards):
        // without these the container's `hostname -I` advertises a
        // non-routable bridge IP and peers can't dial it.
        assert!(script.contains("getent ahostsv4"));
        assert!(script.contains("PRIMARY_NODE_IPV4="));
        assert!(script.contains("-e PRIMARY_NODE_IPV4="));
        assert!(script.contains("-e PRIMARY_NODE_IPV6="));
    }

    #[test]
    fn reverse_mode_script_contains_tunnel_port() {
        let config = SlurmConfig::default();
        let cfg = WrapperScriptConfig {
            slurm_config: &config,
            image_path: "/images/test.tar",
            secondary_id: "sec-02",
            image_name: "test-app",
            image_tag: "latest",
            load_command: "podman --root $PODMAN_STORAGE --runroot $PODMAN_RUN load -i $LOCAL_IMAGE",
            container_command: "my_runner",
            connection: ConnectionMode::Reverse {
                connection_info_dir: "/logs/connection_info",
            },
            run_log_dir: Some("/logs/run_001"),
        };
        let script = generate_wrapper_script(&cfg);
        assert!(script.contains("TUNNEL_PORT"));
        assert!(script.contains("sec-02.info"));
        assert!(script.contains("localhost:$TUNNEL_PORT"));
        assert!(script.contains("my_runner --secondary"));
    }
}
