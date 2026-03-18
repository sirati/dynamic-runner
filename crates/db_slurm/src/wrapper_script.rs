use crate::config::SlurmConfig;

/// Configuration for generating a SLURM wrapper script.
pub struct WrapperScriptConfig<'a> {
    pub slurm_config: &'a SlurmConfig,
    pub image_path: &'a str,
    pub secondary_id: &'a str,
    pub image_name: &'a str,
    pub image_tag: &'a str,
    pub load_command: &'a str,
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
    let rndtmp = format!("/tmp/asm-{rnd_suffix}");

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
        r##"#!/bin/bash
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
trap cleanup EXIT

# Setup Podman environment for SLURM
PODMAN_STORAGE="{podman_storage}"
PODMAN_RUN="{podman_run}"
mkdir -p "$PODMAN_STORAGE" "$PODMAN_RUN"
chmod 700 "$PODMAN_STORAGE" "$PODMAN_RUN"
export XDG_RUNTIME_DIR="$PODMAN_RUN"
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

# Copy Docker image to local /tmp for faster loading
LOCAL_IMAGE="$RNDTMP/asm-tokenizer-docker.tar"
cp "{image_path}" "$LOCAL_IMAGE"

# Load Docker image with Podman
{load_command}
"##,
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

    let image_ref = format!("{}:{}", cfg.image_name, cfg.image_tag);
    let sid = cfg.secondary_id;

    match &cfg.connection {
        ConnectionMode::Reverse { .. } => {
            script.push_str(&format!(
                r##"
# Run container - reverse mode (primary tunnels to secondary via gateway)
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" --runtime /usr/bin/crun run --rm \
    --network host \
    {volumes} \
    {image_ref} \
    dynamic_batch --secondary tcp://localhost:$TUNNEL_PORT --secondary-id {sid} --secondary-quic-port $QUIC_PORT
"##
            ));
        }
        ConnectionMode::Standard {
            gateway_host,
            gateway_port,
        } => {
            script.push_str(&format!(
                r##"
# Run container - standard mode (secondary connects to primary via gateway)
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" --runtime /usr/bin/crun run --rm \
    --network host \
    {volumes} \
    {image_ref} \
    dynamic_batch --secondary tcp://{gateway_host}:{gateway_port} --secondary-id {sid} --secondary-quic-port $QUIC_PORT
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
            image_name: "asm-tokenizer",
            image_tag: "latest",
            load_command: "podman --root $PODMAN_STORAGE --runroot $PODMAN_RUN load -i $LOCAL_IMAGE",
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
    }

    #[test]
    fn reverse_mode_script_contains_tunnel_port() {
        let config = SlurmConfig::default();
        let cfg = WrapperScriptConfig {
            slurm_config: &config,
            image_path: "/images/test.tar",
            secondary_id: "sec-02",
            image_name: "asm-tokenizer",
            image_tag: "latest",
            load_command: "podman --root $PODMAN_STORAGE --runroot $PODMAN_RUN load -i $LOCAL_IMAGE",
            connection: ConnectionMode::Reverse {
                connection_info_dir: "/logs/connection_info",
            },
            run_log_dir: Some("/logs/run_001"),
        };
        let script = generate_wrapper_script(&cfg);
        assert!(script.contains("TUNNEL_PORT"));
        assert!(script.contains("sec-02.info"));
        assert!(script.contains("localhost:$TUNNEL_PORT"));
    }
}
