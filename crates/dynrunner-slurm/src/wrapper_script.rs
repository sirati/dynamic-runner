use crate::config::SlurmConfig;

/// In-container path the framework's control-plane filesystem mount
/// (set on the host via `TaskDeploymentSpec.dynrunner_network_dir`) is
/// bound to, and the value of the `DYNRUNNER_NETWORK` env var the
/// worker uses to locate it.
const DYNRUNNER_NETWORK_CONTAINER_PATH: &str = "/app/dynrunner-network";

// Mirrors `dynamic_runner.packaging.job_manager.SlurmJobManager.generate_wrapper_script`.
// The two consumer-controlled inputs (`extra_run_args`,
// `dynrunner_network_dir`) cross the API as fields on
// `WrapperScriptConfig`; everything else (memory cap probe,
// `--pids-limit=16384` framework default, deterministic
// `--name "$CONTAINER_NAME"`) is framework-internal and emitted
// inline below.

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
    /// Consumer-supplied `podman run` flags injected BEFORE the
    /// `{image_name}:{image_tag}` argument and AFTER the framework's
    /// own flags. Each entry is bash-quoted; empty slice produces no
    /// emission. Mirrors `TaskDeploymentSpec.extra_run_args`.
    pub extra_run_args: &'a [String],
    /// Optional host path bind-mounted at `/app/dynrunner-network`
    /// with `DYNRUNNER_NETWORK=/app/dynrunner-network` exported into
    /// the container. `None` emits neither the volume nor the env.
    /// Mirrors `TaskDeploymentSpec.dynrunner_network_dir`.
    pub dynrunner_network_dir: Option<&'a str>,
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
    // Deterministic container name so the L1.6 watchdog and any
    // out-of-band `podman kill` can address this wrapper's container
    // by a stable handle, independent of podman's anonymous-name
    // assignment. Matches Python's `asm-{rnd}-{secondary_id}`.
    let container_name = format!("asm-{rnd_suffix}-{}", cfg.secondary_id);

    let src_tmp = format!("{rndtmp}/src");
    let out_tmp = format!("{rndtmp}/out");
    let log_tmp = format!("{rndtmp}/log");
    let podman_storage = format!("{rndtmp}/storage");
    let podman_run = format!("{rndtmp}/run");
    let socket_dir = format!("{rndtmp}/sockets");
    let cmd_socket = format!("{socket_dir}/cmd.sock");

    // Honors `prestaged_src_bins_path`: when the SlurmConfig has a
    // pre-staged source override, the wrapper bind-mounts that host
    // path into `/app/src-network` instead of the primary's staging
    // directory. The decision lives on `SlurmConfig` so the wrapper
    // generator stays mount-policy-agnostic.
    let srcbins_network = cfg.slurm_config.srcbins_mount_source();
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

# Deterministic container name so an out-of-band `podman kill` (and
# the L1.6 cgroup-escape watchdog) can address this wrapper's
# container by a stable handle.
CONTAINER_NAME="{container_name}"

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
else
    MEM_FLAGS=""
fi

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

    // Volume mounts (common to both modes). The optional
    // `dynrunner_network_dir` bind is the consumer-controlled
    // control-plane filesystem mount (see
    // `TaskDeploymentSpec.dynrunner_network_dir`); when unset, no
    // line is emitted so the rendered shape is identical to the
    // pre-feature template.
    let dynrunner_volume_line = cfg
        .dynrunner_network_dir
        .map(|host| format!("-v \"{host}:{DYNRUNNER_NETWORK_CONTAINER_PATH}\" \\\n    "))
        .unwrap_or_default();
    let volumes = format!(
        r#"-v "{src_tmp}:/app/src-tmp" \
    -v "{out_tmp}:/app/out-tmp" \
    -v "{log_tmp}:/app/log-tmp" \
    -v "{srcbins_network}:/app/src-network:ro" \
    -v "{output_network}:/app/out-network" \
    -v "{log_network}:/app/log-network" \
    {dynrunner_volume_line}-v "{socket_dir}:/app/sockets""#
    );

    // Env-var hints (common to both modes). The host-IP probe earlier
    // in the script populates these shell vars; podman forwards them
    // into the container where `network::detect_ipv{4,6}` consumes
    // them in preference to `hostname -I`. Empty values are tolerated
    // by the reader (see network.rs) so a probe miss falls back to
    // legacy detection without poisoning the chain.
    //
    // `DYNRUNNER_NETWORK` is exported alongside iff
    // `dynrunner_network_dir` is set, so the worker has a stable
    // entry-point to the control-plane mount. Built without a
    // trailing backslash-continuation so the surrounding template
    // can append its own ` \` regardless of whether the optional
    // env line is present.
    let dynrunner_env_line = cfg
        .dynrunner_network_dir
        .map(|_| {
            format!(
                "\\\n    -e DYNRUNNER_NETWORK=\"{DYNRUNNER_NETWORK_CONTAINER_PATH}\""
            )
        })
        .unwrap_or_default();
    let env_flags = format!(
        r#"-e PRIMARY_NODE_IPV4="$PRIMARY_NODE_IPV4" \
    -e PRIMARY_NODE_IPV6="$PRIMARY_NODE_IPV6"{dynrunner_env_line}"#
    );

    // Consumer-supplied `extra_run_args` interpolated BEFORE the
    // image-tag line and AFTER framework's flags. Each entry is
    // bash-quoted (Python parity with `shlex.quote`) and rendered as
    // one continuation line so the resulting `podman run` block keeps
    // the same readable shape regardless of how many flags the
    // consumer passes. Empty slice → empty string.
    let extra_run_args_block: String = cfg
        .extra_run_args
        .iter()
        .map(|arg| format!("    {} \\\n", bash_quote(arg)))
        .collect();

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
    --name "$CONTAINER_NAME" \
    --pull=never \
    --network host \
    --pids-limit=16384 \
    ${{MEM_FLAGS}} \
    {env_flags} \
    {volumes} \
{extra_run_args_block}    {image_ref} \
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
    --name "$CONTAINER_NAME" \
    --pull=never \
    --network host \
    --pids-limit=16384 \
    ${{MEM_FLAGS}} \
    {env_flags} \
    {volumes} \
{extra_run_args_block}    {image_ref} \
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

/// Bash-quote a string for safe interpolation into a shell command.
///
/// Mirrors Python's :func:`shlex.quote` semantics so consumer-supplied
/// `extra_run_args` survive the journey from `TaskDeploymentSpec`
/// through the Rust generator into the rendered wrapper bash with
/// the same quoting guarantees the Python path provides:
///
/// * empty string → ``''``
/// * value matching ``[A-Za-z0-9_@%+=:,./-]+`` → unchanged
/// * otherwise → wrapped in single quotes, with embedded ``'``
///   replaced by ``'"'"'`` (close-quote / quoted-quote / open-quote)
///
/// Single-concern helper kept inline rather than as a `shlex` crate
/// dep to avoid touching `Cargo.lock` (and the downstream
/// `cargoDeps.hash` invalidation in `nix/wheel.nix`) for one ~20-line
/// function.
fn bash_quote(s: &str) -> String {
    if s.is_empty() {
        return "''".to_string();
    }
    let safe = s.bytes().all(|b| {
        b.is_ascii_alphanumeric()
            || matches!(b, b'_' | b'@' | b'%' | b'+' | b'=' | b':' | b',' | b'.' | b'/' | b'-')
    });
    if safe {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', "'\"'\"'"))
    }
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
    use std::process::Command;

    /// Build a minimal `WrapperScriptConfig` for tests with both new
    /// consumer-facing fields defaulted to "off". Tests that exercise
    /// `extra_run_args` / `dynrunner_network_dir` set them via field
    /// update (`..base_cfg(...)` syntax) so the lifetime of the
    /// borrowed slice/str is the caller's stack frame.
    fn base_cfg<'a>(
        slurm_config: &'a SlurmConfig,
        connection: ConnectionMode<'a>,
        secondary_id: &'a str,
    ) -> WrapperScriptConfig<'a> {
        WrapperScriptConfig {
            slurm_config,
            image_path: "/images/test.tar",
            secondary_id,
            image_name: "test-app",
            image_tag: "latest",
            load_command:
                "podman --root $PODMAN_STORAGE --runroot $PODMAN_RUN load -i $LOCAL_IMAGE",
            container_command: "dynamic_batch_tokenizer",
            connection,
            run_log_dir: None,
            extra_run_args: &[],
            dynrunner_network_dir: None,
        }
    }

    /// Render the script then syntax-check it via `bash -n`. Catches
    /// quoting/continuation breakage that a literal-substring assert
    /// can miss (e.g. an unbalanced quote in `bash_quote` output, a
    /// stray dangling backslash from the env-flags trim sequence).
    fn assert_renders_valid_bash(script: &str) {
        let out = Command::new("bash")
            .arg("-n")
            .arg("/dev/stdin")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                child
                    .stdin
                    .as_mut()
                    .expect("stdin")
                    .write_all(script.as_bytes())?;
                child.wait_with_output()
            });
        match out {
            Ok(o) if o.status.success() => {}
            Ok(o) => panic!(
                "bash -n rejected rendered wrapper:\n--- stderr ---\n{}\n--- script ---\n{}",
                String::from_utf8_lossy(&o.stderr),
                script,
            ),
            Err(e) => panic!("could not spawn bash for syntax check: {e}"),
        }
    }

    #[test]
    fn standard_mode_script_contains_gateway() {
        let config = SlurmConfig::default();
        let cfg = base_cfg(
            &config,
            ConnectionMode::Standard {
                gateway_host: "gateway.example.com",
                gateway_port: 9000,
            },
            "sec-01",
        );
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
        assert_renders_valid_bash(&script);
    }

    #[test]
    fn reverse_mode_script_contains_tunnel_port() {
        let config = SlurmConfig::default();
        let cfg = WrapperScriptConfig {
            run_log_dir: Some("/logs/run_001"),
            container_command: "my_runner",
            ..base_cfg(
                &config,
                ConnectionMode::Reverse {
                    connection_info_dir: "/logs/connection_info",
                },
                "sec-02",
            )
        };
        let script = generate_wrapper_script(&cfg);
        assert!(script.contains("TUNNEL_PORT"));
        assert!(script.contains("sec-02.info"));
        assert!(script.contains("localhost:$TUNNEL_PORT"));
        assert!(script.contains("my_runner --secondary"));
        assert_renders_valid_bash(&script);
    }

    /// Framework-default flags (`--name`, `--pids-limit=16384`,
    /// `${MEM_FLAGS}` placeholder, MemTotal probe block) must appear
    /// verbatim and unconditionally — none of them are
    /// consumer-controlled. The container-name template
    /// `asm-{8-hex}-{secondary_id}` lets the L1.6 watchdog address
    /// the container by a stable handle.
    #[test]
    fn framework_defaults_emitted_in_both_modes() {
        let config = SlurmConfig::default();
        for (label, connection) in [
            (
                "standard",
                ConnectionMode::Standard {
                    gateway_host: "gw",
                    gateway_port: 1,
                },
            ),
            (
                "reverse",
                ConnectionMode::Reverse {
                    connection_info_dir: "/logs/ci",
                },
            ),
        ] {
            let cfg = base_cfg(&config, connection, "sec-z");
            let script = generate_wrapper_script(&cfg);
            assert!(
                script.contains("--name \"$CONTAINER_NAME\""),
                "[{label}] missing --name",
            );
            assert!(
                script.contains("CONTAINER_NAME=\"asm-"),
                "[{label}] container name not emitted with asm- prefix",
            );
            assert!(
                script.contains("-sec-z\""),
                "[{label}] container name suffix missing",
            );
            assert!(
                script.contains("--pids-limit=16384"),
                "[{label}] missing --pids-limit",
            );
            assert!(
                script.contains("${MEM_FLAGS}"),
                "[{label}] missing ${{MEM_FLAGS}} placeholder",
            );
            assert!(
                script.contains("/proc/meminfo"),
                "[{label}] missing MemTotal probe",
            );
            assert!(
                script.contains("--memory=${MEM_BYTES}"),
                "[{label}] missing --memory cap",
            );
            assert!(
                script.contains("--memory-swap=${MEM_BYTES}"),
                "[{label}] missing --memory-swap cap",
            );
            assert_renders_valid_bash(&script);
        }
    }

    /// `dynrunner_network_dir = None` (default) must NOT emit the
    /// volume nor the env var. Conflating it with `log-network` (or
    /// silently writing to it) is the mount-conflation bug the field
    /// exists to fix; the wrapper has to honour the absent case.
    #[test]
    fn dynrunner_network_absent_omits_volume_and_env() {
        let config = SlurmConfig::default();
        let cfg = base_cfg(
            &config,
            ConnectionMode::Standard {
                gateway_host: "gw",
                gateway_port: 1,
            },
            "sec-x",
        );
        let script = generate_wrapper_script(&cfg);
        assert!(!script.contains("/app/dynrunner-network"));
        assert!(!script.contains("DYNRUNNER_NETWORK"));
        assert_renders_valid_bash(&script);
    }

    /// `dynrunner_network_dir = Some(host)` must emit BOTH the
    /// `-v "{host}:/app/dynrunner-network"` mount and the
    /// `-e DYNRUNNER_NETWORK="/app/dynrunner-network"` env, in both
    /// connection modes.
    #[test]
    fn dynrunner_network_present_emits_volume_and_env() {
        let config = SlurmConfig::default();
        for (label, connection) in [
            (
                "standard",
                ConnectionMode::Standard {
                    gateway_host: "gw",
                    gateway_port: 1,
                },
            ),
            (
                "reverse",
                ConnectionMode::Reverse {
                    connection_info_dir: "/logs/ci",
                },
            ),
        ] {
            let cfg = WrapperScriptConfig {
                dynrunner_network_dir: Some("/srv/cluster/dynrunner"),
                ..base_cfg(&config, connection, "sec-y")
            };
            let script = generate_wrapper_script(&cfg);
            assert!(
                script.contains("-v \"/srv/cluster/dynrunner:/app/dynrunner-network\""),
                "[{label}] volume mount missing",
            );
            assert!(
                script.contains("-e DYNRUNNER_NETWORK=\"/app/dynrunner-network\""),
                "[{label}] env var missing",
            );
            assert_renders_valid_bash(&script);
        }
    }

    /// Empty `extra_run_args` must collapse cleanly between the
    /// volume block and the `image:tag` line — no trailing-backslash
    /// continuation pointing at a blank line, no orphan indent. The
    /// rendered bash must still parse and the line immediately
    /// preceding `image:tag` must end with a backslash continuation.
    #[test]
    fn extra_run_args_empty_collapses_cleanly() {
        let config = SlurmConfig::default();
        let cfg = base_cfg(
            &config,
            ConnectionMode::Standard {
                gateway_host: "gw",
                gateway_port: 1,
            },
            "sec-q",
        );
        let script = generate_wrapper_script(&cfg);
        let image_line_start = script
            .find("    test-app:latest")
            .expect("image:tag line not found");
        let preceding = &script[..image_line_start];
        let prev_line = preceding.lines().last().unwrap_or("");
        assert!(
            prev_line.ends_with('\\'),
            "line before image:tag must end with bash continuation, got {prev_line:?}",
        );
        assert!(!prev_line.trim().is_empty(), "blank line above image:tag");
        assert_renders_valid_bash(&script);
    }

    /// Each `extra_run_args` entry must appear, bash-quoted, on its
    /// own continuation line BEFORE the `image:tag` argument. The
    /// quoting check covers the three Python-parity cases: safe (no
    /// change), unsafe (single-quoted), and embedded single-quote
    /// (double-quote-sandwich escape).
    #[test]
    fn extra_run_args_injected_before_image_tag_with_bash_quoting() {
        let config = SlurmConfig::default();
        let args: Vec<String> = vec![
            "--ulimit=nofile=65536".to_string(),
            "--shm-size=2 g".to_string(),       // contains space → single-quoted
            "weird'value".to_string(),          // embedded ' → escape-sandwich
        ];
        let cfg = WrapperScriptConfig {
            extra_run_args: &args,
            ..base_cfg(
                &config,
                ConnectionMode::Standard {
                    gateway_host: "gw",
                    gateway_port: 1,
                },
                "sec-r",
            )
        };
        let script = generate_wrapper_script(&cfg);
        assert!(script.contains("--ulimit=nofile=65536"));
        assert!(script.contains("'--shm-size=2 g'"));
        assert!(script.contains(r#"'weird'"'"'value'"#));
        let image_idx = script
            .find("test-app:latest")
            .expect("image:tag line not found");
        for needle in [
            "--ulimit=nofile=65536",
            "'--shm-size=2 g'",
            r#"'weird'"'"'value'"#,
        ] {
            let pos = script.find(needle).expect(needle);
            assert!(
                pos < image_idx,
                "extra_run_args entry {needle} appears after image:tag",
            );
        }
        assert_renders_valid_bash(&script);
    }

    /// Mirrors the documented `shlex.quote` cases so a future
    /// refactor of the quoter can't silently drift from the Python
    /// parity contract.
    #[test]
    fn bash_quote_matches_python_shlex_quote() {
        assert_eq!(bash_quote(""), "''");
        assert_eq!(bash_quote("safe"), "safe");
        assert_eq!(bash_quote("--key=val"), "--key=val");
        assert_eq!(bash_quote("a/b.c_d-e"), "a/b.c_d-e");
        assert_eq!(bash_quote("with space"), "'with space'");
        assert_eq!(bash_quote("a$b"), "'a$b'");
        assert_eq!(bash_quote("a'b"), r#"'a'"'"'b'"#);
        assert_eq!(bash_quote("'"), r#"''"'"''"#);
    }

    #[test]
    fn prestaged_src_bins_redirects_src_network_mount() {
        // When SlurmConfig has a prestaged path, the wrapper must
        // bind-mount that host path into /app/src-network instead of
        // the default staging dir. Same single-source-of-truth as
        // the Python `get_srcbins_mount_source` flow.
        let config = SlurmConfig {
            prestaged_src_bins_path: Some(std::path::PathBuf::from("/srv/staged-src")),
            ..SlurmConfig::default()
        };
        let cfg = WrapperScriptConfig {
            slurm_config: &config,
            image_path: "/images/test.tar",
            secondary_id: "sec-03",
            image_name: "test-app",
            image_tag: "latest",
            load_command: "podman load -i $LOCAL_IMAGE",
            container_command: "runner",
            connection: ConnectionMode::Standard {
                gateway_host: "gateway.example.com",
                gateway_port: 9000,
            },
            run_log_dir: None,
            extra_run_args: &[],
            dynrunner_network_dir: None,
        };
        let script = generate_wrapper_script(&cfg);
        assert!(
            script.contains("\"/srv/staged-src:/app/src-network:ro\""),
            "wrapper must mount the prestaged host path into /app/src-network",
        );
        assert!(
            !script.contains("\"~/dynamic_batch/src-bins:/app/src-network:ro\""),
            "default staging dir must NOT be mounted when prestaged path is set",
        );
    }
}
