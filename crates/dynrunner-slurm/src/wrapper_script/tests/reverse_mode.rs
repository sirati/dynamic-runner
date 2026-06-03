//! Tests for `ConnectionMode::Reverse` rendering: tunnel-port
//! substitution and the `is_observer=true` flag propagation.

use crate::config::SlurmConfig;
use crate::wrapper_script::{ConnectionMode, WrapperScriptConfig, generate_wrapper_script};

#[test]
fn reverse_mode_script_contains_tunnel_port() {
    let config = SlurmConfig::default();
    let extra: [String; 0] = [];
    let cfg = WrapperScriptConfig {
        slurm_config: &config,
        name_prefix: "asm",
        wrapper_bin_path: None,
        image_path: "/images/test.tar",
        secondary_id: "sec-02",
        image_name: "test-app",
        image_tag: "latest",
        image_tar_basename: "test-app.tar",
        load_command: "podman --root \"$PODMAN_STORAGE\" --runroot \"$PODMAN_RUN\" --cgroup-manager=cgroupfs load < \"$LOCAL_IMAGE\"",
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
        forwarded_argv: &[],
        is_observer: false,
        shutdown_manager_bin_path: None,
        mem_manager_reserved_bytes: None,
    };
    let script = generate_wrapper_script(&cfg);
    assert!(script.contains("TUNNEL_PORT"));
    assert!(script.contains("sec-02.info"));
    assert!(script.contains("localhost:$TUNNEL_PORT"));
    assert!(script.contains("my_runner --secondary"));
    // Reverse-mode connection-info file is the post-L1.7 URI
    // wire format on line 1; the Step-7 v2 envelope adds keys
    // on subsequent lines. Line 1 stays `tcp://%s:%s\n` so v1
    // readers (gateway preparation) keep working unchanged.
    assert!(script.contains(r#"printf 'tcp://%s:%s\n' "$HOSTNAME" "$TUNNEL_PORT""#));
    // v2 envelope marker MUST be present so late-joiner readers
    // (Step 8+) can detect this is a v2 record.
    assert!(script.contains("printf 'version=2\\n'"));
    // is_observer key MUST be present with the rendered literal
    // value. The default-`false` test config drives the
    // false branch — toggling `is_observer = true` on the cfg
    // and re-rendering should emit `is_observer=true` instead.
    assert!(script.contains("printf 'is_observer=false\\n'"));
    // The legacy `key=value` shape on line 1 must not reappear —
    // guards against an accidental revert that the URI parser
    // would reject at runtime.
    assert!(!script.contains("hostname=$HOSTNAME"));
    assert!(!script.contains("tunnel_port=$TUNNEL_PORT"));
}

/// `is_observer = true` propagates into the v2 envelope.
/// Companion to `reverse_mode_script_contains_tunnel_port` which
/// pins the false case.
#[test]
fn reverse_mode_script_renders_is_observer_true() {
    let config = SlurmConfig::default();
    let extra: [String; 0] = [];
    let cfg = WrapperScriptConfig {
        slurm_config: &config,
        name_prefix: "asm",
        wrapper_bin_path: None,
        image_path: "/images/test.tar",
        secondary_id: "obs-01",
        image_name: "test-app",
        image_tag: "latest",
        image_tar_basename: "test-app.tar",
        load_command: "podman --root \"$PODMAN_STORAGE\" --runroot \"$PODMAN_RUN\" --cgroup-manager=cgroupfs load < \"$LOCAL_IMAGE\"",
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
        forwarded_argv: &[],
        is_observer: true,
        shutdown_manager_bin_path: None,
        mem_manager_reserved_bytes: None,
    };
    let script = generate_wrapper_script(&cfg);
    assert!(script.contains("printf 'is_observer=true\\n'"));
}
