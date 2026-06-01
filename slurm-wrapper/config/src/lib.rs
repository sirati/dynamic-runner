//! On-wire configuration schema for the SLURM secondary wrapper binary.
//!
//! [`WrapperConfig`] is the typed replacement for every value that the
//! legacy bash generator
//! (`crates/dynrunner-slurm/src/wrapper_script/generate.rs`) baked into
//! the heredoc at render time. The renderer serializes a `WrapperConfig`
//! to JSON next to the job script; the wrapper binary deserializes it
//! and performs the work the bash used to do.
//!
//! Boundary contract: this struct holds ONLY render-time inputs — values
//! the gateway/submission side knows. Everything the bash computed *at
//! job runtime on the compute node* (free ports, `command -v podman`,
//! `/proc/meminfo`, `getent` peer IPs, `$XDG_RUNTIME_DIR`, hostname,
//! `$SLURM_JOB_ID`) is NOT here — the binary recomputes it on the node,
//! exactly as the bash did.
//!
//! This crate is the single source of truth for the schema: it is a
//! member of the standalone `slurm-wrapper` workspace (built into the
//! musl binary) AND a path dependency of the repo-root `dynrunner-slurm`
//! crate (which constructs and serializes it). Keeping one type on both
//! sides removes any chance of the renderer and the consumer drifting.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Everything the wrapper binary needs that the submission side knows at
/// render time. Field docs cite the `generate.rs` line/section each value
/// replaces so the port stays auditable against the bash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WrapperConfig {
    /// Random hex-8 suffix, fixed at render time by the generator
    /// (`generate.rs:34` `rand_hex8()`). Drives the scratch-dir prefix
    /// `/tmp/asm-<suffix>` (`:35`), the container name
    /// `asm-<suffix>-<secondary_id>` (`:36`), and the shutdown-manager
    /// unit name `dynrunner-shutdown-<suffix>` (`:225`). Kept render-time
    /// (not regenerated in the binary) so the suffix is stable across a
    /// re-exec of the same job script, matching legacy behaviour.
    pub rand_suffix: String,

    /// Identifier of the secondary that runs inside the container
    /// (`config.rs` `secondary_id`). Used in the container name, the
    /// peer-info filename/body, and the `--secondary-id` CLI flag.
    pub secondary_id: String,

    // ---- image (cp + load) — generate.rs:695-711, 725-727 ----
    /// Source docker-archive tar on the compute node to copy from
    /// (`config.rs` `image_path`; `cp "{image_path}" "$LOCAL_IMAGE"`).
    pub image_path: String,
    /// Basename for the node-local copy `"$RNDTMP/<basename>"`
    /// (`config.rs` `image_tar_basename`).
    pub image_tar_basename: String,
    /// Container image name, e.g. `asm-tokenizer` (`config.rs`
    /// `image_name`). Joined as `<name>:<tag>` for the `podman run`
    /// image-ref argument (`generate.rs:734`).
    pub image_name: String,
    /// Container image tag, e.g. `latest` (`config.rs` `image_tag`).
    pub image_tag: String,
    /// Image-load command snippet (`config.rs` `load_command`). Contains
    /// the shell references `$LOCAL_IMAGE`, `$PODMAN_STORAGE`,
    /// `$PODMAN_RUN`; the binary executes it via `bash -c` with those
    /// variables exported, mirroring the bash `if ! {load_command}; then`
    /// failure-marker block (`generate.rs:706-710`).
    pub load_command: String,

    // ---- secondary container-command argv — generate.rs:842-843 ----
    /// In-container entrypoint + args preceding the framework-appended
    /// flags (`config.rs` `container_command`). The bash splices this
    /// unquoted, so the binary shell-word-splits it into argv tokens.
    pub container_command: String,
    /// Verbatim `--cores=<spec>` value (`config.rs` `cores_spec`).
    pub cores_spec: String,
    /// Verbatim `--max-memory=<spec>` value (`config.rs` `max_memory_spec`).
    pub max_memory_spec: String,
    /// When `Some`, renders `--mem-manager-reserved=<bytes>` onto the
    /// secondary argv; when `None`, the flag is omitted
    /// (`generate.rs:748-751`).
    pub mem_manager_reserved_bytes: Option<u64>,
    /// Dispatcher task-specific argv, spliced after `--src-network=...`
    /// (`config.rs` `forwarded_argv`). Passed straight into the podman
    /// argv vector — no bash re-quoting needed since the binary execs
    /// podman directly.
    pub forwarded_argv: Vec<String>,
    /// Consumer `podman run` flags inserted before the image-ref arg
    /// (`config.rs` `extra_run_args`). Passed straight into the argv.
    pub extra_run_args: Vec<String>,

    // ---- bind-mount sources (already resolved to abs paths) ----
    /// Resolved `src-network` mount source — `srcbins_mount_source` or
    /// `slurm_config.src_bins_path()` (`generate.rs:46-49`).
    pub srcbins_network: String,
    /// Resolved `out-network` mount source — `output_dir` or
    /// `slurm_config.output_path()` (`generate.rs:50-53`).
    pub output_network: String,
    /// Resolved `log-network` mount source — `run_log_dir` or
    /// `slurm_config.log_path()` (`generate.rs:54-57`).
    pub log_network: String,
    /// Optional `dynrunner-network` bind-mount source. `Some` renders the
    /// `-v <dir>:/app/dynrunner-network` volume + `DYNRUNNER_NETWORK` env;
    /// `None` omits both (`generate.rs:62-70`).
    pub dynrunner_network_dir: Option<String>,

    // ---- connection mode ----
    /// Gateway-standard vs reverse-tunnel connection
    /// (`config.rs` `ConnectionMode`).
    pub connection: ConnectionMode,
    /// Whether this secondary is a non-promotable observer; written into
    /// the v2 peer-info file as `is_observer=<bool>` (`config.rs`
    /// `is_observer`, `generate.rs:628`).
    pub is_observer: bool,

    // ---- shutdown manager ----
    /// Absolute path to the `dynrunner-slurm-shutdown` binary on the
    /// compute node. `Some` renders the out-of-cgroup shutdown-manager
    /// spawn + cleanup forward; `None` omits both (`config.rs`
    /// `shutdown_manager_bin_path`, `generate.rs:214-315`).
    pub shutdown_manager_bin_path: Option<PathBuf>,
}

/// How the secondary connects to the primary
/// (`config.rs::ConnectionMode`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionMode {
    /// Secondary dials the primary via a gateway host:port
    /// (`generate.rs:781-790`, `:634-642`). The secondary URL is
    /// `tcp://<gateway_host>:<gateway_port>`.
    Standard {
        gateway_host: String,
        gateway_port: u16,
    },
    /// Primary tunnels to the secondary; the secondary writes its
    /// peer-info into `connection_info_dir` for the primary to pick up
    /// (`generate.rs:594-632`, `:776-780`). The secondary URL is
    /// `tcp://localhost:$TUNNEL_PORT`.
    Reverse { connection_info_dir: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(connection: ConnectionMode) -> WrapperConfig {
        WrapperConfig {
            rand_suffix: "2f1d4e89".to_string(),
            secondary_id: "sec-0".to_string(),
            image_path: "/home/u/staged/asm-tokenizer.tar".to_string(),
            image_tar_basename: "asm-tokenizer.tar".to_string(),
            image_name: "asm-tokenizer".to_string(),
            image_tag: "latest".to_string(),
            load_command: "$PODMAN_BIN --root \"$PODMAN_STORAGE\" load -i \"$LOCAL_IMAGE\""
                .to_string(),
            container_command: "python -m asm_tokenizer.secondary".to_string(),
            cores_spec: "-2".to_string(),
            max_memory_spec: "-2G".to_string(),
            mem_manager_reserved_bytes: Some(524_288_000),
            forwarded_argv: vec!["--platform".to_string(), "x86".to_string()],
            extra_run_args: vec!["--ulimit".to_string(), "nofile=8192:8192".to_string()],
            srcbins_network: "/net/srcbins".to_string(),
            output_network: "/net/out".to_string(),
            log_network: "/net/log".to_string(),
            dynrunner_network_dir: Some("/net/dynrunner".to_string()),
            connection,
            is_observer: false,
            shutdown_manager_bin_path: Some(PathBuf::from("/opt/dynrunner-slurm-shutdown")),
        }
    }

    /// The schema must survive a JSON round-trip byte-for-byte at the
    /// value level — this is the renderer→binary contract.
    #[test]
    fn json_round_trip_reverse() {
        let cfg = sample(ConnectionMode::Reverse {
            connection_info_dir: "/net/conn".to_string(),
        });
        let json = serde_json::to_string(&cfg).unwrap();
        let back: WrapperConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg, back);
    }

    #[test]
    fn json_round_trip_standard() {
        let cfg = sample(ConnectionMode::Standard {
            gateway_host: "gw.cluster".to_string(),
            gateway_port: 4433,
        });
        let json = serde_json::to_string(&cfg).unwrap();
        let back: WrapperConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg, back);
    }

    /// Optional fields must round-trip in their `None`/empty shapes too.
    #[test]
    fn json_round_trip_minimal() {
        let mut cfg = sample(ConnectionMode::Standard {
            gateway_host: "gw".to_string(),
            gateway_port: 1,
        });
        cfg.mem_manager_reserved_bytes = None;
        cfg.forwarded_argv.clear();
        cfg.extra_run_args.clear();
        cfg.dynrunner_network_dir = None;
        cfg.shutdown_manager_bin_path = None;
        let json = serde_json::to_string(&cfg).unwrap();
        let back: WrapperConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg, back);
    }
}
