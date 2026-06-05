//! On-wire configuration schema for the SLURM secondary wrapper binary.
//!
//! [`WrapperConfig`] is the typed replacement for every value that the
//! legacy bash generator
//! (`crates/dynrunner-slurm/src/wrapper_script/generate.rs`) baked into
//! the heredoc at render time. The renderer emits a `WrapperConfig` as
//! command-line flags (`WrapperConfig::to_args`) onto the wrapper
//! invocation; the wrapper binary parses those flags back into a
//! `WrapperConfig` (the `cli` feature) and performs the work the bash
//! used to do.
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
//! crate (which constructs and emits it). Keeping one type on both sides
//! removes any chance of the renderer and the consumer drifting.
//!
//! Encode↔decode contract: [`WrapperConfig::to_args`] (always available,
//! no clap needed) emits exactly the flags the `cli`-feature parser
//! accepts, and the parser reconstructs the identical struct. The
//! `to_args() -> parse -> assert_eq` round-trip test is the anti-drift
//! guard. The renderer only links `to_args`; the wrapper binary enables
//! the `cli` feature for the parser.

use std::path::PathBuf;

/// Everything the wrapper binary needs that the submission side knows at
/// render time. Field docs cite the `generate.rs` line/section each value
/// replaces so the port stays auditable against the bash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrapperConfig {
    /// Consumer-supplied short identifier for their program/deployment
    /// (the consumer passes e.g. `"asm"`). Prefixes BOTH the scratch dir
    /// `/tmp/<name_prefix>-<suffix>` and the container name
    /// `<name_prefix>-<suffix>-<secondary_id>`, replacing the legacy
    /// hardcoded `asm` literal — dynrunner is a framework and must not bake
    /// in any one consumer's program name. The framework provides NO
    /// default: the renderer (`dynrunner-slurm` `generate.rs`) must source
    /// this from the consumer's deployment spec.
    pub name_prefix: String,

    /// Random hex-8 suffix, fixed at render time by the generator
    /// (`generate.rs:34` `rand_hex8()`). Drives the scratch-dir prefix
    /// `/tmp/<name_prefix>-<suffix>`, the container name
    /// `<name_prefix>-<suffix>-<secondary_id>`, and the shutdown-manager
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
    /// SHA-256 (hex) of the image tarball, computed framework-side at
    /// upload time (`PodmanImageMetadata.image_hash`). The wrapper uses
    /// it ONLY as the content key for the node-local image cache
    /// (`image.rs::copy_and_load`): a node-local copy keyed by this
    /// digest is reused across every secondary on the node instead of
    /// re-reading the ~GB tarball from the shared FS per job. Content
    /// changes produce a different digest → a different cache path →
    /// automatic invalidation. May be empty for back-compat / test
    /// callers, in which case the cache is bypassed (per-job copy).
    pub image_digest: String,
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
#[derive(Debug, Clone, PartialEq, Eq)]
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

impl WrapperConfig {
    /// Emit the exact CLI flags the `cli`-feature parser accepts — the
    /// inverse of parsing. ALWAYS available (no clap dependency): the
    /// renderer (`dynrunner-slurm`) links only this side. `Option<T>`
    /// fields are omitted when `None`; the two `Vec<String>` fields are
    /// emitted as repeated flags; the `ConnectionMode` enum is emitted as
    /// `--connection <variant>` plus its variant-specific flags.
    pub fn to_args(&self) -> Vec<String> {
        let mut a: Vec<String> = Vec::new();

        let mut push = |flag: &str, value: &str| {
            a.push(flag.to_string());
            a.push(value.to_string());
        };

        push("--name-prefix", &self.name_prefix);
        push("--rand-suffix", &self.rand_suffix);
        push("--secondary-id", &self.secondary_id);

        push("--image-path", &self.image_path);
        push("--image-tar-basename", &self.image_tar_basename);
        push("--image-digest", &self.image_digest);
        push("--image-name", &self.image_name);
        push("--image-tag", &self.image_tag);
        push("--load-command", &self.load_command);

        push("--container-command", &self.container_command);
        push("--cores-spec", &self.cores_spec);
        push("--max-memory-spec", &self.max_memory_spec);
        if let Some(bytes) = self.mem_manager_reserved_bytes {
            push("--mem-manager-reserved-bytes", &bytes.to_string());
        }
        for arg in &self.forwarded_argv {
            push("--forwarded-arg", arg);
        }
        for arg in &self.extra_run_args {
            push("--extra-run-arg", arg);
        }

        push("--srcbins-network", &self.srcbins_network);
        push("--output-network", &self.output_network);
        push("--log-network", &self.log_network);
        if let Some(dir) = &self.dynrunner_network_dir {
            push("--dynrunner-network-dir", dir);
        }

        match &self.connection {
            ConnectionMode::Standard {
                gateway_host,
                gateway_port,
            } => {
                push("--connection", "standard");
                push("--gateway-host", gateway_host);
                push("--gateway-port", &gateway_port.to_string());
            }
            ConnectionMode::Reverse {
                connection_info_dir,
            } => {
                push("--connection", "reverse");
                push("--connection-info-dir", connection_info_dir);
            }
        }

        // Emitted explicitly in both states so the round-trip is unambiguous
        // (a bare presence-flag could not distinguish false from omitted).
        push(
            "--is-observer",
            if self.is_observer { "true" } else { "false" },
        );

        if let Some(path) = &self.shutdown_manager_bin_path {
            push("--shutdown-manager-bin-path", &path.to_string_lossy());
        }

        a
    }
}

#[cfg(feature = "cli")]
mod cli;

#[cfg(feature = "cli")]
pub use cli::parse_args;

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn sample(connection: ConnectionMode) -> WrapperConfig {
        WrapperConfig {
            name_prefix: "asm".to_string(),
            rand_suffix: "2f1d4e89".to_string(),
            secondary_id: "sec-0".to_string(),
            image_path: "/home/u/staged/asm-tokenizer.tar".to_string(),
            image_tar_basename: "asm-tokenizer.tar".to_string(),
            image_digest: "a1b2c3d4e5f6".to_string(),
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
}
