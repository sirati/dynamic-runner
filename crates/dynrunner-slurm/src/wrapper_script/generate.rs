//! [`generate_wrapper_script`]: the canonical secondary-mode wrapper
//! generator. The rendered script is a TINY stub — a shebang plus a
//! single `exec <wrapper-bin> <args…>` line. The Rust musl
//! `dynrunner-slurm-wrapper` binary, invoked by that `exec`, performs
//! the full secondary lifecycle (scratch-dir setup, podman storage,
//! FIFO command-relay, out-of-cgroup shutdown-manager spawn, image
//! load, container run, in-band reap teardown). The `#SBATCH`/entrypoint
//! mechanics are owned by `submit_job`; only this script body differs.
//!
//! The pre-2026-05 inline bash heredoc that this generator used to
//! render was DELETED at root: it carried the old `setsid -f` shutdown
//! fallback (which does NOT escape the slurmd cgroup, so it died with
//! the job on teardown), had no `--cgroup-parent`/`cgroup.procs` adopt,
//! and no bounded in-band reap. The binary owns all of that now. There
//! is no `Option`/`None` "legacy body" branch — `wrapper_bin_path` is
//! mandatory and every secondary runs the binary.
//!
//! See [`super`] for the higher-level rationale and
//! [`WrapperScriptConfig::shutdown_manager_bin_path`] for the
//! out-of-cgroup shutdown-manager contract (now consumed by the binary,
//! threaded through [`WrapperConfig`]).

use std::path::Path;

use dynrunner_slurm_wrapper_config::{ConnectionMode as WireConnectionMode, WrapperConfig};

use super::config::{ConnectionMode, WrapperScriptConfig};
use super::quote::{bash_quote, rand_hex8};

/// Generate the wrapper script for a SLURM job: a `#!/usr/bin/env bash`
/// shebang plus one `exec <wrapper-bin> <args…>` line. The
/// `dynrunner-slurm-wrapper` binary at [`WrapperScriptConfig::wrapper_bin_path`]
/// performs the full secondary lifecycle from the bash-quoted
/// [`WrapperConfig::to_args`] vector the stub encodes.
pub fn generate_wrapper_script(cfg: &WrapperScriptConfig<'_>) -> String {
    let rnd_suffix = rand_hex8();
    generate_wrapper_stub(cfg, cfg.wrapper_bin_path, &rnd_suffix)
}

/// Render the binary-stub wrapper body: `#!/usr/bin/env bash` followed
/// by a single `exec <bin> <args…>` line, where `<args…>` is the
/// [`WrapperConfig::to_args`] vector with each element `bash_quote`-d and
/// space-joined. The Rust musl wrapper binary parses those flags back
/// into a `WrapperConfig` (its `cli` feature) and performs the full
/// secondary lifecycle.
///
/// Every field of the emitted [`WrapperConfig`] is mapped from the
/// [`WrapperScriptConfig`] the renderer was handed plus the render-time
/// values computed here (`rnd_suffix`; the mount-source fallbacks; the
/// `ConnectionMode` translation), so the renderer and the binary share
/// one source of truth for those inputs and cannot drift.
fn generate_wrapper_stub(cfg: &WrapperScriptConfig<'_>, bin: &Path, rnd_suffix: &str) -> String {
    // Mount-source fallbacks: resolve the `/app/*-network` bind-mount
    // sources from the explicit override or the `SlurmConfig` default, so
    // the binary receives the same resolved absolute paths regardless.
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

    let connection = match &cfg.connection {
        ConnectionMode::Standard {
            gateway_host,
            gateway_port,
        } => WireConnectionMode::Standard {
            gateway_host: (*gateway_host).to_string(),
            gateway_port: *gateway_port,
        },
        ConnectionMode::Reverse {
            connection_info_dir,
        } => WireConnectionMode::Reverse {
            connection_info_dir: (*connection_info_dir).to_string(),
        },
    };

    let wire = WrapperConfig {
        name_prefix: cfg.name_prefix.to_string(),
        rand_suffix: rnd_suffix.to_string(),
        secondary_id: cfg.secondary_id.to_string(),
        image_path: cfg.image_path.to_string(),
        image_tar_basename: cfg.image_tar_basename.to_string(),
        image_digest: cfg.image_digest.to_string(),
        image_name: cfg.image_name.to_string(),
        image_tag: cfg.image_tag.to_string(),
        load_command: cfg.load_command.to_string(),
        container_command: cfg.container_command.to_string(),
        cores_spec: cfg.cores_spec.to_string(),
        max_memory_spec: cfg.max_memory_spec.to_string(),
        mem_manager_reserved_bytes: cfg.mem_manager_reserved_bytes,
        secondary_module: cfg.secondary_module.to_string(),
        extra_run_args: cfg.extra_run_args.to_vec(),
        srcbins_network,
        output_network,
        log_network,
        dynrunner_network_dir: cfg.dynrunner_network_dir.map(String::from),
        connection,
        is_observer: cfg.is_observer,
        shutdown_manager_bin_path: cfg.shutdown_manager_bin_path.map(|p| p.to_path_buf()),
    };

    let bin_q = bash_quote(&bin.display().to_string());
    let args_q: String = wire
        .to_args()
        .iter()
        .map(|a| bash_quote(a))
        .collect::<Vec<_>>()
        .join(" ");

    format!("#!/usr/bin/env bash\nexec {bin_q} {args_q}\n")
}
