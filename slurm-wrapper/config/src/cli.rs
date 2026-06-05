//! `cli` feature: clap parser that reconstructs a [`WrapperConfig`] from
//! argv — the inverse of [`WrapperConfig::to_args`].
//!
//! A thin `#[derive(Parser)]` MIRROR struct (`Cli`) holds clap-flat fields
//! (the `ConnectionMode` enum is split into a `--connection` discriminator
//! plus its variant-specific optional flags); `Cli::into_config` assembles
//! the public `WrapperConfig`. Keeping clap attributes on the mirror — not
//! on `WrapperConfig` itself — leaves the schema struct clap-free so the
//! renderer side (and `to_args`) never pulls clap.

use crate::{ConnectionMode, WrapperConfig};
use clap::{Parser, ValueEnum};
use std::path::PathBuf;

/// `--connection` discriminator. Variant-specific flags are validated in
/// [`Cli::into_config`] against the chosen mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ConnectionKind {
    Standard,
    Reverse,
}

/// Flat clap mirror of [`WrapperConfig`]. One flag per scalar field; the
/// two list fields are repeated flags; the connection enum is a
/// discriminator plus its variant fields.
#[derive(Debug, Parser)]
#[command(
    name = "dynrunner-slurm-wrapper",
    about = "SLURM secondary wrapper — config delivered as CLI flags."
)]
struct Cli {
    #[arg(long)]
    name_prefix: String,
    #[arg(long)]
    rand_suffix: String,
    #[arg(long)]
    secondary_id: String,

    #[arg(long)]
    image_path: String,
    #[arg(long)]
    image_tar_basename: String,
    /// SHA-256 (hex) content key for the node-local image cache
    /// (`image.rs`). Empty string → cache bypassed (per-job copy).
    #[arg(long)]
    image_digest: String,
    #[arg(long)]
    image_name: String,
    #[arg(long)]
    image_tag: String,
    // `allow_hyphen_values`: a bash snippet legitimately begins with `-`/`--`;
    // without it clap mistakes the value for the next flag.
    #[arg(long, allow_hyphen_values = true)]
    load_command: String,

    #[arg(long, allow_hyphen_values = true)]
    container_command: String,
    // Specs like `-2` / `-2G` lead with a hyphen.
    #[arg(long, allow_hyphen_values = true)]
    cores_spec: String,
    #[arg(long, allow_hyphen_values = true)]
    max_memory_spec: String,
    #[arg(long)]
    mem_manager_reserved_bytes: Option<u64>,
    /// Repeated: one `--forwarded-arg` per token, order-preserving. Each
    /// value is itself often a `--flag`, so hyphen-leading is allowed.
    #[arg(long = "forwarded-arg", allow_hyphen_values = true)]
    forwarded_arg: Vec<String>,
    /// Repeated: one `--extra-run-arg` per token, order-preserving. Each
    /// value is itself often a `--flag`, so hyphen-leading is allowed.
    #[arg(long = "extra-run-arg", allow_hyphen_values = true)]
    extra_run_arg: Vec<String>,

    #[arg(long)]
    srcbins_network: String,
    #[arg(long)]
    output_network: String,
    #[arg(long)]
    log_network: String,
    #[arg(long)]
    dynrunner_network_dir: Option<String>,

    /// Connection discriminator; selects which of the variant-specific
    /// flags below are required.
    #[arg(long, value_enum)]
    connection: ConnectionKind,
    /// Required iff `--connection standard`.
    #[arg(long)]
    gateway_host: Option<String>,
    /// Required iff `--connection standard`.
    #[arg(long)]
    gateway_port: Option<u16>,
    /// Required iff `--connection reverse`.
    #[arg(long)]
    connection_info_dir: Option<String>,

    /// Explicit `true`/`false` value (not a bare presence flag) so both
    /// states round-trip unambiguously. `ArgAction::Set` forces a value
    /// argument instead of clap's default `SetTrue` presence semantics.
    #[arg(long, action = clap::ArgAction::Set)]
    is_observer: bool,

    #[arg(long)]
    shutdown_manager_bin_path: Option<PathBuf>,
}

impl Cli {
    /// Assemble the public schema, validating the connection-mode flags
    /// against the chosen discriminator.
    fn into_config(self) -> Result<WrapperConfig, String> {
        let connection = match self.connection {
            ConnectionKind::Standard => {
                let gateway_host = self
                    .gateway_host
                    .ok_or("--connection standard requires --gateway-host")?;
                let gateway_port = self
                    .gateway_port
                    .ok_or("--connection standard requires --gateway-port")?;
                if self.connection_info_dir.is_some() {
                    return Err(
                        "--connection-info-dir is only valid with --connection reverse".to_string(),
                    );
                }
                ConnectionMode::Standard {
                    gateway_host,
                    gateway_port,
                }
            }
            ConnectionKind::Reverse => {
                let connection_info_dir = self
                    .connection_info_dir
                    .ok_or("--connection reverse requires --connection-info-dir")?;
                if self.gateway_host.is_some() || self.gateway_port.is_some() {
                    return Err(
                        "--gateway-host/--gateway-port are only valid with --connection standard"
                            .to_string(),
                    );
                }
                ConnectionMode::Reverse {
                    connection_info_dir,
                }
            }
        };

        Ok(WrapperConfig {
            name_prefix: self.name_prefix,
            rand_suffix: self.rand_suffix,
            secondary_id: self.secondary_id,
            image_path: self.image_path,
            image_tar_basename: self.image_tar_basename,
            image_digest: self.image_digest,
            image_name: self.image_name,
            image_tag: self.image_tag,
            load_command: self.load_command,
            container_command: self.container_command,
            cores_spec: self.cores_spec,
            max_memory_spec: self.max_memory_spec,
            mem_manager_reserved_bytes: self.mem_manager_reserved_bytes,
            forwarded_argv: self.forwarded_arg,
            extra_run_args: self.extra_run_arg,
            srcbins_network: self.srcbins_network,
            output_network: self.output_network,
            log_network: self.log_network,
            dynrunner_network_dir: self.dynrunner_network_dir,
            connection,
            is_observer: self.is_observer,
            shutdown_manager_bin_path: self.shutdown_manager_bin_path,
        })
    }
}

/// Parse a full argv (including `argv[0]`) into a [`WrapperConfig`].
/// Clap parse errors (missing/unknown flags, bad value types) surface as
/// the `clap::Error`; semantic connection-mode mismatches surface as a
/// formatted message. The caller maps either to a nonzero exit.
pub fn parse_args<I, T>(argv: I) -> Result<WrapperConfig, String>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    Cli::try_parse_from(argv)
        .map_err(|e| e.to_string())
        .and_then(Cli::into_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::sample;

    /// Anti-drift guard: `to_args() -> parse -> assert_eq`. The argv passed
    /// to the parser must lead with a program-name slot (clap convention).
    fn round_trip(cfg: &WrapperConfig) -> WrapperConfig {
        let mut argv = vec!["dynrunner-slurm-wrapper".to_string()];
        argv.extend(cfg.to_args());
        parse_args(argv).expect("to_args() output must parse back")
    }

    #[test]
    fn round_trip_reverse() {
        let cfg = sample(ConnectionMode::Reverse {
            connection_info_dir: "/net/conn".to_string(),
        });
        assert_eq!(round_trip(&cfg), cfg);
    }

    #[test]
    fn round_trip_standard() {
        let cfg = sample(ConnectionMode::Standard {
            gateway_host: "gw.cluster".to_string(),
            gateway_port: 4433,
        });
        assert_eq!(round_trip(&cfg), cfg);
    }

    /// Optional/empty shapes must round-trip too: `None` scalars omitted,
    /// empty list flags emit nothing, `is_observer=false` still explicit.
    #[test]
    fn round_trip_minimal() {
        let mut cfg = sample(ConnectionMode::Standard {
            gateway_host: "gw".to_string(),
            gateway_port: 1,
        });
        cfg.mem_manager_reserved_bytes = None;
        cfg.forwarded_argv.clear();
        cfg.extra_run_args.clear();
        cfg.dynrunner_network_dir = None;
        cfg.shutdown_manager_bin_path = None;
        cfg.is_observer = true;
        assert_eq!(round_trip(&cfg), cfg);
    }

    /// Repeated list flags preserve order and multiplicity.
    #[test]
    fn round_trip_preserves_list_order() {
        let mut cfg = sample(ConnectionMode::Reverse {
            connection_info_dir: "/c".to_string(),
        });
        cfg.forwarded_argv = vec!["a".into(), "b".into(), "a".into(), "c".into()];
        cfg.extra_run_args = vec!["--x".into(), "1".into()];
        assert_eq!(round_trip(&cfg), cfg);
    }

    /// Missing a required scalar flag is a parse error (nonzero exit).
    #[test]
    fn rejects_missing_required_flag() {
        let argv = vec!["dynrunner-slurm-wrapper", "--name-prefix", "asm"];
        assert!(parse_args(argv).is_err());
    }

    /// Connection-mode flag mismatch is a semantic error.
    #[test]
    fn rejects_reverse_with_gateway_flags() {
        let cfg = sample(ConnectionMode::Standard {
            gateway_host: "gw".to_string(),
            gateway_port: 1,
        });
        // Emit standard args but flip the discriminator to reverse.
        let mut argv = vec!["dynrunner-slurm-wrapper".to_string()];
        argv.extend(cfg.to_args());
        // Replace "standard" with "reverse" after the --connection flag.
        let pos = argv.iter().position(|s| s == "standard").unwrap();
        argv[pos] = "reverse".to_string();
        assert!(parse_args(argv).is_err());
    }
}
