//! `dynrunner-slurm-wrapper` — the SLURM secondary wrapper binary.
//!
//! This binary replaces the bash heredoc that
//! `crates/dynrunner-slurm/src/wrapper_script/generate.rs` used to
//! render. Invoked by a tiny stub script as the sbatch entrypoint
//! (`exec <wrapper-bin> --config <cfg.json>`), it deserializes a
//! [`WrapperConfig`] and performs the full secondary lifecycle:
//!
//!   1. derive scratch-dir layout + create dirs        (`dirs`)
//!   2. resolve podman/rm absolute paths               (`bin_resolve`)
//!   3. install signalfd-based signal provenance       (`signals`)
//!   4. spawn the out-of-cgroup shutdown manager        (`shutdown_spawn`)
//!   5. pre-flight orphan-container sweep               (`preflight`)
//!   6. detect the container memory cap                 (`memcap`)
//!   7. resolve peer IPs + allocate ports + peer-info   (`network`)
//!   8. start the FIFO command relay                    (`relay`)
//!   9. copy + load the image                           (`image`)
//!  10. build argv + run the container to completion    (`podman_run`)
//!  11. teardown: forward shutdown nudge + drain relay  (`teardown`)
//!
//! Each step is a single-concern module with a clean boundary; `main`
//! is the only place that knows the lifecycle order. The actual wiring
//! lands in Phase 2 — for now `main` parses the config and initialises
//! logging so the scaffold compiles and the schema round-trips.

mod bin_resolve;
mod dirs;
mod image;
mod memcap;
mod network;
mod podman_run;
mod preflight;
mod relay;
mod shutdown_spawn;
mod signals;
mod teardown;

use dynrunner_slurm_wrapper_config::WrapperConfig;
use std::path::PathBuf;
use std::process::ExitCode;

/// Stable log prefix so operators can grep wrapper lines out of mixed
/// SLURM job stderr (mirrors the shutdown-manager's `[shutdown-mgr]`).
const LOG_TARGET: &str = "slurm-wrapper";

#[tokio::main]
async fn main() -> ExitCode {
    init_logging();

    let config_path = match parse_config_path(std::env::args().skip(1)) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(target: LOG_TARGET, "argv error: {e}");
            return ExitCode::from(2);
        }
    };

    let cfg = match load_config(&config_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(
                target: LOG_TARGET,
                "failed to load config {}: {e}",
                config_path.display()
            );
            return ExitCode::from(2);
        }
    };

    tracing::info!(
        target: LOG_TARGET,
        secondary_id = %cfg.secondary_id,
        suffix = %cfg.rand_suffix,
        "wrapper starting"
    );

    // Phase 2 wires the lifecycle here. The scaffold validates that the
    // config deserializes and the module boundaries compile.
    run(cfg).await
}

/// Run the secondary lifecycle. Phase 2 fills the body by composing the
/// single-concern modules in the order documented at the module top.
async fn run(_cfg: WrapperConfig) -> ExitCode {
    ExitCode::SUCCESS
}

/// `--config <path>` is the only argument. Mirrors the stub script's
/// `exec <wrapper-bin> --config <cfg.json>`.
fn parse_config_path<I: Iterator<Item = String>>(mut args: I) -> Result<PathBuf, String> {
    match args.next().as_deref() {
        Some("--config") => match args.next() {
            Some(path) => Ok(PathBuf::from(path)),
            None => Err("--config requires a path argument".to_string()),
        },
        Some(other) => Err(format!("unexpected argument: {other} (expected --config <path>)")),
        None => Err("missing --config <path>".to_string()),
    }
}

/// Read + deserialize the [`WrapperConfig`] JSON.
fn load_config(path: &PathBuf) -> Result<WrapperConfig, String> {
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    serde_json::from_slice(&bytes).map_err(|e| e.to_string())
}

/// stderr logging via `tracing`, honouring `RUST_LOG` (default `info`).
fn init_logging() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_config_flag() {
        let args = vec!["--config".to_string(), "/tmp/cfg.json".to_string()];
        assert_eq!(
            parse_config_path(args.into_iter()).unwrap(),
            PathBuf::from("/tmp/cfg.json")
        );
    }

    #[test]
    fn rejects_missing_flag() {
        assert!(parse_config_path(Vec::<String>::new().into_iter()).is_err());
        assert!(parse_config_path(vec!["--config".to_string()].into_iter()).is_err());
        assert!(parse_config_path(vec!["bogus".to_string()].into_iter()).is_err());
    }
}
