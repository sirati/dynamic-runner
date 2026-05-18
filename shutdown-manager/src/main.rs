//! `dynrunner-slurm-shutdown` — entry point.
//!
//! This binary does one thing: parse CLI args, install signal handlers,
//! run the state machine, and clean up. All real work lives in the
//! `dynrunner_slurm_shutdown` library crate; `main` is glue.

use dynrunner_slurm_shutdown::cleanup::{final_cleanup, write_pid_file};
use dynrunner_slurm_shutdown::clock::RealClock;
use dynrunner_slurm_shutdown::config::{Config, parse};
use dynrunner_slurm_shutdown::poll_loop::{PollConfig, run};
use dynrunner_slurm_shutdown::podman::RealPodman;
use dynrunner_slurm_shutdown::process_probe::KillProbe;
use dynrunner_slurm_shutdown::shutdown_flag::ShutdownFlag;
use dynrunner_slurm_shutdown::signals;
use std::process::ExitCode;

/// Stable log prefix so operators can grep for our lines in mixed-job
/// stderr.
const LOG_PREFIX: &str = "[shutdown-mgr]";

fn log(msg: &str) {
    eprintln!("{} {}", LOG_PREFIX, msg);
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let cfg = match parse(argv) {
        Ok(c) => c,
        Err(e) => {
            log(&format!("argv error: {}", e));
            return ExitCode::from(2);
        }
    };
    log(&format!("starting; container={}", cfg.container_name));
    run_with_config(cfg)
}

/// Split out from `main` so it has a return type (cargo's main can't
/// host `?` for our error model cleanly without a wrapper).
fn run_with_config(cfg: Config) -> ExitCode {
    if let Err(e) = write_pid_file(&cfg.pid_file) {
        log(&format!(
            "warning: could not write pid-file {}: {}",
            cfg.pid_file.display(),
            e
        ));
    }
    let flag = ShutdownFlag::new();
    if let Err(e) = signals::install(&flag) {
        log(&format!("fatal: signal install failed: {}", e));
        return ExitCode::from(3);
    }
    let backend = RealPodman::new(cfg.storage_root.clone(), cfg.runroot.clone());
    let clock = RealClock;
    let probe = KillProbe;
    let poll_cfg = PollConfig {
        container_name: cfg.container_name.clone(),
        poll_interval: cfg.poll_interval,
        idle_shutdown: cfg.idle_shutdown,
        secondary_grace: cfg.secondary_grace,
        container_stop_grace: cfg.container_stop_grace,
        wrapper_pid: cfg.wrapper_pid,
    };
    if let Some(p) = cfg.wrapper_pid {
        log(&format!("wrapper-monitor enabled; watching pid {}", p));
    }
    let outcome = run(&backend, &flag, &clock, &probe, &poll_cfg, log);
    log(&format!("state machine completed: {:?}", outcome));
    final_cleanup(&backend, &cfg.tmp_prefix, &cfg.pid_file, log);
    log("exit 0");
    ExitCode::SUCCESS
}
