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
use std::io::Write;
use std::process::ExitCode;
use std::sync::Mutex;

/// Stable log prefix so operators can grep for our lines in mixed-job
/// stderr.
const LOG_PREFIX: &str = "[shutdown-mgr]";

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let cfg = match parse(argv) {
        Ok(c) => c,
        Err(e) => {
            // No `cfg` yet ⇒ no log-file destination. The argv-parse-
            // error path falls back to stderr alone; callers always
            // have stderr in the failure case (systemd-run captures
            // its own command stderr to the journal, and the setsid
            // path's shell redirect captures the same).
            eprintln!("{} argv error: {}", LOG_PREFIX, e);
            return ExitCode::from(2);
        }
    };
    run_with_config(cfg)
}

/// Split out from `main` so it has a return type (cargo's main can't
/// host `?` for our error model cleanly without a wrapper).
fn run_with_config(cfg: Config) -> ExitCode {
    // Open the optional log file FIRST so the very-first "starting"
    // line goes through the same destination as everything else.
    // Best-effort: an open failure degrades to stderr-only and a
    // single warning, never aborts the manager — losing logs is
    // strictly less bad than losing cleanup.
    //
    // Mutex wraps the file because the `log` closure is captured as
    // `FnMut(&str)` today (single-threaded) but the downstream
    // call-site contract has been `FnMut` for years; a future
    // multi-threaded caller would need this. Lock contention is
    // negligible (we log a handful of lines per shutdown).
    let log_file: Option<Mutex<std::fs::File>> = match &cfg.log_file {
        Some(path) => match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            Ok(f) => Some(Mutex::new(f)),
            Err(e) => {
                eprintln!(
                    "{} warning: could not open --log-file {}: {}",
                    LOG_PREFIX,
                    path.display(),
                    e
                );
                None
            }
        },
        None => None,
    };

    // log() destination: stderr always; optional log file when
    // --log-file is set. Stderr write is unconditional so panic
    // backtraces, emergency diagnostics, and the open-failure
    // warning above remain visible in whatever stdio destination
    // systemd or the wrapper happens to give us.
    //
    // `mut` because we hand `&mut log` to `run` and `final_cleanup`
    // (both `L: FnMut(&str)`). `&mut F` is itself `FnMut(&str)` when
    // `F: FnMut(&str)`, so this re-uses the SAME closure across both
    // call sites — file handle in the `log_file` Mutex stays open
    // for the binary's lifetime.
    let mut log = |msg: &str| {
        eprintln!("{} {}", LOG_PREFIX, msg);
        if let Some(file_mtx) = &log_file {
            if let Ok(mut f) = file_mtx.lock() {
                // Best-effort writes: if the disk filled up, the
                // file was unlinked underneath us, or any other I/O
                // error occurred, stderr still has the message so
                // we don't deadlock or panic the manager on logging.
                let _ = writeln!(f, "{} {}", LOG_PREFIX, msg);
                let _ = f.flush();
            }
        }
    };

    log(&format!("starting; container={}", cfg.container_name));
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
    let backend = RealPodman::new(
        cfg.podman_path.clone(),
        cfg.rm_path.clone(),
        cfg.storage_root.clone(),
        cfg.runroot.clone(),
    );
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
    let outcome = run(&backend, &flag, &clock, &probe, &poll_cfg, &mut log);
    log(&format!("state machine completed: {:?}", outcome));
    final_cleanup(&backend, &cfg.tmp_prefix, &cfg.pid_file, &mut log);
    log("exit 0");
    ExitCode::SUCCESS
}
