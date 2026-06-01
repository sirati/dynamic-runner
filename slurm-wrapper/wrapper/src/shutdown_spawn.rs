//! Single concern: spawn the out-of-cgroup shutdown manager
//! (generate.rs:214-296): `systemd-run --user --unit` service mode with a
//! `setsid -f` fallback, same argv as the bash. Phase 1 (1K) fills bodies.

use crate::bin_resolve::ResolvedBins;
use crate::dirs::Layout;
use dynrunner_slurm_wrapper_config::WrapperConfig;

/// Which cgroup-escape primitive actually started the manager — the
/// teardown forward (`teardown.rs`) picks the matching signal path.
#[derive(Debug, Clone)]
pub enum ShutdownMode {
    /// `systemd-run --user --unit=<unit>` (cgroup escape OK).
    Systemd { unit: String },
    /// `setsid -f` fallback; pid captured from the manager's pid-file.
    Setsid { pid: u32 },
    /// Neither primitive available, or the config had no binary path.
    None,
}

/// Spawn the manager and return the mode it started in. `wrapper_pid` is
/// passed as `--wrapper-pid`. When `cfg.shutdown_manager_bin_path` is
/// `None`, returns `ShutdownMode::None` without spawning.
pub fn spawn(
    _cfg: &WrapperConfig,
    _layout: &Layout,
    _bins: &ResolvedBins,
    _wrapper_pid: u32,
) -> ShutdownMode {
    todo!("1K: systemd-run --user service mode + setsid fallback")
}
