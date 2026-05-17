//! Single concern: register OS signal handlers that flip a
//! [`ShutdownFlag`].
//!
//! Both SIGTERM and SIGCONT funnel to the same flag. SIGCONT is used
//! because SLURM's `--signal` can deliver it (some operators prefer
//! SIGCONT over SIGTERM to avoid clashing with workload signal
//! handlers); accepting both lets the wrapper script choose.
//!
//! The closures registered with `signal_hook::low_level::register`
//! run in signal context. They only call [`ShutdownFlag::set`], which
//! is async-signal-safe (atomic store, no allocation).

use crate::shutdown_flag::ShutdownFlag;
use signal_hook::consts::signal::{SIGCONT, SIGTERM};
use signal_hook::low_level;
use std::io;

/// Install SIGTERM + SIGCONT handlers that set `flag`.
///
/// Returns Err if registration fails (e.g. SIGKILL/SIGSTOP attempted —
/// not the case here, but the signature stays honest). On success the
/// handlers stay active for the process lifetime; we don't bother
/// unregistering because the binary exits immediately after the
/// shutdown sequence.
pub fn install(flag: &ShutdownFlag) -> io::Result<()> {
    install_for(flag, SIGTERM)?;
    install_for(flag, SIGCONT)?;
    Ok(())
}

fn install_for(flag: &ShutdownFlag, signum: i32) -> io::Result<()> {
    let flag = flag.clone();
    // SAFETY: closure does only an atomic store, which is async-signal-safe.
    unsafe {
        low_level::register(signum, move || flag.set())?;
    }
    Ok(())
}
