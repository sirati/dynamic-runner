//! Library crate for `dynrunner-slurm-shutdown`. The binary is a thin
//! shim in `main.rs`; everything testable lives here.
//!
//! Module map (one concern each):
//! * `config`        — argv → `Config`
//! * `podman`        — `PodmanBackend` trait + `RealPodman`
//! * `shutdown_flag` — `AtomicBool` flag set by signal handlers
//! * `signals`       — install SIGTERM/SIGCONT handlers on the flag
//! * `poll_loop`     — state machine
//! * `cleanup`       — `/tmp` removal + PID-file lifecycle
//! * `testing`       — `MockBackend` + the flag-coupled `FakeClock`
//!   (poll-loop fixture), re-exporting the reap crate's process-probe
//!   mock; shared by unit and integration tests (LTO-stripped from the
//!   production binary).
//!
//! The host-PID reap state-machine + the `ProcessProbe`/`Clock` traits
//! and `KillProbe`/`RealClock` live in the shared `dynrunner-reap` crate
//! (so the SLURM wrapper and this manager carry ONE reap, not two). They
//! are re-exported below as `crate::process_probe` / `crate::clock` so the
//! manager's modules keep their existing import paths.

pub mod cleanup;
pub mod config;
pub mod poll_loop;
pub mod podman;
pub mod shutdown_flag;
pub mod signals;
pub mod squeue_snapshot;
pub mod testing;

// Re-export the shared reap primitives under the manager's historical
// module paths so call sites (`crate::process_probe::KillProbe`,
// `crate::clock::RealClock`) are unchanged after the lift to the crate.
pub use dynrunner_reap::{clock, process_probe};
