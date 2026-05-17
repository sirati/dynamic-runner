//! Library crate for `dynrunner-slurm-shutdown`. The binary is a thin
//! shim in `main.rs`; everything testable lives here.
//!
//! Module map (one concern each):
//! * `config`        — argv → `Config`
//! * `podman`        — `PodmanBackend` trait + `RealPodman`
//! * `shutdown_flag` — `AtomicBool` flag set by signal handlers
//! * `signals`       — install SIGTERM/SIGCONT handlers on the flag
//! * `clock`         — `Clock` trait + `RealClock` for testable sleeps
//! * `poll_loop`     — state machine
//! * `cleanup`       — `/tmp` removal + PID-file lifecycle
//! * `testing`       — `MockBackend` + `FakeClock` shared by unit and
//!   integration tests (LTO-stripped from the production binary).

pub mod cleanup;
pub mod clock;
pub mod config;
pub mod poll_loop;
pub mod podman;
pub mod shutdown_flag;
pub mod signals;
pub mod testing;
