//! `dynrunner-reap` — the single owner of the "reap a captured host-PID
//! set safely" concern.
//!
//! The reap logic (capture host PIDs with start-time identity,
//! SIGTERM → grace → SIGKILL → verify, PID-reuse-safe) is needed by TWO
//! callers that must never share a process tree:
//!
//!   * the **SLURM wrapper** binary, which performs a bounded SYNCHRONOUS
//!     in-band reap of the container's conmon + workload PIDs inside the
//!     `KillWait` window before it returns to SLURM; and
//!   * the **shutdown-manager** binary, the out-of-cgroup last-resort
//!     survivor that reaps the same PIDs when the wrapper itself died
//!     without grace.
//!
//! Both would otherwise carry a private copy of the SIGTERM→grace→SIGKILL
//! state-machine — the exact duplicated-logic antipattern the project
//! forbids. This crate is that one copy. It is dependency-free beyond
//! `libc` + `std`, so both musl-static binaries can `path`-depend on it
//! without growing their footprint.
//!
//! ## Boundary
//!
//! Callers build a `&[ReapTarget]` (pid + captured start time), pick
//! [`ReapGraces`], and call [`reap::reap_pids`]; they get back a
//! [`reap::ReapStatus`]. Neither caller knows the other exists, how
//! aliveness is determined, or how the signal is delivered.
//!
//! Module map (one concern each):
//! * `process_probe` — `ProcessProbe` trait + `KillProbe` (`kill(pid,0/sig)`)
//! * `clock`         — `Clock` trait + `RealClock` for testable sleeps
//! * `reap`          — the SIGTERM→grace→SIGKILL→verify state-machine
//! * `testing`       — `MockProcessProbe` + `FakeClock` test doubles
//!   shared by this crate's tests and both consumers' tests (LTO-stripped
//!   from the production binaries because they never reference them).

pub mod clock;
pub mod process_probe;
pub mod reap;
pub mod testing;
