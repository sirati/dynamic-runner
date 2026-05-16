//! Integration tests for [`dynrunner_driver::SshMaster`] that need a
//! live sshd. Carry-over of the gateway's `master_lifetime.rs` test
//! suite, re-targeted at the new `SshMaster` API + extended for the
//! locked-design points that only the driver crate tests:
//!
//! - **T3** (`drop_cleans_master`): pinning bug-(g) — dropping a
//!   spawn-master without disconnect must take the daemon down.
//! - **T4** (`master_died_observer_emits_log`): the watcher fires
//!   `tracing::error!("SSH master exited unexpectedly")` on
//!   external master death.
//! - **T5** (`drop_kills_daemon_master`): bug-(g)-redux. Read the
//!   daemon PID via `master_pid()`, drop the master, assert ESRCH.
//! - **T-pid-is-daemon** (`master_pid_is_daemon_not_launcher`):
//!   independently re-derive the daemon PID via a fresh
//!   `ssh -O check` and assert agreement.
//! - **T-invalidation** (`invalidation_semantics`): kill the daemon
//!   externally, assert master_pid() returns last_known_pid (Some),
//!   disconnect() returns Ok(()), Drop is a no-op. Locked points
//!   (h.1), (h.2), (h.3).
//! - **T-adopt-disconnect** (`adopt_disconnect_partial_cleanup`):
//!   spawn from one handle, adopt() from a second, register a
//!   forward via the second handle, disconnect the second, assert
//!   the master is STILL alive (adopt-disconnect is partial cleanup,
//!   not termination — locked point (b)).
//! - **T-panic-in-drop** (`drop_does_not_panic_on_unkillable_master`):
//!   inject a fake kill-ladder via the cfg(test) hook so Drop sees
//!   UnkillableMaster, run the gateway under `catch_unwind`, assert
//!   no panic propagates. Locked point (j).
//!
//! These tests need an actual sshd they can authenticate against.
//! They:
//!   1. Probe TCP `localhost:22` first; skip with a clear message
//!      when sshd isn't running (most CI containers).
//!   2. Generate a temporary ed25519 keypair, append the pubkey to
//!      `~/.ssh/authorized_keys`, run the master against
//!      `localhost`, and clean the pubkey out at test end.
//!
//! Run on a host with sshd:
//!   `cargo test -p dynrunner-driver --test master_lifetime`
//!
//! Test cases live in sibling `master_lifetime/<sub>.rs` files
//! grouped by concern: `lifecycle.rs` (T3/T4/T5/pid-is-daemon),
//! `invalidation.rs` (T-invalidation), `adoption.rs`
//! (T-adopt-disconnect), `panic_safety.rs` (T-panic-in-drop).
//! Shared fixtures live in `helpers.rs`.

// Sub-modules use explicit `#[path]` because cargo treats this file
// as the crate root of a single test binary; the default
// `mod foo;` lookup would search `tests/foo.rs` and clobber its
// sibling integration-test binaries.
#[path = "master_lifetime/helpers.rs"]
mod helpers;

#[path = "master_lifetime/adoption.rs"]
mod adoption;
#[path = "master_lifetime/invalidation.rs"]
mod invalidation;
#[path = "master_lifetime/lifecycle.rs"]
mod lifecycle;
#[path = "master_lifetime/panic_safety.rs"]
mod panic_safety;
