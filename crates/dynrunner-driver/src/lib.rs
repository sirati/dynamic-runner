//! `dynrunner-driver` — public driver primitives for OpenSSH-based
//! cluster harnesses.
//!
//! # What this crate is
//!
//! Single-concern building blocks that downstream consumers (the
//! framework's e2e harness, asm-tokenizer's slurm-test-env owner,
//! third-party harnesses migrating off the framework-internal
//! `dynrunner-gateway::SshGateway`) compose into their own
//! lifecycle. The locked design's eleven points (a)–(l) define the
//! exact API surface; the doc-comments on each item carry the
//! point reference.
//!
//! Module layout (intentional one-concern split):
//! - [`ssh_master`] — owns master daemon lifetime + control path +
//!   watcher + forwarded_ports state. NO command execution.
//! - [`session`] — command execution / scp / sftp over a borrowed
//!   [`ssh_master::SshMaster`]. Future transports plug in here.
//! - [`cluster`] — single TCP probe (`is_running(ssh_port)`).
//! - [`identity`] — `ensure_dispatcher_keypair` + `write_ssh_config`.
//! - [`error`] — `SshMasterError` + `KillLadder`.
//! - [`ssh_target`] — newtype enforcing that error payloads carry
//!   only the `user@host[:port]` string, never identity-file paths.
//!
//! See the crate-level guide in `crates/dynrunner-driver/Cargo.toml`
//! for the dependency rationale.
//!
//! # What this crate is NOT
//!
//! - NOT a `bring_up`/`bring_down`/`provision_user` harness — those
//!   belong to the cluster harness (slurm-test-env's flake apps),
//!   per locked design point (l). Consumers compose with
//!   `subprocess.run` directly.
//! - NOT a replacement for the existing `dynrunner-gateway` crate
//!   surface yet — `SshGateway` continues to exist; the migration
//!   to use this crate internally is a follow-up.

pub mod cluster;
pub mod config;
pub mod error;
pub mod identity;
pub mod session;
pub mod ssh_master;
pub mod ssh_target;

pub use config::SshConfig;
pub use error::{KillLadder, SshMasterError};
pub use identity::{
    IdentityError, WriteSshConfigArgs, ensure_dispatcher_keypair, write_ssh_config,
};
pub use session::{CommandOutcome, Session};
pub use ssh_master::SshMaster;
pub use ssh_target::SshTarget;
