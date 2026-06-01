//! Single concern: pre-flight orphan-container sweep (generate.rs:452-489).
//! Honours DYNRUNNER_DISABLE_PREFLIGHT_PODMAN=1. Phase 1 (1C) fills body.

/// Graceful-stop (-t 10) + `rm -af` orphan podman containers under
/// `/tmp/*/storage` (owned by this user) and the user-default storage.
/// `podman` is the resolved absolute path from `bin_resolve`.
pub fn run(_podman: &str) {
    todo!("1C: orphan-container sweep")
}
