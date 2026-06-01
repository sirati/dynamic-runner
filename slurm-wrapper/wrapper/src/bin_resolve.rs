//! Single concern: resolve absolute paths to `podman` and `rm`
//! (generate.rs:364-387). Faithful port: `command -v` with a fallback to
//! the bare name + a warning. Phase 1 (1B) fills the body.

/// Absolute paths threaded to the shutdown-manager (--podman-path /
/// --rm-path) and used for the wrapper's own podman invocations.
#[derive(Debug, Clone)]
pub struct ResolvedBins {
    pub podman: String,
    pub rm: String,
}

/// `command -v podman` / `command -v rm`, falling back to the bare name
/// with a warning when not found (matches the bash WARNING branches).
pub fn resolve() -> ResolvedBins {
    todo!("1B: resolve podman + rm absolute paths")
}
