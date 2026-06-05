//! Single concern: resolve absolute paths to `podman` and `rm`
//! (generate.rs:364-387). Faithful port: `command -v` with a fallback to
//! the bare name + a warning. Phase 1 (1B) fills the body.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// Absolute paths threaded to the shutdown-manager (--podman-path /
/// --rm-path) and used for the wrapper's own podman invocations.
#[derive(Debug, Clone)]
pub struct ResolvedBins {
    pub podman: String,
    pub rm: String,
}

/// Single source of truth for `command -v <name>`: walk the `$PATH`
/// entries and return the first entry under which `name` is an existing,
/// executable file. `None` when nothing matches (or `$PATH` is
/// unset/empty). Both [`resolve_one`] (absolute-or-bare-name resolution)
/// and [`on_path`] (presence probe) build on this so PATH resolution lives
/// in exactly one place and is uniformly exec-bit correct.
pub fn which(name: &str) -> Option<String> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        // Empty PATH entries are skipped: `command -v` treats them as the
        // cwd, but resolving a system binary against cwd is never what the
        // wrapper wants and is not portable, so honor them as no-ops.
        if dir.as_os_str().is_empty() {
            continue;
        }
        let candidate = dir.join(name);
        if is_executable_file(&candidate) {
            return Some(candidate.to_string_lossy().into_owned());
        }
    }
    None
}

/// `command -v <name>` presence probe: true when `name` resolves to an
/// executable on `$PATH`. Single source of truth shared with
/// `shutdown_spawn`'s `systemd-run` probe — exec-bit correct
/// (a `command -v` for an external requires the executable bit), unlike a
/// bare `is_file()` check.
pub fn on_path(name: &str) -> bool {
    which(name).is_some()
}

/// Mirror `command -v <name>`: returns the resolved absolute path, falling
/// back to the bare `name` when nothing matches (or `$PATH` is unset/empty),
/// exactly as the bash `command -v ... || true` + `[ -z ]` branch does.
fn resolve_one(name: &str) -> String {
    which(name).unwrap_or_else(|| name.to_string())
}

/// True when `path` is a regular file with at least one executable bit set,
/// matching the executability filter `command -v` applies during PATH search.
fn is_executable_file(path: &Path) -> bool {
    match std::fs::metadata(path) {
        Ok(meta) => meta.is_file() && (meta.permissions().mode() & 0o111 != 0),
        Err(_) => false,
    }
}

/// `command -v podman` / `command -v rm`, falling back to the bare name
/// with a warning when not found (matches the bash WARNING branches).
pub fn resolve() -> ResolvedBins {
    let podman = resolve_one("podman");
    if podman == "podman" {
        tracing::warn!(
            "podman not found in wrapper PATH; shutdown-manager cleanup will rely on its \
             --podman-path default (\"podman\", PATH lookup inside the service unit) and may \
             ENOENT under systemd-user-service-mode"
        );
    }
    tracing::info!("Podman binary: {podman}");

    let rm = resolve_one("rm");
    if rm == "rm" {
        tracing::warn!(
            "rm not found in wrapper PATH; shutdown-manager cleanup will rely on its --rm-path \
             default (\"rm\", PATH lookup inside the podman-unshare userns) and likely ENOENT \
             under systemd-user-service-mode"
        );
    }
    tracing::info!("Rm binary: {rm}");

    ResolvedBins { podman, rm }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_one_finds_sh_as_absolute_path() {
        let resolved = resolve_one("sh");
        assert!(
            resolved.starts_with('/'),
            "expected absolute path, got {resolved:?}"
        );
        assert!(
            resolved.ends_with("sh"),
            "expected path ending in sh, got {resolved:?}"
        );
    }

    #[test]
    fn resolve_one_falls_back_to_bare_name_when_missing() {
        assert_eq!(
            resolve_one("definitely-not-a-real-binary-xyzzy"),
            "definitely-not-a-real-binary-xyzzy"
        );
    }

    #[test]
    fn resolve_returns_non_empty_bins() {
        let bins = resolve();
        assert!(!bins.podman.is_empty());
        assert!(!bins.rm.is_empty());
    }
}
