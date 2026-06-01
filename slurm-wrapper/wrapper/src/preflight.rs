//! Single concern: pre-flight orphan-container sweep (generate.rs:452-489).
//! Honours DYNRUNNER_DISABLE_PREFLIGHT_PODMAN=1.

use std::os::unix::fs::MetadataExt;
use std::process::Command;

const LOG_TARGET: &str = "slurm-wrapper";

/// Graceful-stop (-t 10) + `rm -af` orphan podman containers under
/// `/tmp/*/storage` (owned by this user) and the user-default storage.
/// `podman` is the resolved absolute path from `bin_resolve`.
///
/// Faithful port of the bash heredoc (generate.rs:452-489): every podman
/// invocation swallows its errors (mirror of `2>/dev/null || true`), the
/// per-job `/tmp/*/storage` roots are skipped unless they are directories
/// owned by the current euid (mirror of `[ -d ]` + `[ -O ]`), and the
/// 10s graceful-stop window precedes the unconditional `rm -af`.
pub fn run(podman: &str) {
    if std::env::var("DYNRUNNER_DISABLE_PREFLIGHT_PODMAN").as_deref() == Ok("1") {
        tracing::info!(
            target: LOG_TARGET,
            "Pre-flight podman cleanup: skipped (DYNRUNNER_DISABLE_PREFLIGHT_PODMAN=1)"
        );
        return;
    }

    tracing::info!(
        target: LOG_TARGET,
        "Pre-flight: scanning for leftover podman containers..."
    );
    let mut found = false;

    // Phase 1: orphan per-job storage roots under /tmp/.
    let euid = nix::unistd::geteuid();
    if let Ok(entries) = std::fs::read_dir("/tmp") {
        for entry in entries.flatten() {
            let storage = entry.path().join("storage");
            // Mirror `[ -d "$orphan_storage" ] || continue`.
            let meta = match std::fs::metadata(&storage) {
                Ok(m) if m.is_dir() => m,
                _ => continue,
            };
            // Mirror `[ -O "$orphan_storage" ] || continue`: owned by euid.
            if meta.uid() != euid.as_raw() {
                continue;
            }
            // runroot = "${orphan_storage%/storage}/run" == "<entry>/run".
            let runroot = entry.path().join("run");
            let storage = storage.to_string_lossy();
            let runroot = runroot.to_string_lossy();

            // Running containers: graceful stop with 10s grace.
            let running = scoped_ps(podman, &storage, &runroot);
            let ids = parse_container_ids(&running);
            if !ids.is_empty() {
                found = true;
                tracing::info!(
                    target: LOG_TARGET,
                    "Pre-flight: stopping containers in {storage}: {}",
                    ids.join(" ")
                );
                scoped_stop(podman, &storage, &runroot, &ids);
            }
            // All containers (including stopped/exited): release storage
            // layers + bind-mount references held by exited containers.
            scoped_rm_af(podman, &storage, &runroot);
        }
    }

    // Phase 2: user-default rootless storage.
    let default_running = run_podman_capture(Command::new(podman).arg("ps").arg("-q"));
    let default_ids = parse_container_ids(&default_running);
    if !default_ids.is_empty() {
        found = true;
        tracing::info!(
            target: LOG_TARGET,
            "Pre-flight: stopping containers in default storage: {}",
            default_ids.join(" ")
        );
        let mut cmd = Command::new(podman);
        cmd.arg("stop").arg("-t").arg("10").args(&default_ids);
        run_podman_swallow(&mut cmd);
    }
    run_podman_swallow(Command::new(podman).arg("rm").arg("-af"));

    if found {
        tracing::info!(target: LOG_TARGET, "Pre-flight: cleaned up leftover containers");
    } else {
        tracing::info!(target: LOG_TARGET, "Pre-flight: no leftover containers");
    }
}

/// `<podman> --root <storage> --runroot <runroot> --cgroup-manager=cgroupfs ps -q`.
fn scoped_ps(podman: &str, storage: &str, runroot: &str) -> String {
    let mut cmd = Command::new(podman);
    cmd.arg("--root")
        .arg(storage)
        .arg("--runroot")
        .arg(runroot)
        .arg("--cgroup-manager=cgroupfs")
        .arg("ps")
        .arg("-q");
    run_podman_capture(&mut cmd)
}

/// `<podman> --root ... --runroot ... --cgroup-manager=cgroupfs stop -t 10 <ids...>`.
fn scoped_stop(podman: &str, storage: &str, runroot: &str, ids: &[String]) {
    let mut cmd = Command::new(podman);
    cmd.arg("--root")
        .arg(storage)
        .arg("--runroot")
        .arg(runroot)
        .arg("--cgroup-manager=cgroupfs")
        .arg("stop")
        .arg("-t")
        .arg("10")
        .args(ids);
    run_podman_swallow(&mut cmd);
}

/// `<podman> --root ... --runroot ... --cgroup-manager=cgroupfs rm -af`.
fn scoped_rm_af(podman: &str, storage: &str, runroot: &str) {
    let mut cmd = Command::new(podman);
    cmd.arg("--root")
        .arg(storage)
        .arg("--runroot")
        .arg(runroot)
        .arg("--cgroup-manager=cgroupfs")
        .arg("rm")
        .arg("-af");
    run_podman_swallow(&mut cmd);
}

/// Run a podman command, capturing stdout as UTF-8. Mirror of
/// `$(... 2>/dev/null || true)`: any failure yields an empty string.
fn run_podman_capture(cmd: &mut Command) -> String {
    match cmd.output() {
        Ok(out) => String::from_utf8_lossy(&out.stdout).into_owned(),
        Err(_) => String::new(),
    }
}

/// Run a podman command for its side effect, swallowing all errors.
/// Mirror of `... 2>/dev/null || true`.
fn run_podman_swallow(cmd: &mut Command) {
    let _ = cmd.output();
}

/// Split podman `ps -q` stdout into individual container ids. Splits on
/// ASCII whitespace and drops empties, mirroring the unquoted shell
/// expansion `$orphan_running` that word-splits ids into separate args.
fn parse_container_ids(stdout: &str) -> Vec<String> {
    stdout
        .split_ascii_whitespace()
        .map(|s| s.to_owned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_container_ids_splits_and_trims() {
        assert_eq!(
            parse_container_ids("a1\nb2\n  c3 \n"),
            vec!["a1".to_string(), "b2".to_string(), "c3".to_string()]
        );
    }

    #[test]
    fn parse_container_ids_empty_inputs() {
        assert_eq!(parse_container_ids(""), Vec::<String>::new());
        assert_eq!(parse_container_ids("   \n\t  \n"), Vec::<String>::new());
    }

    /// RAII guard: restore (or clear) an env var on drop so tests stay
    /// isolated regardless of the global env state.
    struct EnvGuard {
        key: &'static str,
        prev: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let prev = std::env::var(key).ok();
            std::env::set_var(key, value);
            Self { key, prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }

    #[test]
    fn run_returns_on_disable_env() {
        let _guard = EnvGuard::set("DYNRUNNER_DISABLE_PREFLIGHT_PODMAN", "1");
        // The disable path must return without invoking podman or panicking.
        run("podman");
    }
}
