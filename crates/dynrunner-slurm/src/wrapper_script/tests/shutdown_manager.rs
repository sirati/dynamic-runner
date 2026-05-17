//! Tests for the out-of-cgroup shutdown-manager spawn block.
//! Single concern: pin the rendered shape when
//! `shutdown_manager_bin_path = Some(path)` and the absence-of
//! when `= None`, plus a regression guard that the pre-2026-05
//! inline `setsid -f` watchdog never reappears.
//!
//! The shutdown manager itself (the `dynrunner-slurm-shutdown`
//! binary) is owned by a sibling crate; the wrapper's only job
//! here is to spawn it under `systemd-run --user --scope` so
//! the manager lives in the user's `user@<uid>.service` cgroup
//! (NOT the slurmd job cgroup, which is what killed the old
//! `setsid -f` watchdog on cgroup teardown).

use std::path::{Path, PathBuf};

use crate::config::SlurmConfig;
use crate::wrapper_script::{generate_wrapper_script, WrapperScriptConfig};

use super::standard_cfg;

/// Resolved bin path the wrapper would render. `bash_quote`
/// passes safe-chars verbatim, so a plain ASCII path appears
/// unquoted in the rendered script.
const SHUTDOWN_BIN: &str = "/opt/dynrunner/bin/dynrunner-slurm-shutdown";

fn cfg_with_shutdown_bin<'a>(
    slurm_config: &'a SlurmConfig,
    bin_path: &'a Path,
) -> WrapperScriptConfig<'a> {
    WrapperScriptConfig {
        shutdown_manager_bin_path: Some(bin_path),
        ..standard_cfg(slurm_config, &[])
    }
}

/// When `shutdown_manager_bin_path = Some(path)` the wrapper
/// must emit the systemd-run spawn invocation with all five
/// required CLI args (--container-name, --storage-root, --runroot,
/// --tmp-prefix, --pid-file), addressing the shutdown binary by
/// the resolved path the caller supplied.
#[test]
fn renders_shutdown_manager_spawn_when_path_set() {
    let config = SlurmConfig::default();
    let bin = PathBuf::from(SHUTDOWN_BIN);
    let script = generate_wrapper_script(&cfg_with_shutdown_bin(&config, &bin));

    assert!(
        script.contains("systemd-run --user --scope"),
        "spawn block must use `systemd-run --user --scope` so the \
         manager inherits the user's `user@<uid>.service` cgroup \
         (NOT the slurmd job cgroup); render did not contain it"
    );
    // The unit name is the rnd_suffix-based scope; the prefix
    // alone is a reliable substring.
    assert!(
        script.contains("--unit=\"$SHUTDOWN_SCOPE\""),
        "spawn block must address the scope by `--unit=$SHUTDOWN_SCOPE` so \
         `systemctl --user kill` can later target it; render did not contain it"
    );
    // Bin path appears verbatim (bash_quote keeps safe ASCII paths
    // unchanged).
    assert!(
        script.contains(SHUTDOWN_BIN),
        "rendered script must include the resolved shutdown-binary path"
    );
    // All five required CLI args present (the secondary subagent
    // owns the binary's CLI contract — we just assert we render
    // every one of the documented mandatory args).
    for arg in [
        "--container-name \"$CONTAINER_NAME\"",
        "--storage-root \"$PODMAN_STORAGE\"",
        "--runroot \"$PODMAN_RUN\"",
        "--tmp-prefix \"$RNDTMP\"",
        "--pid-file \"$RNDTMP/shutdown-manager.pid\"",
    ] {
        assert!(
            script.contains(arg),
            "spawn block must include `{arg}` (per CLI contract); render did not contain it"
        );
    }
    // Fallback path (no systemd-run) is rendered: the wrapper must
    // tolerate missing systemd-run gracefully with a stderr warning,
    // not abort.
    assert!(
        script.contains("WARNING: systemd-run not available"),
        "spawn block must emit a stderr warning when systemd-run is \
         missing; render did not contain it"
    );

    // Bash-syntax smoke check on the spawn-block variant. The
    // standard-cfg syntax check runs only on the None variant —
    // a quoting/brace regression in the spawn block would slip
    // through otherwise.
    assert_bash_syntax_ok(&script);
}

/// Shell out to `bash -n` to confirm the rendered script parses.
/// No-ops on a stripped CI sandbox without bash on PATH (matches
/// the pattern in `tests::syntax_and_quote`).
fn assert_bash_syntax_ok(script: &str) {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let bash_available = Command::new("bash")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !bash_available {
        return;
    }
    let mut child = Command::new("bash")
        .args(["-n", "/dev/stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn bash");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    let out = child.wait_with_output().expect("wait bash");
    assert!(
        out.status.success(),
        "bash -n rejected the rendered wrapper:\n\
         STDERR:\n{}\n--- script ---\n{}",
        String::from_utf8_lossy(&out.stderr),
        script,
    );
}

/// `shutdown_manager_bin_path = None` collapses the spawn block to
/// empty: no `systemd-run --user --scope`, no watchdog content.
#[test]
fn renders_no_shutdown_manager_when_path_none() {
    let config = SlurmConfig::default();
    // Default helper already passes None.
    let script = generate_wrapper_script(&standard_cfg(&config, &[]));

    assert!(
        !script.contains("systemd-run --user --scope"),
        "with shutdown_manager_bin_path=None the script must NOT \
         contain the systemd-run spawn invocation"
    );
    assert!(
        !script.contains("WATCHDOG:"),
        "with shutdown_manager_bin_path=None the script must NOT \
         contain the legacy WATCHDOG: log markers"
    );
    // The SHUTDOWN_SCOPE variable should not be referenced anywhere
    // (it's never assigned in the None branch).
    assert!(
        !script.contains("SHUTDOWN_SCOPE"),
        "with shutdown_manager_bin_path=None the script must NOT \
         reference SHUTDOWN_SCOPE; render leaked the variable"
    );
}

/// When the shutdown manager is enabled, the cleanup trap must
/// forward SIGCONT (the wake signal) to the scope via
/// `systemctl --user kill --signal=SIGCONT`. The manager owns the
/// idle-shutdown logic; the wrapper only nudges it.
#[test]
fn cleanup_trap_forwards_to_scope_when_enabled() {
    let config = SlurmConfig::default();
    let bin = PathBuf::from(SHUTDOWN_BIN);
    let script = generate_wrapper_script(&cfg_with_shutdown_bin(&config, &bin));

    assert!(
        script.contains("systemctl --user kill --signal=SIGCONT \"$SHUTDOWN_SCOPE\""),
        "cleanup trap must forward SIGCONT to the shutdown-manager \
         scope via `systemctl --user kill --signal=SIGCONT \
         \"$SHUTDOWN_SCOPE\"`; render did not contain it"
    );
    // The forward must be guarded: when systemd-run isn't on the
    // PATH the spawn block sets SHUTDOWN_SCOPE="" and the cleanup
    // guard skips the kill so we don't error on every trap.
    assert!(
        script.contains("if [ -n \"${SHUTDOWN_SCOPE:-}\" ]; then"),
        "cleanup trap must guard the systemctl call with the \
         `${{SHUTDOWN_SCOPE:-}}` check; render did not contain it"
    );
    // CMD_RELAY teardown stays in the trap regardless.
    assert!(script.contains("kill -TERM \"$CMD_RELAY_PID\""));
    assert!(script.contains("wait \"$CMD_RELAY_PID\""));
}

/// Regression guard: the pre-2026-05 `setsid -f bash` inline
/// watchdog must not reappear under any code path. It signalled
/// the container's pid 1 (= bash, no signal forwarding) and
/// lived inside the slurmd cgroup (so it died on cgroup
/// teardown, defeating its purpose). The replacement runs in
/// `user@<uid>.service` cgroup via `systemd-run --user`.
#[test]
fn no_watchdog_block_present() {
    let config = SlurmConfig::default();
    // Both modes — Some(path) and None — must be free of the
    // legacy pattern. Otherwise a future refactor could silently
    // resurrect it through a conditional.
    let bin = PathBuf::from(SHUTDOWN_BIN);
    let scripts = [
        ("shutdown_manager_disabled", generate_wrapper_script(&standard_cfg(&config, &[]))),
        ("shutdown_manager_enabled", generate_wrapper_script(&cfg_with_shutdown_bin(&config, &bin))),
    ];
    for (label, script) in scripts {
        assert!(
            !script.contains("setsid -f bash"),
            "{label}: rendered script must NOT contain the legacy \
             `setsid -f bash` watchdog pattern"
        );
        assert!(
            !script.contains("DYNRUNNER_DISABLE_TEARDOWN_WATCHDOG"),
            "{label}: rendered script must NOT reference the legacy \
             DYNRUNNER_DISABLE_TEARDOWN_WATCHDOG escape-hatch env var"
        );
        assert!(
            !script.contains("podman teardown watchdog"),
            "{label}: rendered script must NOT contain the legacy \
             watchdog spawn-confirmation echo"
        );
    }
}
