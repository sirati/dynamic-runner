//! Tests for the out-of-cgroup shutdown-manager spawn block.
//! Single concern: pin the rendered shape when
//! `shutdown_manager_bin_path = Some(path)` and the absence-of
//! when `= None`, plus a regression guard that the pre-2026-05
//! inline `setsid -f bash` watchdog never reappears.
//!
//! The shutdown manager itself (the `dynrunner-slurm-shutdown`
//! binary) is owned by a sibling crate; the wrapper's only job
//! here is to spawn it under one of two cgroup-escape primitives,
//! picked at runtime by the rendered bash:
//!
//!   1. `systemd-run --user --scope` (preferred) — manager inherits
//!      the user's `user@<uid>.service` cgroup, NOT the slurmd job
//!      cgroup. Requires a reachable user-systemd bus socket.
//!   2. `setsid -f` (fallback) — manager runs in a new session
//!      inside the slurmd job cgroup. Used when the user-bus probe
//!      fails (no `loginctl enable-linger`, stripped
//!      XDG_RUNTIME_DIR). Cgroup escape is lost but the manager
//!      binary at least starts and reacts to graceful exits.
//!
//! The replaced pre-2026-05 `setsid -f bash` inline watchdog is
//! DIFFERENT: it ran an in-line subshell that signalled the
//! container's pid 1 (= bash, no signal forwarding) and died on
//! cgroup teardown. The new `setsid -f <bin>` fallback launches the
//! real shutdown-manager binary which polls `--wrapper-pid` and
//! handles cleanup directly — no in-line shell, no pid-1 signal.

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
        mem_manager_reserved_bytes: None,
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
    // every one of the documented mandatory args). Each arg now
    // appears TWICE in the render (once per dispatch branch:
    // systemd-run and setsid-f), so substring presence is the
    // contract — count is intentionally not pinned here.
    //
    // `--wrapper-pid "$$"` is the wrapper-monitor opt-in: the
    // shutdown manager polls the wrapper PID each tick and falls
    // through to SIGNAL_SHUTDOWN when the wrapper disappears
    // (closing the SLURM-TIMEOUT proctrack-reap race). `$$` is
    // bash's PID-of-the-current-script — evaluated in the wrapper's
    // bash context at spawn time, NOT inside systemd-run.
    for arg in [
        "--container-name \"$CONTAINER_NAME\"",
        "--storage-root \"$PODMAN_STORAGE\"",
        "--runroot \"$PODMAN_RUN\"",
        "--tmp-prefix \"$RNDTMP\"",
        "--pid-file \"$RNDTMP/shutdown-manager.pid\"",
        "--wrapper-pid \"$$\"",
    ] {
        assert!(
            script.contains(arg),
            "spawn block must include `{arg}` (per CLI contract); render did not contain it"
        );
    }
    // Bus-probe + setsid-fallback shape: the spawn block must pick
    // its primitive at runtime, NOT at render time. Probe is on the
    // captured user-systemd dir (the wrapper's $XDG_RUNTIME_DIR is
    // overridden to $PODMAN_RUN further down for podman; the
    // captured var preserves the original).
    assert!(
        script.contains("SYSTEMD_USER_RUNTIME_DIR=\"$XDG_RUNTIME_DIR\""),
        "wrapper must capture user-systemd XDG_RUNTIME_DIR before the \
         podman override clobbers it; render did not contain the capture"
    );
    assert!(
        script.contains("[ -S \"$SYSTEMD_USER_RUNTIME_DIR/systemd/private\" ]"),
        "spawn block must probe the user-systemd bus socket before \
         choosing the systemd-run path; render did not contain the -S probe"
    );
    assert!(
        script.contains("XDG_RUNTIME_DIR=\"$SYSTEMD_USER_RUNTIME_DIR\" systemd-run --user --scope"),
        "systemd-run invocation must be prefixed with \
         `XDG_RUNTIME_DIR=$SYSTEMD_USER_RUNTIME_DIR` so the bus client \
         reads the captured user-runtime dir, not podman's override; \
         render did not contain the prefix"
    );
    assert!(
        script.contains("elif command -v setsid >/dev/null 2>&1; then"),
        "spawn block must fall back to `setsid -f` when the user-bus is \
         unreachable; render did not contain the setsid elif"
    );
    assert!(
        script.contains("setsid -f"),
        "fallback path must spawn the manager binary via `setsid -f`; \
         render did not contain it"
    );
    // setsid-pid capture via the manager's own pid-file. 50 iter *
    // 0.1s sleep == 5s timeout. Worst-case fork+exec+pid-file-write
    // is sub-millisecond, so 5s is overcomfortable.
    assert!(
        script.contains("for _ in $(seq 1 50); do"),
        "setsid fallback must wait for the manager's pid-file via a \
         poll loop (the pid-file is the only way to recover the pid \
         after setsid detaches); render did not contain the loop"
    );
    assert!(
        script.contains("SHUTDOWN_PID=$(cat \"$RNDTMP/shutdown-manager.pid\""),
        "setsid fallback must read the manager pid from its pid-file; \
         render did not contain the read"
    );
    // Bus-absent warning is rendered (the operator-facing diagnostic
    // when the cgroup-escape downgrade kicks in).
    assert!(
        script.contains("WARNING: no user-systemd bus"),
        "setsid fallback must emit a stderr warning explaining the \
         cgroup-escape downgrade; render did not contain it"
    );

    // Bash-syntax smoke check on the spawn-block variant. The
    // standard-cfg syntax check runs only on the None variant —
    // a quoting/brace regression in the spawn block would slip
    // through otherwise.
    assert_bash_syntax_ok(&script);
}

/// The dispatch is RUNTIME (bash if/elif), not render-time. Both
/// branches must appear in every render with the bin path set, so
/// the runtime probe has a target to dispatch into.
#[test]
fn setsid_fallback_branch_renders_when_systemd_bus_missing() {
    let config = SlurmConfig::default();
    let bin = PathBuf::from(SHUTDOWN_BIN);
    let script = generate_wrapper_script(&cfg_with_shutdown_bin(&config, &bin));

    // Both the systemd-run and the setsid -f primitives are rendered
    // once each; the bash if/elif at runtime picks one. A render-time
    // collapse to a single primitive would be a regression. Match on
    // a substring that's unique to the actual invocation (NOT the
    // narrative comments/echoes that also mention each primitive).
    let systemd_count = script
        .matches("systemd-run --user --scope --quiet --collect --unit=")
        .count();
    let setsid_count = script.matches("setsid -f /").count();
    assert_eq!(
        systemd_count, 1,
        "expected exactly one rendered `systemd-run --user --scope \
         --quiet --collect --unit=...` invocation in the spawn block; \
         got {systemd_count}. Full script:\n{script}"
    );
    assert_eq!(
        setsid_count, 1,
        "expected exactly one rendered `setsid -f /<bin>` fallback \
         invocation in the spawn block; got {setsid_count}. \
         Full script:\n{script}"
    );
    // The two branches are joined by `elif`, not `else if`. (sanity
    // check on the bash shape — bash uses `elif`.)
    assert!(
        script.contains("elif command -v setsid"),
        "branches must be joined by bash `elif`, not separate `if`; \
         render did not contain the elif"
    );
}

/// The `--wrapper-pid` value must be the literal bash sigil `$$`
/// (in double quotes), not a Rust-side-substituted constant. The
/// wrapper script's PID is unknowable at render time; bash
/// evaluates `$$` to the running script's PID at spawn. A renderer
/// regression that inserted, say, an env var or a hard-coded
/// number would break the manager's wrapper-monitor.
///
/// The post-2026-05 spawn block renders TWO occurrences (one per
/// dispatch branch: systemd-run + setsid-f). Both branches must use
/// the same sigil — a regression in either would slip past a
/// one-occurrence count.
#[test]
fn wrapper_pid_renders_as_literal_bash_dollar_dollar() {
    let config = SlurmConfig::default();
    let bin = PathBuf::from(SHUTDOWN_BIN);
    let script = generate_wrapper_script(&cfg_with_shutdown_bin(&config, &bin));
    // The substring must be present exactly — and ONLY inside the
    // spawn block. (No other place in the wrapper uses `--wrapper-pid`.)
    let count = script.matches("--wrapper-pid \"$$\"").count();
    assert_eq!(
        count, 2,
        "expected exactly two occurrences of `--wrapper-pid \"$$\"` (one \
         per dispatch branch: systemd-run + setsid-f); render contained \
         {count}. Full script:\n{script}"
    );
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
/// forward SIGCONT (the wake signal) symmetrically with whatever
/// primitive the spawn block picked at runtime:
///   - systemd-run path => `systemctl --user kill --signal=SIGCONT`
///   - setsid-f path    => `kill -SIGCONT "$SHUTDOWN_PID"`
/// The manager owns the idle-shutdown logic; the wrapper only
/// nudges it.
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
    // PATH the spawn block leaves SHUTDOWN_SCOPE="" and the cleanup
    // guard skips the kill so we don't error on every trap.
    assert!(
        script.contains("if [ -n \"${SHUTDOWN_SCOPE:-}\" ]; then"),
        "cleanup trap must guard the systemctl call with the \
         `${{SHUTDOWN_SCOPE:-}}` check; render did not contain it"
    );
    // Symmetric setsid-pid forward: when the spawn block falls back
    // to `setsid -f`, SHUTDOWN_SCOPE is empty and SHUTDOWN_PID is
    // set. The cleanup trap's elif must signal the pid directly.
    assert!(
        script.contains("elif [ -n \"${SHUTDOWN_PID:-}\" ]; then"),
        "cleanup trap must have an elif guarding the setsid-pid \
         forward; render did not contain it"
    );
    assert!(
        script.contains("kill -SIGCONT \"$SHUTDOWN_PID\""),
        "cleanup trap must forward SIGCONT to SHUTDOWN_PID directly \
         in the setsid-fallback branch; render did not contain it"
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
