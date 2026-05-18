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
//!   1. `systemd-run --user --unit=<name>` (preferred, service mode)
//!      — manager inherits the user's `user@<uid>.service` cgroup,
//!      NOT the slurmd job cgroup. Requires a reachable user-systemd
//!      bus socket. Service mode (no `--scope`) is used because
//!      systemd-run blocks until registration completes and returns
//!      the actual exit code synchronously; the prior scope-mode +
//!      `&` pattern was racy under SLURM TIMEOUT (could reap a
//!      still-handshaking systemd-run before the scope was ever
//!      registered — silent no-op, asm-tokenizer 2026-05-18).
//!   2. `setsid -f` (fallback) — manager runs in a new session
//!      inside the slurmd job cgroup. Used when (a) the user-bus
//!      probe fails (no `loginctl enable-linger`, stripped
//!      XDG_RUNTIME_DIR) OR (b) the bus is present but service
//!      registration fails. Cgroup escape is lost but the manager
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
        script.contains("systemd-run --user --quiet"),
        "spawn block must use `systemd-run --user --quiet` (service \
         mode, no `--scope`) so the manager inherits the user's \
         `user@<uid>.service` cgroup AND so the invocation blocks \
         synchronously until registration completes (no `&`-race \
         vs. SLURM TIMEOUT); render did not contain it"
    );
    // Regression guard: --scope must NOT appear. Scope mode is the
    // racy pattern we're moving away from.
    assert!(
        !script.contains("systemd-run --user --scope"),
        "spawn block must NOT use `systemd-run --user --scope`: scope \
         mode forces a foreground wait that requires `&` to background, \
         and the bus-registration handshake is racy w.r.t. SLURM TIMEOUT \
         (asm-tokenizer 2026-05-18: 2/4 workers silent no-op under \
         forced timeout). Service mode (--unit only) blocks until \
         registered."
    );
    // The unit name is the rnd_suffix-based scope; the prefix
    // alone is a reliable substring.
    assert!(
        script.contains("--unit=\"$SHUTDOWN_SCOPE\""),
        "spawn block must address the unit by `--unit=$SHUTDOWN_SCOPE` so \
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
        // --log-file routes the manager's own log lines to the same
        // per-job file the wrapper expects. Replaces the prior
        // systemd `--property=StandardOutput/StandardError=append:`
        // pair, which silently lost the manager's stdio on the
        // deployed systemd stack (asm-tokenizer 2026-05-18).
        "--log-file \"$SHUTDOWN_LOG_PATH\"",
        // --podman-path threads the wrapper-resolved absolute podman
        // binary path so the systemd-user-service unit doesn't
        // depend on the unit's (minimal) PATH (asm-tokenizer
        // 2026-05-18 post-6a41e3a: every `Command::new("podman")`
        // call ENOENT'd inside the unit). Mirrored in the setsid
        // fallback for CLI-contract symmetry — the count-2 expectation
        // is asserted by a sibling test below.
        "--podman-path \"$PODMAN_BIN\"",
        // --rm-path / --rmdir-path / --find-path thread absolute
        // coreutils binaries into the manager's per-entry cleanup
        // walk. The walk runs every primitive via `podman unshare
        // <bin>`; the bin must resolve absolutely because the unit's
        // PATH is minimal and the userns inherits no host-shell
        // PATH at exec time. Each flag is mirrored in both dispatch
        // branches (count-2 expectations are asserted in dedicated
        // tests below).
        "--rm-path \"$RM_BIN\"",
        "--rmdir-path \"$RMDIR_BIN\"",
        "--find-path \"$FIND_BIN\"",
    ] {
        assert!(
            script.contains(arg),
            "spawn block must include `{arg}` (per CLI contract); render did not contain it"
        );
    }
    // PODMAN_BIN resolution must run BEFORE the spawn block — the
    // spawn block references `"$PODMAN_BIN"` in both dispatch
    // branches, so the resolution has to be earlier in the rendered
    // script. The rendered text is one string, so we just assert
    // the resolution stanza is present (presence + the substring
    // tests below + bash-syntax check together prove ordering).
    assert!(
        script.contains("PODMAN_BIN=\"$(command -v podman 2>/dev/null || true)\""),
        "wrapper must resolve podman absolute path via `command -v` \
         BEFORE the spawn block (the service unit's PATH does NOT \
         inherit the wrapper's PATH); render did not contain the \
         resolution"
    );
    assert!(
        script.contains("WARNING: podman not found in wrapper PATH"),
        "PODMAN_BIN resolution must emit a stderr WARNING when \
         `command -v podman` returns empty (so an operator can spot \
         the misconfiguration in the .err file); render did not \
         contain the warning"
    );

    // Coreutils resolution stanzas — same shape as PODMAN_BIN. The
    // manager's `podman unshare <rm|rmdir|find>` walk needs absolute
    // paths because the systemd-user-service unit's PATH is minimal
    // and the userns inherits no host-shell PATH at exec time.
    assert!(
        script.contains("RM_BIN=\"$(command -v rm 2>/dev/null || true)\""),
        "wrapper must resolve rm absolute path via `command -v` \
         BEFORE the spawn block (the per-entry cleanup walk's \
         --rm-path flag references $RM_BIN in both dispatch \
         branches); render did not contain the resolution"
    );
    assert!(
        script.contains("RMDIR_BIN=\"$(command -v rmdir 2>/dev/null || true)\""),
        "wrapper must resolve rmdir absolute path via `command -v`; \
         render did not contain the resolution"
    );
    assert!(
        script.contains("FIND_BIN=\"$(command -v find 2>/dev/null || true)\""),
        "wrapper must resolve find absolute path via `command -v`; \
         render did not contain the resolution"
    );
    assert!(
        script.contains("WARNING: rm not found in wrapper PATH"),
        "RM_BIN resolution must emit a stderr WARNING when \
         `command -v rm` returns empty; render did not contain it"
    );
    assert!(
        script.contains("WARNING: rmdir not found in wrapper PATH"),
        "RMDIR_BIN resolution must emit a stderr WARNING when \
         `command -v rmdir` returns empty; render did not contain it"
    );
    assert!(
        script.contains("WARNING: find not found in wrapper PATH"),
        "FIND_BIN resolution must emit a stderr WARNING when \
         `command -v find` returns empty (find is load-bearing for \
         the cleanup walk's enumeration primitive — no fallback); \
         render did not contain it"
    );

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
        script.contains(
            "XDG_RUNTIME_DIR=\"$SYSTEMD_USER_RUNTIME_DIR\" systemd-run --user --quiet"
        ),
        "systemd-run invocation must be prefixed with \
         `XDG_RUNTIME_DIR=$SYSTEMD_USER_RUNTIME_DIR` so the bus client \
         reads the captured user-runtime dir, not podman's override; \
         render did not contain the prefix"
    );
    // The setsid fallback is now reached via the post-systemd-run
    // `if [ -z "$SHUTDOWN_MODE" ]` guard (so it also catches a
    // registration-failure return-non-zero from systemd-run, not
    // only a missing bus). Prior shape was an `elif`.
    assert!(
        script.contains("if [ -z \"$SHUTDOWN_MODE\" ] && command -v setsid >/dev/null 2>&1; then"),
        "spawn block must fall back to `setsid -f` when systemd-run is \
         not chosen OR fails registration (via `[ -z $SHUTDOWN_MODE ]` \
         re-entry); render did not contain the fallback guard"
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
    // Cgroup-escape-downgrade warning is rendered (the operator-
    // facing diagnostic when the fallback kicks in).
    assert!(
        script.contains("WARNING: shutdown manager running under setsid"),
        "setsid fallback must emit a stderr warning explaining the \
         cgroup-escape downgrade; render did not contain it"
    );

    // Bash-syntax smoke check on the spawn-block variant. The
    // standard-cfg syntax check runs only on the None variant —
    // a quoting/brace regression in the spawn block would slip
    // through otherwise.
    assert_bash_syntax_ok(&script);
}

/// Service-mode requires two `--property=` overrides:
///   - `Restart=no` so systemd doesn't auto-restart the manager
///     after it intentionally exits at cleanup completion.
///   - `StandardError=journal` so panic backtraces and any
///     pre-`--log-file-open` stderr land in `journalctl --user`
///     as a diagnostic safety net. The manager's normal log
///     output is routed by the binary itself via `--log-file`,
///     NOT by systemd's `StandardOutput/StandardError=append:`
///     properties (those were observed to silently lose stdio
///     under service mode on the deployed systemd stack at
///     asm-tokenizer 2026-05-18 — journal proved the binary
///     exec'd but no output reached the configured path).
///
/// AND: no `&` follows the systemd-run invocation — regression
/// guard against re-introducing the scope-mode race (systemd-run in
/// scope mode is foreground-blocking on CMD's lifetime; backgrounding
/// with `&` puts the bus-registration handshake into a race with
/// SLURM TIMEOUT proctrack reaping).
#[test]
fn service_mode_renders_with_required_properties() {
    let config = SlurmConfig::default();
    let bin = PathBuf::from(SHUTDOWN_BIN);
    let script = generate_wrapper_script(&cfg_with_shutdown_bin(&config, &bin));

    assert!(
        script.contains("--property=Restart=no"),
        "service-mode unit must set Restart=no so systemd does not \
         auto-restart the manager after its intentional post-cleanup \
         exit; render did not contain the property"
    );
    assert!(
        script.contains("--property=StandardError=journal"),
        "service-mode unit must route stderr to the journal as a \
         diagnostic safety net for panic backtraces; render did not \
         contain it"
    );
    // PrivateTmp=false disables systemd's default per-unit /tmp
    // namespace isolation. Without this, the manager's /tmp view
    // diverges from the wrapper's; --tmp-prefix, --log-file,
    // --pid-file, and the podman storage/runroot paths under
    // $RNDTMP would resolve inside a phantom namespace-private
    // tmpfs, NOT the on-disk directories the wrapper created
    // (asm-tokenizer 2026-05-18: journal showed manager ran to
    // completion but produced no on-disk artifacts).
    assert!(
        script.contains("--property=PrivateTmp=false"),
        "service-mode unit must set PrivateTmp=false so the manager \
         shares the wrapper's /tmp view; otherwise --tmp-prefix, \
         --log-file, --pid-file, and storage paths under $RNDTMP \
         resolve inside a namespace-private tmpfs and produce no \
         on-disk artifacts (asm-tokenizer 2026-05-18); render did \
         not contain the property"
    );
    // Regression guard: the systemd `append:<path>` properties were
    // removed because they silently lost the manager's stdio on the
    // deployed systemd stack. Log routing is now the manager's
    // responsibility via `--log-file`, NOT systemd's.
    assert!(
        !script.contains("StandardOutput=append:"),
        "service-mode unit must NOT use `StandardOutput=append:` — \
         the property was observed to silently lose stdio on the \
         deployed systemd stack (asm-tokenizer 2026-05-18); the \
         manager now owns its log destination via `--log-file`. \
         Render contained the legacy property."
    );
    assert!(
        !script.contains("StandardError=append:"),
        "service-mode unit must NOT use `StandardError=append:` — \
         see StandardOutput rationale above; render contained the \
         legacy property."
    );
    // No `&` after the systemd-run invocation. Service mode is
    // synchronous; backgrounding would re-introduce the race that
    // this whole change exists to fix. The substring we look for
    // is the closing of the systemd-run continuation block (right
    // before the bash `; then`) — if a `&` snuck in there it
    // would appear immediately before `; then`. The current
    // continuation now ends with `--podman-path "$PODMAN_BIN" 2>>...`
    // (last arg before the redirect that opens `; then`); the
    // legacy `--log-file "$SHUTDOWN_LOG_PATH" &` shape is also a
    // valid past-regression to guard against.
    assert!(
        !script.contains("--podman-path \"$PODMAN_BIN\" &"),
        "systemd-run must NOT be backgrounded with `&` in service \
         mode — that would re-introduce the scope-mode race the \
         service-mode switch was meant to fix; render contained the `&`"
    );
    assert!(
        !script.contains("--log-file \"$SHUTDOWN_LOG_PATH\" &"),
        "systemd-run must NOT be backgrounded with `&` in service \
         mode (legacy trailing-arg position guard); render contained `&`"
    );
}

/// `--property=PrivateTmp=false` is the load-bearing systemd
/// property that makes the manager's on-disk paths actually
/// resolve to the wrapper's /tmp. Pin its literal presence so
/// a regression that drops it (or changes it to `=true`) breaks
/// loudly. The systemd default for transient units varies by
/// distro/configuration: NixOS user-systemd has been observed to
/// turn it on transparently, while many other distros default
/// it off. Explicit `=false` is invariant across all backends.
#[test]
fn private_tmp_disabled_on_service_unit() {
    let config = SlurmConfig::default();
    let bin = PathBuf::from(SHUTDOWN_BIN);
    let script = generate_wrapper_script(&cfg_with_shutdown_bin(&config, &bin));
    assert!(
        script.contains("--property=PrivateTmp=false"),
        "service-mode unit MUST explicitly disable PrivateTmp; \
         render did not contain `--property=PrivateTmp=false`. Full \
         script:\n{script}"
    );
    // Regression guard: a future maintainer must not switch this
    // to `=true` to "harden" the unit — the wrapper's $RNDTMP
    // tree is the entire mechanism by which the manager
    // communicates artifacts to the wrapper/job, and a private
    // namespace would silently sever that path. Explicit deny.
    assert!(
        !script.contains("--property=PrivateTmp=true"),
        "service-mode unit MUST NOT set PrivateTmp=true — that would \
         re-introduce the namespace-isolation bug (asm-tokenizer \
         2026-05-18) that this property exists to neutralize; \
         render contained the `=true` form"
    );
}

/// `--podman-path` must be threaded into BOTH dispatch branches
/// (systemd-run and setsid-f). The systemd-run branch is the
/// critical one — the user-service unit's PATH does not inherit
/// the wrapper's PATH, so without the explicit path `podman`
/// invocations inside the manager ENOENT (asm-tokenizer
/// 2026-05-18 post-6a41e3a). The setsid branch is mirrored for
/// CLI-contract symmetry — both branches exercise the identical
/// argv shape, so a future maintainer can't accidentally drift
/// them apart.
#[test]
fn podman_path_flag_renders_in_both_dispatch_branches() {
    let config = SlurmConfig::default();
    let bin = PathBuf::from(SHUTDOWN_BIN);
    let script = generate_wrapper_script(&cfg_with_shutdown_bin(&config, &bin));
    let count = script.matches("--podman-path \"$PODMAN_BIN\"").count();
    assert_eq!(
        count, 2,
        "expected exactly two occurrences of `--podman-path \
         \"$PODMAN_BIN\"` (one per dispatch branch: systemd-run + \
         setsid-f), so the manager always knows its podman binary \
         path regardless of which primitive the runtime probe picks; \
         render contained {count}. Full script:\n{script}"
    );
}

/// `--rm-path` mirrors `--podman-path`: the cleanup walk's stage-2
/// (`podman unshare <rm> -- <file>`) needs the absolute coreutils
/// path because the systemd-user-service unit's PATH does NOT
/// contain GNU coreutils on NixOS. Both dispatch branches must
/// thread the flag so a future maintainer cannot drift them apart.
#[test]
fn rm_path_flag_renders_in_both_dispatch_branches() {
    let config = SlurmConfig::default();
    let bin = PathBuf::from(SHUTDOWN_BIN);
    let script = generate_wrapper_script(&cfg_with_shutdown_bin(&config, &bin));
    let count = script.matches("--rm-path \"$RM_BIN\"").count();
    assert_eq!(
        count, 2,
        "expected exactly two occurrences of `--rm-path \"$RM_BIN\"` \
         (one per dispatch branch: systemd-run + setsid-f); render \
         contained {count}. Full script:\n{script}"
    );
}

/// `--rmdir-path` mirrors `--rm-path` for stage-4 of the cleanup
/// walk (`podman unshare <rmdir> -- <dir>`).
#[test]
fn rmdir_path_flag_renders_in_both_dispatch_branches() {
    let config = SlurmConfig::default();
    let bin = PathBuf::from(SHUTDOWN_BIN);
    let script = generate_wrapper_script(&cfg_with_shutdown_bin(&config, &bin));
    let count = script.matches("--rmdir-path \"$RMDIR_BIN\"").count();
    assert_eq!(
        count, 2,
        "expected exactly two occurrences of `--rmdir-path \
         \"$RMDIR_BIN\"` (one per dispatch branch: systemd-run + \
         setsid-f); render contained {count}. Full script:\n{script}"
    );
}

/// `--find-path` is load-bearing: stages 1 and 3 of the cleanup
/// walk invoke `podman unshare <find> <root> ...` to enumerate
/// files and dirs. There is no host-side fallback enumeration
/// (the host UID cannot read subuid-owned podman storage subdirs),
/// so a missing find binary effectively disables cleanup.
#[test]
fn find_path_flag_renders_in_both_dispatch_branches() {
    let config = SlurmConfig::default();
    let bin = PathBuf::from(SHUTDOWN_BIN);
    let script = generate_wrapper_script(&cfg_with_shutdown_bin(&config, &bin));
    let count = script.matches("--find-path \"$FIND_BIN\"").count();
    assert_eq!(
        count, 2,
        "expected exactly two occurrences of `--find-path \
         \"$FIND_BIN\"` (one per dispatch branch: systemd-run + \
         setsid-f); render contained {count}. Full script:\n{script}"
    );
}

/// `--log-file` must be passed in BOTH dispatch branches (systemd-run
/// and setsid-f), so the manager always owns its log destination
/// regardless of which primitive the runtime probe picks. The render
/// emits the spawn block twice — once per branch — so we expect
/// exactly two occurrences of the flag.
#[test]
fn log_file_flag_renders_in_both_dispatch_branches() {
    let config = SlurmConfig::default();
    let bin = PathBuf::from(SHUTDOWN_BIN);
    let script = generate_wrapper_script(&cfg_with_shutdown_bin(&config, &bin));
    let count = script.matches("--log-file \"$SHUTDOWN_LOG_PATH\"").count();
    assert_eq!(
        count, 2,
        "expected exactly two occurrences of `--log-file \
         \"$SHUTDOWN_LOG_PATH\"` (one per dispatch branch: systemd-run \
         + setsid-f), so the manager owns its log destination under \
         either primitive; render contained {count}. Full script:\n{script}"
    );
}

/// The setsid fallback must remain reachable when systemd-run
/// REGISTRATION fails (non-zero exit from the synchronous
/// systemd-run invocation), not only when the bus probe fails.
/// This is the structural reason for moving from `if/elif` to
/// `if + if [ -z "$SHUTDOWN_MODE" ]` — the elif could only catch
/// the bus-absent case; the post-guarded second `if` also catches
/// systemd-run registration-failure (bus reachable, but unit-name
/// collision / property rejected / transient PID1 issue).
#[test]
fn setsid_fallback_reachable_on_systemd_run_failure() {
    let config = SlurmConfig::default();
    let bin = PathBuf::from(SHUTDOWN_BIN);
    let script = generate_wrapper_script(&cfg_with_shutdown_bin(&config, &bin));

    // The systemd-run invocation is wrapped in `if … ; then SHUTDOWN_MODE=systemd; else ...`.
    // On the failure branch we must clear SHUTDOWN_SCOPE so the
    // cleanup forward's `[ -n "$SHUTDOWN_SCOPE" ]` guard skips a
    // dangling unit name, and we must NOT set SHUTDOWN_MODE so the
    // subsequent `[ -z $SHUTDOWN_MODE ]` re-entry into setsid fires.
    assert!(
        script.contains("SYSTEMD_RUN_RC=$?"),
        "systemd-run failure branch must capture the exit code into \
         SYSTEMD_RUN_RC for the operator-facing warning; render did \
         not contain the capture"
    );
    assert!(
        script.contains("WARNING: systemd-run --user --unit failed"),
        "systemd-run failure branch must emit a stderr WARNING with \
         the exit code; render did not contain it"
    );
    assert!(
        script.contains("if [ -z \"$SHUTDOWN_MODE\" ] && command -v setsid >/dev/null 2>&1; then"),
        "spawn block must re-enter the setsid fallback via \
         `if [ -z $SHUTDOWN_MODE ]` AFTER the systemd-run if/else \
         (so registration failure — not only bus-absent — falls \
         through); render did not contain the re-entry guard"
    );
}

/// The dispatch is RUNTIME (two sequential bash `if`s linked by an
/// `[ -z $SHUTDOWN_MODE ]` re-entry guard), not render-time. Both
/// branches must appear in every render with the bin path set, so
/// the runtime probe has a target to dispatch into.
#[test]
fn setsid_fallback_branch_renders_when_systemd_bus_missing() {
    let config = SlurmConfig::default();
    let bin = PathBuf::from(SHUTDOWN_BIN);
    let script = generate_wrapper_script(&cfg_with_shutdown_bin(&config, &bin));

    // Both the systemd-run and the setsid -f primitives are rendered
    // once each; the bash if/if at runtime picks one. A render-time
    // collapse to a single primitive would be a regression. Match on
    // a substring that's unique to the actual invocation (NOT the
    // narrative comments/echoes that also mention each primitive).
    let systemd_count = script.matches("systemd-run --user --quiet \\").count();
    let setsid_count = script.matches("setsid -f /").count();
    assert_eq!(
        systemd_count, 1,
        "expected exactly one rendered `systemd-run --user --quiet \\` \
         invocation in the spawn block (service mode, line-continuation \
         to the --unit/--property/-- block below); got {systemd_count}. \
         Full script:\n{script}"
    );
    assert_eq!(
        setsid_count, 1,
        "expected exactly one rendered `setsid -f /<bin>` fallback \
         invocation in the spawn block; got {setsid_count}. \
         Full script:\n{script}"
    );
    // The two branches are NO LONGER joined by `elif` — the post-
    // 2026-05-18 restructure uses two sequential `if`s linked by an
    // `[ -z $SHUTDOWN_MODE ]` guard so the setsid fallback is reachable
    // BOTH when the bus probe fails AND when systemd-run registration
    // returns non-zero. The legacy `elif` shape would only catch the
    // former.
    assert!(
        !script.contains("elif command -v setsid"),
        "branches must NOT be joined by `elif` (legacy shape — only \
         catches bus-absent, not registration-failure); render still \
         contained the elif"
    );
    assert!(
        script.contains("if [ -z \"$SHUTDOWN_MODE\" ] && command -v setsid"),
        "setsid branch must be guarded by the post-systemd-run \
         `[ -z $SHUTDOWN_MODE ]` re-entry so it catches both \
         bus-absent and registration-failure cases; render did not \
         contain the guard"
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
/// empty: no `systemd-run --user`, no watchdog content.
#[test]
fn renders_no_shutdown_manager_when_path_none() {
    let config = SlurmConfig::default();
    // Default helper already passes None.
    let script = generate_wrapper_script(&standard_cfg(&config, &[]));

    assert!(
        !script.contains("systemd-run --user"),
        "with shutdown_manager_bin_path=None the script must NOT \
         contain any systemd-run spawn invocation"
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
