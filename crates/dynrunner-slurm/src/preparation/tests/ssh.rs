//! Tests for the low-level ssh wire-up: `build_ssh_argv` shape (with
//! and without auth-options), and `verify_tunnel_alive` correctness
//! under concurrent invocation. Driven through the `super::ssh`
//! sub-module's `pub(super)` exposure.

use tokio::process::{Child, Command};
use tokio::task::JoinSet;

use crate::preparation::options::{PrepError, PreparationOptions};
use crate::preparation::ssh::{
    BindProbe, LingerLedger, LingerVerb, TunnelFailureClass, build_bind_probe_argv,
    build_linger_argv, build_release_argv, build_ssh_argv, classify_tunnel_failure,
    linger_fail_reason, linger_succeeded, parse_bind_probe, parse_was_linger, verify_tunnel_alive,
    was_linger_from_probe,
};

/// Ssh spawn argv shape (no auth-options): -J jump_target form,
/// extra_port_forwards fan out, ExitOnForwardFailure present.
/// We test by rebuilding the argv in a sibling pure-function
/// `build_ssh_argv` — extracted so the spawn path is testable
/// without launching a real subprocess.
#[test]
fn argv_no_auth_uses_proxyjump_dash_j() {
    let o = PreparationOptions::new(
        "/logs".into(),
        "gw.example".into(),
        Some("alice".into()),
        22,
        vec![],
        vec![(2222, 9090)],
    );
    let argv = build_ssh_argv("compute01", 40000, 51000, &o);
    // -J alice@gw.example
    let j_idx = argv.iter().position(|s| s == "-J").expect("has -J");
    assert_eq!(argv[j_idx + 1], "alice@gw.example");
    // -R 40000:localhost:51000 + extra -R 9090:localhost:2222
    let rs: Vec<&str> = argv
        .iter()
        .enumerate()
        .filter(|(_, s)| s.as_str() == "-R")
        .map(|(i, _)| argv[i + 1].as_str())
        .collect();
    assert_eq!(rs, vec!["40000:localhost:51000", "9090:localhost:2222"]);
    // ExitOnForwardFailure=yes is present
    assert!(argv.iter().any(|s| s == "ExitOnForwardFailure=yes"));
    // remote user@host targets compute01 with the gateway user
    // (preparation defaults remote_user to gateway_user).
    assert!(argv.iter().any(|s| s == "alice@compute01"));
    // Mux-relevant options pinned OFF: operator ControlMaster/
    // ControlPersist config must not turn this child into a master
    // handoff (instant exit 0, listener on an unowned master).
    assert!(argv.iter().any(|s| s == "ControlPath=none"));
    assert!(argv.iter().any(|s| s == "ControlMaster=no"));
    assert!(argv.iter().any(|s| s == "ControlPersist=no"));
}

/// With auth_options non-empty we MUST NOT use -J (OpenSSH 7.3+
/// regression — -o flags don't propagate). Instead a
/// ProxyCommand= with the auth flags inline.
#[test]
fn argv_with_auth_uses_proxycommand() {
    let auth = vec![
        "-i".to_string(),
        "/tmp/key".into(),
        "-o".into(),
        "IdentitiesOnly=yes".into(),
        "-o".into(),
        "IdentityAgent=none".into(),
    ];
    let o = PreparationOptions::new(
        "/logs".into(),
        "gw.example".into(),
        Some("alice".into()),
        2222,
        auth,
        vec![],
    );
    let argv = build_ssh_argv("compute01", 40000, 51000, &o);
    // No -J
    assert!(!argv.iter().any(|s| s == "-J"));
    // ProxyCommand= present, contains -i /tmp/key, IdentitiesOnly=yes,
    // -p 2222, -W %h:%p, alice@gw.example
    let proxy_cmd = argv
        .iter()
        .find(|s| s.starts_with("ProxyCommand="))
        .expect("has ProxyCommand=");
    assert!(proxy_cmd.contains("'-i' '/tmp/key'"));
    assert!(proxy_cmd.contains("'IdentitiesOnly=yes'"));
    assert!(proxy_cmd.contains("'-p' '2222'"));
    assert!(proxy_cmd.contains("'-W' '%h:%p'"));
    assert!(proxy_cmd.contains("'alice@gw.example'"));
}

/// #415 face (a) diagnostic knob: `tunnel_child_log_level` (env
/// `DYNRUNNER_SSH_TUNNEL_LOGLEVEL` at the boundary) emits `-o LogLevel=<v>`
/// on the tunnel child so a fleet-wide-drop repro can see the rekey /
/// channel-forwarding / mux lines. `None` (the default) emits NOTHING — the
/// quiet production shape — so the knob is strictly opt-in.
#[test]
fn tunnel_child_log_level_knob_emits_loglevel_only_when_set() {
    // Default: no LogLevel option on the tunnel child.
    let o = PreparationOptions::new(
        "/logs".into(),
        "gw.example".into(),
        Some("alice".into()),
        22,
        vec![],
        vec![],
    );
    let argv = build_ssh_argv("compute01", 40000, 51000, &o);
    assert!(
        !argv.iter().any(|s| s.starts_with("LogLevel=")),
        "no LogLevel option without the knob: {argv:?}"
    );

    // Set: `-o LogLevel=DEBUG1` is appended.
    let mut o2 = o.clone();
    o2.tunnel_child_log_level = Some("DEBUG1".to_string());
    let argv2 = build_ssh_argv("compute01", 40000, 51000, &o2);
    let idx = argv2
        .iter()
        .position(|s| s == "LogLevel=DEBUG1")
        .expect("LogLevel=DEBUG1 present when the knob is set");
    assert_eq!(argv2[idx - 1], "-o", "LogLevel must follow a `-o` flag");
}

/// The observer-reconnect pre-rebind RELEASE argv must:
///   1. reach the compute node over the SAME gateway jump as the
///      reverse tunnel (so it can release the binding the tunnel left),
///   2. target the SAME `tunnel_port` (option-A "same port" — the
///      worker's fixed dial target is preserved; a fresh port would
///      break `localhost:<tunnel_port>` with no re-coordination), and
///   3. run a TARGETED kill of only that port's owner — never a `-N`
///      reverse-forward and never a broad sweep.
///
/// This pins the topology decision behind the fix: the release reuses
/// the same hop + same port, it does not re-coordinate a new one.
#[test]
fn release_argv_reuses_jump_and_targets_same_port() {
    let o = PreparationOptions::new(
        "/logs".into(),
        "gw.example".into(),
        Some("alice".into()),
        22,
        vec![],
        // extra_port_forwards must NOT leak into the release command —
        // it only frees the one stale tunnel port.
        vec![(2222, 9090)],
    );
    let argv = build_release_argv("compute01", 40000, &o);
    // Same gateway jump as the tunnel (-J alice@gw.example).
    let j_idx = argv.iter().position(|s| s == "-J").expect("has -J");
    assert_eq!(argv[j_idx + 1], "alice@gw.example");
    // Same compute-node target.
    assert!(argv.iter().any(|s| s == "alice@compute01"));
    // It is NOT a reverse tunnel: no -R, no -N, no ExitOnForwardFailure.
    assert!(!argv.iter().any(|s| s == "-R"), "release must not forward");
    assert!(
        !argv.iter().any(|s| s == "-N"),
        "release must run a command"
    );
    assert!(!argv.iter().any(|s| s == "ExitOnForwardFailure=yes"));
    // The trailing remote command is a targeted release of EXACTLY
    // port 40000 (the same tunnel_port), via fuser then an ss/kill
    // fallback — both scoped to :40000, no other port mentioned.
    let remote_cmd = argv.last().expect("has a trailing remote command");
    assert!(
        remote_cmd.contains("fuser -k 40000/tcp"),
        "release must fuser-kill the exact port: {remote_cmd:?}"
    );
    assert!(
        remote_cmd.contains(":40000"),
        "ss fallback must scope to the exact port: {remote_cmd:?}"
    );
    // No collateral: neither the live primary QUIC port (51000) nor
    // an extra-forward port (9090/2222) may appear in the release cmd.
    assert!(!remote_cmd.contains("51000"));
    assert!(!remote_cmd.contains("9090"));
    assert!(!remote_cmd.contains("2222"));
}

/// With auth_options set, the release command MUST jump via
/// ProxyCommand (same OpenSSH-7.3+ workaround as the tunnel) — never
/// `-J` — so it inherits the auth flags into the inner hop.
#[test]
fn release_argv_with_auth_uses_proxycommand() {
    let auth = vec![
        "-i".to_string(),
        "/tmp/key".into(),
        "-o".into(),
        "IdentitiesOnly=yes".into(),
    ];
    let o = PreparationOptions::new(
        "/logs".into(),
        "gw.example".into(),
        Some("alice".into()),
        2222,
        auth,
        vec![],
    );
    let argv = build_release_argv("compute01", 40000, &o);
    assert!(!argv.iter().any(|s| s == "-J"));
    let proxy_cmd = argv
        .iter()
        .find(|s| s.starts_with("ProxyCommand="))
        .expect("has ProxyCommand=");
    assert!(proxy_cmd.contains("'-i' '/tmp/key'"));
    assert!(proxy_cmd.contains("'-p' '2222'"));
    assert!(proxy_cmd.contains("'-W' '%h:%p'"));
}

/// The setup-side linger ENABLE argv must:
///   1. reach the compute node over the SAME gateway jump as the reverse
///      tunnel (`-J alice@gw.example`) and target the SAME node,
///   2. FORCE a PTY (`-tt`) so the remote `loginctl` runs inside a
///      pam_systemd logind session (the proven interactive-login shape),
///   3. NOT be a reverse tunnel — no `-R`, no `-N`, no
///      `ExitOnForwardFailure`, and no extra-port-forward leakage,
///   4. run a self-targeting `loginctl enable-linger` (no positional user
///      — the ssh login user IS the run user) plus the `WAS_LINGER` probe.
#[test]
fn linger_enable_argv_forces_pty_jumps_and_self_targets() {
    let o = PreparationOptions::new(
        "/logs".into(),
        "gw.example".into(),
        Some("alice".into()),
        22,
        vec![],
        // extra_port_forwards must NOT leak into the linger command.
        vec![(2222, 9090)],
    );
    let argv = build_linger_argv("compute01", LingerVerb::Enable, &o);
    // Same gateway jump as the tunnel.
    let j_idx = argv.iter().position(|s| s == "-J").expect("has -J");
    assert_eq!(argv[j_idx + 1], "alice@gw.example");
    // Same compute-node target.
    assert!(argv.iter().any(|s| s == "alice@compute01"));
    // Forced PTY.
    assert!(
        argv.iter().any(|s| s == "-tt"),
        "must force a PTY: {argv:?}"
    );
    // Not a reverse tunnel.
    assert!(!argv.iter().any(|s| s == "-R"), "linger must not forward");
    assert!(!argv.iter().any(|s| s == "-N"), "linger must run a command");
    assert!(!argv.iter().any(|s| s == "ExitOnForwardFailure=yes"));
    // The trailing remote command enables linger (self-targeting: no
    // positional <user>) and probes the prior state.
    let remote_cmd = argv.last().expect("has a trailing remote command");
    assert!(
        remote_cmd.contains("loginctl enable-linger"),
        "must enable linger: {remote_cmd:?}"
    );
    assert!(
        !remote_cmd.contains("enable-linger alice"),
        "must be self-targeting (no positional user): {remote_cmd:?}"
    );
    assert!(
        remote_cmd.contains("WAS_LINGER="),
        "must probe the prior state: {remote_cmd:?}"
    );
    // The probe is BUS-FREE: a file test on the persistent linger marker,
    // NOT `loginctl show-user` (which needs logind/a session and once
    // misread an operator pre-set linger as off — the restore then wiped it
    // mid-run, fan-killing the cluster).
    assert!(
        remote_cmd.contains("/var/lib/systemd/linger/"),
        "probe must stat the persistent marker: {remote_cmd:?}"
    );
    assert!(
        !remote_cmd.contains("show-user"),
        "probe must not depend on logind: {remote_cmd:?}"
    );
    // No collateral: no extra-forward / QUIC ports in the linger command.
    assert!(!remote_cmd.contains("9090"));
    assert!(!remote_cmd.contains("2222"));
}

/// The RESTORE argv mirrors the enable shape (same jump, forced PTY, no
/// forward) but runs `disable-linger`.
#[test]
fn linger_disable_argv_runs_disable() {
    let o = PreparationOptions::new(
        "/logs".into(),
        "gw.example".into(),
        Some("alice".into()),
        22,
        vec![],
        vec![],
    );
    let argv = build_linger_argv("compute01", LingerVerb::Disable, &o);
    assert!(argv.iter().any(|s| s == "-tt"));
    assert!(!argv.iter().any(|s| s == "-R"));
    let remote_cmd = argv.last().expect("has a trailing remote command");
    assert!(
        remote_cmd.contains("loginctl disable-linger"),
        "must disable linger: {remote_cmd:?}"
    );
    assert!(remote_cmd.contains("DISABLE=ok"));
}

/// With auth_options set, the linger command MUST jump via ProxyCommand
/// (same OpenSSH-7.3+ workaround as the tunnel) — never `-J`.
#[test]
fn linger_argv_with_auth_uses_proxycommand() {
    let auth = vec![
        "-i".to_string(),
        "/tmp/key".into(),
        "-o".into(),
        "IdentitiesOnly=yes".into(),
    ];
    let o = PreparationOptions::new(
        "/logs".into(),
        "gw.example".into(),
        Some("alice".into()),
        2222,
        auth,
        vec![],
    );
    let argv = build_linger_argv("compute01", LingerVerb::Enable, &o);
    assert!(!argv.iter().any(|s| s == "-J"));
    let proxy_cmd = argv
        .iter()
        .find(|s| s.starts_with("ProxyCommand="))
        .expect("has ProxyCommand=");
    assert!(proxy_cmd.contains("'-i' '/tmp/key'"));
    assert!(proxy_cmd.contains("'-p' '2222'"));
    assert!(proxy_cmd.contains("'-W' '%h:%p'"));
}

/// `parse_was_linger` extracts the `WAS_LINGER=` value, tolerating the CRLF
/// line endings a forced PTY (`-tt`) produces and interleaved markers. An
/// absent marker (probe failed) is `None`, which the caller maps to the
/// safe "assume already on" default.
#[test]
fn parse_was_linger_reads_marker_crlf_tolerant() {
    assert_eq!(
        parse_was_linger("WAS_LINGER=yes\r\nENABLE=ok\r\n").as_deref(),
        Some("yes")
    );
    assert_eq!(
        parse_was_linger("WAS_LINGER=no\nENABLE=ok\n").as_deref(),
        Some("no")
    );
    // Empty value: no logind record yet.
    assert_eq!(
        parse_was_linger("WAS_LINGER=\r\nENABLE=fail\r\n").as_deref(),
        Some("")
    );
    // Absent marker (e.g. ssh failed before the printf ran).
    assert_eq!(parse_was_linger("Permission denied\r\n"), None);
}

/// THE restore-decision regression guard: ONLY an explicit `WAS_LINGER=no`
/// permits the run-end restore-disable. The EMPTY-value case is the
/// krater 2026-06-10 post-mortem: a failed bus-dependent probe yielded
/// `WAS_LINGER=` (empty), the old `== "yes"` mapping read it as "was off",
/// and the restore DISABLED an operator pre-set linger while the cluster
/// still ran — re-arming the fan-kill. Empty/absent/garbage must all read
/// as "already on" (restore skipped).
#[test]
fn only_explicit_no_permits_restore() {
    assert!(
        !was_linger_from_probe("WAS_LINGER=no\r\nENABLE=ok\r\n"),
        "explicit no → restore"
    );
    assert!(
        was_linger_from_probe("WAS_LINGER=yes\r\nENABLE=ok\r\n"),
        "yes → skip restore"
    );
    // THE regression: empty value must NOT be read as "was off".
    assert!(
        was_linger_from_probe("WAS_LINGER=\r\nENABLE=ok\r\n"),
        "empty → skip restore"
    );
    // Absent marker (ssh died before the printf) → skip restore.
    assert!(
        was_linger_from_probe("Permission denied\r\n"),
        "absent → skip restore"
    );
    // Garbage value → skip restore.
    assert!(
        was_linger_from_probe("WAS_LINGER=maybe\r\n"),
        "garbage → skip restore"
    );
}

/// `linger_succeeded` keys off the per-verb `=ok` marker, CR-tolerant, and
/// never confuses `ENABLE` with `DISABLE`. A `=fail <reason>` line (the
/// captured-loginctl-error shape) is failure too.
#[test]
fn linger_succeeded_keys_off_per_verb_marker() {
    assert!(linger_succeeded(
        "WAS_LINGER=no\r\nENABLE=ok\r\n",
        LingerVerb::Enable
    ));
    assert!(!linger_succeeded(
        "WAS_LINGER=no\r\nENABLE=fail\r\n",
        LingerVerb::Enable
    ));
    assert!(!linger_succeeded(
        "WAS_LINGER=no\r\nENABLE=fail Could not enable linger: Access denied\r\n",
        LingerVerb::Enable
    ));
    assert!(linger_succeeded(
        "WAS_LINGER=yes\r\nDISABLE=ok\r\n",
        LingerVerb::Disable
    ));
    // An ENABLE=ok line must NOT satisfy a DISABLE query.
    assert!(!linger_succeeded("ENABLE=ok\r\n", LingerVerb::Disable));
}

/// `linger_fail_reason` surfaces the remote loginctl error captured on the
/// fail marker line — the ONLY reliable failure detail, since the forced
/// PTY masks the remote exit status (observed: ssh reported rc=0 for a
/// failed enable). CR-tolerant; per-verb; `None` when the marker is absent
/// (ssh died first) or the reason is empty (legacy bare `=fail`).
#[test]
fn linger_fail_reason_surfaces_remote_error() {
    let out = "WAS_LINGER=no\r\nENABLE=fail Could not enable linger: Access denied\r\n";
    assert_eq!(
        linger_fail_reason(out, LingerVerb::Enable).as_deref(),
        Some("Could not enable linger: Access denied")
    );
    // Bare fail marker (no captured reason) → None.
    assert_eq!(
        linger_fail_reason("ENABLE=fail\r\n", LingerVerb::Enable),
        None
    );
    // Absent marker (ssh failed before loginctl ran) → None.
    assert_eq!(
        linger_fail_reason("Permission denied\r\n", LingerVerb::Enable),
        None
    );
    // Per-verb: an ENABLE fail must not satisfy a DISABLE query.
    assert_eq!(linger_fail_reason(out, LingerVerb::Disable), None);
    // Success output has no fail marker.
    assert_eq!(
        linger_fail_reason("WAS_LINGER=no\r\nENABLE=ok\r\n", LingerVerb::Enable),
        None
    );
}

/// Multi-watcher race regression: with ≥2 watchers calling
/// `verify_tunnel_alive` concurrently, each must observe the
/// outcome of *its own* spawned child — never a sibling's. The
/// pre-fix shape (`tunnels.lock().last_mut()`) was structurally
/// vulnerable to this: watcher A could verify watcher B's child
/// as soon as their `push` order interleaved.
///
/// We exercise the failure branch (each child exits immediately
/// with a unique stderr message) so the test is fast and the
/// stderr-attribution is directly observable in the assertion.
/// Pre-fix, the `last_mut()` lookup made misattribution possible
/// whenever a sibling's `push` interleaved between this
/// watcher's push and verify; the test asserts the post-fix
/// invariant that no such interleaving can ever occur because
/// each watcher operates on its own owned `Child`.
#[test]
fn verify_tunnel_alive_attributes_per_child_under_concurrency() {
    const N: usize = 4;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let outcomes: Vec<(String, PrepError)> = rt.block_on(local.run_until(async move {
        // Spawn N short-lived shells; each emits a marker
        // unique to its index on stderr and exits with rc=1.
        let mut children: Vec<(String, Child)> = Vec::with_capacity(N);
        for i in 0..N {
            let marker = format!("MARK-{i}");
            let mut cmd = Command::new("/bin/sh");
            cmd.arg("-c")
                .arg(format!("printf '%s' '{marker}' >&2; exit 1"));
            cmd.stdin(std::process::Stdio::null());
            cmd.stdout(std::process::Stdio::piped());
            cmd.stderr(std::process::Stdio::piped());
            cmd.kill_on_drop(true);
            let child = cmd.spawn().expect("spawn /bin/sh");
            children.push((format!("secondary-{i}"), child));
        }

        // Verify all in parallel from a JoinSet.
        let mut set: JoinSet<(String, PrepError)> = JoinSet::new();
        for (id, mut child) in children.into_iter() {
            set.spawn_local(async move {
                let err = verify_tunnel_alive(&id, &mut child)
                    .await
                    .expect_err("dying child must surface TunnelFailed");
                (id, err)
            });
        }

        let mut out = Vec::with_capacity(N);
        while let Some(joined) = set.join_next().await {
            out.push(joined.expect("watcher panicked"));
        }
        out
    }));

    assert_eq!(outcomes.len(), N);
    for (id, err) in outcomes {
        match err {
            PrepError::TunnelFailed {
                secondary_id,
                stderr,
                ..
            } => {
                assert_eq!(secondary_id, id);
                // Each child's stderr MUST contain its own
                // marker — pre-fix `last_mut()` could pull
                // sibling stderr instead.
                let idx: usize = id
                    .strip_prefix("secondary-")
                    .and_then(|s| s.parse().ok())
                    .expect("id parses");
                let expected = format!("MARK-{idx}");
                assert_eq!(
                    stderr, expected,
                    "watcher {id} got cross-attributed stderr {stderr:?}"
                );
            }
            other => panic!("expected TunnelFailed, got {other}"),
        }
    }
}

/// Defect (c): a child that writes its decisive line to stderr
/// IMMEDIATELY before exiting must have those bytes captured. The
/// pre-fix shape read stderr only AFTER `child.wait()` returned — a
/// post-reap re-read that races ssh's final
/// "remote port forwarding failed for listen port NNN" flush and can
/// drop it (the OS pipe buffer may already be drained-and-closed by the
/// time the reader looks). The concurrent drain runs the `read_to_end`
/// ALONGSIDE the wait, so every pre-exit byte lands in `TunnelFailed.stderr`.
///
/// We model the worst case directly: the child writes the exact ssh
/// failure line to stderr and exits rc=255 in the SAME shell statement,
/// with no delay — the tightest write-then-exit window.
#[test]
fn verify_tunnel_alive_captures_stderr_written_just_before_exit() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let err: PrepError = rt.block_on(local.run_until(async {
        // Real ssh-shaped failure line; written to stderr then an
        // immediate exit 255 — the decisive line the operator needs.
        let line = "Warning: remote port forwarding failed for listen port 40000";
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c")
            .arg(format!("printf '%s\\n' '{line}' >&2; exit 255"));
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        cmd.kill_on_drop(true);
        let mut child = cmd.spawn().expect("spawn /bin/sh");
        verify_tunnel_alive("secondary-0", &mut child)
            .await
            .expect_err("a child exiting 255 must surface TunnelFailed")
    }));
    match err {
        PrepError::TunnelFailed { rc, stderr, .. } => {
            assert_eq!(rc, Some(255), "exit code must be captured");
            assert!(
                stderr.contains("remote port forwarding failed for listen port 40000"),
                "the decisive pre-exit stderr line must be captured, got {stderr:?}"
            );
        }
        other => panic!("expected TunnelFailed, got {other}"),
    }
}

/// [`LingerLedger`] original-state semantics: FIRST-writer-wins per host.
/// A multi-secondary node's first probe captures the genuine pre-run
/// state; a later (post-our-enable) `yes` must not overwrite it, so the
/// run-end restore still disables what the run enabled.
#[test]
fn ledger_original_state_is_first_writer_wins() {
    let ledger = LingerLedger::default();
    // First probe on nodeA: was OFF (this run enables it).
    ledger.record_enable("nodeA", false, true);
    // Second secondary on the same node reads the now-enabled state.
    ledger.record_enable("nodeA", true, true);
    // nodeB was already lingering before the run.
    ledger.record_enable("nodeB", true, true);

    let restore = ledger.drain_restore_hosts();
    assert_eq!(
        restore,
        vec!["nodeA".to_string()],
        "only the node whose linger this run enabled is restored"
    );
    // Drained: a second call is a no-op (idempotent teardown).
    assert!(ledger.drain_restore_hosts().is_empty());
}

/// [`LingerLedger`] enable-verdict semantics: ANY-success-wins per host
/// (a retried attempt that eventually lands clears an earlier failure),
/// failures are reported sorted, and the drain is consuming.
#[test]
fn ledger_enable_verdict_is_any_success_wins_and_drains() {
    let ledger = LingerLedger::default();
    // nodeC: first attempt failed, retry succeeded → ok.
    ledger.record_enable("nodeC", false, false);
    ledger.record_enable("nodeC", false, true);
    // nodeA, nodeB: never succeeded → failed.
    ledger.record_enable("nodeB", true, false);
    ledger.record_enable("nodeA", true, false);

    let (ok, failed) = ledger.drain_enable_verdicts();
    assert_eq!(ok, 1, "any-success-wins: nodeC counts as ok");
    assert_eq!(
        failed,
        vec!["nodeA".to_string(), "nodeB".to_string()],
        "failed hosts are reported sorted"
    );
    // Drained: a later cohort summarises only its own attempts.
    assert_eq!(ledger.drain_enable_verdicts(), (0, Vec::new()));
}

/// The worker-side BIND-PROBE argv must mirror the one-shot remote-cmd
/// shape (same gateway jump + compute target as the tunnel, bounded
/// handshake, NOT a forward) and carry the structured-marker probe
/// scoped to exactly the tunnel port.
#[test]
fn bind_probe_argv_reuses_jump_and_probes_exact_port() {
    let o = PreparationOptions::new(
        "/logs".into(),
        "gw.example".into(),
        Some("alice".into()),
        22,
        vec![],
        // extra_port_forwards must NOT leak into the probe.
        vec![(2222, 9090)],
    );
    let argv = build_bind_probe_argv("compute01", 40000, &o);
    // Same gateway jump + compute-node target as the tunnel.
    let j_idx = argv.iter().position(|s| s == "-J").expect("has -J");
    assert_eq!(argv[j_idx + 1], "alice@gw.example");
    assert!(argv.iter().any(|s| s == "alice@compute01"));
    // Not a tunnel: no -R / -N / ExitOnForwardFailure; bounded handshake.
    assert!(!argv.iter().any(|s| s == "-R"), "probe must not forward");
    assert!(!argv.iter().any(|s| s == "-N"), "probe must run a command");
    assert!(!argv.iter().any(|s| s == "ExitOnForwardFailure=yes"));
    assert!(argv.iter().any(|s| s == "ConnectTimeout=10"));
    // The remote command reports listeners on EXACTLY :40000 via the
    // structured markers (the ssh exit code is not the channel).
    let remote_cmd = argv.last().expect("has a trailing remote command");
    assert!(remote_cmd.contains("ss -tln"), "probe uses ss: {remote_cmd:?}");
    assert!(remote_cmd.contains(":40000"), "probe scopes the port: {remote_cmd:?}");
    assert!(remote_cmd.contains("TUNNEL_LISTEN="), "structured listener marker");
    assert!(remote_cmd.contains("TUNNEL_PROBE=done"), "probe-ran marker");
    assert!(remote_cmd.contains("TUNNEL_PROBE=no-ss"), "tool-missing marker");
    // No collateral ports.
    assert!(!remote_cmd.contains("9090"));
    assert!(!remote_cmd.contains("2222"));
}

/// With auth_options set, the probe MUST jump via ProxyCommand (same
/// OpenSSH-7.3+ workaround as every other per-node ssh) — never `-J`.
#[test]
fn bind_probe_argv_with_auth_uses_proxycommand() {
    let auth = vec!["-i".to_string(), "/tmp/key".into()];
    let o = PreparationOptions::new(
        "/logs".into(),
        "gw.example".into(),
        Some("alice".into()),
        2222,
        auth,
        vec![],
    );
    let argv = build_bind_probe_argv("compute01", 40000, &o);
    assert!(!argv.iter().any(|s| s == "-J"));
    let proxy_cmd = argv
        .iter()
        .find(|s| s.starts_with("ProxyCommand="))
        .expect("has ProxyCommand=");
    assert!(proxy_cmd.contains("'-i' '/tmp/key'"));
    assert!(proxy_cmd.contains("'-p' '2222'"));
}

/// `parse_bind_probe` verdicts on the healthy shapes: both loopback
/// families, a single family, and wildcard binds all verify. CR-trims
/// for transport safety.
#[test]
fn parse_bind_probe_accepts_loopback_and_wildcard_listeners() {
    // Healthy: both families (the healthy-run production anatomy).
    assert_eq!(
        parse_bind_probe(
            "TUNNEL_LISTEN=127.0.0.1:42655\r\nTUNNEL_LISTEN=[::1]:42655\r\nTUNNEL_PROBE=done\r\n",
            42655
        ),
        BindProbe::Listening {
            listeners: vec!["127.0.0.1:42655".into(), "[::1]:42655".into()],
        }
    );
    // v6-only partial bind STILL verifies: the dual-family dial
    // (transport_factory) carries the run over [::1].
    assert_eq!(
        parse_bind_probe("TUNNEL_LISTEN=[::1]:42655\nTUNNEL_PROBE=done\n", 42655),
        BindProbe::Listening {
            listeners: vec!["[::1]:42655".into()],
        }
    );
    // Wildcard binds (GatewayPorts-style) are loopback-dialable too.
    for addr in ["0.0.0.0:42655", "[::]:42655", "*:42655"] {
        assert!(
            matches!(
                parse_bind_probe(
                    &format!("TUNNEL_LISTEN={addr}\nTUNNEL_PROBE=done\n"),
                    42655
                ),
                BindProbe::Listening { .. }
            ),
            "wildcard listener {addr} must verify"
        );
    }
    // v4-mapped loopback.
    assert!(matches!(
        parse_bind_probe(
            "TUNNEL_LISTEN=[::ffff:127.0.0.1]:42655\nTUNNEL_PROBE=done\n",
            42655
        ),
        BindProbe::Listening { .. }
    ));
}

/// `parse_bind_probe` definite-miss + filtering: probe ran with no
/// listener ⇒ `NotListening`; a NON-loopback listener (the colliding
/// squatter itself on a LAN address) and wrong-port lines must NOT be
/// mistaken for the tunnel.
#[test]
fn parse_bind_probe_definite_miss_and_squatter_filtering() {
    // Probe ran, nothing bound — the production flake's worker state.
    assert_eq!(
        parse_bind_probe("TUNNEL_PROBE=done\r\n", 42655),
        BindProbe::NotListening
    );
    // A LAN-address squatter on the tunnel port is NOT dialable over
    // loopback — must stay a definite miss.
    assert_eq!(
        parse_bind_probe(
            "TUNNEL_LISTEN=10.0.0.5:42655\nTUNNEL_PROBE=done\n",
            42655
        ),
        BindProbe::NotListening
    );
    // Defensive: a listener on a DIFFERENT port never verifies this one.
    assert_eq!(
        parse_bind_probe(
            "TUNNEL_LISTEN=127.0.0.1:42656\nTUNNEL_PROBE=done\n",
            42655
        ),
        BindProbe::NotListening
    );
}

/// `parse_bind_probe` inconclusive shapes: `ss` missing and
/// marker-less output (probe ssh died first) both refuse a verdict —
/// the caller keeps the gate-verified tunnel instead of killing it on
/// probe-infrastructure failure.
#[test]
fn parse_bind_probe_inconclusive_on_missing_tool_or_marker() {
    match parse_bind_probe("TUNNEL_PROBE=no-ss\r\n", 42655) {
        BindProbe::Inconclusive { reason } => {
            assert!(reason.contains("ss"), "reason names the missing tool: {reason}")
        }
        other => panic!("expected Inconclusive, got {other:?}"),
    }
    match parse_bind_probe("Permission denied\r\n", 42655) {
        BindProbe::Inconclusive { reason } => assert!(
            reason.contains("no probe marker"),
            "reason names the absent marker: {reason}"
        ),
        other => panic!("expected Inconclusive, got {other:?}"),
    }
}

// The worker-side port release now runs before EVERY bind (incl. the
// first), with no per-attempt mode to branch on — one `tunnel_spawner`
// serves the cohort, respawn, and reconnect paths identically. The
// release-before-bind behavior that closes the phantom (#408) is pinned
// end-to-end against the stale-holder fixture in
// `tests/respawn.rs::cohort_release_before_first_bind_clears_phantom`.

/// THE failure classifier (one function, both classes, real stderr
/// shapes): pre-banner connection loss — the probabilistic sshd
/// `MaxStartups` random-drop anatomy — is TRANSIENT (worth the retry
/// budget).
#[test]
fn classifier_pre_banner_drop_is_transient() {
    // The canonical MaxStartups random-drop capture: identification
    // exchange died, peer never learned.
    assert_eq!(
        classify_tunnel_failure(
            "kex_exchange_identification: Connection closed by remote host\r\n\
             Connection closed by UNKNOWN port 65535"
        ),
        TunnelFailureClass::Transient
    );
    // Same drop where the resolved peer address IS known: the kex
    // marker must keep it transient even though a bare
    // "Connection closed by <addr> port 22" line is present.
    assert_eq!(
        classify_tunnel_failure(
            "kex_exchange_identification: Connection closed by remote host\r\n\
             Connection closed by 10.153.52.8 port 22"
        ),
        TunnelFailureClass::Transient
    );
    // TCP-level reset before the banner.
    assert_eq!(
        classify_tunnel_failure(
            "kex_exchange_identification: read: Connection reset by peer"
        ),
        TunnelFailureClass::Transient
    );
    assert_eq!(
        classify_tunnel_failure("Connection closed by UNKNOWN port 65535"),
        TunnelFailureClass::Transient
    );
}

/// Auth-class refusals — wrong/missing key, unknown user (the
/// asm-dataset provisioning gap), host-key rejection, too-many-auth —
/// are DETERMINISTIC: every retry refuses identically (ssh emits an
/// explicit auth marker), so the classifier must steer the policy to
/// fail fast. These markers are the ONLY positive proof of an
/// auth-class refusal.
#[test]
fn classifier_auth_class_is_deterministic() {
    assert_eq!(
        classify_tunnel_failure(
            "runuser@gateway.example: Permission denied (publickey,password)."
        ),
        TunnelFailureClass::Deterministic
    );
    assert_eq!(
        classify_tunnel_failure("Host key verification failed."),
        TunnelFailureClass::Deterministic
    );
    assert_eq!(
        classify_tunnel_failure(
            "Received disconnect from 10.153.52.8 port 22:2: Too many authentication failures\r\n\
             Disconnected from 10.153.52.8 port 22"
        ),
        TunnelFailureClass::Deterministic
    );
}

/// REGRESSION (#408): a bare post-banner "Connection closed by <addr>
/// port <p>" — no pre-banner `kex_`/`UNKNOWN` marker, no explicit auth
/// marker — is TRANSIENT, not deterministic. On the establish burst a
/// busy worker sshd load-sheds an already-authenticated session after
/// the banner, emitting exactly this line; a retry (with its same-port
/// release+rebind) virtually always lands. The 31e689bc classifier
/// wrongly fast-failed it, removing the retry that load-shed closes
/// depend on. A genuine auth refusal still carries a step-1 marker, so
/// only the ambiguous bare close changes class here.
#[test]
fn classifier_bare_post_banner_close_is_transient() {
    assert_eq!(
        classify_tunnel_failure("Connection closed by 10.153.52.8 port 22"),
        TunnelFailureClass::Transient
    );
    // Same shape with the trailing newline ssh appends.
    assert_eq!(
        classify_tunnel_failure("Connection closed by 10.153.52.8 port 22\r\n"),
        TunnelFailureClass::Transient
    );
}

/// Under the ProxyCommand jump the proxy ssh's stderr lands on the
/// outer ssh's stderr: a gateway-auth refusal shows BOTH the proxy's
/// "Permission denied" AND the outer ssh's pre-banner
/// "Connection closed by UNKNOWN port 65535". The auth evidence must
/// win — this mixed capture is deterministic.
#[test]
fn classifier_proxyjump_mixed_capture_auth_wins() {
    assert_eq!(
        classify_tunnel_failure(
            "runuser@gateway.example: Permission denied (publickey).\r\n\
             kex_exchange_identification: Connection closed by remote host\r\n\
             Connection closed by UNKNOWN port 65535"
        ),
        TunnelFailureClass::Deterministic
    );
}

/// Unrecognised stderr defaults to TRANSIENT — retrying an unknown
/// failure is the pre-classification behaviour and the safe default
/// (a fail-fast on an unknown shape would turn recoverable blips
/// into dispatch aborts).
#[test]
fn classifier_unknown_defaults_to_transient() {
    assert_eq!(
        classify_tunnel_failure("ssh: connect to host gw port 22: Connection timed out"),
        TunnelFailureClass::Transient
    );
    assert_eq!(classify_tunnel_failure(""), TunnelFailureClass::Transient);
    // Mid-protocol fatal with a "Connection closed by" SUFFIX on the
    // dispatch line (not a bare post-banner close line) stays
    // transient: it is not the auth-rejection shape.
    assert_eq!(
        classify_tunnel_failure(
            "ssh_dispatch_run_fatal: Connection closed by 10.153.52.8 port 22"
        ),
        TunnelFailureClass::Transient
    );
}
