//! Unit tests for the ssh module tree. Auth options, master argv
//! pins, and control-path invariants.

use crate::config::SshConfig;

use super::SshGateway;
use super::argv::{build_master_argv, generate_master_control_path, parse_master_pid};

fn ssh_config_with(identity_file: Option<&str>, config_file: Option<&str>) -> SshConfig {
    SshConfig {
        host: "h".into(),
        port: 22,
        user: None,
        identity_file: identity_file.map(str::to_string),
        config_file: config_file.map(str::to_string),
    }
}

#[test]
fn auth_options_empty_when_unset() {
    let gw = SshGateway::new(ssh_config_with(None, None));
    assert!(gw.auth_options().is_empty());
}

#[test]
fn auth_options_identity_file_emits_full_chain() {
    let gw = SshGateway::new(ssh_config_with(Some("/k/id_ed25519"), None));
    assert_eq!(
        gw.auth_options(),
        vec![
            "-i".to_string(),
            "/k/id_ed25519".to_string(),
            "-o".to_string(),
            "IdentitiesOnly=yes".to_string(),
            "-o".to_string(),
            "IdentityAgent=none".to_string(),
        ]
    );
}

#[test]
fn auth_options_config_file_emits_dash_capital_f() {
    let gw = SshGateway::new(ssh_config_with(None, Some("/etc/ssh/cfg")));
    assert_eq!(
        gw.auth_options(),
        vec!["-F".to_string(), "/etc/ssh/cfg".to_string()],
    );
}

#[test]
fn auth_options_identity_then_config_file_order() {
    // Order matches Python: -i/IdentitiesOnly/IdentityAgent first,
    // then -F. Some downstream consumers (e.g. preparation.py's
    // reverse tunnel) splat this list, so ordering is part of the
    // contract.
    let gw = SshGateway::new(ssh_config_with(Some("/k/id"), Some("/c/f")));
    assert_eq!(
        gw.auth_options(),
        vec![
            "-i".to_string(),
            "/k/id".to_string(),
            "-o".to_string(),
            "IdentitiesOnly=yes".to_string(),
            "-o".to_string(),
            "IdentityAgent=none".to_string(),
            "-F".to_string(),
            "/c/f".to_string(),
        ]
    );
}

/// T1: pin the 18h ServerAlive floor in the master spawn argv.
/// Regression-pin for the anti-leak floor: any change to this
/// must surface in code review and not be a silent override.
#[test]
fn master_argv_pins_18h_serveralive_floor() {
    let argv = build_master_argv(&Vec::new(), "/tmp/dynrunner-m-test.sock", &[], "user@host");
    // We assert the *adjacent pair* form: each `-o` must precede
    // its value. `windows(2)` gives us each consecutive pair so we
    // can match `-o ServerAliveInterval=60`.
    let has_pair = |a: &str, b: &str| argv.windows(2).any(|w| w[0] == a && w[1] == b);
    assert!(
        has_pair("-o", "ServerAliveInterval=60"),
        "missing `-o ServerAliveInterval=60`; argv={argv:?}"
    );
    assert!(
        has_pair("-o", "ServerAliveCountMax=1080"),
        "missing `-o ServerAliveCountMax=1080`; argv={argv:?}"
    );
    // 60s × 1080 = 64800s = 18h. Spell the math out so any future
    // edit that changes one without the other fails this test.
    assert_eq!(60u64 * 1080, 64_800);
}

#[test]
fn master_argv_includes_master_mode_flags() {
    let argv = build_master_argv(&Vec::new(), "/tmp/dynrunner-m-test.sock", &[], "user@host");
    assert!(argv.contains(&"-M".to_string()));
    assert!(argv.contains(&"-N".to_string()));
    // No `-f`: even though `ControlPersist=yes` causes OpenSSH to
    // fork-and-detach a daemon at handshake-end *anyway* (we
    // track that daemon's PID via `ssh -O check`), `-f` adds an
    // unrelated effect — fork *before* full handshake — that has
    // historically masked auth failures by exiting 0 from the
    // foreground process before the failure surfaced. Pin its
    // absence so a regression doesn't silently re-introduce that
    // failure-masking behaviour.
    assert!(
        !argv.contains(&"-f".to_string()),
        "master argv must not contain `-f` (auth-failure masking); argv={argv:?}"
    );
}

#[test]
fn master_argv_threads_control_path_and_target() {
    let argv = build_master_argv(
        &["-p".to_string(), "2222".to_string()],
        "/tmp/dynrunner-m-42-7.sock",
        &[(1234, 5678)],
        "alice@host",
    );
    assert_eq!(argv.first().map(String::as_str), Some("-p"));
    assert_eq!(argv.get(1).map(String::as_str), Some("2222"));
    assert!(argv.contains(&"ControlPath=/tmp/dynrunner-m-42-7.sock".to_string()));
    assert!(argv.contains(&"-R".to_string()));
    assert!(argv.contains(&"0.0.0.0:5678:localhost:1234".to_string()));
    assert_eq!(argv.last().map(String::as_str), Some("alice@host"));
}

/// T2: pin sun_path < 108. The Linux kernel caps Unix socket paths
/// at `sizeof(sockaddr_un.sun_path) = 108` bytes including the NUL
/// terminator; ssh silently fails to bind if the control path
/// exceeds. The path generator must stay well below this bound for
/// any reasonable PID/sequence combination.
#[test]
fn control_path_fits_sockaddr_un() {
    let cp = generate_master_control_path();
    assert!(
        cp.len() < 108,
        "control path is {} bytes ({:?}) — must stay under 108 (sockaddr_un.sun_path cap)",
        cp.len(),
        cp,
    );
    assert!(
        cp.starts_with("/tmp/dynrunner-m-"),
        "control path must live under /tmp with the framework prefix: {cp:?}"
    );
    assert!(
        cp.ends_with(".sock"),
        "control path must end with .sock for grep-ability in operational tooling: {cp:?}"
    );
}

#[test]
fn control_path_unique_across_calls() {
    // Within a single process, the AtomicU64 sequence guarantees
    // distinct paths — pin this to surface any accidental
    // simplification (e.g. fixed name) that re-introduces collision
    // races between concurrent SshGateways.
    let a = generate_master_control_path();
    let b = generate_master_control_path();
    assert_ne!(a, b);
}

#[test]
fn control_path_pessimistic_pid_sequence_still_fits() {
    // Synthesise a worst-case-ish path: 7-digit PID + 19-digit
    // sequence (u64 max ≈ 1.8e19, 19 digits). Even there the
    // total is comfortably below 108 bytes.
    let synthetic = format!("/tmp/dynrunner-m-{}-{}.sock", 9_999_999u32, u64::MAX);
    assert!(
        synthetic.len() < 108,
        "even worst-case PID/seq path is {} bytes ({:?})",
        synthetic.len(),
        synthetic,
    );
}

/// `ssh -O check` produces `Master running (pid=<N>)` followed by
/// a newline. Any digits after `pid=` until the first non-digit
/// is the PID. The parser must extract it cleanly even when the
/// line is embedded in surrounding output.
#[test]
fn parse_master_pid_extracts_from_canonical_output() {
    assert_eq!(
        parse_master_pid("Master running (pid=12345)\n"),
        Some(12345)
    );
}

#[test]
fn parse_master_pid_handles_leading_whitespace_and_extra_lines() {
    let out = "  Master running (pid=42)\nsomething else\n";
    assert_eq!(parse_master_pid(out), Some(42));
}

#[test]
fn parse_master_pid_returns_none_when_marker_absent() {
    // Negative path: the `Stop listening request sent.` reply
    // (which ssh -O stop emits) must NOT be parsed as a PID.
    assert_eq!(parse_master_pid("Stop listening request sent.\n"), None);
    // And: missing `pid=` entirely.
    assert_eq!(parse_master_pid("Master running\n"), None);
    assert_eq!(parse_master_pid(""), None);
}

#[test]
fn parse_master_pid_returns_none_on_non_numeric_pid() {
    // Defence against an OpenSSH version that prints something
    // unexpected after `pid=` — return None, surface the issue
    // up the stack as `CommandFailed`, don't fabricate a PID.
    assert_eq!(parse_master_pid("Master running (pid=abc)"), None);
}

#[test]
fn parse_master_pid_rejects_overflow() {
    // u32 max = 4_294_967_295. Anything wider must yield None
    // rather than silently truncating.
    assert_eq!(
        parse_master_pid("Master running (pid=99999999999999)"),
        None
    );
}

/// The mux-liveness gate: an absent control socket is `false` — the
/// canonical "master never spawned / already dead" shape a fallback
/// caller must see. (`ssh -O check` talks only to the local socket,
/// so this probes offline and fast.)
#[tokio::test]
async fn control_socket_alive_false_when_socket_missing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cp = dir.path().join("no-such-master.sock");
    let cfg = ssh_config_with(None, None);
    assert!(!super::control_socket_alive(&cp.to_string_lossy(), &cfg).await);
}
