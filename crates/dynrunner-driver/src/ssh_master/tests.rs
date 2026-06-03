//! Unit tests for the ssh_master module tree. Loaded by `mod.rs`
//! only under `#[cfg(test)]`. Covers argv pins, control-path
//! length/uniqueness invariants, and the `parse_master_pid` parser.

use super::argv::{build_master_argv, generate_master_control_path};
use super::probe::parse_master_pid;

/// T1 (carried over from the original gateway tests): pin the
/// 18h ServerAlive floor in the master spawn argv.
/// Regression-pin for the anti-leak floor: any change to this
/// must surface in code review and not be a silent override.
#[test]
fn master_argv_pins_18h_serveralive_floor() {
    let argv = build_master_argv(&Vec::new(), "/tmp/dynrunner-m-test.sock", &[], "user@host");
    let has_pair = |a: &str, b: &str| argv.windows(2).any(|w| w[0] == a && w[1] == b);
    assert!(
        has_pair("-o", "ServerAliveInterval=60"),
        "missing `-o ServerAliveInterval=60`; argv={argv:?}"
    );
    assert!(
        has_pair("-o", "ServerAliveCountMax=1080"),
        "missing `-o ServerAliveCountMax=1080`; argv={argv:?}"
    );
    assert_eq!(60u64 * 1080, 64_800);
}

#[test]
fn master_argv_includes_master_mode_flags() {
    let argv = build_master_argv(&Vec::new(), "/tmp/dynrunner-m-test.sock", &[], "user@host");
    assert!(argv.contains(&"-M".to_string()));
    assert!(argv.contains(&"-N".to_string()));
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

#[test]
fn control_path_fits_sockaddr_un() {
    let cp = generate_master_control_path();
    assert!(
        cp.len() < 108,
        "control path is {} bytes ({:?}) — must stay under 108",
        cp.len(),
        cp,
    );
    assert!(cp.starts_with("/tmp/dynrunner-m-"));
    assert!(cp.ends_with(".sock"));
}

#[test]
fn control_path_unique_across_calls() {
    let a = generate_master_control_path();
    let b = generate_master_control_path();
    assert_ne!(a, b);
}

#[test]
fn control_path_pessimistic_pid_sequence_still_fits() {
    let synthetic = format!("/tmp/dynrunner-m-{}-{}.sock", 9_999_999u32, u64::MAX);
    assert!(
        synthetic.len() < 108,
        "even worst-case PID/seq path is {} bytes ({:?})",
        synthetic.len(),
        synthetic,
    );
}

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
    assert_eq!(parse_master_pid("Stop listening request sent.\n"), None);
    assert_eq!(parse_master_pid("Master running\n"), None);
    assert_eq!(parse_master_pid(""), None);
}

#[test]
fn parse_master_pid_returns_none_on_non_numeric_pid() {
    assert_eq!(parse_master_pid("Master running (pid=abc)"), None);
}

#[test]
fn parse_master_pid_rejects_overflow() {
    assert_eq!(
        parse_master_pid("Master running (pid=99999999999999)"),
        None
    );
}
