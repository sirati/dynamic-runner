//! Bash-syntax smoke checks on the rendered secondary wrapper (with
//! and without `forwarded_argv`) and the unit test for the inline
//! `bash_quote` helper. Both checks shell out to `/bin/bash -n` so
//! they no-op on stripped CI sandboxes without `bash` on PATH.

use crate::config::SlurmConfig;
use crate::wrapper_script::quote::bash_quote;
use crate::wrapper_script::{
    generate_test_wrapper_script, generate_wrapper_script, ConnectionMode,
    TestWrapperScriptConfig, WrapperScriptConfig,
};

use super::standard_cfg;

#[test]
fn rendered_script_with_forwarded_argv_passes_bash_syntax_check() {
    use std::io::Write;
    let bash_available = std::process::Command::new("bash")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !bash_available {
        return;
    }
    let config = SlurmConfig::default();
    // Mix of safe tokens, single-quoted globs, embedded apostrophes,
    // and spaces. Spans every branch of bash_quote that the field
    // payload might exercise.
    let forwarded = vec![
        "--platform".to_string(),
        "x64".to_string(),
        "--name-regex".to_string(),
        "x64-gcc-*-*_minigzipsh".to_string(),
        "--label=it's".to_string(),
        "--annotation=hello world".to_string(),
    ];
    let cfg = WrapperScriptConfig {
        forwarded_argv: &forwarded,
        ..standard_cfg(&config, &[])
    };
    let script = generate_wrapper_script(&cfg);
    let mut child = std::process::Command::new("bash")
        .args(["-n", "/dev/stdin"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
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
        "bash -n rejected the wrapper with non-empty forwarded_argv:\n\
         STDERR:\n{}\n--- script ---\n{}",
        String::from_utf8_lossy(&out.stderr),
        script,
    );
}

/// Consumer-supplied `--ulimit nproc=N:N` via `extra_run_args`
/// must land AFTER the framework default in the rendered
/// invocation so podman's last-wins flag parsing applies the
/// consumer's value. Mirrors the pids-limit override semantic
/// (commit 9b3dce0) — same rule, sibling concern.
#[test]
fn rendered_scripts_pass_bash_syntax_check() {
    let bash = match std::process::Command::new("bash")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
    {
        Ok(s) if s.success() => "bash",
        _ => return,
    };

    let config = SlurmConfig::default();
    let standard = generate_wrapper_script(&standard_cfg(&config, &[]));
    let reverse = generate_wrapper_script(&WrapperScriptConfig {
        connection: ConnectionMode::Reverse {
            connection_info_dir: "/logs/connection_info",
        },
        ..standard_cfg(&config, &[])
    });
    let test_wrapper = generate_test_wrapper_script(&TestWrapperScriptConfig {
        image_path: "/images/test.tar",
        image_name: "test-app",
        image_tag: "latest",
        image_tar_basename: "test-app.tar",
        load_command: "podman --root \"$PODMAN_STORAGE\" --runroot \"$PODMAN_RUN\" --cgroup-manager=cgroupfs load < \"$LOCAL_IMAGE\"",
        container_command: "my_runner",
    });

    for (label, script) in [
        ("standard", standard.as_str()),
        ("reverse", reverse.as_str()),
        ("test-wrapper", test_wrapper.as_str()),
    ] {
        use std::io::Write;
        let mut child = std::process::Command::new(bash)
            .args(["-n", "/dev/stdin"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
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
            "bash -n rejected the {label}-mode wrapper:\nSTDERR:\n{}\n--- script ---\n{}",
            String::from_utf8_lossy(&out.stderr),
            script,
        );
    }
}

#[test]
fn bash_quote_examples() {
    assert_eq!(bash_quote("hello"), "hello");
    assert_eq!(bash_quote(""), "''");
    assert_eq!(bash_quote("a b"), "'a b'");
    assert_eq!(bash_quote("it's"), "'it'\\''s'");
    assert_eq!(bash_quote("--pids-limit=16384"), "--pids-limit=16384");
}

