//! Tests for `generate_test_wrapper_script` — the image-validation
//! wrapper variant. Cover the cleanup trap, the rootless-podman
//! teardown, and a bash-syntax smoke check on the rendered output.

use crate::config::SlurmConfig;
use crate::wrapper_script::{
    TestWrapperScriptConfig, generate_test_wrapper_script, generate_wrapper_script,
};

use super::standard_cfg;

#[test]
fn test_wrapper_traps_termination_signals() {
    let script = generate_test_wrapper_script(&test_wrapper_cfg());
    assert!(script.contains("trap cleanup EXIT TERM HUP INT"));
    assert!(script.contains("/tmp/asm-test-"));
    assert!(script.contains("test-app.tar"));
    assert!(script.contains("my_runner --help"));
}

fn test_wrapper_cfg() -> TestWrapperScriptConfig<'static> {
    TestWrapperScriptConfig {
        image_path: "/images/test.tar",
        image_name: "test-app",
        image_tag: "latest",
        image_tar_basename: "test-app.tar",
        load_command: "podman --root \"$PODMAN_STORAGE\" --runroot \"$PODMAN_RUN\" --cgroup-manager=cgroupfs load < \"$LOCAL_IMAGE\"",
        container_command: "my_runner",
    }
}

/// Same fix applies to the image-validation wrapper: it
/// also runs `podman load` into $RNDTMP/storage and so
/// produces the same subuid-mapped tree.
#[test]
fn test_wrapper_cleanup_uses_podman_unshare() {
    let script = generate_test_wrapper_script(&test_wrapper_cfg());
    assert!(script.contains("podman unshare rm -rf -- \"$RNDTMP\""));
    assert!(script.contains("rm -f -- \"$LOCAL_IMAGE\""));
    assert!(!script.contains("sudo rm -rf"));
}

/// Render both wrappers and run `bash -n` on each to catch
/// quoting / brace / heredoc regressions at unit-test time.
/// Without this guard a misplaced `{` or unbalanced quote
/// only surfaces on the compute node, where diagnosis costs
/// a SLURM round-trip.
#[test]
fn rendered_wrapper_passes_bash_syntax_check() {
    use std::io::Write;
    use std::process::Command;

    // Skip cleanly if `bash` isn't on PATH (e.g. a
    // stripped-down CI image). Letting the test fail there
    // would force every consumer of the crate to install
    // bash before they can run `cargo test`, which isn't
    // what this guard is meant to enforce.
    if Command::new("bash").arg("--version").output().is_err() {
        eprintln!("skipping: bash not available on PATH");
        return;
    }

    let config = SlurmConfig::default();
    let secondary = generate_wrapper_script(&standard_cfg(&config, &[]));
    let test_wrapper = generate_test_wrapper_script(&test_wrapper_cfg());

    for (label, script) in [("secondary", secondary), ("test", test_wrapper)] {
        let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
        tmp.write_all(script.as_bytes()).expect("write script");
        let path = tmp.into_temp_path();
        let out = Command::new("bash")
            .arg("-n")
            .arg(&path)
            .output()
            .expect("spawn bash -n");
        assert!(
            out.status.success(),
            "{label} wrapper failed `bash -n`:\nstdout={}\nstderr={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
    }
}
