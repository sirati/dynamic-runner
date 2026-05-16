//! Tests for the wrapper-script's pre-flight podman cleanup. Single
//! concern: pin the load-bearing pattern that closes the
//! orphan-accumulation leak (asm-tokenizer 2026-05-16, 16 stale
//! containers across a 40-node cluster, one alive 7+ hours).

use crate::config::SlurmConfig;
use crate::wrapper_script::generate_wrapper_script;

use super::standard_cfg;

/// The pre-flight block must walk every `/tmp/*/storage` candidate so
/// orphan per-job storage roots (the wrapper's own `$RNDTMP/storage`
/// shape) are reached. Default-storage `podman ps` alone misses them.
#[test]
fn preflight_walks_orphan_per_job_storage_roots() {
    let config = SlurmConfig::default();
    let script = generate_wrapper_script(&standard_cfg(&config, &[]));
    assert!(
        script.contains("for orphan_storage in /tmp/*/storage"),
        "pre-flight must enumerate /tmp/*/storage candidates to reach orphan \
         per-job storage roots"
    );
    // The per-orphan operation must pair `--root` + `--runroot` so
    // podman addresses the orphan's metadata, not the current job's.
    assert!(
        script.contains("podman --root \"$orphan_storage\" --runroot \"$orphan_runroot\"")
    );
}

/// Graceful stop with the 10s grace window — user spec ("podman stop"
/// graceful, not `podman kill`). The grace lets the orphan's process
/// tree flush bind-mount writes before SIGKILL.
#[test]
fn preflight_uses_graceful_stop_with_ten_second_grace() {
    let config = SlurmConfig::default();
    let script = generate_wrapper_script(&standard_cfg(&config, &[]));
    assert!(
        script.contains("stop -t 10 $orphan_running"),
        "orphan stop must be graceful with -t 10 (user-spec'd window)"
    );
    assert!(
        !script.contains("podman kill --signal KILL $orphan_running"),
        "pre-flight must NOT issue ungraceful SIGKILL on orphan containers"
    );
}

/// After stopping running orphans, the wrapper must `podman rm -af`
/// the per-orphan storage too. Exited-but-not-removed containers
/// still hold bind-mount references (peer-documented: the
/// network-output volume corruption observed 2026-05-16); only `rm`
/// releases them.
#[test]
fn preflight_removes_exited_orphans_to_release_bind_mounts() {
    let config = SlurmConfig::default();
    let script = generate_wrapper_script(&standard_cfg(&config, &[]));
    assert!(
        script.contains(
            "podman --root \"$orphan_storage\" --runroot \"$orphan_runroot\" \
             --cgroup-manager=cgroupfs rm -af"
        ),
        "pre-flight must `podman rm -af` per orphan storage so exited \
         containers no longer hold bind-mount references"
    );
}

/// Belt-and-suspenders: the user-default rootless storage is scanned
/// too (covers operators that ran ad-hoc `podman` outside the
/// per-job storage convention).
#[test]
fn preflight_also_scans_default_storage() {
    let config = SlurmConfig::default();
    let script = generate_wrapper_script(&standard_cfg(&config, &[]));
    assert!(
        script.contains("default_running=$(podman ps -q"),
        "pre-flight must also scan the user-default storage"
    );
    assert!(
        script.contains("podman stop -t 10 $default_running"),
        "default-storage stop must use the same graceful window"
    );
    assert!(
        script.contains("podman rm -af 2>/dev/null"),
        "default-storage rm must run unconditionally to clear exited orphans"
    );
}

/// The pre-flight section must be early enough to never race a future
/// container started by THIS wrapper invocation. `mkdir -p
/// "$PODMAN_STORAGE"` must precede it (so the orphan walk can skip
/// past our own empty storage) but the `podman run --rm` invocation
/// must come AFTER it.
#[test]
fn preflight_lands_between_storage_setup_and_container_run() {
    let config = SlurmConfig::default();
    let script = generate_wrapper_script(&standard_cfg(&config, &[]));
    let mkdir_pos = script
        .find("mkdir -p \"$PODMAN_STORAGE\" \"$PODMAN_RUN\"")
        .expect("storage mkdir must be present");
    let preflight_pos = script
        .find("Pre-flight: scanning for leftover podman containers")
        .expect("pre-flight scan banner must be present");
    let podman_run_pos = script
        .find("--cgroup-manager=cgroupfs run --rm")
        .expect("container run line must be present");
    assert!(
        mkdir_pos < preflight_pos && preflight_pos < podman_run_pos,
        "pre-flight must land AFTER storage setup but BEFORE container run \
         (mkdir@{mkdir_pos} preflight@{preflight_pos} run@{podman_run_pos})"
    );
}

/// Operator escape hatch: `DYNRUNNER_DISABLE_PREFLIGHT_PODMAN=1` skips
/// the whole block. Useful for mid-job diagnostics where the operator
/// wants to keep prior containers running.
#[test]
fn preflight_is_disabled_when_env_var_is_set() {
    let config = SlurmConfig::default();
    let script = generate_wrapper_script(&standard_cfg(&config, &[]));
    assert!(
        script.contains("\"${DYNRUNNER_DISABLE_PREFLIGHT_PODMAN:-0}\" = \"1\""),
        "pre-flight must honor DYNRUNNER_DISABLE_PREFLIGHT_PODMAN=1 escape hatch"
    );
}
