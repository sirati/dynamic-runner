//! Single concern: copy the image tar to node-local /tmp and load it
//! into podman storage (generate.rs:695-711).

use crate::bin_resolve::ResolvedBins;
use crate::dirs::Layout;
use dynrunner_slurm_wrapper_config::WrapperConfig;
use std::os::unix::process::CommandExt;
use std::process::Command;

/// Operator-facing marker the bash prints to STDOUT on load failure
/// (generate.rs:707). Consumers scan the `.out` file for this first.
const LOAD_FAILED_STDOUT: &str =
    "ERROR: image load failed; secondary cannot start. See the .err file for the runtime's diagnostic.";
/// The terser STDERR companion (generate.rs:708).
const LOAD_FAILED_STDERR: &str = "ERROR: image load failed; secondary cannot start.";

/// `cp cfg.image_path -> layout.local_image`, then run `cfg.load_command`
/// via `bash -c` with `LOCAL_IMAGE`/`PODMAN_STORAGE`/`PODMAN_RUN` (and the
/// wrapper-shell-scope `PODMAN_BIN`/`RM_BIN`) exported. `Err` carries the
/// operator-facing failure marker text.
pub fn copy_and_load(
    cfg: &WrapperConfig,
    layout: &Layout,
    bins: &ResolvedBins,
) -> Result<(), String> {
    // cp "{image_path}" "$LOCAL_IMAGE" (generate.rs:695-698).
    println!("Copying image to local temp directory...");
    std::fs::copy(&cfg.image_path, &layout.local_image).map_err(|e| {
        format!(
            "failed to copy image {} to {}: {e}",
            cfg.image_path,
            layout.local_image.display()
        )
    })?;
    println!("Image copied to: {}", layout.local_image.display());

    // if ! {load_command}; then ... (generate.rs:700-711). The runtime's
    // own stdout/stderr is inherited so its diagnostic lands in the job's
    // .out/.err exactly as the bash left it.
    //
    // XDG_RUNTIME_DIR is set per-child to `layout.podman_run` (the bash
    // exported it globally at generate.rs:347 for podman's rootless storage
    // cookie; here it is a per-`Command` env so it never clobbers the
    // wrapper's own value that `shutdown_spawn`'s bus probe reads).
    //
    // The child mask reset (signals::child_pre_exec) restores an empty
    // signal mask before exec so the load command (and anything it spawns)
    // sees normal signal disposition rather than the wrapper's blocked set.
    println!("Loading image into container runtime...");
    let mut cmd = Command::new("bash");
    cmd.arg("-c")
        .arg(&cfg.load_command)
        .env("LOCAL_IMAGE", &layout.local_image)
        .env("PODMAN_STORAGE", &layout.podman_storage)
        .env("PODMAN_RUN", &layout.podman_run)
        .env("XDG_RUNTIME_DIR", &layout.podman_run)
        .env("PODMAN_BIN", &bins.podman)
        .env("RM_BIN", &bins.rm);
    // SAFETY: child_pre_exec runs only an async-signal-safe sigprocmask.
    unsafe {
        cmd.pre_exec(crate::signals::child_pre_exec());
    }
    let status = cmd
        .status()
        .map_err(|e| format!("failed to spawn load command via bash: {e}"))?;

    if !status.success() {
        println!("{LOAD_FAILED_STDOUT}");
        eprintln!("{LOAD_FAILED_STDERR}");
        return Err(LOAD_FAILED_STDOUT.to_string());
    }

    println!("Image loaded successfully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_slurm_wrapper_config::ConnectionMode;
    use std::path::PathBuf;
    use tempfile::tempdir;

    /// Build a `WrapperConfig` with every field populated; the test
    /// overrides only `image_path`/`load_command` per case.
    fn cfg_with(image_path: String, load_command: String) -> WrapperConfig {
        WrapperConfig {
            name_prefix: "asm".to_string(),
            rand_suffix: "2f1d4e89".to_string(),
            secondary_id: "sec-0".to_string(),
            image_path,
            image_tar_basename: "asm-tokenizer.tar".to_string(),
            image_name: "asm-tokenizer".to_string(),
            image_tag: "latest".to_string(),
            load_command,
            container_command: "python -m asm_tokenizer.secondary".to_string(),
            cores_spec: "-2".to_string(),
            max_memory_spec: "-2G".to_string(),
            mem_manager_reserved_bytes: Some(524_288_000),
            forwarded_argv: vec!["--platform".to_string(), "x86".to_string()],
            extra_run_args: vec!["--ulimit".to_string(), "nofile=8192:8192".to_string()],
            srcbins_network: "/net/srcbins".to_string(),
            output_network: "/net/out".to_string(),
            log_network: "/net/log".to_string(),
            dynrunner_network_dir: Some("/net/dynrunner".to_string()),
            connection: ConnectionMode::Standard {
                gateway_host: "gw.cluster".to_string(),
                gateway_port: 4433,
            },
            is_observer: false,
            shutdown_manager_bin_path: Some(PathBuf::from("/opt/dynrunner-slurm-shutdown")),
        }
    }

    /// A `Layout` rooted in `root`, with a dummy source tar written and its
    /// bytes returned so callers can assert the copy is faithful.
    fn fixture(
        root: &std::path::Path,
        load_command: &str,
    ) -> (WrapperConfig, Layout, ResolvedBins, Vec<u8>) {
        let rndtmp = root.join("rndtmp");
        std::fs::create_dir_all(&rndtmp).unwrap();
        let src = root.join("source.tar");
        let bytes = b"docker-archive-bytes".to_vec();
        std::fs::write(&src, &bytes).unwrap();

        let layout = Layout {
            rndtmp: rndtmp.clone(),
            container_name: "asm-2f1d4e89-sec-0".to_string(),
            src_tmp: rndtmp.join("src"),
            out_tmp: rndtmp.join("out"),
            log_tmp: rndtmp.join("log"),
            podman_storage: rndtmp.join("storage"),
            podman_run: rndtmp.join("run"),
            socket_dir: rndtmp.join("sockets"),
            cmd_socket: rndtmp.join("sockets/cmd.sock"),
            shutdown_unit_name: "dynrunner-shutdown-2f1d4e89".to_string(),
            shutdown_log_dir: rndtmp.join("log-network/sec-0"),
            shutdown_log_path: rndtmp.join("log-network/sec-0/shutdown-manager.log"),
            shutdown_pid_file: rndtmp.join("shutdown-manager.pid"),
            local_image: rndtmp.join("asm-tokenizer.tar"),
        };
        let cfg = cfg_with(src.to_string_lossy().into_owned(), load_command.to_string());
        let bins = ResolvedBins {
            podman: "podman".to_string(),
            rm: "rm".to_string(),
        };
        (cfg, layout, bins, bytes)
    }

    #[test]
    fn load_success_copies_image() {
        let dir = tempdir().unwrap();
        let (cfg, layout, bins, bytes) = fixture(dir.path(), "true");

        copy_and_load(&cfg, &layout, &bins).expect("load should succeed");

        assert!(layout.local_image.exists());
        assert_eq!(std::fs::read(&layout.local_image).unwrap(), bytes);
    }

    #[test]
    fn load_failure_returns_err() {
        let dir = tempdir().unwrap();
        let (cfg, layout, bins, _) = fixture(dir.path(), "false");

        let err = copy_and_load(&cfg, &layout, &bins).expect_err("non-zero load command must fail");
        assert_eq!(err, LOAD_FAILED_STDOUT);
        // The copy still happened before the load attempt.
        assert!(layout.local_image.exists());
    }

    #[test]
    fn load_command_sees_exported_env() {
        let dir = tempdir().unwrap();
        let probe = dir.path().join("probe.txt");
        // Echo every var the wrapper shell scope provides into one file.
        let cmd = format!(
            "printf '%s\\n%s\\n%s\\n%s\\n%s\\n' \
             \"$LOCAL_IMAGE\" \"$PODMAN_STORAGE\" \"$PODMAN_RUN\" \"$PODMAN_BIN\" \"$RM_BIN\" > {}",
            probe.display()
        );
        let (cfg, layout, bins, _) = fixture(dir.path(), &cmd);

        copy_and_load(&cfg, &layout, &bins).expect("probe load should succeed");

        let got = std::fs::read_to_string(&probe).unwrap();
        let lines: Vec<&str> = got.lines().collect();
        assert_eq!(lines[0], layout.local_image.to_string_lossy());
        assert_eq!(lines[1], layout.podman_storage.to_string_lossy());
        assert_eq!(lines[2], layout.podman_run.to_string_lossy());
        assert_eq!(lines[3], bins.podman);
        assert_eq!(lines[4], bins.rm);
    }
}
