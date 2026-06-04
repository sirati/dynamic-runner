//! Single concern: scratch-directory layout derivation + creation.
//! Faithful port of generate.rs:34-44 (path derivation) and :328-346
//! (mkdir -p + chmod 700). Phase 1 (1A) fills the bodies.

use dynrunner_slurm_wrapper_config::WrapperConfig;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

/// Container-internal mount point of the per-node scratch `log` dir.
/// `[`Layout::log_tmp`]` (host: `<rndtmp>/log`) is bind-mounted here by
/// `podman_run.rs` (`-v <log_tmp>:/app/log-tmp`). Single source of
/// truth for the one bind mount the reaper-panik sentinel rides on, so
/// the host side ([`Layout::reaper_panik_host_path`]) and the container
/// side ([`reaper_panik_container_path`]) can never drift.
pub const LOG_TMP_CONTAINER_PATH: &str = "/app/log-tmp";

/// Fixed basename of the reaper's graceful-last-resort panik sentinel.
/// Framework-owned (dot-prefixed so it never collides with an operator
/// `--panik-file` or a log file): the wrapper injects
/// `--panik-file <container-path>` so the secondary's in-container
/// watcher polls it, and passes the matching HOST path to the
/// shutdown-manager so a surviving orphan can be asked to stop
/// gracefully.
pub const REAPER_PANIK_SENTINEL_BASENAME: &str = ".dynrunner-reaper.panik";

/// Container-internal path of the reaper-panik sentinel — the exact
/// path the secondary's panik watcher is told to poll. Mirror of
/// [`Layout::reaper_panik_host_path`] across the `log_tmp` bind mount.
pub fn reaper_panik_container_path() -> String {
    format!("{LOG_TMP_CONTAINER_PATH}/{REAPER_PANIK_SENTINEL_BASENAME}")
}

/// All scratch paths + derived names, computed from the config's
/// `rand_suffix` and `secondary_id`. Mirrors the bash shell variables
/// RNDTMP, CONTAINER_NAME, {src,out,log}_tmp, PODMAN_STORAGE/RUN,
/// socket_dir, cmd_socket, the shutdown unit name, and LOCAL_IMAGE.
#[derive(Debug, Clone)]
pub struct Layout {
    pub rndtmp: PathBuf,            // /tmp/<name_prefix>-<suffix>    (generate.rs:35)
    pub container_name: String,     // <name_prefix>-<suffix>-<secondary_id> (:36)
    pub src_tmp: PathBuf,           // <rndtmp>/src                   (:38)
    pub out_tmp: PathBuf,           // <rndtmp>/out                   (:39)
    pub log_tmp: PathBuf,           // <rndtmp>/log                   (:40)
    pub podman_storage: PathBuf,    // <rndtmp>/storage               (:41)
    pub podman_run: PathBuf,        // <rndtmp>/run                   (:42)
    pub socket_dir: PathBuf,        // <rndtmp>/sockets               (:43)
    pub cmd_socket: PathBuf,        // <socket_dir>/cmd.sock          (:44)
    pub shutdown_unit_name: String, // dynrunner-shutdown-<suffix>    (:225)
    pub shutdown_log_path: PathBuf, // <rndtmp>/shutdown-manager.log  (:223)
    pub shutdown_pid_file: PathBuf, // <rndtmp>/shutdown-manager.pid  (:250)
    pub local_image: PathBuf,       // <rndtmp>/<image_tar_basename>  (:696)
}

impl Layout {
    /// Pure derivation from config — no filesystem side effects.
    pub fn derive(cfg: &WrapperConfig) -> Self {
        let rndtmp = PathBuf::from(format!("/tmp/{}-{}", cfg.name_prefix, cfg.rand_suffix));
        let container_name = format!(
            "{}-{}-{}",
            cfg.name_prefix, cfg.rand_suffix, cfg.secondary_id
        );

        let src_tmp = rndtmp.join("src");
        let out_tmp = rndtmp.join("out");
        let log_tmp = rndtmp.join("log");
        let podman_storage = rndtmp.join("storage");
        let podman_run = rndtmp.join("run");
        let socket_dir = rndtmp.join("sockets");
        let cmd_socket = socket_dir.join("cmd.sock");

        let shutdown_unit_name = format!("dynrunner-shutdown-{}", cfg.rand_suffix);
        let shutdown_log_path = rndtmp.join("shutdown-manager.log");
        let shutdown_pid_file = rndtmp.join("shutdown-manager.pid");
        let local_image = rndtmp.join(&cfg.image_tar_basename);

        Self {
            rndtmp,
            container_name,
            src_tmp,
            out_tmp,
            log_tmp,
            podman_storage,
            podman_run,
            socket_dir,
            cmd_socket,
            shutdown_unit_name,
            shutdown_log_path,
            shutdown_pid_file,
            local_image,
        }
    }

    /// HOST-side path of the reaper-panik sentinel: the file under the
    /// per-node scratch `log` dir that is bind-mounted into the
    /// container at [`reaper_panik_container_path`]. The
    /// shutdown-manager (on the host) writes here as a graceful last
    /// resort; the secondary's in-container watcher sees it appear at
    /// the mirrored container path and runs its own shutdown. Derived
    /// from [`Self::log_tmp`] so the two sides share one source of
    /// truth.
    pub fn reaper_panik_host_path(&self) -> PathBuf {
        self.log_tmp.join(REAPER_PANIK_SENTINEL_BASENAME)
    }

    /// mkdir -p the scratch tree; chmod 700 on podman storage + run
    /// (generate.rs:330-346).
    pub fn create_dirs(&self) -> std::io::Result<()> {
        // mkdir -p "$RNDTMP" (generate.rs:330)
        std::fs::create_dir_all(&self.rndtmp)?;
        // mkdir -p src out log sockets (generate.rs:331)
        std::fs::create_dir_all(&self.src_tmp)?;
        std::fs::create_dir_all(&self.out_tmp)?;
        std::fs::create_dir_all(&self.log_tmp)?;
        std::fs::create_dir_all(&self.socket_dir)?;
        // mkdir -p storage run (generate.rs:345)
        std::fs::create_dir_all(&self.podman_storage)?;
        std::fs::create_dir_all(&self.podman_run)?;
        // chmod 700 storage run ONLY (generate.rs:346)
        std::fs::set_permissions(&self.podman_storage, std::fs::Permissions::from_mode(0o700))?;
        std::fs::set_permissions(&self.podman_run, std::fs::Permissions::from_mode(0o700))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_slurm_wrapper_config::{ConnectionMode, WrapperConfig};

    fn cfg_with(suffix: &str, secondary_id: &str, basename: &str) -> WrapperConfig {
        WrapperConfig {
            name_prefix: "asm".to_string(),
            rand_suffix: suffix.to_string(),
            secondary_id: secondary_id.to_string(),
            image_path: "/staged/img.tar".to_string(),
            image_tar_basename: basename.to_string(),
            image_name: "img".to_string(),
            image_tag: "latest".to_string(),
            load_command: "true".to_string(),
            container_command: "cmd".to_string(),
            cores_spec: "-2".to_string(),
            max_memory_spec: "-2G".to_string(),
            mem_manager_reserved_bytes: None,
            forwarded_argv: vec![],
            extra_run_args: vec![],
            srcbins_network: "/net/srcbins".to_string(),
            output_network: "/net/out".to_string(),
            log_network: "/net/log".to_string(),
            dynrunner_network_dir: None,
            connection: ConnectionMode::Reverse {
                connection_info_dir: "/net/conn".to_string(),
            },
            is_observer: false,
            shutdown_manager_bin_path: None,
        }
    }

    #[test]
    fn derive_produces_exact_paths() {
        let cfg = cfg_with("2f1d4e89", "sec-0", "img.tar");
        let l = Layout::derive(&cfg);

        assert_eq!(l.rndtmp, PathBuf::from("/tmp/asm-2f1d4e89"));
        assert_eq!(l.container_name, "asm-2f1d4e89-sec-0");
        assert_eq!(l.src_tmp, PathBuf::from("/tmp/asm-2f1d4e89/src"));
        assert_eq!(l.out_tmp, PathBuf::from("/tmp/asm-2f1d4e89/out"));
        assert_eq!(l.log_tmp, PathBuf::from("/tmp/asm-2f1d4e89/log"));
        assert_eq!(l.podman_storage, PathBuf::from("/tmp/asm-2f1d4e89/storage"));
        assert_eq!(l.podman_run, PathBuf::from("/tmp/asm-2f1d4e89/run"));
        assert_eq!(l.socket_dir, PathBuf::from("/tmp/asm-2f1d4e89/sockets"));
        assert_eq!(
            l.cmd_socket,
            PathBuf::from("/tmp/asm-2f1d4e89/sockets/cmd.sock")
        );
        assert_eq!(l.shutdown_unit_name, "dynrunner-shutdown-2f1d4e89");
        assert_eq!(
            l.shutdown_log_path,
            PathBuf::from("/tmp/asm-2f1d4e89/shutdown-manager.log")
        );
        assert_eq!(
            l.shutdown_pid_file,
            PathBuf::from("/tmp/asm-2f1d4e89/shutdown-manager.pid")
        );
        assert_eq!(l.local_image, PathBuf::from("/tmp/asm-2f1d4e89/img.tar"));
    }

    /// The reaper-panik sentinel's HOST path lives under the per-node
    /// `log` dir (the host side of the `/app/log-tmp` bind mount), and
    /// the CONTAINER path is the basename joined to that mount point.
    /// Both derive from the same basename + mount constant so the
    /// host-side reaper write and the in-container watcher poll the
    /// SAME file.
    #[test]
    fn reaper_panik_host_and_container_paths_mirror_across_log_tmp_mount() {
        let cfg = cfg_with("2f1d4e89", "sec-0", "img.tar");
        let l = Layout::derive(&cfg);
        assert_eq!(
            l.reaper_panik_host_path(),
            PathBuf::from("/tmp/asm-2f1d4e89/log/.dynrunner-reaper.panik"),
            "host side must be <log_tmp>/<basename>"
        );
        assert_eq!(
            reaper_panik_container_path(),
            "/app/log-tmp/.dynrunner-reaper.panik",
            "container side must be <log-tmp mount>/<basename>"
        );
        // The host and container paths share the SAME basename — the
        // load-bearing property that makes the bind-mount mirror work.
        assert_eq!(
            l.reaper_panik_host_path().file_name().unwrap().to_str().unwrap(),
            reaper_panik_container_path().rsplit('/').next().unwrap(),
        );
    }

    #[test]
    fn create_dirs_makes_tree_and_chmods_podman() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        let layout = Layout {
            rndtmp: root.to_path_buf(),
            container_name: "asm-x-sec-0".to_string(),
            src_tmp: root.join("src"),
            out_tmp: root.join("out"),
            log_tmp: root.join("log"),
            podman_storage: root.join("storage"),
            podman_run: root.join("run"),
            socket_dir: root.join("sockets"),
            cmd_socket: root.join("sockets/cmd.sock"),
            shutdown_unit_name: "dynrunner-shutdown-x".to_string(),
            shutdown_log_path: root.join("shutdown-manager.log"),
            shutdown_pid_file: root.join("shutdown-manager.pid"),
            local_image: root.join("img.tar"),
        };

        layout.create_dirs().unwrap();

        for d in [
            &layout.rndtmp,
            &layout.src_tmp,
            &layout.out_tmp,
            &layout.log_tmp,
            &layout.socket_dir,
            &layout.podman_storage,
            &layout.podman_run,
        ] {
            assert!(d.is_dir(), "expected dir to exist: {}", d.display());
        }

        for d in [&layout.podman_storage, &layout.podman_run] {
            let mode = std::fs::metadata(d).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o700, "mode for {}", d.display());
        }
    }
}
