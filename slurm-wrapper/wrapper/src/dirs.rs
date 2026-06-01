//! Single concern: scratch-directory layout derivation + creation.
//! Faithful port of generate.rs:34-44 (path derivation) and :328-346
//! (mkdir -p + chmod 700). Phase 1 (1A) fills the bodies.

use dynrunner_slurm_wrapper_config::WrapperConfig;
use std::path::PathBuf;

/// All scratch paths + derived names, computed from the config's
/// `rand_suffix` and `secondary_id`. Mirrors the bash shell variables
/// RNDTMP, CONTAINER_NAME, {src,out,log}_tmp, PODMAN_STORAGE/RUN,
/// socket_dir, cmd_socket, the shutdown unit name, and LOCAL_IMAGE.
#[derive(Debug, Clone)]
pub struct Layout {
    pub rndtmp: PathBuf,            // /tmp/asm-<suffix>              (generate.rs:35)
    pub container_name: String,     // asm-<suffix>-<secondary_id>    (:36)
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
    pub fn derive(_cfg: &WrapperConfig) -> Self {
        todo!("1A: derive paths from cfg.rand_suffix + cfg.secondary_id")
    }

    /// mkdir -p the scratch tree; chmod 700 on podman storage + run
    /// (generate.rs:330-346).
    pub fn create_dirs(&self) -> std::io::Result<()> {
        todo!("1A: mkdir -p scratch tree + chmod 700 storage/run")
    }
}
