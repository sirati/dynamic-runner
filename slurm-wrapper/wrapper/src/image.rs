//! Single concern: copy the image tar to node-local /tmp and load it
//! into podman storage (generate.rs:695-711). Phase 1 (1G) fills body.

use crate::bin_resolve::ResolvedBins;
use crate::dirs::Layout;
use dynrunner_slurm_wrapper_config::WrapperConfig;

/// `cp cfg.image_path -> layout.local_image`, then run `cfg.load_command`
/// via `bash -c` with `LOCAL_IMAGE`/`PODMAN_STORAGE`/`PODMAN_RUN`
/// exported. `Err` carries the operator-facing failure marker text.
pub fn copy_and_load(
    _cfg: &WrapperConfig,
    _layout: &Layout,
    _bins: &ResolvedBins,
) -> Result<(), String> {
    todo!("1G: cp image + bash -c load_command")
}
