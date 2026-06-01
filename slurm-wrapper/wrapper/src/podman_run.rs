//! Single concern: build the `podman run` argv (byte-for-byte the same
//! effective argv as generate.rs:826-843) and run the container to
//! completion. Adds `--log-level=debug` + conmon debug logging (new
//! capability). Phase 1 (1H) fills bodies.

use crate::bin_resolve::ResolvedBins;
use crate::dirs::Layout;
use crate::network::PeerIps;
use dynrunner_slurm_wrapper_config::WrapperConfig;

/// Assemble the full argv vector for the `podman ... run ...` invocation.
/// `mem_cap_bytes == None` omits the `--memory` flags; `secondary_url` is
/// mode-derived (`tcp://localhost:<tunnel_port>` or
/// `tcp://<gateway>:<port>`).
pub fn build_run_argv(
    _cfg: &WrapperConfig,
    _layout: &Layout,
    _bins: &ResolvedBins,
    _mem_cap_bytes: Option<u64>,
    _peer_ips: &PeerIps,
    _quic_port: u16,
    _secondary_url: &str,
) -> Vec<String> {
    todo!("1H: assemble podman run argv")
}
