/// Tracing target marking an event as "important" (LLM-wake-worthy).
///
/// Re-exported from the single cross-crate source of truth
/// ([`dynrunner_core::IMPORTANT_TARGET`]); the `dynrunner-pyo3` dual-sink
/// routes this target to stdio under `--important-stdio-only` while the
/// full log keeps everything. Emitting at this target is the only thing a
/// SLURM-pipeline call site needs to know — the stdio gate is one filter,
/// never a per-call-site `if`.
pub(crate) use dynrunner_core::IMPORTANT_TARGET;

pub mod config;
pub mod job_manager;
pub mod packaging;
pub mod peer_info;
pub mod pipeline;
pub mod preparation;
pub mod respawn;
pub mod wrapper_script;

pub use config::SlurmConfig;
pub use job_manager::{JobStatus, JobStatusInfo, SlurmError, SlurmJobManager};
pub use packaging::{PackagingError, PodmanImageMetadata, PodmanPackaging};
pub use peer_info::{
    parse as parse_peer_info, parse_v1_uri, read_dir_v2 as read_peer_info_dir_v2,
    Builder as PeerInfoBuilder, LegacyUri, PeerInfoError, PeerInfoRecord, PeerInfoVersion,
    ReadDirError as PeerInfoReadDirError,
};
pub use pipeline::{pkill_residual_reverse_tunnels, CleanupSteps, PipelineGuard, PipelineSteps};
pub use preparation::{InfoFileReader, PrepError, PreparationOptions, SlurmPreparation};
pub use respawn::{
    SlurmPreparationTunnelEstablisher, SlurmSecondarySpawner, TunnelEstablisher,
    WrapperScriptGenerator,
};
pub use wrapper_script::{
    generate_test_wrapper_script, generate_wrapper_script, ConnectionMode,
    TestWrapperScriptConfig, WrapperScriptConfig,
};
