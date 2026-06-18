pub mod cgroup;
pub mod manager;
pub mod memprofile;
pub mod memuse;
pub mod monitor;
pub mod oom;
pub mod pool;
pub mod stats;
pub mod worker;
pub mod worker_stdio_tail;

pub use cgroup::{
    CgroupSetupError, NestedCgroupHandle, SubcgroupHandle, prepare_worker_subgroup,
    setup_worker_cgroup, setup_worker_cgroup_default,
};
pub use manager::{
    LocalManager, LocalManagerConfig, RestartContext, RestartPredicate, WorkerFactory,
};
pub use monitor::{ProcStatmMonitor, ResourceMonitor};
pub use oom::{LogTrigger, OomWatcher, OomWatcherConfig, OomWatcherSnapshot};
pub use pool::{EnsureWorkerOutcome, ResourcePressureResult, WorkerPool};
pub use stats::ProcessingStats;
pub use worker::{WorkerEvent, WorkerExitStatus, WorkerHandle};
pub use worker_stdio_tail::{DEFAULT_STDIO_TAIL_BYTES, append_stdio_tail, read_file_tail};
