pub mod worker;
pub mod manager;
pub mod stats;

pub use manager::{LocalManager, LocalManagerConfig, WorkerFactory};
pub use stats::ProcessingStats;
pub use worker::{WorkerHandle, WorkerEvent};
