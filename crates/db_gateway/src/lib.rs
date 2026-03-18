pub mod config;
pub mod local;
pub mod ssh;
pub mod traits;

pub use config::{GatewayConfig, SshConfig};
pub use local::LocalGateway;
pub use ssh::SshGateway;
pub use traits::{CommandResult, Gateway};
