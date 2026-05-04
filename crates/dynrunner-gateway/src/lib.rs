pub mod config;
pub mod filesystem;
pub mod local;
pub mod ssh;
pub mod traits;

pub use config::{GatewayConfig, SshConfig, parse_gateway_url};
pub use filesystem::{DirEntry, Filesystem, FsError};
pub use local::LocalGateway;
pub use ssh::SshGateway;
pub use traits::{CommandResult, Gateway};
