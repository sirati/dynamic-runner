pub mod config;
pub mod local;
pub(crate) mod path;
pub mod shell;
pub mod ssh;
pub mod traits;

pub use config::{GatewayConfig, SshConfig, parse_gateway_url};
pub use local::LocalGateway;
pub use shell::{shell_join, shell_quote};
pub use ssh::{SshGateway, auth_options_for};
pub use traits::{CommandResult, Gateway};
