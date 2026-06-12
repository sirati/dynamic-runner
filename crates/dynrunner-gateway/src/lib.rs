pub mod config;
pub mod local;
pub(crate) mod path;
pub mod shell;
pub mod ssh;
pub mod traits;

pub use config::{GatewayConfig, SshConfig, parse_gateway_url};
pub use local::LocalGateway;
pub use shell::{shell_join, shell_quote};
pub use ssh::{
    MasterForward, SshGateway, auth_options_for, control_socket_alive, master_forward_cancel,
    master_forward_open, no_mux_options, spawn_master_babysitter,
};
pub use traits::{CommandResult, Gateway};
