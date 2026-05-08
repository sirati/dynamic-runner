use std::path::Path;

/// Result of a command execution on a gateway.
#[derive(Debug, Clone)]
pub struct CommandResult {
    pub return_code: i32,
    pub stdout: String,
    pub stderr: String,
}

impl CommandResult {
    pub fn success(&self) -> bool {
        self.return_code == 0
    }
}

/// Gateway trait for executing commands and transferring files,
/// either locally or over SSH.
pub trait Gateway: Send + Sync {
    /// Establish connection to gateway.
    fn connect(
        &mut self,
    ) -> impl std::future::Future<Output = Result<(), GatewayError>> + Send;

    /// Close connection to gateway.
    fn disconnect(
        &mut self,
    ) -> impl std::future::Future<Output = Result<(), GatewayError>> + Send;

    /// Execute a command on the gateway.
    fn execute_command(
        &self,
        cmd: &str,
        cwd: Option<&str>,
    ) -> impl std::future::Future<Output = Result<CommandResult, GatewayError>> + Send;

    /// Transfer a file from local to the gateway.
    fn transfer_file(
        &self,
        local: &Path,
        remote: &str,
    ) -> impl std::future::Future<Output = Result<(), GatewayError>> + Send;

    /// Download a file from the gateway to a local path.
    fn download_file(
        &self,
        remote: &str,
        local: &Path,
    ) -> impl std::future::Future<Output = Result<(), GatewayError>> + Send;

    /// Create a directory on the gateway (including parents).
    fn create_directory(
        &self,
        remote: &str,
    ) -> impl std::future::Future<Output = Result<(), GatewayError>> + Send;

    /// Check if a file exists on the gateway.
    fn file_exists(
        &self,
        remote: &str,
    ) -> impl std::future::Future<Output = Result<bool, GatewayError>> + Send;

    /// Setup SSH remote port forwarding.
    /// Must be called before `connect()` for SSH gateways.
    fn setup_port_forwarding(
        &mut self,
        local_port: u16,
        remote_port: u16,
    ) -> Result<(), GatewayError>;
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("not connected")]
    NotConnected,
    #[error("command failed: {0}")]
    CommandFailed(String),
    /// File copy/transfer operation (`transfer_file`, `download_file`)
    /// failed. Pre-migration the Python gateways raised
    /// `RuntimeError(f"File copy failed: ...")` /
    /// `RuntimeError(f"SCP failed: ...")` for these — the PyO3 mapping
    /// preserves that contract by translating `CopyFailed` to
    /// `PyRuntimeError`. Distinct from `Io` (which maps to `OSError`
    /// and is reserved for filesystem failures unrelated to copy
    /// semantics, e.g. `create_directory`).
    #[error("copy failed: {0}")]
    CopyFailed(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}
