use std::path::Path;

use tokio::process::Command;
use tracing;

use crate::traits::{CommandResult, Gateway, GatewayError};

/// Gateway for local execution (direct filesystem + subprocess).
pub struct LocalGateway {
    connected: bool,
}

impl LocalGateway {
    pub fn new() -> Self {
        Self { connected: false }
    }
}

impl Default for LocalGateway {
    fn default() -> Self {
        Self::new()
    }
}

impl Gateway for LocalGateway {
    async fn connect(&mut self) -> Result<(), GatewayError> {
        tracing::info!("using local gateway (direct access)");
        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), GatewayError> {
        self.connected = false;
        tracing::info!("local gateway disconnected");
        Ok(())
    }

    async fn execute_command(
        &self,
        cmd: &str,
        cwd: Option<&str>,
    ) -> Result<CommandResult, GatewayError> {
        if !self.connected {
            return Err(GatewayError::NotConnected);
        }

        tracing::debug!(cmd, "executing locally");

        let mut command = Command::new("sh");
        command.arg("-c").arg(cmd);
        if let Some(dir) = cwd {
            command.current_dir(dir);
        }

        let output = command.output().await?;

        Ok(CommandResult {
            return_code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }

    async fn transfer_file(
        &self,
        local: &Path,
        remote: &str,
    ) -> Result<(), GatewayError> {
        if !self.connected {
            return Err(GatewayError::NotConnected);
        }

        let dest = Path::new(remote);
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::copy(local, dest).await?;
        tracing::debug!(?local, remote, "file copied locally");
        Ok(())
    }

    async fn download_file(
        &self,
        remote: &str,
        local: &Path,
    ) -> Result<(), GatewayError> {
        if !self.connected {
            return Err(GatewayError::NotConnected);
        }

        if let Some(parent) = local.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::copy(remote, local).await?;
        tracing::debug!(remote, ?local, "file downloaded locally");
        Ok(())
    }

    async fn create_directory(&self, remote: &str) -> Result<(), GatewayError> {
        if !self.connected {
            return Err(GatewayError::NotConnected);
        }

        tokio::fs::create_dir_all(remote).await?;
        tracing::debug!(remote, "directory created");
        Ok(())
    }

    async fn file_exists(&self, remote: &str) -> Result<bool, GatewayError> {
        if !self.connected {
            return Err(GatewayError::NotConnected);
        }

        Ok(tokio::fs::try_exists(remote).await.unwrap_or(false))
    }

    fn setup_port_forwarding(
        &mut self,
        _local_port: u16,
        _remote_port: u16,
    ) -> Result<(), GatewayError> {
        // No-op for local gateway
        Ok(())
    }
}
