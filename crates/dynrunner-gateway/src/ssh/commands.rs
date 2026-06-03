//! Command-execution + file-transfer + port-forwarding bodies plus
//! the unified `Gateway` trait impl. `connect` and `disconnect`
//! delegate to inherent `SshGateway::connect_inner` /
//! `disconnect_inner` in `connect_disconnect.rs` (Rust forbids
//! overlapping `impl Trait for Type` blocks across files, so the
//! trait impl must stay in this file even though the two heaviest
//! methods physically live next door).

use std::path::Path;

use tokio::process::Command;

use crate::traits::{CommandResult, Gateway, GatewayError};

use super::SshGateway;

impl SshGateway {
    pub(super) async fn ssh_command(
        &self,
        remote_cmd: &str,
        cwd: Option<&str>,
    ) -> Result<CommandResult, GatewayError> {
        if !self.connected {
            return Err(GatewayError::NotConnected);
        }

        let full_cmd = match cwd {
            Some(dir) => format!("cd {dir} && {remote_cmd}"),
            None => remote_cmd.to_string(),
        };

        let mut cmd = Command::new("ssh");
        for arg in self.base_ssh_args() {
            cmd.arg(&arg);
        }
        for arg in self.control_args()? {
            cmd.arg(&arg);
        }
        cmd.arg(self.ssh_target());
        cmd.arg(&full_cmd);

        let output = cmd.output().await?;

        Ok(CommandResult {
            return_code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }

    pub(super) async fn check_gateway_ports(&mut self) {
        for &(_, remote_port) in &self.forwarded_ports {
            let result = self
                .ssh_command(
                    &format!("ss -tulpn 2>/dev/null | grep ':{remote_port}'"),
                    None,
                )
                .await;

            match result {
                Ok(cr) if cr.success() && !cr.stdout.is_empty() => {
                    let is_public = cr.stdout.contains("0.0.0.0:")
                        || cr.stdout.contains("*:")
                        || cr.stdout.contains("[::]:");

                    if is_public {
                        tracing::info!(remote_port, "gateway port is publicly accessible");
                        self.gateway_ports_enabled = Some(true);
                    } else {
                        tracing::warn!(
                            remote_port,
                            "gateway port bound to localhost only — GatewayPorts likely disabled"
                        );
                        self.gateway_ports_enabled = Some(false);
                    }
                }
                _ => {
                    tracing::warn!(remote_port, "could not check gateway port binding");
                    self.gateway_ports_enabled = None;
                }
            }
        }
    }
}

impl Gateway for SshGateway {
    async fn connect(&mut self) -> Result<(), GatewayError> {
        self.connect_inner().await
    }

    async fn disconnect(&mut self) -> Result<(), GatewayError> {
        self.disconnect_inner().await
    }

    async fn execute_command(
        &self,
        cmd: &str,
        cwd: Option<&str>,
    ) -> Result<CommandResult, GatewayError> {
        self.ssh_command(cmd, cwd).await
    }

    async fn transfer_file(&self, local: &Path, remote: &str) -> Result<(), GatewayError> {
        if !self.connected {
            return Err(GatewayError::NotConnected);
        }

        let expanded = self.expand_remote_path(remote);

        // Pre-flight unlink of the destination. scp opens the dest with
        // O_WRONLY|O_TRUNC, which fails with EACCES when the existing
        // file is read-only — observed when sources are produced by a
        // nix derivation (mode 0444) and a prior scp upload propagated
        // those bits to the dest. `rm -f` no-ops when the dest is
        // absent and only requires write perm on the parent directory,
        // which scp itself already requires. Best-effort: any failure
        // is swallowed so the canonical scp error still surfaces if
        // the real cause is something else (e.g. parent-dir non-writable).
        let _ = self.ssh_command(&format!("rm -f {expanded}"), None).await;

        let mut cmd = Command::new("scp");
        if self.config.port != 22 {
            cmd.args(["-P", &self.config.port.to_string()]);
        }
        for arg in self.auth_options() {
            cmd.arg(&arg);
        }
        if let Some(cp) = &self.control_path {
            cmd.args(["-o", &format!("ControlPath={cp}")]);
        }
        cmd.arg(local.to_string_lossy().as_ref());
        cmd.arg(format!("{}:{expanded}", self.ssh_target()));

        let output = cmd.output().await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(GatewayError::CopyFailed(stderr.into_owned()));
        }

        tracing::debug!(?local, remote = expanded, "file transferred via SCP");
        Ok(())
    }

    async fn download_file(&self, remote: &str, local: &Path) -> Result<(), GatewayError> {
        if !self.connected {
            return Err(GatewayError::NotConnected);
        }

        let expanded = self.expand_remote_path(remote);

        // Pre-migration Python's ssh_gateway did NOT pre-create the
        // local parent — scp itself would fail and the non-zero exit
        // surfaced as `RuntimeError(f"SCP download failed: ...")`. The
        // Rust port adds a defensive `mkdir -p` on the local parent;
        // route any failure through `CopyFailed` so the observed
        // exception class stays `RuntimeError`, not `OSError`.
        if let Some(parent) = local.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| GatewayError::CopyFailed(format!("SCP download failed: {e}")))?;
        }

        let mut cmd = Command::new("scp");
        if self.config.port != 22 {
            cmd.args(["-P", &self.config.port.to_string()]);
        }
        for arg in self.auth_options() {
            cmd.arg(&arg);
        }
        if let Some(cp) = &self.control_path {
            cmd.args(["-o", &format!("ControlPath={cp}")]);
        }
        cmd.arg(format!("{}:{expanded}", self.ssh_target()));
        cmd.arg(local.to_string_lossy().as_ref());

        let output = cmd.output().await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(GatewayError::CopyFailed(stderr.into_owned()));
        }

        tracing::debug!(remote = expanded, ?local, "file downloaded via SCP");
        Ok(())
    }

    async fn create_directory(&self, remote: &str) -> Result<(), GatewayError> {
        let expanded = self.expand_remote_path(remote);
        let result = self
            .ssh_command(&format!("mkdir -p {expanded}"), None)
            .await?;
        if !result.success() {
            return Err(GatewayError::CommandFailed(format!(
                "mkdir -p failed: {}",
                result.stderr
            )));
        }
        Ok(())
    }

    async fn file_exists(&self, remote: &str) -> Result<bool, GatewayError> {
        let expanded = self.expand_remote_path(remote);
        let result = self
            .ssh_command(&format!("test -e {expanded}"), None)
            .await?;
        Ok(result.success())
    }

    fn setup_port_forwarding(
        &mut self,
        local_port: u16,
        remote_port: u16,
    ) -> Result<(), GatewayError> {
        if self.connected {
            return Err(GatewayError::Other(
                "cannot setup port forwarding after connection established".into(),
            ));
        }
        tracing::info!(local_port, remote_port, "configuring SSH port forwarding");
        self.forwarded_ports.push((local_port, remote_port));
        Ok(())
    }
}
