use std::path::Path;

use tokio::process::Command;
use tracing;

use crate::config::SshConfig;
use crate::filesystem::{DirEntry, Filesystem, FsError};
use crate::traits::{CommandResult, Gateway, GatewayError};

/// Gateway for SSH connections using a persistent ControlMaster connection.
pub struct SshGateway {
    config: SshConfig,
    connected: bool,
    control_dir: Option<tempfile::TempDir>,
    control_path: Option<String>,
    remote_home: Option<String>,
    forwarded_ports: Vec<(u16, u16)>,
    /// Whether GatewayPorts is enabled on the remote SSH server.
    /// `None` = unknown, `Some(true)` = enabled, `Some(false)` = disabled.
    pub gateway_ports_enabled: Option<bool>,
}

impl SshGateway {
    pub fn new(config: SshConfig) -> Self {
        Self {
            config,
            connected: false,
            control_dir: None,
            control_path: None,
            remote_home: None,
            forwarded_ports: Vec::new(),
            gateway_ports_enabled: None,
        }
    }

    fn ssh_target(&self) -> String {
        match &self.config.user {
            Some(user) => format!("{user}@{}", self.config.host),
            None => self.config.host.clone(),
        }
    }

    fn base_ssh_args(&self) -> Vec<String> {
        let mut args = Vec::new();
        if self.config.port != 22 {
            args.push("-p".into());
            args.push(self.config.port.to_string());
        }
        args
    }

    fn control_args(&self) -> Result<Vec<String>, GatewayError> {
        let cp = self
            .control_path
            .as_ref()
            .ok_or(GatewayError::NotConnected)?;
        Ok(vec![
            "-o".into(),
            format!("ControlPath={cp}"),
            "-o".into(),
            "ControlMaster=no".into(),
        ])
    }

    async fn ssh_command(
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

    /// Expand `~` prefix using the detected remote home directory.
    fn expand_remote_path(&self, path: &str) -> String {
        if let (true, Some(home)) = (path.starts_with('~'), &self.remote_home) {
            path.replacen('~', home, 1)
        } else {
            path.to_string()
        }
    }

    async fn check_gateway_ports(&mut self) {
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
        tracing::info!(
            host = %self.config.host,
            port = self.config.port,
            user = ?self.config.user,
            "connecting to SSH gateway"
        );

        let dir = tempfile::tempdir().map_err(|e| GatewayError::Other(e.to_string()))?;
        let cp = format!("{}/control-socket", dir.path().display());
        self.control_path = Some(cp.clone());
        self.control_dir = Some(dir);

        let mut cmd = Command::new("ssh");
        for arg in self.base_ssh_args() {
            cmd.arg(&arg);
        }
        cmd.args([
            "-M",
            "-N",
            "-f",
            "-o",
            &format!("ControlPath={cp}"),
            "-o",
            "ControlMaster=auto",
            "-o",
            "ControlPersist=yes",
        ]);

        // Add port forwarding
        for &(local_port, remote_port) in &self.forwarded_ports {
            cmd.arg("-R");
            cmd.arg(format!("0.0.0.0:{remote_port}:localhost:{local_port}"));
        }

        cmd.arg(self.ssh_target());

        let output = cmd.output().await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(GatewayError::CommandFailed(format!(
                "SSH master connection failed: {stderr}"
            )));
        }

        self.connected = true;
        tracing::info!("SSH master connection established");

        // Detect remote home
        let home_result = self.ssh_command("echo $HOME", None).await?;
        if home_result.success() {
            self.remote_home = Some(home_result.stdout.trim().to_string());
            tracing::info!(home = ?self.remote_home, "detected remote home");
        }

        self.check_gateway_ports().await;

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), GatewayError> {
        if !self.connected {
            return Ok(());
        }

        let mut cmd = Command::new("ssh");
        for arg in self.base_ssh_args() {
            cmd.arg(&arg);
        }
        if let Some(cp) = &self.control_path {
            cmd.args(["-O", "exit", "-o", &format!("ControlPath={cp}")]);
        }
        cmd.arg(self.ssh_target());
        let _ = cmd.output().await;

        self.connected = false;
        // control_dir TempDir drops and cleans up automatically
        tracing::info!("SSH gateway disconnected");
        Ok(())
    }

    async fn execute_command(
        &self,
        cmd: &str,
        cwd: Option<&str>,
    ) -> Result<CommandResult, GatewayError> {
        self.ssh_command(cmd, cwd).await
    }

    async fn transfer_file(
        &self,
        local: &Path,
        remote: &str,
    ) -> Result<(), GatewayError> {
        if !self.connected {
            return Err(GatewayError::NotConnected);
        }

        let expanded = self.expand_remote_path(remote);

        let mut cmd = Command::new("scp");
        if self.config.port != 22 {
            cmd.args(["-P", &self.config.port.to_string()]);
        }
        if let Some(cp) = &self.control_path {
            cmd.args(["-o", &format!("ControlPath={cp}")]);
        }
        cmd.arg(local.to_string_lossy().as_ref());
        cmd.arg(format!("{}:{expanded}", self.ssh_target()));

        let output = cmd.output().await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(GatewayError::TransferFailed(stderr.into_owned()));
        }

        tracing::debug!(?local, remote = expanded, "file transferred via SCP");
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

        let expanded = self.expand_remote_path(remote);

        if let Some(parent) = local.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut cmd = Command::new("scp");
        if self.config.port != 22 {
            cmd.args(["-P", &self.config.port.to_string()]);
        }
        if let Some(cp) = &self.control_path {
            cmd.args(["-o", &format!("ControlPath={cp}")]);
        }
        cmd.arg(format!("{}:{expanded}", self.ssh_target()));
        cmd.arg(local.to_string_lossy().as_ref());

        let output = cmd.output().await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(GatewayError::TransferFailed(stderr.into_owned()));
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

/// Wrap `s` in POSIX single quotes, escaping any embedded single quote as
/// `'\''`. Safe to interpolate into a remote shell command line.
fn shell_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

/// Parse the null-delimited output of
/// `find <dir> -mindepth 1 -maxdepth 1 -L -printf "%y\t%s\t%P\0"`
/// into [`DirEntry`] values. The `-L` flag dereferences symlinks so `%y`
/// reports the target kind; broken symlinks come back as `%y=l` and are
/// skipped silently.
fn parse_find_printf(stdout: &str) -> Vec<DirEntry> {
    let mut out = Vec::new();
    for record in stdout.split('\0') {
        if record.is_empty() {
            continue;
        }
        let mut fields = record.splitn(3, '\t');
        let kind = fields.next().unwrap_or("");
        let size_str = fields.next().unwrap_or("0");
        let name = match fields.next() {
            Some(n) if !n.is_empty() => n.to_owned(),
            _ => continue,
        };
        match kind {
            "d" => out.push(DirEntry::Dir { name }),
            "f" => {
                let size: u64 = size_str.parse().unwrap_or(0);
                out.push(DirEntry::File { name, size });
            }
            // 'l' = broken symlink (under -L); other kinds (sockets, fifos,
            // block/char devices) are filtered out the same way.
            _ => {}
        }
    }
    out.sort_by(|a, b| a.name().cmp(b.name()));
    out
}

impl Filesystem for SshGateway {
    async fn list_dir(&self, path: &str) -> Result<Vec<DirEntry>, FsError> {
        if !self.connected {
            return Err(FsError::NotConnected);
        }
        let expanded = self.expand_remote_path(path);
        let quoted = shell_quote(&expanded);

        // -L follows symlinks for stat: %y reports the target's kind.
        // It's a global option and MUST appear before the path (POSIX +
        // GNU) — placed after the path it parses as a predicate and
        // find exits 1 with "unknown predicate `-L'".
        // %P is the path relative to the find root, which with maxdepth=1
        // is just the entry's basename. \0 separator survives names with
        // newlines/tabs.
        let cmd = format!(
            "find -L {quoted} -mindepth 1 -maxdepth 1 -printf '%y\\t%s\\t%P\\0'"
        );
        let result = self
            .ssh_command(&cmd, None)
            .await
            .map_err(|e| FsError::Other(format!("ssh exec failed: {e}")))?;

        if !result.success() {
            let stderr = result.stderr.trim();
            if stderr.contains("No such file or directory") {
                return Err(FsError::NotFound(expanded));
            }
            if stderr.contains("Not a directory") {
                return Err(FsError::NotADirectory(expanded));
            }
            return Err(FsError::ListingFailed(format!(
                "find exited {}: {stderr}",
                result.return_code,
            )));
        }

        Ok(parse_find_printf(&result.stdout))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_quote_simple() {
        assert_eq!(shell_quote("/tmp/foo"), "'/tmp/foo'");
    }

    #[test]
    fn shell_quote_embedded_single_quote() {
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
    }

    #[test]
    fn parse_find_printf_basic() {
        let stdout = "f\t100\tfile.bin\0d\t4096\tsubdir\0l\t0\tbroken\0";
        let entries = parse_find_printf(stdout);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], DirEntry::File { name: "file.bin".into(), size: 100 });
        assert_eq!(entries[1], DirEntry::Dir { name: "subdir".into() });
    }

    #[test]
    fn parse_find_printf_alphabetical() {
        let stdout = "f\t1\tz\0f\t2\ta\0d\t0\tm\0";
        let entries = parse_find_printf(stdout);
        let names: Vec<_> = entries.iter().map(|e| e.name()).collect();
        assert_eq!(names, vec!["a", "m", "z"]);
    }

    #[test]
    fn parse_find_printf_handles_tabs_in_names() {
        // Tabs inside names: splitn(3) means everything after the second
        // tab is the name. So a name with a tab is preserved.
        let stdout = "f\t10\tname\twith\ttab\0";
        let entries = parse_find_printf(stdout);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name(), "name\twith\ttab");
    }

    #[test]
    fn parse_find_printf_empty() {
        assert!(parse_find_printf("").is_empty());
        assert!(parse_find_printf("\0\0").is_empty());
    }
}
