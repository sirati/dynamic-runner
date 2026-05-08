use std::path::Path;
use std::time::Duration;

use tokio::process::Command;
use tokio::time;
use tracing;

use crate::filesystem::{DirEntry, Filesystem, FsError};
use crate::path::expand_tilde;
use crate::traits::{CommandResult, Gateway, GatewayError};

/// Hard ceiling on a single locally-executed command. Mirrors the Python
/// `subprocess.run(..., timeout=300)` in `local_gateway.py`. A run that
/// exceeds this is killed and reported with the canonical `(-1, "",
/// "Command timed out")` shape so callers can match on the same shape
/// regardless of which gateway implementation answered.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(300);

/// Gateway for local execution (direct filesystem + subprocess).
pub struct LocalGateway {
    connected: bool,
    /// Resolved at construction from `$HOME`. Used to expand a leading `~`
    /// in caller-supplied "remote" paths so that, e.g., a config string
    /// `~/.cache/dynrunner` resolves to the same place the SSH gateway
    /// would have resolved it on the remote host.
    home: Option<String>,
}

impl LocalGateway {
    pub fn new() -> Self {
        Self {
            connected: false,
            home: std::env::var("HOME").ok(),
        }
    }

    /// Apply tilde expansion using the local `$HOME`. When `$HOME` is unset
    /// the path is returned verbatim.
    fn expand(&self, path: &str) -> String {
        expand_tilde(path, self.home.as_deref())
    }
}

impl Default for LocalGateway {
    fn default() -> Self {
        Self::new()
    }
}

impl Gateway for LocalGateway {
    async fn connect(&mut self) -> Result<(), GatewayError> {
        tracing::info!(home = ?self.home, "using local gateway (direct access)");
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
        // `output()` consumes the Command and resolves to (status, stdout,
        // stderr). When the wrapping `time::timeout` elapses we drop that
        // future, which drops the underlying `Child`. `kill_on_drop(true)`
        // turns that drop into a SIGKILL on the spawned process so a
        // runaway command can't outlive its caller.
        command.kill_on_drop(true);

        match time::timeout(COMMAND_TIMEOUT, command.output()).await {
            Ok(Ok(output)) => Ok(CommandResult {
                return_code: output.status.code().unwrap_or(-1),
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            }),
            Ok(Err(e)) => Err(GatewayError::from(e)),
            Err(_elapsed) => {
                tracing::error!(cmd, ?COMMAND_TIMEOUT, "local command timed out");
                Ok(CommandResult {
                    return_code: -1,
                    stdout: String::new(),
                    stderr: "Command timed out".into(),
                })
            }
        }
    }

    async fn transfer_file(
        &self,
        local: &Path,
        remote: &str,
    ) -> Result<(), GatewayError> {
        if !self.connected {
            return Err(GatewayError::NotConnected);
        }

        let expanded = self.expand(remote);
        let dest = Path::new(&expanded);
        // Pre-migration Python wrapped parent `mkdir` + `copy2` in a
        // single try/except that re-raised as
        // `RuntimeError(f"File copy failed: {e}")`. Bare `?` here
        // would surface `GatewayError::Io` (=> Python `OSError`) and
        // silently swap the observed exception class — route both
        // io faults through `CopyFailed` (=> `PyRuntimeError`).
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| GatewayError::CopyFailed(format!("File copy failed: {e}")))?;
        }
        // Pre-flight unlink. `tokio::fs::copy` opens the destination
        // O_WRONLY|O_TRUNC, which fails with EACCES when the existing
        // dest is read-only — observed when sources come from a nix
        // derivation (mode 0444) and a previous copy propagated those
        // bits. Best-effort: when the dest is absent or the unlink fails
        // for another reason, the subsequent copy will surface the real
        // error. Same race class as the SSH gateway's pre-flight `rm -f`.
        tokio::fs::remove_file(dest).await.ok();
        tokio::fs::copy(local, dest)
            .await
            .map_err(|e| GatewayError::CopyFailed(format!("File copy failed: {e}")))?;
        tracing::debug!(?local, remote = %expanded, "file copied locally");
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

        let expanded = self.expand(remote);
        // See `transfer_file`: parent-mkdir + copy share the
        // pre-migration `RuntimeError(f"File copy failed: ...")`
        // contract. Both are routed through `CopyFailed`.
        if let Some(parent) = local.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| GatewayError::CopyFailed(format!("File copy failed: {e}")))?;
        }
        tokio::fs::copy(&expanded, local)
            .await
            .map_err(|e| GatewayError::CopyFailed(format!("File copy failed: {e}")))?;
        tracing::debug!(remote = %expanded, ?local, "file downloaded locally");
        Ok(())
    }

    async fn create_directory(&self, remote: &str) -> Result<(), GatewayError> {
        if !self.connected {
            return Err(GatewayError::NotConnected);
        }

        let expanded = self.expand(remote);
        tokio::fs::create_dir_all(&expanded).await?;
        tracing::debug!(remote = %expanded, "directory created");
        Ok(())
    }

    async fn file_exists(&self, remote: &str) -> Result<bool, GatewayError> {
        if !self.connected {
            return Err(GatewayError::NotConnected);
        }

        let expanded = self.expand(remote);
        Ok(tokio::fs::try_exists(&expanded).await.unwrap_or(false))
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

impl Filesystem for LocalGateway {
    async fn list_dir(&self, path: &str) -> Result<Vec<DirEntry>, FsError> {
        if !self.connected {
            return Err(FsError::NotConnected);
        }

        let expanded = self.expand(path);
        let mut read = match tokio::fs::read_dir(&expanded).await {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(FsError::NotFound(expanded));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotADirectory => {
                return Err(FsError::NotADirectory(expanded));
            }
            Err(e) => return Err(FsError::Io(e)),
        };

        let mut entries = Vec::new();
        while let Some(child) = read.next_entry().await? {
            let name = match child.file_name().into_string() {
                Ok(s) => s,
                Err(_) => {
                    tracing::warn!(
                        path = %expanded,
                        "skipping non-UTF-8 entry in directory listing"
                    );
                    continue;
                }
            };

            // Follow symlinks (matches the historical Python `Path.is_file()`
            // semantics). Broken symlinks bubble up as Err here; skip silently.
            let meta = match tokio::fs::metadata(child.path()).await {
                Ok(m) => m,
                Err(_) => continue,
            };

            if meta.is_dir() {
                entries.push(DirEntry::Dir { name });
            } else if meta.is_file() {
                entries.push(DirEntry::File {
                    name,
                    size: meta.len(),
                });
            }
            // Other kinds (sockets, fifos, block/char devices) are ignored.
        }

        entries.sort_by(|a, b| a.name().cmp(b.name()));
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a gateway whose `home` is a fixed string. Avoids leaking
    /// process `$HOME` into the assertion.
    fn gw_with_home(home: &str) -> LocalGateway {
        LocalGateway {
            connected: true,
            home: Some(home.to_string()),
        }
    }

    #[test]
    fn expand_passes_through_when_no_tilde() {
        let gw = gw_with_home("/home/u");
        assert_eq!(gw.expand("/abs/p"), "/abs/p");
        assert_eq!(gw.expand("rel/p"), "rel/p");
    }

    #[test]
    fn expand_replaces_leading_tilde() {
        let gw = gw_with_home("/home/u");
        assert_eq!(gw.expand("~"), "/home/u");
        assert_eq!(gw.expand("~/foo"), "/home/u/foo");
    }

    #[test]
    fn expand_no_home_falls_through() {
        let gw = LocalGateway {
            connected: true,
            home: None,
        };
        assert_eq!(gw.expand("~/foo"), "~/foo");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn create_directory_expands_tilde() {
        let tmp = tempfile::tempdir().unwrap();
        let mut gw = gw_with_home(tmp.path().to_str().unwrap());
        gw.connect().await.unwrap();

        gw.create_directory("~/nested/leaf").await.unwrap();
        assert!(tmp.path().join("nested/leaf").is_dir());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn file_exists_expands_tilde() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::write(tmp.path().join("marker"), b"x")
            .await
            .unwrap();
        let mut gw = gw_with_home(tmp.path().to_str().unwrap());
        gw.connect().await.unwrap();

        assert!(gw.file_exists("~/marker").await.unwrap());
        assert!(!gw.file_exists("~/missing").await.unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn transfer_file_expands_tilde_and_overwrites_readonly_dest() {
        // Reproduce the 0444 race documented in 9a870f4: a previous copy
        // left the destination read-only. Without the pre-flight unlink,
        // `tokio::fs::copy` would fail with EACCES.
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src.bin");
        tokio::fs::write(&src, b"new").await.unwrap();

        let dest_dir = tmp.path().join("home");
        tokio::fs::create_dir_all(&dest_dir).await.unwrap();
        let dest = dest_dir.join("dst.bin");
        tokio::fs::write(&dest, b"old").await.unwrap();
        // 0o444 — read-only, owner included.
        let mut perms = tokio::fs::metadata(&dest).await.unwrap().permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o444);
        }
        tokio::fs::set_permissions(&dest, perms).await.unwrap();

        let mut gw = gw_with_home(tmp.path().to_str().unwrap());
        gw.connect().await.unwrap();
        gw.transfer_file(&src, "~/home/dst.bin").await.unwrap();

        let got = tokio::fs::read(&dest).await.unwrap();
        assert_eq!(got, b"new");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn download_file_expands_tilde() {
        let tmp = tempfile::tempdir().unwrap();
        let remote = tmp.path().join("payload");
        tokio::fs::write(&remote, b"hello").await.unwrap();
        let local = tmp.path().join("out/copy");

        let mut gw = gw_with_home(tmp.path().to_str().unwrap());
        gw.connect().await.unwrap();
        gw.download_file("~/payload", &local).await.unwrap();

        assert_eq!(tokio::fs::read(&local).await.unwrap(), b"hello");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn transfer_file_missing_source_yields_copy_failed() {
        // Pre-migration Python wrapped `copy2(...)` failures in
        // `RuntimeError(f"File copy failed: {e}")`. The Rust contract
        // is `GatewayError::CopyFailed(...)` (=> Python `RuntimeError`).
        // A bare `?` on `tokio::fs::copy` would surface `Io` (=>
        // `OSError`), which is the bug the audit caught.
        let tmp = tempfile::tempdir().unwrap();
        let mut gw = gw_with_home(tmp.path().to_str().unwrap());
        gw.connect().await.unwrap();

        let missing = tmp.path().join("does-not-exist");
        let dest = tmp.path().join("dst");
        let err = gw
            .transfer_file(&missing, dest.to_str().unwrap())
            .await
            .unwrap_err();
        assert!(
            matches!(err, GatewayError::CopyFailed(_)),
            "expected CopyFailed, got {err:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn download_file_missing_source_yields_copy_failed() {
        let tmp = tempfile::tempdir().unwrap();
        let mut gw = gw_with_home(tmp.path().to_str().unwrap());
        gw.connect().await.unwrap();

        let dest = tmp.path().join("out/copy");
        let err = gw
            .download_file("~/does-not-exist", &dest)
            .await
            .unwrap_err();
        assert!(
            matches!(err, GatewayError::CopyFailed(_)),
            "expected CopyFailed, got {err:?}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn execute_command_timeout_returns_canonical_shape() {
        // `start_paused = true` freezes the tokio clock from t=0. We then
        // race the (virtually) 300-second `sleep 600` against an advance
        // of virtual time past `COMMAND_TIMEOUT`. With paused time, the
        // sleep child is still real but the timer wrapping it is virtual,
        // so `time::timeout` fires immediately once we advance — driving
        // `kill_on_drop` to terminate the sleep without us waiting 300s.
        let mut gw = LocalGateway::new();
        gw.connect().await.unwrap();

        let advance = async {
            // Yield so the spawn of `sleep` has a chance to register
            // with the runtime before we advance virtual time past the
            // timeout boundary.
            tokio::task::yield_now().await;
            tokio::time::advance(COMMAND_TIMEOUT + Duration::from_secs(1)).await;
        };

        let (result, _) = tokio::join!(
            gw.execute_command("sleep 600", None),
            advance,
        );
        let result = result.unwrap();
        assert_eq!(result.return_code, -1);
        assert_eq!(result.stdout, "");
        assert_eq!(result.stderr, "Command timed out");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execute_command_fast_command_succeeds() {
        let mut gw = LocalGateway::new();
        gw.connect().await.unwrap();

        let result = gw.execute_command("echo hi", None).await.unwrap();
        assert_eq!(result.return_code, 0);
        assert_eq!(result.stdout.trim(), "hi");
    }
}
