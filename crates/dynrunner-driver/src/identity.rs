//! Identity helpers: per-cluster ed25519 keypair generation +
//! ssh_config(5) emission.
//!
//! Per locked design point (l): the framework owns
//! `ensure_dispatcher_keypair` and `write_ssh_config`. It does NOT
//! own `provision_user` — that's harness-specific (it shells out to
//! the cluster's own flake app) and consumers compose with
//! subprocess directly.
//!
//! Both functions are sync and side-effecting: they write files
//! under operator-supplied paths. They are not idempotent in the
//! "do nothing if file exists" sense for `write_ssh_config` (it
//! deliberately overwrites every call to capture port/user drift),
//! but `ensure_dispatcher_keypair` IS — re-invoking returns the
//! existing paths without regenerating, because the cluster's
//! authorized_keys already pinned the matching pubkey on first run.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    /// `ssh-keygen` could not be invoked (binary missing).
    #[error("failed to spawn ssh-keygen: {0}")]
    KeygenSpawn(#[source] io::Error),
    /// `ssh-keygen` exited non-zero.
    #[error("ssh-keygen failed: {stderr}")]
    KeygenFailed { stderr: String },
    /// Filesystem operation under the state dir failed.
    #[error("identity io: {0}")]
    Io(#[from] io::Error),
}

/// Generate (or return existing) ed25519 keypair under
/// `<state_dir>/keys/`. The private key is mode 0600.
///
/// Returns `(private_key_path, public_key_path)`. Re-runnable: on
/// second invocation the existing paths are returned without
/// regenerating (the cluster's authorized_keys already has the
/// matching pubkey from the prior provision). The `comment` is
/// derived from `state_dir`'s basename so operators tracing leaked
/// keys can map them back to their source instance.
pub fn ensure_dispatcher_keypair(state_dir: &Path) -> Result<(PathBuf, PathBuf), IdentityError> {
    let keys_dir = state_dir.join("keys");
    fs::create_dir_all(&keys_dir)?;
    let priv_path = keys_dir.join("id_ed25519");
    let pub_path = keys_dir.join("id_ed25519.pub");
    if priv_path.exists() && pub_path.exists() {
        return Ok((priv_path, pub_path));
    }
    // Wipe any half-generated state from a prior failed run. Both
    // unlinks are best-effort because the absent case is normal.
    let _ = fs::remove_file(&priv_path);
    let _ = fs::remove_file(&pub_path);

    let comment = state_dir
        .file_name()
        .and_then(|s| s.to_str())
        .map(|s| format!("dynrunner-{s}"))
        .unwrap_or_else(|| "dynrunner".to_owned());

    let out = Command::new("ssh-keygen")
        .args(["-t", "ed25519", "-N", "", "-C", &comment, "-f"])
        .arg(&priv_path)
        .output()
        .map_err(IdentityError::KeygenSpawn)?;
    if !out.status.success() {
        return Err(IdentityError::KeygenFailed {
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        });
    }

    // mode 0600 on the private key. ssh-keygen already does this on
    // most systems but pin it explicitly so the contract holds even
    // on filesystems with permissive umasks (e.g. NFS mounts).
    set_mode_0600(&priv_path)?;

    Ok((priv_path, pub_path))
}

/// Arguments for [`write_ssh_config`].
///
/// `host_alias` and `host_name` are kept as separate fields on
/// purpose: the alias is the SSH `Host` block label *and* the URL
/// host the dispatcher passes downstream (e.g. into worker
/// `--secondary tcp://<alias>:<port>` URLs); the host_name is the
/// actual DNS or IP the SSH client resolves. For cluster setups
/// where workers must reach the gateway by a network-alias that
/// doesn't resolve from the operator's host (e.g. `slurm-gateway`),
/// `host_alias = "slurm-gateway"` and `host_name = "localhost"` is
/// the canonical configuration. Bundling them into a single field
/// would force every caller to learn that distinction at the API
/// boundary.
#[derive(Debug, Clone)]
pub struct WriteSshConfigArgs {
    /// Directory the file is written under (`<state_dir>/ssh_config`).
    pub state_dir: PathBuf,
    /// `Host <alias>` line — also the URL host downstream.
    pub host_alias: String,
    /// `HostName <name>` line — what SSH actually resolves.
    pub host_name: String,
    /// `Port <port>`.
    pub ssh_port: u16,
    /// `User <user>`.
    pub user: String,
    /// `IdentityFile <path>`.
    pub identity_file: PathBuf,
}

/// Write a per-cluster ssh_config and return its absolute path.
///
/// Pins the framework's hard-rule defaults verbatim:
/// - `IdentitiesOnly yes` + `IdentityAgent none` (no agent
///   consultation — the framework's anti-leak posture against
///   `MaxAuthTries` exhaustion on multi-key agents)
/// - `StrictHostKeyChecking no` + `UserKnownHostsFile /dev/null`
///   (no host-key reuse across instances — every cluster instance
///   regenerates its host keys)
/// - `ServerAliveInterval 30 / ServerAliveCountMax 3 /
///   TCPKeepAlive yes / ConnectTimeout 10` (operator-side
///   keepalive; the master-spawn pin of 60×1080 is the
///   independent floor for framework-spawned masters)
///
/// The file is written with mode 0600.
pub fn write_ssh_config(args: &WriteSshConfigArgs) -> Result<PathBuf, IdentityError> {
    fs::create_dir_all(&args.state_dir)?;
    let cfg_path = args.state_dir.join("ssh_config");
    let identity_str = args.identity_file.to_string_lossy();
    let body = format!(
        "# Auto-generated by dynrunner-driver::identity::write_ssh_config — do not edit.\n\
         # Regenerated on every call.\n\
         \n\
         Host {alias}\n    \
            HostName {host}\n    \
            Port {port}\n    \
            User {user}\n    \
            IdentityFile {identity}\n    \
            IdentitiesOnly yes\n    \
            IdentityAgent none\n    \
            StrictHostKeyChecking no\n    \
            UserKnownHostsFile /dev/null\n    \
            ServerAliveInterval 30\n    \
            ServerAliveCountMax 3\n    \
            TCPKeepAlive yes\n    \
            ConnectTimeout 10\n",
        alias = args.host_alias,
        host = args.host_name,
        port = args.ssh_port,
        user = args.user,
        identity = identity_str,
    );
    fs::write(&cfg_path, body)?;
    set_mode_0600(&cfg_path)?;
    Ok(cfg_path)
}

#[cfg(unix)]
fn set_mode_0600(p: &Path) -> Result<(), io::Error> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(p, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_mode_0600(_p: &Path) -> Result<(), io::Error> {
    // No-op on non-Unix; the framework only targets Unix today.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_ssh_config_emits_pinned_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let args = WriteSshConfigArgs {
            state_dir: dir.path().to_path_buf(),
            host_alias: "slurm-gateway".into(),
            host_name: "localhost".into(),
            ssh_port: 2200,
            user: "alice".into(),
            identity_file: PathBuf::from("/keys/id_ed25519"),
        };
        let p = write_ssh_config(&args).unwrap();
        let content = fs::read_to_string(p).unwrap();
        for needle in [
            "Host slurm-gateway",
            "HostName localhost",
            "Port 2200",
            "User alice",
            "IdentityFile /keys/id_ed25519",
            "IdentitiesOnly yes",
            "IdentityAgent none",
            "StrictHostKeyChecking no",
            "UserKnownHostsFile /dev/null",
        ] {
            assert!(
                content.contains(needle),
                "missing pinned default {needle:?} in:\n{content}"
            );
        }
    }
}
