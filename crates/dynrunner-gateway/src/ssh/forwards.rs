//! Master-side port forwards over the control socket: `ssh -O
//! forward` / `ssh -O cancel`.
//!
//! ONE concern: register/unregister a `-L`/`-R` forward ON the
//! running ControlMaster daemon without opening an sshd SESSION.
//! This is the only safe way for framework subprocesses to add
//! forwards to a shared master: a regular `ssh -N -L` mux client
//! requests a real session from the master (sshd runs the login
//! shell, the null stdin EOFs it, the client exits with the shell's
//! status within milliseconds while the forward lives on) — so a
//! cohort of N such clients burns N `MaxSessions` slots for nothing
//! and defeats any child-lifetime bookkeeping. `-O forward` talks
//! only to the control socket: no session, no `MaxSessions`
//! exposure, an exit code that truthfully reports the registration
//! (rc!=0 + stderr on e.g. a local bind collision), and a matching
//! `-O cancel` for teardown/same-port rebuilds.
//!
//! Used by:
//! - `connect_disconnect.rs` — the adopted-master path adds/undoes
//!   its registered `-R` reverse forwards here;
//! - `dynrunner-slurm`'s local-forward registry — each late-joiner
//!   `-L` leg registers on the gateway master here when the master
//!   is alive (direct `ssh -N -L` dial otherwise).

use tokio::process::Command;

use crate::config::SshConfig;
use crate::traits::GatewayError;

use super::{base_ssh_args_for, ssh_target_for};

/// One master-side forward, in OpenSSH `-L`/`-R` spec terms.
///
/// `spec()` renders the canonical 4-part form
/// `<bind_addr>:<bind_port>:<dest_host>:<dest_port>`; open and
/// cancel must use the IDENTICAL spec (OpenSSH matches cancels
/// against the registered spec string).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MasterForward {
    /// `-L <bind_addr>:<bind_port>:<dest_host>:<dest_port>` — the
    /// master listens locally and carries connections to the dest.
    Local {
        bind_addr: String,
        bind_port: u16,
        dest_host: String,
        dest_port: u16,
    },
    /// `-R <bind_addr>:<bind_port>:<dest_host>:<dest_port>` — the
    /// gateway sshd listens remotely and carries connections back.
    Remote {
        bind_addr: String,
        bind_port: u16,
        dest_host: String,
        dest_port: u16,
    },
}

impl MasterForward {
    fn flag(&self) -> &'static str {
        match self {
            MasterForward::Local { .. } => "-L",
            MasterForward::Remote { .. } => "-R",
        }
    }

    /// The OpenSSH forward spec string (identical shape for `-L`
    /// and `-R`).
    pub fn spec(&self) -> String {
        let (MasterForward::Local {
            bind_addr,
            bind_port,
            dest_host,
            dest_port,
        }
        | MasterForward::Remote {
            bind_addr,
            bind_port,
            dest_host,
            dest_port,
        }) = self;
        format!("{bind_addr}:{bind_port}:{dest_host}:{dest_port}")
    }
}

/// Argv (excluding `ssh` itself) for one `-O forward`/`-O cancel`
/// control operation. Pure so the shape is unit-testable.
fn control_forward_argv(
    op: &str,
    control_path: &str,
    config: &SshConfig,
    forward: &MasterForward,
) -> Vec<String> {
    let mut argv = base_ssh_args_for(config);
    argv.extend([
        "-O".into(),
        op.into(),
        "-o".into(),
        format!("ControlPath={control_path}"),
        forward.flag().into(),
        forward.spec(),
    ]);
    argv.push(ssh_target_for(config));
    argv
}

async fn run_control_forward_op(
    op: &str,
    control_path: &str,
    config: &SshConfig,
    forward: &MasterForward,
) -> Result<(), GatewayError> {
    let argv = control_forward_argv(op, control_path, config, forward);
    let mut cmd = Command::new("ssh");
    cmd.args(&argv);
    let output = cmd
        .output()
        .await
        .map_err(|e| GatewayError::CommandFailed(format!("ssh -O {op} spawn: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(GatewayError::CommandFailed(format!(
            "ssh -O {op} {} {} failed (rc={}): {}",
            forward.flag(),
            forward.spec(),
            output.status.code().unwrap_or(-1),
            stderr.trim()
        )));
    }
    Ok(())
}

/// Register `forward` on the master behind `control_path`.
///
/// rc!=0 (e.g. the local bind is already taken, or the master is
/// gone) surfaces as a [`GatewayError::CommandFailed`] carrying
/// ssh's stderr; the caller owns the fallback policy (the
/// local-forward registry degrades to a direct dial).
pub async fn master_forward_open(
    control_path: &str,
    config: &SshConfig,
    forward: &MasterForward,
) -> Result<(), GatewayError> {
    run_control_forward_op("forward", control_path, config, forward).await
}

/// Unregister `forward` from the master behind `control_path` —
/// the teardown / same-port-rebuild counterpart of
/// [`master_forward_open`]. The spec must match the open verbatim.
pub async fn master_forward_cancel(
    control_path: &str,
    config: &SshConfig,
    forward: &MasterForward,
) -> Result<(), GatewayError> {
    run_control_forward_op("cancel", control_path, config, forward).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> SshConfig {
        SshConfig {
            host: "gw.example.org".into(),
            port: 2222,
            user: Some("alice".into()),
            identity_file: None,
            config_file: None,
        }
    }

    /// The `-L` spec must match what the slurm local-forward path
    /// registers: 127.0.0.1-bound local port to the compute target.
    #[test]
    fn local_forward_argv_shape() {
        let fwd = MasterForward::Local {
            bind_addr: "127.0.0.1".into(),
            bind_port: 15001,
            dest_host: "compute7".into(),
            dest_port: 51200,
        };
        let argv = control_forward_argv("forward", "/tmp/m.sock", &cfg(), &fwd);
        let o = argv.iter().position(|a| a == "-O").expect("-O");
        assert_eq!(argv[o + 1], "forward");
        assert!(
            argv.contains(&"ControlPath=/tmp/m.sock".to_string()),
            "{argv:?}"
        );
        let l = argv.iter().position(|a| a == "-L").expect("-L");
        assert_eq!(argv[l + 1], "127.0.0.1:15001:compute7:51200");
        assert_eq!(argv.last().unwrap(), "alice@gw.example.org");
        // Non-default port rides along (control ops still match on it).
        let p = argv.iter().position(|a| a == "-p").expect("-p");
        assert_eq!(argv[p + 1], "2222");
    }

    /// The `-R` spec must be byte-identical to what the adopted-master
    /// connect path used to build inline ("0.0.0.0:<remote>:localhost:
    /// <local>") — cancels match the registered string verbatim.
    #[test]
    fn remote_forward_spec_matches_legacy_inline_shape() {
        let fwd = MasterForward::Remote {
            bind_addr: "0.0.0.0".into(),
            bind_port: 51200,
            dest_host: "localhost".into(),
            dest_port: 8080,
        };
        assert_eq!(fwd.spec(), "0.0.0.0:51200:localhost:8080");
        let argv = control_forward_argv("cancel", "/tmp/m.sock", &cfg(), &fwd);
        let o = argv.iter().position(|a| a == "-O").expect("-O");
        assert_eq!(argv[o + 1], "cancel");
        let r = argv.iter().position(|a| a == "-R").expect("-R");
        assert_eq!(argv[r + 1], "0.0.0.0:51200:localhost:8080");
    }

    /// Open against a dead/absent socket is a loud typed error (the
    /// fallback trigger for the local-forward registry).
    #[tokio::test]
    async fn open_on_dead_socket_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dead = dir.path().join("no-master.sock");
        let fwd = MasterForward::Local {
            bind_addr: "127.0.0.1".into(),
            bind_port: 1,
            dest_host: "h".into(),
            dest_port: 2,
        };
        let err = master_forward_open(&dead.to_string_lossy(), &cfg(), &fwd)
            .await
            .expect_err("dead socket must error");
        assert!(matches!(err, GatewayError::CommandFailed(_)), "{err:?}");
    }
}
