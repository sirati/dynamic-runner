//! [`SshTarget`] — newtype around the `user@host[:port]` string used
//! by ssh subprocesses.
//!
//! Single concern: keep the operator-visible target identifier
//! distinct from the [`crate::SshConfig`] (which carries identity
//! files and config-file paths). Per peer guidance for the locked
//! design point (a-bis), error payloads must surface the target
//! string for telemetry but must NEVER leak identity-file paths or
//! agent-socket paths. Wrapping the user@host string in a newtype
//! enforces that at the type level: `Display` impls and `Debug`
//! output for [`crate::SshMasterError`] only know about the
//! [`SshTarget`], not the full config.
//!
//! Out-of-scope (kept simple):
//! - parsing `ssh://` URLs (that's `parse_gateway_url` in the
//!   `dynrunner-gateway` crate)
//! - bracketing IPv6 hosts (callers pre-bracket; ssh handles it)
//! - storing port separately from host (ssh accepts `host:port` in
//!   some forms, but our spawn path uses `-p <port>` separately —
//!   the stored string is the value passed as the *target* arg to
//!   ssh, never with an embedded port)

use std::fmt;

/// A `user@host` (or bare `host`) string suitable for use as the
/// final positional argument to `ssh` / `scp` / `ssh -O check`.
///
/// Construction is intentionally simple: any string the caller
/// considers a valid ssh target is accepted. We don't pre-parse or
/// validate — the rejection layer is `ssh` itself, which surfaces
/// the parse failure as [`crate::SshMasterError::HandshakeRefused`]
/// or [`crate::SshMasterError::SpawnFailed`].
///
/// `Display` strips no quoting / escaping; the value is intended for
/// telemetry and for direct `Command::arg(target.as_str())` use,
/// where the OS does the argv hand-off without shell interpolation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SshTarget(String);

impl SshTarget {
    /// Construct from any string-like value. No validation — the
    /// caller is responsible for producing a value `ssh` will accept.
    pub fn new(target: impl Into<String>) -> Self {
        Self(target.into())
    }

    /// Build from optional user + host. Mirrors the small helper
    /// used by `dynrunner-gateway::ssh::SshGateway::ssh_target`.
    pub fn from_user_host(user: Option<&str>, host: &str) -> Self {
        match user {
            Some(u) => Self(format!("{u}@{host}")),
            None => Self(host.to_owned()),
        }
    }

    /// Borrow the underlying string. For passing to
    /// `Command::arg(target.as_str())`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SshTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for SshTarget {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for SshTarget {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_user_host_threads_user() {
        let t = SshTarget::from_user_host(Some("alice"), "host.example");
        assert_eq!(t.as_str(), "alice@host.example");
        assert_eq!(format!("{t}"), "alice@host.example");
    }

    #[test]
    fn from_user_host_drops_user_when_absent() {
        let t = SshTarget::from_user_host(None, "host.example");
        assert_eq!(t.as_str(), "host.example");
    }

    #[test]
    fn debug_does_not_leak_anything_else() {
        // The whole point of the newtype is that Debug formatting
        // yields a tuple containing the string and nothing else —
        // no chance of an identity-file path slipping in.
        let t = SshTarget::new("user@host");
        let s = format!("{t:?}");
        assert!(s.contains("\"user@host\""));
        assert!(!s.contains("identity"));
        assert!(!s.contains("config"));
    }
}
