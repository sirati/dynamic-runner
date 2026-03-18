/// Configuration for SSH connections.
#[derive(Debug, Clone)]
pub struct SshConfig {
    pub host: String,
    pub port: u16,
    pub user: Option<String>,
}

/// Gateway configuration.
#[derive(Debug, Clone)]
pub enum GatewayConfig {
    Local,
    Ssh(SshConfig),
}

/// Parse a gateway URL into a `GatewayConfig`.
///
/// Supported formats:
/// - `local`
/// - `ssh://host`
/// - `ssh://user@host`
/// - `ssh://user@host:port`
pub fn parse_gateway_url(url: &str) -> Result<GatewayConfig, String> {
    if url == "local" {
        return Ok(GatewayConfig::Local);
    }

    if let Some(rest) = url.strip_prefix("ssh://") {
        let (user, host_port) = if let Some(at_pos) = rest.find('@') {
            (Some(rest[..at_pos].to_string()), &rest[at_pos + 1..])
        } else {
            (None, rest)
        };

        let (host, port) = if let Some(colon_pos) = host_port.rfind(':') {
            let port_str = &host_port[colon_pos + 1..];
            let port = port_str
                .parse::<u16>()
                .map_err(|_| format!("invalid port: {port_str}"))?;
            (host_port[..colon_pos].to_string(), port)
        } else {
            (host_port.to_string(), 22)
        };

        return Ok(GatewayConfig::Ssh(SshConfig { host, port, user }));
    }

    Err(format!(
        "invalid gateway URL: {url}. Use 'local' or 'ssh://[user@]host[:port]'"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_local() {
        let config = parse_gateway_url("local").unwrap();
        assert!(matches!(config, GatewayConfig::Local));
    }

    #[test]
    fn parse_ssh_host_only() {
        let config = parse_gateway_url("ssh://myhost").unwrap();
        if let GatewayConfig::Ssh(ssh) = config {
            assert_eq!(ssh.host, "myhost");
            assert_eq!(ssh.port, 22);
            assert!(ssh.user.is_none());
        } else {
            panic!("expected Ssh config");
        }
    }

    #[test]
    fn parse_ssh_user_host_port() {
        let config = parse_gateway_url("ssh://admin@gateway.example.com:2222").unwrap();
        if let GatewayConfig::Ssh(ssh) = config {
            assert_eq!(ssh.host, "gateway.example.com");
            assert_eq!(ssh.port, 2222);
            assert_eq!(ssh.user.as_deref(), Some("admin"));
        } else {
            panic!("expected Ssh config");
        }
    }

    #[test]
    fn parse_invalid() {
        assert!(parse_gateway_url("ftp://host").is_err());
    }
}
