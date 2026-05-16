//! [`EstablishmentPolicy`]: the rate-limiter, retry-budget, and
//! per-tunnel wall-clock cap for the SSH-reverse-tunnel establishment
//! phase. Defaults match the LMU CIP gateway load-balancer contract;
//! see the type docs for the full rationale.

use std::time::Duration;

/// Policy governing one tunnel's establishment phase — the window
/// between "info file shows up" and "ssh -R verified alive past the
/// 3s gate". Owns:
///
/// 1. **Concurrency cap** (`max_concurrent`): how many `ssh -N -R`
///    handshakes may be in flight at the same instant across all
///    watchers in a single `setup_ssh_tunnels` call. The coordinator
///    materialises this as a [`Semaphore`] shared across watchers;
///    each watcher acquires one permit just before `Command::spawn`
///    and releases it once the 3s verify gate has resolved (success
///    or failure).
///
///    Why: LMU's gateway is a load-balancer alias dispatching across
///    several physical hosts, each running OpenSSH with `MaxStartups`
///    typically defaulting to 10:30:100 (random-drop above 10 unauth'd
///    handshakes). With 15 secondaries all racing to handshake
///    simultaneously, ~1/15 ssh subprocesses intermittently sees
///    `Connection closed by ... rc=255` because the gateway sshd
///    randomly dropped the connection. Capping at 4 keeps the
///    in-flight handshake count well below the default budget while
///    still parallelising the bulk of setup.
///
/// 2. **Retry budget** (`attempts`, `backoff`): per-tunnel retry on
///    handshake-time failure. Only `PrepError::TunnelFailed` is
///    retried (info-parse / IO errors are non-recoverable). After
///    `attempts` total tries the last failure surfaces unchanged.
///    `backoff[i]` is the sleep BEFORE attempt `i+1` (so `backoff[0]`
///    sits between attempts 1 and 2).
///
/// 3. **Per-tunnel wall-clock cap** (`per_tunnel_timeout`): a single
///    establishment (across all retries + their inter-attempt sleeps)
///    is bounded by this deadline so a single chronically-failing
///    tunnel can't soak the whole 600s outer budget. Default 90s
///    matches the consumer requirement.
#[derive(Debug, Clone)]
pub struct EstablishmentPolicy {
    pub max_concurrent: usize,
    pub attempts: usize,
    /// Inter-attempt sleeps. `len() = attempts - 1` is the canonical
    /// shape; if shorter the policy reuses the last entry, if longer
    /// the tail is ignored. Empty disables retry waits.
    pub backoff: Vec<Duration>,
    pub per_tunnel_timeout: Duration,
}

impl EstablishmentPolicy {
    /// Backoff sleep BEFORE attempt index `attempt` (0-based). Returns
    /// `None` for `attempt == 0` (no pre-sleep on the first try).
    /// Indexing semantics: `attempt = i` sleeps `backoff[i - 1]`,
    /// clamping at the last element if `backoff` is shorter.
    pub fn backoff_before(&self, attempt: usize) -> Option<Duration> {
        if attempt == 0 {
            return None;
        }
        let idx = attempt - 1;
        self.backoff
            .get(idx)
            .or_else(|| self.backoff.last())
            .copied()
    }
}

impl Default for EstablishmentPolicy {
    fn default() -> Self {
        Self {
            max_concurrent: 4,
            attempts: 3,
            backoff: vec![Duration::from_secs(5), Duration::from_secs(15)],
            per_tunnel_timeout: Duration::from_secs(90),
        }
    }
}
