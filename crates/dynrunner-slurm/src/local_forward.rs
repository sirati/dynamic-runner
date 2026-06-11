//! Per-peer `ssh -L` local-forward tunnels through the gateway — the
//! desktop-side counterpart of the submitter's `ssh -R` reverse-tunnel
//! machinery in [`crate::preparation`].
//!
//! # Concern
//!
//! ONE concern: give a host that can reach ONLY the gateway (the
//! operator's local desktop running `--observer-join-from-peer-info-dir
//! <dir> --gateway <host>`) a dialable `127.0.0.1:<local_port>`
//! endpoint per cluster peer. Each peer's compute-internal
//! `host:quic_port` (from its gateway-side `.info` record) is carried
//! by one `ssh -N -L 127.0.0.1:<local>:<host>:<port> <gateway>` child.
//! The caller substitutes the local endpoints into its seed entries
//! (the dispatcher's seed-builder seam) — this module never sees the
//! wire types.
//!
//! A TCP forward cannot carry QUIC (UDP); the seed-side rewrite
//! declares the substituted endpoints WSS-only (cert cleared), which is
//! the production shape anyway (the wrapper omits `cert_pem_b64`).
//!
//! # Reuse (no parallel-build)
//!
//! The per-tunnel lifecycle policy is the `-R` path's, reused verbatim
//! through the crate-internal seam in [`crate::preparation`]:
//! [`verify_tunnel_alive`] (3s alive-gate + `ExitOnForwardFailure`),
//! [`terminate_child`] (SIGTERM → 5s → SIGKILL), and
//! [`ReconnectEscalation`] (#342's half-dead override: K consecutive
//! alive-noop reconnect ticks force a rebuild past the liveness gate).
//! The auth flag chain comes from [`dynrunner_gateway::auth_options_for`]
//! — the same single source of truth the gateway master uses.
//!
//! # Rebuilds keep the local port
//!
//! A rebuild (observer lost-visibility reconnect) re-spawns the `ssh
//! -L` on the SAME local port: the port is baked into the mesh's
//! authoritative dial info at seed time, and the transport's 5s
//! reconnect ticker redials exactly that endpoint — a fresh port would
//! orphan the dial list with no re-coordination path (the mirror image
//! of the `-R` path's same-port rebind rule from #334). The local side
//! owns the dead child, so freeing the port is a [`terminate_child`],
//! never a remote `fuser -k`.
//!
//! # Host-key policy
//!
//! The tunnel targets the operator's OWN gateway — the same host the
//! [`dynrunner_gateway::SshGateway`] ControlMaster connects to with the
//! user's normal known_hosts — so no `StrictHostKeyChecking=no`
//! override is added (unlike the `-R` path, whose targets are ephemeral
//! compute nodes with regenerating keys).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use dynrunner_gateway::{SshConfig, auth_options_for, shell_join};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use crate::preparation::{
    EscalationVerdict, PrepError, ReconnectEscalation, terminate_child, verify_tunnel_alive,
};

/// One peer's forward target, as harvested from its gateway-side
/// `.info` record: the compute host (the record's legacy-URI host —
/// the same name the `-R` path sshes to, resolvable on the gateway)
/// and the peer's mesh port (`quic_port`; WSS/TCP shares the number).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardTarget {
    pub peer_id: String,
    pub host: String,
    pub port: u16,
}

/// Error from the local-forward registry. Every variant names the
/// exact failing step + peer (silent-branch rule).
#[derive(Debug, thiserror::Error)]
pub enum LocalForwardError {
    #[error("failed to reserve a local port for the forward to `{peer_id}`: {source}")]
    LocalPortBind {
        peer_id: String,
        #[source]
        source: std::io::Error,
    },
    #[error("ssh -L spawn for `{peer_id}` failed: {source}")]
    Spawn {
        peer_id: String,
        #[source]
        source: std::io::Error,
    },
    /// The ssh child exited within the 3s alive-gate —
    /// `ExitOnForwardFailure=yes` makes a failed local bind or a
    /// refused gateway surface here, with ssh's stderr carried along.
    #[error(
        "ssh -L forward for `{peer_id}` failed to establish (rc={rc:?}): {stderr} — \
         check that the gateway is reachable and the local port is free"
    )]
    Establish {
        peer_id: String,
        rc: Option<i32>,
        stderr: String,
    },
    /// Not one of the requested forwards came up — the late-joiner
    /// would have zero dialable endpoints, so the bootstrap must not
    /// proceed into `join_running_cluster`'s connect window.
    #[error(
        "none of the {total} local-forward tunnels could be established — \
         the late-joiner has no dialable endpoint; each per-peer failure \
         was logged above with its cause (unreachable gateway, dead \
         record, local bind failure)"
    )]
    NoneEstablished { total: usize },
    /// A reconnect was requested for a peer this registry never built
    /// a tunnel for — it joined the cluster after the bootstrap fetch.
    #[error(
        "no local-forward tunnel exists for peer `{peer_id}` (it joined after \
         this observer's bootstrap fetch; mid-run tunnel discovery for new \
         peers is not wired — restart the late-joiner to pick it up)"
    )]
    UnknownPeer { peer_id: String },
}

/// Outcome of one [`LocalForwardTunnels::reconnect_one`] call, for the
/// caller's narration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectOutcome {
    /// The tunnel child is alive and the escalation tolerated the tick
    /// — no rebuild (the liveness gate, mirroring the `-R` path).
    AliveNoop { streak: u32 },
    /// The tunnel was rebuilt (dead child, or the half-dead escalation
    /// fired) on the SAME local port.
    Rebuilt { local_port: u16 },
}

/// The spawn future a [`ForwardSpawner`] produces. `!Send` is fine —
/// the registry lives on the observer's `LocalSet` (the same provider
/// physics as the `-R` path's spawner seam).
type SpawnFuture = Pin<Box<dyn Future<Output = Result<Child, std::io::Error>>>>;

/// DI seam mirroring `preparation`'s spawner closures: tests inject a
/// fake child factory; production spawns the real `ssh -N -L`.
/// Args: `(peer_id, local_port, target_host, target_port)`.
type ForwardSpawner = Box<dyn Fn(&str, u16, &str, u16) -> SpawnFuture + Send + Sync>;

/// Build the argv for `ssh -N -L 127.0.0.1:<local>:<host>:<port>
/// <gateway>`. Pure (no I/O) so the shape is unit-testable.
///
/// The keepalive floor matches the gateway master's
/// (`ServerAliveInterval=60 × CountMax=1080 = 18h`) and the `-R`
/// tunnels' — a long observation must not lose its forwards to a
/// transient stall. `ExitOnForwardFailure=yes` makes a failed local
/// bind kill the child inside the 3s alive-gate instead of leaving a
/// forwardless ssh lingering.
pub(crate) fn build_local_forward_argv(
    local_port: u16,
    target_host: &str,
    target_port: u16,
    gateway: &SshConfig,
) -> Vec<String> {
    let mut argv: Vec<String> = vec!["ssh".into()];
    argv.extend(auth_options_for(gateway));
    if gateway.port != 22 {
        argv.push("-p".into());
        argv.push(gateway.port.to_string());
    }
    argv.push("-L".into());
    argv.push(format!(
        "127.0.0.1:{local_port}:{target_host}:{target_port}"
    ));
    let target = match &gateway.user {
        Some(user) => format!("{user}@{}", gateway.host),
        None => gateway.host.clone(),
    };
    argv.push(target);
    argv.extend([
        "-N".into(),
        "-o".into(),
        "ExitOnForwardFailure=yes".into(),
        "-o".into(),
        "ServerAliveInterval=60".into(),
        "-o".into(),
        "ServerAliveCountMax=1080".into(),
        "-o".into(),
        "TCPKeepAlive=yes".into(),
        "-o".into(),
        "LogLevel=ERROR".into(),
    ]);
    argv
}

/// The production spawner: spawn the `ssh -N -L` child for one forward.
fn production_spawner(gateway: SshConfig) -> ForwardSpawner {
    Box::new(move |peer_id, local_port, target_host, target_port| {
        let argv = build_local_forward_argv(local_port, target_host, target_port, &gateway);
        let peer_id = peer_id.to_owned();
        Box::pin(async move {
            tracing::info!(
                peer_id,
                local_port,
                target = %format!("{}:{}", argv_target(&argv), local_port),
                "creating SSH local-forward tunnel through the gateway"
            );
            tracing::debug!(peer_id, cmd = %shell_join(&argv), "ssh -L argv");
            let mut cmd = Command::new(&argv[0]);
            cmd.args(&argv[1..]);
            cmd.stdin(std::process::Stdio::null());
            cmd.stdout(std::process::Stdio::piped());
            cmd.stderr(std::process::Stdio::piped());
            // Layered reaping: `kill_on_drop` covers the orderly drop
            // (registry teardown / displaced rebuild), but a SIGKILL or
            // unhandled SIGTERM of THIS process runs no `Drop` — so the
            // kernel parent-death-signal is what keeps a signalled
            // late-joiner from orphaning its `ssh -L` legs (#425).
            cmd.kill_on_drop(true);
            crate::child_reaping::link_child_death_to_parent(&mut cmd);
            cmd.spawn()
        })
    })
}

/// The gateway target element of a built argv, for the spawn log line.
/// (The target is the last element before the trailing `-N -o ...`
/// options block — recompute it cheaply instead of threading it.)
fn argv_target(argv: &[String]) -> &str {
    argv.iter()
        .position(|a| a == "-N")
        .and_then(|i| i.checked_sub(1))
        .map(|i| argv[i].as_str())
        .unwrap_or("<gateway>")
}

struct ForwardEntry {
    local_port: u16,
    host: String,
    port: u16,
    child: Child,
}

struct Inner {
    entries: HashMap<String, ForwardEntry>,
    escalation: ReconnectEscalation,
}

/// The per-peer local-forward registry: owns every `ssh -L` child from
/// establishment to teardown, keyed by peer id (the `-L` mirror of the
/// `-R` path's `PerSecondaryTunnelRegistry`).
pub struct LocalForwardTunnels {
    spawner: ForwardSpawner,
    inner: Mutex<Inner>,
}

impl LocalForwardTunnels {
    /// Production registry over the given gateway credentials (the
    /// same `SshConfig` the connected [`dynrunner_gateway::SshGateway`]
    /// holds).
    pub fn new(gateway: SshConfig) -> Self {
        Self::with_spawner(production_spawner(gateway), ReconnectEscalation::default())
    }

    /// DI constructor for tests: inject the child factory and the
    /// escalation thresholds.
    pub(crate) fn with_spawner(spawner: ForwardSpawner, escalation: ReconnectEscalation) -> Self {
        Self {
            spawner,
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                escalation,
            }),
        }
    }

    /// Establish one forward per target, sequentially (one gateway
    /// handshake at a time — the same rate-friendliness rationale as
    /// the `-R` path's establishment limiter). Per-target failures are
    /// WARNed and tolerated — `join_running_cluster` fans to every
    /// seed, so one dead record must not brick the bootstrap — but
    /// ZERO successes is a hard, loud error.
    ///
    /// Returns the `peer_id → local_port` endpoint map the caller
    /// substitutes into its seed entries.
    pub async fn establish(
        &self,
        targets: &[ForwardTarget],
    ) -> Result<HashMap<String, u16>, LocalForwardError> {
        let mut endpoints = HashMap::new();
        for t in targets {
            match self.establish_one(t).await {
                Ok(local_port) => {
                    endpoints.insert(t.peer_id.clone(), local_port);
                }
                Err(e) => {
                    tracing::warn!(
                        peer_id = %t.peer_id,
                        target = %format!("{}:{}", t.host, t.port),
                        error = %e,
                        "local-forward tunnel establishment failed; this peer \
                         will not be dialable from this host"
                    );
                }
            }
        }
        if endpoints.is_empty() {
            return Err(LocalForwardError::NoneEstablished {
                total: targets.len(),
            });
        }
        tracing::info!(
            established = endpoints.len(),
            requested = targets.len(),
            "local-forward tunnels up"
        );
        Ok(endpoints)
    }

    /// Establish ONE forward on a freshly-reserved local port and
    /// register it. Replaces (and reaps) any prior entry for the id.
    async fn establish_one(&self, target: &ForwardTarget) -> Result<u16, LocalForwardError> {
        let local_port = reserve_local_port(&target.peer_id)?;
        let child = self
            .spawn_and_gate(&target.peer_id, local_port, &target.host, target.port)
            .await?;
        let displaced = {
            let mut inner = self.inner.lock().await;
            inner.entries.insert(
                target.peer_id.clone(),
                ForwardEntry {
                    local_port,
                    host: target.host.clone(),
                    port: target.port,
                    child,
                },
            )
        };
        if let Some(mut old) = displaced {
            terminate_child(&mut old.child).await;
        }
        Ok(local_port)
    }

    /// Spawn + 3s alive-gate for one forward attempt (shared by the
    /// establish and rebuild paths so the two cannot drift on policy).
    async fn spawn_and_gate(
        &self,
        peer_id: &str,
        local_port: u16,
        host: &str,
        port: u16,
    ) -> Result<Child, LocalForwardError> {
        let mut child = (self.spawner)(peer_id, local_port, host, port)
            .await
            .map_err(|source| LocalForwardError::Spawn {
                peer_id: peer_id.to_owned(),
                source,
            })?;
        match verify_tunnel_alive(peer_id, &mut child).await {
            Ok(()) => Ok(child),
            Err(PrepError::TunnelFailed {
                secondary_id,
                rc,
                stderr,
            }) => Err(LocalForwardError::Establish {
                peer_id: secondary_id,
                rc,
                stderr,
            }),
            Err(e) => Err(LocalForwardError::Establish {
                peer_id: peer_id.to_owned(),
                rc: None,
                stderr: e.to_string(),
            }),
        }
    }

    /// The `peer_id → 127.0.0.1-local-port` endpoint map of every live
    /// registration (the seed-rewrite input).
    pub async fn endpoints(&self) -> HashMap<String, u16> {
        self.inner
            .lock()
            .await
            .entries
            .iter()
            .map(|(id, e)| (id.clone(), e.local_port))
            .collect()
    }

    /// Reconnect-path rebuild for one peer, with the SAME liveness
    /// gate and half-dead escalation as the `-R` path's
    /// `reestablish_one_tunnel`: an alive child is a no-op (the ~60s
    /// lost-visibility cadence must never rebuild its own healthy
    /// forward) until K consecutive alive-noop ticks prove it
    /// half-dead; a dead child is rebuilt immediately. Rebuilds keep
    /// the SAME local port (the mesh's dial info is immutable).
    pub async fn reconnect_one(
        &self,
        peer_id: &str,
    ) -> Result<ReconnectOutcome, LocalForwardError> {
        let mut inner = self.inner.lock().await;
        let alive = match inner.entries.get_mut(peer_id) {
            Some(entry) => matches!(entry.child.try_wait(), Ok(None)),
            None => {
                return Err(LocalForwardError::UnknownPeer {
                    peer_id: peer_id.to_owned(),
                });
            }
        };
        if alive {
            match inner.escalation.on_alive_noop(peer_id, Instant::now()) {
                EscalationVerdict::Tolerate { streak } => {
                    return Ok(ReconnectOutcome::AliveNoop { streak });
                }
                EscalationVerdict::ForceRebuild => {
                    tracing::warn!(
                        peer_id,
                        "local-forward child alive but visibility never recovered \
                         across the escalation window — presuming half-dead and \
                         force-rebuilding (mirror of #342)"
                    );
                }
            }
        }

        let (local_port, host, port) = {
            let entry = inner
                .entries
                .get(peer_id)
                .expect("entry checked present above");
            (entry.local_port, entry.host.clone(), entry.port)
        };
        // Reap the old child FIRST so the local port is free for the
        // same-port rebind (we own the process — the local mirror of
        // the `-R` path's remote `fuser -k` release).
        {
            let entry = inner
                .entries
                .get_mut(peer_id)
                .expect("entry checked present above");
            terminate_child(&mut entry.child).await;
        }
        let child = self
            .spawn_and_gate(peer_id, local_port, &host, port)
            .await?;
        inner
            .entries
            .get_mut(peer_id)
            .expect("entry checked present above")
            .child = child;
        inner.escalation.on_rebuilt(peer_id);
        Ok(ReconnectOutcome::Rebuilt { local_port })
    }

    /// Reap every tunnel child (SIGTERM → 5s → SIGKILL). Idempotent.
    /// `kill_on_drop` on the spawned commands backstops the paths that
    /// never reach this.
    pub async fn teardown(&self) {
        let drained: Vec<ForwardEntry> = {
            let mut inner = self.inner.lock().await;
            inner.entries.drain().map(|(_, e)| e).collect()
        };
        for mut entry in drained {
            terminate_child(&mut entry.child).await;
        }
    }
}

/// Reserve a free localhost TCP port by binding ephemeral and
/// releasing. The ssh child re-binds it microseconds later;
/// `ExitOnForwardFailure` + the 3s alive-gate catch the (rare) race
/// loss loudly.
fn reserve_local_port(peer_id: &str) -> Result<u16, LocalForwardError> {
    let listener =
        std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).map_err(|source| {
            LocalForwardError::LocalPortBind {
                peer_id: peer_id.to_owned(),
                source,
            }
        })?;
    let port = listener
        .local_addr()
        .map_err(|source| LocalForwardError::LocalPortBind {
            peer_id: peer_id.to_owned(),
            source,
        })?
        .port();
    drop(listener);
    Ok(port)
}

/// [`TunnelReconnector`] binding over the local-forward registry: the
/// observer's lost-visibility trigger hands over its roster ids; each
/// known id gets the gated rebuild, unknown ids (peers that joined
/// after the bootstrap fetch) are named loudly and skipped. Mirrors
/// [`crate::observer_reconnect::SlurmPreparationTunnelReconnector`].
pub struct LocalForwardTunnelReconnector {
    tunnels: Arc<LocalForwardTunnels>,
}

impl LocalForwardTunnelReconnector {
    pub fn new(tunnels: Arc<LocalForwardTunnels>) -> Self {
        Self { tunnels }
    }
}

#[async_trait::async_trait(?Send)]
impl dynrunner_manager_distributed::observer::TunnelReconnector for LocalForwardTunnelReconnector {
    async fn reconnect(&self, peer_ids: &[String]) {
        // Best-effort + idempotent, mirroring the `-R` binding: per-id
        // failures are logged, never propagated (the observer's ~60s
        // cadence retries), and an alive-noop is silent at info-level
        // cadence (the gate already narrated via the outcome).
        for peer_id in peer_ids {
            match self.tunnels.reconnect_one(peer_id).await {
                Ok(ReconnectOutcome::Rebuilt { local_port }) => {
                    tracing::info!(
                        peer_id,
                        local_port,
                        "observer rebuilt ssh -L local-forward tunnel"
                    );
                }
                Ok(ReconnectOutcome::AliveNoop { streak }) => {
                    tracing::debug!(
                        peer_id,
                        streak,
                        "local-forward child alive; tolerating (no rebuild)"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        peer_id,
                        error = %e,
                        "local-forward tunnel rebuild failed; will retry on the \
                         next lost-visibility cadence tick"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn ssh_config() -> SshConfig {
        SshConfig {
            host: "gw.example.org".into(),
            port: 22,
            user: Some("alice".into()),
            identity_file: None,
            config_file: None,
        }
    }

    /// A long-lived stand-in for a healthy ssh child.
    fn healthy_child() -> Child {
        let mut cmd = Command::new("sleep");
        cmd.arg("600");
        cmd.kill_on_drop(true);
        cmd.spawn().expect("spawn sleep")
    }

    /// A child that exits immediately, simulating
    /// `ExitOnForwardFailure` killing a failed forward inside the gate.
    fn dying_child() -> Child {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "echo 'remote port forwarding failed' >&2; exit 255"]);
        cmd.kill_on_drop(true);
        cmd.spawn().expect("spawn sh")
    }

    /// Spawner producing healthy children and counting invocations.
    fn counting_spawner(count: Arc<AtomicUsize>) -> ForwardSpawner {
        Box::new(move |_peer, _lport, _host, _port| {
            count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(healthy_child()) })
        })
    }

    #[test]
    fn argv_shape_default_port_and_user() {
        let argv = build_local_forward_argv(15001, "compute7", 51200, &ssh_config());
        assert_eq!(argv[0], "ssh");
        // No auth flags configured, default port 22: no -p.
        assert!(!argv.contains(&"-p".to_string()), "{argv:?}");
        let l = argv.iter().position(|a| a == "-L").expect("-L present");
        assert_eq!(argv[l + 1], "127.0.0.1:15001:compute7:51200");
        assert_eq!(argv[l + 2], "alice@gw.example.org");
        assert!(
            argv.contains(&"ExitOnForwardFailure=yes".to_string()),
            "{argv:?}"
        );
        assert!(argv.contains(&"-N".to_string()), "{argv:?}");
        // Keepalive floor matches the master / -R tunnels.
        assert!(
            argv.contains(&"ServerAliveInterval=60".to_string()),
            "{argv:?}"
        );
        assert!(
            argv.contains(&"ServerAliveCountMax=1080".to_string()),
            "{argv:?}"
        );
    }

    #[test]
    fn argv_shape_custom_port_and_identity() {
        let gw = SshConfig {
            host: "gw".into(),
            port: 2222,
            user: None,
            identity_file: Some("/home/x/key".into()),
            config_file: Some("/home/x/cfg".into()),
        };
        let argv = build_local_forward_argv(1, "h", 2, &gw);
        // Auth chain is the shared single source of truth.
        let i = argv.iter().position(|a| a == "-i").expect("-i present");
        assert_eq!(argv[i + 1], "/home/x/key");
        assert!(argv.contains(&"IdentitiesOnly=yes".to_string()));
        assert!(argv.contains(&"IdentityAgent=none".to_string()));
        let f = argv.iter().position(|a| a == "-F").expect("-F present");
        assert_eq!(argv[f + 1], "/home/x/cfg");
        let p = argv.iter().position(|a| a == "-p").expect("-p present");
        assert_eq!(argv[p + 1], "2222");
        // No user: bare host target.
        let l = argv.iter().position(|a| a == "-L").unwrap();
        assert_eq!(argv[l + 2], "gw");
    }

    /// Mixed cohort: a dead record's forward fails (loud per-peer WARN,
    /// tolerated), the healthy one registers — and the endpoint map
    /// carries exactly the survivors.
    #[tokio::test]
    async fn establish_tolerates_per_peer_failure_keeps_survivors() {
        let spawner: ForwardSpawner = Box::new(|peer, _l, _h, _p| {
            let dead = peer == "secondary-dead";
            Box::pin(async move { Ok(if dead { dying_child() } else { healthy_child() }) })
        });
        let tunnels = LocalForwardTunnels::with_spawner(spawner, ReconnectEscalation::default());
        let targets = vec![
            ForwardTarget {
                peer_id: "secondary-dead".into(),
                host: "compute1".into(),
                port: 51200,
            },
            ForwardTarget {
                peer_id: "secondary-ok".into(),
                host: "compute2".into(),
                port: 51201,
            },
        ];
        let endpoints = tunnels.establish(&targets).await.expect("one survivor");
        assert_eq!(endpoints.len(), 1);
        assert!(endpoints.contains_key("secondary-ok"));
        assert_eq!(tunnels.endpoints().await, endpoints);
        tunnels.teardown().await;
    }

    /// Zero survivors is the hard loud error — the bootstrap must not
    /// proceed into the connect window with no dialable endpoint.
    #[tokio::test]
    async fn establish_all_failed_is_loud_error() {
        let spawner: ForwardSpawner =
            Box::new(|_peer, _l, _h, _p| Box::pin(async { Ok(dying_child()) }));
        let tunnels = LocalForwardTunnels::with_spawner(spawner, ReconnectEscalation::default());
        let targets = vec![ForwardTarget {
            peer_id: "secondary-0".into(),
            host: "compute1".into(),
            port: 51200,
        }];
        let err = tunnels.establish(&targets).await.expect_err("must fail");
        assert!(
            matches!(err, LocalForwardError::NoneEstablished { total: 1 }),
            "{err:?}"
        );
    }

    /// The reconnect gate: an alive child is tolerated (no respawn)
    /// until the escalation threshold, then force-rebuilt on the SAME
    /// local port; a reconnect for an unknown peer errors loudly.
    #[tokio::test]
    async fn reconnect_gates_alive_child_then_force_rebuilds_same_port() {
        let count = Arc::new(AtomicUsize::new(0));
        let tunnels = LocalForwardTunnels::with_spawner(
            counting_spawner(count.clone()),
            // force_after=2 keeps the test short; gap is irrelevant here.
            ReconnectEscalation::new(2, Duration::from_secs(300)),
        );
        let targets = vec![ForwardTarget {
            peer_id: "secondary-0".into(),
            host: "compute1".into(),
            port: 51200,
        }];
        let endpoints = tunnels.establish(&targets).await.expect("established");
        let original_port = endpoints["secondary-0"];
        assert_eq!(count.load(Ordering::SeqCst), 1);

        // Tick 1: alive → tolerated, no respawn.
        let outcome = tunnels.reconnect_one("secondary-0").await.expect("noop");
        assert_eq!(outcome, ReconnectOutcome::AliveNoop { streak: 1 });
        assert_eq!(count.load(Ordering::SeqCst), 1);

        // Tick 2: escalation fires → rebuild on the SAME port.
        let outcome = tunnels.reconnect_one("secondary-0").await.expect("rebuild");
        assert_eq!(
            outcome,
            ReconnectOutcome::Rebuilt {
                local_port: original_port
            }
        );
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(tunnels.endpoints().await["secondary-0"], original_port);

        // Unknown peer: loud typed error.
        let err = tunnels
            .reconnect_one("secondary-late")
            .await
            .expect_err("unknown");
        assert!(
            matches!(err, LocalForwardError::UnknownPeer { .. }),
            "{err:?}"
        );
        tunnels.teardown().await;
    }

    /// A DEAD child is rebuilt immediately (no escalation wait), on the
    /// same local port.
    #[tokio::test]
    async fn reconnect_rebuilds_dead_child_immediately() {
        let count = Arc::new(AtomicUsize::new(0));
        let tunnels = LocalForwardTunnels::with_spawner(
            counting_spawner(count.clone()),
            ReconnectEscalation::default(),
        );
        let targets = vec![ForwardTarget {
            peer_id: "secondary-0".into(),
            host: "compute1".into(),
            port: 51200,
        }];
        let endpoints = tunnels.establish(&targets).await.expect("established");
        let original_port = endpoints["secondary-0"];

        // Kill the child out-of-band (the ungraceful-drop shape).
        {
            let mut inner = tunnels.inner.lock().await;
            let entry = inner.entries.get_mut("secondary-0").unwrap();
            entry.child.start_kill().unwrap();
            let _ = entry.child.wait().await;
        }

        let outcome = tunnels.reconnect_one("secondary-0").await.expect("rebuild");
        assert_eq!(
            outcome,
            ReconnectOutcome::Rebuilt {
                local_port: original_port
            }
        );
        assert_eq!(count.load(Ordering::SeqCst), 2);
        tunnels.teardown().await;
    }

    /// `kill(pid, 0)` liveness probe: `true` iff the pid still exists.
    fn pid_alive(pid: u32) -> bool {
        use nix::errno::Errno;
        use nix::sys::signal::kill;
        use nix::unistd::Pid;
        !matches!(kill(Pid::from_raw(pid as i32), None), Err(Errno::ESRCH))
    }

    /// Poll until `pid` is gone or the grace window expires.
    async fn await_pid_gone(pid: u32) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while pid_alive(pid) {
            assert!(Instant::now() < deadline, "pid {pid} was not reaped in time");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// The explicit teardown chokepoint (layer (c)) reaps every tunnel
    /// child — the orderly success/error exit path the run loop drives.
    #[tokio::test]
    async fn teardown_reaps_every_child() {
        let count = Arc::new(AtomicUsize::new(0));
        let tunnels = LocalForwardTunnels::with_spawner(
            counting_spawner(count.clone()),
            ReconnectEscalation::default(),
        );
        let targets = vec![
            ForwardTarget {
                peer_id: "secondary-0".into(),
                host: "compute1".into(),
                port: 51200,
            },
            ForwardTarget {
                peer_id: "secondary-1".into(),
                host: "compute2".into(),
                port: 51201,
            },
        ];
        tunnels.establish(&targets).await.expect("established");
        let pids: Vec<u32> = {
            let inner = tunnels.inner.lock().await;
            inner
                .entries
                .values()
                .map(|e| e.child.id().expect("child pid"))
                .collect()
        };
        assert_eq!(pids.len(), 2);
        for &pid in &pids {
            assert!(pid_alive(pid), "child {pid} should be alive pre-teardown");
        }
        tunnels.teardown().await;
        for pid in pids {
            await_pid_gone(pid).await;
        }
    }

    /// The orderly-drop backstop (layer (b)): dropping the registry
    /// WITHOUT calling `teardown` still reaps every tunnel child via the
    /// production `kill_on_drop(true)` (here the `healthy_child` stand-in
    /// carries the same flag). This is the path a panic-unwind or an
    /// early scope-exit takes — distinct from the SIGKILL path (covered
    /// by `child_reaping`'s PDEATHSIG test), which runs no `Drop` at all.
    #[tokio::test]
    async fn dropping_registry_reaps_children_via_kill_on_drop() {
        let count = Arc::new(AtomicUsize::new(0));
        let tunnels = LocalForwardTunnels::with_spawner(
            counting_spawner(count.clone()),
            ReconnectEscalation::default(),
        );
        let targets = vec![ForwardTarget {
            peer_id: "secondary-0".into(),
            host: "compute1".into(),
            port: 51200,
        }];
        tunnels.establish(&targets).await.expect("established");
        let pid = {
            let inner = tunnels.inner.lock().await;
            inner.entries["secondary-0"]
                .child
                .id()
                .expect("child pid")
        };
        assert!(pid_alive(pid), "child {pid} should be alive before drop");
        // Drop the registry without teardown — `kill_on_drop` fires.
        drop(tunnels);
        await_pid_gone(pid).await;
    }
}
