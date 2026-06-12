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
//! [`terminate_child`] (SIGTERM → 5s → SIGKILL) for direct legs, and
//! [`ReconnectEscalation`] (#342's half-dead override: K consecutive
//! alive-noop reconnect ticks force a rebuild past the liveness gate).
//! The auth flag chain comes from [`dynrunner_gateway::auth_options_for`]
//! — the same single source of truth the gateway master uses. (The
//! `-R` path's child-longevity `verify_tunnel_alive` gate is NOT
//! reused here: under mux, child lifetime says nothing about the
//! forward — see `# Multiplexing` below.)
//!
//! # Multiplexing over the gateway master
//!
//! The late-joiner already holds a connected
//! [`dynrunner_gateway::SshGateway`] ControlMaster when it builds this
//! registry, so each forward is REGISTERED ON the master via `ssh -O
//! forward -L …` ([`dynrunner_gateway::master_forward_open`]): one
//! real TCP+auth session total, near-instant per-forward
//! establishment, ZERO sshd session channels — which is what makes
//! the concurrent fan-out in [`LocalForwardTunnels::establish`] safe
//! against both `MaxStartups` (one connection) and `MaxSessions` (no
//! sessions). It must NOT be an `ssh -N -L` mux client: such a client
//! asks the master for a real session (sshd runs the login shell, the
//! null stdin EOFs it, the client exits with the shell's status in
//! milliseconds while the forward lives on, master-side) — so a
//! cohort of N clients burns N `MaxSessions` slots (default 10; the
//! 11th leg gets "Session open refused by peer") and reduces
//! child-lifetime gating to noise. That was the LMU 11/11 "failure":
//! every forward was alive on the master while the old child-longevity
//! gate declared it dead. The establishment gate is therefore the
//! FORWARD itself — `127.0.0.1:<local_port>` actually LISTENing
//! ([`gate_forward_listening`]) — never child longevity.
//!
//! The leg builder probes master liveness
//! ([`dynrunner_gateway::control_socket_alive`]) per establishment —
//! initial AND rebuild ride the same seam — and falls back to a
//! DIRECT `ssh -N -L` dial (WARN) when the master is absent, dead, or
//! refuses the registration: a joiner must never be hard-broken by a
//! missing master. Direct dials are real per-process gateway
//! connections, so their concurrency is bounded
//! ([`DIRECT_DIALS_IN_FLIGHT`]) below sshd's `MaxStartups` random-drop
//! threshold, and every direct argv pins
//! [`dynrunner_gateway::no_mux_options`] so an operator ssh_config
//! with `ControlMaster auto`/`ControlPersist yes` cannot silently
//! turn the dial back into a master handoff (instant rc=0 exit, the
//! same misread all over again). One failed attempt is retried once
//! through the full probe-and-build seam (a refused/raced leg must
//! re-resolve, not die).
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

use std::time::Duration;

use dynrunner_gateway::{
    MasterForward, SshConfig, auth_options_for, control_socket_alive, master_forward_cancel,
    master_forward_open, no_mux_options, shell_join,
};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, Semaphore};

use crate::preparation::{EscalationVerdict, ReconnectEscalation, terminate_child};

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

/// The establish future a [`LegEstablisher`] produces. `!Send` is fine
/// — the registry lives on the observer's `LocalSet` (the same
/// provider physics as the `-R` path's spawner seam).
type LegFuture = Pin<Box<dyn Future<Output = Result<ForwardLeg, LocalForwardError>>>>;

/// DI seam mirroring `preparation`'s spawner closures: tests inject a
/// fake leg factory; production resolves mux-vs-direct, dials, and
/// gates on the forward actually listening ([`LegBuilder`]).
/// Args: `(peer_id, local_port, target_host, target_port)`.
type LegEstablisher = Box<dyn Fn(&str, u16, &str, u16) -> LegFuture + Send + Sync>;

/// Build the argv for a DIRECT `ssh -N -L
/// 127.0.0.1:<local>:<host>:<port> <gateway>` dial. Pure (no I/O) so
/// the shape is unit-testable.
///
/// Every mux-relevant option is pinned OFF
/// ([`dynrunner_gateway::no_mux_options`]): an operator ssh_config
/// carrying `ControlMaster auto` + `ControlPersist yes` would
/// otherwise hand this "direct" dial off to an auto-spawned user
/// master and exit 0 within ~100ms — the forward alive on a master
/// the framework neither owns nor gates, the child-lifetime contract
/// (PDEATHSIG reaping, rebuild-by-respawn) silently voided.
///
/// The keepalive floor matches the gateway master's
/// (`ServerAliveInterval=60 × CountMax=1080 = 18h`) and the `-R`
/// tunnels' — a long observation must not lose its forwards to a
/// transient stall. `ExitOnForwardFailure=yes` makes a failed local
/// bind kill the child inside the establishment gate instead of
/// leaving a forwardless ssh lingering.
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
    argv.extend(no_mux_options().iter().map(|o| (*o).to_string()));
    argv
}

/// Establishment budget for a DIRECT dial: full TCP+auth handshake
/// plus the local bind (the `-R` path's historical 3s window).
const DIRECT_ESTABLISH_GATE: Duration = Duration::from_secs(3);

/// Establishment budget for a mux registration: the master binds the
/// local listener before `ssh -O forward` even returns, so this only
/// absorbs scheduler noise.
const MUX_ESTABLISH_GATE: Duration = Duration::from_secs(1);

/// Concurrent DIRECT dials in flight. Each is a real unauthenticated
/// gateway connection until its handshake completes, and sshd's
/// `MaxStartups` (default `10:30:100`) starts RANDOMLY dropping at 10
/// — a concurrent 11-peer cohort of direct dials would re-create the
/// thundering herd the mux path avoids. 4 keeps comfortable headroom
/// for the master itself plus unrelated operator activity while still
/// overlapping handshakes. Mux registrations don't take a slot (they
/// ride the ONE established master connection).
const DIRECT_DIALS_IN_FLIGHT: usize = 4;

/// `true` while something LISTENs on `127.0.0.1:<port>`.
///
/// Bind-probe: try to bind the port ourselves — `AddrInUse` means a
/// listener (the ssh child or the master daemon) holds it; success
/// means nothing does (the probe socket is dropped immediately). No
/// connection is ever made, so probing — establishment gate AND the
/// ~60s reconnect liveness cadence — never sends a spurious dial
/// through the tunnel to the peer. Residual: a FOREIGN process
/// grabbing the reserved port between reservation and ssh's bind
/// reads as "listening" (the same reservation race the `-R` path
/// documents); the leg's child then exits loudly and the reconnect
/// cadence rebuilds.
pub(crate) fn local_port_listening(port: u16) -> bool {
    match std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)) {
        Ok(_probe) => false,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => true,
        Err(e) => {
            tracing::warn!(
                port,
                error = %e,
                "local-forward listen probe failed unexpectedly; treating as not listening"
            );
            false
        }
    }
}

/// The establishment gate: the FORWARD is up iff
/// `127.0.0.1:<local_port>` is LISTENing within `budget` — child
/// longevity proves nothing (a mux registration has no child at all;
/// an unpinned mux client exits rc=0 in milliseconds with its forward
/// alive on the master — the LMU misread).
///
/// `child`: the direct-dial ssh, when there is one. Its EXIT before
/// the port listens is a hard failure (a pinned direct `ssh -N` owns
/// the listener, so child death == forward death) and its rc/stderr
/// carry the diagnosis (`ExitOnForwardFailure` makes refused binds
/// and refused gateways exit here). With no child, only the
/// port/deadline arms apply.
pub(crate) async fn gate_forward_listening(
    peer_id: &str,
    local_port: u16,
    mut child: Option<&mut Child>,
    budget: Duration,
) -> Result<(), LocalForwardError> {
    let deadline = Instant::now() + budget;
    loop {
        if local_port_listening(local_port) {
            tracing::info!(peer_id, local_port, "local forward listening");
            return Ok(());
        }
        if let Some(c) = child.as_deref_mut()
            && let Ok(Some(status)) = c.try_wait()
        {
            // Child gone, forward never came up: drain whatever the
            // ssh wrote before exiting (the write end is closed, so
            // this returns every buffered byte without racing).
            let mut stderr = String::new();
            if let Some(mut pipe) = c.stderr.take() {
                use tokio::io::AsyncReadExt;
                let mut buf = Vec::new();
                let _ = pipe.read_to_end(&mut buf).await;
                stderr = String::from_utf8_lossy(&buf).trim().to_owned();
            }
            return Err(LocalForwardError::Establish {
                peer_id: peer_id.to_owned(),
                rc: status.code(),
                stderr,
            });
        }
        if Instant::now() >= deadline {
            return Err(LocalForwardError::Establish {
                peer_id: peer_id.to_owned(),
                rc: None,
                stderr: format!(
                    "forward did not start listening on 127.0.0.1:{local_port} \
                     within {budget:?}"
                ),
            });
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// One established forward's transport, owned by the registry from
/// establishment to release.
pub(crate) enum ForwardLeg {
    /// Registered on the gateway ControlMaster via `ssh -O forward`
    /// — no process of ours carries it; the master holds the local
    /// listener. Released via the matching `ssh -O cancel` (and
    /// implicitly by master teardown).
    Mux {
        control_path: String,
        gateway: SshConfig,
        spec: MasterForward,
    },
    /// A direct `ssh -N -L` child of ours; the child owns the local
    /// listener, so child lifetime is forward lifetime.
    Direct { child: Child },
}

impl ForwardLeg {
    /// Leg liveness for the reconnect gate: a direct child must be
    /// running; a mux registration must still hold its listener (the
    /// master dying takes the LISTEN down with it).
    fn is_alive(&mut self, local_port: u16) -> bool {
        match self {
            ForwardLeg::Direct { child } => matches!(child.try_wait(), Ok(None)),
            ForwardLeg::Mux { .. } => local_port_listening(local_port),
        }
    }

    /// Free the leg's local port: reap the direct child (we own the
    /// process — the local mirror of the `-R` path's remote
    /// `fuser -k` release), or cancel the master-side registration.
    /// Best-effort on the mux side: a dead master already released
    /// the port, and the cancel failing must not block a rebuild.
    async fn release(&mut self, peer_id: &str) {
        match self {
            ForwardLeg::Direct { child } => terminate_child(child).await,
            ForwardLeg::Mux {
                control_path,
                gateway,
                spec,
            } => {
                if let Err(e) = master_forward_cancel(control_path, gateway, spec).await {
                    tracing::debug!(
                        peer_id,
                        error = %e,
                        "ssh -O cancel for local forward failed (master likely \
                         gone; its listener died with it)"
                    );
                }
            }
        }
    }
}

/// Per-establishment transport selection: multiplex over the gateway
/// master when its control socket answers `ssh -O check`; direct-dial
/// (WARN) when no master was handed over or it is absent/dead. Probed
/// at EVERY establishment — initial and the reconnect rebuild share
/// [`LocalForwardTunnels::establish_leg`], so a master that dies
/// mid-run degrades the next rebuild instead of breaking it.
async fn resolve_mux_control_path<'a>(
    control_path: Option<&'a str>,
    gateway: &SshConfig,
    peer_id: &str,
) -> Option<&'a str> {
    let cp = control_path?;
    if control_socket_alive(cp, gateway).await {
        Some(cp)
    } else {
        tracing::warn!(
            peer_id,
            control_path = %cp,
            "gateway ControlMaster socket is absent or dead; falling back \
             to a direct ssh dial for this forward"
        );
        None
    }
}

/// Run `work` holding one of the semaphore's slots — the in-flight
/// bound for direct dials. Tiny and free-standing so the bounding
/// itself is unit-testable.
async fn with_dial_slot<T>(slots: &Semaphore, work: impl Future<Output = T>) -> T {
    let _permit = slots
        .acquire()
        .await
        .expect("dial-slot semaphore is never closed");
    work.await
}

/// The production leg builder: resolve mux-vs-direct per
/// establishment, dial, and gate on the forward LISTENing.
struct LegBuilder {
    gateway: SshConfig,
    control_path: Option<String>,
    direct_dial_slots: Semaphore,
}

impl LegBuilder {
    fn new(gateway: SshConfig, control_path: Option<String>) -> Self {
        Self {
            gateway,
            control_path,
            direct_dial_slots: Semaphore::new(DIRECT_DIALS_IN_FLIGHT),
        }
    }

    async fn establish(
        &self,
        peer_id: &str,
        local_port: u16,
        target_host: &str,
        target_port: u16,
    ) -> Result<ForwardLeg, LocalForwardError> {
        if let Some(cp) =
            resolve_mux_control_path(self.control_path.as_deref(), &self.gateway, peer_id).await
        {
            match self
                .establish_mux(cp, peer_id, local_port, target_host, target_port)
                .await
            {
                Ok(leg) => return Ok(leg),
                Err(e) => {
                    tracing::warn!(
                        peer_id,
                        error = %e,
                        "mux forward registration on the gateway master failed; \
                         falling back to a direct ssh dial for this forward"
                    );
                }
            }
        }
        self.establish_direct(peer_id, local_port, target_host, target_port)
            .await
    }

    /// Register the forward ON the master (`ssh -O forward -L …`) —
    /// sessionless, so a whole cohort can fan out without touching
    /// sshd's `MaxSessions`/`MaxStartups` — then gate on the master's
    /// listener being up.
    async fn establish_mux(
        &self,
        control_path: &str,
        peer_id: &str,
        local_port: u16,
        target_host: &str,
        target_port: u16,
    ) -> Result<ForwardLeg, LocalForwardError> {
        let spec = MasterForward::Local {
            bind_addr: "127.0.0.1".into(),
            bind_port: local_port,
            dest_host: target_host.to_owned(),
            dest_port: target_port,
        };
        tracing::info!(
            peer_id,
            local = %format!("127.0.0.1:{local_port}"),
            dest = %format!("{target_host}:{target_port}"),
            via = %format!("mux:{control_path}"),
            "registering SSH local-forward on the gateway master"
        );
        master_forward_open(control_path, &self.gateway, &spec)
            .await
            .map_err(|e| LocalForwardError::Establish {
                peer_id: peer_id.to_owned(),
                rc: None,
                stderr: e.to_string(),
            })?;
        let leg = ForwardLeg::Mux {
            control_path: control_path.to_owned(),
            gateway: self.gateway.clone(),
            spec,
        };
        match gate_forward_listening(peer_id, local_port, None, MUX_ESTABLISH_GATE).await {
            Ok(()) => Ok(leg),
            Err(e) => {
                // Registration claimed success but no listener showed:
                // unregister so the master doesn't hold a half-leg.
                let mut leg = leg;
                leg.release(peer_id).await;
                Err(e)
            }
        }
    }

    /// Direct `ssh -N -L` dial (no master): a real per-process
    /// gateway connection, in-flight-bounded against `MaxStartups`,
    /// argv pinned against operator mux config, gated on the child's
    /// own listener.
    async fn establish_direct(
        &self,
        peer_id: &str,
        local_port: u16,
        target_host: &str,
        target_port: u16,
    ) -> Result<ForwardLeg, LocalForwardError> {
        with_dial_slot(&self.direct_dial_slots, async {
            let argv =
                build_local_forward_argv(local_port, target_host, target_port, &self.gateway);
            tracing::info!(
                peer_id,
                local = %format!("127.0.0.1:{local_port}"),
                dest = %format!("{target_host}:{target_port}"),
                via = %argv_target(&argv),
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
            let mut child = cmd.spawn().map_err(|source| LocalForwardError::Spawn {
                peer_id: peer_id.to_owned(),
                source,
            })?;
            gate_forward_listening(peer_id, local_port, Some(&mut child), DIRECT_ESTABLISH_GATE)
                .await?;
            Ok(ForwardLeg::Direct { child })
        })
        .await
    }
}

/// The production establisher over [`LegBuilder`].
fn production_establisher(gateway: SshConfig, control_path: Option<String>) -> LegEstablisher {
    let builder = Arc::new(LegBuilder::new(gateway, control_path));
    Box::new(move |peer_id, local_port, target_host, target_port| {
        let builder = Arc::clone(&builder);
        let peer_id = peer_id.to_owned();
        let target_host = target_host.to_owned();
        Box::pin(async move {
            builder
                .establish(&peer_id, local_port, &target_host, target_port)
                .await
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
    leg: ForwardLeg,
}

struct Inner {
    entries: HashMap<String, ForwardEntry>,
    escalation: ReconnectEscalation,
}

/// The per-peer local-forward registry: owns every forward leg from
/// establishment to teardown, keyed by peer id (the `-L` mirror of the
/// `-R` path's `PerSecondaryTunnelRegistry`).
pub struct LocalForwardTunnels {
    establisher: LegEstablisher,
    inner: Mutex<Inner>,
}

impl LocalForwardTunnels {
    /// Production registry over the given gateway credentials (the
    /// same `SshConfig` the connected [`dynrunner_gateway::SshGateway`]
    /// holds) and that gateway's master control socket
    /// ([`dynrunner_gateway::SshGateway::control_path`]): every leg
    /// registers on the master (`ssh -O forward`) while it answers
    /// `ssh -O check`, and direct-dials (WARN, in-flight-bounded)
    /// otherwise. `None` means there is no master to share — every
    /// leg direct-dials.
    pub fn new(gateway: SshConfig, master_control_path: Option<String>) -> Self {
        Self::with_establisher(
            production_establisher(gateway, master_control_path),
            ReconnectEscalation::default(),
        )
    }

    /// DI constructor for tests: inject the leg factory and the
    /// escalation thresholds.
    pub(crate) fn with_establisher(
        establisher: LegEstablisher,
        escalation: ReconnectEscalation,
    ) -> Self {
        Self {
            establisher,
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                escalation,
            }),
        }
    }

    /// Establish one forward per target, CONCURRENTLY: a sequential
    /// walk pays a full establishment gate per peer before the
    /// bootstrap's connect window even opens (~30s observed at 11
    /// peers); fanned out, the whole cohort gates together.
    /// Concurrency is safe against the gateway sshd because mux legs
    /// are sessionless registrations on the ONE master connection and
    /// direct-dial legs are in-flight-bounded below `MaxStartups`
    /// (see the module's `# Multiplexing` section). Per-target
    /// failures are WARNed and tolerated — `join_running_cluster`
    /// fans to every seed, so one dead record must not brick the
    /// bootstrap — but ZERO successes is a hard, loud error.
    ///
    /// Returns the `peer_id → local_port` endpoint map the caller
    /// substitutes into its seed entries.
    pub async fn establish(
        &self,
        targets: &[ForwardTarget],
    ) -> Result<HashMap<String, u16>, LocalForwardError> {
        let results = futures_util::future::join_all(
            targets
                .iter()
                .map(|t| async move { (t, self.establish_one(t).await) }),
        )
        .await;
        let mut endpoints = HashMap::new();
        for (t, result) in results {
            match result {
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
    /// register it. Replaces (and releases) any prior entry for the
    /// id.
    async fn establish_one(&self, target: &ForwardTarget) -> Result<u16, LocalForwardError> {
        let local_port = reserve_local_port(&target.peer_id)?;
        let leg = self
            .establish_leg(&target.peer_id, local_port, &target.host, target.port)
            .await?;
        let displaced = {
            let mut inner = self.inner.lock().await;
            inner.entries.insert(
                target.peer_id.clone(),
                ForwardEntry {
                    local_port,
                    host: target.host.clone(),
                    port: target.port,
                    leg,
                },
            )
        };
        if let Some(mut old) = displaced {
            old.leg.release(&target.peer_id).await;
        }
        Ok(local_port)
    }

    /// Build + gate one forward leg (shared by the establish and
    /// rebuild paths so the two cannot drift on policy), with ONE
    /// retry through the full seam: a transiently refused or raced
    /// leg re-resolves its transport (the master may have died — or
    /// recovered — in between) instead of failing the peer outright.
    async fn establish_leg(
        &self,
        peer_id: &str,
        local_port: u16,
        host: &str,
        port: u16,
    ) -> Result<ForwardLeg, LocalForwardError> {
        match (self.establisher)(peer_id, local_port, host, port).await {
            Ok(leg) => Ok(leg),
            Err(first) => {
                tracing::warn!(
                    peer_id,
                    local_port,
                    error = %first,
                    "local-forward leg failed to establish; retrying once"
                );
                (self.establisher)(peer_id, local_port, host, port).await
            }
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
    /// `reestablish_one_tunnel`: an alive leg is a no-op (the ~60s
    /// lost-visibility cadence must never rebuild its own healthy
    /// forward) until K consecutive alive-noop ticks prove it
    /// half-dead; a dead leg (direct child exited, or the master —
    /// and with it the mux registration's listener — gone) is rebuilt
    /// immediately. Rebuilds keep the SAME local port (the mesh's
    /// dial info is immutable) and re-resolve their transport, so a
    /// mux leg orphaned by master death comes back as a direct dial.
    pub async fn reconnect_one(
        &self,
        peer_id: &str,
    ) -> Result<ReconnectOutcome, LocalForwardError> {
        let mut inner = self.inner.lock().await;
        let alive = match inner.entries.get_mut(peer_id) {
            Some(entry) => entry.leg.is_alive(entry.local_port),
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
        // Release the old leg FIRST so the local port is free for the
        // same-port rebind (direct: reap our child — the local mirror
        // of the `-R` path's remote `fuser -k` release; mux: cancel
        // the master-side registration).
        {
            let entry = inner
                .entries
                .get_mut(peer_id)
                .expect("entry checked present above");
            entry.leg.release(peer_id).await;
        }
        let leg = self.establish_leg(peer_id, local_port, &host, port).await?;
        inner
            .entries
            .get_mut(peer_id)
            .expect("entry checked present above")
            .leg = leg;
        inner.escalation.on_rebuilt(peer_id);
        Ok(ReconnectOutcome::Rebuilt { local_port })
    }

    /// Release every leg (direct: SIGTERM → 5s → SIGKILL; mux:
    /// `ssh -O cancel`). Idempotent. `kill_on_drop` on direct
    /// children backstops the paths that never reach this; mux
    /// registrations are additionally torn down with the master
    /// itself (gateway disconnect / the master babysitter).
    pub async fn teardown(&self) {
        let drained: Vec<(String, ForwardEntry)> = {
            let mut inner = self.inner.lock().await;
            inner.entries.drain().collect()
        };
        for (peer_id, mut entry) in drained {
            entry.leg.release(&peer_id).await;
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

    fn healthy_leg() -> ForwardLeg {
        ForwardLeg::Direct {
            child: healthy_child(),
        }
    }

    /// The failure a leg whose dial died inside the gate produces,
    /// simulating `ExitOnForwardFailure` killing a refused forward.
    fn dead_leg_error(peer_id: &str) -> LocalForwardError {
        LocalForwardError::Establish {
            peer_id: peer_id.to_owned(),
            rc: Some(255),
            stderr: "remote port forwarding failed".into(),
        }
    }

    /// Establisher producing healthy direct legs and counting
    /// invocations.
    fn counting_establisher(count: Arc<AtomicUsize>) -> LegEstablisher {
        Box::new(move |_peer, _lport, _host, _port| {
            count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(healthy_leg()) })
        })
    }

    /// A currently-free port BELOW the kernel's ephemeral range
    /// (32768+): ports there are never handed out by `bind :0`, so a
    /// "nothing listens here" test premise cannot be yanked away by a
    /// parallel test (or process) receiving the just-freed port —
    /// which is exactly what `reserve_local_port` ports are exposed
    /// to inside a many-test binary.
    fn quiet_port() -> u16 {
        (23990..24190)
            .find(|&p| !local_port_listening(p))
            .expect("a quiet sub-ephemeral port exists")
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

    /// The direct dial pins ALL mux-relevant options off: an operator
    /// ssh_config with `ControlMaster auto`/`ControlPersist yes` must
    /// not be able to hand the dial off to a user master (instant
    /// rc=0 exit, forward parked on an unowned master — the unpinned
    /// half of the LMU misread).
    #[test]
    fn argv_pins_mux_options_off() {
        let argv = build_local_forward_argv(15001, "compute7", 51200, &ssh_config());
        assert!(argv.contains(&"ControlPath=none".to_string()), "{argv:?}");
        assert!(argv.contains(&"ControlMaster=no".to_string()), "{argv:?}");
        assert!(argv.contains(&"ControlPersist=no".to_string()), "{argv:?}");
    }

    /// The fallback gate: a configured-but-dead master socket resolves
    /// to a direct dial (`None`), never a mux argv pointing at a dead
    /// socket. (`ssh -O check` probes only the local socket — offline
    /// and fast.) `None` configured stays `None` without probing.
    #[tokio::test]
    async fn resolve_mux_dead_socket_falls_back_to_direct() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dead = dir.path().join("no-master.sock");
        let dead = dead.to_string_lossy();
        assert_eq!(
            resolve_mux_control_path(Some(&dead), &ssh_config(), "secondary-0").await,
            None
        );
        assert_eq!(
            resolve_mux_control_path(None, &ssh_config(), "secondary-0").await,
            None
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
        let establisher: LegEstablisher = Box::new(|peer, _l, _h, _p| {
            let dead = peer == "secondary-dead";
            let peer = peer.to_owned();
            Box::pin(async move {
                if dead {
                    Err(dead_leg_error(&peer))
                } else {
                    Ok(healthy_leg())
                }
            })
        });
        let tunnels =
            LocalForwardTunnels::with_establisher(establisher, ReconnectEscalation::default());
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
        let establisher: LegEstablisher = Box::new(|peer, _l, _h, _p| {
            let peer = peer.to_owned();
            Box::pin(async move { Err(dead_leg_error(&peer)) })
        });
        let tunnels =
            LocalForwardTunnels::with_establisher(establisher, ReconnectEscalation::default());
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

    /// Establishment is CONCURRENT: every leg build must begin before
    /// any completes. The barrier-gated establisher releases only once
    /// ALL N futures are in flight — a sequential walk (the pre-fix
    /// shape, one full gate per peer) deadlocks on the barrier and
    /// trips the timeout.
    #[tokio::test]
    async fn establish_overlaps_all_spawns() {
        const N: usize = 4;
        let barrier = Arc::new(tokio::sync::Barrier::new(N));
        let establisher: LegEstablisher = Box::new(move |_peer, _l, _h, _p| {
            let barrier = barrier.clone();
            Box::pin(async move {
                barrier.wait().await;
                Ok(healthy_leg())
            })
        });
        let tunnels =
            LocalForwardTunnels::with_establisher(establisher, ReconnectEscalation::default());
        let targets: Vec<ForwardTarget> = (0..N)
            .map(|i| ForwardTarget {
                peer_id: format!("secondary-{i}"),
                host: format!("compute{i}"),
                port: 51200 + i as u16,
            })
            .collect();
        let endpoints = tokio::time::timeout(Duration::from_secs(30), tunnels.establish(&targets))
            .await
            .expect(
                "establish must fan out concurrently — a sequential walk \
                 deadlocks on the all-spawns barrier",
            )
            .expect("all forwards establish");
        assert_eq!(endpoints.len(), N);
        tunnels.teardown().await;
    }

    /// The spawn log line names its three endpoints truthfully:
    /// `local=` the 127.0.0.1 listen side, `dest=` the real forward
    /// destination, `via=` the gateway hop. Regression pin for the
    /// consumer-reported mis-wiring that rendered the gateway paired
    /// with the LOCAL port as `target=`.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn spawn_log_names_local_dest_and_via() {
        let establisher = production_establisher(ssh_config(), None);
        // The log line is emitted before the dial; the leg itself
        // fails fast (an ssh at an unresolvable gateway exits inside
        // the gate) and any child is reaped via kill_on_drop.
        let _ = establisher("secondary-0", 15001, "compute7", 51200).await;
        assert!(logs_contain("local=127.0.0.1:15001"));
        assert!(logs_contain("dest=compute7:51200"));
        assert!(logs_contain("via=alice@gw.example.org"));
    }

    /// The establishment gate verifies the FORWARD, not the child: a
    /// listener on the local port passes the gate even though the
    /// dialing child already exited rc=0 — the mux-client shape that
    /// the old child-longevity gate misread as 10/11 failures on the
    /// LMU gateway (forward alive on the master, client gone in
    /// ~300ms).
    #[tokio::test]
    async fn gate_passes_on_listener_even_with_dead_child() {
        let listener =
            std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).expect("bind");
        let port = listener.local_addr().unwrap().port();
        let mut child = {
            let mut cmd = Command::new("sh");
            cmd.args(["-c", "exit 0"]);
            cmd.stderr(std::process::Stdio::piped());
            cmd.spawn().expect("spawn")
        };
        // Let the child exit first — the gate must STILL pass.
        let _ = child.wait().await;
        gate_forward_listening(
            "secondary-0",
            port,
            Some(&mut child),
            Duration::from_secs(3),
        )
        .await
        .expect("listening forward must pass the gate regardless of child state");
    }

    /// …and the inverse: no listener fails the gate at the deadline
    /// even though the child is alive and healthy-looking (a
    /// handshake that never produces a forward must not be declared
    /// established).
    #[tokio::test]
    async fn gate_fails_without_listener_even_with_live_child() {
        let port = quiet_port();
        let mut child = healthy_child();
        let err = gate_forward_listening(
            "secondary-0",
            port,
            Some(&mut child),
            Duration::from_millis(300),
        )
        .await
        .expect_err("no listener must fail the gate");
        assert!(
            matches!(&err, LocalForwardError::Establish { rc: None, stderr, .. }
                if stderr.contains("did not start listening")),
            "{err:?}"
        );
        terminate_child(&mut child).await;
    }

    /// A direct child dying before its forward listens is a hard
    /// failure carrying the child's rc and stderr (the
    /// `ExitOnForwardFailure` diagnosis channel).
    #[tokio::test]
    async fn gate_captures_direct_child_exit_diagnostics() {
        let port = quiet_port();
        let mut child = {
            let mut cmd = Command::new("sh");
            cmd.args(["-c", "echo 'remote port forwarding failed' >&2; exit 255"]);
            cmd.stderr(std::process::Stdio::piped());
            cmd.spawn().expect("spawn")
        };
        let err = gate_forward_listening(
            "secondary-0",
            port,
            Some(&mut child),
            Duration::from_secs(3),
        )
        .await
        .expect_err("dead child without listener must fail");
        assert!(
            matches!(&err, LocalForwardError::Establish { rc: Some(255), stderr, .. }
                if stderr.contains("remote port forwarding failed")),
            "{err:?}"
        );
    }

    /// Mux-leg liveness is the local LISTEN state (the master holds
    /// the listener; master death takes it down) — never a child.
    #[tokio::test]
    async fn mux_leg_alive_tracks_listener() {
        let port = quiet_port();
        let listener =
            std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).expect("bind");
        let mut leg = ForwardLeg::Mux {
            control_path: "/tmp/dynrunner-m-0-0.sock".into(),
            gateway: ssh_config(),
            spec: MasterForward::Local {
                bind_addr: "127.0.0.1".into(),
                bind_port: port,
                dest_host: "compute1".into(),
                dest_port: 51200,
            },
        };
        assert!(leg.is_alive(port), "listener up ⇒ mux leg alive");
        drop(listener);
        assert!(!leg.is_alive(port), "listener gone ⇒ mux leg dead");
    }

    /// The in-flight bound: with K slots, N>K dials never overlap
    /// more than K deep (the `MaxStartups` guard for the direct
    /// fallback), and all N still complete.
    #[tokio::test]
    async fn dial_slots_bound_concurrency() {
        const SLOTS: usize = 2;
        const N: usize = 6;
        let slots = Arc::new(Semaphore::new(SLOTS));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let high_water = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..N {
            let slots = slots.clone();
            let in_flight = in_flight.clone();
            let high_water = high_water.clone();
            tasks.push(tokio::spawn(async move {
                with_dial_slot(&slots, async {
                    let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    high_water.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                })
                .await;
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let peak = high_water.load(Ordering::SeqCst);
        assert!(
            peak <= SLOTS,
            "in-flight peak {peak} exceeded the {SLOTS}-slot bound"
        );
    }

    /// One failed leg attempt retries once through the full seam (a
    /// transiently refused dial must not cost the peer its forward);
    /// a second failure surfaces.
    #[tokio::test]
    async fn establish_retries_failed_leg_once() {
        let count = Arc::new(AtomicUsize::new(0));
        let establisher: LegEstablisher = {
            let count = count.clone();
            Box::new(move |peer, _l, _h, _p| {
                let n = count.fetch_add(1, Ordering::SeqCst);
                let peer = peer.to_owned();
                Box::pin(async move {
                    if n == 0 {
                        Err(dead_leg_error(&peer))
                    } else {
                        Ok(healthy_leg())
                    }
                })
            })
        };
        let tunnels =
            LocalForwardTunnels::with_establisher(establisher, ReconnectEscalation::default());
        let targets = vec![ForwardTarget {
            peer_id: "secondary-0".into(),
            host: "compute1".into(),
            port: 51200,
        }];
        let endpoints = tunnels
            .establish(&targets)
            .await
            .expect("retry must recover the leg");
        assert_eq!(endpoints.len(), 1);
        assert_eq!(count.load(Ordering::SeqCst), 2, "exactly one retry");
        tunnels.teardown().await;
    }

    /// The reconnect gate: an alive child is tolerated (no respawn)
    /// until the escalation threshold, then force-rebuilt on the SAME
    /// local port; a reconnect for an unknown peer errors loudly.
    #[tokio::test]
    async fn reconnect_gates_alive_child_then_force_rebuilds_same_port() {
        let count = Arc::new(AtomicUsize::new(0));
        let tunnels = LocalForwardTunnels::with_establisher(
            counting_establisher(count.clone()),
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
        let tunnels = LocalForwardTunnels::with_establisher(
            counting_establisher(count.clone()),
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
            let ForwardLeg::Direct { child } = &mut entry.leg else {
                panic!("test establisher builds direct legs");
            };
            child.start_kill().unwrap();
            let _ = child.wait().await;
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
            assert!(
                Instant::now() < deadline,
                "pid {pid} was not reaped in time"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// The explicit teardown chokepoint (layer (c)) reaps every tunnel
    /// child — the orderly success/error exit path the run loop drives.
    #[tokio::test]
    async fn teardown_reaps_every_child() {
        let count = Arc::new(AtomicUsize::new(0));
        let tunnels = LocalForwardTunnels::with_establisher(
            counting_establisher(count.clone()),
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
            let mut inner = tunnels.inner.lock().await;
            inner
                .entries
                .values_mut()
                .map(|e| match &e.leg {
                    ForwardLeg::Direct { child } => child.id().expect("child pid"),
                    ForwardLeg::Mux { .. } => panic!("test establisher builds direct legs"),
                })
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
        let tunnels = LocalForwardTunnels::with_establisher(
            counting_establisher(count.clone()),
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
            match &inner.entries["secondary-0"].leg {
                ForwardLeg::Direct { child } => child.id().expect("child pid"),
                ForwardLeg::Mux { .. } => panic!("test establisher builds direct legs"),
            }
        };
        assert!(pid_alive(pid), "child {pid} should be alive before drop");
        // Drop the registry without teardown — `kill_on_drop` fires.
        drop(tunnels);
        await_pid_gone(pid).await;
    }
}
