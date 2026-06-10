//! Low-level ssh-reverse-tunnel wire-up primitives:
//! [`build_ssh_argv`] (pure argv construction),
//! [`production_spawner`] (the closure passed to
//! [`establish_one_tunnel_inner`](super::establish::establish_one_tunnel_inner)),
//! [`spawn_reverse_tunnel`] (`Command::spawn` of the ssh subprocess),
//! [`verify_tunnel_alive`] (3s sanity-check on the spawned child),
//! [`probe_tunnel_bind`] / [`production_bind_verifier`] (worker-side
//! bind verification closing the partial-`-R`-bind hole), and
//! [`terminate_child`] (SIGTERM, 5s wait, SIGKILL). All consumed by
//! the establishment policy in [`establish`](super::establish) and
//! the cleanup path in [`pipeline`](super::pipeline).

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use dynrunner_gateway::shell::shell_join;
use tokio::process::{Child, Command};

use super::options::{PrepError, PreparationOptions};

/// Shared per-host linger bookkeeping: each compute node's ORIGINAL
/// logind linger state at tunnel-establish time (for the run-end
/// restore) plus the enable VERDICT (for the post-cohort summary).
///
/// Original state: `true` ⇒ the node was ALREADY lingering before this
/// run touched it (the run-end restore leaves it untouched); `false` ⇒
/// the run enabled it (the restore disables it). First-writer-wins per
/// host: a node hosting more than one secondary is probed first by the
/// earliest watcher, capturing the genuine pre-run state; later
/// secondaries on the same node read the (now-enabled-by-us) `yes` but
/// must NOT overwrite the recorded original, so the restore still
/// disables correctly.
///
/// Enable verdict: `true` ⇒ at least one enable attempt on the host
/// reported `ENABLE=ok` (any-success-wins: a retried attempt that
/// eventually lands clears an earlier failure). Consumed by
/// [`summarize_linger_enables`], which emits the per-cohort verdict
/// line — at the IMPORTANT target only when something failed.
///
/// Owned by [`SlurmPreparation`](super::pipeline::SlurmPreparation),
/// populated through the spawner DI seam, drained by [`restore_linger`]
/// at teardown / [`summarize_linger_enables`] after the cohort gather.
/// Whole-run-scoped ⇒ one restore + one summary per run, race-free.
#[derive(Clone, Default)]
pub(super) struct LingerLedger(Arc<StdMutex<LingerLedgerInner>>);

#[derive(Default)]
struct LingerLedgerInner {
    /// host -> ORIGINAL linger state (first-writer-wins).
    original: HashMap<String, bool>,
    /// host -> enable verdict (any-success-wins).
    enable_ok: HashMap<String, bool>,
}

impl LingerLedger {
    /// Record one enable attempt's outcome for `host`: the probed
    /// original state (first-writer-wins) and the enable verdict
    /// (any-success-wins).
    pub(super) fn record_enable(&self, host: &str, was_linger: bool, enabled: bool) {
        let mut inner = self.0.lock().expect("linger ledger mutex poisoned");
        inner.original.entry(host.to_owned()).or_insert(was_linger);
        let verdict = inner.enable_ok.entry(host.to_owned()).or_insert(enabled);
        *verdict = *verdict || enabled;
    }

    /// The hosts whose linger this run ENABLED (original state `false`)
    /// — the restore-disable set. Drains the original-state map so a
    /// second call is a harmless no-op (idempotent teardown).
    pub(super) fn drain_restore_hosts(&self) -> Vec<String> {
        let mut inner = self.0.lock().expect("linger ledger mutex poisoned");
        let hosts = inner
            .original
            .iter()
            .filter(|&(_, &was_on)| !was_on)
            .map(|(host, _)| host.clone())
            .collect();
        inner.original.clear();
        hosts
    }

    /// The cohort enable verdict: `(ok_count, sorted failed hosts)`.
    /// Drains the verdict map so a later cohort (re-entry) summarises
    /// only its own attempts.
    pub(super) fn drain_enable_verdicts(&self) -> (usize, Vec<String>) {
        let mut inner = self.0.lock().expect("linger ledger mutex poisoned");
        let ok = inner.enable_ok.values().filter(|&&v| v).count();
        let mut failed: Vec<String> = inner
            .enable_ok
            .iter()
            .filter(|&(_, &v)| !v)
            .map(|(host, _)| host.clone())
            .collect();
        failed.sort();
        inner.enable_ok.clear();
        (ok, failed)
    }
}

/// Push the shared ssh-invocation prologue — `ssh`, the gateway
/// auth options, and the ProxyJump (`-J` / `ProxyCommand`) hop — onto
/// `argv`. Every per-compute ssh invocation (the reverse tunnel AND
/// the pre-rebind port release) jumps through the gateway identically;
/// centralising the prologue here is the single source of truth for
/// the OpenSSH 7.3+ ProxyCommand workaround so the two call sites can
/// never drift.
///
/// Pure (no I/O).
fn push_jump_prologue(argv: &mut Vec<String>, opts: &PreparationOptions) {
    argv.push("ssh".into());
    argv.extend(opts.auth_options.iter().cloned());

    let jump_target = match &opts.gateway_user {
        Some(u) => format!("{u}@{}", opts.gateway_host),
        None => opts.gateway_host.clone(),
    };

    if !opts.auth_options.is_empty() {
        // -J doesn't propagate -o flags into the inner ssh that
        // it spawns (OpenSSH 7.3+ regression). Use ProxyCommand
        // with the auth flags inline so the inner ssh inherits
        // them as real argv. Same flag list — single source of
        // truth lives on the gateway.
        let mut proxy_parts: Vec<String> = vec!["ssh".into()];
        proxy_parts.extend(opts.auth_options.iter().cloned());
        if opts.gateway_port != 22 {
            proxy_parts.push("-p".into());
            proxy_parts.push(opts.gateway_port.to_string());
        }
        proxy_parts.push("-W".into());
        proxy_parts.push("%h:%p".into());
        proxy_parts.push(jump_target.clone());
        argv.push("-o".into());
        argv.push(format!("ProxyCommand={}", shell_join(&proxy_parts)));
    } else {
        let jump_with_port = if opts.gateway_port != 22 {
            format!("{jump_target}:{}", opts.gateway_port)
        } else {
            jump_target.clone()
        };
        argv.push("-J".into());
        argv.push(jump_with_port);
    }
}

/// The `<user>@<host>` ssh target for the compute node. Remote user
/// defaults to gateway_user, then "root" — matches Python; the actual
/// SLURM compute node typically isn't logged into so this is the
/// master tunnel hop's user.
fn remote_target(remote_host: &str, opts: &PreparationOptions) -> String {
    let remote_user = opts.gateway_user.as_deref().unwrap_or("root");
    format!("{remote_user}@{remote_host}")
}

/// Build the argv for `ssh -N -R <tunnel_port>:localhost:<primary> ...`
/// per the Python implementation's shape — including the auth-options-
/// aware ProxyCommand workaround for OpenSSH 7.3+.
///
/// Pure (no I/O), so the argv shape is unit-testable without spawning
/// a real subprocess.
pub(super) fn build_ssh_argv(
    remote_host: &str,
    tunnel_port: u16,
    primary_quic_port: u16,
    opts: &PreparationOptions,
) -> Vec<String> {
    let mut argv: Vec<String> = Vec::new();
    push_jump_prologue(&mut argv, opts);

    argv.push("-R".into());
    argv.push(format!("{tunnel_port}:localhost:{primary_quic_port}"));

    for (local_port, gateway_port) in &opts.extra_port_forwards {
        argv.push("-R".into());
        argv.push(format!("{gateway_port}:localhost:{local_port}"));
    }

    argv.push(remote_target(remote_host, opts));
    argv.extend([
        "-N".into(),
        "-o".into(),
        "StrictHostKeyChecking=no".into(),
        "-o".into(),
        "UserKnownHostsFile=/dev/null".into(),
        "-o".into(),
        "ExitOnForwardFailure=yes".into(),
        // Keepalive tolerance for the per-secondary `-R` reverse-tunnel.
        // ServerAliveInterval=60 + CountMax=1080 = 18h ceiling before
        // SSH considers the session dead — matches the gateway
        // ControlMaster's floor at `dynrunner_gateway::ssh:132-134`.
        // The pre-fix values (30 × 3 = 90 s) killed the tunnel
        // mid-stream during multi-MB nar-file transfers when the
        // worker's sshd was too busy serving the transfer to PONG
        // ServerAlive within the window (asm-dataset-nix R8 LMU repro:
        // 225 MB narfile starts at HTTP 200, dies partway, retries
        // hit "Could not connect" because the `-R` listener is gone
        // — no auto-reconnect path on the SSH side).
        //
        // Detection of genuinely-dead secondaries is the framework's
        // own `primary_link_failure_threshold/_window` (cf.
        // `dynrunner-manager-distributed::secondary::mod.rs:84,92`),
        // not the ssh tunnel's keepalive. Decoupling here just means
        // the ssh tunnel doesn't get killed by transient unresponsive
        // periods that the framework wouldn't have considered fatal.
        "ServerAliveInterval=60".into(),
        "-o".into(),
        "ServerAliveCountMax=1080".into(),
        "-o".into(),
        "TCPKeepAlive=yes".into(),
    ]);
    argv
}

/// Push the shared ssh-option block for a SHORT one-shot remote command
/// (the stale-port release and the linger enable/restore both run one):
/// disable host-key prompts (`StrictHostKeyChecking=no` +
/// `UserKnownHostsFile=/dev/null`, matching the reverse tunnel) and bound
/// the handshake (`ConnectTimeout=10`) so a wedged node degrades fast
/// rather than soaking the budget. Single source of truth for the one-shot
/// command ssh policy, so the release and linger argv builders never drift.
///
/// Pure (no I/O).
fn push_oneshot_command_opts(argv: &mut Vec<String>) {
    argv.extend([
        "-o".into(),
        "StrictHostKeyChecking=no".into(),
        "-o".into(),
        "UserKnownHostsFile=/dev/null".into(),
        "-o".into(),
        "ConnectTimeout=10".into(),
    ]);
}

/// The targeted remote command that releases a single stale reverse-
/// tunnel listener on the compute node. Kills ONLY the process bound
/// to `tunnel_port/tcp` — the worker-side sshd session that still
/// holds the `-R <tunnel_port>` forwarding from a dropped tunnel.
///
/// This is the worker-side mirror of the local teardown's targeted
/// kill (`pkill 'ssh.*-R [0-9]+:localhost'` in
/// [`crate::pipeline::pkill_residual_reverse_tunnels`]): both kill
/// exactly the per-secondary reverse-tunnel holder for one port, never
/// a broad sweep that could hit a live tunnel. `fuser -k` is preferred
/// (one syscall-precise kill of the socket owner); the `ss`+`kill`
/// fallback covers nodes without psmisc. `:= true` keeps the overall
/// command exit-0 when the port is ALREADY free (the graceful-close
/// case — nothing to release), so the release step is a harmless no-op
/// there rather than a spurious failure.
fn build_release_remote_cmd(tunnel_port: u16) -> String {
    format!(
        "fuser -k {port}/tcp 2>/dev/null \
         || (ss -tlnpH 'sport = :{port}' 2>/dev/null \
             | grep -o 'pid=[0-9]*' | cut -d= -f2 | sort -u \
             | xargs -r kill 2>/dev/null) \
         ; true",
        port = tunnel_port
    )
}

/// Build the argv for the pre-rebind port-release ssh command: the
/// same gateway jump + compute-node target as the reverse tunnel, but
/// running [`build_release_remote_cmd`] instead of holding a `-N -R`
/// forward. No `ExitOnForwardFailure` / keepalive — this is a short
/// one-shot remote command, not a long-lived tunnel.
///
/// Pure (no I/O), so the argv shape is unit-testable without spawning
/// a real subprocess.
pub(super) fn build_release_argv(
    remote_host: &str,
    tunnel_port: u16,
    opts: &PreparationOptions,
) -> Vec<String> {
    let mut argv: Vec<String> = Vec::new();
    push_jump_prologue(&mut argv, opts);
    argv.push(remote_target(remote_host, opts));
    // Short bounded handshake — don't let a wedged release soak the
    // per-tunnel budget; the rebind retry will try again.
    push_oneshot_command_opts(&mut argv);
    argv.push(build_release_remote_cmd(tunnel_port));
    argv
}

/// Force-release a stale worker-side `-R <tunnel_port>` binding before
/// a rebind, then return.
///
/// Best-effort by contract: a failure here (release-ssh couldn't reach
/// the node, kill tool missing, …) is logged and swallowed — the
/// subsequent rebind still runs, and if the port was genuinely free
/// the rebind succeeds regardless. The release exists so that the
/// UNGRACEFUL-drop case (where the worker's sshd still holds the
/// listener, with no FIN/RST to prompt-release it) stops failing the
/// same-port rebind with rc=255 "remote port forwarding failed".
async fn release_stale_reverse_port(
    secondary_id: &str,
    remote_host: &str,
    tunnel_port: u16,
    opts: &PreparationOptions,
) {
    let argv = build_release_argv(remote_host, tunnel_port, opts);
    tracing::info!(
        secondary_id,
        tunnel_port,
        "releasing stale worker-side reverse-tunnel binding before rebind"
    );
    tracing::debug!(secondary_id, cmd = %shell_join(&argv), "release argv");

    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..]);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::piped());
    cmd.kill_on_drop(true);
    match cmd.output().await {
        Ok(out) => {
            if !out.status.success() {
                let stderr = String::from_utf8_lossy(&out.stderr);
                tracing::warn!(
                    secondary_id,
                    tunnel_port,
                    rc = ?out.status.code(),
                    stderr = %stderr.trim(),
                    "stale-port release command returned non-zero; proceeding with rebind anyway"
                );
            }
        }
        Err(e) => {
            tracing::warn!(
                secondary_id,
                tunnel_port,
                error = %e,
                "stale-port release ssh failed to spawn; proceeding with rebind anyway"
            );
        }
    }
}

/// Verdict of the worker-side BIND-VERIFICATION probe that closes the
/// partial-`-R`-bind hole: OpenSSH's sshd binds the remote-forward
/// listener PER ADDRESS FAMILY and replies `SSH2_MSG_REQUEST_SUCCESS`
/// when at least ONE family binds (`channel_setup_fwd_listener_tcpip`
/// sets `success = 1` inside the per-`addrinfo` loop; a failed bind
/// with another candidate remaining is logged only at `verbose`), so a
/// transient v4 collision on the tunnel port yields a v6-only — or, in
/// the worst case the probe also covers, a wrong-host — listener while
/// the submitter-side `ssh -R` survives `ExitOnForwardFailure=yes` and
/// the 3s alive-gate. The probe asks the WORKER what actually got
/// bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum BindProbe {
    /// At least one loopback-dialable listener (`127.x` / `::1` /
    /// wildcard) exists on the tunnel port. The carried `addr:port`
    /// strings are the matching `ss` local addresses, for the log.
    Listening { listeners: Vec<String> },
    /// The probe RAN on the worker (`TUNNEL_PROBE=done` marker seen)
    /// and found NO loopback-dialable listener on the tunnel port —
    /// the definite verification failure.
    NotListening,
    /// The probe could not produce a verdict (ssh failed before the
    /// marker, or `ss` is unavailable on the node). The caller keeps
    /// the gate-verified tunnel and WARNs — a probe-infrastructure
    /// failure must never kill tunnels that met the pre-probe
    /// standard.
    Inconclusive { reason: String },
}

/// The remote shell command that reports every TCP listener on
/// `tunnel_port` as a structured `TUNNEL_LISTEN=<addr>:<port>` line,
/// then `TUNNEL_PROBE=done` (or `TUNNEL_PROBE=no-ss` when iproute2 is
/// absent). Mirrors the `WAS_LINGER=` marker pattern: the structured
/// stdout lines — not the ssh exit code — carry the outcome. Matching
/// any field that ends in `:<port>` (rather than an `ss` `sport =`
/// filter or a positional column) keeps the probe robust across
/// iproute2 versions: only the local-address column of a listener row
/// can end in the literal port (the peer column of a listener is
/// always `*`-ported).
fn build_bind_probe_remote_cmd(tunnel_port: u16) -> String {
    format!(
        "if command -v ss >/dev/null 2>&1; \
         then ss -tln 2>/dev/null \
              | awk '{{ for (i=1;i<=NF;i++) if ($i ~ /:{port}$/) print \"TUNNEL_LISTEN=\" $i }}'; \
              printf 'TUNNEL_PROBE=done\\n'; \
         else printf 'TUNNEL_PROBE=no-ss\\n'; fi",
        port = tunnel_port
    )
}

/// Build the argv for the worker-side bind-verification ssh: the same
/// gateway jump + compute-node target as the reverse tunnel, running
/// [`build_bind_probe_remote_cmd`] as a short one-shot command (plain
/// exec — no PTY needed, the markers go to clean stdout). Same
/// one-shot ssh policy as the release/linger commands.
///
/// Pure (no I/O), so the argv shape is unit-testable without spawning
/// a real subprocess.
pub(super) fn build_bind_probe_argv(
    remote_host: &str,
    tunnel_port: u16,
    opts: &PreparationOptions,
) -> Vec<String> {
    let mut argv: Vec<String> = Vec::new();
    push_jump_prologue(&mut argv, opts);
    argv.push(remote_target(remote_host, opts));
    push_oneshot_command_opts(&mut argv);
    argv.push(build_bind_probe_remote_cmd(tunnel_port));
    argv
}

/// PURE: is this `ss` local ADDRESS (the `addr` half of the reported
/// `addr:port`) reachable through the loopback dial the secondary's
/// bring-up performs (`127.0.0.1` / `[::1]`)? Loopback and wildcard
/// binds qualify; a SPECIFIC non-loopback bind does not — e.g. the
/// colliding squatter itself listening on a LAN address, which must
/// not be mistaken for the tunnel's listener.
fn is_loopback_dialable(addr: &str) -> bool {
    let bare = addr.trim_start_matches('[').trim_end_matches(']');
    bare == "*"
        || bare == "::"
        || bare == "::1"
        || bare == "0.0.0.0"
        || bare.starts_with("127.")
        // v4-mapped loopback (`::ffff:127.0.0.1`).
        || bare.ends_with(":127.0.0.1")
}

/// PURE: parse the bind-probe markers out of the probe ssh's stdout
/// into a [`BindProbe`] verdict. CR-trims for transport safety;
/// ignores `TUNNEL_LISTEN=` lines whose port is not `tunnel_port`
/// (defensive — the remote awk already filters) or whose address is
/// not loopback-dialable (the squatter-on-a-LAN-address case).
pub(super) fn parse_bind_probe(stdout: &str, tunnel_port: u16) -> BindProbe {
    let suffix = format!(":{tunnel_port}");
    let mut probe_ran = false;
    let mut no_tool = false;
    let mut listeners: Vec<String> = Vec::new();
    for line in stdout.lines() {
        let line = line.trim_end_matches('\r').trim();
        if let Some(value) = line.strip_prefix("TUNNEL_LISTEN=") {
            if let Some(addr) = value.strip_suffix(&suffix)
                && is_loopback_dialable(addr)
            {
                listeners.push(value.to_string());
            }
        } else if line == "TUNNEL_PROBE=done" {
            probe_ran = true;
        } else if line == "TUNNEL_PROBE=no-ss" {
            no_tool = true;
        }
    }
    if !listeners.is_empty() {
        BindProbe::Listening { listeners }
    } else if probe_ran {
        BindProbe::NotListening
    } else if no_tool {
        BindProbe::Inconclusive {
            reason: "`ss` unavailable on the worker (iproute2 missing)".into(),
        }
    } else {
        BindProbe::Inconclusive {
            reason: "no probe marker in output (probe ssh failed before the command ran)".into(),
        }
    }
}

/// Run ONE bind-verification ssh round-trip against the worker and
/// parse the verdict. An ssh spawn/exec failure is `Inconclusive` —
/// the probe reports on the TUNNEL only when it actually reached the
/// node.
async fn run_bind_probe(
    secondary_id: &str,
    remote_host: &str,
    tunnel_port: u16,
    opts: &PreparationOptions,
) -> BindProbe {
    let argv = build_bind_probe_argv(remote_host, tunnel_port, opts);
    tracing::debug!(secondary_id, tunnel_port, cmd = %shell_join(&argv), "bind-probe argv");

    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..]);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.kill_on_drop(true);
    match cmd.output().await {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            let verdict = parse_bind_probe(&stdout, tunnel_port);
            if !matches!(verdict, BindProbe::Listening { .. }) {
                let stderr = String::from_utf8_lossy(&out.stderr);
                tracing::debug!(
                    secondary_id,
                    tunnel_port,
                    rc = ?out.status.code(),
                    stdout = %stdout.trim(),
                    stderr = %stderr.trim(),
                    "bind-probe round-trip did not confirm a listener"
                );
            }
            verdict
        }
        Err(e) => BindProbe::Inconclusive {
            reason: format!("bind-probe ssh failed to spawn: {e}"),
        },
    }
}

/// Grace before the single re-probe: the 3s alive-gate can pass while
/// a slow handshake's `tcpip-forward` request is still in flight, so
/// the FIRST miss may just be "not bound YET". One bounded re-probe
/// after this pause separates slightly-slow from never-bound without
/// letting a flapping probe kill healthy tunnels.
const BIND_PROBE_GRACE: Duration = Duration::from_secs(2);

/// Verify the worker-side tunnel listener exists, with one grace
/// re-probe on a first miss (see [`BIND_PROBE_GRACE`]). The verdict
/// semantics are [`BindProbe`]'s; only a repeated definite miss
/// returns [`BindProbe::NotListening`].
pub(super) async fn probe_tunnel_bind(
    secondary_id: &str,
    remote_host: &str,
    tunnel_port: u16,
    opts: &PreparationOptions,
) -> BindProbe {
    let first = run_bind_probe(secondary_id, remote_host, tunnel_port, opts).await;
    if first != BindProbe::NotListening {
        return first;
    }
    tracing::info!(
        secondary_id,
        tunnel_port,
        "worker-side tunnel listener not visible on first probe; re-probing once after grace"
    );
    tokio::time::sleep(BIND_PROBE_GRACE).await;
    run_bind_probe(secondary_id, remote_host, tunnel_port, opts).await
}

/// Build the production bind-verifier closure passed into
/// [`establish_one_tunnel_inner`](super::establish::establish_one_tunnel_inner)
/// alongside the spawner — the same DI seam shape, so the
/// establishment policy stays concern-blind to HOW verification talks
/// to the worker. Tests inject canned verdicts instead.
pub(super) fn production_bind_verifier(
    secondary_id: String,
    opts: PreparationOptions,
) -> impl FnMut(String, u16) -> std::pin::Pin<Box<dyn Future<Output = BindProbe>>> {
    move |host: String, tunnel_port: u16| {
        let secondary_id = secondary_id.clone();
        let opts = opts.clone();
        Box::pin(async move { probe_tunnel_bind(&secondary_id, &host, tunnel_port, &opts).await })
    }
}

/// Which logind linger mutation a [`build_linger_argv`] command performs.
/// `Enable` runs at tunnel-establish time (decouple the worker's
/// `user@<uid>.service` from the submitter's `-R` login session before that
/// session can drop); `Disable` runs at run-end teardown to RESTORE the
/// user's original state when it was off before the run touched it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LingerVerb {
    Enable,
    Disable,
}

impl LingerVerb {
    /// The `loginctl` subcommand. Self-targeting (no positional `<user>`):
    /// the ssh logs into the compute node AS the run user (the same
    /// `remote_target` the reverse tunnel uses), so the bare form mutates
    /// linger for exactly that user — which is the user the worker container
    /// runs as. This matches the proven interactive `loginctl enable-linger`
    /// over an ssh login, and avoids re-deriving the run user's name on the
    /// remote side.
    fn subcommand(self) -> &'static str {
        match self {
            LingerVerb::Enable => "enable-linger",
            LingerVerb::Disable => "disable-linger",
        }
    }
}

/// The remote shell command that (for `Enable`) FIRST reports the user's
/// current linger state on a `WAS_LINGER=yes|no` line, THEN runs the
/// requested `loginctl` mutation and reports its outcome on an
/// `ENABLE=ok|fail` / `DISABLE=ok|fail` line. Both markers go to stdout so
/// the setup can parse them out of a single round-trip even under a forced
/// PTY (`-tt`), which merges stderr onto the same stream.
///
/// The probe is BUS-FREE: a `test -e` on `/var/lib/systemd/linger/<user>` —
/// the persistent marker `enable-linger` writes and systemd itself reads —
/// NOT `loginctl show-user`, which needs logind/a session and proved
/// fragile: on a node where it failed, the probe yielded an EMPTY
/// `WAS_LINGER=` that was misread as "was off", and the run-end restore
/// then DISABLED an operator PRE-SET linger while the cluster still ran —
/// re-arming the fan-kill the linger was protecting against (krater
/// post-mortem 2026-06-10). The file test answers identically in any
/// context and can only say `yes` or `no`.
///
/// The `WAS_LINGER` probe runs for BOTH verbs but is only consumed on
/// `Enable` (the setup captures it so the matching `Disable` restore knows
/// whether the user was already lingering). The mutation's own error output
/// is CAPTURED onto the fail marker line (`{marker}=fail <reason>`) — the
/// structured markers, NOT the ssh exit code, carry the outcome: the forced
/// PTY (`-tt`) masks the remote exit status (ssh reports the PTY session's
/// exit — observed `0` for a FAILED `enable-linger`), so the marker line is
/// the only reliable channel for both the verdict and the reason (e.g. a
/// polkit-restricted node's "Could not enable linger: Access denied").
fn build_linger_remote_cmd(verb: LingerVerb) -> String {
    let sub = verb.subcommand();
    let marker = match verb {
        LingerVerb::Enable => "ENABLE",
        LingerVerb::Disable => "DISABLE",
    };
    format!(
        "U=$(id -un); \
         if test -e \"/var/lib/systemd/linger/$U\"; then W=yes; else W=no; fi; \
         printf 'WAS_LINGER=%s\\n' \"$W\"; \
         if E=$(loginctl {sub} 2>&1); then printf '{marker}=ok\\n'; \
         else printf '{marker}=fail %s\\n' \"$E\"; fi",
    )
}

/// Build the argv for the setup-side linger ssh: the SAME gateway jump +
/// compute-node target as the reverse tunnel, but running
/// [`build_linger_remote_cmd`] instead of holding a `-N -R` forward, and
/// with a FORCED PTY (`-tt`).
///
/// Why `-tt`: the proven cure is an INTERACTIVE `loginctl enable-linger`
/// over an ssh login — which carries a `pam_systemd` logind session. A
/// plain `ssh host 'cmd'` exec MAY register a session (depends on the
/// node's sshd `UsePAM`/`pam_systemd` session-stack config); the wrapper's
/// own self-enable failed precisely because its slurmstepd context had NO
/// logind session. Forcing a PTY makes the enable mirror the interactive
/// login that is known to work, so the enable does not depend on the
/// node's non-interactive-session policy. `-tt` (double) forces PTY
/// allocation even though ssh's local side is not itself a TTY (the setup
/// runs under sbatch/an orchestrator, not a terminal).
///
/// No `-N`/`-R`/`ExitOnForwardFailure`/keepalive: this is a short one-shot
/// remote command, bounded by `ConnectTimeout`, NOT a long-lived tunnel.
///
/// Pure (no I/O), so the argv shape is unit-testable without spawning a
/// real subprocess.
pub(super) fn build_linger_argv(
    remote_host: &str,
    verb: LingerVerb,
    opts: &PreparationOptions,
) -> Vec<String> {
    let mut argv: Vec<String> = Vec::new();
    push_jump_prologue(&mut argv, opts);
    // Force a PTY on the destination so the remote `loginctl` runs inside a
    // pam_systemd logind session (mirrors the proven interactive login).
    argv.push("-tt".into());
    argv.push(remote_target(remote_host, opts));
    // Short bounded handshake — a wedged node must not soak the setup; the
    // worker still launches regardless (linger is best-effort).
    push_oneshot_command_opts(&mut argv);
    argv.push(build_linger_remote_cmd(verb));
    argv
}

/// PURE: parse the `WAS_LINGER=<value>` marker out of the linger ssh's
/// stdout. The remote `printf` writes exactly `WAS_LINGER=yes` /
/// `WAS_LINGER=no` / `WAS_LINGER=` (empty: no logind record yet). Under a
/// forced PTY (`-tt`) lines arrive CRLF-terminated and may be interleaved
/// with other markers, so we scan line-by-line and CR-trim. Returns the
/// trimmed value, or `None` when the marker is absent (the probe itself
/// failed) — the caller treats `None`/non-`yes` as "was NOT lingering",
/// the safe default (worst case is a redundant restore-disable on an
/// already-off state, a harmless no-op).
pub(super) fn parse_was_linger(stdout: &str) -> Option<String> {
    stdout
        .lines()
        .filter_map(|line| line.trim_end_matches('\r').strip_prefix("WAS_LINGER="))
        .next_back()
        .map(|v| v.trim().to_string())
}

/// PURE: the restore decision from the probe output. ONLY an explicit
/// `WAS_LINGER=no` permits the run-end restore-disable; an absent marker,
/// an EMPTY value, or anything unparsable defaults to `true` ("assume
/// already on" → restore skipped). The empty case is load-bearing: a failed
/// bus-dependent probe once yielded `WAS_LINGER=` (empty), which a naive
/// `== "yes"` mapping read as "was off" — and the restore then disabled an
/// operator PRE-SET linger mid-run, fan-killing the still-running cluster
/// (krater post-mortem 2026-06-10). Never disable a state the run cannot
/// PROVE it set.
pub(super) fn was_linger_from_probe(stdout: &str) -> bool {
    !matches!(parse_was_linger(stdout).as_deref(), Some("no"))
}

/// PURE: did the requested mutation report success? Scans for the
/// `<MARKER>=ok` line (`ENABLE=ok` / `DISABLE=ok`) the remote command
/// prints, CR-trimming for the `-tt` PTY. Absent or `=fail …` ⇒ `false`.
pub(super) fn linger_succeeded(stdout: &str, verb: LingerVerb) -> bool {
    let ok = match verb {
        LingerVerb::Enable => "ENABLE=ok",
        LingerVerb::Disable => "DISABLE=ok",
    };
    stdout
        .lines()
        .any(|line| line.trim_end_matches('\r').trim() == ok)
}

/// PURE: the remote `loginctl` error captured on the `<MARKER>=fail <reason>`
/// line — e.g. "Could not enable linger: Access denied". `None` when no fail
/// marker is present (the ssh itself failed before the printf ran) or the
/// captured reason is empty. This — NOT the ssh exit code — is the failure
/// detail: the forced PTY masks the remote exit status (see
/// [`build_linger_remote_cmd`]).
pub(super) fn linger_fail_reason(stdout: &str, verb: LingerVerb) -> Option<String> {
    let fail = match verb {
        LingerVerb::Enable => "ENABLE=fail",
        LingerVerb::Disable => "DISABLE=fail",
    };
    stdout
        .lines()
        .filter_map(|line| line.trim_end_matches('\r').strip_prefix(fail))
        .next_back()
        .map(|r| r.trim().to_string())
        .filter(|r| !r.is_empty())
}

/// ENABLE linger for the run user on `remote_host` over a forced-PTY ssh,
/// capturing the user's ORIGINAL linger state for the run-end restore.
///
/// Best-effort by contract (the owner reverted fail-fast): a failure here
/// (ssh unreachable, polkit-restricted enable, loginctl absent) is WARNed
/// and the run PROCEEDS — the only consequence is the loss of
/// submitter-ssh-drop protection, which the operator must then pre-set or
/// delegate. The reverse tunnel + worker container launch regardless.
///
/// Returns `(was_linger, enabled)`: the captured original state
/// (`was_linger == true` ⇒ the user was ALREADY lingering before this run,
/// so the restore leaves it untouched) and the enable VERDICT (`enabled ==
/// true` ⇒ the remote reported `ENABLE=ok`). On a failed/unparsable probe,
/// `was_linger` is `true` defensively: "assume already on" means the
/// restore will NOT disable a state this run may not have created.
async fn enable_linger_for_node(
    secondary_id: &str,
    remote_host: &str,
    opts: &PreparationOptions,
) -> (bool, bool) {
    let argv = build_linger_argv(remote_host, LingerVerb::Enable, opts);
    tracing::info!(
        secondary_id,
        remote_host,
        "enabling logind linger for the run user (decouple workers from the submitter -R session)"
    );
    tracing::debug!(secondary_id, cmd = %shell_join(&argv), "linger enable argv");

    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..]);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.kill_on_drop(true);
    match cmd.output().await {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            // ONLY an explicit `no` permits the run-end restore — absent,
            // empty, or unparsable probes all read as "already on" (see
            // was_linger_from_probe; never disable a state the run cannot
            // prove it set).
            let was_linger = was_linger_from_probe(&stdout);
            let enabled = linger_succeeded(&stdout, LingerVerb::Enable);
            if enabled {
                tracing::info!(
                    secondary_id,
                    remote_host,
                    was_linger,
                    "linger enabled; workers decoupled from the submitter login session \
                     (transient: restored at run-end unless it was already on)"
                );
            } else {
                // NO `rc` field: the forced PTY masks the remote exit status
                // (ssh reports the PTY session's exit — observed `0` for a
                // FAILED enable), so the status code would actively mislead.
                // The fail marker's captured reason is the real loginctl
                // error; local ssh stderr covers transport-level failures.
                let stderr = String::from_utf8_lossy(&out.stderr);
                let reason = linger_fail_reason(&stdout, LingerVerb::Enable)
                    .unwrap_or_else(|| "no fail marker (ssh died before loginctl ran)".into());
                tracing::warn!(
                    secondary_id,
                    remote_host,
                    was_linger,
                    reason = %reason,
                    ssh_stderr = %stderr.trim(),
                    "could not enable linger on this node — workers are NOT decoupled from the \
                     submitter -R session; a session drop may fan-kill them. Consequence: \
                     reduced resilience only. Remediation: pre-set `loginctl enable-linger` for \
                     the run user (or delegate it via polkit). Proceeding (linger is best-effort)."
                );
            }
            (was_linger, enabled)
        }
        Err(e) => {
            tracing::warn!(
                secondary_id,
                remote_host,
                error = %e,
                "linger-enable ssh failed to spawn; workers NOT decoupled (best-effort). Proceeding."
            );
            // Could not run the probe ⇒ assume already on (skip restore).
            (true, false)
        }
    }
}

/// DISABLE linger for the run user on `remote_host` over a forced-PTY ssh —
/// the run-end RESTORE. Called only for nodes whose `was_linger` was `false`
/// (the run enabled it), so logind is left exactly as it was found.
///
/// Best-effort: a failure is logged and swallowed. Whole-run-scoped (one
/// disable per node at teardown), so there is no per-job race.
async fn disable_linger_for_node(remote_host: &str, opts: &PreparationOptions) {
    let argv = build_linger_argv(remote_host, LingerVerb::Disable, opts);
    tracing::info!(
        remote_host,
        "restoring logind linger to off for the run user (it was not enabled before this run)"
    );
    tracing::debug!(cmd = %shell_join(&argv), "linger disable argv");

    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..]);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.kill_on_drop(true);
    match cmd.output().await {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            if !linger_succeeded(&stdout, LingerVerb::Disable) {
                // No `rc`: the `-tt` PTY masks the remote exit status (see
                // build_linger_remote_cmd) — the fail marker's reason is the
                // real loginctl error.
                let stderr = String::from_utf8_lossy(&out.stderr);
                let reason = linger_fail_reason(&stdout, LingerVerb::Disable)
                    .unwrap_or_else(|| "no fail marker (ssh died before loginctl ran)".into());
                tracing::warn!(
                    remote_host,
                    reason = %reason,
                    ssh_stderr = %stderr.trim(),
                    "could not restore linger to off on this node; leaving it enabled (best-effort)"
                );
            }
        }
        Err(e) => {
            tracing::warn!(
                remote_host,
                error = %e,
                "linger-disable ssh failed to spawn; leaving linger enabled (best-effort)"
            );
        }
    }
}

/// Per-node linger ENABLE + bookkeeping, invoked from the spawner before
/// the reverse tunnel is spawned.
///
/// Records the node's ORIGINAL linger state into the ledger
/// (first-writer-wins per host, so a multi-secondary node restores to its
/// genuine pre-run state) so [`restore_linger`] knows whether to disable it
/// at teardown, and the enable VERDICT (any-success-wins) for the
/// post-cohort [`summarize_linger_enables`]. The enable itself is
/// best-effort (see [`enable_linger_for_node`]); the recorded state drives
/// the restore regardless of whether this particular enable succeeded.
async fn ensure_node_linger(
    secondary_id: &str,
    remote_host: &str,
    opts: &PreparationOptions,
    ledger: &LingerLedger,
) {
    let (was_linger, enabled) = enable_linger_for_node(secondary_id, remote_host, opts).await;
    ledger.record_enable(remote_host, was_linger, enabled);
}

/// Run-end linger RESTORE: for every node the run enabled linger on (its
/// recorded `was_linger == false`), disable it again over a fresh
/// forced-PTY ssh, leaving logind exactly as it was found. Nodes that were
/// ALREADY lingering (`true`) are left untouched. Drains the map so a
/// second call is a harmless no-op (idempotent teardown).
///
/// Whole-run-scoped: called once from
/// [`SlurmPreparation::cleanup`](super::pipeline::SlurmPreparation::cleanup)
/// alongside the tunnel teardown. Best-effort per node (see
/// [`disable_linger_for_node`]).
pub(super) async fn restore_linger(ledger: &LingerLedger, opts: &PreparationOptions) {
    // Snapshot + clear under the ledger's std mutex (not held across the
    // awaits below).
    for host in ledger.drain_restore_hosts() {
        disable_linger_for_node(&host, opts).await;
    }
}

/// Emit the per-cohort linger-enable VERDICT line from the ledger's
/// recorded outcomes, draining them.
///
/// Routing is the point (the per-node WARNs in
/// [`enable_linger_for_node`] carry the detail but are routine prep
/// tracing): when EVERY recorded node enabled OK the summary is a
/// regular `info!` (durable in the full log, never on the
/// `--important-stdio-only` stdout — routine prep must not wake the
/// operator); when ANY node failed, the summary is wake-worthy and goes
/// to the IMPORTANT target — reduced fan-kill resilience on named nodes
/// is exactly the class of condition that stream exists for.
pub(super) fn summarize_linger_enables(ledger: &LingerLedger) {
    let (ok, failed) = ledger.drain_enable_verdicts();
    if failed.is_empty() {
        if ok > 0 {
            tracing::info!(ok, "linger enable: {ok} node(s) ok, 0 failed");
        }
    } else {
        tracing::warn!(
            target: crate::IMPORTANT_TARGET,
            ok,
            failed = failed.len(),
            "linger enable: {ok} node(s) ok, {} failed [{}] — workers on the failed nodes are \
             NOT decoupled from the submitter -R login session (a session drop may fan-kill \
             them). Remediation: pre-set `loginctl enable-linger` for the run user on those \
             nodes or delegate it via polkit.",
            failed.len(),
            failed.join(", "),
        );
    }
}

/// When a spawner invocation force-releases the worker-side
/// `-R <tunnel_port>` binding (via [`release_stale_reverse_port`])
/// before spawning the reverse tunnel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReleaseBeforeSpawn {
    /// Release only on RETRY invocations (the 2nd+ call of the same
    /// spawner). A first attempt targets a fresh port that no prior
    /// tunnel of this run has touched — paying the release round-trip
    /// there would tax every healthy establishment. A retry, by
    /// contrast, only happens because the PREVIOUS attempt failed
    /// (rc=255 within the alive-gate, or bind verification found no
    /// worker-side listener), and in both cases the port may still be
    /// held on the worker: a leftover partial `-R` bind (sshd binds
    /// per address family and reports success if EITHER lands — see
    /// [`build_bind_probe_remote_cmd`]), a stale forward, or the
    /// colliding squatter itself. The worker's dial target
    /// (`localhost:<tunnel_port>`) is FIXED at worker startup, so the
    /// retry cannot move to a fresh port — reclaiming the same one is
    /// the only honest path.
    OnRetry,
    /// Release on EVERY invocation — the observer-reconnect shape,
    /// where the rebuild only runs once the prior tunnel is known
    /// dead/half-dead and the stale worker-side binding is the
    /// expected obstacle.
    Always,
}

/// PURE: does invocation number `prior_spawns` (0-based: the number of
/// spawns this closure performed before the current one) perform the
/// pre-spawn release under `mode`? Extracted so the retry-release
/// semantics are unit-testable without spawning ssh.
pub(super) fn release_before_attempt(mode: ReleaseBeforeSpawn, prior_spawns: usize) -> bool {
    match mode {
        ReleaseBeforeSpawn::Always => true,
        ReleaseBeforeSpawn::OnRetry => prior_spawns > 0,
    }
}

/// Shared builder behind [`production_spawner`] / [`reconnect_spawner`]:
/// the per-attempt ssh wire-up (linger enable → optional worker-side
/// port release → reverse-tunnel spawn), parameterised ONLY by the
/// release mode so the two public spawners cannot drift.
fn tunnel_spawner(
    secondary_id: String,
    opts: PreparationOptions,
    primary_quic_port: u16,
    linger_ledger: LingerLedger,
    mode: ReleaseBeforeSpawn,
) -> impl FnMut(String, u16) -> std::pin::Pin<Box<dyn Future<Output = Result<Child, PrepError>>>> {
    let mut prior_spawns: usize = 0;
    move |host: String, tunnel_port: u16| {
        let release_now = release_before_attempt(mode, prior_spawns);
        prior_spawns += 1;
        let secondary_id = secondary_id.clone();
        let opts = opts.clone();
        let linger_ledger = linger_ledger.clone();
        Box::pin(async move {
            ensure_node_linger(&secondary_id, &host, &opts, &linger_ledger).await;
            if release_now {
                release_stale_reverse_port(&secondary_id, &host, tunnel_port, &opts).await;
            }
            spawn_reverse_tunnel(&secondary_id, &host, tunnel_port, primary_quic_port, &opts).await
        })
    }
}

/// Build the production spawner closure passed into
/// [`establish_one_tunnel_inner`]. Captures `(secondary_id, opts,
/// primary_quic_port)` by move so the returned closure is `'static`
/// and the futures it produces own their data — no borrow-lifetime
/// gymnastics at the call site. Each invocation clones the captured
/// state into the produced future (retry attempts get a fresh future
/// each time).
///
/// Retry attempts (2nd+ invocation) FIRST force-release the
/// worker-side `-R <tunnel_port>` binding before respawning — see
/// [`ReleaseBeforeSpawn::OnRetry`]: a retry only exists because the
/// prior attempt failed, and the same fixed port (the worker's baked-in
/// dial target) must be reclaimed, not re-collided with.
///
/// Linger: before the reverse tunnel is spawned, the run user's logind
/// linger is enabled on the target node (decoupling the worker's
/// `user@<uid>.service` from the submitter's `-R` login session BEFORE that
/// session exists, so a later session drop can't fan-kill the worker). The
/// node's ORIGINAL linger state is recorded into the linger ledger (keyed by
/// host, first-writer-wins so a node hosting multiple secondaries restores
/// to its genuine pre-run state) for the run-end restore in
/// [`restore_linger`], alongside the enable verdict for
/// [`summarize_linger_enables`]. The enable rides this SAME spawner DI seam
/// as the reverse tunnel itself — it is the per-node ssh-wire-up that must
/// happen around this node's tunnel — so the concern-blind establishment
/// policy ([`establish_tunnel`]) never sees it. Best-effort: a failure
/// WARNs and the tunnel + worker proceed.
pub(super) fn production_spawner(
    secondary_id: String,
    opts: PreparationOptions,
    primary_quic_port: u16,
    linger_ledger: LingerLedger,
) -> impl FnMut(String, u16) -> std::pin::Pin<Box<dyn Future<Output = Result<Child, PrepError>>>> {
    tunnel_spawner(
        secondary_id,
        opts,
        primary_quic_port,
        linger_ledger,
        ReleaseBeforeSpawn::OnRetry,
    )
}

/// Build the OBSERVER-RECONNECT spawner closure: identical to
/// [`production_spawner`] except it FIRST force-releases any stale
/// worker-side `-R <tunnel_port>` binding (the listener an ungraceful
/// drop left bound on the worker's sshd) before re-spawning the
/// reverse tunnel on the SAME port.
///
/// Why the release belongs in the spawner (not the establishment
/// policy): the policy engine ([`establish_tunnel`]) is concern-blind
/// — it owns retry/rate-limit/timeout and calls the spawner opaquely.
/// "Free the remote port before this handshake" is an ssh-wire-up
/// concern, so it lives here in the ssh module and rides the same
/// spawner DI seam the tests use. The release runs once per spawn
/// attempt: on the rare case where the first release races the worker
/// sshd's own teardown, a retry attempt re-releases and rebinds.
///
/// `tunnel_port` is unchanged (option-A "same port"): it is the
/// worker's own fixed listen port written into the info file at worker
/// startup, which the worker's mesh dials as `localhost:<tunnel_port>`
/// — a fresh port would break that dial with no re-coordination path.
///
/// Linger: like [`production_spawner`], the run user's linger is enabled on
/// the target node (recording the original state into the ledger)
/// before the rebind. A reconnect rebuilds an EXISTING node's dropped
/// tunnel; re-enabling is idempotent and the first-writer-wins restore map
/// preserves the node's genuine pre-run state.
pub(super) fn reconnect_spawner(
    secondary_id: String,
    opts: PreparationOptions,
    primary_quic_port: u16,
    linger_ledger: LingerLedger,
) -> impl FnMut(String, u16) -> std::pin::Pin<Box<dyn Future<Output = Result<Child, PrepError>>>> {
    tunnel_spawner(
        secondary_id,
        opts,
        primary_quic_port,
        linger_ledger,
        ReleaseBeforeSpawn::Always,
    )
}

/// Spawn the ssh tunnel subprocess from `build_ssh_argv` output.
async fn spawn_reverse_tunnel(
    secondary_id: &str,
    remote_host: &str,
    tunnel_port: u16,
    primary_quic_port: u16,
    opts: &PreparationOptions,
) -> Result<Child, PrepError> {
    let argv = build_ssh_argv(remote_host, tunnel_port, primary_quic_port, opts);

    tracing::info!(
        secondary_id,
        tunnel_port,
        primary_quic_port,
        extras = opts.extra_port_forwards.len(),
        "creating SSH reverse tunnel"
    );
    tracing::debug!(secondary_id, cmd = %shell_join(&argv), "ssh argv");

    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..]);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.kill_on_drop(true);
    let child = cmd.spawn().map_err(PrepError::Io)?;
    Ok(child)
}

/// Verify the just-spawned ssh process stayed alive past the 3s
/// "established" gate. The corresponding Python idiom is
/// `proc.wait(timeout=3)` raising `TimeoutExpired` on success.
///
/// # What the gate proves (and the deliberate residual)
///
/// Child survival past 3s, combined with `ExitOnForwardFailure=yes` in
/// [`build_ssh_argv`], proves the `-R` forward either bound successfully
/// or the handshake is still in flight — ssh exits (rc=255-class,
/// caught here) whenever the remote bind FAILS, so there is no silent
/// non-bind on a completed handshake. The residual is a handshake
/// slower than the 3s window that later fails, or a forward that dies
/// AFTER establishment: both are deliberately NOT probed here (a
/// worker-side `ss` check would cost one extra ssh round-trip per
/// tunnel per attempt across the whole fleet) because the observer's
/// lost-visibility reconnect cadence already closes them — a secondary
/// that never dials in keeps visibility lost, and the reestablish path
/// (liveness gate + half-dead escalation, see
/// [`super::pipeline::SlurmPreparation::reestablish_one_tunnel`])
/// rebuilds the tunnel.
///
/// Operates on a `&mut Child` owned by the caller — no shared-Vec
/// lookup. With ≥2 concurrent watchers this is the only safe shape:
/// using `last_mut()` on a shared `Vec<Child>` would race watcher A
/// onto watcher B's child as soon as their `push` interleaved.
pub(super) async fn verify_tunnel_alive(
    secondary_id: &str,
    child: &mut Child,
) -> Result<(), PrepError> {
    use tokio::io::AsyncReadExt;

    // Take both pipes BEFORE the wait so a CONCURRENT drain can run
    // alongside `child.wait()`. ssh writes its decisive failure line
    // ("Warning: remote port forwarding failed for listen port NNN")
    // to stderr in the window between spawn and exit; reading stderr
    // only AFTER the reap races that final flush — the OS pipe buffer
    // may already be drained-and-closed by the time we look, dropping
    // the one line the operator needs. Draining concurrently captures
    // every pre-exit byte regardless of when the child flushes.
    //
    // stdout is piped at spawn time but otherwise unused on the `-N -R`
    // tunnel; drain it too so a child that writes there cannot wedge on
    // a full pipe (back-pressure) and never exit.
    //
    // On the ALIVE path the pipes are restored to the Child (below) so
    // its long-lived stdout/stderr writes still have a live read end —
    // the concurrent drain only OWNS the pipes for the 3s gate window.
    let mut stderr_pipe = child.stderr.take();
    let mut stdout_pipe = child.stdout.take();

    // exit_info encodes alive/dead-with-rc:
    //   Outer None => still alive past 3s (success).
    //   Outer Some((rc_opt, stderr)) => process exited; rc_opt may be
    //     None (killed by signal). stderr carries every pre-exit byte,
    //     captured by the concurrent drain that ran against the wait.
    let exit_info: Option<(Option<i32>, String)> = {
        // The drain future: read both pipes to EOF concurrently (neither
        // back-pressures the other into a stall). Both reads resolve to
        // EOF only when the child closes its pipe ends — i.e. at/after
        // exit — so while the child is alive this future stays pending,
        // which is exactly what the 3s gate wants.
        let drain_streams = async {
            let mut err_buf = Vec::new();
            let mut out_buf = Vec::new();
            match (stderr_pipe.as_mut(), stdout_pipe.as_mut()) {
                (Some(e), Some(o)) => {
                    let _ = tokio::join!(e.read_to_end(&mut err_buf), o.read_to_end(&mut out_buf));
                }
                (Some(e), None) => {
                    let _ = e.read_to_end(&mut err_buf).await;
                }
                (None, Some(o)) => {
                    let _ = o.read_to_end(&mut out_buf).await;
                }
                (None, None) => {}
            }
            err_buf
        };

        // Run the wait and the concurrent stderr/stdout drain together,
        // bounded by the same 3s budget. `tokio::join!` keeps both
        // polling: the drain completes as the child closes its pipes at
        // exit, and the wait yields the status — so `stderr` holds the
        // final flush rather than a post-reap re-read that could miss it.
        let waited = tokio::time::timeout(Duration::from_secs(3), async {
            let (status, err_buf) = tokio::join!(child.wait(), drain_streams);
            status.map(|s| (s.code(), err_buf))
        })
        .await;
        match waited {
            Err(_elapsed) => None,
            Ok(Ok((rc, err_buf))) => {
                Some((rc, String::from_utf8_lossy(&err_buf).trim().to_string()))
            }
            Ok(Err(e)) => return Err(PrepError::Io(e)),
        }
    };

    match exit_info {
        None => {
            // Alive past the gate: hand the pipes back to the Child so
            // its later stdout/stderr writes keep a live read end (the
            // drain only borrowed them for the gate window).
            child.stderr = stderr_pipe;
            child.stdout = stdout_pipe;
            tracing::info!(secondary_id, "SSH tunnel established");
            Ok(())
        }
        Some((rc, stderr)) => {
            tracing::error!(
                secondary_id,
                rc = ?rc,
                stderr = %stderr,
                "SSH tunnel exited within 3s — forward not established"
            );
            Err(PrepError::TunnelFailed {
                secondary_id: secondary_id.to_owned(),
                rc,
                stderr,
            })
        }
    }
}

/// Send SIGTERM, wait up to 5s, then SIGKILL.
pub(super) async fn terminate_child(child: &mut Child) {
    if let Err(e) = child.start_kill() {
        // Already dead is fine; other errors are logged but don't
        // block the rest of teardown.
        tracing::debug!(error = %e, "start_kill on tunnel subprocess");
    }
    match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "tunnel subprocess wait failed"),
        Err(_) => {
            tracing::warn!("tunnel subprocess did not exit in 5s; force-killing");
            let _ = child.kill().await;
        }
    }
}
