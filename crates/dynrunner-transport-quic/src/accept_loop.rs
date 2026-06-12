//! Resilient QUIC/WSS accept loops — the ONE owner of the
//! "listener outlives any single bad connection" policy, shared by the
//! peer mesh's acceptors ([`crate::peer`]) and the submitter/primary
//! listener ([`crate::network`]).
//!
//! # The defect this closes (run_20260611_200548)
//!
//! The previous accept loops drove each connection's handshake INLINE
//! on the accept path and `break`-ed on any accept error. Two listener
//! killers followed, both produced in numbers by a mass tunnel
//! collapse:
//!
//! - **aborted handshake** — a TCP connect that dies before/during the
//!   WebSocket upgrade (a collapsing ssh forward, a force-rebuilt
//!   tunnel RST-ing its in-flight dial, a port probe) surfaced as an
//!   accept `Err`, and the loop exited FOREVER. Every later redial got
//!   a TCP refusal while a FRESH process (fresh listener) connected in
//!   seconds — the production observer-reconnect wedge.
//! - **stalled handshake** — a connect that blackholes mid-handshake
//!   parked the (timeout-less) inline handshake and with it the WHOLE
//!   accept loop, silently.
//!
//! # The policy
//!
//! - The accept path only ACCEPTS. The per-connection handshake runs on
//!   the spawned per-connection task — via the connection types' own
//!   handshake drivers ([`QuicConnection::from_incoming`],
//!   [`WssConnection::accept_handshake`]), each bounded internally by
//!   their 30s `INBOUND_HANDSHAKE_TIMEOUT`; a failed or stalled
//!   handshake drops THAT connection and nothing else.
//! - A listener-level accept error is logged and retried after
//!   [`ACCEPT_ERROR_BACKOFF`] (transient classes like `ECONNABORTED` /
//!   fd exhaustion must not kill the mesh's ingress; the backoff stops
//!   a hot spin while the condition persists). The loops exit only when
//!   the listener itself is gone (QUIC endpoint closed; the WSS loop
//!   owns its socket, so only LocalSet teardown ends it).
//!
//! # Handshake budget: 30s
//!
//! Two independent fixes of this defect picked 60s (loop-level timeout)
//! and 30s (inside the handshake drivers); the reconciled
//! implementation keeps the drivers' 30s. The longest conformant wait
//! is the QUIC first-bi-stream accept, which resolves only once the
//! dialer WRITES — production dialers send a keepalive/digest within
//! one ~20s cadence period, so 30s covers it with margin, and the
//! first PROTOCOL frame after the handshake is separately bounded by
//! the network-side handlers' 60s `WELCOME_TIMEOUT`. A WSS upgrade is
//! immediate on a live wire; 30s there is already generous.

use std::future::Future;
use std::time::Duration;

use crate::transport::{QuicConnection, QuicListener};
use crate::wss::{WssConnection, WssListener};

/// Pause after a LISTENER-level accept error before re-accepting, so a
/// persistent error condition (e.g. fd exhaustion) degrades to a slow
/// retry loop instead of a hot spin. The success path never sleeps.
pub(crate) const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// WSS accept loop: accept raw TCP, hand each connection's WebSocket
/// handshake + `handler` to its own spawned task. One bad connection
/// never kills or wedges the listener; the loop runs for the life of
/// the `LocalSet`.
pub(crate) async fn wss_accept_loop_resilient<F, Fut>(
    listener: WssListener,
    ctx: &'static str,
    handler: F,
) where
    F: Fn(WssConnection) -> Fut + Clone + 'static,
    Fut: Future<Output = ()> + 'static,
{
    loop {
        match listener.accept_raw().await {
            Ok((stream, peer_addr)) => {
                tracing::debug!(%peer_addr, ctx, "WSS TCP connection accepted");
                let handler = handler.clone();
                tokio::task::spawn_local(async move {
                    // Internally bounded (30s `INBOUND_HANDSHAKE_TIMEOUT`):
                    // a stalled upgrade errors out of the driver itself.
                    match WssConnection::accept_handshake(stream).await {
                        Ok(conn) => handler(conn).await,
                        Err(e) => tracing::debug!(
                            %peer_addr,
                            error = %e,
                            ctx,
                            "inbound WSS upgrade failed; dropping the attempt \
                             (listener kept — the dialer's redial lands fresh)"
                        ),
                    }
                });
            }
            Err(e) => {
                // Listener-level accept(2) fault (transient ECONNABORTED
                // under a reset storm, EMFILE, …): the listener socket is
                // still bound, so keep accepting — paced so a persistent
                // fault cannot busy-spin the executor. The loop ends only
                // with the task (LocalSet teardown).
                tracing::warn!(
                    error = %e,
                    ctx,
                    "WSS accept(2) error; listener kept, retrying after backoff"
                );
                tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
            }
        }
    }
}

/// QUIC accept loop: await each incoming attempt, hand its TLS
/// handshake + first-bi-stream accept + `handler` to its own spawned
/// task. One bad connection never kills or wedges the listener; the
/// loop ends only when the endpoint itself closes.
pub(crate) async fn quic_accept_loop_resilient<F, Fut>(
    listener: QuicListener,
    ctx: &'static str,
    handler: F,
) where
    F: Fn(QuicConnection) -> Fut + Clone + 'static,
    Fut: Future<Output = ()> + 'static,
{
    // `None` — the endpoint itself closed — is the only loop exit.
    loop {
        let Some(incoming) = listener.accept_raw().await else {
            tracing::info!(ctx, "QUIC endpoint closed; accept loop ends");
            break;
        };
        let remote = incoming.remote_address();
        let handler = handler.clone();
        tokio::task::spawn_local(async move {
            // Internally bounded (30s `INBOUND_HANDSHAKE_TIMEOUT`): a
            // dialer that never opens its bi stream errors out of the
            // driver itself.
            match QuicConnection::from_incoming(incoming).await {
                Ok(conn) => handler(conn).await,
                Err(e) => tracing::debug!(
                    %remote,
                    error = %e,
                    ctx,
                    "inbound QUIC handshake failed; dropping the attempt \
                     (listener kept — the dialer's redial lands fresh)"
                ),
            }
        });
    }
}
