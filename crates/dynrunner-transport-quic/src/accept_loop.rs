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
//!   the spawned per-connection task, under [`HANDSHAKE_TIMEOUT`]; its
//!   failure drops THAT connection and nothing else.
//! - A listener-level accept error is logged and retried after
//!   [`ACCEPT_ERROR_BACKOFF`] (transient classes like `ECONNABORTED` /
//!   fd exhaustion must not kill the mesh's ingress; the backoff stops
//!   a hot spin while the condition persists). The loops exit only when
//!   the listener itself is gone (QUIC endpoint closed; the WSS loop
//!   owns its socket, so only LocalSet teardown ends it).

use std::future::Future;
use std::time::Duration;

use crate::transport::{QuicConnection, QuicListener};
use crate::wss::{WssConnection, WssListener};

/// Per-connection handshake budget. Covers the QUIC TLS handshake +
/// first-bi-stream wait (which resolves only once the dialer WRITES —
/// production dialers send a keepalive/digest within one ~20s cadence
/// period) and the WSS upgrade. Aligned with the first-frame
/// `WELCOME_TIMEOUT` the network-side handlers already apply after it.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(60);

/// Pause after a LISTENER-level accept error before re-accepting, so a
/// persistent error condition (e.g. fd exhaustion) degrades to a slow
/// retry loop instead of a hot spin.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

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
                let handler = handler.clone();
                tokio::task::spawn_local(async move {
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, WssListener::handshake(stream))
                        .await
                    {
                        Ok(Ok(conn)) => handler(conn).await,
                        Ok(Err(e)) => tracing::debug!(
                            %peer_addr,
                            error = %e,
                            ctx,
                            "WSS handshake failed; dropping that connection \
                             (listener unaffected)"
                        ),
                        Err(_) => tracing::debug!(
                            %peer_addr,
                            timeout_s = HANDSHAKE_TIMEOUT.as_secs(),
                            ctx,
                            "WSS handshake stalled past the budget; dropping \
                             that connection (listener unaffected)"
                        ),
                    }
                });
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    ctx,
                    "WSS accept error (transient); listener keeps accepting"
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
    loop {
        let Some(incoming) = listener.accept_incoming().await else {
            tracing::info!(ctx, "QUIC endpoint closed; accept loop ends");
            break;
        };
        let remote = incoming.remote_address();
        let handler = handler.clone();
        tokio::task::spawn_local(async move {
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, QuicListener::handshake(incoming)).await {
                Ok(Ok(conn)) => handler(conn).await,
                Ok(Err(e)) => tracing::debug!(
                    %remote,
                    error = %e,
                    ctx,
                    "QUIC handshake failed; dropping that connection \
                     (listener unaffected)"
                ),
                Err(_) => tracing::debug!(
                    %remote,
                    timeout_s = HANDSHAKE_TIMEOUT.as_secs(),
                    ctx,
                    "QUIC handshake stalled past the budget; dropping that \
                     connection (listener unaffected)"
                ),
            }
        });
    }
}
