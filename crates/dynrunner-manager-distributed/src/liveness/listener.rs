//! [`LivenessListener`] — the primary's liveness-beacon receiver.
//!
//! # Concern
//!
//! The primary owns NO local worker pool (it runs no build), so its tokio
//! runtime is never CPU-starved by a co-resident build — the listener is
//! safe to run as an ordinary task there. It binds a dedicated
//! [`tokio::net::UdpSocket`], decodes each inbound liveness datagram, and
//! forwards the asserting node's id to the primary's operational loop,
//! which folds it into the per-secondary death-clock as a UNION with the
//! existing inbound-frame refresh (`record_keepalive`): a secondary is
//! reaped iff BOTH its beacon AND its mesh frames are absent for the
//! threshold.
//!
//! # Boundary
//!
//! The listener knows nothing about `PrimaryCoordinator` /
//! `SecondaryCoordinator` internals. It decodes each beacon and feeds TWO
//! independent subscription styles, both pure outputs of the one decode:
//!
//!  - a PUSH stream: the decoded node-id on an [`tokio::sync::mpsc`]
//!    channel, drained per-datagram by the primary's reaper
//!    (`record_keepalive`) — the secondary→primary direction (#323);
//!  - a POLL view: a [`super::BeaconLiveness`] freshness cell, sampled on
//!    its own cadence by the secondary's failover-detector tick — the
//!    primary→secondaries direction. The listener records every decoded
//!    beacon's receipt time; the reader unions that freshness with its
//!    mesh-frame view of the same node.
//!
//! Each subscriber reads only what it needs and neither knows about the
//! other; the death-clock / election semantics stay the coordinators'
//! concern and the socket-plumbing the listener's.
//!
//! # Run-token filter
//!
//! Each datagram carries a per-run instance token. The listener drops any
//! whose token does not match the run it was constructed for, so a beacon
//! from a PRIOR run (a process that outlived its run and kept sending)
//! cannot refresh a same-id node's death-clock in a NEW run. This is a
//! sanity discriminator on a trusted compute fabric, not authentication.

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::BeaconLiveness;
use super::datagram;

/// Fixed recv buffer. A liveness datagram is the small header plus a short
/// logical node id (`secondary-N` / `SETUP_NODE_ID`); 512 bytes is far
/// more than any well-formed payload and bounds a malformed/oversized one.
const RECV_BUF: usize = 512;

/// A running liveness listener. Owns the recv task; dropping it aborts the
/// task (the socket closes, the channel sender drops).
pub struct LivenessListener {
    task: JoinHandle<()>,
}

impl LivenessListener {
    /// Bind the liveness UDP socket on `bind_addr` and spawn the recv
    /// task. Returns the bound local port (so the caller can advertise it
    /// in its `PeerConnectionInfo.liveness_port`), the receiver end of the
    /// decoded-node-id PUSH channel (drained by the primary's reaper), and
    /// the [`BeaconLiveness`] POLL view (sampled by the secondary's
    /// failover-detector tick). Both are fed from the same decode.
    ///
    /// `expected_token`:
    /// - `Some(t)` enforces the run-token filter — datagrams carrying a
    ///   different token are dropped (a run-wide token cross-checked
    ///   against stale-prior-run beacons).
    /// - `None` accepts any well-formed datagram. This is sound because the
    ///   liveness port is bound EPHEMERALLY per run (`0.0.0.0:0`), so a
    ///   stale prior-run beacon addresses the prior run's now-dead port —
    ///   the port-per-run already isolates runs, and no run-wide token is
    ///   threaded through the boot path today. The token field stays on the
    ///   wire for a future run-wide token without a wire change.
    ///
    /// Must be called on a tokio runtime (the node's healthy runtime — the
    /// primary owns no worker pool, and the secondary's failover-detector
    /// only consults the view, never the build-starved emit path).
    pub async fn bind(
        bind_addr: std::net::SocketAddr,
        expected_token: Option<u64>,
    ) -> std::io::Result<(Self, u16, mpsc::UnboundedReceiver<String>, BeaconLiveness)> {
        let socket = UdpSocket::bind(bind_addr).await?;
        let local_port = socket.local_addr()?.port();
        let (tx, rx) = mpsc::unbounded_channel::<String>();
        let liveness = BeaconLiveness::new();
        let listener_liveness = liveness.clone();
        let task = tokio::task::spawn_local(async move {
            run_listener(socket, expected_token, tx, listener_liveness).await;
        });
        Ok((Self { task }, local_port, rx, liveness))
    }
}

impl Drop for LivenessListener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Recv loop: decode each datagram, drop foreign/stale-token ones, and
/// forward the asserting node's id. Exits when the socket errors
/// permanently or every receiver is dropped (channel send fails).
async fn run_listener(
    socket: UdpSocket,
    expected_token: Option<u64>,
    tx: mpsc::UnboundedSender<String>,
    liveness: BeaconLiveness,
) {
    let mut buf = [0u8; RECV_BUF];
    loop {
        let n = match socket.recv_from(&mut buf).await {
            Ok((n, _from)) => n,
            // A transient recv error (e.g. ICMP port-unreachable surfaced
            // on the socket) is non-fatal; keep listening. A hard error
            // is rare for a bound UDP socket; treat any error uniformly as
            // "skip this read".
            Err(_) => continue,
        };
        let Some(d) = datagram::decode(&buf[..n]) else {
            continue;
        };
        if let Some(expected) = expected_token
            && d.token != expected
        {
            // Stale-run or foreign-run beacon — not a liveness signal for
            // THIS run's death-clock. (Skipped when `expected_token` is
            // `None`: the ephemeral per-run port already isolates runs.)
            continue;
        }
        // POLL view: record this node's beacon-receipt freshness (the
        // secondary's failover-detector polls it). Always updated, even
        // after the PUSH receiver drops — the secondary may still consult
        // the view while no primary drains the channel.
        liveness.record(&d.node_id);
        // PUSH stream: forward the node-id to the primary's reaper. A send
        // failure means no reaper is draining (a pure secondary that never
        // promoted, or a run winding down). That is NOT a reason to stop
        // recording the POLL view, so do not break — only the unused PUSH
        // half is dropped. Stop entirely only if BOTH sinks are gone, which
        // is the listener-handle drop (`task.abort()`).
        let _ = tx.send(d.node_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket as StdUdpSocket;

    /// A datagram with the matching run-token is decoded and the node-id
    /// forwarded; a wrong-token datagram is dropped (stale-run guard).
    #[tokio::test]
    async fn forwards_matching_token_drops_stale() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let bind: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
                let (_listener, port, mut rx, view) =
                    LivenessListener::bind(bind, Some(0x1234)).await.unwrap();

                let sender = StdUdpSocket::bind(("127.0.0.1", 0)).unwrap();
                let dst: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

                // Wrong token → dropped.
                sender
                    .send_to(&datagram::encode("secondary-9", 0x9999), dst)
                    .unwrap();
                // Right token → forwarded.
                sender
                    .send_to(&datagram::encode("secondary-3", 0x1234), dst)
                    .unwrap();

                // The first id we receive must be secondary-3 (the stale
                // one was dropped, never forwarded).
                let got = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
                    .await
                    .expect("a forwarded id arrives")
                    .expect("channel open");
                assert_eq!(got, "secondary-3");
                // The POLL view recorded the SAME accepted beacon (so the
                // secondary's failover-detector can consult it) and never
                // the stale-token one.
                assert!(view.last_seen("secondary-3").is_some());
                assert!(view.last_seen("secondary-9").is_none());
            })
            .await;
    }

    /// The POLL view keeps recording even after the PUSH receiver is
    /// dropped: a pure secondary that never promoted has no reaper draining
    /// the channel, yet its failover-detector must still see the primary's
    /// beacon freshness. (Pre-fix the listener `break`-ed on the first
    /// failed `tx.send`, which would have stopped recording the view.)
    #[tokio::test]
    async fn poll_view_records_after_push_receiver_dropped() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let bind: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
                let (_listener, port, rx, view) =
                    LivenessListener::bind(bind, None).await.unwrap();
                // Drop the PUSH receiver — no primary reaper on this node.
                drop(rx);

                let sender = StdUdpSocket::bind(("127.0.0.1", 0)).unwrap();
                let dst: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
                sender
                    .send_to(&datagram::encode("primary-0", 1), dst)
                    .unwrap();

                // The view must still record the primary's beacon.
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                loop {
                    if view.last_seen("primary-0").is_some() {
                        break;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "the POLL view must record despite the dropped PUSH receiver"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            })
            .await;
    }
}
