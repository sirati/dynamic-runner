//! [`LivenessBeacon`] — the secondary's dedicated-thread liveness emitter.
//!
//! # Concern (the root fix)
//!
//! A secondary's app-level liveness keepalive MUST keep flowing to the
//! primary even while this node's tokio runtime is CPU-STARVED by a
//! co-resident, CPU-bound build subprocess (e.g. a long QEMU cross-arch
//! nix build on a `--cores=1` allocation). The main runtime hosts the
//! select loop, the mesh pump, the QUIC writer tasks, and quinn's UDP
//! driver — ALL on one `current_thread` LocalSet — so when the build
//! pegs the core, every one of them is descheduled and the runtime emits
//! nothing for the build's duration. The primary then sees `>threshold`
//! silence and false-declares this (alive, busy) node dead → requeue
//! thrash + duplicate compute + permanent slot loss.
//!
//! This beacon breaks that coupling: it runs on its OWN OS thread with
//! its OWN [`std::net::UdpSocket`], doing a blocking `send_to` + a
//! `thread::sleep`. A thread that is asleep for almost the whole interval
//! and needs only microseconds to fire a single datagram is promptly
//! scheduled by CFS even on a fully-pegged core (a just-woken sleeper has
//! a low vruntime), and the transmit is a syscall — not gated on the
//! tokio runtime being scheduled. So the beacon survives the starvation
//! that freezes the main runtime.
//!
//! # Boundary
//!
//! The beacon knows NOTHING about elections, the mesh, or operational
//! state. Its entire input surface is: this node's id, a per-run token, a
//! send interval, and a [`BeaconTarget`] cell the runtime publishes the
//! current primary's liveness address into. It reads the target each tick
//! and sends; that is all. The runtime owning "who is primary" stays the
//! single owner of that concern — the beacon just transmits to whatever
//! it publishes.
//!
//! # Lifecycle
//!
//! [`LivenessBeacon::spawn`] returns a [`LivenessBeacon`] handle owning
//! the thread join handle and a shutdown flag. Dropping the handle (or
//! calling [`LivenessBeacon::stop`]) sets the flag; the thread observes it
//! at the top of its next loop turn and exits, then is joined. The
//! interval is sliced so shutdown latency is bounded by
//! [`SHUTDOWN_POLL`], not the full send interval.

use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::datagram;
use super::target::BeaconTarget;

/// Granularity at which the sleeping beacon re-checks its shutdown flag.
/// Bounds teardown latency independent of the (possibly multi-second)
/// send interval.
const SHUTDOWN_POLL: Duration = Duration::from_millis(200);

/// A running liveness beacon. Owns the emitter thread; dropping it stops
/// and joins the thread.
pub struct LivenessBeacon {
    shutdown: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl LivenessBeacon {
    /// Spawn the beacon on a dedicated OS thread.
    ///
    /// `node_id` is this node's logical id (what the primary keys its
    /// death-clock on). `token` is the per-run instance discriminator.
    /// `interval` is the send cadence (typically the keepalive interval).
    /// `target` is the runtime-published current-primary liveness address.
    ///
    /// Binds an ephemeral local `UdpSocket` (`0.0.0.0:0`). On bind failure
    /// the beacon is NOT spawned and `Err` is returned — the caller treats
    /// it as "no beacon" (the union death-clock still has the frame path).
    pub fn spawn(
        node_id: String,
        token: u64,
        interval: Duration,
        target: BeaconTarget,
    ) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(("0.0.0.0", 0))?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let join = std::thread::Builder::new()
            .name(format!("liveness-beacon-{node_id}"))
            .spawn(move || {
                run_beacon(socket, node_id, token, interval, target, thread_shutdown);
            })?;
        Ok(Self {
            shutdown,
            join: Some(join),
        })
    }

    /// Signal the thread to stop and join it. Idempotent; also invoked by
    /// `Drop` so callers that just drop the handle still get a clean join.
    pub fn stop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for LivenessBeacon {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The beacon thread body. Sends one datagram to the currently-published
/// target per interval, sleeping in [`SHUTDOWN_POLL`] slices so the
/// shutdown flag is honoured promptly.
fn run_beacon(
    socket: UdpSocket,
    node_id: String,
    token: u64,
    interval: Duration,
    target: BeaconTarget,
    shutdown: Arc<AtomicBool>,
) {
    let payload = datagram::encode(&node_id, token);
    // Fire immediately on start so a freshly-spawned beacon refreshes the
    // primary's death-clock without waiting a full interval, then settle
    // into the cadence.
    let mut next_send = Instant::now();
    while !shutdown.load(Ordering::Relaxed) {
        let now = Instant::now();
        if now >= next_send {
            // Read the live target each tick: a `PrimaryChanged` that
            // republished a new address is picked up here with zero
            // beacon-side election knowledge. `None` (no primary resolved
            // yet) is a no-op for this tick.
            if let Some(addr) = target.current() {
                // A transient send error (target momentarily unroutable,
                // ICMP-unreachable on a closed socket) is non-fatal: the
                // beacon is a periodic UDP emitter, the next tick retries.
                let _ = socket.send_to(&payload, addr);
            }
            next_send = now + interval;
        }
        // Sleep until the next send or the next shutdown poll, whichever
        // is sooner.
        let until_next = next_send.saturating_duration_since(Instant::now());
        std::thread::sleep(until_next.min(SHUTDOWN_POLL));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket as StdUdpSocket;

    /// The beacon sends datagrams to the published target on its
    /// interval, and a receiver decodes them as this node's liveness
    /// assertion. This is the HEADLINE primitive: emission is driven by
    /// the dedicated thread's own clock, with NO tokio runtime present at
    /// all — proving the beacon does not depend on the (here absent)
    /// runtime that the build would starve.
    #[test]
    fn beacon_sends_to_published_target() {
        let listener = StdUdpSocket::bind(("127.0.0.1", 0)).unwrap();
        listener
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let target = BeaconTarget::new();
        target.publish(Some(addr));

        let _beacon = LivenessBeacon::spawn(
            "secondary-7".into(),
            0xABCD,
            Duration::from_millis(50),
            target,
        )
        .expect("beacon spawns");

        let mut buf = [0u8; 256];
        let (n, _from) = listener.recv_from(&mut buf).expect("beacon datagram arrives");
        let decoded = datagram::decode(&buf[..n]).expect("valid liveness datagram");
        assert_eq!(decoded.node_id, "secondary-7");
        assert_eq!(decoded.token, 0xABCD);
    }

    /// With no target published the beacon emits nothing (no panic, no
    /// spurious send to a bogus address). A later publish starts it.
    #[test]
    fn beacon_noop_until_target_published() {
        let listener = StdUdpSocket::bind(("127.0.0.1", 0)).unwrap();
        listener
            .set_read_timeout(Some(Duration::from_millis(300)))
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let target = BeaconTarget::new();
        let _beacon = LivenessBeacon::spawn(
            "secondary-0".into(),
            1,
            Duration::from_millis(40),
            target.clone(),
        )
        .expect("beacon spawns");

        // No target yet → nothing should arrive within the read timeout.
        let mut buf = [0u8; 256];
        assert!(
            listener.recv_from(&mut buf).is_err(),
            "no datagram before a target is published"
        );

        // Publish → datagrams start flowing.
        target.publish(Some(addr));
        listener
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let (n, _from) = listener.recv_from(&mut buf).expect("datagram after publish");
        assert_eq!(datagram::decode(&buf[..n]).unwrap().node_id, "secondary-0");
    }

    /// Dropping the handle stops the thread (no datagrams after drop).
    #[test]
    fn drop_stops_beacon() {
        let listener = StdUdpSocket::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let target = BeaconTarget::new();
        target.publish(Some(addr));

        {
            let _beacon = LivenessBeacon::spawn(
                "secondary-1".into(),
                1,
                Duration::from_millis(30),
                target,
            )
            .unwrap();
            // Let at least one datagram flow.
            listener
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut buf = [0u8; 256];
            listener.recv_from(&mut buf).expect("at least one datagram");
        } // beacon dropped here → thread stops + joins

        // Drain anything already in the socket buffer, then assert quiet.
        listener
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let mut buf = [0u8; 256];
        while listener.recv_from(&mut buf).is_ok() {}
        // A full interval after stop: nothing new.
        std::thread::sleep(Duration::from_millis(120));
        assert!(
            listener.recv_from(&mut buf).is_err(),
            "no datagrams after the beacon is dropped"
        );
    }
}
