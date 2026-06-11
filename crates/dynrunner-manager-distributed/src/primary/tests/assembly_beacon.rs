//! Production replay (asm-dataset LMU bring-up): the primary's
//! `wait_for_connections` straggler window must not be SILENT.
//!
//! In that run the primary spent 11:06:31 → 11:16:10 waiting out its
//! full 600s `connect_timeout` for 5 lost welcomes while emitting
//! NOTHING the 10 welcomed secondaries could read as "the primary is
//! alive and assembling" — their setup deadlines all fired first and
//! the quorum-proceed landed on a dead fleet. `wait_for_connections`
//! was the ONE waiting state without the jittered anti-entropy digest
//! beacon every sibling wait already runs; this test pins the beacon
//! (the setup-liveness signal the secondaries' re-armable deadline
//! keys on — see `secondary::setup_deadline`).
//!
//! REVERT-CHECK: pre-fix the connect loop had only the recv + deadline
//! arms — zero broadcasts leave this wire for the whole straggler
//! window, and the digest count below stays 0.

use super::*;

use dynrunner_protocol_primary_secondary::MessageType;

/// A primary waiting for welcomes (none ever arrive) must broadcast its
/// setup-liveness digest on the standard jittered anti-entropy cadence
/// (15–25s) — ≥2 broadcasts inside 60s of virtual waiting.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn wait_for_connections_broadcasts_setup_liveness_beacon() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // One registered secondary outbox (the broadcast fan-out
            // target); the inbound is held open but never fed — no
            // welcome ever arrives, the pure straggler-window shape.
            let (sec_tx, mut sec_rx) = tokio_mpsc::unbounded_channel();
            let (_inbound_hold, inbound_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing: HashMap<
                String,
                tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
            > = HashMap::new();
            outgoing.insert("sec-0".to_string(), sec_tx);
            let transport = ChannelPeerTransport::from_raw_channels(
                "setup".into(),
                outgoing,
                inbound_rx,
            );

            let config = PrimaryConfig {
                num_secondaries: 1,
                connect_timeout: Duration::from_secs(120),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let driver = async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let mut digests = 0usize;
                while let Ok(msg) = sec_rx.try_recv() {
                    if matches!(msg.msg_type(), MessageType::StateDigest) {
                        digests += 1;
                    }
                }
                assert!(
                    digests >= 2,
                    "an assembling primary must broadcast its setup-liveness \
                     digest on the anti-entropy cadence while waiting for \
                     welcomes (saw {digests} digest broadcasts in 60s — the \
                     asm-dataset LMU silent-straggler-window shape)"
                );
            };

            let mut no_commands = None;
            tokio::select! {
                res = primary.wait_for_connections(&mut no_commands) => {
                    panic!(
                        "wait_for_connections must still be waiting inside its \
                         120s straggler window, got {res:?}"
                    );
                }
                () = driver => { /* beacon observed while still waiting */ }
            }
        })
        .await;
}
