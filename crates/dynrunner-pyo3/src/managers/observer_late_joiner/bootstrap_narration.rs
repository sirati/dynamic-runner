//! Late-joiner bootstrap-milestone narration on the important
//! (LLM-wake) channel.
//!
//! # Concern
//!
//! Single concern: own the late-joiner's bootstrap *milestone* lines and
//! the ONE thing every emit shares — the [`IMPORTANT_TARGET`] tracing
//! target. A late-joining observer launched under `--important-stdio-only`
//! otherwise boots silently until it seats; these emits surface "what it
//! is doing" at each bootstrap boundary, exactly like the submitter's
//! `run_pipeline`/`preparation` milestones do for the SLURM bring-up.
//!
//! # Why a module, not inline `tracing::info!`s
//!
//! The emit sites live across two files (`gateway_mode.rs` does the
//! gateway connect / fetch / tunnel-dial; `run.rs` does the CRDT-snapshot
//! wait). Inlining `tracing::info!(target: IMPORTANT_TARGET, …)` at each
//! would scatter the importance-target literal + the message text across
//! both — and leave the emit untestable without a live gateway + mesh.
//! Folding them here gives ONE owner of the milestone text + target, and
//! lets the unit tests below assert (via a thread-local importance-channel
//! capture) that each milestone lands on [`IMPORTANT_TARGET`] in the
//! documented bootstrap order — without standing up SSH or a peer mesh.
//! The call sites become trivial one-liners that pass the data they
//! already hold at the seam and know nothing of the target or format.
//!
//! # The bootstrap sequence (and which mode emits which)
//!
//! 1. [`connecting_to_gateway`] → [`fetched_peer_records`] — GATEWAY mode
//!    only (`gateway_mode::acquire_gateway_seed`). LOCAL mode reads the
//!    peer-info dir directly with no gateway, so it has nothing to narrate
//!    here.
//! 2. [`dialing_compute_peers`] → [`compute_peers_connected`] — GATEWAY
//!    mode only (`gateway_mode::acquire_over_connected_gateway`): the
//!    per-peer `ssh -L` local-forward cohort.
//! 3. [`waiting_for_crdt`] → [`crdt_snapshot_received`] — BOTH modes
//!    (`run.rs`, around `join_running_cluster`): the bootstrap snapshot
//!    rendezvous runs identically regardless of how the seed was acquired.

use std::time::Duration;

use dynrunner_core::IMPORTANT_TARGET;

/// Milestone 1a (GATEWAY mode): the gateway connect / info-dir fetch
/// stage is starting. Carries the gateway host so the operator sees
/// exactly which gateway is being reached.
pub(super) fn connecting_to_gateway(host: &str) {
    tracing::info!(
        target: IMPORTANT_TARGET,
        "connecting to gateway {host} to fetch peer-info dir",
    );
}

/// Milestone 1b (GATEWAY mode): the info-dir mirror completed; `count`
/// peer records were fetched from the gateway-side dir.
pub(super) fn fetched_peer_records(count: usize) {
    tracing::info!(
        target: IMPORTANT_TARGET,
        "fetched {count} peer records from gateway",
    );
}

/// Milestone 2a (GATEWAY mode): about to bring up the per-peer
/// `ssh -L` local-forward cohort. Carries the requested tunnel count.
pub(super) fn dialing_compute_peers(requested: usize) {
    tracing::info!(
        target: IMPORTANT_TARGET,
        "dialing compute peers: establishing {requested} tunnel(s)",
    );
}

/// Milestone 2b (GATEWAY mode): the local-forward cohort settled;
/// `connected` of `requested` tunnels established (the registry's
/// established-endpoint count — an untunneled peer keeps its recorded
/// address and is WARNed about separately).
pub(super) fn compute_peers_connected(connected: usize, requested: usize) {
    tracing::info!(
        target: IMPORTANT_TARGET,
        "dialing compute peers: connected {connected}/{requested}",
    );
}

/// Milestone 3a (BOTH modes): the cluster-snapshot bootstrap window is
/// opening. Carries the connect/snapshot budget so the operator knows
/// how long the join may block before it errors loudly.
pub(super) fn waiting_for_crdt(budget: Duration) {
    tracing::info!(
        target: IMPORTANT_TARGET,
        "waiting for CRDT snapshot (deadline {}s)",
        budget.as_secs(),
    );
}

/// Milestone 3b (BOTH modes): the bootstrap snapshot(s) arrived and
/// decoded. Reports the restored task count and the fleet size (the
/// number of peers the snapshot's capability roster knows) — the
/// observer's first concrete picture of the cluster it joined.
pub(super) fn crdt_snapshot_received(tasks: usize, fleet: usize) {
    tracing::info!(
        target: IMPORTANT_TARGET,
        "CRDT snapshot received ({tasks} tasks, fleet {fleet})",
    );
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use dynrunner_core::IMPORTANT_TARGET;

    use super::*;

    // ── tracing capture (importance channel) ──
    //
    // Mirrors the observer failure-response tests' `ImportantCapture`:
    // a thread-local `set_default` subscriber + a tiny capture layer
    // records the `message` field of every `dynrunner_important` event,
    // so the milestone emits are assertable without a live gateway/mesh.

    /// Captures the `message` field of every `dynrunner_important` event.
    struct ImportantCapture {
        records: Arc<Mutex<Vec<String>>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ImportantCapture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event.metadata().target() != IMPORTANT_TARGET {
                return;
            }
            struct Visitor(String);
            impl tracing::field::Visit for Visitor {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    if field.name() == "message" {
                        self.0 = value.to_string();
                    }
                }
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        self.0 = format!("{value:?}");
                    }
                }
            }
            let mut visitor = Visitor(String::new());
            event.record(&mut visitor);
            if let Ok(mut buf) = self.records.lock() {
                buf.push(visitor.0);
            }
        }
    }

    /// Install a thread-local importance-channel capture for the rest of
    /// the current thread's scope. Returns the shared buffer + the guard
    /// (drop to restore the prior subscriber).
    fn capture_important() -> (Arc<Mutex<Vec<String>>>, tracing::dispatcher::DefaultGuard) {
        use tracing_subscriber::layer::SubscriberExt;
        let records: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let layer = ImportantCapture {
            records: Arc::clone(&records),
        };
        let subscriber = tracing_subscriber::Registry::default().with(layer);
        let guard = tracing::subscriber::set_default(subscriber);
        (records, guard)
    }

    /// The three owner-specified stage lines emit IN ORDER and on the
    /// importance target on a successful gateway-mode join. Drives the
    /// six milestone fns in the documented bootstrap sequence — the same
    /// order `gateway_mode.rs` + `run.rs` invoke them — and asserts the
    /// captured importance-channel buffer.
    #[test]
    fn gateway_bootstrap_milestones_emit_in_order_on_important_target() {
        let (records, _guard) = capture_important();

        // 1. gateway connect + fetch.
        connecting_to_gateway("gw.example.org");
        fetched_peer_records(3);
        // 2. tunnel dial.
        dialing_compute_peers(3);
        compute_peers_connected(2, 3);
        // 3. CRDT snapshot wait.
        waiting_for_crdt(Duration::from_secs(60));
        crdt_snapshot_received(7, 4);

        let lines = records.lock().unwrap().clone();
        // Every milestone landed on the importance target (the capture
        // layer admits ONLY IMPORTANT_TARGET events, so a count match
        // proves the target).
        assert_eq!(lines.len(), 6, "all six milestones reach the importance channel: {lines:?}");

        // The three owner-named stages appear in order, each carrying its
        // payload.
        assert!(lines[0].contains("connecting to gateway"), "stage 1: {lines:?}");
        assert!(lines[0].contains("gw.example.org"), "stage 1 names the gateway: {lines:?}");
        assert!(lines[1].contains("fetched 3 peer records"), "stage 1 follow-up: {lines:?}");

        assert!(lines[2].contains("dialing compute peers"), "stage 2: {lines:?}");
        assert!(lines[2].contains("3"), "stage 2 names the requested count: {lines:?}");
        assert!(
            lines[3].contains("connected 2/3"),
            "stage 2 follow-up names connected/requested: {lines:?}"
        );

        assert!(lines[4].contains("waiting for CRDT snapshot"), "stage 3: {lines:?}");
        assert!(lines[4].contains("60s"), "stage 3 names the deadline budget: {lines:?}");
        assert!(
            lines[5].contains("CRDT snapshot received") && lines[5].contains("7 tasks"),
            "stage 3 follow-up names the restored task count + fleet: {lines:?}"
        );
        assert!(lines[5].contains("fleet 4"), "stage 3 follow-up names the fleet size: {lines:?}");
    }

    /// LOCAL mode (no gateway) emits only the stage-3 CRDT-wait pair —
    /// there is no gateway connect or tunnel dial to narrate. Guards
    /// against a future regression that bolts gateway lines onto the
    /// local path.
    #[test]
    fn local_mode_emits_only_crdt_stage() {
        let (records, _guard) = capture_important();
        waiting_for_crdt(Duration::from_secs(60));
        crdt_snapshot_received(0, 1);
        let lines = records.lock().unwrap().clone();
        assert_eq!(lines.len(), 2, "local mode narrates only the CRDT stage: {lines:?}");
        assert!(lines[0].contains("waiting for CRDT snapshot"));
        assert!(lines[1].contains("CRDT snapshot received"));
    }
}
