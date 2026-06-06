//! Peer-info-dir helpers: error mapping + seed-record construction.
//!
//! Both are pure functions called from the run loop; isolated here so
//! the dispatcher body stays focused on orchestration.

use pyo3::prelude::*;

use dynrunner_manager_distributed::cluster_state::ClusterStateSnapshot;
use dynrunner_protocol_primary_secondary::PeerConnectionInfo;
use dynrunner_slurm::{PeerInfoReadDirError, PeerInfoRecord};

use crate::identifier::RunnerIdentifier;

/// Translate a [`PeerInfoReadDirError`] into the right PyError shape
/// for the operator. Single concern: error-mapping at the FFI
/// boundary; keeps the `run()` body focused on orchestration.
pub(super) fn map_read_dir_error(e: PeerInfoReadDirError) -> PyErr {
    match e {
        PeerInfoReadDirError::Io { ref dir, .. } => {
            // io::ErrorKind::NotFound is the most operator-actionable
            // shape (typo'd path, dir not yet created); other I/O
            // errors get the generic OSError shape. The full chain
            // is preserved via the Display impl.
            pyo3::exceptions::PyOSError::new_err(format!(
                "observer late-joiner: peer-info directory unreadable ({dir}): {e}"
            ))
        }
        PeerInfoReadDirError::Parse { ref path, .. } => pyo3::exceptions::PyValueError::new_err(
            format!("observer late-joiner: malformed peer-info file ({path}): {e}"),
        ),
        PeerInfoReadDirError::NoV2Records { ref dir } => {
            // The dir is structurally OK but produced no v2 records
            // — fail loud per the late-joiner design constraint.
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "observer late-joiner: peer-info dir {dir} contains no v2 records — \
                 either the dir is empty, has no `*.info` files, or every file is \
                 legacy v1 (pre-Step-7 wrapper). The late-joiner snapshot RPC \
                 requires the v2 envelope (cert_pem_b64 + quic_port + at least \
                 one of ipv4/ipv6). Re-run the cluster with a Step-7-or-newer \
                 wrapper, or point this flag at a different directory."
            ))
        }
    }
}

/// Convert SLURM-wrapper [`PeerInfoRecord`] entries into the wire-shape
/// [`PeerConnectionInfo`] entries `join_running_cluster` consumes.
///
/// # Filter logic
///
/// `read_peer_info_dir_v2` has already dropped v1 records; we just
/// need to construct the wire struct. Records missing `secondary_id`
/// or `quic_port` (a malformed v2 envelope that nonetheless parsed
/// — e.g. the wrapper crashed between writing the URI line and the
/// envelope tail) are dropped silently here rather than erroring:
/// the joiner's `peer_count() > 0` gate already handles the
/// "no usable seeds survived" case via [`JoinError::NoReachablePeer`].
///
/// [`JoinError::NoReachablePeer`]:
///   dynrunner_protocol_primary_secondary::JoinError::NoReachablePeer
pub(super) fn records_to_seed(records: &[PeerInfoRecord]) -> Vec<PeerConnectionInfo> {
    records
        .iter()
        .filter_map(|r| {
            let secondary_id = r.secondary_id.clone()?;
            let quic_port = r.quic_port?;
            // `cert` is `String` (not `Option<String>`) on the wire
            // frame; v2 records that lack a cert_pem are pre-handshake
            // partial-writes from the wrapper and won't QUIC-dial
            // anyway. Empty string when absent matches the channel
            // transport's test convention and surfaces a CN-mismatch
            // (loud failure) on the dialer, not a silent drop.
            let cert = r.cert_pem.clone().unwrap_or_default();
            Some(PeerConnectionInfo {
                secondary_id,
                cert,
                ipv4: r.ipv4.clone(),
                ipv6: r.ipv6.clone(),
                port: quic_port,
                is_observer: r.is_observer.unwrap_or(false),
            })
        })
        .collect()
}

/// Decode every bootstrap snapshot reply (wire-erased JSON strings) into
/// the typed [`ClusterStateSnapshot`] the cold-join factory restores.
///
/// # Single concern + the BOOTSTRAP-decode fatal discriminator (D-C / D3)
///
/// This is the COLD-JOIN bootstrap decode — the observer requested these
/// snapshots precisely to populate its empty CRDT, so a malformed reply
/// must HARD-FAIL here (an `Err` the constructor propagates with `?`),
/// never be swallowed: continuing on an un-restored empty CRDT would make
/// the observer report a lie (premature run-complete / wrong counts). This
/// is the deliberate counterpart to the STEADY-STATE anti-entropy decode
/// arm (`secondary/dispatch/router.rs` + the observer's `on_cluster_snapshot`),
/// which is WARN-and-keep because the AE-3 recovery cadence re-pulls a
/// fresh snapshot. The discriminator is WHICH FUNCTION the decode lives in:
/// this bootstrap function = fatal; the steady-state loop arm = WARN.
pub(super) fn decode_bootstrap_snapshots(
    snapshot_jsons: &[String],
) -> PyResult<Vec<ClusterStateSnapshot<RunnerIdentifier>>> {
    snapshot_jsons
        .iter()
        .map(|snapshot_json| {
            serde_json::from_str(snapshot_json).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "observer late-joiner: failed to decode ClusterStateSnapshot \
                     from join_running_cluster reply: {e}"
                ))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_slurm::{PeerInfoBuilder, parse_peer_info};

    /// Construct a `PeerInfoRecord` end-to-end via the SLURM-wrapper
    /// public surface (`Builder::format` then `parse`). Goes through
    /// the same bytes the on-disk file carries so the test exercises
    /// the same conversion path the late-joiner runs in production.
    fn record_from_builder(b: PeerInfoBuilder) -> PeerInfoRecord {
        parse_peer_info(&b.format()).expect("builder output round-trips through parse")
    }

    /// `records_to_seed` drops records missing the snapshot-RPC-required
    /// fields (`secondary_id` / `quic_port`) rather than producing a
    /// half-filled `PeerConnectionInfo` that would CN-fail on dial.
    /// The "happy" record passes through with every field carried over.
    #[test]
    fn records_to_seed_drops_records_missing_required_fields() {
        let happy = record_from_builder(
            PeerInfoBuilder::new("compute1", 40001)
                .secondary_id("sec-1")
                .cert_pem("CERT-PEM-HERE")
                .ipv4("10.0.0.1")
                .quic_port(51200)
                .is_observer(false),
        );
        // Missing quic_port: the dial would have nowhere to go.
        let no_port = record_from_builder(
            PeerInfoBuilder::new("compute2", 40002)
                .secondary_id("sec-2")
                .cert_pem("CERT-PEM-HERE"),
        );
        // Missing secondary_id: the snapshot RPC envelope keys on
        // the responder's secondary_id; without it, the joiner
        // can't construct the unicast Address::Peer target.
        let no_id = record_from_builder(PeerInfoBuilder::new("compute3", 40003).quic_port(51202));

        let seed = records_to_seed(&[happy, no_port, no_id]);
        assert_eq!(seed.len(), 1);
        assert_eq!(seed[0].secondary_id, "sec-1");
        assert_eq!(seed[0].port, 51200);
        assert_eq!(seed[0].cert, "CERT-PEM-HERE");
        assert_eq!(seed[0].ipv4.as_deref(), Some("10.0.0.1"));
        assert!(!seed[0].is_observer);
    }

    /// `records_to_seed` carries `is_observer` through verbatim — the
    /// joiner's election filter (Step 7) needs the flag on every
    /// seed entry so it doesn't pick an observer as the responder
    /// preference.
    #[test]
    fn records_to_seed_preserves_is_observer_flag() {
        let observer = record_from_builder(
            PeerInfoBuilder::new("compute1", 40001)
                .secondary_id("obs-1")
                .cert_pem("CERT-OBS")
                .quic_port(51200)
                .is_observer(true),
        );
        let regular = record_from_builder(
            PeerInfoBuilder::new("compute2", 40002)
                .secondary_id("reg-1")
                .cert_pem("CERT-REG")
                .quic_port(51201)
                .is_observer(false),
        );

        let mut seed = records_to_seed(&[observer, regular]);
        seed.sort_by(|a, b| a.secondary_id.cmp(&b.secondary_id));
        assert_eq!(seed.len(), 2);
        assert_eq!(seed[0].secondary_id, "obs-1");
        assert!(seed[0].is_observer);
        assert_eq!(seed[1].secondary_id, "reg-1");
        assert!(!seed[1].is_observer);
    }

    /// Records missing the optional `is_observer` envelope key
    /// default to `false` — matches the pre-Step-7 v2 senders
    /// (cf. PeerConnectionInfo's `#[serde(default)]`).
    #[test]
    fn records_to_seed_defaults_is_observer_to_false() {
        let r = record_from_builder(
            PeerInfoBuilder::new("compute1", 40001)
                .secondary_id("sec-1")
                .cert_pem("CERT")
                .quic_port(51200),
            // no is_observer set
        );
        let seed = records_to_seed(&[r]);
        assert_eq!(seed.len(), 1);
        assert!(!seed[0].is_observer);
    }

    /// BOOTSTRAP-decode fatal (D-C / D3): a malformed bootstrap snapshot
    /// reply HARD-FAILS (the constructor's `?` propagates the `Err`). This
    /// is the counterpart to the steady-state WARN arm — a cold-join MUST
    /// hard-fail on a corrupt INITIAL snapshot rather than observe an empty
    /// CRDT and report a lie.
    #[test]
    fn decode_bootstrap_snapshots_hard_fails_on_malformed() {
        let result = decode_bootstrap_snapshots(&["{not valid json".to_string()]);
        let err = result.expect_err("a malformed bootstrap snapshot must hard-fail");
        assert!(
            err.to_string()
                .contains("failed to decode ClusterStateSnapshot"),
            "the bootstrap-fatal error must name the decode failure: {err}"
        );
    }

    /// The happy path: well-formed bootstrap snapshot JSON decodes into the
    /// typed snapshot the cold-join factory restores.
    #[test]
    fn decode_bootstrap_snapshots_round_trips_valid_payload() {
        use dynrunner_manager_distributed::cluster_state::ClusterState;
        let json = serde_json::to_string(&ClusterState::<RunnerIdentifier>::new().snapshot())
            .expect("snapshot serializes");
        let snaps = decode_bootstrap_snapshots(&[json]).expect("valid snapshot decodes");
        assert_eq!(snaps.len(), 1);
    }
}
