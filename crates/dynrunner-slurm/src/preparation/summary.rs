//! Honest tunnel-setup outcome summary (#278).
//!
//! Single concern: turn the preparation result — the populated
//! `secondary_id -> tunnel_port` map plus the expected cohort size —
//! into ONE truthful, operator-facing summary value. Every reporter
//! (the Rust `tracing` emit in
//! [`SlurmPreparation::setup_ssh_tunnels`](super::pipeline::SlurmPreparation::setup_ssh_tunnels)
//! and the Python-logger emit at the pyo3 pipeline boundary) renders
//! the SAME [`TunnelSetupSummary`], so a partial fleet can never again
//! be announced as "All N SSH tunnels established" — the pre-fix
//! defect, where the pyo3 layer formatted the headline from
//! `num_secondaries` alone without consulting what the setup actually
//! verified ([`gather_under_deadline`](super::pipeline::gather_under_deadline)
//! allows K-of-N partial success by design).
//!
//! The summary derives the MISSING ids from the canonical
//! `secondary-{i}` naming — owned here by [`secondary_id`], the same
//! helper the setup loop uses to spawn its watchers, so the expected
//! set and the established set can never drift on naming.

use std::collections::HashMap;
use std::fmt;

/// The canonical per-secondary identifier: `secondary-{index}`.
///
/// Single source of truth for the naming convention shared by the
/// watcher loop (which spawns one establishment per id), the info-file
/// paths (`connection_info/<id>.info`), and the summary's expected-id
/// enumeration.
pub fn secondary_id(index: usize) -> String {
    format!("secondary-{index}")
}

/// The truthful outcome of a tunnel-setup pass: how many of the
/// expected tunnels were actually VERIFIED established (committed to
/// the store after the alive-gate), and which expected ids are
/// missing.
///
/// `Display` renders the operator headline:
/// * complete  — `All 3 SSH tunnels established`
/// * partial   — `2/3 SSH tunnels established; missing: secondary-2`
///
/// The caller picks the log level off [`Self::is_complete`] (INFO when
/// complete, WARN when any tunnel failed) — the summary itself carries
/// no logging dependency, keeping it pure and renderer-agnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TunnelSetupSummary {
    /// Number of tunnels verified established (the port map's size).
    pub established: usize,
    /// Expected cohort size (`num_secondaries`).
    pub expected: usize,
    /// Expected ids with NO established tunnel, in index order.
    pub missing: Vec<String>,
}

impl TunnelSetupSummary {
    /// Build the summary from the `secondary_id -> tunnel_port` map the
    /// setup returned and the expected cohort size. Missing ids are the
    /// canonical `secondary-{i}` names (i in `0..expected`) absent from
    /// the map, in index order.
    pub fn new(port_map: &HashMap<String, u16>, expected: usize) -> Self {
        let missing: Vec<String> = (0..expected)
            .map(secondary_id)
            .filter(|id| !port_map.contains_key(id))
            .collect();
        Self {
            established: expected - missing.len(),
            expected,
            missing,
        }
    }

    /// Whether every expected tunnel was verified established.
    pub fn is_complete(&self) -> bool {
        self.missing.is_empty()
    }
}

impl fmt::Display for TunnelSetupSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_complete() {
            write!(f, "All {} SSH tunnels established", self.expected)
        } else {
            write!(
                f,
                "{}/{} SSH tunnels established; missing: {}",
                self.established,
                self.expected,
                self.missing.join(", ")
            )
        }
    }
}
