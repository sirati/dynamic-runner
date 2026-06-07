//! The bootstrap/submitter host's mesh node-id.
//!
//! Single concern: own the ONE string the submitter host is keyed by in
//! the peer mesh, so the submitter coordinator's `node_id`, the submitter
//! mesh transport's local id, and every secondary's `bootstrap_primary_id`
//! egress hint all agree on it from one definition.
//!
//! It is a label for the SUBMITTER HOST — the process that launches the
//! run and hosts the bootstrap primary before relocating its primary role
//! to a compute peer (architectural invariant #4: the primary is never
//! pinned to the submitter). It is NOT the primary ROLE (which any peer may
//! hold and which the routing layer resolves via `current_primary()` / the
//! role table, never by this literal), so it is named `"setup"` rather than
//! `"primary"`.

/// The mesh node-id of the bootstrap/submitter host. The submitter
/// coordinator's `node_id`, the submitter mesh transport's local id, and
/// every secondary's `bootstrap_primary_id` MUST agree on this exact
/// string for the reverse-tunnel link to resolve.
pub const SETUP_NODE_ID: &str = "setup";
