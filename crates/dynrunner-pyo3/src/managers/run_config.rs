//! Node-local COMPLETE run-config Namespace — the single source of truth
//! shared by the two consumers that need the full argparse namespace on a
//! promotion-eligible node.
//!
//! # Concern
//!
//! ONE concern: "the node-local COMPLETE run-config Namespace, resolved
//! exactly once from the DELIVERED `forwarded_argv`, observed identically by
//! every consumer that needs the full namespace." Two consumers:
//!   * the run-config FINALIZE (worker `cmd_args` rebuild — fires on the
//!     `AwaitingPrimary → Configuring` transition of a PLAIN secondary, after
//!     the post-welcome push delivers the argv);
//!   * the promoted-primary DISCOVERY driver (`discover_items` — fires at
//!     promotion on a relocate-target, which NEVER receives a primary's
//!     PeerInfo before it is promoted, so its finalize has NOT fired yet).
//!
//! Because the two consumers fire on DIFFERENT lifecycle edges (and on the
//! relocate-target ONLY discovery fires before promotion), the namespace is
//! resolved LAZILY by whichever consumer runs first. The resolved value is
//! cached in a shared cell so the second consumer observes the SAME namespace
//! — no second reparse, no divergence between the worker `cmd_args` and the
//! discovery selection flags.
//!
//! # Module boundary
//!
//! Owns: the resolved-namespace cell + the lazy reparse driver. Does NOT own
//! the reparse LOGIC (the consumer's `finalize_run_config(delivered) ->
//! argparse.Namespace` callable — a pure framework-parser reparse) nor the
//! delivered argv (the secondary coordinator's shared `run_config_handle()`).
//! Callers see: "give me the complete namespace under the GIL"
//! ([`SharedRunConfig::resolve_under_gil`]); they never reach the cell or the
//! callable directly.
//!
//! # Two construction modes
//!
//! * [`SharedRunConfig::deferred`] — the SLURM secondary path: the boot
//!   namespace is INCOMPLETE (framework-regenerated flags only — the
//!   consumer's selection flags + `--skip-existing` arrive over the mesh as
//!   `forwarded_argv`), so the complete namespace is produced by reparsing the
//!   delivered argv through the consumer's finalize callable.
//! * [`SharedRunConfig::pre_resolved`] — the in-process `--multi-computer
//!   local` path: every node shares the submitter's EAGERLY-parsed namespace
//!   directly (no cold-start fetch / deferred reparse), so the complete
//!   namespace is already in hand and seeds the cell at construction.

use std::sync::{Arc, Mutex};

use pyo3::prelude::*;

/// The shared node-local complete run-config namespace + its lazy reparse
/// driver. `Clone` is GIL-independent (every field is `Arc`-backed), so the
/// handle is cloned on the GIL thread into both consumers' closures and read
/// back under a fresh `Python::attach` off the runtime thread.
#[derive(Clone)]
pub(crate) struct SharedRunConfig {
    /// The resolved complete namespace — the SINGLE SOURCE OF TRUTH. `None`
    /// until the first consumer resolves it (deferred mode); pre-populated at
    /// construction in pre-resolved mode.
    cell: Arc<Mutex<Option<Py<PyAny>>>>,
    /// The consumer's `finalize_run_config(delivered_argv) ->
    /// argparse.Namespace` reparse callable. `None` in pre-resolved mode (the
    /// cell is already populated; no reparse can ever be needed). `Arc` so the
    /// struct's `Clone` does not bump the Python refcount (GIL-independent).
    reparse: Option<Arc<Py<PyAny>>>,
    /// The delivered `forwarded_argv` (the secondary coordinator's shared
    /// `run_config_handle()`), read at resolve time so the reparse sees the
    /// post-push value. Unused in pre-resolved mode.
    delivered_argv: Arc<Mutex<Vec<String>>>,
}

impl SharedRunConfig {
    /// Deferred mode (SLURM secondary): the complete namespace is produced on
    /// first resolve by reparsing the delivered argv through `reparse`.
    pub(crate) fn deferred(
        reparse: Py<PyAny>,
        delivered_argv: Arc<Mutex<Vec<String>>>,
    ) -> Self {
        Self {
            cell: Arc::new(Mutex::new(None)),
            reparse: Some(Arc::new(reparse)),
            delivered_argv,
        }
    }

    /// Pre-resolved mode (in-process `--multi-computer local`): the complete
    /// namespace is already parsed (the submitter's own namespace, shared by
    /// every node), so it seeds the cell directly and no reparse callable is
    /// ever consulted.
    pub(crate) fn pre_resolved(namespace: Py<PyAny>) -> Self {
        Self {
            cell: Arc::new(Mutex::new(Some(namespace))),
            reparse: None,
            // Unused in pre-resolved mode — an empty handle keeps the field
            // shape uniform without a second `Option` layer.
            delivered_argv: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Resolve (or return the cached) complete namespace under the GIL.
    /// Idempotent: the first call in deferred mode reparses the delivered argv
    /// and caches the result; every later call (and every call in pre-resolved
    /// mode) returns a refcount-bumped clone of the cached namespace, so the
    /// finalize's worker `cmd_args` and the discovery driver's selection flags
    /// read from ONE namespace.
    ///
    /// Returns a `String` error (the secondary aborts the run on it) so no
    /// `PyErr` crosses a `Send` boundary in the calling closure.
    pub(crate) fn resolve_under_gil(&self, py: Python<'_>) -> Result<Py<PyAny>, String> {
        let mut guard = self
            .cell
            .lock()
            .map_err(|_| "run-config namespace cell mutex poisoned".to_string())?;
        if let Some(existing) = guard.as_ref() {
            return Ok(existing.clone_ref(py));
        }
        // Deferred mode, first resolve: reparse the delivered argv through the
        // consumer finalize. `reparse == None` here is a programmer error —
        // pre-resolved mode always populates the cell at construction.
        let reparse = self.reparse.as_ref().ok_or_else(|| {
            "run-config namespace is unresolved and no reparse callable was \
             registered (pre-resolved mode must populate the cell at \
             construction)"
                .to_string()
        })?;
        let delivered = self
            .delivered_argv
            .lock()
            .map_err(|_| "delivered forwarded_argv mutex poisoned".to_string())?
            .clone();
        let namespace = reparse
            .bind(py)
            .call1((delivered,))
            .map_err(|e| format!("finalize_run_config(delivered_argv) raised: {e}"))?
            .unbind();
        *guard = Some(namespace.clone_ref(py));
        Ok(namespace)
    }
}
