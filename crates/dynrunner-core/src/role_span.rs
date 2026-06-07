//! The cross-crate role-tagging tracing span contract.
//!
//! Single concern: own the ONE span name + the two role tokens that tag a
//! coordinator's run future with the role it runs (primary vs secondary),
//! so the `dynrunner-pyo3` full-log layer can route every event a task
//! emits to a per-role file.
//!
//! There are no other spans in the runtime, and the explicit-target events
//! are concern-keyed (not role-keyed), so a single role span entered at
//! each coordinator's run entry is what attributes EVERY event — including
//! the explicit-target ones — to its role. A promoted host's primary and
//! its secondary are separate `spawn_local` tasks, so each carries its
//! own span context and attribution stays correct across promotion.
//!
//! The span NAME carries the role (not a field value): the name is
//! intrinsic span metadata, always present when walking the event scope in
//! a layer filter, so the routing layer needs no field-value-recording
//! machinery. The coordinators additionally attach a `kind`/`id` field for
//! human-readable verbose output; those fields are display sugar, the name
//! is the routing key.

/// Span name marking a coordinator's run future as the PRIMARY role.
/// Read by the `dynrunner-pyo3` per-role full-log routing layer.
pub const PRIMARY_ROLE_SPAN: &str = "dynrunner_role_primary";

/// Span name marking a coordinator's run future as the SECONDARY role.
/// Read by the `dynrunner-pyo3` per-role full-log routing layer.
pub const SECONDARY_ROLE_SPAN: &str = "dynrunner_role_secondary";

/// Span name marking a coordinator's run future as the OBSERVER role.
/// Read by the `dynrunner-pyo3` per-role full-log routing layer.
///
/// A relocated submitter steps down from primary to a standalone observer
/// on the SAME host (the bootstrap-relocation observer tail); its observer
/// run future carries this span so its events route to a dedicated
/// `observer.log`, keeping the relocated submitter's actions debuggable
/// after it hands the primary role to a compute peer.
pub const OBSERVER_ROLE_SPAN: &str = "dynrunner_role_observer";
