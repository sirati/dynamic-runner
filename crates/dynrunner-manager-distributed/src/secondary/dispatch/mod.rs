//! Inbound primary→secondary message dispatch and its supporting helpers.
//!
//! # Sub-module layout
//!
//! - [`router`] — `dispatch_message`, the wide-match router that
//!   handles every `DistributedMessage` variant arriving over the
//!   primary transport. Length exception (~580 lines): the body is
//!   one cohesive concern (route-then-handle); per-arm extraction
//!   would require threading every destructured wire field through a
//!   method signature for no behavioural gain.
//! - [`helpers`] — `apply_cluster_mutations`, `stage_and_register`,
//!   `report_unresolvable_task`. Used by both the router and by
//!   `wait_for_setup` / `handle_initial_assignment` so each rule has
//!   exactly one writer.

mod helpers;
mod router;
