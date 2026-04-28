//! The runner identifier used Rust-side. Post-B2 this is an opaque
//! string (`Arc<str>`); the Python wrapper layer composes the per-task
//! canonical key (e.g. `"binary/platform/compiler/version/opt"` for
//! the tokenizer task) and hands it across.
//!
//! `TokenizerIdentifier` is kept as an alias for one release so the
//! existing call sites that spell out the type don't all have to flip
//! at once. New code should use `RunnerIdentifier` directly.

pub use dynrunner_core::RunnerIdentifier;

/// Deprecated alias for `RunnerIdentifier`. Kept for one release; new
/// code should use `RunnerIdentifier` (or just `Arc<str>`) directly.
#[deprecated(note = "use RunnerIdentifier (Arc<str>) instead")]
pub type TokenizerIdentifier = RunnerIdentifier;
