//! The runner identifier used Rust-side. Post-B2 this is an opaque
//! string (`Arc<str>`); the Python wrapper layer composes the per-task
//! canonical key (e.g. `"binary/platform/compiler/version/opt"` for
//! the tokenizer task) and hands it across.

pub use dynrunner_core::RunnerIdentifier;
