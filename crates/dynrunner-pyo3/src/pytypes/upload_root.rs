//! `PyUploadRoot` — the Python-visible surface of the framework mount-root
//! selector [`dynrunner_core::UploadRoot`] (#644).
//!
//! Single concern: cross the `UploadRoot` enum over the FFI boundary. A
//! consumer selects WHICH framework bind-mount root an attached file lands
//! under (`UploadRoot.SOURCE` ⇒ the srcbins mount, `UploadRoot.OUTPUT` ⇒ the
//! shared output mount) — it never spells a host path; the framework owns the
//! host→container mount mapping. The two `From` impls are the SINGLE points the
//! core enum is mapped to/from the pyclass; every other boundary (the
//! files-attach extract, the upload-action bridge) goes through them.

use pyo3::prelude::*;

use dynrunner_core::UploadRoot;

/// Python-visible enum mirroring [`dynrunner_core::UploadRoot`]. Exposed as
/// `dynamic_runner.UploadRoot` (re-exported on the consumer surface). The
/// variants compare by value so a consumer can use a member as a dict key in
/// the Python upload callable (`{UploadRoot.SOURCE: srcbins, ...}[root]`).
#[pyclass(name = "UploadRoot", eq, eq_int, from_py_object)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PyUploadRoot {
    /// The gateway srcbins root (`/app/src-network`) — the default. Exposed to
    /// Python as `UploadRoot.SOURCE` (the Python enum-member convention) while
    /// the Rust variant stays CamelCase.
    #[pyo3(name = "SOURCE")]
    Source,
    /// The shared output root (`/app/out-network`). Exposed as
    /// `UploadRoot.OUTPUT`.
    #[pyo3(name = "OUTPUT")]
    Output,
}

impl From<UploadRoot> for PyUploadRoot {
    fn from(root: UploadRoot) -> Self {
        match root {
            UploadRoot::Source => PyUploadRoot::Source,
            UploadRoot::Output => PyUploadRoot::Output,
        }
    }
}

impl From<PyUploadRoot> for UploadRoot {
    fn from(root: PyUploadRoot) -> Self {
        match root {
            PyUploadRoot::Source => UploadRoot::Source,
            PyUploadRoot::Output => UploadRoot::Output,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_both_variants() {
        // Each core variant maps to its pyclass variant and back, with the
        // default (`Source`) preserved — the SINGLE conversion seam.
        for core in [UploadRoot::Source, UploadRoot::Output] {
            let py: PyUploadRoot = core.into();
            let back: UploadRoot = py.into();
            assert_eq!(core, back, "round-trip must preserve the variant");
        }
        assert_eq!(PyUploadRoot::from(UploadRoot::default()), PyUploadRoot::Source);
    }
}
