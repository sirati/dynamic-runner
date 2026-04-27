use std::collections::HashMap;

use pyo3::prelude::*;

use db_comm_api_base::{ResourceKind, ResourceMap as RustResourceMap};

/// Python-facing typed resource map: a kind-name → amount (u64) mapping.
///
/// Construct from a Python dict: `ResourceMap({'memory': 1024**3})`. The
/// kinds are opaque strings; the runner treats every kind interchangeably.
#[pyclass(name = "ResourceMap")]
#[derive(Clone, Debug, Default)]
pub(crate) struct PyResourceMap {
    pub(crate) inner: HashMap<String, u64>,
}

#[pymethods]
impl PyResourceMap {
    #[new]
    #[pyo3(signature = (values = None))]
    fn new(values: Option<HashMap<String, u64>>) -> Self {
        Self {
            inner: values.unwrap_or_default(),
        }
    }

    fn get(&self, kind: &str) -> Option<u64> {
        self.inner.get(kind).copied()
    }

    fn set(&mut self, kind: String, amount: u64) {
        self.inner.insert(kind, amount);
    }

    fn __contains__(&self, kind: &str) -> bool {
        self.inner.contains_key(kind)
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __repr__(&self) -> String {
        format!("ResourceMap({:?})", self.inner)
    }

    /// Materialise as a Python dict for downstream code that wants the
    /// concrete mapping rather than the wrapper.
    fn as_dict(&self) -> HashMap<String, u64> {
        self.inner.clone()
    }
}

impl PyResourceMap {
    /// Convert to the internal Rust representation.
    pub(crate) fn to_rust(&self) -> RustResourceMap {
        self.inner
            .iter()
            .map(|(k, v)| (ResourceKind::new(k.as_str()), *v))
            .collect()
    }

    /// Build from a single (kind, amount) pair — convenience for callers
    /// migrating from the legacy `max_memory: u64` field.
    pub(crate) fn from_single(kind: &str, amount: u64) -> Self {
        let mut inner = HashMap::new();
        inner.insert(kind.to_owned(), amount);
        Self { inner }
    }
}
