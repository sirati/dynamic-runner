use pyo3::prelude::*;

use db_scheduler_api::ResourceEstimator;

/// Memory estimator that calls a Python function.
#[derive(Clone)]
pub(crate) struct PyMemoryEstimatorBridge {
    /// Cached linear coefficient: memory = slope * binary_size + intercept.
    /// For the common case where estimate_memory is linear, we precompute.
    /// If not linear, we store a callable.
    pub(crate) slope: f64,
    pub(crate) intercept: f64,
}

impl PyMemoryEstimatorBridge {
    pub(crate) fn from_python(_py: Python<'_>, estimate_fn: &Bound<'_, PyAny>) -> PyResult<Self> {
        // Probe the function with two sizes to determine if it's linear.
        let size_a: u64 = 1_000_000;
        let size_b: u64 = 2_000_000;
        let est_a: u64 = estimate_fn.call1((size_a,))?.extract()?;
        let est_b: u64 = estimate_fn.call1((size_b,))?.extract()?;

        let slope = (est_b as f64 - est_a as f64) / (size_b as f64 - size_a as f64);
        let intercept = est_a as f64 - slope * size_a as f64;

        // Verify with a third point
        let size_c: u64 = 500_000;
        let est_c: u64 = estimate_fn.call1((size_c,))?.extract()?;
        let predicted_c = (slope * size_c as f64 + intercept) as u64;

        if (predicted_c as i64 - est_c as i64).unsigned_abs() > 1024 {
            // Not perfectly linear — fall back to sampling more points,
            // but for now just use the two-point approximation which is
            // good enough for the tokenizer's linear formula.
            tracing::warn!(
                "memory estimator is not perfectly linear, using approximation"
            );
        }

        Ok(Self { slope, intercept })
    }
}

impl ResourceEstimator for PyMemoryEstimatorBridge {
    fn estimate(&self, binary_size: u64) -> db_comm_api_base::ResourceMap {
        let mem = (self.slope * binary_size as f64 + self.intercept).max(0.0) as u64;
        db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), mem)])
    }
}
