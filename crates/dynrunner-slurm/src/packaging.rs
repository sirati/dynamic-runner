//! Container-image build + transfer abstraction.
//!
//! Mirrors the Python `packaging.podman.PodmanPackaging.build_images`
//! contract (and the `packaging.podman.PodmanImageMetadata` shape)
//! so a SLURM job manager can ask "build the image and put it on the
//! gateway" without owning the build technology.
//!
//! The actual nix-build invocation currently lives in Python. This
//! crate intentionally only defines the trait surface and the
//! delegation entry point on `SlurmJobManager`; a future PyO3-bridged
//! impl will satisfy the trait by marshalling the call back to the
//! Python `PodmanPackaging` (one PyO3 boundary, no logic
//! duplication).
//!
//! Open question for the PyO3 impl (deferred to that unit):
//!
//! * Path-expansion of the returned `remote_path` against the
//!   gateway's tilde-home is done Python-side today (see
//!   `SlurmJobManager._expanded_remote_path` in `job_manager.py`).
//!   When the bridge lands, decide whether the trait impl performs
//!   that normalisation before returning, or whether the Rust caller
//!   gets the raw `~/...` path and a future
//!   `Gateway::expand_remote_home` primitive does the substitution
//!   uniformly.

use std::path::{Path, PathBuf};

use dynrunner_gateway::traits::Gateway;

/// Metadata for the single remote Podman image artifact.
///
/// `uploaded` reflects whether the local hash matched the remote
/// marker (i.e. the upload was a cache hit and skipped). Mirrors
/// `dynamic_runner.packaging.podman.PodmanImageMetadata`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodmanImageMetadata {
    /// Path to the image tarball on the gateway.
    pub remote_path: PathBuf,
    /// SHA-256 of the image tarball, hex-encoded.
    pub image_hash: String,
    /// True when the upload actually ran (i.e. cache miss).
    pub uploaded: bool,
}

/// Errors surfaced from a packaging implementation.
#[derive(Debug, thiserror::Error)]
pub enum PackagingError {
    #[error("image build failed: {0}")]
    BuildFailed(String),
    #[error("image transfer failed: {0}")]
    TransferFailed(String),
    #[error("packaging backend error: {0}")]
    Backend(String),
}

/// Build a container image locally and transfer the artifact to the
/// gateway.
///
/// Implementations own the build technology (nix, docker build, …)
/// and the transfer policy (one-shot scp, layered blob cache, …).
/// The trait is generic over the gateway type so a Rust-native impl
/// can call gateway methods without erasing them through `dyn` (the
/// `Gateway` trait uses RPIT and is not object-safe).
pub trait PodmanPackaging<G: Gateway>: Send + Sync {
    /// Build the image and place its tarball under `output_dir` on
    /// the gateway. `local_project_root` is the source tree the
    /// build runs against (e.g. the directory containing
    /// `flake.nix`).
    fn build_images(
        &self,
        gateway: &G,
        local_project_root: &Path,
        output_dir: &Path,
    ) -> impl std::future::Future<Output = Result<PodmanImageMetadata, PackagingError>> + Send;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use dynrunner_gateway::local::LocalGateway;

    /// In-memory packaging stub that records its inputs and returns a
    /// caller-supplied `PodmanImageMetadata`. Verifies the trait is
    /// callable without dragging nix or a remote gateway into the
    /// test path.
    pub(crate) struct StubPackaging {
        pub(crate) calls: AtomicUsize,
        pub(crate) result: PodmanImageMetadata,
    }

    impl<G: Gateway> PodmanPackaging<G> for StubPackaging {
        async fn build_images(
            &self,
            _gateway: &G,
            _local_project_root: &Path,
            _output_dir: &Path,
        ) -> Result<PodmanImageMetadata, PackagingError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.result.clone())
        }
    }

    #[tokio::test]
    async fn stub_packaging_returns_provided_metadata() {
        let stub = StubPackaging {
            calls: AtomicUsize::new(0),
            result: PodmanImageMetadata {
                remote_path: PathBuf::from("/gateway/images/app.tar.gz"),
                image_hash: "deadbeef".into(),
                uploaded: true,
            },
        };
        let gw = LocalGateway::new();
        let metadata = stub
            .build_images(&gw, Path::new("/proj"), Path::new("/gateway/images"))
            .await
            .expect("stub never fails");
        assert_eq!(
            metadata.remote_path,
            PathBuf::from("/gateway/images/app.tar.gz")
        );
        assert_eq!(metadata.image_hash, "deadbeef");
        assert!(metadata.uploaded);
        assert_eq!(stub.calls.load(Ordering::SeqCst), 1);
    }
}
