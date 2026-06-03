//! Image-staging methods on [`SlurmJobManager`]: delegate to a
//! [`PodmanPackaging`] for the container image build/transfer, and
//! upload Rust source binaries to the gateway's `src_bins_path`. Pure
//! gateway-side file movement — no SLURM lifecycle.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use dynrunner_core::TaskInfo;
use dynrunner_gateway::traits::Gateway;
use tracing;

use crate::packaging::{PodmanImageMetadata, PodmanPackaging};

use super::types::{SlurmError, SlurmJobManager};

impl<G: Gateway> SlurmJobManager<G> {
    pub async fn build_and_transfer_images<P>(
        &self,
        packager: &P,
        local_project_root: &Path,
    ) -> Result<PodmanImageMetadata, SlurmError>
    where
        P: PodmanPackaging<G>,
    {
        // Building-image important event (LLM-wake): occurrence point is
        // the start of the container image build+transfer. Emitted at the
        // importance target so the dual-sink surfaces it on stdio under
        // `--important-stdio-only`.
        tracing::info!(
            target: crate::IMPORTANT_TARGET,
            "Building and transferring container image...",
        );
        let output_dir = self.config.image_path();
        let metadata = packager
            .build_images(&self.gateway, local_project_root, Path::new(&output_dir))
            .await?;
        // Uploading-image important event: occurrence point is the
        // image-transfer result. `uploaded` discriminates an actual
        // upload (cache miss) from a reused remote artifact (cache hit);
        // both are the "image is now on the gateway" milestone. Same
        // importance target.
        tracing::info!(
            target: crate::IMPORTANT_TARGET,
            remote_path = %metadata.remote_path.display(),
            uploaded = metadata.uploaded,
            "container image ready on gateway",
        );
        Ok(metadata)
    }

    /// Upload each binary's underlying file to `<srcbins>/<rel>` on the
    /// gateway so the wrapper's read-only bind-mount of srcbins into
    /// `/app/src-network` actually has the staged source.
    ///
    /// Without this the StageFile pipeline (which tells the secondary
    /// "the file is now at src_network/<rel_path>") points at an empty
    /// directory and every TaskAssignment surfaces as "not pre-staged"
    /// — the framework had no primitive that turned the consumer's
    /// local `--source` tree into a populated `src_network` view on
    /// the cluster.
    ///
    /// Caller-side gating decides WHEN to call this (file-based task,
    /// not `--source-already-staged`); this method assumes the caller
    /// already wants the upload.
    ///
    /// `binary.path` may be:
    ///
    /// * absolute under `source_root` — uploaded to `<srcbins>/<rel>`
    ///   where `<rel>` is the strip-prefixed tail (legacy shape);
    /// * absolute out-of-tree — skipped; the StageFile record ships
    ///   the absolute path which the secondary's `stage_file` handler
    ///   treats as out-of-band-staged (must already exist on the
    ///   secondary by some other means);
    /// * relative — joined with `source_root` for the on-disk read;
    ///   uploaded to `<srcbins>/<binary.path>` verbatim. This is the
    ///   wire-identifier shape consumers should prefer post-Bug-B
    ///   (mirrors the Rust `queue_initial_staging` fix in
    ///   `crates/dynrunner-pyo3/src/managers/primary.rs` and the
    ///   Python `upload_source_binaries` fix in d5d0604).
    ///
    /// Strip-prefix is purely lexical (no canonicalize), matching
    /// `queue_initial_staging`. Symlinked source trees would diverge
    /// from the Python `Path.resolve()` shape uniformly across both
    /// sites — that's a separate latent issue not in this fix's scope.
    pub async fn upload_source_binaries<I>(
        &self,
        binaries: &[TaskInfo<I>],
        source_root: &Path,
    ) -> Result<(), SlurmError> {
        let srcbins_dir = PathBuf::from(self.config.src_bins_path());
        tracing::info!(
            count = binaries.len(),
            srcbins_dir = %srcbins_dir.display(),
            "uploading source files to gateway",
        );

        // Track parent dirs we've already requested so a flat tree
        // doesn't issue N redundant `mkdir -p` round-trips when every
        // file lives under the same subdirectory.
        let mut created_dirs: HashSet<PathBuf> = HashSet::new();
        created_dirs.insert(srcbins_dir.clone());

        let mut uploaded = 0usize;
        for binary in binaries {
            // Resolve the on-disk read location: relative paths join
            // against source_root (post-Bug-B wire-id shape — mirrors
            // the Rust queue_initial_staging fix); absolute paths use
            // binary.path verbatim.
            let local: PathBuf = if binary.path.is_absolute() {
                binary.path.clone()
            } else {
                source_root.join(&binary.path)
            };
            let rel = match local.strip_prefix(source_root) {
                Ok(p) => p.to_path_buf(),
                Err(_) => {
                    tracing::warn!(
                        raw = %binary.path.display(),
                        resolved = %local.display(),
                        source_root = %source_root.display(),
                        "binary is not under --source root; skipping upload \
                         (absolute path will ship as out-of-band; secondary \
                         must already see it).",
                    );
                    continue;
                }
            };
            let remote = srcbins_dir.join(&rel);
            if let Some(parent) = remote.parent()
                && created_dirs.insert(parent.to_path_buf())
            {
                self.gateway
                    .create_directory(&parent.to_string_lossy())
                    .await?;
            }
            self.gateway
                .transfer_file(&local, &remote.to_string_lossy())
                .await?;
            uploaded += 1;
        }
        tracing::info!(
            uploaded,
            total = binaries.len(),
            "source-binary upload complete",
        );
        Ok(())
    }
}
