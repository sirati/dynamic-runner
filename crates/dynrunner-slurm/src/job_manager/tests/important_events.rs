//! "Important" (LLM-wake) event emission from the image build/transfer
//! step.
//!
//! Single concern: pin that `build_and_transfer_images` emits the
//! building-image and uploading-image events on the importance marker
//! target (`crate::IMPORTANT_TARGET`) so the `dynrunner-pyo3` dual-sink
//! routes them to stdio under `--important-stdio-only`. The capture
//! filters strictly on the target so a regression that drops the
//! marker (logging at the default target) fails the test.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tracing::Metadata;
use tracing::subscriber::set_default;
use tracing_subscriber::Registry;
use tracing_subscriber::filter::FilterFn;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

use crate::config::SlurmConfig;
use crate::job_manager::types::SlurmJobManager;
use crate::packaging::{PackagingError, PodmanImageMetadata, PodmanPackaging};
use dynrunner_gateway::local::LocalGateway;
use dynrunner_gateway::traits::Gateway;

/// Records the `message` of every event whose target is the importance
/// marker, so the test asserts the events fired on that target only.
#[derive(Clone, Default)]
struct ImportantCapture(Arc<Mutex<Vec<String>>>);

impl<S> Layer<S> for ImportantCapture
where
    S: tracing::Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        struct MsgVisitor<'a>(&'a mut Option<String>);
        impl tracing::field::Visit for MsgVisitor<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    *self.0 = Some(format!("{value:?}"));
                }
            }
        }
        let mut msg = None;
        event.record(&mut MsgVisitor(&mut msg));
        if let Some(m) = msg {
            self.0.lock().unwrap().push(m);
        }
    }
}

fn important_only() -> FilterFn<fn(&Metadata<'_>) -> bool> {
    fn predicate(meta: &Metadata<'_>) -> bool {
        meta.target() == crate::IMPORTANT_TARGET
    }
    FilterFn::new(predicate as fn(&Metadata<'_>) -> bool)
}

struct StubPackaging(PodmanImageMetadata);

impl<G: Gateway> PodmanPackaging<G> for StubPackaging {
    async fn build_images(
        &self,
        _gateway: &G,
        _local_project_root: &Path,
        _output_dir: &Path,
    ) -> Result<PodmanImageMetadata, PackagingError> {
        Ok(self.0.clone())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn build_and_transfer_emits_building_and_uploading_important_events() {
    let capture = ImportantCapture::default();
    let subscriber = Registry::default().with(capture.clone().with_filter(important_only()));

    // `set_default` returns a thread-local guard that stays active
    // across `.await` points on the current-thread runtime, so the
    // capture observes the events the async build/transfer emits.
    let _guard = set_default(subscriber);

    let manager = SlurmJobManager::new(SlurmConfig::default(), LocalGateway::new());
    let packager = StubPackaging(PodmanImageMetadata {
        remote_path: PathBuf::from("/srv/slurm/image_bin/app.tar.gz"),
        image_hash: "abc123".into(),
        uploaded: true,
    });
    manager
        .build_and_transfer_images(&packager, Path::new("/proj"))
        .await
        .expect("stub never fails");

    let msgs = capture.0.lock().unwrap().clone();
    // Both image events reached the importance target, in order:
    // building (start) then uploading (transfer result).
    assert_eq!(
        msgs.len(),
        2,
        "expected exactly the building + uploading important events, got {msgs:?}"
    );
    assert!(
        msgs[0].contains("Building and transferring container image"),
        "first important event must be the build start: {msgs:?}"
    );
    assert!(
        msgs[1].contains("container image ready on gateway"),
        "second important event must be the upload/transfer result: {msgs:?}"
    );
}
