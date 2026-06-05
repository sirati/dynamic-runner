//! Module-internal tests for the wrapper-script generators. The shared
//! `standard_cfg` helper builds a baseline `WrapperScriptConfig` so each
//! test only overrides the field it exercises; sub-files import it via
//! `super::standard_cfg`.
//!
//! The renderer emits ONLY the `exec <wrapper-bin> <args…>` stub (the
//! legacy inline-bash heredoc was deleted at root), so the surviving
//! tests cover: the stub round-trip / anti-drift contract
//! (`binary_stub`), `bash -n` smoke checks on the rendered stub +
//! `bash_quote` (`syntax_and_quote`), and the image-validation
//! `generate_test_wrapper_script` generator (`test_wrapper`), which is a
//! separate generator and still emits a heredoc. The old heredoc-only
//! test files (`standard_mode`, `reverse_mode`, `argv_quoting`,
//! `cleanup`, `preflight_podman`, `shutdown_manager`) were removed with
//! the dead path they asserted — that behaviour now lives in, and is
//! tested by, the `dynrunner-slurm-wrapper` binary crate.

use crate::config::SlurmConfig;
use crate::wrapper_script::{ConnectionMode, WrapperScriptConfig};
use std::path::Path;

mod binary_stub;
mod syntax_and_quote;
mod test_wrapper;

pub(super) fn standard_cfg<'a>(
    slurm_config: &'a SlurmConfig,
    extra_run_args: &'a [String],
) -> WrapperScriptConfig<'a> {
    WrapperScriptConfig {
        slurm_config,
        // Generic baseline prefix; the legacy `asm` literal is no
        // longer hardcoded in the generator, so the baseline supplies
        // one explicitly.
        name_prefix: "asm",
        // Mandatory: the renderer emits the `exec <wrapper-bin>` stub for
        // this binary path. A representative compute-node path; tests
        // asserting the exact `exec` target override it.
        wrapper_bin_path: Path::new("/gw/dynrunner-slurm-wrapper"),
        image_path: "/images/test.tar",
        secondary_id: "sec-01",
        image_name: "test-app",
        image_tag: "latest",
        image_tar_basename: "test-app.tar",
        image_digest: "testdigest0001",
        load_command: "podman --root \"$PODMAN_STORAGE\" --runroot \"$PODMAN_RUN\" --cgroup-manager=cgroupfs load < \"$LOCAL_IMAGE\"",
        container_command: "dynamic_batch_tokenizer",
        cores_spec: "0",
        max_memory_spec: "-2G",
        connection: ConnectionMode::Standard {
            gateway_host: "gateway.example.com",
            gateway_port: 9000,
        },
        run_log_dir: None,
        dynrunner_network_dir: None,
        srcbins_mount_source: None,
        output_dir: None,
        extra_run_args,
        forwarded_argv: &[],
        is_observer: false,
        // Disabled by default for the test baseline: the
        // out-of-cgroup shutdown-manager spawn block is a separate
        // concern asserted by `tests::shutdown_manager`. Other
        // tests get the legacy CMD_RELAY-only cleanup trap so
        // unrelated regressions don't trip the new feature's
        // assertions.
        shutdown_manager_bin_path: None,
        // Default-off so legacy wrapper-script tests assert on the
        // pre-flag argv shape. The dedicated test for the rendered
        // `--mem-manager-reserved=` flag flips this to `Some(...)`.
        mem_manager_reserved_bytes: None,
    }
}
