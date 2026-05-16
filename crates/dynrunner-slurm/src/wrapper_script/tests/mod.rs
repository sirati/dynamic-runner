//! Module-internal tests for the wrapper-script generators, split by
//! concern. The shared `standard_cfg` helper builds a baseline
//! `WrapperScriptConfig` so each test only overrides the field it
//! exercises; sub-files import it via `super::standard_cfg`.

use crate::config::SlurmConfig;
use crate::wrapper_script::{ConnectionMode, WrapperScriptConfig};

mod argv_quoting;
mod cleanup;
mod preflight_podman;
mod reverse_mode;
mod standard_mode;
mod syntax_and_quote;
mod test_wrapper;

pub(super) fn standard_cfg<'a>(
    slurm_config: &'a SlurmConfig,
    extra_run_args: &'a [String],
) -> WrapperScriptConfig<'a> {
    WrapperScriptConfig {
        slurm_config,
        image_path: "/images/test.tar",
        secondary_id: "sec-01",
        image_name: "test-app",
        image_tag: "latest",
        image_tar_basename: "test-app.tar",
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
    }
}
