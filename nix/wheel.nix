{
  lib,
  buildPythonPackage,
  rustPlatform,
  openssl,
  pkg-config,
}:

# Wheel/Python-package derivation for dynamic_runner.
#
# The PyO3 native extension lives in crates/dynrunner-pyo3 and is built
# via maturin. The resulting Python module is `dynamic_runner._native`
# (configured via [tool.maturin] module-name in the root pyproject.toml).
#
# The cargoDeps hash below is the SRI of the vendored Cargo deps. Any
# Cargo.lock change (added/removed/version-bumped crates, including
# workspace.package version edits which propagate into per-crate
# version entries) invalidates it; recalibrate by setting
#   hash = lib.fakeHash;
# running `nix build .#dynamic-runner --max-jobs 6 --cores 4`, copying
# the "got: sha256-..." value from the failure into this field, and
# only then committing + pushing.
buildPythonPackage {
  pname = "dynamic-runner";
  version = "0.4.0";
  pyproject = true;

  src = lib.cleanSource ./..;

  cargoDeps = rustPlatform.fetchCargoVendor {
    src = lib.cleanSource ./..;
    hash = "sha256-Ah5zjeHenz02kz8ul/6Fnhs79XrJN3RdRoPn9JIGmW8=";
  };

  nativeBuildInputs = [
    rustPlatform.cargoSetupHook
    rustPlatform.maturinBuildHook
    pkg-config
  ];

  buildInputs = [ openssl ];

  doCheck = false;

  meta = with lib; {
    description = "Multi-process / multi-host Python task runner backed by a Rust workspace.";
    homepage = "https://github.com/sirati/dynamic-runner";
    license = licenses.asl20;
    platforms = platforms.unix;
  };
}
