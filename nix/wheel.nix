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
# The cargoDeps hash below is intentionally a fakeHash placeholder; the
# first real build will fail with the expected SRI hash, which gets
# pinned during the calibration step in T1.5.
buildPythonPackage {
  pname = "dynamic-runner";
  version = "0.1.0";
  pyproject = true;

  src = lib.cleanSource ./..;

  cargoDeps = rustPlatform.fetchCargoVendor {
    src = lib.cleanSource ./..;
    hash = lib.fakeHash;
  };

  nativeBuildInputs = [
    rustPlatform.cargoSetupHook
    rustPlatform.maturinBuildHook
    pkg-config
  ];

  buildInputs = [ openssl ];

  doCheck = false;

  meta = with lib; {
    description = "Generic Rust runner backend exposed to Python as dynamic_runner._native";
    homepage = "https://github.com/sirati/dynamic_runner";
    license = licenses.mit;
    platforms = platforms.unix;
  };
}
