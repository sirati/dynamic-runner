{
  lib,
  rustPlatform,
  openssl,
  pkg-config,
  buildPythonPackage,
}:

buildPythonPackage {
  pname = "dynamic-batch-rs";
  version = "0.1.0";
  pyproject = true;

  src = lib.cleanSource ./.;

  cargoDeps = rustPlatform.fetchCargoVendor {
    src = lib.cleanSource ./.;
    hash = "sha256-zgbxq9XoflCH46frBANz//z04vT+JEl8uhHoUPIihC0=";
  };

  buildAndTestSubdir = "crates/db_python_provider";

  nativeBuildInputs = [
    rustPlatform.cargoSetupHook
    rustPlatform.maturinBuildHook
    pkg-config
  ];

  buildInputs = [ openssl ];

  doCheck = false;
}
