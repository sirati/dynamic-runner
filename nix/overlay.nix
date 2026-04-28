final: prev: {
  # Inject `dynamic-runner` into every Python package set on the consuming
  # nixpkgs instance. Consumers can then reference it as
  # `final.python3Packages.dynamic-runner` (or any other pythonXYPackages
  # set), exactly like an upstream nixpkgs Python package.
  pythonPackagesExtensions = (prev.pythonPackagesExtensions or [ ]) ++ [
    (pyFinal: pyPrev: {
      dynamic-runner = pyFinal.callPackage ./wheel.nix { };
    })
  ];
}
