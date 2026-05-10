"""Public driver primitives for OpenSSH-based cluster harnesses.

Re-exports from the Rust extension's ``dynamic_runner._native.driver``
submodule, which is built from ``crates/dynrunner-driver`` via the
PyO3 bindings in ``crates/dynrunner-pyo3/src/driver/``.

Public surface (locked design):
- ``SshMaster`` — owns master daemon lifetime + control path +
  watcher + forwarded_ports state. Two constructors:
  ``SshMaster.spawn(host, ...)`` and
  ``SshMaster.adopt(control_path, target)``. Sync context-manager
  (``__enter__``/``__exit__``) — explicitly NO async-CM (peer-
  locked). ``disconnect()`` is idempotent.
- ``cluster_is_running(ssh_port)`` — TCP probe of
  ``localhost:<ssh_port>``.
- ``ensure_dispatcher_keypair(state_dir)`` — generate (or return
  existing) per-cluster ed25519 keypair under ``<state_dir>/keys/``.
- ``write_ssh_config(...)`` — emit the pinned ssh_config(5)
  defaults (IdentitiesOnly + IdentityAgent=none + no host-key
  reuse) and return the absolute path.

This module is the future home for harness code that today lives
in ``tests/e2e/_ssh_state.py``. The e2e helpers there now delegate
to ``SshMaster`` / ``cluster_is_running`` / ``write_ssh_config``
verbatim — they retain only the harness-specific bring-up /
provisioning subprocess wiring (which is NOT in scope for the
framework crate, per locked design point (l)).
"""

from dynamic_runner._native import driver as _native_driver

SshMaster = _native_driver.SshMaster
cluster_is_running = _native_driver.py_cluster_is_running
ensure_dispatcher_keypair = _native_driver.py_ensure_dispatcher_keypair
write_ssh_config = _native_driver.py_write_ssh_config

__all__ = [
    "SshMaster",
    "cluster_is_running",
    "ensure_dispatcher_keypair",
    "write_ssh_config",
]
