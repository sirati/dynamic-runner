//! PyO3 bindings for the `dynrunner-driver` crate.
//!
//! Surface (locked design point (d)):
//! - single `#[pyclass] SshMaster` exposing
//!   `spawn(...)` / `adopt(path, target)` / `disconnect()` /
//!   getters for control_path / master_pid / target /
//!   forwarded_ports / is_invalidated / is_spawned.
//! - sync `__enter__` / `__exit__` for `with`-block use. NO
//!   `__aenter__` / `__aexit__` — async-CM is deferred (peer-locked).
//! - Rust `Drop` fires on pyclass drop (NOT `__del__`). Python's
//!   GC drops the pyclass cell, which drops the `Mutex<Option<SshMaster>>`,
//!   which drops the inner `SshMaster` and runs the kill ladder.
//!
//! Plus thin function wrappers:
//! - `cluster_is_running(ssh_port)` → `bool`
//! - `ensure_dispatcher_keypair(state_dir)` → `(priv_path, pub_path)`
//! - `write_ssh_config(...)` → `path`
//!
//! Python module path: `dynamic_runner.driver`. The wiring is added
//! to `_native` in `lib.rs` via `m.add_submodule(...)`.

mod ssh_master;

pub(crate) use ssh_master::{
    PySshMaster, py_cluster_is_running, py_ensure_dispatcher_keypair, py_write_ssh_config,
};
