//! Lower a spawned child's CPU scheduling priority before it execs,
//! so the child (and everything it forks) cannot compete with THIS
//! process for CPU at equal scheduler weight.
//!
//! # Concern
//!
//! ONE concern: a child subprocess whose whole process subtree must
//! run at lower scheduling priority than its spawner. `nice` is the
//! kernel mechanism with exactly the right inheritance contract: it is
//! inherited across `fork(2)`/`execve(2)` (so every descendant the
//! child spawns — interpreters, JVMs, compilers — is deprioritised
//! automatically), and an unprivileged process cannot renice itself
//! back UP, so the lowered priority sticks for the entire subtree.
//! Lowering priority requires no capability (raising does), so this
//! works in rootless containers and unprivileged batch allocations.
//!
//! This is a generic process-spawn primitive — it knows nothing of
//! workers, factories, or roles. It is the scheduling sibling of the
//! `dynrunner-slurm` crate's `child_reaping` death-linkage primitive
//! and follows the same `pre_exec` pattern: arm the property in the
//! forked child between `fork(2)` and `execve(2)`, where it lands on
//! the child itself rather than the parent.
//!
//! # Relative, not absolute
//!
//! The increment is RELATIVE (`nice(2)` semantics), not an absolute
//! `setpriority` target: "the child runs N below its spawner" is the
//! actual invariant (the spawner must win the scheduler), and a
//! relative lowering is unconditionally permitted — an absolute target
//! could silently attempt a priority RAISE (needing privilege) if the
//! whole process tree was itself launched under `nice`.

use std::process::Command;

/// Arm a `pre_exec` hook on `cmd` that lowers the forked child's
/// niceness by `niceness` (relative to the spawning process) before
/// `execve(2)`. The kernel clamps the result to the nice range
/// (at most 19).
///
/// Failure policy: a failed renice is swallowed inside the hook — the
/// hook returns `Ok` unconditionally, so a renice problem can NEVER
/// abort the spawn (a child at default priority is strictly better
/// than no child; priority is a quality-of-service property, not a
/// correctness gate). This is deliberate: returning `Err` from
/// `pre_exec` fails the whole `spawn()`. The parent therefore cannot
/// observe a renice failure — acceptable, because lowering priority by
/// a non-negative increment is always permitted for unprivileged
/// processes, making the failure arm theoretical.
pub(crate) fn lower_child_priority(cmd: &mut Command, niceness: i32) {
    use std::os::unix::process::CommandExt;
    // SAFETY: the closure runs in the forked child after `fork(2)` and
    // before `execve(2)`, where only async-signal-safe operations are
    // permitted (no allocator/mutex activity that could deadlock
    // against a parent holding the heap lock at fork time). The body
    // is a single `nice(2)` libc call — bare get/setpriority syscalls
    // plus an errno write, no heap, no locks — and its result is
    // discarded (see failure policy above; errno disambiguation of the
    // legitimate `-1` return value is pointless when the error is
    // swallowed anyway).
    unsafe {
        cmd.pre_exec(move || {
            let _ = libc::nice(niceness);
            Ok(())
        });
    }
}

#[cfg(test)]
mod tests {
    //! End-to-end over a real spawn: `std::process::Command`'s
    //! `pre_exec` field is an opaque sealed-box (not introspectable),
    //! so — exactly like the `child_reaping` and `command_from_rendered`
    //! pre-exec tests — the hook is proven by spawning a trivial child
    //! that reports its own niceness and asserting the observable
    //! effect. The child reads `/proc/self/stat` field 19 (nice);
    //! `awk` inherits the niceness from the pre-exec'd `sh`, which is
    //! the same inheritance contract the worker subtree relies on.

    /// Spawn `sh -c "awk '{print $19}' /proc/self/stat"` through
    /// `lower_child_priority(niceness)` and return the niceness the
    /// child observed for itself.
    fn child_niceness_with(niceness: i32) -> i64 {
        let mut cmd = std::process::Command::new("/bin/sh");
        cmd.arg("-c").arg("awk '{print $19}' /proc/self/stat");
        cmd.stdin(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped());
        super::lower_child_priority(&mut cmd, niceness);
        let out = cmd.output().expect("spawn niceness-reporting child");
        assert!(out.status.success(), "child exited non-success: {:?}", out.status);
        String::from_utf8(out.stdout)
            .expect("stat output is ascii")
            .trim()
            .parse()
            .expect("niceness should be an integer")
    }

    /// This process's own niceness — the baseline the RELATIVE
    /// lowering is asserted against, so the tests hold even when the
    /// test runner itself was launched under `nice`.
    fn own_niceness() -> i64 {
        // SAFETY: plain getpriority(2) query on our own process — no
        // global state mutated. (`-1` is a valid return here, not an
        // error sentinel worth disambiguating for a test baseline.)
        i64::from(unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) })
    }

    /// The armed hook lowers the child by exactly the requested
    /// increment (kernel-clamped at 19), while the spawning process's
    /// own priority is untouched. Lowering is always permitted for
    /// unprivileged processes, so there is no environment to skip in.
    #[test]
    fn child_runs_at_requested_niceness_below_spawner() {
        let base = own_niceness();
        assert_eq!(
            child_niceness_with(10),
            (base + 10).min(19),
            "child must run 10 niceness below its spawner (spawner at {base})"
        );
        // The increment is a parameter, not a baked-in constant: a
        // second value pins that the hook honours what it was handed.
        assert_eq!(
            child_niceness_with(5),
            (base + 5).min(19),
            "child must run 5 niceness below its spawner (spawner at {base})"
        );
        // The lowering happened in the forked child, never the parent.
        assert_eq!(own_niceness(), base, "spawner's own priority must be untouched");
    }
}
