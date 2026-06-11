//! Apply rules + read surface for the replicated custom-message inbox
//! (F5 — IMPORTANT secondary→primary consumer messages).
//!
//! Single concern: the `(origin, seq)`-keyed sticky lattice
//! (`Unhandled ⊑ {Handled, Failed}`, map-absence as the implicit
//! bottom, deterministic Handled-wins join for the never-originated
//! terminal conflict) and its per-origin contiguous-prefix terminal
//! watermark compaction. The `apply` dispatcher routes the
//! `CustomMessagePosted` / `CustomMessageHandled` /
//! `CustomMessageFailed` arms here (the same delegation shape
//! `apply_peer` / `apply_tasks` use); the snapshot restore-merge calls
//! the same compaction so apply == restore by construction. The
//! handler-DISPATCH decision (who invokes the consumer hook, in what
//! order, and the atomic effect+terminal batching) is the primary's
//! concern (`primary/custom_message.rs`) — this module only owns the
//! replicated facts it reads.

use dynrunner_core::Identifier;

use super::types::CustomMsgState;
use super::{ApplyOutcome, ClusterState};

impl<I: Identifier> ClusterState<I> {
    /// Is `(origin, seq)` already subsumed by the per-origin terminal
    /// watermark — i.e. provably terminal (`Handled` or `Failed`; the
    /// compaction erases the label) and physically pruned?
    fn custom_watermark_covers(&self, origin: &str, seq: u64) -> bool {
        self.custom_terminal_watermarks
            .get(origin)
            .is_some_and(|w| seq <= *w)
    }

    /// `CustomMessagePosted { origin, seq, topic, data }` apply rule:
    /// vacant-insert as `Unhandled`; NoOp if the key is present in ANY
    /// state (idempotent under at-least-once delivery — a replayed
    /// landing re-posts the same key; a terminal latch that won the
    /// race locks the late post out) or watermark-subsumed (a
    /// `Posted { seq <= w }` re-application after compaction).
    pub(super) fn apply_custom_message_posted(
        &mut self,
        origin: String,
        seq: u64,
        topic: String,
        data: Vec<u8>,
    ) -> ApplyOutcome {
        if self.custom_watermark_covers(&origin, seq) {
            return ApplyOutcome::NoOp;
        }
        match self.custom_messages.entry((origin, seq)) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(CustomMsgState::Unhandled { topic, data });
                ApplyOutcome::Applied
            }
            std::collections::hash_map::Entry::Occupied(_) => ApplyOutcome::NoOp,
        }
    }

    /// `CustomMessageHandled { origin, seq }` apply rule: the sticky
    /// terminal LATCH (the `DiscoveryDebt` lattice precedent —
    /// terminal state must win regardless of arrival order):
    ///   * `Unhandled → Handled`, DROPPING the payload (Applied);
    ///   * already `Handled` / watermark-subsumed → NoOp (idempotent);
    ///   * `Failed → Handled` (Applied) — the deterministic
    ///     Handled-wins join for the THEORETICAL terminal conflict
    ///     (the primary originates exactly one terminal per message,
    ///     so this arm is convergence insurance, never a live path);
    ///   * key ABSENT → insert `Handled` directly (Applied) — a
    ///     `Handled` that outruns its `Posted` on a different gossip
    ///     path latches first; the late `Posted` then NoOps on the
    ///     occupied entry.
    ///
    /// Every Applied advances the per-origin watermark compaction.
    pub(super) fn apply_custom_message_handled(&mut self, origin: String, seq: u64) -> ApplyOutcome {
        if self.custom_watermark_covers(&origin, seq) {
            return ApplyOutcome::NoOp;
        }
        let outcome = match self.custom_messages.entry((origin.clone(), seq)) {
            std::collections::hash_map::Entry::Occupied(mut e) => match e.get() {
                CustomMsgState::Unhandled { .. } | CustomMsgState::Failed => {
                    e.insert(CustomMsgState::Handled);
                    ApplyOutcome::Applied
                }
                CustomMsgState::Handled => ApplyOutcome::NoOp,
            },
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(CustomMsgState::Handled);
                ApplyOutcome::Applied
            }
        };
        if matches!(outcome, ApplyOutcome::Applied) {
            self.compact_custom_watermark(&origin);
        }
        outcome
    }

    /// `CustomMessageFailed { origin, seq }` apply rule: the `Failed`
    /// twin of [`Self::apply_custom_message_handled`]'s terminal latch:
    ///   * `Unhandled → Failed`, DROPPING the payload (Applied) — the
    ///     handler raised; terminal, never re-dispatched;
    ///   * already `Failed` / watermark-subsumed → NoOp (idempotent);
    ///   * `Handled` → NoOp — the Handled-wins join: `Failed` never
    ///     overwrites `Handled` (theoretical conflict only; see the
    ///     `Handled` arm's mirror);
    ///   * key ABSENT → insert `Failed` directly (Applied) — same
    ///     outrun-the-`Posted` latch as `Handled`.
    ///
    /// Every Applied advances the per-origin watermark compaction
    /// (both terminals are tombstones the GC walks over).
    pub(super) fn apply_custom_message_failed(&mut self, origin: String, seq: u64) -> ApplyOutcome {
        if self.custom_watermark_covers(&origin, seq) {
            return ApplyOutcome::NoOp;
        }
        let outcome = match self.custom_messages.entry((origin.clone(), seq)) {
            std::collections::hash_map::Entry::Occupied(mut e) => match e.get() {
                CustomMsgState::Unhandled { .. } => {
                    e.insert(CustomMsgState::Failed);
                    ApplyOutcome::Applied
                }
                CustomMsgState::Handled | CustomMsgState::Failed => ApplyOutcome::NoOp,
            },
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(CustomMsgState::Failed);
                ApplyOutcome::Applied
            }
        };
        if matches!(outcome, ApplyOutcome::Applied) {
            self.compact_custom_watermark(&origin);
        }
        outcome
    }

    /// Advance `origin`'s contiguous-prefix terminal watermark over
    /// every directly-following terminal tombstone (`Handled` OR
    /// `Failed` — the watermark erases the label), physically dropping
    /// each consumed entry (the GC half of the lattice). Seqs are
    /// per-origin monotonic FROM 1 (the originating secondary's
    /// counter), so a missing watermark means "nothing compacted yet"
    /// and the walk starts at 1. An `Unhandled` (or absent — a gap from
    /// a not-yet-arrived post) entry stops the walk: the watermark only
    /// ever asserts a fully-terminal PREFIX.
    pub(super) fn compact_custom_watermark(&mut self, origin: &str) {
        let mut next = self
            .custom_terminal_watermarks
            .get(origin)
            .map(|w| w + 1)
            .unwrap_or(1);
        let mut advanced = false;
        while matches!(
            self.custom_messages.get(&(origin.to_string(), next)),
            Some(CustomMsgState::Handled | CustomMsgState::Failed)
        ) {
            self.custom_messages.remove(&(origin.to_string(), next));
            next += 1;
            advanced = true;
        }
        if advanced {
            self.custom_terminal_watermarks
                .insert(origin.to_string(), next - 1);
        }
    }

    /// Drop every retained `(origin, seq)` entry the origin's watermark
    /// now subsumes. Called by the restore merge AFTER max-merging an
    /// incoming watermark: a peer's higher watermark PROVES every
    /// `seq <= w` reached a terminal state cluster-wide (the watermark
    /// only advances over terminal tombstones), so a lagging local
    /// entry at-or-below it is stale and pruned — never re-dispatched.
    pub(super) fn prune_below_custom_watermark(&mut self, origin: &str) {
        let Some(w) = self.custom_terminal_watermarks.get(origin).copied() else {
            return;
        };
        self.custom_messages
            .retain(|(o, seq), _| o != origin || *seq > w);
    }

    /// Every `Unhandled` inbox entry, sorted by the `(origin, seq)` key
    /// — the handler-dispatch decision's read surface (the primary
    /// invokes the consumer hook in exactly this order, preserving the
    /// per-origin send order). Terminal entries — `Handled` AND
    /// `Failed` — are never surfaced: a promoted primary replays ONLY
    /// `Unhandled`. Owned clones so the caller holds no borrow against
    /// the `&mut self` coordinator while dispatching.
    pub(crate) fn unhandled_custom_messages(&self) -> Vec<(String, u64, String, Vec<u8>)> {
        let mut out: Vec<(String, u64, String, Vec<u8>)> = self
            .custom_messages
            .iter()
            .filter_map(|((origin, seq), state)| match state {
                CustomMsgState::Unhandled { topic, data } => {
                    Some((origin.clone(), *seq, topic.clone(), data.clone()))
                }
                CustomMsgState::Handled | CustomMsgState::Failed => None,
            })
            .collect();
        out.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
        out
    }

    /// The `Unhandled` inbox KEYS in `(origin, seq)` order — the
    /// backlog-monitor's read surface (`primary/custom_message.rs`'s
    /// keep-up WARN). Identity-only sibling of
    /// [`Self::unhandled_custom_messages`]: the monitor needs counts +
    /// identities every heartbeat tick, and cloning every ≤100 KB
    /// payload for that would be pure waste.
    pub(crate) fn unhandled_custom_message_keys(&self) -> Vec<(String, u64)> {
        let mut out: Vec<(String, u64)> = self
            .custom_messages
            .iter()
            .filter_map(|((origin, seq), state)| match state {
                CustomMsgState::Unhandled { .. } => Some((origin.clone(), *seq)),
                CustomMsgState::Handled | CustomMsgState::Failed => None,
            })
            .collect();
        out.sort();
        out
    }

    /// Test-only read surface (the production reader is
    /// `unhandled_custom_messages`; tests assert on individual states).
    ///
    /// The inbox state for `(origin, seq)`: `None` = never posted here
    /// AND not watermark-subsumed (the implicit lattice bottom). A
    /// watermark-subsumed key reads as `Handled` even though the
    /// tombstone is physically pruned — the watermark IS its record,
    /// and it erases the Handled/Failed label (a compacted `Failed`
    /// also reads `Handled`; the label only ever mattered for the
    /// structured ERROR log at origination time).
    #[cfg(test)]
    pub(crate) fn custom_message_state(&self, origin: &str, seq: u64) -> Option<CustomMsgState> {
        if self.custom_watermark_covers(origin, seq) {
            return Some(CustomMsgState::Handled);
        }
        self.custom_messages
            .get(&(origin.to_string(), seq))
            .cloned()
    }

    /// The per-origin terminal watermark, if any prefix has compacted.
    ///
    /// Read surfaces:
    ///   * the terminal-ordering gate (`primary/terminal_gate.rs`): a
    ///     task terminal stamped `msgs_posted_through = W` is deferred
    ///     until this watermark covers `W` — sound because the
    ///     watermark only advances over a CONTIGUOUS terminal prefix
    ///     (`compact_custom_watermark`), and the important `msg_seq`
    ///     space is dense per origin (droppables are unsequenced), so
    ///     `watermark >= W` proves every important seq `1..=W` is
    ///     Handled/Failed-resolved;
    ///   * tests (alongside `custom_message_state`).
    pub(crate) fn custom_terminal_watermark(&self, origin: &str) -> Option<u64> {
        self.custom_terminal_watermarks.get(origin).copied()
    }

    /// Live (uncompacted) inbox entry count — test/diagnostic surface.
    #[cfg(test)]
    pub(crate) fn custom_message_count(&self) -> usize {
        self.custom_messages.len()
    }
}
