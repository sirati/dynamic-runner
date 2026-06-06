//! `RespawnBudget::should_respawn` decision logic.

use std::collections::HashMap;

use crate::cluster_state::RespawnEventRecord;

use super::types::{RespawnBudget, RespawnDecision};

impl RespawnBudget {
    /// Decide whether a respawn for `original_id` is admissible against
    /// the REPLICATED respawn ledger `events` and the budget's three caps.
    ///
    /// `events` is the replicated grow-only SET keyed by `new_id` (the
    /// minted replacement id, globally unique per accepted event) with a
    /// [`RespawnEventRecord`] value carrying `original_id` / `cause` / `at`.
    /// The decision is purely value-shaped (a count + a family-chain walk +
    /// a per-family max-`at`), so it reads the map VALUES order-independently
    /// — the failover-correctness fix: a promoted primary inherits the
    /// ledger via union-merge, so this consults the SAME ledger the
    /// pre-failover primary did and the budget / cooldown are NOT re-granted.
    ///
    /// Family-chain count: a "family" is the transitive closure of
    /// respawn events whose `new_id` was once another event's
    /// `original_id`. Starting from `original_id`, we expand the
    /// chain to find the head (the first peer the operator originally
    /// provisioned), then count every event in the chain that already
    /// landed. If that count already meets `max_per_secondary`, the
    /// request is rejected with [`RespawnDecision::RejectFamilyBudget`].
    ///
    /// Total budget: any event in the ledger counts toward `max_total`.
    /// The ledger is grow-only (no eviction); once `max_total` respawns
    /// have happened in the lifetime of the run, the next is refused —
    /// operators who want unlimited respawns disable the policy entirely.
    ///
    /// Cooldown: the most recent event whose `new_id` or `original_id`
    /// belongs to the same family must be at least `cooldown` older
    /// than `now`. The cooldown is per-family (not global) so a
    /// well-behaved cluster losing one peer per minute never trips
    /// it. Tested with deterministic timestamps (no wall-clock
    /// dependency).
    ///
    /// The walk is O(ledger.len()) — bounded by `max_total + in-flight`
    /// (the grow-only ledger never exceeds the budget cap). Acceptable on
    /// the operational `select!` arm because it fires at the rate of peer
    /// deaths, not per-task.
    pub fn should_respawn(
        &self,
        original_id: &str,
        events: &HashMap<String, RespawnEventRecord>,
        now: std::time::SystemTime,
    ) -> RespawnDecision {
        // Total budget first — cheapest check, prunes the common
        // exhausted-cluster failure mode before the family walk.
        if (events.len() as u32) >= self.max_total {
            return RespawnDecision::RejectTotalBudget;
        }

        // Walk the chain rooted at `original_id` and tally the count.
        // The chain head is the original peer id; every subsequent
        // entry in the family was minted as a replacement for the
        // previous death. `family_ids` accumulates every id (old +
        // new) we've seen so the cooldown check can match on either
        // side. Each event is `(new_id, record)` — the `new_id` is the
        // map KEY, the `original_id` lives on the record value.
        let mut family_ids: std::collections::HashSet<&str> = std::collections::HashSet::new();
        family_ids.insert(original_id);
        // Iterative expansion: each pass adds events whose
        // `original_id` or `new_id` is in `family_ids`. Realistic
        // chains are very short (≤ max_per_secondary); the loop is
        // bounded by the ledger size.
        let mut grew = true;
        while grew {
            grew = false;
            for (new_id, record) in events {
                if family_ids.contains(record.original_id.as_str())
                    || family_ids.contains(new_id.as_str())
                {
                    if family_ids.insert(record.original_id.as_str()) {
                        grew = true;
                    }
                    if family_ids.insert(new_id.as_str()) {
                        grew = true;
                    }
                }
            }
        }

        let family_count = events
            .iter()
            .filter(|(new_id, record)| {
                family_ids.contains(record.original_id.as_str())
                    || family_ids.contains(new_id.as_str())
            })
            .count() as u32;
        if family_count >= self.max_per_secondary {
            return RespawnDecision::RejectFamilyBudget;
        }

        // Cooldown is family-scoped: find the most recent event in
        // this family and require `now - at >= cooldown`. Walks the
        // ledger once; `max_by_key` returns `None` when the family has
        // no prior events (first respawn → cooldown trivially
        // satisfied). Order-independent: `max_by_key(at)` is invariant
        // to the `HashMap` iteration order.
        if let Some((_, latest)) = events
            .iter()
            .filter(|(new_id, record)| {
                family_ids.contains(record.original_id.as_str())
                    || family_ids.contains(new_id.as_str())
            })
            .max_by_key(|(_, record)| record.at)
        {
            // Saturating: a future-dated `latest.at` (clock skew /
            // test fixture) returns `Duration::ZERO`, which compares
            // to `cooldown` correctly (`ZERO < cooldown` ⇒ reject).
            let elapsed = now
                .duration_since(latest.at)
                .unwrap_or(std::time::Duration::ZERO);
            if elapsed < self.cooldown {
                return RespawnDecision::RejectCooldown;
            }
        }

        RespawnDecision::Accept
    }
}
