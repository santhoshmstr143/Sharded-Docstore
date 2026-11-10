# Failover & Fencing Specification — Sharded-Docstore

This document specifies the failure detection, promotion protocol, split-brain fencing, and rejoin reconciliation procedures for Sharded-Docstore.

---

## 1. Failover State Machine

```
┌─────────────────┐
│ Primary Healthy │
└────────┬────────┘
         │ Heartbeat timeout (D = 5.0s)
         ▼
┌───────────────────────┐
│ Failure Detected      │
│ Coordinator checks F  │
└────────┬──────────────┘
         │ Wait lease expiry + M (3.5s)
         ▼
┌───────────────────────────┐
│ Epoch & Term Bump         │
│ coordinator updates shard │
│ term = term + 1           │
│ ownership_epoch += 1      │
└────────┬──────────────────┘
         │
         ▼
┌───────────────────────────┐
│ Promote Follower          │
│ Follower -> Primary       │
│ Serves reads; rejects     │
│ writes (Degraded Mode)    │
└────────┬──────────────────┘
         │ Old primary restarts & catches up
         ▼
┌───────────────────────────┐
│ Rejoin & Resync           │
│ Truncates divergent tail  │
│ Follower -> SYNCED        │
└────────┬──────────────────┘
         │
         ▼
┌───────────────────────────┐
│ Normal Cluster Operation  │
│ Writes re-enabled         │
└───────────────────────────┘
```

---

## 2. Step-by-Step Promotion Procedure

1. **Failure Detection:** The coordinator monitors heartbeats. If no heartbeat is received from the primary within failure detection timeout $D = 5.0s$, the coordinator marks the primary as down. A shard is also failed over when it has **no registered primary** and there is positive evidence of it:
   - the incumbent primary restarted and re-registered as a follower (nodes recovering from disk never resume authority on their own), or
   - another node of the shard is registered and alive, yet no primary has registered within $D$ of coordinator start.
2. **Candidate Check:** The coordinator contacts the promotion candidate (normally the follower) and reads its `last_applied_seq`. Unless an operator forces it (`POST /admin/failover/{shard}?force=true`), it refuses to promote a follower that the primary's heartbeats last reported as not `SYNCED`, or that is behind the primary's last reported sequence number — promoting it could lose committed writes. If the incumbent primary restarted (its data intact) while the follower is unsynced, the incumbent is re-promoted under a new term instead. Failovers of different shards run concurrently, so one slow failover never delays another shard's recovery.
3. **Log Drain:** Follower finishes applying all durable WAL entries to memory.
4. **Safety Wait:** While a shard is failing over, the coordinator refuses to renew or grant its primary lease (`503 FAILOVER_IN_PROGRESS` on heartbeats). Under the topology lock it then waits until the old primary's lease has definitively expired plus safety margin $M = 0.5s$:
   $$t_{wait} = \text{lease.expires\_at} + M - \text{now()}$$
5. **Durable Failover Intent:** Under the shard's **topology lock** (failover and migration cutover never interleave), the coordinator re-checks that the map still names the failed primary, then writes a failover intent (`failover_intents.json`: new primary, old primary, target term) before changing anything else.
6. **Epoch and Term Bumping:**
   - Shard `term` is incremented by 1.
   - `ownership_epoch` of **every key range** owned by this shard is incremented by 1.
   - Coordinator persists the updated shard map with new `map_version`.
7. **Promotion Notification:** Coordinator promotes the follower via `POST /internal/promote`, sending the term, the shard's authoritative ranges and epochs from the map, and a fresh lease. The follower persists its new term/role/ranges **before** acting as primary. When promotion is confirmed the intent is deleted; otherwise it is retried from the health monitor.
8. **Coordinator crash recovery:** on restart, an intent whose map change is committed is resumed (the promotion is re-sent after the lease quarantine $L + M$); an intent whose map change never happened is discarded, and normal failure detection takes over.
9. **Degraded Mode Operation:** The new primary serves reads immediately, but returns 503 `REPLICATION_UNAVAILABLE` on writes until a follower joins and reaches `SYNCED`.

---

## 3. Rejoin Reconciliation Protocol

When an old crashed primary restarts, it must never resume as primary from its stale state:

1. **Demotion & Registration:** The restarted node recovers as a follower, registers with the coordinator as a follower, and steps down if it was acting as primary. If the map still named it as primary, the coordinator promotes the current follower (step 1 above) and the restarted node becomes the follower; its map slot is never overwritten in place.
2. **Divergence Discovery:**
   - The returning node sends its retained WAL entries (and its `base_seq`, the history covered only by its snapshot after compaction) to the new primary via `POST /internal/divergence`.
   - Entries are matched on their **full identity** (term, seq, op, doc_id, request_id, payload), not just `(term, seq)`.
   - The **divergence point** is the highest contiguous `seq` where both logs hold the identical entry. If the new primary can no longer serve entries after the follower's base, a full snapshot resync is used.
3. **Tail Truncation:**
   - Any local WAL entries beyond the divergence point (uncommitted entries that existed only on the old primary) are **truncated**.
   - In-memory index and idempotency dedup table are rebuilt from the local snapshot plus the truncated WAL.
4. **Catchup & Resync:**
   - The node streams missing oplog records from the new primary via `POST /internal/catchup` (or full snapshot via `/internal/snapshot` if compacted).
5. **State Transition to `SYNCED`:**
   - Once all missing records are durably fsynced and applied, the primary marks the follower as `SYNCED`.
   - Degraded mode terminates and writes resume normally.

---

## 4. Failure Scenarios and Mitigations

| Failure Scenario | Mitigation & Behavior |
|---|---|
| **Primary Crash before Follower ACK** | Uncommitted entry discarded upon failover; retry with same `request_id` executes cleanly on new primary. |
| **Follower ACK lost in transit** | Follower durably stored the entry. After failover, the operation is committed. Retry returns cached outcome. |
| **Split-Brain Network Partition** | Old primary cannot renew lease; old primary rejected by follower due to stale term; safety wait guarantees old lease is dead before new primary accepts writes. |
| **Coordinator Crash** | Primaries continue serving reads and writes until lease expiry; documented limitation: single coordinator prototype. |
| **Coordinator crash mid-failover** | Durable failover intent: a committed map change is finished (promotion re-sent after the $L + M$ quarantine); an uncommitted one is discarded. |
| **Primary restarts before $D$ elapses** | It recovers as a follower; the coordinator sees the incumbent re-registered as follower and promotes the current follower. |
| **Promotion metadata write fails** | The follower returns 500 and stays a follower with its old term; the coordinator keeps retrying. |
| **Unrelated 409 on heartbeat** | Ignored; a primary steps down only on `STALE_TERM`. |
| **WAL write/fsync fails on a node** | The node fail-stops: it stops serving (`NODE_NOT_READY`), heartbeating and holding a lease, so the coordinator fails over; the client gets `OUTCOME_UNKNOWN` and retries with the same `request_id`. |
| **Follower is unsynced when the primary dies** | No automatic promotion (it could lose writes); the shard waits for the primary or an operator's forced failover. |
| **Primary's data directory is wiped** | It restarts empty; the coordinator knows the shard holds data and registers it as a follower, promotes the replica, and the empty node is rebuilt by snapshot resync. |
| **Promotion response lost / RPC fails** | The coordinator only records the new primary once promotion is confirmed (no phantom primary); a node heartbeating as a follower is recorded as one. Promotions without a valid lease grant are refused by the node. |
| **New follower registers in an empty slot** | The coordinator pushes the address to the primary (`/internal/follower`), which resyncs it; it is `SYNCED` only after a successful rejoin. |
| **Follower crashed between WAL append and apply** | It never acknowledges an entry (duplicate replication or reconciliation check) before applying it and durably recording it as committed. |
| **Promotion target never confirms** | Retried, then abandoned after 30 attempts so the shard can fail over to the other replica. |
