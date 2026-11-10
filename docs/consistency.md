# Consistency Model — Sharded-Docstore

This document formally specifies the consistency, replication, and durability guarantees of Sharded-Docstore.

---

## 1. Core Distinctions: Durability, Replication, Consistency, Availability

To reason precisely about distributed state, Sharded-Docstore distinguishes these four orthogonal concepts:

| Dimension | Definition in Sharded-Docstore |
|---|---|
| **Durability** | Persistence against crash. A record is durable on a single node once written and flushed to disk via `fsync()`. |
| **Replication** | The mechanism by which oplog records are propagated across distinct nodes (Primary $\rightarrow$ Follower). |
| **Consistency** | The visibility and freshness invariants observed by clients (Linearizability on primary reads, Bounded Staleness on follower reads, Single-Writer order per shard). |
| **Availability** | The ability of the system to serve reads and writes during partitions or component crashes. |

---

## 2. Write Path and The Commit Point

### 2.1 The Commit Point Definition

> **Commit Point Invariant:** An operation is defined as **COMMITTED** if and only if **the follower replica has durably appended and `fsync`ed the record to its local WAL**.

This rule has critical practical implications:
1. **Source of Truth Across Crashes:** If the primary crashes after appending to its local WAL but before receiving the follower's durable ACK, the record is **uncommitted** unless the follower holds it. When failover occurs, the follower's durable log decides the committed state of the cluster.
2. **Lost Acknowledgements:** If the follower durably fsyncs the record and sends an ACK, but the network drops the ACK or the primary crashes before returning HTTP 200 to the client, the write **is nonetheless committed**. When the client retries using the same `request_id`, the system returns the original outcome idempotently.

---

## 3. The Three-State Write Outcome Model

Every write returns strictly one of three outcomes:

```
                  ┌──────────────────────────────┐
                  │ Client Submits Write Request │
                  └──────────────┬───────────────┘
                                 │
           ┌─────────────────────┼─────────────────────┐
           ▼                     ▼                     ▼
┌────────────────────┐ ┌────────────────────┐ ┌────────────────────┐
│     COMMITTED      │ │      REJECTED      │ │  OUTCOME_UNKNOWN   │
├────────────────────┤ ├────────────────────┤ ├────────────────────┤
│ • Both WALs fsync  │ │ • Failed validation│ │ • Primary fsync ok │
│ • Memory updated   │ │ • Stale epoch/term │ │ • Follower ACK     │
│ • HTTP 200 OK      │ │ • No SYNCED repl   │ │   timed out / lost │
│ • Safe & permanent │ │ • Nothing appended │ │ • HTTP 202 Accepted│
│                    │ │ • Safe to retry    │ │ • Retry same req_id│
└────────────────────┘ └────────────────────┘ └────────────────────┘
```

1. **`COMMITTED` (HTTP 200):** Both primary and follower have written and flushed to disk. The operation is permanent.
2. **`REJECTED` (HTTP 4xx / 503):** The operation was rejected before any WAL append (e.g. duplicate key, validation error, stale lease/term/epoch, or follower not in `SYNCED` state). Safe to retry.
3. **`outcome_unknown` (HTTP 202):** The primary wrote and flushed to its WAL, but did not receive a durable ACK from the follower within the replication timeout ($T = 1.0s$). The operation may or may not have committed on the follower. Further writes on this shard are blocked until reconciled. Retrying with the same `request_id` returns the eventual outcome.

---

## 4. Read Consistency Modes

### 4.1 Primary Reads (`read_mode: "primary"`)
- **Guarantee:** **Linearizable Reads**.
- **Mechanism:** Served exclusively by the primary holding a valid coordinator lease ($remaining\_lease > 0$).
- Guarantees that any read reflects all previously acknowledged writes.

### 4.1.1 Primary reads during a range migration
The guarantee holds through a migration because no two shards ever accept writes for the
same range, and the old owner stops serving the range before the new owner accepts writes:

| Phase | Writes to the range | Reads of the range |
|---|---|---|
| copy / catch-up | source only | source (current data) |
| `PAUSE` … `VERIFY`, map flip | **none** (source returns `RANGE_PAUSED`) | source — no newer version can exist anywhere |
| source *release* | none | source answers `OUT_OF_RANGE`; the router refreshes its map |
| destination activated | destination only | destination |

A router with a stale map therefore never reads an old copy once the destination has taken a
write: the source has already released the range, so the stale request is refused and
re-routed (verified by `test_v4_stale_router_read_after_migration_gets_the_new_owner`).
Reads are not blocked during the write pause; they cannot be stale then because no write is
accepted anywhere for that range.

### 4.2 Follower Reads (`read_mode: "follower"`)
- **Guarantee:** **Bounded Staleness**.
- **Mechanism:** Served directly by the follower replica.
- Response metadata includes `applied_seq` and `replication_lag` (number of operations behind primary).
- In the event that the follower replica is offline, the router automatically falls back to the primary replica and tags `served_by: "primary"`.

---

## 5. Fencing Argument: Term vs Lease

A common distributed systems pitfall is conflating lease validity with write fencing. Sharded-Docstore clearly delineates their responsibilities:

### 5.1 Why Follower Term Fencing Protects Acknowledged Writes
Suppose a network partition isolates the primary $P_1$, and the coordinator promotes follower $F$ to become new primary $P_2$ with term $2$.
- If old primary $P_1$ experiences a pause (e.g. GC pause or asymmetric partition) and attempts to commit a write:
  - $P_1$ sends replication RPC with `term: 1`.
  - Replicas reject the RPC because their local term is $2$.
  - Without the follower's durable fsync, $P_1$ **cannot commit the write**.
- **Conclusion:** It is the **follower's term check**, not the primary lease, that guarantees no acknowledged write is lost or overwritten across failover.

### 5.2 Why the Lease Protects Primary-Read Freshness
If old primary $P_1$ served primary reads while isolated, it could return stale data if $P_2$ has already committed newer writes.
- The coordinator grants a lease of duration $L$. The coordinator **never promotes a new primary until the old lease has expired plus a safety margin $M$** ($L + M$).
- Therefore, $P_1$ stops serving reads before $P_2$ can accept new writes, preventing stale primary reads.

---

## 6. `(term, seq)` Log Matching Rule

Every WAL and oplog entry is identified by the pair:
$$(term, seq)$$

- Within any given `term`, `seq` is strictly monotonic and gap-free.
- When an old primary rejoins after a failover:
  1. It compares its local log against the new primary.
  2. It finds the **divergence point**: the highest contiguous `seq` where both logs hold the identical entry — compared on full identity (term, seq, op, doc_id, request_id, payload), not on `term` alone.
  3. It **truncates any uncommitted tail** beyond this divergence point.
  4. It catches up missing entries from the new primary's log.

---

## 7. Degraded Mode & Architectural Alternatives

### 7.1 Degraded Mode After Failover
In a 2-replica cluster ($P + F$), when the primary crashes and the follower is promoted:
- The newly promoted primary **serves reads immediately** (fresh reads holding a valid lease).
- The promoted primary **rejects writes with `REPLICATION_UNAVAILABLE`** until the old primary restarts, rejoins, resyncs, and transitions to `SYNCED`.
- Synchronous replication is **never relaxed** to single-node durability.

### 7.2 Why Alternatives Were Rejected

| Alternative Considered | Why Rejected |
|---|---|
| **Relaxed Durability (Single-node ACK)** | Violates zero-data-loss invariant. If the lone node crashes before a second replica joins, acknowledged data is permanently lost. |
| **3-Replica Raft Quorum** | Higher resource footprint and operational complexity for prototype scope. A 2-node sync-replication model with coordinator lease guarantees strict consistency at minimal resource overhead. |
