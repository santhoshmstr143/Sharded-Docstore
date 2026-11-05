# Architecture Specification — Sharded-Docstore

Sharded-Docstore is a distributed, horizontally partitioned document store inspired by MongoDB. It provides durable CRUD operations, MongoDB-style filter queries with global pagination, synchronous replication, leader leases, and range partitioning.

---

## 1. System Components

```
                              ┌────────────────┐
                              │  Client / CLI  │
                              └───────┬────────┘
                                      │ HTTP (X-Api-Key)
                                      ▼
                              ┌────────────────┐
                              │  Router Node   │
                              │  (Stateless)   │
                              └───┬───┬────┬───┘
               Routing Cache      │   │    │      Inter-node RPC
               (map_version)      │   │    │      (X-Cluster-Signature HMAC)
        ┌─────────────────────────┘   │    └──────────────────────────┐
        ▼                             ▼                               ▼
┌──────────────┐              ┌──────────────┐                ┌──────────────┐
│ Coordinator  │              │ Shard 0      │                │ Shard 1      │
│ (Stateful)   │              │ Primary (P0) │                │ Primary (P1) │
└──────────────┘              └───┬──────────┘                └───┬──────────┘
                                  │ Sync WAL                      │ Sync WAL
                                  │ Replicate                     │ Replicate
                                  ▼                               ▼
                              ┌──────────────┐                ┌──────────────┐
                              │ Shard 0      │                │ Shard 1      │
                              │ Follower (F0)│                │ Follower (F1)│
                              └──────────────┘                └──────────────┘
```

### 1.1 Router (Stateless Gateway)
- **Role:** Terminates client requests, authenticates `X-Api-Key`, caches the cluster shard map, routes single-document operations directly to the owning shard primary, and coordinates scatter-gather queries across all shards.
- **Concurrency & Resiliency:** Enforces a maximum concurrent request concurrency limit (`MAX_CONCURRENT_REQUESTS = 1000`) and handles shard map cache invalidation on stale routing signals (`OUT_OF_RANGE`, `STALE_EPOCH`, `NOT_PRIMARY`, `LEASE_EXPIRED`), retrying once with the same `request_id`. Every forwarded write carries the routing entry's `ownership_epoch` as a fencing token. Request bodies are capped at 2 MB even when no `Content-Length` is sent; documents at 1 MB.

### 1.2 Coordinator (Cluster Topology & Leases)
- **Role:** Authoritative owner of the persistent cluster shard map, node registrations, and primary leader leases.
- **Lease Manager:** Grants time-bounded leases (`L = 3.0s`) to shard primaries based on periodic heartbeats (`H = 1.0s`), verified using monotonic local clocks.
- **Fencing Authority:** Bumps `map_version` on topology changes, increments per-range `ownership_epoch` on repartitioning or failover, and increments per-shard `term` on leader changes.
- **Crash-safe topology changes:** failovers are recorded as durable intents before the map changes; a per-shard topology lock serialises failover with migration cutover; migrations stuck past cutover are retried by the health monitor.

### 1.3 Shard Node (Primary & Follower Replicas)
- **Primary Replica:** Owns read and write operations for assigned key ranges. Enforces lease validity, locks writes per shard, appends to its local WAL, replicates to the follower synchronously, applies to memory upon follower fsync ACK, and tracks follower replication lag.
- **Follower Replica:** Replicates oplog entries synchronously from the primary, verifies strict sequence order (`seq == last_seq + 1`) and term consistency, appends and fsyncs to its local WAL, responds with durable acknowledgements, and serves follower reads tagged with replication lag.

---

## 2. Data Path

### 2.1 The 10-Step Synchronous Replication Write Path

```
Client         Router               Primary                     Follower
  │  Write(doc)  │                     │                            │
  ├─────────────►│ Range lookup        │                            │
  │              ├────────────────────►│ 1. Acquire write lock      │
  │              │                     │ 2. Check lease, term,      │
  │              │                     │    epoch, SYNCED state     │
  │              │                     │ 3. Assign next seq         │
  │              │                     │ 4. Append & fsync WAL      │
  │              │                     ├───── Replicate(seq,op) ───►│
  │              │                     │                            │ 6. Validate term/seq
  │              │                     │                            │ 7. Append & fsync WAL
  │              │                     │◄──── Ack(seq, durable) ────┤ 8. Send ACK
  │              │                     │ 9. Apply to memory state   │
  │              │                     │ 10. Verify lease valid     │
  │              │◄── COMMITTED ───────┤                            │
  │◄─────────────┤                     │                            │
```

1. **Client Request:** Client generates a unique `request_id` (UUID4) and sends write operation.
2. **Primary Validation:** Primary acquires per-shard write lock. Validates:
   - Primary role and lease validity (remaining lease duration $> T + M = 1.5s$).
   - Current shard `term` and range `ownership_epoch`.
   - Dedup cache check for idempotency.
   - Follower is in `SYNCED` state (otherwise rejects immediately with `REPLICATION_UNAVAILABLE`).
3. **Sequence Assignment:** Primary assigns monotonically increasing, gap-free sequence number `seq`.
4. **Primary WAL Append:** Primary encodes the operation into an `OplogEntry` and durably writes + fsyncs to disk.
5. **Replication RPC:** Primary dispatches `POST /internal/replicate` with payload `(term, seq, op, doc_id, request_id)` to the follower.
6. **Follower Validation:** Follower verifies incoming `term >= local_term` and enforces `seq == last_seq + 1`.
7. **Follower WAL Append:** Follower appends the `OplogEntry` and durably fsyncs to its local disk.
8. **Follower ACK:** Follower returns durable acknowledgement `{"status": "ok", "seq": seq}`.
9. **Primary Memory Apply:** Primary applies the mutation to its in-memory B-Tree index and records the `request_id` in its FIFO dedup cache.
10. **Client Response:** Primary verifies lease is still valid; returns `outcome: "COMMITTED"`.

### 2.2 Read Path

- **Primary Reads (`read_mode: "primary"`):**
  - Serviced by primary replica holding an unexpired lease.
  - Provides linearizable consistency; always reflects the latest acknowledged writes.
  - Response metadata includes `served_by: "primary"` and `replication_lag: null`.
- **Follower Reads (`read_mode: "follower"`):**
  - Serviced by follower replica.
  - Provides bounded staleness; response metadata includes `served_by: "follower"` and `replication_lag: <ops_behind>`.
  - If the follower is unreachable or disconnected, router falls back to primary and tags `served_by: "primary"`.

### 2.3 Query Path (Scatter-Gather)

```
       Router (POST /query)
         │
         ├─── Parallel fan-out (asyncio.gather) ───┐
         │                                          │
         ▼                                          ▼
   Shard 0 (Primary)                          Shard 1 (Primary)
   Local index scan + filter                  Local index scan + filter
   Sort + limit(skip + limit)                 Sort + limit(skip + limit)
         │                                          │
         └───────────── Merge & Re-sort ────────────┘
                              │
                    Apply Global Skip & Limit
                              │
                    QueryResponse to Client
```

- **Shard-Key Routing Optimization:** Queries with an exact match on `doc_id` (`{"doc_id": {"$eq": 100}}` or `{"doc_id": 100}`) bypass scatter-gather and target only the owning shard.
- **Fan-Out:** Non-key filter queries fan out in parallel to all active shards — **one request per shard**, even if the shard owns several disjoint ranges. The request lists the ranges the router's map attributes to that shard; the shard restricts candidates to them before sorting and paginating.
- **Push-Down Limits:** Each shard applies local filtering, sorting, and extracts up to `skip + limit` candidate documents, and reports its exact match count for those ranges.
- **Router Merge:** Router merges candidate streams using a total order comparator, breaks ties deterministically using `doc_id`, applies global `skip` and `limit`, and returns the final paginated dataset. `total` is the sum of the shard counts (ranges in the map are disjoint, so nothing is double-counted); `meta` aggregates the shards' metadata.

---

## 3. Control Path & Consensus

### 3.1 Dynamic Shard Map Architecture

The cluster key space spans non-negative 64-bit integers: `[0, 2^63)`.
- Default bootstrap configuration partitions the space across 3 shards:
  - `shard0`: `[0, 3 074 457 345 618 258 602)`
  - `shard1`: `[3 074 457 345 618 258 602, 6 148 914 691 236 517 204)`
  - `shard2`: `[6 148 914 691 236 517 204, 2^63)`
- The coordinator enforces complete key coverage with zero gaps and zero overlaps.
- Every topology mutation increments `map_version`. Routers detect staleness on cache mismatch and re-sync dynamically from the coordinator.

### 3.2 Leader Leases & Monotonic Clocks

Primary leases prevent split-brain reads without consensus overhead on every read:
- **Timing Invariant:**
  $$H < L < D$$
  $$T + M < L - H$$
  Where:
  - $H = 1.0s$ (Heartbeat interval)
  - $L = 3.0s$ (Lease duration)
  - $D = 5.0s$ (Failure detection timeout)
  - $M = 0.5s$ (Safety margin)
  - $T = 1.0s$ (Replication timeout)
- **Monotonic Elapsed-Time Tracking:** Primary and Coordinator compute lease duration using monotonic clocks (`time.monotonic()`), eliminating susceptibility to wall-clock NTP jumps. The lease length a node derives from a coordinator grant is capped at its own configured $L$, so a coordinator wall-clock jump cannot extend it.
- **Lease Expiry Rule:** If a primary's remaining lease is less than $T + M = 1.5s$, it halts writes or waits for lease renewal before proceeding.

### 3.3 Fencing Primitives

Two distinct fencing primitives protect consistency:
1. `ownership_epoch`: Scoped per **key range**. Incremented on failover for all ranges owned by the failed shard, or during migration for the specific transferred range. Stale epoch writes are rejected with 409 `STALE_EPOCH`.
2. `term`: Scoped per **shard**. Incremented strictly on primary change. Follower rejects replication RPCs carrying an older term with `stale_term`.
3. **Shard identity:** every internal RPC carrying a `shard_id` is rejected with `409 WRONG_SHARD` if it reaches a node of a different shard.

---

## 4. Storage Engine Architecture

Each shard node maintains an isolated, durable storage engine:

```
┌────────────────────────────────────────────────────────┐
│ Shard Node Storage Engine                              │
│                                                        │
│  ┌───────────────────────┐   ┌───────────────────────┐ │
│  │ Write-Ahead Log (WAL) │   │ In-Memory B-Tree      │ │
│  │ (data/shard0.wal)     │   │ (SortedDict)          │ │
│  │ CRC32 JSON-lines      │   │ Primary Key: doc_id   │ │
│  └──────────┬────────────┘   └───────────▲───────────┘ │
│             │ Append + fsync             │             │
│             │                            │ Replay      │
│  ┌──────────▼────────────┐               │ on start    │
│  │ Snapshot Engine       │───────────────┘             │
│  │ (data/shard0.snap)    │                             │
│  │ Point-in-time state   │   ┌───────────────────────┐ │
│  └───────────────────────┘   │ Idempotency Dedup     │ │
│                              │ Bounded FIFO (100k)   │ │
│                              │ 10-minute window      │ │
│                              └───────────────────────┘ │
└────────────────────────────────────────────────────────┘
```

### 4.1 Write-Ahead Log (WAL)
- Format: newline-terminated JSON lines; each record carries a CRC32 of its canonical JSON.
- Every entry encodes: `seq`, `term`, `op`, `doc_id`, `request_id`, `payload`, `entry_id` (SHA-256 identity), and `crc32`.
- Append followed by `os.fsync(fd.fileno())` before returning acknowledgment.
- A torn tail (incomplete final line) is truncated on recovery; a complete record with a bad CRC32 makes the node refuse to start.
- Sequence numbers must be strictly consecutive: replay fails closed on duplicates, gaps or a gap between the snapshot and the first retained entry, and `append` rejects any seq other than the next one.
- After an uncertain write/flush/fsync failure the WAL instance refuses further appends until it is reopened and recovered, so a seq is never reused.

### 4.2 In-Memory Index & Snapshotting
- Primary storage index uses `sortedcontainers.SortedDict` keyed on `doc_id` for $O(\log N)$ point lookups and range scans.
- Snapshots are written periodically (every 1000 operations or 60 seconds, `SNAPSHOT_EVERY_N_OPS` / `SNAPSHOT_EVERY_S`) and on shutdown, atomically via temp file rename + fsync.
- Snapshot encapsulates: documents, applied sequence number, shard term, range epochs, and the idempotency dedup cache.
- **Safe WAL compaction** follows each periodic snapshot: the snapshot is read back and verified first; nothing is compacted during a migration write-pause, past `committed_seq`, past what the follower has acknowledged (unless it is more than 50 000 entries behind, in which case it resyncs from a snapshot), or past an active migration's cursor; the most recent 1 000 entries are always kept.
- Installing a snapshot from the primary resets the local WAL to continue right after it.

### 4.3 Idempotency Dedup Window
- Prevents double execution on network retries.
- Bounded to the last 100,000 requests or 10 minutes (whichever is hit first).
- Implemented as an insertion-ordered `dict` with FIFO eviction on capacity overflow.
- Rebuilt from WAL and snapshot state on restart.
