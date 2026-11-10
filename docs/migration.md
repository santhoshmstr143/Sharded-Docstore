# Live Range Migration Specification — Sharded-Docstore

This document specifies the protocol for live range migration across shards without downtime.

---

## 1. Migration State Machine

The migration of a key range $[R_{start}, R_{end})$ from Shard A (Source) to Shard B (Destination) follows a deterministic multi-phase state machine:

```
┌─────────┐
│ PLANNED │
└────┬────┘
     │ 1. Snapshot taken at seq S on Source
     ▼
┌───────────────┐
│ SNAPSHOT_COPY │ Source streams snapshot data to Destination
└────┬──────────┘
     │ 2. Destination applies snapshot (invisible to queries)
     ▼
┌─────────┐
│ CATCHUP │ Destination replays oplog entries from S to S_current
└────┬────┘
     │ 3. Lag falls below catchup threshold
     ▼
┌───────┐
│ PAUSE │ Source pauses writes on [R_start, R_end) (503 RANGE_PAUSED)
└────┬──┘
     │ 4. Drain remaining pending ops
     ▼
┌───────────────┐
│ FINAL_CATCHUP │ Destination replicates remaining oplog entries
└────┬──────────┘
     │ 5. Verify byte count and record count
     ▼
┌────────┐
│ VERIFY │ Consistency verification between Source and Destination
└────┬───┘
     │ 6. Commit Point: Coordinator bumps ownership_epoch & map_version
     ▼
┌─────────┐
│ CUTOVER │ Destination begins serving writes & reads for range
└────┬────┘
     │ 7. Asynchronous deletion of range data on Source
     ▼
┌─────────┐
│ CLEANUP │
└────┬────┘
     │ 8. Migration marked complete
     ▼
┌──────┐
│ DONE │
└──────┘
```

---

## 2. Invisibility Invariant & The Commit Point

### 2.1 Invisibility Invariant
> **Invariant:** During `SNAPSHOT_COPY`, `CATCHUP`, and `FINAL_CATCHUP`, all data replicated to Destination is **completely invisible** to client queries on Destination until the `CUTOVER` phase.

This prevents dirty reads, partial reads, or duplicate results across shards while migration is in progress.

### 2.2 The Migration Commit Point
The atomic commit point of a migration occurs when:
1. Destination verifies identical state with Source.
2. The **Coordinator atomically persists the updated shard map**, incrementing:
   - The `ownership_epoch` of the migrated range $[R_{start}, R_{end})$.
   - The cluster `map_version`.
3. If a failure occurs before this atomic update, the migration **aborts safely**, and the Source unpauses the range.
4. An exception from the map update does **not** prove the map is unchanged (the map file may already have been replaced). The coordinator re-reads the persisted map: if it gives the range to the Destination, the migration **rolls forward**; it aborts only if the map provably still names the Source; if the map cannot be read, the record stays in `CUTOVER` and is never aborted.
5. `VERIFY` and the map update run while holding the **topology locks** of both shards, so no failover can interleave. Inside the locks the coordinator re-validates that both primaries and both shard terms are unchanged since the migration started; otherwise it aborts before cutover.

---

## 3. Router Behavior During Migration

1. **Routing during `PAUSE`:** When a client issues a write to the migrating range during the brief `PAUSE` phase, the source returns HTTP 503 with error code `RANGE_PAUSED`.
2. **Router Retry:** The router intercepts `RANGE_PAUSED`, applies bounded exponential backoff, queries the coordinator for a newer `map_version`, and reroutes the request to the new owner once cutover finishes.

---

## 4. Transport, Idempotency & Crash Safety

- **Paginated snapshot copy:** the source serves the range in pages (500 docs, also bounded by bytes) via a `doc_id` cursor. The copy is "fuzzy" (later pages may include writes after the first page), which is safe because catch-up replays **every** source op after the first page's `snapshot_seq`, in order and idempotently, and `VERIFY` checks the final checksums. Copy progress (`last_copied_doc_id`, `docs_copied`) is persisted in the migration record.
- **Byte-bounded batches:** documents and oplog entries are sent to the destination in batches bounded by both count (200) and serialized size (max body − 64 KB). A single item that can never fit aborts the migration (documents are limited to 1 MB, so this does not happen for valid data).
- **Idempotent apply:** each copied document / replayed op has a deterministic request id; a retried batch is recognised through the dedup table and is not appended to the WAL again.
- **WAL retention:** while a migration is copying or catching up, the source pins its WAL after the migration's cursor so periodic compaction cannot remove entries still needed (the pin lapses after 5 minutes of inactivity, e.g. an abandoned migration).
- **Mandatory follower cutover:** the destination primary **and its follower** must both confirm the new ownership before `CLEANUP`. If the follower is unreachable the record stays in `CUTOVER`; the coordinator's health monitor retries post-cutover migrations every few seconds until they reach `DONE`.
- **Durable records:** migration records are written atomically (fsync + rename + directory fsync). A record that cannot be parsed makes the coordinator **refuse to start** rather than silently forget an in-flight ownership transfer.
- **Restart:** after a coordinator restart, migrations past cutover roll forward; migrations before cutover are aborted (source unpaused, staged destination data purged).
- **Durable before irreversible:** after the map flip the coordinator re-confirms the map file is crash-durable (file + directory fsync). If that fails, the record stays in `CUTOVER` and nothing on the source is released or deleted until a retry succeeds — a power loss could otherwise bring back the old map after the source gave its data away.
- **Post-cutover order:** (1) the source *releases* the range — it and its follower durably stop owning it, so a router with a stale map gets `OUT_OF_RANGE` and refreshes instead of reading the old copy; (2) the destination primary and follower take ownership; (3) the source purges its copy and unpauses. Addresses are re-read from the current map at each step.
- **Idempotency moves with the data:** committed client dedup records for the range travel with the snapshot pages, and replayed operations keep their original `request_id`, so a client retrying after cutover gets its original result. (These imported records are not yet replicated to the destination's follower.)
- **Real replicas only:** a destination without a follower cannot accept migrated data in cluster mode, and registrations that would change a shard's map are deferred (`TOPOLOGY_BUSY`) while a migration on that shard holds its topology lock. The destination follower address is re-validated before cutover.
- **Abort is confirmed, not assumed:** an aborting migration is recorded as `ABORTING` (non-terminal) until the source has been unpaused *and* the destination's staged data purged; only then is it `ABORTED`. The coordinator's migration monitor retries `ABORTING`, `CUTOVER` and `CLEANUP` records every few seconds, so a failed unpause can no longer leave a range paused forever.
- **Failed replication never orphans a log entry:** if replicating a migration copy, catch-up or purge operation fails after the destination appended it to its WAL, the entry is tracked as pending (`OUTCOME_UNKNOWN`) exactly like a client write and resolved with the follower before anything else is written.
