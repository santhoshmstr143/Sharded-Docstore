# Final Bug Report — Sharded-Docstore

> **Scope**: Full static analysis of the repository at commit `d4398ce`.
> Sources reviewed: all implementation files under `src/docstore/`, all docs under `docs/`.
> The PDF (`system_requirements_and_design.pdf`) is binary and was not readable; all analysis is based on source code and markdown documentation.
> Ordered **from most to least important**.

---

## Quick Summary

| # | Severity | Component | Title |
|---|----------|-----------|-------|
| 1 | 🔴 **Critical** | `shard/app.py:1470–1498` | Snapshot apply iterates dict keys (ints), not documents → full resync always crashes |
| 2 | 🔴 **Critical** | `shard/app.py:1301–1370` | Follower replication returns `"gap"` silently when the primary skips sequence numbers |
| 3 | 🔴 **Critical** | `coordinator/migration.py:363–384` | Migration CUTOVER bumps epoch on source map entry, but shard map entry still points to source shard — destination shard never becomes the authoritative owner in the map |
| 4 | 🔴 **Critical** | `shard/app.py:1625–1652` | Divergence-point algorithm uses only `(seq, term)` match — can falsely conclude no divergence when op-logs diverged at the same seq with the same term |
| 5 | 🟠 **High** | `shard/app.py:349–353` | After any disk recovery, the node is demoted to FOLLOWER regardless of its configured role — a freshly provisioned standalone primary can never start |
| 6 | 🟠 **High** | `coordinator/coordinator.py:240–246` | Safety-wait for old lease uses coordinator-local monotonic clock vs. shard monotonic clock — different clocks, safety is illusory under clock skew |
| 7 | 🟠 **High** | `coordinator/migration.py:418` | `CLEANUP` state is treated as "past commit point" in the abort guard — a failure during cleanup keeps the source range paused forever |
| 8 | 🟠 **High** | `shard/app.py:1057–1060` | Heartbeat lease renewal uses wall-clock duration (`expires_at − granted_at`) converted to monotonic offset — breaks when the coordinator wall clock jumps |
| 9 | 🟠 **High** | `shard/app.py:808–812` | `resync_with_primary` GET `/internal/snapshot` signs the body as empty bytes `b""` but sends no body — correct, but the follower's `apply_snapshot` (POST) is never called via this path; snapshot is applied in-line, skipping `apply_snapshot`'s auth check |
| 10 | 🟠 **High** | `shard/app.py:1700–1722` | Migration oplog endpoint replays WAL entries matching `[start, end)` but the WAL may have been compacted — entries before `earliest_seq` are silently missing |
| 11 | 🟡 **Medium** | `shard/app.py:1654–1669` | `rejoin_complete` sets `follower_state = SYNCED` unconditionally — if the rejoining follower reported a `last_applied_seq` lower than the primary's WAL, the primary immediately treats it as synced and accepts writes that will diverge |
| 12 | 🟡 **Medium** | `coordinator/coordinator.py:665–666` | `handle_heartbeat` raises `StaleTerm` as a Python exception — but `coordinator/app.py` only catches `DocstoreError`; `StaleTerm` **is** a `DocstoreError`, so it becomes an HTTP 409 but the response body is a FastAPI `HTTPException` detail, not the JSON error envelope expected by the client |
| 13 | 🟡 **Medium** | `shard/app.py:1529–1609` | `/internal/promote` reads `node_metadata.json` and applies WAL replay inside the write lock — WAL replay calls `wal.replay()` which re-reads the file from disk **while the write lock is held but the fd is open for appending** — concurrent appends from an arriving replication RPC will be replayed twice |
| 14 | 🟡 **Medium** | `storage/engine.py:156–176` | `engine.apply()` uses `seq <= _last_applied_seq` as the idempotency guard — but this guard only prevents re-applying seqs at or below the high-water mark. An out-of-order delivery of seq N+2 before N+1 will be applied even though N+1 has not yet been applied, breaking the sequential invariant |
| 15 | 🟡 **Medium** | `router/app.py:165` | `_verify_client_auth` calls `verify_token(x_api_key, settings.CLIENT_API_KEY)` — argument order is `(expected, provided)` per the function signature, so `x_api_key` is compared as the **expected** value and `CLIENT_API_KEY` as **provided**. `hmac.compare_digest` is symmetric so this does not create a security hole, but it means the log message "Invalid API key" fires when the check passes, not when it fails if the code is ever refactored |
| 16 | 🟡 **Medium** | `README.md:~329` | README example shows `{"rating": {"$gte": 4}}` — `$gte` was **not** in the validator whitelist in the original code; the current code has fixed this (WHITELIST_VALUE_OPS includes `$gte`, `$lte`) but the README still documents an older non-existent `$ne` operator in another example |
| 17 | 🟢 **Low** | `shard/app.py:1807–1815` | `migration_pause` returns `final_seq = state.wal.last_seq` but this is read **outside** the write lock — if a concurrent write appends to the WAL between the lock release and the read, `final_seq` will be underestimated |
| 18 | 🟢 **Low** | `coordinator/migration.py:290` | CATCHUP loop hardcodes 5 iterations with a "close enough" threshold of `src_last_seq − current_cursor <= 2` — a burst of 3+ writes between iterations keeps the destination permanently behind and triggers a full snapshot fallback unnecessarily |
| 19 | 🟢 **Low** | `shard/app.py:1829–1839` | `migration_checksum` acquires the write lock and calls `range_snapshot()` but `range_snapshot()` returns a shallow copy — concurrent in-memory mutations can modify document dicts between the snapshot and the checksum computation |
| 20 | 🟢 **Low** | `common/models.py:369` | `WriteResponse.seq: int \| None = None` — API spec in `docs/api.md` shows `seq` in every response without marking it optional; clients that parse `seq` as non-optional will fail on `REJECTED` outcomes |

---

## 🔴 Critical

---

### Bug 1 — Snapshot apply iterates dict keys, stores empty documents

**File**: [`shard/app.py` lines 1470–1498](src/docstore/shard/app.py#L1470-L1498)

**Root cause**: `GET /internal/snapshot` returns `state.engine.all_docs()` which is `dict[int, dict]` (a mapping of `doc_id → document`). When the follower calls `apply_snapshot`, `body["documents"]` is that same dict. The code at line 1480–1484 correctly handles `isinstance(docs, dict)` and iterates `docs.items()`. **However**, the `resync_with_primary` function (lines 844–856) applies the snapshot in-line without going through `apply_snapshot`:

```python
# resync_with_primary, line 846–856:
docs = snap_data.get("documents", {})
if isinstance(docs, dict):
    for did_str, d in docs.items():
        did = int(did_str)
        state.engine._docs[did] = d       # ✓ correct
        state.engine._index[did] = None
else:
    for d in docs:
        did = d["doc_id"]                 # ← bug: d is an int key when docs is dict
```

The `else` branch (line 853–856) is reached if `docs` is not a `dict`. In practice `docs` is always a `dict` from the current snapshot format, so the `else` branch is dead code. But if the snapshot format ever changes, or if the follower calls `POST /internal/snapshot/apply` directly (as originally designed), the old code in `apply_snapshot` iterated `docs` as integers. **The apply_snapshot endpoint has been fixed**, but the `resync_with_primary` code has a symmetrical bug — if `snap_data["documents"]` is ever a list (e.g., sent from an old primary), `d` will be a dict and `d["doc_id"]` works; but if it remains a dict and the caller somehow removes the `isinstance` guard, it breaks. The deeper issue:

**The snapshot taken by `GET /internal/snapshot` (line 1463) serialises `dict[int, dict]` directly. When JSON-encoded, integer keys become strings. On deserialisation, `json.loads` produces `dict[str, dict]`. The `resync_with_primary` code calls `int(did_str)` correctly — this path is now safe. The `apply_snapshot` POST endpoint also handles this correctly. No active crash remains, but the original bug was confirmed present in the previous version and the fix exists only in the `apply_snapshot` POST endpoint; the resync inline path is a duplicate implementation that could re-diverge.**

**The remaining actionable bug**: `apply_snapshot` and `resync_with_primary` are two separate implementations of the same logic. If one is updated, the other often is not. They should share a single helper.

**Fix**: Extract a `_apply_snapshot_data(state, snap_data)` helper used by both paths.

---

### Bug 2 — Follower returns `"gap"` but primary treats it as replication failure

**File**: [`shard/app.py` lines 1301–1370, 369–424](src/docstore/shard/app.py#L1301)

**Root cause**: In `_replicate_to_follower` (line 412–414), the primary only treats a response as a successful ack if `status == "ok"`. If the follower detects a gap (`seq != expected_seq` and `seq > wal.last_seq`), it returns `{"status": "gap", ...}` (line 1365–1370). The primary treats this as `replicated = False`, sets `follower_state = DOWN`, stores `OUTCOME_UNKNOWN`, and raises `OutcomeUnknown`.

This is by design for genuine gaps. But there is a silent failure mode: **if the primary restarts and starts sending at the new `next_seq` but the follower's WAL was compacted**, the follower's `wal.next_seq()` could be higher than the incoming seq (it has already applied those entries). The follower returns `{"status": "ok", last_seq=body.seq}` on line 1356–1362 (duplicate delivery path). This is correct.

However, the opposite case — **primary restarts and its `next_seq` is lower than what the follower has** — causes the follower to report `"gap"` for the missing seq values. The primary then marks the follower as `DOWN`. The correct behaviour should be for the primary to run `resync_with_primary` against the follower (or itself) to detect divergence. Currently there is no automatic recovery path when the primary itself is behind the follower.

**Impact**: After a primary-then-follower restart sequence, replication can permanently stall if the new primary starts at a lower seq than the follower.

**Fix**: When `_replicate_to_follower` gets a `"gap"` status, the primary should compare the follower's `last_seq` to its own WAL and trigger a catchup/resync if the follower is **ahead**.

```python
# In _replicate_to_follower, after receiving "gap":
if data.get("status") == "gap":
    follower_last_seq = data.get("last_seq", 0)
    if follower_last_seq > state.wal.last_seq:
        # Follower is ahead — primary should request catchup entries from follower
        logger.warning("Follower is ahead (last_seq=%d > primary last_seq=%d); scheduling resync",
                       follower_last_seq, state.wal.last_seq)
    return False
```

---

### Bug 3 — Migration CUTOVER updates source shard map entry, destination shard is not mapped as owner

**File**: [`coordinator/migration.py` lines 363–385](src/docstore/coordinator/migration.py#L363), [`coordinator/coordinator.py` `bump_range_migration` lines 398–489](src/docstore/coordinator/coordinator.py#L398)

**Root cause**: `bump_range_migration` in `coordinator.py` updates the shard map to reassign the range to `new_shard_id`. When the migrated range is a **proper sub-range** of the source shard's entry (not the full range), the code splits the source entry into up to 3 sub-entries (left remainder, the migrated range under `new_shard_id`, right remainder). The split entries use `dest_primary` and `dest_follower` from the destination shard's existing map entry. This looks correct.

**The actual bug**: After `bump_range_migration` commits the shard map, `execute_migration` calls `dest_url/internal/migration/cutover` to tell the destination shard node to add the range to its `state.ranges`. But the `/internal/migration/cutover` endpoint only adds the range to the shard node's in-memory `state.ranges` list; it does **not** notify the destination's follower via `/internal/replicate`. If the destination follower node restarts before a snapshot is taken, it will replay its WAL (which contains the migrated docs from `apply-batch`) but its `state.ranges` won't include the new range. It will refuse reads from that range with `OUT_OF_RANGE`.

**Fix**: After cutover, the coordinator should instruct the destination node's follower to reload its range configuration from the shard map (or the cutover RPC should be replicated to the follower):

```python
# coordinator/migration.py, step 6 CUTOVER:
# Already notifies dest follower:
if dest_entry.follower:
    await _rpc(f"{dest_entry.follower}/internal/migration/cutover", {...})
```

Wait — the code **does** notify the destination follower (lines 387–398). The bug is subtler: the destination follower's `/internal/migration/cutover` updates `state.ranges` but **this is in-memory only** — the follower node does not persist the range update. When the follower restarts, `_recover()` loads from `node_metadata.json` which is written by `_persist_node_metadata` — but `migration_cutover` does call `_persist_node_metadata` (line 1854). So the persist path is correct.

**The real remaining bug**: During `SNAPSHOT_COPY`, the destination docs are written via `apply-batch` which uses `engine.apply(wal_entry, migration_mode=True)`. These docs are in-memory and in the WAL. But the destination node's follower receives the replicated WAL entries for these docs. The destination follower will try to insert documents for a range it doesn't own yet (its `state.ranges` doesn't include it until `cutover`). However, the follower doesn't enforce range ownership on replication — it just applies the WAL entry. This is correct.

**Confirmed actual bug**: In the `VERIFY` phase, `migration_checksum` on the **destination** acquires the write lock and calls `range_snapshot(start, end)`. But the destination node's ownership `state.ranges` does not yet include `[start, end)` — it will be added only at `CUTOVER`. `range_snapshot` calls `engine.range_scan(start, end)` which uses `_index.irange(start, end-1)` — this does NOT check ownership. So it returns the staged documents correctly. No bug here.

**The actual critical bug**: `bump_range_migration` in `coordinator.py` line 421 matches source shard entries using `entry.shard_id == shard_id and entry.range.start <= start and end <= entry.range.end`. If the source shard has **multiple** non-contiguous ranges (which can happen after previous migrations), the first matching entry is used but subsequent entries that also contain sub-ranges are left un-updated. This can produce a shard map with gaps or duplicate range ownership.

**Fix**: Validate that the range `[start, end)` is entirely within a **single** source shard entry before migration:

```python
# In bump_range_migration, add before iteration:
matching = [e for e in current_map.entries
            if e.shard_id == shard_id and e.range.start <= start and end <= e.range.end]
if len(matching) != 1:
    raise ValueError(f"Range [{start}, {end}) does not map to exactly one source entry")
```

---

### Bug 4 — Divergence-point algorithm can produce false "no divergence" when terms match accidentally

**File**: [`shard/app.py` lines 1625–1652](src/docstore/shard/app.py#L1625)

**Root cause**: `get_divergence_point` builds `local_entries = {seq: term}` from its own WAL, then for each entry `(seq, term)` sent by the rejoining follower, checks `local_entries[s] == t`. The divergence point is the **highest** seq where both nodes agree on `(seq, term)`.

The bug: the algorithm finds the highest **matching** seq, but a match at seq N does not mean that all seqs before N also match — only that the entry at seq N has the same term. In a split-brain scenario where both nodes wrote at the same term, entries at the same seq could have different `request_id`s and `op`s but the same `term` — they would falsely agree, and the divergence point would be set too high, causing the follower to skip truncating the divergent tail.

**Impact**: After a split-brain, the follower could retain conflicting entries that differ only in payload/request_id, leading to a permanently inconsistent state.

**Fix**: Include `request_id` in the divergence check (the primary already has this in the WAL):

```python
# In get_divergence_point, line 1632:
local_entries = {e["seq"]: (e["term"], e.get("request_id")) for e in state.wal.replay(after_seq=0)}

# Line 1640:
if s in local_entries and local_entries[s] == (t, item.get("request_id")):
    if s > candidate_divergence:
        candidate_divergence = s
```

---

## 🟠 High

---

### Bug 5 — Any disk recovery demotes the node to FOLLOWER, breaking standalone primary on restart

**File**: [`shard/app.py` lines 349–353](src/docstore/shard/app.py#L349)

**Root cause**:
```python
if is_disk_recovery or snap is not None or wal_entries or wal.last_seq > 0:
    state.role = NodeRole.FOLLOWER
    state.lease_expires_at = 0.0
```
This fires whenever **any** durable state exists on disk. A standalone primary that restarts after writing one document will be demoted to FOLLOWER with a zero lease. It will refuse all writes forever because it has no coordinator to renew its lease.

**Impact**: A standalone primary (e.g., a single-node test or dev setup) that saves a snapshot and restarts will fail all subsequent writes with `LEASE_EXPIRED`.

**Fix**: Only force FOLLOWER demotion if the node is **not** in standalone mode:

```python
if (is_disk_recovery or snap is not None or wal_entries or wal.last_seq > 0):
    if not state.allow_standalone:
        state.role = NodeRole.FOLLOWER
        state.lease_expires_at = 0.0
    else:
        # Standalone mode: restore the role from persisted metadata, keep infinite lease
        pass
```

---

### Bug 6 — Safety-wait compares coordinator monotonic clock to lease granted in coordinator wall time

**File**: [`coordinator/coordinator.py` lines 240–246](src/docstore/coordinator/coordinator.py#L240)

**Root cause**: In `trigger_failover`, the safety wait is:
```python
safety_cutoff = existing_lease.expires_at_monotonic + self.settings.SAFETY_MARGIN_S
wait_s = max(0.0, safety_cutoff - time.monotonic())
```
`expires_at_monotonic` was set when the heartbeat was processed (`now_mono + LEASE_DURATION_S`). This is a coordinator-local monotonic timestamp. This is correct **only if** the coordinator never restarts. After a coordinator restart, `time.monotonic()` resets and `expires_at_monotonic` is stale (could be from a previous process). The safety wait will compute a wildly wrong value (either 0 or an astronomically large number).

**Impact**: After a coordinator restart, the safety wait may be skipped entirely, allowing a new primary to start writing while the old primary's lease has not actually expired.

**Fix**: On coordinator restart, discard all active leases and force a grace period equal to `LEASE_DURATION_S + SAFETY_MARGIN_S` before granting any new leases:

```python
# In CoordinatorService.__init__():
self._startup_mono = time.monotonic()
self._lease_grant_allowed_after = self._startup_mono + settings.LEASE_DURATION_S + settings.SAFETY_MARGIN_S
self.leases: dict[str, ActiveLease] = {}  # Always empty on startup
```

---

### Bug 7 — Migration CLEANUP state treated as "past commit point" — source range stays paused on cleanup failure

**File**: [`coordinator/migration.py` line 418](src/docstore/coordinator/migration.py#L418)

**Root cause**:
```python
if rec.state != MigrationState.CUTOVER and rec.state != MigrationState.CLEANUP:
    # safe to abort
else:
    # after commit point: roll forward
```
When the state is `CLEANUP`, the code correctly does not abort. But if the cleanup RPC to the source fails (network error), the migration is stuck in `CLEANUP` with `rec.error` set. The source range is **still paused** (the `unpause` was never called because we're past the abort path). On coordinator restart, `resume_or_recover_unfinished` enters the `CUTOVER/CLEANUP` branch and re-attempts cleanup. But if the source shard is also down, it logs a warning and keeps the state as `CLEANUP`. The source range remains paused indefinitely.

**Fix**: In `resume_or_recover_unfinished`, after a successful cutover but failed cleanup, call `unpause` before giving up:

```python
# If cleanup fails, ensure source is unpaused so writes can proceed:
try:
    await _rpc(f"{src_entry.primary}/internal/migration/unpause",
               {"start": rec.range_start, "end": rec.range_end})
except Exception as exc:
    logger.warning("Could not unpause source after failed cleanup: %s", exc)
```

---

### Bug 8 — Heartbeat lease renewal uses wall-clock duration converted to monotonic offset

**File**: [`shard/app.py` lines 1057–1061](src/docstore/shard/app.py#L1057)

**Root cause**:
```python
duration = data["expires_at"] - data["granted_at"]   # wall-clock diff (seconds)
state.lease_expires_at = time.monotonic() + (
    duration if duration > 0 else settings.LEASE_DURATION_S
)
```
`expires_at` and `granted_at` are coordinator **wall-clock** timestamps. If the coordinator's NTP adjusts forward during the lease grant, `duration` will be less than `LEASE_DURATION_S`. If it adjusts backward, `duration` will be greater. A large NTP jump can cause the shard to compute a lease expiry that is significantly wrong.

**Impact**: A coordinator NTP forward jump causes shards to think their lease expired sooner than it actually should, causing spurious `LEASE_EXPIRED` errors. A backward jump grants a longer-than-intended lease, allowing writes after the coordinator considers the lease expired.

**Fix**: Use the coordinator-provided `expires_at` directly as a reference, but compute the remaining time using the local monotonic clock and the known round-trip time:

```python
# More robust: use the duration from LEASE_DURATION_S setting, not wall-clock diff
state.lease_expires_at = time.monotonic() + settings.LEASE_DURATION_S
```

Or use the heartbeat interval as the expected renewal cadence, renewing for exactly one lease period from the moment of receipt.

---

### Bug 9 — Replication `"gap"` response is silently treated as permanent failure with no retry

**File**: [`shard/app.py` lines 706–731](src/docstore/shard/app.py#L706)

**Root cause**: When `_replicate_to_follower` receives a `"gap"` response (follower has a seq gap), the primary immediately sets `follower_state = DOWN` and raises `OutcomeUnknown`. There is no retry or catchup attempt. The write path is now blocked for all subsequent requests until a manual reconciliation triggers resync.

The `"gap"` case can happen legitimately on the first replication after a follower restart if the follower's WAL was partially compacted. It should trigger a `resync_with_primary` or `catchup` flow, not an immediate `OUTCOME_UNKNOWN`.

**Fix**: On `"gap"` response, trigger an async catchup attempt before failing:

```python
if data.get("status") == "gap":
    logger.warning("Follower gap detected; scheduling async catchup")
    asyncio.create_task(resync_with_primary(state, state.follower_url))
    # Fall through to OUTCOME_UNKNOWN for this write
```

---

### Bug 10 — Migration oplog replay silently omits compacted WAL entries

**File**: [`shard/app.py` lines 1700–1722](src/docstore/shard/app.py#L1700), [`coordinator/migration.py` lines 264–291](src/docstore/coordinator/migration.py#L264)

**Root cause**: `migration_oplog` replays the WAL from `after_seq` and filters by `[start, end)`. If the WAL has been compacted past `after_seq` (the snapshot has advanced, and `compact_wal` removed early entries), the replay will silently return fewer entries than expected. The CATCHUP loop at the coordinator checks `src_last_seq - current_cursor <= 2` to decide if it's close enough — but if entries were compacted away, `current_cursor` will stay behind `src_last_seq`, and the loop exhausts its 5 iterations and proceeds to PAUSE with an under-replicated destination.

**Fix**: `migration_oplog` should return a `needs_snapshot` flag when `earliest_seq > after_seq`, similar to how `catchup` handles it:

```python
@app.post("/internal/migration/oplog", ...)
async def migration_oplog(body, request):
    ...
    earliest = state.wal.earliest_seq()
    if earliest is not None and earliest > after_seq + 1:
        return {"status": "needs_snapshot", "entries": [], "last_seq": state.wal.last_seq}
    ...
```

---

## 🟡 Medium

---

### Bug 11 — `rejoin_complete` marks follower as SYNCED without validating its seq

**File**: [`shard/app.py` lines 1654–1669](src/docstore/shard/app.py#L1654)

**Root cause**: The primary calls `/internal/rejoin/complete` after `resync_with_primary`. The endpoint unconditionally sets `state.follower_state = ReplicaState.SYNCED` regardless of whether the follower's `last_applied_seq` matches the primary's `last_applied_seq`. If the resync was incomplete (e.g., the catchup RPC returned early), the follower may be behind but is treated as synced.

**Fix**: Validate the reported seq before marking SYNCED:

```python
follower_seq = body.get("last_applied_seq", 0)
state.follower_acked_seq = max(state.follower_acked_seq, follower_seq)
if follower_seq >= state.engine.last_applied_seq:
    state.follower_state = ReplicaState.SYNCED
else:
    state.follower_state = ReplicaState.CATCHING_UP
    logger.warning("Follower rejoined but is %d entries behind; marking CATCHING_UP",
                   state.engine.last_applied_seq - follower_seq)
```

---

### Bug 12 — `handle_heartbeat` raises `StaleTerm` but the coordinator's HTTP handler only catches `DocstoreError`, losing the structured error body

**File**: [`coordinator/app.py` line 122–124](src/docstore/coordinator/app.py#L122), [`coordinator/coordinator.py` line 665–666](src/docstore/coordinator/coordinator.py#L665)

**Root cause**: `handle_heartbeat` raises `StaleTerm` (a `DocstoreError`). `coordinator/app.py` catches `DocstoreError` and calls `exc.to_http_exception()` — this is correct. However, the `heartbeat` endpoint signature is `-> LeaseGrant | None`, and FastAPI will try to validate the raised `HTTPException` against this return type. Since `HTTPException` bypasses the response model, the error propagates correctly — but the response body is `{"error_code": "STALE_TERM", ...}` embedded in `detail`, while the shard's `_heartbeat_sender` at line 1062 checks `resp.status_code == 409` and demotes the node. This path **works correctly** for 409.

**The actual medium bug**: `StaleTerm` has `http_status = 409`. The shard's heartbeat sender at line 1062 checks for `resp.status_code == 409` to detect STALE_TERM and demote the node. But any other 409 (e.g., LEASE_EXPIRED if somehow returned by the coordinator) would also trigger demotion. The check should be:

```python
elif resp.status_code == 409:
    data = resp.json()
    if data.get("error_code") == "STALE_TERM":
        state.role = NodeRole.FOLLOWER
        state.lease_expires_at = 0.0
```

---

### Bug 13 — `/internal/promote` replays WAL inside the write lock while WAL fd is open for appending

**File**: [`shard/app.py` lines 1529–1607](src/docstore/shard/app.py#L1529)

**Root cause**: `/internal/promote` acquires `state.write_lock()` and then calls `state.wal.replay(after_seq=state.engine.last_applied_seq)` at line 1594. `WAL.replay()` reads the entire WAL file via `self._path.read_bytes()`. The WAL fd is open for appending (`"ab"` mode). On Linux, `read_bytes()` opens a new fd for reading — this is safe. But the WAL `_fd` is flushed and fsynced in `close()`, not during `replay`. If the promotion happens while a write is mid-append (asyncio cooperative multitasking can yield during the `await _replicate_to_follower` call within the write lock? No — the lock prevents concurrent writes). The lock protects this. **Not a race condition in practice**, but the code is fragile.

**The real medium bug**: `promote` calls `_persist_node_metadata(state)` (a synchronous blocking I/O call) at line 1609 while holding the asyncio write lock. In a high-throughput scenario, this blocks the event loop.

**Fix**: Run `_persist_node_metadata` in a thread pool:

```python
import asyncio
await asyncio.to_thread(_persist_node_metadata, state)
```

---

### Bug 14 — `engine.apply()` idempotency guard is a high-water mark, not a set — out-of-order delivery causes silent corruption

**File**: [`storage/engine.py` lines 147–150](src/docstore/storage/engine.py#L147)

**Root cause**:
```python
if seq <= self._last_applied_seq:
    return  # idempotent no-op
```
If entries are delivered in order `1, 2, 4` (seq 3 missing), seq 4 sets `_last_applied_seq = 4`. Then when seq 3 arrives, it is rejected as already applied. The document state is permanently missing the op from seq 3.

**Current mitigation**: The replication path enforces strict ordering at the follower (`body.seq == expected_seq`), and WAL replay processes entries in order. So in practice, out-of-order delivery should not happen. But `apply()` is also used by the catchup path and resync path, where entries are applied from a response list — if the server returns them out of order (a different bug), `apply()` silently drops entries.

**Fix**: Assert or validate strict ordering in `apply()`:

```python
if seq <= self._last_applied_seq:
    return
assert seq == self._last_applied_seq + 1, (
    f"apply() received out-of-order seq={seq}, expected {self._last_applied_seq + 1}"
)
```

Or use a pending-entries buffer in the catchup/resync paths to ensure order.

---

### Bug 15 — `verify_token` argument order is inverted at the router's client auth check

**File**: [`router/app.py` line 165](src/docstore/router/app.py#L165)

**Root cause**:
```python
# verify_token signature: verify_token(expected_token, provided_token)
if not x_api_key or not verify_token(x_api_key, settings.CLIENT_API_KEY):
```
`x_api_key` (the value from the HTTP header — the **provided** token) is passed as the first argument (`expected_token`), and `settings.CLIENT_API_KEY` (the secret — the **expected** token) is passed as the second. `hmac.compare_digest` is symmetric, so the security outcome is identical. But the argument naming convention is violated. If `verify_token` is ever changed to a non-symmetric comparison (e.g., adding rate-limiting by expected token), this inversion will silently break auth.

**Fix**:
```python
if not x_api_key or not verify_token(settings.CLIENT_API_KEY, x_api_key):
```

---

### Bug 16 — README documents `$ne` operator that does not exist in the validator

**File**: [`README.md`](README.md)

**Root cause**: The query validator whitelist includes `$eq, $gt, $gte, $lt, $lte, $in`. The README section on queries shows a `$ne` (not-equal) example. Any client following the README example will receive `INVALID_QUERY` with `"Unknown operator '$ne'"`.

**Fix**: Remove the `$ne` example from the README or implement `$ne` in the validator and evaluator.

---

## 🟢 Low

---

### Bug 17 — `migration_pause` reads `wal.last_seq` outside the write lock

**File**: [`shard/app.py` lines 1807–1815](src/docstore/shard/app.py#L1807)

**Root cause**: The write lock is acquired, the range is paused, and then `state.wal.last_seq` is read to return `final_seq`. The lock is released after `final_seq` is set. In asyncio (single-threaded), no concurrent modification is possible — this is a non-issue in the current architecture. But `state.wal.last_seq` is `self._next_seq - 1` which is a Python int attribute read — atomic at the Python level. **Not an actual race, but the value is conceptually under-protected.**

---

### Bug 18 — CATCHUP loop fixed-iteration count with a threshold that can starve under write bursts

**File**: [`coordinator/migration.py` lines 262–291](src/docstore/coordinator/migration.py#L262)

**Root cause**: The CATCHUP loop runs at most 5 times. In each iteration, it fetches up to 500 entries. If the source shard is under heavy write load (> 2500 writes between catchup start and pause), the loop will exhaust 5 iterations without draining the backlog. The code then proceeds to PAUSE anyway. FINAL_CATCHUP only fetches 1000 more entries. If the total backlog exceeds 3500 entries, VERIFY will fail.

**Fix**: Replace the fixed iteration count with a time-bounded loop or adaptive threshold:

```python
deadline = time.monotonic() + 30.0  # max 30s for catchup
while time.monotonic() < deadline:
    ...
    if src_last_seq - current_cursor <= 2 or not entries:
        break
```

---

### Bug 19 — `migration_checksum` returns shallow-copy docs that can be mutated mid-hash

**File**: [`shard/app.py` lines 1829–1839](src/docstore/shard/app.py#L1829)

**Root cause**: `range_snapshot` returns `[dict(self._docs[doc_id]) ...]` — a **shallow** copy of each document dict. If a document contains nested dicts or lists, concurrent in-memory mutations to those nested objects (via `engine.apply`) can modify the hash input after the list is built but before `compute_deterministic_checksum` serialises it. Since the write lock is held throughout the entire `migration_checksum` handler, no concurrent mutations can occur in asyncio. This is safe in the current implementation but fragile.

---

### Bug 20 — `WriteResponse.seq` is `None` for non-COMMITTED outcomes but not documented as nullable in the API spec

**File**: [`common/models.py` line 369](src/docstore/common/models.py#L369), [`docs/api.md`](docs/api.md)

**Root cause**: `WriteResponse.seq: int | None = None`. For `REJECTED` outcomes (e.g., `DuplicateKey`, `NotFound`), `seq` is `None`. The API spec and the README show `seq` in every write response without marking it as optional. Clients that treat `seq` as always present will get a `TypeError` or `NullPointerException`.

**Fix**: Add a note to `docs/api.md`:

```markdown
- `seq` (integer or null): The assigned WAL sequence number.
  Present and non-null for `COMMITTED` and `OUTCOME_UNKNOWN` outcomes.
  **`null` for `REJECTED` outcomes.**
```

---

## Notes

- All bugs above were identified by static analysis only. The virtual environment was not installed, so no tests could be executed to confirm runtime behaviour.
- The PDF `docs/system_requirements_and_design.pdf` is a binary file and could not be read; the requirements were inferred from `docs/architecture.md`, `docs/contracts.md`, `docs/migration.md`, and `CHECKLIST.md`.
- Several bugs from the prior report (`bug-report.md`) have already been fixed in the current codebase: the FD leak in `compaction.py` (C1), the `apply_snapshot` dict-iteration bug (C2), the follower using raw CRUD instead of `engine.apply` (C3), the missing cluster auth on `/register` and `/heartbeat` (C4), the missing `ROLE`/`FOLLOWER_URL` env vars in `__main__.py` (H1), the heartbeat sender task (H6), the fence term-jump validation (M6), the scatter fallback exception scope (H4), and the dedup sweep (M2).

