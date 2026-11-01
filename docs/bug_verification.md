# Bug Verification Report — Sharded-Docstore

> Re-verified each bug from `final_bug_report.md` against the actual source code at commit `d4398ce`.
> Verdict key: ✅ **Confirmed** | ⚠️ **Partially true** | ❌ **False positive**

---

## Summary Table

| # | Severity | Verdict | Short reason |
|---|----------|---------|--------------|
| 1 | 🔴 Critical | ⚠️ **Partially true** | Two implementations exist (DRY risk), but both are now correct — no active crash |
| 2 | 🔴 Critical | ⚠️ **Partially true** | The "gap" path exists, but the primary does NOT set follower DOWN; it just returns `False` |
| 3 | 🔴 Critical | ✅ **Confirmed** | `bump_range_migration` has no guard against a source shard having multiple matching entries |
| 4 | 🔴 Critical | ✅ **Confirmed** | Divergence check uses only `(seq, term)` — `request_id` is not in the comparison |
| 5 | 🟠 High | ✅ **Confirmed** | Recovery block always sets `FOLLOWER` regardless of `allow_standalone` |
| 6 | 🟠 High | ✅ **Confirmed** | Safety-wait uses `expires_at_monotonic` which is stale after coordinator restart |
| 7 | 🟠 High | ✅ **Confirmed** | CLEANUP failure leaves source paused; the `else` branch does not unpause |
| 8 | 🟠 High | ✅ **Confirmed** | Lease duration computed from wall-clock diff (`expires_at − granted_at`) |
| 9 | 🟠 High | ❌ **False positive** | Primary does NOT set follower DOWN on "gap" — `_replicate_to_follower` just returns `False` and the write-path sets DOWN; same as any replication failure |
| 10 | 🟠 High | ✅ **Confirmed** | `migration_oplog` has no `needs_snapshot` guard; compacted WAL entries silently missing |
| 11 | 🟡 Medium | ⚠️ **Partially true** | `rejoin_complete` marks SYNCED unconditionally — but seq is tracked in `follower_acked_seq` |
| 12 | 🟡 Medium | ⚠️ **Partially true** | Any 409 demotes — but coordinator only returns 409 for `StaleTerm`, so in practice this is safe |
| 13 | 🟡 Medium | ⚠️ **Partially true** | `_persist_node_metadata` is blocking I/O in write lock, but WAL replay race claim is wrong |
| 14 | 🟡 Medium | ⚠️ **Partially true** | Guard is high-water-mark not a set — real, but out-of-order delivery cannot happen through the current write/replicate path |
| 15 | 🟡 Medium | ❌ **False positive** | `hmac.compare_digest` is symmetric; argument order has zero security or functional effect |
| 16 | 🟡 Medium | ❌ **False positive** | README does NOT show `$ne`; it shows `$gte` and `$in` — both are in the whitelist |
| 17 | 🟢 Low | ❌ **False positive** | `wal.last_seq` is read **inside** the write lock (lock is held when `final_seq` is assigned) |
| 18 | 🟢 Low | ✅ **Confirmed** | 5-iteration cap + 500-entry limit is real; heavy burst can exhaust without convergence |
| 19 | 🟢 Low | ⚠️ **Partially true** | Shallow copy is real but write lock is held throughout — concurrent mutation is impossible |
| 20 | 🟢 Low | ✅ **Confirmed** | `WriteResponse.seq: int | None = None` confirmed; API spec does not mark it nullable |

---

## Detailed Verdicts

---

### Bug 1 — Snapshot apply: two implementations (DRY risk)

**Verdict: ⚠️ Partially true**

**Code checked** (`shard/app.py` lines 844–870):

```python
# resync_with_primary inline apply (lines 846–856):
docs = snap_data.get("documents", {})
if isinstance(docs, dict):
    for did_str, d in docs.items():
        did = int(did_str)        # ✓ correct
        state.engine._docs[did] = d
        state.engine._index[did] = None
else:
    for d in docs:
        did = d["doc_id"]         # ← runs if docs is a list (e.g. from old primary)
        state.engine._docs[did] = d
        state.engine._index[did] = None
```

Both `apply_snapshot` (POST) and the inline `resync_with_primary` code handle `isinstance(docs, dict)` correctly. **There is no active crash**. The report correctly identifies that two independent copies of the same logic exist (DRY risk), but overstates it as a critical bug. It is a **code-quality / maintenance issue**, not a runtime bug. The `else` branch for list format is correct if a list is ever returned.

**Downgrade**: 🔴 Critical → 🟢 Low (maintenance risk, not a runtime defect).

---

### Bug 2 — Follower "gap" response treated as permanent failure

**Verdict: ⚠️ Partially true — the framing is wrong, but a real issue exists**

**Code checked** (`shard/app.py` lines 412–418 — `_replicate_to_follower`):

```python
if resp.status_code == 200:
    data = resp.json()
    return bool(data.get("status") == "ok")   # "gap" returns False here
logger.warning(...)
return False
```

Then in the write path (lines 710–731):
```python
replicated = await _replicate_to_follower(...)
if not replicated:
    state.follower_state = ReplicaState.DOWN   # ← this is where DOWN is set
    ...
    raise OutcomeUnknown(...)
```

The report claimed `_replicate_to_follower` itself sets follower DOWN — **that's wrong**. The function just returns `False`. The **write path** sets follower DOWN when `replicated == False` — which it does for **any** failure including "gap". So the root behaviour (a "gap" response → follower DOWN, no retry) is real, but the described mechanism was slightly incorrect.

The underlying concern — that a "gap" response triggers the same `OUTCOME_UNKNOWN` + `DOWN` path as a network failure, with no auto-recovery — is **confirmed valid**.

**Verdict remains**: Real issue, description was imprecise. The "gap" path is handled identically to a network failure; there is no dedicated catchup trigger.

---

### Bug 3 — `bump_range_migration` can produce gaps when source has multiple ranges

**Verdict: ✅ Confirmed**

**Code checked** (`coordinator/coordinator.py` lines 420–476):

```python
for entry in current_map.entries:
    if entry.shard_id == shard_id and entry.range.start <= start and end <= entry.range.end:
        found = True
        ...
        # Splits this entry into up to 3 sub-entries
    else:
        new_entries.append(...)   # ← all other entries pass through unchanged
```

The loop iterates ALL entries. If the source shard has two entries (e.g. `[0, 100)` and `[200, 300)`), and the migration is for range `[50, 80)` (inside the first), the code **correctly** splits the first entry. But there is no pre-check that exactly ONE source entry contains the range. If by some shard-map corruption both entries claim to contain `[50, 80)` (which could happen after previous botched migrations), both would match `found = True` and both would be split — producing duplicate ranges. The `found = True` flag only prevents the "not found" error; it does not limit to a single match.

**Confirmed as a real validation gap.**

---

### Bug 4 — Divergence-point check uses only `(seq, term)`, not `request_id`

**Verdict: ✅ Confirmed**

**Code checked** (`shard/app.py` lines 1632–1642):

```python
local_entries = {e["seq"]: e["term"] for e in state.wal.replay(after_seq=0)}
...
for item in entries:
    s = item.get("seq", 0)
    t = item.get("term", 0)
    if s in local_entries and local_entries[s] == t:   # ← only checks (seq, term)
        if s > candidate_divergence:
            candidate_divergence = s
```

The follower sends `{"seq": e["seq"], "term": e["term"]}` (line 812) — `request_id` is **not** sent at all. So even if the fix were applied on the primary side, the follower would need to send `request_id` too.

The scenario requires a split-brain where two primaries write different operations at the same seq with the same term — this is only possible if fencing fails (which the design prevents with epoch checks). However, the divergence algorithm's correctness relies purely on `(seq, term)` matching, which is technically insufficient as stated. **Confirmed as a theoretical correctness gap.**

---

### Bug 5 — Any disk recovery demotes node to FOLLOWER regardless of standalone mode

**Verdict: ✅ Confirmed**

**Code checked** (`shard/app.py` lines 349–353):

```python
if is_disk_recovery or snap is not None or wal_entries or wal.last_seq > 0:
    state.role = NodeRole.FOLLOWER
    state.lease_expires_at = 0.0
```

No `allow_standalone` check here. A standalone primary that writes one doc and restarts will have `wal.last_seq > 0` → unconditionally demoted to FOLLOWER → zero lease → all writes fail with `LEASE_EXPIRED`. **Confirmed.**

---

### Bug 6 — Safety-wait uses stale `expires_at_monotonic` after coordinator restart

**Verdict: ✅ Confirmed**

**Code checked** (`coordinator/coordinator.py` lines 239–246):

```python
existing_lease = self.leases.get(shard_id)    # leases dict is always empty on restart
wait_s = 0.0
if existing_lease:
    safety_cutoff = existing_lease.expires_at_monotonic + self.settings.SAFETY_MARGIN_S
    wait_s = max(0.0, safety_cutoff - time.monotonic())
```

`self.leases` is initialized as `{}` in `__init__` (line 92) and is never persisted to disk. After a coordinator restart, `self.leases` is always empty, so `existing_lease` is `None` and `wait_s = 0.0` — **the safety wait is always skipped after a coordinator restart**. This means a new primary can be promoted immediately after coordinator restart, even if the old primary's lease has not expired. **Confirmed.**

---

### Bug 7 — CLEANUP failure keeps source range paused forever

**Verdict: ✅ Confirmed**

**Code checked** (`coordinator/migration.py` lines 416–458):

```python
except Exception as exc:
    if rec.state != MigrationState.CUTOVER and rec.state != MigrationState.CLEANUP:
        # Abort path: unpause source ✓
        ...
    else:
        # After commit point: roll forward, save error, RE-RAISE
        rec.error = str(exc)
        self.save_record(rec)
        raise   # ← source remains paused, no unpause called
```

When the CLEANUP RPC fails, the exception handler's `else` branch saves the error and re-raises without calling `/internal/migration/unpause`. The `resume_or_recover_unfinished` logic on the next coordinator startup will re-attempt CLEANUP from line 500 onward, but if the source shard is also unreachable, it will fail again — source stays paused indefinitely. **Confirmed.**

---

### Bug 8 — Heartbeat lease renewal uses wall-clock diff for monotonic offset

**Verdict: ✅ Confirmed**

**Code checked** (`shard/app.py` lines 1057–1061):

```python
if data and "granted_at" in data and "expires_at" in data:
    duration = data["expires_at"] - data["granted_at"]   # wall-clock diff
    state.lease_expires_at = time.monotonic() + (
        duration if duration > 0 else settings.LEASE_DURATION_S
    )
```

`granted_at` and `expires_at` come from `LeaseGrant` (set via coordinator's wall clock). Same pattern at `promote` (line 1547–1550). If NTP slews the coordinator clock between `granted_at` and `expires_at`, `duration` differs from the intended `LEASE_DURATION_S`. **Confirmed.**

---

### Bug 9 — "gap" response treated as permanent failure with no retry

**Verdict: ❌ False positive (as a distinct bug)**

This is a **duplicate of Bug 2**. The "gap" case goes through the exact same code path as any other replication failure — `_replicate_to_follower` returns `False` → write path sets `follower_state = DOWN`. There is no separate mechanism that makes "gap" worse than other replication failures; they're identical in behavior.

The underlying concern (no auto-catchup on replication failure) is valid but is **already described in Bug 2**. Listing it separately as Bug 9 is redundant.

**Remove from report as a separate bug — merge into Bug 2.**

---

### Bug 10 — Migration oplog silently omits compacted WAL entries

**Verdict: ✅ Confirmed**

**Code checked** (`shard/app.py` lines 1701–1722):

```python
for entry in state.wal.replay(after_seq=after_seq):
    doc_id = entry.get("doc_id")
    if doc_id is not None and start <= doc_id < end:
        matched_entries.append(entry)
```

`wal.replay(after_seq=after_seq)` reads the raw WAL file; if the WAL was compacted and entries before `earliest_seq` were removed, those entries are simply not present. There is no check like:

```python
earliest = state.wal.earliest_seq()
if earliest and earliest > after_seq + 1:
    return {"status": "needs_snapshot", ...}
```

The coordinator's CATCHUP loop will silently receive fewer entries than expected and may proceed to PAUSE with an under-replicated destination. **Confirmed.**

---

### Bug 11 — `rejoin_complete` marks SYNCED without validating seq

**Verdict: ⚠️ Partially true**

**Code checked** (`shard/app.py` lines 1654–1669):

```python
async with state.write_lock():
    follower_url = body.get("follower_url")
    if follower_url:
        state.follower_url = follower_url
    state.follower_state = ReplicaState.SYNCED     # ← unconditional
    follower_seq = body.get("last_applied_seq", state.wal.last_seq if state.wal else 0)
    state.follower_acked_seq = max(state.follower_acked_seq, follower_seq)
```

The state IS set to SYNCED unconditionally. However, `follower_acked_seq` is updated with the reported seq — so the primary at least tracks how far behind the follower is. Subsequent writes will still require the follower to ack them. The risk is that marking SYNCED immediately means the coordinator sees the shard as non-degraded even if the follower is behind. **Partially confirmed — the SYNCED flag is premature, but the seq tracking partially mitigates the impact.**

---

### Bug 12 — Any 409 from coordinator demotes shard to FOLLOWER

**Verdict: ⚠️ Partially true**

**Code checked** (`shard/app.py` lines 1062–1067, `coordinator/coordinator.py` lines 665–666):

```python
# Shard heartbeat sender:
elif resp.status_code == 409:
    node_logger.warning("Heartbeat returned 409 STALE_TERM; demoting to follower")
    state.role = NodeRole.FOLLOWER
    state.lease_expires_at = 0.0
```

The shard demotes on any 409 without checking `error_code`. However, examining the coordinator:
- `handle_heartbeat` only raises `StaleTerm` (HTTP 409) when `hb.term < current_term` (line 665)
- No other code path in the coordinator's `/heartbeat` handler produces a 409

In the current implementation, a 409 from `/heartbeat` can **only** come from `StaleTerm`. So demoting on any 409 is safe in practice, though fragile if new error codes with 409 are added. **Partially confirmed — real fragility, no current bug.**

---

### Bug 13 — `_persist_node_metadata` blocking I/O inside write lock at `/internal/promote`

**Verdict: ⚠️ Partially true**

**Code checked** (`shard/app.py` lines 1529–1609):

The claim about WAL replay racing with concurrent appends (inside the write lock) is **incorrect** — asyncio is single-threaded and the write lock prevents concurrent modifications.

However, `_persist_node_metadata(state)` at line 1609 IS a synchronous blocking file I/O call executed while holding `state.write_lock()`. This blocks the asyncio event loop for the duration of the disk write. This is a real but minor issue in practice (the file is small).

**The WAL replay race claim is a false positive. The blocking I/O claim is confirmed but minor.**

---

### Bug 14 — `engine.apply()` high-water-mark guard drops out-of-order entries

**Verdict: ⚠️ Partially true**

**Code checked** (`storage/engine.py` lines 147–150):

```python
if seq <= self._last_applied_seq:
    return  # idempotent no-op
```

This is indeed a high-water-mark guard, not a set. An out-of-order delivery of seq N+2 before N+1 would apply N+2 (setting `_last_applied_seq = N+2`), then reject N+1 as already applied.

However, out-of-order delivery **cannot happen** through the current code paths:
- The primary write path applies entries in sequence
- Replication enforces `body.seq == expected_seq` at the follower
- WAL replay is ordered
- Catchup/resync applies WAL entries in order

This is a real **latent defect** but cannot be triggered without a separate ordering bug. The report correctly identifies it as a potential silent corruption risk. **Partially confirmed — real design fragility, not actively triggerable.**

---

### Bug 15 — `verify_token` argument order inverted

**Verdict: ❌ False positive**

**Code checked** (`router/app.py` line 165, `common/auth.py` lines 58–66):

```python
# auth.py:
def verify_token(expected_token: str, provided_token: str) -> bool:
    return hmac.compare_digest(
        expected_token.encode("utf-8"),
        provided_token.encode("utf-8"),
    )

# router/app.py:
if not x_api_key or not verify_token(x_api_key, settings.CLIENT_API_KEY):
```

`hmac.compare_digest` is **symmetric** — `compare_digest(a, b) == compare_digest(b, a)` always. The function has no side effects and no logging. The argument order inversion has **zero functional, security, or logging impact** in the current code. The report's own analysis states "hmac.compare_digest is symmetric so this does not create a security hole" — making this a non-bug by its own admission.

**Removed as a bug. It is a naming convention inconsistency with zero impact.**

---

### Bug 16 — README documents `$ne` operator that doesn't exist

**Verdict: ❌ False positive**

**Checked the README** (lines 295, 393–404):

```
python -m docstore.client query --filter '{"rating": {"$gte": 4}}' --limit 10
...
{"rating": {"$gte": 4}},
{"tags": {"$in": ["database", "nosql"]}}
...
Supported filter operators: `$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$and`, `$or`.
```

The README uses `$gte` and `$in`, both of which ARE in `WHITELIST_VALUE_OPS`. The README does **not** show `$ne` anywhere. The operator list at line 404 matches the whitelist exactly.

**This bug does not exist in the current codebase.**

---

### Bug 17 — `migration_pause` reads `wal.last_seq` outside the write lock

**Verdict: ❌ False positive**

**Code checked** (`shard/app.py` lines 1800–1815):

```python
async with state.write_lock():                         # lock acquired
    if (start, end) not in state.paused_ranges:
        state.paused_ranges.append((start, end))
    final_seq = state.wal.last_seq if state.wal else 0  # ← still inside `async with`!
    ...
    return {"status": "paused", "final_seq": final_seq}  # return exits the with block
```

`final_seq = state.wal.last_seq` is on line 1807, which is **inside** the `async with state.write_lock()` block (indented under it). The lock is held when this read occurs. The report incorrectly claims the read is "outside the write lock."

**This bug does not exist.**

---

### Bug 18 — CATCHUP loop fixed 5-iteration cap can starve under write bursts

**Verdict: ✅ Confirmed**

**Code checked** (`coordinator/migration.py` lines 262–291):

```python
for _ in range(5):  # Catch-up iterations
    ...
    entries fetched up to limit=500
    ...
    if src_last_seq - current_cursor <= 2 or not entries:
        break
```

Maximum entries consumed: 5 × 500 = 2500. If the source shard receives >2500 new writes between SNAPSHOT_COPY start and CATCHUP completion, the loop exhausts without converging. The code then proceeds to PAUSE anyway (line 296), then FINAL_CATCHUP fetches up to 1000 more. If the backlog > 3500 entries, VERIFY will fail. **Confirmed.**

---

### Bug 19 — `migration_checksum` shallow-copy docs

**Verdict: ⚠️ Partially true**

**Code checked** (`shard/app.py` lines 1829–1839):

```python
async with state.write_lock():
    docs = state.engine.range_snapshot(start, end)
    ...
    count, chk = compute_deterministic_checksum(docs)
```

The write lock is held for the entire handler (range snapshot + checksum computation). In asyncio, there can be no concurrent `engine.apply()` call while the lock is held. Shallow-copy mutation is **impossible** in this context. This is safe as implemented.

The report itself acknowledges "the write lock is held throughout" — making the bug self-refuting. The DRY/fragility concern is real but this is not an active bug.

**Downgrade: not a bug, just a fragile pattern.**

---

### Bug 20 — `WriteResponse.seq` nullable but not documented

**Verdict: ✅ Confirmed**

**Code checked** (`common/models.py` line 369):

```python
seq: int | None = None
```

Confirmed `None` for non-COMMITTED outcomes. The API spec in `docs/api.md` does not note this. **Confirmed documentation gap** that can cause client-side `NullPointerException`/`TypeError`.

---

## Revised Final Tally

| Verdict | Count | Bugs |
|---------|-------|------|
| ✅ **Confirmed** (real bugs) | **9** | 3, 4, 5, 6, 7, 8, 10, 18, 20 |
| ⚠️ **Partially true** (real concern, overstated/misdescribed) | **6** | 1, 2, 11, 12, 13, 14 |
| ❌ **False positive** (not a bug) | **5** | 9, 15, 16, 17, 19 |

### False Positives — Summary of Reasons

| # | Reason |
|---|--------|
| 9 | Duplicate of Bug 2 — same code path, not a distinct bug |
| 15 | `hmac.compare_digest` is symmetric; zero functional or security impact |
| 16 | README does not mention `$ne`; operators shown (`$gte`, `$in`) are in the whitelist |
| 17 | `wal.last_seq` is read **inside** the write lock, not outside |
| 19 | Write lock is held throughout checksum; shallow-copy mutation is impossible |

### Partially True — Key Corrections

| # | What was overstated / wrong |
|---|----------------------------|
| 1 | Both code paths are correct — this is a DRY risk, not an active crash |
| 2 | `_replicate_to_follower` does not set follower DOWN directly — the write-path does |
| 11 | `follower_acked_seq` is still tracked correctly; SYNCED flag is premature but seq isn't lost |
| 12 | In practice only `StaleTerm` produces a 409 on `/heartbeat` |
| 13 | WAL replay race claim is wrong; blocking I/O claim is real but minor |
| 14 | High-water-mark guard is a real design flaw, but cannot be triggered by any current code path |
