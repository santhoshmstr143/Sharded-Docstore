## Verification update

I was able to run the newly added regression tests by providing a temporary ASGI lifespan test helper; I did not modify your project files.

Results:

| Test group | Result |
|---|---:|
| Query, storage, common and shard-map unit tests | 130 passed |
| New audit-fix regression tests | 13 passed |
| New bug and security regression tests | 65 passed |
| Combined targeted integration tests | **78 passed** |

That is meaningful progress. The previous audit's major reproduced issues now have regression coverage, including failover recovery, ambiguous migration cutover, follower metadata updates, WAL sequence integrity, compaction, multi-range scatter-gather and request validation.

But these tests are not a proof of correctness. I'm checking the broader suite and reviewing whether the new tests actually cover the failure boundaries they claim to cover.

# Latest ZIP audit — critical findings

I inspected the latest `Sharded-Docstore-main(6).zip`, including the updated source, tests, coordinator, migration manager, router, storage layer, configuration and Docker setup.

**The audit has improved the project substantially, but I found several serious bugs that the new tests do not cover. I would not consider the database safe for failure recovery yet.**

I also reproduced six important problems directly against this archive:

1. A cluster-mode primary can accept writes without a coordinator-issued lease.
2. A write carrying ownership epoch `999` is accepted by a shard whose local epoch is `1`.
3. A follower acknowledges a duplicate WAL entry even though its committed sequence is still `0` and the document has not been applied.
4. Migration operations can be committed locally when the cluster-mode primary has no follower.
5. An `$in` query can omit matching documents when one shard owns multiple disjoint ranges.
6. A shard returns a document from a range that is paused for migration.

There is also a dangerous internal promotion endpoint: I sent a signed promotion request containing only `{"term": 2}`—without a shard ID or lease—and the follower returned HTTP 200, becoming a primary with a valid local lease.

These are not hypothetical architectural concerns. They are behaviours I observed in the uploaded code.

# 1. Critical bugs — P0

These can cause split-brain, acknowledged writes becoming unavailable, or data loss.

## BUG 1 — Cluster nodes can grant themselves a lease

**Files:**
- `src/docstore/shard/app.py`
- `src/docstore/shard/__main__.py`
- `src/docstore/common/leases.py`

There are three related problems.

**A. Production startup grants a local lease before coordinator authorization.**

In `__main__.py`, the initial lease is calculated as:

```python
initial_lease = time.monotonic() + settings.LEASE_DURATION_S
```

But this is not a lease issued by the coordinator.

The heartbeat sender also waits for its first interval before registering.

Consequently, a cluster-mode primary can accept writes before the coordinator has granted authority.

**B. The application factory is even more permissive.**

`build_app()` defaults to an infinite lease for a primary when no explicit expiration is supplied, even if:

```python
allow_standalone=False
```

**C. Missing or invalid lease grants fail open.**

`lease_duration_from_grant()` returns the maximum local lease duration when the grant is missing or invalid.

Furthermore, `_apply_registration()` does not clear the existing lease when the coordinator returns no lease.

I reproduced a write succeeding with HTTP 201 while the coordinator was unavailable and standalone mode was disabled.

**Required fix:**
- Cluster-mode nodes start with lease expiration `0`.
- Only a valid coordinator-issued lease grants primary authority.
- Missing, expired or malformed grants must result in no lease.
- A denied registration must revoke any unconfirmed local authority.
- Infinite leases must be restricted to explicitly enabled standalone mode.

**Important:** `tests/integration/test_bugs.py` contains a test that expects a missing or expired grant to return the full local lease duration. That expectation is unsafe and must be reversed.

---

## BUG 2 — A primary with a completely wiped data directory can start as a new empty primary

**File:** `src/docstore/shard/app.py`, `_recover()`

The recovery code identifies disk recovery using the existence of metadata, a snapshot or WAL records.

If all three are absent:

- It treats the node as a fresh installation.
- It keeps the configured primary role.
- It configures the default range.
- It becomes ready.
- It can register with the coordinator as the incumbent primary.

Consider this scenario:

1. Primary A contains 10,000 documents.
2. Follower B contains the replicated data.
3. A's persistent volume is accidentally deleted.
4. A restarts with an empty data directory.
5. The coordinator still identifies A as the primary.
6. A registers and receives primary authority.
7. A starts serving an empty database.

There is no reliable check that the old primary's data has disappeared.

**Required fix:**
- Introduce an explicit bootstrap state.
- Distinguish first-time cluster initialization from an existing node losing its data.
- An existing primary with missing local state must not automatically become authoritative.
- Restore from a verified replica or fail closed.
- Add a test that deletes the primary's entire data directory while retaining the follower's data.

**Severity: Critical.**

---

## BUG 3 — Follower can acknowledge a WAL entry that is not committed or applied

**File:** `src/docstore/shard/app.py`

This is one of the most important replication defects.

In `/internal/replicate`, the duplicate-sequence branch:

1. Finds the existing WAL record.
2. Verifies its identity.
3. Returns `status="ok"`.

It does not ensure that the operation has been applied to the engine or that `committed_seq` was durably persisted.

There is a second problem in `_reconcile_pending_write()`.

The primary treats `has_op=True` from the follower as sufficient evidence to commit the operation. It does not require:

```text
follower.committed_seq >= operation.seq
follower.last_applied_seq >= operation.seq
```

It also does not send a durable commit confirmation to the follower.

### Reproduction

I manually created a follower state with:

```text
WAL last sequence: 1
Committed sequence: 0
Applied sequence: 0
Document: absent
```

Then I sent the same replication request again.

The follower returned:

```text
HTTP 200
status: ok
```

But afterwards:

```text
WAL last sequence: 1
Committed sequence: 0
Applied sequence: 0
Document: absent
```

The primary can interpret this as a successful replication acknowledgement and return `COMMITTED`.

If the primary subsequently fails, the follower can still have that operation in its WAL without having committed it according to its recovery metadata.

**Required fix:**
- Define an explicit durable commit protocol.
- Duplicate replication must not acknowledge an unapplied or uncommitted record.
- Reconciliation must check the follower's committed state.
- If the primary decides to commit an existing WAL-only entry, it must send a commit confirmation and wait for durable application/metadata persistence.
- Add a crash test between WAL append, engine application and metadata persistence.

**Severity: Critical.**

---

## BUG 4 — Migration can commit data locally without a follower in cluster mode

**Files:**
- `src/docstore/shard/app.py`
- `src/docstore/coordinator/coordinator.py`
- `src/docstore/coordinator/migration.py`

There are three connected problems.

### A. Follower state defaults to SYNCED

`NodeState.follower_state` defaults to `SYNCED`, even if `follower_url` is `None`.

The heartbeat sender reports that state to the coordinator.

### B. The coordinator trusts the reported state

`is_shard_degraded()` checks the reported replica state. It does not independently establish that a follower actually exists and is configured.

Migration creation checks degradation but does not enforce the existence of both replicas.

### C. Migration replication silently becomes local-only

`_append_replicated()` treats a missing follower URL as success.

It appends to the primary WAL, applies the document and advances the committed sequence without replication.

Similarly, the follower relay path can report success when there is no follower.

### Reproduction

I called `_append_replicated()` with:

```text
allow_standalone = False
follower_url = None
```

It successfully committed a migration operation locally.

**Impact:**

A destination can receive migrated documents, report successful application and complete cutover without actually replicating those documents to its follower.

If the destination primary subsequently fails, its follower may have the ownership metadata but not the documents.

**Required fix:**
- In cluster mode, missing follower configuration must mean `DOWN`, not `SYNCED`.
- Migration must reject shards without healthy, registered replicas.
- `_append_replicated()` must reject local-only commits in cluster mode.
- `_relay_to_follower()` must not report success when a required replica is absent.
- Verify both destination replicas' data and ownership metadata before marking migration `DONE`.

**Severity: Critical.**

---

## BUG 5 — Migration can treat a non-durable shard-map replacement as committed

**Files:**
- `src/docstore/router/shard_map.py`
- `src/docstore/coordinator/migration.py`
- `src/docstore/coordinator/coordinator.py`

`ShardMapManager.save()` performs:

1. Write temporary file.
2. Fsync temporary file.
3. `os.replace()` the shard-map file.
4. Update the in-memory map.
5. Fsync the parent directory.

Consider a failure at step 5.

The rename is visible to the current process, but its durability across a power loss is uncertain.

The migration recovery logic then reloads the visible shard map in `_committed_cutover_epoch()`.

If it sees the destination as the new owner, it treats the cutover as committed and continues with destination activation and source cleanup.

But reading a file after a failed directory fsync does not prove that the rename is crash-durable.

A power failure could restore the previous shard map after the source has already deleted its documents.

**Required fix:**
- Distinguish visible state from durably committed state.
- Do not clean up the source until the map transition is confirmed durable.
- On an ambiguous fsync failure, keep the migration in a recoverable pending state.
- Retry the durability operation or fail-stop the coordinator.
- Add fault-injection tests specifically for directory fsync failure after `os.replace()`.

The durable-term implementation has a related concern: `_DurableShardTerms` rolls back in-memory state when persistence fails, even though the file may already have been replaced.

**Severity: Critical.**

---

# 2. High-severity correctness bugs — P1

## BUG 6 — Ownership-epoch validation accepts future epochs

**File:** `src/docstore/shard/app.py`, `_execute_write()`

The current check is effectively:

```python
if ownership_epoch < local_ownership_epoch:
    reject()
```

It rejects older epochs but accepts newer ones.

I reproduced:

```text
Local ownership epoch: 1
Incoming ownership epoch: 999
Result: HTTP 201
```

A node whose ownership metadata is stale should not accept a request carrying a newer epoch.

There is also an issue with optional tokens: cluster-mode writes can omit `ownership_epoch`.

**Required fix:**

In cluster mode, require:

```python
incoming_ownership_epoch == local_ownership_epoch
```

Reject missing, lower and higher epochs.

Add tests for all three cases.

---

## BUG 7 — `$in` queries lose ranges when a shard owns multiple disjoint ranges

**File:** `src/docstore/router/scatter.py`, `resolve_target_shards()`

The `$in` routing branch groups matching IDs by shard ID.

However, it stores only one `ShardMapEntry` per shard:

```python
entry_map[shard_id] = entry
```

If a shard owns multiple disjoint ranges, earlier entries are overwritten.

### Reproduction

Shard A owns:

```text
[0, 100)
[200, 2^63)
```

Shard B owns:

```text
[100, 200)
```

Query:

```json
{
  "doc_id": {
    "$in": [1, 201]
  }
}
```

Both IDs belong to shard A.

But the router selects only:

```text
[200, 2^63)
```

It omits `[0,100)`.

**Result:** The query silently misses document 1.

**Required fix:**
- Preserve every matching range entry.
- Group by shard only after collecting all relevant ranges.
- Merge the ranges without discarding disjoint ranges.
- Test `$in` with IDs across multiple ranges owned by the same shard.

---

## BUG 8 — Client idempotency is lost when documents migrate

**Files:**
- `src/docstore/coordinator/migration.py`
- `src/docstore/shard/app.py`

The migration snapshot transfers documents and sequence information, but not the source's client deduplication history.

During migration:

- Snapshot copies use synthetic IDs such as `mig-...`.
- Oplog replay uses synthetic IDs such as `catchup-...`.
- The original client `request_id` is not preserved as the destination's deduplication identity.

Consider:

1. A client inserts document 42 with request ID `R1`.
2. The operation commits on the source.
3. Document 42 migrates to another shard.
4. The client retries request ID `R1` after cutover.

The destination does not know that request ID.

It may return `DUPLICATE_KEY` rather than the original committed response.

For update and delete requests, it may execute the operation again.

**Required fix:**
- Transfer deduplication records associated with the migrated range.
- Preserve original request IDs and fingerprints for oplog operations.
- Make destination retries return the original committed result.
- Test insert, update and delete retries immediately after migration.

---

## BUG 9 — Reads are not fenced during migration cutover

**Files:**
- `src/docstore/shard/app.py`
- `src/docstore/coordinator/migration.py`
- `src/docstore/router/scatter.py`

Migration pauses writes on the source range.

But `get_doc()` and `query_docs()` do not reject reads from `paused_ranges`.

I reproduced a GET returning HTTP 200 and the old document while the corresponding range was paused.

Why this matters:

1. Source range is paused.
2. Destination is activated after cutover.
3. New writes begin reaching the destination.
4. Source cleanup has not completed yet.
5. A router with an old shard map sends a read to the source.
6. The source returns the old document successfully.

This violates the documented primary-read consistency guarantee.

**Required fix:**
- Fence reads from a range after its cutover is committed.
- Include ownership epoch/map version in read requests.
- Reject stale read requests.
- Make the router refresh and retry after migration-related fencing responses.
- Add tests where a destination receives a new update and a stale router attempts to read from the source.

---

## BUG 10 — Registration response is not fully applied by the shard

**File:** `src/docstore/shard/app.py`, `_apply_registration()`

The coordinator returns information including:

- Assigned role.
- Current term.
- Ranges.
- Lease.

But `_apply_registration()` does not fully apply the response.

It mainly handles role changes and lease assignment.

Problems:

- It ignores the returned term.
- It ignores authoritative range information.
- If the coordinator returns no lease, the old local lease is not cleared.
- If persisting a role change fails, the node can retain its previous in-memory authority.

**Impact:** A node can remain on an outdated term or range configuration after registration, or retain an unconfirmed lease.

**Required fix:**
- Make registration response application a fail-closed state transition.
- Reconcile term and ownership metadata.
- Clear the lease before processing an ungranted primary registration.
- Do not mark registration successful until the returned state has been durably applied.

---

## BUG 11 — The internal promotion endpoint accepts incomplete authority requests

**Files:**
- `src/docstore/common/models.py`
- `src/docstore/shard/app.py`

`PromoteRequest` allows important fields to be omitted.

`_require_same_shard()` also skips validation when `shard_id` is missing.

When term is missing, promotion defaults to `state.term + 1`.

When lease is missing, the lease helper returns a full local lease duration.

### Reproduction

I sent a correctly signed internal request:

```json
{
  "term": 2
}
```

It contained no shard ID, no ranges and no lease.

The follower returned:

```text
HTTP 200
role: primary
term: 2
lease_valid: true
```

**Required fix:**
- Require `shard_id`.
- Require an explicit authorized term.
- Require the ownership ranges and epochs.
- Require a valid coordinator-issued lease.
- Reject missing or expired lease grants.
- Never infer primary authority from a missing lease.

This is an internal protocol issue, but the cluster secret is shared among nodes, so it should not be treated as harmless.

---

## BUG 12 — Follower replacement is not fully connected to primary replication

**Files:**
- `src/docstore/coordinator/coordinator.py`
- `src/docstore/common/models.py`
- `src/docstore/shard/app.py`

The coordinator can update the shard map when a follower registers in an empty follower slot.

However:

- The primary's `follower_url` is configured locally.
- Registration does not update the primary's `follower_url`.
- Registration response does not give the new follower the primary URL.
- The follower does not independently initiate a complete resynchronization workflow.

This can produce a shard map that identifies a new follower while the primary still has no follower URL or continues using an old one.

**Required fix:**
- Define a complete replica membership update protocol.
- Notify the primary of the new follower address.
- Trigger snapshot/catch-up.
- Verify sequence and ownership epochs.
- Only then report the replica as `SYNCED`.

---

## BUG 13 — Registration topology updates are not serialized with migration and failover

**Files:**
- `src/docstore/coordinator/coordinator.py`
- `src/docstore/coordinator/migration.py`

Failover and migration use `topology_lock`.

But `register_node()` changes the shard map without acquiring that lock.

It can therefore change topology while migration or failover is waiting on RPCs.

For example, a follower can register while migration is running, changing the map after the migration manager captured its initial follower address.

The migration can then continue using an outdated destination-follower address.

**Required fix:**
- Serialize registration-driven map mutations with topology changes.
- Revalidate both primary and follower addresses before cutover.
- Never declare migration complete using a follower address that is no longer authoritative.

---

## BUG 14 — Failed promotion retries can leave a phantom primary

**File:** `src/docstore/coordinator/coordinator.py`

`_build_promote_body()` updates the coordinator's in-memory node role to `PRIMARY` before the promotion RPC succeeds.

`_retry_unconfirmed_promotions()` eventually removes the pending promotion after its retry limit.

However, it does not reliably revert the coordinator's in-memory node role and lease state.

A follower that continues sending heartbeats can refresh `last_seen_monotonic`, even though its heartbeat role is still `FOLLOWER`.

The coordinator can consequently continue treating that node as the primary.

**Impact:** A shard can remain unavailable after promotion repeatedly fails.

**Required fix:**
- Do not mark a node primary before promotion is confirmed.
- Persist promotion state.
- On retry exhaustion, safely clear the phantom primary and lease.
- Either continue durable recovery or choose another validated candidate.
- Test repeated promotion failures with a healthy follower still sending heartbeats.

---

## BUG 15 — Recovery does not validate that committed sequence is recoverable

**File:** `src/docstore/shard/app.py`, `_recover()`

The recovery process loads:

```python
state.committed_seq = max(metadata_committed_seq, snapshot_seq)
```

It then replays the available WAL.

But it does not consistently reject a state where the persisted committed sequence is greater than the highest recoverable sequence in the snapshot and WAL.

Example:

```text
Metadata committed_seq: 10
Snapshot sequence: 0
WAL last sequence: 0
```

The node can retain `committed_seq=10` despite having no records or snapshot representing those committed operations.

**Required fix:**
- Validate committed sequence against recoverable state.
- Require a complete sequence prefix.
- Fail recovery if committed data cannot be reconstructed.
- Never promote a node with an unrecoverable committed prefix.

---

## BUG 16 — WAL truncate/reset failures do not reliably fail-stop the node

**File:** `src/docstore/storage/wal.py`

`_truncate_bytes()` closes the WAL file descriptor and then performs truncation and fsync operations.

If an operation fails, the descriptor may remain `None`, but `_failed` is not necessarily set.

The shard's `_wal_append()` wrapper uses the failed flag to decide whether to fail-stop the node.

Consequently, the node can remain `ready=True` and continue heartbeats even though its WAL is no longer usable.

**Required fix:**
- Mark the WAL failed on every uncertain mutation failure.
- Fail-stop the shard.
- Stop heartbeats and reject writes.
- Add tests for failures during truncate, reset, fsync and reopen.

---

# 3. Lower-severity bugs and scalability issues — P2

## BUG 17 — Empty ownership in a snapshot can be mistaken for missing configuration

**File:** `src/docstore/shard/app.py`, `_reconcile_all_ownership_epochs()`

The code only adds snapshot ownership metadata to its reconciliation sources when the snapshot ownership map is nonempty.

If metadata is missing but a snapshot explicitly records:

```json
{
  "ownership_epochs": {}
}
```

the empty map can be treated as absent.

`_configure_ranges()` can then restore the default environment range.

This is a remaining edge case in the earlier explicit-empty-range fix.

**Required fix:** Distinguish an explicitly empty snapshot ownership map from missing snapshot ownership metadata.

## BUG 18 — WAL replay still reads the entire WAL into memory

**File:** `src/docstore/storage/wal.py`, `replay()`

The migration oplog endpoint now supports bounded response batches.

However, `WAL.replay()` reads the entire WAL file into memory before filtering entries.

Therefore, the response limit does not bound the memory required to produce the response.

This can become expensive when:

- WAL retention grows.
- A migration is long-running.
- The follower is behind.
- A snapshot cannot safely compact old entries.

**Required fix:** Implement streaming WAL replay with incremental validation and bounded memory.

## BUG 19 — Snapshot-only recovery does not fully preserve an explicit empty ownership state

This is related to Bug 17 but deserves a dedicated test.

Test this sequence:

1. Migrate every range away from a shard.
2. Save a snapshot with no ownership ranges.
3. Remove or lose the node metadata file.
4. Restart using the same snapshot.
5. Verify that the node does not reclaim its default range.

The current reconciliation logic does not establish that invariant.

---

# 4. Previous audit findings that are now fixed

I do not want you to waste time asking Antigravity to fix things that are already corrected.

The following improvements are present in this archive:

| Earlier issue | Current assessment |
|---|---|
| Fractional `$lt` routing | Fixed for ordinary fractional values |
| Large integer sorting precision | Fixed |
| Nested Boolean equality | Fixed |
| Non-finite filter/document values | Validation added |
| Maximum document size | Enforced |
| Streaming request-body limit | Improved |
| Nested document copy mutation | Deep-copy fixes present |
| Scatter-gather total count | Fixed |
| Scatter-gather response metadata | Improved |
| WAL duplicate sequence validation | Improved |
| WAL append failure handling | Improved |
| Periodic snapshot and compaction | Implemented |
| Migration snapshot pagination | Implemented |
| Migration catch-up pagination | Implemented |
| Migration abort through WAL-backed deletion | Implemented |
| Source cleanup through WAL-backed deletion | Implemented |
| Durable coordinator terms | Implemented |
| Failover intent persistence | Implemented |
| Migration cutover intent | Implemented |
| Corrupt migration-record handling | Improved |
| Failed recovery heartbeat prevention | Improved |
| Follower sequence/term rejoin checks | Improved |
| Destination follower cutover retry | Improved |
| Router refresh after lease expiration | Improved |

These fixes are meaningful. However, some of them are incomplete at their failure boundaries, as explained in the remaining bugs.

---

# 5. Test results from the latest ZIP

I ran the tests in groups rather than relying only on Antigravity's previous reports.

| Test scope | Result |
|---|---:|
| Unit tests excluding CLI | 144 passed |
| CLI unit tests | 5 passed, 4 failed |
| Integration tests | 236 passed, 1 skipped |
| Fault tests | 16 passed |
| Security tests | 10 passed |
| Python compile check | Passed |

**Combined result: 411 passed, 4 failed, 1 skipped.**

### Why the four CLI tests failed

The environment has Click `8.1.8`, while the project specifies Click `8.5.0`.

The failures involve `CliRunner`'s `Result.stderr` handling and report:

```text
ValueError: I/O operation on closed file.
```

These appear to be environment/dependency-version failures rather than four demonstrated database defects. They still need to be rerun in the project's pinned environment.

### What I could not verify

- Docker smoke testing: Docker is unavailable here.
- `make check`: Ruff, Mypy, Bandit and pip-audit are unavailable.
- Integration tests required a temporary ASGI lifespan compatibility helper because `asgi_lifespan` is not installed in this environment.
- The full monolithic test run timed out, so I ran the test groups separately.

The compile check passed, but I cannot honestly claim that every CI quality gate passed.

---

# 6. Packaging problem in the ZIP

The latest archive contains runtime cluster state under `data/`.

For example, `data/shard_map.json` currently contains entries resembling:

```text
shard0 primary: 127.0.0.1:8002
shard0 follower: 127.0.0.1:8200
shard1 follower: None
shard2 follower: None
```

That is runtime state, not a source-code fixture.

Your `.gitignore` already says:

```text
data/
# Data directories (never commit node state)
```

But the ZIP still includes it, along with other runtime files and Python/test caches.

This is dangerous because the default coordinator configuration uses:

```text
./data/shard_map.json
```

A user running the coordinator outside Docker can accidentally load this stale map and route requests to the wrong services.

**Before final submission, exclude:**

- `data/`
- `.pytest_cache/`
- `__pycache__/`
- Runtime WALs and snapshots.
- Runtime migration records.
- Generated cluster-state files.

Keep test fixtures in a separate `tests/fixtures/` directory with explicit names.

---

# 7. Architecture limitations — not ordinary bugs

Even after the remaining code bugs are fixed, the project still has these limitations:

1. **Single coordinator:** It is not a highly available consensus-based coordinator. Local durable files do not provide coordinator fault tolerance.
2. **Two replicas:** The system cannot tolerate arbitrary multiple failures. It has a primary/follower availability trade-off.
3. **Lease assumptions:** Safety depends on bounded clock behaviour and process pauses.
4. **No global query snapshot:** Scatter-gather queries do not establish one cluster-wide read timestamp. Concurrent changes across shards can produce mixed-time results.
5. **No cross-shard transactions:** A transaction spanning multiple shards is not supported.
6. **Migration pause:** The final catch-up phase temporarily pauses writes.
7. **Shared internal secret:** Internal authentication is based on a shared cluster secret, not independent node identities. It should only be used in a trusted network, with TLS added for an untrusted deployment.

These are acceptable limitations for a student-level distributed database if they are clearly documented and demonstrated honestly.

---

# 8. What I would fix next

Do not send all 19 findings to Antigravity in one massive implementation task. These bugs interact.

My recommended order is:

| Stage | Bugs | Goal |
|---|---|---|
| 1 | 1, 2, 11 | Establish safe authority and startup recovery |
| 2 | 3, 4, 15, 16 | Make replication and recovery crash-safe |
| 3 | 5, 10, 12, 13, 14 | Make coordinator transitions recoverable |
| 4 | 6, 9 | Enforce ownership fencing and read consistency |
| 5 | 7, 8 | Fix query routing and migration idempotency |
| 6 | 17, 18, 19 | Complete storage recovery and scalability hardening |

## Final verdict

The new audit fixes are real, and the test suite is considerably stronger than before.

But I would still classify the project as **not yet safe for its advertised failover and live-migration guarantees**.

The five issues I would refuse to sign off on are:

1. Self-granted primary leases.
2. Empty primary recovery after data-volume loss.
3. Replication acknowledgements without durable follower commitment.
4. Migration commits without an actual follower.
5. Migration cutover proceeding after an ambiguous shard-map durability failure.

These are more important than benchmark performance, feature count or presentation quality.

**My recommendation is to fix those five first, then run fault-injection tests that kill processes at each persistence boundary.** I can turn the remaining findings into a staged, implementation-ready Antigravity prompt without asking it to redo the fixes already present.