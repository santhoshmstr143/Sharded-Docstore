# Sharded-Docstore

A distributed, range-partitioned JSON document store built in Python 3.11 with `asyncio` and `FastAPI`. Features synchronous primary-follower replication, lease-based split-brain prevention, atomic scatter-gather querying, and monotonic epoch tracking.

---

## Architecture & System Overview

```
                                 ┌───────────────┐
                                 │    Clients    │
                                 └───────┬───────┘
                                         │  (HTTP + X-Api-Key)
                                         ▼
                                 ┌───────────────┐
                                 │ Stateless     │
                                 │ Router        │
                                 └───────┬───────┘
                                         │
                 ┌───────────────────────┼───────────────────────┐
                 │                       │                       │
                 ▼                       ▼                       ▼
          ┌─────────────┐         ┌─────────────┐         ┌─────────────┐
          │   Shard 0   │         │   Shard 1   │         │   Shard 2   │
          │ [0, S1)     │         │ [S1, S2)    │         │ [S2, 2^63)  │
          │             │         │             │         │             │
          │ Primary (W) │         │ Primary (W) │         │ Primary (W) │
          │      │      │         │      │      │         │      │      │
          │      ▼ (WAL)│         │      ▼ (WAL)│         │      ▼ (WAL)│
          │ Follower (R)│         │ Follower (R)│         │ Follower (R)│
          └──────┬──────┘         └──────┬──────┘         └──────┬──────┘
                 │                       │                       │
                 └───────────────────────┼───────────────────────┘
                                         │  (Heartbeats & Leases)
                                         ▼
                                 ┌───────────────┐
                                 │  Coordinator  │
                                 │  (Shard Map,  │
                                 │  Epochs,      │
                                 │  Leases)      │
                                 └───────────────┘
```

### Core Components

1. **Coordinator Service (`docstore.coordinator`):**
   - Owns the authoritative cluster shard map with versioning (`map_version` and per-range `ownership_epoch`).
   - Issues time-bounded leases to primaries via periodic node heartbeats ($H$).
   - Enforces a safety cutoff ($L + M$) preventing a newly promoted primary from accepting writes while a former primary's lease could still be active.
   - Records every failover as a durable intent before changing the map, so a coordinator crash mid-failover is finished (or safely discarded) on restart; failover and migration cutover are serialised per shard.
   - Monitors node health and marks shards **degraded** when their follower is not in the `SYNCED` state.
   - Protected admin API for cluster status and range migrations (`X-Admin-Token`).

2. **Stateless Router (`docstore.router`):**
   - Validates client API keys (`X-Api-Key`) using constant-time verification.
   - Caches the shard map; routes CRUD operations directly to the owning primary.
   - Supports `read_mode=follower` with automatic fallback to primary if the follower is down or laggy.
   - Executes parallel scatter-gather queries (`POST /query`) across all shards with local limit queries, tie-breaking, global pagination, and de-duplication.
   - Bounded retries on routing staleness (`OUT_OF_RANGE`, `STALE_EPOCH`, `NOT_PRIMARY`, `LEASE_EXPIRED`, `REPLICATION_UNAVAILABLE`), always reusing the client's `request_id`.
   - Attaches the routing entry's `ownership_epoch` to every write as a fencing token; enforces the 1 MB document and 2 MB body limits (also for bodies sent without `Content-Length`).

3. **Shard Nodes (`docstore.shard`):**
   - **Primary Node:** Accepts write traffic, holds a per-shard write lock, validates range ownership, leases, and follower status. Under normal distributed deployment, replication is strictly **fail-closed**: if no follower is configured or the follower is not in `SYNCED` state, writes are rejected with HTTP 503 `REPLICATION_UNAVAILABLE` before WAL append. When a follower is configured, the primary assigns a sequence number, appends to its WAL, fsyncs, synchronously replicates to the follower (which also fsyncs), commits locally, and updates memory and dedup table. Standalone single-node writes require explicit opt-in via `STANDALONE_MODE=true` or `--standalone`.
   - **Follower Node:** Durably appends replicated oplog entries to its WAL, fsyncs, asynchronously updates memory, detects sequence gaps, supports oplog catch-up (`/internal/catchup`) and snapshot resync (`/internal/snapshot`), and serves reads tagged with `replication_lag`.
   - **Durability housekeeping:** every node takes periodic snapshots (every 1 000 ops or 60 s) and then compacts its WAL safely (verified snapshot first; never past committed / follower-acked / active-migration positions). WAL sequence continuity is verified on every replay.
   - Inter-node communications are authenticated using HMAC-SHA256 cluster signatures (`X-Cluster-Signature`).

### Timing Constraints Invariant

The cluster validates timing parameters at startup:
- $H < L < D$ (Heartbeat interval < Lease duration < Failure detection timeout)
- $T + M < L - H$ (Replication timeout + Safety margin < Remaining lease renewal window)
- Standard defaults: $H = 1.0\text{s}, L = 3.0\text{s}, D = 5.0\text{s}, M = 0.5\text{s}, T = 1.0\text{s}$.

---

## Quickstart & Environment Setup

### 1. Prerequisites
- Python 3.11+
- `make`
- `curl`

### 2. Setup Virtual Environment

```bash
# Clone the repository
git clone https://github.com/santhoshmstr143/Sharded-Docstore.git
cd Sharded-Docstore

# Create virtualenv and install all dependencies (runtime + dev tools)
make setup

# Activate the virtual environment
source .venv/bin/activate
```

### 3. Configure Secrets & Environment

Copy the example configuration to `.env`:

```bash
cp .env.example .env
```

Ensure the tokens meet the security lengths in `.env`:
- `CLUSTER_SECRET`: At least 32 characters (e.g. `cluster-shared-secret-key-32chars-min`)
- `ADMIN_TOKEN`: At least 32 characters (e.g. `coordinator-admin-token-32chars-min`)
- `CLIENT_API_KEY`: At least 16 characters (e.g. `client-api-key-16chars-min`)

---

## How to Run the Cluster Locally

You can run individual components directly using their Python module entry points.

### Step 1: Start the Coordinator

The coordinator manages the persistent shard map and grants leases.

```bash
source .venv/bin/activate
export HOST=127.0.0.1
export PORT=8100
export SHARD_MAP_PATH=./data/shard_map.json
python -m docstore.coordinator
```
*Coordinator starts on `http://127.0.0.1:8100`.*

---

### Step 2: Start Shard Nodes (Primaries & Followers)

To run a replicated shard (e.g., Shard 0 covering `[0, 3074457345618258602)`):

#### Terminal A: Shard 0 Follower
```bash
source .venv/bin/activate
export NODE_ID=node0-follower
export SHARD_ID=shard0
export ROLE=follower
export PORT=8201
export DATA_DIR=./data/node0-follower
python -m docstore.shard
```

#### Terminal B: Shard 0 Primary
```bash
source .venv/bin/activate
export NODE_ID=node0-primary
export SHARD_ID=shard0
export ROLE=primary
export PORT=8200
export FOLLOWER_URL=127.0.0.1:8201
export DATA_DIR=./data/node0-primary
python -m docstore.shard
```

*(Repeat for Shard 1 on ports 8202/8203 and Shard 2 on ports 8204/8205 as needed).*

---

### Step 3: Start the Stateless Router

The router exposes the client-facing API on port 8000:

```bash
source .venv/bin/activate
export HOST=127.0.0.1
export PORT=8000
export SHARD_MAP_PATH=./data/shard_map.json
python -m docstore.router
```
*Router starts on `http://127.0.0.1:8000`.*

---

## Testing & Quality Gates

The test suite contains **416 automated tests** covering unit storage, safe query filtering, shard routing, coordinator leases, fail-closed replication durability, failover fencing, and live range migration (415 pass and 1 Docker smoke test is skipped when no live cluster is running).

### Running Fast Tests (Unit + Integration)

```bash
# Run unit and integration tests (recommended for development)
make test-fast
```
Or directly with `pytest`:
```bash
.venv/bin/pytest tests/ -v
```

### Running Specific Test Modules

```bash
# 1. Coordinator service & leases (Module 6)
.venv/bin/pytest tests/unit/test_coordinator.py -v

# 2. Replication & M4 Gate replicated cluster (Module 7 & M4 Gate)
.venv/bin/pytest tests/integration/test_replication.py -v

# 3. Router CRUD, boundaries & M2 Gate tests (Module 4 & M2 Gate)
.venv/bin/pytest tests/integration/test_router.py -v

# 4. Safe Query filter engine & scatter-gather (Module 5 & M3 Gate)
.venv/bin/pytest tests/unit/test_query.py -v

# 5. Storage engine (WAL CRC32, torn tail, snapshots, dedup) (Module 2)
.venv/bin/pytest tests/unit/test_storage.py -v

# 6. Single shard node durable HTTP API (Module 3 & M1 Gate)
.venv/bin/pytest tests/integration/test_shard_node.py -v

# 7. Failover, fencing & rejoin (Module 8 & M5 Gate)
.venv/bin/pytest tests/fault/test_failover.py -v

# 8. Live range migration & checksum verification (Module 9 & M6 Gate)
.venv/bin/pytest tests/fault/test_migration.py -v

# 9. Regression tests for audited correctness and durability bugs
.venv/bin/pytest tests/integration/test_bugs.py -v
```

### Full Test Suite with Coverage Report

```bash
make test
```
*Generates terminal coverage metrics and an HTML report in `htmlcov/`.*

### Code Quality & Security Audits

All code must pass strict type-checking and security scans:

```bash
# Run Ruff linting and auto-formatting
make lint

# Check linting and formatting without modifying files (CI mode)
make lint-check

# Strict static type-checking (mypy)
make typecheck

# Static security vulnerability analysis (bandit)
make security

# Audit dependencies for known CVEs
make audit

# Run all quality checks together before committing
make check
```

---

## Client CLI & Python SDK (Module 10 / M7)

The project includes a Python client library and CLI tool with automatic request ID generation, transparent retries reusing the same request ID, and typed exceptions:

### 1. Python SDK (`docstore.client`)

```python
from docstore.client import DocStoreClient

# Connect to cluster router & coordinator
with DocStoreClient(router_url="http://127.0.0.1:8000", api_key="secret-key") as client:
    # Insert document (auto-generates and reuses UUID request_id across retries)
    res = client.insert({"doc_id": 100, "name": "Antigravity", "tags": ["db"]})
    print(res["outcome"])  # "COMMITTED"

    # Read from primary (default) or follower with replication lag
    doc_primary = client.get(100, read_mode="primary")
    doc_follower = client.get(100, read_mode="follower")
    print(doc_follower["meta"]["served_by"], doc_follower["meta"]["replication_lag"])

    # Query with filter, sorting, and pagination
    results = client.query(filter={"tags": "db"}, sort_field="name", limit=10)
    print(f"Total found: {results['total']}")

    # Update and delete
    client.update(100, {"doc_id": 100, "name": "Antigravity 2.0"})
    client.delete(100)
```

### 2. Command Line Interface (`python -m docstore.client` or `docstore`)

```bash
# Set client credentials
export CLIENT_API_KEY="client-api-key-16chars-min"
export ADMIN_TOKEN="coordinator-admin-token-32chars-min"

# 1. Insert document
python -m docstore.client insert --doc '{"doc_id": 42, "title": "DistSys", "rating": 5}'

# 2. Read document (primary or follower)
python -m docstore.client get 42 --read-mode primary
python -m docstore.client get 42 --read-mode follower

# 3. Query documents
python -m docstore.client query --filter '{"rating": {"$gte": 4}}' --limit 10

# 4. Check cluster status & topology
python -m docstore.client status
python -m docstore.client shardmap

# 5. Live range migration
python -m docstore.client migrate --from-shard shard0 --to-shard shard1 --start 0 --end 1000

# 6. Bulk-load JSON file
python -m docstore.client bulk-load dataset.json
```

---

## Example Usage with `curl`

Once the cluster (or router + shard node) is running, you can test operations using `curl`.

### 1. Insert a Document (Write Path)

Writes go to the router, which routes to the primary shard owning the `doc_id`. Replicated synchronously to the follower before returning `COMMITTED`:

```bash
curl -X POST http://127.0.0.1:8000/doc \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: client-api-key-16chars-min" \
  -d '{
    "request_id": "client-req-001",
    "document": {
      "doc_id": 42,
      "name": "Distributed Systems",
      "rating": 5,
      "tags": ["database", "python"]
    }
  }'
```

**Expected Response (HTTP 201):**
```json
{
  "outcome": "COMMITTED",
  "request_id": "client-req-001",
  "seq": 1,
  "meta": {
    "served_by": "primary",
    "term": 1,
    "ownership_epoch": 1,
    "applied_seq": 1,
    "replication_lag": null
  }
}
```

### 2. Idempotent Retry Test

Sending the exact same `request_id` returns the cached outcome without duplicate insertion:

```bash
curl -X POST http://127.0.0.1:8000/doc \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: client-api-key-16chars-min" \
  -d '{
    "request_id": "client-req-001",
    "document": {
      "doc_id": 42,
      "name": "Distributed Systems"
    }
  }'
```

### 3. Read from Primary vs Follower

#### Read from Primary (Default):
```bash
curl -X GET http://127.0.0.1:8000/doc/42 \
  -H "X-Api-Key: client-api-key-16chars-min"
```
*(Response has `"served_by": "primary"`)*

#### Read from Follower:
```bash
curl -X GET "http://127.0.0.1:8000/doc/42?read_mode=follower" \
  -H "X-Api-Key: client-api-key-16chars-min"
```
*(Response has `"served_by": "follower"`, `"applied_seq": 1`, and `"replication_lag": 0`)*

### 4. Scatter-Gather Query (`POST /query`)

Query documents matching structured Mongo-style filter expressions with pagination and sorting:

```bash
curl -X POST http://127.0.0.1:8000/query \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: client-api-key-16chars-min" \
  -d '{
    "filter": {
      "$and": [
        {"rating": {"$gte": 4}},
        {"tags": {"$in": ["database", "nosql"]}}
      ]
    },
    "sort_field": "rating",
    "sort_dir": "desc",
    "skip": 0,
    "limit": 10
  }'
```

Supported filter operators: `$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$and`, `$or`.

`total` in the response is the number of matching documents across the whole cluster (not the page size).

### 5. Inspect Coordinator Status (`GET /admin/status`)

Inspect lease states, replica states, terms, failover metrics, and shard health:

```bash
curl -X GET http://127.0.0.1:8100/admin/status \
  -H "X-Admin-Token: coordinator-admin-token-32chars-min"
```

---

## Failover, Fencing & Degraded Mode (Module 8 / M5)

### Promotion Procedure
1. **Detection:** Coordinator detects primary failure if no heartbeat is received within timeout $D$ (default 5.0 s), or when a shard has no registered primary — e.g. the primary restarted and came back as a follower.
2. **Follower Verification:** Coordinator checks that the follower is alive and queries its highest durable sequence number.
3. **Safety Wait:** Coordinator waits for the failed primary's lease to expire plus a safety margin $M$ ($L + M$) before allowing promotion.
4. **Term & Epoch Bump:** A durable failover intent is written first; then the shard `term` is incremented and `ownership_epoch` for all ranges of that shard is bumped. The updated shard map and epoch history are saved durably to disk.
5. **Promotion:** Follower is promoted to PRIMARY via `POST /internal/promote` with the map's authoritative ranges; it persists the new term, role and ranges **before** accepting writes. Unconfirmed promotions are retried, including after a coordinator restart.
6. **Degraded Mode:** The newly promoted primary serves reads immediately (once granted a lease), but rejects writes with `503 REPLICATION_UNAVAILABLE` until a follower is `SYNCED`.

### Old Primary Rejoin & Divergence Truncation
- A returning failed primary never resumes as primary from past state.
- When it restarts or contacts the coordinator, it is forced to role `FOLLOWER`.
- It contacts the new primary via `POST /internal/divergence` to find the highest seq where both logs hold the identical entry (full entry identity, not just `term`).
- Any uncommitted tail beyond the divergence point is truncated from its WAL, and memory index + dedup tables are rebuilt.
- It catches up remaining ops via `/internal/catchup` (or falls back to snapshot resync), marks itself `SYNCED`, and notifies the primary.
- The shard leaves degraded mode and normal synchronous replication resumes.

### Two Metrics Tracked
- **Failover Time:** Crash detection → follower promotion → first successful primary read.
- **Write-Restoration Time:** Crash detection → old primary restarted & resynced to `SYNCED` → first successful write.

### Test Crash Hooks
- Enabled via `ENABLE_TEST_HOOKS=true` for deterministic fault injection:
  1. `before_wal_append`: SIGKILL before appending write to WAL.
  2. `after_primary_fsync`: Crash after primary WAL fsync, before follower replication.
  3. `after_follower_fsync_before_ack`: Crash after follower WAL fsync, before follower returns ACK.
  4. `after_follower_ack_before_client_ack`: Crash after follower ACK, before client reply.
  5. `after_client_ack`: Normal completion crash.

---

## Live Range Migration (Module 9 / M6)

Live range migration allows transparent rebalancing of key ranges between shards without cluster downtime or data loss:

### 1. Multi-Phase Persisted State Machine

Managed and persisted by the Coordinator under `DATA_DIR/migrations/<id>.json`:

```
PLANNED ──► SNAPSHOT_COPY ──► CATCHUP ──► PAUSE ──► FINAL_CATCHUP ──► VERIFY ──► CUTOVER ──► CLEANUP ──► DONE
   │                                                                     │          │
   └──────────────────────────► ABORTED ◄────────────────────────────────┘          └── (Point of No Return)
```

- **SNAPSHOT_COPY:** Copies the range in cursor-paginated pages (500 docs, also bounded by bytes), starting at source sequence number $S$. Streamed to the destination primary via `/internal/migration/apply-batch` in batches bounded by count and serialized size; retried batches are not re-applied. Source keeps accepting live traffic, and pins its WAL after $S$ until catch-up has read it.
- **Normal Replicated Write Path:** Destination ingests migrating records through its normal write-path and replicates them synchronously to its follower.
- **Invisibility Invariant:** Migrating documents on the destination remain invisible to client reads (`GET /doc/{id}`) and queries (`POST /query`) until cutover occurs.
- **CATCHUP:** Source streams WAL oplog entries with $\text{seq} > S$ overlapping $[start, end)$. Replayed idempotently (insert $\to$ upsert, delete $\to$ no-op if missing) on destination. Repeated until replication lag is minimized.
- **PAUSE:** Coordinator temporarily pauses writes for the specific key range only (`RANGE_PAUSED`). Reads and queries continue to be served by the source shard.
- **FINAL_CATCHUP:** Destination catches up with all remaining ops through paused sequence number $H$.
- **VERIFY:** Coordinator verifies that source and destination have identical document counts and matching deterministic **SHA-256 canonical JSON** checksums (documents sorted by `doc_id`, keys sorted).
- **CUTOVER (Commit Point):** Coordinator atomically persists the updated shard map with a bumped `ownership_epoch`. `VERIFY` and this write run under both shards' topology locks, after re-checking that neither primary nor term changed. This disk write is the **point of no return**:
  - *Before Cutover:* Any error or failover aborts the migration, safely unpauses the source, and discards destination state.
  - *After Cutover:* The migration must roll forward. If the map write reports an error, the coordinator re-reads the persisted map and rolls forward whenever the range already moved.
  - Destination primary **and follower** must both confirm the new ownership; until they do, the migration stays in `CUTOVER` and is retried automatically.
- **CLEANUP:** Source deletes old copies and drops the range only after cutover is durable.

### 2. Transparent Cutover & Router Retries

- Router retries transparently on HTTP 503 `RANGE_PAUSED` with bounded exponential backoff (up to 8 attempts, max 0.3s backoff).
- Router automatically reloads the shard map on epoch bumps and redirects to the new owning shard with zero client-visible errors.

### 3. Admin Migration API

```bash
# Start an asynchronous live migration
curl -X POST http://127.0.0.1:8100/admin/migrate \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: coordinator-admin-token-32chars-min" \
  -d '{
    "from_shard": "shard0",
    "to_shard": "shard1",
    "start": 1000,
    "end": 2000,
    "sync": false
  }'

# Inspect migration progress and metrics
curl -X GET http://127.0.0.1:8100/admin/migrate/{migration_id} \
  -H "X-Admin-Token: coordinator-admin-token-32chars-min"
```

---

## Module Roadmap & Status

| Module | Status | Milestone | Description |
|---|---|---|---|
| **0** | ✅ Done | Foundations | Project setup, Make, virtualenv, CI hygiene |
| **1** | ✅ Done | Foundations | System contracts, config, typed errors, models, auth |
| **2** | ✅ Done | M1 Gate | Storage engine (WAL with CRC32/fsync, snapshot, B-tree/index) |
| **3** | ✅ Done | M1 Gate | Basic shard node durable HTTP CRUD API |
| **4** | ✅ Done | M2 Gate | Shard map manager, static keyspace routing, router app |
| **5** | ✅ Done | M3 Gate | Safe query filter engine, tie-breaking scatter-gather |
| **6** | ✅ Done | M4 Gate | Central Coordinator, monotonic leases, epochs, heartbeats |
| **7** | ✅ Done | M4 Gate | Sync durable replication, follower reads, catch-up, lag metrics |
| **8** | ✅ Done | M5 Gate | Failover detection, fencing & demotion/promotion |
| **9** | ✅ Done | M6 Gate | Live range migration, catch-up, pause, epoch cutover |
| **10** | ✅ Done | M7 Gate | Python client library and Click CLI (`docstore-cli`) |
| **12** | ✅ Done | M7 Gate | Security hardening, static analysis, cross-system test suite |
| **13** | ✅ Done | M7 Gate | Benchmarking suite, metrics runner, publication plots |
| **14** | ✅ Done | M7 Gate | Scripted end-to-end interactive demo (`make demo`) |
| **15** | ✅ Done | M7 Gate | Final architecture & consistency documentation and report (`docs/REPORT.md`, `docs/report.tex`) |
| **11** | ✅ Done | M7 Gate | Production Dockerfile and Docker Compose deployment (`make up`) |
| **16** | ✅ Done | M7 Gate | Final release v1.0.0 (tagged and verified) |

---

## Known Limitations

- **Coordinator Single-Point-of-Failure:** Coordinator is not fault-tolerant. If it fails, existing primary leases expire and writes halt until restored.
- **Single-Node Failure Scope:** Failover supports single-node (primary) failure per shard. If both replicas of a shard fail simultaneously, that shard is unavailable.
- **Fail-Closed Synchronous Replication & Degraded Mode:** Writes require durable 2-node synchronous quorum (primary and follower WAL fsync). A primary without a configured follower or with an unavailable follower strictly rejects writes with `503 REPLICATION_UNAVAILABLE` before WAL append. Standalone development mode requires explicit opt-in (`STANDALONE_MODE=true` or `allow_standalone=True`) and is clearly separated from replicated production guarantees.
- **Clock Assumptions:** Elapsed monotonic clock time is used locally for leases; cross-machine clock synchronization is not required.
- **Single-Writer Shard Model:** One write at a time per shard lock provides serialized writes within each individual shard; cross-shard multi-document transactions are not coordinated.
- **Same-request replay:** cluster signatures are bound to method, path, query and body, but carry no timestamp/nonce; someone who can capture internal traffic can re-send that *same* request (see `docs/security.md`).
- **Coordinator restart with a dead primary:** if the coordinator restarts while a primary is down and its follower had registered with the previous coordinator instance, no automatic failover happens (followers do not re-register). Use `POST /admin/failover/{shard_id}`.
- **Interrupted migration copies restart from scratch:** copy progress is persisted, but a coordinator restart aborts any migration that has not reached cutover.
- **Upgrading clients and nodes together:** shard data routes now require `X-Api-Key` and cluster signatures cover the request route, so routers, shards and the coordinator must all run this version. `GET /shardmap` now needs `X-Admin-Token`, and the server refuses to start with the `change-me` placeholder secrets from `.env.example`.
- **Upgrading old data directories:** nodes now verify WAL sequence continuity on startup. `data/` directories written by older builds may contain a WAL gap left by the old snapshot-install path and will refuse to start — delete them (`rm -rf data/`, or remove the Docker volumes) after upgrading. `make clean` does not touch node data.

---

## License

MIT — see [LICENSE](LICENSE).