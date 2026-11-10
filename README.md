# Sharded-Docstore

A small, distributed **JSON document database** inspired by MongoDB's sharded-cluster design,
written in Python 3.11 (`asyncio` + FastAPI).

Documents are spread across several **shards** by their `doc_id`. Every shard keeps two copies
of its data (a **primary** and a **follower**), so no acknowledged write is lost if a machine
crashes. A **router** hides all of this from clients, and a **coordinator** watches the cluster,
replaces a failed primary automatically, and can move key ranges between shards while the
database stays online.

> Course project — *Project 30, Team 50*: Reddem Bhanu Prakash Reddy,
> Mule Mohan Venkata Santosh Reddy, Mathamsetty Viswaj.

---

## Contents

1. [What it does](#what-it-does)
2. [How it works](#how-it-works)
3. [Requirements](#requirements)
4. [Quick start with Docker (recommended)](#quick-start-with-docker-recommended)
5. [Running without Docker](#running-without-docker)
6. [Configuration: the `.env` file and environment variables](#configuration-the-env-file-and-environment-variables)
7. [Using the database](#using-the-database)
8. [Consistency, failover and migration in brief](#consistency-failover-and-migration-in-brief)
9. [Testing and quality checks](#testing-and-quality-checks)
10. [Demo and benchmarks](#demo-and-benchmarks)
11. [Project layout](#project-layout)
12. [Troubleshooting](#troubleshooting)
13. [Known limitations](#known-limitations)
14. [Further documentation](#further-documentation)

---

## What it does

- **Stores JSON documents** keyed by an integer `doc_id` (`0 … 2^63 − 1`): insert, read,
  full-replace update, delete.
- **Shards data by key range.** The key space is split into contiguous ranges; each range is
  owned by one shard. The default cluster has 3 shards.
- **Replicates every write synchronously.** A write is acknowledged only after it has been
  `fsync`ed to the write-ahead log (WAL) of *both* the primary and the follower.
- **Queries with filters**: `$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$and`, `$or`, with
  sorting, `skip` and `limit`. Queries on `doc_id` go to one shard; other queries fan out to all
  shards and the router merges the results.
- **Primary reads vs follower reads.** Primary reads always see every acknowledged write;
  follower reads may be slightly stale and say how far behind they are (`replication_lag`).
- **Survives a primary crash.** The coordinator detects the failure, fences the old primary
  (terms + leases), and promotes the follower — with no acknowledged write lost.
- **Moves key ranges between shards live** (snapshot copy → catch-up → short write pause →
  verified cutover), while the rest of the cluster keeps serving traffic.
- **Idempotent writes.** Every write carries a `request_id`; retrying with the same id never
  applies the write twice.
- **Durable storage** per node: WAL with checksums and strict sequence numbering, plus periodic
  snapshots with safe WAL compaction.
- **Secured**: API key for clients, admin token for operators, HMAC request signatures between
  nodes.

---

## How it works

```
                                 ┌───────────────┐
                                 │    Clients    │   (curl, CLI, Python SDK)
                                 └───────┬───────┘
                                         │  HTTP + X-Api-Key
                                         ▼
                                 ┌───────────────┐
                                 │    Router     │  :8000   stateless; routes by doc_id,
                                 └───────┬───────┘          fans queries out and merges
                 ┌───────────────────────┼───────────────────────┐
                 ▼                       ▼                       ▼
          ┌─────────────┐         ┌─────────────┐         ┌─────────────┐
          │   Shard 0   │         │   Shard 1   │         │   Shard 2   │
          │  [0, S1)    │         │  [S1, S2)   │         │ [S2, 2^63)  │
          │ primary     │         │ primary     │         │ primary     │
          │   │ WAL     │         │   │ WAL     │         │   │ WAL     │
          │   ▼ (sync)  │         │   ▼ (sync)  │         │   ▼ (sync)  │
          │ follower    │         │ follower    │         │ follower    │
          └──────┬──────┘         └──────┬──────┘         └──────┬──────┘
                 └───────────────────────┼───────────────────────┘
                                         │  registration, heartbeats, leases
                                         ▼
                                 ┌───────────────┐
                                 │  Coordinator  │  :8100   shard map, terms, epochs,
                                 └───────────────┘          failover, migration
```

`S1 = 3 074 457 345 618 258 602`, `S2 = 6 148 914 691 236 517 204`.

| Component | Default port | What it does |
|---|---|---|
| **Router** (`docstore.router`) | 8000 | The only thing clients talk to. Checks the API key, finds the shard owning a `doc_id` from the shard map, forwards the request to that shard's primary, retries on stale routing, and runs scatter-gather queries. Holds no data. |
| **Shard node** (`docstore.shard`) | 8200–8205 | Stores documents. A **primary** accepts writes, appends them to its WAL, replicates them to its **follower**, and answers once both have them on disk. A follower serves (possibly stale) reads and takes over if the primary fails. |
| **Coordinator** (`docstore.coordinator`) | 8100 | Owns the persistent **shard map** (which range lives where), shard **terms**, range **ownership epochs** and primary **leases**. Detects failed primaries, runs failover, and drives range migrations. Admin API for operators. |

**Life of a write:** client → router (API key check, find owner) → shard primary (lease check,
dedup check, append + `fsync` WAL) → follower (append + `fsync` WAL, acknowledge) → primary
applies the write and answers `COMMITTED`.

---

## Requirements

- **Python 3.11+**
- **make** and **curl**
- **Docker** with **Docker Compose v2** — only for the Docker quick start
- Linux or macOS (the shell scripts and Makefile assume a POSIX shell)

---

## Quick start with Docker (recommended)

This starts the full cluster: 1 coordinator, 3 shards × 2 nodes, and 1 router.

```bash
# 1. Get the code
git clone https://github.com/santhoshmstr143/Sharded-Docstore.git
cd Sharded-Docstore

# 2. Start everything
make up
```

`make up` does two things:

1. If there is **no `.env` file yet**, it creates one from `.env.example` and fills
   `CLUSTER_SECRET`, `ADMIN_TOKEN` and `CLIENT_API_KEY` with **random secrets**. (If you already
   have a `.env`, it is used as-is — see [Configuration](#configuration-the-env-file-and-environment-variables).)
2. Runs `docker compose -f docker/docker-compose.yml up -d`, which builds the image and starts
   the 8 containers in dependency order (coordinator → followers → primaries → router).

Only the **router (`127.0.0.1:8000`)** and **coordinator (`127.0.0.1:8100`)** are published, and
only on localhost. Shard nodes are reachable only inside the Docker network.

**3. Check it is up**

```bash
curl -s http://127.0.0.1:8000/health      # router
curl -s http://127.0.0.1:8100/health      # coordinator
docker compose -f docker/docker-compose.yml ps
```

**4. Try it** (the secrets live in your `.env`):

```bash
export CLIENT_API_KEY=$(grep '^CLIENT_API_KEY=' .env | cut -d= -f2-)
export ADMIN_TOKEN=$(grep '^ADMIN_TOKEN=' .env | cut -d= -f2-)

# Insert a document
curl -s -X POST http://127.0.0.1:8000/doc \
  -H "Content-Type: application/json" -H "X-Api-Key: $CLIENT_API_KEY" \
  -d '{"request_id": "req-1", "document": {"doc_id": 42, "title": "Hello", "rating": 5}}'

# Read it back
curl -s http://127.0.0.1:8000/doc/42 -H "X-Api-Key: $CLIENT_API_KEY"

# Query
curl -s -X POST http://127.0.0.1:8000/query \
  -H "Content-Type: application/json" -H "X-Api-Key: $CLIENT_API_KEY" \
  -d '{"filter": {"rating": {"$gte": 4}}, "limit": 10}'

# Cluster status (operators)
curl -s http://127.0.0.1:8100/admin/status -H "X-Admin-Token: $ADMIN_TOKEN"
```

**Other Docker commands**

| Command | What it does |
|---|---|
| `make logs` | Follow the logs of all containers |
| `make down` | Stop the cluster (data is kept in Docker volumes) |
| `make reset` | Stop the cluster **and delete all data volumes** |

---

## Running without Docker

First install the project into a virtual environment and create your `.env`:

```bash
make setup                      # creates .venv and installs runtime + dev dependencies
source .venv/bin/activate
cp .env.example .env            # then replace the three secrets — see below
```

The three `change-me` values in `.env.example` are **rejected at startup**; generate real ones:

```bash
python -c "import secrets; print('CLUSTER_SECRET=' + secrets.token_hex(32))"
python -c "import secrets; print('ADMIN_TOKEN=' + secrets.token_hex(32))"
python -c "import secrets; print('CLIENT_API_KEY=' + secrets.token_hex(16))"
```

Every process reads `.env` from the **current directory**, so run all commands from the
repository root.

### Option A — one command: `scripts/local_cluster.sh`

```bash
scripts/local_cluster.sh start     # coordinator, 6 shard nodes, router (background)
scripts/local_cluster.sh status    # health of every process
scripts/local_cluster.sh stop
```

It writes a shard map with the correct local addresses, starts the processes in the right
order, waits for each to be healthy, and keeps data, logs and PID files in `data/local/`
(override with `LOCAL_DATA_DIR`). Delete `data/local/` to start over with an empty cluster.

### Option B — start each process yourself

Use this to see exactly what each process needs. Open one terminal per process, run
`source .venv/bin/activate` in each, and stay in the repository root.

**1. Create the shard map** (tells the router and coordinator where every node lives):

```bash
mkdir -p data/manual
PYTHONPATH=src python - <<'EOF'
from docstore.router.shard_map import ShardMapManager, create_default_3_shard_map
smap = create_default_3_shard_map(
    primaries=["127.0.0.1:8200", "127.0.0.1:8202", "127.0.0.1:8204"],
    followers=["127.0.0.1:8201", "127.0.0.1:8203", "127.0.0.1:8205"],
)
ShardMapManager("data/manual/shard_map.json").save(smap)
EOF
```

**2. Start the coordinator:**

```bash
HOST=127.0.0.1 PORT=8100 SHARD_MAP_PATH=data/manual/shard_map.json \
DATA_DIR=data/manual/coordinator \
PYTHONPATH=src python -m docstore.coordinator
```

**3. Start the shard nodes** — follower first, then its primary. Shard 0:

```bash
# shard0 follower
HOST=127.0.0.1 PORT=8201 NODE_ID=shard0-follower SHARD_ID=shard0 ROLE=follower \
NODE_ADDRESS=127.0.0.1:8201 COORDINATOR_URL=http://127.0.0.1:8100 \
SHARD_RANGE_START=0 SHARD_RANGE_END=3074457345618258602 \
DATA_DIR=data/manual/shard0-follower \
PYTHONPATH=src python -m docstore.shard

# shard0 primary
HOST=127.0.0.1 PORT=8200 NODE_ID=shard0-primary SHARD_ID=shard0 ROLE=primary \
NODE_ADDRESS=127.0.0.1:8200 COORDINATOR_URL=http://127.0.0.1:8100 \
FOLLOWER_URL=127.0.0.1:8201 \
SHARD_RANGE_START=0 SHARD_RANGE_END=3074457345618258602 \
DATA_DIR=data/manual/shard0-primary \
PYTHONPATH=src python -m docstore.shard
```

Repeat for the other shards, changing `NODE_ID`, `SHARD_ID`, ports, `NODE_ADDRESS`,
`FOLLOWER_URL`, `DATA_DIR` and the range:

| Shard | Primary port | Follower port | `SHARD_RANGE_START` | `SHARD_RANGE_END` |
|---|---|---|---|---|
| `shard0` | 8200 | 8201 | `0` | `3074457345618258602` |
| `shard1` | 8202 | 8203 | `3074457345618258602` | `6148914691236517204` |
| `shard2` | 8204 | 8205 | `6148914691236517204` | `9223372036854775808` |

Things that are easy to get wrong:

- `COORDINATOR_URL` is required. Without it a node never registers or renews its lease, and a
  primary starts rejecting writes with `LEASE_EXPIRED` after a few seconds.
- `NODE_ADDRESS` must be the exact address listed for the node in the shard map.
- `SHARD_RANGE_START`/`SHARD_RANGE_END` must match the map; without them a shard claims the
  whole key space.
- Give every node its **own** `DATA_DIR`.

**4. Start the router:**

```bash
HOST=127.0.0.1 PORT=8000 SHARD_MAP_PATH=data/manual/shard_map.json \
PYTHONPATH=src python -m docstore.router
```

The router and the coordinator must point at the **same** shard-map file: the router re-reads
it whenever a shard reports that its routing is stale.

### Option C — no cluster at all: the demo

`make demo` runs the whole cluster **inside one Python process** and walks through loading data,
a primary crash with automatic failover, fencing, rejoin and a live migration. No `.env` and no
ports are needed. See [Demo and benchmarks](#demo-and-benchmarks).

---

## Configuration: the `.env` file and environment variables

Settings are read from **environment variables**, with `./.env` as the fallback (real
environment variables win over `.env`). Docker Compose passes your `.env` to every container
and then sets the per-node values itself.

### 1. Secrets — required, no defaults

| Variable | Minimum length | Used by | Purpose |
|---|---|---|---|
| `CLUSTER_SECRET` | 32 chars | all nodes | Key for the HMAC signatures on node-to-node requests. **Must be identical on every node.** |
| `ADMIN_TOKEN` | 32 chars | coordinator, CLI | Sent as `X-Admin-Token` for operator endpoints (`/admin/*`, `/shardmap`). |
| `CLIENT_API_KEY` | 16 chars | router, shards, clients | Sent as `X-Api-Key` by clients; the router forwards it to shards. |

Startup fails if a secret is too short or still contains the `change-me` placeholder from
`.env.example`. Generate values with `python -c "import secrets; print(secrets.token_hex(32))"`.

### 2. Timing — optional (defaults shown)

| Variable | Default | Meaning |
|---|---|---|
| `HEARTBEAT_INTERVAL_S` | `1.0` | **H** — how often a primary heartbeats and renews its lease |
| `LEASE_DURATION_S` | `3.0` | **L** — how long a primary lease is valid |
| `FAILURE_TIMEOUT_S` | `5.0` | **D** — silence after which the coordinator declares a primary dead |
| `SAFETY_MARGIN_S` | `0.5` | **M** — extra wait after the old lease expires before a new primary may write |
| `REPLICATION_TIMEOUT_S` | `1.0` | **T** — how long a primary waits for its follower's acknowledgement |
| `MIGRATION_CATCHUP_MAX_DURATION_S` | `60.0` | Time limit for a migration's catch-up phase |
| `MIGRATION_FINAL_CATCHUP_MAX_DURATION_S` | `30.0` | Time limit for the catch-up during the write pause |

Startup checks **`H < L < D`** and **`T + M < L − H`** and refuses to run otherwise.

### 3. Storage, snapshots, logging and flags — optional

| Variable | Default | Meaning |
|---|---|---|
| `DATA_DIR` | `./data` | Where a node keeps its WAL, snapshots and metadata (`DATA_DIR/<shard_id>/`). The coordinator keeps migration records in `DATA_DIR/migrations/`. Give each node its own directory. |
| `SNAPSHOT_EVERY_N_OPS` | `1000` | Take a snapshot (then compact the WAL) after this many applied operations; `0` disables |
| `SNAPSHOT_EVERY_S` | `60` | …or after this many seconds; `0` disables |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL` |
| `STANDALONE_MODE` | `false` | Lets a shard primary accept writes **without** a follower (development only — no replication guarantee) |
| `ENABLE_TEST_HOOKS` | `false` | Crash-injection hooks for fault tests. **Never enable in a real deployment.** |

### 4. Per-process variables — set for each process, not in `.env`

Docker Compose and `scripts/local_cluster.sh` set these for you; set them yourself only when
starting processes by hand (Option B).

| Variable | Process | Default | Meaning |
|---|---|---|---|
| `HOST` | all | `127.0.0.1` | Interface to listen on (`0.0.0.0` inside containers) |
| `PORT` | all | router `8000`, coordinator `8100`, shard `8200` | Port to listen on |
| `SHARD_MAP_PATH` | router, coordinator | `./data/shard_map.json` | Shard-map file; must be the **same file** for both. Created with a default 3-shard map if missing. |
| `TERMS_PATH` | coordinator | next to the shard map | File with the durable shard terms |
| `NODE_ID` | shard | `shard-node-default` | Unique name of the node |
| `SHARD_ID` | shard | `shard0` | Which shard this node belongs to (`shard0`, `shard1`, `shard2`) |
| `ROLE` | shard | `primary` | Starting role on a fresh node: `primary` or `follower`. A node that restarts with existing data always comes back as a follower until the coordinator decides otherwise. |
| `FOLLOWER_URL` | shard (primary) | — | `host:port` of this shard's follower |
| `NODE_ADDRESS` | shard | `<NODE_ID>:<PORT>` | Address the node registers with; must match the shard map |
| `COORDINATOR_URL` | shard | — | Coordinator URL, e.g. `http://127.0.0.1:8100`. Required in a cluster. |
| `SHARD_RANGE_START` / `SHARD_RANGE_END` | shard | `0` / `2^63` | Key range `[start, end)` owned on first start (later ownership comes from the node's saved metadata) |
| `SHARD_OWNERSHIP_EPOCH` | shard | `1` | Initial ownership epoch of that range |

### 5. Client variables (CLI and Python SDK)

| Variable | Default | Meaning |
|---|---|---|
| `ROUTER_URL` (or `DOCSTORE_ROUTER_URL`) | `http://127.0.0.1:8000` | Router to send data requests to |
| `COORDINATOR_URL` (or `DOCSTORE_COORDINATOR_URL`) | `http://127.0.0.1:8100` | Coordinator for admin commands |
| `CLIENT_API_KEY` | — | Client API key (same value as the cluster's) |
| `ADMIN_TOKEN` | — | Needed for `status`, `shardmap`, `migrate` |

### Example `.env`

```dotenv
# Secrets (generate your own — these are examples only)
CLUSTER_SECRET=3f6c1e0b9a7d4c2e8f5a1b0c9d7e6f4a3b2c1d0e9f8a7b6c5d4e3f2a1b0c9d8e
ADMIN_TOKEN=9e8d7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a2f1e0d9c8b7a6f5e4d3c2b1a0f9e8d
CLIENT_API_KEY=0a1b2c3d4e5f60718293a4b5c6d7e8f9

# Timing (H < L < D and T + M < L - H)
HEARTBEAT_INTERVAL_S=1.0
LEASE_DURATION_S=3.0
FAILURE_TIMEOUT_S=5.0
SAFETY_MARGIN_S=0.5
REPLICATION_TIMEOUT_S=1.0

# Storage and snapshots
DATA_DIR=./data
SNAPSHOT_EVERY_N_OPS=1000
SNAPSHOT_EVERY_S=60

ENABLE_TEST_HOOKS=false
LOG_LEVEL=INFO
```

> `.env.example` also contains `COORDINATOR_PORT`, `ROUTER_PORT` and `SHARDn_*_PORT` lines.
> **No code reads them** (and two of them have the router/coordinator values swapped); ports come
> from `PORT` and the compose file. You can leave or delete them.
>
> `.env` is git-ignored — never commit it.

### Fixed limits (not configurable)

| Limit | Value |
|---|---|
| Document size | 1 MB (serialized JSON) |
| Request body | 2 MB |
| Query `limit` | default 100, max 1 000; `skip` max 10 000 |
| Filter | depth 5, 50 clauses, `$in` list ≤ 1 000 items, ≤ 10 000 values in total |
| Query response collected by the router | 32 MB |
| Idempotency window | last 100 000 request ids or 10 minutes |
| Router concurrent requests | 1 000 |

---

## Using the database

### Endpoints

**Router — `http://127.0.0.1:8000`** (all except `/health` need `X-Api-Key`)

| Method & path | Body | Purpose |
|---|---|---|
| `POST /doc` | `{"request_id", "document": {"doc_id", ...}}` | Insert (201) |
| `GET /doc/{doc_id}[?read_mode=follower]` | — | Read from the primary (default) or the follower |
| `PUT /doc/{doc_id}` | `{"request_id", "document"}` | Replace the whole document (`doc_id` must match the path) |
| `DELETE /doc/{doc_id}` | `{"request_id"}` | Delete |
| `POST /query` | `{"filter", "sort_field", "sort_dir", "skip", "limit", "read_mode"}` | Filtered query across shards |
| `GET /status` | — | Router status |
| `GET /health` | — | Liveness |

**Coordinator — `http://127.0.0.1:8100`** (operator endpoints need `X-Admin-Token`)

| Method & path | Purpose |
|---|---|
| `GET /admin/status` | Shards, primaries, followers, leases, terms, replica states, failover metrics |
| `GET /shardmap` | The current shard map |
| `POST /admin/migrate` | Start a range migration: `{"from_shard", "to_shard", "range": {"start", "end"}, "sync": false}` |
| `GET /admin/migrate/{id}`, `GET /admin/migrations` | Migration progress |
| `POST /admin/failover/{shard_id}[?force=true]` | Manual failover (`force` promotes even an unsynced follower — may lose writes) |
| `GET /health` | Liveness |

Shard nodes (`/doc`, `/query`, `/status`, `/internal/*`) are not meant to be called directly.
Full request/response formats: [`docs/api.md`](docs/api.md).

### Write outcomes

| HTTP | `outcome` / `error_code` | Meaning | What to do |
|---|---|---|---|
| 201 / 200 | `COMMITTED` | Durable on both replicas | Done |
| 202 | `OUTCOME_UNKNOWN` | Reached the primary's log, but the follower did not confirm in time | **Retry with the same `request_id`** — it is applied at most once |
| 4xx / 503 | e.g. `DUPLICATE_KEY`, `NOT_FOUND`, `REPLICATION_UNAVAILABLE`, `RANGE_PAUSED` | Rejected; nothing was written | Fix the request, or retry later (503) |

After a failover the shard is in **degraded mode**: reads work, writes return
`503 REPLICATION_UNAVAILABLE` until the old primary has rejoined as a follower.

### More `curl` examples

```bash
H=(-H "Content-Type: application/json" -H "X-Api-Key: $CLIENT_API_KEY")

# Retrying the same request_id returns the original result instead of inserting twice
curl -s -X POST http://127.0.0.1:8000/doc "${H[@]}" \
  -d '{"request_id": "req-1", "document": {"doc_id": 42, "title": "Hello", "rating": 5}}'

# Follower read (may be stale; shows replication_lag)
curl -s "http://127.0.0.1:8000/doc/42?read_mode=follower" -H "X-Api-Key: $CLIENT_API_KEY"

# Full replace
curl -s -X PUT http://127.0.0.1:8000/doc/42 "${H[@]}" \
  -d '{"request_id": "req-2", "document": {"doc_id": 42, "title": "Hello again", "rating": 4}}'

# Query with $and / $in, sorted, paginated ("total" counts all matches, not just this page)
curl -s -X POST http://127.0.0.1:8000/query "${H[@]}" -d '{
  "filter": {"$and": [{"rating": {"$gte": 4}}, {"tags": {"$in": ["db", "nosql"]}}]},
  "sort_field": "rating", "sort_dir": "desc", "skip": 0, "limit": 10}'

# Delete
curl -s -X DELETE http://127.0.0.1:8000/doc/42 "${H[@]}" -d '{"request_id": "req-3"}'

# Move keys [1000, 2000) from shard0 to shard1 while the cluster keeps running
curl -s -X POST http://127.0.0.1:8100/admin/migrate \
  -H "Content-Type: application/json" -H "X-Admin-Token: $ADMIN_TOKEN" \
  -d '{"from_shard": "shard0", "to_shard": "shard1", "range": {"start": 1000, "end": 2000}, "sync": false}'
```

### Command-line client

Install it as the `docstore` command with `pip install -e .` (inside the virtual environment),
or run it without installing as `PYTHONPATH=src python -m docstore.client ...`. It reads
`CLIENT_API_KEY`, `ADMIN_TOKEN`, `ROUTER_URL` and `COORDINATOR_URL` from the environment, or
takes `--api-key`, `--admin-token`, `--router-url`, `--coordinator-url`.

```bash
docstore insert --doc '{"doc_id": 42, "title": "DistSys", "rating": 5}'
docstore get 42 --read-mode follower
docstore update 42 --doc '{"doc_id": 42, "title": "DistSys 2", "rating": 4}'
docstore delete 42
docstore query --filter '{"rating": {"$gte": 4}}' --sort-field rating --limit 10
docstore status
docstore shardmap
docstore migrate --from-shard shard0 --to-shard shard1 --start 1000 --end 2000
docstore bulk-load dataset.json
```

Writes accept `--request-id`. If a write ends with `OUTCOME_UNKNOWN`, the CLI prints its
request id — run the same command again with `--request-id <that id>` to resolve it safely.

### Python SDK

Run with `PYTHONPATH=src` (or after `pip install -e .`):

```python
from docstore.client import DocStoreClient

with DocStoreClient(router_url="http://127.0.0.1:8000", api_key="<CLIENT_API_KEY>") as db:
    db.insert({"doc_id": 100, "name": "Ada", "tags": ["db"]})       # request_id generated
    print(db.get(100)["document"])
    print(db.get(100, read_mode="follower")["meta"]["replication_lag"])
    res = db.query(filter={"tags": "db"}, sort_field="name", limit=10)
    print(res["total"], res["documents"])
    db.update(100, {"doc_id": 100, "name": "Ada L."})
    db.delete(100)
```

Retries reuse the same `request_id` automatically. An async client (`AsyncDocStoreClient`) with
the same methods is also available.

---

## Consistency, failover and migration in brief

- **Commit point:** a write is committed once the follower has `fsync`ed it. Primary reads
  (served only while the primary holds a valid lease) see every committed write — also across
  a migration, because the old owner gives up the range before the new owner accepts any write
  (see `docs/consistency.md` §4.1.1). Follower reads may lag and report `replication_lag`.
- **Scope of the guarantees:** they hold for the failure model the project targets — one node
  failing at a time, a single (non-replicated) coordinator, bounded clock drift and process
  pauses shorter than the lease safety margin — and are exercised by the fault-injection tests.
  They are not formally verified, and losing both replicas of a shard loses its data.
- **Fencing:** every primary change increments the shard's **term**; followers reject writes
  from an older term, and a primary stops writing when its lease runs out. A node holds a lease
  **only** when the coordinator grants one — it starts without one and registers at once, so
  a freshly started primary rejects writes for up to one heartbeat interval. Every routed
  write carries the range's **ownership epoch**, which must match the shard's exactly; writes
  routed with an outdated map are rejected and the router refreshes.
- **Lost data directory:** a node that starts with an empty data directory is never made
  primary of a shard that already holds data; it becomes a follower and is rebuilt from the
  replica.
- **Failover:** if a primary stops heartbeating for `D` seconds, the coordinator waits until its
  lease has surely expired (+`M`), records the failover durably, bumps the term and epochs, and
  promotes the follower. A follower that is known to be out of sync is not promoted automatically.
  The old primary later rejoins as a follower, drops any unconfirmed tail of its log, and catches
  up. A primary that simply restarts also comes back as a follower and the coordinator promotes
  its partner.
- **Migration:** `SNAPSHOT_COPY → CATCHUP → PAUSE → FINAL_CATCHUP → VERIFY → CUTOVER → CLEANUP →
  DONE`. Writes to the moving range pause briefly; the router retries them. Source and destination
  checksums must match before the map is switched; after the switch the migration only rolls
  forward. Unfinished migrations are retried or cleanly aborted (`ABORTING → ABORTED`).

Details: [`docs/consistency.md`](docs/consistency.md), [`docs/failover.md`](docs/failover.md),
[`docs/migration.md`](docs/migration.md).

---

## Testing and quality checks

```bash
make test-fast        # unit + integration tests
make test             # full suite with coverage report (htmlcov/)
make test-fault       # crash-injection tests (sets ENABLE_TEST_HOOKS=true)
make test-security    # security tests
make lint             # ruff: auto-fix and format
make lint-check       # ruff: check only
make typecheck        # mypy --strict
make security         # bandit
make audit            # pip-audit on the pinned dependencies
make check            # lint-check + typecheck + security + audit + test-fast
```

Or run pytest directly, e.g. `.venv/bin/pytest tests/integration/test_bugs.py -v`.

The suite has **441 tests**: 440 pass and 1 Docker smoke test
(`tests/integration/test_smoke_docker.py`) is skipped unless a live cluster is running on
`127.0.0.1:8000`. `tests/integration/test_bugs.py` holds the regression tests for the bugs fixed
during the code reviews. `tests/conftest.py` contains a small, documented test-only adapter that
upgrades the older tests' request signatures; see the comment there.

---

## Demo and benchmarks

```bash
make demo     # scripted end-to-end demo, entirely in one process (no Docker, no .env)
make bench    # benchmarks; results in results/, figures in docs/figures/
```

The demo loads data across 3 shards, crashes a primary during writes, shows automatic failover,
fencing of the old primary, its rejoin, a full audit that no acknowledged write was lost, a live
range migration with checksum verification, and tagged follower reads.

---

## Project layout

```
src/docstore/
  common/        settings (.env), models, errors, auth (HMAC / tokens), HTTP helpers, logging
  storage/       WAL, snapshots, in-memory engine + dedup table, WAL compaction
  query/         filter validator, evaluator, sorter, shard-key detection
  shard/         shard node: writes, replication, recovery, failover and migration endpoints
  coordinator/   shard map, terms, leases, failure detection, failover, migration manager
  router/        client API, routing, scatter-gather queries, shard-map cache
  client/        Python SDK (sync + async) and the `docstore` CLI
tests/           unit/, integration/, fault/, security/
scripts/         demo.py, local_cluster.sh
bench/           load generator, benchmark runner, plots
docker/          docker-compose.yml, initial shard_map.json
docs/            design and API documentation, report
```

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Startup error mentioning the `.env.example` **placeholder** | Replace the `change-me` secrets in `.env` with generated values. |
| Startup error about **`H < L < D`** or **`T + M < L - H`** | Your timing variables violate the lease constraints; restore the defaults or adjust them. |
| `401 AUTH_FAILED` | Missing/wrong `X-Api-Key` (data) or `X-Admin-Token` (admin), or nodes started with different `CLUSTER_SECRET`s. |
| Writes fail with `409 LEASE_EXPIRED` | The primary has no coordinator lease: is the coordinator running and is `COORDINATOR_URL` set on the shard nodes? Right after a node starts this is normal for about one heartbeat interval. |
| Writes sent straight to a shard fail with `409 STALE_EPOCH` | In cluster mode every write must carry the range's current `ownership_epoch` (the router adds it). Send writes through the router. |
| Writes fail with `503 REPLICATION_UNAVAILABLE` | The shard's follower is down or still catching up (e.g. right after a failover). Reads keep working; writes resume once the follower is `SYNCED`. |
| A node returns `503 NODE_NOT_READY` / refuses to start | Its local data failed a safety check (corrupt WAL, sequence gap, or a node data directory from an older build). Check its log; for old development data, delete the node's data directory (`rm -rf data/...`, or `make reset` with Docker). |
| A shard never gets a primary after restarting everything | Startup takes a few seconds: the coordinator waits for old leases to expire (`L + M`) and for failure detection (`D`) before promoting. Watch `GET /admin/status`. |
| Address already in use | Another process is on 8000/8100/8200–8205; stop it (or `scripts/local_cluster.sh stop`, `make down`). |

---

## Known limitations

- **Single coordinator.** The coordinator is not replicated. If it is down, leases cannot be
  renewed and writes stop until it is back; its state is on disk and recovered on restart.
- **Single-node failures only.** If both replicas of a shard fail, that shard is unavailable.
- **Writes need both replicas.** A shard whose follower is unavailable rejects writes rather than
  weaken durability (`STANDALONE_MODE` exists for development only).
- **Same-request replay.** Node-to-node signatures cover method, path, query and body but have
  no timestamp, so someone able to capture internal traffic can re-send that *same* request.
  There is no TLS; run the cluster on a trusted network.
- **Coordinator restart while a primary is down.** If the follower had registered with the
  previous coordinator instance, no automatic failover happens; use
  `POST /admin/failover/{shard_id}`.
- **Interrupted migrations** that had not reached cutover are aborted when the coordinator
  restarts, and must be started again.
- **No multi-document transactions**; writes are atomic per document and serialized per shard.
- **Upgrades:** routers, shards and the coordinator must run the same version (request-signing
  and shard authentication changed). Node data from older builds may fail the stricter WAL checks
  and must be deleted.

---

## Further documentation

| Document | Contents |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Components, data and control paths, storage engine |
| [`docs/api.md`](docs/api.md) | Every endpoint, request/response formats, error codes |
| [`docs/contracts.md`](docs/contracts.md) | Exact system contracts (IDs, sequences, terms, epochs, idempotency, queries) |
| [`docs/consistency.md`](docs/consistency.md) | Consistency model and fencing argument |
| [`docs/failover.md`](docs/failover.md) | Failure detection, promotion, rejoin |
| [`docs/migration.md`](docs/migration.md) | Live range migration state machine |
| [`docs/security.md`](docs/security.md) | Authentication, threat model, limitations |
| [`docs/REPORT.md`](docs/REPORT.md) | Evaluation results (latency, failover time, migration pause) |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Branching and commit conventions |

---

## License

MIT — see [`LICENSE`](LICENSE).
