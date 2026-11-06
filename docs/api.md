# API Reference — Sharded-Docstore

This document provides the complete REST API specification for Sharded-Docstore across the **Router**, **Shard Nodes**, and **Coordinator**.

---

## 1. Authentication & Headers

| Header | Description | Target Component | Required On |
|---|---|---|---|
| `X-Api-Key` | Client authentication key (`CLIENT_API_KEY`) | Router, Shard Nodes | All client document and query requests; shard `/doc`, `/query`, `/status` (the router forwards it) |
| `X-Admin-Token` | Cluster administrator token (`ADMIN_TOKEN`) | Coordinator | Admin endpoints (`/admin/*`) and `GET /shardmap` |
| `X-Cluster-Signature` | HMAC-SHA256 hex digest, keyed with `CLUSTER_SECRET`, over `METHOD\npath\nquery\n` followed by the raw body (see `docstore.common.auth.sign_request`) | Coordinator, Shard Nodes | Inter-node RPCs (`/register`, `/heartbeat`, `/internal/*`) |

---

## 2. Response Metadata & Error Model

### 2.1 Response Metadata (`meta`)

Every operational response contains a `meta` block:

```json
{
  "served_by": "primary",
  "term": 1,
  "ownership_epoch": 0,
  "applied_seq": 42,
  "replication_lag": null
}
```

- `served_by`: `"primary"` or `"follower"` (or node identifier).
- `term`: Monotonic term for the shard.
- `ownership_epoch`: Current ownership epoch of the target key range.
- `applied_seq`: Sequence number applied to in-memory state.
- `replication_lag`: `null` for primary reads; integer (operations behind primary) for follower reads.

### 2.2 Standard Error Response

Errors return JSON and **never expose stack traces**:

```json
{
  "error_code": "NOT_FOUND",
  "error_message": "Document with doc_id 100 was not found on this shard."
}
```

#### Error Codes Summary

| Code | HTTP Status | Description |
|---|---|---|
| `AUTH_FAILED` | 401 | Missing or invalid API key, admin token, or HMAC signature |
| `NOT_FOUND` | 404 | Document with specified `doc_id` does not exist |
| `OUT_OF_RANGE` | 400 | `doc_id` outside the key range owned by this shard |
| `INVALID_QUERY` | 400 | Query filter exceeds depth, size, or violates grammar |
| `BODY_TOO_LARGE` | 413 | Request body exceeds 2 MB (enforced even when no `Content-Length` is sent) |
| `DOC_TOO_LARGE` | 413 | A single document's serialized JSON exceeds 1 MB |
| `LENGTH_REQUIRED` | 411 | Chunked (`Transfer-Encoding`) request bodies are not accepted |
| `VALIDATION_ERROR` | 422 | Pydantic payload schema validation failure |
| `DUPLICATE_KEY` | 409 | Document with specified `doc_id` already exists |
| `STALE_EPOCH` | 409 | Write was routed with an `ownership_epoch` older than the shard's epoch for that range |
| `STALE_TERM` | 409 | Request `term` differs from the node's current term (stale leader or stale client) |
| `WRONG_SHARD` | 409 | Internal RPC carried a `shard_id` that does not match the receiving node |
| `NOT_PRIMARY` | 409 | Write or primary read routed to non-primary replica |
| `LEASE_EXPIRED` | 409 | Primary lease expired; cannot serve writes or fresh reads |
| `OUTCOME_UNKNOWN` | 202 | Write appended to primary WAL, but follower ACK not received in time |
| `RANGE_PAUSED` | 503 | Target key range is currently paused for migration |
| `REPLICATION_UNAVAILABLE`| 503 | Shard has no `SYNCED` follower; write rejected before WAL append |
| `SHARD_UNAVAILABLE` | 503 | Target shard primary is down or unreachable |
| `SERVER_BUSY` | 503 | Router reached maximum concurrent request capacity |
| `ROUTING_FAILED` | 500 | Router exceeded retry limit during map recovery |
| `RESULT_TOO_LARGE` | 413 | A query would pull more than 32 MB of shard responses; reduce `skip`/`limit` or narrow the filter |
| `FAILOVER_IN_PROGRESS` | 503 | (Coordinator `/heartbeat`) the shard is failing over; its old primary's lease is not renewed |

#### Router retries and write fencing

- The router reloads its shard map and retries **once** on `OUT_OF_RANGE`, `STALE_EPOCH`,
  `NOT_PRIMARY` and `LEASE_EXPIRED` (the last after a 100 ms pause, giving the coordinator
  time to renew the lease or fail over). `RANGE_PAUSED` is retried with bounded backoff.
- Every retry reuses the **same `request_id`**, so a write that already committed is
  answered from the shard's dedup table instead of executing twice.
- Every forwarded write carries the `ownership_epoch` of the map entry it was routed by.
  The primary rejects it with `STALE_EPOCH` if it holds a newer epoch for that range.
  Clients may additionally pin a shard `term` in write bodies; the primary rejects it with
  `STALE_TERM` if its own term differs.

---

## 3. Router API (Client Endpoints)

The Router listens by default on port `8080`. All document and query endpoints require `X-Api-Key: test-api-key`.

### 3.1 Insert Document (`POST /doc`)

Inserts a new document. `doc_id` must be an integer in `[0, 2^63)`.

- **Headers:** `X-Api-Key: test-api-key`, `Content-Type: application/json`
- **Request Body (`WriteRequest`):**
  ```json
  {
    "request_id": "c0a80101-0000-0000-0000-000000000001",
    "op": "insert",
    "document": {
      "doc_id": 100,
      "user": "alice",
      "email": "alice@example.com",
      "status": "active"
    }
  }
  ```
- **Response (201 Created):**
  ```json
  {
    "outcome": "COMMITTED",
    "request_id": "c0a80101-0000-0000-0000-000000000001",
    "seq": 1,
    "meta": {
      "served_by": "primary",
      "term": 1,
      "ownership_epoch": 0,
      "applied_seq": 1,
      "replication_lag": null
    },
    "error_code": null,
    "error_message": null
  }
  ```

  > **Note (L1):** `seq` is `null` for non-`COMMITTED` outcomes (`REJECTED`, `OUTCOME_UNKNOWN`).
  > Clients must not assume `seq` is always an integer.

### 3.2 Get Document (`GET /doc/{id}`)

Fetches a document by its 64-bit integer ID.

- **Query Parameters:**
  - `read_mode`: `"primary"` (default) or `"follower"`.
- **Curl Example:**
  ```bash
  curl -s -H "X-Api-Key: test-api-key" \
    "http://127.0.0.1:8080/doc/100?read_mode=primary"
  ```
- **Response (200 OK):**
  ```json
  {
    "document": {
      "doc_id": 100,
      "user": "alice",
      "email": "alice@example.com",
      "status": "active"
    },
    "meta": {
      "served_by": "primary",
      "term": 1,
      "ownership_epoch": 0,
      "applied_seq": 1,
      "replication_lag": null
    }
  }
  ```

### 3.3 Update Document (`PUT /doc/{id}`)

Performs full replacement of an existing document. Missing document returns 404 `NOT_FOUND`.

- **Request Body:**
  ```json
  {
    "request_id": "c0a80101-0000-0000-0000-000000000002",
    "op": "update",
    "document": {
      "doc_id": 100,
      "user": "alice",
      "email": "alice_updated@example.com",
      "status": "verified"
    }
  }
  ```
- **Response (200 OK):**
  ```json
  {
    "outcome": "COMMITTED",
    "request_id": "c0a80101-0000-0000-0000-000000000002",
    "seq": 2,
    "meta": {
      "served_by": "primary",
      "term": 1,
      "ownership_epoch": 0,
      "applied_seq": 2,
      "replication_lag": null
    }
  }
  ```

### 3.4 Delete Document (`DELETE /doc/{id}`)

Deletes a document. Retrying with the same `request_id` returns the cached outcome idempotently.

- **Request Body:**
  ```json
  {
    "request_id": "c0a80101-0000-0000-0000-000000000003",
    "op": "delete"
  }
  ```
- **Response (200 OK):**
  ```json
  {
    "outcome": "COMMITTED",
    "request_id": "c0a80101-0000-0000-0000-000000000003",
    "seq": 3,
    "meta": {
      "served_by": "primary",
      "term": 1,
      "ownership_epoch": 0,
      "applied_seq": 3,
      "replication_lag": null
    }
  }
  ```

### 3.5 Query Documents (`POST /query`)

Executes MongoDB-inspired filter queries with scatter-gather parallel execution across shards.

- **Request Body (`QueryFilter`):**
  ```json
  {
    "filter": {
      "status": { "$eq": "active" }
    },
    "sort_field": "user",
    "sort_dir": "asc",
    "skip": 0,
    "limit": 10,
    "read_mode": "primary"
  }
  ```
- **Response (200 OK):**
  - `total` is the number of documents matching the filter **across the whole cluster**,
    not the size of the returned page.
  - The router sends **one request per shard**, even when a shard owns several disjoint
    ranges, and passes the ranges it attributes to that shard so the shard restricts
    candidates before sorting and paginating.
  - `meta` is the shard's own metadata for single-shard queries. For multi-shard queries it
    is a conservative summary: highest `term` / `ownership_epoch`, lowest `applied_seq`,
    worst `replication_lag`; `served_by` is `"mixed"` if shards answered in different roles.
  ```json
  {
    "documents": [
      { "doc_id": 100, "user": "alice", "status": "active" },
      { "doc_id": 250, "user": "bob", "status": "active" }
    ],
    "total": 2,
    "meta": {
      "served_by": "primary",
      "term": 1,
      "ownership_epoch": 0,
      "applied_seq": 3,
      "replication_lag": null
    }
  }
  ```

### 3.6 Router Health & Status (`GET /health`, `GET /status`)

- `GET /health`: Returns `{"status": "ok"}`.
- `GET /status`: Returns router cache version, active shard connections, and latency metrics.

---

## 4. Shard Node Internal RPCs (`/internal/*`)

Used strictly for inter-replica communication. Protected by `X-Cluster-Signature` (HMAC-SHA256).

### 4.1 Replicate Operation (`POST /internal/replicate`)

Primary sends new oplog entries to follower. Follower fsyncs to its WAL before replying.

- **Request:**
  ```json
  {
    "shard_id": "shard0",
    "term": 1,
    "seq": 42,
    "op": "insert",
    "doc_id": 100,
    "request_id": "c0a80101-0000-0000-0000-000000000001",
    "payload": { "doc_id": 100, "user": "alice" }
  }
  ```
- **Response (200 OK):**
  ```json
  {
    "status": "ok",
    "seq": 42,
    "last_seq": 42,
    "error_message": null
  }
  ```

### 4.2 Catchup Oplog (`POST /internal/catchup`)

Follower requests missing entries after restart or network blip.

- **Request:**
  ```json
  {
    "shard_id": "shard0",
    "after_seq": 40
  }
  ```
- **Response (200 OK):**
  ```json
  {
    "entries": [
      {
        "seq": 41,
        "term": 1,
        "op": "insert",
        "doc_id": 99,
        "request_id": "req-99",
        "payload": { "doc_id": 99, "data": "val" },
        "crc32": 12345678
      },
      {
        "seq": 42,
        "term": 1,
        "op": "insert",
        "doc_id": 100,
        "request_id": "c0a80101-0000-0000-0000-000000000001",
        "payload": { "doc_id": 100, "user": "alice" },
        "crc32": 87654321
      }
    ],
    "needs_snapshot": false,
    "last_seq": 42
  }
  ```

### 4.3 Snapshot Resync (`GET /internal/snapshot`, `POST /internal/snapshot/apply`)

- `GET /internal/snapshot`: Exports full point-in-time state of the primary (documents, term, epoch, applied seq, dedup table).
- `POST /internal/snapshot/apply`: Follower applies snapshot atomically when WAL compaction prevents incremental catchup.
  The snapshot is persisted first; only then is the local WAL **reset** to continue at
  `last_applied_seq + 1`, so the retained WAL never has a gap in front of new entries.

All internal RPCs that carry a `shard_id` (replicate, catchup, check-op, divergence,
rejoin/complete, snapshot/apply, fence, promote) reject a mismatching one with
`409 WRONG_SHARD`.

### 4.4 Promote Follower (`POST /internal/promote`)

Sent by the coordinator during failover. Body is strictly validated:

```json
{
  "shard_id": "shard0",
  "term": 2,
  "ownership_epochs": { "0-3074457345618258602": 2 },
  "ranges": [ { "start": 0, "end": 3074457345618258602, "ownership_epoch": 2 } ],
  "lease": { "granted_at": 1700000000.0, "expires_at": 1700000003.0, "...": "..." },
  "follower_url": "127.0.0.1:8200"
}
```

- `term` lower than the node's term → `409 STALE_TERM`.
- The coordinator checks the target first: a follower that its primary last reported as not
  `SYNCED`, or that is behind the primary's last reported sequence number, is not promoted
  automatically (`POST /admin/failover/{shard}?force=true` overrides). If the old primary
  restarted with its data while the follower is unsynced, the old primary is re-promoted
  under a new term instead.
- `ranges` (sent by the coordinator, authoritative from the shard map) replace the node's
  ranges; an epoch lower than the node's own for the same range → `409 STALE_EPOCH`.
  Without `ranges`, `ownership_epochs` must name exactly the node's ranges
  (`409 RANGE_MISMATCH` otherwise).
- The new term, role and ranges are **persisted before** the node starts acting as
  primary; a persistence failure returns 500 and leaves the node a follower.
- Repeating a promotion that already took effect (same term) only refreshes the lease.
- The lease length is `expires_at − granted_at`, capped at the node's own `LEASE_DURATION_S`.

### 4.5 Migration RPCs (`/internal/migration/*`)

- `POST /internal/migration/snapshot` — `{"start", "end", "after_doc_id", "limit", "max_bytes"}`.
  Cursor-paginated range copy; returns `documents`, `snapshot_seq`, `next_after_doc_id`, `done`.
  Omitting `limit` returns the whole range in one response (legacy). Serving a page pins
  the source WAL after `snapshot_seq` so compaction cannot remove entries the migration's
  catch-up still needs (the hold expires after 5 minutes without activity).
- `POST /internal/migration/apply-batch`, `/apply-oplog` — idempotent: a retried batch
  is detected by its deterministic per-document request id and is not re-appended
  (`skipped` in the response). Rejected with 503 while the destination has unresolved
  pending writes.
- `POST /internal/migration/cutover`, `/cleanup`, `/ranges/remove` — range ownership
  changes are persisted to `node_metadata.json` **before** they take effect.

---

## 5. Coordinator API

The coordinator listens by default on port `8200`.

### 5.1 Get Cluster Shard Map (`GET /shardmap`)

Returns active cluster topology, primary/follower endpoints, and epochs. Requires `X-Admin-Token` (it exposes internal node addresses).

- **Curl:**
  ```bash
  curl -s http://127.0.0.1:8200/shardmap -H "X-Admin-Token: $ADMIN_TOKEN"
  ```
- **Response (200 OK):**
  ```json
  {
    "map_version": 1,
    "entries": [
      {
        "shard_id": "shard0",
        "range": { "start": 0, "end": 3074457345618258602 },
        "primary": "127.0.0.1:8000",
        "follower": "127.0.0.1:8100",
        "ownership_epoch": 0,
        "map_version": 1
      },
      {
        "shard_id": "shard1",
        "range": { "start": 3074457345618258602, "end": 6148914691236517204 },
        "primary": "127.0.0.1:8001",
        "follower": "127.0.0.1:8101",
        "ownership_epoch": 0,
        "map_version": 1
      },
      {
        "shard_id": "shard2",
        "range": { "start": 6148914691236517204, "end": 9223372036854775808 },
        "primary": "127.0.0.1:8002",
        "follower": "127.0.0.1:8102",
        "ownership_epoch": 0,
        "map_version": 1
      }
    ]
  }
  ```

### 5.2 Node Registration (`POST /register`)

Nodes register on startup. Protected by HMAC signature.

- **Request:**
  ```json
  {
    "node_id": "shard0-primary",
    "shard_id": "shard0",
    "address": "127.0.0.1:8000",
    "requested_role": "primary"
  }
  ```
- **Response (200 OK):**
  ```json
  {
    "node_id": "shard0-primary",
    "shard_id": "shard0",
    "role": "primary",
    "term": 1,
    "ranges": [
      { "start": 0, "end": 3074457345618258602 }
    ],
    "lease": {
      "node_id": "shard0-primary",
      "shard_id": "shard0",
      "term": 1,
      "granted_at": 100.0,
      "expires_at": 103.0,
      "map_version": 1
    },
    "map_version": 1
  }
  ```

### 5.3 Heartbeat (`POST /heartbeat`)

Primary sends periodic heartbeat every `H=1.0s`. Protected by HMAC signature.

- **Request:**
  ```json
  {
    "node_id": "shard0-primary",
    "shard_id": "shard0",
    "role": "primary",
    "term": 1,
    "last_applied_seq": 42,
    "follower_acked_seq": 42,
    "follower_state": "SYNCED",
    "timestamp_monotonic": 1234.56
  }
  ```
- **Response (200 OK):** Returns renewed `LeaseGrant`.

### 5.4 Coordinator Admin Status (`GET /admin/status`)

Protected by `X-Admin-Token: test-admin-token`.

- **Curl:**
  ```bash
  curl -s -H "X-Admin-Token: test-admin-token" \
    http://127.0.0.1:8200/admin/status
  ```
- **Response (200 OK):**
  ```json
  {
    "map_version": 1,
    "nodes": {
      "shard0-primary": {
        "node_id": "shard0-primary",
        "shard_id": "shard0",
        "address": "127.0.0.1:8000",
        "role": "primary",
        "last_heartbeat": 1234.56
      }
    },
    "leases": {
      "shard0": {
        "node_id": "shard0-primary",
        "shard_id": "shard0",
        "term": 1,
        "granted_at": 100.0,
        "expires_at": 103.0,
        "map_version": 1
      }
    },
    "degraded_shards": []
  }
  ```
