# Security Architecture & Threat Model — Sharded-Docstore

This document outlines the security posture, authentication architecture, attack surface analysis, and honest limitations of Sharded-Docstore.

---

## 1. Authentication Architecture

Sharded-Docstore enforces defense-in-depth authentication using three distinct credentials:

```
┌──────────────┐                  ┌──────────────┐
│ Client / App │                  │ Cluster Admin│
└──────┬───────┘                  └──────┬───────┘
       │ X-Api-Key                       │ X-Admin-Token
       ▼                                 ▼
┌──────────────┐                  ┌──────────────┐
│    Router    │                  │ Coordinator  │
└──────┬───────┘                  └──────┬───────┘
       │                                 │
       │ X-Api-Key (data routes)         │ X-Cluster-Signature (HMAC)
       │ X-Cluster-Signature (HMAC)      │
       ▼                                 ▼
┌────────────────────────────────────────────────┐
│         Shard Nodes (Primary & Follower)       │
└────────────────────────────────────────────────┘
```

1. **Client API Key (`CLIENT_API_KEY`):**
   - Transmitted in the `X-Api-Key` HTTP header.
   - Enforced by the Router on all public data paths (`POST /doc`, `GET /doc/{id}`, `PUT /doc/{id}`, `DELETE /doc/{id}`, `POST /query`).
   - **Also enforced by every shard node** on its own data routes (`/doc`, `/query`, `/status`): the router forwards the key, so a shard port reached directly (another container, a compromised peer, the host via the bridge network) cannot be read or written without it. Only `/health` is unauthenticated.
   - Constant-time comparison prevents timing analysis attacks.
2. **Cluster Secret HMAC (`CLUSTER_SECRET`):**
   - Transmitted in the `X-Cluster-Signature` HTTP header.
   - Enforced on all internal inter-node RPCs:
     - Node registration (`POST /register`)
     - Heartbeats (`POST /heartbeat`)
     - Replication stream (`POST /internal/replicate`)
     - Log catchup (`POST /internal/catchup`)
     - Snapshot transfer (`GET /internal/snapshot`, `POST /internal/snapshot/apply`)
   - Computes HMAC-SHA256 over the **method, path, query string and raw body** (`METHOD\npath\nquery\n` + body) and verifies using `hmac.compare_digest`. Binding the route means a captured signature cannot be replayed against a different endpoint (e.g. a read-only `/internal/migration/checksum` request against the destructive `/internal/migration/cleanup`), and a body-less GET signature is not a reusable token for every internal GET.
   - Internal request bodies are validated strictly (integer types, `0 ≤ start < end ≤ 2^63`, known ops); malformed requests get `422` with no side effects. Internal RPCs naming another shard are rejected with `409 WRONG_SHARD`.
3. **Coordinator Admin Token (`ADMIN_TOKEN`):**
   - Transmitted in the `X-Admin-Token` HTTP header.
   - Strictly isolates administrative actions (`GET /admin/status`, `POST /admin/migrate`, `POST /admin/failover/{shard}`, and the topology view `GET /shardmap`) from general cluster secret RPCs.
4. **No self-granted authority:** a shard node only holds primary authority while it has a lease granted by the coordinator; promotion requests must carry the shard id, term, ownership and a valid lease grant. A shared cluster secret therefore cannot be used to make a node primary with an incomplete request.
5. **Secret hygiene:** settings refuse to start with the placeholder values from `.env.example` (anything containing `change-me`), in addition to the minimum lengths (32 / 32 / 16).

---

## 2. Threat Model & Mitigations

### 2.1 Denial of Service & Resource Exhaustion
- **Payload Limits:** Maximum document size is capped at 1 MB (`413 DOC_TOO_LARGE`, checked on the serialized document at both router and shard); maximum HTTP request body is capped at 2 MB (`413 BODY_TOO_LARGE`). Bodies with `Content-Length` are checked from the header; chunked bodies are rejected (`411`); bodies with neither are buffered by an ASGI middleware only up to the limit and rejected as soon as it is exceeded, so memory use stays bounded.
- **Misaddressed internal RPCs:** an authenticated internal RPC addressed to a different shard is rejected with `409 WRONG_SHARD`.
- **Query Complexity Restrictions:**
  - Maximum query filter nesting depth: 5 levels.
  - Maximum filter clauses: 50 clauses.
  - Maximum `$in` array length: 1,000 items.
  - Maximum query limit: 1,000 documents; maximum skip: 10,000 documents.
- **Query Cost Limits:** a filter may contain at most 10,000 values in total (so huge `$in` items cannot multiply evaluation cost), the shard-side `ranges` restriction accepts at most 64 merged ranges, and one query may pull at most 32 MB of shard responses into the router (`413 RESULT_TOO_LARGE`).
- **Concurrency Throttling:** Router bounds concurrent client requests via `MAX_CONCURRENT_REQUESTS = 1000`. Overloaded requests receive HTTP 503 `SERVER_BUSY`.

### 2.2 Injection & Code Execution Protections
- **Zero Eval:** No `eval()`, `exec()`, `compile()`, or `os.system()` anywhere in the codebase.
- **Safe Evaluation:** Query filtering uses a deterministic recursive descent evaluator (`docstore.query.evaluator`) that matches typed values without dynamic script execution.
- **No Unsafe Serialization:** State persistence and RPC communication use JSON and binary chunks; `pickle` and `yaml.unsafe_load` are forbidden.

### 2.3 Information Leakage
- **Stack Trace Suppression:** All error handlers intercept exceptions and return structured JSON error envelopes (`ErrorResponse`) containing only `error_code` and sanitised `error_message`. Internal exception tracebacks are recorded only in server-side structured logs.
- **No echoing or internal addresses:** validation errors (`422`) report which field failed and why, never the submitted value; `SHARD_UNAVAILABLE` names the shard but not its host:port or the transport error (those go to the logs).

---

## 3. Honest Documented Limitations

The current prototype intentionally documents the following scope limitations:

1. **Single Coordinator:** The coordinator is a single point of failure (SPOF); clustering the coordinator via Raft/Paxos is reserved for future enterprise iterations.
2. **No In-Transit TLS:** Inter-node communication within the container network relies on shared-secret HMAC authentication over HTTP rather than TLS mutual authentication (mTLS).
3. **No Replay Protection Window:** signatures are bound to method, path, query and body, so a request cannot be replayed against another endpoint, but they carry no timestamp or nonce: re-sending the *same* request is possible for anyone who can capture it on an untrusted internal network (most such requests are idempotent).
4. **Single-Host Monotonic Clock Assumption:** Lease safety relies on `time.monotonic()` elapsed time on individual nodes; does not assume synchronized wall clocks (NTP) across physical machines.
