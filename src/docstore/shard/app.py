"""
Shard Node FastAPI application — Modules 3 & 7.

Identity: node_id, shard_id, role, term, owned ranges with ownership_epochs.

Public endpoints:
  POST   /doc                  — insert (primary only, sync replicated)
  PUT    /doc/{doc_id}         — update (full replace)
  DELETE /doc/{doc_id}         — delete
  GET    /doc/{doc_id}         — get (primary with valid lease, or follower with lag)
  POST   /query                — query engine evaluation
  GET    /health               — liveness probe
  GET    /status               — detailed node status (replication lag, lease, etc.)

Internal endpoints (inter-node RPC, cluster auth required):
  POST   /internal/replicate       — primary -> follower synchronous WAL append
  POST   /internal/catchup         — follower catch-up from primary WAL
  GET    /internal/snapshot        — full snapshot resync
  POST   /internal/snapshot/apply  — apply snapshot on follower
  POST   /internal/fence           — bump term

Write path (Module 7):
  1. Client sends write with unique request_id
  2. Primary takes write lock; checks:
     - role == PRIMARY (else NotPrimary)
     - lease remaining > T + M (else LeaseExpired)
     - range ownership (owns doc_id, else OutOfRange)
     - dedup check (cached -> return original result)
     - follower is SYNCED if replicated (else ReplicationUnavailable)
     - pre-WAL validity (insert: DuplicateKey; update/delete: NotFound)
  3. Primary assigns next sequence number
  4. Primary appends to its WAL and fsyncs
  5. Primary sends to follower via /internal/replicate (timeout T=1.0s)
  6. Follower fsyncs to WAL and sends durable acknowledgement
  7. Primary applies to in-memory state and dedup table
  8. If lease is still valid, returns COMMITTED; if expired meanwhile, return outcome_unknown
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from docstore.common.auth import (
    CLUSTER_SIG_HEADER,
    compute_signature,
    make_cluster_auth_dependency,
)
from docstore.common.config import Settings
from docstore.common.errors import (
    DocstoreError,
    DuplicateKey,
    InvalidQuery,
    LeaseExpired,
    NotFound,
    NotPrimary,
    OutcomeUnknown,
    OutOfRange,
    RangePaused,
    ReplicationUnavailable,
    RequestIdReused,
    StaleTerm,
)
from docstore.common.logging import get_logger
from docstore.common.models import (
    DOC_ID_MIN,
    CatchupRequest,
    CatchupResponse,
    Document,
    NodeRole,
    QueryFilter,
    QueryResponse,
    ReadResponse,
    ReplicaState,
    ReplicationRequest,
    ReplicationResponse,
    ResponseMeta,
    WriteOp,
    WriteOutcome,
    WriteResponse,
)
from docstore.common.test_hooks import trigger_hook
from docstore.query import (
    detect_shard_key_constraint,
    evaluate_filter,
    sort_documents,
    validate_query_filter,
)
from docstore.storage.engine import DedupEntry, StorageEngine
from docstore.storage.snapshot import SnapshotManager
from docstore.storage.wal import (
    WAL,
    WALCorruptionError,
    compute_entry_identity,
    has_required_identity_fields,
)

logger = logging.getLogger(__name__)


# ── Module-level request body models ──────────────────────────────────────────


class InsertBody(BaseModel):
    request_id: str = Field(min_length=1, max_length=128)
    document: dict[str, Any] = Field(description="Must contain doc_id.")


class UpdateBody(BaseModel):
    request_id: str = Field(min_length=1, max_length=128)
    document: dict[str, Any] = Field(description="Full replacement document.")


class DeleteBody(BaseModel):
    request_id: str = Field(min_length=1, max_length=128)


# ── Node state ────────────────────────────────────────────────────────────────


class ShardRangeConfig(BaseModel):
    """One owned range with its epoch."""

    start: int = Field(ge=DOC_ID_MIN)
    end: int = Field(ge=1)
    ownership_epoch: int = Field(ge=0)


class NodeState:
    """Mutable runtime state for a single shard node instance."""

    def __init__(self, settings: Settings, node_id: str, shard_id: str) -> None:
        self.settings = settings
        self.node_id = node_id
        self.shard_id = shard_id
        self.role: NodeRole = NodeRole.PRIMARY
        self.term: int = 1
        self.ranges: list[ShardRangeConfig] = []
        self.ready: bool = False
        self.engine: StorageEngine = StorageEngine()
        self.wal: WAL | None = None
        self.snapshot_mgr: SnapshotManager | None = None

        # Lease & replication fields (Module 6 & 7)
        self.lease_expires_at: float = float("inf")
        self.coordinator_url: str | None = None
        self.follower_url: str | None = None
        self.follower_state: ReplicaState = ReplicaState.SYNCED
        self.follower_acked_seq: int = 0
        self.committed_seq: int = 0
        self.primary_seq: int = 0  # tracked by follower for lag measurement
        self.http_client: httpx.AsyncClient | None = None
        self.paused_ranges: list[tuple[int, int]] = []
        self.pending_unresolved_writes: dict[str, dict[str, Any]] = {}
        self.allow_standalone: bool = False
        self.advertise_addr: str = ""

    def owns(self, doc_id: int) -> bool:
        return any(r.start <= doc_id < r.end for r in self.ranges)

    def epoch_for(self, doc_id: int) -> int:
        for r in self.ranges:
            if r.start <= doc_id < r.end:
                return r.ownership_epoch
        return 0

    def is_lease_valid(self) -> bool:
        if self.role != NodeRole.PRIMARY:
            return False
        return time.monotonic() < self.lease_expires_at

    def remaining_lease(self) -> float:
        if self.role != NodeRole.PRIMARY:
            return 0.0
        if self.lease_expires_at == float("inf"):
            return float("inf")
        return max(0.0, self.lease_expires_at - time.monotonic())

    def response_meta(self, doc_id: int | None = None) -> ResponseMeta:
        epoch = self.epoch_for(doc_id) if doc_id is not None else 0
        lag = None
        if self.role == NodeRole.FOLLOWER:
            lag = max(0, self.primary_seq - self.engine.last_applied_seq)

        return ResponseMeta(
            served_by=self.role.value,
            term=self.term,
            ownership_epoch=epoch,
            applied_seq=self.engine.last_applied_seq,
            replication_lag=lag,
        )

    def write_lock(self) -> asyncio.Lock:
        return self.engine.write_lock()


# ── Recovery ──────────────────────────────────────────────────────────────────


def _persist_node_metadata(state: NodeState) -> None:
    """Durably persist node term, role, and range configurations with fsync."""
    data_dir = Path(state.settings.DATA_DIR) / state.shard_id
    data_dir.mkdir(parents=True, exist_ok=True)
    meta_path = data_dir / "node_metadata.json"
    tmp_path = data_dir / "node_metadata.json.tmp"

    data = {
        "term": state.term,
        "role": state.role.value,
        "committed_seq": state.committed_seq,
        "ranges": [r.model_dump() for r in state.ranges],
    }
    payload = json.dumps(data, indent=2).encode("utf-8")
    with open(tmp_path, "wb") as f:
        f.write(payload)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, meta_path)
    dirfd = os.open(str(data_dir), os.O_RDONLY)
    try:
        os.fsync(dirfd)
    finally:
        os.close(dirfd)


def _reconcile_all_ownership_epochs(
    configured_ranges: Sequence[ShardRangeConfig] | None,
    meta_ranges: Sequence[dict[str, Any] | ShardRangeConfig] | None,
    snapshot_epochs: dict[str, Any] | None,
) -> list[ShardRangeConfig]:
    """Reconcile ownership epochs across configured, persisted metadata, and snapshot sources.

    Validates that:
      - All range bounds and epoch numbers are valid.
      - All non-empty sources agree on the exact same set of range bounds.
        If range boundaries differ unexpectedly, raises ValueError (fails closed).
      - Reconciles ownership epochs by taking the maximum across all valid sources,
        ensuring an older snapshot or startup config can never overwrite a newer persisted epoch.
    """
    cfg_map: dict[tuple[int, int], int] = {}
    if configured_ranges:
        for r in configured_ranges:
            if r.start < DOC_ID_MIN or r.end <= r.start or r.ownership_epoch < 0:
                raise ValueError(f"Invalid configured range bounds or epoch: {r}")
            cfg_map[(r.start, r.end)] = r.ownership_epoch

    meta_map: dict[tuple[int, int], int] = {}
    if meta_ranges:
        for item in meta_ranges:
            if isinstance(item, ShardRangeConfig):
                s, e, ep = item.start, item.end, item.ownership_epoch
            elif isinstance(item, dict):
                if "start" not in item or "end" not in item or "ownership_epoch" not in item:
                    raise ValueError(f"Invalid metadata range dict: {item}")
                try:
                    s = int(item["start"])
                    e = int(item["end"])
                    ep = int(item["ownership_epoch"])
                except (ValueError, TypeError) as exc:
                    raise ValueError(f"Non-integer metadata range bounds in {item}") from exc
            else:
                raise ValueError(f"Invalid metadata range type: {type(item).__name__}")
            if s < DOC_ID_MIN or e <= s or ep < 0:
                raise ValueError(f"Invalid metadata range bounds or epoch: {s}-{e}, epoch={ep}")
            meta_map[(s, e)] = ep

    snap_map: dict[tuple[int, int], int] = {}
    if snapshot_epochs is not None:
        if not isinstance(snapshot_epochs, dict):
            raise ValueError(
                f"ownership_epochs must be a dict, got {type(snapshot_epochs).__name__}"
            )
        for range_key, epoch in snapshot_epochs.items():
            if not isinstance(range_key, str):
                raise ValueError(f"Invalid range key type: {type(range_key).__name__}")
            parts = range_key.split("-")
            if len(parts) != 2:
                raise ValueError(f"Invalid range key format: {range_key}")
            try:
                s = int(parts[0])
                e = int(parts[1])
            except ValueError as exc:
                raise ValueError(f"Non-integer range bounds in {range_key}") from exc
            if s < DOC_ID_MIN or e <= s:
                raise ValueError(f"Invalid range boundaries: {s}-{e}")
            if not isinstance(epoch, int) or epoch < 0:
                raise ValueError(f"Invalid ownership epoch {epoch} for range {range_key}")
            snap_map[(s, e)] = epoch

    present_sources: list[tuple[str, set[tuple[int, int]]]] = []
    if cfg_map:
        present_sources.append(("configured", set(cfg_map.keys())))
    if meta_map:
        present_sources.append(("metadata", set(meta_map.keys())))
    if snap_map:
        present_sources.append(("snapshot", set(snap_map.keys())))

    if len(present_sources) > 1:
        base_name, base_keys = present_sources[0]
        for other_name, other_keys in present_sources[1:]:
            if base_keys != other_keys:
                raise ValueError(
                    f"Incompatible range mapping between {base_name} {sorted(base_keys)} "
                    f"and {other_name} {sorted(other_keys)}"
                )

    if not present_sources:
        return []

    target_keys = present_sources[0][1]
    result: list[ShardRangeConfig] = []
    for s, e in sorted(target_keys, key=lambda item: item[0]):
        epoch = 0
        if (s, e) in cfg_map:
            epoch = max(epoch, cfg_map[(s, e)])
        if (s, e) in meta_map:
            epoch = max(epoch, meta_map[(s, e)])
        if (s, e) in snap_map:
            epoch = max(epoch, snap_map[(s, e)])
        result.append(ShardRangeConfig(start=s, end=e, ownership_epoch=epoch))

    return result


def _reconcile_ownership_epochs(
    configured_ranges: list[ShardRangeConfig],
    epoch_map: dict[str, Any] | None,
) -> list[ShardRangeConfig]:
    """Reconcile snapshot ownership epochs with configured ranges."""
    return _reconcile_all_ownership_epochs(
        configured_ranges=configured_ranges,
        meta_ranges=None,
        snapshot_epochs=epoch_map,
    )


def _configure_ranges(state: NodeState) -> None:
    """Set default range from env vars if none configured."""
    if state.ranges:
        return
    start = int(os.environ.get("SHARD_RANGE_START", 0))
    end = int(os.environ.get("SHARD_RANGE_END", 2**63))
    epoch = int(os.environ.get("SHARD_OWNERSHIP_EPOCH", 1))
    state.ranges.append(ShardRangeConfig(start=start, end=end, ownership_epoch=epoch))


async def _recover(state: NodeState) -> None:
    """Load metadata, snapshot, then replay WAL. Raises WALCorruptionError on bad WAL."""
    data_dir = Path(state.settings.DATA_DIR) / state.shard_id
    data_dir.mkdir(parents=True, exist_ok=True)

    snap_mgr = SnapshotManager(data_dir)
    state.snapshot_mgr = snap_mgr
    after_seq = 0

    # 1. Load durable node metadata if present
    meta_path = data_dir / "node_metadata.json"
    is_disk_recovery = meta_path.exists()
    meta_ranges: list[dict[str, Any]] | None = None
    if meta_path.exists():
        try:
            with open(meta_path, encoding="utf-8") as f:
                meta = json.load(f)
                state.term = meta.get("term", state.term)
                state.committed_seq = meta.get("committed_seq", 0)
                if "ranges" in meta:
                    meta_ranges = meta["ranges"]
        except Exception as exc:
            logger.error("Failed to load node metadata from %s: %s", meta_path, exc)
            raise RuntimeError(
                f"Corrupt or unreadable node metadata at {meta_path}: {exc}"
            ) from exc

    # 2. Load snapshot if present
    snap = snap_mgr.load()
    snap_epochs: dict[str, Any] | None = None
    if snap:
        for doc_id, doc in snap["documents"].items():
            state.engine._docs[doc_id] = doc
            state.engine._index[doc_id] = None
        state.engine.last_applied_seq = snap["last_applied_seq"]
        state.term = max(state.term, snap.get("term", 1))
        state.engine.dedup_import(snap.get("dedup_table", {}))
        state.committed_seq = max(state.committed_seq, snap["last_applied_seq"])
        after_seq = snap["last_applied_seq"]
        snap_epochs = snap.get("ownership_epochs")

    # 3. Reconcile all range sources: startup config, persisted metadata, and snapshot
    state.ranges = _reconcile_all_ownership_epochs(
        configured_ranges=state.ranges,
        meta_ranges=meta_ranges,
        snapshot_epochs=snap_epochs,
    )

    _configure_ranges(state)

    wal = WAL(data_dir / "shard.wal")
    wal.open()
    state.wal = wal

    state.committed_seq = max(state.committed_seq, state.engine.last_applied_seq)
    if not is_disk_recovery and snap is None:
        state.committed_seq = wal.last_seq
    elif state.allow_standalone:
        # In standalone mode, all local WAL operations are committed locally.
        state.committed_seq = max(state.committed_seq, wal.last_seq)

    wal_entries = list(wal.replay(after_seq=after_seq))
    for entry in wal_entries:
        entry_term = entry.get("term", 1)
        if entry_term > state.term:
            state.term = entry_term
        rid = entry.get("request_id")

        if entry["seq"] <= state.committed_seq:
            state.engine.apply(entry)
            if rid:
                state.engine.dedup_store(
                    rid,
                    DedupEntry(
                        seq=entry["seq"],
                        outcome=WriteOutcome.COMMITTED,
                        result={"outcome": WriteOutcome.COMMITTED, "seq": entry["seq"]},
                        term=entry_term,
                    ),
                )
        else:
            # Unreconciled WAL tail: do NOT apply to engine yet and do NOT discard prematurely.
            # Record in pending_unresolved_writes and dedup table with OUTCOME_UNKNOWN.
            if rid:
                state.pending_unresolved_writes[rid] = entry
                state.engine.dedup_store(
                    rid,
                    DedupEntry(
                        seq=entry["seq"],
                        outcome=WriteOutcome.OUTCOME_UNKNOWN,
                        result={"outcome": WriteOutcome.OUTCOME_UNKNOWN, "seq": entry["seq"]},
                        term=entry_term,
                    ),
                )
            logger.warning(
                "Node %s recovery: unreconciled WAL tail entry (seq=%d, term=%d, req=%s) "
                "beyond committed_seq=%d; marking as OUTCOME_UNKNOWN",
                state.node_id,
                entry["seq"],
                entry_term,
                rid,
                state.committed_seq,
            )

    # Eager reconciliation if follower is configured and reachable
    if state.follower_url and state.pending_unresolved_writes:
        for p_rid in list(state.pending_unresolved_writes.keys()):
            try:
                await _reconcile_pending_write(state, p_rid)
            except Exception as exc:
                logger.debug("Startup reconciliation for %s deferred: %s", p_rid, exc)

    if is_disk_recovery or snap is not None or wal_entries or wal.last_seq > 0:
        if not state.allow_standalone:
            # A node recovering from disk in cluster mode must NOT restore primary authority.
            # It must start in a fenced/recovering, non-authoritative follower state.
            state.role = NodeRole.FOLLOWER
            state.lease_expires_at = 0.0
        else:
            # Standalone mode: restore configured authority (PRIMARY) and standalone lease
            if state.role == NodeRole.PRIMARY and (
                state.lease_expires_at == 0.0 or state.lease_expires_at is None
            ):
                state.lease_expires_at = float("inf")

    _persist_node_metadata(state)

    logger.info(
        "Recovery complete: shard=%s term=%d last_applied_seq=%d docs=%d",
        state.shard_id,
        state.term,
        state.engine.last_applied_seq,
        state.engine.doc_count(),
    )


# ── Replication Helper ────────────────────────────────────────────────────────


async def _replicate_to_follower(
    state: NodeState,
    seq: int,
    op: WriteOp,
    doc_id: int,
    request_id: str,
    payload: dict[str, Any] | None,
) -> bool:
    """Primary sends operation to follower and awaits fsynced acknowledgement."""
    if not state.follower_url:
        return False

    client = state.http_client
    owns_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=state.settings.REPLICATION_TIMEOUT_S)
        owns_client = True

    try:
        clean_url = state.follower_url.removeprefix("http://").removeprefix("https://").rstrip("/")
        url = f"http://{clean_url}/internal/replicate"
        req_body = {
            "shard_id": state.shard_id,
            "term": state.term,
            "seq": seq,
            "op": op.value,
            "doc_id": doc_id,
            "request_id": request_id,
            "payload": payload,
        }
        body_bytes = json.dumps(req_body, separators=(",", ":")).encode("utf-8")
        sig = compute_signature(state.settings.CLUSTER_SECRET, body_bytes)
        headers = {
            "Content-Type": "application/json",
            CLUSTER_SIG_HEADER: sig,
        }

        resp = await client.post(
            url,
            content=body_bytes,
            headers=headers,
            timeout=state.settings.REPLICATION_TIMEOUT_S,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "gap":
                state.follower_state = ReplicaState.CATCHING_UP
                logger.warning("Follower replication returned gap; marked follower as CATCHING_UP")
                return False
            return bool(data.get("status") == "ok")
        state.follower_state = ReplicaState.DOWN
        logger.warning(
            "Follower replication returned status=%d body=%s", resp.status_code, resp.text
        )
        return False
    except Exception as exc:
        logger.warning("Follower replication call failed: %s", exc)
        return False
    finally:
        if owns_client:
            await client.aclose()


async def _reconcile_pending_write(state: NodeState, request_id: str) -> WriteOutcome:
    """Ask follower whether the pending operation was durably stored and resolve state.

    Protocol rules:
    - Operation identity is (term, seq, request_id).
    - If follower has operation with matching identity -> Mark COMMITTED.
    - If follower is behind (wal_last_seq < seq) -> Keep unresolved (OUTCOME_UNKNOWN).
    - If follower is unreachable or error occurs -> Keep unresolved (OUTCOME_UNKNOWN).
    - If authoritative log reconciliation proves operation is absent (wal_last_seq >= seq
      and follower's log at seq has a conflicting entry or definitively lacks the operation)
      -> Resolve as uncommitted (remove from pending_unresolved_writes and dedup table).
    """
    if not state.follower_url:
        return WriteOutcome.OUTCOME_UNKNOWN

    pending = state.pending_unresolved_writes.get(request_id)
    seq: int | None = None
    term: int = state.term
    if pending:
        seq = pending.get("seq")
        term = pending.get("term", state.term)
    else:
        dedup = state.engine.dedup_check(request_id)
        if dedup:
            seq = dedup.seq

    if seq is None:
        return WriteOutcome.OUTCOME_UNKNOWN

    entry_id = None
    if pending:
        entry_id = compute_entry_identity(
            {
                "term": term,
                "seq": seq,
                "op": pending["op"],
                "doc_id": pending["doc_id"],
                "request_id": request_id,
                "payload": pending.get("payload"),
            }
        )
    elif state.wal:
        for w in state.wal.replay(after_seq=max(0, seq - 1)):
            if w.get("seq") == seq and w.get("request_id") == request_id:
                entry_id = w.get("entry_id") or compute_entry_identity(w)
                break

    clean_url = state.follower_url.removeprefix("http://").removeprefix("https://").rstrip("/")
    url = f"http://{clean_url}/internal/check-op"
    payload = {"term": term, "seq": seq, "request_id": request_id, "entry_id": entry_id}
    body_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    sig = compute_signature(state.settings.CLUSTER_SECRET, body_bytes)
    headers = {
        "Content-Type": "application/json",
        CLUSTER_SIG_HEADER: sig,
    }

    client = state.http_client
    owns_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=state.settings.REPLICATION_TIMEOUT_S)
        owns_client = True

    try:
        resp = await client.post(
            url,
            content=body_bytes,
            headers=headers,
            timeout=state.settings.REPLICATION_TIMEOUT_S,
        )
        if resp.status_code == 200:
            data = resp.json()
            has_op = data.get("has_op", False)
            follower_term = data.get("term", 0)
            wal_last_seq = data.get("wal_last_seq", 0)
            op_term = data.get("op_term")
            op_request_id = data.get("op_request_id")
            op_entry_id = data.get("op_entry_id")

            # 1. Complete operation identity match: (term, seq, request_id, entry_id)
            identity_match = (
                has_op
                and op_term == term
                and op_request_id == request_id
                and None not in (term, seq, request_id, op_term, op_request_id)
            )
            if identity_match and entry_id is not None and op_entry_id is not None:
                if entry_id != op_entry_id:
                    identity_match = False

            if identity_match:
                if pending:
                    apply_entry = {
                        "seq": seq,
                        "term": term,
                        "op": pending["op"],
                        "doc_id": pending["doc_id"],
                        "request_id": request_id,
                        "payload": pending.get("payload"),
                    }
                    state.engine.apply(apply_entry)
                elif state.wal:
                    for w in state.wal.replay(after_seq=max(0, seq - 1)):
                        if w.get("seq") == seq and w.get("request_id") == request_id:
                            state.engine.apply(w)
                            break
                state.engine.dedup_store(
                    request_id,
                    DedupEntry(
                        seq=seq,
                        outcome=WriteOutcome.COMMITTED,
                        result={"outcome": WriteOutcome.COMMITTED, "seq": seq},
                        term=term,
                    ),
                )
                state.committed_seq = max(state.committed_seq, seq)
                _persist_node_metadata(state)
                state.pending_unresolved_writes.pop(request_id, None)
                state.follower_acked_seq = max(state.follower_acked_seq, seq)
                logger.info(
                    "Pending write %s (term=%d, seq=%d) reconciled as COMMITTED",
                    request_id,
                    term,
                    seq,
                )
                return WriteOutcome.COMMITTED

            # 2. Conclusive non-commitment check:
            # - Follower is authoritative for the operation's term (follower_term >= term).
            # - Follower has durably logged at or past seq (wal_last_seq >= seq).
            # - Complete identity check confirmed operation is absent or conflicting (not has_op).
            # We never infer non-commitment from wal_last_seq alone without authoritative term
            # and full identity verification.
            is_authoritative_absent = follower_term >= term and wal_last_seq >= seq and not has_op

            if is_authoritative_absent:
                logger.info(
                    "Authoritative reconciliation: write %s (term=%d, seq=%d) is UNCOMMITTED. "
                    "Discarding uncommitted primary tail down to committed_seq=%d.",
                    request_id,
                    term,
                    seq,
                    state.committed_seq,
                )
                if state.wal and state.wal.last_seq > state.committed_seq:
                    state.wal.truncate_after(state.committed_seq)
                _persist_node_metadata(state)
                state.pending_unresolved_writes.pop(request_id, None)
                state.engine._dedup.pop(request_id, None)
                return WriteOutcome.OUTCOME_UNKNOWN

            # 3. Follower is behind or non-authoritative: commitment status cannot be proven.
            # Retain OUTCOME_UNKNOWN and keep write in pending_unresolved_writes to block writes.
            logger.info(
                "Follower (term=%d, wal_last_seq=%d) cannot prove status of (term=%d, seq=%d); "
                "write %s remains unresolved and subsequent writes remain blocked",
                follower_term,
                wal_last_seq,
                term,
                seq,
                request_id,
            )
            return WriteOutcome.OUTCOME_UNKNOWN
    except Exception as exc:
        logger.warning("Could not reconcile pending write %s with follower: %s", request_id, exc)
    finally:
        if owns_client:
            await client.aclose()

    return WriteOutcome.OUTCOME_UNKNOWN


def _write_fingerprint(op: WriteOp, doc_id: int, payload: dict[str, Any] | None) -> str:
    """Stable hash of a write's content, used to detect request_id reuse with different data."""
    canon = json.dumps(
        {"op": op.value, "doc_id": doc_id, "payload": payload},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


def _subtract_range(ranges: list[ShardRangeConfig], start: int, end: int) -> list[ShardRangeConfig]:
    """Remove [start, end) from owned ranges, handling prefix / suffix / interior / whole."""
    out: list[ShardRangeConfig] = []
    for r in ranges:
        if r.end <= start or r.start >= end:
            out.append(r)
            continue
        if r.start < start:
            out.append(
                ShardRangeConfig(start=r.start, end=start, ownership_epoch=r.ownership_epoch)
            )
        if r.end > end:
            out.append(ShardRangeConfig(start=end, end=r.end, ownership_epoch=r.ownership_epoch))
    return out


# ── Write Helper ──────────────────────────────────────────────────────────────


async def _execute_write(
    state: NodeState,
    request_id: str,
    op: WriteOp,
    doc_id: int,
    payload: dict[str, Any] | None,
) -> WriteResponse:
    """Run a write under the per-shard write lock with replication and lease fencing."""
    fp = _write_fingerprint(op, doc_id, payload)
    async with state.write_lock():
        # 1. Role check: only primary accepts writes
        if state.role != NodeRole.PRIMARY:
            raise NotPrimary(f"Node {state.node_id} is {state.role.value}, not primary.")

        # 2. Lease check: remaining lease > T + M
        min_lease = state.settings.REPLICATION_TIMEOUT_S + state.settings.SAFETY_MARGIN_S
        if state.remaining_lease() <= min_lease:
            rem = state.remaining_lease()
            raise LeaseExpired(f"Primary lease remaining ({rem:.2f}s) <= T+M ({min_lease:.2f}s).")

        # 3. Range ownership & pause check
        for p_start, p_end in state.paused_ranges:
            if p_start <= doc_id < p_end:
                raise RangePaused(f"Range [{p_start}, {p_end}) is paused for migration.")

        if not state.owns(doc_id):
            raise OutOfRange(
                f"doc_id={doc_id} is outside all ranges owned by shard {state.shard_id}."
            )

        # 4. Check & reconcile pending unresolved writes before proceeding
        if state.pending_unresolved_writes:
            if request_id in state.pending_unresolved_writes:
                reconciled = await _reconcile_pending_write(state, request_id)
                if reconciled == WriteOutcome.COMMITTED:
                    ent = state.engine.dedup_check(request_id)
                    s = ent.seq if ent else 0
                    return WriteResponse(
                        outcome=WriteOutcome.COMMITTED,
                        request_id=request_id,
                        seq=s,
                        meta=state.response_meta(doc_id),
                    )
                elif request_id in state.pending_unresolved_writes:
                    p_seq = state.pending_unresolved_writes[request_id]["seq"]
                    raise OutcomeUnknown(
                        f"Previous write attempt (seq={p_seq}) outcome remains unknown on follower."
                    )
            else:
                for p_rid in list(state.pending_unresolved_writes.keys()):
                    await _reconcile_pending_write(state, p_rid)
                if state.pending_unresolved_writes:
                    raise ReplicationUnavailable(
                        "Cannot accept subsequent writes while an earlier write has "
                        "an unresolved replication outcome."
                    )

        # 5. Dedup check: return original result for retried request_id
        existing = state.engine.dedup_check(request_id)
        if existing is not None:
            if existing.fingerprint is not None and existing.fingerprint != fp:
                raise RequestIdReused(
                    f"request_id {request_id!r} was already used for a different write."
                )
            if existing.outcome == WriteOutcome.OUTCOME_UNKNOWN:
                reconciled = await _reconcile_pending_write(state, request_id)
                if reconciled == WriteOutcome.COMMITTED:
                    return WriteResponse(
                        outcome=WriteOutcome.COMMITTED,
                        request_id=request_id,
                        seq=existing.seq,
                        meta=state.response_meta(doc_id),
                    )
                elif request_id in state.pending_unresolved_writes:
                    raise OutcomeUnknown(
                        f"Previous write attempt (seq={existing.seq}) outcome remains unknown."
                    )
            else:
                return WriteResponse(
                    outcome=WriteOutcome(existing.outcome),
                    request_id=request_id,
                    seq=existing.seq,
                    meta=state.response_meta(doc_id),
                )

        # 6. Replicated follower check (or explicit standalone mode)
        if not state.follower_url:
            if not state.allow_standalone:
                raise ReplicationUnavailable(
                    "Primary has no configured follower; synchronous replication unavailable."
                )
        else:
            if state.follower_state != ReplicaState.SYNCED:
                raise ReplicationUnavailable(
                    f"Follower is {state.follower_state.value}; write rejected before WAL append."
                )

        # 7. Pre-WAL business logic validity check
        if op == WriteOp.INSERT:
            if doc_id in state.engine._docs:
                raise DuplicateKey(f"doc_id={doc_id} already exists.")
        elif op in (WriteOp.UPDATE, WriteOp.DELETE):
            if doc_id not in state.engine._docs:
                raise NotFound(f"doc_id={doc_id} not found.")

        # Test hook 1: before WAL append
        trigger_hook(
            "before_wal_append",
            state.settings.ENABLE_TEST_HOOKS,
            request_id=request_id,
            doc_id=doc_id,
        )

        # 8. Assign sequence number & append to primary WAL (durable)
        assert state.wal is not None
        seq = state.wal.next_seq()
        state.wal.append(
            seq=seq,
            term=state.term,
            op=op.value,
            doc_id=doc_id,
            request_id=request_id,
            payload=payload,
        )

        # Test hook 2: after primary fsync
        trigger_hook(
            "after_primary_fsync",
            state.settings.ENABLE_TEST_HOOKS,
            request_id=request_id,
            seq=seq,
            doc_id=doc_id,
        )

        # 9. Synchronous replication to follower
        if state.follower_url:
            replicated = await _replicate_to_follower(state, seq, op, doc_id, request_id, payload)
            if not replicated:
                # Follower failed after primary WAL append: outcome unknown
                state.follower_state = ReplicaState.DOWN
                state.pending_unresolved_writes[request_id] = {
                    "seq": seq,
                    "term": state.term,
                    "op": op.value,
                    "doc_id": doc_id,
                    "payload": payload,
                }
                state.engine.dedup_store(
                    request_id,
                    DedupEntry(
                        seq=seq,
                        outcome=WriteOutcome.OUTCOME_UNKNOWN,
                        result={"outcome": WriteOutcome.OUTCOME_UNKNOWN, "seq": seq},
                        term=state.term,
                        fingerprint=fp,
                    ),
                )
                raise OutcomeUnknown(
                    f"Write appended to primary WAL (seq={seq}), but follower replication failed."
                )
            state.follower_acked_seq = seq
            state.committed_seq = max(state.committed_seq, seq)
            state.pending_unresolved_writes.pop(request_id, None)
            _persist_node_metadata(state)
        elif state.allow_standalone:
            # Explicit standalone mode: local-only commit (clearly distinguished)
            state.committed_seq = max(state.committed_seq, seq)
            _persist_node_metadata(state)
        else:
            raise ReplicationUnavailable(
                "Primary has no configured follower; cannot commit without synchronous replication."
            )

        # 9. Apply to memory state via engine.apply() — unified idempotent codepath (M3)
        apply_entry = {
            "seq": seq,
            "term": state.term,
            "op": op.value,
            "doc_id": doc_id,
            "request_id": request_id,
            "payload": payload,
        }
        state.engine.apply(apply_entry)
        result = {"outcome": WriteOutcome.COMMITTED, "seq": seq}
        state.engine.dedup_store(
            request_id,
            DedupEntry(
                seq=seq,
                outcome=WriteOutcome.COMMITTED,
                result=result,
                term=state.term,
                fingerprint=fp,
            ),
        )

        # Test hook 4: after follower ack before client ack
        trigger_hook(
            "after_follower_ack_before_client_ack",
            state.settings.ENABLE_TEST_HOOKS,
            request_id=request_id,
            seq=seq,
            doc_id=doc_id,
        )

        # 10. Final lease check before client reply
        if state.lease_expires_at != float("inf") and not state.is_lease_valid():
            raise OutcomeUnknown("Primary lease expired before client acknowledgement.")

    # Test hook 5: after client ack
    trigger_hook(
        "after_client_ack",
        state.settings.ENABLE_TEST_HOOKS,
        request_id=request_id,
        seq=seq,
        doc_id=doc_id,
    )

    return WriteResponse(
        outcome=WriteOutcome.COMMITTED,
        request_id=request_id,
        seq=seq,
        meta=state.response_meta(doc_id),
    )


# ── Rejoin & Resync Helper ────────────────────────────────────────────────────


async def resync_with_primary(
    state: NodeState,
    primary_url: str,
    *,
    http_client: httpx.AsyncClient | None = None,
) -> bool:
    """Execute rejoin, divergence discovery, tail truncation and catch-up with new primary."""
    clean_primary = primary_url.removeprefix("http://").removeprefix("https://").rstrip("/")
    owns_client = False
    client = http_client or state.http_client
    if client is None:
        client = httpx.AsyncClient(timeout=state.settings.REPLICATION_TIMEOUT_S + 2.0)
        owns_client = True

    try:
        assert state.wal is not None
        # 1. Collect local log entries with deterministic identities
        local_entries = [
            {
                "seq": e["seq"],
                "term": e["term"],
                "op": e.get("op"),
                "doc_id": e.get("doc_id"),
                "request_id": e.get("request_id", ""),
                "payload": e.get("payload"),
                "entry_id": e.get("entry_id") or compute_entry_identity(e),
            }
            for e in state.wal.replay(after_seq=0)
        ]

        # 2. Query divergence point
        div_url = f"http://{clean_primary}/internal/divergence"
        body_dict = {"shard_id": state.shard_id, "entries": local_entries}
        body_bytes = json.dumps(body_dict, separators=(",", ":")).encode("utf-8")
        sig = compute_signature(state.settings.CLUSTER_SECRET, body_bytes)
        headers = {"Content-Type": "application/json", CLUSTER_SIG_HEADER: sig}

        resp = await client.post(div_url, content=body_bytes, headers=headers)
        if resp.status_code != 200:
            logger.warning(
                "Divergence check failed with status %d: %s", resp.status_code, resp.text
            )
            return False

        div_data = resp.json()
        needs_snapshot = div_data.get("needs_snapshot", False)
        div_seq = div_data.get("divergence_seq", 0)

        async with state.write_lock():
            if needs_snapshot:
                # Full snapshot resync
                snap_url = f"http://{clean_primary}/internal/snapshot"
                snap_sig = compute_signature(state.settings.CLUSTER_SECRET, b"")
                snap_resp = await client.get(snap_url, headers={CLUSTER_SIG_HEADER: snap_sig})
                if snap_resp.status_code != 200:
                    logger.warning("Snapshot fetch failed: %d", snap_resp.status_code)
                    return False

                # 1. Fetch snapshot into temporary data first
                snap_data = snap_resp.json()
                snap_last_applied_seq = int(snap_data.get("last_applied_seq", 0))
                snap_term = int(snap_data.get("term", 0))

                # 2. Safety validation:
                # Safety assumption: Under synchronous replication, a write is only
                # committed after durable replication to both primary and follower.
                # A legitimate primary possessing authoritative shard state must have
                # last_applied_seq >= follower's locally committed_seq. If snapshot
                # last_applied_seq < committed_seq, the primary's snapshot is missing
                # committed transactions. Rejecting resync prevents rolling back history.
                if snap_last_applied_seq < state.committed_seq:
                    logger.error(
                        "Shard %s cannot reconcile via snapshot: snapshot last_applied_seq (%d) "
                        "< locally committed_seq (%d). Refusing to silently overwrite committed "
                        "history. Node fails closed.",
                        state.shard_id,
                        snap_last_applied_seq,
                        state.committed_seq,
                    )
                    return False

                # Term fencing: snapshot must not come from a term earlier than local term
                if snap_term < state.term:
                    logger.error(
                        "Shard %s cannot reconcile via snapshot: snapshot term (%d) < "
                        "local term (%d). Node fails closed.",
                        state.shard_id,
                        snap_term,
                        state.term,
                    )
                    return False

                # 3. Durably persist snapshot before mutating in-memory engine or truncating WAL
                data_dir = Path(state.settings.DATA_DIR) / state.shard_id
                snap_mgr = state.snapshot_mgr or SnapshotManager(data_dir)
                state.snapshot_mgr = snap_mgr

                parsed_docs: dict[int, dict[str, Any]] = {}
                raw_docs = snap_data.get("documents", {})
                if isinstance(raw_docs, dict):
                    for did_str, d in raw_docs.items():
                        parsed_docs[int(did_str)] = d
                else:
                    for d in raw_docs:
                        parsed_docs[int(d["doc_id"])] = d

                target_term = state.term
                if "term" in div_data and div_data["term"] > target_term:
                    target_term = div_data["term"]
                if snap_term > target_term:
                    target_term = snap_term

                snap_ownership_epochs = snap_data.get("ownership_epochs")
                try:
                    reconciled_ranges = _reconcile_ownership_epochs(
                        state.ranges, snap_ownership_epochs
                    )
                except Exception as exc:
                    logger.error(
                        "Shard %s cannot reconcile via snapshot: range mapping incompatible: "
                        "%s. Node fails closed.",
                        state.shard_id,
                        exc,
                    )
                    return False

                # Preserve fencing rules: avoid accepting stale ownership epochs from primary
                if snap_ownership_epochs:
                    is_stale_epoch = False
                    for r in state.ranges:
                        k = f"{r.start}-{r.end}"
                        if (
                            k in snap_ownership_epochs
                            and snap_ownership_epochs[k] < r.ownership_epoch
                        ):
                            logger.error(
                                "Shard %s cannot reconcile via snapshot: snapshot epoch (%d) < "
                                "local epoch (%d) for range %s. Rejecting stale snapshot. "
                                "Node fails closed.",
                                state.shard_id,
                                snap_ownership_epochs[k],
                                r.ownership_epoch,
                                k,
                            )
                            is_stale_epoch = True
                            break
                    if is_stale_epoch:
                        return False

                target_epochs = {f"{r.start}-{r.end}": r.ownership_epoch for r in reconciled_ranges}
                dedup_table = snap_data.get("dedup_table", {})

                try:
                    snap_mgr.save(
                        documents=parsed_docs,
                        last_applied_seq=snap_last_applied_seq,
                        term=target_term,
                        ownership_epochs=target_epochs,
                        dedup_table=dedup_table,
                    )
                except Exception as exc:
                    logger.error(
                        "Shard %s failed to persist snapshot to disk: %s. Node fails closed.",
                        state.shard_id,
                        exc,
                    )
                    return False

                # 4. Now that snapshot is durably saved on disk, update in-memory state
                state.term = target_term
                state.ranges = reconciled_ranges
                state.engine._docs.clear()
                state.engine._index.clear()
                for did, doc in parsed_docs.items():
                    state.engine._docs[did] = doc
                    state.engine._index[did] = None
                state.engine.last_applied_seq = snap_last_applied_seq
                state.engine.dedup_import(dedup_table)

                # 5. Durably truncate WAL only AFTER the new snapshot has been persisted
                if state.wal is not None:
                    state.wal.truncate_after(state.engine.last_applied_seq)
            else:
                # Invariant: Rejoining node must never silently overwrite locally committed history!
                if div_seq < state.committed_seq:
                    logger.error(
                        "Shard %s cannot reconcile: divergence_seq (%d) < locally "
                        "committed_seq (%d). Refusing to silently overwrite committed "
                        "history. Node fails closed.",
                        state.shard_id,
                        div_seq,
                        state.committed_seq,
                    )
                    return False

                if "term" in div_data and div_data["term"] > state.term:
                    state.term = div_data["term"]

                primary_epochs = div_data.get("ownership_epochs")
                if primary_epochs:
                    for r in state.ranges:
                        k = f"{r.start}-{r.end}"
                        if k in primary_epochs and primary_epochs[k] < r.ownership_epoch:
                            logger.error(
                                "Shard %s cannot reconcile via divergence: primary epoch (%d) < "
                                "local epoch (%d) for range %s. Rejecting stale primary.",
                                state.shard_id,
                                primary_epochs[k],
                                r.ownership_epoch,
                                k,
                            )
                            return False
                    try:
                        state.ranges = _reconcile_ownership_epochs(state.ranges, primary_epochs)
                    except Exception as exc:
                        logger.error(
                            "Shard %s cannot reconcile ownership epochs from primary: %s",
                            state.shard_id,
                            exc,
                        )
                        return False
                # Truncate divergent tail after div_seq
                state.wal.truncate_after(div_seq)
                # Rebuild in-memory engine and dedup up to div_seq
                state.engine._docs.clear()
                state.engine._index.clear()
                state.engine._dedup.clear()
                state.engine.last_applied_seq = 0
                for entry in state.wal.replay(after_seq=0):
                    state.engine.apply(entry)
                    rid = entry.get("request_id")
                    if rid:
                        state.engine.dedup_store(
                            rid,
                            DedupEntry(
                                seq=entry["seq"],
                                outcome=WriteOutcome.COMMITTED,
                                result={"outcome": WriteOutcome.COMMITTED, "seq": entry["seq"]},
                                term=entry.get("term", state.term),
                            ),
                        )

                # Catchup missing entries with seq > div_seq
                catchup_url = f"http://{clean_primary}/internal/catchup"
                req_b = {"shard_id": state.shard_id, "after_seq": div_seq}
                b_bytes = json.dumps(req_b, separators=(",", ":")).encode("utf-8")
                c_sig = compute_signature(state.settings.CLUSTER_SECRET, b_bytes)
                c_resp = await client.post(
                    catchup_url,
                    content=b_bytes,
                    headers={
                        "Content-Type": "application/json",
                        CLUSTER_SIG_HEADER: c_sig,
                    },
                )
                if c_resp.status_code != 200:
                    logger.warning(
                        "Catchup failed with status %d: %s", c_resp.status_code, c_resp.text
                    )
                    return False

                c_data = c_resp.json()
                for entry in c_data.get("entries", []):
                    state.wal.append(
                        seq=entry["seq"],
                        term=entry["term"],
                        op=entry["op"],
                        doc_id=entry["doc_id"],
                        request_id=entry.get("request_id", ""),
                        payload=entry.get("payload"),
                    )
                    if entry.get("term", 0) > state.term:
                        state.term = entry["term"]
                    state.engine.apply(entry)
                    rid = entry.get("request_id")
                    if rid:
                        state.engine.dedup_store(
                            rid,
                            DedupEntry(
                                seq=entry["seq"],
                                outcome=WriteOutcome.COMMITTED,
                                result={"outcome": WriteOutcome.COMMITTED, "seq": entry["seq"]},
                            ),
                        )

            state.committed_seq = max(state.committed_seq, state.engine.last_applied_seq)
            _persist_node_metadata(state)

            # Inform primary of completion
            state.role = NodeRole.FOLLOWER
            state.primary_seq = div_data.get("last_seq", state.engine.last_applied_seq)
            complete_url = f"http://{clean_primary}/internal/rejoin/complete"
            comp_body = {
                "shard_id": state.shard_id,
                "node_id": state.node_id,
                "last_applied_seq": state.engine.last_applied_seq,
                "term": state.term,
                "committed_seq": state.committed_seq,
                "ownership_epochs": {f"{r.start}-{r.end}": r.ownership_epoch for r in state.ranges},
            }
            comp_bytes = json.dumps(comp_body, separators=(",", ":")).encode("utf-8")
            comp_sig = compute_signature(state.settings.CLUSTER_SECRET, comp_bytes)
            comp_resp = await client.post(
                complete_url,
                content=comp_bytes,
                headers={"Content-Type": "application/json", CLUSTER_SIG_HEADER: comp_sig},
            )
            if comp_resp.status_code != 200:
                logger.error(
                    "Primary rejected rejoin_complete for shard %s on node %s: HTTP %d: %s",
                    state.shard_id,
                    state.node_id,
                    comp_resp.status_code,
                    comp_resp.text,
                )
                return False

            logger.info(
                "Resync complete for shard %s on node %s: caught up to seq %d",
                state.shard_id,
                state.node_id,
                state.engine.last_applied_seq,
            )
            return True
    except Exception as exc:
        logger.warning("Resync with primary failed: %s", exc)
        return False
    finally:
        if owns_client:
            await client.aclose()


# ── App factory ───────────────────────────────────────────────────────────────


def build_app(
    settings: Settings,
    *,
    node_id: str,
    shard_id: str,
    ranges: list[ShardRangeConfig] | None = None,
    role: NodeRole = NodeRole.PRIMARY,
    term: int = 1,
    lease_expires_at: float | None = None,
    follower_url: str | None = None,
    follower_state: ReplicaState = ReplicaState.SYNCED,
    follower_transport: httpx.AsyncBaseTransport | None = None,
    coordinator_url: str | None = None,
    coordinator_transport: httpx.AsyncBaseTransport | None = None,
    http_client: httpx.AsyncClient | None = None,
    allow_standalone: bool = False,
    advertise_addr: str | None = None,
) -> FastAPI:
    """Factory: create and return the configured FastAPI shard-node app."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        state = NodeState(settings, node_id=node_id, shard_id=shard_id)
        state.allow_standalone = (
            allow_standalone
            or getattr(settings, "STANDALONE_MODE", False)
            or (os.environ.get("STANDALONE_MODE", "false").lower() == "true")
        )
        if ranges:
            state.ranges = list(ranges)
        state.role = role
        state.term = term
        state.follower_url = follower_url
        state.follower_state = follower_state
        state.advertise_addr = (
            advertise_addr
            or os.environ.get("NODE_ADDRESS")
            or f"{node_id}:{os.environ.get('PORT', '8200')}"
        )

        coord_url = coordinator_url or os.environ.get("COORDINATOR_URL")
        state.coordinator_url = coord_url

        if lease_expires_at is not None:
            state.lease_expires_at = lease_expires_at
        elif role == NodeRole.PRIMARY:
            # Standalone default to inf so unit tests not testing leases pass
            state.lease_expires_at = float("inf")
        else:
            state.lease_expires_at = 0.0

        # Configure HTTP client for outbound replication
        owns_client = False
        if http_client is not None:
            state.http_client = http_client
        elif follower_transport and follower_url:
            clean_url = follower_url.removeprefix("http://").removeprefix("https://")
            mounts = {f"http://{clean_url}": follower_transport}
            state.http_client = httpx.AsyncClient(
                mounts=mounts, timeout=settings.REPLICATION_TIMEOUT_S
            )
            owns_client = True

        app.state.node = state
        node_logger = get_logger(__name__, node_id=node_id, shard_id=shard_id)
        node_logger.info("Shard node starting: %s shard=%s role=%s", node_id, shard_id, role.value)

        try:
            await _recover(state)
            state.ready = True
            node_logger.info("Node ready: %s", node_id)
        except WALCorruptionError as exc:
            node_logger.error("WAL corruption — node refusing to serve: %s", exc)
        except Exception as exc:
            node_logger.error("Recovery failed: %s", exc, exc_info=True)

        # Background heartbeat sender task (Module 8 / H6)
        async def _heartbeat_sender() -> None:
            if not coord_url:
                return
            clean_coord = coord_url.removeprefix("http://").removeprefix("https://").rstrip("/")
            url = f"http://{clean_coord}/heartbeat"
            reg_url = f"http://{clean_coord}/register"
            client_kwargs: dict[str, Any] = {"timeout": settings.HEARTBEAT_INTERVAL_S}
            if coordinator_transport:
                client_kwargs["transport"] = coordinator_transport
            registered = False
            async with httpx.AsyncClient(**client_kwargs) as hb_client:
                while True:
                    await asyncio.sleep(settings.HEARTBEAT_INTERVAL_S)
                    # A node that failed recovery must NOT look healthy to the coordinator.
                    if not state.ready:
                        continue

                    # 1. Register (idempotent) until the coordinator acknowledges us.
                    if not registered:
                        try:
                            reg_body = {
                                "node_id": state.node_id,
                                "shard_id": state.shard_id,
                                "address": state.advertise_addr,
                                "requested_role": state.role.value,
                            }
                            rb = json.dumps(reg_body, separators=(",", ":")).encode("utf-8")
                            rsig = compute_signature(settings.CLUSTER_SECRET, rb)
                            rresp = await hb_client.post(
                                reg_url,
                                content=rb,
                                headers={
                                    "Content-Type": "application/json",
                                    CLUSTER_SIG_HEADER: rsig,
                                },
                            )
                            registered = rresp.status_code == 200
                        except Exception as exc:
                            node_logger.debug("Registration failed (will retry): %s", exc)

                    if state.role != NodeRole.PRIMARY:
                        continue
                    try:
                        hb_data = {
                            "node_id": state.node_id,
                            "shard_id": state.shard_id,
                            "role": state.role.value,
                            "term": state.term,
                            "last_applied_seq": state.engine.last_applied_seq,
                            "follower_acked_seq": state.follower_acked_seq,
                            "follower_state": state.follower_state.value,
                            "timestamp_monotonic": time.monotonic(),
                        }
                        body_b = json.dumps(hb_data, separators=(",", ":")).encode("utf-8")
                        sig = compute_signature(settings.CLUSTER_SECRET, body_b)
                        headers = {"Content-Type": "application/json", CLUSTER_SIG_HEADER: sig}
                        # Lease safety: count from when the request was SENT, never from when
                        # the reply arrived, so we can never outlive the coordinator's lease.
                        t_send = time.monotonic()
                        resp = await hb_client.post(url, content=body_b, headers=headers)
                        if resp.status_code == 200:
                            data = resp.json()
                            if data and "granted_at" in data and "expires_at" in data:
                                duration = data["expires_at"] - data["granted_at"]
                                state.lease_expires_at = t_send + (
                                    duration if duration > 0 else settings.LEASE_DURATION_S
                                )
                            elif data is None:
                                registered = False  # coordinator does not know us; re-register
                        elif resp.status_code == 409:
                            node_logger.warning(
                                "Heartbeat returned 409 STALE_TERM; demoting to follower"
                            )
                            state.role = NodeRole.FOLLOWER
                            state.lease_expires_at = 0.0
                    except Exception as exc:
                        node_logger.debug("Heartbeat sending failed: %s", exc)

        # Primary-side recovery: when the follower is not SYNCED, ask it to re-sync.
        async def _follower_recovery() -> None:
            if not coord_url:
                return
            while True:
                await asyncio.sleep(settings.HEARTBEAT_INTERVAL_S)
                if (
                    not state.ready
                    or state.role != NodeRole.PRIMARY
                    or not state.follower_url
                    or state.follower_state == ReplicaState.SYNCED
                ):
                    continue
                try:
                    clean = state.follower_url.removeprefix("http://").removeprefix("https://")
                    raw = json.dumps(
                        {"primary_url": state.advertise_addr}, separators=(",", ":")
                    ).encode("utf-8")
                    sig = compute_signature(settings.CLUSTER_SECRET, raw)
                    client = state.http_client
                    own = client is None
                    if client is None:
                        client = httpx.AsyncClient(timeout=settings.REPLICATION_TIMEOUT_S + 10.0)
                    try:
                        await client.post(
                            f"http://{clean.rstrip('/')}/internal/resync",
                            content=raw,
                            headers={"Content-Type": "application/json", CLUSTER_SIG_HEADER: sig},
                            timeout=settings.REPLICATION_TIMEOUT_S + 10.0,
                        )
                    finally:
                        if own:
                            await client.aclose()
                except Exception as exc:
                    node_logger.debug("Follower resync request failed (will retry): %s", exc)

        recovery_task = asyncio.create_task(_follower_recovery())
        hb_task = asyncio.create_task(_heartbeat_sender())

        yield

        for t in (hb_task, recovery_task):
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

        if owns_client and state.http_client:
            await state.http_client.aclose()

        if state.ready and state.wal and state.snapshot_mgr:
            node_logger.info("Shutdown: saving snapshot")
            try:
                state.snapshot_mgr.save(
                    documents=state.engine.all_docs(),
                    last_applied_seq=state.engine.last_applied_seq,
                    term=state.term,
                    ownership_epochs={
                        f"{r.start}-{r.end}": r.ownership_epoch for r in state.ranges
                    },
                    dedup_table=state.engine.dedup_export(),
                )
            except Exception as exc:
                node_logger.error("Snapshot on shutdown failed: %s", exc)
            state.wal.close()

    app = FastAPI(title=f"Shard Node — {shard_id}", version="0.1.0", lifespan=lifespan)

    # ── Middleware ────────────────────────────────────────────────────────────

    @app.middleware("http")
    async def _body_size_limit(request: Request, call_next: Any) -> Response:
        cl = request.headers.get("content-length")
        if "transfer-encoding" in request.headers:
            return JSONResponse(
                status_code=411,
                content={
                    "error_code": "LENGTH_REQUIRED",
                    "error_message": "Chunked bodies not accepted; send Content-Length.",
                },
            )
        try:
            too_big = bool(cl) and int(cl or 0) > settings.MAX_BODY_SIZE_BYTES
        except ValueError:
            too_big = True
        if too_big:
            return JSONResponse(
                status_code=413,
                content={
                    "error_code": "BODY_TOO_LARGE",
                    "error_message": "Request body exceeds 2 MB.",
                },
            )
        response: Response = await call_next(request)
        return response

    cluster_auth = make_cluster_auth_dependency(lambda: settings.CLUSTER_SECRET)

    def get_node(request: Request) -> NodeState:
        state: NodeState = request.app.state.node
        if not state.ready:
            raise HTTPException(
                status_code=503,
                detail={"error_code": "NODE_NOT_READY", "error_message": "Node is recovering."},
            )
        return state

    # ── Public Endpoints ──────────────────────────────────────────────────────

    @app.post("/doc", status_code=201)
    async def insert_doc(body: InsertBody, request: Request) -> WriteResponse:
        state = get_node(request)
        if state.role != NodeRole.PRIMARY:
            raise NotPrimary(
                f"Node {state.node_id} is a follower; cannot accept writes."
            ).to_http_exception()

        try:
            doc = Document.model_validate(body.document)
        except Exception as exc:
            raise HTTPException(
                status_code=422,
                detail={"error_code": "VALIDATION_ERROR", "error_message": str(exc)},
            ) from exc

        doc_dict = doc.model_dump()
        doc_id = doc_dict["doc_id"]
        try:
            return await _execute_write(state, body.request_id, WriteOp.INSERT, doc_id, doc_dict)
        except DocstoreError as exc:
            raise exc.to_http_exception() from exc

    @app.put("/doc/{doc_id}")
    async def update_doc(doc_id: int, body: UpdateBody, request: Request) -> WriteResponse:
        state = get_node(request)
        if state.role != NodeRole.PRIMARY:
            raise NotPrimary(
                f"Node {state.node_id} is a follower; cannot accept writes."
            ).to_http_exception()

        try:
            doc = Document.model_validate(body.document)
        except Exception as exc:
            raise HTTPException(
                status_code=422,
                detail={"error_code": "VALIDATION_ERROR", "error_message": str(exc)},
            ) from exc

        if doc.doc_id != doc_id:
            raise HTTPException(
                status_code=400,
                detail={
                    "error_code": "VALIDATION_ERROR",
                    "error_message": "doc_id in path and body must match.",
                },
            )

        doc_dict = doc.model_dump()
        try:
            return await _execute_write(state, body.request_id, WriteOp.UPDATE, doc_id, doc_dict)
        except DocstoreError as exc:
            raise exc.to_http_exception() from exc

    @app.delete("/doc/{doc_id}")
    async def delete_doc(doc_id: int, body: DeleteBody, request: Request) -> WriteResponse:
        state = get_node(request)
        if state.role != NodeRole.PRIMARY:
            raise NotPrimary(
                f"Node {state.node_id} is a follower; cannot accept writes."
            ).to_http_exception()

        try:
            return await _execute_write(state, body.request_id, WriteOp.DELETE, doc_id, None)
        except DocstoreError as exc:
            raise exc.to_http_exception() from exc

    @app.get("/doc/{doc_id}")
    async def get_doc(doc_id: int, request: Request) -> ReadResponse:
        state = get_node(request)
        if not state.owns(doc_id):
            raise OutOfRange(f"doc_id={doc_id} outside owned ranges.").to_http_exception()

        if state.role == NodeRole.PRIMARY and not state.is_lease_valid():
            raise LeaseExpired("Primary lease has expired.").to_http_exception()

        doc = state.engine.get(doc_id)
        if doc is None:
            raise NotFound(f"doc_id={doc_id} not found.").to_http_exception()

        return ReadResponse(
            document=Document.model_validate(doc),
            meta=state.response_meta(doc_id),
        )

    @app.post("/query")
    async def query_docs(body: QueryFilter, request: Request) -> QueryResponse:
        state = get_node(request)
        try:
            validate_query_filter(body.filter)
        except InvalidQuery as exc:
            raise exc.to_http_exception() from exc

        if state.role == NodeRole.PRIMARY and not state.is_lease_valid():
            raise LeaseExpired("Primary lease has expired.").to_http_exception()

        # Collect candidate documents according to shard-key constraints & ownership
        constraint = detect_shard_key_constraint(body.filter)
        candidates: list[dict[str, Any]] = []

        if constraint.kind == "exact":
            assert constraint.doc_id is not None
            if state.owns(constraint.doc_id):
                doc = state.engine.get(constraint.doc_id)
                if doc is not None:
                    candidates.append(doc)
        elif constraint.kind == "in":
            for doc_id in constraint.doc_ids or []:
                if state.owns(doc_id):
                    doc = state.engine.get(doc_id)
                    if doc is not None:
                        candidates.append(doc)
        elif constraint.kind == "range":
            assert constraint.range_start is not None and constraint.range_end is not None
            for r in state.ranges:
                s = max(constraint.range_start, r.start)
                e = min(constraint.range_end, r.end)
                if s < e:
                    candidates.extend(state.engine.range_scan(s, e))
        else:
            for r in state.ranges:
                candidates.extend(state.engine.range_scan(r.start, r.end))

        matched = [doc for doc in candidates if evaluate_filter(doc, body.filter)]
        sorted_docs = sort_documents(matched, sort_field=body.sort_field, sort_dir=body.sort_dir)
        total = len(sorted_docs)
        paginated = sorted_docs[body.skip : body.skip + body.limit]

        return QueryResponse(
            documents=paginated,
            total=total,
            meta=state.response_meta(),
        )

    @app.get("/health")
    async def health(request: Request) -> dict[str, str]:
        state: NodeState = request.app.state.node
        if not state.ready:
            raise HTTPException(
                status_code=503,
                detail={"error_code": "NODE_NOT_READY", "error_message": "Recovering."},
            )
        return {"status": "ok"}

    @app.get("/status")
    async def node_status(request: Request) -> dict[str, Any]:
        state: NodeState = request.app.state.node
        lag = None
        if state.role == NodeRole.FOLLOWER:
            lag = max(0, state.primary_seq - state.engine.last_applied_seq)

        return {
            "node_id": state.node_id,
            "shard_id": state.shard_id,
            "role": state.role.value,
            "term": state.term,
            "ready": state.ready,
            "last_applied_seq": state.engine.last_applied_seq,
            "doc_count": state.engine.doc_count(),
            "lease_remaining_s": round(state.remaining_lease(), 3),
            "follower_state": state.follower_state.value,
            "follower_acked_seq": state.follower_acked_seq,
            "replication_lag": lag,
            "ranges": [
                {"start": r.start, "end": r.end, "epoch": r.ownership_epoch} for r in state.ranges
            ],
        }

    # ── Internal Replication & Catchup Endpoints ──────────────────────────────

    @app.post("/internal/replicate", dependencies=[Depends(cluster_auth)])
    async def replicate(body: ReplicationRequest, request: Request) -> ReplicationResponse:
        state = get_node(request)
        if state.role == NodeRole.PRIMARY:
            raise HTTPException(
                status_code=409,
                detail={
                    "error_code": "NOT_FOLLOWER",
                    "error_message": "A primary never accepts replication traffic.",
                },
            )
        if body.term < state.term:
            raise StaleTerm(
                f"Replication term {body.term} < node term {state.term}"
            ).to_http_exception()
        if body.term > state.term:
            state.term = body.term

        assert state.wal is not None
        expected_seq = state.wal.next_seq()

        if body.seq == expected_seq:
            # 1. Append to follower WAL and fsync
            state.wal.append(
                seq=body.seq,
                term=body.term,
                op=body.op.value,
                doc_id=body.doc_id,
                request_id=body.request_id,
                payload=body.payload,
            )
            # Test hook 3: after follower fsync before ack
            trigger_hook(
                "after_follower_fsync_before_ack",
                state.settings.ENABLE_TEST_HOOKS,
                request_id=body.request_id,
                seq=body.seq,
                doc_id=body.doc_id,
            )
            # 2. Apply via engine.apply() — idempotent; safe after crash-restart (C3)
            repl_entry = {
                "seq": body.seq,
                "term": body.term,
                "op": body.op.value,
                "doc_id": body.doc_id,
                "request_id": body.request_id,
                "payload": body.payload,
            }
            state.engine.apply(repl_entry)
            state.primary_seq = body.seq
            state.committed_seq = max(state.committed_seq, body.seq)
            _persist_node_metadata(state)
            state.engine.dedup_store(
                body.request_id,
                DedupEntry(
                    seq=body.seq,
                    outcome=WriteOutcome.COMMITTED,
                    result={"outcome": WriteOutcome.COMMITTED, "seq": body.seq},
                    term=body.term,
                ),
            )
            return ReplicationResponse(status="ok", seq=body.seq, last_seq=body.seq)

        if body.seq <= (state.wal.last_seq if state.wal else 0):
            # Duplicate delivery: only acknowledge if the stored entry is IDENTICAL.
            stored = None
            if state.wal:
                for w in state.wal.replay(after_seq=max(0, body.seq - 1)):
                    if w.get("seq") == body.seq:
                        stored = w
                    break
            incoming = {
                "term": body.term,
                "seq": body.seq,
                "op": body.op.value,
                "doc_id": body.doc_id,
                "request_id": body.request_id,
                "payload": body.payload,
            }
            if stored is not None and compute_entry_identity(stored) == compute_entry_identity(
                incoming
            ):
                return ReplicationResponse(
                    status="ok", seq=body.seq, last_seq=state.wal.last_seq if state.wal else 0
                )
            return ReplicationResponse(
                status="conflict",
                seq=body.seq,
                last_seq=state.wal.last_seq if state.wal else 0,
                error_message="Entry at this seq differs from the stored entry.",
            )

        # Gap detected
        return ReplicationResponse(
            status="gap",
            seq=body.seq,
            last_seq=state.wal.last_seq if state.wal else 0,
            error_message=f"Gap detected: expected {expected_seq}, got {body.seq}",
        )

    @app.post("/internal/catchup", dependencies=[Depends(cluster_auth)])
    async def catchup(body: CatchupRequest, request: Request) -> CatchupResponse:
        state = get_node(request)
        assert state.wal is not None

        earliest = state.wal.earliest_seq()
        if earliest is not None and earliest > body.after_seq + 1:
            return CatchupResponse(
                entries=[],
                needs_snapshot=True,
                last_seq=state.wal.last_seq,
            )

        entries = list(state.wal.replay(after_seq=body.after_seq))
        return CatchupResponse(
            entries=entries,
            needs_snapshot=False,
            last_seq=state.wal.last_seq,
        )

    @app.post("/internal/check-op", dependencies=[Depends(cluster_auth)])
    async def check_op(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        seq = body.get("seq")
        term = body.get("term")
        request_id = body.get("request_id")

        wal_last_seq = state.wal.last_seq if state.wal else 0
        last_applied_seq = state.engine.last_applied_seq

        if seq is None or term is None or not request_id:
            return {
                "status": "invalid_identity",
                "has_op": False,
                "term": state.term,
                "wal_last_seq": wal_last_seq,
                "committed_seq": state.committed_seq,
                "last_applied_seq": last_applied_seq,
                "op_term": None,
                "op_request_id": None,
                "entry_found": False,
            }

        req_entry_id = body.get("entry_id")
        has_op = False
        op_term = None
        op_request_id = None
        op_entry_id = None
        entry_found = False

        # 1. Search WAL for entry at this seq
        if state.wal:
            for w in state.wal.replay(after_seq=max(0, seq - 1)):
                if w.get("seq") == seq:
                    entry_found = True
                    op_term = w.get("term")
                    op_request_id = w.get("request_id")
                    op_entry_id = w.get("entry_id") or compute_entry_identity(w)
                    if op_term == term and op_request_id == request_id:
                        if req_entry_id is None or req_entry_id == op_entry_id:
                            has_op = True
                    break
                if w.get("seq", 0) > seq:
                    break

        # 2. Check dedup table if not matched in WAL
        if not has_op:
            dedup_entry = state.engine.dedup_check(request_id)
            if (
                dedup_entry
                and dedup_entry.outcome == WriteOutcome.COMMITTED
                and dedup_entry.seq == seq
                and dedup_entry.term == term
            ):
                has_op = True
                entry_found = True
                op_term = dedup_entry.term
                op_request_id = request_id

        return {
            "status": "ok",
            "has_op": has_op,
            "term": state.term,
            "wal_last_seq": wal_last_seq,
            "committed_seq": state.committed_seq,
            "last_applied_seq": last_applied_seq,
            "op_term": op_term,
            "op_request_id": op_request_id,
            "op_entry_id": op_entry_id,
            "entry_found": entry_found,
        }

    @app.get("/internal/snapshot", dependencies=[Depends(cluster_auth)])
    async def get_snapshot(request: Request) -> dict[str, Any]:
        state = get_node(request)
        return {
            "documents": state.engine.all_docs(),
            "last_applied_seq": state.engine.last_applied_seq,
            "term": state.term,
            "ownership_epochs": {f"{r.start}-{r.end}": r.ownership_epoch for r in state.ranges},
            "dedup_table": state.engine.dedup_export(),
        }

    @app.post("/internal/snapshot/apply", dependencies=[Depends(cluster_auth)])
    async def apply_snapshot(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        async with state.write_lock():
            snap_seq = int(body.get("last_applied_seq", 0))
            if snap_seq < state.committed_seq:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Cannot apply snapshot with last_applied_seq ({snap_seq}) < "
                        f"local committed_seq ({state.committed_seq})"
                    ),
                )
            snap_term = int(body.get("term", state.term))
            if snap_term < state.term:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Cannot apply snapshot with term ({snap_term}) < local term ({state.term})"
                    ),
                )

            reconciled_ranges = state.ranges
            snap_epochs = body.get("ownership_epochs")
            if snap_epochs is not None:
                try:
                    reconciled_ranges = _reconcile_ownership_epochs(state.ranges, snap_epochs)
                except Exception as exc:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Incompatible snapshot ownership epochs: {exc}",
                    ) from exc

                for r in state.ranges:
                    k = f"{r.start}-{r.end}"
                    if k in snap_epochs and snap_epochs[k] < r.ownership_epoch:
                        raise HTTPException(
                            status_code=409,
                            detail=(
                                f"Snapshot epoch ({snap_epochs[k]}) < local epoch "
                                f"({r.ownership_epoch}) for range {k}"
                            ),
                        )

            # C2: all_docs() returns dict[int, dict]; iterating the dict yields keys,
            # not documents. Handle both the dict format (current) and a list format.
            docs = body.get("documents", {})
            parsed_docs: dict[int, dict[str, Any]] = {}
            if isinstance(docs, dict):
                for doc_id_str, doc in docs.items():
                    parsed_docs[int(doc_id_str)] = doc
            else:
                for doc in docs:
                    parsed_docs[int(doc["doc_id"])] = doc

            target_term = max(state.term, snap_term)
            target_epochs = {f"{r.start}-{r.end}": r.ownership_epoch for r in reconciled_ranges}
            dedup_table = body.get("dedup_table", {})

            # Durably persist snapshot BEFORE modifying in-memory state or truncating WAL!
            if state.snapshot_mgr:
                try:
                    state.snapshot_mgr.save(
                        documents=parsed_docs,
                        last_applied_seq=snap_seq,
                        term=target_term,
                        ownership_epochs=target_epochs,
                        dedup_table=dedup_table,
                    )
                except Exception as exc:
                    logger.error("Failed to durably persist applied snapshot: %s", exc)
                    raise HTTPException(
                        status_code=500,
                        detail=f"Failed to durably persist snapshot: {exc}",
                    ) from exc

            state.engine._docs.clear()
            state.engine._index.clear()
            for doc_id, doc in parsed_docs.items():
                state.engine._docs[doc_id] = doc
                state.engine._index[doc_id] = None

            state.engine.last_applied_seq = snap_seq
            state.primary_seq = snap_seq
            state.term = target_term
            state.ranges = reconciled_ranges
            state.engine.dedup_import(dedup_table)
            state.committed_seq = max(state.committed_seq, snap_seq)
            _persist_node_metadata(state)

            if state.wal is not None:
                state.wal.truncate_after(state.engine.last_applied_seq)

            return {"status": "ok", "applied_seq": state.engine.last_applied_seq}

    @app.post("/internal/fence", dependencies=[Depends(cluster_auth)])
    async def fence(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        new_term = body.get("term")
        # M6: Only accept exactly current_term+1 to prevent a misbehaving peer
        # from jumping the term to max-int and permanently blocking replication.
        if new_term is not None:
            if new_term == state.term + 1:
                state.term = new_term
                if state.role == NodeRole.PRIMARY:
                    state.role = NodeRole.FOLLOWER
                    state.lease_expires_at = 0.0
            elif new_term > state.term + 1:
                logger.warning(
                    "Fence rejected: term jump %d -> %d exceeds +1; ignoring.",
                    state.term,
                    new_term,
                )
        return {"status": "ok", "term": state.term}

    @app.post("/internal/resync", dependencies=[Depends(cluster_auth)])
    async def resync(body: dict[str, Any], request: Request) -> dict[str, Any]:
        """Follower: reconcile with the given primary and report back (rejoin/complete)."""
        state = get_node(request)
        if state.role != NodeRole.FOLLOWER:
            raise HTTPException(
                status_code=409,
                detail={"error_code": "NOT_FOLLOWER", "error_message": "Node is not a follower."},
            )
        primary_url = body.get("primary_url")
        if not isinstance(primary_url, str) or not primary_url:
            raise HTTPException(
                status_code=400,
                detail={"error_code": "VALIDATION_ERROR", "error_message": "primary_url required"},
            )
        ok = await resync_with_primary(state, primary_url)
        return {"status": "ok" if ok else "failed"}

    @app.get("/internal/status", dependencies=[Depends(cluster_auth)])
    async def internal_status(request: Request) -> dict[str, Any]:
        state = get_node(request)
        return {
            "node_id": state.node_id,
            "shard_id": state.shard_id,
            "role": state.role.value,
            "term": state.term,
            "last_applied_seq": state.engine.last_applied_seq,
            "doc_count": state.engine.doc_count(),
        }

    @app.post("/internal/promote", dependencies=[Depends(cluster_auth)])
    async def promote(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        async with state.write_lock():
            new_term = body.get("term", state.term + 1)
            if not isinstance(new_term, int) or new_term < state.term:
                raise StaleTerm(
                    f"Promotion term {new_term!r} is behind node term {state.term}."
                ).to_http_exception()
            state.term = new_term
            state.role = NodeRole.PRIMARY

            epochs = body.get("ownership_epochs", {})
            for r in state.ranges:
                key = f"{r.start}-{r.end}"
                if key in epochs:
                    r.ownership_epoch = epochs[key]
                else:
                    r.ownership_epoch += 1

            lease = body.get("lease")
            if lease:
                duration = lease.get("expires_at", 0) - lease.get("granted_at", 0)
                state.lease_expires_at = time.monotonic() + (
                    duration if duration > 0 else state.settings.LEASE_DURATION_S
                )
            else:
                state.lease_expires_at = time.monotonic() + state.settings.LEASE_DURATION_S

            state.follower_url = body.get("follower_url") or state.follower_url
            state.follower_state = (
                ReplicaState.DOWN
            )  # Degraded mode until rejoining follower is SYNCED

            # Reconcile any pending unresolved writes with follower if reachable
            if state.pending_unresolved_writes and state.follower_url:
                for p_rid in list(state.pending_unresolved_writes.keys()):
                    try:
                        await _reconcile_pending_write(state, p_rid)
                    except Exception as exc:
                        logger.debug("Promotion reconciliation for %s deferred: %s", p_rid, exc)

            # Establish correct recovery point using durable commitment metadata and engine state
            data_dir = Path(state.settings.DATA_DIR) / state.shard_id
            meta_path = data_dir / "node_metadata.json"
            if meta_path.exists():
                try:
                    with open(meta_path, encoding="utf-8") as f:
                        disk_meta = json.load(f)
                        state.committed_seq = max(
                            state.committed_seq, disk_meta.get("committed_seq", 0)
                        )
                except Exception as exc:
                    logger.debug("Failed to read node_metadata on promote: %s", exc)
            state.committed_seq = max(state.committed_seq, state.engine.last_applied_seq)

            # Truncate uncommitted tails ONLY if no unresolved writes remain awaiting reconciliation
            if state.wal and not state.pending_unresolved_writes:
                if state.wal.last_seq > state.committed_seq:
                    logger.warning(
                        "Promotion recovery for node %s: discarding uncommitted tail "
                        "seq %d..%d (committed_seq=%d)",
                        state.node_id,
                        state.committed_seq + 1,
                        state.wal.last_seq,
                        state.committed_seq,
                    )
                    state.wal.truncate_after(state.committed_seq)

                for entry in state.wal.replay(after_seq=state.engine.last_applied_seq):
                    if entry["seq"] <= state.committed_seq:
                        state.engine.apply(entry)
                        rid = entry.get("request_id")
                        if rid:
                            state.engine.dedup_store(
                                rid,
                                DedupEntry(
                                    seq=entry["seq"],
                                    outcome=WriteOutcome.COMMITTED,
                                    result={"outcome": WriteOutcome.COMMITTED, "seq": entry["seq"]},
                                    term=entry.get("term", state.term),
                                ),
                            )

            _persist_node_metadata(state)

            logger.info(
                "Promoted node %s to PRIMARY for shard %s in term %d (degraded mode)",
                state.node_id,
                state.shard_id,
                state.term,
            )
            return {
                "status": "ok",
                "node_id": state.node_id,
                "role": "primary",
                "term": state.term,
                "last_applied_seq": state.engine.last_applied_seq,
            }

    @app.post("/internal/divergence", dependencies=[Depends(cluster_auth)])
    async def get_divergence_point(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        assert state.wal is not None

        entries = body.get("entries", [])
        earliest = state.wal.earliest_seq() or 1
        last_seq = state.wal.last_seq

        # Map local primary entries by seq
        primary_entries = list(state.wal.replay(after_seq=0))
        primary_by_seq = {int(e["seq"]): e for e in primary_entries}

        candidate_divergence = 0
        first_conflict_seq: int | None = None
        needs_snapshot = False

        sorted_entries = sorted(entries, key=lambda x: int(x.get("seq", 0)))

        # If follower's earliest entry is older than primary's earliest available entry
        if sorted_entries and int(sorted_entries[0].get("seq", 0)) < earliest:
            # Primary has pruned/compacted its WAL before earliest_seq; cannot verify from here
            needs_snapshot = True
        elif not sorted_entries:
            candidate_divergence = 0
        else:
            expected_start = (
                earliest if (earliest in primary_by_seq and 1 not in primary_by_seq) else 1
            )
            if (
                int(sorted_entries[0].get("seq", 0)) > expected_start
                and expected_start in primary_by_seq
            ):
                first_conflict_seq = int(sorted_entries[0].get("seq", 0))

            if first_conflict_seq is None:
                for item in sorted_entries:
                    s = int(item.get("seq", 0))
                    if s <= 0:
                        first_conflict_seq = 1
                        break

                    # Contiguity check: each matching entry must follow the previous matching seq
                    if candidate_divergence == 0:
                        if s != expected_start and expected_start in primary_by_seq:
                            first_conflict_seq = s
                            break
                    else:
                        if s != candidate_divergence + 1:
                            # Contiguity broken in follower log
                            first_conflict_seq = s
                            break

                    # Primary log reached its end before follower log
                    if s not in primary_by_seq:
                        break

                    p_entry = primary_by_seq[s]

                    # 1. Primary entry validation (never blindly trust stored entry_id)
                    if not has_required_identity_fields(p_entry):
                        logger.error(
                            "Primary WAL entry at seq=%d lacks required identity fields", s
                        )
                        first_conflict_seq = s
                        break

                    try:
                        recomputed_p_identity = compute_entry_identity(p_entry)
                    except Exception:
                        first_conflict_seq = s
                        break

                    stored_p_id = p_entry.get("entry_id")
                    if stored_p_id is not None and stored_p_id != recomputed_p_identity:
                        logger.error(
                            "Primary WAL entry at seq=%d corrupted: stored entry_id != "
                            "recomputed identity",
                            s,
                        )
                        first_conflict_seq = s
                        break
                    p_identity = recomputed_p_identity

                    # 2. Follower entry validation:
                    # Do not accept an entry that contains only entry_id but lacks
                    # fields needed to verify it
                    if not has_required_identity_fields(item):
                        first_conflict_seq = s
                        break

                    try:
                        recomputed_f_identity = compute_entry_identity(item)
                    except Exception:
                        first_conflict_seq = s
                        break

                    # If entry_id is supplied, compare with recomputed identity
                    supplied_f_id = item.get("entry_id")
                    if supplied_f_id is not None and supplied_f_id != recomputed_f_identity:
                        first_conflict_seq = s
                        break
                    f_identity = recomputed_f_identity

                    # 3. Term comparison
                    item_term = int(item.get("term", 0))
                    p_term = int(p_entry.get("term", 0))
                    if item_term != p_term:
                        first_conflict_seq = s
                        break

                    # 4. Request ID comparison
                    f_req = str(item.get("request_id") or "")
                    p_req = str(p_entry.get("request_id") or "")
                    if f_req != p_req:
                        first_conflict_seq = s
                        break

                    # 5. Complete entry identity comparison
                    if f_identity != p_identity:
                        first_conflict_seq = s
                        break

                    candidate_divergence = s

        return {
            "status": "ok",
            "divergence_seq": candidate_divergence,
            "last_seq": last_seq,
            "term": state.term,
            "ownership_epochs": {f"{r.start}-{r.end}": r.ownership_epoch for r in state.ranges},
            "needs_snapshot": needs_snapshot,
            "first_conflict_seq": first_conflict_seq,
        }

    @app.post("/internal/rejoin/complete", dependencies=[Depends(cluster_auth)])
    async def rejoin_complete(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        async with state.write_lock():
            if state.role != NodeRole.PRIMARY:
                raise HTTPException(
                    status_code=400,
                    detail=f"Node {state.node_id} is not primary for shard {state.shard_id}",
                )

            # 1. Term check: follower must be on the primary's term
            if "term" in body:
                f_term = int(body["term"])
                if f_term != state.term:
                    logger.warning(
                        "Follower rejoin rejected: term mismatch (%d != primary term %d)",
                        f_term,
                        state.term,
                    )
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"Term mismatch: follower term {f_term} != primary term {state.term}"
                        ),
                    )

            # 2. Sequence check: follower must have caught up to at least primary's committed_seq
            follower_seq = body.get("last_applied_seq", state.wal.last_seq if state.wal else 0)
            if follower_seq < state.committed_seq:
                logger.warning(
                    "Follower rejoin rejected: sequence lag (follower_seq=%d < primary "
                    "committed_seq=%d)",
                    follower_seq,
                    state.committed_seq,
                )
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Follower has not caught up: seq {follower_seq} < "
                        f"committed_seq {state.committed_seq}"
                    ),
                )

            # 3. Ownership epoch & range mapping validation
            f_epochs = body.get("ownership_epochs")
            if f_epochs is not None and isinstance(f_epochs, dict):
                p_epochs = {f"{r.start}-{r.end}": r.ownership_epoch for r in state.ranges}
                if set(f_epochs.keys()) != set(p_epochs.keys()):
                    logger.warning(
                        "Follower rejoin rejected: incompatible range mapping: %s vs %s",
                        f_epochs,
                        p_epochs,
                    )
                    raise HTTPException(
                        status_code=400,
                        detail="Incompatible range mapping between primary and follower",
                    )
                for rk, p_ep in p_epochs.items():
                    f_ep = f_epochs.get(rk)
                    if f_ep is not None and f_ep < p_ep:
                        logger.warning(
                            "Follower rejoin rejected: stale epoch for range %s (%d < %d)",
                            rk,
                            f_ep,
                            p_ep,
                        )
                        raise HTTPException(
                            status_code=409,
                            detail=f"Follower has stale ownership epoch for {rk}: {f_ep} < {p_ep}",
                        )

            follower_url = body.get("follower_url")
            if follower_url:
                state.follower_url = follower_url
            state.follower_state = ReplicaState.SYNCED
            state.follower_acked_seq = max(state.follower_acked_seq, follower_seq)
            logger.info(
                "Follower rejoin complete for shard %s: follower_url=%s, state=SYNCED",
                state.shard_id,
                state.follower_url,
            )
            return {"status": "ok", "follower_state": "SYNCED"}

    # ── Live Range Migration Internal Endpoints (Module 9) ────────────────────

    @app.post("/internal/migration/snapshot", dependencies=[Depends(cluster_auth)])
    async def migration_snapshot(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        start = body["start"]
        end = body["end"]
        async with state.write_lock():
            # Resolve all pending writes before migration snapshot
            if state.pending_unresolved_writes:
                for req_id in list(state.pending_unresolved_writes.keys()):
                    await _reconcile_pending_write(state, req_id)
            if state.pending_unresolved_writes:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error_code": "REPLICATION_UNAVAILABLE",
                        "error_message": (
                            "Unresolved pending writes exist; cannot start migration snapshot."
                        ),
                    },
                )
            s_seq = state.engine.last_applied_seq
            docs = state.engine.range_snapshot(start, end)
            return {
                "status": "ok",
                "snapshot_seq": s_seq,
                "documents": docs,
            }

    @app.post("/internal/migration/oplog", dependencies=[Depends(cluster_auth)])
    async def migration_oplog(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        start = body["start"]
        end = body["end"]
        after_seq = body.get("after_seq", 0)
        limit = body.get("limit", 1000)

        assert state.wal is not None
        earliest = state.wal.earliest_seq()
        if earliest is not None and earliest > after_seq + 1:
            return {
                "status": "compacted",
                "needs_snapshot": True,
                "earliest_seq": earliest,
                "entries": [],
                "last_seq": state.wal.last_seq,
                "error_message": (
                    f"Requested cursor after_seq={after_seq} is older than earliest "
                    f"retained seq={earliest}; snapshot required."
                ),
            }

        matched_entries = []
        for entry in state.wal.replay(after_seq=after_seq):
            doc_id = entry.get("doc_id")
            if doc_id is not None and start <= doc_id < end:
                matched_entries.append(entry)
                if len(matched_entries) >= limit:
                    break

        return {
            "status": "ok",
            "entries": matched_entries,
            "last_seq": state.wal.last_seq,
        }

    @app.post("/internal/migration/apply-batch", dependencies=[Depends(cluster_auth)])
    async def migration_apply_batch(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        docs = body.get("documents", [])
        mig_id = body.get("migration_id", "mig")

        assert state.wal is not None
        async with state.write_lock():
            try:
                for doc in docs:
                    doc_id = doc["doc_id"]
                    req_id = f"mig-{mig_id}-{doc_id}"
                    seq = state.wal.next_seq()
                    wal_entry = state.wal.append(
                        seq=seq,
                        op=WriteOp.INSERT.value,
                        doc_id=doc_id,
                        term=state.term,
                        request_id=req_id,
                        payload=doc,
                    )
                    if state.follower_url:
                        ok = await _replicate_to_follower(
                            state, seq, WriteOp.INSERT, doc_id, req_id, doc
                        )
                        if not ok:
                            raise HTTPException(
                                status_code=503,
                                detail={
                                    "error_code": "REPLICATION_UNAVAILABLE",
                                    "error_message": "Follower replication failed during batch",
                                },
                            )
                        state.follower_acked_seq = max(state.follower_acked_seq, seq)

                    state.engine.apply(wal_entry, migration_mode=True)
                    state.committed_seq = max(state.committed_seq, seq)
            finally:
                _persist_node_metadata(state)
            return {"status": "ok", "count": len(docs), "last_seq": state.wal.last_seq}

    @app.post("/internal/migration/apply-oplog", dependencies=[Depends(cluster_auth)])
    async def migration_apply_oplog(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        entries = body.get("entries", [])
        mig_id = body.get("migration_id", "mig")

        assert state.wal is not None
        async with state.write_lock():
            try:
                for entry in entries:
                    doc_id = entry["doc_id"]
                    op = WriteOp(entry["op"])
                    payload = entry.get("payload")
                    req_id = f"catchup-{mig_id}-{doc_id}-{entry['seq']}"

                    seq = state.wal.next_seq()
                    wal_entry = state.wal.append(
                        seq=seq,
                        op=op.value,
                        doc_id=doc_id,
                        term=state.term,
                        request_id=req_id,
                        payload=payload,
                    )
                    if state.follower_url:
                        ok = await _replicate_to_follower(state, seq, op, doc_id, req_id, payload)
                        if not ok:
                            raise HTTPException(
                                status_code=503,
                                detail={
                                    "error_code": "REPLICATION_UNAVAILABLE",
                                    "error_message": "Follower replication failed during catchup",
                                },
                            )
                        state.follower_acked_seq = max(state.follower_acked_seq, seq)

                    state.engine.apply(wal_entry, migration_mode=True)
                    state.committed_seq = max(state.committed_seq, seq)
            finally:
                _persist_node_metadata(state)
            return {"status": "ok", "replayed": len(entries), "last_seq": state.wal.last_seq}

    @app.post("/internal/migration/pause", dependencies=[Depends(cluster_auth)])
    async def migration_pause(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        start = body["start"]
        end = body["end"]
        async with state.write_lock():
            if (start, end) not in state.paused_ranges:
                state.paused_ranges.append((start, end))
            final_seq = state.wal.last_seq if state.wal else 0
            logger.info(
                "Paused range [%d, %d) on shard %s at seq %d",
                start,
                end,
                state.shard_id,
                final_seq,
            )
            return {"status": "paused", "final_seq": final_seq}

    @app.post("/internal/migration/unpause", dependencies=[Depends(cluster_auth)])
    async def migration_unpause(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        start = body["start"]
        end = body["end"]
        async with state.write_lock():
            state.paused_ranges = [
                r for r in state.paused_ranges if not (r[0] == start and r[1] == end)
            ]
            logger.info("Unpaused range [%d, %d) on shard %s", start, end, state.shard_id)
            return {"status": "ok"}

    @app.post("/internal/migration/checksum", dependencies=[Depends(cluster_auth)])
    async def migration_checksum(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        start = body["start"]
        end = body["end"]
        async with state.write_lock():
            docs = state.engine.range_snapshot(start, end)
            from docstore.coordinator.migration import compute_deterministic_checksum

            count, chk = compute_deterministic_checksum(docs)
            return {"status": "ok", "doc_count": count, "checksum": chk}

    @app.post("/internal/migration/cutover", dependencies=[Depends(cluster_auth)])
    async def migration_cutover(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        start = body["start"]
        end = body["end"]
        epoch = body["ownership_epoch"]
        async with state.write_lock():
            existing = next((r for r in state.ranges if r.start == start and r.end == end), None)
            if existing:
                existing.ownership_epoch = epoch
            else:
                state.ranges.append(ShardRangeConfig(start=start, end=end, ownership_epoch=epoch))
                state.ranges.sort(key=lambda r: r.start)
            _persist_node_metadata(state)
            logger.info(
                "Cutover range [%d, %d) epoch %d to shard %s (node %s)",
                start,
                end,
                epoch,
                state.shard_id,
                state.node_id,
            )
            return {"status": "ok", "ranges": [r.model_dump() for r in state.ranges]}

    async def _purge_range_docs(state: NodeState, start: int, end: int, prefix: str) -> int:
        """Delete every doc in [start, end) through the WAL, replicated and committed."""
        assert state.wal is not None
        docs = state.engine.range_snapshot(start, end)
        try:
            for d in docs:
                doc_id = d["doc_id"]
                seq = state.wal.next_seq()
                req_id = f"{prefix}-{doc_id}"
                entry = state.wal.append(
                    seq=seq,
                    op=WriteOp.DELETE.value,
                    doc_id=doc_id,
                    term=state.term,
                    request_id=req_id,
                    payload=None,
                )
                if state.follower_url:
                    ok = await _replicate_to_follower(
                        state, seq, WriteOp.DELETE, doc_id, req_id, None
                    )
                    if not ok:
                        raise HTTPException(
                            status_code=503,
                            detail={
                                "error_code": "REPLICATION_UNAVAILABLE",
                                "error_message": "Follower replication failed during purge",
                            },
                        )
                    state.follower_acked_seq = max(state.follower_acked_seq, seq)
                state.engine.apply(entry, migration_mode=True)
                state.committed_seq = max(state.committed_seq, seq)
        finally:
            _persist_node_metadata(state)
        return len(docs)

    async def _relay_to_follower(state: NodeState, path: str, body: dict[str, Any]) -> bool:
        if not state.follower_url:
            return True
        client = state.http_client or httpx.AsyncClient(
            timeout=state.settings.REPLICATION_TIMEOUT_S
        )
        try:
            clean = state.follower_url.removeprefix("http://").removeprefix("https://").rstrip("/")
            raw = json.dumps(body, separators=(",", ":")).encode("utf-8")
            sig = compute_signature(state.settings.CLUSTER_SECRET, raw)
            resp = await client.post(
                f"http://{clean}{path}",
                content=raw,
                headers={"Content-Type": "application/json", CLUSTER_SIG_HEADER: sig},
                timeout=state.settings.REPLICATION_TIMEOUT_S,
            )
            return resp.status_code == 200
        except Exception as exc:
            logger.warning("Relay %s to follower failed: %s", path, exc)
            return False
        finally:
            if state.http_client is None:
                await client.aclose()

    @app.post("/internal/migration/cleanup", dependencies=[Depends(cluster_auth)])
    async def migration_cleanup(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        start = body["start"]
        end = body["end"]
        async with state.write_lock():
            # 1. Stop owning the range (works for prefix / suffix / interior / whole ranges)
            state.ranges = _subtract_range(state.ranges, start, end)
            _persist_node_metadata(state)
            # 2. Delete docs through the WAL so the follower and restarts see it
            deleted = await _purge_range_docs(state, start, end, "cleanup")
            # 3. Only now unpause, so no write can slip in while docs are being removed
            state.paused_ranges = [
                r for r in state.paused_ranges if not (r[0] == start and r[1] == end)
            ]
            # 4. Tell the follower to drop the range as well (docs already replicated)
            relayed = await _relay_to_follower(
                state, "/internal/migration/ranges/remove", {"start": start, "end": end}
            )
            if not relayed:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error_code": "REPLICATION_UNAVAILABLE",
                        "error_message": "Follower did not ack range removal; retry cleanup.",
                    },
                )
            logger.info(
                "Cleaned up range [%d, %d) on %s (%d docs)", start, end, state.shard_id, deleted
            )
            return {"status": "ok", "deleted_docs": deleted}

    @app.post("/internal/migration/ranges/remove", dependencies=[Depends(cluster_auth)])
    async def migration_ranges_remove(body: dict[str, Any], request: Request) -> dict[str, Any]:
        """Follower side of cleanup: drop ownership only (docs arrive via replicated DELETEs)."""
        state = get_node(request)
        start = body["start"]
        end = body["end"]
        async with state.write_lock():
            state.ranges = _subtract_range(state.ranges, start, end)
            state.paused_ranges = [
                r for r in state.paused_ranges if not (r[0] == start and r[1] == end)
            ]
            _persist_node_metadata(state)
            return {"status": "ok", "ranges": [r.model_dump() for r in state.ranges]}

    @app.post("/internal/migration/abort", dependencies=[Depends(cluster_auth)])
    async def migration_abort(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        start = body["start"]
        end = body["end"]
        async with state.write_lock():
            # Only purge staged docs that are NOT in a range this shard owns.
            if any(r.start < end and start < r.end for r in state.ranges):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error_code": "RANGE_OWNED",
                        "error_message": "Refusing to purge a range this shard owns.",
                    },
                )
            purged = await _purge_range_docs(state, start, end, "abort")
            logger.info("Aborted migration on %s; purged %d staged docs", state.shard_id, purged)
            return {"status": "aborted", "purged_docs": purged}

    # ── Error handler ─────────────────────────────────────────────────────────

    @app.exception_handler(HTTPException)
    async def _http_exc_handler(request: Request, exc: HTTPException) -> JSONResponse:
        if isinstance(exc.detail, dict):
            detail = exc.detail
        else:
            detail = {"error_code": "ERROR", "error_message": str(exc.detail)}
        return JSONResponse(status_code=exc.status_code, content=detail)

    return app


def create_default_app() -> FastAPI:
    """Factory for uvicorn (--factory) using settings from environment."""
    from docstore.common.config import get_settings

    settings = get_settings()
    node_id = os.environ.get("NODE_ID", "shard-node-default")
    shard_id = os.environ.get("SHARD_ID", "shard0")
    return build_app(settings, node_id=node_id, shard_id=shard_id)
