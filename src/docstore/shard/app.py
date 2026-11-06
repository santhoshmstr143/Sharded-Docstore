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
import itertools
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
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

from docstore.common.auth import (
    CLUSTER_SIG_HEADER,
    make_client_auth_dependency,
    make_cluster_auth_dependency,
    sign_request,
)
from docstore.common.config import Settings
from docstore.common.errors import (
    DocstoreError,
    DocTooLarge,
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
    StaleEpoch,
    StaleTerm,
)
from docstore.common.http import BodyLimitMiddleware
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
    safe_validation_message,
    serialized_doc_size,
)
from docstore.common.test_hooks import trigger_hook
from docstore.query import (
    detect_shard_key_constraint,
    evaluate_filter,
    sort_documents,
    validate_query_filter,
)
from docstore.storage.compaction import compact_wal
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


class _FencedWrite(BaseModel):
    """Optional fencing tokens attached by the router (ownership_epoch) or client (term).

    The primary rejects a write whose ownership_epoch is older than the epoch it holds
    for the range (stale routing) or whose term differs from its own (stale leader).
    """

    ownership_epoch: StrictInt | None = Field(default=None, ge=0)
    term: StrictInt | None = Field(default=None, ge=0)


class InsertBody(_FencedWrite):
    request_id: str = Field(min_length=1, max_length=128)
    document: dict[str, Any] = Field(description="Must contain doc_id.")


class UpdateBody(_FencedWrite):
    request_id: str = Field(min_length=1, max_length=128)
    document: dict[str, Any] = Field(description="Full replacement document.")


class DeleteBody(_FencedWrite):
    request_id: str = Field(min_length=1, max_length=128)


_MAX_QUERY_RANGES = 64
_KEYSPACE_END = 2**63


def _merge_ranges(ranges: Sequence[tuple[int, int]]) -> list[tuple[int, int]]:
    """Sort and coalesce overlapping / adjacent half-open ranges."""
    merged: list[tuple[int, int]] = []
    for s, e in sorted(ranges):
        if merged and s <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))
    return merged


class ShardQueryBody(QueryFilter):
    """Shard-side query: the router may restrict evaluation to the ranges it
    attributes to this shard, so pagination happens *after* the range restriction.

    The list is bounded and validated: duplicate / overlapping entries used to be
    scanned once each, letting one small request multiply the scan cost unboundedly.
    """

    ranges: list[tuple[StrictInt, StrictInt]] | None = Field(
        default=None, max_length=_MAX_QUERY_RANGES
    )

    @field_validator("ranges")
    @classmethod
    def _valid_ranges(cls, v: list[tuple[int, int]] | None) -> list[tuple[int, int]] | None:
        if v is None:
            return None
        for s, e in v:
            if not (0 <= s < e <= _KEYSPACE_END):
                raise ValueError(f"invalid range [{s}, {e})")
        return _merge_ranges(v)


# ── Node state ────────────────────────────────────────────────────────────────


class ShardRangeConfig(BaseModel):
    """One owned range with its epoch."""

    start: int = Field(ge=DOC_ID_MIN)
    end: int = Field(ge=1)
    ownership_epoch: int = Field(ge=0)


class PromoteRequest(BaseModel):
    """Body of /internal/promote, validated strictly (BUG 29)."""

    model_config = ConfigDict(extra="ignore")

    shard_id: str | None = None
    term: StrictInt | None = None
    ownership_epochs: dict[str, StrictInt] | None = None
    ranges: list[ShardRangeConfig] | None = None
    lease: dict[str, Any] | None = None
    follower_url: str | None = None


# Recent WAL entries always kept after compaction (lagging follower / migration catch-up).
_WAL_RETAIN_ENTRIES = 1_000
_SNAPSHOT_POLL_S = 1.0
# A migration that has not touched its WAL hold for this long is considered abandoned
# (the coordinator aborts pre-cutover migrations on restart); its hold stops pinning.
_MIGRATION_HOLD_TTL_S = 300.0


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
        # (start, end) -> (oplog cursor seq, last-touched monotonic): WAL entries after the
        # cursor must survive compaction until the migration's catch-up has read them.
        self.migration_holds: dict[tuple[int, int], tuple[int, float]] = {}

    def hold_migration(self, start: int, end: int, seq: int, *, reset: bool = False) -> None:
        prev = self.migration_holds.get((start, end))
        keep = seq if reset or prev is None else max(prev[0], seq)
        self.migration_holds[(start, end)] = (keep, time.monotonic())

    def release_migration(self, start: int, end: int) -> None:
        self.migration_holds.pop((start, end), None)

    def migration_retain_seq(self) -> int | None:
        now = time.monotonic()
        live = [seq for seq, t in self.migration_holds.values() if now - t < _MIGRATION_HOLD_TTL_S]
        return min(live) if live else None

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


def _persist_node_metadata(
    state: NodeState,
    *,
    term: int | None = None,
    role: NodeRole | None = None,
    ranges: Sequence[ShardRangeConfig] | None = None,
) -> None:
    """Durably persist node term, role, and range configurations with fsync.

    The keyword overrides let callers persist a *candidate* state before activating
    it in memory, so a persistence failure never leaves the node acting on authority
    (term / role / ownership) that is not on disk.
    """
    data_dir = Path(state.settings.DATA_DIR) / state.shard_id
    data_dir.mkdir(parents=True, exist_ok=True)
    meta_path = data_dir / "node_metadata.json"
    tmp_path = data_dir / "node_metadata.json.tmp"

    data = {
        "term": state.term if term is None else term,
        "role": (state.role if role is None else role).value,
        "committed_seq": state.committed_seq,
        "ranges": [r.model_dump() for r in (state.ranges if ranges is None else ranges)],
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
    meta_has_ranges = False
    if meta_path.exists():
        try:
            with open(meta_path, encoding="utf-8") as f:
                meta = json.load(f)
                state.term = meta.get("term", state.term)
                state.committed_seq = meta.get("committed_seq", 0)
                if "ranges" in meta:
                    meta_ranges = meta["ranges"]
                    meta_has_ranges = True
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
    if meta_has_ranges and not meta_ranges:
        # Persisted metadata says this node *explicitly* owns nothing (every range was
        # migrated away). That is not "missing configuration": never fall back to the
        # environment default range, which would reclaim another shard's keys.
        if state.ranges:
            raise ValueError(
                "Incompatible range mapping between configured "
                f"{sorted((r.start, r.end) for r in state.ranges)} and metadata []"
            )
        state.ranges = []
    else:
        state.ranges = _reconcile_all_ownership_epochs(
            configured_ranges=state.ranges,
            meta_ranges=meta_ranges,
            snapshot_epochs=snap_epochs,
        )
        _configure_ranges(state)

    wal = WAL(data_dir / "shard.wal")
    wal.open()
    state.wal = wal

    if snap and snap.get("installed"):
        earliest = wal.earliest_seq()
        if earliest is not None and earliest <= after_seq:
            # Crashed after installing a snapshot from the primary but before resetting the
            # local WAL: the old entries are superseded (possibly divergent) history.
            logger.warning(
                "Recovery: discarding local WAL superseded by installed snapshot at seq %d",
                after_seq,
            )
            wal.reset(after_seq + 1)

    state.committed_seq = max(state.committed_seq, state.engine.last_applied_seq)

    # base_seq: the retained WAL must continue the snapshot without a gap.
    # Replay first: wal.last_seq is only known once the log has been read.
    wal_entries = list(wal.replay(after_seq=after_seq, base_seq=after_seq))
    if state.allow_standalone and (is_disk_recovery or snap is not None):
        # Standalone mode has no follower: every locally durable WAL entry is committed.
        # (Computing this before the replay saw last_seq == 0 and wedged the node with a
        # permanently "pending" write after a crash between WAL append and metadata write.)
        state.committed_seq = max(state.committed_seq, wal.last_seq)
    # Without metadata or a snapshot nothing is known to be committed: any WAL entries
    # stay unresolved until reconciled with a follower.
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


def _rebuild_engine_from_disk(state: NodeState) -> None:
    """Rebuild in-memory docs + dedup from the local snapshot (if any) and the WAL.

    The WAL may have been compacted, so it alone is not the full history: start from
    the snapshot and replay only the entries after it.
    """
    if state.wal is None:  # explicit: asserts are stripped under -O
        raise RuntimeError("state.wal is unavailable")
    state.engine._docs.clear()
    state.engine._index.clear()
    state.engine._doc_seq.clear()
    state.engine._dedup.clear()
    state.engine.last_applied_seq = 0
    base = 0
    snap = state.snapshot_mgr.load() if state.snapshot_mgr is not None else None
    if snap:
        for did, doc in snap["documents"].items():
            state.engine._docs[did] = doc
            state.engine._index[did] = None
        base = int(snap["last_applied_seq"])
        state.engine.last_applied_seq = base
        state.engine.dedup_import(snap.get("dedup_table", {}))
    for entry in state.wal.replay(after_seq=base):
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


_MISSING: Any = object()


def _bad_body(msg: str) -> HTTPException:
    return HTTPException(
        status_code=422, detail={"error_code": "VALIDATION_ERROR", "error_message": msg}
    )


def _strict_int(
    body: dict[str, Any],
    name: str,
    *,
    lo: int = 0,
    hi: int = 2**63,
    default: Any = _MISSING,
) -> int:
    """Read an integer field from an internal request body, strictly (no bool / str)."""
    if name not in body or body[name] is None:
        if default is _MISSING:
            raise _bad_body(f"'{name}' is required")
        return int(default)
    v = body[name]
    if isinstance(v, bool) or not isinstance(v, int) or not (lo <= v <= hi):
        raise _bad_body(f"'{name}' must be an integer in [{lo}, {hi}]")
    return v


def _body_range(body: dict[str, Any]) -> tuple[int, int]:
    """``start`` / ``end`` of an internal range request: integers, 0 <= start < end <= 2^63."""
    start = _strict_int(body, "start", hi=_KEYSPACE_END - 1)
    end = _strict_int(body, "end", lo=1, hi=_KEYSPACE_END)
    if start >= end:
        raise _bad_body(f"empty or inverted range [{start}, {end})")
    return start, end


def _migration_id(body: dict[str, Any]) -> str:
    mig = body.get("migration_id", "mig")
    if not isinstance(mig, str) or not mig or len(mig) > 128:
        raise _bad_body("'migration_id' must be a non-empty string of at most 128 chars")
    return mig


def _require_same_shard(state: NodeState, shard_id: Any) -> None:
    """Reject an internal RPC addressed to a different shard (BUG 8)."""
    if shard_id is not None and shard_id != state.shard_id:
        raise HTTPException(
            status_code=409,
            detail={
                "error_code": "WRONG_SHARD",
                "error_message": (
                    f"Request for shard {shard_id!r} reached node of shard {state.shard_id!r}."
                ),
            },
        )


async def snapshot_and_compact(state: NodeState) -> int:
    """Take a durable, verified snapshot and compact the WAL safely (BUG 12).

    Safety rules:
      * snapshot only state already applied (committed) in memory;
      * verify the snapshot reads back before deleting any WAL entry;
      * never compact during a migration write-pause, past committed_seq, or past what
        the follower has acknowledged (compact_wal's retention cap forces a snapshot
        resync for a follower that is hopelessly behind);
      * always retain the most recent ``_WAL_RETAIN_ENTRIES`` entries.

    Returns the number of WAL entries removed.
    """
    async with state.write_lock():
        wal = state.wal
        mgr = state.snapshot_mgr
        if wal is None or mgr is None or wal.failed:
            return 0
        snap_seq = state.engine.last_applied_seq
        doc_count = state.engine.doc_count()
        mgr.save(
            documents=state.engine.all_docs(),
            last_applied_seq=snap_seq,
            term=state.term,
            ownership_epochs={f"{r.start}-{r.end}": r.ownership_epoch for r in state.ranges},
            dedup_table=state.engine.dedup_export(),
        )
        loaded = mgr.load()
        if (
            not loaded
            or int(loaded.get("last_applied_seq", -1)) != snap_seq
            or len(loaded.get("documents", {})) != doc_count
        ):
            raise RuntimeError("Snapshot verification failed; WAL left uncompacted.")

        if state.paused_ranges:
            return 0  # a migration is inside its write-pause / final catch-up window

        safe_seq = min(snap_seq, max(state.committed_seq, 0)) - _WAL_RETAIN_ENTRIES
        hold = state.migration_retain_seq()
        if hold is not None:
            safe_seq = min(safe_seq, hold)  # an active migration still needs these entries
        if safe_seq <= 0:
            return 0
        if state.role == NodeRole.PRIMARY and state.follower_url:
            follower_acked = state.follower_acked_seq
        else:
            follower_acked = safe_seq

        wal.close()
        try:
            removed = compact_wal(
                wal.path,
                snapshot_seq=safe_seq,
                follower_acked_seq=follower_acked,
            )
        finally:
            wal.open()  # re-open on the (possibly replaced) file; next_seq is unchanged
        return removed


def lease_duration_from_grant(lease: dict[str, Any] | None, local_max_s: float) -> float:
    """Lease length the shard may assume from a coordinator grant (final report #8).

    ``expires_at - granted_at`` comes from the coordinator's wall clock. It is only
    trusted as an upper bound and is always capped by the locally configured lease
    duration, so a coordinator clock jump (or a misconfigured, longer coordinator
    lease) can never make this node believe it holds authority for longer than L.
    The result is then added to the node's *own* monotonic clock by the caller.
    """
    duration = 0.0
    if isinstance(lease, dict):
        try:
            duration = float(lease.get("expires_at", 0)) - float(lease.get("granted_at", 0))
        except (TypeError, ValueError):
            duration = 0.0
    if duration <= 0:
        return local_max_s
    return min(duration, local_max_s)


def _parse_snapshot_docs(raw_docs: Any) -> dict[int, dict[str, Any]]:
    """Normalise snapshot documents (``{doc_id: doc}`` or ``[doc, ...]``) to int keys.

    Single implementation shared by every snapshot-install path (final report #1).
    """
    parsed: dict[int, dict[str, Any]] = {}
    if isinstance(raw_docs, dict):
        for did, doc in raw_docs.items():
            parsed[int(did)] = doc
    elif isinstance(raw_docs, list):
        for doc in raw_docs:
            parsed[int(doc["doc_id"])] = doc
    elif raw_docs is not None:
        raise ValueError(f"Unsupported snapshot documents type: {type(raw_docs).__name__}")
    return parsed


async def _apply_registration(state: NodeState, data: dict[str, Any], t_send: float) -> None:
    """Act on the coordinator's registration decision (BUG 1).

    * Assigned FOLLOWER while acting as primary -> step down (persisted first).
    * Assigned PRIMARY with a lease -> adopt the lease, counted from send time.
    A node that recovered from disk registers as follower; the coordinator then
    promotes the current follower instead, and this node rejoins via resync.
    """
    role = data.get("role")
    if role == NodeRole.FOLLOWER.value and state.role == NodeRole.PRIMARY:
        async with state.write_lock():
            _persist_node_metadata(state, role=NodeRole.FOLLOWER)
            state.role = NodeRole.FOLLOWER
            state.lease_expires_at = 0.0
        logger.warning(
            "Node %s: coordinator registered it as FOLLOWER; stepped down", state.node_id
        )
    elif role == NodeRole.PRIMARY.value and state.role == NodeRole.PRIMARY:
        lease = data.get("lease")
        if isinstance(lease, dict) and "granted_at" in lease and "expires_at" in lease:
            state.lease_expires_at = t_send + lease_duration_from_grant(
                lease, state.settings.LEASE_DURATION_S
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
        sig = sign_request(state.settings.CLUSTER_SECRET, "POST", url, body_bytes)
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
    payload = {
        "shard_id": state.shard_id,
        "term": term,
        "seq": seq,
        "request_id": request_id,
        "entry_id": entry_id,
    }
    body_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    sig = sign_request(state.settings.CLUSTER_SECRET, "POST", url, body_bytes)
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
                    state.engine.apply(
                        apply_entry, migration_mode=bool(pending.get("migration_mode"))
                    )
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


async def _require_no_pending_writes(state: NodeState, what: str) -> None:
    """Reconcile unresolved writes before appending more log entries (caller holds the lock).

    Appending/applying a new entry while an earlier WAL entry is still unresolved
    (appended but not applied) would advance the engine past it, so a later
    "COMMITTED" reconciliation of that entry could never be applied.
    """
    for rid in list(state.pending_unresolved_writes.keys()):
        await _reconcile_pending_write(state, rid)
    if state.pending_unresolved_writes:
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "REPLICATION_UNAVAILABLE",
                "error_message": f"Unresolved pending writes exist; cannot {what}.",
            },
        )


def _fail_stop(state: NodeState, reason: str) -> None:
    """Take the node out of service after an unrecoverable local I/O failure.

    A node that cannot write its WAL must not keep looking healthy: it stops serving
    (every endpoint returns NODE_NOT_READY), stops heartbeating and drops its lease, so
    the coordinator fails over to the follower. A restart re-runs recovery.
    """
    state.ready = False
    state.lease_expires_at = 0.0
    logger.critical("Node %s fail-stopped: %s", state.node_id, reason)


def _wal_append(state: NodeState, **record: Any) -> dict[str, Any]:
    """Append to the WAL; on an uncertain I/O failure fail-stop the node, then re-raise."""
    if state.wal is None:
        raise RuntimeError("WAL is not open.")
    try:
        return state.wal.append(**record)
    except Exception as exc:
        if state.wal.failed:
            _fail_stop(state, f"WAL append failed: {exc}")
        raise


def _mark_pending(
    state: NodeState,
    request_id: str,
    seq: int,
    op: WriteOp,
    doc_id: int,
    payload: dict[str, Any] | None,
    *,
    fingerprint: str | None = None,
    migration_mode: bool = False,
) -> None:
    """Track a WAL entry whose replication outcome is unknown.

    Every path that appends to the WAL and then fails to replicate MUST call this:
    an untracked entry would leave the engine behind the WAL, so the next write
    would commit but fail to apply (and its retry would execute twice).
    """
    state.follower_state = ReplicaState.DOWN
    state.pending_unresolved_writes[request_id] = {
        "seq": seq,
        "term": state.term,
        "op": op.value,
        "doc_id": doc_id,
        "payload": payload,
        "migration_mode": migration_mode,
    }
    state.engine.dedup_store(
        request_id,
        DedupEntry(
            seq=seq,
            outcome=WriteOutcome.OUTCOME_UNKNOWN,
            result={"outcome": WriteOutcome.OUTCOME_UNKNOWN, "seq": seq},
            term=state.term,
            fingerprint=fingerprint,
        ),
    )


def _persist_commit_best_effort(state: NodeState) -> None:
    """Persist committed_seq after a write is already durable on both replicas.

    The write is committed once the follower fsynced it; failing to record the
    high-water mark locally only means recovery will re-confirm it with the
    follower. It must not turn a committed write into an error for the client.
    """
    try:
        _persist_node_metadata(state)
    except Exception as exc:
        logger.error(
            "Node %s: committed write durable on both replicas, but persisting "
            "committed_seq=%d failed: %s",
            state.node_id,
            state.committed_seq,
            exc,
        )


async def _append_replicated(
    state: NodeState,
    op: WriteOp,
    doc_id: int,
    request_id: str,
    payload: dict[str, Any] | None,
    *,
    what: str,
) -> int:
    """Append + replicate + apply one internal (migration / purge) operation.

    On replication failure the entry is tracked as pending (never orphaned) and a
    503 is raised; the caller may retry once the pending entry is reconciled.
    Caller holds the write lock and has verified no writes are pending.
    """
    if state.wal is None:  # explicit: asserts are stripped under -O
        raise RuntimeError("state.wal is unavailable")
    seq = state.wal.next_seq()
    entry = _wal_append(
        state,
        seq=seq,
        op=op.value,
        doc_id=doc_id,
        term=state.term,
        request_id=request_id,
        payload=payload,
    )
    if state.follower_url:
        ok = await _replicate_to_follower(state, seq, op, doc_id, request_id, payload)
        if not ok:
            _mark_pending(state, request_id, seq, op, doc_id, payload, migration_mode=True)
            raise HTTPException(
                status_code=503,
                detail={
                    "error_code": "REPLICATION_UNAVAILABLE",
                    "error_message": f"Follower replication failed during {what}",
                },
            )
        state.follower_acked_seq = max(state.follower_acked_seq, seq)
    state.engine.apply(entry, migration_mode=True)
    state.committed_seq = max(state.committed_seq, seq)
    _store_committed(state, request_id, seq)
    return seq


def _store_committed(state: NodeState, request_id: str, seq: int) -> None:
    state.engine.dedup_store(
        request_id,
        DedupEntry(
            seq=seq,
            outcome=WriteOutcome.COMMITTED,
            result={"outcome": WriteOutcome.COMMITTED, "seq": seq},
            term=state.term,
        ),
    )


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
    *,
    ownership_epoch: int | None = None,
    term: int | None = None,
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

        # 3b. Fencing tokens: reject writes routed with stale ownership / term info.
        if ownership_epoch is not None and ownership_epoch < state.epoch_for(doc_id):
            raise StaleEpoch(
                f"Write carries ownership_epoch {ownership_epoch} < current "
                f"{state.epoch_for(doc_id)} for doc_id={doc_id}."
            )
        if term is not None and term != state.term:
            raise StaleTerm(f"Write carries term {term}; primary is at term {state.term}.")

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
        if state.wal is None:  # explicit: asserts are stripped under -O
            raise RuntimeError("state.wal is unavailable")
        seq = state.wal.next_seq()
        try:
            _wal_append(
                state,
                seq=seq,
                term=state.term,
                op=op.value,
                doc_id=doc_id,
                request_id=request_id,
                payload=payload,
            )
        except Exception as exc:
            if state.wal.failed:
                # The record may or may not be on disk: the outcome is genuinely unknown.
                raise OutcomeUnknown(
                    f"WAL write failed (seq={seq}); node stopped serving. "
                    "Retry with the same request_id."
                ) from exc
            raise

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
                _mark_pending(state, request_id, seq, op, doc_id, payload, fingerprint=fp)
                raise OutcomeUnknown(
                    f"Write appended to primary WAL (seq={seq}), but follower replication failed."
                )
            state.follower_acked_seq = seq
            state.committed_seq = max(state.committed_seq, seq)
            state.pending_unresolved_writes.pop(request_id, None)
            _persist_commit_best_effort(state)
        elif state.allow_standalone:
            # Explicit standalone mode: local-only commit (clearly distinguished)
            state.committed_seq = max(state.committed_seq, seq)
            _persist_commit_best_effort(state)
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
        if state.wal is None:  # explicit: asserts are stripped under -O
            raise RuntimeError("state.wal is unavailable")
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
        # base_seq: history below the first retained WAL entry lives only in the local
        # snapshot (compaction / snapshot install). It is committed history.
        base_seq = (local_entries[0]["seq"] - 1) if local_entries else state.wal.last_seq
        body_dict = {"shard_id": state.shard_id, "entries": local_entries, "base_seq": base_seq}
        body_bytes = json.dumps(body_dict, separators=(",", ":")).encode("utf-8")
        sig = sign_request(state.settings.CLUSTER_SECRET, "POST", div_url, body_bytes)
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
                snap_sig = sign_request(state.settings.CLUSTER_SECRET, "GET", snap_url)
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

                parsed_docs = _parse_snapshot_docs(snap_data.get("documents", {}))

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
                        installed=True,
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

                # 5. Only AFTER the new snapshot is durable, drop the whole local WAL: the
                #    snapshot supersedes it, and keeping an old prefix would leave a
                #    sequence gap (or divergent history) in front of the next entries.
                if state.wal is not None:
                    state.wal.reset(state.engine.last_applied_seq + 1)
                catchup_from = state.engine.last_applied_seq
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
                # Rebuild in-memory engine and dedup up to div_seq (snapshot + WAL)
                _rebuild_engine_from_disk(state)

                catchup_from = div_seq

            # Catch up every entry after the installed snapshot / divergence point.
            # Both paths need this: after a snapshot install the primary may still hold
            # WAL entries beyond its snapshot (e.g. a pending write); skipping them would
            # leave the follower "SYNCED" yet missing entries the primary waits for.
            catchup_url = f"http://{clean_primary}/internal/catchup"
            req_b = {"shard_id": state.shard_id, "after_seq": catchup_from}
            b_bytes = json.dumps(req_b, separators=(",", ":")).encode("utf-8")
            c_sig = sign_request(state.settings.CLUSTER_SECRET, "POST", catchup_url, b_bytes)
            c_resp = await client.post(
                catchup_url,
                content=b_bytes,
                headers={
                    "Content-Type": "application/json",
                    CLUSTER_SIG_HEADER: c_sig,
                },
            )
            if c_resp.status_code != 200:
                logger.warning("Catchup failed with status %d: %s", c_resp.status_code, c_resp.text)
                return False

            c_data = c_resp.json()
            if c_data.get("needs_snapshot"):
                # The primary compacted past our position since the divergence check;
                # fail this round: the next attempt is told to install a snapshot.
                logger.warning(
                    "Catch-up from seq %d needs a snapshot; retrying resync later", catchup_from
                )
                return False
            for entry in c_data.get("entries", []):
                _wal_append(
                    state,
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
            comp_sig = sign_request(state.settings.CLUSTER_SECRET, "POST", complete_url, comp_bytes)
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
                            rsig = sign_request(settings.CLUSTER_SECRET, "POST", reg_url, rb)
                            t_reg = time.monotonic()
                            rresp = await hb_client.post(
                                reg_url,
                                content=rb,
                                headers={
                                    "Content-Type": "application/json",
                                    CLUSTER_SIG_HEADER: rsig,
                                },
                            )
                            registered = rresp.status_code == 200
                            if registered:
                                await _apply_registration(state, rresp.json(), t_reg)
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
                        sig = sign_request(settings.CLUSTER_SECRET, "POST", url, body_b)
                        headers = {"Content-Type": "application/json", CLUSTER_SIG_HEADER: sig}
                        # Lease safety: count from when the request was SENT, never from when
                        # the reply arrived, so we can never outlive the coordinator's lease.
                        t_send = time.monotonic()
                        resp = await hb_client.post(url, content=body_b, headers=headers)
                        if resp.status_code == 200:
                            data = resp.json()
                            if data and "granted_at" in data and "expires_at" in data:
                                state.lease_expires_at = t_send + lease_duration_from_grant(
                                    data, settings.LEASE_DURATION_S
                                )
                            elif data is None:
                                registered = False  # coordinator does not know us; re-register
                        elif resp.status_code == 409:
                            # Step down only on an explicit stale-term fence (final report #12);
                            # any other 409 is an unrelated rejection and must not demote us.
                            try:
                                err_code = resp.json().get("error_code")
                            except Exception:
                                err_code = None
                            if err_code == "STALE_TERM":
                                node_logger.warning(
                                    "Heartbeat returned 409 STALE_TERM; demoting to follower"
                                )
                                state.lease_expires_at = 0.0  # stop serving immediately
                                async with state.write_lock():
                                    _persist_node_metadata(state, role=NodeRole.FOLLOWER)
                                    state.role = NodeRole.FOLLOWER
                            else:
                                node_logger.warning(
                                    "Heartbeat rejected with 409 %s; not demoting", err_code
                                )
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
                    sig = sign_request(settings.CLUSTER_SECRET, "POST", "/internal/resync", raw)
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

        # Periodic snapshot + safe WAL compaction (BUG 12)
        async def _snapshot_loop() -> None:
            every_n = settings.SNAPSHOT_EVERY_N_OPS
            every_s = settings.SNAPSHOT_EVERY_S
            if every_n <= 0 and every_s <= 0:
                return
            last_seq = state.engine.last_applied_seq
            last_t = time.monotonic()
            while True:
                await asyncio.sleep(_SNAPSHOT_POLL_S)
                if not state.ready:
                    continue
                ops = state.engine.last_applied_seq - last_seq
                if ops <= 0:
                    last_seq = min(last_seq, state.engine.last_applied_seq)
                    continue
                due_ops = every_n > 0 and ops >= every_n
                due_time = every_s > 0 and time.monotonic() - last_t >= every_s
                if not (due_ops or due_time):
                    continue
                try:
                    removed = await snapshot_and_compact(state)
                    last_seq = state.engine.last_applied_seq
                    last_t = time.monotonic()
                    node_logger.info(
                        "Periodic snapshot at seq %d; compacted %d WAL entries", last_seq, removed
                    )
                except Exception as exc:
                    node_logger.error("Periodic snapshot/compaction failed: %s", exc)

        recovery_task = asyncio.create_task(_follower_recovery())
        hb_task = asyncio.create_task(_heartbeat_sender())
        snap_task = asyncio.create_task(_snapshot_loop())

        yield

        for t in (hb_task, recovery_task, snap_task):
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
    app.add_middleware(BodyLimitMiddleware, max_bytes=settings.MAX_BODY_SIZE_BYTES)

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
    # Data routes are reachable on the shard's port, not just via the router: they must
    # authenticate the client API key themselves (the router forwards it).
    client_auth = make_client_auth_dependency(lambda: settings.CLIENT_API_KEY)

    def _check_doc_size(doc: dict[str, Any]) -> None:
        size = serialized_doc_size(doc)
        if size > settings.MAX_DOC_SIZE_BYTES:
            raise DocTooLarge(
                f"Document is {size} bytes; limit is {settings.MAX_DOC_SIZE_BYTES} bytes."
            )

    def get_node(request: Request) -> NodeState:
        state: NodeState = request.app.state.node
        if not state.ready:
            raise HTTPException(
                status_code=503,
                detail={"error_code": "NODE_NOT_READY", "error_message": "Node is recovering."},
            )
        return state

    # ── Public Endpoints ──────────────────────────────────────────────────────

    @app.post("/doc", status_code=201, dependencies=[Depends(client_auth)])
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
                detail={
                    "error_code": "VALIDATION_ERROR",
                    "error_message": safe_validation_message(exc),
                },
            ) from exc

        doc_dict = doc.model_dump()
        doc_id = doc_dict["doc_id"]
        try:
            _check_doc_size(doc_dict)
            return await _execute_write(
                state,
                body.request_id,
                WriteOp.INSERT,
                doc_id,
                doc_dict,
                ownership_epoch=body.ownership_epoch,
                term=body.term,
            )
        except DocstoreError as exc:
            raise exc.to_http_exception() from exc

    @app.put("/doc/{doc_id}", dependencies=[Depends(client_auth)])
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
                detail={
                    "error_code": "VALIDATION_ERROR",
                    "error_message": safe_validation_message(exc),
                },
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
            _check_doc_size(doc_dict)
            return await _execute_write(
                state,
                body.request_id,
                WriteOp.UPDATE,
                doc_id,
                doc_dict,
                ownership_epoch=body.ownership_epoch,
                term=body.term,
            )
        except DocstoreError as exc:
            raise exc.to_http_exception() from exc

    @app.delete("/doc/{doc_id}", dependencies=[Depends(client_auth)])
    async def delete_doc(doc_id: int, body: DeleteBody, request: Request) -> WriteResponse:
        state = get_node(request)
        if state.role != NodeRole.PRIMARY:
            raise NotPrimary(
                f"Node {state.node_id} is a follower; cannot accept writes."
            ).to_http_exception()

        try:
            return await _execute_write(
                state,
                body.request_id,
                WriteOp.DELETE,
                doc_id,
                None,
                ownership_epoch=body.ownership_epoch,
                term=body.term,
            )
        except DocstoreError as exc:
            raise exc.to_http_exception() from exc

    @app.get("/doc/{doc_id}", dependencies=[Depends(client_auth)])
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

    @app.post("/query", dependencies=[Depends(client_auth)])
    async def query_docs(body: ShardQueryBody, request: Request) -> QueryResponse:
        state = get_node(request)
        try:
            validate_query_filter(body.filter)
        except InvalidQuery as exc:
            raise exc.to_http_exception() from exc

        if state.role == NodeRole.PRIMARY and not state.is_lease_valid():
            raise LeaseExpired("Primary lease has expired.").to_http_exception()

        # Effective ranges: owned ranges, optionally restricted to the ranges the router
        # attributes to this shard. Restricting BEFORE sort/skip/limit keeps pagination
        # correct when one shard owns several disjoint ranges.
        effective: list[tuple[int, int]] = [(r.start, r.end) for r in state.ranges]
        if body.ranges is not None:
            restricted: list[tuple[int, int]] = []
            for o_s, o_e in effective:
                for q_s, q_e in body.ranges:
                    lo, hi = max(o_s, q_s), min(o_e, q_e)
                    if lo < hi:
                        restricted.append((lo, hi))
            effective = _merge_ranges(restricted)
            # The caller believes this shard owns these ranges. If it does not own all
            # of them, the caller's map is stale (e.g. a range migrated away): answering
            # with a silent partial result would hide documents, so refuse instead and
            # let the router refresh its map and retry.
            covered = sum(hi - lo for lo, hi in effective)
            requested = sum(e - s for s, e in body.ranges)
            if covered != requested:
                raise OutOfRange(
                    f"Shard {state.shard_id} does not own all requested ranges "
                    f"{body.ranges}; owned {[(r.start, r.end) for r in state.ranges]}."
                ).to_http_exception()

        def _in_effective(did: int) -> bool:
            return any(lo <= did < hi for lo, hi in effective)

        # Collect candidate documents according to shard-key constraints & ownership
        constraint = detect_shard_key_constraint(body.filter)
        candidates: list[dict[str, Any]] = []

        if constraint.kind == "exact":
            if constraint.doc_id is None:  # explicit: asserts are stripped under -O
                raise RuntimeError("constraint.doc_id is unavailable")
            if _in_effective(constraint.doc_id):
                doc = state.engine.get(constraint.doc_id)
                if doc is not None:
                    candidates.append(doc)
        elif constraint.kind == "in":
            for doc_id in constraint.doc_ids or []:
                if _in_effective(doc_id):
                    doc = state.engine.get(doc_id)
                    if doc is not None:
                        candidates.append(doc)
        elif constraint.kind == "range":
            if (
                constraint.range_start is None or constraint.range_end is None
            ):  # explicit: asserts are stripped under -O
                raise RuntimeError("constraint.range_start / constraint.range_end unavailable")
            for lo, hi in effective:
                s = max(constraint.range_start, lo)
                e = min(constraint.range_end, hi)
                if s < e:
                    candidates.extend(state.engine.range_scan(s, e))
        else:
            for lo, hi in effective:
                candidates.extend(state.engine.range_scan(lo, hi))

        matched = [doc for doc in candidates if evaluate_filter(doc, body.filter)]
        try:
            sorted_docs = sort_documents(
                matched, sort_field=body.sort_field, sort_dir=body.sort_dir
            )
        except InvalidQuery as exc:  # e.g. sorting on an array / object field
            raise exc.to_http_exception() from exc
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

    @app.get("/status", dependencies=[Depends(client_auth)])
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
        _require_same_shard(state, body.shard_id)
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

        if state.wal is None:  # explicit: asserts are stripped under -O
            raise RuntimeError("state.wal is unavailable")
        expected_seq = state.wal.next_seq()

        if body.seq == expected_seq:
            # 1. Append to follower WAL and fsync
            _wal_append(
                state,
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
        _require_same_shard(state, body.shard_id)
        if state.wal is None:  # explicit: asserts are stripped under -O
            raise RuntimeError("state.wal is unavailable")

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
        _require_same_shard(state, body.get("shard_id"))
        seq = body.get("seq")
        term = body.get("term")
        request_id = body.get("request_id")

        wal_last_seq = state.wal.last_seq if state.wal else 0
        last_applied_seq = state.engine.last_applied_seq

        if (
            not isinstance(seq, int)
            or isinstance(seq, bool)
            or seq < 1
            or not isinstance(term, int)
            or isinstance(term, bool)
            or not isinstance(request_id, str)
            or not request_id
        ):
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
        _require_same_shard(state, body.get("shard_id"))
        async with state.write_lock():
            snap_seq = _strict_int(body, "last_applied_seq", default=0)
            if snap_seq < state.committed_seq:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Cannot apply snapshot with last_applied_seq ({snap_seq}) < "
                        f"local committed_seq ({state.committed_seq})"
                    ),
                )
            snap_term = _strict_int(body, "term", default=state.term)
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

            # C2: all_docs() returns dict[int, dict]; accept dict or list format.
            try:
                parsed_docs = _parse_snapshot_docs(body.get("documents", {}))
            except (ValueError, TypeError, KeyError) as exc:
                raise HTTPException(
                    status_code=400, detail=f"Malformed snapshot documents: {exc}"
                ) from exc

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
                        installed=True,
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

            # The snapshot supersedes all local history: restart the WAL right after it.
            if state.wal is not None:
                state.wal.reset(state.engine.last_applied_seq + 1)

            return {"status": "ok", "applied_seq": state.engine.last_applied_seq}

    @app.post("/internal/fence", dependencies=[Depends(cluster_auth)])
    async def fence(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        _require_same_shard(state, body.get("shard_id"))
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
    async def promote(body: PromoteRequest, request: Request) -> dict[str, Any]:
        state = get_node(request)
        _require_same_shard(state, body.shard_id)

        def _reject(code: str, msg: str) -> HTTPException:
            return HTTPException(status_code=409, detail={"error_code": code, "error_message": msg})

        async with state.write_lock():
            # 1. Validate the request completely before touching any state.
            new_term = body.term if body.term is not None else state.term + 1
            if new_term < 1 or new_term < state.term:
                raise StaleTerm(
                    f"Promotion term {new_term!r} is behind node term {state.term}."
                ).to_http_exception()

            current = {(r.start, r.end): r.ownership_epoch for r in state.ranges}
            if body.ranges is not None:
                # Coordinator sends the authoritative ranges for this shard from the map.
                candidate = sorted(
                    (r.model_copy() for r in body.ranges), key=lambda r: (r.start, r.end)
                )
                for prev, nxt in itertools.pairwise(candidate):
                    if nxt.start < prev.end:
                        raise _reject("INVALID_RANGES", "Promotion ranges overlap.")
                for r in candidate:
                    if r.end <= r.start:
                        raise _reject("INVALID_RANGES", f"Empty range [{r.start}, {r.end}).")
                    old = current.get((r.start, r.end))
                    if old is not None and r.ownership_epoch < old:
                        raise _reject(
                            "STALE_EPOCH",
                            f"Promotion epoch {r.ownership_epoch} < local {old} "
                            f"for [{r.start}, {r.end}).",
                        )
            elif body.ownership_epochs is not None:
                keys = {f"{a}-{b}" for a, b in current}
                if set(body.ownership_epochs) != keys:
                    raise _reject(
                        "RANGE_MISMATCH",
                        f"Promotion epochs for {sorted(body.ownership_epochs)} do not match "
                        f"owned ranges {sorted(keys)}.",
                    )
                candidate = []
                for (a, b), old in sorted(current.items()):
                    ep = body.ownership_epochs[f"{a}-{b}"]
                    if ep < old:
                        raise _reject(
                            "STALE_EPOCH", f"Promotion epoch {ep} < local {old} for [{a}, {b})."
                        )
                    candidate.append(ShardRangeConfig(start=a, end=b, ownership_epoch=ep))
            else:
                candidate = [
                    ShardRangeConfig(start=a, end=b, ownership_epoch=old + 1)
                    for (a, b), old in sorted(current.items())
                ]

            # Idempotent retry of an already-applied promotion: refresh lease only.
            if state.role == NodeRole.PRIMARY and new_term == state.term:
                if [(r.start, r.end, r.ownership_epoch) for r in candidate] != [
                    (r.start, r.end, r.ownership_epoch) for r in state.ranges
                ]:
                    _persist_node_metadata(state, ranges=candidate)
                    state.ranges = candidate
                state.lease_expires_at = time.monotonic() + _lease_duration(body.lease)
                return _promote_result(state)

            # 2. Prepare the log: reconcile pending writes, settle the committed prefix.
            if body.follower_url:
                state.follower_url = body.follower_url
            if state.pending_unresolved_writes and state.follower_url:
                for p_rid in list(state.pending_unresolved_writes.keys()):
                    try:
                        await _reconcile_pending_write(state, p_rid)
                    except Exception as exc:
                        logger.debug("Promotion reconciliation for %s deferred: %s", p_rid, exc)

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

            # 3. Persist the candidate authority FIRST. If this fails the node keeps its
            #    old term / role / ranges and the coordinator sees the error (fail closed).
            _persist_node_metadata(state, term=new_term, role=NodeRole.PRIMARY, ranges=candidate)

            # 4. Only now activate it. Role goes last so nothing serves as primary early.
            state.term = new_term
            state.ranges = candidate
            state.follower_state = ReplicaState.DOWN  # degraded until follower is SYNCED
            state.lease_expires_at = time.monotonic() + _lease_duration(body.lease)
            state.role = NodeRole.PRIMARY

            logger.info(
                "Promoted node %s to PRIMARY for shard %s in term %d (degraded mode)",
                state.node_id,
                state.shard_id,
                state.term,
            )
            return _promote_result(state)

    def _lease_duration(lease: dict[str, Any] | None) -> float:
        return lease_duration_from_grant(lease, float(settings.LEASE_DURATION_S))

    def _promote_result(state: NodeState) -> dict[str, Any]:
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
        _require_same_shard(state, body.get("shard_id"))
        if state.wal is None:  # explicit: asserts are stripped under -O
            raise RuntimeError("state.wal is unavailable")

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

        # base_seq: the follower's history at or below this seq lives only in its
        # snapshot (WAL compacted / snapshot installed). That prefix is committed history,
        # so matching starts right after it.
        try:
            base_seq = (
                int(sorted_entries[0].get("seq", 0)) - 1
                if sorted_entries
                else int(body.get("base_seq", 0) or 0)
            )
        except (TypeError, ValueError):
            base_seq = 0
        base_seq = max(0, base_seq)

        if base_seq > last_seq or base_seq + 1 < earliest:
            # We cannot continue the follower's log from its base (this includes an empty,
            # freshly provisioned follower against a compacted WAL): full snapshot needed.
            needs_snapshot = True
        # If follower's earliest entry is older than primary's earliest available entry
        elif sorted_entries and int(sorted_entries[0].get("seq", 0)) < earliest:
            # Primary has pruned/compacted its WAL before earliest_seq; cannot verify from here
            needs_snapshot = True
        elif not sorted_entries:
            candidate_divergence = base_seq
        else:
            expected_start = (
                earliest if (earliest in primary_by_seq and 1 not in primary_by_seq) else 1
            )
            if base_seq > 0:
                candidate_divergence = base_seq  # contiguity continues from the base
            elif (
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
        _require_same_shard(state, body.get("shard_id"))
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

            # 2. Sequence check: the follower must hold EVERY entry in the primary's WAL,
            #    not just the committed prefix. A pending (unresolved) tail entry that the
            #    follower lacks could never be confirmed, so marking it SYNCED would block
            #    all writes behind that entry indefinitely.
            follower_seq = body.get("last_applied_seq", state.wal.last_seq if state.wal else 0)
            required_seq = max(state.committed_seq, state.wal.last_seq if state.wal else 0)
            if follower_seq < required_seq:
                logger.warning(
                    "Follower rejoin rejected: sequence lag (follower_seq=%d < primary "
                    "required_seq=%d)",
                    follower_seq,
                    required_seq,
                )
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Follower has not caught up: seq {follower_seq} < "
                        f"primary log end {required_seq}"
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
        start, end = _body_range(body)
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
            # Pin the WAL after the copy's start seq until catch-up has consumed it.
            state.hold_migration(start, end, s_seq, reset=body.get("after_doc_id") is None)
            limit = (
                None if body.get("limit") is None else _strict_int(body, "limit", lo=1, hi=10_000)
            )
            if limit is None:
                # Legacy single-shot copy of the whole range.
                docs = state.engine.range_snapshot(start, end)
                return {
                    "status": "ok",
                    "snapshot_seq": s_seq,
                    "documents": docs,
                    "next_after_doc_id": docs[-1]["doc_id"] if docs else None,
                    "done": True,
                }
            # Cursor-paginated copy, bounded by count and serialized bytes.
            after_doc_id = (
                None
                if body.get("after_doc_id") is None
                else _strict_int(body, "after_doc_id", hi=_KEYSPACE_END - 1)
            )
            max_bytes = _strict_int(
                body,
                "max_bytes",
                lo=1,
                hi=64 * 1024 * 1024,
                default=settings.MAX_BODY_SIZE_BYTES // 2,
            )
            fetched = state.engine.range_page(
                start, end, after_doc_id=after_doc_id, limit=max(1, int(limit))
            )
            page: list[dict[str, Any]] = []
            size = 0
            for d in fetched:
                d_size = serialized_doc_size(d)
                if page and size + d_size > max_bytes:
                    break
                page.append(d)
                size += d_size
            done = len(page) == len(fetched) and len(fetched) < max(1, int(limit))
            return {
                "status": "ok",
                "snapshot_seq": s_seq,
                "documents": page,
                "next_after_doc_id": page[-1]["doc_id"] if page else after_doc_id,
                "done": done,
            }

    @app.post("/internal/migration/oplog", dependencies=[Depends(cluster_auth)])
    async def migration_oplog(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        start, end = _body_range(body)
        after_seq = _strict_int(body, "after_seq", default=0)
        limit = _strict_int(body, "limit", lo=1, hi=10_000, default=1000)

        if state.wal is None:  # explicit: asserts are stripped under -O
            raise RuntimeError("state.wal is unavailable")
        state.hold_migration(start, end, after_seq)  # cursor advanced: release older entries
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
        mig_id = _migration_id(body)
        if not isinstance(docs, list):
            raise _bad_body("'documents' must be a list")
        for d in docs:
            try:
                Document.model_validate(d)
            except Exception as exc:
                raise _bad_body("invalid document in batch") from exc
        skipped = 0

        if state.wal is None:  # explicit: asserts are stripped under -O
            raise RuntimeError("state.wal is unavailable")
        async with state.write_lock():
            await _require_no_pending_writes(state, "apply migration batch")
            try:
                for doc in docs:
                    doc_id = doc["doc_id"]
                    req_id = f"mig-{mig_id}-{doc_id}"
                    prior = state.engine.dedup_check(req_id)
                    if prior is not None and prior.outcome == WriteOutcome.COMMITTED:
                        skipped += 1  # retried batch: already durably applied
                        continue
                    await _append_replicated(
                        state, WriteOp.INSERT, doc_id, req_id, doc, what="batch"
                    )
            finally:
                _persist_node_metadata(state)
            return {
                "status": "ok",
                "count": len(docs),
                "skipped": skipped,
                "last_seq": state.wal.last_seq,
            }

    @app.post("/internal/migration/apply-oplog", dependencies=[Depends(cluster_auth)])
    async def migration_apply_oplog(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        entries = body.get("entries", [])
        mig_id = _migration_id(body)
        if not isinstance(entries, list):
            raise _bad_body("'entries' must be a list")
        for e in entries:
            if not isinstance(e, dict):
                raise _bad_body("each oplog entry must be an object")
            _strict_int(e, "seq", lo=1)
            _strict_int(e, "doc_id", hi=_KEYSPACE_END - 1)
            if e.get("op") not in {o.value for o in WriteOp}:
                raise _bad_body(f"invalid op {e.get('op')!r}")
            if e.get("payload") is not None and not isinstance(e.get("payload"), dict):
                raise _bad_body("'payload' must be an object or null")
        skipped = 0

        if state.wal is None:  # explicit: asserts are stripped under -O
            raise RuntimeError("state.wal is unavailable")
        async with state.write_lock():
            await _require_no_pending_writes(state, "apply migration oplog")
            try:
                for entry in entries:
                    doc_id = entry["doc_id"]
                    op = WriteOp(entry["op"])
                    payload = entry.get("payload")
                    req_id = f"catchup-{mig_id}-{doc_id}-{entry['seq']}"
                    prior = state.engine.dedup_check(req_id)
                    if prior is not None and prior.outcome == WriteOutcome.COMMITTED:
                        skipped += 1  # this source op was already replayed
                        continue

                    await _append_replicated(state, op, doc_id, req_id, payload, what="catchup")
            finally:
                _persist_node_metadata(state)
            return {
                "status": "ok",
                "replayed": len(entries) - skipped,
                "skipped": skipped,
                "last_seq": state.wal.last_seq,
            }

    @app.post("/internal/migration/pause", dependencies=[Depends(cluster_auth)])
    async def migration_pause(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        start, end = _body_range(body)
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
        start, end = _body_range(body)
        async with state.write_lock():
            state.paused_ranges = [
                r for r in state.paused_ranges if not (r[0] == start and r[1] == end)
            ]
            state.release_migration(start, end)  # migration aborted
            logger.info("Unpaused range [%d, %d) on shard %s", start, end, state.shard_id)
            return {"status": "ok"}

    @app.post("/internal/migration/checksum", dependencies=[Depends(cluster_auth)])
    async def migration_checksum(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        start, end = _body_range(body)
        async with state.write_lock():
            docs = state.engine.range_snapshot(start, end)
            from docstore.coordinator.migration import compute_deterministic_checksum

            count, chk = compute_deterministic_checksum(docs)
            return {"status": "ok", "doc_count": count, "checksum": chk}

    @app.post("/internal/migration/cutover", dependencies=[Depends(cluster_auth)])
    async def migration_cutover(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        start, end = _body_range(body)
        epoch = _strict_int(body, "ownership_epoch")
        async with state.write_lock():
            candidate = [r.model_copy() for r in state.ranges]
            existing = next((r for r in candidate if r.start == start and r.end == end), None)
            if existing:
                existing.ownership_epoch = max(existing.ownership_epoch, epoch)
            else:
                candidate.append(ShardRangeConfig(start=start, end=end, ownership_epoch=epoch))
                candidate.sort(key=lambda r: r.start)
            # Durable first, then visible: a failed persist must not change ownership.
            _persist_node_metadata(state, ranges=candidate)
            state.ranges = candidate
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
        if state.wal is None:  # explicit: asserts are stripped under -O
            raise RuntimeError("state.wal is unavailable")
        await _require_no_pending_writes(state, "purge range")
        docs = state.engine.range_snapshot(start, end)
        try:
            for d in docs:
                doc_id = d["doc_id"]
                # Unique per operation (seq), so repeated purges never share a dedup key.
                req_id = f"{prefix}-{state.wal.next_seq()}-{doc_id}"
                await _append_replicated(state, WriteOp.DELETE, doc_id, req_id, None, what="purge")
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
            sig = sign_request(state.settings.CLUSTER_SECRET, "POST", path, raw)
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
        start, end = _body_range(body)
        async with state.write_lock():
            # 1. Stop owning the range (works for prefix / suffix / interior / whole ranges)
            candidate = _subtract_range(state.ranges, start, end)
            _persist_node_metadata(state, ranges=candidate)
            state.ranges = candidate
            # 2. Delete docs through the WAL so the follower and restarts see it
            deleted = await _purge_range_docs(state, start, end, "cleanup")
            # 3. Only now unpause, so no write can slip in while docs are being removed
            state.paused_ranges = [
                r for r in state.paused_ranges if not (r[0] == start and r[1] == end)
            ]
            state.release_migration(start, end)
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
        start, end = _body_range(body)
        async with state.write_lock():
            candidate = _subtract_range(state.ranges, start, end)
            _persist_node_metadata(state, ranges=candidate)
            state.ranges = candidate
            state.paused_ranges = [
                r for r in state.paused_ranges if not (r[0] == start and r[1] == end)
            ]
            return {"status": "ok", "ranges": [r.model_dump() for r in state.ranges]}

    @app.post("/internal/migration/abort", dependencies=[Depends(cluster_auth)])
    async def migration_abort(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        start, end = _body_range(body)
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

    @app.exception_handler(RequestValidationError)
    async def _request_validation_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        # FastAPI's default 422 echoes the submitted input back; report only what failed.
        return JSONResponse(
            status_code=422,
            content={
                "error_code": "VALIDATION_ERROR",
                "error_message": safe_validation_message(exc),
            },
        )

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
