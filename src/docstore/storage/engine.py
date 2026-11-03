"""
In-memory storage engine with ordered index and dedup table.

Design:
- ``_docs``: ``dict[int, dict]`` — primary document store keyed by doc_id.
- ``_index``: ``SortedDict[int, None]`` — ordered doc_id index for range scans.
- ``_doc_seq``: ``dict[int, int]`` — per-document last-modified seq.
- ``_dedup``: ``dict[str, DedupEntry]`` — request_id → cached result for
  idempotency; bounded by size and TTL.
- One asyncio.Lock guards all mutations (one writer at a time).

Apply semantics:
- ``apply(entry, *, migration_mode=False)`` is idempotent by seq:
  re-applying the same seq is a no-op.
- In migration_mode: insert → upsert, delete → no-op if missing.
"""

from __future__ import annotations

import asyncio
import copy
import time
from dataclasses import dataclass, field
from typing import Any

from sortedcontainers import SortedDict

from docstore.common.errors import DuplicateKey, NotFound
from docstore.common.models import DOC_ID_MAX, DOC_ID_MIN, WriteOp

# ── Dedup ─────────────────────────────────────────────────────────────────────

_DEDUP_MAX_ENTRIES: int = 100_000
_DEDUP_TTL_S: float = 600.0  # 10 minutes


@dataclass
class DedupEntry:
    seq: int
    outcome: str
    result: dict[str, Any]
    term: int = 1
    created_at: float = field(default_factory=time.time)
    fingerprint: str | None = None


# ── Engine ────────────────────────────────────────────────────────────────────


class StorageEngine:
    """In-memory document store with an ordered index and dedup table.

    Thread-safety: one ``asyncio.Lock`` (``_lock``) must be held for all
    mutation operations.  Use :meth:`write_lock` as an async context manager.
    """

    def __init__(self) -> None:
        self._docs: dict[int, dict[str, Any]] = {}
        self._index: SortedDict = SortedDict()
        self._doc_seq: dict[int, int] = {}  # doc_id → last-modified seq
        self._dedup: dict[str, DedupEntry] = {}
        self._last_applied_seq: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()
        self._write_count: int = 0  # used for periodic dedup eviction (M2)

    # ── Lock ──────────────────────────────────────────────────────────────────

    def write_lock(self) -> asyncio.Lock:
        """Return the shard-level write lock.

        Usage::

            async with engine.write_lock():
                engine.insert(doc)
        """
        return self._lock

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def insert(self, doc: dict[str, Any], *, seq: int) -> None:
        """Insert a new document.  Raises :class:`DuplicateKey` if already exists."""
        doc_id: int = doc["doc_id"]
        if doc_id in self._docs:
            raise DuplicateKey(f"doc_id={doc_id} already exists.")
        self._docs[doc_id] = copy.deepcopy(doc)
        self._index[doc_id] = None
        self._doc_seq[doc_id] = seq

    def update(self, doc: dict[str, Any], *, seq: int) -> None:
        """Full-replace update.  Raises :class:`NotFound` if missing."""
        doc_id: int = doc["doc_id"]
        if doc_id not in self._docs:
            raise NotFound(f"doc_id={doc_id} not found.")
        self._docs[doc_id] = copy.deepcopy(doc)
        self._doc_seq[doc_id] = seq

    def delete(self, doc_id: int, *, seq: int) -> None:
        """Delete a document.  Raises :class:`NotFound` if missing."""
        if doc_id not in self._docs:
            raise NotFound(f"doc_id={doc_id} not found.")
        del self._docs[doc_id]
        del self._index[doc_id]
        self._doc_seq.pop(doc_id, None)

    def get(self, doc_id: int) -> dict[str, Any] | None:
        """Return the document or ``None`` if not found."""
        return copy.deepcopy(self._docs[doc_id]) if doc_id in self._docs else None

    # ── Range scan ────────────────────────────────────────────────────────────

    def range_scan(
        self,
        start: int,
        end: int,
    ) -> list[dict[str, Any]]:
        """Return all documents with doc_id in ``[start, end)``, ordered by doc_id."""
        result = []
        for doc_id in self._index.irange(start, end - 1):
            result.append(dict(self._docs[doc_id]))
        return result

    # ── Point-in-time snapshot of a range (for migration) ─────────────────────

    def range_snapshot(self, start: int, end: int) -> list[dict[str, Any]]:
        """Return a deep copy of all documents in ``[start, end)`` at this instant.

        Caller must hold the write lock while calling this method so the
        copy is consistent.
        """
        return [dict(self._docs[doc_id]) for doc_id in self._index.irange(start, end - 1)]

    # ── Idempotent apply (WAL replay) ─────────────────────────────────────────

    def apply(
        self,
        entry: dict[str, Any],
        *,
        migration_mode: bool = False,
    ) -> None:
        """Apply one WAL entry to the engine.

        Idempotent: if ``entry['seq'] <= self._last_applied_seq`` and the
        document state is already consistent, this is a no-op.

        In *migration_mode*:
        - INSERT → upsert (overwrite if exists)
        - DELETE → no-op if not found
        """
        seq: int = entry["seq"]
        if seq <= self._last_applied_seq:
            # Already applied — idempotent no-op.
            return

        op = entry["op"]
        doc_id: int = entry["doc_id"]
        payload: dict[str, Any] | None = entry.get("payload")

        if op == WriteOp.INSERT:
            if migration_mode:
                # Upsert
                self._docs[doc_id] = copy.deepcopy(payload or {})
                self._index[doc_id] = None
                self._doc_seq[doc_id] = seq
            else:
                if doc_id not in self._docs:
                    self._docs[doc_id] = copy.deepcopy(payload or {})
                    self._index[doc_id] = None
                    self._doc_seq[doc_id] = seq
        elif op == WriteOp.UPDATE:
            if doc_id in self._docs:
                self._docs[doc_id] = copy.deepcopy(payload or {})
                self._doc_seq[doc_id] = seq
        elif op == WriteOp.DELETE:
            if doc_id in self._docs:
                del self._docs[doc_id]
                del self._index[doc_id]
                self._doc_seq.pop(doc_id, None)
            # else: migration_mode no-op or idempotent

        self._last_applied_seq = seq

    # ── Dedup table ───────────────────────────────────────────────────────────

    def dedup_check(self, request_id: str) -> DedupEntry | None:
        """Return the cached result for *request_id*, or ``None``."""
        entry = self._dedup.get(request_id)
        if entry is None:
            return None
        # Evict if TTL exceeded
        if time.time() - entry.created_at > _DEDUP_TTL_S:
            del self._dedup[request_id]
            return None
        return entry

    def dedup_store(self, request_id: str, entry: DedupEntry) -> None:
        """Cache *entry* for *request_id*; evict oldest if over the size cap.

        Also runs a full TTL sweep every 1000 writes so that stale entries
        don't accumulate unboundedly under sustained high-throughput (M2).
        """
        self._dedup[request_id] = entry
        self._write_count += 1
        # Proactive TTL sweep every 1000 stores (M2)
        if self._write_count % 1_000 == 0:
            self._evict_stale_dedup()
        # Evict oldest entries when over the size cap
        while len(self._dedup) > _DEDUP_MAX_ENTRIES:
            oldest_key = next(iter(self._dedup))
            del self._dedup[oldest_key]

    def _evict_stale_dedup(self) -> None:
        """Proactively remove all dedup entries whose TTL has expired (M2)."""
        now = time.time()
        stale_keys = [rid for rid, e in self._dedup.items() if now - e.created_at > _DEDUP_TTL_S]
        for rid in stale_keys:
            del self._dedup[rid]

    def dedup_rebuild(self, entries: list[dict[str, Any]]) -> None:
        """Rebuild the dedup table from WAL entries (called during recovery)."""
        self._dedup.clear()
        for entry in entries:
            rid = entry.get("request_id")
            if rid:
                outcome = "COMMITTED"
                self.dedup_store(
                    rid,
                    DedupEntry(
                        seq=entry["seq"],
                        outcome=outcome,
                        result={"outcome": outcome, "seq": entry["seq"]},
                    ),
                )

    def dedup_export(self) -> dict[str, Any]:
        """Export the dedup table for inclusion in a snapshot."""
        return {
            rid: {
                "seq": e.seq,
                "outcome": e.outcome,
                "result": e.result,
                "term": e.term,
                "created_at": e.created_at,
                "fingerprint": e.fingerprint,
            }
            for rid, e in self._dedup.items()
        }

    def dedup_import(self, data: dict[str, Any]) -> None:
        """Restore the dedup table from a snapshot."""
        self._dedup.clear()
        now = time.time()
        for rid, v in data.items():
            entry = DedupEntry(
                seq=v["seq"],
                outcome=v["outcome"],
                result=v["result"],
                term=v.get("term", 1),
                created_at=v.get("created_at", now),
                fingerprint=v.get("fingerprint"),
            )
            # Skip entries older than TTL
            if now - entry.created_at <= _DEDUP_TTL_S:
                self._dedup[rid] = entry

    # ── State accessors ───────────────────────────────────────────────────────

    @property
    def last_applied_seq(self) -> int:
        return self._last_applied_seq

    @last_applied_seq.setter
    def last_applied_seq(self, value: int) -> None:
        self._last_applied_seq = value

    def doc_count(self) -> int:
        return len(self._docs)

    def all_docs(self) -> dict[int, dict[str, Any]]:
        """Return a shallow copy of the full document store (for snapshots)."""
        return dict(self._docs)

    def validate_doc_id_in_range(self, doc_id: int, start: int, end: int) -> bool:
        """Return True if doc_id is in the half-open range [start, end)."""
        return DOC_ID_MIN <= doc_id <= DOC_ID_MAX and start <= doc_id < end
