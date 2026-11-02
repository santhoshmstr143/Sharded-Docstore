"""
Snapshot manager for Sharded-Docstore.

A snapshot captures the full state needed to reconstruct a shard:
  - all documents
  - last_applied_seq + term
  - per-range ownership epochs
  - dedup table

Write strategy:
  1. Serialise state to JSON in memory.
  2. Write to a temp file in the same directory.
  3. fsync the temp file.
  4. Atomic rename temp → canonical path.
  5. fsync the directory.

Recovery = latest snapshot + WAL entries after last_applied_seq.
WAL compaction must not remove entries still needed by a lagging follower
or an active migration (tracked externally; ``SnapshotManager`` only manages
the snapshot file itself).
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SNAPSHOT_FILENAME = "snapshot.json"
SNAPSHOT_TMP_SUFFIX = ".tmp"


class SnapshotManager:
    """Manages creation and loading of shard snapshots."""

    def __init__(self, data_dir: Path) -> None:
        self._dir = data_dir
        self._path = data_dir / SNAPSHOT_FILENAME
        self._tmp_path = data_dir / (SNAPSHOT_FILENAME + SNAPSHOT_TMP_SUFFIX)

    # ── Save ──────────────────────────────────────────────────────────────────

    def save(
        self,
        *,
        documents: dict[int, dict[str, Any]],
        last_applied_seq: int,
        term: int,
        ownership_epochs: dict[str, int],
        dedup_table: dict[str, Any],
    ) -> None:
        """Atomically write a new snapshot.

        Args:
            documents:        ``{doc_id: document_dict}`` — full document store.
            last_applied_seq: Highest seq durably applied and included here.
            term:             Current shard term.
            ownership_epochs: ``{shard_range_key: epoch}`` mapping.
            dedup_table:      Serialisable dedup table state.
        """
        self._dir.mkdir(parents=True, exist_ok=True)

        snapshot: dict[str, Any] = {
            "version": 1,
            "saved_at": time.time(),
            "last_applied_seq": last_applied_seq,
            "term": term,
            "ownership_epochs": ownership_epochs,
            "dedup_table": dedup_table,
            "documents": {str(k): v for k, v in documents.items()},
        }

        payload = json.dumps(snapshot, separators=(",", ":")).encode("utf-8")

        # Write → fsync temp
        with open(self._tmp_path, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())

        # Atomic rename
        os.replace(self._tmp_path, self._path)

        # fsync directory so rename is durable
        _fsync_dir(self._dir)

        logger.info(
            "Snapshot saved: last_applied_seq=%d term=%d docs=%d",
            last_applied_seq,
            term,
            len(documents),
        )

    # ── Load ──────────────────────────────────────────────────────────────────

    def load(self) -> dict[str, Any] | None:
        """Return the latest snapshot dict, or ``None`` if no snapshot exists."""
        if not self._path.exists():
            return None

        try:
            raw = self._path.read_bytes()
            snapshot: dict[str, Any] = json.loads(raw)
        except (OSError, json.JSONDecodeError) as exc:
            logger.error("Failed to load snapshot from %s: %s", self._path, exc)
            raise RuntimeError(f"Snapshot file is corrupt: {exc}") from exc

        # Re-key documents back to int
        snapshot["documents"] = {int(k): v for k, v in snapshot.get("documents", {}).items()}

        logger.info(
            "Snapshot loaded: last_applied_seq=%d term=%d docs=%d",
            snapshot.get("last_applied_seq", 0),
            snapshot.get("term", 0),
            len(snapshot["documents"]),
        )
        return snapshot

    def exists(self) -> bool:
        return self._path.exists()

    def path(self) -> Path:
        return self._path


def _fsync_dir(directory: Path) -> None:
    dirfd = os.open(str(directory), os.O_RDONLY)
    try:
        os.fsync(dirfd)
    finally:
        os.close(dirfd)
