"""
Shard Map management and routing lookups — Module 4.1.

Responsibilities:
- ShardMap schema validation (contiguous, non-overlapping, full coverage of [0, 2^63)).
- Atomic file persistence (temp file -> fsync -> rename -> parent fsync).
- Append-only epoch history log.
- Bootstrap 3-shard default configuration.
- doc_id and range lookup helpers.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import uuid
from pathlib import Path

from docstore.common.errors import OutOfRange
from docstore.common.models import (
    DOC_ID_MAX,
    DOC_ID_MIN,
    ShardMap,
    ShardMapEntry,
    ShardRange,
)
from docstore.storage.wal import _fsync_dir

logger = logging.getLogger(__name__)

# Full key space: [0, 2^63)
KEY_SPACE_START: int = DOC_ID_MIN
KEY_SPACE_END: int = 2**63  # 9_223_372_036_854_775_808

# Three equal partitions of [0, 2^63)
_S1 = KEY_SPACE_END // 3  # 3_074_457_345_618_258_602
_S2 = _S1 * 2  # 6_148_914_691_236_517_204


def create_default_3_shard_map(
    primaries: list[str] | None = None,
    followers: list[str | None] | None = None,
    map_version: int = 1,
) -> ShardMap:
    """Create a balanced 3-shard map covering the entire [0, 2^63) key space."""
    if primaries is None:
        primaries = ["127.0.0.1:8200", "127.0.0.1:8201", "127.0.0.1:8202"]
    if followers is None:
        followers = [None, None, None]

    if len(primaries) != 3:
        raise ValueError("Exactly 3 primaries are required for 3-shard configuration.")
    if len(followers) != 3:
        raise ValueError("Exactly 3 follower definitions (or None) are required.")

    entries = [
        ShardMapEntry(
            shard_id="shard0",
            range=ShardRange(start=0, end=_S1),
            primary=primaries[0],
            follower=followers[0],
            ownership_epoch=1,
            map_version=map_version,
        ),
        ShardMapEntry(
            shard_id="shard1",
            range=ShardRange(start=_S1, end=_S2),
            primary=primaries[1],
            follower=followers[1],
            ownership_epoch=1,
            map_version=map_version,
        ),
        ShardMapEntry(
            shard_id="shard2",
            range=ShardRange(start=_S2, end=KEY_SPACE_END),
            primary=primaries[2],
            follower=followers[2],
            ownership_epoch=1,
            map_version=map_version,
        ),
    ]

    return ShardMap(map_version=map_version, entries=entries)


def validate_shard_map(shard_map: ShardMap) -> None:
    """Validate that shard_map satisfies all static contracts:
    - Every entry has a non-empty primary string.
    - Ranges are contiguous and non-overlapping.
    - Full coverage of [0, 2^63).
    """
    for entry in shard_map.entries:
        if not entry.primary or not entry.primary.strip():
            raise ValueError(f"Shard {entry.shard_id!r} has empty primary node address.")

    # Coverage validation is also built into ShardMap model_validator
    sorted_entries = sorted(shard_map.entries, key=lambda e: e.range.start)
    if not sorted_entries:
        raise ValueError("Shard map must have at least one entry.")

    expected_start = KEY_SPACE_START
    for entry in sorted_entries:
        if entry.range.start != expected_start:
            raise ValueError(
                f"Gap or overlap in shard map: expected start={expected_start}, "
                f"got {entry.range.start} for shard {entry.shard_id!r}."
            )
        expected_start = entry.range.end

    if expected_start != KEY_SPACE_END:
        raise ValueError(
            f"Shard map does not cover full key space [0, 2^63); coverage ends at {expected_start}."
        )


class ShardMapManager:
    """Manages loading, validating, caching, querying, and updating the shard map."""

    def __init__(
        self,
        path: Path | str | None = None,
        history_log_path: Path | str | None = None,
    ) -> None:
        self.path: Path | None = Path(path) if path else None
        self.history_log_path: Path | None = Path(history_log_path) if history_log_path else None
        self._shard_map: ShardMap | None = None
        # L3: track last-saved content hash to avoid duplicate history entries
        self._last_saved_hash: str | None = None

    @property
    def shard_map(self) -> ShardMap:
        if self._shard_map is None:
            if self.path and self.path.exists():
                self.load()
            else:
                raise RuntimeError("Shard map is not loaded and no existing map file found.")
        if self._shard_map is None:  # explicit: asserts are stripped under -O
            raise RuntimeError("self._shard_map is unavailable")
        return self._shard_map

    def set_map(self, shard_map: ShardMap) -> None:
        """Directly set and validate the cached shard map (useful in tests/bootstrap)."""
        validate_shard_map(shard_map)
        self._shard_map = shard_map

    def load(self, path: Path | str | None = None) -> ShardMap:
        """Load and validate the shard map from disk."""
        target_path = Path(path) if path else self.path
        if target_path is None or not target_path.exists():
            raise FileNotFoundError(f"Shard map file not found: {target_path}")

        raw = target_path.read_text(encoding="utf-8")
        data = json.loads(raw)
        shard_map = ShardMap.model_validate(data)
        validate_shard_map(shard_map)

        self._shard_map = shard_map
        if path:
            self.path = Path(path)
        logger.info(
            "Loaded shard map v%d with %d shards", shard_map.map_version, len(shard_map.entries)
        )
        return shard_map

    def reload(self) -> ShardMap:
        """Reload the shard map from its configured path."""
        return self.load(self.path)

    def save(
        self,
        shard_map: ShardMap,
        path: Path | str | None = None,
        append_history: bool = True,
    ) -> None:
        """Atomically persist shard map to disk and record epoch transition."""
        validate_shard_map(shard_map)
        target_path = Path(path) if path else self.path
        if target_path is None:
            raise ValueError("Target path must be provided to save shard map.")

        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target_path.parent / f".tmp.{uuid.uuid4().hex}.shard_map.json"

        content = shard_map.model_dump_json(indent=2)
        with open(tmp_path, "wb") as f:
            f.write(content.encode("utf-8"))
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, target_path)
        # From here on the new map is visible on disk; keep the in-memory view in sync
        # with it even if the directory fsync below fails, so the two never disagree.
        self._shard_map = shard_map
        self.path = target_path
        _fsync_dir(target_path.parent)

        if append_history:
            # L3: only append history when the map content actually changed.
            # The map file replaced above is the authoritative commit point. The epoch
            # history is an audit trail written afterwards: failing to append it must
            # NOT surface as "save failed", because callers would wrongly conclude the
            # topology change did not happen (e.g. abort a migration whose ownership
            # already moved). Log loudly instead.
            new_hash = hashlib.sha256(
                json.dumps(json.loads(shard_map.model_dump_json()), sort_keys=True).encode()
            ).hexdigest()
            if new_hash != self._last_saved_hash:
                try:
                    self._append_history_log(shard_map, target_path)
                    self._last_saved_hash = new_hash
                except OSError as exc:
                    logger.error(
                        "Shard map v%d COMMITTED but epoch-history append failed: %s",
                        shard_map.map_version,
                        exc,
                    )

        logger.info("Saved shard map v%d atomically to %s", shard_map.map_version, target_path)

    def _append_history_log(self, shard_map: ShardMap, target_path: Path) -> None:
        """Append an entry to the append-only epoch history log."""
        hist_path = self.history_log_path
        if hist_path is None:
            hist_path = target_path.parent / "epoch_history.log"

        hist_path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "timestamp": time.time(),
            "map_version": shard_map.map_version,
            "entries": [
                {
                    "shard_id": e.shard_id,
                    "range": {"start": e.range.start, "end": e.range.end},
                    "primary": e.primary,
                    "follower": e.follower,
                    "ownership_epoch": e.ownership_epoch,
                }
                for e in shard_map.entries
            ],
        }
        line = (json.dumps(record, separators=(",", ":")) + "\n").encode("utf-8")
        with open(hist_path, "ab") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def get_shard_for_doc_id(self, doc_id: int) -> ShardMapEntry:
        """Return the shard map entry owning the given doc_id."""
        if isinstance(doc_id, bool) or not isinstance(doc_id, int):
            raise OutOfRange(f"doc_id must be an integer, got {type(doc_id).__name__}")
        if doc_id < DOC_ID_MIN or doc_id > DOC_ID_MAX:
            raise OutOfRange(f"doc_id={doc_id} out of range [{DOC_ID_MIN}, {DOC_ID_MAX}]")

        current_map = self.shard_map
        for entry in current_map.entries:
            if entry.range.start <= doc_id < entry.range.end:
                return entry

        raise OutOfRange(f"No shard owns doc_id={doc_id} in map v{current_map.map_version}")

    def get_shards_for_range(self, start: int, end: int) -> list[ShardMapEntry]:
        """Return all shard map entries whose range overlaps [start, end)."""
        if isinstance(start, bool) or isinstance(end, bool):
            raise OutOfRange("Range bounds cannot be booleans")
        if start < DOC_ID_MIN or end > KEY_SPACE_END or start >= end:
            raise OutOfRange(f"Invalid range [{start}, {end})")

        current_map = self.shard_map
        overlapping: list[ShardMapEntry] = []
        for entry in sorted(current_map.entries, key=lambda e: e.range.start):
            # Check overlap between [entry.range.start, entry.range.end) and [start, end)
            if max(entry.range.start, start) < min(entry.range.end, end):
                overlapping.append(entry)

        return overlapping
