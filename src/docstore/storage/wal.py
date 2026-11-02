"""
Write-Ahead Log (WAL) for Sharded-Docstore.

Format: one JSON-line per record, each ending with exactly ``\\n``.
Fields per record: seq, term, op, doc_id, request_id, payload, crc32.

Rules:
- Every acknowledged write is appended + flushed + fsynced before returning.
- Seq numbers are gap-free and monotonically increasing starting at 1.
- Replay on startup:
    - Torn tail (incomplete final line, no trailing ``\\n``): silently truncate
      and continue — the write was never acknowledged.
    - Bad checksum on a *complete* (newline-terminated) record: recovery fails;
      the node refuses to serve until the WAL is repaired or resynced.
- ``truncate_after(seq)`` discards all records with seq > given value (fsynced).
"""

from __future__ import annotations

import binascii
import hashlib
import json
import logging
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any, BinaryIO

logger = logging.getLogger(__name__)

# ── Sentinel / constants ──────────────────────────────────────────────────────

_CRC_FIELD = "crc32"


# ── Helpers ───────────────────────────────────────────────────────────────────


def compute_entry_identity(record: dict[str, Any]) -> str:
    """Compute deterministic SHA-256 identity of a WAL entry.

    The identity covers:
    - term: int
    - seq: int
    - op: str
    - doc_id: int
    - request_id: str
    - payload: canonical JSON dict or None

    Transient / checksum fields (crc32, entry_id) are excluded.
    Dictionary keys are sorted recursively and compact separators are used
    to guarantee equivalent JSON objects produce identical hashes regardless
    of key ordering.
    """
    op_val = record.get("op")
    if op_val is not None and hasattr(op_val, "value"):
        op_str = str(op_val.value)
    else:
        op_str = str(op_val or "")

    canonical_entry = {
        "doc_id": int(record["doc_id"]),
        "op": op_str,
        "payload": record.get("payload"),
        "request_id": str(record.get("request_id") or ""),
        "seq": int(record["seq"]),
        "term": int(record["term"]),
    }
    canonical_bytes = json.dumps(canonical_entry, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(canonical_bytes).hexdigest()


def _crc32_of(record: dict) -> int:  # type: ignore[type-arg]
    """CRC32 over the record serialised without the crc32 field."""
    payload = {k: v for k, v in record.items() if k != _CRC_FIELD}
    serialised = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return binascii.crc32(serialised.encode("utf-8")) & 0xFFFFFFFF


def _check_crc(record: dict) -> bool:  # type: ignore[type-arg]
    stored = record.get(_CRC_FIELD)
    return stored == _crc32_of(record)


# ── WAL class ─────────────────────────────────────────────────────────────────


class WALCorruptionError(RuntimeError):
    """Raised when a complete WAL record has a bad checksum.

    The node must refuse to serve until the WAL is repaired or resynced.
    """


class WAL:
    """Append-only Write-Ahead Log.

    One instance per shard node.  Call :meth:`open` (or use as a context
    manager) before any other method.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fd: BinaryIO | None = None
        self._next_seq: int = 1

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def open(self) -> None:
        """Open (or create) the WAL file for appending."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = open(self._path, "ab")

    def close(self) -> None:
        if self._fd is not None:
            self._fd.flush()
            os.fsync(self._fd.fileno())
            self._fd.close()
            self._fd = None

    def __enter__(self) -> WAL:
        self.open()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    # ── Replay ────────────────────────────────────────────────────────────────

    def replay(self, after_seq: int = 0) -> Iterator[dict]:  # type: ignore[type-arg]
        """Yield all WAL records with seq > *after_seq* in order.

        Handles torn tail (incomplete last line) by truncating it.
        Raises :class:`WALCorruptionError` on a bad-checksum complete record.
        """
        if not self._path.exists():
            return

        raw = self._path.read_bytes()

        # Detect torn tail: file does not end with newline
        if raw and not raw.endswith(b"\n"):
            # Find the last complete record boundary
            last_newline = raw.rfind(b"\n")
            if last_newline == -1:
                # No complete record at all — entire file is torn
                logger.warning(
                    "WAL %s has no complete records (torn write); truncating to empty",
                    self._path,
                )
                self._truncate_bytes(0)
                return
            # Truncate the incomplete tail
            safe_len = last_newline + 1
            logger.warning(
                "WAL %s has a torn tail; truncating %d trailing bytes",
                self._path,
                len(raw) - safe_len,
            )
            self._truncate_bytes(safe_len)
            raw = raw[:safe_len]

        max_seq = after_seq
        for lineno, line in enumerate(raw.splitlines(), start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise WALCorruptionError(
                    f"WAL {self._path}: line {lineno} is not valid JSON: {exc}"
                ) from exc

            if not _check_crc(record):
                raise WALCorruptionError(
                    f"WAL {self._path}: line {lineno} has a bad CRC32 "
                    f"(seq={record.get('seq')}). Node refuses to start."
                )

            seq = record["seq"]
            if seq > after_seq:
                yield record
                max_seq = max(max_seq, seq)

        self._next_seq = max_seq + 1

    # ── Append ────────────────────────────────────────────────────────────────

    def append(
        self,
        *,
        seq: int,
        term: int,
        op: str,
        doc_id: int,
        request_id: str,
        payload: dict | None,  # type: ignore[type-arg]
    ) -> dict:  # type: ignore[type-arg]
        """Append one record, fsync, and return the stored record dict."""
        if self._fd is None:
            raise RuntimeError("WAL is not open; call open() first.")

        record: dict = {  # type: ignore[type-arg]
            "seq": seq,
            "term": term,
            "op": op,
            "doc_id": doc_id,
            "request_id": request_id,
            "payload": payload,
        }
        record["entry_id"] = compute_entry_identity(record)
        record[_CRC_FIELD] = _crc32_of(record)

        line = json.dumps(record, separators=(",", ":")) + "\n"
        self._fd.write(line.encode("utf-8"))
        self._fd.flush()
        os.fsync(self._fd.fileno())

        self._next_seq = seq + 1
        return record

    def next_seq(self) -> int:
        """Return the next seq number to use."""
        return self._next_seq

    @property
    def last_seq(self) -> int:
        """Return the highest assigned sequence number."""
        return self._next_seq - 1

    def earliest_seq(self) -> int | None:
        """Return the sequence number of the first record in the WAL, if any."""
        if not self._path.exists():
            return None
        try:
            with open(self._path, "rb") as f:
                first_line = f.readline().strip()
                if not first_line:
                    return None
                rec = json.loads(first_line)
                return int(rec["seq"])
        except Exception:
            return None

    # ── Truncation ────────────────────────────────────────────────────────────

    def truncate_after(self, seq: int) -> None:
        """Discard all records with seq > *seq* (fsynced).

        Used to remove a divergent uncommitted tail on a new-term primary
        or to clean up after a snapshot.
        """
        if not self._path.exists():
            return

        # Read all complete records up to and including *seq*
        raw = self._path.read_bytes()
        keep_lines: list[bytes] = []
        for line in raw.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            try:
                record = json.loads(stripped)
            except json.JSONDecodeError:
                break  # treat as torn tail boundary
            if record.get("seq", 0) <= seq:
                keep_lines.append(stripped + b"\n")
            else:
                break  # WAL is ordered; stop once we pass seq

        keep_bytes = b"".join(keep_lines)
        self._truncate_bytes(len(keep_bytes))
        self._next_seq = seq + 1
        logger.info("WAL truncated after seq=%d; next_seq=%d", seq, self._next_seq)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _truncate_bytes(self, length: int) -> None:
        """Truncate the WAL file to *length* bytes, fsynced."""
        # Must close current fd, truncate, re-open for appending
        if self._fd is not None:
            self._fd.flush()
            os.fsync(self._fd.fileno())
            self._fd.close()
            self._fd = None

        with open(self._path, "r+b") as f:
            f.truncate(length)
            os.fsync(f.fileno())

        # Fsync the directory so the truncation is durable
        _fsync_dir(self._path.parent)

        # Re-open for appending
        self._fd = open(self._path, "ab")


def _fsync_dir(directory: Path) -> None:
    """Fsync the directory entry (for rename/create durability)."""
    try:
        dirfd = os.open(str(directory), os.O_RDONLY)
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)
    except OSError:
        pass  # best-effort on platforms that don't support dir fsync
