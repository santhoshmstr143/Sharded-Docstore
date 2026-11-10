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
import errno
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


REQUIRED_IDENTITY_FIELDS = ("term", "seq", "op", "doc_id")


def has_required_identity_fields(record: dict[str, Any]) -> bool:
    """Validate whether record has all required fields to deterministically compute identity."""
    for f in REQUIRED_IDENTITY_FIELDS:
        if f not in record or record[f] is None:
            return False
    op = record["op"]
    if hasattr(op, "value"):
        op = op.value
    if not isinstance(op, str) or not op.strip():
        return False
    try:
        int(record["doc_id"])
        int(record["seq"])
        int(record["term"])
    except (ValueError, TypeError):
        return False
    return True


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
    if not has_required_identity_fields(record):
        raise ValueError(
            f"Record lacks required identity fields {REQUIRED_IDENTITY_FIELDS}: {record}"
        )

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


class WALFailedError(RuntimeError):
    """Raised when the WAL hit an I/O failure whose on-disk effect is uncertain.

    After a failed write/flush/fsync the record may or may not be on disk, so the
    in-memory ``next_seq`` can no longer be trusted. The instance refuses every
    further append until the node reopens the WAL and re-runs recovery.
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
        self._failed: bool = False

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def open(self) -> None:
        """Open (or create) the WAL file for appending."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = open(self._path, "ab")
        self._failed = False

    @property
    def path(self) -> Path:
        return self._path

    @property
    def failed(self) -> bool:
        """True once an uncertain I/O failure poisoned this instance."""
        return self._failed

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

    def replay(self, after_seq: int = 0, *, base_seq: int | None = None) -> Iterator[dict]:  # type: ignore[type-arg]
        """Yield all WAL records with seq > *after_seq* in order.

        Handles torn tail (incomplete last line) by truncating it.
        Raises :class:`WALCorruptionError` on a bad-checksum complete record, and on
        any break in sequence continuity (duplicate, decreasing or skipped seq).

        If *base_seq* is given (recovery: the snapshot's last_applied_seq), the
        retained WAL must also start no later than ``base_seq + 1`` - otherwise
        operations between the snapshot and the WAL are missing.
        """
        if not self._path.exists():
            return

        # Torn tail: drop an incomplete final line (found by scanning back from the end,
        # without reading the whole file).
        safe_len = self._complete_prefix_len()
        size = self._path.stat().st_size
        if safe_len < size:
            if safe_len == 0:
                logger.warning(
                    "WAL %s has no complete records (torn write); truncating to empty",
                    self._path,
                )
            else:
                logger.warning(
                    "WAL %s has a torn tail; truncating %d trailing bytes",
                    self._path,
                    size - safe_len,
                )
            self._truncate_bytes(safe_len)
            if safe_len == 0:
                return

        # Stream line by line: memory stays bounded by one record, not the WAL size.
        max_seq = after_seq
        prev_seq: int | None = None
        with open(self._path, "rb") as f:
            for lineno, line in enumerate(f, start=1):
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

                seq = record.get("seq")
                if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
                    raise WALCorruptionError(
                        f"WAL {self._path}: line {lineno} has an invalid seq {seq!r}."
                    )
                if prev_seq is None:
                    if base_seq is not None and seq > base_seq + 1:
                        raise WALCorruptionError(
                            f"WAL {self._path}: first retained seq={seq} leaves a gap after "
                            f"snapshot seq={base_seq}."
                        )
                elif seq != prev_seq + 1:
                    kind = "duplicate/out-of-order" if seq <= prev_seq else "gap in"
                    raise WALCorruptionError(
                        f"WAL {self._path}: line {lineno}: {kind} sequence "
                        f"(seq={seq} after seq={prev_seq}). Node refuses to start."
                    )
                prev_seq = seq
                if seq > after_seq:
                    yield record
                    max_seq = max(max_seq, seq)

        self._next_seq = max_seq + 1

    def _complete_prefix_len(self, chunk: int = 64 * 1024) -> int:
        """Byte length of the WAL up to and including its last newline."""
        size = self._path.stat().st_size
        if size == 0:
            return 0
        with open(self._path, "rb") as f:
            f.seek(size - 1)
            if f.read(1) == b"\n":
                return size
            pos = size
            while pos > 0:
                start = max(0, pos - chunk)
                f.seek(start)
                buf = f.read(pos - start)
                idx = buf.rfind(b"\n")
                if idx != -1:
                    return start + idx + 1
                pos = start
        return 0

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
        if self._failed:
            raise WALFailedError(
                f"WAL {self._path} is unusable after an earlier I/O failure; "
                "reopen and recover before appending."
            )
        if seq != self._next_seq:
            # Only an empty WAL may start later than next_seq (its earlier history lives
            # in a snapshot, as after compaction). Anything else is a duplicate or a gap.
            empty = os.fstat(self._fd.fileno()).st_size == 0
            if not (empty and seq > self._next_seq):
                raise ValueError(
                    f"WAL {self._path}: append seq={seq} breaks continuity "
                    f"(expected {self._next_seq})."
                )

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
        try:
            self._fd.write(line.encode("utf-8"))
            self._fd.flush()
            os.fsync(self._fd.fileno())
        except BaseException:
            # The record may or may not have reached the disk: never reuse this seq
            # from this instance. Recovery (reopen + replay) decides what is durable.
            self._failed = True
            raise

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
        """Return the sequence number of the first retained record.

        An empty WAL whose history was superseded by a snapshot (``next_seq > 1``)
        reports ``next_seq``: every earlier seq is only available via snapshot.
        """
        if not self._path.exists():
            return self._next_seq if self._next_seq > 1 else None
        try:
            with open(self._path, "rb") as f:
                first_line = f.readline().strip()
                if not first_line:
                    return self._next_seq if self._next_seq > 1 else None
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

        # Stream: find the byte offset just after the last record with seq <= *seq*.
        keep_len = 0
        offset = 0
        with open(self._path, "rb") as f:
            for line in f:
                offset += len(line)
                stripped = line.strip()
                if not stripped:
                    continue
                if not line.endswith(b"\n"):
                    break  # torn tail boundary
                try:
                    record = json.loads(stripped)
                except json.JSONDecodeError:
                    break  # treat as torn tail boundary
                if record.get("seq", 0) <= seq:
                    keep_len = offset
                else:
                    break  # WAL is ordered; stop once we pass seq

        self._truncate_bytes(keep_len)
        self._next_seq = seq + 1
        logger.info("WAL truncated after seq=%d; next_seq=%d", seq, self._next_seq)

    def reset(self, next_seq: int) -> None:
        """Discard the whole WAL (fsynced) and continue numbering at *next_seq*.

        Used when a snapshot supersedes the entire local history, so the retained
        WAL stays contiguous with the snapshot instead of keeping a stale prefix.
        """
        if self._path.exists():
            self._truncate_bytes(0)
        self._next_seq = next_seq
        logger.info("WAL reset; next_seq=%d", self._next_seq)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _truncate_bytes(self, length: int) -> None:
        """Truncate the WAL file to *length* bytes, fsynced.

        Any failure leaves the on-disk state uncertain: the instance is marked failed
        (no further appends) so the owning node fail-stops and re-runs recovery.
        """
        try:
            self._truncate_bytes_unchecked(length)
        except BaseException:
            self._failed = True
            raise

    def _truncate_bytes_unchecked(self, length: int) -> None:
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


# errno values meaning "this platform/filesystem cannot fsync a directory" - the
# only failures that are safe to ignore. Everything else is a real I/O error.
_DIR_FSYNC_UNSUPPORTED = {
    errno.EINVAL,
    getattr(errno, "ENOTSUP", errno.EINVAL),
    getattr(errno, "EOPNOTSUPP", errno.EINVAL),
}


def _fsync_dir(directory: Path) -> None:
    """Fsync the directory entry (for rename/create durability).

    Raises ``OSError`` on genuine failures. Only platforms that cannot fsync a
    directory at all (Windows, or filesystems returning EINVAL/ENOTSUP) are skipped.
    """
    if os.name == "nt":  # directories cannot be opened for fsync on Windows
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    dirfd = os.open(str(directory), flags)
    try:
        os.fsync(dirfd)
    except OSError as exc:
        if exc.errno not in _DIR_FSYNC_UNSUPPORTED:
            raise
    finally:
        os.close(dirfd)
