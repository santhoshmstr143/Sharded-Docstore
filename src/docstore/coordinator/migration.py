"""
Live Range Migration state machine, persistence, and coordinator orchestration — Module 9.

Implements:
PLANNED → SNAPSHOT_COPY → CATCHUP → PAUSE → FINAL_CATCHUP → VERIFY → CUTOVER → CLEANUP → DONE
(or ABORTED before CUTOVER).
"""

from __future__ import annotations

import asyncio
import enum
import hashlib
import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
from pydantic import BaseModel, Field

from docstore.common.auth import CLUSTER_SIG_HEADER, sign_request
from docstore.storage.wal import _fsync_dir

if TYPE_CHECKING:
    from docstore.coordinator.coordinator import CoordinatorService

logger = logging.getLogger(__name__)


# Max docs/entries per RPC; batches are ALSO bounded by serialized bytes (see
# _batches_by_size), since a count alone cannot keep a body under the size limit.
_BATCH_DOCS = 200
# Room reserved in each request body for the JSON envelope around the items.
_BATCH_ENVELOPE_BYTES = 64 * 1024
# Source snapshot pages: documents per page (also bounded by bytes on the source).
_SNAPSHOT_PAGE_DOCS = 500


def _batches_by_size(
    items: list[dict[str, Any]], max_bytes: int, max_items: int = _BATCH_DOCS
) -> list[list[dict[str, Any]]]:
    """Split *items* so each batch's serialized size stays within *max_bytes*.

    Raises ``RuntimeError`` if a single item cannot fit in any request.
    """
    batches: list[list[dict[str, Any]]] = []
    batch: list[dict[str, Any]] = []
    size = 2  # "[]"
    for item in items:
        item_size = len(json.dumps(item, separators=(",", ":")).encode("utf-8")) + 1
        if item_size + 2 > max_bytes:
            raise RuntimeError(
                f"Item (doc_id={item.get('doc_id')}) is {item_size} bytes, larger than the "
                f"{max_bytes}-byte migration batch limit."
            )
        if batch and (size + item_size > max_bytes or len(batch) >= max_items):
            batches.append(batch)
            batch, size = [], 2
        batch.append(item)
        size += item_size
    if batch:
        batches.append(batch)
    return batches


class MigrationState(enum.StrEnum):
    PLANNED = "PLANNED"
    SNAPSHOT_COPY = "SNAPSHOT_COPY"
    CATCHUP = "CATCHUP"
    PAUSE = "PAUSE"
    FINAL_CATCHUP = "FINAL_CATCHUP"
    VERIFY = "VERIFY"
    CUTOVER = "CUTOVER"
    CLEANUP = "CLEANUP"
    DONE = "DONE"
    # Abort decided but not yet confirmed (source unpaused + destination purged).
    # Non-terminal: retried until both succeed, so a failed unpause can never leave
    # the source range paused forever.
    ABORTING = "ABORTING"
    ABORTED = "ABORTED"


_STALLED_STATES = (MigrationState.CUTOVER, MigrationState.CLEANUP, MigrationState.ABORTING)


class MigrationRecord(BaseModel):
    """Durable state record for an individual live range migration."""

    migration_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    from_shard: str
    to_shard: str
    range_start: int
    range_end: int
    snapshot_start_seq: int | None = None
    last_copied_doc_id: int | None = None
    last_caught_up_seq: int | None = None
    state: MigrationState = MigrationState.PLANNED
    final_cutover_epoch: int | None = None
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)
    docs_copied: int = 0
    catchup_ops_replayed: int = 0
    pause_duration_s: float = 0.0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


def compute_deterministic_checksum(docs: list[dict[str, Any]]) -> tuple[int, str]:
    """Calculate document count and canonical SHA-256 over documents sorted by doc_id."""
    sorted_docs = sorted(docs, key=lambda d: d["doc_id"])
    hasher = hashlib.sha256()
    for d in sorted_docs:
        canonical = json.dumps(d, sort_keys=True, separators=(",", ":"))
        hasher.update(canonical.encode("utf-8"))
    return len(sorted_docs), hasher.hexdigest()


class MigrationManager:
    """Manages creation, execution, and persistence of live range migrations."""

    def __init__(self, data_dir: str, cluster_secret: str) -> None:
        self.data_dir = Path(data_dir) / "migrations"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.cluster_secret = cluster_secret
        self.migrations: dict[str, MigrationRecord] = {}
        # Migrations currently driven by execute_migration (not to be retried concurrently).
        self._executing: set[str] = set()
        self._load_all()

    def _load_all(self) -> None:
        """Load all persisted migration records from disk.

        Fails closed: a record that cannot be parsed may describe a migration that
        already crossed cutover, and silently skipping it would lose the state needed
        to finish cleanup. Only a record that is readable JSON *and* clearly terminal
        (DONE / ABORTED) but fails schema validation is skipped with a warning.
        """
        for p in sorted(self.data_dir.glob("*.json")):
            try:
                with p.open("r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as exc:
                raise RuntimeError(
                    f"Corrupt migration record {p}: {exc}. Refusing to start: it may "
                    "describe an in-flight ownership transfer. Inspect or restore it."
                ) from exc
            try:
                rec = MigrationRecord.model_validate(data)
            except Exception as exc:
                state = data.get("state") if isinstance(data, dict) else None
                if state in (MigrationState.DONE.value, MigrationState.ABORTED.value):
                    logger.warning("Skipping invalid terminal migration record %s: %s", p, exc)
                    continue
                raise RuntimeError(
                    f"Invalid non-terminal migration record {p}: {exc}. Refusing to start."
                ) from exc
            self.migrations[rec.migration_id] = rec

    def save_record(self, record: MigrationRecord) -> None:
        """Atomically persist a migration record to disk."""
        record.updated_at = time.time()
        self.migrations[record.migration_id] = record
        path = self.data_dir / f"{record.migration_id}.json"
        tmp_path = path.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            f.write(record.model_dump_json(indent=2))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
        _fsync_dir(self.data_dir)

    def is_range_migrating(self, start: int, end: int) -> bool:
        """Check if an active (non-terminal) migration overlaps with [start, end)."""
        for m in self.migrations.values():
            if m.state not in (MigrationState.DONE, MigrationState.ABORTED):
                if max(start, m.range_start) < min(end, m.range_end):
                    return True
        return False

    def create_migration(
        self,
        from_shard: str,
        to_shard: str,
        start: int,
        end: int,
        coord: CoordinatorService,
    ) -> MigrationRecord:
        """Validate and create a new planned migration."""
        if from_shard == to_shard:
            raise ValueError("from_shard and to_shard must be distinct.")

        # 1. One active migration per key range
        if self.is_range_migrating(start, end):
            raise ValueError(f"An active migration already overlaps with range [{start}, {end}).")

        # 2. Check neither shard is degraded or mid-failover
        if coord.is_shard_degraded(from_shard):
            raise ValueError(f"Source shard {from_shard} is degraded; cannot start migration.")
        if coord.is_shard_degraded(to_shard):
            raise ValueError(f"Destination shard {to_shard} is degraded; cannot start migration.")
        if from_shard in coord.failover_in_progress or to_shard in coord.failover_in_progress:
            raise ValueError("One of the shards is currently undergoing failover.")

        # 3. Verify range exists on source shard
        cur_map = coord.map_mgr.shard_map
        owns_range = any(
            e.shard_id == from_shard and e.range.start <= start and end <= e.range.end
            for e in cur_map.entries
        )
        if not owns_range:
            raise ValueError(
                f"Source shard {from_shard} does not own the full range [{start}, {end})."
            )

        rec = MigrationRecord(
            from_shard=from_shard,
            to_shard=to_shard,
            range_start=start,
            range_end=end,
            state=MigrationState.PLANNED,
        )
        self.save_record(rec)
        return rec

    async def execute_migration(
        self,
        migration_id: str,
        coord: CoordinatorService,
        *,
        http_client: httpx.AsyncClient | None = None,
        on_phase_hook: Any = None,
    ) -> MigrationRecord:
        """Execute the migration state machine asynchronously."""
        rec = self.migrations.get(migration_id)
        if not rec:
            raise ValueError(f"Unknown migration_id {migration_id}")

        owns_client = False
        client = http_client
        if client is None:
            client = httpx.AsyncClient(timeout=10.0)
            owns_client = True

        async def _rpc(url: str, body: dict[str, Any]) -> dict[str, Any]:
            body_bytes = json.dumps(body, separators=(",", ":")).encode("utf-8")
            clean_url = url.removeprefix("http://").removeprefix("https://")
            target = f"http://{clean_url}"
            sig = sign_request(self.cluster_secret, "POST", target, body_bytes)
            headers = {"Content-Type": "application/json", CLUSTER_SIG_HEADER: sig}
            resp = await client.post(target, content=body_bytes, headers=headers)
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"RPC to {url} failed status={resp.status_code} body={resp.text}"
                )
            result_data: dict[str, Any] = resp.json()
            return result_data

        pause_start: float | None = None
        batch_bytes = (
            max(1024, int(getattr(coord.settings, "MAX_BODY_SIZE_BYTES", 2 * 1024 * 1024)))
            - _BATCH_ENVELOPE_BYTES
        )
        self._executing.add(rec.migration_id)

        try:
            cur_map = coord.map_mgr.shard_map
            src_entry = next((e for e in cur_map.entries if e.shard_id == rec.from_shard), None)
            dest_entry = next((e for e in cur_map.entries if e.shard_id == rec.to_shard), None)
            if not src_entry or not src_entry.primary:
                raise RuntimeError(f"Source shard {rec.from_shard} primary not found.")
            if not dest_entry or not dest_entry.primary:
                raise RuntimeError(f"Destination shard {rec.to_shard} primary not found.")

            src_url = src_entry.primary
            dest_url = dest_entry.primary
            # Topology this migration was planned against; re-validated before cutover.
            start_terms = (
                coord.shard_terms.get(rec.from_shard, 1),
                coord.shard_terms.get(rec.to_shard, 1),
            )

            # Check degraded/failover status
            def _check_abort_condition() -> None:
                if coord.is_shard_degraded(rec.from_shard) or coord.is_shard_degraded(rec.to_shard):
                    raise RuntimeError("Shard became degraded during migration; aborting.")
                if (
                    rec.from_shard in coord.failover_in_progress
                    or rec.to_shard in coord.failover_in_progress
                ):
                    raise RuntimeError("Failover occurred during migration; aborting.")

            _check_abort_condition()

            # ── 1. SNAPSHOT_COPY ──────────────────────────────────────────────
            rec.state = MigrationState.SNAPSHOT_COPY
            self.save_record(rec)
            logger.info("Migration %s: starting SNAPSHOT_COPY", rec.migration_id)

            # Cursor-paginated copy: bounded source memory and response size. The copy
            # is "fuzzy" (later pages may reflect writes after the first page), which is
            # safe because catch-up replays EVERY op after the first page's seq in order
            # (idempotent upserts/deletes), and VERIFY checks the final checksums.
            s_seq: int | None = None
            after_doc_id: int | None = None
            rec.docs_copied = 0
            while True:
                page = await _rpc(
                    f"{src_url}/internal/migration/snapshot",
                    {
                        "start": rec.range_start,
                        "end": rec.range_end,
                        "after_doc_id": after_doc_id,
                        "limit": _SNAPSHOT_PAGE_DOCS,
                        "max_bytes": batch_bytes,
                    },
                )
                if s_seq is None:
                    s_seq = int(page.get("snapshot_seq", 0))
                    rec.snapshot_start_seq = s_seq
                    rec.last_caught_up_seq = s_seq
                    self.save_record(rec)
                docs: list[dict[str, Any]] = page.get("documents", [])
                # Client idempotency records for this slice of the range travel with it
                # (sent with the page's first batch, or alone if the page has no docs).
                dedup = page.get("dedup") or {}
                batches = _batches_by_size(docs, batch_bytes) or ([[]] if dedup else [])
                # Ingest into destination via normal replicated path
                for i, batch in enumerate(batches):
                    await _rpc(
                        f"{dest_url}/internal/migration/apply-batch",
                        {
                            "migration_id": rec.migration_id,
                            "documents": batch,
                            "source_seq": s_seq,
                            **({"dedup": dedup} if i == 0 and dedup else {}),
                        },
                    )
                if docs:
                    after_doc_id = int(docs[-1]["doc_id"])
                    rec.docs_copied += len(docs)
                    rec.last_copied_doc_id = after_doc_id  # durable copy progress
                    self.save_record(rec)
                if page.get("done", True) or not docs:
                    break
            if s_seq is None:  # explicit: asserts are stripped under -O
                raise RuntimeError("s_seq is unavailable")

            if on_phase_hook:
                await on_phase_hook(MigrationState.SNAPSHOT_COPY)

            _check_abort_condition()

            # ── 2. CATCHUP ───────────────────────────────────────────────────
            rec.state = MigrationState.CATCHUP
            self.save_record(rec)
            logger.info("Migration %s: starting CATCHUP", rec.migration_id)

            current_cursor = s_seq
            catchup_deadline = time.monotonic() + getattr(
                coord.settings, "MIGRATION_CATCHUP_MAX_DURATION_S", 60.0
            )
            consecutive_no_progress = 0

            while time.monotonic() < catchup_deadline:
                _check_abort_condition()
                oplog_resp = await _rpc(
                    f"{src_url}/internal/migration/oplog",
                    {
                        "start": rec.range_start,
                        "end": rec.range_end,
                        "after_seq": current_cursor,
                        "limit": 500,
                    },
                )
                if oplog_resp.get("needs_snapshot") or oplog_resp.get("status") == "compacted":
                    raise RuntimeError(
                        f"Migration {rec.migration_id} oplog cursor {current_cursor} was "
                        "compacted away; new snapshot required."
                    )
                entries: list[dict[str, Any]] = oplog_resp.get("entries", [])
                src_last_seq: int = oplog_resp.get("last_seq", current_cursor)

                if entries:
                    for batch in _batches_by_size(entries, batch_bytes):
                        await _rpc(
                            f"{dest_url}/internal/migration/apply-oplog",
                            {"migration_id": rec.migration_id, "entries": batch},
                        )
                    rec.catchup_ops_replayed += len(entries)
                    next_cursor = max(e["seq"] for e in entries)
                    if next_cursor <= current_cursor:
                        consecutive_no_progress += 1
                        if consecutive_no_progress >= 5:
                            raise RuntimeError(
                                f"Migration {rec.migration_id} made no forward progress during "
                                f"catchup (cursor stalled at {current_cursor})"
                            )
                    else:
                        consecutive_no_progress = 0
                        current_cursor = next_cursor
                        rec.last_caught_up_seq = current_cursor
                        self.save_record(rec)
                else:
                    # No matching entries in this slice
                    if src_last_seq <= current_cursor or src_last_seq - current_cursor <= 2:
                        break
                    current_cursor = src_last_seq
                    rec.last_caught_up_seq = current_cursor
                    self.save_record(rec)
                    break

                if src_last_seq - current_cursor <= 2:
                    break

            if time.monotonic() >= catchup_deadline and (src_last_seq - current_cursor > 2):
                dur = getattr(coord.settings, "MIGRATION_CATCHUP_MAX_DURATION_S", 60.0)
                raise RuntimeError(
                    f"Migration {rec.migration_id} catch-up timed out after {dur}s without "
                    f"converging (lag={src_last_seq - current_cursor})"
                )

            _check_abort_condition()

            # ── 3. PAUSE ─────────────────────────────────────────────────────
            rec.state = MigrationState.PAUSE
            self.save_record(rec)
            logger.info("Migration %s: starting PAUSE", rec.migration_id)
            pause_start = time.monotonic()

            pause_resp = await _rpc(
                f"{src_url}/internal/migration/pause",
                {"start": rec.range_start, "end": rec.range_end},
            )
            final_h_seq: int = pause_resp.get("final_seq", current_cursor)

            # ── 4. FINAL_CATCHUP ─────────────────────────────────────────────
            rec.state = MigrationState.FINAL_CATCHUP
            self.save_record(rec)
            logger.info(
                "Migration %s: starting FINAL_CATCHUP to seq %d",
                rec.migration_id,
                final_h_seq,
            )

            final_dur = getattr(coord.settings, "MIGRATION_FINAL_CATCHUP_MAX_DURATION_S", 30.0)
            final_deadline = time.monotonic() + final_dur
            final_no_progress = 0

            while current_cursor < final_h_seq:
                _check_abort_condition()
                if time.monotonic() >= final_deadline:
                    raise RuntimeError(
                        f"Migration {rec.migration_id} final catchup timed out after {final_dur}s; "
                        f"cursor={current_cursor}, target={final_h_seq}"
                    )

                final_oplog_resp = await _rpc(
                    f"{src_url}/internal/migration/oplog",
                    {
                        "start": rec.range_start,
                        "end": rec.range_end,
                        "after_seq": current_cursor,
                        "limit": 1000,
                    },
                )
                if (
                    final_oplog_resp.get("needs_snapshot")
                    or final_oplog_resp.get("status") == "compacted"
                ):
                    raise RuntimeError(
                        f"Migration {rec.migration_id} final catchup cursor {current_cursor} "
                        "was compacted away; new snapshot required."
                    )
                final_entries = final_oplog_resp.get("entries", [])
                src_last_seq = final_oplog_resp.get("last_seq", current_cursor)
                if final_entries:
                    for batch in _batches_by_size(final_entries, batch_bytes):
                        await _rpc(
                            f"{dest_url}/internal/migration/apply-oplog",
                            {"migration_id": rec.migration_id, "entries": batch},
                        )
                    rec.catchup_ops_replayed += len(final_entries)
                    next_cursor = max(e["seq"] for e in final_entries)
                    if next_cursor <= current_cursor:
                        final_no_progress += 1
                        if final_no_progress >= 5:
                            raise RuntimeError(
                                f"Migration {rec.migration_id} final catchup made no forward "
                                f"progress (cursor stalled at {current_cursor})"
                            )
                    else:
                        final_no_progress = 0
                        current_cursor = next_cursor
                        rec.last_caught_up_seq = current_cursor
                        self.save_record(rec)
                elif src_last_seq >= final_h_seq:
                    current_cursor = final_h_seq
                    rec.last_caught_up_seq = final_h_seq
                    self.save_record(rec)
                    break
                else:
                    final_no_progress += 1
                    if final_no_progress >= 5:
                        raise RuntimeError(
                            f"Migration {rec.migration_id} final catchup made no progress: "
                            f"cursor={current_cursor}, target={final_h_seq}, "
                            f"src_last_seq={src_last_seq}"
                        )
                    if time.monotonic() >= final_deadline:
                        raise RuntimeError(
                            f"Migration {rec.migration_id} final catchup timed out after "
                            f"{final_dur}s; cursor={current_cursor}, target={final_h_seq}"
                        )
                    await asyncio.sleep(0.01)

            rec.last_caught_up_seq = max(rec.last_caught_up_seq or 0, current_cursor)
            self.save_record(rec)

            if final_h_seq > (rec.last_caught_up_seq or 0):
                raise RuntimeError(
                    f"Migration {rec.migration_id} catch-up did not converge: "
                    f"destination at seq {rec.last_caught_up_seq}, source paused at {final_h_seq}."
                )

            # ── 5. VERIFY + 6. CUTOVER, serialized against failover ─────────
            # Both shards' topology locks are held (sorted order: no deadlock) so no
            # failover can promote a different primary between verification and the map
            # flip. Everything the cutover depends on is re-validated inside the lock.
            lock_ids = sorted({rec.from_shard, rec.to_shard})
            locks = [coord.topology_lock(sid) for sid in lock_ids]
            for lk in locks:
                await lk.acquire()
            try:
                _check_abort_condition()
                cur_map = coord.map_mgr.shard_map
                now_src = next((e for e in cur_map.entries if e.shard_id == rec.from_shard), None)
                now_dest = next((e for e in cur_map.entries if e.shard_id == rec.to_shard), None)
                now_terms = (
                    coord.shard_terms.get(rec.from_shard, 1),
                    coord.shard_terms.get(rec.to_shard, 1),
                )
                if (
                    now_src is None
                    or now_dest is None
                    or now_src.primary != src_url
                    or now_dest.primary != dest_url
                    or now_dest.follower != dest_entry.follower
                    or now_terms != start_terms
                ):
                    raise RuntimeError(
                        "Topology changed during migration (primary or term moved); aborting "
                        "before cutover."
                    )

                rec.state = MigrationState.VERIFY
                self.save_record(rec)
                logger.info("Migration %s: starting VERIFY", rec.migration_id)

                src_chk = await _rpc(
                    f"{src_url}/internal/migration/checksum",
                    {"start": rec.range_start, "end": rec.range_end},
                )
                dest_chk = await _rpc(
                    f"{dest_url}/internal/migration/checksum",
                    {"start": rec.range_start, "end": rec.range_end},
                )

                if src_chk.get("doc_count") != dest_chk.get("doc_count") or src_chk.get(
                    "checksum"
                ) != dest_chk.get("checksum"):
                    raise RuntimeError(
                        f"Verification mismatch: source count={src_chk.get('doc_count')} "
                        f"dest count={dest_chk.get('doc_count')}, "
                        f"source checksum={src_chk.get('checksum')} "
                        f"dest checksum={dest_chk.get('checksum')}"
                    )

                # ── 6. CUTOVER (Point of No Return) ──────────────────────────
                # Persist the intent FIRST: if we crash after the map flips, recovery must
                # roll forward instead of aborting a migration whose ownership moved.
                rec.state = MigrationState.CUTOVER
                self.save_record(rec)
                try:
                    new_epoch = coord.bump_range_migration(
                        shard_id=rec.from_shard,
                        start=rec.range_start,
                        end=rec.range_end,
                        new_shard_id=rec.to_shard,
                    )
                except Exception as bump_exc:
                    # An exception does NOT prove the map is unchanged (e.g. the map file
                    # was replaced and a later step failed). Consult the authoritative
                    # persisted map; abort only if it provably still names the source.
                    committed = self._committed_cutover_epoch(coord, rec)
                    if committed is False:
                        rec.state = MigrationState.VERIFY  # provably not flipped: abortable
                        self.save_record(rec)
                        raise
                    if committed is None:
                        # Cannot tell: stay in CUTOVER so we roll forward, never abort.
                        raise RuntimeError(
                            f"Cutover outcome unknown after map update error: {bump_exc}"
                        ) from bump_exc
                    logger.warning(
                        "Migration %s: map update raised (%s) but the cutover IS committed "
                        "at epoch %d; rolling forward",
                        rec.migration_id,
                        bump_exc,
                        committed,
                    )
                    new_epoch = committed
                rec.final_cutover_epoch = new_epoch
                if pause_start is not None:
                    rec.pause_duration_s = time.monotonic() - pause_start
                self.save_record(rec)
                logger.info(
                    "Migration %s: CUTOVER committed at epoch %d", rec.migration_id, new_epoch
                )
            finally:
                for lk in reversed(locks):
                    lk.release()

            # Nothing irreversible happens until the map change is crash-durable: a visible
            # but non-durable map could revert after a power loss, after the source had
            # already given the range away. Raising here leaves the record in CUTOVER.
            coord.map_mgr.ensure_durable()
            await self._post_cutover(coord, rec, new_epoch, _rpc)

            # ── 7. CLEANUP ───────────────────────────────────────────────────
            rec.state = MigrationState.CLEANUP
            self.save_record(rec)
            logger.info("Migration %s: starting CLEANUP", rec.migration_id)

            await _rpc(
                f"{src_url}/internal/migration/cleanup",
                {"start": rec.range_start, "end": rec.range_end},
            )

            # ── 8. DONE ──────────────────────────────────────────────────────
            rec.state = MigrationState.DONE
            self.save_record(rec)
            logger.info("Migration %s: successfully COMPLETED", rec.migration_id)
            return rec

        except Exception as exc:
            logger.error("Migration %s failed: %s", rec.migration_id, exc)
            if rec.state != MigrationState.CUTOVER and rec.state != MigrationState.CLEANUP:
                # Abort safely before commit point
                rec.state = MigrationState.ABORTING
                rec.error = str(exc)
                if pause_start is not None:
                    rec.pause_duration_s = time.monotonic() - pause_start
                self.save_record(rec)
                abort_ok = True
                try:
                    # Unpause source
                    cur_map = coord.map_mgr.shard_map
                    src_entry = next(
                        (e for e in cur_map.entries if e.shard_id == rec.from_shard),
                        None,
                    )
                    if src_entry and src_entry.primary:
                        await _rpc(
                            f"{src_entry.primary}/internal/migration/unpause",
                            {"start": rec.range_start, "end": rec.range_end},
                        )
                except Exception as unpause_exc:
                    abort_ok = False
                    logger.warning("Failed to unpause source on abort: %s", unpause_exc)
                try:
                    # Abort staged data on destination
                    cur_map = coord.map_mgr.shard_map
                    dest_entry = next(
                        (e for e in cur_map.entries if e.shard_id == rec.to_shard),
                        None,
                    )
                    if dest_entry and dest_entry.primary:
                        await _rpc(
                            f"{dest_entry.primary}/internal/migration/abort",
                            {"start": rec.range_start, "end": rec.range_end},
                        )
                except Exception as abort_exc:
                    abort_ok = False
                    logger.warning("Failed to clean destination on abort: %s", abort_exc)
                if abort_ok:
                    rec.state = MigrationState.ABORTED
                    self.save_record(rec)
                else:
                    logger.warning(
                        "Migration %s left in ABORTING; the health monitor will retry",
                        rec.migration_id,
                    )
                raise
            else:
                # After cutover, roll forward (keep in CUTOVER or CLEANUP, do not abort)
                rec.error = str(exc)
                self.save_record(rec)
                raise
        finally:
            self._executing.discard(rec.migration_id)
            if owns_client:
                await client.aclose()

    @staticmethod
    def _committed_cutover_epoch(
        coord: CoordinatorService, rec: MigrationRecord
    ) -> int | bool | None:
        """Decide from the persisted map whether this migration's cutover is committed.

        Returns the destination epoch if the map gives the range to the destination,
        ``False`` if it provably still gives it to the source, ``None`` if unknown.
        """
        try:
            smap = coord.map_mgr.reload()
        except Exception as exc:
            logger.error("Cannot re-read shard map to resolve cutover outcome: %s", exc)
            return None
        for e in smap.entries:
            if e.range.start <= rec.range_start and rec.range_end <= e.range.end:
                if e.shard_id == rec.to_shard:
                    return e.ownership_epoch
                if e.shard_id == rec.from_shard:
                    return False
        return None

    async def _post_cutover(
        self, coord: CoordinatorService, rec: MigrationRecord, epoch: int, rpc: Any
    ) -> None:
        """Source release -> destination primary + follower activation.

        1. The source durably stops owning the range (on its follower too), so a router
           with a stale map can no longer read the old copy there (it is told
           OUT_OF_RANGE and refreshes) — no stale reads once the destination takes writes.
        2. The destination primary AND follower take ownership. The follower is
           mandatory: if the destination primary later fails, the promoted follower must
           own the range.
        Addresses are read from the CURRENT map (a failover or follower replacement may
        have changed them). Any failure leaves the record in CUTOVER for retry.
        """
        cur = coord.map_mgr.shard_map
        src = next((e for e in cur.entries if e.shard_id == rec.from_shard), None)
        dest = next((e for e in cur.entries if e.shard_id == rec.to_shard), None)
        if dest is None or not dest.primary:
            raise RuntimeError(
                f"Destination shard {rec.to_shard} primary entry missing from shard map"
            )
        if src is None or not src.primary:
            raise RuntimeError(
                f"Source shard {rec.from_shard} primary entry missing from shard map"
            )
        body = {"start": rec.range_start, "end": rec.range_end}
        await rpc(f"{src.primary}/internal/migration/release", body)
        cutover_body = {**body, "ownership_epoch": epoch}
        await rpc(f"{dest.primary}/internal/migration/cutover", cutover_body)
        if dest.follower:
            await rpc(f"{dest.follower}/internal/migration/cutover", cutover_body)

    async def retry_post_cutover(
        self,
        coord: CoordinatorService,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> list[MigrationRecord]:
        """Periodically drive stalled migrations to a terminal state.

        CUTOVER / CLEANUP roll forward to DONE; ABORTING finishes its abort (source
        unpause + destination purge) to ABORTED.
        """
        pending = [
            r
            for r in self.migrations.values()
            if r.state in _STALLED_STATES and r.migration_id not in self._executing
        ]
        if not pending:
            return []
        return await self.resume_or_recover_unfinished(
            coord, http_client=http_client, only_post_cutover=True
        )

    async def resume_or_recover_unfinished(
        self,
        coord: CoordinatorService,
        *,
        http_client: httpx.AsyncClient | None = None,
        only_post_cutover: bool = False,
    ) -> list[MigrationRecord]:
        """Scan and automatically recover or roll forward unfinished migrations on startup."""
        unfinished = [
            r
            for r in self.migrations.values()
            if r.state not in (MigrationState.DONE, MigrationState.ABORTED)
            and r.migration_id not in self._executing
            and (not only_post_cutover or r.state in _STALLED_STATES)
        ]
        if not unfinished:
            return []

        owns_client = False
        client = http_client
        if client is None:
            client = httpx.AsyncClient(timeout=10.0)
            owns_client = True

        async def _rpc(url: str, body: dict[str, Any]) -> dict[str, Any]:
            body_bytes = json.dumps(body, separators=(",", ":")).encode("utf-8")
            clean_url = url.removeprefix("http://").removeprefix("https://")
            target = f"http://{clean_url}"
            sig = sign_request(self.cluster_secret, "POST", target, body_bytes)
            headers = {"Content-Type": "application/json", CLUSTER_SIG_HEADER: sig}
            resp = await client.post(target, content=body_bytes, headers=headers)
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"RPC to {url} failed status={resp.status_code} body={resp.text}"
                )
            result_data: dict[str, Any] = resp.json()
            return result_data

        recovered_records = []
        try:
            for rec in unfinished:
                logger.info(
                    "Found unfinished migration %s in state %s; resuming recovery",
                    rec.migration_id,
                    rec.state,
                )
                cur_map = coord.map_mgr.shard_map
                src_entry = next((e for e in cur_map.entries if e.shard_id == rec.from_shard), None)
                dest_entry = next((e for e in cur_map.entries if e.shard_id == rec.to_shard), None)

                if rec.state in (MigrationState.CUTOVER, MigrationState.CLEANUP):
                    # Point of no return has passed: MUST roll forward!
                    logger.info(
                        "Migration %s is past cutover; rolling forward to DONE",
                        rec.migration_id,
                    )
                    covering = [
                        e
                        for e in cur_map.entries
                        if e.shard_id == rec.to_shard
                        and e.range.start <= rec.range_start
                        and rec.range_end <= e.range.end
                    ]
                    epoch = rec.final_cutover_epoch
                    if not epoch:
                        # Intent was logged but no epoch recorded: the flip may or may not
                        # have happened. Look at the map to find out.
                        if covering:
                            epoch = covering[0].ownership_epoch
                        else:
                            epoch = coord.bump_range_migration(
                                shard_id=rec.from_shard,
                                start=rec.range_start,
                                end=rec.range_end,
                                new_shard_id=rec.to_shard,
                            )
                        rec.final_cutover_epoch = epoch
                    rec.state = MigrationState.CLEANUP
                    self.save_record(rec)

                    recovery_errors: list[str] = []
                    try:
                        coord.map_mgr.ensure_durable()
                        await self._post_cutover(coord, rec, epoch, _rpc)
                    except Exception as exc:
                        err = f"Post-cutover activation failed: {exc}"
                        logger.warning("%s", err)
                        recovery_errors.append(err)

                    if recovery_errors:
                        pass  # never clean up the source before activation succeeded
                    elif not src_entry or not src_entry.primary:
                        err = f"Source shard {rec.from_shard} primary entry missing from shard map"
                        logger.warning("%s", err)
                        recovery_errors.append(err)
                    else:
                        try:
                            await _rpc(
                                f"{src_entry.primary}/internal/migration/cleanup",
                                {"start": rec.range_start, "end": rec.range_end},
                            )
                        except Exception as exc:
                            err = f"Cleanup RPC to src primary {src_entry.primary} failed: {exc}"
                            logger.warning("%s", err)
                            recovery_errors.append(err)

                    if recovery_errors:
                        rec.state = MigrationState.CLEANUP
                        rec.error = "; ".join(recovery_errors)
                        self.save_record(rec)
                        logger.warning(
                            "Migration %s recovery incomplete: %s; retained in CLEANUP for retry",
                            rec.migration_id,
                            rec.error,
                        )
                    else:
                        rec.state = MigrationState.DONE
                        rec.error = None
                        self.save_record(rec)
                        recovered_records.append(rec)
                        logger.info(
                            "Migration %s successfully rolled forward to DONE",
                            rec.migration_id,
                        )

                else:
                    # Before cutover: safely abort
                    logger.info("Migration %s is before cutover; safely aborting", rec.migration_id)
                    abort_errors: list[str] = []

                    if not src_entry or not src_entry.primary:
                        err = f"Source shard {rec.from_shard} primary entry missing from shard map"
                        logger.warning("%s", err)
                        abort_errors.append(err)
                    else:
                        try:
                            await _rpc(
                                f"{src_entry.primary}/internal/migration/unpause",
                                {"start": rec.range_start, "end": rec.range_end},
                            )
                        except Exception as exc:
                            err = f"Unpause RPC to src primary {src_entry.primary} failed: {exc}"
                            logger.warning("%s", err)
                            abort_errors.append(err)

                    if not dest_entry or not dest_entry.primary:
                        err = (
                            f"Destination shard {rec.to_shard} primary entry missing from shard map"
                        )
                        logger.warning("%s", err)
                        abort_errors.append(err)
                    else:
                        try:
                            await _rpc(
                                f"{dest_entry.primary}/internal/migration/abort",
                                {"start": rec.range_start, "end": rec.range_end},
                            )
                        except Exception as exc:
                            err = f"Abort RPC to dest primary {dest_entry.primary} failed: {exc}"
                            logger.warning("%s", err)
                            abort_errors.append(err)

                    if abort_errors:
                        rec.error = "; ".join(abort_errors)
                        self.save_record(rec)
                        logger.warning(
                            "Migration %s abort incomplete: %s; retained in %s for retry",
                            rec.migration_id,
                            rec.error,
                            rec.state,
                        )
                    else:
                        rec.state = MigrationState.ABORTED
                        rec.error = "Aborted automatically on coordinator restart (pre-cutover)."
                        self.save_record(rec)
                        recovered_records.append(rec)
                        logger.info("Migration %s aborted successfully", rec.migration_id)

        finally:
            if owns_client:
                await client.aclose()

        return recovered_records
