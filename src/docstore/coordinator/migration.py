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

from docstore.common.auth import CLUSTER_SIG_HEADER, compute_signature

if TYPE_CHECKING:
    from docstore.coordinator.coordinator import CoordinatorService

logger = logging.getLogger(__name__)


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
    ABORTED = "ABORTED"


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
        self._load_all()

    def _load_all(self) -> None:
        """Load all persisted migration records from disk."""
        for p in self.data_dir.glob("*.json"):
            try:
                with p.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    rec = MigrationRecord.model_validate(data)
                    self.migrations[rec.migration_id] = rec
            except Exception as exc:
                logger.warning("Failed to load migration record from %s: %s", p, exc)

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
            sig = compute_signature(self.cluster_secret, body_bytes)
            headers = {"Content-Type": "application/json", CLUSTER_SIG_HEADER: sig}
            clean_url = url.removeprefix("http://").removeprefix("https://")
            resp = await client.post(f"http://{clean_url}", content=body_bytes, headers=headers)
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"RPC to {url} failed status={resp.status_code} body={resp.text}"
                )
            result_data: dict[str, Any] = resp.json()
            return result_data

        pause_start: float | None = None

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

            snap_resp = await _rpc(
                f"{src_url}/internal/migration/snapshot",
                {"start": rec.range_start, "end": rec.range_end},
            )
            s_seq: int = snap_resp.get("snapshot_seq", 0)
            docs: list[dict[str, Any]] = snap_resp.get("documents", [])
            rec.snapshot_start_seq = s_seq
            rec.last_caught_up_seq = s_seq
            rec.docs_copied = len(docs)
            if docs:
                rec.last_copied_doc_id = max(d["doc_id"] for d in docs)
            self.save_record(rec)

            # Ingest into destination via normal replicated path
            if docs:
                await _rpc(
                    f"{dest_url}/internal/migration/apply-batch",
                    {
                        "migration_id": rec.migration_id,
                        "documents": docs,
                        "source_seq": s_seq,
                    },
                )

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
                    await _rpc(
                        f"{dest_url}/internal/migration/apply-oplog",
                        {
                            "migration_id": rec.migration_id,
                            "entries": entries,
                        },
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
                    await _rpc(
                        f"{dest_url}/internal/migration/apply-oplog",
                        {
                            "migration_id": rec.migration_id,
                            "entries": final_entries,
                        },
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

            # ── 5. VERIFY ────────────────────────────────────────────────────
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

            # ── 6. CUTOVER (Point of No Return) ──────────────────────────────
            new_epoch = coord.bump_range_migration(
                shard_id=rec.from_shard,
                start=rec.range_start,
                end=rec.range_end,
                new_shard_id=rec.to_shard,
            )
            rec.state = MigrationState.CUTOVER
            rec.final_cutover_epoch = new_epoch
            if pause_start is not None:
                rec.pause_duration_s = time.monotonic() - pause_start
            self.save_record(rec)
            logger.info("Migration %s: CUTOVER committed at epoch %d", rec.migration_id, new_epoch)

            # Activate range on destination
            await _rpc(
                f"{dest_url}/internal/migration/cutover",
                {
                    "start": rec.range_start,
                    "end": rec.range_end,
                    "ownership_epoch": new_epoch,
                },
            )
            # Also notify destination follower if present
            if dest_entry.follower:
                try:
                    await _rpc(
                        f"{dest_entry.follower}/internal/migration/cutover",
                        {
                            "start": rec.range_start,
                            "end": rec.range_end,
                            "ownership_epoch": new_epoch,
                        },
                    )
                except Exception as exc:
                    logger.warning("Failed to notify destination follower of cutover: %s", exc)

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
                rec.state = MigrationState.ABORTED
                rec.error = str(exc)
                if pause_start is not None:
                    rec.pause_duration_s = time.monotonic() - pause_start
                self.save_record(rec)
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
                    logger.warning("Failed to clean destination on abort: %s", abort_exc)
                raise
            else:
                # After cutover, roll forward (keep in CUTOVER or CLEANUP, do not abort)
                rec.error = str(exc)
                self.save_record(rec)
                raise
        finally:
            if owns_client:
                await client.aclose()

    async def resume_or_recover_unfinished(
        self,
        coord: CoordinatorService,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> list[MigrationRecord]:
        """Scan and automatically recover or roll forward unfinished migrations on startup."""
        unfinished = [
            r
            for r in self.migrations.values()
            if r.state not in (MigrationState.DONE, MigrationState.ABORTED)
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
            sig = compute_signature(self.cluster_secret, body_bytes)
            headers = {"Content-Type": "application/json", CLUSTER_SIG_HEADER: sig}
            clean_url = url.removeprefix("http://").removeprefix("https://")
            resp = await client.post(f"http://{clean_url}", content=body_bytes, headers=headers)
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
                    epoch = rec.final_cutover_epoch
                    if not epoch:
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

                    if not dest_entry or not dest_entry.primary:
                        err = (
                            f"Destination shard {rec.to_shard} primary entry missing from shard map"
                        )
                        logger.warning("%s", err)
                        recovery_errors.append(err)
                    else:
                        try:
                            await _rpc(
                                f"{dest_entry.primary}/internal/migration/cutover",
                                {
                                    "start": rec.range_start,
                                    "end": rec.range_end,
                                    "ownership_epoch": epoch,
                                },
                            )
                        except Exception as exc:
                            err = f"Cutover RPC to dest primary {dest_entry.primary} failed: {exc}"
                            logger.warning("%s", err)
                            recovery_errors.append(err)

                    if dest_entry and dest_entry.follower:
                        try:
                            await _rpc(
                                f"{dest_entry.follower}/internal/migration/cutover",
                                {
                                    "start": rec.range_start,
                                    "end": rec.range_end,
                                    "ownership_epoch": epoch,
                                },
                            )
                        except Exception as exc:
                            err = (
                                f"Cutover RPC to dest follower {dest_entry.follower} failed: {exc}"
                            )
                            logger.warning("%s", err)
                            recovery_errors.append(err)

                    if not src_entry or not src_entry.primary:
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
