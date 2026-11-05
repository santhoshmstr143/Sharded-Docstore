"""
Coordinator service logic — Module 6.

Responsibilities:
- Owns the persistent shard map and epoch history log
- Two version concepts: global map_version and per-range ownership_epoch
- Bump rules: on failover bump epoch of every range of the affected shard + shard term;
  on migration bump the migrated range's epoch
- Node registration: assigns primary / follower role and initial lease
- Heartbeat tracking with monotonic clock: failure detection (D=5.0s)
- Lease management: granted to primary for L=3.0s, renewed every H=1.0s
- Safety margin: replacement primary cannot write while old lease may be valid (expiry + M=0.5s)
- Shard degraded status: degraded if shard has no SYNCED follower
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from docstore.common.auth import CLUSTER_SIG_HEADER, compute_signature
from docstore.common.config import Settings
from docstore.common.errors import InvalidQuery, StaleTerm
from docstore.common.logging import get_logger
from docstore.common.models import (
    Heartbeat,
    LeaseGrant,
    NodeRegistration,
    NodeRole,
    RegistrationResponse,
    ReplicaState,
    ShardMap,
    ShardMapEntry,
    ShardRange,
)
from docstore.router.shard_map import ShardMapManager, create_default_3_shard_map
from docstore.storage.wal import _fsync_dir as _wal_fsync_dir

logger = get_logger(__name__)


def normalize_url(url: str | None) -> str:
    """Normalize a node address URL for robust host/port comparisons."""
    if not url:
        return ""
    clean = url.strip()
    clean = clean.removeprefix("http://").removeprefix("https://").rstrip("/")
    return clean.lower()


def _fsync_dir(dir_path: Path) -> None:
    """Flush directory metadata to disk; genuine failures propagate (see wal._fsync_dir)."""
    _wal_fsync_dir(dir_path)


class _DurableShardTerms(dict[str, int]):
    """In-memory term dictionary with synchronous, durable atomic persistence on disk."""

    def __init__(
        self,
        initial: dict[str, int],
        on_persist: Callable[[], None] | None = None,
    ) -> None:
        super().__init__(initial)
        self._on_persist = on_persist

    def __setitem__(self, key: str, value: int) -> None:
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            raise ValueError(f"Shard term must be a positive integer >= 1, got {value!r}")
        old_val = self.get(key)
        super().__setitem__(key, value)
        if self._on_persist is not None:
            try:
                self._on_persist()
            except Exception:
                if old_val is not None:
                    super().__setitem__(key, old_val)
                else:
                    super().pop(key, None)
                raise

    def update(self, *args: Any, **kwargs: Any) -> None:
        old_state = dict(self)
        temp = dict(*args, **kwargs)
        for k, v in temp.items():
            if not isinstance(v, int) or isinstance(v, bool) or v < 1:
                raise ValueError(f"Shard term must be a positive integer >= 1, got {v!r}")
            super().__setitem__(k, v)
        if self._on_persist is not None:
            try:
                self._on_persist()
            except Exception:
                self.clear()
                super().update(old_state)
                raise


@dataclass
class RegisteredNode:
    node_id: str
    shard_id: str
    address: str
    role: NodeRole
    term: int
    last_seen_monotonic: float = field(default_factory=time.monotonic)


@dataclass
class ActiveLease:
    node_id: str
    shard_id: str
    term: int
    granted_at_wall: float
    expires_at_wall: float
    expires_at_monotonic: float

    def remaining(self, now: float) -> float:
        return max(0.0, self.expires_at_monotonic - now)


class CoordinatorService:
    """Core state and business logic of the Coordinator node."""

    def __init__(
        self,
        settings: Settings,
        map_path: Path | str | None = None,
        shard_map_path: Path | str | None = None,
        terms_path: Path | str | None = None,
    ) -> None:
        self.settings = settings
        chosen_path = shard_map_path or map_path
        target_path = Path(chosen_path) if chosen_path else Path(settings.SHARD_MAP_PATH)
        self.map_mgr = ShardMapManager(target_path)

        # Initialize shard map if not present
        if not target_path.exists():
            default_map = create_default_3_shard_map()
            self.map_mgr.save(default_map)
        else:
            self.map_mgr.reload()

        self.terms_path = Path(terms_path) if terms_path else target_path.parent / "terms.json"
        recovered_terms = self._load_terms(self.terms_path)

        self.nodes: dict[str, RegisteredNode] = {}
        self.leases: dict[str, ActiveLease] = {}  # shard_id -> ActiveLease
        self.replica_states: dict[str, ReplicaState] = {}  # shard_id -> ReplicaState
        self.failover_metrics: list[dict[str, Any]] = []
        self.failover_in_progress: set[str] = set()
        # shard_id -> (new_primary_addr, old_primary_addr): promotions whose shard-map
        # change is committed but which the follower never confirmed. Backed by the
        # durable failover-intent file so a coordinator restart can finish them.
        self._unconfirmed_promotions: dict[str, tuple[str, str | None]] = {}
        # Per-shard topology lock: failover and migration cutover never interleave.
        self._topology_locks: dict[str, asyncio.Lock] = {}
        self._last_headless_attempt: dict[str, float] = {}
        self._started_at_monotonic: float = time.monotonic()
        self._recovery_quarantine_until_monotonic: float = (
            self._started_at_monotonic + settings.LEASE_DURATION_S + settings.SAFETY_MARGIN_S
        )

        from docstore.coordinator.migration import MigrationManager

        self.migration_mgr = MigrationManager(settings.DATA_DIR, settings.CLUSTER_SECRET)

        # Initialize shard terms with recovered terms or default 1
        initial_terms: dict[str, int] = {}
        for entry in self.map_mgr.shard_map.entries:
            initial_terms[entry.shard_id] = recovered_terms.get(entry.shard_id, 1)
            self.replica_states[entry.shard_id] = ReplicaState.DOWN

        for shard_id, term in recovered_terms.items():
            if shard_id not in initial_terms:
                initial_terms[shard_id] = term

        self.shard_terms: dict[str, int] = _DurableShardTerms(
            initial_terms, on_persist=lambda: self._persist_terms()
        )

        # Ensure terms are persisted if file did not exist or new shards were added
        if not self.terms_path.exists() or len(recovered_terms) != len(self.shard_terms):
            self._persist_terms()

        self.intents_path = target_path.parent / "failover_intents.json"
        self._failover_intents: dict[str, dict[str, Any]] = self._load_intents()
        self._recover_failover_intents()

    # ── Durable failover intents (BUG 2) ──────────────────────────────────────

    def _load_intents(self) -> dict[str, dict[str, Any]]:
        if not self.intents_path.exists():
            return {}
        try:
            data = json.loads(self.intents_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise ValueError(f"Corrupt failover intent file {self.intents_path}: {exc}") from exc
        if not isinstance(data, dict):
            raise ValueError(f"Corrupt failover intent file {self.intents_path}")
        return data

    def _persist_intents(self) -> None:
        self.intents_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.intents_path.parent / f".tmp.{uuid.uuid4().hex}.failover_intents.json"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._failover_intents, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.intents_path)
            _fsync_dir(self.intents_path.parent)
        except Exception:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            raise

    def _set_intent(self, shard_id: str, intent: dict[str, Any] | None) -> None:
        old = self._failover_intents.get(shard_id)
        if intent is None:
            self._failover_intents.pop(shard_id, None)
        else:
            self._failover_intents[shard_id] = intent
        try:
            self._persist_intents()
        except Exception:
            if old is None:
                self._failover_intents.pop(shard_id, None)
            else:
                self._failover_intents[shard_id] = old
            raise

    def _map_names_primary(self, shard_id: str, addr: str) -> bool:
        return any(
            e.shard_id == shard_id and normalize_url(e.primary) == normalize_url(addr)
            for e in self.map_mgr.shard_map.entries
        )

    def _recover_failover_intents(self) -> None:
        """Finish or discard failovers that were in flight when the coordinator stopped."""
        for shard_id, intent in list(self._failover_intents.items()):
            new_primary = intent.get("new_primary")
            old_primary = intent.get("old_primary")
            target_term = int(intent.get("target_term", 0))
            if (
                isinstance(new_primary, str)
                and self._map_names_primary(shard_id, new_primary)
                and self.shard_terms.get(shard_id, 1) >= target_term
            ):
                # Map already names the new primary: the promotion MUST be completed.
                logger.warning(
                    "Recovered unfinished failover of shard %s: promotion of %s pending",
                    shard_id,
                    new_primary,
                )
                self._unconfirmed_promotions[shard_id] = (
                    new_primary,
                    old_primary if isinstance(old_primary, str) else None,
                )
            else:
                # The map never changed: nothing to finish. Failure detection re-triggers
                # failover if the primary is still gone (a bumped term just fences it).
                logger.warning(
                    "Discarding failover intent for shard %s: map change was never committed",
                    shard_id,
                )
                self._set_intent(shard_id, None)

    def topology_lock(self, shard_id: str) -> asyncio.Lock:
        lock = self._topology_locks.get(shard_id)
        if lock is None:
            lock = asyncio.Lock()
            self._topology_locks[shard_id] = lock
        return lock

    def _build_promote_body(
        self, shard_id: str, new_primary: str, old_primary: str | None
    ) -> dict[str, Any]:
        """Promotion request from the CURRENT map/term, with a freshly granted lease."""
        entries = [e for e in self.map_mgr.shard_map.entries if e.shard_id == shard_id]
        term = self.shard_terms.get(shard_id, 1)
        now_mono = time.monotonic()
        now_wall = time.time()
        promoted_node_id = f"{shard_id}-promoted"
        for n in self.nodes.values():
            if n.shard_id == shard_id and normalize_url(n.address) == normalize_url(new_primary):
                promoted_node_id = n.node_id
                n.role = NodeRole.PRIMARY
                n.term = term
                n.last_seen_monotonic = now_mono
            elif (
                old_primary
                and n.shard_id == shard_id
                and normalize_url(n.address) == normalize_url(old_primary)
            ):
                n.role = NodeRole.FOLLOWER
        lease = LeaseGrant(
            node_id=promoted_node_id,
            shard_id=shard_id,
            term=term,
            granted_at=now_wall,
            expires_at=now_wall + self.settings.LEASE_DURATION_S,
            map_version=self.map_mgr.shard_map.map_version,
        )
        self.leases[shard_id] = ActiveLease(
            node_id=promoted_node_id,
            shard_id=shard_id,
            term=term,
            granted_at_wall=now_wall,
            expires_at_wall=lease.expires_at,
            expires_at_monotonic=now_mono + self.settings.LEASE_DURATION_S,
        )
        return {
            "shard_id": shard_id,
            "term": term,
            "ownership_epochs": {
                f"{e.range.start}-{e.range.end}": e.ownership_epoch for e in entries
            },
            # Authoritative ownership from the map: a follower that missed a migration
            # cutover notification still gets the right ranges on promotion.
            "ranges": [
                {"start": e.range.start, "end": e.range.end, "ownership_epoch": e.ownership_epoch}
                for e in sorted(entries, key=lambda e: e.range.start)
            ],
            "lease": lease.model_dump(),
            "follower_url": old_primary,
        }

    def _load_terms(self, path: Path) -> dict[str, int]:
        """Load and strictly validate shard terms metadata from disk.

        Raises ValueError if corrupted, unparseable, or containing invalid terms.
        """
        if not path.exists():
            return {}
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as exc:
            raise ValueError(f"Corrupted or unreadable terms metadata file {path}: {exc}") from exc

        if not isinstance(data, dict):
            raise ValueError(
                f"Corrupted terms metadata in {path}: expected JSON object, "
                f"got {type(data).__name__}"
            )

        terms: dict[str, int] = {}
        for shard_id, term_val in data.items():
            if not isinstance(shard_id, str) or not shard_id:
                raise ValueError(f"Invalid shard_id in terms metadata: {shard_id!r}")
            if not isinstance(term_val, int) or isinstance(term_val, bool) or term_val < 1:
                raise ValueError(
                    f"Invalid term value for shard {shard_id!r} in terms metadata: {term_val!r}"
                )
            terms[shard_id] = term_val
        return terms

    def _persist_terms(self) -> None:
        """Durably persist shard terms to disk using atomic file replacement and fsync."""
        self.terms_path.parent.mkdir(parents=True, exist_ok=True)
        temp_file = self.terms_path.parent / f".tmp.{uuid.uuid4().hex}.terms.json"
        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(dict(self.shard_terms), f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_file, self.terms_path)
            _fsync_dir(self.terms_path.parent)
        except Exception:
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except OSError:
                    pass
            raise

    def is_shard_degraded(self, shard_id: str) -> bool:
        """A shard is degraded if its follower is not in SYNCED state."""
        return self.replica_states.get(shard_id, ReplicaState.DOWN) != ReplicaState.SYNCED

    async def resume_unfinished_migrations(
        self, http_client: httpx.AsyncClient | None = None
    ) -> list[Any]:
        """Resume or abort unfinished migrations found upon coordinator startup."""
        return await self.migration_mgr.resume_or_recover_unfinished(self, http_client=http_client)

    # ── Shard Map & Epoch Management ──────────────────────────────────────────

    @property
    def shard_map(self) -> ShardMap:
        return self.map_mgr.shard_map

    def bump_shard_failover(
        self,
        shard_id: str,
        new_primary_url: str | None = None,
        new_follower_url: str | None = None,
    ) -> int:
        """Bump ownership_epoch of EVERY range of shard_id and bump shard term.

        Returns the new term.
        """
        current_map = self.map_mgr.shard_map
        new_entries = []
        found = False

        for entry in current_map.entries:
            if entry.shard_id == shard_id:
                found = True
                update_dict: dict[str, Any] = {
                    "ownership_epoch": entry.ownership_epoch + 1,
                    "map_version": current_map.map_version + 1,
                }
                if new_primary_url is not None:
                    old_primary = entry.primary
                    update_dict["primary"] = new_primary_url
                    if new_follower_url is not None:
                        update_dict["follower"] = new_follower_url
                    elif entry.follower == new_primary_url:
                        update_dict["follower"] = old_primary
                new_entries.append(entry.model_copy(update=update_dict))
            else:
                new_entries.append(
                    entry.model_copy(update={"map_version": current_map.map_version + 1})
                )

        if not found:
            raise ValueError(f"Unknown shard {shard_id!r}")

        self.shard_terms[shard_id] = self.shard_terms.get(shard_id, 1) + 1
        new_map = ShardMap(map_version=current_map.map_version + 1, entries=new_entries)
        self.map_mgr.save(new_map)

        logger.info(
            "Bumped failover epochs for shard %s: term=%d map_v=%d",
            shard_id,
            self.shard_terms[shard_id],
            new_map.map_version,
        )
        return self.shard_terms[shard_id]

    async def trigger_failover(
        self,
        shard_id: str,
        *,
        http_client: httpx.AsyncClient | None = None,
        crash_time: float | None = None,
    ) -> bool:
        """Execute step-by-step failover and promotion for a shard (Module 8)."""
        if shard_id in self.failover_in_progress:
            logger.warning(
                "Failover already in progress for shard %s; double failover blocked",
                shard_id,
            )
            return False

        current_map = self.map_mgr.shard_map
        entries = [e for e in current_map.entries if e.shard_id == shard_id]
        if not entries:
            logger.warning("Failover requested for unknown shard %s", shard_id)
            return False

        entry = entries[0]
        primary_addr = entry.primary
        follower_addr = entry.follower
        if not follower_addr:
            logger.warning("Failover for shard %s aborted: no follower configured", shard_id)
            return False

        self.failover_in_progress.add(shard_id)
        try:
            now_wall = time.time()
            detected_at = now_wall

            # Step 2: Verify follower is alive
            owns_client = False
            client = http_client
            if client is None:
                client = httpx.AsyncClient(timeout=self.settings.REPLICATION_TIMEOUT_S)
                owns_client = True

            try:
                clean_follower = follower_addr.removeprefix("http://").removeprefix("https://")
                url = f"http://{clean_follower}/internal/status"
                sig = compute_signature(self.settings.CLUSTER_SECRET, b"")
                headers = {CLUSTER_SIG_HEADER: sig}
                resp = await client.get(url, headers=headers)
                if resp.status_code != 200:
                    logger.warning(
                        "Failover for shard %s failed: follower returned status %d",
                        shard_id,
                        resp.status_code,
                    )
                    return False
            except Exception as exc:
                logger.warning(
                    "Failover for shard %s failed: follower unreachable: %s "
                    "(single-node failure only)",
                    shard_id,
                    exc,
                )
                return False
            finally:
                if owns_client:
                    await client.aclose()

            # Step 4: Wait until old primary's lease expired + safety margin M
            existing_lease = self.leases.get(shard_id)
            wait_s = 0.0
            if existing_lease:
                safety_cutoff = existing_lease.expires_at_monotonic + self.settings.SAFETY_MARGIN_S
                wait_s = max(0.0, safety_cutoff - time.monotonic())
            else:
                # Coordinator restart: wait for quarantine window to elapse
                # so any old lease is guaranteed to have expired
                wait_s = max(0.0, self._recovery_quarantine_until_monotonic - time.monotonic())

            if wait_s > 0:
                logger.info("Safety wait for old primary lease expiry + M: %.2fs", wait_s)
                await asyncio.sleep(wait_s)

            async with self.topology_lock(shard_id):
                # Re-validate under the lock: a concurrent topology change (e.g. a
                # registration-driven address change) invalidates this failover.
                if not self._map_names_primary(shard_id, primary_addr):
                    logger.warning(
                        "Failover for shard %s aborted: primary changed to another node", shard_id
                    )
                    return False

                # Step 5 & 6: durable intent FIRST, then bump term/epochs and persist map.
                # A crash anywhere after this point is finished (or discarded) on restart.
                self._set_intent(
                    shard_id,
                    {
                        "new_primary": follower_addr,
                        "old_primary": primary_addr,
                        "target_term": self.shard_terms.get(shard_id, 1) + 1,
                        "created_at": time.time(),
                    },
                )
                try:
                    new_term = self.bump_shard_failover(
                        shard_id,
                        new_primary_url=follower_addr,
                        new_follower_url=primary_addr,
                    )
                except Exception:
                    # The bump failed before or during the map write. If the map did not
                    # change, drop the intent; otherwise it stays for recovery.
                    if not self._map_names_primary(shard_id, follower_addr):
                        self._set_intent(shard_id, None)
                    raise
                promoted_map = self.map_mgr.shard_map

                # Step 7 & 8: grant a fresh lease and promote the follower.
                promote_client = http_client
                owns_promote_client = False
                if promote_client is None:
                    promote_client = httpx.AsyncClient(
                        timeout=self.settings.REPLICATION_TIMEOUT_S + 2.0
                    )
                    owns_promote_client = True
                req_body = self._build_promote_body(shard_id, follower_addr, primary_addr)
                try:
                    promoted_ok = await self._send_promote(
                        promote_client, follower_addr, req_body, attempts=3
                    )
                finally:
                    if owns_promote_client:
                        await promote_client.aclose()
                if not promoted_ok:
                    # The map already names the follower as primary. Keep retrying from
                    # the health monitor (and after restarts, via the durable intent).
                    self._unconfirmed_promotions[shard_id] = (follower_addr, primary_addr)
                    logger.error("Promotion of %s not confirmed; will keep retrying", follower_addr)
                    return False
                self._set_intent(shard_id, None)

            # Step 9: Set replica state to DOWN (Degraded mode)
            self.replica_states[shard_id] = ReplicaState.DOWN

            promoted_time = time.time()
            self.failover_metrics.append(
                {
                    "shard_id": shard_id,
                    "crash_timestamp": crash_time
                    or (detected_at - self.settings.FAILURE_TIMEOUT_S),
                    "detection_timestamp": detected_at,
                    "promotion_timestamp": promoted_time,
                    "first_read_timestamp": None,
                    "rejoin_timestamp": None,
                    "synced_timestamp": None,
                    "first_write_timestamp": None,
                }
            )

            logger.info(
                "Failover successful: shard %s promoted %s to PRIMARY in term %d "
                "(map_v=%d, degraded=True)",
                shard_id,
                follower_addr,
                new_term,
                promoted_map.map_version,
            )
            return True
        finally:
            self.failover_in_progress.discard(shard_id)

    async def _send_promote(
        self,
        client: httpx.AsyncClient,
        follower_addr: str,
        body: dict[str, Any],
        *,
        attempts: int = 1,
    ) -> bool:
        """POST /internal/promote (idempotent for the same term) with bounded retries."""
        clean = follower_addr.removeprefix("http://").removeprefix("https://")
        raw = json.dumps(body, separators=(",", ":")).encode("utf-8")
        sig = compute_signature(self.settings.CLUSTER_SECRET, raw)
        headers = {"Content-Type": "application/json", CLUSTER_SIG_HEADER: sig}
        for attempt in range(attempts):
            try:
                resp = await client.post(
                    f"http://{clean}/internal/promote", content=raw, headers=headers
                )
                if resp.status_code == 200:
                    return True
                logger.error(
                    "Promotion RPC to %s returned %d: %s",
                    follower_addr,
                    resp.status_code,
                    resp.text,
                )
                if resp.status_code == 409:
                    return False  # stale term: retrying cannot help
            except Exception as exc:
                logger.warning("Promotion RPC to %s failed: %s", follower_addr, exc)
            if attempt + 1 < attempts:
                await asyncio.sleep(0.2 * (attempt + 1))
        return False

    async def _retry_unconfirmed_promotions(self, http_client: httpx.AsyncClient | None) -> None:
        for shard_id, (addr, old_primary) in list(self._unconfirmed_promotions.items()):
            if shard_id in self.failover_in_progress:
                continue
            if shard_id not in self.leases and time.monotonic() < (
                self._recovery_quarantine_until_monotonic
            ):
                # After a coordinator restart an old lease may still be live somewhere:
                # never hand out primary authority before the quarantine elapses.
                continue
            client = http_client or httpx.AsyncClient(
                timeout=self.settings.REPLICATION_TIMEOUT_S + 2.0
            )
            try:
                async with self.topology_lock(shard_id):
                    body = self._build_promote_body(shard_id, addr, old_primary)
                    ok = await self._send_promote(client, addr, body)
                    if ok:
                        self._set_intent(shard_id, None)
            finally:
                if http_client is None:
                    await client.aclose()
            if ok:
                del self._unconfirmed_promotions[shard_id]
                self.replica_states[shard_id] = ReplicaState.DOWN
                logger.info("Deferred promotion of %s for shard %s confirmed", addr, shard_id)

    async def check_and_trigger_failover(
        self,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> list[str]:
        """Examine nodes and trigger promotion if primary has not sent heartbeats within D."""
        await self._retry_unconfirmed_promotions(http_client)
        now_mono = time.monotonic()
        promoted: list[str] = []

        seen: set[str] = set()
        for entry in self.map_mgr.shard_map.entries:
            shard_id = entry.shard_id
            if shard_id in seen:
                continue  # a shard owning several ranges is checked once
            seen.add(shard_id)
            if shard_id in self.failover_in_progress or shard_id in self._unconfirmed_promotions:
                continue

            primary_node = None
            for n in self.nodes.values():
                if n.shard_id == shard_id and n.role == NodeRole.PRIMARY:
                    primary_node = n
                    break

            if primary_node is None and entry.follower:
                # Headless shard: no node holds primary authority, and we have positive
                # evidence of it (never act merely because nobody registered at all):
                #  * the incumbent restarted and re-registered as a follower (nodes that
                #    recover from disk never resume authority on their own), or
                #  * another node of this shard is registered and alive, yet no primary
                #    has registered within D of coordinator start.
                # Promote the follower; the restarted node rejoins it via resync.
                shard_nodes = [n for n in self.nodes.values() if n.shard_id == shard_id]
                incumbent_demoted = any(
                    n.role == NodeRole.FOLLOWER
                    and normalize_url(n.address) == normalize_url(entry.primary)
                    for n in shard_nodes
                )
                peer_alive_without_primary = now_mono - self._started_at_monotonic >= (
                    self.settings.FAILURE_TIMEOUT_S
                ) and any(
                    now_mono - n.last_seen_monotonic < self.settings.FAILURE_TIMEOUT_S
                    for n in shard_nodes
                )
                last = self._last_headless_attempt.get(shard_id, float("-inf"))
                if (incumbent_demoted or peer_alive_without_primary) and (
                    now_mono - last >= self.settings.HEARTBEAT_INTERVAL_S
                ):
                    self._last_headless_attempt[shard_id] = now_mono
                    logger.warning(
                        "Shard %s has no registered primary (incumbent_demoted=%s); "
                        "promoting follower %s",
                        shard_id,
                        incumbent_demoted,
                        entry.follower,
                    )
                    if await self.trigger_failover(shard_id, http_client=http_client):
                        promoted.append(shard_id)
                continue

            if primary_node is not None:
                elapsed = now_mono - primary_node.last_seen_monotonic
                if elapsed >= self.settings.FAILURE_TIMEOUT_S:
                    logger.warning(
                        "Primary failure detected on shard %s (node=%s, elapsed=%.2fs >= D=%.2fs)",
                        shard_id,
                        primary_node.node_id,
                        elapsed,
                        self.settings.FAILURE_TIMEOUT_S,
                    )
                    success = await self.trigger_failover(
                        shard_id,
                        http_client=http_client,
                        crash_time=time.time() - elapsed,
                    )
                    if success:
                        promoted.append(shard_id)

        return promoted

    def bump_range_migration(
        self,
        shard_id: str,
        start: int,
        end: int,
        new_shard_id: str | None = None,
    ) -> int:
        """Bump ownership_epoch of migrated range, optionally reassigning to new_shard_id."""
        current_map = self.map_mgr.shard_map
        new_entries = []
        found = False
        new_epoch = 1

        dest_primary = None
        dest_follower = None
        if new_shard_id:
            for e in current_map.entries:
                if e.shard_id == new_shard_id:
                    dest_primary = e.primary
                    dest_follower = e.follower
                    break

        for entry in current_map.entries:
            if entry.shard_id == shard_id and entry.range.start <= start and end <= entry.range.end:
                if found:
                    raise ValueError(
                        f"Ambiguous range mapping: multiple entries match [{start}, {end}) "
                        f"for shard {shard_id!r}"
                    )
                found = True
                new_epoch = entry.ownership_epoch + 1

                if entry.range.start == start and entry.range.end == end:
                    new_entries.append(
                        entry.model_copy(
                            update={
                                "shard_id": new_shard_id or entry.shard_id,
                                "primary": dest_primary or entry.primary,
                                "follower": dest_follower or entry.follower,
                                "ownership_epoch": new_epoch,
                                "map_version": current_map.map_version + 1,
                            }
                        )
                    )
                else:
                    if entry.range.start < start:
                        new_entries.append(
                            ShardMapEntry(
                                shard_id=entry.shard_id,
                                range=ShardRange(start=entry.range.start, end=start),
                                primary=entry.primary,
                                follower=entry.follower,
                                ownership_epoch=entry.ownership_epoch,
                                map_version=current_map.map_version + 1,
                            )
                        )
                    new_entries.append(
                        ShardMapEntry(
                            shard_id=new_shard_id or entry.shard_id,
                            range=ShardRange(start=start, end=end),
                            primary=dest_primary or entry.primary,
                            follower=dest_follower or entry.follower,
                            ownership_epoch=new_epoch,
                            map_version=current_map.map_version + 1,
                        )
                    )
                    if end < entry.range.end:
                        new_entries.append(
                            ShardMapEntry(
                                shard_id=entry.shard_id,
                                range=ShardRange(start=end, end=entry.range.end),
                                primary=entry.primary,
                                follower=entry.follower,
                                ownership_epoch=entry.ownership_epoch,
                                map_version=current_map.map_version + 1,
                            )
                        )
            else:
                new_entries.append(
                    entry.model_copy(update={"map_version": current_map.map_version + 1})
                )

        if not found:
            raise ValueError(f"Range [{start}, {end}) not found for shard {shard_id!r}")

        new_entries.sort(key=lambda e: e.range.start)
        new_map = ShardMap(map_version=current_map.map_version + 1, entries=new_entries)
        self.map_mgr.save(new_map)
        logger.info(
            "Bumped range migration for [%d, %d): epoch=%d map_v=%d to_shard=%s",
            start,
            end,
            new_epoch,
            new_map.map_version,
            new_shard_id,
        )
        return new_epoch

    # ── Node Registration & Role Assignment ───────────────────────────────────

    def register_node(self, reg: NodeRegistration) -> RegistrationResponse:
        """Register a restarted or newly started node.

        A restarted node must obtain a fresh lease and never resumes from stale state.
        """
        now_mono = time.monotonic()
        now_wall = time.time()
        shard_id = reg.shard_id
        current_map = self.map_mgr.shard_map

        # Find matching shard entry
        entries = [e for e in current_map.entries if e.shard_id == shard_id]
        if not entries:
            raise InvalidQuery(f"Unknown shard_id {shard_id!r}")

        ranges = [e.range for e in entries]
        term = self.shard_terms.get(shard_id, 1)

        # Determine role
        primary_addr = entries[0].primary
        follower_addr = entries[0].follower

        clean_reg = normalize_url(reg.address)
        clean_pri = normalize_url(primary_addr)
        clean_fol = normalize_url(follower_addr)

        # Check if shard already has both primary and follower
        if clean_pri and clean_fol:
            if clean_reg != clean_pri and clean_reg != clean_fol:
                raise ValueError(f"Shard {shard_id!r} already has primary and follower.")

        if clean_pri and clean_reg != clean_pri:
            # If a primary is already running for this shard and this node is not it,
            # this node must be registered as FOLLOWER to prevent stale primary split-brain.
            role = NodeRole.FOLLOWER
        elif reg.requested_role:
            role = reg.requested_role
        else:
            # Check addresses in shard map
            if clean_reg == clean_pri or not clean_pri:
                role = NodeRole.PRIMARY
            elif clean_reg == clean_fol:
                role = NodeRole.FOLLOWER
            else:
                # Default first registered node to primary, second to follower
                existing_for_shard = [
                    n
                    for n in self.nodes.values()
                    if n.shard_id == shard_id and n.node_id != reg.node_id
                ]
                has_primary = any(n.role == NodeRole.PRIMARY for n in existing_for_shard)
                has_follower = any(n.role == NodeRole.FOLLOWER for n in existing_for_shard)
                if has_primary and has_follower:
                    raise ValueError(f"Shard {shard_id!r} already has primary and follower.")
                role = NodeRole.FOLLOWER if has_primary else NodeRole.PRIMARY

        if role == NodeRole.FOLLOWER:
            for m in reversed(self.failover_metrics):
                if m["shard_id"] == shard_id and m.get("rejoin_timestamp") is None:
                    m["rejoin_timestamp"] = now_wall
                    break

        # Update address in shard map if needed
        updated_entries = []
        map_changed = False
        for entry in current_map.entries:
            if entry.shard_id == shard_id:
                if role == NodeRole.PRIMARY and normalize_url(entry.primary) != clean_reg:
                    # H3: bump ownership_epoch so routers detect the address change
                    # immediately via epoch staleness, not just after TTL expiry.
                    entry = entry.model_copy(
                        update={
                            "primary": reg.address,
                            "ownership_epoch": entry.ownership_epoch + 1,
                        }
                    )
                    map_changed = True
                elif (
                    role == NodeRole.FOLLOWER
                    and normalize_url(entry.follower) != clean_reg
                    and normalize_url(entry.primary) != clean_reg
                ):
                    # (A restarted incumbent primary registering as follower keeps its map
                    # slot; the health monitor fails over to the current follower, which
                    # swaps the two addresses atomically.)
                    entry = entry.model_copy(update={"follower": reg.address})
                    map_changed = True
            updated_entries.append(entry)

        if map_changed:
            current_map = ShardMap(
                map_version=current_map.map_version + 1,
                entries=updated_entries,
            )
            self.map_mgr.save(current_map)

        # Record registered node
        self.nodes[reg.node_id] = RegisteredNode(
            node_id=reg.node_id,
            shard_id=shard_id,
            address=reg.address,
            role=role,
            term=term,
            last_seen_monotonic=now_mono,
        )

        lease_grant: LeaseGrant | None = None
        if role == NodeRole.PRIMARY:
            # Check if an old lease from a DIFFERENT primary is still within expiry + M
            existing_lease = self.leases.get(shard_id)
            if existing_lease is not None and existing_lease.node_id != reg.node_id:
                safety_cutoff = existing_lease.expires_at_monotonic + self.settings.SAFETY_MARGIN_S
                if now_mono < safety_cutoff:
                    # Old lease from another node still potentially active:
                    # cannot grant write lease yet
                    logger.warning(
                        "Registration: cannot grant lease to replacement primary %s: "
                        "old lease for %s still active until %.2f (mono=%.2f)",
                        reg.node_id,
                        existing_lease.node_id,
                        safety_cutoff,
                        now_mono,
                    )
                    return RegistrationResponse(
                        node_id=reg.node_id,
                        shard_id=shard_id,
                        role=role,
                        term=term,
                        ranges=ranges,
                        lease=None,
                        map_version=current_map.map_version,
                    )
            if existing_lease is None and clean_reg != clean_pri:
                if now_mono < self._recovery_quarantine_until_monotonic:
                    logger.warning(
                        "Registration: coordinator recovery quarantine active until %.2f "
                        "(mono=%.2f); cannot grant lease to non-incumbent node %s",
                        self._recovery_quarantine_until_monotonic,
                        now_mono,
                        reg.node_id,
                    )
                    return RegistrationResponse(
                        node_id=reg.node_id,
                        shard_id=shard_id,
                        role=role,
                        term=term,
                        ranges=ranges,
                        lease=None,
                        map_version=current_map.map_version,
                    )

            # Grant fresh lease
            expires_wall = now_wall + self.settings.LEASE_DURATION_S
            expires_mono = now_mono + self.settings.LEASE_DURATION_S
            self.leases[shard_id] = ActiveLease(
                node_id=reg.node_id,
                shard_id=shard_id,
                term=term,
                granted_at_wall=now_wall,
                expires_at_wall=expires_wall,
                expires_at_monotonic=expires_mono,
            )
            lease_grant = LeaseGrant(
                node_id=reg.node_id,
                shard_id=shard_id,
                term=term,
                granted_at=now_wall,
                expires_at=expires_wall,
                map_version=current_map.map_version,
            )

        logger.info(
            "Registered node %s as %s for shard %s (lease=%s)",
            reg.node_id,
            role.value,
            shard_id,
            lease_grant is not None,
        )

        return RegistrationResponse(
            node_id=reg.node_id,
            shard_id=shard_id,
            role=role,
            term=term,
            ranges=ranges,
            lease=lease_grant,
            map_version=current_map.map_version,
        )

    # ── Heartbeats & Lease Renewal ────────────────────────────────────────────

    def handle_heartbeat(self, hb: Heartbeat) -> LeaseGrant | None:
        """Process heartbeat from a node.

        Primary heartbeats renew the primary's lease and report follower state.
        Fences stale, unregistered, or unauthorized primaries permanently.
        """
        now_mono = time.monotonic()
        now_wall = time.time()
        shard_id = hb.shard_id
        current_term = self.shard_terms.get(shard_id, 1)

        # 1. Term fence: heartbeat term must match durable term exactly
        if hb.term < current_term:
            raise StaleTerm(f"Heartbeat carries stale term {hb.term} < current {current_term}")
        if hb.term > current_term:
            raise StaleTerm(
                f"Heartbeat carries unknown future term {hb.term} > current {current_term}"
            )

        # 2. Registration fence: unknown nodes must never be inserted or granted authority
        if hb.node_id not in self.nodes:
            logger.warning(
                "Heartbeat rejected: unknown/unregistered node %s for shard %s",
                hb.node_id,
                shard_id,
            )
            return None

        registered = self.nodes[hb.node_id]

        # 3. Shard validation
        if registered.shard_id != shard_id:
            logger.warning(
                "Heartbeat rejected: node %s belongs to shard %s, not %s",
                hb.node_id,
                registered.shard_id,
                shard_id,
            )
            return None

        # Update last seen and term for the registered node
        registered.last_seen_monotonic = now_mono
        registered.term = hb.term

        # Follower heartbeats never receive leases
        if hb.role == NodeRole.FOLLOWER:
            return None

        if hb.role == NodeRole.PRIMARY:
            # 4. Registered role fence: node must be registered as PRIMARY
            if registered.role != NodeRole.PRIMARY:
                logger.warning(
                    "Heartbeat rejected: node %s registered as %s cannot claim PRIMARY authority",
                    hb.node_id,
                    registered.role.value,
                )
                return None

            # 5. Authoritative primary address fence
            shard_entry = next(
                (e for e in self.map_mgr.shard_map.entries if e.shard_id == shard_id),
                None,
            )
            if shard_entry is None:
                logger.warning("Heartbeat rejected: unknown shard %s in shard map", shard_id)
                return None

            clean_node = normalize_url(registered.address)
            clean_primary = normalize_url(shard_entry.primary)
            if not clean_node or clean_node != clean_primary:
                logger.warning(
                    "Heartbeat rejected: node %s (addr=%s) is not the authorized primary (%s) "
                    "in shard map",
                    hb.node_id,
                    registered.address,
                    shard_entry.primary,
                )
                return None

            # 6. Conflicting active lease fence
            existing_lease = self.leases.get(shard_id)
            if existing_lease is not None and existing_lease.node_id != hb.node_id:
                safety_cutoff = existing_lease.expires_at_monotonic + self.settings.SAFETY_MARGIN_S
                if now_mono < safety_cutoff:
                    logger.warning(
                        "Heartbeat: conflicting primary %s rejected; "
                        "active lease held by %s until %.2f (mono=%.2f)",
                        hb.node_id,
                        existing_lease.node_id,
                        safety_cutoff,
                        now_mono,
                    )
                    return None

            # Update follower state as seen by primary
            prev_state = self.replica_states.get(shard_id)
            self.replica_states[shard_id] = hb.follower_state
            if hb.follower_state == ReplicaState.SYNCED and prev_state != ReplicaState.SYNCED:
                now_wall = time.time()
                for m in reversed(self.failover_metrics):
                    if m["shard_id"] == shard_id and m.get("synced_timestamp") is None:
                        m["synced_timestamp"] = now_wall
                        break

            # Renew lease for the authorized primary
            expires_wall = now_wall + self.settings.LEASE_DURATION_S
            expires_mono = now_mono + self.settings.LEASE_DURATION_S
            self.leases[shard_id] = ActiveLease(
                node_id=hb.node_id,
                shard_id=shard_id,
                term=hb.term,
                granted_at_wall=now_wall,
                expires_at_wall=expires_wall,
                expires_at_monotonic=expires_mono,
            )
            return LeaseGrant(
                node_id=hb.node_id,
                shard_id=shard_id,
                term=hb.term,
                granted_at=now_wall,
                expires_at=expires_wall,
                map_version=self.map_mgr.shard_map.map_version,
            )

        return None

    def record_first_read(self, shard_id: str) -> None:
        """Record the timestamp of the first successful read after failover promotion."""
        now_wall = time.time()
        for m in reversed(self.failover_metrics):
            if m["shard_id"] == shard_id and m.get("first_read_timestamp") is None:
                m["first_read_timestamp"] = now_wall
                break

    def record_first_write(self, shard_id: str) -> None:
        """Record the timestamp of the first successful write after failover rejoin."""
        now_wall = time.time()
        for m in reversed(self.failover_metrics):
            if m["shard_id"] == shard_id and m.get("first_write_timestamp") is None:
                m["first_write_timestamp"] = now_wall
                break

    # ── Status Inspection ─────────────────────────────────────────────────────

    def get_cluster_status(self) -> dict[str, Any]:
        """Return comprehensive cluster and shard health status."""
        now_mono = time.monotonic()
        shards_info: dict[str, Any] = {}

        # Group map entries by shard_id
        entries_by_shard: dict[str, list[ShardMapEntry]] = {}
        for entry in self.map_mgr.shard_map.entries:
            entries_by_shard.setdefault(entry.shard_id, []).append(entry)

        for shard_id, entries in entries_by_shard.items():
            rep_state = self.replica_states.get(shard_id, ReplicaState.DOWN)
            is_degraded = rep_state != ReplicaState.SYNCED
            active_lease = self.leases.get(shard_id)
            lease_remaining = 0.0
            lease_holder = None
            if active_lease and active_lease.expires_at_monotonic > now_mono:
                lease_remaining = round(active_lease.expires_at_monotonic - now_mono, 3)
                lease_holder = active_lease.node_id

            shards_info[shard_id] = {
                "primary": entries[0].primary,
                "follower": entries[0].follower,
                "replica_state": rep_state.value,
                "degraded": is_degraded,
                "term": self.shard_terms.get(shard_id, 1),
                "lease_holder": lease_holder,
                "lease_remaining_s": lease_remaining,
                "ranges": [
                    {"start": e.range.start, "end": e.range.end, "epoch": e.ownership_epoch}
                    for e in entries
                ],
            }

        # Calculate durations for metrics
        calculated_metrics = []
        for m in self.failover_metrics:
            c = dict(m)
            crash_t = m.get("crash_timestamp")
            first_read_t = m.get("first_read_timestamp") or m.get("promotion_timestamp")
            if crash_t and first_read_t:
                c["failover_time_s"] = round(first_read_t - crash_t, 3)
            first_write_t = m.get("first_write_timestamp")
            if crash_t and first_write_t:
                c["write_restoration_time_s"] = round(first_write_t - crash_t, 3)
            calculated_metrics.append(c)

        return {
            "status": "ok",
            "map_version": self.map_mgr.shard_map.map_version,
            "shards": shards_info,
            "failover_metrics": calculated_metrics,
            "nodes": [
                {
                    "node_id": n.node_id,
                    "shard_id": n.shard_id,
                    "address": n.address,
                    "role": n.role.value,
                    "term": n.term,
                    "alive": (now_mono - n.last_seen_monotonic) < self.settings.FAILURE_TIMEOUT_S,
                    "last_seen_seconds_ago": round(now_mono - n.last_seen_monotonic, 2),
                }
                for n in self.nodes.values()
            ],
        }
