"""Regression tests for bugs found in two code audits.

Section 1 (``test_bugN_*``) covers the second audit: recovery, failover, migration,
WAL durability, query and API hardening. Section 2 (``test_reportN_*``) covers the
items from the first audit that still needed fixes.
"""

from __future__ import annotations

import asyncio
import errno
import json
import os
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from docstore.common.auth import CLIENT_KEY_HEADER, CLUSTER_SIG_HEADER, compute_signature
from docstore.common.config import Settings
from docstore.common.http import BodyLimitMiddleware
from docstore.common.models import (
    NodeRegistration,
    NodeRole,
    ReplicaState,
    ShardMap,
    ShardMapEntry,
    ShardRange,
)
from docstore.coordinator.coordinator import CoordinatorService
from docstore.coordinator.migration import (
    MigrationManager,
    MigrationState,
    _batches_by_size,
)
from docstore.query.shard_key import detect_shard_key_constraint
from docstore.router.app import build_router_app
from docstore.router.shard_map import ShardMapManager, create_default_3_shard_map
from docstore.shard import app as shard_app_mod
from docstore.shard.app import (
    NodeState,
    ShardRangeConfig,
    _apply_registration,
    _parse_snapshot_docs,
    _recover,
    build_app,
    lease_duration_from_grant,
    snapshot_and_compact,
)
from docstore.storage import wal as wal_mod
from docstore.storage.engine import StorageEngine
from docstore.storage.wal import WAL, WALCorruptionError, WALFailedError

pytestmark = pytest.mark.integration

SECRET = "s" * 32
ADMIN = "a" * 32
CLIENT = "c" * 16
KEYSPACE_END = 2**63


def _settings(d: Path, **kw: Any) -> Settings:
    base: dict[str, Any] = {
        "CLUSTER_SECRET": SECRET,
        "ADMIN_TOKEN": ADMIN,
        "CLIENT_API_KEY": CLIENT,
        "DATA_DIR": str(d),
        "HEARTBEAT_INTERVAL_S": 0.2,
        "LEASE_DURATION_S": 1.0,
        "FAILURE_TIMEOUT_S": 3.0,
        "SAFETY_MARGIN_S": 0.1,
        "REPLICATION_TIMEOUT_S": 0.5,
    }
    base.update(kw)
    return Settings(**base)


def _sig(body: dict[str, Any]) -> tuple[bytes, dict[str, str]]:
    raw = json.dumps(body, separators=(",", ":")).encode()
    return raw, {
        "Content-Type": "application/json",
        CLUSTER_SIG_HEADER: compute_signature(SECRET, raw),
    }


def _standalone(d: Path, shard_id: str = "s0", ranges: list[ShardRangeConfig] | None = None) -> Any:
    return build_app(
        _settings(d),
        node_id=f"{shard_id}-n",
        shard_id=shard_id,
        ranges=ranges,
        allow_standalone=True,
    )


async def _insert(c: AsyncClient, doc: dict[str, Any], rid: str, **extra: Any) -> httpx.Response:
    return await c.post("/doc", json={"request_id": rid, "document": doc, **extra})


def _wal_line(seq: int, term: int = 1, doc_id: int = 1) -> bytes:
    rec: dict[str, Any] = {
        "seq": seq,
        "term": term,
        "op": "insert",
        "doc_id": doc_id,
        "request_id": f"r{seq}",
        "payload": {"doc_id": doc_id},
    }
    rec["entry_id"] = wal_mod.compute_entry_identity(rec)
    rec["crc32"] = wal_mod._crc32_of(rec)
    return (json.dumps(rec, separators=(",", ":")) + "\n").encode()


# ═══════════════════════════════════════════════════════════════════════════
# Section 1 — second audit (recovery, failover, migration, WAL, API)
# ═══════════════════════════════════════════════════════════════════════════


# ── BUG 1: restarted primary / headless shard ────────────────────────────────


@pytest.mark.asyncio
async def test_bug1_restarted_incumbent_is_replaced_by_follower(tmp_path: Path) -> None:
    settings = _settings(tmp_path / "coord")
    smap = create_default_3_shard_map(
        primaries=["p0:1", "p1:1", "p2:1"], followers=["f0:1", None, None]
    )
    map_path = tmp_path / "coord" / "shard_map.json"
    ShardMapManager(map_path).save(smap)
    svc = CoordinatorService(settings, map_path=map_path)

    svc.register_node(NodeRegistration(node_id="p0", shard_id="shard0", address="p0:1"))
    # Primary restarts from disk and re-registers as a follower (it never self-promotes).
    resp = svc.register_node(
        NodeRegistration(
            node_id="p0", shard_id="shard0", address="p0:1", requested_role=NodeRole.FOLLOWER
        )
    )
    assert resp.role == NodeRole.FOLLOWER
    entry = next(e for e in svc.shard_map.entries if e.shard_id == "shard0")
    assert (entry.primary, entry.follower) == ("p0:1", "f0:1")  # map slot not clobbered

    follower = build_app(
        _settings(tmp_path / "f0"), node_id="f0", shard_id="shard0", role=NodeRole.FOLLOWER
    )
    async with LifespanManager(follower):
        async with AsyncClient(mounts={"http://f0:1": ASGITransport(app=follower)}) as client:
            promoted = await svc.check_and_trigger_failover(http_client=client)
        assert promoted == ["shard0"]
        entry = next(e for e in svc.shard_map.entries if e.shard_id == "shard0")
        assert (entry.primary, entry.follower) == ("f0:1", "p0:1")
        node = follower.state.node
        assert node.role == NodeRole.PRIMARY
        # promotion carried the map's authoritative ranges
        assert [(r.start, r.end) for r in node.ranges] == [(entry.range.start, entry.range.end)]


@pytest.mark.asyncio
async def test_bug1_node_steps_down_when_registered_as_follower(tmp_path: Path) -> None:
    app = _standalone(tmp_path)
    async with LifespanManager(app):
        state = app.state.node
        assert state.role == NodeRole.PRIMARY
        await _apply_registration(state, {"role": "follower"}, time.monotonic())
        assert state.role == NodeRole.FOLLOWER
        meta = json.loads((tmp_path / "s0" / "node_metadata.json").read_text())
        assert meta["role"] == "follower"


# ── BUG 2: crash-safe failover intent ───────────────────────────────────────


def test_bug2_committed_failover_intent_is_resumed_after_restart(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    smap = create_default_3_shard_map(followers=["f0:1", None, None])
    map_path = tmp_path / "shard_map.json"
    ShardMapManager(map_path).save(smap)
    svc = CoordinatorService(settings, map_path=map_path)
    svc._set_intent(
        "shard0",
        {"new_primary": "f0:1", "old_primary": "127.0.0.1:8200", "target_term": 2},
    )
    svc.bump_shard_failover("shard0", new_primary_url="f0:1", new_follower_url="127.0.0.1:8200")
    # crash before promotion: a new coordinator must finish it
    svc2 = CoordinatorService(settings, map_path=map_path)
    assert svc2._unconfirmed_promotions["shard0"][0] == "f0:1"


def test_bug2_uncommitted_failover_intent_is_discarded(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    map_path = tmp_path / "shard_map.json"
    ShardMapManager(map_path).save(create_default_3_shard_map(followers=["f0:1", None, None]))
    svc = CoordinatorService(settings, map_path=map_path)
    svc._set_intent("shard0", {"new_primary": "f0:1", "old_primary": "x", "target_term": 2})
    svc2 = CoordinatorService(settings, map_path=map_path)  # crash before the map changed
    assert "shard0" not in svc2._unconfirmed_promotions
    assert json.loads((tmp_path / "failover_intents.json").read_text()) == {}


# ── BUG 3 / 29: promotion persists before activating; strict validation ─────


@pytest.mark.asyncio
async def test_bug3_promote_fails_closed_when_metadata_persist_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = build_app(_settings(tmp_path), node_id="f", shard_id="s0", role=NodeRole.FOLLOWER)
    async with LifespanManager(app):
        state = app.state.node
        term_before = state.term
        real = shard_app_mod._persist_node_metadata

        def failing(st: NodeState, **kw: Any) -> None:
            if kw.get("role") == NodeRole.PRIMARY:
                raise OSError(errno.EIO, "disk gone")
            real(st, **kw)

        monkeypatch.setattr(shard_app_mod, "_persist_node_metadata", failing)
        body, h = _sig({"shard_id": "s0", "term": term_before + 1})
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://t") as c:
            r = await c.post("/internal/promote", content=body, headers=h)
        assert r.status_code == 500
        assert state.role == NodeRole.FOLLOWER
        assert state.term == term_before
        assert not state.is_lease_valid()


@pytest.mark.asyncio
async def test_bug29_promote_rejects_wrong_shard_and_mismatched_ranges(tmp_path: Path) -> None:
    app = build_app(_settings(tmp_path), node_id="f", shard_id="s0", role=NodeRole.FOLLOWER)
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            body, h = _sig({"shard_id": "s9", "term": 5})
            r = await c.post("/internal/promote", content=body, headers=h)
            assert r.status_code == 409 and r.json()["error_code"] == "WRONG_SHARD"
            body, h = _sig({"shard_id": "s0", "term": 5, "ownership_epochs": {"0-10": 3}})
            r = await c.post("/internal/promote", content=body, headers=h)
            assert r.status_code == 409 and r.json()["error_code"] == "RANGE_MISMATCH"
        assert app.state.node.role == NodeRole.FOLLOWER


# ── BUG 4 / 5 / 6: migration cutover safety ─────────────────────────────────


async def _migration_cluster(tmp_path: Path, dest_follower: str = "") -> Any:
    settings = _settings(tmp_path)
    coord = CoordinatorService(settings, map_path=str(tmp_path / "coordinator" / "shard_map.json"))
    app0 = build_app(
        settings,
        node_id="node0",
        shard_id="shard0",
        ranges=[ShardRangeConfig(start=0, end=200, ownership_epoch=1)],
        allow_standalone=True,
    )
    app1 = build_app(
        settings,
        node_id="node1",
        shard_id="shard1",
        ranges=[ShardRangeConfig(start=200, end=KEYSPACE_END, ownership_epoch=1)],
        allow_standalone=True,
    )
    coord.map_mgr.save(
        ShardMap(
            map_version=1,
            entries=[
                ShardMapEntry(
                    shard_id="shard0",
                    range=ShardRange(start=0, end=200),
                    primary="http://node0",
                    follower="",
                    ownership_epoch=1,
                    map_version=1,
                ),
                ShardMapEntry(
                    shard_id="shard1",
                    range=ShardRange(start=200, end=KEYSPACE_END),
                    primary="http://node1",
                    follower=dest_follower,
                    ownership_epoch=1,
                    map_version=1,
                ),
            ],
        )
    )
    coord.replica_states["shard0"] = ReplicaState.SYNCED
    coord.replica_states["shard1"] = ReplicaState.SYNCED
    return coord, app0, app1


@pytest.mark.asyncio
async def test_bug4_ambiguous_map_update_rolls_forward_not_abort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    coord, app0, app1 = await _migration_cluster(tmp_path)
    async with LifespanManager(app0), LifespanManager(app1):
        t0, t1 = ASGITransport(app=app0), ASGITransport(app=app1)
        async with AsyncClient(transport=t0, base_url="http://node0") as c0:
            for i in range(5):
                assert (await _insert(c0, {"doc_id": 100 + i}, f"i{i}")).status_code == 201

        real_bump = coord.bump_range_migration

        def bump_then_fail(*a: Any, **kw: Any) -> int:
            real_bump(*a, **kw)  # the map IS committed ...
            raise OSError(errno.EIO, "epoch-history fsync failed")  # ... but we see an error

        monkeypatch.setattr(coord, "bump_range_migration", bump_then_fail)
        rec = coord.migration_mgr.create_migration("shard0", "shard1", 100, 200, coord)
        async with AsyncClient(mounts={"http://node0": t0, "http://node1": t1}) as client:
            res = await coord.migration_mgr.execute_migration(
                rec.migration_id, coord, http_client=client
            )
        assert res.state == MigrationState.DONE  # rolled forward; staged data NOT purged
        assert app1.state.node.engine.doc_count() == 5
        assert app0.state.node.engine.doc_count() == 0


@pytest.mark.asyncio
async def test_bug5_dest_follower_cutover_failure_blocks_done_then_retries(
    tmp_path: Path,
) -> None:
    coord, app0, app1 = await _migration_cluster(tmp_path, dest_follower="http://node1f")
    follower = build_app(
        _settings(tmp_path / "f"),
        node_id="node1f",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=200, end=KEYSPACE_END, ownership_epoch=1)],
    )
    async with LifespanManager(app0), LifespanManager(app1), LifespanManager(follower):
        t0, t1 = ASGITransport(app=app0), ASGITransport(app=app1)

        async def follower_down(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("down", request=request)

        rec = coord.migration_mgr.create_migration("shard0", "shard1", 100, 200, coord)
        down = {
            "http://node0": t0,
            "http://node1": t1,
            "http://node1f": httpx.MockTransport(follower_down),
        }
        async with AsyncClient(mounts=down) as client:
            with pytest.raises(httpx.ConnectError):
                await coord.migration_mgr.execute_migration(
                    rec.migration_id, coord, http_client=client
                )
        assert coord.migration_mgr.migrations[rec.migration_id].state == MigrationState.CUTOVER

        up = {"http://node0": t0, "http://node1": t1, "http://node1f": ASGITransport(app=follower)}
        async with AsyncClient(mounts=up) as client:
            await coord.migration_mgr.retry_post_cutover(coord, http_client=client)
        assert coord.migration_mgr.migrations[rec.migration_id].state == MigrationState.DONE
        assert (100, 200) in [(r.start, r.end) for r in follower.state.node.ranges]


@pytest.mark.asyncio
async def test_bug6_topology_change_during_migration_aborts_before_cutover(
    tmp_path: Path,
) -> None:
    coord, app0, app1 = await _migration_cluster(tmp_path)
    async with LifespanManager(app0), LifespanManager(app1):
        t0, t1 = ASGITransport(app=app0), ASGITransport(app=app1)

        async def failover_happens(phase: MigrationState) -> None:
            if phase == MigrationState.SNAPSHOT_COPY:
                coord.shard_terms["shard1"] = coord.shard_terms.get("shard1", 1) + 1

        rec = coord.migration_mgr.create_migration("shard0", "shard1", 100, 200, coord)
        async with AsyncClient(mounts={"http://node0": t0, "http://node1": t1}) as client:
            with pytest.raises(RuntimeError, match="Topology changed"):
                await coord.migration_mgr.execute_migration(
                    rec.migration_id, coord, http_client=client, on_phase_hook=failover_happens
                )
        assert coord.migration_mgr.migrations[rec.migration_id].state == MigrationState.ABORTED
        owner = next(e for e in coord.shard_map.entries if e.range.start <= 150 < e.range.end)
        assert owner.shard_id == "shard0"


# ── BUG 7: explicitly empty ownership survives restart ──────────────────────


@pytest.mark.asyncio
async def test_bug7_empty_ranges_are_not_replaced_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHARD_RANGE_START", "0")
    monkeypatch.setenv("SHARD_RANGE_END", str(KEYSPACE_END))
    data_dir = tmp_path / "s0"
    data_dir.mkdir(parents=True)
    (data_dir / "node_metadata.json").write_text(
        json.dumps({"term": 3, "role": "follower", "committed_seq": 0, "ranges": []})
    )
    state = NodeState(_settings(tmp_path), node_id="n", shard_id="s0")
    await _recover(state)
    assert state.ranges == []
    assert not state.owns(5)


# ── BUG 8: internal RPCs check shard identity ───────────────────────────────


@pytest.mark.asyncio
async def test_bug8_cross_shard_replication_and_catchup_rejected(tmp_path: Path) -> None:
    app = build_app(_settings(tmp_path), node_id="f", shard_id="s0", role=NodeRole.FOLLOWER)
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            body, h = _sig(
                {
                    "shard_id": "other",
                    "term": 1,
                    "seq": 1,
                    "op": "insert",
                    "doc_id": 1,
                    "request_id": "r1",
                    "payload": {"doc_id": 1},
                }
            )
            assert (await c.post("/internal/replicate", content=body, headers=h)).status_code == 409
            body, h = _sig({"shard_id": "other", "after_seq": 0})
            assert (await c.post("/internal/catchup", content=body, headers=h)).status_code == 409
        assert app.state.node.wal.last_seq == 0


# ── BUG 9 / 14 / 21: durability of metadata files ───────────────────────────


def test_bug9_directory_fsync_errors_propagate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def eio(fd: int) -> None:
        raise OSError(errno.EIO, "I/O error")

    monkeypatch.setattr(os, "fsync", eio)
    with pytest.raises(OSError):
        wal_mod._fsync_dir(tmp_path)

    def einval(fd: int) -> None:
        raise OSError(errno.EINVAL, "unsupported")

    monkeypatch.setattr(os, "fsync", einval)
    wal_mod._fsync_dir(tmp_path)  # platform limitation: tolerated


def test_bug14_migration_record_fsyncs_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from docstore.coordinator import migration as mig_mod

    calls: list[Path] = []
    monkeypatch.setattr(mig_mod, "_fsync_dir", lambda d: calls.append(Path(d)))
    mgr = MigrationManager(str(tmp_path), SECRET)
    mgr.save_record(
        mig_mod.MigrationRecord(from_shard="a", to_shard="b", range_start=0, range_end=1)
    )
    assert calls == [tmp_path / "migrations"]


def test_bug21_history_failure_does_not_report_uncommitted_map(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mgr = ShardMapManager(tmp_path / "shard_map.json")
    mgr.save(create_default_3_shard_map())

    def boom(*a: Any, **kw: Any) -> None:
        raise OSError(errno.EIO, "history fsync failed")

    monkeypatch.setattr(mgr, "_append_history_log", boom)
    new_map = create_default_3_shard_map(map_version=2)
    mgr.save(new_map)  # must not raise: the map file is the commit point
    assert ShardMapManager(tmp_path / "shard_map.json").reload().map_version == 2


# ── BUG 10 / 11: WAL sequence integrity ─────────────────────────────────────


def test_bug10_failed_fsync_poisons_wal_and_never_reuses_seq(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "shard.wal"
    wal = WAL(path)
    wal.open()
    list(wal.replay())
    wal.append(seq=1, term=1, op="insert", doc_id=1, request_id="a", payload={"doc_id": 1})

    real_fsync = os.fsync

    def flaky(fd: int) -> None:
        raise OSError(errno.EIO, "fsync failed")

    monkeypatch.setattr(os, "fsync", flaky)
    with pytest.raises(OSError):
        wal.append(seq=2, term=1, op="insert", doc_id=2, request_id="b", payload={"doc_id": 2})
    monkeypatch.setattr(os, "fsync", real_fsync)
    with pytest.raises(WALFailedError):
        wal.append(seq=2, term=1, op="insert", doc_id=3, request_id="c", payload={"doc_id": 3})

    # Reopen + recover: the next seq follows whatever actually reached the disk.
    wal2 = WAL(path)
    wal2.open()
    seqs = [r["seq"] for r in wal2.replay()]
    nxt = wal2.next_seq()
    wal2.append(seq=nxt, term=1, op="insert", doc_id=4, request_id="d", payload={"doc_id": 4})
    all_seqs = [*seqs, nxt]
    assert all_seqs == list(range(1, len(all_seqs) + 1))  # gap-free, no duplicates


def test_bug10_append_rejects_reused_seq(tmp_path: Path) -> None:
    wal = WAL(tmp_path / "w.wal")
    wal.open()
    wal.append(seq=1, term=1, op="insert", doc_id=1, request_id="a", payload=None)
    with pytest.raises(ValueError, match="continuity"):
        wal.append(seq=1, term=1, op="insert", doc_id=2, request_id="b", payload=None)


@pytest.mark.parametrize("seqs", [[1, 2, 2], [1, 2, 4], [2, 1]])
def test_bug11_replay_rejects_broken_sequence(tmp_path: Path, seqs: list[int]) -> None:
    path = tmp_path / "w.wal"
    path.write_bytes(b"".join(_wal_line(s) for s in seqs))
    wal = WAL(path)
    wal.open()
    with pytest.raises(WALCorruptionError):
        list(wal.replay())


def test_bug11_replay_rejects_gap_after_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "w.wal"
    path.write_bytes(_wal_line(7) + _wal_line(8))
    wal = WAL(path)
    wal.open()
    assert [r["seq"] for r in wal.replay(base_seq=6)] == [7, 8]
    with pytest.raises(WALCorruptionError):
        list(wal.replay(base_seq=4))  # seqs 5..6 missing


# ── BUG 12: periodic snapshot + safe compaction ─────────────────────────────


@pytest.mark.asyncio
async def test_bug12_snapshot_and_compaction_preserve_data_across_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(shard_app_mod, "_WAL_RETAIN_ENTRIES", 2)
    app = _standalone(tmp_path)
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            for i in range(10):
                assert (await _insert(c, {"doc_id": i, "v": i}, f"r{i}")).status_code == 201
        removed = await snapshot_and_compact(app.state.node)
        assert removed == 8
        assert app.state.node.wal.earliest_seq() == 9

    app2 = _standalone(tmp_path)
    async with LifespanManager(app2):
        node = app2.state.node
        assert node.ready and node.engine.doc_count() == 10
        async with AsyncClient(transport=ASGITransport(app=app2), base_url="http://t") as c:
            r = await _insert(c, {"doc_id": 99}, "after-restart")
            assert r.status_code == 201 and r.json()["seq"] == 11
            # dedup survives compaction (rebuilt from the snapshot)
            assert (await _insert(c, {"doc_id": 0, "v": 0}, "r0")).json()["seq"] == 1


@pytest.mark.asyncio
async def test_bug12_compaction_respects_migration_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(shard_app_mod, "_WAL_RETAIN_ENTRIES", 0)
    app = _standalone(tmp_path)
    async with LifespanManager(app):
        node = app.state.node
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            for i in range(10):
                await _insert(c, {"doc_id": i}, f"r{i}")
        node.hold_migration(0, 100, 4)  # an active migration still reads entries after seq 4
        assert await snapshot_and_compact(node) == 4
        assert node.wal.earliest_seq() == 5


# ── BUG 13: corrupt migration records fail closed ───────────────────────────


def test_bug13_corrupt_migration_record_fails_closed(tmp_path: Path) -> None:
    mig_dir = tmp_path / "migrations"
    mig_dir.mkdir()
    (mig_dir / "x.json").write_text("{not json")
    with pytest.raises(RuntimeError, match="Corrupt migration record"):
        MigrationManager(str(tmp_path), SECRET)


# ── BUG 15 / 16 / 17: migration transport ───────────────────────────────────


@pytest.mark.asyncio
async def test_bug15_retried_apply_batch_is_idempotent(tmp_path: Path) -> None:
    app = _standalone(tmp_path, ranges=[ShardRangeConfig(start=0, end=10, ownership_epoch=1)])
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            body, h = _sig({"migration_id": "m1", "documents": [{"doc_id": 50}, {"doc_id": 51}]})
            r1 = await c.post("/internal/migration/apply-batch", content=body, headers=h)
            last = app.state.node.wal.last_seq
            r2 = await c.post("/internal/migration/apply-batch", content=body, headers=h)
        assert r1.json()["skipped"] == 0 and r2.json()["skipped"] == 2
        assert app.state.node.wal.last_seq == last  # nothing appended on retry


def test_bug16_batches_are_bounded_by_bytes() -> None:
    docs = [{"doc_id": i, "blob": "x" * 20_000} for i in range(200)]
    batches = _batches_by_size(docs, max_bytes=100_000)
    assert sum(len(b) for b in batches) == 200
    for b in batches:
        assert len(json.dumps(b, separators=(",", ":"))) <= 100_000
    with pytest.raises(RuntimeError, match="larger than"):
        _batches_by_size([{"doc_id": 1, "blob": "x" * 200_000}], max_bytes=100_000)


@pytest.mark.asyncio
async def test_bug17_migration_snapshot_is_paginated(tmp_path: Path) -> None:
    app = _standalone(tmp_path)
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            for i in range(5):
                await _insert(c, {"doc_id": i}, f"r{i}")
            got: list[int] = []
            after = None
            while True:
                body, h = _sig({"start": 0, "end": 100, "after_doc_id": after, "limit": 2})
                page = (
                    await c.post("/internal/migration/snapshot", content=body, headers=h)
                ).json()
                assert len(page["documents"]) <= 2
                got += [d["doc_id"] for d in page["documents"]]
                after = page["next_after_doc_id"]
                if page["done"]:
                    break
        assert got == [0, 1, 2, 3, 4]


# ── BUG 18 / 19 / 20: scatter-gather over a shard owning disjoint ranges ────


@pytest.mark.asyncio
async def test_bug18_19_20_scatter_with_multi_range_shard(tmp_path: Path) -> None:
    a = _standalone(
        tmp_path / "a",
        "sa",
        [
            ShardRangeConfig(start=0, end=100, ownership_epoch=1),
            ShardRangeConfig(start=200, end=300, ownership_epoch=1),
        ],
    )
    b = _standalone(
        tmp_path / "b",
        "sb",
        [
            ShardRangeConfig(start=100, end=200, ownership_epoch=1),
            ShardRangeConfig(start=300, end=KEYSPACE_END, ownership_epoch=1),
        ],
    )

    def entry(sid: str, s: int, e: int, addr: str) -> ShardMapEntry:
        return ShardMapEntry(
            shard_id=sid,
            range=ShardRange(start=s, end=e),
            primary=addr,
            ownership_epoch=1,
            map_version=1,
        )

    smap = ShardMap(
        map_version=1,
        entries=[
            entry("sa", 0, 100, "a:1"),
            entry("sb", 100, 200, "b:1"),
            entry("sa", 200, 300, "a:1"),
            entry("sb", 300, KEYSPACE_END, "b:1"),
        ],
    )
    mgr = ShardMapManager(tmp_path / "map.json")
    mgr.save(smap)
    async with LifespanManager(a), LifespanManager(b):
        ta, tb = ASGITransport(app=a), ASGITransport(app=b)
        async with AsyncClient(transport=ta, base_url="http://t") as ca:
            for i in list(range(0, 15)) + list(range(200, 205)):
                await _insert(ca, {"doc_id": i, "tag": "x"}, f"a{i}")
        async with AsyncClient(transport=tb, base_url="http://t") as cb:
            for i in (150, 400):
                await _insert(cb, {"doc_id": i, "tag": "x"}, f"b{i}")

        router = build_router_app(
            _settings(tmp_path / "r"), mgr, shard_transports={"a:1": ta, "b:1": tb}
        )
        async with LifespanManager(router):
            async with AsyncClient(transport=ASGITransport(app=router), base_url="http://r") as rc:
                r = await rc.post(
                    "/query",
                    json={
                        "filter": {"tag": "x"},
                        "sort_field": "doc_id",
                        "sort_dir": "desc",
                        "limit": 6,
                    },
                    headers={CLIENT_KEY_HEADER: CLIENT},
                )
        data = r.json()
        assert [d["doc_id"] for d in data["documents"]] == [400, 204, 203, 202, 201, 200]
        assert data["total"] == 22  # all matches, not the page size
        assert data["meta"] is not None and data["meta"]["served_by"] == "primary"


# ── BUG 22 / 23 / 24 / 26 / 27 / 28: API, validation, storage ──────────────


def test_bug22_huge_integer_bounds_do_not_crash_routing() -> None:
    for f in (
        {"doc_id": {"$gt": 10**400}},
        {"doc_id": {"$lt": 10**400}},
        {"doc_id": 10**400},
        {"doc_id": {"$in": [10**400, 3]}},
    ):
        detect_shard_key_constraint(f)  # must not raise OverflowError
    c = detect_shard_key_constraint({"doc_id": {"$gt": 10**400}})
    assert c.range_start == c.range_end  # empty range


@pytest.mark.asyncio
async def test_bug23_oversized_document_rejected(tmp_path: Path) -> None:
    app = _standalone(tmp_path)
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await _insert(c, {"doc_id": 1, "blob": "x" * (1024 * 1024 + 10)}, "big")
        assert r.status_code == 413 and r.json()["error_code"] == "DOC_TOO_LARGE"
        assert app.state.node.wal.last_seq == 0


@pytest.mark.asyncio
async def test_bug24_body_limit_enforced_without_content_length() -> None:
    reached: list[bytes] = []

    async def inner(scope: Any, receive: Any, send: Any) -> None:
        msg = await receive()
        reached.append(msg["body"])
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    mw = BodyLimitMiddleware(inner, max_bytes=10)

    async def run(chunks: list[bytes]) -> int:
        msgs = [
            {"type": "http.request", "body": ch, "more_body": i < len(chunks) - 1}
            for i, ch in enumerate(chunks)
        ]
        sent: list[dict[str, Any]] = []

        async def receive() -> dict[str, Any]:
            return msgs.pop(0)

        async def send(m: dict[str, Any]) -> None:
            sent.append(m)

        await mw({"type": "http", "headers": []}, receive, send)
        return int(sent[0]["status"])

    assert await run([b"12345", b"678901"]) == 413  # 11 bytes, no Content-Length
    assert reached == []
    assert await run([b"12345", b"67890"]) == 200
    assert reached == [b"1234567890"]


@pytest.mark.asyncio
async def test_bug25_router_refreshes_and_retries_on_lease_expired(tmp_path: Path) -> None:
    mgr = ShardMapManager(tmp_path / "map.json")
    mgr.save(create_default_3_shard_map(primaries=["p:1", "q:1", "r:1"]))
    seen: list[dict[str, Any]] = []

    def shard(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        if len(seen) == 1:
            return httpx.Response(409, json={"error_code": "LEASE_EXPIRED", "error_message": "x"})
        return httpx.Response(201, json={"outcome": "COMMITTED", "seq": 1})

    router = build_router_app(
        _settings(tmp_path / "r"), mgr, shard_transports={"p:1": httpx.MockTransport(shard)}
    )
    async with LifespanManager(router):
        async with AsyncClient(transport=ASGITransport(app=router), base_url="http://r") as rc:
            r = await rc.post(
                "/doc",
                json={"request_id": "same", "document": {"doc_id": 1}},
                headers={CLIENT_KEY_HEADER: CLIENT},
            )
    assert r.status_code == 201
    assert len(seen) == 2 and seen[0]["request_id"] == seen[1]["request_id"] == "same"
    assert seen[0]["ownership_epoch"] == 1  # router attaches the fencing token (BUG 26)


@pytest.mark.asyncio
async def test_bug26_stale_ownership_epoch_and_term_rejected(tmp_path: Path) -> None:
    app = _standalone(tmp_path, ranges=[ShardRangeConfig(start=0, end=100, ownership_epoch=3)])
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await _insert(c, {"doc_id": 1}, "r1", ownership_epoch=2)
            assert r.status_code == 409 and r.json()["error_code"] == "STALE_EPOCH"
            r = await _insert(c, {"doc_id": 1}, "r2", term=99)
            assert r.status_code == 409 and r.json()["error_code"] == "STALE_TERM"
            assert app.state.node.wal.last_seq == 0  # rejected before the WAL
            assert (await _insert(c, {"doc_id": 1}, "r3", ownership_epoch=3)).status_code == 201


def test_bug27_range_copies_do_not_alias_stored_documents() -> None:
    eng = StorageEngine()
    eng.insert({"doc_id": 1, "nested": {"items": [1]}}, seq=1)
    for copy_ in (eng.range_snapshot(0, 10)[0], eng.range_scan(0, 10)[0], eng.all_docs()[1]):
        copy_["nested"]["items"].append("corruption")
    assert eng.get(1) == {"doc_id": 1, "nested": {"items": [1]}}


@pytest.mark.asyncio
async def test_bug28_cutover_ranges_unchanged_when_persist_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _standalone(tmp_path, ranges=[ShardRangeConfig(start=0, end=100, ownership_epoch=1)])
    async with LifespanManager(app):

        def fail(*a: Any, **kw: Any) -> None:
            raise OSError(errno.EIO, "disk gone")

        monkeypatch.setattr(shard_app_mod, "_persist_node_metadata", fail)
        body, h = _sig({"start": 100, "end": 200, "ownership_epoch": 2})
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://t") as c:
            r = await c.post("/internal/migration/cutover", content=body, headers=h)
        assert r.status_code == 500
        assert [(x.start, x.end) for x in app.state.node.ranges] == [(0, 100)]
        assert not app.state.node.owns(150)


# ═══════════════════════════════════════════════════════════════════════════
# Section 2 — first audit (remaining items)
# ═══════════════════════════════════════════════════════════════════════════


# ── #1: one snapshot-document parser ─────────────────────────────────────────


def test_report1_snapshot_docs_parsed_from_dict_or_list() -> None:
    assert _parse_snapshot_docs({"5": {"doc_id": 5}}) == {5: {"doc_id": 5}}
    assert _parse_snapshot_docs([{"doc_id": 7}]) == {7: {"doc_id": 7}}
    with pytest.raises(ValueError):
        _parse_snapshot_docs("garbage")


# ── #8: lease length is capped by the local configuration ───────────────────


def test_report8_wall_clock_lease_is_capped_locally() -> None:
    assert lease_duration_from_grant({"granted_at": 100.0, "expires_at": 103.0}, 3.0) == 3.0
    # coordinator wall clock jumped forward between the two stamps: still capped at L
    assert lease_duration_from_grant({"granted_at": 100.0, "expires_at": 900.0}, 3.0) == 3.0
    # shorter grant is honoured (safe direction)
    assert lease_duration_from_grant({"granted_at": 100.0, "expires_at": 101.5}, 3.0) == 1.5
    # nonsense -> local default
    assert lease_duration_from_grant({"granted_at": 5.0, "expires_at": 1.0}, 3.0) == 3.0
    assert lease_duration_from_grant(None, 3.0) == 3.0


# ── #12: only a STALE_TERM 409 demotes the primary ──────────────────────────


@pytest.mark.asyncio
async def test_report12_only_stale_term_409_demotes(tmp_path: Path) -> None:
    hb_error = {"code": "SOMETHING_ELSE"}
    heartbeats = 0

    def coordinator(request: httpx.Request) -> httpx.Response:
        nonlocal heartbeats
        if request.url.path == "/register":
            return httpx.Response(200, json={"role": "primary", "term": 1})
        heartbeats += 1
        return httpx.Response(409, json={"error_code": hb_error["code"], "error_message": "x"})

    app = build_app(
        _settings(tmp_path, HEARTBEAT_INTERVAL_S=0.1),
        node_id="n",
        shard_id="s0",
        role=NodeRole.PRIMARY,
        coordinator_url="coord:1",
        coordinator_transport=httpx.MockTransport(coordinator),
    )
    async with LifespanManager(app):
        state = app.state.node
        for _ in range(50):
            if heartbeats >= 2:
                break
            await asyncio.sleep(0.05)
        assert heartbeats >= 2
        assert state.role == NodeRole.PRIMARY  # unrelated 409: no demotion

        hb_error["code"] = "STALE_TERM"
        for _ in range(50):
            if state.role == NodeRole.FOLLOWER:
                break
            await asyncio.sleep(0.05)
        assert state.role == NodeRole.FOLLOWER
        meta = json.loads((tmp_path / "s0" / "node_metadata.json").read_text())
        assert meta["role"] == "follower"


# ── #14: out-of-order apply can no longer hide an entry ──────────────────────


def test_report14_engine_refuses_out_of_order_apply() -> None:
    eng = StorageEngine()
    eng.apply({"seq": 1, "op": "insert", "doc_id": 1, "payload": {"doc_id": 1}})
    with pytest.raises(ValueError, match="Out-of-order"):
        eng.apply({"seq": 3, "op": "insert", "doc_id": 3, "payload": {"doc_id": 3}})
    eng.apply({"seq": 2, "op": "insert", "doc_id": 2, "payload": {"doc_id": 2}})
    eng.apply({"seq": 2, "op": "insert", "doc_id": 9, "payload": {"doc_id": 9}})  # dup: no-op
    assert eng.last_applied_seq == 2 and eng.get(9) is None


@pytest.mark.asyncio
async def test_report14_migration_apply_waits_for_pending_writes(tmp_path: Path) -> None:
    app = build_app(_settings(tmp_path), node_id="n", shard_id="s0", allow_standalone=True)
    async with LifespanManager(app):
        state = app.state.node
        # An appended-but-unresolved write at seq 1 (not yet applied to the engine).
        state.wal.append(
            seq=1, term=1, op="insert", doc_id=1, request_id="p1", payload={"doc_id": 1}
        )
        state.pending_unresolved_writes["p1"] = {
            "seq": 1,
            "term": 1,
            "op": "insert",
            "doc_id": 1,
            "payload": {"doc_id": 1},
        }
        raw = json.dumps({"migration_id": "m", "documents": [{"doc_id": 50}]}).encode()
        headers = {
            "Content-Type": "application/json",
            CLUSTER_SIG_HEADER: compute_signature(SECRET, raw),
        }
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.post("/internal/migration/apply-batch", content=raw, headers=headers)
        assert r.status_code == 503
        assert state.wal.last_seq == 1  # nothing appended past the unresolved entry
        assert state.engine.last_applied_seq == 0
