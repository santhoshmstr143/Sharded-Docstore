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


# ═══════════════════════════════════════════════════════════════════════════
# Section 3 — security & correctness review, phase 1
# ═══════════════════════════════════════════════════════════════════════════


def _signed(method: str, path: str, body: dict[str, Any] | None = None) -> tuple[bytes, dict]:
    """Route-bound cluster signature exactly as production code produces it."""
    from docstore.common.auth import cluster_headers

    raw = b"" if body is None else json.dumps(body, separators=(",", ":")).encode()
    return raw, cluster_headers(SECRET, method, path, raw)


@pytest.mark.raw_cluster_auth
@pytest.mark.asyncio
async def test_sec1_shard_data_routes_require_client_api_key(tmp_path: Path) -> None:
    app = _standalone(tmp_path)
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            doc = {"request_id": "r1", "document": {"doc_id": 1}}
            assert (await c.post("/doc", json=doc)).status_code == 401
            assert (await c.get("/doc/1")).status_code == 401
            assert (await c.post("/query", json={"filter": {}})).status_code == 401
            assert (await c.get("/status")).status_code == 401
            bad = {CLIENT_KEY_HEADER: "x" * 16}
            assert (await c.post("/doc", json=doc, headers=bad)).status_code == 401
            good = {CLIENT_KEY_HEADER: CLIENT}
            assert (await c.post("/doc", json=doc, headers=good)).status_code == 201
            assert (await c.get("/doc/1", headers=good)).status_code == 200
            assert (await c.get("/health")).status_code == 200  # liveness stays open
        assert app.state.node.engine.doc_count() == 1


@pytest.mark.raw_cluster_auth
@pytest.mark.asyncio
async def test_sec3_signature_cannot_be_replayed_against_another_endpoint(tmp_path: Path) -> None:
    app = _standalone(tmp_path, ranges=[ShardRangeConfig(start=0, end=100, ownership_epoch=1)])
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            await c.post(
                "/doc",
                json={"request_id": "r1", "document": {"doc_id": 5}},
                headers={CLIENT_KEY_HEADER: CLIENT},
            )
            raw, h = _signed("POST", "/internal/migration/checksum", {"start": 0, "end": 100})
            assert (
                await c.post("/internal/migration/checksum", content=raw, headers=h)
            ).status_code == 200
            # Same bytes + signature against the destructive endpoint: rejected.
            r = await c.post("/internal/migration/cleanup", content=raw, headers=h)
            assert r.status_code == 401
            # Legacy body-only signature: rejected.
            legacy = {
                "Content-Type": "application/json",
                CLUSTER_SIG_HEADER: compute_signature(SECRET, raw),
            }
            assert (
                await c.post("/internal/migration/checksum", content=raw, headers=legacy)
            ).status_code == 401
            # A GET signature for one path is not a token for another path / query.
            _, gh = _signed("GET", "/internal/status")
            assert (await c.get("/internal/status", headers=gh)).status_code == 200
            assert (await c.get("/internal/snapshot", headers=gh)).status_code == 401
            assert (await c.get("/internal/status?x=1", headers=gh)).status_code == 401
        assert app.state.node.engine.doc_count() == 1
        assert [(r.start, r.end) for r in app.state.node.ranges] == [(0, 100)]


def test_sec_no_body_only_signing_in_src() -> None:
    """Production code must sign method+path+body; only auth.py may use the raw HMAC.

    (tests/conftest.py's adapter relies on this to never mask a production bug.)
    """
    src = Path(__file__).resolve().parents[2] / "src" / "docstore"
    offenders = [
        str(p.relative_to(src))
        for p in src.rglob("*.py")
        if p.name != "auth.py" and "compute_signature(" in p.read_text()
    ]
    assert offenders == []


@pytest.mark.raw_cluster_auth
@pytest.mark.asyncio
async def test_sec_router_forwards_api_key_and_route_bound_signature(tmp_path: Path) -> None:
    from docstore.common.auth import verify_request_signature

    mgr = ShardMapManager(tmp_path / "map.json")
    mgr.save(create_default_3_shard_map(primaries=["p:1", "q:1", "r:1"]))
    seen: list[httpx.Request] = []

    def shard(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/query":
            return httpx.Response(200, json={"documents": [], "total": 0})
        return httpx.Response(201, json={"outcome": "COMMITTED", "seq": 1})

    transports = {a: httpx.MockTransport(shard) for a in ("p:1", "q:1", "r:1")}
    router = build_router_app(_settings(tmp_path / "r"), mgr, shard_transports=transports)
    async with LifespanManager(router):
        async with AsyncClient(transport=ASGITransport(app=router), base_url="http://r") as rc:
            h = {CLIENT_KEY_HEADER: CLIENT}
            await rc.post("/doc", json={"request_id": "a", "document": {"doc_id": 1}}, headers=h)
            await rc.post("/query", json={"filter": {}}, headers=h)
    assert seen
    for req in seen:
        assert req.headers[CLIENT_KEY_HEADER] == CLIENT
        assert verify_request_signature(
            SECRET,
            req.method,
            req.url.path,
            req.url.query.decode(),
            req.content,
            req.headers[CLUSTER_SIG_HEADER],
        )


@pytest.mark.asyncio
async def test_sec4_query_ranges_are_bounded_and_merged(tmp_path: Path) -> None:
    app = _standalone(tmp_path)
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            for i in range(5):
                await _insert(c, {"doc_id": i}, f"r{i}")
            too_many = [[i, i + 1] for i in range(65)]
            assert (
                await c.post("/query", json={"filter": {}, "ranges": too_many})
            ).status_code == 422
            bad = {"filter": {}, "ranges": [[10, 5]]}
            assert (await c.post("/query", json=bad)).status_code == 422
            dupes = {"filter": {}, "ranges": [[0, KEYSPACE_END]] * 60}
            r = await c.post("/query", json=dupes)
            assert r.status_code == 200 and r.json()["total"] == 5  # scanned once, not 60x


def _two_shard_setup(tmp_path: Path, smap: ShardMap) -> tuple[Any, Any, ShardMapManager]:
    """Shard sa owns [0, 100), sb owns [100, 2^63); *smap* is what the map file says."""
    a = _standalone(tmp_path / "a", "sa", [ShardRangeConfig(start=0, end=100, ownership_epoch=1)])
    b = _standalone(
        tmp_path / "b", "sb", [ShardRangeConfig(start=100, end=KEYSPACE_END, ownership_epoch=1)]
    )
    mgr = ShardMapManager(tmp_path / "map.json")
    mgr.save(smap)
    return a, b, mgr


def _entry(sid: str, s: int, e: int, addr: str, epoch: int = 1) -> ShardMapEntry:
    return ShardMapEntry(
        shard_id=sid,
        range=ShardRange(start=s, end=e),
        primary=addr,
        ownership_epoch=epoch,
        map_version=1,
    )


@pytest.mark.asyncio
async def test_q1_and_of_two_doc_id_in_sets_returns_intersection(tmp_path: Path) -> None:
    smap = ShardMap(
        map_version=1, entries=[_entry("sa", 0, 100, "a:1"), _entry("sb", 100, KEYSPACE_END, "b:1")]
    )
    a, b, mgr = _two_shard_setup(tmp_path, smap)
    async with LifespanManager(a), LifespanManager(b):
        ta, tb = ASGITransport(app=a), ASGITransport(app=b)
        async with AsyncClient(transport=ta, base_url="http://t") as ca:
            for i in (1, 2, 3):
                await _insert(ca, {"doc_id": i}, f"a{i}")
        async with AsyncClient(transport=tb, base_url="http://t") as cb:
            await _insert(cb, {"doc_id": 150}, "b150")
        router = build_router_app(
            _settings(tmp_path / "r"), mgr, shard_transports={"a:1": ta, "b:1": tb}
        )
        async with LifespanManager(router):
            async with AsyncClient(transport=ASGITransport(app=router), base_url="http://r") as rc:
                h = {CLIENT_KEY_HEADER: CLIENT}
                f1 = {"$and": [{"doc_id": {"$in": [1, 2]}}, {"doc_id": {"$in": [2, 3]}}]}
                r = await rc.post("/query", json={"filter": f1}, headers=h)
                assert [d["doc_id"] for d in r.json()["documents"]] == [2]
                f2 = {"$and": [{"doc_id": {"$in": [1, 150]}}, {"doc_id": {"$in": [2, 3]}}]}
                r = await rc.post("/query", json={"filter": f2}, headers=h)
                assert r.json()["documents"] == [] and r.json()["total"] == 0


@pytest.mark.asyncio
async def test_ctl1_stale_router_query_refreshes_instead_of_dropping_range(tmp_path: Path) -> None:
    current = ShardMap(
        map_version=2, entries=[_entry("sa", 0, 100, "a:1"), _entry("sb", 100, KEYSPACE_END, "b:1")]
    )
    a, b, mgr = _two_shard_setup(tmp_path, current)
    # The router still caches the pre-migration map: sa owned [0, 200).
    mgr.set_map(
        ShardMap(
            map_version=1,
            entries=[_entry("sa", 0, 200, "a:1"), _entry("sb", 200, KEYSPACE_END, "b:1")],
        )
    )
    async with LifespanManager(a), LifespanManager(b):
        ta, tb = ASGITransport(app=a), ASGITransport(app=b)
        async with AsyncClient(transport=tb, base_url="http://t") as cb:
            await _insert(cb, {"doc_id": 150, "tag": "x"}, "b150")
        router = build_router_app(
            _settings(tmp_path / "r"), mgr, shard_transports={"a:1": ta, "b:1": tb}
        )
        async with LifespanManager(router):
            async with AsyncClient(transport=ASGITransport(app=router), base_url="http://r") as rc:
                r = await rc.post(
                    "/query", json={"filter": {"tag": "x"}}, headers={CLIENT_KEY_HEADER: CLIENT}
                )
        assert r.status_code == 200
        assert [d["doc_id"] for d in r.json()["documents"]] == [150]
        assert mgr.shard_map.map_version == 2  # the router refreshed its map


@pytest.mark.asyncio
async def test_shard2_failed_internal_replication_is_tracked_not_orphaned(tmp_path: Path) -> None:
    def follower(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error_code": "ERROR", "error_message": "down"})

    client = httpx.AsyncClient(mounts={"http://f:1": httpx.MockTransport(follower)})
    app = build_app(
        _settings(tmp_path, SNAPSHOT_EVERY_N_OPS=0, SNAPSHOT_EVERY_S=0),
        node_id="p",
        shard_id="s0",
        follower_url="f:1",
        http_client=client,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )
    async with LifespanManager(app):
        node = app.state.node
        raw, h = _signed(
            "POST",
            "/internal/migration/apply-batch",
            {"migration_id": "m", "documents": [{"doc_id": 5}]},
        )
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://t") as c:
            r = await c.post("/internal/migration/apply-batch", content=raw, headers=h)
            assert r.status_code == 503
            assert list(node.pending_unresolved_writes) == ["mig-m-5"]
            assert node.follower_state == ReplicaState.DOWN
            # Next client write is rejected cleanly (never committed-then-500).
            for _ in range(2):
                r = await _insert(c, {"doc_id": 7}, "w7")
                assert r.status_code in (202, 503), r.text
        rids = [e["request_id"] for e in node.wal.replay()]
        assert rids == ["mig-m-5"]  # no duplicate / orphaned appends
    await client.aclose()


class _Lossy(httpx.AsyncBaseTransport):
    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner, self.down = inner, False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self.down:
            raise httpx.ConnectError("down", request=request)
        return await self.inner.handle_async_request(request)


@pytest.mark.asyncio
async def test_shard6_7_snapshot_resync_catches_up_and_fresh_follower_bootstraps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(shard_app_mod, "_WAL_RETAIN_ENTRIES", 1)
    rng = [ShardRangeConfig(start=0, end=10**6, ownership_epoch=1)]
    st = lambda d: _settings(d, SNAPSHOT_EVERY_N_OPS=0, SNAPSHOT_EVERY_S=0)  # noqa: E731
    fol = build_app(
        st(tmp_path / "f"), node_id="f", shard_id="s0", role=NodeRole.FOLLOWER, ranges=rng
    )
    ftx = _Lossy(ASGITransport(app=fol))
    holder: dict[str, Any] = {}

    class _ToPrimary(httpx.AsyncBaseTransport):
        async def handle_async_request(self, req: httpx.Request) -> httpx.Response:
            return await ASGITransport(app=holder["p"]).handle_async_request(req)

    client = httpx.AsyncClient(mounts={"http://f": ftx, "http://p": _ToPrimary()})
    pri = build_app(
        st(tmp_path / "p"),
        node_id="p",
        shard_id="s0",
        ranges=rng,
        follower_url="http://f",
        http_client=client,
    )
    holder["p"] = pri
    async with LifespanManager(fol), LifespanManager(pri):
        P, F = pri.state.node, fol.state.node
        async with AsyncClient(transport=ASGITransport(app=pri), base_url="http://p") as c:
            for i in range(1, 6):
                assert (await _insert(c, {"doc_id": i}, f"w{i}")).status_code == 201
            assert await snapshot_and_compact(P) > 0
            assert P.wal.earliest_seq() > 1

            # (#7) An empty replacement follower bootstraps from the compacted primary.
            fresh = build_app(
                st(tmp_path / "g"), node_id="g", shard_id="s0", role=NodeRole.FOLLOWER, ranges=rng
            )
            async with LifespanManager(fresh):
                assert await shard_app_mod.resync_with_primary(
                    fresh.state.node, "http://p", http_client=client
                )
                assert fresh.state.node.engine.doc_count() == 5

            # (#6) A pending write survives a snapshot-path resync and is then confirmed.
            ftx.down = True
            assert (await _insert(c, {"doc_id": 6}, "w6")).status_code == 202
            ftx.down = False
            assert await shard_app_mod.resync_with_primary(F, "http://p", http_client=client)
            assert F.wal.last_seq == P.wal.last_seq  # follower got the pending tail too
            r = await _insert(c, {"doc_id": 7}, "w7")
            assert r.status_code == 201, r.text
            assert P.engine.get(6) is not None and not P.pending_unresolved_writes
    await client.aclose()


@pytest.mark.asyncio
async def test_shard8_standalone_crash_between_wal_and_metadata_recovers(tmp_path: Path) -> None:
    rng = [ShardRangeConfig(start=0, end=10**6, ownership_epoch=1)]

    def mk() -> Any:
        return build_app(
            _settings(tmp_path, SNAPSHOT_EVERY_N_OPS=0, SNAPSHOT_EVERY_S=0),
            node_id="n",
            shard_id="s0",
            ranges=rng,
            allow_standalone=True,
        )

    app = mk()
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            assert (await _insert(c, {"doc_id": 1}, "w1")).status_code == 201
        w = app.state.node.wal
        # Crash point: w2 is durable in the WAL, committed_seq not yet persisted.
        w.append(
            seq=w.next_seq(), term=1, op="insert", doc_id=2, request_id="w2", payload={"doc_id": 2}
        )
    (tmp_path / "s0" / "snapshot.json").unlink(missing_ok=True)  # a crash writes no snapshot

    app2 = mk()
    async with LifespanManager(app2):
        node = app2.state.node
        assert not node.pending_unresolved_writes
        assert node.engine.get(2) is not None
        async with AsyncClient(transport=ASGITransport(app=app2), base_url="http://t") as c:
            assert (await _insert(c, {"doc_id": 3}, "w3")).status_code == 201
            retry = await _insert(c, {"doc_id": 2}, "w2")
            assert retry.status_code == 201 and retry.json()["seq"] == 2


# ═══════════════════════════════════════════════════════════════════════════
# Section 4 — security & correctness review, phase 2
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_p10_wal_fsync_failure_fail_stops_the_node(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _standalone(tmp_path)
    async with LifespanManager(app):
        node = app.state.node
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            assert (await _insert(c, {"doc_id": 1}, "ok")).status_code == 201

            real_fsync = os.fsync

            def broken(fd: int) -> None:
                raise OSError(errno.EIO, "disk gone")

            monkeypatch.setattr(os, "fsync", broken)
            r = await _insert(c, {"doc_id": 2}, "w2")
            monkeypatch.setattr(os, "fsync", real_fsync)
            # The record may be on disk: the client is told the outcome is unknown.
            assert r.status_code == 202 and r.json()["error_code"] == "OUTCOME_UNKNOWN"
            # ...and the node takes itself out of service (no lease, no heartbeats).
            assert node.ready is False and not node.is_lease_valid()
            assert (await c.get("/health")).status_code == 503
            assert (await _insert(c, {"doc_id": 3}, "w3")).status_code == 503


def _coord_with_follower(tmp_path: Path) -> CoordinatorService:
    smap = create_default_3_shard_map(
        primaries=["p0:1", "p1:1", "p2:1"], followers=["f0:1", None, None]
    )
    map_path = tmp_path / "coord" / "shard_map.json"
    ShardMapManager(map_path).save(smap)
    return CoordinatorService(_settings(tmp_path / "coord"), map_path=map_path)


def _status_transport(seq: int, seen: list[str]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(f"{request.url.host}{request.url.path}")
        if request.url.path == "/internal/status":
            return httpx.Response(200, json={"last_applied_seq": seq})
        return httpx.Response(200, json={"status": "ok"})

    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_p12_unsynced_or_lagging_follower_is_not_promoted(tmp_path: Path) -> None:
    from docstore.common.models import Heartbeat

    svc = _coord_with_follower(tmp_path)
    svc._recovery_quarantine_until_monotonic = 0.0
    svc.register_node(NodeRegistration(node_id="p0", shard_id="shard0", address="p0:1"))
    hb = Heartbeat(
        node_id="p0",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=1,
        last_applied_seq=50,
        follower_acked_seq=40,
        follower_state=ReplicaState.CATCHING_UP,
        timestamp_monotonic=time.monotonic(),
    )
    svc.handle_heartbeat(hb)
    svc.leases.clear()
    seen: list[str] = []
    async with AsyncClient(transport=_status_transport(50, seen)) as client:
        # Follower last reported CATCHING_UP: refused even though it answers.
        assert await svc.trigger_failover("shard0", http_client=client) is False
        # Synced, but behind the primary's last reported seq: refused.
        svc.replica_states["shard0"] = ReplicaState.SYNCED
    async with AsyncClient(transport=_status_transport(10, seen)) as client:
        assert await svc.trigger_failover("shard0", http_client=client) is False
    assert svc.shard_terms["shard0"] == 1
    assert not any(p.endswith("/internal/promote") for p in seen)
    # Explicit operator override still works.
    async with AsyncClient(transport=_status_transport(10, seen)) as client:
        assert await svc.trigger_failover("shard0", http_client=client, force=True) is True


@pytest.mark.asyncio
async def test_p12_restarted_incumbent_is_repromoted_when_follower_unsynced(tmp_path: Path) -> None:
    svc = _coord_with_follower(tmp_path)
    svc._recovery_quarantine_until_monotonic = 0.0
    svc.register_node(NodeRegistration(node_id="p0", shard_id="shard0", address="p0:1"))
    svc._replica_reported.add("shard0")
    svc.replica_states["shard0"] = ReplicaState.CATCHING_UP
    # The primary restarts from disk and re-registers as a follower.
    svc.register_node(
        NodeRegistration(
            node_id="p0", shard_id="shard0", address="p0:1", requested_role=NodeRole.FOLLOWER
        )
    )
    svc.leases.clear()
    seen: list[str] = []
    async with AsyncClient(transport=_status_transport(0, seen)) as client:
        assert await svc.check_and_trigger_failover(http_client=client) == ["shard0"]
    entry = next(e for e in svc.shard_map.entries if e.shard_id == "shard0")
    assert (entry.primary, entry.follower) == ("p0:1", "f0:1")  # incumbent kept, not swapped
    assert svc.shard_terms["shard0"] == 2
    assert "p0/internal/promote" in seen and "f0/internal/promote" not in seen


@pytest.mark.asyncio
async def test_p13_no_lease_renewal_or_grant_while_failing_over(tmp_path: Path) -> None:
    from docstore.common.errors import FailoverInProgress
    from docstore.common.models import Heartbeat

    svc = _coord_with_follower(tmp_path)
    svc.register_node(NodeRegistration(node_id="p0", shard_id="shard0", address="p0:1"))
    svc.failover_in_progress.add("shard0")
    hb = Heartbeat(
        node_id="p0",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=1,
        last_applied_seq=1,
        follower_acked_seq=1,
        follower_state=ReplicaState.SYNCED,
        timestamp_monotonic=time.monotonic(),
    )
    with pytest.raises(FailoverInProgress):
        svc.handle_heartbeat(hb)
    resp = svc.register_node(NodeRegistration(node_id="p0", shard_id="shard0", address="p0:1"))
    assert resp.lease is None


def test_p15_placeholder_secrets_are_rejected(tmp_path: Path) -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="placeholder"):
        _settings(tmp_path, CLUSTER_SECRET="change-me-cluster-secret-min-32-chars")
    with pytest.raises(ValidationError, match="placeholder"):
        _settings(tmp_path, CLIENT_API_KEY="change-me-client-api-key")


@pytest.mark.asyncio
async def test_p16_query_engine_rejections(tmp_path: Path) -> None:
    from docstore.query import validate_query_filter

    app = _standalone(tmp_path)
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            await _insert(c, {"doc_id": 1, "x": [1, 2]}, "a")
            # Sorting on an array field is a client error, not a 500.
            r = await c.post("/query", json={"filter": {}, "sort_field": "x"})
            assert r.status_code == 400 and r.json()["error_code"] == "INVALID_QUERY"
            # NaN in a stored document is rejected.
            r = await c.post(
                "/doc",
                content=b'{"request_id":"n","document":{"doc_id":2,"x":NaN}}',
                headers={"Content-Type": "application/json"},
            )
            assert r.status_code == 422
    # A filter whose $in items are huge lists exceeds the value budget.
    from docstore.common.errors import InvalidQuery

    with pytest.raises(InvalidQuery, match="values"):
        validate_query_filter({"a": {"$in": [[0] * 600] * 100}})
    validate_query_filter({"a": {"$in": list(range(1000))}})  # normal use still fine


@pytest.mark.asyncio
async def test_p16_router_passes_invalid_query_through(tmp_path: Path) -> None:
    smap = ShardMap(
        map_version=1,
        entries=[_entry("sa", 0, 100, "a:1"), _entry("sb", 100, KEYSPACE_END, "b:1")],
    )
    a, b, mgr = _two_shard_setup(tmp_path, smap)
    async with LifespanManager(a), LifespanManager(b):
        ta, tb = ASGITransport(app=a), ASGITransport(app=b)
        async with AsyncClient(transport=ta, base_url="http://t") as ca:
            await _insert(ca, {"doc_id": 1, "x": [1]}, "a1")
        router = build_router_app(
            _settings(tmp_path / "r"), mgr, shard_transports={"a:1": ta, "b:1": tb}
        )
        async with LifespanManager(router):
            async with AsyncClient(transport=ASGITransport(app=router), base_url="http://r") as rc:
                r = await rc.post(
                    "/query",
                    json={"filter": {}, "sort_field": "x"},
                    headers={CLIENT_KEY_HEADER: CLIENT},
                )
        assert r.status_code == 400 and r.json()["error_code"] == "INVALID_QUERY"


def test_p17_client_migrate_and_transport_errors() -> None:
    from docstore.client.client import DocStoreClient, DocStoreClientError

    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path == "/admin/migrate":
            raise httpx.ReadTimeout("slow", request=request)
        if request.url.path == "/doc":
            if len([r for r in calls if r.url.path == "/doc"]) == 1:
                raise httpx.RemoteProtocolError("dropped", request=request)
            return httpx.Response(201, json={"outcome": "COMMITTED", "seq": 1})
        return httpx.Response(500, json=["not", "a", "dict"])

    client = DocStoreClient(
        "http://r",
        "http://c",
        "k" * 16,
        "a" * 32,
        backoff_base_s=0.0,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    # migrate: one attempt only, documented request shape.
    with pytest.raises(DocStoreClientError):
        client.migrate("shard0", "shard1", 0, 10, sync=False)
    migrate_calls = [r for r in calls if r.url.path == "/admin/migrate"]
    assert len(migrate_calls) == 1
    assert json.loads(migrate_calls[0].content)["range"] == {"start": 0, "end": 10}
    # A dropped connection is retried with the SAME request_id.
    assert client.insert({"doc_id": 1}, request_id="rid-1")["outcome"] == "COMMITTED"
    doc_calls = [json.loads(r.content)["request_id"] for r in calls if r.url.path == "/doc"]
    assert doc_calls == ["rid-1", "rid-1"]
    # A non-object error body does not crash the client.
    with pytest.raises(DocStoreClientError):
        client.get(1)


# ═══════════════════════════════════════════════════════════════════════════
# Section 5 — security & correctness review, phase 3
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_l1_malformed_internal_bodies_are_rejected_without_side_effects(
    tmp_path: Path,
) -> None:
    app = _standalone(tmp_path, ranges=[ShardRangeConfig(start=0, end=100, ownership_epoch=1)])
    async with LifespanManager(app):
        node = app.state.node
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            bad = [
                ("/internal/migration/pause", {"start": "a", "end": None}),
                ("/internal/migration/cutover", {"start": 500, "end": 10, "ownership_epoch": 2}),
                ("/internal/migration/cutover", {"start": 0, "end": 10, "ownership_epoch": True}),
                ("/internal/migration/oplog", {"start": 0, "end": 10, "after_seq": "x"}),
                ("/internal/migration/snapshot", {"start": 0, "end": 10, "limit": "abc"}),
                ("/internal/migration/apply-batch", {"documents": [{"doc_id": -1}]}),
                (
                    "/internal/migration/apply-oplog",
                    {"entries": [{"seq": 1, "doc_id": 1, "op": "x"}]},
                ),
                ("/internal/snapshot/apply", {"last_applied_seq": {}}),
            ]
            for path, body in bad:
                raw, h = _signed("POST", path, body)
                r = await c.post(path, content=raw, headers=h)
                assert r.status_code == 422, (path, r.status_code, r.text)
            # Nothing was paused, owned or written; writes still work.
            assert node.paused_ranges == []
            assert [(r.start, r.end) for r in node.ranges] == [(0, 100)]
            assert (await _insert(c, {"doc_id": 5}, "after")).status_code == 201


@pytest.mark.asyncio
async def test_l2_errors_do_not_leak_addresses_or_input(tmp_path: Path) -> None:
    mgr = ShardMapManager(tmp_path / "map.json")
    mgr.save(create_default_3_shard_map(primaries=["10.9.8.7:8200", "q:1", "r:1"]))

    def down(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused to 10.9.8.7:8200", request=request)

    router = build_router_app(
        _settings(tmp_path / "r"),
        mgr,
        shard_transports={"10.9.8.7:8200": httpx.MockTransport(down)},
    )
    async with LifespanManager(router):
        async with AsyncClient(transport=ASGITransport(app=router), base_url="http://r") as rc:
            h = {CLIENT_KEY_HEADER: CLIENT}
            r = await rc.get("/doc/1", headers=h)
            assert r.status_code == 503 and "10.9.8.7" not in r.text
            secret_value = "SSN-123-45-6789"
            r = await rc.post(
                "/doc", json={"request_id": "x", "document": {"doc_id": secret_value}}, headers=h
            )
            assert r.status_code == 422 and secret_value not in r.text
            r = await rc.post(
                "/doc", json={"request_id": secret_value * 20, "document": {}}, headers=h
            )
            assert r.status_code == 422 and secret_value not in r.text


@pytest.mark.asyncio
async def test_l3_query_response_size_is_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from docstore.router import scatter

    monkeypatch.setattr(scatter, "QUERY_MAX_RESPONSE_BYTES", 2_000)
    smap = ShardMap(
        map_version=1,
        entries=[_entry("sa", 0, 100, "a:1"), _entry("sb", 100, KEYSPACE_END, "b:1")],
    )
    a, b, mgr = _two_shard_setup(tmp_path, smap)
    async with LifespanManager(a), LifespanManager(b):
        ta, tb = ASGITransport(app=a), ASGITransport(app=b)
        async with AsyncClient(transport=ta, base_url="http://t") as ca:
            for i in range(20):
                await _insert(ca, {"doc_id": i, "blob": "x" * 200}, f"a{i}")
        router = build_router_app(
            _settings(tmp_path / "r"), mgr, shard_transports={"a:1": ta, "b:1": tb}
        )
        async with LifespanManager(router):
            async with AsyncClient(transport=ASGITransport(app=router), base_url="http://r") as rc:
                h = {CLIENT_KEY_HEADER: CLIENT}
                r = await rc.post("/query", json={"filter": {}, "limit": 100}, headers=h)
                assert r.status_code == 413 and r.json()["error_code"] == "RESULT_TOO_LARGE"
                r = await rc.post("/query", json={"filter": {}, "limit": 2}, headers=h)
                assert r.status_code == 200


def test_l4_migration_never_copies_source_follower_into_destination(tmp_path: Path) -> None:
    smap = create_default_3_shard_map(
        primaries=["p0:1", "p1:1", "p2:1"], followers=["f0:1", None, None]
    )
    map_path = tmp_path / "shard_map.json"
    ShardMapManager(map_path).save(smap)
    svc = CoordinatorService(_settings(tmp_path), map_path=map_path)
    s0 = next(e for e in svc.shard_map.entries if e.shard_id == "shard0")
    svc.bump_range_migration("shard0", s0.range.start, s0.range.start + 10, new_shard_id="shard1")
    for e in svc.shard_map.entries:
        if e.shard_id == "shard1":
            assert (e.primary, e.follower) == ("p1:1", None)
        if e.shard_id == "shard0":
            assert (e.primary, e.follower) == ("p0:1", "f0:1")
    with pytest.raises(ValueError, match="Unknown destination"):
        svc.bump_range_migration(
            "shard0", s0.range.start + 10, s0.range.start + 20, new_shard_id="nope"
        )


@pytest.mark.asyncio
async def test_l5_crash_after_snapshot_install_before_wal_reset_recovers(tmp_path: Path) -> None:
    from docstore.storage.snapshot import SnapshotManager

    data_dir = tmp_path / "s0"
    data_dir.mkdir(parents=True)
    # Stale local history 1..3 (from before the resync) ...
    (data_dir / "shard.wal").write_bytes(b"".join(_wal_line(s, doc_id=s) for s in (1, 2, 3)))
    # ... superseded by a snapshot installed from the primary at seq 5.
    SnapshotManager(data_dir).save(
        documents={10: {"doc_id": 10}},
        last_applied_seq=5,
        term=2,
        ownership_epochs={f"0-{KEYSPACE_END}": 1},
        dedup_table={},
        installed=True,
    )
    app = build_app(
        _settings(tmp_path),
        node_id="n",
        shard_id="s0",
        allow_standalone=True,
        ranges=[ShardRangeConfig(start=0, end=KEYSPACE_END, ownership_epoch=1)],
    )
    async with LifespanManager(app):
        node = app.state.node
        assert node.ready
        assert node.wal.earliest_seq() == 6 and node.wal.last_seq == 5
        assert node.engine.get(10) is not None and node.engine.get(1) is None


@pytest.mark.asyncio
async def test_l6_one_failing_shard_does_not_block_other_failovers(tmp_path: Path) -> None:
    smap = create_default_3_shard_map(
        primaries=["p0:1", "p1:1", "p2:1"], followers=["f0:1", "f1:1", None]
    )
    map_path = tmp_path / "coord" / "shard_map.json"
    ShardMapManager(map_path).save(smap)
    svc = CoordinatorService(_settings(tmp_path / "coord"), map_path=map_path)
    svc._recovery_quarantine_until_monotonic = 0.0
    for sid, addr in (("shard0", "p0:1"), ("shard1", "p1:1")):
        svc.register_node(NodeRegistration(node_id=addr, shard_id=sid, address=addr))
        svc.nodes[addr].last_seen_monotonic -= 60  # both primaries silent past D
    svc.leases.clear()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "f0":
            raise httpx.ConnectError("f0 down", request=request)
        return httpx.Response(200, json={"last_applied_seq": 0, "status": "ok"})

    async with AsyncClient(transport=httpx.MockTransport(handler)) as client:
        promoted = await svc.check_and_trigger_failover(http_client=client)
    assert promoted == ["shard1"]
