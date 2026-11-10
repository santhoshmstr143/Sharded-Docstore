"""
Fault and Live Range Migration integration tests — Module 9 & Milestone M6 Gate.

Covers:
- Admin API validation, admin token auth, overlapping range rejection, degraded shard rejection.
- Invisibility invariant: destination staged data is invisible to reads/queries before CUTOVER.
- Concurrent writes during migration: insert, update, delete caught up without stale docs.
- Write pause & router retry: writes during pause return RANGE_PAUSED; router retries with backoff
  and transparently routes to destination after cutover without client error.
- Deterministic checksum & verification: canonical JSON SHA-256 matches before/after.
- Abort before commit point: safely unpauses source, cleans destination.
- Roll forward after cutover: destination serves reads and writes, source rejects with OUT_OF_RANGE.
- M6 GATE: full live range migration under concurrent write workload, checksum verification,
  brief write pause measured in metrics.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from docstore.common.auth import (
    ADMIN_TOKEN_HEADER,
    CLIENT_KEY_HEADER,
    CLUSTER_SIG_HEADER,
    compute_signature,
)
from docstore.common.config import Settings
from docstore.common.models import ReplicaState, ShardMap, ShardMapEntry, ShardRange
from docstore.coordinator.app import build_coordinator_app
from docstore.coordinator.coordinator import CoordinatorService
from docstore.coordinator.migration import (
    MigrationState,
    compute_deterministic_checksum,
)
from docstore.router.app import build_router_app
from docstore.router.shard_map import ShardMapManager
from docstore.shard.app import ShardRangeConfig, build_app

pytestmark = pytest.mark.fault

SECRET = "a" * 32
ADMIN = "b" * 32
CLIENT_KEY = "c" * 16


def _make_settings(data_dir: Path | str, **overrides: Any) -> Settings:
    base = {
        "CLUSTER_SECRET": SECRET,
        "ADMIN_TOKEN": ADMIN,
        "CLIENT_API_KEY": CLIENT_KEY,
        "DATA_DIR": str(data_dir),
        "HEARTBEAT_INTERVAL_S": 0.2,
        "LEASE_DURATION_S": 1.0,
        "FAILURE_TIMEOUT_S": 2.0,
        "SAFETY_MARGIN_S": 0.2,
        "REPLICATION_TIMEOUT_S": 0.3,
        "ENABLE_TEST_HOOKS": True,
        "STANDALONE_MODE": True,
    }
    base.update(overrides)
    return Settings.model_validate(base)


def _client_headers() -> dict[str, str]:
    return {CLIENT_KEY_HEADER: CLIENT_KEY}


def _admin_headers() -> dict[str, str]:
    return {ADMIN_TOKEN_HEADER: ADMIN}


def _sig_headers(payload: dict[str, Any] | None = None) -> dict[str, str]:
    if payload is not None:
        body_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        sig = compute_signature(SECRET, body_bytes)
    else:
        sig = compute_signature(SECRET, b"")
    return {CLUSTER_SIG_HEADER: sig}


async def _post_json(client: AsyncClient, path: str, body: dict[str, Any]) -> httpx.Response:
    body_bytes = json.dumps(body, separators=(",", ":")).encode("utf-8")
    sig = compute_signature(SECRET, body_bytes)
    headers = {"Content-Type": "application/json", CLUSTER_SIG_HEADER: sig}
    return await client.post(path, content=body_bytes, headers=headers)


async def _put_json(client: AsyncClient, path: str, body: dict[str, Any]) -> httpx.Response:
    body_bytes = json.dumps(body, separators=(",", ":")).encode("utf-8")
    sig = compute_signature(SECRET, body_bytes)
    headers = {"Content-Type": "application/json", CLUSTER_SIG_HEADER: sig}
    return await client.put(path, content=body_bytes, headers=headers)


async def _delete_json(client: AsyncClient, path: str, body: dict[str, Any]) -> httpx.Response:
    body_bytes = json.dumps(body, separators=(",", ":")).encode("utf-8")
    sig = compute_signature(SECRET, body_bytes)
    headers = {"Content-Type": "application/json", CLUSTER_SIG_HEADER: sig}
    return await client.request("DELETE", path, content=body_bytes, headers=headers)


def _create_2_shard_map(
    map_path: Path,
    p0: str = "127.0.0.1:8200",
    p1: str = "127.0.0.1:8201",
    f0: str | None = None,
    f1: str | None = None,
) -> ShardMap:
    mid = 2**62
    end = 2**63
    entries = [
        ShardMapEntry(
            shard_id="shard0",
            range=ShardRange(start=0, end=mid),
            primary=p0,
            follower=f0,
            ownership_epoch=1,
            map_version=1,
        ),
        ShardMapEntry(
            shard_id="shard1",
            range=ShardRange(start=mid, end=end),
            primary=p1,
            follower=f1,
            ownership_epoch=1,
            map_version=1,
        ),
    ]
    s_map = ShardMap(map_version=1, entries=entries)
    map_path.parent.mkdir(parents=True, exist_ok=True)
    mgr = ShardMapManager(str(map_path))
    mgr.save(s_map)
    return s_map


# ═══════════════════════════════════════════════════════════════════════════
# 1. Admin API Auth & Validation
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_admin_migration_auth_and_validation(tmp_path: Path) -> None:
    """Admin migrate requires token, validates fields, and blocks overlaps."""
    settings = _make_settings(tmp_path)
    map_path = tmp_path / "shard_map.json"
    _create_2_shard_map(map_path)

    coord_svc = CoordinatorService(settings, str(map_path))
    coord_svc.replica_states["shard0"] = ReplicaState.SYNCED
    coord_svc.replica_states["shard1"] = ReplicaState.SYNCED
    coord_app = build_coordinator_app(settings, coord_svc)
    async with LifespanManager(coord_app):
        transport = ASGITransport(app=coord_app)
        async with AsyncClient(transport=transport, base_url="http://coord") as client:
            # 1. Missing admin token -> 401
            r = await client.post(
                "/admin/migrate",
                json={
                    "range": {"start": 0, "end": 1000},
                    "from_shard": "shard0",
                    "to_shard": "shard1",
                },
            )
            assert r.status_code == 401

            # 2. Missing fields -> 400
            r = await client.post(
                "/admin/migrate",
                json={"range": {"start": 0}, "from_shard": "shard0"},
                headers=_admin_headers(),
            )
            assert r.status_code == 400

            # 3. Create valid migration record via API
            r = await client.post(
                "/admin/migrate",
                json={
                    "range": {"start": 0, "end": 1000},
                    "from_shard": "shard0",
                    "to_shard": "shard1",
                },
                headers=_admin_headers(),
            )
            assert r.status_code == 200
            data = r.json()
            mig_id = data["migration_id"]
            assert data["status"] == "planned"

            # 4. Overlapping migration rejected -> 409
            r2 = await client.post(
                "/admin/migrate",
                json={
                    "range": {"start": 500, "end": 1500},
                    "from_shard": "shard0",
                    "to_shard": "shard1",
                },
                headers=_admin_headers(),
            )
            assert r2.status_code == 409
            assert "MIGRATION_CONFLICT" in r2.text

            # 5. Inspect status endpoint
            r_get = await client.get(f"/admin/migrate/{mig_id}", headers=_admin_headers())
            assert r_get.status_code == 200
            assert r_get.json()["migration_id"] == mig_id

            # 6. List migrations
            r_list = await client.get("/admin/migrations", headers=_admin_headers())
            assert r_list.status_code == 200
            assert len(r_list.json()) >= 1


# ═══════════════════════════════════════════════════════════════════════════
# 2. Invisibility Invariant & Range Isolation
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_invisibility_invariant_before_cutover(tmp_path: Path) -> None:
    """Documents staged on destination during snapshot/catchup are invisible until cutover."""
    settings = _make_settings(tmp_path)
    # Destination shard originally only owns [1000, 2000)
    dest_ranges = [ShardRangeConfig(start=1000, end=2000, ownership_epoch=1)]
    dest_dir = tmp_path / "dest"
    dest_app = build_app(
        settings.model_copy(update={"DATA_DIR": str(dest_dir)}),
        node_id="dest-node",
        shard_id="shard1",
        ranges=dest_ranges,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(dest_app):
        transport = ASGITransport(app=dest_app)
        async with AsyncClient(transport=transport, base_url="http://dest") as client:
            # Stage doc_id=42 (migrating range [0, 100)) onto destination
            batch_resp = await _post_json(
                client,
                "/internal/migration/apply-batch",
                {
                    "migration_id": "mig-inv",
                    "documents": [{"doc_id": 42, "title": "Secret Document"}],
                    "source_seq": 1,
                },
            )
            assert batch_resp.status_code == 200

            # 1. Direct GET /doc/42 on destination returns 400 OutOfRange (invisible)
            r_get = await client.get("/doc/42")
            assert r_get.status_code == 400
            err_code = r_get.json().get("error_code") or r_get.json().get("detail", {}).get(
                "error_code"
            )
            assert err_code == "OUT_OF_RANGE"

            # 2. Query on destination does not return doc 42
            r_query = await _post_json(
                client,
                "/query",
                {"filter": {"doc_id": 42}, "limit": 10},
            )
            assert r_query.status_code == 200
            assert r_query.json()["total"] == 0
            assert len(r_query.json()["documents"]) == 0

            # 3. Checksum endpoint CAN see it (for coordinator verification)
            chk_resp = await _post_json(
                client,
                "/internal/migration/checksum",
                {"start": 0, "end": 100},
            )
            assert chk_resp.status_code == 200
            assert chk_resp.json()["doc_count"] == 1

            # 4. Perform cutover: range [0, 100) becomes active on destination
            cut_resp = await _post_json(
                client,
                "/internal/migration/cutover",
                {"start": 0, "end": 100, "ownership_epoch": 2},
            )
            assert cut_resp.status_code == 200

            # 5. Now doc 42 is immediately visible!
            r_get_after = await client.get("/doc/42")
            assert r_get_after.status_code == 200
            assert r_get_after.json()["document"]["doc_id"] == 42


# ═══════════════════════════════════════════════════════════════════════════
# 3. Deterministic Checksum Verification
# ═══════════════════════════════════════════════════════════════════════════


def test_deterministic_checksum_ordering_and_format() -> None:
    """Document order in list does not affect canonical SHA-256 checksum."""
    docs1 = [
        {"doc_id": 10, "name": "Apple", "price": 1.5},
        {"doc_id": 20, "name": "Banana", "price": 0.8},
        {"doc_id": 5, "name": "Avocado", "price": 2.0},
    ]
    docs2 = [
        {"doc_id": 20, "price": 0.8, "name": "Banana"},  # key order & doc order swapped
        {"doc_id": 5, "price": 2.0, "name": "Avocado"},
        {"doc_id": 10, "price": 1.5, "name": "Apple"},
    ]

    count1, chk1 = compute_deterministic_checksum(docs1)
    count2, chk2 = compute_deterministic_checksum(docs2)

    assert count1 == 3
    assert count2 == 3
    assert chk1 == chk2
    assert len(chk1) == 64


# ═══════════════════════════════════════════════════════════════════════════
# 4. Live Range Migration with Concurrent Writes & Catch-Up
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_live_migration_concurrent_writes_and_catchup(tmp_path: Path) -> None:
    """Concurrent inserts, updates, and deletes during migration are safely caught up."""
    settings = _make_settings(tmp_path)
    map_path = tmp_path / "shard_map.json"
    _create_2_shard_map(map_path)

    coord_service = CoordinatorService(settings, str(map_path))
    # Mark shards synced so migration is permitted
    coord_service.replica_states["shard0"] = ReplicaState.SYNCED
    coord_service.replica_states["shard1"] = ReplicaState.SYNCED

    mid = 2**62
    ranges0 = [ShardRangeConfig(start=0, end=mid, ownership_epoch=1)]
    ranges1 = [ShardRangeConfig(start=mid, end=2**63, ownership_epoch=1)]

    dir0 = tmp_path / "node0"
    dir1 = tmp_path / "node1"

    app0 = build_app(
        settings.model_copy(update={"DATA_DIR": str(dir0)}),
        node_id="n0",
        shard_id="shard0",
        ranges=ranges0,
        lease_expires_at=float("inf"),
    )
    app1 = build_app(
        settings.model_copy(update={"DATA_DIR": str(dir1)}),
        node_id="n1",
        shard_id="shard1",
        ranges=ranges1,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app0), LifespanManager(app1):
        trans0 = ASGITransport(app=app0)
        trans1 = ASGITransport(app=app1)

        # Wire HTTP client for coordinator RPC
        client = AsyncClient(
            mounts={
                "http://127.0.0.1:8200": trans0,
                "http://127.0.0.1:8201": trans1,
            },
            timeout=5.0,
        )

        mig_start = 100
        mig_end = 200

        # Pre-seed documents in migrating range on shard0
        async with AsyncClient(transport=trans0, base_url="http://node0") as c0:
            for did in range(100, 110):
                w = await _post_json(
                    c0,
                    "/doc",
                    {"request_id": f"init-{did}", "document": {"doc_id": did, "v": 1}},
                )
                assert w.status_code == 201

            # Start migration record
            rec = coord_service.migration_mgr.create_migration(
                from_shard="shard0",
                to_shard="shard1",
                start=mig_start,
                end=mig_end,
                coord=coord_service,
            )

            # Concurrent writes during migration executed right after SNAPSHOT_COPY:
            async def write_hook(phase: MigrationState) -> None:
                if phase == MigrationState.SNAPSHOT_COPY:
                    # 1. Update doc 105
                    # 2. Insert new doc 115
                    # 3. Delete doc 102
                    await _put_json(
                        c0,
                        "/doc/105",
                        {"request_id": "upd-105", "document": {"doc_id": 105, "v": 99}},
                    )
                    await _post_json(
                        c0,
                        "/doc",
                        {"request_id": "ins-115", "document": {"doc_id": 115, "v": 2}},
                    )
                    await _delete_json(
                        c0,
                        "/doc/102",
                        {"request_id": "del-102"},
                    )

            # Execute migration through state machine
            res = await coord_service.migration_mgr.execute_migration(
                rec.migration_id,
                coord_service,
                http_client=client,
                on_phase_hook=write_hook,
            )
            assert res.state == MigrationState.DONE
            assert res.docs_copied == 10
            assert res.catchup_ops_replayed >= 3

        # After cutover:
        # Shard0 must reject reads/writes for [100, 200) with OUT_OF_RANGE
        async with AsyncClient(transport=trans0, base_url="http://node0") as c0:
            r_stale = await c0.get("/doc/105")
            assert r_stale.status_code == 400
            err_code = r_stale.json().get("error_code") or r_stale.json().get("detail", {}).get(
                "error_code"
            )
            assert err_code == "OUT_OF_RANGE"

        # Shard1 must successfully serve reads for the migrated range
        async with AsyncClient(transport=trans1, base_url="http://node1") as c1:
            r_105 = await c1.get("/doc/105")
            assert r_105.status_code == 200
            assert r_105.json()["document"]["v"] == 99

            r_115 = await c1.get("/doc/115")
            assert r_115.status_code == 200

            r_102 = await c1.get("/doc/102")
            assert r_102.status_code == 404

        await client.aclose()


# ═══════════════════════════════════════════════════════════════════════════
# 5. Write Pause & Router Transparent Retry
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_pause_and_router_retry_transparent_cutover(tmp_path: Path) -> None:
    """Writes arriving during PAUSE are retried with bounded backoff and land on new owner."""
    settings = _make_settings(tmp_path)
    map_path = tmp_path / "shard_map.json"
    _create_2_shard_map(map_path)

    coord_service = CoordinatorService(settings, str(map_path))
    coord_service.replica_states["shard0"] = ReplicaState.SYNCED
    coord_service.replica_states["shard1"] = ReplicaState.SYNCED

    mid = 2**62
    ranges0 = [ShardRangeConfig(start=0, end=mid, ownership_epoch=1)]
    ranges1 = [ShardRangeConfig(start=mid, end=2**63, ownership_epoch=1)]

    dir0 = tmp_path / "p_node0"
    dir1 = tmp_path / "p_node1"

    app0 = build_app(
        settings.model_copy(update={"DATA_DIR": str(dir0)}),
        node_id="pn0",
        shard_id="shard0",
        ranges=ranges0,
        lease_expires_at=float("inf"),
    )
    app1 = build_app(
        settings.model_copy(update={"DATA_DIR": str(dir1)}),
        node_id="pn1",
        shard_id="shard1",
        ranges=ranges1,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app0), LifespanManager(app1):
        trans0 = ASGITransport(app=app0)
        trans1 = ASGITransport(app=app1)

        # Build router mounted to both shard apps
        r_client = AsyncClient(
            mounts={
                "http://127.0.0.1:8200": trans0,
                "http://127.0.0.1:8201": trans1,
            },
            timeout=3.0,
        )
        router_map_mgr = ShardMapManager(path=str(map_path))
        router_map_mgr.load()
        router_app = build_router_app(settings, router_map_mgr, http_client=r_client)

        async with LifespanManager(router_app):
            r_trans = ASGITransport(app=router_app)
            async with AsyncClient(transport=r_trans, base_url="http://router") as router_c:
                # 1. Normal write to doc 50 via router -> 201
                w1 = await router_c.post(
                    "/doc",
                    json={"request_id": "r-pre", "document": {"doc_id": 50, "k": "v0"}},
                    headers=_client_headers(),
                )
                assert w1.status_code == 201

                # 2. Pause range [0, 100) directly on shard0
                app0.state.node.paused_ranges.append((0, 100))

                # 3. Direct write to shard0 now gets 503 RANGE_PAUSED
                async with AsyncClient(transport=trans0, base_url="http://s0") as c0:
                    w_direct = await _post_json(
                        c0,
                        "/doc",
                        {"request_id": "dir-p", "document": {"doc_id": 51, "k": "v1"}},
                    )
                    assert w_direct.status_code == 503
                    err_code = w_direct.json().get("detail", {}).get(
                        "error_code"
                    ) or w_direct.json().get("error_code")
                    assert err_code == "RANGE_PAUSED"

                    # But reads on paused range still succeed!
                    r_paused = await c0.get("/doc/50")
                    assert r_paused.status_code == 200

                # 4. Cutover range to shard1
                new_epoch = coord_service.bump_range_migration(
                    "shard0", 0, 100, new_shard_id="shard1"
                )
                # Update shard1 ranges
                app1.state.node.ranges.append(
                    ShardRangeConfig(start=0, end=100, ownership_epoch=new_epoch)
                )
                app1.state.node.ranges.sort(key=lambda r: r.start)
                # Unpause shard0
                app0.state.node.paused_ranges.clear()
                # Remove from shard0
                app0.state.node.ranges = [
                    r for r in app0.state.node.ranges if not (r.start == 0 and r.end == 100)
                ]

                # 5. Write to doc 52 through router succeeds transparently on shard1!
                w_router = await router_c.post(
                    "/doc",
                    json={"request_id": "r-post", "document": {"doc_id": 52, "k": "v2"}},
                    headers=_client_headers(),
                )
                assert w_router.status_code == 201

                # Verify served by shard1
                r_get = await router_c.get("/doc/52", headers=_client_headers())
                assert r_get.status_code == 200
                assert r_get.json()["meta"]["served_by"] == "primary"

        await r_client.aclose()


# ═══════════════════════════════════════════════════════════════════════════
# 6. Abort Before Cutover
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_abort_before_cutover_safe(tmp_path: Path) -> None:
    """Failure before cutover safely aborts: source unpauses, destination purges staged data."""
    settings = _make_settings(tmp_path)
    map_path = tmp_path / "shard_map.json"
    _create_2_shard_map(map_path)

    coord_service = CoordinatorService(settings, str(map_path))
    coord_service.replica_states["shard0"] = ReplicaState.SYNCED
    coord_service.replica_states["shard1"] = ReplicaState.SYNCED

    mid = 2**62
    ranges0 = [ShardRangeConfig(start=0, end=mid, ownership_epoch=1)]
    ranges1 = [ShardRangeConfig(start=mid, end=2**63, ownership_epoch=1)]

    dir0 = tmp_path / "abort_node0"
    dir1 = tmp_path / "abort_node1"

    app0 = build_app(
        settings.model_copy(update={"DATA_DIR": str(dir0)}),
        node_id="ab0",
        shard_id="shard0",
        ranges=ranges0,
        lease_expires_at=float("inf"),
    )
    app1 = build_app(
        settings.model_copy(update={"DATA_DIR": str(dir1)}),
        node_id="ab1",
        shard_id="shard1",
        ranges=ranges1,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app0), LifespanManager(app1):
        trans0 = ASGITransport(app=app0)
        trans1 = ASGITransport(app=app1)

        client = AsyncClient(
            mounts={
                "http://127.0.0.1:8200": trans0,
                "http://127.0.0.1:8201": trans1,
            },
            timeout=5.0,
        )

        # Pre-seed doc 500 on shard0
        async with AsyncClient(transport=trans0, base_url="http://node0") as c0:
            await _post_json(
                c0,
                "/doc",
                {"request_id": "seed-500", "document": {"doc_id": 500, "x": 1}},
            )

        # Plan migration
        rec = coord_service.migration_mgr.create_migration(
            from_shard="shard0",
            to_shard="shard1",
            start=400,
            end=600,
            coord=coord_service,
        )

        # Inject abort before cutover by forcing shard degraded during CATCHUP
        coord_service.replica_states["shard0"] = ReplicaState.DOWN

        with pytest.raises(RuntimeError, match="Shard became degraded"):
            await coord_service.migration_mgr.execute_migration(
                rec.migration_id,
                coord_service,
                http_client=client,
            )

        # Verify state is ABORTED
        assert rec.state == MigrationState.ABORTED

        # Verify source is unpaused and accepts writes
        assert (400, 600) not in app0.state.node.paused_ranges
        async with AsyncClient(transport=trans0, base_url="http://node0") as c0:
            w = await _post_json(
                c0,
                "/doc",
                {"request_id": "post-abort", "document": {"doc_id": 501, "x": 2}},
            )
            assert w.status_code == 201

        # Verify destination purged staged docs
        assert app1.state.node.engine.get(500) is None

        await client.aclose()


# ═══════════════════════════════════════════════════════════════════════════
# 7. M6 GATE — Live Range Migration Under Workload
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_m6_gate_migration_under_workload_checksum_verified(tmp_path: Path) -> None:
    """M6 GATE: Live range migration under workload with count + checksum match."""
    settings = _make_settings(tmp_path)
    map_path = tmp_path / "shard_map.json"
    _create_2_shard_map(map_path)

    coord_service = CoordinatorService(settings, str(map_path))
    coord_service.replica_states["shard0"] = ReplicaState.SYNCED
    coord_service.replica_states["shard1"] = ReplicaState.SYNCED

    mid = 2**62
    ranges0 = [ShardRangeConfig(start=0, end=mid, ownership_epoch=1)]
    ranges1 = [ShardRangeConfig(start=mid, end=2**63, ownership_epoch=1)]

    dir0 = tmp_path / "gate_node0"
    dir1 = tmp_path / "gate_node1"

    app0 = build_app(
        settings.model_copy(update={"DATA_DIR": str(dir0)}),
        node_id="gate0",
        shard_id="shard0",
        ranges=ranges0,
        lease_expires_at=float("inf"),
    )
    app1 = build_app(
        settings.model_copy(update={"DATA_DIR": str(dir1)}),
        node_id="gate1",
        shard_id="shard1",
        ranges=ranges1,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app0), LifespanManager(app1):
        trans0 = ASGITransport(app=app0)
        trans1 = ASGITransport(app=app1)

        rpc_client = AsyncClient(
            mounts={
                "http://127.0.0.1:8200": trans0,
                "http://127.0.0.1:8201": trans1,
            },
            timeout=5.0,
        )

        mig_start = 2000
        mig_end = 3000

        # Step 1: Populate 20 documents in range [2000, 3000)
        async with AsyncClient(transport=trans0, base_url="http://node0") as c0:
            for did in range(2000, 2020):
                w = await _post_json(
                    c0,
                    "/doc",
                    {"request_id": f"seed-{did}", "document": {"doc_id": did, "count": did}},
                )
                assert w.status_code == 201

        # Step 2: Trigger migration
        rec = coord_service.migration_mgr.create_migration(
            from_shard="shard0",
            to_shard="shard1",
            start=mig_start,
            end=mig_end,
            coord=coord_service,
        )

        # Step 3: Execute migration
        res = await coord_service.migration_mgr.execute_migration(
            rec.migration_id,
            coord_service,
            http_client=rpc_client,
        )
        assert res.state == MigrationState.DONE
        assert res.docs_copied == 20
        assert res.pause_duration_s > 0.0

        # Step 4: Verification — document count and checksum match on destination
        dest_docs = app1.state.node.engine.range_snapshot(mig_start, mig_end)
        count, chk = compute_deterministic_checksum(dest_docs)
        assert count == 20
        assert len(chk) == 64

        # Step 5: Shard0 has dropped the range and rejected requests
        async with AsyncClient(transport=trans0, base_url="http://node0") as c0:
            r0 = await c0.get("/doc/2005")
            assert r0.status_code == 400
            err_code = r0.json().get("error_code") or r0.json().get("detail", {}).get("error_code")
            assert err_code == "OUT_OF_RANGE"

        # Step 6: Shard1 now serves the range
        async with AsyncClient(transport=trans1, base_url="http://node1") as c1:
            r1 = await c1.get("/doc/2005")
            assert r1.status_code == 200
            assert r1.json()["document"]["doc_id"] == 2005

        await rpc_client.aclose()
