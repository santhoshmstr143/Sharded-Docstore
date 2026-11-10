"""
Integration tests for Router and M2 Gate — Module 4.

Covers:
- Client API key auth (missing, invalid, valid).
- Shard-key routing to 3 shards.
- CRUD through the router: insert, get, update, delete.
- Boundary keys: 0, s1-1, s1, s2-1, s2, 2^63-1.
- Idempotency: request_id pass-through and retry behavior.
- Stale-map refresh: router reloads map on OutOfRange/NotPrimary and retries once.
- Internal shard endpoints (/internal/*) are not reachable on router.
- Concurrency limiter.
- M2 GATE: 3 shard nodes + 1 router, 1000 docs distributed, restart persistence.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from docstore.common.auth import CLIENT_KEY_HEADER
from docstore.common.config import Settings
from docstore.router.app import build_router_app
from docstore.router.shard_map import (
    _S1,
    _S2,
    KEY_SPACE_END,
    ShardMapManager,
    create_default_3_shard_map,
)
from docstore.shard.app import ShardRangeConfig, build_app

pytestmark = pytest.mark.integration

SECRET = "a" * 32
ADMIN = "b" * 32
CLIENT_KEY = "c" * 16


def _make_settings(data_dir: Path | str) -> Settings:
    return Settings(
        CLUSTER_SECRET=SECRET,
        ADMIN_TOKEN=ADMIN,
        CLIENT_API_KEY=CLIENT_KEY,
        DATA_DIR=str(data_dir),
        STANDALONE_MODE=True,
    )


def _auth_headers() -> dict[str, str]:
    return {CLIENT_KEY_HEADER: CLIENT_KEY}


# ═══════════════════════════════════════════════════════════
# Authentication & Access Control Tests
# ═══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_router_auth_missing_key(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    mgr = ShardMapManager()
    mgr.set_map(create_default_3_shard_map())
    router_app = build_router_app(settings, mgr)

    async with LifespanManager(router_app):
        transport = ASGITransport(app=router_app)
        async with AsyncClient(transport=transport, base_url="http://router") as c:
            r = await c.post("/doc", json={"request_id": "r1", "document": {"doc_id": 1}})
            assert r.status_code == 401
            data = r.json()
            err_code = data.get("error_code") or data.get("detail", {}).get("error_code")
            assert err_code == "AUTH_FAILED"


@pytest.mark.asyncio
async def test_router_auth_wrong_key(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    mgr = ShardMapManager()
    mgr.set_map(create_default_3_shard_map())
    router_app = build_router_app(settings, mgr)

    async with LifespanManager(router_app):
        transport = ASGITransport(app=router_app)
        async with AsyncClient(transport=transport, base_url="http://router") as c:
            headers = {CLIENT_KEY_HEADER: "wrong-api-key-here"}
            r = await c.post(
                "/doc", json={"request_id": "r1", "document": {"doc_id": 1}}, headers=headers
            )
            assert r.status_code == 401
            data = r.json()
            err_code = data.get("error_code") or data.get("detail", {}).get("error_code")
            assert err_code == "AUTH_FAILED"


@pytest.mark.asyncio
async def test_internal_endpoints_not_exposed(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    mgr = ShardMapManager()
    mgr.set_map(create_default_3_shard_map())
    router_app = build_router_app(settings, mgr)

    async with LifespanManager(router_app):
        transport = ASGITransport(app=router_app)
        async with AsyncClient(transport=transport, base_url="http://router") as c:
            r1 = await c.get("/internal/status", headers=_auth_headers())
            assert r1.status_code == 404

            r2 = await c.post("/internal/replicate", headers=_auth_headers())
            assert r2.status_code == 404


# ═══════════════════════════════════════════════════════════
# 3-Shard Cluster Fixture & CRUD Tests
# ═══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_router_crud_and_routing(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)

    # 3 shards covering [0, s1), [s1, s2), [s2, 2^63)
    r0 = [ShardRangeConfig(start=0, end=_S1, ownership_epoch=1)]
    r1 = [ShardRangeConfig(start=_S1, end=_S2, ownership_epoch=1)]
    r2 = [ShardRangeConfig(start=_S2, end=KEY_SPACE_END, ownership_epoch=1)]

    app0 = build_app(
        settings, node_id="node0", shard_id="shard0", ranges=r0, lease_expires_at=float("inf")
    )
    app1 = build_app(
        settings, node_id="node1", shard_id="shard1", ranges=r1, lease_expires_at=float("inf")
    )
    app2 = build_app(
        settings, node_id="node2", shard_id="shard2", ranges=r2, lease_expires_at=float("inf")
    )

    async with LifespanManager(app0), LifespanManager(app1), LifespanManager(app2):
        transports = {
            "127.0.0.1:8200": ASGITransport(app=app0),
            "127.0.0.1:8201": ASGITransport(app=app1),
            "127.0.0.1:8202": ASGITransport(app=app2),
        }
        mgr = ShardMapManager()
        mgr.set_map(create_default_3_shard_map())

        router_app = build_router_app(settings, mgr, shard_transports=transports)
        async with LifespanManager(router_app):
            router_transport = ASGITransport(app=router_app)
            async with AsyncClient(transport=router_transport, base_url="http://router") as client:
                headers = _auth_headers()

                # 1. Insert documents targeting each shard
                # Shard 0: doc_id = 100
                r_ins0 = await client.post(
                    "/doc",
                    json={"request_id": "req-1", "document": {"doc_id": 100, "name": "Doc0"}},
                    headers=headers,
                )
                assert r_ins0.status_code == 201

                # Shard 1: doc_id = _S1 + 50
                doc1_id = _S1 + 50
                r_ins1 = await client.post(
                    "/doc",
                    json={"request_id": "req-2", "document": {"doc_id": doc1_id, "name": "Doc1"}},
                    headers=headers,
                )
                assert r_ins1.status_code == 201

                # Shard 2: doc_id = _S2 + 50
                doc2_id = _S2 + 50
                r_ins2 = await client.post(
                    "/doc",
                    json={"request_id": "req-3", "document": {"doc_id": doc2_id, "name": "Doc2"}},
                    headers=headers,
                )
                assert r_ins2.status_code == 201

                # Verify documents landed on their specific shard nodes
                assert app0.state.node.engine.get(100) is not None
                assert app1.state.node.engine.get(100) is None
                assert app1.state.node.engine.get(doc1_id) is not None
                assert app2.state.node.engine.get(doc2_id) is not None

                # 2. Get document
                r_get = await client.get("/doc/100", headers=headers)
                assert r_get.status_code == 200
                assert r_get.json()["document"]["name"] == "Doc0"

                # 3. Update document
                r_upd = await client.put(
                    "/doc/100",
                    json={
                        "request_id": "req-4",
                        "document": {"doc_id": 100, "name": "Doc0-Updated"},
                    },
                    headers=headers,
                )
                assert r_upd.status_code == 200
                r_get_updated = await client.get("/doc/100", headers=headers)
                assert r_get_updated.json()["document"]["name"] == "Doc0-Updated"

                # 4. Delete document
                r_del = await client.request(
                    "DELETE",
                    "/doc/100",
                    json={"request_id": "req-5"},
                    headers=headers,
                )
                assert r_del.status_code == 200
                r_get_missing = await client.get("/doc/100", headers=headers)
                assert r_get_missing.status_code == 404


# ═══════════════════════════════════════════════════════════
# Boundary Keys Tests
# ═══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_router_boundary_keys(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    r0 = [ShardRangeConfig(start=0, end=_S1, ownership_epoch=1)]
    r1 = [ShardRangeConfig(start=_S1, end=_S2, ownership_epoch=1)]
    r2 = [ShardRangeConfig(start=_S2, end=KEY_SPACE_END, ownership_epoch=1)]

    app0 = build_app(
        settings, node_id="node0", shard_id="shard0", ranges=r0, lease_expires_at=float("inf")
    )
    app1 = build_app(
        settings, node_id="node1", shard_id="shard1", ranges=r1, lease_expires_at=float("inf")
    )
    app2 = build_app(
        settings, node_id="node2", shard_id="shard2", ranges=r2, lease_expires_at=float("inf")
    )

    async with LifespanManager(app0), LifespanManager(app1), LifespanManager(app2):
        transports = {
            "127.0.0.1:8200": ASGITransport(app=app0),
            "127.0.0.1:8201": ASGITransport(app=app1),
            "127.0.0.1:8202": ASGITransport(app=app2),
        }
        mgr = ShardMapManager()
        mgr.set_map(create_default_3_shard_map())

        router_app = build_router_app(settings, mgr, shard_transports=transports)
        async with LifespanManager(router_app):
            router_transport = ASGITransport(app=router_app)
            async with AsyncClient(transport=router_transport, base_url="http://router") as client:
                headers = _auth_headers()

                boundary_keys = [
                    (0, app0),
                    (_S1 - 1, app0),
                    (_S1, app1),
                    (_S2 - 1, app1),
                    (_S2, app2),
                    (KEY_SPACE_END - 1, app2),
                ]

                for idx, (k, target_app) in enumerate(boundary_keys):
                    resp = await client.post(
                        "/doc",
                        json={"request_id": f"bnd-{idx}", "document": {"doc_id": k, "k": idx}},
                        headers=headers,
                    )
                    assert resp.status_code == 201
                    assert target_app.state.node.engine.get(k) is not None


# ═══════════════════════════════════════════════════════════
# Idempotency and Retries
# ═══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_router_idempotency_pass_through(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    r0 = [ShardRangeConfig(start=0, end=_S1, ownership_epoch=1)]
    r1 = [ShardRangeConfig(start=_S1, end=_S2, ownership_epoch=1)]
    r2 = [ShardRangeConfig(start=_S2, end=KEY_SPACE_END, ownership_epoch=1)]

    app0 = build_app(
        settings, node_id="node0", shard_id="shard0", ranges=r0, lease_expires_at=float("inf")
    )
    app1 = build_app(
        settings, node_id="node1", shard_id="shard1", ranges=r1, lease_expires_at=float("inf")
    )
    app2 = build_app(
        settings, node_id="node2", shard_id="shard2", ranges=r2, lease_expires_at=float("inf")
    )

    async with LifespanManager(app0), LifespanManager(app1), LifespanManager(app2):
        transports = {
            "127.0.0.1:8200": ASGITransport(app=app0),
            "127.0.0.1:8201": ASGITransport(app=app1),
            "127.0.0.1:8202": ASGITransport(app=app2),
        }
        mgr = ShardMapManager()
        mgr.set_map(create_default_3_shard_map())

        router_app = build_router_app(settings, mgr, shard_transports=transports)
        async with LifespanManager(router_app):
            router_transport = ASGITransport(app=router_app)
            async with AsyncClient(transport=router_transport, base_url="http://router") as client:
                headers = _auth_headers()
                body = {"request_id": "same-rid", "document": {"doc_id": 42, "msg": "hello"}}

                r1 = await client.post("/doc", json=body, headers=headers)
                r2 = await client.post("/doc", json=body, headers=headers)
                assert r1.status_code == 201
                assert r2.status_code == 200 or r2.status_code == 201
                assert r1.json()["outcome"] == r2.json()["outcome"]


# ═══════════════════════════════════════════════════════════
# Stale-map Refresh Test
# ═══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_router_stale_map_refresh_and_retry(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    map_path = tmp_path / "shard_map.json"

    # Start shard nodes
    r0 = [ShardRangeConfig(start=0, end=_S1, ownership_epoch=1)]
    r1 = [ShardRangeConfig(start=_S1, end=_S2, ownership_epoch=1)]
    r2 = [ShardRangeConfig(start=_S2, end=KEY_SPACE_END, ownership_epoch=1)]

    app0 = build_app(
        settings, node_id="node0", shard_id="shard0", ranges=r0, lease_expires_at=float("inf")
    )
    app1 = build_app(
        settings, node_id="node1", shard_id="shard1", ranges=r1, lease_expires_at=float("inf")
    )
    app2 = build_app(
        settings, node_id="node2", shard_id="shard2", ranges=r2, lease_expires_at=float("inf")
    )

    async with LifespanManager(app0), LifespanManager(app1), LifespanManager(app2):
        transports = {
            "127.0.0.1:8200": ASGITransport(app=app0),
            "127.0.0.1:8201": ASGITransport(app=app1),
            "127.0.0.1:8202": ASGITransport(app=app2),
        }

        # Save correct map to disk
        correct_map = create_default_3_shard_map(map_version=2)
        mgr = ShardMapManager(path=map_path)
        mgr.save(correct_map)

        # But initialize router with an initially stale map that mistakenly routes [0, s2) to shard0
        # When shard0 rejects doc_id in [s1, s2) with OutOfRange,
        # router reloads from disk and succeeds!
        stale_map = create_default_3_shard_map(map_version=1)
        # Point shard1's primary address initially to shard0 (simulating stale routing)
        stale_map.entries[1].primary = "127.0.0.1:8200"

        router_app = build_router_app(settings, mgr, shard_transports=transports)
        mgr._shard_map = stale_map  # inject stale map in router cache

        async with LifespanManager(router_app):
            router_transport = ASGITransport(app=router_app)
            async with AsyncClient(transport=router_transport, base_url="http://router") as client:
                headers = _auth_headers()
                doc_id = _S1 + 100  # belongs to shard1

                # Send write: initial attempt hits shard0 -> OutOfRange
                # -> router reloads map from disk -> retries shard1 -> success!
                resp = await client.post(
                    "/doc",
                    json={
                        "request_id": "stale-retry-1",
                        "document": {"doc_id": doc_id, "v": "recovered"},
                    },
                    headers=headers,
                )
                assert resp.status_code == 201
                assert app1.state.node.engine.get(doc_id) is not None


# ═══════════════════════════════════════════════════════════
# ✅ M2 GATE: 3 Shard Nodes + Router Running 1,000 Docs + Restart
# ═══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_m2_gate_1000_docs_distribution_and_restart(tmp_path: Path) -> None:
    """M2 Gate test:
    - 3 shard nodes + 1 router.
    - Insert 1,000 docs across full key range; verify distribution across all 3 shards.
    - Get / update / delete through router.
    - Restart all nodes -> verify data persists and routing remains correct.
    """
    data_dir = str(tmp_path / "cluster_data")
    settings = _make_settings(data_dir)

    r0 = [ShardRangeConfig(start=0, end=_S1, ownership_epoch=1)]
    r1 = [ShardRangeConfig(start=_S1, end=_S2, ownership_epoch=1)]
    r2 = [ShardRangeConfig(start=_S2, end=KEY_SPACE_END, ownership_epoch=1)]

    # ── Phase 1: Boot cluster, insert 1000 docs ───────────────────────────────
    app0 = build_app(
        settings, node_id="node0", shard_id="shard0", ranges=r0, lease_expires_at=float("inf")
    )
    app1 = build_app(
        settings, node_id="node1", shard_id="shard1", ranges=r1, lease_expires_at=float("inf")
    )
    app2 = build_app(
        settings, node_id="node2", shard_id="shard2", ranges=r2, lease_expires_at=float("inf")
    )

    doc_ids: list[int] = []

    async with LifespanManager(app0), LifespanManager(app1), LifespanManager(app2):
        transports = {
            "127.0.0.1:8200": ASGITransport(app=app0),
            "127.0.0.1:8201": ASGITransport(app=app1),
            "127.0.0.1:8202": ASGITransport(app=app2),
        }
        mgr = ShardMapManager()
        mgr.set_map(create_default_3_shard_map())

        router_app = build_router_app(settings, mgr, shard_transports=transports)
        async with LifespanManager(router_app):
            router_transport = ASGITransport(app=router_app)
            async with AsyncClient(transport=router_transport, base_url="http://router") as client:
                headers = _auth_headers()

                # Generate 1,000 doc_ids distributed evenly across shards:
                # 334 in shard0, 333 in shard1, 333 in shard2
                for i in range(334):
                    doc_ids.append(1000 + i)
                for i in range(333):
                    doc_ids.append(_S1 + 1000 + i)
                for i in range(333):
                    doc_ids.append(_S2 + 1000 + i)

                assert len(doc_ids) == 1000

                # Insert 1000 docs in batches concurrently
                async def _insert_one(idx: int, did: int) -> None:
                    res = await client.post(
                        "/doc",
                        json={
                            "request_id": f"req-1k-{idx}",
                            "document": {"doc_id": did, "num": idx},
                        },
                        headers=headers,
                    )
                    assert res.status_code == 201

                batch_size = 50
                for b_start in range(0, 1000, batch_size):
                    batch = [
                        _insert_one(j, doc_ids[j])
                        for j in range(b_start, min(b_start + batch_size, 1000))
                    ]
                    await asyncio.gather(*batch)

                # Verify distribution: every shard got roughly 1/3 of the documents!
                count0 = app0.state.node.engine.doc_count()
                count1 = app1.state.node.engine.doc_count()
                count2 = app2.state.node.engine.doc_count()

                assert count0 == 334, f"Shard 0 count {count0} != 334"
                assert count1 == 333, f"Shard 1 count {count1} != 333"
                assert count2 == 333, f"Shard 2 count {count2} != 333"
                assert count0 + count1 + count2 == 1000

                # Spot-check Get / Update / Delete through router
                test_id = doc_ids[0]
                g = await client.get(f"/doc/{test_id}", headers=headers)
                assert g.status_code == 200

                u = await client.put(
                    f"/doc/{test_id}",
                    json={"request_id": "upd-test", "document": {"doc_id": test_id, "num": 99999}},
                    headers=headers,
                )
                assert u.status_code == 200

                d = await client.request(
                    "DELETE",
                    f"/doc/{test_id}",
                    json={"request_id": "del-test"},
                    headers=headers,
                )
                assert d.status_code == 200

    # ── Phase 2: Restart all nodes & router -> verify persistence ─────────────
    app0_reboot = build_app(
        settings, node_id="node0", shard_id="shard0", ranges=r0, lease_expires_at=float("inf")
    )
    app1_reboot = build_app(
        settings, node_id="node1", shard_id="shard1", ranges=r1, lease_expires_at=float("inf")
    )
    app2_reboot = build_app(
        settings, node_id="node2", shard_id="shard2", ranges=r2, lease_expires_at=float("inf")
    )

    async with (
        LifespanManager(app0_reboot),
        LifespanManager(app1_reboot),
        LifespanManager(app2_reboot),
    ):
        transports_reboot = {
            "127.0.0.1:8200": ASGITransport(app=app0_reboot),
            "127.0.0.1:8201": ASGITransport(app=app1_reboot),
            "127.0.0.1:8202": ASGITransport(app=app2_reboot),
        }
        mgr_reboot = ShardMapManager()
        mgr_reboot.set_map(create_default_3_shard_map())

        router_reboot = build_router_app(settings, mgr_reboot, shard_transports=transports_reboot)
        async with LifespanManager(router_reboot):
            router_transport2 = ASGITransport(app=router_reboot)
            async with AsyncClient(transport=router_transport2, base_url="http://router") as client:
                headers = _auth_headers()

                # Verify all other 999 docs survived reboot
                assert app0_reboot.state.node.engine.doc_count() == 333  # 334 - 1 deleted
                assert app1_reboot.state.node.engine.doc_count() == 333
                assert app2_reboot.state.node.engine.doc_count() == 333

                # Spot-check reads across all 3 shards
                r_s0 = await client.get(f"/doc/{doc_ids[1]}", headers=headers)
                r_s1 = await client.get(f"/doc/{doc_ids[335]}", headers=headers)
                r_s2 = await client.get(f"/doc/{doc_ids[668]}", headers=headers)

                assert r_s0.status_code == 200
                assert r_s1.status_code == 200
                assert r_s2.status_code == 200
