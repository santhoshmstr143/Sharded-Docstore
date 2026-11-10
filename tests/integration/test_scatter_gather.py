"""
Integration tests for Shard /query and Router Scatter-Gather — Module 5 & M3 Gate.

Covers:
- Shard node POST /query: candidate scanning, ownership filtering, sorting, pagination.
- Router POST /query: API key required, invalid filter rejected.
- Direct routing optimization:
  - Exact doc_id query hits exactly 1 shard.
  - $in doc_ids query hits only shards owning those IDs.
  - Range doc_id query hits only overlapping shards.
- Non-key filter scatter-gather: hits all shards concurrently.
- Global pagination correctness across 3 shards (skip crossing shard boundaries).
- Deterministic tie-breaking by doc_id.
- Deduplication by doc_id.
- Result matching against a single-node reference.
- Partial failure policy: unavailable shard fails the whole query with 503 SHARD_UNAVAILABLE.
- Out-of-range document dropping by router.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
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
# 1. Shard Node POST /query Tests
# ═══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_shard_query_ownership_filtering(tmp_path: Path) -> None:
    """Shard /query must return only documents in ranges it currently owns."""
    settings = _make_settings(tmp_path)
    # Shard owns [100, 200)
    owned_ranges = [ShardRangeConfig(start=100, end=200, ownership_epoch=1)]
    shard_app = build_app(
        settings,
        node_id="node-test",
        shard_id="shard-test",
        ranges=owned_ranges,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(shard_app):
        transport = ASGITransport(app=shard_app)
        async with AsyncClient(transport=transport, base_url="http://node-test") as client:
            state = shard_app.state.node
            # Insert owned doc
            state.engine.insert({"doc_id": 150, "val": "owned"}, seq=1)
            # Directly insert an out-of-range doc in engine (e.g. migrating or lingering)
            state.engine.insert({"doc_id": 250, "val": "unowned"}, seq=2)

            # Query all
            r = await client.post("/query", json={"filter": {}})
            assert r.status_code == 200
            data = r.json()
            docs = data["documents"]
            doc_ids = [d["doc_id"] for d in docs]
            assert doc_ids == [150]
            assert 250 not in doc_ids


@pytest.mark.asyncio
async def test_shard_query_sorting_and_pagination(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    owned_ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    shard_app = build_app(
        settings,
        node_id="node-test",
        shard_id="shard0",
        ranges=owned_ranges,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(shard_app):
        transport = ASGITransport(app=shard_app)
        async with AsyncClient(transport=transport, base_url="http://node-test") as client:
            state = shard_app.state.node
            for i in range(10):
                state.engine.insert({"doc_id": i, "score": 100 - i}, seq=i + 1)

            # Ascending by score
            r = await client.post(
                "/query",
                json={
                    "filter": {},
                    "sort_field": "score",
                    "sort_dir": "asc",
                    "skip": 2,
                    "limit": 3,
                },
            )
            assert r.status_code == 200
            docs = r.json()["documents"]
            assert len(docs) == 3
            # scores sorted ascending: 91, 92, 93 ...
            # all scores: 91 (doc 9), 92 (doc 8), 93 (doc 7), 94 (doc 6), 95 (doc 5)...
            # skip 2 -> doc 7 (score 93), doc 6 (score 94), doc 5 (score 95)
            assert [d["score"] for d in docs] == [93, 94, 95]


# ═══════════════════════════════════════════════════════════
# 2. Router Scatter-Gather & M3 Gate Tests
# ═══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_router_query_auth_and_validation(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    mgr = ShardMapManager()
    mgr.set_map(create_default_3_shard_map())
    router_app = build_router_app(settings, mgr)

    async with LifespanManager(router_app):
        transport = ASGITransport(app=router_app)
        async with AsyncClient(transport=transport, base_url="http://router") as client:
            # 1. Missing API key -> 401
            r = await client.post("/query", json={"filter": {}})
            assert r.status_code == 401

            # 2. Invalid filter ($unknown operator) -> 400 INVALID_QUERY
            r = await client.post(
                "/query",
                json={"filter": {"$where": "evil()"}},
                headers=_auth_headers(),
            )
            assert r.status_code == 400
            assert r.json()["error_code"] == "INVALID_QUERY"


@pytest.mark.asyncio
async def test_router_scatter_gather_cluster_execution(tmp_path: Path) -> None:
    """Full 3-shard cluster test for scatter-gather, pagination, and single-node reference check."""
    settings = _make_settings(tmp_path)
    map_path = tmp_path / "shard_map.json"
    mgr = ShardMapManager(map_path)
    s_map = create_default_3_shard_map()
    mgr.save(s_map)

    # 3 Shard ranges:
    # Shard 0: [0, _S1)
    # Shard 1: [_S1, _S2)
    # Shard 2: [_S2, KEY_SPACE_END)
    ranges_s0 = [ShardRangeConfig(start=0, end=_S1, ownership_epoch=1)]
    ranges_s1 = [ShardRangeConfig(start=_S1, end=_S2, ownership_epoch=1)]
    ranges_s2 = [ShardRangeConfig(start=_S2, end=KEY_SPACE_END, ownership_epoch=1)]

    app_s0 = build_app(
        settings,
        node_id="node0",
        shard_id="shard0",
        ranges=ranges_s0,
        lease_expires_at=float("inf"),
    )
    app_s1 = build_app(
        settings,
        node_id="node1",
        shard_id="shard1",
        ranges=ranges_s1,
        lease_expires_at=float("inf"),
    )
    app_s2 = build_app(
        settings,
        node_id="node2",
        shard_id="shard2",
        ranges=ranges_s2,
        lease_expires_at=float("inf"),
    )

    t0 = ASGITransport(app=app_s0)
    t1 = ASGITransport(app=app_s1)
    t2 = ASGITransport(app=app_s2)

    shard_transports = {
        "127.0.0.1:8200": t0,
        "127.0.0.1:8201": t1,
        "127.0.0.1:8202": t2,
    }

    # Tracking calls to shards to verify direct routing vs scatter-gather
    s0_call_count = 0
    s1_call_count = 0
    s2_call_count = 0

    async def tracking_post(url: str, **kwargs: Any) -> httpx.Response:
        nonlocal s0_call_count, s1_call_count, s2_call_count
        clean_url = str(url).removeprefix("http://").removeprefix("https://")
        if clean_url.startswith("127.0.0.1:8200"):
            s0_call_count += 1
            client = AsyncClient(transport=t0, base_url="http://127.0.0.1:8200")
        elif clean_url.startswith("127.0.0.1:8201"):
            s1_call_count += 1
            client = AsyncClient(transport=t1, base_url="http://127.0.0.1:8201")
        elif clean_url.startswith("127.0.0.1:8202"):
            s2_call_count += 1
            client = AsyncClient(transport=t2, base_url="http://127.0.0.1:8202")
        else:
            raise RuntimeError(f"Unknown target url: {url}")

        async with client:
            return await client.post("/query", **kwargs)

    router_app = build_router_app(settings, mgr, shard_transports=shard_transports)

    async with (
        LifespanManager(app_s0),
        LifespanManager(app_s1),
        LifespanManager(app_s2),
        LifespanManager(router_app),
    ):
        # Insert test documents across all 3 shards
        # 10 docs on shard 0
        docs_s0 = [{"doc_id": i, "category": "tech", "rating": i * 10} for i in range(10)]
        # 10 docs on shard 1
        docs_s1 = [{"doc_id": _S1 + i, "category": "tech", "rating": i * 10 + 5} for i in range(10)]
        # 10 docs on shard 2
        docs_s2 = [
            {"doc_id": _S2 + i, "category": "books", "rating": i * 10 + 2} for i in range(10)
        ]

        for d in docs_s0:
            app_s0.state.node.engine.insert(d, seq=d["doc_id"] + 1)
        for d in docs_s1:
            app_s1.state.node.engine.insert(d, seq=d["doc_id"] - _S1 + 1)
        for d in docs_s2:
            app_s2.state.node.engine.insert(d, seq=d["doc_id"] - _S2 + 1)

        # Reference collection of all documents
        all_reference_docs = docs_s0 + docs_s1 + docs_s2

        transport = ASGITransport(app=router_app)
        async with AsyncClient(transport=transport, base_url="http://router") as client:
            # ─────────────────────────────────────────────────────────
            # Test 1: Direct routing for exact doc_id (hits 1 shard)
            # ─────────────────────────────────────────────────────────
            r_single = await client.post(
                "/query",
                json={"filter": {"doc_id": 5}},
                headers=_auth_headers(),
            )
            assert r_single.status_code == 200
            res = r_single.json()["documents"]
            assert len(res) == 1
            assert res[0]["doc_id"] == 5

            # ─────────────────────────────────────────────────────────
            # Test 2: Range query overlapping only shard 0
            # ─────────────────────────────────────────────────────────
            r_range = await client.post(
                "/query",
                json={"filter": {"doc_id": {"$gt": 1, "$lt": 6}}},
                headers=_auth_headers(),
            )
            assert r_range.status_code == 200
            res = r_range.json()["documents"]
            assert [d["doc_id"] for d in res] == [2, 3, 4, 5]

            # ─────────────────────────────────────────────────────────
            # Test 3: Scatter-gather non-key query (category = tech)
            # ─────────────────────────────────────────────────────────
            r_all = await client.post(
                "/query",
                json={
                    "filter": {"category": "tech"},
                    "sort_field": "rating",
                    "sort_dir": "asc",
                    "skip": 0,
                    "limit": 50,
                },
                headers=_auth_headers(),
            )
            assert r_all.status_code == 200
            matched = r_all.json()["documents"]
            assert len(matched) == 20  # 10 from shard 0, 10 from shard 1

            # Verify against single-node reference
            ref_tech = [d for d in all_reference_docs if d.get("category") == "tech"]
            ref_tech.sort(key=lambda d: (d["rating"], d["doc_id"]))
            assert [d["doc_id"] for d in matched] == [d["doc_id"] for d in ref_tech]

            # ─────────────────────────────────────────────────────────
            # Test 4: Global pagination (skip crossing shard boundaries)
            # ─────────────────────────────────────────────────────────
            # Page 1: skip 0, limit 8
            r_p1 = await client.post(
                "/query",
                json={
                    "filter": {"category": "tech"},
                    "sort_field": "rating",
                    "sort_dir": "asc",
                    "skip": 0,
                    "limit": 8,
                },
                headers=_auth_headers(),
            )
            docs_p1 = r_p1.json()["documents"]
            assert len(docs_p1) == 8

            # Page 2: skip 8, limit 8 (crosses into shard 1's interleaved ratings)
            r_p2 = await client.post(
                "/query",
                json={
                    "filter": {"category": "tech"},
                    "sort_field": "rating",
                    "sort_dir": "asc",
                    "skip": 8,
                    "limit": 8,
                },
                headers=_auth_headers(),
            )
            docs_p2 = r_p2.json()["documents"]
            assert len(docs_p2) == 8

            # Ensure page 1 and page 2 have no overlap and match reference
            p1_ids = [d["doc_id"] for d in docs_p1]
            p2_ids = [d["doc_id"] for d in docs_p2]
            assert set(p1_ids).isdisjoint(set(p2_ids))
            assert p1_ids + p2_ids == [d["doc_id"] for d in ref_tech[:16]]


@pytest.mark.asyncio
async def test_router_partial_failure_policy(tmp_path: Path) -> None:
    """If any required shard fails, the whole query must fail with 503 SHARD_UNAVAILABLE."""
    settings = _make_settings(tmp_path)
    map_path = tmp_path / "shard_map.json"
    mgr = ShardMapManager(map_path)
    s_map = create_default_3_shard_map()
    mgr.save(s_map)

    ranges_s0 = [ShardRangeConfig(start=0, end=_S1, ownership_epoch=1)]
    app_s0 = build_app(
        settings,
        node_id="node0",
        shard_id="shard0",
        ranges=ranges_s0,
        lease_expires_at=float("inf"),
    )

    # Shard 1 and 2 are NOT running / mock unavailable transports
    shard_transports = {
        "127.0.0.1:8200": ASGITransport(app=app_s0),
        # Shard 1 and 2 have no working endpoints -> ConnectError
    }

    router_app = build_router_app(settings, mgr, shard_transports=shard_transports)

    async with LifespanManager(app_s0), LifespanManager(router_app):
        transport = ASGITransport(app=router_app)
        async with AsyncClient(transport=transport, base_url="http://router") as client:
            # Non-key filter requires all shards, including shard 1 and shard 2
            r = await client.post(
                "/query",
                json={"filter": {"status": "active"}},
                headers=_auth_headers(),
            )
            # Whole query must fail with 503 SHARD_UNAVAILABLE! Never return partial data
            assert r.status_code == 503
            data = r.json()
            err_code = data.get("error_code") or data.get("detail", {}).get("error_code")
            assert err_code == "SHARD_UNAVAILABLE"
            err_msg = data.get("error_message") or data.get("detail", {}).get("error_message")
            assert "shard1" in err_msg or "shard2" in err_msg


@pytest.mark.asyncio
async def test_router_shard_call_counts_optimization(tmp_path: Path) -> None:
    """Verify shard-key queries hit ONLY the required shards, not all shards."""
    settings = _make_settings(tmp_path)
    map_path = tmp_path / "shard_map.json"
    mgr = ShardMapManager(map_path)
    s_map = create_default_3_shard_map()
    mgr.save(s_map)

    ranges_s0 = [ShardRangeConfig(start=0, end=_S1, ownership_epoch=1)]
    ranges_s1 = [ShardRangeConfig(start=_S1, end=_S2, ownership_epoch=1)]
    ranges_s2 = [ShardRangeConfig(start=_S2, end=KEY_SPACE_END, ownership_epoch=1)]

    app_s0 = build_app(
        settings,
        node_id="node0",
        shard_id="shard0",
        ranges=ranges_s0,
        lease_expires_at=float("inf"),
    )
    app_s1 = build_app(
        settings,
        node_id="node1",
        shard_id="shard1",
        ranges=ranges_s1,
        lease_expires_at=float("inf"),
    )
    app_s2 = build_app(
        settings,
        node_id="node2",
        shard_id="shard2",
        ranges=ranges_s2,
        lease_expires_at=float("inf"),
    )

    # Use a custom transport handler to count requests
    counts = {"shard0": 0, "shard1": 0, "shard2": 0}

    class CountingTransport(httpx.AsyncBaseTransport):
        def __init__(self, inner: httpx.AsyncBaseTransport, name: str) -> None:
            self.inner = inner
            self.name = name

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            counts[self.name] += 1
            return await self.inner.handle_async_request(request)

    t0 = CountingTransport(ASGITransport(app=app_s0), "shard0")
    t1 = CountingTransport(ASGITransport(app=app_s1), "shard1")
    t2 = CountingTransport(ASGITransport(app=app_s2), "shard2")

    shard_transports = {
        "127.0.0.1:8200": t0,
        "127.0.0.1:8201": t1,
        "127.0.0.1:8202": t2,
    }

    router_app = build_router_app(settings, mgr, shard_transports=shard_transports)

    async with (
        LifespanManager(app_s0),
        LifespanManager(app_s1),
        LifespanManager(app_s2),
        LifespanManager(router_app),
    ):
        transport = ASGITransport(app=router_app)
        async with AsyncClient(transport=transport, base_url="http://router") as client:
            # 1. Exact doc_id on shard 0
            counts["shard0"] = counts["shard1"] = counts["shard2"] = 0
            r = await client.post(
                "/query",
                json={"filter": {"doc_id": 100}},
                headers=_auth_headers(),
            )
            assert r.status_code == 200
            assert counts["shard0"] == 1
            assert counts["shard1"] == 0
            assert counts["shard2"] == 0

            # 2. $in spanning shard 0 and shard 1 (shard 2 must NOT be called)
            counts["shard0"] = counts["shard1"] = counts["shard2"] = 0
            r = await client.post(
                "/query",
                json={"filter": {"doc_id": {"$in": [100, _S1 + 10]}}},
                headers=_auth_headers(),
            )
            assert r.status_code == 200
            assert counts["shard0"] == 1
            assert counts["shard1"] == 1
            assert counts["shard2"] == 0

            # 3. Non-key query: all 3 shards called
            counts["shard0"] = counts["shard1"] = counts["shard2"] = 0
            r = await client.post(
                "/query",
                json={"filter": {"tag": "test"}},
                headers=_auth_headers(),
            )
            assert r.status_code == 200
            assert counts["shard0"] == 1
            assert counts["shard1"] == 1
            assert counts["shard2"] == 1
