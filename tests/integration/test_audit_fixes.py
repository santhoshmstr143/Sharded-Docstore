"""Regression tests for the bugs found in the code audit (each failed before the fix)."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from docstore.common.auth import (
    CLIENT_KEY_HEADER,
    CLUSTER_SIG_HEADER,
    compute_signature,
    verify_signature,
)
from docstore.common.config import Settings
from docstore.common.models import NodeRole
from docstore.coordinator.app import build_coordinator_app
from docstore.coordinator.coordinator import CoordinatorService
from docstore.query.evaluator import evaluate_filter
from docstore.query.shard_key import detect_shard_key_constraint
from docstore.query.sorter import sort_documents
from docstore.router.app import build_router_app
from docstore.router.shard_map import ShardMapManager, create_default_3_shard_map
from docstore.shard.app import ShardRangeConfig, build_app
from docstore.storage.engine import StorageEngine

pytestmark = pytest.mark.integration
SECRET = "a" * 32
CLIENT_KEY = "c" * 16


def _settings(d: Path, **kw: Any) -> Settings:
    base = {
        "CLUSTER_SECRET": SECRET,
        "ADMIN_TOKEN": "b" * 32,
        "CLIENT_API_KEY": CLIENT_KEY,
        "DATA_DIR": str(d),
    }
    base.update(kw)
    return Settings.model_validate(base)


def _sig(body: dict[str, Any]) -> tuple[bytes, dict[str, str]]:
    raw = json.dumps(body, separators=(",", ":")).encode()
    return raw, {
        "Content-Type": "application/json",
        CLUSTER_SIG_HEADER: compute_signature(SECRET, raw),
    }


def _standalone(d: Path, ranges: list[ShardRangeConfig] | None = None) -> Any:
    return build_app(_settings(d), node_id="n", shard_id="s0", ranges=ranges, allow_standalone=True)


# ── query layer ──────────────────────────────────────────────────────────────


def test_float_bounds_route_to_the_right_range() -> None:
    lt = detect_shard_key_constraint({"doc_id": {"$lt": 5.5}})
    assert (lt.range_start, lt.range_end) == (0, 6)  # doc 5 < 5.5 must be included
    gt = detect_shard_key_constraint({"doc_id": {"$gt": -0.5}})
    assert gt.range_start == 0  # doc 0 > -0.5 must be included


def test_non_finite_numbers_never_crash_routing() -> None:
    for bad in (float("nan"), float("inf")):
        for f in ({"doc_id": bad}, {"doc_id": {"$gt": bad}}, {"doc_id": {"$in": [bad]}}):
            detect_shard_key_constraint(f)  # must not raise


def test_nested_bool_is_not_number() -> None:
    assert not evaluate_filter({"a": [1]}, {"a": [True]})
    assert not evaluate_filter({"a": {"x": 1}}, {"a": {"x": True}})
    assert evaluate_filter({"a": [1, "x"]}, {"a": [1, "x"]})


def test_sort_exact_for_large_and_huge_ints() -> None:
    docs = [{"doc_id": 1, "v": 2**53 + 1}, {"doc_id": 2, "v": 2**53}]
    assert [d["doc_id"] for d in sort_documents(docs, "v")] == [2, 1]
    sort_documents([{"doc_id": 1, "v": 10**400}, {"doc_id": 2, "v": 1}], "v")  # no OverflowError


def test_engine_returns_independent_copies() -> None:
    e = StorageEngine()
    e.insert({"doc_id": 1, "tags": ["a"]}, seq=1)
    e.get(1)["tags"].append("X")  # type: ignore[index]
    assert e.get(1) == {"doc_id": 1, "tags": ["a"]}


def test_non_ascii_signature_is_rejected_not_crashed() -> None:
    assert verify_signature(SECRET, b"", "é" * 64) is False


# ── shard node ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_migrated_docs_survive_crash_restart(tmp_path: Path) -> None:
    app = _standalone(tmp_path)
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            body, h = _sig({"migration_id": "m", "documents": [{"doc_id": i} for i in range(1, 6)]})
            assert (
                await c.post("/internal/migration/apply-batch", content=body, headers=h)
            ).status_code == 200
    for f in tmp_path.rglob("snapshot.json"):  # emulate SIGKILL: no shutdown snapshot
        f.unlink()
    app2 = _standalone(tmp_path)
    async with LifespanManager(app2):
        n = app2.state.node
        assert n.engine.doc_count() == 5
        assert not n.pending_unresolved_writes


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [(500, 1000, [(0, 500)]), (0, 400, [(400, 1000)]), (300, 600, [(0, 300), (600, 1000)])],
)
async def test_cleanup_shrinks_prefix_suffix_and_interior(
    tmp_path: Path, start: int, end: int, expected: list[tuple[int, int]]
) -> None:
    app = _standalone(tmp_path, [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)])
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            body, h = _sig({"start": start, "end": end})
            assert (
                await c.post("/internal/migration/cleanup", content=body, headers=h)
            ).status_code == 200
            assert [(r.start, r.end) for r in app.state.node.ranges] == expected


@pytest.mark.asyncio
async def test_promote_never_lowers_term_and_primary_rejects_replication(tmp_path: Path) -> None:
    app = _standalone(tmp_path)
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            n = app.state.node

            def promotion(term: int) -> dict[str, Any]:
                return {
                    "shard_id": "s0",
                    "term": term,
                    "ownership_epochs": {f"{r.start}-{r.end}": r.ownership_epoch for r in n.ranges},
                    "lease": {"granted_at": 0.0, "expires_at": 3600.0},
                }

            body, h = _sig(promotion(n.term + 1))
            assert (await c.post("/internal/promote", content=body, headers=h)).status_code == 200
            body, h = _sig(promotion(1))  # lower than the node's term now
            assert (await c.post("/internal/promote", content=body, headers=h)).status_code == 409
            assert n.term >= 2
            body, h = _sig(
                {
                    "shard_id": "s0",
                    "term": n.term,
                    "seq": n.wal.next_seq(),
                    "op": "insert",
                    "doc_id": 9,
                    "request_id": "x",
                    "payload": {"doc_id": 9},
                }
            )
            assert (await c.post("/internal/replicate", content=body, headers=h)).status_code == 409


@pytest.mark.asyncio
async def test_follower_never_acks_a_conflicting_duplicate_seq(tmp_path: Path) -> None:
    app = build_app(_settings(tmp_path), node_id="f", shard_id="s0", role=NodeRole.FOLLOWER)
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:

            def msg(doc: int) -> tuple[bytes, dict[str, str]]:
                return _sig(
                    {
                        "shard_id": "s0",
                        "term": 1,
                        "seq": 1,
                        "op": "insert",
                        "doc_id": doc,
                        "request_id": "r",
                        "payload": {"doc_id": doc},
                    }
                )

            assert (
                await c.post("/internal/replicate", content=msg(1)[0], headers=msg(1)[1])
            ).json()["status"] == "ok"
            assert (
                await c.post("/internal/replicate", content=msg(1)[0], headers=msg(1)[1])
            ).json()["status"] == "ok"  # true duplicate
            assert (
                await c.post("/internal/replicate", content=msg(2)[0], headers=msg(2)[1])
            ).json()["status"] == "conflict"


@pytest.mark.asyncio
async def test_failed_recovery_node_does_not_heartbeat(tmp_path: Path) -> None:
    (tmp_path / "s0").mkdir()
    (tmp_path / "s0" / "shard.wal").write_bytes(
        b'{"seq":1,"term":1,"op":"insert","doc_id":1,"request_id":"x","payload":{"doc_id":1},"entry_id":"z","crc32":123}\n'
    )
    seen: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req.url.path)
        return httpx.Response(200, json=None)

    app = build_app(
        _settings(tmp_path, HEARTBEAT_INTERVAL_S=0.1),
        node_id="n",
        shard_id="s0",
        coordinator_url="http://coord",
        coordinator_transport=httpx.MockTransport(handler),
        lease_expires_at=float("inf"),
    )
    async with LifespanManager(app):
        await asyncio.sleep(0.5)
        assert app.state.node.ready is False
    assert seen == []


# ── router ───────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_router_auth_status_nan_and_chunked(tmp_path: Path) -> None:
    mgr = ShardMapManager()
    mgr.set_map(create_default_3_shard_map())
    app = build_router_app(_settings(tmp_path), mgr)
    async with LifespanManager(app):
        async with AsyncClient(
            transport=ASGITransport(app=app, raise_app_exceptions=False), base_url="http://r"
        ) as c:
            h = {CLIENT_KEY_HEADER: CLIENT_KEY, "Content-Type": "application/json"}
            assert (await c.get("/status")).status_code == 401
            assert (
                await c.post(
                    "/doc", content=b'{"x":1}', headers={"Content-Type": "application/json"}
                )
            ).status_code == 401
            for raw in (b'{"filter":{"doc_id":NaN}}', b'{"filter":{"doc_id":{"$gt":Infinity}}}'):
                assert (await c.post("/query", content=raw, headers=h)).status_code == 400

            async def gen() -> Any:
                yield b"{}"

            assert (await c.post("/doc", content=gen(), headers=h)).status_code == 411


# ── cluster wiring (what `python -m docstore.shard` actually does) ───────────


class _Flaky(httpx.AsyncBaseTransport):
    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner = inner
        self.fail = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self.fail:
            raise httpx.ConnectTimeout("simulated blip")
        return await self.inner.handle_async_request(request)


@pytest.mark.asyncio
async def test_registered_primary_keeps_lease_and_recovers_from_network_blip(
    tmp_path: Path,
) -> None:
    logging.disable(logging.CRITICAL)
    kw = {
        "HEARTBEAT_INTERVAL_S": 0.5,
        "LEASE_DURATION_S": 3.0,
        "FAILURE_TIMEOUT_S": 5.0,
        "SAFETY_MARGIN_S": 0.5,
        "REPLICATION_TIMEOUT_S": 1.0,
    }
    (tmp_path / "coord").mkdir()
    ShardMapManager(tmp_path / "coord" / "shard_map.json").save(
        create_default_3_shard_map(
            primaries=["p:8200", "x:1", "y:1"], followers=["fol:8201", None, None]
        )
    )
    svc = CoordinatorService(
        _settings(tmp_path / "coord", **kw), shard_map_path=tmp_path / "coord" / "shard_map.json"
    )
    capp = build_coordinator_app(_settings(tmp_path / "coord", **kw), svc)
    rng = [ShardRangeConfig(start=0, end=3074457345618258602, ownership_epoch=1)]
    pri = build_app(
        _settings(tmp_path / "p", **kw),
        node_id="p",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=rng,
        follower_url="fol:8201",
        advertise_addr="p:8200",
        coordinator_url="http://coord",
        coordinator_transport=ASGITransport(app=capp),
        lease_expires_at=time.monotonic() + 3.0,
    )
    fol = build_app(
        _settings(tmp_path / "f", **kw),
        node_id="f",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
        ranges=rng,
        advertise_addr="fol:8201",
        coordinator_url="http://coord",
        coordinator_transport=ASGITransport(app=capp),
        http_client=httpx.AsyncClient(mounts={"http://p:8200": ASGITransport(app=pri)}),
    )
    flaky = _Flaky(ASGITransport(app=fol))
    async with LifespanManager(capp), LifespanManager(fol), LifespanManager(pri):
        pri.state.node.http_client = httpx.AsyncClient(
            mounts={"http://fol:8201": flaky}, timeout=1.0
        )
        async with AsyncClient(transport=ASGITransport(app=pri), base_url="http://t") as c:

            def w(i: int) -> Any:
                return c.post("/doc", json={"request_id": f"r{i}", "document": {"doc_id": i}})

            assert (await w(1)).status_code == 201
            await asyncio.sleep(4.0)  # longer than the initial 3 s lease: must have been renewed
            assert sorted(svc.nodes) == ["f", "p"]
            assert (await w(2)).status_code == 201
            flaky.fail = True
            assert (await w(3)).status_code == 202
            flaky.fail = False
            for i in range(20):
                await asyncio.sleep(0.5)
                if (await w(100 + i)).status_code == 201:
                    break
            else:
                pytest.fail("writes never resumed after a transient network blip")
            assert pri.state.node.follower_state.value == "SYNCED"
