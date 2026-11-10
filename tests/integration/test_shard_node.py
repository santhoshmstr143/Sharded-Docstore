"""
Integration tests for the shard node HTTP API — Module 3.

Uses httpx.AsyncClient with the FastAPI ASGI test transport (no real server).
Each test gets a fresh temp directory so WAL / snapshots are isolated.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from docstore.common.auth import CLUSTER_SIG_HEADER, compute_signature
from docstore.shard.app import build_app

pytestmark = pytest.mark.integration

# ── Helpers ───────────────────────────────────────────────────────────────────

SECRET = "a" * 32
ADMIN = "b" * 32
APIKEY = "c" * 16


def _make_settings(data_dir: str) -> Any:
    from docstore.common.config import Settings

    return Settings.model_validate(
        {
            "CLUSTER_SECRET": SECRET,
            "ADMIN_TOKEN": ADMIN,
            "CLIENT_API_KEY": APIKEY,
            "DATA_DIR": data_dir,
            "STANDALONE_MODE": True,
        }
    )


def _sig(body: bytes) -> dict[str, str]:
    return {CLUSTER_SIG_HEADER: compute_signature(SECRET, body)}


def _jbody(payload: dict[str, Any]) -> tuple[bytes, dict[str, str]]:
    """Return (content_bytes, headers) for a JSON request body."""
    body = json.dumps(payload).encode()
    return body, {"content-type": "application/json"}


def _rid() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def tmp_data(tmp_path: Path) -> str:
    return str(tmp_path)


@pytest_asyncio.fixture
async def client(tmp_data: str) -> AsyncClient:
    settings = _make_settings(tmp_data)
    app = build_app(settings, node_id="test-node", shard_id="shard0", lease_expires_at=float("inf"))
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


# ── Health / Status ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_health_ok(client: AsyncClient) -> None:
    r = await client.get("/health")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_status_fields(client: AsyncClient) -> None:
    r = await client.get("/status")
    assert r.status_code == 200
    data = r.json()
    assert data["ready"] is True
    assert "last_applied_seq" in data
    assert data["doc_count"] == 0


# ── Insert ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_insert_success(client: AsyncClient) -> None:
    r = await client.post(
        "/doc",
        json={"request_id": _rid(), "document": {"doc_id": 1, "name": "Alice"}},
    )
    assert r.status_code == 201
    data = r.json()
    assert data["outcome"] == "COMMITTED"
    assert data["seq"] == 1


@pytest.mark.asyncio
async def test_insert_zero_doc_id(client: AsyncClient) -> None:
    r = await client.post("/doc", json={"request_id": _rid(), "document": {"doc_id": 0}})
    assert r.status_code == 201


@pytest.mark.asyncio
async def test_insert_duplicate_returns_409(client: AsyncClient) -> None:
    await client.post("/doc", json={"request_id": _rid(), "document": {"doc_id": 10}})
    r = await client.post("/doc", json={"request_id": _rid(), "document": {"doc_id": 10}})
    assert r.status_code == 409
    assert r.json()["error_code"] == "DUPLICATE_KEY"


@pytest.mark.asyncio
async def test_insert_retry_same_request_id(client: AsyncClient) -> None:
    rid = _rid()
    body = {"request_id": rid, "document": {"doc_id": 20}}
    r1 = await client.post("/doc", json=body)
    r2 = await client.post("/doc", json=body)
    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r1.json()["seq"] == r2.json()["seq"]  # dedup: same seq returned


# ── Get ───────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_existing(client: AsyncClient) -> None:
    await client.post("/doc", json={"request_id": _rid(), "document": {"doc_id": 5, "x": 99}})
    r = await client.get("/doc/5")
    assert r.status_code == 200
    assert r.json()["document"]["x"] == 99


@pytest.mark.asyncio
async def test_get_missing_returns_404(client: AsyncClient) -> None:
    r = await client.get("/doc/9999")
    assert r.status_code == 404
    assert r.json()["error_code"] == "NOT_FOUND"


# ── Update ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_update_full_replace(client: AsyncClient) -> None:
    await client.post("/doc", json={"request_id": _rid(), "document": {"doc_id": 7, "a": 1}})
    r = await client.put("/doc/7", json={"request_id": _rid(), "document": {"doc_id": 7, "b": 2}})
    assert r.status_code == 200
    doc = (await client.get("/doc/7")).json()["document"]
    assert "a" not in doc
    assert doc["b"] == 2


@pytest.mark.asyncio
async def test_update_missing_returns_404(client: AsyncClient) -> None:
    r = await client.put("/doc/999", json={"request_id": _rid(), "document": {"doc_id": 999}})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_update_doc_id_mismatch_returns_400(client: AsyncClient) -> None:
    r = await client.put("/doc/1", json={"request_id": _rid(), "document": {"doc_id": 2}})
    assert r.status_code == 400


# ── Delete ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_delete_existing(client: AsyncClient) -> None:
    await client.post("/doc", json={"request_id": _rid(), "document": {"doc_id": 3}})
    body, headers = _jbody({"request_id": _rid()})
    r = await client.request("DELETE", "/doc/3", content=body, headers=headers)
    assert r.status_code == 200
    assert r.json()["outcome"] == "COMMITTED"
    r2 = await client.get("/doc/3")
    assert r2.status_code == 404


@pytest.mark.asyncio
async def test_delete_missing_returns_404(client: AsyncClient) -> None:
    body, headers = _jbody({"request_id": _rid()})
    r = await client.request("DELETE", "/doc/8888", content=body, headers=headers)
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_retry_same_request_id(client: AsyncClient) -> None:
    rid = _rid()
    await client.post("/doc", json={"request_id": _rid(), "document": {"doc_id": 50}})
    body, headers = _jbody({"request_id": rid})
    r1 = await client.request("DELETE", "/doc/50", content=body, headers=headers)
    r2 = await client.request("DELETE", "/doc/50", content=body, headers=headers)
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json()["seq"] == r2.json()["seq"]  # same seq = dedup worked


# ── Out of range ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_insert_out_of_range(tmp_path: Path) -> None:
    """Node configured for [0, 100) only; doc_id=200 must be rejected."""
    import os

    os.environ["SHARD_RANGE_START"] = "0"
    os.environ["SHARD_RANGE_END"] = "100"
    os.environ["SHARD_OWNERSHIP_EPOCH"] = "1"
    settings = _make_settings(str(tmp_path / "narrow"))
    app = build_app(
        settings, node_id="n-narrow", shard_id="shard-narrow", lease_expires_at=float("inf")
    )
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.post("/doc", json={"request_id": _rid(), "document": {"doc_id": 200}})
            assert r.status_code == 400
            assert r.json()["error_code"] == "OUT_OF_RANGE"
    os.environ.pop("SHARD_RANGE_START", None)
    os.environ.pop("SHARD_RANGE_END", None)
    os.environ.pop("SHARD_OWNERSHIP_EPOCH", None)


# ── Boundary keys ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_boundary_start_accepted(client: AsyncClient) -> None:
    r = await client.post("/doc", json={"request_id": _rid(), "document": {"doc_id": 0}})
    assert r.status_code == 201


@pytest.mark.asyncio
async def test_boundary_max_doc_id_accepted(client: AsyncClient) -> None:
    max_id = (2**63) - 1
    r = await client.post("/doc", json={"request_id": _rid(), "document": {"doc_id": max_id}})
    assert r.status_code == 201


@pytest.mark.asyncio
async def test_bool_doc_id_rejected(client: AsyncClient) -> None:
    r = await client.post("/doc", json={"request_id": _rid(), "document": {"doc_id": True}})
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_negative_doc_id_rejected(client: AsyncClient) -> None:
    r = await client.post("/doc", json={"request_id": _rid(), "document": {"doc_id": -1}})
    assert r.status_code == 422


# ── Internal auth ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_internal_status_requires_auth(client: AsyncClient) -> None:
    r = await client.get("/internal/status")
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_internal_status_with_valid_auth(client: AsyncClient) -> None:
    body_bytes = b""
    headers = _sig(body_bytes)
    r = await client.get("/internal/status", headers=headers)
    assert r.status_code == 200
    assert "node_id" in r.json()


# ── Oversized body ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_oversized_body_rejected(client: AsyncClient) -> None:
    large = "x" * (2 * 1024 * 1024 + 1)
    r = await client.post(
        "/doc",
        content=large.encode(),
        headers={
            "content-length": str(len(large) + 1),
            "content-type": "application/json",
        },
    )
    assert r.status_code == 413


# ── WAL corruption — node must refuse ────────────────────────────────────────


@pytest.mark.asyncio
async def test_corrupted_wal_node_returns_503(tmp_path: Path) -> None:
    shard_id = "shard-corrupt"
    data_dir = tmp_path / shard_id
    data_dir.mkdir(parents=True, exist_ok=True)

    from docstore.storage.wal import WAL

    wal = WAL(data_dir / "shard.wal")
    wal.open()
    wal.append(seq=1, term=1, op="insert", doc_id=1, request_id="r1", payload={"doc_id": 1})
    wal.close()

    wal_path = data_dir / "shard.wal"
    lines = wal_path.read_text().splitlines()
    record = json.loads(lines[0])
    record["crc32"] = 0xDEADBEEF
    wal_path.write_text(json.dumps(record) + "\n")

    settings = _make_settings(str(tmp_path))
    app = build_app(
        settings, node_id="corrupt-node", shard_id=shard_id, lease_expires_at=float("inf")
    )
    # LifespanManager starts lifespan; recovery fails but app still starts
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.get("/health")
            assert r.status_code == 503


# ── Restart persistence ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_restart_data_persists(tmp_path: Path) -> None:
    """Insert docs, shutdown (snapshot saved), restart, data must still be there."""
    data_dir = str(tmp_path)

    # First boot
    settings1 = _make_settings(data_dir)
    app1 = build_app(settings1, node_id="n", shard_id="shard0", lease_expires_at=float("inf"))
    async with LifespanManager(app1):
        transport1 = ASGITransport(app=app1)
        async with AsyncClient(transport=transport1, base_url="http://test") as c:
            r1 = await c.post(
                "/doc",
                json={"request_id": _rid(), "document": {"doc_id": 100, "val": "hello"}},
            )
            r2 = await c.post(
                "/doc",
                json={"request_id": _rid(), "document": {"doc_id": 200, "val": "world"}},
            )
            assert r1.status_code == 201
            assert r2.status_code == 201
    # LifespanManager exits → graceful shutdown → snapshot saved

    # Second boot
    settings2 = _make_settings(data_dir)
    app2 = build_app(settings2, node_id="n", shard_id="shard0", lease_expires_at=float("inf"))
    async with LifespanManager(app2):
        transport2 = ASGITransport(app=app2)
        async with AsyncClient(transport=transport2, base_url="http://test") as c:
            g100 = await c.get("/doc/100")
            g200 = await c.get("/doc/200")
            assert g100.status_code == 200
            assert g100.json()["document"]["val"] == "hello"
            assert g200.status_code == 200
            assert g200.json()["document"]["val"] == "world"
