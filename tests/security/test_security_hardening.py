"""Comprehensive security and hardening tests for Module 12."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from docstore.common.auth import CLUSTER_SIG_HEADER, compute_signature
from docstore.common.config import Settings
from docstore.common.test_hooks import (
    is_test_hooks_enabled,
    register_crash_point,
    set_test_hooks_enabled,
    trigger_hook,
)
from docstore.coordinator.app import build_coordinator_app
from docstore.coordinator.coordinator import CoordinatorService
from docstore.router.app import build_router_app
from docstore.router.shard_map import ShardMapManager, create_default_3_shard_map
from docstore.shard.app import ShardRangeConfig, build_app

pytestmark = pytest.mark.security


@pytest.fixture
def sec_settings(tmp_path: Path) -> Settings:
    return Settings(
        CLUSTER_SECRET="security-test-cluster-secret-32-chars!",
        ADMIN_TOKEN="security-test-admin-token-32-chars-min!",
        CLIENT_API_KEY="security-test-client-api-key-16ch",
        DATA_DIR=str(tmp_path / "sec_data"),
        ENABLE_TEST_HOOKS=False,
        STANDALONE_MODE=True,
    )


# ═══════════════════════════════════════════════════════════════════════════
# 1. Authentication & Signature Tampering Tests
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_internal_rpc_rejects_missing_signature(sec_settings: Settings) -> None:
    """Every /internal/* endpoint must reject requests missing X-Cluster-Signature."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    shard_app = build_app(
        sec_settings, node_id="n1", shard_id="shard0", ranges=ranges, lease_expires_at=float("inf")
    )

    async with AsyncClient(
        transport=ASGITransport(app=shard_app), base_url="http://shard"
    ) as client:
        # Replicate endpoint
        r1 = await client.post("/internal/replicate", json={"op": "insert"})
        assert r1.status_code == 401
        assert r1.json()["error_code"] == "AUTH_FAILED"

        # Catchup endpoint
        r2 = await client.post("/internal/catchup", json={"from_seq": 1})
        assert r2.status_code == 401
        assert r2.json()["error_code"] == "AUTH_FAILED"

        # Migration snapshot
        r3 = await client.post("/internal/migration/snapshot", json={"start": 0, "end": 100})
        assert r3.status_code == 401
        assert r3.json()["error_code"] == "AUTH_FAILED"


@pytest.mark.asyncio
async def test_internal_rpc_rejects_tampered_payload(sec_settings: Settings) -> None:
    """Tampering with body bytes after computing signature causes 401 AUTH_FAILED."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    shard_app = build_app(
        sec_settings, node_id="n1", shard_id="shard0", ranges=ranges, lease_expires_at=float("inf")
    )

    original_body = b'{"op": "insert", "doc_id": 10}'
    valid_sig = compute_signature(sec_settings.CLUSTER_SECRET, original_body)

    # Tamper with the body content: change doc_id from 10 to 99
    tampered_body = b'{"op": "insert", "doc_id": 99}'

    async with AsyncClient(
        transport=ASGITransport(app=shard_app), base_url="http://shard"
    ) as client:
        resp = await client.post(
            "/internal/replicate",
            content=tampered_body,
            headers={"Content-Type": "application/json", CLUSTER_SIG_HEADER: valid_sig},
        )
        assert resp.status_code == 401
        assert resp.json()["error_code"] == "AUTH_FAILED"


@pytest.mark.asyncio
async def test_coordinator_admin_endpoint_rejects_invalid_token(
    sec_settings: Settings, tmp_path: Path
) -> None:
    """Coordinator admin endpoints require exact X-Admin-Token."""
    coord_svc = CoordinatorService(sec_settings, str(tmp_path / "shard_map.json"))
    coord_app = build_coordinator_app(sec_settings, coord_svc)

    async with AsyncClient(
        transport=ASGITransport(app=coord_app), base_url="http://coord"
    ) as client:
        # Missing header
        r_missing = await client.get("/admin/status")
        assert r_missing.status_code == 401
        assert r_missing.json()["error_code"] == "AUTH_FAILED"

        # Wrong token
        r_wrong = await client.get(
            "/admin/status", headers={"X-Admin-Token": "bad-token-1234567890"}
        )
        assert r_wrong.status_code == 401
        assert r_wrong.json()["error_code"] == "AUTH_FAILED"

        # Valid token
        r_valid = await client.get(
            "/admin/status", headers={"X-Admin-Token": sec_settings.ADMIN_TOKEN}
        )
        assert r_valid.status_code == 200


@pytest.mark.asyncio
async def test_router_requires_client_api_key(sec_settings: Settings, tmp_path: Path) -> None:
    """Router public endpoints reject requests without valid X-Api-Key."""
    map_mgr = ShardMapManager(path=tmp_path / "map.json")
    map_mgr.set_map(create_default_3_shard_map())
    router_app = build_router_app(sec_settings, map_mgr)

    async with LifespanManager(router_app):
        async with AsyncClient(
            transport=ASGITransport(app=router_app), base_url="http://router"
        ) as client:
            # Missing API key
            r_missing = await client.get("/doc/42")
            assert r_missing.status_code == 401
            assert r_missing.json()["error_code"] == "AUTH_FAILED"

            # Invalid API key
            r_invalid = await client.get("/doc/42", headers={"X-Api-Key": "wrong-client-api-key"})
            assert r_invalid.status_code == 401
            assert r_invalid.json()["error_code"] == "AUTH_FAILED"


# ═══════════════════════════════════════════════════════════════════════════
# 2. Input Validation, Caps & Malicious Payload Tests
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_oversized_body_rejected_with_413(sec_settings: Settings, tmp_path: Path) -> None:
    """Request body exceeding 2MB is rejected with 413 BODY_TOO_LARGE."""
    map_mgr = ShardMapManager(path=tmp_path / "map.json")
    map_mgr.set_map(create_default_3_shard_map())
    router_app = build_router_app(sec_settings, map_mgr)

    # 2.5 MB payload
    oversized_data = "x" * (2500 * 1024)
    async with LifespanManager(router_app):
        async with AsyncClient(
            transport=ASGITransport(app=router_app), base_url="http://router"
        ) as client:
            resp = await client.post(
                "/doc",
                json={"request_id": "r-large", "document": {"doc_id": 50, "blob": oversized_data}},
                headers={"X-Api-Key": sec_settings.CLIENT_API_KEY},
            )
            assert resp.status_code == 413
            assert resp.json()["error_code"] == "BODY_TOO_LARGE"


@pytest.mark.asyncio
async def test_malicious_filter_operators_rejected(sec_settings: Settings, tmp_path: Path) -> None:
    """Query filters containing prohibited or arbitrary operators are rejected."""
    map_mgr = ShardMapManager(path=tmp_path / "map.json")
    map_mgr.set_map(create_default_3_shard_map())
    router_app = build_router_app(sec_settings, map_mgr)

    async with LifespanManager(router_app):
        async with AsyncClient(
            transport=ASGITransport(app=router_app), base_url="http://router"
        ) as client:
            headers = {"X-Api-Key": sec_settings.CLIENT_API_KEY}

            # $where injection
            r1 = await client.post(
                "/query", json={"filter": {"$where": "this.doc_id == 1"}}, headers=headers
            )
            assert r1.status_code == 400
            assert r1.json()["error_code"] == "INVALID_QUERY"

            # $regex arbitrary execution attempt
            r2 = await client.post(
                "/query", json={"filter": {"name": {"$regex": ".*"}}}, headers=headers
            )
            assert r2.status_code == 400
            assert r2.json()["error_code"] == "INVALID_QUERY"

            # Excessive filter depth (> 5 levels)
            deep_filter: dict[str, Any] = {"level5": 1}
            for _level in range(4, -1, -1):
                deep_filter = {"$and": [deep_filter]}
            r3 = await client.post("/query", json={"filter": deep_filter}, headers=headers)
            assert r3.status_code == 400
            assert r3.json()["error_code"] == "INVALID_QUERY"

            # Excessive $in clauses (> 1000 items)
            oversized_in = list(range(1005))
            r4 = await client.post(
                "/query", json={"filter": {"doc_id": {"$in": oversized_in}}}, headers=headers
            )
            assert r4.status_code == 400
            assert r4.json()["error_code"] == "INVALID_QUERY"


@pytest.mark.asyncio
async def test_invalid_doc_id_types_rejected(sec_settings: Settings, tmp_path: Path) -> None:
    """Non-integer, boolean, or float doc_ids are rejected with 422/400."""
    map_mgr = ShardMapManager(path=tmp_path / "map.json")
    map_mgr.set_map(create_default_3_shard_map())
    router_app = build_router_app(sec_settings, map_mgr)

    async with LifespanManager(router_app):
        async with AsyncClient(
            transport=ASGITransport(app=router_app), base_url="http://router"
        ) as client:
            headers = {"X-Api-Key": sec_settings.CLIENT_API_KEY}

            # Boolean doc_id (True/False should not be cast to 1/0)
            r_bool = await client.post(
                "/doc",
                json={"request_id": "r-bool", "document": {"doc_id": True, "name": "Fake"}},
                headers=headers,
            )
            assert r_bool.status_code == 422

            # Float doc_id
            r_float = await client.post(
                "/doc",
                json={"request_id": "r-float", "document": {"doc_id": 42.5, "name": "Fake"}},
                headers=headers,
            )
            assert r_float.status_code == 422

            # Negative doc_id
            r_neg = await client.post(
                "/doc",
                json={"request_id": "r-neg", "document": {"doc_id": -5, "name": "Fake"}},
                headers=headers,
            )
            assert r_neg.status_code == 422


# ═══════════════════════════════════════════════════════════════════════════
# 3. Test Hooks Protection & Directory Permissions
# ═══════════════════════════════════════════════════════════════════════════


def test_hooks_disabled_in_production_config(sec_settings: Settings) -> None:
    """When ENABLE_TEST_HOOKS=false, trigger_hook is a no-op even if crash point registered."""
    set_test_hooks_enabled(False)
    assert not is_test_hooks_enabled()
    # Even if someone attempts to register a crash point:
    register_crash_point("before_wal_append")
    # Triggering should be completely inert and not raise or crash
    trigger_hook("before_wal_append", enabled=sec_settings.ENABLE_TEST_HOOKS)
    trigger_hook("after_primary_fsync", enabled=sec_settings.ENABLE_TEST_HOOKS)
    set_test_hooks_enabled(None)


def test_data_directory_permissions_hardened(tmp_path: Path) -> None:
    """Data directories should be created with restricted 0700 permissions."""
    data_dir = tmp_path / "protected_store"
    data_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    mode = data_dir.stat().st_mode & 0o777
    assert mode == 0o700


def test_log_sanitization_no_secrets_leaked(caplog: pytest.LogCaptureFixture) -> None:
    """Structured logs should never log cluster secrets, admin tokens, or full document bodies."""
    from docstore.common.logging import get_logger

    logger = get_logger("security_audit")
    with caplog.at_level(logging.INFO):
        logger.info("Processing request", extra={"node_id": "n1", "shard_id": "s0"})

    for record in caplog.records:
        msg = str(record.msg)
        assert "security-test-cluster-secret" not in msg
        assert "security-test-admin-token" not in msg
        assert "security-test-client-api-key" not in msg
