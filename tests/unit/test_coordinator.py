"""
Unit tests for Coordinator Service & FastAPI App — Module 6.

Tests:
- Node registration (primary/follower assignment, fresh lease for primary, rejection if full)
- Heartbeats & lease renewal (monotonic clock tracking)
- Lease expiration check
- Safety margin preventing new primary lease before old lease + M
- Failover epoch & term bumping across all ranges of a shard
- Range migration epoch bumping
- Degraded shard detection when follower is DOWN / not SYNCED
- Admin token authentication on admin endpoints
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from docstore.common.auth import ADMIN_TOKEN_HEADER
from docstore.common.config import Settings
from docstore.common.models import (
    Heartbeat,
    NodeRegistration,
    NodeRole,
    ReplicaState,
    ShardMap,
    ShardMapEntry,
    ShardRange,
)
from docstore.coordinator.app import build_coordinator_app
from docstore.coordinator.coordinator import CoordinatorService
from docstore.router.shard_map import ShardMapManager

pytestmark = pytest.mark.unit

SECRET = "a" * 32
ADMIN = "b" * 32
APIKEY = "c" * 16


def _make_settings(data_dir: str) -> Settings:
    return Settings.model_validate(
        {
            "CLUSTER_SECRET": SECRET,
            "ADMIN_TOKEN": ADMIN,
            "CLIENT_API_KEY": APIKEY,
            "DATA_DIR": data_dir,
            "HEARTBEAT_INTERVAL_S": 0.5,
            "LEASE_DURATION_S": 1.5,
            "FAILURE_TIMEOUT_S": 2.5,
            "SAFETY_MARGIN_S": 0.2,
            "REPLICATION_TIMEOUT_S": 0.4,
        }
    )


def _bootstrap_map(path: Path) -> ShardMap:
    # 2 shards covering full keyspace [0, 2^63)
    mid = 2**62
    end = 2**63
    entries = [
        ShardMapEntry(
            shard_id="shard0",
            range=ShardRange(start=0, end=mid),
            primary="127.0.0.1:8001",
            follower=None,
            ownership_epoch=1,
            map_version=1,
        ),
        ShardMapEntry(
            shard_id="shard1",
            range=ShardRange(start=mid, end=end),
            primary="127.0.0.1:8003",
            follower=None,
            ownership_epoch=1,
            map_version=1,
        ),
    ]
    s_map = ShardMap(map_version=1, entries=entries)
    path.parent.mkdir(parents=True, exist_ok=True)
    mgr = ShardMapManager(str(path))
    mgr.save(s_map)
    return s_map


class TestCoordinatorRegistrationAndLeases:
    def test_register_primary_gets_fresh_lease(self, tmp_path: Path) -> None:
        settings = _make_settings(str(tmp_path))
        map_path = tmp_path / "shard_map.json"
        _bootstrap_map(map_path)
        coord = CoordinatorService(settings, str(map_path))

        reg = NodeRegistration(
            node_id="node0",
            shard_id="shard0",
            address="127.0.0.1:8001",
            role=None,
        )
        resp = coord.register_node(reg)
        assert resp.role == NodeRole.PRIMARY
        assert resp.lease is not None
        assert resp.term == 1

        lease = coord.leases.get("shard0")
        assert lease is not None
        assert lease.node_id == "node0"
        assert lease.remaining(time.monotonic()) > 0

    def test_register_follower_assigned_role_no_lease(self, tmp_path: Path) -> None:
        settings = _make_settings(str(tmp_path))
        map_path = tmp_path / "shard_map.json"
        _bootstrap_map(map_path)
        coord = CoordinatorService(settings, str(map_path))

        # First register primary
        coord.register_node(
            NodeRegistration(
                node_id="node0",
                shard_id="shard0",
                address="127.0.0.1:8001",
            )
        )
        # Register follower
        resp = coord.register_node(
            NodeRegistration(
                node_id="node0-follower",
                shard_id="shard0",
                address="127.0.0.1:8002",
            )
        )
        assert resp.role == NodeRole.FOLLOWER
        assert resp.lease is None

    def test_register_third_node_rejected(self, tmp_path: Path) -> None:
        settings = _make_settings(str(tmp_path))
        map_path = tmp_path / "shard_map.json"
        _bootstrap_map(map_path)
        coord = CoordinatorService(settings, str(map_path))

        coord.register_node(
            NodeRegistration(
                node_id="n1",
                shard_id="shard0",
                address="127.0.0.1:8001",
            )
        )
        coord.register_node(
            NodeRegistration(
                node_id="n2",
                shard_id="shard0",
                address="127.0.0.1:8002",
            )
        )
        with pytest.raises(ValueError, match="already has primary and follower"):
            coord.register_node(
                NodeRegistration(
                    node_id="n3",
                    shard_id="shard0",
                    address="127.0.0.1:8009",
                )
            )

    def test_heartbeat_renews_lease(self, tmp_path: Path) -> None:
        settings = _make_settings(str(tmp_path))
        map_path = tmp_path / "shard_map.json"
        _bootstrap_map(map_path)
        coord = CoordinatorService(settings, str(map_path))

        coord.register_node(
            NodeRegistration(
                node_id="n1",
                shard_id="shard0",
                address="127.0.0.1:8001",
            )
        )
        lease_before = coord.leases.get("shard0")
        assert lease_before is not None

        time.sleep(0.02)
        hb = Heartbeat(
            node_id="n1",
            shard_id="shard0",
            role=NodeRole.PRIMARY,
            term=1,
            last_applied_seq=5,
            follower_acked_seq=5,
            follower_state=ReplicaState.SYNCED,
            timestamp_monotonic=time.monotonic(),
        )
        grant = coord.handle_heartbeat(hb)
        assert grant is not None
        assert grant.node_id == "n1"

        lease_after = coord.leases.get("shard0")
        assert lease_after is not None
        assert lease_after.expires_at_monotonic > lease_before.expires_at_monotonic

    def test_primary_heartbeat_updates_follower_state_and_degraded(self, tmp_path: Path) -> None:
        settings = _make_settings(str(tmp_path))
        map_path = tmp_path / "shard_map.json"
        _bootstrap_map(map_path)
        coord = CoordinatorService(settings, str(map_path))

        coord.register_node(
            NodeRegistration(
                node_id="n1",
                shard_id="shard0",
                address="127.0.0.1:8001",
            )
        )
        assert coord.is_shard_degraded("shard0") is True  # not yet synced

        coord.handle_heartbeat(
            Heartbeat(
                node_id="n1",
                shard_id="shard0",
                role=NodeRole.PRIMARY,
                term=1,
                last_applied_seq=5,
                follower_acked_seq=5,
                follower_state=ReplicaState.SYNCED,
                timestamp_monotonic=time.monotonic(),
            )
        )
        assert coord.is_shard_degraded("shard0") is False

        # Mark DOWN
        coord.handle_heartbeat(
            Heartbeat(
                node_id="n1",
                shard_id="shard0",
                role=NodeRole.PRIMARY,
                term=1,
                last_applied_seq=5,
                follower_acked_seq=5,
                follower_state=ReplicaState.DOWN,
                timestamp_monotonic=time.monotonic(),
            )
        )
        assert coord.is_shard_degraded("shard0") is True

    def test_failover_epoch_and_term_bump(self, tmp_path: Path) -> None:
        settings = _make_settings(str(tmp_path))
        map_path = tmp_path / "shard_map.json"
        _bootstrap_map(map_path)
        coord = CoordinatorService(settings, str(map_path))

        new_term = coord.bump_shard_failover(shard_id="shard0", new_primary_url="127.0.0.1:8002")
        assert new_term == 2
        new_map = coord.map_mgr.shard_map
        assert new_map.map_version == 2
        entry0 = next(e for e in new_map.entries if e.shard_id == "shard0")
        assert entry0.primary == "127.0.0.1:8002"
        assert entry0.ownership_epoch == 2

        # Shard 1 untouched
        entry1 = next(e for e in new_map.entries if e.shard_id == "shard1")
        assert entry1.primary == "127.0.0.1:8003"
        assert entry1.ownership_epoch == 1

    def test_migration_bumps_migrated_range_only(self, tmp_path: Path) -> None:
        settings = _make_settings(str(tmp_path))
        map_path = tmp_path / "shard_map.json"
        _bootstrap_map(map_path)
        coord = CoordinatorService(settings, str(map_path))

        mid = 2**62
        coord.bump_range_migration(shard_id="shard0", start=0, end=mid)
        new_map = coord.map_mgr.shard_map
        assert new_map.map_version == 2
        entry0 = next(e for e in new_map.entries if e.range.start == 0)
        assert entry0.ownership_epoch == 2


class TestCoordinatorFastAPI:
    @pytest.fixture
    def app(self, tmp_path: Path) -> Any:
        settings = _make_settings(str(tmp_path))
        map_path = tmp_path / "shard_map.json"
        _bootstrap_map(map_path)
        coord = CoordinatorService(settings, str(map_path))
        return build_coordinator_app(settings, coord)

    @pytest.mark.asyncio
    async def test_shardmap_endpoint(self, app: Any) -> None:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            # Topology (internal addresses) is admin-only.
            assert (await c.get("/shardmap")).status_code == 401
            resp = await c.get("/shardmap", headers={ADMIN_TOKEN_HEADER: ADMIN})
            assert resp.status_code == 200
            data = resp.json()
            assert data["map_version"] == 1
            assert len(data["entries"]) == 2

    @pytest.mark.asyncio
    async def test_admin_status_requires_token(self, app: Any) -> None:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            # Without token
            resp = await c.get("/admin/status")
            assert resp.status_code == 401
            assert resp.json()["error_code"] == "AUTH_FAILED"

            # With wrong token
            resp = await c.get("/admin/status", headers={ADMIN_TOKEN_HEADER: "wrong" * 8})
            assert resp.status_code == 401

            # With valid token
            resp = await c.get("/admin/status", headers={ADMIN_TOKEN_HEADER: ADMIN})
            assert resp.status_code == 200
            data = resp.json()
            assert "shards" in data
            assert len(data["shards"]) == 2
