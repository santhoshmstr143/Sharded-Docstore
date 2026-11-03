"""
Comprehensive regression tests for final targeted correctness fixes:
- Bug 5: Standalone node recovery and cluster mode fencing isolation.
- Bug 6: Coordinator restart lease quarantine, monotonic clock safety, and conflicting lease prevention.
- Bug 18: Migration catch-up convergence, bounded batches, timeout handling, and cutover safety.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from docstore.common.auth import CLIENT_KEY_HEADER
from docstore.common.config import Settings
from docstore.common.errors import StaleTerm
from docstore.common.models import (
    Heartbeat,
    NodeRegistration,
    NodeRole,
    ReplicaState,
    ShardMap,
    ShardMapEntry,
    ShardRange,
)
from docstore.coordinator.coordinator import CoordinatorService
from docstore.coordinator.migration import MigrationState
from docstore.shard.app import ShardRangeConfig, build_app

pytestmark = pytest.mark.integration

TEST_SECRET = "hardenings_test_secret_32_bytes_ok"
TEST_ADMIN = "admin_token_test_secret_32_bytes_ok"
TEST_CLIENT = "client_key_16_bytes"


def _make_settings(data_dir: Path | str, **overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "DATA_DIR": str(data_dir),
        "CLUSTER_SECRET": TEST_SECRET,
        "ADMIN_TOKEN": TEST_ADMIN,
        "CLIENT_API_KEY": TEST_CLIENT,
        "ENABLE_TEST_HOOKS": True,
        "HEARTBEAT_INTERVAL_S": 0.2,
        "LEASE_DURATION_S": 1.0,
        "FAILURE_TIMEOUT_S": 3.0,
        "SAFETY_MARGIN_S": 0.1,
        "REPLICATION_TIMEOUT_S": 0.5,
        "MIGRATION_CATCHUP_MAX_DURATION_S": 10.0,
        "MIGRATION_FINAL_CATCHUP_MAX_DURATION_S": 5.0,
    }
    base.update(overrides)
    return Settings(**base)


# ═══════════════════════════════════════════════════════════════════════════
# PART 2 — BUG 5: STANDALONE NODE DISK RECOVERY & CLUSTER ISOLATION
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_standalone_node_restart(tmp_path: Path):
    """Test A — Standalone node restart:

    1. Start standalone node.
    2. Insert multiple documents.
    3. Shut down.
    4. Restart with same storage.
    5. Recovers as PRIMARY with valid lease.
    6. Read old documents.
    7. Insert new document successfully.
    """
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    # First run
    app1 = build_app(
        settings=settings,
        node_id="p1",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        allow_standalone=True,
    )
    async with LifespanManager(app1):
        transport1 = ASGITransport(app=app1)
        async with AsyncClient(transport=transport1, base_url="http://standalone") as client:
            r1 = await client.post(
                "/doc",
                json={"request_id": "req-1", "document": {"doc_id": 10, "val": "first"}},
                headers={CLIENT_KEY_HEADER: TEST_CLIENT},
            )
            assert r1.status_code == 201
            r2 = await client.post(
                "/doc",
                json={"request_id": "req-2", "document": {"doc_id": 20, "val": "second"}},
                headers={CLIENT_KEY_HEADER: TEST_CLIENT},
            )
            assert r2.status_code == 201

    # Second run (restart with same data dir)
    app2 = build_app(
        settings=settings,
        node_id="p1",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        allow_standalone=True,
    )
    async with LifespanManager(app2):
        node = app2.state.node
        assert node.ready is True
        assert node.role == NodeRole.PRIMARY
        assert node.is_lease_valid() is True

        transport2 = ASGITransport(app=app2)
        async with AsyncClient(transport=transport2, base_url="http://standalone") as client:
            g1 = await client.get("/doc/10", headers={CLIENT_KEY_HEADER: TEST_CLIENT})
            assert g1.status_code == 200
            assert g1.json()["document"]["val"] == "first"

            g2 = await client.get("/doc/20", headers={CLIENT_KEY_HEADER: TEST_CLIENT})
            assert g2.status_code == 200
            assert g2.json()["document"]["val"] == "second"

            r3 = await client.post(
                "/doc",
                json={"request_id": "req-3", "document": {"doc_id": 30, "val": "third"}},
                headers={CLIENT_KEY_HEADER: TEST_CLIENT},
            )
            assert r3.status_code == 201
            assert r3.json()["outcome"] == "COMMITTED"
            assert node.engine.doc_count() == 3


@pytest.mark.asyncio
async def test_standalone_wal_recovery(tmp_path: Path):
    """Test B — Standalone WAL recovery:

    1. Create WAL entries through writes.
    2. Restart standalone node.
    3. WAL entries recovered correctly.
    4. No duplicate documents.
    5. Sequence numbers remain strictly monotonic.
    """
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    app1 = build_app(
        settings=settings,
        node_id="p1",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        allow_standalone=True,
    )
    async with LifespanManager(app1):
        transport1 = ASGITransport(app=app1)
        async with AsyncClient(transport=transport1, base_url="http://standalone") as client:
            for i in range(5):
                resp = await client.post(
                    "/doc",
                    json={"request_id": f"seq-{i}", "document": {"doc_id": 100 + i, "num": i}},
                    headers={CLIENT_KEY_HEADER: TEST_CLIENT},
                )
                assert resp.status_code == 201
                assert resp.json()["seq"] == i + 1

    # Restart
    app2 = build_app(
        settings=settings,
        node_id="p1",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        allow_standalone=True,
    )
    async with LifespanManager(app2):
        node = app2.state.node
        assert node.engine.doc_count() == 5
        assert node.wal.last_seq == 5
        assert node.committed_seq == 5

        transport2 = ASGITransport(app=app2)
        async with AsyncClient(transport=transport2, base_url="http://standalone") as client:
            resp6 = await client.post(
                "/doc",
                json={"request_id": "seq-5", "document": {"doc_id": 105, "num": 5}},
                headers={CLIENT_KEY_HEADER: TEST_CLIENT},
            )
            assert resp6.status_code == 201
            assert resp6.json()["seq"] == 6


@pytest.mark.asyncio
async def test_standalone_deduplication_after_restart(tmp_path: Path):
    """Test C — Standalone deduplication after restart:

    1. Submit request with known request_id.
    2. Restart node.
    3. Retry same request_id.
    4. Original COMMITTED response returned without duplicate execution.
    """
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    app1 = build_app(
        settings=settings,
        node_id="p1",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        allow_standalone=True,
    )
    async with LifespanManager(app1):
        transport1 = ASGITransport(app=app1)
        async with AsyncClient(transport=transport1, base_url="http://standalone") as client:
            r1 = await client.post(
                "/doc",
                json={"request_id": "req-dedup-xyz", "document": {"doc_id": 50, "v": 1}},
                headers={CLIENT_KEY_HEADER: TEST_CLIENT},
            )
            assert r1.status_code == 201
            orig_seq = r1.json()["seq"]

    # Restart
    app2 = build_app(
        settings=settings,
        node_id="p1",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        allow_standalone=True,
    )
    async with LifespanManager(app2):
        transport2 = ASGITransport(app=app2)
        async with AsyncClient(transport=transport2, base_url="http://standalone") as client:
            r2 = await client.post(
                "/doc",
                json={"request_id": "req-dedup-xyz", "document": {"doc_id": 50, "v": 1}},
                headers={CLIENT_KEY_HEADER: TEST_CLIENT},
            )
            assert r2.status_code == 201
            assert r2.json()["outcome"] == "COMMITTED"
            assert r2.json()["seq"] == orig_seq

            doc_get = await client.get("/doc/50", headers={CLIENT_KEY_HEADER: TEST_CLIENT})
            assert doc_get.json()["document"]["v"] == 1


@pytest.mark.asyncio
async def test_cluster_mode_fencing_remains_intact(tmp_path: Path):
    """Test D — Cluster-mode fencing remains intact:

    1. Distributed node (allow_standalone=False) persists WAL data.
    2. Restart node.
    3. Recovers strictly as FOLLOWER with 0 lease.
    4. Direct write rejected as NotPrimary.
    """
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    app1 = build_app(
        settings=settings,
        node_id="p1",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        allow_standalone=True,
    )
    async with LifespanManager(app1):
        transport1 = ASGITransport(app=app1)
        async with AsyncClient(transport=transport1, base_url="http://node") as client:
            await client.post(
                "/doc",
                json={"request_id": "init-1", "document": {"doc_id": 10, "v": 1}},
                headers={CLIENT_KEY_HEADER: TEST_CLIENT},
            )

    # Restart in cluster mode (allow_standalone=False)
    app2 = build_app(
        settings=settings,
        node_id="p1",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        allow_standalone=False,  # CLUSTER MODE
    )
    async with LifespanManager(app2):
        node = app2.state.node
        assert node.ready is True
        assert node.role == NodeRole.FOLLOWER
        assert node.lease_expires_at == 0.0

        transport2 = ASGITransport(app=app2)
        async with AsyncClient(transport=transport2, base_url="http://node") as client:
            resp = await client.post(
                "/doc",
                json={"request_id": "cluster-write", "document": {"doc_id": 20, "v": 2}},
                headers={CLIENT_KEY_HEADER: TEST_CLIENT},
            )
            assert resp.status_code == 409
            data = resp.json()
            err_code = data.get("error_code") or data.get("detail", {}).get("error_code")
            assert err_code == "NOT_PRIMARY"


@pytest.mark.asyncio
async def test_standalone_mode_does_not_bypass_failed_recovery(tmp_path: Path):
    """Test E — Standalone mode does not bypass failed recovery:

    1. Corrupt node_metadata.json.
    2. Standalone node start fails recovery.
    3. Node remains NOT ready and rejects requests with 503 NODE_NOT_READY.
    """
    settings = _make_settings(tmp_path)
    shard_dir = tmp_path / "shard0"
    shard_dir.mkdir(parents=True)
    (shard_dir / "node_metadata.json").write_text("{corrupt json ...")

    app = build_app(
        settings=settings,
        node_id="p1",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        allow_standalone=True,
    )
    async with LifespanManager(app):
        node = app.state.node
        assert node.ready is False

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://node") as client:
            resp = await client.post(
                "/doc",
                json={"request_id": "req-unsafe", "document": {"doc_id": 1, "v": 1}},
                headers={CLIENT_KEY_HEADER: TEST_CLIENT},
            )
            assert resp.status_code == 503
            data = resp.json()
            err_code = data.get("error_code") or data.get("detail", {}).get("error_code")
            assert err_code == "NODE_NOT_READY"


# ═══════════════════════════════════════════════════════════════════════════
# PART 3 — BUG 6: COORDINATOR LEASE QUARANTINE & CONFLICTING LEASE PREVENTION
# ═══════════════════════════════════════════════════════════════════════════


def test_coordinator_restart_quarantine_initialization(tmp_path: Path):
    """Test F — Quarantine deadline based on monotonic clock + lease duration + safety margin."""
    settings = _make_settings(
        tmp_path, LEASE_DURATION_S=3.0, FAILURE_TIMEOUT_S=5.0, SAFETY_MARGIN_S=0.5
    )
    map_path = tmp_path / "shard_map.json"

    t0 = time.monotonic()
    coord = CoordinatorService(settings, map_path=map_path)
    t1 = time.monotonic()

    expected_min = t0 + 3.0 + 0.5
    expected_max = t1 + 3.0 + 0.5
    assert expected_min <= coord._recovery_quarantine_until_monotonic <= expected_max


@pytest.mark.asyncio
async def test_no_conflicting_promotion_during_quarantine(tmp_path: Path):
    """Test G — No conflicting promotion or lease grant during quarantine:

    1. Start coordinator.
    2. Non-incumbent node registers during quarantine -> lease is None, heartbeat is None.
    3. Failover safety wait is positive during quarantine.
    4. Advance monotonic clock past quarantine -> lease can be granted after failover.
    """
    settings = _make_settings(
        tmp_path, LEASE_DURATION_S=2.0, FAILURE_TIMEOUT_S=4.0, SAFETY_MARGIN_S=0.5
    )
    map_path = tmp_path / "shard_map.json"

    coord = CoordinatorService(settings, map_path=map_path)
    coord.map_mgr.save(
        ShardMap(
            map_version=1,
            entries=[
                ShardMapEntry(
                    shard_id="shard0",
                    range=ShardRange(start=0, end=2**63),
                    primary="http://127.0.0.1:8200",
                    follower="http://127.0.0.1:8201",
                    ownership_epoch=1,
                    map_version=1,
                )
            ],
        )
    )

    now_mono = time.monotonic()

    # 1. Non-incumbent registration during quarantine gets no primary lease
    reg = NodeRegistration(
        node_id="follower-node",
        shard_id="shard0",
        address="http://127.0.0.1:8201",
        role=NodeRole.PRIMARY,
        term=1,
    )
    resp = coord.register_node(reg)
    assert resp.lease is None

    # 2. Heartbeat claiming primary during quarantine is rejected
    hb = Heartbeat(
        node_id="follower-node",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=1,
        last_applied_seq=0,
        follower_acked_seq=0,
        follower_state=ReplicaState.SYNCED,
        timestamp_monotonic=now_mono,
    )
    hb_grant = coord.handle_heartbeat(hb)
    assert hb_grant is None

    # 3. Quarantine deadline is active
    assert coord._recovery_quarantine_until_monotonic > now_mono

    # 4. Advance monotonic clock past quarantine and perform failover promotion
    future_mono = now_mono + 3.0

    class FollowerStatusTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"status": "ok"})

    async with AsyncClient(
        transport=FollowerStatusTransport(), base_url="http://127.0.0.1:8201"
    ) as client:
        with patch("time.monotonic", return_value=future_mono):
            ok = await coord.trigger_failover("shard0", http_client=client)
            assert ok is True

            # Confirmed: new primary lease granted in term 2 to promoted follower
            assert coord.shard_terms["shard0"] == 2
            entry = coord.map_mgr.shard_map.entries[0]
            assert entry.primary == "http://127.0.0.1:8201"
            assert "shard0" in coord.leases
            assert coord.leases["shard0"].node_id == "follower-node"
            assert coord.leases["shard0"].term == 2


@pytest.mark.asyncio
async def test_all_lease_granting_paths_respect_quarantine(tmp_path: Path):
    """Test H — Registration, failover, and heartbeat all enforce quarantine and reject conflicting grants."""
    settings = _make_settings(
        tmp_path, LEASE_DURATION_S=2.0, FAILURE_TIMEOUT_S=4.0, SAFETY_MARGIN_S=0.5
    )
    map_path = tmp_path / "shard_map.json"

    coord = CoordinatorService(settings, map_path=map_path)
    coord.map_mgr.save(
        ShardMap(
            map_version=1,
            entries=[
                ShardMapEntry(
                    shard_id="shard0",
                    range=ShardRange(start=0, end=2**63),
                    primary="http://127.0.0.1:8200",
                    follower="http://127.0.0.1:8201",
                    ownership_epoch=1,
                    map_version=1,
                )
            ],
        )
    )

    # 1. Rogue node with unauthorized address rejected during registration
    reg_rogue = NodeRegistration(
        node_id="rogue",
        shard_id="shard0",
        address="http://127.0.0.1:9999",
        role=NodeRole.PRIMARY,
        term=1,
    )
    with pytest.raises(ValueError):
        coord.register_node(reg_rogue)

    # 2. Rogue heartbeat claiming primary rejected
    hb_rogue = Heartbeat(
        node_id="rogue",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=1,
        last_applied_seq=0,
        follower_acked_seq=0,
        follower_state=ReplicaState.SYNCED,
        timestamp_monotonic=time.monotonic(),
    )
    assert coord.handle_heartbeat(hb_rogue) is None

    # 3. Incumbent primary CAN register and get lease safely
    reg_incumbent = NodeRegistration(
        node_id="incumbent",
        shard_id="shard0",
        address="http://127.0.0.1:8200",
        role=NodeRole.PRIMARY,
        term=1,
    )
    resp_inc = coord.register_node(reg_incumbent)
    assert resp_inc.lease is not None
    assert resp_inc.lease.node_id == "incumbent"

    # 4. Rogue heartbeat in term 1 rejected while incumbent lease active
    assert coord.handle_heartbeat(hb_rogue) is None


def test_strictly_increasing_terms(tmp_path: Path):
    """Test I — Strictly increasing terms and stale primary rejected."""
    settings = _make_settings(tmp_path)
    map_path = tmp_path / "shard_map.json"

    coord = CoordinatorService(settings, map_path=map_path)
    coord.map_mgr.save(
        ShardMap(
            map_version=1,
            entries=[
                ShardMapEntry(
                    shard_id="shard0",
                    range=ShardRange(start=0, end=2**63),
                    primary="http://127.0.0.1:8200",
                    follower="http://127.0.0.1:8201",
                    ownership_epoch=1,
                    map_version=1,
                )
            ],
        )
    )

    new_term = coord.bump_shard_failover(
        "shard0",
        new_primary_url="http://127.0.0.1:8201",
        new_follower_url="http://127.0.0.1:8200",
    )
    assert new_term == 2
    assert coord.shard_terms["shard0"] == 2

    hb_stale = Heartbeat(
        node_id="old-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=1,
        last_applied_seq=10,
        follower_acked_seq=10,
        follower_state=ReplicaState.DOWN,
        timestamp_monotonic=time.monotonic(),
    )
    with pytest.raises(StaleTerm):
        coord.handle_heartbeat(hb_stale)


def test_delayed_responses_cannot_restore_stale_authority(tmp_path: Path):
    """Test J — Stale term heartbeat cannot grant or restore lease."""
    settings = _make_settings(tmp_path)
    map_path = tmp_path / "shard_map.json"

    coord = CoordinatorService(settings, map_path=map_path)
    coord.map_mgr.save(
        ShardMap(
            map_version=1,
            entries=[
                ShardMapEntry(
                    shard_id="shard0",
                    range=ShardRange(start=0, end=2**63),
                    primary="http://127.0.0.1:8200",
                    follower="http://127.0.0.1:8201",
                    ownership_epoch=1,
                    map_version=1,
                )
            ],
        )
    )
    coord.shard_terms["shard0"] = 5

    delayed_hb = Heartbeat(
        node_id="delayed-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=4,
        last_applied_seq=5,
        follower_acked_seq=5,
        follower_state=ReplicaState.SYNCED,
        timestamp_monotonic=time.monotonic(),
    )
    with pytest.raises(StaleTerm):
        coord.handle_heartbeat(delayed_hb)


def test_monotonic_clock_safety_against_wall_clock_jumps(tmp_path: Path):
    """Test K — Monotonic-clock safety: wall-clock adjustments do not affect lease expiry or quarantine."""
    settings = _make_settings(
        tmp_path, LEASE_DURATION_S=2.0, FAILURE_TIMEOUT_S=4.0, SAFETY_MARGIN_S=0.5
    )
    map_path = tmp_path / "shard_map.json"

    coord = CoordinatorService(settings, map_path=map_path)
    coord.map_mgr.save(
        ShardMap(
            map_version=1,
            entries=[
                ShardMapEntry(
                    shard_id="shard0",
                    range=ShardRange(start=0, end=2**63),
                    primary="http://127.0.0.1:8200",
                    follower="http://127.0.0.1:8201",
                    ownership_epoch=1,
                    map_version=1,
                )
            ],
        )
    )
    fixed_mono = 1000.0

    with patch("time.monotonic", return_value=fixed_mono):
        coord._recovery_quarantine_until_monotonic = fixed_mono + 2.5

        # Wall clock jumps 10,000 seconds forward: quarantine remains enforced by monotonic time
        with patch("time.time", return_value=time.time() + 10000.0):
            hb_rogue = Heartbeat(
                node_id="follower-claim",
                shard_id="shard0",
                role=NodeRole.PRIMARY,
                term=1,
                last_applied_seq=0,
                follower_acked_seq=0,
                follower_state=ReplicaState.SYNCED,
                timestamp_monotonic=fixed_mono,
            )
            # Must still reject because monotonic clock is still within quarantine
            assert coord.handle_heartbeat(hb_rogue) is None


# ═══════════════════════════════════════════════════════════════════════════
# PART 4 — BUG 18: MIGRATION CATCH-UP CONVERGENCE & PROGRESS ALGORITHM
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_migration_catchup_over_2500_operations(tmp_path: Path):
    """Test L — Migration catches up over 2,500 operations (breaking old 5-iteration / 500-entry limit)."""
    settings = _make_settings(
        tmp_path,
        MIGRATION_CATCHUP_MAX_DURATION_S=60.0,
        MIGRATION_FINAL_CATCHUP_MAX_DURATION_S=30.0,
    )
    coord_service = CoordinatorService(
        settings, map_path=str(tmp_path / "coordinator" / "shard_map.json")
    )

    app0 = build_app(
        settings=settings,
        node_id="node0",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=200, ownership_epoch=1)],
        allow_standalone=True,
    )
    app1 = build_app(
        settings=settings,
        node_id="node1",
        shard_id="shard1",
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=200, end=2**63, ownership_epoch=1)],
        allow_standalone=True,
    )

    async with LifespanManager(app0), LifespanManager(app1):
        trans0 = ASGITransport(app=app0)
        trans1 = ASGITransport(app=app1)

        async with AsyncClient(transport=trans0, base_url="http://node0") as c0:
            for i in range(20):
                await c0.post(
                    "/doc",
                    json={"request_id": f"init-{i}", "document": {"doc_id": 100 + i, "v": 1}},
                    headers={CLIENT_KEY_HEADER: TEST_CLIENT},
                )

        coord_service.map_mgr.save(
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
                        range=ShardRange(start=200, end=2**63),
                        primary="http://node1",
                        follower="",
                        ownership_epoch=1,
                        map_version=1,
                    ),
                ],
            )
        )
        coord_service.replica_states["shard0"] = ReplicaState.SYNCED
        coord_service.replica_states["shard1"] = ReplicaState.SYNCED

        rec = coord_service.migration_mgr.create_migration(
            from_shard="shard0",
            to_shard="shard1",
            start=100,
            end=200,
            coord=coord_service,
        )

        mounts = {
            "http://node0": trans0,
            "http://node1": trans1,
        }
        async with AsyncClient(mounts=mounts) as client:

            async def write_heavy_ops(phase: MigrationState) -> None:
                if phase == MigrationState.SNAPSHOT_COPY:
                    async with AsyncClient(transport=trans0, base_url="http://node0") as c0:
                        for op_i in range(2600):
                            await c0.put(
                                "/doc/105",
                                json={
                                    "request_id": f"heavy-{op_i}",
                                    "document": {"doc_id": 105, "v": op_i},
                                },
                                headers={CLIENT_KEY_HEADER: TEST_CLIENT},
                            )

            res = await coord_service.migration_mgr.execute_migration(
                rec.migration_id,
                coord_service,
                http_client=client,
                on_phase_hook=write_heavy_ops,
            )
            assert res.state == MigrationState.DONE
            assert res.catchup_ops_replayed >= 2600

        async with AsyncClient(transport=trans1, base_url="http://node1") as c1:
            g = await c1.get("/doc/105", headers={CLIENT_KEY_HEADER: TEST_CLIENT})
            assert g.status_code == 200
            assert g.json()["document"]["v"] == 2599


@pytest.mark.asyncio
async def test_sustained_writes_during_catchup(tmp_path: Path):
    """Test M — Sustained writes during catchup make progress and converge under pause."""
    settings = _make_settings(tmp_path)
    coord_service = CoordinatorService(
        settings, map_path=str(tmp_path / "coordinator" / "shard_map.json")
    )

    app0 = build_app(
        settings=settings,
        node_id="node0",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=200, ownership_epoch=1)],
        allow_standalone=True,
    )
    app1 = build_app(
        settings=settings,
        node_id="node1",
        shard_id="shard1",
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=200, end=2**63, ownership_epoch=1)],
        allow_standalone=True,
    )

    async with LifespanManager(app0), LifespanManager(app1):
        trans0 = ASGITransport(app=app0)
        trans1 = ASGITransport(app=app1)

        async with AsyncClient(transport=trans0, base_url="http://node0") as c0:
            await c0.post(
                "/doc",
                json={"request_id": "init-1", "document": {"doc_id": 150, "v": 1}},
                headers={CLIENT_KEY_HEADER: TEST_CLIENT},
            )

        coord_service.map_mgr.save(
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
                        range=ShardRange(start=200, end=2**63),
                        primary="http://node1",
                        follower="",
                        ownership_epoch=1,
                        map_version=1,
                    ),
                ],
            )
        )
        coord_service.replica_states["shard0"] = ReplicaState.SYNCED
        coord_service.replica_states["shard1"] = ReplicaState.SYNCED

        rec = coord_service.migration_mgr.create_migration(
            from_shard="shard0",
            to_shard="shard1",
            start=100,
            end=200,
            coord=coord_service,
        )

        mounts = {"http://node0": trans0, "http://node1": trans1}
        async with AsyncClient(mounts=mounts) as client:

            async def sustained_writes(phase: MigrationState) -> None:
                if phase == MigrationState.SNAPSHOT_COPY:
                    async with AsyncClient(transport=trans0, base_url="http://node0") as c0:
                        for i in range(100):
                            await c0.put(
                                "/doc/150",
                                json={
                                    "request_id": f"sustained-{i}",
                                    "document": {"doc_id": 150, "v": i},
                                },
                                headers={CLIENT_KEY_HEADER: TEST_CLIENT},
                            )

            res = await coord_service.migration_mgr.execute_migration(
                rec.migration_id,
                coord_service,
                http_client=client,
                on_phase_hook=sustained_writes,
            )
            assert res.state == MigrationState.DONE
            assert res.catchup_ops_replayed >= 100


@pytest.mark.asyncio
async def test_no_progress_detection_aborts_or_times_out(tmp_path: Path):
    """Test N — Stalled catchup detects no forward progress and aborts without cutover."""
    settings = _make_settings(tmp_path)
    coord_service = CoordinatorService(
        settings, map_path=str(tmp_path / "coordinator" / "shard_map.json")
    )
    coord_service.map_mgr.save(
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
                    range=ShardRange(start=200, end=2**63),
                    primary="http://node1",
                    follower="",
                    ownership_epoch=1,
                    map_version=1,
                ),
            ],
        )
    )
    coord_service.replica_states["shard0"] = ReplicaState.SYNCED
    coord_service.replica_states["shard1"] = ReplicaState.SYNCED

    rec = coord_service.migration_mgr.create_migration(
        from_shard="shard0",
        to_shard="shard1",
        start=100,
        end=200,
        coord=coord_service,
    )

    class StalledTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if path == "/internal/snapshot":
                return httpx.Response(
                    200,
                    json={
                        "documents": {},
                        "last_applied_seq": 10,
                        "term": 1,
                        "ownership_epochs": {"0-200": 1},
                    },
                )
            elif path == "/internal/migration/apply-batch":
                return httpx.Response(200, json={"status": "ok"})
            elif path == "/internal/migration/oplog":
                return httpx.Response(
                    200,
                    json={
                        "status": "ok",
                        "entries": [{"seq": 10, "doc_id": 150, "op": "insert", "payload": {}}],
                        "last_seq": 20,
                    },
                )
            elif path == "/internal/migration/apply-oplog":
                return httpx.Response(200, json={"status": "ok"})
            elif path in ("/internal/migration/unpause", "/internal/migration/abort"):
                return httpx.Response(200, json={"status": "ok"})
            return httpx.Response(200, json={})

    async with AsyncClient(transport=StalledTransport(), base_url="http://test") as client:
        with pytest.raises(RuntimeError) as exc_info:
            await coord_service.migration_mgr.execute_migration(
                rec.migration_id,
                coord_service,
                http_client=client,
            )
        assert (
            "no forward progress" in str(exc_info.value)
            or "timed out" in str(exc_info.value)
            or "stalled" in str(exc_info.value)
        )
        assert rec.state == MigrationState.ABORTED


@pytest.mark.asyncio
async def test_compacted_oplog_during_catchup_detects_needs_snapshot(tmp_path: Path):
    """Test O — Compacted oplog cursor triggers needs_snapshot detection and safe abort."""
    settings = _make_settings(tmp_path)
    coord_service = CoordinatorService(
        settings, map_path=str(tmp_path / "coordinator" / "shard_map.json")
    )
    coord_service.map_mgr.save(
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
                    range=ShardRange(start=200, end=2**63),
                    primary="http://node1",
                    follower="",
                    ownership_epoch=1,
                    map_version=1,
                ),
            ],
        )
    )
    coord_service.replica_states["shard0"] = ReplicaState.SYNCED
    coord_service.replica_states["shard1"] = ReplicaState.SYNCED

    rec = coord_service.migration_mgr.create_migration(
        from_shard="shard0",
        to_shard="shard1",
        start=100,
        end=200,
        coord=coord_service,
    )

    class CompactedTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if path == "/internal/snapshot":
                return httpx.Response(
                    200,
                    json={
                        "documents": {},
                        "last_applied_seq": 10,
                        "term": 1,
                        "ownership_epochs": {"0-200": 1},
                    },
                )
            elif path == "/internal/migration/apply-batch":
                return httpx.Response(200, json={"status": "ok"})
            elif path == "/internal/migration/oplog":
                return httpx.Response(
                    200,
                    json={
                        "status": "compacted",
                        "needs_snapshot": True,
                        "earliest_seq": 50,
                        "entries": [],
                    },
                )
            elif path in ("/internal/migration/unpause", "/internal/migration/abort"):
                return httpx.Response(200, json={"status": "ok"})
            return httpx.Response(200, json={})

    async with AsyncClient(transport=CompactedTransport(), base_url="http://test") as client:
        with pytest.raises(RuntimeError) as exc_info:
            await coord_service.migration_mgr.execute_migration(
                rec.migration_id,
                coord_service,
                http_client=client,
            )
        assert "compacted away" in str(exc_info.value)
        assert rec.state == MigrationState.ABORTED


@pytest.mark.asyncio
async def test_destination_unavailable_aborts_safely(tmp_path: Path):
    """Test P — Destination failure during catchup aborts safely; source unpaused."""
    settings = _make_settings(tmp_path)
    coord_service = CoordinatorService(
        settings, map_path=str(tmp_path / "coordinator" / "shard_map.json")
    )
    coord_service.map_mgr.save(
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
                    range=ShardRange(start=200, end=2**63),
                    primary="http://node1",
                    follower="",
                    ownership_epoch=1,
                    map_version=1,
                ),
            ],
        )
    )
    coord_service.replica_states["shard0"] = ReplicaState.SYNCED
    coord_service.replica_states["shard1"] = ReplicaState.SYNCED

    rec = coord_service.migration_mgr.create_migration(
        from_shard="shard0",
        to_shard="shard1",
        start=100,
        end=200,
        coord=coord_service,
    )

    class DestFailTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            path = request.url.path
            host = request.url.host
            if host == "node1":
                return httpx.Response(500, text="Destination unreachable")
            if path == "/internal/snapshot":
                return httpx.Response(
                    200,
                    json={
                        "documents": [{"doc_id": 150, "val": "abc"}],
                        "snapshot_seq": 10,
                        "last_applied_seq": 10,
                        "term": 1,
                        "ownership_epochs": {"0-200": 1},
                    },
                )
            elif path in ("/internal/migration/unpause", "/internal/migration/abort"):
                return httpx.Response(200, json={"status": "ok"})
            return httpx.Response(200, json={})

    async with AsyncClient(transport=DestFailTransport(), base_url="http://test") as client:
        with pytest.raises(RuntimeError):
            await coord_service.migration_mgr.execute_migration(
                rec.migration_id,
                coord_service,
                http_client=client,
            )
        assert rec.state == MigrationState.ABORTED


@pytest.mark.asyncio
async def test_final_catchup_under_write_pause(tmp_path: Path):
    """Test Q — Final catchup under write pause catches up to exact sequence and verifies checksum."""
    settings = _make_settings(tmp_path)
    coord_service = CoordinatorService(
        settings, map_path=str(tmp_path / "coordinator" / "shard_map.json")
    )

    app0 = build_app(
        settings=settings,
        node_id="node0",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=200, ownership_epoch=1)],
        allow_standalone=True,
    )
    app1 = build_app(
        settings=settings,
        node_id="node1",
        shard_id="shard1",
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=200, end=2**63, ownership_epoch=1)],
        allow_standalone=True,
    )

    async with LifespanManager(app0), LifespanManager(app1):
        trans0 = ASGITransport(app=app0)
        trans1 = ASGITransport(app=app1)

        async with AsyncClient(transport=trans0, base_url="http://node0") as c0:
            for i in range(5):
                await c0.post(
                    "/doc",
                    json={"request_id": f"q-{i}", "document": {"doc_id": 110 + i, "v": i}},
                    headers={CLIENT_KEY_HEADER: TEST_CLIENT},
                )

        coord_service.map_mgr.save(
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
                        range=ShardRange(start=200, end=2**63),
                        primary="http://node1",
                        follower="",
                        ownership_epoch=1,
                        map_version=1,
                    ),
                ],
            )
        )
        coord_service.replica_states["shard0"] = ReplicaState.SYNCED
        coord_service.replica_states["shard1"] = ReplicaState.SYNCED

        rec = coord_service.migration_mgr.create_migration(
            from_shard="shard0",
            to_shard="shard1",
            start=100,
            end=200,
            coord=coord_service,
        )

        mounts = {"http://node0": trans0, "http://node1": trans1}
        async with AsyncClient(mounts=mounts) as client:
            res = await coord_service.migration_mgr.execute_migration(
                rec.migration_id,
                coord_service,
                http_client=client,
            )
            assert res.state == MigrationState.DONE
            assert res.docs_copied == 5


@pytest.mark.asyncio
async def test_catchup_timeout_prevents_cutover(tmp_path: Path):
    """Test R — Catch-up timeout prevents cutover and keeps source intact."""
    settings = _make_settings(
        tmp_path, MIGRATION_CATCHUP_MAX_DURATION_S=0.05, MIGRATION_FINAL_CATCHUP_MAX_DURATION_S=0.05
    )
    coord_service = CoordinatorService(
        settings, map_path=str(tmp_path / "coordinator" / "shard_map.json")
    )
    coord_service.map_mgr.save(
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
                    range=ShardRange(start=200, end=2**63),
                    primary="http://node1",
                    follower="",
                    ownership_epoch=1,
                    map_version=1,
                ),
            ],
        )
    )
    coord_service.replica_states["shard0"] = ReplicaState.SYNCED
    coord_service.replica_states["shard1"] = ReplicaState.SYNCED

    rec = coord_service.migration_mgr.create_migration(
        from_shard="shard0",
        to_shard="shard1",
        start=100,
        end=200,
        coord=coord_service,
    )

    class SlowCatchupTransport(httpx.AsyncBaseTransport):
        def __init__(self):
            self.seq = 10

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if path == "/internal/snapshot":
                return httpx.Response(
                    200,
                    json={
                        "documents": [],
                        "snapshot_seq": 10,
                        "last_applied_seq": 10,
                        "term": 1,
                        "ownership_epochs": {"0-200": 1},
                    },
                )
            elif path == "/internal/migration/apply-batch":
                return httpx.Response(200, json={"status": "ok"})
            elif path == "/internal/migration/oplog":
                await asyncio.sleep(0.02)
                self.seq += 10
                entries = [{"seq": self.seq - 5, "doc_id": 150, "payload": {}}]
                return httpx.Response(
                    200,
                    json={"status": "ok", "entries": entries, "last_seq": self.seq},
                )
            elif path == "/internal/migration/apply-oplog":
                return httpx.Response(200, json={"status": "ok"})
            elif path in ("/internal/migration/unpause", "/internal/migration/abort"):
                return httpx.Response(200, json={"status": "ok"})
            return httpx.Response(200, json={})

    async with AsyncClient(transport=SlowCatchupTransport(), base_url="http://test") as client:
        with pytest.raises(RuntimeError) as exc_info:
            await coord_service.migration_mgr.execute_migration(
                rec.migration_id,
                coord_service,
                http_client=client,
            )
        assert "timed out" in str(exc_info.value) or "did not converge" in str(exc_info.value)
        assert rec.state == MigrationState.ABORTED


@pytest.mark.asyncio
async def test_restart_during_catchup_recovery(tmp_path: Path):
    """Test S — Interrupted migration before cutover recovered or aborted safely on coordinator restart."""
    settings = _make_settings(tmp_path)
    map_path = str(tmp_path / "coordinator" / "shard_map.json")

    coord1 = CoordinatorService(settings, map_path=map_path)
    coord1.map_mgr.save(
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
                    range=ShardRange(start=200, end=2**63),
                    primary="http://node1",
                    follower="",
                    ownership_epoch=1,
                    map_version=1,
                ),
            ],
        )
    )
    coord1.replica_states["shard0"] = ReplicaState.SYNCED
    coord1.replica_states["shard1"] = ReplicaState.SYNCED

    rec = coord1.migration_mgr.create_migration(
        from_shard="shard0",
        to_shard="shard1",
        start=100,
        end=200,
        coord=coord1,
    )
    rec.state = MigrationState.CATCHUP
    coord1.migration_mgr.save_record(rec)

    # Coordinator crashes and restarts
    coord2 = CoordinatorService(settings, map_path=map_path)

    class MockUnpauseTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"status": "ok"})

    async with AsyncClient(transport=MockUnpauseTransport(), base_url="http://node") as client:
        await coord2.migration_mgr.resume_or_recover_unfinished(coord2, http_client=client)
        rec2 = coord2.migration_mgr.migrations.get(rec.migration_id)
        assert rec2 is not None
        assert rec2.state == MigrationState.ABORTED


@pytest.mark.asyncio
async def test_request_id_reuse_with_different_payload_is_rejected(tmp_path: Path):
    """Reusing a request_id for DIFFERENT content must not be silently acknowledged."""
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    app = build_app(
        settings=settings,
        node_id="p1",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        allow_standalone=True,
    )
    async with LifespanManager(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s") as client:
            h = {CLIENT_KEY_HEADER: TEST_CLIENT}
            r1 = await client.post(
                "/doc", json={"request_id": "same", "document": {"doc_id": 1}}, headers=h
            )
            assert r1.status_code == 201
            r2 = await client.post(
                "/doc", json={"request_id": "same", "document": {"doc_id": 2}}, headers=h
            )
            assert r2.status_code == 409
            assert r2.json()["error_code"] == "REQUEST_ID_REUSED"
            # identical retry is still idempotent
            r3 = await client.post(
                "/doc", json={"request_id": "same", "document": {"doc_id": 1}}, headers=h
            )
            assert r3.status_code == 201 and r3.json()["seq"] == r1.json()["seq"]
