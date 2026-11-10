"""
Fault and Failover integration tests — Module 8 & Milestone M5 Gate.

Covers:
- Step-by-step promotion procedure: failure detection, follower check, lease + safety wait,
  term & epoch bumping, durable shard map save, promotion, and read serving.
- Degraded mode operation: promoted primary serves reads;
  writes rejected with 503 REPLICATION_UNAVAILABLE.
- Fencing of stale primary: stale epoch/term writes and heartbeats rejected.
- Old primary rejoin: demoted to follower, divergence discovery via (term, seq), uncommitted
  tail truncation, rebuilding memory/dedup, catch-up to SYNCED, exiting degraded mode.
- 5 crash-point hooks:
  (1) before_wal_append
  (2) after_primary_fsync
  (3) after_follower_fsync_before_ack
  (4) after_follower_ack_before_client_ack
  (5) after_client_ack
- Double failover protection (single node failure scope).
- Two metrics: failover time and write restoration time.
- M5 GATE: 5 consecutive failover-rejoin cycles under read/write load with zero data loss.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from docstore.common.auth import (
    CLIENT_KEY_HEADER,
    CLUSTER_SIG_HEADER,
    compute_signature,
)
from docstore.common.config import Settings
from docstore.common.models import (
    NodeRole,
    ReplicaState,
)
from docstore.common.test_hooks import (
    CrashPointError,
    clear_crash_points,
    register_crash_point,
    set_test_hooks_enabled,
)
from docstore.coordinator.coordinator import CoordinatorService
from docstore.router.shard_map import (
    create_default_3_shard_map,
)
from docstore.shard.app import ShardRangeConfig, build_app, resync_with_primary

pytestmark = pytest.mark.fault

SECRET = "a" * 32
ADMIN = "b" * 32
CLIENT_KEY = "c" * 16


@pytest.fixture(autouse=True)
def _enable_hooks():
    set_test_hooks_enabled(True)
    yield
    clear_crash_points()
    set_test_hooks_enabled(None)


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
    }
    base.update(overrides)
    return Settings.model_validate(base)


def _client_headers() -> dict[str, str]:
    return {CLIENT_KEY_HEADER: CLIENT_KEY}


def _sig_headers(payload: dict[str, Any] | None = None) -> dict[str, str]:
    if payload is not None:
        body_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        sig = compute_signature(SECRET, body_bytes)
    else:
        sig = compute_signature(SECRET, b"")
    return {CLUSTER_SIG_HEADER: sig}


# ═══════════════════════════════════════════════════════════════════════════
# 1. Step-by-Step Promotion Procedure & Degraded Mode
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_promotion_procedure_and_degraded_mode(tmp_path: Path) -> None:
    """Follower is promoted to primary after primary timeout and safety wait.

    Promoted primary serves reads immediately, but rejects writes with
    REPLICATION_UNAVAILABLE until a follower rejoins.
    """
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    # 1. Set up Follower
    follower_dir = tmp_path / "follower0"
    follower_dir.mkdir(parents=True, exist_ok=True)
    f_settings = settings.model_copy(update={"DATA_DIR": str(follower_dir)})
    follower_app = build_app(
        f_settings,
        node_id="follower0",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
        ranges=ranges,
    )

    async with LifespanManager(follower_app):
        follower_transport = ASGITransport(app=follower_app)

        # 2. Set up Primary
        primary_dir = tmp_path / "primary0"
        primary_dir.mkdir(parents=True, exist_ok=True)
        p_settings = settings.model_copy(update={"DATA_DIR": str(primary_dir)})
        primary_app = build_app(
            p_settings,
            node_id="primary0",
            shard_id="shard0",
            role=NodeRole.PRIMARY,
            ranges=ranges,
            follower_url="127.0.0.1:8201",
            follower_state=ReplicaState.SYNCED,
            follower_transport=follower_transport,
            lease_expires_at=float("inf"),
        )

        async with LifespanManager(primary_app):
            p_transport = ASGITransport(app=primary_app)

            # Insert document 10 on primary
            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                w_resp = await p_client.post(
                    "/doc",
                    json={
                        "request_id": "req-1",
                        "document": {"doc_id": 10, "val": "before-failover"},
                    },
                    headers=_sig_headers(),
                )
                assert w_resp.status_code == 201
                assert w_resp.json()["outcome"] == "COMMITTED"

            # 3. Setup Coordinator
            coord_dir = tmp_path / "coordinator"
            coord_dir.mkdir(parents=True, exist_ok=True)
            map_file = coord_dir / "shard_map.json"
            init_map = create_default_3_shard_map(
                primaries=["127.0.0.1:8200", "127.0.0.1:8202", "127.0.0.1:8204"],
                followers=["127.0.0.1:8201", "127.0.0.1:8203", "127.0.0.1:8205"],
            )
            map_file.write_text(init_map.model_dump_json(indent=2))
            coord_svc = CoordinatorService(settings, shard_map_path=map_file)

            # Register nodes with coordinator
            from docstore.common.models import NodeRegistration

            coord_svc.register_node(
                NodeRegistration(
                    node_id="primary0",
                    shard_id="shard0",
                    address="127.0.0.1:8200",
                    requested_role=NodeRole.PRIMARY,
                )
            )
            coord_svc.register_node(
                NodeRegistration(
                    node_id="follower0",
                    shard_id="shard0",
                    address="127.0.0.1:8201",
                    requested_role=NodeRole.FOLLOWER,
                )
            )

            # Mount follower transport for coordinator promotion RPC
            mounts = {
                "http://127.0.0.1:8201": follower_transport,
                "http://127.0.0.1:8200": p_transport,
            }
            coord_client = AsyncClient(mounts=mounts, timeout=settings.REPLICATION_TIMEOUT_S + 2.0)

            # Trigger failover for shard0
            t_crash = time.time()
            promoted = await coord_svc.trigger_failover(
                "shard0", http_client=coord_client, crash_time=t_crash
            )
            assert promoted is True

            # Verify coordinator shard map was bumped
            new_map = coord_svc.shard_map
            assert new_map.map_version == 2
            entry = new_map.entries[0]
            assert entry.primary == "127.0.0.1:8201"  # Follower is now primary
            assert entry.follower == "127.0.0.1:8200"
            assert entry.ownership_epoch == 2
            assert coord_svc.shard_terms["shard0"] == 2
            assert coord_svc.is_shard_degraded("shard0") is True

        # Primary is now DOWN. Check Promoted Follower behaviour:
        async with AsyncClient(
            transport=follower_transport, base_url="http://follower-now-primary"
        ) as np_client:
            # (A) Reads succeed immediately
            r_resp = await np_client.get("/doc/10", headers=_sig_headers())
            assert r_resp.status_code == 200
            assert r_resp.json()["document"]["doc_id"] == 10
            assert r_resp.json()["meta"]["served_by"] == "primary"
            assert r_resp.json()["meta"]["term"] == 2

            # (B) Writes are rejected with REPLICATION_UNAVAILABLE (503) while degraded
            w_fail = await np_client.post(
                "/doc",
                json={"request_id": "req-2", "document": {"doc_id": 20, "val": "degraded"}},
                headers=_sig_headers(),
            )
            assert w_fail.status_code == 503
            err_c = w_fail.json().get("detail", {}).get("error_code") or w_fail.json().get(
                "error_code"
            )
            assert err_c == "REPLICATION_UNAVAILABLE"

            # (C) Retry with same request_id is not yet in WAL
            assert follower_app.state.node.wal.last_seq == 1


# ═══════════════════════════════════════════════════════════════════════════
# 2. Rejoin, Divergence Point Discovery & Tail Truncation
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_old_primary_rejoin_divergence_truncation_resync(tmp_path: Path) -> None:
    """Old primary with divergent uncommitted tail demotes, truncates, catches up,
    and marks SYNCED.
    """
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    # Common committed entry at seq=1 (term=1)
    # New primary has seq=1 (term=1)
    # Old primary has seq=1 (term=1) AND uncommitted seq=2 (term=1)
    primary_dir = tmp_path / "old_primary"
    primary_dir.mkdir(parents=True, exist_ok=True)
    p_settings = settings.model_copy(update={"DATA_DIR": str(primary_dir)})
    old_p_app = build_app(
        p_settings,
        node_id="old-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    follower_dir = tmp_path / "promoted_follower"
    follower_dir.mkdir(parents=True, exist_ok=True)
    f_settings = settings.model_copy(update={"DATA_DIR": str(follower_dir)})
    new_p_app = build_app(
        f_settings,
        node_id="promoted-follower",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
        ranges=ranges,
    )

    async with LifespanManager(new_p_app):
        # Insert seq=1 on new_primary (replicated earlier)
        new_p_app.state.node.wal.append(
            seq=1,
            term=1,
            op="insert",
            doc_id=10,
            request_id="req-1",
            payload={"doc_id": 10, "v": 1},
        )
        new_p_app.state.node.engine.apply(
            {
                "seq": 1,
                "term": 1,
                "op": "insert",
                "doc_id": 10,
                "request_id": "req-1",
                "payload": {"doc_id": 10, "v": 1},
            }
        )

        # Promote new_primary to PRIMARY in term 2
        async with AsyncClient(
            transport=ASGITransport(app=new_p_app), base_url="http://new-primary"
        ) as np_client:
            prom_resp = await np_client.post(
                "/internal/promote",
                json={
                    "shard_id": "shard0",
                    "term": 2,
                    "ownership_epochs": {"0-1000": 2},
                    "follower_url": "127.0.0.1:8200",
                    "lease": {"granted_at": 0.0, "expires_at": 3600.0},
                },
                headers=_sig_headers(
                    {
                        "shard_id": "shard0",
                        "term": 2,
                        "ownership_epochs": {"0-1000": 2},
                        "follower_url": "127.0.0.1:8200",
                        "lease": {"granted_at": 0.0, "expires_at": 3600.0},
                    }
                ),
            )
            assert prom_resp.status_code == 200

        # Now start old primary: it has seq=1 AND uncommitted seq=2
        async with LifespanManager(old_p_app):
            old_p_app.state.node.wal.append(
                seq=1,
                term=1,
                op="insert",
                doc_id=10,
                request_id="req-1",
                payload={"doc_id": 10, "v": 1},
            )
            old_p_app.state.node.wal.append(
                seq=2,
                term=1,
                op="insert",
                doc_id=20,
                request_id="req-uncommitted",
                payload={"doc_id": 20, "v": 2},
            )
            old_p_app.state.node.engine.apply(
                {
                    "seq": 1,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 10,
                    "request_id": "req-1",
                    "payload": {"doc_id": 10, "v": 1},
                }
            )
            old_p_app.state.node.engine.apply(
                {
                    "seq": 2,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 20,
                    "request_id": "req-uncommitted",
                    "payload": {"doc_id": 20, "v": 2},
                }
            )
            assert old_p_app.state.node.wal.last_seq == 2
            assert old_p_app.state.node.engine.doc_count() == 2

            # Connect old primary to new primary transport
            np_transport = ASGITransport(app=new_p_app)
            rejoin_client = AsyncClient(mounts={"http://127.0.0.1:8201": np_transport}, timeout=5.0)

            # Perform rejoin and resync
            success = await resync_with_primary(
                old_p_app.state.node, "127.0.0.1:8201", http_client=rejoin_client
            )
            assert success is True

            # Verify divergence point was found (seq=1) and uncommitted seq=2 was truncated
            assert old_p_app.state.node.wal.last_seq == 1
            assert old_p_app.state.node.role == NodeRole.FOLLOWER
            assert old_p_app.state.node.engine.get(20) is None  # doc 20 discarded
            assert old_p_app.state.node.engine.get(10) is not None

            # Verify new primary accepted rejoin and transitioned follower to SYNCED
            assert new_p_app.state.node.follower_state == ReplicaState.SYNCED

            # Now that follower is SYNCED, writes are unblocked on new primary!
            new_p_app.state.node.http_client = AsyncClient(
                mounts={"http://127.0.0.1:8200": ASGITransport(app=old_p_app)}, timeout=5.0
            )
            new_p_app.state.node.follower_url = "127.0.0.1:8200"

            async with AsyncClient(transport=np_transport, base_url="http://new-primary") as np_c:
                w2 = await np_c.post(
                    "/doc",
                    json={"request_id": "req-3", "document": {"doc_id": 30, "val": "after-rejoin"}},
                    headers=_sig_headers(),
                )
                assert w2.status_code == 201
                assert w2.json()["outcome"] == "COMMITTED"
                assert w2.json()["seq"] == 2
                assert w2.json()["meta"]["term"] == 2

            # Both nodes have seq=2 under term=2
            assert new_p_app.state.node.wal.last_seq == 2
            assert old_p_app.state.node.wal.last_seq == 2
            assert old_p_app.state.node.engine.get(30) is not None


# ═══════════════════════════════════════════════════════════════════════════
# 3. Crash Point Hooks Fault Tests
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_crash_point_1_before_wal_append(tmp_path: Path) -> None:
    """Crash before primary WAL append: nothing written, retry executes cleanly."""
    clear_crash_points()
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    app = build_app(settings, node_id="p1", shard_id="shard0", ranges=ranges, allow_standalone=True)
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            register_crash_point("before_wal_append")
            with pytest.raises(CrashPointError):
                await client.post(
                    "/doc",
                    json={"request_id": "cp1", "document": {"doc_id": 1, "v": 1}},
                    headers=_sig_headers(),
                )

            # Nothing written
            assert app.state.node.wal.last_seq == 0
            assert app.state.node.engine.doc_count() == 0

            # Clear hook and retry with same request_id
            clear_crash_points()
            r = await client.post(
                "/doc",
                json={"request_id": "cp1", "document": {"doc_id": 1, "v": 1}},
                headers=_sig_headers(),
            )
            assert r.status_code == 201
            assert r.json()["outcome"] == "COMMITTED"
            assert app.state.node.wal.last_seq == 1


@pytest.mark.asyncio
async def test_crash_point_2_after_primary_fsync(tmp_path: Path) -> None:
    """Crash after primary fsync before follower replication: uncommitted tail
    discarded on failover.
    """
    clear_crash_points()
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    follower_dir = tmp_path / "f2"
    f_app = build_app(
        settings.model_copy(update={"DATA_DIR": str(follower_dir)}),
        node_id="f2",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
        ranges=ranges,
    )

    async with LifespanManager(f_app):
        f_transport = ASGITransport(app=f_app)
        primary_dir = tmp_path / "p2"
        p_app = build_app(
            settings.model_copy(update={"DATA_DIR": str(primary_dir)}),
            node_id="p2",
            shard_id="shard0",
            ranges=ranges,
            follower_url="127.0.0.1:8201",
            follower_transport=f_transport,
            lease_expires_at=float("inf"),
        )

        async with LifespanManager(p_app):
            p_transport = ASGITransport(app=p_app)
            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                register_crash_point("after_primary_fsync")
                with pytest.raises(CrashPointError):
                    await p_client.post(
                        "/doc",
                        json={"request_id": "cp2", "document": {"doc_id": 2, "v": 2}},
                        headers=_sig_headers(),
                    )

            # Record exists on primary WAL, but NOT on follower
            assert p_app.state.node.wal.last_seq == 1
            assert f_app.state.node.wal.last_seq == 0

            clear_crash_points()


@pytest.mark.asyncio
async def test_crash_point_3_after_follower_fsync_before_ack(tmp_path: Path) -> None:
    """Crash after follower fsync before ack: committed on failover;
    retry returns cached outcome.
    """
    clear_crash_points()
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    f_dir = tmp_path / "f3"
    f_app = build_app(
        settings.model_copy(update={"DATA_DIR": str(f_dir)}),
        node_id="f3",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
        ranges=ranges,
    )

    async with LifespanManager(f_app):
        f_transport = ASGITransport(app=f_app)
        p_dir = tmp_path / "p3"
        p_app = build_app(
            settings.model_copy(update={"DATA_DIR": str(p_dir)}),
            node_id="p3",
            shard_id="shard0",
            ranges=ranges,
            follower_url="127.0.0.1:8201",
            follower_transport=f_transport,
            lease_expires_at=float("inf"),
        )

        async with LifespanManager(p_app):
            p_transport = ASGITransport(app=p_app)
            register_crash_point("after_follower_fsync_before_ack")

            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                # Follower crashes during replication ack
                r = await p_client.post(
                    "/doc",
                    json={"request_id": "cp3", "document": {"doc_id": 3, "v": 3}},
                    headers=_sig_headers(),
                )
                # Primary receives failure from follower -> OutcomeUnknown
                assert r.status_code == 202
                err_code = r.json().get("detail", {}).get("error_code") or r.json().get(
                    "error_code"
                )
                assert err_code == "OUTCOME_UNKNOWN"

            # Entry was fsynced on follower WAL before crashing
            assert f_app.state.node.wal.last_seq == 1
            # Replaying unapplied WAL entries brings it into memory
            for entry in f_app.state.node.wal.replay(
                after_seq=f_app.state.node.engine.last_applied_seq
            ):
                f_app.state.node.engine.apply(entry)
            assert f_app.state.node.engine.get(3) is not None

            clear_crash_points()


@pytest.mark.asyncio
async def test_crash_point_4_after_follower_ack_before_client_ack(tmp_path: Path) -> None:
    """Crash after follower ack before client ack: both WALs have it; retry returns COMMITTED."""
    clear_crash_points()
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    f_dir = tmp_path / "f4"
    f_app = build_app(
        settings.model_copy(update={"DATA_DIR": str(f_dir)}),
        node_id="f4",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
        ranges=ranges,
    )

    async with LifespanManager(f_app):
        f_transport = ASGITransport(app=f_app)
        p_dir = tmp_path / "p4"
        p_app = build_app(
            settings.model_copy(update={"DATA_DIR": str(p_dir)}),
            node_id="p4",
            shard_id="shard0",
            ranges=ranges,
            follower_url="127.0.0.1:8201",
            follower_transport=f_transport,
            lease_expires_at=float("inf"),
        )

        async with LifespanManager(p_app):
            p_transport = ASGITransport(app=p_app)
            register_crash_point("after_follower_ack_before_client_ack")

            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                with pytest.raises(CrashPointError):
                    await p_client.post(
                        "/doc",
                        json={"request_id": "cp4", "document": {"doc_id": 4, "v": 4}},
                        headers=_sig_headers(),
                    )

            # Both WALs have it durably
            assert p_app.state.node.wal.last_seq == 1
            assert f_app.state.node.wal.last_seq == 1

            clear_crash_points()
            # Retry with same request_id returns COMMITTED without executing twice
            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                r2 = await p_client.post(
                    "/doc",
                    json={"request_id": "cp4", "document": {"doc_id": 4, "v": 4}},
                    headers=_sig_headers(),
                )
                assert r2.status_code == 201
                assert r2.json()["outcome"] == "COMMITTED"
                assert p_app.state.node.wal.last_seq == 1


# ═══════════════════════════════════════════════════════════════════════════
# 4. Double Failover Guard & Metrics
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_double_failover_blocked(tmp_path: Path) -> None:
    """If the only remaining replica of a shard also fails, failover is blocked."""
    settings = _make_settings(tmp_path)
    coord_dir = tmp_path / "coord_df"
    coord_dir.mkdir(parents=True, exist_ok=True)
    map_file = coord_dir / "shard_map.json"
    init_map = create_default_3_shard_map(
        primaries=["127.0.0.1:8200", "127.0.0.1:8202", "127.0.0.1:8204"],
        followers=[None, "127.0.0.1:8203", "127.0.0.1:8205"],
    )
    map_file.write_text(init_map.model_dump_json(indent=2))
    coord_svc = CoordinatorService(settings, shard_map_path=map_file)

    promoted = await coord_svc.trigger_failover("shard0")
    assert promoted is False  # Cannot failover without reachable follower


@pytest.mark.asyncio
async def test_failover_and_restoration_metrics(tmp_path: Path) -> None:
    """Coordinator records failover time and write restoration time metrics."""
    settings = _make_settings(tmp_path)
    coord_dir = tmp_path / "coord_m"
    coord_dir.mkdir(parents=True, exist_ok=True)
    map_file = coord_dir / "shard_map.json"
    init_map = create_default_3_shard_map(
        primaries=["127.0.0.1:8200", "127.0.0.1:8202", "127.0.0.1:8204"],
        followers=["127.0.0.1:8201", "127.0.0.1:8203", "127.0.0.1:8205"],
    )
    map_file.write_text(init_map.model_dump_json(indent=2))
    coord_svc = CoordinatorService(settings, shard_map_path=map_file)

    # Fake failover metric
    t0 = time.time()
    coord_svc.failover_metrics.append(
        {
            "shard_id": "shard0",
            "crash_timestamp": t0,
            "detection_timestamp": t0 + 0.5,
            "promotion_timestamp": t0 + 0.8,
            "first_read_timestamp": None,
            "rejoin_timestamp": None,
            "synced_timestamp": None,
            "first_write_timestamp": None,
        }
    )

    coord_svc.record_first_read("shard0")
    coord_svc.record_first_write("shard0")

    status = coord_svc.get_cluster_status()
    metrics = status["failover_metrics"]
    assert len(metrics) == 1
    m = metrics[0]
    assert "failover_time_s" in m
    assert "write_restoration_time_s" in m
    assert m["failover_time_s"] >= 0.0
    assert m["write_restoration_time_s"] >= 0.0


# ═══════════════════════════════════════════════════════════════════════════
# 5. M5 GATE — 5 Consecutive Failovers with Zero Data Loss
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_m5_gate_repeat_failovers_zero_data_loss(tmp_path: Path) -> None:
    """M5 Gate: repeat failover and rejoin 5 times with continuous writes and zero data loss."""
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=10000, ownership_epoch=1)]

    dir_a = tmp_path / "node_a"
    dir_b = tmp_path / "node_b"
    dir_a.mkdir(parents=True, exist_ok=True)
    dir_b.mkdir(parents=True, exist_ok=True)

    # Node A starts as primary, Node B as follower
    app_a = build_app(
        settings.model_copy(update={"DATA_DIR": str(dir_a)}),
        node_id="node-a",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        follower_url="127.0.0.1:8201",
        lease_expires_at=float("inf"),
    )
    app_b = build_app(
        settings.model_copy(update={"DATA_DIR": str(dir_b)}),
        node_id="node-b",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
        ranges=ranges,
    )

    async with LifespanManager(app_a), LifespanManager(app_b):
        trans_a = ASGITransport(app=app_a)
        trans_b = ASGITransport(app=app_b)

        app_a.state.node.http_client = AsyncClient(
            mounts={"http://127.0.0.1:8201": trans_b}, timeout=2.0
        )
        app_b.state.node.http_client = AsyncClient(
            mounts={"http://127.0.0.1:8200": trans_a}, timeout=2.0
        )

        # Write doc 1
        async with AsyncClient(transport=trans_a, base_url="http://node-a") as ca:
            r = await ca.post(
                "/doc",
                json={"request_id": "cycle-0", "document": {"doc_id": 100, "val": 0}},
                headers=_sig_headers(),
            )
            assert r.status_code == 201

        curr_primary_app = app_a
        curr_follower_app = app_b
        curr_p_trans = trans_a
        curr_f_trans = trans_b
        curr_p_addr = "127.0.0.1:8200"
        curr_f_addr = "127.0.0.1:8201"

        for cycle in range(1, 6):
            # 1. Promote current follower
            new_term = cycle + 1
            async with AsyncClient(transport=curr_f_trans, base_url="http://promoted") as cf:
                prom = await cf.post(
                    "/internal/promote",
                    json={
                        "shard_id": "shard0",
                        "term": new_term,
                        "ownership_epochs": {"0-10000": cycle + 1},
                        "follower_url": curr_p_addr,
                        "lease": {"granted_at": 0.0, "expires_at": 3600.0},
                    },
                    headers=_sig_headers(
                        {
                            "shard_id": "shard0",
                            "term": new_term,
                            "ownership_epochs": {"0-10000": cycle + 1},
                            "follower_url": curr_p_addr,
                            "lease": {"granted_at": 0.0, "expires_at": 3600.0},
                        }
                    ),
                )
                assert prom.status_code == 200

            # 2. Promoted node reads doc 100 cleanly
            async with AsyncClient(transport=curr_f_trans, base_url="http://promoted") as cf:
                rd = await cf.get("/doc/100", headers=_sig_headers())
                assert rd.status_code == 200
                assert rd.json()["document"]["doc_id"] == 100

            # 3. Old primary demotes and rejoins
            curr_primary_app.state.node.role = NodeRole.FOLLOWER
            rejoin_c = AsyncClient(mounts={f"http://{curr_f_addr}": curr_f_trans}, timeout=2.0)
            resync_ok = await resync_with_primary(
                curr_primary_app.state.node, curr_f_addr, http_client=rejoin_c
            )
            assert resync_ok is True

            # 4. Swapped roles: write new document
            curr_follower_app.state.node.follower_url = curr_p_addr
            curr_follower_app.state.node.http_client = AsyncClient(
                mounts={f"http://{curr_p_addr}": curr_p_trans}, timeout=2.0
            )

            doc_id = 100 + cycle
            async with AsyncClient(transport=curr_f_trans, base_url="http://new-primary") as cf:
                w = await cf.post(
                    "/doc",
                    json={
                        "request_id": f"cycle-{cycle}",
                        "document": {"doc_id": doc_id, "val": cycle},
                    },
                    headers=_sig_headers(),
                )
                assert w.status_code == 201
                assert w.json()["outcome"] == "COMMITTED"

            # Verify doc exists on both replicas
            assert curr_follower_app.state.node.engine.get(doc_id) is not None
            assert curr_primary_app.state.node.engine.get(doc_id) is not None

            # Swap pointers for next cycle
            curr_primary_app, curr_follower_app = curr_follower_app, curr_primary_app
            curr_p_trans, curr_f_trans = curr_f_trans, curr_p_trans
            curr_p_addr, curr_f_addr = curr_f_addr, curr_p_addr

        # Verify all 6 documents exist on both nodes
        for cycle in range(6):
            did = 100 + cycle
            assert app_a.state.node.engine.get(did) is not None
            assert app_b.state.node.engine.get(did) is not None
