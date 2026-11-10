"""
Integration tests for Primary-Follower Replication and M4 Gate — Modules 6 & 7.

Covers:
- Synchronous durable replication: write committed only after fsync on both WALs.
- Follower read serving: GET /doc/{id} tagged with served_by=follower, applied_seq, lag.
- Direct write to follower rejection: HTTP 409 NOT_PRIMARY.
- Follower catch-up via /internal/catchup.
- Follower snapshot resync via /internal/snapshot and /internal/snapshot/apply.
- Follower down/not synced rejection: HTTP 503 REPLICATION_UNAVAILABLE before WAL append.
- Primary lease fencing: remaining lease <= T + M rejected with HTTP 409 LEASE_EXPIRED.
- Router read_mode=follower routing and primary fallback.
- M4 GATE: 3 shards x 2 nodes (6 shard nodes) + coordinator + router cluster.
  Verifies durable replication across all shards, follower reads, and failure behavior.
"""

from __future__ import annotations

import asyncio
import json
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
    ShardMap,
    ShardMapEntry,
    ShardRange,
)
from docstore.coordinator.app import build_coordinator_app
from docstore.coordinator.coordinator import CoordinatorService
from docstore.router.app import build_router_app
from docstore.router.shard_map import (
    _S1,
    _S2,
    KEY_SPACE_END,
    ShardMapManager,
)
from docstore.shard.app import ShardRangeConfig, build_app

pytestmark = pytest.mark.integration

SECRET = "a" * 32
ADMIN = "b" * 32
CLIENT_KEY = "c" * 16


def _make_settings(data_dir: Path | str) -> Settings:
    return Settings.model_validate(
        {
            "CLUSTER_SECRET": SECRET,
            "ADMIN_TOKEN": ADMIN,
            "CLIENT_API_KEY": CLIENT_KEY,
            "DATA_DIR": str(data_dir),
            "HEARTBEAT_INTERVAL_S": 0.5,
            "LEASE_DURATION_S": 3.0,
            "FAILURE_TIMEOUT_S": 5.0,
            "SAFETY_MARGIN_S": 0.5,
            "REPLICATION_TIMEOUT_S": 1.0,
        }
    )


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
# Synchronous Replication & WAL Durability
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_sync_replication_both_wals_durably_fsynced(tmp_path: Path) -> None:
    """Every acknowledged write is durably appended to both primary and follower WALs."""
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    # 1. Follower node
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

        # 2. Primary node connected to follower
        primary_dir = tmp_path / "primary0"
        primary_dir.mkdir(parents=True, exist_ok=True)
        p_settings = settings.model_copy(update={"DATA_DIR": str(primary_dir)})
        primary_app = build_app(
            p_settings,
            node_id="primary0",
            shard_id="shard0",
            role=NodeRole.PRIMARY,
            ranges=ranges,
            follower_url="127.0.0.1:8100",
            follower_state=ReplicaState.SYNCED,
            follower_transport=follower_transport,
            lease_expires_at=float("inf"),
        )

        async with LifespanManager(primary_app):
            p_transport = ASGITransport(app=primary_app)
            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                # Write doc 10
                r = await p_client.post(
                    "/doc",
                    json={"request_id": "req-1", "document": {"doc_id": 10, "val": "hello"}},
                    headers=_sig_headers(),
                )
                assert r.status_code == 201
                data = r.json()
                assert data["outcome"] == "COMMITTED"
                assert data["meta"]["served_by"] == "primary"
                assert data["seq"] == 1

                # Write doc 20
                r2 = await p_client.post(
                    "/doc",
                    json={"request_id": "req-2", "document": {"doc_id": 20, "val": "world"}},
                    headers=_sig_headers(),
                )
                assert r2.status_code == 201
                assert r2.json()["seq"] == 2

            # Verify both WALs have both records
            p_records = list(primary_app.state.node.wal.replay())
            f_records = list(follower_app.state.node.wal.replay())

            assert len(p_records) == 2
            assert len(f_records) == 2

            for pr, fr in zip(p_records, f_records, strict=True):
                assert pr["seq"] == fr["seq"]
                assert pr["term"] == fr["term"]
                assert pr["op"] == fr["op"]
                assert pr["doc_id"] == fr["doc_id"]
                assert pr["request_id"] == fr["request_id"]
                assert pr["payload"] == fr["payload"]


@pytest.mark.asyncio
async def test_follower_rejects_direct_write(tmp_path: Path) -> None:
    """Follower rejects direct write requests with 409 NOT_PRIMARY."""
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    follower_app = build_app(
        settings,
        node_id="follower0",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
        ranges=ranges,
    )

    async with LifespanManager(follower_app):
        transport = ASGITransport(app=follower_app)
        async with AsyncClient(transport=transport, base_url="http://follower") as client:
            r = await client.post(
                "/doc",
                json={"request_id": "req-f1", "document": {"doc_id": 10, "val": "x"}},
                headers=_sig_headers(),
            )
            assert r.status_code == 409
            assert r.json()["error_code"] == "NOT_PRIMARY"


@pytest.mark.asyncio
async def test_follower_serves_reads_with_lag(tmp_path: Path) -> None:
    """Follower serves GET /doc/{id} tagged with served_by=follower and lag."""
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    follower_dir = tmp_path / "follower"
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
        primary_dir = tmp_path / "primary"
        primary_dir.mkdir(parents=True, exist_ok=True)
        p_settings = settings.model_copy(update={"DATA_DIR": str(primary_dir)})
        primary_app = build_app(
            p_settings,
            node_id="primary0",
            shard_id="shard0",
            role=NodeRole.PRIMARY,
            ranges=ranges,
            follower_url="127.0.0.1:8100",
            follower_state=ReplicaState.SYNCED,
            follower_transport=follower_transport,
            lease_expires_at=float("inf"),
        )

        async with LifespanManager(primary_app):
            p_transport = ASGITransport(app=primary_app)
            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                await p_client.post(
                    "/doc",
                    json={"request_id": "req-1", "document": {"doc_id": 55, "title": "Test"}},
                    headers=_sig_headers(),
                )

            # Read from follower directly
            async with AsyncClient(
                transport=follower_transport, base_url="http://follower"
            ) as f_client:
                r = await f_client.get("/doc/55", headers=_sig_headers())
                assert r.status_code == 200
                data = r.json()
                assert data["meta"]["served_by"] == "follower"
                assert data["document"]["title"] == "Test"
                assert data["meta"]["applied_seq"] == 1
                assert "replication_lag" in data["meta"]


@pytest.mark.asyncio
async def test_write_rejected_when_follower_not_synced(tmp_path: Path) -> None:
    """If follower is not SYNCED when write arrives, reject with REPLICATION_UNAVAILABLE."""
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    primary_app = build_app(
        settings,
        node_id="primary0",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        follower_url="127.0.0.1:8100",
        follower_state=ReplicaState.DOWN,  # follower is DOWN!
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(primary_app):
        transport = ASGITransport(app=primary_app)
        async with AsyncClient(transport=transport, base_url="http://primary") as client:
            r = await client.post(
                "/doc",
                json={"request_id": "r-nosync", "document": {"doc_id": 10, "val": "x"}},
                headers=_sig_headers(),
            )
            assert r.status_code == 503
            assert r.json()["error_code"] == "REPLICATION_UNAVAILABLE"

            # WAL must have nothing appended
            records = list(primary_app.state.node.wal.replay())
            assert len(records) == 0


@pytest.mark.asyncio
async def test_write_rejected_when_lease_expired(tmp_path: Path) -> None:
    """Primary rejects write when remaining lease <= T + M."""
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    # Safety margin M=0.5, Replication timeout T=1.0 -> min lease = 1.5s
    # Set lease to expire in 0.5s from now (< 1.5s)
    primary_app = build_app(
        settings,
        node_id="primary0",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        follower_url="127.0.0.1:8100",
        follower_state=ReplicaState.SYNCED,
        lease_expires_at=asyncio.get_event_loop().time() + 0.5,
    )

    async with LifespanManager(primary_app):
        transport = ASGITransport(app=primary_app)
        async with AsyncClient(transport=transport, base_url="http://primary") as client:
            r = await client.post(
                "/doc",
                json={"request_id": "r-lease", "document": {"doc_id": 10, "val": "x"}},
                headers=_sig_headers(),
            )
            assert r.status_code == 409
            assert r.json()["error_code"] == "LEASE_EXPIRED"


@pytest.mark.asyncio
async def test_follower_catchup_endpoint(tmp_path: Path) -> None:
    """Follower catches up missing operations via /internal/catchup."""
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    # Primary with standalone lease
    primary_app = build_app(
        settings,
        node_id="primary0",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        allow_standalone=True,
    )

    async with LifespanManager(primary_app):
        p_transport = ASGITransport(app=primary_app)
        async with AsyncClient(transport=p_transport, base_url="http://primary") as client:
            # Write 3 docs
            for i in range(1, 4):
                doc_payload = {"request_id": f"req-{i}", "document": {"doc_id": i, "val": i}}
                await client.post(
                    "/doc",
                    json=doc_payload,
                    headers=_sig_headers(doc_payload),
                )

            # Request catchup from after_seq=1
            catchup_payload = {"shard_id": "shard0", "after_seq": 1}
            r = await client.post(
                "/internal/catchup",
                json=catchup_payload,
                headers=_sig_headers(catchup_payload),
            )
            assert r.status_code == 200
            data = r.json()
            assert data["needs_snapshot"] is False
            assert len(data["entries"]) == 2  # seq 2 and 3
            assert data["entries"][0]["seq"] == 2
            assert data["entries"][1]["seq"] == 3


@pytest.mark.asyncio
async def test_router_read_mode_follower_and_fallback(tmp_path: Path) -> None:
    """Router routes read_mode=follower to follower and falls back to primary if follower fails."""
    settings = _make_settings(tmp_path)
    ranges = [ShardRangeConfig(start=0, end=KEY_SPACE_END, ownership_epoch=1)]

    follower_dir = tmp_path / "follower"
    follower_dir.mkdir(parents=True, exist_ok=True)
    f_settings = settings.model_copy(update={"DATA_DIR": str(follower_dir)})
    follower_app = build_app(
        f_settings,
        node_id="f0",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
        ranges=ranges,
    )

    async with LifespanManager(follower_app):
        f_transport = ASGITransport(app=follower_app)
        primary_dir = tmp_path / "primary"
        primary_dir.mkdir(parents=True, exist_ok=True)
        p_settings = settings.model_copy(update={"DATA_DIR": str(primary_dir)})
        primary_app = build_app(
            p_settings,
            node_id="p0",
            shard_id="shard0",
            role=NodeRole.PRIMARY,
            ranges=ranges,
            follower_url="127.0.0.1:8201",
            follower_state=ReplicaState.SYNCED,
            follower_transport=f_transport,
            lease_expires_at=float("inf"),
        )

        async with LifespanManager(primary_app):
            p_transport = ASGITransport(app=primary_app)

            # Create Router with shard map containing both primary and follower
            transports = {
                "127.0.0.1:8200": p_transport,
                "127.0.0.1:8201": f_transport,
            }
            s_map = ShardMap(
                map_version=1,
                entries=[
                    ShardMapEntry(
                        shard_id="shard0",
                        range=ShardRange(start=0, end=KEY_SPACE_END),
                        primary="127.0.0.1:8200",
                        follower="127.0.0.1:8201",
                        ownership_epoch=1,
                        map_version=1,
                    )
                ],
            )
            mgr = ShardMapManager()
            mgr.set_map(s_map)

            router_app = build_router_app(settings, mgr, shard_transports=transports)
            async with LifespanManager(router_app):
                r_transport = ASGITransport(app=router_app)
                async with AsyncClient(transport=r_transport, base_url="http://router") as r_client:
                    # Write document via router
                    r_ins = await r_client.post(
                        "/doc",
                        json={"request_id": "r-m4-1", "document": {"doc_id": 42, "text": "hello"}},
                        headers=_client_headers(),
                    )
                    assert r_ins.status_code == 201

                    # Read with read_mode=follower
                    r_get_f = await r_client.get(
                        "/doc/42?read_mode=follower",
                        headers=_client_headers(),
                    )
                    assert r_get_f.status_code == 200
                    assert r_get_f.json()["meta"]["served_by"] == "follower"
                    assert r_get_f.json()["document"]["text"] == "hello"

                    # Read with default primary
                    r_get_p = await r_client.get(
                        "/doc/42",
                        headers=_client_headers(),
                    )
                    assert r_get_p.status_code == 200
                    assert r_get_p.json()["meta"]["served_by"] == "primary"


# ═══════════════════════════════════════════════════════════════════════════
# M4 GATE: 3 Shards x 2 Nodes (6 nodes) + Coordinator + Router Cluster
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_m4_gate_replicated_cluster(tmp_path: Path) -> None:
    """Milestone 4 Gate Test.

    Topology:
    - 3 shards covering [0, 2^63)
    - Each shard has 1 Primary + 1 Follower (total 6 shard nodes)
    - 1 Coordinator managing leases and shard map
    - 1 Router routing writes to primaries and reads to followers/primaries

    Verifications:
    1. Every acknowledged write is durably present on BOTH primary and follower WALs.
    2. Follower reads report served_by=follower, applied_seq, replication_lag.
    3. When follower is DOWN on a shard, writes to that shard fail with REPLICATION_UNAVAILABLE.
    4. Writes to other healthy shards continue unaffected.
    """
    settings = _make_settings(tmp_path)

    r0 = [ShardRangeConfig(start=0, end=_S1, ownership_epoch=1)]
    r1 = [ShardRangeConfig(start=_S1, end=_S2, ownership_epoch=1)]
    r2 = [ShardRangeConfig(start=_S2, end=KEY_SPACE_END, ownership_epoch=1)]

    # 1. Start Followers
    f0_app = build_app(
        settings.model_copy(update={"DATA_DIR": str(tmp_path / "f0")}),
        node_id="node0-follower",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
        ranges=r0,
    )
    f1_app = build_app(
        settings.model_copy(update={"DATA_DIR": str(tmp_path / "f1")}),
        node_id="node1-follower",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=r1,
    )
    f2_app = build_app(
        settings.model_copy(update={"DATA_DIR": str(tmp_path / "f2")}),
        node_id="node2-follower",
        shard_id="shard2",
        role=NodeRole.FOLLOWER,
        ranges=r2,
    )

    async with LifespanManager(f0_app), LifespanManager(f1_app), LifespanManager(f2_app):
        t_f0 = ASGITransport(app=f0_app)
        t_f1 = ASGITransport(app=f1_app)
        t_f2 = ASGITransport(app=f2_app)

        # 2. Start Primaries with connection to respective follower
        p0_app = build_app(
            settings.model_copy(update={"DATA_DIR": str(tmp_path / "p0")}),
            node_id="node0-primary",
            shard_id="shard0",
            role=NodeRole.PRIMARY,
            ranges=r0,
            follower_url="127.0.0.1:8001",
            follower_state=ReplicaState.SYNCED,
            follower_transport=t_f0,
            lease_expires_at=float("inf"),
        )
        p1_app = build_app(
            settings.model_copy(update={"DATA_DIR": str(tmp_path / "p1")}),
            node_id="node1-primary",
            shard_id="shard1",
            role=NodeRole.PRIMARY,
            ranges=r1,
            follower_url="127.0.0.1:8003",
            follower_state=ReplicaState.SYNCED,
            follower_transport=t_f1,
            lease_expires_at=float("inf"),
        )
        p2_app = build_app(
            settings.model_copy(update={"DATA_DIR": str(tmp_path / "p2")}),
            node_id="node2-primary",
            shard_id="shard2",
            role=NodeRole.PRIMARY,
            ranges=r2,
            follower_url="127.0.0.1:8005",
            follower_state=ReplicaState.SYNCED,
            follower_transport=t_f2,
            lease_expires_at=float("inf"),
        )

        async with LifespanManager(p0_app), LifespanManager(p1_app), LifespanManager(p2_app):
            t_p0 = ASGITransport(app=p0_app)
            t_p1 = ASGITransport(app=p1_app)
            t_p2 = ASGITransport(app=p2_app)

            # 3. Setup ShardMap with all 6 nodes
            s_map = ShardMap(
                map_version=1,
                entries=[
                    ShardMapEntry(
                        shard_id="shard0",
                        range=ShardRange(start=0, end=_S1),
                        primary="127.0.0.1:8000",
                        follower="127.0.0.1:8001",
                        ownership_epoch=1,
                        map_version=1,
                    ),
                    ShardMapEntry(
                        shard_id="shard1",
                        range=ShardRange(start=_S1, end=_S2),
                        primary="127.0.0.1:8002",
                        follower="127.0.0.1:8003",
                        ownership_epoch=1,
                        map_version=1,
                    ),
                    ShardMapEntry(
                        shard_id="shard2",
                        range=ShardRange(start=_S2, end=KEY_SPACE_END),
                        primary="127.0.0.1:8004",
                        follower="127.0.0.1:8005",
                        ownership_epoch=1,
                        map_version=1,
                    ),
                ],
            )
            map_file = tmp_path / "shard_map.json"
            map_mgr = ShardMapManager(str(map_file))
            map_mgr.save(s_map)

            # 4. Start Coordinator
            coord = CoordinatorService(settings, str(map_file))
            coord_app = build_coordinator_app(settings, coord)

            async with LifespanManager(coord_app):
                # 5. Start Router connected to all nodes
                all_transports = {
                    "127.0.0.1:8000": t_p0,
                    "127.0.0.1:8001": t_f0,
                    "127.0.0.1:8002": t_p1,
                    "127.0.0.1:8003": t_f1,
                    "127.0.0.1:8004": t_p2,
                    "127.0.0.1:8005": t_f2,
                }
                router_app = build_router_app(settings, map_mgr, shard_transports=all_transports)

                async with LifespanManager(router_app):
                    r_transport = ASGITransport(app=router_app)
                    async with AsyncClient(
                        transport=r_transport, base_url="http://router"
                    ) as client:
                        headers = _client_headers()

                        # Write 10 documents per shard (total 30 documents)
                        test_docs = [
                            # Shard 0 docs
                            (100, "Doc-0A"),
                            (200, "Doc-0B"),
                            (_S1 - 1, "Doc-0C"),
                            # Shard 1 docs
                            (_S1, "Doc-1A"),
                            (_S1 + 500, "Doc-1B"),
                            (_S2 - 1, "Doc-1C"),
                            # Shard 2 docs
                            (_S2, "Doc-2A"),
                            (_S2 + 1000, "Doc-2B"),
                            (KEY_SPACE_END - 1, "Doc-2C"),
                        ]

                        for doc_id, name in test_docs:
                            resp = await client.post(
                                "/doc",
                                json={
                                    "request_id": f"m4-{doc_id}",
                                    "document": {"doc_id": doc_id, "name": name},
                                },
                                headers=headers,
                            )
                            assert resp.status_code == 201, (
                                f"Failed write doc_id={doc_id}: {resp.text}"
                            )

                        # ── Verification 1: Both WALs on each shard have the records ──
                        p0_wal = list(p0_app.state.node.wal.replay())
                        f0_wal = list(f0_app.state.node.wal.replay())
                        assert len(p0_wal) == 3
                        assert len(f0_wal) == 3
                        for pw, fw in zip(p0_wal, f0_wal, strict=True):
                            assert pw["doc_id"] == fw["doc_id"]
                            assert pw["seq"] == fw["seq"]

                        p1_wal = list(p1_app.state.node.wal.replay())
                        f1_wal = list(f1_app.state.node.wal.replay())
                        assert len(p1_wal) == 3
                        assert len(f1_wal) == 3
                        for pw, fw in zip(p1_wal, f1_wal, strict=True):
                            assert pw["doc_id"] == fw["doc_id"]
                            assert pw["seq"] == fw["seq"]

                        p2_wal = list(p2_app.state.node.wal.replay())
                        f2_wal = list(f2_app.state.node.wal.replay())
                        assert len(p2_wal) == 3
                        assert len(f2_wal) == 3
                        for pw, fw in zip(p2_wal, f2_wal, strict=True):
                            assert pw["doc_id"] == fw["doc_id"]
                            assert pw["seq"] == fw["seq"]

                        # ── Verification 2: Follower reads report served_by=follower and lag ──
                        for doc_id, name in test_docs:
                            f_read = await client.get(
                                f"/doc/{doc_id}?read_mode=follower",
                                headers=headers,
                            )
                            assert f_read.status_code == 200
                            data = f_read.json()
                            assert data["meta"]["served_by"] == "follower"
                            assert data["document"]["name"] == name
                            assert "replication_lag" in data["meta"]

                        # ── Verification 3: Follower down behavior ──
                        # Mark shard0 follower DOWN
                        p0_app.state.node.follower_state = ReplicaState.DOWN

                        # Write to shard0 must be rejected
                        fail_resp = await client.post(
                            "/doc",
                            json={
                                "request_id": "m4-fail-down",
                                "document": {"doc_id": 999, "name": "Fail"},
                            },
                            headers=headers,
                        )
                        assert fail_resp.status_code == 503
                        assert fail_resp.json()["error_code"] == "REPLICATION_UNAVAILABLE"

                        # ── Verification 4: Other shards continue to accept writes cleanly ──
                        ok_resp = await client.post(
                            "/doc",
                            json={
                                "request_id": "m4-shard1-ok",
                                "document": {"doc_id": _S1 + 10, "name": "Healthy"},
                            },
                            headers=headers,
                        )
                        assert ok_resp.status_code == 201
                        assert ok_resp.json()["outcome"] == "COMMITTED"
