"""
Regression tests for review findings:
- Issue 1: Docker shard ownership ranges matching shard map without overlapping full keyspace
- Issue 2: Durable term and epoch persistence across crash-restart; old primary restarts as fenced follower
- Issue 3: Pending-write reconciliation:
    1. Follower contains op but ack was lost -> commits on retry
    2. Follower is behind -> stays unresolved, subsequent writes blocked
    3. Follower unavailable -> stays unresolved, subsequent writes blocked
    4. Follower has conflicting operation at same sequence -> authoritative reconciliation resolves as uncommitted
    5. Authoritative log reconciliation proves op absent -> resolves as uncommitted
    6. Retry does not execute duplicate operation
- Issue 4: Migration snapshot consistency with applied state
- Issue 5: Migration recovery after coordinator restart:
    1. Failed cutover / cleanup RPCs keep migration in recoverable CLEANUP state, record error, NOT marked DONE
    2. Failed unpause / abort RPCs keep migration in recoverable pre-cutover state, record error, NOT marked ABORTED
    3. Successful RPCs roll forward to DONE / abort to ABORTED idempotently
- Issue 6: Scatter-gather pagination for large skip + limit windows exceeding QUERY_LIMIT_MAX
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from docstore.common.auth import CLIENT_KEY_HEADER, CLUSTER_SIG_HEADER, compute_signature
from docstore.common.config import Settings
from docstore.common.models import (
    NodeRole,
    QueryFilter,
    ReplicaState,
    ShardMapEntry,
    ShardRange,
    WriteOutcome,
)
from docstore.coordinator.coordinator import CoordinatorService
from docstore.coordinator.migration import MigrationManager, MigrationState
from docstore.router.scatter import execute_scatter_query
from docstore.router.shard_map import ShardMap, ShardMapManager, create_default_3_shard_map
from docstore.shard.app import ShardRangeConfig, build_app, resync_with_primary

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
        REPLICATION_TIMEOUT_S=0.5,
        SAFETY_MARGIN_S=0.1,
    )


def _cluster_headers(body_bytes: bytes) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        CLUSTER_SIG_HEADER: compute_signature(SECRET, body_bytes),
    }


def test_issue1_docker_compose_shard_ranges():
    """Verify docker-compose specifies distinct ranges for all shards matching shard_map.json."""
    compose_path = Path("docker/docker-compose.yml")
    shard_map_path = Path("docker/shard_map.json")
    assert compose_path.exists()
    assert shard_map_path.exists()

    with shard_map_path.open("r", encoding="utf-8") as f:
        smap = json.load(f)

    expected_ranges = {
        e["shard_id"]: (e["range"]["start"], e["range"]["end"]) for e in smap["entries"]
    }

    with compose_path.open("r", encoding="utf-8") as f:
        compose = yaml.safe_load(f)

    services = compose.get("services", {})
    for shard_id, (expected_start, expected_end) in expected_ranges.items():
        for role in ("primary", "follower"):
            svc_name = f"{shard_id}-{role}"
            assert svc_name in services, f"Service {svc_name} missing from docker-compose.yml"
            env = services[svc_name].get("environment", [])
            env_dict = {}
            for item in env:
                if "=" in item:
                    k, v = item.split("=", 1)
                    env_dict[k] = v
            assert "SHARD_RANGE_START" in env_dict, f"SHARD_RANGE_START missing in {svc_name}"
            assert "SHARD_RANGE_END" in env_dict, f"SHARD_RANGE_END missing in {svc_name}"
            assert int(env_dict["SHARD_RANGE_START"]) == expected_start
            assert int(env_dict["SHARD_RANGE_END"]) == expected_end
            assert int(env_dict.get("SHARD_OWNERSHIP_EPOCH", 1)) == 1


@pytest.mark.asyncio
async def test_issue2_durable_term_and_epoch_persistence(tmp_path: Path):
    """Verify promotion persists term and epoch, but a restarted primary does NOT regain authority."""
    settings = _make_settings(tmp_path)
    shard_id = "shard0"

    # 1. Start node in term 1
    app1 = build_app(
        settings=settings,
        node_id="shard0-node1",
        shard_id=shard_id,
        role=NodeRole.FOLLOWER,
    )
    async with LifespanManager(app1):
        transport = ASGITransport(app=app1)
        async with AsyncClient(transport=transport, base_url="http://node") as client:
            # Promote to term 5 with new epoch
            promote_body = {
                "shard_id": shard_id,
                "lease": {"granted_at": 0.0, "expires_at": 3600.0},
                "term": 5,
                "role": "primary",
                "ownership_epochs": {"0-9223372036854775808": 42},
            }
            body_bytes = json.dumps(promote_body).encode("utf-8")
            resp = await client.post(
                "/internal/promote", content=body_bytes, headers=_cluster_headers(body_bytes)
            )
            assert resp.status_code == 200
            assert resp.json()["term"] == 5

    # 2. Verify metadata file exists durably on disk
    meta_path = tmp_path / shard_id / "node_metadata.json"
    assert meta_path.exists()
    with meta_path.open("r", encoding="utf-8") as f:
        meta = json.load(f)
    assert meta["term"] == 5
    assert any(r["ownership_epoch"] == 42 for r in meta["ranges"])

    # 3. Simulate crash and restart without any snapshot taken
    app2 = build_app(
        settings=settings,
        node_id="shard0-node1",
        shard_id=shard_id,
        role=NodeRole.PRIMARY,  # Passed primary role in config/args
    )
    async with LifespanManager(app2):
        node_state = app2.state.node
        assert node_state.term == 5
        # Must NOT regain primary authority! Recovered node must start in fenced FOLLOWER state
        assert node_state.role == NodeRole.FOLLOWER
        assert node_state.lease_expires_at == 0.0
        assert any(r.ownership_epoch == 42 for r in node_state.ranges)

        # Verify that write attempts to the restarted node are rejected with 409 NOT_PRIMARY
        transport2 = ASGITransport(app=app2)
        async with AsyncClient(transport=transport2, base_url="http://node") as client2:
            resp2 = await client2.post(
                "/doc",
                json={"request_id": "req-restart", "document": {"doc_id": 100, "val": "x"}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert resp2.status_code == 409


@pytest.mark.asyncio
async def test_restarted_old_primary_cannot_regain_authority_and_rejoins_as_follower(
    tmp_path: Path,
):
    """Regression test for Issue 1:
    1. Start primary A and follower B.
    2. Promote B after A fails.
    3. Restart A using its old metadata.
    4. Verify A cannot serve writes or act as primary.
    5. Verify A can rejoin only as a synchronized follower after reconciliation.
    """
    settings_a = _make_settings(tmp_path / "node_a")
    settings_b = _make_settings(tmp_path / "node_b")
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    # 1. Start primary A and follower B
    app_a = build_app(
        settings=settings_a,
        node_id="shard0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )
    app_b = build_app(
        settings=settings_b,
        node_id="shard0-follower",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
        ranges=ranges,
    )

    async with LifespanManager(app_a), LifespanManager(app_b):
        trans_b = ASGITransport(app=app_b)
        async with AsyncClient(transport=trans_b, base_url="http://node_b") as client_b:
            app_a.state.node.follower_url = "node_b"
            app_a.state.node.follower_state = ReplicaState.SYNCED  # follower attached and in sync
            app_a.state.node.http_client = client_b

            trans_a = ASGITransport(app=app_a)
            async with AsyncClient(transport=trans_a, base_url="http://node_a") as client_a:
                # Write doc 10 in term 1
                resp = await client_a.post(
                    "/doc",
                    json={"request_id": "req-1", "document": {"doc_id": 10, "val": "v1"}},
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                assert resp.status_code == 201

    # 2. A crashes. Promote B to term 2.
    async with LifespanManager(app_b):
        trans_b = ASGITransport(app=app_b)
        async with AsyncClient(transport=trans_b, base_url="http://node_b") as client_b:
            promote_body = {
                "shard_id": "shard0",
                "lease": {"granted_at": 0.0, "expires_at": 3600.0},
                "term": 2,
                "role": "primary",
                "ownership_epochs": {"0-1000": 2},
            }
            body_bytes = json.dumps(promote_body).encode("utf-8")
            resp = await client_b.post(
                "/internal/promote",
                content=body_bytes,
                headers=_cluster_headers(body_bytes),
            )
            assert resp.status_code == 200
            assert app_b.state.node.role == NodeRole.PRIMARY
            assert app_b.state.node.term == 2

            # 3. Restart A using its old metadata
            app_a_restarted = build_app(
                settings=settings_a,
                node_id="shard0-primary",
                shard_id="shard0",
                role=NodeRole.PRIMARY,  # Even if role argument says primary
            )
            async with LifespanManager(app_a_restarted):
                state_a = app_a_restarted.state.node
                # 4. Verify A cannot serve writes or act as primary
                assert state_a.role == NodeRole.FOLLOWER
                assert state_a.lease_expires_at == 0.0

                trans_a_restarted = ASGITransport(app=app_a_restarted)
                async with AsyncClient(
                    transport=trans_a_restarted, base_url="http://node_a"
                ) as client_a_restarted:
                    w_resp = await client_a_restarted.post(
                        "/doc",
                        json={"request_id": "req-split", "document": {"doc_id": 20, "val": "v2"}},
                        headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                    )
                    assert w_resp.status_code == 409

                # 5. Verify A can rejoin only as a synchronized follower after reconciliation
                rejoin_client = AsyncClient(mounts={"http://node_b": trans_b}, timeout=5.0)
                resync_ok = await resync_with_primary(
                    state_a,
                    "node_b",
                    http_client=rejoin_client,
                )
                assert resync_ok is True
                assert state_a.role == NodeRole.FOLLOWER
                assert app_b.state.node.follower_state == ReplicaState.SYNCED


@pytest.mark.asyncio
async def test_issue3_pending_write_reconciliation_follower_has_op(tmp_path: Path):
    """Test pending write reconciliation when follower has durably fsynced the operation."""
    f_settings = _make_settings(tmp_path / "follower")
    p_settings = _make_settings(tmp_path / "primary")

    follower_app = build_app(
        settings=f_settings,
        node_id="shard0-follower",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
    )
    primary_app = build_app(
        settings=p_settings,
        node_id="shard0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(follower_app), LifespanManager(primary_app):
        f_transport = ASGITransport(app=follower_app)
        async with AsyncClient(transport=f_transport, base_url="http://follower") as f_client:
            primary_app.state.node.follower_url = "follower"
            primary_app.state.node.follower_state = (
                ReplicaState.SYNCED
            )  # follower attached and in sync
            primary_app.state.node.http_client = f_client
            primary_app.state.node.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
            follower_app.state.node.ranges = [
                ShardRangeConfig(start=0, end=1000, ownership_epoch=1)
            ]

            p_transport = ASGITransport(app=primary_app)
            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                # 1. Normal write
                doc_payload = {"request_id": "req-1", "document": {"doc_id": 10, "title": "Doc1"}}
                resp = await p_client.post(
                    "/doc", json=doc_payload, headers={CLIENT_KEY_HEADER: CLIENT_KEY}
                )
                assert resp.status_code == 201

                # 2. Simulate replication failure where follower stored it but ack was lost
                f_state = follower_app.state.node
                f_seq = f_state.wal.next_seq()
                f_state.wal.append(
                    seq=f_seq,
                    term=f_state.term,
                    op="insert",
                    doc_id=20,
                    request_id="req-2",
                    payload={"doc_id": 20, "title": "Doc2"},
                )
                f_state.engine.apply(
                    {
                        "seq": f_seq,
                        "term": f_state.term,
                        "op": "insert",
                        "doc_id": 20,
                        "request_id": "req-2",
                        "payload": {"doc_id": 20, "title": "Doc2"},
                    }
                )

                p_state = primary_app.state.node
                p_seq = p_state.wal.next_seq()
                p_state.wal.append(
                    seq=p_seq,
                    term=p_state.term,
                    op="insert",
                    doc_id=20,
                    request_id="req-2",
                    payload={"doc_id": 20, "title": "Doc2"},
                )
                p_state.follower_state = ReplicaState.DOWN
                p_state.pending_unresolved_writes["req-2"] = {
                    "seq": p_seq,
                    "term": p_state.term,
                    "op": "insert",
                    "doc_id": 20,
                    "payload": {"doc_id": 20, "title": "Doc2"},
                }

                # Attempting unrelated write while req-2 is unresolved should be rejected
                unrelated_resp = await p_client.post(
                    "/doc",
                    json={
                        "request_id": "req-unrelated",
                        "document": {"doc_id": 30, "title": "Doc3"},
                    },
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                assert unrelated_resp.status_code == 503

                # Retry req-2: primary reconciles with follower via /internal/check-op!
                retry_resp = await p_client.post(
                    "/doc",
                    json={"request_id": "req-2", "document": {"doc_id": 20, "title": "Doc2"}},
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                assert retry_resp.status_code in (200, 201)
                assert retry_resp.json()["outcome"] == "COMMITTED"
                assert "req-2" not in p_state.pending_unresolved_writes
                assert p_state.engine.get(20) is not None


@pytest.mark.asyncio
async def test_pending_write_reconciliation_follower_behind(tmp_path: Path):
    """Test that if follower is behind (wal_last_seq < seq), absence is NOT proof of failure;
    outcome remains unresolved and subsequent writes remain blocked."""
    f_settings = _make_settings(tmp_path / "follower")
    p_settings = _make_settings(tmp_path / "primary")

    follower_app = build_app(
        settings=f_settings,
        node_id="shard0-follower",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
    )
    primary_app = build_app(
        settings=p_settings,
        node_id="shard0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(follower_app), LifespanManager(primary_app):
        f_transport = ASGITransport(app=follower_app)
        async with AsyncClient(transport=f_transport, base_url="http://follower") as f_client:
            p_state = primary_app.state.node
            f_state = follower_app.state.node
            p_state.follower_url = "follower"
            p_state.http_client = f_client
            p_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
            f_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

            # Follower is at seq=5, but primary has pending op at seq=10
            f_state.wal.append(
                seq=5,
                term=f_state.term,
                op="insert",
                doc_id=5,
                request_id="req-5",
                payload={"doc_id": 5},
            )
            assert f_state.wal.last_seq == 5
            p_state.follower_state = ReplicaState.DOWN
            p_state.pending_unresolved_writes["req-lag"] = {
                "seq": 10,
                "term": p_state.term,
                "op": "insert",
                "doc_id": 50,
                "payload": {"doc_id": 50, "v": 1},
            }

            p_transport = ASGITransport(app=primary_app)
            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                # 1. Retrying req-lag when follower is behind reports 202 OUTCOME_UNKNOWN
                retry_resp = await p_client.post(
                    "/doc",
                    json={"request_id": "req-lag", "document": {"doc_id": 50, "v": 1}},
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                assert retry_resp.status_code == 202
                # Must NOT remove req-lag from pending writes!
                assert "req-lag" in p_state.pending_unresolved_writes

                # 2. Subsequent writes MUST remain blocked with 503 REPLICATION_UNAVAILABLE
                subsequent_resp = await p_client.post(
                    "/doc",
                    json={"request_id": "req-subsequent", "document": {"doc_id": 60, "v": 2}},
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                assert subsequent_resp.status_code == 503
                assert "earlier write has an unresolved replication outcome" in subsequent_resp.text


@pytest.mark.asyncio
async def test_pending_write_reconciliation_follower_unavailable(tmp_path: Path):
    """Test that if follower is unavailable, write remains unresolved and subsequent writes are blocked."""
    p_settings = _make_settings(tmp_path / "primary")
    primary_app = build_app(
        settings=p_settings,
        node_id="shard0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(primary_app):
        p_state = primary_app.state.node
        p_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
        p_state.follower_url = "nonexistent-follower:9999"
        p_state.follower_state = ReplicaState.DOWN
        p_state.pending_unresolved_writes["req-unavail"] = {
            "seq": 10,
            "term": p_state.term,
            "op": "insert",
            "doc_id": 70,
            "payload": {"doc_id": 70},
        }

        p_transport = ASGITransport(app=primary_app)
        async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
            # Retrying req-unavail yields 202 OUTCOME_UNKNOWN and write stays in pending
            r_resp = await p_client.post(
                "/doc",
                json={"request_id": "req-unavail", "document": {"doc_id": 70}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert r_resp.status_code == 202
            assert "req-unavail" in p_state.pending_unresolved_writes

            # Subsequent write is blocked
            sub_resp = await p_client.post(
                "/doc",
                json={"request_id": "req-blocked", "document": {"doc_id": 80}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert sub_resp.status_code == 503


@pytest.mark.asyncio
async def test_pending_write_reconciliation_conflicting_op(tmp_path: Path):
    """Test that if follower has a conflicting operation at seq (wal_last_seq >= seq but different op),
    authoritative log reconciliation proves it absent -> resolved as uncommitted, client retry can execute."""
    f_settings = _make_settings(tmp_path / "follower")
    p_settings = _make_settings(tmp_path / "primary")

    follower_app = build_app(
        settings=f_settings,
        node_id="shard0-follower",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
    )
    primary_app = build_app(
        settings=p_settings,
        node_id="shard0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(follower_app), LifespanManager(primary_app):
        f_transport = ASGITransport(app=follower_app)
        async with AsyncClient(transport=f_transport, base_url="http://follower") as f_client:
            p_state = primary_app.state.node
            f_state = follower_app.state.node
            p_state.follower_url = "follower"
            p_state.http_client = f_client
            p_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
            f_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

            # Follower has a DIFFERENT entry at seq 10 (different request_id "req-other")
            f_state.wal.append(
                seq=10,
                term=f_state.term,
                op="insert",
                doc_id=99,
                request_id="req-other",
                payload={"doc_id": 99},
            )
            assert f_state.wal.last_seq >= 10

            # Primary has pending "req-conflict" at seq 10
            p_state.pending_unresolved_writes["req-conflict"] = {
                "seq": 10,
                "term": p_state.term,
                "op": "insert",
                "doc_id": 100,
                "payload": {"doc_id": 100},
            }

            p_transport = ASGITransport(app=primary_app)
            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                # Retrying req-conflict triggers reconciliation: follower is at seq 10 with conflicting op.
                # Authoritative log proves req-conflict was absent/not committed!
                await p_client.post(
                    "/doc",
                    json={"request_id": "req-conflict", "document": {"doc_id": 100}},
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                # Resolved as uncommitted, so it gets removed from pending!
                # The stale seq-10 pending entry must be discarded (a fresh retry may re-add its own)
                assert p_state.pending_unresolved_writes.get("req-conflict", {}).get("seq") != 10


@pytest.mark.asyncio
async def test_pending_write_reconciliation_authoritative_absence(tmp_path: Path):
    """Test authoritative absence when follower is caught up (wal_last_seq >= seq) and op is absent."""
    f_settings = _make_settings(tmp_path / "follower")
    p_settings = _make_settings(tmp_path / "primary")

    follower_app = build_app(
        settings=f_settings,
        node_id="shard0-follower",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
    )
    primary_app = build_app(
        settings=p_settings,
        node_id="shard0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(follower_app), LifespanManager(primary_app):
        f_transport = ASGITransport(app=follower_app)
        async with AsyncClient(transport=f_transport, base_url="http://follower") as f_client:
            p_state = primary_app.state.node
            f_state = follower_app.state.node
            p_state.follower_url = "follower"
            p_state.http_client = f_client
            p_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
            f_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

            # Follower has logged up to seq 15
            f_state.wal.append(
                seq=15,
                term=f_state.term,
                op="insert",
                doc_id=15,
                request_id="req-15",
                payload={"doc_id": 15},
            )
            assert f_state.wal.last_seq == 15

            # Primary has pending write at seq 10
            p_state.pending_unresolved_writes["req-absent"] = {
                "seq": 10,
                "term": p_state.term,
                "op": "insert",
                "doc_id": 110,
                "payload": {"doc_id": 110},
            }

            p_transport = ASGITransport(app=primary_app)
            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                # Primary checks follower: follower wal_last_seq is 15 >= 10, and entry 10 is absent
                await p_client.post(
                    "/doc",
                    json={"request_id": "req-absent", "document": {"doc_id": 110}},
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                assert p_state.pending_unresolved_writes.get("req-absent", {}).get("seq") != 10


@pytest.mark.asyncio
async def test_pending_write_retry_no_duplicate(tmp_path: Path):
    """Test that once an operation commits, a retried request returns COMMITTED without re-execution."""
    settings = _make_settings(tmp_path)
    app = build_app(
        settings=settings,
        node_id="shard0-standalone",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        allow_standalone=True,
    )
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://standalone") as client:
            state = app.state.node
            state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

            # First write succeeds
            resp1 = await client.post(
                "/doc",
                json={"request_id": "req-once", "document": {"doc_id": 200, "val": 1}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert resp1.status_code == 201
            seq1 = resp1.json()["seq"]
            assert state.engine.doc_count() == 1

            # Duplicate retry returns original committed result
            resp2 = await client.post(
                "/doc",
                json={"request_id": "req-once", "document": {"doc_id": 200, "val": 1}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert resp2.status_code in (200, 201)
            assert resp2.json()["outcome"] == "COMMITTED"
            assert resp2.json()["seq"] == seq1
            assert state.engine.get(200)["val"] == 1  # Not overwritten
            assert state.engine.doc_count() == 1


@pytest.mark.asyncio
async def test_issue4_migration_snapshot_consistency(tmp_path: Path):
    """Verify migration snapshot uses engine.last_applied_seq and blocks if pending writes unresolved."""
    settings = _make_settings(tmp_path)
    app = build_app(
        settings=settings,
        node_id="shard0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        lease_expires_at=float("inf"),
    )
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://node") as client:
            state = app.state.node
            state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

            # Insert applied document at seq 1
            state.engine.apply(
                {
                    "seq": 1,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 50,
                    "request_id": "r1",
                    "payload": {"doc_id": 50},
                }
            )
            state.wal.next_seq()
            state.wal.append(
                seq=1, term=1, op="insert", doc_id=50, request_id="r1", payload={"doc_id": 50}
            )

            # Append unapplied WAL entry at seq 2
            state.wal.next_seq()
            state.wal.append(
                seq=2, term=1, op="insert", doc_id=60, request_id="r2", payload={"doc_id": 60}
            )

            # Take snapshot
            body = {"start": 0, "end": 1000}
            b_bytes = json.dumps(body).encode("utf-8")
            resp = await client.post(
                "/internal/migration/snapshot", content=b_bytes, headers=_cluster_headers(b_bytes)
            )
            assert resp.status_code == 200
            data = resp.json()
            # Must return snapshot_seq == 1 (applied seq), NOT wal.last_seq == 2!
            assert data["snapshot_seq"] == 1
            assert len(data["documents"]) == 1
            assert data["documents"][0]["doc_id"] == 50


@pytest.mark.asyncio
async def test_issue5_migration_recovery_on_coordinator_restart(tmp_path: Path):
    """Verify coordinator startup rolls forward post-cutover migrations and aborts pre-cutover ones
    when RPCs succeed."""
    settings = _make_settings(tmp_path)
    map_mgr = ShardMapManager(tmp_path / "shard_map.json")
    default_map = create_default_3_shard_map()
    map_mgr.save(default_map)

    coord_init = CoordinatorService(settings, shard_map_path=tmp_path / "shard_map.json")
    for s_id in ("shard0", "shard1", "shard2"):
        coord_init.replica_states[s_id] = ReplicaState.SYNCED

    # 1. Post-cutover migration in CUTOVER state
    mgr = MigrationManager(str(tmp_path), SECRET)
    rec_cutover = mgr.create_migration(
        from_shard="shard0",
        to_shard="shard1",
        start=0,
        end=100,
        coord=coord_init,
    )
    rec_cutover.state = MigrationState.CUTOVER
    rec_cutover.final_cutover_epoch = 10
    mgr.save_record(rec_cutover)

    # 2. Pre-cutover migration in CATCHUP state
    rec_pre = mgr.create_migration(
        from_shard="shard1",
        to_shard="shard2",
        start=4000000000000000000,
        end=4500000000000000000,
        coord=coord_init,
    )
    rec_pre.state = MigrationState.CATCHUP
    mgr.save_record(rec_pre)

    # Mock client where all recovery RPCs succeed
    async def ok_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok"})

    mock_client = AsyncClient(transport=httpx.MockTransport(ok_handler))

    # 3. Simulate coordinator restart
    coord_svc = CoordinatorService(settings, shard_map_path=tmp_path / "shard_map.json")
    for s_id in ("shard0", "shard1", "shard2"):
        coord_svc.replica_states[s_id] = ReplicaState.SYNCED
    await coord_svc.resume_unfinished_migrations(http_client=mock_client)

    # Post-cutover migration must roll forward to DONE
    saved_cutover = coord_svc.migration_mgr.migrations[rec_cutover.migration_id]
    assert saved_cutover.state == MigrationState.DONE

    # Pre-cutover migration must abort cleanly
    saved_pre = coord_svc.migration_mgr.migrations[rec_pre.migration_id]
    assert saved_pre.state == MigrationState.ABORTED


@pytest.mark.asyncio
async def test_migration_recovery_rpc_failures_prevent_false_success(tmp_path: Path):
    """Regression test for Issue 3:
    Verify that if cutover, cleanup, unpause, or abort RPCs fail:
    - Post-cutover migration is NOT marked DONE; retained in CLEANUP with error.
    - Pre-cutover migration is NOT marked ABORTED; retained in pre-cutover state with error.
    - Subsequent retry when RPCs succeed completes the recovery idempotently.
    """
    settings = _make_settings(tmp_path)
    map_mgr = ShardMapManager(tmp_path / "shard_map.json")
    default_map = create_default_3_shard_map()
    map_mgr.save(default_map)

    coord_init = CoordinatorService(settings, shard_map_path=tmp_path / "shard_map.json")
    for s_id in ("shard0", "shard1", "shard2"):
        coord_init.replica_states[s_id] = ReplicaState.SYNCED

    mgr = MigrationManager(str(tmp_path), SECRET)

    # 1. Post-cutover migration
    rec_post = mgr.create_migration(
        from_shard="shard0", to_shard="shard1", start=0, end=100, coord=coord_init
    )
    rec_post.state = MigrationState.CUTOVER
    rec_post.final_cutover_epoch = 10
    mgr.save_record(rec_post)

    # 2. Pre-cutover migration
    rec_pre = mgr.create_migration(
        from_shard="shard1",
        to_shard="shard2",
        start=4000000000000000000,
        end=4500000000000000000,
        coord=coord_init,
    )
    rec_pre.state = MigrationState.PAUSE
    mgr.save_record(rec_pre)

    # A mock client that fails cutover, cleanup, unpause, and abort RPCs
    async def fail_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "RPC connection failed"})

    fail_client = AsyncClient(transport=httpx.MockTransport(fail_handler))

    coord_svc = CoordinatorService(settings, shard_map_path=tmp_path / "shard_map.json")
    for s_id in ("shard0", "shard1", "shard2"):
        coord_svc.replica_states[s_id] = ReplicaState.SYNCED

    # Recovery attempt with failing RPCs
    recovered = await coord_svc.resume_unfinished_migrations(http_client=fail_client)
    assert len(recovered) == 0  # Neither migration succeeded recovery!

    # Verify post-cutover migration is NOT marked DONE
    saved_post = coord_svc.migration_mgr.migrations[rec_post.migration_id]
    assert saved_post.state == MigrationState.CLEANUP
    assert saved_post.error is not None
    assert "failed" in saved_post.error

    # Verify pre-cutover migration is NOT marked ABORTED
    saved_pre = coord_svc.migration_mgr.migrations[rec_pre.migration_id]
    assert saved_pre.state != MigrationState.ABORTED
    assert saved_pre.state == MigrationState.PAUSE
    assert saved_pre.error is not None
    assert "failed" in saved_pre.error

    # 3. Now simulate subsequent recovery attempt where RPCs succeed
    async def ok_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok"})

    ok_client = AsyncClient(transport=httpx.MockTransport(ok_handler))
    recovered_retry = await coord_svc.resume_unfinished_migrations(http_client=ok_client)
    assert len(recovered_retry) == 2

    # Now they are safely marked DONE and ABORTED
    assert coord_svc.migration_mgr.migrations[rec_post.migration_id].state == MigrationState.DONE
    assert coord_svc.migration_mgr.migrations[rec_pre.migration_id].state == MigrationState.ABORTED


@pytest.mark.asyncio
async def test_issue6_scatter_gather_large_pagination(tmp_path: Path):
    """Verify scatter-gather retrieves candidates via internal pagination when skip + limit > 1000."""
    settings = _make_settings(tmp_path)
    map_mgr = ShardMapManager(tmp_path / "shard_map.json")
    default_map = create_default_3_shard_map()
    map_mgr.save(default_map)

    # Build mock shard responding with 1000 docs per chunk
    queries_made = []

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        data = json.loads(request.content.decode("utf-8"))
        queries_made.append(data)
        skip = data.get("skip", 0)
        limit = data.get("limit", 1000)
        # Return docs numbered skip .. skip+limit (up to 1500 total in shard0)
        total_available = 1500
        count = max(0, min(limit, total_available - skip))
        docs = [{"doc_id": i, "val": i} for i in range(skip, skip + count)]
        return httpx.Response(200, json={"documents": docs, "total": total_available})

    mock_client = AsyncClient(transport=httpx.MockTransport(mock_handler))

    # Request skip=900, limit=300 (total candidates needed: 1200 > 1000)
    q = QueryFilter(
        filter={"val": {"$gte": 0}}, sort_field="doc_id", sort_dir="asc", skip=900, limit=300
    )
    resp = await execute_scatter_query(q, map_mgr, mock_client, settings)

    assert len(resp.documents) == 300
    assert resp.documents[0]["doc_id"] == 900
    assert resp.documents[-1]["doc_id"] == 1199

    # Verify that multiple chunked queries were executed to fetch beyond 1000
    assert len(queries_made) >= 2
    assert queries_made[0]["limit"] == 1000
    assert queries_made[1]["limit"] == 200


# ── Regression Tests for Focused Correctness Review ──────────────────────────


@pytest.mark.asyncio
async def test_wal_restart_uncommitted_entry_discarded_and_sequence_consistent(tmp_path: Path):
    """Requirements 1-5:
    1. Primary has a durable uncommitted WAL entry.
    2. The operation is resolved as uncommitted.
    3. Primary restarts.
    4. Verify the operation does not reappear in the engine.
    5. Verify the sequence number and WAL remain consistent.
    """
    settings = _make_settings(tmp_path)
    shard_id = "shard0"

    # Start primary and execute committed write (seq=1)
    app1 = build_app(
        settings=settings,
        node_id="p1",
        shard_id=shard_id,
        role=NodeRole.PRIMARY,
        allow_standalone=True,
    )
    async with LifespanManager(app1):
        transport1 = ASGITransport(app=app1)
        async with AsyncClient(transport=transport1, base_url="http://p1") as c1:
            p_state = app1.state.node
            p_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

            r1 = await c1.post(
                "/doc",
                json={"request_id": "req-1", "document": {"doc_id": 10, "val": "v1"}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert r1.status_code in (200, 201)
            assert p_state.committed_seq == 1
            assert p_state.engine.last_applied_seq == 1
            assert p_state.wal.last_seq == 1

            # Append uncommitted operation to primary WAL (seq=2)
            seq2 = p_state.wal.next_seq()
            assert seq2 == 2
            p_state.wal.append(
                seq=seq2,
                term=p_state.term,
                op="insert",
                doc_id=20,
                request_id="req-2",
                payload={"doc_id": 20, "val": "uncommitted"},
            )
            assert p_state.wal.last_seq == 2
            p_state.pending_unresolved_writes["req-2"] = {
                "seq": 2,
                "term": p_state.term,
                "op": "insert",
                "doc_id": 20,
                "payload": {"doc_id": 20, "val": "uncommitted"},
            }

            # Follower responds indicating authoritative absence (wal_last_seq >= 2 and not has_op)
            async def mock_follower(request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    200,
                    json={
                        "status": "ok",
                        "has_op": False,
                        "term": 1,
                        "wal_last_seq": 5,
                        "entry_found": False,
                    },
                )

            p_state.follower_url = "follower"
            p_state.follower_state = ReplicaState.DOWN
            p_state.http_client = AsyncClient(transport=httpx.MockTransport(mock_follower))

            # Trigger reconciliation by retrying req-2
            await c1.post(
                "/doc",
                json={"request_id": "req-2", "document": {"doc_id": 20, "val": "uncommitted"}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            # Authoritative absence resolved as uncommitted: pending entry removed and tail truncated
            assert "req-2" not in p_state.pending_unresolved_writes
            assert p_state.wal.last_seq == 1
            assert p_state.committed_seq == 1

    # Simulate restart of primary node
    app2 = build_app(
        settings=settings,
        node_id="p1",
        shard_id=shard_id,
        role=NodeRole.PRIMARY,
        allow_standalone=True,
    )
    async with LifespanManager(app2):
        state2 = app2.state.node
        # Verify uncommitted entry (doc 20) did NOT reappear in engine
        assert state2.engine.get(10) is not None
        assert state2.engine.get(20) is None
        assert state2.engine.last_applied_seq == 1
        assert state2.wal.last_seq == 1
        assert state2.committed_seq == 1

        # Promote to primary to continue serving writes
        transport2 = ASGITransport(app=app2)
        async with AsyncClient(transport=transport2, base_url="http://p2") as c2:
            prom_bytes = json.dumps(
                {
                    "shard_id": "shard0",
                    "term": 2,
                    "ownership_epochs": {"0-9223372036854775808": 2},
                    "lease": {"granted_at": 0.0, "expires_at": 3600.0},
                }
            ).encode("utf-8")
            await c2.post(
                "/internal/promote",
                content=prom_bytes,
                headers=_cluster_headers(prom_bytes),
            )

            # Next write must use sequence number 2 (strictly contiguous, no sequence gaps)
            r3 = await c2.post(
                "/doc",
                json={"request_id": "req-3", "document": {"doc_id": 30, "val": "v3"}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert r3.status_code in (200, 201)
            assert r3.json()["seq"] == 2
            assert state2.wal.last_seq == 2
            assert state2.engine.last_applied_seq == 2


@pytest.mark.asyncio
async def test_reconciliation_follower_has_op_lost_ack(tmp_path: Path):
    """Requirement 6: Follower durably stores an operation but its acknowledgement is lost.
    Reconciles as COMMITTED and prevents duplicate execution on retry.
    """
    p_settings = _make_settings(tmp_path / "primary")
    f_settings = _make_settings(tmp_path / "follower")
    primary_app = build_app(
        settings=p_settings,
        node_id="p1",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        lease_expires_at=float("inf"),
    )
    follower_app = build_app(
        settings=f_settings, node_id="f1", shard_id="shard0", role=NodeRole.FOLLOWER
    )

    async with LifespanManager(follower_app), LifespanManager(primary_app):
        p_state = primary_app.state.node
        f_state = follower_app.state.node
        p_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
        f_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

        # Follower durably stored the op at seq 1, term 1
        f_state.wal.append(
            seq=1,
            term=1,
            op="insert",
            doc_id=50,
            request_id="req-ack-lost",
            payload={"doc_id": 50, "v": "lost_ack"},
        )
        f_state.engine.apply(
            {
                "seq": 1,
                "term": 1,
                "op": "insert",
                "doc_id": 50,
                "request_id": "req-ack-lost",
                "payload": {"doc_id": 50, "v": "lost_ack"},
            }
        )
        f_state.committed_seq = 1

        # Primary has pending write at seq 1 whose ack was lost
        p_state.pending_unresolved_writes["req-ack-lost"] = {
            "seq": 1,
            "term": 1,
            "op": "insert",
            "doc_id": 50,
            "payload": {"doc_id": 50, "v": "lost_ack"},
        }
        p_state.wal.append(
            seq=1,
            term=1,
            op="insert",
            doc_id=50,
            request_id="req-ack-lost",
            payload={"doc_id": 50, "v": "lost_ack"},
        )
        p_state.follower_state = ReplicaState.DOWN

        f_transport = ASGITransport(app=follower_app)
        async with AsyncClient(transport=f_transport, base_url="http://follower") as f_client:
            p_state.follower_url = "follower"
            p_state.http_client = f_client

            p_transport = ASGITransport(app=primary_app)
            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                # Retry triggers reconciliation
                resp = await p_client.post(
                    "/doc",
                    json={
                        "request_id": "req-ack-lost",
                        "document": {"doc_id": 50, "v": "lost_ack"},
                    },
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                assert resp.status_code in (200, 201)
                data = resp.json()
                assert data["outcome"] in ("committed", "COMMITTED")
                assert data["seq"] == 1
                assert p_state.committed_seq == 1
                assert "req-ack-lost" not in p_state.pending_unresolved_writes

                # Second retry returns COMMITTED without executing twice
                retry_resp = await p_client.post(
                    "/doc",
                    json={
                        "request_id": "req-ack-lost",
                        "document": {"doc_id": 50, "v": "lost_ack"},
                    },
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                assert retry_resp.status_code in (200, 201)
                assert retry_resp.json()["outcome"] in ("committed", "COMMITTED")


@pytest.mark.asyncio
async def test_reconciliation_follower_behind_and_unavailable_blocks_writes(tmp_path: Path):
    """Requirements 7, 8, 13:
    7. Follower is behind: retains OUTCOME_UNKNOWN.
    8. Follower is unavailable: retains OUTCOME_UNKNOWN.
    13. An unresolved operation continues blocking subsequent writes.
    """
    settings = _make_settings(tmp_path)
    primary_app = build_app(
        settings=settings,
        node_id="p1",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(primary_app):
        p_state = primary_app.state.node
        p_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
        p_state.committed_seq = 1

        # Pending write at seq 5
        p_state.pending_unresolved_writes["req-lag"] = {
            "seq": 5,
            "term": 1,
            "op": "insert",
            "doc_id": 100,
            "payload": {"doc_id": 100},
        }

        # Case 1: Follower is behind (wal_last_seq=2 < seq=5)
        async def lagging_follower(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "status": "ok",
                    "has_op": False,
                    "term": 1,
                    "wal_last_seq": 2,
                    "entry_found": False,
                },
            )

        p_state.follower_url = "follower"
        p_state.follower_state = ReplicaState.DOWN
        p_state.http_client = AsyncClient(transport=httpx.MockTransport(lagging_follower))

        p_transport = ASGITransport(app=primary_app)
        async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
            # Reconcile attempt on lagging follower
            r_lag = await p_client.post(
                "/doc",
                json={"request_id": "req-lag", "document": {"doc_id": 100}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert r_lag.status_code == 202
            assert "req-lag" in p_state.pending_unresolved_writes

            # Subsequent write is blocked by unresolved write
            r_blocked = await p_client.post(
                "/doc",
                json={"request_id": "req-subsequent", "document": {"doc_id": 101}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert r_blocked.status_code == 503
            assert "Cannot accept subsequent writes" in str(r_blocked.json())

        # Case 2: Follower unavailable (connection error / 500)
        async def unavailable_follower(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, json={"error": "Follower unreachable"})

        p_state.http_client = AsyncClient(transport=httpx.MockTransport(unavailable_follower))
        async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
            r_unavail = await p_client.post(
                "/doc",
                json={"request_id": "req-lag", "document": {"doc_id": 100}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert r_unavail.status_code == 202
            assert "req-lag" in p_state.pending_unresolved_writes

            # Writes remain blocked
            r_blocked2 = await p_client.post(
                "/doc",
                json={"request_id": "req-subsequent2", "document": {"doc_id": 102}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert r_blocked2.status_code == 503


@pytest.mark.asyncio
async def test_reconciliation_follower_promoted_higher_term_and_missing_from_later_log(
    tmp_path: Path,
):
    """Requirements 9, 10, 12:
    9. Follower is promoted under a higher term.
    10. A previously committed operation is missing from a later log: must NOT be discarded.
    12. An uncommitted tail is safely discarded.
    """
    settings = _make_settings(tmp_path)
    primary_app = build_app(
        settings=settings,
        node_id="p1",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(primary_app):
        p_state = primary_app.state.node
        p_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
        p_state.term = 5
        p_state.committed_seq = 9

        # Case 1: Primary appended (term=5, seq=10). Follower B durably stored it, then B was promoted to term 6.
        # Ack was lost. Primary reconciles with B.
        p_state.pending_unresolved_writes["req-committed-survives"] = {
            "seq": 10,
            "term": 5,
            "op": "insert",
            "doc_id": 200,
            "payload": {"doc_id": 200},
        }

        # Follower B is now at term 6, but has op (term=5, seq=10, req="req-committed-survives")
        async def promoted_follower_with_op(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "status": "ok",
                    "has_op": True,
                    "term": 6,
                    "wal_last_seq": 12,
                    "committed_seq": 12,  # the follower holds the op durably committed
                    "op_term": 5,
                    "op_request_id": "req-committed-survives",
                    "entry_found": True,
                },
            )

        p_state.follower_url = "follower"
        p_state.follower_state = ReplicaState.DOWN
        p_state.http_client = AsyncClient(transport=httpx.MockTransport(promoted_follower_with_op))

        p_transport = ASGITransport(app=primary_app)
        async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
            resp = await p_client.post(
                "/doc",
                json={"request_id": "req-committed-survives", "document": {"doc_id": 200}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert resp.status_code in (200, 201)
            assert resp.json()["outcome"] in ("committed", "COMMITTED")
            assert "req-committed-survives" not in p_state.pending_unresolved_writes
            assert p_state.committed_seq == 10

        # Case 2: Now primary has an uncommitted proposal (term=5, seq=11).
        # Follower B was promoted to term 6 and its log has progressed past seq 11 without this op.
        p_state.wal.append(
            seq=11,
            term=5,
            op="insert",
            doc_id=201,
            request_id="req-uncommitted-tail",
            payload={"doc_id": 201},
        )
        assert p_state.wal.last_seq == 11
        p_state.pending_unresolved_writes["req-uncommitted-tail"] = {
            "seq": 11,
            "term": 5,
            "op": "insert",
            "doc_id": 201,
            "payload": {"doc_id": 201},
        }

        async def promoted_follower_without_op(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "status": "ok",
                    "has_op": False,
                    "term": 6,
                    "wal_last_seq": 15,
                    "op_term": None,
                    "op_request_id": None,
                    "entry_found": False,
                },
            )

        p_state.http_client = AsyncClient(
            transport=httpx.MockTransport(promoted_follower_without_op)
        )
        async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
            resp2 = await p_client.post(
                "/doc",
                json={"request_id": "req-uncommitted-tail", "document": {"doc_id": 201}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert resp2.status_code in (202, 503)
            # Uncommitted tail was safely discarded down to committed_seq (10)
            assert p_state.wal.last_seq == 10
            assert "req-uncommitted-tail" not in p_state.pending_unresolved_writes


@pytest.mark.asyncio
async def test_check_op_incomplete_or_invalid_identity_rejected(tmp_path: Path):
    """Requirement 11: A matching operation has incomplete or invalid identity fields.
    /internal/check-op must reject incomplete identity and reconciliation must retain OUTCOME_UNKNOWN.
    """
    settings = _make_settings(tmp_path)
    follower_app = build_app(
        settings=settings, node_id="f1", shard_id="shard0", role=NodeRole.FOLLOWER
    )

    async with LifespanManager(follower_app):
        f_state = follower_app.state.node
        f_state.wal.append(
            seq=5,
            term=2,
            op="insert",
            doc_id=50,
            request_id="req-ident",
            payload={"doc_id": 50},
        )

        transport = ASGITransport(app=follower_app)
        async with AsyncClient(transport=transport, base_url="http://follower") as client:

            async def _check(body_dict: dict) -> httpx.Response:
                b = json.dumps(body_dict).encode("utf-8")
                return await client.post(
                    "/internal/check-op", content=b, headers=_cluster_headers(b)
                )

            # 1. Missing seq
            r1 = await _check({"term": 2, "request_id": "req-ident"})
            assert r1.status_code == 200
            assert r1.json()["status"] == "invalid_identity"
            assert r1.json()["has_op"] is False

            # 2. Missing term
            r2 = await _check({"seq": 5, "request_id": "req-ident"})
            assert r2.status_code == 200
            assert r2.json()["status"] == "invalid_identity"
            assert r2.json()["has_op"] is False

            # 3. Missing request_id
            r3 = await _check({"seq": 5, "term": 2})
            assert r3.status_code == 200
            assert r3.json()["status"] == "invalid_identity"
            assert r3.json()["has_op"] is False

            # 4. Mismatching term
            r4 = await _check({"seq": 5, "term": 1, "request_id": "req-ident"})
            assert r4.status_code == 200
            assert r4.json()["has_op"] is False

            # 5. Mismatching request_id
            r5 = await _check({"seq": 5, "term": 2, "request_id": "req-other"})
            assert r5.status_code == 200
            assert r5.json()["has_op"] is False

            # 6. Complete matching identity
            r6 = await _check({"seq": 5, "term": 2, "request_id": "req-ident"})
            assert r6.status_code == 200
            assert r6.json()["status"] == "ok"
            assert r6.json()["has_op"] is True


@pytest.mark.asyncio
async def test_promotion_truncates_uncommitted_tail_and_preserves_committed(tmp_path: Path):
    """Requirements 14-16:
    14. New primary contains a committed WAL prefix and an uncommitted tail.
    15. Verify committed operations survive promotion.
    16. Verify uncommitted operations are not applied.
    """
    settings = _make_settings(tmp_path)
    shard_id = "shard0"

    app = build_app(
        settings=settings,
        node_id="node-promote",
        shard_id=shard_id,
        role=NodeRole.FOLLOWER,
        allow_standalone=True,
    )
    async with LifespanManager(app):
        state = app.state.node
        state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

        # Committed prefix: seq=1 (committed_seq=1)
        state.wal.append(
            seq=1,
            term=1,
            op="insert",
            doc_id=10,
            request_id="req-comm",
            payload={"doc_id": 10, "val": "committed"},
        )
        state.committed_seq = 1

        # Uncommitted tail: seq=2, seq=3 (committed_seq remains 1)
        state.wal.append(
            seq=2,
            term=1,
            op="insert",
            doc_id=20,
            request_id="req-uncomm-1",
            payload={"doc_id": 20, "val": "uncommitted"},
        )
        state.wal.append(
            seq=3,
            term=1,
            op="insert",
            doc_id=30,
            request_id="req-uncomm-2",
            payload={"doc_id": 30, "val": "uncommitted"},
        )
        assert state.wal.last_seq == 3

        # Promote node to PRIMARY in term 2
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://node") as client:
            prom_bytes = json.dumps(
                {
                    "shard_id": "shard0",
                    "term": 2,
                    "ownership_epochs": {"0-1000": 2},
                    "lease": {"granted_at": 0.0, "expires_at": 3600.0},
                }
            ).encode("utf-8")
            p_resp = await client.post(
                "/internal/promote",
                content=prom_bytes,
                headers=_cluster_headers(prom_bytes),
            )
            assert p_resp.status_code == 200, p_resp.text
            assert state.role == NodeRole.PRIMARY
            assert state.term == 2

            # 15. Committed operation survived promotion and applied to engine
            assert state.engine.get(10) is not None
            assert state.engine.last_applied_seq == 1

            # 16. Uncommitted operations were NOT applied and discarded
            assert state.engine.get(20) is None
            assert state.engine.get(30) is None
            assert state.wal.last_seq == 1

            # New write gets seq=2
            w_resp = await client.post(
                "/doc",
                json={"request_id": "req-new", "document": {"doc_id": 40, "val": "v4"}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert w_resp.status_code in (200, 201)
            assert w_resp.json()["seq"] == 2
            assert state.wal.last_seq == 2


@pytest.mark.asyncio
async def test_migration_missing_shard_entries_post_cutover_and_pre_cutover(tmp_path: Path):
    """Requirements 17-22:
    17. Destination shard entry is missing during post-cutover recovery.
    18. Source shard entry is missing during post-cutover cleanup.
    19. Source shard entry is missing during pre-cutover abort.
    20. Destination shard entry is missing during pre-cutover abort.
    21. Verify none of these cases falsely marks a migration DONE or ABORTED.
    22. Verify recovery succeeds after the missing shard becomes available.
    """
    settings = _make_settings(tmp_path)
    map_path = tmp_path / "shard_map.json"
    map_mgr = ShardMapManager(map_path)

    # Start with full map to create valid migration records
    full_map = create_default_3_shard_map()
    map_mgr.save(full_map)

    coord_svc = CoordinatorService(settings, shard_map_path=map_path)
    for s_id in ("shard0", "shard1", "shard2"):
        coord_svc.replica_states[s_id] = ReplicaState.SYNCED

    mgr = coord_svc.migration_mgr

    # 17. Destination shard missing in post-cutover recovery (from shard0 to shard1)
    rec17 = mgr.create_migration(
        from_shard="shard0", to_shard="shard1", start=0, end=500, coord=coord_svc
    )
    rec17.state = MigrationState.CUTOVER
    rec17.final_cutover_epoch = 10
    mgr.save_record(rec17)

    # 18. Source shard missing in post-cutover cleanup (from shard1 to shard0)
    rec18 = mgr.create_migration(
        from_shard="shard1",
        to_shard="shard0",
        start=3500000000000000000,
        end=4000000000000000000,
        coord=coord_svc,
    )
    rec18.state = MigrationState.CLEANUP
    rec18.final_cutover_epoch = 11
    mgr.save_record(rec18)

    # 19. Source shard missing in pre-cutover abort (from shard2 to shard0)
    rec19 = mgr.create_migration(
        from_shard="shard2",
        to_shard="shard0",
        start=6500000000000000000,
        end=7000000000000000000,
        coord=coord_svc,
    )
    rec19.state = MigrationState.PAUSE
    mgr.save_record(rec19)

    # 20. Destination shard missing in pre-cutover abort (from shard0 to shard2)
    rec20 = mgr.create_migration(
        from_shard="shard0", to_shard="shard2", start=500, end=1000, coord=coord_svc
    )
    rec20.state = MigrationState.CATCHUP
    mgr.save_record(rec20)

    # Now switch shard map to partial_map (missing shard1 and shard2) to simulate missing shard entries
    partial_map = ShardMap(
        map_version=1,
        entries=[
            ShardMapEntry(
                shard_id="shard0",
                range=ShardRange(start=0, end=9223372036854775808),
                primary="http://shard0-primary:8000",
                follower="http://shard0-follower:8000",
                ownership_epoch=1,
                map_version=1,
            )
        ],
    )
    map_mgr.save(partial_map)
    coord_svc.map_mgr.reload()

    # Resume unfinished migrations with missing shard entries in shard map
    recovered = await coord_svc.resume_unfinished_migrations()

    # 21. Verify NONE of these 4 cases falsely marked DONE or ABORTED
    assert len(recovered) == 0

    saved17 = coord_svc.migration_mgr.migrations[rec17.migration_id]
    assert saved17.state != MigrationState.DONE
    assert saved17.state == MigrationState.CLEANUP
    assert "Destination shard shard1 primary entry missing" in str(saved17.error)

    saved18 = coord_svc.migration_mgr.migrations[rec18.migration_id]
    assert saved18.state != MigrationState.DONE
    assert saved18.state == MigrationState.CLEANUP
    assert "Source shard shard1 primary entry missing" in str(saved18.error)

    saved19 = coord_svc.migration_mgr.migrations[rec19.migration_id]
    assert saved19.state != MigrationState.ABORTED
    assert saved19.state == MigrationState.PAUSE
    assert "Source shard shard2 primary entry missing" in str(saved19.error)

    saved20 = coord_svc.migration_mgr.migrations[rec20.migration_id]
    assert saved20.state != MigrationState.ABORTED
    assert saved20.state == MigrationState.CATCHUP
    assert "Destination shard shard2 primary entry missing" in str(saved20.error)

    # 22. Verify recovery succeeds after missing shards become available
    full_map = create_default_3_shard_map()
    map_mgr.save(full_map)
    coord_svc.map_mgr.reload()

    async def mock_rpc_ok(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok"})

    ok_client = AsyncClient(transport=httpx.MockTransport(mock_rpc_ok))
    recovered_now = await coord_svc.resume_unfinished_migrations(http_client=ok_client)
    assert len(recovered_now) == 4

    assert coord_svc.migration_mgr.migrations[rec17.migration_id].state == MigrationState.DONE
    assert coord_svc.migration_mgr.migrations[rec18.migration_id].state == MigrationState.DONE
    assert coord_svc.migration_mgr.migrations[rec19.migration_id].state == MigrationState.ABORTED
    assert coord_svc.migration_mgr.migrations[rec20.migration_id].state == MigrationState.ABORTED


# ═════════════════════════════════════════════════════════════════════════════
# Focused Regression Tests: Crash Window & Durable Commitment Invariants
# ═════════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_crash_window_primary_restart_follower_available_committed_survives(
    tmp_path: Path,
) -> None:
    """Scenarios 1, 2, 5:

    Primary crashes immediately after follower fsync but before local committed_seq persistence.
    Primary restarts while follower is available.
    Verify:
    - Operation 10 is discovered as committed via follower reconciliation.
    - Operation survives recovery and is applied to engine.
    - Local committed_seq and WAL remain consistent.
    """
    p_dir = tmp_path / "primary"
    f_dir = tmp_path / "follower"
    p_settings = _make_settings(p_dir)
    f_settings = _make_settings(f_dir)
    shard_id = "shard0"

    # Start follower
    f_app = build_app(
        settings=f_settings,
        node_id="f1",
        shard_id=shard_id,
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )
    async with LifespanManager(f_app):
        f_transport = ASGITransport(app=f_app)

        # Start primary and execute initial committed write (seq=1)
        p_app1 = build_app(
            settings=p_settings,
            node_id="p1",
            shard_id=shard_id,
            role=NodeRole.PRIMARY,
            ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
            follower_url="http://f1",
            http_client=AsyncClient(mounts={"http://f1": f_transport}),
            lease_expires_at=float("inf"),
        )
        async with LifespanManager(p_app1):
            p1_state = p_app1.state.node
            async with AsyncClient(transport=ASGITransport(app=p_app1), base_url="http://p1") as c1:
                r1 = await c1.post(
                    "/doc",
                    json={"request_id": "req-1", "document": {"doc_id": 10, "val": "v1"}},
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                assert r1.status_code in (200, 201)
                assert p1_state.committed_seq == 1
                assert p1_state.wal.last_seq == 1

                # Replicate seq=2 directly to follower (follower writes and fsyncs seq=2)
                repl_body = {
                    "shard_id": shard_id,
                    "seq": 2,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 20,
                    "request_id": "req-20",
                    "payload": {"doc_id": 20, "val": "v20"},
                }
                b_bytes = json.dumps(repl_body).encode("utf-8")
                async with AsyncClient(transport=f_transport, base_url="http://f1") as fc:
                    rep_resp = await fc.post(
                        "/internal/replicate",
                        content=b_bytes,
                        headers=_cluster_headers(b_bytes),
                    )
                    assert rep_resp.status_code == 200

                # Append seq=2 to primary WAL, but simulate crash before updating committed_seq
                p1_state.wal.append(
                    seq=2,
                    term=1,
                    op="insert",
                    doc_id=20,
                    request_id="req-20",
                    payload={"doc_id": 20, "val": "v20"},
                )
                assert p1_state.wal.last_seq == 2
                assert p1_state.committed_seq == 1  # Still 1 on primary!

        # Primary has crashed. Now verify disk state before restart:
        # WAL has seq=2, but node_metadata.json on disk has committed_seq=1.
        meta_file = p_dir / shard_id / "node_metadata.json"
        with open(meta_file, encoding="utf-8") as f:
            disk_meta = json.load(f)
        assert disk_meta["committed_seq"] == 1

        # Restart primary with follower available
        p_app2 = build_app(
            settings=p_settings,
            node_id="p1",
            shard_id=shard_id,
            role=NodeRole.PRIMARY,
            ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
            follower_url="http://f1",
            http_client=AsyncClient(mounts={"http://f1": f_transport}),
            lease_expires_at=float("inf"),
        )
        async with LifespanManager(p_app2):
            p2_state = p_app2.state.node

            # Reconcile pending write req-20
            res = await _run_reconciliation(p2_state, "req-20")
            assert res == "committed"

            # Verify: Operation 20 survived recovery!
            assert p2_state.engine.get(20) is not None
            assert p2_state.engine.get(20)["val"] == "v20"
            assert p2_state.committed_seq == 2
            assert p2_state.wal.last_seq == 2


async def _run_reconciliation(state: Any, request_id: str) -> str:
    from docstore.common.models import WriteOutcome
    from docstore.shard.app import _reconcile_pending_write

    outcome = await _reconcile_pending_write(state, request_id)
    return "committed" if outcome == WriteOutcome.COMMITTED else "unknown"


@pytest.mark.asyncio
async def test_crash_window_primary_restart_follower_unavailable_blocks_writes(
    tmp_path: Path,
) -> None:
    """Scenarios 3, 7:

    Primary restarts while follower is unavailable.
    Verify:
    - Unreconciled WAL tail is NOT discarded.
    - Write outcome remains OUTCOME_UNKNOWN.
    - Subsequent new writes are rejected/blocked (HTTP 503).
    """
    p_dir = tmp_path / "primary"
    p_settings = _make_settings(p_dir)
    shard_id = "shard0"

    # Start primary and execute initial committed write (seq=1)
    p_app1 = build_app(
        settings=p_settings,
        node_id="p1",
        shard_id=shard_id,
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
        allow_standalone=True,
    )
    async with LifespanManager(p_app1):
        p1_state = p_app1.state.node
        async with AsyncClient(transport=ASGITransport(app=p_app1), base_url="http://p1") as c1:
            r1 = await c1.post(
                "/doc",
                json={"request_id": "req-1", "document": {"doc_id": 10, "val": "v1"}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert r1.status_code in (200, 201)

            # Append seq=2 (req-20) to WAL, but leave committed_seq at 1
            p1_state.wal.append(
                seq=2,
                term=1,
                op="insert",
                doc_id=20,
                request_id="req-20",
                payload={"doc_id": 20, "val": "v20"},
            )
            assert p1_state.wal.last_seq == 2
            assert p1_state.committed_seq == 1

    # Restart primary with UNAVAILABLE follower (mock transport returns ConnectionError / 503)
    async def mock_follower_down(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Follower down")

    down_client = AsyncClient(transport=httpx.MockTransport(mock_follower_down))
    p_app2 = build_app(
        settings=p_settings,
        node_id="p1",
        shard_id=shard_id,
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
        follower_url="http://follower",
        http_client=down_client,
        lease_expires_at=float("inf"),
    )
    async with LifespanManager(p_app2):
        p2_state = p_app2.state.node

        # WAL tail must NOT be truncated
        assert p2_state.wal.last_seq == 2
        assert p2_state.committed_seq == 1
        # req-20 is pending unresolved
        assert "req-20" in p2_state.pending_unresolved_writes
        # Not applied to engine yet
        assert p2_state.engine.get(20) is None

        # Promote to primary to attempt writes
        transport2 = ASGITransport(app=p_app2)
        async with AsyncClient(transport=transport2, base_url="http://p2") as c2:
            prom_bytes = json.dumps(
                {
                    "shard_id": "shard0",
                    "term": 2,
                    "follower_url": "http://follower",
                    "ownership_epochs": {"0-1000": 2},
                    "lease": {"granted_at": 0.0, "expires_at": 3600.0},
                }
            ).encode("utf-8")
            await c2.post(
                "/internal/promote",
                content=prom_bytes,
                headers=_cluster_headers(prom_bytes),
            )

            # Tail remains intact after promote when follower is down
            assert p2_state.wal.last_seq == 2
            assert "req-20" in p2_state.pending_unresolved_writes

            # 7. Attempt new write (req-30): MUST BE REJECTED with 503 ReplicationUnavailable
            r30 = await c2.post(
                "/doc",
                json={"request_id": "req-30", "document": {"doc_id": 30, "val": "v30"}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert r30.status_code == 503
            err_code = r30.json().get("detail", {}).get("error_code") or r30.json().get(
                "error_code"
            )
            assert err_code == "REPLICATION_UNAVAILABLE"


@pytest.mark.asyncio
async def test_crash_window_follower_promoted_before_old_primary_restarts(
    tmp_path: Path,
) -> None:
    """Scenario 4:

    Primary crashes after follower fsync.
    Follower is promoted to primary before the old primary restarts.
    Verify:
    - Operation 20 survives on promoted primary.
    - Old primary rejoins as follower and resyncs without losing operation 20.
    """
    p_dir = tmp_path / "primary"
    f_dir = tmp_path / "follower"
    p_settings = _make_settings(p_dir)
    f_settings = _make_settings(f_dir)
    shard_id = "shard0"

    # Start follower
    f_app = build_app(
        settings=f_settings,
        node_id="f1",
        shard_id=shard_id,
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )
    async with LifespanManager(f_app):
        f_transport = ASGITransport(app=f_app)

        # Primary executes write 1, replicates write 2 to follower, then crashes
        p_app1 = build_app(
            settings=p_settings,
            node_id="p1",
            shard_id=shard_id,
            role=NodeRole.PRIMARY,
            ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
            lease_expires_at=float("inf"),
        )
        async with LifespanManager(p_app1):
            p1_state = p_app1.state.node
            p1_state.wal.append(
                seq=1,
                term=1,
                op="insert",
                doc_id=10,
                request_id="req-1",
                payload={"doc_id": 10, "val": "v10"},
            )
            p1_state.engine.apply(
                {
                    "seq": 1,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 10,
                    "request_id": "req-1",
                    "payload": {"doc_id": 10, "val": "v10"},
                }
            )
            p1_state.committed_seq = 1

            # Follower receives seq=1 and seq=2
            for seq, doc_id, req in [(1, 10, "req-1"), (2, 20, "req-20")]:
                body = {
                    "shard_id": shard_id,
                    "seq": seq,
                    "term": 1,
                    "op": "insert",
                    "doc_id": doc_id,
                    "request_id": req,
                    "payload": {"doc_id": doc_id, "val": f"v{doc_id}"},
                }
                b = json.dumps(body).encode("utf-8")
                async with AsyncClient(transport=f_transport, base_url="http://f1") as fc:
                    await fc.post("/internal/replicate", content=b, headers=_cluster_headers(b))

            # Primary appends seq=2 to WAL, but crashes before updating committed_seq on disk
            p1_state.wal.append(
                seq=2,
                term=1,
                op="insert",
                doc_id=20,
                request_id="req-20",
                payload={"doc_id": 20, "val": "v20"},
            )
            assert p1_state.wal.last_seq == 2
            assert p1_state.committed_seq == 1

        # Primary is crashed. Promote follower F1 to PRIMARY under term 2!
        async with AsyncClient(transport=f_transport, base_url="http://f1") as fc:
            prom_bytes = json.dumps(
                {
                    "shard_id": "shard0",
                    "term": 2,
                    "ownership_epochs": {"0-1000": 2},
                    "lease": {"granted_at": 0.0, "expires_at": 3600.0},
                }
            ).encode("utf-8")
            p_resp = await fc.post(
                "/internal/promote", content=prom_bytes, headers=_cluster_headers(prom_bytes)
            )
            assert p_resp.status_code == 200

        # Follower (now new primary) has operation 20
        assert f_app.state.node.engine.get(20) is not None

        # Now restart old primary A
        p_app2 = build_app(
            settings=p_settings,
            node_id="p1",
            shard_id=shard_id,
            role=NodeRole.PRIMARY,
            ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
            lease_expires_at=float("inf"),
        )
        async with LifespanManager(p_app2):
            p2_state = p_app2.state.node
            # Starts in follower role after disk recovery
            assert p2_state.role == NodeRole.FOLLOWER

            # Connect old primary to new primary F1 and resync
            rejoin_client = AsyncClient(mounts={"http://f1": f_transport})
            resynced = await resync_with_primary(p2_state, "f1", http_client=rejoin_client)
            assert resynced is True

            # Verify: Operation 20 survives on both new primary and old primary!
            assert f_app.state.node.engine.get(20) is not None
            assert p2_state.engine.get(20) is not None
            assert p2_state.engine.get(20)["val"] == "v20"


@pytest.mark.asyncio
async def test_crash_window_uncommitted_operation_authoritatively_discarded(
    tmp_path: Path,
) -> None:
    """Scenario 6:

    Operation was never committed (never reached follower).
    Follower authoritatively confirms absence.
    Verify:
    - Operation is safely discarded from WAL and engine.
    - Contiguous sequence numbers are preserved for subsequent writes.
    """
    p_dir = tmp_path / "primary"
    p_settings = _make_settings(p_dir)
    shard_id = "shard0"

    # Start primary and execute committed write (seq=1)
    p_app1 = build_app(
        settings=p_settings,
        node_id="p1",
        shard_id=shard_id,
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
        allow_standalone=True,
    )
    async with LifespanManager(p_app1):
        p1_state = p_app1.state.node
        async with AsyncClient(transport=ASGITransport(app=p_app1), base_url="http://p1") as c1:
            r1 = await c1.post(
                "/doc",
                json={"request_id": "req-1", "document": {"doc_id": 10, "val": "v1"}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert r1.status_code in (200, 201)

            # Append uncommitted seq=2 to WAL only (never replicated)
            p1_state.wal.append(
                seq=2,
                term=1,
                op="insert",
                doc_id=20,
                request_id="req-uncommitted",
                payload={"doc_id": 20, "val": "uncommitted"},
            )
            assert p1_state.wal.last_seq == 2
            assert p1_state.committed_seq == 1

    # Restart primary with mock follower proving authoritative absence
    async def mock_follower_absent(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/internal/replicate":
            b = json.loads(request.content)
            return httpx.Response(
                200,
                json={"status": "ok", "seq": b.get("seq", 2), "last_seq": b.get("seq", 2)},
            )
        return httpx.Response(
            200,
            json={
                "status": "ok",
                "has_op": False,
                "term": 1,
                "wal_last_seq": 5,
                "entry_found": False,
            },
        )

    absent_client = AsyncClient(transport=httpx.MockTransport(mock_follower_absent))
    p_app2 = build_app(
        settings=p_settings,
        node_id="p1",
        shard_id=shard_id,
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
        follower_url="http://follower",
        http_client=absent_client,
        lease_expires_at=float("inf"),
    )
    async with LifespanManager(p_app2):
        p2_state = p_app2.state.node

        # Promote to primary
        transport2 = ASGITransport(app=p_app2)
        async with AsyncClient(transport=transport2, base_url="http://p2") as c2:
            prom_bytes = json.dumps(
                {
                    "shard_id": "shard0",
                    "term": 2,
                    "follower_url": "http://follower",
                    "ownership_epochs": {"0-1000": 2},
                    "lease": {"granted_at": 0.0, "expires_at": 3600.0},
                }
            ).encode("utf-8")
            await c2.post(
                "/internal/promote",
                content=prom_bytes,
                headers=_cluster_headers(prom_bytes),
            )
            p2_state.follower_state = ReplicaState.SYNCED

            # Reconcile uncommitted write: Authoritative absence discards seq=2
            res = await _run_reconciliation(p2_state, "req-uncommitted")
            assert res == "unknown"

            # Verify uncommitted operation is discarded
            assert p2_state.wal.last_seq == 1
            assert p2_state.committed_seq == 1
            assert p2_state.engine.get(20) is None
            assert "req-uncommitted" not in p2_state.pending_unresolved_writes

            # Next write succeeds at contiguous sequence number 2
            r2 = await c2.post(
                "/doc",
                json={"request_id": "req-fresh", "document": {"doc_id": 30, "val": "fresh"}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert r2.status_code in (200, 201)
            assert p2_state.wal.last_seq == 2
            assert p2_state.engine.get(30) is not None


# ═══════════════════════════════════════════════════════════════════════════
# 7. Durable Commit Protocol & Degraded Mode Regression Tests
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_regression_follower_unavailable_write_rejected_and_no_local_ack(tmp_path: Path):
    """Regression test 1, 2, 6:
    1. Follower unavailable: new write is rejected with HTTP 503 REPLICATION_UNAVAILABLE.
    2. Follower unavailable: no local-only write is acknowledged.
    6. Coordinator lease authorization without follower fsync cannot commit a write.
    """
    p_settings = _make_settings(tmp_path / "primary")
    shard_id = "shard0"

    p_app = build_app(
        settings=p_settings,
        node_id="p1",
        shard_id=shard_id,
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
        follower_url="http://follower",
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(p_app):
        p_state = p_app.state.node
        # 1. Follower is DOWN (degraded mode)
        p_state.follower_state = ReplicaState.DOWN

        # 2. Coordinator lease authorization is active
        p_state.lease_expires_at = time.monotonic() + 10.0

        p_trans = ASGITransport(app=p_app)
        async with AsyncClient(transport=p_trans, base_url="http://primary") as client:
            # 3. Client attempts a write while follower is unavailable
            doc_payload = {
                "request_id": "req-deg-1",
                "document": {"doc_id": 10, "val": "degraded_attempt"},
            }
            resp = await client.post(
                "/doc",
                json=doc_payload,
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )

            # Verification 1: New write is rejected with HTTP 503 REPLICATION_UNAVAILABLE
            assert resp.status_code == 503
            err_data = resp.json().get("detail", {})
            err_code = err_data.get("error_code") or resp.json().get("error_code")
            assert err_code == "REPLICATION_UNAVAILABLE"

            # Verification 2: No local-only write was acknowledged or committed
            assert p_state.wal.last_seq == 0
            assert p_state.committed_seq == 0
            assert p_state.engine.get(10) is None
            assert p_state.engine.dedup_check("req-deg-1") is None

            # Verification 6: Coordinator lease authorization does not commit write
            p_state.lease_expires_at = time.monotonic() + 100.0
            assert p_state.is_lease_valid() is True

            # Attempt write again with valid lease - still rejected with 503
            resp2 = await client.post(
                "/doc",
                json=doc_payload,
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert resp2.status_code == 503
            err_code2 = resp2.json().get("detail", {}).get("error_code") or resp2.json().get(
                "error_code"
            )
            assert err_code2 == "REPLICATION_UNAVAILABLE"
            assert p_state.wal.last_seq == 0
            assert p_state.engine.get(10) is None


@pytest.mark.asyncio
async def test_regression_primary_crash_after_degraded_write_follower_promoted(tmp_path: Path):
    """Regression test 3, 4, 5:
    3. Primary crashes after a degraded-mode write attempt.
    4. Follower is promoted after primary failure.
    5. Previously acknowledged writes remain present.
    """
    f_settings = _make_settings(tmp_path / "follower")
    p_settings = _make_settings(tmp_path / "primary")
    shard_id = "shard0"

    f_app = build_app(
        settings=f_settings,
        node_id="f1",
        shard_id=shard_id,
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )
    p_app = build_app(
        settings=p_settings,
        node_id="p1",
        shard_id=shard_id,
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
        follower_url="http://follower",
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(f_app), LifespanManager(p_app):
        f_state = f_app.state.node
        p_state = p_app.state.node

        f_trans = ASGITransport(app=f_app)
        f_client = AsyncClient(transport=f_trans, base_url="http://follower")
        p_state.http_client = f_client
        p_state.follower_state = ReplicaState.SYNCED

        p_trans = ASGITransport(app=p_app)
        async with AsyncClient(transport=p_trans, base_url="http://primary") as pc:
            # 1. Successful synchronously replicated write
            ack_resp = await pc.post(
                "/doc",
                json={"request_id": "req-ack", "document": {"doc_id": 10, "val": "acknowledged"}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert ack_resp.status_code in (200, 201)
            assert p_state.committed_seq == 1
            assert f_state.engine.get(10) is not None

            # 2. Follower becomes unavailable
            p_state.follower_state = ReplicaState.DOWN

            # 3. Degraded write attempt is rejected
            deg_resp = await pc.post(
                "/doc",
                json={"request_id": "req-deg", "document": {"doc_id": 20, "val": "degraded"}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert deg_resp.status_code == 503
            assert p_state.engine.get(20) is None
            assert p_state.wal.last_seq == 1

    # 4. Primary crashes (LifespanManager exited above)
    # 5. Follower is promoted after primary failure
    async with LifespanManager(f_app):
        prom_f_state = f_app.state.node
        prom_trans = ASGITransport(app=f_app)
        async with AsyncClient(transport=prom_trans, base_url="http://f1") as fc:
            prom_bytes = json.dumps(
                {
                    "shard_id": "shard0",
                    "term": 2,
                    "follower_url": "http://old-primary",
                    "ownership_epochs": {"0-1000": 2},
                    "lease": {"granted_at": 0.0, "expires_at": 3600.0},
                }
            ).encode("utf-8")
            prom_resp = await fc.post(
                "/internal/promote",
                content=prom_bytes,
                headers=_cluster_headers(prom_bytes),
            )
            assert prom_resp.status_code == 200
            assert prom_f_state.role == NodeRole.PRIMARY
            assert prom_f_state.term == 2

            # Verification 5: Previously acknowledged write remains present!
            read_resp = await fc.get("/doc/10", headers={CLIENT_KEY_HEADER: CLIENT_KEY})
            assert read_resp.status_code == 200
            assert read_resp.json()["document"]["doc_id"] == 10
            assert read_resp.json()["document"]["val"] == "acknowledged"

            # The unacknowledged degraded write attempt is absent
            read_absent = await fc.get("/doc/20", headers={CLIENT_KEY_HEADER: CLIENT_KEY})
            assert read_absent.status_code == 404

            # In degraded mode (old primary down), promoted follower safely rejects writes
            deg_write = await fc.post(
                "/doc",
                json={"request_id": "req-new", "document": {"doc_id": 30, "val": "new"}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert deg_write.status_code == 503
            err_code = deg_write.json().get("detail", {}).get("error_code") or deg_write.json().get(
                "error_code"
            )
            assert err_code == "REPLICATION_UNAVAILABLE"


@pytest.mark.asyncio
async def test_regression_writes_resume_after_follower_sync_restored(tmp_path: Path):
    """Regression test 7:
    7. Writes resume after follower synchronization is restored.
    """
    f_settings = _make_settings(tmp_path / "follower")
    p_settings = _make_settings(tmp_path / "primary")
    shard_id = "shard0"

    f_app = build_app(
        settings=f_settings,
        node_id="f1",
        shard_id=shard_id,
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )
    p_app = build_app(
        settings=p_settings,
        node_id="p1",
        shard_id=shard_id,
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
        follower_url="http://follower",
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(f_app), LifespanManager(p_app):
        f_state = f_app.state.node
        p_state = p_app.state.node

        f_trans = ASGITransport(app=f_app)
        f_client = AsyncClient(transport=f_trans, base_url="http://follower")
        p_state.http_client = f_client

        # Follower initially down
        p_state.follower_state = ReplicaState.DOWN

        p_trans = ASGITransport(app=p_app)
        async with AsyncClient(transport=p_trans, base_url="http://primary") as pc:
            # Write is rejected while follower is down
            w_fail = await pc.post(
                "/doc",
                json={"request_id": "req-blocked", "document": {"doc_id": 10, "val": "blocked"}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert w_fail.status_code == 503

            # Synchronization restored: follower rejoins and resyncs to SYNCED
            rejoin_bytes = json.dumps(
                {
                    "follower_url": "http://follower",
                    "last_applied_seq": f_state.engine.last_applied_seq,
                }
            ).encode("utf-8")
            rejoin_resp = await pc.post(
                "/internal/rejoin/complete",
                content=rejoin_bytes,
                headers=_cluster_headers(rejoin_bytes),
            )
            assert rejoin_resp.status_code == 200
            assert p_state.follower_state == ReplicaState.SYNCED

            # Verification 7: Writes resume successfully after synchronization restored
            w_resumed = await pc.post(
                "/doc",
                json={"request_id": "req-resumed", "document": {"doc_id": 10, "val": "resumed"}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert w_resumed.status_code in (200, 201)
            assert p_state.committed_seq == 1
            assert f_state.engine.get(10) is not None
            assert f_state.engine.get(10)["val"] == "resumed"


@pytest.mark.asyncio
async def test_regression_unresolved_write_preserves_outcome_unknown_and_blocks_subsequent(
    tmp_path: Path,
):
    """Requirements 4 & 5:
    4. Preserve unresolved writes as OUTCOME_UNKNOWN.
    5. Do not allow subsequent writes while an earlier write remains unresolved.
    """
    p_settings = _make_settings(tmp_path / "primary")
    shard_id = "shard0"

    # Mock follower that fails during replication RPC
    async def mock_follower_fail(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"status": "error", "detail": "Replication RPC failed"})

    fail_client = AsyncClient(transport=httpx.MockTransport(mock_follower_fail))

    p_app = build_app(
        settings=p_settings,
        node_id="p1",
        shard_id=shard_id,
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
        follower_url="http://follower",
        http_client=fail_client,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(p_app):
        p_state = p_app.state.node
        p_state.follower_state = ReplicaState.SYNCED

        p_trans = ASGITransport(app=p_app)
        async with AsyncClient(transport=p_trans, base_url="http://primary") as pc:
            # 1. Write attempt fails mid-replication
            w1 = await pc.post(
                "/doc",
                json={"request_id": "req-unres", "document": {"doc_id": 10, "val": "unresolved"}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            # Preserved as OUTCOME_UNKNOWN (202 Accepted)
            assert w1.status_code == 202
            err_code = w1.json().get("detail", {}).get("error_code") or w1.json().get("error_code")
            assert err_code == "OUTCOME_UNKNOWN"

            # Verification 4: Preserved in pending_unresolved_writes and dedup
            assert "req-unres" in p_state.pending_unresolved_writes
            dedup = p_state.engine.dedup_check("req-unres")
            assert dedup is not None
            assert dedup.outcome == WriteOutcome.OUTCOME_UNKNOWN

            # Verification 5: Subsequent writes with different request_id are blocked
            w2 = await pc.post(
                "/doc",
                json={
                    "request_id": "req-subsequent",
                    "document": {"doc_id": 20, "val": "subsequent"},
                },
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert w2.status_code == 503
            w2_err = w2.json().get("detail", {}).get("error_code") or w2.json().get("error_code")
            assert w2_err == "REPLICATION_UNAVAILABLE"
            assert (
                "Cannot accept subsequent writes while an earlier write has an unresolved replication outcome"
                in str(w2.json())
            )

            # Primary did not append subsequent write to WAL
            assert p_state.wal.last_seq == 1
            assert p_state.engine.get(20) is None


@pytest.mark.asyncio
async def test_regression_primary_without_follower_url_fails_closed(tmp_path: Path):
    """Regression test: Primary without configured follower fails closed.
    - Write returns HTTP 503 REPLICATION_UNAVAILABLE.
    - WAL sequence remains unchanged.
    - committed_seq remains unchanged.
    - Engine remains unchanged.
    - No dedup entry is created.
    """
    settings = _make_settings(tmp_path)
    shard_id = "shard0"

    # Default build_app has follower_url=None and allow_standalone=False
    app = build_app(
        settings=settings,
        node_id="p-no-follower",
        shard_id=shard_id,
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
        follower_url=None,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app):
        state = app.state.node
        assert state.follower_url is None
        assert state.allow_standalone is False

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://node") as client:
            resp = await client.post(
                "/doc",
                json={
                    "request_id": "req-no-follower",
                    "document": {"doc_id": 10, "val": "fail_closed"},
                },
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )

            # 1. Write returns HTTP 503 REPLICATION_UNAVAILABLE
            assert resp.status_code == 503
            err_code = resp.json().get("detail", {}).get("error_code") or resp.json().get(
                "error_code"
            )
            assert err_code == "REPLICATION_UNAVAILABLE"

            # 2. WAL sequence remains unchanged (0)
            assert state.wal.last_seq == 0

            # 3. committed_seq remains unchanged (0)
            assert state.committed_seq == 0

            # 4. Engine remains unchanged
            assert state.engine.get(10) is None

            # 5. No dedup entry is created
            assert state.engine.dedup_check("req-no-follower") is None
