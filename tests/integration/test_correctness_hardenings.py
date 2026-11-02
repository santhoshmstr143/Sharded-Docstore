"""
Comprehensive regression tests for correctness hardenings:
1. WriteResponse API contract and semantics validation.
2. Follower rejoin validation on Primary (stale term, sequence lag, epoch mismatch).
3. Snapshot apply safety (stale sequence, stale term, stale epoch, fsync failure leaves WAL intact).
4. Migration oplog compaction detection and safe abort.
5. Coordinator recovery quarantine window and ambiguous range mapping rejection.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError

from docstore.common.auth import CLUSTER_SIG_HEADER, compute_signature
from docstore.common.config import Settings
from docstore.common.models import (
    NodeRegistration,
    NodeRole,
    ReplicaState,
    ShardMapEntry,
    WriteOutcome,
    WriteResponse,
)
from docstore.coordinator.coordinator import CoordinatorService
from docstore.coordinator.migration import MigrationManager, MigrationRecord, MigrationState
from docstore.shard.app import ShardRangeConfig, build_app

pytestmark = pytest.mark.integration

TEST_SECRET = "hardenings_test_secret_32_bytes_ok"


def _make_settings(data_dir: Path | str) -> Settings:
    return Settings(
        DATA_DIR=str(data_dir),
        CLUSTER_SECRET=TEST_SECRET,
        ENABLE_TEST_HOOKS=True,
        REPLICATION_TIMEOUT_S=1.0,
        SAFETY_MARGIN_S=0.1,
    )


def _sig_headers(body: dict[str, Any], secret: str = TEST_SECRET) -> dict[str, str]:
    b_bytes = json.dumps(body, separators=(",", ":")).encode("utf-8")
    sig = compute_signature(secret, b_bytes)
    return {"Content-Type": "application/json", CLUSTER_SIG_HEADER: sig}


def _err_msg(resp: httpx.Response) -> str:
    data = resp.json()
    return str(data.get("error_message") or data.get("detail") or data)


# ─────────────────────────────────────────────────────────────────────────────
# 1. WriteResponse Contract Validation
# ─────────────────────────────────────────────────────────────────────────────


def test_write_response_contract_committed_requires_seq() -> None:
    """COMMITTED outcome must have a non-None seq."""
    valid = WriteResponse(outcome=WriteOutcome.COMMITTED, seq=42, request_id="r1")
    assert valid.seq == 42

    with pytest.raises(ValidationError) as exc:
        WriteResponse(outcome=WriteOutcome.COMMITTED, seq=None, request_id="r1")
    assert "COMMITTED writes must provide a valid seq" in str(exc.value)


def test_write_response_contract_rejected_forbids_seq() -> None:
    """REJECTED outcome must have seq=None."""
    valid = WriteResponse(
        outcome=WriteOutcome.REJECTED,
        seq=None,
        error_message="Not primary",
        request_id="r2",
    )
    assert valid.seq is None

    with pytest.raises(ValidationError) as exc:
        WriteResponse(
            outcome=WriteOutcome.REJECTED,
            seq=10,
            error_message="Not primary",
            request_id="r2",
        )
    assert "REJECTED writes must not provide a seq" in str(exc.value)


def test_write_response_contract_outcome_unknown_allows_optional_seq() -> None:
    """OUTCOME_UNKNOWN allows seq to be None or an unconfirmed seq."""
    resp1 = WriteResponse(outcome=WriteOutcome.OUTCOME_UNKNOWN, seq=None, request_id="r3")
    assert resp1.seq is None
    resp2 = WriteResponse(outcome=WriteOutcome.OUTCOME_UNKNOWN, seq=5, request_id="r3")
    assert resp2.seq == 5


# ─────────────────────────────────────────────────────────────────────────────
# 2. Primary Follower Rejoin Validation
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_rejoin_complete_stale_term_rejected(tmp_path: Path) -> None:
    """Primary rejects follower rejoin when follower term is stale."""
    settings = _make_settings(tmp_path)
    app = build_app(
        settings,
        node_id="shard-0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )

    async with LifespanManager(app):
        state = app.state.node
        state.term = 3
        state.follower_state = ReplicaState.CATCHING_UP

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            body = {
                "follower_node_id": "shard-0-follower",
                "term": 2,  # Stale term
                "last_applied_seq": 0,
                "ownership_epochs": {"0-1000": 1},
            }
            resp = await client.post(
                "/internal/rejoin/complete",
                json=body,
                headers=_sig_headers(body),
            )
            assert resp.status_code == 409
            assert "Term mismatch" in _err_msg(resp)
            assert state.follower_state == ReplicaState.CATCHING_UP


@pytest.mark.asyncio
async def test_rejoin_complete_sequence_lag_rejected(tmp_path: Path) -> None:
    """Primary rejects follower rejoin when follower has not reached committed_seq."""
    settings = _make_settings(tmp_path)
    app = build_app(
        settings,
        node_id="shard-0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )

    async with LifespanManager(app):
        state = app.state.node
        state.term = 1
        state.committed_seq = 50
        state.follower_state = ReplicaState.CATCHING_UP

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            body = {
                "follower_node_id": "shard-0-follower",
                "term": 1,
                "last_applied_seq": 49,  # Lagging behind committed_seq 50
                "ownership_epochs": {"0-1000": 1},
            }
            resp = await client.post(
                "/internal/rejoin/complete",
                json=body,
                headers=_sig_headers(body),
            )
            assert resp.status_code == 409
            assert "Follower has not caught up" in _err_msg(resp)
            assert state.follower_state == ReplicaState.CATCHING_UP


@pytest.mark.asyncio
async def test_rejoin_complete_stale_epoch_rejected(tmp_path: Path) -> None:
    """Primary rejects follower rejoin when follower has a stale or mismatched epoch."""
    settings = _make_settings(tmp_path)
    app = build_app(
        settings,
        node_id="shard-0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=5)],
    )

    async with LifespanManager(app):
        state = app.state.node
        state.term = 1
        state.committed_seq = 10
        state.follower_state = ReplicaState.CATCHING_UP

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            body = {
                "follower_node_id": "shard-0-follower",
                "term": 1,
                "last_applied_seq": 10,
                "ownership_epochs": {"0-1000": 4},  # Stale epoch 4 < 5
            }
            resp = await client.post(
                "/internal/rejoin/complete",
                json=body,
                headers=_sig_headers(body),
            )
            assert resp.status_code == 409
            assert "stale ownership epoch" in _err_msg(resp).lower()
            assert state.follower_state == ReplicaState.CATCHING_UP


@pytest.mark.asyncio
async def test_rejoin_complete_healthy_follower_synced(tmp_path: Path) -> None:
    """Primary marks follower SYNCED when term, seq, and epochs match primary state."""
    settings = _make_settings(tmp_path)
    app = build_app(
        settings,
        node_id="shard-0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=2)],
    )

    async with LifespanManager(app):
        state = app.state.node
        state.term = 2
        state.committed_seq = 10
        state.follower_state = ReplicaState.CATCHING_UP

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            body = {
                "follower_node_id": "shard-0-follower",
                "term": 2,
                "last_applied_seq": 10,
                "ownership_epochs": {"0-1000": 2},
            }
            resp = await client.post(
                "/internal/rejoin/complete",
                json=body,
                headers=_sig_headers(body),
            )
            assert resp.status_code == 200
            assert resp.json()["status"] == "ok"
            assert state.follower_state == ReplicaState.SYNCED


# ─────────────────────────────────────────────────────────────────────────────
# 3. Snapshot Apply Safety
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_apply_snapshot_stale_seq_rejected(tmp_path: Path) -> None:
    """Node rejects snapshot if snapshot last_applied_seq < local committed_seq."""
    settings = _make_settings(tmp_path)
    app = build_app(
        settings,
        node_id="shard-0-node",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )

    async with LifespanManager(app):
        state = app.state.node
        state.committed_seq = 10
        state.wal.append(seq=10, term=1, op="insert", doc_id=1, request_id="r1", payload={"a": 1})

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            body = {
                "snapshot_id": "snap-1",
                "last_applied_seq": 5,  # Stale: 5 < 10
                "term": 1,
                "documents": {1: {"_id": 1, "a": 1}},
                "ownership_epochs": {"0-1000": 1},
            }
            resp = await client.post(
                "/internal/snapshot/apply",
                json=body,
                headers=_sig_headers(body),
            )
            assert resp.status_code == 409
            assert "last_applied_seq (5)" in _err_msg(resp)
            assert state.committed_seq == 10
            assert state.wal.last_seq == 10


@pytest.mark.asyncio
async def test_apply_snapshot_stale_term_rejected(tmp_path: Path) -> None:
    """Node rejects snapshot if snapshot term < local term."""
    settings = _make_settings(tmp_path)
    app = build_app(
        settings,
        node_id="shard-0-node",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )

    async with LifespanManager(app):
        state = app.state.node
        state.term = 3

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            body = {
                "snapshot_id": "snap-1",
                "last_applied_seq": 10,
                "term": 2,  # Stale term: 2 < 3
                "documents": {1: {"_id": 1, "a": 1}},
                "ownership_epochs": {"0-1000": 1},
            }
            resp = await client.post(
                "/internal/snapshot/apply",
                json=body,
                headers=_sig_headers(body),
            )
            assert resp.status_code == 409
            assert "Cannot apply snapshot with term (2) < local term (3)" in _err_msg(resp)


@pytest.mark.asyncio
async def test_apply_snapshot_durability_failure_leaves_wal_intact(tmp_path: Path) -> None:
    """If snapshot persistence fails (e.g. dir fsync failure), WAL is NOT truncated."""
    settings = _make_settings(tmp_path)
    app = build_app(
        settings,
        node_id="shard-0-node",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )

    async with LifespanManager(app):
        state = app.state.node
        state.term = 1
        state.committed_seq = 2
        state.wal.append(seq=1, term=1, op="insert", doc_id=1, request_id="r1", payload={"v": 1})
        state.wal.append(seq=2, term=1, op="insert", doc_id=2, request_id="r2", payload={"v": 2})
        state.wal.append(seq=3, term=1, op="insert", doc_id=3, request_id="r3", payload={"v": 3})
        assert state.wal.last_seq == 3

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            body = {
                "snapshot_id": "snap-pers-fail",
                "last_applied_seq": 2,
                "term": 1,
                "documents": {1: {"_id": 1, "v": 1}, 2: {"_id": 2, "v": 2}},
                "ownership_epochs": {"0-1000": 1},
            }

            with patch.object(
                state.snapshot_mgr,
                "save",
                side_effect=OSError("Disk failure during snapshot fsync"),
            ):
                resp = await client.post(
                    "/internal/snapshot/apply",
                    json=body,
                    headers=_sig_headers(body),
                )
                assert resp.status_code == 500
                assert "Failed to durably persist snapshot" in _err_msg(resp)

            # Verify WAL was NOT truncated: all 3 entries remain intact
            assert state.wal.last_seq == 3
            entries = list(state.wal.replay(after_seq=0))
            assert len(entries) == 3
            assert [e["seq"] for e in entries] == [1, 2, 3]


# ─────────────────────────────────────────────────────────────────────────────
# 4. Migration Oplog Compaction Detection
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_migration_oplog_compacted_cursor_returns_needs_snapshot(tmp_path: Path) -> None:
    """When after_seq has been compacted away, /internal/migration/oplog signals needs_snapshot."""
    settings = _make_settings(tmp_path)
    app = build_app(
        settings,
        node_id="shard-0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )

    async with LifespanManager(app):
        state = app.state.node
        state.is_write_ready = True
        for s in range(10, 16):
            state.wal.append(
                seq=s, term=1, op="insert", doc_id=s, request_id=f"r-{s}", payload={"v": s}
            )

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            body = {
                "migration_id": "mig-compact-1",
                "start": 0,
                "end": 1000,
                "after_seq": 2,
                "limit": 10,
            }
            resp = await client.post(
                "/internal/migration/oplog",
                json=body,
                headers=_sig_headers(body),
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["needs_snapshot"] is True
            assert data["status"] == "compacted"
            assert data["earliest_seq"] == 10
            assert data["entries"] == []


@pytest.mark.asyncio
async def test_migration_manager_aborts_safely_when_oplog_compacted(tmp_path: Path) -> None:
    """Migration catchup safely aborts if oplog returns needs_snapshot=True."""
    settings = _make_settings(tmp_path)
    manager = MigrationManager(settings.DATA_DIR, settings.CLUSTER_SECRET)
    coord = CoordinatorService(settings)
    coord.replica_states["shard0"] = ReplicaState.SYNCED
    coord.replica_states["shard1"] = ReplicaState.SYNCED

    rec = MigrationRecord(
        migration_id="mig-rec-1",
        from_shard="shard0",
        to_shard="shard1",
        range_start=0,
        range_end=500,
        state=MigrationState.CATCHUP,
        last_caught_up_seq=5,
    )
    manager.migrations[rec.migration_id] = rec

    class MockResponse:
        def __init__(self, data: dict[str, Any]) -> None:
            self._data = data
            self.status_code = 200

        def json(self) -> dict[str, Any]:
            return self._data

        def raise_for_status(self) -> None:
            pass

    class MockAsyncClient:
        async def post(self, url: str, **kwargs: Any) -> MockResponse:
            if "oplog" in url:
                return MockResponse(
                    {
                        "status": "compacted",
                        "needs_snapshot": True,
                        "earliest_seq": 20,
                        "entries": [],
                    }
                )
            return MockResponse({"status": "ok"})

    with pytest.raises(RuntimeError) as exc:
        await manager.execute_migration(
            rec.migration_id,
            coord,
            http_client=MockAsyncClient(),  # type: ignore[arg-type]
        )
    assert "was compacted away; new snapshot required" in str(exc.value)


# ─────────────────────────────────────────────────────────────────────────────
# 5. Coordinator Quarantine & Ambiguous Range Validation
# ─────────────────────────────────────────────────────────────────────────────


def test_coordinator_restart_quarantine_initialization(tmp_path: Path) -> None:
    """Coordinator initializes recovery quarantine window on startup."""
    settings = _make_settings(tmp_path)
    now = time.monotonic()
    coord = CoordinatorService(settings)
    assert coord._recovery_quarantine_until_monotonic >= now + settings.LEASE_DURATION_S


@pytest.mark.asyncio
async def test_coordinator_failover_waits_for_quarantine_when_no_lease(tmp_path: Path) -> None:
    """Coordinator failover honors recovery quarantine window before granting new lease."""
    settings = _make_settings(tmp_path)
    coord = CoordinatorService(settings)
    # Set a tiny quarantine 0.2s into the future
    coord._recovery_quarantine_until_monotonic = time.monotonic() + 0.2

    matching_entry = coord.map_mgr.shard_map.entries[0]
    shard_id = matching_entry.shard_id
    follower_addr = matching_entry.follower or "127.0.0.1:8002"

    coord.register_node(
        NodeRegistration(
            node_id="node-p",
            shard_id=shard_id,
            address=matching_entry.primary,
            requested_role=NodeRole.PRIMARY,
        )
    )
    coord.register_node(
        NodeRegistration(
            node_id="node-f",
            shard_id=shard_id,
            address=follower_addr,
            requested_role=NodeRole.FOLLOWER,
        )
    )

    # Simulate coordinator restart: leases dict is empty, so no active lease exists in memory
    coord.leases.clear()

    # Mock health probe check in step 3 and promote RPC in step 8
    class MockHealthResp:
        status_code = 200
        text = "ok"

    class MockClient:
        async def get(self, *args: Any, **kwargs: Any) -> MockHealthResp:
            return MockHealthResp()

        async def post(self, *args: Any, **kwargs: Any) -> MockHealthResp:
            return MockHealthResp()

        async def aclose(self) -> None:
            pass

    t0 = time.monotonic()
    await coord.trigger_failover(shard_id, http_client=MockClient())  # type: ignore[arg-type]
    elapsed = time.monotonic() - t0
    assert elapsed >= 0.15


def test_coordinator_ambiguous_range_mapping_rejected(tmp_path: Path) -> None:
    """Coordinator bump_range_migration rejects ambiguous overlapping range entries."""
    settings = _make_settings(tmp_path)
    coord = CoordinatorService(settings)

    matching_entry = coord.map_mgr.shard_map.entries[0]
    duplicate_entry = ShardMapEntry(
        range=matching_entry.range,
        shard_id=matching_entry.shard_id,
        primary=matching_entry.primary,
        follower=matching_entry.follower,
        ownership_epoch=matching_entry.ownership_epoch,
        map_version=matching_entry.map_version,
    )
    coord.map_mgr.shard_map.entries.append(duplicate_entry)

    with pytest.raises(ValueError) as exc:
        coord.bump_range_migration(
            shard_id=matching_entry.shard_id,
            start=matching_entry.range.start,
            end=matching_entry.range.end,
        )
    assert "Ambiguous range mapping" in str(exc.value)
