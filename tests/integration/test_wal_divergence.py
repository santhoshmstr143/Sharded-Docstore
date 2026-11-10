"""
Tests for Bug #4: WAL divergence detection and log reconciliation.

Verifies:
1. Deterministic entry identity (SHA-256 over canonical JSON).
2. Key ordering independence for dictionary payloads.
3. Divergence detection when (term, seq) match but request IDs differ.
4. Divergence detection when (term, seq, request_id) match but payloads differ.
5. Divergence followed by a later matching entry (longest contiguous prefix).
6. Normal matching histories.
7. Full node recovery after legitimate primary failover.
8. Refusal to silently overwrite locally committed history (fail-closed safety).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from docstore.common.auth import CLUSTER_SIG_HEADER, compute_signature
from docstore.common.config import Settings
from docstore.common.models import NodeRole, ReplicaState, WriteOutcome
from docstore.shard.app import (
    NodeState,
    ShardRangeConfig,
    _recover,
    build_app,
    resync_with_primary,
)
from docstore.storage.engine import DedupEntry
from docstore.storage.snapshot import SnapshotManager
from docstore.storage.wal import compute_entry_identity

pytestmark = pytest.mark.integration


TEST_SECRET = "test-cluster-secret-32-chars-long-ok!"


def _make_settings(data_dir: Path) -> Settings:
    data_dir.mkdir(parents=True, exist_ok=True)
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


# ─────────────────────────────────────────────────────────────────────────────
# 1. Deterministic Entry Identity & Key Ordering
# ─────────────────────────────────────────────────────────────────────────────


def test_wal_entry_identity_deterministic_key_ordering() -> None:
    """Equivalent JSON payloads produce the same identity regardless of key order."""
    entry1 = {
        "seq": 1,
        "term": 1,
        "op": "insert",
        "doc_id": 42,
        "request_id": "req-1",
        "payload": {"name": "alice", "age": 30, "metadata": {"b": 2, "a": 1}},
    }
    entry2 = {
        "seq": 1,
        "term": 1,
        "op": "insert",
        "doc_id": 42,
        "request_id": "req-1",
        "payload": {"metadata": {"a": 1, "b": 2}, "age": 30, "name": "alice"},
    }
    assert compute_entry_identity(entry1) == compute_entry_identity(entry2)


def test_wal_entry_identity_differences() -> None:
    """Any variation in term, seq, op, doc_id, request_id, or payload alters identity."""
    base = {
        "seq": 2,
        "term": 1,
        "op": "insert",
        "doc_id": 10,
        "request_id": "req-base",
        "payload": {"v": 1},
    }
    base_id = compute_entry_identity(base)

    # Different request_id
    diff_req = dict(base, request_id="req-other")
    assert compute_entry_identity(diff_req) != base_id

    # Different payload
    diff_payload = dict(base, payload={"v": 2})
    assert compute_entry_identity(diff_payload) != base_id

    # Different term
    diff_term = dict(base, term=2)
    assert compute_entry_identity(diff_term) != base_id

    # Different op
    diff_op = dict(base, op="update")
    assert compute_entry_identity(diff_op) != base_id

    # Different doc_id
    diff_doc = dict(base, doc_id=99)
    assert compute_entry_identity(diff_doc) != base_id


# ─────────────────────────────────────────────────────────────────────────────
# 2. Endpoint Divergence Tests (/internal/divergence)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_divergence_same_term_and_seq_different_request_id(tmp_path: Path) -> None:
    """Same (term=1, seq=2) but different request_id must be detected as conflict."""
    settings = _make_settings(tmp_path / "primary")
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    app = build_app(
        settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app):
        wal = app.state.node.wal
        wal.append(seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1})
        wal.append(seq=2, term=1, op="insert", doc_id=2, request_id="req-primary", payload={"v": 2})

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://primary"
        ) as client:
            follower_entries = [
                {
                    "seq": 1,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 1,
                    "request_id": "req-1",
                    "payload": {"v": 1},
                    "entry_id": compute_entry_identity(
                        {
                            "seq": 1,
                            "term": 1,
                            "op": "insert",
                            "doc_id": 1,
                            "request_id": "req-1",
                            "payload": {"v": 1},
                        }
                    ),
                },
                {
                    "seq": 2,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 2,
                    "request_id": "req-follower",  # DIFFERENT REQUEST ID
                    "payload": {"v": 2},
                    "entry_id": compute_entry_identity(
                        {
                            "seq": 2,
                            "term": 1,
                            "op": "insert",
                            "doc_id": 2,
                            "request_id": "req-follower",
                            "payload": {"v": 2},
                        }
                    ),
                },
            ]
            body = {"shard_id": "shard0", "entries": follower_entries}
            resp = await client.post("/internal/divergence", json=body, headers=_sig_headers(body))
            assert resp.status_code == 200
            data = resp.json()
            assert data["divergence_seq"] == 1
            assert data["first_conflict_seq"] == 2


@pytest.mark.asyncio
async def test_divergence_same_term_seq_req_different_payload(tmp_path: Path) -> None:
    """Same (term=1, seq=2, request_id='req-2') but different payload must diverge."""
    settings = _make_settings(tmp_path / "primary")
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    app = build_app(
        settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app):
        wal = app.state.node.wal
        wal.append(seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1})
        wal.append(
            seq=2,
            term=1,
            op="insert",
            doc_id=2,
            request_id="req-2",
            payload={"val": "primary_data"},
        )

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://primary"
        ) as client:
            follower_entries = [
                {
                    "seq": 1,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 1,
                    "request_id": "req-1",
                    "payload": {"v": 1},
                    "entry_id": compute_entry_identity(
                        {
                            "seq": 1,
                            "term": 1,
                            "op": "insert",
                            "doc_id": 1,
                            "request_id": "req-1",
                            "payload": {"v": 1},
                        }
                    ),
                },
                {
                    "seq": 2,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 2,
                    "request_id": "req-2",
                    "payload": {"val": "follower_data"},  # DIFFERENT PAYLOAD
                    "entry_id": compute_entry_identity(
                        {
                            "seq": 2,
                            "term": 1,
                            "op": "insert",
                            "doc_id": 2,
                            "request_id": "req-2",
                            "payload": {"val": "follower_data"},
                        }
                    ),
                },
            ]
            body = {"shard_id": "shard0", "entries": follower_entries}
            resp = await client.post("/internal/divergence", json=body, headers=_sig_headers(body))
            assert resp.status_code == 200
            data = resp.json()
            assert data["divergence_seq"] == 1
            assert data["first_conflict_seq"] == 2


@pytest.mark.asyncio
async def test_divergence_identical_entries_different_key_order(tmp_path: Path) -> None:
    """Identical entries with reversed key order in dicts must match with no conflict."""
    settings = _make_settings(tmp_path / "primary")
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    app = build_app(
        settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app):
        wal = app.state.node.wal
        wal.append(
            seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"z": 10, "a": 20}
        )

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://primary"
        ) as client:
            follower_entries = [
                {
                    "seq": 1,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 1,
                    "request_id": "req-1",
                    "payload": {"a": 20, "z": 10},  # Different key order
                    "entry_id": compute_entry_identity(
                        {
                            "seq": 1,
                            "term": 1,
                            "op": "insert",
                            "doc_id": 1,
                            "request_id": "req-1",
                            "payload": {"a": 20, "z": 10},
                        }
                    ),
                }
            ]
            body = {"shard_id": "shard0", "entries": follower_entries}
            resp = await client.post("/internal/divergence", json=body, headers=_sig_headers(body))
            assert resp.status_code == 200
            data = resp.json()
            assert data["divergence_seq"] == 1
            assert data["first_conflict_seq"] is None


@pytest.mark.asyncio
async def test_divergence_followed_by_later_matching_entry(tmp_path: Path) -> None:
    """A later matching entry (seq=3) must NOT mask an earlier conflict at seq=2."""
    settings = _make_settings(tmp_path / "primary")
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    app = build_app(
        settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app):
        wal = app.state.node.wal
        wal.append(seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1})
        wal.append(
            seq=2, term=1, op="insert", doc_id=2, request_id="req-2-p", payload={"v": 2}
        )  # Diverges here
        wal.append(seq=3, term=1, op="insert", doc_id=3, request_id="req-3", payload={"v": 3})

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://primary"
        ) as client:
            follower_entries = [
                {
                    "seq": 1,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 1,
                    "request_id": "req-1",
                    "payload": {"v": 1},
                    "entry_id": compute_entry_identity(
                        {
                            "seq": 1,
                            "term": 1,
                            "op": "insert",
                            "doc_id": 1,
                            "request_id": "req-1",
                            "payload": {"v": 1},
                        }
                    ),
                },
                {
                    "seq": 2,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 2,
                    "request_id": "req-2-f",  # Divergent
                    "payload": {"v": 999},
                    "entry_id": compute_entry_identity(
                        {
                            "seq": 2,
                            "term": 1,
                            "op": "insert",
                            "doc_id": 2,
                            "request_id": "req-2-f",
                            "payload": {"v": 999},
                        }
                    ),
                },
                {
                    "seq": 3,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 3,
                    "request_id": "req-3",  # Identical later entry
                    "payload": {"v": 3},
                    "entry_id": compute_entry_identity(
                        {
                            "seq": 3,
                            "term": 1,
                            "op": "insert",
                            "doc_id": 3,
                            "request_id": "req-3",
                            "payload": {"v": 3},
                        }
                    ),
                },
            ]
            body = {"shard_id": "shard0", "entries": follower_entries}
            resp = await client.post("/internal/divergence", json=body, headers=_sig_headers(body))
            assert resp.status_code == 200
            data = resp.json()
            # Must stop at seq 1, first conflict is seq 2, seq 3 is NOT accepted
            assert data["divergence_seq"] == 1
            assert data["first_conflict_seq"] == 2


@pytest.mark.asyncio
async def test_divergence_normal_matching_histories(tmp_path: Path) -> None:
    """Contiguous matching entries up to seq 4 with no conflicts."""
    settings = _make_settings(tmp_path / "primary")
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    app = build_app(
        settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app):
        wal = app.state.node.wal
        for i in range(1, 5):
            wal.append(
                seq=i, term=1, op="insert", doc_id=i, request_id=f"req-{i}", payload={"v": i}
            )

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://primary"
        ) as client:
            follower_entries = [
                {
                    "seq": i,
                    "term": 1,
                    "op": "insert",
                    "doc_id": i,
                    "request_id": f"req-{i}",
                    "payload": {"v": i},
                    "entry_id": compute_entry_identity(
                        {
                            "seq": i,
                            "term": 1,
                            "op": "insert",
                            "doc_id": i,
                            "request_id": f"req-{i}",
                            "payload": {"v": i},
                        }
                    ),
                }
                for i in range(1, 5)
            ]
            body = {"shard_id": "shard0", "entries": follower_entries}
            resp = await client.post("/internal/divergence", json=body, headers=_sig_headers(body))
            assert resp.status_code == 200
            data = resp.json()
            assert data["divergence_seq"] == 4
            assert data["first_conflict_seq"] is None


# ─────────────────────────────────────────────────────────────────────────────
# 3. Full Node Failover & Recovery Regression Tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_recovery_after_legitimate_primary_failover(tmp_path: Path) -> None:
    """Legitimate failover: old primary truncates uncommitted tail, catches up with new primary."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    # 1. New primary (promoted follower) in term 2
    f_settings = _make_settings(tmp_path / "new_primary")
    new_p_app = build_app(
        f_settings,
        node_id="new-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    # 2. Old primary: seq 1 committed, seq 2 uncommitted in term 1
    p_settings = _make_settings(tmp_path / "old_primary")
    old_p_app = build_app(
        p_settings, node_id="old-primary", shard_id="shard0", role=NodeRole.FOLLOWER, ranges=ranges
    )

    async with LifespanManager(new_p_app):
        new_p_state = new_p_app.state.node
        new_p_state.term = 2

        # Common committed entry at seq=1 (term=1)
        new_p_state.wal.append(
            seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1}
        )
        new_p_state.engine.apply(
            {
                "seq": 1,
                "term": 1,
                "op": "insert",
                "doc_id": 1,
                "request_id": "req-1",
                "payload": {"v": 1},
            }
        )
        new_p_state.committed_seq = 1

        # New primary committed write at seq=2 in term 2
        new_p_state.wal.append(
            seq=2, term=2, op="insert", doc_id=2, request_id="req-2-new", payload={"v": 200}
        )
        new_p_state.engine.apply(
            {
                "seq": 2,
                "term": 2,
                "op": "insert",
                "doc_id": 2,
                "request_id": "req-2-new",
                "payload": {"v": 200},
            }
        )
        new_p_state.committed_seq = 2

        async with LifespanManager(old_p_app):
            old_p_state = old_p_app.state.node
            old_p_state.term = 1
            old_p_state.wal.append(
                seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1}
            )
            old_p_state.engine.apply(
                {
                    "seq": 1,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 1,
                    "request_id": "req-1",
                    "payload": {"v": 1},
                }
            )
            old_p_state.committed_seq = 1  # Only seq 1 was committed

            # Uncommitted entry at seq 2 in term 1
            old_p_state.wal.append(
                seq=2,
                term=1,
                op="insert",
                doc_id=99,
                request_id="req-uncommitted",
                payload={"v": 99},
            )
            old_p_state.engine.apply(
                {
                    "seq": 2,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 99,
                    "request_id": "req-uncommitted",
                    "payload": {"v": 99},
                }
            )
            assert old_p_state.wal.last_seq == 2
            assert old_p_state.engine.get(99) is not None

            # Connect old primary to new primary
            np_transport = ASGITransport(app=new_p_app)
            rejoin_client = AsyncClient(mounts={"http://new-primary": np_transport}, timeout=5.0)

            # Execute rejoin / resync
            ok = await resync_with_primary(old_p_state, "new-primary", http_client=rejoin_client)
            assert ok is True

            # Old primary has truncated divergent seq 2 and caught up with new primary's seq 2
            assert old_p_state.wal.last_seq == 2
            assert old_p_state.engine.get(99) is None  # Uncommitted doc 99 discarded
            assert old_p_state.engine.get(2) is not None  # Doc 2 from new primary applied
            assert old_p_state.engine.get(2)["v"] == 200
            assert old_p_state.term == 2
            assert new_p_state.follower_state == ReplicaState.SYNCED


@pytest.mark.asyncio
async def test_rejoin_refuses_to_overwrite_committed_history_fail_closed(tmp_path: Path) -> None:
    """If divergence occurs before rejoining node's committed_seq, fail closed immediately."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    # New primary has seq 1 and seq 2
    f_settings = _make_settings(tmp_path / "primary")
    primary_app = build_app(
        f_settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    # Rejoining node also COMMITTED seq 2 with different data
    r_settings = _make_settings(tmp_path / "rejoining")
    rejoin_app = build_app(
        r_settings, node_id="rejoining", shard_id="shard0", role=NodeRole.FOLLOWER, ranges=ranges
    )

    async with LifespanManager(primary_app):
        p_state = primary_app.state.node
        p_state.wal.append(
            seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1}
        )
        p_state.wal.append(
            seq=2, term=2, op="insert", doc_id=2, request_id="req-2-p", payload={"v": "primary"}
        )

        async with LifespanManager(rejoin_app):
            r_state = rejoin_app.state.node
            r_state.wal.append(
                seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1}
            )
            r_state.wal.append(
                seq=2,
                term=1,
                op="insert",
                doc_id=2,
                request_id="req-2-r",
                payload={"v": "rejoining"},
            )
            r_state.committed_seq = 2  # LOCALLY COMMITTED!

            np_transport = ASGITransport(app=primary_app)
            rejoin_client = AsyncClient(mounts={"http://primary": np_transport}, timeout=5.0)

            # Must FAIL CLOSED and refuse to overwrite locally committed seq 2
            ok = await resync_with_primary(r_state, "primary", http_client=rejoin_client)
            assert ok is False
            # WAL tail was NOT truncated
            assert r_state.wal.last_seq == 2
            assert r_state.committed_seq == 2


# ─────────────────────────────────────────────────────────────────────────────
# 9. Entry Validation Against Actual Content (Issue 2)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_divergence_entry_validation_correct_fields_and_entry_id(tmp_path: Path) -> None:
    """Correct entry fields and correct entry_id match normally."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    settings = _make_settings(tmp_path / "node")
    app = build_app(
        settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app):
        state = app.state.node
        state.wal.append(seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1})
        rec2 = state.wal.append(
            seq=2, term=1, op="insert", doc_id=2, request_id="req-2", payload={"v": 2}
        )

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://primary") as client:
            entries = [
                {
                    "seq": 1,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 1,
                    "request_id": "req-1",
                    "payload": {"v": 1},
                    "entry_id": compute_entry_identity(
                        {
                            "seq": 1,
                            "term": 1,
                            "op": "insert",
                            "doc_id": 1,
                            "request_id": "req-1",
                            "payload": {"v": 1},
                        }
                    ),
                },
                {
                    "seq": 2,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 2,
                    "request_id": "req-2",
                    "payload": {"v": 2},
                    "entry_id": rec2["entry_id"],
                },
            ]
            resp = await client.post(
                "/internal/divergence",
                json={"shard_id": "shard0", "entries": entries},
                headers=_sig_headers({"shard_id": "shard0", "entries": entries}),
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["divergence_seq"] == 2
            assert data["first_conflict_seq"] is None


@pytest.mark.asyncio
async def test_divergence_entry_validation_stale_entry_id_diff_payload(tmp_path: Path) -> None:
    """Different payload but stale entry_id copied from primary is detected as a conflict."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    settings = _make_settings(tmp_path / "node")
    app = build_app(
        settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app):
        state = app.state.node
        state.wal.append(seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1})
        rec2 = state.wal.append(
            seq=2,
            term=1,
            op="insert",
            doc_id=2,
            request_id="req-2",
            payload={"v": "primary_data"},
        )

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://primary") as client:
            # Follower sends different payload, but copied primary's entry_id
            entries = [
                {
                    "seq": 1,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 1,
                    "request_id": "req-1",
                    "payload": {"v": 1},
                },
                {
                    "seq": 2,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 2,
                    "request_id": "req-2",
                    "payload": {"v": "tampered_follower_data"},
                    "entry_id": rec2["entry_id"],  # copied from primary!
                },
            ]
            resp = await client.post(
                "/internal/divergence",
                json={"shard_id": "shard0", "entries": entries},
                headers=_sig_headers({"shard_id": "shard0", "entries": entries}),
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["divergence_seq"] == 1
            assert data["first_conflict_seq"] == 2


@pytest.mark.asyncio
async def test_divergence_entry_validation_stale_entry_id_diff_request_id(tmp_path: Path) -> None:
    """Different request_id but stale entry_id copied from primary is detected as a conflict."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    settings = _make_settings(tmp_path / "node")
    app = build_app(
        settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app):
        state = app.state.node
        state.wal.append(seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1})
        rec2 = state.wal.append(
            seq=2, term=1, op="insert", doc_id=2, request_id="req-primary", payload={"v": 2}
        )

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://primary") as client:
            entries = [
                {
                    "seq": 1,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 1,
                    "request_id": "req-1",
                    "payload": {"v": 1},
                },
                {
                    "seq": 2,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 2,
                    "request_id": "req-follower-diff",
                    "payload": {"v": 2},
                    "entry_id": rec2["entry_id"],  # copied from primary
                },
            ]
            resp = await client.post(
                "/internal/divergence",
                json={"shard_id": "shard0", "entries": entries},
                headers=_sig_headers({"shard_id": "shard0", "entries": entries}),
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["divergence_seq"] == 1
            assert data["first_conflict_seq"] == 2


@pytest.mark.asyncio
async def test_divergence_entry_validation_incorrect_supplied_entry_id(tmp_path: Path) -> None:
    """Incorrect/bogus supplied entry_id is detected as a conflict."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    settings = _make_settings(tmp_path / "node")
    app = build_app(
        settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app):
        state = app.state.node
        state.wal.append(seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1})
        state.wal.append(seq=2, term=1, op="insert", doc_id=2, request_id="req-2", payload={"v": 2})

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://primary") as client:
            entries = [
                {
                    "seq": 1,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 1,
                    "request_id": "req-1",
                    "payload": {"v": 1},
                },
                {
                    "seq": 2,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 2,
                    "request_id": "req-2",
                    "payload": {"v": 2},
                    "entry_id": "bogus_corrupted_hash",
                },
            ]
            resp = await client.post(
                "/internal/divergence",
                json={"shard_id": "shard0", "entries": entries},
                headers=_sig_headers({"shard_id": "shard0", "entries": entries}),
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["divergence_seq"] == 1
            assert data["first_conflict_seq"] == 2


@pytest.mark.asyncio
async def test_divergence_entry_validation_missing_required_fields_fails_closed(
    tmp_path: Path,
) -> None:
    """Entry supplying only entry_id or missing required identity fields fails closed."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    settings = _make_settings(tmp_path / "node")
    app = build_app(
        settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app):
        state = app.state.node
        state.wal.append(seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1})
        rec2 = state.wal.append(
            seq=2, term=1, op="insert", doc_id=2, request_id="req-2", payload={"v": 2}
        )

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://primary") as client:
            # Entry 2 supplies entry_id but lacks op and doc_id
            entries = [
                {
                    "seq": 1,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 1,
                    "request_id": "req-1",
                    "payload": {"v": 1},
                },
                {
                    "seq": 2,
                    "term": 1,
                    "entry_id": rec2["entry_id"],
                },
            ]
            resp = await client.post(
                "/internal/divergence",
                json={"shard_id": "shard0", "entries": entries},
                headers=_sig_headers({"shard_id": "shard0", "entries": entries}),
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["divergence_seq"] == 1
            assert data["first_conflict_seq"] == 2


@pytest.mark.asyncio
async def test_divergence_legacy_record_without_entry_id(tmp_path: Path) -> None:
    """Legacy WAL records without entry_id compute identity from original fields and compare correctly."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    settings = _make_settings(tmp_path / "node")
    app = build_app(
        settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app):
        state = app.state.node
        state.wal.append(seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1})
        state.wal.append(seq=2, term=1, op="insert", doc_id=2, request_id="req-2", payload={"v": 2})

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://primary") as client:
            # Neither entry supplies entry_id (legacy client/follower)
            entries = [
                {
                    "seq": 1,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 1,
                    "request_id": "req-1",
                    "payload": {"v": 1},
                },
                {
                    "seq": 2,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 2,
                    "request_id": "req-2",
                    "payload": {"v": 2},
                },
            ]
            resp = await client.post(
                "/internal/divergence",
                json={"shard_id": "shard0", "entries": entries},
                headers=_sig_headers({"shard_id": "shard0", "entries": entries}),
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["divergence_seq"] == 2
            assert data["first_conflict_seq"] is None


# ─────────────────────────────────────────────────────────────────────────────
# 10. Snapshot Recovery Committed-History Protection (Issue 1)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_snapshot_resync_below_local_committed_seq_rejected(tmp_path: Path) -> None:
    """Snapshot sequence below local committed_seq is rejected and all local state is preserved."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    p_settings = _make_settings(tmp_path / "primary")
    primary_app = build_app(
        p_settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    r_settings = _make_settings(tmp_path / "follower")
    follower_app = build_app(
        r_settings, node_id="follower", shard_id="shard0", role=NodeRole.FOLLOWER, ranges=ranges
    )

    async with LifespanManager(primary_app):
        p_state = primary_app.state.node
        # Primary has snapshot at seq 1 and compacted WAL starting at seq 5
        p_state.engine.apply(
            {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": "p1"}}
        )
        p_state.engine.last_applied_seq = 1
        p_state.term = 1
        # Primary's WAL only has seq 5
        p_state.wal.append(
            seq=5, term=1, op="insert", doc_id=5, request_id="req-5", payload={"v": 5}
        )
        p_state.follower_state = ReplicaState.CATCHING_UP

        async with LifespanManager(follower_app):
            f_state = follower_app.state.node
            # Follower has locally COMMITTED seq 2 (doc 1 and doc 2)
            f_state.wal.append(
                seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": "f1"}
            )
            f_state.wal.append(
                seq=2, term=1, op="insert", doc_id=2, request_id="req-2", payload={"v": "f2"}
            )
            f_state.engine.apply(
                {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": "f1"}}
            )
            f_state.engine.apply(
                {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": "f2"}}
            )
            f_state.engine.last_applied_seq = 2
            f_state.committed_seq = 2
            f_state.term = 1

            p_transport = ASGITransport(app=primary_app)
            client = AsyncClient(mounts={"http://primary": p_transport}, timeout=5.0)

            # Follower attempts resync with primary.
            # Primary WAL earliest_seq is 5 > follower earliest 1 -> needs_snapshot = True.
            # Primary snapshot last_applied_seq is 1 < follower committed_seq (2).
            # Follower must REJECT and fail closed.
            ok = await resync_with_primary(f_state, "primary", http_client=client)
            assert ok is False

            # Verify ALL follower local state is completely preserved:
            assert f_state.committed_seq == 2
            assert f_state.engine.last_applied_seq == 2
            assert f_state.engine.get(1) == {"v": "f1"}
            assert f_state.engine.get(2) == {"v": "f2"}
            assert f_state.wal.last_seq == 2
            assert f_state.term == 1
            assert p_state.follower_state != ReplicaState.SYNCED


@pytest.mark.asyncio
async def test_snapshot_resync_equal_to_local_committed_seq_safe(tmp_path: Path) -> None:
    """Snapshot sequence equal to local committed_seq safely replaces state."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    p_settings = _make_settings(tmp_path / "primary")
    primary_app = build_app(
        p_settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    r_settings = _make_settings(tmp_path / "follower")
    follower_app = build_app(
        r_settings, node_id="follower", shard_id="shard0", role=NodeRole.FOLLOWER, ranges=ranges
    )

    async with LifespanManager(primary_app):
        p_state = primary_app.state.node
        # Primary has snapshot at seq 2 and WAL compacted starting at seq 5
        p_state.engine.apply(
            {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
        )
        p_state.engine.apply(
            {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": 2}}
        )
        p_state.engine.last_applied_seq = 2
        p_state.term = 1
        # Primary WAL compacted up to its snapshot (it can never be compacted past
        # it): earlier entries are only available via snapshot resync.
        p_state.wal.reset(p_state.engine.last_applied_seq + 1)
        p_state.follower_state = ReplicaState.CATCHING_UP

        async with LifespanManager(follower_app):
            f_state = follower_app.state.node
            f_state.wal.append(
                seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1}
            )
            f_state.wal.append(
                seq=2, term=1, op="insert", doc_id=2, request_id="req-2", payload={"v": 2}
            )
            f_state.engine.apply(
                {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
            )
            f_state.engine.apply(
                {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": 2}}
            )
            f_state.engine.last_applied_seq = 2
            f_state.committed_seq = 2
            f_state.term = 1

            p_transport = ASGITransport(app=primary_app)
            client = AsyncClient(mounts={"http://primary": p_transport}, timeout=5.0)

            # Snapshot last_applied_seq (2) == committed_seq (2): Safe!
            ok = await resync_with_primary(f_state, "primary", http_client=client)
            assert ok is True
            assert f_state.engine.last_applied_seq == 2
            assert f_state.committed_seq == 2
            assert p_state.follower_state == ReplicaState.SYNCED


@pytest.mark.asyncio
async def test_snapshot_resync_above_local_committed_seq_succeeds(tmp_path: Path) -> None:
    """Snapshot sequence above local committed_seq succeeds and advances state."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    p_settings = _make_settings(tmp_path / "primary")
    primary_app = build_app(
        p_settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    r_settings = _make_settings(tmp_path / "follower")
    follower_app = build_app(
        r_settings, node_id="follower", shard_id="shard0", role=NodeRole.FOLLOWER, ranges=ranges
    )

    async with LifespanManager(primary_app):
        p_state = primary_app.state.node
        # Primary has snapshot at seq 3 and WAL starting at seq 5
        p_state.engine.apply(
            {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
        )
        p_state.engine.apply(
            {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": 2}}
        )
        p_state.engine.apply(
            {"seq": 3, "term": 1, "op": "insert", "doc_id": 3, "payload": {"v": 3}}
        )
        p_state.engine.last_applied_seq = 3
        p_state.term = 1
        # Primary WAL compacted up to its snapshot (it can never be compacted past
        # it): earlier entries are only available via snapshot resync.
        p_state.wal.reset(p_state.engine.last_applied_seq + 1)
        p_state.follower_state = ReplicaState.CATCHING_UP

        async with LifespanManager(follower_app):
            f_state = follower_app.state.node
            f_state.wal.append(
                seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1}
            )
            f_state.wal.append(
                seq=2, term=1, op="insert", doc_id=2, request_id="req-2", payload={"v": 2}
            )
            f_state.engine.apply(
                {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
            )
            f_state.engine.apply(
                {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": 2}}
            )
            f_state.engine.last_applied_seq = 2
            f_state.committed_seq = 2
            f_state.term = 1

            p_transport = ASGITransport(app=primary_app)
            client = AsyncClient(mounts={"http://primary": p_transport}, timeout=5.0)

            # Snapshot last_applied_seq (3) > committed_seq (2): Succeeds!
            ok = await resync_with_primary(f_state, "primary", http_client=client)
            assert ok is True
            assert f_state.engine.last_applied_seq == 3
            assert f_state.committed_seq == 3
            assert f_state.engine.get(3) == {"v": 3}
            assert p_state.follower_state == ReplicaState.SYNCED


@pytest.mark.asyncio
async def test_failed_snapshot_validation_does_not_notify_primary(tmp_path: Path) -> None:
    """Failed snapshot validation must not call /internal/rejoin/complete or mark follower as synced."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    p_settings = _make_settings(tmp_path / "primary")
    primary_app = build_app(
        p_settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    r_settings = _make_settings(tmp_path / "follower")
    follower_app = build_app(
        r_settings, node_id="follower", shard_id="shard0", role=NodeRole.FOLLOWER, ranges=ranges
    )

    rejoin_complete_called = False

    async with LifespanManager(primary_app):
        p_state = primary_app.state.node
        p_state.engine.apply(
            {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
        )
        p_state.engine.last_applied_seq = 1
        p_state.term = 1
        p_state.wal.append(
            seq=5, term=1, op="insert", doc_id=5, request_id="req-5", payload={"v": 5}
        )
        p_state.follower_state = ReplicaState.CATCHING_UP

        # Wrap or monitor transport
        async def tracking_app(scope, receive, send):
            nonlocal rejoin_complete_called
            if scope.get("type") == "http" and scope.get("path") == "/internal/rejoin/complete":
                rejoin_complete_called = True
            await primary_app(scope, receive, send)

        tracking_transport = ASGITransport(app=tracking_app)

        async with LifespanManager(follower_app):
            f_state = follower_app.state.node
            f_state.wal.append(
                seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1}
            )
            f_state.wal.append(
                seq=2, term=1, op="insert", doc_id=2, request_id="req-2", payload={"v": 2}
            )
            f_state.engine.apply(
                {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
            )
            f_state.engine.apply(
                {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": 2}}
            )
            f_state.committed_seq = 2

            client = AsyncClient(mounts={"http://primary": tracking_transport}, timeout=5.0)

            ok = await resync_with_primary(f_state, "primary", http_client=client)
            assert ok is False
            assert rejoin_complete_called is False
            assert p_state.follower_state != ReplicaState.SYNCED


# ─────────────────────────────────────────────────────────────────────────────
# 11. Snapshot Durability & Recovery Order Tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_snapshot_recovery_persists_snapshot_and_reconstructs_on_restart(
    tmp_path: Path,
) -> None:
    """Snapshot recovery persists snapshot.json so restart recovers docs, seq, and dedup."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    p_settings = _make_settings(tmp_path / "primary")
    primary_app = build_app(
        p_settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    f_dir = tmp_path / "follower"
    f_settings = _make_settings(f_dir)
    follower_app = build_app(
        f_settings, node_id="follower", shard_id="shard0", role=NodeRole.FOLLOWER, ranges=ranges
    )

    async with LifespanManager(primary_app):
        p_state = primary_app.state.node
        # Primary has snapshot up to seq 4 with multiple docs and dedup entry
        p_state.engine.apply(
            {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 100}}
        )
        p_state.engine.apply(
            {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": 200}}
        )
        p_state.engine.apply(
            {"seq": 3, "term": 1, "op": "insert", "doc_id": 3, "payload": {"v": 300}}
        )
        p_state.engine.apply(
            {"seq": 4, "term": 1, "op": "insert", "doc_id": 4, "payload": {"v": 400}}
        )
        p_state.engine.last_applied_seq = 4
        p_state.term = 1
        p_state.engine.dedup_store(
            "req-snap-4",
            DedupEntry(
                seq=4,
                outcome=WriteOutcome.COMMITTED,
                result={"outcome": WriteOutcome.COMMITTED, "seq": 4},
                term=1,
            ),
        )
        # WAL starts at seq 10 (compacted)
        # Primary WAL compacted up to its snapshot (it can never be compacted past
        # it): earlier entries are only available via snapshot resync.
        p_state.wal.reset(p_state.engine.last_applied_seq + 1)
        p_state.follower_state = ReplicaState.CATCHING_UP

        snap_file = f_dir / "shard0" / "snapshot.json"
        assert not snap_file.exists()

        async with LifespanManager(follower_app):
            f_state = follower_app.state.node
            f_state.wal.append(
                seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1}
            )
            f_state.wal.append(
                seq=2, term=1, op="insert", doc_id=2, request_id="req-2", payload={"v": 2}
            )
            f_state.engine.apply(
                {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
            )
            f_state.engine.apply(
                {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": 2}}
            )
            f_state.engine.last_applied_seq = 2
            f_state.committed_seq = 2
            f_state.term = 1

            client = AsyncClient(mounts={"http://primary": ASGITransport(app=primary_app)})
            ok = await resync_with_primary(f_state, "primary", http_client=client)
            assert ok is True

            # 1. Verify snapshot.json was durably saved on disk
            assert snap_file.exists()
            assert f_state.engine.last_applied_seq == 4
            assert f_state.committed_seq == 4
            assert p_state.follower_state == ReplicaState.SYNCED

    # 2. Restart follower from disk and verify full state recovery
    restarted_app = build_app(
        f_settings, node_id="follower", shard_id="shard0", role=NodeRole.FOLLOWER, ranges=ranges
    )
    async with LifespanManager(restarted_app):
        r_state = restarted_app.state.node
        # Documents recovered from snapshot
        assert r_state.engine.get(1) == {"v": 100}
        assert r_state.engine.get(2) == {"v": 200}
        assert r_state.engine.get(3) == {"v": 300}
        assert r_state.engine.get(4) == {"v": 400}
        # Sequence numbers preserved
        assert r_state.engine.last_applied_seq == 4
        assert r_state.committed_seq == 4
        # Deduplication state preserved
        dedup_entry = r_state.engine.dedup_check("req-snap-4")
        assert dedup_entry is not None
        assert dedup_entry.outcome == WriteOutcome.COMMITTED
        assert dedup_entry.seq == 4


@pytest.mark.asyncio
async def test_snapshot_persistence_failure_fails_closed_without_truncation(tmp_path: Path) -> None:
    """Failure during snapshot save fails closed without truncating WAL or syncing follower."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    p_settings = _make_settings(tmp_path / "primary")
    primary_app = build_app(
        p_settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    f_dir = tmp_path / "follower"
    f_settings = _make_settings(f_dir)
    follower_app = build_app(
        f_settings, node_id="follower", shard_id="shard0", role=NodeRole.FOLLOWER, ranges=ranges
    )

    async with LifespanManager(primary_app):
        p_state = primary_app.state.node
        p_state.engine.apply(
            {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
        )
        p_state.engine.apply(
            {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": 2}}
        )
        p_state.engine.apply(
            {"seq": 3, "term": 1, "op": "insert", "doc_id": 3, "payload": {"v": 3}}
        )
        p_state.engine.last_applied_seq = 3
        p_state.term = 1
        p_state.wal.append(
            seq=10, term=1, op="insert", doc_id=10, request_id="req-10", payload={"v": 10}
        )
        p_state.follower_state = ReplicaState.CATCHING_UP

        async with LifespanManager(follower_app):
            f_state = follower_app.state.node
            f_state.wal.append(
                seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1}
            )
            f_state.wal.append(
                seq=2, term=1, op="insert", doc_id=2, request_id="req-2", payload={"v": 2}
            )
            f_state.engine.apply(
                {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
            )
            f_state.engine.apply(
                {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": 2}}
            )
            f_state.engine.last_applied_seq = 2
            f_state.committed_seq = 2

            # Inject failure into snapshot_mgr.save
            def failing_save(*args: Any, **kwargs: Any) -> None:
                raise OSError("Disk write failed / simulated I/O error")

            assert f_state.snapshot_mgr is not None
            f_state.snapshot_mgr.save = failing_save  # type: ignore[method-assign]

            client = AsyncClient(mounts={"http://primary": ASGITransport(app=primary_app)})
            ok = await resync_with_primary(f_state, "primary", http_client=client)

            # Must fail closed
            assert ok is False
            # WAL must NOT be truncated
            assert f_state.wal.last_seq == 2
            # Engine must NOT have applied unpersisted snapshot
            assert f_state.engine.get(3) is None
            assert f_state.engine.last_applied_seq == 2
            # Primary must NOT mark follower as SYNCED
            assert p_state.follower_state != ReplicaState.SYNCED


@pytest.mark.asyncio
async def test_wal_truncation_occurs_only_after_durable_snapshot_save(tmp_path: Path) -> None:
    """Verify WAL is truncated strictly after durable snapshot persistence completes."""
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    p_settings = _make_settings(tmp_path / "primary")
    primary_app = build_app(
        p_settings,
        node_id="primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
        lease_expires_at=float("inf"),
    )

    f_dir = tmp_path / "follower"
    f_settings = _make_settings(f_dir)
    follower_app = build_app(
        f_settings, node_id="follower", shard_id="shard0", role=NodeRole.FOLLOWER, ranges=ranges
    )

    snap_file = f_dir / "shard0" / "snapshot.json"

    async with LifespanManager(primary_app):
        p_state = primary_app.state.node
        p_state.engine.apply(
            {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
        )
        p_state.engine.apply(
            {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": 2}}
        )
        p_state.engine.apply(
            {"seq": 3, "term": 1, "op": "insert", "doc_id": 3, "payload": {"v": 3}}
        )
        p_state.engine.last_applied_seq = 3
        p_state.term = 1
        # Primary WAL compacted up to its snapshot (it can never be compacted past
        # it): earlier entries are only available via snapshot resync.
        p_state.wal.reset(p_state.engine.last_applied_seq + 1)

        async with LifespanManager(follower_app):
            f_state = follower_app.state.node
            f_state.wal.append(
                seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1}
            )
            f_state.wal.append(
                seq=2, term=1, op="insert", doc_id=2, request_id="req-2", payload={"v": 2}
            )
            f_state.engine.apply(
                {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
            )
            f_state.engine.apply(
                {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": 2}}
            )
            f_state.committed_seq = 2

            snapshot_existed_at_truncation = False
            # A snapshot install discards the local WAL via reset() (the snapshot supersedes
            # it); the discard must still happen only after the snapshot is durable.
            orig_reset = f_state.wal.reset

            def tracking_reset(next_seq: int) -> None:
                nonlocal snapshot_existed_at_truncation
                # Check that snapshot.json already exists and is readable when the WAL is reset
                if snap_file.exists() and snap_file.stat().st_size > 0:
                    snapshot_existed_at_truncation = True
                orig_reset(next_seq)

            f_state.wal.reset = tracking_reset  # type: ignore[method-assign]

            client = AsyncClient(mounts={"http://primary": ASGITransport(app=primary_app)})
            ok = await resync_with_primary(f_state, "primary", http_client=client)

            assert ok is True
            assert snapshot_existed_at_truncation is True
            assert snap_file.exists()


# ─────────────────────────────────────────────────────────────────────────────
# 6. Issue 1: Directory fsync error handling during snapshot resync
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_directory_fsync_failure_during_snapshot_resync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Directory fsync failure during snapshot resync:
    - Save reports failure.
    - WAL is not truncated.
    - Follower is not marked SYNCED.
    - Previous WAL remains sufficient for recovery.
    """
    primary_dir = tmp_path / "primary"
    follower_dir = tmp_path / "follower"
    p_settings = _make_settings(primary_dir)
    f_settings = _make_settings(follower_dir)

    primary_app = build_app(
        p_settings,
        node_id="primary",
        shard_id="shard1",
        role=NodeRole.PRIMARY,
        follower_url="follower",
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
        lease_expires_at=float("inf"),
    )
    follower_app = build_app(
        f_settings,
        node_id="follower",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )

    async with LifespanManager(primary_app):
        p_state = primary_app.state.node
        p_state.follower_state = ReplicaState.CATCHING_UP
        p_state.engine.apply(
            {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
        )
        p_state.engine.apply(
            {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": 2}}
        )
        p_state.engine.last_applied_seq = 2
        p_state.term = 1
        p_state.wal.append(
            seq=10, term=1, op="insert", doc_id=10, request_id="req-10", payload={"v": 10}
        )

        async with LifespanManager(follower_app):
            f_state = follower_app.state.node
            f_state.wal.append(
                seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1}
            )
            f_state.wal.append(
                seq=2, term=1, op="insert", doc_id=2, request_id="req-2", payload={"v": 2}
            )
            f_state.engine.apply(
                {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
            )
            f_state.engine.apply(
                {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": 2}}
            )
            f_state.committed_seq = 2

            # Simulate directory fsync failure on follower
            def mock_fsync_dir(d: Path) -> None:
                raise OSError("Simulated directory fsync failure on snapshot rename")

            monkeypatch.setattr("docstore.storage.snapshot._fsync_dir", mock_fsync_dir)

            client = AsyncClient(mounts={"http://primary": ASGITransport(app=primary_app)})
            ok = await resync_with_primary(f_state, "primary", http_client=client)

            # 1. Resync reports failure
            assert ok is False

            # 2. WAL is not truncated (both entries remain)
            wal_entries = list(f_state.wal.replay(after_seq=0))
            assert len(wal_entries) == 2
            assert [e["seq"] for e in wal_entries] == [1, 2]

            # 3. Follower is not marked SYNCED on primary
            assert p_state.follower_state != ReplicaState.SYNCED

            # 4. Engine was not modified
            assert f_state.engine.last_applied_seq == 2

    # 5. Verify the previous WAL remains sufficient for recovery after failed directory fsync
    recover_app = build_app(
        f_settings,
        node_id="follower-rec",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )
    async with LifespanManager(recover_app):
        rec_state = recover_app.state.node
        assert rec_state.ready is True
        assert rec_state.engine.last_applied_seq == 2
        assert rec_state.engine.get(1) == {"v": 1}
        assert rec_state.engine.get(2) == {"v": 2}


# ─────────────────────────────────────────────────────────────────────────────
# 7. Issue 2: Ownership Epochs Restoration & Range Reconciliation
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_snapshot_newer_ownership_epoch_updates_runtime_ranges(tmp_path: Path) -> None:
    """Snapshot contains newer ownership epoch than follower's existing range:
    runtime epoch is updated.
    """
    primary_dir = tmp_path / "primary"
    follower_dir = tmp_path / "follower"
    p_settings = _make_settings(primary_dir)
    f_settings = _make_settings(follower_dir)

    primary_app = build_app(
        p_settings,
        node_id="primary",
        shard_id="shard1",
        role=NodeRole.PRIMARY,
        follower_url="follower",
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=5)],
        lease_expires_at=float("inf"),
    )
    follower_app = build_app(
        f_settings,
        node_id="follower",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )

    async with LifespanManager(primary_app):
        p_state = primary_app.state.node
        p_state.engine.apply(
            {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
        )
        p_state.engine.apply(
            {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": 2}}
        )
        p_state.engine.last_applied_seq = 2
        # Primary WAL compacted up to its snapshot (it can never be compacted past
        # it): earlier entries are only available via snapshot resync.
        p_state.wal.reset(p_state.engine.last_applied_seq + 1)

        async with LifespanManager(follower_app):
            f_state = follower_app.state.node
            f_state.wal.append(
                seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1}
            )
            f_state.engine.apply(
                {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
            )
            f_state.committed_seq = 1
            assert f_state.ranges[0].ownership_epoch == 1

            client = AsyncClient(mounts={"http://primary": ASGITransport(app=primary_app)})
            ok = await resync_with_primary(f_state, "primary", http_client=client)

            assert ok is True
            # Runtime state.ranges must reflect authoritative snapshot epoch (5)
            assert f_state.ranges[0].ownership_epoch == 5

            # Snapshot on disk must reflect epoch 5
            snap = f_state.snapshot_mgr.load()
            assert snap is not None
            assert snap["ownership_epochs"] == {"0-1000": 5}

            # Node metadata must reflect epoch 5
            meta_file = follower_dir / "shard1" / "node_metadata.json"
            assert meta_file.exists()
            meta = json.loads(meta_file.read_text())
            assert meta["ranges"][0]["ownership_epoch"] == 5


@pytest.mark.asyncio
async def test_restart_after_snapshot_recovery_correct_epoch_restored(tmp_path: Path) -> None:
    """Restart after snapshot recovery: correct epoch is restored."""
    follower_dir = tmp_path / "follower"
    f_settings = _make_settings(follower_dir)

    # Initial boot: follower gets snapshot with epoch=4
    app1 = build_app(
        f_settings,
        node_id="follower",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )
    async with LifespanManager(app1):
        s1 = app1.state.node
        # Durably save snapshot with authoritative epoch=4
        s1.snapshot_mgr.save(
            documents={1: {"doc_id": 1, "val": "v1"}},
            last_applied_seq=1,
            term=2,
            ownership_epochs={"0-1000": 4},
            dedup_table={},
        )
        # Reconcile runtime ranges
        s1.ranges[0].ownership_epoch = 4

    # Restart node: configured with initial epoch=1, but disk has snapshot with epoch=4
    app2 = build_app(
        f_settings,
        node_id="follower",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )
    async with LifespanManager(app2):
        s2 = app2.state.node
        assert s2.ready is True
        # Must restore epoch=4 into runtime ranges despite initial config having 1
        assert s2.ranges[0].ownership_epoch == 4


@pytest.mark.asyncio
async def test_snapshot_incompatible_range_mapping_fails_closed(tmp_path: Path) -> None:
    """Snapshot range mapping is incompatible: recovery and resync fail closed."""
    # Subcase A: resync_with_primary with incompatible ranges
    primary_dir = tmp_path / "primary"
    follower_dir = tmp_path / "follower"
    p_settings = _make_settings(primary_dir)
    f_settings = _make_settings(follower_dir)

    # Primary owns [0, 500) while Follower is configured for [0, 1000)
    primary_app = build_app(
        p_settings,
        node_id="primary",
        shard_id="shard1",
        role=NodeRole.PRIMARY,
        follower_url="follower",
        ranges=[ShardRangeConfig(start=0, end=500, ownership_epoch=2)],
        lease_expires_at=float("inf"),
    )
    follower_app = build_app(
        f_settings,
        node_id="follower",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )

    async with LifespanManager(primary_app):
        p_state = primary_app.state.node
        p_state.engine.apply(
            {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
        )
        p_state.engine.apply(
            {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": 2}}
        )
        p_state.engine.last_applied_seq = 2
        p_state.wal.append(
            seq=10, term=1, op="insert", doc_id=10, request_id="req-10", payload={"v": 10}
        )

        async with LifespanManager(follower_app):
            f_state = follower_app.state.node
            f_state.wal.append(
                seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1}
            )
            f_state.engine.apply(
                {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
            )
            f_state.committed_seq = 1

            client = AsyncClient(mounts={"http://primary": ASGITransport(app=primary_app)})
            ok = await resync_with_primary(f_state, "primary", http_client=client)

            # Resync must fail closed!
            assert ok is False
            # Follower ranges must remain intact
            assert f_state.ranges[0].start == 0
            assert f_state.ranges[0].end == 1000
            assert f_state.ranges[0].ownership_epoch == 1

    # Subcase B: direct recovery with incompatible snapshot range mapping fails closed
    node_dir = tmp_path / "incompatible_node"
    node_settings = _make_settings(node_dir)
    data_dir = node_dir / "shard1"
    data_dir.mkdir(parents=True, exist_ok=True)
    snap_mgr = SnapshotManager(data_dir)
    snap_mgr.save(
        documents={1: {"doc_id": 1}},
        last_applied_seq=1,
        term=1,
        ownership_epochs={"0-500": 2},  # snapshot has [0, 500)
        dedup_table={},
    )

    node_state = NodeState(node_settings, node_id="test-node", shard_id="shard1")
    node_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    # _recover must fail closed (raise ValueError)
    with pytest.raises(ValueError, match=r"Incompatible range mapping"):
        await _recover(node_state)


@pytest.mark.asyncio
async def test_existing_correct_epoch_is_preserved(tmp_path: Path) -> None:
    """Existing correct epoch is preserved; stale snapshot epoch is not accepted."""
    node_dir = tmp_path / "node_preserve"
    node_settings = _make_settings(node_dir)
    data_dir = node_dir / "shard1"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Snapshot on disk has older epoch (2)
    snap_mgr = SnapshotManager(data_dir)
    snap_mgr.save(
        documents={1: {"doc_id": 1, "v": 1}},
        last_applied_seq=1,
        term=1,
        ownership_epochs={"0-1000": 2},
        dedup_table={},
    )

    # Local configured range / metadata has newer epoch (5)
    node_state = NodeState(node_settings, node_id="test-node", shard_id="shard1")
    node_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=5)]

    await _recover(node_state)

    # Existing correct epoch (5) must be preserved!
    assert node_state.ranges[0].ownership_epoch == 5


@pytest.mark.asyncio
async def test_ownership_epochs_consistent_between_snapshot_runtime_and_metadata(
    tmp_path: Path,
) -> None:
    """Ownership epochs remain consistent between snapshot, runtime state, and node metadata."""
    primary_dir = tmp_path / "primary"
    follower_dir = tmp_path / "follower"
    p_settings = _make_settings(primary_dir)
    f_settings = _make_settings(follower_dir)

    primary_app = build_app(
        p_settings,
        node_id="primary",
        shard_id="shard1",
        role=NodeRole.PRIMARY,
        follower_url="follower",
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=7)],
        lease_expires_at=float("inf"),
    )
    follower_app = build_app(
        f_settings,
        node_id="follower",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )

    async with LifespanManager(primary_app):
        p_state = primary_app.state.node
        p_state.engine.apply(
            {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
        )
        p_state.engine.apply(
            {"seq": 2, "term": 1, "op": "insert", "doc_id": 2, "payload": {"v": 2}}
        )
        p_state.engine.last_applied_seq = 2
        # Primary WAL compacted up to its snapshot (it can never be compacted past
        # it): earlier entries are only available via snapshot resync.
        p_state.wal.reset(p_state.engine.last_applied_seq + 1)

        async with LifespanManager(follower_app):
            f_state = follower_app.state.node
            f_state.wal.append(
                seq=1, term=1, op="insert", doc_id=1, request_id="req-1", payload={"v": 1}
            )
            f_state.engine.apply(
                {"seq": 1, "term": 1, "op": "insert", "doc_id": 1, "payload": {"v": 1}}
            )
            f_state.committed_seq = 1

            client = AsyncClient(mounts={"http://primary": ASGITransport(app=primary_app)})
            ok = await resync_with_primary(f_state, "primary", http_client=client)
            assert ok is True

            # 1. Runtime state
            runtime_epoch = f_state.ranges[0].ownership_epoch

            # 2. Persisted snapshot
            snap = f_state.snapshot_mgr.load()
            assert snap is not None
            snap_epoch = snap["ownership_epochs"]["0-1000"]

            # 3. Node metadata
            meta_file = follower_dir / "shard1" / "node_metadata.json"
            meta = json.loads(meta_file.read_text())
            meta_epoch = meta["ranges"][0]["ownership_epoch"]

            # All three must be strictly consistent and equal to 7
            assert runtime_epoch == 7
            assert snap_epoch == 7
            assert meta_epoch == 7


# ─────────────────────────────────────────────────────────────────────────────
# 8. Ownership-Epoch Recovery Regression Tests (Prompt 1D: Tests A-F)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_recovery_epoch_reconciliation_metadata_highest(tmp_path: Path) -> None:
    """Test A: Persisted metadata epoch 9, configured epoch 1, snapshot epoch 5.
    Expected runtime epoch = 9.
    """
    node_dir = tmp_path / "node_a"
    settings = _make_settings(node_dir)
    data_dir = node_dir / "shard1"
    data_dir.mkdir(parents=True, exist_ok=True)

    # 1. Persisted metadata on disk with epoch 9
    meta_path = data_dir / "node_metadata.json"
    meta_path.write_text(
        json.dumps(
            {
                "term": 1,
                "role": "follower",
                "committed_seq": 1,
                "ranges": [{"start": 0, "end": 1000, "ownership_epoch": 9}],
            }
        )
    )

    # 2. Persisted snapshot on disk with epoch 5
    snap_mgr = SnapshotManager(data_dir)
    snap_mgr.save(
        documents={1: {"doc_id": 1, "v": 1}},
        last_applied_seq=1,
        term=1,
        ownership_epochs={"0-1000": 5},
        dedup_table={},
    )

    # 3. Startup configuration supplies epoch 1
    app = build_app(
        settings,
        node_id="node-a",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )
    async with LifespanManager(app):
        state = app.state.node
        assert state.ready is True
        assert state.ranges[0].ownership_epoch == 9

        # Ensure node_metadata.json preserves epoch 9
        meta = json.loads(meta_path.read_text())
        assert meta["ranges"][0]["ownership_epoch"] == 9


@pytest.mark.asyncio
async def test_recovery_epoch_reconciliation_snapshot_highest(tmp_path: Path) -> None:
    """Test B: Persisted metadata epoch 9, configured epoch 1, snapshot epoch 12.
    Expected runtime epoch = 12.
    """
    node_dir = tmp_path / "node_b"
    settings = _make_settings(node_dir)
    data_dir = node_dir / "shard1"
    data_dir.mkdir(parents=True, exist_ok=True)

    # 1. Persisted metadata with epoch 9
    meta_path = data_dir / "node_metadata.json"
    meta_path.write_text(
        json.dumps(
            {
                "term": 1,
                "role": "follower",
                "committed_seq": 1,
                "ranges": [{"start": 0, "end": 1000, "ownership_epoch": 9}],
            }
        )
    )

    # 2. Persisted snapshot with epoch 12
    snap_mgr = SnapshotManager(data_dir)
    snap_mgr.save(
        documents={1: {"doc_id": 1, "v": 1}},
        last_applied_seq=1,
        term=1,
        ownership_epochs={"0-1000": 12},
        dedup_table={},
    )

    # 3. Startup configuration with epoch 1
    app = build_app(
        settings,
        node_id="node-b",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )
    async with LifespanManager(app):
        state = app.state.node
        assert state.ready is True
        assert state.ranges[0].ownership_epoch == 12

        # Metadata should be updated to epoch 12
        meta = json.loads(meta_path.read_text())
        assert meta["ranges"][0]["ownership_epoch"] == 12


@pytest.mark.asyncio
async def test_recovery_epoch_reconciliation_configured_highest(tmp_path: Path) -> None:
    """Test C: Persisted metadata epoch 9, configured epoch 12, snapshot epoch 5.
    Expected runtime epoch = 12.
    """
    node_dir = tmp_path / "node_c"
    settings = _make_settings(node_dir)
    data_dir = node_dir / "shard1"
    data_dir.mkdir(parents=True, exist_ok=True)

    # 1. Persisted metadata with epoch 9
    meta_path = data_dir / "node_metadata.json"
    meta_path.write_text(
        json.dumps(
            {
                "term": 1,
                "role": "follower",
                "committed_seq": 1,
                "ranges": [{"start": 0, "end": 1000, "ownership_epoch": 9}],
            }
        )
    )

    # 2. Persisted snapshot with epoch 5
    snap_mgr = SnapshotManager(data_dir)
    snap_mgr.save(
        documents={1: {"doc_id": 1, "v": 1}},
        last_applied_seq=1,
        term=1,
        ownership_epochs={"0-1000": 5},
        dedup_table={},
    )

    # 3. Startup configuration with epoch 12
    app = build_app(
        settings,
        node_id="node-c",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=12)],
    )
    async with LifespanManager(app):
        state = app.state.node
        assert state.ready is True
        assert state.ranges[0].ownership_epoch == 12

        meta = json.loads(meta_path.read_text())
        assert meta["ranges"][0]["ownership_epoch"] == 12


@pytest.mark.asyncio
async def test_recovery_incompatible_metadata_range_mapping_fails_closed(
    tmp_path: Path,
) -> None:
    """Test D: Incompatible persisted metadata range mapping: recovery must fail closed."""
    node_dir = tmp_path / "node_d"
    settings = _make_settings(node_dir)
    data_dir = node_dir / "shard1"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Persisted metadata has [0, 500)
    meta_path = data_dir / "node_metadata.json"
    meta_path.write_text(
        json.dumps(
            {
                "term": 1,
                "role": "follower",
                "committed_seq": 1,
                "ranges": [{"start": 0, "end": 500, "ownership_epoch": 9}],
            }
        )
    )

    # Configured range is [0, 1000)
    app = build_app(
        settings,
        node_id="node-d",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )
    async with LifespanManager(app):
        state = app.state.node
        # Recovery must fail closed; node must not become ready
        assert state.ready is False

    # Also test direct _recover call raises ValueError
    node_state = NodeState(settings, node_id="node-d-direct", shard_id="shard1")
    node_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    with pytest.raises(ValueError, match=r"Incompatible range mapping.*metadata"):
        await _recover(node_state)


@pytest.mark.asyncio
async def test_recovery_metadata_directory_fsync_failure_not_ready(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test E: Directory fsync failure while persisting reconciled node metadata:
    Recovery must not report successful readiness.
    """
    node_dir = tmp_path / "node_e"
    settings = _make_settings(node_dir)
    data_dir = node_dir / "shard1"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Setup valid metadata and snapshot
    meta_path = data_dir / "node_metadata.json"
    meta_path.write_text(
        json.dumps(
            {
                "term": 1,
                "role": "follower",
                "committed_seq": 0,  # nothing committed yet: no WAL/snapshot needed
                "ranges": [{"start": 0, "end": 1000, "ownership_epoch": 9}],
            }
        )
    )

    # Monkeypatch os.fsync to raise OSError when called on directory fd
    orig_open = os.open
    dir_fds: set[int] = set()

    def tracking_open(path: str | bytes | int | Path, flags: int, *args: Any, **kwargs: Any) -> int:
        fd = orig_open(str(path), flags, *args, **kwargs)
        if str(path) == str(data_dir):
            dir_fds.add(fd)
        return fd

    orig_fsync = os.fsync

    def failing_fsync(fd: int) -> None:
        if fd in dir_fds:
            raise OSError("Simulated metadata directory fsync failure")
        orig_fsync(fd)

    monkeypatch.setattr(os, "open", tracking_open)
    monkeypatch.setattr(os, "fsync", failing_fsync)

    app = build_app(
        settings,
        node_id="node-e",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )
    async with LifespanManager(app):
        state = app.state.node
        # Recovery must fail; node must not report readiness!
        assert state.ready is False

    # Also test direct _recover call raises OSError
    node_state = NodeState(settings, node_id="node-e-direct", shard_id="shard1")
    node_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
    with pytest.raises(OSError, match="Simulated metadata directory fsync failure"):
        await _recover(node_state)


@pytest.mark.asyncio
async def test_recovery_highest_epoch_survives_another_restart(tmp_path: Path) -> None:
    """Test F: Restart after successful reconciliation:
    The highest epoch must survive another restart.
    """
    node_dir = tmp_path / "node_f"
    settings = _make_settings(node_dir)
    data_dir = node_dir / "shard1"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Initial state on disk: metadata has epoch 9, snapshot has epoch 5
    meta_path = data_dir / "node_metadata.json"
    meta_path.write_text(
        json.dumps(
            {
                "term": 1,
                "role": "follower",
                "committed_seq": 1,
                "ranges": [{"start": 0, "end": 1000, "ownership_epoch": 9}],
            }
        )
    )
    snap_mgr = SnapshotManager(data_dir)
    snap_mgr.save(
        documents={1: {"doc_id": 1, "v": 1}},
        last_applied_seq=1,
        term=1,
        ownership_epochs={"0-1000": 5},
        dedup_table={},
    )

    # First start: configured with epoch 1 -> reconciles to max(1, 9, 5) = 9
    app1 = build_app(
        settings,
        node_id="node-f",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )
    async with LifespanManager(app1):
        s1 = app1.state.node
        assert s1.ready is True
        assert s1.ranges[0].ownership_epoch == 9

    # Reconciled metadata was persisted with epoch 9.
    meta1 = json.loads(meta_path.read_text())
    assert meta1["ranges"][0]["ownership_epoch"] == 9

    # Second start (another restart): again configured with epoch 1
    app2 = build_app(
        settings,
        node_id="node-f",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )
    async with LifespanManager(app2):
        s2 = app2.state.node
        assert s2.ready is True
        # Highest epoch 9 must survive!
        assert s2.ranges[0].ownership_epoch == 9


@pytest.mark.asyncio
async def test_recovery_epoch_reconciliation_metadata_15_config_2_snapshot_8(
    tmp_path: Path,
) -> None:
    """Test G: Metadata epoch 15, configuration epoch 2, snapshot epoch 8.
    Expected final epoch: 15.
    """
    node_dir = tmp_path / "node_g"
    settings = _make_settings(node_dir)
    data_dir = node_dir / "shard1"
    data_dir.mkdir(parents=True, exist_ok=True)

    # 1. Persisted metadata with epoch 15
    meta_path = data_dir / "node_metadata.json"
    meta_path.write_text(
        json.dumps(
            {
                "term": 1,
                "role": "follower",
                "committed_seq": 1,
                "ranges": [{"start": 0, "end": 1000, "ownership_epoch": 15}],
            }
        )
    )

    # 2. Persisted snapshot with epoch 8
    snap_mgr = SnapshotManager(data_dir)
    snap_mgr.save(
        documents={1: {"doc_id": 1, "v": 1}},
        last_applied_seq=1,
        term=1,
        ownership_epochs={"0-1000": 8},
        dedup_table={},
    )

    # 3. Startup configuration supplies epoch 2
    app = build_app(
        settings,
        node_id="node-g",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=2)],
    )
    async with LifespanManager(app):
        state = app.state.node
        assert state.ready is True
        assert state.ranges[0].ownership_epoch == 15

        # Metadata should persist reconciled epoch 15
        meta = json.loads(meta_path.read_text())
        assert meta["ranges"][0]["ownership_epoch"] == 15


@pytest.mark.asyncio
async def test_recovery_multiple_ranges_independent_reconciliation(tmp_path: Path) -> None:
    """Test H: At least two different ranges with different epoch values.
    Verify independent reconciliation per logical range.
    """
    node_dir = tmp_path / "node_h"
    settings = _make_settings(node_dir)
    data_dir = node_dir / "shard1"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Range 1 [0, 500): meta=10, snap=4, config=2 -> final 10
    # Range 2 [500, 1000): meta=3, snap=12, config=7 -> final 12
    meta_path = data_dir / "node_metadata.json"
    meta_path.write_text(
        json.dumps(
            {
                "term": 1,
                "role": "follower",
                "committed_seq": 1,
                "ranges": [
                    {"start": 0, "end": 500, "ownership_epoch": 10},
                    {"start": 500, "end": 1000, "ownership_epoch": 3},
                ],
            }
        )
    )

    snap_mgr = SnapshotManager(data_dir)
    snap_mgr.save(
        documents={1: {"doc_id": 1, "v": 1}, 600: {"doc_id": 600, "v": 600}},
        last_applied_seq=1,
        term=1,
        ownership_epochs={"0-500": 4, "500-1000": 12},
        dedup_table={},
    )

    app = build_app(
        settings,
        node_id="node-h",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[
            ShardRangeConfig(start=0, end=500, ownership_epoch=2),
            ShardRangeConfig(start=500, end=1000, ownership_epoch=7),
        ],
    )
    async with LifespanManager(app):
        state = app.state.node
        assert state.ready is True
        assert len(state.ranges) == 2

        # Verify independent reconciliation
        r0 = next(r for r in state.ranges if r.start == 0 and r.end == 500)
        r1 = next(r for r in state.ranges if r.start == 500 and r.end == 1000)
        assert r0.ownership_epoch == 10
        assert r1.ownership_epoch == 12

        # Verify durable metadata reflects both independent epochs
        meta = json.loads(meta_path.read_text())
        m_r0 = next(r for r in meta["ranges"] if r["start"] == 0 and r["end"] == 500)
        m_r1 = next(r for r in meta["ranges"] if r["start"] == 500 and r["end"] == 1000)
        assert m_r0["ownership_epoch"] == 10
        assert m_r1["ownership_epoch"] == 12


@pytest.mark.asyncio
async def test_recovery_corrupted_metadata_fails_safe_and_not_ready(tmp_path: Path) -> None:
    """Test I: Corrupted or malformed node_metadata.json.
    Verify safe failure and that the node cannot become READY.
    """
    node_dir = tmp_path / "node_i"
    settings = _make_settings(node_dir)
    data_dir = node_dir / "shard1"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Write corrupt (non-JSON) data to node_metadata.json
    meta_path = data_dir / "node_metadata.json"
    meta_path.write_bytes(b"INVALID_JSON_CORRUPTED{{{")

    app = build_app(
        settings,
        node_id="node-i",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=1)],
    )

    async with LifespanManager(app):
        state = app.state.node
        # Node must not become ready
        assert state.ready is False

        # /health endpoint must return 503 NODE_NOT_READY
        client = AsyncClient(transport=ASGITransport(app=app), base_url="http://node")
        resp = await client.get("/health")
        assert resp.status_code == 503
        data = resp.json()
        assert data.get("error_code") == "NODE_NOT_READY"

    # Direct _recover call raises RuntimeError
    direct_state = NodeState(settings, node_id="node-i-direct", shard_id="shard1")
    with pytest.raises(RuntimeError, match=r"Corrupt or unreadable node metadata"):
        await _recover(direct_state)


@pytest.mark.asyncio
async def test_recovery_metadata_persistence_fsync_atomic_replacement_and_reload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test J: Successful metadata persistence, including file fsync, atomic replacement,
    directory fsync, and successful reload.
    """
    import builtins

    node_dir = tmp_path / "node_j"
    settings = _make_settings(node_dir)
    data_dir = node_dir / "shard1"
    data_dir.mkdir(parents=True, exist_ok=True)

    call_sequence: list[str] = []
    tmp_file_path = data_dir / "node_metadata.json.tmp"
    meta_file_path = data_dir / "node_metadata.json"
    active_tmp_fd: int | None = None
    active_dir_fd: int | None = None

    orig_builtin_open = builtins.open

    def tracking_builtin_open(file: Any, *args: Any, **kwargs: Any) -> Any:
        f = orig_builtin_open(file, *args, **kwargs)
        if str(file) == str(tmp_file_path):
            nonlocal active_tmp_fd
            active_tmp_fd = f.fileno()
            call_sequence.append("open_tmp")
        return f

    orig_open = os.open

    def tracking_open(path: str | bytes | int | Path, flags: int, *args: Any, **kwargs: Any) -> int:
        fd = orig_open(str(path), flags, *args, **kwargs)
        if str(path) == str(data_dir):
            nonlocal active_dir_fd
            active_dir_fd = fd
            call_sequence.append("open_dir")
        return fd

    orig_fsync = os.fsync

    def tracking_fsync(fd: int) -> None:
        if active_dir_fd is not None and fd == active_dir_fd:
            call_sequence.append("fsync_dir")
        elif active_tmp_fd is not None and fd == active_tmp_fd:
            call_sequence.append("fsync_file")
        orig_fsync(fd)

    orig_close = os.close

    def tracking_close(fd: int) -> None:
        nonlocal active_dir_fd
        if active_dir_fd is not None and fd == active_dir_fd:
            active_dir_fd = None
        orig_close(fd)

    orig_replace = os.replace

    def tracking_replace(src: str | bytes | Path, dst: str | bytes | Path) -> None:
        if str(src) == str(tmp_file_path) and str(dst) == str(meta_file_path):
            nonlocal active_tmp_fd
            active_tmp_fd = None
            call_sequence.append("atomic_replace")
        orig_replace(str(src), str(dst))

    monkeypatch.setattr(builtins, "open", tracking_builtin_open)
    monkeypatch.setattr(os, "open", tracking_open)
    monkeypatch.setattr(os, "fsync", tracking_fsync)
    monkeypatch.setattr(os, "close", tracking_close)
    monkeypatch.setattr(os, "replace", tracking_replace)

    app = build_app(
        settings,
        node_id="node-j",
        shard_id="shard1",
        role=NodeRole.PRIMARY,
        ranges=[ShardRangeConfig(start=0, end=1000, ownership_epoch=5)],
        lease_expires_at=float("inf"),
    )

    async with LifespanManager(app):
        state = app.state.node
        assert state.ready is True

        # Verify operation sequence:
        # 1. open_tmp
        # 2. fsync_file (file fsync before rename)
        # 3. atomic_replace (atomic replacement after file fsync)
        # 4. open_dir + fsync_dir (directory fsync after replacement)
        assert "open_tmp" in call_sequence
        assert "fsync_file" in call_sequence
        assert "atomic_replace" in call_sequence
        assert "open_dir" in call_sequence
        assert "fsync_dir" in call_sequence

        idx_fsync_file = call_sequence.index("fsync_file")
        idx_replace = call_sequence.index("atomic_replace")
        idx_fsync_dir = call_sequence.index("fsync_dir")

        assert idx_fsync_file < idx_replace, "File fsync must happen before atomic replacement"
        assert idx_replace < idx_fsync_dir, "Directory fsync must happen after atomic replacement"

    # Reload check: verify node_metadata.json on disk can be loaded and matches expected state
    assert meta_file_path.exists()
    reloaded_meta = json.loads(meta_file_path.read_text())
    assert reloaded_meta["ranges"][0]["ownership_epoch"] == 5
    assert reloaded_meta["ranges"][0]["start"] == 0
    assert reloaded_meta["ranges"][0]["end"] == 1000

    # Restart another node instance from the persisted metadata to verify complete reload
    app_reloaded = build_app(
        settings,
        node_id="node-j-reloaded",
        shard_id="shard1",
        role=NodeRole.FOLLOWER,
    )
    async with LifespanManager(app_reloaded):
        reloaded_state = app_reloaded.state.node
        assert reloaded_state.ready is True
        assert len(reloaded_state.ranges) == 1
        assert reloaded_state.ranges[0].ownership_epoch == 5
