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
from pathlib import Path
from typing import Any

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from docstore.common.auth import CLUSTER_SIG_HEADER, compute_signature
from docstore.common.config import Settings
from docstore.common.models import NodeRole, ReplicaState
from docstore.shard.app import ShardRangeConfig, build_app, resync_with_primary
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
        settings, node_id="primary", shard_id="shard0", role=NodeRole.PRIMARY, ranges=ranges
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
        settings, node_id="primary", shard_id="shard0", role=NodeRole.PRIMARY, ranges=ranges
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
        settings, node_id="primary", shard_id="shard0", role=NodeRole.PRIMARY, ranges=ranges
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
        settings, node_id="primary", shard_id="shard0", role=NodeRole.PRIMARY, ranges=ranges
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
        settings, node_id="primary", shard_id="shard0", role=NodeRole.PRIMARY, ranges=ranges
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
        f_settings, node_id="new-primary", shard_id="shard0", role=NodeRole.PRIMARY, ranges=ranges
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
        f_settings, node_id="primary", shard_id="shard0", role=NodeRole.PRIMARY, ranges=ranges
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
