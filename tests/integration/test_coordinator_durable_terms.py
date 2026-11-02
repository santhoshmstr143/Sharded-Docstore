"""Regression test suite for Coordinator durable term persistence and stale primary fencing.

Verifies:
- Test A: Term survives coordinator restart (advances from 1 to 2, survives restart).
- Test B: Old primary rejected after quarantine (term 1 heartbeat rejected after restart & quarantine).
- Test C: New primary remains authorized (B at term 2 registers/renews and retains authority).
- Test D: Unknown node cannot claim authority via heartbeat after quarantine.
- Test E: Delayed old-term heartbeat rejected and never restores authority.
- Test F: Repeated coordinator restarts preserve monotonically increasing terms.
- Test G: Durable persistence failure prevents promotion and new primary activation.
- Test H: Normal operation regression (registration, heartbeat renewal, failover, rejoin).
- Invariant: Only the currently authorized primary at current durable term can receive a valid primary lease.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pytest
from httpx import AsyncClient

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


def _make_settings(
    tmp_path: Path,
    **overrides: Any,
) -> Settings:
    base: dict[str, Any] = {
        "CLUSTER_SECRET": "c" * 32,
        "ADMIN_TOKEN": "a" * 32,
        "CLIENT_API_KEY": "k" * 16,
        "DATA_DIR": str(tmp_path),
        "SHARD_MAP_PATH": str(tmp_path / "shard_map.json"),
        "HEARTBEAT_INTERVAL_S": 0.1,
        "LEASE_DURATION_S": 1.0,
        "FAILURE_TIMEOUT_S": 3.0,
        "SAFETY_MARGIN_S": 0.1,
        "REPLICATION_TIMEOUT_S": 0.1,
        "ENABLE_TEST_HOOKS": True,
    }
    base.update(overrides)
    return Settings.model_validate(base)


def _bootstrap_cluster(tmp_path: Path, settings: Settings) -> CoordinatorService:
    map_path = tmp_path / "shard_map.json"
    coord = CoordinatorService(settings, map_path=map_path)
    initial_map = ShardMap(
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
    coord.map_mgr.save(initial_map)
    return coord


class MockTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok", "message": "success"})


# ─────────────────────────────────────────────────────────────────────────────
# Test A — Term survives coordinator restart
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_a_term_survives_coordinator_restart(tmp_path: Path):
    """Test A:
    1. Start coordinator.
    2. Promote a follower, advancing term from 1 to 2.
    3. Shut down coordinator.
    4. Recreate coordinator using the same persistent data directory.
    5. Verify recovered term is still 2.
    """
    settings = _make_settings(tmp_path)
    coord1 = _bootstrap_cluster(tmp_path, settings)
    assert coord1.shard_terms["shard0"] == 1

    # Register initial primary A and follower B
    coord1.register_node(
        NodeRegistration(
            node_id="nodeA",
            shard_id="shard0",
            address="http://127.0.0.1:8200",
            role=NodeRole.PRIMARY,
            term=1,
        )
    )
    coord1.register_node(
        NodeRegistration(
            node_id="nodeB",
            shard_id="shard0",
            address="http://127.0.0.1:8201",
            role=NodeRole.FOLLOWER,
            term=1,
        )
    )

    # Promote follower B via failover, advancing term from 1 to 2
    async with AsyncClient(transport=MockTransport(), base_url="http://127.0.0.1:8201") as client:
        ok = await coord1.trigger_failover("shard0", http_client=client)
        assert ok is True

    assert coord1.shard_terms["shard0"] == 2
    assert (tmp_path / "terms.json").exists()

    # Shut down coordinator and recreate using the same persistent directory
    del coord1

    coord2 = CoordinatorService(settings, map_path=tmp_path / "shard_map.json")
    assert coord2.shard_terms["shard0"] == 2


# ─────────────────────────────────────────────────────────────────────────────
# Test B — Old primary rejected after quarantine
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_b_old_primary_rejected_after_quarantine(tmp_path: Path):
    """Test B:
    1. Start with primary A at term 1.
    2. Fail over to primary B at term 2.
    3. Restart coordinator.
    4. Wait until quarantine expires.
    5. Send a heartbeat from old primary A at term 1.
    6. Verify it is rejected.
    7. Verify no lease is issued to A.
    """
    settings = _make_settings(tmp_path, LEASE_DURATION_S=0.6, SAFETY_MARGIN_S=0.1)
    coord1 = _bootstrap_cluster(tmp_path, settings)

    coord1.register_node(
        NodeRegistration(
            node_id="nodeA",
            shard_id="shard0",
            address="http://127.0.0.1:8200",
            role=NodeRole.PRIMARY,
            term=1,
        )
    )
    coord1.register_node(
        NodeRegistration(
            node_id="nodeB",
            shard_id="shard0",
            address="http://127.0.0.1:8201",
            role=NodeRole.FOLLOWER,
            term=1,
        )
    )

    # Fail over to primary B at term 2
    async with AsyncClient(transport=MockTransport(), base_url="http://127.0.0.1:8201") as client:
        ok = await coord1.trigger_failover("shard0", http_client=client)
        assert ok is True

    assert coord1.shard_terms["shard0"] == 2

    # Restart coordinator
    del coord1
    coord2 = CoordinatorService(settings, map_path=tmp_path / "shard_map.json")
    assert coord2.shard_terms["shard0"] == 2

    # Wait until recovery quarantine expires
    wait_time = coord2._recovery_quarantine_until_monotonic - time.monotonic() + 0.1
    if wait_time > 0:
        await asyncio.sleep(wait_time)
    assert time.monotonic() > coord2._recovery_quarantine_until_monotonic

    # Send heartbeat from old primary A at term 1
    hb_old = Heartbeat(
        node_id="nodeA",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=1,
        last_applied_seq=10,
        follower_acked_seq=10,
        follower_state=ReplicaState.SYNCED,
        timestamp_monotonic=time.monotonic(),
    )

    # Must be rejected (StaleTerm)
    with pytest.raises(StaleTerm):
        coord2.handle_heartbeat(hb_old)

    # Verify no lease is issued to A
    assert coord2.leases.get("shard0") is None or coord2.leases["shard0"].node_id != "nodeA"

    # Even if A registered as follower after restart and tries to heartbeat as PRIMARY:
    coord2.register_node(
        NodeRegistration(
            node_id="nodeA",
            shard_id="shard0",
            address="http://127.0.0.1:8200",
            role=NodeRole.FOLLOWER,
            term=2,
        )
    )
    # A sending heartbeat at stale term 1
    with pytest.raises(StaleTerm):
        coord2.handle_heartbeat(hb_old)

    # A sending heartbeat claiming PRIMARY at term 2
    hb_claim = Heartbeat(
        node_id="nodeA",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=2,
        last_applied_seq=10,
        follower_acked_seq=10,
        follower_state=ReplicaState.SYNCED,
        timestamp_monotonic=time.monotonic(),
    )
    grant = coord2.handle_heartbeat(hb_claim)
    assert grant is None
    assert coord2.leases.get("shard0") is None or coord2.leases["shard0"].node_id != "nodeA"


# ─────────────────────────────────────────────────────────────────────────────
# Test C — New primary remains authorized
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_c_new_primary_remains_authorized(tmp_path: Path):
    """Test C:
    After the same restart, verify B at term 2 can register/renew correctly and retain authority.
    """
    settings = _make_settings(tmp_path, LEASE_DURATION_S=0.6, SAFETY_MARGIN_S=0.1)
    coord1 = _bootstrap_cluster(tmp_path, settings)

    coord1.register_node(
        NodeRegistration(
            node_id="nodeA",
            shard_id="shard0",
            address="http://127.0.0.1:8200",
            role=NodeRole.PRIMARY,
            term=1,
        )
    )
    coord1.register_node(
        NodeRegistration(
            node_id="nodeB",
            shard_id="shard0",
            address="http://127.0.0.1:8201",
            role=NodeRole.FOLLOWER,
            term=1,
        )
    )

    async with AsyncClient(transport=MockTransport(), base_url="http://127.0.0.1:8201") as client:
        ok = await coord1.trigger_failover("shard0", http_client=client)
        assert ok is True

    # Restart coordinator
    del coord1
    coord2 = CoordinatorService(settings, map_path=tmp_path / "shard_map.json")
    assert coord2.shard_terms["shard0"] == 2

    # Node B registers after restart (incumbent primary matching shard map)
    reg_b = NodeRegistration(
        node_id="nodeB",
        shard_id="shard0",
        address="http://127.0.0.1:8201",
        role=NodeRole.PRIMARY,
        term=2,
    )
    resp_b = coord2.register_node(reg_b)
    assert resp_b.role == NodeRole.PRIMARY
    assert resp_b.term == 2
    assert resp_b.lease is not None
    assert resp_b.lease.node_id == "nodeB"
    assert resp_b.lease.term == 2

    # Node B sends valid primary heartbeat at term 2
    hb_b = Heartbeat(
        node_id="nodeB",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=2,
        last_applied_seq=20,
        follower_acked_seq=15,
        follower_state=ReplicaState.SYNCED,
        timestamp_monotonic=time.monotonic(),
    )
    lease_b = coord2.handle_heartbeat(hb_b)
    assert lease_b is not None
    assert lease_b.node_id == "nodeB"
    assert lease_b.term == 2
    assert coord2.leases["shard0"].node_id == "nodeB"


# ─────────────────────────────────────────────────────────────────────────────
# Test D — Unknown node cannot claim authority
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_d_unknown_node_cannot_claim_authority(tmp_path: Path):
    """Test D:
    Send a primary heartbeat from an unregistered node after quarantine expires. It must be rejected.
    """
    settings = _make_settings(tmp_path, LEASE_DURATION_S=0.6, SAFETY_MARGIN_S=0.1)
    coord = _bootstrap_cluster(tmp_path, settings)

    # Wait past quarantine
    await asyncio.sleep(0.8)
    assert time.monotonic() > coord._recovery_quarantine_until_monotonic

    # Unregistered node sends primary heartbeat matching term 1
    hb_unknown = Heartbeat(
        node_id="unknown-node-xyz",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=1,
        last_applied_seq=0,
        follower_acked_seq=0,
        follower_state=ReplicaState.SYNCED,
        timestamp_monotonic=time.monotonic(),
    )
    grant = coord.handle_heartbeat(hb_unknown)
    assert grant is None
    assert "unknown-node-xyz" not in coord.nodes
    assert "shard0" not in coord.leases


# ─────────────────────────────────────────────────────────────────────────────
# Test E — Delayed old-term heartbeat
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_e_delayed_old_term_heartbeat(tmp_path: Path):
    """Test E:
    Deliver a delayed heartbeat from A after B has been promoted. It must never restore A's authority.
    """
    settings = _make_settings(tmp_path)
    coord = _bootstrap_cluster(tmp_path, settings)

    coord.register_node(
        NodeRegistration(
            node_id="nodeA",
            shard_id="shard0",
            address="http://127.0.0.1:8200",
            role=NodeRole.PRIMARY,
            term=1,
        )
    )
    coord.register_node(
        NodeRegistration(
            node_id="nodeB",
            shard_id="shard0",
            address="http://127.0.0.1:8201",
            role=NodeRole.FOLLOWER,
            term=1,
        )
    )

    # Promote B
    async with AsyncClient(transport=MockTransport(), base_url="http://127.0.0.1:8201") as client:
        await coord.trigger_failover("shard0", http_client=client)

    assert coord.shard_terms["shard0"] == 2
    assert coord.leases["shard0"].node_id == "nodeB"

    # Delayed heartbeat from node A in term 1
    delayed_hb = Heartbeat(
        node_id="nodeA",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=1,
        last_applied_seq=5,
        follower_acked_seq=5,
        follower_state=ReplicaState.SYNCED,
        timestamp_monotonic=time.monotonic(),
    )

    with pytest.raises(StaleTerm):
        coord.handle_heartbeat(delayed_hb)

    # Verify B's authority is completely untouched
    assert coord.leases["shard0"].node_id == "nodeB"
    assert coord.leases["shard0"].term == 2


# ─────────────────────────────────────────────────────────────────────────────
# Test F — Repeated coordinator restarts
# ─────────────────────────────────────────────────────────────────────────────
def test_f_repeated_coordinator_restarts(tmp_path: Path):
    """Test F:
    Restart the coordinator multiple times and verify terms never decrease.
    """
    settings = _make_settings(tmp_path)
    map_path = tmp_path / "shard_map.json"
    coord = _bootstrap_cluster(tmp_path, settings)

    assert coord.shard_terms["shard0"] == 1

    # Bump failover directly across multiple cycles
    t1 = coord.bump_shard_failover("shard0")
    assert t1 == 2
    t2 = coord.bump_shard_failover("shard0")
    assert t2 == 3

    # Restart 1
    coord_r1 = CoordinatorService(settings, map_path=map_path)
    assert coord_r1.shard_terms["shard0"] == 3

    t3 = coord_r1.bump_shard_failover("shard0")
    assert t3 == 4

    # Restart 2
    coord_r2 = CoordinatorService(settings, map_path=map_path)
    assert coord_r2.shard_terms["shard0"] == 4

    # Restart 3 without any bumps
    coord_r3 = CoordinatorService(settings, map_path=map_path)
    assert coord_r3.shard_terms["shard0"] == 4

    # Verify file content on disk is strictly valid and matches
    terms_file = tmp_path / "terms.json"
    with open(terms_file) as f:
        data = json.load(f)
    assert data["shard0"] == 4


# ─────────────────────────────────────────────────────────────────────────────
# Test G — Durable persistence failure
# ─────────────────────────────────────────────────────────────────────────────
def test_g_durable_persistence_failure(tmp_path: Path):
    """Test G:
    Simulate term metadata persistence failure during promotion.
    The coordinator must not activate the new primary without a durable term increment.
    """
    settings = _make_settings(tmp_path)
    coord = _bootstrap_cluster(tmp_path, settings)

    assert coord.shard_terms["shard0"] == 1
    original_map = coord.shard_map

    # Simulate disk failure during term persistence
    with patch.object(coord, "_persist_terms", side_effect=OSError("Disk write error: ENOSPC")):
        with pytest.raises(OSError, match="ENOSPC"):
            coord.bump_shard_failover(
                "shard0",
                new_primary_url="http://127.0.0.1:8201",
                new_follower_url="http://127.0.0.1:8200",
            )

    # Term must have rolled back to 1
    assert coord.shard_terms["shard0"] == 1

    # Shard map must not have been modified
    assert coord.shard_map.map_version == original_map.map_version
    assert coord.shard_map.entries[0].primary == "http://127.0.0.1:8200"

    # Disk file must still have term 1
    terms_file = tmp_path / "terms.json"
    with open(terms_file) as f:
        data = json.load(f)
    assert data["shard0"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# Test H — Normal operation regression
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_h_normal_operation_regression(tmp_path: Path):
    """Test H:
    Verify ordinary registration, incumbent heartbeat renewal, failover,
    and old-primary rejoin still work.
    """
    settings = _make_settings(tmp_path)
    coord = _bootstrap_cluster(tmp_path, settings)

    # 1. Ordinary registration of primary and follower
    reg_a = coord.register_node(
        NodeRegistration(
            node_id="nodeA",
            shard_id="shard0",
            address="http://127.0.0.1:8200",
            role=NodeRole.PRIMARY,
            term=1,
        )
    )
    assert reg_a.role == NodeRole.PRIMARY
    assert reg_a.lease is not None

    reg_b = coord.register_node(
        NodeRegistration(
            node_id="nodeB",
            shard_id="shard0",
            address="http://127.0.0.1:8201",
            role=NodeRole.FOLLOWER,
            term=1,
        )
    )
    assert reg_b.role == NodeRole.FOLLOWER
    assert reg_b.lease is None

    # 2. Incumbent heartbeat renewal
    hb_a = Heartbeat(
        node_id="nodeA",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=1,
        last_applied_seq=1,
        follower_acked_seq=1,
        follower_state=ReplicaState.SYNCED,
        timestamp_monotonic=time.monotonic(),
    )
    grant_a = coord.handle_heartbeat(hb_a)
    assert grant_a is not None
    assert grant_a.node_id == "nodeA"
    assert grant_a.term == 1

    # 3. Failover promotion
    async with AsyncClient(transport=MockTransport(), base_url="http://127.0.0.1:8201") as client:
        ok = await coord.trigger_failover("shard0", http_client=client)
        assert ok is True

    assert coord.shard_terms["shard0"] == 2
    assert coord.leases["shard0"].node_id == "nodeB"
    assert coord.leases["shard0"].term == 2

    # 4. Old primary A rejoins as follower
    reg_rejoin = coord.register_node(
        NodeRegistration(
            node_id="nodeA",
            shard_id="shard0",
            address="http://127.0.0.1:8200",
            role=NodeRole.FOLLOWER,
            term=2,
        )
    )
    assert reg_rejoin.role == NodeRole.FOLLOWER
    assert reg_rejoin.lease is None

    # 5. Follower heartbeat
    hb_rejoin = Heartbeat(
        node_id="nodeA",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
        term=2,
        last_applied_seq=1,
        follower_acked_seq=1,
        follower_state=ReplicaState.SYNCED,
        timestamp_monotonic=time.monotonic(),
    )
    assert coord.handle_heartbeat(hb_rejoin) is None


# ─────────────────────────────────────────────────────────────────────────────
# Test: Corrupted Term Metadata Safety
# ─────────────────────────────────────────────────────────────────────────────
def test_corrupted_term_metadata_rejected_safely(tmp_path: Path):
    """Corrupted or incompatible term metadata must cause safe startup failure."""
    settings = _make_settings(tmp_path)
    terms_file = tmp_path / "terms.json"

    # Write corrupt JSON
    terms_file.write_text("invalid JSON {{{", encoding="utf-8")
    with pytest.raises(ValueError, match="Corrupted or unreadable terms"):
        CoordinatorService(settings, map_path=tmp_path / "shard_map.json")

    # Write invalid term value (negative or string or non-int)
    terms_file.write_text(json.dumps({"shard0": -5}), encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid term value"):
        CoordinatorService(settings, map_path=tmp_path / "shard_map.json")

    terms_file.write_text(json.dumps({"shard0": True}), encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid term value"):
        CoordinatorService(settings, map_path=tmp_path / "shard_map.json")


# ─────────────────────────────────────────────────────────────────────────────
# Test: Safety Invariant Across Restarts, Delayed Messages, and Failovers
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_safety_invariant_split_brain_prevention(tmp_path: Path):
    """Safety Invariant:
    For a given shard, no node other than the currently authorized primary
    at the current durable term may receive a valid primary lease.
    """
    settings = _make_settings(tmp_path, LEASE_DURATION_S=0.6, SAFETY_MARGIN_S=0.1)
    coord = _bootstrap_cluster(tmp_path, settings)

    # Initial term = 1, primary = nodeA
    coord.register_node(
        NodeRegistration(
            node_id="nodeA",
            shard_id="shard0",
            address="http://127.0.0.1:8200",
            role=NodeRole.PRIMARY,
            term=1,
        )
    )
    coord.register_node(
        NodeRegistration(
            node_id="nodeB",
            shard_id="shard0",
            address="http://127.0.0.1:8201",
            role=NodeRole.FOLLOWER,
            term=1,
        )
    )

    # Failover to B -> term becomes 2
    async with AsyncClient(transport=MockTransport(), base_url="http://127.0.0.1:8201") as client:
        await coord.trigger_failover("shard0", http_client=client)

    assert coord.shard_terms["shard0"] == 2
    assert coord.leases["shard0"].node_id == "nodeB"

    # Restart coordinator
    coord_restarted = CoordinatorService(settings, map_path=tmp_path / "shard_map.json")
    assert coord_restarted.shard_terms["shard0"] == 2

    # Wait past quarantine
    await asyncio.sleep(0.8)
    assert time.monotonic() > coord_restarted._recovery_quarantine_until_monotonic

    # Verify candidate nodes trying to get lease:
    # 1. Unregistered node claiming primary -> rejected
    hb_unreg = Heartbeat(
        node_id="rogue_node",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=2,
        last_applied_seq=1,
        follower_acked_seq=1,
        follower_state=ReplicaState.SYNCED,
        timestamp_monotonic=time.monotonic(),
    )
    assert coord_restarted.handle_heartbeat(hb_unreg) is None
    assert "shard0" not in coord_restarted.leases

    # 2. Old primary nodeA claiming primary at stale term 1 -> rejected
    hb_a_term1 = Heartbeat(
        node_id="nodeA",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=1,
        last_applied_seq=1,
        follower_acked_seq=1,
        follower_state=ReplicaState.SYNCED,
        timestamp_monotonic=time.monotonic(),
    )
    with pytest.raises(StaleTerm):
        coord_restarted.handle_heartbeat(hb_a_term1)

    # 3. Old primary nodeA claiming future term 3 -> rejected
    hb_a_term3 = Heartbeat(
        node_id="nodeA",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=3,
        last_applied_seq=1,
        follower_acked_seq=1,
        follower_state=ReplicaState.SYNCED,
        timestamp_monotonic=time.monotonic(),
    )
    with pytest.raises(StaleTerm):
        coord_restarted.handle_heartbeat(hb_a_term3)

    # 4. Old primary nodeA claiming current term 2 -> rejected (address is follower, not shard map primary)
    coord_restarted.register_node(
        NodeRegistration(
            node_id="nodeA",
            shard_id="shard0",
            address="http://127.0.0.1:8200",
            role=NodeRole.FOLLOWER,
            term=2,
        )
    )
    hb_a_term2 = Heartbeat(
        node_id="nodeA",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=2,
        last_applied_seq=1,
        follower_acked_seq=1,
        follower_state=ReplicaState.SYNCED,
        timestamp_monotonic=time.monotonic(),
    )
    assert coord_restarted.handle_heartbeat(hb_a_term2) is None
    assert "shard0" not in coord_restarted.leases

    # 5. Only authorized primary nodeB matching shard map at term 2 receives lease
    reg_b = coord_restarted.register_node(
        NodeRegistration(
            node_id="nodeB",
            shard_id="shard0",
            address="http://127.0.0.1:8201",
            role=NodeRole.PRIMARY,
            term=2,
        )
    )
    assert reg_b.lease is not None
    assert reg_b.lease.node_id == "nodeB"

    hb_b = Heartbeat(
        node_id="nodeB",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        term=2,
        last_applied_seq=5,
        follower_acked_seq=5,
        follower_state=ReplicaState.SYNCED,
        timestamp_monotonic=time.monotonic(),
    )
    lease_b = coord_restarted.handle_heartbeat(hb_b)
    assert lease_b is not None
    assert lease_b.node_id == "nodeB"
    assert coord_restarted.leases["shard0"].node_id == "nodeB"
