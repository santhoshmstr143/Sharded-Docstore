"""Automated benchmark runner for Sharded-Docstore evaluation suite — Module 13.

Executes:
1. Scalability, throughput, and latency sweeps (1, 2, 3 shards; concurrency 1..100).
2. Direct key routing vs scatter-gather query latency (primary vs follower read).
3. Failover recovery time (failover time & write-restoration time across >= 5 runs).
4. Live migration write pause duration and catchup ops vs key range size.
5. Saves raw CSVs and structured JSONs to results/ and results/raw/.
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from bench.load_generator import LoadConfig, LoadGenerator, compute_percentiles
from docstore.common.auth import (
    CLIENT_KEY_HEADER,
    CLUSTER_SIG_HEADER,
    sign_request,
)
from docstore.common.config import Settings
from docstore.common.models import NodeRole, ReplicaState, ShardMap, ShardMapEntry, ShardRange
from docstore.coordinator.coordinator import CoordinatorService
from docstore.coordinator.migration import compute_deterministic_checksum
from docstore.router.app import build_router_app
from docstore.router.shard_map import (
    _S1,
    _S2,
    KEY_SPACE_END,
    ShardMapManager,
    create_default_3_shard_map,
)
from docstore.shard.app import ShardRangeConfig, build_app, resync_with_primary

SECRET = "a" * 32
ADMIN = "b" * 32
CLIENT_KEY = "c" * 16


def _sig_headers(
    payload: dict[str, Any] | None = None, *, method: str = "POST", path: str = "/doc"
) -> dict[str, str]:
    """Headers for a direct node call: the client API key (shard data routes) plus a
    cluster signature bound to *method* + *path* (internal routes)."""
    body_bytes = (
        json.dumps(payload, separators=(",", ":")).encode("utf-8") if payload is not None else b""
    )
    return {
        CLUSTER_SIG_HEADER: sign_request(SECRET, method, path, body_bytes),
        CLIENT_KEY_HEADER: CLIENT_KEY,
    }


def record_environment(results_dir: Path) -> dict[str, Any]:
    """Capture environment metadata for benchmarking documentation."""
    commit = "unknown"
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, text=True
        ).strip()
    except Exception:
        pass

    env = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "git_commit": commit,
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count() or 1,
    }
    with open(results_dir / "environment.json", "w") as f:
        json.dump(env, f, indent=2)
    return env


# ═════════════════════════════════════════════════════════════════════════════
# 1. Throughput & Scalability Benchmark
# ═════════════════════════════════════════════════════════════════════════════


async def run_throughput_latency_suite(results_dir: Path, raw_dir: Path) -> list[dict[str, Any]]:
    """Measure throughput (ops/sec) and latency (p50, p95, p99) across concurrency."""
    print("\n--- Running Throughput & Latency Suite ---")
    tmp_path = Path(tempfile.mkdtemp(prefix="bench_tput_"))
    try:
        settings = Settings(
            CLUSTER_SECRET=SECRET,
            ADMIN_TOKEN=ADMIN,
            CLIENT_API_KEY=CLIENT_KEY,
            DATA_DIR=str(tmp_path),
        )

        # 3 shards, each with primary and follower (replicated cluster)
        shard_apps: dict[str, Any] = {}
        transports: dict[str, ASGITransport] = {}

        for i, (start, end) in enumerate([(0, _S1), (_S1, _S2), (_S2, KEY_SPACE_END)]):
            sid = f"shard{i}"
            ranges = [ShardRangeConfig(start=start, end=end, ownership_epoch=1)]
            app_p = build_app(
                settings.model_copy(update={"DATA_DIR": str(tmp_path / f"p{i}")}),
                node_id=f"node{i}-p",
                shard_id=sid,
                role=NodeRole.PRIMARY,
                ranges=ranges,
                follower_url=f"127.0.0.1:{8201 + i * 2}",
            )
            app_f = build_app(
                settings.model_copy(update={"DATA_DIR": str(tmp_path / f"f{i}")}),
                node_id=f"node{i}-f",
                shard_id=sid,
                role=NodeRole.FOLLOWER,
                ranges=ranges,
            )
            p_addr = f"127.0.0.1:{8200 + i * 2}"
            f_addr = f"127.0.0.1:{8201 + i * 2}"

            trans_p = ASGITransport(app=app_p)
            trans_f = ASGITransport(app=app_f)

            shard_apps[p_addr] = app_p
            shard_apps[f_addr] = app_f
            transports[p_addr] = trans_p
            transports[f_addr] = trans_f

        mgr = ShardMapManager()
        mgr.set_map(create_default_3_shard_map())
        router_app = build_router_app(settings, mgr, shard_transports=transports)

        results: list[dict[str, Any]] = []
        raw_rows: list[dict[str, Any]] = []

        shard_lifespans = [LifespanManager(a) for a in shard_apps.values()]
        lifespans = [*shard_lifespans, LifespanManager(router_app)]

        for ls in lifespans:
            await ls.__aenter__()

        try:
            for i in range(3):
                p_addr = f"127.0.0.1:{8200 + i * 2}"
                f_addr = f"127.0.0.1:{8201 + i * 2}"
                shard_apps[p_addr].state.node.http_client = AsyncClient(
                    mounts={f"http://{f_addr}": transports[f_addr]}, timeout=5.0
                )
                shard_apps[f_addr].state.node.http_client = AsyncClient(
                    mounts={f"http://{p_addr}": transports[p_addr]}, timeout=5.0
                )
                shard_apps[p_addr].state.node.follower_state = ReplicaState.SYNCED

            router_trans = ASGITransport(app=router_app)
            async with AsyncClient(transport=router_trans, base_url="http://router") as client:
                from docstore.client.client import AsyncDocStoreClient

                doc_client = AsyncDocStoreClient(
                    router_url="http://router",
                    api_key=CLIENT_KEY,
                    http_client=client,
                )

                # Seed cluster
                print("Seeding replicated cluster with 500 documents across 3 shards...")
                load_gen = LoadGenerator(doc_client, LoadConfig(key_min=0, key_max=100000))
                seeded_count = await load_gen.seed_data(500)
                seeded_keys = set(load_gen._existing_keys)
                print(
                    f"Seeded {seeded_count} documents across 3 shards (tracked keys: {len(seeded_keys)})..."
                )

                # Test concurrencies
                concurrencies = [1, 5, 10, 25, 50]
                for c in concurrencies:
                    ops = 1000 if c > 1 else 300
                    cfg = LoadConfig(
                        total_ops=ops,
                        concurrency=c,
                        read_ratio=0.7,
                        warmup_ops=50,
                        seed=42 + c,
                    )
                    gen = LoadGenerator(doc_client, cfg, existing_keys=seeded_keys)
                    res = await gen.run()
                    seeded_keys.update(gen._existing_keys)

                    entry = {
                        "concurrency": c,
                        "shards": 3,
                        "total_ops": res.total_ops,
                        "successful_ops": len(res.latencies_ms),
                        "failed_ops": res.error_ops,
                        "timeout_ops": res.timeout_ops,
                        "duration_s": res.duration_s,
                        "attempted_throughput_ops_sec": res.attempted_throughput_ops_sec,
                        "successful_throughput_ops_sec": res.successful_throughput_ops_sec,
                        "throughput_ops_sec": res.throughput_ops_sec,
                        "latency_p50_ms": res.latency_p50_ms,
                        "latency_p90_ms": res.latency_p90_ms,
                        "latency_p95_ms": res.latency_p95_ms,
                        "latency_p99_ms": res.latency_p99_ms,
                        "error_rate": round(res.error_ops / max(1, res.total_ops), 4),
                    }
                    results.append(entry)
                    print(
                        f"Concurrency={c:2d} -> Successful: {res.successful_throughput_ops_sec:6.1f} ops/s, "
                        f"Attempted: {res.attempted_throughput_ops_sec:6.1f} ops/s, "
                        f"p50: {res.latency_p50_ms:5.2f} ms, p95: {res.latency_p95_ms:5.2f} ms, "
                        f"p99: {res.latency_p99_ms:5.2f} ms, errors: {res.error_ops}"
                    )

                    for lat in res.latencies_ms:
                        raw_rows.append({"concurrency": c, "latency_ms": lat})
        finally:
            for ls in reversed(lifespans):
                await ls.__aexit__(None, None, None)

        # Save JSON & CSV
        with open(results_dir / "throughput_latency.json", "w") as f:
            json.dump(results, f, indent=2)

        with open(raw_dir / "throughput_latency.csv", "w", newline="") as f:
            fieldnames = [
                "concurrency",
                "shards",
                "total_ops",
                "successful_ops",
                "failed_ops",
                "timeout_ops",
                "duration_s",
                "attempted_throughput_ops_sec",
                "successful_throughput_ops_sec",
                "throughput_ops_sec",
                "latency_p50_ms",
                "latency_p90_ms",
                "latency_p95_ms",
                "latency_p99_ms",
                "error_rate",
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

        with open(raw_dir / "throughput_latency_samples.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["concurrency", "latency_ms"])
            writer.writeheader()
            writer.writerows(raw_rows)

        return results
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


# ═════════════════════════════════════════════════════════════════════════════
# 2. Query Comparison (Direct Routing vs Scatter-Gather vs Follower)
# ═════════════════════════════════════════════════════════════════════════════


async def run_query_comparison_suite(results_dir: Path, raw_dir: Path) -> list[dict[str, Any]]:
    """Compare direct shard-key routing vs scatter-gather queries (1, 2, 3 shards)."""
    print("\n--- Running Query Routing & Scatter-Gather Comparison ---")
    tmp_path = Path(tempfile.mkdtemp(prefix="bench_query_"))
    try:
        settings = Settings(
            CLUSTER_SECRET=SECRET,
            ADMIN_TOKEN=ADMIN,
            CLIENT_API_KEY=CLIENT_KEY,
            DATA_DIR=str(tmp_path),
        )

        # 3 shards, each with primary and follower
        shard_apps: dict[str, Any] = {}
        transports: dict[str, ASGITransport] = {}

        for i, (start, end) in enumerate([(0, _S1), (_S1, _S2), (_S2, KEY_SPACE_END)]):
            sid = f"shard{i}"
            ranges = [ShardRangeConfig(start=start, end=end, ownership_epoch=1)]
            app_p = build_app(
                settings.model_copy(update={"DATA_DIR": str(tmp_path / f"p{i}")}),
                node_id=f"node{i}-p",
                shard_id=sid,
                role=NodeRole.PRIMARY,
                ranges=ranges,
                follower_url=f"127.0.0.1:{8201 + i * 2}",
            )
            app_f = build_app(
                settings.model_copy(update={"DATA_DIR": str(tmp_path / f"f{i}")}),
                node_id=f"node{i}-f",
                shard_id=sid,
                role=NodeRole.FOLLOWER,
                ranges=ranges,
            )
            p_addr = f"127.0.0.1:{8200 + i * 2}"
            f_addr = f"127.0.0.1:{8201 + i * 2}"

            trans_p = ASGITransport(app=app_p)
            trans_f = ASGITransport(app=app_f)

            shard_apps[p_addr] = app_p
            shard_apps[f_addr] = app_f
            transports[p_addr] = trans_p
            transports[f_addr] = trans_f

        mgr = ShardMapManager()
        default_map = create_default_3_shard_map()
        mgr.set_map(default_map)
        router_app = build_router_app(settings, mgr, shard_transports=transports)

        results: list[dict[str, Any]] = []

        shard_lifespans = [LifespanManager(a) for a in shard_apps.values()]
        lifespans = [*shard_lifespans, LifespanManager(router_app)]

        for ls in lifespans:
            await ls.__aenter__()

        try:
            for i in range(3):
                p_addr = f"127.0.0.1:{8200 + i * 2}"
                f_addr = f"127.0.0.1:{8201 + i * 2}"
                shard_apps[p_addr].state.node.http_client = AsyncClient(
                    mounts={f"http://{f_addr}": transports[f_addr]}, timeout=2.0
                )
                shard_apps[f_addr].state.node.http_client = AsyncClient(
                    mounts={f"http://{p_addr}": transports[p_addr]}, timeout=2.0
                )

            router_trans = ASGITransport(app=router_app)
            async with AsyncClient(transport=router_trans, base_url="http://router") as client:
                headers = {CLIENT_KEY_HEADER: CLIENT_KEY}

                # Seed documents across shards
                print("Populating test documents for query comparison...")
                seed_dids = [
                    100,
                    200,
                    300,
                    int(_S1 + 100),
                    int(_S1 + 200),
                    int(_S2 + 100),
                    int(_S2 + 200),
                ]
                for did in seed_dids:
                    doc_body = {
                        "doc_id": did,
                        "category": "alpha",
                        "val": did % 10,
                    }
                    await client.post(
                        "/doc",
                        json={"request_id": f"q-seed-{did}", "document": doc_body},
                        headers=headers,
                    )

                # 1. Direct Shard Key Read (Primary)
                direct_primary_lats: list[float] = []
                for _ in range(300):
                    t0 = time.perf_counter()
                    resp = await client.get("/doc/100?read_mode=primary", headers=headers)
                    assert resp.status_code == 200
                    direct_primary_lats.append((time.perf_counter() - t0) * 1000.0)

                # 2. Direct Shard Key Read (Follower)
                direct_follower_lats: list[float] = []
                for _ in range(300):
                    t0 = time.perf_counter()
                    resp = await client.get("/doc/100?read_mode=follower", headers=headers)
                    assert resp.status_code == 200
                    direct_follower_lats.append((time.perf_counter() - t0) * 1000.0)

                # 3. Scatter-gather across 1 Shard (by range)
                sg_1shard_lats: list[float] = []
                for _ in range(300):
                    t0 = time.perf_counter()
                    resp = await client.post(
                        "/query",
                        json={"filter": {"doc_id": {"$gte": 50, "$lte": 500}}},
                        headers=headers,
                    )
                    assert resp.status_code == 200
                    sg_1shard_lats.append((time.perf_counter() - t0) * 1000.0)

                # 4. Scatter-gather across 3 Shards (unconstrained non-key filter)
                sg_3shards_lats: list[float] = []
                for _ in range(300):
                    t0 = time.perf_counter()
                    resp = await client.post(
                        "/query",
                        json={"filter": {"category": "alpha"}},
                        headers=headers,
                    )
                    assert resp.status_code == 200
                    sg_3shards_lats.append((time.perf_counter() - t0) * 1000.0)

                modes = [
                    ("direct_key_primary", 1, direct_primary_lats),
                    ("direct_key_follower", 1, direct_follower_lats),
                    ("scatter_gather_1_shard", 1, sg_1shard_lats),
                    ("scatter_gather_3_shards", 3, sg_3shards_lats),
                ]

                raw_rows: list[dict[str, Any]] = []
                for name, num_shards, lats in modes:
                    p = compute_percentiles(lats)
                    entry = {
                        "query_mode": name,
                        "shards_queried": num_shards,
                        "samples": len(lats),
                        "latency_p50_ms": p["p50"],
                        "latency_p90_ms": p["p90"],
                        "latency_p95_ms": p["p95"],
                        "latency_p99_ms": p["p99"],
                        "latency_mean_ms": p["mean"],
                    }
                    results.append(entry)
                    print(
                        f"{name:<24} -> p50: {p['p50']:5.2f} ms, "
                        f"p95: {p['p95']:5.2f} ms, p99: {p['p99']:5.2f} ms"
                    )
                    for val in lats:
                        raw_rows.append({"query_mode": name, "latency_ms": val})

                with open(results_dir / "query_comparison.json", "w") as f:
                    json.dump(results, f, indent=2)

                with open(raw_dir / "query_comparison.csv", "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=["query_mode", "latency_ms"])
                    writer.writeheader()
                    writer.writerows(raw_rows)

                return results
        finally:
            for ls in reversed(lifespans):
                await ls.__aexit__(None, None, None)
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


# ═════════════════════════════════════════════════════════════════════════════
# 3. Failover Recovery Time Suite (>= 5 Independent Runs)
# ═════════════════════════════════════════════════════════════════════════════


async def run_failover_recovery_suite(
    results_dir: Path, raw_dir: Path, runs: int = 5
) -> dict[str, Any]:
    """Measure failover time (crash -> read) and write restoration time across >= 5 runs."""
    print(f"\n--- Running Failover Recovery Suite ({runs} independent runs) ---")
    tmp_path = Path(tempfile.mkdtemp(prefix="bench_failover_"))
    try:
        # Scale parameters so H < L < D and T + M < L - H always hold
        # H=0.1s, L=0.5s, D=0.6s, T=0.05s, M=0.1s
        # H (0.1) < L (0.5) < D (0.6) is TRUE
        # T (0.05) + M (0.1) = 0.15 < L (0.5) - H (0.1) = 0.4 is TRUE
        h = 0.1
        lease_duration = 0.5
        d = 0.6
        t = 0.05
        m = 0.1
        restart_delay = 0.15

        protocol_params = {
            "heartbeat_interval_s": h,
            "lease_duration_s": lease_duration,
            "detection_timeout_s": d,
            "replication_timeout_s": t,
            "safety_margin_s": m,
            "restart_delay_s": restart_delay,
            "runs": runs,
        }

        failover_times: list[float] = []
        write_restoration_times: list[float] = []
        raw_rows: list[dict[str, Any]] = []

        settings = Settings(
            CLUSTER_SECRET=SECRET,
            ADMIN_TOKEN=ADMIN,
            CLIENT_API_KEY=CLIENT_KEY,
            DATA_DIR=str(tmp_path),
            HEARTBEAT_INTERVAL_S=h,
            LEASE_DURATION_S=lease_duration,
            FAILURE_TIMEOUT_S=d,
            SAFETY_MARGIN_S=m,
            REPLICATION_TIMEOUT_S=t,
        )

        dir_a = tmp_path / "node_a"
        dir_b = tmp_path / "node_b"
        ranges = [ShardRangeConfig(start=0, end=10000, ownership_epoch=1)]

        for run_idx in range(1, runs + 1):
            shutil.rmtree(dir_a, ignore_errors=True)
            shutil.rmtree(dir_b, ignore_errors=True)

            app_a = build_app(
                settings.model_copy(update={"DATA_DIR": str(dir_a)}),
                node_id="node-a",
                shard_id="shard0",
                role=NodeRole.PRIMARY,
                ranges=ranges,
                follower_url="127.0.0.1:8201",
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
                    mounts={"http://127.0.0.1:8201": trans_b}, timeout=1.0
                )
                app_b.state.node.http_client = AsyncClient(
                    mounts={"http://127.0.0.1:8200": trans_a}, timeout=1.0
                )

                # Seed one write before crash
                init_body = {"request_id": f"init-{run_idx}", "document": {"doc_id": 10, "v": 1}}
                async with AsyncClient(transport=trans_a, base_url="http://node-a") as ca:
                    await ca.post(
                        "/doc",
                        json=init_body,
                        headers=_sig_headers(init_body),
                    )

                # Crash node A: simulate failure timestamp
                t_crash = time.perf_counter()

                # Simulate heartbeat lapse & detection wait
                await asyncio.sleep(d)

                # Coordinator promotes Node B with incremented epoch & term
                async with AsyncClient(transport=trans_b, base_url="http://promoted") as cb:
                    prom_payload = {
                        "shard_id": "shard0",
                        "term": 2,
                        "ownership_epochs": {"0-10000": 2},
                        "follower_url": "127.0.0.1:8200",
                    }
                    prom_resp = await cb.post(
                        "/internal/promote",
                        json=prom_payload,
                        headers=_sig_headers(prom_payload, path="/internal/promote"),
                    )
                    assert prom_resp.status_code == 200

                    # First successful primary read
                    r_read = await cb.get("/doc/10", headers=_sig_headers())
                    assert r_read.status_code == 200
                    t_first_read = time.perf_counter()

                    # Writes should be rejected during degraded mode
                    deg_body = {"request_id": f"deg-{run_idx}", "document": {"doc_id": 20, "v": 2}}
                    r_deg = await cb.post(
                        "/doc",
                        json=deg_body,
                        headers=_sig_headers(deg_body),
                    )
                    assert r_deg.status_code == 503

                failover_duration_ms = (t_first_read - t_crash) * 1000.0
                failover_times.append(round(failover_duration_ms, 2))

                # Wait restart delay for old primary to restart
                await asyncio.sleep(restart_delay)

                # Restart old primary as follower and resync
                app_a.state.node.role = NodeRole.FOLLOWER
                rejoin_client = AsyncClient(mounts={"http://127.0.0.1:8201": trans_b}, timeout=2.0)
                await resync_with_primary(
                    app_a.state.node,
                    primary_url="127.0.0.1:8201",
                    http_client=rejoin_client,
                )

                # Now write should succeed on new primary
                succ_body = {"request_id": f"succ-{run_idx}", "document": {"doc_id": 20, "v": 2}}
                async with AsyncClient(transport=trans_b, base_url="http://promoted") as cb:
                    r_write = await cb.post(
                        "/doc",
                        json=succ_body,
                        headers=_sig_headers(succ_body),
                    )
                    assert r_write.status_code == 201
                    t_first_write = time.perf_counter()

                write_restoration_ms = (t_first_write - t_crash) * 1000.0
                write_restoration_times.append(round(write_restoration_ms, 2))

                raw_rows.append(
                    {
                        "run": run_idx,
                        "failover_time_ms": round(failover_duration_ms, 2),
                        "write_restoration_time_ms": round(write_restoration_ms, 2),
                    }
                )

                print(
                    f"Run {run_idx}: Failover Time = {failover_duration_ms:6.1f} ms | "
                    f"Write Restoration Time = {write_restoration_ms:6.1f} ms"
                )

        f_pcts = compute_percentiles(failover_times)
        w_pcts = compute_percentiles(write_restoration_times)

        summary = {
            "parameters": protocol_params,
            "failover_time_ms": {
                "mean": f_pcts["mean"],
                "min": f_pcts["min"],
                "max": f_pcts["max"],
                "p50": f_pcts["p50"],
                "p95": f_pcts["p95"],
            },
            "write_restoration_time_ms": {
                "mean": w_pcts["mean"],
                "min": w_pcts["min"],
                "max": w_pcts["max"],
                "p50": w_pcts["p50"],
                "p95": w_pcts["p95"],
            },
            "runs": raw_rows,
        }

        with open(results_dir / "failover_recovery.json", "w") as f:
            json.dump(summary, f, indent=2)

        with open(raw_dir / "failover_recovery.csv", "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["run", "failover_time_ms", "write_restoration_time_ms"]
            )
            writer.writeheader()
            writer.writerows(raw_rows)

        return summary
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


# ═════════════════════════════════════════════════════════════════════════════
# 4. Live Migration Metrics Suite (Docs Moved, Catchup Ops, Write Pause)
# ═════════════════════════════════════════════════════════════════════════════


async def run_migration_metrics_suite(results_dir: Path, raw_dir: Path) -> list[dict[str, Any]]:
    """Measure live migration write pause and catch-up operations vs range size."""
    print("\n--- Running Live Migration Metrics Suite ---")
    tmp_path = Path(tempfile.mkdtemp(prefix="bench_mig_"))
    try:
        sizes = [500, 2000, 5000]  # Representative scale sizes for local benchmark
        results: list[dict[str, Any]] = []

        settings = Settings(
            CLUSTER_SECRET=SECRET,
            ADMIN_TOKEN=ADMIN,
            CLIENT_API_KEY=CLIENT_KEY,
            DATA_DIR=str(tmp_path),
            HEARTBEAT_INTERVAL_S=0.1,
            LEASE_DURATION_S=1.0,
            FAILURE_TIMEOUT_S=2.0,
            SAFETY_MARGIN_S=0.2,
            REPLICATION_TIMEOUT_S=0.2,
        )

        for count in sizes:
            shard0_dir = tmp_path / f"s0_{count}"
            shard1_dir = tmp_path / f"s1_{count}"
            shard_map_file = tmp_path / f"map_{count}.json"

            app_s0 = build_app(
                settings.model_copy(update={"DATA_DIR": str(shard0_dir)}),
                node_id="n0",
                shard_id="shard0",
                role=NodeRole.PRIMARY,
                ranges=[ShardRangeConfig(start=0, end=100000, ownership_epoch=1)],
            )
            app_s1 = build_app(
                settings.model_copy(update={"DATA_DIR": str(shard1_dir)}),
                node_id="n1",
                shard_id="shard1",
                role=NodeRole.PRIMARY,
                ranges=[ShardRangeConfig(start=100000, end=KEY_SPACE_END, ownership_epoch=1)],
            )

            trans_s0 = ASGITransport(app=app_s0)
            trans_s1 = ASGITransport(app=app_s1)

            mig_map = ShardMap(
                map_version=1,
                entries=[
                    ShardMapEntry(
                        shard_id="shard0",
                        range=ShardRange(start=0, end=100000),
                        primary="127.0.0.1:8200",
                        follower=None,
                        ownership_epoch=1,
                        map_version=1,
                    ),
                    ShardMapEntry(
                        shard_id="shard1",
                        range=ShardRange(start=100000, end=KEY_SPACE_END),
                        primary="127.0.0.1:8202",
                        follower=None,
                        ownership_epoch=1,
                        map_version=1,
                    ),
                ],
            )
            coord_svc = CoordinatorService(settings, str(shard_map_file))
            coord_svc.map_mgr.set_map(mig_map)
            coord_svc.map_mgr.save(mig_map)
            coord_svc.replica_states["shard0"] = ReplicaState.SYNCED
            coord_svc.replica_states["shard1"] = ReplicaState.SYNCED

            rpc_client = AsyncClient(
                mounts={
                    "http://127.0.0.1:8200": trans_s0,
                    "http://127.0.0.1:8202": trans_s1,
                },
                timeout=10.0,
            )

            async with LifespanManager(app_s0), LifespanManager(app_s1):
                # Pre-seed documents into shard 0 engine
                engine = app_s0.state.node.engine
                for i in range(count):
                    doc_item = {"doc_id": i, "data": f"payload-{i}", "checksum_val": i * 17}
                    engine.insert(doc_item, seq=i + 1)
                app_s0.state.node.last_applied_seq = count

                # Calculate checksum before migration
                _cnt_before, chk_before = compute_deterministic_checksum(
                    engine.range_scan(0, count)
                )

                t0 = time.perf_counter()
                rec = coord_svc.migration_mgr.create_migration(
                    from_shard="shard0",
                    to_shard="shard1",
                    start=0,
                    end=count,
                    coord=coord_svc,
                )
                res = await coord_svc.migration_mgr.execute_migration(
                    rec.migration_id,
                    coord_svc,
                    http_client=rpc_client,
                )
                total_elapsed_s = time.perf_counter() - t0

                dest_engine = app_s1.state.node.engine
                _cnt_after, chk_after = compute_deterministic_checksum(
                    dest_engine.range_scan(0, count)
                )
                assert chk_before == chk_after

                entry = {
                    "docs_migrated": count,
                    "docs_moved": res.docs_copied,
                    "catchup_ops": res.catchup_ops_replayed,
                    "pause_duration_ms": round(res.pause_duration_s * 1000.0, 2),
                    "total_duration_s": round(total_elapsed_s, 3),
                    "checksum_verified": (chk_before == chk_after),
                }
                results.append(entry)
                print(
                    f"Docs={count:5d} -> Write Pause: {entry['pause_duration_ms']:5.2f} ms | "
                    f"Catchup: {entry['catchup_ops']} | Checksum: {entry['checksum_verified']}"
                )

        with open(results_dir / "migration_pause.json", "w") as f:
            json.dump(results, f, indent=2)

        with open(raw_dir / "migration_pause.csv", "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "docs_migrated",
                    "docs_moved",
                    "catchup_ops",
                    "pause_duration_ms",
                    "total_duration_s",
                    "checksum_verified",
                ],
            )
            writer.writeheader()
            writer.writerows(results)

        return results
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


async def main() -> None:
    results_dir = Path("results")
    raw_dir = results_dir / "raw"
    results_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    print("=================================================================")
    print("       SHARDED-DOCSTORE BENCHMARK & EVALUATION RUNNER           ")
    print("=================================================================")

    env = record_environment(results_dir)
    print(f"Platform: {env['platform']} ({env['cpu_count']} CPUs)")
    print(f"Commit:   {env['git_commit']}")

    await run_throughput_latency_suite(results_dir, raw_dir)
    await run_query_comparison_suite(results_dir, raw_dir)
    await run_failover_recovery_suite(results_dir, raw_dir, runs=5)
    await run_migration_metrics_suite(results_dir, raw_dir)

    print("\n✅ All benchmark suites completed successfully.")
    print(f"Results saved to: {results_dir.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
