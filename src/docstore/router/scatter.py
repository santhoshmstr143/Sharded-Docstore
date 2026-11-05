"""
Router scatter-gather query execution — Module 5.3.

Responsibilities:
- Inspect filter for doc_id constraints (exact, $in, range, all)
- Route to minimal required shards (1 shard for exact doc_id; overlapping shards for range/$in)
- Query each shard ONCE (even if it owns several disjoint ranges), passing the ranges
  the map attributes to it so the shard restricts *before* sorting and paginating
- Query shards concurrently with local skip=0 and local limit = skip + limit
- Enforce partial-failure policy: whole query fails if any required shard fails or times out
- Filter out documents outside the ranges attributed to the responding shard
- Report the exact cluster-wide match count and aggregated response metadata
- Merge sorted streams using the total order comparator and doc_id tie-breaker
- Deduplicate by doc_id and apply global skip and limit
"""

from __future__ import annotations

import asyncio
import heapq
import json
from typing import Any

import httpx
from fastapi import HTTPException

from docstore.common.auth import CLUSTER_SIG_HEADER, compute_signature
from docstore.common.config import Settings
from docstore.common.errors import OutOfRange
from docstore.common.logging import get_logger
from docstore.common.models import (
    QUERY_LIMIT_MAX,
    QueryFilter,
    QueryResponse,
    ResponseMeta,
    ShardMapEntry,
)
from docstore.query.evaluator import is_number
from docstore.query.shard_key import (
    ShardKeyConstraint,
    detect_shard_key_constraint,
)
from docstore.query.sorter import extract_sort_key
from docstore.router.shard_map import ShardMapManager

logger = get_logger(__name__)


def _specialize_in_filter(
    filter_dict: dict[str, Any],
    shard_doc_ids: list[int],
) -> dict[str, Any]:
    """Create a copy of filter_dict with doc_id.$in restricted to shard_doc_ids."""
    new_filter = dict(filter_dict)
    if "doc_id" in new_filter and isinstance(new_filter["doc_id"], dict):
        d = dict(new_filter["doc_id"])
        d["$in"] = shard_doc_ids
        new_filter["doc_id"] = d
        return new_filter

    if "$and" in new_filter and isinstance(new_filter["$and"], list):
        new_and = []
        for sub in new_filter["$and"]:
            if isinstance(sub, dict) and "doc_id" in sub:
                new_and.append(_specialize_in_filter(sub, shard_doc_ids))
            else:
                new_and.append(sub)
        new_filter["$and"] = new_and
        return new_filter

    return new_filter


def resolve_target_shards(
    map_mgr: ShardMapManager,
    filter_dict: dict[str, Any],
) -> list[tuple[ShardMapEntry, dict[str, Any]]]:
    """Determine which shards must be queried and the specialized filter for each.

    Returns
    -------
    list[tuple[ShardMapEntry, dict[str, Any]]]
        List of (shard_entry, filter_for_shard).
    """
    constraint: ShardKeyConstraint = detect_shard_key_constraint(filter_dict)

    if constraint.kind == "exact":
        assert constraint.doc_id is not None
        try:
            entry = map_mgr.get_shard_for_doc_id(constraint.doc_id)
            return [(entry, filter_dict)]
        except OutOfRange:
            # If doc_id is outside the valid keyspace, no shard owns it
            return []

    if constraint.kind == "in":
        assert constraint.doc_ids is not None
        grouped: dict[str, list[int]] = {}
        entry_map: dict[str, ShardMapEntry] = {}

        for did in constraint.doc_ids:
            try:
                entry = map_mgr.get_shard_for_doc_id(did)
                grouped.setdefault(entry.shard_id, []).append(did)
                entry_map[entry.shard_id] = entry
            except OutOfRange:
                continue

        targets = []
        for shard_id, ids in grouped.items():
            entry = entry_map[shard_id]
            specialized = _specialize_in_filter(filter_dict, ids)
            targets.append((entry, specialized))
        return targets

    if constraint.kind == "range":
        assert constraint.range_start is not None and constraint.range_end is not None
        if constraint.range_start >= constraint.range_end:
            return []
        entries = map_mgr.get_shards_for_range(constraint.range_start, constraint.range_end)
        return [(entry, filter_dict) for entry in entries]

    # Non-key filter: scatter to all shards
    return [(entry, filter_dict) for entry in map_mgr.shard_map.entries]


class _ShardTarget:
    """One shard to query: its addresses, every range the map gives it, and its filter."""

    def __init__(self, entry: ShardMapEntry, filter_dict: dict[str, Any]) -> None:
        self.entry = entry
        self.filter = filter_dict
        self.ranges: list[tuple[int, int]] = [(entry.range.start, entry.range.end)]

    def owns(self, did: int) -> bool:
        return any(s <= did < e for s, e in self.ranges)


def group_targets_by_shard(
    targets: list[tuple[ShardMapEntry, dict[str, Any]]],
) -> list[_ShardTarget]:
    """Collapse map entries of the same shard into a single query target."""
    grouped: dict[str, _ShardTarget] = {}
    for entry, f in targets:
        t = grouped.get(entry.shard_id)
        if t is None:
            grouped[entry.shard_id] = _ShardTarget(entry, f)
        else:
            t.ranges.append((entry.range.start, entry.range.end))
    for t in grouped.values():
        t.ranges.sort()
    return list(grouped.values())


async def _query_single_shard(
    client: httpx.AsyncClient,
    target_info: _ShardTarget,
    payload: dict[str, Any],
    cluster_secret: str,
    timeout_s: float,
    needed_candidates: int,
) -> tuple[list[dict[str, Any]], int, dict[str, Any] | None]:
    """Query one shard (restricted to its attributed ranges) up to needed_candidates.

    Returns (candidate docs in shard sort order, shard-wide match total, shard meta).
    """
    entry = target_info.entry
    payload = dict(payload)
    payload["ranges"] = [list(r) for r in target_info.ranges]
    target = entry.primary
    if payload.get("read_mode") == "follower" and entry.follower:
        target = entry.follower

    clean_target = target.removeprefix("http://").removeprefix("https://")
    url = f"http://{clean_target}/query"

    collected_docs: list[dict[str, Any]] = []
    current_skip = 0
    shard_total: int | None = None
    shard_meta: dict[str, Any] | None = None

    while len(collected_docs) < needed_candidates:
        chunk_limit = min(needed_candidates - len(collected_docs), QUERY_LIMIT_MAX)
        chunk_payload = dict(payload)
        chunk_payload["skip"] = current_skip
        chunk_payload["limit"] = chunk_limit

        body_bytes = json.dumps(chunk_payload, separators=(",", ":")).encode("utf-8")
        sig = compute_signature(cluster_secret, body_bytes)
        headers = {
            "Content-Type": "application/json",
            CLUSTER_SIG_HEADER: sig,
        }

        try:
            resp = await client.post(url, content=body_bytes, headers=headers, timeout=timeout_s)
        except (httpx.ConnectError, httpx.TimeoutException) as exc:
            # H4: only fall back to primary for genuine connectivity failures.
            # Other exceptions (INVALID_QUERY, STALE_EPOCH, etc.) must propagate.
            if target != entry.primary:
                logger.warning(
                    "Follower %s unreachable (%s); falling back to primary %s",
                    target,
                    exc,
                    entry.primary,
                )
                primary = entry.primary.removeprefix("http://").removeprefix("https://")
                resp = await client.post(
                    f"http://{primary}/query",
                    content=body_bytes,
                    headers=headers,
                    timeout=timeout_s,
                )
            else:
                raise
        if resp.status_code != 200:
            try:
                data = resp.json()
                err_code = data.get("detail", {}).get("error_code") or data.get("error_code")
                err_msg = data.get("detail", {}).get("error_message") or data.get("error_message")
            except Exception:
                err_code = "SHARD_ERROR"
                err_msg = resp.text
            raise RuntimeError(
                f"Shard {entry.shard_id} returned HTTP {resp.status_code} ({err_code}): {err_msg}"
            )

        data = resp.json()
        if shard_total is None:
            raw_total = data.get("total")
            shard_total = raw_total if isinstance(raw_total, int) else 0
            meta = data.get("meta")
            shard_meta = meta if isinstance(meta, dict) else None
        docs = data.get("documents", [])
        if not docs:
            break

        # Filter out any document outside the ranges the map attributes to this shard
        for doc in docs:
            if isinstance(doc, dict) and "doc_id" in doc and is_number(doc["doc_id"]):
                did = int(doc["doc_id"])
                if target_info.owns(did):
                    collected_docs.append(doc)
                else:
                    logger.warning(
                        "Dropping out-of-range document %d returned by shard %s %s",
                        did,
                        entry.shard_id,
                        target_info.ranges,
                    )

        current_skip += len(docs)
        if len(docs) < chunk_limit:
            # Shard has returned all matching documents
            break

    return collected_docs, shard_total or 0, shard_meta


def _aggregate_meta(metas: list[dict[str, Any] | None]) -> ResponseMeta | None:
    """Cluster-level metadata: one shard -> its meta; several -> a conservative summary
    (highest term/epoch seen, lowest applied_seq, worst replication lag)."""
    parsed: list[ResponseMeta] = []
    for m in metas:
        if m is None:
            continue
        try:
            parsed.append(ResponseMeta.model_validate(m))
        except Exception as exc:
            logger.debug("Ignoring malformed shard meta %r: %s", m, exc)
    if not parsed:
        return None
    if len(parsed) == 1:
        return parsed[0]
    served = {m.served_by for m in parsed}
    lags = [m.replication_lag for m in parsed if m.replication_lag is not None]
    return ResponseMeta(
        served_by=served.pop() if len(served) == 1 else "mixed",
        term=max(m.term for m in parsed),
        ownership_epoch=max(m.ownership_epoch for m in parsed),
        applied_seq=min(m.applied_seq for m in parsed),
        replication_lag=max(lags) if lags else None,
    )


async def execute_scatter_query(
    query: QueryFilter,
    map_mgr: ShardMapManager,
    http_client: httpx.AsyncClient,
    settings: Settings,
) -> QueryResponse:
    """Execute scatter-gather query with retries and stream merging.

    Raises :class:`fastapi.HTTPException` with 503 SHARD_UNAVAILABLE on partial failure.
    """
    # Candidate depth: local skip=0, candidates needed = skip + limit
    needed_candidates = query.skip + query.limit
    timeout_s = min(settings.REPLICATION_TIMEOUT_S + 2.0, 5.0)

    # Allow up to 2 attempts (initial + 1 bounded retry on shard failure)
    last_error: Exception | None = None
    failed_shard_desc: str = "unknown"

    for attempt in range(2):
        targets = group_targets_by_shard(resolve_target_shards(map_mgr, query.filter))
        if not targets:
            return QueryResponse(documents=[], total=0)

        tasks = []
        for tgt in targets:
            shard_payload = {
                "filter": tgt.filter,
                "sort_field": query.sort_field,
                "sort_dir": query.sort_dir,
                "read_mode": query.read_mode.value,
            }
            tasks.append(
                _query_single_shard(
                    http_client,
                    tgt,
                    shard_payload,
                    settings.CLUSTER_SECRET,
                    timeout_s,
                    needed_candidates,
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        failed_entry: ShardMapEntry | None = None
        shard_lists: list[list[dict[str, Any]]] = []
        total = 0
        metas: list[dict[str, Any] | None] = []

        for tgt, res in zip(targets, results, strict=False):
            entry = tgt.entry
            if isinstance(res, BaseException):
                failed_entry = entry
                last_error = res if isinstance(res, Exception) else RuntimeError(str(res))
                failed_shard_desc = f"{entry.shard_id} ({entry.primary})"
                break
            docs, shard_total, shard_meta = res
            shard_lists.append(docs)
            # Each shard counted only its attributed (map-disjoint) ranges: exact sum.
            total += shard_total
            metas.append(shard_meta)

        if failed_entry is not None:
            if attempt == 0:
                logger.warning(
                    "Scatter-gather query failed on shard %s (%s); reloading map and retrying once",
                    failed_shard_desc,
                    last_error,
                )
                try:
                    map_mgr.reload()
                except Exception as exc:
                    logger.debug("Map reload failed: %s", exc)
                continue
            # Second attempt also failed: partial failure policy
            raise HTTPException(
                status_code=503,
                detail={
                    "error_code": "SHARD_UNAVAILABLE",
                    "error_message": f"Shard {failed_shard_desc} is unavailable: {last_error}",
                },
            )

        # Merge sorted candidate streams using total order comparator
        merged_stream = heapq.merge(
            *shard_lists,
            key=lambda d: extract_sort_key(d, query.sort_field, query.sort_dir),
        )

        # Deduplicate by doc_id
        seen_ids: set[int] = set()
        deduped: list[dict[str, Any]] = []
        for doc in merged_stream:
            did = int(doc["doc_id"])
            if did not in seen_ids:
                seen_ids.add(did)
                deduped.append(doc)

        # Global pagination: apply skip then limit
        paginated = deduped[query.skip : query.skip + query.limit]
        return QueryResponse(
            documents=paginated,
            total=max(total, len(deduped)),
            meta=_aggregate_meta(metas),
        )

    raise HTTPException(
        status_code=503,
        detail={
            "error_code": "SHARD_UNAVAILABLE",
            "error_message": f"Shard {failed_shard_desc} is unavailable: {last_error}",
        },
    )
