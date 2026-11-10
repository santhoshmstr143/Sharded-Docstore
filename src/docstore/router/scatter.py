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

from docstore.common.auth import CLIENT_KEY_HEADER, CLUSTER_SIG_HEADER, sign_request
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
        if constraint.doc_id is None:  # explicit: asserts are stripped under -O
            raise RuntimeError("constraint.doc_id is unavailable")
        try:
            entry = map_mgr.get_shard_for_doc_id(constraint.doc_id)
            return [(entry, filter_dict)]
        except OutOfRange:
            # If doc_id is outside the valid keyspace, no shard owns it
            return []

    if constraint.kind == "in":
        if constraint.doc_ids is None:  # explicit: asserts are stripped under -O
            raise RuntimeError("constraint.doc_ids is unavailable")
        # Keep EVERY map entry the ids fall into: a shard may own several disjoint
        # ranges, and keeping one entry per shard would restrict its query to that one
        # range and silently drop matches in the others. group_targets_by_shard() later
        # merges a shard's entries into one request covering all of its hit ranges.
        hit: dict[tuple[str, int], ShardMapEntry] = {}
        for did in constraint.doc_ids:
            try:
                entry = map_mgr.get_shard_for_doc_id(did)
            except OutOfRange:
                continue
            hit[(entry.shard_id, entry.range.start)] = entry

        # Send the client's filter unchanged. Rewriting doc_id.$in per shard used to
        # clobber every other doc_id constraint in an $and (wrong results); the shard
        # already restricts evaluation to its own ranges, so no rewrite is needed.
        return [(entry, filter_dict) for entry in hit.values()]

    if constraint.kind == "range":
        if (
            constraint.range_start is None or constraint.range_end is None
        ):  # explicit: asserts are stripped under -O
            raise RuntimeError("constraint.range_start / constraint.range_end unavailable")
        if constraint.range_start >= constraint.range_end:
            return []
        entries = map_mgr.get_shards_for_range(constraint.range_start, constraint.range_end)
        return [(entry, filter_dict) for entry in entries]

    # Non-key filter: scatter to all shards
    return [(entry, filter_dict) for entry in map_mgr.shard_map.entries]


class _ClientQueryError(Exception):
    """A shard rejected the query itself (not a shard failure): pass it to the client."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


_CLIENT_QUERY_ERRORS = frozenset({"INVALID_QUERY", "VALIDATION_ERROR"})

# Upper bound on shard response bytes one query may pull into the router (all shards,
# all chunks). skip + limit allows up to 11k candidates of up to 1 MB each otherwise.
QUERY_MAX_RESPONSE_BYTES = 32 * 1024 * 1024


class _ByteBudget:
    def __init__(self, limit: int) -> None:
        self.remaining = limit

    def charge(self, n: int) -> None:
        self.remaining -= n
        if self.remaining < 0:
            raise _ClientQueryError(
                413,
                "RESULT_TOO_LARGE",
                "Query result exceeds the response size limit; reduce skip/limit or "
                "narrow the filter.",
            )


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
    api_key: str = "",
    budget: _ByteBudget | None = None,
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
        sig = sign_request(cluster_secret, "POST", url, body_bytes)
        headers = {
            "Content-Type": "application/json",
            CLUSTER_SIG_HEADER: sig,
            CLIENT_KEY_HEADER: api_key,
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
            if resp.status_code in (400, 422) and err_code in _CLIENT_QUERY_ERRORS:
                # The query is invalid (e.g. sort on an array field): retrying or
                # reporting the shard as unavailable would be wrong.
                raise _ClientQueryError(resp.status_code, str(err_code), str(err_msg))
            raise RuntimeError(
                f"Shard {entry.shard_id} returned HTTP {resp.status_code} ({err_code}): {err_msg}"
            )

        if budget is not None:
            budget.charge(len(resp.content))
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
    failed_shard_desc: str = "unknown"  # with address: logs only
    failed_shard_id: str = "unknown"  # safe for client-facing messages

    for attempt in range(2):
        targets = group_targets_by_shard(resolve_target_shards(map_mgr, query.filter))
        if not targets:
            return QueryResponse(documents=[], total=0)

        tasks = []
        budget = _ByteBudget(QUERY_MAX_RESPONSE_BYTES)
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
                    settings.CLIENT_API_KEY,
                    budget,
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        failed_entry: ShardMapEntry | None = None
        shard_lists: list[list[dict[str, Any]]] = []
        total = 0
        metas: list[dict[str, Any] | None] = []

        for res in results:
            if isinstance(res, _ClientQueryError):
                raise HTTPException(
                    status_code=res.status,
                    detail={"error_code": res.code, "error_message": res.message},
                )
        for tgt, res in zip(targets, results, strict=False):
            entry = tgt.entry
            if isinstance(res, BaseException):
                failed_entry = entry
                last_error = res if isinstance(res, Exception) else RuntimeError(str(res))
                failed_shard_desc = f"{entry.shard_id} ({entry.primary})"
                failed_shard_id = entry.shard_id
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
                    "error_message": f"Shard {failed_shard_id} is unavailable.",
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
            "error_message": f"Shard {failed_shard_id} is unavailable.",
        },
    )
