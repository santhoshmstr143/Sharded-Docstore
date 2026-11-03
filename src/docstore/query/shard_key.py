"""
Shard-key constraint detector for Sharded-Docstore — Module 5.1 / 5.3.

Analyzes filter AST to determine whether the query pins doc_id:
- Exact doc_id (direct routing to 1 shard)
- $in doc_ids (route only to shards owning those IDs)
- Range on doc_id (route only to overlapping shards)
- Unconstrained / complex (scatter to all shards)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from docstore.common.models import DOC_ID_MAX, DOC_ID_MIN
from docstore.query.evaluator import is_number

KEY_SPACE_START = DOC_ID_MIN
KEY_SPACE_END = DOC_ID_MAX + 1


@dataclass(frozen=True)
class ShardKeyConstraint:
    kind: str  # "exact" | "in" | "range" | "all"
    doc_id: int | None = None
    doc_ids: list[int] | None = None
    range_start: int | None = None  # inclusive
    range_end: int | None = None  # exclusive


def _finite(x: Any) -> bool:
    return is_number(x) and math.isfinite(x)


def detect_shard_key_constraint(filter_dict: dict[str, Any]) -> ShardKeyConstraint:
    """Analyze filter to extract doc_id constraints."""
    if not filter_dict:
        return ShardKeyConstraint(kind="all")

    # 1. Direct doc_id in top-level dict
    if "doc_id" in filter_dict:
        val = filter_dict["doc_id"]
        if _finite(val):
            if val != int(val):  # fractional doc_id can never match an integer key
                return ShardKeyConstraint(kind="in", doc_ids=[])
            return ShardKeyConstraint(kind="exact", doc_id=int(val))

        if isinstance(val, dict):
            if "$eq" in val and _finite(val["$eq"]):
                if val["$eq"] != int(val["$eq"]):
                    return ShardKeyConstraint(kind="in", doc_ids=[])
                return ShardKeyConstraint(kind="exact", doc_id=int(val["$eq"]))

            if "$in" in val and isinstance(val["$in"], list):
                # Filter to valid int IDs
                ids = [int(x) for x in val["$in"] if _finite(x) and x == int(x)]
                return ShardKeyConstraint(kind="in", doc_ids=ids)

            has_gt = "$gt" in val and _finite(val["$gt"])
            has_lt = "$lt" in val and _finite(val["$lt"])

            if has_gt or has_lt:
                # doc_id > g  <=>  doc_id >= floor(g) + 1 ;  doc_id < l  <=>  doc_id < ceil(l)
                start = math.floor(val["$gt"]) + 1 if has_gt else KEY_SPACE_START
                end = math.ceil(val["$lt"]) if has_lt else KEY_SPACE_END
                # Clamp to valid key space
                start = max(start, KEY_SPACE_START)
                end = min(end, KEY_SPACE_END)
                if start >= end:
                    # Empty range
                    return ShardKeyConstraint(kind="range", range_start=start, range_end=start)
                return ShardKeyConstraint(kind="range", range_start=start, range_end=end)

    # 2. Check top-level $and
    if "$and" in filter_dict and isinstance(filter_dict["$and"], list):
        for subclause in filter_dict["$and"]:
            if isinstance(subclause, dict):
                sub_res = detect_shard_key_constraint(subclause)
                if sub_res.kind != "all":
                    return sub_res

    return ShardKeyConstraint(kind="all")
