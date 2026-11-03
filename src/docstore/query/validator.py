"""
Query filter validator for Sharded-Docstore — Module 5.1.

Enforces grammar, operator whitelist, nesting depth, clause caps, and $in size limits.
Pure Python — no eval, exec, pickle, or user regex.
"""

from __future__ import annotations

import math
from typing import Any

from docstore.common.errors import InvalidQuery
from docstore.common.models import (
    FILTER_IN_MAX_SIZE,
    FILTER_MAX_CLAUSES,
    FILTER_MAX_DEPTH,
)

WHITELIST_LOGICAL_OPS = frozenset({"$and", "$or"})
WHITELIST_VALUE_OPS = frozenset({"$eq", "$gt", "$gte", "$lt", "$lte", "$in"})
WHITELIST_ALL_OPS = WHITELIST_LOGICAL_OPS | WHITELIST_VALUE_OPS


def _reject_non_finite(value: Any, depth: int = 0) -> None:
    """NaN / Infinity are not valid JSON and break routing and comparisons."""
    if isinstance(value, float) and not math.isfinite(value):
        raise InvalidQuery("Non-finite numbers (NaN/Infinity) are not allowed in filters.")
    if depth > 20:
        raise InvalidQuery("Filter value nesting is too deep.")
    if isinstance(value, dict):
        for v in value.values():
            _reject_non_finite(v, depth + 1)
    elif isinstance(value, list):
        for v in value:
            _reject_non_finite(v, depth + 1)


def validate_query_filter(filter_dict: Any) -> None:
    """Validate a query filter dict against grammar and security limits.

    Raises :class:`docstore.common.errors.InvalidQuery` on any violation.
    """
    if not isinstance(filter_dict, dict):
        raise InvalidQuery(f"Filter must be a dictionary, got {type(filter_dict).__name__!r}.")

    if not filter_dict:
        # Empty filter is valid (matches all documents)
        return

    _reject_non_finite(filter_dict)
    clause_count = 0

    def _validate_node(node: Any, current_depth: int) -> None:
        nonlocal clause_count

        if current_depth > FILTER_MAX_DEPTH:
            raise InvalidQuery(
                f"Filter depth {current_depth} exceeds maximum allowed depth of {FILTER_MAX_DEPTH}."
            )

        if not isinstance(node, dict):
            raise InvalidQuery(f"Expected clause dictionary, got {type(node).__name__!r}.")

        for key, value in node.items():
            if not isinstance(key, str) or not key:
                raise InvalidQuery("Field or operator keys must be non-empty strings.")

            clause_count += 1
            if clause_count > FILTER_MAX_CLAUSES:
                raise InvalidQuery(
                    f"Clause count {clause_count} exceeds maximum allowed of {FILTER_MAX_CLAUSES}."
                )

            if key.startswith("$"):
                # Must be a recognized top-level / logical operator
                if key not in WHITELIST_LOGICAL_OPS:
                    raise InvalidQuery(f"Unknown or misplaced operator: {key!r}.")

                if not isinstance(value, list) or len(value) == 0:
                    raise InvalidQuery(f"Operator {key!r} requires a non-empty list of clauses.")

                for subclause in value:
                    _validate_node(subclause, current_depth + 1)
            else:
                # Field condition
                if isinstance(value, dict):
                    # Could be operator expressions like {"$gt": 5, "$lt": 10}
                    has_ops = any(k.startswith("$") for k in value.keys())
                    if has_ops:
                        _validate_field_operators(key, value, current_depth + 1)
                    else:
                        # Exact match on an object literal: {"field": {"a": 1}}
                        pass
                elif isinstance(value, (int, float, str, bool, type(None), list)):
                    # Exact equality shorthand: {"field": value}
                    pass
                else:
                    raise InvalidQuery(
                        f"Unsupported value type for field {key!r}: {type(value).__name__!r}."
                    )

    def _validate_field_operators(field: str, op_dict: dict[str, Any], current_depth: int) -> None:
        nonlocal clause_count

        if current_depth > FILTER_MAX_DEPTH:
            raise InvalidQuery(
                f"Filter depth {current_depth} exceeds maximum allowed depth of {FILTER_MAX_DEPTH}."
            )

        for op, val in op_dict.items():
            if not op.startswith("$"):
                raise InvalidQuery(
                    f"Cannot mix operator and non-operator keys under field {field!r}."
                )

            clause_count += 1
            if clause_count > FILTER_MAX_CLAUSES:
                raise InvalidQuery(
                    f"Clause count {clause_count} exceeds maximum allowed of {FILTER_MAX_CLAUSES}."
                )

            if op not in WHITELIST_VALUE_OPS:
                raise InvalidQuery(f"Unknown operator {op!r} on field {field!r}.")

            if op == "$in":
                if not isinstance(val, list):
                    raise InvalidQuery(
                        f"Operator '$in' requires a list, got {type(val).__name__!r}."
                    )
                if len(val) > FILTER_IN_MAX_SIZE:
                    raise InvalidQuery(
                        f"'$in' array size {len(val)} exceeds max allowed of {FILTER_IN_MAX_SIZE}."
                    )
            elif op in ("$gt", "$gte", "$lt", "$lte"):
                # Scalars only: int, float, str. Booleans are not allowed for comparison ops
                if isinstance(val, bool) or not isinstance(val, (int, float, str)):
                    raise InvalidQuery(
                        f"Operator {op!r} requires numeric or string scalar, "
                        f"got {type(val).__name__!r}."
                    )
            elif op == "$eq":
                # Any valid scalar or None
                if not isinstance(val, (int, float, str, bool, type(None), list, dict)):
                    raise InvalidQuery(f"Unsupported value type for '$eq': {type(val).__name__!r}.")

    _validate_node(filter_dict, 1)
