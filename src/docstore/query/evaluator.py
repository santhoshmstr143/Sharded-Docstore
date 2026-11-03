"""
Pure-Python query evaluator for Sharded-Docstore — Module 5.1.

Evaluates query filter ASTs against documents.
Strict type safety: booleans are not numbers; missing fields never match;
null matches only explicit null.
No eval, exec, pickle, or user regex.
"""

from __future__ import annotations

from typing import Any


def get_dotted_field(doc: dict[str, Any], path: str) -> tuple[bool, Any]:
    """Traverse a dotted path in a nested document.

    Returns
    -------
    tuple[bool, Any]
        (found, value): found is True if the path exists, False otherwise.
    """
    parts = path.split(".")
    curr: Any = doc
    for part in parts:
        if not isinstance(curr, dict) or part not in curr:
            return (False, None)
        curr = curr[part]
    return (True, curr)


def is_bool(val: Any) -> bool:
    """True if val is specifically a boolean (Python bool)."""
    return isinstance(val, bool)


def is_number(val: Any) -> bool:
    """True if val is an int or float, but NOT a boolean."""
    return isinstance(val, (int, float)) and not isinstance(val, bool)


def is_string(val: Any) -> bool:
    """True if val is a string."""
    return isinstance(val, str)


def type_safe_equal(doc_val: Any, target_val: Any) -> bool:
    """Compare two values for equality respecting type boundaries.

    In Python, True == 1 and False == 0. Here, booleans and numbers never match.
    """
    if is_bool(doc_val):
        return is_bool(target_val) and (doc_val is target_val or doc_val == target_val)
    if is_bool(target_val):
        return False

    if is_number(doc_val):
        return is_number(target_val) and doc_val == target_val
    if is_number(target_val):
        return False

    if is_string(doc_val):
        return is_string(target_val) and doc_val == target_val
    if is_string(target_val):
        return False

    if doc_val is None:
        return target_val is None
    if target_val is None:
        return False

    # Lists and dicts: recursive, so [1] never equals [True] and {"x":1} never equals {"x":True}
    if isinstance(doc_val, list) and isinstance(target_val, list):
        return len(doc_val) == len(target_val) and all(
            type_safe_equal(a, b) for a, b in zip(doc_val, target_val, strict=True)
        )
    if isinstance(doc_val, dict) and isinstance(target_val, dict):
        return doc_val.keys() == target_val.keys() and all(
            type_safe_equal(doc_val[k], target_val[k]) for k in doc_val
        )

    return False


def _eval_operator(op: str, doc_val: Any, found: bool, target: Any) -> bool:
    """Evaluate a single operator against a document field value.

    Contracts:
    - Missing field never matches any operator.
    - null matches only an explicit null value.
    - $gt / $gte / $lt / $lte compare only same-type scalars (number ↔ number,
      string ↔ string).  Booleans are NOT numbers.
    """
    if not found:
        # A missing field never matches any operator
        return False

    if op == "$eq":
        return type_safe_equal(doc_val, target)

    if op == "$gt":
        if is_number(doc_val) and is_number(target):
            return bool(doc_val > target)
        if is_string(doc_val) and is_string(target):
            return bool(doc_val > target)
        return False

    if op == "$gte":
        if is_number(doc_val) and is_number(target):
            return bool(doc_val >= target)
        if is_string(doc_val) and is_string(target):
            return bool(doc_val >= target)
        return False

    if op == "$lt":
        if is_number(doc_val) and is_number(target):
            return bool(doc_val < target)
        if is_string(doc_val) and is_string(target):
            return bool(doc_val < target)
        return False

    if op == "$lte":
        if is_number(doc_val) and is_number(target):
            return bool(doc_val <= target)
        if is_string(doc_val) and is_string(target):
            return bool(doc_val <= target)
        return False

    if op == "$in":
        if not isinstance(target, list):
            return False
        return any(type_safe_equal(doc_val, item) for item in target)

    return False


def evaluate_filter(doc: dict[str, Any], filter_dict: dict[str, Any]) -> bool:
    """Evaluate whether doc satisfies the filter conditions.

    Parameters
    ----------
    doc : dict[str, Any]
        Document dictionary.
    filter_dict : dict[str, Any]
        Validated filter specification.
    """
    if not filter_dict:
        return True

    for key, condition in filter_dict.items():
        if key == "$and":
            if not isinstance(condition, list):
                return False
            if not all(evaluate_filter(doc, subclause) for subclause in condition):
                return False
        elif key == "$or":
            if not isinstance(condition, list):
                return False
            if not any(evaluate_filter(doc, subclause) for subclause in condition):
                return False
        else:
            # Field condition: dotted-path lookup
            found, doc_val = get_dotted_field(doc, key)

            if isinstance(condition, dict):
                has_ops = any(k.startswith("$") for k in condition.keys())
                if has_ops:
                    # All operator conditions on this field must match
                    for op, target in condition.items():
                        if not _eval_operator(op, doc_val, found, target):
                            return False
                else:
                    # Object literal equality
                    if not _eval_operator("$eq", doc_val, found, condition):
                        return False
            else:
                # Shorthand equality: {"field": value}
                if not _eval_operator("$eq", doc_val, found, condition):
                    return False

    return True
