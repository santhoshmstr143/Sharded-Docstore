"""
Sorting and total-order implementation for Sharded-Docstore — Module 5.1.

Total sort order (contracts §5):
type rank (missing < null < boolean < number < string), then value.
Arrays and objects are not sortable.
doc_id is the deterministic tie-breaker for every sort.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Any

from docstore.common.errors import InvalidQuery
from docstore.query.evaluator import get_dotted_field, is_bool, is_number, is_string


class TypeRank(IntEnum):
    MISSING = 0
    NULL = 1
    BOOLEAN = 2
    NUMBER = 3
    STRING = 4


@dataclass(frozen=True)
class SortValue:
    """Comparable container for a document's sort-field value and tie-breaker."""

    rank: TypeRank
    val: Any
    doc_id: int
    descending: bool = False

    def __lt__(self, other: SortValue) -> bool:
        if not self.descending:
            if self.rank != other.rank:
                return self.rank < other.rank
            if self.val != other.val:
                return bool(self.val < other.val)
            return self.doc_id < other.doc_id
        else:
            if self.rank != other.rank:
                return self.rank > other.rank
            if self.val != other.val:
                return bool(self.val > other.val)
            # doc_id remains deterministic tie-breaker
            return self.doc_id < other.doc_id


def extract_sort_key(
    doc: dict[str, Any],
    sort_field: str | None,
    sort_dir: str = "asc",
) -> SortValue:
    """Compute the SortValue for a document."""
    doc_id = int(doc["doc_id"])
    descending = sort_dir.lower() == "desc"

    if sort_field is None or sort_field == "doc_id":
        return SortValue(
            rank=TypeRank.NUMBER,
            val=doc_id,
            doc_id=doc_id,
            descending=descending,
        )

    found, raw_val = get_dotted_field(doc, sort_field)
    if not found:
        return SortValue(
            rank=TypeRank.MISSING,
            val=0,
            doc_id=doc_id,
            descending=descending,
        )

    if raw_val is None:
        return SortValue(
            rank=TypeRank.NULL,
            val=0,
            doc_id=doc_id,
            descending=descending,
        )

    if is_bool(raw_val):
        return SortValue(
            rank=TypeRank.BOOLEAN,
            val=1 if raw_val else 0,
            doc_id=doc_id,
            descending=descending,
        )

    if is_number(raw_val):
        return SortValue(
            rank=TypeRank.NUMBER,
            val=raw_val,
            doc_id=doc_id,
            descending=descending,
        )

    if is_string(raw_val):
        return SortValue(
            rank=TypeRank.STRING,
            val=raw_val,
            doc_id=doc_id,
            descending=descending,
        )

    raise InvalidQuery(f"Field {sort_field!r} has non-sortable type {type(raw_val).__name__!r}.")


def sort_documents(
    docs: list[dict[str, Any]],
    sort_field: str | None = None,
    sort_dir: str = "asc",
) -> list[dict[str, Any]]:
    """Sort a list of documents using the contract's total order and tie-breaker."""
    return sorted(docs, key=lambda d: extract_sort_key(d, sort_field, sort_dir))
