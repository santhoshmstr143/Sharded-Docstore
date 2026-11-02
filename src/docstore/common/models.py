"""
Pydantic v2 data models for Sharded-Docstore.

All models used across components (shard node, router, coordinator, client)
are defined here so that interface contracts are enforced consistently.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# ── Constants ─────────────────────────────────────────────────────────────────

DOC_ID_MIN: int = 0
DOC_ID_MAX: int = (2**63) - 1

MAX_DOC_SIZE_BYTES: int = 1 * 1024 * 1024  # 1 MB
MAX_BODY_SIZE_BYTES: int = 2 * 1024 * 1024  # 2 MB

QUERY_LIMIT_DEFAULT: int = 100
QUERY_LIMIT_MAX: int = 1_000
QUERY_SKIP_MAX: int = 10_000
FILTER_MAX_DEPTH: int = 5
FILTER_MAX_CLAUSES: int = 50
FILTER_IN_MAX_SIZE: int = 1_000


# ── Enums ─────────────────────────────────────────────────────────────────────


class WriteOp(StrEnum):
    """Supported write operations stored in the WAL."""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


class NodeRole(StrEnum):
    """Role of a shard node."""

    PRIMARY = "primary"
    FOLLOWER = "follower"


class ReplicaState(StrEnum):
    """Replication state of the follower as seen by the primary."""

    SYNCED = "SYNCED"
    CATCHING_UP = "CATCHING_UP"
    DOWN = "DOWN"


class WriteOutcome(StrEnum):
    """Three possible outcomes of a write operation (see contracts §6)."""

    COMMITTED = "COMMITTED"
    REJECTED = "REJECTED"
    OUTCOME_UNKNOWN = "outcome_unknown"


class ReadMode(StrEnum):
    """Whether the client wants a primary or follower read."""

    PRIMARY = "primary"
    FOLLOWER = "follower"


class MigrationState(StrEnum):
    """State machine for live range migration."""

    PLANNED = "PLANNED"
    SNAPSHOT_COPY = "SNAPSHOT_COPY"
    CATCHUP = "CATCHUP"
    PAUSE = "PAUSE"
    FINAL_CATCHUP = "FINAL_CATCHUP"
    VERIFY = "VERIFY"
    CUTOVER = "CUTOVER"
    CLEANUP = "CLEANUP"
    DONE = "DONE"
    ABORTED = "ABORTED"


# ── Shared validator for doc_id ───────────────────────────────────────────────


def _validate_doc_id(v: Any) -> int:
    """Validate a doc_id value.

    Rejects booleans (Python treats True as int(1)), non-integers,
    negatives, and values >= 2^63.
    """
    # Must check bool BEFORE int: bool is a subclass of int in Python
    if isinstance(v, bool):
        raise ValueError(
            "doc_id must be an integer, not a boolean; "
            "Python treats True/False as 1/0 but we reject them explicitly."
        )
    if not isinstance(v, int):
        raise ValueError(f"doc_id must be a non-negative integer, got {type(v).__name__!r}.")
    if v < DOC_ID_MIN:
        raise ValueError(f"doc_id must be a non-negative integer (>= 0), got {v}.")
    if v > DOC_ID_MAX:
        raise ValueError(f"doc_id must be <= {DOC_ID_MAX} (2^63-1), got {v}.")
    return v


# ── Core document models ──────────────────────────────────────────────────────


class Document(BaseModel):
    """A stored JSON document.

    ``doc_id`` is the shard key (non-negative 64-bit integer).
    All other fields are arbitrary JSON and stored as-is.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    doc_id: int = Field(description="Shard key; non-negative 64-bit integer.")

    @field_validator("doc_id", mode="before")
    @classmethod
    def validate_doc_id(cls, v: Any) -> int:
        """Reject booleans, non-integers, negatives, and out-of-range values."""
        return _validate_doc_id(v)

    def extra_fields(self) -> dict[str, Any]:
        """Return fields beyond ``doc_id``."""
        return dict(self.model_extra) if self.model_extra else {}


class WriteRequest(BaseModel):
    """Payload for a client write sent to the router / shard node."""

    request_id: str = Field(
        min_length=1,
        max_length=128,
        description="Client-generated UUID4; retries reuse the same value.",
    )
    op: WriteOp
    document: Document | None = Field(
        default=None,
        description="Full document body; None for DELETE operations.",
    )
    # Epoch / term — optional at the router level; required at the shard level.
    # Filled in by the router from the shard map; shard validates them.
    ownership_epoch: int | None = Field(default=None, ge=0)
    term: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _check_document_present_for_write(self) -> WriteRequest:
        if self.op in (WriteOp.INSERT, WriteOp.UPDATE) and self.document is None:
            raise ValueError(f"'document' is required for op={self.op.value!r}.")
        return self


class QueryFilter(BaseModel):
    """A filter expression passed to the query engine.

    Kept opaque here (``dict[str, Any]``); structural validation is
    performed by ``docstore.query.validator``.
    """

    filter: dict[str, Any] = Field(default_factory=dict)
    sort_field: str | None = Field(default=None, max_length=256)
    sort_dir: str = Field(default="asc", pattern=r"^(asc|desc)$")
    skip: int = Field(default=0, ge=0, le=QUERY_SKIP_MAX)
    limit: int = Field(default=QUERY_LIMIT_DEFAULT, ge=1, le=QUERY_LIMIT_MAX)
    read_mode: ReadMode = ReadMode.PRIMARY


# ── Shard map models ─────────────────────────────────────────────────────────


class ShardRange(BaseModel):
    """A half-open key range ``[start, end)``."""

    start: int = Field(ge=0, description="Inclusive lower bound.")
    end: int = Field(ge=1, description="Exclusive upper bound.")

    @model_validator(mode="after")
    def _check_order(self) -> ShardRange:
        if self.start >= self.end:
            raise ValueError(
                f"Shard range must satisfy start < end; got [{self.start}, {self.end})."
            )
        return self


class ShardMapEntry(BaseModel):
    """One entry in the cluster shard map."""

    shard_id: str
    range: ShardRange
    primary: str = Field(description="host:port of the primary node.")
    follower: str | None = Field(
        default=None, description="host:port of the follower node; None during setup."
    )
    ownership_epoch: int = Field(ge=0)
    map_version: int = Field(ge=0)


class ShardMap(BaseModel):
    """The full cluster shard map returned by the coordinator."""

    map_version: int = Field(ge=0)
    entries: list[ShardMapEntry]

    @model_validator(mode="after")
    def _validate_coverage(self) -> ShardMap:
        """Ensure entries are non-overlapping and together cover [0, 2^63)."""
        if not self.entries:
            raise ValueError("Shard map must have at least one entry.")
        sorted_entries = sorted(self.entries, key=lambda e: e.range.start)
        expected_start = 0
        for entry in sorted_entries:
            if entry.range.start != expected_start:
                raise ValueError(
                    f"Gap or overlap in shard map: expected start={expected_start}, "
                    f"got {entry.range.start} for shard {entry.shard_id!r}."
                )
            expected_start = entry.range.end
        if expected_start != 2**63:
            raise ValueError(
                f"Shard map does not cover full key space [0, 2^63); "
                f"coverage ends at {expected_start}."
            )
        return self


# ── WAL / replication models ─────────────────────────────────────────────────


class OplogEntry(BaseModel):
    """One record in the Write-Ahead Log (WAL) and replication stream."""

    seq: int = Field(ge=1, description="Gap-free sequence number starting at 1.")
    term: int = Field(ge=1, description="Shard term; bumped on each primary change.")
    op: WriteOp
    doc_id: int = Field(ge=DOC_ID_MIN, le=DOC_ID_MAX)
    request_id: str = Field(min_length=1, max_length=128)
    payload: dict[str, Any] | None = Field(
        default=None, description="Document body for insert/update; None for delete."
    )
    crc32: int = Field(description="CRC32 over the serialised record (excluding crc32 itself).")

    @field_validator("doc_id", mode="before")
    @classmethod
    def validate_doc_id(cls, v: Any) -> int:
        return _validate_doc_id(v)


# ── Heartbeat / lease models ─────────────────────────────────────────────────


class Heartbeat(BaseModel):
    """Sent by each primary to the coordinator every H seconds."""

    node_id: str
    shard_id: str
    role: NodeRole
    term: int = Field(ge=0)
    last_applied_seq: int = Field(ge=0)
    follower_acked_seq: int = Field(
        ge=0,
        description="Highest seq durably acked by the follower (for WAL retention).",
    )
    follower_state: ReplicaState = Field(
        description="State of the follower as observed by the primary."
    )
    timestamp_monotonic: float = Field(
        description="Monotonic clock value on the sending node (for elapsed-time only)."
    )


class LeaseGrant(BaseModel):
    """Lease granted by the coordinator to a primary."""

    node_id: str
    shard_id: str
    term: int = Field(ge=0)
    granted_at: float = Field(description="Coordinator wall-clock time of grant.")
    expires_at: float = Field(description="Coordinator wall-clock time of expiry.")
    map_version: int = Field(ge=0)


class NodeRegistration(BaseModel):
    """Payload sent by a node to register with the coordinator."""

    node_id: str
    shard_id: str
    address: str = Field(description="host:port for reaching this node.")
    requested_role: NodeRole | None = Field(default=None)


class RegistrationResponse(BaseModel):
    """Returned by the coordinator after successful registration."""

    node_id: str
    shard_id: str
    role: NodeRole
    term: int = Field(ge=0)
    ranges: list[ShardRange]
    lease: LeaseGrant | None = None
    map_version: int = Field(ge=0)


class ReplicationRequest(BaseModel):
    """Replication payload sent from primary to follower."""

    shard_id: str
    term: int = Field(ge=0)
    seq: int = Field(ge=1)
    op: WriteOp
    doc_id: int = Field(ge=DOC_ID_MIN, le=DOC_ID_MAX)
    request_id: str = Field(min_length=1, max_length=128)
    payload: dict[str, Any] | None = None


class ReplicationResponse(BaseModel):
    """Acknowledgement returned by the follower."""

    status: str = Field(description="'ok', 'gap', or 'stale_term'")
    seq: int = Field(ge=0)
    last_seq: int | None = None
    error_message: str | None = None


class CatchupRequest(BaseModel):
    """Catchup request sent by a follower to fetch missing oplog entries."""

    shard_id: str
    after_seq: int = Field(ge=0)


class CatchupResponse(BaseModel):
    """Catchup response containing WAL entries or snapshot fallback notice."""

    entries: list[dict[str, Any]]
    needs_snapshot: bool = False
    last_seq: int = Field(ge=0)


# ── Response envelopes ────────────────────────────────────────────────────────


class ResponseMeta(BaseModel):
    """Metadata attached to every response (contracts §9)."""

    served_by: str
    term: int = Field(ge=0)
    ownership_epoch: int = Field(ge=0)
    applied_seq: int = Field(ge=0)
    replication_lag: int | None = Field(
        default=None,
        description="Ops behind primary; None for primary reads.",
    )


class WriteResponse(BaseModel):
    """Response returned to the client for every write operation.

    Semantics:
      - COMMITTED: Write was durably replicated and committed; seq is a non-null integer.
      - REJECTED: Write was rejected before state mutation; seq is None.
      - OUTCOME_UNKNOWN: Write was appended locally but replication outcome could not be
        confirmed; client must resolve using request_id or retry safely.
    """

    outcome: WriteOutcome
    request_id: str
    seq: int | None = None
    meta: ResponseMeta | None = None
    error_code: str | None = None
    error_message: str | None = None

    @model_validator(mode="after")
    def _validate_outcome_semantics(self) -> WriteResponse:
        if self.outcome == WriteOutcome.COMMITTED and self.seq is None:
            raise ValueError("COMMITTED writes must provide a valid seq.")
        if self.outcome == WriteOutcome.REJECTED and self.seq is not None:
            raise ValueError("REJECTED writes must not provide a seq.")
        return self


class ReadResponse(BaseModel):
    """Response returned to the client for a GET /doc/{id} request."""

    document: Document | None
    meta: ResponseMeta


class ErrorResponse(BaseModel):
    """Structured error body — never contains stack traces."""

    error_code: str
    error_message: str


class QueryResponse(BaseModel):
    """Response returned for a POST /query request."""

    documents: list[dict[str, Any]]
    total: int
    meta: ResponseMeta | None = None
