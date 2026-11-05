"""
Typed exceptions and HTTP error mappings for Sharded-Docstore.

All errors map to a structured JSON body:
    {"error_code": "<CODE>", "error_message": "<human text>"}

Stack traces are NEVER included in HTTP responses; log them separately.
"""

from __future__ import annotations

from typing import ClassVar

from fastapi import HTTPException


class DocstoreError(Exception):
    """Base class for all application-level errors."""

    http_status: ClassVar[int] = 500
    error_code: ClassVar[str] = "INTERNAL_ERROR"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def to_http_exception(self) -> HTTPException:
        """Convert to a FastAPI HTTPException with a safe error body."""
        return HTTPException(
            status_code=self.http_status,
            detail={
                "error_code": self.error_code,
                "error_message": self.message,
                # Never add traceback or internal state here.
            },
        )


# ── Fencing / ownership errors (409) ─────────────────────────────────────────


class StaleEpoch(DocstoreError):
    """Write carries an ``ownership_epoch`` behind the current epoch."""

    http_status = 409
    error_code = "STALE_EPOCH"


class StaleTerm(DocstoreError):
    """Write or replication message carries a ``term`` behind the current term."""

    http_status = 409
    error_code = "STALE_TERM"


class NotPrimary(DocstoreError):
    """Request arrived at a node that is not currently the primary."""

    http_status = 409
    error_code = "NOT_PRIMARY"


class LeaseExpired(DocstoreError):
    """The primary's lease has expired; it cannot serve primary reads or writes."""

    http_status = 409
    error_code = "LEASE_EXPIRED"


class RequestIdReused(DocstoreError):
    """The same ``request_id`` was reused for a write with different content."""

    http_status = 409
    error_code = "REQUEST_ID_REUSED"


class DuplicateKey(DocstoreError):
    """Insert attempted with a ``doc_id`` that already exists."""

    http_status = 409
    error_code = "DUPLICATE_KEY"


# ── Client / routing errors (4xx) ────────────────────────────────────────────


class InvalidQuery(DocstoreError):
    """Query filter violates grammar, depth, clause, or ``$in`` size limits."""

    http_status = 400
    error_code = "INVALID_QUERY"


class NotFound(DocstoreError):
    """Document with the requested ``doc_id`` does not exist."""

    http_status = 404
    error_code = "NOT_FOUND"


class OutOfRange(DocstoreError):
    """``doc_id`` falls outside all ranges owned by this shard."""

    http_status = 400
    error_code = "OUT_OF_RANGE"


class AuthError(DocstoreError):
    """Invalid cluster secret, admin token, or client API key."""

    http_status = 401
    error_code = "AUTH_FAILED"


class BodyTooLarge(DocstoreError):
    """Request body or document exceeds the configured size limit."""

    http_status = 413
    error_code = "BODY_TOO_LARGE"


class DocTooLarge(DocstoreError):
    """A single document's serialized size exceeds MAX_DOC_SIZE_BYTES."""

    http_status = 413
    error_code = "DOC_TOO_LARGE"


class ValidationError(DocstoreError):
    """Pydantic input validation failure (wrong types, missing fields, etc.)."""

    http_status = 422
    error_code = "VALIDATION_ERROR"


# ── Availability / replication errors (503) ───────────────────────────────────


class RangePaused(DocstoreError):
    """Range is paused for live migration; writes are temporarily rejected."""

    http_status = 503
    error_code = "RANGE_PAUSED"


class ReplicationUnavailable(DocstoreError):
    """No ``SYNCED`` follower exists; write rejected *before* WAL append.

    Outcome is definite (``REJECTED``); safe to retry with the same ``request_id``.
    """

    http_status = 503
    error_code = "REPLICATION_UNAVAILABLE"


# ── Uncertain outcome (202) ───────────────────────────────────────────────────


class OutcomeUnknown(DocstoreError):
    """Write was WAL-appended on the primary but no durable follower ack received.

    The write *may* later commit (if the follower catches up) or be discarded (if
    a failover occurs first).  The client MUST retry with the same ``request_id``.
    """

    http_status = 202
    error_code = "OUTCOME_UNKNOWN"
