"""
HMAC-SHA256 authentication for inter-node RPC and admin endpoints.

Design:
- Cluster nodes authenticate each other with an HMAC-SHA256 signature in the
  ``X-Cluster-Signature`` header. The signature covers the HTTP method, the
  path, the query string and the raw body (see :func:`canonical_request`), so a
  signature captured for one endpoint can never be replayed against another
  (e.g. a read-only ``/internal/migration/checksum`` against ``.../cleanup``),
  and a body-less GET signature is not a universal token.
- Admin endpoints use a separate ``X-Admin-Token`` header.
- Client-facing router endpoints use ``X-Api-Key``.
- All comparisons use ``hmac.compare_digest`` (constant-time) to prevent
  timing-based side-channel attacks.

Limitations (documented in docs/security.md):
- No replay protection for the *same* request (no timestamp / nonce in the HMAC).
- Intended for a single-host Docker Compose network only.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Annotated
from urllib.parse import urlsplit

from fastapi import Header, HTTPException, Request

from .errors import AuthError

# ── Header names ──────────────────────────────────────────────────────────────

CLUSTER_SIG_HEADER = "X-Cluster-Signature"
ADMIN_TOKEN_HEADER = "X-Admin-Token"  # noqa: S105 (header name, not a secret)  # nosec B105
CLIENT_KEY_HEADER = "X-Api-Key"


# ── Core HMAC utilities ───────────────────────────────────────────────────────


def compute_signature(secret: str, body: bytes) -> str:
    """Compute HMAC-SHA256 of *body* using *secret*.

    Returns the lowercase hex digest.
    """
    return hmac.new(
        secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()


def canonical_request(method: str, path: str, query: str, body: bytes) -> bytes:
    """Bytes covered by a cluster request signature: method, path, query, body."""
    return f"{method.upper()}\n{path or '/'}\n{query}\n".encode() + body


def sign_request(secret: str, method: str, url: str, body: bytes = b"") -> str:
    """Signature for an outgoing cluster request to *url* (absolute URL or path).

    The host is not covered (nodes are addressed by changing host:port), but the
    method, path, query string and body are.
    """
    parts = urlsplit(url)
    return compute_signature(secret, canonical_request(method, parts.path, parts.query, body))


def cluster_headers(secret: str, method: str, url: str, body: bytes = b"") -> dict[str, str]:
    """Headers for an outgoing cluster request (JSON content type when a body is sent)."""
    headers = {CLUSTER_SIG_HEADER: sign_request(secret, method, url, body)}
    if body:
        headers["Content-Type"] = "application/json"
    return headers


def verify_request_signature(
    secret: str, method: str, path: str, query: str, body: bytes, provided: str
) -> bool:
    """Constant-time check of a request signature produced by :func:`sign_request`."""
    return verify_signature(secret, canonical_request(method, path, query, body), provided)


def verify_signature(secret: str, body: bytes, provided: str) -> bool:
    """Return True if *provided* matches the expected HMAC of *body*.

    Uses ``hmac.compare_digest`` for constant-time comparison.
    """
    expected = compute_signature(secret, body)
    return hmac.compare_digest(
        expected.lower().encode("utf-8"), provided.lower().encode("utf-8", errors="replace")
    )


def verify_token(expected_token: str, provided_token: str) -> bool:
    """Compare two tokens in constant time.

    Uses ``hmac.compare_digest`` to prevent timing attacks.
    """
    return hmac.compare_digest(
        expected_token.encode("utf-8"),
        provided_token.encode("utf-8"),
    )


# ── FastAPI dependency factories ─────────────────────────────────────────────


def make_cluster_auth_dependency(get_secret: ClusterSecretGetter) -> ClusterAuthDep:
    """Return a FastAPI dependency that validates the cluster HMAC signature.

    ``get_secret`` is a zero-argument callable returning the cluster secret string.
    This factory pattern lets tests inject a custom secret without touching env vars.

    Usage::

        from docstore.common.auth import make_cluster_auth_dependency
        from docstore.common.config import get_settings

        verify_cluster = make_cluster_auth_dependency(
            lambda: get_settings().CLUSTER_SECRET
        )

        @app.post("/internal/replicate", dependencies=[Depends(verify_cluster)])
        async def replicate(...): ...
    """

    async def _dependency(
        request: Request,
        x_cluster_signature: Annotated[str, Header(alias=CLUSTER_SIG_HEADER)] = "",
    ) -> None:
        if not x_cluster_signature:
            raise AuthError("Missing X-Cluster-Signature header.").to_http_exception()
        body = await request.body()
        secret = get_secret()
        if not verify_request_signature(
            secret,
            request.method,
            request.url.path,
            request.url.query,
            body,
            x_cluster_signature,
        ):
            raise AuthError("Invalid cluster signature.").to_http_exception()

    return _dependency  # type: ignore[return-value]


def make_admin_auth_dependency(get_token: AdminTokenGetter) -> AdminAuthDep:
    """Return a FastAPI dependency that validates the admin token.

    The admin token is compared using ``hmac.compare_digest``.
    """

    async def _dependency(
        x_admin_token: Annotated[str, Header(alias=ADMIN_TOKEN_HEADER)] = "",
    ) -> None:
        if not x_admin_token:
            raise AuthError("Missing X-Admin-Token header.").to_http_exception()
        expected = get_token()
        if not verify_token(expected, x_admin_token):
            raise AuthError("Invalid admin token.").to_http_exception()

    return _dependency  # type: ignore[return-value]


def make_client_auth_dependency(get_api_key: ApiKeyGetter) -> ClientAuthDep:
    """Return a FastAPI dependency that validates the client API key."""

    async def _dependency(
        x_api_key: Annotated[str, Header(alias=CLIENT_KEY_HEADER)] = "",
    ) -> None:
        if not x_api_key:
            raise HTTPException(
                status_code=401,
                detail={"error_code": "AUTH_FAILED", "error_message": "Missing X-Api-Key header."},
            )
        expected = get_api_key()
        if not verify_token(expected, x_api_key):
            raise HTTPException(
                status_code=401,
                detail={"error_code": "AUTH_FAILED", "error_message": "Invalid API key."},
            )

    return _dependency  # type: ignore[return-value]


# ── Type aliases (for documentation clarity) ─────────────────────────────────

from collections.abc import Callable  # noqa: E402

ClusterSecretGetter = Callable[[], str]
AdminTokenGetter = Callable[[], str]
ApiKeyGetter = Callable[[], str]
ClusterAuthDep = Callable[..., None]
AdminAuthDep = Callable[..., None]
ClientAuthDep = Callable[..., None]
