"""
Shared async HTTP client for inter-node RPC.

Rules (from the security spec):
- Every outbound call has an explicit timeout.
- Retries are bounded (``MAX_RETRIES``); exponential backoff with jitter.
- Connection pool is capped to avoid resource exhaustion.
- 5xx responses and connection errors are retried; 4xx are not (they are
  definitive negative answers).
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import httpx

# ── Defaults ─────────────────────────────────────────────────────────────────

DEFAULT_TIMEOUT = httpx.Timeout(
    connect=2.0,
    read=5.0,
    write=5.0,
    pool=1.0,
)

CONNECTION_LIMITS = httpx.Limits(
    max_connections=20,
    max_keepalive_connections=10,
    keepalive_expiry=30.0,
)

MAX_RETRIES: int = 3
BACKOFF_BASE: float = 0.05  # 50 ms base
BACKOFF_MAX: float = 2.0  # cap at 2 s
BACKOFF_JITTER: float = 0.05  # ±50 ms jitter


# ── Client factory ────────────────────────────────────────────────────────────


def make_client(
    *,
    timeout: httpx.Timeout = DEFAULT_TIMEOUT,
    limits: httpx.Limits = CONNECTION_LIMITS,
    auth_headers: dict[str, str] | None = None,
) -> httpx.AsyncClient:
    """Create an ``httpx.AsyncClient`` with project-standard settings.

    Args:
        timeout:       Per-operation timeout configuration.
        limits:        Connection pool limits.
        auth_headers:  Default headers added to every request (e.g. the
                       cluster signature header *key* — the value is computed
                       per request by the caller).

    Returns:
        An :class:`httpx.AsyncClient` that must be closed by the caller
        (use :func:`client_session` for automatic lifecycle management).
    """
    return httpx.AsyncClient(
        timeout=timeout,
        limits=limits,
        headers=auth_headers or {},
        follow_redirects=False,
    )


@asynccontextmanager
async def client_session(
    *,
    timeout: httpx.Timeout = DEFAULT_TIMEOUT,
    auth_headers: dict[str, str] | None = None,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Async context manager that creates and closes an httpx client."""
    async with make_client(timeout=timeout, auth_headers=auth_headers) as client:
        yield client


# ── Retry helper ─────────────────────────────────────────────────────────────


async def post_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    json: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    max_retries: int = MAX_RETRIES,
) -> httpx.Response:
    """POST *json* to *url* with bounded exponential backoff + jitter.

    Retries on:
    - ``httpx.ConnectError``, ``httpx.TimeoutException``, ``httpx.RemoteProtocolError``
    - HTTP 5xx responses

    Does NOT retry on 4xx (definitive client errors).

    Raises:
        httpx.HTTPStatusError: On non-retryable 4xx responses.
        httpx.TransportError:  If all retry attempts fail with a transport error.
    """
    last_exc: Exception | None = None
    last_response: httpx.Response | None = None

    for attempt in range(max_retries + 1):
        try:
            response = await client.post(url, json=json, headers=headers)
            if response.status_code < 500:
                # Success or a definitive 4xx — return immediately, no retry.
                return response
            # 5xx — fall through to retry logic
            last_response = response
        except (
            httpx.ConnectError,
            httpx.TimeoutException,
            httpx.RemoteProtocolError,
        ) as exc:
            last_exc = exc

        if attempt < max_retries:
            delay = min(
                BACKOFF_BASE * (2**attempt) + random.uniform(0, BACKOFF_JITTER),  # noqa: S311 - backoff jitter only
                BACKOFF_MAX,
            )
            await asyncio.sleep(delay)

    # Exhausted retries
    if last_exc is not None:
        raise last_exc
    assert last_response is not None  # guaranteed if we entered the loop
    return last_response


async def get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    max_retries: int = MAX_RETRIES,
) -> httpx.Response:
    """GET *url* with bounded exponential backoff + jitter.

    Retry logic mirrors :func:`post_with_retry`.
    """
    last_exc: Exception | None = None
    last_response: httpx.Response | None = None

    for attempt in range(max_retries + 1):
        try:
            response = await client.get(url, params=params, headers=headers)
            if response.status_code < 500:
                return response
            last_response = response
        except (
            httpx.ConnectError,
            httpx.TimeoutException,
            httpx.RemoteProtocolError,
        ) as exc:
            last_exc = exc

        if attempt < max_retries:
            delay = min(
                BACKOFF_BASE * (2**attempt) + random.uniform(0, BACKOFF_JITTER),  # noqa: S311
                BACKOFF_MAX,
            )
            await asyncio.sleep(delay)

    if last_exc is not None:
        raise last_exc
    assert last_response is not None
    return last_response


# ── Request body limit (ASGI) ────────────────────────────────────────────────


class BodyLimitMiddleware:
    """Enforce a maximum request body size even when no Content-Length is sent.

    Requests that declare ``Content-Length`` (checked by the app middleware) or use
    ``Transfer-Encoding`` (rejected with 411 by the app middleware) pass through.
    A request that declares neither is buffered here up to ``max_bytes``: one byte
    more and it is rejected with 413 before the application sees it, so memory use
    stays bounded and the limit cannot be bypassed by omitting the header.
    """

    def __init__(self, app: Any, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        names = {k.lower() for k, _ in scope.get("headers", [])}
        if b"content-length" in names or b"transfer-encoding" in names:
            await self.app(scope, receive, send)
            return

        chunks: list[bytes] = []
        total = 0
        while True:
            message = await receive()
            if message["type"] != "http.request":
                # Client disconnected before finishing the body: hand it on as-is.
                async def _disconnected(msg: Any = message) -> Any:
                    return msg

                await self.app(scope, _disconnected, send)
                return
            body = message.get("body", b"")
            total += len(body)
            if total > self.max_bytes:
                await _send_json(
                    send,
                    413,
                    {
                        "error_code": "BODY_TOO_LARGE",
                        "error_message": f"Request body exceeds {self.max_bytes} bytes.",
                    },
                )
                return
            chunks.append(body)
            if not message.get("more_body", False):
                break

        buffered = b"".join(chunks)
        replayed = False

        async def _replay() -> Any:
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": buffered, "more_body": False}
            return await receive()

        await self.app(scope, _replay, send)


async def _send_json(send: Any, status: int, payload: dict[str, Any]) -> None:
    import json

    raw = json.dumps(payload).encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(raw)).encode("ascii")),
            ],
        }
    )
    await send({"type": "http.response.body", "body": raw})
