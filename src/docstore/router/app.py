"""
Stateless Router FastAPI application — Module 4.2.

Key responsibilities:
- Client authentication via X-Api-Key (constant-time comparison).
- Cached ShardMap lookup: routes client requests to the owning primary shard node.
- Preserves client request_id for end-to-end idempotency.
- Bounded retry (max 1) on stale map (OutOfRange / StaleEpoch / NotPrimary).
- Concurrency limiter (DoS protection via semaphore).
- Internal shard endpoints (/internal/*) are NOT exposed.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from docstore.common.auth import (
    CLIENT_KEY_HEADER,
    CLUSTER_SIG_HEADER,
    sign_request,
    verify_token,
)
from docstore.common.config import Settings
from docstore.common.errors import DocTooLarge, InvalidQuery, OutOfRange
from docstore.common.http import BodyLimitMiddleware
from docstore.common.logging import get_logger
from docstore.common.models import (
    DOC_ID_MAX,
    DOC_ID_MIN,
    Document,
    QueryFilter,
    QueryResponse,
    ShardMapEntry,
    safe_validation_message,
    serialized_doc_size,
)
from docstore.query import validate_query_filter
from docstore.router.scatter import execute_scatter_query
from docstore.router.shard_map import ShardMapManager

logger = get_logger(__name__)


# ── Request / Response models ─────────────────────────────────────────────────


class _ClientFencing(BaseModel):
    # Optional: a client that tracks the shard term may pin it; the primary rejects
    # the write if its term differs. ownership_epoch is always set by the router.
    term: int | None = Field(default=None, ge=0)


class RouterInsertBody(_ClientFencing):
    request_id: str = Field(min_length=1, max_length=128)
    document: dict[str, Any] = Field(description="Document containing doc_id.")


class RouterUpdateBody(_ClientFencing):
    request_id: str = Field(min_length=1, max_length=128)
    document: dict[str, Any] = Field(description="Full replacement document.")


class RouterDeleteBody(_ClientFencing):
    request_id: str = Field(min_length=1, max_length=128)


_WRITE_METHODS = ("POST", "PUT", "DELETE")


def _forward_write_body(body: dict[str, Any], term: int | None) -> dict[str, Any]:
    if term is not None:
        body["term"] = term
    return body


# ── App factory ───────────────────────────────────────────────────────────────


def build_router_app(
    settings: Settings,
    map_mgr: ShardMapManager,
    *,
    shard_transports: dict[str, httpx.AsyncBaseTransport] | None = None,
    http_client: httpx.AsyncClient | None = None,
    max_concurrent_requests: int | None = None,
) -> FastAPI:
    """Create and return the stateless FastAPI router application.

    Parameters
    ----------
    settings : Settings
        Cluster settings including secrets and timeouts.
    map_mgr : ShardMapManager
        Shard map manager holding the routing table.
    shard_transports : dict[str, httpx.BaseTransport], optional
        Mapping of 'host:port' -> ASGITransport for in-memory integration testing.
    http_client : httpx.AsyncClient, optional
        Pre-configured HTTP client (overrides shard_transports if provided).
    max_concurrent_requests : int, optional
        Max concurrent in-flight requests (defaults to settings.MAX_CONCURRENT_REQUESTS).
    """
    concurrency_limit = max_concurrent_requests or settings.MAX_CONCURRENT_REQUESTS
    semaphore = asyncio.Semaphore(concurrency_limit)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        app.state.map_mgr = map_mgr
        app.state.settings = settings
        app.state.semaphore = semaphore

        # Set up HTTP client for forwarding
        if http_client is not None:
            client = http_client
            owns_client = False
        elif shard_transports:
            # Mount transports by host:port
            mounts: dict[str, httpx.AsyncBaseTransport | None] = {}
            for target, transport in shard_transports.items():
                target_clean = target.removeprefix("http://").removeprefix("https://")
                mounts[f"http://{target_clean}"] = transport
            client = httpx.AsyncClient(mounts=mounts, timeout=settings.REPLICATION_TIMEOUT_S + 2.0)
            owns_client = True
        else:
            client = httpx.AsyncClient(timeout=settings.REPLICATION_TIMEOUT_S + 2.0)
            owns_client = True

        app.state.http_client = client
        logger.info("Router started with map version %d", map_mgr.shard_map.map_version)
        yield
        if owns_client:
            await client.aclose()
        logger.info("Router shut down")

    app = FastAPI(title="DocStore Router", version="0.1.0", lifespan=lifespan)
    app.add_middleware(BodyLimitMiddleware, max_bytes=settings.MAX_BODY_SIZE_BYTES)

    # ── Concurrency & Body Size Middleware ─────────────────────────────────────

    @app.middleware("http")
    async def _router_middleware(request: Request, call_next: Any) -> Response:
        cl = request.headers.get("content-length")
        if "transfer-encoding" in request.headers:
            return JSONResponse(
                status_code=411,
                content={
                    "error_code": "LENGTH_REQUIRED",
                    "error_message": "Chunked bodies not accepted; send Content-Length.",
                },
            )
        try:
            too_big = bool(cl) and int(cl or 0) > settings.MAX_BODY_SIZE_BYTES
        except ValueError:
            too_big = True
        if too_big:
            return JSONResponse(
                status_code=413,
                content={
                    "error_code": "BODY_TOO_LARGE",
                    "error_message": "Request body exceeds 2 MB.",
                },
            )

        sem: asyncio.Semaphore = request.app.state.semaphore
        try:
            # Try to acquire semaphore without long stall
            await asyncio.wait_for(sem.acquire(), timeout=5.0)
        except TimeoutError:
            return JSONResponse(
                status_code=503,
                content={
                    "error_code": "SERVER_BUSY",
                    "error_message": "Too many concurrent requests.",
                },
            )

        try:
            response: Response = await call_next(request)
            return response
        finally:
            sem.release()

    # ── Client Auth Dependency ────────────────────────────────────────────────

    def _verify_client_auth(
        x_api_key: str | None = Header(default=None, alias=CLIENT_KEY_HEADER),
    ) -> None:  # used as a FastAPI dependency on every client-facing route
        if not x_api_key or not verify_token(settings.CLIENT_API_KEY, x_api_key):
            raise HTTPException(
                status_code=401,
                detail={
                    "error_code": "AUTH_FAILED",
                    "error_message": "Invalid or missing API key.",
                },
            )

    # ── Forwarding helper with bounded retry ──────────────────────────────────

    async def _forward_to_host(
        req: Request,
        target_host: str,
        method: str,
        path: str,
        body_dict: dict[str, Any] | None,
    ) -> httpx.Response:
        client: httpx.AsyncClient = req.app.state.http_client
        cluster_secret: str = req.app.state.settings.CLUSTER_SECRET
        clean_target = target_host.removeprefix("http://").removeprefix("https://")
        url = f"http://{clean_target}{path}"

        if body_dict is not None:
            body_bytes = json.dumps(body_dict, separators=(",", ":")).encode("utf-8")
            sig = sign_request(cluster_secret, method, url, body_bytes)
            headers = {"Content-Type": "application/json", CLUSTER_SIG_HEADER: sig}
            content = body_bytes
        else:
            sig = sign_request(cluster_secret, method, url)
            headers = {CLUSTER_SIG_HEADER: sig}
            content = b""
        # Shard data routes authenticate the client API key (BUG: they used to be open).
        headers[CLIENT_KEY_HEADER] = req.app.state.settings.CLIENT_API_KEY

        return await client.request(method=method, url=url, content=content, headers=headers)

    def _check_doc_size(doc: dict[str, Any]) -> None:
        size = serialized_doc_size(doc)
        if size > settings.MAX_DOC_SIZE_BYTES:
            raise DocTooLarge(
                f"Document is {size} bytes; limit is {settings.MAX_DOC_SIZE_BYTES} bytes."
            ).to_http_exception()

    async def _forward_request(
        req: Request,
        doc_id: int,
        method: str,
        path: str,
        body_dict: dict[str, Any] | None,
        target_role: str = "primary",
    ) -> JSONResponse:
        """Route request to owning shard, refreshing map once if node reports stale map.

        Writes carry the ownership_epoch of the map entry they were routed by, so the
        primary can reject a write routed on stale ownership. The request_id is reused
        unchanged on every retry, so a retried write is deduplicated by the shard.
        """
        mgr: ShardMapManager = req.app.state.map_mgr
        base_body = body_dict

        pause_retries = 0
        max_pause_retries = 8

        # Attempt up to 2 times (initial + 1 retry on stale map or replication unavailable)
        attempt = 0
        while attempt < 2:
            try:
                entry: ShardMapEntry = mgr.get_shard_for_doc_id(doc_id)
            except OutOfRange as exc:
                if attempt == 0:
                    try:
                        mgr.reload()
                        attempt += 1
                        continue
                    except Exception as reload_exc:
                        logger.debug("Map reload failed: %s", reload_exc)
                raise exc.to_http_exception() from exc

            if base_body is not None and method in _WRITE_METHODS:
                body_dict = {**base_body, "ownership_epoch": entry.ownership_epoch}

            # Read from follower if requested and available
            if target_role == "follower" and method in ("GET", "HEAD") and entry.follower:
                clean_follower = entry.follower.removeprefix("http://").removeprefix("https://")
                try:
                    resp = await _forward_to_host(req, clean_follower, method, path, body_dict)
                    if resp.status_code < 500:
                        try:
                            resp_json = resp.json()
                        except Exception:
                            resp_json = {"detail": resp.text}
                        return JSONResponse(status_code=resp.status_code, content=resp_json)
                except (httpx.ConnectError, httpx.TimeoutException) as exc:
                    logger.debug("Follower read failed, falling back to primary: %s", exc)

            primary = entry.primary.removeprefix("http://").removeprefix("https://")

            try:
                resp = await _forward_to_host(req, primary, method, path, body_dict)
            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                if attempt == 0:
                    try:
                        mgr.reload()
                        attempt += 1
                        continue
                    except Exception as reload_exc:
                        logger.debug("Map reload failed: %s", reload_exc)
                logger.warning("Shard %s (%s) unavailable: %s", entry.shard_id, primary, exc)
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error_code": "SHARD_UNAVAILABLE",
                        # Internal addresses / exception text stay in the log only.
                        "error_message": f"Shard {entry.shard_id} is unavailable.",
                    },
                ) from exc

            # Check if shard reports stale routing (OutOfRange/StaleEpoch/NotPrimary)
            # or replication unavailable or range paused
            if resp.status_code in (400, 409, 503):
                try:
                    data = resp.json()
                    err_code = data.get("detail", {}).get("error_code") or data.get("error_code")
                    if err_code == "RANGE_PAUSED" and pause_retries < max_pause_retries:
                        pause_retries += 1
                        delay = min(0.05 * (1.5**pause_retries), 0.3)
                        logger.info(
                            "Range paused for doc_id=%d; retrying (attempt %d/%d, %.2fs)",
                            doc_id,
                            pause_retries,
                            max_pause_retries,
                            delay,
                        )
                        await asyncio.sleep(delay)
                        try:
                            mgr.reload()
                        except Exception as reload_exc:
                            logger.debug("Map reload failed: %s", reload_exc)
                        continue
                    if attempt == 0 and err_code in (
                        "OUT_OF_RANGE",
                        "STALE_EPOCH",
                        "NOT_PRIMARY",
                        "LEASE_EXPIRED",
                    ):
                        if err_code == "LEASE_EXPIRED":
                            # The primary lost (or has not yet renewed) its lease: give the
                            # coordinator a moment to renew it or fail over, then re-route.
                            await asyncio.sleep(0.1)
                        logger.warning(
                            "Shard %s returned %s on doc_id=%d; reloading map and retrying once",
                            entry.shard_id,
                            err_code,
                            doc_id,
                        )
                        mgr.reload()
                        attempt += 1
                        continue
                    if attempt == 0 and err_code == "REPLICATION_UNAVAILABLE":
                        logger.warning(
                            "Shard %s returned REPLICATION_UNAVAILABLE; retrying with backoff",
                            entry.shard_id,
                        )
                        await asyncio.sleep(0.05)
                        attempt += 1
                        continue
                except Exception as parse_exc:
                    logger.debug("Failed parsing error body from shard: %s", parse_exc)

            # Return response from shard directly
            try:
                resp_json = resp.json()
            except Exception:
                resp_json = {"detail": resp.text}

            return JSONResponse(status_code=resp.status_code, content=resp_json)

        raise HTTPException(
            status_code=500,
            detail={
                "error_code": "ROUTING_FAILED",
                "error_message": "Exceeded routing retry attempts.",
            },
        )

    # ── Public Router API ─────────────────────────────────────────────────────

    @app.post("/doc", status_code=201, dependencies=[Depends(_verify_client_auth)])
    async def insert_doc(
        body: RouterInsertBody,
        request: Request,
    ) -> JSONResponse:
        try:
            doc = Document.model_validate(body.document)
        except Exception as exc:
            raise HTTPException(
                status_code=422,
                detail={
                    "error_code": "VALIDATION_ERROR",
                    "error_message": safe_validation_message(exc),
                },
            ) from exc

        doc_dict = doc.model_dump()
        doc_id = doc_dict["doc_id"]
        _check_doc_size(doc_dict)
        forward_body = _forward_write_body(
            {"request_id": body.request_id, "document": doc_dict}, body.term
        )

        return await _forward_request(
            request,
            doc_id=doc_id,
            method="POST",
            path="/doc",
            body_dict=forward_body,
        )

    @app.put("/doc/{doc_id}", dependencies=[Depends(_verify_client_auth)])
    async def update_doc(
        doc_id: int,
        body: RouterUpdateBody,
        request: Request,
    ) -> JSONResponse:
        if isinstance(doc_id, bool) or doc_id < DOC_ID_MIN or doc_id > DOC_ID_MAX:
            raise OutOfRange(f"doc_id={doc_id} out of range").to_http_exception()

        try:
            doc = Document.model_validate(body.document)
        except Exception as exc:
            raise HTTPException(
                status_code=422,
                detail={
                    "error_code": "VALIDATION_ERROR",
                    "error_message": safe_validation_message(exc),
                },
            ) from exc

        if doc.doc_id != doc_id:
            raise HTTPException(
                status_code=400,
                detail={
                    "error_code": "VALIDATION_ERROR",
                    "error_message": "doc_id in path and body must match.",
                },
            )

        doc_dict = doc.model_dump()
        _check_doc_size(doc_dict)
        forward_body = _forward_write_body(
            {"request_id": body.request_id, "document": doc_dict}, body.term
        )

        return await _forward_request(
            request,
            doc_id=doc_id,
            method="PUT",
            path=f"/doc/{doc_id}",
            body_dict=forward_body,
        )

    @app.delete("/doc/{doc_id}", dependencies=[Depends(_verify_client_auth)])
    async def delete_doc(
        doc_id: int,
        body: RouterDeleteBody,
        request: Request,
    ) -> JSONResponse:
        if isinstance(doc_id, bool) or doc_id < DOC_ID_MIN or doc_id > DOC_ID_MAX:
            raise OutOfRange(f"doc_id={doc_id} out of range").to_http_exception()

        forward_body = _forward_write_body({"request_id": body.request_id}, body.term)
        return await _forward_request(
            request,
            doc_id=doc_id,
            method="DELETE",
            path=f"/doc/{doc_id}",
            body_dict=forward_body,
        )

    @app.get("/doc/{doc_id}", dependencies=[Depends(_verify_client_auth)])
    async def get_doc(
        doc_id: int,
        request: Request,
        read_mode: str = Query(default="primary"),
    ) -> JSONResponse:
        if isinstance(doc_id, bool) or doc_id < DOC_ID_MIN or doc_id > DOC_ID_MAX:
            raise OutOfRange(f"doc_id={doc_id} out of range").to_http_exception()

        return await _forward_request(
            request,
            doc_id=doc_id,
            method="GET",
            path=f"/doc/{doc_id}",
            body_dict=None,
            target_role=read_mode,
        )

    @app.post("/query", dependencies=[Depends(_verify_client_auth)])
    async def query_docs(
        body: QueryFilter,
        request: Request,
    ) -> QueryResponse:
        try:
            validate_query_filter(body.filter)
        except InvalidQuery as exc:
            raise exc.to_http_exception() from exc

        return await execute_scatter_query(
            query=body,
            map_mgr=request.app.state.map_mgr,
            http_client=request.app.state.http_client,
            settings=request.app.state.settings,
        )

    @app.get("/health")
    async def health(request: Request) -> dict[str, Any]:
        mgr: ShardMapManager = request.app.state.map_mgr
        return {
            "status": "ok",
            "map_version": mgr.shard_map.map_version,
            "shard_count": len(mgr.shard_map.entries),
        }

    @app.get("/status", dependencies=[Depends(_verify_client_auth)])
    async def status(request: Request) -> dict[str, Any]:
        mgr: ShardMapManager = request.app.state.map_mgr
        return {
            "status": "ok",
            "map_version": mgr.shard_map.map_version,
            "shards": [
                {
                    "shard_id": e.shard_id,
                    "range": {"start": e.range.start, "end": e.range.end},
                    "primary": e.primary,
                    "follower": e.follower,
                    "ownership_epoch": e.ownership_epoch,
                }
                for e in mgr.shard_map.entries
            ],
        }

    # ── Error handler ─────────────────────────────────────────────────────────

    @app.exception_handler(RequestValidationError)
    async def _request_validation_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        # FastAPI's default 422 echoes the submitted input back; report only what failed.
        return JSONResponse(
            status_code=422,
            content={
                "error_code": "VALIDATION_ERROR",
                "error_message": safe_validation_message(exc),
            },
        )

    @app.exception_handler(HTTPException)
    async def _http_exc_handler(request: Request, exc: HTTPException) -> JSONResponse:
        if isinstance(exc.detail, dict):
            detail = exc.detail
        else:
            detail = {"error_code": "ERROR", "error_message": str(exc.detail)}
        return JSONResponse(status_code=exc.status_code, content=detail)

    return app
