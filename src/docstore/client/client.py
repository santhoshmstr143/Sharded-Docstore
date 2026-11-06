"""Python client library for Sharded-Docstore.

Provides synchronous and asynchronous clients wrapping the router and coordinator APIs.
Features automatic request_id generation, idempotent retries with bounded exponential
backoff (reusing the same request_id), and typed exceptions.
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Any

import httpx

from docstore.common.models import QueryFilter, ReadMode

_DEFAULT: Any = object()  # sentinel: "use the client's configured timeout"


class DocStoreClientError(Exception):
    """Base exception for client errors."""

    def __init__(self, message: str, status_code: int | None = None, error_code: str | None = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.error_code = error_code
        # Set for writes: retry with this id to resolve OUTCOME_UNKNOWN idempotently.
        self.request_id: str | None = None


class NotFoundError(DocStoreClientError):
    """Document not found."""


class DuplicateKeyError(DocStoreClientError):
    """Document with this doc_id already exists."""


class AuthenticationError(DocStoreClientError):
    """Invalid client API key or admin token."""


class OutOfRangeError(DocStoreClientError):
    """Document ID falls outside cluster keyspace."""


class InvalidQueryError(DocStoreClientError):
    """Query filter violates grammar or constraints."""


class ReplicationUnavailableError(DocStoreClientError):
    """Replication is degraded or unavailable on the target shard."""


class OutcomeUnknownError(DocStoreClientError):
    """Operation outcome unknown (replicated append did not confirm in time)."""


def _map_error(
    status_code: int, data: dict[str, Any], request_id: str | None = None
) -> DocStoreClientError:
    """Map HTTP error status and response payload to typed DocStoreClientError.

    ``request_id`` (for writes) is attached so an OUTCOME_UNKNOWN write can be retried
    idempotently with the same id.
    """
    err = _map_error_type(status_code, data)
    err.request_id = request_id
    return err


def _map_error_type(status_code: int, data: dict[str, Any]) -> DocStoreClientError:
    code = data.get("error_code") or (
        data.get("detail", {}).get("error_code") if isinstance(data.get("detail"), dict) else None
    )
    msg = data.get("error_message") or (
        data.get("detail", {}).get("error_message")
        if isinstance(data.get("detail"), dict)
        else str(data.get("detail", data))
    )

    if status_code == 404 or code == "NOT_FOUND":
        return NotFoundError(msg, status_code, code)
    if status_code == 409 and code == "DUPLICATE_KEY":
        return DuplicateKeyError(msg, status_code, code)
    if status_code == 401 or code == "AUTH_FAILED":
        return AuthenticationError(msg, status_code, code)
    if status_code == 400 and code == "OUT_OF_RANGE":
        return OutOfRangeError(msg, status_code, code)
    if status_code == 400 and code == "INVALID_QUERY":
        return InvalidQueryError(msg, status_code, code)
    if status_code == 503 and code == "REPLICATION_UNAVAILABLE":
        return ReplicationUnavailableError(msg, status_code, code)
    if status_code == 202 or code == "OUTCOME_UNKNOWN":
        return OutcomeUnknownError(msg, status_code, code)

    return DocStoreClientError(msg, status_code, code)


class DocStoreClient:
    """Synchronous client for interacting with the Sharded-Docstore cluster.

    Wraps the stateless Router for CRUD and queries, and the Coordinator
    for admin operations (shard map, cluster status, live range migration).
    """

    def __init__(
        self,
        router_url: str | None = None,
        coordinator_url: str | None = None,
        api_key: str | None = None,
        admin_token: str | None = None,
        *,
        timeout_s: float = 10.0,
        max_retries: int = 5,
        backoff_base_s: float = 0.1,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.router_url = (
            router_url
            or os.environ.get("ROUTER_URL")
            or os.environ.get("DOCSTORE_ROUTER_URL")
            or "http://127.0.0.1:8000"
        ).rstrip("/")
        self.coordinator_url = (
            coordinator_url
            or os.environ.get("COORDINATOR_URL")
            or os.environ.get("DOCSTORE_COORDINATOR_URL")
            or "http://127.0.0.1:8100"
        ).rstrip("/")
        self.api_key = api_key or os.environ.get("CLIENT_API_KEY", "")
        self.admin_token = admin_token or os.environ.get("ADMIN_TOKEN", "")
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.backoff_base_s = backoff_base_s

        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(timeout=timeout_s)

    def close(self) -> None:
        """Close underlying HTTP client if owned."""
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> DocStoreClient:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _client_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-Api-Key"] = self.api_key
        return headers

    def _admin_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.admin_token:
            headers["X-Admin-Token"] = self.admin_token
        return headers

    def _request_with_retry(
        self,
        method: str,
        url: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        retry: bool = True,
        timeout: Any = _DEFAULT,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        req_headers = headers or self._client_headers()
        backoff = self.backoff_base_s

        attempts = self.max_retries + 1 if retry else 1
        extra: dict[str, Any] = {} if timeout is _DEFAULT else {"timeout": timeout}
        for attempt in range(attempts):
            try:
                resp = self._client.request(
                    method=method,
                    url=url,
                    json=json_body,
                    params=params,
                    headers=req_headers,
                    **extra,
                )
                if resp.status_code in (200, 201):
                    return resp.json()  # type: ignore[no-any-return]

                # Check for retryable responses
                try:
                    data = resp.json()
                except Exception:
                    data = {}
                if not isinstance(data, dict):  # e.g. a proxy's list / string error body
                    data = {"error_message": str(data)}

                code = data.get("error_code")
                is_retryable = resp.status_code in (202, 503) or code in (
                    "REPLICATION_UNAVAILABLE",
                    "RANGE_PAUSED",
                    "SERVER_BUSY",
                    "OUTCOME_UNKNOWN",
                )

                if is_retryable and attempt < attempts - 1:
                    time.sleep(backoff)
                    backoff = min(backoff * 2.0, 1.5)
                    continue

                raise _map_error(resp.status_code, data, request_id)

            except httpx.TransportError as exc:
                # Any dropped / failed connection (connect, timeout, read/write errors,
                # protocol errors). Writes are safe to retry: they reuse their request_id.
                if attempt < attempts - 1:
                    time.sleep(backoff)
                    backoff = min(backoff * 2.0, 1.5)
                    continue
                err = DocStoreClientError(f"Network error after {attempt} retries: {exc}")
                err.request_id = request_id
                raise err from exc

        raise DocStoreClientError("Exceeded maximum retries")

    # ── CRUD Operations ────────────────────────────────────────────────────────

    def insert(self, doc: dict[str, Any], request_id: str | None = None) -> dict[str, Any]:
        """Insert a new document via the router.

        Generates a unique request_id if not provided, and reuses it on retry.
        """
        rid = request_id or str(uuid.uuid4())
        payload = {"request_id": rid, "document": doc}
        return self._request_with_retry(
            "POST",
            f"{self.router_url}/doc",
            json_body=payload,
            request_id=rid,
        )

    def get(self, doc_id: int, read_mode: str | ReadMode = ReadMode.PRIMARY) -> dict[str, Any]:
        """Retrieve a document by ID.

        read_mode can be 'primary' or 'follower'.
        """
        mode_val = read_mode.value if isinstance(read_mode, ReadMode) else str(read_mode)
        return self._request_with_retry(
            "GET",
            f"{self.router_url}/doc/{doc_id}",
            params={"read_mode": mode_val},
        )

    def update(
        self,
        doc_id: int,
        doc: dict[str, Any],
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Replace a document by ID.

        Generates a unique request_id if not provided, and reuses it on retry.
        """
        rid = request_id or str(uuid.uuid4())
        payload = {"request_id": rid, "document": doc}
        return self._request_with_retry(
            "PUT",
            f"{self.router_url}/doc/{doc_id}",
            json_body=payload,
            request_id=rid,
        )

    def delete(self, doc_id: int, request_id: str | None = None) -> dict[str, Any]:
        """Delete a document by ID.

        Generates a unique request_id if not provided, and reuses it on retry.
        """
        rid = request_id or str(uuid.uuid4())
        payload = {"request_id": rid}
        return self._request_with_retry(
            "DELETE",
            f"{self.router_url}/doc/{doc_id}",
            json_body=payload,
            request_id=rid,
        )

    def query(
        self,
        filter: dict[str, Any] | None = None,
        *,
        sort_field: str | None = None,
        sort_dir: str = "asc",
        skip: int = 0,
        limit: int = 100,
        read_mode: str | ReadMode = ReadMode.PRIMARY,
    ) -> dict[str, Any]:
        """Execute a Mongo-style filter query across cluster shards."""
        mode_val = ReadMode(read_mode) if isinstance(read_mode, str) else read_mode
        q = QueryFilter(
            filter=filter or {},
            sort_field=sort_field,
            sort_dir=sort_dir,
            skip=skip,
            limit=limit,
            read_mode=mode_val,
        )
        return self._request_with_retry(
            "POST",
            f"{self.router_url}/query",
            json_body=q.model_dump(),
        )

    # ── Admin & Cluster Operations ─────────────────────────────────────────────

    def shard_map(self) -> dict[str, Any]:
        """Fetch current cluster topology and map_version."""
        return self._request_with_retry(
            "GET", f"{self.coordinator_url}/shardmap", headers=self._admin_headers()
        )

    def status(self) -> dict[str, Any]:
        """Fetch cluster administrative health, leases, and replica status."""
        return self._request_with_retry(
            "GET",
            f"{self.coordinator_url}/admin/status",
            headers=self._admin_headers(),
        )

    def migrate(
        self,
        from_shard: str,
        to_shard: str,
        start: int,
        end: int,
        sync: bool = True,
    ) -> dict[str, Any]:
        """Trigger a live key-range migration on the coordinator."""
        payload = {
            "from_shard": from_shard,
            "to_shard": to_shard,
            "range": {"start": start, "end": end},
            "sync": sync,
        }
        # Not idempotent: never retried (a retry would hit "migration already overlaps"
        # while the first one is still running). A synchronous migration can take far
        # longer than the default timeout, so it gets none.
        return self._request_with_retry(
            "POST",
            f"{self.coordinator_url}/admin/migrate",
            json_body=payload,
            headers=self._admin_headers(),
            retry=False,
            timeout=None if sync else _DEFAULT,
        )

    def migration_status(self, migration_id: str) -> dict[str, Any]:
        """Check status of a specific range migration."""
        return self._request_with_retry(
            "GET",
            f"{self.coordinator_url}/admin/migrate/{migration_id}",
            headers=self._admin_headers(),
        )

    def bulk_load(self, docs: list[dict[str, Any]]) -> dict[str, Any]:
        """Bulk load a list of documents sequentially using unique request_ids."""
        succeeded = 0
        failed = 0
        errors: list[dict[str, Any]] = []

        for d in docs:
            did = d.get("doc_id")
            try:
                self.insert(d)
                succeeded += 1
            except Exception as exc:
                failed += 1
                errors.append({"doc_id": did, "error": str(exc)})

        return {
            "total": len(docs),
            "succeeded": succeeded,
            "failed": failed,
            "errors": errors,
        }


class AsyncDocStoreClient:
    """Asynchronous client for interacting with the Sharded-Docstore cluster."""

    def __init__(
        self,
        router_url: str | None = None,
        coordinator_url: str | None = None,
        api_key: str | None = None,
        admin_token: str | None = None,
        *,
        timeout_s: float = 10.0,
        max_retries: int = 5,
        backoff_base_s: float = 0.1,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.router_url = (
            router_url
            or os.environ.get("ROUTER_URL")
            or os.environ.get("DOCSTORE_ROUTER_URL")
            or "http://127.0.0.1:8000"
        ).rstrip("/")
        self.coordinator_url = (
            coordinator_url
            or os.environ.get("COORDINATOR_URL")
            or os.environ.get("DOCSTORE_COORDINATOR_URL")
            or "http://127.0.0.1:8100"
        ).rstrip("/")
        self.api_key = api_key or os.environ.get("CLIENT_API_KEY", "")
        self.admin_token = admin_token or os.environ.get("ADMIN_TOKEN", "")
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.backoff_base_s = backoff_base_s

        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(timeout=timeout_s)

    async def aclose(self) -> None:
        """Close underlying async HTTP client if owned."""
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> AsyncDocStoreClient:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.aclose()

    def _client_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-Api-Key"] = self.api_key
        return headers

    def _admin_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.admin_token:
            headers["X-Admin-Token"] = self.admin_token
        return headers

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        retry: bool = True,
        timeout: Any = _DEFAULT,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        import asyncio

        req_headers = headers or self._client_headers()
        backoff = self.backoff_base_s

        attempts = self.max_retries + 1 if retry else 1
        extra: dict[str, Any] = {} if timeout is _DEFAULT else {"timeout": timeout}
        for attempt in range(attempts):
            try:
                resp = await self._client.request(
                    method=method,
                    url=url,
                    json=json_body,
                    params=params,
                    headers=req_headers,
                    **extra,
                )
                if resp.status_code in (200, 201):
                    return resp.json()  # type: ignore[no-any-return]

                try:
                    data = resp.json()
                except Exception:
                    data = {}
                if not isinstance(data, dict):  # e.g. a proxy's list / string error body
                    data = {"error_message": str(data)}

                code = data.get("error_code")
                is_retryable = resp.status_code in (202, 503) or code in (
                    "REPLICATION_UNAVAILABLE",
                    "RANGE_PAUSED",
                    "SERVER_BUSY",
                    "OUTCOME_UNKNOWN",
                )

                if is_retryable and attempt < attempts - 1:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2.0, 1.5)
                    continue

                raise _map_error(resp.status_code, data, request_id)

            except httpx.TransportError as exc:
                # Any dropped / failed connection (connect, timeout, read/write errors,
                # protocol errors). Writes are safe to retry: they reuse their request_id.
                if attempt < attempts - 1:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2.0, 1.5)
                    continue
                err = DocStoreClientError(f"Network error after {attempt} retries: {exc}")
                err.request_id = request_id
                raise err from exc

        raise DocStoreClientError("Exceeded maximum retries")

    async def insert(self, doc: dict[str, Any], request_id: str | None = None) -> dict[str, Any]:
        rid = request_id or str(uuid.uuid4())
        payload = {"request_id": rid, "document": doc}
        return await self._request_with_retry(
            "POST", f"{self.router_url}/doc", json_body=payload, request_id=rid
        )

    async def get(
        self,
        doc_id: int,
        read_mode: str | ReadMode = ReadMode.PRIMARY,
    ) -> dict[str, Any]:
        mode_val = read_mode.value if isinstance(read_mode, ReadMode) else str(read_mode)
        return await self._request_with_retry(
            "GET",
            f"{self.router_url}/doc/{doc_id}",
            params={"read_mode": mode_val},
        )

    async def update(
        self,
        doc_id: int,
        doc: dict[str, Any],
        request_id: str | None = None,
    ) -> dict[str, Any]:
        rid = request_id or str(uuid.uuid4())
        payload = {"request_id": rid, "document": doc}
        return await self._request_with_retry(
            "PUT",
            f"{self.router_url}/doc/{doc_id}",
            json_body=payload,
            request_id=rid,
        )

    async def delete(self, doc_id: int, request_id: str | None = None) -> dict[str, Any]:
        rid = request_id or str(uuid.uuid4())
        payload = {"request_id": rid}
        return await self._request_with_retry(
            "DELETE",
            f"{self.router_url}/doc/{doc_id}",
            json_body=payload,
            request_id=rid,
        )

    async def query(
        self,
        filter: dict[str, Any] | None = None,
        *,
        sort_field: str | None = None,
        sort_dir: str = "asc",
        skip: int = 0,
        limit: int = 100,
        read_mode: str | ReadMode = ReadMode.PRIMARY,
    ) -> dict[str, Any]:
        mode_val = ReadMode(read_mode) if isinstance(read_mode, str) else read_mode
        q = QueryFilter(
            filter=filter or {},
            sort_field=sort_field,
            sort_dir=sort_dir,
            skip=skip,
            limit=limit,
            read_mode=mode_val,
        )
        return await self._request_with_retry(
            "POST",
            f"{self.router_url}/query",
            json_body=q.model_dump(),
        )

    async def shard_map(self) -> dict[str, Any]:
        return await self._request_with_retry(
            "GET", f"{self.coordinator_url}/shardmap", headers=self._admin_headers()
        )

    async def status(self) -> dict[str, Any]:
        return await self._request_with_retry(
            "GET",
            f"{self.coordinator_url}/admin/status",
            headers=self._admin_headers(),
        )

    async def migrate(
        self,
        from_shard: str,
        to_shard: str,
        start: int,
        end: int,
        sync: bool = True,
    ) -> dict[str, Any]:
        payload = {
            "from_shard": from_shard,
            "to_shard": to_shard,
            "range": {"start": start, "end": end},
            "sync": sync,
        }
        return await self._request_with_retry(
            "POST",
            f"{self.coordinator_url}/admin/migrate",
            json_body=payload,
            headers=self._admin_headers(),
            retry=False,
            timeout=None if sync else _DEFAULT,
        )
