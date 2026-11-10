"""
FastAPI application for the Coordinator — Module 6.

Public endpoints:
- GET  /shardmap   — fetch current cluster shard map
- POST /register   — node startup registration (cluster auth required)
- POST /heartbeat  — node periodic heartbeats & primary lease renewals (cluster auth required)
- GET  /health     — liveness probe

Admin endpoints (X-Admin-Token required):
- GET  /admin/status   — detailed cluster status (replica states, leases, terms)
- POST /admin/migrate  — trigger range migration (stub for Module 9)
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from docstore.common.auth import make_admin_auth_dependency, make_cluster_auth_dependency
from docstore.common.config import Settings
from docstore.common.errors import DocstoreError
from docstore.common.logging import get_logger
from docstore.common.models import (
    Heartbeat,
    LeaseGrant,
    NodeRegistration,
    RegistrationResponse,
    ShardMap,
    safe_validation_message,
)
from docstore.coordinator.coordinator import CoordinatorService

logger = get_logger(__name__)


def build_coordinator_app(
    settings: Settings,
    coordinator_svc: CoordinatorService | None = None,
) -> FastAPI:
    """Create and return the FastAPI Coordinator application."""
    svc = coordinator_svc or CoordinatorService(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        app.state.coordinator = svc
        app.state.settings = settings
        logger.info(
            "Coordinator started with map version %d",
            svc.shard_map.map_version,
        )

        # Resume or abort unfinished migrations on coordinator startup
        try:
            await svc.resume_unfinished_migrations()
        except Exception as exc:
            logger.warning("Unfinished migration recovery check error: %s", exc)

        # H5 / Module 8: Background health monitor — checks for timed-out nodes and auto-failovers.
        async def _health_monitor() -> None:
            while True:
                await asyncio.sleep(settings.HEARTBEAT_INTERVAL_S)
                try:
                    await svc.check_and_trigger_failover()
                except Exception as exc:
                    logger.warning("Health monitor failover check error: %s", exc)
                try:
                    await svc.push_follower_updates()
                except Exception as exc:
                    logger.warning("Follower membership update error: %s", exc)

        # Separate loop: migration RPCs (10 s timeouts) must never delay failure detection.
        async def _migration_monitor() -> None:
            while True:
                await asyncio.sleep(settings.HEARTBEAT_INTERVAL_S * 5)
                # Drive migrations stuck past cutover (e.g. destination follower was down)
                # or in ABORTING to completion, without waiting for a coordinator restart.
                try:
                    await svc.migration_mgr.retry_post_cutover(svc)
                except Exception as exc:
                    logger.warning("Stalled migration retry error: %s", exc)

        tasks = [
            asyncio.create_task(_health_monitor()),
            asyncio.create_task(_migration_monitor()),
        ]

        yield

        for t in tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

        logger.info("Coordinator shut down")

    app = FastAPI(title="DocStore Coordinator", version="0.1.0", lifespan=lifespan)
    app.state.coordinator = svc
    app.state.settings = settings

    # ── Auth dependencies ─────────────────────────────────────────────────────

    admin_auth = make_admin_auth_dependency(lambda: settings.ADMIN_TOKEN)
    # C4: cluster_auth guards /register and /heartbeat (contracts §9)
    cluster_auth = make_cluster_auth_dependency(lambda: settings.CLUSTER_SECRET)

    def get_service(request: Request) -> CoordinatorService:
        return request.app.state.coordinator  # type: ignore[no-any-return]

    # ── Public Endpoints ──────────────────────────────────────────────────────

    @app.get("/shardmap", dependencies=[Depends(admin_auth)])
    async def get_shard_map(request: Request) -> ShardMap:
        service = get_service(request)
        return service.shard_map

    @app.post("/register", dependencies=[Depends(cluster_auth)])
    async def register(
        body: NodeRegistration,
        request: Request,
    ) -> RegistrationResponse:
        service = get_service(request)
        try:
            return service.register_node(body)
        except DocstoreError as exc:
            raise exc.to_http_exception() from exc

    @app.post("/heartbeat", dependencies=[Depends(cluster_auth)])
    async def heartbeat(
        body: Heartbeat,
        request: Request,
    ) -> LeaseGrant | None:
        service = get_service(request)
        try:
            return service.handle_heartbeat(body)
        except DocstoreError as exc:
            raise exc.to_http_exception() from exc

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    # ── Admin Endpoints ───────────────────────────────────────────────────────

    @app.get("/admin/status", dependencies=[Depends(admin_auth)])
    async def admin_status(request: Request) -> dict[str, Any]:
        service = get_service(request)
        return service.get_cluster_status()

    @app.post("/admin/failover/{shard_id}", dependencies=[Depends(admin_auth)])
    async def admin_failover(
        shard_id: str, request: Request, force: bool = False
    ) -> dict[str, Any]:
        """Manually trigger failover for a shard.

        ``force=true`` promotes the follower even if it is known to be unsynced or behind
        the primary's last reported sequence number (may lose committed writes).
        """
        service = get_service(request)
        promoted = await service.trigger_failover(shard_id, force=force)
        if not promoted:
            raise HTTPException(
                status_code=400,
                detail={
                    "error_code": "FAILOVER_FAILED",
                    "error_message": f"Failover failed or blocked for shard {shard_id}.",
                },
            )
        return {"status": "ok", "shard_id": shard_id, "promoted": True}

    @app.post("/admin/migrate", dependencies=[Depends(admin_auth)])
    async def admin_migrate(
        body: dict[str, Any],
        sync: bool = False,
    ) -> dict[str, Any]:
        """Trigger live range migration from one shard to another."""
        rng = body.get("range")
        if not isinstance(rng, dict):
            # Also accept top-level start/end (the shape documented in the README).
            rng = {"start": body.get("start"), "end": body.get("end")}
        start = rng.get("start")
        end = rng.get("end")
        from_shard = body.get("from_shard")
        to_shard = body.get("to_shard")
        if start is None or end is None or not from_shard or not to_shard:
            raise HTTPException(
                status_code=400,
                detail={
                    "error_code": "VALIDATION_ERROR",
                    "error_message": "Missing required fields: range, from_shard, to_shard",
                },
            )

        try:
            rec = svc.migration_mgr.create_migration(
                from_shard=from_shard,
                to_shard=to_shard,
                start=int(start),
                end=int(end),
                coord=svc,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=409,
                detail={"error_code": "MIGRATION_CONFLICT", "error_message": str(exc)},
            ) from exc

        if sync or body.get("sync"):
            try:
                completed_rec = await svc.migration_mgr.execute_migration(rec.migration_id, svc)
                return completed_rec.to_dict()
            except Exception as exc:
                raise HTTPException(
                    status_code=500,
                    detail={"error_code": "MIGRATION_FAILED", "error_message": str(exc)},
                ) from exc

        async def _run() -> None:
            try:
                await svc.migration_mgr.execute_migration(rec.migration_id, svc)
            except Exception as exc:
                logger.error("Background migration %s failed: %s", rec.migration_id, exc)

        task = asyncio.create_task(_run())
        bg_tasks: set[asyncio.Task[Any]] = getattr(app.state, "_bg_tasks", set())
        bg_tasks.add(task)
        task.add_done_callback(bg_tasks.discard)
        app.state._bg_tasks = bg_tasks

        return {
            "status": "planned",
            "migration_id": rec.migration_id,
            "from_shard": rec.from_shard,
            "to_shard": rec.to_shard,
            "range": {"start": rec.range_start, "end": rec.range_end},
        }

    @app.get("/admin/migrate/{migration_id}", dependencies=[Depends(admin_auth)])
    async def get_migration(migration_id: str) -> dict[str, Any]:
        rec = svc.migration_mgr.migrations.get(migration_id)
        if not rec:
            raise HTTPException(
                status_code=404,
                detail={
                    "error_code": "NOT_FOUND",
                    "error_message": f"Migration {migration_id} not found",
                },
            )
        return rec.to_dict()

    @app.get("/admin/migrations", dependencies=[Depends(admin_auth)])
    async def list_migrations() -> list[dict[str, Any]]:
        return [m.to_dict() for m in svc.migration_mgr.migrations.values()]

    # ── Error handler (no stack traces) ───────────────────────────────────────

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
