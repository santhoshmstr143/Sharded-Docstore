"""
Shared pytest fixtures.

Populated progressively as modules are implemented.
Keep fixtures scoped as tightly as possible (function > module > session).
"""

import os

# Tests must not depend on a developer's git-ignored .env file.
os.environ.setdefault("CLUSTER_SECRET", "test-cluster-secret-32-chars-long-ok!")
os.environ.setdefault("ADMIN_TOKEN", "test-admin-token-32-chars-long-okay!")
os.environ.setdefault("CLIENT_API_KEY", "test-client-api-key-ok")


# ── Test-only request adapter ────────────────────────────────────────────────
#
# Cluster requests are signed over method + path + query + body
# (docstore.common.auth.sign_request). Many older tests build *body-only* signatures
# through small helpers, and call shard data routes directly without the client API
# key (those routes used to be unauthenticated). Rather than rewrite ~150 call sites,
# this adapter upgrades such requests at the in-process transport:
#
#   * a body-only signature that is VALID for the target app's own cluster secret is
#     replaced by the equivalent route-bound signature (a wrong/forged signature is
#     left untouched, so negative auth tests still fail as they should);
#   * a request to a shard's /doc, /query or /status that carries no X-Api-Key gets
#     the shard's configured key;
#   * a direct write to a shard's /doc without an ``ownership_epoch`` gets the epoch the
#     shard currently holds for that doc_id — exactly what the router attaches (cluster
#     mode rejects writes without one).
#
# Production code never emits body-only signatures (enforced by
# tests/integration/test_bugs.py::test_sec_no_body_only_signing_in_src), so this cannot
# mask a production signing bug. Tests marked ``raw_cluster_auth`` bypass the adapter
# and exercise the real wire behaviour (rejection of body-only / cross-route
# signatures, missing API keys, router key forwarding).

import httpx
import pytest

from docstore.common.auth import (
    CLIENT_KEY_HEADER,
    CLUSTER_SIG_HEADER,
    sign_request,
    verify_signature,
)

_SHARD_DATA_PREFIXES = ("/doc", "/query", "/status")
_original_asgi_handle = httpx.ASGITransport.handle_async_request


def _target_settings(app: object) -> tuple[object | None, bool]:
    state = getattr(app, "state", None)
    if state is None:
        return None, False
    node = getattr(state, "node", None)
    settings = getattr(state, "settings", None) or getattr(node, "settings", None)
    return settings, node is not None


async def _adapted_handle(self: httpx.ASGITransport, request: httpx.Request) -> httpx.Response:
    settings, is_shard = _target_settings(self.app)
    if settings is not None:
        body = await request.aread()
        if (
            is_shard
            and request.method in ("POST", "PUT", "DELETE")
            and request.url.path.startswith("/doc")
        ):
            request, body = _with_ownership_epoch(self.app, request, body)
        secret = settings.CLUSTER_SECRET  # type: ignore[attr-defined]
        sig = request.headers.get(CLUSTER_SIG_HEADER)
        if sig and verify_signature(secret, body, sig):
            request.headers[CLUSTER_SIG_HEADER] = sign_request(
                secret, request.method, str(request.url), body
            )
        if (
            is_shard
            and request.url.path.startswith(_SHARD_DATA_PREFIXES)
            and CLIENT_KEY_HEADER not in request.headers
        ):
            request.headers[CLIENT_KEY_HEADER] = settings.CLIENT_API_KEY  # type: ignore[attr-defined]
    return await _original_asgi_handle(self, request)


def _with_ownership_epoch(
    app: object, request: httpx.Request, body: bytes
) -> tuple[httpx.Request, bytes]:
    import json

    try:
        payload = json.loads(body) if body else None
    except ValueError:
        return request, body
    if not isinstance(payload, dict) or "ownership_epoch" in payload:
        return request, body
    tail = request.url.path.rsplit("/", 1)[-1]
    if tail.isdigit():
        doc_id: object = int(tail)
    else:
        doc = payload.get("document")
        doc_id = doc.get("doc_id") if isinstance(doc, dict) else None
    if not isinstance(doc_id, int) or isinstance(doc_id, bool):
        return request, body
    payload["ownership_epoch"] = app.state.node.epoch_for(doc_id)  # type: ignore[attr-defined]
    new_body = json.dumps(payload).encode("utf-8")
    headers = [(k, v) for k, v in request.headers.items() if k.lower() != "content-length"]
    return httpx.Request(request.method, request.url, headers=headers, content=new_body), new_body


@pytest.fixture(autouse=True)
def _legacy_test_request_adapter(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch):
    if request.node.get_closest_marker("raw_cluster_auth") is None:
        monkeypatch.setattr(httpx.ASGITransport, "handle_async_request", _adapted_handle)
    yield
