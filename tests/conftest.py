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
#     the shard's configured key.
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


@pytest.fixture(autouse=True)
def _legacy_test_request_adapter(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch):
    if request.node.get_closest_marker("raw_cluster_auth") is None:
        monkeypatch.setattr(httpx.ASGITransport, "handle_async_request", _adapted_handle)
    yield
