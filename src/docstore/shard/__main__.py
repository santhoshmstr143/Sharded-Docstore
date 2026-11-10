"""
Entry point: python -m docstore.shard

Environment variables (all required unless defaults are noted):
  NODE_ID               unique node identifier (e.g. "shard0-primary")
  SHARD_ID              shard identifier      (e.g. "shard0")
  ROLE                  node role: "primary" (default) or "follower"  ← H1
  FOLLOWER_URL          host:port of the follower (primary only)       ← H1
  STANDALONE_MODE       set to "true" to disable lease protection      ← L2
  SHARD_RANGE_START     inclusive start of owned range (default 0)
  SHARD_RANGE_END       exclusive end of owned range   (default 2^63)
  SHARD_OWNERSHIP_EPOCH epoch for this range           (default 1)
  PORT                  listen port                    (default 8200)
  CLUSTER_SECRET        ≥32-char shared secret
  ADMIN_TOKEN           ≥32-char admin token
  CLIENT_API_KEY        ≥16-char client API key
  DATA_DIR              base data directory            (default ./data)
"""

from __future__ import annotations

import os

import uvicorn

from docstore.common.config import get_settings
from docstore.common.logging import configure_root_logger, get_logger
from docstore.common.models import NodeRole
from docstore.shard.app import build_app


def main() -> None:
    settings = get_settings()
    configure_root_logger(settings.LOG_LEVEL)
    startup_logger = get_logger(__name__)

    node_id = os.environ.get("NODE_ID", "shard-node-default")
    shard_id = os.environ.get("SHARD_ID", "shard0")
    port = int(os.environ.get("PORT", "8200"))
    host = os.environ.get("HOST", "127.0.0.1")

    # H1: honour ROLE and FOLLOWER_URL env vars (previously ignored)
    role_str = os.environ.get("ROLE", "primary").lower()
    role = NodeRole.FOLLOWER if role_str == "follower" else NodeRole.PRIMARY

    follower_url: str | None = None
    if role == NodeRole.PRIMARY:
        follower_url = os.environ.get("FOLLOWER_URL") or None

    # L2: require explicit opt-in for infinite lease (standalone / dev mode)
    standalone = os.environ.get("STANDALONE_MODE", "false").lower() == "true"
    if standalone:
        startup_logger.warning(
            "STANDALONE_MODE=true — primary lease protection is DISABLED. "
            "Do NOT use this in a production coordinated deployment."
        )
        initial_lease: float | None = float("inf")
    else:
        # No authority until the coordinator grants a lease at registration: a node must
        # never accept writes (or primary reads) on a lease it gave itself.
        initial_lease = 0.0

    app = build_app(
        settings,
        node_id=node_id,
        shard_id=shard_id,
        role=role,
        follower_url=follower_url,
        lease_expires_at=initial_lease,
        allow_standalone=standalone,
    )

    uvicorn.run(app, host=host, port=port, log_config=None)


if __name__ == "__main__":
    main()
