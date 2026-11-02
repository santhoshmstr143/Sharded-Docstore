"""
Entry point for running the Coordinator: python -m docstore.coordinator

Environment variables:
  HOST             listen host (default 127.0.0.1)
  PORT             listen port (default 8100)
  SHARD_MAP_PATH   path to persistent shard map file (default ./data/shard_map.json)
  ADMIN_TOKEN      >=32-char coordinator admin token
  CLUSTER_SECRET   >=32-char cluster shared secret
"""

from __future__ import annotations

import os

import uvicorn

from docstore.common.config import get_settings
from docstore.common.logging import configure_root_logger
from docstore.coordinator.app import build_coordinator_app
from docstore.coordinator.coordinator import CoordinatorService


def main() -> None:
    settings = get_settings()
    configure_root_logger(settings.LOG_LEVEL)

    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8100"))
    map_path = os.environ.get("SHARD_MAP_PATH", settings.SHARD_MAP_PATH)
    terms_path = os.environ.get("TERMS_PATH", None)

    service = CoordinatorService(settings, map_path=map_path, terms_path=terms_path)
    app = build_coordinator_app(settings, service)
    uvicorn.run(app, host=host, port=port, log_config=None)


if __name__ == "__main__":
    main()
