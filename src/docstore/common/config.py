"""
Configuration settings for Sharded-Docstore nodes.

Loaded from environment variables (or a .env file).
All timing constraints are validated at startup:
  - H < L < D
  - T + M < L - H
"""

from __future__ import annotations

import functools

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Cluster-wide and node-local configuration.

    All values are read from environment variables.
    Copy ``.env.example`` to ``.env`` and fill in real values.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # ignore unknown env vars
    )

    # ── Authentication (required — no defaults) ───────────────────────────────
    CLUSTER_SECRET: str = Field(
        min_length=32,
        description="Shared secret for inter-node HMAC authentication.",
    )
    ADMIN_TOKEN: str = Field(
        min_length=32,
        description="Token for coordinator admin endpoints.",
    )
    CLIENT_API_KEY: str = Field(
        min_length=16,
        description="API key required by clients hitting the router.",
    )

    # ── Timing parameters ─────────────────────────────────────────────────────
    HEARTBEAT_INTERVAL_S: float = Field(
        default=1.0,
        gt=0,
        description="Heartbeat interval H (seconds). Must satisfy H < L < D.",
    )
    LEASE_DURATION_S: float = Field(
        default=3.0,
        gt=0,
        description="Lease duration L (seconds).",
    )
    FAILURE_TIMEOUT_S: float = Field(
        default=5.0,
        gt=0,
        description="Failure-detection timeout D (seconds).",
    )
    SAFETY_MARGIN_S: float = Field(
        default=0.5,
        gt=0,
        description="Safety margin M (seconds). Writes need remaining lease > T + M.",
    )
    REPLICATION_TIMEOUT_S: float = Field(
        default=1.0,
        gt=0,
        description="Replication timeout T (seconds).",
    )
    MIGRATION_CATCHUP_MAX_DURATION_S: float = Field(
        default=60.0,
        gt=0,
        description="Maximum duration in seconds for pre-pause migration catch-up.",
    )
    MIGRATION_FINAL_CATCHUP_MAX_DURATION_S: float = Field(
        default=30.0,
        gt=0,
        description="Maximum duration in seconds for post-pause final migration catch-up.",
    )

    # ── Storage ───────────────────────────────────────────────────────────────
    DATA_DIR: str = Field(
        default="./data",
        description="Base directory for WAL files and snapshots.",
    )

    # ── Feature flags ────────────────────────────────────────────────────────
    ENABLE_TEST_HOOKS: bool = Field(
        default=False,
        description="Enable crash-point hooks for fault tests. NEVER true in production.",
    )
    STANDALONE_MODE: bool = Field(
        default=False,
        description="Explicit opt-in for standalone development mode (unreplicated commits).",
    )

    # ── Logging ───────────────────────────────────────────────────────────────
    LOG_LEVEL: str = Field(
        default="INFO",
        pattern=r"^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$",
        description="Log level.",
    )

    # ── Derived / computed constants (not from env) ───────────────────────────
    MAX_DOC_SIZE_BYTES: int = 1 * 1024 * 1024  # 1 MB
    MAX_BODY_SIZE_BYTES: int = 2 * 1024 * 1024  # 2 MB
    QUERY_LIMIT_DEFAULT: int = 100
    QUERY_LIMIT_MAX: int = 1_000
    QUERY_SKIP_MAX: int = 10_000
    FILTER_MAX_DEPTH: int = 5
    FILTER_MAX_CLAUSES: int = 50
    FILTER_IN_MAX_SIZE: int = 1_000
    SNAPSHOT_EVERY_N_OPS: int = 1_000
    SNAPSHOT_EVERY_S: float = 60.0
    DEDUP_MAX_ENTRIES: int = 100_000
    DEDUP_TTL_S: float = 600.0  # 10 minutes

    DOC_ID_MAX: int = (2**63) - 1
    DOC_ID_MIN: int = 0

    MAX_CONCURRENT_REQUESTS: int = 1_000
    SHARD_MAP_PATH: str = "./data/shard_map.json"

    @model_validator(mode="after")
    def _validate_timing_constraints(self) -> Settings:
        """Enforce H < L < D and T + M < L - H at startup."""
        H = self.HEARTBEAT_INTERVAL_S
        L = self.LEASE_DURATION_S
        D = self.FAILURE_TIMEOUT_S
        T = self.REPLICATION_TIMEOUT_S
        M = self.SAFETY_MARGIN_S

        if not (H < L < D):
            raise ValueError(
                f"Timing constraint H < L < D violated: "
                f"H={H} L={L} D={D}. Adjust HEARTBEAT_INTERVAL_S, "
                f"LEASE_DURATION_S, or FAILURE_TIMEOUT_S."
            )

        if not (T + M < L - H):
            raise ValueError(
                f"Timing constraint T + M < L - H violated: "
                f"T+M={T + M:.3f} >= L-H={L - H:.3f}. "
                f"A freshly renewed lease must leave enough time for a write."
            )

        return self


@functools.lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached global Settings instance.

    Call ``get_settings.cache_clear()`` in tests to reload from env.
    """
    return Settings()  # type: ignore[call-arg]
