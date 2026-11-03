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
