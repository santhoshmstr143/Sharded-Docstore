# ─────────────────────────────────────────────────────────────────────────────
# Makefile — Sharded-Docstore
# ─────────────────────────────────────────────────────────────────────────────
.DEFAULT_GOAL := help
PYTHON        := python3.11
VENV          := .venv
PIP           := $(VENV)/bin/pip
PYTEST        := $(VENV)/bin/pytest
RUFF          := $(VENV)/bin/ruff
MYPY          := $(VENV)/bin/mypy
BANDIT        := $(VENV)/bin/bandit
PIP_AUDIT     := $(VENV)/bin/pip-audit

# ── Environment setup ────────────────────────────────────────────────────────
.PHONY: setup
setup:           ## Create venv and install all dependencies
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install -r requirements-dev.txt
	$(VENV)/bin/pre-commit install
	@echo "✅  Setup complete. Activate with: source $(VENV)/bin/activate"

# ── Code quality ─────────────────────────────────────────────────────────────
.PHONY: lint
lint:            ## Run ruff linter (auto-fix safe issues)
	$(RUFF) check src/ tests/ --fix
	$(RUFF) format src/ tests/

.PHONY: lint-check
lint-check:      ## Run ruff in check-only mode (CI)
	$(RUFF) check src/ tests/
	$(RUFF) format src/ tests/ --check

.PHONY: typecheck
typecheck:       ## Run mypy strict type-checking on src/
	$(MYPY) src/docstore/

.PHONY: security
security:        ## Run bandit static security analysis
	$(BANDIT) -r src/ -ll

.PHONY: audit
audit:           ## Audit dependencies for known vulnerabilities
	$(PIP_AUDIT) -r requirements.txt -r requirements-dev.txt

# ── Testing ──────────────────────────────────────────────────────────────────
.PHONY: test
test:            ## Run the full test suite with coverage
	$(PYTEST) tests/ -m "unit or integration or fault or security" \
		--cov=src/docstore --cov-report=term-missing --cov-report=html

.PHONY: test-fast
test-fast:       ## Run unit + integration tests only (skip slow fault tests)
	$(PYTEST) tests/ -m "unit or integration" -q

.PHONY: test-unit
test-unit:       ## Run unit tests only
	$(PYTEST) tests/ -m unit -q

.PHONY: test-fault
test-fault:      ## Run fault-injection tests (requires ENABLE_TEST_HOOKS=true)
	ENABLE_TEST_HOOKS=true $(PYTEST) tests/ -m fault -v

.PHONY: test-security
test-security:   ## Run security-specific tests
	$(PYTEST) tests/ -m security -v

# ── Full quality gate (run before every commit) ───────────────────────────────
.PHONY: check
check: lint-check typecheck security audit test-fast   ## Run all quality gates

# ── Application tasks ────────────────────────────────────────────────────────
.PHONY: demo
demo:            ## Run the scripted end-to-end demo
	PYTHONPATH=src $(VENV)/bin/python scripts/demo.py

.PHONY: bench
bench:           ## Run performance benchmarks and generate figures
	PYTHONPATH=src $(VENV)/bin/python -m bench.runner
	PYTHONPATH=src $(VENV)/bin/python -m bench.plot

# ── Docker ───────────────────────────────────────────────────────────────────
.PHONY: up
up:              ## Start the full Docker Compose cluster
	@if [ ! -f .env ]; then \
		sed -e "s/^CLUSTER_SECRET=.*/CLUSTER_SECRET=$$(python3 -c 'import secrets;print(secrets.token_hex(32))')/" \
		    -e "s/^ADMIN_TOKEN=.*/ADMIN_TOKEN=$$(python3 -c 'import secrets;print(secrets.token_hex(32))')/" \
		    -e "s/^CLIENT_API_KEY=.*/CLIENT_API_KEY=$$(python3 -c 'import secrets;print(secrets.token_hex(16))')/" \
		    .env.example > .env; \
		echo 'Generated .env with random secrets'; \
	fi
	docker compose -f docker/docker-compose.yml up -d

.PHONY: down
down:            ## Stop the Docker Compose cluster
	docker compose -f docker/docker-compose.yml down

.PHONY: logs
logs:            ## Tail logs from all services
	docker compose -f docker/docker-compose.yml logs -f

.PHONY: reset
reset:           ## Stop cluster and remove all volumes (destroys data!)
	docker compose -f docker/docker-compose.yml down -v

# ── Housekeeping ─────────────────────────────────────────────────────────────
.PHONY: clean
clean:           ## Remove caches, coverage, and build artefacts
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache htmlcov .coverage coverage.xml dist build

.PHONY: help
help:            ## Show this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} \
	     /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
