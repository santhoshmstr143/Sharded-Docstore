"""Command Line Interface (CLI) for Sharded-Docstore.

Provides commands to interact with the Sharded-Docstore cluster:
  - insert, get, update, delete
  - query (with filter, sorting, pagination, read_mode)
  - status, shardmap
  - migrate, bulk-load
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import click

from docstore.client.client import (
    AuthenticationError,
    DocStoreClient,
    DocStoreClientError,
    DuplicateKeyError,
    InvalidQueryError,
    NotFoundError,
)


def _output_json(data: Any) -> None:
    """Print formatted JSON to stdout."""
    click.echo(json.dumps(data, indent=2, sort_keys=True))


_REQUEST_ID_OPTION = click.option(
    "--request-id",
    type=str,
    default=None,
    help="Idempotency key. Reuse the id printed after an OUTCOME_UNKNOWN error to retry safely.",
)


def _handle_error(exc: Exception) -> None:
    """Print clean error message to stderr and exit with semantic exit code."""
    rid = getattr(exc, "request_id", None)
    if rid and isinstance(exc, DocStoreClientError):
        # Without this id an OUTCOME_UNKNOWN write cannot be retried idempotently.
        click.echo(f"request_id: {rid} (retry with --request-id {rid})", err=True)
    if isinstance(exc, NotFoundError):
        click.echo(f"Error: {exc.message} ({exc.error_code or 'NOT_FOUND'})", err=True)
        sys.exit(2)
    elif isinstance(exc, DuplicateKeyError):
        click.echo(f"Error: {exc.message} ({exc.error_code or 'DUPLICATE_KEY'})", err=True)
        sys.exit(3)
    elif isinstance(exc, AuthenticationError):
        click.echo(f"Error: Authentication failed: {exc.message}", err=True)
        sys.exit(4)
    elif isinstance(exc, InvalidQueryError):
        click.echo(f"Error: {exc.message} ({exc.error_code or 'INVALID_QUERY'})", err=True)
        sys.exit(5)
    elif isinstance(exc, DocStoreClientError):
        click.echo(f"Error: {exc.message} [{exc.status_code or ''}]", err=True)
        sys.exit(1)
    else:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)


@click.group()
@click.option(
    "--router-url",
    envvar=["ROUTER_URL", "DOCSTORE_ROUTER_URL"],
    default="http://127.0.0.1:8000",
    help="Stateless Router URL",
)
@click.option(
    "--coordinator-url",
    envvar=["COORDINATOR_URL", "DOCSTORE_COORDINATOR_URL"],
    default="http://127.0.0.1:8100",
    help="Coordinator Service URL",
)
@click.option(
    "--api-key",
    envvar="CLIENT_API_KEY",
    default=None,
    help="Client API Key for Router",
)
@click.option(
    "--admin-token",
    envvar="ADMIN_TOKEN",
    default=None,
    help="Admin Token for Coordinator",
)
@click.pass_context
def cli(
    ctx: click.Context,
    router_url: str,
    coordinator_url: str,
    api_key: str | None,
    admin_token: str | None,
) -> None:
    """Sharded-Docstore CLI tool."""
    ctx.ensure_object(dict)
    if "client" not in ctx.obj:
        ctx.obj["client"] = DocStoreClient(
            router_url=router_url,
            coordinator_url=coordinator_url,
            api_key=api_key,
            admin_token=admin_token,
        )


# ── Subcommand: insert ────────────────────────────────────────────────────────


@cli.command("insert")
@click.option("--doc", "-d", type=str, help="Document as JSON string")
@click.option(
    "--file",
    "-f",
    type=click.Path(exists=True),
    help="Path to JSON file containing document",
)
@_REQUEST_ID_OPTION
@click.pass_context
def insert_cmd(
    ctx: click.Context, doc: str | None, file: str | None, request_id: str | None
) -> None:
    """Insert a new document."""
    client: DocStoreClient = ctx.obj["client"]
    try:
        if file:
            payload = json.loads(Path(file).read_text(encoding="utf-8"))
        elif doc:
            payload = json.loads(doc)
        else:
            raise click.UsageError("Must specify either --doc or --file.")

        result = client.insert(payload, request_id=request_id)
        _output_json(result)
    except Exception as exc:
        _handle_error(exc)


# ── Subcommand: get ───────────────────────────────────────────────────────────


@cli.command("get")
@click.argument("doc_id", type=int)
@click.option(
    "--read-mode",
    type=click.Choice(["primary", "follower"], case_sensitive=False),
    default="primary",
    help="Target replica for read ('primary' or 'follower')",
)
@click.pass_context
def get_cmd(ctx: click.Context, doc_id: int, read_mode: str) -> None:
    """Fetch a document by ID."""
    client: DocStoreClient = ctx.obj["client"]
    try:
        result = client.get(doc_id, read_mode=read_mode)
        _output_json(result)
    except Exception as exc:
        _handle_error(exc)


# ── Subcommand: update ────────────────────────────────────────────────────────


@cli.command("update")
@click.argument("doc_id", type=int)
@click.option("--doc", "-d", type=str, help="Replacement document as JSON string")
@click.option("--file", "-f", type=click.Path(exists=True), help="Path to JSON file")
@_REQUEST_ID_OPTION
@click.pass_context
def update_cmd(
    ctx: click.Context, doc_id: int, doc: str | None, file: str | None, request_id: str | None
) -> None:
    """Replace a document by ID."""
    client: DocStoreClient = ctx.obj["client"]
    try:
        if file:
            payload = json.loads(Path(file).read_text(encoding="utf-8"))
        elif doc:
            payload = json.loads(doc)
        else:
            raise click.UsageError("Must specify either --doc or --file.")

        result = client.update(doc_id, payload, request_id=request_id)
        _output_json(result)
    except Exception as exc:
        _handle_error(exc)


# ── Subcommand: delete ────────────────────────────────────────────────────────


@cli.command("delete")
@click.argument("doc_id", type=int)
@_REQUEST_ID_OPTION
@click.pass_context
def delete_cmd(ctx: click.Context, doc_id: int, request_id: str | None) -> None:
    """Delete a document by ID."""
    client: DocStoreClient = ctx.obj["client"]
    try:
        result = client.delete(doc_id, request_id=request_id)
        _output_json(result)
    except Exception as exc:
        _handle_error(exc)


# ── Subcommand: query ─────────────────────────────────────────────────────────


@cli.command("query")
@click.option(
    "--filter",
    "-q",
    "filter_expr",
    type=str,
    default="{}",
    help="Filter expression JSON",
)
@click.option("--sort-field", "-s", type=str, default=None, help="Field name to sort by")
@click.option(
    "--sort-dir",
    type=click.Choice(["asc", "desc"], case_sensitive=False),
    default="asc",
    help="Sort direction",
)
@click.option("--skip", type=int, default=0, help="Number of records to skip")
@click.option("--limit", type=int, default=100, help="Max records to return")
@click.option(
    "--read-mode",
    type=click.Choice(["primary", "follower"], case_sensitive=False),
    default="primary",
    help="Target replica for reads",
)
@click.pass_context
def query_cmd(
    ctx: click.Context,
    filter_expr: str,
    sort_field: str | None,
    sort_dir: str,
    skip: int,
    limit: int,
    read_mode: str,
) -> None:
    """Execute a filter query across cluster shards."""
    client: DocStoreClient = ctx.obj["client"]
    try:
        f = json.loads(filter_expr)
        result = client.query(
            filter=f,
            sort_field=sort_field,
            sort_dir=sort_dir,
            skip=skip,
            limit=limit,
            read_mode=read_mode,
        )
        _output_json(result)
    except Exception as exc:
        _handle_error(exc)


# ── Subcommand: status ────────────────────────────────────────────────────────


@cli.command("status")
@click.pass_context
def status_cmd(ctx: click.Context) -> None:
    """Display cluster health, node statuses, and active leases."""
    client: DocStoreClient = ctx.obj["client"]
    try:
        result = client.status()
        _output_json(result)
    except Exception as exc:
        _handle_error(exc)


# ── Subcommand: shardmap ──────────────────────────────────────────────────────


@cli.command("shardmap")
@click.pass_context
def shardmap_cmd(ctx: click.Context) -> None:
    """Fetch current shard map topology and ownership epochs."""
    client: DocStoreClient = ctx.obj["client"]
    try:
        result = client.shard_map()
        _output_json(result)
    except Exception as exc:
        _handle_error(exc)


# ── Subcommand: migrate ───────────────────────────────────────────────────────


@cli.command("migrate")
@click.option("--from-shard", required=True, type=str, help="Source shard ID")
@click.option("--to-shard", required=True, type=str, help="Destination shard ID")
@click.option("--start", required=True, type=int, help="Key range inclusive start")
@click.option("--end", required=True, type=int, help="Key range exclusive end")
@click.option("--sync/--no-sync", default=True, help="Wait for migration to complete")
@click.pass_context
def migrate_cmd(
    ctx: click.Context,
    from_shard: str,
    to_shard: str,
    start: int,
    end: int,
    sync: bool,
) -> None:
    """Trigger live range migration between shards."""
    client: DocStoreClient = ctx.obj["client"]
    try:
        result = client.migrate(
            from_shard=from_shard,
            to_shard=to_shard,
            start=start,
            end=end,
            sync=sync,
        )
        _output_json(result)
    except Exception as exc:
        _handle_error(exc)


# ── Subcommand: bulk-load ─────────────────────────────────────────────────────


@cli.command("bulk-load")
@click.argument("file", type=click.Path(exists=True))
@click.pass_context
def bulk_load_cmd(ctx: click.Context, file: str) -> None:
    """Bulk-load a JSON file containing a list of documents."""
    client: DocStoreClient = ctx.obj["client"]
    try:
        data = json.loads(Path(file).read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise click.UsageError("File must contain a JSON array of documents.")
        result = client.bulk_load(data)
        _output_json(result)
    except Exception as exc:
        _handle_error(exc)


def main() -> None:
    """CLI entrypoint."""
    cli(obj={})


if __name__ == "__main__":
    main()
