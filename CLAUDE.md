# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

A Python MCP server that connects Claude to Databricks SQL warehouses and jobs. Supports two transports: stdio (local) and HTTP/SSE (remote).

## Commands

```bash
# Install (Python 3.11+ required)
pip install -e .

# Run stdio server (local — for Claude Desktop / Claude Code)
databricks-mcp
# or directly:
python3.11 src/server.py

# Run HTTP/SSE server (remote — for Claude.ai connector)
pip install starlette uvicorn
uvicorn src.sse_server:app --host 0.0.0.0 --port 8000

# Health check (SSE mode)
curl http://localhost:8000/health
```

No test suite exists yet.

## Transport modes

**`src/server.py`** — Core MCP server (stdio). All tools and guardrail logic live here. Claude Desktop launches it via `python3.11 src/server.py`; registered in `~/Library/Application Support/Claude/claude_desktop_config.json` under `mcpServers.databricks`.

**`src/sse_server.py`** — Thin Starlette wrapper that imports `app` from `server.py` unchanged and exposes it over HTTP/SSE for remote Claude.ai connectors. Routes: `GET /sse`, `POST /messages/`, `GET /health`. Adds optional bearer-token auth via `MCP_BEARER_TOKEN` env var.

The PAT token must be passed via `env.DATABRICKS_TOKEN` in the MCP config — **not** hardcoded in `config/settings.json`.

## Tools exposed

| Tool | Description |
|---|---|
| `run_sql` | Execute a read-only SELECT query |
| `browse_catalog` | Navigate catalogs → schemas → tables → columns |
| `read_table` | Read rows with explicit column selection |
| `trigger_job` | Trigger a whitelisted Databricks job |

## Guardrails (all enforced at MCP server level)

| Guardrail | Config key | Default |
|---|---|---|
| Block Bronze layer | `block_bronze` | `true` — rejects queries referencing `bronze_*` |
| Block SELECT * | `block_select_star` | `true` — columns must be explicit |
| Read-only | — (always on) | Blocks INSERT/UPDATE/DELETE/DROP etc. |
| Row limit | `max_rows` | 1000 (Gold/general) |
| Silver row limit | `silver_max_rows` | 10000 (auto-applied to `silver_*` queries) |
| Query timeout | `query_timeout_sec` | 30s (enforced via asyncio.wait_for) |
| Result size cap | `max_result_bytes` | 2 MB |
| Max concurrency | `max_concurrent_queries` | 3 (asyncio.Semaphore) |
| Result cache TTL | `result_cache_ttl_sec` | 3600s (in-memory, keyed by normalized SQL) |
| Catalog whitelist | `allowed_catalogs` | `[]` = all allowed |
| Schema whitelist | `allowed_schemas` | `[]` = all allowed |
| Job whitelist | `allowed_job_ids` | `[]` = all allowed |

## Request lifecycle (`run_sql` / `read_table`)

1. Guardrail checks run synchronously (read-only, no-bronze, no-select-star, whitelist)
2. `inject_limit()` appends `LIMIT` if absent — Silver tables (`silver_*\.` pattern) get `silver_max_rows`, others get `max_rows`
3. In-memory TTL cache checked: dict keyed by `md5(normalized_sql)`, 1-hour default TTL
4. Cache miss: query runs in a `ThreadPoolExecutor` thread (Databricks SDK is sync), gated by `asyncio.Semaphore(MAX_CONCURRENT_QUERIES)` and `asyncio.wait_for(timeout=QUERY_TIMEOUT_SEC)`
5. `truncate_result()` enforces the byte cap before returning

Entry point: `run()` is a sync wrapper around `asyncio.run(main())` — required for the `databricks-mcp` CLI script.

## Adding/changing guardrails

Edit `config/settings.json` — no code changes needed for threshold adjustments.
To add a new enforcement rule, add a helper function in the "Guardrail helpers" section of `src/server.py` and call it from the relevant tool functions.
