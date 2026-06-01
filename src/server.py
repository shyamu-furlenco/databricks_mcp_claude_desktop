"""
Databricks MCP Server
Exposes: SQL execution, catalog browsing, table reads, job triggers
Guardrails: read-only, no-bronze, no-select-star, schema whitelist,
            row/result limits, query timeout, concurrency cap, result cache
"""

import os
import re
import json
import time
import hashlib
import asyncio
import logging
import contextvars
from concurrent.futures import ThreadPoolExecutor

from databricks import sql as databricks_sql
from databricks.sdk import WorkspaceClient

import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("databricks-mcp")

# ── Config ────────────────────────────────────────────────────────────────────
def load_config() -> dict:
    cfg_path = os.path.join(os.path.dirname(__file__), "../config/settings.json")
    with open(cfg_path) as f:
        return json.load(f)

CFG = load_config()

DATABRICKS_HOST  = os.getenv("DATABRICKS_HOST",     CFG["databricks"]["host"])
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN",     CFG["databricks"]["token"])  # fallback for stdio/local mode
HTTP_PATH        = os.getenv("DATABRICKS_HTTP_PATH", CFG["databricks"]["http_path"])

# Per-connection token (set by SSE middleware for remote mode; falls back to DATABRICKS_TOKEN for stdio)
_token_var: contextvars.ContextVar[str] = contextvars.ContextVar("databricks_token", default="")

GUARDRAILS             = CFG["guardrails"]
ALLOWED_CATALOGS       = set(GUARDRAILS.get("allowed_catalogs", []))
ALLOWED_SCHEMAS        = set(GUARDRAILS.get("allowed_schemas",  []))
MAX_ROWS               = int(GUARDRAILS.get("max_rows",              1000))
SILVER_MAX_ROWS        = int(GUARDRAILS.get("silver_max_rows",      10000))
QUERY_TIMEOUT_SEC      = int(GUARDRAILS.get("query_timeout_sec",        30))
MAX_RESULT_BYTES       = int(GUARDRAILS.get("max_result_bytes",  2_000_000))
MAX_CONCURRENT_QUERIES = int(GUARDRAILS.get("max_concurrent_queries",    3))
RESULT_CACHE_TTL_SEC   = int(GUARDRAILS.get("result_cache_ttl_sec",   3600))
BLOCK_BRONZE           = bool(GUARDRAILS.get("block_bronze",           True))
BLOCK_SELECT_STAR      = bool(GUARDRAILS.get("block_select_star",      True))

# ── Guardrail patterns ────────────────────────────────────────────────────────
_WRITE_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|CREATE|ALTER|MERGE|REPLACE|GRANT|REVOKE|COPY)\b",
    re.IGNORECASE,
)
# Catches bronze_something. (catalog or schema reference in a dotted path)
_BRONZE_PATTERN = re.compile(r"\bbronze[_\w]*\s*\.", re.IGNORECASE)
# Catches SELECT * FROM or SELECT *, col — but not COUNT(*)
_SELECT_STAR_PATTERN = re.compile(r"\bSELECT\s+\*\s*(?:,|\bFROM\b)", re.IGNORECASE)
# Detects Silver table references for auto-LIMIT
_SILVER_PATTERN = re.compile(r"\bsilver[_\w]*\s*\.", re.IGNORECASE)

# ── Guardrail helpers ─────────────────────────────────────────────────────────
def enforce_read_only(sql: str):
    m = _WRITE_PATTERN.search(sql)
    if m:
        raise PermissionError(f"Write operation '{m.group()}' is not allowed. This MCP is read-only.")

def enforce_no_bronze(sql: str):
    if BLOCK_BRONZE and _BRONZE_PATTERN.search(sql):
        raise PermissionError(
            "Bronze layer queries are not allowed. "
            "Ask the data team to create a Gold or Silver view for this data."
        )

def enforce_no_select_star(sql: str):
    if BLOCK_SELECT_STAR and _SELECT_STAR_PATTERN.search(sql):
        raise PermissionError(
            "SELECT * is not allowed. Specify only the columns you need "
            "(e.g. SELECT col1, col2 FROM ...)."
        )

def enforce_catalog_whitelist(catalog: str):
    if BLOCK_BRONZE and re.match(r"^bronze", catalog, re.IGNORECASE):
        raise PermissionError(f"Bronze catalog '{catalog}' is blocked.")
    if ALLOWED_CATALOGS and catalog not in ALLOWED_CATALOGS:
        raise PermissionError(
            f"Catalog '{catalog}' is not in the allowed list: {sorted(ALLOWED_CATALOGS)}"
        )

def enforce_schema_whitelist(schema: str):
    if BLOCK_BRONZE and re.match(r"^bronze", schema, re.IGNORECASE):
        raise PermissionError(f"Bronze schema '{schema}' is blocked.")
    if ALLOWED_SCHEMAS and schema not in ALLOWED_SCHEMAS:
        raise PermissionError(
            f"Schema '{schema}' is not in the allowed list: {sorted(ALLOWED_SCHEMAS)}"
        )

def inject_limit(sql: str, max_rows: int) -> str:
    """Append LIMIT if none exists. Silver queries get SILVER_MAX_ROWS."""
    clean = sql.rstrip().rstrip(";")
    if not re.search(r"\bLIMIT\s+\d+", clean, re.IGNORECASE):
        limit = SILVER_MAX_ROWS if _SILVER_PATTERN.search(clean) else max_rows
        clean = f"{clean} LIMIT {limit}"
    return clean

def truncate_result(rows: list[dict], max_bytes: int) -> tuple[list[dict], bool]:
    buf, truncated = [], False
    total = 0
    for row in rows:
        size = len(json.dumps(row))
        if total + size > max_bytes:
            truncated = True
            break
        buf.append(row)
        total += size
    return buf, truncated

# ── Result cache (in-memory, TTL-based) ───────────────────────────────────────
_result_cache: dict = {}

def _sql_cache_key(query: str) -> str:
    token = _token_var.get() or DATABRICKS_TOKEN
    return hashlib.md5(f"{token}:{query.strip().lower()}".encode()).hexdigest()

def _cache_get(key: str):
    entry = _result_cache.get(key)
    if entry:
        ts, result = entry
        if time.time() - ts < RESULT_CACHE_TTL_SEC:
            return result
        del _result_cache[key]
    return None

def _cache_set(key: str, result) -> None:
    _result_cache[key] = (time.time(), result)

# ── Async executor + concurrency cap ─────────────────────────────────────────
_executor = ThreadPoolExecutor(max_workers=4)
_query_semaphore = asyncio.Semaphore(MAX_CONCURRENT_QUERIES)

async def _run_sql_blocking(query: str):
    """Run a synchronous SQL query in a thread with timeout and concurrency enforcement."""
    key = _sql_cache_key(query)
    cached = _cache_get(key)
    if cached is not None:
        log.info(f"cache hit: {query[:80]!r}")
        return cached

    def _execute():
        conn = get_sql_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                columns = [d[0] for d in cur.description] if cur.description else []
                raw_rows = cur.fetchall()
                return columns, [dict(zip(columns, r)) for r in raw_rows]
        finally:
            conn.close()

    async with _query_semaphore:
        loop = asyncio.get_running_loop()
        result = await asyncio.wait_for(
            loop.run_in_executor(_executor, _execute),
            timeout=QUERY_TIMEOUT_SEC,
        )

    _cache_set(key, result)
    return result

# ── Databricks connections ────────────────────────────────────────────────────
def _active_token() -> str:
    token = _token_var.get() or DATABRICKS_TOKEN
    if not token:
        raise PermissionError(
            "No Databricks token. In Claude.ai, set your PAT as the connector Bearer token."
        )
    return token

def get_sql_connection():
    return databricks_sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=HTTP_PATH,
        access_token=_active_token(),
    )

def get_workspace_client() -> WorkspaceClient:
    return WorkspaceClient(host=DATABRICKS_HOST, token=_active_token())

# ── MCP server setup ──────────────────────────────────────────────────────────
app = Server("databricks-mcp")

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    if name == "run_sql":
        return await tool_run_sql(arguments)
    elif name == "browse_catalog":
        return await tool_browse_catalog(arguments)
    elif name == "read_table":
        return await tool_read_table(arguments)
    elif name == "trigger_job":
        return await tool_trigger_job(arguments)
    else:
        raise ValueError(f"Unknown tool: {name}")


# ── Tool: run_sql ──────────────────────────────────────────────────────────────
async def tool_run_sql(args: dict) -> list[types.TextContent]:
    query = args.get("query", "").strip()
    if not query:
        raise ValueError("'query' is required.")

    enforce_read_only(query)
    enforce_no_bronze(query)
    enforce_no_select_star(query)
    query = inject_limit(query, MAX_ROWS)

    start = time.time()
    try:
        columns, rows = await _run_sql_blocking(query)
    except asyncio.TimeoutError:
        raise TimeoutError(
            f"Query exceeded {QUERY_TIMEOUT_SEC}s timeout. "
            "Simplify the query or ask the data team to build a Gold materialized view."
        )

    rows, truncated = truncate_result(rows, MAX_RESULT_BYTES)
    elapsed = round(time.time() - start, 2)

    result = {
        "row_count": len(rows),
        "truncated": truncated,
        "elapsed_sec": elapsed,
        "columns": columns,
        "rows": rows,
    }
    if truncated:
        result["warning"] = f"Result truncated at {MAX_RESULT_BYTES} bytes. Refine your query."

    log.info(f"run_sql rows={len(rows)} elapsed={elapsed}s truncated={truncated} query={query[:80]!r}")
    return [types.TextContent(type="text", text=json.dumps(result, default=str, indent=2))]


# ── Tool: browse_catalog ───────────────────────────────────────────────────────
async def tool_browse_catalog(args: dict) -> list[types.TextContent]:
    level   = args.get("level", "catalogs")
    catalog = args.get("catalog", "")
    schema  = args.get("schema",  "")

    if catalog:
        enforce_catalog_whitelist(catalog)
    if schema:
        enforce_schema_whitelist(schema)

    table = args.get("table", "")
    bkey = f"browse:{level}:{catalog.lower()}:{schema.lower()}:{table.lower()}"
    cached_items = _cache_get(bkey)
    if cached_items is not None:
        log.info(f"cache hit: browse_catalog level={level}")
        return [types.TextContent(type="text", text=json.dumps({"level": level, "items": cached_items}, indent=2))]

    def _browse():
        conn = get_sql_connection()
        try:
            with conn.cursor() as cur:
                if level == "catalogs":
                    cur.execute("SHOW CATALOGS")
                    result = [r[0] for r in cur.fetchall()]
                    if BLOCK_BRONZE:
                        result = [c for c in result if not re.match(r"^bronze", c, re.IGNORECASE)]
                    if ALLOWED_CATALOGS:
                        result = [i for i in result if i in ALLOWED_CATALOGS]
                elif level == "schemas":
                    if not catalog:
                        raise ValueError("'catalog' required for level=schemas")
                    cur.execute(f"SHOW SCHEMAS IN {catalog}")
                    result = [r[0] for r in cur.fetchall()]
                    if BLOCK_BRONZE:
                        result = [s for s in result if not re.match(r"^bronze", s, re.IGNORECASE)]
                    if ALLOWED_SCHEMAS:
                        result = [i for i in result if i in ALLOWED_SCHEMAS]
                elif level == "tables":
                    if not catalog or not schema:
                        raise ValueError("'catalog' and 'schema' required for level=tables")
                    cur.execute(f"SHOW TABLES IN {catalog}.{schema}")
                    # SHOW TABLES returns: (namespace, tableName, isTemporary)
                    result = [{"table": r[1], "is_temporary": r[2]} for r in cur.fetchall()]
                elif level == "columns":
                    if not catalog or not schema or not table:
                        raise ValueError("'catalog', 'schema', 'table' required for level=columns")
                    cur.execute(f"DESCRIBE {catalog}.{schema}.{table}")
                    result = [{"col_name": r[0], "data_type": r[1], "comment": r[2]} for r in cur.fetchall()]
                else:
                    raise ValueError(f"Unknown level '{level}'. Use: catalogs|schemas|tables|columns")
                return result
        finally:
            conn.close()

    async with _query_semaphore:
        loop = asyncio.get_running_loop()
        try:
            items = await asyncio.wait_for(
                loop.run_in_executor(_executor, _browse),
                timeout=QUERY_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            raise TimeoutError(f"Catalog browse exceeded {QUERY_TIMEOUT_SEC}s timeout.")

    _cache_set(bkey, items)
    log.info(f"browse_catalog level={level} catalog={catalog!r} schema={schema!r} items={len(items)}")
    return [types.TextContent(type="text", text=json.dumps({"level": level, "items": items}, indent=2))]


# ── Tool: read_table ───────────────────────────────────────────────────────────
async def tool_read_table(args: dict) -> list[types.TextContent]:
    catalog = args.get("catalog", "")
    schema  = args.get("schema",  "")
    table   = args.get("table",   "")
    columns = args.get("columns", "")
    filters = args.get("where",   "")

    if not (catalog and schema and table):
        raise ValueError("'catalog', 'schema', and 'table' are required.")
    if not columns or columns.strip() == "*":
        raise PermissionError(
            "SELECT * is not allowed. Specify column names via the 'columns' parameter "
            "(e.g. 'id, name, amount')."
        )

    enforce_catalog_whitelist(catalog)
    enforce_schema_whitelist(schema)

    col_expr = ", ".join(c.strip() for c in columns.split(","))
    query = f"SELECT {col_expr} FROM {catalog}.{schema}.{table}"
    if filters:
        enforce_read_only(filters)
        query += f" WHERE {filters}"
    query = inject_limit(query, MAX_ROWS)

    try:
        col_names, rows = await _run_sql_blocking(query)
    except asyncio.TimeoutError:
        raise TimeoutError(f"Query exceeded {QUERY_TIMEOUT_SEC}s timeout.")

    rows, truncated = truncate_result(rows, MAX_RESULT_BYTES)
    result = {"row_count": len(rows), "truncated": truncated, "columns": col_names, "rows": rows}
    if truncated:
        result["warning"] = f"Result truncated at {MAX_RESULT_BYTES} bytes."

    log.info(f"read_table {catalog}.{schema}.{table} rows={len(rows)} truncated={truncated}")
    return [types.TextContent(type="text", text=json.dumps(result, default=str, indent=2))]


# ── Tool: trigger_job ──────────────────────────────────────────────────────────
async def tool_trigger_job(args: dict) -> list[types.TextContent]:
    job_id          = args.get("job_id")
    notebook_params = args.get("notebook_params", {})
    python_params   = args.get("python_params",   [])

    if not job_id:
        raise ValueError("'job_id' is required.")

    allowed_jobs = GUARDRAILS.get("allowed_job_ids", [])
    if allowed_jobs and int(job_id) not in [int(j) for j in allowed_jobs]:
        raise PermissionError(
            f"Job {job_id} is not in the allowed jobs list: {allowed_jobs}"
        )

    wc = get_workspace_client()
    run = wc.jobs.run_now(
        job_id=int(job_id),
        notebook_params=notebook_params or None,
        python_params=python_params or None,
    )
    result = {
        "run_id":  run.run_id,
        "job_id":  job_id,
        "status":  "triggered",
        "run_url": f"https://{DATABRICKS_HOST}/jobs/{job_id}/runs/{run.run_id}",
    }
    log.info(f"trigger_job job={job_id} run={run.run_id}")
    return [types.TextContent(type="text", text=json.dumps(result, indent=2))]


# ── Tool listing ──────────────────────────────────────────────────────────────
@app.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="run_sql",
            description=(
                "Execute a read-only SQL SELECT query on Databricks. "
                f"Max {MAX_ROWS} rows (Silver tables: {SILVER_MAX_ROWS} rows), {QUERY_TIMEOUT_SEC}s timeout. "
                "Rules enforced: no SELECT *, no Bronze tables, no write operations. "
                "Always prefer Gold tables first; fall back to Silver only when needed."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": (
                            "SQL SELECT statement. Must list columns explicitly — "
                            "SELECT * is blocked. Never query bronze_* catalogs or schemas."
                        ),
                    },
                },
                "required": ["query"],
            },
        ),
        types.Tool(
            name="browse_catalog",
            description=(
                "Browse Databricks Unity Catalog hierarchy: catalogs → schemas → tables → columns. "
                "Bronze catalogs and schemas are automatically hidden."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "level":   {"type": "string", "enum": ["catalogs", "schemas", "tables", "columns"]},
                    "catalog": {"type": "string", "description": "Required for schemas/tables/columns"},
                    "schema":  {"type": "string", "description": "Required for tables/columns"},
                    "table":   {"type": "string", "description": "Required for columns"},
                },
                "required": ["level"],
            },
        ),
        types.Tool(
            name="read_table",
            description=(
                f"Read rows from a Databricks table. Max {MAX_ROWS} rows (Silver: {SILVER_MAX_ROWS} rows). "
                "Column names are required — SELECT * is blocked. Supports optional WHERE filtering."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "catalog": {"type": "string"},
                    "schema":  {"type": "string"},
                    "table":   {"type": "string"},
                    "columns": {
                        "type": "string",
                        "description": "Required: comma-separated column names (e.g. 'id, name, amount'). '*' is blocked.",
                    },
                    "where": {"type": "string", "description": "Optional WHERE clause (omit the WHERE keyword)"},
                },
                "required": ["catalog", "schema", "table", "columns"],
            },
        ),
        types.Tool(
            name="trigger_job",
            description="Trigger a Databricks job by job ID. Only whitelisted job IDs are allowed.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id":          {"type": "integer", "description": "Databricks job ID"},
                    "notebook_params": {"type": "object",  "description": "Key-value params for notebook jobs"},
                    "python_params":   {"type": "array",   "items": {"type": "string"}},
                },
                "required": ["job_id"],
            },
        ),
    ]


# ── Entry point ───────────────────────────────────────────────────────────────
async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


def run():
    asyncio.run(main())


if __name__ == "__main__":
    run()
