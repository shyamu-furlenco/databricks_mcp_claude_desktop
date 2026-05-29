# Databricks MCP Server

A Python MCP (Model Context Protocol) server that connects **Claude Desktop** (or any MCP-compatible client) to your **Databricks SQL warehouse**. Ask questions in plain English — Claude writes the SQL, runs it, and explains the results.

---

## Table of Contents

1. [What it does](#what-it-does)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Get your Databricks credentials](#get-your-databricks-credentials)
5. [Configure the server](#configure-the-server)
6. [Connect to Claude Desktop (local / stdio)](#connect-to-claude-desktop-local--stdio)
7. [Connect to Claude.ai (remote / SSE)](#connect-to-claudeai-remote--sse)
8. [Available tools](#available-tools)
9. [Guardrails reference](#guardrails-reference)
10. [Customising guardrails](#customising-guardrails)
11. [Troubleshooting](#troubleshooting)

---

## What it does

Claude can use four tools through this server:

| Tool | What Claude can do |
|---|---|
| `run_sql` | Run any read-only SELECT query |
| `browse_catalog` | Explore catalogs → schemas → tables → columns |
| `read_table` | Read rows from a specific table with filters |
| `trigger_job` | Trigger a whitelisted Databricks job |

All queries are enforced server-side: read-only, no Bronze tables, no `SELECT *`, automatic row limits, and a 30-second timeout.

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.11 or higher |
| Claude Desktop | Latest (for local stdio mode) |
| Databricks workspace | Any plan with a SQL warehouse |

---

## Installation

```bash
# 1. Clone or download the project
cd /path/to/databricks-mcp

# 2. Install the package and dependencies
pip install -e .
```

This installs:
- `mcp` — the MCP protocol library
- `databricks-sql-connector` — SQL warehouse connector
- `databricks-sdk` — Databricks API client (for job triggers)

To use the remote HTTP/SSE mode, also install:
```bash
pip install starlette uvicorn
```

---

## Get your Databricks credentials

You need three values from your Databricks workspace.

### 1. Workspace host

This is the domain of your Databricks workspace, without `https://`.

Example: `dbc-abc12345-6789.cloud.databricks.com`

Find it: open your workspace in a browser → copy the hostname from the URL bar.

### 2. Personal Access Token (PAT)

1. In Databricks, click your username (top right) → **Settings**
2. Go to **Developer** → **Access tokens**
3. Click **Generate new token**
4. Give it a name (e.g. `claude-mcp`) and a reasonable expiry
5. Copy the token — it starts with `dapi...`

> **Keep this token secret.** Store it only in the Claude Desktop `env` block, never in `config/settings.json`.

### 3. SQL Warehouse HTTP path

1. In Databricks, go to **SQL Warehouses** (left sidebar)
2. Click your warehouse → **Connection details** tab
3. Copy the **HTTP path** — it looks like `/sql/1.0/warehouses/abc123456789`

---

## Configure the server

Open `config/settings.json` and fill in your host and HTTP path:

```json
{
  "databricks": {
    "host": "your-workspace.cloud.databricks.com",
    "token": "",
    "http_path": "/sql/1.0/warehouses/your_warehouse_id"
  },
  "guardrails": {
    "allowed_catalogs": [],
    "allowed_schemas": [],
    "allowed_job_ids": [],
    "max_rows": 1000,
    "silver_max_rows": 10000,
    "query_timeout_sec": 30,
    "max_result_bytes": 2000000,
    "max_concurrent_queries": 3,
    "result_cache_ttl_sec": 3600,
    "block_bronze": true,
    "block_select_star": true
  }
}
```

> **Leave `token` blank.** The token is passed via an environment variable in the next step — never store it in this file.

---

## Connect to Claude Desktop (local / stdio)

This is the standard setup for using the MCP on your local machine.

### Step 1 — Open the Claude Desktop config file

```
~/Library/Application Support/Claude/claude_desktop_config.json
```

Open it in any text editor. If it does not exist yet, create it.

### Step 2 — Add the MCP server entry

Add a `databricks` entry under `mcpServers`. Replace the path and token with your own values:

```json
{
  "mcpServers": {
    "databricks": {
      "command": "python3.11",
      "args": ["/full/path/to/databricks-mcp/src/server.py"],
      "cwd": "/full/path/to/databricks-mcp",
      "env": {
        "DATABRICKS_TOKEN": "dapi_your_token_here"
      }
    }
  }
}
```

> Replace `/full/path/to/databricks-mcp` with the actual path where you downloaded this project.  
> Replace `dapi_your_token_here` with your Personal Access Token.

### Step 3 — Restart Claude Desktop

Fully quit and reopen Claude Desktop (Cmd+Q, then relaunch). The Databricks tools will appear in the tool panel.

### Step 4 — Test it

Ask Claude: *"Show me the available catalogs in Databricks"*

Claude will call `browse_catalog` and list your catalogs. If it works, you are set up.

---

## Connect to Claude.ai (remote / SSE)

Use this to connect claude.ai to a Databricks instance running on a remote server.

### Step 1 — Run the SSE server

On your remote server:

```bash
export DATABRICKS_TOKEN="dapi_your_token_here"

# Optional: protect the endpoint with a bearer token
export MCP_BEARER_TOKEN="a_secret_you_choose"

uvicorn src.sse_server:app --host 0.0.0.0 --port 8000
```

Verify it is running:

```bash
curl http://your-server:8000/health
# Expected: {"status": "ok", "server": "databricks-mcp"}
```

### Step 2 — Add the connector in Claude.ai

1. Go to **claude.ai** → your profile → **Settings** → **Integrations**
2. Add a new MCP connector
3. Enter the SSE URL: `https://your-server:8000/sse`
4. If you set `MCP_BEARER_TOKEN`, enter it as the bearer token

> Run behind HTTPS (nginx or a cloud load balancer as TLS terminator) before exposing to the internet.

---

## Available tools

### `run_sql`

Run a read-only SELECT query.

```
Ask: "What are the top 10 accounts by MRR this month?"
```

Claude writes and runs SQL like:
```sql
SELECT account_id, mrr_usd
FROM prod.gold_revenue.gold_mrr_monthly
WHERE month_start = '2024-01-01'
ORDER BY mrr_usd DESC
LIMIT 10
```

Rules enforced automatically: no write operations, no `SELECT *`, no Bronze tables, LIMIT auto-added if missing.

---

### `browse_catalog`

Explore the Unity Catalog hierarchy.

```
Ask: "What tables are available in the gold_revenue schema?"
```

Claude calls `browse_catalog` step by step through: `catalogs` → `schemas` → `tables` → `columns`. Bronze catalogs and schemas are automatically hidden.

---

### `read_table`

Read rows from a specific table with optional filtering.

```
Ask: "Show me churned accounts from gold_mrr_monthly for January 2024"
```

The `columns` parameter is required — `*` is blocked. The `where` parameter is optional (no `WHERE` keyword needed).

---

### `trigger_job`

Trigger a Databricks job by ID.

```
Ask: "Run the daily revenue refresh job"
```

Only job IDs listed in `allowed_job_ids` can be triggered. Leave it empty (`[]`) to block all job triggers.

---

## Guardrails reference

All guardrails are enforced by the MCP server — Claude cannot bypass them.

| Guardrail | Setting | Default | Description |
|---|---|---|---|
| Block Bronze | `block_bronze` | `true` | Rejects queries referencing `bronze_*` catalogs or schemas |
| Block SELECT * | `block_select_star` | `true` | Requires explicit column names |
| Read-only | always on | — | Blocks INSERT, UPDATE, DELETE, DROP, TRUNCATE, CREATE, ALTER, MERGE, REPLACE, GRANT, REVOKE, COPY |
| Gold row limit | `max_rows` | 1000 | LIMIT auto-appended for non-Silver queries |
| Silver row limit | `silver_max_rows` | 10000 | LIMIT auto-appended for `silver_*` queries |
| Query timeout | `query_timeout_sec` | 30s | Hard timeout per query |
| Result size cap | `max_result_bytes` | 2000000 | Result truncated if it exceeds 2 MB |
| Max concurrency | `max_concurrent_queries` | 3 | Max simultaneous SQL warehouse calls |
| Result cache | `result_cache_ttl_sec` | 3600s | Identical queries served from memory for 1 hour |
| Catalog whitelist | `allowed_catalogs` | `[]` (all) | If non-empty, only listed catalogs are accessible |
| Schema whitelist | `allowed_schemas` | `[]` (all) | If non-empty, only listed schemas are accessible |
| Job whitelist | `allowed_job_ids` | `[]` (all) | If non-empty, only listed job IDs can be triggered |

---

## Customising guardrails

All thresholds live in `config/settings.json`. No code changes needed — edit and restart.

**Restrict to specific catalogs and schemas:**
```json
"allowed_catalogs": ["prod"],
"allowed_schemas": ["gold_revenue", "gold_product"]
```

**Raise the row limit:**
```json
"max_rows": 5000
```

**Allow specific jobs only:**
```json
"allowed_job_ids": [12345, 67890]
```

**Increase timeout for slow queries:**
```json
"query_timeout_sec": 60
```

---

## Security checklist

- [ ] PAT token is in the Claude Desktop `env` block, not in `config/settings.json`
- [ ] `allowed_catalogs` and `allowed_schemas` are set to your prod namespaces
- [ ] `allowed_job_ids` lists only the jobs Claude should be able to trigger
- [ ] Remote SSE mode runs behind HTTPS and has `MCP_BEARER_TOKEN` set
- [ ] PAT token is rotated every 90 days

---

## Troubleshooting

**Claude Desktop shows no Databricks tools**
- Verify the path in `claude_desktop_config.json` points to the actual `server.py` location
- Check `python3.11 --version` — Python 3.11+ is required
- Fully quit Claude Desktop (Cmd+Q) and reopen — closing the window is not a full restart

**Authentication error / connection refused**
- Check the `DATABRICKS_TOKEN` in the `env` block of `claude_desktop_config.json`
- Verify the token has not expired (Databricks → Settings → Access tokens)
- Confirm `host` in `settings.json` has no `https://` prefix
- Confirm `http_path` matches the SQL warehouse connection details exactly

**"Query exceeded 30s timeout"**
- Ask Claude to use a Gold materialized view instead of querying Silver directly
- Add a more specific WHERE clause to reduce data scanned
- Increase `query_timeout_sec` in `settings.json`

**"Bronze layer queries are not allowed"**
- The query referenced a `bronze_*` catalog or schema
- Ask the data team to create a Gold or Silver view for the data you need

**"SELECT * is not allowed"**
- Specify the columns you need: `SELECT col1, col2, col3 FROM ...`

**Server crashes on startup**

Run it manually to see the error:
```bash
cd /path/to/databricks-mcp
python3.11 src/server.py
```

Check that `config/settings.json` is valid JSON and contains the correct `host` and `http_path`.

---

## Project structure

```
databricks-mcp/
├── src/
│   ├── server.py       # Core MCP server (stdio) — all tools and guardrails
│   └── sse_server.py   # HTTP/SSE wrapper for remote Claude.ai connectors
├── config/
│   └── settings.json   # Guardrail config (edit this to tune thresholds)
├── pyproject.toml
└── README.md
```
