# Databricks MCP Server

A production-ready MCP server for Databricks with guardrails, compatible with Claude.ai connectors.

## Tools exposed

| Tool | Description |
|---|---|
| `run_sql` | Execute read-only SELECT queries |
| `browse_catalog` | Navigate catalogs → schemas → tables → columns |
| `read_table` | Read rows with optional column/filter selection |
| `trigger_job` | Trigger whitelisted Databricks jobs |

## Guardrails

| Guardrail | Config key | Default |
|---|---|---|
| Read-only enforcement | — (always on) | Blocks INSERT/UPDATE/DELETE/DROP etc. |
| Catalog whitelist | `allowed_catalogs` | `[]` = all allowed |
| Schema whitelist | `allowed_schemas` | `[]` = all allowed |
| Row limit | `max_rows` | 1000 |
| Result size cap | `max_result_bytes` | 2 MB |
| Query timeout | `query_timeout_sec` | 30s |
| Job whitelist | `allowed_job_ids` | `[]` = all allowed |

---

## Setup

### 1. Install dependencies

```bash
pip install -e .
# or
pip install mcp databricks-sql-connector databricks-sdk
```

### 2. Configure

Edit `config/settings.json`:

```json
{
  "databricks": {
    "host": "your-workspace.azuredatabricks.net",
    "token": "YOUR_PAT_TOKEN",
    "http_path": "/sql/1.0/warehouses/YOUR_WAREHOUSE_ID"
  },
  "guardrails": {
    "allowed_catalogs": ["prod_catalog"],
    "allowed_schemas":  ["analytics", "reporting"],
    "allowed_job_ids":  [12345, 67890],
    "max_rows": 500,
    "query_timeout_sec": 20,
    "max_result_bytes": 1000000
  }
}
```

Or use environment variables:
```bash
export DATABRICKS_HOST=your-workspace.azuredatabricks.net
export DATABRICKS_TOKEN=dapi...
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abc123
```

---

## Running

### Option A: Local (stdio) — for Claude Desktop / Claude Code

Add to `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "databricks": {
      "command": "python3.11",
      "args": ["/path/to/databricks-mcp/src/server.py"],
      "cwd": "/path/to/databricks-mcp",
      "env": {
        "DATABRICKS_TOKEN": "dapi..."
      }
    }
  }
}
```

> **Security:** Pass the PAT token via `env` in the MCP config, not by hardcoding it in `config/settings.json`.

### Option B: Remote HTTP (SSE) — for Claude.ai connector

```bash
# Install extra deps
pip install starlette uvicorn

# Set optional bearer token for auth
export MCP_BEARER_TOKEN=your-secret-token

# Start server
uvicorn src.sse_server:app --host 0.0.0.0 --port 8000
```

Then in Claude.ai → Connectors → Add custom connector:
- **URL**: `https://your-host:8000/sse`
- **Auth**: Bearer token (if MCP_BEARER_TOKEN is set)

---

## Security checklist

- [ ] Use a scoped PAT (read-only SQL warehouse access only)
- [ ] Set `allowed_catalogs` and `allowed_schemas` to your prod namespaces
- [ ] Set `allowed_job_ids` to only the jobs Claude should trigger
- [ ] Keep `max_rows` ≤ 1000 to prevent large data exfil
- [ ] Run behind a VPN or set `MCP_BEARER_TOKEN` if hosting remotely
- [ ] Rotate the PAT every 90 days

---

## Project structure

```
databricks-mcp/
├── src/
│   ├── server.py       # Core MCP server (stdio)
│   └── sse_server.py   # SSE wrapper for remote hosting
├── config/
│   └── settings.json   # Guardrail config
├── pyproject.toml
└── README.md
```
