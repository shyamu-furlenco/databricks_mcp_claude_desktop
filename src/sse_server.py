"""
Remote SSE transport layer — wrap the stdio MCP server for HTTP hosting.

Run from the databricks-mcp/ directory:
  uvicorn src.sse_server:app --host 0.0.0.0 --port 8000

Each user passes their Databricks PAT in one of two ways:
  1. URL query param (Claude.ai connector): .../sse?token=dapi...
  2. Authorization header (programmatic):   Authorization: Bearer dapi...

The server stores no token — each connection authenticates with the caller's PAT.
"""

from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.requests import Request
from starlette.responses import JSONResponse
import uvicorn

import logging
log = logging.getLogger("databricks-mcp")

from .server import app as mcp_app, _token_var

# ── SSE transport ──────────────────────────────────────────────────────────────
sse = SseServerTransport("/messages/")


async def handle_sse(request: Request):
    # Accept token from query param (Claude.ai UI) or Authorization header (programmatic)
    token = request.query_params.get("token", "")
    if not token:
        auth = request.headers.get("Authorization", "")
        token = auth.removeprefix("Bearer ").strip()
    if not token:
        log.warning("SSE connection rejected: no token provided")
        return JSONResponse(
            {"error": "Unauthorized: pass your Databricks PAT as ?token=<your-pat> in the URL"},
            status_code=401,
        )
    log.info(f"SSE connection accepted, token prefix={token[:8]}...")
    # Bind token to this connection's async context — propagates into all tool calls
    _token_var.set(token)
    async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
        await mcp_app.run(streams[0], streams[1], mcp_app.create_initialization_options())
    log.info("SSE connection closed")


# ── Health check ───────────────────────────────────────────────────────────────
async def health(request: Request):
    return JSONResponse({"status": "ok", "server": "databricks-mcp"})


# ── Starlette app ──────────────────────────────────────────────────────────────
app = Starlette(
    routes=[
        Route("/health", health),
        Route("/sse",    handle_sse),
        Mount("/messages/", app=sse.handle_post_message),
    ],
)

if __name__ == "__main__":
    uvicorn.run("src.sse_server:app", host="0.0.0.0", port=8000, reload=False)
