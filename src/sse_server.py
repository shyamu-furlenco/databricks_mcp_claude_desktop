"""
Remote SSE transport layer — wrap the stdio MCP server for HTTP hosting.

Run from the databricks-mcp/ directory:
  uvicorn src.sse_server:app --host 0.0.0.0 --port 8000

Add to Claude.ai connector URL:  https://your-host:8000/sse
"""

from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
import uvicorn

from .server import app as mcp_app   # reuse exact same MCP server

# ── Optional: Bearer token auth middleware ─────────────────────────────────
import os
BEARER_TOKEN = os.getenv("MCP_BEARER_TOKEN", "")

class BearerAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if BEARER_TOKEN:
            auth = request.headers.get("Authorization", "")
            if auth != f"Bearer {BEARER_TOKEN}":
                return JSONResponse({"error": "Unauthorized"}, status_code=401)
        return await call_next(request)

# ── SSE transport ──────────────────────────────────────────────────────────
sse = SseServerTransport("/messages/")

async def handle_sse(request: Request):
    async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
        await mcp_app.run(streams[0], streams[1], mcp_app.create_initialization_options())

async def handle_messages(request: Request):
    await sse.handle_post_message(request.scope, request.receive, request._send)

# ── Health check ──────────────────────────────────────────────────────────
async def health(request: Request):
    return JSONResponse({"status": "ok", "server": "databricks-mcp"})

# ── Starlette app ─────────────────────────────────────────────────────────
app = Starlette(
    routes=[
        Route("/health", health),
        Route("/sse",    handle_sse),
        Mount("/messages/", routes=[Route("/{path:path}", handle_messages, methods=["POST"])]),
    ],
    middleware=[Middleware(BearerAuthMiddleware)] if BEARER_TOKEN else [],
)

if __name__ == "__main__":
    uvicorn.run("src.sse_server:app", host="0.0.0.0", port=8000, reload=False)
