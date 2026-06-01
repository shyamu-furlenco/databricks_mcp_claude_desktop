"""
Remote SSE transport layer — wrap the stdio MCP server for HTTP hosting.

Run from the databricks-mcp/ directory:
  uvicorn src.sse_server:app --host 0.0.0.0 --port 8000

Each user passes their Databricks PAT in the connector URL:
  https://<host>/sse?token=dapi<personal-access-token>
"""

import logging
from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import Receive, Scope, Send
import uvicorn

from .server import app as mcp_app, _token_var

log = logging.getLogger("databricks-mcp")

sse = SseServerTransport("/messages/")


async def _handle_sse(scope: Scope, receive: Receive, send: Send) -> None:
    """Pure ASGI handler — writes directly to send, never returns a Response object."""
    request = Request(scope, receive, send)
    token = request.query_params.get("token", "")
    if not token:
        auth = request.headers.get("Authorization", "")
        token = auth.removeprefix("Bearer ").strip()
    if not token:
        log.warning("SSE rejected: no token provided")
        await JSONResponse(
            {"error": "Unauthorized: pass your Databricks PAT as ?token=<pat> in the URL"},
            status_code=401,
        )(scope, receive, send)
        return
    log.info(f"SSE accepted, token={token[:8]}...")
    _token_var.set(token)
    async with sse.connect_sse(scope, receive, send) as streams:
        await mcp_app.run(streams[0], streams[1], mcp_app.create_initialization_options())
    log.info("SSE connection closed")


async def health(request: Request):
    return JSONResponse({"status": "ok", "server": "databricks-mcp"})


# Starlette handles health + messages + lifespan; /sse is intercepted before it
_inner = Starlette(
    routes=[
        Route("/health", health),
        Mount("/messages/", app=sse.handle_post_message),
    ],
)


class _Router:
    """Intercept /sse before Starlette so it never hits Route's response-call requirement."""

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and scope.get("path") == "/sse":
            await _handle_sse(scope, receive, send)
        else:
            await _inner(scope, receive, send)


app = _Router()

if __name__ == "__main__":
    uvicorn.run("src.sse_server:app", host="0.0.0.0", port=8000, reload=False)
