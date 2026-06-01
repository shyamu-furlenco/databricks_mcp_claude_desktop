"""
Remote MCP transport layer with OAuth 2.0 + PKCE for Claude.ai connector auth.

Supports both transports:
  - New (Streamable HTTP): Claude.ai sends POST /sse with message in body,
    receives SSE stream back. Session persists via Mcp-Session-Id header.
  - Legacy (SSE): GET /sse establishes long-lived stream, POST /messages/ for messages.

OAuth flow (required by Claude.ai for remote connectors):
  1. Claude.ai discovers OAuth metadata at /.well-known/oauth-*
  2. Registers as a client   POST /oauth/register
  3. Opens /oauth/authorize  → user enters Databricks PAT in HTML form
  4. Server redirects with auth code
  5. Claude.ai exchanges code → POST /oauth/token → gets PAT as Bearer token
  6. All MCP calls carry    Authorization: Bearer <dapi-pat>
"""

import asyncio
import base64
import hashlib
import logging
import secrets
import time
from html import escape
from urllib.parse import urlencode

from mcp.server.sse import SseServerTransport
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.types import Receive, Scope, Send
import uvicorn

from .server import app as mcp_app, _token_var

log = logging.getLogger("databricks-mcp")

# ── Transport setup ────────────────────────────────────────────────────────────

sse = SseServerTransport("/messages/")  # legacy transport

try:
    from mcp.server.streamable_http import StreamableHTTPServerTransport
    _HAS_STREAMABLE = True
    log.info("StreamableHTTPServerTransport available — new transport enabled")
except ImportError:
    _HAS_STREAMABLE = False
    log.warning("mcp.server.streamable_http not available — only legacy SSE supported")

# Session store for new Streamable HTTP transport: {session_id: transport}
_http_sessions: dict = {}

# ── OAuth helpers ──────────────────────────────────────────────────────────────

# In-memory auth-code store: {code: {token, code_challenge, expires}}
_auth_codes: dict = {}


def _base_url(scope: Scope) -> str:
    headers = dict(scope.get("headers", []))
    proto = headers.get(b"x-forwarded-proto", b"").decode() or "https"
    host  = headers.get(b"host", b"localhost").decode()
    return f"{proto}://{host}"


def _extract_token(scope: Scope) -> str:
    headers = dict(scope.get("headers", []))
    auth = headers.get(b"authorization", b"").decode()
    token = auth.removeprefix("Bearer ").strip()
    if not token:
        qs = scope.get("query_string", b"").decode()
        for part in qs.split("&"):
            if part.startswith("token="):
                token = part[6:]
                break
    return token


# ── OAuth discovery ────────────────────────────────────────────────────────────

async def _well_known_resource(scope: Scope):
    base = _base_url(scope)
    log.info(f"OAuth resource metadata, base={base}")
    return JSONResponse({"resource": base, "authorization_servers": [base]})


async def _well_known_auth_server(scope: Scope):
    base = _base_url(scope)
    log.info(f"OAuth server metadata, base={base}")
    return JSONResponse({
        "issuer": base,
        "authorization_endpoint": f"{base}/oauth/authorize",
        "token_endpoint": f"{base}/oauth/token",
        "registration_endpoint": f"{base}/oauth/register",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none"],
        "scopes_supported": ["mcp"],
    })


# ── OAuth endpoints ────────────────────────────────────────────────────────────

async def _oauth_register(request: Request):
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    client_id = secrets.token_hex(8)
    log.info(f"OAuth client registered: {client_id}")
    return JSONResponse({
        "client_id": client_id,
        "redirect_uris": body.get("redirect_uris", []),
        "grant_types": ["authorization_code"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
    }, status_code=201)


async def _oauth_authorize(request: Request):
    if request.method == "GET":
        p = dict(request.query_params)
        redirect_uri   = p.get("redirect_uri", "")
        state          = p.get("state", "")
        code_challenge = p.get("code_challenge", "")
        log.info(f"OAuth authorize GET, redirect={redirect_uri[:60]}")
        html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>Databricks MCP – Authorise</title>
<style>
  body{{font-family:system-ui,sans-serif;max-width:480px;margin:80px auto;padding:0 20px}}
  h2{{margin-bottom:6px}}p{{color:#555;margin-bottom:20px}}
  input[type=text]{{width:100%;padding:10px;font-size:14px;border:1px solid #ccc;
    border-radius:6px;box-sizing:border-box}}
  button{{margin-top:12px;padding:10px 24px;background:#1a56db;color:#fff;border:none;
    border-radius:6px;cursor:pointer;font-size:14px}}
  button:hover{{background:#1344b5}}
  small{{display:block;margin-top:10px;color:#888}}
</style></head><body>
<h2>Connect Claude.ai to Databricks</h2>
<p>Enter your Databricks Personal Access Token (PAT) to authorise this connection.</p>
<form method="POST">
  <input type="hidden" name="redirect_uri"   value="{escape(redirect_uri)}">
  <input type="hidden" name="state"          value="{escape(state)}">
  <input type="hidden" name="code_challenge" value="{escape(code_challenge)}">
  <input type="text" name="pat" placeholder="dapi..." autocomplete="off" required>
  <button type="submit">Authorise</button>
  <small>Databricks workspace → User Settings → Developer → Access Tokens → Generate new token</small>
</form></body></html>"""
        return HTMLResponse(html)

    form           = await request.form()
    pat            = form.get("pat", "").strip()
    redirect_uri   = form.get("redirect_uri", "")
    state          = form.get("state", "")
    code_challenge = form.get("code_challenge", "")
    log.info(f"OAuth authorize POST, pat_prefix={pat[:8] if pat else 'EMPTY'}")

    if not pat:
        return HTMLResponse("<p>PAT is required.</p>", status_code=400)

    code = secrets.token_urlsafe(24)
    _auth_codes[code] = {"token": pat, "code_challenge": code_challenge, "expires": time.time() + 300}
    log.info(f"Auth code issued, redirecting to {redirect_uri[:60]}")
    return RedirectResponse(f"{redirect_uri}?{urlencode({'code': code, 'state': state})}", status_code=302)


async def _oauth_token(request: Request):
    code = code_verifier = ""
    try:
        form = await request.form()
        code, code_verifier = form.get("code", ""), form.get("code_verifier", "")
    except Exception:
        try:
            body = await request.json()
            code, code_verifier = body.get("code", ""), body.get("code_verifier", "")
        except Exception:
            return JSONResponse({"error": "invalid_request"}, status_code=400)

    log.info(f"OAuth token exchange, code={code[:8] if code else 'EMPTY'}")
    entry = _auth_codes.pop(code, None)
    if not entry or time.time() > entry["expires"]:
        return JSONResponse({"error": "invalid_grant"}, status_code=400)

    if entry["code_challenge"]:
        expected = base64.urlsafe_b64encode(
            hashlib.sha256(code_verifier.encode()).digest()
        ).rstrip(b"=").decode()
        if expected != entry["code_challenge"]:
            log.warning("PKCE verification failed")
            return JSONResponse({"error": "invalid_grant"}, status_code=400)

    log.info("OAuth token issued successfully")
    return JSONResponse({"access_token": entry["token"], "token_type": "Bearer", "expires_in": 86400 * 30})


# ── MCP endpoint ───────────────────────────────────────────────────────────────

async def _handle_mcp(scope: Scope, receive: Receive, send: Send) -> None:
    token = _extract_token(scope)
    if not token:
        log.warning("MCP request rejected: no token")
        await JSONResponse({"error": "Unauthorized"}, status_code=401)(scope, receive, send)
        return

    method = scope.get("method", "GET").upper()
    log.info(f"MCP {method} /sse token={token[:8]}...")
    _token_var.set(token)

    if method == "POST" and _HAS_STREAMABLE:
        # New Streamable HTTP transport — message is in the POST body
        request = Request(scope, receive, send)
        session_id = request.headers.get("mcp-session-id")

        if session_id and session_id in _http_sessions:
            transport = _http_sessions[session_id]
            log.info(f"Reusing session {session_id[:8]}...")
        else:
            transport = StreamableHTTPServerTransport(
                mcp_session_id=secrets.token_hex(16),
                is_json_response_enabled=False,
            )
            new_sid = transport.mcp_session_id
            _http_sessions[new_sid] = transport
            log.info(f"New session {new_sid[:8]}...")

            async def _run_server():
                try:
                    async with transport.connect() as (read, write):
                        await mcp_app.run(read, write, mcp_app.create_initialization_options())
                except Exception as e:
                    log.error(f"MCP session error: {e}", exc_info=True)
                finally:
                    _http_sessions.pop(new_sid, None)
                    log.info(f"Session {new_sid[:8]} ended")

            asyncio.create_task(_run_server())

        await transport.handle_request(scope, receive, send)

    else:
        # Legacy GET SSE transport
        async with sse.connect_sse(scope, receive, send) as streams:
            await mcp_app.run(streams[0], streams[1], mcp_app.create_initialization_options())
        log.info("Legacy SSE connection closed")


# ── Health ─────────────────────────────────────────────────────────────────────

async def _health(scope: Scope, receive: Receive, send: Send) -> None:
    await JSONResponse({"status": "ok", "server": "databricks-mcp",
                        "streamable_http": _HAS_STREAMABLE})(scope, receive, send)


# ── ASGI router ────────────────────────────────────────────────────────────────

async def app(scope: Scope, receive: Receive, send: Send) -> None:
    if scope["type"] == "lifespan":
        while True:
            msg = await receive()
            if msg["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif msg["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return
        return

    if scope["type"] != "http":
        return

    path = scope.get("path", "")

    if path == "/health":
        await _health(scope, receive, send)
    elif path == "/.well-known/oauth-protected-resource":
        await (await _well_known_resource(scope))(scope, receive, send)
    elif path == "/.well-known/oauth-authorization-server":
        await (await _well_known_auth_server(scope))(scope, receive, send)
    elif path == "/oauth/register":
        req = Request(scope, receive, send)
        await (await _oauth_register(req))(scope, receive, send)
    elif path == "/oauth/authorize":
        req = Request(scope, receive, send)
        await (await _oauth_authorize(req))(scope, receive, send)
    elif path == "/oauth/token":
        req = Request(scope, receive, send)
        await (await _oauth_token(req))(scope, receive, send)
    elif path == "/sse":
        await _handle_mcp(scope, receive, send)
    elif path.startswith("/messages/"):
        await sse.handle_post_message(scope, receive, send)
    else:
        await JSONResponse({"error": "not found"}, status_code=404)(scope, receive, send)


if __name__ == "__main__":
    uvicorn.run("src.sse_server:app", host="0.0.0.0", port=8000, reload=False)
