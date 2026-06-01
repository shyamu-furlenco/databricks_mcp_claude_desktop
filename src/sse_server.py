"""
Remote SSE transport layer with OAuth 2.0 + PKCE for Claude.ai connector auth.

Claude.ai requires OAuth for remote MCP servers. We implement a minimal
OAuth server where the "authorization" step is the user entering their
Databricks PAT in an HTML form. The PAT becomes the Bearer access token.

Flow:
  1. Claude.ai discovers OAuth metadata at /.well-known/oauth-*
  2. Claude.ai registers as a client  (POST /oauth/register)
  3. Claude.ai opens /oauth/authorize → user enters their Databricks PAT
  4. Server stores PAT behind a short-lived auth code, redirects back
  5. Claude.ai exchanges code for access token  (POST /oauth/token)
  6. Claude.ai sends Authorization: Bearer <dapi-pat> on every MCP call
"""

import base64
import hashlib
import os
import secrets
import time
import logging
from html import escape
from urllib.parse import urlencode

from mcp.server.sse import SseServerTransport
from starlette.requests import Request
from starlette.responses import JSONResponse, HTMLResponse, RedirectResponse
from starlette.types import Receive, Scope, Send
import uvicorn

from .server import app as mcp_app, _token_var

log = logging.getLogger("databricks-mcp")

sse = SseServerTransport("/messages/")

# In-memory auth-code store: {code: {token, code_challenge, expires}}
_auth_codes: dict = {}


def _base_url(scope: Scope) -> str:
    """Return the public-facing base URL, respecting Render's TLS proxy headers."""
    headers = dict(scope.get("headers", []))
    # Render terminates TLS and forwards X-Forwarded-Proto: https
    proto = headers.get(b"x-forwarded-proto", b"").decode() or "https"
    host  = headers.get(b"host", b"localhost").decode()
    return f"{proto}://{host}"


# ── OAuth discovery ────────────────────────────────────────────────────────────

async def _well_known_resource(scope: Scope):
    base = _base_url(scope)
    log.info(f"OAuth resource metadata requested, base={base}")
    return JSONResponse({
        "resource": base,
        "authorization_servers": [base],
    })

async def _well_known_auth_server(scope: Scope):
    base = _base_url(scope)
    log.info(f"OAuth server metadata requested, base={base}")
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
    """Dynamic client registration — accept any client, return a client_id."""
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
    """Show a PAT entry form (GET) or process it and redirect with auth code (POST)."""
    if request.method == "GET":
        params         = dict(request.query_params)
        redirect_uri   = params.get("redirect_uri", "")
        state          = params.get("state", "")
        code_challenge = params.get("code_challenge", "")
        log.info(f"OAuth authorize GET redirect_uri={redirect_uri[:60]}")

        # HTML-escape all values embedded in the form
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Databricks MCP – Connect</title>
  <style>
    body {{ font-family: system-ui, sans-serif; max-width: 480px; margin: 80px auto; padding: 0 20px; }}
    h2   {{ margin-bottom: 6px; }}
    p    {{ color: #555; margin-bottom: 20px; }}
    input[type=text] {{
      width: 100%; padding: 10px; font-size: 14px;
      border: 1px solid #ccc; border-radius: 6px; box-sizing: border-box;
    }}
    button {{
      margin-top: 12px; padding: 10px 24px; background: #1a56db;
      color: white; border: none; border-radius: 6px; cursor: pointer; font-size: 14px;
    }}
    button:hover {{ background: #1344b5; }}
    small {{ display: block; margin-top: 10px; color: #888; }}
  </style>
</head>
<body>
  <h2>Connect Claude.ai to Databricks</h2>
  <p>Enter your Databricks Personal Access Token (PAT) to authorise this connection.</p>
  <form method="POST">
    <input type="hidden" name="redirect_uri"   value="{escape(redirect_uri)}">
    <input type="hidden" name="state"          value="{escape(state)}">
    <input type="hidden" name="code_challenge" value="{escape(code_challenge)}">
    <input type="text" name="pat" placeholder="dapi..." autocomplete="off" required>
    <button type="submit">Authorise</button>
    <small>Generate a PAT: Databricks workspace → User Settings → Developer → Access Tokens</small>
  </form>
</body>
</html>"""
        return HTMLResponse(html)

    # POST — user submitted the form
    form           = await request.form()
    pat            = form.get("pat", "").strip()
    redirect_uri   = form.get("redirect_uri", "")
    state          = form.get("state", "")
    code_challenge = form.get("code_challenge", "")
    log.info(f"OAuth authorize POST pat_prefix={pat[:8] if pat else 'EMPTY'}")

    if not pat:
        return HTMLResponse("<p>PAT is required.</p>", status_code=400)

    code = secrets.token_urlsafe(24)
    _auth_codes[code] = {
        "token":          pat,
        "code_challenge": code_challenge,
        "expires":        time.time() + 300,
    }
    qs = urlencode({"code": code, "state": state})
    log.info(f"OAuth code issued, redirecting to {redirect_uri[:60]}")
    return RedirectResponse(f"{redirect_uri}?{qs}", status_code=302)


async def _oauth_token(request: Request):
    """Exchange auth code for access token (the Databricks PAT)."""
    code = code_verifier = ""
    try:
        form          = await request.form()
        code          = form.get("code", "")
        code_verifier = form.get("code_verifier", "")
    except Exception:
        try:
            body          = await request.json()
            code          = body.get("code", "")
            code_verifier = body.get("code_verifier", "")
        except Exception:
            return JSONResponse({"error": "invalid_request"}, status_code=400)

    log.info(f"OAuth token exchange for code={code[:8] if code else 'EMPTY'}")

    entry = _auth_codes.pop(code, None)
    if not entry or time.time() > entry["expires"]:
        return JSONResponse({"error": "invalid_grant"}, status_code=400)

    # Verify PKCE S256
    if entry["code_challenge"]:
        expected = (
            base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest())
            .rstrip(b"=")
            .decode()
        )
        if expected != entry["code_challenge"]:
            return JSONResponse({"error": "invalid_grant"}, status_code=400)

    log.info("OAuth token issued")
    return JSONResponse({
        "access_token": entry["token"],
        "token_type":   "Bearer",
        "expires_in":   60 * 60 * 24 * 30,
    })


# ── SSE endpoint ───────────────────────────────────────────────────────────────

async def _handle_sse(scope: Scope, receive: Receive, send: Send) -> None:
    """Pure ASGI SSE handler — token arrives as Authorization: Bearer <pat>."""
    request = Request(scope, receive, send)
    # OAuth access token arrives as Bearer header
    auth  = request.headers.get("Authorization", "")
    token = auth.removeprefix("Bearer ").strip()
    # Also accept ?token= for direct curl testing
    if not token:
        token = request.query_params.get("token", "")

    if not token:
        log.warning("SSE rejected: no token")
        await JSONResponse(
            {"error": "Unauthorized: complete OAuth flow or pass ?token=<pat>"},
            status_code=401,
        )(scope, receive, send)
        return

    log.info(f"SSE accepted, token={token[:8]}...")
    _token_var.set(token)
    async with sse.connect_sse(scope, receive, send) as streams:
        await mcp_app.run(streams[0], streams[1], mcp_app.create_initialization_options())
    log.info("SSE connection closed")


# ── Health ─────────────────────────────────────────────────────────────────────

async def _health(scope: Scope, receive: Receive, send: Send) -> None:
    await JSONResponse({"status": "ok", "server": "databricks-mcp"})(scope, receive, send)


# ── Router ─────────────────────────────────────────────────────────────────────

async def _dispatch(scope: Scope, receive: Receive, send: Send) -> None:
    if scope["type"] not in ("http", "lifespan"):
        return

    if scope["type"] == "lifespan":
        while True:
            msg = await receive()
            if msg["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif msg["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return

    path   = scope.get("path", "")
    method = scope.get("method", "GET")

    # Route table
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
        resp = await _oauth_authorize(req)
        await resp(scope, receive, send)
    elif path == "/oauth/token":
        req = Request(scope, receive, send)
        await (await _oauth_token(req))(scope, receive, send)
    elif path == "/sse":
        await _handle_sse(scope, receive, send)
    elif path.startswith("/messages/"):
        await sse.handle_post_message(scope, receive, send)
    else:
        await JSONResponse({"error": "not found"}, status_code=404)(scope, receive, send)


app = _dispatch

if __name__ == "__main__":
    uvicorn.run("src.sse_server:app", host="0.0.0.0", port=8000, reload=False)
