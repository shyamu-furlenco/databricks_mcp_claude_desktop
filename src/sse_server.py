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
import collections
import hashlib
import logging
import os
import secrets
import time
from html import escape
from urllib.parse import urlencode, urlparse

from mcp.server.sse import SseServerTransport
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.types import Receive, Scope, Send
import uvicorn

from .server import app as mcp_app, _token_var
from . import server as _server_module

log = logging.getLogger("databricks-mcp")

# ── Remote-mode safety: disable global token fallback ─────────────────────────
# In SSE/remote mode every user MUST supply their own Databricks PAT via OAuth.
# If DATABRICKS_TOKEN is set on the server it would silently share one identity
# across all users, bypassing per-user auth entirely.
if _server_module.DATABRICKS_TOKEN:
    log.critical(
        "DATABRICKS_TOKEN env var is set on this server — this shares ONE Databricks "
        "identity with ALL users and defeats per-user auth. "
        "Go to Render → Environment and DELETE the DATABRICKS_TOKEN variable, then redeploy."
    )
_server_module.DATABRICKS_TOKEN = ""  # forcibly clear; each user must authenticate via OAuth

# ── Transport setup ────────────────────────────────────────────────────────────

sse = SseServerTransport("/messages/")  # legacy transport

try:
    from mcp.server.streamable_http import StreamableHTTPServerTransport
    _HAS_STREAMABLE = True
    log.info("StreamableHTTPServerTransport available — new transport enabled")
except ImportError:
    _HAS_STREAMABLE = False
    log.warning("mcp.server.streamable_http not available — only legacy SSE supported")

# Session store for new Streamable HTTP transport
# {session_id: {"transport": ..., "ready": asyncio.Event}}
_http_sessions: dict = {}

# ── OAuth helpers ──────────────────────────────────────────────────────────────

# In-memory auth-code store: {code: {token, code_challenge, expires}}
_auth_codes: dict = {}

# Set SERVER_BASE_URL env var on Render to prevent Host-header injection poisoning
# the OAuth discovery metadata (/.well-known/oauth-authorization-server).
# e.g.  SERVER_BASE_URL=https://your-service.onrender.com
_SERVER_BASE_URL: str = os.getenv("SERVER_BASE_URL", "").rstrip("/")


def _base_url(scope: Scope) -> str:
    if _SERVER_BASE_URL:
        return _SERVER_BASE_URL
    # Fallback: derive from headers — only safe when Render strips the Host header
    headers = dict(scope.get("headers", []))
    proto = headers.get(b"x-forwarded-proto", b"").decode() or "https"
    host  = headers.get(b"host", b"localhost").decode()
    log.warning("SERVER_BASE_URL env var not set — Host header used for OAuth base URL (spoofable)")
    return f"{proto}://{host}"


def _extract_token(scope: Scope) -> str:
    headers = dict(scope.get("headers", []))
    auth = headers.get(b"authorization", b"").decode()
    return auth.removeprefix("Bearer ").strip()


# ── Rate limiting (per IP, in-memory) ─────────────────────────────────────────

_rl_buckets: dict[str, list[float]] = collections.defaultdict(list)
_RL_WINDOW_SEC = 60
_RL_MAX = 20


def _client_ip(scope: Scope) -> str:
    headers = dict(scope.get("headers", []))
    xff = headers.get(b"x-forwarded-for", b"").decode()
    if xff:
        # Use rightmost entry — appended by Render's trusted proxy, not spoofable by client
        return xff.split(",")[-1].strip()
    client = scope.get("client")
    return client[0] if client else "unknown"


def _rate_ok(ip: str, limit: int = _RL_MAX) -> bool:
    now = time.time()
    _rl_buckets[ip] = [t for t in _rl_buckets[ip] if now - t < _RL_WINDOW_SEC]
    if len(_rl_buckets[ip]) >= limit:
        return False
    _rl_buckets[ip].append(now)
    return True


# Allowed redirect_uri origins for OAuth (prevents redirect phishing)
_ALLOWED_REDIRECT_HOSTS = {"claude.ai"}


def _redirect_allowed(uri: str) -> bool:
    """Accept claude.ai (and subdomains) or localhost for dev."""
    p = urlparse(uri)
    host = p.netloc.split(":")[0]
    if p.scheme == "https" and (host == "claude.ai" or host.endswith(".claude.ai")):
        return True
    if host in ("localhost", "127.0.0.1"):
        return True
    return False


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
    if not _rate_ok(_client_ip(request.scope), limit=10):
        return JSONResponse({"error": "rate_limit_exceeded"}, status_code=429)
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


_SECURITY_HEADERS = {
    "X-Frame-Options": "DENY",
    "X-Content-Type-Options": "nosniff",
    "Content-Security-Policy": "frame-ancestors 'none'",
}


async def _oauth_authorize(request: Request):
    if not _rate_ok(_client_ip(request.scope), limit=20):
        return JSONResponse({"error": "rate_limit_exceeded"}, status_code=429)

    # Prune expired codes on every authorize call — prevents unbounded memory growth
    now = time.time()
    for k in [k for k, v in _auth_codes.items() if now > v["expires"]]:
        del _auth_codes[k]

    if request.method == "GET":
        p = dict(request.query_params)
        redirect_uri   = p.get("redirect_uri", "")
        state          = p.get("state", "")
        code_challenge = p.get("code_challenge", "")

        if not code_challenge:
            return JSONResponse({"error": "invalid_request", "error_description": "code_challenge is required (PKCE S256)"}, status_code=400)
        if not _redirect_allowed(redirect_uri):
            log.warning(f"OAuth authorize: blocked redirect_uri={redirect_uri[:80]}")
            return JSONResponse({"error": "invalid_request", "error_description": "redirect_uri not permitted"}, status_code=400)

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
        return HTMLResponse(html, headers=_SECURITY_HEADERS)

    form           = await request.form()
    pat            = form.get("pat", "").strip()
    redirect_uri   = form.get("redirect_uri", "")
    state          = form.get("state", "")
    code_challenge = form.get("code_challenge", "")
    log.info(f"OAuth authorize POST, pat_present={bool(pat)}")

    # Re-validate redirect_uri on POST — the hidden field value could be tampered
    if not _redirect_allowed(redirect_uri):
        log.warning(f"OAuth authorize POST: blocked redirect_uri={redirect_uri[:80]}")
        return HTMLResponse("<p>Invalid redirect URI.</p>", status_code=400)

    if not pat:
        return HTMLResponse("<p>PAT is required.</p>", status_code=400)

    code = secrets.token_urlsafe(24)
    _auth_codes[code] = {"token": pat, "code_challenge": code_challenge, "expires": time.time() + 300}
    log.info(f"Auth code issued, redirecting to {redirect_uri[:60]}")
    return RedirectResponse(f"{redirect_uri}?{urlencode({'code': code, 'state': state})}", status_code=302)


async def _oauth_token(request: Request):
    if not _rate_ok(_client_ip(request.scope), limit=10):
        return JSONResponse({"error": "rate_limit_exceeded"}, status_code=429)

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

    log.info(f"OAuth token exchange, code_present={bool(code)}")
    entry = _auth_codes.pop(code, None)
    if not entry or time.time() > entry["expires"]:
        return JSONResponse({"error": "invalid_grant"}, status_code=400)

    # PKCE is always required — codes issued without code_challenge are rejected
    if not entry["code_challenge"] or not code_verifier:
        log.warning("PKCE missing on token exchange")
        return JSONResponse({"error": "invalid_grant"}, status_code=400)
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
    log.info(f"MCP {method} /sse token_present=True")
    _token_var.set(token)

    if method == "POST" and _HAS_STREAMABLE:
        # New Streamable HTTP transport — message is in the POST body
        request = Request(scope, receive, send)
        session_id = request.headers.get("mcp-session-id")

        if session_id and session_id in _http_sessions:
            entry = _http_sessions[session_id]
            transport = entry["transport"]
            log.info(f"Reusing session {session_id[:8]}...")
            # Session already running — handle request directly
            await transport.handle_request(scope, receive, send)
        else:
            transport = StreamableHTTPServerTransport(
                mcp_session_id=secrets.token_hex(16),
                is_json_response_enabled=False,
            )
            new_sid = transport.mcp_session_id
            ready = asyncio.Event()
            _http_sessions[new_sid] = {"transport": transport, "ready": ready}
            log.info(f"New session {new_sid[:8]}...")

            async def _run_server():
                try:
                    async with transport.connect() as (read, write):
                        ready.set()  # signal: connect() is done, handle_request() can proceed
                        await mcp_app.run(read, write, mcp_app.create_initialization_options())
                except Exception as e:
                    log.error(f"MCP session error: {e}", exc_info=True)
                finally:
                    ready.set()  # unblock handle_request() even on error
                    _http_sessions.pop(new_sid, None)
                    log.info(f"Session {new_sid[:8]} ended")

            asyncio.create_task(_run_server())

            # Wait until connect() has set up the streams before handling the request
            await ready.wait()
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
