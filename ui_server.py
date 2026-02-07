#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from typing import Dict, Iterable, Optional

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else v


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


def _env_bool(name: str, default: str = "0") -> bool:
    v = _env(name, default).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


# Upstream API location (used by the optional reverse-proxy).
# Defaults to localhost because api_server.py defaults to API_HOST=127.0.0.1.
API_UPSTREAM = _env("UI_API_UPSTREAM", _env("UI_API_BASE", "http://127.0.0.1:8001"))

# If enabled, the UI server reverse-proxies /api/* to API_UPSTREAM.
# This avoids the common pitfall where the browser tries to connect to 127.0.0.1 (its own machine).
UI_PROXY_API = _env_bool("UI_PROXY_API", "1")

app = FastAPI(title="GoodWe Control UI")


# -------------------------
# Reverse proxy (optional)
# -------------------------

_httpx_client = None  # created lazily on first request


def _hop_by_hop_headers() -> set:
    # RFC 7230 hop-by-hop headers must not be forwarded.
    return {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
        "host",
    }


def _filter_headers(headers: Iterable[tuple[str, str]]) -> Dict[str, str]:
    bad = _hop_by_hop_headers()
    out: Dict[str, str] = {}
    for k, v in headers:
        lk = k.lower()
        if lk in bad:
            continue
        out[k] = v
    return out


async def _get_httpx():
    global _httpx_client
    if _httpx_client is None:
        import httpx

        # No hard-coded timeout: SSE streams should be long-lived.
        _httpx_client = httpx.AsyncClient(timeout=None)
    return _httpx_client


@app.on_event("shutdown")
async def _shutdown_httpx() -> None:
    global _httpx_client
    if _httpx_client is not None:
        try:
            await _httpx_client.aclose()
        finally:
            _httpx_client = None


@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])  # type: ignore[misc]
async def proxy_api(path: str, request: Request) -> Response:
    """Reverse-proxy /api/* to the upstream API.

    This keeps the browser on the same origin as the UI, avoiding CORS + 127.0.0.1 pitfalls.

    Disable with UI_PROXY_API=0 if you want the browser to connect directly to api_server.
    """

    if not UI_PROXY_API:
        return Response(status_code=404, content=b"UI_PROXY_API disabled")

    upstream = API_UPSTREAM.rstrip("/")
    url = f"{upstream}/api/{path}"

    client = await _get_httpx()

    # Forward query params and body.
    body = await request.body()
    headers = _filter_headers(request.headers.items())

    # Best-effort: preserve Accept so SSE keeps correct content type.
    try:
        async with client.stream(
            request.method,
            url,
            params=dict(request.query_params),
            content=body if body else None,
            headers=headers,
        ) as resp:
            resp_headers = _filter_headers(resp.headers.items())

            # NOTE: do not set content-length when streaming.
            resp_headers.pop("content-length", None)

            media_type = resp.headers.get("content-type")

            return StreamingResponse(
                resp.aiter_bytes(),
                status_code=resp.status_code,
                headers=resp_headers,
                media_type=media_type,
            )
    except Exception as e:
        return Response(status_code=502, content=f"Upstream API error: {e}".encode("utf-8"))


# -------------------------
# UI page
# -------------------------


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    # If we are proxying, the browser should use relative paths (same origin).
    # Otherwise, it should connect directly to UI_API_BASE.
    api_base_for_browser = "" if UI_PROXY_API else _env("UI_API_BASE", "")
    api_base_js = json.dumps(api_base_for_browser)

    proxy_banner = "(proxied)" if UI_PROXY_API else "(direct)"

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>GoodWe Control — Live</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; background: #0b0f14; color: #e6edf3; }}
    header {{ padding: 12px 16px; border-bottom: 1px solid #202938; display: flex; gap: 12px; align-items: baseline; }}
    header h1 {{ font-size: 16px; margin: 0; font-weight: 600; }}
    header .status {{ font-size: 12px; opacity: 0.8; }}
    main {{ padding: 16px; display: grid; gap: 12px; grid-template-columns: 1fr; }}
    .grid {{ display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); }}
    .card {{ background: #0f1723; border: 1px solid #202938; border-radius: 10px; padding: 12px; }}
    .card h2 {{ font-size: 13px; margin: 0 0 8px; opacity: 0.9; }}
    .kv {{ display: grid; grid-template-columns: 140px 1fr; gap: 4px 10px; font-size: 13px; }}
    .kv div:nth-child(odd) {{ opacity: 0.75; }}
    .pill {{ display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; border: 1px solid #2a3547; }}
    .pill.ok {{ background: rgba(46, 160, 67, 0.15); border-color: rgba(46, 160, 67, 0.45); }}
    .pill.bad {{ background: rgba(248, 81, 73, 0.15); border-color: rgba(248, 81, 73, 0.45); }}
    .pill.warn {{ background: rgba(210, 153, 34, 0.15); border-color: rgba(210, 153, 34, 0.45); }}
    .log {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; white-space: pre; overflow: auto; max-height: 45vh; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
    th, td {{ border-bottom: 1px solid #202938; padding: 6px 8px; text-align: left; }}
    th {{ opacity: 0.8; font-weight: 600; }}
    .muted {{ opacity: 0.7; }}
  </style>
</head>
<body>
  <header>
    <h1>GoodWe Control — Live</h1>
    <div class=\"status\" id=\"status\">connecting…</div>
  </header>

  <main>
    <div class=\"grid\">
      <div class=\"card\">
        <h2>Decision</h2>
        <div class=\"kv\">
          <div>export_costs</div><div id=\"export_costs\" class=\"muted\">—</div>
          <div>want_limit</div><div id=\"want_limit\" class=\"muted\">—</div>
          <div>want_enabled</div><div id=\"want_enabled\" class=\"muted\">—</div>
          <div>reason</div><div id=\"reason\" class=\"muted\">—</div>
          <div>write</div><div id=\"write\" class=\"muted\">—</div>
        </div>
      </div>

      <div class=\"card\">
        <h2>Amber</h2>
        <div class=\"kv\">
          <div>feedIn</div><div id=\"amber_feedin\" class=\"muted\">—</div>
          <div>import</div><div id=\"amber_import\" class=\"muted\">—</div>
          <div>age</div><div id=\"amber_age\" class=\"muted\">—</div>
          <div>interval_end</div><div id=\"amber_end\" class=\"muted\">—</div>
        </div>
      </div>

      <div class=\"card\">
        <h2>AlphaESS</h2>
        <div class=\"kv\">
          <div>SOC</div><div id=\"alpha_soc\" class=\"muted\">—</div>
          <div>pload</div><div id=\"alpha_pload\" class=\"muted\">—</div>
          <div>pbat</div><div id=\"alpha_pbat\" class=\"muted\">—</div>
          <div>pgrid</div><div id=\"alpha_pgrid\" class=\"muted\">—</div>
          <div>age</div><div id=\"alpha_age\" class=\"muted\">—</div>
        </div>
      </div>

      <div class=\"card\">
        <h2>GoodWe</h2>
        <div class=\"kv\">
          <div>gen</div><div id=\"gw_gen\" class=\"muted\">—</div>
          <div>feed</div><div id=\"gw_feed\" class=\"muted\">—</div>
          <div>temp</div><div id=\"gw_temp\" class=\"muted\">—</div>
          <div>meterOK</div><div id=\"gw_meter\" class=\"muted\">—</div>
          <div>wifi</div><div id=\"gw_wifi\" class=\"muted\">—</div>
        </div>
      </div>
    </div>

    <div class=\"card\">
      <h2>Live stream</h2>
      <div class=\"log\" id=\"log\"></div>
    </div>

    <div class=\"card\">
      <h2>Recent events</h2>
      <table>
        <thead>
          <tr>
            <th>id</th>
            <th>ts_local</th>
            <th>feedIn</th>
            <th>export_costs</th>
            <th>want_pct</th>
            <th>reason</th>
          </tr>
        </thead>
        <tbody id=\"rows\"></tbody>
      </table>
    </div>
  </main>

<script>
const API_BASE = {api_base_js};
const MODE = {json.dumps(proxy_banner)};
const $ = (id) => document.getElementById(id);
let lastId = 0;

function pill(ok, text) {{
  const cls = ok === true ? 'pill ok' : (ok === false ? 'pill bad' : 'pill warn');
  return `<span class="${{cls}}">${{text}}</span>`;
}}

function fmt(x, suffix='') {{
  if (x === null || x === undefined) return '—';
  return `${{x}}${{suffix}}`;
}}

function appendLog(line) {{
  const el = $('log');
  el.textContent += line + "\n";
  el.scrollTop = el.scrollHeight;
}}

function addRow(e) {{
  const d = e.data || {{}};
  const amber = d.sources?.amber || {{}};
  const dec = d.decision || {{}};
  const tr = document.createElement('tr');
  tr.innerHTML = `
    <td>${{e.id}}</td>
    <td>${{fmt(e.ts_local)}}</td>
    <td>${{fmt(amber.feedin_c, 'c')}}</td>
    <td>${{String(dec.export_costs)}}</td>
    <td>${{fmt(dec.want_pct, '%')}}</td>
    <td>${{(dec.reason || '').toString().slice(0, 80)}}</td>
  `;
  const rows = $('rows');
  rows.prepend(tr);
  while (rows.children.length > 50) rows.removeChild(rows.lastChild);
}}

function render(e) {{
  const d = e.data || {{}};
  const amber = d.sources?.amber || {{}};
  const alpha = d.sources?.alpha || {{}};
  const gw = d.sources?.goodwe || {{}};
  const dec = d.decision || {{}};
  const act = d.actuation || {{}};

  $('export_costs').innerHTML = dec.export_costs ? pill(false, 'true (costs)') : pill(true, 'false (ok)');
  $('want_limit').textContent = fmt(dec.want_pct, '%') + ' (~' + fmt(dec.target_w, 'W') + ')';
  $('want_enabled').textContent = fmt(dec.want_enabled);
  $('reason').textContent = fmt(dec.reason);
  $('write').textContent = act.write_attempted ? (act.write_ok ? 'ok' : ('failed: ' + fmt(act.write_error))) : 'not attempted';

  $('amber_feedin').textContent = fmt(amber.feedin_c, 'c');
  $('amber_import').textContent = fmt(amber.import_c, 'c');
  $('amber_age').textContent = fmt(amber.age_s, 's');
  $('amber_end').textContent = fmt(amber.interval_end_utc);

  $('alpha_soc').textContent = fmt(alpha.soc_pct, '%');
  $('alpha_pload').textContent = fmt(alpha.pload_w, 'W');
  $('alpha_pbat').textContent = fmt(alpha.pbat_w, 'W');
  $('alpha_pgrid').textContent = fmt(alpha.pgrid_w, 'W');
  $('alpha_age').textContent = fmt(alpha.age_s, 's');

  $('gw_gen').textContent = fmt(gw.gen_w, 'W');
  $('gw_feed').textContent = fmt(gw.feed_w, 'W');
  $('gw_temp').textContent = fmt(gw.temp_c, 'C');
  $('gw_meter').textContent = fmt(gw.meter_ok);
  $('gw_wifi').textContent = fmt(gw.wifi_pct, '%');

  appendLog(`[${{e.ts_local}}] feedIn=${{fmt(amber.feedin_c,'c')}} export_costs=${{dec.export_costs}} want=${{fmt(dec.want_pct,'%')}} enabled=${{dec.want_enabled}} reason=${{(dec.reason||'').toString()}}`);
  addRow(e);
}}

async function init() {{
  const baseLabel = API_BASE && API_BASE.length ? API_BASE : window.location.origin;
  $('status').textContent = `API ${{MODE}}: ${{baseLabel}}`;

  try {{
    const r = await fetch(`${{API_BASE}}/api/events/latest`);
    if (r.ok) {{
      const e = await r.json();
      lastId = e.id || 0;
      render(e);
    }}
  }} catch (e) {{
    $('status').textContent = `API unreachable ${{MODE}}: ${{baseLabel}}`;
  }}

  const es = new EventSource(`${{API_BASE}}/api/sse/events?after_id=${{lastId}}`);
  es.addEventListener('event', (msg) => {{
    try {{
      const e = JSON.parse(msg.data);
      lastId = Math.max(lastId, e.id || 0);
      render(e);
      $('status').textContent = `connected ${{MODE}} (last id: ${{lastId}})`;
    }} catch (err) {{
      // ignore
    }}
  }});
  es.onerror = () => {{
    $('status').textContent = `disconnected ${{MODE}} — retrying…`;
  }};
}}

init();
</script>
</body>
</html>"""


if __name__ == "__main__":
    import uvicorn

    host = _env("UI_HOST", "0.0.0.0")
    port = _env_int("UI_PORT", 8000)
    uvicorn.run(app, host=host, port=port, log_level="info")
