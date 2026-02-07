#!/usr/bin/env python3
from __future__ import annotations

import html
import json
import os
import sqlite3
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

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
# api_server.py typically listens on 127.0.0.1:8001.
API_UPSTREAM = _env("UI_API_UPSTREAM", _env("UI_API_BASE", "http://127.0.0.1:8001")).rstrip("/")

# If enabled, the UI server reverse-proxies /api/* to API_UPSTREAM.
# This avoids the common pitfall where the browser tries to connect to 127.0.0.1 (its own machine).
UI_PROXY_API = _env_bool("UI_PROXY_API", "1")

# DB path for server-side rendering fallback (works even if JS is blocked).
DB_PATH = _env("UI_DB_PATH", _env("API_DB_PATH", _env("INGEST_DB_PATH", "data/events.sqlite3")))

# Optional server-side auto-refresh. Useful when JS is blocked.
# IMPORTANT: If you want the JS/SSE live mode, leave this at 0 (default).
UI_REFRESH_SEC = _env_int("UI_REFRESH_SEC", 0)

# Build id to defeat caching (changes each UI server start unless overridden)
BUILD_ID = _env("UI_BUILD_ID", str(int(time.time())))

app = FastAPI(title="GoodWe Control UI")

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
    """Reverse-proxy /api/* to the upstream API."""
    if not UI_PROXY_API:
        return Response(status_code=404, content=b"UI_PROXY_API disabled")

    url = f"{API_UPSTREAM}/api/{path}"

    client = await _get_httpx()

    body = await request.body()
    headers = _filter_headers(request.headers.items())
    params = dict(request.query_params)

    accept = (request.headers.get("accept") or "").lower()
    want_stream = path.startswith("sse/") or ("text/event-stream" in accept)

    try:
        req = client.build_request(
            request.method,
            url,
            params=params,
            content=body if body else None,
            headers=headers,
        )

        if not want_stream:
            resp = await client.send(req, stream=False)
            resp_headers = _filter_headers(resp.headers.items())
            resp_headers.setdefault("cache-control", "no-store")
            return Response(
                content=resp.content,
                status_code=resp.status_code,
                headers=resp_headers,
                media_type=resp.headers.get("content-type"),
            )

        resp = await client.send(req, stream=True)
        resp_headers = _filter_headers(resp.headers.items())
        resp_headers.pop("content-length", None)
        resp_headers.setdefault("cache-control", "no-cache")
        resp_headers.setdefault("x-accel-buffering", "no")

        async def gen():
            try:
                async for chunk in resp.aiter_bytes():
                    if chunk:
                        yield chunk
            finally:
                await resp.aclose()

        return StreamingResponse(
            gen(),
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    except Exception as e:
        return Response(status_code=502, content=f"Upstream API error: {e}".encode("utf-8"))


# -------------------------
# DB helpers for server-side render
# -------------------------


def _db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Readers are fine while writers are writing due to WAL.
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
    return conn


def _row_to_event(row: sqlite3.Row) -> Dict[str, Any]:
    d: Dict[str, Any] = dict(row)
    data_json = d.get("data_json")
    if isinstance(data_json, str):
        try:
            d["data"] = json.loads(data_json)
        except Exception:
            d["data"] = None
    else:
        d["data"] = None
    return d


def _load_latest_and_recent(limit: int = 50) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]], Optional[str]]:
    try:
        conn = _db_connect(DB_PATH)
    except Exception as e:
        return None, [], f"db open failed: {e}"

    try:
        latest_row = conn.execute("SELECT * FROM events ORDER BY id DESC LIMIT 1").fetchone()
        latest = _row_to_event(latest_row) if latest_row else None
        rows = conn.execute(
            "SELECT id, ts_local, export_costs, want_pct, want_enabled, reason, data_json FROM events ORDER BY id DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
        recent = [_row_to_event(r) for r in rows]
        return latest, recent, None
    except Exception as e:
        return None, [], f"db query failed: {e}"
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _html_escape(s: Any) -> str:
    if s is None:
        return "-"
    return html.escape(str(s))


def _extract_display(latest: Optional[Dict[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {
        "export_costs": "-",
        "want_limit": "-",
        "want_enabled": "-",
        "reason": "-",
        "write": "-",
        "amber_feedin": "-",
        "amber_import": "-",
        "amber_age": "-",
        "amber_end": "-",
        "alpha_soc": "-",
        "alpha_pload": "-",
        "alpha_pbat": "-",
        "alpha_pgrid": "-",
        "alpha_age": "-",
        "gw_gen": "-",
        "gw_feed": "-",
        "gw_temp": "-",
        "gw_meter": "-",
        "gw_wifi": "-",
    }
    if not latest:
        return out

    data = latest.get("data") or {}
    sources = (data.get("sources") or {}) if isinstance(data, dict) else {}
    decision = (data.get("decision") or {}) if isinstance(data, dict) else {}
    act = (data.get("actuation") or {}) if isinstance(data, dict) else {}

    amber = sources.get("amber") or {}
    alpha = sources.get("alpha") or {}
    goodwe = sources.get("goodwe") or {}

    def _fmt(v: Any, suf: str = "") -> str:
        if v is None:
            return "-"
        return f"{v}{suf}"

    export_costs = decision.get("export_costs")
    out["export_costs"] = "true (costs)" if export_costs else "false (ok)"

    want_pct = decision.get("want_pct", latest.get("want_pct"))
    target_w = decision.get("target_w")
    if target_w:
        out["want_limit"] = f"{want_pct}% (~{target_w}W)"
    else:
        out["want_limit"] = _fmt(want_pct, "%")

    out["want_enabled"] = _fmt(decision.get("want_enabled", latest.get("want_enabled")))
    out["reason"] = _fmt(decision.get("reason", latest.get("reason")))

    if act.get("write_attempted"):
        out["write"] = "ok" if act.get("write_ok") else ("failed: " + _fmt(act.get("write_error")))
    else:
        out["write"] = "not attempted"

    out["amber_feedin"] = _fmt(amber.get("feedin_c"), "c")
    out["amber_import"] = _fmt(amber.get("import_c"), "c")
    out["amber_age"] = _fmt(amber.get("age_s"), "s")
    out["amber_end"] = _fmt(amber.get("interval_end_utc"))

    out["alpha_soc"] = _fmt(alpha.get("soc_pct"), "%")
    out["alpha_pload"] = _fmt(alpha.get("pload_w"), "W")
    out["alpha_pbat"] = _fmt(alpha.get("pbat_w"), "W")
    out["alpha_pgrid"] = _fmt(alpha.get("pgrid_w"), "W")
    out["alpha_age"] = _fmt(alpha.get("age_s"), "s")

    out["gw_gen"] = _fmt(goodwe.get("gen_w"), "W")
    out["gw_feed"] = _fmt(goodwe.get("feed_w"), "W")
    out["gw_temp"] = _fmt(goodwe.get("temp_c"), "C")
    out["gw_meter"] = _fmt(goodwe.get("meter_ok"))
    out["gw_wifi"] = _fmt(goodwe.get("wifi_pct"), "%")
    return out


# -------------------------
# UI templates
# -------------------------

_HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta http-equiv="Cache-Control" content="no-store" />
  <meta http-equiv="Pragma" content="no-cache" />
  __META_REFRESH__
  <title>GoodWe Control - Live</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; background: #0b0f14; color: #e6edf3; }
    header { padding: 12px 16px; border-bottom: 1px solid #202938; display: flex; gap: 12px; align-items: baseline; }
    header h1 { font-size: 16px; margin: 0; font-weight: 600; }
    header .status { font-size: 12px; opacity: 0.85; }
    main { padding: 16px; display: grid; gap: 12px; grid-template-columns: 1fr; }
    .grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); }
    .card { background: #0f1723; border: 1px solid #202938; border-radius: 10px; padding: 12px; }
    .card h2 { font-size: 13px; margin: 0 0 8px; opacity: 0.9; }
    .kv { display: grid; grid-template-columns: 140px 1fr; gap: 4px 10px; font-size: 13px; }
    .kv div:nth-child(odd) { opacity: 0.75; }
    .pill { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; border: 1px solid #2a3547; }
    .pill.ok { background: rgba(46, 160, 67, 0.15); border-color: rgba(46, 160, 67, 0.45); }
    .pill.bad { background: rgba(248, 81, 73, 0.15); border-color: rgba(248, 81, 73, 0.45); }
    .pill.warn { background: rgba(210, 153, 34, 0.15); border-color: rgba(210, 153, 34, 0.45); }
    .log { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; white-space: pre; overflow: auto; max-height: 40vh; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th, td { border-bottom: 1px solid #202938; padding: 6px 8px; text-align: left; }
    th { opacity: 0.8; font-weight: 600; }
    .muted { opacity: 0.7; }
    .err { border-color: rgba(248, 81, 73, 0.55); background: rgba(248, 81, 73, 0.08); }
    .err pre { margin: 0; white-space: pre-wrap; word-break: break-word; }
    .build { margin-left: auto; opacity: 0.55; font-size: 11px; }
    .small { font-size: 12px; opacity: 0.8; }
  </style>
</head>
<body data-build="__BUILD__" data-mode="__MODE__">
  <header>
    <h1>GoodWe Control - Live</h1>
    <div class="status" id="status">__STATUS__</div>
    <div class="build">build: __BUILD__</div>
  </header>

  <main>
    <div class="card">
      <h2>Info</h2>
      <div class="small">DB: __DB_PATH__</div>
      <div class="small">Mode: __MODE__</div>
      <div class="small">If JS is blocked, set UI_REFRESH_SEC=2 for server-side auto-refresh.</div>
    </div>

    __DB_ERROR__

    <div class="card err" id="uiError" style="display:none;">
      <h2>UI error</h2>
      <pre id="uiErrorText"></pre>
    </div>

    <div class="grid">
      <div class="card">
        <h2>Decision</h2>
        <div class="kv">
          <div>export_costs</div><div id="export_costs" class="muted">__export_costs__</div>
          <div>want_limit</div><div id="want_limit" class="muted">__want_limit__</div>
          <div>want_enabled</div><div id="want_enabled" class="muted">__want_enabled__</div>
          <div>reason</div><div id="reason" class="muted">__reason__</div>
          <div>write</div><div id="write" class="muted">__write__</div>
        </div>
      </div>

      <div class="card">
        <h2>Amber</h2>
        <div class="kv">
          <div>feedIn</div><div id="amber_feedin" class="muted">__amber_feedin__</div>
          <div>import</div><div id="amber_import" class="muted">__amber_import__</div>
          <div>age</div><div id="amber_age" class="muted">__amber_age__</div>
          <div>interval_end</div><div id="amber_end" class="muted">__amber_end__</div>
        </div>
      </div>

      <div class="card">
        <h2>AlphaESS</h2>
        <div class="kv">
          <div>SOC</div><div id="alpha_soc" class="muted">__alpha_soc__</div>
          <div>pload</div><div id="alpha_pload" class="muted">__alpha_pload__</div>
          <div>pbat</div><div id="alpha_pbat" class="muted">__alpha_pbat__</div>
          <div>pgrid</div><div id="alpha_pgrid" class="muted">__alpha_pgrid__</div>
          <div>age</div><div id="alpha_age" class="muted">__alpha_age__</div>
        </div>
      </div>

      <div class="card">
        <h2>GoodWe</h2>
        <div class="kv">
          <div>gen</div><div id="gw_gen" class="muted">__gw_gen__</div>
          <div>feed</div><div id="gw_feed" class="muted">__gw_feed__</div>
          <div>temp</div><div id="gw_temp" class="muted">__gw_temp__</div>
          <div>meterOK</div><div id="gw_meter" class="muted">__gw_meter__</div>
          <div>wifi</div><div id="gw_wifi" class="muted">__gw_wifi__</div>
        </div>
      </div>
    </div>

    <div class="card">
      <h2>Live stream</h2>
      <div class="log" id="log"></div>
    </div>

    <div class="card">
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
        <tbody id="rows">__ROWS__</tbody>
      </table>
    </div>

    <script src="/app.js?v=__BUILD__"></script>
  </main>
</body>
</html>
"""


_JS_TEMPLATE = """// app.js (conservative JS: no template literals, no arrow functions)
(function() {
  function $(id) { return document.getElementById(id); }

  function showError(msg) {
    var box = $('uiError');
    var pre = $('uiErrorText');
    if (box && pre) {
      box.style.display = 'block';
      pre.textContent = msg;
    }
  }

  window.addEventListener('error', function(e) {
    showError('error: ' + e.message + '\n' + e.filename + ':' + e.lineno + ':' + e.colno);
  });

  window.addEventListener('unhandledrejection', function(e) {
    showError('unhandledrejection: ' + String(e.reason));
  });

  function fmt(x, suffix) {
    if (suffix === undefined) suffix = '';
    if (x === null || x === undefined) return '—';
    return String(x) + suffix;
  }

  function appendLog(line) {
    var el = $('log');
    if (!el) return;
    el.textContent += line;
    el.textContent += String.fromCharCode(10);
    el.scrollTop = el.scrollHeight;
  }

  function setStatus(text) {
    var st = $('status');
    if (st) st.textContent = text;
  }

  // Prove JS executed (will show in ui_server logs).
  try {
    var img = new Image();
    img.src = '/js_ping?t=' + (new Date().getTime());
  } catch (e) {}

  function getNested(obj, path, fallback) {
    // path like ['sources','amber','feedin_c']
    var cur = obj;
    for (var i = 0; i < path.length; i++) {
      if (!cur) return fallback;
      cur = cur[path[i]];
    }
    if (cur === undefined || cur === null) return fallback;
    return cur;
  }

  function renderEvent(e) {
    var d = e.data || {};
    var sources = d.sources || {};
    var amber = sources.amber || {};
    var alpha = sources.alpha || {};
    var gw = sources.goodwe || {};

    var dec = d.decision || {
      export_costs: Boolean(e.export_costs),
      want_pct: e.want_pct,
      want_enabled: e.want_enabled,
      reason: e.reason,
      target_w: undefined
    };
    var act = d.actuation || {};

    var el;

    el = $('export_costs');
    if (el) el.textContent = dec.export_costs ? 'true (costs)' : 'false (ok)';

    el = $('want_limit');
    if (el) el.textContent = fmt(dec.want_pct, '%') + (dec.target_w ? (' (~' + fmt(dec.target_w, 'W') + ')') : '');

    el = $('want_enabled');
    if (el) el.textContent = fmt(dec.want_enabled);

    el = $('reason');
    if (el) el.textContent = fmt(dec.reason);

    el = $('write');
    if (el) el.textContent = act.write_attempted ? (act.write_ok ? 'ok' : ('failed: ' + fmt(act.write_error))) : 'not attempted';

    el = $('amber_feedin');
    if (el) el.textContent = fmt(amber.feedin_c, 'c');

    el = $('amber_import');
    if (el) el.textContent = fmt(amber.import_c, 'c');

    el = $('amber_age');
    if (el) el.textContent = fmt(amber.age_s, 's');

    el = $('amber_end');
    if (el) el.textContent = fmt(amber.interval_end_utc);

    el = $('alpha_soc');
    if (el) el.textContent = fmt(alpha.soc_pct, '%');

    el = $('alpha_pload');
    if (el) el.textContent = fmt(alpha.pload_w, 'W');

    el = $('alpha_pbat');
    if (el) el.textContent = fmt(alpha.pbat_w, 'W');

    el = $('alpha_pgrid');
    if (el) el.textContent = fmt(alpha.pgrid_w, 'W');

    el = $('alpha_age');
    if (el) el.textContent = fmt(alpha.age_s, 's');

    el = $('gw_gen');
    if (el) el.textContent = fmt(gw.gen_w, 'W');

    el = $('gw_feed');
    if (el) el.textContent = fmt(gw.feed_w, 'W');

    el = $('gw_temp');
    if (el) el.textContent = fmt(gw.temp_c, 'C');

    el = $('gw_meter');
    if (el) el.textContent = fmt(gw.meter_ok);

    el = $('gw_wifi');
    if (el) el.textContent = fmt(gw.wifi_pct, '%');

    appendLog('[' + fmt(e.ts_local) + '] feedIn=' + fmt(amber.feedin_c,'c') +
              ' export_costs=' + String(dec.export_costs) +
              ' want=' + fmt(dec.want_pct,'%') +
              ' enabled=' + fmt(dec.want_enabled) +
              ' reason=' + String(dec.reason || ''));
  }

  function addRow(e) {
    var d = e.data || {};
    var amber = getNested(d, ['sources','amber'], {});
    var dec = d.decision || {};

    var tr = document.createElement('tr');
    tr.innerHTML =
      '<td>' + fmt(e.id) + '</td>' +
      '<td>' + fmt(e.ts_local) + '</td>' +
      '<td>' + fmt(amber.feedin_c, 'c') + '</td>' +
      '<td>' + String(dec.export_costs) + '</td>' +
      '<td>' + fmt(dec.want_pct, '%') + '</td>' +
      '<td>' + String((dec.reason || '')).slice(0, 80) + '</td>';

    var rows = $('rows');
    if (!rows) return;
    if (rows.firstChild) rows.insertBefore(tr, rows.firstChild);
    else rows.appendChild(tr);

    while (rows.children.length > 50) rows.removeChild(rows.lastChild);
  }

  function httpGetJson(url, onOk, onErr) {
    try {
      var xhr = new XMLHttpRequest();
      xhr.open('GET', url, true);
      xhr.setRequestHeader('Cache-Control', 'no-store');
      xhr.onreadystatechange = function() {
        if (xhr.readyState !== 4) return;
        if (xhr.status >= 200 && xhr.status < 300) {
          try { onOk(JSON.parse(xhr.responseText)); }
          catch (e) { onErr('JSON parse failed: ' + e); }
        } else {
          onErr('HTTP ' + xhr.status + ' ' + xhr.statusText + ' body=' + xhr.responseText);
        }
      };
      xhr.send(null);
    } catch (e) {
      onErr('XHR failed: ' + e);
    }
  }

  var lastId = 0;
  var es = null;
  var reconnectTimer = null;

  function connectSSE() {
    if (es) {
      try { es.close(); } catch (e) {}
      es = null;
    }
    var url = '/api/sse/events?after_id=' + String(lastId);
    appendLog('connecting SSE: ' + url);
    setStatus('connecting SSE (after_id=' + String(lastId) + ')');

    try {
      es = new EventSource(url);
    } catch (e) {
      setStatus('EventSource failed: ' + e);
      return;
    }

    es.addEventListener('event', function(msg) {
      try {
        var ev = JSON.parse(msg.data);
        if (ev && ev.id) lastId = Math.max(lastId, ev.id);
        renderEvent(ev);
        addRow(ev);
        setStatus('connected (last id: ' + String(lastId) + ')');
      } catch (e) {
        showError('SSE parse/render error: ' + e + '\nraw: ' + msg.data);
      }
    });

    es.onerror = function() {
      setStatus('SSE disconnected - retrying...');
      appendLog('SSE disconnected - retry in 2s');
      if (reconnectTimer) return;
      reconnectTimer = setTimeout(function() {
        reconnectTimer = null;
        connectSSE();
      }, 2000);
    };
  }

  function init() {
    var build = document.body ? document.body.getAttribute('data-build') : '';
    var mode = document.body ? document.body.getAttribute('data-mode') : '';
    setStatus('js running (mode ' + mode + ', build ' + build + ')');

    httpGetJson('/api/events/latest', function(e) {
      try {
        lastId = e.id || 0;
        renderEvent(e);
        addRow(e);
        setStatus('api ok (latest id: ' + String(lastId) + ') - connecting SSE...');
        connectSSE();
      } catch (err) {
        showError('render(latest) failed: ' + err);
      }
    }, function(err) {
      // If api_server is down, we still have server-side render.
      showError('GET /api/events/latest failed: ' + err);
      setStatus('api failed - using server render only');
      // Poll every 5s as a last resort (no SSE)
      setInterval(function() {
        httpGetJson('/api/events/latest', function(e2) {
          lastId = e2.id || lastId;
          renderEvent(e2);
          setStatus('api ok (polled latest id: ' + String(lastId) + ')');
        }, function() {});
      }, 5000);
    });
  }

  try { init(); } catch (e) { showError('init threw: ' + e); }
})();
"""


@app.get("/js_ping")
def js_ping() -> Response:
    # Used to confirm the browser actually executed JS (will appear in server logs).
    return Response(content=b"ok", media_type="text/plain", headers={"cache-control": "no-store"})


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    latest, recent, db_error = _load_latest_and_recent(limit=50)
    display = _extract_display(latest)

    rows_html: List[str] = []
    for e in recent:
        data = e.get("data") or {}
        sources = (data.get("sources") or {}) if isinstance(data, dict) else {}
        amber = sources.get("amber") or {}
        feedin = amber.get("feedin_c")
        decision = (data.get("decision") or {}) if isinstance(data, dict) else {}
        export_costs = decision.get("export_costs")
        want_pct = decision.get("want_pct", e.get("want_pct"))
        reason = decision.get("reason", e.get("reason"))

        rows_html.append(
            "<tr>"
            f"<td>{_html_escape(e.get('id'))}</td>"
            f"<td>{_html_escape(e.get('ts_local'))}</td>"
            f"<td>{_html_escape(feedin)}c</td>"
            f"<td>{_html_escape(export_costs)}</td>"
            f"<td>{_html_escape(want_pct)}%</td>"
            f"<td>{_html_escape(str(reason)[:120] if reason is not None else '-') }</td>"
            "</tr>"
        )

    meta_refresh = ""
    if UI_REFRESH_SEC and UI_REFRESH_SEC > 0:
        meta_refresh = f'<meta http-equiv="refresh" content="{UI_REFRESH_SEC}" />'

    db_err_block = ""
    if db_error:
        db_err_block = (
            '<div class="card err">'
            '<h2>DB error</h2>'
            f'<pre>{_html_escape(db_error)}</pre>'
            "</div>"
        )

    mode = "proxied" if UI_PROXY_API else "direct"
    status = f"server render ok (latest id {latest.get('id') if latest else 0})"

    html_doc = _HTML_TEMPLATE
    html_doc = html_doc.replace("__META_REFRESH__", meta_refresh)
    html_doc = html_doc.replace("__BUILD__", BUILD_ID)
    html_doc = html_doc.replace("__MODE__", mode)
    html_doc = html_doc.replace("__STATUS__", _html_escape(status))
    html_doc = html_doc.replace("__DB_PATH__", _html_escape(DB_PATH))
    html_doc = html_doc.replace("__DB_ERROR__", db_err_block)
    html_doc = html_doc.replace("__ROWS__", "".join(rows_html) if rows_html else "")

    for k, v in display.items():
        html_doc = html_doc.replace(f"__{k}__", _html_escape(v))

    return HTMLResponse(content=html_doc, headers={"cache-control": "no-store"})


@app.get("/app.js")
def app_js() -> Response:
    return Response(
        content=_JS_TEMPLATE,
        media_type="application/javascript; charset=utf-8",
        headers={"cache-control": "no-store"},
    )


if __name__ == "__main__":
    import uvicorn

    host = _env("UI_HOST", "0.0.0.0")
    port = _env_int("UI_PORT", 8000)
    uvicorn.run(app, host=host, port=port, log_level="info")
