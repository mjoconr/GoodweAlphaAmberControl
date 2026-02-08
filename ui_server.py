#!/usr/bin/env python3
from __future__ import annotations

import html
import json
import logging
import os
import sqlite3
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

from logging_setup import setup_logging

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse


logger = logging.getLogger("ui")


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


def _q_int(request: Request, *names: str, default: Optional[int] = None) -> Optional[int]:
    # Parse int from query params for any of the given names.
    qp = request.query_params
    for n in names:
        if n in qp:
            raw = str(qp.get(n, "")).strip()
            if raw == "":
                continue
            try:
                return int(raw)
            except Exception:
                return default
    return default


def _q_bool(request: Request, *names: str, default: bool = False) -> bool:
    qp = request.query_params
    for n in names:
        if n in qp:
            raw = str(qp.get(n, "")).strip().lower()
            if raw in ("1", "true", "yes", "y", "on"):
                return True
            if raw in ("0", "false", "no", "n", "off"):
                return False
            # presence without value => True
            return True
    return default


API_UPSTREAM = _env("UI_API_UPSTREAM", _env("UI_API_BASE", "http://127.0.0.1:8001")).rstrip("/")
UI_PROXY_API = _env_bool("UI_PROXY_API", "1")
DB_PATH = _env("UI_DB_PATH", _env("API_DB_PATH", _env("INGEST_DB_PATH", "data/events.sqlite3")))
UI_REFRESH_SEC_DEFAULT = _env_int("UI_REFRESH_SEC", 0)
BUILD_ID = _env("UI_BUILD_ID", str(int(time.time())))

app = FastAPI(title="GoodWe Control UI")
_httpx_client = None  # created lazily on first request


def _hop_by_hop_headers() -> set:
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
        logger.exception("proxy_api upstream error method=%s path=%s url=%s", request.method, path, url)
        return Response(status_code=502, content=f"Upstream API error: {e}".encode("utf-8"))


def _db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
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
        logger.exception("db open failed db=%s", DB_PATH)
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
        logger.exception("db query failed db=%s", DB_PATH)
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
    .log { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; white-space: pre; overflow: auto; max-height: 40vh; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th, td { border-bottom: 1px solid #202938; padding: 6px 8px; text-align: left; }
    th { opacity: 0.8; font-weight: 600; }
    .muted { opacity: 0.7; }
    .err { border-color: rgba(248, 81, 73, 0.55); background: rgba(248, 81, 73, 0.08); border: 1px solid rgba(248, 81, 73, 0.55); border-radius: 10px; padding: 12px; }
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
      <div class="small">Refresh: __REFRESH_LABEL__</div>
      <div class="small">Tip: For server-only refresh use <code>/?refresh=2</code> (or <code>/?UI_REFRESH_SEC=2</code>). For SSE live mode use <code>/</code> (no refresh).</div>
    </div>

    __DB_ERROR__

    <div class="err" id="uiError" style="display:none;">
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

    __SCRIPT_TAG__
  </main>
</body>
</html>
"""

_JS_TEMPLATE = """(function() {
  function $(id) { return document.getElementById(id); }

  function showError(msg) {
    var box = $('uiError');
    var pre = $('uiErrorText');
    if (box && pre) { box.style.display = 'block'; pre.textContent = msg; }
  }

  window.addEventListener('error', function(e) {
    showError('error: ' + e.message + '\\n' + e.filename + ':' + e.lineno + ':' + e.colno);
  });
  window.addEventListener('unhandledrejection', function(e) {
    showError('unhandledrejection: ' + String(e.reason));
  });

  function fmt(x, suffix) {
    if (suffix === undefined) suffix = '';
    if (x === null || x === undefined) return '—';
    return String(x) + suffix;
  }

  function toNum(x) {
    var n = Number(x);
    return (isFinite(n) ? n : null);
  }

  // Live "age" counters: age_s is captured at event time. We tick it locally between SSE updates.
  var ageBase = {
    amber: { baseAge: null, baseTs: null },
    alpha: { baseAge: null, baseTs: null }
  };

  function setAgeBase(key, age_s, eventTsMs) {
    var a = toNum(age_s);
    var t = toNum(eventTsMs);
    if (a === null || t === null) return;
    ageBase[key] = { baseAge: a, baseTs: t };
  }

  function calcAgeSeconds(key) {
    var b = ageBase[key];
    if (!b || b.baseAge === null || b.baseTs === null) return null;
    var now = Date.now();
    var delta = (now - b.baseTs) / 1000.0;
    var val = b.baseAge + delta;
    if (!isFinite(val)) return null;
    if (val < 0) val = 0;
    return Math.floor(val);
  }

  function renderAgesNow() {
    var a = calcAgeSeconds('amber');
    if (a !== null && $('amber_age')) $('amber_age').textContent = String(a) + 's';
    var b2 = calcAgeSeconds('alpha');
    if (b2 !== null && $('alpha_age')) $('alpha_age').textContent = String(b2) + 's';
  }

  // Tick once per second (does nothing until bases are set).
  setInterval(renderAgesNow, 1000);

    var seenIds = {};

  function seedSeenIds() {
    var rows = $('rows');
    if (!rows || !rows.children) return;
    for (var i = 0; i < rows.children.length; i++) {
      var tr = rows.children[i];
      try {
        var tds = tr.getElementsByTagName('td');
        if (!tds || tds.length < 1) continue;
        var id = parseInt((tds[0].textContent || '').trim(), 10);
        if (!isNaN(id)) seenIds[id] = true;
      } catch (e) {}
    }
  }

  function addRow(e) {
    if (!e || e.id === null || e.id === undefined) return;
    var id = parseInt(e.id, 10);
    if (isNaN(id)) return;
    if (seenIds[id]) return;
    seenIds[id] = true;

    var d = e.data || {};
    var sources = d.sources || {};
    var amber = sources.amber || {};
    var dec = d.decision || {};

    var tr = document.createElement('tr');
    tr.setAttribute('data-id', String(id));
    tr.innerHTML =
      '<td>' + fmt(id) + '</td>' +
      '<td>' + fmt(e.ts_local) + '</td>' +
      '<td>' + fmt(amber.feedin_c, 'c') + '</td>' +
      '<td>' + String(dec.export_costs) + '</td>' +
      '<td>' + fmt(dec.want_pct, '%') + '</td>' +
      '<td>' + String((dec.reason || '')).slice(0, 80) + '</td>';

    var rows = $('rows');
    if (!rows) return;
    if (rows.firstChild) rows.insertBefore(tr, rows.firstChild);
    else rows.appendChild(tr);

    while (rows.children.length > 50) {
      var last = rows.lastChild;
      if (!last) break;
      try {
        var tds = last.getElementsByTagName('td');
        if (tds && tds.length) {
          var lastId = parseInt((tds[0].textContent || '').trim(), 10);
          if (!isNaN(lastId)) delete seenIds[lastId];
        }
      } catch (e) {}
      rows.removeChild(last);
    }
  }

function appendLog(line) {
    var el = $('log');
    if (!el) return;
    el.textContent += line + String.fromCharCode(10);
    el.scrollTop = el.scrollHeight;
  }

  function setStatus(text) {
    var st = $('status');
    if (st) st.textContent = text;
  }

  // Prove JS executed (watch ui_server logs for /js_ping).
  try { (new Image()).src = '/js_ping?t=' + (new Date().getTime()); } catch (e) {}

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
      reason: e.reason
    };
    var act = d.actuation || {};

    if ($('export_costs')) $('export_costs').textContent = dec.export_costs ? 'true (costs)' : 'false (ok)';
    if ($('want_limit')) $('want_limit').textContent = fmt(dec.want_pct, '%');
    if ($('want_enabled')) $('want_enabled').textContent = fmt(dec.want_enabled);
    if ($('reason')) $('reason').textContent = fmt(dec.reason);
    if ($('write')) $('write').textContent = act.write_attempted ? (act.write_ok ? 'ok' : ('failed: ' + fmt(act.write_error))) : 'not attempted';

    if ($('amber_feedin')) $('amber_feedin').textContent = fmt(amber.feedin_c, 'c');
    if ($('amber_import')) $('amber_import').textContent = fmt(amber.import_c, 'c');
    setAgeBase('amber', amber.age_s, (d && d.ts_epoch_ms) ? d.ts_epoch_ms : (e.ts_epoch_ms || null));
    if ($('amber_end')) $('amber_end').textContent = fmt(amber.interval_end_utc);

    if ($('alpha_soc')) $('alpha_soc').textContent = fmt(alpha.soc_pct, '%');
    if ($('alpha_pload')) $('alpha_pload').textContent = fmt(alpha.pload_w, 'W');
    if ($('alpha_pbat')) $('alpha_pbat').textContent = fmt(alpha.pbat_w, 'W');
    if ($('alpha_pgrid')) $('alpha_pgrid').textContent = fmt(alpha.pgrid_w, 'W');
    setAgeBase('alpha', alpha.age_s, (d && d.ts_epoch_ms) ? d.ts_epoch_ms : (e.ts_epoch_ms || null));
    renderAgesNow();

    if ($('gw_gen')) $('gw_gen').textContent = fmt(gw.gen_w, 'W');
    if ($('gw_feed')) $('gw_feed').textContent = fmt(gw.feed_w, 'W');
    if ($('gw_temp')) $('gw_temp').textContent = fmt(gw.temp_c, 'C');
    if ($('gw_meter')) $('gw_meter').textContent = fmt(gw.meter_ok);
    if ($('gw_wifi')) $('gw_wifi').textContent = fmt(gw.wifi_pct, '%');

    appendLog('[' + fmt(e.ts_local) + '] feedIn=' + fmt(amber.feedin_c,'c') + ' export_costs=' + String(dec.export_costs) + ' want=' + fmt(dec.want_pct,'%') + ' reason=' + String(dec.reason || ''));
  }

  function httpGetJson(url, onOk, onErr) {
    try {
      var xhr = new XMLHttpRequest();
      xhr.open('GET', url, true);
      xhr.setRequestHeader('Cache-Control', 'no-store');
      xhr.onreadystatechange = function() {
        if (xhr.readyState !== 4) return;
        if (xhr.status >= 200 && xhr.status < 300) {
          try { onOk(JSON.parse(xhr.responseText)); } catch (e) { onErr('JSON parse failed: ' + e); }
        } else {
          onErr('HTTP ' + xhr.status + ' ' + xhr.statusText + ' body=' + xhr.responseText);
        }
      };
      xhr.send(null);
    } catch (e) { onErr('XHR failed: ' + e); }
  }

  var lastId = 0;
  var es = null;
  var reconnectTimer = null;

  function connectSSE() {
    if (es) { try { es.close(); } catch (e) {} es = null; }
    var url = '/api/sse/events?after_id=' + String(lastId);
    appendLog('connecting SSE: ' + url);
    setStatus('connecting SSE (after_id=' + String(lastId) + ')');

    try { es = new EventSource(url); }
    catch (e) { setStatus('EventSource failed: ' + e); return; }

    es.addEventListener('event', function(msg) {
      try {
        var ev = JSON.parse(msg.data);
        if (ev && ev.id) lastId = Math.max(lastId, ev.id);
        renderEvent(ev);
        addRow(ev);
        setStatus('connected (last id: ' + String(lastId) + ')');
      } catch (e) { showError('SSE parse/render error: ' + e + '\\nraw: ' + msg.data); }
    });

    es.onerror = function() {
      setStatus('SSE disconnected - retrying...');
      if (reconnectTimer) return;
      reconnectTimer = setTimeout(function() { reconnectTimer = null; connectSSE(); }, 2000);
    };
  }

  function init() {
    var build = document.body ? document.body.getAttribute('data-build') : '';
    var mode = document.body ? document.body.getAttribute('data-mode') : '';
    setStatus('js running (mode ' + mode + ', build ' + build + ')');
    seedSeenIds();

    httpGetJson('/api/events/latest', function(e) {
      lastId = e.id || 0;
      renderEvent(e);
      addRow(e);
      setStatus('api ok (latest id: ' + String(lastId) + ') - connecting SSE...');
      connectSSE();
    }, function(err) {
      showError('GET /api/events/latest failed: ' + err);
      setStatus('api failed - using server render only');
    });
  }

  try { init(); } catch (e) { showError('init threw: ' + e); }
})();"""
_REACT_HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta http-equiv="Cache-Control" content="no-store" />
  <meta http-equiv="Pragma" content="no-cache" />
  <title>GoodWe Control - React</title>
  <style>
    :root {
      --bg: #0b0f14;
      --panel: #0f1723;
      --border: #202938;
      --text: #e6edf3;
      --muted: rgba(230,237,243,0.72);
      --bad: rgba(248, 81, 73, 0.95);
      --warn: rgba(245, 159, 0, 0.95);
      --ok: rgba(63, 185, 80, 0.95);
    }
    body { margin: 0; background: var(--bg); color: var(--text); font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; }
    header { padding: 12px 16px; border-bottom: 1px solid var(--border); display:flex; align-items: baseline; gap: 12px; }
    header h1 { font-size: 16px; margin: 0; font-weight: 600; }
    header .status { font-size: 12px; opacity: 0.85; }
    header .build { margin-left: auto; opacity: 0.55; font-size: 11px; }
    main { padding: 16px; display: grid; gap: 12px; }
    .grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); }
    .card { background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 12px; }
    .card h2 { font-size: 13px; margin: 0 0 8px; opacity: 0.9; }
    .kv { display: grid; grid-template-columns: 140px 1fr; gap: 4px 10px; font-size: 13px; }
    .kv div:nth-child(odd) { opacity: 0.75; }
    .row { display:flex; gap: 10px; align-items: center; flex-wrap: wrap; }
    .btn { border: 1px solid var(--border); background: rgba(255,255,255,0.02); color: var(--text); border-radius: 8px; padding: 6px 10px; cursor:pointer; font-size: 12px; }
    .btn:hover { background: rgba(255,255,255,0.04); }
    .sel { border: 1px solid var(--border); background: rgba(255,255,255,0.02); color: var(--text); border-radius: 8px; padding: 6px 10px; font-size: 12px; }
    .muted { color: var(--muted); }
    .pill { font-size: 11px; padding: 2px 8px; border-radius: 999px; border: 1px solid var(--border); }
    .pill.ok { border-color: rgba(63,185,80,0.35); color: rgba(63,185,80,0.95); background: rgba(63,185,80,0.07); }
    .pill.warn { border-color: rgba(245,159,0,0.35); color: rgba(245,159,0,0.95); background: rgba(245,159,0,0.07); }
    .pill.bad { border-color: rgba(248,81,73,0.35); color: rgba(248,81,73,0.95); background: rgba(248,81,73,0.07); }
    .chartWrap { display:grid; gap: 8px; }
    .chartHead { display:flex; gap: 10px; align-items: baseline; flex-wrap: wrap; }
    .legend { display:flex; gap: 10px; flex-wrap: wrap; font-size: 12px; opacity: 0.95; }
    .legend label { display:flex; gap: 6px; align-items:center; cursor:pointer; }
    .legend .sw { width: 10px; height: 10px; border-radius: 2px; background: rgba(255,255,255,0.35); border: 1px solid rgba(255,255,255,0.15); }
    .svgBox { width: 100%; height: 220px; border: 1px solid var(--border); border-radius: 10px; background: rgba(0,0,0,0.12); }
    .tooltip { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; white-space: pre; }
    .ticker { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; white-space: pre; max-height: 200px; overflow:auto; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th, td { border-bottom: 1px solid var(--border); padding: 6px 8px; text-align: left; }
    th { opacity: 0.8; font-weight: 600; }
    .err { border: 1px solid rgba(248,81,73,0.55); background: rgba(248,81,73,0.08); border-radius: 10px; padding: 12px; }
    .err pre { margin: 0; white-space: pre-wrap; word-break: break-word; }
    a { color: rgba(88,166,255,0.95); text-decoration: none; }
    a:hover { text-decoration: underline; }
  </style>
</head>
<body data-build="__BUILD__" data-mode="__MODE__">
  <header>
    <h1>GoodWe Control - React</h1>
    <div class="status" id="status">loading…</div>
    <div class="build">build: __BUILD__</div>
  </header>

  <main>
    <div class="card">
      <div class="row" style="justify-content:space-between;">
        <div>
          <div style="font-size:12px; opacity:0.85;">Mode: __MODE__</div>
          <div style="font-size:12px; opacity:0.85;">DB: __DB_PATH__</div>
          <div style="font-size:12px; opacity:0.85;">API: __API_UPSTREAM__</div>
        </div>
        <div class="row">
          <a class="btn" href="/">Classic UI</a>
          <span class="muted" style="font-size:12px;">Experimental React UI (no build step)</span>
        </div>
      </div>
      <div class="muted" style="font-size:12px; margin-top:8px;">
        Tip: If you don't have internet access on this network, load React from CDN won't work. In that case use the Classic UI (or we can vendor React locally later).
      </div>
    </div>

    <div id="root"></div>

    <div id="bootError" class="err" style="display:none;">
      <h2 style="font-size:13px; margin:0 0 8px; opacity:0.9;">UI error</h2>
      <pre id="bootErrorText"></pre>
    </div>

    <script>
      // Friendly boot error reporting (e.g. CDN blocked)
      function bootError(msg) {
        var box = document.getElementById('bootError');
        var pre = document.getElementById('bootErrorText');
        if (box && pre) { box.style.display = 'block'; pre.textContent = msg; }
        var st = document.getElementById('status');
        if (st) st.textContent = 'boot failed';
      }
      window.addEventListener('error', function(e) {
        bootError('error: ' + e.message + '\\n' + e.filename + ':' + e.lineno + ':' + e.colno);
      });
      window.addEventListener('unhandledrejection', function(e) {
        bootError('unhandledrejection: ' + String(e.reason));
      });
    </script>

    <!-- React from CDN (no build step). If you prefer, we can vendor these locally later. -->
    <script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
    <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
    <script src="/react_app.js?v=__BUILD__"></script>
  </main>
</body>
</html>
"""


_REACT_APP_JS = r"""(function() {
  // React globals are loaded via CDN scripts in /react.
  if (!window.React || !window.ReactDOM) {
    var st = document.getElementById('status');
    if (st) st.textContent = 'React not available (CDN blocked?)';
    return;
  }

  var e = React.createElement;
  var useEffect = React.useEffect;
  var useMemo = React.useMemo;
  var useRef = React.useRef;
  var useState = React.useState;

  function clamp(n, lo, hi) { return Math.max(lo, Math.min(hi, n)); }

  function get(obj, path, def) {
    try {
      var cur = obj;
      for (var i = 0; i < path.length; i++) {
        if (cur === null || cur === undefined) return def;
        cur = cur[path[i]];
      }
      return (cur === undefined) ? def : cur;
    } catch (_) { return def; }
  }

  function fmt(x, suf) {
    if (suf === undefined) suf = '';
    if (x === null || x === undefined) return '—';
    return String(x) + suf;
  }

  function tsLabel(ms) {
    if (!ms) return '—';
    try { return new Date(ms).toLocaleTimeString(); }
    catch (_) { return String(ms); }
  }

  function fetchJSON(url) {
    return fetch(url, { cache: 'no-store' }).then(function(r) {
      if (!r.ok) throw new Error(r.status + ' ' + r.statusText);
      return r.json();
    });
  }

  function buildPill(kind, text) {
    return e('span', { className: 'pill ' + kind }, text);
  }

  function uniqPush(arr, item, maxLen) {
    var out = arr.slice();
    out.unshift(item);
    if (out.length > maxLen) out.length = maxLen;
    return out;
  }

  function decimate(points, maxN) {
    if (!points || points.length <= maxN) return points;
    var step = points.length / maxN;
    var out = [];
    for (var i = 0; i < maxN; i++) {
      out.push(points[Math.floor(i * step)]);
    }
    return out;
  }

  function computeRange(seriesList) {
    var minY = Infinity, maxY = -Infinity;
    for (var s = 0; s < seriesList.length; s++) {
      var pts = seriesList[s].points;
      for (var i = 0; i < pts.length; i++) {
        var y = pts[i][1];
        if (y === null || y === undefined || isNaN(y)) continue;
        if (y < minY) minY = y;
        if (y > maxY) maxY = y;
      }
    }
    if (minY === Infinity) { minY = 0; maxY = 1; }
    if (minY === maxY) { minY -= 1; maxY += 1; }
    // Pad range slightly
    var pad = (maxY - minY) * 0.08;
    return { minY: minY - pad, maxY: maxY + pad };
  }

  function computeXRange(seriesList) {
    var minX = Infinity, maxX = -Infinity;
    for (var s = 0; s < seriesList.length; s++) {
      var pts = seriesList[s].points;
      for (var i = 0; i < pts.length; i++) {
        var x = pts[i][0];
        if (x === null || x === undefined || isNaN(x)) continue;
        if (x < minX) minX = x;
        if (x > maxX) maxX = x;
      }
    }
    if (minX === Infinity) { minX = 0; maxX = 1; }
    if (minX === maxX) { minX -= 1; maxX += 1; }
    return { minX: minX, maxX: maxX };
  }

function LineChart(props) {
    var title = props.title;
    var subtitle = props.subtitle;
    var series = props.series;
    var height = props.height || 220;
    var maxPoints = props.maxPoints || 600;
    var showZero = props.showZero || false;
    var yLines = props.yLines || []; // [{y,label,kind}]
    var yUnit = props.yUnit || '';
    var initialEnabled = props.initialEnabled || null;
    var markers = props.markers || []; // [{ts,label,kind}]

    var enabled0 = {};
    for (var i = 0; i < series.length; i++) enabled0[series[i].key] = true;
    if (initialEnabled) {
      for (var k in enabled0) enabled0[k] = !!initialEnabled[k];
    }

    var _a = useState(enabled0), enabled = _a[0], setEnabled = _a[1];
    var _b = useState(null), hoverTs = _b[0], setHoverTs = _b[1];

    var boxRef = useRef(null);

    function nearestPoint(points, targetTs) {
      if (!points || !points.length) return null;
      var lo = 0, hi = points.length - 1;
      while (lo < hi) {
        var mid = (lo + hi) >> 1;
        if (points[mid][0] < targetTs) lo = mid + 1;
        else hi = mid;
      }
      var idx = lo;
      if (idx > 0 && Math.abs(points[idx - 1][0] - targetTs) <= Math.abs(points[idx][0] - targetTs)) idx--;
      return points[idx];
    }

    var decimated = useMemo(function() {
      var out = [];
      for (var i = 0; i < series.length; i++) {
        var s = series[i];
        if (!enabled[s.key]) continue;
        out.push({ key: s.key, name: s.name, color: s.color, points: decimate(s.points, maxPoints) });
      }
      return out;
    }, [series, enabled, maxPoints]);

    var range = useMemo(function() { return computeRange(decimated); }, [decimated]);
    var xRange = useMemo(function() { return computeXRange(decimated); }, [decimated]);

    function xOfTs(ts) {
      var den = (xRange.maxX - xRange.minX) || 1;
      var t = (ts - xRange.minX) / den;
      return clamp(t, 0, 1) * 1000.0;
    }

    function yOf(y) {
      var t = (y - range.minY) / (range.maxY - range.minY);
      t = 1.0 - t;
      return clamp(t, 0, 1) * (height - 20) + 10; // padding
    }

    function onMove(ev) {
      var el = boxRef.current;
      if (!el) return;
      var rect = el.getBoundingClientRect();
      var x = ev.clientX - rect.left;
      var w = rect.width || 1;
      var t = clamp(x / w, 0, 1);
      var targetTs = xRange.minX + t * (xRange.maxX - xRange.minX);
      if (!decimated.length || !decimated[0].points.length) { setHoverTs(null); return; }
      var anchor = nearestPoint(decimated[0].points, targetTs);
      setHoverTs(anchor ? anchor[0] : targetTs);
    }

    function onLeave() { setHoverTs(null); }

    var paths = [];
    for (var s = 0; s < decimated.length; s++) {
      var pts = decimated[s].points;
      var p = '';
      for (var i = 0; i < pts.length; i++) {
        var x = xOfTs(pts[i][0]);
        var y = yOf(pts[i][1]);
        p += (i === 0 ? 'M' : 'L') + x.toFixed(1) + ',' + y.toFixed(1);
      }
      paths.push(e('path', {
        key: decimated[s].key,
        d: p,
        fill: 'none',
        stroke: decimated[s].color || 'rgba(255,255,255,0.55)',
        strokeWidth: 2,
        vectorEffect: 'non-scaling-stroke'
      }));
    }

    var zeroLine = null;
    if (showZero && range.minY < 0 && range.maxY > 0) {
      var zy = yOf(0);
      zeroLine = e('line', { x1: 0, y1: zy, x2: 1000, y2: zy, stroke: 'rgba(255,255,255,0.18)', strokeWidth: 1, vectorEffect: 'non-scaling-stroke' });
    }

    var yLineEls = [];
    for (var j = 0; j < yLines.length; j++) {
      var yl = yLines[j];
      if (yl.y === null || yl.y === undefined || isNaN(yl.y)) continue;
      if (yl.y < range.minY || yl.y > range.maxY) continue;
      var ly = yOf(yl.y);
      var col = (yl.kind === 'bad') ? 'rgba(248,81,73,0.60)' : (yl.kind === 'warn') ? 'rgba(245,159,0,0.55)' : 'rgba(255,255,255,0.22)';
      yLineEls.push(e('line', { key: 'yl_' + j, x1: 0, y1: ly, x2: 1000, y2: ly, stroke: col, strokeWidth: 1, vectorEffect: 'non-scaling-stroke' }));
      if (yl.label) {
        yLineEls.push(e('text', { key: 'yl_t_' + j, x: 6, y: clamp(ly - 4, 12, height - 6), fill: col, fontSize: 11 }, yl.label));
      }
    }

    var markerEls = [];
    if (markers && markers.length) {
      for (var m = 0; m < markers.length; m++) {
        var mk = markers[m];
        if (!mk || mk.ts === null || mk.ts === undefined || isNaN(mk.ts)) continue;
        if (mk.ts < xRange.minX || mk.ts > xRange.maxX) continue;
        var mx = xOfTs(mk.ts);
        var mcol = (mk.kind === 'bad') ? 'rgba(248,81,73,0.65)' : (mk.kind === 'warn') ? 'rgba(245,159,0,0.55)' : 'rgba(63,185,80,0.55)';
        var titleText = tsLabel(mk.ts) + '  ' + String(mk.label || 'event');
        markerEls.push(
          e('line', { key: 'm_' + m, x1: mx, y1: 0, x2: mx, y2: height, stroke: mcol, strokeWidth: 1, vectorEffect: 'non-scaling-stroke' },
            e('title', null, titleText)
          )
        );
        markerEls.push(
          e('circle', { key: 'mc_' + m, cx: mx, cy: 10, r: 2.2, fill: mcol, stroke: 'rgba(0,0,0,0.0)' },
            e('title', null, titleText)
          )
        );
      }
    }

    var hoverLine = null;
    var tooltip = null;
    if (hoverTs !== null && decimated.length) {
      var hx = xOfTs(hoverTs);
      hoverLine = e('line', { x1: hx, y1: 0, x2: hx, y2: height, stroke: 'rgba(255,255,255,0.20)', strokeWidth: 1, vectorEffect: 'non-scaling-stroke' });

      var lines = [tsLabel(hoverTs)];
      for (var s2 = 0; s2 < decimated.length; s2++) {
        var np = nearestPoint(decimated[s2].points, hoverTs);
        var val = np ? np[1] : null;
        lines.push(decimated[s2].name + ': ' + fmt(val, yUnit));
      }

      // include any markers at (roughly) this timestamp
      if (markers && markers.length) {
        var near = [];
        for (var mm = 0; mm < markers.length; mm++) {
          var mk2 = markers[mm];
          if (!mk2 || mk2.ts === null || mk2.ts === undefined) continue;
          if (Math.abs(mk2.ts - hoverTs) <= 1500) near.push(String(mk2.label || 'event'));
        }
        if (near.length) lines.push('events: ' + near.slice(0, 3).join(' | ') + (near.length > 3 ? '…' : ''));
      }

      tooltip = e('div', { className: 'tooltip muted' }, lines.join('\n'));
    }

    var legendItems = [];
    for (var s3 = 0; s3 < series.length; s3++) {
      (function(sx) {
        legendItems.push(
          e('label', { key: sx.key },
            e('input', {
              type: 'checkbox',
              checked: !!enabled[sx.key],
              onChange: function(ev) {
                var next = Object.assign({}, enabled);
                next[sx.key] = !!ev.target.checked;
                setEnabled(next);
              }
            }),
            e('span', { className: 'sw', style: { background: sx.color || 'rgba(255,255,255,0.35)' } }),
            e('span', null, sx.name)
          )
        );
      })(series[s3]);
    }

    return e('div', { className: 'card chartWrap' },
      e('div', { className: 'chartHead' },
        e('h2', null, title),
        subtitle ? e('span', { className: 'muted', style: { fontSize: '12px' } }, subtitle) : null
      ),
      e('div', { className: 'legend' }, legendItems),
      e('div', null,
        e('svg', {
          ref: boxRef,
          className: 'svgBox',
          viewBox: '0 0 1000 ' + String(height),
          preserveAspectRatio: 'none',
          onMouseMove: onMove,
          onMouseLeave: onLeave
        },
          e('g', null,
            zeroLine,
            yLineEls,
            markerEls,
            paths,
            hoverLine
          )
        )
      ),
      tooltip ? e('div', null, tooltip) : null
    );
  }

function EventTable(props) {
    var events = props.events || [];
    return e('div', { className: 'card' },
      e('h2', null, 'Recent events (debug)'),
      e('div', { className: 'muted', style: { fontSize: '12px', marginBottom: '8px' } }, 'Oldest → newest (limited).'),
      e('table', null,
        e('thead', null,
          e('tr', null,
            e('th', null, 'id'),
            e('th', null, 'ts_local'),
            e('th', null, 'feedIn'),
            e('th', null, 'export_costs'),
            e('th', null, 'want_pct'),
            e('th', null, 'reason')
          )
        ),
        e('tbody', null,
          events.slice(-200).map(function(ev) {
            var d = ev.data || {};
            var amber = get(d, ['sources','amber'], {}) || {};
            var dec = get(d, ['decision'], {}) || {};
            return e('tr', { key: ev.id },
              e('td', null, fmt(ev.id)),
              e('td', null, fmt(ev.ts_local)),
              e('td', null, fmt(amber.feedin_c, 'c')),
              e('td', null, String(!!dec.export_costs)),
              e('td', null, fmt(dec.want_pct, '%')),
              e('td', null, String(dec.reason || '').slice(0, 120))
            );
          })
        )
      )
    );
  }

  function Dashboard() {
    var _a = useState([]), events = _a[0], setEvents = _a[1];
    var _b = useState(null), latest = _b[0], setLatest = _b[1];
    var _c = useState('booting…'), status = _c[0], setStatus = _c[1];
    var _d = useState(null), err = _d[0], setErr = _d[1];
    var _e = useState([]), ticker = _e[0], setTicker = _e[1];
    var _f = useState('15m'), range = _f[0], setRange = _f[1];
    var _now = useState(Date.now()), nowMs = _now[0], setNowMs = _now[1];
    var _g = useState(false), showDebug = _g[0], setShowDebug = _g[1];
    var _h = useState(true), showMarkers = _h[0], setShowMarkers = _h[1];

    var esRef = useRef(null);
    var lastIdRef = useRef(0);
    var lastKeyRef = useRef('');

    useEffect(function() {
      var t = setInterval(function() { setNowMs(Date.now()); }, 1000);
      return function() { try { clearInterval(t); } catch (_) {} };
    }, []);

    function setHeaderStatus(text) {
      setStatus(text);
      var st = document.getElementById('status');
      if (st) st.textContent = text;
    }

    function pushTicker(msg) {
      var line = tsLabel(Date.now()) + '  ' + msg;
      setTicker(function(prev) { return uniqPush(prev, line, 80); });
    }

    function importantKey(ev) {
      var d = ev.data || {};
      var dec = get(d, ['decision'], {}) || {};
      var act = get(d, ['actuation'], {}) || {};
      var gw = get(d, ['sources','goodwe'], {}) || {};
      var alpha = get(d, ['sources','alpha'], {}) || {};
      var amber = get(d, ['sources','amber'], {}) || {};
      return [
        String(!!dec.export_costs),
        String(dec.want_pct),
        String(dec.want_enabled),
        String(dec.reason || ''),
        String(!!act.write_attempted),
        String(!!act.write_ok),
        String(act.write_error || ''),
        String(gw.meter_ok),
        String(gw.wifi_pct),
        String(alpha.ok),
        String(alpha.soc_pct),
        String(amber.state),
      ].join('|');
    }

    function maybeTicker(prevEv, ev) {
      if (!prevEv) return;
      var pd = prevEv.data || {};
      var d = ev.data || {};
      var pdec = get(pd, ['decision'], {}) || {};
      var dec = get(d, ['decision'], {}) || {};
      var pact = get(pd, ['actuation'], {}) || {};
      var act = get(d, ['actuation'], {}) || {};
      var psrc = get(pd, ['sources'], {}) || {};
      var src = get(d, ['sources'], {}) || {};
      var pgw = get(psrc, ['goodwe'], {}) || {};
      var gw = get(src, ['goodwe'], {}) || {};
      var palpha = get(psrc, ['alpha'], {}) || {};
      var alpha = get(src, ['alpha'], {}) || {};
      var pamber = get(psrc, ['amber'], {}) || {};
      var amber = get(src, ['amber'], {}) || {};

      function changed(a,b) { return String(a) !== String(b); }

      if (changed(pdec.reason, dec.reason)) pushTicker('reason → ' + String(dec.reason));
      if (changed(pdec.want_pct, dec.want_pct)) pushTicker('want_pct → ' + fmt(dec.want_pct, '%'));
      if (changed(pdec.export_costs, dec.export_costs)) pushTicker('export_costs → ' + String(!!dec.export_costs));
      if (act.write_attempted && !pact.write_attempted) {
        pushTicker('write attempt (want ' + fmt(dec.want_pct, '%') + ')');
      }
      if (changed(pact.write_ok, act.write_ok) && act.write_attempted) {
        if (act.write_ok) pushTicker('write OK');
        else pushTicker('write FAILED: ' + String(act.write_error || ''));
      }
      if (changed(pgw.meter_ok, gw.meter_ok)) pushTicker('GoodWe meterOK → ' + String(gw.meter_ok));
      if (changed(pgw.wifi_pct, gw.wifi_pct)) pushTicker('GoodWe wifi → ' + fmt(gw.wifi_pct, '%'));
      if (changed(palpha.ok, alpha.ok)) pushTicker('Alpha ok → ' + String(alpha.ok));
      if (changed(pamber.state, amber.state)) pushTicker('Amber state → ' + String(amber.state));
    }

    function connectSSE() {
      if (esRef.current) {
        try { esRef.current.close(); } catch (_) {}
        esRef.current = null;
      }

      var lastId = lastIdRef.current || 0;
      var url = '/api/sse/events?after_id=' + String(lastId);
      setHeaderStatus('connecting SSE (after_id=' + String(lastId) + ')');

      var es;
      try { es = new EventSource(url); }
      catch (e2) { setErr(String(e2)); setHeaderStatus('EventSource failed'); return; }

      esRef.current = es;

      es.addEventListener('event', function(msg) {
        try {
          var ev = JSON.parse(msg.data);
          if (ev && ev.id) lastIdRef.current = Math.max(lastIdRef.current, ev.id);
          setLatest(ev);
          setEvents(function(prev) {
            var next = prev.concat([ev]);
            if (next.length > 4000) next = next.slice(next.length - 4000);
            return next;
          });
          setHeaderStatus('connected (last id: ' + String(lastIdRef.current) + ')');
        } catch (e3) {
          setErr('SSE parse error: ' + e3);
        }
      });

      es.onerror = function() {
        setHeaderStatus('SSE disconnected - retrying…');
        try { es.close(); } catch (_) {}
        esRef.current = null;
        setTimeout(function() { connectSSE(); }, 2000);
      };
    }

    useEffect(function() {
      var cancelled = false;

      function boot() {
        setErr(null);
        setHeaderStatus('loading latest…');
        fetchJSON('/api/events/latest').then(function(lat) {
          if (cancelled) return;
          setLatest(lat);
          lastIdRef.current = lat.id || 0;

          // Fetch some history (by id window). We can't query by time, so do by id.
          var historyN = 1200;
          var afterId = Math.max(0, (lastIdRef.current || 0) - historyN);
          setHeaderStatus('loading history…');
          return fetchJSON('/api/events?after_id=' + String(afterId) + '&limit=' + String(historyN));
        }).then(function(res) {
          if (cancelled) return;
          if (res && res.events) {
            setEvents(res.events);
            if (res.events.length) {
              // generate initial ticker based on last item
              var last = res.events[res.events.length - 1];
              lastKeyRef.current = importantKey(last);
            }
          }
          setHeaderStatus('api ok (latest id: ' + String(lastIdRef.current) + ') - connecting SSE…');
          connectSSE();
        }).catch(function(e2) {
          if (cancelled) return;
          setErr(String(e2));
          setHeaderStatus('api failed');
        });
      }

      boot();

      return function() {
        cancelled = true;
        if (esRef.current) { try { esRef.current.close(); } catch (_) {} esRef.current = null; }
      };
    }, []);

    // update ticker on latest change
    useEffect(function() {
      if (!latest) return;
      setEvents(function(prev) {
        if (!prev.length) return prev;
        var prevEv = prev[prev.length - 1];
        if (prevEv && prevEv.id === latest.id) return prev;
        // ticker based on previous event
        try { maybeTicker(prevEv, latest); } catch (_) {}
        return prev;
      });
    }, [latest]);

    var viewEvents = useMemo(function() {
      if (!events.length) return events;
      var lastTs = get(events[events.length - 1], ['ts_epoch_ms'], null) || get(get(events[events.length - 1], ['data'], {}), ['ts_epoch_ms'], null);
      if (!lastTs) return events;

      var durMs = 15 * 60 * 1000;
      if (range === '1h') durMs = 60 * 60 * 1000;
      if (range === '6h') durMs = 6 * 60 * 60 * 1000;
      if (range === '24h') durMs = 24 * 60 * 60 * 1000;

      var minTs = lastTs - durMs;
      var out = [];
      for (var i = 0; i < events.length; i++) {
        var ev = events[i];
        var ts = get(ev, ['ts_epoch_ms'], null);
        if (!ts) ts = get(get(ev, ['data'], {}), ['ts_epoch_ms'], null);
        if (ts && ts >= minTs) out.push(ev);
      }
      return out;
    }, [events, range]);

    // push ticker for new events list changes (by comparing keys)
    useEffect(function() {
      if (!events.length) return;
      var last = events[events.length - 1];
      var k = importantKey(last);
      if (k !== lastKeyRef.current) {
        // don't spam on boot, only once we have a previous key
        if (lastKeyRef.current) {
          try { maybeTicker(events.length > 1 ? events[events.length - 2] : null, last); } catch (_) {}
        }
        lastKeyRef.current = k;
      }
    }, [events]);

    var cards = useMemo(function() {
      var ev = latest || (events.length ? events[events.length - 1] : null);
      if (!ev) return null;
      var d = ev.data || {};
      var src = get(d, ['sources'], {}) || {};
      var amber = get(src, ['amber'], {}) || {};
      var alpha = get(src, ['alpha'], {}) || {};
      var gw = get(src, ['goodwe'], {}) || {};
      var dec = get(d, ['decision'], {}) || {};
      var act = get(d, ['actuation'], {}) || {};

      var writeText = 'not attempted';
      if (act.write_attempted) writeText = act.write_ok ? 'ok' : ('failed: ' + String(act.write_error || ''));
      var wantLimit = fmt(dec.want_pct, '%');
      if (dec.target_w) wantLimit = fmt(dec.want_pct, '%') + ' (~' + fmt(dec.target_w, 'W') + ')';

      var eventTsMs = get(d, ['ts_epoch_ms'], null);

      function adjAge(age, ts) {
        var a = Number(age);
        var t = Number(ts);
        if (!isFinite(a) || !isFinite(t)) return age;
        var v = a + (nowMs - t) / 1000.0;
        if (!isFinite(v)) return age;
        if (v < 0) v = 0;
        return Math.floor(v);
      }

      var amberAge = adjAge(amber.age_s, eventTsMs);
      var alphaAge = adjAge(alpha.age_s, eventTsMs);
      var amberPill = (amber.state === 'ok') ? 'ok' : (amber.state ? 'warn' : 'warn');
      var alphaPill = (alpha.ok) ? 'ok' : 'warn';

      return e('div', { className: 'grid' },
        e('div', { className: 'card' },
          e('h2', null, 'Decision'),
          e('div', { className: 'kv' },
            e('div', null, 'export_costs'), e('div', { className: 'muted' }, String(!!dec.export_costs)),
            e('div', null, 'want_limit'), e('div', { className: 'muted' }, wantLimit),
            e('div', null, 'want_enabled'), e('div', { className: 'muted' }, fmt(dec.want_enabled)),
            e('div', null, 'reason'), e('div', { className: 'muted' }, String(dec.reason || '—')),
            e('div', null, 'write'), e('div', { className: 'muted' }, writeText)
          )
        ),
        e('div', { className: 'card' },
          e('h2', null, 'Amber'),
          e('div', { className: 'row' },
            buildPill(amberPill, String(amber.state || 'unknown')),
            e('span', { className: 'muted', style: { fontSize: '12px' } }, 'age ' + fmt(amberAge, 's'))
          ),
          e('div', { className: 'kv' },
            e('div', null, 'feedIn'), e('div', { className: 'muted' }, fmt(amber.feedin_c, 'c')),
            e('div', null, 'import'), e('div', { className: 'muted' }, fmt(amber.import_c, 'c')),
            e('div', null, 'interval_end'), e('div', { className: 'muted' }, fmt(amber.interval_end_utc))
          )
        ),
        e('div', { className: 'card' },
          e('h2', null, 'AlphaESS'),
          e('div', { className: 'row' },
            buildPill(alphaPill, alpha.ok ? 'ok' : 'not ok'),
            e('span', { className: 'muted', style: { fontSize: '12px' } }, 'age ' + fmt(alphaAge, 's'))
          ),
          e('div', { className: 'kv' },
            e('div', null, 'SOC'), e('div', { className: 'muted' }, fmt(alpha.soc_pct, '%')),
            e('div', null, 'pload'), e('div', { className: 'muted' }, fmt(alpha.pload_w, 'W')),
            e('div', null, 'pbat'), e('div', { className: 'muted' }, fmt(alpha.pbat_w, 'W')),
            e('div', null, 'pgrid'), e('div', { className: 'muted' }, fmt(alpha.pgrid_w, 'W'))
          )
        ),
        e('div', { className: 'card' },
          e('h2', null, 'GoodWe'),
          e('div', { className: 'kv' },
            e('div', null, 'gen'), e('div', { className: 'muted' }, fmt(gw.gen_w, 'W')),
            e('div', null, 'feed'), e('div', { className: 'muted' }, fmt(gw.feed_w, 'W')),
            e('div', null, 'temp'), e('div', { className: 'muted' }, fmt(gw.temp_c, 'C')),
            e('div', null, 'meterOK'), e('div', { className: 'muted' }, fmt(gw.meter_ok)),
            e('div', null, 'wifi'), e('div', { className: 'muted' }, fmt(gw.wifi_pct, '%'))
          )
        )
      );
    }, [latest, events, nowMs]);

    var charts = useMemo(function() {
      if (!viewEvents.length) return null;

      function ptsOf(path) {
        var out = [];
        for (var i = 0; i < viewEvents.length; i++) {
          var ev = viewEvents[i];
          var ts = get(ev, ['ts_epoch_ms'], null);
          if (!ts) ts = get(get(ev, ['data'], {}), ['ts_epoch_ms'], null);
          var val = get(get(ev, ['data'], {}), path, null);
          if (val === null || val === undefined) continue;
          out.push([ts, Number(val)]);
        }
        return out;
      }

      var powerGen = ptsOf(['sources','goodwe','gen_w']);
      var powerLoad = ptsOf(['sources','alpha','pload_w']);
      var powerGrid = ptsOf(['sources','alpha','pgrid_w']);
      var powerBat = ptsOf(['sources','alpha','pbat_w']);

      var priceImport = ptsOf(['sources','amber','import_c']);
      var priceFeed = ptsOf(['sources','amber','feedin_c']);

      var wantPct = ptsOf(['decision','want_pct']);
      // actual readback pct (if present)
      var actualPct = [];
      for (var i2 = 0; i2 < viewEvents.length; i2++) {
        var ev2 = viewEvents[i2];
        var ts2 = get(ev2, ['ts_epoch_ms'], null);
        if (!ts2) ts2 = get(get(ev2, ['data'], {}), ['ts_epoch_ms'], null);
        var cur = get(get(ev2, ['data'], {}), ['sources','goodwe','current_limit'], null);
        var pct = cur && cur.pct !== undefined ? Number(cur.pct) : null;
        if (pct === null || pct === undefined || isNaN(pct)) continue;
        actualPct.push([ts2, pct]);
      }

      var threshold = null;
      try {
        var last = viewEvents[viewEvents.length - 1];
        threshold = get(get(last, ['data'], {}), ['decision','export_cost_threshold_c'], null);
      } catch (_) {}
      var yLines = [];
      if (threshold !== null && threshold !== undefined) yLines.push({ y: Number(threshold), label: 'thresh ' + String(threshold) + 'c', kind: 'warn' });

      function evTs(ev) {
        var ts = get(ev, ['ts_epoch_ms'], null);
        if (!ts) ts = get(get(ev, ['data'], {}), ['ts_epoch_ms'], null);
        return ts ? Number(ts) : null;
      }

      function sev(kind) { return (kind === 'bad') ? 2 : (kind === 'warn') ? 1 : 0; }
      var markerMap = {};

      function mergeMarker(ts, kind, label) {
        if (!ts) return;
        var key = String(Math.round(ts));
        var cur = markerMap[key];
        if (!cur) {
          markerMap[key] = { ts: ts, kind: kind || 'warn', label: label || 'event' };
          return;
        }
        if (sev(kind) > sev(cur.kind)) cur.kind = kind;
        // combine labels if different
        if (label && cur.label.indexOf(label) === -1) cur.label += ' | ' + label;
      }

      for (var mi = 0; mi < viewEvents.length; mi++) {
        var evm = viewEvents[mi];
        var tsM = evTs(evm);
        if (!tsM) continue;

        var dM = evm.data || {};
        var decM = get(dM, ['decision'], {}) || {};
        var actM = get(dM, ['actuation'], {}) || {};

        if (mi > 0) {
          var prev = viewEvents[mi - 1];
          var pd = prev.data || {};
          var pdec = get(pd, ['decision'], {}) || {};

          if (String(pdec.reason) !== String(decM.reason) && decM.reason) {
            mergeMarker(tsM, 'warn', 'reason → ' + String(decM.reason));
          }
          if (String(!!pdec.export_costs) !== String(!!decM.export_costs)) {
            mergeMarker(tsM, decM.export_costs ? 'bad' : 'ok', 'export_costs → ' + String(!!decM.export_costs));
          }
        }

        if (actM.write_attempted) {
          if (actM.write_ok) mergeMarker(tsM, 'ok', 'write OK');
          else if (actM.write_error) mergeMarker(tsM, 'bad', 'write FAILED: ' + String(actM.write_error));
          else mergeMarker(tsM, 'warn', 'write attempt');
        }
      }

      var markers = Object.keys(markerMap).map(function(k) { return markerMap[k]; }).sort(function(a,b) { return a.ts - b.ts; });

      return e('div', { style: { display: 'grid', gap: '12px' } },
        e(LineChart, {
          title: 'Power flows',
          subtitle: 'GoodWe gen, Alpha load/grid/battery (' + range + ' view)',
          yUnit: 'W',
          showZero: true,
          markers: showMarkers ? markers : [],
          series: [
            { key: 'gen', name: 'gen_w', color: 'rgba(88,166,255,0.85)', points: powerGen },
            { key: 'load', name: 'pload_w', color: 'rgba(167,231,131,0.85)', points: powerLoad },
            { key: 'grid', name: 'pgrid_w', color: 'rgba(245,159,0,0.85)', points: powerGrid },
            { key: 'bat', name: 'pbat_w', color: 'rgba(248,81,73,0.85)', points: powerBat },
          ]
        }),
        e(LineChart, {
          title: 'Prices',
          subtitle: 'Amber import vs feedIn (' + range + ' view)',
          yUnit: 'c',
          showZero: true,
          yLines: yLines,
          markers: showMarkers ? markers : [],
          series: [
            { key: 'import', name: 'import_c', color: 'rgba(167,231,131,0.85)', points: priceImport },
            { key: 'feed', name: 'feedin_c', color: 'rgba(88,166,255,0.85)', points: priceFeed },
          ]
        }),
        e(LineChart, {
          title: 'Control output',
          subtitle: 'want_pct vs GoodWe readback pct (' + range + ' view)',
          yUnit: '%',
          showZero: false,
          markers: showMarkers ? markers : [],
          series: [
            { key: 'want', name: 'want_pct', color: 'rgba(245,159,0,0.85)', points: wantPct },
            { key: 'actual', name: 'actual_pct', color: 'rgba(88,166,255,0.85)', points: actualPct },
          ]
        })
      );
    }, [viewEvents, range, showMarkers]);

    return e('div', null,
      err ? e('div', { className: 'err' }, e('pre', null, String(err))) : null,
      e('div', { className: 'card' },
        e('div', { className: 'row' },
          e('span', { className: 'muted', style: { fontSize: '12px' } }, 'View range:'),
          e('select', {
            className: 'sel',
            value: range,
            onChange: function(ev) { setRange(ev.target.value); }
          },
            e('option', { value: '15m' }, '15m'),
            e('option', { value: '1h' }, '1h'),
            e('option', { value: '6h' }, '6h'),
            e('option', { value: '24h' }, '24h')
          ),
          e('button', { className: 'btn', onClick: function() { setShowDebug(!showDebug); } }, showDebug ? 'Hide debug' : 'Show debug'),
          e('label', { className: 'muted', style: { fontSize: '12px', display:'flex', gap:'6px', alignItems:'center' } },
            e('input', { type:'checkbox', checked: !!showMarkers, onChange: function(ev) { setShowMarkers(!!ev.target.checked); } }),
            e('span', null, 'Event markers')
          ),
          e('span', { className: 'muted', style: { fontSize: '12px' } }, 'events in view: ' + String(viewEvents.length))
        )
      ),
      cards,
      charts,
      e('div', { className: 'grid' },
        e('div', { className: 'card' },
          e('h2', null, 'Change ticker'),
          e('div', { className: 'muted', style: { fontSize: '12px', marginBottom: '8px' } }, 'Only logs meaningful changes (reason, want_pct, export_costs, write ok/fail, etc).'),
          e('div', { className: 'ticker' }, (ticker.length ? ticker.join('\\n') : '—'))
        ),
        e('div', { className: 'card' },
          e('h2', null, 'Live snapshot'),
          e('div', { className: 'muted', style: { fontSize: '12px', marginBottom: '8px' } }, 'Latest event (quick sanity check).'),
          e('div', { className: 'tooltip muted' }, latest ? JSON.stringify({
            id: latest.id,
            ts_local: latest.ts_local,
            export_costs: get(get(latest, ['data'], {}), ['decision','export_costs'], null),
            want_pct: get(get(latest, ['data'], {}), ['decision','want_pct'], null),
            reason: get(get(latest, ['data'], {}), ['decision','reason'], null),
            gw_gen: get(get(latest, ['data'], {}), ['sources','goodwe','gen_w'], null),
            alpha_pgrid: get(get(latest, ['data'], {}), ['sources','alpha','pgrid_w'], null),
            amber_feedin: get(get(latest, ['data'], {}), ['sources','amber','feedin_c'], null),
          }, null, 2) : '—')
        )
      ),
      showDebug ? e(EventTable, { events: events }) : null
    );
  }

  function App() {
    return e(Dashboard);
  }

  try {
    var rootEl = document.getElementById('root');
    if (!rootEl) return;
    if (ReactDOM.createRoot) {
      ReactDOM.createRoot(rootEl).render(e(App));
    } else {
      ReactDOM.render(e(App), rootEl);
    }
  } catch (e2) {
    var st = document.getElementById('status');
    if (st) st.textContent = 'render failed';
    var box = document.getElementById('bootError');
    var pre = document.getElementById('bootErrorText');
    if (box && pre) { box.style.display = 'block'; pre.textContent = String(e2); }
  }
})();"""





_CHARTJS_HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta http-equiv="Cache-Control" content="no-store" />
  <meta http-equiv="Pragma" content="no-cache" />
  <title>GoodWe Control - Chart.js</title>
  <style>
    :root {
      --bg: #0b0f14;
      --panel: #0f1723;
      --border: #202938;
      --text: #e6edf3;
      --muted: rgba(230,237,243,0.72);
    }
    body { margin: 0; background: var(--bg); color: var(--text); font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; }
    header { padding: 12px 16px; border-bottom: 1px solid var(--border); display:flex; align-items: baseline; gap: 12px; }
    header h1 { font-size: 16px; margin: 0; font-weight: 600; }
    header .status { font-size: 12px; opacity: 0.85; }
    header .build { margin-left: auto; opacity: 0.55; font-size: 11px; }
    main { padding: 16px; display: grid; gap: 12px; }
    .card { background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 12px; }
    .row { display:flex; gap: 10px; align-items: center; flex-wrap: wrap; }
    .btn { border: 1px solid var(--border); background: rgba(255,255,255,0.02); color: var(--text); border-radius: 8px; padding: 6px 10px; cursor:pointer; font-size: 12px; text-decoration:none; }
    .btn:hover { background: rgba(255,255,255,0.04); }
    .sel { border: 1px solid var(--border); background: rgba(255,255,255,0.02); color: var(--text); border-radius: 8px; padding: 6px 10px; font-size: 12px; }
    .muted { color: var(--muted); }
    canvas { width: 100% !important; height: 280px !important; }
    pre { margin: 0; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; white-space: pre; max-height: 160px; overflow:auto; }
  </style>
</head>
<body data-build="__BUILD__">
  <header>
    <h1>GoodWe Control - Chart.js</h1>
    <div class="status" id="status">loading…</div>
    <div class="build">build: __BUILD__</div>
  </header>

  <main>
    <div class="card">
      <div class="row" style="justify-content:space-between;">
        <div>
          <div class="muted" style="font-size:12px;">Mode: __MODE__</div>
          <div class="muted" style="font-size:12px;">DB: __DB_PATH__</div>
        </div>
        <div class="row">
          <a class="btn" href="/">Classic UI</a>
          <a class="btn" href="/react">React UI</a>
          <span class="muted" style="font-size:12px;">Plain HTML + Chart.js example (no build step)</span>
        </div>
      </div>

      <div class="row" style="margin-top:8px;">
        <span class="muted" style="font-size:12px;">View:</span>
        <select id="range" class="sel">
          <option value="15m">15m</option>
          <option value="1h">1h</option>
          <option value="6h">6h</option>
          <option value="24h">24h</option>
        </select>
        <span class="muted" style="font-size:12px;">(auto-updates via SSE)</span>
      </div>
    </div>

    <div class="card">
      <h2 style="font-size:13px; margin:0 0 8px; opacity:0.9;">Power flows</h2>
      <canvas id="powerChart"></canvas>
      <div class="muted" style="font-size:12px; margin-top:6px;">gen_w, pload_w, pgrid_w, pbat_w</div>
    </div>

    <div class="card">
      <h2 style="font-size:13px; margin:0 0 8px; opacity:0.9;">Change log (example)</h2>
      <pre id="log">—</pre>
    </div>

    <!-- Chart.js from CDN (example). If you need offline use, we can vendor it locally. -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <script src="/chartjs_app.js?v=__BUILD__"></script>
  </main>
</body>
</html>
"""


_CHARTJS_APP_JS = r"""(function() {
  if (!window.Chart) {
    var st = document.getElementById('status');
    if (st) st.textContent = 'Chart.js not available (CDN blocked?)';
    return;
  }

  function $(id) { return document.getElementById(id); }

  function tsLabel(ms) {
    if (!ms) return '—';
    try { return new Date(ms).toLocaleTimeString(); }
    catch (_) { return String(ms); }
  }

  function logLine(s) {
    var el = $('log');
    if (!el) return;
    if (el.textContent === '—') el.textContent = '';
    el.textContent += tsLabel(Date.now()) + '  ' + s + '\\n';
    el.scrollTop = el.scrollHeight;
    // cap log
    var lines = el.textContent.split('\\n');
    if (lines.length > 120) el.textContent = lines.slice(lines.length - 120).join('\\n');
  }

  function get(obj, path, def) {
    try {
      var cur = obj;
      for (var i = 0; i < path.length; i++) {
        if (cur === null || cur === undefined) return def;
        cur = cur[path[i]];
      }
      return (cur === undefined) ? def : cur;
    } catch (_) { return def; }
  }

  function evTs(ev) {
    var ts = get(ev, ['ts_epoch_ms'], null);
    if (!ts) ts = get(get(ev, ['data'], {}), ['ts_epoch_ms'], null);
    return ts ? Number(ts) : null;
  }

  function extract(ev) {
    var d = ev.data || {};
    return {
      ts: evTs(ev),
      gen: get(d, ['sources','goodwe','gen_w'], null),
      load: get(d, ['sources','alpha','pload_w'], null),
      grid: get(d, ['sources','alpha','pgrid_w'], null),
      bat: get(d, ['sources','alpha','pbat_w'], null),
      reason: get(d, ['decision','reason'], null),
      id: ev.id || 0
    };
  }

  function fetchJSON(url) {
    return fetch(url, { cache: 'no-store' }).then(function(r) {
      if (!r.ok) throw new Error(r.status + ' ' + r.statusText);
      return r.json();
    });
  }

  var ctx = $('powerChart').getContext('2d');
  var powerChart = new Chart(ctx, {
    type: 'line',
    data: {
      datasets: [
        { label: 'gen_w', data: [], parsing: false },
        { label: 'pload_w', data: [], parsing: false },
        { label: 'pgrid_w', data: [], parsing: false },
        { label: 'pbat_w', data: [], parsing: false },
      ]
    },
    options: {
      animation: false,
      responsive: true,
      interaction: { mode: 'nearest', intersect: false },
      plugins: {
        legend: { display: true, labels: { color: '#e6edf3' } },
        tooltip: {
          callbacks: {
            title: function(items) {
              if (!items || !items.length) return '';
              return tsLabel(items[0].parsed.x);
            }
          }
        }
      },
      scales: {
        x: {
          type: 'linear',
          ticks: {
            color: 'rgba(230,237,243,0.72)',
            callback: function(v) { return tsLabel(v); },
            maxTicksLimit: 8
          },
          grid: { color: 'rgba(255,255,255,0.07)' }
        },
        y: {
          ticks: { color: 'rgba(230,237,243,0.72)' },
          grid: { color: 'rgba(255,255,255,0.07)' }
        }
      }
    }
  });

  var lastId = 0;
  var es = null;
  var prevReason = null;

  function windowMs() {
    var sel = $('range');
    var v = sel ? sel.value : '15m';
    if (v === '1h') return 60 * 60 * 1000;
    if (v === '6h') return 6 * 60 * 60 * 1000;
    if (v === '24h') return 24 * 60 * 60 * 1000;
    return 15 * 60 * 1000;
  }

  function prune() {
    var w = windowMs();
    var now = Date.now();
    var minX = now - w;
    for (var i = 0; i < powerChart.data.datasets.length; i++) {
      var ds = powerChart.data.datasets[i];
      while (ds.data.length && ds.data[0].x < minX) ds.data.shift();
    }
  }

  function addPoint(ts, vals) {
    if (!ts) return;
    powerChart.data.datasets[0].data.push({ x: ts, y: Number(vals.gen) });
    powerChart.data.datasets[1].data.push({ x: ts, y: Number(vals.load) });
    powerChart.data.datasets[2].data.push({ x: ts, y: Number(vals.grid) });
    powerChart.data.datasets[3].data.push({ x: ts, y: Number(vals.bat) });
    prune();
    powerChart.update('none');
  }

  function setStatus(s) {
    var st = $('status');
    if (st) st.textContent = s;
  }

  function connectSSE() {
    if (es) { try { es.close(); } catch (_) {} es = null; }
    var url = '/api/sse/events?after_id=' + String(lastId);
    setStatus('connecting SSE (after_id=' + String(lastId) + ')');
    logLine('SSE connect ' + url);

    try { es = new EventSource(url); }
    catch (e2) { setStatus('EventSource failed: ' + e2); logLine('EventSource failed: ' + e2); return; }

    es.addEventListener('event', function(msg) {
      try {
        var ev = JSON.parse(msg.data);
        var x = extract(ev);
        if (x.id) lastId = Math.max(lastId, x.id);
        if (x.ts) addPoint(x.ts, x);

        if (x.reason && x.reason !== prevReason) {
          logLine('reason → ' + x.reason);
          prevReason = x.reason;
        }

        setStatus('connected (last id ' + String(lastId) + ')');
      } catch (e3) {
        logLine('SSE parse error: ' + e3);
      }
    });

    es.onerror = function() {
      setStatus('SSE disconnected - retrying…');
      logLine('SSE disconnected - retrying…');
      try { es.close(); } catch (_) {}
      es = null;
      setTimeout(connectSSE, 2000);
    };
  }

  function boot() {
    setStatus('loading latest…');
    fetchJSON('/api/events/latest').then(function(lat) {
      var x = extract(lat);
      lastId = x.id || 0;
      prevReason = x.reason;
      setStatus('loading history…');

      var historyN = 800;
      var afterId = Math.max(0, lastId - historyN);
      return fetchJSON('/api/events?after_id=' + String(afterId) + '&limit=' + String(historyN));
    }).then(function(res) {
      if (res && res.events) {
        for (var i = 0; i < res.events.length; i++) {
          var x = extract(res.events[i]);
          if (x.id) lastId = Math.max(lastId, x.id);
          if (x.ts) addPoint(x.ts, x);
        }
      }
      setStatus('api ok (latest id ' + String(lastId) + ') - connecting SSE…');
      connectSSE();
    }).catch(function(e2) {
      setStatus('boot failed: ' + e2);
      logLine('boot failed: ' + e2);
    });

    var sel = $('range');
    if (sel) sel.addEventListener('change', function() { prune(); powerChart.update('none'); });
  }

  boot();
})();"""


@app.get("/js_ping")
def js_ping() -> Response:
    # Used by the browser to confirm JS executed.
    logger.debug("js_ping")
    return Response(content=b"ok", media_type="text/plain", headers={"cache-control": "no-store"})


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    refresh_sec = _q_int(request, "refresh", "UI_REFRESH_SEC", "ui_refresh_sec", default=UI_REFRESH_SEC_DEFAULT)
    if refresh_sec is None:
        refresh_sec = UI_REFRESH_SEC_DEFAULT
    if refresh_sec < 0:
        refresh_sec = 0
    if refresh_sec > 3600:
        refresh_sec = 3600

    nojs = _q_bool(request, "nojs", "no_js", default=False)

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
            f"<td>{_html_escape(str(reason)[:120] if reason is not None else '-')}</td>"
            "</tr>"
        )

    meta_refresh = ""
    if refresh_sec and refresh_sec > 0:
        meta_refresh = f'<meta http-equiv="refresh" content="{refresh_sec}" />'

    db_err_block = ""
    if db_error:
        db_err_block = (
            '<div class="err"><h2>DB error</h2>'
            f'<pre>{_html_escape(db_error)}</pre></div>'
        )

    mode = "proxied" if UI_PROXY_API else "direct"
    status = f"server render ok (latest id {latest.get('id') if latest else 0})"
    if refresh_sec and refresh_sec > 0:
        status += f" - refresh {refresh_sec}s"
    else:
        status += " - SSE mode"

    refresh_label = "off (SSE live)" if refresh_sec == 0 else f"{refresh_sec}s (server refresh)"
    script_tag = "" if nojs else f'<script src="/app.js?v={BUILD_ID}"></script>'

    html_doc = _HTML_TEMPLATE
    html_doc = html_doc.replace("__META_REFRESH__", meta_refresh)
    html_doc = html_doc.replace("__BUILD__", BUILD_ID)
    html_doc = html_doc.replace("__MODE__", mode)
    html_doc = html_doc.replace("__STATUS__", _html_escape(status))
    html_doc = html_doc.replace("__DB_PATH__", _html_escape(DB_PATH))
    html_doc = html_doc.replace("__REFRESH_LABEL__", _html_escape(refresh_label))
    html_doc = html_doc.replace("__DB_ERROR__", db_err_block)
    html_doc = html_doc.replace("__ROWS__", "".join(rows_html) if rows_html else "")
    html_doc = html_doc.replace("__SCRIPT_TAG__", script_tag)

    for k, v in display.items():
        html_doc = html_doc.replace(f"__{k}__", _html_escape(v))

    return HTMLResponse(content=html_doc, headers={"cache-control": "no-store"})

@app.get("/react", response_class=HTMLResponse)
def react_index(request: Request) -> HTMLResponse:
    # Experimental React-based UI (served without a build step; React is loaded from a CDN).
    mode = "proxied" if UI_PROXY_API else "direct"
    html_doc = _REACT_HTML_TEMPLATE
    html_doc = html_doc.replace("__BUILD__", BUILD_ID)
    html_doc = html_doc.replace("__MODE__", mode)
    html_doc = html_doc.replace("__DB_PATH__", _html_escape(DB_PATH))
    html_doc = html_doc.replace("__API_UPSTREAM__", _html_escape(API_UPSTREAM))
    return HTMLResponse(content=html_doc, headers={"cache-control": "no-store"})


@app.get("/react_app.js")
def react_app_js() -> Response:
    return Response(
        content=_REACT_APP_JS,
        media_type="application/javascript; charset=utf-8",
        headers={"cache-control": "no-store"},
    )





@app.get("/chartjs", response_class=HTMLResponse)
def chartjs_index(request: Request) -> HTMLResponse:
    # Plain HTML + Chart.js example UI (auto-updates via SSE).
    mode = "proxied" if UI_PROXY_API else "direct"
    html_doc = _CHARTJS_HTML_TEMPLATE
    html_doc = html_doc.replace("__BUILD__", BUILD_ID)
    html_doc = html_doc.replace("__MODE__", mode)
    html_doc = html_doc.replace("__DB_PATH__", _html_escape(DB_PATH))
    return HTMLResponse(content=html_doc, headers={"cache-control": "no-store"})


@app.get("/chartjs_app.js")
def chartjs_app_js() -> Response:
    return Response(
        content=_CHARTJS_APP_JS,
        media_type="application/javascript; charset=utf-8",
        headers={"cache-control": "no-store"},
    )

@app.get("/app.js")
def app_js() -> Response:
    return Response(
        content=_JS_TEMPLATE,
        media_type="application/javascript; charset=utf-8",
        headers={"cache-control": "no-store"},
    )


if __name__ == "__main__":
    import uvicorn

    # Rotating file logs (LOG_DIR/ui.log) + optional stdout
    debug_default = _env("DEBUG", "").strip().lower() in ("1", "true", "yes", "y", "on")
    setup_logging("ui", debug_default=debug_default)

    host = _env("UI_HOST", "0.0.0.0")
    port = _env_int("UI_PORT", 8000)

    logger.info("[start] ui host=%s port=%s api_upstream=%s ui_proxy_api=%s db=%s", host, port, API_UPSTREAM, UI_PROXY_API, DB_PATH)

    uvicorn.run(app, host=host, port=port, log_level="info", log_config=None)
