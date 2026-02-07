#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Dict, Generator, Optional

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse


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


def _db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # For read-only API workloads, WAL is important when ingest is writing.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    d = dict(row)
    try:
        d["data"] = json.loads(d.pop("data_json"))
    except Exception:
        d["data"] = None
        d.pop("data_json", None)
    return d


DB_PATH = _env("API_DB_PATH", _env("INGEST_DB_PATH", "data/events.sqlite3"))
SSE_POLL_SEC = float(_env("API_SSE_POLL_SEC", "0.5"))

# CORS: comma-separated origins. Example:
# API_CORS_ORIGINS=http://localhost:8000,http://192.168.1.50:8000
CORS_ORIGINS_RAW = _env("API_CORS_ORIGINS", "")
CORS_ORIGINS = [o.strip() for o in CORS_ORIGINS_RAW.split(",") if o.strip()]

app = FastAPI(title="GoodWe Control Events API")

if CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_credentials=False,
        allow_methods=["*"]
        ,
        allow_headers=["*"],
    )


@app.get("/api/health")
def health() -> Dict[str, Any]:
    try:
        conn = _db_connect(DB_PATH)
        try:
            cur = conn.execute("SELECT 1")
            cur.fetchone()
        finally:
            conn.close()
        return {"ok": True, "db": DB_PATH}
    except Exception as e:
        return {"ok": False, "db": DB_PATH, "error": str(e)}


@app.get("/api/events/latest")
def latest_event() -> JSONResponse:
    conn = _db_connect(DB_PATH)
    try:
        row = conn.execute("SELECT * FROM events ORDER BY id DESC LIMIT 1").fetchone()
        if not row:
            return JSONResponse(status_code=404, content={"error": "no events"})
        return JSONResponse(content=_row_to_dict(row))
    finally:
        conn.close()


@app.get("/api/events")
def list_events(
    limit: int = Query(200, ge=1, le=5000),
    after_id: int = Query(0, ge=0),
) -> JSONResponse:
    conn = _db_connect(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT * FROM events WHERE id > ? ORDER BY id ASC LIMIT ?", (int(after_id), int(limit))
        ).fetchall()
        return JSONResponse(content={"events": [_row_to_dict(r) for r in rows]})
    finally:
        conn.close()


@app.get("/api/events/{event_row_id}")
def get_event(event_row_id: int) -> JSONResponse:
    conn = _db_connect(DB_PATH)
    try:
        row = conn.execute("SELECT * FROM events WHERE id = ?", (int(event_row_id),)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        return JSONResponse(content=_row_to_dict(row))
    finally:
        conn.close()


@app.get("/api/events/by-event-id/{event_id}")
def get_event_by_event_id(event_id: str) -> JSONResponse:
    conn = _db_connect(DB_PATH)
    try:
        row = conn.execute("SELECT * FROM events WHERE event_id = ?", (str(event_id),)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        return JSONResponse(content=_row_to_dict(row))
    finally:
        conn.close()


@app.patch("/api/events/{event_row_id}/note")
def set_note(event_row_id: int, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    note = str(payload.get("note") or "").strip()
    conn = _db_connect(DB_PATH)
    try:
        row = conn.execute("SELECT event_id FROM events WHERE id = ?", (int(event_row_id),)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        event_id = row["event_id"]
        conn.execute(
            "INSERT INTO event_notes(event_id, note, updated_ts_utc) VALUES(?,?,?) "
            "ON CONFLICT(event_id) DO UPDATE SET note=excluded.note, updated_ts_utc=excluded.updated_ts_utc",
            (event_id, note, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
        )
        conn.commit()
        return {"ok": True, "event_id": event_id, "note": note}
    finally:
        conn.close()


@app.get("/api/events/{event_row_id}/note")
def get_note(event_row_id: int) -> Dict[str, Any]:
    conn = _db_connect(DB_PATH)
    try:
        row = conn.execute("SELECT event_id FROM events WHERE id = ?", (int(event_row_id),)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        event_id = row["event_id"]
        note_row = conn.execute(
            "SELECT note, updated_ts_utc FROM event_notes WHERE event_id = ?", (event_id,)
        ).fetchone()
        if not note_row:
            return {"event_id": event_id, "note": "", "updated_ts_utc": None}
        return {"event_id": event_id, "note": note_row["note"], "updated_ts_utc": note_row["updated_ts_utc"]}
    finally:
        conn.close()


@app.delete("/api/events/{event_row_id}")
def delete_event(event_row_id: int) -> Dict[str, Any]:
    conn = _db_connect(DB_PATH)
    try:
        row = conn.execute("SELECT event_id FROM events WHERE id = ?", (int(event_row_id),)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        event_id = row["event_id"]
        conn.execute("DELETE FROM event_notes WHERE event_id = ?", (event_id,))
        conn.execute("DELETE FROM events WHERE id = ?", (int(event_row_id),))
        conn.commit()
        return {"ok": True, "id": int(event_row_id), "event_id": event_id}
    finally:
        conn.close()


@app.get("/api/sse/events")
def sse_events(after_id: int = Query(0, ge=0)) -> StreamingResponse:
    def gen() -> Generator[str, None, None]:
        last_id = int(after_id)
        last_hb = time.time()

        while True:
            try:
                conn = _db_connect(DB_PATH)
                try:
                    rows = conn.execute(
                        "SELECT * FROM events WHERE id > ? ORDER BY id ASC LIMIT 500", (last_id,)
                    ).fetchall()
                finally:
                    conn.close()

                if rows:
                    for r in rows:
                        d = _row_to_dict(r)
                        last_id = int(d.get("id") or last_id)
                        yield "event: event\n" + "data: " + json.dumps(d, separators=(",", ":")) + "\n\n"
                    continue

                # Heartbeat every ~15 seconds so proxies don't close the stream.
                if time.time() - last_hb >= 15.0:
                    yield ": hb\n\n"
                    last_hb = time.time()

                time.sleep(max(0.1, float(SSE_POLL_SEC)))

            except GeneratorExit:
                return
            except Exception:
                # Avoid tight looping on DB errors.
                time.sleep(1.0)

    return StreamingResponse(gen(), media_type="text/event-stream")


if __name__ == "__main__":
    import uvicorn

    host = _env("API_HOST", "127.0.0.1")
    port = _env_int("API_PORT", 8001)

    uvicorn.run(app, host=host, port=port, log_level="info")
