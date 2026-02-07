#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


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


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row

    # Better concurrency characteristics for a read-heavy UI process.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL UNIQUE,
            ts_utc TEXT,
            ts_local TEXT,
            ts_epoch_ms INTEGER,
            host TEXT,
            pid INTEGER,
            loop INTEGER,
            export_costs INTEGER,
            want_pct INTEGER,
            want_enabled INTEGER,
            reason TEXT,
            data_json TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_ts_epoch_ms ON events(ts_epoch_ms)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS event_notes (
            event_id TEXT PRIMARY KEY,
            note TEXT,
            updated_ts_utc TEXT
        )
        """
    )

    conn.commit()
    return conn


def _extract_columns(event: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    decision = event.get("decision") if isinstance(event.get("decision"), dict) else {}
    export_costs = decision.get("export_costs")
    want_pct = decision.get("want_pct")
    want_enabled = decision.get("want_enabled")
    reason = decision.get("reason")

    cols = {
        "event_id": event.get("event_id"),
        "ts_utc": event.get("ts_utc"),
        "ts_local": event.get("ts_local"),
        "ts_epoch_ms": event.get("ts_epoch_ms"),
        "host": event.get("host"),
        "pid": event.get("pid"),
        "loop": event.get("loop"),
        "export_costs": 1 if export_costs else 0 if export_costs is not None else None,
        "want_pct": int(want_pct) if want_pct is not None else None,
        "want_enabled": int(want_enabled) if want_enabled is not None else None,
        "reason": str(reason) if reason is not None else None,
    }

    payload = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
    return cols, payload


def _ingest_one(conn: sqlite3.Connection, json_path: Path) -> bool:
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            event = json.load(f)
        if not isinstance(event, dict):
            return False

        cols, payload = _extract_columns(event)
        if not cols.get("event_id"):
            return False

        cur = conn.execute(
            """
            INSERT OR IGNORE INTO events(
                event_id, ts_utc, ts_local, ts_epoch_ms, host, pid, loop,
                export_costs, want_pct, want_enabled, reason, data_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                cols.get("event_id"),
                cols.get("ts_utc"),
                cols.get("ts_local"),
                cols.get("ts_epoch_ms"),
                cols.get("host"),
                cols.get("pid"),
                cols.get("loop"),
                cols.get("export_costs"),
                cols.get("want_pct"),
                cols.get("want_enabled"),
                cols.get("reason"),
                payload,
            ),
        )
        conn.commit()
        return cur.rowcount == 1
    except Exception:
        return False


def _move_processed(src: Path, processed_dir: Path) -> None:
    processed_dir.mkdir(parents=True, exist_ok=True)
    dst = processed_dir / src.name
    # If dst exists (duplicate), append .dup + epoch
    if dst.exists():
        dst = processed_dir / f"{src.stem}.dup{int(time.time())}{src.suffix}"
    shutil.move(str(src), str(dst))


def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest control-loop JSON events into SQLite")
    ap.add_argument("--export-dir", default=_env("EVENT_EXPORT_DIR", "export/events"))
    ap.add_argument("--processed-dir", default=_env("INGEST_PROCESSED_DIR", "export/processed"))
    ap.add_argument("--db", default=_env("INGEST_DB_PATH", "data/events.sqlite3"))
    ap.add_argument("--poll", type=float, default=float(_env("INGEST_POLL_SEC", "1")))
    ap.add_argument("--delete", action="store_true", default=_env_bool("INGEST_DELETE_AFTER_IMPORT", False))
    args = ap.parse_args()

    export_dir = Path(args.export_dir).expanduser()
    processed_dir = Path(args.processed_dir).expanduser()
    db_path = Path(args.db).expanduser()

    export_dir.mkdir(parents=True, exist_ok=True)
    conn = _init_db(db_path)

    print(f"[ingest] export_dir={export_dir} processed_dir={processed_dir} db={db_path} delete={bool(args.delete)}")

    try:
        while True:
            # Only ingest stable .json files (control.py writes via atomic rename).
            paths = sorted(p for p in export_dir.glob("*.json") if p.is_file())
            if not paths:
                time.sleep(max(0.2, float(args.poll)))
                continue

            for path in paths:
                inserted = _ingest_one(conn, path)
                if inserted:
                    if args.delete:
                        try:
                            path.unlink(missing_ok=True)
                        except Exception:
                            pass
                    else:
                        try:
                            _move_processed(path, processed_dir)
                        except Exception:
                            # If move fails, don't reprocess forever: rename in-place.
                            try:
                                path.rename(path.with_suffix(path.suffix + ".done"))
                            except Exception:
                                pass
                else:
                    # If it's malformed, quarantine it so we don't spin on it.
                    try:
                        _move_processed(path, processed_dir / "bad")
                    except Exception:
                        pass

            time.sleep(max(0.1, float(args.poll)))

    except KeyboardInterrupt:
        print("[ingest] stopping")
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
