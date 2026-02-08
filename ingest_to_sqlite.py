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

import logging

from logging_setup import setup_logging


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


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

# Logging
DEBUG = _env_bool("DEBUG", False)
LOG = setup_logging("ingest", debug_default=DEBUG)


def _clamp_float(v: Optional[float]) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _clamp_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _q(v: Optional[float], step: float) -> Optional[float]:
    """Quantize a float to the nearest step (for change-detection)."""
    if v is None:
        return None
    try:
        if step <= 0:
            return float(v)
        return round(float(v) / step) * step
    except Exception:
        return None


def _get(d: Any, *path: str) -> Any:
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _event_signature(
    event: Dict[str, Any],
    *,
    mode: str,
    watt_step: int,
    price_step: float,
    soc_step: float,
) -> str:
    """Build a stable signature for de-duplicating near-identical events.

    Important: this signature intentionally ignores volatile fields like timestamps, pid, loop, and age_s.

    Modes:
        - telemetry: includes quantized power/telemetry (more rows, more detail)
        - decision: focuses on decision/limits + prices + SOC (fewer rows)
        - decision_only: focuses on decision/limits + prices (fewest rows)
    """
    m = (mode or "telemetry").strip().lower()

    # Core decision fields (do NOT include 'reason' because it can contain volatile numbers)
    decision_sig: Dict[str, Any] = {
        "export_costs": _get(event, "decision", "export_costs"),
        "want_pct": _clamp_int(_get(event, "decision", "want_pct")),
        "want_enabled": _clamp_int(_get(event, "decision", "want_enabled")),
        "target_w": _clamp_int(_get(event, "decision", "target_w")),
        "export_cost_threshold_c": _clamp_float(_get(event, "decision", "export_cost_threshold_c")),
    }

    # Actuation fields: keep these so a write attempt always gets recorded
    act_sig: Dict[str, Any] = {
        "write_attempted": _get(event, "actuation", "write_attempted"),
        "write_ok": _get(event, "actuation", "write_ok"),
        "write_error": _get(event, "actuation", "write_error"),
    }

    # Prices + interval boundary are important and change relatively infrequently
    amber_sig: Dict[str, Any] = {
        "state": _get(event, "sources", "amber", "state"),
        "interval_end_utc": _get(event, "sources", "amber", "interval_end_utc"),
        "import_c": _q(_clamp_float(_get(event, "sources", "amber", "import_c")), price_step),
        "feedin_c": _q(_clamp_float(_get(event, "sources", "amber", "feedin_c")), price_step),
    }

    # Current inverter limit changes are meaningful, keep them
    goodwe_limit_sig: Dict[str, Any] = {
        "pwr_limit_fn": _get(event, "sources", "goodwe", "pwr_limit_fn"),
        "profile": _get(event, "sources", "goodwe", "profile"),
        "cur_enabled": _clamp_int(_get(event, "sources", "goodwe", "current_limit", "enabled")),
        "cur_pct": _clamp_int(_get(event, "sources", "goodwe", "current_limit", "pct")),
        "cur_pct10": _clamp_int(_get(event, "sources", "goodwe", "current_limit", "pct10")),
    }

    alpha_base_sig: Dict[str, Any] = {
        "enabled": _get(event, "sources", "alpha", "enabled"),
        "ok": _get(event, "sources", "alpha", "ok"),
        "sys_sn": _get(event, "sources", "alpha", "sys_sn"),
        "batt_state": _get(event, "sources", "alpha", "batt_state"),
    }

    sig: Dict[str, Any] = {
        "decision": decision_sig,
        "actuation": act_sig,
        "sources": {
            "amber": amber_sig,
            "goodwe": goodwe_limit_sig,
            "alpha": alpha_base_sig,
        },
    }

    # SOC is useful but changes relatively slowly; include for 'decision' and 'telemetry'
    if m in ("telemetry", "decision"):
        sig["sources"]["alpha"]["soc_pct"] = _q(_clamp_float(_get(event, "sources", "alpha", "soc_pct")), soc_step)

    if m == "telemetry":
        ws = float(max(1, watt_step))
        sig["sources"]["goodwe"].update(
            {
                "gen_w": _q(_clamp_float(_get(event, "sources", "goodwe", "gen_w")), ws),
                "feed_w": _q(_clamp_float(_get(event, "sources", "goodwe", "feed_w")), ws),
                "pv_est_w": _q(_clamp_float(_get(event, "sources", "goodwe", "pv_est_w")), ws),
                "temp_c": _q(_clamp_float(_get(event, "sources", "goodwe", "temp_c")), 0.2),
                "wifi_pct": _clamp_int(_get(event, "sources", "goodwe", "wifi_pct")),
                "meter_ok": _get(event, "sources", "goodwe", "meter_ok"),
            }
        )
        sig["sources"]["alpha"].update(
            {
                "pload_w": _q(_clamp_float(_get(event, "sources", "alpha", "pload_w")), ws),
                "pbat_w": _q(_clamp_float(_get(event, "sources", "alpha", "pbat_w")), ws),
                "pgrid_w": _q(_clamp_float(_get(event, "sources", "alpha", "pgrid_w")), ws),
                "charge_w": _q(_clamp_float(_get(event, "sources", "alpha", "charge_w")), ws),
                "discharge_w": _q(_clamp_float(_get(event, "sources", "alpha", "discharge_w")), ws),
            }
        )

    return json.dumps(sig, ensure_ascii=False, sort_keys=True, separators=(",", ":"))



def _wal_path(db_path: Path) -> Path:
    # SQLite WAL file naming is "<db>-wal"
    return Path(str(db_path) + "-wal")


def _init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Keep one writer connection open for performance, but ensure WAL checkpoints happen.
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row

    # Better concurrency characteristics for a read-heavy UI process.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Reduce the surprise of a tiny main DB file with a large -wal file.
    # Default SQLite autocheckpoint is 1000 pages (~4MB at 4KB pages). We use a smaller default.
    conn.execute(f"PRAGMA wal_autocheckpoint={_env_int('INGEST_WAL_AUTOCHECKPOINT_PAGES', 200)}")

    # Avoid transient lock failures if the API/UI hit the DB at the same time.
    conn.execute(f"PRAGMA busy_timeout={_env_int('INGEST_BUSY_TIMEOUT_MS', 5000)}")

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
            slimmed INTEGER DEFAULT 0,
            data_json TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_ts_epoch_ms ON events(ts_epoch_ms)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id)")

    try:
        conn.execute("ALTER TABLE events ADD COLUMN slimmed INTEGER DEFAULT 0")
    except Exception:
        # older DB already has the column
        pass
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_slimmed ON events(slimmed)")

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


def _load_last_signature(
    conn: sqlite3.Connection,
    *,
    mode: str,
    watt_step: int,
    price_step: float,
    soc_step: float,
) -> Tuple[Optional[str], int]:
    """Prime the de-duplication state from the last row in the DB."""
    try:
        row = conn.execute(
            "SELECT data_json, ts_epoch_ms FROM events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not row:
            return None, int(time.time() * 1000.0)
        last_ms = int(row["ts_epoch_ms"]) if row["ts_epoch_ms"] is not None else int(time.time() * 1000.0)
        data_json = row["data_json"]
        ev = json.loads(data_json)
        if not isinstance(ev, dict):
            return None, last_ms
        return _event_signature(ev, mode=mode, watt_step=watt_step, price_step=price_step, soc_step=soc_step), last_ms
    except Exception:
        return None, int(time.time() * 1000.0)


def _ingest_one(
    conn: sqlite3.Connection,
    json_path: Path,
    *,
    dedupe_enabled: bool,
    dedupe_force_ms: int,
    dedupe_mode: str,
    dedupe_watt_step: int,
    dedupe_price_step: float,
    dedupe_soc_step: float,
    dedupe_last_sig: Optional[str],
    dedupe_last_insert_ms: int,
) -> Tuple[bool, Optional[str], int, bool]:
    """Ingest one JSON file.

    Returns:
        (handled_ok, new_last_sig, new_last_insert_ms, inserted)
    """
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            event = json.load(f)
        if not isinstance(event, dict):
            return False, dedupe_last_sig, dedupe_last_insert_ms, False

        cols, payload = _extract_columns(event)
        if not cols.get("event_id"):
            return False, dedupe_last_sig, dedupe_last_insert_ms, False

        # Optional de-duplication: skip near-identical events to reduce DB growth.
        event_ms = cols.get("ts_epoch_ms")
        if event_ms is None:
            event_ms = int(time.time() * 1000.0)
        else:
            try:
                event_ms = int(event_ms)
            except Exception:
                event_ms = int(time.time() * 1000.0)

        if dedupe_enabled:
            sig = _event_signature(
                event,
                mode=dedupe_mode,
                watt_step=int(max(1, dedupe_watt_step)),
                price_step=float(dedupe_price_step),
                soc_step=float(dedupe_soc_step),
            )
            if sig == dedupe_last_sig and (int(event_ms) - int(dedupe_last_insert_ms)) < int(dedupe_force_ms):
                if DEBUG:
                    LOG.debug(
                        f"[ingest] dedupe skip id={cols.get('event_id')} loop={cols.get('loop')} "
                        f"delta_ms={int(event_ms) - int(dedupe_last_insert_ms)}"
                    )
                return True, dedupe_last_sig, int(dedupe_last_insert_ms), False

        # Treat duplicates as success so we still move/delete the file.
        conn.execute(
            """
            INSERT INTO events(
                event_id, ts_utc, ts_local, ts_epoch_ms, host, pid, loop,
                export_costs, want_pct, want_enabled, reason, data_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO NOTHING
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
        # Update de-dupe state after a successful insert.
        if dedupe_enabled:
            try:
                dedupe_last_sig = _event_signature(
                    event,
                    mode=dedupe_mode,
                    watt_step=int(max(1, dedupe_watt_step)),
                    price_step=float(dedupe_price_step),
                    soc_step=float(dedupe_soc_step),
                )
            except Exception:
                pass
            dedupe_last_insert_ms = int(event_ms)
        return True, dedupe_last_sig, int(dedupe_last_insert_ms), True
    except Exception:
        return False, dedupe_last_sig, dedupe_last_insert_ms, False


def _move_processed(src: Path, processed_dir: Path) -> None:
    processed_dir.mkdir(parents=True, exist_ok=True)
    dst = processed_dir / src.name
    # If dst exists (duplicate), append .dup + epoch
    if dst.exists():
        dst = processed_dir / f"{src.stem}.dup{int(time.time())}{src.suffix}"
    shutil.move(str(src), str(dst))


def _maybe_checkpoint(conn: sqlite3.Connection, db_path: Path, truncate_mb: int) -> None:
    wal = _wal_path(db_path)
    wal_bytes = wal.stat().st_size if wal.exists() else 0

    # TRUNCATE keeps the -wal file from growing indefinitely and makes the main DB reflect changes.
    # If there are active readers, TRUNCATE may not fully truncate; that's fine.
    mode = "TRUNCATE" if wal_bytes >= int(truncate_mb) * 1024 * 1024 else "PASSIVE"
    try:
        conn.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()
    except Exception:
        # Never crash the ingester due to checkpoint issues.
        pass


def _slim_event_json(data_json: str) -> Tuple[str, bool]:
    """Remove large raw payloads from the stored event JSON.

    Returns (new_json, changed).
    """
    try:
        ev = json.loads(data_json)
        if not isinstance(ev, dict):
            return data_json, False

        sources = ev.get("sources")
        changed = False

        if isinstance(sources, dict):
            amber = sources.get("amber")
            if isinstance(amber, dict):
                if "raw_prices" in amber:
                    amber.pop("raw_prices", None)
                    changed = True
                if "raw_usage" in amber:
                    amber.pop("raw_usage", None)
                    changed = True

            alpha = sources.get("alpha")
            if isinstance(alpha, dict):
                if "raw" in alpha:
                    alpha.pop("raw", None)
                    changed = True

        if not changed:
            return data_json, False

        return json.dumps(ev, ensure_ascii=False, separators=(",", ":")), True
    except Exception:
        return data_json, False


def _retention_run(
    conn: sqlite3.Connection,
    *,
    full_hours: int,
    delete_after_days: int,
    slim_batch: int,
) -> None:
    """Apply retention to the events table.

    - For rows older than full_hours: strip heavy raw fields, mark slimmed=1
    - Optionally delete rows older than delete_after_days
    """
    now_ms = int(time.time() * 1000.0)

    # Slim older rows
    if full_hours > 0:
        cutoff_ms = now_ms - int(full_hours) * 3600 * 1000
        rows = conn.execute(
            """
            SELECT id, data_json
            FROM events
            WHERE ts_epoch_ms IS NOT NULL
              AND ts_epoch_ms < ?
              AND (slimmed IS NULL OR slimmed = 0)
            ORDER BY id
            LIMIT ?
            """,
            (int(cutoff_ms), int(max(1, slim_batch))),
        ).fetchall()

        if rows:
            for r in rows:
                rid = int(r["id"])
                data_json = r["data_json"]
                new_json, changed = _slim_event_json(data_json)
                if changed:
                    conn.execute(
                        "UPDATE events SET data_json = ?, slimmed = 1 WHERE id = ?",
                        (new_json, rid),
                    )
                else:
                    conn.execute("UPDATE events SET slimmed = 1 WHERE id = ?", (rid,))
            conn.commit()

    # Delete very old rows
    if delete_after_days > 0:
        del_cutoff_ms = now_ms - int(delete_after_days) * 86400 * 1000
        # Delete notes for those events too
        try:
            conn.execute(
                """
                DELETE FROM event_notes
                WHERE event_id IN (
                    SELECT event_id FROM events
                    WHERE ts_epoch_ms IS NOT NULL AND ts_epoch_ms < ?
                )
                """,
                (int(del_cutoff_ms),),
            )
        except Exception:
            pass

        conn.execute(
            "DELETE FROM events WHERE ts_epoch_ms IS NOT NULL AND ts_epoch_ms < ?",
            (int(del_cutoff_ms),),
        )
        conn.commit()


def _prune_processed_dir(processed_dir: Path, retention_days: int) -> None:
    if retention_days <= 0:
        return
    try:
        if not processed_dir.exists():
            return
        cutoff = time.time() - (int(retention_days) * 86400)
        for p in processed_dir.rglob("*"):
            if not p.is_file():
                continue
            try:
                st = p.stat()
                if st.st_mtime < cutoff:
                    p.unlink(missing_ok=True)
            except Exception:
                pass
    except Exception:
        pass


def _vacuum_db(db_path: Path) -> None:
    # VACUUM must run outside any open transaction.
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    try:
        conn.execute("PRAGMA journal_mode=DELETE")
    except Exception:
        pass
    try:
        conn.execute("VACUUM")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest control-loop JSON events into SQLite")
    ap.add_argument("--export-dir", default=_env("EVENT_EXPORT_DIR", "export/events"))
    ap.add_argument("--processed-dir", default=_env("INGEST_PROCESSED_DIR", "export/processed"))
    ap.add_argument("--db", default=_env("INGEST_DB_PATH", "data/events.sqlite3"))
    ap.add_argument("--poll", type=float, default=float(_env("INGEST_POLL_SEC", "1")))
    ap.add_argument("--delete", action="store_true", default=_env_bool("INGEST_DELETE_AFTER_IMPORT", False))
    ap.add_argument("--vacuum", action="store_true", default=False, help="Run VACUUM on the DB and exit")
    args = ap.parse_args()

    export_dir = Path(args.export_dir).expanduser()
    processed_dir = Path(args.processed_dir).expanduser()
    db_path = Path(args.db).expanduser()

    export_dir.mkdir(parents=True, exist_ok=True)
    if args.vacuum:
        LOG.info(f"[ingest] vacuum db={db_path}")
        _vacuum_db(db_path)
        return 0
    conn = _init_db(db_path)

    ckpt_every = _env_float("INGEST_WAL_CHECKPOINT_SEC", 30.0)
    truncate_mb = _env_int("INGEST_WAL_TRUNCATE_MB", 16)
    last_ckpt = time.monotonic()

    retention_enabled = _env_bool("INGEST_RETENTION_ENABLED", True)
    retention_full_hours = _env_int("INGEST_RETENTION_FULL_HOURS", 48)
    retention_delete_after_days = _env_int("INGEST_RETENTION_DELETE_AFTER_DAYS", 30)
    retention_every_sec = _env_float("INGEST_RETENTION_EVERY_SEC", 300.0)
    retention_slim_batch = _env_int("INGEST_RETENTION_SLIM_BATCH", 500)
    processed_retention_days = _env_int("INGEST_PROCESSED_RETENTION_DAYS", 0)

    dedupe_enabled = _env_bool("INGEST_DEDUP_ENABLED", False)
    dedupe_force_sec = _env_float("INGEST_DEDUP_FORCE_SEC", 30.0)
    dedupe_mode = _env("INGEST_DEDUP_MODE", "telemetry")
    dedupe_watt_step = _env_int("INGEST_DEDUP_WATT_STEP", 10)
    dedupe_price_step = _env_float("INGEST_DEDUP_PRICE_STEP", 0.001)
    dedupe_soc_step = _env_float("INGEST_DEDUP_SOC_STEP", 0.1)

    dedupe_last_sig: Optional[str] = None
    dedupe_last_insert_ms: int = int(time.time() * 1000.0)
    if dedupe_enabled:
        dedupe_last_sig, dedupe_last_insert_ms = _load_last_signature(
            conn,
            mode=dedupe_mode,
            watt_step=dedupe_watt_step,
            price_step=dedupe_price_step,
            soc_step=dedupe_soc_step,
        )

    last_retention = time.monotonic()

    # Periodic stats to confirm de-duplication effectiveness
    stats_every = _env_float("INGEST_STATS_EVERY_SEC", 60.0)
    stats_scanned = 0
    stats_inserted = 0
    stats_skipped = 0
    stats_bad = 0
    last_stats = time.monotonic()


    LOG.info(
        f"[ingest] export_dir={export_dir} processed_dir={processed_dir} db={db_path} "
        f"delete={bool(args.delete)} ckpt={ckpt_every}s truncate_mb={truncate_mb} "
        f"dedupe={'on' if dedupe_enabled else 'off'} force_sec={dedupe_force_sec} mode={dedupe_mode} "
        f"watt_step={dedupe_watt_step} price_step={dedupe_price_step} soc_step={dedupe_soc_step} "
        f"retention={'on' if retention_enabled else 'off'} full_h={retention_full_hours} "
        f"del_d={retention_delete_after_days} ret_every={retention_every_sec}s slim_batch={retention_slim_batch} "
        f"proc_ret_d={processed_retention_days}"
    )

    try:
        while True:
            # Only ingest stable .json files (control.py writes via atomic rename).
            paths = sorted(p for p in export_dir.glob("*.json") if p.is_file())
            if not paths:
                # Still checkpoint occasionally so the main DB stays up to date.
                now = time.monotonic()
                if ckpt_every > 0 and (now - last_ckpt) >= ckpt_every:
                    _maybe_checkpoint(conn, db_path, truncate_mb)
                    last_ckpt = now

                if retention_enabled and retention_every_sec > 0 and (now - last_retention) >= retention_every_sec:
                    try:
                        _retention_run(
                            conn,
                            full_hours=retention_full_hours,
                            delete_after_days=retention_delete_after_days,
                            slim_batch=retention_slim_batch,
                        )
                    except Exception:
                        pass
                    try:
                        _prune_processed_dir(processed_dir, processed_retention_days)
                    except Exception:
                        pass
                    last_retention = now
                time.sleep(max(0.2, float(args.poll)))
                continue

            for path in paths:
                stats_scanned += 1
                ok, dedupe_last_sig, dedupe_last_insert_ms, _inserted = _ingest_one(
                    conn,
                    path,
                    dedupe_enabled=dedupe_enabled,
                    dedupe_force_ms=int(max(0.0, float(dedupe_force_sec)) * 1000.0),
                    dedupe_mode=dedupe_mode,
                    dedupe_watt_step=dedupe_watt_step,
                    dedupe_price_step=dedupe_price_step,
                    dedupe_soc_step=dedupe_soc_step,
                    dedupe_last_sig=dedupe_last_sig,
                    dedupe_last_insert_ms=int(dedupe_last_insert_ms),
                )
                if ok:
                    if _inserted:
                        stats_inserted += 1
                    else:
                        stats_skipped += 1
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
                    stats_bad += 1
                    # If it's malformed, quarantine it so we don't spin on it.
                    try:
                        _move_processed(path, processed_dir / "bad")
                    except Exception:
                        pass

            now = time.monotonic()
            if ckpt_every > 0 and (now - last_ckpt) >= ckpt_every:
                _maybe_checkpoint(conn, db_path, truncate_mb)
                last_ckpt = now

            if retention_enabled and retention_every_sec > 0 and (now - last_retention) >= retention_every_sec:
                try:
                    _retention_run(
                        conn,
                        full_hours=retention_full_hours,
                        delete_after_days=retention_delete_after_days,
                        slim_batch=retention_slim_batch,
                    )
                except Exception:
                    pass
                try:
                    _prune_processed_dir(processed_dir, processed_retention_days)
                except Exception:
                    pass
                last_retention = now

            if stats_every > 0 and (now - last_stats) >= stats_every:
                try:
                    qlen = sum(1 for _ in export_dir.glob("*.json"))
                except Exception:
                    qlen = -1
                LOG.info(
                    f"[ingest] stats scanned={stats_scanned} inserted={stats_inserted} skipped={stats_skipped} bad={stats_bad} qlen={qlen}"
                )
                stats_scanned = 0
                stats_inserted = 0
                stats_skipped = 0
                stats_bad = 0
                last_stats = now

            time.sleep(max(0.1, float(args.poll)))

    except KeyboardInterrupt:
        LOG.info("[ingest] stopping")
        return 0
    finally:
        try:
            # A final truncate checkpoint so the main DB file contains all recent changes.
            _maybe_checkpoint(conn, db_path, truncate_mb=0)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
