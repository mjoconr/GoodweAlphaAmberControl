#!/usr/bin/env python3
from __future__ import annotations

import os
import time
import threading
import json
import math
import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, date
from typing import Any, Dict, Optional, Tuple, List

import requests

try:
    from pymodbus.client import ModbusTcpClient
except Exception:
    ModbusTcpClient = None


def _maybe_load_env_file(path: str) -> None:
    """Best-effort loader for bash-style KEY=VALUE files (e.g. ~/.amber).

    This is mainly to help when a file is `source`d without exporting variables, which
    means Python won't see them via os.getenv().
    """
    try:
        p = os.path.expanduser(path)
        if not os.path.exists(p):
            return
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                # Allow: export KEY=VALUE
                if s.lower().startswith("export "):
                    s = s[7:].strip()
                if "=" not in s:
                    continue
                k, v = s.split("=", 1)
                k = k.strip()
                v = v.strip()
                if not k:
                    continue
                # Strip surrounding quotes
                if (len(v) >= 2) and ((v[0] == v[-1]) and v[0] in ('"', "'")):
                    v = v[1:-1]
                # Don't overwrite an explicitly exported env var
                if os.getenv(k) is None:
                    os.environ[k] = v
    except Exception:
        return


# Load ~/.amber if present (harmless if you already export vars).
_maybe_load_env_file("~/.amber")


# -------------------- Helpers --------------------


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    return v


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


def _iso_now() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%dT%H:%M:%S")


def _pct_from_watts(target_w: int, rated_w: int) -> int:
    if rated_w <= 0:
        return 0
    pct = int(round((float(target_w) / float(rated_w)) * 100.0))
    if pct < 0:
        pct = 0
    if pct > 100:
        pct = 100
    return pct


def _clamp_int(x: int, lo: int, hi: int) -> int:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


# -------------------- Configuration --------------------

# Amber
AMBER_SITE_ID = _env("AMBER_SITE_ID", "...")
AMBER_API_KEY = _env("AMBER_API_KEY", "...")

# Amber polling: price/usage data is interval-based (often 5 minutes in AU).
AMBER_RESOLUTION_MIN = _env_int("AMBER_RESOLUTION_MIN", 5)
AMBER_FETCH_USAGE = _env_bool("AMBER_FETCH_USAGE", True)
AMBER_USAGE_RESOLUTION_MIN = _env_int("AMBER_USAGE_RESOLUTION_MIN", 5)
AMBER_MAX_STALE_SEC = _env_int("AMBER_MAX_STALE_SEC", 900)

EXPORT_COST_THRESHOLD_C = _env_float("EXPORT_COST_THRESHOLD_C", 0.0)

# GoodWe / Modbus
GOODWE_HOST = _env("GOODWE_HOST", _env("GOODWE", "127.0.0.1:502"))
GOODWE_UNIT = _env_int("GOODWE_UNIT", _env_int("UNIT", 247))
MODBUS_TIMEOUT_SEC = _env_float("MODBUS_TIMEOUT_SEC", _env_float("TIMEOUT", 3.0))
MODBUS_RETRIES = _env_int("MODBUS_RETRIES", _env_int("RETRIES", 3))

MODBUS_RECONNECT_ON_ERROR = _env_bool("MODBUS_RECONNECT_ON_ERROR", True)
MODBUS_RECONNECT_MIN_BACKOFF_SEC = _env_float("MODBUS_RECONNECT_MIN_BACKOFF_SEC", 1.0)
MODBUS_RECONNECT_MAX_BACKOFF_SEC = _env_float("MODBUS_RECONNECT_MAX_BACKOFF_SEC", 30.0)

GOODWE_EXPORT_LIMIT_MODE = _env("GOODWE_EXPORT_LIMIT_MODE", "pct")  # pct or active_pct
GOODWE_EXPORT_SWITCH_REG = _env_int("GOODWE_EXPORT_SWITCH_REG", 291)
GOODWE_EXPORT_PCT_REG = _env_int("GOODWE_EXPORT_PCT_REG", 292)
GOODWE_EXPORT_PCT10_REG = _env_int("GOODWE_EXPORT_PCT10_REG", 293)
GOODWE_ACTIVE_PCT_REG = _env_int("GOODWE_ACTIVE_PCT_REG", 256)

GOODWE_RATED_W = _env_int("GOODWE_RATED_W", 5000)
GOODWE_ALWAYS_ENABLED = _env_bool("GOODWE_ALWAYS_ENABLED", True)

# Control loop
SLEEP_SECONDS = _env_float("SLEEP_SECONDS", 10.0)
MIN_SECONDS_BETWEEN_WRITES = _env_float("MIN_SECONDS_BETWEEN_WRITES", 10.0)
MIN_PCT_STEP = _env_int("MIN_PCT_STEP", 1)
LIMIT_SMOOTHING = _env_float("LIMIT_SMOOTHING", 0.2)

DEBUG = _env_bool("DEBUG", False)

# Reduce noisy pymodbus console output unless DEBUG is enabled.
# (Some pymodbus versions emit "Exception response ..." at ERROR level.)
if not DEBUG:
    for _ln in (
        "pymodbus",
        "pymodbus.transaction",
        "pymodbus.client",
        "pymodbus.framer",
    ):
        try:
            logging.getLogger(_ln).setLevel(logging.CRITICAL)
        except Exception:
            pass

# GoodWe runtime profile
RUNTIME_PROFILE = _env("RUNTIME_PROFILE", "auto")  # auto, dns, mt

# AlphaESS OpenAPI
ALPHAESS_OPENAPI_ENABLED = _env_bool("ALPHAESS_OPENAPI_ENABLED", True) or _env_bool("ALPHAESS_ENABLED", False)

ALPHAESS_APP_ID = _env("ALPHAESS_APP_ID", "...")
ALPHAESS_APP_SECRET = _env("ALPHAESS_APP_SECRET", "...")
ALPHAESS_SYS_SN = _env("ALPHAESS_SYS_SN", "...")
ALPHAESS_UNIT_INDEX = _env_int("ALPHAESS_UNIT_INDEX", 1)
ALPHAESS_VERIFY_SSL = _env_bool("ALPHAESS_VERIFY_SSL", True)

# AlphaESS sign conventions (note user's comment: positive means costing money, etc.)
ALPHAESS_PBAT_POSITIVE_IS_CHARGE = _env_bool("ALPHAESS_PBAT_POSITIVE_IS_CHARGE", True)
ALPHAESS_PGRID_POSITIVE_IS_IMPORT = _env_bool("ALPHAESS_PGRID_POSITIVE_IS_IMPORT", True)

# AlphaESS idle/charge detection
ALPHAESS_PBAT_IDLE_THRESHOLD_W = _env_int("ALPHAESS_PBAT_IDLE_THRESHOLD_W", 50)

# AlphaESS polling behaviour
ALPHAESS_MIN_POLL_INTERVAL_SEC = _env_int("ALPHAESS_MIN_POLL_INTERVAL_SEC", 5)
ALPHAESS_POLL_TIMEOUT_SEC = _env_float("ALPHAESS_POLL_TIMEOUT_SEC", 12.0)
ALPHAESS_MAX_STALE_SEC = _env_int("ALPHAESS_MAX_STALE_SEC", 30)

# Grid feedback tuning
ALPHAESS_GRID_FEEDBACK_GAIN = _env_float("ALPHAESS_GRID_FEEDBACK_GAIN", 1.0)
ALPHAESS_GRID_IMPORT_BIAS_W = _env_int("ALPHAESS_GRID_IMPORT_BIAS_W", 50)

# Battery considered "full" at/above this SOC percentage.
# Many systems only report SOC as an integer, so use 99.5 by default to treat 100% as full.
ALPHAESS_FULL_SOC_PCT = _env_float("ALPHAESS_FULL_SOC_PCT", 99.5)
# Treat battery as "near full" at/above this SOC.
# Below this threshold we use a "soft" mode (allow 100% until export exceeds a small allowance).
# At/above this threshold (or SOC unknown) we use a "strict" mode to keep export near zero.
# Default equals FULL_SOC so behaviour is unchanged unless you set it.

# When export would cost money and the battery is NOT full, allow a small amount of export
# before we start backing the GoodWe off (helps the battery begin charging / avoid oscillation).

# When export would cost money (feed-in price < threshold), it can be useful to
# deliberately leave headroom for the battery to *start* charging (self-consumption mode).
# If SOC is below this threshold, we assume the battery can absorb up to AUTO_CHARGE_W.
# Set AUTO_CHARGE_W=0 to disable this behaviour.
ALPHAESS_AUTO_CHARGE_BELOW_SOC_PCT = _env_float("ALPHAESS_AUTO_CHARGE_BELOW_SOC_PCT", 90.0)
ALPHAESS_AUTO_CHARGE_W = _env_int("ALPHAESS_AUTO_CHARGE_W", 1500)
ALPHAESS_AUTO_CHARGE_MAX_W = _env_int("ALPHAESS_AUTO_CHARGE_MAX_W", 3000)

# -------------------- Amber --------------------


class AmberClient:
    def __init__(self, site_id: str, api_key: str, timeout: float = 10.0):
        self.site_id = str(site_id or "")
        self.api_key = str(api_key or "")
        self.timeout = float(timeout)

    @staticmethod
    def _parse_time(ts: Any) -> Optional[datetime]:
        if ts is None:
            return None
        s = str(ts).strip()
        if not s:
            return None
        try:
            # Amber typically returns RFC3339/ISO 8601 (often with timezone offset)
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        if not self.site_id or not self.api_key:
            raise RuntimeError("AMBER_SITE_ID / AMBER_API_KEY not set")
        url = f"https://api.amber.com.au/v1/sites/{self.site_id}{path}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        r = requests.get(url, headers=headers, params=params or {}, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def get_current_prices(
        self, resolution_min: int = 5
    ) -> Tuple[Optional[float], Optional[float], Optional[datetime], Optional[datetime], Any]:
        """Return (import_c, feedin_c, interval_start_utc, interval_end_utc, raw_json)."""
        data = self._get("/prices/current", params={"resolution": int(resolution_min)})

        import_c: Optional[float] = None
        feedin_c: Optional[float] = None
        interval_start: Optional[datetime] = None
        interval_end: Optional[datetime] = None

        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue

                per_kwh = item.get("perKwh")
                if per_kwh is None:
                    continue

                channel = str(item.get("channelType") or item.get("channel") or "").lower()
                descriptor = str(item.get("descriptor") or item.get("type") or "").lower()
                tariff = str(item.get("tariffInformation") or "").lower()

                is_feed = ("feed" in channel) or ("feed" in descriptor) or ("feed" in tariff)

                try:
                    per_kwh_f = float(per_kwh)
                except Exception:
                    continue

                if is_feed:
                    feedin_c = per_kwh_f
                else:
                    import_c = per_kwh_f

                st = self._parse_time(item.get("startTime") or item.get("start_time"))
                en = self._parse_time(item.get("endTime") or item.get("end_time"))
                if st and (interval_start is None or st > interval_start):
                    # startTime should be the same across channels; prefer newest seen
                    interval_start = st
                if en and (interval_end is None or en > interval_end):
                    interval_end = en

        return import_c, feedin_c, interval_start, interval_end, data

    def get_usage(
        self,
        start_date: str,
        end_date: str,
        resolution_min: int = 5,
    ) -> Any:
        """Raw usage intervals (kWh) for a date range."""
        params = {"startDate": start_date, "endDate": end_date, "resolution": int(resolution_min)}
        return self._get("/usage", params=params)


@dataclass
class AmberSnapshot:
    ts: float
    import_c: Optional[float]
    feedin_c: Optional[float]
    interval_start_utc: Optional[datetime]
    interval_end_utc: Optional[datetime]
    import_power_w: Optional[int] = None
    feedin_power_w: Optional[int] = None
    raw_prices: Any = None
    raw_usage: Any = None
    last_error: Optional[str] = None


class AmberPoller:
    """Poll Amber prices/usage at the interval boundary (default 5 minutes)."""

    def __init__(
        self,
        client: AmberClient,
        resolution_min: int = 5,
        poll_slack_sec: int = 2,
        max_stale_sec: int = 900,
        fetch_usage: bool = True,
        usage_resolution_min: int = 5,
        retry_backoff_sec: int = 30,
    ):
        self.client = client
        self.resolution_min = int(resolution_min)
        self.poll_slack_sec = int(poll_slack_sec)
        self.max_stale_sec = int(max_stale_sec)
        self.fetch_usage = bool(fetch_usage)
        self.usage_resolution_min = int(usage_resolution_min)
        self.retry_backoff_sec = int(retry_backoff_sec)

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="amber-poller", daemon=True)

        self._snapshot: Optional[AmberSnapshot] = None
        self._next_due_ts: float = 0.0

        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        try:
            self._thread.join(timeout=5)
        except Exception:
            pass

    def get_snapshot(self) -> Optional[AmberSnapshot]:
        with self._lock:
            return self._snapshot

    def is_ok(self) -> bool:
        snap = self.get_snapshot()
        if snap is None:
            return False
        age = time.time() - float(snap.ts)
        return age <= float(self.max_stale_sec) and (snap.feedin_c is not None)

    def _set_snapshot(self, snap: AmberSnapshot, next_due_ts: float) -> None:
        with self._lock:
            self._snapshot = snap
            self._next_due_ts = float(next_due_ts)

    @staticmethod
    def _avg_power_w(kwh: float, start_utc: datetime, end_utc: datetime) -> Optional[int]:
        try:
            dur_s = (end_utc - start_utc).total_seconds()
            if dur_s <= 0:
                return None
            # kWh -> W average over interval
            w = (float(kwh) * 1000.0) / (dur_s / 3600.0)
            return int(round(w))
        except Exception:
            return None

    def _extract_usage_powers(
        self, usage_json: Any, target_end_utc: Optional[datetime]
    ) -> Tuple[Optional[int], Optional[int], Any]:
        """Return (import_power_w, feedin_power_w, raw_usage_used)."""
        if not isinstance(usage_json, list):
            return None, None, usage_json

        best_general = None  # (end_utc, power_w)
        best_feed = None

        for item in usage_json:
            if not isinstance(item, dict):
                continue
            ch = str(item.get("channelType") or item.get("channel") or "").lower()
            kwh = item.get("kwh")
            st = AmberClient._parse_time(item.get("startTime") or item.get("start_time"))
            en = AmberClient._parse_time(item.get("endTime") or item.get("end_time"))
            if st is None or en is None or kwh is None:
                continue
            if target_end_utc is not None:
                # Only consider intervals that are <= the current price interval end
                if en > target_end_utc:
                    continue
            try:
                kwh_f = float(kwh)
            except Exception:
                continue

            pw = self._avg_power_w(kwh_f, st, en)
            if pw is None:
                continue

            if "feed" in ch:
                if best_feed is None or en > best_feed[0]:
                    best_feed = (en, pw)
            else:
                if best_general is None or en > best_general[0]:
                    best_general = (en, pw)

        import_w = best_general[1] if best_general else None
        feed_w = best_feed[1] if best_feed else None
        return import_w, feed_w, usage_json

    def _run(self) -> None:
        # Force initial fetch immediately.
        self._next_due_ts = 0.0

        while not self._stop.is_set():
            now = time.time()
            due = self._next_due_ts
            if due and now < due:
                time.sleep(min(1.0, due - now))
                continue

            last_error: Optional[str] = None
            raw_prices = None
            raw_usage = None
            import_w = None
            feed_w = None

            try:
                import_c, feed_c, st_utc, en_utc, raw_prices = self.client.get_current_prices(
                    resolution_min=self.resolution_min
                )

                if self.fetch_usage:
                    # Usage can lag slightly; still useful for a 5-minute average view.
                    today = date.today().isoformat()
                    try:
                        raw_usage = self.client.get_usage(
                            start_date=today, end_date=today, resolution_min=self.usage_resolution_min
                        )
                    except Exception:
                        # Some accounts/APIs may not accept 5-minute resolution for usage; fall back to 30.
                        raw_usage = self.client.get_usage(start_date=today, end_date=today, resolution_min=30)

                    import_w, feed_w, raw_usage = self._extract_usage_powers(raw_usage, en_utc)

                snap = AmberSnapshot(
                    ts=time.time(),
                    import_c=import_c,
                    feedin_c=feed_c,
                    interval_start_utc=st_utc,
                    interval_end_utc=en_utc,
                    import_power_w=import_w,
                    feedin_power_w=feed_w,
                    raw_prices=raw_prices,
                    raw_usage=raw_usage,
                    last_error=None,
                )

                # Schedule next poll at end of interval (plus small slack).
                if en_utc is not None:
                    next_due = float(en_utc.timestamp()) + float(self.poll_slack_sec)
                else:
                    next_due = time.time() + float(self.resolution_min * 60)

                self._set_snapshot(snap, next_due)
                continue

            except Exception as e:
                last_error = str(e)

            # On error: store a snapshot with error and retry with a small backoff.
            snap = AmberSnapshot(
                ts=time.time(),
                import_c=None,
                feedin_c=None,
                interval_start_utc=None,
                interval_end_utc=None,
                raw_prices=raw_prices,
                raw_usage=raw_usage,
                last_error=last_error,
            )
            self._set_snapshot(snap, time.time() + float(self.retry_backoff_sec))


# -------------------- GoodWe Modbus --------------------


def _split_host_port(host: str) -> Tuple[str, int]:
    s = str(host or "").strip()
    if ":" in s:
        h, p = s.rsplit(":", 1)
        try:
            return h, int(p)
        except Exception:
            return h, 502
    return s, 502


class GoodWeModbus:
    def __init__(
        self,
        host: str,
        unit: int,
        timeout_sec: float = 3.0,
        retries: int = 3,
        reconnect_on_error: bool = True,
        reconnect_min_backoff_sec: float = 1.0,
        reconnect_max_backoff_sec: float = 30.0,
    ):
        self.host, self.port = _split_host_port(host)
        self.unit = int(unit)
        self.timeout_sec = float(timeout_sec)
        self.retries = int(retries)

        self.reconnect_on_error = bool(reconnect_on_error)
        self.reconnect_min_backoff_sec = float(reconnect_min_backoff_sec)
        self.reconnect_max_backoff_sec = float(reconnect_max_backoff_sec)

        self.client = None

        # Reconnect state (exponential backoff)
        self._reconnect_failures = 0
        self._next_reconnect_ts = 0.0

    def connect(self) -> bool:
        if ModbusTcpClient is None:
            raise RuntimeError("pymodbus not available")

        # Always create a fresh socket on connect/reconnect.
        self.close()
        self.client = ModbusTcpClient(host=self.host, port=self.port, timeout=self.timeout_sec)

        try:
            ok = bool(self.client.connect())
        except Exception:
            ok = False

        if ok:
            # Reset backoff on success
            self._reconnect_failures = 0
            self._next_reconnect_ts = 0.0
            return True

        # Failed to connect; drop the client so later calls can retry cleanly.
        self.close()
        return False

    def close(self) -> None:
        if self.client:
            try:
                self.client.close()
            except Exception:
                pass
        self.client = None

    @staticmethod
    def _is_connection_error(exc: BaseException) -> bool:
        """Best-effort detection of socket/transport failures (Broken pipe, reset, timeout, etc.)."""
        try:
            msg = str(exc).lower()
        except Exception:
            msg = ""

        # Don't treat our compatibility errors as connection errors.
        if "signature mismatch" in msg:
            return False

        # Common built-in connection errors
        if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, TimeoutError)):
            return True

        # OS-level errors often include errno
        if isinstance(exc, OSError):
            eno = getattr(exc, "errno", None)
            if eno in (32, 54, 57, 58, 60, 61, 64, 65, 101, 104, 110, 111, 112, 113):
                return True

        # Fallback string checks
        for needle in (
            "broken pipe",
            "connection reset",
            "connection aborted",
            "connection refused",
            "timed out",
            "timeout",
            "no route to host",
            "network is unreachable",
            "not connected",
            "connection closed",
            "socket",
        ):
            if needle in msg:
                return True

        return False

    def _maybe_reconnect(self, where: str, exc: BaseException) -> None:
        if not self.reconnect_on_error:
            return
        if not self._is_connection_error(exc):
            return

        now = time.time()
        if now < float(self._next_reconnect_ts):
            return

        # Exponential backoff: min_backoff * 2^failures, capped.
        failures = int(self._reconnect_failures)
        backoff = float(self.reconnect_min_backoff_sec) * (2.0 ** float(failures))
        backoff = max(float(self.reconnect_min_backoff_sec), backoff)
        backoff = min(float(self.reconnect_max_backoff_sec), backoff)

        print(f"  [goodwe] modbus reconnecting ({self.host}:{self.port}) after {where}: {exc}")

        if self.connect():
            print(f"  [goodwe] modbus reconnected ({self.host}:{self.port})")
            return

        self._reconnect_failures = failures + 1
        self._next_reconnect_ts = now + backoff
        print(f"  [goodwe] modbus reconnect failed; next retry in {backoff:.1f}s")

    def _read_holding(self, address: int, count: int) -> List[int]:
        if not self.client:
            self._maybe_reconnect("read_holding:no_client", RuntimeError("Modbus client not connected"))
            if not self.client:
                raise RuntimeError("Modbus client not connected")

        def _call_rr() -> Any:
            # Pymodbus keyword changes across versions:
            #   - older: unit=
            #   - mid:   slave=
            #   - newer: device_id=
            # We try the common variants in a few call shapes.
            fn = self.client.read_holding_registers  # type: ignore[attr-defined]
            uid = int(self.unit)

            attempts: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
                ((), {"address": address, "count": count, "device_id": uid}),
                ((), {"address": address, "count": count, "slave": uid}),
                ((), {"address": address, "count": count, "unit": uid}),
                ((address, count), {"device_id": uid}),
                ((address, count), {"slave": uid}),
                ((address, count), {"unit": uid}),
                ((address, count, uid), {}),  # sometimes slave/unit is positional
                ((address,), {"count": count, "device_id": uid}),
                ((address,), {"count": count, "slave": uid}),
                ((address,), {"count": count, "unit": uid}),
            ]

            last_te: Optional[Exception] = None
            for a, kw in attempts:
                try:
                    rr = fn(*a, **kw)
                    if DEBUG:
                        used = "positional" if not kw else ",".join(sorted(kw.keys()))
                        print(f"  [pymodbus] read_holding_registers compat used: {used}")
                    return rr
                except TypeError as e:
                    last_te = e
                    continue
            raise RuntimeError(f"read_holding_registers signature mismatch: {last_te}")

        last_exc: Optional[Exception] = None
        for _ in range(max(1, self.retries)):
            try:
                rr = _call_rr()
                if rr is None:
                    raise RuntimeError("no response")
                if getattr(rr, "isError", lambda: False)():
                    # If the inverter replies with a deterministic Modbus exception
                    # (e.g. illegal function/address), retries won't help.
                    exc_code = getattr(rr, "exception_code", None)
                    err = RuntimeError(str(rr))
                    if exc_code in (1, 2, 3, 4):
                        last_exc = err
                        break
                    raise err
                regs = getattr(rr, "registers", None)
                if regs is None:
                    raise RuntimeError("no registers")
                return list(regs)
            except Exception as e:
                last_exc = e
                self._maybe_reconnect("read_holding", e)
                time.sleep(0.05)
        raise RuntimeError(f"read_holding_registers failed: {last_exc}")

    def _read_input(self, address: int, count: int) -> List[int]:
        if not self.client:
            self._maybe_reconnect("read_input:no_client", RuntimeError("Modbus client not connected"))
            if not self.client:
                raise RuntimeError("Modbus client not connected")

        def _call_rr() -> Any:
            # Pymodbus keyword changes across versions:
            #   - older: unit=
            #   - mid:   slave=
            #   - newer: device_id=
            fn = self.client.read_input_registers  # type: ignore[attr-defined]
            uid = int(self.unit)

            attempts: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
                ((), {"address": address, "count": count, "device_id": uid}),
                ((), {"address": address, "count": count, "slave": uid}),
                ((), {"address": address, "count": count, "unit": uid}),
                ((address, count), {"device_id": uid}),
                ((address, count), {"slave": uid}),
                ((address, count), {"unit": uid}),
                ((address, count, uid), {}),  # sometimes slave/unit is positional
                ((address,), {"count": count, "device_id": uid}),
                ((address,), {"count": count, "slave": uid}),
                ((address,), {"count": count, "unit": uid}),
            ]

            last_te: Optional[Exception] = None
            for a, kw in attempts:
                try:
                    rr = fn(*a, **kw)
                    if DEBUG:
                        used = "positional" if not kw else ",".join(sorted(kw.keys()))
                        print(f"  [pymodbus] read_input_registers compat used: {used}")
                    return rr
                except TypeError as e:
                    last_te = e
                    continue
            raise RuntimeError(f"read_input_registers signature mismatch: {last_te}")

        last_exc: Optional[Exception] = None
        for _ in range(max(1, self.retries)):
            try:
                rr = _call_rr()
                if rr is None:
                    raise RuntimeError("no response")
                if getattr(rr, "isError", lambda: False)():
                    # If the inverter replies with a deterministic Modbus exception
                    # (e.g. illegal function/address), retries won't help.
                    exc_code = getattr(rr, "exception_code", None)
                    err = RuntimeError(str(rr))
                    if exc_code in (1, 2, 3, 4):
                        last_exc = err
                        break
                    raise err
                regs = getattr(rr, "registers", None)
                if regs is None:
                    raise RuntimeError("no registers")
                return list(regs)
            except Exception as e:
                last_exc = e
                self._maybe_reconnect("read_input", e)
                time.sleep(0.05)
        raise RuntimeError(f"read_input_registers failed: {last_exc}")

    def _write_register(self, address: int, value: int) -> None:
        if not self.client:
            self._maybe_reconnect("write_register:no_client", RuntimeError("Modbus client not connected"))
            if not self.client:
                raise RuntimeError("Modbus client not connected")

        def _call_wr() -> Any:
            # Pymodbus keyword changes across versions:
            #   - older: unit=
            #   - mid:   slave=
            #   - newer: device_id=
            fn = self.client.write_register  # type: ignore[attr-defined]
            uid = int(self.unit)
            v = int(value)

            attempts: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
                ((), {"address": address, "value": v, "device_id": uid}),
                ((), {"address": address, "value": v, "slave": uid}),
                ((), {"address": address, "value": v, "unit": uid}),
                ((address, v), {"device_id": uid}),
                ((address, v), {"slave": uid}),
                ((address, v), {"unit": uid}),
                ((address, v, uid), {}),  # sometimes slave/unit is positional
                ((address,), {"value": v, "device_id": uid}),
                ((address,), {"value": v, "slave": uid}),
                ((address,), {"value": v, "unit": uid}),
            ]

            last_te: Optional[Exception] = None
            for a, kw in attempts:
                try:
                    rr = fn(*a, **kw)
                    if DEBUG:
                        used = "positional" if not kw else ",".join(sorted(kw.keys()))
                        print(f"  [pymodbus] write_register compat used: {used}")
                    return rr
                except TypeError as e:
                    last_te = e
                    continue
            raise RuntimeError(f"write_register signature mismatch: {last_te}")

        last_exc: Optional[Exception] = None
        for _ in range(max(1, self.retries)):
            try:
                rr = _call_wr()
                if rr is None:
                    raise RuntimeError("no response")
                if getattr(rr, "isError", lambda: False)():
                    # If the inverter replies with a deterministic Modbus exception
                    # (e.g. illegal function/address), retries won't help.
                    exc_code = getattr(rr, "exception_code", None)
                    err = RuntimeError(str(rr))
                    if exc_code in (1, 2, 3, 4):
                        last_exc = err
                        break
                    raise err
                return
            except Exception as e:
                last_exc = e
                self._maybe_reconnect("write_register", e)
                time.sleep(0.05)
        raise RuntimeError(f"write_register failed: {last_exc}")

    def read_u16(self, reg: int) -> int:
        return int(self._read_holding(reg, 1)[0])

    def read_u16s(self, reg: int, count: int) -> List[int]:
        return [int(x) for x in self._read_holding(reg, count)]

    def read_input_u16(self, reg: int) -> int:
        return int(self._read_input(reg, 1)[0])

    def read_input_u16s(self, reg: int, count: int) -> List[int]:
        return [int(x) for x in self._read_input(reg, count)]

    def write_u16(self, reg: int, value: int) -> None:
        self._write_register(reg, int(value))


class GoodWePowerLimiter:
    def __init__(
        self,
        modbus: GoodWeModbus,
        rated_w: int,
        mode: str = "pct",
        export_switch_reg: int = 291,
        export_pct_reg: int = 292,
        export_pct10_reg: int = 293,
        active_pct_reg: int = 256,
        always_enabled: bool = True,
    ):
        self.modbus = modbus
        self.rated_w = int(rated_w)
        self.mode = str(mode).strip().lower()
        self.export_switch_reg = int(export_switch_reg)
        self.export_pct_reg = int(export_pct_reg)
        self.export_pct10_reg = int(export_pct10_reg)
        self.active_pct_reg = int(active_pct_reg)
        self.always_enabled = bool(always_enabled)

    def read_current(self) -> Dict[str, int]:
        if self.mode == "active_pct":
            enabled = int(self.modbus.read_u16(self.export_switch_reg))
            pct = int(self.modbus.read_u16(self.active_pct_reg))
            return {"enabled": enabled, "pct": pct, "pct10": 0}
        enabled = int(self.modbus.read_u16(self.export_switch_reg))
        pct = int(self.modbus.read_u16(self.export_pct_reg))
        pct10 = int(self.modbus.read_u16(self.export_pct10_reg))
        return {"enabled": enabled, "pct": pct, "pct10": pct10}

    def set_limit_pct(self, enabled: bool, pct: int) -> None:
        pct = _clamp_int(int(pct), 0, 100)
        if self.always_enabled:
            enabled = True

        self.modbus.write_u16(self.export_switch_reg, 1 if enabled else 0)

        if self.mode == "active_pct":
            self.modbus.write_u16(self.active_pct_reg, pct)
            return

        self.modbus.write_u16(self.export_pct_reg, pct)
        self.modbus.write_u16(self.export_pct10_reg, int(pct * 10))


# -------------------- GoodWe runtime (fast feedback) --------------------


@dataclass
class GoodWeRuntime:
    ts: float
    gen_power_w: Optional[int] = None
    pv_power_est_w: Optional[int] = None
    feed_power_w: Optional[int] = None
    power_limit_fn: Optional[int] = None
    meter_ok: Optional[int] = None
    wifi: Optional[int] = None
    inverter_temp_c: Optional[float] = None
    raw: Optional[Dict[str, Any]] = None


def _u16_to_i16(u: int) -> int:
    u = int(u) & 0xFFFF
    return u - 0x10000 if u & 0x8000 else u


def _regs_to_i32(hi: int, lo: int) -> int:
    v = ((int(hi) & 0xFFFF) << 16) | (int(lo) & 0xFFFF)
    if v & 0x80000000:
        v -= 0x100000000
    return int(v)


class GoodWeRuntimeReader:
    """Read a small set of runtime values for feedback.

    This is best-effort only (for logging/telemetry and optional future control loops).
    Different GoodWe models/firmware expose runtime data via different register maps and
    sometimes via *input* registers (function 04) rather than holding registers (function 03).

    To avoid spamming the console with Modbus exception responses, the default "auto"
    mode probes the candidate profiles once and then sticks with the first one that works.
    If neither works, runtime reads are disabled.
    """

    def __init__(self, modbus: GoodWeModbus, profile: str = "auto"):
        self.modbus = modbus
        self.profile = str(profile).strip().lower()
        self._resolved_profile: Optional[str] = None
        self._last_probe_error: Optional[str] = None

        # If the user explicitly picked a profile, lock to it.
        if self.profile in ("dns", "mt", "off", "none", "disabled"):
            self._resolved_profile = self.profile

    def read(self) -> GoodWeRuntime:
        p = self.profile

        if p in ("off", "none", "disabled"):
            return GoodWeRuntime(ts=time.time(), raw={"profile": "off"})

        if p == "dns":
            return self._read_dns()

        if p == "mt":
            return self._read_mt()

        # auto: probe once then stick.
        if p == "auto":
            if self._resolved_profile == "dns":
                return self._read_dns()
            if self._resolved_profile == "mt":
                return self._read_mt()
            if self._resolved_profile in ("off", "none", "disabled"):
                return GoodWeRuntime(ts=time.time(), raw={"profile": "off", "err": self._last_probe_error})

            for candidate in ("dns", "mt"):
                try:
                    if candidate == "dns":
                        rt = self._read_dns()
                    else:
                        rt = self._read_mt()
                    self._resolved_profile = candidate
                    return rt
                except Exception as e:
                    self._last_probe_error = str(e)
                    continue

            self._resolved_profile = "off"
            return GoodWeRuntime(ts=time.time(), raw={"profile": "off", "err": self._last_probe_error})

        # Unknown profile -> disabled
        return GoodWeRuntime(ts=time.time(), raw={"profile": "off", "err": f"unknown profile: {p}"})

    def _read_regs_best_effort(self, base_reg: int, count: int) -> List[int]:
        """Try input registers first, then holding registers."""
        try:
            return self.modbus.read_input_u16s(int(base_reg), int(count))
        except Exception:
            return self.modbus.read_u16s(int(base_reg), int(count))

    def _read_dns(self) -> GoodWeRuntime:
        # Best-effort guesses for GW5000-DNS style.
        regs = self._read_regs_best_effort(35103, 20)

        # Very rough decoding; if this mapping isn't right on your box, it's still safe.
        gen = _regs_to_i32(regs[0], regs[1])  # may be total active power
        feed = _regs_to_i32(regs[2], regs[3])  # may be meter power
        temp = _u16_to_i16(regs[10]) / 10.0

        return GoodWeRuntime(
            ts=time.time(),
            gen_power_w=int(gen),
            feed_power_w=int(feed),
            inverter_temp_c=float(temp),
            raw={"profile": "dns", "regs": regs},
        )

    def _read_mt(self) -> GoodWeRuntime:
        # Alternative profile.
        regs = self._read_regs_best_effort(36001, 20)

        gen = _regs_to_i32(regs[0], regs[1])
        feed = _regs_to_i32(regs[2], regs[3])
        temp = _u16_to_i16(regs[10]) / 10.0

        return GoodWeRuntime(
            ts=time.time(),
            gen_power_w=int(gen),
            feed_power_w=int(feed),
            inverter_temp_c=float(temp),
            raw={"profile": "mt", "regs": regs},
        )


# -------------------- AlphaESS OpenAPI (fast feedback) --------------------


@dataclass
class AlphaEssSnapshot:
    ts: float
    sys_sn: str
    pload_w: Optional[int] = None

    # raw readings
    pbat_w_raw: Optional[int] = None
    pgrid_w_raw: Optional[int] = None

    # normalised
    pbat_w: Optional[int] = None  # + charge, - discharge
    pgrid_w: Optional[int] = None  # + import, - export

    ppv_w: Optional[int] = None
    pev_w: Optional[int] = None
    soc_pct: Optional[float] = None

    batt_state: str = "unknown"
    charge_w: int = 0
    discharge_w: int = 0
    grid_import_w: int = 0
    grid_export_w: int = 0

    raw: Optional[Dict[str, Any]] = None


def _ci_get(d: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d:
            return d[k]
    # case-insensitive
    lower = {str(k).lower(): v for k, v in d.items()}
    for k in keys:
        lk = str(k).lower()
        if lk in lower:
            return lower[lk]
    return None


def _to_int_w(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        if isinstance(x, bool):
            return int(x)
        if isinstance(x, (int, float)):
            return int(round(float(x)))
        s = str(x).strip()
        if s == "":
            return None
        return int(round(float(s)))
    except Exception:
        return None


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


class AlphaEssOpenApiClient:
    def __init__(
        self,
        app_id: str,
        app_secret: str,
        timeout_sec: float = 12.0,
        verify_ssl: bool = True,
        base_url: str = "https://openapi.alphaess.com/api",
    ):
        self.app_id = str(app_id or "")
        self.app_secret = str(app_secret or "")
        self.timeout_sec = float(timeout_sec)
        self.verify_ssl = bool(verify_ssl)
        self.base_url = str(base_url or "").rstrip("/")

        if not self.app_id or not self.app_secret:
            raise RuntimeError("ALPHAESS_APP_ID / ALPHAESS_APP_SECRET must be set")

    def _headers(self) -> Dict[str, str]:
        ts = str(int(time.time()))
        sign_src = f"{self.app_id}{self.app_secret}{ts}".encode("utf-8")
        sign = hashlib.sha512(sign_src).hexdigest()
        return {"appId": self.app_id, "timeStamp": ts, "sign": sign}

    @staticmethod
    def _unwrap(resp: Any) -> Any:
        if isinstance(resp, dict):
            code = resp.get("code")
            msg = resp.get("msg") or resp.get("message")
            # Many AlphaESS endpoints wrap payload in {"code":200, "data":...}
            if "data" in resp:
                if code not in (None, 0, 200, "0", "200"):
                    raise RuntimeError(f"AlphaESS API error code={code} msg={msg}")
                return resp.get("data")
        return resp

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        r = requests.get(
            url,
            headers=self._headers(),
            params=params or {},
            timeout=self.timeout_sec,
            verify=self.verify_ssl,
        )
        r.raise_for_status()
        return r.json()

    def get_ess_list(self) -> List[Dict[str, Any]]:
        data = self._unwrap(self._get("getEssList"))
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict) and "list" in data and isinstance(data["list"], list):
            return [x for x in data["list"] if isinstance(x, dict)]
        return []

    def get_last_power_data(self, sys_sn: str) -> Dict[str, Any]:
        data = self._unwrap(self._get("getLastPowerData", params={"sysSn": str(sys_sn)}))
        if isinstance(data, dict):
            return data
        # Some variants may return a list with one dict
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]
        return {}


class AlphaEssOpenApiPoller:
    """Poll AlphaESS OpenAPI (openapi.alphaess.com) on a short interval for control feedback."""

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        sys_sn: str,
        unit_index: int,
        timeout_sec: float,
        verify_ssl: bool = True,
    ):
        self._client = AlphaEssOpenApiClient(
            app_id=app_id, app_secret=app_secret, timeout_sec=timeout_sec, verify_ssl=verify_ssl
        )

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="alphaess-poller", daemon=True)

        self._units: List[Dict[str, Any]] = []
        self._sys_sn = self._resolve_sys_sn(sys_sn=sys_sn, unit_index=unit_index)

        self._last_snapshot: Optional[AlphaEssSnapshot] = None
        self._last_error: Optional[str] = None

        self._thread.start()

    def _resolve_sys_sn(self, sys_sn: str, unit_index: int) -> str:
        s = str(sys_sn or "").strip()
        idx: Optional[int] = None

        # If ALPHAESS_SYS_SN is numeric, treat as unit index (1-based).
        if s.isdigit():
            try:
                idx = int(s)
            except Exception:
                idx = None
        if idx is None and unit_index:
            idx = int(unit_index)

        # If it's not purely numeric and not blank, treat as real sysSn.
        if s and not s.isdigit() and s != "...":
            return s

        # Resolve by index from getEssList.
        self._units = self._client.get_ess_list()
        if idx is None or idx <= 0:
            idx = 1
        if self._units and 1 <= idx <= len(self._units):
            u = self._units[idx - 1]
            sn = u.get("sysSn") or u.get("syssn") or u.get("sn") or u.get("serial") or u.get("serialNumber")
            if sn:
                return str(sn)

        # As a fallback, try first unit.
        if self._units:
            u = self._units[0]
            sn = u.get("sysSn") or u.get("syssn") or u.get("sn") or u.get("serial") or u.get("serialNumber")
            if sn:
                return str(sn)

        raise RuntimeError("Unable to resolve AlphaESS sysSn. Set ALPHAESS_SYS_SN to the full sysSn value.")

    def get_units(self) -> List[Dict[str, Any]]:
        if self._units:
            return list(self._units)
        try:
            self._units = self._client.get_ess_list()
        except Exception:
            pass
        return list(self._units)

    def get_sys_sn(self) -> str:
        return str(self._sys_sn)

    def get_last_error(self) -> Optional[str]:
        with self._lock:
            return self._last_error

    def get_snapshot(self) -> Optional[AlphaEssSnapshot]:
        with self._lock:
            return self._last_snapshot

    def close(self) -> None:
        self._stop.set()
        try:
            self._thread.join(timeout=5)
        except Exception:
            pass

    def _set_snapshot(self, snap: AlphaEssSnapshot) -> None:
        with self._lock:
            self._last_snapshot = snap
            self._last_error = None

    def _set_error(self, err: str) -> None:
        with self._lock:
            self._last_error = err

    def _run(self) -> None:
        interval = max(1, int(ALPHAESS_MIN_POLL_INTERVAL_SEC))

        while not self._stop.is_set():
            try:
                raw = self._client.get_last_power_data(self._sys_sn)

                pload = _to_int_w(_ci_get(raw, "pLoad", "pload", "load", "loadPower"))
                pbat = _to_int_w(_ci_get(raw, "pBat", "pbat", "bat", "battery", "batteryPower"))
                pgrid = _to_int_w(_ci_get(raw, "pGrid", "pgrid", "grid", "gridPower"))
                ppv = _to_int_w(_ci_get(raw, "pPv", "ppv", "pv", "pvPower"))
                pev = _to_int_w(_ci_get(raw, "pEv", "pev", "ev", "evPower"))
                soc = _to_float(_ci_get(raw, "soc", "socPct", "soc_pct", "batSoc", "batterySoc"))

                snap = AlphaEssSnapshot(
                    ts=time.time(),
                    sys_sn=str(self._sys_sn),
                    pload_w=pload,
                    pbat_w_raw=pbat,
                    pgrid_w_raw=pgrid,
                    ppv_w=ppv,
                    pev_w=pev,
                    soc_pct=soc,
                    raw=dict(raw) if isinstance(raw, dict) else {"raw": raw},
                )

                # Normalise pBat sign so + means charging, - means discharging
                if pbat is not None:
                    snap.pbat_w = pbat if ALPHAESS_PBAT_POSITIVE_IS_CHARGE else -pbat
                    if abs(snap.pbat_w) < int(ALPHAESS_PBAT_IDLE_THRESHOLD_W):
                        snap.batt_state = "idle"
                    elif snap.pbat_w > 0:
                        snap.batt_state = "charging"
                        snap.charge_w = int(abs(snap.pbat_w))
                    else:
                        snap.batt_state = "discharging"
                        snap.discharge_w = int(abs(snap.pbat_w))

                # Normalise pGrid sign so + means import, - means export
                if pgrid is not None:
                    snap.pgrid_w = pgrid if ALPHAESS_PGRID_POSITIVE_IS_IMPORT else -pgrid
                    if snap.pgrid_w > 0:
                        snap.grid_import_w = int(snap.pgrid_w)
                    elif snap.pgrid_w < 0:
                        snap.grid_export_w = int(abs(snap.pgrid_w))

                self._set_snapshot(snap)

            except Exception as e:
                self._set_error(str(e))

            # Sleep in small chunks so close() is responsive
            for _ in range(interval):
                if self._stop.is_set():
                    break
                time.sleep(1.0)


# -------------------- Main loop --------------------


def main() -> int:
    if AMBER_SITE_ID == "..." or AMBER_API_KEY == "...":
        print("[error] missing AMBER_SITE_ID and/or AMBER_API_KEY")
        return 2

    print(
        f"[start] amber_site_id={AMBER_SITE_ID} goodwe={GOODWE_HOST} unit={GOODWE_UNIT} "
        f"timeout={MODBUS_TIMEOUT_SEC:.1f}s retries={MODBUS_RETRIES} "
        f"reconnect={'on' if MODBUS_RECONNECT_ON_ERROR else 'off'} "
        f"backoff={MODBUS_RECONNECT_MIN_BACKOFF_SEC:.1f}-{MODBUS_RECONNECT_MAX_BACKOFF_SEC:.1f}s"
    )
    print(
        f"[start] limit_mode={GOODWE_EXPORT_LIMIT_MODE} export_switch_reg={GOODWE_EXPORT_SWITCH_REG} "
        f"export_pct_reg={GOODWE_EXPORT_PCT_REG} export_pct10_reg={GOODWE_EXPORT_PCT10_REG} "
        f"active_pct_reg={GOODWE_ACTIVE_PCT_REG} rated={GOODWE_RATED_W}W "
        f"always_enabled={GOODWE_ALWAYS_ENABLED} threshold={EXPORT_COST_THRESHOLD_C}c"
    )
    print(f"[start] runtime_profile={RUNTIME_PROFILE} (auto tries dns then mt)")
    print(
        f"[start] amber_api_key={'set' if (AMBER_API_KEY and AMBER_API_KEY != '...') else 'missing'} "
        f"amber_res={AMBER_RESOLUTION_MIN}m amber_fetch_usage={AMBER_FETCH_USAGE} "
        f"alphaess_openapi_enabled={ALPHAESS_OPENAPI_ENABLED} "
        f"alphaess_keys={'set' if (ALPHAESS_APP_ID and ALPHAESS_APP_SECRET and ALPHAESS_APP_ID != '...') else 'missing'}"
    )
    print(
        f"[start] alphaess_idle_threshold={ALPHAESS_PBAT_IDLE_THRESHOLD_W}W "
        f"auto_charge_below_soc={ALPHAESS_AUTO_CHARGE_BELOW_SOC_PCT}% "
        f"auto_charge_w={ALPHAESS_AUTO_CHARGE_W}W auto_charge_max_w={ALPHAESS_AUTO_CHARGE_MAX_W}W "
        f"full_soc={ALPHAESS_FULL_SOC_PCT}%"
    )

    amber = AmberClient(AMBER_SITE_ID, AMBER_API_KEY)
    amber_poller = AmberPoller(
        amber,
        resolution_min=AMBER_RESOLUTION_MIN,
        max_stale_sec=AMBER_MAX_STALE_SEC,
        fetch_usage=AMBER_FETCH_USAGE,
        usage_resolution_min=AMBER_USAGE_RESOLUTION_MIN,
    )

    modbus = GoodWeModbus(
        GOODWE_HOST,
        unit=GOODWE_UNIT,
        timeout_sec=MODBUS_TIMEOUT_SEC,
        retries=MODBUS_RETRIES,
        reconnect_on_error=MODBUS_RECONNECT_ON_ERROR,
        reconnect_min_backoff_sec=MODBUS_RECONNECT_MIN_BACKOFF_SEC,
        reconnect_max_backoff_sec=MODBUS_RECONNECT_MAX_BACKOFF_SEC,
    )

    if not modbus.connect():
        print("[error] failed to connect to GoodWe Modbus TCP")
        try:
            amber_poller.close()
        except Exception:
            pass
        return 3

    limiter = GoodWePowerLimiter(
        modbus=modbus,
        rated_w=GOODWE_RATED_W,
        mode=GOODWE_EXPORT_LIMIT_MODE,
        export_switch_reg=GOODWE_EXPORT_SWITCH_REG,
        export_pct_reg=GOODWE_EXPORT_PCT_REG,
        export_pct10_reg=GOODWE_EXPORT_PCT10_REG,
        active_pct_reg=GOODWE_ACTIVE_PCT_REG,
        always_enabled=GOODWE_ALWAYS_ENABLED,
    )

    alpha_poller: Optional[AlphaEssOpenApiPoller] = None
    if ALPHAESS_OPENAPI_ENABLED:
        try:
            alpha_poller = AlphaEssOpenApiPoller(
                app_id=ALPHAESS_APP_ID,
                app_secret=ALPHAESS_APP_SECRET,
                sys_sn=ALPHAESS_SYS_SN,
                unit_index=ALPHAESS_UNIT_INDEX,
                timeout_sec=ALPHAESS_POLL_TIMEOUT_SEC,
                verify_ssl=ALPHAESS_VERIFY_SSL,
            )
        except Exception as e:
            print(f"[error] alphaess init failed: {e}")
            try:
                amber_poller.close()
            except Exception:
                pass
            try:
                modbus.close()
            except Exception:
                pass
            return 4

    reader = GoodWeRuntimeReader(modbus, RUNTIME_PROFILE)

    last_limit_state: Optional[Tuple[int, int]] = None  # (enabled, pct)
    last_write_ts = 0.0

    try:
        while True:
            try:
                # ---- Amber (slow, interval-based) ----
                amber_snap = amber_poller.get_snapshot() if amber_poller else None
                import_c = amber_snap.import_c if amber_snap else None
                feed_c = amber_snap.feedin_c if amber_snap else None

                amber_age_s: Optional[int] = None
                amber_ok = False
                amber_err = None
                amber_import_w = None
                amber_feed_w = None
                amber_interval_end = None
                if amber_snap is not None:
                    amber_age_s = int(time.time() - float(amber_snap.ts))
                    amber_ok = amber_age_s <= int(AMBER_MAX_STALE_SEC) and (feed_c is not None)
                    amber_err = amber_snap.last_error
                    amber_import_w = amber_snap.import_power_w
                    amber_feed_w = amber_snap.feedin_power_w
                    amber_interval_end = amber_snap.interval_end_utc

                # If Amber is stale/unavailable, be conservative: assume export may be costing money.
                export_costs_calc: Optional[str] = None
                if amber_ok:
                    try:
                        fc = float(feed_c) if feed_c is not None else float("nan")
                        th = float(EXPORT_COST_THRESHOLD_C)
                        if math.isnan(fc):
                            export_costs = True
                            export_costs_calc = "feedIn=nan -> costs=True"
                        else:
                            export_costs = fc > th # DO NOT CHANGE THIS IT IS CORRECT
                            export_costs_calc = f"feedIn={fc:.3f}c>{th:.3f}c => costs={export_costs}"
                    except Exception as _e:
                        export_costs = True
                        export_costs_calc = f"feedIn err -> costs=True ({_e})" if DEBUG else "feedIn err -> costs=True"
                    amber_state = "ok"
                else:
                    export_costs = True
                    export_costs_calc = "amber_stale -> costs=True"
                    amber_state = "stale" if amber_snap is not None else "none"

                # ---- GoodWe (fast) ----
                runtime = reader.read()

                gen_w = runtime.gen_power_w if runtime else None
                pv_est_w = runtime.pv_power_est_w if runtime else None
                feed_w = runtime.feed_power_w if runtime else None
                pwr_limit_fn = runtime.power_limit_fn if runtime else None
                meter_ok = runtime.meter_ok if runtime else None
                wifi = runtime.wifi if runtime else None
                temp_c = runtime.inverter_temp_c if runtime else None

                # Some firmwares/maps return 0xFFFF_FFFF for unsupported values, which decodes to -1.
                if gen_w == -1:
                    gen_w = None
                if feed_w == -1:
                    feed_w = None
                # Basic sanity bounds (avoid confusing logs)
                if gen_w is not None and abs(int(gen_w)) > 30000:
                    gen_w = None
                if feed_w is not None and abs(int(feed_w)) > 30000:
                    feed_w = None

                # ---- AlphaESS (fast) ----
                alpha_snap: Optional[AlphaEssSnapshot] = None
                alpha_ok = False
                alpha_age_s: Optional[int] = None
                if alpha_poller:
                    alpha_snap = alpha_poller.get_snapshot()
                    if alpha_snap is not None:
                        alpha_age_s = int(time.time() - alpha_snap.ts)
                        alpha_ok = (alpha_age_s <= ALPHAESS_MAX_STALE_SEC) and (alpha_snap.pload_w is not None)

                # Friendly status strings
                def _fmt_opt(x: Optional[float], suffix: str = "") -> str:
                    if x is None:
                        return "?"
                    if isinstance(x, float):
                        return f"{x:.3f}{suffix}"
                    return f"{x}{suffix}"

                amber_desc = f"amber={amber_state}"
                if DEBUG and export_costs_calc:
                    amber_desc += f"(calc={export_costs_calc})"
                if amber_age_s is not None:
                    amber_desc += f"(age={amber_age_s}s)"
                if amber_interval_end is not None:
                    amber_desc += f"(end={amber_interval_end.isoformat()}Z)"
                if amber_err and DEBUG:
                    amber_desc += f"(err={amber_err})"
                if amber_import_w is not None or amber_feed_w is not None:
                    amber_desc += f" pImport={_fmt_opt(amber_import_w,'W')} pFeed={_fmt_opt(amber_feed_w,'W')}"

                alpha_desc = "alpha=off"
                if alpha_poller:
                    if not alpha_ok:
                        err = alpha_poller.get_last_error()
                        if alpha_snap is None:
                            alpha_desc = f"alpha=err({err})" if err else "alpha=err"
                        else:
                            alpha_desc = f"alpha=stale(age={alpha_age_s}s)"
                    else:
                        soc_s = f"{alpha_snap.soc_pct:.0f}%" if alpha_snap.soc_pct is not None else "?"
                        pbat_s = f"{alpha_snap.pbat_w:+d}W" if alpha_snap.pbat_w is not None else "?"
                        pgrid_s = f"{alpha_snap.pgrid_w:+d}W" if alpha_snap.pgrid_w is not None else "?"
                        pload_s = f"{alpha_snap.pload_w}W" if alpha_snap.pload_w is not None else "?"
                        age_s = f"{alpha_age_s}s" if alpha_age_s is not None else "?"
                        alpha_desc = (
                            f"alpha=ok(age={age_s} soc={soc_s} state={alpha_snap.batt_state} "
                            f"pload={pload_s} pbat={pbat_s} pgrid={pgrid_s})"
                        )
                # Desired inverter power limit (W).
                # If exporting to the grid would cost money (Amber feed-in price < threshold),
                # aim to keep grid export near zero by matching PV output to:
                #   house load + (battery charge, if any)
                # with optional auto-charge headroom when SOC is low.
                if export_costs:
                    if not alpha_ok:
                        target_w = 0
                        target_reason = "alpha_stale" if alpha_poller else "alpha_disabled"
                    else:
                        pload_w = int(alpha_snap.pload_w or 0)
                        measured_charge_w = int(alpha_snap.charge_w or 0)
                        desired_charge_w = int(measured_charge_w)

                        # Optional: if SOC is low and the battery appears idle, leave headroom
                        # for it to begin charging (prevents a "never starts charging" equilibrium).
                        auto_add_w = 0
                        if (
                            alpha_snap.soc_pct is not None
                            and ALPHAESS_AUTO_CHARGE_W > 0
                            and ALPHAESS_AUTO_CHARGE_BELOW_SOC_PCT > 0
                        ):
                            try:
                                soc2 = float(alpha_snap.soc_pct)
                            except Exception:
                                soc2 = None
                            if soc2 is not None and soc2 < float(ALPHAESS_AUTO_CHARGE_BELOW_SOC_PCT):
                                cap = int(max(0, min(int(ALPHAESS_AUTO_CHARGE_W), int(ALPHAESS_AUTO_CHARGE_MAX_W))))
                                if cap > desired_charge_w:
                                    auto_add_w = int(cap - desired_charge_w)
                                    desired_charge_w = int(cap)

                        base_w = int(max(0, pload_w + desired_charge_w))
                        target_w_f = float(base_w)

                        if alpha_snap.pgrid_w is not None:
                            # If importing, allow more PV; if exporting, back PV off.
                            target_w_f += ALPHAESS_GRID_FEEDBACK_GAIN * float(alpha_snap.grid_import_w)
                            target_w_f -= ALPHAESS_GRID_FEEDBACK_GAIN * float(alpha_snap.grid_export_w)

                            # Bias slightly toward import to avoid small accidental export.
                            target_w_f -= float(ALPHAESS_GRID_IMPORT_BIAS_W)

                        target_w = int(round(max(0.0, min(float(GOODWE_RATED_W), target_w_f))))
                        target_reason = f"pload={pload_w}W charge={desired_charge_w}W" + (
                            f"(auto+{auto_add_w}W)" if auto_add_w else ""
                        )
                else:
                    target_w = int(GOODWE_RATED_W)
                    target_reason = "export_allowed"

                target_pct_raw = _pct_from_watts(target_w, GOODWE_RATED_W)

                # Smooth the percentage to avoid rapid oscillation.
                prev_pct = last_limit_state[1] if last_limit_state is not None else target_pct_raw
                if LIMIT_SMOOTHING > 0.0:
                    target_pct = int(
                        round(
                            (float(prev_pct) * float(LIMIT_SMOOTHING))
                            + (float(target_pct_raw) * (1.0 - float(LIMIT_SMOOTHING)))
                        )
                    )
                else:
                    target_pct = int(target_pct_raw)

                target_pct = max(0, min(100, int(target_pct)))
                target_w = int(round((target_pct / 100.0) * GOODWE_RATED_W))

                want_enabled_raw = 1 if export_costs else 0
                want_enabled = 1 if (want_enabled_raw or GOODWE_ALWAYS_ENABLED) else 0
                want_pct = target_pct

                now = time.time()
                can_write = (now - last_write_ts) >= float(MIN_SECONDS_BETWEEN_WRITES)

                current_limit = limiter.read_current()

                print(
                    f"[{_iso_now()}] import={_fmt_opt(import_c,'c')} feedIn={_fmt_opt(feed_c,'c')} "
                    f"thresh={EXPORT_COST_THRESHOLD_C}c export_costs={export_costs} {amber_desc} "
                    f"gen={_fmt_opt(gen_w,'W')} pv_est={_fmt_opt(pv_est_w,'W')} feed={_fmt_opt(feed_w,'W')} "
                    f"pwrLimitFn={_fmt_opt(pwr_limit_fn)} meterOK={_fmt_opt(meter_ok)} wifi={_fmt_opt(wifi,'%')} temp={_fmt_opt(temp_c,'C')} "
                    f"{alpha_desc} reason={target_reason} "
                    f"-> want_limit={want_pct}% (~{target_w}W) (enabled={want_enabled}) "
                    f"cur_limit={current_limit}"
                )

                need_write = False
                if last_limit_state is None:
                    need_write = True
                else:
                    prev_enabled, prev_pct2 = last_limit_state
                    if prev_enabled != want_enabled:
                        need_write = True
                    elif abs(int(prev_pct2) - int(want_pct)) >= int(MIN_PCT_STEP):
                        need_write = True

                if can_write and need_write:
                    try:
                        limiter.set_limit_pct(bool(want_enabled), int(want_pct))
                        last_limit_state = (want_enabled, want_pct)
                        last_write_ts = now
                    except Exception as e:
                        print(f"  [goodwe] write failed: {e}")
                elif not can_write and DEBUG:
                    print("  [goodwe] skipping write (rate limited)")

            except Exception as e:
                print(f"[error] {e}")

            time.sleep(float(SLEEP_SECONDS))

    finally:
        try:
            if amber_poller:
                amber_poller.close()
        except Exception:
            pass
        try:
            if alpha_poller:
                alpha_poller.close()
        except Exception:
            pass
        try:
            modbus.close()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

