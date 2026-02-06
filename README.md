````markdown
# GoodWe + Amber + AlphaESS export-cost guard

This project controls a **GoodWe GW5000-DNS-30** (Modbus-TCP) using:
- **Amber** prices (import + feed-in) to decide whether exporting is financially bad
- **AlphaESS OpenAPI** telemetry (battery SOC / pGrid / load / charge) to keep export near zero *when export would cost money*

It is designed to **avoid paying to export**, while allowing normal production when feed-in is positive.

---

## Quick start

### 1) Install requirements
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
````

### 2) Create `.env`

Copy `env.example` to `.env` and fill in at least:

* `AMBER_API_KEY`
* `AMBER_SITE_ID`
* `GOODWE_HOST`
* `GOODWE_UNIT`
* `ALPHAESS_APP_ID`
* `ALPHAESS_APP_SECRET`
* `ALPHAESS_SYS_SN` (or use the numeric index shortcut)

```bash
cp env.example .env
nano .env
```

### 3) Run

`start.sh` exports variables from `.env` and runs `control.py`:

```bash
./start.sh
```

Or run directly (make sure env vars are exported):

```bash
set -a; source .env; set +a
python3 control.py
```

### 4) Emergency 'turn limiter off'

`turnoff.py` writes register 256 back to 100%.

```bash
python3 turnoff.py --host 192.168.1.10 --port 502 --unit 247 --value 100
```

---

## What the controller does (logic)

Every loop, it:

1. **Fetches Amber current prices**

* `import` = cents/kWh you pay
* `feedIn` = cents/kWh you receive (can be negative)

2. Computes:

* `export_costs = (feedIn < EXPORT_COST_THRESHOLD_C)`

  * Default threshold is `0.0c`
  * Meaning: if feed-in becomes negative, exporting costs money.

* If you want to avoid exporting unless feed-in is above some minimum (even if it’s still positive), set `EXPORT_COST_THRESHOLD_C` to that value (e.g. `1.0`).

3. **Fail-safe behaviour**

* If Amber is stale/unavailable beyond `AMBER_MAX_STALE_SEC`, the script assumes exporting **may** be costly.
* If exporting is assumed costly AND AlphaESS data is stale/unavailable, it sets GoodWe output to **0%** (stop generation) to avoid accidental export.

---

## Control behaviour when export would cost money

When `export_costs=True` (i.e. `feedIn < EXPORT_COST_THRESHOLD_C`), the controller aims to keep **grid export near zero** by limiting the GW5000 to roughly:

* **target PV** ≈ `pload + battery_charge` (then trimmed using `pGrid` feedback)
* a small **import bias** (`ALPHAESS_GRID_IMPORT_BIAS_W`) is applied to avoid tiny accidental exports

This naturally does what you want near full battery: as the battery charge tapers down, `battery_charge` drops, so the target drops and the GW5000 backs off automatically — no extra “near‑full SOC” threshold is required.

Optional “auto charge headroom” (helps charging start when SOC is low):

* If `SOC < ALPHAESS_AUTO_CHARGE_BELOW_SOC_PCT` and `ALPHAESS_AUTO_CHARGE_W > 0`, the controller will assume the battery can absorb up to `ALPHAESS_AUTO_CHARGE_W` (clamped by `ALPHAESS_AUTO_CHARGE_MAX_W`) and will leave PV headroom accordingly.
* Set `ALPHAESS_AUTO_CHARGE_W=0` to disable this behaviour.

## When export does NOT cost money

If `export_costs=False`, the controller requests **100% output**.

Whether it also disables GoodWe’s export limit function depends on:

* `GOODWE_ALWAYS_ENABLED=1` (default): keep limiting enabled but set % to 100
* `GOODWE_ALWAYS_ENABLED=0`: disable export limiting when export is allowed

---

## Sign conventions (important)

AlphaESS values can have different sign conventions depending on firmware/endpoint.

This script normalises internally to:

* `pBat`: **+ charging**, **- discharging**
* `pGrid`: **+ import**, **- export**

If your readings look backwards in logs, flip:

* `ALPHAESS_PBAT_POSITIVE_IS_CHARGE`
* `ALPHAESS_PGRID_POSITIVE_IS_IMPORT`

Example:
If the log shows `pgrid=+200W` while you are clearly exporting, set:
`ALPHAESS_PGRID_POSITIVE_IS_IMPORT=0`

---

## GoodWe registers and modes

Two limiter modes exist:

### `GOODWE_EXPORT_LIMIT_MODE=active_pct` (recommended)

* Writes the active power limit percentage into:

  * `GOODWE_ACTIVE_PCT_REG` (default 256)
* Also writes `GOODWE_EXPORT_SWITCH_REG` (default 291) on/off

### `GOODWE_EXPORT_LIMIT_MODE=pct`

* Writes the percentage into:

  * `GOODWE_EXPORT_PCT_REG` (default 292)
  * `GOODWE_EXPORT_PCT10_REG` (default 293) as % × 10
* Also writes `GOODWE_EXPORT_SWITCH_REG` (default 291) on/off

---

## Modbus reliability / reconnect

The Modbus client includes:

* Compatibility across pymodbus versions (`unit=` vs `slave=` vs `device_id=`)
* Auto-reconnect on common socket errors (Broken pipe, reset, timeout, etc.)

Tune with:

* `MODBUS_RECONNECT_ON_ERROR`
* `MODBUS_RECONNECT_MIN_BACKOFF_SEC`
* `MODBUS_RECONNECT_MAX_BACKOFF_SEC`

---

## Tuning / troubleshooting

### `It keeps limiting even when feedIn is positive`

* Confirm your pricing sign:

  * feedIn > 0 should mean you’re paid
  * feedIn < 0 means you pay
* `EXPORT_COST_THRESHOLD_C` should usually be `0.0`

### `pGrid looks wrong`

Flip `ALPHAESS_PGRID_POSITIVE_IS_IMPORT`.

### `Battery never shows charging/discharging correctly`

Flip `ALPHAESS_PBAT_POSITIVE_IS_CHARGE` or adjust `ALPHAESS_PBAT_IDLE_THRESHOLD_W`.

### `Too many writes / oscillation`

Increase:

* `MIN_SECONDS_BETWEEN_WRITES`
* `LIMIT_SMOOTHING`
* `MIN_PCT_STEP`

### Enable deeper logs

Set:

* `DEBUG=1`

---

## Safety note

This controller can materially affect inverter output.
Test carefully, start with conservative settings, and monitor logs when making changes.

```
