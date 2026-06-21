#!/usr/bin/env python3
"""
EURUSDT - Institutional Quant Suite
Tick-Data Engine - Volume Profile - TPO - Footprint - Heatmap
localhost:5000  |  pip install flask websocket-client requests

DATA ENGINE
  SQLite tick store (raw aggTrades), resumable 2-month backfill
  Binance aggTrades REST, paginated 1000 trades/call
  Gap detection + repair, automatic pruning past 2 months
  Hands off to live WebSocket once caught up to real-time
  Multi-timeframe bar/footprint aggregation from raw ticks

TABS
  1. ORDER BOOK    - full 5000-level, resting/structural labels
  2. HEATMAP       - quant-grade, per-frame normalized, voids
  3. VOLUME PROFILE- session + fixed, POC/VAH/VAL, HVN/LVN, naked
  4. TPO           - lettered periods, IB, value area, singles
  5. FOOTPRINT     - bid x ask per price per bar, stacked imbalance
  6. CLUSTERS      - persistent magnet zones
  7. ABSORPTION    - large aggression absorbed without move
  8. STOP ZONES    - swing-structure stop-loss liquidity map
  9. PREDICTION    - Monte Carlo GBM, Kalman, Hurst, Entropy
 10. REPORT        - full institutional market analysis
"""

import json, math, random, statistics, datetime, calendar, threading, time, collections, sqlite3, os
import websocket, requests
from flask import Flask, jsonify, request

# ============================================================
#  CONFIG
# ============================================================
SYMBOL       = "eurusdt"
SYM_UP       = "EURUSDT"
PORT         = 5000
DEPTH_LIMIT  = 5000
DISPLAY_DEPTH = 50

SCAN_INTERVAL_S      = 5.0
MIN_STRUCTURAL_AGE_S = 300
RESTING_GRACE_S      = 600
TOP_RESTING_SHOWN    = 24
SIG_WINDOW           = 25
SIG_LOCAL_MULT       = 4.0
SIG_GLOBAL_MULT      = 2.0
MIN_BASELINE         = 1e-6
PROFILE_BUCKET       = 0.0001
ROUND_STEP           = 0.0050
ROUND_TOL            = 0.0003
MAX_EVENTS           = 300

HEAT_INTERVAL_S  = 2.0
HEAT_MAX_FRAMES  = 200
HEAT_HALF_WIDTH  = 150
BUBBLE_ALPHA     = 0.05
BUBBLE_MULT      = 3.0
BUBBLE_MIN       = 0.05
MAX_BUBBLES      = 150

PRICE_HIST_LEN  = 600
MC_PATHS        = 300
MC_HOURS        = 5
CACHE_TTL       = 30

DB_PATH                = os.path.join(os.path.dirname(os.path.abspath(__file__)), "eurusdt_ticks.db")
BACKFILL_DAYS          = 60
LIVE_FLUSH_INTERVAL_S  = 2.0
MAINTENANCE_INTERVAL_S = 1800
BACKFILL_RATE_SLEEP_S  = 0.15
MAX_FETCH_RETRIES      = 5
DEFAULT_TICK_SIZE      = 0.0001

REST_URL      = f"https://api.binance.com/api/v3/depth?symbol={SYM_UP}&limit={DEPTH_LIMIT}"
AGGTRADES_URL = "https://api.binance.com/api/v3/aggTrades"
WS_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    f"{SYMBOL}@depth@100ms/{SYMBOL}@aggTrade"
)

# ============================================================
#  TICK-DATA ENGINE
# ============================================================
def db_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_schema(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS ticks (
        agg_id  INTEGER PRIMARY KEY,
        price   REAL NOT NULL,
        qty     REAL NOT NULL,
        ts      INTEGER NOT NULL,
        is_sell INTEGER NOT NULL
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ticks_ts ON ticks(ts)")
    conn.execute("""CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY, value TEXT
    )""")
    conn.commit()


def get_meta(conn, key, default=None):
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def set_meta(conn, key, value):
    conn.execute(
        "INSERT INTO meta(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)))
    conn.commit()


def insert_ticks(conn, trades):
    if not trades:
        return 0
    rows = [(t["a"], float(t["p"]), float(t["q"]), int(t["T"]), 1 if t["m"] else 0) for t in trades]
    conn.executemany(
        "INSERT OR IGNORE INTO ticks(agg_id,price,qty,ts,is_sell) VALUES (?,?,?,?,?)", rows)
    conn.commit()
    return len(rows)


def prune_old_ticks(conn):
    cutoff = int((time.time() - BACKFILL_DAYS * 86400) * 1000)
    cur = conn.execute("DELETE FROM ticks WHERE ts < ?", (cutoff,))
    conn.commit()
    return cur.rowcount


def find_gaps(conn, limit=500):
    return conn.execute("""
        SELECT gstart, gend FROM (
            SELECT agg_id+1 AS gstart,
                   LEAD(agg_id) OVER (ORDER BY agg_id) - 1 AS gend
            FROM ticks
        ) WHERE gend IS NOT NULL AND gend >= gstart
        LIMIT ?
    """, (limit,)).fetchall()


def fetch_agg_trades(from_id=None, start_time=None, limit=1000):
    params = {"symbol": SYM_UP, "limit": limit}
    if from_id is not None:
        params["fromId"] = from_id
    elif start_time is not None:
        params["startTime"] = start_time
    backoff = 2
    for _ in range(MAX_FETCH_RETRIES):
        try:
            r = requests.get(AGGTRADES_URL, params=params, timeout=15)
            if r.status_code in (418, 429):
                wait = max(backoff, int(r.headers.get("Retry-After", backoff)))
                print(f"[BACKFILL] Rate limited ({r.status_code}) - waiting {wait}s")
                time.sleep(wait)
                backoff = min(backoff * 2, 120)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            print(f"[BACKFILL] Request error: {e} - retrying in {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)
    return []


def repair_gaps(conn):
    gaps = find_gaps(conn)
    if not gaps:
        return 0
    print(f"[BACKFILL] Found {len(gaps)} gap(s) in tick history - repairing")
    filled = 0
    for gstart, gend in gaps:
        fid = gstart
        guard = 0
        while fid <= gend and guard < 500:
            guard += 1
            trades = fetch_agg_trades(from_id=fid, limit=1000)
            if not trades:
                break
            trades = [t for t in trades if t["a"] <= gend]
            if trades:
                filled += insert_ticks(conn, trades)
                fid = trades[-1]["a"] + 1
            else:
                break
            if fid > gend:
                break
            time.sleep(BACKFILL_RATE_SLEEP_S)
    return filled


def session_start_ms():
    now = datetime.datetime.utcnow()
    start = datetime.datetime(now.year, now.month, now.day)
    return int(calendar.timegm(start.timetuple()) * 1000)


def session_start_ms_for_date(date_str):
    y, m, d = map(int, date_str.split("-"))
    dt = datetime.datetime(y, m, d)
    return int(calendar.timegm(dt.timetuple()) * 1000)


backfill_progress = {
    "status": "starting", "synced_days": 0.0, "target_days": BACKFILL_DAYS,
    "last_synced_ts": None, "pct": 0.0, "ticks_stored": 0,
}
backfill_lock = threading.Lock()


def update_progress(**kw):
    with backfill_lock:
        backfill_progress.update(kw)


def backfill_loop():
    conn = db_conn()
    init_schema(conn)
    load_poc_history(conn)

    update_progress(status="checking gaps")
    repair_gaps(conn)

    target_start_ms = int((time.time() - BACKFILL_DAYS * 86400) * 1000)
    last_id_meta = get_meta(conn, "last_synced_id")

    if last_id_meta is None:
        first = fetch_agg_trades(start_time=target_start_ms, limit=1)
        if not first:
            print("[BACKFILL] No trades returned for start window - going live with empty history")
            update_progress(status="live", pct=100.0)
            conn.close()
            maintenance_forever()
            return
        next_id = first[0]["a"]
        print(f"[BACKFILL] Fresh backfill starting {BACKFILL_DAYS}d back, first agg_id={next_id}")
    else:
        next_id = int(last_id_meta) + 1
        print(f"[BACKFILL] Resuming from last synced agg_id={last_id_meta}")

    update_progress(status="backfilling")

    while True:
        trades = fetch_agg_trades(from_id=next_id, limit=1000)
        if not trades:
            time.sleep(2)
            continue
        insert_ticks(conn, trades)
        last_t = trades[-1]
        next_id = last_t["a"] + 1
        set_meta(conn, "last_synced_id", last_t["a"])
        set_meta(conn, "last_synced_ts", last_t["T"])

        cur_now = int(time.time() * 1000)
        synced_days = max(0.0, (last_t["T"] - target_start_ms) / 86400000.0)
        pct = max(0.0, min(100.0, synced_days / BACKFILL_DAYS * 100.0))
        count_row = conn.execute("SELECT COUNT(*) FROM ticks").fetchone()
        update_progress(last_synced_ts=last_t["T"], synced_days=round(synced_days, 2),
                         pct=round(pct, 1), ticks_stored=count_row[0])

        if cur_now - last_t["T"] < 5000:
            break
        time.sleep(BACKFILL_RATE_SLEEP_S)

    prune_old_ticks(conn)
    update_progress(status="live", pct=100.0)
    print("[BACKFILL] Caught up to live - handing off to WebSocket tick ingestion")
    conn.close()
    maintenance_forever()


def maintenance_forever():
    while True:
        time.sleep(MAINTENANCE_INTERVAL_S)
        try:
            c2 = db_conn()
            repaired = repair_gaps(c2)
            pruned = prune_old_ticks(c2)
            count_row = c2.execute("SELECT COUNT(*) FROM ticks").fetchone()
            update_progress(ticks_stored=count_row[0])
            if repaired or pruned:
                print(f"[MAINTENANCE] repaired={repaired} pruned={pruned}")
            c2.close()
        except Exception as e:
            print(f"[MAINTENANCE] error: {e}")


live_tick_buffer = []
live_tick_lock = threading.Lock()


def buffer_live_tick(agg_id, price, qty, ts, is_sell):
    with live_tick_lock:
        live_tick_buffer.append((agg_id, price, qty, ts, 1 if is_sell else 0))


def tick_flush_loop():
    while True:
        time.sleep(LIVE_FLUSH_INTERVAL_S)
        with live_tick_lock:
            batch = live_tick_buffer[:]
            live_tick_buffer.clear()
        if not batch:
            continue
        try:
            conn = db_conn()
            conn.executemany(
                "INSERT OR IGNORE INTO ticks(agg_id,price,qty,ts,is_sell) VALUES (?,?,?,?,?)", batch)
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[TICKSTORE] flush error: {e}")


def get_bars(conn, tf_seconds, lookback_bars, tick_size, max_ticks=80000):
    lookback_bars = max(1, min(lookback_bars, 400))
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - lookback_bars * tf_seconds * 1000
    rows = conn.execute(
        "SELECT price, qty, ts, is_sell FROM ticks WHERE ts >= ? ORDER BY ts DESC LIMIT ?",
        (start_ms, max_ticks)).fetchall()
    rows.reverse()
    tf_ms = tf_seconds * 1000
    bars = {}
    for price, qty, ts, is_sell in rows:
        bucket = (ts // tf_ms) * tf_ms
        b = bars.get(bucket)
        if b is None:
            b = {"o": price, "h": price, "l": price, "c": price,
                 "buy_vol": 0.0, "sell_vol": 0.0, "cells": {}}
            bars[bucket] = b
        if price > b["h"]: b["h"] = price
        if price < b["l"]: b["l"] = price
        b["c"] = price
        pb = round(price / tick_size) * tick_size
        pbk = f"{pb:.5f}"
        cell = b["cells"].get(pbk)
        if cell is None:
            cell = {"buy": 0.0, "sell": 0.0}
            b["cells"][pbk] = cell
        if is_sell:
            b["sell_vol"] += qty
            cell["sell"] += qty
        else:
            b["buy_vol"] += qty
            cell["buy"] += qty
    ordered = sorted(bars.items())
    return ordered[-lookback_bars:]


# ============================================================
#  VOLUME PROFILE
# ============================================================
def compute_volume_profile(conn, mode, tf_seconds, lookback_bars, tick_size, va_pct, max_ticks=150000):
    if mode == "session":
        start_ms = session_start_ms()
    else:
        lookback_bars = max(1, min(lookback_bars, 400))
        start_ms = int(time.time() * 1000) - lookback_bars * tf_seconds * 1000

    rows = conn.execute(
        "SELECT price, qty, is_sell FROM ticks WHERE ts >= ? ORDER BY ts DESC LIMIT ?",
        (start_ms, max_ticks)).fetchall()
    if not rows:
        return None

    bucket_map = {}
    for price, qty, is_sell in rows:
        pb = round(price / tick_size) * tick_size
        pbk = f"{pb:.5f}"
        e = bucket_map.get(pbk)
        if e is None:
            e = {"buy": 0.0, "sell": 0.0}
            bucket_map[pbk] = e
        if is_sell: e["sell"] += qty
        else: e["buy"] += qty

    levels = [{"price": p, "buy": round(v["buy"], 4), "sell": round(v["sell"], 4),
               "total": round(v["buy"] + v["sell"], 4)} for p, v in bucket_map.items()]
    levels.sort(key=lambda x: float(x["price"]))

    poc_idx = max(range(len(levels)), key=lambda i: levels[i]["total"])
    total_vol = sum(l["total"] for l in levels)
    target = total_vol * va_pct
    lo = hi = poc_idx
    acc = levels[poc_idx]["total"]
    while acc < target and (lo > 0 or hi < len(levels) - 1):
        lower_val = levels[lo - 1]["total"] if lo > 0 else -1
        upper_val = levels[hi + 1]["total"] if hi < len(levels) - 1 else -1
        if upper_val >= lower_val:
            hi += 1; acc += levels[hi]["total"]
        else:
            lo -= 1; acc += levels[lo]["total"]
    vah = levels[hi]["price"]; val = levels[lo]["price"]

    avg = total_vol / len(levels) if levels else 0
    for i, l in enumerate(levels):
        if l["total"] > avg * 1.5: l["node"] = "HVN"
        elif l["total"] < avg * 0.5: l["node"] = "LVN"
        else: l["node"] = "NORMAL"
        l["in_value_area"] = (lo <= i <= hi)

    return {"levels": levels, "poc": levels[poc_idx]["price"], "vah": vah, "val": val,
            "total_volume": round(total_vol, 4), "mode": mode, "tick_size": tick_size,
            "va_pct": va_pct, "start_ms": start_ms}


session_poc_history = []


def load_poc_history(conn):
    global session_poc_history
    raw = get_meta(conn, "session_poc_history")
    if raw:
        try:
            session_poc_history = json.loads(raw)
        except Exception:
            session_poc_history = []


def save_poc_history(conn):
    set_meta(conn, "session_poc_history", json.dumps(session_poc_history[-20:]))


def finalize_session_poc(conn, day_start_ms, day_end_ms, date_str):
    rows = conn.execute(
        "SELECT price, qty FROM ticks WHERE ts >= ? AND ts < ?", (day_start_ms, day_end_ms)).fetchall()
    if not rows:
        return
    bucket_map = {}
    for price, qty in rows:
        pb = round(round(price / DEFAULT_TICK_SIZE) * DEFAULT_TICK_SIZE, 5)
        bucket_map[pb] = bucket_map.get(pb, 0.0) + qty
    if not bucket_map:
        return
    poc_price = max(bucket_map.items(), key=lambda kv: kv[1])[0]
    session_poc_history.append({"date": date_str, "poc": poc_price, "end_ms": day_end_ms, "tested": False})
    save_poc_history(conn)


def check_naked_pocs(conn):
    changed = False
    for h in session_poc_history:
        if h.get("tested"):
            continue
        poc_price = float(h["poc"])
        row = conn.execute(
            "SELECT 1 FROM ticks WHERE ts > ? AND price BETWEEN ? AND ? LIMIT 1",
            (h["end_ms"], poc_price - 0.00006, poc_price + 0.00006)).fetchone()
        if row:
            h["tested"] = True
            changed = True
    if changed:
        save_poc_history(conn)
    return session_poc_history[-10:]


# ============================================================
#  TPO (TIME PRICE OPPORTUNITY / MARKET PROFILE)
# ============================================================
def compute_tpo(conn, period_min, lookback_periods, tick_size, va_pct, max_ticks=150000):
    period_min = max(1, min(period_min, 120))
    lookback_periods = max(2, min(lookback_periods, 48))
    period_ms = period_min * 60 * 1000
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - lookback_periods * period_ms

    rows = conn.execute(
        "SELECT price, ts FROM ticks WHERE ts >= ? ORDER BY ts DESC LIMIT ?",
        (start_ms, max_ticks)).fetchall()
    if not rows:
        return None
    rows.reverse()

    letters_pool = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    periods = {}
    for price, ts in rows:
        idx = int((ts - start_ms) // period_ms)
        pb = round(round(price / tick_size) * tick_size, 5)
        periods.setdefault(idx, set()).add(pb)
    if not periods:
        return None

    sorted_idx = sorted(periods.keys())
    idx_to_letter = {idx: letters_pool[i % len(letters_pool)] for i, idx in enumerate(sorted_idx)}
    row_tpo = {}
    for idx in sorted_idx:
        letter = idx_to_letter[idx]
        for pb in periods[idx]:
            row_tpo.setdefault(pb, []).append(letter)
    rows_list = sorted(row_tpo.items())

    poc_pos = max(range(len(rows_list)), key=lambda i: len(rows_list[i][1]))
    total_tpo = sum(len(v) for _, v in rows_list)
    target = total_tpo * va_pct
    lo = hi = poc_pos
    acc = len(rows_list[poc_pos][1])
    while acc < target and (lo > 0 or hi < len(rows_list) - 1):
        lower_c = len(rows_list[lo - 1][1]) if lo > 0 else -1
        upper_c = len(rows_list[hi + 1][1]) if hi < len(rows_list) - 1 else -1
        if upper_c >= lower_c:
            hi += 1; acc += len(rows_list[hi][1])
        else:
            lo -= 1; acc += len(rows_list[lo][1])
    vah = rows_list[hi][0]; val = rows_list[lo][0]

    first_letter = idx_to_letter[sorted_idx[0]]
    ib_prices = [p for p, ls in rows_list if first_letter in ls]
    ib_high = max(ib_prices) if ib_prices else None
    ib_low = min(ib_prices) if ib_prices else None
    single_prints = [p for p, ls in rows_list if len(ls) == 1]

    return {
        "rows": [{"price": p, "letters": "".join(ls), "count": len(ls)} for p, ls in rows_list],
        "poc": rows_list[poc_pos][0], "vah": vah, "val": val,
        "ib_high": ib_high, "ib_low": ib_low, "single_prints": single_prints,
        "period_min": period_min, "n_periods": len(sorted_idx), "value_area_pct": va_pct,
    }


# ============================================================
#  FOOTPRINT
# ============================================================
def detect_stacked(rows_sorted, imbalance_flags, min_run=3):
    stacked = set()
    run_type, run_prices = None, []
    for p, _ in rows_sorted:
        flag = imbalance_flags.get(p, "")
        cur_type = "BUY" if "BUY_IMBALANCE" in flag else ("SELL" if "SELL_IMBALANCE" in flag else None)
        if cur_type and cur_type == run_type:
            run_prices.append(p)
        else:
            if run_type and len(run_prices) >= min_run:
                stacked.update(run_prices)
            run_type = cur_type
            run_prices = [p] if cur_type else []
    if run_type and len(run_prices) >= min_run:
        stacked.update(run_prices)
    return stacked


def compute_footprint(conn, tf_seconds, n_bars, tick_size, imb_ratio):
    n_bars = max(5, min(n_bars, 80))
    bars = get_bars(conn, tf_seconds, n_bars, tick_size)
    out, cum_delta = [], 0.0

    for bucket_ms, b in bars:
        cells = b["cells"]
        rows_sorted = sorted(cells.items(), key=lambda x: float(x[0]))
        imbalance_flags = {}
        for p, c in rows_sorted:
            lower_key = f"{round(float(p) - tick_size, 5):.5f}"
            lower_sell = cells.get(lower_key, {}).get("sell", 0.0)
            if lower_sell > 0 and c["buy"] >= lower_sell * imb_ratio:
                imbalance_flags[p] = "BUY_IMBALANCE"
            higher_key = f"{round(float(p) + tick_size, 5):.5f}"
            higher_buy = cells.get(higher_key, {}).get("buy", 0.0)
            if higher_buy > 0 and c["sell"] >= higher_buy * imb_ratio:
                existing = imbalance_flags.get(p, "")
                imbalance_flags[p] = (existing + "|" if existing else "") + "SELL_IMBALANCE"

        stacked = detect_stacked(rows_sorted, imbalance_flags)
        bar_poc = max(rows_sorted, key=lambda x: x[1]["buy"] + x[1]["sell"])[0] if rows_sorted else None
        total_vol = b["buy_vol"] + b["sell_vol"]
        delta = b["buy_vol"] - b["sell_vol"]
        cum_delta += delta
        out.append({
            "ts": bucket_ms, "o": round(b["o"], 5), "h": round(b["h"], 5),
            "l": round(b["l"], 5), "c": round(b["c"], 5),
            "buy_vol": round(b["buy_vol"], 4), "sell_vol": round(b["sell_vol"], 4),
            "total_vol": round(total_vol, 4), "delta": round(delta, 4),
            "cum_delta": round(cum_delta, 4), "poc": bar_poc,
            "cells": [{"price": p, "buy": round(c["buy"], 4), "sell": round(c["sell"], 4),
                       "imbalance": imbalance_flags.get(p, ""), "stacked": p in stacked}
                      for p, c in rows_sorted],
        })
    return out


# ============================================================
#  MATH ENGINES
# ============================================================
class KalmanFilter:
    def __init__(self):
        self.x = None
        self.P = 1.0
        self.Q = 1e-6
        self.R = 1e-4

    def update(self, z):
        if self.x is None:
            self.x = z
            return self.x
        P2 = self.P + self.Q
        K = P2 / (P2 + self.R)
        self.x = self.x + K * (z - self.x)
        self.P = (1.0 - K) * P2
        return self.x


def hurst_exponent(prices):
    n = len(prices)
    if n < 20:
        return 0.5
    pairs = []
    for w in [max(5, n // 8), max(5, n // 4), max(5, n // 2), n]:
        sub = prices[-w:]
        m = sum(sub) / len(sub)
        dev = [p - m for p in sub]
        cum, run = [], 0.0
        for d in dev:
            run += d
            cum.append(run)
        R = max(cum) - min(cum)
        var = sum((p - m) ** 2 for p in sub) / len(sub)
        S = math.sqrt(var) if var > 0 else 1e-10
        if R > 0 and S > 0:
            pairs.append((math.log(len(sub)), math.log(R / S)))
    if len(pairs) < 2:
        return 0.5
    xs = [v[0] for v in pairs]; ys = [v[1] for v in pairs]
    mx = sum(xs) / len(xs); my = sum(ys) / len(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs)
    return max(0.0, min(1.0, num / den)) if den > 0 else 0.5


def shannon_entropy(vals, bins=30):
    if len(vals) < 2:
        return 0.0
    lo, hi = min(vals), max(vals)
    if lo == hi:
        return 0.0
    bw = (hi - lo) / bins
    cnt = [0] * bins
    for v in vals:
        cnt[min(int((v - lo) / bw), bins - 1)] += 1
    n = len(vals)
    H = 0.0
    for c in cnt:
        if c > 0:
            p = c / n
            H -= p * math.log2(p)
    return H


def _rolling_mean_excl(qtys, win):
    n = len(qtys); half = win // 2
    pre = [0.0] * (n + 1)
    for i, q in enumerate(qtys):
        pre[i + 1] = pre[i] + q
    out = []
    for i in range(n):
        lo = max(0, i - half); hi = min(n, i + half + 1)
        tot = pre[hi] - pre[lo]; cnt = hi - lo
        out.append((tot - qtys[i]) / max(cnt - 1, 1))
    return out


def find_sig_levels(levels, side):
    n = len(levels)
    if n < 8:
        return []
    qtys = [q for _, q in levels]
    gmed = statistics.median(qtys)
    lmean = _rolling_mean_excl(qtys, SIG_WINDOW)
    out = []
    for i, (p, q) in enumerate(levels):
        base = max(lmean[i], MIN_BASELINE)
        if q / base >= SIG_LOCAL_MULT and q / max(gmed, MIN_BASELINE) >= SIG_GLOBAL_MULT:
            out.append({"price": p, "side": side, "qty": q})
    return out


def compute_prediction(ph):
    prices = list(ph)
    if len(prices) < 20:
        return None
    lr = [math.log(prices[i] / prices[i - 1]) for i in range(1, len(prices)) if prices[i - 1] > 0]
    if len(lr) < 5:
        return None
    n = len(lr)
    mu_s = sum(lr) / n
    var_s = sum((r - mu_s) ** 2 for r in lr) / max(n - 1, 1)
    sig_s = math.sqrt(var_s) if var_s > 0 else 1e-6
    sph = 360.0
    mu_h = mu_s * sph
    sig_h = sig_s * math.sqrt(sph)
    H = hurst_exponent(prices)
    cur = kf.x if kf.x else prices[-1]
    dt = 1.0 / 60
    steps = MC_HOURS * 60
    terms = []
    for _ in range(MC_PATHS):
        p = cur
        for _ in range(steps):
            z = random.gauss(0, 1)
            p *= math.exp((mu_h - 0.5 * sig_h ** 2) * dt + sig_h * math.sqrt(dt) * z)
        terms.append(p)
    terms.sort()
    nt = len(terms)
    p5 = terms[max(0, int(nt * 0.05))]; p25 = terms[max(0, int(nt * 0.25))]
    p50 = terms[max(0, int(nt * 0.50))]; p75 = terms[max(0, int(nt * 0.75))]
    p95 = terms[max(0, int(nt * 0.95))]
    ent = shannon_entropy(terms, bins=30)
    ne = ent / math.log2(30) if math.log2(30) > 0 else 0
    hadj = max(0, (H - 0.5) * 0.4)
    conf = max(0, min(100, (1 - ne) * 75 + hadj * 100))
    regime = "TRENDING" if H > 0.55 else ("MEAN-REVERTING" if H < 0.45 else "NEUTRAL")
    bias = "BULLISH" if p50 > cur else "BEARISH"
    return {"current": round(cur, 5), "p5": round(p5, 5), "p25": round(p25, 5),
            "p50": round(p50, 5), "p75": round(p75, 5), "p95": round(p95, 5),
            "confidence": round(conf, 1), "entropy": round(ent, 3), "hurst": round(H, 3),
            "mu_h_pct": round(mu_h * 100, 4), "sig_h_pct": round(sig_h * 100, 4),
            "regime": regime, "bias": bias, "hours": MC_HOURS, "n_paths": MC_PATHS}


def compute_clusters(frames_list):
    if len(frames_list) < 5:
        return []
    nf = len(frames_list)
    pstat = {}
    for fr in frames_list:
        for p, q in fr.get("bids", {}).items():
            if p not in pstat: pstat[p] = {"c": 0, "t": 0.0, "side": "BID"}
            pstat[p]["c"] += 1; pstat[p]["t"] += q
        for p, q in fr.get("asks", {}).items():
            if p not in pstat: pstat[p] = {"c": 0, "t": 0.0, "side": "ASK"}
            pstat[p]["c"] += 1; pstat[p]["t"] += q
    min_c = max(3, int(nf * 0.25))
    cands = [{"price": p, "side": s["side"], "avg_qty": round(s["t"] / s["c"], 4),
              "persist": round(s["c"] / nf, 3), "count": s["c"]}
             for p, s in pstat.items() if s["c"] >= min_c]
    if not cands:
        return []
    qtys = [c["avg_qty"] for c in cands]
    med = statistics.median(qtys)
    try: sd = statistics.stdev(qtys)
    except Exception: sd = 0.01
    thresh = med + 1.5 * sd
    cands = [c for c in cands if c["avg_qty"] >= thresh]
    cands.sort(key=lambda c: c["avg_qty"] * c["persist"], reverse=True)
    return cands[:30]


def compute_absorption(frames_list, trades_list):
    if not trades_list or len(frames_list) < 3:
        return []
    zs = 0.0005
    zones = {}
    for tr in trades_list:
        zk = f"{round(tr['price'] / zs) * zs:.4f}"
        if zk not in zones: zones[zk] = {"vol": 0.0, "count": 0}
        zones[zk]["vol"] += tr["qty"]; zones[zk]["count"] += 1
    out = []
    for zk, st in zones.items():
        zp = float(zk)
        near = [f["mid"] for f in frames_list if abs(f["mid"] - zp) < zs * 4]
        if len(near) < 2:
            continue
        rng = max(near) - min(near)
        score = min(100.0, st["vol"] / max(rng * 10000, 1e-6))
        if score >= 5:
            out.append({"price": zk, "vol": round(st["vol"], 4), "count": st["count"],
                        "range_pips": round(rng * 10000, 2), "score": round(score, 1)})
    out.sort(key=lambda z: z["score"], reverse=True)
    return out[:20]


def compute_stop_zones(ph):
    prices = list(ph)
    n = len(prices)
    if n < 30:
        return {"sell_stops": [], "buy_stops": [], "atr_pips": 0, "swing_highs": [], "swing_lows": []}
    ranges = [abs(prices[i] - prices[i - 1]) for i in range(1, n)]
    atr = sum(ranges[-14:]) / min(14, len(ranges)) if ranges else 0.0001
    win = 5
    highs, lows = [], []
    for i in range(win, n - win):
        seg = prices[i - win:i + win + 1]
        if prices[i] == max(seg): highs.append(prices[i])
        if prices[i] == min(seg): lows.append(prices[i])
    offset = max(0.0003, atr * 0.25)
    u_highs = sorted(set(round(h, 5) for h in highs), reverse=True)[:8]
    u_lows = sorted(set(round(l, 5) for l in lows))[:8]
    sell_stops = [{"price": round(h + offset, 5), "swing": round(h, 5), "type": "SELL_STOP",
                   "density": round(min(1.0, atr / max(offset, 1e-8)), 3)} for h in u_highs]
    buy_stops = [{"price": round(l - offset, 5), "swing": round(l, 5), "type": "BUY_STOP",
                  "density": round(min(1.0, atr / max(offset, 1e-8)), 3)} for l in u_lows]
    return {"sell_stops": sell_stops, "buy_stops": buy_stops, "atr_pips": round(atr * 10000, 2),
            "swing_highs": [round(h, 5) for h in u_highs], "swing_lows": [round(l, 5) for l in u_lows]}


def _gen_analysis(mid, pred, resting, stops, magnet, vp):
    if not mid:
        return "Waiting for live market data..."
    lines = [f"EURUSDT is currently trading at {mid:.5f}."]
    if pred:
        lines.append(
            f"The mathematical regime is {pred.get('regime','?')} (Hurst H={pred.get('hurst','?')}). "
            f"The 5-hour Monte Carlo model ({pred.get('n_paths',0)} paths) shows a "
            f"{pred.get('bias','?')} bias with {pred.get('confidence',0):.0f}% confidence and "
            f"Shannon entropy of {pred.get('entropy','?')} bits. Median projected price is "
            f"{pred.get('p50','?')} (95% range: {pred.get('p5','?')} - {pred.get('p95','?')})."
        )
    if vp:
        lines.append(
            f"Session Volume Profile POC is {vp.get('poc','?')}, value area "
            f"{vp.get('val','?')} - {vp.get('vah','?')}."
        )
    struct = [r for r in resting if r.get("structural") and r["status"] == "RESTING"]
    if struct:
        top = struct[0]
        lines.append(
            f"Dominant structural resting order: {top['side']} at {top['price']} "
            f"({top['current_qty']} EUR, {round(top['age_s']/60,1)} min old, {top['fill_pct']}% filled)."
        )
    if magnet:
        lines.append(
            f"Session magnet (Market Profile POC) sits at {magnet['price']}, "
            f"where price has spent {magnet['pct']}% of today's session."
        )
    atr = stops.get("atr_pips", 0)
    sell_s = stops.get("sell_stops", [])
    buy_s = stops.get("buy_stops", [])
    if atr:
        lines.append(f"ATR = {atr:.1f} pips.")
    if sell_s:
        z = ", ".join(str(z["price"]) for z in sell_s[:3])
        lines.append(f"Sell-side stop clusters above swing highs: {z}.")
    if buy_s:
        z = ", ".join(str(z["price"]) for z in buy_s[:3])
        lines.append(f"Buy-side stop clusters below swing lows: {z}.")
    return " ".join(lines)


# ============================================================
#  SHARED LIVE-DASHBOARD STATE
# ============================================================
state = {
    "events": collections.deque(maxlen=MAX_EVENTS),
    "bids_display": [], "asks_display": [],
    "resting": [], "resting_map": {},
    "connected": False, "ts": 0,
    "bid_levels": 0, "ask_levels": 0,
    "buy_vol": 0.0, "sell_vol": 0.0,
    "tick": 0, "mid": None,
    "session_open": None, "session_high": None, "session_low": None,
    "vwap": None, "magnet": None, "profile_top": [],
}
lock = threading.Lock()

price_history = collections.deque(maxlen=PRICE_HIST_LEN)
kf = KalmanFilter()

pred_cache = {"r": None, "ts": 0}
clus_cache = {"r": None, "ts": 0}
abs_cache  = {"r": None, "ts": 0}
stop_cache = {"r": None, "ts": 0}
rep_cache  = {"r": None, "ts": 0}
vp_cache   = {}
tpo_cache  = {}
fp_cache   = {}

full_bids = {}; full_asks = {}; book_lock = threading.Lock()
last_update_id = 0
sync_state = "buffering"; event_buffer = []; buf_lock = threading.Lock()
last_scan = 0.0

resting_trackers = {}
time_profile = {}
last_profile_ts = None
last_magnet_price = None
session_date = None
session_open = None
session_high = None
session_low = None
vwap_num = 0.0; vwap_den = 0.0

heat_lock = threading.Lock()
heat_frames = collections.deque(maxlen=HEAT_MAX_FRAMES)
heat_counter = 0
last_heat_ts = 0.0
notable_trades = collections.deque(maxlen=MAX_BUBBLES)
trade_ema = None


# ============================================================
#  SESSION MANAGEMENT
# ============================================================
def maybe_reset_session():
    global session_date, session_open, session_high, session_low, vwap_num, vwap_den
    today = datetime.datetime.utcnow().date().isoformat()
    if session_date != today:
        if session_date is not None:
            try:
                prev_start = session_start_ms_for_date(session_date)
                prev_end = session_start_ms()
                c = db_conn()
                finalize_session_poc(c, prev_start, prev_end, session_date)
                c.close()
            except Exception as e:
                print(f"[POC] finalize error: {e}")
        session_date = today
        session_open = session_high = session_low = None
        vwap_num = vwap_den = 0.0
        time_profile.clear()
        resting_trackers.clear()
        print(f"[SESSION] New UTC day {today}")


def update_session(mid):
    global last_profile_ts, session_open, session_high, session_low
    now = time.time()
    if session_open is None:
        session_open = session_high = session_low = mid
    else:
        session_high = max(session_high, mid)
        session_low = min(session_low, mid)
    if last_profile_ts is not None:
        dt = now - last_profile_ts
        bk = f"{round(mid / PROFILE_BUCKET) * PROFILE_BUCKET:.4f}"
        time_profile[bk] = time_profile.get(bk, 0.0) + dt
    last_profile_ts = now


def get_magnet():
    if not time_profile:
        return None
    bk, secs = max(time_profile.items(), key=lambda kv: kv[1])
    tot = sum(time_profile.values()) or 1.0
    return {"price": bk, "seconds": round(secs), "pct": round(secs / tot * 100, 1)}


def get_top_profile(n=5):
    tot = sum(time_profile.values()) or 1.0
    items = sorted(time_profile.items(), key=lambda kv: kv[1], reverse=True)[:n]
    return [{"price": p, "pct": round(s / tot * 100, 1)} for p, s in items]


def get_magnet_log():
    global last_magnet_price
    m = get_magnet()
    if m and m["price"] != last_magnet_price:
        if last_magnet_price is not None:
            log_ev("MAGNET_SHIFT", m["price"], None,
                   f"Session magnet shifted {last_magnet_price} -> {m['price']} ({m['pct']}%)")
        last_magnet_price = m["price"]
    return m


def log_ev(etype, price, side, detail):
    with lock:
        state["events"].appendleft({"type": etype, "price": price, "side": side,
                                     "detail": detail, "ts": time.time()})


# ============================================================
#  RESTING ORDER TRACKER
# ============================================================
def scan_structural(bids_s, asks_s):
    now = time.time()
    bid_c = {c["price"]: c for c in find_sig_levels(bids_s, "BID")}
    ask_c = {c["price"]: c for c in find_sig_levels(asks_s, "ASK")}
    cands = {**bid_c, **ask_c}
    bid_bk = dict(bids_s); ask_bk = dict(asks_s)
    for price, c in cands.items():
        t = resting_trackers.get(price)
        if t is None:
            resting_trackers[price] = {"side": c["side"], "first_seen": now,
                                        "peak_qty": c["qty"], "current_qty": c["qty"]}
            log_ev("NEW_RESTING", price, c["side"], f"New resting order: {c['qty']:.2f} EUR")
        else:
            t["current_qty"] = c["qty"]; t["peak_qty"] = max(t["peak_qty"], c["qty"])
            t.pop("_gone_at", None)
    for price, t in list(resting_trackers.items()):
        if price in cands:
            continue
        bk = bid_bk if t["side"] == "BID" else ask_bk
        cur = bk.get(price)
        if cur is None:
            if "_gone_at" not in t:
                t["_gone_at"] = now
                log_ev("CLEARED", price, t["side"], f"Level cleared (peak {t['peak_qty']:.2f} EUR)")
            t["current_qty"] = 0.0
        else:
            old = t["current_qty"]; t["current_qty"] = cur; t.pop("_gone_at", None)
            if old > 0 and cur < old * 0.5:
                log_ev("PARTIAL_FILL", price, t["side"], f"Size {old:.2f}->{cur:.2f} EUR")
    stale = [p for p, t in resting_trackers.items()
             if t.get("_gone_at") and now - t["_gone_at"] > RESTING_GRACE_S]
    for p in stale:
        resting_trackers.pop(p, None)


def build_resting():
    now = time.time()
    out = []
    for price, t in resting_trackers.items():
        age = now - t["first_seen"]
        peak = t["peak_qty"] or 1e-9
        cur = t["current_qty"]
        fp = max(0.0, min(100.0, (peak - cur) / peak * 100))
        gone = "_gone_at" in t
        status = "GONE" if gone else ("RESTING" if fp < 15 else ("PARTIAL" if fp < 60 else "MOSTLY_FILLED"))
        pf = float(price)
        base = round(pf / ROUND_STEP) * ROUND_STEP
        nr = min([round(base + i * ROUND_STEP, 4) for i in (-1, 0, 1)], key=lambda c: abs(pf - c))
        out.append({"price": price, "side": t["side"], "age_s": round(age),
                    "peak_qty": round(peak, 4), "current_qty": round(cur, 4),
                    "fill_pct": round(fp, 1), "status": status,
                    "structural": age >= MIN_STRUCTURAL_AGE_S and not gone,
                    "near_round": abs(pf - nr) <= ROUND_TOL, "nearest_round": round(nr, 4)})
    RANK = {"RESTING": 0, "PARTIAL": 1, "MOSTLY_FILLED": 2, "GONE": 3}
    out.sort(key=lambda r: (RANK.get(r["status"], 9), -r["peak_qty"]))
    return out[:TOP_RESTING_SHOWN]


# ============================================================
#  HEATMAP SAMPLER
# ============================================================
def sample_heat(bids_s, asks_s, mid):
    global last_heat_ts, heat_counter
    now = time.time()
    if now - last_heat_ts < HEAT_INTERVAL_S:
        return
    last_heat_ts = now
    heat_counter += 1
    frame = {"idx": heat_counter, "ts": now, "mid": mid,
             "bids": {p: round(q, 4) for p, q in bids_s[:HEAT_HALF_WIDTH]},
             "asks": {p: round(q, 4) for p, q in asks_s[:HEAT_HALF_WIDTH]}}
    with heat_lock:
        heat_frames.append(frame)


# ============================================================
#  LIVE ORDER BOOK SYNC (Binance official local-book recipe)
# ============================================================
def fetch_snapshot():
    global last_update_id
    r = requests.get(REST_URL, timeout=10)
    r.raise_for_status()
    d = r.json()
    with book_lock:
        full_bids.clear(); full_asks.clear()
        for p, q in d["bids"]:
            qf = float(q)
            if qf > 0: full_bids[p] = qf
        for p, q in d["asks"]:
            qf = float(q)
            if qf > 0: full_asks[p] = qf
        last_update_id = d["lastUpdateId"]
    print(f"[BOOK] Snapshot loaded uid={last_update_id} bids={len(full_bids)} asks={len(full_asks)}")


def apply_ev(ev):
    global last_update_id
    with book_lock:
        for p, q in ev.get("b", []):
            qf = float(q)
            if qf == 0: full_bids.pop(p, None)
            else: full_bids[p] = qf
        for p, q in ev.get("a", []):
            qf = float(q)
            if qf == 0: full_asks.pop(p, None)
            else: full_asks[p] = qf
        last_update_id = ev["u"]


def sync_book():
    global sync_state
    time.sleep(2.0)
    try:
        fetch_snapshot()
    except Exception as e:
        print(f"[BOOK] Snapshot error: {e} - retry in 5s")
        time.sleep(5)
        threading.Thread(target=sync_book, daemon=True).start()
        return
    with buf_lock:
        buffered = list(event_buffer); event_buffer.clear()
    buffered = [e for e in buffered if e["u"] > last_update_id]
    start = next((i for i, e in enumerate(buffered) if e["U"] <= last_update_id + 1 <= e["u"]), None)
    applied = 0
    if start is not None:
        for e in buffered[start:]:
            apply_ev(e); applied += 1
    sync_state = "synced"
    print(f"[BOOK] Synced uid={last_update_id} replayed={applied}")


def trigger_resync():
    global sync_state
    if sync_state == "buffering": return
    sync_state = "buffering"
    with buf_lock: event_buffer.clear()
    threading.Thread(target=sync_book, daemon=True).start()


# ============================================================
#  DETECTION CYCLE
# ============================================================
def run_cycle():
    global last_scan
    maybe_reset_session()
    with book_lock:
        bids_s = sorted(full_bids.items(), key=lambda kv: float(kv[0]), reverse=True)
        asks_s = sorted(full_asks.items(), key=lambda kv: float(kv[0]))
    if not bids_s or not asks_s: return
    mid = (float(bids_s[0][0]) + float(asks_s[0][0])) / 2
    kf.update(mid)
    price_history.append(mid)
    update_session(mid)
    sample_heat(bids_s, asks_s, mid)
    now = time.time()
    if now - last_scan >= SCAN_INTERVAL_S:
        last_scan = now
        scan_structural(bids_s, asks_s)
    rs = build_resting()
    mag = get_magnet_log()
    if mag:
        mp = float(mag["price"])
        mag["confluence"] = next((r for r in rs if abs(float(r["price"]) - mp) <= PROFILE_BUCKET * 2), None)
    rm = {r["price"]: r for r in rs}
    with lock:
        state["tick"] += 1
        state["bid_levels"] = len(bids_s); state["ask_levels"] = len(asks_s)
        state["ts"] = last_update_id
        state["bids_display"] = bids_s[:DISPLAY_DEPTH]; state["asks_display"] = asks_s[:DISPLAY_DEPTH]
        state["mid"] = mid
        state["session_open"] = session_open; state["session_high"] = session_high
        state["session_low"] = session_low
        state["vwap"] = (vwap_num / vwap_den) if vwap_den > 0 else None
        state["magnet"] = mag
        state["profile_top"] = get_top_profile()
        state["resting"] = rs; state["resting_map"] = rm


# ============================================================
#  WEBSOCKET HANDLERS
# ============================================================
def on_message(ws_app, raw):
    global vwap_num, vwap_den, trade_ema
    msg = json.loads(raw)
    data = msg.get("data", {})
    etype = data.get("e")
    if etype == "depthUpdate":
        if sync_state == "buffering":
            with buf_lock: event_buffer.append(data)
            return
        if data["U"] > last_update_id + 1:
            trigger_resync(); return
        apply_ev(data)
        with lock: state["connected"] = True
        run_cycle()
    elif etype == "aggTrade":
        pf = float(data.get("p", "0"))
        qty = float(data.get("q", 0))
        vwap_num += pf * qty; vwap_den += qty
        is_sell = bool(data.get("m", False))
        side = "SELL" if is_sell else "BUY"
        trade_ema_local = (BUBBLE_ALPHA * qty + (1 - BUBBLE_ALPHA) * trade_ema
                            if trade_ema is not None else qty)
        globals()["trade_ema"] = trade_ema_local
        if qty > max(trade_ema_local * BUBBLE_MULT, BUBBLE_MIN):
            with heat_lock:
                notable_trades.append({"ts": time.time(), "price": pf, "qty": round(qty, 4), "side": side})
        with lock:
            if side == "BUY": state["buy_vol"] += qty
            else: state["sell_vol"] += qty
        agg_id = data.get("a")
        if agg_id is not None:
            buffer_live_tick(agg_id, pf, qty, int(data.get("T", time.time() * 1000)), is_sell)


def on_open(ws_app):
    global sync_state
    print("[WS] Connected")
    with lock: state["connected"] = True
    sync_state = "buffering"
    with buf_lock: event_buffer.clear()
    threading.Thread(target=sync_book, daemon=True).start()


def on_error(ws_app, err):
    print(f"[WS] Error: {err}")
    with lock: state["connected"] = False


def on_close(ws_app, code, msg):
    print("[WS] Closed - reconnecting in 5s")
    with lock: state["connected"] = False
    time.sleep(5); run_ws()


def run_ws():
    ws_app = websocket.WebSocketApp(WS_URL, on_open=on_open, on_message=on_message,
                                     on_error=on_error, on_close=on_close)
    ws_app.run_forever(ping_interval=20, ping_timeout=10)


# ============================================================
#  FLASK ROUTES
# ============================================================
app = Flask(__name__)


def _cached(cache, fn, *args):
    now = time.time()
    if cache.get("r") is None or now - cache.get("ts", 0) > CACHE_TTL:
        cache["r"] = fn(*args)
        cache["ts"] = now
    return cache["r"]


def _cached_keyed(cache_dict, key, fn, *args, ttl=10):
    now = time.time()
    entry = cache_dict.get(key)
    if entry is None or now - entry["ts"] > ttl:
        entry = {"r": fn(*args), "ts": now}
        cache_dict[key] = entry
    return entry["r"]


@app.route("/")
def index():
    return HTML, 200, {"Content-Type": "text/html; charset=utf-8"}


@app.route("/api/data")
def api_data():
    with lock:
        bds = list(state["bids_display"]); ads = list(state["asks_display"])
        rm = dict(state["resting_map"]); rs = list(state["resting"])
        evs = list(state["events"])[:60]
        conn_ = state["connected"]
        bv = state["buy_vol"]; sv = state["sell_vol"]
        bl = state["bid_levels"]; al = state["ask_levels"]
        so = state["session_open"]; sh = state["session_high"]; slo = state["session_low"]
        vw = state["vwap"]; mag = state["magnet"]; pt = list(state["profile_top"]); ts = state["ts"]
    book_ready = (sync_state == "synced")
    mpf = float(mag["price"]) if mag else None

    def enrich(rows):
        out, cum = [], 0.0
        for p, q in rows:
            cum += q
            pf = float(p)
            magn = mpf is not None and abs(pf - mpf) < PROFILE_BUCKET * 0.5
            out.append({"price": p, "qty": f"{q:.4f}", "cumul": f"{cum:.4f}",
                        "resting": rm.get(p), "is_magnet": magn})
        return out

    return jsonify({
        "bids": enrich(bds), "asks": enrich(ads), "resting": rs, "events": evs,
        "connected": conn_, "book_ready": book_ready, "bid_levels": bl, "ask_levels": al,
        "buy_vol": round(bv, 2), "sell_vol": round(sv, 2), "ts": ts,
        "session_open": so, "session_high": sh, "session_low": slo, "vwap": vw,
        "magnet": mag, "profile_top": pt,
        "min_structural_age_min": round(MIN_STRUCTURAL_AGE_S / 60, 1),
    })


@app.route("/api/heatmap")
def api_heatmap():
    since = request.args.get("since", default=0, type=int)
    with heat_lock:
        frames = [f for f in heat_frames if f["idx"] > since]
        li = heat_frames[-1]["idx"] if heat_frames else since
        trd = [{"ts": t["ts"], "price": t["price"], "qty": t["qty"], "side": t["side"]}
               for t in notable_trades]
    with lock:
        bands = [{"price": r["price"], "side": r["side"], "status": r["status"]}
                 for r in state["resting"] if r["status"] in ("RESTING", "PARTIAL")]
    return jsonify({"frames": frames, "latest_index": li, "bands": bands,
                    "trades": trd, "sample_interval_s": HEAT_INTERVAL_S})


@app.route("/api/clusters")
def api_clusters():
    with heat_lock:
        fl = list(heat_frames)
    return jsonify(_cached(clus_cache, compute_clusters, fl) or [])


@app.route("/api/absorption")
def api_absorption():
    with heat_lock:
        fl = list(heat_frames); trd = list(notable_trades)
    return jsonify(_cached(abs_cache, compute_absorption, fl, trd) or [])


@app.route("/api/stopzones")
def api_stopzones():
    return jsonify(_cached(stop_cache, compute_stop_zones, list(price_history)) or {})


@app.route("/api/prediction")
def api_prediction():
    return jsonify(_cached(pred_cache, compute_prediction, list(price_history)) or {})


@app.route("/api/backfill_status")
def api_backfill_status():
    with backfill_lock:
        return jsonify(dict(backfill_progress))


@app.route("/api/volume_profile")
def api_volume_profile():
    mode = request.args.get("mode", "session")
    tf = request.args.get("tf", default=900, type=int)
    lookback = request.args.get("lookback", default=100, type=int)
    tick_size = request.args.get("tick_size", default=DEFAULT_TICK_SIZE, type=float)
    va_pct = request.args.get("va_pct", default=70, type=int) / 100.0
    key = f"{mode}:{tf}:{lookback}:{tick_size}:{va_pct}"
    conn = db_conn()
    try:
        result = _cached_keyed(vp_cache, key, compute_volume_profile,
                                conn, mode, tf, lookback, tick_size, va_pct, ttl=8)
        naked = check_naked_pocs(conn)
        return jsonify({"profile": result, "naked_pocs": naked})
    finally:
        conn.close()


@app.route("/api/tpo")
def api_tpo():
    period_min = request.args.get("period", default=30, type=int)
    lookback_periods = request.args.get("lookback", default=16, type=int)
    tick_size = request.args.get("tick_size", default=DEFAULT_TICK_SIZE, type=float)
    va_pct = request.args.get("va_pct", default=70, type=int) / 100.0
    key = f"{period_min}:{lookback_periods}:{tick_size}:{va_pct}"
    conn = db_conn()
    try:
        result = _cached_keyed(tpo_cache, key, compute_tpo,
                                conn, period_min, lookback_periods, tick_size, va_pct, ttl=8)
        return jsonify(result or {})
    finally:
        conn.close()


@app.route("/api/footprint")
def api_footprint():
    tf = request.args.get("tf", default=60, type=int)
    n_bars = request.args.get("bars", default=40, type=int)
    tick_size = request.args.get("tick_size", default=DEFAULT_TICK_SIZE, type=float)
    imb_ratio = request.args.get("imb_ratio", default=3.0, type=float)
    key = f"{tf}:{n_bars}:{tick_size}:{imb_ratio}"
    conn = db_conn()
    try:
        result = _cached_keyed(fp_cache, key, compute_footprint,
                                conn, tf, n_bars, tick_size, imb_ratio, ttl=5)
        return jsonify(result or [])
    finally:
        conn.close()


@app.route("/api/report")
def api_report():
    now = time.time()
    if rep_cache.get("r") and now - rep_cache.get("ts", 0) < CACHE_TTL:
        return jsonify(rep_cache["r"])
    with lock:
        mid_v = state.get("mid"); rs_v = list(state.get("resting", []))
        so_v = state.get("session_open"); sh_v = state.get("session_high")
        sl_v = state.get("session_low"); vw_v = state.get("vwap")
        mag_v = state.get("magnet"); bl_v = state.get("bid_levels"); al_v = state.get("ask_levels")
    pred_v = pred_cache.get("r") or {}
    stop_v = stop_cache.get("r") or {}
    clus_v = clus_cache.get("r") or []
    vp_v = None
    for v in vp_cache.values():
        if v["r"] and v["r"].get("mode") == "session":
            vp_v = v["r"]
            break
    rep = {
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z", "symbol": "EURUSDT", "mid": mid_v,
        "session": {"open": so_v, "high": sh_v, "low": sl_v, "vwap": vw_v,
                    "book_depth": f"{bl_v} bid / {al_v} ask"},
        "regime": {"hurst": pred_v.get("hurst"), "type": pred_v.get("regime"), "bias": pred_v.get("bias"),
                   "confidence": pred_v.get("confidence"), "entropy": pred_v.get("entropy")},
        "prediction": pred_v, "structural_levels": [r for r in rs_v if r.get("structural")],
        "stop_zones": stop_v, "top_clusters": clus_v[:10], "magnet": mag_v,
        "volume_profile": vp_v,
        "analysis": _gen_analysis(mid_v, pred_v, rs_v, stop_v, mag_v, vp_v),
    }
    rep_cache["r"] = rep; rep_cache["ts"] = now
    return jsonify(rep)


# ============================================================
#  CANDLES - SQL-aggregated OHLCV for the candlestick chart
#  (4+ weeks navigable via pan/zoom, windowed fetch by time range)
# ============================================================
def get_candles_sql(conn, tf_seconds, start_ms, end_ms, max_bars=1500):
    tf_ms = tf_seconds * 1000
    rows = conn.execute("""
        WITH ordered AS (
            SELECT ts, price, qty, is_sell, (ts / ?) * ? AS bucket
            FROM ticks
            WHERE ts >= ? AND ts < ?
            ORDER BY ts
        ),
        marked AS (
            SELECT bucket, ts, price, qty, is_sell,
                   FIRST_VALUE(price) OVER (PARTITION BY bucket ORDER BY ts
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS open_p,
                   LAST_VALUE(price) OVER (PARTITION BY bucket ORDER BY ts
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close_p
            FROM ordered
        ),
        grouped AS (
            SELECT bucket,
                   MAX(open_p)  AS o,
                   MAX(price)   AS h,
                   MIN(price)   AS l,
                   MAX(close_p) AS c,
                   SUM(qty)     AS vol,
                   SUM(CASE WHEN is_sell = 0 THEN qty ELSE 0 END) AS buy_vol,
                   SUM(CASE WHEN is_sell = 1 THEN qty ELSE 0 END) AS sell_vol,
                   COUNT(*)     AS n_trades
            FROM marked
            GROUP BY bucket
        )
        SELECT * FROM (
            SELECT * FROM grouped ORDER BY bucket DESC LIMIT ?
        ) ORDER BY bucket ASC
    """, (tf_ms, tf_ms, start_ms, end_ms, max_bars)).fetchall()
    return [{"ts": r[0], "o": round(r[1], 5), "h": round(r[2], 5), "l": round(r[3], 5),
             "c": round(r[4], 5), "vol": round(r[5], 4), "buy_vol": round(r[6], 4),
             "sell_vol": round(r[7], 4), "n_trades": r[8]} for r in rows]


def get_data_span(conn):
    row = conn.execute("SELECT MIN(ts), MAX(ts), COUNT(*) FROM ticks").fetchone()
    return {"min_ts": row[0], "max_ts": row[1], "count": row[2]}


# ============================================================
#  MULTI-SESSION COMPOSITE PROFILE (TPO staircase view)
#  One volume-profile shape per session, POC-line across sessions
# ============================================================
def compute_session_profiles(conn, n_sessions, tick_size, va_pct):
    n_sessions = max(1, min(n_sessions, 40))
    today_start = session_start_ms()
    out = []
    for i in range(n_sessions, 0, -1):
        day_start = today_start - i * 86400000
        day_end = day_start + 86400000
        rows = conn.execute(
            "SELECT price, qty, is_sell FROM ticks WHERE ts >= ? AND ts < ?",
            (day_start, day_end)).fetchall()
        if not rows:
            continue
        bucket_map = {}
        for price, qty, is_sell in rows:
            pb = round(price / tick_size) * tick_size
            pbk = f"{pb:.5f}"
            e = bucket_map.get(pbk)
            if e is None:
                e = {"buy": 0.0, "sell": 0.0}
                bucket_map[pbk] = e
            if is_sell: e["sell"] += qty
            else: e["buy"] += qty
        if not bucket_map:
            continue
        levels = [{"price": p, "buy": round(v["buy"], 4), "sell": round(v["sell"], 4),
                   "total": round(v["buy"] + v["sell"], 4)} for p, v in bucket_map.items()]
        levels.sort(key=lambda x: float(x["price"]))
        poc_idx = max(range(len(levels)), key=lambda i: levels[i]["total"])
        total_vol = sum(l["total"] for l in levels)
        target = total_vol * va_pct
        lo = hi = poc_idx
        acc = levels[poc_idx]["total"]
        while acc < target and (lo > 0 or hi < len(levels) - 1):
            lower_val = levels[lo - 1]["total"] if lo > 0 else -1
            upper_val = levels[hi + 1]["total"] if hi < len(levels) - 1 else -1
            if upper_val >= lower_val:
                hi += 1; acc += levels[hi]["total"]
            else:
                lo -= 1; acc += levels[lo]["total"]
        date_str = datetime.datetime.utcfromtimestamp(day_start / 1000).date().isoformat()
        out.append({
            "date": date_str, "day_start": day_start, "day_end": day_end,
            "levels": levels, "poc": levels[poc_idx]["price"],
            "vah": levels[hi]["price"], "val": levels[lo]["price"],
            "total_volume": round(total_vol, 4),
            "open": round(rows[0][0], 5), "close": round(rows[-1][0], 5),
            "high": round(max(r[0] for r in rows), 5), "low": round(min(r[0] for r in rows), 5),
        })
    return out


# ============================================================
#  FLASK ROUTES - new endpoints (candles, session profiles,
#  windowed footprint)
# ============================================================
@app.route("/api/candles")
def api_candles():
    tf = request.args.get("tf", default=900, type=int)
    end_ms = request.args.get("end", default=None, type=int)
    start_ms = request.args.get("start", default=None, type=int)
    max_bars = min(request.args.get("max_bars", default=1000, type=int), 1500)
    conn = db_conn()
    try:
        if end_ms is None:
            end_ms = int(time.time() * 1000)
        if start_ms is None:
            start_ms = end_ms - max_bars * tf * 1000
        candles = get_candles_sql(conn, tf, start_ms, end_ms, max_bars)
        span = get_data_span(conn)
        return jsonify({"candles": candles, "tf": tf, "data_span": span})
    finally:
        conn.close()


@app.route("/api/session_profiles")
def api_session_profiles():
    n_sessions = request.args.get("n", default=10, type=int)
    tick_size = request.args.get("tick_size", default=DEFAULT_TICK_SIZE, type=float)
    va_pct = request.args.get("va_pct", default=70, type=int) / 100.0
    conn = db_conn()
    try:
        result = compute_session_profiles(conn, n_sessions, tick_size, va_pct)
        return jsonify(result)
    finally:
        conn.close()


@app.route("/api/footprint_window")
def api_footprint_window():
    tf = request.args.get("tf", default=60, type=int)
    end_ms = request.args.get("end", default=None, type=int)
    start_ms = request.args.get("start", default=None, type=int)
    n_bars = min(request.args.get("bars", default=40, type=int), 100)
    tick_size = request.args.get("tick_size", default=DEFAULT_TICK_SIZE, type=float)
    imb_ratio = request.args.get("imb_ratio", default=3.0, type=float)
    conn = db_conn()
    try:
        if end_ms is None:
            end_ms = int(time.time() * 1000)
        if start_ms is None:
            start_ms = end_ms - n_bars * tf * 1000
        rows = conn.execute(
            "SELECT price, qty, ts, is_sell FROM ticks WHERE ts >= ? AND ts < ? ORDER BY ts LIMIT 200000",
            (start_ms, end_ms)).fetchall()
        tf_ms = tf * 1000
        bars = {}
        for price, qty, ts, is_sell in rows:
            bucket = (ts // tf_ms) * tf_ms
            b = bars.get(bucket)
            if b is None:
                b = {"o": price, "h": price, "l": price, "c": price,
                     "buy_vol": 0.0, "sell_vol": 0.0, "cells": {}}
                bars[bucket] = b
            if price > b["h"]: b["h"] = price
            if price < b["l"]: b["l"] = price
            b["c"] = price
            pb = round(price / tick_size) * tick_size
            pbk = f"{pb:.5f}"
            cell = b["cells"].get(pbk)
            if cell is None:
                cell = {"buy": 0.0, "sell": 0.0}
                b["cells"][pbk] = cell
            if is_sell:
                b["sell_vol"] += qty; cell["sell"] += qty
            else:
                b["buy_vol"] += qty; cell["buy"] += qty
        ordered = sorted(bars.items())

        out = []
        for bucket_ms, b in ordered:
            cells = b["cells"]
            rows_sorted = sorted(cells.items(), key=lambda x: float(x[0]))
            imbalance_flags = {}
            for p, c in rows_sorted:
                lower_key = f"{round(float(p) - tick_size, 5):.5f}"
                lower_sell = cells.get(lower_key, {}).get("sell", 0.0)
                if lower_sell > 0 and c["buy"] >= lower_sell * imb_ratio:
                    imbalance_flags[p] = "BUY_IMBALANCE"
                higher_key = f"{round(float(p) + tick_size, 5):.5f}"
                higher_buy = cells.get(higher_key, {}).get("buy", 0.0)
                if higher_buy > 0 and c["sell"] >= higher_buy * imb_ratio:
                    existing = imbalance_flags.get(p, "")
                    imbalance_flags[p] = (existing + "|" if existing else "") + "SELL_IMBALANCE"
            stacked = detect_stacked(rows_sorted, imbalance_flags)
            bar_poc = max(rows_sorted, key=lambda x: x[1]["buy"] + x[1]["sell"])[0] if rows_sorted else None
            total_vol = b["buy_vol"] + b["sell_vol"]
            delta = b["buy_vol"] - b["sell_vol"]
            out.append({
                "ts": bucket_ms, "o": round(b["o"], 5), "h": round(b["h"], 5),
                "l": round(b["l"], 5), "c": round(b["c"], 5),
                "buy_vol": round(b["buy_vol"], 4), "sell_vol": round(b["sell_vol"], 4),
                "total_vol": round(total_vol, 4), "delta": round(delta, 4), "poc": bar_poc,
                "cells": [{"price": p, "buy": round(c["buy"], 4), "sell": round(c["sell"], 4),
                           "imbalance": imbalance_flags.get(p, ""), "stacked": p in stacked}
                          for p, c in rows_sorted],
            })
        span = get_data_span(conn)
        return jsonify({"bars": out, "tf": tf, "start": start_ms, "end": end_ms, "data_span": span})
    finally:
        conn.close()


# ============================================================
#  HTML / CSS / JS FRONTEND
# ============================================================
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>EURUSDT - Pro Quant Suite</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#07090e;--s1:#0b0e16;--s2:#0f1420;--bdr:#171f30;--bdr2:#1e2840;
  --bid:#00c87a;--ask:#ff3d5a;--mag:#ffd166;--wall:#64b4ff;--susp:#d4881a;--void:#b46aff;
  --text:#c0d0e0;--muted:#3a4a5e;--bright:#e8f4ff;--mono:'Courier New',monospace;
}
html,body{height:100%;background:var(--bg);color:var(--text);font-family:var(--mono);font-size:11px;overflow:hidden}
select,button.tool{background:var(--s2);color:var(--text);border:1px solid var(--bdr2);border-radius:3px;font-family:var(--mono);font-size:8px;padding:3px 6px;cursor:pointer}
button.tool.active{background:var(--mag);color:#1a1400;border-color:var(--mag)}
#hdr{height:50px;background:var(--s1);border-bottom:1px solid var(--bdr);display:flex;align-items:center;gap:6px;padding:0 8px;flex-shrink:0;overflow-x:auto}
.pair{font-size:13px;font-weight:700;color:var(--bright);letter-spacing:1.2px;flex-shrink:0}
.hdot{width:8px;height:8px;border-radius:50%;background:#2a3a4a;flex-shrink:0}
.hdot.live{background:var(--bid);animation:blink 1.1s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.15}}
.pill{display:flex;flex-direction:column;background:var(--s2);border:1px solid var(--bdr);border-radius:4px;padding:3px 6px;flex-shrink:0;min-width:55px}
.pill .l{font-size:6px;color:var(--muted);letter-spacing:.5px;margin-bottom:1px}
.pill .v{font-size:10px;font-weight:700}
.flow-pill{flex:1;max-width:80px}
.flow-bar{height:5px;border-radius:3px;background:var(--bdr);overflow:hidden;margin-top:4px}
.flow-fill{height:100%;background:var(--bid);border-radius:3px;transition:width .6s}
#tabbar{height:28px;display:flex;align-items:stretch;background:var(--s1);border-bottom:1px solid var(--bdr);flex-shrink:0;overflow-x:auto}
.tb{background:transparent;border:none;border-bottom:2px solid transparent;color:var(--muted);font-family:var(--mono);font-size:8px;letter-spacing:.4px;font-weight:700;padding:0 9px;cursor:pointer;white-space:nowrap}
.tb.active{color:var(--bright);border-bottom-color:var(--mag)}
.ptitle{height:24px;display:flex;align-items:center;justify-content:space-between;padding:0 8px;background:var(--s1);border-bottom:1px solid var(--bdr);font-size:7px;letter-spacing:.7px;color:var(--muted);font-weight:700;flex-shrink:0}
.toolbar{display:flex;gap:4px;padding:4px 8px;background:var(--s1);border-bottom:1px solid var(--bdr);flex-wrap:wrap;flex-shrink:0;align-items:center}
.pbody{flex:1;overflow-y:auto}
.pbody::-webkit-scrollbar{width:3px}
.pbody::-webkit-scrollbar-thumb{background:var(--bdr2)}
#grid{display:grid;grid-template-columns:1fr 1fr;height:calc(100vh - 78px)}
.panel{display:flex;flex-direction:column;border-right:1px solid var(--bdr);overflow:hidden}
.panel:last-child{border-right:none}
.rpanel{display:flex;flex-direction:column;border-right:none}
.rtop{flex:0 0 34%;display:flex;flex-direction:column;border-bottom:2px solid var(--bdr2)}
.rmid{flex:0 0 32%;display:flex;flex-direction:column;border-bottom:2px solid var(--bdr2)}
.rbot{flex:1;display:flex;flex-direction:column}
.rtop .pbody,.rmid .pbody,.rbot .pbody{flex:1;overflow-y:auto}
.col-hdr{display:grid;grid-template-columns:1fr 1fr 1fr 22px;padding:4px 10px;font-size:7px;color:var(--muted);border-bottom:1px solid var(--bdr);background:var(--s1);position:sticky;top:0;z-index:5}
.col-hdr span:nth-child(2){text-align:center}
.col-hdr span:nth-child(3){text-align:right}
.brow{display:grid;grid-template-columns:1fr 1fr 1fr 22px;padding:3px 10px;position:relative;align-items:center;border-bottom:1px solid #0b0f17}
.brow:hover{background:rgba(255,255,255,.025)}
.dbar{position:absolute;top:0;bottom:0;opacity:.1;pointer-events:none}
.ask-row .dbar{right:0;background:var(--ask)}.bid-row .dbar{left:0;background:var(--bid)}
.pr{font-weight:700;font-size:11px;position:relative;z-index:1}
.ask-row .pr{color:var(--ask)}.bid-row .pr{color:var(--bid)}
.qy{text-align:center;position:relative;z-index:1}
.cu{text-align:right;font-size:9px;color:var(--muted);position:relative;z-index:1}
.fl{text-align:center;font-size:12px;position:relative;z-index:1;line-height:1}
.rest-r{background:rgba(100,180,255,.10);border-left:2px solid rgba(100,180,255,.5)}
.rest-p{background:rgba(240,160,48,.08);border-left:2px solid rgba(240,160,48,.4)}
.mag-row{background:rgba(255,209,102,.14);border-left:2px solid rgba(255,209,102,.8)}
.spdiv{display:flex;justify-content:space-around;align-items:center;padding:5px 10px;background:#090c13;border-top:1px solid var(--bdr);border-bottom:1px solid var(--bdr);font-size:8px;color:var(--muted);flex-shrink:0}
.spdiv span{display:flex;flex-direction:column;align-items:center;gap:2px}
.spdiv b{font-size:12px;color:var(--bright);font-weight:700}
.struct-block{padding:8px 10px;display:flex;flex-direction:column;gap:5px}
.struct-row{display:flex;justify-content:space-between;font-size:9.5px;gap:8px}
.sk{color:var(--muted);white-space:nowrap}.sv{color:var(--bright);font-weight:700;text-align:right}
.pbar-row{display:flex;align-items:center;gap:6px;font-size:8px;color:var(--muted);padding:2px 10px}
.pbar-track{flex:1;height:6px;background:var(--bdr);border-radius:3px;overflow:hidden}
.pbar-fill{height:100%;background:var(--mag);border-radius:3px}
.pbar-price{width:62px;text-align:right;color:var(--text)}
.pbar-pct{width:32px;text-align:right}
.icard{padding:6px 10px;border-bottom:1px solid var(--bdr);display:grid;grid-template-columns:1fr auto;gap:6px;align-items:start}
.icard:hover{background:rgba(255,255,255,.02)}
.iprice{font-size:12px;font-weight:700}
.bid-ice .iprice{color:var(--bid)}.ask-ice .iprice{color:var(--ask)}
.imeta{font-size:8px;color:var(--muted);margin-top:2px;line-height:1.5}
.ibadge{font-size:7px;padding:2px 5px;border-radius:2px;font-weight:700;margin-left:6px;background:rgba(255,255,255,.06);color:var(--muted);border:1px solid var(--bdr2)}
.iscore{font-size:13px;font-weight:700;padding:2px 4px}
.erow{padding:4px 10px;border-bottom:1px solid #0c1018;display:grid;grid-template-columns:auto 1fr auto;gap:7px;align-items:start}
.etag{font-size:7px;font-weight:700;padding:2px 5px;border-radius:2px;letter-spacing:.4px;white-space:nowrap;margin-top:1px}
.t-NEW{background:rgba(100,180,255,.18);color:#64b4ff}
.t-PART{background:rgba(255,61,90,.2);color:#ff3d5a}
.t-CLR{background:rgba(240,160,48,.2);color:#f0a030}
.t-MAG{background:rgba(255,209,102,.2);color:#ffd166}
.epr{font-weight:700;font-size:10px}
.edet{font-size:9px;color:var(--muted);margin-top:1px}
.ets{font-size:8px;color:var(--muted);white-space:nowrap;margin-top:1px}
.empty{padding:18px 12px;color:var(--muted);font-size:9.5px;line-height:1.7}
.legend{display:flex;gap:9px;align-items:center;flex-wrap:wrap;padding:5px 10px;background:var(--s1);border-top:1px solid var(--bdr);font-size:7.5px;color:var(--muted);flex-shrink:0}
.cwrap{position:relative;flex:1;overflow:hidden;background:var(--bg);touch-action:none}
canvas{width:100%;height:100%;display:block}
.crosshair-label{position:absolute;background:rgba(10,14,22,0.92);border:1px solid var(--bdr2);border-radius:3px;padding:3px 6px;font-size:9px;color:var(--bright);pointer-events:none;display:none;z-index:20;white-space:nowrap}
.pred-grid{display:grid;grid-template-columns:1fr 260px;flex:1;overflow:hidden}
.pred-metrics{padding:12px;display:flex;flex-direction:column;gap:8px;overflow-y:auto;border-left:1px solid var(--bdr)}
.met-card{background:var(--s2);border:1px solid var(--bdr);border-radius:4px;padding:8px}
.met-title{font-size:7px;color:var(--muted);letter-spacing:.7px;margin-bottom:6px}
.met-val{font-size:18px;font-weight:700}
.met-sub{font-size:8px;color:var(--muted);margin-top:2px}
.report-body{padding:12px;display:flex;flex-direction:column;gap:12px;overflow-y:auto}
.rep-section{background:var(--s2);border:1px solid var(--bdr);border-radius:4px;padding:10px}
.rep-title{font-size:7.5px;color:var(--muted);letter-spacing:.8px;margin-bottom:8px;font-weight:700}
.rep-row{display:flex;justify-content:space-between;font-size:9px;padding:2px 0;border-bottom:1px solid var(--bdr)}
.rep-row:last-child{border-bottom:none}
.rep-k{color:var(--muted)}.rep-v{color:var(--bright);font-weight:700}
.rep-analysis{font-size:9.5px;line-height:1.7;color:var(--text);background:var(--s2);border:1px solid var(--bdr);border-radius:4px;padding:12px}
#fpScroll{overflow:auto;white-space:nowrap;flex:1}
.fp-bar{display:inline-flex;flex-direction:column;width:96px;vertical-align:top;border-right:1px solid var(--bdr);flex-shrink:0}
.fp-hdr{padding:4px;font-size:7px;color:var(--muted);border-bottom:1px solid var(--bdr2);text-align:center;background:var(--s1)}
.fp-hdr b{display:block;font-size:8.5px;color:var(--bright)}
.fp-row{display:grid;grid-template-columns:1fr 1fr;font-size:7.5px;border-bottom:1px solid #0b0f17;position:relative}
.fp-cell{padding:1.5px 3px;text-align:center;position:relative;z-index:1}
.fp-sell{color:#ff8a96}
.fp-buy{color:#7fe8b8}
.fp-poc{outline:1px solid var(--mag);outline-offset:-1px}
.fp-stacked{outline:1.5px solid #ffd166;box-shadow:0 0 4px rgba(255,209,102,.5)}
.fp-priceline{font-size:6.5px;color:var(--muted);text-align:center;padding:1px 0;background:rgba(255,255,255,.02)}
</style>
</head>
<body>

<div id="hdr">
  <span class="hdot" id="hdot"></span>
  <span class="pair">EUR/USDT</span>
  <div class="pill"><span class="l">MID</span><span class="v" id="midP" style="color:var(--bright)">-</span></div>
  <div class="pill"><span class="l">OPEN</span><span class="v" id="sessO" style="color:#8899aa;font-size:9px">-</span></div>
  <div class="pill" style="min-width:90px"><span class="l">RANGE</span><span class="v" id="sessR" style="color:#8899aa;font-size:8px">-</span></div>
  <div class="pill"><span class="l">VWAP</span><span class="v" id="vwapV" style="color:#8899aa;font-size:9px">-</span></div>
  <div class="pill" style="min-width:105px"><span class="l">MAGNET</span><span class="v" id="magV" style="color:var(--mag);font-size:9px">-</span></div>
  <div class="pill"><span class="l">DEPTH</span><span class="v" id="depV" style="color:var(--wall);font-size:9px">-</span></div>
  <div class="pill flow-pill"><span class="l">BUY/SELL</span><div class="flow-bar"><div class="flow-fill" id="flowB" style="width:50%"></div></div></div>
  <div class="pill" style="min-width:125px"><span class="l">TICK DATA (60d)</span><span class="v" id="backfillV" style="font-size:8px;color:var(--wall)">-</span></div>
  <div style="margin-left:auto;font-size:8px;color:var(--muted);flex-shrink:0" id="connLbl">Connecting...</div>
</div>

<div id="tabbar">
  <button class="tb active" onclick="tab('chart')">CHART</button>
  <button class="tb" onclick="tab('book')">BOOK</button>
  <button class="tb" onclick="tab('heat')">HEATMAP</button>
  <button class="tb" onclick="tab('vp')">VOL PROFILE</button>
  <button class="tb" onclick="tab('tpo')">TPO</button>
  <button class="tb" onclick="tab('fp')">FOOTPRINT</button>
  <button class="tb" onclick="tab('clus')">CLUSTERS</button>
  <button class="tb" onclick="tab('abso')">ABSORPTION</button>
  <button class="tb" onclick="tab('stop')">STOP ZONES</button>
  <button class="tb" onclick="tab('pred')">PREDICTION</button>
  <button class="tb" onclick="tab('rep')">REPORT</button>
</div>

<div id="v-chart" style="display:flex;flex-direction:column;height:calc(100vh - 78px)">
  <div class="toolbar">
    <button class="tool" id="tfBtn-60" onclick="chartSetTf(60)">1m</button>
    <button class="tool" id="tfBtn-300" onclick="chartSetTf(300)">5m</button>
    <button class="tool active" id="tfBtn-900" onclick="chartSetTf(900)">15m</button>
    <button class="tool" id="tfBtn-3600" onclick="chartSetTf(3600)">1H</button>
    <button class="tool" id="tfBtn-14400" onclick="chartSetTf(14400)">4H</button>
    <button class="tool" id="tfBtn-86400" onclick="chartSetTf(86400)">1D</button>
    <span style="width:1px;background:var(--bdr2);align-self:stretch;margin:0 3px"></span>
    <button class="tool" id="drawTrendBtn" onclick="chartSetDrawMode('trend')">+ Trend</button>
    <button class="tool" id="drawHBtn" onclick="chartSetDrawMode('hline')">+ H-Line</button>
    <button class="tool" onclick="chartClearDrawings()">Clear</button>
    <span style="width:1px;background:var(--bdr2);align-self:stretch;margin:0 3px"></span>
    <button class="tool" onclick="chartZoom(0.7)">Zoom +</button>
    <button class="tool" onclick="chartZoom(1.4)">Zoom -</button>
    <button class="tool" onclick="chartResetView()">Reset</button>
    <span id="chartInfo" style="margin-left:auto;color:var(--muted);font-size:7.5px">-</span>
  </div>
  <div class="cwrap" id="chartWrap"><canvas id="chartCanvas"></canvas><div class="crosshair-label" id="chartCross"></div></div>
  <div class="legend">
    <span style="color:var(--bid)">UP CANDLE</span><span style="color:var(--ask)">DOWN CANDLE</span>
    <span style="color:var(--wall)">VOLUME-TAPER BANDS = high-volume zones, fading toward present (liquidity magnets)</span>
    <span>drag to pan - wheel/buttons to zoom - tap Draw then 2 points for trendline, 1 point for h-line</span>
  </div>
</div>

<div id="v-book" style="display:none;grid-template-columns:1fr 1fr;height:calc(100vh - 78px)">
  <div class="panel">
    <div class="ptitle"><span>ORDER BOOK - 5000-LEVEL DEPTH</span><span id="seqLbl">seq -</span></div>
    <div class="pbody">
      <div class="col-hdr"><span>PRICE</span><span>QTY (EUR)</span><span>CUMUL</span><span></span></div>
      <div id="asks"></div>
      <div class="spdiv">
        <span><div>BEST BID</div><b id="sBid">-</b></span>
        <span><div>SPREAD</div><b id="sSpr">-</b></span>
        <span><div>BEST ASK</div><b id="sAsk">-</b></span>
      </div>
      <div id="bids"></div>
    </div>
    <div class="legend"><span>M=MAGNET</span><span>R=RESTING</span><span>P=PARTIAL</span><span style="color:var(--muted)">Structural after <span id="legAge">5</span>min</span></div>
  </div>
  <div class="panel rpanel">
    <div class="rtop">
      <div class="ptitle"><span>SESSION STRUCTURE</span><span style="color:var(--muted)">UTC DAY</span></div>
      <div class="pbody">
        <div class="struct-block">
          <div class="struct-row"><span class="sk">Open</span><span class="sv" id="stO">-</span></div>
          <div class="struct-row"><span class="sk">Range</span><span class="sv" id="stR">-</span></div>
          <div class="struct-row"><span class="sk">VWAP</span><span class="sv" id="stV">-</span></div>
          <div class="struct-row"><span class="sk">Magnet (POC)</span><span class="sv" id="stM">-</span></div>
          <div class="struct-row" id="confRow" style="display:none"><span class="sk">Confluence</span><span class="sv" id="stC" style="color:var(--mag)">-</span></div>
        </div>
        <div id="profBars"></div>
      </div>
    </div>
    <div class="rmid">
      <div class="ptitle"><span>STRUCTURAL RESTING ORDERS</span><span id="restCnt" style="color:var(--wall)">0</span></div>
      <div class="pbody" id="restList"><div class="empty">Scanning...</div></div>
    </div>
    <div class="rbot">
      <div class="ptitle"><span>STRUCTURAL EVENTS</span><span id="evCnt" style="color:var(--muted)">0</span></div>
      <div class="pbody" id="evLog"><div class="empty">Monitoring...</div></div>
    </div>
  </div>
</div>

<div id="v-heat" style="display:none;flex-direction:column;height:calc(100vh - 78px)">
  <div class="ptitle"><span>QUANT LIQUIDITY HEATMAP</span><span id="heatInfo" style="color:var(--muted)">0 frames</span></div>
  <div class="cwrap" id="heatWrap"><canvas id="heatCanvas"></canvas><div class="crosshair-label" id="heatCross"></div></div>
  <div class="legend">
    <span style="color:#3ab0ff">HIGH BID</span><span style="color:#ff5060">HIGH ASK</span>
    <span style="color:var(--void)">VOID</span><span style="color:#fff">MID PRICE</span>
    <span style="color:#ffa028">STRUCTURAL</span><span style="color:#3ddc84">BUY PRINT</span><span style="color:#ff5d6c">SELL PRINT</span>
  </div>
</div>

<div id="v-vp" style="display:none;flex-direction:column;height:calc(100vh - 78px)">
  <div class="ptitle"><span>VOLUME PROFILE (SINGLE RANGE)</span><span id="vpInfo" style="color:var(--muted)">-</span></div>
  <div class="toolbar">
    <select id="vpMode" onchange="refreshVP(true)"><option value="session">SESSION</option><option value="fixed">FIXED RANGE</option></select>
    <select id="vpTf" onchange="refreshVP(true)"><option value="60">1m</option><option value="300">5m</option><option value="900" selected>15m</option><option value="3600">1H</option><option value="14400">4H</option></select>
    <select id="vpLookback" onchange="refreshVP(true)"><option value="50">50 bars</option><option value="100" selected>100 bars</option><option value="200">200 bars</option></select>
    <select id="vpTick" onchange="refreshVP(true)"><option value="0.00005">0.5 pip</option><option value="0.0001" selected>1 pip</option><option value="0.0005">5 pip</option><option value="0.001">10 pip</option></select>
    <select id="vpVA" onchange="refreshVP(true)"><option value="60">VA 60%</option><option value="70" selected>VA 70%</option><option value="80">VA 80%</option><option value="90">VA 90%</option></select>
  </div>
  <div style="flex:1;display:grid;grid-template-columns:1fr 200px;overflow:hidden">
    <div class="cwrap" id="vpWrap"><canvas id="vpCanvas"></canvas></div>
    <div class="pred-metrics" id="vpMetrics" style="overflow-y:auto"><div class="empty">Loading...</div></div>
  </div>
  <div class="legend"><span style="color:var(--bid)">BUY VOL</span><span style="color:var(--ask)">SELL VOL</span><span style="color:var(--mag)">POC</span><span style="color:#64b4ff">VAH/VAL</span></div>
</div>

<div id="v-tpo" style="display:none;flex-direction:column;height:calc(100vh - 78px)">
  <div class="ptitle"><span>TPO - MULTI-SESSION COMPOSITE PROFILE</span><span id="tpoInfo" style="color:var(--muted)">-</span></div>
  <div class="toolbar">
    <select id="tpoN" onchange="refreshTPO()"><option value="5">5 sessions</option><option value="10" selected>10 sessions</option><option value="20">20 sessions</option><option value="30">30 sessions</option></select>
    <select id="tpoTick" onchange="refreshTPO()"><option value="0.0001" selected>1 pip</option><option value="0.0005">5 pip</option><option value="0.001">10 pip</option></select>
    <select id="tpoVA" onchange="refreshTPO()"><option value="60">VA 60%</option><option value="70" selected>VA 70%</option><option value="80">VA 80%</option></select>
    <span style="margin-left:auto;color:var(--muted);font-size:7px">drag to pan across sessions</span>
  </div>
  <div class="cwrap" id="tpoWrap"><canvas id="tpoCanvas"></canvas></div>
  <div class="legend"><span>each column = one session/day, shaped by relative volume per row</span><span style="color:var(--mag)">- POC line across sessions</span><span style="color:#64b4ff">VAH/VAL band</span></div>
</div>

<div id="v-fp" style="display:none;flex-direction:column;height:calc(100vh - 78px)">
  <div class="ptitle"><span>FOOTPRINT - NAVIGABLE ACROSS FULL TICK HISTORY</span><span id="fpInfo" style="color:var(--muted)">-</span></div>
  <div class="toolbar">
    <select id="fpTf" onchange="fpReload()"><option value="60" selected>1m</option><option value="300">5m</option><option value="900">15m</option><option value="3600">1H</option></select>
    <select id="fpBars" onchange="fpReload()"><option value="20">20 bars</option><option value="40" selected>40 bars</option><option value="60">60 bars</option></select>
    <select id="fpTick" onchange="fpReload()"><option value="0.0001" selected>1 pip</option><option value="0.0005">5 pip</option><option value="0.001">10 pip</option></select>
    <select id="fpImb" onchange="fpReload()"><option value="2">2x imbalance</option><option value="3" selected>3x imbalance</option><option value="4">4x imbalance</option></select>
    <button class="tool" onclick="fpPage(-1)">&lt;&lt; Older</button>
    <button class="tool" onclick="fpPage(1)">Newer &gt;&gt;</button>
    <button class="tool" onclick="fpReload()">Now</button>
  </div>
  <div id="fpScroll" class="pbody"><div class="empty">Loading footprint...</div></div>
  <div class="legend"><span style="color:var(--bid)">BUY</span><span style="color:var(--ask)">SELL</span><span style="color:var(--mag)">BAR POC</span><span style="color:#ffd166">STACKED IMBALANCE</span></div>
</div>

<div id="v-clus" style="display:none;flex-direction:column;height:calc(100vh - 78px)">
  <div class="ptitle"><span>LIQUIDITY CLUSTERS</span><span id="clusCnt" style="color:var(--wall)">0 clusters</span></div>
  <div style="flex:1;display:grid;grid-template-columns:1fr 1fr;overflow:hidden">
    <div style="display:flex;flex-direction:column;border-right:1px solid var(--bdr);overflow:hidden">
      <div class="ptitle" style="height:22px"><span style="color:var(--bid)">BID CLUSTERS</span></div>
      <div class="pbody" id="clusBid"><div class="empty">Building...</div></div>
    </div>
    <div style="display:flex;flex-direction:column;overflow:hidden">
      <div class="ptitle" style="height:22px"><span style="color:var(--ask)">ASK CLUSTERS</span></div>
      <div class="pbody" id="clusAsk"><div class="empty">Building...</div></div>
    </div>
  </div>
  <div class="legend"><span>cluster = outsized resting qty persisting across frames</span></div>
</div>

<div id="v-abso" style="display:none;flex-direction:column;height:calc(100vh - 78px)">
  <div class="ptitle"><span>ABSORPTION MAP</span><span id="absCnt" style="color:#e080ff">0 zones</span></div>
  <div class="pbody" id="absList"><div class="empty">Detecting...</div></div>
  <div class="legend"><span style="color:#e080ff">HIGH ABSORPTION</span></div>
</div>

<div id="v-stop" style="display:none;flex-direction:column;height:calc(100vh - 78px)">
  <div class="ptitle"><span>STOP-LOSS LIQUIDITY MAP</span><span id="atrLbl" style="color:var(--muted)">ATR -</span></div>
  <div class="pbody" id="stopList"><div class="empty">Detecting...</div></div>
  <div class="legend"><span style="color:var(--ask)">SELL STOPS</span><span style="color:var(--bid)">BUY STOPS</span></div>
</div>

<div id="v-pred" style="display:none;flex-direction:column;height:calc(100vh - 78px)">
  <div class="ptitle"><span>5-HOUR MONTE CARLO</span><span id="predInfo" style="color:var(--muted)">-</span></div>
  <div class="pred-grid">
    <div class="cwrap"><canvas id="predCanvas"></canvas></div>
    <div class="pred-metrics" id="predMetrics"><div class="empty">Computing...</div></div>
  </div>
  <div class="legend"><span style="color:var(--mag)">MEDIAN</span></div>
</div>

<div id="v-rep" style="display:none;flex-direction:column;height:calc(100vh - 78px)">
  <div class="ptitle"><span>INSTITUTIONAL MARKET REPORT</span><span id="repTs" style="color:var(--muted)">-</span></div>
  <div class="report-body" id="repBody"><div class="empty">Generating...</div></div>
</div>

<script>
var prevMid = null, prevEvLen = -1, activeTab = 'chart';
var HEAT_INTERVAL_S = 2;

var TAB_ORDER = ['chart','book','heat','vp','tpo','fp','clus','abso','stop','pred','rep'];
var VIEW_IDS  = TAB_ORDER.map(function(t){ return 'v-' + t; });

function tab(id) {
  activeTab = id;
  var btns = document.querySelectorAll('.tb');
  for (var i=0;i<btns.length;i++){ btns[i].classList.toggle('active', TAB_ORDER[i] === id); }
  VIEW_IDS.forEach(function(vid){
    var el = document.getElementById(vid);
    if (!el) return;
    el.style.display = (vid === 'v-' + id) ? (id === 'book' ? 'grid' : 'flex') : 'none';
  });
  if (id === 'chart') { resizeCvs(chartCvs, chartCtx); drawChart(); chartFetch(); }
  if (id === 'heat')  { resizeCvs(heatCvs, heatCtx); drawHeat(); }
  if (id === 'pred')  { resizeCvs(predCvs, predCtx); drawPred(); }
  if (id === 'vp')    { resizeCvs(vpCvs, vpCtx); refreshVP(false); }
  if (id === 'tpo')   { resizeCvs(tpoCvs, tpoCtx); refreshTPO(); }
  if (id === 'fp')    { if (!fpLoaded) fpReload(); }
}

var heatCvs = document.getElementById('heatCanvas'); var heatCtx = heatCvs.getContext('2d');
var predCvs = document.getElementById('predCanvas'); var predCtx = predCvs.getContext('2d');
var vpCvs   = document.getElementById('vpCanvas');   var vpCtx   = vpCvs.getContext('2d');
var tpoCvs  = document.getElementById('tpoCanvas');  var tpoCtx  = tpoCvs.getContext('2d');
var chartCvs = document.getElementById('chartCanvas'); var chartCtx = chartCvs.getContext('2d');

function resizeCvs(cvs, ctx) {
  var dpr = window.devicePixelRatio || 1;
  var r = cvs.parentElement.getBoundingClientRect();
  cvs.width = Math.max(1, Math.floor(r.width * dpr));
  cvs.height = Math.max(1, Math.floor(r.height * dpr));
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
}
window.addEventListener('resize', function(){
  if (activeTab === 'heat')  { resizeCvs(heatCvs, heatCtx); drawHeat(); }
  if (activeTab === 'pred')  { resizeCvs(predCvs, predCtx); drawPred(); }
  if (activeTab === 'vp')    { resizeCvs(vpCvs, vpCtx); drawVP(); }
  if (activeTab === 'tpo')   { resizeCvs(tpoCvs, tpoCtx); drawTPOChart(); }
  if (activeTab === 'chart') { resizeCvs(chartCvs, chartCtx); drawChart(); }
});

// =====================================================================
//  CANDLESTICK CHART - pan/zoom/drawing tools/volume-taper overlay
// =====================================================================
var chartTf = 900;
var chartCandles = [];
var chartViewStart = null, chartViewEnd = null;
var chartDataSpan = null;
var chartDrawMode = 'none';
var chartPendingPoint = null;
var chartDrawings = [];
var chartGeom = null;
var chartFetchTimer = null;
var chartDragging = false, chartDragStartX = 0, chartDragStartView = null;

try { chartDrawings = JSON.parse(localStorage.getItem('eurusdt_drawings') || '[]'); } catch(e) { chartDrawings = []; }
function saveDrawings() { try { localStorage.setItem('eurusdt_drawings', JSON.stringify(chartDrawings)); } catch(e) {} }

function chartSetTf(tf) {
  chartTf = tf;
  ['60','300','900','3600','14400','86400'].forEach(function(t){
    var b = document.getElementById('tfBtn-'+t);
    if (b) b.classList.toggle('active', parseInt(t) === tf);
  });
  chartResetView();
}
function chartResetView() {
  var now = Date.now();
  chartViewEnd = now;
  chartViewStart = now - 130 * chartTf * 1000;
  chartFetch();
}
function chartZoom(factor) {
  if (chartViewStart == null) return;
  var mid = (chartViewStart + chartViewEnd) / 2;
  var halfW = (chartViewEnd - chartViewStart) / 2 * factor;
  chartViewStart = mid - halfW; chartViewEnd = mid + halfW;
  drawChart();
  scheduleChartFetch();
}
function chartSetDrawMode(mode) {
  chartDrawMode = (chartDrawMode === mode) ? 'none' : mode;
  chartPendingPoint = null;
  document.getElementById('drawTrendBtn').classList.toggle('active', chartDrawMode === 'trend');
  document.getElementById('drawHBtn').classList.toggle('active', chartDrawMode === 'hline');
}
function chartClearDrawings() { chartDrawings = []; saveDrawings(); drawChart(); }

function scheduleChartFetch() {
  if (chartFetchTimer) clearTimeout(chartFetchTimer);
  chartFetchTimer = setTimeout(chartFetch, 220);
}

function chartFetch() {
  if (chartViewStart == null) chartResetView();
  var url = '/api/candles?tf=' + chartTf + '&start=' + Math.floor(chartViewStart) + '&end=' + Math.ceil(chartViewEnd) + '&max_bars=1200';
  fetch(url).then(function(r){ return r.ok ? r.json() : null; }).then(function(d){
    if (!d) return;
    chartCandles = d.candles || [];
    chartDataSpan = d.data_span;
    document.getElementById('chartInfo').textContent = chartCandles.length + ' candles - ' +
      (chartDataSpan && chartDataSpan.count ? (Math.round((chartDataSpan.max_ts-chartDataSpan.min_ts)/86400000*10)/10 + 'd of tick data available') : 'no tick data yet');
    drawChart();
  }).catch(function(){});
}

function chartPriceToY(p, lo, hi, H) { return H - ((p - lo) / ((hi - lo) || 1e-9)) * H; }
function chartTimeToX(t, vs, ve, W) { return ((t - vs) / ((ve - vs) || 1)) * W; }

function drawChart() {
  var dpr = window.devicePixelRatio || 1;
  var W = chartCvs.width / dpr, H = chartCvs.height / dpr;
  chartCtx.fillStyle = '#07090e'; chartCtx.fillRect(0,0,W,H);
  if (chartViewStart == null) { chartGeom = null; return; }
  var vs = chartViewStart, ve = chartViewEnd;

  if (!chartCandles.length) {
    chartCtx.fillStyle = '#3a4a5e'; chartCtx.font = '11px monospace';
    chartCtx.fillText('No candles in this range yet - tick backfill may still be in progress.', 12, 24);
    chartGeom = {vs:vs, ve:ve, W:W, H:H, lo:1, hi:1.2};
    return;
  }

  var lo = Infinity, hi = -Infinity;
  chartCandles.forEach(function(c){ if (c.l<lo) lo=c.l; if (c.h>hi) hi=c.h; });
  var pad = (hi-lo)*0.08 || 0.0005;
  lo -= pad; hi += pad;
  var axisW = 58;
  var plotW = W - axisW;

  // volume-taper overlay: bands at high-volume rows of the visible window, fading toward present (right edge)
  var volMap = {};
  var tickSz = Math.max(0.00005, (hi-lo)/120);
  chartCandles.forEach(function(c){
    var pb = (Math.round(((c.h+c.l)/2)/tickSz)*tickSz).toFixed(5);
    volMap[pb] = (volMap[pb]||0) + c.vol;
  });
  var volEntries = Object.entries(volMap);
  if (volEntries.length) {
    var maxVol = Math.max.apply(null, volEntries.map(function(e){return e[1];}));
    volEntries.sort(function(a,b){return b[1]-a[1];});
    var topN = volEntries.slice(0, 4);
    topN.forEach(function(e){
      var pf = parseFloat(e[0]);
      var y = chartPriceToY(pf, lo, hi, H);
      var rowH = Math.max(2, (tickSz/(hi-lo))*H);
      var strength = e[1]/maxVol;
      var grad = chartCtx.createLinearGradient(axisW, 0, W, 0);
      grad.addColorStop(0, 'rgba(100,180,255,' + (0.05+0.20*strength) + ')');
      grad.addColorStop(1, 'rgba(100,180,255,' + (0.22+0.35*strength) + ')');
      chartCtx.fillStyle = grad;
      chartCtx.fillRect(axisW, y-rowH, plotW, rowH*2);
    });
  }

  // grid + price axis
  chartCtx.strokeStyle = 'rgba(255,255,255,0.05)'; chartCtx.fillStyle = '#7a93ad';
  chartCtx.font = '8px monospace'; chartCtx.textAlign = 'left';
  for (var gi=0; gi<=6; gi++) {
    var gp = lo + (hi-lo)*(gi/6);
    var gy = chartPriceToY(gp, lo, hi, H);
    chartCtx.beginPath(); chartCtx.moveTo(axisW,gy); chartCtx.lineTo(W,gy); chartCtx.stroke();
    chartCtx.fillText(gp.toFixed(5), 2, Math.max(9,Math.min(H-3,gy-2)));
  }

  // candles
  var bw = Math.max(1, Math.min(18, (plotW / Math.max(1,chartCandles.length)) * 0.7));
  chartCandles.forEach(function(c){
    var cx = axisW + chartTimeToX(c.ts + chartTf*500, vs, ve, plotW);
    if (cx < axisW-bw || cx > W+bw) return;
    var up = c.c >= c.o;
    var col = up ? '#00c87a' : '#ff3d5a';
    chartCtx.strokeStyle = col; chartCtx.fillStyle = col; chartCtx.lineWidth = 1;
    var yH = chartPriceToY(c.h, lo, hi, H), yL = chartPriceToY(c.l, lo, hi, H);
    chartCtx.beginPath(); chartCtx.moveTo(cx, yH); chartCtx.lineTo(cx, yL); chartCtx.stroke();
    var yO = chartPriceToY(c.o, lo, hi, H), yC = chartPriceToY(c.c, lo, hi, H);
    var bodyTop = Math.min(yO,yC), bodyH = Math.max(1, Math.abs(yC-yO));
    chartCtx.fillRect(cx-bw/2, bodyTop, bw, bodyH);
  });

  // drawings (trendlines + horizontal lines), stored in price/time space
  chartDrawings.forEach(function(d){
    chartCtx.strokeStyle = '#ffd166'; chartCtx.lineWidth = 1.3; chartCtx.setLineDash([]);
    if (d.type === 'hline') {
      var hy = chartPriceToY(d.price, lo, hi, H);
      chartCtx.beginPath(); chartCtx.moveTo(axisW, hy); chartCtx.lineTo(W, hy); chartCtx.stroke();
      chartCtx.fillStyle = '#ffd166'; chartCtx.font = '8px monospace';
      chartCtx.fillText(d.price.toFixed(5), W-58, hy-3);
    } else if (d.type === 'trend') {
      var x1 = axisW + chartTimeToX(d.t1, vs, ve, plotW), y1 = chartPriceToY(d.p1, lo, hi, H);
      var x2 = axisW + chartTimeToX(d.t2, vs, ve, plotW), y2 = chartPriceToY(d.p2, lo, hi, H);
      chartCtx.beginPath(); chartCtx.moveTo(x1,y1); chartCtx.lineTo(x2,y2); chartCtx.stroke();
      chartCtx.fillStyle = '#ffd166'; chartCtx.beginPath(); chartCtx.arc(x1,y1,2.5,0,Math.PI*2); chartCtx.fill();
      chartCtx.beginPath(); chartCtx.arc(x2,y2,2.5,0,Math.PI*2); chartCtx.fill();
    }
  });
  if (chartPendingPoint) {
    var px = axisW + chartTimeToX(chartPendingPoint.t, vs, ve, plotW);
    var py = chartPriceToY(chartPendingPoint.p, lo, hi, H);
    chartCtx.fillStyle = '#ffd166'; chartCtx.beginPath(); chartCtx.arc(px,py,3,0,Math.PI*2); chartCtx.fill();
  }

  chartGeom = {vs:vs, ve:ve, W:W, H:H, lo:lo, hi:hi, axisW:axisW, plotW:plotW};
}

function chartXYtoPriceTime(px, py) {
  var g = chartGeom; if (!g) return null;
  var t = g.vs + ((px - g.axisW) / g.plotW) * (g.ve - g.vs);
  var p = g.lo + (1 - py / g.H) * (g.hi - g.lo);
  return {t:t, p:p};
}

var chartWrap = document.getElementById('chartWrap');
var chartCross = document.getElementById('chartCross');

function chartPointerDown(evt) {
  var rect = chartWrap.getBoundingClientRect();
  var x = (evt.touches?evt.touches[0].clientX:evt.clientX) - rect.left;
  var y = (evt.touches?evt.touches[0].clientY:evt.clientY) - rect.top;
  if (chartDrawMode !== 'none') {
    var pt = chartXYtoPriceTime(x, y);
    if (!pt) return;
    if (chartDrawMode === 'hline') {
      chartDrawings.push({type:'hline', price: pt.p});
      saveDrawings(); chartSetDrawMode('hline'); drawChart();
    } else if (chartDrawMode === 'trend') {
      if (!chartPendingPoint) { chartPendingPoint = pt; drawChart(); }
      else {
        chartDrawings.push({type:'trend', t1:chartPendingPoint.t, p1:chartPendingPoint.p, t2:pt.t, p2:pt.p});
        chartPendingPoint = null; saveDrawings();
        chartSetDrawMode('trend'); drawChart();
      }
    }
    return;
  }
  chartDragging = true; chartDragStartX = x; chartDragStartView = {s:chartViewStart, e:chartViewEnd};
}
function chartPointerMove(evt) {
  var rect = chartWrap.getBoundingClientRect();
  var x = (evt.touches?evt.touches[0].clientX:evt.clientX) - rect.left;
  var y = (evt.touches?evt.touches[0].clientY:evt.clientY) - rect.top;
  if (chartGeom) {
    var pt = chartXYtoPriceTime(x,y);
    if (pt) {
      chartCross.style.display = 'block';
      chartCross.style.left = Math.min(chartGeom.W-120, x+8)+'px';
      chartCross.style.top = Math.max(2,y-20)+'px';
      chartCross.textContent = pt.p.toFixed(5);
    }
  }
  if (!chartDragging) return;
  var dx = x - chartDragStartX;
  var span = chartDragStartView.e - chartDragStartView.s;
  var deltaT = -(dx / (chartGeom ? chartGeom.plotW : 600)) * span;
  chartViewStart = chartDragStartView.s + deltaT;
  chartViewEnd = chartDragStartView.e + deltaT;
  drawChart();
  scheduleChartFetch();
}
function chartPointerUp() { chartDragging = false; }
chartWrap.addEventListener('mousedown', chartPointerDown);
chartWrap.addEventListener('mousemove', chartPointerMove);
chartWrap.addEventListener('mouseup', chartPointerUp);
chartWrap.addEventListener('mouseleave', function(){ chartDragging=false; chartCross.style.display='none'; });
chartWrap.addEventListener('touchstart', function(e){ chartPointerDown(e); }, {passive:true});
chartWrap.addEventListener('touchmove', function(e){ chartPointerMove(e); }, {passive:true});
chartWrap.addEventListener('touchend', chartPointerUp);
chartWrap.addEventListener('wheel', function(e){
  e.preventDefault();
  chartZoom(e.deltaY > 0 ? 1.15 : 0.87);
}, {passive:false});

// =====================================================================
//  HEATMAP
// =====================================================================
var heatFrames = [], heatLastIdx = 0, heatBands = [], heatTrades = [];
var lastHeatGeom = null;

function interpColor(stops, t) {
  t = Math.max(0, Math.min(1, t));
  for (var i = 1; i < stops.length; i++) {
    if (t <= stops[i][0]) {
      var p0 = stops[i-1][0], c0 = stops[i-1][1];
      var p1 = stops[i][0], c1 = stops[i][1];
      var f = (t - p0) / ((p1 - p0) || 1);
      return [0,1,2].map(function(k){ return Math.round(c0[k] + (c1[k]-c0[k])*f); });
    }
  }
  return stops[stops.length-1][1];
}
function heatColorBid(t) { return interpColor([[0,[10,12,30]],[0.25,[20,40,90]],[0.55,[30,110,200]],[0.8,[70,190,230]],[1,[210,245,255]]], t); }
function heatColorAsk(t) { return interpColor([[0,[10,12,30]],[0.25,[90,20,40]],[0.55,[200,40,55]],[0.8,[255,120,90]],[1,[255,235,210]]], t); }
function percentile(arr, p) {
  if (!arr.length) return 0.001;
  var s = arr.slice().sort(function(a,b){return a-b;});
  return s[Math.min(s.length-1, Math.floor(s.length*p))] || 0.001;
}

function drawHeat() {
  var dpr = window.devicePixelRatio || 1;
  var W = heatCvs.width / dpr, H = heatCvs.height / dpr;
  heatCtx.fillStyle = '#07090e'; heatCtx.fillRect(0, 0, W, H);
  if (!heatFrames.length) {
    heatCtx.fillStyle = '#3a4a5e'; heatCtx.font = '11px monospace';
    heatCtx.fillText('Building heatmap...', 12, 24); lastHeatGeom = null; return;
  }
  var minP = Infinity, maxP = -Infinity;
  heatFrames.forEach(function(f){
    for (var p in f.bids) { var pf = parseFloat(p); if (pf<minP) minP=pf; if (pf>maxP) maxP=pf; }
    for (var p2 in f.asks) { var pf2 = parseFloat(p2); if (pf2<minP) minP=pf2; if (pf2>maxP) maxP=pf2; }
  });
  var pad = (maxP-minP)*0.05 || 0.0005; minP -= pad; maxP += pad;
  function prToY(p) { return H - ((p-minP)/(maxP-minP))*H; }
  var COB = 30, cW = Math.max(1, (W-COB)/heatFrames.length);
  var step = 0.0001;
  var lastFrame = heatFrames[heatFrames.length-1];
  var allP = Object.keys(lastFrame.bids).concat(Object.keys(lastFrame.asks)).map(parseFloat).sort(function(a,b){return a-b;});
  if (allP.length>1) {
    var diffs=[]; for (var i=1;i<allP.length;i++){var d=allP[i]-allP[i-1]; if(d>0) diffs.push(d);}
    if (diffs.length){ diffs.sort(function(a,b){return a-b;}); step=diffs[Math.floor(diffs.length/2)]; }
  }
  var rowH = Math.max(1, (step/(maxP-minP))*H);
  heatFrames.forEach(function(f, ci){
    var x = ci*cW;
    var bNorm = percentile(Object.values(f.bids), 0.92)||0.001;
    var aNorm = percentile(Object.values(f.asks), 0.92)||0.001;
    var fAllP = Object.keys(f.bids).concat(Object.keys(f.asks)).map(parseFloat);
    if (fAllP.length) {
      var fMin=Math.min.apply(null,fAllP), fMax=Math.max.apply(null,fAllP);
      heatCtx.fillStyle='rgba(150,90,220,0.22)';
      heatCtx.fillRect(x, prToY(fMax)-rowH, cW+1, prToY(fMin)-prToY(fMax)+rowH*2);
      for (var pr=fMin; pr<=fMax+step*0.5; pr+=step) {
        var pk=pr.toFixed(5), pkR=(Math.round(pr/step)*step).toFixed(5);
        var has=(f.bids[pk]!==undefined)||(f.asks[pk]!==undefined)||(f.bids[pkR]!==undefined)||(f.asks[pkR]!==undefined);
        if (!has) { heatCtx.fillStyle='rgba(180,106,255,0.40)'; heatCtx.fillRect(x, prToY(pr)-rowH/2, cW+1, rowH+1); }
      }
    }
    for (var p3 in f.bids) {
      var t=Math.pow(Math.min(1,f.bids[p3]/bNorm),0.6); var rgb=heatColorBid(t);
      heatCtx.fillStyle='rgb('+rgb[0]+','+rgb[1]+','+rgb[2]+')';
      heatCtx.fillRect(x, prToY(parseFloat(p3))-rowH/2, cW+0.6, rowH+1);
    }
    for (var p4 in f.asks) {
      var t2=Math.pow(Math.min(1,f.asks[p4]/aNorm),0.6); var rgb2=heatColorAsk(t2);
      heatCtx.fillStyle='rgb('+rgb2[0]+','+rgb2[1]+','+rgb2[2]+')';
      heatCtx.fillRect(x, prToY(parseFloat(p4))-rowH/2, cW+0.6, rowH+1);
    }
  });
  heatBands.forEach(function(b){ heatCtx.fillStyle='rgba(255,160,40,0.30)'; heatCtx.fillRect(0, prToY(parseFloat(b.price))-rowH, W, rowH*2); });
  heatCtx.strokeStyle='#fff'; heatCtx.lineWidth=1.5; heatCtx.beginPath();
  heatFrames.forEach(function(f,ci){ var x=ci*cW+cW/2,y=prToY(f.mid); if(ci===0) heatCtx.moveTo(x,y); else heatCtx.lineTo(x,y); });
  heatCtx.stroke();
  var t0=heatFrames[0].ts, t1=heatFrames[heatFrames.length-1].ts, tSpan=Math.max(t1-t0,1);
  heatTrades.forEach(function(tr){
    if (tr.ts<t0||tr.ts>t1) return;
    var x=((tr.ts-t0)/tSpan)*(W-COB), y=prToY(tr.price), rad=Math.max(2.5,Math.min(13,Math.sqrt(tr.qty)*2.4));
    heatCtx.beginPath(); heatCtx.fillStyle=tr.side==='BUY'?'rgba(61,220,132,0.65)':'rgba(255,93,108,0.65)';
    heatCtx.arc(x,y,rad,0,Math.PI*2); heatCtx.fill();
  });
  var last=heatFrames[heatFrames.length-1];
  var maxQ=Math.max.apply(null, Object.values(last.bids).concat(Object.values(last.asks)).concat([0.001]));
  for (var pb in last.bids) { var pfb=parseFloat(pb); if(pfb<minP||pfb>maxP) continue;
    heatCtx.fillStyle='rgba(0,200,122,0.75)'; heatCtx.fillRect(W-COB, prToY(pfb)-rowH/2, (last.bids[pb]/maxQ)*COB, rowH); }
  for (var pa in last.asks) { var pfa=parseFloat(pa); if(pfa<minP||pfa>maxP) continue;
    heatCtx.fillStyle='rgba(255,61,90,0.75)'; heatCtx.fillRect(W-COB, prToY(pfa)-rowH/2, (last.asks[pa]/maxQ)*COB, rowH); }
  heatCtx.fillStyle='rgba(7,9,14,0.55)'; heatCtx.fillRect(0,0,58,H);
  heatCtx.fillStyle='#7a93ad'; heatCtx.font='8px monospace'; heatCtx.textAlign='left';
  for (var gi=0; gi<=7; gi++) {
    var pgf=minP+(maxP-minP)*(gi/7), gy=prToY(pgf);
    heatCtx.strokeStyle='rgba(255,255,255,0.05)'; heatCtx.beginPath(); heatCtx.moveTo(0,gy); heatCtx.lineTo(W-COB,gy); heatCtx.stroke();
    heatCtx.fillText(pgf.toFixed(5), 3, Math.max(9,Math.min(H-3,gy-2)));
  }
  document.getElementById('heatInfo').textContent = heatFrames.length+' frames';
  lastHeatGeom = {minP:minP,maxP:maxP,W:W,H:H};
}
function refreshHeat() {
  if (activeTab !== 'heat') return;
  fetch('/api/heatmap?since='+heatLastIdx).then(function(r){return r.ok?r.json():null;}).then(function(d){
    if (!d) return;
    if (d.frames && d.frames.length) { heatFrames = heatFrames.concat(d.frames); if (heatFrames.length>220) heatFrames=heatFrames.slice(heatFrames.length-220); heatLastIdx=d.latest_index; }
    heatBands = d.bands||[]; heatTrades = d.trades||[];
    drawHeat();
  }).catch(function(){});
}

// =====================================================================
//  VOLUME PROFILE (single range)
// =====================================================================
var vpData = null;
function drawVP() {
  var dpr=window.devicePixelRatio||1, W=vpCvs.width/dpr, H=vpCvs.height/dpr;
  vpCtx.fillStyle='#07090e'; vpCtx.fillRect(0,0,W,H);
  var prof = vpData && vpData.profile;
  if (!prof || !prof.levels || !prof.levels.length) { vpCtx.fillStyle='#3a4a5e'; vpCtx.font='11px monospace'; vpCtx.fillText('Waiting for tick data...',12,24); return; }
  var levels = prof.levels;
  var minP=parseFloat(levels[0].price), maxP=parseFloat(levels[levels.length-1].price);
  var pad=(maxP-minP)*0.03||0.0005, lo=minP-pad, hi=maxP+pad;
  function prToY(p){ return H-((p-lo)/(hi-lo))*H; }
  var rowH=Math.max(1.5,H/levels.length);
  var maxTotal=Math.max.apply(null, levels.map(function(l){return l.total;}).concat([0.0001]));
  var axisW=56, barMaxW=W-axisW-6;
  levels.forEach(function(l){
    var y=prToY(parseFloat(l.price));
    if (l.node==='HVN'){ vpCtx.fillStyle='rgba(255,255,255,0.04)'; vpCtx.fillRect(axisW,y-rowH/2,barMaxW,rowH); }
    var buyW=(l.buy/maxTotal)*barMaxW, sellW=(l.sell/maxTotal)*barMaxW;
    vpCtx.fillStyle = l.in_value_area?'rgba(255,61,90,0.85)':'rgba(255,61,90,0.4)'; vpCtx.fillRect(axisW,y-rowH/2,sellW,Math.max(1,rowH-0.5));
    vpCtx.fillStyle = l.in_value_area?'rgba(0,200,122,0.85)':'rgba(0,200,122,0.4)'; vpCtx.fillRect(axisW+sellW,y-rowH/2,buyW,Math.max(1,rowH-0.5));
  });
  [['vah','#64b4ff'],['val','#64b4ff'],['poc','#ffd166']].forEach(function(pair){
    var k=pair[0], c=pair[1]; if (prof[k]==null) return;
    var y=prToY(parseFloat(prof[k]));
    vpCtx.strokeStyle=c; vpCtx.lineWidth=k==='poc'?2:1; vpCtx.setLineDash(k==='poc'?[]:[4,3]);
    vpCtx.beginPath(); vpCtx.moveTo(axisW,y); vpCtx.lineTo(W,y); vpCtx.stroke(); vpCtx.setLineDash([]);
  });
  vpCtx.fillStyle='#7a93ad'; vpCtx.font='8px monospace'; vpCtx.textAlign='right';
  var step=Math.max(1,Math.floor(levels.length/12));
  for (var i=0;i<levels.length;i+=step){ var y2=prToY(parseFloat(levels[i].price)); vpCtx.fillText(levels[i].price, axisW-4, Math.max(9,Math.min(H-3,y2+3))); }
}
function refreshVP(force) {
  if (activeTab!=='vp' && !force) return;
  var mode=document.getElementById('vpMode').value, tfv=document.getElementById('vpTf').value;
  var lookback=document.getElementById('vpLookback').value, tick=document.getElementById('vpTick').value, va=document.getElementById('vpVA').value;
  fetch('/api/volume_profile?mode='+mode+'&tf='+tfv+'&lookback='+lookback+'&tick_size='+tick+'&va_pct='+va)
    .then(function(r){return r.ok?r.json():null;}).then(function(d){
      if (!d) return; vpData=d; resizeCvs(vpCvs,vpCtx); drawVP();
      var p=vpData.profile;
      document.getElementById('vpInfo').textContent = p?(p.levels.length+' rows - '+p.total_volume+' EUR'):'building...';
      var naked=vpData.naked_pocs||[];
      var nakedHtml = naked.length ? naked.slice().reverse().map(function(n){
        return '<div class="struct-row"><span class="sk">'+n.date+(n.tested?'':' *')+'</span><span class="sv" style="color:'+(n.tested?'var(--muted)':'var(--mag)')+'">'+n.poc+(n.tested?' (tested)':' (NAKED)')+'</span></div>';
      }).join('') : '<div class="empty">No prior sessions yet.</div>';
      document.getElementById('vpMetrics').innerHTML = p ? (
        '<div class="met-card"><div class="met-title">POC</div><div class="met-val" style="color:var(--mag)">'+p.poc+'</div></div>'+
        '<div class="met-card"><div class="met-title">VALUE AREA</div><div class="met-sub" style="font-size:10px">'+p.val+' to '+p.vah+'</div></div>'+
        '<div class="met-card"><div class="met-title">TOTAL VOLUME</div><div class="met-sub" style="font-size:10px">'+p.total_volume+' EUR</div></div>'+
        '<div class="met-card"><div class="met-title">NAKED POCs</div>'+nakedHtml+'</div>'
      ) : '<div class="empty">Waiting for tick data...</div>';
    }).catch(function(){});
}

// =====================================================================
//  TPO - MULTI-SESSION COMPOSITE STAIRCASE
// =====================================================================
var tpoSessions = [], tpoPanX = 0, tpoDragging = false, tpoDragStartX = 0, tpoDragStartPan = 0;
function tpoVolColor(t) {
  return interpColor([[0,[20,40,140]],[0.35,[20,150,140]],[0.6,[60,200,90]],[0.8,[230,210,40]],[1,[230,60,40]]], t);
}
function drawTPOChart() {
  var dpr=window.devicePixelRatio||1, W=tpoCvs.width/dpr, H=tpoCvs.height/dpr;
  tpoCtx.fillStyle='#07090e'; tpoCtx.fillRect(0,0,W,H);
  if (!tpoSessions.length) { tpoCtx.fillStyle='#3a4a5e'; tpoCtx.font='11px monospace'; tpoCtx.fillText('Waiting for tick data (need full UTC sessions)...',12,24); return; }
  var allLevels = [];
  tpoSessions.forEach(function(s){ s.levels.forEach(function(l){ allLevels.push(parseFloat(l.price)); }); });
  var lo=Math.min.apply(null,allLevels), hi=Math.max.apply(null,allLevels);
  var pad=(hi-lo)*0.04||0.0005; lo-=pad; hi+=pad;
  function prToY(p){ return H-((p-lo)/(hi-lo))*H; }
  var colW = Math.max(60, (W-40)/Math.max(1,Math.min(tpoSessions.length,6)));
  var axisW=50;
  var poc_pts = [];
  tpoSessions.forEach(function(s, si){
    var x0 = axisW + si*colW - tpoPanX;
    if (x0 + colW < 0 || x0 > W) { return; }
    var maxTotal = Math.max.apply(null, s.levels.map(function(l){return l.total;}).concat([0.0001]));
    var rowH = Math.max(1, H / s.levels.length);
    s.levels.forEach(function(l){
      var y = prToY(parseFloat(l.price));
      var w = (l.total/maxTotal) * (colW*0.82);
      var rgb = tpoVolColor(l.total/maxTotal);
      tpoCtx.fillStyle = 'rgb('+rgb[0]+','+rgb[1]+','+rgb[2]+')';
      tpoCtx.fillRect(x0, y-rowH/2, Math.max(1,w), Math.max(1,rowH));
    });
    // value area band
    tpoCtx.strokeStyle='rgba(100,180,255,0.6)'; tpoCtx.lineWidth=1; tpoCtx.setLineDash([3,2]);
    tpoCtx.beginPath(); tpoCtx.moveTo(x0, prToY(parseFloat(s.vah))); tpoCtx.lineTo(x0+colW*0.82, prToY(parseFloat(s.vah))); tpoCtx.stroke();
    tpoCtx.beginPath(); tpoCtx.moveTo(x0, prToY(parseFloat(s.val))); tpoCtx.lineTo(x0+colW*0.82, prToY(parseFloat(s.val))); tpoCtx.stroke();
    tpoCtx.setLineDash([]);
    // POC marker
    var py = prToY(parseFloat(s.poc));
    tpoCtx.strokeStyle='#ffd166'; tpoCtx.lineWidth=2;
    tpoCtx.beginPath(); tpoCtx.moveTo(x0, py); tpoCtx.lineTo(x0+colW*0.82, py); tpoCtx.stroke();
    poc_pts.push({x: x0+colW*0.41, y: py});
    // date label
    tpoCtx.fillStyle='#7a93ad'; tpoCtx.font='8px monospace'; tpoCtx.textAlign='center';
    tpoCtx.fillText(s.date.slice(5), x0+colW*0.41, H-4);
  });
  // POC trend line across sessions
  if (poc_pts.length > 1) {
    tpoCtx.strokeStyle='rgba(255,209,102,0.9)'; tpoCtx.lineWidth=1.4; tpoCtx.beginPath();
    poc_pts.forEach(function(pt,i){ if(i===0) tpoCtx.moveTo(pt.x,pt.y); else tpoCtx.lineTo(pt.x,pt.y); });
    tpoCtx.stroke();
  }
  tpoCtx.fillStyle='rgba(7,9,14,0.6)'; tpoCtx.fillRect(0,0,axisW,H);
  tpoCtx.fillStyle='#7a93ad'; tpoCtx.font='8px monospace'; tpoCtx.textAlign='left';
  for (var gi=0; gi<=6; gi++) { var gp=lo+(hi-lo)*(gi/6), gy=prToY(gp); tpoCtx.fillText(gp.toFixed(4), 2, Math.max(9,Math.min(H-3,gy))); }
}
function refreshTPO() {
  var n = document.getElementById('tpoN').value, tick = document.getElementById('tpoTick').value, va = document.getElementById('tpoVA').value;
  fetch('/api/session_profiles?n='+n+'&tick_size='+tick+'&va_pct='+va).then(function(r){return r.ok?r.json():null;}).then(function(d){
    if (!d) return; tpoSessions = d; tpoPanX = 0;
    resizeCvs(tpoCvs, tpoCtx); drawTPOChart();
    document.getElementById('tpoInfo').textContent = d.length + ' sessions loaded';
  }).catch(function(){});
}
var tpoWrap = document.getElementById('tpoWrap');
tpoWrap.addEventListener('mousedown', function(e){ tpoDragging=true; tpoDragStartX=e.clientX; tpoDragStartPan=tpoPanX; });
tpoWrap.addEventListener('mousemove', function(e){ if(!tpoDragging) return; tpoPanX = tpoDragStartPan - (e.clientX-tpoDragStartX); drawTPOChart(); });
tpoWrap.addEventListener('mouseup', function(){ tpoDragging=false; });
tpoWrap.addEventListener('mouseleave', function(){ tpoDragging=false; });
tpoWrap.addEventListener('touchstart', function(e){ tpoDragging=true; tpoDragStartX=e.touches[0].clientX; tpoDragStartPan=tpoPanX; }, {passive:true});
tpoWrap.addEventListener('touchmove', function(e){ if(!tpoDragging) return; tpoPanX = tpoDragStartPan - (e.touches[0].clientX-tpoDragStartX); drawTPOChart(); }, {passive:true});
tpoWrap.addEventListener('touchend', function(){ tpoDragging=false; });

// =====================================================================
//  FOOTPRINT - windowed paging across full tick history
// =====================================================================
var fpEnd = null, fpLoaded = false;
function fpReload() { fpEnd = Date.now(); fpLoadWindow(); }
function fpPage(dir) {
  var tf = parseInt(document.getElementById('fpTf').value);
  var bars = parseInt(document.getElementById('fpBars').value);
  if (fpEnd == null) fpEnd = Date.now();
  fpEnd = fpEnd + dir * bars * tf * 1000;
  fpEnd = Math.min(fpEnd, Date.now());
  fpLoadWindow();
}
function fpLoadWindow() {
  var tf = document.getElementById('fpTf').value, bars = document.getElementById('fpBars').value;
  var tick = document.getElementById('fpTick').value, imb = document.getElementById('fpImb').value;
  if (fpEnd == null) fpEnd = Date.now();
  var start = fpEnd - bars * tf * 1000;
  document.getElementById('fpScroll').innerHTML = '<div class="empty">Loading...</div>';
  fetch('/api/footprint_window?tf='+tf+'&start='+Math.floor(start)+'&end='+Math.ceil(fpEnd)+'&bars='+bars+'&tick_size='+tick+'&imb_ratio='+imb)
    .then(function(r){return r.ok?r.json():null;}).then(function(d){
      if (!d) return;
      fpLoaded = true;
      var span = d.data_span;
      document.getElementById('fpInfo').textContent = d.bars.length + ' bars' +
        (span && span.count ? (' - history available back to ' + new Date(span.min_ts).toISOString().slice(0,10)) : '');
      if (!d.bars.length) { document.getElementById('fpScroll').innerHTML = '<div class="empty">No trades in this window. Try paging Older, or wait for backfill.</div>'; return; }
      var cumDelta = 0;
      var html = d.bars.map(function(bar){
        cumDelta += bar.delta;
        var rows = bar.cells.slice().sort(function(a,b){ return parseFloat(b.price)-parseFloat(a.price); });
        var maxCell = Math.max.apply(null, rows.map(function(c){return Math.max(c.buy,c.sell);}).concat([0.0001]));
        var t = new Date(bar.ts).toLocaleString('en-GB',{hour12:false,month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'});
        var deltaCol = bar.delta>=0?'var(--bid)':'var(--ask)';
        var rowsHtml = rows.map(function(c){
          var isPoc = c.price === bar.poc;
          var sellBg = 'rgba(255,61,90,'+Math.min(0.55,c.sell/maxCell*0.55)+')';
          var buyBg = 'rgba(0,200,122,'+Math.min(0.55,c.buy/maxCell*0.55)+')';
          var cls = isPoc?'fp-poc':''; var stackCls = c.stacked?'fp-stacked':'';
          return '<div class="fp-priceline">'+c.price+'</div><div class="fp-row '+cls+' '+stackCls+'">'+
            '<div class="fp-cell fp-sell" style="background:'+sellBg+'">'+(c.sell||'')+'</div>'+
            '<div class="fp-cell fp-buy" style="background:'+buyBg+'">'+(c.buy||'')+'</div></div>';
        }).join('');
        return '<div class="fp-bar"><div class="fp-hdr">'+t+'<b>D'+(bar.delta>=0?'+':'')+bar.delta+'</b><span style="color:'+deltaCol+'">CD'+(cumDelta>=0?'+':'')+cumDelta.toFixed(2)+'</span></div>'+rowsHtml+'</div>';
      }).join('');
      document.getElementById('fpScroll').innerHTML = '<div style="white-space:nowrap">'+html+'</div>';
      document.getElementById('fpScroll').scrollLeft = 999999;
    }).catch(function(){});
}

// =====================================================================
//  PREDICTION
// =====================================================================
var predData = null;
function drawPred() {
  var d=predData, dpr=window.devicePixelRatio||1, W=predCvs.width/dpr, H=predCvs.height/dpr;
  predCtx.fillStyle='#07090e'; predCtx.fillRect(0,0,W,H);
  if (!d || !d.current) { predCtx.fillStyle='#3a4a5e'; predCtx.font='11px monospace'; predCtx.fillText('Computing Monte Carlo simulation...',12,24); return; }
  var span=Math.max(d.p95-d.p5,0.0002), minP=d.p5-span*0.1, maxP=d.p95+span*0.1;
  function prToY(p){ return H-((p-minP)/(maxP-minP))*H; }
  function hrToX(h){ return (h/d.hours)*W; }
  var cx=d.current, hr=d.hours;
  [{lo:d.p5,hi:d.p95,c:'rgba(20,50,130,0.35)'},{lo:d.p25,hi:d.p75,c:'rgba(30,90,200,0.45)'},
   {lo:d.p50-(d.p75-d.p25)*0.15,hi:d.p50+(d.p75-d.p25)*0.15,c:'rgba(60,140,240,0.50)'}].forEach(function(b){
    predCtx.beginPath(); predCtx.moveTo(hrToX(0),prToY(cx)); predCtx.lineTo(hrToX(hr),prToY(b.hi)); predCtx.lineTo(hrToX(hr),prToY(b.lo));
    predCtx.closePath(); predCtx.fillStyle=b.c; predCtx.fill();
  });
  predCtx.strokeStyle='#ffd166'; predCtx.lineWidth=2; predCtx.beginPath(); predCtx.moveTo(hrToX(0),prToY(cx)); predCtx.lineTo(hrToX(hr),prToY(d.p50)); predCtx.stroke();
  predCtx.strokeStyle='rgba(255,255,255,0.4)'; predCtx.lineWidth=1; predCtx.setLineDash([4,4]);
  predCtx.beginPath(); predCtx.moveTo(hrToX(0),prToY(cx)); predCtx.lineTo(W,prToY(cx)); predCtx.stroke(); predCtx.setLineDash([]);
  predCtx.fillStyle='#fff'; predCtx.beginPath(); predCtx.arc(hrToX(0),prToY(cx),4,0,Math.PI*2); predCtx.fill();
  predCtx.fillStyle='#ffd166'; predCtx.beginPath(); predCtx.arc(hrToX(hr),prToY(d.p50),4,0,Math.PI*2); predCtx.fill();
  predCtx.fillStyle='#5a7a9a'; predCtx.font='8px monospace'; predCtx.textAlign='left';
  [[d.p95,'95%'],[d.p75,'75%'],[d.p50,'50%'],[d.p25,'25%'],[d.p5,'5%']].forEach(function(pr2){
    var y=prToY(pr2[0]); predCtx.fillText(pr2[1]+' '+pr2[0].toFixed(5), hrToX(hr)+4, Math.max(10,Math.min(H-3,y)));
  });
  for (var i=0;i<=d.hours;i++){ predCtx.fillStyle='#3a4a5e'; predCtx.textAlign='center'; predCtx.fillText(i+'h', hrToX(i), H-4); }
  var bColor = d.bias==='BULLISH'?'#00c87a':'#ff3d5a';
  predCtx.fillStyle=bColor; predCtx.font='13px monospace'; predCtx.textAlign='left'; predCtx.fillText(d.bias+' - '+d.confidence+'% CONF',10,20);
}
function refreshPrediction() {
  if (activeTab!=='pred') return;
  fetch('/api/prediction').then(function(r){return r.ok?r.json():null;}).then(function(d){
    if (!d || !d.current) return; predData=d; resizeCvs(predCvs,predCtx); drawPred();
    var regCol=d.regime==='TRENDING'?'var(--bid)':(d.regime==='MEAN-REVERTING'?'var(--ask)':'var(--mag)');
    var biCol=d.bias==='BULLISH'?'var(--bid)':'var(--ask)';
    document.getElementById('predInfo').textContent = d.n_paths+' paths - '+d.hours+'h horizon';
    document.getElementById('predMetrics').innerHTML =
      '<div class="met-card"><div class="met-title">CONFIDENCE</div><div class="met-val" style="color:'+(d.confidence>=60?'var(--bid)':d.confidence>=35?'var(--mag)':'var(--ask)')+'">'+d.confidence+'%</div></div>'+
      '<div class="met-card"><div class="met-title">REGIME</div><div class="met-val" style="color:'+regCol+'">'+d.regime+'</div><div class="met-sub">H='+d.hurst+'</div></div>'+
      '<div class="met-card"><div class="met-title">5H BIAS</div><div class="met-val" style="color:'+biCol+'">'+d.bias+'</div><div class="met-sub">Median: '+d.p50+'</div></div>'+
      '<div class="met-card"><div class="met-title">RANGE</div><div class="met-sub" style="font-size:10px">95%: '+d.p5+' to '+d.p95+'</div></div>';
  }).catch(function(){});
}

// =====================================================================
//  REPORT
// =====================================================================
function refreshReport() {
  if (activeTab !== 'rep') return;
  fetch('/api/report').then(function(r){return r.ok?r.json():null;}).then(function(d){
    if (!d) return;
    var ts = new Date(d.timestamp).toLocaleString('en-GB',{timeZone:'UTC',hour12:false});
    document.getElementById('repTs').textContent = 'UTC '+ts;
    var s=d.session||{}, reg=d.regime||{}, pred=d.prediction||{}, sz=d.stop_zones||{}, vp=d.volume_profile;
    function mkRow(k,v){ return '<div class="rep-row"><span class="rep-k">'+k+'</span><span class="rep-v">'+(v==null?'-':v)+'</span></div>'; }
    var regCol = reg.type==='TRENDING'?'var(--bid)':(reg.type==='MEAN-REVERTING'?'var(--ask)':'var(--mag)');
    var html = '<div class="rep-section"><div class="rep-title">MARKET STATE</div>'+
      mkRow('Symbol','EURUSDT')+mkRow('Mid', d.mid!=null?d.mid.toFixed(5):null)+
      mkRow('Session Open', s.open!=null?s.open.toFixed(5):null)+mkRow('Session High', s.high!=null?s.high.toFixed(5):null)+
      mkRow('Session Low', s.low!=null?s.low.toFixed(5):null)+mkRow('VWAP', s.vwap!=null?s.vwap.toFixed(5):null)+
      mkRow('Book Depth', s.book_depth)+mkRow('Magnet (POC)', d.magnet?(d.magnet.price+' ('+d.magnet.pct+'%)'):null)+
      '</div><div class="rep-section"><div class="rep-title">MATHEMATICAL REGIME</div>'+
      mkRow('Regime', '<span style="color:'+regCol+'">'+(reg.type||'-')+'</span>')+mkRow('Hurst', reg.hurst)+
      mkRow('5h Bias', '<span style="color:'+(reg.bias==='BULLISH'?'var(--bid)':'var(--ask)')+'">'+(reg.bias||'-')+'</span>')+
      mkRow('Confidence', reg.confidence!=null?reg.confidence+'%':null)+
      '</div><div class="rep-section"><div class="rep-title">5-HOUR PREDICTION</div>'+
      (pred.p50?mkRow('Median',pred.p50)+mkRow('95% Range', pred.p5+' to '+pred.p95):'<div class="rep-row"><span class="rep-k">Status</span><span class="rep-v">Computing...</span></div>')+
      '</div><div class="rep-section"><div class="rep-title">VOLUME PROFILE (SESSION)</div>'+
      (vp?mkRow('POC',vp.poc)+mkRow('Value Area', vp.val+' to '+vp.vah):'<div class="rep-row"><span class="rep-k">Status</span><span class="rep-v">Building...</span></div>')+
      '</div><div class="rep-section"><div class="rep-title">STRUCTURAL RESTING ORDERS</div>'+
      (d.structural_levels&&d.structural_levels.length?d.structural_levels.slice(0,8).map(function(r2){return mkRow(r2.side+' '+r2.price, r2.status+' - '+r2.current_qty+' EUR');}).join(''):'<div class="rep-row"><span class="rep-k">Status</span><span class="rep-v">None yet</span></div>')+
      '</div><div class="rep-section"><div class="rep-title">STOP LOSS ZONES</div>'+
      (sz.atr_pips?mkRow('ATR', sz.atr_pips+' pips'):'')+
      (sz.sell_stops||[]).slice(0,3).map(function(z){return mkRow('SELL '+z.price, 'above '+z.swing);}).join('')+
      (sz.buy_stops||[]).slice(0,3).map(function(z){return mkRow('BUY '+z.price, 'below '+z.swing);}).join('')+
      '</div><div class="rep-section"><div class="rep-title">ANALYSIS</div><div class="rep-analysis">'+(d.analysis||'-')+'</div></div>';
    document.getElementById('repBody').innerHTML = html;
  }).catch(function(){});
}

// =====================================================================
//  BACKFILL STATUS / ORDER BOOK / CLUSTERS / ABSORPTION / STOP ZONES
// =====================================================================
function refreshBackfill() {
  fetch('/api/backfill_status').then(function(r){return r.ok?r.json():null;}).then(function(d){
    if (!d) return; var el=document.getElementById('backfillV');
    if (d.status==='live') { el.textContent=d.target_days+'d LIVE'; el.style.color='var(--bid)'; }
    else if (d.status==='backfilling') { el.textContent=d.synced_days+'/'+d.target_days+'d - '+d.pct+'%'; el.style.color='var(--mag)'; }
    else { el.textContent=d.status; el.style.color='var(--muted)'; }
  }).catch(function(){});
}

var REST_LBL = {RESTING:['R','rest-r'],PARTIAL:['P','rest-p'],MOSTLY_FILLED:['F','rest-p'],GONE:['X','']};
function rRowClass(r,isMag){ if(isMag) return 'mag-row'; if(!r) return ''; var e=REST_LBL[r.status]; return e?e[1]:''; }
function rFlag(r,isMag){ if(isMag) return '<span class="fl" style="color:var(--mag)">M</span>'; if(!r) return '<span class="fl" style="opacity:.1">.</span>'; var e=REST_LBL[r.status]||['.','']; return '<span class="fl">'+e[0]+'</span>'; }

function refreshBook() {
  fetch('/api/data').then(function(r){return r.ok?r.json():null;}).then(function(d){
    if (!d) return;
    document.getElementById('legAge').textContent = d.min_structural_age_min;
    var dot=document.getElementById('hdot'); dot.className = d.book_ready?'hdot live':'hdot';
    document.getElementById('connLbl').textContent = !d.connected?'Reconnecting...':(d.book_ready?'Binance WS Live':'Syncing book...');
    document.getElementById('depV').textContent = d.bid_levels+'/'+d.ask_levels;
    document.getElementById('sessO').textContent = d.session_open!=null?d.session_open.toFixed(5):'-';
    var rng=(d.session_low!=null&&d.session_high!=null)?(d.session_low.toFixed(4)+' to '+d.session_high.toFixed(4)):'-';
    document.getElementById('sessR').textContent = rng;
    document.getElementById('vwapV').textContent = d.vwap!=null?d.vwap.toFixed(5):'-';
    document.getElementById('stO').textContent = document.getElementById('sessO').textContent;
    document.getElementById('stR').textContent = rng;
    document.getElementById('stV').textContent = document.getElementById('vwapV').textContent;
    if (!d.bids.length && !d.asks.length) return;
    var bb=parseFloat(d.bids[0]?d.bids[0].price:0), ba=parseFloat(d.asks[0]?d.asks[0].price:0);
    var mid=(bb+ba)/2, sp=ba-bb;
    var mEl=document.getElementById('midP'); mEl.textContent=mid.toFixed(5);
    if (prevMid!==null) mEl.style.color = mid>=prevMid?'var(--bid)':'var(--ask)';
    prevMid=mid;
    document.getElementById('sBid').textContent=bb.toFixed(5);
    document.getElementById('sAsk').textContent=ba.toFixed(5);
    document.getElementById('sSpr').textContent=sp.toFixed(5);
    document.getElementById('seqLbl').textContent='seq '+d.ts;
    var tv=d.buy_vol+d.sell_vol;
    if (tv>0) document.getElementById('flowB').style.width=(d.buy_vol/tv*100).toFixed(1)+'%';
    if (d.magnet) {
      var mz=d.magnet;
      document.getElementById('magV').innerHTML = mz.price+' <span style="font-size:7px;opacity:.7">('+mz.pct+'%)</span>';
      document.getElementById('stM').textContent = mz.price+' ('+mz.pct+'%)';
      if (mz.confluence) { document.getElementById('confRow').style.display='flex'; document.getElementById('stC').textContent = mz.confluence.side+' @ '+mz.confluence.price; }
      else document.getElementById('confRow').style.display='none';
    }
    var maxP2=Math.max.apply(null, d.profile_top.map(function(p){return p.pct;}).concat([1]));
    document.getElementById('profBars').innerHTML = d.profile_top.map(function(p){
      return '<div class="pbar-row"><span class="pbar-price">'+p.price+'</span><div class="pbar-track"><div class="pbar-fill" style="width:'+(p.pct/maxP2*100).toFixed(0)+'%"></div></div><span class="pbar-pct">'+p.pct+'%</span></div>';
    }).join('');
    var allC=d.bids.concat(d.asks).map(function(x){return parseFloat(x.cumul);});
    var maxC=Math.max.apply(null, allC.concat([1]));
    document.getElementById('asks').innerHTML = d.asks.slice().reverse().map(function(row){
      var pct=(parseFloat(row.cumul)/maxC*100).toFixed(1);
      return '<div class="brow ask-row '+rRowClass(row.resting,row.is_magnet)+'"><div class="dbar" style="width:'+pct+'%"></div><span class="pr">'+row.price+'</span><span class="qy">'+row.qty+'</span><span class="cu">'+row.cumul+'</span>'+rFlag(row.resting,row.is_magnet)+'</div>';
    }).join('');
    document.getElementById('bids').innerHTML = d.bids.map(function(row){
      var pct=(parseFloat(row.cumul)/maxC*100).toFixed(1);
      return '<div class="brow bid-row '+rRowClass(row.resting,row.is_magnet)+'"><div class="dbar" style="width:'+pct+'%"></div><span class="pr">'+row.price+'</span><span class="qy">'+row.qty+'</span><span class="cu">'+row.cumul+'</span>'+rFlag(row.resting,row.is_magnet)+'</div>';
    }).join('');
    document.getElementById('restCnt').textContent = d.resting.length+' tracked';
    document.getElementById('restList').innerHTML = d.resting.length===0 ? '<div class="empty">No structural resting orders yet.</div>' :
      d.resting.map(function(rr){
        var sc=rr.side==='BID'?'bid-ice':'ask-ice';
        var ag=rr.age_s>3600?(Math.floor(rr.age_s/3600)+'h'+Math.floor((rr.age_s%3600)/60)+'m'):rr.age_s>60?(Math.floor(rr.age_s/60)+'m'+(rr.age_s%60)+'s'):(rr.age_s+'s');
        var badge=rr.structural?'':'<span class="ibadge">BUILDING</span>';
        return '<div class="icard '+sc+'"><div><div style="display:flex;align-items:center"><span class="iprice">'+rr.price+'</span>'+badge+'<span style="font-size:7.5px;color:var(--muted);margin-left:8px">'+rr.side+' - '+ag+'</span></div><div class="imeta">'+rr.status+' - peak '+rr.peak_qty+' to now '+rr.current_qty+' EUR ('+rr.fill_pct+'%)</div></div><div class="iscore" style="color:var(--wall)">'+rr.current_qty+'</div></div>';
      }).join('');
    if (d.events.length !== prevEvLen) {
      prevEvLen = d.events.length; document.getElementById('evCnt').textContent = ''+d.events.length;
      var TL={NEW_RESTING:['NEW','t-NEW'],PARTIAL_FILL:['FILL','t-PART'],CLEARED:['CLEARED','t-CLR'],MAGNET_SHIFT:['MAGNET','t-MAG']};
      document.getElementById('evLog').innerHTML = d.events.map(function(ev){
        var pair=TL[ev.type]||[ev.type,'t-NEW'], lbl=pair[0], cls=pair[1];
        var t2=new Date(ev.ts*1000).toLocaleTimeString('en-GB',{hour12:false});
        var pc=ev.side==='BID'?'var(--bid)':(ev.side==='ASK'?'var(--ask)':'var(--muted)');
        return '<div class="erow"><span class="etag '+cls+'">'+lbl+'</span><div><div><span class="epr" style="color:'+pc+'">'+ev.price+'</span><span style="font-size:8px;color:var(--muted);margin-left:6px">'+(ev.side||'')+'</span></div><div class="edet">'+ev.detail+'</div></div><div class="ets">'+t2+'</div></div>';
      }).join('') || '<div class="empty">Monitoring.</div>';
    }
  }).catch(function(){});
}

function refreshClusters() {
  if (activeTab!=='clus') return;
  fetch('/api/clusters').then(function(r){return r.ok?r.json():null;}).then(function(d){
    if (!d) return;
    document.getElementById('clusCnt').textContent = d.length+' clusters';
    var bids=d.filter(function(c){return c.side==='BID';}).sort(function(a,b){return b.avg_qty-a.avg_qty;});
    var asks=d.filter(function(c){return c.side==='ASK';}).sort(function(a,b){return b.avg_qty-a.avg_qty;});
    var maxQ=Math.max.apply(null, d.map(function(c){return c.avg_qty*c.persist;}).concat([0.001]));
    function renderSide(items,side){
      if (items.length===0) return '<div class="empty">Building...</div>';
      return items.map(function(c,i){
        var str=c.avg_qty*c.persist, pct=(str/maxQ*100).toFixed(1), isMag=i===0&&c.persist>0.6;
        var pCol=side==='BID'?'var(--bid)':'var(--ask)';
        return '<div style="padding:5px 10px;border-bottom:1px solid var(--bdr);display:flex;align-items:center;gap:8px">'+
          '<span style="width:64px;font-weight:700;font-size:10px;color:'+pCol+'">'+c.price+'</span>'+
          '<div style="flex:1;height:8px;background:var(--bdr);border-radius:4px;overflow:hidden"><div style="height:100%;width:'+pct+'%;background:'+(side==='BID'?'linear-gradient(90deg,#1a4a9a,#3ab0ff)':'linear-gradient(90deg,#9a1a2a,#ff5060)')+';border-radius:4px"></div></div>'+
          '<span style="width:50px;text-align:right;font-size:9px;color:var(--muted)">'+c.avg_qty+' EUR'+(isMag?'<span style="font-size:7px;padding:1px 4px;background:rgba(255,209,102,.2);color:var(--mag);border-radius:2px;margin-left:4px">*</span>':'')+'</span></div>';
      }).join('');
    }
    document.getElementById('clusBid').innerHTML = renderSide(bids,'BID');
    document.getElementById('clusAsk').innerHTML = renderSide(asks,'ASK');
  }).catch(function(){});
}

function refreshAbsorption() {
  if (activeTab!=='abso') return;
  fetch('/api/absorption').then(function(r){return r.ok?r.json():null;}).then(function(d){
    if (!d) return;
    document.getElementById('absCnt').textContent = d.length+' zones';
    document.getElementById('absList').innerHTML = d.length===0 ? '<div class="empty">No absorption zones yet.</div>' :
      d.map(function(z){
        return '<div style="padding:6px 10px;border-bottom:1px solid var(--bdr);display:grid;grid-template-columns:70px 1fr 60px;gap:8px;align-items:center">'+
        '<div><span style="font-weight:700;color:#c0a0ff">'+z.price+'</span><div style="font-size:7.5px;color:var(--muted);margin-top:2px">'+z.count+' trades - '+z.vol+' EUR</div></div>'+
        '<div style="height:8px;background:var(--bdr);border-radius:4px;overflow:hidden"><div style="height:100%;width:'+z.score+'%;background:linear-gradient(90deg,#6030c0,#e080ff);border-radius:4px"></div></div>'+
        '<span style="font-size:12px;font-weight:700;color:#e080ff;text-align:right">'+z.score+'</span></div>';
      }).join('');
  }).catch(function(){});
}

function refreshStopZones() {
  if (activeTab!=='stop') return;
  fetch('/api/stopzones').then(function(r){return r.ok?r.json():null;}).then(function(d){
    if (!d) return;
    document.getElementById('atrLbl').textContent = 'ATR '+(d.atr_pips||'-')+' pips';
    var html='';
    (d.sell_stops||[]).forEach(function(z){
      html += '<div style="padding:5px 10px;border-bottom:1px solid var(--bdr);display:grid;grid-template-columns:auto 1fr auto;gap:8px;align-items:center">'+
        '<span style="color:var(--ask);font-weight:700;font-size:10px">UP '+z.price+'</span>'+
        '<div style="height:6px;background:var(--bdr);border-radius:3px;overflow:hidden"><div style="height:100%;width:'+(z.density*100).toFixed(0)+'%;background:rgba(255,61,90,.7);border-radius:3px"></div></div>'+
        '<span style="font-size:7.5px;color:var(--muted)">above '+z.swing+'</span></div>';
    });
    if ((d.sell_stops||[]).length && (d.buy_stops||[]).length) html += '<div style="text-align:center;padding:5px;background:rgba(255,255,255,.03);font-size:8px;color:var(--muted);border-top:1px solid var(--bdr);border-bottom:1px solid var(--bdr)">MID PRICE</div>';
    (d.buy_stops||[]).forEach(function(z){
      html += '<div style="padding:5px 10px;border-bottom:1px solid var(--bdr);display:grid;grid-template-columns:auto 1fr auto;gap:8px;align-items:center">'+
        '<span style="color:var(--bid);font-weight:700;font-size:10px">DN '+z.price+'</span>'+
        '<div style="height:6px;background:var(--bdr);border-radius:3px;overflow:hidden"><div style="height:100%;width:'+(z.density*100).toFixed(0)+'%;background:rgba(0,200,122,.7);border-radius:3px"></div></div>'+
        '<span style="font-size:7.5px;color:var(--muted)">below '+z.swing+'</span></div>';
    });
    document.getElementById('stopList').innerHTML = html || '<div class="empty">Detecting swings...</div>';
  }).catch(function(){});
}

tab('chart');
refreshBook();
setInterval(refreshBook, 1000);
setInterval(refreshHeat, 1000);
setInterval(refreshClusters, 5000);
setInterval(refreshAbsorption, 5000);
setInterval(refreshStopZones, 10000);
setInterval(refreshPrediction, 30000);
setInterval(refreshReport, 30000);
setInterval(refreshBackfill, 5000);
setInterval(function(){ if (activeTab==='chart' && chartViewEnd && Date.now()-chartViewEnd < 5*chartTf*1000) chartFetch(); }, 5000);
refreshHeat(); refreshClusters(); refreshAbsorption(); refreshStopZones();
refreshPrediction(); refreshReport(); refreshBackfill();
</script>
</body>
</html>"""


# ============================================================
#  ENTRY POINT
# ============================================================
if __name__ == "__main__":
    print("EURUSDT Pro Quant Suite")
    print("Open: http://127.0.0.1:" + str(PORT))
    print("Tick DB: " + DB_PATH)
    print("11 tabs: Chart, Book, Heatmap, VolProfile, TPO, Footprint, Clusters, Absorption, StopZones, Prediction, Report")

    _c = db_conn(); init_schema(_c); load_poc_history(_c); _c.close()
    threading.Thread(target=run_ws, daemon=True).start()
    threading.Thread(target=backfill_loop, daemon=True).start()
    threading.Thread(target=tick_flush_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
