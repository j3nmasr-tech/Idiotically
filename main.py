#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Production-ready Premium SMC Scanner (Set B architecture)
- Full SMC core: OB/FVG/Liquidity/BOS/MSS detection
- ATR-based TP/SL
- SL-cluster deprioritization
- Momentum & volatility filters
- Top N USDT coins, multi-timeframe scan
"""

import os, time, asyncio, logging, datetime
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from collections import defaultdict, deque

# ---------------- ENV ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")  # optional
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))
TOP_N = int(os.getenv("TOP_N", 1))
MIN_VOLUME = float(os.getenv("MIN_VOLUME", 1000000))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", 0.002))
BTC_PAIR = os.getenv("BTC_PAIR", "BTC-USDT-SWAP")
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 3600))
DAILY_SUMMARY_HOUR = int(os.getenv("DAILY_SUMMARY_HOUR", 23))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m", "1h", "4h"]

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("smc_bot")
db_lock = asyncio.Lock()

# ---------------- TELEGRAM ----------------
def escape_html(msg: str) -> str:
    if not msg:
        return "-"
    return str(msg).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

async def tg(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram creds missing.")
        return
    if not msg:
        log.error("Empty Telegram message")
        return
    safe_msg = escape_html(msg)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": safe_msg, "parse_mode":"HTML"})
            if r.status_code != 200:
                log.error(f"Telegram send failed: {r.status_code} | {r.text}")
        except Exception as e:
            log.error(f"Telegram send exception: {e}")

# ---------------- SQLITE ----------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            side TEXT,
            entry REAL,
            sl REAL,
            tp1 REAL,
            tp2 REAL,
            tp3 REAL,
            timestamp TEXT,
            status TEXT,
            reason TEXT,
            score INTEGER,
            tp1_hit INTEGER DEFAULT 0,
            tp2_hit INTEGER DEFAULT 0,
            tp3_hit INTEGER DEFAULT 0
        );
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS pauses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reason TEXT,
            timestamp TEXT
        );
        """)
        await db.commit()

# ---------------- OHLCV ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.warning(f"OHLCV fetch failed for {symbol} {timeframe}: {e}")
        return None

# ---------------- INDICATORS ----------------
def atr(df: pd.DataFrame, period=14):
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.DataFrame({
        "h-l": high - low,
        "h-pc": (high - close.shift(1)).abs(),
        "l-pc": (low - close.shift(1)).abs()
    }).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()

def sma(series: pd.Series, period: int):
    return series.rolling(period, min_periods=1).mean()

# ---------------- SMC CORE ----------------
def detect_swing_points(df: pd.DataFrame):
    if len(df) < 5:
        return None
    last = df.iloc[-1]
    prev = df.iloc[-3:-1]
    swing_high = last["high"] > prev["high"].max()
    swing_low = last["low"] < prev["low"].min()
    return swing_high, swing_low

def detect_active_range(df: pd.DataFrame, lookback=10):
    last = df.iloc[-lookback:]
    return last["high"].max(), last["low"].min()

def detect_liquidity_pools(df: pd.DataFrame):
    hh, ll = detect_swing_points(df)
    return hh, ll

def detect_sweep(df: pd.DataFrame):
    if len(df) < 6:
        return False, False
    last = df.iloc[-1]
    prev = df.iloc[-5:-1]
    return last["high"] > prev["high"].max(), last["low"] < prev["low"].min()

def detect_bos_mss(df: pd.DataFrame):
    hh, ll = detect_sweep(df)
    return hh, ll

def detect_fvg(df: pd.DataFrame):
    if len(df) < 3:
        return False, False
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    bull = c2["low"] > c1["high"] and c3["low"] > c2["high"]
    bear = c2["high"] < c1["low"] and c3["high"] < c2["low"]
    return bull, bear

def detect_order_blocks(df: pd.DataFrame):
    if len(df) < 3:
        return None, None, None
    candle = df.iloc[-3]
    if candle["close"] > candle["open"]:
        return "bullish", candle["open"], candle["low"]
    return "bearish", candle["high"], candle["open"]

# ---------------- SL-CLUSTER ----------------
recent_sl = defaultdict(lambda: deque())
def record_sl_hit(symbol: str, lookback_minutes=30):
    now = time.time()
    dq = recent_sl[symbol]
    dq.append(now)
    cutoff = now - lookback_minutes * 60
    while dq and dq[0] < cutoff:
        dq.popleft()
def deprioritized(symbol: str, threshold=3, lookback=30):
    dq = recent_sl[symbol]
    now = time.time()
    cutoff = now - lookback * 60
    while dq and dq[0] < cutoff:
        dq.popleft()
    return len(dq) >= threshold

# ---------------- FILTER HELPERS ----------------
def against_higher_tf(df_low, df_high):
    if df_high is None or len(df_high) < 2:
        return False
    last_low = df_low["close"].iloc[-1]
    last_high = df_high["close"].iloc[-1]
    prev_high = df_high["close"].iloc[-2]
    high_trend_up = last_high > prev_high
    low_trend_up = last_low > df_low["close"].iloc[-2]
    if (low_trend_up and not high_trend_up) or (not low_trend_up and high_trend_up):
        return True
    return False

def weak_bos_sweep(bos_hh, bos_ll, sweep_h, sweep_l):
    if not (bos_hh or bos_ll):
        return True
    if not (sweep_h or sweep_l):
        return True
    return False

def mid_range_entry(df):
    last = df.iloc[-1]
    candle_mid = (last["high"] + last["low"]) / 2
    if last["close"] < candle_mid*1.02 and last["close"] > candle_mid*0.98:
        return True
    return False

def choppy_market(df, lookback=10):
    if len(df) < lookback:
        return False
    recent = df.iloc[-lookback:]
    avg_range = (recent["high"] - recent["low"]).mean()
    avg_close = recent["close"].mean()
    if avg_range / avg_close < 0.003:
        return True
    return False

# ---------------- FIXED SIGNAL GENERATOR ----------------
def generate_signal(df: pd.DataFrame, symbol: str, context=None):
    if context is None:
        context = {}
    tf = context.get("tf","15m")
    if df is None or len(df) < 6:
        return None
    last = df["close"].iloc[-1]
    ob_type, ob_hi, ob_lo = detect_order_blocks(df)
    if ob_type is None:
        return None
    bull_fvg, bear_fvg = detect_fvg(df)
    sweep_h, sweep_l = detect_sweep(df)
    bos_hh, bos_ll = detect_bos_mss(df)
    side = "BUY" if ob_type=="bullish" else "SELL"
    df_15m = context.get("df_15m")
    if against_higher_tf(df, df_15m): return None
    if weak_bos_sweep(bos_hh, bos_ll, sweep_h, sweep_l): return None
    if mid_range_entry(df): return None
    if choppy_market(df): return None
    score = 0
    reasons = []
    if ob_type=="bullish": score+=2; reasons.append("OB Bull +2")
    else: score+=2; reasons.append("OB Bear +2")
    if bull_fvg: score+=2; reasons.append("FVG Bull +2")
    elif bear_fvg: score+=2; reasons.append("FVG Bear +2")
    score+=2; reasons.append("BOS +2")
    if sweep_h or sweep_l: score+=1; reasons.append("Sweep +1")
    else: reasons.append("No Sweep +0")
    atr_val = None
    if df_15m is not None and len(df_15m)>=10:
        atr_val = float(atr(df_15m,14).iloc[-1])
    entry = float(last)
    tp_mult, sl_mult = 0.8, 1.0
    if atr_val:
        if side=="BUY":
            sl = entry - sl_mult*atr_val
            tp1 = entry + tp_mult*atr_val
            tp2 = entry + tp_mult*1.5*atr_val
            tp3 = entry + tp_mult*2.5*atr_val
        else:
            sl = entry + sl_mult*atr_val
            tp1 = entry - tp_mult*atr_val
            tp2 = entry - tp_mult*1.5*atr_val
            tp3 = entry - tp_mult*2.5*atr_val
    else:
        if side=="BUY":
            sl = float(ob_lo)
            tp1 = entry*1.004; tp2 = entry*1.008; tp3 = entry*1.012
        else:
            sl = float(ob_hi)
            tp1 = entry*0.996; tp2 = entry*0.992; tp3 = entry*0.988
    if sl==entry:
        sl = entry - entry*0.002 if side=="BUY" else entry + entry*0.002
    return {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "score": score,
        "reason": "Set B SMC Signal",
        "reason_list": reasons
    }

# ---------------- LOG SIGNAL ----------------
async def log_signal(sig):
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (sig["symbol"],sig["side"],sig["entry"],sig["sl"],sig["tp1"],sig["tp2"],sig["tp3"],
                  datetime.datetime.utcnow().isoformat(),"OPEN",sig["reason"],sig["score"]))
            await db.commit()

# ---------------- MONITOR ----------------
async def monitor_signals(exchange):
    while True:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("""
                    SELECT id,symbol,side,entry,sl,tp1,tp2,tp3,tp1_hit,tp2_hit,tp3_hit,status 
                    FROM signals WHERE status='OPEN'
                """) as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status = row
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get("last")
                        if last_price is None: continue
                        hits=[]; sl_hit=False
                        if side=="BUY":
                            if not tp1_hit and last_price>=tp1: hits.append("TP1"); tp1_hit=1
                            if not tp2_hit and last_price>=tp2: hits.append("TP2"); tp2_hit=1
                            if not tp3_hit and last_price>=tp3: hits.append("TP3"); tp3_hit=1
                            if last_price<=sl: hits.append("SL"); status="CLOSED"; sl_hit=True
                        else:
                            if not tp1_hit and last_price<=tp1: hits.append("TP1"); tp1_hit=1
                            if not tp2_hit and last_price<=tp2: hits.append("TP2"); tp2_hit=1
                            if not tp3_hit and last_price<=tp3: hits.append("TP3"); tp3_hit=1
                            if last_price>=sl: hits.append("SL"); status="CLOSED"; sl_hit=True
                        if hits:
                            await tg(f"🎯 {symbol} {side} update\nEntry:{entry}\nLast:{last_price}\nHits:{','.join(hits)}\nSL:{sl}\nTP1:{tp1} TP2:{tp2} TP3:{tp3}")
                        if sl_hit:
                            record_sl_hit(symbol)
                        async with db_lock:
                            await db.execute("""
                                UPDATE signals SET tp1_hit=?,tp2_hit=?,tp3_hit=?,status=? WHERE id=?
                            """,(tp1_hit,tp2_hit,tp3_hit,status,sig_id))
                await db.commit()
        except Exception as e:
            log.exception("monitor error: %s", e)
            await tg(f"❌ Monitor error: {e}")
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- SCAN LOOP ----------------
# ---------------- SAFE FETCH HELPERS & SCAN LOOP (freeze-proof) ----------------
# concurrency semaphore to avoid blasting the exchange
OHLCV_SEMAPHORE = asyncio.Semaphore(8)  # tune between 4-16 depending on environment

async def safe_fetch_tickers(exchange, timeout=8):
    """Fetch tickers with timeout and return dict (or {})"""
    try:
        return await asyncio.wait_for(exchange.fetch_tickers(), timeout=timeout)
    except Exception as e:
        log.error(f"safe_fetch_tickers error/timeout: {e}")
        return {}

async def fetch_ohlcv_with_sem(exchange, symbol, tf, limit=200):
    """Internal wrapper that runs fetch_ohlcv under a semaphore to limit concurrency."""
    async with OHLCV_SEMAPHORE:
        return await fetch_ohlcv(exchange, symbol, tf, limit)

async def safe_fetch_ohlcv(exchange, symbol, tf, limit=200, timeout=7, retry=True):
    """
    Protect fetch_ohlcv with timeout and an optional retry.
    Returns None on failure.
    """
    try:
        return await asyncio.wait_for(fetch_ohlcv_with_sem(exchange, symbol, tf, limit), timeout=timeout)
    except Exception as e:
        log.error(f"{symbol} {tf} OHLCV timeout/error: {e}")
        if retry:
            # quick short retry with smaller limit
            try:
                return await asyncio.wait_for(fetch_ohlcv_with_sem(exchange, symbol, tf, max(50, limit//4)), timeout=5)
            except Exception as e2:
                log.error(f"{symbol} {tf} OHLCV retry failed: {e2}")
        return None

# Cached ticker fetch control
_last_ticker_fetch = 0.0
_cached_tickers = {}

async def get_top_tickers(exchange, cooldown=30):
    """
    Returns a dict of tickers (cached for `cooldown` seconds).
    Only returns USDT pairs. Never blocks forever thanks to safe_fetch_tickers.
    """
    global _last_ticker_fetch, _cached_tickers
    now = time.time()
    if _cached_tickers and (now - _last_ticker_fetch) < cooldown:
        return _cached_tickers
    tickers = await safe_fetch_tickers(exchange)
    if not tickers:
        # keep previous cached if empty to avoid chopping to nothing
        return _cached_tickers or {}
    # keep only USDT pairs early to reduce memory
    tickers = {s: v for s, v in tickers.items() if s.endswith("USDT")}
    _cached_tickers = tickers
    _last_ticker_fetch = now
    return _cached_tickers

# Main scan loop (drop-in replacement)
last_signal_time = {}
async def scan_loop(exchange):
    """
    Freeze-proof scan loop:
    - fetches top tickers (cached)
    - for each symbol fetches ALL TIMEFRAMES in parallel (once)
    - uses safe_fetch_ohlcv (timeout + retry)
    - reuses parent TFs for all small TF checks
    """
    global last_signal_time
    while True:
        loop_start = time.time()
        try:
            tickers = await get_top_tickers(exchange, cooldown=30)
            if not tickers:
                log.warning("No tickers available this cycle.")
                await asyncio.sleep(max(1, SCAN_INTERVAL))
                continue

            # build top by liquidity
            top = sorted(
                [(s, v.get("quoteVolume", 0) or 0) for s, v in tickers.items()],
                key=lambda x: x[1],
                reverse=True
            )[:TOP_N]

            for symbol, _vol in top:
                # deprioritization (recent SL cluster) check
                if deprioritized(symbol):
                    continue

                # Prepare tasks: fetch all TFs for this symbol in parallel, but safely
                tasks = {tf: asyncio.create_task(safe_fetch_ohlcv(exchange, symbol, tf, 200)) for tf in TIMEFRAMES}

                # Await all tasks and build ohlcv DataFrames cache
                ohlcvs = {}
                for tf, task in tasks.items():
                    data = await task
                    if not data:
                        continue
                    df = pd.DataFrame(data, columns=["ts", "open", "high", "low", "close", "vol"])
                    for c in ["open", "high", "low", "close", "vol"]:
                        df[c] = pd.to_numeric(df[c], errors="coerce")
                    ohlcvs[tf] = df

                # Ensure we have at least parent TFs that filters rely on
                # if you prefer strictness, require 15m and 1h/4h
                if "15m" not in ohlcvs:
                    # skip symbol - parent TF missing
                    continue

                # Iterate TFs and generate signals using cached parent data
                for tf in TIMEFRAMES:
                    key = f"{symbol}:{tf}"

                    # cooldown per (symbol,tf)
                    if key in last_signal_time and time.time() - last_signal_time[key] < 1800:
                        continue

                    df = ohlcvs.get(tf)
                    if df is None or len(df) < 6:
                        continue

                    # Build context using cached parents
                    context = {
                        "tf": tf,
                        "df_15m": ohlcvs.get("15m"),
                        "df_1h": ohlcvs.get("1h"),
                        "df_4h": ohlcvs.get("4h"),
                    }

                    # Generate signal (unchanged)
                    try:
                        sig = generate_signal(df, symbol, context)
                    except Exception as e:
                        log.exception("generate_signal EXCEPTION for %s %s: %s", symbol, tf, e)
                        sig = None

                    if sig:
                        # send + log
                        await tg(
                            f"🚀 {sig['symbol']} ({tf}) {sig['side']}\n"
                            f"Entry:{sig['entry']}\nSL:{sig['sl']}\n"
                            f"TP1:{sig['tp1']} TP2:{sig['tp2']} TP3:{sig['tp3']}\n"
                            f"Score:{sig['score']}\n"
                            f"Breakdown:{', '.join(sig['reason_list'])}"
                        )
                        await log_signal(sig)
                        last_signal_time[key] = time.time()

        except Exception as e:
            log.exception("scan loop fatal error: %s", e)
            # notify but keep loop running
            try:
                await tg(f"❌ Scan loop error: {e}")
            except Exception:
                pass

        # maintain a steady SCAN_INTERVAL cadence
        elapsed = time.time() - loop_start
        await asyncio.sleep(max(1, SCAN_INTERVAL - elapsed))
        
# ---------------- FASTAPI ----------------
app = FastAPI()
@app.post("/webhook")
async def webhook(request: Request):
    token = request.headers.get("X-Auth","")
    if token!=WEBHOOK_SECRET: raise HTTPException(403,"Invalid secret")
    data = await request.json()
    log.info("Webhook received: %s", data)
    return {"ok":True}

# ---------------- MAIN ----------------
async def main():
    await init_db()
    exchange = ccxt.okx({"enableRateLimit": True})
    await asyncio.gather(
        scan_loop(exchange),
        monitor_signals(exchange)
    )

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--http", action="store_true")
    args = p.parse_args()
    if args.http:
        # Run as FastAPI HTTP server
        uvicorn.run(app, host="0.0.0.0", port=9000)
    else:
        # Run as standalone scanner bot
        asyncio.run(main())