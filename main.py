#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER (Enhanced + Elite Features)
- Fully live early signals
- RomeOPT 6-step logic (upgraded with BOS/CHOCH, FVG, quality OB, volume checks)
- TP/SL tracking with ATR or OB
- Dynamic TP/SL updates (market-structure-based)
- Telegram alerts
- Async SQLite logging (with detailed JSON diagnostics)
- Filters: Score >=5, Displacement +2, Sweep+2 OR Zone+1, avoid counter-trend
- Improved Order Block detection
- Adaptive Market Regime detection
- HTF + Sweep scoring threshold
- Elite multi-timeframe confirmation (15m,1h,4h)
"""

import os
import time
import asyncio
import logging
import datetime
import json
import math
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from collections import defaultdict, deque

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))
TOP_N = int(os.getenv("TOP_N", 60))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
MIN_SCORE = 5
CRITICAL_FACTORS_MIN = 2  # HTF Alignment + Liquidity Sweep minimum

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_bot")
db_lock = asyncio.Lock()
db_conn = None

# ---------------- TELEGRAM ----------------
def escape_html(msg: str) -> str:
    if not msg: return "-"
    return str(msg).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

async def tg(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    safe_msg = escape_html(msg)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": safe_msg, "parse_mode":"HTML"})
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

# ---------------- DATABASE (patched: adds details column) ----------------
async def init_db():
    global db_conn
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute("PRAGMA journal_mode=WAL;")
    await db_conn.execute("PRAGMA synchronous=NORMAL;")
    await db_conn.execute("""
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
            tp3_hit INTEGER DEFAULT 0,
            latest_ob TEXT,
            details TEXT
        );
    """)
    await db_conn.commit()

# ---------------- OHLCV ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.debug("fetch_ohlcv failed for %s %s: %s", symbol, timeframe, e)
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

# ---------------- MARKET REGIME ----------------
async def detect_market_regime(df: pd.DataFrame):
    ma_htf = df["close"].rolling(50).mean().iloc[-1]
    price = df["close"].iloc[-1]
    recent_high = df["high"].iloc[-20:].max()
    recent_low = df["low"].iloc[-20:].min()
    range_pct = (recent_high - recent_low) / max(1e-8, recent_low)
    if price > ma_htf and range_pct > 0.02:
        return "BULL"
    elif price < ma_htf and range_pct > 0.02:
        return "BEAR"
    else:
        return "RANGE"

# ---------------- MULTI-TIMEFRAME ELITE CONFIRM ----------------
async def elite_tf_alignment(exchange, symbol: str, side: str):
    tfs = ["15m","1h","4h"]
    for tf in tfs:
        ohlcv = await fetch_ohlcv(exchange, symbol, tf, 50)
        if not ohlcv: return False
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
        trend = df["close"].iloc[-1] - df["close"].iloc[-5]
        trend_side = "BUY" if trend>0 else "SELL"
        if trend_side != side:
            return False
    return True

# ---------------- ROMEOPT 6-STEP SIGNAL (patched & extended with helpers) ----------------

# ---------- Helpers: BOS / CHOCH detection ----------
def detect_bos_choch(df: pd.DataFrame, swing_lookback=20):
    """
    Detect simple BOS and CHOCH.
    Returns: dict {
      has_bos: bool,
      bos_side: "BUY"/"SELL"/None,
      has_choch: bool,
      choch_info: {...}
    }
    """
    res = {"has_bos": False, "bos_side": None, "has_choch": False, "choch_info": None}

    if len(df) < 10:
        return res

    # Swing highs / lows over tail window
    tail = df.tail(swing_lookback)
    recent_high = tail['high'].max()
    recent_low = tail['low'].min()
    last_close = df['close'].iloc[-1]
    # momentum condition: last close vs close 3 bars ago
    prev3_close = df['close'].iloc[-4] if len(df) >= 4 else df['close'].iloc[-1]

    # BOS detection
    if last_close > recent_high and last_close > prev3_close:
        res["has_bos"] = True; res["bos_side"] = "BUY"
    elif last_close < recent_low and last_close < prev3_close:
        res["has_bos"] = True; res["bos_side"] = "SELL"

    # CHOCH via EMA crossover on tail
    ema20 = df['close'].ewm(span=20).mean()
    ema50 = df['close'].ewm(span=50).mean()
    # crossover in last 5 bars
    try:
        ema_now = ema20.iloc[-1] - ema50.iloc[-1]
        ema_prev = ema20.iloc[-6] - ema50.iloc[-6]
        has_choch = (ema_now > 0 and ema_prev < 0) or (ema_now < 0 and ema_prev > 0)
        res["has_choch"] = bool(has_choch)
        res["choch_info"] = {"ema20": float(ema20.iloc[-1]), "ema50": float(ema50.iloc[-1])}
    except Exception:
        res["has_choch"] = False

    return res

# ---------- Helpers: volume spike ----------
def vol_spike(df: pd.DataFrame, idx=-1, factor=1.5, lookback=20):
    if len(df) < 5:
        return False
    vol_avg = df['vol'].tail(lookback).mean()
    try:
        return float(df['vol'].iloc[idx]) > vol_avg * factor
    except Exception:
        return False

# ---------- Helpers: find fvgs ----------
def find_fvgs(df: pd.DataFrame, lookback=200):
    """
    Detect recent 3-candle FVGs in tail.
    Returns list of fvgs [{type, low, high, idx, premium(bool)}] newest first.
    """
    fvgs = []
    n = len(df)
    if n < 5:
        return fvgs
    for i in range(2, min(n-1, lookback)):
        # pick candles by relative position from tail
        j = n - 1 - i
        if j - 2 < 0:
            continue
        c2 = df.iloc[j]     # older
        c1 = df.iloc[j+1]
        c0 = df.iloc[j+2]   # newer
        # bullish FVG: older high < newer low (gap up)
        if c2['high'] < c0['low']:
            ema50 = df['close'].ewm(span=50).mean().iloc[j+2]
            premium = c0['low'] > ema50
            fvgs.append({"type":"bullish","low":float(c2['high']),"high":float(c0['low']),"idx": j+2,"premium":premium})
        # bearish FVG: older low > newer high (gap down)
        if c2['low'] > c0['high']:
            ema50 = df['close'].ewm(span=50).mean().iloc[j+2]
            premium = c0['high'] < ema50
            fvgs.append({"type":"bearish","low":float(c0['high']),"high":float(c2['low']),"idx": j+2,"premium":premium})
    # newest first
    return sorted(fvgs, key=lambda x: x['idx'], reverse=True)

# ---------- Helpers: quality order block detection ----------
def find_quality_order_block(df: pd.DataFrame, lookback=80):
    """
    Stricter OB search. Returns single latest OB dict or None:
      {"type":"bullish"/"bearish", "low":..., "high":..., "idx":...}
    """
    n = len(df)
    if n < 6:
        return None
    for i in range(n-3, max(3, n - lookback), -1):
        candle = df.iloc[i]
        prev = df.iloc[i-1]
        nxt = df.iloc[i+1] if i+1 < n else None
        body = abs(candle['close'] - candle['open'])
        rng = candle['high'] - candle['low'] + 1e-9
        body_ratio = body / rng if rng > 0 else 0
        # volume check
        vol_ok = True
        try:
            vol_avg = df['vol'].tail(30).mean()
            vol_ok = float(candle['vol']) >= max(1, vol_avg * 0.6)
        except Exception:
            vol_ok = True

        # Bullish OB candidate: prev bearish -> candle bullish + swept low + confirmation next
        if prev['close'] < prev['open'] and candle['close'] > candle['open'] and body_ratio > 0.25 and vol_ok:
            # check for swept low (candle low < prev low)
            if candle['low'] < prev['low']:
                # optional confirmation: next candle closes bullish
                if nxt is not None and nxt['close'] > candle['close']:
                    low = float(min(candle['low'], prev['low']))
                    high = float(max(candle['close'], prev['close']))
                    return {"type":"bullish","low":low,"high":high,"idx": i-1}
        # Bearish OB candidate
        if prev['close'] > prev['open'] and candle['close'] < candle['open'] and body_ratio > 0.25 and vol_ok:
            if candle['high'] > prev['high']:
                if nxt is not None and nxt['close'] < candle['close']:
                    low = float(min(candle['close'], prev['close']))
                    high = float(max(candle['high'], prev['high']))
                    return {"type":"bearish","low":low,"high":high,"idx": i-1}
    return None

# ---------- Helpers: confirm market structure shift ----------
def confirm_market_structure_shift(df: pd.DataFrame, side: str):
    """
    Quick confirmation: requires at least one HL (for BUY) or LH (for SELL) forming in recent swings.
    """
    if len(df) < 20:
        return False
    highs = df['high']
    lows = df['low']
    # find local swing highs/lows using rolling windows
    swing_highs = highs[(highs == highs.rolling(5, center=True).max())].dropna()
    swing_lows = lows[(lows == lows.rolling(5, center=True).min())].dropna()
    recent_highs = swing_highs.tail(3).values if len(swing_highs)>0 else []
    recent_lows = swing_lows.tail(3).values if len(swing_lows)>0 else []
    if side == "BUY":
        if len(recent_lows) >= 2 and recent_lows[-1] > recent_lows[-2]:
            return True
    else:
        if len(recent_highs) >= 2 and recent_highs[-1] < recent_highs[-2]:
            return True
    return False

# ---------- Helpers: liquidity path check ----------
def check_liquidity_path(df: pd.DataFrame, side: str, entry: float, tp: float):
    """
    Return True if path to TP is reasonably clear (no recent touch).
    """
    if len(df) < 15:
        return True
    tail_highs = df['high'].tail(15)
    tail_lows = df['low'].tail(15)
    if side == "BUY":
        recent_touch = (tail_highs >= tp * 0.995).any()
        return not bool(recent_touch)
    else:
        recent_touch = (tail_lows <= tp * 1.005).any()
        return not bool(recent_touch)

# ---------- Utilities ----------
def numeric_safe(x):
    try:
        return float(x)
    except Exception:
        return None

# ---------------- TP/SL HELPERS (unchanged) ----------------
def romeopt_tp_sl(entry, side, atr_val, ob_zone, df):
    """
    OPTIMIZED TP/SL using market structure + ATR
    """
    recent_high = df['high'].iloc[-10:].max()  # Shorter lookback for relevance
    recent_low = df['low'].iloc[-10:].min()

    if side == "BUY":
        sl_ob = ob_zone["low"] - (atr_val * 0.3)
        sl_structure = recent_low - (atr_val * 0.3)
        sl = min(sl_ob, sl_structure)

        risk = entry - sl

        min_risk = atr_val * 0.5  # At least half ATR
        if risk < min_risk:
            risk = min_risk
            sl = entry - risk

        base_tp1 = entry + (risk * 0.8)
        base_tp2 = entry + (risk * 1.5)
        base_tp3 = entry + (risk * 2.5)

        nearest_resistance = df['high'].tail(20).max()  # Last 20 candles only
        major_resistance = df['high'].tail(50).max()    # Last 50 candles

        tp1 = min(base_tp1, nearest_resistance) if nearest_resistance > entry else base_tp1
        tp2 = min(base_tp2, major_resistance) if major_resistance > tp1 else base_tp2
        tp3 = base_tp3  # Extended target

        min_tp_gap = risk * 0.3  # Minimum 30% of risk between TPs

        tp1 = max(tp1, entry + (risk * 0.5))  # At least 0.5R profit
        tp2 = max(tp2, tp1 + min_tp_gap)      # Meaningful gap from TP1
        tp3 = max(tp3, tp2 + min_tp_gap)      # Meaningful gap from TP2

    else:  # SELL
        sl_ob = ob_zone["high"] + (atr_val * 0.3)
        sl_structure = recent_high + (atr_val * 0.3)
        sl = max(sl_ob, sl_structure)

        risk = sl - entry

        min_risk = atr_val * 0.5
        if risk < min_risk:
            risk = min_risk
            sl = entry + risk

        base_tp1 = entry - (risk * 0.8)
        base_tp2 = entry - (risk * 1.5)
        base_tp3 = entry - (risk * 2.5)

        nearest_support = df['low'].tail(20).min()
        major_support = df['low'].tail(50).min()

        tp1 = max(base_tp1, nearest_support) if nearest_support < entry else base_tp1
        tp2 = max(base_tp2, major_support) if major_support < tp1 else base_tp2
        tp3 = base_tp3  # Extended target

        min_tp_gap = risk * 0.3

        tp1 = min(tp1, entry - (risk * 0.5))  # At least 0.5R profit
        tp2 = min(tp2, tp1 - min_tp_gap)      # Meaningful gap from TP1
        tp3 = min(tp3, tp2 - min_tp_gap)      # Meaningful gap from TP2

    return sl, tp1, tp2, tp3

# ---------- update_tp_sl_live (patched to use quality OB finder) ----------
def update_tp_sl_live(sig: dict, df: pd.DataFrame):
    latest_ob = find_quality_order_block(df)
    if not latest_ob:
        # preserve existing if no new ob
        return sig
    atr_val = float(atr(df,14).iloc[-1])
    # use sig entry_limit if present else sig entry
    entry_for_calc = sig.get("entry_limit", sig.get("entry"))
    side = sig["side"]
    sl,tp1,tp2,tp3 = romeopt_tp_sl(entry_for_calc, side, atr_val, latest_ob, df)
    sig["sl"]=sl; sig["tp1"]=tp1; sig["tp2"]=tp2; sig["tp3"]=tp3
    sig["latest_ob"]=latest_ob
    return sig

# ---------------- SL CLUSTER ----------------
recent_sl = defaultdict(lambda: deque())
def record_sl_hit(symbol: str, lookback_minutes=30):
    now = time.time(); dq = recent_sl[symbol]; dq.append(now)
    cutoff = now - lookback_minutes*60
    while dq and dq[0]<cutoff: dq.popleft()
def deprioritized(symbol: str, threshold=3, lookback=30):
    dq = recent_sl[symbol]; now=time.time(); cutoff=now-lookback*60
    while dq and dq[0]<cutoff: dq.popleft()
    return len(dq)>=threshold

# ---------------- LOG SIGNAL (patched to store details) ----------------
async def log_signal(sig):
    async with db_lock:
        details_json = json.dumps(sig.get("detailed", {}), default=str)
        await db_conn.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score,latest_ob,details)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (sig["symbol"],sig["side"],sig["entry"],sig.get("sl"),sig.get("tp1"),sig.get("tp2"),sig.get("tp3"),
              datetime.datetime.utcnow().isoformat(),"PENDING",sig["reason"],sig["score"],str(sig.get("latest_ob","")), details_json))
        await db_conn.commit()

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    # This coroutine checks OPEN/PENDING signals for hits and updates statuses.
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("SELECT id,symbol,side,entry,sl,tp1,tp2,tp3,tp1_hit,tp2_hit,tp3_hit,status,details FROM signals WHERE status IN ('OPEN','PENDING')") as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status, details = row
                        # convert hits from None to 0 if necessary
                        tp1_hit = tp1_hit or 0
                        tp2_hit = tp2_hit or 0
                        tp3_hit = tp3_hit or 0

                        try:
                            ticker = await exchange.fetch_ticker(symbol)
                        except Exception as e:
                            log.debug("fetch_ticker failed for %s: %s", symbol, e)
                            continue
                        last_price = ticker.get("last")
                        if last_price is None: continue

                        ohlcv = await fetch_ohlcv(exchange, symbol, "1m", 50)
                        if ohlcv:
                            df_live = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                            for c in ["open","high","low","close","vol"]: df_live[c]=pd.to_numeric(df_live[c],errors="coerce")
                            sig = {"symbol":symbol,"side":side,"entry":entry,"sl":sl,"tp1":tp1,"tp2":tp2,"tp3":tp3}
                            sig = update_tp_sl_live(sig, df_live)
                            sl,tp1,tp2,tp3 = sig["sl"], sig["tp1"], sig["tp2"], sig["tp3"]

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
                            # include a compact details snippet in alerts for quick triage
                            try:
                                diag = json.loads(details) if details else {}
                            except Exception:
                                diag = {}
                            alert = (f"🎯 {symbol} {side} update\nEntry:{entry}\nLast:{last_price}\n"
                                     f"Hits:{','.join(hits)}\nSL:{sl}\nTP1:{tp1} TP2:{tp2} TP3:{tp3}\n"
                                     f"Status:{status}\nScore:{diag.get('score','N/A')}")
                            await tg(alert)

                        if sl_hit: record_sl_hit(symbol)
                        await db_conn.execute("UPDATE signals SET tp1_hit=?,tp2_hit=?,tp3_hit=?,status=? WHERE id=?",
                                             (tp1_hit,tp2_hit,tp3_hit,status,sig_id))
                await db_conn.commit()
        except Exception as e: log.exception("monitor error: %s", e)
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- SCAN LOOP ----------------
last_signal_time = {}
async def scan_loop(exchange):
    while True:
        t0=time.time()
        try:
            tickers = await exchange.fetch_tickers()
            top = sorted([(s,v.get("quoteVolume",0)) for s,v in tickers.items() if s.endswith("USDT")], key=lambda x:x[1], reverse=True)[:TOP_N]
            signals_found = 0
            for symbol,_ in top:
                if deprioritized(symbol): continue
                for tf in TIMEFRAMES:
                    key=f"{symbol}:{tf}"
                    if key in last_signal_time and time.time()-last_signal_time[key]<60: continue
                    ohlcv = await fetch_ohlcv(exchange,symbol,tf,200)
                    if not ohlcv: continue
                    df=pd.DataFrame(ohlcv,columns=["ts","open","high","low","close","vol"])
                    for c in ["open","high","low","close","vol"]: df[c]=pd.to_numeric(df[c],errors="coerce")
                    sig = await generate_signal_romeopt(exchange,df,symbol,tf)
                    if sig:
                        htf_flag = sig.get("htf_alignment", "N/A")
                        sweep_flag = sig.get("liquidity_sweep", "N/A")
                        # condensed details for alerts; the full diagnostics stored in DB details
                        breakdown = ', '.join(sig.get('reason_list', []))[:700]  # shorten alert if too long
                        await tg(f"🏆 {sig['symbol']} ({tf}) {sig['side']}\nEntry:{sig['entry']} (limit:{sig.get('entry_limit')})\nSL:{sig.get('sl')}\nTP1:{sig.get('tp1')} TP2:{sig.get('tp2')} TP3:{sig.get('tp3')}\nScore:{sig['score']}\nHTF:{htf_flag} Sweep:{sweep_flag}\nBreakdown:{breakdown}")
                        await log_signal(sig)
                        last_signal_time[key]=time.time()
                        signals_found+=1
            log.info(f"📊 Scan complete: {signals_found} RomeOPT signals found")
        except Exception as e: log.exception("scan error: %s", e)
        elapsed=time.time()-t0
        await asyncio.sleep(max(1,SCAN_INTERVAL-elapsed))

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
    global exchange
    exchange = ccxt.okx({"enableRateLimit": True})
    await tg("🏆 ROMEOPT 6-Step Scanner Started - Live Early Signals")
    # run scan and monitor concurrently
    await asyncio.gather(scan_loop(exchange), monitor_signals())

if __name__=="__main__":
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--http", action="store_true")
    args=p.parse_args()
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=9000)
    else:
        try:
            asyncio.run(main())
        finally:
            # close DB connection gracefully if exists
            if db_conn:
                try:
                    asyncio.run(db_conn.close())
                except Exception:
                    # if event loop closed, ignore
                    pass