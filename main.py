#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Modular Premium SMC Scanner (Set B) - Production-ready
Supports independent steps:
1. DB init
2. Monitor signals
3. Scan loop
4. Signal generator
5. HTF trend & choppiness
6. Indicators
"""

import os, time, asyncio, logging, datetime, math
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
from collections import defaultdict, deque
from fastapi import FastAPI, Request, HTTPException
import uvicorn

# ---------------- ENV ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")
DB_PATH = os.getenv("DB_PATH", "./data/signals.db")

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))
TOP_N = int(os.getenv("TOP_N", 1))
BTC_PAIR = os.getenv("BTC_PAIR", "BTC-USDT-SWAP")
TIMEFRAMES = ["1m","3m","5m","15m","30m"]

# Filters
MIN_SCORE_TO_SIGNAL = int(os.getenv("MIN_SCORE_TO_SIGNAL", 5))
CHOPPINESS_REJECT = float(os.getenv("CHOPPINESS_REJECT", 61.0))
WICK_DOMINANCE_REJECT = float(os.getenv("WICK_DOMINANCE_REJECT", 1.2))
MIN_SL_ATR_MULT = float(os.getenv("MIN_SL_ATR_MULT", 0.6))
MIN_ATR_FOR_SCAN = float(os.getenv("MIN_ATR_FOR_SCAN", 0.0001))
HTF_ALIGNMENT_ENFORCE = os.getenv("HTF_ALIGNMENT_ENFORCE", "1m,3m,5m")
HTF_ALIGNMENT_SET = set([x.strip() for x in HTF_ALIGNMENT_ENFORCE.split(",") if x.strip()])

DEFAULT_TIMEOUT = float(os.getenv("API_CALL_TIMEOUT", 8.0))
DEFAULT_RETRIES = int(os.getenv("API_CALL_RETRIES", 2))

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("smc_bot")
db_lock = asyncio.Lock()

# ---------------- TELEGRAM ----------------
def escape_html(msg: str) -> str:
    if not msg: return "-"
    return str(msg).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

async def tg(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    if not msg: return
    safe_msg = escape_html(msg)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            r = await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID,"text":safe_msg,"parse_mode":"HTML"})
            if r.status_code != 200: log.error(f"Telegram send failed: {r.status_code} | {r.text}")
        except Exception as e:
            log.debug(f"Telegram exception: {e}")

# ---------------- SQLITE ----------------
async def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
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
        );""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS pauses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reason TEXT,
            timestamp TEXT
        );""")
        await db.commit()
    log.info("DB initialized successfully")

# ---------------- SAFE CALL ----------------
async def safe_call(fn, *args, timeout=DEFAULT_TIMEOUT, retries=DEFAULT_RETRIES, **kwargs):
    last_err = None
    for attempt in range(retries+1):
        try:
            coro = fn(*args, **kwargs)
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError as e:
            last_err = e
            log.debug(f"Timeout {fn} attempt {attempt+1}")
        except Exception as e:
            last_err = e
            log.debug(f"Exception {fn} attempt {attempt+1}: {e}")
        await asyncio.sleep(0.25)
    log.warning(f"safe_call ultimate fail {fn}: {last_err}")
    return None

# ---------------- OHLCV ----------------
async def fetch_ohlcv(exchange, symbol, timeframe, limit=200):
    try:
        res = await safe_call(exchange.fetch_ohlcv, symbol, timeframe, None, limit, {})
        return res
    except Exception as e:
        log.debug(f"fetch_ohlcv failed {symbol} {timeframe}: {e}")
        return None

# ---------------- INDICATORS ----------------
def sma(series: pd.Series, period: int): return series.rolling(period,min_periods=1).mean()

def atr(df: pd.DataFrame, period=14):
    if df is None or len(df)<2: return pd.Series([])
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.DataFrame({
        "h-l": (high-low).abs(),
        "h-pc": (high-close.shift(1)).abs(),
        "l-pc": (low-close.shift(1)).abs()
    }).max(axis=1)
    return tr.rolling(period,min_periods=1).mean()

def choppiness_index(df: pd.DataFrame, period=14):
    if df is None or len(df)<period+1: return 50.0
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat([(high-low).abs(), (high-close.shift(1)).abs(), (low-close.shift(1)).abs()], axis=1).max(axis=1)
    atr_sum = tr.rolling(period,min_periods=1).sum()
    hh = high.rolling(period,min_periods=1).max()
    ll = low.rolling(period,min_periods=1).min()
    denom = (hh-ll).replace(0,1e-12)
    frac = (atr_sum/denom).replace([float('inf'),-float('inf')],0).fillna(0)
    scaled = frac.apply(lambda x: math.log(x+1e-12) if x>0 else -10)
    minv, maxv = float(scaled.min()), float(scaled.max())
    if maxv-minv<1e-9: return 50.0
    return float(100*(scaled.iloc[-1]-minv)/(maxv-minv))

def wick_dominance(df: pd.DataFrame, lookback=12):
    if df is None or len(df)<lookback: return 0.0
    sample = df.iloc[-lookback:]
    bodies = (sample["close"]-sample["open"]).abs().replace(0,1e-12)
    upper_wick = (sample["high"]-sample[["open","close"]].max(axis=1)).abs()
    lower_wick = (sample[["open","close"]].min(axis=1)-sample["low"]).abs()
    ratio = ((upper_wick+lower_wick)/2)/bodies
    ratio = ratio.replace([float('inf'),-float('inf')],0).fillna(0)
    return float(ratio.mean())

# ---------------- SMC DETECTION ----------------
def detect_order_blocks(df: pd.DataFrame):
    if df is None or len(df)<3: return None,None,None
    candle = df.iloc[-3]
    try:
        if candle["close"]>candle["open"]: return "bullish", float(candle["open"]), float(candle["low"])
        return "bearish", float(candle["high"]), float(candle["open"])
    except: return None,None,None

def detect_fvg(df: pd.DataFrame):
    if df is None or len(df)<3: return False,False
    c1,c2,c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    bull = (c2["low"]>c1["high"]) and (c3["low"]>c2["high"])
    bear = (c2["high"]<c1["low"]) and (c3["high"]<c2["low"])
    return bull,bear

def detect_sweep(df: pd.DataFrame):
    if df is None or len(df)<6: return False,False
    last = df.iloc[-1]; prev=df.iloc[-5:-1]
    return last["high"]>prev["high"].max(), last["low"]<prev["low"].min()

def strong_sweep_or_bos(df: pd.DataFrame, lookback=6):
    if df is None or len(df)<lookback: return False
    sweep_h, sweep_l = detect_sweep(df)
    atr_val = float(atr(df,14).iloc[-1]) if len(df)>0 else MIN_ATR_FOR_SCAN
    if sweep_h: return float(df["high"].iloc[-1]-df["high"].iloc[-2])>=0.5*atr_val
    if sweep_l: return float(df["low"].iloc[-2]-df["low"].iloc[-1])>=0.5*atr_val
    last_body = abs(df["close"].iloc[-1]-df["open"].iloc[-1])
    return last_body>=0.4*atr_val

# ---------------- HTF TREND ----------------
def compute_htf_trend(df: pd.DataFrame, fast=5, slow=20):
    if df is None or len(df)<slow+2: return "NEUTRAL"
    close = df["close"].astype(float)
    sfast, sslow = sma(close,fast).iloc[-1], sma(close,slow).iloc[-1]
    if sfast>sslow*1.0006: return "BUY"
    if sfast<sslow*0.9994: return "SELL"
    return "NEUTRAL"

# ---------------- SL-CLUSTER ----------------
recent_sl = defaultdict(lambda: deque())
def record_sl_hit(symbol, lookback_minutes=30):
    now = time.time(); dq=recent_sl[symbol]; dq.append(now)
    cutoff = now-lookback_minutes*60
    while dq and dq[0]<cutoff: dq.popleft()
def deprioritized(symbol, threshold=3, lookback=30):
    dq=recent_sl[symbol]; now=time.time(); cutoff=now-lookback*60
    while dq and dq[0]<cutoff: dq.popleft()
    return len(dq)>=threshold

# ---------------- SIGNAL GENERATOR ----------------
def generate_signal(df: pd.DataFrame, symbol: str, context=None):
    if context is None: context={}
    tf=context.get("tf","15m")
    if df is None or len(df)<6: return None

    last=float(df["close"].iloc[-1])
    ob_type, ob_hi, ob_lo = detect_order_blocks(df)
    if ob_type is None: return None

    bull_fvg, bear_fvg = detect_fvg(df)
    sweep_h, sweep_l = detect_sweep(df)

    if not strong_sweep_or_bos(df): return None

    score=0; reasons=[]
    if ob_type=="bullish": score+=2; reasons.append("OB Bull +2")
    else: score+=2; reasons.append("OB Bear +2")
    if bull_fvg: score+=2; reasons.append("FVG Bull +2")
    elif bear_fvg: score+=2; reasons.append("FVG Bear +2")
    score+=2; reasons.append("BOS +2")
    if sweep_h or sweep_l: score+=1; reasons.append("Sweep +1")
    else: reasons.append("No Sweep +0")

    wick_ratio = wick_dominance(df)
    if wick_ratio>=WICK_DOMINANCE_REJECT: return None

    side="BUY" if ob_type=="bullish" else "SELL"

    btc_trend=context.get("btc_trend","NEUTRAL")
    btc_choppy=context.get("btc_choppy",None)
    if tf in HTF_ALIGNMENT_SET and btc_trend in ("BUY","SELL"):
        if side!=btc_trend: return None

    if btc_choppy is not None and btc_choppy>=CHOPPINESS_REJECT: return None

    atr_val=None
    df15=context.get("df_15m")
    if df15 is not None and len(df15)>=10:
        atr_series=atr(df15,14)
        if len(atr_series)>0: atr_val=float(atr_series.iloc[-1])

    entry=float(last)
    tp_mult, sl_mult = 0.8, 1.0
    if atr_val and atr_val>0:
        if side=="BUY":
            sl=entry-sl_mult*atr_val; tp1=entry+tp_mult*atr_val; tp2=entry+tp_mult*1.5*atr_val; tp3=entry+tp_mult*2.5*atr_val
        else:
            sl=entry+sl_mult*atr_val; tp1=entry-tp_mult*atr_val; tp2=entry-tp_mult*1.5*atr_val; tp3=entry-tp_mult*2.5*atr_val
    else:
        if side=="BUY": sl=float(ob_lo); tp1=entry*1.004; tp2=entry*1.008; tp3=entry*1.012
        else: sl=float(ob_hi); tp1=entry*0.996; tp2=entry*0.992; tp3=entry*0.988

    if score<MIN_SCORE_TO_SIGNAL: return None

    return {"symbol":symbol,"side":side,"entry":entry,"sl":sl,"tp1":tp1,"tp2":tp2,"tp3":tp3,"score":score,"reason_list":reasons}

# ---------------- LOG SIGNAL ----------------
async def log_signal(sig):
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (sig["symbol"],sig["side"],sig["entry"],sig["sl"],sig["tp1"],sig["tp2"],sig["tp3"],
                  datetime.datetime.utcnow().isoformat(),"OPEN","SMC Signal",sig["score"]))
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
                        ticker = await safe_call(exchange.fetch_ticker,symbol)
                        if not ticker: continue
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
                        if hits: await tg(f"{symbol} update {hits}")
                        if sl_hit: record_sl_hit(symbol)
                        async with db_lock:
                            await db.execute("""
                            UPDATE signals SET tp1_hit=?,tp2_hit=?,tp3_hit=?,status=? WHERE id=?
                            """,(tp1_hit,tp2_hit,tp3_hit,status,sig_id))
            await asyncio.sleep(SCAN_INTERVAL)
        except Exception as e:
            log.exception("Monitor error: %s", e)
            await asyncio.sleep(5)

# ---------------- SCAN LOOP ----------------
last_signal_time = {}

async def scan_loop(exchange):
    while True:
        t0 = time.time()
        try:
            # --- prefetch BTC 15m for HTF alignment and choppiness once per scan ---
            btc_trend = "NEUTRAL"
            btc_choppy = None
            df_btc15 = None
            try:
                btc_15_ohlcv = await fetch_ohlcv(exchange, BTC_PAIR, "15m", 200)
                if btc_15_ohlcv:
                    df_btc15 = pd.DataFrame(btc_15_ohlcv, columns=["ts","open","high","low","close","vol"])
                    for c in ["open","high","low","close","vol"]:
                        df_btc15[c] = pd.to_numeric(df_btc15[c], errors="coerce")
                    btc_trend = compute_htf_trend(df_btc15, fast=5, slow=20)
                    btc_choppy = choppiness_index(df_btc15, period=14)
            except Exception as e:
                log.debug("BTC prefetch/trend failed: %s", e)

            # --- fetch tickers ---
            tickers = await safe_call(exchange.fetch_tickers)
            if not tickers:
                log.warning("fetch_tickers failed/timeout; sleeping briefly")
                await asyncio.sleep(1)
                continue

            # --- choose top N by volume ---
            tv = []
            for s, v in tickers.items():
                try:
                    qv = v.get("quoteVolume") or v.get("quoteVolume24h") or v.get("baseVolume") or 0
                    tv.append((s, float(qv)))
                except:
                    continue
            top = sorted([x for x in tv if str(x[0]).endswith("USDT")], key=lambda x:x[1], reverse=True)[:TOP_N]

            # --- scan each symbol ---
            for symbol, vol in top:
                if deprioritized(symbol):
                    log.debug(f"Symbol {symbol} deprioritized due to SL cluster")
                    continue

                ohlcvs = {}
                for tf in TIMEFRAMES:
                    key = f"{symbol}:{tf}"
                    if key in last_signal_time and time.time() - last_signal_time[key] < 1800:
                        continue

                    ohlcv = await fetch_ohlcv(exchange, symbol, tf, 200)
                    if not ohlcv:
                        continue

                    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                    for c in ["open","high","low","close","vol"]:
                        df[c] = pd.to_numeric(df[c], errors="coerce")
                    ohlcvs[tf] = df

                    # --- build context for signal generator ---
                    context = {
                        "tf": tf,
                        "df_15m": ohlcvs.get("15m") or df_btc15,
                        "df_1h": ohlcvs.get("1h"),
                        "btc_trend": btc_trend,
                        "btc_choppy": btc_choppy
                    }

                    # --- generate signal ---
                    sig = generate_signal(df, symbol, context)
                    if sig:
                        last_signal_time[key] = time.time()
                        await log_signal(sig)
                        await tg(
                            f"🚀 {sig['symbol']} ({tf}) {sig['side']}\n"
                            f"Entry:{sig['entry']}\nSL:{sig['sl']}\nTP1:{sig['tp1']} TP2:{sig['tp2']} TP3:{sig['tp3']}\n"
                            f"Score:{sig['score']}\nBreakdown:{', '.join(sig['reason_list'])}"
                        )

        except Exception as e:
            log.exception("scan_loop exception: %s", e)
            await tg(f"❌ Scan loop error: {e}")

        elapsed = time.time() - t0
        await asyncio.sleep(max(1, SCAN_INTERVAL - elapsed))
        
# ---------------- MAIN ----------------
async def main():
    # Initialize DB
    await init_db()

    # Initialize exchange (OKX in this example)
    exchange = ccxt.okx({
        "enableRateLimit": True
    })

    # Start scan and monitor loops as independent tasks
    asyncio.create_task(scan_loop(exchange))
    await asyncio.sleep(0.5)  # small sleep for scheduling fairness
    asyncio.create_task(monitor_signals(exchange))

    # Keep main alive; can add heartbeat or maintenance here
    try:
        while True:
            await asyncio.sleep(3600)  # just sleep and let tasks run
    except KeyboardInterrupt:
        log.info("Shutdown requested")
        await tg("⚡ Bot shutdown requested")
        await exchange.close()
        