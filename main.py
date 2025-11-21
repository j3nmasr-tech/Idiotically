#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Production-ready Premium SMC Scanner (Set B architecture) - UPGRADED & FIXED FOR DEPLOY
- Full SMC core: OB/FVG/Liquidity/BOS/MSS detection
- ATR-based TP/SL
- SL-cluster deprioritization
- Momentum & volatility filters
- Top N USDT coins, multi-timeframe scan
- Added filters:
    * 15m BTC trend alignment (for 1m/3m/5m entries)
    * Choppiness index rejection
    * Wick-dominance rejection
    * Strong BOS/Sweep validation
    * Volatility-aware SL sanity
- Fixes to prevent freezes:
    * safe_call wrapper with timeouts + retries for CCXT calls
    * fetch_ohlcv uses safe_call
    * fetch_tickers uses safe_call
    * main() starts scan and monitor as independent tasks (no blocking gather)
"""

import os, time, asyncio, logging, datetime, math
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
DB_PATH = os.getenv("DB_PATH", "/app/data/signals.db")

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))
TOP_N = int(os.getenv("TOP_N", 1))
MIN_VOLUME = float(os.getenv("MIN_VOLUME", 1000000))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", 0.002))
BTC_PAIR = os.getenv("BTC_PAIR", "BTC-USDT-SWAP")
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 3600))
DAILY_SUMMARY_HOUR = int(os.getenv("DAILY_SUMMARY_HOUR", 23))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m", "1h", "4h"]

# ---------------- TUNABLE FILTERS ----------------
MIN_SCORE_TO_SIGNAL = int(os.getenv("MIN_SCORE_TO_SIGNAL", 5))
CHOPPINESS_REJECT = float(os.getenv("CHOPPINESS_REJECT", 61.0))  # classic CI threshold (higher == more choppy)
WICK_DOMINANCE_REJECT = float(os.getenv("WICK_DOMINANCE_REJECT", 1.2))  # avg wick / body ratio
MIN_SL_ATR_MULT = float(os.getenv("MIN_SL_ATR_MULT", 0.6))  # min acceptable SL distance in ATR multiples
MIN_ATR_FOR_SCAN = float(os.getenv("MIN_ATR_FOR_SCAN", 0.0001))  # avoid division by zero
HTF_ALIGNMENT_ENFORCE = os.getenv("HTF_ALIGNMENT_ENFORCE", "1m,3m,5m")  # TFs where alignment required
HTF_ALIGNMENT_SET = set([x.strip() for x in HTF_ALIGNMENT_ENFORCE.split(",") if x.strip()])

# ---------------- SAFE CALL CONFIG ----------------
DEFAULT_TIMEOUT = float(os.getenv("API_CALL_TIMEOUT", 8.0))
DEFAULT_RETRIES = int(os.getenv("API_CALL_RETRIES", 2))

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("smc_bot")
db_lock = asyncio.Lock()

# ---------------- TELEGRAM ----------------
def escape_html(msg: str) -> str:
    if not msg:
        return "-"
    return str(msg).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

async def tg(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.debug("Telegram creds missing or suppressed.")
        return
    if not msg:
        log.error("Empty Telegram message")
        return
    safe_msg = escape_html(msg)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            r = await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": safe_msg, "parse_mode":"HTML"})
            if r.status_code != 200:
                log.error(f"Telegram send failed: {r.status_code} | {r.text}")
        except Exception as e:
            log.debug(f"Telegram send exception: {e}")

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

# ---------------- SAFE CALL ----------------
async def safe_call(fn, *args, timeout=DEFAULT_TIMEOUT, retries=DEFAULT_RETRIES, **kwargs):
    """
    Wrapper to call async functions with timeout and retry.
    Returns None on ultimate failure.
    """
    last_err = None
    for attempt in range(retries + 1):
        try:
            # wrap the underlying coroutine with asyncio.wait_for
            coro = fn(*args, **kwargs)
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError as e:
            last_err = e
            log.debug(f"safe_call timeout on {getattr(fn,'__name__',str(fn))} attempt {attempt+1}/{retries+1}")
        except Exception as e:
            last_err = e
            log.debug(f"safe_call exception on {getattr(fn,'__name__',str(fn))} attempt {attempt+1}/{retries+1}: {e}")
        # small backoff
        await asyncio.sleep(0.25)
    log.warning(f"safe_call failed for {getattr(fn,'__name__',str(fn))}: {last_err}")
    return None

# ---------------- OHLCV (uses safe_call) ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try:
        # ccxt signature: fetch_ohlcv(symbol, timeframe, since=None, limit=None, params={})
        res = await safe_call(exchange.fetch_ohlcv, symbol, timeframe, None, limit, {})
        return res
    except Exception as e:
        log.debug(f"fetch_ohlcv wrapper exception for {symbol} {timeframe}: {e}")
        return None

# ---------------- INDICATORS ----------------
def atr(df: pd.DataFrame, period=14):
    if df is None or len(df) < 2:
        return pd.Series([])
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.DataFrame({
        "h-l": (high - low).abs(),
        "h-pc": (high - close.shift(1)).abs(),
        "l-pc": (low - close.shift(1)).abs()
    }).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()

def sma(series: pd.Series, period: int):
    return series.rolling(period, min_periods=1).mean()

def choppiness_index(df: pd.DataFrame, period=14):
    """
    Returns the Choppiness Index (0-100). Higher values indicate chop.
    Safe version: no deprecated pandas options, no warnings, no freeze.
    """
    if df is None or len(df) < period + 1:
        return 50.0  # neutral default

    high = pd.to_numeric(df["high"], errors='coerce')
    low = pd.to_numeric(df["low"], errors='coerce')
    close = pd.to_numeric(df["close"], errors='coerce')

    # True Range approximation
    tr = pd.concat([
        (high - low).abs(),
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs()
    ], axis=1).max(axis=1)

    atr_sum = tr.rolling(period, min_periods=1).sum()
    hh = high.rolling(period, min_periods=1).max()
    ll = low.rolling(period, min_periods=1).min()
    denom = hh - ll
    denom = denom.replace(0, 1e-12)

    frac = atr_sum / denom
    frac = pd.to_numeric(frac, errors='coerce').fillna(0)

    # log transform
    scaled = frac.apply(lambda x: math.log(x + 1e-12) if x > 0 else -10)

    minv, maxv = float(scaled.min()), float(scaled.max())
    if maxv - minv < 1e-9:
        return 50.0

    ci = 100.0 * (scaled.iloc[-1] - minv) / (maxv - minv)
    return float(ci)

# ---------------- SMC CORE ----------------
def detect_swing_points(df: pd.DataFrame):
    if df is None or len(df) < 5:
        return None, None
    last = df.iloc[-1]
    prev = df.iloc[-3:-1]
    swing_high = last["high"] > prev["high"].max()
    swing_low = last["low"] < prev["low"].min()
    return swing_high, swing_low

def detect_active_range(df: pd.DataFrame, lookback=10):
    if df is None or len(df) < lookback:
        return None, None
    last = df.iloc[-lookback:]
    return last["high"].max(), last["low"].min()

def detect_liquidity_pools(df: pd.DataFrame):
    hh, ll = detect_swing_points(df)
    return hh, ll

def detect_sweep(df: pd.DataFrame):
    if df is None or len(df) < 6:
        return False, False
    last = df.iloc[-1]
    prev = df.iloc[-5:-1]
    return last["high"] > prev["high"].max(), last["low"] < prev["low"].min()

def detect_bos_mss(df: pd.DataFrame):
    hh, ll = detect_sweep(df)
    return hh, ll

def detect_fvg(df: pd.DataFrame):
    if df is None or len(df) < 3:
        return False, False
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    bull = (c2["low"] > c1["high"]) and (c3["low"] > c2["high"])
    bear = (c2["high"] < c1["low"]) and (c3["high"] < c2["low"])
    return bull, bear

def detect_order_blocks(df: pd.DataFrame):
    if df is None or len(df) < 3:
        return None, None, None
    candle = df.iloc[-3]
    try:
        if candle["close"] > candle["open"]:
            return "bullish", float(candle["open"]), float(candle["low"])
        return "bearish", float(candle["high"]), float(candle["open"])
    except Exception:
        return None, None, None

# ---------------- SMC STRENGTH VALIDATORS ----------------
def wick_dominance(df: pd.DataFrame, lookback=12):
    if df is None or len(df) < lookback:
        return 0.0
    sample = df.iloc[-lookback:]
    bodies = (sample["close"] - sample["open"]).abs().replace(0, 1e-12)
    upper_wick = (sample["high"] - sample[["open","close"]].max(axis=1)).abs()
    lower_wick = (sample[["open","close"]].min(axis=1) - sample["low"]).abs()
    wick = (upper_wick + lower_wick) / 2.0
    ratio = (wick / bodies).replace([float('inf'), -float('inf')], 0).fillna(0)
    return float(ratio.mean())

def strong_sweep_or_bos(df: pd.DataFrame, lookback=6):
    if df is None or len(df) < lookback:
        return False
    sweep_h, sweep_l = detect_sweep(df)
    atr_series = atr(df, 14)
    atr_val = float(atr_series.iloc[-1]) if len(atr_series) > 0 else None
    if atr_val is None or atr_val < MIN_ATR_FOR_SCAN:
        atr_val = max(MIN_ATR_FOR_SCAN, float(df["high"].iloc[-lookback:].max() - df["low"].iloc[-lookback:].min()) / lookback)
    if sweep_h:
        amplitude = float(df["high"].iloc[-1] - df["high"].iloc[-2])
        return amplitude >= 0.5 * atr_val
    if sweep_l:
        amplitude = float(df["low"].iloc[-2] - df["low"].iloc[-1])
        return amplitude >= 0.5 * atr_val
    last_body = abs(df["close"].iloc[-1] - df["open"].iloc[-1])
    return last_body >= 0.4 * atr_val

# ---------------- HTF TREND ----------------
def compute_htf_trend(df: pd.DataFrame, fast=5, slow=20):
    if df is None or len(df) < slow + 2:
        return "NEUTRAL"
    close = df["close"].astype(float)
    sfast = sma(close, fast).iloc[-1]
    sslow = sma(close, slow).iloc[-1]
    if sfast > sslow * 1.0006:
        return "BUY"
    if sfast < sslow * 0.9994:
        return "SELL"
    return "NEUTRAL"

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

# ---------------- SIGNAL GENERATOR ----------------
def generate_signal(df: pd.DataFrame, symbol: str, context=None):
    if context is None:
        context = {}
    tf = context.get("tf","15m")

    if df is None or len(df) < 6:
        return None

    last = float(df["close"].iloc[-1])

    ob_type, ob_hi, ob_lo = detect_order_blocks(df)
    if ob_type is None:
        return None

    bull_fvg, bear_fvg = detect_fvg(df)
    sweep_h, sweep_l = detect_sweep(df)
    bos_hh, bos_ll = detect_bos_mss(df)

    if not strong_sweep_or_bos(df):
        return None

    score = 0
    reasons = []

    if ob_type=="bullish": score+=2; reasons.append("OB Bull +2")
    else: score+=2; reasons.append("OB Bear +2")

    if bull_fvg: score+=2; reasons.append("FVG Bull +2")
    elif bear_fvg: score+=2; reasons.append("FVG Bear +2")

    score+=2; reasons.append("BOS +2")
    if sweep_h or sweep_l: score+=1; reasons.append("Sweep +1")
    else: reasons.append("No Sweep +0")

    wick_ratio = wick_dominance(df, lookback=12)
    if wick_ratio >= WICK_DOMINANCE_REJECT:
        reasons.append(f"WickDominant:{wick_ratio:.2f}")
        return None
    else:
        reasons.append(f"WickRatio:{wick_ratio:.2f}")

    side = "BUY" if ob_type=="bullish" else "SELL"

    btc_trend = context.get("btc_trend", "NEUTRAL")
    btc_choppy = context.get("btc_choppy", None)
    if tf in HTF_ALIGNMENT_SET and btc_trend in ("BUY","SELL"):
        if side != btc_trend:
            reasons.append(f"HTF_MISALIGN:{side}!={btc_trend}")
            return None
        else:
            reasons.append(f"HTF_ALIGN:{side}=={btc_trend}")

    if btc_choppy is not None:
        if btc_choppy >= CHOPPINESS_REJECT:
            reasons.append(f"BTC_Choppy:{btc_choppy:.1f}")
            return None
        else:
            reasons.append(f"BTC_Choppy:{btc_choppy:.1f}")

    atr_val = None
    df15 = context.get("df_15m")
    if df15 is not None and len(df15)>=10:
        try:
            atr_series = atr(df15,14)
            if len(atr_series)>0:
                atr_val = float(atr_series.iloc[-1])
        except Exception:
            atr_val = None

    entry = float(last)
    tp_mult, sl_mult = 0.8, 1.0
    if atr_val and atr_val > 0:
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

    if atr_val and atr_val > 0:
        sl_distance = abs(entry - sl)
        min_allowed = max(MIN_SL_ATR_MULT * atr_val, atr_val * 0.25)
        if sl_distance < min_allowed:
            if side == "BUY":
                new_sl = entry - min_allowed
            else:
                new_sl = entry + min_allowed
            sl = float(new_sl)
            reasons.append(f"SL_widened_to_at_least_{min_allowed:.6f}")
    else:
        min_pct = 0.0015
        sl_distance = abs(entry - sl)
        if sl_distance < entry * min_pct:
            if side == "BUY":
                sl = entry - entry * min_pct
            else:
                sl = entry + entry * min_pct
            reasons.append(f"SL_widened_pct_{min_pct}")

    if score < MIN_SCORE_TO_SIGNAL:
        reasons.append(f"ScoreTooLow:{score}")
        return None

    return {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "sl": float(sl),
        "tp1": float(tp1),
        "tp2": float(tp2),
        "tp3": float(tp3),
        "score": score,
        "reason": "Set B SMC Signal (upgraded filters)",
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
                        try:
                            ticker = await safe_call(exchange.fetch_ticker, symbol)
                        except Exception:
                            ticker = None
                        if not ticker:
                            continue
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
last_signal_time = {}
async def scan_loop(exchange):
    while True:
        t0=time.time()
        try:
            # --- prefetch BTC 15m for HTF alignment and choppiness once per scan ---
            btc_trend = "NEUTRAL"
            btc_choppy = None
            try:
                btc_15_ohlcv = await fetch_ohlcv(exchange, BTC_PAIR, "15m", 200)
                if btc_15_ohlcv:
                    df_btc15 = pd.DataFrame(btc_15_ohlcv, columns=["ts","open","high","low","close","vol"])
                    for c in ["open","high","low","close","vol"]: df_btc15[c]=pd.to_numeric(df_btc15[c],errors="coerce")
                    btc_trend = compute_htf_trend(df_btc15, fast=5, slow=20)
                    btc_choppy = choppiness_index(df_btc15, period=14)
            except Exception as e:
                log.debug("BTC prefetch/trend failed: %s", e)

            tickers = await safe_call(exchange.fetch_tickers)
            if not tickers:
                log.warning("fetch_tickers failed/timeout; sleeping briefly")
                await asyncio.sleep(1)
                continue

            # choose top by quoteVolume robustly
            tv = []
            for s, v in tickers.items():
                try:
                    qv = v.get("quoteVolume") or v.get("quoteVolume24h") or v.get("baseVolume") or 0
                    tv.append((s, float(qv)))
                except Exception:
                    continue
            top = sorted([x for x in tv if isinstance(x[1], (int, float)) and str(x[0]).endswith("USDT")], key=lambda x:x[1], reverse=True)[:TOP_N]

            for symbol,_ in top:
                if deprioritized(symbol): 
                    log.debug(f"Symbol {symbol} deprioritized due to SL cluster")
                    continue
                ohlcvs={}
                for tf in TIMEFRAMES:
                    key=f"{symbol}:{tf}"
                    if key in last_signal_time and time.time()-last_signal_time[key]<1800:
                        continue
                    ohlcv = await fetch_ohlcv(exchange,symbol,tf,200)
                    if not ohlcv:
                        continue
                    df=pd.DataFrame(ohlcv,columns=["ts","open","high","low","close","vol"])
                    for c in ["open","high","low","close","vol"]: df[c]=pd.to_numeric(df[c],errors="coerce")
                    ohlcvs[tf]=df
                    context={"tf":tf,"df_15m":ohlcvs.get("15m"),"df_1h":ohlcvs.get("1h"),
                             "btc_trend": btc_trend, "btc_choppy": btc_choppy}
                    if tf in ("1m","3m","5m"):
                        if "15m" not in ohlcvs: 
                            ohlcv15 = await fetch_ohlcv(exchange,symbol,"15m",200)
                            if ohlcv15: ohlcvs["15m"]=pd.DataFrame(ohlcv15,columns=["ts","open","high","low","close","vol"])
                        if "1h" not in ohlcvs:
                            ohlcv1h = await fetch_ohlcv(exchange,symbol,"1h",200)
                            if ohlcv1h: ohlcvs["1h"]=pd.DataFrame(ohlcv1h,columns=["ts","open","high","low","close","vol"])
                        context["df_15m"]=ohlcvs.get("15m"); context["df_1h"]=ohlcvs.get("1h")
                    sig = generate_signal(df,symbol,context)
                    if sig:
                        await tg(f"🚀 {sig['symbol']} ({tf}) {sig['side']}\nEntry:{sig['entry']}\nSL:{sig['sl']}\nTP1:{sig['tp1']} TP2:{sig['tp2']} TP3:{sig['tp3']}\nScore:{sig['score']}\nBreakdown:{', '.join(sig['reason_list'])}")
                        await log_signal(sig)
                        last_signal_time[key]=time.time()
        except Exception as e:
            log.exception("scan error: %s", e)
            await tg(f"❌ Scan error: {e}")
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
    exchange = ccxt.okx({"enableRateLimit": True})

    # start each loop as dedicated task to avoid event-loop starvation
    asyncio.create_task(scan_loop(exchange))
    # small sleep to allow loop scheduling fairness
    await asyncio.sleep(0.8)
    asyncio.create_task(monitor_signals(exchange))

    # keep main alive; optional heartbeat or maintenance can be added
    while True:
        await asyncio.sleep(3600)

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
        except KeyboardInterrupt:
            log.info("Shutdown requested")