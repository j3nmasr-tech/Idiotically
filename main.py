#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Production-ready Modular SMC Scanner (Set B)
- Async CCXT (OKX)
- Async SQLite
- Telegram notifications
- HTF trend + choppiness filtering
- Resilient to crashes and timeouts
"""

import os, time, asyncio, logging, datetime, math
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
from collections import defaultdict, deque

# ---------------- ENV ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = os.getenv("DB_PATH", "./data/signals.db")

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))
TOP_N = int(os.getenv("TOP_N", 10))
BTC_PAIR = os.getenv("BTC_PAIR", "BTC-USDT-SWAP")
TIMEFRAMES = ["1m","3m","5m","15m","30m"]

MIN_SCORE_TO_SIGNAL = int(os.getenv("MIN_SCORE_TO_SIGNAL", 5))
CHOPPINESS_REJECT = float(os.getenv("CHOPPINESS_REJECT", 61.0))
WICK_DOMINANCE_REJECT = float(os.getenv("WICK_DOMINANCE_REJECT", 1.2))
HTF_ALIGNMENT_ENFORCE = os.getenv("HTF_ALIGNMENT_ENFORCE", "15m")
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
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or not msg: return
    safe_msg = escape_html(msg)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID,"text":safe_msg,"parse_mode":"HTML"})
            if r.status_code != 200:
                log.warning(f"Telegram send failed: {r.status_code} | {r.text}")
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
        await db.commit()
    log.info("DB initialized")

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
    ob_type="bullish" if last>df["open"].iloc[-1] else "bearish"
    if ob_type is None: return None

    score=5
    side="BUY" if ob_type=="bullish" else "SELL"
    btc_trend=context.get("btc_trend","NEUTRAL")
    btc_choppy=context.get("btc_choppy",None)

    if tf in HTF_ALIGNMENT_SET and btc_trend in ("BUY","SELL"):
        if side!=btc_trend: return None
    if btc_choppy is not None and btc_choppy>=CHOPPINESS_REJECT: return None

    atr_val = float(atr(df,14).iloc[-1]) if len(df)>0 else 0.0
    entry=float(last)
    tp_mult, sl_mult = 0.8, 1.0
    if atr_val>0:
        if side=="BUY":
            sl=entry-sl_mult*atr_val; tp1=entry+tp_mult*atr_val; tp2=entry+tp_mult*1.5*atr_val; tp3=entry+tp_mult*2.5*atr_val
        else:
            sl=entry+sl_mult*atr_val; tp1=entry-tp_mult*atr_val; tp2=entry-tp_mult*1.5*atr_val; tp3=entry-tp_mult*2.5*atr_val
    else:
        sl=entry*0.998 if side=="BUY" else entry*1.002
        tp1=entry*1.004 if side=="BUY" else entry*0.996
        tp2=entry*1.008 if side=="BUY" else entry*0.992
        tp3=entry*1.012 if side=="BUY" else entry*0.988

    return {"symbol":symbol,"side":side,"entry":entry,"sl":sl,"tp1":tp1,"tp2":tp2,"tp3":tp3,"score":score,"reason_list":["SMC Signal"]}

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
            btc_trend = "NEUTRAL"
            btc_choppy = None
            df_btc15 = None
            try:
                btc_15_ohlcv = await fetch_ohlcv(exchange, BTC_PAIR, "15m", 200)
                if btc_15_ohlcv:
                    df_btc15 = pd.DataFrame(btc_15_ohlcv, columns=["ts","open","high","low","close","vol"])
                    for c in ["open","high","low","close","vol"]:
                        df_btc15[c] = pd.to_numeric(df_btc15[c], errors="coerce")
                    btc_trend = "BUY" if df_btc15["close"].iloc[-1]>df_btc15["close"].iloc[-2] else "SELL"
                    btc_choppy = choppiness_index(df_btc15)
            except Exception as e:
                log.debug("BTC prefetch failed: %s", e)

            tickers = await safe_call(exchange.fetch_tickers)
            if not tickers: 
                await asyncio.sleep(1)
                continue

            tv = [(s, float(v.get("quoteVolume") or 0)) for s,v in tickers.items() if str(s).endswith("USDT")]
            top = sorted(tv, key=lambda x:x[1], reverse=True)[:TOP_N]

            for symbol,_ in top:
                if deprioritized(symbol): continue

                for tf in TIMEFRAMES:
                    key = f"{symbol}:{tf}"
                    if key in last_signal_time and time.time() - last_signal_time[key] < 1800:
                        continue
                    ohlcv = await fetch_ohlcv(exchange, symbol, tf, 200)
                    if not ohlcv: continue
                    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                    for c in ["open","high","low","close","vol"]:
                        df[c] = pd.to_numeric(df[c], errors="coerce")

                    context = {"tf":tf,"df_15m":df_btc15,"btc_trend":btc_trend,"btc_choppy":btc_choppy}
                    sig = generate_signal(df, symbol, context)
                    if sig:
                        last_signal_time[key] = time.time()
                        await log_signal(sig)
                        await tg(f"🚀 {sig['symbol']} ({tf}) {sig['side']} Entry:{sig['entry']} SL:{sig['sl']} TP1:{sig['tp1']} TP2:{sig['tp2']} TP3:{sig['tp3']}")

        except Exception as e:
            log.exception("scan_loop error: %s", e)
        elapsed = time.time()-t0
        await asyncio.sleep(max(1, SCAN_INTERVAL-elapsed))

# ---------------- MAIN ----------------
async def main():
    await init_db()
    exchange = ccxt.okx({"enableRateLimit": True})
    try:
        scan_task = asyncio.create_task(scan_loop(exchange))
        monitor_task = asyncio.create_task(monitor_signals(exchange))
        log.info("Bot started")
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Shutdown requested")
    finally:
        scan_task.cancel(); monitor_task.cancel()
        await asyncio.gather(scan_task, monitor_task, return_exceptions=True)
        await exchange.close()
        await tg("⚡ Bot shutdown completed")
        log.info("Bot stopped")

# ---------------- RUN ----------------
if __name__ == "__main__":
    asyncio.run(main())