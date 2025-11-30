#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER
- Fully live early signals
- RomeOPT 6-step logic
- TP/SL tracking with ATR or OB
- Dynamic TP/SL updates
- Telegram alerts
- Async SQLite logging
- Filters: Score >=5, Displacement +2, Sweep+2 OR Zone+1, avoid counter-trend

MODIFIED: Enforced gating - only emit signals that have BOTH:
    HTF Alignment == +1  AND  Liquidity Sweep == +2
"""

import os, time, asyncio, logging, datetime
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
TOP_N = int(os.getenv("TOP_N", 10))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 3600))
MIN_SCORE = 5

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

# ---------------- DATABASE ----------------
async def init_db():
    global db_conn
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
            latest_ob TEXT
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

# ---------------- ROMEOPT 6-STEP SIGNAL ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    """
    Returns signal dict or None.
    NOTE: This function now enforces the gating:
        HTF Alignment == +1 AND Liquidity Sweep == +2
    """
    if df is None or len(df) < 20: return None
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    score = 0
    reasons = []

    # Step 1: Liquidity Sweep (existing simple logic)
    sweep_high = last["high"] > prev5["high"].max()
    sweep_low = last["low"] < prev5["low"].min()
    has_sweep = sweep_high or sweep_low
    liquidity_sweep = 2 if has_sweep else 0  # <-- ADDED: numeric sweep flag
    if has_sweep:
        score += 2
        reasons.append("Liquidity Sweep +2")
    else:
        reasons.append("No Sweep +0")

    # Step 2: Displacement
    displacement = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    has_disp = displacement > 0.6
    if has_disp:
        score += 2
        reasons.append("Displacement +2")
    else:
        reasons.append("No Displacement +0")

    # Step 3: Zone Approach
    ob_zone = None
    for i in range(len(df)-20, len(df)-1):
        candle, prev_candle = df.iloc[i], df.iloc[i-1]
        if candle["close"]>candle["open"] and prev_candle["close"]<prev_candle["open"]:
            ob_zone={"type":"bullish","low":candle["low"],"high":candle["close"]}; break
        elif candle["close"]<candle["open"] and prev_candle["close"]>prev_candle["open"]:
            ob_zone={"type":"bearish","low":candle["close"],"high":candle["high"]}; break

    has_zone=False
    ob_type = ob_zone["type"] if ob_zone else None
    if ob_zone:
        if ob_type=="bullish" and last["close"] <= ob_zone["high"]:
            score+=1; reasons.append("Zone Approach +1"); has_zone=True
        elif ob_type=="bearish" and last["close"] >= ob_zone["low"]:
            score+=1; reasons.append("Zone Approach +1"); has_zone=True
        else:
            reasons.append("Zone Approach +0")
    else:
        reasons.append("Zone Approach +0")

    # Step 4: Premium/Discount
    if ob_zone:
        if ob_type=="bullish" and last["close"]<ob_zone["high"]:
            score+=1; reasons.append("Premium/Discount +1")
        elif ob_type=="bearish" and last["close"]>ob_zone["low"]:
            score+=1; reasons.append("Premium/Discount +1")
        else:
            reasons.append("Premium/Discount +0")
    else:
        reasons.append("Premium/Discount +0")

    # Step 5: HTF Alignment
    tf_map={"1m":"15m","3m":"30m","5m":"1h","15m":"4h","30m":"1h"}
    htf=tf_map.get(tf,"15m")
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, htf, 50)
    htf_alignment = 0  # numeric flag 1 or 0
    if ohlcv_htf:
        df_htf = pd.DataFrame(ohlcv_htf, columns=["ts","open","high","low","close","vol"])
        # basic HTF trend detection: simple slope over last 5 candles
        trend = df_htf["close"].iloc[-1] - df_htf["close"].iloc[-5]
        htf_dir = "bullish" if trend>0 else "bearish"
        if ob_type and htf_dir==ob_type:
            score+=1
            htf_alignment = 1
            reasons.append("HTF Alignment +1")
        else:
            reasons.append("HTF Alignment +0")
    else:
        reasons.append("HTF Alignment ?")  # no HTF data available

    # Step 6: Momentum
    momentum_ratio = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    if ob_type=="bullish" and momentum_ratio>0.5 and last["close"]>last["open"]:
        score+=1; reasons.append("Momentum +1")
    elif ob_type=="bearish" and momentum_ratio>0.5 and last["close"]<last["open"]:
        score+=1; reasons.append("Momentum +1")
    else:
        reasons.append("Momentum +0")

    if not ob_type:
        # We require an Order Block (OB) to compute TP/SL etc. If missing, we bail.
        return None

    side = "BUY" if ob_type=="bullish" else "SELL"

    # Initial TP/SL (ATR-based)
    atr_val = float(atr(df,14).iloc[-1])
    entry = float(last["close"])
    sig = {
        "symbol":symbol, "side":side, "entry":entry,
        "score":score, "reason":"RomeOPT 6-Step",
        "reason_list":reasons,
        # expose flags for logging / downstream
        "htf_alignment": htf_alignment,
        "liquidity_sweep": liquidity_sweep
    }

    # --------- FINAL GATING: ENFORCE BOTH HTF ALIGNMENT AND LIQUIDITY SWEEP ----------
    # Only pass signals that have BOTH htf_alignment == 1 AND liquidity_sweep == 2
    if htf_alignment != 1:
        # suppressed due to HTF misalignment
        return None
    if liquidity_sweep != 2:
        # suppressed due to missing sweep
        return None
    # ------------------------------------------------------------------------------

    # Additional winning filters (kept for robustness)
    if score < MIN_SCORE: 
        return None
    if not has_disp:
        return None
    # Must have either sweep (we already enforced it) or zone (we already require ob_type)
    trend_ma = df["close"].rolling(20).mean().iloc[-1]
    if (side=="BUY" and last["close"]<trend_ma) or (side=="SELL" and last["close"]>trend_ma):
        return None

    sig = update_tp_sl_live(sig, df)
    return sig

# ---------------- TP/SL HELPERS ----------------
def romeopt_tp_sl(entry, side, atr_val, ob_zone):
    if side=="BUY":
        sl = min(entry - atr_val, ob_zone["low"])
        tp1 = entry + 0.8*atr_val
        tp2 = entry + 1.5*atr_val
        tp3 = entry + 2.5*atr_val
    else:
        sl = max(entry + atr_val, ob_zone["high"])
        tp1 = entry - 0.8*atr_val
        tp2 = entry - 1.5*atr_val
        tp3 = entry - 2.5*atr_val
    return sl,tp1,tp2,tp3

def find_latest_ob(df: pd.DataFrame):
    for i in range(len(df)-20, len(df)-1):
        candle, prev_candle = df.iloc[i], df.iloc[i-1]
        if candle["close"]>candle["open"] and prev_candle["close"]<prev_candle["open"]:
            return {"type":"bullish","low":candle["low"],"high":candle["close"]}
        elif candle["close"]<candle["open"] and prev_candle["close"]>prev_candle["open"]:
            return {"type":"bearish","low":candle["close"],"high":candle["high"]}
    return None

def update_tp_sl_live(sig: dict, df: pd.DataFrame):
    latest_ob = find_latest_ob(df)
    if not latest_ob: return sig
    atr_val = float(atr(df,14).iloc[-1])
    entry = sig["entry"]
    side = sig["side"]
    sl,tp1,tp2,tp3 = romeopt_tp_sl(entry, side, atr_val, latest_ob)
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

# ---------------- LOG SIGNAL ----------------
async def log_signal(sig):
    async with db_lock:
        await db_conn.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score,latest_ob)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (sig["symbol"],sig["side"],sig["entry"],sig.get("sl"),sig.get("tp1"),sig.get("tp2"),sig.get("tp3"),
              datetime.datetime.utcnow().isoformat(),"OPEN",sig["reason"],sig["score"],str(sig.get("latest_ob",""))))
        await db_conn.commit()

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("SELECT id,symbol,side,entry,sl,tp1,tp2,tp3,tp1_hit,tp2_hit,tp3_hit,status FROM signals WHERE status='OPEN'") as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status = row
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get("last")
                        if last_price is None: continue

                        # Update TP/SL dynamically
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
                            await tg(f"🎯 {symbol} {side} update\nEntry:{entry}\nLast:{last_price}\nHits:{','.join(hits)}\nSL:{sl}\nTP1:{tp1} TP2:{tp2} TP3:{tp3}")

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
                        # Include HTF and Sweep flags in alert (if present)
                        htf_flag = sig.get("htf_alignment", "N/A")
                        sweep_flag = sig.get("liquidity_sweep", "N/A")
                        await tg(f"🏆 {sig['symbol']} ({tf}) {sig['side']}\nEntry:{sig['entry']}\nSL:{sig.get('sl')}\nTP1:{sig.get('tp1')} TP2:{sig.get('tp2')} TP3:{sig.get('tp3')}\nScore:{sig['score']}\nHTF:{htf_flag} Sweep:{sweep_flag}\nBreakdown:{', '.join(sig['reason_list'])}")
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
            if db_conn:
                asyncio.run(db_conn.close())