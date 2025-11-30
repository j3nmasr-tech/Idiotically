#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER
- Fully live early signals
- RomeOPT 6-step logic
- TP/SL tracking with ATR or OB
- Telegram alerts
- Async SQLite logging
- Filters: Score >=5, Displacement +2, Sweep+2 OR Zone+1, avoid counter-trend
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

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))  # fast scan
TOP_N = int(os.getenv("TOP_N", 20))
TIMEFRAMES = ["1m", "3m", "5m"]
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 3600))

# Filters
MIN_SCORE = 5

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_bot")
db_lock = asyncio.Lock()
db_conn = None  # global SQLite connection

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
            tp3_hit INTEGER DEFAULT 0
        );
    """)
    await db_conn.commit()

# ---------------- OHLCV ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except:
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
def generate_signal_romeopt(df: pd.DataFrame, symbol: str):
    if df is None or len(df) < 6: return None
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]

    score = 0
    reasons = []

    # Step 1: Early Liquidity Sweep
    sweep_high = last["high"] > prev5["high"].max()
    sweep_low = last["low"] < prev5["low"].min()
    if sweep_high or sweep_low:
        score += 2
        reasons.append("Liquidity Sweep +2")
        has_sweep = True
    else:
        reasons.append("No Sweep +0")
        has_sweep = False

    # Step 2: Early Displacement (momentum candle)
    displacement = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    if displacement > 0.6:
        score += 2
        reasons.append("Displacement +2")
        has_disp = True
    else:
        reasons.append("No Displacement +0")
        has_disp = False

    # Step 3: Early Zone Approach (approaching OB)
    ob_type = None
    ob_hi, ob_lo = last["high"], last["low"]
    candle = df.iloc[-3]
    if candle["close"] > candle["open"]:
        ob_type = "bullish"
        ob_lo = candle["low"]
        ob_hi = candle["open"]
    else:
        ob_type = "bearish"
        ob_lo = candle["open"]
        ob_hi = candle["high"]

    if ob_type == "bullish" and last["close"] <= ob_hi:
        score += 1; reasons.append("Zone Approach +1"); has_zone = True
    elif ob_type == "bearish" and last["close"] >= ob_lo:
        score += 1; reasons.append("Zone Approach +1"); has_zone = True
    else:
        reasons.append("Zone Approach +0"); has_zone = False

    # Step 4: Relaxed Premium/Discount
    range_high = df["high"].tail(10).max()
    range_low = df["low"].tail(10).min()
    pos = (last["close"] - range_low) / (range_high - range_low + 1e-8)
    if ob_type=="bullish" and pos < 0.6:
        score +=1; reasons.append("Premium/Discount +1")
    elif ob_type=="bearish" and pos > 0.4:
        score +=1; reasons.append("Premium/Discount +1")
    else:
        reasons.append("Premium/Discount +0")

    # Step 5: Relaxed HTF Alignment
    score +=1; reasons.append("HTF Relaxed +1")

    # Step 6: Early Momentum
    if ob_type=="bullish" and last["close"] > last["open"]:
        score +=1; reasons.append("Momentum +1")
    elif ob_type=="bearish" and last["close"] < last["open"]:
        score +=1; reasons.append("Momentum +1")
    else:
        reasons.append("Momentum +0")

    side = "BUY" if ob_type=="bullish" else "SELL"

    # ---------------- Apply Winning Filters ----------------
    # 1. Score >= MIN_SCORE
    if score < MIN_SCORE: return None
    # 2. Must have Displacement +2
    if not has_disp: return None
    # 3. Must have Liquidity Sweep +2 OR Zone Approach +1
    if not (has_sweep or has_zone): return None
    # 4. Avoid counter-trend signals (example: check against simple MA)
    # For simplicity assume market trend check: bullish if last close > rolling mean
    trend = df["close"].rolling(20).mean().iloc[-1]
    if (side=="BUY" and last["close"] < trend) or (side=="SELL" and last["close"] > trend):
        return None

    # TP/SL calculation
    atr_val = float(atr(df,14).iloc[-1])
    entry = float(last["close"])
    if side=="BUY":
        sl = entry - atr_val
        tp1 = entry + 0.8*atr_val
        tp2 = entry + 1.5*atr_val
        tp3 = entry + 2.5*atr_val
    else:
        sl = entry + atr_val
        tp1 = entry - 0.8*atr_val
        tp2 = entry - 1.5*atr_val
        tp3 = entry - 2.5*atr_val

    return {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "score": score,
        "reason": "RomeOPT 6-Step",
        "reason_list": reasons
    }

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
            INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (sig["symbol"],sig["side"],sig["entry"],sig["sl"],sig["tp1"],sig["tp2"],sig["tp3"],
              datetime.datetime.utcnow().isoformat(),"OPEN",sig["reason"],sig["score"]))
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
                    sig = generate_signal_romeopt(df,symbol)
                    if sig:
                        await tg(f"🏆 {sig['symbol']} ({tf}) {sig['side']}\nEntry:{sig['entry']}\nSL:{sig['sl']}\nTP1:{sig['tp1']} TP2:{sig['tp2']} TP3:{sig['tp3']}\nScore:{sig['score']}\nBreakdown:{', '.join(sig['reason_list'])}")
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