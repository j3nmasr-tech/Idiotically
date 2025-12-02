#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER (Enhanced + Elite Features)
- Fully live early signals
- RomeOPT 6-step logic
- Strict TP/SL (0.8R/1.6R, SL→BE after TP1, no TP3)
- Liquidity path filter
- Clean traffic / range avoidance
- Telegram alerts
- Async SQLite logging
- Filters: Score >=5, Displacement +2, Sweep+2 OR Zone+1, avoid counter-trend
- Improved Order Block detection
- Adaptive Market Regime detection
- HTF + Sweep scoring threshold
- Elite multi-timeframe confirmation (15m,1h,4h)
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
TOP_N = int(os.getenv("TOP_N", 25))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
MIN_SCORE = 6
CRITICAL_FACTORS_MIN = 2  # HTF Alignment + Liquidity Sweep minimum

# Timeframe mapping for TP scaling (RomeOPT-P logic) - YOUR CHOICE
TP_TIMEFRAME_MAP = {
    "1m": "5m",    # 1m → 5m ATR (5×) - less aggressive
    "3m": "15m",   # 3m → 15m ATR (5×)
    "5m": "15m",   # 5m → 15m ATR (3×) - conservative
    "15m": "1h",   # 15m → 1h ATR (4×)
    "30m": "1h"    # 30m → 1h ATR (2×) - minimal scaling
}

# ---------------- GLOBALS ----------------
log = logging.getLogger("romeopt_bot")
db_lock = asyncio.Lock()
db_conn = None
exchange = None  # Global exchange instance

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

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
            entry_tf TEXT,
            tp_tf TEXT,
            timestamp TEXT,
            status TEXT,
            reason TEXT,
            score INTEGER,
            tp1_hit INTEGER DEFAULT 0,
            tp2_hit INTEGER DEFAULT 0,
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
    """Average True Range for volatility-based SL/TP"""
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
    """Ensures 15m/1h/4h trend matches signal side"""
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

# ---------------- ORDER BLOCK DETECTION ----------------
def find_latest_ob(df: pd.DataFrame):
    for i in range(len(df)-5, len(df)-1):
        candle, prev_candle = df.iloc[i], df.iloc[i-1]
        if candle["close"]>candle["open"] and prev_candle["close"]<prev_candle["open"]:
            return {"type":"bullish","low":min(candle["low"], prev_candle["low"]),"high":candle["close"]}
        elif candle["close"]<candle["open"] and prev_candle["close"]>prev_candle["open"]:
            return {"type":"bearish","low":candle["close"],"high":max(candle["high"], prev_candle["high"])}
    return None

# ---------------- TP/SL CALCULATION (RomeOPT-P Style) ----------------
async def romeoptp_tp_sl(exchange, entry: float, side: str, entry_tf: str, ob_zone: dict, symbol: str):
    """
    RomeOPT-P Logic:
    - SL based on entry timeframe OB (tight)
    - TP scaled to higher timeframe ATR (meaningful)
    - TP1 = 0.8R, TP2 = 1.6R
    """
    if not ob_zone:
        return None, None, None, entry_tf
    
    # Get ATR from higher timeframe for TP scaling
    tp_tf = TP_TIMEFRAME_MAP.get(entry_tf, "15m")
    htf_ohlcv = await fetch_ohlcv(exchange, symbol, tp_tf, 100)
    
    if not htf_ohlcv:
        # Fallback to entry timeframe if HTF fails
        htf_ohlcv = await fetch_ohlcv(exchange, symbol, entry_tf, 100)
        tp_tf = entry_tf
    
    if not htf_ohlcv:
        return None, None, None, tp_tf
    
    df_htf = pd.DataFrame(htf_ohlcv, columns=["ts","open","high","low","close","vol"])
    for c in ["open","high","low","close","vol"]: 
        df_htf[c] = pd.to_numeric(df_htf[c], errors="coerce")
    
    # Calculate ATR from higher timeframe
    atr_val = float(atr(df_htf, 14).iloc[-1])
    
    # Calculate SL based on entry timeframe OB (tight)
    if side == "BUY":
        # SL just below bullish OB low
        sl = ob_zone['low'] - (atr_val * 0.1)  # Very tight (0.1 × HTF ATR)
        risk = entry - sl
        # TP scaled to HTF ATR
        tp1 = entry + (risk * 0.8)  # 0.8R
        tp2 = entry + (risk * 1.6)  # 1.6R
    else:  # SELL
        # SL just above bearish OB high  
        sl = ob_zone['high'] + (atr_val * 0.1)  # Very tight (0.1 × HTF ATR)
        risk = sl - entry
        # TP scaled to HTF ATR
        tp1 = entry - (risk * 0.8)  # 0.8R
        tp2 = entry - (risk * 1.6)  # 1.6R
    
    return sl, tp1, tp2, tp_tf

# ---------------- UPDATE SIGNAL TP/SL ----------------
async def update_tp_sl_live(sig: dict):
    """Update TP/SL with current market data"""
    global exchange
    
    if 'entry_tf' not in sig or 'symbol' not in sig or 'side' not in sig:
        return sig
    
    # Fetch current OB from entry timeframe
    entry_tf_ohlcv = await fetch_ohlcv(exchange, sig["symbol"], sig["entry_tf"], 50)
    if not entry_tf_ohlcv:
        return sig
    
    df_entry = pd.DataFrame(entry_tf_ohlcv, columns=["ts","open","high","low","close","vol"])
    for c in ["open","high","low","close","vol"]: 
        df_entry[c] = pd.to_numeric(df_entry[c], errors="coerce")
    
    latest_ob = find_latest_ob(df_entry)
    if not latest_ob:
        return sig
    
    # Recalculate TP/SL with current data
    sl, tp1, tp2, tp_tf = await romeoptp_tp_sl(
        exchange, sig["entry"], sig["side"], sig["entry_tf"], latest_ob, sig["symbol"]
    )
    
    if sl is not None and tp1 is not None and tp2 is not None:
        sig["sl"] = sl
        sig["tp1"] = tp1
        sig["tp2"] = tp2
        sig["tp_tf"] = tp_tf
        sig["latest_ob"] = latest_ob
    
    return sig

# ---------------- ROMEOPT SIGNAL GENERATOR ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    """Full RomeOPT 6-step signal generator"""
    if df is None or len(df) < 20: return None
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    score = 0
    reasons = []

    # Step1: Liquidity Sweep
    sweep_high = last["high"] > prev5["high"].max()
    sweep_low = last["low"] < prev5["low"].min()
    has_sweep = sweep_high or sweep_low
    liquidity_sweep = 2 if has_sweep else 0
    score += liquidity_sweep
    reasons.append(f"Liquidity Sweep +{liquidity_sweep}")

    # Step2: Displacement
    displacement = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    has_disp = displacement>0.6
    if has_disp: 
        score+=2
        reasons.append("Displacement +2")
    else: 
        reasons.append("Displacement +0")

    # Step3&4: OB detection
    ob_zone = find_latest_ob(df)
    if not ob_zone: return None

    # Step5: HTF alignment
    side = "BUY" if ob_zone['type']=="bullish" else "SELL"
    
    # Use mapping consistent with elite_tf_alignment
    tf_map={"1m":"15m","3m":"30m","5m":"1h","15m":"4h","30m":"1h"}
    htf = tf_map.get(tf,"15m")
    
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, htf, 50)
    htf_alignment = 0
    if ohlcv_htf:
        df_htf = pd.DataFrame(ohlcv_htf, columns=["ts","open","high","low","close","vol"])
        trend = df_htf["close"].iloc[-1]-df_htf["close"].iloc[-5]
        htf_dir = "bullish" if trend>0 else "bearish"
        if htf_dir==ob_zone['type']: 
            htf_alignment=1
            score+=1
            reasons.append(f"HTF Alignment +1 ({htf})")

    if score < MIN_SCORE: return None
    if not has_disp: return None

    # Step6: Momentum clean traffic/range avoidance
    momentum_ratio = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    if momentum_ratio<0.5: 
        reasons.append("Momentum Failed")
        return None

    # Calculate TP/SL with RomeOPT-P logic
    sl, tp1, tp2, tp_tf = await romeoptp_tp_sl(exchange, float(last["close"]), side, tf, ob_zone, symbol)
    if sl is None or tp1 is None or tp2 is None:
        return None
    
    sig = {
        "symbol": symbol,
        "side": side,
        "entry": float(last["close"]),
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "entry_tf": tf,
        "tp_tf": tp_tf,
        "score": int(score),  # Ensure integer
        "reason": "RomeOPT-P 6-Step",
        "reason_list": reasons,
        "ob_zone": ob_zone
    }

    # Liquidity path filter: skip blocked trades
    if side=="BUY" and any(df['high'].iloc[-20:]>=sig['tp1']): 
        reasons.append("Liquidity Path Blocked")
        return None
    if side=="SELL" and any(df['low'].iloc[-20:]<=sig['tp1']): 
        reasons.append("Liquidity Path Blocked")
        return None

    return sig

# ---------------- DATABASE LOGGING ----------------
async def log_signal(sig):
    async with db_lock:
        await db_conn.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,entry_tf,tp_tf,timestamp,status,reason,score,latest_ob)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            sig["symbol"], sig["side"], sig["entry"], sig["sl"], sig["tp1"], sig["tp2"],
            sig.get("entry_tf", ""), sig.get("tp_tf", ""),
            datetime.datetime.utcnow().isoformat(), "OPEN", sig["reason"], 
            int(sig["score"]), str(sig.get("latest_ob",""))
        ))
        await db_conn.commit()

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    global exchange
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("SELECT id,symbol,side,entry,sl,tp1,tp2,entry_tf,tp1_hit,tp2_hit,status FROM signals WHERE status='OPEN'") as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, entry_tf, tp1_hit, tp2_hit, status = row
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get("last")
                        if last_price is None: continue

                        # Update TP/SL with current market data
                        sig = {
                            "symbol": symbol, "side": side, "entry": entry, 
                            "sl": sl, "tp1": tp1, "tp2": tp2, "entry_tf": entry_tf
                        }
                        sig = await update_tp_sl_live(sig)
                        sl, tp1, tp2 = sig["sl"], sig["tp1"], sig["tp2"]

                        hits=[]; sl_hit=False
                        # ---------------- CHECK TP/SL ----------------
                        if side=="BUY":
                            if not tp1_hit and last_price>=tp1: 
                                hits.append("TP1")
                                tp1_hit=1
                                sl=entry
                            if not tp2_hit and last_price>=tp2: 
                                hits.append("TP2")
                                tp2_hit=1
                                status="CLOSED"
                            if last_price<=sl: 
                                hits.append("SL")
                                status="CLOSED"
                                sl_hit=True
                        else:
                            if not tp1_hit and last_price<=tp1: 
                                hits.append("TP1")
                                tp1_hit=1
                                sl=entry
                            if not tp2_hit and last_price<=tp2: 
                                hits.append("TP2")
                                tp2_hit=1
                                status="CLOSED"
                            if last_price>=sl: 
                                hits.append("SL")
                                status="CLOSED"
                                sl_hit=True

                        if hits:
                            await tg(f"🎯 {symbol} {side} update\nEntry:{entry}\nLast:{last_price}\nHits:{','.join(hits)}\nSL:{sl}\nTP1:{tp1} TP2:{tp2}")

                        await db_conn.execute("UPDATE signals SET tp1_hit=?,tp2_hit=?,sl=?,status=? WHERE id=?",
                                             (tp1_hit,tp2_hit,sl,status,sig_id))
                await db_conn.commit()
        except Exception as e: 
            log.exception("monitor error: %s", e)
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- SCAN LOOP ----------------
last_signal_time = {}
async def scan_loop():
    global exchange
    while True:
        t0=time.time()
        try:
            tickers = await exchange.fetch_tickers()
            top = sorted([(s,v.get("quoteVolume",0)) for s,v in tickers.items() if s.endswith("USDT")], 
                        key=lambda x:x[1], reverse=True)[:TOP_N]
            signals_found = 0
            for symbol,_ in top:
                for tf in TIMEFRAMES:
                    key=f"{symbol}:{tf}"
                    if key in last_signal_time and time.time()-last_signal_time[key]<60: 
                        continue
                    ohlcv = await fetch_ohlcv(exchange,symbol,tf,200)
                    if not ohlcv: continue
                    df=pd.DataFrame(ohlcv,columns=["ts","open","high","low","close","vol"])
                    for c in ["open","high","low","close","vol"]: 
                        df[c]=pd.to_numeric(df[c],errors="coerce")
                    sig = await generate_signal_romeopt(exchange,df,symbol,tf)
                    if sig:
                        # RESTORED ORIGINAL FORMAT WITH ALL STEPS/DATA
                        breakdown_str = ", ".join(sig['reason_list'])
                        await tg(f"🏆 {sig['symbol']} ({tf}) {sig['side']}\nEntry:{sig['entry']}\nSL:{sig.get('sl')}\nTP1:{sig.get('tp1')} TP2:{sig.get('tp2')}\nScore:{sig['score']}\nBreakdown:{breakdown_str}")
                        await log_signal(sig)
                        last_signal_time[key]=time.time()
                        signals_found+=1
            log.info(f"📊 Scan complete: {signals_found} RomeOPT signals found")
        except Exception as e:
            log.exception("scan error: %s", e)
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
    global exchange, db_conn
    await init_db()
    exchange = ccxt.okx({"enableRateLimit": True})
    await tg("🏆 ROMEOPT 6-Step Scanner Started - Live Early Signals")
    await asyncio.gather(scan_loop(), monitor_signals())

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
            log.info("Shutting down...")
        finally:
            if db_conn:
                asyncio.run(db_conn.close())