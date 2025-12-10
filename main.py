#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER (Enhanced + Elite Features) - WITH CODE 2 TP/SL SYSTEM
- Fully live early signals
- RomeOPT 6-step logic
- Uses Code 2's RomeOPT-P TP/SL system (0.8R/1.6R, tight SL)
- Telegram alerts
- Async SQLite logging
- Filters: Score >=5, Displacement +2, Sweep+2 OR Zone+1, avoid counter-trend
- Improved Order Block detection
- Adaptive Market Regime detection
- HTF + Sweep scoring threshold
- Elite multi-timeframe confirmation (15m,1h,4h)
- 🎯 MOMENTUM FILTER: 0.8 threshold (was 0.5)
- 📊 ENHANCED BREAKDOWN: Shows all numerical values
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
TOP_N = int(os.getenv("TOP_N", 60))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
MIN_SCORE = 5
CRITICAL_FACTORS_MIN = 2  # HTF Alignment + Liquidity Sweep minimum

# ===== ADDED FROM CODE 2: TP/SL CONFIG =====
# Timeframe mapping for TP scaling (RomeOPT-P logic)
TP_TIMEFRAME_MAP = {
    "1m": "5m",    # 1m → 5m ATR (5×) - less aggressive
    "3m": "15m",   # 3m → 15m ATR (5×)
    "5m": "15m",   # 5m → 15m ATR (3×) - conservative
    "15m": "1h",   # 15m → 1h ATR (4×)
    "30m": "1h"    # 30m → 1h ATR (2×) - minimal scaling
}

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_bot")
db_lock = asyncio.Lock()
db_conn = None
exchange = None  # Global exchange instance

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
            latest_ob TEXT,
            entry_tf TEXT DEFAULT '',   -- Added for TP/SL system
            tp_tf TEXT DEFAULT ''       -- Added for TP/SL system
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

# ===== ADDED FROM CODE 2: ROMEO-P TP/SL SYSTEM =====
async def romeoptp_tp_sl(exchange, entry: float, side: str, entry_tf: str, ob_zone: dict, symbol: str):
    """
    RomeOPT-P Logic from Code 2:
    - SL based on entry timeframe OB (tight)
    - TP scaled to higher timeframe ATR (meaningful)
    - TP1 = 0.8R, TP2 = 1.6R
    - TP3 is not used (as per Code 2)
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
    
    # Calculate SL based on entry timeframe OB (tight) - Code 2's approach
    if side == "BUY":
        # SL just below bullish OB low
        sl = ob_zone['low'] - (atr_val * 0.1)  # Very tight (0.1 × HTF ATR)
        risk = entry - sl
        # TP scaled to HTF ATR - Code 2's RomeOPT-P logic
        tp1 = entry + (risk * 0.8)  # 0.8R
        tp2 = entry + (risk * 1.6)  # 1.6R
    else:  # SELL
        # SL just above bearish OB high  
        sl = ob_zone['high'] + (atr_val * 0.1)  # Very tight (0.1 × HTF ATR)
        risk = sl - entry
        # TP scaled to HTF ATR
        tp1 = entry - (risk * 0.8)  # 0.8R
        tp2 = entry - (risk * 1.6)  # 1.6R
    
    # Code 2 doesn't use TP3, but we'll keep it for compatibility
    tp3 = None
    
    return sl, tp1, tp2, tp3, tp_tf

def find_latest_ob(df: pd.DataFrame):
    for i in range(len(df)-5, len(df)-1):
        candle, prev_candle = df.iloc[i], df.iloc[i-1]
        if candle["close"]>candle["open"] and prev_candle["close"]<prev_candle["open"]:
            return {"type":"bullish","low":min(candle["low"], prev_candle["low"]),"high":candle["close"]}
        elif candle["close"]<candle["open"] and prev_candle["close"]>prev_candle["open"]:
            return {"type":"bearish","low":candle["close"],"high":max(candle["high"], prev_candle["high"])}
    return None

# ===== MODIFIED: UPDATE TP/SL WITH CODE 2'S SYSTEM =====
async def update_tp_sl_live(sig: dict, df: pd.DataFrame):
    """Update TP/SL with Code 2's RomeOPT-P system"""
    latest_ob = find_latest_ob(df)
    if not latest_ob: 
        # If no OB found, fallback to basic TP/SL
        entry = sig["entry"]
        side = sig["side"]
        atr_val = float(atr(df, 14).iloc[-1])
        
        if side == "BUY":
            sig["sl"] = entry * 0.99  # 1% stop loss
            sig["tp1"] = entry * 1.01  # 1% take profit 1
            sig["tp2"] = entry * 1.02  # 2% take profit 2
            sig["tp3"] = entry * 1.03  # 3% take profit 3
        else:
            sig["sl"] = entry * 1.01  # 1% stop loss
            sig["tp1"] = entry * 0.99  # 1% take profit 1
            sig["tp2"] = entry * 0.98  # 2% take profit 2
            sig["tp3"] = entry * 0.97  # 3% take profit 3
        sig["latest_ob"] = "basic"
        sig["tp_tf"] = sig.get("entry_tf", "")
        return sig
    
    # Use Code 2's RomeOPT-P TP/SL system
    entry = sig["entry"]
    side = sig["side"]
    entry_tf = sig.get("entry_tf", "15m")  # Default if not set
    
    sl, tp1, tp2, tp3, tp_tf = await romeoptp_tp_sl(
        exchange, entry, side, entry_tf, latest_ob, sig["symbol"]
    )
    
    if sl is not None and tp1 is not None and tp2 is not None:
        sig["sl"] = sl
        sig["tp1"] = tp1
        sig["tp2"] = tp2
        sig["tp3"] = tp3  # May be None (Code 2 doesn't use TP3)
        sig["latest_ob"] = latest_ob
        sig["tp_tf"] = tp_tf
    else:
        # Fallback if calculation fails
        atr_val = float(atr(df, 14).iloc[-1])
        if side == "BUY":
            sig["sl"] = entry * 0.99
            sig["tp1"] = entry * 1.01
            sig["tp2"] = entry * 1.02
            sig["tp3"] = entry * 1.03
        else:
            sig["sl"] = entry * 1.01
            sig["tp1"] = entry * 0.99
            sig["tp2"] = entry * 0.98
            sig["tp3"] = entry * 0.97
        sig["latest_ob"] = latest_ob
        sig["tp_tf"] = sig.get("entry_tf", "")
    
    return sig

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

# ---------------- ROMEOPT 6-STEP SIGNAL ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    if df is None or len(df) < 20: return None
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    score = 0
    reasons = []
    
    # Store all calculation values for breakdown
    calc_values = {}

    # Step 1: Liquidity Sweep
    sweep_high = last["high"] > prev5["high"].max()
    sweep_low = last["low"] < prev5["low"].min()
    has_sweep = sweep_high or sweep_low
    liquidity_sweep = 2 if has_sweep else 0
    score += liquidity_sweep
    sweep_type = "HIGH" if sweep_high else ("LOW" if sweep_low else "NONE")
    reasons.append(f"Liquidity Sweep +{liquidity_sweep}")
    calc_values["sweep_type"] = sweep_type
    calc_values["sweep_score"] = liquidity_sweep

    # Step 2: Displacement
    displacement = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    calc_values["displacement_value"] = round(displacement, 2)
    has_disp = displacement > 0.6
    if has_disp:
        score += 2; reasons.append(f"Displacement +2 ({displacement:.2f})")
    else:
        reasons.append(f"Displacement +0 ({displacement:.2f})")

    # Step 3 & 4: Order Block & Zone
    ob_zone = None
    for i in range(len(df)-5, len(df)-1):
        candle, prev_candle = df.iloc[i], df.iloc[i-1]
        if candle["close"]>candle["open"] and prev_candle["close"]<prev_candle["open"]:
            ob_zone={"type":"bullish","low":min(candle["low"], prev_candle["low"]),"high":candle["close"]}; break
        elif candle["close"]<candle["open"] and prev_candle["close"]>prev_candle["open"]:
            ob_zone={"type":"bearish","low":candle["close"],"high":max(candle["high"], prev_candle["high"])}; break

    if ob_zone:
        ob_type = ob_zone["type"]
        zone_approach = 0
        if ob_type=="bullish" and last["close"] <= ob_zone["high"]: 
            score+=1; zone_approach=1; reasons.append("Zone Approach +1")
        elif ob_type=="bearish" and last["close"] >= ob_zone["low"]: 
            score+=1; zone_approach=1; reasons.append("Zone Approach +1")
        else: 
            reasons.append("Zone Approach +0")
        calc_values["zone_approach"] = zone_approach
        calc_values["ob_type"] = ob_type
        calc_values["ob_low"] = round(ob_zone["low"], 6)
        calc_values["ob_high"] = round(ob_zone["high"], 6)
    else:
        reasons.append("Zone Approach +0"); ob_type=None
        calc_values["zone_approach"] = 0
        calc_values["ob_type"] = "NONE"

    # Step 5: HTF Alignment
    tf_map={"1m":"15m","3m":"30m","5m":"1h","15m":"4h","30m":"1h"}
    htf=tf_map.get(tf,"15m")
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, htf, 50)
    htf_alignment = 0
    htf_trend_value = 0
    if ohlcv_htf:
        df_htf = pd.DataFrame(ohlcv_htf, columns=["ts","open","high","low","close","vol"])
        trend = df_htf["close"].iloc[-1] - df_htf["close"].iloc[-5]
        htf_trend_value = round(trend, 6)
        htf_dir = "bullish" if trend>0 else "bearish"
        if ob_type and htf_dir==ob_type:
            score+=1; htf_alignment=1; reasons.append(f"HTF Alignment +1 ({htf_dir} {trend:+.6f})")
        else:
            reasons.append(f"HTF Alignment +0 ({htf_dir} {trend:+.6f})")
        calc_values["htf_trend"] = htf_trend_value
        calc_values["htf_direction"] = htf_dir
    else:
        reasons.append("HTF Alignment ?")
        calc_values["htf_trend"] = 0
        calc_values["htf_direction"] = "UNKNOWN"

    # 🎯 STEP 6: MOMENTUM (CHANGED FROM 0.5 to 0.8 THRESHOLD)
    momentum_ratio = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    calc_values["momentum_value"] = round(momentum_ratio, 2)
    
    if ob_type=="bullish" and momentum_ratio>=0.8 and last["close"]>last["open"]:  # CHANGED >0.5 to >=0.8
        score+=1; reasons.append(f"Momentum +1 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 1
    elif ob_type=="bearish" and momentum_ratio>=0.8 and last["close"]<last["open"]:  # CHANGED >0.5 to >=0.8
        score+=1; reasons.append(f"Momentum +1 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 1
    else:
        reasons.append(f"Momentum +0 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 0

    if not ob_type: return None
    side = "BUY" if ob_type=="bullish" else "SELL"
    entry = float(last["close"])

    # ---------------- CRITICAL FILTERS ----------------
    critical_score = htf_alignment + liquidity_sweep
    if critical_score < CRITICAL_FACTORS_MIN: return None
    if score < MIN_SCORE: return None
    if not has_disp: return None
    
    # ---------------- NEW: HTF ALIGNMENT MANDATORY FILTER ----------------
    if htf_alignment != 1:  # MUST HAVE HTF Alignment = 1
        return None

    market_regime = await detect_market_regime(df)
    if (market_regime=="BULL" and side=="SELL") or (market_regime=="BEAR" and side=="BUY"): return None

    trend_ma = df["close"].rolling(20).mean().iloc[-1]
    if (side=="BUY" and last["close"]<trend_ma) or (side=="SELL" and last["close"]>trend_ma): return None

    # ---------------- ELITE MTF CONFIRMATION ----------------
    if not await elite_tf_alignment(exchange, symbol, side):
        return None
    reasons.append("Elite MTF Alignment ✅")

    sig = {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "score": score,
        "reason": "RomeOPT 6-Step",
        "reason_list": reasons,
        "htf_alignment": htf_alignment,
        "liquidity_sweep": liquidity_sweep,
        "momentum_ratio": momentum_ratio,  # Store actual momentum value
        "calc_values": calc_values,  # Store all calculation values
        "entry_tf": tf  # Added for TP/SL system
    }
    
    # ===== MODIFIED: Use Code 2's TP/SL system =====
    sig = await update_tp_sl_live(sig, df)
    
    # ---------------- TP1 DISTANCE FILTER (KEPT FROM ORIGINAL) ----------------
    if sig and "sl" in sig and "tp1" in sig:
        risk = abs(sig["entry"] - sig["sl"])
        tp1_distance = abs(sig["tp1"] - sig["entry"])
        
        # Reject if TP1 is less than 10% of risk (meaningless profit)
        if tp1_distance < risk * 0.1:
            return None
    
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
        # Ensure tp3 is always a valid float (not None)
        tp3 = sig.get("tp3")
        if tp3 is None:
            # Code 2 doesn't use TP3, so we'll calculate a conservative one
            entry = sig["entry"]
            if sig["side"] == "BUY":
                tp3 = entry * 1.02  # 2% above entry as default (conservative)
            else:
                tp3 = entry * 0.98  # 2% below entry as default (conservative)
        
        await db_conn.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score,latest_ob,entry_tf,tp_tf)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            sig["symbol"], sig["side"], sig["entry"], sig.get("sl"), sig.get("tp1"), sig.get("tp2"), tp3,
            datetime.datetime.utcnow().isoformat(), "OPEN", sig["reason"], sig["score"], 
            str(sig.get("latest_ob","")), sig.get("entry_tf", ""), sig.get("tp_tf", "")
        ))
        await db_conn.commit()

# ===== MODIFIED: MONITOR SIGNALS WITH CODE 2's UPDATES =====
async def monitor_signals():
    global exchange
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("SELECT id,symbol,side,entry,sl,tp1,tp2,tp3,tp1_hit,tp2_hit,tp3_hit,status,entry_tf FROM signals WHERE status='OPEN'") as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status, entry_tf = row
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get("last")
                        if last_price is None: continue

                        # Update TP/SL levels with current market data using Code 2's system
                        ohlcv = await fetch_ohlcv(exchange, symbol, "1m", 50)
                        if ohlcv:
                            df_live = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                            for c in ["open","high","low","close","vol"]: 
                                df_live[c] = pd.to_numeric(df_live[c], errors="coerce")
                            
                            sig = {
                                "symbol": symbol, "side": side, "entry": entry, 
                                "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
                                "entry_tf": entry_tf if entry_tf else "15m"
                            }
                            
                            # Use Code 2's update logic
                            sig = await update_tp_sl_live(sig, df_live)
                            sl, tp1, tp2, tp3 = sig["sl"], sig["tp1"], sig["tp2"], sig["tp3"]

                        hits=[]; sl_hit=False
                        # ===== MODIFIED: Code 2's TP logic (TP1→breakeven, no TP3) =====
                        if side=="BUY":
                            if not tp1_hit and tp1 is not None and last_price>=tp1: 
                                hits.append("TP1")
                                tp1_hit=1
                                sl=entry  # Move SL to breakeven after TP1 (Code 2 logic)
                            if not tp2_hit and tp2 is not None and last_price>=tp2: 
                                hits.append("TP2")
                                tp2_hit=1
                                # Code 2 closes trade after TP2 (no TP3)
                                status="CLOSED"
                            if sl is not None and last_price<=sl: 
                                hits.append("SL")
                                status="CLOSED"
                                sl_hit=True
                        else:
                            if not tp1_hit and tp1 is not None and last_price<=tp1: 
                                hits.append("TP1")
                                tp1_hit=1
                                sl=entry  # Move SL to breakeven after TP1
                            if not tp2_hit and tp2 is not None and last_price<=tp2: 
                                hits.append("TP2")
                                tp2_hit=1
                                # Code 2 closes trade after TP2
                                status="CLOSED"
                            if sl is not None and last_price>=sl: 
                                hits.append("SL")
                                status="CLOSED"
                                sl_hit=True

                        if hits:
                            await tg(f"🎯 {symbol} {side} update\nEntry:{entry}\nLast:{last_price}\nHits:{','.join(hits)}\nSL:{sl}\nTP1:{tp1} TP2:{tp2} TP3:{tp3 if tp3 else 'N/A'}")

                        if sl_hit: record_sl_hit(symbol)
                        await db_conn.execute(
                            "UPDATE signals SET tp1_hit=?,tp2_hit=?,tp3_hit=?,sl=?,status=? WHERE id=?",
                            (tp1_hit, tp2_hit, tp3_hit, sl, status, sig_id)
                        )
                await db_conn.commit()
        except Exception as e: 
            log.exception("monitor error: %s", e)
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
                        # ENHANCED BREAKDOWN FORMAT WITH TP/SL INFO
                        calc = sig.get("calc_values", {})
                        breakdown_lines = [
                            f"🏆 {sig['symbol']} ({tf}) {sig['side']}",
                            f"Entry: {sig['entry']:.6f}",
                            f"Score: {sig['score']}/6",
                            f"TP System: RomeOPT-P (Code 2)",
                            f"",
                            f"📊 BREAKDOWN VALUES:",
                            f"• Sweep: {calc.get('sweep_type', 'NONE')} (+{calc.get('sweep_score', 0)})",
                            f"• Displacement: {calc.get('displacement_value', 0):.2f}",
                            f"• OB Type: {calc.get('ob_type', 'NONE')}",
                            f"• Zone Approach: +{calc.get('zone_approach', 0)}",
                            f"• HTF: {calc.get('htf_direction', '?')} ({calc.get('htf_trend', 0):+.6f})",
                            f"• Momentum: {calc.get('momentum_value', 0):.2f} (+{calc.get('momentum_score', 0)})",
                            f"",
                            f"🎯 TARGETS (RomeOPT-P System):",
                            f"Entry TF: {sig.get('entry_tf', '?')}",
                            f"TP TF: {sig.get('tp_tf', '?')}",
                            f"SL: {sig.get('sl'):.6f}",
                            f"TP1 (0.8R): {sig.get('tp1'):.6f} → SL to breakeven",
                            f"TP2 (1.6R): {sig.get('tp2'):.6f} → Close trade",
                            f"TP3: {sig.get('tp3', 'N/A'):.6f if sig.get('tp3') else 'N/A'}",
                            f"",
                            f"💎 MOMENTUM: {sig.get('momentum_ratio', 0):.2f} {'✅ PASS' if sig.get('momentum_ratio', 0) >= 0.8 else '❌ FAIL'}"
                        ]
                        
                        await tg("\n".join(breakdown_lines))
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
    global exchange
    await init_db()
    exchange = ccxt.okx({"enableRateLimit": True})
    await tg("🏆 ROMEOPT 6-Step Scanner Started - Live Early Signals")
    await tg("🎯 MOMENTUM FILTER: 0.8 threshold activated (was 0.5)")
    await tg("📊 ENHANCED BREAKDOWN: All values now visible")
    await tg("🎯 TP/SL SYSTEM: Using Code 2's RomeOPT-P logic")
    await tg("   - TP1: 0.8R → SL to breakeven")
    await tg("   - TP2: 1.6R → Close trade")
    await tg("   - Tight SL: 0.1×HTF ATR")
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