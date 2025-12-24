#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER - LIQUIDITY PRIMARY, SWING FALLBACK
- PRIMARY TP: Liquidity target (Code 1 logic)
- FALLBACK TP: Swing high/low if no liquidity found
- Enhanced features from Code 2
- WORKING TP/SL Telegram alerts
"""

import os, time, asyncio, logging, datetime
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from collections import defaultdict, deque
import numpy as np

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))
TOP_N = int(os.getenv("TOP_N", 60))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
MIN_SCORE = 5
CRITICAL_FACTORS_MIN = 2

# ---------------- FORCED FILTER PARAMETERS ----------------
MOMENTUM_STRONG_THRESHOLD = 0.70
MOMENTUM_GOOD_THRESHOLD = 0.65
DISPLACEMENT_MIN_THRESHOLD = 0.60

# ---------------- OB DISTANCE FILTER PARAMETERS ----------------
OB_DISTANCE_MAX_THRESHOLD = 0.70
OB_DISTANCE_OPTIMAL_MAX = 0.50

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_bot")
db_lock = asyncio.Lock()
db_conn = None
exchange = None

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
            tp REAL,
            timestamp TEXT,
            status TEXT,
            reason TEXT,
            score INTEGER,
            tp_hit INTEGER DEFAULT 0,
            latest_ob TEXT,
            tp_type TEXT,
            tp_locked INTEGER DEFAULT 1,
            ob_type TEXT,
            sweep_type TEXT,
            momentum_value REAL,
            displacement_value REAL,
            ob_distance_pct REAL,
            ob_distance_filter TEXT,
            tp_source TEXT
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

# ---------------- FORCED FILTER FUNCTION ----------------
def force_filter_trade(momentum_value: float, displacement_value: float) -> bool:
    if momentum_value >= MOMENTUM_STRONG_THRESHOLD:
        return True
    if momentum_value >= MOMENTUM_GOOD_THRESHOLD and displacement_value >= DISPLACEMENT_MIN_THRESHOLD:
        return True
    return False

# ---------------- OB DISTANCE FILTER FUNCTION ----------------
def check_ob_distance_filter(entry_price: float, ob_midpoint: float, ob_low: float = None, ob_high: float = None) -> dict:
    if ob_midpoint is None or entry_price == 0:
        return {"passed": False, "distance_pct": 100, "distance_abs": 0, "status": "NO_OB", "quality": "REJECTED"}
    
    distance_abs = abs(entry_price - ob_midpoint)
    distance_pct = (distance_abs / entry_price) * 100
    
    if distance_pct <= OB_DISTANCE_OPTIMAL_MAX:
        quality = "PREMIUM"
        passed = True
    elif distance_pct <= OB_DISTANCE_MAX_THRESHOLD:
        quality = "GOOD"
        passed = True
    else:
        quality = "EXTENDED"
        passed = False
    
    return {
        "passed": passed,
        "distance_pct": distance_pct,
        "distance_abs": distance_abs,
        "status": "PASS" if passed else "FAIL",
        "quality": quality
    }

# ================ LIQUIDITY PRIMARY, SWING FALLBACK TP LOGIC ================

def romeopt_market_state(df, atr_val):
    """FROM CODE 1: Market state detection"""
    if len(df) < 3:
        return "BALANCED"
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    body_ratio = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    candle_size = last["high"] - last["low"]
    price_movement = abs(last["close"] - prev["close"])
    
    strong_displacement = (
        body_ratio > 0.7 and
        candle_size > atr_val * 1.2 and
        price_movement > atr_val * 0.5
    )
    
    return "IMBALANCED" if strong_displacement else "BALANCED"

def romeopt_internal_liquidity(df, side, atr_val, lookback=15):
    """FROM CODE 1: Internal liquidity detection"""
    if side == "SELL":
        lows = df['low'].iloc[-lookback:].dropna()
        if len(lows) < 5:
            return None
        
        tolerance = atr_val * 0.15
        potential_targets = []
        for i in range(len(lows)):
            current_low = lows.iloc[i]
            nearby_count = (abs(lows - current_low) <= tolerance).sum()
            if nearby_count >= 2:
                potential_targets.append((current_low, nearby_count))
        
        if potential_targets:
            best_target = min(potential_targets, key=lambda x: x[0])[0]
            return best_target
        
    else:  # BUY
        highs = df['high'].iloc[-lookback:].dropna()
        if len(highs) < 5:
            return None
        
        tolerance = atr_val * 0.15
        potential_targets = []
        
        for i in range(len(highs)):
            current_high = highs.iloc[i]
            nearby_count = (abs(highs - current_high) <= tolerance).sum()
            if nearby_count >= 2:
                potential_targets.append((current_high, nearby_count))
        
        if potential_targets:
            best_target = max(potential_targets, key=lambda x: x[0])[0]
            return best_target
    
    return None

def romeopt_external_liquidity(df, side, lookback=50):
    """FROM CODE 1: External liquidity detection"""
    if side == "SELL":
        return df['low'].iloc[-lookback:].min()
    else:  # BUY
        return df['high'].iloc[-lookback:].max()

def find_swing_high_low(df, side, lookback=20):
    """
    FALLBACK TP: Find swing high/low
    For BUY: Find recent swing high
    For SELL: Find recent swing low
    """
    if len(df) < lookback:
        return None
    
    if side == "BUY":
        # Find swing highs (peak detection)
        highs = df['high'].iloc[-lookback:].values
        swing_highs = []
        
        for i in range(1, len(highs)-1):
            if highs[i] > highs[i-1] and highs[i] > highs[i+1]:
                swing_highs.append(highs[i])
        
        if swing_highs:
            # Get the most recent swing high
            return max(swing_highs)
        else:
            # Fallback to highest high in lookback
            return df['high'].iloc[-lookback:].max()
    
    else:  # SELL
        # Find swing lows (trough detection)
        lows = df['low'].iloc[-lookback:].values
        swing_lows = []
        
        for i in range(1, len(lows)-1):
            if lows[i] < lows[i-1] and lows[i] < lows[i+1]:
                swing_lows.append(lows[i])
        
        if swing_lows:
            # Get the most recent swing low
            return min(swing_lows)
        else:
            # Fallback to lowest low in lookback
            return df['low'].iloc[-lookback:].min()

def romeopt_tp_sl_with_fallback(entry, side, atr_val, ob_zone, df):
    """
    PRIMARY: Liquidity target (Code 1 logic)
    FALLBACK: Swing high/low if no liquidity found
    """
    market_state = romeopt_market_state(df, atr_val)
    tp_source = "UNKNOWN"
    
    # ================ PRIMARY: TRY LIQUIDITY TARGET ================
    tp = None
    tp_type = ""
    
    if market_state == "BALANCED":
        tp = romeopt_internal_liquidity(df, side, atr_val)
        if tp:
            tp_type = f"LIQUIDITY_RANGE: Visual {'Lows' if side == 'SELL' else 'Highs'} Cluster"
            tp_source = "LIQUIDITY"
    else:  # IMBALANCED
        tp = romeopt_external_liquidity(df, side)
        if tp:
            tp_type = f"LIQUIDITY_TREND: Range {'Low' if side == 'SELL' else 'High'}"
            tp_source = "LIQUIDITY"
    
    # ================ FALLBACK: SWING HIGH/LOW ================
    if tp is None:
        # No liquidity found, use swing high/low
        swing_tp = find_swing_high_low(df, side)
        if swing_tp:
            tp = swing_tp
            tp_type = f"SWING_{'HIGH' if side == 'BUY' else 'LOW'}: Recent {'Swing High' if side == 'BUY' else 'Swing Low'}"
            tp_source = "SWING"
            log.info(f"⚠️ No liquidity found, using fallback swing {'high' if side == 'BUY' else 'low'}: {tp:.6f}")
        else:
            log.debug(f"❌ No liquidity OR swing found for {side}")
            return None
    
    # Safety check - reject if recently swept (only for liquidity targets)
    if tp_source == "LIQUIDITY":
        recent_candles = min(10, len(df))
        if side == "SELL":
            recent_touch = any(
                abs(df['low'].iloc[-i] - tp) <= atr_val * 0.1
                for i in range(1, recent_candles)
            )
        else:  # BUY
            recent_touch = any(
                abs(df['high'].iloc[-i] - tp) <= atr_val * 0.1
                for i in range(1, recent_candles)
            )
        
        if recent_touch:
            log.debug(f"❌ Liquidity recently swept for {side} at {tp}")
            # Try swing fallback
            swing_tp = find_swing_high_low(df, side)
            if swing_tp:
                tp = swing_tp
                tp_type = f"SWING_{'HIGH' if side == 'BUY' else 'LOW'}_FALLBACK"
                tp_source = "SWING_FALLBACK"
                log.info(f"⚠️ Liquidity swept, using swing fallback: {tp:.6f}")
            else:
                return None
    
    # ================ CALCULATE SL ================
    if side == "BUY":
        sl = ob_zone["low"] - (atr_val * 0.3)
        recent_low = df['low'].iloc[-10:].min()
        sl = min(sl, recent_low - (atr_val * 0.3))
        
        min_risk = atr_val * 0.5
        risk = entry - sl
        if risk < min_risk:
            risk = min_risk
            sl = entry - risk
        
        # Ensure TP is valid (above entry, at least 0.5R)
        if tp <= entry:
            log.debug(f"❌ TP {tp} not above entry {entry} for BUY")
            return None
            
        reward = tp - entry
        if reward < risk * 0.5:
            log.debug(f"❌ TP reward {reward/risk:.2f}R < 0.5R minimum")
            return None
        
    else:  # SELL
        sl = ob_zone["high"] + (atr_val * 0.3)
        recent_high = df['high'].iloc[-10:].max()
        sl = max(sl, recent_high + (atr_val * 0.3))
        
        min_risk = atr_val * 0.5
        risk = sl - entry
        if risk < min_risk:
            risk = min_risk
            sl = entry + risk
        
        # Ensure TP is valid (below entry, at least 0.5R)
        if tp >= entry:
            log.debug(f"❌ TP {tp} not below entry {entry} for SELL")
            return None
            
        reward = entry - tp
        if reward < risk * 0.5:
            log.debug(f"❌ TP reward {reward/risk:.2f}R < 0.5R minimum")
            return None
    
    log.info(f"✅ {side} {entry:.6f} | TP Source: {tp_source}")
    log.info(f"   SL: {sl:.6f} | TP: {tp:.6f} | Type: {tp_type}")
    log.info(f"   Risk: {risk:.6f} | R:R: {abs(tp-entry)/risk:.2f}:1")
    
    return sl, tp, tp_type, tp_source

# ================ END TP LOGIC ================

# ---------------- ELITE TF ALIGNMENT ----------------
async def elite_tf_alignment(exchange, symbol: str, side: str):
    tfs = ["15m","1h","4h"]
    for tf in tfs:
        ohlcv = await fetch_ohlcv(exchange, symbol, tf, 50)
        if not ohlcv or len(ohlcv) < 10: return False
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
        if len(df) < 5: return False
        trend = df["close"].iloc[-1] - df["close"].iloc[-5]
        trend_side = "BUY" if trend>0 else "SELL"
        if trend_side != side:
            return False
    return True

# ---------------- SIGNAL GENERATION ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    if df is None or len(df) < 20: return None
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    score = 0
    reasons = []
    
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
    ob_midpoint = None
    
    for i in range(len(df)-5, len(df)-1):
        candle, prev_candle = df.iloc[i], df.iloc[i-1]
        if candle["close"]>candle["open"] and prev_candle["close"]<prev_candle["open"]:
            ob_zone={"type":"bullish","low":min(candle["low"], prev_candle["low"]),"high":candle["close"]}
            ob_midpoint = (ob_zone["low"] + ob_zone["high"]) / 2
            break
        elif candle["close"]<candle["open"] and prev_candle["close"]>prev_candle["open"]:
            ob_zone={"type":"bearish","low":candle["close"],"high":max(candle["high"], prev_candle["high"])}
            ob_midpoint = (ob_zone["low"] + ob_zone["high"]) / 2
            break

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
        calc_values["ob_midpoint"] = ob_midpoint
    else:
        reasons.append("Zone Approach +0")
        ob_type = None
        calc_values["zone_approach"] = 0
        calc_values["ob_type"] = "NONE"
        calc_values["ob_midpoint"] = None

    # Step 5: HTF Alignment
    tf_map={"1m":"15m","3m":"30m","5m":"1h","15m":"4h","30m":"1h"}
    htf=tf_map.get(tf,"15m")
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, htf, 50)
    htf_alignment = 0
    if ohlcv_htf and len(ohlcv_htf) >= 5:
        df_htf = pd.DataFrame(ohlcv_htf, columns=["ts","open","high","low","close","vol"])
        if len(df_htf) >= 5:
            trend = df_htf["close"].iloc[-1] - df_htf["close"].iloc[-5]
            htf_dir = "bullish" if trend>0 else "bearish"
            if ob_type and htf_dir==ob_type:
                score+=1; htf_alignment=1; reasons.append(f"HTF Alignment +1 ({htf_dir} {trend:+.6f})")
            else:
                reasons.append(f"HTF Alignment +0 ({htf_dir} {trend:+.6f})")
            calc_values["htf_trend"] = round(trend, 6)
            calc_values["htf_direction"] = htf_dir
        else:
            reasons.append("HTF Alignment ? (insufficient data)")
            calc_values["htf_trend"] = 0
            calc_values["htf_direction"] = "UNKNOWN"
    else:
        reasons.append("HTF Alignment ? (no data)")
        calc_values["htf_trend"] = 0
        calc_values["htf_direction"] = "UNKNOWN"

    # Step 6: Momentum
    momentum_ratio = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    calc_values["momentum_value"] = round(momentum_ratio, 2)
    
    if ob_type=="bullish" and momentum_ratio>=0.8 and last["close"]>last["open"]:
        score+=1; reasons.append(f"Momentum +1 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 1
    elif ob_type=="bearish" and momentum_ratio>=0.8 and last["close"]<last["open"]:
        score+=1; reasons.append(f"Momentum +1 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 1
    else:
        reasons.append(f"Momentum +0 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 0

    if not ob_type: return None
    side_str = "BUY" if ob_type=="bullish" else "SELL"
    entry = float(last["close"])

    # ---------------- CRITICAL FILTERS ----------------
    critical_score = htf_alignment + liquidity_sweep
    if critical_score < CRITICAL_FACTORS_MIN: return None
    if score < MIN_SCORE: return None
    if not has_disp: return None
    if htf_alignment != 1: return None

    # ---------------- FORCED FILTER ----------------
    displacement_val = calc_values["displacement_value"]
    momentum_val = calc_values["momentum_value"]
    
    if not force_filter_trade(momentum_val, displacement_val):
        reasons.append(f"❌ FORCED FILTER REJECTED: Mom={momentum_val:.2f}, Disp={displacement_val:.2f}")
        return None
    
    filter_reason = "Mom≥0.70" if momentum_val >= MOMENTUM_STRONG_THRESHOLD else "Mom≥0.65 & Disp≥0.60"
    reasons.append(f"✅ FORCED FILTER PASSED: {filter_reason}")

    # ---------------- OB DISTANCE FILTER ----------------
    if ob_midpoint is not None:
        ob_distance_filter = check_ob_distance_filter(
            entry_price=entry,
            ob_midpoint=ob_midpoint,
            ob_low=ob_zone["low"] if ob_zone else None,
            ob_high=ob_zone["high"] if ob_zone else None
        )
        
        calc_values["ob_distance_filter"] = ob_distance_filter
        calc_values["ob_distance_pct"] = ob_distance_filter["distance_pct"]
        
        if not ob_distance_filter["passed"]:
            reasons.append(f"❌ OB DISTANCE FILTER REJECTED: {ob_distance_filter['distance_pct']:.2f}% > {OB_DISTANCE_MAX_THRESHOLD}%")
            calc_values["ob_distance_filter_status"] = "FAIL"
            return None
        
        reasons.append(f"✅ OB DISTANCE FILTER PASSED: {ob_distance_filter['distance_pct']:.2f}% ({ob_distance_filter['quality']})")
        calc_values["ob_distance_filter_status"] = "PASS"
    else:
        calc_values["ob_distance_filter"] = {"passed": False, "distance_pct": 100, "status": "NO_OB"}
        calc_values["ob_distance_pct"] = 100
        calc_values["ob_distance_filter_status"] = "NO_OB"

    # ---------------- ELITE MTF CONFIRMATION ----------------
    if not await elite_tf_alignment(exchange, symbol, side_str):
        return None
    reasons.append("Elite MTF Alignment ✅")

    # ---------------- LIQUIDITY PRIMARY, SWING FALLBACK TP CALCULATION ----------------
    atr_val = float(atr(df, 14).iloc[-1])
    result = romeopt_tp_sl_with_fallback(entry, side_str, atr_val, ob_zone, df)
    
    # REJECT if no valid TP found
    if result is None:
        reasons.append("❌ NO VALID TP FOUND (liquidity or swing)")
        return None
    
    sl, tp, tp_type, tp_source = result
    
    sig = {
        "symbol": symbol,
        "side": side_str,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "score": score,
        "reason": "RomeOPT 6-Step",
        "reason_list": reasons,
        "htf_alignment": htf_alignment,
        "liquidity_sweep": liquidity_sweep,
        "momentum_ratio": momentum_ratio,
        "calc_values": calc_values,
        "tp_type": tp_type,
        "tp_source": tp_source
    }
    
    # ---------------- FINAL FORCED VALIDATION ----------------
    if not force_filter_trade(momentum_val, displacement_val):
        log.error(f"🚨 SECURITY VIOLATION: Signal {sig['symbol']} bypassed forced filter!")
        return None
    
    log.info(f"✅ Signal {sig['symbol']} passed all filters: Mom={momentum_val:.2f}, Disp={displacement_val:.2f}, TP Source: {tp_source}")
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
        calc = sig.get("calc_values", {})
        ob_distance_filter = calc.get("ob_distance_filter", {})
        
        await db_conn.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp,timestamp,status,reason,score,tp_hit,latest_ob,tp_type,tp_locked,ob_type,sweep_type,momentum_value,displacement_value,ob_distance_pct,ob_distance_filter,tp_source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            sig["symbol"],
            sig["side"],
            sig["entry"],
            sig.get("sl"),
            sig.get("tp"),
            datetime.datetime.utcnow().isoformat(),
            "OPEN",
            sig["reason"],
            sig["score"],
            0,
            str(sig.get("latest_ob","")),
            sig.get("tp_type", ""),
            1,
            calc.get("ob_type", ""),
            calc.get("sweep_type", ""),
            calc.get("momentum_value", 0),
            calc.get("displacement_value", 0),
            calc.get("ob_distance_pct", 0),
            ob_distance_filter.get("status", "UNKNOWN"),
            sig.get("tp_source", "UNKNOWN")
        ))
        await db_conn.commit()

# ---------------- MONITOR SIGNALS WITH WORKING ALERTS ----------------
async def monitor_signals():
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("SELECT id,symbol,side,entry,sl,tp,tp_hit,status FROM signals WHERE status='OPEN'") as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp, tp_hit, status = row
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get("last")
                        if last_price is None: continue

                        hits=[]; sl_hit=False; new_tp_hit = tp_hit
                        
                        if side=="BUY":
                            if not tp_hit and tp is not None and last_price>=tp: 
                                hits.append("TP"); new_tp_hit = 1
                            if sl is not None and last_price<=sl: 
                                hits.append("SL"); status="CLOSED"; sl_hit=True
                        else:
                            if not tp_hit and tp is not None and last_price<=tp: 
                                hits.append("TP"); new_tp_hit = 1
                            if sl is not None and last_price>=sl: 
                                hits.append("SL"); status="CLOSED"; sl_hit=True

                        # FIXED: Send Telegram alert when TP/SL is hit
                        if hits:
                            # Get additional signal info
                            async with db_conn.execute("SELECT tp_type,tp_source FROM signals WHERE id=?", (sig_id,)) as cursor2:
                                extra_info = await cursor2.fetchone()
                                tp_type = extra_info[0] if extra_info else ""
                                tp_source = extra_info[1] if extra_info else ""
                            
                            # Format alert message
                            current_time = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
                            
                            if "SL" in hits:
                                loss = abs(last_price - entry)
                                loss_pct = abs((last_price - entry) / entry * 100)
                                alert_msg = f"""
🚨 **STOP LOSS HIT** 🚨

{symbol} {side}
Entry: {entry:.6f}
SL: {sl:.6f}
Last Price: {last_price:.6f}
Loss: {loss:.6f} ({loss_pct:.2f}%)
TP Type: {tp_type}
TP Source: {tp_source}
Time: {current_time}
                                """
                            elif "TP" in hits:
                                profit = last_price - entry if side == "BUY" else entry - last_price
                                profit_pct = (profit / entry * 100)
                                alert_msg = f"""
🎉 **TAKE PROFIT HIT** 🎉

{symbol} {side}
Entry: {entry:.6f}
TP: {tp:.6f}
Last Price: {last_price:.6f}
Profit: {profit:.6f} ({profit_pct:.2f}%)
TP Type: {tp_type}
TP Source: {tp_source}
Time: {current_time}
                                """
                            
                            await tg(alert_msg)
                            log.info(f"📢 Alert sent for {symbol}: {hits}")

                        if sl_hit: record_sl_hit(symbol)
                        
                        # Update database if something changed
                        if new_tp_hit != tp_hit or status != "OPEN":
                            await db_conn.execute("UPDATE signals SET tp_hit=?,status=? WHERE id=?",
                                                 (new_tp_hit, status, sig_id))
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
                        calc = sig.get("calc_values", {})
                        momentum_val = calc.get("momentum_value", 0)
                        displacement_val = calc.get("displacement_value", 0)
                        ob_distance_pct = calc.get("ob_distance_pct", 100)
                        ob_filter_status = calc.get("ob_distance_filter_status", "UNKNOWN")
                        tp_source = sig.get("tp_source", "UNKNOWN")
                        
                        filter_passed = force_filter_trade(momentum_val, displacement_val)
                        
                        # Calculate risk/reward
                        risk = abs(sig['entry'] - sig['sl'])
                        reward = abs(sig['tp'] - sig['entry'])
                        rr = reward / risk if risk > 0 else 0
                        
                        # Enhanced breakdown
                        breakdown_lines = [
                            f"🏆 {sig['symbol']} ({tf}) {sig['side']}",
                            f"Entry: {sig['entry']:.6f} | Score: {sig['score']}/6",
                            f"",
                            f"📊 CORE METRICS:",
                            f"• Displacement: {displacement_val:.2f}",
                            f"• Momentum: {momentum_val:.2f}",
                            f"• HTF: {calc.get('htf_direction', '?')}",
                            f"• Sweep: {calc.get('sweep_type', 'NONE')}",
                            f"• OB Type: {calc.get('ob_type', 'NONE')}",
                            f"",
                            f"📏 OB DISTANCE FILTER:",
                            f"• Status: {ob_filter_status}",
                            f"• Distance: {ob_distance_pct:.2f}%",
                            f"• Quality: {calc.get('ob_distance_filter', {}).get('quality', 'N/A')}",
                            f"",
                            f"🔒 FORCED FILTER:",
                            f"• {'✅ PASS' if filter_passed else '❌ FAIL'}",
                            f"• Momentum ≥ {MOMENTUM_STRONG_THRESHOLD if momentum_val >= MOMENTUM_STRONG_THRESHOLD else MOMENTUM_GOOD_THRESHOLD}",
                            f"• Displacement ≥ {DISPLACEMENT_MIN_THRESHOLD}",
                            f"",
                            f"🎯 TP STRATEGY:",
                            f"• Source: {tp_source}",
                            f"• Type: {sig.get('tp_type', 'N/A')}",
                            f"• SL: {sig.get('sl'):.6f}",
                            f"• TP: {sig.get('tp'):.6f}",
                            f"• R:R: {rr:.2f}:1",
                            f"• Risk: {risk:.6f}",
                            f"",
                            f"💎 ROMEOPT PHILOSOPHY:",
                            f"Primary: Liquidity target",
                            f"Fallback: Swing {'high' if sig['side'] == 'BUY' else 'low'}"
                        ]
                        
                        # Highlight if using fallback
                        if "SWING" in tp_source:
                            breakdown_lines.insert(1, f"⚠️ USING FALLBACK TP STRATEGY")
                        
                        await tg("\n".join(breakdown_lines))
                        await log_signal(sig)
                        last_signal_time[key]=time.time()
                        signals_found+=1
            
            log.info(f"📊 Scan complete: {signals_found} RomeOPT signals (Liquidity Primary, Swing Fallback)")
        
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
    global exchange
    await init_db()
    
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"}
    })
    
    # Start announcement
    await tg("🏆 ROMEOPT 6-STEP SCANNER STARTED")
    await tg("🎯 TP STRATEGY: Liquidity Primary, Swing Fallback")
    await tg("  1. PRIMARY: Find liquidity target (Code 1 logic)")
    await tg("  2. FALLBACK: Swing high/low if no liquidity found")
    await tg("📊 ENHANCED: OB Distance Filter + Detailed Breakdown")
    await tg("🔒 FORCED FILTER: Momentum ≥ 0.70 OR (≥0.65 & Disp≥0.60)")
    await tg("📏 OB DISTANCE: ≤ 0.70% required")
    await tg("📢 ALERTS: TP/SL alerts enabled")
    await tg("💎 PHILOSOPHY: Always find a target")
    
    # Start main loops
    await asyncio.gather(
        scan_loop(exchange),
        monitor_signals()
    )

if __name__=="__main__":
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--http", action="store_true", help="Run HTTP server")
    args=p.parse_args()
    
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=9000)
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Bot stopped by user")
        finally:
            if db_conn:
                asyncio.run(db_conn.close())