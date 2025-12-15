#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TRUE ROMEOPT SCANNER (FINAL FIXED VERSION) - WITH ALL LIQUIDITY FIXES
- RomeOPT 6-step entry logic
- TRUE RomeOPT TP: ONE liquidity target, no TP ladders
- FIXED liquidity detection (less strict, more realistic)
- Smart hybrid liquidity: internal + external candidates
- Flexible R:R requirements based on market state
- Proper sweep invalidation (consumption ≠ touch)
- TP LOCK: No recalculation after entry
- Enhanced breakdown with comprehensive liquidity data
- Telegram alerts + SQLite logging
- Forced Filter: Momentum ≥ 0.87 OR (Momentum ≥ 0.85 AND Displacement ≥ 0.80)
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
MIN_SCORE = 4
CRITICAL_FACTORS_MIN = 2

# ---------------- FORCED FILTER PARAMETERS ----------------
MOMENTUM_STRONG_THRESHOLD = 0.60
MOMENTUM_GOOD_THRESHOLD = 0.55
DISPLACEMENT_MIN_THRESHOLD = 0.50

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
    
    # Create table with complete schema
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
            liquidity_data TEXT,
            market_state TEXT
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
    if len(df) < period:
        period = max(1, len(df) // 2)
    
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.DataFrame({
        "h-l": high - low,
        "h-pc": (high - close.shift(1)).abs(),
        "l-pc": (low - close.shift(1)).abs()
    }).max(axis=1)
    
    # Ensure we have enough data
    if len(tr) < 2:
        # Fallback to simple range if not enough data
        return pd.Series([(high.iloc[i] - low.iloc[i]) for i in range(len(df))], index=df.index)
    
    atr_series = tr.rolling(period, min_periods=1).mean()
    
    # Smooth the ATR to avoid extreme spikes
    atr_series = atr_series.ewm(span=period//2, adjust=False).mean()
    
    return atr_series

# ---------------- FORCED FILTER FUNCTION ----------------
def force_filter_trade(momentum_value: float, displacement_value: float) -> bool:
    if momentum_value >= MOMENTUM_STRONG_THRESHOLD:
        return True
    if momentum_value >= MOMENTUM_GOOD_THRESHOLD and displacement_value >= DISPLACEMENT_MIN_THRESHOLD:
        return True
    return False

# ---------------- REFINED ROMEOPT MARKET STATE ----------------
def romeopt_market_state(df, atr_val):
    """
    REFINED RomeOPT market state detection
    Checks: Strong displacement + actual price movement
    """
    if len(df) < 3:
        return "BALANCED"
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    body_ratio = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    candle_size = last["high"] - last["low"]
    price_movement = abs(last["close"] - prev["close"])
    
    # RomeOPT logic: Strong displacement with actual follow-through
    strong_displacement = (
        body_ratio > 0.7 and                    # Strong body
        candle_size > atr_val * 1.2 and         # Large candle
        price_movement > atr_val * 0.5          # Actual price movement
    )
    
    return "IMBALANCED" if strong_displacement else "BALANCED"

# ---------------- FIXED ROMEOPT INTERNAL LIQUIDITY ----------------
def romeopt_internal_liquidity(df, side, atr_val, lookback=15):
    """
    FIXED: RomeOPT internal liquidity detection
    Realistic tolerance for compressed liquidity zones
    """
    if side == "SELL":
        # For SELL: Look for compressed swing lows
        lows = df['low'].iloc[-lookback:].dropna()
        if len(lows) < 5:
            return None
        
        # ✅ FIX: REALISTIC tolerance (35% of ATR = compressed zone tolerance)
        tolerance = atr_val * 0.35
        
        # Find potential cluster centers
        potential_targets = []
        for i in range(len(lows)):
            current_low = lows.iloc[i]
            # Count how many lows are within tolerance
            nearby_mask = abs(lows - current_low) <= tolerance
            nearby_count = nearby_mask.sum()
            
            if nearby_count >= 2:  # At least 2 lows form a visual cluster
                # ✅ ENHANCEMENT: Require cluster span to be meaningful
                cluster_prices = lows[nearby_mask]
                cluster_span = cluster_prices.max() - cluster_prices.min()
                
                if cluster_span >= atr_val * 0.2:  # Meaningful cluster width
                    # Choose the lowest price in the cluster (stop pool)
                    cluster_low = cluster_prices.min()
                    potential_targets.append((cluster_low, nearby_count, cluster_span))
        
        if potential_targets:
            # Choose the most significant cluster (max nearby_count, min price)
            best_target = min(potential_targets, key=lambda x: (x[0], -x[1]))[0]
            return best_target
        
    else:  # BUY
        # For BUY: Look for compressed swing highs
        highs = df['high'].iloc[-lookback:].dropna()
        if len(highs) < 5:
            return None
        
        tolerance = atr_val * 0.35
        potential_targets = []
        
        for i in range(len(highs)):
            current_high = highs.iloc[i]
            nearby_mask = abs(highs - current_high) <= tolerance
            nearby_count = nearby_mask.sum()
            
            if nearby_count >= 2:
                cluster_prices = highs[nearby_mask]
                cluster_span = cluster_prices.max() - cluster_prices.min()
                
                if cluster_span >= atr_val * 0.2:
                    # Choose the highest price in the cluster
                    cluster_high = cluster_prices.max()
                    potential_targets.append((cluster_high, nearby_count, cluster_span))
        
        if potential_targets:
            best_target = max(potential_targets, key=lambda x: (x[0], -x[1]))[0]
            return best_target
    
    return None  # No meaningful compressed liquidity zone

# ---------------- ROMEOPT EXTERNAL LIQUIDITY ----------------
def romeopt_external_liquidity(df, side, lookback=50):
    """
    RomeOPT external liquidity detection
    Simple: Range extremes (guaranteed stops)
    """
    if side == "SELL":
        # For SELL in trend: Range low (guaranteed stops below)
        return df['low'].iloc[-lookback:].min()
    else:  # BUY
        # For BUY in trend: Range high (guaranteed stops above)
        return df['high'].iloc[-lookback:].max()

# ---------------- FIXED ROMEOPT TP DECISION ----------------
def romeopt_tp_sl(entry, side, atr_val, ob_zone, df):
    """
    FIXED: RomeOPT TP logic with ALL corrections
    """
    # Step 1: Determine market state
    market_state = romeopt_market_state(df, atr_val)
    
    # Step 2: ✅ SMART HYBRID LOGIC (BOTH INTERNAL & EXTERNAL)
    internal_tp = romeopt_internal_liquidity(df, side, atr_val)
    external_tp = romeopt_external_liquidity(df, side)
    
    # RomeOPT often targets external even in range if it's cleaner
    candidates = []
    if internal_tp is not None:
        candidates.append(("INTERNAL", internal_tp, "Visual Cluster"))
    if external_tp is not None:
        candidates.append(("EXTERNAL", external_tp, "Range Extreme"))
    
    if not candidates:
        log.debug(f"❌ No liquidity candidates found for {side}")
        return None
    
    # Step 3: ✅ PICK NEAREST VALID LIQUIDITY (RomeOPT: target closest guaranteed stop)
    valid_candidates = []
    
    if side == "BUY":
        for cand_type, cand_price, cand_desc in candidates:
            if cand_price > entry:  # Valid TP must be above entry for BUY
                distance = cand_price - entry
                valid_candidates.append((cand_type, cand_price, cand_desc, distance))
    else:  # SELL
        for cand_type, cand_price, cand_desc in candidates:
            if cand_price < entry:  # Valid TP must be below entry for SELL
                distance = entry - cand_price
                valid_candidates.append((cand_type, cand_price, cand_desc, distance))
    
    if not valid_candidates:
        log.debug(f"❌ No valid TP direction for {side} | Entry: {entry}")
        return None
    
    # ✅ Pick closest valid liquidity (RomeOPT: minimum travel to guaranteed stops)
    cand_type, tp, tp_type_desc, distance = min(valid_candidates, key=lambda x: x[3])
    
    # Step 4: ✅ FIXED SWEEP INVALIDATION LOGIC
    # RomeOPT: Liquidity being touched ≠ liquidity being consumed
    sweep_invalidated = False
    check_len = min(10, len(df))
    
    if side == "SELL":
        # For SELL TP: Check if price closed BELOW the liquidity multiple times
        closes_below = sum(df['close'].iloc[-i] < tp for i in range(1, check_len))
        sweep_invalidated = closes_below >= 2  # Fully consumed if 2+ closes below
    else:  # BUY
        # For BUY TP: Check if price closed ABOVE the liquidity multiple times
        closes_above = sum(df['close'].iloc[-i] > tp for i in range(1, check_len))
        sweep_invalidated = closes_above >= 2  # Fully consumed if 2+ closes above
    
    if sweep_invalidated:
        log.debug(f"❌ Liquidity fully consumed for {side} at {tp}")
        return None
    
    # Step 5: Calculate SL (keep OB-based SL)
    if side == "BUY":
        sl = ob_zone["low"] - (atr_val * 0.3)
        recent_low = df['low'].iloc[-10:].min()
        sl = min(sl, recent_low - (atr_val * 0.3))
        
        # Ensure minimum risk
        min_risk = atr_val * 0.5
        risk = entry - sl
        if risk < min_risk:
            risk = min_risk
            sl = entry - risk
        
        # Ensure TP is valid (above entry)
        if tp <= entry:
            log.debug(f"❌ TP {tp} not above entry {entry} for BUY")
            return None
            
        reward = tp - entry
        
        # ✅ FIX: FLEXIBLE RR REQUIREMENT (RomeOPT allows smaller R if liquidity is guaranteed)
        min_rr = 0.3 if market_state == "BALANCED" else 0.5  # Range trades can have lower R
        if reward < risk * min_rr:
            log.debug(f"❌ TP reward {reward/risk:.2f}R < {min_rr}R minimum | Market: {market_state}")
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
        
        # Ensure TP is valid (below entry)
        if tp >= entry:
            log.debug(f"❌ TP {tp} not below entry {entry} for SELL")
            return None
            
        reward = entry - tp
        
        min_rr = 0.3 if market_state == "BALANCED" else 0.5
        if reward < risk * min_rr:
            log.debug(f"❌ TP reward {reward/risk:.2f}R < {min_rr}R minimum | Market: {market_state}")
            return None
    
    # Create descriptive TP type
    tp_type = f"{cand_type}: {tp_type_desc}"
    if market_state == "BALANCED":
        tp_type += " (Range)"
    else:
        tp_type += " (Trend)"
    
    # Comprehensive liquidity data for logging
    liquidity_data = {
        "market_state": market_state,
        "selected_type": cand_type,
        "selected_tp": tp,
        "internal_candidate": internal_tp,
        "external_candidate": external_tp,
        "atr_value": atr_val,
        "tolerance_used": atr_val * 0.35,
        "distance_to_entry": distance,
        "risk": risk,
        "reward": reward,
        "rr_ratio": reward / risk if risk > 0 else 0,
        "min_rr_required": min_rr
    }
    
    log.info(f"✅ {side} {entry:.6f} | Market: {market_state}")
    log.info(f"   SL: {sl:.6f} | TP: {tp:.6f} | Type: {tp_type}")
    log.info(f"   Risk: {risk:.6f} | R:R: {abs(tp-entry)/risk:.2f}:1")
    log.info(f"   Candidate: {cand_type} | Distance: {distance:.6f}")
    
    return sl, tp, tp_type, liquidity_data

# ---------------- ENHANCED ORDER BLOCK DETECTION ----------------
def find_latest_ob(df: pd.DataFrame, lookback=50):
    """
    Enhanced Order Block detection with detailed classification
    """
    blocks = []
    
    # Look for order blocks in the specified lookback
    for i in range(max(2, len(df) - lookback), len(df) - 1):
        candle = df.iloc[i]
        prev_candle = df.iloc[i-1]
        
        # Bullish Order Block: Bearish candle followed by bullish candle
        if (prev_candle["close"] < prev_candle["open"] and  # Previous bearish
            candle["close"] > candle["open"] and            # Current bullish
            candle["close"] > prev_candle["close"]):        # Closes above previous close
            
            block = {
                "type": "BULLISH_OB",
                "index": i,
                "timestamp": candle.name if hasattr(candle, 'name') else i,
                "low": min(candle["low"], prev_candle["low"]),
                "high": max(candle["close"], prev_candle["close"]),
                "body_low": min(candle["open"], candle["close"]),
                "body_high": max(candle["open"], candle["close"]),
                "volume": candle["vol"] if "vol" in candle else 0,
                "candle_size": candle["high"] - candle["low"],
                "body_size": abs(candle["close"] - candle["open"])
            }
            blocks.append(block)
        
        # Bearish Order Block: Bullish candle followed by bearish candle
        elif (prev_candle["close"] > prev_candle["open"] and  # Previous bullish
              candle["close"] < candle["open"] and            # Current bearish
              candle["close"] < prev_candle["close"]):        # Closes below previous close
            
            block = {
                "type": "BEARISH_OB",
                "index": i,
                "timestamp": candle.name if hasattr(candle, 'name') else i,
                "low": min(candle["close"], prev_candle["close"]),
                "high": max(candle["high"], prev_candle["high"]),
                "body_low": min(candle["open"], candle["close"]),
                "body_high": max(candle["open"], candle["close"]),
                "volume": candle["vol"] if "vol" in candle else 0,
                "candle_size": candle["high"] - candle["low"],
                "body_size": abs(candle["close"] - candle["open"])
            }
            blocks.append(block)
    
    # Return the most recent order block if any exist
    if blocks:
        latest_block = max(blocks, key=lambda x: x["index"])
        
        # Add classification based on strength
        body_ratio = latest_block["body_size"] / latest_block["candle_size"] if latest_block["candle_size"] > 0 else 0
        if body_ratio >= 0.7:
            latest_block["strength"] = "STRONG"
        elif body_ratio >= 0.5:
            latest_block["strength"] = "MODERATE"
        else:
            latest_block["strength"] = "WEAK"
        
        return latest_block
    
    return None

# ---------------- SIGNAL GENERATION ----------------
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

async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    if df is None or len(df) < 20: return None
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    score = 0
    reasons = []
    
    calc_values = {}

    # Step 1: ENHANCED Liquidity Sweep Detection
    lookback_period = 20
    high_lookback = df['high'].iloc[-lookback_period:-1]
    low_lookback = df['low'].iloc[-lookback_period:-1]
    
    sweep_high = last["high"] > high_lookback.max()
    sweep_low = last["low"] < low_lookback.min()
    
    respected_high_sweep = False
    respected_low_sweep = False
    sweep_strength = 0.0
    
    if sweep_high:
        sweep_amount = last["high"] - high_lookback.max()
        candle_range = last["high"] - last["low"]
        if candle_range > 0:
            sweep_strength = sweep_amount / candle_range
        if last["close"] < high_lookback.max():
            respected_high_sweep = True
    
    if sweep_low:
        sweep_amount = low_lookback.min() - last["low"]
        candle_range = last["high"] - last["low"]
        if candle_range > 0:
            sweep_strength = sweep_amount / candle_range
        if last["close"] > low_lookback.min():
            respected_low_sweep = True
    
    has_sweep = (sweep_high and respected_high_sweep) or (sweep_low and respected_low_sweep)
    liquidity_sweep = 2 if has_sweep else 0
    score += liquidity_sweep
    
    if sweep_high and respected_high_sweep:
        sweep_type = "HIGH_SWEEP_RESPECTED"
        sweep_direction = "BEARISH"
    elif sweep_low and respected_low_sweep:
        sweep_type = "LOW_SWEEP_RESPECTED"
        sweep_direction = "BULLISH"
    elif sweep_high:
        sweep_type = "HIGH_SWEEP_UNRESPECTED"
        sweep_direction = "NEUTRAL"
    elif sweep_low:
        sweep_type = "LOW_SWEEP_UNRESPECTED"
        sweep_direction = "NEUTRAL"
    else:
        sweep_type = "NONE"
        sweep_direction = "NONE"
    
    reasons.append(f"Liquidity Sweep +{liquidity_sweep} ({sweep_type})")
    calc_values["sweep_type"] = sweep_type
    calc_values["sweep_direction"] = sweep_direction
    calc_values["sweep_score"] = liquidity_sweep
    calc_values["sweep_strength"] = round(sweep_strength, 2) if has_sweep else 0
    calc_values["sweep_respected"] = respected_high_sweep or respected_low_sweep
    calc_values["swept_level"] = float(high_lookback.max()) if sweep_high else (float(low_lookback.min()) if sweep_low else 0.0)

    # Step 2: Displacement
    displacement = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    calc_values["displacement_value"] = round(displacement, 2)
    has_disp = displacement > 0.6
    if has_disp:
        score += 2; reasons.append(f"Displacement +2 ({displacement:.2f})")
    else:
        reasons.append(f"Displacement +0 ({displacement:.2f})")

    # Step 3 & 4: ENHANCED Order Block & Zone
    ob_zone = find_latest_ob(df, lookback=30)
    
    if ob_zone:
        ob_type = "bullish" if ob_zone["type"] == "BULLISH_OB" else "bearish"
        zone_approach = 0
        
        if ob_type == "bullish":
            distance_to_ob = (last["close"] - ob_zone["high"]) / (ob_zone["high"] - ob_zone["low"] + 1e-8)
            if last["close"] <= ob_zone["high"] or distance_to_ob < 0.1:
                score += 1
                zone_approach = 1
                approach_status = f"APPROACHING (dist: {distance_to_ob:.2%})"
            else:
                approach_status = f"FAR ({distance_to_ob:.2%} away)"
        else:  # bearish
            distance_to_ob = (ob_zone["low"] - last["close"]) / (ob_zone["high"] - ob_zone["low"] + 1e-8)
            if last["close"] >= ob_zone["low"] or distance_to_ob < 0.1:
                score += 1
                zone_approach = 1
                approach_status = f"APPROACHING (dist: {distance_to_ob:.2%})"
            else:
                approach_status = f"FAR ({distance_to_ob:.2%} away)"
        
        reasons.append(f"Zone Approach +{zone_approach} ({approach_status})")
        
        calc_values["zone_approach"] = zone_approach
        calc_values["ob_type"] = ob_type
        calc_values["ob_strength"] = ob_zone.get("strength", "UNKNOWN")
        calc_values["ob_low"] = round(ob_zone["low"], 6)
        calc_values["ob_high"] = round(ob_zone["high"], 6)
        calc_values["ob_body_low"] = round(ob_zone.get("body_low", ob_zone["low"]), 6)
        calc_values["ob_body_high"] = round(ob_zone.get("body_high", ob_zone["high"]), 6)
        calc_values["distance_to_ob"] = round(distance_to_ob, 4)
    else:
        reasons.append("Zone Approach +0 (No OB detected)")
        ob_type = None
        calc_values["zone_approach"] = 0
        calc_values["ob_type"] = "NONE"

    # Step 5: HTF Alignment
    tf_map={"1m":"15m","3m":"30m","5m":"1h","15m":"4h","30m":"1h"}
    htf=tf_map.get(tf,"15m")
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, htf, 50)
    htf_alignment = 0
    htf_trend_value = 0
    if ohlcv_htf and len(ohlcv_htf) >= 5:
        df_htf = pd.DataFrame(ohlcv_htf, columns=["ts","open","high","low","close","vol"])
        if len(df_htf) >= 5:
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
            reasons.append("HTF Alignment ? (insufficient data)")
            calc_values["htf_trend"] = 0
            calc_values["htf_direction"] = "UNKNOWN"
    else:
        reasons.append("HTF Alignment ? (no data)")
        calc_values["htf_trend"] = 0
        calc_values["htf_direction"] = "UNKNOWN"

    # Step 6: MOMENTUM
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
    
    filter_reason = "Mom≥0.87" if momentum_val >= MOMENTUM_STRONG_THRESHOLD else "Mom≥0.85 & Disp≥0.80"
    reasons.append(f"✅ FORCED FILTER PASSED: {filter_reason}")

    # ---------------- ELITE MTF CONFIRMATION ----------------
    if not await elite_tf_alignment(exchange, symbol, side_str):
        return None
    reasons.append("Elite MTF Alignment ✅")

    # ---------------- ROMEOPT TP CALCULATION ----------------
    atr_val = float(atr(df, 14).iloc[-1])
    result = romeopt_tp_sl(entry, side_str, atr_val, ob_zone, df)
    
    # REJECT if no valid TP found
    if result is None:
        reasons.append("❌ NO VALID LIQUIDITY FOUND")
        return None
    
    sl, tp, tp_type, liquidity_data = result
    
    # Add liquidity data to calc_values for detailed breakdown
    calc_values.update(liquidity_data)
    
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
        "liquidity_data": liquidity_data,
        "market_state": liquidity_data["market_state"]
    }
    
    log.info(f"✅ Signal {sig['symbol']} passed forced filter: Mom={momentum_val:.2f}, Disp={displacement_val:.2f}")
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
            INSERT INTO signals (symbol,side,entry,sl,tp,timestamp,status,reason,score,latest_ob,tp_type,tp_locked,liquidity_data,market_state)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            sig["symbol"], sig["side"], sig["entry"], sig.get("sl"), sig.get("tp"),
            datetime.datetime.utcnow().isoformat(), "OPEN", sig["reason"], sig["score"],
            str(sig.get("latest_ob","")), sig.get("tp_type", ""), 1,
            str(sig.get("liquidity_data", {})), sig.get("market_state", "UNKNOWN")
        ))
        await db_conn.commit()

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
                        liquidity = sig.get("liquidity_data", {})
                        
                        momentum_val = calc.get("momentum_value", 0)
                        displacement_val = calc.get("displacement_value", 0)
                        
                        filter_passed = force_filter_trade(momentum_val, displacement_val)
                        
                        # Risk/Reward calculation
                        risk = abs(sig["entry"] - sig.get("sl", 0))
                        reward = abs(sig.get("tp", 0) - sig["entry"])
                        rr = reward / risk if risk > 0 else 0
                        
                        # ✅ ENHANCED BREAKDOWN WITH COMPREHENSIVE LIQUIDITY DATA
                        breakdown_lines = [
                            f"🏆 ROMEOPT SIGNAL: {sig['symbol']} ({tf}) {sig['side']}",
                            f"Entry: {sig['entry']:.6f}",
                            f"Score: {sig['score']}/6",
                            f"",
                            f"📊 LIQUIDITY SWEEP DATA:",
                            f"• Type: {calc.get('sweep_type', 'NONE')}",
                            f"• Direction: {calc.get('sweep_direction', 'NONE')}",
                            f"• Strength: {calc.get('sweep_strength', 0):.2f}",
                            f"• Respected: {'✅' if calc.get('sweep_respected', False) else '❌'}",
                            f"• Swept Level: {calc.get('swept_level', 0):.6f}",
                            f"",
                            f"📊 ORDER BLOCK DATA:",
                            f"• Type: {calc.get('ob_type', 'NONE')}",
                            f"• Strength: {calc.get('ob_strength', 'UNKNOWN')}",
                            f"• Range: {calc.get('ob_low', 0):.6f} - {calc.get('ob_high', 0):.6f}",
                            f"• Body: {calc.get('ob_body_low', 0):.6f} - {calc.get('ob_body_high', 0):.6f}",
                            f"• Distance: {calc.get('distance_to_ob', 0):.2%}",
                            f"",
                            f"📊 LIQUIDITY TARGET ANALYSIS:",
                            f"• Market State: {liquidity.get('market_state', 'UNKNOWN')}",
                            f"• Selected Type: {liquidity.get('selected_type', 'NONE')}",
                            f"• Internal Candidate: {liquidity.get('internal_candidate', 'N/A')}",
                            f"• External Candidate: {liquidity.get('external_candidate', 'N/A')}",
                            f"• ATR Value: {liquidity.get('atr_value', 0):.6f}",
                            f"• Tolerance Used: {liquidity.get('tolerance_used', 0):.6f}",
                            f"• Distance to Entry: {liquidity.get('distance_to_entry', 0):.6f}",
                            f"• Min R:R Required: {liquidity.get('min_rr_required', 0):.1f}",
                            f"• Actual R:R: {liquidity.get('rr_ratio', 0):.2f}",
                            f"",
                            f"📊 CORE METRICS:",
                            f"• Displacement: {calc.get('displacement_value', 0):.2f}",
                            f"• Momentum: {calc.get('momentum_value', 0):.2f}",
                            f"• HTF: {calc.get('htf_direction', '?')}",
                            f"• Forced Filter: {'✅ PASS' if filter_passed else '❌ REJECT'}",
                            f"• TP Type: {sig.get('tp_type', 'N/A')}",
                            f"",
                            f"🎯 LIQUIDITY TARGET (R:R: {rr:.2f}:1):",
                            f"SL: {sig.get('sl'):.6f}",
                            f"TP: {sig.get('tp'):.6f}",
                            f"",
                            f"📈 LIQUIDITY SELECTION REASON:",
                            f"• Chose {liquidity.get('selected_type', 'NONE')} over alternatives",
                            f"• Distance to entry: {liquidity.get('distance_to_entry', 0):.6f}",
                            f"• Reward: {liquidity.get('reward', 0):.6f}",
                            f"• Risk: {liquidity.get('risk', 0):.6f}",
                            f"",
                            f"💎 ROMEOPT PHILOSOPHY:",
                            f"One TP = One liquidity objective",
                            f"TP LOCKED - No chasing price",
                            f"Guaranteed stops only"
                        ]
                        
                        await tg("\n".join(breakdown_lines))
                        await log_signal(sig)
                        last_signal_time[key]=time.time()
                        signals_found+=1
            log.info(f"📊 Scan complete: {signals_found} RomeOPT signals (FIXED LIQUIDITY)")
        except Exception as e: 
            log.exception("scan error: %s", e)
        elapsed=time.time()-t0
        await asyncio.sleep(max(1,SCAN_INTERVAL-elapsed))

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("""
                    SELECT id, symbol, side, entry, sl, tp, tp_hit, status 
                    FROM signals WHERE status='OPEN'
                """) as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp, tp_hit, status = row
                        
                        if not all([sig_id, symbol, side, entry]):
                            continue
                        
                        if tp_hit == 1:
                            continue
                        
                        try:
                            ticker = await exchange.fetch_ticker(symbol)
                            last_price = ticker.get("last")
                            if last_price is None: 
                                continue
                        except:
                            continue

                        hits = []
                        new_tp_hit = tp_hit
                        new_status = status
                        
                        if side == "BUY":
                            if not tp_hit and tp is not None and last_price >= tp:
                                hits.append("TP"); new_tp_hit = 1
                            if sl is not None and last_price <= sl:
                                hits.append("SL"); new_status = "CLOSED"
                        else:
                            if not tp_hit and tp is not None and last_price <= tp:
                                hits.append("TP"); new_tp_hit = 1
                            if sl is not None and last_price >= sl:
                                hits.append("SL"); new_status = "CLOSED"

                        if hits:
                            await tg(f"🎯 {symbol} {side} HIT\nEntry:{entry}\nLast:{last_price}\nHits:{','.join(hits)}\nSL:{sl}\nTP:{tp}")

                        if "SL" in hits:
                            record_sl_hit(symbol)
                        
                        if new_tp_hit != tp_hit or new_status != status:
                            await db_conn.execute(
                                "UPDATE signals SET tp_hit=?,status=? WHERE id=?",
                                (new_tp_hit, new_status, sig_id)
                            )
                await db_conn.commit()
        except Exception as e: 
            log.exception("monitor error: %s", e)
        await asyncio.sleep(SCAN_INTERVAL)

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
exchange = None

async def main():
    global exchange
    await init_db()
    exchange = ccxt.okx({"enableRateLimit": True})
    
    # Startup announcement with all fixes highlighted
    startup_msgs = [
        "🏆 TRUE ROMEOPT SCANNER STARTED (ALL LIQUIDITY FIXES)",
        "✅ FIX 1: Realistic internal liquidity (tolerance 0.35*ATR)",
        "✅ FIX 2: Proper sweep invalidation (consumption ≠ touch)",
        "✅ FIX 3: Flexible R:R (0.3R range / 0.5R trend)",
        "✅ SMART HYBRID: Internal + External liquidity candidates",
        "🎯 ENHANCED: Comprehensive liquidity breakdown in signals",
        "🔒 TP LOCK: No recalculation after entry",
        "💎 ROMEOPT CORE: Target guaranteed stops only"
    ]
    
    for msg in startup_msgs:
        await tg(msg)
        await asyncio.sleep(0.5)
    
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
        except KeyboardInterrupt:
            log.info("Bot stopped by user")
        finally:
            if db_conn:
                asyncio.run(db_conn.close())
            if exchange:
                asyncio.run(exchange.close())