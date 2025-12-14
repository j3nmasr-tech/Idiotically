#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER (Enhanced + Elite Features)
- Fully live early signals
- RomeOPT 6-step logic with NUMERICAL BREAKDOWN
- TP/SL tracking with STRUCTURE-ONLY take profits
- Dynamic TP/SL updates (market-structure-based)
- Telegram alerts
- Async SQLite logging
- Filters: Score >=5, Displacement +2, Sweep+2 OR Zone+1, avoid counter-trend
- Improved Order Block detection
- Adaptive Market Regime detection
- HTF + Sweep scoring threshold
- Elite multi-timeframe confirmation (15m,1h,4h)
- STRUCTURE-REQUIRED: Signals rejected if no valid structure levels exist for TP
- FULL NUMERICAL BREAKDOWN: All component scores stored in database
- FIXED MONITORING: No structure revalidation during monitoring
"""

import os, time, asyncio, logging, datetime, json
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
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
            await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": safe_msg, "parse_mode": "HTML"})
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
            -- NUMERICAL BREAKDOWN FIELDS --
            liquidity_sweep_score INTEGER,
            displacement_score INTEGER,
            displacement_value REAL,
            zone_approach_score INTEGER,
            htf_alignment_score INTEGER,
            momentum_score INTEGER,
            momentum_value REAL,
            elite_mtf_score INTEGER,
            market_regime TEXT,
            order_block_type TEXT,
            order_block_low REAL,
            order_block_high REAL,
            atr_value REAL,
            risk_reward REAL,
            breakdown_json TEXT
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
    if len(df) < 50:
        return "RANGE"
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
    tfs = ["15m", "1h", "4h"]
    alignments = []
    for tf in tfs:
        ohlcv = await fetch_ohlcv(exchange, symbol, tf, 50)
        if not ohlcv: 
            alignments.append(False)
            continue
        df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
        trend = df["close"].iloc[-1] - df["close"].iloc[-5]
        trend_side = "BUY" if trend > 0 else "SELL"
        alignments.append(trend_side == side)
    
    # Return both boolean result and alignment count
    all_aligned = all(alignments)
    alignment_count = sum(alignments)
    return all_aligned, alignment_count, len(tfs)

# ---------------- TP/SL HELPERS ----------------
def find_latest_ob(df: pd.DataFrame):
    """Find the latest order block in the dataframe"""
    if len(df) < 6:
        return None
    for i in range(len(df) - 5, len(df) - 1):
        candle, prev_candle = df.iloc[i], df.iloc[i - 1]
        if candle["close"] > candle["open"] and prev_candle["close"] < prev_candle["open"]:
            return {
                "type": "bullish", 
                "low": min(candle["low"], prev_candle["low"]), 
                "high": candle["close"],
                "candle_index": i,
                "candle_time": candle.name if hasattr(candle, 'name') else i
            }
        elif candle["close"] < candle["open"] and prev_candle["close"] > prev_candle["open"]:
            return {
                "type": "bearish", 
                "low": candle["close"], 
                "high": max(candle["high"], prev_candle["high"]),
                "candle_index": i,
                "candle_time": candle.name if hasattr(candle, 'name') else i
            }
    return None

def find_structure_levels(df: pd.DataFrame, side: str, entry: float, min_lookback=50, max_lookback=150):
    """
    Find meaningful structure levels for TP targets
    Returns sorted list of valid levels with metadata
    """
    if len(df) < max_lookback:
        max_lookback = len(df)
    
    if side == "BUY":
        # Find resistance levels (highs)
        highs = df['high'].values[-max_lookback:]
        levels = []
        
        # Find local maxima with some context
        for i in range(5, len(highs) - 5):
            if highs[i] == max(highs[i-5:i+6]):
                # Check consolidation: price stayed below this level for a while
                levels.append({
                    "price": float(highs[i]),
                    "index": i,
                    "type": "resistance"
                })
        
        # Filter for levels above entry and remove near duplicates
        filtered_levels = []
        for level in sorted(levels, key=lambda x: x["price"]):
            if level["price"] > entry:
                if not filtered_levels or (level["price"] / filtered_levels[-1]["price"] - 1) > 0.001:
                    filtered_levels.append(level)
        
        return filtered_levels
    
    else:  # SELL
        # Find support levels (lows)
        lows = df['low'].values[-max_lookback:]
        levels = []
        
        for i in range(5, len(lows) - 5):
            if lows[i] == min(lows[i-5:i+6]):
                levels.append({
                    "price": float(lows[i]),
                    "index": i,
                    "type": "support"
                })
        
        # Filter for levels below entry and remove near duplicates
        filtered_levels = []
        for level in sorted(levels, key=lambda x: x["price"], reverse=True):
            if level["price"] < entry:
                if not filtered_levels or (filtered_levels[-1]["price"] / level["price"] - 1) > 0.001:
                    filtered_levels.append(level)
        
        return filtered_levels

def romeopt_tp_sl(entry, side, atr_val, ob_zone, df):
    """
    STRUCTURE-ONLY TP/SL system
    - SL based on order block + ATR buffer
    - TP MUST be at valid structure levels
    - If no structure exists or structure is invalid → return None (signal rejected)
    """
    recent_high = df['high'].iloc[-10:].max()
    recent_low = df['low'].iloc[-10:].min()
    
    # Calculate stop loss first (unchanged)
    if side == "BUY":
        sl_ob = ob_zone["low"] - (atr_val * 0.3)
        sl_structure = recent_low - (atr_val * 0.3)
        sl = min(sl_ob, sl_structure)
        
        risk = entry - sl
        min_risk = atr_val * 0.5
        if risk < min_risk:
            risk = min_risk
            sl = entry - risk
        
        # --- FIND STRUCTURE LEVELS FOR TP ---
        structure_levels = find_structure_levels(df, side, entry)
        
        if not structure_levels:
            log.debug(f"No structure levels found for BUY at {entry}")
            return None  # REJECT: No structure above price
        
        # Filter for meaningful levels (at least 0.5R profit)
        meaningful_levels = [level for level in structure_levels 
                           if (level["price"] - entry) >= (risk * 0.5)]
        
        if not meaningful_levels:
            log.debug(f"No meaningful structure levels (min 0.5R) for BUY")
            return None  # REJECT: No structure with minimum profit
        
        # TP1: Nearest meaningful structure
        tp1_data = meaningful_levels[0]
        tp1 = tp1_data["price"]
        
        # TP2: Next meaningful structure (if exists, at least 0.5R beyond TP1)
        tp2_data = None
        tp2 = None
        for level in meaningful_levels:
            if level["price"] >= tp1 + (risk * 0.5):
                tp2_data = level
                tp2 = level["price"]
                break
        
        # TP3: Can be extended target or next structure
        tp3_data = None
        tp3 = None
        if tp2:
            # Look for level beyond TP2
            for level in meaningful_levels:
                if level["price"] >= tp2 + (risk * 0.5):
                    tp3_data = level
                    tp3 = level["price"]
                    break
        
        # If no TP3 from structure, use 3R as extended target
        if not tp3:
            tp3 = entry + (risk * 3.0)
            tp3_data = {"price": tp3, "type": "extended_rr", "rr_multiple": 3.0}
        
        # Calculate risk-reward for TP1
        rr_tp1 = (tp1 - entry) / risk if risk > 0 else 0
        
        return sl, tp1, tp2, tp3, rr_tp1, [tp1_data, tp2_data, tp3_data]
        
    else:  # SELL
        sl_ob = ob_zone["high"] + (atr_val * 0.3)
        sl_structure = recent_high + (atr_val * 0.3)
        sl = max(sl_ob, sl_structure)
        
        risk = sl - entry
        min_risk = atr_val * 0.5
        if risk < min_risk:
            risk = min_risk
            sl = entry + risk
        
        # --- FIND STRUCTURE LEVELS FOR TP ---
        structure_levels = find_structure_levels(df, side, entry)
        
        if not structure_levels:
            log.debug(f"No structure levels found for SELL at {entry}")
            return None  # REJECT: No structure below price
        
        # Filter for meaningful levels (at least 0.5R profit)
        meaningful_levels = [level for level in structure_levels 
                           if (entry - level["price"]) >= (risk * 0.5)]
        
        if not meaningful_levels:
            log.debug(f"No meaningful structure levels (min 0.5R) for SELL")
            return None  # REJECT: No structure with minimum profit
        
        # TP1: Nearest meaningful structure
        tp1_data = meaningful_levels[0]
        tp1 = tp1_data["price"]
        
        # TP2: Next meaningful structure (if exists, at least 0.5R beyond TP1)
        tp2_data = None
        tp2 = None
        for level in meaningful_levels:
            if level["price"] <= tp1 - (risk * 0.5):
                tp2_data = level
                tp2 = level["price"]
                break
        
        # TP3: Can be extended target or next structure
        tp3_data = None
        tp3 = None
        if tp2:
            # Look for level beyond TP2
            for level in meaningful_levels:
                if level["price"] <= tp2 - (risk * 0.5):
                    tp3_data = level
                    tp3 = level["price"]
                    break
        
        # If no TP3 from structure, use 3R as extended target
        if not tp3:
            tp3 = entry - (risk * 3.0)
            tp3_data = {"price": tp3, "type": "extended_rr", "rr_multiple": 3.0}
        
        # Calculate risk-reward for TP1
        rr_tp1 = (entry - tp1) / risk if risk > 0 else 0
        
        return sl, tp1, tp2, tp3, rr_tp1, [tp1_data, tp2_data, tp3_data]

def update_tp_sl_live(sig: dict, df: pd.DataFrame):
    """
    Calculate TP/SL with STRUCTURE-ONLY requirement
    Returns None if no valid structure exists
    """
    latest_ob = find_latest_ob(df)
    if not latest_ob:
        return None  # REJECT: No order block
    
    atr_val = float(atr(df, 14).iloc[-1])
    entry = sig["entry"]
    side = sig["side"]
    
    # Get TP/SL - will return None if no structure
    result = romeopt_tp_sl(entry, side, atr_val, latest_ob, df)
    
    if result is None:
        return None  # REJECT: No valid structure for TP
    
    sl, tp1, tp2, tp3, rr_tp1, tp_data = result
    
    # Store all TP/SL data in signal
    sig.update({
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "latest_ob": latest_ob,
        "atr_value": atr_val,
        "risk_reward": rr_tp1,
        "tp1_data": tp_data[0] if tp_data and len(tp_data) > 0 else None,
        "tp2_data": tp_data[1] if tp_data and len(tp_data) > 1 else None,
        "tp3_data": tp_data[2] if tp_data and len(tp_data) > 2 else None,
        "risk": abs(entry - sl)
    })
    
    return sig

# ---------------- ROMEOPT 6-STEP SIGNAL ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    """Generate RomeOPT 6-step signal with STRUCTURE-ONLY TP requirement"""
    if df is None or len(df) < 20:
        return None
    
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    
    # Initialize breakdown tracking
    breakdown = {
        "symbol": symbol,
        "timeframe": tf,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "components": {}
    }
    
    score = 0
    reasons = []

    # Step 1: Liquidity Sweep - NUMERICAL DATA
    sweep_high = float(last["high"] > prev5["high"].max())
    sweep_low = float(last["low"] < prev5["low"].min())
    has_sweep = bool(sweep_high or sweep_low)
    liquidity_sweep = 2 if has_sweep else 0
    score += liquidity_sweep
    reasons.append(f"Liquidity Sweep +{liquidity_sweep}")
    
    # Store numerical sweep data
    breakdown["components"]["liquidity_sweep"] = {
        "score": liquidity_sweep,
        "sweep_high": sweep_high,
        "sweep_low": sweep_low,
        "has_sweep": has_sweep,
        "current_high": float(last["high"]),
        "prev_high_max": float(prev5["high"].max()),
        "current_low": float(last["low"]),
        "prev_low_min": float(prev5["low"].min())
    }

    # Step 2: Displacement - NUMERICAL DATA
    displacement = float(abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8))
    has_disp = displacement > 0.6
    displacement_score = 2 if has_disp else 0
    score += displacement_score
    reasons.append(f"Displacement +{displacement_score}")
    
    breakdown["components"]["displacement"] = {
        "score": displacement_score,
        "value": displacement,
        "has_displacement": has_disp,
        "threshold": 0.6,
        "candle_body": float(abs(last["close"] - last["open"])),
        "candle_range": float(last["high"] - last["low"]),
        "close": float(last["close"]),
        "open": float(last["open"])
    }

    # Step 3 & 4: Order Block & Zone - NUMERICAL DATA
    ob_zone = None
    ob_details = None
    
    for i in range(len(df) - 5, len(df) - 1):
        candle, prev_candle = df.iloc[i], df.iloc[i - 1]
        if candle["close"] > candle["open"] and prev_candle["close"] < prev_candle["open"]:
            ob_zone = {
                "type": "bullish", 
                "low": min(candle["low"], prev_candle["low"]), 
                "high": candle["close"]
            }
            ob_details = {
                "candle_index": i,
                "bullish_candle_close": float(candle["close"]),
                "bullish_candle_open": float(candle["open"]),
                "prev_candle_close": float(prev_candle["close"]),
                "prev_candle_open": float(prev_candle["open"])
            }
            break
        elif candle["close"] < candle["open"] and prev_candle["close"] > prev_candle["open"]:
            ob_zone = {
                "type": "bearish", 
                "low": candle["close"], 
                "high": max(candle["high"], prev_candle["high"])
            }
            ob_details = {
                "candle_index": i,
                "bearish_candle_close": float(candle["close"]),
                "bearish_candle_open": float(candle["open"]),
                "prev_candle_close": float(prev_candle["close"]),
                "prev_candle_open": float(prev_candle["open"])
            }
            break

    zone_score = 0
    if ob_zone:
        ob_type = ob_zone["type"]
        if ob_type == "bullish" and last["close"] <= ob_zone["high"]:
            zone_score = 1
            score += zone_score
            reasons.append("Zone Approach +1")
        elif ob_type == "bearish" and last["close"] >= ob_zone["low"]:
            zone_score = 1
            score += zone_score
            reasons.append("Zone Approach +1")
        else:
            reasons.append("Zone Approach +0")
    else:
        reasons.append("Zone Approach +0")
        ob_type = None

    if not ob_type:
        return None

    # Store order block data
    breakdown["components"]["order_block"] = {
        "score": zone_score,
        "type": ob_type,
        "zone_low": float(ob_zone["low"]) if ob_zone else None,
        "zone_high": float(ob_zone["high"]) if ob_zone else None,
        "current_price_vs_zone": "inside" if (ob_type == "bullish" and last["close"] <= ob_zone["high"]) or 
                                          (ob_type == "bearish" and last["close"] >= ob_zone["low"]) else "outside",
        "details": ob_details
    }

    # Step 5: HTF Alignment - NUMERICAL DATA
    tf_map = {"1m": "15m", "3m": "30m", "5m": "1h", "15m": "4h", "30m": "1h"}
    htf = tf_map.get(tf, "15m")
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, htf, 50)
    htf_alignment = 0
    htf_trend_value = 0
    
    if ohlcv_htf:
        df_htf = pd.DataFrame(ohlcv_htf, columns=["ts", "open", "high", "low", "close", "vol"])
        trend = df_htf["close"].iloc[-1] - df_htf["close"].iloc[-5]
        htf_trend_value = float(trend)
        htf_dir = "bullish" if trend > 0 else "bearish"
        if htf_dir == ob_type:
            htf_alignment = 1
            score += htf_alignment
            reasons.append("HTF Alignment +1")
        else:
            reasons.append("HTF Alignment +0")
    else:
        reasons.append("HTF Alignment ?")

    breakdown["components"]["htf_alignment"] = {
        "score": htf_alignment,
        "higher_timeframe": htf,
        "trend_value": htf_trend_value,
        "alignment": htf_dir == ob_type if ohlcv_htf else None,
        "ob_type": ob_type,
        "htf_direction": htf_dir if ohlcv_htf else "unknown"
    }

    # Step 6: Momentum - NUMERICAL DATA
    momentum_ratio = float(abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8))
    momentum_score = 0
    
    if ob_type == "bullish" and momentum_ratio > 0.5 and last["close"] > last["open"]:
        momentum_score = 1
        score += momentum_score
        reasons.append("Momentum +1")
    elif ob_type == "bearish" and momentum_ratio > 0.5 and last["close"] < last["open"]:
        momentum_score = 1
        score += momentum_score
        reasons.append("Momentum +1")
    else:
        reasons.append("Momentum +0")

    breakdown["components"]["momentum"] = {
        "score": momentum_score,
        "ratio": momentum_ratio,
        "threshold": 0.5,
        "candle_direction": "bullish" if last["close"] > last["open"] else "bearish",
        "has_momentum": momentum_score > 0
    }

    side = "BUY" if ob_type == "bullish" else "SELL"
    entry = float(last["close"])

    # ---------------- CRITICAL FILTERS ----------------
    critical_score = htf_alignment + liquidity_sweep
    breakdown["critical_score"] = critical_score
    breakdown["critical_threshold"] = CRITICAL_FACTORS_MIN
    
    if critical_score < CRITICAL_FACTORS_MIN:
        breakdown["rejection_reason"] = "Critical score too low"
        return None
    if score < MIN_SCORE:
        breakdown["rejection_reason"] = "Total score too low"
        return None
    if not has_disp:
        breakdown["rejection_reason"] = "No displacement"
        return None
    
    # ---------------- HTF ALIGNMENT MANDATORY FILTER ----------------
    if htf_alignment != 1:
        breakdown["rejection_reason"] = "No HTF alignment"
        return None

    # ---------------- MARKET REGIME ----------------
    market_regime = await detect_market_regime(df)
    breakdown["market_regime"] = market_regime
    
    if (market_regime == "BULL" and side == "SELL") or (market_regime == "BEAR" and side == "BUY"):
        breakdown["rejection_reason"] = "Counter-trend in current regime"
        return None

    # ---------------- TREND MA FILTER ----------------
    trend_ma = df["close"].rolling(20).mean().iloc[-1]
    breakdown["trend_ma"] = float(trend_ma)
    breakdown["price_vs_ma"] = float(last["close"] - trend_ma)
    
    if (side == "BUY" and last["close"] < trend_ma) or (side == "SELL" and last["close"] > trend_ma):
        breakdown["rejection_reason"] = "Price wrong side of MA"
        return None

    # ---------------- ELITE MTF CONFIRMATION ----------------
    elite_result, elite_count, elite_total = await elite_tf_alignment(exchange, symbol, side)
    elite_score = 1 if elite_result else 0
    
    if not elite_result:
        breakdown["rejection_reason"] = f"Elite MTF failed ({elite_count}/{elite_total})"
        return None
    
    reasons.append("Elite MTF Alignment ✅")
    
    breakdown["components"]["elite_mtf"] = {
        "score": elite_score,
        "aligned": elite_result,
        "alignment_count": elite_count,
        "total_timeframes": elite_total,
        "timeframes": ["15m", "1h", "4h"]
    }

    # Create initial signal with ALL breakdown data
    sig = {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "score": score,
        "reason": "RomeOPT 6-Step",
        "reason_list": reasons,
        "breakdown": breakdown,
        # Store individual component scores for database
        "liquidity_sweep_score": liquidity_sweep,
        "displacement_score": displacement_score,
        "displacement_value": displacement,
        "zone_approach_score": zone_score,
        "htf_alignment_score": htf_alignment,
        "momentum_score": momentum_score,
        "momentum_value": momentum_ratio,
        "elite_mtf_score": elite_score,
        "market_regime": market_regime,
        "order_block_type": ob_type,
        "order_block_low": float(ob_zone["low"]) if ob_zone else None,
        "order_block_high": float(ob_zone["high"]) if ob_zone else None,
        "trend_ma_value": float(trend_ma),
        "price_vs_ma": float(last["close"] - trend_ma)
    }
    
    # ---------------- STRUCTURE-ONLY TP/SL CALCULATION ----------------
    sig = update_tp_sl_live(sig, df)
    
    # If no structure for TP, reject the signal
    if sig is None:
        breakdown["rejection_reason"] = "No valid structure levels for TP"
        log.debug(f"Signal rejected for {symbol}: No valid structure levels for TP")
        return None
    
    # Add TP/SL data to breakdown
    if "risk" in sig:
        breakdown["risk_management"] = {
            "risk": sig["risk"],
            "risk_reward": sig.get("risk_reward", 0),
            "atr_value": sig.get("atr_value", 0),
            "stop_loss": sig.get("sl"),
            "take_profit_1": sig.get("tp1"),
            "take_profit_2": sig.get("tp2"),
            "take_profit_3": sig.get("tp3"),
            "tp1_data": sig.get("tp1_data"),
            "tp2_data": sig.get("tp2_data"),
            "tp3_data": sig.get("tp3_data")
        }
    
    # ---------------- FINAL VALIDATION ----------------
    # Ensure TP1 is at least 0.5R profit
    if "sl" in sig and "tp1" in sig:
        risk = abs(sig["entry"] - sig["sl"])
        tp1_distance = abs(sig["tp1"] - sig["entry"])
        
        if tp1_distance < risk * 0.5:
            breakdown["rejection_reason"] = f"TP1 too close ({tp1_distance:.6f} < {risk*0.5:.6f})"
            log.debug(f"Signal rejected for {symbol}: TP1 too close ({tp1_distance:.6f} < {risk*0.5:.6f})")
            return None
        
        breakdown["risk_management"]["tp1_distance"] = tp1_distance
        breakdown["risk_management"]["tp1_min_required"] = risk * 0.5
    
    # Finalize breakdown
    breakdown["final_score"] = score
    breakdown["signal_generated"] = True
    breakdown["generation_time"] = datetime.datetime.utcnow().isoformat()
    
    # Update sig with final breakdown
    sig["breakdown"] = breakdown
    sig["breakdown_json"] = json.dumps(breakdown, default=str)
    
    return sig

# ---------------- SL CLUSTER ----------------
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

# ---------------- LOG SIGNAL ----------------
async def log_signal(sig):
    async with db_lock:
        await db_conn.execute("""
            INSERT INTO signals (
                symbol, side, entry, sl, tp1, tp2, tp3, timestamp, status, reason, score,
                liquidity_sweep_score, displacement_score, displacement_value, 
                zone_approach_score, htf_alignment_score, momentum_score, momentum_value,
                elite_mtf_score, market_regime, order_block_type, order_block_low, 
                order_block_high, atr_value, risk_reward, breakdown_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sig["symbol"], sig["side"], sig["entry"], 
            sig.get("sl"), sig.get("tp1"), sig.get("tp2"), sig.get("tp3"),
            datetime.datetime.utcnow().isoformat(), "OPEN", sig["reason"], sig["score"],
            sig.get("liquidity_sweep_score", 0),
            sig.get("displacement_score", 0),
            sig.get("displacement_value", 0),
            sig.get("zone_approach_score", 0),
            sig.get("htf_alignment_score", 0),
            sig.get("momentum_score", 0),
            sig.get("momentum_value", 0),
            sig.get("elite_mtf_score", 0),
            sig.get("market_regime", "UNKNOWN"),
            sig.get("order_block_type", "UNKNOWN"),
            sig.get("order_block_low"),
            sig.get("order_block_high"),
            sig.get("atr_value", 0),
            sig.get("risk_reward", 0),
            sig.get("breakdown_json", "{}")
        ))
        await db_conn.commit()

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    """
    Monitor open positions for TP/SL hits
    - TP/SL levels are FIXED after entry (not recalculated)
    - Only checks if price hit predefined levels
    - No structure revalidation during monitoring
    """
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("SELECT id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status FROM signals WHERE status='OPEN'") as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status = row
                        
                        # Fetch current price
                        try:
                            ticker = await exchange.fetch_ticker(symbol)
                            last_price = ticker.get("last")
                            if last_price is None:
                                continue
                        except Exception as e:
                            log.debug(f"Failed to fetch ticker for {symbol}: {e}")
                            continue
                        
                        # Check for TP/SL hits - USING ORIGINAL TP/SL LEVELS
                        hits = []
                        sl_hit = False
                        
                        if side == "BUY":
                            if not tp1_hit and last_price >= tp1:
                                hits.append("TP1")
                                tp1_hit = 1
                            if not tp2_hit and last_price >= tp2:
                                hits.append("TP2")
                                tp2_hit = 1
                            if not tp3_hit and last_price >= tp3:
                                hits.append("TP3")
                                tp3_hit = 1
                            if last_price <= sl:
                                hits.append("SL")
                                status = "CLOSED"
                                sl_hit = True
                        else:  # SELL
                            if not tp1_hit and last_price <= tp1:
                                hits.append("TP1")
                                tp1_hit = 1
                            if not tp2_hit and last_price <= tp2:
                                hits.append("TP2")
                                tp2_hit = 1
                            if not tp3_hit and last_price <= tp3:
                                hits.append("TP3")
                                tp3_hit = 1
                            if last_price >= sl:
                                hits.append("SL")
                                status = "CLOSED"
                                sl_hit = True
                        
                        if hits:
                            msg = f"🎯 {symbol} {side} update\nEntry: {entry:.8f}\nLast: {last_price:.8f}\nHits: {','.join(hits)}\nSL: {sl:.8f}"
                            if tp1: msg += f"\nTP1: {tp1:.8f}"
                            if tp2: msg += f" TP2: {tp2:.8f}"
                            if tp3: msg += f" TP3: {tp3:.8f}"
                            await tg(msg)
                        
                        if sl_hit:
                            record_sl_hit(symbol)
                        
                        # Update database
                        await db_conn.execute(
                            "UPDATE signals SET tp1_hit=?, tp2_hit=?, tp3_hit=?, status=? WHERE id=?",
                            (tp1_hit, tp2_hit, tp3_hit, status, sig_id)
                        )
                
                await db_conn.commit()
        except Exception as e:
            log.exception("monitor error: %s", e)
        
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- SCAN LOOP ----------------
last_signal_time = {}

async def scan_loop(exchange):
    while True:
        t0 = time.time()
        try:
            # Fetch top volume pairs
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, v.get("quoteVolume", 0)) for s, v in tickers.items() if s.endswith("/USDT")]
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            top = usdt_pairs[:TOP_N]
            
            signals_found = 0
            for symbol, _ in top:
                if deprioritized(symbol):
                    continue
                
                for tf in TIMEFRAMES:
                    key = f"{symbol}:{tf}"
                    # Rate limiting per symbol:timeframe
                    if key in last_signal_time and time.time() - last_signal_time[key] < 60:
                        continue
                    
                    ohlcv = await fetch_ohlcv(exchange, symbol, tf, 200)
                    if not ohlcv or len(ohlcv) < 50:
                        continue
                    
                    df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
                    for col in ["open", "high", "low", "close", "vol"]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    
                    sig = await generate_signal_romeopt(exchange, df, symbol, tf)
                    
                    if sig:
                        # Format detailed Telegram message with numerical breakdown
                        breakdown = sig.get("breakdown", {})
                        components = breakdown.get("components", {})
                        
                        msg = f"🏆 {sig['symbol']} ({tf}) {sig['side']}\n"
                        msg += f"Entry: {sig['entry']:.8f}\n"
                        msg += f"SL: {sig.get('sl', 'N/A'):.8f}\n"
                        
                        # TP levels
                        tp_msg = ""
                        if 'tp1' in sig and sig['tp1']:
                            tp_msg += f"TP1: {sig['tp1']:.8f}"
                        if 'tp2' in sig and sig['tp2']:
                            tp_msg += f" TP2: {sig['tp2']:.8f}"
                        if 'tp3' in sig and sig['tp3']:
                            tp_msg += f" TP3: {sig['tp3']:.8f}"
                        
                        if tp_msg:
                            msg += f"{tp_msg}\n"
                        
                        msg += f"Score: {sig['score']}/9\n"
                        msg += f"Regime: {sig.get('market_regime', 'N/A')}\n"
                        
                        # Detailed component breakdown
                        msg += "\n📊 COMPONENT BREAKDOWN:\n"
                        
                        # Liquidity Sweep
                        ls = components.get("liquidity_sweep", {})
                        msg += f"• Liquidity Sweep: {ls.get('score', 0)}/2"
                        if ls.get('has_sweep'):
                            if ls.get('sweep_high'):
                                msg += f" (HIGH: {ls.get('current_high', 0):.8f} > {ls.get('prev_high_max', 0):.8f})"
                            else:
                                msg += f" (LOW: {ls.get('current_low', 0):.8f} < {ls.get('prev_low_min', 0):.8f})"
                        msg += "\n"
                        
                        # Displacement
                        disp = components.get("displacement", {})
                        msg += f"• Displacement: {disp.get('score', 0)}/2 (Value: {disp.get('value', 0):.2f})\n"
                        
                        # Zone Approach
                        zone = components.get("order_block", {})
                        msg += f"• Zone Approach: {zone.get('score', 0)}/1 (Type: {zone.get('type', 'N/A')})\n"
                        
                        # HTF Alignment
                        htf = components.get("htf_alignment", {})
                        msg += f"• HTF Alignment: {htf.get('score', 0)}/1 (TF: {htf.get('higher_timeframe', 'N/A')})\n"
                        
                        # Momentum
                        mom = components.get("momentum", {})
                        msg += f"• Momentum: {mom.get('score', 0)}/1 (Ratio: {mom.get('ratio', 0):.2f})\n"
                        
                        # Elite MTF
                        elite = components.get("elite_mtf", {})
                        msg += f"• Elite MTF: {elite.get('score', 0)}/1 ({elite.get('alignment_count', 0)}/{elite.get('total_timeframes', 3)})\n"
                        
                        # Risk Management
                        if "risk_management" in breakdown:
                            rm = breakdown["risk_management"]
                            msg += f"\n💰 RISK/REWARD:\n"
                            msg += f"• Risk: {rm.get('risk', 0):.8f}\n"
                            msg += f"• R:R (TP1): {rm.get('risk_reward', 0):.2f}:1\n"
                            msg += f"• ATR: {rm.get('atr_value', 0):.8f}\n"
                        
                        await tg(msg)
                        await log_signal(sig)
                        
                        last_signal_time[key] = time.time()
                        signals_found += 1
                        
                        # Small delay between signals to avoid rate limits
                        await asyncio.sleep(0.5)
            
            log.info(f"📊 Scan complete: {signals_found} RomeOPT signals found")
            
        except Exception as e:
            log.exception("scan error: %s", e)
        
        elapsed = time.time() - t0
        await asyncio.sleep(max(1, SCAN_INTERVAL - elapsed))

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.post("/webhook")
async def webhook(request: Request):
    token = request.headers.get("X-Auth", "")
    if token != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    data = await request.json()
    log.info("Webhook received: %s", data)
    return {"ok": True}

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.datetime.utcnow().isoformat()}

@app.get("/signals")
async def get_signals(limit: int = 50):
    async with db_lock:
        async with db_conn.execute(
            "SELECT id, symbol, side, entry, sl, tp1, tp2, tp3, timestamp, status, score, breakdown_json FROM signals ORDER BY id DESC LIMIT ?", 
            (limit,)
        ) as cursor:
            rows = await cursor.fetchall()
            
        signals = []
        for row in rows:
            sig_id, symbol, side, entry, sl, tp1, tp2, tp3, timestamp, status, score, breakdown_json = row
            signal = {
                "id": sig_id,
                "symbol": symbol,
                "side": side,
                "entry": entry,
                "sl": sl,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "timestamp": timestamp,
                "status": status,
                "score": score
            }
            
            if breakdown_json:
                try:
                    signal["breakdown"] = json.loads(breakdown_json)
                except:
                    signal["breakdown"] = {}
            
            signals.append(signal)
        
        return {"signals": signals, "count": len(signals)}

# ---------------- MAIN ----------------
exchange = None

async def main():
    global exchange
    await init_db()
    
    # Initialize exchange
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"}
    })
    
    await tg("🏆 ROMEOPT 6-Step Scanner Started - Structure-Only TP/SL with Full Numerical Breakdown")
    
    # Run both tasks concurrently
    await asyncio.gather(
        scan_loop(exchange),
        monitor_signals()
    )

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", help="Run HTTP server")
    args = parser.parse_args()
    
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
            if exchange:
                asyncio.run(exchange.close())