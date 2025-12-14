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
- 🎯 ENHANCED BREAKDOWN FORMAT: Same detailed format as FORCED FILTER version
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
    
    # First, check if table exists and get its current columns
    cursor = await db_conn.execute("PRAGMA table_info(signals)")
    columns = await cursor.fetchall()
    column_names = [col[1] for col in columns] if columns else []
    
    # Create table if it doesn't exist with all new columns
    if not column_names:
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
    else:
        # Table exists, add missing columns
        new_columns = [
            ("liquidity_sweep_score", "INTEGER"),
            ("displacement_score", "INTEGER"),
            ("displacement_value", "REAL"),
            ("zone_approach_score", "INTEGER"),
            ("htf_alignment_score", "INTEGER"),
            ("momentum_score", "INTEGER"),
            ("momentum_value", "REAL"),
            ("elite_mtf_score", "INTEGER"),
            ("market_regime", "TEXT"),
            ("order_block_type", "TEXT"),
            ("order_block_low", "REAL"),
            ("order_block_high", "REAL"),
            ("atr_value", "REAL"),
            ("risk_reward", "REAL"),
            ("breakdown_json", "TEXT")
        ]
        
        for col_name, col_type in new_columns:
            if col_name not in column_names:
                log.info(f"Adding missing column: {col_name}")
                await db_conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_type}")
    
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

# ---------------- ENHANCED SWEEP ANALYSIS ----------------
def analyze_sweep_details(df: pd.DataFrame, lookback=5):
    """Analyze sweep with detailed information - doesn't affect signal logic"""
    if len(df) < lookback + 1:
        return {"type": "NONE", "details": {}}
    
    current_candle = df.iloc[-1]
    lookback_candles = df.iloc[-(lookback+1):-1]
    
    current_high = current_candle["high"]
    current_low = current_candle["low"]
    prev_highs = lookback_candles["high"].values
    prev_lows = lookback_candles["low"].values
    
    max_prev_high = np.max(prev_highs) if len(prev_highs) > 0 else current_high
    min_prev_low = np.min(prev_lows) if len(prev_lows) > 0 else current_low
    
    # Check for high sweep
    if current_high > max_prev_high:
        extension = current_high - max_prev_high
        return {
            "type": "HIGH",
            "details": {
                "current_high": current_high,
                "previous_high": max_prev_high,
                "extension": extension,
                "extension_pct": (extension / max_prev_high * 100) if max_prev_high > 0 else 0,
                "strength": "STRONG" if extension > (current_high * 0.001) else "MODERATE",
                "wick_size": current_high - max(current_candle["open"], current_candle["close"]),
                "volume": current_candle["vol"],
                "candle_body": current_candle["close"] - current_candle["open"],
                "candle_range": current_candle["high"] - current_candle["low"]
            }
        }
    
    # Check for low sweep
    elif current_low < min_prev_low:
        extension = min_prev_low - current_low
        return {
            "type": "LOW",
            "details": {
                "current_low": current_low,
                "previous_low": min_prev_low,
                "extension": extension,
                "extension_pct": (extension / min_prev_low * 100) if min_prev_low > 0 else 0,
                "strength": "STRONG" if extension > (current_low * 0.001) else "MODERATE",
                "wick_size": min(current_candle["open"], current_candle["close"]) - current_low,
                "volume": current_candle["vol"],
                "candle_body": current_candle["open"] - current_candle["close"],
                "candle_range": current_candle["high"] - current_candle["low"]
            }
        }
    
    return {"type": "NONE", "details": {}}

# ---------------- FORMAT NUMBER ----------------
def format_number(value, decimals=6):
    """Format number with appropriate decimal places"""
    if isinstance(value, (int, float)):
        if abs(value) >= 1000:
            return f"{value:,.2f}"
        elif abs(value) >= 1:
            return f"{value:.4f}"
        else:
            return f"{value:.{decimals}f}"
    return str(value)

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
    
    # ENHANCED: Add sweep details for breakdown (doesn't affect signal)
    sweep_analysis = analyze_sweep_details(df)
    breakdown["components"]["liquidity_sweep"] = {
        "score": liquidity_sweep,
        "sweep_high": sweep_high,
        "sweep_low": sweep_low,
        "has_sweep": has_sweep,
        "current_high": float(last["high"]),
        "prev_high_max": float(prev5["high"].max()),
        "current_low": float(last["low"]),
        "prev_low_min": float(prev5["low"].min()),
        "sweep_details": sweep_analysis
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
                "prev_candle_open": float(prev_candle["open"]),
                "candle_range": float(candle["high"] - candle["low"])
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
                "prev_candle_open": float(prev_candle["open"]),
                "candle_range": float(candle["high"] - candle["low"])
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

    # Calculate detailed OB info for breakdown
    if ob_zone:
        ob_low = ob_zone["low"]
        ob_high = ob_zone["high"]
        ob_range = ob_high - ob_low
        ob_mid = (ob_low + ob_high) / 2
        distance_to_price = abs(last["close"] - ob_mid)
        distance_pct = (distance_to_price / last["close"] * 100) if last["close"] > 0 else 100
        
        # Calculate OB strength
        if ob_details and "candle_range" in ob_details:
            strength = ob_details["candle_range"] / ob_range if ob_range > 0 else 0
        else:
            strength = 0
        
        ob_breakdown = {
            "type": ob_type,
            "low": float(ob_low),
            "high": float(ob_high),
            "midpoint": float(ob_mid),
            "range": float(ob_range),
            "distance_to_price": float(distance_to_price),
            "distance_pct": float(distance_pct),
            "strength": float(strength),
            "in_zone": True if (ob_type == "bullish" and last["close"] <= ob_zone["high"]) or 
                               (ob_type == "bearish" and last["close"] >= ob_zone["low"]) else False
        }
        
        if ob_details:
            ob_breakdown["details"] = ob_details
    else:
        ob_breakdown = {}

    # Store order block data
    breakdown["components"]["order_block"] = {
        "score": zone_score,
        "type": ob_type,
        "zone_low": float(ob_zone["low"]) if ob_zone else None,
        "zone_high": float(ob_zone["high"]) if ob_zone else None,
        "current_price_vs_zone": "inside" if (ob_type == "bullish" and last["close"] <= ob_zone["high"]) or 
                                          (ob_type == "bearish" and last["close"] >= ob_zone["low"]) else "outside",
        "breakdown": ob_breakdown
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
            sig.get("displacement_value", 0.0),
            sig.get("zone_approach_score", 0),
            sig.get("htf_alignment_score", 0),
            sig.get("momentum_score", 0),
            sig.get("momentum_value", 0.0),
            sig.get("elite_mtf_score", 0),
            sig.get("market_regime", "UNKNOWN"),
            sig.get("order_block_type", "UNKNOWN"),
            sig.get("order_block_low", 0.0),
            sig.get("order_block_high", 0.0),
            sig.get("atr_value", 0.0),
            sig.get("risk_reward", 0.0),
            sig.get("breakdown_json", "{}")
        ))
        await db_conn.commit()

# ---------------- ENHANCED BREAKDOWN FORMATTING ----------------
async def send_enhanced_breakdown(sig):
    """
    Format and send the enhanced breakdown in the same style as code 1
    """
    breakdown = sig.get("breakdown", {})
    components = breakdown.get("components", {})
    
    # Start building the enhanced breakdown
    breakdown_lines = [
        f"🏆 {sig['symbol']} ({breakdown.get('timeframe', 'N/A')}) {sig['side']}",
        f"Entry: {format_number(sig['entry'])} | Score: {sig['score']}/9",
        f""
    ]
    
    # 📊 SWEEP DETAILS SECTION
    breakdown_lines.append(f"⚡ LIQUIDITY SWEEP DETAILS:")
    sweep_info = components.get("liquidity_sweep", {})
    sweep_details = sweep_info.get("sweep_details", {}).get("details", {})
    
    if sweep_info.get("has_sweep"):
        sweep_type = "HIGH" if sweep_info.get("sweep_high") else "LOW"
        breakdown_lines.extend([
            f"  • Type: {sweep_type} SWEEP",
            f"  • Score: +{sweep_info.get('score', 0)}",
            f"  • Current: {format_number(sweep_details.get('current_high' if sweep_type == 'HIGH' else 'current_low'))}",
            f"  • Previous: {format_number(sweep_details.get('previous_high' if sweep_type == 'HIGH' else 'previous_low'))}",
            f"  • Extension: {format_number(sweep_details.get('extension', 0))}",
            f"  • Extension %: {sweep_details.get('extension_pct', 0):.2f}%",
            f"  • Strength: {sweep_details.get('strength', 'N/A')}",
            f"  • Wick Size: {format_number(sweep_details.get('wick_size', 0))}",
            f"  • Volume: {format_number(sweep_details.get('volume', 0))}"
        ])
    else:
        breakdown_lines.append(f"  • No significant sweep detected")
    
    breakdown_lines.append(f"")
    
    # 🔷 ORDER BLOCK DETAILS SECTION
    breakdown_lines.append(f"🔷 ORDER BLOCK DETAILS:")
    ob_info = components.get("order_block", {})
    ob_breakdown = ob_info.get("breakdown", {})
    
    if ob_info.get("type"):
        ob_type = ob_info.get("type", "").upper()
        ob_low = ob_info.get("zone_low", 0)
        ob_high = ob_info.get("zone_high", 0)
        ob_range = ob_high - ob_low if ob_high and ob_low else 0
        ob_mid = (ob_low + ob_high) / 2 if ob_high and ob_low else 0
        distance_to_entry = abs(sig['entry'] - ob_mid) if ob_mid else 0
        distance_pct = (distance_to_entry / sig['entry'] * 100) if sig['entry'] > 0 else 0
        in_zone = ob_breakdown.get("in_zone", False)
        
        breakdown_lines.extend([
            f"  • Type: {ob_type} OB",
            f"  • Zone Approach: +{ob_info.get('score', 0)}",
            f"  • OB Range: {format_number(ob_low)} - {format_number(ob_high)}",
            f"  • Range Size: {format_number(ob_range)}",
            f"  • Midpoint: {format_number(ob_mid)}",
            f"  • Distance to Entry: {format_number(distance_to_entry)} ({distance_pct:.2f}%)",
            f"  • In Zone: {'✅ YES' if in_zone else '❌ NO'}",
            f"  • Strength: {ob_breakdown.get('strength', 0):.2f}"
        ])
    else:
        breakdown_lines.append(f"  • No order block detected")
    
    breakdown_lines.append(f"")
    
    # 📊 KEY METRICS SECTION
    breakdown_lines.append(f"📊 KEY METRICS:")
    
    # Displacement
    disp_info = components.get("displacement", {})
    disp_value = disp_info.get("value", 0)
    disp_score = disp_info.get("score", 0)
    breakdown_lines.append(f"  • Displacement: {disp_value:.2f} ({'✅ STRONG' if disp_value >= 0.6 else '⚠️ WEAK'})")
    
    # Momentum
    mom_info = components.get("momentum", {})
    mom_value = mom_info.get("ratio", 0)
    breakdown_lines.append(f"  • Momentum: {mom_value:.2f} {'✅ PASS' if mom_value >= 0.5 else '❌ FAIL'}")
    
    # HTF Alignment
    htf_info = components.get("htf_alignment", {})
    htf_trend = htf_info.get("trend_value", 0)
    htf_dir = htf_info.get("htf_direction", "?")
    breakdown_lines.append(f"  • HTF Trend: {htf_trend:+.6f}")
    breakdown_lines.append(f"  • HTF Direction: {htf_dir}")
    
    # Elite MTF
    elite_info = components.get("elite_mtf", {})
    elite_count = elite_info.get("alignment_count", 0)
    elite_total = elite_info.get("total_timeframes", 3)
    breakdown_lines.append(f"  • Elite MTF: {elite_count}/{elite_total} aligned")
    
    breakdown_lines.append(f"")
    
    # 🏛️ STRUCTURE-ONLY TP VALIDATION
    breakdown_lines.append(f"🏛️ STRUCTURE-ONLY TP VALIDATION:")
    
    risk_mgmt = breakdown.get("risk_management", {})
    if risk_mgmt:
        # Check if TPs are from structure
        tp1_data = risk_mgmt.get("tp1_data", {})
        tp2_data = risk_mgmt.get("tp2_data", {})
        tp3_data = risk_mgmt.get("tp3_data", {})
        
        breakdown_lines.append(f"  • TP1 Source: {tp1_data.get('type', 'structure') if tp1_data else 'N/A'}")
        if tp2_data:
            breakdown_lines.append(f"  • TP2 Source: {tp2_data.get('type', 'structure')}")
        if tp3_data:
            tp3_type = tp3_data.get('type', 'structure')
            if tp3_type == "extended_rr":
                breakdown_lines.append(f"  • TP3 Source: Extended target ({tp3_data.get('rr_multiple', 0)}R)")
            else:
                breakdown_lines.append(f"  • TP3 Source: {tp3_type}")
        
        breakdown_lines.append(f"  • Structure Levels Found: ✅ YES")
    else:
        breakdown_lines.append(f"  • Structure Levels Found: ❌ NO (Signal would be rejected)")
    
    breakdown_lines.append(f"")
    
    # 🎯 TARGETS SECTION
    breakdown_lines.append(f"🎯 STRUCTURE-BASED TARGETS:")
    
    if "sl" in sig and sig["sl"]:
        risk = abs(sig['entry'] - sig['sl'])
        breakdown_lines.extend([
            f"  SL: {format_number(sig.get('sl', 0))}",
            f"  TP1: {format_number(sig.get('tp1', 0))} ({risk_mgmt.get('risk_reward', 0):.1f}R)",
        ])
        
        if sig.get('tp2'):
            tp2_dist = abs(sig['tp2'] - sig['entry'])
            tp2_r = tp2_dist / risk if risk > 0 else 0
            breakdown_lines.append(f"  TP2: {format_number(sig.get('tp2', 0))} ({tp2_r:.1f}R)")
        
        if sig.get('tp3'):
            tp3_dist = abs(sig['tp3'] - sig['entry'])
            tp3_r = tp3_dist / risk if risk > 0 else 0
            breakdown_lines.append(f"  TP3: {format_number(sig.get('tp3', 0))} ({tp3_r:.1f}R)")
        
        breakdown_lines.append(f"  Risk: {format_number(risk)}")
        breakdown_lines.append(f"  ATR: {format_number(risk_mgmt.get('atr_value', 0))}")
    
    breakdown_lines.append(f"")
    
    # 📈 MARKET CONDITIONS
    breakdown_lines.append(f"📈 MARKET CONDITIONS:")
    breakdown_lines.extend([
        f"  • Regime: {sig.get('market_regime', 'N/A')}",
        f"  • Trend MA: {format_number(sig.get('trend_ma_value', 0))}",
        f"  • Price vs MA: {format_number(sig.get('price_vs_ma', 0))}"
    ])
    
    # 🔍 FILTER STATUS
    breakdown_lines.append(f"")
    breakdown_lines.append(f"🔍 FILTER STATUS:")
    
    # Critical filters
    crit_score = breakdown.get("critical_score", 0)
    crit_thresh = breakdown.get("critical_threshold", 2)
    breakdown_lines.append(f"  • Critical Score: {crit_score}/{crit_thresh} {'✅ PASS' if crit_score >= crit_thresh else '❌ FAIL'}")
    
    # Minimum score
    min_score = MIN_SCORE
    actual_score = sig.get("score", 0)
    breakdown_lines.append(f"  • Minimum Score: {actual_score}/{min_score} {'✅ PASS' if actual_score >= min_score else '❌ FAIL'}")
    
    # HTF Alignment
    htf_score = sig.get("htf_alignment_score", 0)
    breakdown_lines.append(f"  • HTF Alignment: {'✅ MANDATORY PASS' if htf_score == 1 else '❌ MANDATORY FAIL'}")
    
    # Elite MTF
    elite_score = sig.get("elite_mtf_score", 0)
    breakdown_lines.append(f"  • Elite MTF: {'✅ PASS' if elite_score == 1 else '❌ FAIL'}")
    
    # STRUCTURE VALIDATION
    has_structure = "risk_management" in breakdown
    breakdown_lines.append(f"  • Structure Validation: {'✅ PASS' if has_structure else '❌ FAIL (NO TRADE)'}")
    
    # Clean up empty lines
    breakdown_lines = [line for line in breakdown_lines if line != ""]
    
    # Send to Telegram
    try:
        await tg("\n".join(breakdown_lines))
    except Exception as e:
        log.error(f"Failed to send Telegram breakdown: {e}")

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    """
    Monitor open positions for TP/SL hits
    - TP/SL levels are FIXED after entry (not recalculated)
    - Only checks if price hit predefined levels
    - No structure revalidation during monitoring
    - FIXED: Handle None values for TP2/TP3
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
                            # Check TP2 only if it exists and is not None
                            if tp2 is not None and not tp2_hit and last_price >= tp2:
                                hits.append("TP2")
                                tp2_hit = 1
                            # Check TP3 only if it exists and is not None
                            if tp3 is not None and not tp3_hit and last_price >= tp3:
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
                            # Check TP2 only if it exists and is not None
                            if tp2 is not None and not tp2_hit and last_price <= tp2:
                                hits.append("TP2")
                                tp2_hit = 1
                            # Check TP3 only if it exists and is not None
                            if tp3 is not None and not tp3_hit and last_price <= tp3:
                                hits.append("TP3")
                                tp3_hit = 1
                            if last_price >= sl:
                                hits.append("SL")
                                status = "CLOSED"
                                sl_hit = True
                        
                        if hits:
                            msg = f"🎯 {symbol} {side} update\nEntry: {entry:.8f}\nLast: {last_price:.8f}\nHits: {','.join(hits)}\nSL: {sl:.8f}"
                            if tp1: msg += f"\nTP1: {tp1:.8f}"
                            if tp2: msg += f" TP2: {tp2:.8f}" if tp2 else ""
                            if tp3: msg += f" TP3: {tp3:.8f}" if tp3 else ""
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
                        # Send enhanced breakdown (same format as code 1)
                        await send_enhanced_breakdown(sig)
                        
                        # Log to database
                        await log_signal(sig)
                        
                        last_signal_time[key] = time.time()
                        signals_found += 1
                        
                        # Small delay between signals to avoid rate limits
                        await asyncio.sleep(0.5)
            
            log.info(f"📊 Scan complete: {signals_found} RomeOPT signals found (Structure-Only TP/SL)")
            
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
    
    await tg("🏆 ROMEOPT 6-Step Scanner Started - Structure-Only TP/SL with Enhanced Breakdown")
    await tg("📊 ENHANCED BREAKDOWN ACTIVATED - Full OB & Sweep Details")
    await tg("🏛️ STRUCTURE-ONLY TP/SL: TPs must be at valid structure levels")
    await tg("📈 NO STRUCTURE = NO TRADE: Signals rejected if no valid TP levels")
    
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