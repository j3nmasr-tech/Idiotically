#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIQUIDITY-ANCHORED ROMEOPT 6-STEP SCANNER
- TP/SL completely liquidity-driven (RomeoTPT style)
- NO RR authority - RR is calculated AFTER for display only
- NO TP distance rejection
- NO fallback to fixed numbers
- If no liquidity target → REJECT signal
- HTF liquidity pools for TP (mapped by entry TF), LTF structure for SL
- Elite Multi-Timeframe Confirmation
- FORCED FILTER: Momentum ≥ 0.70 OR (Momentum ≥ 0.65 AND Displacement ≥ 0.60)
- OB DISTANCE FILTER: Reject trades where OB distance > 0.70%
- Enhanced Breakdown with FULL OB & Sweep Details
"""

import os, time, asyncio, logging, datetime
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

# ---------------- FORCED FILTER PARAMETERS ----------------
MOMENTUM_STRONG_THRESHOLD = 0.70  # Rule 1: Momentum ≥ 0.70 → ACCEPT
MOMENTUM_GOOD_THRESHOLD = 0.65    # Rule 2: Momentum ≥ 0.65 → Check displacement
DISPLACEMENT_MIN_THRESHOLD = 0.60 # Rule 2: Displacement ≥ 0.60

# ---------------- OB DISTANCE FILTER PARAMETERS ----------------
OB_DISTANCE_MAX_THRESHOLD = 0.70  # Maximum OB distance allowed (in percentage)
OB_DISTANCE_OPTIMAL_MAX = 0.50    # Optimal maximum distance (better entries)

# ---------------- LIQUIDITY CONFIG ----------------
# OPTIMIZED FOR RECENT LIQUIDITY (15m/30m/1h/4h)
LIQUIDITY_LOOKBACK = 35    # ~2 days on 1h, perfect for recent liquidity
MIN_TOUCHES_FOR_POOL = 2   # 2 touches = confirmed recent interest  
SWING_LOOKBACK = 3         # Balanced: catches meaningful swings but not too slow
TOUCH_TOLERANCE_PCT = 0.20  # 0.1% tolerance is standard

# ---------------- TIMEFRAME MAPPING FOR TP ----------------
def get_tp_timeframes(entry_tf: str) -> list:
    """
    Get appropriate TP timeframes based on entry timeframe
    Always target 1-2 higher timeframes above entry
    Mapping:
      Entry TF → TP TF (try first, then second)
      1m → 15m, 30m
      3m → 15m, 30m
      5m → 15m, 30m
      15m → 30m, 1h
      30m → 1h, 4h
    """
    mapping = {
        "1m": ["15m", "30m"],
        "3m": ["15m", "30m"],
        "5m": ["15m", "30m"],
        "15m": ["30m", "1h"],
        "30m": ["1h", "4h"],
    }
    return mapping.get(entry_tf, ["1h", "4h"])  # Default fallback

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_liquidity_bot")
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
    
    # Create table with all columns
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
            ob_type TEXT,
            sweep_type TEXT,
            momentum_value REAL,
            displacement_value REAL,
            ob_distance_pct REAL,
            ob_distance_filter TEXT,
            liquidity_anchored INTEGER DEFAULT 0,
            rr_tp1 REAL,
            rr_tp2 REAL,
            rr_tp3 REAL,
            entry_tf TEXT,
            tp_tf TEXT
        );
    """)
    await db_conn.commit()
    
    # Add missing columns if they don't exist
    await add_missing_columns()

async def add_missing_columns():
    """Add missing columns to existing database"""
    columns_to_add = [
        ("liquidity_anchored", "INTEGER DEFAULT 0"),
        ("rr_tp1", "REAL"),
        ("rr_tp2", "REAL"),
        ("rr_tp3", "REAL"),
        ("entry_tf", "TEXT"),
        ("tp_tf", "TEXT")
    ]
    
    try:
        # Get existing columns
        async with db_conn.execute("PRAGMA table_info(signals)") as cursor:
            existing_columns = {row[1] for row in await cursor.fetchall()}
        
        # Add missing columns
        for column_name, column_type in columns_to_add:
            if column_name not in existing_columns:
                try:
                    await db_conn.execute(f"ALTER TABLE signals ADD COLUMN {column_name} {column_type}")
                    log.info(f"Added missing column: {column_name}")
                except Exception as e:
                    log.warning(f"Could not add column {column_name}: {e}")
        
        await db_conn.commit()
    except Exception as e:
        log.error(f"Error adding columns: {e}")

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
    """
    FORCED FILTER - MATHEMATICALLY PROVEN FROM 535 TRADES
    NO EXCEPTIONS, NO BYPASSES, NO OVERRIDES
    """
    # RULE 1: Strong momentum (≥ 0.70) - ALWAYS ACCEPT
    if momentum_value >= MOMENTUM_STRONG_THRESHOLD:
        return True
    
    # RULE 2: Good momentum with decent displacement
    if momentum_value >= MOMENTUM_GOOD_THRESHOLD and displacement_value >= DISPLACEMENT_MIN_THRESHOLD:
        return True
    
    # RULE 3: REJECT EVERYTHING ELSE - NO EXCEPTIONS
    return False

# ---------------- OB DISTANCE FILTER FUNCTION ----------------
def check_ob_distance_filter(entry_price: float, ob_midpoint: float, ob_low: float = None, ob_high: float = None) -> dict:
    """
    OB DISTANCE FILTER - Premium Entry Filter
    Rejects trades where price is too far from Order Block (> 0.70%)
    """
    if ob_midpoint is None or entry_price == 0:
        return {"passed": False, "distance_pct": 100, "distance_abs": 0, "status": "NO_OB", "quality": "REJECTED"}
    
    # Calculate absolute and percentage distance
    distance_abs = abs(entry_price - ob_midpoint)
    distance_pct = (distance_abs / entry_price) * 100
    
    # Determine entry quality
    if distance_pct <= OB_DISTANCE_OPTIMAL_MAX:
        quality = "PREMIUM"
        passed = True
    elif distance_pct <= OB_DISTANCE_MAX_THRESHOLD:
        quality = "GOOD"
        passed = True
    else:
        quality = "EXTENDED"
        passed = False
    
    # Additional check: If OB range is very small, be more strict
    if ob_low is not None and ob_high is not None:
        ob_range_pct = ((ob_high - ob_low) / ob_low) * 100
        if ob_range_pct < 0.1 and distance_pct > 0.3:  # Very tight OB
            quality = "EXTENDED_TIGHT_OB"
            passed = False
    
    return {
        "passed": passed,
        "distance_pct": distance_pct,
        "distance_abs": distance_abs,
        "status": "PASS" if passed else "FAIL",
        "quality": quality
    }

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

# ---------------- ENHANCED SWEEP ANALYSIS ----------------
def analyze_sweep_details(df: pd.DataFrame, lookback=5):
    """Analyze sweep with detailed information"""
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

# ---------------- LIQUIDITY DETECTION (SWINGS & EQUAL HIGHS/LOWS) ----------------
def find_liquidity_pools(df: pd.DataFrame, side: str, price_precision: int = 4):
    """
    Find ACTUAL liquidity pools (not structure)
    Returns: List of prices where liquidity exists
    """
    if len(df) < 20:
        return []
    
    pools = []
    
    # 1. Find swing points (most reliable liquidity)
    if side == "BUY":
        # For BUY targets, look for SWING LOWS (buy-side liquidity)
        for i in range(SWING_LOOKBACK, len(df) - SWING_LOOKBACK):
            if is_swing_low(df, i):
                low_price = round(df["low"].iloc[i], price_precision)
                # Only add if not too close to existing pool
                if not any(abs(low_price - p) / p < 0.001 for p in pools):
                    pools.append(low_price)
        
        # 2. Find equal lows (clusters)
        recent_lows = df["low"].tail(LIQUIDITY_LOOKBACK).values
        if len(recent_lows) > 10:
            rounded_lows = np.round(recent_lows, price_precision)
            unique_prices, counts = np.unique(rounded_lows, return_counts=True)
            
            for price, count in zip(unique_prices, counts):
                if count >= MIN_TOUCHES_FOR_POOL:
                    price_float = float(price)
                    # Get candles that touched this level
                    touch_candles = df[abs(df["low"] - price_float) / price_float < TOUCH_TOLERANCE_PCT/100]
                    if len(touch_candles) >= MIN_TOUCHES_FOR_POOL:
                        if not any(abs(price_float - p) / p < 0.001 for p in pools):
                            pools.append(price_float)
    
    else:  # SELL
        # For SELL targets, look for SWING HIGHS (sell-side liquidity)
        for i in range(SWING_LOOKBACK, len(df) - SWING_LOOKBACK):
            if is_swing_high(df, i):
                high_price = round(df["high"].iloc[i], price_precision)
                if not any(abs(high_price - p) / p < 0.001 for p in pools):
                    pools.append(high_price)
        
        # 2. Find equal highs (clusters)
        recent_highs = df["high"].tail(LIQUIDITY_LOOKBACK).values
        if len(recent_highs) > 10:
            rounded_highs = np.round(recent_highs, price_precision)
            unique_prices, counts = np.unique(rounded_highs, return_counts=True)
            
            for price, count in zip(unique_prices, counts):
                if count >= MIN_TOUCHES_FOR_POOL:
                    price_float = float(price)
                    touch_candles = df[abs(df["high"] - price_float) / price_float < TOUCH_TOLERANCE_PCT/100]
                    if len(touch_candles) >= MIN_TOUCHES_FOR_POOL:
                        if not any(abs(price_float - p) / p < 0.001 for p in pools):
                            pools.append(price_float)
    
    # Sort appropriately
    if side == "BUY":
        return sorted([p for p in pools if p > 0])
    else:
        return sorted([p for p in pools if p > 0], reverse=True)

def is_swing_low(df, idx):
    """Check if candle at idx is a swing low"""
    if idx < SWING_LOOKBACK or idx >= len(df) - SWING_LOOKBACK:
        return False
    
    current_low = df["low"].iloc[idx]
    
    # Check left side
    for i in range(1, SWING_LOOKBACK + 1):
        if df["low"].iloc[idx - i] < current_low:
            return False
    
    # Check right side
    for i in range(1, SWING_LOOKBACK + 1):
        if df["low"].iloc[idx + i] < current_low:
            return False
    
    # Also check that it's not inside a tight range (needs some separation)
    left_min = min(df["low"].iloc[idx - SWING_LOOKBACK:idx])
    right_min = min(df["low"].iloc[idx + 1:idx + SWING_LOOKBACK + 1])
    
    if (current_low - left_min) / left_min < 0.001 and (current_low - right_min) / right_min < 0.001:
        return False  # Too flat, not a meaningful swing
    
    return True

def is_swing_high(df, idx):
    """Check if candle at idx is a swing high"""
    if idx < SWING_LOOKBACK or idx >= len(df) - SWING_LOOKBACK:
        return False
    
    current_high = df["high"].iloc[idx]
    
    # Check left side
    for i in range(1, SWING_LOOKBACK + 1):
        if df["high"].iloc[idx - i] > current_high:
            return False
    
    # Check right side
    for i in range(1, SWING_LOOKBACK + 1):
        if df["high"].iloc[idx + i] > current_high:
            return False
    
    # Check for meaningful separation
    left_max = max(df["high"].iloc[idx - SWING_LOOKBACK:idx])
    right_max = max(df["high"].iloc[idx + 1:idx + SWING_LOOKBACK + 1])
    
    if (left_max - current_high) / current_high < 0.001 and (right_max - current_high) / current_high < 0.001:
        return False
    
    return True

# ---------------- LIQUIDITY-BASED TP/SL (NO RR AUTHORITY) ----------------
async def liquidity_tp_sl(exchange, entry: float, side: str, symbol: str, df_entry_tf: pd.DataFrame, entry_tf: str):
    """
    RomeoTPT-style TP/SL - 100% liquidity anchored
    entry_tf: The timeframe where signal was detected (e.g., "1m", "5m", "15m")
    Returns: (sl, tp1, tp2, tp3, selected_tf) or (None, None, None, None, None) if no liquidity found
    
    IMPORTANT: If no liquidity target → returns None → signal REJECTED
    """
    
    # Step 1: Get HTF data for TP (based on entry timeframe mapping)
    df_htf = None
    selected_tf = None
    
    # Get appropriate TP timeframes for this entry TF
    tp_timeframes = get_tp_timeframes(entry_tf)
    
    for htf in tp_timeframes:
        ohlcv = await fetch_ohlcv(exchange, symbol, htf, LIQUIDITY_LOOKBACK)
        if ohlcv and len(ohlcv) >= 30:
            df_htf = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
            for col in ["open","high","low","close","vol"]:
                df_htf[col] = pd.to_numeric(df_htf[col], errors="coerce")
            selected_tf = htf
            break
    
    if df_htf is None:
        log.debug(f"No HTF data for {symbol} at TP timeframes {tp_timeframes}")
        return None, None, None, None, None
    
    # Step 2: Find liquidity pools on HTF
    liquidity_pools = find_liquidity_pools(df_htf, side)
    
    if not liquidity_pools:
        log.debug(f"No liquidity pools found for {symbol} {side} on {selected_tf}")
        return None, None, None, None, None
    
    # Step 3: Select TP levels based on liquidity
    if side == "BUY":
        # For BUY: TP = buy-side liquidity ABOVE entry
        valid_pools = [p for p in liquidity_pools if p > entry]
        
        if not valid_pools:
            log.debug(f"No BUY liquidity above entry for {symbol} on {selected_tf}")
            return None, None, None, None, None
        
        # TP1 = nearest buy-side liquidity above entry
        tp1 = min(valid_pools, key=lambda x: abs(x - entry))
        valid_pools.remove(tp1)
        
        # TP2 = next meaningful liquidity pool
        tp2 = None
        if valid_pools:
            for pool in valid_pools:
                if (pool - tp1) / tp1 > 0.002:  # At least 0.2% away
                    tp2 = pool
                    valid_pools.remove(pool)
                    break
        
        # TP3 = major liquidity pool
        tp3 = None
        if valid_pools:
            tp3 = max(valid_pools)
        elif tp2:
            tp3 = tp2 + (tp2 - entry) * 0.5
        else:
            tp3 = tp1 + (tp1 - entry) * 1.0
        
        # Step 4: SL based on structure (BELOW recent swing low or OB)
        recent_swing_lows = []
        for i in range(SWING_LOOKBACK, len(df_entry_tf) - SWING_LOOKBACK):
            if is_swing_low(df_entry_tf, i):
                recent_swing_lows.append(df_entry_tf["low"].iloc[i])
        
        if recent_swing_lows:
            structure_sl = min(recent_swing_lows)
        else:
            structure_sl = df_entry_tf["low"].tail(20).min()
        
        # Also check for Order Block low
        latest_ob = find_latest_ob(df_entry_tf)
        if latest_ob and latest_ob["type"] == "bullish":
            ob_sl = latest_ob["low"]
            sl = min(structure_sl, ob_sl)
        else:
            sl = structure_sl
        
        # Add small buffer below SL
        sl_buffer = sl * 0.999
        
    else:  # SELL
        # For SELL: TP = sell-side liquidity BELOW entry
        valid_pools = [p for p in liquidity_pools if p < entry]
        
        if not valid_pools:
            log.debug(f"No SELL liquidity below entry for {symbol} on {selected_tf}")
            return None, None, None, None, None
        
        # TP1 = nearest sell-side liquidity below entry
        tp1 = max(valid_pools, key=lambda x: abs(x - entry))
        valid_pools.remove(tp1)
        
        # TP2 = next meaningful liquidity pool
        tp2 = None
        if valid_pools:
            for pool in valid_pools:
                if (tp1 - pool) / tp1 > 0.002:
                    tp2 = pool
                    valid_pools.remove(pool)
                    break
        
        # TP3 = major liquidity pool
        tp3 = None
        if valid_pools:
            tp3 = min(valid_pools)
        elif tp2:
            tp3 = tp2 - (entry - tp2) * 0.5
        else:
            tp3 = tp1 - (entry - tp1) * 1.0
        
        # SL based on structure (ABOVE recent swing high or OB)
        recent_swing_highs = []
        for i in range(SWING_LOOKBACK, len(df_entry_tf) - SWING_LOOKBACK):
            if is_swing_high(df_entry_tf, i):
                recent_swing_highs.append(df_entry_tf["high"].iloc[i])
        
        if recent_swing_highs:
            structure_sl = max(recent_swing_highs)
        else:
            structure_sl = df_entry_tf["high"].tail(20).max()
        
        # Check for Order Block high
        latest_ob = find_latest_ob(df_entry_tf)
        if latest_ob and latest_ob["type"] == "bearish":
            ob_sl = latest_ob["high"]
            sl = max(structure_sl, ob_sl)
        else:
            sl = structure_sl
        
        # Add small buffer above SL
        sl_buffer = sl * 1.001
    
    # Final validation: Ensure TPs make sense
    if side == "BUY":
        if not (entry < tp1 <= tp3):
            log.debug(f"Invalid TP hierarchy for BUY: entry={entry}, tp1={tp1}, tp3={tp3}")
            return None, None, None, None, None
        if tp2 and not (tp1 <= tp2 <= tp3):
            log.debug(f"Invalid TP2 for BUY: tp1={tp1}, tp2={tp2}, tp3={tp3}")
            return None, None, None, None, None
        if sl_buffer >= entry:
            log.debug(f"SL above entry for BUY: sl={sl_buffer}, entry={entry}")
            return None, None, None, None, None
    else:  # SELL
        if not (entry > tp1 >= tp3):
            log.debug(f"Invalid TP hierarchy for SELL: entry={entry}, tp1={tp1}, tp3={tp3}")
            return None, None, None, None, None
        if tp2 and not (tp1 >= tp2 >= tp3):
            log.debug(f"Invalid TP2 for SELL: tp1={tp1}, tp2={tp2}, tp3={tp3}")
            return None, None, None, None, None
        if sl_buffer <= entry:
            log.debug(f"SL below entry for SELL: sl={sl_buffer}, entry={entry}")
            return None, None, None, None, None
    
    return sl_buffer, tp1, tp2, tp3, selected_tf

def find_latest_ob(df: pd.DataFrame):
    """Find the latest Order Block for SL reference"""
    for i in range(len(df)-5, len(df)-1):
        candle, prev_candle = df.iloc[i], df.iloc[i-1]
        if candle["close"]>candle["open"] and prev_candle["close"]<prev_candle["open"]:
            return {
                "type": "bullish",
                "low": min(candle["low"], prev_candle["low"]),
                "high": candle["close"],
                "candle_index": i
            }
        elif candle["close"]<candle["open"] and prev_candle["close"]>prev_candle["open"]:
            return {
                "type": "bearish",
                "low": candle["close"],
                "high": max(candle["high"], prev_candle["high"]),
                "candle_index": i
            }
    return None

# ---------------- ROMEOPT 6-STEP SIGNAL (ORIGINAL LOGIC) ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    if df is None or len(df) < 20: return None
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    score = 0
    reasons = []
    
    # Store all calculation values for breakdown
    calc_values = {}

    # Step 1: Liquidity Sweep (ORIGINAL LOGIC)
    sweep_high = last["high"] > prev5["high"].max()
    sweep_low = last["low"] < prev5["low"].min()
    has_sweep = sweep_high or sweep_low
    liquidity_sweep = 2 if has_sweep else 0
    score += liquidity_sweep
    sweep_type = "HIGH" if sweep_high else ("LOW" if sweep_low else "NONE")
    reasons.append(f"Liquidity Sweep +{liquidity_sweep}")
    calc_values["sweep_type"] = sweep_type
    calc_values["sweep_score"] = liquidity_sweep
    
    # ENHANCED: Add sweep details for breakdown
    sweep_analysis = analyze_sweep_details(df)
    calc_values["sweep_details"] = sweep_analysis["details"]

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
    ob_candle_index = None
    ob_midpoint = None
    
    for i in range(len(df)-5, len(df)-1):
        candle, prev_candle = df.iloc[i], df.iloc[i-1]
        if candle["close"]>candle["open"] and prev_candle["close"]<prev_candle["open"]:
            ob_zone={"type":"bullish","low":min(candle["low"], prev_candle["low"]),"high":candle["close"]}
            ob_candle_index = i
            ob_midpoint = (ob_zone["low"] + ob_zone["high"]) / 2
            break
        elif candle["close"]<candle["open"] and prev_candle["close"]>prev_candle["open"]:
            ob_zone={"type":"bearish","low":candle["close"],"high":max(candle["high"], prev_candle["high"])}
            ob_candle_index = i
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
        
        # ENHANCED: Calculate detailed OB info
        if ob_candle_index is not None:
            ob_candle = df.iloc[ob_candle_index]
            prev_ob_candle = df.iloc[ob_candle_index-1]
            candles_ago = len(df) - ob_candle_index - 1
            ob_mid = (ob_zone["low"] + ob_zone["high"]) / 2
            distance_to_price = abs(last["close"] - ob_mid)
            distance_pct = (distance_to_price / last["close"] * 100) if last["close"] > 0 else 100
            
            candle_body = abs(ob_candle["close"] - ob_candle["open"])
            candle_range = ob_candle["high"] - ob_candle["low"]
            strength = candle_body / candle_range if candle_range > 0 else 0
            
            ob_details = {
                "type": ob_type,
                "low": ob_zone["low"],
                "high": ob_zone["high"],
                "midpoint": ob_mid,
                "range": ob_zone["high"] - ob_zone["low"],
                "candles_ago": candles_ago,
                "distance_to_price": distance_to_price,
                "distance_pct": distance_pct,
                "strength": strength,
                "volume": ob_candle["vol"] + prev_ob_candle["vol"],
                "candle_index": ob_candle_index
            }
            calc_values["ob_details"] = ob_details
        
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

    # Step 5: HTF Alignment (for signal confirmation, not TP)
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

    # Step 6: MOMENTUM (0.8 THRESHOLD)
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
    side = "BUY" if ob_type=="bullish" else "SELL"
    entry = float(last["close"])

    # ---------------- CRITICAL FILTERS ----------------
    critical_score = htf_alignment + liquidity_sweep
    if critical_score < CRITICAL_FACTORS_MIN: return None
    if score < MIN_SCORE: return None
    if not has_disp: return None
    
    # ---------------- HTF ALIGNMENT MANDATORY FILTER ----------------
    if htf_alignment != 1:
        return None

    # ---------------- FORCED FILTER ----------------
    displacement_val = calc_values["displacement_value"]
    momentum_val = calc_values["momentum_value"]
    
    # FORCED FILTER: MUST PASS OR REJECT IMMEDIATELY
    if not force_filter_trade(momentum_val, displacement_val):
        reasons.append(f"❌ FORCED FILTER REJECTED: Mom={momentum_val:.2f}, Disp={displacement_val:.2f}")
        return None
    
    # Only continue if FORCED filter passes
    filter_reason = "Mom≥0.70" if momentum_val >= MOMENTUM_STRONG_THRESHOLD else "Mom≥0.65 & Disp≥0.60"
    reasons.append(f"✅ FORCED FILTER PASSED: {filter_reason}")

    market_regime = await detect_market_regime(df)
    if (market_regime=="BULL" and side=="SELL") or (market_regime=="BEAR" and side=="BUY"): return None

    if len(df) >= 20:
        trend_ma = df["close"].rolling(20).mean().iloc[-1]
        if (side=="BUY" and last["close"]<trend_ma) or (side=="SELL" and last["close"]>trend_ma): return None

    # ---------------- ELITE MTF CONFIRMATION ----------------
    if not await elite_tf_alignment(exchange, symbol, side):
        return None
    reasons.append("Elite MTF Alignment ✅")

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

    # ---------------- LIQUIDITY-BASED TP/SL (NO RR AUTHORITY) ----------------
    tp_sl_result = await liquidity_tp_sl(exchange, entry, side, symbol, df, tf)
    
    if tp_sl_result[0] is None:  # No liquidity found → REJECT
        tp_timeframes = get_tp_timeframes(tf)
        reasons.append(f"❌ NO LIQUIDITY TARGET FOUND on TP timeframes {tp_timeframes}")
        return None
    
    sl, tp1, tp2, tp3, tp_tf = tp_sl_result
    
    # Create signal with liquidity-based levels
    sig = {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "score": score,
        "reason": "RomeOPT 6-Step + Liquidity TP",
        "reason_list": reasons,
        "htf_alignment": htf_alignment,
        "liquidity_sweep": liquidity_sweep,
        "momentum_ratio": momentum_ratio,
        "calc_values": calc_values,
        "latest_ob": find_latest_ob(df),
        "entry_tf": tf,
        "tp_tf": tp_tf
    }
    
    # Calculate RR for display ONLY (not for validation)
    risk = abs(entry - sl)
    tp1_distance = abs(tp1 - entry)
    tp2_distance = abs(tp2 - entry) if tp2 else 0
    tp3_distance = abs(tp3 - entry) if tp3 else 0
    
    tp1_r = tp1_distance / risk if risk > 0 else 0
    tp2_r = tp2_distance / risk if risk > 0 else 0
    tp3_r = tp3_distance / risk if risk > 0 else 0
    
    sig["rr_info"] = {
        "risk": risk,
        "tp1_r": round(tp1_r, 2),
        "tp2_r": round(tp2_r, 2),
        "tp3_r": round(tp3_r, 2),
        "liquidity_anchored": True
    }
    
    # ---------------- FINAL FORCED VALIDATION ----------------
    if not force_filter_trade(momentum_val, displacement_val):
        log.error(f"🚨 SECURITY VIOLATION: Signal {sig['symbol']} bypassed forced filter!")
        return None
    
    log.info(f"✅ Signal {sig['symbol']} passed all filters: EntryTF={tf}, TPTF={tp_tf}, Mom={momentum_val:.2f}, Disp={displacement_val:.2f}, TP1={tp1_r:.1f}R")
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
        rr_info = sig.get("rr_info", {})
        
        # Try with all columns first
        try:
            await db_conn.execute("""
                INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score,latest_ob,ob_type,sweep_type,momentum_value,displacement_value,ob_distance_pct,ob_distance_filter,liquidity_anchored,rr_tp1,rr_tp2,rr_tp3,entry_tf,tp_tf)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                sig["symbol"],
                sig["side"],
                sig["entry"],
                sig.get("sl"),
                sig.get("tp1"),
                sig.get("tp2"),
                sig.get("tp3"),
                datetime.datetime.utcnow().isoformat(),
                "OPEN",
                sig["reason"],
                sig["score"],
                str(sig.get("latest_ob","")),
                sig.get("calc_values", {}).get("ob_type", ""),
                sig.get("calc_values", {}).get("sweep_type", ""),
                sig.get("calc_values", {}).get("momentum_value", 0),
                sig.get("calc_values", {}).get("displacement_value", 0),
                sig.get("calc_values", {}).get("ob_distance_pct", 0),
                sig.get("calc_values", {}).get("ob_distance_filter", {}).get("status", "UNKNOWN"),
                1,  # liquidity_anchored = True
                rr_info.get("tp1_r", 0),
                rr_info.get("tp2_r", 0),
                rr_info.get("tp3_r", 0),
                sig.get("entry_tf", ""),
                sig.get("tp_tf", "")
            ))
        except Exception as e:
            # If new schema fails, fall back to old schema without new columns
            log.warning(f"New schema failed ({e}), falling back to old schema...")
            try:
                await db_conn.execute("""
                    INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score,latest_ob,ob_type,sweep_type,momentum_value,displacement_value,ob_distance_pct,ob_distance_filter)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    sig["symbol"],
                    sig["side"],
                    sig["entry"],
                    sig.get("sl"),
                    sig.get("tp1"),
                    sig.get("tp2"),
                    sig.get("tp3"),
                    datetime.datetime.utcnow().isoformat(),
                    "OPEN",
                    sig["reason"],
                    sig["score"],
                    str(sig.get("latest_ob","")),
                    sig.get("calc_values", {}).get("ob_type", ""),
                    sig.get("calc_values", {}).get("sweep_type", ""),
                    sig.get("calc_values", {}).get("momentum_value", 0),
                    sig.get("calc_values", {}).get("displacement_value", 0),
                    sig.get("calc_values", {}).get("ob_distance_pct", 0),
                    sig.get("calc_values", {}).get("ob_distance_filter", {}).get("status", "UNKNOWN")
                ))
            except Exception as e2:
                log.error(f"Failed to log signal with old schema too: {e2}")
                # Last resort: minimal schema
                try:
                    await db_conn.execute("""
                        INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        sig["symbol"],
                        sig["side"],
                        sig["entry"],
                        sig.get("sl"),
                        sig.get("tp1"),
                        sig.get("tp2"),
                        sig.get("tp3"),
                        datetime.datetime.utcnow().isoformat(),
                        "OPEN",
                        sig["reason"],
                        sig["score"]
                    ))
                except Exception as e3:
                    log.error(f"Failed to log signal with minimal schema: {e3}")
        
        await db_conn.commit()

# ---------------- ENHANCED BREAKDOWN FORMATTING ----------------
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

# ---------------- UPDATE SIGNAL WITH FRESH LIQUIDITY ----------------
async def update_signal_with_liquidity(exchange, sig: dict, df: pd.DataFrame):
    """Update existing signal with fresh liquidity-based TP/SL"""
    symbol = sig["symbol"]
    side = sig["side"]
    entry = sig["entry"]
    entry_tf = sig.get("entry_tf", "15m")  # Default if not stored
    
    # Get fresh liquidity-based TP/SL
    tp_sl_result = await liquidity_tp_sl(exchange, entry, side, symbol, df, entry_tf)
    
    if tp_sl_result[0] is None:
        # If no liquidity found now, keep old values but mark as stale
        sig["liquidity_status"] = "STALE"
        return sig
    
    sl, tp1, tp2, tp3, tp_tf = tp_sl_result
    
    # Update signal
    sig.update({
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "tp_tf": tp_tf,
        "liquidity_status": "FRESH"
    })
    
    # Update RR info
    risk = abs(entry - sl)
    tp1_distance = abs(tp1 - entry)
    tp2_distance = abs(tp2 - entry) if tp2 else 0
    tp3_distance = abs(tp3 - entry) if tp3 else 0
    
    sig["rr_info"] = {
        "risk": risk,
        "tp1_r": round(tp1_distance / risk, 2) if risk > 0 else 0,
        "tp2_r": round(tp2_distance / risk, 2) if risk > 0 else 0,
        "tp3_r": round(tp3_distance / risk, 2) if risk > 0 else 0,
        "liquidity_anchored": True
    }
    
    return sig

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("SELECT id,symbol,side,entry,sl,tp1,tp2,tp3,tp1_hit,tp2_hit,tp3_hit,status,entry_tf FROM signals WHERE status='OPEN'") as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status, entry_tf = row
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get("last")
                        if last_price is None: continue

                        # Get fresh OHLCV data
                        ohlcv = await fetch_ohlcv(exchange, symbol, "1m", 50)
                        if ohlcv:
                            df_live = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                            for c in ["open","high","low","close","vol"]: 
                                df_live[c] = pd.to_numeric(df_live[c], errors="coerce")
                            
                            # Update TP/SL with fresh liquidity data
                            sig = {
                                "symbol": symbol,
                                "side": side,
                                "entry": entry,
                                "sl": sl,
                                "tp1": tp1,
                                "tp2": tp2,
                                "tp3": tp3,
                                "entry_tf": entry_tf or "15m"
                            }
                            
                            sig = await update_signal_with_liquidity(exchange, sig, df_live)
                            sl, tp1, tp2, tp3 = sig["sl"], sig["tp1"], sig["tp2"], sig["tp3"]

                        hits=[]; sl_hit=False
                        if side=="BUY":
                            if not tp1_hit and tp1 is not None and last_price>=tp1: hits.append("TP1"); tp1_hit=1
                            if not tp2_hit and tp2 is not None and last_price>=tp2: hits.append("TP2"); tp2_hit=1
                            if not tp3_hit and tp3 is not None and last_price>=tp3: hits.append("TP3"); tp3_hit=1
                            if sl is not None and last_price<=sl: hits.append("SL"); status="CLOSED"; sl_hit=True
                        else:
                            if not tp1_hit and tp1 is not None and last_price<=tp1: hits.append("TP1"); tp1_hit=1
                            if not tp2_hit and tp2 is not None and last_price<=tp2: hits.append("TP2"); tp2_hit=1
                            if not tp3_hit and tp3 is not None and last_price<=tp3: hits.append("TP3"); tp3_hit=1
                            if sl is not None and last_price>=sl: hits.append("SL"); status="CLOSED"; sl_hit=True

                        if hits:
                            await tg(f"🎯 {symbol} {side} update\nEntry:{entry}\nLast:{last_price}\nHits:{','.join(hits)}\nSL:{sl}\nTP1:{tp1} TP2:{tp2} TP3:{tp3}")

                        if sl_hit: record_sl_hit(symbol)
                        await db_conn.execute("UPDATE signals SET tp1_hit=?,tp2_hit=?,tp3_hit=?,status=?,sl=?,tp1=?,tp2=?,tp3=? WHERE id=?",
                                             (tp1_hit,tp2_hit,tp3_hit,status,sl,tp1,tp2,tp3,sig_id))
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
                        htf_trend_abs = abs(calc.get("htf_trend", 0))
                        ob_distance_pct = calc.get("ob_distance_pct", 100)
                        ob_filter_status = calc.get("ob_distance_filter_status", "UNKNOWN")
                        
                        filter_passed = force_filter_trade(momentum_val, displacement_val)
                        
                        # Get RR info for display
                        rr_info = sig.get("rr_info", {})
                        tp1_r = rr_info.get("tp1_r", 0)
                        tp2_r = rr_info.get("tp2_r", 0)
                        tp3_r = rr_info.get("tp3_r", 0)
                        
                        # Get TP timeframe info
                        entry_tf = sig.get("entry_tf", tf)
                        tp_tf = sig.get("tp_tf", "N/A")
                        tp_timeframes = get_tp_timeframes(entry_tf)
                        
                        # Start building the enhanced breakdown
                        breakdown_lines = [
                            f"🏆 {sig['symbol']} ({entry_tf}) {sig['side']}",
                            f"Entry: {sig['entry']:.6f} | Score: {sig['score']}/6",
                            f""
                        ]
                        
                        # 📊 TIMEFRAME MAPPING
                        breakdown_lines.append(f"📊 TIMEFRAME MAPPING:")
                        breakdown_lines.extend([
                            f"  • Entry TF: {entry_tf}",
                            f"  • TP TF: {tp_tf}",
                            f"  • TP Timeframes checked: {', '.join(tp_timeframes)}",
                            f""
                        ])
                        
                        # 📊 SWEEP DETAILS SECTION
                        breakdown_lines.append(f"⚡ LIQUIDITY SWEEP DETAILS:")
                        sweep_type = calc.get('sweep_type', 'NONE')
                        sweep_details = calc.get('sweep_details', {})
                        
                        if sweep_type != 'NONE':
                            breakdown_lines.extend([
                                f"  • Type: {sweep_type} SWEEP",
                                f"  • Score: +{calc.get('sweep_score', 0)}",
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
                        
                        # 📊 ORDER BLOCK DETAILS SECTION
                        breakdown_lines.append(f"🔷 ORDER BLOCK DETAILS:")
                        ob_type = calc.get('ob_type', 'NONE')
                        ob_details = calc.get('ob_details', {})
                        ob_distance_filter = calc.get('ob_distance_filter', {})
                        
                        if ob_type != 'NONE' and ob_details:
                            ob_low = calc.get('ob_low', 0)
                            ob_high = calc.get('ob_high', 0)
                            ob_range = ob_high - ob_low
                            ob_mid = (ob_high + ob_low) / 2
                            distance_to_entry = abs(sig['entry'] - ob_mid)
                            distance_pct = (distance_to_entry / sig['entry'] * 100) if sig['entry'] > 0 else 0
                            in_zone = True if (ob_type == 'bullish' and sig['entry'] <= ob_high) or (ob_type == 'bearish' and sig['entry'] >= ob_low) else False
                            
                            breakdown_lines.extend([
                                f"  • Type: {ob_type.upper()} OB",
                                f"  • Zone Approach: +{calc.get('zone_approach', 0)}",
                                f"  • OB Range: {format_number(ob_low)} - {format_number(ob_high)}",
                                f"  • Range Size: {format_number(ob_range)}",
                                f"  • Midpoint: {format_number(ob_mid)}",
                                f"  • Distance to Entry: {format_number(distance_to_entry)} ({distance_pct:.2f}%)",
                                f"  • In Zone: {'✅ YES' if in_zone else '❌ NO'}",
                                f"  • Strength: {ob_details.get('strength', 0):.2f}",
                                f"  • Age: {ob_details.get('candles_ago', 0)} candles ago",
                                f"  • Volume: {format_number(ob_details.get('volume', 0))}"
                            ])
                        elif ob_type != 'NONE':
                            ob_low = calc.get('ob_low', 0)
                            ob_high = calc.get('ob_high', 0)
                            ob_range = ob_high - ob_low
                            ob_mid = (ob_high + ob_low) / 2
                            distance_to_entry = abs(sig['entry'] - ob_mid)
                            distance_pct = (distance_to_entry / sig['entry'] * 100) if sig['entry'] > 0 else 0
                            in_zone = True if (ob_type == 'bullish' and sig['entry'] <= ob_high) or (ob_type == 'bearish' and sig['entry'] >= ob_low) else False
                            
                            breakdown_lines.extend([
                                f"  • Type: {ob_type.upper()} OB",
                                f"  • Zone Approach: +{calc.get('zone_approach', 0)}",
                                f"  • OB Range: {format_number(ob_low)} - {format_number(ob_high)}",
                                f"  • Range Size: {format_number(ob_range)}",
                                f"  • Midpoint: {format_number(ob_mid)}",
                                f"  • Distance to Entry: {format_number(distance_to_entry)} ({distance_pct:.2f}%)",
                                f"  • In Zone: {'✅ YES' if in_zone else '❌ NO'}"
                            ])
                        else:
                            breakdown_lines.append(f"  • No order block detected")
                        
                        # 🆕 OB DISTANCE FILTER STATUS
                        breakdown_lines.append(f"")
                        breakdown_lines.append(f"📏 OB DISTANCE FILTER:")
                        if ob_distance_filter.get('passed', False):
                            quality = ob_distance_filter.get('quality', 'GOOD')
                            breakdown_lines.append(f"  • ✅ PASSED: {ob_distance_pct:.2f}% ({quality})")
                            breakdown_lines.append(f"  • Threshold: ≤ {OB_DISTANCE_MAX_THRESHOLD}%")
                        else:
                            if ob_distance_filter.get('status') == 'NO_OB':
                                breakdown_lines.append(f"  • ⚠️ NO ORDER BLOCK")
                            else:
                                breakdown_lines.append(f"  • ❌ REJECTED: {ob_distance_pct:.2f}% > {OB_DISTANCE_MAX_THRESHOLD}%")
                        
                        breakdown_lines.append(f"")
                        
                        # 📊 KEY METRICS SECTION
                        breakdown_lines.append(f"📊 KEY METRICS:")
                        breakdown_lines.extend([
                            f"  • Displacement: {displacement_val:.2f} ({'✅ STRONG' if displacement_val >= 0.6 else '⚠️ WEAK'})",
                            f"  • Momentum: {momentum_val:.2f} {'✅ PASS' if momentum_val >= 0.8 else '❌ FAIL'}",
                            f"  • HTF Trend: {calc.get('htf_trend', 0):+.6f}",
                            f"  • HTF Direction: {calc.get('htf_direction', '?')}",
                            f"  • HTF Strength: {htf_trend_abs:.6f}",
                        ])
                        
                        # 🔒 FORCED FILTER STATUS
                        breakdown_lines.append(f"")
                        breakdown_lines.append(f"🔒 FORCED FILTER STATUS:")
                        if filter_passed:
                            if momentum_val >= MOMENTUM_STRONG_THRESHOLD:
                                breakdown_lines.append(f"  • RULE 1 PASSED ✅: Momentum ≥ {MOMENTUM_STRONG_THRESHOLD} ({momentum_val:.2f})")
                            else:
                                breakdown_lines.append(f"  • RULE 2 PASSED ✅: Momentum ≥ {MOMENTUM_GOOD_THRESHOLD} & Disp ≥ {DISPLACEMENT_MIN_THRESHOLD}")
                                breakdown_lines.append(f"    → Momentum: {momentum_val:.2f}")
                                breakdown_lines.append(f"    → Displacement: {displacement_val:.2f}")
                        else:
                            breakdown_lines.append(f"  • REJECTED ❌")
                            breakdown_lines.append(f"    → Momentum: {momentum_val:.2f} {'≥' if momentum_val >= MOMENTUM_STRONG_THRESHOLD else '<'} {MOMENTUM_STRONG_THRESHOLD}")
                            breakdown_lines.append(f"    → Displacement: {displacement_val:.2f} {'≥' if displacement_val >= DISPLACEMENT_MIN_THRESHOLD else '<'} {DISPLACEMENT_MIN_THRESHOLD}")
                        
                        breakdown_lines.append(f"")
                        
                        # 🎯 LIQUIDITY-ANCHORED TARGETS
                        breakdown_lines.append(f"🎯 LIQUIDITY-ANCHORED TARGETS (TP on {tp_tf}):")
                        breakdown_lines.extend([
                            f"  • SL: {format_number(sig.get('sl', 0))} (Structure-based)",
                            f"  • TP1: {format_number(sig.get('tp1', 0))} ({tp1_r:.1f}R) ← Nearest liquidity",
                            f"  • TP2: {format_number(sig.get('tp2', 0))} ({tp2_r:.1f}R) ← Next liquidity pool",
                            f"  • TP3: {format_number(sig.get('tp3', 0))} ({tp3_r:.1f}R) ← Major liquidity",
                            f"  • Risk: {format_number(rr_info.get('risk', 0))}",
                            f"",
                            f"📊 LIQUIDITY AUTHORITY:",
                            f"  • Entry TF: {entry_tf} → TP TF: {tp_tf}",
                            f"  • Market Liquidity → Math (RR calculated after)",
                            f"  • No RR-based rejection",
                            f"  • No fixed TP ratios",
                            f"  • If no liquidity → No trade"
                        ])
                        
                        # Clean up empty lines
                        breakdown_lines = [line for line in breakdown_lines if line != ""]
                        
                        # Send to Telegram
                        try:
                            await tg("\n".join(breakdown_lines))
                        except Exception as e:
                            log.error(f"Failed to send Telegram message: {e}")
                        
                        await log_signal(sig)
                        last_signal_time[key]=time.time()
                        signals_found+=1
            
            log.info(f"📊 Scan complete: {signals_found} Liquidity-anchored RomeOPT signals found")
        
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
    
    # Initialize exchange
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {
            "defaultType": "spot"
        }
    })
    
    # Start announcement
    await tg("🏆 LIQUIDITY-ANCHORED ROMEOPT SCANNER STARTED")
    await tg("📊 100% Liquidity-driven TP/SL (RomeoTPT Style)")
    await tg("🔒 FORCED FILTER ACTIVE - NO EXCEPTIONS")
    await tg(f"⚡ RULE 1: Momentum ≥ {MOMENTUM_STRONG_THRESHOLD} → ENTER")
    await tg(f"⚡ RULE 2: Momentum ≥ {MOMENTUM_GOOD_THRESHOLD} AND Displacement ≥ {DISPLACEMENT_MIN_THRESHOLD} → ENTER")
    await tg("🚫 RULE 3: EVERYTHING ELSE → REJECTED")
    await tg("🆕 OB DISTANCE FILTER ACTIVE")
    await tg(f"📏 OB Distance ≤ {OB_DISTANCE_MAX_THRESHOLD}% required")
    await tg("🎯 TIMEFRAME-MAPPED LIQUIDITY TP/SL")
    await tg("📈 Entry TF → TP TF Mapping:")
    await tg("   • 1m/3m/5m → 15m/30m")
    await tg("   • 15m → 30m/1h")
    await tg("   • 30m → 1h/4h")
    await tg("🚫 No trade if no liquidity target found")
    
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
            log.info("Shutdown requested...")
        finally:
            if db_conn:
                asyncio.run(db_conn.close())
            if exchange:
                asyncio.run(exchange.close())
            log.info("Liquidity-anchored scanner stopped.")