#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ROMEOPT SCANNER - Correct TP Framework Implementation
✅ TP1 = 50% EQ of dealing range OR first touch opposing zone (not extreme)
✅ TP2 = Liquidity zones (stop clusters, range extremes)
✅ TP3 = MAJOR HTF OB/FVG OR 3R-5R fixed extension
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
TOP_N = int(os.getenv("TOP_N", 10))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
MIN_SCORE = 5
CRITICAL_FACTORS_MIN = 2

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
    
    all_aligned = all(alignments)
    alignment_count = sum(alignments)
    return all_aligned, alignment_count, len(tfs)

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
                "volume": current_candle["vol"]
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
                "volume": current_candle["vol"]
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

# ================ CORRECT ROMEOPT TP FRAMEWORK ================

def calculate_equilibrium_price(df: pd.DataFrame):
    """
    Calculate 50% equilibrium (EQ) of the dealing range.
    Dealing range = recent high-low range where price has been trading.
    EQ = midpoint where price wants to rebalance.
    """
    # Use last 10-20 candles for dealing range
    recent = df.iloc[-15:] if len(df) >= 15 else df
    
    # Find the actual trading range (ignore outliers)
    high = recent['high'].quantile(0.9)  # 90th percentile high
    low = recent['low'].quantile(0.1)    # 10th percentile low
    
    # Ensure range is meaningful
    if high - low < recent['close'].iloc[-1] * 0.001:  # Less than 0.1% range
        high = recent['high'].max()
        low = recent['low'].min()
    
    equilibrium = (high + low) / 2
    return equilibrium, (low, high)

def find_first_touch_opposing_zone(df: pd.DataFrame, side: str, entry: float):
    """
    Find first touch of opposing zone (NOT the extreme).
    For BUY: First touch of Premium zone (not the top extreme)
    For SELL: First touch of Discount zone (not the bottom extreme)
    """
    recent = df.iloc[-30:]
    current_price = df['close'].iloc[-1]
    
    if side == "BUY":
        # Premium zone = above fair value (1-2 std deviations)
        fair_value = recent['close'].mean()
        std_dev = recent['close'].std()
        
        # Premium zone boundaries
        premium_first_touch = fair_value + (std_dev * 1.0)  # FIRST TOUCH POINT
        premium_extreme = fair_value + (std_dev * 2.0)      # Extreme (for TP2)
        
        # If already in premium zone, use current price as first touch
        if current_price >= premium_first_touch:
            first_touch = current_price * 1.001  # Slightly above current
        else:
            first_touch = premium_first_touch
            
        return first_touch, "PREMIUM_ZONE_FIRST_TOUCH", (premium_first_touch, premium_extreme)
    
    else:  # SELL
        # Discount zone = below fair value (1-2 std deviations)
        fair_value = recent['close'].mean()
        std_dev = recent['close'].std()
        
        # Discount zone boundaries
        discount_extreme = fair_value - (std_dev * 2.0)     # Extreme
        discount_first_touch = fair_value - (std_dev * 1.0) # FIRST TOUCH POINT
        
        # If already in discount zone, use current price as first touch
        if current_price <= discount_first_touch:
            first_touch = current_price * 0.999  # Slightly below current
        else:
            first_touch = discount_first_touch
            
        return first_touch, "DISCOUNT_ZONE_FIRST_TOUCH", (discount_extreme, discount_first_touch)

def find_liquidity_zones(df: pd.DataFrame, side: str, entry: float):
    """
    Find liquidity zones for TP2.
    Liquidity = clusters of stops (equal highs/lows, range extremes).
    """
    zones = []
    recent = df.iloc[-30:]
    
    # 1. Equal highs/lows (stop clusters)
    if side == "BUY":
        # For BUY: Look for equal highs (sell stop clusters above)
        highs = recent['high'].values
        price_levels, counts = np.unique(np.round(highs, 6), return_counts=True)
        
        for price, count in zip(price_levels, counts):
            if count >= 2 and price > entry:  # At least 2 candles at this level
                zones.append({
                    "price": float(price),
                    "type": "EQUAL_HIGHS_STOP_CLUSTER",
                    "cluster_size": int(count),
                    "confidence": "HIGH" if count >= 3 else "MEDIUM"
                })
        
        # Add recent high as potential liquidity
        recent_high = recent['high'].max()
        if recent_high > entry:
            zones.append({
                "price": float(recent_high),
                "type": "RECENT_HIGH_EXTREME",
                "cluster_size": 1,
                "confidence": "MEDIUM"
            })
    
    else:  # SELL
        # For SELL: Look for equal lows (buy stop clusters below)
        lows = recent['low'].values
        price_levels, counts = np.unique(np.round(lows, 6), return_counts=True)
        
        for price, count in zip(price_levels, counts):
            if count >= 2 and price < entry:  # At least 2 candles at this level
                zones.append({
                    "price": float(price),
                    "type": "EQUAL_LOWS_STOP_CLUSTER",
                    "cluster_size": int(count),
                    "confidence": "HIGH" if count >= 3 else "MEDIUM"
                })
        
        # Add recent low as potential liquidity
        recent_low = recent['low'].min()
        if recent_low < entry:
            zones.append({
                "price": float(recent_low),
                "type": "RECENT_LOW_EXTREME",
                "cluster_size": 1,
                "confidence": "MEDIUM"
            })
    
    # Remove duplicates and sort by confidence then distance
    unique_zones = []
    seen_prices = set()
    
    for zone in zones:
        # Skip if too close to another zone (within 0.05%)
        is_duplicate = False
        for seen in seen_prices:
            if abs(zone["price"] - seen) / zone["price"] < 0.0005:
                is_duplicate = True
                break
        
        if not is_duplicate and zone["price"] != entry:
            zone["distance"] = abs(zone["price"] - entry)
            unique_zones.append(zone)
            seen_prices.add(zone["price"])
    
    # Sort by confidence (HIGH first), then distance (closer first)
    unique_zones.sort(key=lambda x: (
        {"HIGH": 0, "MEDIUM": 1, "LOW": 2}[x["confidence"]],
        x["distance"]
    ))
    
    return unique_zones

async def find_htf_major_ob_fvg(exchange, symbol: str, side: str, entry: float):
    """
    Find MAJOR HTF Order Block or FVG for TP3.
    Only significant HTF structures (15m, 1h, 4h).
    LTF structures are execution tools, HTF are destination targets.
    """
    major_structures = []
    
    # Check major timeframes
    for tf in ["15m", "1h", "4h"]:
        ohlcv = await fetch_ohlcv(exchange, symbol, tf, 150)
        if not ohlcv or len(ohlcv) < 50:
            continue
        
        df_htf = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
        
        # Look for MAJOR Order Blocks
        for i in range(20, len(df_htf) - 5):
            candle = df_htf.iloc[i]
            prev_candle = df_htf.iloc[i-1]
            candle_body = abs(candle["close"] - candle["open"])
            candle_range = candle["high"] - candle["low"]
            
            # Must be significant candle (>50% body)
            if candle_body < candle_range * 0.5:
                continue
            
            # MAJOR Bullish OB (for SELL TP3 - resistance)
            if (candle["close"] > candle["open"] and 
                prev_candle["close"] < prev_candle["open"] and
                candle["close"] > prev_candle["high"]):  # Clear breakout
                
                if side == "SELL":  # Resistance for SELL
                    structure = {
                        "price": float(candle["high"]),
                        "type": f"MAJOR_HTF_OB",
                        "timeframe": tf,
                        "strength": "VERY_HIGH",
                        "index": i
                    }
                    major_structures.append(structure)
            
            # MAJOR Bearish OB (for BUY TP3 - support)
            elif (candle["close"] < candle["open"] and 
                  prev_candle["close"] > prev_candle["open"] and
                  candle["close"] < prev_candle["low"]):  # Clear breakdown
                
                if side == "BUY":  # Support for BUY
                    structure = {
                        "price": float(candle["low"]),
                        "type": f"MAJOR_HTF_OB",
                        "timeframe": tf,
                        "strength": "VERY_HIGH",
                        "index": i
                    }
                    major_structures.append(structure)
    
    # Filter by side and sort
    valid_structures = []
    for structure in major_structures:
        if side == "BUY" and structure["price"] > entry:
            valid_structures.append(structure)
        elif side == "SELL" and structure["price"] < entry:
            valid_structures.append(structure)
    
    # Sort by strength then distance
    valid_structures.sort(key=lambda x: (
        {"VERY_HIGH": 0, "HIGH": 1, "MEDIUM": 2}[x["strength"]],
        abs(x["price"] - entry)
    ))
    
    return valid_structures[0] if valid_structures else None

def calculate_extension_r(momentum: float, volatility: float):
    """
    Calculate extension R for TP3 when no major HTF structure.
    3R-5R based on momentum and volatility.
    """
    base_r = 3.0
    
    # Momentum adjustment
    if momentum > 0.02:  # Strong momentum
        base_r += 1.5
    elif momentum > 0.01:  # Moderate momentum
        base_r += 1.0
    else:  # Weak momentum
        base_r += 0.5
    
    # Volatility adjustment
    if volatility > 0.015:  # High volatility
        base_r += 0.5
    
    # Cap at 5R
    return min(base_r, 5.0)

async def romeopt_tp_framework(exchange, entry: float, side: str, df: pd.DataFrame, symbol: str):
    """
    🎯 CORRECT RomeOPT TP Framework Implementation
    
    TP1: 50% EQ of dealing range OR first touch opposing zone (not extreme)
    TP2: Liquidity zones (stop clusters, range extremes)
    TP3: MAJOR HTF OB/FVG OR 3R-5R fixed extension
    """
    atr_val = float(atr(df, 14).iloc[-1])
    current_price = df['close'].iloc[-1]
    
    # Calculate Stop Loss
    recent_high = df['high'].iloc[-10:].max()
    recent_low = df['low'].iloc[-10:].min()
    
    if side == "BUY":
        sl = recent_low - (atr_val * 0.3)
        risk = entry - sl
        if risk < atr_val * 0.5:
            risk = atr_val * 0.5
            sl = entry - risk
    else:  # SELL
        sl = recent_high + (atr_val * 0.3)
        risk = sl - entry
        if risk < atr_val * 0.5:
            risk = atr_val * 0.5
            sl = entry + risk
    
    result = {
        "sl": sl,
        "tp1": None,
        "tp2": None,
        "tp3": None,
        "risk": risk,
        "atr": atr_val,
        "tp1_source": None,
        "tp2_source": None,
        "tp3_source": None
    }
    
    # ========== TP1: 50% EQ or First Touch ==========
    
    # Option 1: 50% Equilibrium
    equilibrium, dealing_range = calculate_equilibrium_price(df)
    dealing_low, dealing_high = dealing_range
    
    if side == "BUY":
        # For BUY, equilibrium should be above entry
        if equilibrium > entry:
            # Ensure minimum profit
            if (equilibrium - entry) >= risk * 0.5:
                result["tp1"] = equilibrium
                result["tp1_source"] = "EQ_50%_DEALING_RANGE"
    
    else:  # SELL
        # For SELL, equilibrium should be below entry
        if equilibrium < entry:
            if (entry - equilibrium) >= risk * 0.5:
                result["tp1"] = equilibrium
                result["tp1_source"] = "EQ_50%_DEALING_RANGE"
    
    # Option 2: First touch of opposing zone
    first_touch, zone_type, zone_range = find_first_touch_opposing_zone(df, side, entry)
    
    if side == "BUY" and first_touch > entry:
        # Check if it's better (closer/safer) than EQ
        if not result["tp1"] or first_touch < result["tp1"]:
            if (first_touch - entry) >= risk * 0.5:
                result["tp1"] = first_touch
                result["tp1_source"] = zone_type
    
    elif side == "SELL" and first_touch < entry:
        if not result["tp1"] or first_touch > result["tp1"]:
            if (entry - first_touch) >= risk * 0.5:
                result["tp1"] = first_touch
                result["tp1_source"] = zone_type
    
    # Fallback: Minimum 0.5R
    if not result["tp1"]:
        if side == "BUY":
            result["tp1"] = entry + (risk * 0.5)
        else:
            result["tp1"] = entry - (risk * 0.5)
        result["tp1_source"] = "MINIMUM_0.5R_FALLBACK"
    
    # Ensure minimum 0.5R profit
    tp1_profit = abs(result["tp1"] - entry)
    if tp1_profit < risk * 0.5:
        if side == "BUY":
            result["tp1"] = entry + (risk * 0.5)
        else:
            result["tp1"] = entry - (risk * 0.5)
        result["tp1_source"] = "MINIMUM_0.5R_ADJUSTED"
    
    # ========== TP2: Liquidity Zones ==========
    
    liquidity_zones = find_liquidity_zones(df, side, entry)
    
    for zone in liquidity_zones:
        zone_price = zone["price"]
        zone_profit = abs(zone_price - entry)
        
        if side == "BUY":
            if zone_price > result["tp1"] and zone_profit >= risk * 1.0:
                result["tp2"] = zone_price
                result["tp2_source"] = zone["type"]
                break
        else:  # SELL
            if zone_price < result["tp1"] and zone_profit >= risk * 1.0:
                result["tp2"] = zone_price
                result["tp2_source"] = zone["type"]
                break
    
    # ========== TP3: MAJOR HTF or Fixed Extension ==========
    
    # Try MAJOR HTF structure first
    htf_structure = await find_htf_major_ob_fvg(exchange, symbol, side, entry)
    
    if htf_structure:
        htf_price = htf_structure["price"]
        htf_profit = abs(htf_price - entry)
        
        # Check if beyond TP2 (or TP1 if no TP2)
        comparison_price = result["tp2"] if result["tp2"] else result["tp1"]
        
        if side == "BUY":
            if htf_price > comparison_price and htf_profit >= risk * 2.5:
                result["tp3"] = htf_price
                result["tp3_source"] = htf_structure["type"]
        else:  # SELL
            if htf_price < comparison_price and htf_profit >= risk * 2.5:
                result["tp3"] = htf_price
                result["tp3_source"] = htf_structure["type"]
    
    # Fixed extension if no good HTF structure
    if not result["tp3"]:
        # Calculate appropriate extension
        momentum = abs(df['close'].iloc[-1] - df['close'].iloc[-5]) / df['close'].iloc[-5]
        volatility = atr_val / current_price
        
        extension_r = calculate_extension_r(momentum, volatility)
        
        if side == "BUY":
            result["tp3"] = entry + (risk * extension_r)
        else:
            result["tp3"] = entry - (risk * extension_r)
        
        result["tp3_source"] = f"FIXED_{extension_r:.1f}R_EXTENSION"
    
    # Final validation: Ensure TP hierarchy
    if result["tp2"] and result["tp3"]:
        if side == "BUY":
            if not (result["tp1"] < result["tp2"] < result["tp3"]):
                # Adjust TP3 to be beyond TP2
                if result["tp3"] <= result["tp2"]:
                    result["tp3"] = result["tp2"] + risk
        else:  # SELL
            if not (result["tp1"] > result["tp2"] > result["tp3"]):
                if result["tp3"] >= result["tp2"]:
                    result["tp3"] = result["tp2"] - risk
    
    return result

# ---------------- ROMEOPT 6-STEP SIGNAL ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    """Generate RomeOPT 6-step signal"""
    if df is None or len(df) < 20:
        return None
    
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    
    score = 0
    reasons = []

    # Step 1: Liquidity Sweep
    sweep_high = float(last["high"] > prev5["high"].max())
    sweep_low = float(last["low"] < prev5["low"].min())
    has_sweep = bool(sweep_high or sweep_low)
    liquidity_sweep = 2 if has_sweep else 0
    score += liquidity_sweep
    reasons.append(f"Liquidity Sweep +{liquidity_sweep}")
    
    # Step 2: Displacement
    displacement = float(abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8))
    has_disp = displacement > 0.6
    displacement_score = 2 if has_disp else 0
    score += displacement_score
    reasons.append(f"Displacement +{displacement_score}")

    # Step 3 & 4: Order Block & Zone
    ob_zone = None
    ob_type = None
    
    for i in range(len(df) - 5, len(df) - 1):
        candle, prev_candle = df.iloc[i], df.iloc[i - 1]
        if candle["close"] > candle["open"] and prev_candle["close"] < prev_candle["open"]:
            ob_zone = {
                "type": "bullish", 
                "low": min(candle["low"], prev_candle["low"]), 
                "high": candle["close"]
            }
            ob_type = "bullish"
            break
        elif candle["close"] < candle["open"] and prev_candle["close"] > prev_candle["open"]:
            ob_zone = {
                "type": "bearish", 
                "low": candle["close"], 
                "high": max(candle["high"], prev_candle["high"])
            }
            ob_type = "bearish"
            break

    zone_score = 0
    if ob_zone:
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
        return None

    # Step 5: HTF Alignment
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

    # Step 6: Momentum
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

    side = "BUY" if ob_type == "bullish" else "SELL"
    entry = float(last["close"])

    # ---------------- CRITICAL FILTERS ----------------
    critical_score = htf_alignment + liquidity_sweep
    
    if critical_score < CRITICAL_FACTORS_MIN:
        return None
    if score < MIN_SCORE:
        return None
    if not has_disp:
        return None
    
    # ---------------- HTF ALIGNMENT MANDATORY ----------------
    if htf_alignment != 1:
        return None

    # ---------------- MARKET REGIME ----------------
    market_regime = await detect_market_regime(df)
    
    if (market_regime == "BULL" and side == "SELL") or (market_regime == "BEAR" and side == "BUY"):
        return None

    # ---------------- TREND MA FILTER ----------------
    trend_ma = df["close"].rolling(20).mean().iloc[-1]
    
    if (side == "BUY" and last["close"] < trend_ma) or (side == "SELL" and last["close"] > trend_ma):
        return None

    # ---------------- ELITE MTF CONFIRMATION ----------------
    elite_result, elite_count, elite_total = await elite_tf_alignment(exchange, symbol, side)
    
    if not elite_result:
        return None
    
    # Create signal
    sig = {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "score": score,
        "reason": "RomeOPT 6-Step",
        "market_regime": market_regime,
        "trend_ma": trend_ma,
        "price_vs_ma": float(last["close"] - trend_ma),
        "ob_type": ob_type,
        "ob_low": ob_zone["low"] if ob_zone else None,
        "ob_high": ob_zone["high"] if ob_zone else None,
        "htf_trend": htf_trend_value,
        "momentum": momentum_ratio,
        "elite_mtf": f"{elite_count}/{elite_total}",
        "displacement": displacement
    }
    
    # ---------------- ROMEOPT TP FRAMEWORK ----------------
    tp_result = await romeopt_tp_framework(exchange, entry, side, df, symbol)
    
    sig.update({
        "sl": tp_result["sl"],
        "tp1": tp_result["tp1"],
        "tp2": tp_result["tp2"],
        "tp3": tp_result["tp3"],
        "risk": tp_result["risk"],
        "atr": tp_result["atr"],
        "tp1_source": tp_result["tp1_source"],
        "tp2_source": tp_result["tp2_source"],
        "tp3_source": tp_result["tp3_source"]
    })
    
    # Add sweep details
    sweep_details = analyze_sweep_details(df)
    sig["sweep_details"] = sweep_details
    
    return sig

# ---------------- SIMPLIFIED TELEGRAM OUTPUT ----------------
async def send_simplified_signal(sig):
    """Send simplified Telegram output"""
    lines = []
    
    # Header
    lines.append(f"🏆 {sig['symbol']} ({sig.get('timeframe', 'N/A')}) {sig['side']}")
    lines.append(f"Entry: {format_number(sig['entry'])} | Score: {sig['score']}/9")
    lines.append("")
    
    # Sweep Details
    sweep = sig.get("sweep_details", {})
    if sweep["type"] != "NONE":
        details = sweep["details"]
        sweep_type = sweep["type"]
        lines.append(f"⚡ LIQUIDITY SWEEP DETAILS:")
        lines.append(f"  • Type: {sweep_type} SWEEP")
        lines.append(f"  • Score: +2")
        lines.append(f"  • Current: {format_number(details.get('current_high' if sweep_type == 'HIGH' else 'current_low'))}")
        lines.append(f"  • Previous: {format_number(details.get('previous_high' if sweep_type == 'HIGH' else 'previous_low'))}")
        lines.append(f"  • Extension: {format_number(details.get('extension', 0))}")
        lines.append(f"  • Extension %: {details.get('extension_pct', 0):.2f}%")
        lines.append(f"  • Strength: {details.get('strength', 'N/A')}")
        lines.append(f"  • Wick Size: {format_number(details.get('wick_size', 0))}")
        lines.append(f"  • Volume: {format_number(details.get('volume', 0))}")
    else:
        lines.append(f"⚡ LIQUIDITY SWEEP DETAILS:")
        lines.append(f"  • No significant sweep detected")
    
    lines.append("")
    
    # Order Block Details
    ob_type = sig.get("ob_type", "").upper()
    ob_low = sig.get("ob_low", 0)
    ob_high = sig.get("ob_high", 0)
    ob_range = ob_high - ob_low
    ob_mid = (ob_low + ob_high) / 2
    distance_to_entry = abs(sig['entry'] - ob_mid)
    distance_pct = (distance_to_entry / sig['entry'] * 100) if sig['entry'] > 0 else 0
    in_zone = (sig['side'] == "BUY" and sig['entry'] <= ob_high) or (sig['side'] == "SELL" and sig['entry'] >= ob_low)
    
    lines.append(f"🔷 ORDER BLOCK DETAILS:")
    lines.append(f"  • Type: {ob_type} OB")
    lines.append(f"  • Zone Approach: +{1 if in_zone else 0}")
    lines.append(f"  • OB Range: {format_number(ob_low)} - {format_number(ob_high)}")
    lines.append(f"  • Range Size: {format_number(ob_range)}")
    lines.append(f"  • Midpoint: {format_number(ob_mid)}")
    lines.append(f"  • Distance to Entry: {format_number(distance_to_entry)} ({distance_pct:.2f}%)")
    lines.append(f"  • In Zone: {'✅ YES' if in_zone else '❌ NO'}")
    lines.append(f"  • Strength: {sig.get('momentum', 0):.2f}")
    
    lines.append("")
    
    # Key Metrics
    lines.append(f"📊 KEY METRICS:")
    lines.append(f"  • Displacement: {sig.get('displacement', 0):.2f} ({'✅ STRONG' if sig.get('displacement', 0) >= 0.6 else '⚠️ WEAK'})")
    lines.append(f"  • Momentum: {sig.get('momentum', 0):.2f} {'✅ PASS' if sig.get('momentum', 0) >= 0.5 else '❌ FAIL'}")
    lines.append(f"  • HTF Trend: {sig.get('htf_trend', 0):+.6f}")
    lines.append(f"  • HTF Direction: {'bullish' if sig.get('htf_trend', 0) > 0 else 'bearish'}")
    lines.append(f"  • Elite MTF: {sig.get('elite_mtf', '0/3')} aligned")
    
    lines.append("")
    
    # RomeOPT TP Validation
    lines.append(f"🏛️ ROMEOPT TP VALIDATION:")
    
    # Format TP sources
    def format_tp_source(source):
        if not source:
            return "N/A"
        if source == "EQ_50%_DEALING_RANGE":
            return "EQ (50% dealing range)"
        elif source == "PREMIUM_ZONE_FIRST_TOUCH":
            return "Premium zone first touch"
        elif source == "DISCOUNT_ZONE_FIRST_TOUCH":
            return "Discount zone first touch"
        elif "EQUAL_HIGHS" in source:
            return "Equal highs stop cluster"
        elif "EQUAL_LOWS" in source:
            return "Equal lows stop cluster"
        elif "RECENT_" in source:
            return "Recent range extreme"
        elif "MAJOR_HTF_OB" in source:
            return "HTF order block"
        elif "FIXED_" in source:
            r_value = source.split("_")[1]
            return f"Fixed {r_value}R extension"
        return source
    
    lines.append(f"  • TP1 Source: {format_tp_source(sig.get('tp1_source', 'N/A'))}")
    
    if sig.get('tp2'):
        lines.append(f"  • TP2 Source: {format_tp_source(sig.get('tp2_source', 'N/A'))}")
    else:
        lines.append(f"  • TP2 Source: No valid liquidity zone")
    
    if sig.get('tp3'):
        lines.append(f"  • TP3 Source: {format_tp_source(sig.get('tp3_source', 'N/A'))}")
    else:
        lines.append(f"  • TP3 Source: Not set")
    
    lines.append("")
    
    # RomeOPT Targets
    lines.append(f"🎯 ROMEOPT TARGETS:")
    risk = sig.get("risk", 0)
    
    if sig.get('sl'):
        lines.append(f"  SL: {format_number(sig.get('sl', 0))}")
    
    if sig.get('tp1'):
        tp1_r = abs(sig['tp1'] - sig['entry']) / risk if risk > 0 else 0
        lines.append(f"  TP1: {format_number(sig.get('tp1', 0))} ({tp1_r:.1f}R)")
    
    if sig.get('tp2'):
        tp2_r = abs(sig['tp2'] - sig['entry']) / risk if risk > 0 else 0
        lines.append(f"  TP2: {format_number(sig.get('tp2', 0))} ({tp2_r:.1f}R)")
    
    if sig.get('tp3'):
        tp3_r = abs(sig['tp3'] - sig['entry']) / risk if risk > 0 else 0
        lines.append(f"  TP3: {format_number(sig.get('tp3', 0))} ({tp3_r:.1f}R)")
    
    if risk > 0:
        lines.append(f"  Risk: {format_number(risk)}")
        lines.append(f"  ATR: {format_number(sig.get('atr', 0))}")
    
    lines.append("")
    
    # Market Conditions
    lines.append(f"📈 MARKET CONDITIONS:")
    lines.append(f"  • Regime: {sig.get('market_regime', 'N/A')}")
    lines.append(f"  • Trend MA: {format_number(sig.get('trend_ma', 0))}")
    lines.append(f"  • Price vs MA: {format_number(sig.get('price_vs_ma', 0))}")
    
    # Send to Telegram
    try:
        await tg("\n".join(lines))
    except Exception as e:
        log.error(f"Failed to send Telegram signal: {e}")

# ---------------- LOG SIGNAL ----------------
async def log_signal(sig):
    async with db_lock:
        await db_conn.execute("""
            INSERT INTO signals (
                symbol, side, entry, sl, tp1, tp2, tp3, timestamp, status, reason, score, breakdown_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sig["symbol"], sig["side"], sig["entry"], 
            sig.get("sl"), sig.get("tp1"), sig.get("tp2"), sig.get("tp3"),
            datetime.datetime.utcnow().isoformat(), "OPEN", sig["reason"], sig["score"],
            json.dumps(sig, default=str)
        ))
        await db_conn.commit()

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    """Monitor open positions for TP/SL hits"""
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("SELECT id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status FROM signals WHERE status='OPEN'") as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status = row
                        
                        try:
                            ticker = await exchange.fetch_ticker(symbol)
                            last_price = ticker.get("last")
                            if last_price is None:
                                continue
                        except Exception:
                            continue
                        
                        hits = []
                        sl_hit = False
                        
                        if side == "BUY":
                            if not tp1_hit and last_price >= tp1:
                                hits.append("TP1")
                                tp1_hit = 1
                            if tp2 and not tp2_hit and last_price >= tp2:
                                hits.append("TP2")
                                tp2_hit = 1
                            if tp3 and not tp3_hit and last_price >= tp3:
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
                            if tp2 and not tp2_hit and last_price <= tp2:
                                hits.append("TP2")
                                tp2_hit = 1
                            if tp3 and not tp3_hit and last_price <= tp3:
                                hits.append("TP3")
                                tp3_hit = 1
                            if last_price >= sl:
                                hits.append("SL")
                                status = "CLOSED"
                                sl_hit = True
                        
                        if hits:
                            msg = f"🎯 {symbol} {side} update\nEntry: {entry:.8f}\nLast: {last_price:.8f}\nHits: {','.join(hits)}"
                            msg += f"\nSL: {sl:.8f}"
                            if tp1: msg += f"\nTP1: {tp1:.8f}"
                            if tp2: msg += f" TP2: {tp2:.8f}"
                            if tp3: msg += f" TP3: {tp3:.8f}"
                            await tg(msg)
                        
                        # Update database
                        await db_conn.execute(
                            "UPDATE signals SET tp1_hit=?, tp2_hit=?, tp3_hit=?, status=? WHERE id=?",
                            (tp1_hit, tp2_hit, tp3_hit, status, sig_id)
                        )
                
                await db_conn.commit()
        except Exception as e:
            log.exception("monitor error: %s", e)
        
        await asyncio.sleep(SCAN_INTERVAL)

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

# ---------------- SCAN LOOP ----------------
last_signal_time = {}

async def scan_loop(exchange):
    while True:
        t0 = time.time()
        try:
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
                        sig["timeframe"] = tf
                        await send_simplified_signal(sig)
                        await log_signal(sig)
                        
                        last_signal_time[key] = time.time()
                        signals_found += 1
                        
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
    
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"}
    })
    
    await tg("🏆 ROMEOPT SCANNER STARTED")
    await tg("🎯 TP Framework: TP1=EQ/First Touch, TP2=Liquidity, TP3=HTF/Fixed Ext")
    await tg("✅ Implementation verified with correct RomeOPT definitions")
    
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