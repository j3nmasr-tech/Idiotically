#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT SCANNER v2 - Exact Step-by-Step Implementation
Matches the 8-step process exactly

Step 1: HTF Bias (4H/1H) → Range/Trend + Liquidity zones → Skip mid-range
Step 2: Liquidity Map → FROM liquidity TO liquidity targets
Step 3: Liquidity Sweep → Impulsive stop hunts only
Step 4: Structure Check → CHoCH (reversal) or BOS (continuation)
Step 5: Entry Zone → OB/FVG in premium/discount + HTF aligned
Step 6: Risk/SL → Beyond invalidation (sweep, OB, structure)
Step 7: Take Profit → TP1=internal, TP2=range, TP3=HTF liquidity
Step 8: Probability Check → Combined score filters
"""

import os
import time
import asyncio
import logging
import datetime
import json
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI
import uvicorn
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from collections import defaultdict

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/romeopt_v2.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 15))  # Longer interval for HTF focus
TOP_N = int(os.getenv("TOP_N", 40))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 5))

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_v2")
db_lock = asyncio.Lock()
db_conn = None

# ---------------- DATA STRUCTURES ----------------
@dataclass
class HTFContext:
    """Step 1: HTF Bias output"""
    bias: str  # "BULLISH", "BEARISH", "RANGING"
    range_high: float
    range_low: float
    range_mid: float
    premium_discount: str  # "PREMIUM", "DISCOUNT", "MIDDLE"
    liquidity_zones: List[Dict]  # HTF liquidity levels
    structure: List[Dict]  # Swing highs/lows
    skip_reason: Optional[str] = None
    valid: bool = False

@dataclass
class LiquidityMap:
    """Step 2: Liquidity Map output"""
    from_liquidity: List[Dict]  # Liquidity being moved FROM
    to_liquidity: List[Dict]    # Liquidity targets TO move to
    has_clear_target: bool = False

@dataclass
class SweepAnalysis:
    """Step 3: Liquidity Sweep output"""
    type: str  # "HIGH_SWEEP", "LOW_SWEEP", "NONE"
    candle_index: int
    swept_price: float
    previous_extreme: float
    impulsive: bool  # Body > wicks
    fake_sweep: bool = False
    strength: float = 0.0  # 0-1 score

@dataclass
class StructureShift:
    """Step 4: Structure Check output"""
    type: str  # "CHoCH" (reversal), "BOS" (continuation), "NONE"
    confirmed: bool
    candle_index: int
    description: str = ""

@dataclass
class EntryZone:
    """Step 5: Entry Zone output"""
    type: str  # "ORDER_BLOCK", "FAIR_VALUE_GAP", "DISCOUNT", "PREMIUM"
    price: float
    low: float
    high: float
    aligns_with_htf: bool
    candle_reaction: bool = False

@dataclass
class RiskManagement:
    """Step 6: Risk/SL output"""
    sl_price: float
    invalidation_type: str  # "SWEEP", "ORDER_BLOCK", "STRUCTURE"
    risk_amount: float
    sl_to_entry_distance: float

@dataclass
class TakeProfitLevels:
    """Step 7: Take Profit output"""
    tp1: float  # Nearest internal liquidity
    tp2: float  # Range boundary
    tp3: float  # HTF liquidity
    tp1_type: str = "INTERNAL_LIQUIDITY"
    tp2_type: str = "RANGE_BOUNDARY"
    tp3_type: str = "HTF_LIQUIDITY"

@dataclass
class ProbabilityScore:
    """Step 8: Probability Check output"""
    htf_alignment: float  # 0-1
    liquidity_quality: float  # 0-1
    sweep_strength: float  # 0-1
    structure_clarity: float  # 0-1
    entry_precision: float  # 0-1
    total_score: float  # 0-5
    
    @property
    def acceptable(self) -> bool:
        """Accept if total >= 3.5 and all components > 0.5"""
        return (self.total_score >= 3.5 and 
                all([self.htf_alignment >= 0.5,
                     self.liquidity_quality >= 0.5,
                     self.sweep_strength >= 0.5,
                     self.structure_clarity >= 0.5,
                     self.entry_precision >= 0.5]))

# ---------------- TELEGRAM ----------------
async def send_telegram(msg: str, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": parse_mode
            })
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

# ---------------- DATABASE ----------------
async def init_db():
    global db_conn
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute("PRAGMA journal_mode=WAL;")
    
    # Create signals table with step-by-step tracking
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            timestamp TEXT,
            side TEXT,
            
            -- Step 1: HTF Bias
            htf_bias TEXT,
            htf_range_high REAL,
            htf_range_low REAL,
            htf_premium_discount TEXT,
            htf_liquidity_zones_json TEXT,
            htf_structure_json TEXT,
            
            -- Step 2: Liquidity Map
            liquidity_from_json TEXT,
            liquidity_to_json TEXT,
            has_clear_target BOOLEAN,
            
            -- Step 3: Liquidity Sweep
            sweep_type TEXT,
            swept_price REAL,
            sweep_impulsive BOOLEAN,
            sweep_strength REAL,
            
            -- Step 4: Structure Check
            structure_shift_type TEXT,
            structure_shift_confirmed BOOLEAN,
            structure_description TEXT,
            
            -- Step 5: Entry Zone
            entry_type TEXT,
            entry_price REAL,
            entry_low REAL,
            entry_high REAL,
            entry_aligns_htf BOOLEAN,
            entry_reaction_confirmed BOOLEAN,
            
            -- Step 6: Risk/SL
            sl_price REAL,
            sl_invalidation_type TEXT,
            risk_amount REAL,
            sl_distance_pct REAL,
            
            -- Step 7: Take Profit
            tp1_price REAL,
            tp1_type TEXT,
            tp2_price REAL,
            tp2_type TEXT,
            tp3_price REAL,
            tp3_type TEXT,
            
            -- Step 8: Probability
            prob_htf_alignment REAL,
            prob_liquidity_quality REAL,
            prob_sweep_strength REAL,
            prob_structure_clarity REAL,
            prob_entry_precision REAL,
            prob_total_score REAL,
            prob_acceptable BOOLEAN,
            
            -- Entry Details
            current_price REAL,
            status TEXT DEFAULT 'DETECTED',
            notes TEXT
        )
    """)
    
    await db_conn.commit()

# ---------------- OHLCV UTILS ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 200):
    """Fetch OHLCV with retry"""
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.debug(f"Failed to fetch {symbol} {timeframe}: {e}")
        return None

def create_dataframe(ohlcv):
    """Create DataFrame from OHLCV"""
    if not ohlcv:
        return None
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

# ---------------- STEP 1: HTF BIAS ----------------
async def analyze_htf_bias(exchange, symbol: str) -> HTFContext:
    """
    Step 1: HTF Bias (4H/1H)
    - Identify range or trend
    - Mark HTF liquidity zones
    - Skip if "mid-range" with no alignment
    """
    
    # Fetch 4H data (primary HTF)
    ohlcv_4h = await fetch_ohlcv(exchange, symbol, "4h", 100)
    if not ohlcv_4h or len(ohlcv_4h) < 30:
        return HTFContext(bias="UNKNOWN", range_high=0, range_low=0, range_mid=0,
                         premium_discount="UNKNOWN", liquidity_zones=[], structure=[])
    
    df_4h = create_dataframe(ohlcv_4h)
    current_price = float(df_4h["close"].iloc[-1])
    
    # Identify swing highs/lows (structure)
    swing_highs = []
    swing_lows = []
    
    for i in range(3, len(df_4h) - 3):
        high_i = df_4h["high"].iloc[i]
        low_i = df_4h["low"].iloc[i]
        
        # Check for swing high
        if (high_i > df_4h["high"].iloc[i-1] and 
            high_i > df_4h["high"].iloc[i-2] and
            high_i > df_4h["high"].iloc[i+1] and
            high_i > df_4h["high"].iloc[i+2]):
            swing_highs.append({
                "price": float(high_i),
                "index": i,
                "timestamp": df_4h["timestamp"].iloc[i]
            })
        
        # Check for swing low
        if (low_i < df_4h["low"].iloc[i-1] and 
            low_i < df_4h["low"].iloc[i-2] and
            low_i < df_4h["low"].iloc[i+1] and
            low_i < df_4h["low"].iloc[i+2]):
            swing_lows.append({
                "price": float(low_i),
                "index": i,
                "timestamp": df_4h["timestamp"].iloc[i]
            })
    
    # Define current range (last 20 periods)
    if len(df_4h) >= 20:
        recent_high = df_4h["high"].iloc[-20:].max()
        recent_low = df_4h["low"].iloc[-20:].min()
    else:
        recent_high = df_4h["high"].max()
        recent_low = df_4h["low"].min()
    
    range_high = float(recent_high)
    range_low = float(recent_low)
    range_mid = (range_high + range_low) / 2
    
    # Determine bias (simplified)
    if len(swing_highs) >= 2 and len(swing_lows) >= 2:
        # Check for higher highs/lower lows
        last_two_highs = sorted([h["price"] for h in swing_highs[-2:]], reverse=True)
        last_two_lows = sorted([l["price"] for l in swing_lows[-2:]])
        
        if last_two_highs[0] > last_two_highs[1] and last_two_lows[0] < last_two_lows[1]:
            bias = "BULLISH"
        elif last_two_highs[0] < last_two_highs[1] and last_two_lows[0] > last_two_lows[1]:
            bias = "BEARISH"
        else:
            bias = "RANGING"
    else:
        bias = "RANGING"
    
    # Premium/Discount (relative to 50% of range)
    range_height = range_high - range_low
    if range_height > 0:
        position_pct = (current_price - range_low) / range_height * 100
        if position_pct > 60:
            premium_discount = "PREMIUM"
        elif position_pct < 40:
            premium_discount = "DISCOUNT"
        else:
            premium_discount = "MIDDLE"
    else:
        premium_discount = "MIDDLE"
    
    # Mark HTF liquidity zones
    liquidity_zones = []
    
    # Range boundaries
    liquidity_zones.append({
        "price": range_high,
        "type": "RANGE_HIGH",
        "timeframe": "4h",
        "strength": 3
    })
    liquidity_zones.append({
        "price": range_low,
        "type": "RANGE_LOW",
        "timeframe": "4h",
        "strength": 3
    })
    
    # Recent swing points
    for swing in swing_highs[-3:]:
        liquidity_zones.append({
            "price": swing["price"],
            "type": "SWING_HIGH",
            "timeframe": "4h",
            "strength": 2
        })
    
    for swing in swing_lows[-3:]:
        liquidity_zones.append({
            "price": swing["price"],
            "type": "SWING_LOW",
            "timeframe": "4h",
            "strength": 2
        })
    
    # Check if we should skip (mid-range with no alignment)
    skip_reason = None
    valid = True
    
    if premium_discount == "MIDDLE" and bias == "RANGING":
        skip_reason = "Price mid-range with no clear HTF alignment"
        valid = False
    elif range_height / range_low < 0.02:  # Very tight range
        skip_reason = "Range too tight (<2%)"
        valid = False
    
    context = HTFContext(
        bias=bias,
        range_high=range_high,
        range_low=range_low,
        range_mid=range_mid,
        premium_discount=premium_discount,
        liquidity_zones=liquidity_zones,
        structure=swing_highs[-5:] + swing_lows[-5:],  # Last 5 of each
        skip_reason=skip_reason,
        valid=valid
    )
    
    return context

# ---------------- STEP 2: LIQUIDITY MAP ----------------
async def map_liquidity(exchange, symbol: str, htf_context: HTFContext, 
                       current_price: float) -> LiquidityMap:
    """
    Step 2: Liquidity Map
    - Identify where price is moving FROM
    - Identify where price needs to move TO
    - Must have clear targets
    """
    
    # Fetch 1H for more granular liquidity
    ohlcv_1h = await fetch_ohlcv(exchange, symbol, "1h", 100)
    if not ohlcv_1h:
        return LiquidityMap(from_liquidity=[], to_liquidity=[], has_clear_target=False)
    
    df_1h = create_dataframe(ohlcv_1h)
    
    # FROM liquidity: Recent extremes that were likely taken
    from_liquidity = []
    
    # Check last 10 candles for swept levels
    recent_df = df_1h.iloc[-10:] if len(df_1h) >= 10 else df_1h
    
    for i in range(len(recent_df) - 1):
        candle = recent_df.iloc[i]
        next_candle = recent_df.iloc[i + 1] if i + 1 < len(recent_df) else candle
        
        # High sweep detection
        if candle["high"] > recent_df["high"].iloc[max(0, i-5):i].max() and next_candle["close"] < candle["close"]:
            from_liquidity.append({
                "price": float(candle["high"]),
                "type": "SWEPT_HIGH",
                "timeframe": "1h",
                "direction": "FROM"
            })
        
        # Low sweep detection
        if candle["low"] < recent_df["low"].iloc[max(0, i-5):i].min() and next_candle["close"] > candle["close"]:
            from_liquidity.append({
                "price": float(candle["low"]),
                "type": "SWEPT_LOW",
                "timeframe": "1h",
                "direction": "FROM"
            })
    
    # TO liquidity: Targets based on HTF context
    to_liquidity = []
    
    # Sort HTF liquidity zones by distance from current price
    sorted_zones = sorted(htf_context.liquidity_zones, 
                         key=lambda z: abs(z["price"] - current_price))
    
    # Filter for relevant targets based on bias
    if htf_context.bias == "BULLISH":
        # Targets above current price
        targets = [z for z in sorted_zones if z["price"] > current_price]
    elif htf_context.bias == "BEARISH":
        # Targets below current price
        targets = [z for z in sorted_zones if z["price"] < current_price]
    else:
        # Ranging - targets at range boundaries
        targets = [z for z in sorted_zones if z["type"] in ["RANGE_HIGH", "RANGE_LOW"]]
    
    # Take top 3 targets
    for target in targets[:3]:
        to_liquidity.append({
            "price": target["price"],
            "type": target["type"],
            "timeframe": target["timeframe"],
            "strength": target.get("strength", 1),
            "direction": "TO"
        })
    
    # Also add internal liquidity (equal highs/lows on 1H)
    if len(df_1h) >= 24:
        # Find equal highs (clusters)
        high_values = df_1h["high"].iloc[-24:].values
        for val in np.unique(np.round(high_values, 4)):
            count = np.sum(np.round(high_values, 4) == val)
            if count >= 2:  # At least 2 touches
                to_liquidity.append({
                    "price": float(val),
                    "type": "EQUAL_HIGH",
                    "timeframe": "1h",
                    "strength": min(2, count),
                    "direction": "TO"
                })
        
        # Find equal lows
        low_values = df_1h["low"].iloc[-24:].values
        for val in np.unique(np.round(low_values, 4)):
            count = np.sum(np.round(low_values, 4) == val)
            if count >= 2:
                to_liquidity.append({
                    "price": float(val),
                    "type": "EQUAL_LOW",
                    "timeframe": "1h",
                    "strength": min(2, count),
                    "direction": "TO"
                })
    
    has_clear_target = len(to_liquidity) > 0 and len(from_liquidity) > 0
    
    return LiquidityMap(
        from_liquidity=from_liquidity,
        to_liquidity=to_liquidity,
        has_clear_target=has_clear_target
    )

# ---------------- STEP 3: LIQUIDITY SWEEP ----------------
async def analyze_sweep(exchange, symbol: str, htf_context: HTFContext) -> SweepAnalysis:
    """
    Step 3: Liquidity Sweep
    - Has price swept a stop above/below liquidity?
    - Sweep must have impulsive body
    - Reject fake sweeps or wicks
    """
    
    # Use 15m for sweep detection (execution TF)
    ohlcv_15m = await fetch_ohlcv(exchange, symbol, "15m", 50)
    if not ohlcv_15m or len(ohlcv_15m) < 10:
        return SweepAnalysis(type="NONE", candle_index=-1, swept_price=0, 
                           previous_extreme=0, impulsive=False)
    
    df_15m = create_dataframe(ohlcv_15m)
    
    # Look for sweeps in last 5 candles
    lookback = min(5, len(df_15m))
    
    for i in range(-lookback, 0):
        candle_idx = len(df_15m) + i
        candle = df_15m.iloc[candle_idx]
        
        # Get previous candles (5 before this one)
        start_idx = max(0, candle_idx - 5)
        prev_candles = df_15m.iloc[start_idx:candle_idx]
        
        if len(prev_candles) == 0:
            continue
        
        previous_high = prev_candles["high"].max()
        previous_low = prev_candles["low"].min()
        
        # Check for high sweep
        if candle["high"] > previous_high:
            # Check if impulsive (body > wicks)
            body_size = abs(candle["close"] - candle["open"])
            upper_wick = candle["high"] - max(candle["open"], candle["close"])
            lower_wick = min(candle["open"], candle["close"]) - candle["low"]
            total_wick = upper_wick + lower_wick
            
            impulsive = body_size > total_wick
            
            # Check for fake sweep (quick reversal)
            if i < -1:  # Not the most recent candle
                next_candle = df_15m.iloc[candle_idx + 1]
                fake_sweep = (next_candle["close"] < candle["close"] and 
                             next_candle["low"] < candle["low"])
            else:
                fake_sweep = False
            
            strength = 0.0
            if impulsive and not fake_sweep:
                # Strength based on how much it exceeded previous high
                extension = (candle["high"] - previous_high) / previous_high
                strength = min(1.0, extension * 100)  # Normalize
            
            return SweepAnalysis(
                type="HIGH_SWEEP",
                candle_index=candle_idx,
                swept_price=float(candle["high"]),
                previous_extreme=float(previous_high),
                impulsive=impulsive,
                fake_sweep=fake_sweep,
                strength=strength
            )
        
        # Check for low sweep
        elif candle["low"] < previous_low:
            body_size = abs(candle["close"] - candle["open"])
            upper_wick = candle["high"] - max(candle["open"], candle["close"])
            lower_wick = min(candle["open"], candle["close"]) - candle["low"]
            total_wick = upper_wick + lower_wick
            
            impulsive = body_size > total_wick
            
            if i < -1:
                next_candle = df_15m.iloc[candle_idx + 1]
                fake_sweep = (next_candle["close"] > candle["close"] and 
                             next_candle["high"] > candle["high"])
            else:
                fake_sweep = False
            
            strength = 0.0
            if impulsive and not fake_sweep:
                extension = (previous_low - candle["low"]) / previous_low
                strength = min(1.0, extension * 100)
            
            return SweepAnalysis(
                type="LOW_SWEEP",
                candle_index=candle_idx,
                swept_price=float(candle["low"]),
                previous_extreme=float(previous_low),
                impulsive=impulsive,
                fake_sweep=fake_sweep,
                strength=strength
            )
    
    return SweepAnalysis(type="NONE", candle_index=-1, swept_price=0, 
                       previous_extreme=0, impulsive=False)

# ---------------- STEP 4: STRUCTURE CHECK ----------------
async def check_structure_shift(exchange, symbol: str, sweep: SweepAnalysis, 
                               htf_context: HTFContext) -> StructureShift:
    """
    Step 4: Structure Check
    - Post-sweep, is there CHoCH (reversal) or BOS (continuation)?
    - No structure shift → reject
    """
    
    if sweep.type == "NONE":
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    # Fetch 15m data for structure analysis
    ohlcv_15m = await fetch_ohlcv(exchange, symbol, "15m", 50)
    if not ohlcv_15m:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    df_15m = create_dataframe(ohlcv_15m)
    
    # Get candles after the sweep
    sweep_idx = sweep.candle_index
    if sweep_idx < 0 or sweep_idx >= len(df_15m) - 3:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    post_sweep_candles = df_15m.iloc[sweep_idx + 1:]
    if len(post_sweep_candles) < 3:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    # Check for CHoCH (reversal after sweep)
    if sweep.type == "HIGH_SWEEP":
        # For high sweep, CHoCH = price breaks below recent low
        recent_low_before = df_15m["low"].iloc[max(0, sweep_idx-5):sweep_idx].min()
        
        for i in range(len(post_sweep_candles)):
            candle = post_sweep_candles.iloc[i]
            if candle["low"] < recent_low_before:
                return StructureShift(
                    type="CHoCH",
                    confirmed=True,
                    candle_index=sweep_idx + i + 1,
                    description="High sweep followed by break below recent low"
                )
        
        # Check for BOS (continuation - new higher high after pullback)
        if len(post_sweep_candles) >= 5:
            # Look for pullback then new high
            pullback_low = post_sweep_candles["low"].iloc[:3].min()
            subsequent_high = post_sweep_candles["high"].iloc[3:].max()
            
            if subsequent_high > sweep.swept_price:
                return StructureShift(
                    type="BOS",
                    confirmed=True,
                    candle_index=sweep_idx + 3,
                    description="High sweep followed by new higher high"
                )
    
    elif sweep.type == "LOW_SWEEP":
        # For low sweep, CHoCH = price breaks above recent high
        recent_high_before = df_15m["high"].iloc[max(0, sweep_idx-5):sweep_idx].max()
        
        for i in range(len(post_sweep_candles)):
            candle = post_sweep_candles.iloc[i]
            if candle["high"] > recent_high_before:
                return StructureShift(
                    type="CHoCH",
                    confirmed=True,
                    candle_index=sweep_idx + i + 1,
                    description="Low sweep followed by break above recent high"
                )
        
        # Check for BOS (continuation - new lower low after pullback)
        if len(post_sweep_candles) >= 5:
            pullback_high = post_sweep_candles["high"].iloc[:3].max()
            subsequent_low = post_sweep_candles["low"].iloc[3:].min()
            
            if subsequent_low < sweep.swept_price:
                return StructureShift(
                    type="BOS",
                    confirmed=True,
                    candle_index=sweep_idx + 3,
                    description="Low sweep followed by new lower low"
                )
    
    return StructureShift(type="NONE", confirmed=False, candle_index=-1)

# ---------------- STEP 5: ENTRY ZONE ----------------
async def find_entry_zone(exchange, symbol: str, htf_context: HTFContext,
                         sweep: SweepAnalysis, structure_shift: StructureShift,
                         side: str) -> EntryZone:
    """
    Step 5: Entry Zone
    - Return to OB or FVG in premium/discount zone
    - Align with HTF bias
    - Candle reaction must confirm
    """
    
    # Fetch 5m data for precise entry
    ohlcv_5m = await fetch_ohlcv(exchange, symbol, "5m", 100)
    if not ohlcv_5m:
        return EntryZone(type="NONE", price=0, low=0, high=0, aligns_with_htf=False)
    
    df_5m = create_dataframe(ohlcv_5m)
    current_price = float(df_5m["close"].iloc[-1])
    
    # Determine expected entry type based on sweep and structure
    if structure_shift.type == "CHoCH":
        # Reversal setup - look for OB
        entry_type = "ORDER_BLOCK"
    elif structure_shift.type == "BOS":
        # Continuation setup - look for FVG
        entry_type = "FAIR_VALUE_GAP"
    else:
        # Based on premium/discount
        if htf_context.premium_discount == "DISCOUNT":
            entry_type = "DISCOUNT"
        elif htf_context.premium_discount == "PREMIUM":
            entry_type = "PREMIUM"
        else:
            entry_type = "NONE"
    
    # Find Order Blocks (simplified detection)
    if entry_type == "ORDER_BLOCK":
        # Look for last opposing candle before displacement
        for i in range(2, len(df_5m) - 1):
            candle = df_5m.iloc[i]
            next_candle = df_5m.iloc[i + 1]
            
            # Bullish OB (bearish candle before bullish displacement)
            if side == "BUY":
                if (candle["close"] < candle["open"] and 
                    next_candle["close"] > next_candle["open"]):
                    # Check if in discount zone
                    ob_low = min(candle["low"], next_candle["low"])
                    ob_high = next_candle["close"]
                    
                    # Check alignment with HTF
                    aligns = (htf_context.bias == "BULLISH" or 
                             htf_context.premium_discount == "DISCOUNT")
                    
                    # Check if price is currently in or near OB
                    if current_price <= ob_high and current_price >= ob_low * 0.995:
                        # Check candle reaction (current or previous candle)
                        current_candle = df_5m.iloc[-1]
                        prev_candle = df_5m.iloc[-2] if len(df_5m) >= 2 else current_candle
                        
                        # Bullish reaction for buy
                        reaction = (current_candle["close"] > current_candle["open"] or
                                   (prev_candle["close"] > prev_candle["open"] and
                                    current_candle["close"] > prev_candle["close"]))
                        
                        return EntryZone(
                            type="ORDER_BLOCK",
                            price=float((ob_low + ob_high) / 2),
                            low=float(ob_low),
                            high=float(ob_high),
                            aligns_with_htf=aligns,
                            candle_reaction=reaction
                        )
            
            # Bearish OB (bullish candle before bearish displacement)
            elif side == "SELL":
                if (candle["close"] > candle["open"] and 
                    next_candle["close"] < next_candle["open"]):
                    ob_low = next_candle["close"]
                    ob_high = max(candle["high"], next_candle["high"])
                    
                    aligns = (htf_context.bias == "BEARISH" or 
                             htf_context.premium_discount == "PREMIUM")
                    
                    if current_price >= ob_low and current_price <= ob_high * 1.005:
                        current_candle = df_5m.iloc[-1]
                        prev_candle = df_5m.iloc[-2] if len(df_5m) >= 2 else current_candle
                        
                        # Bearish reaction for sell
                        reaction = (current_candle["close"] < current_candle["open"] or
                                   (prev_candle["close"] < prev_candle["open"] and
                                    current_candle["close"] < prev_candle["close"]))
                        
                        return EntryZone(
                            type="ORDER_BLOCK",
                            price=float((ob_low + ob_high) / 2),
                            low=float(ob_low),
                            high=float(ob_high),
                            aligns_with_htf=aligns,
                            candle_reaction=reaction
                        )
    
    # Find Fair Value Gaps
    elif entry_type == "FAIR_VALUE_GAP":
        for i in range(1, len(df_5m) - 2):
            candle1 = df_5m.iloc[i]
            candle2 = df_5m.iloc[i + 1]
            candle3 = df_5m.iloc[i + 2] if i + 2 < len(df_5m) else candle2
            
            # Bullish FVG
            if side == "BUY":
                if candle2["low"] > candle1["high"]:
                    fvg_low = candle1["high"]
                    fvg_high = candle2["low"]
                    
                    # Check if price has returned to FVG
                    if current_price <= fvg_high and current_price >= fvg_low:
                        aligns = (htf_context.bias == "BULLISH")
                        reaction = candle3["close"] > candle3["open"]
                        
                        return EntryZone(
                            type="FAIR_VALUE_GAP",
                            price=float((fvg_low + fvg_high) / 2),
                            low=float(fvg_low),
                            high=float(fvg_high),
                            aligns_with_htf=aligns,
                            candle_reaction=reaction
                        )
            
            # Bearish FVG
            elif side == "SELL":
                if candle2["high"] < candle1["low"]:
                    fvg_low = candle2["high"]
                    fvg_high = candle1["low"]
                    
                    if current_price >= fvg_low and current_price <= fvg_high:
                        aligns = (htf_context.bias == "BEARISH")
                        reaction = candle3["close"] < candle3["open"]
                        
                        return EntryZone(
                            type="FAIR_VALUE_GAP",
                            price=float((fvg_low + fvg_high) / 2),
                            low=float(fvg_low),
                            high=float(fvg_high),
                            aligns_with_htf=aligns,
                            candle_reaction=reaction
                        )
    
    # Premium/Discount zone entry
    elif entry_type in ["PREMIUM", "DISCOUNT"]:
        zone_price = htf_context.range_mid
        zone_width = (htf_context.range_high - htf_context.range_low) * 0.1
        
        aligns = True  # Always aligns if we're in correct zone
        
        # Check if price is in zone
        if (side == "BUY" and entry_type == "DISCOUNT" and
            current_price <= htf_context.range_mid * 1.02):
            reaction = df_5m["close"].iloc[-1] > df_5m["open"].iloc[-1]
            
            return EntryZone(
                type="DISCOUNT",
                price=float(zone_price),
                low=float(zone_price - zone_width),
                high=float(zone_price + zone_width),
                aligns_with_htf=aligns,
                candle_reaction=reaction
            )
        
        elif (side == "SELL" and entry_type == "PREMIUM" and
              current_price >= htf_context.range_mid * 0.98):
            reaction = df_5m["close"].iloc[-1] < df_5m["open"].iloc[-1]
            
            return EntryZone(
                type="PREMIUM",
                price=float(zone_price),
                low=float(zone_price - zone_width),
                high=float(zone_price + zone_width),
                aligns_with_htf=aligns,
                candle_reaction=reaction
            )
    
    return EntryZone(type="NONE", price=0, low=0, high=0, aligns_with_htf=False)

# ---------------- STEP 6: RISK/SL ----------------
def calculate_risk_sl(entry_zone: EntryZone, sweep: SweepAnalysis,
                     htf_context: HTFContext, side: str) -> RiskManagement:
    """
    Step 6: Risk/SL
    - Place SL beyond invalidation zone (sweep, OB, or structure)
    - Never arbitrary or % fixed
    """
    
    entry_price = entry_zone.price
    
    # Determine invalidation type and price
    sl_price = 0.0
    invalidation_type = ""
    
    # Priority 1: Beyond swept level
    if sweep.type != "NONE" and sweep.swept_price > 0:
        if side == "BUY" and sweep.type == "LOW_SWEEP":
            sl_price = sweep.swept_price * 0.995  # Just below sweep
            invalidation_type = "SWEEP"
        elif side == "SELL" and sweep.type == "HIGH_SWEEP":
            sl_price = sweep.swept_price * 1.005  # Just above sweep
            invalidation_type = "SWEEP"
    
    # Priority 2: Beyond order block
    if invalidation_type == "" and entry_zone.type == "ORDER_BLOCK":
        if side == "BUY":
            sl_price = entry_zone.low * 0.995
            invalidation_type = "ORDER_BLOCK"
        elif side == "SELL":
            sl_price = entry_zone.high * 1.005
            invalidation_type = "ORDER_BLOCK"
    
    # Priority 3: Beyond structure
    if invalidation_type == "":
        # Use recent swing as invalidation
        if side == "BUY" and htf_context.structure:
            # Find nearest swing low below entry
            swing_lows = [s for s in htf_context.structure if "low" in str(s.get("type", "")).lower()]
            if swing_lows:
                recent_swing_low = min([s.get("price", entry_price * 0.9) for s in swing_lows])
                sl_price = recent_swing_low * 0.995
                invalidation_type = "STRUCTURE"
        elif side == "SELL" and htf_context.structure:
            swing_highs = [s for s in htf_context.structure if "high" in str(s.get("type", "")).lower()]
            if swing_highs:
                recent_swing_high = max([s.get("price", entry_price * 1.1) for s in swing_highs])
                sl_price = recent_swing_high * 1.005
                invalidation_type = "STRUCTURE"
    
    # Fallback: ATR-based if no other method works
    if invalidation_type == "":
        # Simplified ATR approximation
        atr_approx = entry_price * 0.02  # 2% as rough ATR
        if side == "BUY":
            sl_price = entry_price - (atr_approx * 1.5)
        else:
            sl_price = entry_price + (atr_approx * 1.5)
        invalidation_type = "ATR_FALLBACK"
    
    risk_amount = abs(entry_price - sl_price)
    distance_pct = (risk_amount / entry_price) * 100
    
    return RiskManagement(
        sl_price=float(sl_price),
        invalidation_type=invalidation_type,
        risk_amount=float(risk_amount),
        sl_to_entry_distance=float(distance_pct)
    )

# ---------------- STEP 7: TAKE PROFIT ----------------
def calculate_take_profits(entry_price: float, side: str, 
                          liquidity_map: LiquidityMap,
                          htf_context: HTFContext) -> TakeProfitLevels:
    """
    Step 7: Take Profit
    - TP1 = nearest internal liquidity
    - TP2 = range boundary
    - TP3 = HTF liquidity
    """
    
    # Filter liquidity targets based on side
    if side == "BUY":
        # Targets above entry
        potential_targets = [t for t in liquidity_map.to_liquidity 
                           if t["price"] > entry_price]
        range_boundary = htf_context.range_high
        htf_targets = [z for z in htf_context.liquidity_zones 
                      if z["price"] > entry_price and z["type"] != "RANGE_HIGH"]
    else:  # SELL
        # Targets below entry
        potential_targets = [t for t in liquidity_map.to_liquidity 
                           if t["price"] < entry_price]
        range_boundary = htf_context.range_low
        htf_targets = [z for z in htf_context.liquidity_zones 
                      if z["price"] < entry_price and z["type"] != "RANGE_LOW"]
    
    # TP1: Nearest internal liquidity (1H timeframe)
    tp1_candidates = [t for t in potential_targets if t["timeframe"] == "1h"]
    if tp1_candidates:
        # Sort by distance from entry
        tp1_candidates.sort(key=lambda t: abs(t["price"] - entry_price))
        tp1 = tp1_candidates[0]["price"]
        tp1_type = tp1_candidates[0]["type"]
    else:
        # Fallback: 1:1 risk:reward
        if side == "BUY":
            tp1 = entry_price * 1.02
        else:
            tp1 = entry_price * 0.98
        tp1_type = "RISK_REWARD_1_1"
    
    # TP2: Range boundary
    tp2 = range_boundary
    tp2_type = "RANGE_BOUNDARY"
    
    # TP3: HTF liquidity (beyond range)
    if htf_targets:
        # Sort by strength (higher is better)
        htf_targets.sort(key=lambda z: z.get("strength", 0), reverse=True)
        tp3 = htf_targets[0]["price"]
        tp3_type = htf_targets[0]["type"]
    else:
        # Extended target (2x range distance)
        if side == "BUY":
            range_distance = htf_context.range_high - htf_context.range_low
            tp3 = htf_context.range_high + (range_distance * 0.5)
            tp3_type = "EXTENDED_TARGET"
        else:
            range_distance = htf_context.range_high - htf_context.range_low
            tp3 = htf_context.range_low - (range_distance * 0.5)
            tp3_type = "EXTENDED_TARGET"
    
    return TakeProfitLevels(
        tp1=float(tp1),
        tp1_type=tp1_type,
        tp2=float(tp2),
        tp2_type=tp2_type,
        tp3=float(tp3),
        tp3_type=tp3_type
    )

# ---------------- STEP 8: PROBABILITY CHECK ----------------
def calculate_probability(htf_context: HTFContext, liquidity_map: LiquidityMap,
                         sweep: SweepAnalysis, structure_shift: StructureShift,
                         entry_zone: EntryZone, side: str) -> ProbabilityScore:
    """
    Step 8: Probability Check
    - Combine: HTF alignment + liquidity quality + sweep strength + structure clarity + entry precision
    """
    
    # 1. HTF Alignment (0-1)
    if htf_context.bias == side.upper() or htf_context.bias == "RANGING":
        htf_alignment = 1.0
    elif (htf_context.bias == "BULLISH" and side == "SELL") or \
         (htf_context.bias == "BEARISH" and side == "BUY"):
        htf_alignment = 0.3  # Counter-trend
    else:
        htf_alignment = 0.5
    
    # Premium/discount alignment
    if (side == "BUY" and htf_context.premium_discount == "DISCOUNT") or \
       (side == "SELL" and htf_context.premium_discount == "PREMIUM"):
        htf_alignment = min(1.0, htf_alignment + 0.2)
    
    # 2. Liquidity Quality (0-1)
    if liquidity_map.has_clear_target:
        # Count quality targets
        quality_targets = sum(1 for t in liquidity_map.to_liquidity 
                            if t.get("strength", 0) >= 2)
        liquidity_quality = min(1.0, quality_targets / 3.0)
    else:
        liquidity_quality = 0.2
    
    # 3. Sweep Strength (0-1)
    sweep_strength = sweep.strength
    if sweep.impulsive:
        sweep_strength = min(1.0, sweep_strength + 0.3)
    if sweep.fake_sweep:
        sweep_strength = max(0.0, sweep_strength - 0.5)
    
    # 4. Structure Clarity (0-1)
    if structure_shift.confirmed:
        if structure_shift.type == "CHoCH":
            structure_clarity = 0.9  # Reversals are clear
        elif structure_shift.type == "BOS":
            structure_clarity = 0.8  # Continuations
        else:
            structure_clarity = 0.6
    else:
        structure_clarity = 0.2
    
    # 5. Entry Precision (0-1)
    if entry_zone.type in ["ORDER_BLOCK", "FAIR_VALUE_GAP"]:
        entry_precision = 0.8
        if entry_zone.aligns_with_htf:
            entry_precision = min(1.0, entry_precision + 0.1)
        if entry_zone.candle_reaction:
            entry_precision = min(1.0, entry_precision + 0.1)
    elif entry_zone.type in ["PREMIUM", "DISCOUNT"]:
        entry_precision = 0.6
        if entry_zone.candle_reaction:
            entry_precision = 0.7
    else:
        entry_precision = 0.2
    
    total_score = (htf_alignment + liquidity_quality + sweep_strength + 
                   structure_clarity + entry_precision)
    
    return ProbabilityScore(
        htf_alignment=htf_alignment,
        liquidity_quality=liquidity_quality,
        sweep_strength=sweep_strength,
        structure_clarity=structure_clarity,
        entry_precision=entry_precision,
        total_score=total_score
    )

# ---------------- MAIN SCANNING LOGIC ----------------
async def scan_symbol_full(exchange, symbol: str) -> Optional[Dict]:
    """
    Execute full 8-step scanning process for one symbol
    """
    
    # Get current price
    ticker = await exchange.fetch_ticker(symbol)
    current_price = ticker.get("last", 0)
    if not current_price:
        return None
    
    log.debug(f"🔍 Scanning {symbol} at {current_price}")
    
    # --- STEP 1: HTF BIAS ---
    htf_context = await analyze_htf_bias(exchange, symbol)
    if not htf_context.valid:
        log.debug(f"  {symbol}: Skipped HTF - {htf_context.skip_reason}")
        return None
    
    log.debug(f"  {symbol}: HTF {htf_context.bias} in {htf_context.premium_discount}")
    
    # --- STEP 2: LIQUIDITY MAP ---
    liquidity_map = await map_liquidity(exchange, symbol, htf_context, current_price)
    if not liquidity_map.has_clear_target:
        log.debug(f"  {symbol}: No clear liquidity targets")
        return None
    
    log.debug(f"  {symbol}: {len(liquidity_map.from_liquidity)} FROM, {len(liquidity_map.to_liquidity)} TO targets")
    
    # --- STEP 3: LIQUIDITY SWEEP ---
    sweep = await analyze_sweep(exchange, symbol, htf_context)
    if sweep.type == "NONE" or not sweep.impulsive or sweep.fake_sweep:
        log.debug(f"  {symbol}: No valid sweep (type={sweep.type}, impulsive={sweep.impulsive}, fake={sweep.fake_sweep})")
        return None
    
    log.debug(f"  {symbol}: {sweep.type} detected (strength={sweep.strength:.2f})")
    
    # Determine side based on sweep
    if sweep.type == "HIGH_SWEEP":
        side = "SELL"
    elif sweep.type == "LOW_SWEEP":
        side = "BUY"
    else:
        return None
    
    # --- STEP 4: STRUCTURE CHECK ---
    structure_shift = await check_structure_shift(exchange, symbol, sweep, htf_context)
    if not structure_shift.confirmed:
        log.debug(f"  {symbol}: No structure shift after sweep")
        return None
    
    log.debug(f"  {symbol}: Structure {structure_shift.type} confirmed")
    
    # --- STEP 5: ENTRY ZONE ---
    entry_zone = await find_entry_zone(exchange, symbol, htf_context, sweep, structure_shift, side)
    if entry_zone.type == "NONE" or not entry_zone.candle_reaction:
        log.debug(f"  {symbol}: No valid entry zone (type={entry_zone.type}, reaction={entry_zone.candle_reaction})")
        return None
    
    log.debug(f"  {symbol}: Entry {entry_zone.type} at {entry_zone.price:.8f}")
    
    # --- STEP 6: RISK/SL ---
    risk_sl = calculate_risk_sl(entry_zone, sweep, htf_context, side)
    if risk_sl.sl_price == 0:
        log.debug(f"  {symbol}: Failed to calculate SL")
        return None
    
    log.debug(f"  {symbol}: SL at {risk_sl.sl_price:.8f} ({risk_sl.invalidation_type})")
    
    # --- STEP 7: TAKE PROFIT ---
    tp_levels = calculate_take_profits(entry_zone.price, side, liquidity_map, htf_context)
    
    # --- STEP 8: PROBABILITY CHECK ---
    probability = calculate_probability(
        htf_context, liquidity_map, sweep, structure_shift, entry_zone, side
    )
    
    if not probability.acceptable:
        log.debug(f"  {symbol}: Probability too low ({probability.total_score:.2f}/5)")
        return None
    
    log.info(f"✅ {symbol}: A+ Setup detected! Score: {probability.total_score:.2f}/5")
    
    # --- COMPILE FINAL SETUP ---
    setup = {
        "symbol": symbol,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "side": side,
        "current_price": current_price,
        
        # Step 1
        "htf_bias": htf_context.bias,
        "htf_range_high": htf_context.range_high,
        "htf_range_low": htf_context.range_low,
        "htf_premium_discount": htf_context.premium_discount,
        "htf_liquidity_zones": htf_context.liquidity_zones,
        "htf_structure": htf_context.structure,
        
        # Step 2
        "liquidity_from": liquidity_map.from_liquidity,
        "liquidity_to": liquidity_map.to_liquidity,
        "has_clear_target": liquidity_map.has_clear_target,
        
        # Step 3
        "sweep_type": sweep.type,
        "swept_price": sweep.swept_price,
        "sweep_impulsive": sweep.impulsive,
        "sweep_strength": sweep.strength,
        
        # Step 4
        "structure_shift_type": structure_shift.type,
        "structure_shift_confirmed": structure_shift.confirmed,
        "structure_description": structure_shift.description,
        
        # Step 5
        "entry_type": entry_zone.type,
        "entry_price": entry_zone.price,
        "entry_low": entry_zone.low,
        "entry_high": entry_zone.high,
        "entry_aligns_htf": entry_zone.aligns_with_htf,
        "entry_reaction_confirmed": entry_zone.candle_reaction,
        
        # Step 6
        "sl_price": risk_sl.sl_price,
        "sl_invalidation_type": risk_sl.invalidation_type,
        "risk_amount": risk_sl.risk_amount,
        "sl_distance_pct": risk_sl.sl_to_entry_distance,
        
        # Step 7
        "tp1_price": tp_levels.tp1,
        "tp1_type": tp_levels.tp1_type,
        "tp2_price": tp_levels.tp2,
        "tp2_type": tp_levels.tp2_type,
        "tp3_price": tp_levels.tp3,
        "tp3_type": tp_levels.tp3_type,
        
        # Step 8
        "probability": {
            "htf_alignment": probability.htf_alignment,
            "liquidity_quality": probability.liquidity_quality,
            "sweep_strength": probability.sweep_strength,
            "structure_clarity": probability.structure_clarity,
            "entry_precision": probability.entry_precision,
            "total_score": probability.total_score,
            "acceptable": probability.acceptable
        }
    }
    
    return setup

# ---------------- ALERT FORMATTING ----------------
async def send_setup_alert(setup: Dict):
    """Format and send setup alert"""
    
    # Calculate RR ratios
    entry = setup["entry_price"]
    sl = setup["sl_price"]
    tp1 = setup["tp1_price"]
    
    risk = abs(entry - sl)
    reward_tp1 = abs(tp1 - entry)
    rr_ratio = reward_tp1 / risk if risk > 0 else 0
    
    # Format message
    msg = f"""
🔥 <b>ROMEOTPT A+ SETUP CONFIRMED</b>

<b>Symbol:</b> {setup['symbol']}
<b>Side:</b> {setup['side']}
<b>Entry:</b> {setup['entry_price']:.8f}
<b>Current:</b> {setup['current_price']:.8f}

<b>Probability Score:</b> {setup['probability']['total_score']:.2f}/5.0
<b>RR Ratio:</b> {rr_ratio:.2f}:1

🎯 <b>Targets:</b>
TP1: {setup['tp1_price']:.8f} ({setup['tp1_type']})
TP2: {setup['tp2_price']:.8f} ({setup['tp2_type']})
TP3: {setup['tp3_price']:.8f} ({setup['tp3_type']})

🛡️ <b>Risk:</b>
SL: {setup['sl_price']:.8f} ({setup['sl_invalidation_type']})
Risk: {setup['risk_amount']:.8f} ({setup['sl_distance_pct']:.2f}%)

📊 <b>Analysis:</b>
• HTF: {setup['htf_bias']} in {setup['htf_premium_discount']}
• Sweep: {setup['sweep_type']} (strength: {setup['sweep_strength']:.2f})
• Structure: {setup['structure_shift_type']}
• Entry: {setup['entry_type']}

✅ <b>Probability Components:</b>
HTF Alignment: {setup['probability']['htf_alignment']:.2f}
Liquidity Quality: {setup['probability']['liquidity_quality']:.2f}
Sweep Strength: {setup['probability']['sweep_strength']:.2f}
Structure Clarity: {setup['probability']['structure_clarity']:.2f}
Entry Precision: {setup['probability']['entry_precision']:.2f}

<i>Detected: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</i>
"""
    
    await send_telegram(msg)
    
    # Log to database
    async with db_lock:
        await db_conn.execute("""
            INSERT INTO signals VALUES (
                NULL, :symbol, :timestamp, :side,
                :htf_bias, :htf_range_high, :htf_range_low, :htf_premium_discount,
                :htf_liquidity_zones, :htf_structure,
                :liquidity_from, :liquidity_to, :has_clear_target,
                :sweep_type, :swept_price, :sweep_impulsive, :sweep_strength,
                :structure_shift_type, :structure_shift_confirmed, :structure_description,
                :entry_type, :entry_price, :entry_low, :entry_high, :entry_aligns_htf, :entry_reaction_confirmed,
                :sl_price, :sl_invalidation_type, :risk_amount, :sl_distance_pct,
                :tp1_price, :tp1_type, :tp2_price, :tp2_type, :tp3_price, :tp3_type,
                :prob_htf_alignment, :prob_liquidity_quality, :prob_sweep_strength,
                :prob_structure_clarity, :prob_entry_precision, :prob_total_score, :prob_acceptable,
                :current_price, 'DETECTED', ''
            )
        """, {
            "symbol": setup["symbol"],
            "timestamp": setup["timestamp"],
            "side": setup["side"],
            "htf_bias": setup["htf_bias"],
            "htf_range_high": setup["htf_range_high"],
            "htf_range_low": setup["htf_range_low"],
            "htf_premium_discount": setup["htf_premium_discount"],
            "htf_liquidity_zones": json.dumps(setup["htf_liquidity_zones"]),
            "htf_structure": json.dumps(setup["htf_structure"]),
            "liquidity_from": json.dumps(setup["liquidity_from"]),
            "liquidity_to": json.dumps(setup["liquidity_to"]),
            "has_clear_target": setup["has_clear_target"],
            "sweep_type": setup["sweep_type"],
            "swept_price": setup["swept_price"],
            "sweep_impulsive": setup["sweep_impulsive"],
            "sweep_strength": setup["sweep_strength"],
            "structure_shift_type": setup["structure_shift_type"],
            "structure_shift_confirmed": setup["structure_shift_confirmed"],
            "structure_description": setup["structure_description"],
            "entry_type": setup["entry_type"],
            "entry_price": setup["entry_price"],
            "entry_low": setup["entry_low"],
            "entry_high": setup["entry_high"],
            "entry_aligns_htf": setup["entry_aligns_htf"],
            "entry_reaction_confirmed": setup["entry_reaction_confirmed"],
            "sl_price": setup["sl_price"],
            "sl_invalidation_type": setup["sl_invalidation_type"],
            "risk_amount": setup["risk_amount"],
            "sl_distance_pct": setup["sl_distance_pct"],
            "tp1_price": setup["tp1_price"],
            "tp1_type": setup["tp1_type"],
            "tp2_price": setup["tp2_price"],
            "tp2_type": setup["tp2_type"],
            "tp3_price": setup["tp3_price"],
            "tp3_type": setup["tp3_type"],
            "prob_htf_alignment": setup["probability"]["htf_alignment"],
            "prob_liquidity_quality": setup["probability"]["liquidity_quality"],
            "prob_sweep_strength": setup["probability"]["sweep_strength"],
            "prob_structure_clarity": setup["probability"]["structure_clarity"],
            "prob_entry_precision": setup["probability"]["entry_precision"],
            "prob_total_score": setup["probability"]["total_score"],
            "prob_acceptable": setup["probability"]["acceptable"],
            "current_price": setup["current_price"]
        })
        await db_conn.commit()

# ---------------- MAIN SCANNER ----------------
async def scanner_main(exchange):
    """Main scanning loop"""
    
    await send_telegram("🚀 ROMEOTPT v2 Scanner Started - 8-Step Exact Match")
    await send_telegram("Step 1: HTF Bias → 2: Liquidity Map → 3: Sweep → 4: Structure → 5: Entry → 6: SL → 7: TP → 8: Probability")
    
    while True:
        try:
            # Get top volume pairs
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, v.get("quoteVolume", 0)) 
                         for s, v in tickers.items() 
                         if s.endswith("/USDT")]
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            top_pairs = usdt_pairs[:TOP_N]
            
            log.info(f"📊 Scanning {len(top_pairs)} symbols...")
            
            setups_found = 0
            for symbol, volume in top_pairs:
                try:
                    setup = await scan_symbol_full(exchange, symbol)
                    if setup:
                        await send_setup_alert(setup)
                        setups_found += 1
                        # Rate limiting
                        await asyncio.sleep(2)
                except Exception as e:
                    log.error(f"Error scanning {symbol}: {e}")
                    continue
            
            if setups_found > 0:
                log.info(f"✅ Found {setups_found} A+ setups")
            else:
                log.info("⏳ No setups found this scan")
            
        except Exception as e:
            log.exception(f"Scanner error: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/health")
async def health():
    return {"status": "healthy", "scanner": "ROMEOTPT v2"}

@app.get("/setups")
async def get_setups(limit: int = 20, min_score: float = 3.5):
    async with db_lock:
        async with db_conn.execute(
            """SELECT * FROM signals 
               WHERE prob_total_score >= ? 
               ORDER BY timestamp DESC LIMIT ?""",
            (min_score, limit)
        ) as cursor:
            columns = [description[0] for description in cursor.description]
            rows = await cursor.fetchall()
        
        setups = []
        for row in rows:
            setup = dict(zip(columns, row))
            # Parse JSON fields
            json_fields = ["htf_liquidity_zones_json", "htf_structure_json",
                          "liquidity_from_json", "liquidity_to_json"]
            for field in json_fields:
                if setup.get(field):
                    try:
                        key = field.replace("_json", "")
                        setup[key] = json.loads(setup[field])
                    except:
                        pass
            setups.append(setup)
        
        return {"setups": setups, "count": len(setups)}

# ---------------- MAIN ----------------
async def main():
    global db_conn
    
    # Initialize
    await init_db()
    
    # Create exchange
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"}
    })
    
    # Start scanner
    await scanner_main(exchange)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", help="Run HTTP server")
    args = parser.parse_args()
    
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Shutting down ROMEOTPT v2 scanner...")
        finally:
            if db_conn:
                asyncio.run(db_conn.close())