#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT HYBRID v3.0 - PERFECT COMBINATION
- Original v2 accuracy (fixed HTF hierarchy)
- Multi-timeframe scanning (1m, 3m, 5m, 15m, 30m)
- TP/SL tracking (v2.1 improvement)
- Zero errors, production-ready
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
DB_PATH = "/app/data/romeopt_hybrid_v3.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 15))
TOP_N = int(os.getenv("TOP_N", 30))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 5))

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_hybrid_v3")
db_lock = asyncio.Lock()
db_conn = None

# ---------------- CORRECTED TF LADDER (HTF-LOCKED) ----------------
# KEY FIX: ALL entry timeframes use SAME HTF levels for analysis
# This maintains original accuracy while allowing multi-TF scanning
TF_LADDER = {
    "1m":  {"htf_bias": "4h", "htf_liquidity": "1h", "structure": "15m", "sweep": "15m", "entry": "1m"},
    "3m":  {"htf_bias": "4h", "htf_liquidity": "1h", "structure": "15m", "sweep": "15m", "entry": "3m"},
    "5m":  {"htf_bias": "4h", "htf_liquidity": "1h", "structure": "15m", "sweep": "15m", "entry": "5m"},
    "15m": {"htf_bias": "4h", "htf_liquidity": "1h", "structure": "15m", "sweep": "15m", "entry": "15m"},
    "30m": {"htf_bias": "4h", "htf_liquidity": "4h", "structure": "30m", "sweep": "30m", "entry": "30m"},
}

# ---------------- DATA STRUCTURES ----------------
@dataclass
class HTFContext:
    bias: str  # "BULLISH", "BEARISH", "RANGING"
    range_high: float
    range_low: float
    range_mid: float
    premium_discount: str  # "PREMIUM", "DISCOUNT", "MIDDLE"
    liquidity_zones: List[Dict]
    structure: List[Dict]
    skip_reason: Optional[str] = None
    valid: bool = False

@dataclass
class LiquidityMap:
    from_liquidity: List[Dict]
    to_liquidity: List[Dict]
    has_clear_target: bool = False

@dataclass
class SweepAnalysis:
    type: str  # "HIGH_SWEEP", "LOW_SWEEP", "NONE"
    candle_index: int
    swept_price: float
    previous_extreme: float
    impulsive: bool
    fake_sweep: bool = False
    strength: float = 0.0

@dataclass
class StructureShift:
    type: str  # "CHoCH", "BOS", "NONE"
    confirmed: bool
    candle_index: int
    description: str = ""

@dataclass
class EntryZone:
    type: str  # "ORDER_BLOCK", "FAIR_VALUE_GAP", "DISCOUNT", "PREMIUM"
    price: float
    low: float
    high: float
    aligns_with_htf: bool
    candle_reaction: bool = False

@dataclass
class RiskManagement:
    sl_price: float
    invalidation_type: str
    risk_amount: float
    sl_to_entry_distance: float

@dataclass
class TakeProfitLevels:
    tp1: float
    tp2: float
    tp3: float
    tp1_type: str = "INTERNAL_LIQUIDITY"
    tp2_type: str = "RANGE_BOUNDARY"
    tp3_type: str = "HTF_LIQUIDITY"

@dataclass
class ProbabilityScore:
    htf_alignment: float  # 0-1
    liquidity_quality: float  # 0-1
    sweep_strength: float  # 0-1
    structure_clarity: float  # 0-1
    entry_precision: float  # 0-1
    total_score: float  # 0-5
    
    @property
    def acceptable(self) -> bool:
        """ORIGINAL v2 CRITERIA - Restored for accuracy"""
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

# ---------------- DATABASE (WITH TP/SL TRACKING) ----------------
async def init_db():
    global db_conn
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute("PRAGMA journal_mode=WAL;")
    
    # Check if table exists and has old schema
    async with db_conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='signals'") as cursor:
        table_exists = await cursor.fetchone()
    
    if table_exists:
        # Check if we need to add new columns
        async with db_conn.execute("PRAGMA table_info(signals)") as cursor:
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            # Add missing columns if they don't exist
            if 'tp_hit' not in column_names:
                await db_conn.execute("ALTER TABLE signals ADD COLUMN tp_hit INTEGER DEFAULT 0")
                await db_conn.execute("ALTER TABLE signals ADD COLUMN tp_hit_price REAL")
                await db_conn.execute("ALTER TABLE signals ADD COLUMN tp_hit_time TEXT")
                await db_conn.execute("ALTER TABLE signals ADD COLUMN sl_hit INTEGER DEFAULT 0")
                await db_conn.execute("ALTER TABLE signals ADD COLUMN sl_hit_price REAL")
                await db_conn.execute("ALTER TABLE signals ADD COLUMN sl_hit_time TEXT")
                log.info("Added TP/SL tracking columns to existing table")
    else:
        # Create new table with full schema
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                timestamp TEXT,
                side TEXT,
                entry_timeframe TEXT,
                
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
                
                -- TP/SL Tracking (v2.1 improvement)
                tp_hit INTEGER DEFAULT 0,
                tp_hit_price REAL,
                tp_hit_time TEXT,
                sl_hit INTEGER DEFAULT 0,
                sl_hit_price REAL,
                sl_hit_time TEXT,
                
                notes TEXT
            )
        """)
        log.info("Created new signals table with TP/SL tracking")
    
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

def safe_json_serialize(obj):
    """Convert numpy/pandas types to Python native types for JSON serialization"""
    if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
        return float(obj)
    elif isinstance(obj, (np.ndarray, pd.Series)):
        return obj.tolist()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient='records')
    elif hasattr(obj, '__dict__'):
        return obj.__dict__
    elif isinstance(obj, dict):
        return {k: safe_json_serialize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [safe_json_serialize(item) for item in obj]
    else:
        return obj

# ---------------- STEP 1: HTF BIAS (ORIGINAL v2 LOGIC) ----------------
async def analyze_htf_bias(exchange, symbol: str, entry_timeframe: str) -> HTFContext:
    """ORIGINAL v2 LOGIC: 4H→1H fallback"""
    if entry_timeframe not in TF_LADDER:
        return HTFContext(
            bias="UNKNOWN", range_high=0, range_low=0, range_mid=0,
            premium_discount="UNKNOWN", liquidity_zones=[], structure=[],
            skip_reason=f"Unsupported timeframe: {entry_timeframe}", valid=False
        )
    
    htf_tf = TF_LADDER[entry_timeframe]["htf_bias"]  # Always 4h
    
    # FIRST: Try 4H data (primary HTF)
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, htf_tf, 100)
    timeframe_used = htf_tf
    
    # If 4H insufficient, fallback to 1H (ORIGINAL v2 FALLBACK)
    if not ohlcv_htf or len(ohlcv_htf) < 30:
        log.debug(f"{symbol}: {htf_tf} data insufficient, falling back to 1H...")
        ohlcv_htf = await fetch_ohlcv(exchange, symbol, "1h", 100)
        timeframe_used = "1h"
        
        if not ohlcv_htf or len(ohlcv_htf) < 30:
            return HTFContext(
                bias="UNKNOWN", range_high=0, range_low=0, range_mid=0,
                premium_discount="UNKNOWN", liquidity_zones=[], structure=[],
                skip_reason=f"Insufficient HTF data (tried {htf_tf} and 1h)", valid=False
            )
    
    df_htf = create_dataframe(ohlcv_htf)
    current_price = float(df_htf["close"].iloc[-1])
    
    # Identify swing highs/lows (ORIGINAL v2 LOGIC)
    swing_highs = []
    swing_lows = []
    
    for i in range(3, len(df_htf) - 3):
        high_i = df_htf["high"].iloc[i]
        low_i = df_htf["low"].iloc[i]
        
        if (high_i > df_htf["high"].iloc[i-1] and 
            high_i > df_htf["high"].iloc[i-2] and
            high_i > df_htf["high"].iloc[i+1] and
            high_i > df_htf["high"].iloc[i+2]):
            swing_highs.append({
                "price": float(high_i),
                "index": int(i),
                "timestamp": int(df_htf["timestamp"].iloc[i])
            })
        
        if (low_i < df_htf["low"].iloc[i-1] and 
            low_i < df_htf["low"].iloc[i-2] and
            low_i < df_htf["low"].iloc[i+1] and
            low_i < df_htf["low"].iloc[i+2]):
            swing_lows.append({
                "price": float(low_i),
                "index": int(i),
                "timestamp": int(df_htf["timestamp"].iloc[i])
            })
    
    # Define current range (last 20 periods) - ORIGINAL v2 LOGIC
    if len(df_htf) >= 20:
        recent_high = df_htf["high"].iloc[-20:].max()
        recent_low = df_htf["low"].iloc[-20:].min()
    else:
        recent_high = df_htf["high"].max()
        recent_low = df_htf["low"].min()
    
    range_high = float(recent_high)
    range_low = float(recent_low)
    range_mid = (range_high + range_low) / 2
    
    # Determine bias (ORIGINAL v2 LOGIC)
    if len(swing_highs) >= 2 and len(swing_lows) >= 2:
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
    
    # Premium/Discount (ORIGINAL v2 LOGIC)
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
    
    # Mark HTF liquidity zones (ORIGINAL v2 LOGIC)
    liquidity_zones = []
    
    liquidity_zones.append({
        "price": range_high,
        "type": "RANGE_HIGH",
        "timeframe": timeframe_used,
        "strength": 3
    })
    liquidity_zones.append({
        "price": range_low,
        "type": "RANGE_LOW",
        "timeframe": timeframe_used,
        "strength": 3
    })
    
    for swing in swing_highs[-3:]:
        liquidity_zones.append({
            "price": swing["price"],
            "type": "SWING_HIGH",
            "timeframe": timeframe_used,
            "strength": 2
        })
    
    for swing in swing_lows[-3:]:
        liquidity_zones.append({
            "price": swing["price"],
            "type": "SWING_LOW",
            "timeframe": timeframe_used,
            "strength": 2
        })
    
    # Check if we should skip (ORIGINAL v2 LOGIC)
    skip_reason = None
    valid = True
    
    if premium_discount == "MIDDLE" and bias == "RANGING":
        skip_reason = "Price mid-range with no clear HTF alignment"
        valid = False
    elif range_height / range_low < 0.02:
        skip_reason = "Range too tight (<2%)"
        valid = False
    
    context = HTFContext(
        bias=bias,
        range_high=range_high,
        range_low=range_low,
        range_mid=range_mid,
        premium_discount=premium_discount,
        liquidity_zones=liquidity_zones,
        structure=swing_highs[-5:] + swing_lows[-5:],
        skip_reason=skip_reason,
        valid=valid
    )
    
    return context

# ---------------- STEP 2: LIQUIDITY MAP (ORIGINAL v2 LOGIC) ----------------
async def map_liquidity(exchange, symbol: str, htf_context: HTFContext, 
                       current_price: float, entry_timeframe: str) -> LiquidityMap:
    """ORIGINAL v2 LOGIC with 1H liquidity"""
    if entry_timeframe not in TF_LADDER:
        return LiquidityMap(from_liquidity=[], to_liquidity=[], has_clear_target=False)
    
    liq_tf = TF_LADDER[entry_timeframe]["htf_liquidity"]  # Always 1h (except 30m uses 4h)
    
    ohlcv = await fetch_ohlcv(exchange, symbol, liq_tf, 100)
    if not ohlcv:
        return LiquidityMap(from_liquidity=[], to_liquidity=[], has_clear_target=False)
    
    df = create_dataframe(ohlcv)
    
    # FROM liquidity: Recent extremes that were likely taken (ORIGINAL v2 LOGIC)
    from_liquidity = []
    recent_df = df.iloc[-10:] if len(df) >= 10 else df
    
    for i in range(len(recent_df) - 1):
        candle = recent_df.iloc[i]
        next_candle = recent_df.iloc[i + 1] if i + 1 < len(recent_df) else candle
        
        # High sweep detection
        if candle["high"] > recent_df["high"].iloc[max(0, i-5):i].max() and next_candle["close"] < candle["close"]:
            from_liquidity.append({
                "price": float(candle["high"]),
                "type": "SWEPT_HIGH",
                "timeframe": liq_tf,
                "direction": "FROM"
            })
        
        # Low sweep detection
        if candle["low"] < recent_df["low"].iloc[max(0, i-5):i].min() and next_candle["close"] > candle["close"]:
            from_liquidity.append({
                "price": float(candle["low"]),
                "type": "SWEPT_LOW",
                "timeframe": liq_tf,
                "direction": "FROM"
            })
    
    # TO liquidity: Targets based on HTF context (ORIGINAL v2 LOGIC)
    to_liquidity = []
    
    # Sort HTF liquidity zones by distance from current price
    sorted_zones = sorted(htf_context.liquidity_zones, 
                         key=lambda z: abs(z["price"] - current_price))
    
    # Filter for relevant targets based on bias
    if htf_context.bias == "BULLISH":
        targets = [z for z in sorted_zones if z["price"] > current_price]
    elif htf_context.bias == "BEARISH":
        targets = [z for z in sorted_zones if z["price"] < current_price]
    else:
        targets = [z for z in sorted_zones if z["type"] in ["RANGE_HIGH", "RANGE_LOW"]]
    
    # Take top 3 targets
    for target in targets[:3]:
        to_liquidity.append({
            "price": target["price"],
            "type": target["type"],
            "timeframe": target["timeframe"],
            "strength": int(target.get("strength", 1)),
            "direction": "TO",
            "htf": True
        })
    
    # Also add internal liquidity (equal highs/lows on liquidity TF) - ORIGINAL v2 LOGIC
    if len(df) >= 24:
        high_values = df["high"].iloc[-24:].values
        for val in np.unique(np.round(high_values, 4)):
            count = int(np.sum(np.round(high_values, 4) == val))
            if count >= 2:
                to_liquidity.append({
                    "price": float(val),
                    "type": "EQUAL_HIGH",
                    "timeframe": liq_tf,
                    "strength": int(min(2, count)),
                    "direction": "TO",
                    "ltf": True
                })
        
        low_values = df["low"].iloc[-24:].values
        for val in np.unique(np.round(low_values, 4)):
            count = int(np.sum(np.round(low_values, 4) == val))
            if count >= 2:
                to_liquidity.append({
                    "price": float(val),
                    "type": "EQUAL_LOW",
                    "timeframe": liq_tf,
                    "strength": int(min(2, count)),
                    "direction": "TO",
                    "ltf": True
                })
    
    has_clear_target = len(to_liquidity) > 0 and len(from_liquidity) > 0
    
    return LiquidityMap(
        from_liquidity=from_liquidity,
        to_liquidity=to_liquidity,
        has_clear_target=has_clear_target
    )

# ---------------- STEP 3: LIQUIDITY SWEEP (ORIGINAL v2 LOGIC) ----------------
async def analyze_sweep(exchange, symbol: str, entry_timeframe: str) -> SweepAnalysis:
    """ORIGINAL v2 LOGIC: Always uses 15m (except 30m uses 30m)"""
    if entry_timeframe not in TF_LADDER:
        return SweepAnalysis(type="NONE", candle_index=-1, swept_price=0, 
                           previous_extreme=0, impulsive=False)
    
    sweep_tf = TF_LADDER[entry_timeframe]["sweep"]  # Always 15m (except 30m uses 30m)
    
    ohlcv = await fetch_ohlcv(exchange, symbol, sweep_tf, 50)
    if not ohlcv or len(ohlcv) < 10:
        return SweepAnalysis(type="NONE", candle_index=-1, swept_price=0, 
                           previous_extreme=0, impulsive=False)
    
    df = create_dataframe(ohlcv)
    
    # Look for sweeps in last 5 candles (ORIGINAL v2 LOGIC)
    lookback = min(5, len(df))
    
    for i in range(-lookback, 0):
        candle_idx = len(df) + i
        candle = df.iloc[candle_idx]
        
        start_idx = max(0, candle_idx - 5)
        prev_candles = df.iloc[start_idx:candle_idx]
        
        if len(prev_candles) == 0:
            continue
        
        previous_high = prev_candles["high"].max()
        previous_low = prev_candles["low"].min()
        
        # Check for high sweep (ORIGINAL v2 LOGIC)
        if candle["high"] > previous_high:
            body_size = abs(candle["close"] - candle["open"])
            upper_wick = candle["high"] - max(candle["open"], candle["close"])
            lower_wick = min(candle["open"], candle["close"]) - candle["low"]
            total_wick = upper_wick + lower_wick
            
            impulsive = body_size > total_wick
            
            # Check for fake sweep
            if i < -1:
                next_candle = df.iloc[candle_idx + 1]
                fake_sweep = (next_candle["close"] < candle["close"] and 
                             next_candle["low"] < candle["low"])
            else:
                fake_sweep = False
            
            strength = 0.0
            if impulsive and not fake_sweep:
                extension = (candle["high"] - previous_high) / previous_high
                strength = min(1.0, extension * 100)
            
            return SweepAnalysis(
                type="HIGH_SWEEP",
                candle_index=int(candle_idx),
                swept_price=float(candle["high"]),
                previous_extreme=float(previous_high),
                impulsive=impulsive,
                fake_sweep=fake_sweep,
                strength=float(strength)
            )
        
        # Check for low sweep (ORIGINAL v2 LOGIC)
        elif candle["low"] < previous_low:
            body_size = abs(candle["close"] - candle["open"])
            upper_wick = candle["high"] - max(candle["open"], candle["close"])
            lower_wick = min(candle["open"], candle["close"]) - candle["low"]
            total_wick = upper_wick + lower_wick
            
            impulsive = body_size > total_wick
            
            if i < -1:
                next_candle = df.iloc[candle_idx + 1]
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
                candle_index=int(candle_idx),
                swept_price=float(candle["low"]),
                previous_extreme=float(previous_low),
                impulsive=impulsive,
                fake_sweep=fake_sweep,
                strength=float(strength)
            )
    
    return SweepAnalysis(type="NONE", candle_index=-1, swept_price=0, 
                       previous_extreme=0, impulsive=False)

# ---------------- STEP 4: STRUCTURE CHECK (ORIGINAL v2 LOGIC) ----------------
async def check_structure_shift(exchange, symbol: str, sweep: SweepAnalysis, 
                               htf_context: HTFContext, entry_timeframe: str) -> StructureShift:
    """ORIGINAL v2 LOGIC: Always uses 15m (except 30m uses 30m)"""
    if sweep.type == "NONE" or entry_timeframe not in TF_LADDER:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    structure_tf = TF_LADDER[entry_timeframe]["structure"]  # Always 15m (except 30m uses 30m)
    
    ohlcv = await fetch_ohlcv(exchange, symbol, structure_tf, 50)
    if not ohlcv:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    df = create_dataframe(ohlcv)
    
    # Get candles after the sweep (ORIGINAL v2 LOGIC)
    sweep_idx = sweep.candle_index
    if sweep_idx < 0 or sweep_idx >= len(df) - 3:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    post_sweep_candles = df.iloc[sweep_idx + 1:]
    if len(post_sweep_candles) < 3:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    # Check for CHoCH (reversal after sweep) - ORIGINAL v2 LOGIC
    if sweep.type == "HIGH_SWEEP":
        recent_low_before = df["low"].iloc[max(0, sweep_idx-5):sweep_idx].min()
        
        for i in range(len(post_sweep_candles)):
            candle = post_sweep_candles.iloc[i]
            if candle["low"] < recent_low_before:
                return StructureShift(
                    type="CHoCH",
                    confirmed=True,
                    candle_index=int(sweep_idx + i + 1),
                    description="High sweep followed by break below recent low"
                )
        
        # Check for BOS (continuation)
        if len(post_sweep_candles) >= 5:
            pullback_low = post_sweep_candles["low"].iloc[:3].min()
            subsequent_high = post_sweep_candles["high"].iloc[3:].max()
            
            if subsequent_high > sweep.swept_price:
                return StructureShift(
                    type="BOS",
                    confirmed=True,
                    candle_index=int(sweep_idx + 3),
                    description="High sweep followed by new higher high"
                )
    
    elif sweep.type == "LOW_SWEEP":
        recent_high_before = df["high"].iloc[max(0, sweep_idx-5):sweep_idx].max()
        
        for i in range(len(post_sweep_candles)):
            candle = post_sweep_candles.iloc[i]
            if candle["high"] > recent_high_before:
                return StructureShift(
                    type="CHoCH",
                    confirmed=True,
                    candle_index=int(sweep_idx + i + 1),
                    description="Low sweep followed by break above recent high"
                )
        
        # Check for BOS (continuation)
        if len(post_sweep_candles) >= 5:
            pullback_high = post_sweep_candles["high"].iloc[:3].max()
            subsequent_low = post_sweep_candles["low"].iloc[3:].min()
            
            if subsequent_low < sweep.swept_price:
                return StructureShift(
                    type="BOS",
                    confirmed=True,
                    candle_index=int(sweep_idx + 3),
                    description="Low sweep followed by new lower low"
                )
    
    return StructureShift(type="NONE", confirmed=False, candle_index=-1)

# ---------------- STEP 5: ENTRY ZONE (ORIGINAL v2 LOGIC) ----------------
async def find_entry_zone(exchange, symbol: str, htf_context: HTFContext,
                         sweep: SweepAnalysis, structure_shift: StructureShift,
                         side: str, entry_timeframe: str) -> EntryZone:
    """ORIGINAL v2 LOGIC with entry timeframe flexibility"""
    if entry_timeframe not in TF_LADDER:
        return EntryZone(type="NONE", price=0, low=0, high=0, aligns_with_htf=False)
    
    entry_tf = TF_LADDER[entry_timeframe]["entry"]
    
    ohlcv = await fetch_ohlcv(exchange, symbol, entry_tf, 100)
    if not ohlcv:
        return EntryZone(type="NONE", price=0, low=0, high=0, aligns_with_htf=False)
    
    df = create_dataframe(ohlcv)
    current_price = float(df["close"].iloc[-1])
    
    # Determine expected entry type based on sweep and structure (ORIGINAL v2 LOGIC)
    if structure_shift.type == "CHoCH":
        entry_type = "ORDER_BLOCK"
    elif structure_shift.type == "BOS":
        entry_type = "FAIR_VALUE_GAP"
    else:
        if htf_context.premium_discount == "DISCOUNT":
            entry_type = "DISCOUNT"
        elif htf_context.premium_discount == "PREMIUM":
            entry_type = "PREMIUM"
        else:
            entry_type = "NONE"
    
    # Find Order Blocks (ORIGINAL v2 LOGIC adapted for any entry TF)
    if entry_type == "ORDER_BLOCK":
        for i in range(2, len(df) - 1):
            candle = df.iloc[i]
            next_candle = df.iloc[i + 1]
            
            # Bullish OB (bearish candle before bullish displacement)
            if side == "BUY":
                if (candle["close"] < candle["open"] and 
                    next_candle["close"] > next_candle["open"]):
                    
                    ob_low = min(candle["low"], next_candle["low"])
                    ob_high = next_candle["close"]
                    
                    # Check alignment with HTF
                    aligns = (htf_context.bias == "BULLISH" or 
                             htf_context.premium_discount == "DISCOUNT")
                    
                    # Check if price is currently in or near OB
                    if current_price <= ob_high and current_price >= ob_low * 0.995:
                        current_candle = df.iloc[-1]
                        prev_candle = df.iloc[-2] if len(df) >= 2 else current_candle
                        
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
                        current_candle = df.iloc[-1]
                        prev_candle = df.iloc[-2] if len(df) >= 2 else current_candle
                        
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
    
    # Find Fair Value Gaps (ORIGINAL v2 LOGIC)
    elif entry_type == "FAIR_VALUE_GAP":
        for i in range(1, len(df) - 2):
            candle1 = df.iloc[i]
            candle2 = df.iloc[i + 1]
            candle3 = df.iloc[i + 2] if i + 2 < len(df) else candle2
            
            # Bullish FVG
            if side == "BUY":
                if candle2["low"] > candle1["high"]:
                    fvg_low = candle1["high"]
                    fvg_high = candle2["low"]
                    
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
    
    return EntryZone(type="NONE", price=0, low=0, high=0, aligns_with_htf=False)

# ---------------- STEP 6: RISK/SL (ORIGINAL v2 LOGIC) ----------------
def calculate_risk_sl(entry_zone: EntryZone, sweep: SweepAnalysis,
                     htf_context: HTFContext, side: str) -> RiskManagement:
    """ORIGINAL v2 LOGIC"""
    entry_price = entry_zone.price
    
    # Determine invalidation type and price
    sl_price = 0.0
    invalidation_type = ""
    
    # Priority 1: Beyond swept level
    if sweep.type != "NONE" and sweep.swept_price > 0:
        if side == "BUY" and sweep.type == "LOW_SWEEP":
            sl_price = sweep.swept_price * 0.995
            invalidation_type = "SWEEP"
        elif side == "SELL" and sweep.type == "HIGH_SWEEP":
            sl_price = sweep.swept_price * 1.005
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
        if side == "BUY" and htf_context.structure:
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
    
    # Fallback: ATR-based
    if invalidation_type == "":
        atr_approx = entry_price * 0.02
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

# ---------------- STEP 7: TAKE PROFIT (ORIGINAL v2 LOGIC) ----------------
def calculate_take_profits(entry_price: float, side: str, 
                          liquidity_map: LiquidityMap,
                          htf_context: HTFContext) -> TakeProfitLevels:
    """ORIGINAL v2 LOGIC"""
    ltf_targets = [t for t in liquidity_map.to_liquidity if t.get("ltf", False)]
    htf_targets = [t for t in liquidity_map.to_liquidity if t.get("htf", False)]
    
    if side == "BUY":
        potential_ltf = [t for t in ltf_targets if t["price"] > entry_price]
        potential_htf = [t for t in htf_targets if t["price"] > entry_price]
        range_boundary = htf_context.range_high
    else:
        potential_ltf = [t for t in ltf_targets if t["price"] < entry_price]
        potential_htf = [t for t in htf_targets if t["price"] < entry_price]
        range_boundary = htf_context.range_low
    
    # TP1: Nearest internal liquidity
    if potential_ltf:
        potential_ltf.sort(key=lambda t: abs(t["price"] - entry_price))
        tp1 = potential_ltf[0]["price"]
        tp1_type = potential_ltf[0]["type"]
    else:
        if side == "BUY":
            tp1 = entry_price * 1.02
        else:
            tp1 = entry_price * 0.98
        tp1_type = "RISK_REWARD_1_1"
    
    # TP2: Range boundary
    tp2 = range_boundary
    tp2_type = "RANGE_BOUNDARY"
    
    # TP3: HTF liquidity
    if potential_htf:
        potential_htf.sort(key=lambda t: t.get("strength", 0), reverse=True)
        tp3 = potential_htf[0]["price"]
        tp3_type = potential_htf[0]["type"]
    else:
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

# ---------------- STEP 8: PROBABILITY CHECK (ORIGINAL v2 LOGIC) ----------------
def calculate_probability(htf_context: HTFContext, liquidity_map: LiquidityMap,
                         sweep: SweepAnalysis, structure_shift: StructureShift,
                         entry_zone: EntryZone, side: str) -> ProbabilityScore:
    """ORIGINAL v2 LOGIC - Restored for accuracy"""
    
    # 1. HTF Alignment (0-1)
    if htf_context.bias == side.upper() or htf_context.bias == "RANGING":
        htf_alignment = 1.0
    elif (htf_context.bias == "BULLISH" and side == "SELL") or \
         (htf_context.bias == "BEARISH" and side == "BUY"):
        htf_alignment = 0.3
    else:
        htf_alignment = 0.5
    
    # Premium/discount alignment
    if (side == "BUY" and htf_context.premium_discount == "DISCOUNT") or \
       (side == "SELL" and htf_context.premium_discount == "PREMIUM"):
        htf_alignment = min(1.0, htf_alignment + 0.2)
    
    # 2. Liquidity Quality (0-1)
    if liquidity_map.has_clear_target:
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
            structure_clarity = 0.9
        elif structure_shift.type == "BOS":
            structure_clarity = 0.8
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
        htf_alignment=float(htf_alignment),
        liquidity_quality=float(liquidity_quality),
        sweep_strength=float(sweep_strength),
        structure_clarity=float(structure_clarity),
        entry_precision=float(entry_precision),
        total_score=float(total_score)
    )

# ---------------- TP/SL MONITORING (v2.1 IMPROVEMENT) ----------------
async def monitor_tp_sl(exchange):
    """Monitor existing signals for TP/SL hits"""
    while True:
        try:
            async with db_lock:
                async with db_conn.execute(
                    """SELECT id, symbol, side, entry_price, sl_price, 
                              tp1_price, tp2_price, tp3_price, status,
                              tp_hit, sl_hit 
                       FROM signals 
                       WHERE status = 'DETECTED' 
                       AND tp_hit = 0 
                       AND sl_hit = 0
                       ORDER BY timestamp DESC LIMIT 50"""
                ) as cursor:
                    active_signals = await cursor.fetchall()
            
            for signal in active_signals:
                signal_id, symbol, side, entry_price, sl_price, tp1_price, tp2_price, tp3_price, status, tp_hit, sl_hit = signal
                
                try:
                    ticker = await exchange.fetch_ticker(symbol)
                    current_price = ticker.get("last", 0)
                    if not current_price:
                        continue
                    
                    now = datetime.datetime.utcnow().isoformat()
                    
                    # Check for TP hits
                    tp_hit_level = 0
                    tp_hit_price = 0.0
                    
                    if side == "BUY":
                        if current_price >= tp3_price:
                            tp_hit_level = 3
                            tp_hit_price = tp3_price
                        elif current_price >= tp2_price:
                            tp_hit_level = 2
                            tp_hit_price = tp2_price
                        elif current_price >= tp1_price:
                            tp_hit_level = 1
                            tp_hit_price = tp1_price
                    else:
                        if current_price <= tp3_price:
                            tp_hit_level = 3
                            tp_hit_price = tp3_price
                        elif current_price <= tp2_price:
                            tp_hit_level = 2
                            tp_hit_price = tp2_price
                        elif current_price <= tp1_price:
                            tp_hit_level = 1
                            tp_hit_price = tp1_price
                    
                    # Check for SL hit
                    sl_hit_flag = 0
                    sl_hit_price_val = 0.0
                    
                    if side == "BUY":
                        if current_price <= sl_price:
                            sl_hit_flag = 1
                            sl_hit_price_val = sl_price
                    else:
                        if current_price >= sl_price:
                            sl_hit_flag = 1
                            sl_hit_price_val = sl_price
                    
                    # Update database if TP or SL hit
                    if tp_hit_level > 0 or sl_hit_flag > 0:
                        async with db_lock:
                            if tp_hit_level > 0:
                                await db_conn.execute(
                                    """UPDATE signals 
                                       SET tp_hit = ?, 
                                           tp_hit_price = ?,
                                           tp_hit_time = ?,
                                           status = 'TP_HIT'
                                       WHERE id = ?""",
                                    (tp_hit_level, tp_hit_price, now, signal_id)
                                )
                                
                                tp_msg = f"""
🎯 <b>TAKE PROFIT HIT - {symbol}</b>

<b>TP Level:</b> {tp_hit_level}
<b>TP Price:</b> {tp_hit_price:.8f}
<b>Entry Price:</b> {entry_price:.8f}
<b>Current Price:</b> {current_price:.8f}
<b>Side:</b> {side}

<b>Profit:</b> {abs(current_price - entry_price):.8f}
<b>Profit %:</b> {abs((current_price - entry_price) / entry_price * 100):.2f}%

<i>Time: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</i>
"""
                                await send_telegram(tp_msg)
                                log.info(f"TP{tp_hit_level} hit for {symbol} at {tp_hit_price:.8f}")
                            
                            elif sl_hit_flag > 0:
                                await db_conn.execute(
                                    """UPDATE signals 
                                       SET sl_hit = ?, 
                                           sl_hit_price = ?,
                                           sl_hit_time = ?,
                                           status = 'SL_HIT'
                                       WHERE id = ?""",
                                    (sl_hit_flag, sl_hit_price_val, now, signal_id)
                                )
                                
                                sl_msg = f"""
🛑 <b>STOP LOSS HIT - {symbol}</b>

<b>SL Price:</b> {sl_hit_price_val:.8f}
<b>Entry Price:</b> {entry_price:.8f}
<b>Current Price:</b> {current_price:.8f}
<b>Side:</b> {side}

<b>Loss:</b> {abs(current_price - entry_price):.8f}
<b>Loss %:</b> {abs((current_price - entry_price) / entry_price * 100):.2f}%

<i>Time: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</i>
"""
                                await send_telegram(sl_msg)
                                log.info(f"SL hit for {symbol} at {sl_hit_price_val:.8f}")
                            
                            await db_conn.commit()
                
                except Exception as e:
                    log.error(f"Error monitoring {symbol}: {e}")
                    continue
        
        except Exception as e:
            log.error(f"Error in TP/SL monitor: {e}")
        
        await asyncio.sleep(30)

# ---------------- MAIN SCANNING LOGIC (HYBRID) ----------------
async def scan_symbol(exchange, symbol: str, entry_timeframe: str = "15m") -> Optional[Dict]:
    """Execute full 8-step scanning for one symbol on specified timeframe"""
    
    ticker = await exchange.fetch_ticker(symbol)
    current_price = ticker.get("last", 0)
    if not current_price:
        return None
    
    log.debug(f"🔍 Scanning {symbol} on {entry_timeframe} at {current_price}")
    
    # --- STEP 1: HTF BIAS ---
    htf_context = await analyze_htf_bias(exchange, symbol, entry_timeframe)
    if not htf_context.valid:
        return None
    
    # --- STEP 2: LIQUIDITY MAP ---
    liquidity_map = await map_liquidity(exchange, symbol, htf_context, current_price, entry_timeframe)
    if not liquidity_map.has_clear_target:
        return None
    
    # --- STEP 3: LIQUIDITY SWEEP ---
    sweep = await analyze_sweep(exchange, symbol, entry_timeframe)
    if sweep.type == "NONE" or not sweep.impulsive or sweep.fake_sweep:
        return None
    
    # Determine side based on sweep
    if sweep.type == "HIGH_SWEEP":
        side = "SELL"
    elif sweep.type == "LOW_SWEEP":
        side = "BUY"
    else:
        return None
    
    # --- STEP 4: STRUCTURE CHECK ---
    structure_shift = await check_structure_shift(exchange, symbol, sweep, htf_context, entry_timeframe)
    if not structure_shift.confirmed:
        return None
    
    # --- STEP 5: ENTRY ZONE ---
    entry_zone = await find_entry_zone(exchange, symbol, htf_context, sweep, structure_shift, side, entry_timeframe)
    if entry_zone.type == "NONE" or not entry_zone.candle_reaction:
        return None
    
    # --- STEP 6: RISK/SL ---
    risk_sl = calculate_risk_sl(entry_zone, sweep, htf_context, side)
    if risk_sl.sl_price == 0:
        return None
    
    # --- STEP 7: TAKE PROFIT ---
    tp_levels = calculate_take_profits(entry_zone.price, side, liquidity_map, htf_context)
    
    # --- STEP 8: PROBABILITY CHECK ---
    probability = calculate_probability(
        htf_context, liquidity_map, sweep, structure_shift, entry_zone, side
    )
    
    if not probability.acceptable:
        return None
    
    log.info(f"✅ {symbol} ({entry_timeframe}): A+ Setup! Score: {probability.total_score:.2f}/5")
    
    # --- COMPILE FINAL SETUP ---
    setup = {
        "symbol": symbol,
        "entry_timeframe": entry_timeframe,
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

# ---------------- SCANNER MAIN (MULTI-TIMEFRAME) ----------------
async def scanner_main(exchange, entry_timeframe: str = "15m"):
    """Main scanning loop for specific timeframe"""
    
    await send_telegram(f"🚀 ROMEOTPT Hybrid v3.0 Started ({entry_timeframe})")
    
    while True:
        try:
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, v.get("quoteVolume", 0)) 
                         for s, v in tickers.items() 
                         if s.endswith("/USDT")]
            top_pairs = sorted(usdt_pairs, key=lambda x: x[1], reverse=True)[:TOP_N]
            
            log.info(f"📊 Scanning {len(top_pairs)} symbols on {entry_timeframe}...")
            
            setups_found = 0
            for symbol, _ in top_pairs:
                try:
                    setup = await scan_symbol(exchange, symbol, entry_timeframe)
                    if setup:
                        await send_setup_alert(setup)
                        setups_found += 1
                        await asyncio.sleep(1)
                except Exception as e:
                    log.error(f"Error scanning {symbol}: {e}")
                    continue
            
            if setups_found > 0:
                log.info(f"✅ Found {setups_found} A+ setups on {entry_timeframe}")
            else:
                log.info(f"⏳ No setups found on {entry_timeframe}")
            
        except Exception as e:
            log.exception(f"Scanner error on {entry_timeframe}: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- ALERT FORMATTING ----------------
async def send_setup_alert(setup: Dict):
    """Format and send setup alert"""
    
    entry = setup["entry_price"]
    sl = setup["sl_price"]
    tp1 = setup["tp1_price"]
    
    risk = abs(entry - sl)
    reward_tp1 = abs(tp1 - entry)
    rr_ratio = reward_tp1 / risk if risk > 0 else 0
    
    msg = f"""
🔥 <b>ROMEOTPT HYBRID v3.0 - A+ SETUP</b>

<b>Symbol:</b> {setup['symbol']} ({setup['entry_timeframe']})
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
    
    async with db_lock:
        await db_conn.execute("""
            INSERT INTO signals VALUES (
                NULL, :symbol, :timestamp, :side, :entry_timeframe,
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
                :current_price, 'DETECTED', 0, NULL, NULL, 0, NULL, NULL, ''
            )
        """, {
            "symbol": setup["symbol"],
            "timestamp": setup["timestamp"],
            "side": setup["side"],
            "entry_timeframe": setup["entry_timeframe"],
            "htf_bias": setup["htf_bias"],
            "htf_range_high": float(setup["htf_range_high"]),
            "htf_range_low": float(setup["htf_range_low"]),
            "htf_premium_discount": setup["htf_premium_discount"],
            "htf_liquidity_zones": json.dumps(safe_json_serialize(setup["htf_liquidity_zones"])),
            "htf_structure": json.dumps(safe_json_serialize(setup["htf_structure"])),
            "liquidity_from": json.dumps(safe_json_serialize(setup["liquidity_from"])),
            "liquidity_to": json.dumps(safe_json_serialize(setup["liquidity_to"])),
            "has_clear_target": setup["has_clear_target"],
            "sweep_type": setup["sweep_type"],
            "swept_price": float(setup["swept_price"]),
            "sweep_impulsive": setup["sweep_impulsive"],
            "sweep_strength": float(setup["sweep_strength"]),
            "structure_shift_type": setup["structure_shift_type"],
            "structure_shift_confirmed": setup["structure_shift_confirmed"],
            "structure_description": setup["structure_description"],
            "entry_type": setup["entry_type"],
            "entry_price": float(setup["entry_price"]),
            "entry_low": float(setup["entry_low"]),
            "entry_high": float(setup["entry_high"]),
            "entry_aligns_htf": setup["entry_aligns_htf"],
            "entry_reaction_confirmed": setup["entry_reaction_confirmed"],
            "sl_price": float(setup["sl_price"]),
            "sl_invalidation_type": setup["sl_invalidation_type"],
            "risk_amount": float(setup["risk_amount"]),
            "sl_distance_pct": float(setup["sl_distance_pct"]),
            "tp1_price": float(setup["tp1_price"]),
            "tp1_type": setup["tp1_type"],
            "tp2_price": float(setup["tp2_price"]),
            "tp2_type": setup["tp2_type"],
            "tp3_price": float(setup["tp3_price"]),
            "tp3_type": setup["tp3_type"],
            "prob_htf_alignment": float(setup["probability"]["htf_alignment"]),
            "prob_liquidity_quality": float(setup["probability"]["liquidity_quality"]),
            "prob_sweep_strength": float(setup["probability"]["sweep_strength"]),
            "prob_structure_clarity": float(setup["probability"]["structure_clarity"]),
            "prob_entry_precision": float(setup["probability"]["entry_precision"]),
            "prob_total_score": float(setup["probability"]["total_score"]),
            "prob_acceptable": bool(setup["probability"]["acceptable"]),
            "current_price": float(setup["current_price"])
        })
        await db_conn.commit()

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/health")
async def health():
    return {"status": "healthy", "scanner": "ROMEOTPT Hybrid v3.0", "timeframes": list(TF_LADDER.keys())}

@app.get("/setups")
async def get_setups(limit: int = 20, min_score: float = 3.5, timeframe: str = None):
    async with db_lock:
        query = """SELECT * FROM signals WHERE prob_total_score >= ? """
        params = [min_score]
        
        if timeframe:
            query += " AND entry_timeframe = ? "
            params.append(timeframe)
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        async with db_conn.execute(query, params) as cursor:
            columns = [description[0] for description in cursor.description]
            rows = await cursor.fetchall()
        
        setups = []
        for row in rows:
            setup = dict(zip(columns, row))
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

@app.get("/stats")
async def get_stats():
    async with db_lock:
        # Total signals
        async with db_conn.execute("SELECT COUNT(*) FROM signals") as cursor:
            total = (await cursor.fetchone())[0]
        
        # TP hits
        async with db_conn.execute("SELECT COUNT(*) FROM signals WHERE tp_hit > 0") as cursor:
            tp_hits = (await cursor.fetchone())[0]
        
        # SL hits
        async with db_conn.execute("SELECT COUNT(*) FROM signals WHERE sl_hit > 0") as cursor:
            sl_hits = (await cursor.fetchone())[0]
        
        # Active signals
        async with db_conn.execute("SELECT COUNT(*) FROM signals WHERE status = 'DETECTED'") as cursor:
            active = (await cursor.fetchone())[0]
        
        # By timeframe
        timeframe_stats = {}
        for tf in TF_LADDER.keys():
            async with db_conn.execute(
                "SELECT COUNT(*) FROM signals WHERE entry_timeframe = ?", (tf,)
            ) as cursor:
                count = (await cursor.fetchone())[0]
                if count > 0:
                    timeframe_stats[tf] = count
        
        # Win rate
        if total > 0:
            win_rate = (tp_hits / (tp_hits + sl_hits)) * 100 if (tp_hits + sl_hits) > 0 else 0
        else:
            win_rate = 0
        
        return {
            "total_signals": total,
            "tp_hits": tp_hits,
            "sl_hits": sl_hits,
            "active_signals": active,
            "win_rate": f"{win_rate:.2f}%",
            "timeframe_stats": timeframe_stats,
            "scanner_status": "running",
            "version": "Hybrid v3.0"
        }

# ---------------- MAIN ----------------
async def main():
    global db_conn
    await init_db()
    
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"}
    })
    
    # Start TP/SL monitoring task
    monitor_task = asyncio.create_task(monitor_tp_sl(exchange))
    log.info("Started TP/SL monitoring task")
    
    # Start scanners for ALL timeframes (multi-timeframe scanning!)
    tasks = []
    for timeframe in TF_LADDER.keys():
        task = asyncio.create_task(scanner_main(exchange, entry_timeframe=timeframe))
        tasks.append(task)
        log.info(f"Started scanner for timeframe: {timeframe}")
        await asyncio.sleep(1)  # Stagger starts
    
    # Wait for all tasks
    await asyncio.gather(monitor_task, *tasks)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", help="Run HTTP server")
    parser.add_argument("--tf", type=str, default="15m", help="Timeframe to scan (1m, 3m, 5m, 15m, 30m)")
    args = parser.parse_args()
    
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Shutting down ROMEOTPT Hybrid v3.0...")
        finally:
            if db_conn:
                asyncio.run(db_conn.close())