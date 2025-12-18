#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT SCANNER v2 - EXACT FIXES ONLY
ONLY implementing what was specified in your instructions.
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

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 15))
TOP_N = int(os.getenv("TOP_N", 30))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 5))

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_v2")
db_lock = asyncio.Lock()
db_conn = None

# ---------------- DYNAMIC TF LADDER ----------------
TF_LADDER = {
    "1m":  {"structure": "1m",  "sweep": "3m",  "liquidity": "15m", "bias": "1h"},
    "3m":  {"structure": "3m",  "sweep": "5m",  "liquidity": "15m", "bias": "1h"},
    "5m":  {"structure": "5m",  "sweep": "15m", "liquidity": "30m", "bias": "1h"},
    "15m": {"structure": "15m", "sweep": "30m", "liquidity": "1h",  "bias": "4h"},
    "30m": {"structure": "30m", "sweep": "1h",  "liquidity": "4h",  "bias": "4h"},
}

# ---------------- DATA STRUCTURES ----------------
@dataclass
class HTFContext:
    bias: str
    range_high: float
    range_low: float
    range_mid: float
    premium_discount: str
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
    type: str
    candle_index: int
    swept_price: float
    previous_extreme: float
    impulsive: bool
    fake_sweep: bool = False
    strength: float = 0.0

@dataclass
class StructureShift:
    type: str
    confirmed: bool
    candle_index: int
    description: str = ""

@dataclass
class EntryZone:
    type: str
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
    htf_alignment: float
    liquidity_quality: float
    sweep_strength: float
    structure_clarity: float
    entry_precision: float
    total_score: float
    
    @property
    def acceptable(self) -> bool:
        """FIX 5: Romeo's strategic probability gate"""
        liquidity_strong = self.liquidity_quality >= 0.7
        structure_strong = self.structure_clarity >= 0.7
        total_acceptable = self.total_score >= 3.3
        return liquidity_strong and structure_strong and total_acceptable

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
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            timestamp TEXT,
            side TEXT,
            entry_timeframe TEXT,
            htf_bias TEXT,
            htf_range_high REAL,
            htf_range_low REAL,
            htf_premium_discount TEXT,
            htf_liquidity_zones_json TEXT,
            htf_structure_json TEXT,
            liquidity_from_json TEXT,
            liquidity_to_json TEXT,
            has_clear_target BOOLEAN,
            sweep_type TEXT,
            swept_price REAL,
            sweep_impulsive BOOLEAN,
            sweep_strength REAL,
            structure_shift_type TEXT,
            structure_shift_confirmed BOOLEAN,
            structure_description TEXT,
            entry_type TEXT,
            entry_price REAL,
            entry_low REAL,
            entry_high REAL,
            entry_aligns_htf BOOLEAN,
            entry_reaction_confirmed BOOLEAN,
            sl_price REAL,
            sl_invalidation_type TEXT,
            risk_amount REAL,
            sl_distance_pct REAL,
            tp1_price REAL,
            tp1_type TEXT,
            tp2_price REAL,
            tp2_type TEXT,
            tp3_price REAL,
            tp3_type TEXT,
            prob_htf_alignment REAL,
            prob_liquidity_quality REAL,
            prob_sweep_strength REAL,
            prob_structure_clarity REAL,
            prob_entry_precision REAL,
            prob_total_score REAL,
            prob_acceptable BOOLEAN,
            current_price REAL,
            status TEXT DEFAULT 'DETECTED',
            notes TEXT
        )
    """)
    await db_conn.commit()

# ---------------- OHLCV UTILS ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 200):
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.debug(f"Failed to fetch {symbol} {timeframe}: {e}")
        return None

def create_dataframe(ohlcv):
    if not ohlcv:
        return None
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def safe_json_serialize(obj):
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

# ---------------- STEP 1: HTF BIAS ----------------
async def analyze_htf_bias(exchange, symbol: str, entry_timeframe: str) -> HTFContext:
    """
    DYNAMIC: Uses appropriate HTF based on entry timeframe from TF_LADDER
    """
    if entry_timeframe not in TF_LADDER:
        return HTFContext(
            bias="UNKNOWN", range_high=0, range_low=0, range_mid=0,
            premium_discount="UNKNOWN", liquidity_zones=[], structure=[],
            skip_reason=f"Unsupported timeframe: {entry_timeframe}", valid=False
        )
    
    htf_tf = TF_LADDER[entry_timeframe]["bias"]
    
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, htf_tf, 100)
    if not ohlcv_htf or len(ohlcv_htf) < 30:
        return HTFContext(
            bias="UNKNOWN", range_high=0, range_low=0, range_mid=0,
            premium_discount="UNKNOWN", liquidity_zones=[], structure=[],
            skip_reason=f"Insufficient {htf_tf} data", valid=False
        )
    
    df_htf = create_dataframe(ohlcv_htf)
    current_price = float(df_htf["close"].iloc[-1])
    
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
    
    if len(df_htf) >= 20:
        recent_high = df_htf["high"].iloc[-20:].max()
        recent_low = df_htf["low"].iloc[-20:].min()
    else:
        recent_high = df_htf["high"].max()
        recent_low = df_htf["low"].min()
    
    range_high = float(recent_high)
    range_low = float(recent_low)
    range_mid = (range_high + range_low) / 2
    
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
    
    liquidity_zones = []
    liquidity_zones.append({
        "price": range_high,
        "type": "RANGE_HIGH",
        "timeframe": htf_tf,
        "strength": 3
    })
    liquidity_zones.append({
        "price": range_low,
        "type": "RANGE_LOW",
        "timeframe": htf_tf,
        "strength": 3
    })
    
    for swing in swing_highs[-3:]:
        liquidity_zones.append({
            "price": swing["price"],
            "type": "SWING_HIGH",
            "timeframe": htf_tf,
            "strength": 2
        })
    
    for swing in swing_lows[-3:]:
        liquidity_zones.append({
            "price": swing["price"],
            "type": "SWING_LOW",
            "timeframe": htf_tf,
            "strength": 2
        })
    
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

# ---------------- STEP 2: LIQUIDITY MAP ----------------
async def map_liquidity(exchange, symbol: str, htf_context: HTFContext, 
                       current_price: float, entry_timeframe: str) -> LiquidityMap:
    """
    DYNAMIC: Uses appropriate liquidity TF from TF_LADDER
    """
    if entry_timeframe not in TF_LADDER:
        return LiquidityMap(from_liquidity=[], to_liquidity=[], has_clear_target=False)
    
    liq_tf = TF_LADDER[entry_timeframe]["liquidity"]
    
    ohlcv = await fetch_ohlcv(exchange, symbol, liq_tf, 100)
    if not ohlcv:
        return LiquidityMap(from_liquidity=[], to_liquidity=[], has_clear_target=False)
    
    df = create_dataframe(ohlcv)
    
    from_liquidity = []
    recent_df = df.iloc[-10:] if len(df) >= 10 else df
    
    for i in range(len(recent_df) - 1):
        candle = recent_df.iloc[i]
        next_candle = recent_df.iloc[i + 1] if i + 1 < len(recent_df) else candle
        
        if candle["high"] > recent_df["high"].iloc[max(0, i-5):i].max() and next_candle["close"] < candle["close"]:
            from_liquidity.append({
                "price": float(candle["high"]),
                "type": "SWEPT_HIGH",
                "timeframe": liq_tf,
                "direction": "FROM"
            })
        
        if candle["low"] < recent_df["low"].iloc[max(0, i-5):i].min() and next_candle["close"] > candle["close"]:
            from_liquidity.append({
                "price": float(candle["low"]),
                "type": "SWEPT_LOW",
                "timeframe": liq_tf,
                "direction": "FROM"
            })
    
    to_liquidity = []
    
    # LTF equal highs/lows (nearest magnets) - NOW DYNAMIC BASED ON ENTRY TF
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
    
    # Add HTF liquidity as TP3 only
    for zone in htf_context.liquidity_zones:
        to_liquidity.append({
            "price": zone["price"],
            "type": f"HTF_{zone['type']}",
            "timeframe": zone["timeframe"],
            "strength": zone.get("strength", 1),
            "direction": "TO",
            "ltf": False,
            "htf_only": True
        })
    
    has_clear_target = len(to_liquidity) > 0 and len(from_liquidity) > 0
    
    return LiquidityMap(
        from_liquidity=from_liquidity,
        to_liquidity=to_liquidity,
        has_clear_target=has_clear_target
    )

# ---------------- STEP 3: LIQUIDITY SWEEP ----------------
async def analyze_sweep(exchange, symbol: str, htf_context: HTFContext, entry_timeframe: str) -> SweepAnalysis:
    """
    FIX 1: Less strict sweep detection + DYNAMIC TF
    Accepts partial body sweeps OR follow-through confirmation
    Uses sweep TF from TF_LADDER
    """
    if entry_timeframe not in TF_LADDER:
        return SweepAnalysis(type="NONE", candle_index=-1, swept_price=0, 
                           previous_extreme=0, impulsive=False)
    
    sweep_tf = TF_LADDER[entry_timeframe]["sweep"]
    ohlcv = await fetch_ohlcv(exchange, symbol, sweep_tf, 50)
    if not ohlcv or len(ohlcv) < 10:
        return SweepAnalysis(type="NONE", candle_index=-1, swept_price=0, 
                           previous_extreme=0, impulsive=False)
    
    df = create_dataframe(ohlcv)
    
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
        
        if candle["high"] > previous_high:
            body_size = abs(candle["close"] - candle["open"])
            upper_wick = candle["high"] - max(candle["open"], candle["close"])
            lower_wick = min(candle["open"], candle["close"]) - candle["low"]
            total_wick = upper_wick + lower_wick
            
            # FIX 1: Check follow-through
            has_follow_through = False
            if i < -1:
                next_candle = df.iloc[candle_idx + 1]
                has_follow_through = (next_candle["close"] < candle["close"])
            
            # FIX 1: Accept if EITHER body > wick OR has follow-through
            impulsive = (body_size > total_wick) or has_follow_through
            
            fake_sweep = False
            if i < -1:
                next_candle = df.iloc[candle_idx + 1]
                fake_sweep = (next_candle["close"] < candle["close"] and 
                             next_candle["low"] < candle["low"])
            
            strength = 0.0
            if impulsive and not fake_sweep:
                extension = (candle["high"] - previous_high) / previous_high
                strength = min(1.0, extension * 100)
                if has_follow_through:
                    strength = min(1.0, strength + 0.2)
            
            return SweepAnalysis(
                type="HIGH_SWEep",
                candle_index=int(candle_idx),
                swept_price=float(candle["high"]),
                previous_extreme=float(previous_high),
                impulsive=impulsive,
                fake_sweep=fake_sweep,
                strength=float(strength)
            )
        
        elif candle["low"] < previous_low:
            body_size = abs(candle["close"] - candle["open"])
            upper_wick = candle["high"] - max(candle["open"], candle["close"])
            lower_wick = min(candle["open"], candle["close"]) - candle["low"]
            total_wick = upper_wick + lower_wick
            
            has_follow_through = False
            if i < -1:
                next_candle = df.iloc[candle_idx + 1]
                has_follow_through = (next_candle["close"] > candle["close"])
            
            impulsive = (body_size > total_wick) or has_follow_through
            
            fake_sweep = False
            if i < -1:
                next_candle = df.iloc[candle_idx + 1]
                fake_sweep = (next_candle["close"] > candle["close"] and 
                             next_candle["high"] > candle["high"])
            
            strength = 0.0
            if impulsive and not fake_sweep:
                extension = (previous_low - candle["low"]) / previous_low
                strength = min(1.0, extension * 100)
                if has_follow_through:
                    strength = min(1.0, strength + 0.2)
            
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

# ---------------- STEP 4: STRUCTURE CHECK ----------------
async def check_structure_shift(exchange, symbol: str, sweep: SweepAnalysis, 
                               htf_context: HTFContext, entry_timeframe: str) -> StructureShift:
    """
    FIX 2: Immediate structure detection + DYNAMIC TF
    Allows CHoCH/BOS on same candle or within 2 candles
    Uses structure TF from TF_LADDER
    """
    if sweep.type == "NONE" or entry_timeframe not in TF_LADDER:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    structure_tf = TF_LADDER[entry_timeframe]["structure"]
    ohlcv = await fetch_ohlcv(exchange, symbol, structure_tf, 50)
    if not ohlcv:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    df = create_dataframe(ohlcv)
    sweep_idx = sweep.candle_index
    
    if sweep_idx < 0 or sweep_idx >= len(df) - 3:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    sweep_candle = df.iloc[sweep_idx]
    
    if sweep.type == "HIGH_SWEEP":
        start_idx = max(0, sweep_idx - 5)
        prev_candles = df.iloc[start_idx:sweep_idx]
        
        if len(prev_candles) > 0:
            recent_low_before = prev_candles["low"].min()
            
            # FIX 2: Check CHoCH on sweep candle close
            if sweep_candle["close"] < recent_low_before:
                return StructureShift(
                    type="CHoCH",
                    confirmed=True,
                    candle_index=int(sweep_idx),
                    description="High sweep closed below recent low (immediate CHoCH)"
                )
            
            # FIX 2: Check within next 2 candles (not 3+)
            for i in range(1, min(3, len(df) - sweep_idx)):
                next_candle = df.iloc[sweep_idx + i]
                if next_candle["low"] < recent_low_before:
                    return StructureShift(
                        type="CHoCH",
                        confirmed=True,
                        candle_index=int(sweep_idx + i),
                        description=f"High sweep followed by CHoCH within {i} candle(s)"
                    )
        
        # Check for BOS (could be on same candle)
        if sweep_idx > 0:
            candle_before = df.iloc[sweep_idx - 1]
            if sweep_candle["close"] > sweep_candle["open"]:
                return StructureShift(
                    type="BOS",
                    confirmed=True,
                    candle_index=int(sweep_idx),
                    description="Bullish sweep candle (immediate BOS)"
                )
    
    elif sweep.type == "LOW_SWEEP":
        start_idx = max(0, sweep_idx - 5)
        prev_candles = df.iloc[start_idx:sweep_idx]
        
        if len(prev_candles) > 0:
            recent_high_before = prev_candles["high"].max()
            
            if sweep_candle["close"] > recent_high_before:
                return StructureShift(
                    type="CHoCH",
                    confirmed=True,
                    candle_index=int(sweep_idx),
                    description="Low sweep closed above recent high (immediate CHoCH)"
                )
            
            for i in range(1, min(3, len(df) - sweep_idx)):
                next_candle = df.iloc[sweep_idx + i]
                if next_candle["high"] > recent_high_before:
                    return StructureShift(
                        type="CHoCH",
                        confirmed=True,
                        candle_index=int(sweep_idx + i),
                        description=f"Low sweep followed by CHoCH within {i} candle(s)"
                    )
        
        if sweep_idx > 0:
            candle_before = df.iloc[sweep_idx - 1]
            if sweep_candle["close"] < sweep_candle["open"]:
                return StructureShift(
                    type="BOS",
                    confirmed=True,
                    candle_index=int(sweep_idx),
                    description="Bearish sweep candle (immediate BOS)"
                )
    
    return StructureShift(type="NONE", confirmed=False, candle_index=-1)

# ---------------- STEP 5: ENTRY ZONE ----------------
async def find_entry_zone(exchange, symbol: str, htf_context: HTFContext,
                         sweep: SweepAnalysis, structure_shift: StructureShift,
                         side: str, entry_timeframe: str) -> EntryZone:
    """
    DYNAMIC: Uses entry timeframe (not hardcoded 5m)
    """
    if entry_timeframe not in TF_LADDER:
        return EntryZone(type="NONE", price=0, low=0, high=0, aligns_with_htf=False)
    
    ohlcv = await fetch_ohlcv(exchange, symbol, entry_timeframe, 100)
    if not ohlcv:
        return EntryZone(type="NONE", price=0, low=0, high=0, aligns_with_htf=False)
    
    df = create_dataframe(ohlcv)
    current_price = float(df["close"].iloc[-1])
    
    # FIX 3: STRICT ALIGNMENT - No OR conditions
    if side == "BUY":
        aligns_with_htf = (htf_context.bias == "BULLISH" and 
                          htf_context.premium_discount == "DISCOUNT")
    else:  # SELL
        aligns_with_htf = (htf_context.bias == "BEARISH" and 
                          htf_context.premium_discount == "PREMIUM")
    
    if structure_shift.type == "CHoCH":
        entry_type = "ORDER_BLOCK"
    elif structure_shift.type == "BOS":
        entry_type = "FAIR_VALUE_GAP"
    else:
        entry_type = "NONE"
    
    if entry_type == "ORDER_BLOCK":
        for i in range(2, len(df) - 1):
            candle = df.iloc[i]
            next_candle = df.iloc[i + 1]
            
            if side == "BUY":
                if (candle["close"] < candle["open"] and 
                    next_candle["close"] > next_candle["open"]):
                    ob_low = min(candle["low"], next_candle["low"])
                    ob_high = next_candle["close"]
                    
                    if current_price <= ob_high and current_price >= ob_low * 0.995:
                        current_candle = df.iloc[-1]
                        prev_candle = df.iloc[-2] if len(df) >= 2 else current_candle
                        
                        reaction = (current_candle["close"] > current_candle["open"] or
                                   (prev_candle["close"] > prev_candle["open"] and
                                    current_candle["close"] > prev_candle["close"]))
                        
                        return EntryZone(
                            type="ORDER_BLOCK",
                            price=float((ob_low + ob_high) / 2),
                            low=float(ob_low),
                            high=float(ob_high),
                            aligns_with_htf=aligns_with_htf,
                            candle_reaction=reaction
                        )
            
            elif side == "SELL":
                if (candle["close"] > candle["open"] and 
                    next_candle["close"] < next_candle["open"]):
                    ob_low = next_candle["close"]
                    ob_high = max(candle["high"], next_candle["high"])
                    
                    if current_price >= ob_low and current_price <= ob_high * 1.005:
                        current_candle = df.iloc[-1]
                        prev_candle = df.iloc[-2] if len(df) >= 2 else current_candle
                        
                        reaction = (current_candle["close"] < current_candle["open"] or
                                   (prev_candle["close"] < prev_candle["open"] and
                                    current_candle["close"] < prev_candle["close"]))
                        
                        return EntryZone(
                            type="ORDER_BLOCK",
                            price=float((ob_low + ob_high) / 2),
                            low=float(ob_low),
                            high=float(ob_high),
                            aligns_with_htf=aligns_with_htf,
                            candle_reaction=reaction
                        )
    
    return EntryZone(type="NONE", price=0, low=0, high=0, aligns_with_htf=False)

# ---------------- STEP 6: RISK/SL ----------------
def calculate_risk_sl(entry_zone: EntryZone, sweep: SweepAnalysis,
                     htf_context: HTFContext, side: str) -> RiskManagement:
    entry_price = entry_zone.price
    sl_price = 0.0
    invalidation_type = ""
    
    if sweep.type != "NONE" and sweep.swept_price > 0:
        if side == "BUY" and sweep.type == "LOW_SWEEP":
            sl_price = sweep.swept_price * 0.995
            invalidation_type = "SWEEP"
        elif side == "SELL" and sweep.type == "HIGH_SWEEP":
            sl_price = sweep.swept_price * 1.005
            invalidation_type = "SWEEP"
    
    if invalidation_type == "" and entry_zone.type == "ORDER_BLOCK":
        if side == "BUY":
            sl_price = entry_zone.low * 0.995
            invalidation_type = "ORDER_BLOCK"
        elif side == "SELL":
            sl_price = entry_zone.high * 1.005
            invalidation_type = "ORDER_BLOCK"
    
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

# ---------------- STEP 7: TAKE PROFIT ----------------
def calculate_take_profits(entry_price: float, side: str, 
                          liquidity_map: LiquidityMap,
                          htf_context: HTFContext) -> TakeProfitLevels:
    """
    FIX 4: LTF liquidity first (nearest magnet)
    """
    ltf_targets = [t for t in liquidity_map.to_liquidity if t.get("ltf", False)]
    htf_targets = [t for t in liquidity_map.to_liquidity if t.get("htf_only", False)]
    
    if side == "BUY":
        potential_ltf = [t for t in ltf_targets if t["price"] > entry_price]
        potential_htf = [t for t in htf_targets if t["price"] > entry_price]
        range_boundary = htf_context.range_high
    else:
        potential_ltf = [t for t in ltf_targets if t["price"] < entry_price]
        potential_htf = [t for t in htf_targets if t["price"] < entry_price]
        range_boundary = htf_context.range_low
    
    # FIX 4: TP1 = Nearest LTF target
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
    
    if len(potential_ltf) >= 2:
        tp2 = potential_ltf[1]["price"]
        tp2_type = potential_ltf[1]["type"]
    else:
        tp2 = range_boundary
        tp2_type = "RANGE_BOUNDARY"
    
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

# ---------------- STEP 8: PROBABILITY CHECK ----------------
def calculate_probability(htf_context: HTFContext, liquidity_map: LiquidityMap,
                         sweep: SweepAnalysis, structure_shift: StructureShift,
                         entry_zone: EntryZone, side: str) -> ProbabilityScore:
    # 1. HTF Alignment
    if side == "BUY":
        htf_alignment = 1.0 if (htf_context.bias == "BULLISH" and 
                               htf_context.premium_discount == "DISCOUNT") else 0.2
    else:
        htf_alignment = 1.0 if (htf_context.bias == "BEARISH" and 
                               htf_context.premium_discount == "PREMIUM") else 0.2
    
    # 2. Liquidity Quality
    if liquidity_map.has_clear_target:
        if len(liquidity_map.to_liquidity) >= 2:
            avg_strength = sum(t.get("strength", 0) for t in liquidity_map.to_liquidity) / len(liquidity_map.to_liquidity)
            liquidity_quality = min(1.0, avg_strength / 3.0 * 1.5)
        else:
            liquidity_quality = 0.5
    else:
        liquidity_quality = 0.1
    
    # 3. Sweep Strength
    sweep_strength = sweep.strength
    if sweep.impulsive:
        sweep_strength = min(1.0, sweep_strength + 0.2)
    if sweep.fake_sweep:
        sweep_strength = max(0.3, sweep_strength - 0.3)
    
    # 4. Structure Clarity
    if structure_shift.confirmed:
        if structure_shift.type == "CHoCH":
            structure_clarity = 0.9
        elif structure_shift.type == "BOS":
            structure_clarity = 0.8
        else:
            structure_clarity = 0.6
        
        if structure_shift.candle_index == sweep.candle_index:
            structure_clarity = min(1.0, structure_clarity + 0.1)
    else:
        structure_clarity = 0.1
    
    # 5. Entry Precision
    if entry_zone.type in ["ORDER_BLOCK", "FAIR_VALUE_GAP"]:
        entry_precision = 0.7
        if entry_zone.aligns_with_htf:
            entry_precision = min(1.0, entry_precision + 0.2)
        if entry_zone.candle_reaction:
            entry_precision = min(1.0, entry_precision + 0.1)
    elif entry_zone.type in ["PREMIUM", "DISCOUNT"]:
        entry_precision = 0.5
        if entry_zone.candle_reaction:
            entry_precision = 0.6
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

# ---------------- MAIN SCANNING LOGIC ----------------
async def scan_symbol(exchange, symbol: str, entry_timeframe: str = "15m") -> Optional[Dict]:
    """
    TF-AGNOSTIC scanner - uses TF_LADDER for dynamic timeframe scaling
    """
    
    if entry_timeframe not in TF_LADDER:
        log.debug(f"Unsupported timeframe: {entry_timeframe}")
        return None
    
    ticker = await exchange.fetch_ticker(symbol)
    current_price = ticker.get("last", 0)
    if not current_price:
        return None
    
    log.debug(f"🔍 Scanning {symbol} on {entry_timeframe} at {current_price}")
    
    # Step 1: HTF Bias (dynamic from TF_LADDER)
    htf_context = await analyze_htf_bias(exchange, symbol, entry_timeframe)
    if not htf_context.valid:
        return None
    
    # Step 2: Liquidity Map (dynamic from TF_LADDER)
    liquidity_map = await map_liquidity(exchange, symbol, htf_context, current_price, entry_timeframe)
    if not liquidity_map.has_clear_target:
        return None
    
    # Step 3: Sweep (dynamic from TF_LADDER)
    sweep = await analyze_sweep(exchange, symbol, htf_context, entry_timeframe)
    if sweep.type == "NONE":
        return None
    
    side = "SELL" if sweep.type == "HIGH_SWEEP" else "BUY"
    
    # Step 4: Structure (dynamic from TF_LADDER)
    structure_shift = await check_structure_shift(exchange, symbol, sweep, htf_context, entry_timeframe)
    if not structure_shift.confirmed:
        return None
    
    # Step 5: Entry Zone (uses entry timeframe)
    entry_zone = await find_entry_zone(exchange, symbol, htf_context, sweep, structure_shift, side, entry_timeframe)
    if entry_zone.type == "NONE":
        return None
    
    # Double-check strict alignment
    if side == "BUY":
        if not (htf_context.bias == "BULLISH" and htf_context.premium_discount == "DISCOUNT"):
            return None
    else:
        if not (htf_context.bias == "BEARISH" and htf_context.premium_discount == "PREMIUM"):
            return None
    
    # Step 6: Risk/SL
    risk_sl = calculate_risk_sl(entry_zone, sweep, htf_context, side)
    
    # Step 7: Take Profit
    tp_levels = calculate_take_profits(entry_zone.price, side, liquidity_map, htf_context)
    
    # Step 8: Probability
    probability = calculate_probability(
        htf_context, liquidity_map, sweep, structure_shift, entry_zone, side
    )
    
    if not probability.acceptable:
        return None
    
    log.info(f"✅ {symbol} ({entry_timeframe}): Setup! Score: {probability.total_score:.2f}/5")
    
    setup = {
        "symbol": symbol,
        "timeframe": entry_timeframe,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "side": side,
        "current_price": current_price,
        "htf_bias": htf_context.bias,
        "htf_range_high": htf_context.range_high,
        "htf_range_low": htf_context.range_low,
        "htf_premium_discount": htf_context.premium_discount,
        "htf_liquidity_zones": htf_context.liquidity_zones,
        "htf_structure": htf_context.structure,
        "liquidity_from": liquidity_map.from_liquidity,
        "liquidity_to": liquidity_map.to_liquidity,
        "has_clear_target": liquidity_map.has_clear_target,
        "sweep_type": sweep.type,
        "swept_price": sweep.swept_price,
        "sweep_impulsive": sweep.impulsive,
        "sweep_strength": sweep.strength,
        "structure_shift_type": structure_shift.type,
        "structure_shift_confirmed": structure_shift.confirmed,
        "structure_description": structure_shift.description,
        "entry_type": entry_zone.type,
        "entry_price": entry_zone.price,
        "entry_low": entry_zone.low,
        "entry_high": entry_zone.high,
        "entry_aligns_htf": entry_zone.aligns_with_htf,
        "entry_reaction_confirmed": entry_zone.candle_reaction,
        "sl_price": risk_sl.sl_price,
        "sl_invalidation_type": risk_sl.invalidation_type,
        "risk_amount": risk_sl.risk_amount,
        "sl_distance_pct": risk_sl.sl_to_entry_distance,
        "tp1_price": tp_levels.tp1,
        "tp1_type": tp_levels.tp1_type,
        "tp2_price": tp_levels.tp2,
        "tp2_type": tp_levels.tp2_type,
        "tp3_price": tp_levels.tp3,
        "tp3_type": tp_levels.tp3_type,
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

# ---------------- SCANNER MAIN ----------------
async def scanner_main(exchange, entry_timeframe: str = "15m"):
    """
    Main scanner - works on specified timeframe using TF_LADDER
    """
    
    await send_telegram(f"🚀 ROMEOTPT Scanner Started ({entry_timeframe})")
    await send_telegram(f"📊 Using TF_LADDER: {TF_LADDER[entry_timeframe]}")
    await send_telegram("✅ FIXES: 1) Less strict sweeps 2) Immediate structure 3) Strict alignment 4) LTF TP first 5) Strategic probability")
    
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
                log.info(f"✅ Found {setups_found} setups on {entry_timeframe}")
            else:
                log.info(f"⏳ No setups found on {entry_timeframe}")
            
        except Exception as e:
            log.exception(f"Scanner error: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- ALERT FORMATTING ----------------
async def send_setup_alert(setup: Dict):
    entry = setup["entry_price"]
    sl = setup["sl_price"]
    tp1 = setup["tp1_price"]
    
    risk = abs(entry - sl)
    reward_tp1 = abs(tp1 - entry)
    rr_ratio = reward_tp1 / risk if risk > 0 else 0
    
    msg = f"""
🔥 <b>ROMEOTPT A+ SETUP CONFIRMED</b>

<b>Symbol:</b> {setup['symbol']} ({setup['timeframe']})
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
                :current_price, 'DETECTED', ''
            )
        """, {
            "symbol": setup["symbol"],
            "timestamp": setup["timestamp"],
            "side": setup["side"],
            "entry_timeframe": setup["timeframe"],
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
    return {"status": "healthy", "scanner": "ROMEOTPT v2 Fixed"}

@app.get("/setups")
async def get_setups(limit: int = 20, min_score: float = 3.3):
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
    await init_db()
    
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"}
    })
    
    # Run scanner on specified timeframe (can be changed to "1m", "3m", "5m", "15m", "30m")
    await scanner_main(exchange, entry_timeframe="15m")

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
            log.info("Shutting down ROMEOTPT scanner...")
        finally:
            if db_conn:
                asyncio.run(db_conn.close())