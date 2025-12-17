#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT SCANNER v2.1 - Multi-Timeframe Pairs + TP/SL Notifications
Exact 8-step logic applied to multiple timeframe pairs
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
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass
from collections import defaultdict

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/romeopt_v2_1.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 15))
TOP_N = int(os.getenv("TOP_N", 60))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 5))

# ---------------- TIME FRAME PAIRS (Analysis → Entry) ----------------
TIMEFRAME_PAIRS = [
    {"analysis_tf": "15m", "entry_tf": "5m", "name": "15m→5m"},
    {"analysis_tf": "30m", "entry_tf": "15m", "name": "30m→15m"},
    {"analysis_tf": "1h", "entry_tf": "30m", "name": "1h→30m"},
    {"analysis_tf": "4h", "entry_tf": "1h", "name": "4h→1h"},
]

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_v2_1")
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
    liquidity_zones: List[Dict]
    structure: List[Dict]
    skip_reason: Optional[str] = None
    valid: bool = False

@dataclass
class LiquidityMap:
    """Step 2: Liquidity Map output"""
    from_liquidity: List[Dict]
    to_liquidity: List[Dict]
    has_clear_target: bool = False

@dataclass
class SweepAnalysis:
    """Step 3: Liquidity Sweep output"""
    type: str  # "HIGH_SWEEP", "LOW_SWEEP", "NONE"
    candle_index: int
    swept_price: float
    previous_extreme: float
    impulsive: bool
    fake_sweep: bool = False
    strength: float = 0.0

@dataclass
class StructureShift:
    """Step 4: Structure Check output"""
    type: str  # "CHoCH", "BOS", "NONE"
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
    invalidation_type: str
    risk_amount: float
    sl_to_entry_distance: float

@dataclass
class TakeProfitLevels:
    """Step 7: Take Profit output"""
    tp1: float
    tp2: float
    tp3: float
    tp1_type: str = "INTERNAL_LIQUIDITY"
    tp2_type: str = "RANGE_BOUNDARY"
    tp3_type: str = "HTF_LIQUIDITY"

@dataclass
class ProbabilityScore:
    """Step 8: Probability Check output"""
    htf_alignment: float
    liquidity_quality: float
    sweep_strength: float
    structure_clarity: float
    entry_precision: float
    total_score: float
    
    @property
    def acceptable(self) -> bool:
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
    
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            timestamp TEXT,
            side TEXT,
            timeframe_pair TEXT,
            analysis_tf TEXT,
            entry_tf TEXT,
            
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
            tp_hit_time TEXT,
            sl_hit_time TEXT,
            notes TEXT
        )
    """)
    
    await db_conn.commit()

# ---------------- UTILS ----------------
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
    """Convert numpy/pandas types to Python native types"""
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

def get_tf_ms(tf: str) -> int:
    """Convert timeframe string to milliseconds"""
    tf_map = {
        "1m": 60000,
        "3m": 180000,
        "5m": 300000,
        "15m": 900000,
        "30m": 1800000,
        "1h": 3600000,
        "4h": 14400000,
    }
    return tf_map.get(tf, 60000)

# ---------------- STEP 1: HTF BIAS (FOR ANALYSIS TF) ----------------
async def analyze_htf_bias(exchange, symbol: str, analysis_tf: str) -> HTFContext:
    """Step 1: HTF Bias for the analysis timeframe"""
    
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, analysis_tf, 100)
    
    if not ohlcv_htf or len(ohlcv_htf) < 30:
        return HTFContext(
            bias="UNKNOWN", range_high=0, range_low=0, range_mid=0,
            premium_discount="UNKNOWN", liquidity_zones=[], structure=[],
            skip_reason=f"Insufficient {analysis_tf} data", valid=False
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
        "timeframe": analysis_tf,
        "strength": 3
    })
    liquidity_zones.append({
        "price": range_low,
        "type": "RANGE_LOW",
        "timeframe": analysis_tf,
        "strength": 3
    })
    
    for swing in swing_highs[-3:]:
        liquidity_zones.append({
            "price": swing["price"],
            "type": "SWING_HIGH",
            "timeframe": analysis_tf,
            "strength": 2
        })
    
    for swing in swing_lows[-3:]:
        liquidity_zones.append({
            "price": swing["price"],
            "type": "SWING_LOW",
            "timeframe": analysis_tf,
            "strength": 2
        })
    
    skip_reason = None
    valid = True
    
    if premium_discount == "MIDDLE" and bias == "RANGING":
        skip_reason = "Price mid-range with no clear alignment"
        valid = False
    elif range_height / range_low < 0.02:
        skip_reason = "Range too tight (<2%)"
        valid = False
    
    return HTFContext(
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

# ---------------- STEP 2: LIQUIDITY MAP ----------------
async def map_liquidity(exchange, symbol: str, htf_context: HTFContext, 
                       current_price: float, entry_tf: str) -> LiquidityMap:
    """Step 2: Liquidity Map using entry timeframe"""
    
    ohlcv_entry = await fetch_ohlcv(exchange, symbol, entry_tf, 100)
    if not ohlcv_entry:
        return LiquidityMap(from_liquidity=[], to_liquidity=[], has_clear_target=False)
    
    df_entry = create_dataframe(ohlcv_entry)
    
    from_liquidity = []
    recent_df = df_entry.iloc[-10:] if len(df_entry) >= 10 else df_entry
    
    for i in range(len(recent_df) - 1):
        candle = recent_df.iloc[i]
        next_candle = recent_df.iloc[i + 1] if i + 1 < len(recent_df) else candle
        
        if candle["high"] > recent_df["high"].iloc[max(0, i-5):i].max() and next_candle["close"] < candle["close"]:
            from_liquidity.append({
                "price": float(candle["high"]),
                "type": "SWEPT_HIGH",
                "timeframe": entry_tf,
                "direction": "FROM"
            })
        
        if candle["low"] < recent_df["low"].iloc[max(0, i-5):i].min() and next_candle["close"] > candle["close"]:
            from_liquidity.append({
                "price": float(candle["low"]),
                "type": "SWEPT_LOW",
                "timeframe": entry_tf,
                "direction": "FROM"
            })
    
    to_liquidity = []
    sorted_zones = sorted(htf_context.liquidity_zones, 
                         key=lambda z: abs(z["price"] - current_price))
    
    if htf_context.bias == "BULLISH":
        targets = [z for z in sorted_zones if z["price"] > current_price]
    elif htf_context.bias == "BEARISH":
        targets = [z for z in sorted_zones if z["price"] < current_price]
    else:
        targets = [z for z in sorted_zones if z["type"] in ["RANGE_HIGH", "RANGE_LOW"]]
    
    for target in targets[:3]:
        to_liquidity.append({
            "price": target["price"],
            "type": target["type"],
            "timeframe": target["timeframe"],
            "strength": int(target.get("strength", 1)),
            "direction": "TO"
        })
    
    if len(df_entry) >= 24:
        high_values = df_entry["high"].iloc[-24:].values
        for val in np.unique(np.round(high_values, 4)):
            count = int(np.sum(np.round(high_values, 4) == val))
            if count >= 2:
                to_liquidity.append({
                    "price": float(val),
                    "type": "EQUAL_HIGH",
                    "timeframe": entry_tf,
                    "strength": int(min(2, count)),
                    "direction": "TO"
                })
        
        low_values = df_entry["low"].iloc[-24:].values
        for val in np.unique(np.round(low_values, 4)):
            count = int(np.sum(np.round(low_values, 4) == val))
            if count >= 2:
                to_liquidity.append({
                    "price": float(val),
                    "type": "EQUAL_LOW",
                    "timeframe": entry_tf,
                    "strength": int(min(2, count)),
                    "direction": "TO"
                })
    
    has_clear_target = len(to_liquidity) > 0 and len(from_liquidity) > 0
    
    return LiquidityMap(
        from_liquidity=from_liquidity,
        to_liquidity=to_liquidity,
        has_clear_target=has_clear_target
    )

# ---------------- STEP 3: LIQUIDITY SWEEP ----------------
async def analyze_sweep(exchange, symbol: str, analysis_tf: str, entry_tf: str) -> SweepAnalysis:
    """Step 3: Sweep on analysis TF, impulsive check on entry TF"""
    
    ohlcv_analysis = await fetch_ohlcv(exchange, symbol, analysis_tf, 50)
    if not ohlcv_analysis or len(ohlcv_analysis) < 10:
        return SweepAnalysis(type="NONE", candle_index=-1, swept_price=0, 
                           previous_extreme=0, impulsive=False)
    
    df_analysis = create_dataframe(ohlcv_analysis)
    lookback = min(5, len(df_analysis))
    
    for i in range(-lookback, 0):
        candle_idx = len(df_analysis) + i
        candle = df_analysis.iloc[candle_idx]
        
        start_idx = max(0, candle_idx - 5)
        prev_candles = df_analysis.iloc[start_idx:candle_idx]
        
        if len(prev_candles) == 0:
            continue
        
        previous_high = prev_candles["high"].max()
        previous_low = prev_candles["low"].min()
        
        if candle["high"] > previous_high:
            impulsive = await check_impulsive_on_entry_tf(exchange, symbol, entry_tf, 
                                                         int(candle["timestamp"]), "HIGH")
            
            fake_sweep = False
            if i < -1:
                next_candle = df_analysis.iloc[candle_idx + 1]
                fake_sweep = (next_candle["close"] < candle["close"] and 
                             next_candle["low"] < candle["low"])
            
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
        
        elif candle["low"] < previous_low:
            impulsive = await check_impulsive_on_entry_tf(exchange, symbol, entry_tf,
                                                         int(candle["timestamp"]), "LOW")
            
            fake_sweep = False
            if i < -1:
                next_candle = df_analysis.iloc[candle_idx + 1]
                fake_sweep = (next_candle["close"] > candle["close"] and 
                             next_candle["high"] > candle["high"])
            
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

async def check_impulsive_on_entry_tf(exchange, symbol: str, entry_tf: str, 
                                     sweep_timestamp: int, sweep_type: str) -> bool:
    """Check if sweep was impulsive on entry timeframe"""
    ohlcv_entry = await fetch_ohlcv(exchange, symbol, entry_tf, 20)
    if not ohlcv_entry:
        return False
    
    df_entry = create_dataframe(ohlcv_entry)
    
    for i in range(len(df_entry)):
        candle = df_entry.iloc[i]
        candle_end_time = candle["timestamp"] + get_tf_ms(entry_tf)
        
        if candle["timestamp"] <= sweep_timestamp <= candle_end_time:
            body_size = abs(candle["close"] - candle["open"])
            upper_wick = candle["high"] - max(candle["open"], candle["close"])
            lower_wick = min(candle["open"], candle["close"]) - candle["low"]
            total_wick = upper_wick + lower_wick
            
            return bool(body_size > total_wick)
    
    return False

# ---------------- STEP 4: STRUCTURE CHECK ----------------
async def check_structure_shift(exchange, symbol: str, sweep: SweepAnalysis, 
                               htf_context: HTFContext) -> StructureShift:
    """Step 4: Structure Check on analysis timeframe"""
    
    if sweep.type == "NONE":
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    analysis_tf = htf_context.liquidity_zones[0]["timeframe"] if htf_context.liquidity_zones else "15m"
    ohlcv_analysis = await fetch_ohlcv(exchange, symbol, analysis_tf, 50)
    if not ohlcv_analysis:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    df_analysis = create_dataframe(ohlcv_analysis)
    sweep_idx = sweep.candle_index
    
    if sweep_idx < 0 or sweep_idx >= len(df_analysis) - 3:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    post_sweep_candles = df_analysis.iloc[sweep_idx + 1:]
    if len(post_sweep_candles) < 3:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    if sweep.type == "HIGH_SWEEP":
        recent_low_before = df_analysis["low"].iloc[max(0, sweep_idx-5):sweep_idx].min()
        
        for i in range(len(post_sweep_candles)):
            candle = post_sweep_candles.iloc[i]
            if candle["low"] < recent_low_before:
                return StructureShift(
                    type="CHoCH",
                    confirmed=True,
                    candle_index=int(sweep_idx + i + 1),
                    description="High sweep followed by break below recent low"
                )
        
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
        recent_high_before = df_analysis["high"].iloc[max(0, sweep_idx-5):sweep_idx].max()
        
        for i in range(len(post_sweep_candles)):
            candle = post_sweep_candles.iloc[i]
            if candle["high"] > recent_high_before:
                return StructureShift(
                    type="CHoCH",
                    confirmed=True,
                    candle_index=int(sweep_idx + i + 1),
                    description="Low sweep followed by break above recent high"
                )
        
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

# ---------------- STEP 5: ENTRY ZONE ----------------
async def find_entry_zone(exchange, symbol: str, htf_context: HTFContext,
                         sweep: SweepAnalysis, structure_shift: StructureShift,
                         side: str, entry_tf: str) -> EntryZone:
    """Step 5: Entry Zone on entry timeframe"""
    
    ohlcv_entry = await fetch_ohlcv(exchange, symbol, entry_tf, 100)
    if not ohlcv_entry:
        return EntryZone(type="NONE", price=0, low=0, high=0, aligns_with_htf=False)
    
    df_entry = create_dataframe(ohlcv_entry)
    current_price = float(df_entry["close"].iloc[-1])
    
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
    
    if entry_type == "ORDER_BLOCK":
        for i in range(2, len(df_entry) - 1):
            candle = df_entry.iloc[i]
            next_candle = df_entry.iloc[i + 1]
            
            if side == "BUY":
                if (candle["close"] < candle["open"] and 
                    next_candle["close"] > next_candle["open"]):
                    ob_low = min(candle["low"], next_candle["low"])
                    ob_high = next_candle["close"]
                    
                    aligns = (htf_context.bias == "BULLISH" or 
                             htf_context.premium_discount == "DISCOUNT")
                    
                    if current_price <= ob_high and current_price >= ob_low * 0.995:
                        current_candle = df_entry.iloc[-1]
                        prev_candle = df_entry.iloc[-2] if len(df_entry) >= 2 else current_candle
                        
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
            
            elif side == "SELL":
                if (candle["close"] > candle["open"] and 
                    next_candle["close"] < next_candle["open"]):
                    ob_low = next_candle["close"]
                    ob_high = max(candle["high"], next_candle["high"])
                    
                    aligns = (htf_context.bias == "BEARISH" or 
                             htf_context.premium_discount == "PREMIUM")
                    
                    if current_price >= ob_low and current_price <= ob_high * 1.005:
                        current_candle = df_entry.iloc[-1]
                        prev_candle = df_entry.iloc[-2] if len(df_entry) >= 2 else current_candle
                        
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
    
    elif entry_type == "FAIR_VALUE_GAP":
        for i in range(1, len(df_entry) - 2):
            candle1 = df_entry.iloc[i]
            candle2 = df_entry.iloc[i + 1]
            candle3 = df_entry.iloc[i + 2] if i + 2 < len(df_entry) else candle2
            
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
    
    elif entry_type in ["PREMIUM", "DISCOUNT"]:
        zone_price = htf_context.range_mid
        zone_width = (htf_context.range_high - htf_context.range_low) * 0.1
        
        aligns = True
        
        if (side == "BUY" and entry_type == "DISCOUNT" and
            current_price <= htf_context.range_mid * 1.02):
            reaction = df_entry["close"].iloc[-1] > df_entry["open"].iloc[-1]
            
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
            reaction = df_entry["close"].iloc[-1] < df_entry["open"].iloc[-1]
            
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
    """Step 6: Risk/SL"""
    
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
    """Step 7: Take Profit"""
    
    if side == "BUY":
        potential_targets = [t for t in liquidity_map.to_liquidity 
                           if t["price"] > entry_price]
        range_boundary = htf_context.range_high
        htf_targets = [z for z in htf_context.liquidity_zones 
                      if z["price"] > entry_price and z["type"] != "RANGE_HIGH"]
    else:
        potential_targets = [t for t in liquidity_map.to_liquidity 
                           if t["price"] < entry_price]
        range_boundary = htf_context.range_low
        htf_targets = [z for z in htf_context.liquidity_zones 
                      if z["price"] < entry_price and z["type"] != "RANGE_LOW"]
    
    tp1_candidates = [t for t in potential_targets if t["timeframe"] in ["5m", "15m", "30m", "1h"]]
    if tp1_candidates:
        tp1_candidates.sort(key=lambda t: abs(t["price"] - entry_price))
        tp1 = tp1_candidates[0]["price"]
        tp1_type = tp1_candidates[0]["type"]
    else:
        if side == "BUY":
            tp1 = entry_price * 1.02
        else:
            tp1 = entry_price * 0.98
        tp1_type = "RISK_REWARD_1_1"
    
    tp2 = range_boundary
    tp2_type = "RANGE_BOUNDARY"
    
    if htf_targets:
        htf_targets.sort(key=lambda z: z.get("strength", 0), reverse=True)
        tp3 = htf_targets[0]["price"]
        tp3_type = htf_targets[0]["type"]
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
    """Step 8: Probability Check"""
    
    if htf_context.bias == side.upper() or htf_context.bias == "RANGING":
        htf_alignment = 1.0
    elif (htf_context.bias == "BULLISH" and side == "SELL") or \
         (htf_context.bias == "BEARISH" and side == "BUY"):
        htf_alignment = 0.3
    else:
        htf_alignment = 0.5
    
    if (side == "BUY" and htf_context.premium_discount == "DISCOUNT") or \
       (side == "SELL" and htf_context.premium_discount == "PREMIUM"):
        htf_alignment = min(1.0, htf_alignment + 0.2)
    
    if liquidity_map.has_clear_target:
        quality_targets = sum(1 for t in liquidity_map.to_liquidity 
                            if t.get("strength", 0) >= 2)
        liquidity_quality = min(1.0, quality_targets / 3.0)
    else:
        liquidity_quality = 0.2
    
    sweep_strength = sweep.strength
    if sweep.impulsive:
        sweep_strength = min(1.0, sweep_strength + 0.3)
    if sweep.fake_sweep:
        sweep_strength = max(0.0, sweep_strength - 0.5)
    
    if structure_shift.confirmed:
        if structure_shift.type == "CHoCH":
            structure_clarity = 0.9
        elif structure_shift.type == "BOS":
            structure_clarity = 0.8
        else:
            structure_clarity = 0.6
    else:
        structure_clarity = 0.2
    
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

# ---------------- TP/SL MONITORING ----------------
async def monitor_tp_sl():
    """Monitor active trades for TP/SL hits"""
    while True:
        try:
            async with db_lock:
                async with db_conn.execute(
                    """SELECT id, symbol, side, entry_price, sl_price, 
                       tp1_price, tp2_price, tp3_price, status 
                       FROM signals WHERE status IN ('DETECTED', 'ACTIVE', 'TP1_HIT', 'TP2_HIT')"""
                ) as cursor:
                    active_trades = await cursor.fetchall()
            
            for trade in active_trades:
                trade_id, symbol, side, entry, sl, tp1, tp2, tp3, status = trade
                
                try:
                    # Create new exchange instance for thread safety
                    exchange = ccxt.okx({"enableRateLimit": True, "options": {"defaultType": "spot"}})
                    ticker = await exchange.fetch_ticker(symbol)
                    current_price = ticker.get("last", 0)
                    await exchange.close()
                    
                    if not current_price:
                        continue
                    
                    new_status = status
                    hit_time = datetime.datetime.utcnow().isoformat()
                    
                    if side == "BUY":
                        if current_price <= sl and status not in ["SL_HIT", "TP3_HIT"]:
                            new_status = "SL_HIT"
                            await send_tp_sl_notification(trade_id, symbol, "SL", current_price, entry, hit_time)
                        elif current_price >= tp3 and status != "TP3_HIT":
                            new_status = "TP3_HIT"
                            await send_tp_sl_notification(trade_id, symbol, "TP3", current_price, entry, hit_time)
                        elif current_price >= tp2 and status not in ["TP2_HIT", "TP3_HIT", "SL_HIT"]:
                            new_status = "TP2_HIT"
                            await send_tp_sl_notification(trade_id, symbol, "TP2", current_price, entry, hit_time)
                        elif current_price >= tp1 and status not in ["TP1_HIT", "TP2_HIT", "TP3_HIT", "SL_HIT"]:
                            new_status = "TP1_HIT"
                            await send_tp_sl_notification(trade_id, symbol, "TP1", current_price, entry, hit_time)
                    
                    elif side == "SELL":
                        if current_price >= sl and status not in ["SL_HIT", "TP3_HIT"]:
                            new_status = "SL_HIT"
                            await send_tp_sl_notification(trade_id, symbol, "SL", current_price, entry, hit_time)
                        elif current_price <= tp3 and status != "TP3_HIT":
                            new_status = "TP3_HIT"
                            await send_tp_sl_notification(trade_id, symbol, "TP3", current_price, entry, hit_time)
                        elif current_price <= tp2 and status not in ["TP2_HIT", "TP3_HIT", "SL_HIT"]:
                            new_status = "TP2_HIT"
                            await send_tp_sl_notification(trade_id, symbol, "TP2", current_price, entry, hit_time)
                        elif current_price <= tp1 and status not in ["TP1_HIT", "TP2_HIT", "TP3_HIT", "SL_HIT"]:
                            new_status = "TP1_HIT"
                            await send_tp_sl_notification(trade_id, symbol, "TP1", current_price, entry, hit_time)
                    
                    if new_status != status:
                        if "HIT" in new_status:
                            field = "tp_hit_time" if "TP" in new_status else "sl_hit_time"
                            async with db_lock:
                                await db_conn.execute(
                                    f"UPDATE signals SET status = ?, {field} = ? WHERE id = ?",
                                    (new_status, hit_time, trade_id)
                                )
                                await db_conn.commit()
                        else:
                            async with db_lock:
                                await db_conn.execute(
                                    "UPDATE signals SET status = ? WHERE id = ?",
                                    (new_status, trade_id)
                                )
                                await db_conn.commit()
                            
                except Exception as e:
                    log.error(f"Error monitoring {symbol}: {e}")
                    continue
            
            await asyncio.sleep(10)
            
        except Exception as e:
            log.error(f"TP/SL monitor error: {e}")
            await asyncio.sleep(30)

async def send_tp_sl_notification(trade_id: int, symbol: str, hit_type: str, 
                                 current_price: float, entry_price: float, hit_time: str):
    """Send TP/SL hit notification"""
    
    pnl_pct = ((current_price - entry_price) / entry_price * 100)
    if "TP" in hit_type:
        emoji = "✅"
        action = f"TAKE PROFIT {hit_type.replace('TP', '')} HIT"
        color = "🟢"
    else:
        emoji = "❌"
        action = "STOP LOSS HIT"
        color = "🔴"
        pnl_pct = -abs(pnl_pct)
    
    msg = f"""
{emoji} <b>{color} TRADE UPDATE: {action}</b>

<b>Signal ID:</b> #{trade_id}
<b>Symbol:</b> {symbol}
<b>Type:</b> {hit_type}
<b>Entry:</b> {entry_price:.8f}
<b>Exit:</b> {current_price:.8f}
<b>PnL:</b> {pnl_pct:+.2f}%

<i>Time: {hit_time}</i>
"""
    
    await send_telegram(msg)
    log.info(f"📢 {symbol}: {hit_type} hit at {current_price:.8f} ({pnl_pct:+.2f}%)")

# ---------------- MAIN SCANNING LOGIC ----------------
async def scan_symbol_for_tf_pair(exchange, symbol: str, tf_pair: Dict) -> Optional[Dict]:
    """Execute full 8-step process for one symbol on one TF pair"""
    
    analysis_tf = tf_pair["analysis_tf"]
    entry_tf = tf_pair["entry_tf"]
    pair_name = tf_pair["name"]
    
    ticker = await exchange.fetch_ticker(symbol)
    current_price = ticker.get("last", 0)
    if not current_price:
        return None
    
    log.debug(f"🔍 [{pair_name}] Scanning {symbol} at {current_price}")
    
    # STEP 1: HTF BIAS
    htf_context = await analyze_htf_bias(exchange, symbol, analysis_tf)
    if not htf_context.valid:
        log.debug(f"  [{pair_name}] {symbol}: Skipped HTF - {htf_context.skip_reason}")
        return None
    
    # STEP 2: LIQUIDITY MAP
    liquidity_map = await map_liquidity(exchange, symbol, htf_context, current_price, entry_tf)
    if not liquidity_map.has_clear_target:
        log.debug(f"  [{pair_name}] {symbol}: No clear liquidity targets")
        return None
    
    # STEP 3: LIQUIDITY SWEEP
    sweep = await analyze_sweep(exchange, symbol, analysis_tf, entry_tf)
    if sweep.type == "NONE" or not sweep.impulsive or sweep.fake_sweep:
        log.debug(f"  [{pair_name}] {symbol}: No valid sweep")
        return None
    
    if sweep.type == "HIGH_SWEEP":
        side = "SELL"
    elif sweep.type == "LOW_SWEEP":
        side = "BUY"
    else:
        return None
    
    # STEP 4: STRUCTURE CHECK
    structure_shift = await check_structure_shift(exchange, symbol, sweep, htf_context)
    if not structure_shift.confirmed:
        log.debug(f"  [{pair_name}] {symbol}: No structure shift")
        return None
    
    # STEP 5: ENTRY ZONE
    entry_zone = await find_entry_zone(exchange, symbol, htf_context, sweep, structure_shift, side, entry_tf)
    if entry_zone.type == "NONE" or not entry_zone.candle_reaction:
        log.debug(f"  [{pair_name}] {symbol}: No valid entry zone")
        return None
    
    # STEP 6: RISK/SL
    risk_sl = calculate_risk_sl(entry_zone, sweep, htf_context, side)
    if risk_sl.sl_price == 0:
        log.debug(f"  [{pair_name}] {symbol}: Failed to calculate SL")
        return None
    
    # STEP 7: TAKE PROFIT
    tp_levels = calculate_take_profits(entry_zone.price, side, liquidity_map, htf_context)
    
    # STEP 8: PROBABILITY CHECK
    probability = calculate_probability(htf_context, liquidity_map, sweep, structure_shift, entry_zone, side)
    
    if not probability.acceptable:
        log.debug(f"  [{pair_name}] {symbol}: Probability too low ({probability.total_score:.2f}/5)")
        return None
    
    log.info(f"✅ [{pair_name}] {symbol}: A+ Setup! Score: {probability.total_score:.2f}/5")
    
    setup = {
        "symbol": symbol,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "side": side,
        "current_price": current_price,
        "timeframe_pair": pair_name,
        "analysis_tf": analysis_tf,
        "entry_tf": entry_tf,
        
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

async def send_setup_alert(setup: Dict):
    """Format and send setup alert"""
    
    entry = setup["entry_price"]
    sl = setup["sl_price"]
    tp1 = setup["tp1_price"]
    risk = abs(entry - sl)
    reward_tp1 = abs(tp1 - entry)
    rr_ratio = reward_tp1 / risk if risk > 0 else 0
    
    msg = f"""
🔥 <b>ROMEOTPT A+ SETUP - {setup['timeframe_pair']}</b>

<b>Symbol:</b> {setup['symbol']}
<b>Side:</b> {setup['side']}
<b>Analysis TF:</b> {setup['analysis_tf']}
<b>Entry TF:</b> {setup['entry_tf']}

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

✅ <b>TP/SL Monitoring ACTIVE - You will get updates automatically</b>

<i>Detected: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</i>
"""
    
    await send_telegram(msg)
    
    async with db_lock:
        await db_conn.execute("""
            INSERT INTO signals VALUES (
                NULL, :symbol, :timestamp, :side, :timeframe_pair, :analysis_tf, :entry_tf,
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
                :current_price, 'DETECTED', NULL, NULL, ''
            )
        """, {
            "symbol": setup["symbol"],
            "timestamp": setup["timestamp"],
            "side": setup["side"],
            "timeframe_pair": setup["timeframe_pair"],
            "analysis_tf": setup["analysis_tf"],
            "entry_tf": setup["entry_tf"],
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
            "sweep_impulsive": bool(setup["sweep_impulsive"]),
            "sweep_strength": float(setup["sweep_strength"]),
            "structure_shift_type": setup["structure_shift_type"],
            "structure_shift_confirmed": bool(setup["structure_shift_confirmed"]),
            "structure_description": setup["structure_description"],
            "entry_type": setup["entry_type"],
            "entry_price": float(setup["entry_price"]),
            "entry_low": float(setup["entry_low"]),
            "entry_high": float(setup["entry_high"]),
            "entry_aligns_htf": bool(setup["entry_aligns_htf"]),
            "entry_reaction_confirmed": bool(setup["entry_reaction_confirmed"]),
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

# ---------------- MAIN SCANNER ----------------
async def scanner_main(exchange):
    """Main scanning loop"""
    
    await send_telegram("🚀 ROMEOTPT v2.1 Multi-TF Scanner Started")
    await send_telegram("Timeframe Pairs: 15m→5m | 30m→15m | 1h→30m | 4h→1h")
    await send_telegram("✅ TP/SL Monitoring ACTIVE - Automatic updates on hits")
    
    # Start TP/SL monitoring in background
    asyncio.create_task(monitor_tp_sl())
    
    while True:
        try:
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, v.get("quoteVolume", 0)) 
                         for s, v in tickers.items() 
                         if s.endswith("/USDT")]
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            top_pairs = usdt_pairs[:TOP_N]
            
            log.info(f"📊 Scanning {len(top_pairs)} symbols across {len(TIMEFRAME_PAIRS)} TF pairs...")
            
            setups_found = 0
            for symbol, volume in top_pairs:
                for tf_pair in TIMEFRAME_PAIRS:
                    try:
                        setup = await scan_symbol_for_tf_pair(exchange, symbol, tf_pair)
                        if setup:
                            await send_setup_alert(setup)
                            setups_found += 1
                            await asyncio.sleep(1)
                    except Exception as e:
                        log.error(f"Error scanning {symbol} on {tf_pair['name']}: {e}")
                        continue
            
            if setups_found > 0:
                log.info(f"✅ Found {setups_found} A+ setups across all TF pairs")
            else:
                log.info("⏳ No setups found this scan")
            
        except Exception as e:
            log.exception(f"Scanner error: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/health")
async def health():
    return {"status": "healthy", "scanner": "ROMEOTPT v2.1 Multi-TF"}

@app.get("/setups")
async def get_setups(limit: int = 20, min_score: float = 3.5, status: str = None):
    async with db_lock:
        query = """SELECT * FROM signals WHERE prob_total_score >= ? """
        params = [min_score]
        
        if status:
            query += " AND status = ?"
            params.append(status)
        
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

# ---------------- MAIN ----------------
async def main():
    global db_conn
    
    await init_db()
    
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"}
    })
    
    await scanner_main(exchange)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", help="Run HTTP server")
    parser.add_argument("--single-tf", type=str, help="Run single TF pair (e.g., '15m→5m')")
    args = parser.parse_args()
    
    if args.single_tf:
        TIMEFRAME_PAIRS = [p for p in TIMEFRAME_PAIRS if p["name"] == args.single_tf]
        log.info(f"Running in single-TF mode: {args.single_tf}")
    
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Shutting down ROMEOTPT v2.1 scanner...")
        finally:
            if db_conn:
                asyncio.run(db_conn.close())