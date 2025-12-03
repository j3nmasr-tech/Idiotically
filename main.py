#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ROMEOPT-P 10/10 ULTIMATE SCANNER
- Complete RomeOPT-P 6-step with full SMC elements
- BOS/CHOCH detection + FVG premium/discount zones
- Multi-timeframe structure confluence
- Professional entry refinement
- Institutional-grade risk management
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
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

# ============================================================================
# ENUMS & DATA STRUCTURES
# ============================================================================

class MarketRegime(Enum):
    TRENDING_BULL = "TRENDING_BULL"
    TRENDING_BEAR = "TRENDING_BEAR"
    RANGING = "RANGING"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    LOW_VOLATILITY = "LOW_VOLATILITY"

class OBQuality(Enum):
    FRESH = 4  # Untested, highest probability
    TESTED_1 = 3  # 1 test, still strong
    TESTED_2 = 2  # 2 tests, weakening
    MITIGATED = 1  # Multiple tests, low probability
    INVALID = 0  # Broken structure

class FVGType(Enum):
    PREMIUM = "PREMIUM"  # Above EMA50 in uptrend
    DISCOUNT = "DISCOUNT"  # Below EMA50 in downtrend
    NEUTRAL = "NEUTRAL"

@dataclass
class MarketStructure:
    swing_points: List[Dict]
    order_blocks: List[Dict]
    fair_value_gaps: List[Dict]
    bos_choch: List[Dict]
    trend_direction: str

# ============================================================================
# CONFIGURATION
# ============================================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))
TOP_N = int(os.getenv("TOP_N", 25))  # Reduced for quality focus
TIMEFRAMES = ["5m", "15m", "1h"]  # Cleaner timeframes
MIN_SCORE = 7  # Increased for higher quality
CRITICAL_FACTORS_MIN = 3  # HTF + Sweep + Structure alignment

# RomeOPT-P Timeframe Hierarchy
HTF_MAP = {
    "5m": "1h",
    "15m": "4h", 
    "1h": "4h"
}

# ============================================================================
# LOGGING & TELEGRAM
# ============================================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(name)s | %(levelname)s | %(message)s")
log = logging.getLogger("romeopt_10_10")
db_lock = asyncio.Lock()
db_conn = None

def escape_html(msg: str) -> str:
    if not msg: return "-"
    return str(msg).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

async def tg(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: 
        return
    safe_msg = escape_html(msg)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID, 
                "text": safe_msg, 
                "parse_mode": "HTML"
            })
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

# ============================================================================
# DATABASE
# ============================================================================

async def init_db():
    global db_conn
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute("PRAGMA journal_mode=WAL;")
    await db_conn.execute("PRAGMA synchronous=NORMAL;")
    
    # Enhanced schema for 10/10 system
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS signals_10_10 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry REAL NOT NULL,
            sl REAL NOT NULL,
            tp1 REAL NOT NULL,
            tp2 REAL NOT NULL,
            entry_tf TEXT NOT NULL,
            score INTEGER NOT NULL,
            confluence_score REAL NOT NULL,
            mtf_alignment INTEGER NOT NULL,
            bos_choch_confirmed INTEGER DEFAULT 0,
            fvg_type TEXT,
            ob_quality TEXT,
            volume_confirmation REAL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'OPEN',
            tp1_hit INTEGER DEFAULT 0,
            tp2_hit INTEGER DEFAULT 0,
            pnl REAL DEFAULT 0,
            pnl_pct REAL DEFAULT 0,
            reason TEXT
        )
    """)
    
    await db_conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_signals_status ON signals_10_10(status)
    """)
    
    await db_conn.commit()
    log.info("Database initialized with 10/10 schema")

# ============================================================================
# CORE SMC FUNCTIONS (10/10 IMPLEMENTATION)
# ============================================================================

async def fetch_ohlcv_safe(exchange, symbol: str, timeframe: str, limit=200):
    """Safe OHLCV fetching with error handling"""
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if not ohlcv or len(ohlcv) < 20:
            return None
        
        # Validate data structure
        if len(ohlcv[0]) < 6:
            log.warning(f"Invalid OHLCV format for {symbol} {timeframe}")
            return None
            
        return ohlcv
    except Exception as e:
        log.debug(f"fetch_ohlcv failed for {symbol} {timeframe}: {e}")
        return None

def calculate_atr(df: pd.DataFrame, period=14):
    """Robust ATR calculation"""
    try:
        high = df["high"].astype(float)
        low = df["low"].astype(float)
        close = df["close"].astype(float).shift(1)
        
        tr1 = high - low
        tr2 = (high - close).abs()
        tr3 = (low - close).abs()
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr_series = tr.rolling(period, min_periods=1).mean()
        return atr_series
    except Exception as e:
        log.error(f"ATR calculation failed: {e}")
        return pd.Series([0.0] * len(df))

def detect_swing_points(df: pd.DataFrame, sensitivity=2):
    """Detect swing highs and lows for structure analysis"""
    swing_points = []
    high_series = df['high'].values
    low_series = df['low'].values
    
    for i in range(sensitivity, len(df) - sensitivity):
        # Swing High
        if all(high_series[i] > high_series[i-j] for j in range(1, sensitivity+1)) and \
           all(high_series[i] > high_series[i+j] for j in range(1, sensitivity+1)):
            swing_points.append({
                'index': i,
                'price': high_series[i],
                'type': 'high',
                'strength': 1.0
            })
        
        # Swing Low
        if all(low_series[i] < low_series[i-j] for j in range(1, sensitivity+1)) and \
           all(low_series[i] < low_series[i+j] for j in range(1, sensitivity+1)):
            swing_points.append({
                'index': i,
                'price': low_series[i],
                'type': 'low',
                'strength': 1.0
            })
    
    return swing_points

def detect_bos_choch(df: pd.DataFrame, swing_points: List[Dict]):
    """Detect Break of Structure (BOS) and Change of Character (CHOCH)"""
    bos_points = []
    choch_points = []
    
    if len(swing_points) < 4:
        return bos_points, choch_points
    
    # Sort by index
    swing_points.sort(key=lambda x: x['index'])
    
    highs = [s for s in swing_points if s['type'] == 'high']
    lows = [s for s in swing_points if s['type'] == 'low']
    
    # Detect Higher High (HH) / Lower High (LH)
    for i in range(1, len(highs)):
        if highs[i]['price'] > highs[i-1]['price']:
            bos_points.append({
                'index': highs[i]['index'],
                'price': highs[i]['price'],
                'type': 'HH',
                'previous': highs[i-1]['price']
            })
        else:
            choch_points.append({
                'index': highs[i]['index'],
                'price': highs[i]['price'],
                'type': 'LH',
                'previous': highs[i-1]['price']
            })
    
    # Detect Higher Low (HL) / Lower Low (LL)
    for i in range(1, len(lows)):
        if lows[i]['price'] > lows[i-1]['price']:
            bos_points.append({
                'index': lows[i]['index'],
                'price': lows[i]['price'],
                'type': 'HL',
                'previous': lows[i-1]['price']
            })
        else:
            choch_points.append({
                'index': lows[i]['index'],
                'price': lows[i]['price'],
                'type': 'LL',
                'previous': lows[i-1]['price']
            })
    
    return bos_points, choch_points

def identify_order_blocks_advanced(df: pd.DataFrame):
    """Advanced OB detection with quality grading"""
    ob_blocks = []
    
    for i in range(2, len(df) - 2):
        current = df.iloc[i]
        prev1 = df.iloc[i-1]
        prev2 = df.iloc[i-2]
        
        # Volume confirmation
        vol_avg = df['volume'].iloc[max(0, i-10):i].mean()
        vol_confirmation = current['volume'] > vol_avg * 1.5 if vol_avg > 0 else False
        
        # Bullish Order Block (Bearish candle followed by bullish engulfing)
        if (prev1['close'] < prev1['open'] and  # Previous bearish
            current['close'] > current['open'] and  # Current bullish
            current['low'] < prev1['low'] and  # Takes liquidity
            current['close'] > prev1['open']):  # Closes above previous open
            
            # Quality grading
            body_size = abs(current['close'] - current['open'])
            wick_ratio = (current['high'] - current['low'] - body_size) / body_size if body_size > 0 else 1
            
            ob_blocks.append({
                'index': i,
                'type': 'bullish',
                'low': min(current['low'], prev1['low']),
                'high': max(current['close'], prev1['close']),
                'body_low': min(current['open'], current['close']),
                'body_high': max(current['open'], current['close']),
                'volume_confirmation': vol_confirmation,
                'wick_ratio': wick_ratio,
                'quality': OBQuality.FRESH if wick_ratio < 0.5 and vol_confirmation else OBQuality.TESTED_1
            })
        
        # Bearish Order Block (Bullish candle followed by bearish engulfing)
        elif (prev1['close'] > prev1['open'] and  # Previous bullish
              current['close'] < current['open'] and  # Current bearish
              current['high'] > prev1['high'] and  # Takes liquidity
              current['close'] < prev1['open']):  # Closes below previous open
            
            body_size = abs(current['close'] - current['open'])
            wick_ratio = (current['high'] - current['low'] - body_size) / body_size if body_size > 0 else 1
            
            ob_blocks.append({
                'index': i,
                'type': 'bearish',
                'low': min(current['close'], prev1['close']),
                'high': max(current['high'], prev1['high']),
                'body_low': min(current['open'], current['close']),
                'body_high': max(current['open'], current['close']),
                'volume_confirmation': vol_confirmation,
                'wick_ratio': wick_ratio,
                'quality': OBQuality.FRESH if wick_ratio < 0.5 and vol_confirmation else OBQuality.TESTED_1
            })
    
    return ob_blocks

def detect_fair_value_gaps(df: pd.DataFrame):
    """Detect FVGs with premium/discount classification"""
    fvgs = []
    
    for i in range(2, len(df) - 1):
        current_low = df['low'].iloc[i]
        prev_high = df['high'].iloc[i-1]
        current_high = df['high'].iloc[i]
        prev_low = df['low'].iloc[i-1]
        
        # Calculate EMA for premium/discount
        ema50 = df['close'].ewm(span=50, adjust=False).mean().iloc[i]
        
        # Bullish FVG (price gapped up)
        if current_low > prev_high:
            is_premium = current_low > ema50
            
            fvgs.append({
                'index': i,
                'type': 'bullish',
                'low': prev_high,
                'high': current_low,
                'midpoint': (prev_high + current_low) / 2,
                'fvg_type': FVGType.PREMIUM if is_premium else FVGType.DISCOUNT,
                'size': current_low - prev_high
            })
        
        # Bearish FVG (price gapped down)
        elif current_high < prev_low:
            is_discount = current_high < ema50
            
            fvgs.append({
                'index': i,
                'type': 'bearish',
                'low': current_high,
                'high': prev_low,
                'midpoint': (current_high + prev_low) / 2,
                'fvg_type': FVGType.DISCOUNT if is_discount else FVGType.PREMIUM,
                'size': prev_low - current_high
            })
    
    return fvgs

def analyze_market_structure(df: pd.DataFrame) -> MarketStructure:
    """Complete market structure analysis"""
    swing_points = detect_swing_points(df)
    bos_points, choch_points = detect_bos_choch(df, swing_points)
    order_blocks = identify_order_blocks_advanced(df)
    fair_value_gaps = detect_fair_value_gaps(df)
    
    # Determine trend direction
    if bos_points:
        recent_bos = sorted(bos_points, key=lambda x: x['index'], reverse=True)[:3]
        hh_count = sum(1 for p in recent_bos if p['type'] == 'HH')
        hl_count = sum(1 for p in recent_bos if p['type'] == 'HL')
        trend = "bullish" if hh_count + hl_count >= 2 else "bearish"
    else:
        trend = "neutral"
    
    return MarketStructure(
        swing_points=swing_points,
        order_blocks=order_blocks,
        fair_value_gaps=fair_value_gaps,
        bos_choch=bos_points + choch_points,
        trend_direction=trend
    )

# ============================================================================
# MULTI-TIMEFRAME ANALYSIS (10/10 LOGIC)
# ============================================================================

async def analyze_multi_timeframe_structure(exchange, symbol: str, entry_tf: str):
    """Analyze structure across multiple timeframes"""
    structure_map = {}
    
    # Analyze higher timeframe first
    htf = HTF_MAP.get(entry_tf, "4h")
    ohlcv_htf = await fetch_ohlcv_safe(exchange, symbol, htf, 100)
    
    if ohlcv_htf:
        df_htf = pd.DataFrame(ohlcv_htf, columns=["timestamp","open","high","low","close","volume"])
        for col in ["open","high","low","close","volume"]:
            df_htf[col] = pd.to_numeric(df_htf[col], errors="coerce")
        
        structure_htf = analyze_market_structure(df_htf)
        structure_map[htf] = structure_htf
    
    # Analyze entry timeframe
    ohlcv_entry = await fetch_ohlcv_safe(exchange, symbol, entry_tf, 100)
    if ohlcv_entry:
        df_entry = pd.DataFrame(ohlcv_entry, columns=["timestamp","open","high","low","close","volume"])
        for col in ["open","high","low","close","volume"]:
            df_entry[col] = pd.to_numeric(df_entry[col], errors="coerce")
        
        structure_entry = analyze_market_structure(df_entry)
        structure_map[entry_tf] = structure_entry
    
    return structure_map

def calculate_mtf_confluence(structure_map: Dict, side: str) -> float:
    """Calculate MTF confluence score (0-1)"""
    if not structure_map:
        return 0.0
    
    scores = []
    
    for tf, structure in structure_map.items():
        tf_score = 0.0
        
        # Structure alignment
        if structure.trend_direction == side.lower():
            tf_score += 0.3
        
        # BOS/CHOCH confirmation
        if structure.bos_choch:
            if side == "BUY":
                bullish_signals = sum(1 for p in structure.bos_choch if p['type'] in ['HH', 'HL'])
                tf_score += (bullish_signals / len(structure.bos_choch)) * 0.3
            else:
                bearish_signals = sum(1 for p in structure.bos_choch if p['type'] in ['LL', 'LH'])
                tf_score += (bearish_signals / len(structure.bos_choch)) * 0.3
        
        # OB quality
        if structure.order_blocks:
            relevant_obs = [ob for ob in structure.order_blocks 
                          if ob['type'] == ('bullish' if side == "BUY" else 'bearish')]
            if relevant_obs:
                avg_quality = sum(ob['quality'].value for ob in relevant_obs) / len(relevant_obs)
                tf_score += (avg_quality / 4.0) * 0.2
        
        # FVG alignment
        if structure.fair_value_gaps:
            relevant_fvgs = [fvg for fvg in structure.fair_value_gaps 
                           if fvg['type'] == ('bullish' if side == "BUY" else 'bearish')]
            if relevant_fvgs:
                premium_count = sum(1 for fvg in relevant_fvgs 
                                  if fvg.get('fvg_type') == (FVGType.PREMIUM if side == "BUY" else FVGType.DISCOUNT))
                tf_score += (premium_count / max(len(relevant_fvgs), 1)) * 0.2
        
        scores.append(tf_score)
    
    return sum(scores) / len(scores) if scores else 0.0

# ============================================================================
# ENTRY REFINEMENT (10/10 LOGIC)
# ============================================================================

def calculate_entry_refinement_score(df: pd.DataFrame, side: str, entry_price: float, structure: MarketStructure) -> float:
    """Calculate entry refinement score (0-1)"""
    score = 0.0
    
    # 1. Volume confirmation (0.3 points)
    last_candle = df.iloc[-1]
    vol_avg = df['volume'].iloc[-20:-1].mean()
    vol_ratio = last_candle['volume'] / vol_avg if vol_avg > 0 else 1
    
    if vol_ratio > 2.0:
        score += 0.3
    elif vol_ratio > 1.5:
        score += 0.2
    elif vol_ratio > 1.2:
        score += 0.1
    
    # 2. Displacement quality (0.3 points)
    body_size = abs(last_candle['close'] - last_candle['open'])
    candle_range = last_candle['high'] - last_candle['low']
    if candle_range > 0:
        displacement = body_size / candle_range
        if displacement > 0.7:
            score += 0.3
        elif displacement > 0.6:
            score += 0.2
        elif displacement > 0.5:
            score += 0.1
    
    # 3. OB proximity (0.2 points)
    if structure.order_blocks:
        relevant_obs = [ob for ob in structure.order_blocks 
                      if ob['type'] == ('bullish' if side == "BUY" else 'bearish')]
        if relevant_obs:
            closest_ob = min(relevant_obs, 
                           key=lambda ob: abs(entry_price - ob['midpoint']))
            distance_pct = abs(entry_price - closest_ob['midpoint']) / entry_price
            if distance_pct < 0.001:  # Within 0.1%
                score += 0.2
            elif distance_pct < 0.002:  # Within 0.2%
                score += 0.1
    
    # 4. FVG alignment (0.2 points)
    if structure.fair_value_gaps:
        relevant_fvgs = [fvg for fvg in structure.fair_value_gaps 
                       if fvg['type'] == ('bullish' if side == "BUY" else 'bearish')]
        for fvg in relevant_fvgs:
            if fvg['low'] <= entry_price <= fvg['high']:
                if (side == "BUY" and fvg.get('fvg_type') == FVGType.DISCOUNT) or \
                   (side == "SELL" and fvg.get('fvg_type') == FVGType.PREMIUM):
                    score += 0.2
                    break
    
    return min(score, 1.0)

# ============================================================================
# ROMEOPT-P 10/10 SIGNAL GENERATOR
# ============================================================================

async def generate_romeopt_10_10_signal(exchange, symbol: str, tf: str):
    """Complete RomeOPT-P 10/10 signal generation"""
    try:
        # Fetch data
        ohlcv = await fetch_ohlcv_safe(exchange, symbol, tf, 100)
        if not ohlcv:
            return None
        
        df = pd.DataFrame(ohlcv, columns=["timestamp","open","high","low","close","volume"])
        for col in ["open","high","low","close","volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        if len(df) < 30:
            return None
        
        # Analyze market structure
        structure = analyze_market_structure(df)
        
        # Multi-timeframe analysis
        mtf_structure = await analyze_multi_timeframe_structure(exchange, symbol, tf)
        
        # Check for recent OB
        if not structure.order_blocks:
            return None
        
        # Get most recent relevant OB
        recent_obs = sorted(structure.order_blocks, key=lambda x: x['index'], reverse=True)
        latest_ob = recent_obs[0]
        
        # Determine trade side
        side = "BUY" if latest_ob['type'] == 'bullish' else "SELL"
        current_price = float(df['close'].iloc[-1])
        
        # Check if price is at OB zone
        if side == "BUY":
            if not (latest_ob['low'] <= current_price <= latest_ob['high']):
                return None
        else:
            if not (latest_ob['low'] <= current_price <= latest_ob['high']):
                return None
        
        # Calculate confluence scores
        mtf_confluence = calculate_mtf_confluence(mtf_structure, side)
        entry_refinement = calculate_entry_refinement_score(df, side, current_price, structure)
        
        # Check minimum thresholds
        if mtf_confluence < 0.7:  # 70% MTF confluence minimum
            return None
        
        if entry_refinement < 0.6:  # 60% entry refinement minimum
            return None
        
        # Calculate final score (0-10)
        base_score = 0
        reasons = []
        
        # 1. Liquidity Sweep (+2)
        last_candle = df.iloc[-1]
        prev_highs = df['high'].iloc[-10:-1]
        prev_lows = df['low'].iloc[-10:-1]
        
        sweep_high = last_candle['high'] > prev_highs.max()
        sweep_low = last_candle['low'] < prev_lows.min()
        
        if sweep_high or sweep_low:
            base_score += 2
            reasons.append("Liquidity Sweep ✅")
        
        # 2. Displacement with volume (+2)
        body_size = abs(last_candle['close'] - last_candle['open'])
        candle_range = last_candle['high'] - last_candle['low']
        vol_avg = df['volume'].iloc[-10:-1].mean()
        
        if candle_range > 0:
            displacement = body_size / candle_range
            vol_confirmation = last_candle['volume'] > vol_avg * 1.5 if vol_avg > 0 else False
            
            if displacement > 0.6 and vol_confirmation:
                base_score += 2
                reasons.append("Strong Displacement with Volume ✅")
            elif displacement > 0.6:
                base_score += 1
                reasons.append("Displacement ✅")
        
        # 3. OB Quality (+2)
        ob_quality = latest_ob['quality'].value
        if ob_quality >= 3:  # FRESH or TESTED_1
            base_score += 2
            reasons.append(f"Quality OB ({latest_ob['quality'].name}) ✅")
        elif ob_quality >= 2:
            base_score += 1
            reasons.append(f"Decent OB ({latest_ob['quality'].name})")
        
        # 4. MTF Alignment (+2)
        base_score += int(mtf_confluence * 2)
        reasons.append(f"MTF Confluence: {mtf_confluence:.1%} ✅")
        
        # 5. Entry Refinement (+2)
        base_score += int(entry_refinement * 2)
        reasons.append(f"Entry Refinement: {entry_refinement:.1%} ✅")
        
        # Minimum score check
        if base_score < MIN_SCORE:
            return None
        
        # Calculate TP/SL with RomeOPT-P logic
        atr_val = float(calculate_atr(df).iloc[-1])
        stop_loss, take_profits = calculate_romeopt_tp_sl(
            current_price, side, atr_val, latest_ob, df, structure
        )
        
        # Risk/Reward check
        risk = abs(current_price - stop_loss)
        reward1 = abs(take_profits[0] - current_price)
        
        if risk == 0 or reward1 / risk < 1.5:
            return None
        
        # Create signal
        signal = {
            "symbol": symbol,
            "side": side,
            "entry": current_price,
            "sl": stop_loss,
            "tp1": take_profits[0],
            "tp2": take_profits[1] if len(take_profits) > 1 else take_profits[0],
            "entry_tf": tf,
            "score": base_score,
            "confluence_score": mtf_confluence,
            "entry_refinement": entry_refinement,
            "mtf_alignment": int(mtf_confluence * 100),
            "bos_choch_confirmed": 1 if structure.bos_choch else 0,
            "fvg_type": "PREMIUM" if structure.fair_value_gaps and 
                        structure.fair_value_gaps[-1].get('fvg_type') == FVGType.PREMIUM else "DISCOUNT",
            "ob_quality": latest_ob['quality'].name,
            "volume_confirmation": float(last_candle['volume'] / vol_avg) if vol_avg > 0 else 1.0,
            "reason": " | ".join(reasons),
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        
        return signal
        
    except Exception as e:
        log.error(f"Signal generation failed for {symbol} {tf}: {e}")
        return None

def calculate_romeopt_tp_sl(entry: float, side: str, atr_val: float, ob_zone: Dict, 
                           df: pd.DataFrame, structure: MarketStructure) -> Tuple[float, List[float]]:
    """RomeOPT-P TP/SL calculation with market structure"""
    
    # Find swing points for TP targets
    swing_points = structure.swing_points
    
    if side == "BUY":
        # Stop Loss: Below OB low with ATR buffer
        sl = ob_zone['low'] - (atr_val * 0.15)
        
        # Find nearest swing highs for TP
        swing_highs = [s['price'] for s in swing_points if s['type'] == 'high']
        if swing_highs:
            nearest_resistance = min([h for h in swing_highs if h > entry], default=entry * 1.02)
        else:
            nearest_resistance = entry * 1.02
        
        # Calculate risk
        risk = entry - sl
        
        # TP1: 1.0R or nearest resistance
        tp1 = min(entry + risk * 1.0, nearest_resistance)
        
        # TP2: 1.8R or next major swing
        if len(swing_highs) > 1:
            swing_highs_sorted = sorted([h for h in swing_highs if h > tp1])
            tp2 = swing_highs_sorted[0] if swing_highs_sorted else entry + risk * 1.8
        else:
            tp2 = entry + risk * 1.8
        
        take_profits = [tp1, tp2]
        
    else:  # SELL
        # Stop Loss: Above OB high with ATR buffer
        sl = ob_zone['high'] + (atr_val * 0.15)
        
        # Find nearest swing lows for TP
        swing_lows = [s['price'] for s in swing_points if s['type'] == 'low']
        if swing_lows:
            nearest_support = max([l for l in swing_lows if l < entry], default=entry * 0.98)
        else:
            nearest_support = entry * 0.98
        
        # Calculate risk
        risk = sl - entry
        
        # TP1: 1.0R or nearest support
        tp1 = max(entry - risk * 1.0, nearest_support)
        
        # TP2: 1.8R or next major swing
        if len(swing_lows) > 1:
            swing_lows_sorted = sorted([l for l in swing_lows if l < tp1], reverse=True)
            tp2 = swing_lows_sorted[0] if swing_lows_sorted else entry - risk * 1.8
        else:
            tp2 = entry - risk * 1.8
        
        take_profits = [tp1, tp2]
    
    return sl, take_profits

# ============================================================================
# SIGNAL LOGGING & MONITORING
# ============================================================================

async def log_signal_10_10(sig: Dict):
    """Log 10/10 signal to database"""
    async with db_lock:
        try:
            await db_conn.execute("""
                INSERT INTO signals_10_10 
                (symbol, side, entry, sl, tp1, tp2, entry_tf, score, confluence_score, 
                 mtf_alignment, bos_choch_confirmed, fvg_type, ob_quality, 
                 volume_confirmation, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sig["symbol"], sig["side"], sig["entry"], sig["sl"], 
                sig["tp1"], sig["tp2"], sig["entry_tf"], sig["score"],
                sig["confluence_score"], sig["mtf_alignment"], 
                sig["bos_choch_confirmed"], sig.get("fvg_type", "N/A"),
                sig.get("ob_quality", "N/A"), sig.get("volume_confirmation", 1.0),
                sig["reason"]
            ))
            await db_conn.commit()
            
            # Send Telegram alert
            alert_msg = f"""
🏆 <b>ROMEOPT-P 10/10 SIGNAL</b>
Pair: {sig['symbol']} ({sig['entry_tf']})
Side: {sig['side']}
Entry: {sig['entry']:.4f}
SL: {sig['sl']:.4f}
TP1: {sig['tp1']:.4f} (1.0R)
TP2: {sig['tp2']:.4f} (1.8R)
Score: {sig['score']}/10
Confluence: {sig['confluence_score']:.1%}
Entry Refinement: {sig.get('entry_refinement', 0):.1%}
OB Quality: {sig.get('ob_quality', 'N/A')}
            """
            await tg(alert_msg)
            
        except Exception as e:
            log.error(f"Failed to log signal: {e}")

async def monitor_signals_10_10(exchange):
    """Monitor and manage 10/10 signals"""
    while True:
        try:
            async with db_lock:
                cursor = await db_conn.execute("""
                    SELECT id, symbol, side, entry, sl, tp1, tp2, tp1_hit, tp2_hit, status 
                    FROM signals_10_10 
                    WHERE status = 'OPEN'
                """)
                
                rows = await cursor.fetchall()
                
                for row in rows:
                    sig_id, symbol, side, entry, sl, tp1, tp2, tp1_hit, tp2_hit, status = row
                    
                    # Get current price
                    try:
                        ticker = await exchange.fetch_ticker(symbol)
                        current_price = ticker.get('last')
                        if not current_price:
                            continue
                    except:
                        continue
                    
                    # Check TP/SL
                    hits = []
                    new_tp1_hit = tp1_hit
                    new_tp2_hit = tp2_hit
                    new_status = status
                    
                    if side == "BUY":
                        if not tp1_hit and current_price >= tp1:
                            hits.append("TP1")
                            new_tp1_hit = 1
                            # Move SL to breakeven
                            new_sl = entry
                            await db_conn.execute(
                                "UPDATE signals_10_10 SET sl = ? WHERE id = ?",
                                (new_sl, sig_id)
                            )
                        
                        if not tp2_hit and current_price >= tp2:
                            hits.append("TP2")
                            new_tp2_hit = 1
                            new_status = "CLOSED"
                        
                        if current_price <= sl:
                            hits.append("SL")
                            new_status = "CLOSED"
                    
                    else:  # SELL
                        if not tp1_hit and current_price <= tp1:
                            hits.append("TP1")
                            new_tp1_hit = 1
                            # Move SL to breakeven
                            new_sl = entry
                            await db_conn.execute(
                                "UPDATE signals_10_10 SET sl = ? WHERE id = ?",
                                (new_sl, sig_id)
                            )
                        
                        if not tp2_hit and current_price <= tp2:
                            hits.append("TP2")
                            new_tp2_hit = 1
                            new_status = "CLOSED"
                        
                        if current_price >= sl:
                            hits.append("SL")
                            new_status = "CLOSED"
                    
                    # Update database
                    if hits:
                        await db_conn.execute("""
                            UPDATE signals_10_10 
                            SET tp1_hit = ?, tp2_hit = ?, status = ?
                            WHERE id = ?
                        """, (new_tp1_hit, new_tp2_hit, new_status, sig_id))
                        
                        # Calculate PnL for closed trades
                        if new_status == "CLOSED":
                            exit_price = current_price
                            if side == "BUY":
                                pnl = (exit_price - entry)
                            else:
                                pnl = (entry - exit_price)
                            
                            pnl_pct = (pnl / entry) * 100
                            
                            await db_conn.execute("""
                                UPDATE signals_10_10 
                                SET pnl = ?, pnl_pct = ?
                                WHERE id = ?
                            """, (pnl, pnl_pct, sig_id))
                        
                        # Send update alert
                        update_msg = f"""
🎯 <b>TRADE UPDATE</b>
{symbol} {side}
Entry: {entry:.4f}
Current: {current_price:.4f}
Hits: {', '.join(hits)}
Status: {new_status}
                        """
                        await tg(update_msg)
                
                await db_conn.commit()
                
        except Exception as e:
            log.error(f"Monitor error: {e}")
        
        await asyncio.sleep(5)

# ============================================================================
# MAIN SCANNING LOOP
# ============================================================================

async def scan_loop_10_10(exchange):
    """10/10 Scanning loop"""
    last_scan = {}
    
    while True:
        try:
            # Get top volume pairs
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, v.get('quoteVolume', 0)) 
                         for s, v in tickers.items() 
                         if '/USDT' in s and ':' not in s]
            
            top_symbols = sorted(usdt_pairs, key=lambda x: x[1], reverse=True)[:TOP_N]
            
            signals_found = 0
            
            for symbol, volume in top_symbols:
                # Check cooldown
                if symbol in last_scan:
                    if time.time() - last_scan[symbol] < 30:  # 30 second cooldown
                        continue
                
                for tf in TIMEFRAMES:
                    signal = await generate_romeopt_10_10_signal(exchange, symbol, tf)
                    
                    if signal:
                        await log_signal_10_10(signal)
                        last_scan[symbol] = time.time()
                        signals_found += 1
                        break  # Only one signal per symbol per scan
            
            if signals_found:
                log.info(f"Found {signals_found} 10/10 signals")
            
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"Scan error: {e}")
            await asyncio.sleep(10)

# ============================================================================
# MAIN APPLICATION
# ============================================================================

async def main_10_10():
    """Main 10/10 application"""
    # Initialize database
    await init_db()
    
    # Initialize exchange
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {
            "defaultType": "spot"
        }
    })
    
    # Startup message
    await tg("🚀 <b>ROMEOPT-P 10/10 SYSTEM STARTED</b>\n"
             "✅ Full SMC Implementation\n"
             "✅ BOS/CHOCH Detection\n"
             "✅ FVG Premium/Discount Zones\n"
             "✅ Multi-Timeframe Confluence\n"
             "✅ Professional Entry Refinement")
    
    log.info("RomeOPT-P 10/10 System started")
    
    # Run scanner and monitor
    await asyncio.gather(
        scan_loop_10_10(exchange),
        monitor_signals_10_10(exchange)
    )

if __name__ == "__main__":
    # Command line parsing
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", help="Start HTTP server")
    args = parser.parse_args()
    
    if args.http:
        # FastAPI server
        app = FastAPI()
        
        @app.post("/webhook")
        async def webhook(request: Request):
            token = request.headers.get("X-Auth", "")
            if token != WEBHOOK_SECRET:
                raise HTTPException(status_code=403, detail="Invalid secret")
            data = await request.json()
            log.info(f"Webhook received: {data}")
            return {"ok": True}
        
        @app.get("/health")
        async def health():
            return {"status": "healthy", "system": "RomeOPT-P 10/10"}
        
        uvicorn.run(app, host="0.0.0.0", port=9000)
    else:
        # Run trading system
        try:
            asyncio.run(main_10_10())
        except KeyboardInterrupt:
            log.info("Shutdown requested")
        finally:
            if db_conn:
                asyncio.run(db_conn.close())
            log.info("RomeOPT-P 10/10 System stopped")