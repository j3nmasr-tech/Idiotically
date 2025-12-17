#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TRUE ROMEOPT SCANNER - HIGHER TIMEFRAME TP EDITION (FINAL COMPACT)
- RomeOPT 6-step entry logic
- TP SET ON HIGHER TIMEFRAME for structural liquidity
- Entry TF → TP TF mapping: 1m→15m/30m, 3m→15m/30m, 5m→15m/30m, 15m→30m/1h, 30m→1h/4h
- External liquidity = range extremes on higher TF
- Enhanced OB detection on higher timeframe
- TP LOCK remains active
- Compact professional signal format
"""

import os
import time
import asyncio
import logging
import datetime
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from collections import defaultdict, deque
from typing import Optional, Dict, List, Tuple, Any

# =============== CONFIGURATION ===============
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))
TOP_N = int(os.getenv("TOP_N", 60))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
MIN_SCORE = 4
CRITICAL_FACTORS_MIN = 2

# =============== TP TIMEFRAME MAPPING ===============
TP_TF_MAPPING = {
    "1m": ["15m", "30m"],
    "3m": ["15m", "30m"],
    "5m": ["15m", "30m"],
    "15m": ["30m", "1h"],
    "30m": ["1h", "4h"]
}

DEFAULT_TP_TF = {
    "1m": "15m",
    "3m": "15m",
    "5m": "30m",
    "15m": "1h",
    "30m": "4h"
}

# =============== FORCED FILTER PARAMETERS ===============
MOMENTUM_STRONG_THRESHOLD = 0.60
MOMENTUM_GOOD_THRESHOLD = 0.55
DISPLACEMENT_MIN_THRESHOLD = 0.50

# =============== LOGGING ===============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("romeopt_htf_bot")

# =============== GLOBALS ===============
db_lock = asyncio.Lock()
db_conn = None
exchange = None
last_signal_time = {}
recent_sl = defaultdict(lambda: deque())

# =============== HELPER FUNCTIONS ===============
def format_price(value: Optional[float]) -> str:
    """Format price with 6 decimals or return N/A"""
    if value is None:
        return "N/A"
    return f"{value:.6f}"

def escape_html(msg: str) -> str:
    """Escape HTML for Telegram messages"""
    if not msg:
        return "-"
    return str(msg).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

async def tg(msg: str):
    """Send message to Telegram"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not configured")
        return
    
    safe_msg = escape_html(msg)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            await client.post(
                url,
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": safe_msg,
                    "parse_mode": "HTML"
                }
            )
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

# =============== DATABASE FUNCTIONS ===============
async def init_db():
    """Initialize database with complete schema and ensure all columns exist"""
    global db_conn
    
    try:
        db_conn = await aiosqlite.connect(DB_PATH)
        await db_conn.execute("PRAGMA journal_mode=WAL;")
        await db_conn.execute("PRAGMA synchronous=NORMAL;")
        
        # Check if table exists
        async with db_conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='signals'") as cursor:
            table_exists = await cursor.fetchone()
        
        if table_exists:
            # Get existing columns
            async with db_conn.execute("PRAGMA table_info(signals)") as cursor:
                existing_columns = await cursor.fetchall()
                existing_column_names = [col[1] for col in existing_columns]
            
            # Define required columns
            required_columns = [
                ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
                ("symbol", "TEXT NOT NULL"),
                ("side", "TEXT NOT NULL"),
                ("entry", "REAL NOT NULL"),
                ("sl", "REAL"),
                ("tp", "REAL NOT NULL"),
                ("timestamp", "TEXT NOT NULL"),
                ("status", "TEXT DEFAULT 'OPEN'"),
                ("reason", "TEXT"),
                ("score", "INTEGER"),
                ("tp_hit", "INTEGER DEFAULT 0"),
                ("latest_ob", "TEXT"),
                ("tp_type", "TEXT"),
                ("tp_locked", "INTEGER DEFAULT 1"),
                ("entry_tf", "TEXT"),
                ("tp_tf", "TEXT"),
                ("tp_distance_pips", "REAL"),
                ("tp_distance_percent", "REAL"),
                ("rr_ratio", "REAL"),
                ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            ]
            
            # Add missing columns
            for col_name, col_type in required_columns:
                if col_name not in existing_column_names:
                    try:
                        await db_conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_type}")
                        log.info(f"✅ Added missing column: {col_name}")
                    except Exception as e:
                        log.warning(f"Could not add column {col_name}: {e}")
            
            await db_conn.commit()
            log.info("✅ Database table updated successfully")
        else:
            # Create table if it doesn't exist
            await db_conn.execute("""
                CREATE TABLE signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    entry REAL NOT NULL,
                    sl REAL,
                    tp REAL NOT NULL,
                    timestamp TEXT NOT NULL,
                    status TEXT DEFAULT 'OPEN',
                    reason TEXT,
                    score INTEGER,
                    tp_hit INTEGER DEFAULT 0,
                    latest_ob TEXT,
                    tp_type TEXT,
                    tp_locked INTEGER DEFAULT 1,
                    entry_tf TEXT,
                    tp_tf TEXT,
                    tp_distance_pips REAL,
                    tp_distance_percent REAL,
                    rr_ratio REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            log.info("✅ Database table created successfully")
        
        # Create performance tracking table if it doesn't exist
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                side TEXT,
                entry_tf TEXT,
                tp_tf TEXT,
                entry_price REAL,
                tp_price REAL,
                sl_price REAL,
                result TEXT,
                pnl_percent REAL,
                rr_achieved REAL,
                duration_minutes INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indices for better performance
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp)")
        
        await db_conn.commit()
        log.info("✅ Database initialized successfully")
        
    except Exception as e:
        log.error(f"❌ Database initialization failed: {e}")
        raise

# =============== DATA FETCHING ===============
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 200):
    """Fetch OHLCV data with error handling"""
    try:
        return await exchange.fetch_ohlcv(
            symbol,
            timeframe=timeframe,
            limit=limit
        )
    except ccxt.NetworkError as e:
        log.debug(f"Network error for {symbol} {timeframe}: {e}")
        return None
    except ccxt.ExchangeError as e:
        log.debug(f"Exchange error for {symbol} {timeframe}: {e}")
        return None
    except Exception as e:
        log.debug(f"Error fetching {symbol} {timeframe}: {e}")
        return None

# =============== INDICATORS ===============
def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate Average True Range"""
    high = df["high"]
    low = df["low"]
    close = df["close"]
    
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period, min_periods=1).mean()
    
    return atr

# =============== FORCED FILTER ===============
def force_filter_trade(momentum_value: float, displacement_value: float) -> bool:
    """Apply forced filter based on momentum and displacement"""
    if momentum_value >= MOMENTUM_STRONG_THRESHOLD:
        return True
    if momentum_value >= MOMENTUM_GOOD_THRESHOLD and displacement_value >= DISPLACEMENT_MIN_THRESHOLD:
        return True
    return False

# =============== MARKET STATE DETECTION ===============
def romeopt_market_state(df: pd.DataFrame, atr_val: float) -> str:
    """Determine market state (BALANCED/IMBALANCED)"""
    if len(df) < 3:
        return "BALANCED"
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    body_ratio = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    candle_size = last["high"] - last["low"]
    price_movement = abs(last["close"] - prev["close"])
    
    # RomeOPT displacement logic
    strong_displacement = (
        body_ratio > 0.7 and
        candle_size > atr_val * 1.2 and
        price_movement > atr_val * 0.5
    )
    
    return "IMBALANCED" if strong_displacement else "BALANCED"

# =============== LIQUIDITY DETECTION ===============
def romeopt_internal_liquidity(df: pd.DataFrame, side: str, atr_val: float, lookback: int = 15) -> Optional[float]:
    """Find internal liquidity clusters"""
    tolerance = atr_val * 0.15
    
    if side == "SELL":
        lows = df['low'].iloc[-lookback:].dropna()
        if len(lows) < 5:
            return None
        
        potential_targets = []
        for i in range(len(lows)):
            current_low = lows.iloc[i]
            nearby_count = (abs(lows - current_low) <= tolerance).sum()
            if nearby_count >= 2:
                potential_targets.append((current_low, nearby_count))
        
        if potential_targets:
            return min(potential_targets, key=lambda x: x[0])[0]
    
    else:  # BUY
        highs = df['high'].iloc[-lookback:].dropna()
        if len(highs) < 5:
            return None
        
        potential_targets = []
        for i in range(len(highs)):
            current_high = highs.iloc[i]
            nearby_count = (abs(highs - current_high) <= tolerance).sum()
            if nearby_count >= 2:
                potential_targets.append((current_high, nearby_count))
        
        if potential_targets:
            return max(potential_targets, key=lambda x: x[0])[0]
    
    return None

def romeopt_external_liquidity(df: pd.DataFrame, side: str, lookback: int = 50) -> Optional[float]:
    """Find external range extremes"""
    if side == "SELL":
        return df['low'].iloc[-lookback:].min()
    else:  # BUY
        return df['high'].iloc[-lookback:].max()

# =============== ORDER BLOCK DETECTION ===============
def find_latest_ob(df: pd.DataFrame, lookback: int = 50) -> Optional[Dict]:
    """Find the latest order block with enhanced classification"""
    blocks = []
    
    start_idx = max(2, len(df) - lookback)
    for i in range(start_idx, len(df) - 1):
        candle = df.iloc[i]
        prev_candle = df.iloc[i-1]
        
        # Bullish Order Block
        if (prev_candle["close"] < prev_candle["open"] and
            candle["close"] > candle["open"] and
            candle["close"] > prev_candle["close"]):
            
            block = {
                "type": "BULLISH_OB",
                "index": i,
                "low": min(candle["low"], prev_candle["low"]),
                "high": max(candle["close"], prev_candle["close"]),
                "body_low": min(candle["open"], candle["close"]),
                "body_high": max(candle["open"], candle["close"])
            }
            blocks.append(block)
        
        # Bearish Order Block
        elif (prev_candle["close"] > prev_candle["open"] and
              candle["close"] < candle["open"] and
              candle["close"] < prev_candle["close"]):
            
            block = {
                "type": "BEARISH_OB",
                "index": i,
                "low": min(candle["close"], prev_candle["close"]),
                "high": max(candle["high"], prev_candle["high"]),
                "body_low": min(candle["open"], candle["close"]),
                "body_high": max(candle["open"], candle["close"])
            }
            blocks.append(block)
    
    if blocks:
        latest_block = max(blocks, key=lambda x: x["index"])
        
        # Calculate block strength
        body_size = latest_block["body_high"] - latest_block["body_low"]
        candle_size = latest_block["high"] - latest_block["low"]
        body_ratio = body_size / candle_size if candle_size > 0 else 0
        
        if body_ratio >= 0.7:
            latest_block["strength"] = "STRONG"
        elif body_ratio >= 0.5:
            latest_block["strength"] = "MODERATE"
        else:
            latest_block["strength"] = "WEAK"
        
        latest_block["body_ratio"] = round(body_ratio, 2)
        
        return latest_block
    
    return None

# =============== HIGHER TIMEFRAME TP TARGETING ===============
async def get_higher_tf_liquidity(
    exchange, 
    symbol: str, 
    entry_tf: str, 
    side: str, 
    entry_price: float
) -> Optional[Tuple[float, str, str, Dict]]:
    """
    Find liquidity target on higher timeframe
    Returns: (tp_price, tp_tf_used, liquidity_type, htf_data)
    """
    available_tfs = TP_TF_MAPPING.get(entry_tf, [DEFAULT_TP_TF.get(entry_tf, "15m")])
    
    for tp_tf in available_tfs:
        try:
            # Fetch higher timeframe data
            ohlcv = await fetch_ohlcv(exchange, symbol, tp_tf, limit=200)
            if not ohlcv or len(ohlcv) < 50:
                continue
            
            df_htf = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
            
            # Calculate ATR on higher timeframe
            htf_atr_val = float(calculate_atr(df_htf, 14).iloc[-1])
            
            # Determine market state on higher TF
            htf_market_state = romeopt_market_state(df_htf, htf_atr_val)
            
            # Find appropriate liquidity
            tp = None
            liquidity_type = ""
            
            if htf_market_state == "BALANCED":
                tp = romeopt_internal_liquidity(df_htf, side, htf_atr_val, lookback=30)
                liquidity_type = "RANGE_CLUSTER"
            else:
                tp = romeopt_external_liquidity(df_htf, side, lookback=100)
                liquidity_type = "TREND_EXTREME"
            
            if tp is None:
                continue
            
            # Validate TP distance and direction
            if side == "BUY":
                if tp <= entry_price:
                    continue
                
                distance_pips = tp - entry_price
                distance_percent = (distance_pips / entry_price) * 100
                
                min_distance = entry_price * 0.003  # 0.3% minimum
                max_distance = entry_price * 0.03   # 3% maximum
                
                if distance_pips < min_distance or distance_pips > max_distance:
                    continue
                    
            else:  # SELL
                if tp >= entry_price:
                    continue
                
                distance_pips = entry_price - tp
                distance_percent = (distance_pips / entry_price) * 100
                
                min_distance = entry_price * 0.003
                max_distance = entry_price * 0.03
                
                if distance_pips < min_distance or distance_pips > max_distance:
                    continue
            
            # Check for recent sweeps
            recent_candles = min(20, len(df_htf))
            recent_touch = False
            
            if side == "SELL":
                recent_touch = any(
                    abs(df_htf['low'].iloc[-i] - tp) <= htf_atr_val * 0.15
                    for i in range(1, min(recent_candles, 10))
                )
            else:
                recent_touch = any(
                    abs(df_htf['high'].iloc[-i] - tp) <= htf_atr_val * 0.15
                    for i in range(1, min(recent_candles, 10))
                )
            
            if recent_touch:
                continue
            
            # Prepare HTF data
            htf_data = {
                "tf": tp_tf,
                "market_state": htf_market_state,
                "atr": round(htf_atr_val, 6),
                "range_low": float(df_htf['low'].iloc[-100:].min()),
                "range_high": float(df_htf['high'].iloc[-100:].max()),
                "current_price": float(df_htf['close'].iloc[-1]),
                "distance_pips": round(distance_pips, 6),
                "distance_percent": round(distance_percent, 2),
                "liquidity_type": liquidity_type
            }
            
            return tp, tp_tf, liquidity_type, htf_data
            
        except Exception as e:
            log.debug(f"Error analyzing {tp_tf} for {symbol}: {e}")
            continue
    
    # Fallback to default TF
    default_tf = DEFAULT_TP_TF.get(entry_tf, "15m")
    
    try:
        ohlcv = await fetch_ohlcv(exchange, symbol, default_tf, limit=100)
        if ohlcv and len(ohlcv) >= 30:
            df_default = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
            
            if side == "BUY":
                tp = df_default['high'].iloc[-50:].max()
                if tp <= entry_price:
                    tp = entry_price * 1.015  # 1.5% as fallback
            else:
                tp = df_default['low'].iloc[-50:].min()
                if tp >= entry_price:
                    tp = entry_price * 0.985  # 1.5% as fallback
            
            distance_pips = abs(tp - entry_price)
            distance_percent = (distance_pips / entry_price) * 100
            
            htf_data = {
                "tf": default_tf,
                "market_state": "FALLBACK",
                "atr": 0,
                "range_low": 0,
                "range_high": 0,
                "current_price": 0,
                "distance_pips": round(distance_pips, 6),
                "distance_percent": round(distance_percent, 2),
                "liquidity_type": "FALLBACK_TARGET"
            }
            
            return tp, default_tf, "FALLBACK", htf_data
    except Exception as e:
        log.debug(f"Fallback failed for {symbol}: {e}")
    
    # Ultimate fallback
    if side == "BUY":
        tp = entry_price * 1.01  # 1% target
    else:
        tp = entry_price * 0.99  # 1% target
    
    return tp, "1h", "ULTIMATE_FALLBACK", {}

# =============== ROMEOPT TP/SL CALCULATION (HTF VERSION) ===============
async def romeopt_tp_sl_htf(
    exchange, 
    entry: float, 
    side: str, 
    atr_val: float, 
    ob_zone: Dict, 
    df_entry: pd.DataFrame, 
    symbol: str, 
    entry_tf: str
) -> Optional[Tuple[float, float, str, str, Dict]]:
    """Calculate TP on higher timeframe, SL on entry timeframe"""
    
    # Get HTF liquidity target
    tp_result = await get_higher_tf_liquidity(exchange, symbol, entry_tf, side, entry)
    
    if not tp_result:
        log.debug(f"No HTF liquidity found for {side} {symbol}")
        return None
    
    tp_price, tp_tf, liquidity_type, htf_data = tp_result
    
    # Calculate Stop Loss based on entry timeframe
    if side == "BUY":
        # SL below OB zone
        sl = ob_zone["low"] - (atr_val * 0.3)
        recent_low = df_entry['low'].iloc[-10:].min()
        sl = min(sl, recent_low - (atr_val * 0.3))
        
        # Ensure minimum risk
        min_risk = atr_val * 0.5
        risk = entry - sl
        if risk < min_risk:
            risk = min_risk
            sl = entry - risk
        
        # Final validation
        if tp_price <= entry:
            return None
        
        reward = tp_price - entry
        if reward < risk * 0.5:
            return None
        
    else:  # SELL
        # SL above OB zone
        sl = ob_zone["high"] + (atr_val * 0.3)
        recent_high = df_entry['high'].iloc[-10:].max()
        sl = max(sl, recent_high + (atr_val * 0.3))
        
        min_risk = atr_val * 0.5
        risk = sl - entry
        if risk < min_risk:
            risk = min_risk
            sl = entry + risk
        
        if tp_price >= entry:
            return None
        
        reward = entry - tp_price
        if reward < risk * 0.5:
            return None
    
    tp_type = f"HTF_{tp_tf}_{liquidity_type}"
    
    log.info(f"✅ {side} {symbol} | Entry: {entry:.6f}")
    log.info(f"   Entry TF: {entry_tf} → TP TF: {tp_tf}")
    log.info(f"   SL: {sl:.6f} | TP: {tp_price:.6f}")
    log.info(f"   Risk: {risk:.6f} | R:R: {reward/risk:.2f}:1")
    
    return sl, tp_price, tp_type, tp_tf, htf_data

# =============== SIGNAL GENERATION ===============
async def generate_signal_romeopt_htf(
    exchange, 
    df: pd.DataFrame, 
    symbol: str, 
    tf: str
) -> Optional[Dict]:
    """Generate RomeOPT signal with HTF TP targeting"""
    
    if df is None or len(df) < 20:
        return None
    
    last = df.iloc[-1]
    score = 0
    reasons = []
    calc_values = {}
    
    # Step 1: Liquidity Sweep Detection
    lookback_period = 20
    high_lookback = df['high'].iloc[-lookback_period:-1]
    low_lookback = df['low'].iloc[-lookback_period:-1]
    
    sweep_high = last["high"] > high_lookback.max()
    sweep_low = last["low"] < low_lookback.min()
    
    sweep_type = "NONE"
    sweep_respected = False
    
    if sweep_high and last["close"] < high_lookback.max():
        sweep_type = "HIGH_SWEEP_RESPECTED"
        sweep_respected = True
        score += 2
    elif sweep_low and last["close"] > low_lookback.min():
        sweep_type = "LOW_SWEEP_RESPECTED"
        sweep_respected = True
        score += 2
    
    reasons.append(f"Sweep: {sweep_type} (+{2 if sweep_respected else 0})")
    calc_values["sweep_type"] = sweep_type
    calc_values["sweep_respected"] = sweep_respected
    
    # Step 2: Displacement
    displacement = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    calc_values["displacement_value"] = round(displacement, 2)
    
    if displacement > 0.6:
        score += 2
        reasons.append(f"Displacement +2 ({displacement:.2f})")
    else:
        reasons.append(f"Displacement +0 ({displacement:.2f})")
    
    # Step 3 & 4: Order Block Detection
    ob_zone = find_latest_ob(df, lookback=30)
    
    if not ob_zone:
        reasons.append("No OB detected")
        return None
    
    ob_type = "bullish" if ob_zone["type"] == "BULLISH_OB" else "bearish"
    calc_values["ob_type"] = ob_type
    calc_values["ob_strength"] = ob_zone.get("strength", "UNKNOWN")
    
    # Check zone approach
    zone_approach = 0
    if ob_type == "bullish" and last["close"] <= ob_zone["high"]:
        score += 1
        zone_approach = 1
        reasons.append("Zone Approach +1 (bullish)")
    elif ob_type == "bearish" and last["close"] >= ob_zone["low"]:
        score += 1
        zone_approach = 1
        reasons.append("Zone Approach +1 (bearish)")
    else:
        reasons.append("Zone Approach +0")
    
    calc_values["zone_approach"] = zone_approach
    
    # Step 5: HTF Alignment
    tf_map = {"1m": "15m", "3m": "30m", "5m": "1h", "15m": "4h", "30m": "1h"}
    htf = tf_map.get(tf, "15m")
    
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, htf, 50)
    htf_alignment = 0
    
    if ohlcv_htf and len(ohlcv_htf) >= 5:
        df_htf = pd.DataFrame(ohlcv_htf, columns=["ts", "open", "high", "low", "close", "vol"])
        trend = df_htf["close"].iloc[-1] - df_htf["close"].iloc[-5]
        htf_dir = "bullish" if trend > 0 else "bearish"
        
        if htf_dir == ob_type:
            score += 1
            htf_alignment = 1
            reasons.append(f"HTF Alignment +1 ({htf_dir})")
        else:
            reasons.append(f"HTF Alignment +0 ({htf_dir} vs {ob_type})")
    else:
        reasons.append("HTF Alignment ?")
    
    # Step 6: Momentum
    momentum_ratio = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    calc_values["momentum_value"] = round(momentum_ratio, 2)
    
    if ((ob_type == "bullish" and momentum_ratio >= 0.8 and last["close"] > last["open"]) or
        (ob_type == "bearish" and momentum_ratio >= 0.8 and last["close"] < last["open"])):
        score += 1
        reasons.append(f"Momentum +1 ({momentum_ratio:.2f})")
    else:
        reasons.append(f"Momentum +0 ({momentum_ratio:.2f})")
    
    # Critical filters
    critical_score = htf_alignment + (2 if sweep_respected else 0)
    if critical_score < CRITICAL_FACTORS_MIN:
        return None
    
    if score < MIN_SCORE:
        return None
    
    if displacement <= 0.6:
        return None
    
    if htf_alignment != 1:
        return None
    
    # Forced filter
    if not force_filter_trade(calc_values["momentum_value"], calc_values["displacement_value"]):
        reasons.append("Forced filter rejected")
        return None
    
    # Determine side
    side_str = "BUY" if ob_type == "bullish" else "SELL"
    entry = float(last["close"])
    
    # Elite MTF confirmation
    async def elite_tf_alignment(exchange, symbol: str, side: str) -> bool:
        tfs = ["15m", "1h", "4h"]
        for tf_check in tfs:
            ohlcv = await fetch_ohlcv(exchange, symbol, tf_check, 50)
            if not ohlcv or len(ohlcv) < 10:
                return False
            df_tf = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
            if len(df_tf) < 5:
                return False
            trend = df_tf["close"].iloc[-1] - df_tf["close"].iloc[-5]
            trend_side = "BUY" if trend > 0 else "SELL"
            if trend_side != side:
                return False
        return True
    
    if not await elite_tf_alignment(exchange, symbol, side_str):
        return None
    
    # Calculate TP/SL with HTF targeting
    atr_val = float(calculate_atr(df, 14).iloc[-1])
    result = await romeopt_tp_sl_htf(exchange, entry, side_str, atr_val, ob_zone, df, symbol, tf)
    
    if result is None:
        reasons.append("No valid TP/SL found")
        return None
    
    sl, tp, tp_type, tp_tf, htf_data = result
    
    # Calculate R:R ratio
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    rr_ratio = reward / risk if risk > 0 else 0
    
    sig = {
        "symbol": symbol,
        "side": side_str,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "score": score,
        "reason": " | ".join(reasons),
        "reason_list": reasons,
        "calc_values": calc_values,
        "tp_type": tp_type,
        "entry_tf": tf,
        "tp_tf": tp_tf,
        "htf_data": htf_data,
        "rr_ratio": round(rr_ratio, 2)
    }
    
    log.info(f"✅ Generated HTF signal: {symbol} {side_str} Score:{score}")
    return sig

# =============== SIGNAL LOGGING ===============
async def log_signal_htf(sig: Dict):
    """Log signal to database"""
    async with db_lock:
        try:
            # First check if table has all required columns
            async with db_conn.execute("PRAGMA table_info(signals)") as cursor:
                columns = await cursor.fetchall()
                column_names = [col[1] for col in columns]
            
            # Prepare data with fallbacks for missing columns
            htf_data = sig.get("htf_data", {})
            
            # Build the INSERT query dynamically based on available columns
            insert_columns = [
                "symbol", "side", "entry", "sl", "tp", "timestamp", 
                "status", "reason", "score", "latest_ob", "tp_type", 
                "tp_locked", "entry_tf", "tp_tf", "rr_ratio"
            ]
            
            insert_values = [
                sig["symbol"],
                sig["side"],
                sig["entry"],
                sig.get("sl"),
                sig.get("tp"),
                datetime.datetime.utcnow().isoformat(),
                "OPEN",
                sig.get("reason", ""),
                sig.get("score", 0),
                str(sig.get("calc_values", {}).get("ob_type", "")),
                sig.get("tp_type", ""),
                1,
                sig.get("entry_tf", ""),
                sig.get("tp_tf", ""),
                sig.get("rr_ratio", 0)
            ]
            
            # Add optional columns if they exist in the table
            if "tp_distance_pips" in column_names:
                insert_columns.append("tp_distance_pips")
                insert_values.append(htf_data.get("distance_pips", 0))
            
            if "tp_distance_percent" in column_names:
                insert_columns.append("tp_distance_percent")
                insert_values.append(htf_data.get("distance_percent", 0))
            
            # Build and execute the query
            columns_str = ", ".join(insert_columns)
            placeholders = ", ".join(["?" for _ in insert_columns])
            query = f"INSERT INTO signals ({columns_str}) VALUES ({placeholders})"
            
            await db_conn.execute(query, insert_values)
            await db_conn.commit()
            log.debug(f"Logged signal for {sig['symbol']}")
            
        except Exception as e:
            log.error(f"Error logging signal: {e}")
            # Try a simpler insert as fallback
            try:
                await db_conn.execute("""
                    INSERT INTO signals (symbol, side, entry, sl, tp, timestamp, status, reason, score)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    sig["symbol"],
                    sig["side"],
                    sig["entry"],
                    sig.get("sl"),
                    sig.get("tp"),
                    datetime.datetime.utcnow().isoformat(),
                    "OPEN",
                    sig.get("reason", ""),
                    sig.get("score", 0)
                ))
                await db_conn.commit()
                log.warning(f"Used fallback logging for {sig['symbol']}")
            except Exception as e2:
                log.error(f"Fallback logging also failed: {e2}")

# =============== COMPACT SIGNAL ALERT FORMAT ===============
async def send_compact_signal_alert(sig: Dict):
    """Send compact, professional signal alert"""
    
    calc = sig.get("calc_values", {})
    htf_data = sig.get("htf_data", {})
    
    # Key metrics
    risk = abs(sig['entry'] - sig.get('sl', 0))
    reward = abs(sig.get('tp', 0) - sig['entry'])
    rr_ratio = sig.get('rr_ratio', 0)
    
    # Signal direction emoji
    direction_emoji = "🟢" if sig['side'] == 'BUY' else "🔴"
    
    message_lines = [
        f"<b>{direction_emoji} ROMEOPT SIGNAL</b>",
        f"<code>{sig['symbol']} • {sig['side']} • Score: {sig.get('score', 0)}/6</code>",
        "",
        f"<b>Entry:</b> <code>{sig['entry']:.6f}</code>",
        f"<b>TF:</b> {sig.get('entry_tf')} → {sig.get('tp_tf')}",
        "",
        f"<b>SL:</b> <code>{format_price(sig.get('sl'))}</code>",
        f"<b>TP:</b> <code>{format_price(sig.get('tp'))}</code>",
        "",
        f"<b>Risk:</b> {risk:.6f}",
        f"<b>Reward:</b> {reward:.6f}",
        f"<b>R:R:</b> {rr_ratio:.2f}:1",
        "",
        f"<b>Displacement:</b> {calc.get('displacement_value', 0):.2f}",
        f"<b>Momentum:</b> {calc.get('momentum_value', 0):.2f}",
        f"<b>Sweep:</b> {calc.get('sweep_type', 'N/A')}",
        "",
        f"<i>HTF Target: {htf_data.get('liquidity_type', 'N/A')} • {htf_data.get('distance_percent', 0):.1f}% away</i>",
        f"<i>{datetime.datetime.utcnow().strftime('%H:%M:%S')} UTC</i>"
    ]
    
    await tg("\n".join(message_lines))

# =============== SIGNAL MONITORING ===============
async def monitor_signals():
    """Monitor open signals for TP/SL hits"""
    while True:
        try:
            async with db_lock:
                # Use a simpler query that doesn't depend on optional columns
                async with db_conn.execute(
                    "SELECT id, symbol, side, entry, sl, tp, tp_hit, status FROM signals WHERE status='OPEN'"
                ) as cursor:
                    rows = await cursor.fetchall()
                    
                for row in rows:
                    sig_id, symbol, side, entry, sl, tp, tp_hit, status = row
                    
                    # Skip if TP already hit
                    if tp_hit == 1:
                        continue
                    
                    # Get current price
                    try:
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get("last")
                        if last_price is None:
                            continue
                    except Exception:
                        continue
                    
                    # Check for TP/SL hits
                    hits = []
                    new_tp_hit = tp_hit
                    new_status = status
                    
                    if side == "BUY":
                        if tp is not None and last_price >= tp:
                            hits.append("TP")
                            new_tp_hit = 1
                        if sl is not None and last_price <= sl:
                            hits.append("SL")
                            new_status = "CLOSED"
                    else:  # SELL
                        if tp is not None and last_price <= tp:
                            hits.append("TP")
                            new_tp_hit = 1
                        if sl is not None and last_price >= sl:
                            hits.append("SL")
                            new_status = "CLOSED"
                    
                    # Send alert and update if hits occurred
                    if hits:
                        # Use helper function for proper formatting
                        sl_display = format_price(sl)
                        tp_display = format_price(tp)
                        
                        # Determine hit emoji
                        if "TP" in hits:
                            hit_emoji = "🎯"
                        elif "SL" in hits:
                            hit_emoji = "💥"
                        else:
                            hit_emoji = "📌"
                        
                        alert_msg = (
                            f"{hit_emoji} <b>SIGNAL HIT</b>\n"
                            f"<code>{symbol} {side}</code>\n"
                            f"Entry: {entry:.6f}\n"
                            f"Current: {last_price:.6f}\n"
                            f"Hits: {', '.join(hits)}\n"
                            f"SL: {sl_display}\n"
                            f"TP: {tp_display}"
                        )
                        await tg(alert_msg)
                        
                        # Record SL hits for deprioritization
                        if "SL" in hits:
                            record_sl_hit(symbol)
                    
                    # Update database if status changed
                    if new_tp_hit != tp_hit or new_status != status:
                        await db_conn.execute(
                            "UPDATE signals SET tp_hit=?, status=? WHERE id=?",
                            (new_tp_hit, new_status, sig_id)
                        )
                
                await db_conn.commit()
                
        except Exception as e:
            log.error(f"Monitor error: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

# =============== SL CLUSTER MANAGEMENT ===============
def record_sl_hit(symbol: str, lookback_minutes: int = 30):
    """Record SL hit for a symbol"""
    now = time.time()
    dq = recent_sl[symbol]
    dq.append(now)
    
    cutoff = now - (lookback_minutes * 60)
    while dq and dq[0] < cutoff:
        dq.popleft()

def deprioritized(symbol: str, threshold: int = 3, lookback: int = 30) -> bool:
    """Check if symbol should be deprioritized due to recent SL hits"""
    dq = recent_sl[symbol]
    now = time.time()
    cutoff = now - (lookback * 60)
    
    while dq and dq[0] < cutoff:
        dq.popleft()
    
    return len(dq) >= threshold

# =============== SCAN LOOP ===============
async def scan_loop_htf(exchange):
    """Main scanning loop with HTF TP targeting"""
    
    while True:
        start_time = time.time()
        
        try:
            # Fetch top volume pairs
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, v.get("quoteVolume", 0)) 
                         for s, v in tickers.items() 
                         if s.endswith("/USDT")]
            
            top_pairs = sorted(usdt_pairs, key=lambda x: x[1], reverse=True)[:TOP_N]
            
            signals_found = 0
            
            for symbol, volume in top_pairs:
                # Skip deprioritized symbols
                if deprioritized(symbol):
                    continue
                
                # Scan each timeframe
                for tf in TIMEFRAMES:
                    key = f"{symbol}:{tf}"
                    
                    # Rate limiting
                    if key in last_signal_time:
                        time_since_last = time.time() - last_signal_time[key]
                        if time_since_last < 60:  # 1 minute cooldown per symbol:TF
                            continue
                    
                    # Fetch data
                    ohlcv = await fetch_ohlcv(exchange, symbol, tf, 200)
                    if not ohlcv or len(ohlcv) < 50:
                        continue
                    
                    # Create DataFrame
                    df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
                    for col in ["open", "high", "low", "close", "vol"]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    
                    # Generate signal
                    sig = await generate_signal_romeopt_htf(exchange, df, symbol, tf)
                    
                    if sig:
                        # Send compact alert
                        await send_compact_signal_alert(sig)
                        
                        # Log to database
                        await log_signal_htf(sig)
                        
                        # Update rate limiting
                        last_signal_time[key] = time.time()
                        signals_found += 1
            
            log.info(f"📊 Scan complete. Signals found: {signals_found}")
            
        except Exception as e:
            log.error(f"Scan loop error: {e}")
        
        # Maintain scan interval
        elapsed = time.time() - start_time
        sleep_time = max(1, SCAN_INTERVAL - elapsed)
        await asyncio.sleep(sleep_time)

# =============== FASTAPI WEBHOOK ===============
app = FastAPI(title="RomeOPT HTF Scanner")

@app.get("/")
async def root():
    return {
        "status": "running",
        "service": "RomeOPT HTF Scanner",
        "version": "2.0.0",
        "tp_mapping": TP_TF_MAPPING
    }

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.datetime.utcnow().isoformat()}

@app.get("/signals")
async def get_signals(limit: int = 10, status: str = "OPEN"):
    async with db_lock:
        async with db_conn.execute(
            "SELECT * FROM signals WHERE status=? ORDER BY id DESC LIMIT ?",
            (status, limit)
        ) as cursor:
            columns = [description[0] for description in cursor.description]
            rows = await cursor.fetchall()
            
        signals = []
        for row in rows:
            signals.append(dict(zip(columns, row)))
        
        return {"signals": signals, "count": len(signals)}

@app.post("/webhook")
async def webhook(request: Request):
    """External webhook endpoint"""
    token = request.headers.get("X-Auth", "")
    if token != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    
    data = await request.json()
    log.info(f"Webhook received: {data}")
    
    return {"status": "received", "data": data}

# =============== MAIN FUNCTION ===============
async def main():
    """Main application entry point"""
    
    # Initialize database
    await init_db()
    
    # Initialize exchange
    global exchange
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {
            "defaultType": "spot"
        }
    })
    
    # Startup message
    startup_msg = [
        "🚀 <b>ROMEOPT HTF SCANNER STARTED</b> 🚀",
        "",
        "<b>🎯 HIGHER TIMEFRAME TP TARGETING:</b>",
        "• 1m → 15m/30m TP",
        "• 3m → 15m/30m TP", 
        "• 5m → 15m/30m TP",
        "• 15m → 30m/1h TP",
        "• 30m → 1h/4h TP",
        "",
        "<b>📊 COMPACT SIGNAL FORMAT</b>",
        "• Clean professional alerts",
        "• All key info at a glance",
        "• Mobile optimized",
        "",
        f"<b>Scan Interval:</b> {SCAN_INTERVAL}s",
        f"<b>Top Pairs:</b> {TOP_N}",
        f"<b>Timeframes:</b> {', '.join(TIMEFRAMES)}",
        "",
        "<i>🔒 TP locked on HTF liquidity • RomeOPT v2.0</i>"
    ]
    
    await tg("\n".join(startup_msg))
    log.info("RomeOPT HTF Scanner started successfully")
    
    # Start scanning and monitoring
    try:
        await asyncio.gather(
            scan_loop_htf(exchange),
            monitor_signals()
        )
    except KeyboardInterrupt:
        log.info("Shutdown requested")
    finally:
        # Cleanup
        if db_conn:
            await db_conn.close()
        if exchange:
            await exchange.close()
        log.info("Scanner shutdown complete")

# =============== COMMAND LINE INTERFACE ===============
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="RomeOPT HTF Scanner")
    parser.add_argument("--http", action="store_true", help="Start HTTP server")
    parser.add_argument("--port", type=int, default=9000, help="HTTP server port")
    
    args = parser.parse_args()
    
    if args.http:
        # Start FastAPI server
        log.info(f"Starting HTTP server on port {args.port}")
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=args.port,
            log_level="info"
        )
    else:
        # Start scanner
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Scanner stopped by user")
        except Exception as e:
            log.error(f"Fatal error: {e}")
            raise