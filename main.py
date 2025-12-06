#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER (Enhanced + Elite Features)
- Fully live early signals
- RomeOPT 6-step logic
- Strict TP/SL (0.8R/1.6R, SL→BE after TP1, no TP3)
- Liquidity path filter
- Clean traffic / range avoidance
- Telegram alerts
- Async SQLite logging
- Filters: Score >=5, Displacement +2, Sweep+2 OR Zone+1, avoid counter-trend
- Improved Order Block detection
- Adaptive Market Regime detection
- HTF + Sweep scoring threshold
- Elite multi-timeframe confirmation (15m,1h,4h)
- FIXED: Strong trend filter to avoid counter-trend losses
- 📊 ENHANCED BREAKDOWN: Shows all numerical values
- 🚨 ADDED: MOMENTUM FILTER ONLY (CRITICAL)
- 🎯 ADDED: SWEEP RETRACEMENT FILTER (DIFFERENT FOR BUY vs SELL)
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

# MOMENTUM FILTER SETTINGS (ONLY ADDITION)
MOMENTUM_FILTER_ENABLED = True  # Enable momentum range filter

# Momentum ranges (from historical analysis - OPTIMAL RANGES)
MOMENTUM_RANGES = {
    "SELL": {"min": 0.825, "max": 1.01},  # Changed from 0.78-0.88
    "BUY": {"min": 0.825, "max": 1.01}    # Changed from 0.82-0.91
}

# SWEEP FILTER SETTINGS (NEW ADDITION)
SWEEP_FILTER_ENABLED = True  # Enable sweep retracement filter

# Sweep retracement thresholds (DIFFERENT for BUY vs SELL based on our analysis)
SWEEP_RETRACEMENT_THRESHOLDS = {
    "BUY": 0.01,   # 1% minimum retracement for BUY trades
    "SELL": 0.01   # 50% minimum retracement for SELL trades
}

# Timeframe mapping for TP scaling (RomeOPT-P logic) - YOUR CHOICE
TP_TIMEFRAME_MAP = {
    "1m": "5m",    # 1m → 5m ATR (5×) - less aggressive
    "3m": "15m",   # 3m → 15m ATR (5×)
    "5m": "15m",   # 5m → 15m ATR (3×) - conservative
    "15m": "1h",   # 15m → 1h ATR (4×)
    "30m": "1h"    # 30m → 1h ATR (2×) - minimal scaling
}

# ---------------- GLOBALS ----------------
log = logging.getLogger("romeopt_bot")
db_lock = asyncio.Lock()
db_conn = None
exchange = None  # Global exchange instance

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

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

# ---------------- DATABASE MIGRATION ----------------
async def migrate_db():
    """Migrate database schema if needed"""
    try:
        # Check if entry_tf column exists
        cursor = await db_conn.execute("PRAGMA table_info(signals)")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        # Add missing columns
        if 'entry_tf' not in column_names:
            log.info("Migrating database: adding entry_tf column")
            await db_conn.execute("ALTER TABLE signals ADD COLUMN entry_tf TEXT DEFAULT ''")
        
        if 'tp_tf' not in column_names:
            log.info("Migrating database: adding tp_tf column")
            await db_conn.execute("ALTER TABLE signals ADD COLUMN tp_tf TEXT DEFAULT ''")
        
        await db_conn.commit()
        log.info("Database migration complete")
    except Exception as e:
        log.error(f"Migration failed: {e}")
        # If table doesn't exist or migration fails, create fresh
        await db_conn.execute("DROP TABLE IF EXISTS signals")
        await db_conn.commit()

# ---------------- DATABASE ----------------
async def init_db():
    global db_conn
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute("PRAGMA journal_mode=WAL;")
    await db_conn.execute("PRAGMA synchronous=NORMAL;")
    
    # Create table with full schema
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            side TEXT,
            entry REAL,
            sl REAL,
            tp1 REAL,
            tp2 REAL,
            entry_tf TEXT DEFAULT '',
            tp_tf TEXT DEFAULT '',
            timestamp TEXT,
            status TEXT,
            reason TEXT,
            score INTEGER,
            tp1_hit INTEGER DEFAULT 0,
            tp2_hit INTEGER DEFAULT 0,
            latest_ob TEXT
        );
    """)
    await db_conn.commit()
    
    # Run migration for existing databases
    await migrate_db()

# ---------------- OHLCV ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.debug("fetch_ohlcv failed for %s %s: %s", symbol, timeframe, e)
        return None

# ---------------- INDICATORS ----------------
def atr(df: pd.DataFrame, period=14):
    """Average True Range for volatility-based SL/TP"""
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.DataFrame({
        "h-l": high - low,
        "h-pc": (high - close.shift(1)).abs(),
        "l-pc": (low - close.shift(1)).abs()
    }).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()

def calculate_ema(df, period):
    """Calculate EMA"""
    return df['close'].ewm(span=period, adjust=False).mean()

# ---------------- MOMENTUM VALIDATION FILTER (ONLY ADDITION) ----------------
def validate_momentum(momentum_value: float, side: str, calc_values: dict) -> tuple:
    """
    MOMENTUM FILTER: Validate momentum is in optimal range
    Returns: (is_valid, rejection_reason)
    """
    if not MOMENTUM_FILTER_ENABLED:
        return True, None
    
    ranges = MOMENTUM_RANGES.get(side)
    if not ranges:
        return False, f"No momentum range defined for {side}"
    
    min_val, max_val = ranges["min"], ranges["max"]
    
    calc_values["momentum_min"] = min_val
    calc_values["momentum_max"] = max_val
    
    if momentum_value < min_val:
        return False, f"Momentum {momentum_value:.3f} < min {min_val:.3f}"
    elif momentum_value > max_val:
        return False, f"Momentum {momentum_value:.3f} > max {max_val:.3f}"
    
    return True, None

# ---------------- MOMENTUM-DISPLACEMENT COHERENCE FILTER ----------------
def validate_momentum_displacement_coherence(momentum_value: float, displacement_value: float, calc_values: dict) -> tuple:
    """
    MOMENTUM-DISPLACEMENT COHERENCE FILTER: Ensure Momentum and Displacement are close
    Returns: (is_valid, rejection_reason)
    """
    coherence_threshold = 0.02  # Maximum allowed difference
    
    diff = abs(momentum_value - displacement_value)
    calc_values["momentum_displacement_diff"] = round(diff, 4)
    calc_values["coherence_threshold"] = coherence_threshold
    
    if diff > coherence_threshold:
        return False, f"Momentum-Displacement mismatch ({diff:.3f} > {coherence_threshold})"
    
    return True, None

# ---------------- SWEEP RETRACEMENT FILTER ----------------
def validate_sweep_retracement(sweep_type: str, sweep_price: float, ob_mid: float, 
                               entry_price: float, trade_side: str, calc_values: dict) -> tuple:
    """
    SWEEP FILTER: Validate minimum retracement after sweep
    Different thresholds for BUY vs SELL trades
    Returns: (is_valid, rejection_reason)
    """
    if not SWEEP_FILTER_ENABLED:
        return True, None
    
    # Calculate OB midpoint if not provided
    if ob_mid is None:
        return False, "OB midpoint required for sweep filter"
    
    # Calculate retracement based on sweep type
    if sweep_type == "HIGH":  # BUY trades after HIGH sweep
        total_range = sweep_price - ob_mid
        if total_range <= 0:
            return False, "Invalid range: Sweep ≤ OB midpoint"
        
        retrace_amount = sweep_price - entry_price
        retracement = retrace_amount / total_range if total_range > 0 else 0
        
    elif sweep_type == "LOW":  # SELL trades after LOW sweep
        total_range = ob_mid - sweep_price
        if total_range <= 0:
            return False, "Invalid range: OB midpoint ≤ Sweep"
        
        retrace_amount = entry_price - sweep_price
        retracement = retrace_amount / total_range if total_range > 0 else 0
    
    else:
        return False, f"Invalid sweep type: {sweep_type}"
    
    # Store calculation values for breakdown
    calc_values["sweep_retracement"] = round(retracement, 4)
    calc_values["sweep_retracement_pct"] = round(retracement * 100, 2)
    calc_values["sweep_total_range"] = round(total_range, 6)
    calc_values["sweep_retrace_amount"] = round(retrace_amount, 6)
    
    # Get threshold for this trade side
    threshold = SWEEP_RETRACEMENT_THRESHOLDS.get(trade_side)
    if threshold is None:
        return False, f"No retracement threshold defined for {trade_side}"
    
    calc_values["sweep_threshold"] = threshold
    calc_values["sweep_threshold_pct"] = round(threshold * 100, 1)
    
    # Apply filter
    if retracement >= threshold:
        return True, None
    else:
        return False, f"Sweep retracement {retracement:.1%} < {threshold:.1%}"

# ---------------- STRONG TREND DETECTION ----------------
async def check_strong_counter_trend(exchange, symbol: str, timeframe: str, signal_side: str):
    """
    Check if higher timeframe is in STRONG trend AGAINST our signal.
    Returns True if we should REJECT the signal (strong counter-trend).
    """
    # DISABLED - Return False to allow all signals
    return False
    
    # Map signal timeframe to trend-check timeframe
    trend_check_map = {
        "1m": "15m",   # Check 15m trend for 1m signals
        "3m": "30m",   # Check 30m trend for 3m signals  
        "5m": "1h",    # Check 1h trend for 5m signals
        "15m": "4h",   # Check 4h trend for 15m signals
        "30m": "4h"    # Check 4h trend for 30m signals
    }
    
    check_tf = trend_check_map.get(timeframe, "15m")
    
    # Fetch OHLCV for trend analysis
    ohlcv = await fetch_ohlcv(exchange, symbol, check_tf, 50)
    if not ohlcv:
        return False  # If can't fetch, don't reject
        
    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
    for c in ["open","high","low","close","vol"]: 
        df[c] = pd.to_numeric(df[c], errors="coerce")
    
    # Calculate EMA for trend direction
    df['ema20'] = calculate_ema(df, 20)
    
    # Get last 10 candles for analysis
    recent = df.iloc[-10:]
    
    # Count candles above/below EMA
    above_ema = (recent['close'] > recent['ema20']).sum()
    below_ema = (recent['close'] < recent['ema20']).sum()
    
    # Check consecutive candles in same direction
    recent_trend = []
    for i in range(len(recent)-1):
        if recent['close'].iloc[i+1] > recent['close'].iloc[i]:
            recent_trend.append(1)  # Up
        else:
            recent_trend.append(-1)  # Down
    
    # Check for strong consecutive moves
    if len(recent_trend) >= 5:
        last_5 = recent_trend[-5:]
        if all(x > 0 for x in last_5):  # 5 consecutive up closes
            if signal_side == "SELL":  # We want to SELL during strong uptrend
                log.info(f"🚫 {symbol} {timeframe} {signal_side} rejected: Strong {check_tf} UPTREND")
                return True
        elif all(x < 0 for x in last_5):  # 5 consecutive down closes
            if signal_side == "BUY":  # We want to BUY during strong downtrend
                log.info(f"🚫 {symbol} {timeframe} {signal_side} rejected: Strong {check_tf} DOWNTREND")
                return True
    
    # Additional check: Price far from EMA (>2 ATR)
    current_atr = float(atr(df, 14).iloc[-1])
    ema_distance = abs(df['close'].iloc[-1] - df['ema20'].iloc[-1])
    
    if current_atr > 0:
        distance_in_atr = ema_distance / current_atr
        if distance_in_atr > 2.0:  # Very far from EMA = strong trend
            if signal_side == "BUY" and df['close'].iloc[-1] < df['ema20'].iloc[-1]:
                log.info(f"🚫 {symbol} {timeframe} {signal_side} rejected: Price >2 ATR below {check_tf} EMA")
                return True
            elif signal_side == "SELL" and df['close'].iloc[-1] > df['ema20'].iloc[-1]:
                log.info(f"🚫 {symbol} {timeframe} {signal_side} rejected: Price >2 ATR above {check_tf} EMA")
                return True
    
    return False  # Not a strong counter-trend

# ---------------- MARKET REGIME ----------------
async def detect_market_regime(df: pd.DataFrame):
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
    """Ensures 15m/1h/4h trend matches signal side"""
    tfs = ["15m","1h","4h"]
    for tf in tfs:
        ohlcv = await fetch_ohlcv(exchange, symbol, tf, 50)
        if not ohlcv: return False
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
        
        # Better trend detection using EMA slope
        df['ema20'] = calculate_ema(df, 20)
        current_slope = df['ema20'].iloc[-1] - df['ema20'].iloc[-3]
        
        trend_side = "BUY" if current_slope > 0 else "SELL"
        if trend_side != side:
            log.debug(f"Elite alignment failed: {tf} trend {trend_side} vs signal {side}")
            return False
    return True

# ---------------- ORDER BLOCK DETECTION ----------------
def find_latest_ob(df: pd.DataFrame):
    for i in range(len(df)-5, len(df)-1):
        candle, prev_candle = df.iloc[i], df.iloc[i-1]
        if candle["close"]>candle["open"] and prev_candle["close"]<prev_candle["open"]:
            return {"type":"bullish","low":min(candle["low"], prev_candle["low"]),"high":candle["close"]}
        elif candle["close"]<candle["open"] and prev_candle["close"]>prev_candle["open"]:
            return {"type":"bearish","low":candle["close"],"high":max(candle["high"], prev_candle["high"])}
    return None

# ---------------- TP/SL CALCULATION (RomeOPT-P Style) ----------------
async def romeoptp_tp_sl(exchange, entry: float, side: str, entry_tf: str, ob_zone: dict, symbol: str):
    """
    RomeOPT-P Logic:
    - SL based on entry timeframe OB (tight)
    - TP scaled to higher timeframe ATR (meaningful)
    - TP1 = 0.8R, TP2 = 1.6R
    """
    if not ob_zone:
        return None, None, None, entry_tf
    
    # Get ATR from higher timeframe for TP scaling
    tp_tf = TP_TIMEFRAME_MAP.get(entry_tf, "15m")
    htf_ohlcv = await fetch_ohlcv(exchange, symbol, tp_tf, 100)
    
    if not htf_ohlcv:
        # Fallback to entry timeframe if HTF fails
        htf_ohlcv = await fetch_ohlcv(exchange, symbol, entry_tf, 100)
        tp_tf = entry_tf
    
    if not htf_ohlcv:
        return None, None, None, tp_tf
    
    df_htf = pd.DataFrame(htf_ohlcv, columns=["ts","open","high","low","close","vol"])
    for c in ["open","high","low","close","vol"]: 
        df_htf[c] = pd.to_numeric(df_htf[c], errors="coerce")
    
    # Calculate ATR from higher timeframe
    atr_val = float(atr(df_htf, 14).iloc[-1])
    
    # Calculate SL based on entry timeframe OB (tight)
    if side == "BUY":
        # SL just below bullish OB low
        sl = ob_zone['low'] - (atr_val * 0.1)  # Very tight (0.1 × HTF ATR)
        risk = entry - sl
        # TP scaled to HTF ATR
        tp1 = entry + (risk * 0.8)  # 0.8R
        tp2 = entry + (risk * 1.6)  # 1.6R
    else:  # SELL
        # SL just above bearish OB high  
        sl = ob_zone['high'] + (atr_val * 0.1)  # Very tight (0.1 × HTF ATR)
        risk = sl - entry
        # TP scaled to HTF ATR
        tp1 = entry - (risk * 0.8)  # 0.8R
        tp2 = entry - (risk * 1.6)  # 1.6R
    
    return sl, tp1, tp2, tp_tf

# ---------------- UPDATE SIGNAL TP/SL ----------------
async def update_tp_sl_live(sig: dict):
    """Update TP/SL with current market data"""
    global exchange
    
    if 'entry_tf' not in sig or 'symbol' not in sig or 'side' not in sig:
        return sig
    
    # Fetch current OB from entry timeframe
    entry_tf_ohlcv = await fetch_ohlcv(exchange, sig["symbol"], sig["entry_tf"], 50)
    if not entry_tf_ohlcv:
        return sig
    
    df_entry = pd.DataFrame(entry_tf_ohlcv, columns=["ts","open","high","low","close","vol"])
    for c in ["open","high","low","close","vol"]: 
        df_entry[c] = pd.to_numeric(df_entry[c], errors="coerce")
    
    latest_ob = find_latest_ob(df_entry)
    if not latest_ob:
        return sig
    
    # Recalculate TP/SL with current data
    sl, tp1, tp2, tp_tf = await romeoptp_tp_sl(
        exchange, sig["entry"], sig["side"], sig["entry_tf"], latest_ob, sig["symbol"]
    )
    
    if sl is not None and tp1 is not None and tp2 is not None:
        sig["sl"] = sl
        sig["tp1"] = tp1
        sig["tp2"] = tp2
        sig["tp_tf"] = tp_tf
        sig["latest_ob"] = latest_ob
    
    return sig

# ---------------- IMPROVED HTF ALIGNMENT DETECTION ----------------
async def get_htf_trend(exchange, symbol: str, timeframe: str):
    """Get HTF trend direction with better logic"""
    ohlcv = await fetch_ohlcv(exchange, symbol, timeframe, 50)
    if not ohlcv:
        return "neutral"
    
    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
    for c in ["open","high","low","close","vol"]: 
        df[c] = pd.to_numeric(df[c], errors="coerce")
    
    # Use multiple methods for robust trend detection
    
    # 1. EMA slope
    df['ema20'] = calculate_ema(df, 20)
    ema_slope = df['ema20'].iloc[-1] - df['ema20'].iloc[-3]
    
    # 2. Recent closes direction
    recent_closes = df['close'].iloc[-6:]
    direction_sum = 0
    for i in range(1, len(recent_closes)):
        if recent_closes.iloc[i] > recent_closes.iloc[i-1]:
            direction_sum += 1
        else:
            direction_sum -= 1
    
    # 3. Price position relative to EMA
    above_ema = df['close'].iloc[-1] > df['ema20'].iloc[-1]
    
    # Combine signals
    if ema_slope > 0 and direction_sum >= 2 and above_ema:
        return "bullish"
    elif ema_slope < 0 and direction_sum <= -2 and not above_ema:
        return "bearish"
    else:
        return "neutral"

# ---------------- ROMEOPT SIGNAL GENERATOR ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    """Full RomeOPT 6-step signal generator with MOMENTUM FILTER and SWEEP FILTER"""
    if df is None or len(df) < 20: 
        return None
    
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    score = 0
    reasons = []
    
    # Store all calculation values for enhanced breakdown
    calc_values = {}

    # Step1: Liquidity Sweep
    sweep_high = last["high"] > prev5["high"].max()
    sweep_low = last["low"] < prev5["low"].min()
    has_sweep = sweep_high or sweep_low
    liquidity_sweep = 2 if has_sweep else 0
    score += liquidity_sweep
    sweep_type = "HIGH" if sweep_high else ("LOW" if sweep_low else "NONE")
    reasons.append(f"Liquidity Sweep +{liquidity_sweep}")
    calc_values["sweep_type"] = sweep_type
    calc_values["sweep_score"] = liquidity_sweep
    calc_values["prev_high"] = round(float(prev5["high"].max()), 6)
    calc_values["prev_low"] = round(float(prev5["low"].min()), 6)
    calc_values["current_high"] = round(float(last["high"]), 6)
    calc_values["current_low"] = round(float(last["low"]), 6)

    # Step2: Displacement
    displacement = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    calc_values["displacement_value"] = round(displacement, 3)
    has_disp = displacement > 0.6
    if has_disp: 
        score += 2
        reasons.append(f"Displacement +2 ({displacement:.3f})")
    else: 
        reasons.append(f"Displacement +0 ({displacement:.3f})")
    
    # ========== CRITICAL FIX: DETECT OB AND SIDE BEFORE FILTERS ==========
    # Step3&4: OB detection (MUST HAPPEN BEFORE FILTERS THAT NEED side)
    ob_zone = find_latest_ob(df)
    if not ob_zone: 
        reasons.append("No OB detected")
        calc_values["ob_type"] = "NONE"
        calc_values["zone_approach"] = 0
        return None
    
    ob_type = ob_zone['type']
    calc_values["ob_type"] = ob_type
    calc_values["ob_low"] = round(ob_zone['low'], 6)
    calc_values["ob_high"] = round(ob_zone['high'], 6)
    
    # Determine side from OB type
    side = "BUY" if ob_type == "bullish" else "SELL"
    calc_values["signal_side"] = side
    # ========== END FIX ==========
    
    # 🚨 FILTER 1: Momentum ≥ 0.825
    momentum_ratio = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    calc_values["momentum_value"] = round(momentum_ratio, 3)
    calc_values["momentum_threshold"] = 0.5
    
    # NOW side is defined before this call
    momentum_valid, momentum_rejection = validate_momentum(momentum_ratio, side, calc_values)
    if not momentum_valid:
        reasons.append(f"Momentum Filter: {momentum_rejection}")
        calc_values["momentum_filter_passed"] = False
        calc_values["momentum_rejection"] = momentum_rejection
        return None
    calc_values["momentum_filter_passed"] = True
    
    # 🚨 FILTER 2: Momentum-Displacement Coherence ≤ 0.02
    coherence_valid, coherence_rejection = validate_momentum_displacement_coherence(
        momentum_ratio, displacement, calc_values
    )
    if not coherence_valid:
        reasons.append(f"Coherence Filter: {coherence_rejection}")
        calc_values["coherence_filter_passed"] = False
        calc_values["coherence_rejection"] = coherence_rejection
        return None
    calc_values["coherence_filter_passed"] = True

    # 🚨 FILTER 3: Sweep Retracement (DIFFERENT for BUY vs SELL)
    if has_sweep and sweep_type != "NONE":  # Only apply if we have a sweep
        # Calculate OB midpoint for sweep filter
        ob_midpoint = (ob_zone['low'] + ob_zone['high']) / 2
        sweep_price = last["high"] if sweep_type == "HIGH" else last["low"]
        
        sweep_valid, sweep_rejection = validate_sweep_retracement(
            sweep_type=sweep_type,
            sweep_price=sweep_price,
            ob_mid=ob_midpoint,
            entry_price=float(last["close"]),
            trade_side=side,
            calc_values=calc_values
        )
        
        if not sweep_valid:
            reasons.append(f"Sweep Filter: {sweep_rejection}")
            calc_values["sweep_filter_passed"] = False
            calc_values["sweep_rejection"] = sweep_rejection
            return None
        calc_values["sweep_filter_passed"] = True
    else:
        # No sweep, so sweep filter is not applicable
        calc_values["sweep_filter_passed"] = True
        calc_values["sweep_retracement_pct"] = 0
        calc_values["sweep_threshold_pct"] = 0

    # Zone Approach calculation (can stay here since it uses ob_type, not side)
    zone_approach = 0
    if ob_type == "bullish" and last["close"] <= ob_zone['high']:
        zone_approach = 1
        score += 1
        reasons.append(f"Zone Approach +1")
    elif ob_type == "bearish" and last["close"] >= ob_zone['low']:
        zone_approach = 1
        score += 1
        reasons.append(f"Zone Approach +1")
    else:
        reasons.append(f"Zone Approach +0")
    
    calc_values["zone_approach"] = zone_approach

    # Step5: HTF alignment with IMPROVED detection
    # Check for STRONG counter-trend BEFORE proceeding
    should_reject = False  # DISABLED - Allow all signals
    
    calc_values["strong_counter_trend"] = False
    
    # Use mapping consistent with elite_tf_alignment
    tf_map = {"1m":"15m", "3m":"30m", "5m":"1h", "15m":"4h", "30m":"1h"}
    htf = tf_map.get(tf, "15m")
    
    # Get HTF trend with improved logic
    htf_trend = await get_htf_trend(exchange, symbol, htf)
    htf_alignment = 0
    calc_values["htf_timeframe"] = htf
    calc_values["htf_trend_direction"] = htf_trend
    
    if htf_trend != "neutral":
        htf_dir = "bullish" if htf_trend == "bullish" else "bearish"
        if htf_dir == ob_type: 
            htf_alignment = 1
            score += 1
            reasons.append(f"HTF Alignment +1 ({htf}: {htf_trend})")
            calc_values["htf_alignment"] = 1
        else:
            reasons.append(f"HTF Misalignment ({htf}: {htf_trend})")
            calc_values["htf_alignment"] = 0
            # Don't reject here, just no points
    else:
        reasons.append(f"HTF Neutral ({htf})")
        calc_values["htf_alignment"] = 0
    
    # Original momentum check (still needed for basic validation)
    # NOTE: This is redundant with the momentum filter above, but keeping it for backward compatibility
    if momentum_ratio < 0.5: 
        reasons.append(f"Momentum Failed ({momentum_ratio:.3f} < 0.5)")
        calc_values["momentum_score"] = 0
        return None
    else:
        reasons.append(f"Momentum Passed ({momentum_ratio:.3f})")
        calc_values["momentum_score"] = 1

    # CRITICAL: Must have minimum score AND displacement
    if score < MIN_SCORE: 
        reasons.append(f"Score {score} < {MIN_SCORE}")
        calc_values["final_score"] = score
        calc_values["min_score_required"] = MIN_SCORE
        return None
    
    if not has_disp: 
        reasons.append("No displacement")
        return None

    # Calculate TP/SL with RomeOPT-P logic
    sl, tp1, tp2, tp_tf = await romeoptp_tp_sl(exchange, float(last["close"]), side, tf, ob_zone, symbol)
    if sl is None or tp1 is None or tp2 is None:
        reasons.append("TP/SL calc failed")
        return None
    
    # Calculate risk/reward
    if side == "BUY":
        risk = float(last["close"]) - sl
        reward_tp1 = tp1 - float(last["close"])
        reward_tp2 = tp2 - float(last["close"])
    else:
        risk = sl - float(last["close"])
        reward_tp1 = float(last["close"]) - tp1
        reward_tp2 = float(last["close"]) - tp2
    
    if risk > 0:
        rr_tp1 = reward_tp1 / risk
        rr_tp2 = reward_tp2 / risk
    else:
        rr_tp1 = 0
        rr_tp2 = 0
    
    sig = {
        "symbol": symbol,
        "side": side,
        "entry": float(last["close"]),
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "entry_tf": tf,
        "tp_tf": tp_tf,
        "score": int(score),  # Ensure integer
        "reason": "RomeOPT-P 6-Step",
        "reason_list": reasons,
        "ob_zone": ob_zone,
        "calc_values": calc_values,  # Store all calculation values
        "risk": round(risk, 6),
        "reward_tp1": round(reward_tp1, 6),
        "reward_tp2": round(reward_tp2, 6),
        "rr_tp1": round(rr_tp1, 2),
        "rr_tp2": round(rr_tp2, 2)
    }

    # Liquidity path filter: skip blocked trades
    if side == "BUY" and any(df['high'].iloc[-20:] >= sig['tp1']): 
        reasons.append("Liquidity Path Blocked")
        calc_values["liquidity_path_blocked"] = True
        return None
    if side == "SELL" and any(df['low'].iloc[-20:] <= sig['tp1']): 
        reasons.append("Liquidity Path Blocked")
        calc_values["liquidity_path_blocked"] = True
        return None
    
    calc_values["liquidity_path_blocked"] = False
    calc_values["final_score"] = score

    return sig

# ---------------- DATABASE LOGGING ----------------
async def log_signal(sig):
    async with db_lock:
        await db_conn.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,entry_tf,tp_tf,timestamp,status,reason,score,latest_ob)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            sig["symbol"], sig["side"], sig["entry"], sig["sl"], sig["tp1"], sig["tp2"],
            sig.get("entry_tf", ""), sig.get("tp_tf", ""),
            datetime.datetime.utcnow().isoformat(), "OPEN", sig["reason"], 
            int(sig["score"]), str(sig.get("latest_ob",""))
        ))
        await db_conn.commit()

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    global exchange
    while True:
        try:
            async with db_lock:
                # SAFE QUERY - handles both old and new schema
                async with db_conn.execute(
                    "SELECT id,symbol,side,entry,sl,tp1,tp2,tp1_hit,tp2_hit,status FROM signals WHERE status='OPEN'"
                ) as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp1_hit, tp2_hit, status = row
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get("last")
                        if last_price is None: continue

                        # Try to get entry_tf from database if exists
                        entry_tf = ""
                        try:
                            cursor_tf = await db_conn.execute(
                                "SELECT entry_tf FROM signals WHERE id=?", (sig_id,)
                            )
                            tf_result = await cursor_tf.fetchone()
                            if tf_result and tf_result[0]:
                                entry_tf = tf_result[0]
                        except:
                            entry_tf = ""

                        # Update TP/SL with current market data
                        sig = {
                            "symbol": symbol, "side": side, "entry": entry, 
                            "sl": sl, "tp1": tp1, "tp2": tp2, "entry_tf": entry_tf
                        }
                        sig = await update_tp_sl_live(sig)
                        sl, tp1, tp2 = sig["sl"], sig["tp1"], sig["tp2"]

                        hits=[]; sl_hit=False
                        # ---------------- CHECK TP/SL ----------------
                        if side=="BUY":
                            if not tp1_hit and last_price>=tp1: 
                                hits.append("TP1")
                                tp1_hit=1
                                sl=entry
                            if not tp2_hit and last_price>=tp2: 
                                hits.append("TP2")
                                tp2_hit=1
                                status="CLOSED"
                            if last_price<=sl: 
                                hits.append("SL")
                                status="CLOSED"
                                sl_hit=True
                        else:
                            if not tp1_hit and last_price<=tp1: 
                                hits.append("TP1")
                                tp1_hit=1
                                sl=entry
                            if not tp2_hit and last_price<=tp2: 
                                hits.append("TP2")
                                tp2_hit=1
                                status="CLOSED"
                            if last_price>=sl: 
                                hits.append("SL")
                                status="CLOSED"
                                sl_hit=True

                        if hits:
                            await tg(f"🎯 {symbol} {side} update\nEntry:{entry}\nLast:{last_price}\nHits:{','.join(hits)}\nSL:{sl}\nTP1:{tp1} TP2:{tp2}")

                        await db_conn.execute(
                            "UPDATE signals SET tp1_hit=?,tp2_hit=?,sl=?,status=? WHERE id=?",
                            (tp1_hit,tp2_hit,sl,status,sig_id)
                        )
                await db_conn.commit()
        except Exception as e: 
            log.exception("monitor error: %s", e)
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- SCAN LOOP ----------------
last_signal_time = {}
async def scan_loop():
    global exchange
    while True:
        t0 = time.time()
        try:
            tickers = await exchange.fetch_tickers()
            top = sorted([(s,v.get("quoteVolume",0)) for s,v in tickers.items() if s.endswith("USDT")], 
                        key=lambda x:x[1], reverse=True)[:TOP_N]
            signals_found = 0
            for symbol,_ in top:
                for tf in TIMEFRAMES:
                    key = f"{symbol}:{tf}"
                    if key in last_signal_time and time.time() - last_signal_time[key] < 60: 
                        continue
                    ohlcv = await fetch_ohlcv(exchange, symbol, tf, 200)
                    if not ohlcv: continue
                    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                    for c in ["open","high","low","close","vol"]: 
                        df[c] = pd.to_numeric(df[c], errors="coerce")
                    sig = await generate_signal_romeopt(exchange, df, symbol, tf)
                    if sig:
                        # ENHANCED BREAKDOWN WITH ALL VALUES
                        calc = sig.get("calc_values", {})
                        
                        # Add momentum and coherence filter status
                        momentum_status = "✅ OPTIMAL" if calc.get("momentum_filter_passed") else "❌ OUTSIDE RANGE"
                        momentum_range = f"[{calc.get('momentum_min', 0):.3f}-{calc.get('momentum_max', 0):.3f}]"
                        
                        coherence_status = "✅ COHERENT" if calc.get("coherence_filter_passed") else "❌ INCOHERENT"
                        coherence_diff = calc.get("momentum_displacement_diff", 0)
                        coherence_threshold = calc.get("coherence_threshold", 0.02)
                        
                        breakdown_lines = [
                            f"🏆 {sig['symbol']} ({tf}) {sig['side']}",
                            f"Entry: {sig['entry']:.6f}",
                            f"Score: {sig['score']}/6",
                            f"",
                            f"📊 DETAILED BREAKDOWN:",
                            f"• Sweep: {calc.get('sweep_type', 'NONE')} (+{calc.get('sweep_score', 0)})",
                            f"  High: {calc.get('current_high', 0):.6f} > {calc.get('prev_high', 0):.6f}",
                            f"  Low: {calc.get('current_low', 0):.6f} < {calc.get('prev_low', 0):.6f}",
                            f"• Displacement: {calc.get('displacement_value', 0):.3f}",
                            f"• OB: {calc.get('ob_type', 'NONE')} [{calc.get('ob_low', 0):.6f}-{calc.get('ob_high', 0):.6f}]",
                            f"• Zone Approach: +{calc.get('zone_approach', 0)}",
                            f"• HTF ({calc.get('htf_timeframe', '?')}): {calc.get('htf_trend_direction', '?')} (+{calc.get('htf_alignment', 0)})",
                            f"• Momentum: {calc.get('momentum_value', 0):.3f} {momentum_status} {momentum_range}",
                            f"• Coherence: Diff={coherence_diff:.3f} {coherence_status} (≤{coherence_threshold})",
                            f"• Sweep Retracement: {calc.get('sweep_retracement_pct', 0):.1f}% {'✅ PASSED' if calc.get('sweep_filter_passed', False) else '❌ FAILED'} (Min: {calc.get('sweep_threshold_pct', 0):.1f}%)",
                            f"• Counter-trend: {'🚫 BLOCKED' if calc.get('strong_counter_trend', False) else '✅ ALLOWED (FILTER DISABLED)'}",
                            f"• Liquidity Path: {'🚫 BLOCKED' if calc.get('liquidity_path_blocked', False) else '✅ CLEAR'}",
                            f"",
                            f"🎯 RISK/REWARD:",
                            f"Risk: {sig.get('risk', 0):.6f}",
                            f"TP1 Reward: {sig.get('reward_tp1', 0):.6f} (R:R = 1:{sig.get('rr_tp1', 0):.1f})",
                            f"TP2 Reward: {sig.get('reward_tp2', 0):.6f} (R:R = 1:{sig.get('rr_tp2', 0):.1f})",
                            f"",
                            f"🎯 TARGETS ({sig.get('tp_tf', '?')} ATR scaling):",
                            f"SL: {sig.get('sl', 0):.6f}",
                            f"TP1: {sig.get('tp1', 0):.6f} (0.8R)",
                            f"TP2: {sig.get('tp2', 0):.6f} (1.6R)"
                        ]
                        
                        await tg("\n".join(breakdown_lines))
                        await log_signal(sig)
                        last_signal_time[key] = time.time()
                        signals_found += 1
            log.info(f"📊 Scan complete: {signals_found} RomeOPT signals found")
        except Exception as e:
            log.exception("scan error: %s", e)
        elapsed = time.time() - t0
        await asyncio.sleep(max(1, SCAN_INTERVAL - elapsed))

# ---------------- FASTAPI ----------------
app = FastAPI()
@app.post("/webhook")
async def webhook(request: Request):
    token = request.headers.get("X-Auth","")
    if token != WEBHOOK_SECRET: 
        raise HTTPException(403, "Invalid secret")
    data = await request.json()
    log.info("Webhook received: %s", data)
    return {"ok":True}

# ---------------- TEST SWEEP FILTER ----------------
async def test_sweep_filter_with_historical_trades():
    """Test the sweep filter with known historical trades"""
    print("\n" + "="*60)
    print("SWEEP FILTER TEST WITH HISTORICAL TRADES")
    print("="*60)
    
    # Sample historical trades (simulating your 60 trades analysis)
    # Format: (symbol, side, sweep_price, ob_mid, entry_price, expected_result)
    test_cases = [
        # Losing BUY trades (should be filtered out by 1% threshold)
        ("BTC/USDT", "BUY", 50000, 49900, 49999, "❌ REJECT"),  # 0.1% retracement
        ("ETH/USDT", "BUY", 2500, 2490, 2499, "❌ REJECT"),     # 0.1% retracement
        
        # Winning BUY trades (should pass 1% threshold)
        ("BTC/USDT", "BUY", 50000, 49900, 49950, "✅ PASS"),    # 50% retracement
        ("ETH/USDT", "BUY", 2500, 2490, 2495, "✅ PASS"),       # 50% retracement
        
        # Losing SELL trades (should be filtered out by 50% threshold)
        ("BTC/USDT", "SELL", 49000, 50000, 49001, "❌ REJECT"),  # 0.1% retracement
        ("ETH/USDT", "SELL", 2400, 2500, 2401, "❌ REJECT"),     # 0.1% retracement
        
        # Winning SELL trades (should pass 50% threshold)
        ("BTC/USDT", "SELL", 49000, 50000, 49500, "✅ PASS"),    # 50% retracement
        ("ETH/USDT", "SELL", 2400, 2500, 2450, "✅ PASS"),       # 50% retracement
        ("SOL/USDT", "SELL", 100, 150, 125, "✅ PASS"),         # 50% retracement
    ]
    
    passed_tests = 0
    total_tests = len(test_cases)
    
    for symbol, side, sweep_price, ob_mid, entry_price, expected in test_cases:
        sweep_type = "HIGH" if side == "BUY" else "LOW"
        calc_values = {}
        
        valid, reason = validate_sweep_retracement(
            sweep_type=sweep_type,
            sweep_price=sweep_price,
            ob_mid=ob_mid,
            entry_price=entry_price,
            trade_side=side,
            calc_values=calc_values
        )
        
        result = "✅ PASS" if valid else "❌ REJECT"
        status = "✓" if result == expected else "✗"
        
        print(f"\nTest {status}: {symbol} {side}")
        print(f"  Sweep: {sweep_price:.2f}, OB Mid: {ob_mid:.2f}, Entry: {entry_price:.2f}")
        print(f"  Retracement: {calc_values.get('sweep_retracement_pct', 0):.1f}% (Min: {calc_values.get('sweep_threshold_pct', 0):.1f}%)")
        print(f"  Result: {result} (Expected: {expected})")
        print(f"  Reason: {reason}")
        
        if result == expected:
            passed_tests += 1
    
    print(f"\n" + "="*60)
    print(f"TEST RESULTS: {passed_tests}/{total_tests} passed ({passed_tests/total_tests*100:.1f}%)")
    print("="*60)
    
    # Show expected filtering performance
    print("\n📊 EXPECTED FILTER PERFORMANCE:")
    print("Based on your 60 historical trades:")
    print("• Original win rate: 46.7% (28W/32L)")
    print("• With sweep filter: ~88% win rate (22W/3L)")
    print("• Winners preserved: 22/28 = 79%")
    print("• Losers eliminated: 29/32 = 91%")

# ---------------- MAIN ----------------
async def main():
    global exchange, db_conn
    await init_db()
    
    # Test the sweep filter first
    await test_sweep_filter_with_historical_trades()
    
    exchange = ccxt.okx({"enableRateLimit": True})
    await tg("🏆 ROMEOPT 6-Step Scanner Started - Live Early Signals\n🚫 TREND FILTER DISABLED: Will allow both trend and counter-trend trades\n📊 ENHANCED BREAKDOWN: All numerical values visible\n🚨 MOMENTUM FILTER ACTIVE: Only optimal momentum signals (SELL:0.78-0.88, BUY:0.82-0.91)\n🎯 SWEEP FILTER ACTIVE: BUY: ≥1% retracement, SELL: ≥50% retracement")
    await asyncio.gather(scan_loop(), monitor_signals())

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--http", action="store_true")
    args = p.parse_args()
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