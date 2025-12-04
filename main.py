#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER (Enhanced + Elite Features)
- Fully live early signals
- RomeOPT 6-step logic (PRACTICAL VERSION with debugging)
- RomeOPT-P TP/SL system (0.8R/1.6R, SL→BE after TP1)
- Telegram alerts
- Async SQLite logging (with detailed JSON diagnostics)
- Filters: Score >=4, realistic conditions
- All timeframes: 1m, 3m, 5m, 15m, 30m, 1h
"""

import os
import time
import asyncio
import logging
import datetime
import json
import math
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
TOP_N = int(os.getenv("TOP_N", 15))
# ALL TIMEFRAMES INCLUDING SHORT ONES
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m", "1h"]
# Timeframe-specific minimum scores
TF_MIN_SCORES = {
    "1m": 3,    # Most lenient for 1m
    "3m": 3,    # Lenient for 3m
    "5m": 4,    # Standard for 5m
    "15m": 4,   # Standard for 15m
    "30m": 4,   # Standard for 30m
    "1h": 5     # Strictest for 1h
}
DEFAULT_MIN_SCORE = 4
CRITICAL_FACTORS_MIN = 1  # Only require 1 critical factor

# ---------------- ROMEOPT-P TP CONFIG ----------------
# Timeframe mapping for TP scaling (RomeOPT-P logic)
TP_TIMEFRAME_MAP = {
    "1m": "5m",    # 1m → 5m ATR (5×) - less aggressive
    "3m": "15m",   # 3m → 15m ATR (5×)
    "5m": "15m",   # 5m → 15m ATR (3×) - conservative
    "15m": "1h",   # 15m → 1h ATR (4×)
    "30m": "1h",   # 30m → 1h ATR (2×) - minimal scaling
    "1h": "4h"     # 1h → 4h ATR (4×)
}

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_bot")
db_lock = asyncio.Lock()
db_conn = None
exchange = None  # Global exchange instance

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
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
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
            latest_ob TEXT,
            details TEXT,
            entry_tf TEXT DEFAULT '',
            tp_tf TEXT DEFAULT ''
        );
    """)
    await db_conn.commit()

# ---------------- OHLCV ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if ohlcv and len(ohlcv) > 0:
            return ohlcv
    except Exception as e:
        log.debug(f"fetch_ohlcv failed for {symbol} {timeframe}: {e}")
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

# ---------------- MULTI-TIMEFRAME ALIGNMENT (PRACTICAL) ----------------
async def elite_tf_alignment(exchange, symbol: str, side: str):
    """
    More practical: Require only 2/3 higher timeframes to align
    """
    tfs = ["15m", "1h", "4h"]
    alignments = 0
    total_checked = 0
    
    for tf in tfs:
        try:
            ohlcv = await fetch_ohlcv(exchange, symbol, tf, 50)
            if not ohlcv or len(ohlcv) < 10:
                continue
                
            df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
            for col in ["open","high","low","close","vol"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            
            # Simple trend detection
            if len(df) >= 10:
                current_price = df["close"].iloc[-1]
                ma20 = df["close"].rolling(20).mean().iloc[-1]
                
                if side == "BUY":
                    if current_price > ma20:
                        alignments += 1
                else:  # SELL
                    if current_price < ma20:
                        alignments += 1
                
                total_checked += 1
                
        except Exception as e:
            log.debug(f"elite_tf_alignment error for {symbol} {tf}: {e}")
            continue
    
    # Require at least 2/3 alignments if we checked all, otherwise be lenient
    if total_checked >= 2:
        return alignments >= max(2, total_checked * 0.6)
    return True  # Be lenient if we can't check enough timeframes

# ---------------- BOS/CHOCH DETECTION (PRACTICAL) ----------------
def detect_bos_choch(df: pd.DataFrame, swing_lookback=15):
    """
    More practical BOS/CHOCH detection
    """
    res = {"has_bos": False, "bos_side": None, "has_choch": False, "choch_info": None}
    
    if len(df) < swing_lookback:
        return res
    
    # Calculate recent swing highs/lows
    tail = df.tail(swing_lookback)
    recent_high = tail['high'].max()
    recent_low = tail['low'].min()
    last_close = df['close'].iloc[-1]
    prev_close = df['close'].iloc[-2] if len(df) >= 2 else last_close
    
    # Calculate dynamic threshold (1% of range)
    price_range = recent_high - recent_low
    if price_range > 0:
        threshold = price_range * 0.01
    else:
        threshold = recent_high * 0.001  # Fallback
    
    # BOS Detection (more lenient)
    if last_close > recent_high - threshold and last_close > prev_close:
        res["has_bos"] = True
        res["bos_side"] = "BUY"
    elif last_close < recent_low + threshold and last_close < prev_close:
        res["has_bos"] = True
        res["bos_side"] = "SELL"
    
    # CHOCH Detection (EMA crossover)
    try:
        ema20 = df['close'].ewm(span=20, min_periods=1).mean()
        ema50 = df['close'].ewm(span=50, min_periods=1).mean()
        
        if len(df) >= 10:
            ema20_now = ema20.iloc[-1]
            ema50_now = ema50.iloc[-1]
            ema20_prev = ema20.iloc[-3]
            ema50_prev = ema50.iloc[-3]
            
            # Check for crossover
            if (ema20_now > ema50_now and ema20_prev <= ema50_prev) or \
               (ema20_now < ema50_now and ema20_prev >= ema50_prev):
                res["has_choch"] = True
                res["choch_info"] = {
                    "ema20": float(ema20_now),
                    "ema50": float(ema50_now),
                    "trend": "BULL" if ema20_now > ema50_now else "BEAR"
                }
    except Exception:
        pass
    
    # If no clear side from BOS, use CHOCH
    if res["bos_side"] is None and res["has_choch"] and res["choch_info"]:
        res["bos_side"] = "BUY" if res["choch_info"]["trend"] == "BULL" else "SELL"
    
    return res

# ---------------- VOLUME SPIKE (PRACTICAL) ----------------
def vol_spike(df: pd.DataFrame, idx=-1, factor=1.3, lookback=20):
    """
    More lenient volume spike detection
    """
    if len(df) < lookback:
        return True  # Not enough data, don't reject
        
    try:
        vol_series = df['vol'].tail(lookback)
        if vol_series.isnull().all():
            return True
            
        vol_avg = vol_series.mean()
        current_vol = float(df['vol'].iloc[idx])
        
        # Allow if volume is above average or if it's significantly higher than recent low
        if vol_avg > 0:
            return current_vol > vol_avg * factor
        else:
            # Check if volume is increasing
            if len(df) >= 3:
                prev_vol = float(df['vol'].iloc[idx-1])
                return current_vol > prev_vol * 1.2
    except Exception:
        pass
    
    return True  # Don't reject on error

# ---------------- FVG DETECTION (PRACTICAL) ----------------
def find_fvgs(df: pd.DataFrame, lookback=100):
    """
    Find Fair Value Gaps with more practical approach
    """
    fvgs = []
    n = len(df)
    if n < 10:
        return fvgs
    
    # Check last 50 candles for FVGs
    start_idx = max(0, n - min(lookback, n))
    
    for i in range(start_idx, n-2):
        if i+2 >= n:
            break
            
        c0 = df.iloc[i]    # First candle
        c1 = df.iloc[i+1]  # Gap candle
        c2 = df.iloc[i+2]  # Confirmation candle
        
        # Bullish FVG: c2 high < c0 low (gap up)
        if c2['high'] < c0['low']:
            # Verify it's not just a small gap
            gap_size = abs(c0['low'] - c2['high'])
            if gap_size > (c0['high'] - c0['low']) * 0.1:  # At least 10% of candle size
                fvgs.append({
                    "type": "bullish",
                    "low": float(c2['high']),
                    "high": float(c0['low']),
                    "idx": i,
                    "size": float(gap_size)
                })
        
        # Bearish FVG: c2 low > c0 high (gap down)
        elif c2['low'] > c0['high']:
            gap_size = abs(c2['low'] - c0['high'])
            if gap_size > (c0['high'] - c0['low']) * 0.1:
                fvgs.append({
                    "type": "bearish",
                    "low": float(c0['high']),
                    "high": float(c2['low']),
                    "idx": i,
                    "size": float(gap_size)
                })
    
    # Return only recent FVGs (last 20)
    return sorted(fvgs, key=lambda x: x['idx'], reverse=True)[:20]

# ---------------- ORDER BLOCK DETECTION (PRACTICAL) ----------------
def find_quality_order_block(df: pd.DataFrame, lookback=50):
    """
    Find quality order blocks with more practical approach
    """
    n = len(df)
    if n < 10:
        return None
    
    # Start from recent candles
    for i in range(n-3, max(1, n - lookback), -1):
        if i < 1 or i+1 >= n:
            continue
            
        candle = df.iloc[i]
        prev_candle = df.iloc[i-1]
        
        # Calculate candle properties
        candle_body = abs(candle['close'] - candle['open'])
        candle_range = candle['high'] - candle['low']
        
        if candle_range <= 0:
            continue
            
        body_ratio = candle_body / candle_range
        
        # Bullish Order Block: Strong bear candle followed by bullish reaction
        if (prev_candle['close'] < prev_candle['open'] and  # Prev was bearish
            candle['close'] > candle['open'] and             # Current is bullish
            body_ratio > 0.3 and                             # Has decent body
            candle['low'] <= prev_candle['low']):            # Takes out previous low
            
            # Check if next candle confirms (price moves up)
            if i+1 < n:
                next_candle = df.iloc[i+1]
                if next_candle['close'] > candle['close']:
                    return {
                        "type": "bullish",
                        "low": float(min(candle['low'], prev_candle['low'])),
                        "high": float(max(candle['close'], prev_candle['close'])),
                        "idx": i,
                        "strength": float(body_ratio)
                    }
        
        # Bearish Order Block: Strong bull candle followed by bearish reaction
        elif (prev_candle['close'] > prev_candle['open'] and  # Prev was bullish
              candle['close'] < candle['open'] and             # Current is bearish
              body_ratio > 0.3 and                             # Has decent body
              candle['high'] >= prev_candle['high']):          # Takes out previous high
            
            # Check if next candle confirms (price moves down)
            if i+1 < n:
                next_candle = df.iloc[i+1]
                if next_candle['close'] < candle['close']:
                    return {
                        "type": "bearish",
                        "low": float(min(candle['close'], prev_candle['close'])),
                        "high": float(max(candle['high'], prev_candle['high'])),
                        "idx": i,
                        "strength": float(body_ratio)
                    }
    
    return None

# ---------------- SIMPLE ORDER BLOCK DETECTION (RomeOPT version) ----------------
def find_latest_ob(df: pd.DataFrame):
    """
    Simple Order Block detection for RomeOPT TP/SL
    Returns: {"type": "bullish"/"bearish", "low": price, "high": price}
    """
    for i in range(len(df)-5, len(df)-1):
        candle, prev_candle = df.iloc[i], df.iloc[i-1]
        if candle["close"]>candle["open"] and prev_candle["close"]<prev_candle["open"]:
            return {"type":"bullish","low":min(candle["low"], prev_candle["low"]),"high":candle["close"]}
        elif candle["close"]<candle["open"] and prev_candle["close"]>prev_candle["open"]:
            return {"type":"bearish","low":candle["close"],"high":max(candle["high"], prev_candle["high"])}
    return None

# ===== WINNER PATTERN FILTER =====
def filter_winner_patterns(signal: dict) -> tuple:
    """
    Filter signals based on winner patterns analysis
    Returns: (should_reject, rejection_reason)
    
    Criteria from analysis:
    1. HTF_Align present (91% of winners had this)
    2. Either BOS or CHOCH (not both missing)
    3. FVG almost always present (98% of winners)
    """
    reason_list = signal.get("reason_list", [])
    if not reason_list:
        return True, "No breakdown data"
    
    # Criterion 1: HTF_Align must be present
    has_htf_align = "HTF_Align" in reason_list
    
    # Criterion 2: Either BOS or CHOCH must be present (not both missing)
    has_bos = "BOS" in reason_list
    has_choch = "CHOCH" in reason_list
    
    # Criterion 3: FVG must be present
    has_fvg = "FVG" in reason_list
    
    # Check all criteria
    if not has_htf_align:
        return True, "Missing HTF_Alignment"
    
    if not has_bos and not has_choch:
        return True, "Missing both BOS and CHOCH"
    
    if not has_fvg:
        return True, "Missing FVG"
    
    return False, ""  # Signal passes all filters
# ===========================================

# ---------------- MARKET STRUCTURE SHIFT (PRACTICAL) ----------------
def confirm_market_structure_shift(df: pd.DataFrame, side: str):
    """
    More practical market structure shift detection
    """
    if len(df) < 15:
        return True  # Not enough data, don't reject
        
    # Simple structure detection using swing points
    highs = df['high'].values
    lows = df['low'].values
    
    # Find local highs and lows (simplified)
    local_highs = []
    local_lows = []
    
    for i in range(2, len(df)-2):
        if highs[i] >= highs[i-2] and highs[i] >= highs[i-1] and highs[i] >= highs[i+1] and highs[i] >= highs[i+2]:
            local_highs.append((i, highs[i]))
        if lows[i] <= lows[i-2] and lows[i] <= lows[i-1] and lows[i] <= lows[i+1] and lows[i] <= lows[i+2]:
            local_lows.append((i, lows[i]))
    
    # Check last 3 swing points
    if len(local_highs) >= 3 and len(local_lows) >= 3:
        recent_highs = sorted(local_highs[-3:], key=lambda x: x[0])
        recent_lows = sorted(local_lows[-3:], key=lambda x: x[0])
        
        if side == "BUY":
            # Check for higher lows
            if recent_lows[-1][1] > recent_lows[-2][1]:
                return True
        else:  # SELL
            # Check for lower highs
            if recent_highs[-1][1] < recent_highs[-2][1]:
                return True
    
    return True  # Default to True if can't determine

# ---------------- ROMEOPT-P TP/SL CALCULATION ----------------
async def romeoptp_tp_sl(exchange, entry: float, side: str, entry_tf: str, ob_zone: dict, symbol: str):
    """
    RomeOPT-P Logic:
    - SL based on entry timeframe OB (tight)
    - TP scaled to higher timeframe ATR (meaningful)
    - TP1 = 0.8R, TP2 = 1.6R (NO TP3 in RomeOPT)
    """
    if not ob_zone:
        return None, None, None, None, entry_tf
    
    # Get ATR from higher timeframe for TP scaling
    tp_tf = TP_TIMEFRAME_MAP.get(entry_tf, "15m")
    htf_ohlcv = await fetch_ohlcv(exchange, symbol, tp_tf, 100)
    
    if not htf_ohlcv:
        # Fallback to entry timeframe if HTF fails
        htf_ohlcv = await fetch_ohlcv(exchange, symbol, entry_tf, 100)
        tp_tf = entry_tf
    
    if not htf_ohlcv:
        return None, None, None, None, tp_tf
    
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
        # TP scaled to HTF ATR - RomeOPT: 0.8R and 1.6R (NO TP3)
        tp1 = entry + (risk * 0.8)  # 0.8R
        tp2 = entry + (risk * 1.6)  # 1.6R
        tp3 = None  # RomeOPT doesn't use TP3
    else:  # SELL
        # SL just above bearish OB high  
        sl = ob_zone['high'] + (atr_val * 0.1)  # Very tight (0.1 × HTF ATR)
        risk = sl - entry
        # TP scaled to HTF ATR - RomeOPT: 0.8R and 1.6R (NO TP3)
        tp1 = entry - (risk * 0.8)  # 0.8R
        tp2 = entry - (risk * 1.6)  # 1.6R
        tp3 = None  # RomeOPT doesn't use TP3
    
    return sl, tp1, tp2, tp3, tp_tf

# ---------------- UPDATE SIGNAL TP/SL (RomeOPT version) ----------------
async def update_tp_sl_live_romeopt(sig: dict):
    """Update TP/SL with current market data using RomeOPT logic"""
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
    
    # Recalculate TP/SL with current data using RomeOPT logic
    sl, tp1, tp2, tp3, tp_tf = await romeoptp_tp_sl(
        exchange, sig["entry"], sig["side"], sig["entry_tf"], latest_ob, sig["symbol"]
    )
    
    if sl is not None and tp1 is not None and tp2 is not None:
        sig["sl"] = sl
        sig["tp1"] = tp1
        sig["tp2"] = tp2
        sig["tp3"] = tp3  # Will be None for RomeOPT
        sig["tp_tf"] = tp_tf
        sig["latest_ob"] = latest_ob
    
    return sig

# ---------------- SIGNAL GENERATION (PRACTICAL) ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    """
    Main function to generate a RomeOPT signal with practical validation
    """
    # Timeframe-specific adjustments
    min_data_required = {
        "1m": 100,   # Need more candles for 1m
        "3m": 80,    # Need fewer for 3m
        "5m": 70,    # Even fewer for 5m
        "15m": 60,   # Standard for 15m
        "30m": 50,   # Standard for 30m
        "1h": 40     # Standard for 1h
    }.get(tf, 50)
    
    min_score_required = TF_MIN_SCORES.get(tf, DEFAULT_MIN_SCORE)
    
    if df is None or len(df) < min_data_required:
        log.debug(f"{symbol} {tf}: Insufficient data ({len(df) if df else 0} candles, need {min_data_required})")
        return None
    
    # Log data quality
    log.debug(f"{symbol} {tf}: Checking with {len(df)} candles, latest price: {df['close'].iloc[-1]}")
    
    # Step 1: Detect BOS/CHOCH (relaxed)
    ms_shift = detect_bos_choch(df)
    if not ms_shift["has_bos"] and not ms_shift["has_choch"]:
        log.debug(f"{symbol} {tf}: No BOS/CHOCH detected")
        return None
    
    side = ms_shift["bos_side"]
    if side is None:
        log.debug(f"{symbol} {tf}: Could not determine side from BOS/CHOCH")
        return None
    
    log.debug(f"{symbol} {tf}: Side determined as {side} from BOS/CHOCH")
    
    # Step 2: Volume spike (more lenient for shorter timeframes)
    vol_factor = 1.2 if tf in ["1m", "3m"] else 1.3  # More lenient for 1m/3m
    vol_ok = vol_spike(df, factor=vol_factor)
    if not vol_ok:
        log.debug(f"{symbol} {tf}: Volume spike check failed")
        # Don't return None, just note it
    
    # Step 3: Find FVGs (adjust lookback for shorter timeframes)
    fvg_lookback = 150 if tf in ["1m", "3m"] else 100  # More candles for shorter TFs
    fvgs = find_fvgs(df, lookback=fvg_lookback)
    if not fvgs:
        log.debug(f"{symbol} {tf}: No FVGs found")
        # Don't return None, FVG is nice-to-have
    
    # Step 4: Quality OB (important but not mandatory)
    ob_zone = find_quality_order_block(df)
    if not ob_zone:
        log.debug(f"{symbol} {tf}: No quality OB found")
        # Don't return None, continue
    
    # Step 5: Market structure shift (lenient)
    mss_ok = confirm_market_structure_shift(df, side)
    if not mss_ok:
        log.debug(f"{symbol} {tf}: Market structure shift not confirmed")
        # Don't return None, continue
    
    # Step 6: Higher timeframe alignment (more lenient for shorter timeframes)
    elite_ok = await elite_tf_alignment(exchange, symbol, side)
    if not elite_ok:
        log.debug(f"{symbol} {tf}: Higher timeframe alignment failed")
        # Don't return None, continue
    
    # Entry price
    entry = df["close"].iloc[-1]
    
    # Get TP/SL levels using ROMEOPT-P logic
    sl, tp1, tp2, tp3, tp_tf = await romeoptp_tp_sl(exchange, entry, side, tf, ob_zone, symbol)
    
    if sl is None or tp1 is None or tp2 is None:
        log.debug(f"{symbol} {tf}: RomeOPT TP/SL calculation failed")
        return None
    
    # Score calculation (more balanced)
    score = 0
    if ms_shift["has_bos"]: score += 2
    if ms_shift["has_choch"]: score += 1
    if vol_ok: score += 1
    if elite_ok: score += 2
    if fvgs: score += 1
    if mss_ok: score += 1
    if ob_zone: score += 2
    
    # Additional points for strong signals
    # Strong volume spike
    if vol_ok and vol_spike(df, factor=2.0):  # Very high volume
        score += 1
    
    # Strong order block
    if ob_zone and ob_zone.get("strength", 0) > 0.5:
        score += 1
    
    # Multiple FVGs
    if fvgs and len(fvgs) >= 2:
        score += 1
    
    # Minimum score check (timeframe-specific)
    if score < min_score_required:
        log.debug(f"{symbol} {tf}: Score {score} below minimum {min_score_required} for {tf}")
        return None
    
    # Create signal
    signal = {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,  # Will be None for RomeOPT
        "score": score,
        "entry_tf": tf,
        "tp_tf": tp_tf,
        "reason": f"RomeOPT-P signal on {tf}",
        "detailed": {
            "timeframe": tf,
            "bos": ms_shift.get("has_bos", False),
            "choch": ms_shift.get("has_choch", False),
            "fvgs": len(fvgs) if fvgs else 0,
            "ob_zone": bool(ob_zone),
            "elite_ok": elite_ok,
            "vol_ok": vol_ok,
            "mss_ok": mss_ok,
            "risk_reward": round((tp1 - entry) / (entry - sl), 2) if side == "BUY" else round((entry - tp1) / (sl - entry), 2)
        },
        "reason_list": [],
        "ob_zone": ob_zone
    }
    
    # Build reason list
    if ms_shift["has_bos"]: signal["reason_list"].append("BOS")
    if ms_shift["has_choch"]: signal["reason_list"].append("CHOCH")
    if vol_ok: signal["reason_list"].append("Volume")
    if fvgs: signal["reason_list"].append("FVG")
    if ob_zone: signal["reason_list"].append("OB")
    if mss_ok: signal["reason_list"].append("MSS")
    if elite_ok: signal["reason_list"].append("HTF_Align")
    
    # ===== WINNER PATTERN FILTER =====
    should_reject, reject_reason = filter_winner_patterns(signal)
    if should_reject:
        log.debug(f"❌ Winner pattern filter REJECTED {symbol} {side} on {tf}: {reject_reason}")
        return None
    
    # Add filter info to reason list
    signal["reason_list"].append("WinnerFilter✅")
    # ==================================
    
    log.info(f"✓ {symbol} {tf}: Generated {side} signal with score {score}")
    return signal

# ---------------- LOG SIGNAL ----------------
async def log_signal(sig):
    async with db_lock:
        details_json = json.dumps(sig.get("detailed", {}), default=str)
        await db_conn.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score,latest_ob,details,entry_tf,tp_tf)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            sig["symbol"],
            sig["side"],
            sig["entry"],
            sig.get("sl"),
            sig.get("tp1"),
            sig.get("tp2"),
            sig.get("tp3"),  # May be None for RomeOPT
            datetime.datetime.utcnow().isoformat(),
            "PENDING",
            sig.get("reason", ""),
            sig.get("score", 0),
            str(sig.get("ob_zone", "")),
            details_json,
            sig.get("entry_tf", ""),
            sig.get("tp_tf", "")
        ))
        await db_conn.commit()
        log.info(f"Logged signal for {sig['symbol']} to database")

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

# ---------------- MONITOR SIGNALS (RomeOPT version) ----------------
async def monitor_signals():
    """
    Monitor open signals for TP/SL hits - RomeOPT version
    SL → BE after TP1 hit
    """
    global exchange
    if exchange is None:
        log.error("Exchange not initialized in monitor_signals")
        return
        
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("""
                    SELECT id,symbol,side,entry,sl,tp1,tp2,tp3,tp1_hit,tp2_hit,status,entry_tf 
                    FROM signals 
                    WHERE status IN ('OPEN','PENDING')
                """) as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, status, entry_tf = row
                        tp1_hit = tp1_hit or 0
                        tp2_hit = tp2_hit or 0

                        # Fetch current price
                        try:
                            ticker = await exchange.fetch_ticker(symbol)
                            last_price = ticker.get("last")
                            if last_price is None:
                                continue
                        except Exception as e:
                            log.debug(f"fetch_ticker failed for {symbol}: {e}")
                            continue

                        # Update TP/SL with current data using RomeOPT logic
                        sig = {
                            "symbol": symbol,
                            "side": side,
                            "entry": entry,
                            "sl": sl,
                            "tp1": tp1,
                            "tp2": tp2,
                            "tp3": tp3,
                            "entry_tf": entry_tf if entry_tf else "15m"  # Default
                        }
                        sig = await update_tp_sl_live_romeopt(sig)
                        sl, tp1, tp2, tp3 = sig["sl"], sig["tp1"], sig["tp2"], sig["tp3"]

                        # Check for TP/SL hits - RomeOPT rules
                        hits = []
                        sl_hit = False
                        
                        if side == "BUY":
                            if not tp1_hit and last_price >= tp1:
                                hits.append("TP1")
                                tp1_hit = 1
                                # RomeOPT: Move SL to breakeven after TP1 hit
                                sl = entry
                                log.info(f"✅ {symbol}: TP1 hit, moving SL to breakeven at {entry}")
                            
                            if not tp2_hit and last_price >= tp2:
                                hits.append("TP2")
                                tp2_hit = 1
                                status = "CLOSED"
                                log.info(f"✅ {symbol}: TP2 hit, closing trade")
                            
                            if last_price <= sl:
                                hits.append("SL")
                                status = "CLOSED"
                                sl_hit = True
                                log.info(f"❌ {symbol}: SL hit at {sl}")
                        
                        else:  # SELL
                            if not tp1_hit and last_price <= tp1:
                                hits.append("TP1")
                                tp1_hit = 1
                                # RomeOPT: Move SL to breakeven after TP1 hit
                                sl = entry
                                log.info(f"✅ {symbol}: TP1 hit, moving SL to breakeven at {entry}")
                            
                            if not tp2_hit and last_price <= tp2:
                                hits.append("TP2")
                                tp2_hit = 1
                                status = "CLOSED"
                                log.info(f"✅ {symbol}: TP2 hit, closing trade")
                            
                            if last_price >= sl:
                                hits.append("SL")
                                status = "CLOSED"
                                sl_hit = True
                                log.info(f"❌ {symbol}: SL hit at {sl}")

                        # Send alert if hits
                        if hits:
                            alert_msg = (f"🎯 {symbol} {side} Update\n"
                                       f"Entry: {entry:.8f}\n"
                                       f"Last: {last_price:.8f}\n"
                                       f"Hits: {', '.join(hits)}\n"
                                       f"SL: {sl:.8f}\n"
                                       f"TP1: {tp1:.8f} TP2: {tp2:.8f}")
                            
                            # Add note about SL movement if TP1 hit
                            if "TP1" in hits:
                                alert_msg += f"\n📈 SL moved to breakeven at {entry:.8f}"
                            
                            await tg(alert_msg)

                        # Record SL hit
                        if sl_hit:
                            record_sl_hit(symbol)

                        # Update database
                        await db_conn.execute("""
                            UPDATE signals 
                            SET tp1_hit=?, tp2_hit=?, sl=?, status=? 
                            WHERE id=?
                        """, (tp1_hit, tp2_hit, sl, status, sig_id))
                
                await db_conn.commit()
                
        except Exception as e:
            log.exception(f"Monitor error: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- SCAN LOOP ----------------
last_signal_time = {}
async def scan_loop(exchange):
    """
    Main scanning loop with timeframe-specific cooldowns
    """
    # Timeframe-specific cooldowns (seconds)
    cooldown_map = {
        "1m": 60,    # 1 minute cooldown for 1m signals
        "3m": 120,   # 2 minutes for 3m
        "5m": 180,   # 3 minutes for 5m
        "15m": 300,  # 5 minutes for 15m
        "30m": 600,  # 10 minutes for 30m
        "1h": 1800,  # 30 minutes for 1h
    }
    
    while True:
        t0 = time.time()
        signals_found = 0
        
        try:
            # Fetch top volume symbols
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, v.get("quoteVolume", 0)) 
                         for s, v in tickers.items() 
                         if s.endswith("/USDT") or s.endswith("USDT")]
            
            if not usdt_pairs:
                log.warning("No USDT pairs found")
                await asyncio.sleep(SCAN_INTERVAL)
                continue
            
            top = sorted(usdt_pairs, key=lambda x: x[1], reverse=True)[:TOP_N]
            log.info(f"📈 Scanning top {len(top)} symbols by volume across {len(TIMEFRAMES)} timeframes")
            
            for symbol, volume in top:
                # Skip if deprioritized
                if deprioritized(symbol):
                    log.debug(f"Skipping deprioritized {symbol}")
                    continue
                
                for tf in TIMEFRAMES:
                    key = f"{symbol}:{tf}"
                    
                    # Check timeframe-specific cooldown
                    cooldown = cooldown_map.get(tf, 300)
                    if key in last_signal_time and time.time() - last_signal_time[key] < cooldown:
                        continue
                    
                    log.debug(f"Checking {symbol} on {tf}")
                    
                    # Fetch OHLCV data
                    ohlcv = await fetch_ohlcv(exchange, symbol, tf, 200)
                    if not ohlcv or len(ohlcv) < 100:
                        log.debug(f"Insufficient data for {symbol} {tf}")
                        continue
                    
                    # Create DataFrame
                    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                    for col in ["open","high","low","close","vol"]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    
                    # Generate signal
                    sig = await generate_signal_romeopt(exchange, df, symbol, tf)
                    
                    if sig:
                        # Prepare alert message
                        breakdown = ', '.join(sig.get('reason_list', []))
                        risk_reward = sig["detailed"].get("risk_reward", 0)
                        
                        # RomeOPT alert format (no TP3)
                        alert_msg = (f"🏆 {sig['symbol']} ({tf}) {sig['side']}\n"
                                   f"Entry: {sig['entry']:.8f}\n"
                                   f"SL: {sig.get('sl', 0):.8f}\n"
                                   f"TP1: {sig.get('tp1', 0):.8f} (0.8R)\n"
                                   f"TP2: {sig.get('tp2', 0):.8f} (1.6R)\n"
                                   f"Score: {sig['score']} | R:R: {risk_reward}:1\n"
                                   f"Breakdown: {breakdown}\n"
                                   f"⚠️ RomeOPT-P: SL→BE after TP1")
                        
                        # Send alert and log
                        await tg(alert_msg)
                        await log_signal(sig)
                        
                        # Update last signal time
                        last_signal_time[key] = time.time()
                        signals_found += 1
                        
                        log.info(f"✅ Found {sig['side']} signal for {sig['symbol']} on {tf} (Score: {sig['score']})")
            
            log.info(f"📊 Scan complete: {signals_found} signals found across all timeframes")
            
        except Exception as e:
            log.exception(f"Scan error: {e}")
        
        # Sleep for remaining interval
        elapsed = time.time() - t0
        sleep_time = max(1, SCAN_INTERVAL - elapsed)
        await asyncio.sleep(sleep_time)

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/")
async def root():
    return {"status": "RomeOPT Scanner is running", "timeframes": TIMEFRAMES}

@app.post("/webhook")
async def webhook(request: Request):
    token = request.headers.get("X-Auth", "")
    if token != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    
    data = await request.json()
    log.info(f"Webhook received: {data}")
    
    # Process webhook data if needed
    return {"ok": True}

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.datetime.utcnow().isoformat()}

@app.get("/stats")
async def stats():
    """Get scanner statistics"""
    async with db_lock:
        async with db_conn.execute("SELECT COUNT(*) as total, COUNT(CASE WHEN status='OPEN' THEN 1 END) as open FROM signals") as cursor:
            row = await cursor.fetchone()
            total_signals = row[0] if row else 0
            open_signals = row[1] if row else 0
        
        async with db_conn.execute("SELECT COUNT(*) as today FROM signals WHERE DATE(timestamp) = DATE('now')") as cursor:
            row = await cursor.fetchone()
            today_signals = row[0] if row else 0
    
    return {
        "status": "running",
        "timeframes_active": TIMEFRAMES,
        "signals_total": total_signals,
        "signals_open": open_signals,
        "signals_today": today_signals,
        "scan_interval": SCAN_INTERVAL,
        "top_pairs": TOP_N,
        "tp_system": "RomeOPT-P (0.8R/1.6R, SL→BE after TP1)"
    }

# ---------------- CLEANUP FUNCTION ----------------
async def cleanup():
    """Cleanup resources properly"""
    global exchange, db_conn
    
    log.info("Cleaning up resources...")
    
    if exchange:
        try:
            await exchange.close()
            log.info("Exchange connection closed")
        except Exception as e:
            log.error(f"Error closing exchange: {e}")
    
    if db_conn:
        try:
            await db_conn.close()
            log.info("Database connection closed")
        except Exception as e:
            log.error(f"Error closing database: {e}")

# ---------------- MAIN ----------------
async def main():
    global exchange, db_conn
    
    # Initialize
    log.info("Starting RomeOPT-P Scanner with all timeframes...")
    await init_db()
    
    # Initialize exchange
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "timeout": 30000,
        "rateLimit": 1000,
    })
    
    # Test connection
    try:
        await exchange.load_markets()
        log.info(f"✅ Connected to {exchange.name}")
        log.info(f"📊 Scanning on timeframes: {', '.join(TIMEFRAMES)}")
        log.info(f"🎯 RomeOPT-P TP System: 0.8R/1.6R, SL→BE after TP1")
    except Exception as e:
        log.error(f"Failed to connect to exchange: {e}")
        await cleanup()
        return
    
    # Send startup message
    await tg(f"🏆 ROMEOPT-P Scanner Started\n"
             f"📈 Timeframes: {', '.join(TIMEFRAMES)}\n"
             f"⚙️ Top {TOP_N} pairs | Interval: {SCAN_INTERVAL}s\n"
             f"🎯 TP: 0.8R/1.6R | SL→BE after TP1")
    
    # Run scanner and monitor
    try:
        await asyncio.gather(
            scan_loop(exchange),
            monitor_signals()
        )
    except asyncio.CancelledError:
        log.info("Scanner tasks cancelled")
    except Exception as e:
        log.exception(f"Unexpected error in main: {e}")
    finally:
        await cleanup()

# ---------------- ENTRY POINT ----------------
if __name__ == "__main__":
    import argparse
    import signal
    
    parser = argparse.ArgumentParser(description="RomeOPT Scanner")
    parser.add_argument("--http", action="store_true", help="Run HTTP server")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        log.debug("Debug logging enabled")
    
    if args.http:
        log.info("Starting HTTP server on port 9000")
        uvicorn.run(app, host="0.0.0.0", port=9000)
    else:
        # Setup signal handlers for graceful shutdown
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Create task
        main_task = loop.create_task(main())
        
        # Signal handling
        def signal_handler(signame):
            log.info(f"Received signal {signame}, shutting down...")
            main_task.cancel()
        
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: signal_handler(s.name))
        
        try:
            loop.run_until_complete(main_task)
        except asyncio.CancelledError:
            log.info("Main task cancelled")
        except Exception as e:
            log.exception(f"Fatal error: {e}")
        finally:
            # Run cleanup one more time
            try:
                loop.run_until_complete(cleanup())
            except:
                pass
            
            # Close loop
            loop.close()
            log.info("Scanner shutdown complete")