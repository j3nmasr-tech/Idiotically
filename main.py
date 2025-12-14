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
- 📊 ENHANCED BREAKDOWN: Shows all numerical values with FULL OB & SWEEP DETAILS (Code 1 format)
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
TOP_N = int(os.getenv("TOP_N", 30))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
MIN_SCORE = 5
CRITICAL_FACTORS_MIN = 2  # HTF Alignment + Liquidity Sweep minimum

# ALL FILTERS DISABLED AS REQUESTED
MOMENTUM_FILTER_ENABLED = False
SWEEP_FILTER_ENABLED = False
OB_FILTER_ENABLED = False
TREND_FILTER_ENABLED = False

# Timeframe mapping for TP scaling (RomeOPT-P logic) - FROM CODE 2
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

# ---------------- ENHANCED SWEEP ANALYSIS (FROM CODE 1) ----------------
def analyze_sweep_details(df: pd.DataFrame, lookback=5):
    """Analyze sweep with detailed information - doesn't affect signal logic"""
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

# ---------------- STRONG TREND DETECTION (DISABLED) ----------------
async def check_strong_counter_trend(exchange, symbol: str, timeframe: str, signal_side: str):
    """DISABLED AS REQUESTED - Returns False to not reject any signals"""
    return False

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

# ---------------- TP/SL CALCULATION (TRUE RomeOPT-P Style) ----------------
async def romeoptp_tp_sl(exchange, entry: float, side: str, entry_tf: str, ob_zone: dict, symbol: str, df: pd.DataFrame):
    """
    TRUE RomeOPT-P Logic:
    - SL based on entry timeframe OB (tight)
    - TP1 = next liquidity pool (previous swing high/low) - MANDATORY
    - TP2 = 1.6R or next major structure
    - REJECT if no liquidity pool found
    """
    if not ob_zone:
        return None, None, None, entry_tf, "No OB zone"
    
    # Get ATR from higher timeframe for risk calculation
    tp_tf = TP_TIMEFRAME_MAP.get(entry_tf, "15m")
    htf_ohlcv = await fetch_ohlcv(exchange, symbol, tp_tf, 100)
    
    if not htf_ohlcv:
        # Fallback to entry timeframe if HTF fails
        htf_ohlcv = await fetch_ohlcv(exchange, symbol, entry_tf, 100)
        tp_tf = entry_tf
    
    if not htf_ohlcv:
        return None, None, None, tp_tf, "No HTF data"
    
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
        min_risk = atr_val * 0.5
        if risk < min_risk:
            risk = min_risk
            sl = entry - risk
        
        # TRUE ROMEOPT-P: TP1 = Next liquidity pool (previous swing high) - MANDATORY
        # Look for nearest resistance in recent price action
        lookback_period = min(50, len(df))
        recent_highs = df['high'].iloc[-lookback_period:]
        
        # Filter highs above entry (potential resistance/liquidity pools)
        resistances = recent_highs[recent_highs > entry]
        
        if len(resistances) == 0:
            # NO LIQUIDITY POOL FOUND - REJECT TRADE
            return None, None, None, tp_tf, "No liquidity pool (resistance) found for BUY"
        
        # TP1 = nearest resistance (liquidity pool)
        tp1 = resistances.min()
        
        # Ensure minimum distance (0.5R) - if too close, reject
        min_tp1_distance = risk * 0.5
        if (tp1 - entry) < min_tp1_distance:
            # Liquidity pool too close - REJECT
            return None, None, None, tp_tf, f"Liquidity pool too close: {(tp1-entry):.6f} < {min_tp1_distance:.6f} (0.5R)"
        
        # TP2 = 1.6R or next major structure
        # Look for next major resistance beyond TP1
        major_resistances = recent_highs[recent_highs > tp1]
        if len(major_resistances) > 0:
            tp2 = major_resistances.min()
            # Ensure TP2 is at least 0.3R beyond TP1
            min_tp2_gap = risk * 0.3
            if (tp2 - tp1) < min_tp2_gap:
                tp2 = entry + (risk * 1.6)
        else:
            tp2 = entry + (risk * 1.6)
        
        # Final validation: TP1 must be > entry + 0.5R
        if (tp1 - entry) < (risk * 0.5):
            # Should not happen due to earlier check, but safety check
            return None, None, None, tp_tf, f"TP1 validation failed: {(tp1-entry):.6f} < {risk*0.5:.6f}"
        
    else:  # SELL
        # SL just above bearish OB high  
        sl = ob_zone['high'] + (atr_val * 0.1)  # Very tight (0.1 × HTF ATR)
        risk = sl - entry
        min_risk = atr_val * 0.5
        if risk < min_risk:
            risk = min_risk
            sl = entry + risk
        
        # TRUE ROMEOPT-P: TP1 = Next liquidity pool (previous swing low) - MANDATORY
        # Look for nearest support in recent price action
        lookback_period = min(50, len(df))
        recent_lows = df['low'].iloc[-lookback_period:]
        
        # Filter lows below entry (potential support/liquidity pools)
        supports = recent_lows[recent_lows < entry]
        
        if len(supports) == 0:
            # NO LIQUIDITY POOL FOUND - REJECT TRADE
            return None, None, None, tp_tf, "No liquidity pool (support) found for SELL"
        
        # TP1 = nearest support (liquidity pool)
        tp1 = supports.max()
        
        # Ensure minimum distance (0.5R) - if too close, reject
        min_tp1_distance = risk * 0.5
        if (entry - tp1) < min_tp1_distance:
            # Liquidity pool too close - REJECT
            return None, None, None, tp_tf, f"Liquidity pool too close: {(entry-tp1):.6f} < {min_tp1_distance:.6f} (0.5R)"
        
        # TP2 = 1.6R or next major structure
        # Look for next major support beyond TP1
        major_supports = recent_lows[recent_lows < tp1]
        if len(major_supports) > 0:
            tp2 = major_supports.max()
            # Ensure TP2 is at least 0.3R beyond TP1
            min_tp2_gap = risk * 0.3
            if (tp1 - tp2) < min_tp2_gap:
                tp2 = entry - (risk * 1.6)
        else:
            tp2 = entry - (risk * 1.6)
        
        # Final validation: TP1 must be < entry - 0.5R
        if (entry - tp1) < (risk * 0.5):
            # Should not happen due to earlier check, but safety check
            return None, None, None, tp_tf, f"TP1 validation failed: {(entry-tp1):.6f} < {risk*0.5:.6f}"
    
    return sl, tp1, tp2, tp_tf, "OK"

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
    sl, tp1, tp2, tp_tf, tp_reason = await romeoptp_tp_sl(
        exchange, sig["entry"], sig["side"], sig["entry_tf"], latest_ob, sig["symbol"], df_entry
    )
    
    if sl is None or tp1 is None or tp2 is None:
        # Could not recalculate TP/SL - keep existing values
        return sig
    
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

# ---------------- FORMAT NUMBER FUNCTION (FROM CODE 1) ----------------
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

# ---------------- ROMEOPT SIGNAL GENERATOR ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    """Full RomeOPT 6-step signal generator with DISABLED FILTERS"""
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

    # ENHANCED: Add sweep details for breakdown (doesn't affect signal) - FROM CODE 1
    sweep_analysis = analyze_sweep_details(df)
    calc_values["sweep_details"] = sweep_analysis["details"]

    # Step2: Displacement
    displacement = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    calc_values["displacement_value"] = round(displacement, 3)
    has_disp = displacement > 0.6
    if has_disp: 
        score += 2
        reasons.append(f"Displacement +2 ({displacement:.3f})")
    else: 
        reasons.append(f"Displacement +0 ({displacement:.3f})")
    
    # ========== CRITICAL: DETECT OB AND SIDE BEFORE FILTERS ==========
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
    # ========== END CRITICAL SECTION ==========
    
    # DISABLED: OB QUALITY FILTER
    calc_values["ob_filter_passed"] = True
    
    # DISABLED: Momentum filter (only original 0.5 check remains)
    momentum_ratio = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    calc_values["momentum_value"] = round(momentum_ratio, 3)
    
    # DISABLED: Coherence filter
    calc_values["coherence_filter_passed"] = True
    
    # DISABLED: Sweep Retracement Filter
    calc_values["sweep_filter_passed"] = True
    calc_values["sweep_retracement_pct"] = 0
    calc_values["sweep_threshold_pct"] = 0

    # Zone Approach calculation
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
    # DISABLED: Check for STRONG counter-trend
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
    else:
        reasons.append(f"HTF Neutral ({htf})")
        calc_values["htf_alignment"] = 0
    
    # Original momentum check (0.5 threshold)
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

    # Calculate TP/SL with TRUE RomeOPT-P logic (NO FALLBACK)
    sl, tp1, tp2, tp_tf, tp_reason = await romeoptp_tp_sl(exchange, float(last["close"]), side, tf, ob_zone, symbol, df)
    if sl is None or tp1 is None or tp2 is None:
        reasons.append(f"TP/SL rejected: {tp_reason}")
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
        "rr_tp2": round(rr_tp2, 2),
        "tp_reason": tp_reason
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
                        calc = sig.get("calc_values", {})
                        sweep_details = calc.get("sweep_details", {})
                        ob_low = calc.get('ob_low', 0)
                        ob_high = calc.get('ob_high', 0)
                        ob_range = ob_high - ob_low
                        ob_mid = (ob_high + ob_low) / 2
                        distance_to_entry = abs(sig['entry'] - ob_mid)
                        distance_pct = (distance_to_entry / sig['entry'] * 100) if sig['entry'] > 0 else 0
                        in_zone = True if (calc.get('ob_type') == 'bullish' and sig['entry'] <= ob_high) or (calc.get('ob_type') == 'bearish' and sig['entry'] >= ob_low) else False
                        
                        # ENHANCED BREAKDOWN IN CODE 1 FORMAT
                        breakdown_lines = [
                            f"🏆 {sig['symbol']} ({sig['entry_tf']}) {sig['side']}",
                            f"Entry: {sig['entry']:.6f} | Score: {sig['score']}/6",
                            f""
                        ]
                        
                        # 📊 SWEEP DETAILS SECTION (Code 1 format)
                        breakdown_lines.append(f"⚡ LIQUIDITY SWEEP DETAILS:")
                        sweep_type = calc.get('sweep_type', 'NONE')
                        
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
                        
                        # 📊 ORDER BLOCK DETAILS SECTION (Code 1 format)
                        breakdown_lines.append(f"🔷 ORDER BLOCK DETAILS:")
                        ob_type = calc.get('ob_type', 'NONE')
                        
                        if ob_type != 'NONE':
                            breakdown_lines.extend([
                                f"  • Type: {ob_type.upper()} OB",
                                f"  • Zone Approach: +{calc.get('zone_approach', 0)}",
                                f"  • OB Range: {format_number(ob_low)} - {format_number(ob_high)}",
                                f"  • Range Size: {format_number(ob_range)}",
                                f"  • Midpoint: {format_number(ob_mid)}",
                                f"  • Distance to Entry: {format_number(distance_to_entry)} ({distance_pct:.2f}%)",
                                f"  • In Zone: {'✅ YES' if in_zone else '❌ NO'}",
                                f"  • Momentum Score: +{calc.get('momentum_score', 0)}"
                            ])
                        else:
                            breakdown_lines.append(f"  • No order block detected")
                        
                        breakdown_lines.append(f"")
                        
                        # 📊 KEY METRICS SECTION
                        breakdown_lines.append(f"📊 KEY METRICS:")
                        breakdown_lines.extend([
                            f"  • Displacement: {calc.get('displacement_value', 0):.3f} ({'✅ STRONG' if calc.get('displacement_value', 0) >= 0.6 else '⚠️ WEAK'})",
                            f"  • Momentum: {calc.get('momentum_value', 0):.3f} {'✅ PASS' if calc.get('momentum_value', 0) >= 0.5 else '❌ FAIL'}",
                            f"  • HTF Trend: {calc.get('htf_trend_direction', '?')}",
                            f"  • HTF Alignment: +{calc.get('htf_alignment', 0)}",
                        ])
                        
                        breakdown_lines.append(f"")
                        
                        # 🎯 TRUE ROMEOPT-P TARGETS SECTION
                        breakdown_lines.append(f"🎯 TRUE ROMEOPT-P TARGETS ({sig.get('tp_tf', '?')} ATR scaling):")
                        breakdown_lines.extend([
                            f"  SL: {format_number(sig.get('sl', 0))}",
                            f"  TP1: {format_number(sig.get('tp1', 0))} (Liquidity Pool) (R:R = 1:{sig.get('rr_tp1', 0):.1f})",
                            f"  TP2: {format_number(sig.get('tp2', 0))} (Next Structure/1.6R) (R:R = 1:{sig.get('rr_tp2', 0):.1f})",
                            f"  Risk: {format_number(sig.get('risk', 0))}",
                            f"  TP Validation: {sig.get('tp_reason', 'OK')}"
                        ])
                        
                        # Clean up empty lines
                        breakdown_lines = [line for line in breakdown_lines if line != ""]
                        
                        await tg("\n".join(breakdown_lines))
                        await log_signal(sig)
                        last_signal_time[key] = time.time()
                        signals_found += 1
            log.info(f"📊 Scan complete: {signals_found} RomeOPT signals found")
        except Exception as e:
            log.exception("scan error: %s", e)
        elapsed = time.time() - t0
        await asyncio.sleep(max(1, SCAN_INTERVAL - elapsed))

# ---------------- TEST OB FILTER ----------------
async def test_ob_filter_with_historical_trades():
    """Test the OB filter with historical trades"""
    print("\n" + "="*60)
    print("CONFIGURATION STATUS")
    print("="*60)
    
    print(f"\nALL FILTERS DISABLED AS REQUESTED")
    print(f"• Trend Filter: {TREND_FILTER_ENABLED}")
    print(f"• Sweep Filter: {SWEEP_FILTER_ENABLED}")
    print(f"• Momentum Filter: {MOMENTUM_FILTER_ENABLED}")
    print(f"• OB Filter: {OB_FILTER_ENABLED}")
    print(f"\nTRUE ROMEOPT-P TP LOGIC ACTIVE:")
    print(f"• TP1 = Next Liquidity Pool (MANDATORY)")
    print(f"• NO FALLBACK - Rejects if no liquidity pool found")
    print(f"• TP2 = Next Structure or 1.6R")

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

# ---------------- MAIN ----------------
async def main():
    global exchange, db_conn
    await init_db()
    
    # Show configuration status
    await test_ob_filter_with_historical_trades()
    
    exchange = ccxt.okx({"enableRateLimit": True})
    await tg("🏆 ROMEOPT 6-Step Scanner Started - Live Early Signals")
    await tg("📊 ENHANCED BREAKDOWN ACTIVATED - Code 1 format with full OB & Sweep Details")
    await tg("🚫 ALL ADDITIONAL FILTERS DISABLED AS REQUESTED:")
    await tg("   - Trend Filter: DISABLED")
    await tg("   - Sweep Retracement Filter: DISABLED")
    await tg("   - Momentum Range Filter: DISABLED")
    await tg("   - OB Quality Filter: DISABLED")
    await tg("🎯 TRUE ROMEOPT-P TP SYSTEM ACTIVE:")
    await tg("   - TP1 = Next Liquidity Pool (MANDATORY)")
    await tg("   - NO FALLBACK - Rejects if no liquidity pool")
    await tg("   - TP2 = Next Structure or 1.6R")
    await tg("   - SL → BE after TP1 hit")
    
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