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
- 🆕 ENHANCED BREAKDOWN: Shows all numerical values with FULL OB & SWEEP DETAILS (like Code 1)
- 🚫 COUNTER-TREND FILTER: DISABLED (Allows all trades regardless of HTF trend)
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
TOP_N = int(os.getenv("TOP_N", 20))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
MIN_SCORE = 5
CRITICAL_FACTORS_MIN = 2  # HTF Alignment + Liquidity Sweep minimum

# Timeframe mapping for TP scaling (RomeOPT-P logic)
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
exchange = None

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
    """Check and add missing columns to existing database"""
    try:
        # Get current table schema
        async with db_conn.execute("PRAGMA table_info(signals)") as cursor:
            columns = await cursor.fetchall()
            existing_columns = [col[1] for col in columns]
        
        log.info(f"Existing columns: {existing_columns}")
        
        # List of columns that should exist
        required_columns = [
            ("htf_trend_value", "REAL"),
            ("ob_distance_pct", "REAL"),
            ("sweep_score", "INTEGER DEFAULT 0"),
            ("zone_approach_score", "INTEGER DEFAULT 0"),
            ("htf_alignment_score", "INTEGER DEFAULT 0"),
            ("momentum_score", "INTEGER DEFAULT 0")
        ]
        
        # Add missing columns
        for column_name, column_type in required_columns:
            if column_name not in existing_columns:
                try:
                    await db_conn.execute(f"ALTER TABLE signals ADD COLUMN {column_name} {column_type}")
                    log.info(f"✅ Added missing column: {column_name}")
                except Exception as e:
                    log.warning(f"Could not add column {column_name}: {e}")
        
        await db_conn.commit()
        log.info("✅ Database migration complete")
        
    except Exception as e:
        log.error(f"Migration failed: {e}")

# ---------------- DATABASE ----------------
async def init_db():
    global db_conn
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute("PRAGMA journal_mode=WAL;")
    await db_conn.execute("PRAGMA synchronous=NORMAL;")
    
    # Create table with enhanced columns for breakdown
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
            latest_ob TEXT,
            ob_type TEXT,
            sweep_type TEXT,
            momentum_value REAL,
            displacement_value REAL,
            htf_trend_value REAL DEFAULT 0,
            ob_distance_pct REAL DEFAULT 0,
            sweep_score INTEGER DEFAULT 0,
            zone_approach_score INTEGER DEFAULT 0,
            htf_alignment_score INTEGER DEFAULT 0,
            momentum_score INTEGER DEFAULT 0
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

# ---------------- FORMAT NUMBER HELPER (FROM CODE 1) ----------------
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

# ---------------- STRONG TREND DETECTION (DISABLED) ----------------
async def check_strong_counter_trend(exchange, symbol: str, timeframe: str, signal_side: str):
    """
    DISABLED: Trend filter is turned off
    Returns False to allow all trades regardless of HTF trend
    """
    # Always return False - no trend filtering
    return False

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
    tfs = ["15m","1h","4h"]
    for tf in tfs:
        ohlcv = await fetch_ohlcv(exchange, symbol, tf, 50)
        if not ohlcv: return False
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
        
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
            return {
                "type": "bullish",
                "low": min(candle["low"], prev_candle["low"]),
                "high": candle["close"],
                "candle_index": i,
                "candle": candle,
                "prev_candle": prev_candle
            }
        elif candle["close"]<candle["open"] and prev_candle["close"]>prev_candle["open"]:
            return {
                "type": "bearish",
                "low": candle["close"],
                "high": max(candle["high"], prev_candle["high"]),
                "candle_index": i,
                "candle": candle,
                "prev_candle": prev_candle
            }
    return None

# ---------------- TP/SL CALCULATION (RomeOPT-P Style) ----------------
async def romeoptp_tp_sl(exchange, entry: float, side: str, entry_tf: str, ob_zone: dict, symbol: str):
    if not ob_zone:
        return None, None, None, entry_tf
    
    tp_tf = TP_TIMEFRAME_MAP.get(entry_tf, "15m")
    htf_ohlcv = await fetch_ohlcv(exchange, symbol, tp_tf, 100)
    
    if not htf_ohlcv:
        htf_ohlcv = await fetch_ohlcv(exchange, symbol, entry_tf, 100)
        tp_tf = entry_tf
    
    if not htf_ohlcv:
        return None, None, None, tp_tf
    
    df_htf = pd.DataFrame(htf_ohlcv, columns=["ts","open","high","low","close","vol"])
    for c in ["open","high","low","close","vol"]: 
        df_htf[c] = pd.to_numeric(df_htf[c], errors="coerce")
    
    atr_val = float(atr(df_htf, 14).iloc[-1])
    
    if side == "BUY":
        sl = ob_zone['low'] - (atr_val * 0.1)
        risk = entry - sl
        tp1 = entry + (risk * 0.8)  # 0.8R
        tp2 = entry + (risk * 1.6)  # 1.6R
    else:
        sl = ob_zone['high'] + (atr_val * 0.1)
        risk = sl - entry
        tp1 = entry - (risk * 0.8)  # 0.8R
        tp2 = entry - (risk * 1.6)  # 1.6R
    
    return sl, tp1, tp2, tp_tf

# ---------------- UPDATE SIGNAL TP/SL ----------------
async def update_tp_sl_live(sig: dict):
    global exchange
    
    if 'entry_tf' not in sig or 'symbol' not in sig or 'side' not in sig:
        return sig
    
    entry_tf_ohlcv = await fetch_ohlcv(exchange, sig["symbol"], sig["entry_tf"], 50)
    if not entry_tf_ohlcv:
        return sig
    
    df_entry = pd.DataFrame(entry_tf_ohlcv, columns=["ts","open","high","low","close","vol"])
    for c in ["open","high","low","close","vol"]: 
        df_entry[c] = pd.to_numeric(df_entry[c], errors="coerce")
    
    latest_ob = find_latest_ob(df_entry)
    if not latest_ob:
        return sig
    
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
    ohlcv = await fetch_ohlcv(exchange, symbol, timeframe, 50)
    if not ohlcv:
        return "neutral"
    
    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
    for c in ["open","high","low","close","vol"]: 
        df[c] = pd.to_numeric(df[c], errors="coerce")
    
    df['ema20'] = calculate_ema(df, 20)
    ema_slope = df['ema20'].iloc[-1] - df['ema20'].iloc[-3]
    
    recent_closes = df['close'].iloc[-6:]
    direction_sum = 0
    for i in range(1, len(recent_closes)):
        if recent_closes.iloc[i] > recent_closes.iloc[i-1]:
            direction_sum += 1
        else:
            direction_sum -= 1
    
    above_ema = df['close'].iloc[-1] > df['ema20'].iloc[-1]
    
    if ema_slope > 0 and direction_sum >= 2 and above_ema:
        return "bullish"
    elif ema_slope < 0 and direction_sum <= -2 and not above_ema:
        return "bearish"
    else:
        return "neutral"

# ---------------- ROMEOPT SIGNAL GENERATOR WITH ENHANCED BREAKDOWN ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    if df is None or len(df) < 20: 
        return None
    
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    score = 0
    reasons = []
    
    # Store calculation values for enhanced breakdown (like Code 1)
    calc_values = {}

    # Step1: Liquidity Sweep
    sweep_high = last["high"] > prev5["high"].max()
    sweep_low = last["low"] < prev5["low"].min()
    has_sweep = sweep_high or sweep_low
    liquidity_sweep = 2 if has_sweep else 0
    score += liquidity_sweep
    sweep_type = "HIGH" if sweep_high else ("LOW" if sweep_low else "NONE")
    reasons.append(f"Liquidity Sweep +{liquidity_sweep}")
    
    # Store for breakdown
    calc_values["sweep_type"] = sweep_type
    calc_values["sweep_score"] = liquidity_sweep
    
    # Enhanced sweep analysis (from Code 1)
    sweep_analysis = analyze_sweep_details(df)
    calc_values["sweep_details"] = sweep_analysis["details"]

    # Step2: Displacement
    displacement = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    calc_values["displacement_value"] = round(displacement, 2)
    has_disp = displacement > 0.6
    if has_disp: 
        score += 2
        reasons.append(f"Displacement +2 ({displacement:.2f})")
    else: 
        reasons.append(f"Displacement +0 ({displacement:.2f})")

    # Step3&4: OB detection with enhanced details
    ob_zone = find_latest_ob(df)
    if not ob_zone: 
        reasons.append("No OB detected")
        return None
    
    ob_type = ob_zone['type']
    calc_values["ob_type"] = ob_type
    
    # Store detailed OB info for breakdown
    if ob_zone and 'candle' in ob_zone:
        ob_candle = ob_zone['candle']
        prev_ob_candle = ob_zone['prev_candle']
        candles_ago = len(df) - ob_zone['candle_index'] - 1
        ob_mid = (ob_zone['low'] + ob_zone['high']) / 2
        distance_to_price = abs(last["close"] - ob_mid)
        distance_pct = (distance_to_price / last["close"] * 100) if last["close"] > 0 else 100
        
        candle_body = abs(ob_candle["close"] - ob_candle["open"])
        candle_range = ob_candle["high"] - ob_candle["low"]
        strength = candle_body / candle_range if candle_range > 0 else 0
        
        ob_details = {
            "type": ob_type,
            "low": ob_zone['low'],
            "high": ob_zone['high'],
            "midpoint": ob_mid,
            "range": ob_zone['high'] - ob_zone['low'],
            "candles_ago": candles_ago,
            "distance_to_price": distance_to_price,
            "distance_pct": distance_pct,
            "strength": strength,
            "volume": ob_candle["vol"] + prev_ob_candle["vol"],
            "candle_index": ob_zone['candle_index']
        }
        calc_values["ob_details"] = ob_details
        calc_values["ob_distance_pct"] = distance_pct
        calc_values["ob_low"] = round(ob_zone['low'], 6)
        calc_values["ob_high"] = round(ob_zone['high'], 6)
        calc_values["ob_midpoint"] = ob_mid

    # Step5: HTF alignment with IMPROVED detection
    side = "BUY" if ob_zone['type'] == "bullish" else "SELL"
    
    # DISABLED: Check for STRONG counter-trend (always allows trades)
    # should_reject = await check_strong_counter_trend(exchange, symbol, tf, side)
    # if should_reject:
    #     reasons.append(f"Strong HTF trend against {side} → Rejected")
    #     return None
    reasons.append("Counter-Trend Filter: DISABLED")
    
    tf_map = {"1m":"15m", "3m":"30m", "5m":"1h", "15m":"4h", "30m":"1h"}
    htf = tf_map.get(tf, "15m")
    
    htf_trend = await get_htf_trend(exchange, symbol, htf)
    htf_alignment = 0
    
    if htf_trend != "neutral":
        htf_dir = "bullish" if htf_trend == "bullish" else "bearish"
        if htf_dir == ob_zone['type']: 
            htf_alignment = 1
            score += 1
            reasons.append(f"HTF Alignment +1 ({htf}: {htf_trend})")
            calc_values["htf_alignment_score"] = 1
        else:
            reasons.append(f"HTF Misalignment ({htf}: {htf_trend})")
            calc_values["htf_alignment_score"] = 0
        calc_values["htf_direction"] = htf_dir
    else:
        reasons.append(f"HTF Neutral ({htf})")
        calc_values["htf_direction"] = "neutral"
        calc_values["htf_alignment_score"] = 0
    
    # Store HTF trend value for breakdown
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, htf, 50)
    if ohlcv_htf and len(ohlcv_htf) >= 5:
        df_htf = pd.DataFrame(ohlcv_htf, columns=["ts","open","high","low","close","vol"])
        if len(df_htf) >= 5:
            trend_value = df_htf["close"].iloc[-1] - df_htf["close"].iloc[-5]
            calc_values["htf_trend_value"] = round(trend_value, 6)
        else:
            calc_values["htf_trend_value"] = 0
    else:
        calc_values["htf_trend_value"] = 0

    # CRITICAL: Must have minimum score AND displacement
    if score < MIN_SCORE: 
        reasons.append(f"Score {score} < {MIN_SCORE}")
        return None
    
    if not has_disp: 
        reasons.append("No displacement")
        return None

    # Step6: Momentum clean traffic/range avoidance
    momentum_ratio = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    calc_values["momentum_value"] = round(momentum_ratio, 2)
    
    if momentum_ratio >= 0.8:
        score += 1
        reasons.append(f"Momentum +1 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 1
    else:
        reasons.append(f"Momentum +0 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 0

    # Calculate TP/SL with RomeOPT-P logic
    sl, tp1, tp2, tp_tf = await romeoptp_tp_sl(exchange, float(last["close"]), side, tf, ob_zone, symbol)
    if sl is None or tp1 is None or tp2 is None:
        reasons.append("TP/SL calc failed")
        return None
    
    # Check Zone Approach (like Code 1)
    zone_approach = 0
    if ob_type == "bullish" and last["close"] <= ob_zone['high']:
        zone_approach = 1
    elif ob_type == "bearish" and last["close"] >= ob_zone['low']:
        zone_approach = 1
    calc_values["zone_approach_score"] = zone_approach
    
    sig = {
        "symbol": symbol,
        "side": side,
        "entry": float(last["close"]),
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "entry_tf": tf,
        "tp_tf": tp_tf,
        "score": int(score),
        "reason": "RomeOPT-P 6-Step",
        "reason_list": reasons,
        "calc_values": calc_values,
        "ob_zone": ob_zone
    }

    # Liquidity path filter: skip blocked trades
    if side == "BUY" and any(df['high'].iloc[-20:] >= sig['tp1']): 
        reasons.append("Liquidity Path Blocked")
        return None
    if side == "SELL" and any(df['low'].iloc[-20:] <= sig['tp1']): 
        reasons.append("Liquidity Path Blocked")
        return None

    return sig

# ---------------- ENHANCED BREAKDOWN FORMATTING (FROM CODE 1) ----------------
async def send_enhanced_breakdown(sig: dict):
    """Send enhanced breakdown like Code 1"""
    calc = sig.get("calc_values", {})
    symbol = sig["symbol"]
    side = sig["side"]
    entry = sig["entry"]
    score = sig["score"]
    entry_tf = sig.get("entry_tf", "")
    
    # Calculate risk and TP distances
    risk = abs(sig.get('entry', 0) - sig.get('sl', 0))
    tp1_dist = abs(sig.get('tp1', 0) - sig.get('entry', 0)) if sig.get('tp1') else 0
    tp2_dist = abs(sig.get('tp2', 0) - sig.get('entry', 0)) if sig.get('tp2') else 0
    
    # Calculate R multiples
    tp1_r = tp1_dist / risk if risk > 0 else 0
    tp2_r = tp2_dist / risk if risk > 0 else 0
    
    # Start building enhanced breakdown
    breakdown_lines = [
        f"🏆 {symbol} ({entry_tf}) {side}",
        f"Entry: {entry:.6f} | Score: {score}/6",
        f""
    ]
    
    # 📊 SWEEP DETAILS SECTION
    breakdown_lines.append(f"⚡ LIQUIDITY SWEEP DETAILS:")
    sweep_type = calc.get('sweep_type', 'NONE')
    sweep_details = calc.get('sweep_details', {})
    
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
    
    # 📊 ORDER BLOCK DETAILS SECTION
    breakdown_lines.append(f"🔷 ORDER BLOCK DETAILS:")
    ob_type = calc.get('ob_type', 'NONE')
    ob_details = calc.get('ob_details', {})
    
    if ob_type != 'NONE' and ob_details:
        ob_low = calc.get('ob_low', 0)
        ob_high = calc.get('ob_high', 0)
        ob_range = ob_high - ob_low
        ob_mid = (ob_high + ob_low) / 2
        distance_to_entry = abs(entry - ob_mid)
        distance_pct = (distance_to_entry / entry * 100) if entry > 0 else 0
        in_zone = True if (ob_type == 'bullish' and entry <= ob_high) or (ob_type == 'bearish' and entry >= ob_low) else False
        
        breakdown_lines.extend([
            f"  • Type: {ob_type.upper()} OB",
            f"  • Zone Approach: +{calc.get('zone_approach_score', 0)}",
            f"  • OB Range: {format_number(ob_low)} - {format_number(ob_high)}",
            f"  • Range Size: {format_number(ob_range)}",
            f"  • Midpoint: {format_number(ob_mid)}",
            f"  • Distance to Entry: {format_number(distance_to_entry)} ({distance_pct:.2f}%)",
            f"  • In Zone: {'✅ YES' if in_zone else '❌ NO'}",
            f"  • Strength: {ob_details.get('strength', 0):.2f}",
            f"  • Age: {ob_details.get('candles_ago', 0)} candles ago",
            f"  • Volume: {format_number(ob_details.get('volume', 0))}"
        ])
    elif ob_type != 'NONE':
        ob_low = calc.get('ob_low', 0)
        ob_high = calc.get('ob_high', 0)
        ob_range = ob_high - ob_low
        ob_mid = (ob_high + ob_low) / 2
        distance_to_entry = abs(entry - ob_mid)
        distance_pct = (distance_to_entry / entry * 100) if entry > 0 else 0
        
        breakdown_lines.extend([
            f"  • Type: {ob_type.upper()} OB",
            f"  • Zone Approach: +{calc.get('zone_approach_score', 0)}",
            f"  • OB Range: {format_number(ob_low)} - {format_number(ob_high)}",
            f"  • Range Size: {format_number(ob_range)}",
            f"  • Midpoint: {format_number(ob_mid)}",
            f"  • Distance to Entry: {format_number(distance_to_entry)} ({distance_pct:.2f}%)"
        ])
    else:
        breakdown_lines.append(f"  • No order block detected")
    
    breakdown_lines.append(f"")
    
    # 📊 KEY METRICS SECTION
    breakdown_lines.append(f"📊 KEY METRICS:")
    breakdown_lines.extend([
        f"  • Displacement: {calc.get('displacement_value', 0):.2f} ({'✅ STRONG' if calc.get('displacement_value', 0) >= 0.6 else '⚠️ WEAK'})",
        f"  • Momentum: {calc.get('momentum_value', 0):.2f} {'✅ PASS' if calc.get('momentum_value', 0) >= 0.8 else '❌ FAIL'}",
        f"  • HTF Trend: {calc.get('htf_trend_value', 0):+.6f}",
        f"  • HTF Direction: {calc.get('htf_direction', '?')}",
        f"  • HTF Alignment Score: +{calc.get('htf_alignment_score', 0)}",
        f"  • Momentum Score: +{calc.get('momentum_score', 0)}",
        f"  • Counter-Trend Filter: 🚫 DISABLED (allows all trades)"
    ])
    
    breakdown_lines.append(f"")
    
    # 🎯 ROMEOPT-P TARGETS SECTION
    breakdown_lines.append(f"🎯 TARGETS (RomeOPT-P Style):")
    breakdown_lines.extend([
        f"  SL: {format_number(sig.get('sl', 0))}",
        f"  TP1: {format_number(sig.get('tp1', 0))} ({tp1_r:.1f}R) → SL→BE after hit",
        f"  TP2: {format_number(sig.get('tp2', 0))} ({tp2_r:.1f}R) → Close position",
        f"  Risk: {format_number(risk)}",
        f"  Entry TF: {entry_tf} | TP TF: {sig.get('tp_tf', '')}",
        f"  RomeOPT-P: TP1=0.8R, TP2=1.6R, SL→BE after TP1"
    ])
    
    # Clean up empty lines
    breakdown_lines = [line for line in breakdown_lines if line != ""]
    
    # Send to Telegram
    try:
        await tg("\n".join(breakdown_lines))
    except Exception as e:
        log.error(f"Failed to send enhanced breakdown: {e}")
    
    return breakdown_lines

# ---------------- DATABASE LOGGING ----------------
async def log_signal(sig):
    async with db_lock:
        calc = sig.get("calc_values", {})
        await db_conn.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,entry_tf,tp_tf,timestamp,status,reason,score,latest_ob,ob_type,sweep_type,momentum_value,displacement_value,htf_trend_value,ob_distance_pct,sweep_score,zone_approach_score,htf_alignment_score,momentum_score)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            sig["symbol"], 
            sig["side"], 
            sig["entry"], 
            sig["sl"], 
            sig["tp1"], 
            sig["tp2"],
            sig.get("entry_tf", ""), 
            sig.get("tp_tf", ""),
            datetime.datetime.utcnow().isoformat(), 
            "OPEN", 
            sig["reason"], 
            int(sig["score"]), 
            str(sig.get("ob_zone", "")),
            calc.get("ob_type", ""),
            calc.get("sweep_type", ""),
            calc.get("momentum_value", 0),
            calc.get("displacement_value", 0),
            calc.get("htf_trend_value", 0),
            calc.get("ob_distance_pct", 0),
            calc.get("sweep_score", 0),
            calc.get("zone_approach_score", 0),
            calc.get("htf_alignment_score", 0),
            calc.get("momentum_score", 0)
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

                        sig = {
                            "symbol": symbol, "side": side, "entry": entry, 
                            "sl": sl, "tp1": tp1, "tp2": tp2, "entry_tf": entry_tf
                        }
                        sig = await update_tp_sl_live(sig)
                        sl, tp1, tp2 = sig["sl"], sig["tp1"], sig["tp2"]

                        hits=[]; sl_hit=False
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
                        # Send enhanced breakdown (like Code 1)
                        await send_enhanced_breakdown(sig)
                        
                        await log_signal(sig)
                        last_signal_time[key] = time.time()
                        signals_found += 1
            log.info(f"📊 Scan complete: {signals_found} RomeOPT-P signals found (Enhanced Breakdown, No Trend Filter)")
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

# ---------------- MAIN ----------------
async def main():
    global exchange, db_conn
    await init_db()
    exchange = ccxt.okx({"enableRateLimit": True})
    await tg("🏆 ROMEOPT 6-Step Scanner Started - Live Early Signals")
    await tg("🚫 COUNTER-TREND FILTER: DISABLED (Allows all trades)")
    await tg("📊 ENHANCED BREAKDOWN ACTIVATED: Full OB & Sweep Details")
    await tg("💰 ROMEOPT-P STYLE: TP1=0.8R, TP2=1.6R, SL→BE after TP1")
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