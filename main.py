#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TRUE ROMEOPT SCANNER (Final Refined Version) - WITH ENHANCED SWEEP & OB DATA
- RomeOPT 6-step entry logic
- TRUE RomeOPT TP: ONE liquidity target, no TP ladders
- Simple but accurate market state detection
- ATR-based tolerance for liquidity detection
- External liquidity = range extremes (not local swings)
- TP LOCK: No recalculation after entry
- Telegram alerts + SQLite logging
- Forced Filter: Momentum ≥ 0.87 OR (Momentum ≥ 0.85 AND Displacement ≥ 0.80)
- ENHANCED: Comprehensive liquidity sweep and order block data
"""

import os, time, asyncio, logging, datetime
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
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
MIN_SCORE = 4
CRITICAL_FACTORS_MIN = 2

# ---------------- FORCED FILTER PARAMETERS ----------------
MOMENTUM_STRONG_THRESHOLD = 0.60
MOMENTUM_GOOD_THRESHOLD = 0.55
DISPLACEMENT_MIN_THRESHOLD = 0.50

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_bot")
db_lock = asyncio.Lock()
db_conn = None

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

# ---------------- COMPLETE DATABASE MIGRATION ----------------
async def migrate_db():
    """Complete database migration from old schema to new schema"""
    global db_conn
    
    try:
        # Check if table exists at all
        async with db_conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='signals'") as cursor:
            table_exists = await cursor.fetchone()
        
        if not table_exists:
            log.info("Table 'signals' doesn't exist yet, will create new schema")
            return
        
        # Get current columns
        async with db_conn.execute("PRAGMA table_info(signals)") as cursor:
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]
        
        log.info(f"Current columns: {column_names}")
        
        # List of required columns for new schema
        required_columns = {
            'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
            'symbol': 'TEXT',
            'side': 'TEXT',
            'entry': 'REAL',
            'sl': 'REAL',
            'tp': 'REAL',
            'timestamp': 'TEXT',
            'status': 'TEXT',
            'reason': 'TEXT',
            'score': 'INTEGER',
            'tp_hit': 'INTEGER DEFAULT 0',
            'latest_ob': 'TEXT',
            'tp_type': 'TEXT',
            'tp_locked': 'INTEGER DEFAULT 1'
        }
        
        # Add missing columns
        for col_name, col_type in required_columns.items():
            if col_name not in column_names:
                try:
                    await db_conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_type}")
                    log.info(f"✅ Added missing column: {col_name}")
                except Exception as e:
                    log.warning(f"Could not add column {col_name}: {e}")
        
        # If old TP columns exist, migrate data from tp1 to tp
        if 'tp1' in column_names and 'tp' in column_names:
            # Check if tp column is empty but tp1 has data
            async with db_conn.execute("SELECT COUNT(*) FROM signals WHERE tp IS NULL AND tp1 IS NOT NULL") as cursor:
                count = await cursor.fetchone()
                if count and count[0] > 0:
                    log.info(f"🚀 Migrating {count[0]} records from tp1 to tp...")
                    await db_conn.execute("UPDATE signals SET tp = tp1 WHERE tp IS NULL AND tp1 IS NOT NULL")
                    
                    # Also migrate tp1_hit to tp_hit if needed
                    if 'tp1_hit' in column_names:
                        await db_conn.execute("UPDATE signals SET tp_hit = tp1_hit WHERE tp_hit = 0 AND tp1_hit = 1")
                    
                    log.info("✅ Data migration complete")
        
        await db_conn.commit()
        
    except Exception as e:
        log.error(f"Migration error: {e}")

# ---------------- INIT DATABASE ----------------
async def init_db():
    global db_conn
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute("PRAGMA journal_mode=WAL;")
    await db_conn.execute("PRAGMA synchronous=NORMAL;")
    
    # Create table if it doesn't exist (NEW SCHEMA)
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            side TEXT,
            entry REAL,
            sl REAL,
            tp REAL,
            timestamp TEXT,
            status TEXT,
            reason TEXT,
            score INTEGER,
            tp_hit INTEGER DEFAULT 0,
            latest_ob TEXT,
            tp_type TEXT,
            tp_locked INTEGER DEFAULT 1
        );
    """)
    await db_conn.commit()
    
    # Run complete migration
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
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.DataFrame({
        "h-l": high - low,
        "h-pc": (high - close.shift(1)).abs(),
        "l-pc": (low - close.shift(1)).abs()
    }).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()

# ---------------- FORCED FILTER FUNCTION ----------------
def force_filter_trade(momentum_value: float, displacement_value: float) -> bool:
    if momentum_value >= MOMENTUM_STRONG_THRESHOLD:
        return True
    if momentum_value >= MOMENTUM_GOOD_THRESHOLD and displacement_value >= DISPLACEMENT_MIN_THRESHOLD:
        return True
    return False

# ---------------- REFINED ROMEOPT MARKET STATE ----------------
def romeopt_market_state(df, atr_val):
    """
    REFINED RomeOPT market state detection
    Checks: Strong displacement + actual price movement
    """
    if len(df) < 3:
        return "BALANCED"
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    body_ratio = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    candle_size = last["high"] - last["low"]
    price_movement = abs(last["close"] - prev["close"])
    
    # RomeOPT logic: Strong displacement with actual follow-through
    strong_displacement = (
        body_ratio > 0.7 and                    # Strong body
        candle_size > atr_val * 1.2 and         # Large candle
        price_movement > atr_val * 0.5          # Actual price movement
    )
    
    return "IMBALANCED" if strong_displacement else "BALANCED"

# ---------------- FIXED: LIQUIDITY CONSUMPTION CHECK ----------------
def is_liquidity_consumed(df, tp, side, atr_val, lookback=10):
    """
    FIXED: Liquidity is only consumed when price CLOSES BEYOND it with momentum
    """
    recent_candles = min(lookback, len(df))
    
    for i in range(1, recent_candles + 1):
        candle = df.iloc[-i]
        
        if side == "SELL":
            close_below = candle["close"] < tp
            momentum = (candle["close"] < candle["open"]) and \
                       (abs(candle["close"] - candle["open"]) > atr_val * 0.3)
            
            if close_below and momentum:
                return True
                
        else:  # BUY
            close_above = candle["close"] > tp
            momentum = (candle["close"] > candle["open"]) and \
                       (abs(candle["close"] - candle["open"]) > atr_val * 0.3)
            
            if close_above and momentum:
                return True
    
    return False

# ---------------- FIXED: ROMEOPT INTERNAL LIQUIDITY ----------------
def romeopt_internal_liquidity(df, side, atr_val, lookback=15):
    """
    FIXED RomeOPT internal liquidity detection with detailed logging
    """
    if side == "SELL":
        lows = df['low'].iloc[-lookback:].dropna()
        if len(lows) < 5:
            log.debug("❌ INTERNAL LIQ: Not enough low data (SELL)")
            return None
        
        price_median = lows.median()
        tolerance_percentage = 0.001
        tolerance = price_median * tolerance_percentage
        min_tolerance = atr_val * 0.5
        tolerance = max(tolerance, min_tolerance)
        max_tolerance = atr_val * 2.0
        tolerance = min(tolerance, max_tolerance)
        
        log.debug(f"🔍 INTERNAL LIQ SELL: Price median={price_median:.6f}, ATR={atr_val:.6f}, Tolerance={tolerance:.6f}")
        log.debug(f"   Lows in lookback: {list(lows.round(6))}")
        
        potential_targets = []
        price_levels = sorted(lows.unique())
        
        current_zone = []
        zones = []
        
        for price in price_levels:
            if not current_zone:
                current_zone.append(price)
            elif price - current_zone[-1] <= tolerance * 2:
                current_zone.append(price)
            else:
                zones.append(current_zone.copy())
                current_zone = [price]
        
        if current_zone:
            zones.append(current_zone)
        
        log.debug(f"   Found {len(zones)} potential zones")
        
        for idx, zone in enumerate(zones):
            if len(zone) >= 2:
                zone_low = min(zone)
                zone_high = max(zone)
                touch_count = sum(1 for low in lows if zone_low <= low <= zone_high)
                density = touch_count / len(lows)
                
                potential_targets.append({
                    'price': zone_low,
                    'strength': touch_count,
                    'density': density,
                    'zone_range': (zone_low, zone_high)
                })
                
                log.debug(f"   Zone {idx}: {zone_low:.6f}-{zone_high:.6f}, {touch_count} touches, density={density:.2f}")
        
        if potential_targets:
            best = max(potential_targets, key=lambda x: (x['strength'], -x['price']))
            log.debug(f"✅ INTERNAL LIQ SELL: Best zone at {best['price']:.6f} ({best['strength']} touches)")
            return best['price']
        else:
            log.debug("❌ INTERNAL LIQ SELL: No valid zones found (need ≥2 touches)")
            return None
        
    else:  # BUY
        highs = df['high'].iloc[-lookback:].dropna()
        if len(highs) < 5:
            log.debug("❌ INTERNAL LIQ: Not enough high data (BUY)")
            return None
        
        price_median = highs.median()
        tolerance_percentage = 0.001
        tolerance = price_median * tolerance_percentage
        min_tolerance = atr_val * 0.5
        tolerance = max(tolerance, min_tolerance)
        max_tolerance = atr_val * 2.0
        tolerance = min(tolerance, max_tolerance)
        
        log.debug(f"🔍 INTERNAL LIQ BUY: Price median={price_median:.6f}, ATR={atr_val:.6f}, Tolerance={tolerance:.6f}")
        log.debug(f"   Highs in lookback: {list(highs.round(6))}")
        
        potential_targets = []
        price_levels = sorted(highs.unique())
        current_zone = []
        zones = []
        
        for price in price_levels:
            if not current_zone:
                current_zone.append(price)
            elif price - current_zone[-1] <= tolerance * 2:
                current_zone.append(price)
            else:
                zones.append(current_zone.copy())
                current_zone = [price]
        
        if current_zone:
            zones.append(current_zone)
        
        log.debug(f"   Found {len(zones)} potential zones")
        
        for idx, zone in enumerate(zones):
            if len(zone) >= 2:
                zone_low = min(zone)
                zone_high = max(zone)
                touch_count = sum(1 for high in highs if zone_low <= high <= zone_high)
                density = touch_count / len(highs)
                
                potential_targets.append({
                    'price': zone_high,
                    'strength': touch_count,
                    'density': density,
                    'zone_range': (zone_low, zone_high)
                })
                
                log.debug(f"   Zone {idx}: {zone_low:.6f}-{zone_high:.6f}, {touch_count} touches, density={density:.2f}")
        
        if potential_targets:
            best = max(potential_targets, key=lambda x: (x['strength'], x['price']))
            log.debug(f"✅ INTERNAL LIQ BUY: Best zone at {best['price']:.6f} ({best['strength']} touches)")
            return best['price']
        else:
            log.debug("❌ INTERNAL LIQ BUY: No valid zones found (need ≥2 touches)")
            return None
    
    return None

# ---------------- REFINED ROMEOPT EXTERNAL LIQUIDITY ----------------
def romeopt_external_liquidity(df, side, lookback=50):
    """
    REFINED RomeOPT external liquidity detection
    """
    if side == "SELL":
        ext_liq = df['low'].iloc[-lookback:].min()
        log.debug(f"🔍 EXTERNAL LIQ SELL: Range low = {ext_liq:.6f} (lookback {lookback})")
        return ext_liq
    else:  # BUY
        ext_liq = df['high'].iloc[-lookback:].max()
        log.debug(f"🔍 EXTERNAL LIQ BUY: Range high = {ext_liq:.6f} (lookback {lookback})")
        return ext_liq

# ---------------- FIXED: ROMEOPT TP DECISION WITH DETAILED LOGGING ----------------
def romeopt_tp_sl(entry, side, atr_val, ob_zone, df):
    """
    FIXED RomeOPT TP logic with detailed rejection logging
    """
    log.debug(f"🔍 ROMEOPT TP CALCULATION START: {side} {entry:.6f}, ATR={atr_val:.6f}")
    
    # Step 1: Determine market state
    market_state = romeopt_market_state(df, atr_val)
    log.debug(f"   Market State: {market_state}")
    
    # Step 2: Find liquidity based on market state
    tp = None
    tp_type = ""
    
    if market_state == "BALANCED":
        log.debug("   Market BALANCED → Looking for INTERNAL liquidity")
        tp = romeopt_internal_liquidity(df, side, atr_val)
        if tp:
            tp_type = f"RANGE: Visual {'Lows' if side == 'SELL' else 'Highs'} Cluster"
            log.debug(f"✅ Found INTERNAL liquidity: {tp:.6f}")
    else:  # IMBALANCED
        log.debug("   Market IMBALANCED → Looking for EXTERNAL liquidity")
        tp = romeopt_external_liquidity(df, side)
        if tp:
            tp_type = f"TREND: Range {'Low' if side == 'SELL' else 'High'}"
            log.debug(f"✅ Found EXTERNAL liquidity: {tp:.6f}")
    
    # REJECT if no obvious liquidity found
    if tp is None:
        log.debug(f"❌ ROMEOPT TP REJECTED: No liquidity found for {side} | Market: {market_state}")
        log.debug(f"   Entry: {entry:.6f}, ATR: {atr_val:.6f}")
        return None
    
    log.debug(f"✅ Potential TP found: {tp:.6f} ({tp_type})")
    
    # FIXED: Step 3 - Check if liquidity was CONSUMED, not just touched
    if is_liquidity_consumed(df, tp, side, atr_val):
        log.debug(f"❌ ROMEOPT TP REJECTED: Liquidity CONSUMED at {tp:.6f}")
        log.debug(f"   Price closed beyond TP with momentum")
        return None
    else:
        log.debug(f"✅ Liquidity NOT consumed at {tp:.6f}")
    
    # Step 4: Calculate SL (keep OB-based SL)
    if side == "BUY":
        sl = ob_zone["low"] - (atr_val * 0.3)
        recent_low = df['low'].iloc[-10:].min()
        sl = min(sl, recent_low - (atr_val * 0.3))
        
        # Ensure minimum risk
        min_risk = atr_val * 0.5
        risk = entry - sl
        if risk < min_risk:
            risk = min_risk
            sl = entry - risk
        
        log.debug(f"   BUY SL Calculation:")
        log.debug(f"   - OB low: {ob_zone['low']:.6f}")
        log.debug(f"   - Recent low: {recent_low:.6f}")
        log.debug(f"   - Initial SL: {sl:.6f}")
        log.debug(f"   - Risk: {risk:.6f}")
        log.debug(f"   - Min risk required: {min_risk:.6f}")
        
        # Ensure TP is valid (above entry, at least 0.5R)
        if tp <= entry:
            log.debug(f"❌ ROMEOPT TP REJECTED: TP {tp:.6f} not above entry {entry:.6f} for BUY")
            return None
            
        reward = tp - entry
        log.debug(f"   Reward: {reward:.6f}")
        
        if reward < risk * 0.5:
            log.debug(f"❌ ROMEOPT TP REJECTED: TP reward {reward/risk:.2f}R < 0.5R minimum")
            log.debug(f"   Reward: {reward:.6f}, Risk: {risk:.6f}, R:R: {reward/risk:.2f}")
            return None
        
    else:  # SELL
        sl = ob_zone["high"] + (atr_val * 0.3)
        recent_high = df['high'].iloc[-10:].max()
        sl = max(sl, recent_high + (atr_val * 0.3))
        
        min_risk = atr_val * 0.5
        risk = sl - entry
        if risk < min_risk:
            risk = min_risk
            sl = entry + risk
        
        log.debug(f"   SELL SL Calculation:")
        log.debug(f"   - OB high: {ob_zone['high']:.6f}")
        log.debug(f"   - Recent high: {recent_high:.6f}")
        log.debug(f"   - Initial SL: {sl:.6f}")
        log.debug(f"   - Risk: {risk:.6f}")
        log.debug(f"   - Min risk required: {min_risk:.6f}")
        
        # Ensure TP is valid (below entry, at least 0.5R)
        if tp >= entry:
            log.debug(f"❌ ROMEOPT TP REJECTED: TP {tp:.6f} not below entry {entry:.6f} for SELL")
            return None
            
        reward = entry - tp
        log.debug(f"   Reward: {reward:.6f}")
        
        if reward < risk * 0.5:
            log.debug(f"❌ ROMEOPT TP REJECTED: TP reward {reward/risk:.2f}R < 0.5R minimum")
            log.debug(f"   Reward: {reward:.6f}, Risk: {risk:.6f}, R:R: {reward/risk:.2f}")
            return None
    
    log.info(f"✅ ROMEOPT TP ACCEPTED: {side} {entry:.6f}")
    log.info(f"   Market: {market_state}")
    log.info(f"   Liquidity Target: {tp:.6f} ({tp_type})")
    log.info(f"   SL: {sl:.6f} | Risk: {risk:.6f} | R:R: {abs(tp-entry)/risk:.2f}:1")
    
    return sl, tp, tp_type

# ---------------- ENHANCED ORDER BLOCK DETECTION ----------------
def find_latest_ob(df: pd.DataFrame, lookback=50):
    """
    Enhanced Order Block detection with detailed classification
    """
    blocks = []
    
    for i in range(max(2, len(df) - lookback), len(df) - 1):
        candle = df.iloc[i]
        prev_candle = df.iloc[i-1]
        
        if (prev_candle["close"] < prev_candle["open"] and
            candle["close"] > candle["open"] and
            candle["close"] > prev_candle["close"]):
            
            block = {
                "type": "BULLISH_OB",
                "index": i,
                "timestamp": candle.name if hasattr(candle, 'name') else i,
                "low": min(candle["low"], prev_candle["low"]),
                "high": max(candle["close"], prev_candle["close"]),
                "body_low": min(candle["open"], candle["close"]),
                "body_high": max(candle["open"], candle["close"]),
                "volume": candle["vol"] if "vol" in candle else 0,
                "candle_size": candle["high"] - candle["low"],
                "body_size": abs(candle["close"] - candle["open"]),
                "wick_ratio": (candle["high"] - max(candle["open"], candle["close"])) / 
                              (candle["high"] - candle["low"]) if (candle["high"] - candle["low"]) > 0 else 0
            }
            blocks.append(block)
        
        elif (prev_candle["close"] > prev_candle["open"] and
              candle["close"] < candle["open"] and
              candle["close"] < prev_candle["close"]):
            
            block = {
                "type": "BEARISH_OB",
                "index": i,
                "timestamp": candle.name if hasattr(candle, 'name') else i,
                "low": min(candle["close"], prev_candle["close"]),
                "high": max(candle["high"], prev_candle["high"]),
                "body_low": min(candle["open"], candle["close"]),
                "body_high": max(candle["open"], candle["close"]),
                "volume": candle["vol"] if "vol" in candle else 0,
                "candle_size": candle["high"] - candle["low"],
                "body_size": abs(candle["close"] - candle["open"]),
                "wick_ratio": (min(candle["open"], candle["close"]) - candle["low"]) / 
                              (candle["high"] - candle["low"]) if (candle["high"] - candle["low"]) > 0 else 0
            }
            blocks.append(block)
    
    if blocks:
        latest_block = max(blocks, key=lambda x: x["index"])
        
        body_ratio = latest_block["body_size"] / latest_block["candle_size"] if latest_block["candle_size"] > 0 else 0
        if body_ratio >= 0.7:
            latest_block["strength"] = "STRONG"
        elif body_ratio >= 0.5:
            latest_block["strength"] = "MODERATE"
        else:
            latest_block["strength"] = "WEAK"
        
        if latest_block["type"] == "BULLISH_OB":
            subsequent_candles = df.iloc[latest_block["index"]+1:min(latest_block["index"]+10, len(df))]
            latest_block["tested"] = any(candle["low"] <= latest_block["high"] for _, candle in subsequent_candles.iterrows())
        else:
            subsequent_candles = df.iloc[latest_block["index"]+1:min(latest_block["index"]+10, len(df))]
            latest_block["tested"] = any(candle["high"] >= latest_block["low"] for _, candle in subsequent_candles.iterrows())
        
        log.debug(f"✅ Found {latest_block['type']} OB: {latest_block['low']:.6f}-{latest_block['high']:.6f}")
        return latest_block
    
    log.debug("❌ No Order Block found")
    return None

# ---------------- SIGNAL GENERATION WITH REJECTION LOGGING ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    if df is None or len(df) < 20: 
        log.debug(f"❌ {symbol} {tf}: Not enough data (<20 candles)")
        return None
    
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    score = 0
    reasons = []
    
    calc_values = {}

    log.debug(f"🔍 SIGNAL CHECK: {symbol} {tf} at {last['close']:.6f}")

    # Step 1: Liquidity Sweep
    lookback_period = 20
    high_lookback = df['high'].iloc[-lookback_period:-1]
    low_lookback = df['low'].iloc[-lookback_period:-1]
    
    sweep_high = last["high"] > high_lookback.max()
    sweep_low = last["low"] < low_lookback.min()
    
    respected_high_sweep = False
    respected_low_sweep = False
    sweep_strength = 0.0
    
    if sweep_high:
        sweep_amount = last["high"] - high_lookback.max()
        candle_range = last["high"] - last["low"]
        if candle_range > 0:
            sweep_strength = sweep_amount / candle_range
        if last["close"] < high_lookback.max():
            respected_high_sweep = True
    
    if sweep_low:
        sweep_amount = low_lookback.min() - last["low"]
        candle_range = last["high"] - last["low"]
        if candle_range > 0:
            sweep_strength = sweep_amount / candle_range
        if last["close"] > low_lookback.min():
            respected_low_sweep = True
    
    has_sweep = (sweep_high and respected_high_sweep) or (sweep_low and respected_low_sweep)
    liquidity_sweep = 2 if has_sweep else 0
    score += liquidity_sweep
    
    if sweep_high and respected_high_sweep:
        sweep_type = "HIGH_SWEEP_RESPECTED"
        sweep_direction = "BEARISH"
    elif sweep_low and respected_low_sweep:
        sweep_type = "LOW_SWEEP_RESPECTED"
        sweep_direction = "BULLISH"
    else:
        sweep_type = "NONE"
        sweep_direction = "NONE"
    
    reasons.append(f"Liquidity Sweep +{liquidity_sweep} ({sweep_type})")
    calc_values["sweep_type"] = sweep_type
    calc_values["sweep_direction"] = sweep_direction
    calc_values["sweep_score"] = liquidity_sweep
    calc_values["sweep_strength"] = round(sweep_strength, 2) if has_sweep else 0
    calc_values["sweep_respected"] = respected_high_sweep or respected_low_sweep
    calc_values["swept_level"] = float(high_lookback.max()) if sweep_high else (float(low_lookback.min()) if sweep_low else 0.0)

    log.debug(f"   Sweep: {sweep_type}, Score: {liquidity_sweep}/2")

    # Step 2: Displacement
    displacement = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    calc_values["displacement_value"] = round(displacement, 2)
    has_disp = displacement > 0.6
    if has_disp:
        score += 2; reasons.append(f"Displacement +2 ({displacement:.2f})")
        log.debug(f"✅ Displacement: {displacement:.2f} (+2)")
    else:
        reasons.append(f"Displacement +0 ({displacement:.2f})")
        log.debug(f"❌ Displacement: {displacement:.2f} (<0.6)")

    # Step 3 & 4: Order Block & Zone
    ob_zone = find_latest_ob(df, lookback=30)
    
    if ob_zone:
        ob_type = "bullish" if ob_zone["type"] == "BULLISH_OB" else "bearish"
        zone_approach = 0
        
        if ob_type == "bullish":
            distance_to_ob = (last["close"] - ob_zone["high"]) / (ob_zone["high"] - ob_zone["low"] + 1e-8)
            if last["close"] <= ob_zone["high"] or distance_to_ob < 0.1:
                score += 1
                zone_approach = 1
                approach_status = f"APPROACHING (dist: {distance_to_ob:.2%})"
                log.debug(f"✅ Zone Approach: APPROACHING bullish OB")
            else:
                approach_status = f"FAR ({distance_to_ob:.2%} away)"
                log.debug(f"❌ Zone Approach: FAR from bullish OB ({distance_to_ob:.2%})")
        else:
            distance_to_ob = (ob_zone["low"] - last["close"]) / (ob_zone["high"] - ob_zone["low"] + 1e-8)
            if last["close"] >= ob_zone["low"] or distance_to_ob < 0.1:
                score += 1
                zone_approach = 1
                approach_status = f"APPROACHING (dist: {distance_to_ob:.2%})"
                log.debug(f"✅ Zone Approach: APPROACHING bearish OB")
            else:
                approach_status = f"FAR ({distance_to_ob:.2%} away)"
                log.debug(f"❌ Zone Approach: FAR from bearish OB ({distance_to_ob:.2%})")
        
        reasons.append(f"Zone Approach +{zone_approach} ({approach_status})")
        
        calc_values["zone_approach"] = zone_approach
        calc_values["ob_type"] = ob_type
        calc_values["ob_strength"] = ob_zone.get("strength", "UNKNOWN")
        calc_values["ob_tested"] = ob_zone.get("tested", False)
        calc_values["ob_low"] = round(ob_zone["low"], 6)
        calc_values["ob_high"] = round(ob_zone["high"], 6)
        calc_values["distance_to_ob"] = round(distance_to_ob, 4)
    else:
        reasons.append("Zone Approach +0 (No OB detected)")
        ob_type = None
        calc_values["zone_approach"] = 0
        calc_values["ob_type"] = "NONE"
        log.debug(f"❌ No Order Block found")

    # Step 5: HTF Alignment
    tf_map={"1m":"15m","3m":"30m","5m":"1h","15m":"4h","30m":"1h"}
    htf=tf_map.get(tf,"15m")
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, htf, 50)
    htf_alignment = 0
    htf_trend_value = 0
    
    if ohlcv_htf and len(ohlcv_htf) >= 5:
        df_htf = pd.DataFrame(ohlcv_htf, columns=["ts","open","high","low","close","vol"])
        if len(df_htf) >= 5:
            trend = df_htf["close"].iloc[-1] - df_htf["close"].iloc[-5]
            htf_trend_value = round(trend, 6)
            htf_dir = "bullish" if trend>0 else "bearish"
            if ob_type and htf_dir==ob_type:
                score+=1; htf_alignment=1
                reasons.append(f"HTF Alignment +1 ({htf_dir} {trend:+.6f})")
                log.debug(f"✅ HTF Alignment: {htf_dir} matches OB {ob_type}")
            else:
                reasons.append(f"HTF Alignment +0 ({htf_dir} {trend:+.6f})")
                log.debug(f"❌ HTF Alignment: {htf_dir} doesn't match OB {ob_type}")
            calc_values["htf_trend"] = htf_trend_value
            calc_values["htf_direction"] = htf_dir
        else:
            reasons.append("HTF Alignment ? (insufficient data)")
            calc_values["htf_trend"] = 0
            calc_values["htf_direction"] = "UNKNOWN"
            log.debug(f"❌ HTF Alignment: insufficient HTF data")
    else:
        reasons.append("HTF Alignment ? (no data)")
        calc_values["htf_trend"] = 0
        calc_values["htf_direction"] = "UNKNOWN"
        log.debug(f"❌ HTF Alignment: no HTF data")

    # Step 6: MOMENTUM
    momentum_ratio = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    calc_values["momentum_value"] = round(momentum_ratio, 2)
    
    if ob_type=="bullish" and momentum_ratio>=0.8 and last["close"]>last["open"]:
        score+=1; reasons.append(f"Momentum +1 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 1
        log.debug(f"✅ Momentum: {momentum_ratio:.2f} (+1)")
    elif ob_type=="bearish" and momentum_ratio>=0.8 and last["close"]<last["open"]:
        score+=1; reasons.append(f"Momentum +1 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 1
        log.debug(f"✅ Momentum: {momentum_ratio:.2f} (+1)")
    else:
        reasons.append(f"Momentum +0 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 0
        log.debug(f"❌ Momentum: {momentum_ratio:.2f} (<0.8 or wrong direction)")

    if not ob_type: 
        log.debug(f"❌ SIGNAL REJECTED: No OB type")
        return None
    
    side_str = "BUY" if ob_type=="bullish" else "SELL"
    entry = float(last["close"])

    # ---------------- CRITICAL FILTERS ----------------
    critical_score = htf_alignment + liquidity_sweep
    log.debug(f"   Critical Score: {critical_score} (HTF: {htf_alignment}, Sweep: {liquidity_sweep})")
    
    if critical_score < CRITICAL_FACTORS_MIN: 
        log.debug(f"❌ SIGNAL REJECTED: Critical score {critical_score} < {CRITICAL_FACTORS_MIN}")
        return None
    
    if score < MIN_SCORE: 
        log.debug(f"❌ SIGNAL REJECTED: Score {score} < {MIN_SCORE}")
        return None
    
    if not has_disp: 
        log.debug(f"❌ SIGNAL REJECTED: No displacement ({displacement:.2f} < 0.6)")
        return None
    
    if htf_alignment != 1: 
        log.debug(f"❌ SIGNAL REJECTED: HTF alignment {htf_alignment} != 1")
        return None

    log.debug(f"✅ Passed critical filters: Score={score}, Critical={critical_score}")

    # ---------------- FORCED FILTER ----------------
    displacement_val = calc_values["displacement_value"]
    momentum_val = calc_values["momentum_value"]
    
    log.debug(f"   Forced Filter Check: Mom={momentum_val:.2f}, Disp={displacement_val:.2f}")
    log.debug(f"   Thresholds: Mom≥{MOMENTUM_STRONG_THRESHOLD} OR (Mom≥{MOMENTUM_GOOD_THRESHOLD} AND Disp≥{DISPLACEMENT_MIN_THRESHOLD})")
    
    if not force_filter_trade(momentum_val, displacement_val):
        log.debug(f"❌ SIGNAL REJECTED: Forced filter failed")
        log.debug(f"   Mom={momentum_val:.2f}, Disp={displacement_val:.2f}")
        return None
    
    filter_reason = "Mom≥0.87" if momentum_val >= MOMENTUM_STRONG_THRESHOLD else "Mom≥0.85 & Disp≥0.80"
    reasons.append(f"✅ FORCED FILTER PASSED: {filter_reason}")
    log.debug(f"✅ Forced filter passed: {filter_reason}")

    # ---------------- ELITE MTF CONFIRMATION ----------------
    if not await elite_tf_alignment(exchange, symbol, side_str):
        log.debug(f"❌ SIGNAL REJECTED: Elite MTF alignment failed")
        return None
    reasons.append("Elite MTF Alignment ✅")
    log.debug(f"✅ Elite MTF alignment passed")

    # ---------------- ROMEOPT TP CALCULATION ----------------
    atr_val = float(atr(df, 14).iloc[-1])
    log.debug(f"   ATR Value: {atr_val:.6f}")
    
    result = romeopt_tp_sl(entry, side_str, atr_val, ob_zone, df)
    
    if result is None:
        log.debug(f"❌ SIGNAL REJECTED: No valid TP found (romeopt_tp_sl returned None)")
        reasons.append("❌ NO VALID LIQUIDITY FOUND")
        return None
    
    sl, tp, tp_type = result
    
    sig = {
        "symbol": symbol,
        "side": side_str,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "score": score,
        "reason": "RomeOPT 6-Step",
        "reason_list": reasons,
        "htf_alignment": htf_alignment,
        "liquidity_sweep": liquidity_sweep,
        "momentum_ratio": momentum_ratio,
        "calc_values": calc_values,
        "tp_type": tp_type
    }
    
    log.info(f"✅ SIGNAL ACCEPTED: {sig['symbol']} {sig['side']} at {sig['entry']:.6f}")
    log.info(f"   Score: {score}/6, TP: {tp:.6f}, R:R: {(tp-entry)/(entry-sl):.2f}:1")
    return sig

# ---------------- REST OF THE CODE REMAINS UNCHANGED ----------------
async def elite_tf_alignment(exchange, symbol: str, side: str):
    tfs = ["15m","1h","4h"]
    for tf in tfs:
        ohlcv = await fetch_ohlcv(exchange, symbol, tf, 50)
        if not ohlcv or len(ohlcv) < 10: return False
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
        if len(df) < 5: return False
        trend = df["close"].iloc[-1] - df["close"].iloc[-5]
        trend_side = "BUY" if trend>0 else "SELL"
        if trend_side != side:
            return False
    return True

def update_tp_sl_live(sig: dict, df: pd.DataFrame):
    if sig.get("tp_hit", 0) == 1:
        return sig
    
    latest_ob = find_latest_ob(df)
    if not latest_ob:
        return None
    
    if sig["side"] == "BUY":
        if df['low'].iloc[-1] < latest_ob["low"]:
            return None
    else:
        if df['high'].iloc[-1] > latest_ob["high"]:
            return None
    
    return sig

recent_sl = defaultdict(lambda: deque())
def record_sl_hit(symbol: str, lookback_minutes=30):
    now = time.time(); dq = recent_sl[symbol]; dq.append(now)
    cutoff = now - lookback_minutes*60
    while dq and dq[0]<cutoff: dq.popleft()
def deprioritized(symbol: str, threshold=3, lookback=30):
    dq = recent_sl[symbol]; now=time.time(); cutoff=now-lookback*60
    while dq and dq[0]<cutoff: dq.popleft()
    return len(dq)>=threshold

async def log_signal(sig):
    async with db_lock:
        await db_conn.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp,timestamp,status,reason,score,latest_ob,tp_type,tp_locked)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (sig["symbol"],sig["side"],sig["entry"],sig.get("sl"),sig.get("tp"),
              datetime.datetime.utcnow().isoformat(),"OPEN",sig["reason"],sig["score"],
              str(sig.get("latest_ob","")), sig.get("tp_type", ""), 1))
        await db_conn.commit()

async def monitor_signals():
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("PRAGMA table_info(signals)") as cursor:
                    columns = await cursor.fetchall()
                    column_names = [col[1] for col in columns]
                
                select_fields = []
                if 'id' in column_names: select_fields.append('id')
                if 'symbol' in column_names: select_fields.append('symbol')
                if 'side' in column_names: select_fields.append('side')
                if 'entry' in column_names: select_fields.append('entry')
                if 'sl' in column_names: select_fields.append('sl')
                if 'tp' in column_names: select_fields.append('tp')
                else: select_fields.append('NULL as tp')
                
                if 'tp_hit' in column_names: select_fields.append('tp_hit')
                else: select_fields.append('0 as tp_hit')
                
                if 'status' in column_names: select_fields.append('status')
                
                query = f"SELECT {','.join(select_fields)} FROM signals WHERE status='OPEN'"
                
                async with db_conn.execute(query) as cursor:
                    async for row in cursor:
                        row_dict = dict(zip(select_fields, row))
                        sig_id = row_dict.get('id')
                        symbol = row_dict.get('symbol')
                        side = row_dict.get('side')
                        entry = row_dict.get('entry')
                        sl = row_dict.get('sl')
                        tp = row_dict.get('tp')
                        tp_hit = row_dict.get('tp_hit', 0)
                        status = row_dict.get('status')
                        
                        if not all([sig_id, symbol, side, entry]):
                            continue
                        
                        if tp_hit == 1:
                            continue
                        
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get("last")
                        if last_price is None: 
                            continue

                        hits = []
                        new_tp_hit = tp_hit
                        new_status = status
                        
                        if side == "BUY":
                            if not tp_hit and tp is not None and last_price >= tp:
                                hits.append("TP"); new_tp_hit = 1
                            if sl is not None and last_price <= sl:
                                hits.append("SL"); new_status = "CLOSED"
                        else:
                            if not tp_hit and tp is not None and last_price <= tp:
                                hits.append("TP"); new_tp_hit = 1
                            if sl is not None and last_price >= sl:
                                hits.append("SL"); new_status = "CLOSED"

                        if hits:
                            await tg(f"🎯 {symbol} {side} HIT\nEntry:{entry}\nLast:{last_price}\nHits:{','.join(hits)}\nSL:{sl}\nTP:{tp}")

                        if "SL" in hits:
                            record_sl_hit(symbol)
                        
                        if new_tp_hit != tp_hit or new_status != status:
                            await db_conn.execute("UPDATE signals SET tp_hit=?,status=? WHERE id=?",
                                                 (new_tp_hit, new_status, sig_id))
                await db_conn.commit()
        except Exception as e: 
            log.exception("monitor error: %s", e)
        await asyncio.sleep(SCAN_INTERVAL)

last_signal_time = {}
async def scan_loop(exchange):
    while True:
        t0=time.time()
        try:
            tickers = await exchange.fetch_tickers()
            top = sorted([(s,v.get("quoteVolume",0)) for s,v in tickers.items() if s.endswith("USDT")], key=lambda x:x[1], reverse=True)[:TOP_N]
            signals_found = 0
            for symbol,_ in top:
                if deprioritized(symbol): continue
                for tf in TIMEFRAMES:
                    key=f"{symbol}:{tf}"
                    if key in last_signal_time and time.time()-last_signal_time[key]<60: continue
                    ohlcv = await fetch_ohlcv(exchange,symbol,tf,200)
                    if not ohlcv: continue
                    df=pd.DataFrame(ohlcv,columns=["ts","open","high","low","close","vol"])
                    for c in ["open","high","low","close","vol"]: df[c]=pd.to_numeric(df[c],errors="coerce")
                    sig = await generate_signal_romeopt(exchange,df,symbol,tf)
                    if sig:
                        calc = sig.get("calc_values", {})
                        momentum_val = calc.get("momentum_value", 0)
                        displacement_val = calc.get("displacement_value", 0)
                        
                        filter_passed = force_filter_trade(momentum_val, displacement_val)
                        
                        risk = abs(sig["entry"] - sig.get("sl", 0))
                        reward = abs(sig.get("tp", 0) - sig["entry"])
                        rr = reward / risk if risk > 0 else 0
                        
                        breakdown_lines = [
                            f"🏆 ROMEOPT SIGNAL: {sig['symbol']} ({tf}) {sig['side']}",
                            f"Entry: {sig['entry']:.6f}",
                            f"Score: {sig['score']}/6",
                            f"",
                            f"📊 LIQUIDITY SWEEP DATA:",
                            f"• Type: {calc.get('sweep_type', 'NONE')}",
                            f"• Direction: {calc.get('sweep_direction', 'NONE')}",
                            f"• Strength: {calc.get('sweep_strength', 0):.2f}",
                            f"• Respected: {'✅' if calc.get('sweep_respected', False) else '❌'}",
                            f"• Swept Level: {calc.get('swept_level', 0):.6f}",
                            f"",
                            f"📊 ORDER BLOCK DATA:",
                            f"• Type: {calc.get('ob_type', 'NONE')}",
                            f"• Strength: {calc.get('ob_strength', 'UNKNOWN')}",
                            f"• Tested: {'✅' if calc.get('ob_tested', False) else '❌'}",
                            f"• Range: {calc.get('ob_low', 0):.6f} - {calc.get('ob_high', 0):.6f}",
                            f"• Distance: {calc.get('distance_to_ob', 0):.2%}",
                            f"",
                            f"📊 CORE METRICS:",
                            f"• Displacement: {calc.get('displacement_value', 0):.2f}",
                            f"• Momentum: {calc.get('momentum_value', 0):.2f}",
                            f"• HTF: {calc.get('htf_direction', '?')}",
                            f"• Forced Filter: {'✅ PASS' if filter_passed else '❌ REJECT'}",
                            f"• TP Type: {sig.get('tp_type', 'N/A')}",
                            f"",
                            f"🎯 LIQUIDITY TARGET (R:R: {rr:.2f}:1):",
                            f"SL: {sig.get('sl'):.6f}",
                            f"TP: {sig.get('tp'):.6f}",
                            f"",
                            f"💎 ROMEOPT PHILOSOPHY:",
                            f"One TP = One liquidity objective",
                            f"TP LOCKED - No chasing price"
                        ]
                        
                        await tg("\n".join(breakdown_lines))
                        await log_signal(sig)
                        last_signal_time[key]=time.time()
                        signals_found+=1
            log.info(f"📊 Scan complete: {signals_found} RomeOPT signals")
        except Exception as e: log.exception("scan error: %s", e)
        elapsed=time.time()-t0
        await asyncio.sleep(max(1,SCAN_INTERVAL-elapsed))

app = FastAPI()
@app.post("/webhook")
async def webhook(request: Request):
    token = request.headers.get("X-Auth","")
    if token!=WEBHOOK_SECRET: raise HTTPException(403,"Invalid secret")
    data = await request.json()
    log.info("Webhook received: %s", data)
    return {"ok":True}

async def main():
    await init_db()
    global exchange
    exchange = ccxt.okx({"enableRateLimit": True})
    await tg("🏆 TRUE ROMEOPT SCANNER STARTED (ENHANCED LOGGING)")
    await tg("🔍 Detailed rejection logging enabled")
    await asyncio.gather(scan_loop(exchange), monitor_signals())

if __name__=="__main__":
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--http", action="store_true")
    args=p.parse_args()
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=9000)
    else:
        try:
            asyncio.run(main())
        finally:
            if db_conn:
                asyncio.run(db_conn.close())