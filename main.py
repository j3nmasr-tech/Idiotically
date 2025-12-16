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

# ---------------- OPTIMIZED ROMEOPT INTERNAL LIQUIDITY ----------------
def romeopt_internal_liquidity(df, side, atr_val, lookback=18):
    """
    OPTIMIZED RomeOPT internal liquidity detection
    More flexible but still pure RomeOPT
    """
    if side == "SELL":
        # For SELL: Look for visual equal lows
        lows = df['low'].iloc[-lookback:].dropna()
        if len(lows) < 4:  # Reduced from 5
            return None
        
        # Increased tolerance: 18% of ATR (better visual detection)
        tolerance = atr_val * 0.18
        
        # Find potential cluster centers with weighted scoring
        potential_targets = []
        for i in range(len(lows)):
            current_low = lows.iloc[i]
            
            # Check how many lows are within tolerance
            nearby_lows = lows[abs(lows - current_low) <= tolerance]
            nearby_count = len(nearby_lows)
            
            # RomeOPT: Give preference to RECENT clusters
            recency_score = (lookback - i) / lookback  # 1.0 = most recent
            
            # Accept if: 
            # 1. ≥2 nearby lows OR 
            # 2. Single low but very recent (last 3 candles)
            if nearby_count >= 2 or (nearby_count >= 1 and i >= len(lows)-3):
                weighted_score = nearby_count * (1.0 + recency_score)
                potential_targets.append((current_low, weighted_score, nearby_count))
        
        if potential_targets:
            # Choose the most obvious visual cluster
            # Weighted by: cluster size + recency
            best_target = min(potential_targets, key=lambda x: (x[0], -x[1]))[0]
            return best_target
        
    else:  # BUY
        # For BUY: Look for visual equal highs
        highs = df['high'].iloc[-lookback:].dropna()
        if len(highs) < 4:
            return None
        
        tolerance = atr_val * 0.18
        potential_targets = []
        
        for i in range(len(highs)):
            current_high = highs.iloc[i]
            nearby_highs = highs[abs(highs - current_high) <= tolerance]
            nearby_count = len(nearby_highs)
            
            recency_score = (lookback - i) / lookback
            
            if nearby_count >= 2 or (nearby_count >= 1 and i >= len(highs)-3):
                weighted_score = nearby_count * (1.0 + recency_score)
                potential_targets.append((current_high, weighted_score, nearby_count))
        
        if potential_targets:
            best_target = max(potential_targets, key=lambda x: (x[0], -x[1]))[0]
            return best_target
    
    return None  # No visual liquidity cluster

# ---------------- REFINED ROMEOPT EXTERNAL LIQUIDITY ----------------
def romeopt_external_liquidity(df, side, lookback=50):
    """
    REFINED RomeOPT external liquidity detection
    Simple: Range extremes (RomeOPT prefers guaranteed stops)
    """
    if side == "SELL":
        # For SELL in trend: Range low (guaranteed stops below)
        return df['low'].iloc[-lookback:].min()
    else:  # BUY
        # For BUY in trend: Range high (guaranteed stops above)
        return df['high'].iloc[-lookback:].max()

# ---------------- ROMEOPT TP DECISION (REFINED VERSION) ----------------
def romeopt_tp_sl(entry, side, atr_val, ob_zone, df):
    """
    REFINED RomeOPT TP logic with all fixes
    """
    # Step 1: Determine market state
    market_state = romeopt_market_state(df, atr_val)
    
    # Step 2: Find liquidity based on market state
    tp = None
    tp_type = ""
    
    if market_state == "BALANCED":
        # RANGE: Look for internal liquidity clusters
        tp = romeopt_internal_liquidity(df, side, atr_val)
        if tp:
            tp_type = f"RANGE: Visual {'Lows' if side == 'SELL' else 'Highs'} Cluster"
    else:  # IMBALANCED
        # TREND: Look for external range extremes
        tp = romeopt_external_liquidity(df, side)
        if tp:
            tp_type = f"TREND: Range {'Low' if side == 'SELL' else 'High'}"
    
    # REJECT if no obvious liquidity found
    if tp is None:
        log.debug(f"❌ No obvious liquidity found for {side} | Market: {market_state}")
        return None
    
    # Step 3: RomeOPT OPTIMAL sweep detection
    # Reject only if price has CLOSED beyond liquidity (true sweep)
    # Wick touches are OK - liquidity remains valid
    
    recent_candles = min(8, len(df))
    candles_to_check = 3  # Only check most recent 3 candles (not 10)
    
    if side == "SELL":
        # For SELL: Liquidity is at lows, reject if price CLOSED below it
        true_sweep_detected = False
        wick_touches = 0
        
        for i in range(1, min(candles_to_check + 1, recent_candles)):
            candle_low = df['low'].iloc[-i]
            candle_close = df['close'].iloc[-i]
            
            # Check 1: TRUE SWEEP - price closed below liquidity
            if candle_close < tp - (atr_val * 0.03):  # 3% ATR below target
                true_sweep_detected = True
                log.debug(f"❌ TRUE SWEEP DETECTED: Candle {i} closed at {candle_close:.6f} below liquidity {tp:.6f}")
                break
            
            # Check 2: Wick touched but didn't sweep (this is OK for RomeOPT)
            elif abs(candle_low - tp) <= atr_val * 0.15:  # 15% ATR tolerance for wick
                wick_touches += 1
                log.debug(f"⚠️  Wick touch #{wick_touches} on candle {i} (low: {candle_low:.6f})")
        
        if true_sweep_detected:
            return None
        elif wick_touches > 0:
            log.debug(f"✅ Liquidity at {tp:.6f} has {wick_touches} wick touch(es) but NOT swept")
            
    else:  # BUY
        # For BUY: Liquidity is at highs, reject if price CLOSED above it
        true_sweep_detected = False
        wick_touches = 0
        
        for i in range(1, min(candles_to_check + 1, recent_candles)):
            candle_high = df['high'].iloc[-i]
            candle_close = df['close'].iloc[-i]
            
            # Check 1: TRUE SWEEP - price closed above liquidity
            if candle_close > tp + (atr_val * 0.03):  # 3% ATR above target
                true_sweep_detected = True
                log.debug(f"❌ TRUE SWEEP DETECTED: Candle {i} closed at {candle_close:.6f} above liquidity {tp:.6f}")
                break
            
            # Check 2: Wick touched but didn't sweep
            elif abs(candle_high - tp) <= atr_val * 0.15:
                wick_touches += 1
                log.debug(f"⚠️  Wick touch #{wick_touches} on candle {i} (high: {candle_high:.6f})")
        
        if true_sweep_detected:
            return None
        elif wick_touches > 0:
            log.debug(f"✅ Liquidity at {tp:.6f} has {wick_touches} wick touch(es) but NOT swept")
    
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
        
        # Ensure TP is valid (above entry, at least 0.5R)
        if tp <= entry:
            log.debug(f"❌ TP {tp} not above entry {entry} for BUY")
            return None
            
        reward = tp - entry
        if reward < risk * 0.5:
            log.debug(f"❌ TP reward {reward/risk:.2f}R < 0.5R minimum")
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
        
        # Ensure TP is valid (below entry, at least 0.5R)
        if tp >= entry:
            log.debug(f"❌ TP {tp} not below entry {entry} for SELL")
            return None
            
        reward = entry - tp
        if reward < risk * 0.5:
            log.debug(f"❌ TP reward {reward/risk:.2f}R < 0.5R minimum")
            return None
    
    log.info(f"✅ {side} {entry:.6f} | Market: {market_state}")
    log.info(f"   SL: {sl:.6f} | TP: {tp:.6f} | Type: {tp_type}")
    log.info(f"   Risk: {risk:.6f} | R:R: {abs(tp-entry)/risk:.2f}:1")
    
    return sl, tp, tp_type

# ---------------- ENHANCED ORDER BLOCK DETECTION ----------------
def find_latest_ob(df: pd.DataFrame, lookback=50):
    """
    Enhanced Order Block detection with detailed classification
    Returns comprehensive OB data
    """
    blocks = []
    
    # Look for order blocks in the specified lookback
    for i in range(max(2, len(df) - lookback), len(df) - 1):
        candle = df.iloc[i]
        prev_candle = df.iloc[i-1]
        
        # Bullish Order Block: Bearish candle followed by bullish candle
        if (prev_candle["close"] < prev_candle["open"] and  # Previous bearish
            candle["close"] > candle["open"] and            # Current bullish
            candle["close"] > prev_candle["close"]):        # Closes above previous close
            
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
        
        # Bearish Order Block: Bullish candle followed by bearish candle
        elif (prev_candle["close"] > prev_candle["open"] and  # Previous bullish
              candle["close"] < candle["open"] and            # Current bearish
              candle["close"] < prev_candle["close"]):        # Closes below previous close
            
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
    
    # Return the most recent order block if any exist
    if blocks:
        latest_block = max(blocks, key=lambda x: x["index"])
        
        # Add classification based on strength
        body_ratio = latest_block["body_size"] / latest_block["candle_size"] if latest_block["candle_size"] > 0 else 0
        if body_ratio >= 0.7:
            latest_block["strength"] = "STRONG"
        elif body_ratio >= 0.5:
            latest_block["strength"] = "MODERATE"
        else:
            latest_block["strength"] = "WEAK"
        
        # Check if OB has been tested
        if latest_block["type"] == "BULLISH_OB":
            subsequent_candles = df.iloc[latest_block["index"]+1:min(latest_block["index"]+10, len(df))]
            latest_block["tested"] = any(candle["low"] <= latest_block["high"] for _, candle in subsequent_candles.iterrows())
        else:  # BEARISH_OB
            subsequent_candles = df.iloc[latest_block["index"]+1:min(latest_block["index"]+10, len(df))]
            latest_block["tested"] = any(candle["high"] >= latest_block["low"] for _, candle in subsequent_candles.iterrows())
        
        return latest_block
    
    return None

# ---------------- REST OF SIGNAL GENERATION ----------------
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

async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    if df is None or len(df) < 20: return None
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    score = 0
    reasons = []
    
    calc_values = {}

    # Step 1: ENHANCED Liquidity Sweep Detection
    lookback_period = 20
    high_lookback = df['high'].iloc[-lookback_period:-1]
    low_lookback = df['low'].iloc[-lookback_period:-1]
    
    # Check for sweeping previous highs/lows with more precision
    sweep_high = last["high"] > high_lookback.max()
    sweep_low = last["low"] < low_lookback.min()
    
    # Check if sweep was respected (price closed back inside range)
    respected_high_sweep = False
    respected_low_sweep = False
    sweep_strength = 0.0
    
    if sweep_high:
        # Calculate how much it swept the high
        sweep_amount = last["high"] - high_lookback.max()
        candle_range = last["high"] - last["low"]
        if candle_range > 0:
            sweep_strength = sweep_amount / candle_range
        # Check if closed below the swept level (respected)
        if last["close"] < high_lookback.max():
            respected_high_sweep = True
    
    if sweep_low:
        # Calculate how much it swept the low
        sweep_amount = low_lookback.min() - last["low"]
        candle_range = last["high"] - last["low"]
        if candle_range > 0:
            sweep_strength = sweep_amount / candle_range
        # Check if closed above the swept level (respected)
        if last["close"] > low_lookback.min():
            respected_low_sweep = True
    
    has_sweep = (sweep_high and respected_high_sweep) or (sweep_low and respected_low_sweep)
    
    liquidity_sweep = 2 if has_sweep else 0
    score += liquidity_sweep
    
    # Enhanced sweep type classification
    if sweep_high and respected_high_sweep:
        sweep_type = "HIGH_SWEEP_RESPECTED"
        sweep_direction = "BEARISH"
    elif sweep_low and respected_low_sweep:
        sweep_type = "LOW_SWEEP_RESPECTED"
        sweep_direction = "BULLISH"
    elif sweep_high:
        sweep_type = "HIGH_SWEEP_UNRESPECTED"
        sweep_direction = "NEUTRAL"
    elif sweep_low:
        sweep_type = "LOW_SWEEP_UNRESPECTED"
        sweep_direction = "NEUTRAL"
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

    # Step 2: Displacement
    displacement = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    calc_values["displacement_value"] = round(displacement, 2)
    has_disp = displacement > 0.6
    if has_disp:
        score += 2; reasons.append(f"Displacement +2 ({displacement:.2f})")
    else:
        reasons.append(f"Displacement +0 ({displacement:.2f})")

    # Step 3 & 4: ENHANCED Order Block & Zone
    ob_zone = find_latest_ob(df, lookback=30)
    
    if ob_zone:
        ob_type = "bullish" if ob_zone["type"] == "BULLISH_OB" else "bearish"
        zone_approach = 0
        
        # Enhanced zone approach check
        if ob_type == "bullish":
            # For bullish OB, check if price is approaching from above
            distance_to_ob = (last["close"] - ob_zone["high"]) / (ob_zone["high"] - ob_zone["low"] + 1e-8)
            if last["close"] <= ob_zone["high"] or distance_to_ob < 0.1:
                score += 1
                zone_approach = 1
                approach_status = f"APPROACHING (dist: {distance_to_ob:.2%})"
            else:
                approach_status = f"FAR ({distance_to_ob:.2%} away)"
        else:  # bearish
            # For bearish OB, check if price is approaching from below
            distance_to_ob = (ob_zone["low"] - last["close"]) / (ob_zone["high"] - ob_zone["low"] + 1e-8)
            if last["close"] >= ob_zone["low"] or distance_to_ob < 0.1:
                score += 1
                zone_approach = 1
                approach_status = f"APPROACHING (dist: {distance_to_ob:.2%})"
            else:
                approach_status = f"FAR ({distance_to_ob:.2%} away)"
        
        reasons.append(f"Zone Approach +{zone_approach} ({approach_status})")
        
        # Store comprehensive OB data
        calc_values["zone_approach"] = zone_approach
        calc_values["ob_type"] = ob_type
        calc_values["ob_strength"] = ob_zone.get("strength", "UNKNOWN")
        calc_values["ob_tested"] = ob_zone.get("tested", False)
        calc_values["ob_low"] = round(ob_zone["low"], 6)
        calc_values["ob_high"] = round(ob_zone["high"], 6)
        calc_values["ob_body_low"] = round(ob_zone.get("body_low", ob_zone["low"]), 6)
        calc_values["ob_body_high"] = round(ob_zone.get("body_high", ob_zone["high"]), 6)
        calc_values["ob_candle_size"] = round(ob_zone.get("candle_size", 0), 6)
        calc_values["ob_body_ratio"] = round(ob_zone.get("body_size", 0) / ob_zone.get("candle_size", 1) if ob_zone.get("candle_size", 0) > 0 else 0, 2)
        calc_values["ob_volume"] = ob_zone.get("volume", 0)
        calc_values["distance_to_ob"] = round(distance_to_ob, 4)
    else:
        reasons.append("Zone Approach +0 (No OB detected)")
        ob_type = None
        calc_values["zone_approach"] = 0
        calc_values["ob_type"] = "NONE"

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
                score+=1; htf_alignment=1; reasons.append(f"HTF Alignment +1 ({htf_dir} {trend:+.6f})")
            else:
                reasons.append(f"HTF Alignment +0 ({htf_dir} {trend:+.6f})")
            calc_values["htf_trend"] = htf_trend_value
            calc_values["htf_direction"] = htf_dir
        else:
            reasons.append("HTF Alignment ? (insufficient data)")
            calc_values["htf_trend"] = 0
            calc_values["htf_direction"] = "UNKNOWN"
    else:
        reasons.append("HTF Alignment ? (no data)")
        calc_values["htf_trend"] = 0
        calc_values["htf_direction"] = "UNKNOWN"

    # Step 6: MOMENTUM
    momentum_ratio = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    calc_values["momentum_value"] = round(momentum_ratio, 2)
    
    if ob_type=="bullish" and momentum_ratio>=0.8 and last["close"]>last["open"]:
        score+=1; reasons.append(f"Momentum +1 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 1
    elif ob_type=="bearish" and momentum_ratio>=0.8 and last["close"]<last["open"]:
        score+=1; reasons.append(f"Momentum +1 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 1
    else:
        reasons.append(f"Momentum +0 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 0

    if not ob_type: return None
    side_str = "BUY" if ob_type=="bullish" else "SELL"
    entry = float(last["close"])

    # ---------------- CRITICAL FILTERS ----------------
    critical_score = htf_alignment + liquidity_sweep
    if critical_score < CRITICAL_FACTORS_MIN: return None
    if score < MIN_SCORE: return None
    if not has_disp: return None
    if htf_alignment != 1: return None

    # ---------------- FORCED FILTER ----------------
    displacement_val = calc_values["displacement_value"]
    momentum_val = calc_values["momentum_value"]
    
    if not force_filter_trade(momentum_val, displacement_val):
        reasons.append(f"❌ FORCED FILTER REJECTED: Mom={momentum_val:.2f}, Disp={displacement_val:.2f}")
        return None
    
    filter_reason = "Mom≥0.87" if momentum_val >= MOMENTUM_STRONG_THRESHOLD else "Mom≥0.85 & Disp≥0.80"
    reasons.append(f"✅ FORCED FILTER PASSED: {filter_reason}")

    # ---------------- ELITE MTF CONFIRMATION ----------------
    if not await elite_tf_alignment(exchange, symbol, side_str):
        return None
    reasons.append("Elite MTF Alignment ✅")

    # ---------------- ROMEOPT TP CALCULATION ----------------
    atr_val = float(atr(df, 14).iloc[-1])
    result = romeopt_tp_sl(entry, side_str, atr_val, ob_zone, df)
    
    # REJECT if no valid TP found
    if result is None:
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
    
    log.info(f"✅ Signal {sig['symbol']} passed forced filter: Mom={momentum_val:.2f}, Disp={displacement_val:.2f}")
    return sig

# ---------------- REFINED UPDATE TP/SL LIVE ----------------
def update_tp_sl_live(sig: dict, df: pd.DataFrame):
    """
    REFINED: Only update if TP hasn't been hit AND structure invalidated
    RomeOPT commits to liquidity objective - no chasing price
    """
    # TP LOCK: If TP already hit, do nothing
    if sig.get("tp_hit", 0) == 1:
        return sig
    
    # Only update if structure is completely invalid
    latest_ob = find_latest_ob(df)
    if not latest_ob:
        # No OB anymore - structure invalid, close trade
        return None
    
    # Check if price has taken out the OB (structure break)
    if sig["side"] == "BUY":
        if df['low'].iloc[-1] < latest_ob["low"]:
            return None  # OB broken, close
    else:  # SELL
        if df['high'].iloc[-1] > latest_ob["high"]:
            return None  # OB broken, close
    
    # Structure still valid - keep original TP (RomeOPT doesn't chase)
    return sig

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

# ---------------- LOG SIGNAL ----------------
async def log_signal(sig):
    async with db_lock:
        await db_conn.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp,timestamp,status,reason,score,latest_ob,tp_type,tp_locked)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (sig["symbol"],sig["side"],sig["entry"],sig.get("sl"),sig.get("tp"),
              datetime.datetime.utcnow().isoformat(),"OPEN",sig["reason"],sig["score"],
              str(sig.get("latest_ob","")), sig.get("tp_type", ""), 1))
        await db_conn.commit()

# ---------------- ROBUST MONITOR SIGNALS ----------------
async def monitor_signals():
    while True:
        try:
            async with db_lock:
                # Get current columns to build dynamic query
                async with db_conn.execute("PRAGMA table_info(signals)") as cursor:
                    columns = await cursor.fetchall()
                    column_names = [col[1] for col in columns]
                
                # Build query with fallback for missing columns
                select_fields = []
                if 'id' in column_names:
                    select_fields.append('id')
                if 'symbol' in column_names:
                    select_fields.append('symbol')
                if 'side' in column_names:
                    select_fields.append('side')
                if 'entry' in column_names:
                    select_fields.append('entry')
                if 'sl' in column_names:
                    select_fields.append('sl')
                if 'tp' in column_names:
                    select_fields.append('tp')
                else:
                    select_fields.append('NULL as tp')  # Fallback
                
                if 'tp_hit' in column_names:
                    select_fields.append('tp_hit')
                else:
                    select_fields.append('0 as tp_hit')  # Default value
                
                if 'status' in column_names:
                    select_fields.append('status')
                
                query = f"SELECT {','.join(select_fields)} FROM signals WHERE status='OPEN'"
                
                async with db_conn.execute(query) as cursor:
                    async for row in cursor:
                        # Map row to variables based on query structure
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
                        
                        # Check if TP already hit
                        if tp_hit == 1:
                            continue
                        
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get("last")
                        if last_price is None: 
                            continue

                        # RomeOPT: TP LOCK - Don't recalculate unless structure broken
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
                        
                        # Only update if something changed
                        if new_tp_hit != tp_hit or new_status != status:
                            await db_conn.execute("UPDATE signals SET tp_hit=?,status=? WHERE id=?",
                                                 (new_tp_hit, new_status, sig_id))
                await db_conn.commit()
        except Exception as e: 
            log.exception("monitor error: %s", e)
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- SCAN LOOP ----------------
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
                        
                        # Calculate R:R
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
                            f"• Body: {calc.get('ob_body_low', 0):.6f} - {calc.get('ob_body_high', 0):.6f}",
                            f"• Body Ratio: {calc.get('ob_body_ratio', 0):.2f}",
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
            log.info(f"📊 Scan complete: {signals_found} RomeOPT signals (TP LOCKED)")
        except Exception as e: log.exception("scan error: %s", e)
        elapsed=time.time()-t0
        await asyncio.sleep(max(1,SCAN_INTERVAL-elapsed))

# ---------------- FASTAPI ----------------
app = FastAPI()
@app.post("/webhook")
async def webhook(request: Request):
    token = request.headers.get("X-Auth","")
    if token!=WEBHOOK_SECRET: raise HTTPException(403,"Invalid secret")
    data = await request.json()
    log.info("Webhook received: %s", data)
    return {"ok":True}

# ---------------- MAIN ----------------
async def main():
    await init_db()
    global exchange
    exchange = ccxt.okx({"enableRateLimit": True})
    await tg("🏆 TRUE ROMEOPT SCANNER STARTED (ENHANCED SWEEP & OB DATA)")
    await tg("🎯 ENHANCED: Comprehensive liquidity sweep analysis")
    await tg("📊 ENHANCED: Detailed order block classification")
    await tg("🔒 TP LOCK: No recalculation after entry")
    await tg("⚡ EXTERNAL LIQUIDITY: Range extremes only")
    await tg("💎 ROMEOPT CORE: Target where price MUST go")
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