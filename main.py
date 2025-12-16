#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TRUE ROMEOPT SCANNER (Final Refined Version) - WITH ENHANCED DEBUGGING
- Added comprehensive debugging for liquidity and TP rejection reasons
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

# ---------------- ENHANCED DEBUGGING ----------------
DEBUG_MODE = True  # Set to False in production
DEBUG_LOG_FILE = "/app/data/debug_signals.log"

# Setup enhanced debugging logging
debug_logger = logging.getLogger("debug_logger")
debug_logger.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)
debug_handler = logging.FileHandler(DEBUG_LOG_FILE)
debug_handler.setFormatter(logging.Formatter(
    "%(asctime)s | SYMBOL=%(symbol)s | TF=%(timeframe)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))
debug_logger.addHandler(debug_handler)

# ---------------- MAIN LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_bot")
db_lock = asyncio.Lock()
db_conn = None

# ---------------- DEBUG HELPER CLASS ----------------
class SignalDebugger:
    """Enhanced debugging for signal generation and rejection reasons"""
    
    def __init__(self, symbol, timeframe):
        self.symbol = symbol
        self.timeframe = timeframe
        self.rejection_reasons = []
        self.warnings = []
        self.debug_data = {}
        self.start_time = time.time()
        
    def log(self, level, message, **kwargs):
        """Enhanced logging with symbol context"""
        extra = {'symbol': self.symbol, 'timeframe': self.timeframe}
        extra.update(kwargs)
        debug_logger.log(level, message, extra=extra)
        
    def reject(self, reason, details=None):
        """Record rejection reason with details"""
        self.rejection_reasons.append(reason)
        self.log(logging.WARNING, f"❌ REJECTED: {reason}", details=details)
        return False
    
    def warn(self, warning, details=None):
        """Record warning"""
        self.warnings.append(warning)
        self.log(logging.WARNING, f"⚠️ WARNING: {warning}", details=details)
        
    def info(self, message, details=None):
        """Record informational message"""
        self.log(logging.INFO, f"ℹ️ INFO: {message}", details=details)
        
    def success(self, message, details=None):
        """Record success"""
        self.log(logging.INFO, f"✅ SUCCESS: {message}", details=details)
        
    def debug_data_point(self, key, value):
        """Store debug data"""
        self.debug_data[key] = value
        
    def get_summary(self):
        """Get debugging summary"""
        elapsed = time.time() - self.start_time
        return {
            'symbol': self.symbol,
            'timeframe': self.timeframe,
            'rejection_reasons': self.rejection_reasons,
            'warnings': self.warnings,
            'debug_data': self.debug_data,
            'elapsed_seconds': round(elapsed, 3)
        }

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
def force_filter_trade(momentum_value: float, displacement_value: float, debugger=None) -> bool:
    """Enhanced forced filter with debugging"""
    if debugger:
        debugger.debug_data_point('momentum_value', momentum_value)
        debugger.debug_data_point('displacement_value', displacement_value)
        debugger.debug_data_point('momentum_threshold_strong', MOMENTUM_STRONG_THRESHOLD)
        debugger.debug_data_point('momentum_threshold_good', MOMENTUM_GOOD_THRESHOLD)
        debugger.debug_data_point('displacement_threshold', DISPLACEMENT_MIN_THRESHOLD)
    
    if momentum_value >= MOMENTUM_STRONG_THRESHOLD:
        if debugger:
            debugger.success(f"Momentum ≥ {MOMENTUM_STRONG_THRESHOLD} ({momentum_value:.2f})")
        return True
    
    if momentum_value >= MOMENTUM_GOOD_THRESHOLD and displacement_value >= DISPLACEMENT_MIN_THRESHOLD:
        if debugger:
            debugger.success(f"Momentum ≥ {MOMENTUM_GOOD_THRESHOLD} ({momentum_value:.2f}) AND Displacement ≥ {DISPLACEMENT_MIN_THRESHOLD} ({displacement_value:.2f})")
        return True
    
    if debugger:
        debugger.reject(
            f"Forced filter failed: Mom={momentum_value:.2f}, Disp={displacement_value:.2f}",
            {
                'required_momentum_strong': MOMENTUM_STRONG_THRESHOLD,
                'required_momentum_good': MOMENTUM_GOOD_THRESHOLD,
                'required_displacement': DISPLACEMENT_MIN_THRESHOLD,
                'actual_momentum': momentum_value,
                'actual_displacement': displacement_value
            }
        )
    return False

# ---------------- REFINED ROMEOPT MARKET STATE ----------------
def romeopt_market_state(df, atr_val, debugger=None):
    """
    REFINED RomeOPT market state detection with debugging
    """
    if len(df) < 3:
        if debugger:
            debugger.reject("Insufficient data for market state", {'df_length': len(df)})
        return "BALANCED"
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    body_ratio = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    candle_size = last["high"] - last["low"]
    price_movement = abs(last["close"] - prev["close"])
    
    if debugger:
        debugger.debug_data_point('market_state_body_ratio', round(body_ratio, 3))
        debugger.debug_data_point('market_state_candle_size', round(candle_size, 6))
        debugger.debug_data_point('market_state_atr', round(atr_val, 6))
        debugger.debug_data_point('market_state_price_movement', round(price_movement, 6))
        debugger.debug_data_point('market_state_candle_vs_atr', round(candle_size / atr_val, 2) if atr_val > 0 else 0)
    
    # RomeOPT logic: Strong displacement with actual follow-through
    strong_displacement = (
        body_ratio > 0.7 and                    # Strong body
        candle_size > atr_val * 1.2 and         # Large candle
        price_movement > atr_val * 0.5          # Actual price movement
    )
    
    result = "IMBALANCED" if strong_displacement else "BALANCED"
    
    if debugger:
        if strong_displacement:
            debugger.success(f"Market state: IMBALANCED", {
                'body_ratio': round(body_ratio, 3),
                'candle_size_atr_ratio': round(candle_size / atr_val, 2) if atr_val > 0 else 0,
                'price_movement_atr_ratio': round(price_movement / atr_val, 2) if atr_val > 0 else 0
            })
        else:
            debugger.info(f"Market state: BALANCED", {
                'body_ratio': round(body_ratio, 3),
                'candle_size_atr_ratio': round(candle_size / atr_val, 2) if atr_val > 0 else 0,
                'price_movement_atr_ratio': round(price_movement / atr_val, 2) if atr_val > 0 else 0
            })
    
    return result

# ---------------- REFINED ROMEOPT INTERNAL LIQUIDITY ----------------
def romeopt_internal_liquidity(df, side, atr_val, lookback=15, debugger=None):
    """
    REFINED RomeOPT internal liquidity detection with debugging
    """
    if debugger:
        debugger.info(f"Looking for internal liquidity for {side} side")
        debugger.debug_data_point('internal_liquidity_lookback', lookback)
        debugger.debug_data_point('internal_liquidity_atr', round(atr_val, 6))
    
    tolerance = atr_val * 0.15
    if debugger:
        debugger.debug_data_point('internal_liquidity_tolerance', round(tolerance, 6))
    
    if side == "SELL":
        # For SELL: Look for obvious equal lows
        lows = df['low'].iloc[-lookback:].dropna()
        if debugger:
            debugger.debug_data_point('internal_liquidity_lows_count', len(lows))
            debugger.debug_data_point('internal_liquidity_lows_min', round(lows.min(), 6) if len(lows) > 0 else None)
            debugger.debug_data_point('internal_liquidity_lows_max', round(lows.max(), 6) if len(lows) > 0 else None)
        
        if len(lows) < 5:
            if debugger:
                debugger.reject(f"Insufficient lows data for internal liquidity", {'lows_count': len(lows), 'min_required': 5})
            return None
        
        # Find potential cluster centers
        potential_targets = []
        cluster_details = []
        
        for i in range(len(lows)):
            current_low = lows.iloc[i]
            # Count how many lows are within tolerance
            nearby_mask = abs(lows - current_low) <= tolerance
            nearby_count = nearby_mask.sum()
            nearby_prices = lows[nearby_mask].tolist()
            
            if nearby_count >= 2:  # At least 2 lows form a visual cluster
                potential_targets.append((current_low, nearby_count))
                cluster_details.append({
                    'price': current_low,
                    'count': nearby_count,
                    'nearby_prices': nearby_prices
                })
        
        if debugger and cluster_details:
            debugger.debug_data_point('internal_liquidity_clusters_found', len(cluster_details))
            for i, cluster in enumerate(cluster_details[:3]):  # Log first 3 clusters
                debugger.info(f"Cluster {i+1}: Price={cluster['price']:.6f}, Count={cluster['count']}")
        
        if potential_targets:
            # Choose the lowest price among clusters (most obvious stop pool)
            best_target = min(potential_targets, key=lambda x: x[0])[0]
            if debugger:
                debugger.success(f"Found internal liquidity cluster for SELL", {
                    'target_price': best_target,
                    'clusters_found': len(potential_targets),
                    'cluster_counts': [f"{p:.6f}({c})" for p, c in potential_targets]
                })
            return best_target
        
    else:  # BUY
        # For BUY: Look for obvious equal highs
        highs = df['high'].iloc[-lookback:].dropna()
        if debugger:
            debugger.debug_data_point('internal_liquidity_highs_count', len(highs))
            debugger.debug_data_point('internal_liquidity_highs_min', round(highs.min(), 6) if len(highs) > 0 else None)
            debugger.debug_data_point('internal_liquidity_highs_max', round(highs.max(), 6) if len(highs) > 0 else None)
        
        if len(highs) < 5:
            if debugger:
                debugger.reject(f"Insufficient highs data for internal liquidity", {'highs_count': len(highs), 'min_required': 5})
            return None
        
        potential_targets = []
        cluster_details = []
        
        for i in range(len(highs)):
            current_high = highs.iloc[i]
            nearby_mask = abs(highs - current_high) <= tolerance
            nearby_count = nearby_mask.sum()
            nearby_prices = highs[nearby_mask].tolist()
            
            if nearby_count >= 2:
                potential_targets.append((current_high, nearby_count))
                cluster_details.append({
                    'price': current_high,
                    'count': nearby_count,
                    'nearby_prices': nearby_prices
                })
        
        if debugger and cluster_details:
            debugger.debug_data_point('internal_liquidity_clusters_found', len(cluster_details))
            for i, cluster in enumerate(cluster_details[:3]):
                debugger.info(f"Cluster {i+1}: Price={cluster['price']:.6f}, Count={cluster['count']}")
        
        if potential_targets:
            # Choose the highest price among clusters
            best_target = max(potential_targets, key=lambda x: x[0])[0]
            if debugger:
                debugger.success(f"Found internal liquidity cluster for BUY", {
                    'target_price': best_target,
                    'clusters_found': len(potential_targets),
                    'cluster_counts': [f"{p:.6f}({c})" for p, c in potential_targets]
                })
            return best_target
    
    if debugger:
        debugger.reject(f"No obvious visual liquidity cluster found", {
            'side': side,
            'lookback': lookback,
            'tolerance': round(tolerance, 6),
            'min_cluster_size': 2
        })
    return None

# ---------------- REFINED ROMEOPT EXTERNAL LIQUIDITY ----------------
def romeopt_external_liquidity(df, side, lookback=50, debugger=None):
    """
    REFINED RomeOPT external liquidity detection with debugging
    """
    if debugger:
        debugger.info(f"Looking for external liquidity for {side} side")
        debugger.debug_data_point('external_liquidity_lookback', lookback)
    
    if side == "SELL":
        # For SELL in trend: Range low (guaranteed stops below)
        target = df['low'].iloc[-lookback:].min()
        if debugger:
            debugger.success(f"External liquidity for SELL (range low)", {
                'target_price': target,
                'lookback_range': f"{df.index[-lookback] if len(df) > lookback else 'start'} to {df.index[-1]}"
            })
        return target
    else:  # BUY
        # For BUY in trend: Range high (guaranteed stops above)
        target = df['high'].iloc[-lookback:].max()
        if debugger:
            debugger.success(f"External liquidity for BUY (range high)", {
                'target_price': target,
                'lookback_range': f"{df.index[-lookback] if len(df) > lookback else 'start'} to {df.index[-1]}"
            })
        return target

# ---------------- ROMEOPT TP DECISION (REFINED VERSION WITH DEBUGGING) ----------------
def romeopt_tp_sl(entry, side, atr_val, ob_zone, df, debugger=None):
    """
    REFINED RomeOPT TP logic with enhanced debugging
    """
    if debugger:
        debugger.info("Starting RomeOPT TP/SL calculation")
        debugger.debug_data_point('entry_price', entry)
        debugger.debug_data_point('side', side)
        debugger.debug_data_point('atr_value', round(atr_val, 6))
        debugger.debug_data_point('ob_zone_low', ob_zone.get("low", 0) if ob_zone else None)
        debugger.debug_data_point('ob_zone_high', ob_zone.get("high", 0) if ob_zone else None)
    
    # Step 1: Determine market state
    market_state = romeopt_market_state(df, atr_val, debugger)
    if debugger:
        debugger.debug_data_point('market_state', market_state)
    
    # Step 2: Find liquidity based on market state
    tp = None
    tp_type = ""
    
    if market_state == "BALANCED":
        # RANGE: Look for internal liquidity clusters
        if debugger:
            debugger.info("Market is BALANCED, looking for internal liquidity clusters")
        tp = romeopt_internal_liquidity(df, side, atr_val, debugger=debugger)
        if tp:
            tp_type = f"RANGE: Visual {'Lows' if side == 'SELL' else 'Highs'} Cluster"
    else:  # IMBALANCED
        # TREND: Look for external range extremes
        if debugger:
            debugger.info("Market is IMBALANCED, looking for external range extremes")
        tp = romeopt_external_liquidity(df, side, debugger=debugger)
        if tp:
            tp_type = f"TREND: Range {'Low' if side == 'SELL' else 'High'}"
    
    # REJECT if no obvious liquidity found
    if tp is None:
        if debugger:
            debugger.reject(
                f"No obvious liquidity found for {side} | Market: {market_state}",
                {
                    'side': side,
                    'market_state': market_state,
                    'entry_price': entry
                }
            )
        return None
    
    if debugger:
        debugger.debug_data_point('tp_found', tp)
        debugger.debug_data_point('tp_type', tp_type)
        debugger.success(f"Liquidity target found: {tp:.6f} ({tp_type})")
    
    # Step 3: Safety check - reject if recently swept
    recent_candles = min(10, len(df))
    recent_touch = False
    touch_details = []
    
    if side == "SELL":
        for i in range(1, recent_candles):
            candle_low = df['low'].iloc[-i]
            distance = abs(candle_low - tp)
            within_tolerance = distance <= atr_val * 0.1
            if within_tolerance:
                recent_touch = True
                touch_details.append({
                    'candle_index': -i,
                    'candle_low': candle_low,
                    'distance': distance,
                    'within_tolerance': within_tolerance
                })
    else:  # BUY
        for i in range(1, recent_candles):
            candle_high = df['high'].iloc[-i]
            distance = abs(candle_high - tp)
            within_tolerance = distance <= atr_val * 0.1
            if within_tolerance:
                recent_touch = True
                touch_details.append({
                    'candle_index': -i,
                    'candle_high': candle_high,
                    'distance': distance,
                    'within_tolerance': within_tolerance
                })
    
    if recent_touch:
        if debugger:
            debugger.reject(
                f"Liquidity recently swept for {side} at {tp:.6f}",
                {
                    'target_price': tp,
                    'touch_details': touch_details[:3],  # Show first 3 touches
                    'atr_10_percent': atr_val * 0.1
                }
            )
        return None
    elif debugger:
        debugger.success("Liquidity target NOT recently swept")
    
    # Step 4: Calculate SL (keep OB-based SL)
    if debugger:
        debugger.info("Calculating Stop Loss")
    
    if side == "BUY":
        # Initial SL calculation
        sl = ob_zone["low"] - (atr_val * 0.3)
        recent_low = df['low'].iloc[-10:].min()
        sl = min(sl, recent_low - (atr_val * 0.3))
        
        if debugger:
            debugger.debug_data_point('sl_initial_ob', ob_zone["low"] - (atr_val * 0.3))
            debugger.debug_data_point('sl_initial_recent', recent_low - (atr_val * 0.3))
            debugger.debug_data_point('sl_before_risk_adjust', sl)
        
        # Ensure minimum risk
        min_risk = atr_val * 0.5
        risk = entry - sl
        
        if debugger:
            debugger.debug_data_point('risk_before_adjust', risk)
            debugger.debug_data_point('min_risk_required', min_risk)
        
        if risk < min_risk:
            if debugger:
                debugger.warn(f"Risk {risk:.6f} < min risk {min_risk:.6f}, adjusting SL")
            risk = min_risk
            sl = entry - risk
        
        # Ensure TP is valid (above entry, at least 0.5R)
        if tp <= entry:
            if debugger:
                debugger.reject(
                    f"TP {tp:.6f} not above entry {entry:.6f} for BUY",
                    {
                        'tp': tp,
                        'entry': entry,
                        'tp_minus_entry': tp - entry
                    }
                )
            return None
        
        reward = tp - entry
        reward_risk_ratio = reward / risk if risk > 0 else 0
        
        if debugger:
            debugger.debug_data_point('reward', reward)
            debugger.debug_data_point('risk', risk)
            debugger.debug_data_point('reward_risk_ratio', round(reward_risk_ratio, 2))
        
        if reward < risk * 0.5:
            if debugger:
                debugger.reject(
                    f"TP reward {reward:.6f} < 0.5R minimum ({risk * 0.5:.6f})",
                    {
                        'reward': reward,
                        'min_reward_required': risk * 0.5,
                        'ratio': round(reward_risk_ratio, 2)
                    }
                )
            return None
            
    else:  # SELL
        # Initial SL calculation
        sl = ob_zone["high"] + (atr_val * 0.3)
        recent_high = df['high'].iloc[-10:].max()
        sl = max(sl, recent_high + (atr_val * 0.3))
        
        if debugger:
            debugger.debug_data_point('sl_initial_ob', ob_zone["high"] + (atr_val * 0.3))
            debugger.debug_data_point('sl_initial_recent', recent_high + (atr_val * 0.3))
            debugger.debug_data_point('sl_before_risk_adjust', sl)
        
        min_risk = atr_val * 0.5
        risk = sl - entry
        
        if debugger:
            debugger.debug_data_point('risk_before_adjust', risk)
            debugger.debug_data_point('min_risk_required', min_risk)
        
        if risk < min_risk:
            if debugger:
                debugger.warn(f"Risk {risk:.6f} < min risk {min_risk:.6f}, adjusting SL")
            risk = min_risk
            sl = entry + risk
        
        # Ensure TP is valid (below entry, at least 0.5R)
        if tp >= entry:
            if debugger:
                debugger.reject(
                    f"TP {tp:.6f} not below entry {entry:.6f} for SELL",
                    {
                        'tp': tp,
                        'entry': entry,
                        'tp_minus_entry': tp - entry
                    }
                )
            return None
        
        reward = entry - tp
        reward_risk_ratio = reward / risk if risk > 0 else 0
        
        if debugger:
            debugger.debug_data_point('reward', reward)
            debugger.debug_data_point('risk', risk)
            debugger.debug_data_point('reward_risk_ratio', round(reward_risk_ratio, 2))
        
        if reward < risk * 0.5:
            if debugger:
                debugger.reject(
                    f"TP reward {reward:.6f} < 0.5R minimum ({risk * 0.5:.6f})",
                    {
                        'reward': reward,
                        'min_reward_required': risk * 0.5,
                        'ratio': round(reward_risk_ratio, 2)
                    }
                )
            return None
    
    if debugger:
        debugger.success(f"✅ {side} {entry:.6f} | Market: {market_state}")
        debugger.success(f"   SL: {sl:.6f} | TP: {tp:.6f} | Type: {tp_type}")
        debugger.success(f"   Risk: {risk:.6f} | R:R: {abs(tp-entry)/risk:.2f}:1")
        debugger.debug_data_point('final_sl', sl)
        debugger.debug_data_point('final_tp', tp)
        debugger.debug_data_point('final_rr_ratio', round(abs(tp-entry)/risk, 2))
    
    return sl, tp, tp_type

# ---------------- ENHANCED ORDER BLOCK DETECTION ----------------
def find_latest_ob(df: pd.DataFrame, lookback=50, debugger=None):
    """
    Enhanced Order Block detection with debugging
    """
    blocks = []
    
    if debugger:
        debugger.info(f"Looking for order blocks (lookback={lookback})")
    
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
    
    if debugger:
        debugger.debug_data_point('ob_blocks_found', len(blocks))
    
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
        
        if debugger:
            debugger.success(f"Found order block: {latest_block['type']}", {
                'index': latest_block['index'],
                'strength': latest_block['strength'],
                'tested': latest_block['tested'],
                'range': f"{latest_block['low']:.6f} - {latest_block['high']:.6f}",
                'body_ratio': round(body_ratio, 2)
            })
        
        return latest_block
    
    if debugger:
        debugger.info("No order blocks found in lookback period")
    return None

# ---------------- REST OF SIGNAL GENERATION (WITH DEBUGGING) ----------------
async def elite_tf_alignment(exchange, symbol: str, side: str, debugger=None):
    tfs = ["15m","1h","4h"]
    alignment_results = []
    
    for tf in tfs:
        ohlcv = await fetch_ohlcv(exchange, symbol, tf, 50)
        if not ohlcv or len(ohlcv) < 10:
            alignment_results.append({"tf": tf, "result": "insufficient_data"})
            continue
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
        if len(df) < 5:
            alignment_results.append({"tf": tf, "result": "insufficient_candles"})
            continue
        trend = df["close"].iloc[-1] - df["close"].iloc[-5]
        trend_side = "BUY" if trend>0 else "SELL"
        matches = trend_side == side
        alignment_results.append({
            "tf": tf,
            "result": "match" if matches else "mismatch",
            "trend": round(trend, 6),
            "trend_side": trend_side
        })
        
        if not matches:
            if debugger:
                debugger.reject(f"HTF alignment failed on {tf}: {trend_side} != {side}", {
                    'timeframe': tf,
                    'trend': trend,
                    'trend_side': trend_side,
                    'required_side': side
                })
            return False
    
    if debugger:
        debugger.success("All HTF alignments passed", {'alignments': alignment_results})
    return True

async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    """Enhanced signal generation with comprehensive debugging"""
    
    # Initialize debugger for this signal
    debugger = SignalDebugger(symbol, tf)
    debugger.info(f"Starting signal generation for {symbol} on {tf}")
    
    if df is None or len(df) < 20:
        debugger.reject("Insufficient data", {'df_length': len(df) if df is not None else 0})
        return None
    
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    score = 0
    reasons = []
    
    calc_values = {}

    # Step 1: ENHANCED Liquidity Sweep Detection
    debugger.info("Step 1: Liquidity Sweep Detection")
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
    
    debugger.debug_data_point('sweep_detection', {
        'sweep_high': sweep_high,
        'sweep_low': sweep_low,
        'respected_high': respected_high_sweep,
        'respected_low': respected_low_sweep,
        'has_sweep': has_sweep,
        'sweep_score': liquidity_sweep,
        'sweep_type': sweep_type,
        'sweep_direction': sweep_direction
    })

    # Step 2: Displacement
    debugger.info("Step 2: Displacement Calculation")
    displacement = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    calc_values["displacement_value"] = round(displacement, 2)
    has_disp = displacement > 0.6
    if has_disp:
        score += 2
        reasons.append(f"Displacement +2 ({displacement:.2f})")
        debugger.success(f"Displacement passed: {displacement:.2f}")
    else:
        reasons.append(f"Displacement +0 ({displacement:.2f})")
        debugger.warn(f"Displacement low: {displacement:.2f}")
    
    debugger.debug_data_point('displacement', {
        'value': displacement,
        'has_disp': has_disp,
        'score_added': 2 if has_disp else 0
    })

    # Step 3 & 4: ENHANCED Order Block & Zone
    debugger.info("Step 3-4: Order Block Detection and Zone Approach")
    ob_zone = find_latest_ob(df, lookback=30, debugger=debugger)
    
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
                debugger.success(f"Bullish OB approach: distance={distance_to_ob:.2%}")
            else:
                approach_status = f"FAR ({distance_to_ob:.2%} away)"
                debugger.warn(f"Bullish OB far: distance={distance_to_ob:.2%}")
        else:  # bearish
            # For bearish OB, check if price is approaching from below
            distance_to_ob = (ob_zone["low"] - last["close"]) / (ob_zone["high"] - ob_zone["low"] + 1e-8)
            if last["close"] >= ob_zone["low"] or distance_to_ob < 0.1:
                score += 1
                zone_approach = 1
                approach_status = f"APPROACHING (dist: {distance_to_ob:.2%})"
                debugger.success(f"Bearish OB approach: distance={distance_to_ob:.2%}")
            else:
                approach_status = f"FAR ({distance_to_ob:.2%} away)"
                debugger.warn(f"Bearish OB far: distance={distance_to_ob:.2%}")
        
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
        debugger.reject("No order block detected")
        return None
    
    debugger.debug_data_point('order_block', {
        'type': ob_type,
        'zone_approach': zone_approach,
        'distance_to_ob': calc_values["distance_to_ob"]
    })

    # Step 5: HTF Alignment
    debugger.info("Step 5: HTF Alignment Check")
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
                score+=1
                htf_alignment=1
                reasons.append(f"HTF Alignment +1 ({htf_dir} {trend:+.6f})")
                debugger.success(f"HTF alignment passed: {htf_dir} ({trend:+.6f})")
            else:
                reasons.append(f"HTF Alignment +0 ({htf_dir} {trend:+.6f})")
                debugger.reject(f"HTF alignment failed: {htf_dir} != {ob_type}")
                return None
            calc_values["htf_trend"] = htf_trend_value
            calc_values["htf_direction"] = htf_dir
        else:
            reasons.append("HTF Alignment ? (insufficient data)")
            calc_values["htf_trend"] = 0
            calc_values["htf_direction"] = "UNKNOWN"
            debugger.reject("HTF insufficient data")
            return None
    else:
        reasons.append("HTF Alignment ? (no data)")
        calc_values["htf_trend"] = 0
        calc_values["htf_direction"] = "UNKNOWN"
        debugger.reject("HTF no data")
        return None
    
    debugger.debug_data_point('htf_alignment', {
        'alignment': htf_alignment,
        'trend': htf_trend_value,
        'direction': htf_dir
    })

    # Step 6: MOMENTUM
    debugger.info("Step 6: Momentum Check")
    momentum_ratio = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    calc_values["momentum_value"] = round(momentum_ratio, 2)
    
    if ob_type=="bullish" and momentum_ratio>=0.8 and last["close"]>last["open"]:
        score+=1
        reasons.append(f"Momentum +1 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 1
        debugger.success(f"Momentum passed: {momentum_ratio:.2f}")
    elif ob_type=="bearish" and momentum_ratio>=0.8 and last["close"]<last["open"]:
        score+=1
        reasons.append(f"Momentum +1 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 1
        debugger.success(f"Momentum passed: {momentum_ratio:.2f}")
    else:
        reasons.append(f"Momentum +0 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 0
        debugger.warn(f"Momentum low: {momentum_ratio:.2f}")
    
    debugger.debug_data_point('momentum', {
        'ratio': momentum_ratio,
        'score_added': calc_values["momentum_score"]
    })

    if not ob_type:
        debugger.reject("No order block type determined")
        return None
    
    side_str = "BUY" if ob_type=="bullish" else "SELL"
    entry = float(last["close"])
    
    debugger.info(f"Signal side: {side_str}, Entry: {entry:.6f}, Score: {score}/6")

    # ---------------- CRITICAL FILTERS ----------------
    debugger.info("Applying critical filters")
    critical_score = htf_alignment + liquidity_sweep
    debugger.debug_data_point('critical_score', {
        'htf_alignment': htf_alignment,
        'liquidity_sweep': liquidity_sweep,
        'total': critical_score,
        'required': CRITICAL_FACTORS_MIN
    })
    
    if critical_score < CRITICAL_FACTORS_MIN:
        debugger.reject(f"Critical score {critical_score} < {CRITICAL_FACTORS_MIN}", {
            'critical_score': critical_score,
            'required': CRITICAL_FACTORS_MIN
        })
        return None
    
    if score < MIN_SCORE:
        debugger.reject(f"Score {score} < {MIN_SCORE}", {
            'score': score,
            'required': MIN_SCORE
        })
        return None
    
    if not has_disp:
        debugger.reject("No displacement", {'displacement': displacement})
        return None
    
    if htf_alignment != 1:
        debugger.reject("HTF alignment not 1", {'htf_alignment': htf_alignment})
        return None

    # ---------------- FORCED FILTER ----------------
    debugger.info("Applying forced filter")
    displacement_val = calc_values["displacement_value"]
    momentum_val = calc_values["momentum_value"]
    
    if not force_filter_trade(momentum_val, displacement_val, debugger):
        # debugger already logs rejection in the function
        return None
    
    filter_reason = "Mom≥0.87" if momentum_val >= MOMENTUM_STRONG_THRESHOLD else "Mom≥0.85 & Disp≥0.80"
    reasons.append(f"✅ FORCED FILTER PASSED: {filter_reason}")
    debugger.success(f"Forced filter passed: {filter_reason}")

    # ---------------- ELITE MTF CONFIRMATION ----------------
    debugger.info("Checking elite MTF confirmation")
    if not await elite_tf_alignment(exchange, symbol, side_str, debugger):
        # debugger already logs rejection in the function
        return None
    reasons.append("Elite MTF Alignment ✅")
    debugger.success("Elite MTF alignment passed")

    # ---------------- ROMEOPT TP CALCULATION ----------------
    debugger.info("Starting RomeOPT TP calculation")
    atr_val = float(atr(df, 14).iloc[-1])
    result = romeopt_tp_sl(entry, side_str, atr_val, ob_zone, df, debugger)
    
    # REJECT if no valid TP found
    if result is None:
        reasons.append("❌ NO VALID LIQUIDITY FOUND")
        # debugger already logs rejection in the function
        return None
    
    sl, tp, tp_type = result
    
    # Calculate R:R for summary
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
        "reason": "RomeOPT 6-Step",
        "reason_list": reasons,
        "htf_alignment": htf_alignment,
        "liquidity_sweep": liquidity_sweep,
        "momentum_ratio": momentum_ratio,
        "calc_values": calc_values,
        "tp_type": tp_type,
        "rr_ratio": rr_ratio
    }
    
    # Log final success with debug summary
    debugger.success(f"Signal generated successfully!", {
        'entry': entry,
        'sl': sl,
        'tp': tp,
        'rr_ratio': round(rr_ratio, 2),
        'score': score,
        'tp_type': tp_type
    })
    
    # Log debug summary to file
    debug_summary = debugger.get_summary()
    debug_summary['signal_generated'] = True
    debug_summary['final_score'] = score
    debug_summary['rr_ratio'] = rr_ratio
    
    debug_logger.info(f"DEBUG SUMMARY: {debug_summary}", 
                     extra={'symbol': symbol, 'timeframe': tf})
    
    log.info(f"✅ Signal {sig['symbol']} passed forced filter: Mom={momentum_val:.2f}, Disp={displacement_val:.2f}")
    return sig

# ---------------- REFINED UPDATE TP/SL LIVE ----------------
def update_tp_sl_live(sig: dict, df: pd.DataFrame):
    """
    REFINED: Only update if TP hasn't been hit AND structure invalidated
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
    
    # Structure still valid - keep original TP
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

# ---------------- DEBUG WEBHOOK ENDPOINT ----------------
@app.get("/debug/latest")
async def get_latest_debug():
    """Get latest debug information from log file"""
    try:
        with open(DEBUG_LOG_FILE, 'r') as f:
            lines = f.readlines()[-100:]  # Last 100 lines
        return {"debug_log": lines}
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/rejections")
async def get_rejection_summary(hours: int = 24):
    """Get summary of rejection reasons for the last N hours"""
    try:
        cutoff_time = time.time() - (hours * 3600)
        rejections = defaultdict(int)
        
        with open(DEBUG_LOG_FILE, 'r') as f:
            for line in f:
                if "❌ REJECTED:" in line:
                    # Extract rejection reason
                    parts = line.split("❌ REJECTED:")
                    if len(parts) > 1:
                        reason = parts[1].split("|")[0].strip()
                        rejections[reason] += 1
        
        return {"rejection_summary": dict(rejections)}
    except Exception as e:
        return {"error": str(e)}

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
                if deprioritized(symbol): 
                    log.debug(f"Skipping {symbol} - deprioritized due to recent SL hits")
                    continue
                for tf in TIMEFRAMES:
                    key=f"{symbol}:{tf}"
                    if key in last_signal_time and time.time()-last_signal_time[key]<60: 
                        continue
                    ohlcv = await fetch_ohlcv(exchange,symbol,tf,200)
                    if not ohlcv: 
                        continue
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
        except Exception as e: 
            log.exception("scan error: %s", e)
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
    await tg("🏆 TRUE ROMEOPT SCANNER STARTED (ENHANCED DEBUGGING)")
    await tg("🔍 DEBUG MODE: ON - Detailed logging enabled")
    await tg("📊 DEBUG LOG: /app/data/debug_signals.log")
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