#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT SCANNER v2 - Score-Only Implementation
With Position Monitoring & SL/TP Alerts
FIXED VERSION: Prevents duplicate signals and sends exit messages
"""

import os
import time
import asyncio
import logging
import datetime
import json
import hashlib
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI
import uvicorn
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from collections import defaultdict

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/romeopt_v2.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 15))
TOP_N = int(os.getenv("TOP_N", 10))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 5))

# Cooldown settings
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", 60))  # Default 1 hour cooldown
MIN_PRICE_CHANGE = float(os.getenv("MIN_PRICE_CHANGE", 0.01))  # 1% minimum price change

# Monitoring settings
MONITOR_INTERVAL = int(os.getenv("MONITOR_INTERVAL", 30))  # Check positions every 30 seconds

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_v2")
db_lock = asyncio.Lock()
db_conn = None

# ---------------- DEBUGGING ----------------
DEBUG_MODE = True
last_activity_time = time.time()
watchdog_interval = 30  # seconds

async def watchdog():
    """Watchdog to detect and recover from freezing"""
    global last_activity_time
    
    while True:
        await asyncio.sleep(watchdog_interval)
        
        time_since_last_activity = time.time() - last_activity_time
        if time_since_last_activity > watchdog_interval * 2:
            log.error(f"⚠️ WATCHDOG: No activity for {time_since_last_activity:.1f}s! Potential freeze detected.")
            
            # Try to send a debug message
            try:
                await send_telegram(f"⚠️ Scanner watchdog detected potential freeze after {time_since_last_activity:.1f}s of inactivity")
            except:
                pass
            
            # Reset activity time
            last_activity_time = time.time()

def update_activity():
    """Update last activity timestamp"""
    global last_activity_time
    last_activity_time = time.time()

# ---------------- DATA STRUCTURES ----------------
@dataclass
class HTFContext:
    """Step 1: HTF Bias output"""
    bias: str  # "BULLISH", "BEARISH", "RANGING"
    range_high: float
    range_low: float
    range_mid: float
    premium_discount: str  # "PREMIUM", "DISCOUNT", "MIDDLE"
    liquidity_zones: List[Dict]  # HTF liquidity levels
    structure: List[Dict]  # Swing highs/lows
    skip_reason: Optional[str] = None
    valid: bool = False

@dataclass
class LiquidityMap:
    """Step 2: Liquidity Map output"""
    from_liquidity: List[Dict]  # Liquidity being moved FROM
    to_liquidity: List[Dict]    # Liquidity targets TO move to
    has_clear_target: bool = False

@dataclass
class SweepAnalysis:
    """Step 3: Liquidity Sweep output"""
    type: str  # "HIGH_SWEEP", "LOW_SWEEP", "NONE"
    candle_index: int
    swept_price: float
    previous_extreme: float
    impulsive: bool  # Body > wicks
    fake_sweep: bool = False
    strength: float = 0.0  # 0-1 score

@dataclass
class StructureShift:
    """Step 4: Structure Check output"""
    type: str  # "CHoCH" (reversal), "BOS" (continuation), "NONE"
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
    invalidation_type: str  # "SWEEP", "ORDER_BLOCK", "STRUCTURE"
    risk_amount: float
    sl_to_entry_distance: float

@dataclass
class TakeProfitLevels:
    """Step 7: Take Profit output"""
    tp1: float  # Nearest internal liquidity
    tp2: float  # Range boundary
    tp3: float  # HTF liquidity
    tp1_type: str = "INTERNAL_LIQUIDITY"
    tp2_type: str = "RANGE_BOUNDARY"
    tp3_type: str = "HTF_LIQUIDITY"

@dataclass
class ProbabilityScore:
    """Step 8: Probability Check output"""
    htf_alignment: float  # 0-1
    liquidity_quality: float  # 0-1
    sweep_strength: float  # 0-1
    structure_clarity: float  # 0-1
    entry_precision: float  # 0-1
    total_score: float  # 0-5
    
    @property
    def acceptable(self) -> bool:
        """Accept if total >= MIN_SCORE (no component minimums)"""
        MIN_SCORE = 0.5  # ← CHANGE THIS NUMBER TO ADJUST SENSITIVITY
        return self.total_score >= MIN_SCORE

# ---------------- POSITION TRACKING ----------------
@dataclass
class PositionStatus:
    ACTIVE = 'ACTIVE'
    CLOSED_SL = 'CLOSED_SL'
    CLOSED_TP1 = 'CLOSED_TP1'
    CLOSED_TP2 = 'CLOSED_TP2'
    CLOSED_TP3 = 'CLOSED_TP3'
    EXPIRED = 'EXPIRED'

# ---------------- TELEGRAM ----------------
async def send_telegram(msg: str, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": parse_mode
            })
            update_activity()
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

# ---------------- DEDUPLICATION HELPERS ----------------
def generate_setup_hash(setup: Dict) -> str:
    """Generate a unique hash for a setup based on key parameters"""
    # Key parameters that define a unique setup
    params = [
        setup['symbol'],
        setup['side'],
        f"{setup['entry_price']:.8f}",
        f"{setup['sl_price']:.8f}",
        f"{setup['tp1_price']:.8f}",
    ]
    
    # Join and hash
    param_string = "|".join(params)
    return hashlib.md5(param_string.encode()).hexdigest()[:16]

async def should_send_alert(setup: Dict) -> bool:
    """
    Check if we should send an alert for this setup
    Returns True if no recent alert for this setup
    """
    setup_hash = generate_setup_hash(setup)
    symbol = setup['symbol']
    side = setup['side']
    
    async with db_lock:
        # 1. Check if already has active position for same symbol/side
        cursor = await db_conn.execute("""
            SELECT COUNT(*) FROM positions 
            WHERE symbol = ? AND side = ? AND status = 'ACTIVE'
        """, (symbol, side))
        active_count = (await cursor.fetchone())[0]
        
        if active_count > 0:
            log.debug(f"  {symbol}: Already has active {side} position, skipping")
            return False
        
        # 2. Check cooldown table (existing logic)
        cursor = await db_conn.execute("""
            SELECT last_alert_time, alert_count 
            FROM signal_cooldown 
            WHERE symbol = ? AND entry_hash = ?
        """, (symbol, setup_hash))
        
        row = await cursor.fetchone()
        
        if row:
            last_alert_time = datetime.datetime.fromisoformat(row[0])
            alert_count = row[1]
            now = datetime.datetime.utcnow()
            
            # Dynamic cooldown
            dynamic_cooldown = COOLDOWN_MINUTES * (alert_count ** 0.5)
            minutes_since_last = (now - last_alert_time).total_seconds() / 60
            
            if minutes_since_last < dynamic_cooldown:
                log.debug(f"  {symbol}: Cooldown active ({minutes_since_last:.1f}min < {dynamic_cooldown:.1f}min)")
                return False
            
            # Update existing
            await db_conn.execute("""
                UPDATE signal_cooldown 
                SET last_alert_time = ?, alert_count = alert_count + 1
                WHERE symbol = ? AND entry_hash = ?
            """, (now.isoformat(), symbol, setup_hash))
            await db_conn.commit()
            return True
        else:
            # Insert new
            await db_conn.execute("""
                INSERT INTO signal_cooldown (symbol, side, entry_hash, last_alert_time)
                VALUES (?, ?, ?, ?)
            """, (symbol, side, setup_hash, now.isoformat()))
            await db_conn.commit()
            return True

async def is_similar_to_recent_setup(setup: Dict) -> bool:
    """
    Check if setup is too similar to recent ones for the same symbol
    """
    symbol = setup['symbol']
    current_entry = setup['entry_price']
    current_side = setup['side']
    
    # Check active positions first
    async with db_lock:
        cursor = await db_conn.execute("""
            SELECT entry_price 
            FROM positions 
            WHERE symbol = ? AND side = ? AND status = 'ACTIVE'
            LIMIT 1
        """, (symbol, current_side))
        
        active_pos = await cursor.fetchone()
        if active_pos:
            prev_entry = active_pos[0]
            price_diff_pct = abs(current_entry - prev_entry) / prev_entry * 100
            if price_diff_pct < MIN_PRICE_CHANGE:
                log.debug(f"  {symbol}: Similar to active position (price diff: {price_diff_pct:.2f}%)")
                return True
    
    # Check recent signals (last 2 hours instead of 4)
    cutoff = (datetime.datetime.utcnow() - datetime.timedelta(hours=2)).isoformat()
    
    async with db_lock:
        cursor = await db_conn.execute("""
            SELECT entry_price, timestamp 
            FROM signals 
            WHERE symbol = ? AND side = ? AND timestamp > ?
            ORDER BY timestamp DESC LIMIT 3
        """, (symbol, current_side, cutoff))
        
        rows = await cursor.fetchall()
        
        for row in rows:
            prev_entry = row[0]
            prev_time = datetime.datetime.fromisoformat(row[1])
            now = datetime.datetime.utcnow()
            
            price_diff_pct = abs(current_entry - prev_entry) / prev_entry * 100
            
            # If price is very similar and recent
            if price_diff_pct < MIN_PRICE_CHANGE and (now - prev_time).total_seconds() < 7200:
                log.debug(f"  {symbol}: Setup too similar to recent signal "
                         f"(price diff: {price_diff_pct:.2f}% < {MIN_PRICE_CHANGE}%)")
                return True
    
    return False

# ---------------- DATABASE ----------------
async def init_db():
    global db_conn
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute("PRAGMA journal_mode=WAL;")
    await db_conn.execute("PRAGMA busy_timeout=5000;")
    
    # Create signals table with step-by-step tracking
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            timestamp TEXT,
            side TEXT,
            
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
            notes TEXT
        )
    """)
    
    # Create cooldown tracking table
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_cooldown (
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_hash TEXT NOT NULL,
            last_alert_time TEXT NOT NULL,
            alert_count INTEGER DEFAULT 1,
            PRIMARY KEY (symbol, entry_hash)
        )
    """)
    
    # Create positions tracking table
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_price REAL NOT NULL,
            sl_price REAL NOT NULL,
            tp1_price REAL NOT NULL,
            tp2_price REAL NOT NULL,
            tp3_price REAL NOT NULL,
            entry_time TEXT NOT NULL,
            exit_time TEXT,
            exit_price REAL,
            exit_type TEXT,  -- 'SL', 'TP1', 'TP2', 'TP3', 'EXPIRED', 'MANUAL'
            pnl_percent REAL,
            status TEXT DEFAULT 'ACTIVE',
            pnl_updated_time TEXT,
            FOREIGN KEY (signal_id) REFERENCES signals(id) ON DELETE CASCADE
        )
    """)
    
    # Create position updates table for tracking price movements
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS position_updates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            position_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            current_price REAL NOT NULL,
            pnl_percent REAL,
            distance_to_sl_pct REAL,
            distance_to_tp1_pct REAL,
            distance_to_tp2_pct REAL,
            distance_to_tp3_pct REAL,
            FOREIGN KEY (position_id) REFERENCES positions(id) ON DELETE CASCADE
        )
    """)
    
    # Create indexes for faster lookups
    await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol_timestamp ON signals(symbol, timestamp)")
    await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_cooldown_time ON signal_cooldown(last_alert_time)")
    await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status)")
    await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_positions_symbol_status ON positions(symbol, status)")
    await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_position_updates_position ON position_updates(position_id, timestamp)")
    
    await db_conn.commit()

# ---------------- POSITION MANAGEMENT ----------------
async def create_position_from_setup(setup: Dict, signal_id: int) -> int:
    """Create a new position from a setup and return position ID"""
    async with db_lock:
        cursor = await db_conn.execute("""
            INSERT INTO positions 
            (signal_id, symbol, side, entry_price, sl_price, 
             tp1_price, tp2_price, tp3_price, entry_time, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE')
            RETURNING id
        """, (
            signal_id,
            setup['symbol'],
            setup['side'],
            setup['entry_price'],
            setup['sl_price'],
            setup['tp1_price'],
            setup['tp2_price'],
            setup['tp3_price'],
            setup['timestamp']
        ))
        
        position_id = (await cursor.fetchone())[0]
        await db_conn.commit()
        
        return position_id

async def log_position_update(position_id: int, current_price: float,
                             entry_price: float, sl_price: float,
                             tp1_price: float, tp2_price: float, tp3_price: float):
    """Log a position update with PnL calculation"""
    
    # Calculate PnL
    pnl_percent = 0.0
    if entry_price > 0:
        pnl_percent = ((current_price - entry_price) / entry_price) * 100
    
    # Calculate distances
    distance_to_sl = 0.0
    distance_to_tp1 = 0.0
    distance_to_tp2 = 0.0
    distance_to_tp3 = 0.0
    
    if entry_price > 0:
        distance_to_sl = abs(current_price - sl_price) / entry_price * 100
        distance_to_tp1 = abs(current_price - tp1_price) / entry_price * 100
        distance_to_tp2 = abs(current_price - tp2_price) / entry_price * 100
        distance_to_tp3 = abs(current_price - tp3_price) / entry_price * 100
    
    async with db_lock:
        await db_conn.execute("""
            INSERT INTO position_updates 
            (position_id, timestamp, current_price, pnl_percent,
             distance_to_sl_pct, distance_to_tp1_pct, distance_to_tp2_pct, distance_to_tp3_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            position_id,
            datetime.datetime.utcnow().isoformat(),
            current_price,
            pnl_percent,
            distance_to_sl,
            distance_to_tp1,
            distance_to_tp2,
            distance_to_tp3
        ))
        
        # Update position PnL
        await db_conn.execute("""
            UPDATE positions 
            SET pnl_percent = ?, pnl_updated_time = ?
            WHERE id = ?
        """, (pnl_percent, datetime.datetime.utcnow().isoformat(), position_id))
        
        await db_conn.commit()

async def close_position(position_id: int, exit_price: float, exit_type: str):
    """Close a position and calculate final PnL"""
    async with db_lock:
        # Get position details
        cursor = await db_conn.execute("""
            SELECT symbol, side, entry_price FROM positions 
            WHERE id = ? AND status = 'ACTIVE'
        """, (position_id,))
        
        position = await cursor.fetchone()
        if not position:
            log.warning(f"Position {position_id} not found or already closed")
            return False
        
        symbol, side, entry_price = position
        
        # Calculate final PnL
        pnl_percent = ((exit_price - entry_price) / entry_price) * 100
        
        # Update position
        await db_conn.execute("""
            UPDATE positions 
            SET exit_time = ?, exit_price = ?, exit_type = ?, 
                pnl_percent = ?, status = 'CLOSED'
            WHERE id = ?
        """, (
            datetime.datetime.utcnow().isoformat(),
            exit_price,
            exit_type,
            pnl_percent,
            position_id
        ))
        
        await db_conn.commit()
        
        log.info(f"Closed position {position_id} ({symbol}) at {exit_price} via {exit_type}")
        return True

def calculate_time_in_trade(entry_time_str: str) -> str:
    """Calculate time elapsed since entry"""
    try:
        entry_time = datetime.datetime.fromisoformat(entry_time_str)
        now = datetime.datetime.utcnow()
        diff = now - entry_time
        
        days = diff.days
        hours = diff.seconds // 3600
        minutes = (diff.seconds % 3600) // 60
        
        if days > 0:
            return f"{days}d {hours}h"
        elif hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m"
    except:
        return "Unknown"

# ---------------- POSITION MONITORING ----------------
async def monitor_positions_lightweight():
    """Lightweight position monitor - runs less frequently"""
    log.info("Starting lightweight position monitor...")
    
    # Start with a delay to let scanner initialize
    await asyncio.sleep(60)
    
    while True:
        try:
            update_activity()
            log.debug("Position monitor cycle starting...")
            
            # Get all active positions
            async with db_lock:
                cursor = await db_conn.execute("""
                    SELECT id, signal_id, symbol, side, entry_price, 
                           sl_price, tp1_price, tp2_price, tp3_price, entry_time
                    FROM positions 
                    WHERE status = 'ACTIVE'
                """)  # Removed LIMIT - monitor all positions
                active_positions = await cursor.fetchall()
            
            if not active_positions:
                log.debug("No active positions to monitor")
                await asyncio.sleep(MONITOR_INTERVAL)
                continue
            
            log.info(f"Monitoring {len(active_positions)} active positions")
            
            # Create a temporary exchange instance for monitoring
            monitor_exchange = ccxt.okx({
                "enableRateLimit": True,
                "options": {"defaultType": "spot"}
            })
            
            for position in active_positions:
                try:
                    (position_id, signal_id, symbol, side, entry_price, 
                     sl_price, tp1_price, tp2_price, tp3_price, entry_time) = position
                    
                    # Fetch current price with retry
                    current_price = 0
                    for attempt in range(3):
                        try:
                            ticker = await monitor_exchange.fetch_ticker(symbol)
                            current_price = ticker.get('last', 0)
                            if current_price > 0:
                                break
                        except Exception as e:
                            log.warning(f"Attempt {attempt+1} failed for {symbol}: {e}")
                            await asyncio.sleep(1)
                    else:
                        log.error(f"Failed to get price for {symbol} after 3 attempts")
                        continue
                    
                    if current_price <= 0:
                        continue
                    
                    # Log position update
                    await log_position_update(
                        position_id, current_price, entry_price, 
                        sl_price, tp1_price, tp2_price, tp3_price
                    )
                    
                    # Check for SL/TP hits
                    sl_hit = False
                    tp_hit_level = None
                    exit_price = current_price
                    
                    if side.upper() == 'BUY':
                        # For BUY positions
                        if current_price <= sl_price:
                            sl_hit = True
                        elif current_price >= tp3_price:
                            tp_hit_level = 'TP3'
                        elif current_price >= tp2_price:
                            tp_hit_level = 'TP2'
                        elif current_price >= tp1_price:
                            tp_hit_level = 'TP1'
                    
                    elif side.upper() == 'SELL':
                        # For SELL positions
                        if current_price >= sl_price:
                            sl_hit = True
                        elif current_price <= tp3_price:
                            tp_hit_level = 'TP3'
                        elif current_price <= tp2_price:
                            tp_hit_level = 'TP2'
                        elif current_price <= tp1_price:
                            tp_hit_level = 'TP1'
                    
                    # Handle exit if any level hit
                    if sl_hit or tp_hit_level:
                        exit_type = 'SL' if sl_hit else tp_hit_level
                        log.info(f"Position {position_id} ({symbol}) hit {exit_type} at {current_price}")
                        
                        success = await close_position(position_id, exit_price, exit_type)
                        if success:
                            await send_position_closure_alert(
                                symbol, side, entry_price, exit_price, 
                                exit_type, entry_time, position_id
                            )
                        else:
                            log.error(f"Failed to close position {position_id}")
                    
                    # Check for expired positions (older than 3 days instead of 7)
                    entry_time_dt = datetime.datetime.fromisoformat(entry_time)
                    now = datetime.datetime.utcnow()
                    
                    if (now - entry_time_dt).days >= 3:
                        exit_type = 'EXPIRED'
                        log.info(f"Position {position_id} ({symbol}) expired after 3 days")
                        
                        success = await close_position(position_id, current_price, exit_type)
                        if success:
                            await send_position_closure_alert(
                                symbol, side, entry_price, current_price, 
                                exit_type, entry_time, position_id
                            )
                    
                except Exception as e:
                    log.error(f"Error monitoring position {position_id} ({symbol}): {e}")
                    continue
            
            # Close the monitor exchange
            await monitor_exchange.close()
            
        except Exception as e:
            log.error(f"Position monitoring error: {e}")
        
        # Wait for next monitoring cycle
        log.debug(f"Position monitor sleeping for {MONITOR_INTERVAL}s")
        await asyncio.sleep(MONITOR_INTERVAL)

async def send_position_closure_alert(symbol: str, side: str, entry_price: float,
                                     exit_price: float, exit_type: str,
                                     entry_time: str, position_id: int):
    """Send alert when position is closed"""
    
    # Calculate PnL
    pnl_percent = ((exit_price - entry_price) / entry_price) * 100
    pnl_abs = exit_price - entry_price
    time_in_trade = calculate_time_in_trade(entry_time)
    
    # Format message based on exit type
    if exit_type == 'SL':
        emoji = "🛑"
        title = "STOP LOSS HIT"
        color = "🔴"
    elif exit_type == 'EXPIRED':
        emoji = "⏰"
        title = "POSITION EXPIRED"
        color = "🟡"
    else:
        emoji = "🎯"
        title = "TAKE PROFIT HIT"
        color = "🟢"
    
    pnl_color = "🟢" if pnl_percent > 0 else "🔴"
    
    msg = f"""
{emoji} <b>{title}</b>

<b>Symbol:</b> {symbol}
<b>Side:</b> {side}
<b>Position ID:</b> {position_id}

<b>Entry Price:</b> {entry_price:.8f}
<b>Exit Price:</b> {exit_price:.8f}
<b>Exit Type:</b> {exit_type}

<b>PnL:</b> {pnl_color} {pnl_percent:+.2f}% ({pnl_abs:+.8f})
<b>Time in Trade:</b> {time_in_trade}

{color} <b>Level Hit:</b> {exit_type}

<i>Closed: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</i>
"""
    
    await send_telegram(msg)

# ---------------- OHLCV UTILS ----------------
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

# ---------------- FIX: JSON SERIALIZATION HELPER ----------------
def safe_json_serialize(obj):
    """Convert numpy/pandas types to Python native types for JSON serialization"""
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

# ---------------- STEP 1: HTF BIAS ----------------
async def analyze_htf_bias(exchange, symbol: str) -> HTFContext:
    """
    Step 1: HTF Bias - No longer skips mid-range
    """
    
    # Try 4H data (primary HTF)
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, "4h", 100)
    timeframe_used = "4h"
    
    # If 4H insufficient, fallback to 1H
    if not ohlcv_htf or len(ohlcv_htf) < 30:
        log.debug(f"{symbol}: 4H data insufficient, falling back to 1H...")
        ohlcv_htf = await fetch_ohlcv(exchange, symbol, "1h", 100)
        timeframe_used = "1h"
        
        # If 1H also insufficient, return empty context
        if not ohlcv_htf or len(ohlcv_htf) < 30:
            return HTFContext(
                bias="UNKNOWN", range_high=0, range_low=0, range_mid=0,
                premium_discount="UNKNOWN", liquidity_zones=[], structure=[],
                skip_reason="Insufficient HTF data", valid=True  # Still valid for scoring
            )
    
    df_htf = create_dataframe(ohlcv_htf)
    current_price = float(df_htf["close"].iloc[-1])
    
    # Identify swing highs/lows
    swing_highs = []
    swing_lows = []
    
    for i in range(3, len(df_htf) - 3):
        high_i = df_htf["high"].iloc[i]
        low_i = df_htf["low"].iloc[i]
        
        # Check for swing high
        if (high_i > df_htf["high"].iloc[i-1] and 
            high_i > df_htf["high"].iloc[i-2] and
            high_i > df_htf["high"].iloc[i+1] and
            high_i > df_htf["high"].iloc[i+2]):
            swing_highs.append({
                "price": float(high_i),
                "index": int(i),
                "timestamp": int(df_htf["timestamp"].iloc[i])
            })
        
        # Check for swing low
        if (low_i < df_htf["low"].iloc[i-1] and 
            low_i < df_htf["low"].iloc[i-2] and
            low_i < df_htf["low"].iloc[i+1] and
            low_i < df_ltf["low"].iloc[i+2]):
            swing_lows.append({
                "price": float(low_i),
                "index": int(i),
                "timestamp": int(df_htf["timestamp"].iloc[i])
            })
    
    # Define current range
    if len(df_htf) >= 20:
        recent_high = df_htf["high"].iloc[-20:].max()
        recent_low = df_htf["low"].iloc[-20:].min()
    else:
        recent_high = df_htf["high"].max()
        recent_low = df_htf["low"].min()
    
    range_high = float(recent_high)
    range_low = float(recent_low)
    range_mid = (range_high + range_low) / 2
    
    # Determine bias
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
    
    # Premium/Discount
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
    
    # Mark HTF liquidity zones
    liquidity_zones = []
    
    # Range boundaries
    liquidity_zones.append({
        "price": range_high,
        "type": "RANGE_HIGH",
        "timeframe": timeframe_used,
        "strength": 3
    })
    liquidity_zones.append({
        "price": range_low,
        "type": "RANGE_LOW",
        "timeframe": timeframe_used,
        "strength": 3
    })
    
    # Recent swing points
    for swing in swing_highs[-3:]:
        liquidity_zones.append({
            "price": swing["price"],
            "type": "SWING_HIGH",
            "timeframe": timeframe_used,
            "strength": 2
        })
    
    for swing in swing_lows[-3:]:
        liquidity_zones.append({
            "price": swing["price"],
            "type": "SWING_LOW",
            "timeframe": timeframe_used,
            "strength": 2
        })
    
    context = HTFContext(
        bias=bias,
        range_high=range_high,
        range_low=range_low,
        range_mid=range_mid,
        premium_discount=premium_discount,
        liquidity_zones=liquidity_zones,
        structure=swing_highs[-5:] + swing_lows[-5:],
        skip_reason=None,
        valid=True  # Always valid now
    )
    
    return context

# ---------------- STEP 2: LIQUIDITY MAP ----------------
async def map_liquidity(exchange, symbol: str, htf_context: HTFContext, 
                       current_price: float) -> LiquidityMap:
    """
    Step 2: Liquidity Map - Always returns something
    """
    
    # Fetch 1H for more granular liquidity
    ohlcv_1h = await fetch_ohlcv(exchange, symbol, "1h", 100)
    if not ohlcv_1h:
        return LiquidityMap(from_liquidity=[], to_liquidity=[], has_clear_target=False)
    
    df_1h = create_dataframe(ohlcv_1h)
    
    # FROM liquidity: Recent extremes
    from_liquidity = []
    
    recent_df = df_1h.iloc[-10:] if len(df_1h) >= 10 else df_1h
    
    for i in range(len(recent_df) - 1):
        candle = recent_df.iloc[i]
        next_candle = recent_df.iloc[i + 1] if i + 1 < len(recent_df) else candle
        
        # High sweep detection
        if candle["high"] > recent_df["high"].iloc[max(0, i-5):i].max() and next_candle["close"] < candle["close"]:
            from_liquidity.append({
                "price": float(candle["high"]),
                "type": "SWEPT_HIGH",
                "timeframe": "1h",
                "direction": "FROM"
            })
        
        # Low sweep detection
        if candle["low"] < recent_df["low"].iloc[max(0, i-5):i].min() and next_candle["close"] > candle["close"]:
            from_liquidity.append({
                "price": float(candle["low"]),
                "type": "SWEPT_LOW",
                "timeframe": "1h",
                "direction": "FROM"
            })
    
    # TO liquidity: Targets based on HTF context
    to_liquidity = []
    
    # Sort HTF liquidity zones by distance
    sorted_zones = sorted(htf_context.liquidity_zones, 
                         key=lambda z: abs(z["price"] - current_price))
    
    # Filter for relevant targets
    if htf_context.bias == "BULLISH":
        targets = [z for z in sorted_zones if z["price"] > current_price]
    elif htf_context.bias == "BEARISH":
        targets = [z for z in sorted_zones if z["price"] < current_price]
    else:
        targets = [z for z in sorted_zones if z["type"] in ["RANGE_HIGH", "RANGE_LOW"]]
    
    # Take top 3 targets
    for target in targets[:3]:
        to_liquidity.append({
            "price": target["price"],
            "type": target["type"],
            "timeframe": target["timeframe"],
            "strength": int(target.get("strength", 1)),
            "direction": "TO"
        })
    
    # Also add internal liquidity
    if len(df_1h) >= 24:
        high_values = df_1h["high"].iloc[-24:].values
        for val in np.unique(np.round(high_values, 4)):
            count = int(np.sum(np.round(high_values, 4) == val))
            if count >= 2:
                to_liquidity.append({
                    "price": float(val),
                    "type": "EQUAL_HIGH",
                    "timeframe": "1h",
                    "strength": int(min(2, count)),
                    "direction": "TO"
                })
        
        low_values = df_1h["low"].iloc[-24:].values
        for val in np.unique(np.round(low_values, 4)):
            count = int(np.sum(np.round(low_values, 4) == val))
            if count >= 2:
                to_liquidity.append({
                    "price": float(val),
                    "type": "EQUAL_LOW",
                    "timeframe": "1h",
                    "strength": int(min(2, count)),
                    "direction": "TO"
                })
    
    # Always has at least some targets (even if empty)
    has_clear_target = len(to_liquidity) > 0
    
    return LiquidityMap(
        from_liquidity=from_liquidity,
        to_liquidity=to_liquidity,
        has_clear_target=has_clear_target
    )

# ---------------- STEP 3: LIQUIDITY SWEEP ----------------
async def analyze_sweep(exchange, symbol: str, htf_context: HTFContext) -> SweepAnalysis:
    """
    Step 3: Liquidity Sweep - Always returns something
    """
    
    # Use 15m for sweep detection
    ohlcv_15m = await fetch_ohlcv(exchange, symbol, "15m", 50)
    if not ohlcv_15m or len(ohlcv_15m) < 10:
        return SweepAnalysis(type="NONE", candle_index=-1, swept_price=0, 
                           previous_extreme=0, impulsive=False)
    
    df_15m = create_dataframe(ohlcv_15m)
    
    # Look for sweeps in last 5 candles
    lookback = min(5, len(df_15m))
    
    for i in range(-lookback, 0):
        candle_idx = len(df_15m) + i
        candle = df_15m.iloc[candle_idx]
        
        # Get previous candles
        start_idx = max(0, candle_idx - 5)
        prev_candles = df_15m.iloc[start_idx:candle_idx]
        
        if len(prev_candles) == 0:
            continue
        
        previous_high = prev_candles["high"].max()
        previous_low = prev_candles["low"].min()
        
        # Check for high sweep
        if candle["high"] > previous_high:
            body_size = abs(candle["close"] - candle["open"])
            upper_wick = candle["high"] - max(candle["open"], candle["close"])
            lower_wick = min(candle["open"], candle["close"]) - candle["low"]
            total_wick = upper_wick + lower_wick
            
            impulsive = body_size > total_wick
            
            # Don't check for fake sweeps
            fake_sweep = False
            
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
        
        # Check for low sweep
        elif candle["low"] < previous_low:
            body_size = abs(candle["close"] - candle["open"])
            upper_wick = candle["high"] - max(candle["open"], candle["close"])
            lower_wick = min(candle["open"], candle["close"]) - candle["low"]
            total_wick = upper_wick + lower_wick
            
            impulsive = body_size > total_wick
            
            fake_sweep = False
            
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

# ---------------- STEP 4: STRUCTURE CHECK ----------------
async def check_structure_shift(exchange, symbol: str, sweep: SweepAnalysis, 
                               htf_context: HTFContext) -> StructureShift:
    """
    Step 4: Structure Check - Always returns something
    """
    
    if sweep.type == "NONE":
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    # Fetch 15m data
    ohlcv_15m = await fetch_ohlcv(exchange, symbol, "15m", 50)
    if not ohlcv_15m:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    df_15m = create_dataframe(ohlcv_15m)
    
    # Get candles after the sweep
    sweep_idx = sweep.candle_index
    if sweep_idx < 0 or sweep_idx >= len(df_15m) - 3:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    post_sweep_candles = df_15m.iloc[sweep_idx + 1:]
    if len(post_sweep_candles) < 3:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    # Check for CHoCH or BOS
    if sweep.type == "HIGH_SWEEP":
        recent_low_before = df_15m["low"].iloc[max(0, sweep_idx-5):sweep_idx].min()
        
        for i in range(len(post_sweep_candles)):
            candle = post_sweep_candles.iloc[i]
            if candle["low"] < recent_low_before:
                return StructureShift(
                    type="CHoCH",
                    confirmed=True,
                    candle_index=int(sweep_idx + i + 1),
                    description="High sweep followed by break below recent low"
                )
        
        # Check for BOS
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
        recent_high_before = df_15m["high"].iloc[max(0, sweep_idx-5):sweep_idx].max()
        
        for i in range(len(post_sweep_candles)):
            candle = post_sweep_candles.iloc[i]
            if candle["high"] > recent_high_before:
                return StructureShift(
                    type="CHoCH",
                    confirmed=True,
                    candle_index=int(sweep_idx + i + 1),
                    description="Low sweep followed by break above recent high"
                )
        
        # Check for BOS
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
                         side: str) -> EntryZone:
    """
    Step 5: Entry Zone - Always returns something
    """
    
    # Fetch 5m data
    ohlcv_5m = await fetch_ohlcv(exchange, symbol, "5m", 100)
    if not ohlcv_5m:
        # Return a default entry zone at current price
        ticker = await exchange.fetch_ticker(symbol)
        current_price = ticker.get("last", 0)
        return EntryZone(
            type="DEFAULT",
            price=float(current_price),
            low=float(current_price * 0.99),
            high=float(current_price * 1.01),
            aligns_with_htf=True,
            candle_reaction=True
        )
    
    df_5m = create_dataframe(ohlcv_5m)
    current_price = float(df_5m["close"].iloc[-1])
    
    # Try to find best entry type
    entry_type = "DEFAULT"
    
    # Try Order Blocks first
    for i in range(2, len(df_5m) - 1):
        candle = df_5m.iloc[i]
        next_candle = df_5m.iloc[i + 1]
        
        # Bullish OB
        if side == "BUY":
            if (candle["close"] < candle["open"] and 
                next_candle["close"] > next_candle["open"]):
                ob_low = min(candle["low"], next_candle["low"])
                ob_high = next_candle["close"]
                
                aligns = (htf_context.bias == "BULLISH" or 
                         htf_context.premium_discount == "DISCOUNT")
                
                if current_price <= ob_high and current_price >= ob_low * 0.995:
                    current_candle = df_5m.iloc[-1]
                    prev_candle = df_5m.iloc[-2] if len(df_5m) >= 2 else current_candle
                    
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
        
        # Bearish OB
        elif side == "SELL":
            if (candle["close"] > candle["open"] and 
                next_candle["close"] < next_candle["open"]):
                ob_low = next_candle["close"]
                ob_high = max(candle["high"], next_candle["high"])
                
                aligns = (htf_context.bias == "BEARISH" or 
                         htf_context.premium_discount == "PREMIUM")
                
                if current_price >= ob_low and current_price <= ob_high * 1.005:
                    current_candle = df_5m.iloc[-1]
                    prev_candle = df_5m.iloc[-2] if len(df_5m) >= 2 else current_candle
                    
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
    
    # Default entry at current price
    return EntryZone(
        type="DEFAULT",
        price=float(current_price),
        low=float(current_price * 0.99),
        high=float(current_price * 1.01),
        aligns_with_htf=True,
        candle_reaction=True
    )

# ---------------- STEP 6: RISK/SL ----------------
def calculate_risk_sl(entry_zone: EntryZone, sweep: SweepAnalysis,
                     htf_context: HTFContext, side: str) -> RiskManagement:
    """
    Step 6: Risk/SL - Always calculates something
    """
    
    entry_price = entry_zone.price
    
    # Determine invalidation
    sl_price = 0.0
    invalidation_type = ""
    
    # Try swept level
    if sweep.type != "NONE" and sweep.swept_price > 0:
        if side == "BUY" and sweep.type == "LOW_SWEEP":
            sl_price = sweep.swept_price * 0.995
            invalidation_type = "SWEEP"
        elif side == "SELL" and sweep.type == "HIGH_SWEEP":
            sl_price = sweep.swept_price * 1.005
            invalidation_type = "SWEEP"
    
    # Try order block
    if invalidation_type == "" and entry_zone.type == "ORDER_BLOCK":
        if side == "BUY":
            sl_price = entry_zone.low * 0.995
            invalidation_type = "ORDER_BLOCK"
        elif side == "SELL":
            sl_price = entry_zone.high * 1.005
            invalidation_type = "ORDER_BLOCK"
    
    # Default: 2% ATR
    if invalidation_type == "":
        atr_approx = entry_price * 0.02
        if side == "BUY":
            sl_price = entry_price - (atr_approx * 1.5)
        else:
            sl_price = entry_price + (atr_approx * 1.5)
        invalidation_type = "ATR"
    
    risk_amount = abs(entry_price - sl_price)
    distance_pct = (risk_amount / entry_price) * 100 if entry_price > 0 else 0
    
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
    """
    Step 7: Take Profit - Always calculates something
    """
    
    # Filter targets based on side
    if side == "BUY":
        potential_targets = [t for t in liquidity_map.to_liquidity 
                           if t["price"] > entry_price]
        range_boundary = htf_context.range_high
        htf_targets = [z for z in htf_context.liquidity_zones 
                      if z["price"] > entry_price and z["type"] != "RANGE_HIGH"]
    else:  # SELL
        potential_targets = [t for t in liquidity_map.to_liquidity 
                           if t["price"] < entry_price]
        range_boundary = htf_context.range_low
        htf_targets = [z for z in htf_context.liquidity_zones 
                      if z["price"] < entry_price and z["type"] != "RANGE_LOW"]
    
    # TP1: Nearest target or 2% RR
    tp1_candidates = [t for t in potential_targets if t["timeframe"] == "1h"]
    if tp1_candidates:
        tp1_candidates.sort(key=lambda t: abs(t["price"] - entry_price))
        tp1 = tp1_candidates[0]["price"]
        tp1_type = tp1_candidates[0]["type"]
    else:
        if side == "BUY":
            tp1 = entry_price * 1.02
        else:
            tp1 = entry_price * 0.98
        tp1_type = "RISK_REWARD_2%"
    
    # TP2: Range boundary
    tp2 = range_boundary
    tp2_type = "RANGE_BOUNDARY"
    
    # TP3: HTF liquidity or extended target
    if htf_targets:
        htf_targets.sort(key=lambda z: z.get("strength", 0), reverse=True)
        tp3 = htf_targets[0]["price"]
        tp3_type = htf_targets[0]["type"]
    else:
        if side == "BUY":
            range_distance = htf_context.range_high - htf_context.range_low
            tp3 = htf_context.range_high + (range_distance * 0.5)
            tp3_type = "EXTENDED"
        else:
            range_distance = htf_context.range_high - htf_context.range_low
            tp3 = htf_context.range_low - (range_distance * 0.5)
            tp3_type = "EXTENDED"
    
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
    """
    Step 8: Probability Check - Always calculates score
    """
    
    # 1. HTF Alignment (0-1)
    if htf_context.bias == side.upper() or htf_context.bias == "RANGING":
        htf_alignment = 1.0
    elif (htf_context.bias == "BULLISH" and side == "SELL") or \
         (htf_context.bias == "BEARISH" and side == "BUY"):
        htf_alignment = 0.3  # Counter-trend
    else:
        htf_alignment = 0.5
    
    # Premium/discount alignment
    if (side == "BUY" and htf_context.premium_discount == "DISCOUNT") or \
       (side == "SELL" and htf_context.premium_discount == "PREMIUM"):
        htf_alignment = min(1.0, htf_alignment + 0.2)
    
    # 2. Liquidity Quality (0-1)
    if liquidity_map.has_clear_target:
        quality_targets = sum(1 for t in liquidity_map.to_liquidity 
                            if t.get("strength", 0) >= 2)
        liquidity_quality = min(1.0, quality_targets / 3.0)
    else:
        liquidity_quality = 0.1  # Lower but not zero
    
    # 3. Sweep Strength (0-1)
    sweep_strength = sweep.strength
    if sweep.impulsive:
        sweep_strength = min(1.0, sweep_strength + 0.3)
    
    # 4. Structure Clarity (0-1)
    if structure_shift.confirmed:
        if structure_shift.type == "CHoCH":
            structure_clarity = 0.9
        elif structure_shift.type == "BOS":
            structure_clarity = 0.8
        else:
            structure_clarity = 0.6
    else:
        structure_clarity = 0.1  # Lower but not zero
    
    # 5. Entry Precision (0-1)
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
        entry_precision = 0.3  # Default entry still gets some points
    
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

# ---------------- MAIN SCANNING LOGIC ----------------
async def scan_symbol_full(exchange, symbol: str) -> Optional[Dict]:
    """
    Execute full 8-step process - NO HARD FILTERS, only score matters
    """
    
    # Get current price
    ticker = await exchange.fetch_ticker(symbol)
    current_price = ticker.get("last", 0)
    if not current_price:
        return None
    
    # --- STEP 1: HTF BIAS ---
    htf_context = await analyze_htf_bias(exchange, symbol)
    
    # --- STEP 2: LIQUIDITY MAP ---
    liquidity_map = await map_liquidity(exchange, symbol, htf_context, current_price)
    
    # --- STEP 3: LIQUIDITY SWEEP ---
    sweep = await analyze_sweep(exchange, symbol, htf_context)
    
    # Determine side based on sweep or price action
    if sweep.type == "HIGH_SWEEP":
        side = "SELL"
    elif sweep.type == "LOW_SWEEP":
        side = "BUY"
    else:
        # Default side based on HTF bias
        if htf_context.bias == "BULLISH":
            side = "BUY"
        elif htf_context.bias == "BEARISH":
            side = "SELL"
        else:
            # Random side if no bias
            import random
            side = random.choice(["BUY", "SELL"])
    
    # --- STEP 4: STRUCTURE CHECK ---
    structure_shift = await check_structure_shift(exchange, symbol, sweep, htf_context)
    
    # --- STEP 5: ENTRY ZONE ---
    entry_zone = await find_entry_zone(exchange, symbol, htf_context, sweep, structure_shift, side)
    
    # --- STEP 6: RISK/SL ---
    risk_sl = calculate_risk_sl(entry_zone, sweep, htf_context, side)
    
    # --- STEP 7: TAKE PROFIT ---
    tp_levels = calculate_take_profits(entry_zone.price, side, liquidity_map, htf_context)
    
    # --- STEP 8: PROBABILITY CHECK ---
    probability = calculate_probability(
        htf_context, liquidity_map, sweep, structure_shift, entry_zone, side
    )
    
    # ONLY CHECK: Total score threshold
    if not probability.acceptable:
        log.debug(f"  {symbol}: Score too low ({probability.total_score:.2f}/5)")
        return None
    
    log.info(f"✅ {symbol}: Setup detected! Score: {probability.total_score:.2f}/5")
    
    # --- COMPILE FINAL SETUP ---
    setup = {
        "symbol": symbol,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "side": side,
        "current_price": current_price,
        
        # Step 1
        "htf_bias": htf_context.bias,
        "htf_range_high": htf_context.range_high,
        "htf_range_low": htf_context.range_low,
        "htf_premium_discount": htf_context.premium_discount,
        "htf_liquidity_zones": htf_context.liquidity_zones,
        "htf_structure": htf_context.structure,
        
        # Step 2
        "liquidity_from": liquidity_map.from_liquidity,
        "liquidity_to": liquidity_map.to_liquidity,
        "has_clear_target": liquidity_map.has_clear_target,
        
        # Step 3
        "sweep_type": sweep.type,
        "swept_price": sweep.swept_price,
        "sweep_impulsive": sweep.impulsive,
        "sweep_strength": sweep.strength,
        
        # Step 4
        "structure_shift_type": structure_shift.type,
        "structure_shift_confirmed": structure_shift.confirmed,
        "structure_description": structure_shift.description,
        
        # Step 5
        "entry_type": entry_zone.type,
        "entry_price": entry_zone.price,
        "entry_low": entry_zone.low,
        "entry_high": entry_zone.high,
        "entry_aligns_htf": entry_zone.aligns_with_htf,
        "entry_reaction_confirmed": entry_zone.candle_reaction,
        
        # Step 6
        "sl_price": risk_sl.sl_price,
        "sl_invalidation_type": risk_sl.invalidation_type,
        "risk_amount": risk_sl.risk_amount,
        "sl_distance_pct": risk_sl.sl_to_entry_distance,
        
        # Step 7
        "tp1_price": tp_levels.tp1,
        "tp1_type": tp_levels.tp1_type,
        "tp2_price": tp_levels.tp2,
        "tp2_type": tp_levels.tp2_type,
        "tp3_price": tp_levels.tp3,
        "tp3_type": tp_levels.tp3_type,
        
        # Step 8
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

# ---------------- ALERT FORMATTING ----------------
async def send_setup_alert(setup: Dict):
    """Format and send setup alert"""
    
    # Calculate RR ratios
    entry = setup["entry_price"]
    sl = setup["sl_price"]
    tp1 = setup["tp1_price"]
    
    risk = abs(entry - sl)
    reward_tp1 = abs(tp1 - entry)
    rr_ratio = reward_tp1 / risk if risk > 0 else 0
    
    # Format message
    msg = f"""
🔥 <b>ROMEOTPT SETUP CONFIRMED</b>

<b>Symbol:</b> {setup['symbol']}
<b>Side:</b> {setup['side']}
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

✅ <b>Probability Components:</b>
HTF Alignment: {setup['probability']['htf_alignment']:.2f}
Liquidity Quality: {setup['probability']['liquidity_quality']:.2f}
Sweep Strength: {setup['probability']['sweep_strength']:.2f}
Structure Clarity: {setup['probability']['structure_clarity']:.2f}
Entry Precision: {setup['probability']['entry_precision']:.2f}

<i>Detected: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</i>
"""
    
    await send_telegram(msg)
    
    # Save to database AND create position for monitoring
    async with db_lock:
        cursor = await db_conn.execute("""
            INSERT INTO signals (
                symbol, timestamp, side,
                htf_bias, htf_range_high, htf_range_low, htf_premium_discount,
                htf_liquidity_zones_json, htf_structure_json,
                liquidity_from_json, liquidity_to_json, has_clear_target,
                sweep_type, swept_price, sweep_impulsive, sweep_strength,
                structure_shift_type, structure_shift_confirmed, structure_description,
                entry_type, entry_price, entry_low, entry_high, entry_aligns_htf, entry_reaction_confirmed,
                sl_price, sl_invalidation_type, risk_amount, sl_distance_pct,
                tp1_price, tp1_type, tp2_price, tp2_type, tp3_price, tp3_type,
                prob_htf_alignment, prob_liquidity_quality, prob_sweep_strength,
                prob_structure_clarity, prob_entry_precision, prob_total_score, prob_acceptable,
                current_price, status, notes
            ) VALUES (
                :symbol, :timestamp, :side,
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
                :current_price, 'DETECTED', ''
            )
        """, {
            "symbol": setup["symbol"],
            "timestamp": setup["timestamp"],
            "side": setup["side"],
            "htf_bias": setup["htf_bias"],
            "htf_range_high": float(setup["htf_range_high"]),
            "htf_range_low": float(setup["htf_range_low"]),
            "htf_premium_discount": setup["htf_premium_discount"],
            "htf_liquidity_zones": json.dumps(setup["htf_liquidity_zones"]),
            "htf_structure": json.dumps(setup["htf_structure"]),
            "liquidity_from": json.dumps(setup["liquidity_from"]),
            "liquidity_to": json.dumps(setup["liquidity_to"]),
            "has_clear_target": setup["has_clear_target"],
            "sweep_type": setup["sweep_type"],
            "swept_price": float(setup["swept_price"]),
            "sweep_impulsive": setup["sweep_impulsive"],
            "sweep_strength": float(setup["sweep_strength"]),
            "structure_shift_type": setup["structure_shift_type"],
            "structure_shift_confirmed": setup["structure_shift_confirmed"],
            "structure_description": setup["structure_description"],
            "entry_type": setup["entry_type"],
            "entry_price": float(setup["entry_price"]),
            "entry_low": float(setup["entry_low"]),
            "entry_high": float(setup["entry_high"]),
            "entry_aligns_htf": setup["entry_aligns_htf"],
            "entry_reaction_confirmed": setup["entry_reaction_confirmed"],
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
        
        # Get the signal ID for potential later use
        signal_id = cursor.lastrowid
        
        # CREATE POSITION FOR MONITORING (FIXED - UNCOMMENTED)
        position_id = await create_position_from_setup(setup, signal_id)
        log.info(f"Created position {position_id} for {setup['symbol']}")

# ---------------- MAIN SCANNER ----------------
async def scanner_main(exchange):
    """Main scanning loop"""
    
    await send_telegram("🚀 ROMEOTPT v2 Scanner Started - Score-Only Mode")
    await send_telegram("ℹ️ Now with position monitoring and exit alerts")
    
    cycle_count = 0
    
    while True:
        cycle_count += 1
        update_activity()
        
        try:
            log.info(f"🔄 Scan cycle #{cycle_count} starting...")
            
            # Get top volume pairs
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, v.get("quoteVolume", 0)) 
                         for s, v in tickers.items() 
                         if s.endswith("/USDT")]
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            top_pairs = usdt_pairs[:TOP_N]
            
            log.info(f"📊 Scanning {len(top_pairs)} symbols...")
            
            setups_found = 0
            symbols_scanned = 0
            
            for symbol, volume in top_pairs:
                try:
                    # Yield control to prevent freezing
                    await asyncio.sleep(0.01)
                    
                    setup = await scan_symbol_full(exchange, symbol)
                    symbols_scanned += 1
                    
                    if setup:
                        # Check deduplication
                        if await should_send_alert(setup):
                            if not await is_similar_to_recent_setup(setup):
                                await send_setup_alert(setup)
                                setups_found += 1
                            else:
                                log.debug(f"  {symbol}: Setup too similar to recent one, skipping alert")
                        else:
                            log.debug(f"  {symbol}: Setup in cooldown")
                    
                    # Small delay between symbols
                    await asyncio.sleep(0.05)
                    
                except Exception as e:
                    log.error(f"Error scanning {symbol}: {e}")
                    continue
            
            log.info(f"✅ Scan #{cycle_count} complete: Scanned {symbols_scanned}/{len(top_pairs)} symbols, found {setups_found} setups")
            
            if setups_found > 0:
                log.info(f"🎯 Found {setups_found} new setups in cycle #{cycle_count}")
            else:
                log.info(f"⏳ No new setups found in cycle #{cycle_count}")
            
        except Exception as e:
            log.exception(f"Scanner error in cycle #{cycle_count}: {e}")
            # Send debug alert
            try:
                await send_telegram(f"⚠️ Scanner error in cycle #{cycle_count}: {str(e)[:100]}...")
            except:
                pass
        
        # Calculate sleep time with debug
        log.info(f"😴 Sleeping for {SCAN_INTERVAL} seconds before next scan...")
        update_activity()
        
        # Sleep in chunks to allow watchdog to detect freezing
        sleep_remaining = SCAN_INTERVAL
        while sleep_remaining > 0:
            chunk = min(5, sleep_remaining)  # Sleep in 5-second chunks
            await asyncio.sleep(chunk)
            sleep_remaining -= chunk
            update_activity()

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/health")
async def health():
    update_activity()
    return {"status": "healthy", "scanner": "ROMEOTPT v2 Score-Only", "last_activity": last_activity_time}

@app.get("/setups")
async def get_setups(limit: int = 20, min_score: float = 1.5):
    update_activity()
    async with db_lock:
        async with db_conn.execute(
            """SELECT * FROM signals 
               WHERE prob_total_score >= ? 
               ORDER BY timestamp DESC LIMIT ?""",
            (min_score, limit)
        ) as cursor:
            columns = [description[0] for description in cursor.description]
            rows = await cursor.fetchall()
        
        setups = []
        for row in rows:
            setup = dict(zip(columns, row))
            # Parse JSON fields
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

@app.get("/positions")
async def get_positions(status: str = "ACTIVE", limit: int = 50):
    update_activity()
    async with db_lock:
        if status.upper() == "ALL":
            query = """SELECT p.*, s.timestamp as signal_time, 
                              s.entry_type, s.sweep_type, s.structure_shift_type,
                              s.prob_total_score
                       FROM positions p
                       LEFT JOIN signals s ON p.signal_id = s.id
                       ORDER BY p.entry_time DESC LIMIT ?"""
            params = (limit,)
        else:
            query = """SELECT p.*, s.timestamp as signal_time, 
                              s.entry_type, s.sweep_type, s.structure_shift_type,
                              s.prob_total_score
                       FROM positions p
                       LEFT JOIN signals s ON p.signal_id = s.id
                       WHERE p.status = ?
                       ORDER BY p.entry_time DESC LIMIT ?"""
            params = (status.upper(), limit)
        
        async with db_conn.execute(query, params) as cursor:
            columns = [description[0] for description in cursor.description]
            rows = await cursor.fetchall()
        
        positions = []
        for row in rows:
            pos = dict(zip(columns, row))
            
            # Calculate current PnL if active
            if pos['status'] == 'ACTIVE':
                # Try to get latest price from position_updates
                cursor2 = await db_conn.execute(
                    """SELECT current_price FROM position_updates 
                       WHERE position_id = ? ORDER BY timestamp DESC LIMIT 1""",
                    (pos['id'],)
                )
                latest_price = await cursor2.fetchone()
                if latest_price:
                    current_price = latest_price[0]
                    pos['current_price'] = current_price
                    pos['current_pnl'] = ((current_price - pos['entry_price']) / pos['entry_price']) * 100
            
            positions.append(pos)
        
        return {"positions": positions, "count": len(positions)}

@app.post("/positions/{position_id}/close")
async def close_position_manual(position_id: int, price: Optional[float] = None):
    update_activity()
    try:
        # Get position details
        async with db_lock:
            cursor = await db_conn.execute(
                "SELECT symbol, side, entry_price, status FROM positions WHERE id = ?",
                (position_id,)
            )
            position = await cursor.fetchone()
            
            if not position:
                return {"error": "Position not found"}
            
            symbol, side, entry_price, status = position
            
            if status != 'ACTIVE':
                return {"error": f"Position already closed (status: {status})"}
            
            # If no price provided, get current price
            if price is None:
                exchange = ccxt.okx({"enableRateLimit": True})
                ticker = await exchange.fetch_ticker(symbol)
                price = ticker.get('last', 0)
                await exchange.close()
            
            if not price or price <= 0:
                return {"error": "Invalid price"}
            
            # Close position
            success = await close_position(position_id, price, 'MANUAL')
            
            if success:
                await send_position_closure_alert(
                    symbol, side, entry_price, price, 'MANUAL',
                    datetime.datetime.utcnow().isoformat(), position_id
                )
                return {"success": True, "message": f"Position {position_id} closed at {price}"}
            else:
                return {"error": "Failed to close position"}
    
    except Exception as e:
        return {"error": str(e)}

# ---------------- CLEANUP ----------------
async def cleanup_old_data():
    """Clean up old data to prevent database bloat"""
    while True:
        try:
            update_activity()
            
            # Clean old signals (older than 30 days)
            cutoff_30d = (datetime.datetime.utcnow() - datetime.timedelta(days=30)).isoformat()
            async with db_lock:
                await db_conn.execute(
                    "DELETE FROM signals WHERE timestamp < ?",
                    (cutoff_30d,)
                )
                
                # Clean old positions
                await db_conn.execute(
                    """DELETE FROM positions 
                       WHERE status != 'ACTIVE' AND entry_time < ?""",
                    (cutoff_30d,)
                )
                
                # Clean old position updates
                cutoff_7d = (datetime.datetime.utcnow() - datetime.timedelta(days=7)).isoformat()
                await db_conn.execute(
                    """DELETE FROM position_updates 
                       WHERE timestamp < ?""",
                    (cutoff_7d,)
                )
                
                # Clean old cooldown entries (older than 7 days)
                await db_conn.execute(
                    "DELETE FROM signal_cooldown WHERE last_alert_time < ?",
                    (cutoff_7d,)
                )
                
                await db_conn.commit()
            
            log.info("🧹 Cleaned up old database entries")
            
        except Exception as e:
            log.error(f"Cleanup error: {e}")
        
        # Run cleanup once per day
        await asyncio.sleep(24 * 60 * 60)

# ---------------- MAIN ----------------
async def main():
    global db_conn
    
    log.info("🚀 Starting ROMEOTPT v2 Scanner with watchdog...")
    
    # Initialize
    await init_db()
    
    # Start watchdog
    watchdog_task = asyncio.create_task(watchdog())
    
    # Start cleanup task
    cleanup_task = asyncio.create_task(cleanup_old_data())
    
    # Start position monitor
    monitor_task = asyncio.create_task(monitor_positions_lightweight())
    
    # Create exchange
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"}
    })
    
    # Start scanner
    try:
        await scanner_main(exchange)
    except KeyboardInterrupt:
        log.info("Shutting down ROMEOTPT v2 scanner...")
    except Exception as e:
        log.error(f"Fatal error in scanner: {e}")
        await send_telegram(f"🚨 ROMEOTPT Scanner crashed: {str(e)}")
    finally:
        # Cancel all tasks
        watchdog_task.cancel()
        cleanup_task.cancel()
        monitor_task.cancel()
        
        if db_conn:
            await db_conn.close()
        await exchange.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", help="Run HTTP server")
    args = parser.parse_args()
    
    if args.http:
        # Start scanner in background
        import threading
        def run_scanner():
            asyncio.run(main())
        
        scanner_thread = threading.Thread(target=run_scanner, daemon=True)
        scanner_thread.start()
        
        # Run HTTP server
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Shutting down ROMEOTPT v2 scanner...")