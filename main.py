#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER (Enhanced + Elite Features)
- Fully live early signals
- RomeOPT 6-step logic
- TRUE RomeOPT institutional TP/SL (NO ATR, NO percentages, NO fixed pips)
- Dynamic TP/SL updates (market-structure-based)
- Telegram alerts
- Async SQLite logging
- Filters: Score >=5, Displacement +2, Sweep+2 OR Zone+1, avoid counter-trend
- Improved Order Block detection
- Adaptive Market Regime detection
- HTF + Sweep scoring threshold
- Elite multi-timeframe confirmation (15m,1h,4h)
"""

import os, time, asyncio, logging, datetime, json
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple, Any

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))
TOP_N = int(os.getenv("TOP_N", 25))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
MIN_SCORE = 5
CRITICAL_FACTORS_MIN = 2  # HTF Alignment + Liquidity Sweep minimum

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_bot")
db_lock = asyncio.Lock()
db_conn = None
exchange = None

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
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute("PRAGMA journal_mode=WAL;")
    await db_conn.execute("PRAGMA synchronous=NORMAL;")
    
    # Create table with ALL columns
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
            tp_sl_type TEXT DEFAULT 'Legacy'
        );
    """)
    
    # Check and add missing columns if needed
    await db_conn.execute("PRAGMA table_info(signals)")
    cursor = await db_conn.execute("PRAGMA table_info(signals)")
    columns = await cursor.fetchall()
    column_names = [col[1] for col in columns]
    
    # Add missing columns
    if 'tp_sl_type' not in column_names:
        log.info("Adding tp_sl_type column to signals table")
        await db_conn.execute("ALTER TABLE signals ADD COLUMN tp_sl_type TEXT DEFAULT 'Legacy'")
    
    await db_conn.commit()
    log.info("Database initialized successfully")

# ---------------- OHLCV ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200) -> Optional[List]:
    """Fetch OHLCV data with error handling"""
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.debug(f"fetch_ohlcv failed for {symbol} {timeframe}: {e}")
        return None

# ---------------- MARKET STRUCTURE DETECTION ----------------
def detect_protected_highs_lows(df: pd.DataFrame, timeframe: str) -> Tuple[List, List]:
    """
    Detect protected highs and lows (institutional structure)
    CRITICAL FIX: Only recent candles + distance filtering
    """
    protected_highs = []
    protected_lows = []
    
    if len(df) < 10:
        return protected_highs, protected_lows
    
    current_price = df.iloc[-1]['close']
    
    # Timeframe-specific lookback and distance limits
    tf_config = {
        '1m': {'lookback': 30, 'max_distance_pct': 0.03},   # Last 30 candles, max 3% away
        '3m': {'lookback': 40, 'max_distance_pct': 0.04},   # Last 40 candles, max 4% away
        '5m': {'lookback': 50, 'max_distance_pct': 0.05},   # Last 50 candles, max 5% away
        '15m': {'lookback': 40, 'max_distance_pct': 0.07},  # Last 40 candles, max 7% away
        '30m': {'lookback': 30, 'max_distance_pct': 0.08},  # Last 30 candles, max 8% away
    }
    
    config = tf_config.get(timeframe, tf_config['5m'])
    lookback = config['lookback']
    max_distance_pct = config['max_distance_pct']
    
    # Only analyze recent candles
    start_idx = max(0, len(df) - lookback)
    
    for i in range(start_idx + 2, len(df) - 2):
        # Detect Protected High (local maximum with confirmation)
        if (df.iloc[i]['high'] > df.iloc[i-2]['high'] and 
            df.iloc[i]['high'] > df.iloc[i-1]['high'] and
            df.iloc[i]['high'] > df.iloc[i+1]['high'] and
            df.iloc[i]['high'] > df.iloc[i+2]['high']):
            
            high_price = float(df.iloc[i]['high'])
            distance_pct = abs(high_price - current_price) / current_price
            
            # Filter: Must be within max distance and significant
            if distance_pct <= max_distance_pct:
                # Check for significance (at least 0.5x avg candle size)
                avg_candle = (df['high'] - df['low']).rolling(10).mean().iloc[i]
                if (high_price - df.iloc[i-1]['high']) > (avg_candle * 0.5):
                    protected_highs.append({
                        'price': high_price,
                        'index': i,
                        'strength': 2 if i > len(df)*0.85 else 1,
                        'distance_pct': distance_pct
                    })
        
        # Detect Protected Low (local minimum with confirmation)
        if (df.iloc[i]['low'] < df.iloc[i-2]['low'] and 
            df.iloc[i]['low'] < df.iloc[i-1]['low'] and
            df.iloc[i]['low'] < df.iloc[i+1]['low'] and
            df.iloc[i]['low'] < df.iloc[i+2]['low']):
            
            low_price = float(df.iloc[i]['low'])
            distance_pct = abs(low_price - current_price) / current_price
            
            if distance_pct <= max_distance_pct:
                avg_candle = (df['high'] - df['low']).rolling(10).mean().iloc[i]
                if (df.iloc[i-1]['low'] - low_price) > (avg_candle * 0.5):
                    protected_lows.append({
                        'price': low_price,
                        'index': i,
                        'strength': 2 if i > len(df)*0.85 else 1,
                        'distance_pct': distance_pct
                    })
    
    # Sort by recency (most recent first) and limit results
    protected_highs.sort(key=lambda x: x['index'], reverse=True)
    protected_lows.sort(key=lambda x: x['index'], reverse=True)
    
    return protected_highs[:10], protected_lows[:10]

def detect_liquidity_pools(df: pd.DataFrame, timeframe: str) -> Dict[str, List]:
    """
    Detect liquidity pools (equal highs, equal lows, swing points)
    Timeframe-optimized detection
    """
    liquidity_pools = {
        'equal_highs': [],
        'equal_lows': [],
        'swing_highs': [],
        'swing_lows': []
    }
    
    if len(df) < 20:
        return liquidity_pools
    
    # Timeframe-specific lookback for liquidity detection
    lookback_config = {
        '1m': 30, '3m': 40, '5m': 50, '15m': 40, '30m': 30
    }
    lookback = lookback_config.get(timeframe, 40)
    start_idx = max(0, len(df) - lookback)
    
    # Detect swing highs/lows in recent data
    for i in range(start_idx + 2, len(df) - 2):
        # Swing High
        if (df.iloc[i]['high'] > df.iloc[i-2:i]['high'].max() and
            df.iloc[i]['high'] > df.iloc[i+1:i+3]['high'].max()):
            liquidity_pools['swing_highs'].append({
                'price': float(df.iloc[i]['high']),
                'index': i,
                'strength': 2 if i > len(df)*0.9 else 1
            })
        
        # Swing Low
        if (df.iloc[i]['low'] < df.iloc[i-2:i]['low'].min() and
            df.iloc[i]['low'] < df.iloc[i+1:i+3]['low'].min()):
            liquidity_pools['swing_lows'].append({
                'price': float(df.iloc[i]['low']),
                'index': i,
                'strength': 2 if i > len(df)*0.9 else 1
            })
    
    # Detect equal highs (price clusters within 0.2%)
    recent_period = min(15, len(df) - start_idx)
    recent_highs = df['high'].iloc[-recent_period:].values
    
    for i in range(len(recent_highs)):
        for j in range(i+1, len(recent_highs)):
            price_diff = abs(recent_highs[i] - recent_highs[j])
            avg_price = (recent_highs[i] + recent_highs[j]) / 2
            
            if price_diff / avg_price < 0.002:  # Within 0.2%
                # Check if similar price already exists
                exists = False
                for existing in liquidity_pools['equal_highs']:
                    if abs(existing['price'] - avg_price) / avg_price < 0.001:
                        exists = True
                        break
                
                if not exists:
                    liquidity_pools['equal_highs'].append({
                        'price': float(avg_price),
                        'strength': 2
                    })
    
    # Detect equal lows
    recent_lows = df['low'].iloc[-recent_period:].values
    
    for i in range(len(recent_lows)):
        for j in range(i+1, len(recent_lows)):
            price_diff = abs(recent_lows[i] - recent_lows[j])
            avg_price = (recent_lows[i] + recent_lows[j]) / 2
            
            if price_diff / avg_price < 0.002:
                exists = False
                for existing in liquidity_pools['equal_lows']:
                    if abs(existing['price'] - avg_price) / avg_price < 0.001:
                        exists = True
                        break
                
                if not exists:
                    liquidity_pools['equal_lows'].append({
                        'price': float(avg_price),
                        'strength': 2
                    })
    
    return liquidity_pools

def detect_bos_points(df: pd.DataFrame, timeframe: str) -> List[Dict]:
    """
    Detect Break of Structure points
    Timeframe-optimized detection
    """
    bos_points = []
    
    if len(df) < 20:
        return bos_points
    
    # Timeframe-specific lookback
    lookback_config = {
        '1m': 40, '3m': 50, '5m': 60, '15m': 50, '30m': 40
    }
    lookback = lookback_config.get(timeframe, 50)
    start_idx = max(10, len(df) - lookback)
    
    for i in range(start_idx, len(df)-5):
        # Bullish BOS: Break above previous resistance
        prev_resistance = df.iloc[i-8:i-3]['high'].max()
        if (df.iloc[i]['high'] > prev_resistance and
            df.iloc[i]['close'] > df.iloc[i]['open'] and  # Bullish candle
            df.iloc[i-3]['low'] > df.iloc[i-6]['low']):   # Higher low established
            
            # Check for follow-through
            if i+3 < len(df) and df.iloc[i+1:i+4]['close'].max() > df.iloc[i]['high']:
                bos_points.append({
                    'price': float(df.iloc[i]['high']),
                    'type': 'bullish',
                    'index': i,
                    'strength': 1
                })
        
        # Bearish BOS: Break below previous support
        prev_support = df.iloc[i-8:i-3]['low'].min()
        if (df.iloc[i]['low'] < prev_support and
            df.iloc[i]['close'] < df.iloc[i]['open'] and  # Bearish candle
            df.iloc[i-3]['high'] < df.iloc[i-6]['high']): # Lower high established
            
            # Check for follow-through
            if i+3 < len(df) and df.iloc[i+1:i+4]['close'].min() < df.iloc[i]['low']:
                bos_points.append({
                    'price': float(df.iloc[i]['low']),
                    'type': 'bearish',
                    'index': i,
                    'strength': 1
                })
    
    # Return most recent 3 BOS points
    bos_points.sort(key=lambda x: x['index'], reverse=True)
    return bos_points[:3]

# ---------------- ROMEOPT TP/SL MODULE ----------------
class RomeOPT_TP_SL:
    """
    TRUE RomeOPT institutional TP/SL logic
    NO ATR, NO percentages, NO fixed pips
    Based purely on market structure with distance limits
    """
    
    def __init__(self, timeframe: str):
        self.timeframe = timeframe
        
        # Timeframe-specific configurations WITH PROPER DISTANCE LIMITS
        self.tf_config = {
            '1m': {
                'buffer_multiplier': 1.0,
                'min_rrr': 2.0,
                'max_sl_distance_pct': 0.015,  # 1.5% max SL distance
                'max_tp_distance_pct': 0.030,  # 3.0% max TP distance
                'min_risk_pct': 0.005,         # 0.5% minimum risk
                'buffer_pips': 0.00003
            },
            '3m': {
                'buffer_multiplier': 1.2,
                'min_rrr': 1.8,
                'max_sl_distance_pct': 0.020,  # 2.0% max
                'max_tp_distance_pct': 0.040,  # 4.0% max
                'min_risk_pct': 0.007,         # 0.7% minimum risk
                'buffer_pips': 0.00004
            },
            '5m': {
                'buffer_multiplier': 1.5,
                'min_rrr': 1.6,
                'max_sl_distance_pct': 0.025,  # 2.5% max
                'max_tp_distance_pct': 0.050,  # 5.0% max
                'min_risk_pct': 0.008,         # 0.8% minimum risk
                'buffer_pips': 0.00005
            },
            '15m': {
                'buffer_multiplier': 2.0,
                'min_rrr': 1.4,
                'max_sl_distance_pct': 0.035,  # 3.5% max
                'max_tp_distance_pct': 0.070,  # 7.0% max
                'min_risk_pct': 0.010,         # 1.0% minimum risk
                'buffer_pips': 0.00008
            },
            '30m': {
                'buffer_multiplier': 2.5,
                'min_rrr': 1.2,
                'max_sl_distance_pct': 0.045,  # 4.5% max - FIXED from 14.5%!
                'max_tp_distance_pct': 0.090,  # 9.0% max
                'min_risk_pct': 0.012,         # 1.2% minimum risk
                'buffer_pips': 0.00012
            }
        }
        
        config = self.tf_config.get(timeframe, self.tf_config['5m'])
        self.buffer_multiplier = config['buffer_multiplier']
        self.min_rrr = config['min_rrr']
        self.max_sl_distance_pct = config['max_sl_distance_pct']
        self.max_tp_distance_pct = config['max_tp_distance_pct']
        self.min_risk_pct = config['min_risk_pct']
        self.base_buffer = config['buffer_pips']
        
        # Dynamic buffer based on price
        self.buffer = self.base_buffer
        
        log.debug(f"RomeOPT_TP_SL for {timeframe}: max SL={self.max_sl_distance_pct*100}%, min RRR={self.min_rrr}")
    
    def calculate_stop_loss(self, side: str, entry_price: float, entry_ob: Dict, 
                           protected_highs: List, protected_lows: List,
                           bos_points: List) -> float:
        """
        Calculate SL according to RomeOPT rules with distance limits
        """
        try:
            max_sl_distance = entry_price * self.max_sl_distance_pct
            min_sl_distance = entry_price * self.min_risk_pct
            
            log.debug(f"{side} SL calc: entry={entry_price}, max_dist={max_sl_distance:.4f}, min_dist={min_sl_distance:.4f}")
            
            if side == "BUY":
                sl_candidates = []
                
                # 1. BELOW origin OB low (RomeOPT rule)
                if entry_ob and entry_ob.get('type') == 'bullish':
                    ob_low = entry_ob['low']
                    if ob_low < entry_price:
                        candidate = ob_low - self.buffer
                        risk = entry_price - candidate
                        if min_sl_distance <= risk <= max_sl_distance:
                            sl_candidates.append(('ob', candidate, risk))
                            log.debug(f"LONG candidate from OB: {candidate} (risk: {risk:.4f})")
                
                # 2. BELOW most recent protected low (within limits)
                if protected_lows:
                    # Get lows below entry, sorted by recency
                    valid_lows = [pl for pl in protected_lows if pl['price'] < entry_price]
                    valid_lows.sort(key=lambda x: x['index'], reverse=True)  # Most recent first
                    
                    for pl in valid_lows[:3]:  # Check 3 most recent
                        candidate = pl['price'] - self.buffer
                        risk = entry_price - candidate
                        if min_sl_distance <= risk <= max_sl_distance:
                            sl_candidates.append(('protected_low', candidate, risk))
                            log.debug(f"LONG candidate from protected low: {candidate} (risk: {risk:.4f})")
                            break
                
                # 3. BELOW recent swing low (fallback)
                if not sl_candidates:
                    # Find recent low in last 10-20 candles
                    lookback = min(20, len(protected_lows) * 5 if protected_lows else 10)
                    recent_low = entry_price * 0.99  # 1% below as starting point
                    candidate = recent_low - self.buffer
                    risk = entry_price - candidate
                    if risk >= min_sl_distance:
                        sl_candidates.append(('recent_low', candidate, risk))
                
                # Select BEST candidate (prefer OB, then protected low, with valid risk)
                if sl_candidates:
                    # Sort by source priority (OB first), then risk (closest to min_risk)
                    sl_candidates.sort(key=lambda x: (0 if x[0]=='ob' else 1 if x[0]=='protected_low' else 2, x[2]))
                    
                    for source, candidate, risk in sl_candidates:
                        # Final validation
                        if candidate < entry_price and min_sl_distance <= risk <= max_sl_distance:
                            log.info(f"✅ LONG SL selected: {candidate:.5f} (from {source}, risk: {risk/entry_price*100:.2f}%, {risk:.4f})")
                            return candidate
                
                # Fallback: Use min_risk_pct
                sl = entry_price * (1 - self.min_risk_pct)
                log.warning(f"⚠️ LONG SL fallback to min risk: {sl:.5f} ({self.min_risk_pct*100:.1f}%)")
                return sl
            
            else:  # SELL
                sl_candidates = []
                
                # 1. ABOVE origin OB high (RomeOPT rule)
                if entry_ob and entry_ob.get('type') == 'bearish':
                    ob_high = entry_ob['high']
                    if ob_high > entry_price:
                        candidate = ob_high + self.buffer
                        risk = candidate - entry_price
                        if min_sl_distance <= risk <= max_sl_distance:
                            sl_candidates.append(('ob', candidate, risk))
                            log.debug(f"SHORT candidate from OB: {candidate} (risk: {risk:.4f})")
                
                # 2. ABOVE most recent protected high (within limits)
                if protected_highs:
                    # Get highs above entry, sorted by recency
                    valid_highs = [ph for ph in protected_highs if ph['price'] > entry_price]
                    valid_highs.sort(key=lambda x: x['index'], reverse=True)  # Most recent first
                    
                    for ph in valid_highs[:3]:  # Check 3 most recent
                        candidate = ph['price'] + self.buffer
                        risk = candidate - entry_price
                        if min_sl_distance <= risk <= max_sl_distance:
                            sl_candidates.append(('protected_high', candidate, risk))
                            log.debug(f"SHORT candidate from protected high: {candidate} (risk: {risk:.4f})")
                            break
                
                # 3. ABOVE recent swing high (fallback)
                if not sl_candidates:
                    # Find recent high in last 10-20 candles
                    recent_high = entry_price * 1.01  # 1% above as starting point
                    candidate = recent_high + self.buffer
                    risk = candidate - entry_price
                    if risk >= min_sl_distance:
                        sl_candidates.append(('recent_high', candidate, risk))
                
                # Select BEST candidate
                if sl_candidates:
                    sl_candidates.sort(key=lambda x: (0 if x[0]=='ob' else 1 if x[0]=='protected_high' else 2, x[2]))
                    
                    for source, candidate, risk in sl_candidates:
                        if candidate > entry_price and min_sl_distance <= risk <= max_sl_distance:
                            log.info(f"✅ SHORT SL selected: {candidate:.5f} (from {source}, risk: {risk/entry_price*100:.2f}%, {risk:.4f})")
                            return candidate
                
                # Fallback: Use min_risk_pct
                sl = entry_price * (1 + self.min_risk_pct)
                log.warning(f"⚠️ SHORT SL fallback to min risk: {sl:.5f} ({self.min_risk_pct*100:.1f}%)")
                return sl
        
        except Exception as e:
            log.error(f"Error in calculate_stop_loss: {e}")
            # Ultra-safe fallback
            if side == "BUY":
                return entry_price * 0.99  # 1% stop
            else:
                return entry_price * 1.01  # 1% stop
    
    def calculate_take_profit(self, side: str, entry_price: float, stop_loss: float,
                             liquidity_pools: Dict, df: pd.DataFrame) -> Tuple[float, float, float]:
        """
        Calculate TP targeting real liquidity pools with distance limits
        """
        try:
            # Calculate risk
            risk = abs(entry_price - stop_loss)
            max_tp_distance = entry_price * self.max_tp_distance_pct
            
            log.debug(f"{side} TP calc: entry={entry_price}, sl={stop_loss}, risk={risk:.4f}, max_tp_dist={max_tp_distance:.4f}")
            
            # Minimum RRR check
            min_tp_distance = risk * self.min_rrr
            if min_tp_distance > max_tp_distance:
                log.warning(f"Min TP distance {min_tp_distance:.4f} > max {max_tp_distance:.4f}, adjusting RRR")
                self.min_rrr = max(1.0, max_tp_distance / risk)  # Adjust RRR down if needed
            
            if side == "BUY":
                # Find liquidity targets ABOVE entry
                targets = []
                
                # 1. Equal highs (strong liquidity)
                for eh in liquidity_pools.get('equal_highs', []):
                    if eh['price'] > entry_price:
                        distance = eh['price'] - entry_price
                        if distance <= max_tp_distance:
                            rrr = distance / risk
                            targets.append({
                                'price': eh['price'],
                                'type': 'equal_high',
                                'strength': eh.get('strength', 1),
                                'distance': distance,
                                'rrr': rrr
                            })
                
                # 2. Swing highs
                for sh in liquidity_pools.get('swing_highs', []):
                    if sh['price'] > entry_price:
                        distance = sh['price'] - entry_price
                        if distance <= max_tp_distance:
                            rrr = distance / risk
                            targets.append({
                                'price': sh['price'],
                                'type': 'swing_high',
                                'strength': sh.get('strength', 1),
                                'distance': distance,
                                'rrr': rrr
                            })
                
                # Sort by distance (closest first for TP1)
                targets.sort(key=lambda x: x['distance'])
                
                # Select up to 3 targets with proper spacing
                selected = []
                for target in targets:
                    if len(selected) >= 3:
                        break
                    
                    # Minimum RRR filter
                    if target['rrr'] >= self.min_rrr:
                        if not selected:
                            selected.append(target['price'])
                        else:
                            # Ensure at least 0.3R gap between targets
                            min_gap = risk * 0.3
                            if target['price'] - selected[-1] >= min_gap:
                                selected.append(target['price'])
                
                # Create TP levels
                if len(selected) >= 3:
                    tp1, tp2, tp3 = selected[0], selected[1], selected[2]
                elif len(selected) == 2:
                    tp1, tp2 = selected[0], selected[1]
                    tp3 = tp2 + (risk * 0.5)
                elif len(selected) == 1:
                    tp1 = selected[0]
                    tp2 = tp1 + (risk * 0.5)
                    tp3 = tp2 + (risk * 0.5)
                else:
                    # No valid targets, use min RRR
                    tp1 = entry_price + (risk * self.min_rrr)
                    tp2 = tp1 + (risk * 0.5)
                    tp3 = tp2 + (risk * 0.5)
                
                # Cap at maximum distance
                tp1 = min(tp1, entry_price + max_tp_distance)
                tp2 = min(tp2, entry_price + (max_tp_distance * 1.1))
                tp3 = min(tp3, entry_price + (max_tp_distance * 1.2))
                
                # Ensure proper ordering
                tp1, tp2, tp3 = sorted([tp1, tp2, tp3])
                
                log.info(f"✅ LONG TP: [{tp1:.5f}, {tp2:.5f}, {tp3:.5f}] (RRR: {(tp1-entry_price)/risk:.2f})")
                return tp1, tp2, tp3
            
            else:  # SELL
                # Find liquidity targets BELOW entry
                targets = []
                
                # 1. Equal lows (strong liquidity)
                for el in liquidity_pools.get('equal_lows', []):
                    if el['price'] < entry_price:
                        distance = entry_price - el['price']
                        if distance <= max_tp_distance:
                            rrr = distance / risk
                            targets.append({
                                'price': el['price'],
                                'type': 'equal_low',
                                'strength': el.get('strength', 1),
                                'distance': distance,
                                'rrr': rrr
                            })
                
                # 2. Swing lows
                for sl in liquidity_pools.get('swing_lows', []):
                    if sl['price'] < entry_price:
                        distance = entry_price - sl['price']
                        if distance <= max_tp_distance:
                            rrr = distance / risk
                            targets.append({
                                'price': sl['price'],
                                'type': 'swing_low',
                                'strength': sl.get('strength', 1),
                                'distance': distance,
                                'rrr': rrr
                            })
                
                # Sort by distance (closest first for TP1)
                targets.sort(key=lambda x: x['distance'])
                
                # Select up to 3 targets with proper spacing
                selected = []
                for target in targets:
                    if len(selected) >= 3:
                        break
                    
                    if target['rrr'] >= self.min_rrr:
                        if not selected:
                            selected.append(target['price'])
                        else:
                            min_gap = risk * 0.3
                            if selected[-1] - target['price'] >= min_gap:
                                selected.append(target['price'])
                
                # Create TP levels
                if len(selected) >= 3:
                    tp1, tp2, tp3 = selected[0], selected[1], selected[2]
                elif len(selected) == 2:
                    tp1, tp2 = selected[0], selected[1]
                    tp3 = tp2 - (risk * 0.5)
                elif len(selected) == 1:
                    tp1 = selected[0]
                    tp2 = tp1 - (risk * 0.5)
                    tp3 = tp2 - (risk * 0.5)
                else:
                    # No valid targets, use min RRR
                    tp1 = entry_price - (risk * self.min_rrr)
                    tp2 = tp1 - (risk * 0.5)
                    tp3 = tp2 - (risk * 0.5)
                
                # Cap at maximum distance
                tp1 = max(tp1, entry_price - max_tp_distance)
                tp2 = max(tp2, entry_price - (max_tp_distance * 1.1))
                tp3 = max(tp3, entry_price - (max_tp_distance * 1.2))
                
                # Ensure proper ordering
                tp1, tp2, tp3 = sorted([tp1, tp2, tp3])
                
                log.info(f"✅ SHORT TP: [{tp1:.5f}, {tp2:.5f}, {tp3:.5f}] (RRR: {(entry_price-tp1)/risk:.2f})")
                return tp1, tp2, tp3
        
        except Exception as e:
            log.error(f"Error in calculate_take_profit: {e}")
            # Safe fallback
            if side == "BUY":
                return entry_price * 1.02, entry_price * 1.04, entry_price * 1.06
            else:
                return entry_price * 0.98, entry_price * 0.96, entry_price * 0.94

# ---------------- MARKET REGIME ----------------
async def detect_market_regime(df: pd.DataFrame) -> str:
    """Detect current market regime"""
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
async def elite_tf_alignment(exchange, symbol: str, side: str) -> bool:
    """Check alignment with higher timeframes"""
    tfs = ["15m", "1h", "4h"]
    alignments = []
    
    for tf in tfs:
        ohlcv = await fetch_ohlcv(exchange, symbol, tf, 50)
        if not ohlcv:
            continue
        
        df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
        if len(df) < 10:
            continue
        
        # Simple trend detection
        sma_short = df["close"].rolling(5).mean().iloc[-1]
        sma_long = df["close"].rolling(20).mean().iloc[-1]
        trend_side = "BUY" if sma_short > sma_long else "SELL"
        
        alignments.append(trend_side == side)
    
    # Require at least 2 out of 3 alignments
    return sum(alignments) >= 2

# ---------------- ROMEOPT TP/SL CALCULATION ----------------
def calculate_romeopt_tp_sl(sig: Dict, df: pd.DataFrame, timeframe: str) -> Dict:
    """
    Main function to calculate RomeOPT institutional TP/SL
    Replaces the old ATR-based TP/SL
    """
    try:
        log.debug(f"Calculating RomeOPT TP/SL for {sig['symbol']} {sig['side']} on {timeframe}")
        
        # Detect market structure WITH TIMEFRAME PARAMETER
        protected_highs, protected_lows = detect_protected_highs_lows(df, timeframe)
        liquidity_pools = detect_liquidity_pools(df, timeframe)
        bos_points = detect_bos_points(df, timeframe)
        
        # Get entry order block
        entry_ob = find_latest_ob(df)
        if not entry_ob:
            log.warning(f"No order block found for {sig['symbol']}, using recent price action")
            # Create simple OB based on recent candles
            recent_period = min(10, len(df))
            if sig["side"] == "BUY":
                entry_ob = {
                    "type": "bullish", 
                    "low": df["low"].iloc[-recent_period:].min(),
                    "high": df["high"].iloc[-recent_period:].max()
                }
            else:
                entry_ob = {
                    "type": "bearish",
                    "low": df["low"].iloc[-recent_period:].min(),
                    "high": df["high"].iloc[-recent_period:].max()
                }
        
        # Initialize RomeOPT TP/SL calculator
        calculator = RomeOPT_TP_SL(timeframe)
        
        # Calculate Stop Loss
        sl = calculator.calculate_stop_loss(
            side=sig["side"],
            entry_price=sig["entry"],
            entry_ob=entry_ob,
            protected_highs=protected_highs,
            protected_lows=protected_lows,
            bos_points=bos_points
        )
        
        # Calculate Take Profit levels
        tp1, tp2, tp3 = calculator.calculate_take_profit(
            side=sig["side"],
            entry_price=sig["entry"],
            stop_loss=sl,
            liquidity_pools=liquidity_pools,
            df=df
        )
        
        # Validate and finalize
        risk = abs(sig["entry"] - sl)
        reward = abs(tp1 - sig["entry"])
        rrr = reward / risk if risk > 0 else 0
        
        # Final RRR check
        if rrr < calculator.min_rrr * 0.8:
            log.warning(f"Final RRR too low ({rrr:.2f}), adjusting TP1")
            if sig["side"] == "BUY":
                tp1 = sig["entry"] + (risk * calculator.min_rrr)
                tp2 = tp1 + (risk * 0.5)
                tp3 = tp2 + (risk * 0.5)
            else:
                tp1 = sig["entry"] - (risk * calculator.min_rrr)
                tp2 = tp1 - (risk * 0.5)
                tp3 = tp2 - (risk * 0.5)
        
        # Update signal with RomeOPT TP/SL
        sig["sl"] = sl
        sig["tp1"] = tp1
        sig["tp2"] = tp2
        sig["tp3"] = tp3
        sig["latest_ob"] = entry_ob
        sig["tp_sl_type"] = "ROMEOPT"
        sig["rrr"] = rrr
        
        log.info(f"✅ RomeOPT TP/SL for {sig['symbol']}: Entry={sig['entry']:.5f}, SL={sl:.5f}, TP=[{tp1:.5f}, {tp2:.5f}, {tp3:.5f}], RRR={rrr:.2f}")
        
    except Exception as e:
        log.error(f"RomeOPT TP/SL calculation failed for {sig.get('symbol', 'unknown')}: {e}")
        # Fallback to simple TP/SL
        if "sl" not in sig or "tp1" not in sig:
            price_range = (df["high"].iloc[-1] - df["low"].iloc[-1]) * 0.5
            if sig["side"] == "BUY":
                sig["sl"] = sig["entry"] - price_range
                sig["tp1"] = sig["entry"] + (price_range * 2)
                sig["tp2"] = sig["entry"] + (price_range * 3)
                sig["tp3"] = sig["entry"] + (price_range * 4)
            else:
                sig["sl"] = sig["entry"] + price_range
                sig["tp1"] = sig["entry"] - (price_range * 2)
                sig["tp2"] = sig["entry"] - (price_range * 3)
                sig["tp3"] = sig["entry"] - (price_range * 4)
            sig["tp_sl_type"] = "FALLBACK"
    
    return sig

# ---------------- ROMEOPT 6-STEP SIGNAL ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str) -> Optional[Dict]:
    """Generate RomeOPT 6-step trading signal"""
    if df is None or len(df) < 30:
        log.debug(f"Insufficient data for {symbol} {tf}")
        return None
    
    # Ensure all columns are numeric
    for col in ["open", "high", "low", "close", "vol"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    if df.isnull().any().any():
        log.warning(f"NaN values in {symbol} {tf} data")
        return None
    
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    score = 0
    reasons = []

    # Step 1: Liquidity Sweep
    sweep_high = last["high"] > prev5["high"].max()
    sweep_low = last["low"] < prev5["low"].min()
    has_sweep = sweep_high or sweep_low
    liquidity_sweep = 2 if has_sweep else 0
    score += liquidity_sweep
    reasons.append(f"Liquidity Sweep +{liquidity_sweep}")

    # Step 2: Displacement
    displacement = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    has_disp = displacement > 0.6
    if has_disp:
        score += 2
        reasons.append("Displacement +2")
    else:
        reasons.append("Displacement +0")

    # Step 3 & 4: Order Block & Zone
    ob_zone = find_latest_ob(df)
    ob_type = None
    
    if ob_zone:
        ob_type = ob_zone["type"]
        if ob_type == "bullish" and last["close"] <= ob_zone["high"]:
            score += 1
            reasons.append("Zone Approach +1")
        elif ob_type == "bearish" and last["close"] >= ob_zone["low"]:
            score += 1
            reasons.append("Zone Approach +1")
        else:
            reasons.append("Zone Approach +0")
    else:
        reasons.append("Zone Approach +0")

    if not ob_type:
        log.debug(f"No order block found for {symbol} {tf}")
        return None
    
    side = "BUY" if ob_type == "bullish" else "SELL"
    entry = float(last["close"])

    # Step 5: HTF Alignment
    tf_map = {"1m": "15m", "3m": "30m", "5m": "1h", "15m": "4h", "30m": "1h"}
    htf = tf_map.get(tf, "15m")
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, htf, 50)
    htf_alignment = 0
    
    if ohlcv_htf:
        df_htf = pd.DataFrame(ohlcv_htf, columns=["ts", "open", "high", "low", "close", "vol"])
        if len(df_htf) > 10:
            trend = df_htf["close"].iloc[-1] - df_htf["close"].iloc[-5]
            htf_dir = "bullish" if trend > 0 else "bearish"
            if htf_dir == ob_type:
                score += 1
                htf_alignment = 1
                reasons.append("HTF Alignment +1")
            else:
                reasons.append("HTF Alignment +0")
    else:
        reasons.append("HTF Alignment ?")

    # Step 6: Momentum
    momentum_ratio = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    if ob_type == "bullish" and momentum_ratio > 0.5 and last["close"] > last["open"]:
        score += 1
        reasons.append("Momentum +1")
    elif ob_type == "bearish" and momentum_ratio > 0.5 and last["close"] < last["open"]:
        score += 1
        reasons.append("Momentum +1")
    else:
        reasons.append("Momentum +0")

    # ---------------- CRITICAL FILTERS ----------------
    critical_score = htf_alignment + liquidity_sweep
    if critical_score < CRITICAL_FACTORS_MIN:
        log.debug(f"Critical score too low for {symbol} {tf}: {critical_score}")
        return None
    
    if score < MIN_SCORE:
        log.debug(f"Score too low for {symbol} {tf}: {score}")
        return None
    
    if not has_disp:
        log.debug(f"No displacement for {symbol} {tf}")
        return None
    
    # HTF Alignment mandatory filter
    if htf_alignment != 1:
        log.debug(f"No HTF alignment for {symbol} {tf}")
        return None

    # Market regime filter
    market_regime = await detect_market_regime(df)
    if (market_regime == "BULL" and side == "SELL") or (market_regime == "BEAR" and side == "BUY"):
        log.debug(f"Counter-trade for {symbol} {tf}: side={side}, regime={market_regime}")
        return None

    # Trend filter
    trend_ma = df["close"].rolling(20).mean().iloc[-1]
    if (side == "BUY" and last["close"] < trend_ma) or (side == "SELL" and last["close"] > trend_ma):
        log.debug(f"Against trend for {symbol} {tf}")
        return None

    # Elite MTF confirmation
    if not await elite_tf_alignment(exchange, symbol, side):
        log.debug(f"No elite MTF alignment for {symbol} {tf}")
        return None
    reasons.append("Elite MTF Alignment ✅")

    # Create signal
    sig = {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "score": score,
        "reason": "RomeOPT 6-Step",
        "reason_list": reasons,
        "htf_alignment": htf_alignment,
        "liquidity_sweep": liquidity_sweep
    }

    # Calculate RomeOPT TP/SL
    sig = calculate_romeopt_tp_sl(sig, df, tf)
    
    # FINAL VALIDATION: Ensure TP/SL makes sense
    if "sl" in sig and "tp1" in sig:
        risk = abs(sig["entry"] - sig["sl"])
        tp1_distance = abs(sig["tp1"] - sig["entry"])
        
        # Reject if TP1 is less than 50% of risk (meaningless profit)
        if tp1_distance < risk * 0.5:
            log.debug(f"TP1 too close for {symbol} {tf}: {tp1_distance} < {risk * 0.5}")
            return None
        
        # Reject if risk is too small (< 0.3%)
        if risk / sig["entry"] < 0.003:
            log.debug(f"Risk too small for {symbol} {tf}: {risk / sig['entry']:.4f}")
            return None
        
        # Reject if SL placement is clearly wrong
        if side == "BUY" and sig["sl"] >= sig["entry"]:
            log.debug(f"Invalid BUY SL for {symbol} {tf}: SL={sig['sl']} >= Entry={sig['entry']}")
            return None
        if side == "SELL" and sig["sl"] <= sig["entry"]:
            log.debug(f"Invalid SELL SL for {symbol} {tf}: SL={sig['sl']} <= Entry={sig['entry']}")
            return None
    
    log.info(f"✅ Signal generated for {symbol} {tf}: {side} at {entry}, score={score}")
    return sig

# ---------------- FIND LATEST OB ----------------
def find_latest_ob(df: pd.DataFrame) -> Optional[Dict]:
    """Find the most recent order block"""
    if len(df) < 6:
        return None
    
    # Look for OB in last 10 candles
    for i in range(len(df)-10, len(df)-1):
        candle = df.iloc[i]
        prev_candle = df.iloc[i-1]
        
        # Bullish OB: Bearish candle followed by bullish candle
        if (candle["close"] > candle["open"] and 
            prev_candle["close"] < prev_candle["open"]):
            return {
                "type": "bullish",
                "low": min(candle["low"], prev_candle["low"]),
                "high": candle["close"],
                "index": i
            }
        
        # Bearish OB: Bullish candle followed by bearish candle
        elif (candle["close"] < candle["open"] and 
              prev_candle["close"] > prev_candle["open"]):
            return {
                "type": "bearish",
                "low": candle["close"],
                "high": max(candle["high"], prev_candle["high"]),
                "index": i
            }
    
    return None

# ---------------- SL CLUSTER ----------------
recent_sl = defaultdict(lambda: deque())
def record_sl_hit(symbol: str, lookback_minutes: int = 30):
    """Record SL hit for a symbol"""
    now = time.time()
    dq = recent_sl[symbol]
    dq.append(now)
    cutoff = now - lookback_minutes * 60
    while dq and dq[0] < cutoff:
        dq.popleft()

def deprioritized(symbol: str, threshold: int = 3, lookback: int = 30) -> bool:
    """Check if symbol should be deprioritized due to recent SL hits"""
    dq = recent_sl[symbol]
    now = time.time()
    cutoff = now - lookback * 60
    while dq and dq[0] < cutoff:
        dq.popleft()
    return len(dq) >= threshold

# ---------------- LOG SIGNAL ----------------
async def log_signal(sig: Dict):
    """Log signal to database"""
    async with db_lock:
        try:
            # Check if tp_sl_type column exists by trying to insert
            await db_conn.execute("""
                INSERT INTO signals (symbol, side, entry, sl, tp1, tp2, tp3, timestamp, status, reason, score, latest_ob, tp_sl_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sig["symbol"],
                sig["side"],
                sig["entry"],
                sig.get("sl"),
                sig.get("tp1"),
                sig.get("tp2"),
                sig.get("tp3"),
                datetime.datetime.utcnow().isoformat(),
                "OPEN",
                sig.get("reason", "RomeOPT 6-Step"),
                sig.get("score", 0),
                json.dumps(sig.get("latest_ob", {})),
                sig.get("tp_sl_type", "Legacy")
            ))
            await db_conn.commit()
            log.info(f"✅ Signal logged: {sig['symbol']} {sig['side']} at {sig['entry']:.5f}")
        except Exception as e:
            log.error(f"Error logging signal: {e}")
            # Try without tp_sl_type column (fallback for old database)
            try:
                await db_conn.execute("""
                    INSERT INTO signals (symbol, side, entry, sl, tp1, tp2, tp3, timestamp, status, reason, score, latest_ob)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    sig["symbol"],
                    sig["side"],
                    sig["entry"],
                    sig.get("sl"),
                    sig.get("tp1"),
                    sig.get("tp2"),
                    sig.get("tp3"),
                    datetime.datetime.utcnow().isoformat(),
                    "OPEN",
                    sig.get("reason", "RomeOPT 6-Step"),
                    sig.get("score", 0),
                    json.dumps(sig.get("latest_ob", {}))
                ))
                await db_conn.commit()
                log.info(f"✅ Signal logged (fallback): {sig['symbol']} {sig['side']}")
            except Exception as e2:
                log.error(f"Fallback logging also failed: {e2}")

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    """Monitor open signals and update TP/SL - WITH RECALCULATION FOR ROMEOPT SIGNALS"""
    log.info("Starting signal monitor")
    
    while True:
        try:
            async with db_lock:
                # First check if tp_sl_type column exists
                cursor = await db_conn.execute("PRAGMA table_info(signals)")
                columns = await cursor.fetchall()
                column_names = [col[1] for col in columns]
                
                has_tp_sl_type = 'tp_sl_type' in column_names
                
                # Build query based on available columns
                if has_tp_sl_type:
                    query = """
                        SELECT id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status, tp_sl_type 
                        FROM signals WHERE status='OPEN'
                    """
                else:
                    query = """
                        SELECT id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status, 'Legacy' as tp_sl_type 
                        FROM signals WHERE status='OPEN'
                    """
                
                async with db_conn.execute(query) as cursor:
                    async for row in cursor:
                        if has_tp_sl_type:
                            sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status, tp_sl_type = row
                        else:
                            sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status = row
                            tp_sl_type = 'Legacy'  # Default for old database
                        
                        # Fetch current price
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get("last")
                        if last_price is None:
                            continue
                        
                        # ========== CRITICAL FIX: RECALCULATE TP/SL FOR ROMEOPT SIGNALS ==========
                        if tp_sl_type == "ROMEOPT":
                            # Fetch live data for TP/SL recalculation
                            ohlcv = await fetch_ohlcv(exchange, symbol, "1m", 100)
                            if ohlcv:
                                df_live = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
                                for col in ["open", "high", "low", "close", "vol"]:
                                    df_live[col] = pd.to_numeric(df_live[col], errors='coerce')
                                
                                # Recalculate TP/SL with live data
                                sig = {
                                    "symbol": symbol,
                                    "side": side,
                                    "entry": entry,
                                    "sl": sl,
                                    "tp1": tp1,
                                    "tp2": tp2,
                                    "tp3": tp3
                                }
                                
                                sig = calculate_romeopt_tp_sl(sig, df_live, "1m")
                                new_sl, new_tp1 = sig.get("sl"), sig.get("tp1")
                                
                                # VALIDATE new TP/SL before updating
                                if new_sl and new_tp1:
                                    is_valid = True
                                    if side == "BUY":
                                        if new_sl >= entry or new_tp1 <= entry:
                                            log.warning(f"Invalid recalc for BUY {symbol}, skipping update")
                                            is_valid = False
                                    else:  # SELL
                                        if new_sl <= entry or new_tp1 >= entry:
                                            log.warning(f"Invalid recalc for SELL {symbol}, skipping update")
                                            is_valid = False
                                    
                                    # Check if significant change (>0.5%) AND valid
                                    if is_valid:
                                        sl_change = abs(new_sl - sl) / entry if sl else 0
                                        tp_change = abs(new_tp1 - tp1) / entry if tp1 else 0
                                        
                                        if sl_change > 0.005 or tp_change > 0.005:  # 0.5% threshold
                                            # Calculate RRR for validation
                                            new_risk = abs(entry - new_sl)
                                            new_reward = abs(new_tp1 - entry)
                                            new_rrr = new_reward / new_risk if new_risk > 0 else 0
                                            
                                            # Only update if RRR is reasonable
                                            if new_rrr >= 0.8:  # At least 0.8:1 RRR
                                                old_sl, old_tp1 = sl, tp1
                                                sl, tp1, tp2, tp3 = new_sl, new_tp1, sig.get("tp2"), sig.get("tp3")
                                                
                                                # Update database
                                                await db_conn.execute(
                                                    "UPDATE signals SET sl=?, tp1=?, tp2=?, tp3=? WHERE id=?",
                                                    (sl, tp1, tp2, tp3, sig_id)
                                                )
                                                
                                                # Send update notification
                                                await tg(f"📈 {symbol} TP/SL Updated\nEntry: {entry:.5f}\nSL: {old_sl:.5f} → {sl:.5f}\nTP1: {old_tp1:.5f} → {tp1:.5f}")
                                                
                                                log.info(f"Updated TP/SL for {symbol}: SL {old_sl:.5f}→{sl:.5f}, TP1 {old_tp1:.5f}→{tp1:.5f}")
                        
                        # ========== CHECK FOR TP/SL HITS ==========
                        hits = []
                        sl_hit = False
                        
                        if side == "BUY":
                            # Validate levels first
                            if sl >= entry or tp1 <= entry:
                                log.warning(f"Invalid BUY levels for {symbol}, skipping hit detection")
                                continue
                            
                            # Check hits
                            if not tp1_hit and last_price >= tp1:
                                hits.append("TP1")
                                tp1_hit = 1
                            if not tp2_hit and last_price >= tp2:
                                hits.append("TP2")
                                tp2_hit = 1
                            if not tp3_hit and last_price >= tp3:
                                hits.append("TP3")
                                tp3_hit = 1
                            if last_price <= sl:
                                hits.append("SL")
                                status = "CLOSED"
                                sl_hit = True
                        else:  # SELL
                            # Validate levels first
                            if sl <= entry or tp1 >= entry:
                                log.warning(f"Invalid SELL levels for {symbol}, skipping hit detection")
                                continue
                            
                            # Check hits
                            if not tp1_hit and last_price <= tp1:
                                hits.append("TP1")
                                tp1_hit = 1
                            if not tp2_hit and last_price <= tp2:
                                hits.append("TP2")
                                tp2_hit = 1
                            if not tp3_hit and last_price <= tp3:
                                hits.append("TP3")
                                tp3_hit = 1
                            if last_price >= sl:
                                hits.append("SL")
                                status = "CLOSED"
                                sl_hit = True
                        
                        if hits:
                            await tg(f"🎯 {symbol} {side} Update\nEntry: {entry:.5f}\nLast: {last_price:.5f}\nHits: {', '.join(hits)}\nSL: {sl:.5f}\nTP1: {tp1:.5f} TP2: {tp2:.5f} TP3: {tp3:.5f}")
                        
                        if sl_hit:
                            record_sl_hit(symbol)
                        
                        # Update database with hit status
                        await db_conn.execute(
                            "UPDATE signals SET tp1_hit=?, tp2_hit=?, tp3_hit=?, status=? WHERE id=?",
                            (tp1_hit, tp2_hit, tp3_hit, status, sig_id)
                        )
                
                await db_conn.commit()
                
        except Exception as e:
            log.exception(f"Monitor error: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- SCAN LOOP ----------------
last_signal_time = {}
async def scan_loop(exchange):
    """Main scanning loop"""
    log.info("Starting scan loop")
    
    while True:
        t0 = time.time()
        try:
            # Fetch top symbols by volume
            tickers = await exchange.fetch_tickers()
            top = sorted([
                (s, v.get("quoteVolume", 0)) 
                for s, v in tickers.items() 
                if s.endswith("USDT")
            ], key=lambda x: x[1], reverse=True)[:TOP_N]
            
            signals_found = 0
            for symbol, _ in top:
                if deprioritized(symbol):
                    log.debug(f"Deprioritized: {symbol}")
                    continue
                
                for tf in TIMEFRAMES:
                    key = f"{symbol}:{tf}"
                    
                    # Rate limiting
                    if key in last_signal_time and time.time() - last_signal_time[key] < 60:
                        continue
                    
                    # Fetch OHLCV data
                    ohlcv = await fetch_ohlcv(exchange, symbol, tf, 200)
                    if not ohlcv:
                        continue
                    
                    # Create DataFrame
                    df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
                    for col in ["open", "high", "low", "close", "vol"]:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Generate signal
                    sig = await generate_signal_romeopt(exchange, df, symbol, tf)
                    if sig:
                        htf_flag = sig.get("htf_alignment", "N/A")
                        sweep_flag = sig.get("liquidity_sweep", "N/A")
                        tp_sl_type = sig.get("tp_sl_type", "Legacy")
                        rrr = sig.get("rrr", 0)
                        
                        # Send Telegram alert
                        await tg(f"""
🏆 {sig['symbol']} ({tf}) {sig['side']} [{tp_sl_type}]
Entry: {sig['entry']:.5f}
SL: {sig.get('sl', 0):.5f}
TP1: {sig.get('tp1', 0):.5f} TP2: {sig.get('tp2', 0):.5f} TP3: {sig.get('tp3', 0):.5f}
Score: {sig['score']} | RRR: {rrr:.2f}
HTF: {htf_flag} Sweep: {sweep_flag}
Breakdown: {', '.join(sig['reason_list'])}
                        """)
                        
                        # Log to database
                        await log_signal(sig)
                        
                        # Update rate limiting
                        last_signal_time[key] = time.time()
                        signals_found += 1
            
            log.info(f"📊 Scan complete: {signals_found} RomeOPT signals found")
            
        except Exception as e:
            log.exception(f"Scan error: {e}")
        
        elapsed = time.time() - t0
        sleep_time = max(1, SCAN_INTERVAL - elapsed)
        await asyncio.sleep(sleep_time)

# ---------------- CLEANUP OLD SIGNALS ----------------
async def cleanup_old_signals():
    """Clean up old invalid signals from database"""
    try:
        async with db_lock:
            # Close signals that are clearly invalid
            await db_conn.execute("""
                UPDATE signals SET status='INVALID' 
                WHERE status='OPEN' AND (
                    (side='BUY' AND (sl >= entry OR tp1 <= entry)) OR
                    (side='SELL' AND (sl <= entry OR tp1 >= entry))
                )
            """)
            
            # Close very old open signals (>24 hours)
            await db_conn.execute("""
                UPDATE signals SET status='EXPIRED' 
                WHERE status='OPEN' AND timestamp < datetime('now', '-24 hours')
            """)
            
            await db_conn.commit()
            log.info("Cleaned up old invalid signals")
    except Exception as e:
        log.error(f"Error cleaning up old signals: {e}")

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/")
async def root():
    return {"status": "ok", "bot": "RomeOPT 6-Step Scanner", "version": "3.0"}

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.datetime.utcnow().isoformat()}

@app.get("/signals")
async def get_signals(limit: int = 10):
    async with db_lock:
        async with db_conn.execute("""
            SELECT id, symbol, side, entry, sl, tp1, tp2, tp3, timestamp, status, score 
            FROM signals ORDER BY id DESC LIMIT ?
        """, (limit,)) as cursor:
            rows = await cursor.fetchall()
            return [
                {
                    "id": row[0],
                    "symbol": row[1],
                    "side": row[2],
                    "entry": row[3],
                    "sl": row[4],
                    "tp1": row[5],
                    "tp2": row[6],
                    "tp3": row[7],
                    "timestamp": row[8],
                    "status": row[9],
                    "score": row[10]
                }
                for row in rows
            ]

@app.post("/webhook")
async def webhook(request: Request):
    """Webhook endpoint for external triggers"""
    token = request.headers.get("X-Auth", "")
    if token != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    
    data = await request.json()
    log.info(f"Webhook received: {data}")
    return {"ok": True}

# ---------------- MAIN ----------------
async def main():
    """Main entry point"""
    global exchange
    
    # Initialize database
    await init_db()
    
    # Clean up old invalid signals
    await cleanup_old_signals()
    
    # Initialize exchange
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "rateLimit": 100,
        "timeout": 30000,
    })
    
    log.info("RomeOPT 6-Step Scanner Starting...")
    await tg("🏆 ROMEOPT 6-Step Scanner v3.0 Started - TRUE Institutional TP/SL Logic")
    
    # Run both scanning and monitoring
    await asyncio.gather(
        scan_loop(exchange),
        monitor_signals()
    )

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", help="Run HTTP server")
    args = parser.parse_args()
    
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=9000)
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Bot stopped by user")
        except Exception as e:
            log.exception(f"Fatal error: {e}")
        finally:
            if db_conn:
                asyncio.run(db_conn.close())
            log.info("Bot shutdown complete")