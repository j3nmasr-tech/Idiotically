#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ULTRA-FOCUSED 5-METHOD SCANNER - COMPLETE VERSION
- Supply/Demand Zones
- FVG (Fair Value Gaps)
- Order Blocks
- Liquidity Grab
- Breaker Blocks
- Timeframes: 15m, 30m, 1h, 2h, 3h, 4h
- MIN_CONFIDENCE = 0.1 for data collection
"""

import os
import time
import asyncio
import logging
import datetime
import json
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

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 30))
TOP_N = int(os.getenv("TOP_N", 60))
TIMEFRAMES = ["15m", "30m", "1h", "2h", "3h", "4h"]
MIN_ZONE_SIZE = 3
MIN_CONFIDENCE = 0.1  # 10% for data collection

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("scanner_final")
db_lock = asyncio.Lock()
db_conn = None
exchange = None

# ---------------- TELEGRAM ----------------
async def tg(msg: str):
    """Send Telegram message"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        async with httpx.AsyncClient() as client:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": "HTML"
            })
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")

# ---------------- DATABASE ----------------
async def migrate_db():
    """Migrate database to add missing columns"""
    try:
        # Check if table exists
        async with db_conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='signals'") as cursor:
            if not await cursor.fetchone():
                return True  # Table doesn't exist, will be created
        
        # Get current columns
        async with db_conn.execute("PRAGMA table_info(signals)") as cursor:
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]
        
        # Define required columns
        required_columns = {
            'method': 'TEXT NOT NULL',
            'method_details': 'TEXT',
            'strength': 'INTEGER DEFAULT 0',
            'confidence': 'REAL DEFAULT 0',
            'timeframe': 'TEXT NOT NULL',
            'zone_high': 'REAL',
            'zone_low': 'REAL',
            'tp_hit': 'INTEGER DEFAULT 0',
            'sl_hit': 'INTEGER DEFAULT 0',
            'rr_ratio': 'REAL',
            'risk_pct': 'REAL',
            'reward_pct': 'REAL',
            'numeric_breakdown': 'TEXT'
        }
        
        # Add missing columns
        for col_name, col_type in required_columns.items():
            if col_name not in column_names:
                try:
                    await db_conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_type}")
                    log.info(f"✅ Added column: {col_name}")
                except Exception as e:
                    log.warning(f"Could not add column {col_name}: {e}")
        
        await db_conn.commit()
        return True
    except Exception as e:
        log.error(f"Migration error: {e}")
        return False

async def init_db():
    """Initialize database with proper migration"""
    global db_conn
    try:
        db_conn = await aiosqlite.connect(DB_PATH)
        await db_conn.execute("PRAGMA journal_mode=WAL;")
        await db_conn.execute("PRAGMA synchronous=NORMAL;")
        
        # Create signals table if it doesn't exist
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry REAL NOT NULL,
                sl REAL NOT NULL,
                tp REAL NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'OPEN',
                method TEXT NOT NULL,
                method_details TEXT,
                strength INTEGER DEFAULT 0,
                confidence REAL DEFAULT 0,
                timeframe TEXT NOT NULL,
                zone_high REAL,
                zone_low REAL,
                tp_hit INTEGER DEFAULT 0,
                sl_hit INTEGER DEFAULT 0,
                rr_ratio REAL,
                risk_pct REAL,
                reward_pct REAL,
                numeric_breakdown TEXT
            )
        """)
        
        # Migrate existing table
        await migrate_db()
        
        # Create indexes (after ensuring columns exist)
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol_timeframe ON signals(symbol, timeframe);")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON signals(status);")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON signals(timestamp);")
        
        # Try to create method index, but don't fail if column doesn't exist
        try:
            await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_method ON signals(method);")
        except Exception as e:
            log.warning(f"Could not create method index (column may not exist yet): {e}")
        
        await db_conn.commit()
        log.info("Database initialized successfully")
        return True
    except Exception as e:
        log.error(f"Database initialization error: {e}")
        if db_conn:
            await db_conn.close()
        return False

# ---------------- OHLCV FETCH ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 300) -> Optional[List]:
    """Fetch OHLCV data"""
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.warning(f"Error fetching {symbol} {timeframe}: {e}")
        return None

# ---------------- CALCULATION UTILITIES ----------------
def calculate_atr(df: pd.DataFrame, period: int = 14) -> float:
    """Calculate Average True Range"""
    if len(df) < period:
        return 0.0
    
    try:
        high = df['high']
        low = df['low']
        close = df['close']
        
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period, min_periods=period).mean()
        
        return float(atr.iloc[-1]) if pd.notna(atr.iloc[-1]) else 0.0
    except Exception as e:
        log.warning(f"ATR calculation error: {e}")
        return 0.0

def calculate_price_stats(df: pd.DataFrame, lookback: int = 20) -> Dict[str, float]:
    """Calculate price statistics"""
    if len(df) < lookback:
        lookback = len(df)
    
    try:
        recent = df.iloc[-lookback:]
        
        high = float(recent['high'].max())
        low = float(recent['low'].min())
        close = float(recent['close'].iloc[-1])
        
        if low > 0:
            range_pct = float((high - low) / low * 100)
        else:
            range_pct = 0.0
        
        if len(recent) > 1 and recent['close'].mean() > 0:
            volatility = float(recent['high'].std() / recent['close'].mean() * 100)
        else:
            volatility = 0.0
        
        return {
            'high': high,
            'low': low,
            'close': close,
            'range_pct': range_pct,
            'volatility': volatility
        }
    except Exception as e:
        log.warning(f"Price stats calculation error: {e}")
        return {
            'high': 0.0,
            'low': 0.0,
            'close': 0.0,
            'range_pct': 0.0,
            'volatility': 0.0
        }

# ================ 5 CORE METHODS ================

def find_supply_demand_zones(df: pd.DataFrame, lookback: int = 100) -> List[Dict]:
    """Find Supply/Demand Zones"""
    zones = []
    
    if len(df) < 20:
        return zones
    
    try:
        atr_val = calculate_atr(df, 14)
        if atr_val == 0:
            return zones
        
        prices = df.iloc[-lookback:]
        highs = prices['high'].values
        lows = prices['low'].values
        
        # Use clustering
        price_range = max(highs.max(), lows.max()) - min(highs.min(), lows.min())
        if price_range == 0:
            return zones
        
        num_bins = max(10, min(30, len(prices) // 10))
        bins = np.linspace(min(lows.min(), highs.min()), max(lows.max(), highs.max()), num_bins)
        
        for i in range(len(bins) - 1):
            bin_low = bins[i]
            bin_high = bins[i + 1]
            
            # Count highs in this bin
            highs_in_bin = sum(1 for h in highs if bin_low <= h <= bin_high)
            if highs_in_bin >= MIN_ZONE_SIZE:
                zones.append({
                    'type': 'SUPPLY',
                    'price': float((bin_low + bin_high) / 2),
                    'high': float(bin_high),
                    'low': float(bin_low),
                    'strength': highs_in_bin,
                    'method': 'supply_demand',
                    'details': f"Supply: {highs_in_bin} highs at {bin_low:.6f}-{bin_high:.6f}"
                })
            
            # Count lows in this bin
            lows_in_bin = sum(1 for l in lows if bin_low <= l <= bin_high)
            if lows_in_bin >= MIN_ZONE_SIZE:
                zones.append({
                    'type': 'DEMAND',
                    'price': float((bin_low + bin_high) / 2),
                    'high': float(bin_high),
                    'low': float(bin_low),
                    'strength': lows_in_bin,
                    'method': 'supply_demand',
                    'details': f"Demand: {lows_in_bin} lows at {bin_low:.6f}-{bin_high:.6f}"
                })
        
        return zones
    except Exception as e:
        log.warning(f"Supply/Demand zones error: {e}")
        return []

def find_fvg(df: pd.DataFrame, lookback: int = 50) -> List[Dict]:
    """Find Fair Value Gaps"""
    fvgs = []
    
    if len(df) < 3:
        return fvgs
    
    try:
        atr_val = calculate_atr(df, 14)
        
        for i in range(2, min(lookback, len(df))):
            prev = df.iloc[-i-1]
            curr = df.iloc[-i]
            
            # Bullish FVG
            if prev['high'] < curr['low']:
                gap_size = curr['low'] - prev['high']
                gap_size_atr = gap_size / atr_val if atr_val > 0 else 0
                
                if gap_size_atr > 0.3:
                    fvgs.append({
                        'type': 'BULLISH_FVG',
                        'gap_top': float(curr['low']),
                        'gap_bottom': float(prev['high']),
                        'gap_size': float(gap_size),
                        'method': 'fvg',
                        'details': f"Bullish FVG: {gap_size:.6f}"
                    })
            
            # Bearish FVG
            elif prev['low'] > curr['high']:
                gap_size = prev['low'] - curr['high']
                gap_size_atr = gap_size / atr_val if atr_val > 0 else 0
                
                if gap_size_atr > 0.3:
                    fvgs.append({
                        'type': 'BEARISH_FVG',
                        'gap_top': float(prev['low']),
                        'gap_bottom': float(curr['high']),
                        'gap_size': float(gap_size),
                        'method': 'fvg',
                        'details': f"Bearish FVG: {gap_size:.6f}"
                    })
        
        return fvgs
    except Exception as e:
        log.warning(f"FVG error: {e}")
        return []

def find_order_blocks(df: pd.DataFrame, lookback: int = 50) -> List[Dict]:
    """Find Order Blocks"""
    obs = []
    
    if len(df) < 3:
        return obs
    
    try:
        for i in range(2, min(lookback, len(df))):
            candle1 = df.iloc[-i-1]
            candle2 = df.iloc[-i]
            
            candle1_size = candle1['high'] - candle1['low']
            candle2_size = candle2['high'] - candle2['low']
            
            if candle1_size == 0 or candle2_size == 0:
                continue
            
            candle1_body = abs(candle1['close'] - candle1['open'])
            candle2_body = abs(candle2['close'] - candle2['open'])
            
            candle1_body_ratio = candle1_body / candle1_size
            candle2_body_ratio = candle2_body / candle2_size
            
            # Bullish OB
            if (candle1['close'] < candle1['open'] and
                candle2['close'] > candle2['open'] and
                candle1_body_ratio > 0.6 and
                candle2_body_ratio > 0.6):
                
                obs.append({
                    'type': 'BULLISH_OB',
                    'low': float(min(candle1['low'], candle2['low'])),
                    'high': float(max(candle1['high'], candle2['high'])),
                    'method': 'order_block',
                    'details': "Bullish Order Block"
                })
            
            # Bearish OB
            elif (candle1['close'] > candle1['open'] and
                  candle2['close'] < candle2['open'] and
                  candle1_body_ratio > 0.6 and
                  candle2_body_ratio > 0.6):
                
                obs.append({
                    'type': 'BEARISH_OB',
                    'low': float(min(candle1['low'], candle2['low'])),
                    'high': float(max(candle1['high'], candle2['high'])),
                    'method': 'order_block',
                    'details': "Bearish Order Block"
                })
        
        return obs
    except Exception as e:
        log.warning(f"Order blocks error: {e}")
        return []

def find_liquidity_grab(df: pd.DataFrame, lookback: int = 30) -> List[Dict]:
    """Find Liquidity Grabs"""
    lgs = []
    
    if len(df) < 10:
        return lgs
    
    try:
        for i in range(3, min(lookback, len(df))):
            candle = df.iloc[-i]
            prev_lows = df['low'].iloc[-i-5:-i].values if i+5 < len(df) else df['low'].iloc[:].values
            prev_highs = df['high'].iloc[-i-5:-i].values if i+5 < len(df) else df['high'].iloc[:].values
            
            # Bullish LG
            if candle['low'] < min(prev_lows):
                if i > 1:
                    next_candle = df.iloc[-i+1]
                    if next_candle['close'] > candle['high']:
                        lgs.append({
                            'type': 'BULLISH_LG',
                            'sweep_price': float(candle['low']),
                            'method': 'liquidity_grab',
                            'details': f"Bullish Liquidity Grab at {candle['low']:.6f}"
                        })
            
            # Bearish LG
            elif candle['high'] > max(prev_highs):
                if i > 1:
                    next_candle = df.iloc[-i+1]
                    if next_candle['close'] < candle['low']:
                        lgs.append({
                            'type': 'BEARISH_LG',
                            'sweep_price': float(candle['high']),
                            'method': 'liquidity_grab',
                            'details': f"Bearish Liquidity Grab at {candle['high']:.6f}"
                        })
        
        return lgs
    except Exception as e:
        log.warning(f"Liquidity grab error: {e}")
        return []

def find_breaker_blocks(df: pd.DataFrame, lookback: int = 50) -> List[Dict]:
    """Find Breaker Blocks"""
    breakers = []
    
    if len(df) < 20:
        return breakers
    
    try:
        # Find swing points
        for i in range(2, len(df) - 1):
            if i >= lookback:
                break
            
            # Swing low
            if (df['low'].iloc[-i] < df['low'].iloc[-i-1] and
                df['low'].iloc[-i] < df['low'].iloc[-i+1]):
                
                # Check for break and recovery
                for j in range(1, min(5, i)):
                    idx = -i + j
                    if idx >= 0:
                        break_candle = df.iloc[idx]
                        if (break_candle['low'] < df['low'].iloc[-i] and
                            break_candle['close'] > df['low'].iloc[-i]):
                            
                            breakers.append({
                                'type': 'BULLISH_BREAKER',
                                'structure_price': float(df['low'].iloc[-i]),
                                'method': 'breaker_block',
                                'details': f"Bullish Breaker at {df['low'].iloc[-i]:.6f}"
                            })
                            break
            
            # Swing high
            if (df['high'].iloc[-i] > df['high'].iloc[-i-1] and
                df['high'].iloc[-i] > df['high'].iloc[-i+1]):
                
                for j in range(1, min(5, i)):
                    idx = -i + j
                    if idx >= 0:
                        break_candle = df.iloc[idx]
                        if (break_candle['high'] > df['high'].iloc[-i] and
                            break_candle['close'] < df['high'].iloc[-i]):
                            
                            breakers.append({
                                'type': 'BEARISH_BREAKER',
                                'structure_price': float(df['high'].iloc[-i]),
                                'method': 'breaker_block',
                                'details': f"Bearish Breaker at {df['high'].iloc[-i]:.6f}"
                            })
                            break
        
        return breakers
    except Exception as e:
        log.warning(f"Breaker blocks error: {e}")
        return []

# ================ SIGNAL GENERATION ================

def generate_numeric_breakdown(signal: Dict, df: pd.DataFrame) -> str:
    """Generate numeric breakdown for signal"""
    try:
        breakdown = []
        
        # Price statistics
        stats = calculate_price_stats(df, 20)
        atr_val = calculate_atr(df, 14)
        
        breakdown.append("=== PRICE STATISTICS ===")
        breakdown.append(f"Current: {signal['entry']:.6f}")
        breakdown.append(f"High: {stats['high']:.6f}")
        breakdown.append(f"Low: {stats['low']:.6f}")
        breakdown.append(f"Range: {stats['range_pct']:.2f}%")
        breakdown.append(f"ATR: {atr_val:.6f}")
        
        # Trade parameters
        risk = abs(signal['entry'] - signal['sl'])
        reward = abs(signal['tp'] - signal['entry'])
        rr = reward / risk if risk > 0 else 0
        risk_pct = (risk / signal['entry']) * 100 if signal['entry'] > 0 else 0
        reward_pct = (reward / signal['entry']) * 100 if signal['entry'] > 0 else 0
        
        breakdown.append("\n=== TRADE ===")
        breakdown.append(f"Entry: {signal['entry']:.6f}")
        breakdown.append(f"SL: {signal['sl']:.6f} (Risk: {risk_pct:.2f}%)")
        breakdown.append(f"TP: {signal['tp']:.6f} (Reward: {reward_pct:.2f}%)")
        breakdown.append(f"R:R: {rr:.2f}:1")
        
        # Signal info
        breakdown.append(f"\n=== SIGNAL ===")
        breakdown.append(f"Method: {signal['method']}")
        breakdown.append(f"Side: {signal['side']}")
        breakdown.append(f"TF: {signal['timeframe']}")
        breakdown.append(f"Strength: {signal.get('strength', 0)}")
        breakdown.append(f"Confidence: {signal.get('confidence', 0):.2f}")
        
        return "\n".join(breakdown)
    except Exception as e:
        return f"Breakdown error: {e}"

def calculate_tp_sl(signal: Dict, df: pd.DataFrame, atr_val: float) -> Tuple[Optional[float], Optional[float]]:
    """Calculate Take Profit and Stop Loss"""
    try:
        entry = signal['entry']
        side = signal['side']
        
        if atr_val == 0:
            return None, None
        
        # Base calculation
        if side == 'BUY':
            sl = entry - (atr_val * 1.5)
            tp = entry + (2 * (entry - sl))
            
            recent_low = df['low'].iloc[-10:].min()
            sl = min(sl, recent_low - (atr_val * 0.3))
        else:
            sl = entry + (atr_val * 1.5)
            tp = entry - (2 * (sl - entry))
            
            recent_high = df['high'].iloc[-10:].max()
            sl = max(sl, recent_high + (atr_val * 0.3))
        
        # Validate
        if side == 'BUY':
            if sl >= entry or tp <= entry:
                return None, None
        else:
            if sl <= entry or tp >= entry:
                return None, None
        
        return sl, tp
    except Exception as e:
        log.warning(f"TP/SL error: {e}")
        return None, None

def analyze_all_methods(df: pd.DataFrame, symbol: str, timeframe: str) -> Optional[Dict]:
    """Analyze all 5 methods and generate best signal"""
    if len(df) < 100:
        return None
    
    try:
        # Run all methods
        zones = find_supply_demand_zones(df)
        fvgs = find_fvg(df)
        obs = find_order_blocks(df)
        lgs = find_liquidity_grab(df)
        breakers = find_breaker_blocks(df)
        
        current_price = df['close'].iloc[-1]
        atr_val = calculate_atr(df, 14)
        all_signals = []
        
        # Process zones
        for zone in zones:
            distance = abs(current_price - zone['price']) / zone['price'] * 100
            if distance < 3:
                confidence = min(0.95, zone['strength'] / 15)
                
                if zone['type'] == 'DEMAND':
                    all_signals.append({
                        'symbol': symbol,
                        'side': 'BUY',
                        'entry': current_price,
                        'method': 'supply_demand',
                        'strength': zone['strength'],
                        'confidence': confidence,
                        'timeframe': timeframe,
                        'zone_high': zone['high'],
                        'zone_low': zone['low'],
                        'method_details': zone['details']
                    })
                else:
                    all_signals.append({
                        'symbol': symbol,
                        'side': 'SELL',
                        'entry': current_price,
                        'method': 'supply_demand',
                        'strength': zone['strength'],
                        'confidence': confidence,
                        'timeframe': timeframe,
                        'zone_high': zone['high'],
                        'zone_low': zone['low'],
                        'method_details': zone['details']
                    })
        
        # Process FVGs
        for fvg in fvgs:
            if fvg['type'] == 'BULLISH_FVG' and fvg['gap_bottom'] <= current_price <= fvg['gap_top']:
                confidence = 0.7
                if confidence >= MIN_CONFIDENCE:
                    all_signals.append({
                        'symbol': symbol,
                        'side': 'BUY',
                        'entry': current_price,
                        'method': 'fvg',
                        'strength': 6,
                        'confidence': confidence,
                        'timeframe': timeframe,
                        'method_details': fvg['details']
                    })
            elif fvg['type'] == 'BEARISH_FVG' and fvg['gap_bottom'] <= current_price <= fvg['gap_top']:
                confidence = 0.7
                if confidence >= MIN_CONFIDENCE:
                    all_signals.append({
                        'symbol': symbol,
                        'side': 'SELL',
                        'entry': current_price,
                        'method': 'fvg',
                        'strength': 6,
                        'confidence': confidence,
                        'timeframe': timeframe,
                        'method_details': fvg['details']
                    })
        
        # Process Order Blocks
        for ob in obs:
            if ob['type'] == 'BULLISH_OB' and ob['low'] <= current_price <= ob['high']:
                confidence = 0.75
                if confidence >= MIN_CONFIDENCE:
                    all_signals.append({
                        'symbol': symbol,
                        'side': 'BUY',
                        'entry': current_price,
                        'method': 'order_block',
                        'strength': 7,
                        'confidence': confidence,
                        'timeframe': timeframe,
                        'method_details': ob['details']
                    })
            elif ob['type'] == 'BEARISH_OB' and ob['low'] <= current_price <= ob['high']:
                confidence = 0.75
                if confidence >= MIN_CONFIDENCE:
                    all_signals.append({
                        'symbol': symbol,
                        'side': 'SELL',
                        'entry': current_price,
                        'method': 'order_block',
                        'strength': 7,
                        'confidence': confidence,
                        'timeframe': timeframe,
                        'method_details': ob['details']
                    })
        
        # Process Liquidity Grabs
        for lg in lgs:
            if lg['type'] == 'BULLISH_LG':
                distance = abs(current_price - lg['sweep_price']) / current_price * 100
                if distance < 5:
                    confidence = 0.65
                    if confidence >= MIN_CONFIDENCE:
                        all_signals.append({
                            'symbol': symbol,
                            'side': 'BUY',
                            'entry': current_price,
                            'method': 'liquidity_grab',
                            'strength': 6,
                            'confidence': confidence,
                            'timeframe': timeframe,
                            'method_details': lg['details']
                        })
            elif lg['type'] == 'BEARISH_LG':
                distance = abs(current_price - lg['sweep_price']) / current_price * 100
                if distance < 5:
                    confidence = 0.65
                    if confidence >= MIN_CONFIDENCE:
                        all_signals.append({
                            'symbol': symbol,
                            'side': 'SELL',
                            'entry': current_price,
                            'method': 'liquidity_grab',
                            'strength': 6,
                            'confidence': confidence,
                            'timeframe': timeframe,
                            'method_details': lg['details']
                        })
        
        # Process Breaker Blocks
        for breaker in breakers:
            if breaker['type'] == 'BULLISH_BREAKER':
                distance = abs(current_price - breaker['structure_price']) / current_price * 100
                if distance < 3:
                    confidence = 0.7
                    if confidence >= MIN_CONFIDENCE:
                        all_signals.append({
                            'symbol': symbol,
                            'side': 'BUY',
                            'entry': current_price,
                            'method': 'breaker_block',
                            'strength': 8,
                            'confidence': confidence,
                            'timeframe': timeframe,
                            'method_details': breaker['details']
                        })
            elif breaker['type'] == 'BEARISH_BREAKER':
                distance = abs(current_price - breaker['structure_price']) / current_price * 100
                if distance < 3:
                    confidence = 0.7
                    if confidence >= MIN_CONFIDENCE:
                        all_signals.append({
                            'symbol': symbol,
                            'side': 'SELL',
                            'entry': current_price,
                            'method': 'breaker_block',
                            'strength': 8,
                            'confidence': confidence,
                            'timeframe': timeframe,
                            'method_details': breaker['details']
                        })
        
        # Sort by confidence
        all_signals.sort(key=lambda x: x['confidence'], reverse=True)
        
        if all_signals:
            best_signal = all_signals[0]
            
            # Calculate TP/SL
            sl, tp = calculate_tp_sl(best_signal, df, atr_val)
            
            if sl and tp:
                best_signal['sl'] = sl
                best_signal['tp'] = tp
                
                # Calculate metrics
                risk = abs(best_signal['entry'] - sl)
                reward = abs(tp - best_signal['entry'])
                rr = reward / risk if risk > 0 else 0
                risk_pct = (risk / best_signal['entry']) * 100 if best_signal['entry'] > 0 else 0
                reward_pct = (reward / best_signal['entry']) * 100 if best_signal['entry'] > 0 else 0
                
                best_signal['rr_ratio'] = rr
                best_signal['risk_pct'] = risk_pct
                best_signal['reward_pct'] = reward_pct
                
                # Generate breakdown
                best_signal['numeric_breakdown'] = generate_numeric_breakdown(best_signal, df)
                
                return best_signal
        
        return None
    except Exception as e:
        log.warning(f"Analysis error: {e}")
        return None

# ---------------- SIGNAL LOGGING ----------------
async def log_signal(sig: Dict):
    """Log signal to database"""
    try:
        async with db_lock:
            await db_conn.execute("""
                INSERT INTO signals (
                    symbol, side, entry, sl, tp, status, method, method_details,
                    strength, confidence, timeframe, zone_high, zone_low,
                    rr_ratio, risk_pct, reward_pct, numeric_breakdown
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sig['symbol'],
                sig['side'],
                sig['entry'],
                sig['sl'],
                sig['tp'],
                'OPEN',
                sig['method'],
                sig.get('method_details', ''),
                sig.get('strength', 0),
                sig.get('confidence', 0),
                sig['timeframe'],
                sig.get('zone_high'),
                sig.get('zone_low'),
                sig.get('rr_ratio', 0),
                sig.get('risk_pct', 0),
                sig.get('reward_pct', 0),
                sig.get('numeric_breakdown', '')
            ))
            await db_conn.commit()
    except Exception as e:
        log.error(f"Log error: {e}")

# ---------------- SIGNAL ALERT ----------------
async def send_signal_alert(sig: Dict):
    """Send signal alert to Telegram"""
    try:
        rr = sig.get('rr_ratio', 0)
        confidence = sig.get('confidence', 0) * 100
        
        message = f"""
🔔 **5-METHOD SCANNER** 🔔

{sig['symbol']} | {sig['timeframe']}
{sig['side']} via {sig['method']}

Entry: {sig['entry']:.6f}
SL: {sig['sl']:.6f}
TP: {sig['tp']:.6f}
R:R: {rr:.2f}:1

Strength: {sig.get('strength', 0)}
Confidence: {confidence:.1f}%

{sig.get('method_details', '')}
"""
        await tg(message)
    except Exception as e:
        log.error(f"Alert error: {e}")

# ---------------- SCAN LOOP ----------------
last_signal_time = {}

async def scan_loop(exchange):
    """Main scanning loop"""
    while True:
        start_time = time.time()
        
        try:
            # Fetch tickers
            tickers = await exchange.fetch_tickers()
            usdt_pairs = []
            
            for symbol, data in tickers.items():
                if symbol.endswith("/USDT") or symbol.endswith("USDT"):
                    volume = data.get("quoteVolume", 0)
                    if volume > 0:
                        usdt_pairs.append((symbol, volume))
            
            # Sort and take top N
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            top_pairs = usdt_pairs[:TOP_N]
            
            signals_found = 0
            
            for symbol, volume in top_pairs:
                # Normalize symbol
                if not "/" in symbol and symbol.endswith("USDT"):
                    symbol = symbol.replace("USDT", "/USDT")
                
                for timeframe in TIMEFRAMES:
                    # Rate limiting
                    key = f"{symbol}:{timeframe}"
                    if key in last_signal_time:
                        if time.time() - last_signal_time[key] < 300:
                            continue
                    
                    # Fetch data
                    ohlcv = await fetch_ohlcv(exchange, symbol, timeframe, 200)
                    if not ohlcv or len(ohlcv) < 100:
                        continue
                    
                    # Create DataFrame
                    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                    for col in ["open", "high", "low", "close", "volume"]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    
                    # Analyze
                    signal = analyze_all_methods(df, symbol, timeframe)
                    
                    if signal:
                        # Send alert
                        await send_signal_alert(signal)
                        
                        # Log to database
                        await log_signal(signal)
                        
                        # Update timing
                        last_signal_time[key] = time.time()
                        signals_found += 1
                        
                        log.info(f"Signal: {symbol} {timeframe} {signal['side']} {signal['method']}")
            
            log.info(f"Scan: {signals_found} signals")
            
        except Exception as e:
            log.exception(f"Scan error: {e}")
        
        # Wait
        elapsed = time.time() - start_time
        sleep_time = max(10, SCAN_INTERVAL - elapsed)
        await asyncio.sleep(sleep_time)

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    """Monitor open signals for TP/SL hits"""
    while True:
        try:
            async with db_lock:
                # Get open signals
                async with db_conn.execute("""
                    SELECT id, symbol, side, entry, sl, tp, tp_hit, sl_hit, timeframe, method 
                    FROM signals WHERE status='OPEN'
                """) as cursor:
                    signals = await cursor.fetchall()
                
                for sig in signals:
                    sig_id, symbol, side, entry, sl, tp, tp_hit, sl_hit, timeframe, method = sig
                    
                    # Get price
                    try:
                        ticker = await exchange.fetch_ticker(symbol)
                        current_price = ticker.get("last")
                        
                        if current_price is None:
                            continue
                        
                        update_needed = False
                        new_tp_hit = tp_hit
                        new_sl_hit = sl_hit
                        new_status = 'OPEN'
                        
                        if side == "BUY":
                            if not tp_hit and current_price >= tp:
                                new_tp_hit = 1
                                update_needed = True
                                profit = current_price - entry
                                await tg(f"✅ TP HIT: {symbol}\nProfit: {profit:.6f}")
                            
                            if not sl_hit and current_price <= sl:
                                new_sl_hit = 1
                                new_status = 'CLOSED'
                                update_needed = True
                                loss = entry - current_price
                                await tg(f"❌ SL HIT: {symbol}\nLoss: {loss:.6f}")
                        
                        else:
                            if not tp_hit and current_price <= tp:
                                new_tp_hit = 1
                                update_needed = True
                                profit = entry - current_price
                                await tg(f"✅ TP HIT: {symbol}\nProfit: {profit:.6f}")
                            
                            if not sl_hit and current_price >= sl:
                                new_sl_hit = 1
                                new_status = 'CLOSED'
                                update_needed = True
                                loss = current_price - entry
                                await tg(f"❌ SL HIT: {symbol}\nLoss: {loss:.6f}")
                        
                        if update_needed:
                            await db_conn.execute("""
                                UPDATE signals SET tp_hit=?, sl_hit=?, status=? WHERE id=?
                            """, (new_tp_hit, new_sl_hit, new_status, sig_id))
                    
                    except Exception as e:
                        log.error(f"Monitor error {symbol}: {e}")
                
                await db_conn.commit()
                
        except Exception as e:
            log.exception(f"Monitor error: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/")
async def root():
    return {"status": "running", "scanner": "5-Method"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/signals")
async def get_signals(limit: int = 20, status: str = "OPEN"):
    try:
        async with db_lock:
            async with db_conn.execute("""
                SELECT * FROM signals WHERE status=? ORDER BY timestamp DESC LIMIT ?
            """, (status, limit)) as cursor:
                rows = await cursor.fetchall()
                columns = [description[0] for description in cursor.description]
        
        signals = [dict(zip(columns, row)) for row in rows]
        return {"count": len(signals), "signals": signals}
    except Exception as e:
        return {"error": str(e)}

@app.post("/webhook")
async def webhook(request: Request):
    token = request.headers.get("X-Auth", "")
    if token != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    
    data = await request.json()
    return {"ok": True}

# ---------------- MAIN ----------------
async def main():
    global exchange, db_conn
    
    log.info("Starting scanner...")
    
    try:
        # Initialize database
        if not await init_db():
            log.error("Failed to initialize database")
            return
        
        # Initialize exchange
        exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"}
        })
        
        # Test connection
        await exchange.fetch_ticker("BTC/USDT")
        
        # Startup message
        await tg(f"""
🚀 5-METHOD SCANNER STARTED
Methods: Supply/Demand, FVG, Order Blocks, Liquidity Grab, Breaker Blocks
Timeframes: {', '.join(TIMEFRAMES)}
Confidence: {MIN_CONFIDENCE*100}% min
        """)
        
        # Start tasks
        await asyncio.gather(
            scan_loop(exchange),
            monitor_signals()
        )
        
    except KeyboardInterrupt:
        log.info("Stopped by user")
    except Exception as e:
        log.exception(f"Fatal error: {e}")
    finally:
        # Cleanup
        if db_conn:
            await db_conn.close()
        if exchange:
            await exchange.close()
        log.info("Scanner stopped")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", help="Run HTTP server")
    parser.add_argument("--port", type=int, default=9000, help="HTTP port")
    args = parser.parse_args()
    
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=args.port)
    else:
        asyncio.run(main())