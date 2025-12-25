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
log = logging.getLogger("scanner_v4")
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
async def init_db():
    """Initialize database"""
    global db_conn
    try:
        db_conn = await aiosqlite.connect(DB_PATH)
        await db_conn.execute("PRAGMA journal_mode=WAL;")
        await db_conn.execute("PRAGMA synchronous=NORMAL;")
        
        # Create signals table
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
        
        # Create indexes
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol_timeframe ON signals(symbol, timeframe);")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON signals(status);")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON signals(timestamp);")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_method ON signals(method);")
        
        await db_conn.commit()
        log.info("Database initialized successfully")
    except Exception as e:
        log.error(f"Database initialization error: {e}")
        raise

# ---------------- OHLCV FETCH ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 300) -> Optional[List]:
    """Fetch OHLCV data"""
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except ccxt.NetworkError as e:
        log.warning(f"Network error fetching {symbol} {timeframe}: {e}")
        return None
    except ccxt.ExchangeError as e:
        log.warning(f"Exchange error fetching {symbol} {timeframe}: {e}")
        return None
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
        
        # Calculate range percentage
        if low > 0:
            range_pct = float((high - low) / low * 100)
        else:
            range_pct = 0.0
        
        # Calculate volatility
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
        
        # Use clustering to find zones
        price_range = max(highs.max(), lows.max()) - min(highs.min(), lows.min())
        if price_range == 0:
            return zones
        
        num_bins = max(10, min(30, len(prices) // 10))
        bins = np.linspace(min(lows.min(), highs.min()), max(lows.max(), highs.max()), num_bins)
        
        # Find supply zones (clusters of highs)
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
                    'details': f"Supply zone: {highs_in_bin} highs at {bin_low:.6f}-{bin_high:.6f}"
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
                    'details': f"Demand zone: {lows_in_bin} lows at {bin_low:.6f}-{bin_high:.6f}"
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
        if atr_val == 0:
            return fvgs
        
        for i in range(2, min(lookback, len(df))):
            prev_candle = df.iloc[-i-1]
            curr_candle = df.iloc[-i]
            
            # Bullish FVG (gap up)
            if prev_candle['high'] < curr_candle['low']:
                gap_size = curr_candle['low'] - prev_candle['high']
                gap_size_atr = gap_size / atr_val if atr_val > 0 else 0
                
                if gap_size_atr > 0.3:  # Minimum gap size
                    fvgs.append({
                        'type': 'BULLISH_FVG',
                        'gap_top': float(curr_candle['low']),
                        'gap_bottom': float(prev_candle['high']),
                        'gap_size': float(gap_size),
                        'gap_size_atr': float(gap_size_atr),
                        'method': 'fvg',
                        'details': f"Bullish FVG: {gap_size:.6f} ({gap_size_atr:.2f} ATR)"
                    })
            
            # Bearish FVG (gap down)
            elif prev_candle['low'] > curr_candle['high']:
                gap_size = prev_candle['low'] - curr_candle['high']
                gap_size_atr = gap_size / atr_val if atr_val > 0 else 0
                
                if gap_size_atr > 0.3:
                    fvgs.append({
                        'type': 'BEARISH_FVG',
                        'gap_top': float(prev_candle['low']),
                        'gap_bottom': float(curr_candle['high']),
                        'gap_size': float(gap_size),
                        'gap_size_atr': float(gap_size_atr),
                        'method': 'fvg',
                        'details': f"Bearish FVG: {gap_size:.6f} ({gap_size_atr:.2f} ATR)"
                    })
        
        return fvgs
    except Exception as e:
        log.warning(f"FVG finding error: {e}")
        return []

def find_order_blocks(df: pd.DataFrame, lookback: int = 50) -> List[Dict]:
    """Find Order Blocks"""
    order_blocks = []
    
    if len(df) < 3:
        return order_blocks
    
    try:
        atr_val = calculate_atr(df, 14)
        
        for i in range(2, min(lookback, len(df))):
            candle1 = df.iloc[-i-1]
            candle2 = df.iloc[-i]
            
            # Calculate candle metrics
            candle1_size = candle1['high'] - candle1['low']
            candle2_size = candle2['high'] - candle2['low']
            
            if candle1_size == 0 or candle2_size == 0:
                continue
            
            candle1_body = abs(candle1['close'] - candle1['open'])
            candle2_body = abs(candle2['close'] - candle2['open'])
            
            candle1_body_ratio = candle1_body / candle1_size
            candle2_body_ratio = candle2_body / candle2_size
            
            # Bullish Order Block (strong bear then strong bull)
            if (candle1['close'] < candle1['open'] and  # Bear candle
                candle2['close'] > candle2['open'] and  # Bull candle
                candle1_body_ratio > 0.6 and  # Reasonable strength
                candle2_body_ratio > 0.6):
                
                block_low = min(candle1['low'], candle2['low'])
                block_high = max(candle1['high'], candle2['high'])
                block_size = block_high - block_low
                block_size_atr = block_size / atr_val if atr_val > 0 else 0
                
                order_blocks.append({
                    'type': 'BULLISH_OB',
                    'low': float(block_low),
                    'high': float(block_high),
                    'size': float(block_size),
                    'size_atr': float(block_size_atr),
                    'method': 'order_block',
                    'details': f"Bullish OB: {block_size:.6f} ({block_size_atr:.2f} ATR)"
                })
            
            # Bearish Order Block (strong bull then strong bear)
            elif (candle1['close'] > candle1['open'] and  # Bull candle
                  candle2['close'] < candle2['open'] and  # Bear candle
                  candle1_body_ratio > 0.6 and
                  candle2_body_ratio > 0.6):
                
                block_low = min(candle1['low'], candle2['low'])
                block_high = max(candle1['high'], candle2['high'])
                block_size = block_high - block_low
                block_size_atr = block_size / atr_val if atr_val > 0 else 0
                
                order_blocks.append({
                    'type': 'BEARISH_OB',
                    'low': float(block_low),
                    'high': float(block_high),
                    'size': float(block_size),
                    'size_atr': float(block_size_atr),
                    'method': 'order_block',
                    'details': f"Bearish OB: {block_size:.6f} ({block_size_atr:.2f} ATR)"
                })
        
        return order_blocks
    except Exception as e:
        log.warning(f"Order blocks error: {e}")
        return []

def find_liquidity_grab(df: pd.DataFrame, lookback: int = 30) -> List[Dict]:
    """Find Liquidity Grabs"""
    liquidity_grabs = []
    
    if len(df) < 10:
        return liquidity_grabs
    
    try:
        atr_val = calculate_atr(df, 14)
        
        for i in range(3, min(lookback, len(df))):
            swing_candle = df.iloc[-i]
            prev_candle = df.iloc[-i-1]
            
            # Get previous extremes for context
            if i + 5 < len(df):
                prev_lows = df['low'].iloc[-i-5:-i].values
                prev_highs = df['high'].iloc[-i-5:-i].values
            else:
                prev_lows = df['low'].iloc[:].values
                prev_highs = df['high'].iloc[:].values
            
            # Bullish Liquidity Grab (sweep lows then reversal)
            if (swing_candle['low'] < min(prev_lows) and  # Sweeps previous lows
                swing_candle['low'] < prev_candle['low']):  # Lower low
                
                # Check for reversal in next candle
                if i > 1:
                    next_candle = df.iloc[-i+1]
                    reversal_strength = (next_candle['close'] - swing_candle['low']) / atr_val if atr_val > 0 else 0
                    
                    if reversal_strength > 0.5:  # Minimum reversal
                        sweep_depth = (min(prev_lows) - swing_candle['low']) / atr_val if atr_val > 0 else 0
                        
                        liquidity_grabs.append({
                            'type': 'BULLISH_LG',
                            'sweep_price': float(swing_candle['low']),
                            'previous_low': float(min(prev_lows)),
                            'reversal_price': float(next_candle['close']),
                            'sweep_depth_atr': float(sweep_depth),
                            'reversal_strength_atr': float(reversal_strength),
                            'method': 'liquidity_grab',
                            'details': f"Bullish LG: Sweep {sweep_depth:.2f} ATR, Reversal {reversal_strength:.2f} ATR"
                        })
            
            # Bearish Liquidity Grab (sweep highs then reversal)
            elif (swing_candle['high'] > max(prev_highs) and  # Sweeps previous highs
                  swing_candle['high'] > prev_candle['high']):  # Higher high
                
                if i > 1:
                    next_candle = df.iloc[-i+1]
                    reversal_strength = (swing_candle['high'] - next_candle['close']) / atr_val if atr_val > 0 else 0
                    
                    if reversal_strength > 0.5:
                        sweep_depth = (swing_candle['high'] - max(prev_highs)) / atr_val if atr_val > 0 else 0
                        
                        liquidity_grabs.append({
                            'type': 'BEARISH_LG',
                            'sweep_price': float(swing_candle['high']),
                            'previous_high': float(max(prev_highs)),
                            'reversal_price': float(next_candle['close']),
                            'sweep_depth_atr': float(sweep_depth),
                            'reversal_strength_atr': float(reversal_strength),
                            'method': 'liquidity_grab',
                            'details': f"Bearish LG: Sweep {sweep_depth:.2f} ATR, Reversal {reversal_strength:.2f} ATR"
                        })
        
        return liquidity_grabs
    except Exception as e:
        log.warning(f"Liquidity grab error: {e}")
        return []

def find_breaker_blocks(df: pd.DataFrame, lookback: int = 50) -> List[Dict]:
    """Find Breaker Blocks"""
    breaker_blocks = []
    
    if len(df) < 20:
        return breaker_blocks
    
    try:
        atr_val = calculate_atr(df, 14)
        
        # Find swing highs and lows
        swing_highs = []
        swing_lows = []
        
        for i in range(2, len(df) - 1):
            if i >= lookback:
                break
            
            # Check for swing high
            if (df['high'].iloc[-i] > df['high'].iloc[-i-1] and
                df['high'].iloc[-i] > df['high'].iloc[-i+1]):
                swing_highs.append({
                    'price': float(df['high'].iloc[-i]),
                    'index': -i
                })
            
            # Check for swing low
            if (df['low'].iloc[-i] < df['low'].iloc[-i-1] and
                df['low'].iloc[-i] < df['low'].iloc[-i+1]):
                swing_lows.append({
                    'price': float(df['low'].iloc[-i]),
                    'index': -i
                })
        
        # Analyze breaker blocks
        for swing_low in swing_lows[:5]:  # Check recent 5 swing lows
            # Look for breakdown and recovery
            for i in range(1, min(10, abs(swing_low['index']))):
                idx = swing_low['index'] + i
                if idx >= 0:
                    candle = df.iloc[idx]
                    if (candle['low'] < swing_low['price'] and  # Breaks below
                        candle['close'] > swing_low['price']):  # Closes above (recovery)
                        
                        break_depth = swing_low['price'] - candle['low']
                        break_depth_atr = break_depth / atr_val if atr_val > 0 else 0
                        
                        breaker_blocks.append({
                            'type': 'BULLISH_BREAKER',
                            'structure_price': float(swing_low['price']),
                            'break_price': float(candle['low']),
                            'break_depth_atr': float(break_depth_atr),
                            'method': 'breaker_block',
                            'details': f"Bullish Breaker: Break {break_depth:.6f} ({break_depth_atr:.2f} ATR)"
                        })
                        break
        
        for swing_high in swing_highs[:5]:  # Check recent 5 swing highs
            # Look for breakout and rejection
            for i in range(1, min(10, abs(swing_high['index']))):
                idx = swing_high['index'] + i
                if idx >= 0:
                    candle = df.iloc[idx]
                    if (candle['high'] > swing_high['price'] and  # Breaks above
                        candle['close'] < swing_high['price']):  # Closes below (rejection)
                        
                        break_depth = candle['high'] - swing_high['price']
                        break_depth_atr = break_depth / atr_val if atr_val > 0 else 0
                        
                        breaker_blocks.append({
                            'type': 'BEARISH_BREAKER',
                            'structure_price': float(swing_high['price']),
                            'break_price': float(candle['high']),
                            'break_depth_atr': float(break_depth_atr),
                            'method': 'breaker_block',
                            'details': f"Bearish Breaker: Break {break_depth:.6f} ({break_depth_atr:.2f} ATR)"
                        })
                        break
        
        return breaker_blocks
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
        breakdown.append(f"Current Price: {signal['entry']:.6f}")
        breakdown.append(f"Recent High: {stats['high']:.6f}")
        breakdown.append(f"Recent Low: {stats['low']:.6f}")
        breakdown.append(f"20-bar Range: {stats['range_pct']:.2f}%")
        breakdown.append(f"Volatility: {stats['volatility']:.2f}%")
        breakdown.append(f"ATR(14): {atr_val:.6f}")
        
        # Trade parameters
        risk_abs = abs(signal['entry'] - signal['sl'])
        reward_abs = abs(signal['tp'] - signal['entry'])
        rr_ratio = reward_abs / risk_abs if risk_abs > 0 else 0
        risk_pct = (risk_abs / signal['entry']) * 100 if signal['entry'] > 0 else 0
        reward_pct = (reward_abs / signal['entry']) * 100 if signal['entry'] > 0 else 0
        
        breakdown.append("\n=== TRADE PARAMETERS ===")
        breakdown.append(f"Entry: {signal['entry']:.6f}")
        breakdown.append(f"SL: {signal['sl']:.6f} (Risk: {risk_abs:.6f} = {risk_pct:.2f}%)")
        breakdown.append(f"TP: {signal['tp']:.6f} (Reward: {reward_abs:.6f} = {reward_pct:.2f}%)")
        breakdown.append(f"R:R Ratio: {rr_ratio:.2f}:1")
        
        if rr_ratio > 0:
            breakeven_winrate = 1 / (1 + rr_ratio) * 100
            breakdown.append(f"Breakeven Win Rate: {breakeven_winrate:.1f}%")
        
        # Signal info
        breakdown.append(f"\n=== SIGNAL INFO ===")
        breakdown.append(f"Method: {signal['method']}")
        breakdown.append(f"Side: {signal['side']}")
        breakdown.append(f"Timeframe: {signal['timeframe']}")
        breakdown.append(f"Strength: {signal.get('strength', 0)}/10")
        breakdown.append(f"Confidence: {signal.get('confidence', 0):.2f}")
        
        # Method-specific details
        method_details = signal.get('method_details', '')
        if method_details:
            breakdown.append(f"\n=== METHOD DETAILS ===")
            breakdown.append(method_details)
        
        # Risk management example
        breakdown.append(f"\n=== RISK MANAGEMENT ===")
        breakdown.append(f"Position Size Formula: Risk Amount / (Entry - SL)")
        breakdown.append(f"Example: $100 risk on {signal['symbol']}:")
        breakdown.append(f"  Position Size = $100 / {risk_abs:.6f} = {100/risk_abs if risk_abs > 0 else 0:.2f} units")
        
        return "\n".join(breakdown)
    except Exception as e:
        log.warning(f"Numeric breakdown error: {e}")
        return f"Error generating breakdown: {e}"

def calculate_tp_sl(signal: Dict, df: pd.DataFrame, atr_val: float) -> Tuple[Optional[float], Optional[float]]:
    """Calculate Take Profit and Stop Loss"""
    try:
        entry = signal['entry']
        side = signal['side']
        method = signal['method']
        
        if atr_val == 0:
            return None, None
        
        # Base SL calculation (1.5 ATR)
        if side == 'BUY':
            base_sl = entry - (atr_val * 1.5)
        else:
            base_sl = entry + (atr_val * 1.5)
        
        # Method-specific adjustments
        if method == 'supply_demand':
            if side == 'BUY':
                # For demand zones, SL below zone
                zone_low = signal.get('zone_low', base_sl)
                sl = min(base_sl, zone_low - (atr_val * 0.5))
                tp = entry + (2 * (entry - sl))  # 2:1 RR
            else:
                # For supply zones, SL above zone
                zone_high = signal.get('zone_high', base_sl)
                sl = max(base_sl, zone_high + (atr_val * 0.5))
                tp = entry - (2 * (sl - entry))  # 2:1 RR
        
        elif method == 'fvg':
            if side == 'BUY':
                # SL below FVG bottom
                gap_bottom = signal.get('gap_bottom', base_sl)
                sl = min(base_sl, gap_bottom - (atr_val * 0.3))
                tp = entry + (2.5 * (entry - sl))  # 2.5:1 RR
            else:
                # SL above FVG top
                gap_top = signal.get('gap_top', base_sl)
                sl = max(base_sl, gap_top + (atr_val * 0.3))
                tp = entry - (2.5 * (sl - entry))  # 2.5:1 RR
        
        elif method == 'order_block':
            if side == 'BUY':
                # SL below order block
                block_low = signal.get('low', base_sl)
                sl = min(base_sl, block_low - (atr_val * 0.5))
                tp = entry + (2 * (entry - sl))  # 2:1 RR
            else:
                # SL above order block
                block_high = signal.get('high', base_sl)
                sl = max(base_sl, block_high + (atr_val * 0.5))
                tp = entry - (2 * (sl - entry))  # 2:1 RR
        
        elif method == 'liquidity_grab':
            if side == 'BUY':
                # SL below sweep price
                sweep_price = signal.get('sweep_price', base_sl)
                sl = min(base_sl, sweep_price - (atr_val * 0.2))
                tp = entry + (3 * (entry - sl))  # 3:1 RR (strong reversal)
            else:
                # SL above sweep price
                sweep_price = signal.get('sweep_price', base_sl)
                sl = max(base_sl, sweep_price + (atr_val * 0.2))
                tp = entry - (3 * (sl - entry))  # 3:1 RR
        
        elif method == 'breaker_block':
            if side == 'BUY':
                # SL below structure
                structure_price = signal.get('structure_price', base_sl)
                sl = min(base_sl, structure_price - (atr_val * 0.5))
                tp = entry + (2 * (entry - sl))  # 2:1 RR
            else:
                # SL above structure
                structure_price = signal.get('structure_price', base_sl)
                sl = max(base_sl, structure_price + (atr_val * 0.5))
                tp = entry - (2 * (sl - entry))  # 2:1 RR
        
        else:
            # Default calculation
            if side == 'BUY':
                sl = base_sl
                tp = entry + (2 * (entry - sl))  # 2:1 RR
            else:
                sl = base_sl
                tp = entry - (2 * (sl - entry))  # 2:1 RR
        
        # Ensure minimum RR of 1.5:1
        if side == 'BUY':
            min_tp = entry + (1.5 * (entry - sl))
            tp = max(tp, min_tp)
        else:
            min_tp = entry - (1.5 * (sl - entry))
            tp = min(tp, min_tp)
        
        # Final validation
        if side == 'BUY':
            if sl >= entry or tp <= entry:
                return None, None
        else:
            if sl <= entry or tp >= entry:
                return None, None
        
        return sl, tp
    except Exception as e:
        log.warning(f"TP/SL calculation error: {e}")
        return None, None

def analyze_all_methods(df: pd.DataFrame, symbol: str, timeframe: str) -> Optional[Dict]:
    """Analyze all 5 methods and generate best signal"""
    if len(df) < 100:
        return None
    
    try:
        # Run all detection methods
        zones = find_supply_demand_zones(df)
        fvgs = find_fvg(df)
        obs = find_order_blocks(df)
        lgs = find_liquidity_grab(df)
        breakers = find_breaker_blocks(df)
        
        current_price = df['close'].iloc[-1]
        atr_val = calculate_atr(df, 14)
        
        all_signals = []
        
        # Process Supply/Demand Zones
        for zone in zones:
            distance = abs(current_price - zone['price']) / zone['price'] * 100
            
            if distance < 3:  # Within 3% of zone
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
                elif zone['type'] == 'SUPPLY':
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
                confidence = min(0.9, fvg['gap_size_atr'] * 0.4)
                if confidence >= MIN_CONFIDENCE:
                    all_signals.append({
                        'symbol': symbol,
                        'side': 'BUY',
                        'entry': current_price,
                        'method': 'fvg',
                        'strength': int(fvg['gap_size_atr'] * 8),
                        'confidence': confidence,
                        'timeframe': timeframe,
                        'gap_top': fvg['gap_top'],
                        'gap_bottom': fvg['gap_bottom'],
                        'method_details': fvg['details']
                    })
            
            elif fvg['type'] == 'BEARISH_FVG' and fvg['gap_bottom'] <= current_price <= fvg['gap_top']:
                confidence = min(0.9, fvg['gap_size_atr'] * 0.4)
                if confidence >= MIN_CONFIDENCE:
                    all_signals.append({
                        'symbol': symbol,
                        'side': 'SELL',
                        'entry': current_price,
                        'method': 'fvg',
                        'strength': int(fvg['gap_size_atr'] * 8),
                        'confidence': confidence,
                        'timeframe': timeframe,
                        'gap_top': fvg['gap_top'],
                        'gap_bottom': fvg['gap_bottom'],
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
                        'low': ob['low'],
                        'high': ob['high'],
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
                        'low': ob['low'],
                        'high': ob['high'],
                        'method_details': ob['details']
                    })
        
        # Process Liquidity Grabs
        for lg in lgs:
            if lg['type'] == 'BULLISH_LG':
                distance = abs(current_price - lg['sweep_price']) / current_price * 100
                if distance < 5:  # Within 5% of sweep
                    confidence = min(0.8, lg['reversal_strength_atr'] * 0.3)
                    if confidence >= MIN_CONFIDENCE:
                        all_signals.append({
                            'symbol': symbol,
                            'side': 'BUY',
                            'entry': current_price,
                            'method': 'liquidity_grab',
                            'strength': int(lg['reversal_strength_atr'] * 6),
                            'confidence': confidence,
                            'timeframe': timeframe,
                            'sweep_price': lg['sweep_price'],
                            'method_details': lg['details']
                        })
            
            elif lg['type'] == 'BEARISH_LG':
                distance = abs(current_price - lg['sweep_price']) / current_price * 100
                if distance < 5:
                    confidence = min(0.8, lg['reversal_strength_atr'] * 0.3)
                    if confidence >= MIN_CONFIDENCE:
                        all_signals.append({
                            'symbol': symbol,
                            'side': 'SELL',
                            'entry': current_price,
                            'method': 'liquidity_grab',
                            'strength': int(lg['reversal_strength_atr'] * 6),
                            'confidence': confidence,
                            'timeframe': timeframe,
                            'sweep_price': lg['sweep_price'],
                            'method_details': lg['details']
                        })
        
        # Process Breaker Blocks
        for breaker in breakers:
            if breaker['type'] == 'BULLISH_BREAKER':
                distance = abs(current_price - breaker['structure_price']) / current_price * 100
                if distance < 3:  # Close to structure
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
                            'structure_price': breaker['structure_price'],
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
                            'structure_price': breaker['structure_price'],
                            'method_details': breaker['details']
                        })
        
        # Sort by confidence * strength
        all_signals.sort(key=lambda x: x['confidence'] * (x['strength'] / 10), reverse=True)
        
        # Process best signal
        if all_signals:
            best_signal = all_signals[0]
            
            # Calculate TP/SL
            sl, tp = calculate_tp_sl(best_signal, df, atr_val)
            
            if sl and tp:
                # Add TP/SL to signal
                best_signal['sl'] = sl
                best_signal['tp'] = tp
                
                # Calculate risk/reward metrics
                risk_abs = abs(best_signal['entry'] - sl)
                reward_abs = abs(tp - best_signal['entry'])
                rr_ratio = reward_abs / risk_abs if risk_abs > 0 else 0
                risk_pct = (risk_abs / best_signal['entry']) * 100 if best_signal['entry'] > 0 else 0
                reward_pct = (reward_abs / best_signal['entry']) * 100 if best_signal['entry'] > 0 else 0
                
                best_signal['rr_ratio'] = rr_ratio
                best_signal['risk_pct'] = risk_pct
                best_signal['reward_pct'] = reward_pct
                
                # Generate numeric breakdown
                numeric_breakdown = generate_numeric_breakdown(best_signal, df)
                best_signal['numeric_breakdown'] = numeric_breakdown
                
                return best_signal
        
        return None
    except Exception as e:
        log.warning(f"Signal analysis error: {e}")
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
        log.error(f"Error logging signal: {e}")

# ---------------- SIGNAL ALERT ----------------
async def send_signal_alert(sig: Dict):
    """Send signal alert to Telegram"""
    try:
        rr = sig.get('rr_ratio', 0)
        confidence = sig.get('confidence', 0) * 100
        risk_pct = sig.get('risk_pct', 0)
        reward_pct = sig.get('reward_pct', 0)
        
        message = f"""
🔔 **5-METHOD SCANNER ALERT** 🔔

🏷️ **{sig['symbol']}** | {sig['timeframe']}
📈 **{sig['side']}** via {sig['method'].upper().replace('_', ' ')}

💰 **TRADE SETUP**
Entry: `{sig['entry']:.6f}`
SL: `{sig['sl']:.6f}` (Risk: {risk_pct:.2f}%)
TP: `{sig['tp']:.6f}` (Reward: {reward_pct:.2f}%)
R:R: `{rr:.2f}:1`

📊 **SIGNAL METRICS**
Strength: {sig.get('strength', 0)}/10
Confidence: {confidence:.1f}%

🔍 **METHOD DETAILS**
{sig.get('method_details', 'No additional details')}

💎 **NUMERIC BREAKDOWN**
Full breakdown saved in database.

⏰ Time: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}
"""
        await tg(message)
    except Exception as e:
        log.error(f"Error sending signal alert: {e}")

# ---------------- SCAN LOOP ----------------
last_signal_time = {}

async def scan_loop(exchange):
    """Main scanning loop"""
    while True:
        start_time = time.time()
        
        try:
            # Fetch top volume pairs
            tickers = await exchange.fetch_tickers()
            usdt_pairs = []
            
            for symbol, data in tickers.items():
                if symbol.endswith("/USDT") or symbol.endswith("USDT"):
                    volume = data.get("quoteVolume", 0)
                    if volume > 0:
                        usdt_pairs.append((symbol, volume))
            
            # Sort by volume and take top N
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            top_pairs = usdt_pairs[:TOP_N]
            
            signals_found = 0
            
            for symbol, volume in top_pairs:
                # Normalize symbol format
                if not "/" in symbol and symbol.endswith("USDT"):
                    symbol = symbol.replace("USDT", "/USDT")
                
                for timeframe in TIMEFRAMES:
                    # Rate limiting
                    key = f"{symbol}:{timeframe}"
                    if key in last_signal_time:
                        time_since_last = time.time() - last_signal_time[key]
                        if time_since_last < 300:  # 5 minute cooldown
                            continue
                    
                    # Fetch OHLCV data
                    ohlcv = await fetch_ohlcv(exchange, symbol, timeframe, 300)
                    if not ohlcv or len(ohlcv) < 100:
                        continue
                    
                    # Create DataFrame
                    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                    for col in ["open", "high", "low", "close", "volume"]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    
                    # Analyze for signals
                    signal = analyze_all_methods(df, symbol, timeframe)
                    
                    if signal:
                        # Send alert
                        await send_signal_alert(signal)
                        
                        # Log to database
                        await log_signal(signal)
                        
                        # Update last signal time
                        last_signal_time[key] = time.time()
                        signals_found += 1
                        
                        log.info(f"✅ Signal: {symbol} {timeframe} {signal['side']} via {signal['method']} (Confidence: {signal['confidence']:.2f})")
            
            # Log scan completion
            elapsed = time.time() - start_time
            log.info(f"📊 Scan completed in {elapsed:.1f}s. Found {signals_found} signals.")
            
        except Exception as e:
            log.exception(f"Scan loop error: {e}")
        
        # Wait for next scan
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
                    open_signals = await cursor.fetchall()
                
                for signal in open_signals:
                    sig_id, symbol, side, entry, sl, tp, tp_hit, sl_hit, timeframe, method = signal
                    
                    # Fetch current price
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
                                profit_pct = (profit / entry) * 100
                                await tg(f"🎯 TP HIT: {symbol} ({timeframe}) {method}\nEntry: {entry:.6f} → TP: {tp:.6f}\nProfit: {profit:.6f} ({profit_pct:.2f}%)")
                            
                            if not sl_hit and current_price <= sl:
                                new_sl_hit = 1
                                new_status = 'CLOSED'
                                update_needed = True
                                loss = entry - current_price
                                loss_pct = (loss / entry) * 100
                                await tg(f"🛑 SL HIT: {symbol} ({timeframe}) {method}\nEntry: {entry:.6f} → SL: {sl:.6f}\nLoss: {loss:.6f} ({loss_pct:.2f}%)")
                        
                        else:  # SELL
                            if not tp_hit and current_price <= tp:
                                new_tp_hit = 1
                                update_needed = True
                                profit = entry - current_price
                                profit_pct = (profit / entry) * 100
                                await tg(f"🎯 TP HIT: {symbol} ({timeframe}) {method}\nEntry: {entry:.6f} → TP: {tp:.6f}\nProfit: {profit:.6f} ({profit_pct:.2f}%)")
                            
                            if not sl_hit and current_price >= sl:
                                new_sl_hit = 1
                                new_status = 'CLOSED'
                                update_needed = True
                                loss = current_price - entry
                                loss_pct = (loss / entry) * 100
                                await tg(f"🛑 SL HIT: {symbol} ({timeframe}) {method}\nEntry: {entry:.6f} → SL: {sl:.6f}\nLoss: {loss:.6f} ({loss_pct:.2f}%)")
                        
                        # Update database if needed
                        if update_needed:
                            await db_conn.execute("""
                                UPDATE signals SET tp_hit=?, sl_hit=?, status=? WHERE id=?
                            """, (new_tp_hit, new_sl_hit, new_status, sig_id))
                    
                    except Exception as e:
                        log.error(f"Error monitoring {symbol}: {e}")
                        continue
                
                await db_conn.commit()
                
        except Exception as e:
            log.exception(f"Monitor error: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- FASTAPI ----------------
app = FastAPI(title="5-Method Scanner API", version="1.0.0")

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "status": "running",
        "service": "5-Method Price Action Scanner",
        "methods": ["supply_demand", "fvg", "order_block", "liquidity_grab", "breaker_block"],
        "timeframes": TIMEFRAMES,
        "min_confidence": MIN_CONFIDENCE,
        "version": "1.0.0"
    }

@app.get("/health")
async def health():
    """Health check endpoint"""
    try:
        # Check database connection
        if db_conn:
            async with db_conn.execute("SELECT 1") as cursor:
                await cursor.fetchone()
        
        # Check exchange connection
        if exchange:
            await exchange.fetch_ticker("BTC/USDT")
        
        return {
            "status": "healthy",
            "database": "connected",
            "exchange": "connected",
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.datetime.utcnow().isoformat()
        }

@app.get("/signals")
async def get_signals(
    limit: int = 20,
    status: str = "OPEN",
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
    method: Optional[str] = None
):
    """Get signals from database"""
    try:
        query = "SELECT * FROM signals WHERE status = ?"
        params = [status]
        
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        
        if timeframe:
            query += " AND timeframe = ?"
            params.append(timeframe)
        
        if method:
            query += " AND method = ?"
            params.append(method)
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        async with db_lock:
            async with db_conn.execute(query, params) as cursor:
                rows = await cursor.fetchall()
                columns = [description[0] for description in cursor.description]
        
        signals = []
        for row in rows:
            signal = dict(zip(columns, row))
            # Parse numeric breakdown for display
            if signal.get('numeric_breakdown'):
                signal['numeric_breakdown_lines'] = signal['numeric_breakdown'].split('\n')
            signals.append(signal)
        
        return {
            "count": len(signals),
            "signals": signals,
            "filters": {
                "status": status,
                "symbol": symbol,
                "timeframe": timeframe,
                "method": method,
                "limit": limit
            }
        }
    except Exception as e:
        log.error(f"API error in /signals: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/signal/{signal_id}")
async def get_signal(signal_id: int):
    """Get specific signal by ID"""
    try:
        async with db_lock:
            async with db_conn.execute("SELECT * FROM signals WHERE id = ?", (signal_id,)) as cursor:
                row = await cursor.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Signal not found")
        
        columns = [description[0] for description in cursor.description]
        signal = dict(zip(columns, row))
        
        # Parse numeric breakdown
        if signal.get('numeric_breakdown'):
            signal['numeric_breakdown_lines'] = signal['numeric_breakdown'].split('\n')
        
        return signal
    except Exception as e:
        log.error(f"API error in /signal/{signal_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_stats():
    """Get scanner statistics"""
    try:
        async with db_lock:
            # Total signals
            async with db_conn.execute("SELECT COUNT(*) FROM signals") as cursor:
                total_signals = (await cursor.fetchone())[0]
            
            # Open signals
            async with db_conn.execute("SELECT COUNT(*) FROM signals WHERE status = 'OPEN'") as cursor:
                open_signals = (await cursor.fetchone())[0]
            
            # By method
            async with db_conn.execute("SELECT method, COUNT(*) FROM signals GROUP BY method") as cursor:
                by_method = {row[0]: row[1] for row in await cursor.fetchall()}
            
            # By timeframe
            async with db_conn.execute("SELECT timeframe, COUNT(*) FROM signals GROUP BY timeframe") as cursor:
                by_timeframe = {row[0]: row[1] for row in await cursor.fetchall()}
            
            # TP/SL stats
            async with db_conn.execute("SELECT COUNT(*) FROM signals WHERE tp_hit = 1") as cursor:
                tp_hits = (await cursor.fetchone())[0]
            
            async with db_conn.execute("SELECT COUNT(*) FROM signals WHERE sl_hit = 1") as cursor:
                sl_hits = (await cursor.fetchone())[0]
        
        return {
            "total_signals": total_signals,
            "open_signals": open_signals,
            "tp_hits": tp_hits,
            "sl_hits": sl_hits,
            "by_method": by_method,
            "by_timeframe": by_timeframe,
            "min_confidence": MIN_CONFIDENCE,
            "scan_interval": SCAN_INTERVAL,
            "top_n": TOP_N
        }
    except Exception as e:
        log.error(f"API error in /stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/webhook")
async def webhook(request: Request):
    """Webhook endpoint for external triggers"""
    try:
        # Check authorization
        token = request.headers.get("X-Auth", "")
        if token != WEBHOOK_SECRET:
            raise HTTPException(status_code=403, detail="Invalid secret")
        
        # Parse data
        data = await request.json()
        log.info(f"Webhook received: {data}")
        
        # You can add webhook processing logic here
        return {
            "ok": True,
            "message": "Webhook received",
            "data": data,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
    except Exception as e:
        log.error(f"Webhook error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ---------------- MAIN ----------------
async def main():
    """Main application entry point"""
    global exchange, db_conn
    
    log.info("🚀 Starting 5-Method Price Action Scanner...")
    
    try:
        # Initialize database
        log.info("📊 Initializing database...")
        await init_db()
        
        # Initialize exchange
        log.info("💱 Initializing exchange connection...")
        exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
            "timeout": 30000
        })
        
        # Test exchange connection
        await exchange.fetch_ticker("BTC/USDT")
        log.info("✅ Exchange connection established")
        
        # Startup announcement
        startup_msg = f"""
🚀 **5-METHOD SCANNER STARTED** 🚀

🎯 **Methods Active:**
1. Supply/Demand Zones
2. Fair Value Gaps (FVG)
3. Order Blocks
4. Liquidity Grabs
5. Breaker Blocks

⏰ **Timeframes:** {', '.join(TIMEFRAMES)}
📊 **Top Pairs:** {TOP_N} by volume
🔄 **Scan Interval:** {SCAN_INTERVAL}s

📈 **Features:**
• Complete numeric breakdown for every signal
• ATR-based risk management
• Confidence scoring (min: {MIN_CONFIDENCE*100}%)
• TP/SL alerts with profit/loss calculation
• Database tracking with API access
• Webhook support

💎 **Philosophy:** Pure price action, no indicators
        """
        
        await tg(startup_msg)
        log.info("✅ Scanner started successfully")
        
        # Start scanner and monitor concurrently
        await asyncio.gather(
            scan_loop(exchange),
            monitor_signals()
        )
        
    except KeyboardInterrupt:
        log.info("👋 Scanner stopped by user")
    except Exception as e:
        log.exception(f"💥 Fatal error: {e}")
    finally:
        # Cleanup
        log.info("🧹 Cleaning up resources...")
        try:
            if db_conn:
                await db_conn.close()
                log.info("✅ Database connection closed")
        except Exception as e:
            log.error(f"Error closing database: {e}")
        
        try:
            if exchange:
                await exchange.close()
                log.info("✅ Exchange connection closed")
        except Exception as e:
            log.error(f"Error closing exchange: {e}")
        
        log.info("👋 Scanner shutdown complete")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="5-Method Price Action Scanner")
    parser.add_argument("--http", action="store_true", help="Run HTTP API server")
    parser.add_argument("--port", type=int, default=9000, help="HTTP server port")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="HTTP server host")
    
    args = parser.parse_args()
    
    if args.http:
        # Run HTTP server
        log.info(f"🌐 Starting HTTP server on {args.host}:{args.port}")
        uvicorn.run(
            app,
            host=args.host,
            port=args.port,
            log_level="info",
            access_log=True
        )
    else:
        # Run scanner
        asyncio.run(main())