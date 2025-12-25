#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ULTRA-FOCUSED 5-METHOD SCANNER WITH FULL NUMERIC BREAKDOWN
- Supply/Demand Zones with cluster analysis
- FVG (Fair Value Gaps) with gap metrics
- Order Blocks with strength calculation
- Liquidity Grab with sweep measurements
- Breaker Blocks with structure analysis
- COMPLETE NUMERIC BREAKDOWN FOR EVERY SIGNAL
"""

import os, time, asyncio, logging, datetime, json, math
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
MIN_CONFIDENCE = 0.1  # Minimum confidence score

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("ultra_scanner_v2")
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
        );
    """)
    
    # Create indexes for faster queries
    await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol_timeframe ON signals(symbol, timeframe);")
    await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON signals(status);")
    await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON signals(timestamp);")
    
    await db_conn.commit()

# ---------------- OHLCV ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 300) -> Optional[List]:
    """Fetch OHLCV data with error handling"""
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
    
    high = df['high']
    low = df['low']
    close = df['close']
    
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period, min_periods=period).mean()
    
    return float(atr.iloc[-1]) if pd.notna(atr.iloc[-1]) else 0.0

def calculate_price_stats(df: pd.DataFrame, lookback: int = 20) -> Dict:
    """Calculate price statistics"""
    if len(df) < lookback:
        lookback = len(df)
    
    recent = df.iloc[-lookback:]
    
    return {
        'high': float(recent['high'].max()),
        'low': float(recent['low'].min()),
        'close': float(recent['close'].iloc[-1]),
        'range_pct': float((recent['high'].max() - recent['low'].min()) / recent['low'].min() * 100),
        'volatility': float(recent['high'].std() / recent['close'].mean() * 100) if len(recent) > 1 else 0.0
    }

# ================ 5 CORE METHODS WITH NUMERIC BREAKDOWN ================

# 1. SUPPLY/DEMAND ZONES
def find_supply_demand_zones(df: pd.DataFrame, lookback: int = 100) -> List[Dict]:
    """Find supply/demand zones with numeric analysis"""
    zones = []
    
    if len(df) < 20:
        return zones
    
    # Calculate ATR for normalization
    atr_val = calculate_atr(df, 14)
    if atr_val == 0:
        return zones
    
    # Get price data
    prices = df.iloc[-lookback:]
    highs = prices['high'].values
    lows = prices['low'].values
    
    # Find price clusters using histogram approach
    price_range = max(highs.max(), lows.max()) - min(highs.min(), lows.min())
    num_bins = max(10, min(50, len(prices) // 10))
    
    if price_range == 0:
        return zones
    
    # Create bins for clustering
    bins = np.linspace(min(lows.min(), highs.min()), 
                      max(lows.max(), highs.max()), num_bins)
    
    # Analyze highs for supply zones
    high_counts, _ = np.histogram(highs, bins=bins)
    for i in range(len(high_counts) - 1):
        if high_counts[i] >= MIN_ZONE_SIZE:
            zone_low = bins[i]
            zone_high = bins[i + 1]
            zone_mid = (zone_low + zone_high) / 2
            
            # Calculate zone strength metrics
            candles_in_zone = sum(1 for h in highs if zone_low <= h <= zone_high)
            zone_density = candles_in_zone / len(prices) * 100
            
            # Check if price was rejected from this zone
            price_after = df.iloc[-10:]['close'] if len(df) > 10 else df['close']
            rejections = sum(1 for price in price_after 
                           if price < zone_mid and any(abs(h - zone_mid) < atr_val * 0.1 
                                                      for h in highs[-5:]))
            
            if candles_in_zone >= MIN_ZONE_SIZE:
                zones.append({
                    'type': 'SUPPLY',
                    'price': float(zone_mid),
                    'high': float(zone_high),
                    'low': float(zone_low),
                    'strength': int(candles_in_zone),
                    'density_pct': float(zone_density),
                    'atr_multiple': float((zone_high - zone_low) / atr_val),
                    'rejections': rejections,
                    'method': 'supply_demand',
                    'details': f"Cluster: {candles_in_zone} candles, Density: {zone_density:.1f}%, ATR: {(zone_high-zone_low)/atr_val:.2f}x"
                })
    
    # Analyze lows for demand zones
    low_counts, _ = np.histogram(lows, bins=bins)
    for i in range(len(low_counts) - 1):
        if low_counts[i] >= MIN_ZONE_SIZE:
            zone_low = bins[i]
            zone_high = bins[i + 1]
            zone_mid = (zone_low + zone_high) / 2
            
            # Calculate zone strength metrics
            candles_in_zone = sum(1 for l in lows if zone_low <= l <= zone_high)
            zone_density = candles_in_zone / len(prices) * 100
            
            # Check if price bounced from this zone
            price_after = df.iloc[-10:]['close'] if len(df) > 10 else df['close']
            bounces = sum(1 for price in price_after 
                         if price > zone_mid and any(abs(l - zone_mid) < atr_val * 0.1 
                                                    for l in lows[-5:]))
            
            if candles_in_zone >= MIN_ZONE_SIZE:
                zones.append({
                    'type': 'DEMAND',
                    'price': float(zone_mid),
                    'high': float(zone_high),
                    'low': float(zone_low),
                    'strength': int(candles_in_zone),
                    'density_pct': float(zone_density),
                    'atr_multiple': float((zone_high - zone_low) / atr_val),
                    'bounces': bounces,
                    'method': 'supply_demand',
                    'details': f"Cluster: {candles_in_zone} candles, Density: {zone_density:.1f}%, ATR: {(zone_high-zone_low)/atr_val:.2f}x"
                })
    
    return zones

# 2. FVG (FAIR VALUE GAPS)
def find_fvg(df: pd.DataFrame, lookback: int = 50) -> List[Dict]:
    """Find Fair Value Gaps with gap analysis"""
    fvgs = []
    
    if len(df) < 3:
        return fvgs
    
    atr_val = calculate_atr(df, 14)
    if atr_val == 0:
        return fvgs
    
    for i in range(2, min(lookback, len(df))):
        prev_candle = df.iloc[-i-1]
        curr_candle = df.iloc[-i]
        
        # Bullish FVG: Previous high < current low
        if prev_candle['high'] < curr_candle['low']:
            gap_size = curr_candle['low'] - prev_candle['high']
            gap_size_atr = gap_size / atr_val
            
            if gap_size_atr > 0.5:  # Minimum gap size
                # Calculate fill percentage if gap is being filled
                current_price = df['close'].iloc[-1]
                fill_pct = 0
                if current_price <= curr_candle['low'] and current_price >= prev_candle['high']:
                    fill_pct = ((current_price - prev_candle['high']) / gap_size) * 100
                
                fvgs.append({
                    'type': 'BULLISH_FVG',
                    'gap_top': float(curr_candle['low']),
                    'gap_bottom': float(prev_candle['high']),
                    'gap_size': float(gap_size),
                    'gap_size_atr': float(gap_size_atr),
                    'fill_percentage': float(fill_pct),
                    'candle_index': -i,
                    'method': 'fvg',
                    'details': f"Gap: {gap_size:.6f} ({gap_size_atr:.2f}ATR), Fill: {fill_pct:.1f}%"
                })
        
        # Bearish FVG: Previous low > current high
        elif prev_candle['low'] > curr_candle['high']:
            gap_size = prev_candle['low'] - curr_candle['high']
            gap_size_atr = gap_size / atr_val
            
            if gap_size_atr > 0.5:
                current_price = df['close'].iloc[-1]
                fill_pct = 0
                if current_price <= prev_candle['low'] and current_price >= curr_candle['high']:
                    fill_pct = ((prev_candle['low'] - current_price) / gap_size) * 100
                
                fvgs.append({
                    'type': 'BEARISH_FVG',
                    'gap_top': float(prev_candle['low']),
                    'gap_bottom': float(curr_candle['high']),
                    'gap_size': float(gap_size),
                    'gap_size_atr': float(gap_size_atr),
                    'fill_percentage': float(fill_pct),
                    'candle_index': -i,
                    'method': 'fvg',
                    'details': f"Gap: {gap_size:.6f} ({gap_size_atr:.2f}ATR), Fill: {fill_pct:.1f}%"
                })
    
    # Sort by gap size (largest first)
    fvgs.sort(key=lambda x: x['gap_size'], reverse=True)
    return fvgs[:5]

# 3. ORDER BLOCKS
def find_order_blocks(df: pd.DataFrame, lookback: int = 50) -> List[Dict]:
    """Find Order Blocks with momentum analysis"""
    order_blocks = []
    
    if len(df) < 3:
        return order_blocks
    
    atr_val = calculate_atr(df, 14)
    
    for i in range(2, min(lookback, len(df))):
        candle1 = df.iloc[-i-1]
        candle2 = df.iloc[-i]
        
        # Calculate candle metrics
        candle1_size = candle1['high'] - candle1['low']
        candle2_size = candle2['high'] - candle2['low']
        candle1_body = abs(candle1['close'] - candle1['open'])
        candle2_body = abs(candle2['close'] - candle2['open'])
        
        if candle1_size == 0 or candle2_size == 0:
            continue
        
        candle1_body_ratio = candle1_body / candle1_size
        candle2_body_ratio = candle2_body / candle2_size
        
        # Bullish Order Block: Strong bear then strong bull
        if (candle1['close'] < candle1['open'] and  # Bear candle
            candle2['close'] > candle2['open'] and  # Bull candle
            candle1_body_ratio > 0.7 and  # Strong bear
            candle2_body_ratio > 0.7):    # Strong bull
            
            block_low = min(candle1['low'], candle2['low'])
            block_high = max(candle1['high'], candle2['high'])
            block_size = block_high - block_low
            block_mid = (block_low + block_high) / 2
            
            # Calculate block strength
            momentum = (candle2['close'] - candle1['open']) / atr_val if atr_val > 0 else 0
            volume_ratio = candle2['vol'] / candle1['vol'] if candle1['vol'] > 0 else 1
            
            order_blocks.append({
                'type': 'BULLISH_OB',
                'low': float(block_low),
                'high': float(block_high),
                'mid': float(block_mid),
                'size': float(block_size),
                'size_atr': float(block_size / atr_val) if atr_val > 0 else 0,
                'momentum': float(momentum),
                'volume_ratio': float(volume_ratio),
                'candle1_body': float(candle1_body_ratio),
                'candle2_body': float(candle2_body_ratio),
                'candle_indices': [-i-1, -i],
                'method': 'order_block',
                'details': f"Size: {block_size:.6f} ({block_size/atr_val:.2f}ATR), Momentum: {momentum:.2f}ATR, Vol Ratio: {volume_ratio:.2f}x"
            })
        
        # Bearish Order Block: Strong bull then strong bear
        elif (candle1['close'] > candle1['open'] and  # Bull candle
              candle2['close'] < candle2['open'] and  # Bear candle
              candle1_body_ratio > 0.7 and  # Strong bull
              candle2_body_ratio > 0.7):    # Strong bear
            
            block_low = min(candle1['low'], candle2['low'])
            block_high = max(candle1['high'], candle2['high'])
            block_size = block_high - block_low
            block_mid = (block_low + block_high) / 2
            
            # Calculate block strength
            momentum = (candle1['open'] - candle2['close']) / atr_val if atr_val > 0 else 0
            volume_ratio = candle2['vol'] / candle1['vol'] if candle1['vol'] > 0 else 1
            
            order_blocks.append({
                'type': 'BEARISH_OB',
                'low': float(block_low),
                'high': float(block_high),
                'mid': float(block_mid),
                'size': float(block_size),
                'size_atr': float(block_size / atr_val) if atr_val > 0 else 0,
                'momentum': float(momentum),
                'volume_ratio': float(volume_ratio),
                'candle1_body': float(candle1_body_ratio),
                'candle2_body': float(candle2_body_ratio),
                'candle_indices': [-i-1, -i],
                'method': 'order_block',
                'details': f"Size: {block_size:.6f} ({block_size/atr_val:.2f}ATR), Momentum: {momentum:.2f}ATR, Vol Ratio: {volume_ratio:.2f}x"
            })
    
    return order_blocks

# 4. LIQUIDITY GRAB
def find_liquidity_grab(df: pd.DataFrame, lookback: int = 30) -> List[Dict]:
    """Find Liquidity Grabs with sweep analysis"""
    liquidity_grabs = []
    
    if len(df) < 10:
        return liquidity_grabs
    
    atr_val = calculate_atr(df, 14)
    
    for i in range(3, min(lookback, len(df))):
        swing_candle = df.iloc[-i]
        prev_candle = df.iloc[-i-1]
        
        # Check previous 5 candles for swing low/high context
        if i + 5 < len(df):
            prev_lows = df['low'].iloc[-i-5:-i].values
            prev_highs = df['high'].iloc[-i-5:-i].values
        else:
            prev_lows = df['low'].iloc[:].values
            prev_highs = df['high'].iloc[:].values
        
        # Bullish Liquidity Grab (sweep lows then reversal)
        if (swing_candle['low'] < min(prev_lows) and  # Sweeps previous lows
            swing_candle['low'] < prev_candle['low']):  # Lower low
            
            # Check for reversal in next candles
            if i > 1:
                next_candle = df.iloc[-i+1]
                reversal_strength = (next_candle['close'] - swing_candle['low']) / atr_val if atr_val > 0 else 0
                
                if reversal_strength > 0.5:  # Minimum reversal strength
                    sweep_depth = (min(prev_lows) - swing_candle['low']) / atr_val if atr_val > 0 else 0
                    
                    liquidity_grabs.append({
                        'type': 'BULLISH_LG',
                        'sweep_price': float(swing_candle['low']),
                        'previous_low': float(min(prev_lows)),
                        'reversal_price': float(next_candle['close']),
                        'sweep_depth_atr': float(sweep_depth),
                        'reversal_strength_atr': float(reversal_strength),
                        'candle_index': -i,
                        'method': 'liquidity_grab',
                        'details': f"Sweep: {sweep_depth:.2f}ATR below prev low, Reversal: {reversal_strength:.2f}ATR"
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
                        'candle_index': -i,
                        'method': 'liquidity_grab',
                        'details': f"Sweep: {sweep_depth:.2f}ATR above prev high, Reversal: {reversal_strength:.2f}ATR"
                    })
    
    return liquidity_grabs

# 5. BREAKER BLOCKS
def find_breaker_blocks(df: pd.DataFrame, lookback: int = 50) -> List[Dict]:
    """Find Breaker Blocks with structure analysis"""
    breaker_blocks = []
    
    if len(df) < 20:
        return breaker_blocks
    
    atr_val = calculate_atr(df, 14)
    
    # Find swing highs and lows
    swing_highs = []
    swing_lows = []
    
    for i in range(2, len(df) - 1):
        if i >= lookback:
            break
            
        high = df['high'].iloc[-i]
        low = df['low'].iloc[-i]
        
        # Check for swing high
        if (high > df['high'].iloc[-i-1] and 
            high > df['high'].iloc[-i+1]):
            swing_highs.append({
                'price': float(high),
                'index': -i,
                'candle': df.iloc[-i]
            })
        
        # Check for swing low
        if (low < df['low'].iloc[-i-1] and 
            low < df['low'].iloc[-i+1]):
            swing_lows.append({
                'price': float(low),
                'index': -i,
                'candle': df.iloc[-i]
            })
    
    # Analyze breaker blocks
    current_price = df['close'].iloc[-1]
    
    # Bullish Breaker: Failed breakdown of swing low
    for swing_low in swing_lows[:5]:  # Check most recent 5 swing lows
        breakdown_candles = []
        
        # Find candles that broke below the swing low
        for i in range(1, min(10, abs(swing_low['index']))):
            idx = swing_low['index'] + i
            if idx >= 0:
                candle = df.iloc[idx]
                if candle['low'] < swing_low['price']:
                    breakdown_candles.append({
                        'candle': candle,
                        'break_depth': swing_low['price'] - candle['low'],
                        'close_above': candle['close'] > swing_low['price']
                    })
        
        if breakdown_candles:
            # Check if any breakdown candle closed above the swing low (failed breakdown)
            for breakdown in breakdown_candles:
                if breakdown['close_above']:
                    break_depth_atr = breakdown['break_depth'] / atr_val if atr_val > 0 else 0
                    
                    breaker_blocks.append({
                        'type': 'BULLISH_BREAKER',
                        'structure_price': float(swing_low['price']),
                        'break_price': float(breakdown['candle']['low']),
                        'break_depth_atr': float(break_depth_atr),
                        'recovery_pct': float((breakdown['candle']['close'] - breakdown['candle']['low']) / 
                                            (swing_low['price'] - breakdown['candle']['low']) * 100),
                        'candle_index': swing_low['index'],
                        'method': 'breaker_block',
                        'details': f"Break: {breakdown['break_depth']:.6f} ({break_depth_atr:.2f}ATR), Recovery: {((breakdown['candle']['close'] - breakdown['candle']['low']) / (swing_low['price'] - breakdown['candle']['low']) * 100):.1f}%"
                    })
                    break
    
    # Bearish Breaker: Failed breakout of swing high
    for swing_high in swing_highs[:5]:  # Check most recent 5 swing highs
        breakout_candles = []
        
        # Find candles that broke above the swing high
        for i in range(1, min(10, abs(swing_high['index']))):
            idx = swing_high['index'] + i
            if idx >= 0:
                candle = df.iloc[idx]
                if candle['high'] > swing_high['price']:
                    breakout_candles.append({
                        'candle': candle,
                        'break_depth': candle['high'] - swing_high['price'],
                        'close_below': candle['close'] < swing_high['price']
                    })
        
        if breakout_candles:
            # Check if any breakout candle closed below the swing high (failed breakout)
            for breakout in breakout_candles:
                if breakout['close_below']:
                    break_depth_atr = breakout['break_depth'] / atr_val if atr_val > 0 else 0
                    
                    breaker_blocks.append({
                        'type': 'BEARISH_BREAKER',
                        'structure_price': float(swing_high['price']),
                        'break_price': float(breakout['candle']['high']),
                        'break_depth_atr': float(break_depth_atr),
                        'rejection_pct': float((breakout['candle']['high'] - breakout['candle']['close']) / 
                                             (breakout['candle']['high'] - swing_high['price']) * 100),
                        'candle_index': swing_high['index'],
                        'method': 'breaker_block',
                        'details': f"Break: {breakout['break_depth']:.6f} ({break_depth_atr:.2f}ATR), Rejection: {((breakout['candle']['high'] - breakout['candle']['close']) / (breakout['candle']['high'] - swing_high['price']) * 100):.1f}%"
                    })
                    break
    
    return breaker_blocks

# ================ SIGNAL GENERATION WITH NUMERIC BREAKDOWN ================

def generate_numeric_breakdown(signal: Dict, df: pd.DataFrame) -> str:
    """Generate comprehensive numeric breakdown for signal"""
    breakdown = []
    
    # Price statistics
    stats = calculate_price_stats(df, 20)
    atr_val = calculate_atr(df, 14)
    
    breakdown.append("=== PRICE STATISTICS ===")
    breakdown.append(f"Current Price: {signal['entry']:.6f}")
    breakdown.append(f"Recent High: {stats['high']:.6f} (Δ: {(stats['high'] - signal['entry']):.6f})")
    breakdown.append(f"Recent Low: {stats['low']:.6f} (Δ: {(signal['entry'] - stats['low']):.6f})")
    breakdown.append(f"20-bar Range: {stats['range_pct']:.2f}%")
    breakdown.append(f"Volatility: {stats['volatility']:.2f}%")
    breakdown.append(f"ATR(14): {atr_val:.6f}")
    
    breakdown.append("\n=== TRADE PARAMETERS ===")
    risk_abs = abs(signal['entry'] - signal['sl'])
    reward_abs = abs(signal['tp'] - signal['entry'])
    rr_ratio = reward_abs / risk_abs if risk_abs > 0 else 0
    risk_pct = (risk_abs / signal['entry']) * 100
    reward_pct = (reward_abs / signal['entry']) * 100
    
    breakdown.append(f"Entry: {signal['entry']:.6f}")
    breakdown.append(f"SL: {signal['sl']:.6f} (Risk: {risk_abs:.6f} = {risk_pct:.2f}%)")
    breakdown.append(f"TP: {signal['tp']:.6f} (Reward: {reward_abs:.6f} = {reward_pct:.2f}%)")
    breakdown.append(f"R:R Ratio: {rr_ratio:.2f}:1")
    breakdown.append(f"Required Win Rate: {1/(1+rr_ratio)*100:.1f}% for break-even")
    
    # Method-specific breakdown
    breakdown.append(f"\n=== METHOD: {signal['method']} ===")
    
    if signal['method'] == 'supply_demand':
        zone_size = signal.get('zone_high', 0) - signal.get('zone_low', 0)
        breakdown.append(f"Zone Range: {signal.get('zone_low', 0):.6f} - {signal.get('zone_high', 0):.6f}")
        breakdown.append(f"Zone Width: {zone_size:.6f} ({zone_size/atr_val:.2f} ATR)")
        breakdown.append(f"Zone Strength: {signal.get('strength', 0)} candles")
        breakdown.append(f"Distance to Zone: {abs(signal['entry'] - signal.get('zone_mid', signal['entry'])):.6f}")
        
    elif signal['method'] == 'fvg':
        gap_size = signal.get('gap_size', 0)
        breakdown.append(f"Gap Range: {signal.get('gap_bottom', 0):.6f} - {signal.get('gap_top', 0):.6f}")
        breakdown.append(f"Gap Size: {gap_size:.6f} ({gap_size/atr_val:.2f} ATR)")
        breakdown.append(f"Fill %: {signal.get('fill_percentage', 0):.1f}%")
        
    elif signal['method'] == 'order_block':
        block_size = signal.get('size', 0)
        breakdown.append(f"Block Range: {signal.get('low', 0):.6f} - {signal.get('high', 0):.6f}")
        breakdown.append(f"Block Size: {block_size:.6f} ({block_size/atr_val:.2f} ATR)")
        breakdown.append(f"Momentum: {signal.get('momentum', 0):.2f} ATR")
        breakdown.append(f"Volume Ratio: {signal.get('volume_ratio', 0):.2f}x")
        
    elif signal['method'] == 'liquidity_grab':
        breakdown.append(f"Sweep Price: {signal.get('sweep_price', 0):.6f}")
        breakdown.append(f"Sweep Depth: {signal.get('sweep_depth_atr', 0):.2f} ATR")
        breakdown.append(f"Reversal Strength: {signal.get('reversal_strength_atr', 0):.2f} ATR")
        
    elif signal['method'] == 'breaker_block':
        break_depth = abs(signal.get('break_price', 0) - signal.get('structure_price', 0))
        breakdown.append(f"Structure Price: {signal.get('structure_price', 0):.6f}")
        breakdown.append(f"Break Price: {signal.get('break_price', 0):.6f}")
        breakdown.append(f"Break Depth: {break_depth:.6f} ({break_depth/atr_val:.2f} ATR)")
        breakdown.append(f"Recovery/Rejection: {signal.get('recovery_pct', signal.get('rejection_pct', 0)):.1f}%")
    
    # Risk management
    breakdown.append("\n=== RISK MANAGEMENT ===")
    breakdown.append(f"Position Size Calc: Risk% / ({risk_pct:.2f}% Stop Loss)")
    breakdown.append(f"Example: 2% risk = 2 / {risk_pct:.2f} = {(2/risk_pct*100 if risk_pct > 0 else 0):.1f}% of capital")
    
    # Time analysis
    current_time = datetime.datetime.utcnow()
    breakdown.append(f"\n=== TIME ANALYSIS ===")
    breakdown.append(f"Signal Time: {current_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    breakdown.append(f"TF: {signal['timeframe']}")
    
    return "\n".join(breakdown)

def analyze_all_methods(df: pd.DataFrame, symbol: str, timeframe: str) -> Optional[Dict]:
    """Run all 5 methods and generate signals with numeric breakdown"""
    if len(df) < 100:
        return None
    
    # Run all detection methods
    zones = find_supply_demand_zones(df)
    fvgs = find_fvg(df)
    obs = find_order_blocks(df)
    lgs = find_liquidity_grab(df)
    breakers = find_breaker_blocks(df)
    
    # Combine all signals
    all_signals = []
    
    # Process zones
    current_price = df['close'].iloc[-1]
    atr_val = calculate_atr(df, 14)
    
    for zone in zones:
        distance = abs(current_price - zone['price']) / zone['price'] * 100
        
        if zone['type'] == 'DEMAND' and distance < 2.5:  # Within 2.5% of demand zone
            signal = {
                'symbol': symbol,
                'side': 'BUY',
                'entry': current_price,
                'method': 'supply_demand',
                'strength': zone['strength'],
                'confidence': min(0.95, zone['strength'] / 10 + zone['density_pct'] / 100),
                'timeframe': timeframe,
                'zone_high': zone['high'],
                'zone_low': zone['low'],
                'zone_mid': zone['price'],
                **{k: v for k, v in zone.items() if k not in ['type', 'method', 'details']}
            }
            all_signals.append(signal)
            
        elif zone['type'] == 'SUPPLY' and distance < 2.5:  # Within 2.5% of supply zone
            signal = {
                'symbol': symbol,
                'side': 'SELL',
                'entry': current_price,
                'method': 'supply_demand',
                'strength': zone['strength'],
                'confidence': min(0.95, zone['strength'] / 10 + zone['density_pct'] / 100),
                'timeframe': timeframe,
                'zone_high': zone['high'],
                'zone_low': zone['low'],
                'zone_mid': zone['price'],
                **{k: v for k, v in zone.items() if k not in ['type', 'method', 'details']}
            }
            all_signals.append(signal)
    
    # Process FVGs
    for fvg in fvgs:
        if fvg['type'] == 'BULLISH_FVG' and fvg['gap_bottom'] <= current_price <= fvg['gap_top']:
            signal = {
                'symbol': symbol,
                'side': 'BUY',
                'entry': current_price,
                'method': 'fvg',
                'strength': int(fvg['gap_size_atr'] * 10),
                'confidence': min(0.9, fvg['gap_size_atr'] * 0.5 + (100 - fvg['fill_percentage']) / 200),
                'timeframe': timeframe,
                **{k: v for k, v in fvg.items() if k not in ['type', 'method', 'details']}
            }
            all_signals.append(signal)
            
        elif fvg['type'] == 'BEARISH_FVG' and fvg['gap_bottom'] <= current_price <= fvg['gap_top']:
            signal = {
                'symbol': symbol,
                'side': 'SELL',
                'entry': current_price,
                'method': 'fvg',
                'strength': int(fvg['gap_size_atr'] * 10),
                'confidence': min(0.9, fvg['gap_size_atr'] * 0.5 + (100 - fvg['fill_percentage']) / 200),
                'timeframe': timeframe,
                **{k: v for k, v in fvg.items() if k not in ['type', 'method', 'details']}
            }
            all_signals.append(signal)
    
    # Process Order Blocks
    for ob in obs:
        if ob['type'] == 'BULLISH_OB' and ob['low'] <= current_price <= ob['high']:
            signal = {
                'symbol': symbol,
                'side': 'BUY',
                'entry': current_price,
                'method': 'order_block',
                'strength': int(ob['momentum'] * 5 + ob['volume_ratio'] * 2),
                'confidence': min(0.85, ob['candle1_body'] * 0.4 + ob['candle2_body'] * 0.4 + min(ob['volume_ratio'], 2) * 0.1),
                'timeframe': timeframe,
                **{k: v for k, v in ob.items() if k not in ['type', 'method', 'details']}
            }
            all_signals.append(signal)
            
        elif ob['type'] == 'BEARISH_OB' and ob['low'] <= current_price <= ob['high']:
            signal = {
                'symbol': symbol,
                'side': 'SELL',
                'entry': current_price,
                'method': 'order_block',
                'strength': int(ob['momentum'] * 5 + ob['volume_ratio'] * 2),
                'confidence': min(0.85, ob['candle1_body'] * 0.4 + ob['candle2_body'] * 0.4 + min(ob['volume_ratio'], 2) * 0.1),
                'timeframe': timeframe,
                **{k: v for k, v in ob.items() if k not in ['type', 'method', 'details']}
            }
            all_signals.append(signal)
    
    # Process Liquidity Grabs
    for lg in lgs:
        if lg['type'] == 'BULLISH_LG' and abs(lg['candle_index']) <= 10:  # Recent (last 10 candles)
            distance = abs(current_price - lg['sweep_price']) / lg['sweep_price'] * 100
            if distance < 5:  # Within 5% of sweep
                signal = {
                    'symbol': symbol,
                    'side': 'BUY',
                    'entry': current_price,
                    'method': 'liquidity_grab',
                    'strength': int(lg['reversal_strength_atr'] * 10),
                    'confidence': min(0.8, lg['reversal_strength_atr'] * 0.4 + (1 - distance/20)),
                    'timeframe': timeframe,
                    **{k: v for k, v in lg.items() if k not in ['type', 'method', 'details']}
                }
                all_signals.append(signal)
                
        elif lg['type'] == 'BEARISH_LG' and abs(lg['candle_index']) <= 10:
            distance = abs(current_price - lg['sweep_price']) / lg['sweep_price'] * 100
            if distance < 5:
                signal = {
                    'symbol': symbol,
                    'side': 'SELL',
                    'entry': current_price,
                    'method': 'liquidity_grab',
                    'strength': int(lg['reversal_strength_atr'] * 10),
                    'confidence': min(0.8, lg['reversal_strength_atr'] * 0.4 + (1 - distance/20)),
                    'timeframe': timeframe,
                    **{k: v for k, v in lg.items() if k not in ['type', 'method', 'details']}
                }
                all_signals.append(signal)
    
    # Process Breaker Blocks
    for breaker in breakers:
        if breaker['type'] == 'BULLISH_BREAKER':
            distance = abs(current_price - breaker['structure_price']) / breaker['structure_price'] * 100
            if distance < 3:  # Close to structure
                signal = {
                    'symbol': symbol,
                    'side': 'BUY',
                    'entry': current_price,
                    'method': 'breaker_block',
                    'strength': 8,
                    'confidence': min(0.75, 0.6 + (3 - distance) / 15),
                    'timeframe': timeframe,
                    **{k: v for k, v in breaker.items() if k not in ['type', 'method', 'details']}
                }
                all_signals.append(signal)
                
        elif breaker['type'] == 'BEARISH_BREAKER':
            distance = abs(current_price - breaker['structure_price']) / breaker['structure_price'] * 100
            if distance < 3:
                signal = {
                    'symbol': symbol,
                    'side': 'SELL',
                    'entry': current_price,
                    'method': 'breaker_block',
                    'strength': 8,
                    'confidence': min(0.75, 0.6 + (3 - distance) / 15),
                    'timeframe': timeframe,
                    **{k: v for k, v in breaker.items() if k not in ['type', 'method', 'details']}
                }
                all_signals.append(signal)
    
    # Sort by confidence * strength
    all_signals.sort(key=lambda x: x['confidence'] * (x['strength'] / 10), reverse=True)
    
    # Process best signal
    if all_signals and all_signals[0]['confidence'] >= MIN_CONFIDENCE:
        best_signal = all_signals[0]
        
        # Calculate TP/SL
        sl, tp = calculate_tp_sl(best_signal, df, atr_val)
        
        if sl and tp:
            # Add TP/SL to signal
            best_signal['sl'] = sl
            best_signal['tp'] = tp
            
            # Calculate RR
            risk_abs = abs(best_signal['entry'] - sl)
            reward_abs = abs(tp - best_signal['entry'])
            rr_ratio = reward_abs / risk_abs if risk_abs > 0 else 0
            risk_pct = (risk_abs / best_signal['entry']) * 100
            reward_pct = (reward_abs / best_signal['entry']) * 100
            
            best_signal['rr_ratio'] = rr_ratio
            best_signal['risk_pct'] = risk_pct
            best_signal['reward_pct'] = reward_pct
            
            # Generate numeric breakdown
            numeric_breakdown = generate_numeric_breakdown(best_signal, df)
            best_signal['numeric_breakdown'] = numeric_breakdown
            
            # Add method details
            if best_signal['method'] == 'supply_demand':
                for zone in zones:
                    if (zone['type'] == ('DEMAND' if best_signal['side'] == 'BUY' else 'SUPPLY') and
                        abs(zone['price'] - best_signal.get('zone_mid', 0)) < 0.000001):
                        best_signal['method_details'] = zone['details']
                        break
            elif best_signal['method'] == 'fvg':
                for fvg in fvgs:
                    if fvg['type'] == ('BULLISH_FVG' if best_signal['side'] == 'BUY' else 'BEARISH_FVG'):
                        best_signal['method_details'] = fvg['details']
                        break
            elif best_signal['method'] == 'order_block':
                for ob in obs:
                    if ob['type'] == ('BULLISH_OB' if best_signal['side'] == 'BUY' else 'BEARISH_OB'):
                        best_signal['method_details'] = ob['details']
                        break
            elif best_signal['method'] == 'liquidity_grab':
                for lg in lgs:
                    if lg['type'] == ('BULLISH_LG' if best_signal['side'] == 'BUY' else 'BEARISH_LG'):
                        best_signal['method_details'] = lg['details']
                        break
            elif best_signal['method'] == 'breaker_block':
                for breaker in breakers:
                    if breaker['type'] == ('BULLISH_BREAKER' if best_signal['side'] == 'BUY' else 'BEARISH_BREAKER'):
                        best_signal['method_details'] = breaker['details']
                        break
            
            return best_signal
    
    return None

def calculate_tp_sl(signal: Dict, df: pd.DataFrame, atr_val: float) -> Tuple[Optional[float], Optional[float]]:
    """Calculate TP and SL based on signal type"""
    entry = signal['entry']
    side = signal['side']
    method = signal['method']
    
    if atr_val == 0:
        return None, None
    
    # Base SL at 1.5 ATR
    if side == 'BUY':
        base_sl = entry - (atr_val * 1.5)
    else:
        base_sl = entry + (atr_val * 1.5)
    
    # Method-specific adjustments
    if method == 'supply_demand':
        if side == 'BUY':
            # SL below demand zone
            zone_low = signal.get('zone_low', base_sl)
            sl = min(base_sl, zone_low - (atr_val * 0.5))
            # TP at next supply or 2:1 RR
            tp = entry + (2 * (entry - sl))
        else:  # SELL
            # SL above supply zone
            zone_high = signal.get('zone_high', base_sl)
            sl = max(base_sl, zone_high + (atr_val * 0.5))
            # TP at next demand or 2:1 RR
            tp = entry - (2 * (sl - entry))
    
    elif method == 'fvg':
        if side == 'BUY':
            # SL below FVG
            gap_bottom = signal.get('gap_bottom', base_sl)
            sl = min(base_sl, gap_bottom - (atr_val * 0.3))
            # TP at top of FVG or 2.5:1 RR
            gap_top = signal.get('gap_top', entry + (2.5 * (entry - sl)))
            tp = min(gap_top, entry + (2.5 * (entry - sl)))
        else:  # SELL
            # SL above FVG
            gap_top = signal.get('gap_top', base_sl)
            sl = max(base_sl, gap_top + (atr_val * 0.3))
            # TP at bottom of FVG or 2.5:1 RR
            gap_bottom = signal.get('gap_bottom', entry - (2.5 * (sl - entry)))
            tp = max(gap_bottom, entry - (2.5 * (sl - entry)))
    
    elif method == 'order_block':
        if side == 'BUY':
            # SL below order block
            block_low = signal.get('low', base_sl)
            sl = min(base_sl, block_low - (atr_val * 0.5))
            # TP at block high or 2:1 RR
            block_high = signal.get('high', entry + (2 * (entry - sl)))
            tp = min(block_high, entry + (2 * (entry - sl)))
        else:  # SELL
            # SL above order block
            block_high = signal.get('high', base_sl)
            sl = max(base_sl, block_high + (atr_val * 0.5))
            # TP at block low or 2:1 RR
            block_low = signal.get('low', entry - (2 * (sl - entry)))
            tp = max(block_low, entry - (2 * (sl - entry)))
    
    elif method == 'liquidity_grab':
        if side == 'BUY':
            # SL below sweep low
            sweep_price = signal.get('sweep_price', base_sl)
            sl = min(base_sl, sweep_price - (atr_val * 0.2))
            # TP at recent high or 3:1 RR
            recent_high = df['high'].iloc[-20:].max()
            tp = min(recent_high, entry + (3 * (entry - sl)))
        else:  # SELL
            # SL above sweep high
            sweep_price = signal.get('sweep_price', base_sl)
            sl = max(base_sl, sweep_price + (atr_val * 0.2))
            # TP at recent low or 3:1 RR
            recent_low = df['low'].iloc[-20:].min()
            tp = max(recent_low, entry - (3 * (sl - entry)))
    
    elif method == 'breaker_block':
        if side == 'BUY':
            # SL below structure
            structure_price = signal.get('structure_price', base_sl)
            sl = min(base_sl, structure_price - (atr_val * 0.5))
            # TP at previous resistance or 2:1 RR
            tp = entry + (2 * (entry - sl))
        else:  # SELL
            # SL above structure
            structure_price = signal.get('structure_price', base_sl)
            sl = max(base_sl, structure_price + (atr_val * 0.5))
            # TP at previous support or 2:1 RR
            tp = entry - (2 * (sl - entry))
    
    else:
        # Default 2:1 RR
        if side == 'BUY':
            sl = base_sl
            tp = entry + (2 * (entry - sl))
        else:
            sl = base_sl
            tp = entry - (2 * (sl - entry))
    
    # Ensure minimum RR of 1.5:1
    if side == 'BUY':
        min_tp = entry + (1.5 * (entry - sl))
        tp = max(tp, min_tp)
    else:
        min_tp = entry - (1.5 * (sl - entry))
        tp = min(tp, min_tp)
    
    # Ensure valid values
    if side == 'BUY':
        if sl >= entry or tp <= entry:
            return None, None
    else:
        if sl <= entry or tp >= entry:
            return None, None
    
    return sl, tp

# ---------------- SIGNAL LOGGING ----------------
async def log_signal(sig: Dict):
    """Log signal to database"""
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

# ---------------- TELEGRAM FORMATTING ----------------
async def send_signal_alert(sig: Dict):
    """Send formatted signal alert to Telegram"""
    rr = sig.get('rr_ratio', 0)
    confidence = sig.get('confidence', 0) * 100
    
    message = f"""
🔔 **5-METHOD SCANNER SIGNAL** 🔔

🏷️ **{sig['symbol']}** | {sig['timeframe']}
📈 **{sig['side']}** via {sig['method'].upper().replace('_', ' ')}

💰 **TRADE SETUP**
Entry: `{sig['entry']:.6f}`
SL: `{sig['sl']:.6f}` (Risk: {sig.get('risk_pct', 0):.2f}%)
TP: `{sig['tp']:.6f}` (Reward: {sig.get('reward_pct', 0):.2f}%)
R:R: `{rr:.2f}:1`

📊 **SIGNAL STRENGTH**
Strength: {sig.get('strength', 0)}/10
Confidence: {confidence:.1f}%

🔍 **METHOD DETAILS**
{sig.get('method_details', 'No details')}

💎 **BREAKDOWN AVAILABLE**
Full numeric breakdown saved in database.

⏰ Time: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}
"""
    
    await tg(message)

# ---------------- SCAN LOOP ----------------
last_signal_time = {}
async def scan_loop(exchange):
    """Main scanning loop"""
    while True:
        start_time = time.time()
        
        try:
            # Fetch top volume pairs
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, v.get("quoteVolume", 0)) 
                         for s, v in tickers.items() 
                         if s.endswith("/USDT") or s.endswith("USDT")]
            
            # Sort by volume
            top_pairs = sorted(usdt_pairs, key=lambda x: x[1], reverse=True)[:TOP_N]
            
            signals_found = 0
            
            for symbol, volume in top_pairs:
                # Normalize symbol format
                if not "/" in symbol and symbol.endswith("USDT"):
                    symbol = symbol.replace("USDT", "/USDT")
                
                for timeframe in TIMEFRAMES:
                    # Rate limiting
                    key = f"{symbol}:{timeframe}"
                    if (key in last_signal_time and 
                        time.time() - last_signal_time[key] < 300):  # 5 min cooldown
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
                        # Send alert and log signal
                        await send_signal_alert(signal)
                        await log_signal(signal)
                        
                        # Update last signal time
                        last_signal_time[key] = time.time()
                        signals_found += 1
                        
                        log.info(f"✅ Signal found: {symbol} {timeframe} {signal['side']} via {signal['method']}")
            
            log.info(f"📊 Scan completed in {time.time() - start_time:.1f}s. Found {signals_found} signals.")
            
        except Exception as e:
            log.exception(f"Scan error: {e}")
        
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
                        
                        # Check for TP/SL hits
                        update_fields = {}
                        
                        if side == "BUY":
                            if not tp_hit and current_price >= tp:
                                update_fields['tp_hit'] = 1
                                profit = current_price - entry
                                profit_pct = (profit / entry) * 100
                                await tg(f"🎯 TP HIT: {symbol} ({timeframe})\nEntry: {entry:.6f} → TP: {tp:.6f}\nProfit: {profit:.6f} ({profit_pct:.2f}%)")
                            
                            if not sl_hit and current_price <= sl:
                                update_fields['sl_hit'] = 1
                                update_fields['status'] = 'CLOSED'
                                loss = entry - current_price
                                loss_pct = (loss / entry) * 100
                                await tg(f"🛑 SL HIT: {symbol} ({timeframe})\nEntry: {entry:.6f} → SL: {sl:.6f}\nLoss: {loss:.6f} ({loss_pct:.2f}%)")
                        
                        else:  # SELL
                            if not tp_hit and current_price <= tp:
                                update_fields['tp_hit'] = 1
                                profit = entry - current_price
                                profit_pct = (profit / entry) * 100
                                await tg(f"🎯 TP HIT: {symbol} ({timeframe})\nEntry: {entry:.6f} → TP: {tp:.6f}\nProfit: {profit:.6f} ({profit_pct:.2f}%)")
                            
                            if not sl_hit and current_price >= sl:
                                update_fields['sl_hit'] = 1
                                update_fields['status'] = 'CLOSED'
                                loss = current_price - entry
                                loss_pct = (loss / entry) * 100
                                await tg(f"🛑 SL HIT: {symbol} ({timeframe})\nEntry: {entry:.6f} → SL: {sl:.6f}\nLoss: {loss:.6f} ({loss_pct:.2f}%)")
                        
                        # Update database if needed
                        if update_fields:
                            set_clause = ', '.join([f"{k}=?" for k in update_fields.keys()])
                            values = list(update_fields.values()) + [sig_id]
                            await db_conn.execute(f"UPDATE signals SET {set_clause} WHERE id=?", values)
                
                await db_conn.commit()
                
        except Exception as e:
            log.exception(f"Monitor error: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- FASTAPI ----------------
app = FastAPI(title="5-Method Scanner API")

@app.get("/")
async def root():
    return {
        "status": "running",
        "scanner": "5-Method Price Action Scanner",
        "methods": ["supply_demand", "fvg", "order_block", "liquidity_grab", "breaker_block"],
        "timeframes": TIMEFRAMES
    }

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.datetime.utcnow().isoformat()}

@app.get("/signals")
async def get_signals(
    limit: int = 20, 
    status: str = "OPEN",
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None
):
    """Get signals from database"""
    query = "SELECT * FROM signals WHERE status = ?"
    params = [status]
    
    if symbol:
        query += " AND symbol = ?"
        params.append(symbol)
    
    if timeframe:
        query += " AND timeframe = ?"
        params.append(timeframe)
    
    query += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)
    
    async with db_lock:
        async with db_conn.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            columns = [description[0] for description in cursor.description]
    
    signals = []
    for row in rows:
        signal = dict(zip(columns, row))
        # Parse numeric breakdown if it exists
        if signal.get('numeric_breakdown'):
            try:
                signal['numeric_breakdown_parsed'] = signal['numeric_breakdown'].split('\n')
            except:
                signal['numeric_breakdown_parsed'] = []
        signals.append(signal)
    
    return {"count": len(signals), "signals": signals}

@app.get("/signal/{signal_id}")
async def get_signal(signal_id: int):
    """Get specific signal by ID"""
    async with db_lock:
        async with db_conn.execute("SELECT * FROM signals WHERE id = ?", (signal_id,)) as cursor:
            row = await cursor.fetchone()
    
    if not row:
        raise HTTPException(status_code=404, detail="Signal not found")
    
    columns = [description[0] for description in cursor.description]
    signal = dict(zip(columns, row))
    
    # Parse numeric breakdown
    if signal.get('numeric_breakdown'):
        try:
            signal['numeric_breakdown_parsed'] = signal['numeric_breakdown'].split('\n')
        except:
            signal['numeric_breakdown_parsed'] = []
    
    return signal

@app.post("/webhook")
async def webhook(request: Request):
    """Webhook endpoint for external triggers"""
    token = request.headers.get("X-Auth", "")
    if token != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    
    data = await request.json()
    log.info(f"Webhook received: {data}")
    
    # You can add webhook processing logic here
    return {"ok": True, "message": "Webhook received", "data": data}

# ---------------- MAIN ----------------
async def main():
    """Main application entry point"""
    global exchange
    
    # Initialize database
    await init_db()
    
    # Initialize exchange
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"}
    })
    
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
• Confidence scoring
• TP/SL alerts
• Database tracking

💎 **Philosophy:** Pure price action, no indicators
"""
    
    await tg(startup_msg)
    log.info("Scanner started successfully")
    
    # Run scanner and monitor concurrently
    await asyncio.gather(
        scan_loop(exchange),
        monitor_signals()
    )

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="5-Method Price Action Scanner")
    parser.add_argument("--http", action="store_true", help="Run HTTP API server")
    parser.add_argument("--port", type=int, default=9000, help="HTTP server port")
    
    args = parser.parse_args()
    
    if args.http:
        # Run HTTP server
        uvicorn.run(app, host="0.0.0.0", port=args.port)
    else:
        # Run scanner
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Scanner stopped by user")
        except Exception as e:
            log.exception(f"Fatal error: {e}")
        finally:
            # Cleanup
            if db_conn:
                asyncio.run(db_conn.close())
            if exchange:
                asyncio.run(exchange.close())