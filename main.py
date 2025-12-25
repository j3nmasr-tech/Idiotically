#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ULTRA-FOCUSED 5-METHOD SCANNER - FRESH DATABASE VERSION
- Supply/Demand Zones, FVG, Order Blocks, Liquidity Grab, Breaker Blocks
- Timeframes: 15m, 30m, 1h, 2h, 3h, 4h
- MIN_CONFIDENCE = 0.1 for data collection
- FRESH DATABASE - WILL DELETE OLD DATA
- ADDED: Duplicate signal prevention system
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
TOP_N = int(os.getenv("TOP_N", 15))
TIMEFRAMES = ["15m", "30m", "1h", "2h", "3h", "4h"]
MIN_ZONE_SIZE = 3
MIN_CONFIDENCE = 0.1  # 10% for data collection
FRESH_DATABASE = True  # Set to True to delete old database

# Duplicate prevention settings
SIGNAL_COOLDOWN_MINUTES = 30  # Don't repeat same signal for 30 minutes
MIN_PRICE_MOVEMENT_PCT = 0.5  # Require at least 0.5% price movement
PRICE_BUCKET_SIZE_PCT = 0.1  # Group prices within 0.1% as duplicates

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("scanner_fresh")
db_lock = asyncio.Lock()
db_conn = None
exchange = None

# ---------------- DUPLICATE PREVENTION ----------------
processed_signals = set()  # Set of signal hashes
signal_cooldown = {}  # symbol:method:side -> timestamp
last_price_movement = {}  # symbol -> last checked price

def get_signal_hash(signal: Dict) -> str:
    """Create unique hash for signal to prevent duplicates"""
    # Create base string with key components
    base_str = f"{signal['symbol']}:{signal['method']}:{signal['side']}:{signal['timeframe']}"
    
    # Add price bucket (group similar prices together)
    price_bucket = int(signal['entry'] / (signal['entry'] * (PRICE_BUCKET_SIZE_PCT / 100)))
    base_str += f":{price_bucket}"
    
    # Add time bucket (group by 5-minute intervals)
    time_bucket = int(time.time() / (5 * 60))  # 5-minute buckets
    base_str += f":{time_bucket}"
    
    # Create hash
    return hashlib.md5(base_str.encode()).hexdigest()

def check_signal_cooldown(signal: Dict) -> bool:
    """Check if signal is in cooldown period"""
    key = f"{signal['symbol']}:{signal['method']}:{signal['side']}"
    current_time = time.time()
    
    if key in signal_cooldown:
        if current_time - signal_cooldown[key] < SIGNAL_COOLDOWN_MINUTES * 60:
            return True  # Still in cooldown
    
    # Update cooldown
    signal_cooldown[key] = current_time
    
    # Clean old cooldowns (older than 2 hours)
    cleanup_time = current_time - (120 * 60)  # 2 hours
    keys_to_remove = [k for k, t in signal_cooldown.items() if t < cleanup_time]
    for k in keys_to_remove:
        signal_cooldown.pop(k, None)
    
    return False  # Not in cooldown

def has_sufficient_price_movement(df: pd.DataFrame, symbol: str) -> bool:
    """Check if price has moved enough to justify a new signal"""
    if len(df) < 20:
        return False
    
    current_price = df['close'].iloc[-1]
    
    # Check price movement in last 20 candles
    recent_high = df['high'].iloc[-20:].max()
    recent_low = df['low'].iloc[-20:].min()
    
    if recent_low == 0:
        return False
    
    price_range_pct = ((recent_high - recent_low) / recent_low) * 100
    
    # Also check volatility (std dev of returns)
    returns = df['close'].pct_change().dropna()
    if len(returns) > 10:
        volatility = returns.std() * 100
        # Require either decent range or volatility
        if price_range_pct < MIN_PRICE_MOVEMENT_PCT and volatility < 0.3:
            return False
    
    return price_range_pct >= MIN_PRICE_MOVEMENT_PCT

def merge_similar_signals(signals: List[Dict]) -> List[Dict]:
    """Merge signals that are too similar"""
    if not signals:
        return []
    
    merged = []
    signals.sort(key=lambda x: x['confidence'], reverse=True)
    
    for signal in signals:
        is_similar = False
        
        for existing in merged:
            # Check if similar (same symbol, side, within price bucket)
            if (signal['symbol'] == existing['symbol'] and 
                signal['side'] == existing['side'] and
                abs(signal['entry'] - existing['entry']) / existing['entry'] < (PRICE_BUCKET_SIZE_PCT / 100)):
                
                is_similar = True
                # Keep the one with higher confidence
                if signal['confidence'] > existing['confidence']:
                    merged.remove(existing)
                    merged.append(signal)
                break
        
        if not is_similar:
            merged.append(signal)
    
    return merged

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
async def init_db_fresh():
    """Initialize fresh database - DROPS OLD TABLE!"""
    global db_conn
    try:
        # Remove old database file if fresh start requested
        if FRESH_DATABASE and os.path.exists(DB_PATH):
            log.warning("⚠️ DELETING old database for fresh start!")
            os.remove(DB_PATH)
        
        db_conn = await aiosqlite.connect(DB_PATH)
        await db_conn.execute("PRAGMA journal_mode=WAL;")
        await db_conn.execute("PRAGMA synchronous=NORMAL;")
        
        # DROP old table if exists
        await db_conn.execute("DROP TABLE IF EXISTS signals;")
        
        # Create FRESH table with ALL columns
        await db_conn.execute("""
            CREATE TABLE signals (
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
                numeric_breakdown TEXT,
                signal_hash TEXT UNIQUE
            )
        """)
        
        # Create indexes
        await db_conn.execute("CREATE INDEX idx_symbol_timeframe ON signals(symbol, timeframe);")
        await db_conn.execute("CREATE INDEX idx_status ON signals(status);")
        await db_conn.execute("CREATE INDEX idx_timestamp ON signals(timestamp);")
        await db_conn.execute("CREATE INDEX idx_method ON signals(method);")
        await db_conn.execute("CREATE INDEX idx_signal_hash ON signals(signal_hash);")
        
        await db_conn.commit()
        log.info("✅ Fresh database created with all columns")
        return True
    except Exception as e:
        log.error(f"Database creation error: {e}")
        if db_conn:
            await db_conn.close()
        return False

# ---------------- OHLCV FETCH ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 200) -> Optional[List]:
    """Fetch OHLCV data"""
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.debug(f"Error fetching {symbol} {timeframe}: {e}")
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
    except:
        return 0.0

# ================ 5 CORE METHODS - SIMPLIFIED ================

def find_supply_demand_zones(df: pd.DataFrame, lookback: int = 100) -> List[Dict]:
    """Find Supply/Demand Zones - SIMPLIFIED"""
    zones = []
    if len(df) < 20:
        return zones
    
    try:
        prices = df.iloc[-lookback:]
        current_price = df['close'].iloc[-1]
        
        # Look for demand zones (support)
        lows = prices['low'].values
        for i in range(len(lows) - 4):
            if lows[i] <= current_price <= lows[i] * 1.03:  # Within 3%
                cluster = lows[i:i+3]
                if max(cluster) - min(cluster) < current_price * 0.01:  # Tight cluster
                    zones.append({
                        'type': 'DEMAND',
                        'price': float(np.mean(cluster)),
                        'method': 'supply_demand',
                        'details': f"Demand zone near current price"
                    })
                    break
        
        # Look for supply zones (resistance)
        highs = prices['high'].values
        for i in range(len(highs) - 4):
            if highs[i] * 0.97 <= current_price <= highs[i]:  # Within 3%
                cluster = highs[i:i+3]
                if max(cluster) - min(cluster) < current_price * 0.01:
                    zones.append({
                        'type': 'SUPPLY',
                        'price': float(np.mean(cluster)),
                        'method': 'supply_demand',
                        'details': f"Supply zone near current price"
                    })
                    break
        
        return zones
    except:
        return []

def find_fvg(df: pd.DataFrame, lookback: int = 20) -> List[Dict]:
    """Find Fair Value Gaps - SIMPLIFIED"""
    fvgs = []
    if len(df) < 3:
        return fvgs
    
    try:
        current_price = df['close'].iloc[-1]
        
        # Check recent candles for gaps
        for i in range(1, min(lookback, len(df)-1)):
            prev = df.iloc[-i-1]
            curr = df.iloc[-i]
            
            # Bullish FVG
            if prev['high'] < curr['low']:
                if prev['high'] <= current_price <= curr['low']:
                    fvgs.append({
                        'type': 'BULLISH_FVG',
                        'method': 'fvg',
                        'details': f"Bullish FVG at {current_price:.6f}"
                    })
            
            # Bearish FVG
            elif prev['low'] > curr['high']:
                if curr['high'] <= current_price <= prev['low']:
                    fvgs.append({
                        'type': 'BEARISH_FVG',
                        'method': 'fvg',
                        'details': f"Bearish FVG at {current_price:.6f}"
                    })
        
        return fvgs[:3]  # Return max 3
    except:
        return []

def find_order_blocks(df: pd.DataFrame, lookback: int = 20) -> List[Dict]:
    """Find Order Blocks - SIMPLIFIED"""
    obs = []
    if len(df) < 3:
        return obs
    
    try:
        current_price = df['close'].iloc[-1]
        
        for i in range(1, min(lookback, len(df)-1)):
            candle1 = df.iloc[-i-1]
            candle2 = df.iloc[-i]
            
            # Bullish OB (bear then bull)
            if (candle1['close'] < candle1['open'] and  # Bear
                candle2['close'] > candle2['open']):    # Bull
                
                low_price = min(candle1['low'], candle2['low'])
                high_price = max(candle1['high'], candle2['high'])
                
                if low_price <= current_price <= high_price:
                    obs.append({
                        'type': 'BULLISH_OB',
                        'method': 'order_block',
                        'details': f"Bullish OB at {current_price:.6f}"
                    })
            
            # Bearish OB (bull then bear)
            elif (candle1['close'] > candle1['open'] and  # Bull
                  candle2['close'] < candle2['open']):    # Bear
                
                low_price = min(candle1['low'], candle2['low'])
                high_price = max(candle1['high'], candle2['high'])
                
                if low_price <= current_price <= high_price:
                    obs.append({
                        'type': 'BEARISH_OB',
                        'method': 'order_block',
                        'details': f"Bearish OB at {current_price:.6f}"
                    })
        
        return obs[:3]
    except:
        return []

def find_liquidity_grab(df: pd.DataFrame, lookback: int = 15) -> List[Dict]:
    """Find Liquidity Grabs - SIMPLIFIED"""
    lgs = []
    if len(df) < 10:
        return lgs
    
    try:
        current_price = df['close'].iloc[-1]
        
        for i in range(2, min(lookback, len(df)-2)):
            candle = df.iloc[-i]
            next_candle = df.iloc[-i+1]
            
            # Bullish LG (sweep low then reversal)
            if candle['low'] < df['low'].iloc[-i-5:-i].min():
                if next_candle['close'] > candle['high']:
                    if abs(current_price - candle['low']) < current_price * 0.02:  # Within 2%
                        lgs.append({
                            'type': 'BULLISH_LG',
                            'method': 'liquidity_grab',
                            'details': f"Bullish LG near {current_price:.6f}"
                        })
            
            # Bearish LG (sweep high then reversal)
            if candle['high'] > df['high'].iloc[-i-5:-i].max():
                if next_candle['close'] < candle['low']:
                    if abs(current_price - candle['high']) < current_price * 0.02:
                        lgs.append({
                            'type': 'BEARISH_LG',
                            'method': 'liquidity_grab',
                            'details': f"Bearish LG near {current_price:.6f}"
                        })
        
        return lgs[:2]
    except:
        return []

def find_breaker_blocks(df: pd.DataFrame, lookback: int = 30) -> List[Dict]:
    """Find Breaker Blocks - SIMPLIFIED"""
    breakers = []
    if len(df) < 20:
        return breakers
    
    try:
        current_price = df['close'].iloc[-1]
        
        # Simple swing detection
        for i in range(2, min(lookback, len(df)-2)):
            # Swing low
            if (df['low'].iloc[-i] < df['low'].iloc[-i-1] and
                df['low'].iloc[-i] < df['low'].iloc[-i+1]):
                
                # Check if price broke below and recovered
                for j in range(1, min(5, i)):
                    if df['low'].iloc[-i+j] < df['low'].iloc[-i]:  # Broke below
                        if df['close'].iloc[-i+j] > df['low'].iloc[-i]:  # Recovered
                            if abs(current_price - df['low'].iloc[-i]) < current_price * 0.02:
                                breakers.append({
                                    'type': 'BULLISH_BREAKER',
                                    'method': 'breaker_block',
                                    'details': f"Bullish Breaker near {current_price:.6f}"
                                })
                                break
            
            # Swing high
            if (df['high'].iloc[-i] > df['high'].iloc[-i-1] and
                df['high'].iloc[-i] > df['high'].iloc[-i+1]):
                
                for j in range(1, min(5, i)):
                    if df['high'].iloc[-i+j] > df['high'].iloc[-i]:  # Broke above
                        if df['close'].iloc[-i+j] < df['high'].iloc[-i]:  # Rejected
                            if abs(current_price - df['high'].iloc[-i]) < current_price * 0.02:
                                breakers.append({
                                    'type': 'BEARISH_BREAKER',
                                    'method': 'breaker_block',
                                    'details': f"Bearish Breaker near {current_price:.6f}"
                                })
                                break
        
        return breakers[:2]
    except:
        return []

# ================ SIGNAL GENERATION ================

def generate_numeric_breakdown(signal: Dict, df: pd.DataFrame) -> str:
    """Generate simple numeric breakdown"""
    try:
        atr_val = calculate_atr(df, 14)
        risk = abs(signal['entry'] - signal['sl'])
        reward = abs(signal['tp'] - signal['entry'])
        rr = reward / risk if risk > 0 else 0
        
        breakdown = [
            f"Price: {signal['entry']:.6f}",
            f"ATR: {atr_val:.6f}",
            f"Risk: {risk:.6f}",
            f"Reward: {reward:.6f}",
            f"R:R: {rr:.2f}:1",
            f"Method: {signal['method']}",
            f"Confidence: {signal.get('confidence', 0):.2f}"
        ]
        return "\n".join(breakdown)
    except:
        return "Breakdown not available"

def calculate_tp_sl(signal: Dict, df: pd.DataFrame, atr_val: float) -> Tuple[Optional[float], Optional[float]]:
    """Calculate Take Profit and Stop Loss"""
    try:
        entry = signal['entry']
        side = signal['side']
        
        if atr_val == 0:
            atr_val = entry * 0.01  # Default 1%
        
        if side == 'BUY':
            sl = entry - (atr_val * 1.5)
            tp = entry + (2 * (entry - sl))
            
            # Ensure valid
            if sl >= entry or tp <= entry:
                sl = entry - (entry * 0.02)  # 2% stop
                tp = entry + (entry * 0.04)  # 4% target
        else:
            sl = entry + (atr_val * 1.5)
            tp = entry - (2 * (sl - entry))
            
            if sl <= entry or tp >= entry:
                sl = entry + (entry * 0.02)  # 2% stop
                tp = entry - (entry * 0.04)  # 4% target
        
        return sl, tp
    except:
        # Fallback calculation
        if signal['side'] == 'BUY':
            return signal['entry'] * 0.98, signal['entry'] * 1.04
        else:
            return signal['entry'] * 1.02, signal['entry'] * 0.96

def analyze_all_methods(df: pd.DataFrame, symbol: str, timeframe: str) -> Optional[Dict]:
    """Analyze all 5 methods with duplicate prevention checks"""
    if len(df) < 50:
        return None
    
    try:
        # Check 1: Require minimum price movement
        if not has_sufficient_price_movement(df, symbol):
            return None
        
        # Get current price
        current_price = df['close'].iloc[-1]
        
        # Run all methods
        zones = find_supply_demand_zones(df)
        fvgs = find_fvg(df)
        obs = find_order_blocks(df)
        lgs = find_liquidity_grab(df)
        breakers = find_breaker_blocks(df)
        
        # Collect all signals
        all_signals = []
        
        # Add zone signals
        for zone in zones:
            confidence = 0.6
            if confidence >= MIN_CONFIDENCE:
                all_signals.append({
                    'symbol': symbol,
                    'side': 'BUY' if zone['type'] == 'DEMAND' else 'SELL',
                    'entry': current_price,
                    'method': 'supply_demand',
                    'strength': 5,
                    'confidence': confidence,
                    'timeframe': timeframe,
                    'method_details': zone['details']
                })
        
        # Add FVG signals
        for fvg in fvgs:
            confidence = 0.55
            if confidence >= MIN_CONFIDENCE:
                all_signals.append({
                    'symbol': symbol,
                    'side': 'BUY' if fvg['type'] == 'BULLISH_FVG' else 'SELL',
                    'entry': current_price,
                    'method': 'fvg',
                    'strength': 4,
                    'confidence': confidence,
                    'timeframe': timeframe,
                    'method_details': fvg['details']
                })
        
        # Add OB signals
        for ob in obs:
            confidence = 0.65
            if confidence >= MIN_CONFIDENCE:
                all_signals.append({
                    'symbol': symbol,
                    'side': 'BUY' if ob['type'] == 'BULLISH_OB' else 'SELL',
                    'entry': current_price,
                    'method': 'order_block',
                    'strength': 6,
                    'confidence': confidence,
                    'timeframe': timeframe,
                    'method_details': ob['details']
                })
        
        # Add LG signals
        for lg in lgs:
            confidence = 0.7
            if confidence >= MIN_CONFIDENCE:
                all_signals.append({
                    'symbol': symbol,
                    'side': 'BUY' if lg['type'] == 'BULLISH_LG' else 'SELL',
                    'entry': current_price,
                    'method': 'liquidity_grab',
                    'strength': 7,
                    'confidence': confidence,
                    'timeframe': timeframe,
                    'method_details': lg['details']
                })
        
        # Add breaker signals
        for breaker in breakers:
            confidence = 0.6
            if confidence >= MIN_CONFIDENCE:
                all_signals.append({
                    'symbol': symbol,
                    'side': 'BUY' if breaker['type'] == 'BULLISH_BREAKER' else 'SELL',
                    'entry': current_price,
                    'method': 'breaker_block',
                    'strength': 5,
                    'confidence': confidence,
                    'timeframe': timeframe,
                    'method_details': breaker['details']
                })
        
        # Merge similar signals
        all_signals = merge_similar_signals(all_signals)
        
        # Pick best signal if any
        if all_signals:
            # Sort by confidence
            all_signals.sort(key=lambda x: x['confidence'], reverse=True)
            best_signal = all_signals[0]
            
            # Calculate TP/SL
            atr_val = calculate_atr(df, 14)
            sl, tp = calculate_tp_sl(best_signal, df, atr_val)
            
            if sl and tp:
                best_signal['sl'] = sl
                best_signal['tp'] = tp
                
                # Calculate R:R
                risk = abs(best_signal['entry'] - sl)
                reward = abs(tp - best_signal['entry'])
                rr = reward / risk if risk > 0 else 0
                risk_pct = (risk / best_signal['entry']) * 100
                reward_pct = (reward / best_signal['entry']) * 100
                
                best_signal['rr_ratio'] = rr
                best_signal['risk_pct'] = risk_pct
                best_signal['reward_pct'] = reward_pct
                
                # Add breakdown
                best_signal['numeric_breakdown'] = generate_numeric_breakdown(best_signal, df)
                
                # Add signal hash for duplicate tracking
                best_signal['signal_hash'] = get_signal_hash(best_signal)
                
                return best_signal
        
        return None
    except Exception as e:
        log.warning(f"Analysis error: {e}")
        return None

# ---------------- SIGNAL LOGGING ----------------
async def log_signal(sig: Dict):
    """Log signal to database with duplicate check"""
    try:
        signal_hash = sig.get('signal_hash')
        if not signal_hash:
            signal_hash = get_signal_hash(sig)
        
        async with db_lock:
            # Check for duplicate signal hash
            async with db_conn.execute("""
                SELECT COUNT(*) FROM signals WHERE signal_hash=?
            """, (signal_hash,)) as cursor:
                count = (await cursor.fetchone())[0]
            
            if count > 0:
                log.info(f"⏭️ Database duplicate found (hash): {sig['symbol']} {sig['method']}")
                return
            
            # Check for recent similar signal (same symbol, method, side, similar price)
            async with db_conn.execute("""
                SELECT COUNT(*) FROM signals 
                WHERE symbol=? AND method=? AND side=?
                AND timestamp > datetime('now', '-2 hours')
                AND ABS(entry - ?) / ? < ?
            """, (
                sig['symbol'], 
                sig['method'], 
                sig['side'],
                sig['entry'], 
                sig['entry'],
                PRICE_BUCKET_SIZE_PCT / 100  # Convert to decimal
            )) as cursor:
                count = (await cursor.fetchone())[0]
            
            if count > 0:
                log.info(f"⏭️ Similar signal found in last 2 hours: {sig['symbol']} {sig['method']}")
                return
            
            # Insert new signal
            await db_conn.execute("""
                INSERT INTO signals (
                    symbol, side, entry, sl, tp, status, method, method_details,
                    strength, confidence, timeframe, rr_ratio, risk_pct, reward_pct, 
                    numeric_breakdown, signal_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                sig.get('rr_ratio', 0),
                sig.get('risk_pct', 0),
                sig.get('reward_pct', 0),
                sig.get('numeric_breakdown', ''),
                signal_hash
            ))
            await db_conn.commit()
            log.info(f"✅ Signal logged: {sig['symbol']} {sig['method']}")
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
last_scan_time = {}

async def scan_loop(exchange):
    """Main scanning loop with duplicate prevention"""
    while True:
        start_time = time.time()
        
        try:
            # Fetch top pairs
            tickers = await exchange.fetch_tickers()
            usdt_pairs = []
            
            for symbol in tickers:
                if symbol.endswith("/USDT"):
                    volume = tickers[symbol].get("quoteVolume", 0)
                    if volume > 0:
                        usdt_pairs.append((symbol, volume))
            
            # Sort and take top N
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            top_pairs = usdt_pairs[:TOP_N]
            
            signals_found = 0
            
            for symbol, volume in top_pairs:
                for timeframe in TIMEFRAMES:
                    # Skip problematic timeframes
                    if timeframe == "3h":  # OKX doesn't support 3h
                        continue
                    
                    # Rate limiting per symbol/timeframe
                    key = f"{symbol}:{timeframe}"
                    if key in last_scan_time:
                        if time.time() - last_scan_time[key] < 60:  # 1 minute minimum between scans
                            continue
                    
                    last_scan_time[key] = time.time()
                    
                    # Fetch data
                    ohlcv = await fetch_ohlcv(exchange, symbol, timeframe, 150)
                    if not ohlcv or len(ohlcv) < 50:
                        continue
                    
                    # Create DataFrame
                    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                    for col in ["open", "high", "low", "close", "volume"]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    
                    # Analyze
                    signal = analyze_all_methods(df, symbol, timeframe)
                    
                    if signal:
                        # Check cooldown
                        if check_signal_cooldown(signal):
                            log.info(f"⏭️ Signal in cooldown: {signal['symbol']} {signal['method']}")
                            continue
                        
                        # Check processed signals (in-memory)
                        signal_hash = signal.get('signal_hash', get_signal_hash(signal))
                        if signal_hash in processed_signals:
                            log.info(f"⏭️ Signal already processed: {signal['symbol']} {signal['method']}")
                            continue
                        
                        # Add to processed signals
                        processed_signals.add(signal_hash)
                        
                        # Clean old processed signals (older than 1 hour)
                        if len(processed_signals) > 1000:
                            # Simple cleanup - just clear if gets too large
                            processed_signals.clear()
                        
                        # Send alert
                        await send_signal_alert(signal)
                        
                        # Log to database
                        await log_signal(signal)
                        
                        signals_found += 1
            
            log.info(f"Scan complete: {signals_found} signals found")
            
        except Exception as e:
            log.error(f"Scan error: {e}")
        
        # Wait for next scan
        elapsed = time.time() - start_time
        sleep_time = max(10, SCAN_INTERVAL - elapsed)
        await asyncio.sleep(sleep_time)

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    """Monitor open signals"""
    while True:
        try:
            async with db_lock:
                # Get open signals
                async with db_conn.execute("""
                    SELECT id, symbol, side, entry, sl, tp, tp_hit, sl_hit, timeframe 
                    FROM signals WHERE status='OPEN'
                """) as cursor:
                    signals = await cursor.fetchall()
                
                for sig in signals:
                    sig_id, symbol, side, entry, sl, tp, tp_hit, sl_hit, timeframe = sig
                    
                    # Get current price
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
                        
                        else:  # SELL
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
                        log.error(f"Price check error {symbol}: {e}")
                
                await db_conn.commit()
                
        except Exception as e:
            log.error(f"Monitor error: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/")
async def root():
    return {"status": "running", "scanner": "5-Method Fresh"}

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

# ---------------- MAIN ----------------
async def main():
    global exchange, db_conn
    
    log.info("🚀 Starting FRESH 5-Method Scanner...")
    
    try:
        # Initialize fresh database
        log.info("🆕 Creating fresh database...")
        if not await init_db_fresh():
            log.error("❌ Failed to create database")
            return
        
        # Initialize exchange
        exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"}
        })
        
        # Test connection
        await exchange.fetch_ticker("BTC/USDT")
        log.info("✅ Exchange connected")
        
        # Startup message
        await tg(f"""
🚀 5-METHOD SCANNER STARTED (FRESH DATABASE)

Methods: Supply/Demand, FVG, Order Blocks, Liquidity Grab, Breaker Blocks
Timeframes: 15m, 30m, 1h, 2h, 4h (3h skipped for OKX)
Confidence: {MIN_CONFIDENCE*100}% minimum
Database: Fresh start - all columns exist

DUPLICATE PREVENTION ENABLED:
- Cooldown: {SIGNAL_COOLDOWN_MINUTES} minutes per symbol/method/side
- Min price movement: {MIN_PRICE_MOVEMENT_PCT}%
- Price grouping: {PRICE_BUCKET_SIZE_PCT}%
        """)
        
        log.info("✅ Scanner ready - starting loops...")
        
        # Start scanning and monitoring
        await asyncio.gather(
            scan_loop(exchange),
            monitor_signals()
        )
        
    except KeyboardInterrupt:
        log.info("👋 Stopped by user")
    except Exception as e:
        log.error(f"💥 Fatal error: {e}")
    finally:
        # Cleanup
        if db_conn:
            await db_conn.close()
        if exchange:
            await exchange.close()
        log.info("✅ Scanner stopped cleanly")

if __name__ == "__main__":
    # Run scanner
    asyncio.run(main())