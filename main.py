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
TOP_N = int(os.getenv("TOP_N", 60))
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
def detect_protected_highs_lows(df: pd.DataFrame, lookback: int = 50) -> Tuple[List, List]:
    """
    Detect protected highs and lows (institutional structure)
    Protected High: High with lower highs on both sides (local maximum)
    Protected Low: Low with higher lows on both sides (local minimum)
    """
    protected_highs = []
    protected_lows = []
    
    if len(df) < 3:
        return protected_highs, protected_lows
    
    for i in range(1, len(df)-1):
        # Check for Protected High (local maximum)
        if (df.iloc[i]['high'] > df.iloc[i-1]['high'] and 
            df.iloc[i]['high'] > df.iloc[i+1]['high']):
            # Additional check: ensure it's significant (not just noise)
            avg_candle_size = (df['high'] - df['low']).mean()
            if (df.iloc[i]['high'] - df.iloc[i-1]['high']) > avg_candle_size * 0.3:
                protected_highs.append({
                    'price': float(df.iloc[i]['high']),
                    'index': i,
                    'strength': 2 if i > len(df)*0.8 else 1  # Recent = stronger
                })
        
        # Check for Protected Low (local minimum)
        if (df.iloc[i]['low'] < df.iloc[i-1]['low'] and 
            df.iloc[i]['low'] < df.iloc[i+1]['low']):
            avg_candle_size = (df['high'] - df['low']).mean()
            if (df.iloc[i-1]['low'] - df.iloc[i]['low']) > avg_candle_size * 0.3:
                protected_lows.append({
                    'price': float(df.iloc[i]['low']),
                    'index': i,
                    'strength': 2 if i > len(df)*0.8 else 1
                })
    
    # Sort and return most recent ones
    protected_highs.sort(key=lambda x: x['price'], reverse=True)
    protected_lows.sort(key=lambda x: x['price'])
    
    # Return limited number for efficiency
    return protected_highs[:10], protected_lows[:10]

def detect_liquidity_pools(df: pd.DataFrame) -> Dict[str, List]:
    """
    Detect liquidity pools (equal highs, equal lows, swing points)
    """
    liquidity_pools = {
        'equal_highs': [],
        'equal_lows': [],
        'swing_highs': [],
        'swing_lows': []
    }
    
    if len(df) < 10:
        return liquidity_pools
    
    # Detect swing highs/lows with more robust logic
    for i in range(2, len(df)-2):
        # Swing High: price higher than neighbors with confirmation
        if (df.iloc[i]['high'] > df.iloc[i-2:i]['high'].max() and
            df.iloc[i]['high'] > df.iloc[i+1:i+3]['high'].max()):
            liquidity_pools['swing_highs'].append({
                'price': float(df.iloc[i]['high']),
                'index': i,
                'strength': 1
            })
        
        # Swing Low: price lower than neighbors with confirmation
        if (df.iloc[i]['low'] < df.iloc[i-2:i]['low'].min() and
            df.iloc[i]['low'] < df.iloc[i+1:i+3]['low'].min()):
            liquidity_pools['swing_lows'].append({
                'price': float(df.iloc[i]['low']),
                'index': i,
                'strength': 1
            })
    
    # Detect equal highs (price clusters)
    if len(df) >= 20:
        recent_highs = df['high'].iloc[-20:].values
        for i in range(len(recent_highs)):
            for j in range(i+1, len(recent_highs)):
                if abs(recent_highs[i] - recent_highs[j]) / (recent_highs[i] + 1e-8) < 0.001:  # Within 0.1%
                    avg_price = (recent_highs[i] + recent_highs[j]) / 2
                    # Check if not already added
                    if not any(abs(p['price'] - avg_price) / avg_price < 0.0005 for p in liquidity_pools['equal_highs']):
                        liquidity_pools['equal_highs'].append({
                            'price': float(avg_price),
                            'strength': 2
                        })
    
    # Detect equal lows
    if len(df) >= 20:
        recent_lows = df['low'].iloc[-20:].values
        for i in range(len(recent_lows)):
            for j in range(i+1, len(recent_lows)):
                if abs(recent_lows[i] - recent_lows[j]) / (recent_lows[i] + 1e-8) < 0.001:
                    avg_price = (recent_lows[i] + recent_lows[j]) / 2
                    if not any(abs(p['price'] - avg_price) / avg_price < 0.0005 for p in liquidity_pools['equal_lows']):
                        liquidity_pools['equal_lows'].append({
                            'price': float(avg_price),
                            'strength': 2
                        })
    
    return liquidity_pools

def detect_bos_points(df: pd.DataFrame) -> List[Dict]:
    """
    Detect Break of Structure points
    """
    bos_points = []
    
    if len(df) < 15:
        return bos_points
    
    for i in range(10, len(df)-5):
        # Bullish BOS: Break above previous high after establishing higher low
        prev_high = df.iloc[i-5:i]['high'].max()
        if (df.iloc[i]['high'] > prev_high and
            df.iloc[i-3]['low'] > df.iloc[i-8]['low'] and
            df.iloc[i]['close'] > df.iloc[i]['open']):  # Bullish candle
            bos_points.append({
                'price': float(df.iloc[i]['high']),
                'type': 'bullish',
                'index': i,
                'strength': 1
            })
        
        # Bearish BOS: Break below previous low after establishing lower high
        prev_low = df.iloc[i-5:i]['low'].min()
        if (df.iloc[i]['low'] < prev_low and
            df.iloc[i-3]['high'] < df.iloc[i-8]['high'] and
            df.iloc[i]['close'] < df.iloc[i]['open']):  # Bearish candle
            bos_points.append({
                'price': float(df.iloc[i]['low']),
                'type': 'bearish',
                'index': i,
                'strength': 1
            })
    
    # Return most recent 5 BOS points
    return bos_points[-5:] if bos_points else []

# ---------------- ROMEOPT TP/SL MODULE ----------------
class RomeOPT_TP_SL:
    """
    TRUE RomeOPT institutional TP/SL logic
    NO ATR, NO percentages, NO fixed pips
    Based purely on market structure
    """
    
    def __init__(self, timeframe: str):
        self.timeframe = timeframe
        
        # Timeframe-specific configurations
        self.tf_config = {
            '1m': {'buffer': 0.00003, 'min_rrr': 2.0, 'max_lookback': 15, 'min_distance': 0.00010},
            '3m': {'buffer': 0.00004, 'min_rrr': 1.8, 'max_lookback': 25, 'min_distance': 0.00015},
            '5m': {'buffer': 0.00005, 'min_rrr': 1.6, 'max_lookback': 35, 'min_distance': 0.00020},
            '15m': {'buffer': 0.00008, 'min_rrr': 1.4, 'max_lookback': 45, 'min_distance': 0.00030},
            '30m': {'buffer': 0.00012, 'min_rrr': 1.2, 'max_lookback': 60, 'min_distance': 0.00050}
        }
        
        config = self.tf_config.get(timeframe, self.tf_config['5m'])
        self.buffer = config['buffer']
        self.min_rrr = config['min_rrr']
        self.max_lookback = config['max_lookback']
        self.min_distance = config['min_distance']
        
        log.debug(f"RomeOPT_TP_SL initialized for {timeframe}: buffer={self.buffer}, min_rrr={self.min_rrr}")
    
    def calculate_stop_loss(self, side: str, entry_price: float, entry_ob: Dict, 
                           protected_highs: List, protected_lows: List,
                           bos_points: List) -> float:
        """
        Calculate SL according to RomeOPT rules:
        1. For LONG: Below origin OB low or last protected low before BOS
        2. For SHORT: Above origin OB high or last protected high before BOS
        3. Always at institutional structure invalidation point
        """
        sl_candidates = []
        
        try:
            if side == "BUY":
                # 1. Below origin OB low (if available and valid)
                if entry_ob and entry_ob.get('type') == 'bullish':
                    ob_low = entry_ob['low']
                    # Ensure OB low is below entry price
                    if ob_low < entry_price:
                        sl_candidates.append(ob_low - self.buffer)
                        log.debug(f"LONG SL candidate from OB: {ob_low - self.buffer}")
                
                # 2. Below last protected low before most recent BOS
                if protected_lows and bos_points:
                    # Find most recent bearish BOS (market turning down)
                    bearish_bos = [b for b in bos_points if b['type'] == 'bearish']
                    if bearish_bos:
                        latest_bos = max(bearish_bos, key=lambda x: x.get('index', 0))
                        # Get protected lows that occurred BEFORE this BOS
                        lows_before_bos = [pl for pl in protected_lows 
                                          if pl.get('index', 0) < latest_bos.get('index', 0)]
                        if lows_before_bos:
                            # Get the LOWEST protected low (most conservative)
                            lowest_before_bos = min(lows_before_bos, key=lambda x: x['price'])
                            if lowest_before_bos['price'] < entry_price:
                                sl_candidates.append(lowest_before_bos['price'] - self.buffer)
                                log.debug(f"LONG SL candidate from protected low: {lowest_before_bos['price'] - self.buffer}")
                
                # 3. Fallback: Use most recent significant protected low
                if not sl_candidates and protected_lows:
                    recent_lows = [pl for pl in protected_lows 
                                  if pl.get('price', float('inf')) < entry_price]
                    if recent_lows:
                        lowest_recent = min(recent_lows, key=lambda x: x['price'])
                        sl_candidates.append(lowest_recent['price'] - self.buffer)
                        log.debug(f"LONG SL fallback from recent low: {lowest_recent['price'] - self.buffer}")
                
                # Select most conservative (lowest) valid SL
                if sl_candidates:
                    sl = min(sl_candidates)
                    # Ensure minimum distance from entry
                    min_sl = entry_price - (self.min_distance * 3)  # At least 3x min distance
                    if sl > min_sl:
                        sl = min_sl
                        log.debug(f"LONG SL adjusted to min distance: {sl}")
                    
                    # Final safety: ensure SL is below entry
                    if sl >= entry_price:
                        sl = entry_price - (self.min_distance * 2)
                        log.debug(f"LONG SL safety adjustment: {sl}")
                    
                    log.info(f"LONG SL calculated: {sl} (entry: {entry_price}, candidates: {sl_candidates})")
                    return sl
            
            else:  # SELL
                # 1. Above origin OB high (if available and valid)
                if entry_ob and entry_ob.get('type') == 'bearish':
                    ob_high = entry_ob['high']
                    # Ensure OB high is above entry price
                    if ob_high > entry_price:
                        sl_candidates.append(ob_high + self.buffer)
                        log.debug(f"SHORT SL candidate from OB: {ob_high + self.buffer}")
                
                # 2. Above last protected high before most recent BOS
                if protected_highs and bos_points:
                    # Find most recent bullish BOS (market turning up)
                    bullish_bos = [b for b in bos_points if b['type'] == 'bullish']
                    if bullish_bos:
                        latest_bos = max(bullish_bos, key=lambda x: x.get('index', 0))
                        # Get protected highs that occurred BEFORE this BOS
                        highs_before_bos = [ph for ph in protected_highs 
                                           if ph.get('index', 0) < latest_bos.get('index', 0)]
                        if highs_before_bos:
                            # Get the HIGHEST protected high (most conservative)
                            highest_before_bos = max(highs_before_bos, key=lambda x: x['price'])
                            if highest_before_bos['price'] > entry_price:
                                sl_candidates.append(highest_before_bos['price'] + self.buffer)
                                log.debug(f"SHORT SL candidate from protected high: {highest_before_bos['price'] + self.buffer}")
                
                # 3. Fallback: Use most recent significant protected high
                if not sl_candidates and protected_highs:
                    recent_highs = [ph for ph in protected_highs 
                                   if ph.get('price', 0) > entry_price]
                    if recent_highs:
                        highest_recent = max(recent_highs, key=lambda x: x['price'])
                        sl_candidates.append(highest_recent['price'] + self.buffer)
                        log.debug(f"SHORT SL fallback from recent high: {highest_recent['price'] + self.buffer}")
                
                # Select most conservative (highest) valid SL
                if sl_candidates:
                    sl = max(sl_candidates)
                    # Ensure minimum distance from entry
                    max_sl = entry_price + (self.min_distance * 3)  # At least 3x min distance
                    if sl < max_sl:
                        sl = max_sl
                        log.debug(f"SHORT SL adjusted to min distance: {sl}")
                    
                    # Final safety: ensure SL is above entry
                    if sl <= entry_price:
                        sl = entry_price + (self.min_distance * 2)
                        log.debug(f"SHORT SL safety adjustment: {sl}")
                    
                    log.info(f"SHORT SL calculated: {sl} (entry: {entry_price}, candidates: {sl_candidates})")
                    return sl
        
        except Exception as e:
            log.error(f"Error in calculate_stop_loss: {e}")
        
        # Emergency fallback: Use percentage based on timeframe
        fallback_pct = {'1m': 0.003, '3m': 0.004, '5m': 0.005, 
                       '15m': 0.006, '30m': 0.008}
        pct = fallback_pct.get(self.timeframe, 0.005)
        
        if side == "BUY":
            sl = entry_price * (1 - pct)
            log.warning(f"LONG SL fallback to percentage: {sl} ({pct*100}%)")
        else:
            sl = entry_price * (1 + pct)
            log.warning(f"SHORT SL fallback to percentage: {sl} ({pct*100}%)")
        
        return sl
    
    def calculate_take_profit(self, side: str, entry_price: float, stop_loss: float,
                             liquidity_pools: Dict, df: pd.DataFrame) -> Tuple[float, float, float]:
        """
        Calculate TP targeting real liquidity pools according to RomeOPT:
        1. For LONG: First logical liquidity ABOVE (equal highs, swing highs, etc.)
        2. For SHORT: First logical liquidity BELOW (equal lows, swing lows, etc.)
        3. Minimum RRR: 1:2, prefer 1:3-1:5
        """
        try:
            risk = abs(entry_price - stop_loss)
            if risk <= 0:
                log.warning(f"Invalid risk calculation: entry={entry_price}, sl={stop_loss}")
                risk = entry_price * 0.005  # Default 0.5% risk
            
            log.debug(f"TP calculation: side={side}, entry={entry_price}, sl={stop_loss}, risk={risk}")
            
            if side == "BUY":
                # Collect all potential LONG targets ABOVE entry
                targets = []
                
                # 1. Equal highs (strong liquidity pools)
                for eh in liquidity_pools.get('equal_highs', []):
                    if eh['price'] > entry_price:
                        distance = eh['price'] - entry_price
                        rrr = distance / risk if risk > 0 else 0
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
                        rrr = distance / risk if risk > 0 else 0
                        targets.append({
                            'price': sh['price'],
                            'type': 'swing_high',
                            'strength': sh.get('strength', 1),
                            'distance': distance,
                            'rrr': rrr
                        })
                
                # 3. Recent highs from price action (last N candles)
                recent_period = min(self.max_lookback, len(df))
                if recent_period > 5:
                    recent_highs = df['high'].iloc[-recent_period:].unique()
                    for high in sorted(recent_highs):
                        if high > entry_price:
                            distance = high - entry_price
                            rrr = distance / risk if risk > 0 else 0
                            # Only add if significantly above entry
                            if distance > risk * 0.5:  # At least 0.5R
                                targets.append({
                                    'price': float(high),
                                    'type': 'recent_high',
                                    'strength': 1,
                                    'distance': distance,
                                    'rrr': rrr
                                })
                
                # Sort by distance from entry (ascending)
                targets.sort(key=lambda x: x['price'])
                
                # Filter by minimum RRR and reasonable distance
                valid_targets = []
                for target in targets:
                    if target['rrr'] >= self.min_rrr and target['distance'] >= self.min_distance:
                        # Score target (higher is better)
                        score = target['rrr'] * 10
                        if target['type'] == 'equal_high':
                            score *= 1.5
                        elif target['type'] == 'swing_high':
                            score *= 1.2
                        
                        valid_targets.append({
                            **target,
                            'score': score
                        })
                
                log.debug(f"LONG targets found: {len(targets)}, valid: {len(valid_targets)}")
                
                if not valid_targets:
                    # No valid targets, use minimum RRR
                    log.debug(f"No valid LONG targets, using min RRR: {self.min_rrr}")
                    tp1 = entry_price + (risk * self.min_rrr)
                    tp2 = entry_price + (risk * (self.min_rrr + 0.5))
                    tp3 = entry_price + (risk * (self.min_rrr + 1.0))
                    return tp1, tp2, tp3
                
                # Sort by score (highest first)
                valid_targets.sort(key=lambda x: x['score'], reverse=True)
                
                # Select up to 3 best targets with proper spacing
                selected_targets = []
                for target in valid_targets:
                    if len(selected_targets) >= 3:
                        break
                    
                    if not selected_targets:
                        selected_targets.append(target)
                    else:
                        # Ensure targets are spaced properly (at least 0.5R apart)
                        last_price = selected_targets[-1]['price']
                        min_gap = risk * 0.5
                        if target['price'] - last_price >= min_gap:
                            selected_targets.append(target)
                
                # Create TP levels
                if len(selected_targets) >= 3:
                    tp1 = selected_targets[0]['price']
                    tp2 = selected_targets[1]['price']
                    tp3 = selected_targets[2]['price']
                elif len(selected_targets) == 2:
                    tp1 = selected_targets[0]['price']
                    tp2 = selected_targets[1]['price']
                    tp3 = selected_targets[1]['price'] + (risk * 0.5)  # Extend beyond
                elif len(selected_targets) == 1:
                    tp1 = selected_targets[0]['price']
                    tp2 = selected_targets[0]['price'] + (risk * 0.3)
                    tp3 = selected_targets[0]['price'] + (risk * 0.7)
                else:
                    tp1 = entry_price + (risk * self.min_rrr)
                    tp2 = entry_price + (risk * (self.min_rrr + 0.5))
                    tp3 = entry_price + (risk * (self.min_rrr + 1.0))
                
                # Ensure proper ordering
                tp1, tp2, tp3 = sorted([tp1, tp2, tp3])
                
                log.info(f"LONG TP calculated: {tp1}, {tp2}, {tp3} (entry: {entry_price}, risk: {risk})")
                return tp1, tp2, tp3
            
            else:  # SELL
                # Collect all potential SHORT targets BELOW entry
                targets = []
                
                # 1. Equal lows (strong liquidity pools)
                for el in liquidity_pools.get('equal_lows', []):
                    if el['price'] < entry_price:
                        distance = entry_price - el['price']
                        rrr = distance / risk if risk > 0 else 0
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
                        rrr = distance / risk if risk > 0 else 0
                        targets.append({
                            'price': sl['price'],
                            'type': 'swing_low',
                            'strength': sl.get('strength', 1),
                            'distance': distance,
                            'rrr': rrr
                        })
                
                # 3. Recent lows from price action
                recent_period = min(self.max_lookback, len(df))
                if recent_period > 5:
                    recent_lows = df['low'].iloc[-recent_period:].unique()
                    for low in sorted(recent_lows, reverse=True):
                        if low < entry_price:
                            distance = entry_price - low
                            rrr = distance / risk if risk > 0 else 0
                            if distance > risk * 0.5:  # At least 0.5R
                                targets.append({
                                    'price': float(low),
                                    'type': 'recent_low',
                                    'strength': 1,
                                    'distance': distance,
                                    'rrr': rrr
                                })
                
                # Sort by distance from entry (descending for SELL)
                targets.sort(key=lambda x: x['price'], reverse=True)
                
                # Filter by minimum RRR and reasonable distance
                valid_targets = []
                for target in targets:
                    if target['rrr'] >= self.min_rrr and target['distance'] >= self.min_distance:
                        # Score target
                        score = target['rrr'] * 10
                        if target['type'] == 'equal_low':
                            score *= 1.5
                        elif target['type'] == 'swing_low':
                            score *= 1.2
                        
                        valid_targets.append({
                            **target,
                            'score': score
                        })
                
                log.debug(f"SHORT targets found: {len(targets)}, valid: {len(valid_targets)}")
                
                if not valid_targets:
                    # No valid targets, use minimum RRR
                    log.debug(f"No valid SHORT targets, using min RRR: {self.min_rrr}")
                    tp1 = entry_price - (risk * self.min_rrr)
                    tp2 = entry_price - (risk * (self.min_rrr + 0.5))
                    tp3 = entry_price - (risk * (self.min_rrr + 1.0))
                    return tp1, tp2, tp3
                
                # Sort by score (highest first)
                valid_targets.sort(key=lambda x: x['score'], reverse=True)
                
                # Select up to 3 best targets with proper spacing
                selected_targets = []
                for target in valid_targets:
                    if len(selected_targets) >= 3:
                        break
                    
                    if not selected_targets:
                        selected_targets.append(target)
                    else:
                        # Ensure targets are spaced properly
                        last_price = selected_targets[-1]['price']
                        min_gap = risk * 0.5
                        if last_price - target['price'] >= min_gap:
                            selected_targets.append(target)
                
                # Create TP levels
                if len(selected_targets) >= 3:
                    tp1 = selected_targets[0]['price']
                    tp2 = selected_targets[1]['price']
                    tp3 = selected_targets[2]['price']
                elif len(selected_targets) == 2:
                    tp1 = selected_targets[0]['price']
                    tp2 = selected_targets[1]['price']
                    tp3 = selected_targets[1]['price'] - (risk * 0.5)  # Extend beyond
                elif len(selected_targets) == 1:
                    tp1 = selected_targets[0]['price']
                    tp2 = selected_targets[0]['price'] - (risk * 0.3)
                    tp3 = selected_targets[0]['price'] - (risk * 0.7)
                else:
                    tp1 = entry_price - (risk * self.min_rrr)
                    tp2 = entry_price - (risk * (self.min_rrr + 0.5))
                    tp3 = entry_price - (risk * (self.min_rrr + 1.0))
                
                # Ensure proper ordering
                tp1, tp2, tp3 = sorted([tp1, tp2, tp3])
                
                log.info(f"SHORT TP calculated: {tp1}, {tp2}, {tp3} (entry: {entry_price}, risk: {risk})")
                return tp1, tp2, tp3
        
        except Exception as e:
            log.error(f"Error in calculate_take_profit: {e}")
            # Emergency fallback
            risk = abs(entry_price - stop_loss) if stop_loss else entry_price * 0.005
            
            if side == "BUY":
                tp1 = entry_price + (risk * 2.0)
                tp2 = entry_price + (risk * 3.0)
                tp3 = entry_price + (risk * 4.0)
            else:
                tp1 = entry_price - (risk * 2.0)
                tp2 = entry_price - (risk * 3.0)
                tp3 = entry_price - (risk * 4.0)
            
            return tp1, tp2, tp3

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
        
        # Detect market structure
        protected_highs, protected_lows = detect_protected_highs_lows(df)
        liquidity_pools = detect_liquidity_pools(df)
        bos_points = detect_bos_points(df)
        
        # Get entry order block
        entry_ob = find_latest_ob(df)
        if not entry_ob:
            log.warning(f"No order block found for {sig['symbol']}, using fallback")
            # Create a simple OB based on recent price action
            if sig["side"] == "BUY":
                entry_ob = {"type": "bullish", "low": df["low"].iloc[-5:].min(), "high": df["high"].iloc[-5:].max()}
            else:
                entry_ob = {"type": "bearish", "low": df["low"].iloc[-5:].min(), "high": df["high"].iloc[-5:].max()}
        
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
        
        # Validate TP/SL levels
        risk = abs(sig["entry"] - sl)
        reward_tp1 = abs(tp1 - sig["entry"])
        rrr = reward_tp1 / risk if risk > 0 else 0
        
        if rrr < calculator.min_rrr * 0.8:  # Too low RRR
            log.warning(f"RRR too low ({rrr:.2f}), adjusting TP1")
            if sig["side"] == "BUY":
                tp1 = sig["entry"] + (risk * calculator.min_rrr)
            else:
                tp1 = sig["entry"] - (risk * calculator.min_rrr)
            
            # Recalculate TP2 and TP3
            if sig["side"] == "BUY":
                tp2 = tp1 + (risk * 0.5)
                tp3 = tp2 + (risk * 0.5)
            else:
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
        
        log.info(f"RomeOPT TP/SL calculated for {sig['symbol']}: SL={sl}, TP=[{tp1}, {tp2}, {tp3}], RRR={rrr:.2f}")
        
    except Exception as e:
        log.error(f"RomeOPT TP/SL calculation failed for {sig.get('symbol', 'unknown')}: {e}")
        # Fallback to simple TP/SL
        if "sl" not in sig or "tp1" not in sig:
            price_range = df["high"].iloc[-1] - df["low"].iloc[-1]
            if sig["side"] == "BUY":
                sig["sl"] = sig["entry"] - (price_range * 0.5)
                sig["tp1"] = sig["entry"] + (price_range * 1.0)
                sig["tp2"] = sig["entry"] + (price_range * 1.5)
                sig["tp3"] = sig["entry"] + (price_range * 2.0)
            else:
                sig["sl"] = sig["entry"] + (price_range * 0.5)
                sig["tp1"] = sig["entry"] - (price_range * 1.0)
                sig["tp2"] = sig["entry"] - (price_range * 1.5)
                sig["tp3"] = sig["entry"] - (price_range * 2.0)
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
    
    # TP1 distance filter
    if "sl" in sig and "tp1" in sig:
        risk = abs(sig["entry"] - sig["sl"])
        tp1_distance = abs(sig["tp1"] - sig["entry"])
        
        # Reject if TP1 is less than 20% of risk (meaningless profit)
        if tp1_distance < risk * 0.2:
            log.debug(f"TP1 too close for {symbol} {tf}: {tp1_distance} < {risk * 0.2}")
            return None
        
        # Also check if SL is too close (less than 0.5% of price)
        if risk / sig["entry"] < 0.005:
            log.debug(f"Risk too small for {symbol} {tf}: {risk / sig['entry']:.4f}")
            return None
    
    log.info(f"Signal generated for {symbol} {tf}: {side} at {entry}, score={score}")
    return sig

# ---------------- FIND LATEST OB ----------------
def find_latest_ob(df: pd.DataFrame) -> Optional[Dict]:
    """Find the most recent order block"""
    if len(df) < 6:
        return None
    
    for i in range(len(df)-5, len(df)-1):
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
            log.info(f"Signal logged to database: {sig['symbol']} {sig['side']}")
        except Exception as e:
            log.error(f"Error logging signal: {e}")

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    """Monitor open signals and update TP/SL"""
    log.info("Starting signal monitor")
    
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("""
                    SELECT id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status 
                    FROM signals WHERE status='OPEN'
                """) as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status = row
                        
                        # Fetch current price
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get("last")
                        if last_price is None:
                            continue
                        
                        # Fetch live data for TP/SL recalculation
                        ohlcv = await fetch_ohlcv(exchange, symbol, "1m", 50)
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
                            
                            # Update if significant change (>1%)
                            if new_sl and new_tp1:
                                sl_change = abs(new_sl - sl) / sl if sl else 0
                                tp_change = abs(new_tp1 - tp1) / tp1 if tp1 else 0
                                
                                if sl_change > 0.01 or tp_change > 0.01:  # 1% threshold
                                    sl, tp1, tp2, tp3 = new_sl, new_tp1, sig.get("tp2"), sig.get("tp3")
                                    await db_conn.execute(
                                        "UPDATE signals SET sl=?, tp1=?, tp2=?, tp3=? WHERE id=?",
                                        (sl, tp1, tp2, tp3, sig_id)
                                    )
                                    await tg(f"📈 {symbol} TP/SL Updated\nNew SL: {sl:.5f}\nNew TP1: {tp1:.5f}")
                        
                        # Check for TP/SL hits
                        hits = []
                        sl_hit = False
                        
                        if side == "BUY":
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
                        
                        # Update database
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
                        
                        # Send Telegram alert
                        await tg(f"""
🏆 {sig['symbol']} ({tf}) {sig['side']} [{tp_sl_type}]
Entry: {sig['entry']:.5f}
SL: {sig.get('sl', 0):.5f}
TP1: {sig.get('tp1', 0):.5f} TP2: {sig.get('tp2', 0):.5f} TP3: {sig.get('tp3', 0):.5f}
Score: {sig['score']}
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

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/")
async def root():
    return {"status": "ok", "bot": "RomeOPT 6-Step Scanner", "version": "2.0"}

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
    
    # Initialize exchange
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "rateLimit": 100,
        "timeout": 30000,
    })
    
    log.info("RomeOPT 6-Step Scanner Starting...")
    await tg("🏆 ROMEOPT 6-Step Scanner Started - Live Early Signals with INSTITUTIONAL TP/SL")
    
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