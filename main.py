#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT SCANNER v4.1 - LIQUIDITY-FOCUSED WITH FULL 8-STEP DETAILS
Professional trading with complete transparency on all 8 steps
SIMPLE SIGNAL TRACKING: Unique by (Symbol, Side, Rounded_Score)
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
from fastapi import FastAPI
import uvicorn
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH = os.getenv("DB_PATH", "/app/data/romeopt_v4_1.db")

# Scanner settings
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 45))
TOP_N = int(os.getenv("TOP_N", 60))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 2))

# Signal thresholds
MIN_QUALITY_SCORE = float(os.getenv("MIN_QUALITY_SCORE", 3.0))

# Deduplication settings
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", 15))
SIGNAL_VALIDITY_HOURS = int(os.getenv("SIGNAL_VALIDITY_HOURS", 72))

# Rate limiting settings
MAX_REQUESTS_PER_SECOND = int(os.getenv("MAX_REQUESTS_PER_SECOND", 8))
RATE_LIMIT_RETRIES = int(os.getenv("RATE_LIMIT_RETRIES", 3))
RATE_LIMIT_BACKOFF_FACTOR = float(os.getenv("RATE_LIMIT_BACKOFF_FACTOR", 1.8))

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("romeopt_v4_1")

# ---------------- RATE LIMITER ----------------
class RateLimiter:
    """Conservative rate limiter for OKX API"""
    
    def __init__(self):
        self.max_rps = MAX_REQUESTS_PER_SECOND
        self.max_concurrent = MAX_CONCURRENT
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.request_times = []
        self.min_delay = 0.15
        self.backoff_factor = RATE_LIMIT_BACKOFF_FACTOR
        self.max_retries = RATE_LIMIT_RETRIES
        
    async def wait_if_needed(self):
        """Conservative waiting with jitter"""
        now = time.time()
        
        # Clean old request times
        self.request_times = [t for t in self.request_times if now - t < 1.2]
        
        # Check if we're at the limit
        if len(self.request_times) >= self.max_rps:
            wait_time = 1.2 - (now - self.request_times[0])
            if wait_time > 0:
                wait_time += np.random.uniform(0.05, 0.15)
                await asyncio.sleep(wait_time)
        
        # Add this request
        self.request_times.append(now)
        
        # Minimum delay between all requests
        await asyncio.sleep(0.05)
    
    async def execute_with_backoff(self, func, *args, **kwargs):
        """Execute with conservative backoff"""
        async with self.semaphore:
            for attempt in range(self.max_retries):
                try:
                    await self.wait_if_needed()
                    result = await func(*args, **kwargs)
                    await asyncio.sleep(0.02)
                    return result
                except Exception as e:
                    error_str = str(e)
                    if any(phrase in error_str for phrase in ["Too Many Requests", "50011", "429", "rate limit"]):
                        wait_time = self.min_delay * (self.backoff_factor ** attempt)
                        wait_time += np.random.uniform(0.1, 0.3)
                        log.warning(f"Rate limited, attempt {attempt+1}/{self.max_retries}, waiting {wait_time:.2f}s")
                        await asyncio.sleep(wait_time)
                    else:
                        raise e
            raise Exception(f"Failed after {self.max_retries} retries")

# Initialize rate limiter globally
rate_limiter = RateLimiter()

# ---------------- DATA STRUCTURES ----------------
@dataclass
class SetupEligibility:
    """LAYER 1: Fast eligibility check"""
    eligible: bool = False
    side: str = ""
    entry_price: float = 0.0
    entry_type: str = ""
    disqualify_reason: str = ""

@dataclass
class LiquiditySetup:
    """Liquidity-based setup details"""
    sl_price: float = 0.0
    tp_targets: List[float] = None
    tp_sources: List[Dict] = None  # NEW: Source info for each TP
    liquidity_analysis: Dict = None
    rr_ratio: float = 0.0

@dataclass
class SetupQuality:
    """LAYER 2: Quality metrics with detailed step tracking"""
    sweep_strength: float = 0.0
    structure_shift: bool = False
    from_liquidity_exists: bool = False
    confirmation_candle: bool = False
    htfc_alignment_score: float = 0.0
    total_score: float = 0.0
    eight_steps_status: Dict = None
    
    @property
    def quality_tier(self) -> str:
        if self.total_score >= 4.0:
            return "A+"
        elif self.total_score >= 3.0:
            return "A"
        elif self.total_score >= 2.5:
            return "B"
        else:
            return "C"

# ---------------- SIGNAL TRACKER (SYMBOL + SIDE + SCORE) ----------------
class SignalTracker:
    """Track signals by (Symbol, Side, Rounded_Score) only"""
    
    def __init__(self):
        self.active_signals = {}  # key: (symbol, side, rounded_score) -> signal data
        self.outcome_stats = {
            'total_signals': 0,
            'tp1_hits': 0,
            'tp2_hits': 0,
            'tp3_hits': 0,
            'sl_hits': 0,
            'expired': 0,
            'active': 0,
            'win_rate': 0.0,
            'avg_pnl_pct': 0.0
        }
    
    def get_signal_key(self, setup: Dict) -> tuple:
        """Create unique key: (symbol, side, rounded_score)"""
        symbol = setup.get('symbol', '')
        side = setup.get('side', '')
        quality_score = setup.get('quality', {}).get('total_score', 0)
        
        # Round score to nearest 0.25 (1.0, 1.25, 1.5, 1.75, 2.0, etc.)
        rounded_score = round(quality_score * 4) / 4
        
        return (symbol, side, rounded_score)
    
    def is_new_signal(self, setup: Dict) -> Tuple[bool, str]:
        """Check if this (symbol, side, rounded_score) is NEW"""
        key = self.get_signal_key(setup)
        
        if key in self.active_signals:
            signal = self.active_signals[key]
            
            # Check if signal is still active
            if signal.get('status') == 'active':
                # Check if expired
                now = datetime.datetime.utcnow()
                age_minutes = (now - signal['first_seen']).total_seconds() / 60
                
                if age_minutes > (SIGNAL_VALIDITY_HOURS * 60):
                    # Signal expired - remove it
                    self.remove_signal_by_key(key, f"Expired after {SIGNAL_VALIDITY_HOURS}h")
                    return True, "Old signal expired, allowing new one"
                
                # Signal still active - DO NOT SEND
                return False, f"Active signal exists (Score: {key[2]}, {age_minutes:.1f}m old)"
            
            # Signal exists but is closed (hit TP/SL)
            return True, "Previous signal closed, allowing new one"
        
        return True, "No active signal for this symbol+side+score"
    
    def should_send_alert(self, setup: Dict) -> bool:
        """Should we send alert for this setup?"""
        is_new, reason = self.is_new_signal(setup)
        return is_new
    
    def update_signal(self, setup: Dict, alerted: bool = False):
        """Add or update signal"""
        key = self.get_signal_key(setup)
        now = datetime.datetime.utcnow()
        
        if key not in self.active_signals:
            # NEW SIGNAL
            self.active_signals[key] = {
                'setup': setup,
                'first_seen': now,
                'last_alerted': now if alerted else None,
                'last_checked': now,
                'alert_count': 1 if alerted else 0,
                'status': 'active',
                'outcome': 'active',
                'highest_price': setup.get('current_price', 0),
                'lowest_price': setup.get('current_price', 0),
                'price_at_alert': setup.get('current_price', 0) if alerted else None,
                'outcome_details': None,
                'signal_key': key  # Store the key for reference
            }
            self.outcome_stats['total_signals'] += 1
            self.outcome_stats['active'] += 1
            
            symbol, side, score = key
            log.info(f"📝 New signal registered: {symbol} {side} Score:{score}")
        else:
            # EXISTING SIGNAL - update tracking metrics only
            current_price = setup.get('current_price', 0)
            self.active_signals[key]['highest_price'] = max(
                self.active_signals[key]['highest_price'],
                current_price
            )
            self.active_signals[key]['lowest_price'] = min(
                self.active_signals[key]['lowest_price'],
                current_price
            )
            self.active_signals[key]['last_checked'] = now
    
    def check_signal_outcome(self, setup: Dict, current_price: float) -> Optional[Dict]:
        """Check if THIS SPECIFIC SIGNAL has hit TP or SL"""
        key = self.get_signal_key(setup)
        
        if key not in self.active_signals:
            return None
        
        signal = self.active_signals[key]
        
        # Don't check if already closed
        if signal.get('status') != 'active':
            return None
        
        # Don't check too soon (minimum 3 minutes)
        now = datetime.datetime.utcnow()
        time_since_alert = (now - signal['first_seen']).total_seconds()
        if time_since_alert < 180:  # 3 minutes
            return None
        
        setup_data = signal.get('setup', {})
        if not setup_data:
            return None
        
        side = setup_data.get('side', '')
        entry = setup_data.get('entry_price', 0)
        tp_targets = setup_data.get('tp_targets', [])
        sl = setup_data.get('sl_price', 0)
        
        if entry == 0 or sl == 0:
            return None
        
        outcome = None
        
        # Check TP hits
        for i, tp in enumerate(tp_targets):
            if tp == 0:
                continue
                
            if side == "BUY" and current_price >= tp:
                pnl_pct = (current_price - entry) / entry * 100
                outcome = {
                    'type': f'TP{i+1}_HIT',
                    'price': current_price,
                    'pnl_pct': pnl_pct,
                    'bars_held': int(time_since_alert / 60),
                    'max_favorable': (signal['highest_price'] - entry) / entry * 100,
                    'max_adverse': (entry - signal['lowest_price']) / entry * 100,
                    'tp_level': i+1,
                    'signal_key': key
                }
                break
            elif side == "SELL" and current_price <= tp:
                pnl_pct = (entry - current_price) / entry * 100
                outcome = {
                    'type': f'TP{i+1}_HIT',
                    'price': current_price,
                    'pnl_pct': pnl_pct,
                    'bars_held': int(time_since_alert / 60),
                    'max_favorable': (entry - signal['lowest_price']) / entry * 100,
                    'max_adverse': (signal['highest_price'] - entry) / entry * 100,
                    'tp_level': i+1,
                    'signal_key': key
                }
                break
        
        # Check SL hit
        if not outcome:
            if (side == "BUY" and current_price <= sl) or (side == "SELL" and current_price >= sl):
                if side == "BUY":
                    pnl_pct = (current_price - entry) / entry * 100
                    max_fav = (signal['highest_price'] - entry) / entry * 100
                else:
                    pnl_pct = (entry - current_price) / entry * 100
                    max_fav = (entry - signal['lowest_price']) / entry * 100
                
                outcome = {
                    'type': 'SL_HIT',
                    'price': current_price,
                    'pnl_pct': pnl_pct,
                    'bars_held': int(time_since_alert / 60),
                    'max_favorable': max_fav,
                    'max_adverse': abs(entry - sl) / entry * 100,
                    'tp_level': 0,
                    'signal_key': key
                }
        
        if outcome:
            signal['outcome'] = outcome['type'].lower()
            signal['outcome_details'] = outcome
            signal['closed_at'] = now
            signal['closed_price'] = current_price
            signal['status'] = 'closed'
            
            # Update stats
            self.outcome_stats['active'] -= 1
            
            if 'TP1_HIT' in outcome['type']:
                self.outcome_stats['tp1_hits'] += 1
            elif 'TP2_HIT' in outcome['type']:
                self.outcome_stats['tp2_hits'] += 1
            elif 'TP3_HIT' in outcome['type']:
                self.outcome_stats['tp3_hits'] += 1
            elif outcome['type'] == 'SL_HIT':
                self.outcome_stats['sl_hits'] += 1
            
            # Update win rate
            wins = self.outcome_stats['tp1_hits'] + self.outcome_stats['tp2_hits'] + self.outcome_stats['tp3_hits']
            losses = self.outcome_stats['sl_hits']
            total_closed = wins + losses
            
            if total_closed > 0:
                self.outcome_stats['win_rate'] = wins / total_closed * 100
                
                # Update average PnL
                if 'avg_pnl_pct' not in self.outcome_stats or self.outcome_stats['avg_pnl_pct'] == 0:
                    self.outcome_stats['avg_pnl_pct'] = outcome['pnl_pct']
                else:
                    self.outcome_stats['avg_pnl_pct'] = (
                        self.outcome_stats['avg_pnl_pct'] * (total_closed - 1) + outcome['pnl_pct']
                    ) / total_closed
            
            return outcome
        
        return None
    
    def remove_signal_by_key(self, key: tuple, reason: str = "expired"):
        """Remove signal by key"""
        if key in self.active_signals:
            signal = self.active_signals.pop(key)
            signal['status'] = 'expired'
            signal['expired_at'] = datetime.datetime.utcnow()
            signal['expired_reason'] = reason
            
            self.outcome_stats['active'] -= 1
            self.outcome_stats['expired'] += 1
            
            symbol, side, score = key
            log.debug(f"Removed signal: {symbol} {side} Score:{score} - {reason}")
    
    def cleanup_old_signals(self):
        """Remove expired signals"""
        now = datetime.datetime.utcnow()
        expired_keys = []
        
        for key, data in self.active_signals.items():
            if data.get('status') == 'active':
                age_minutes = (now - data['first_seen']).total_seconds() / 60
                if age_minutes > (SIGNAL_VALIDITY_HOURS * 60):
                    expired_keys.append(key)
        
        for key in expired_keys:
            self.remove_signal_by_key(key, f"Expired after {SIGNAL_VALIDITY_HOURS}h")
        
        if expired_keys:
            log.info(f"Cleaned up {len(expired_keys)} expired signals")
    
    def get_stats(self) -> Dict:
        """Get tracking statistics"""
        active_count = len([s for s in self.active_signals.values() if s.get('status') == 'active'])
        
        buy_signals = 0
        sell_signals = 0
        
        for signal in self.active_signals.values():
            if signal.get('status') == 'active':
                setup = signal.get('setup', {})
                if setup.get('side') == 'BUY':
                    buy_signals += 1
                elif setup.get('side') == 'SELL':
                    sell_signals += 1
        
        return {
            'active_signals': active_count,
            'signals_by_side': {
                'BUY': buy_signals,
                'SELL': sell_signals
            },
            'outcome_stats': self.outcome_stats
        }

# Initialize tracker globally
signal_tracker = SignalTracker()
db_lock = asyncio.Lock()
db_conn = None

# ---------------- TELEGRAM ----------------
async def send_telegram(msg: str, parse_mode="HTML"):
    """Send message to Telegram"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not set")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True
            })
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

# ---------------- SAFE API WRAPPERS ----------------
async def safe_fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 100):
    """Fetch OHLCV with rate limiting"""
    return await rate_limiter.execute_with_backoff(
        exchange.fetch_ohlcv, symbol, timeframe=timeframe, limit=limit
    )

async def safe_fetch_ticker(exchange, symbol: str):
    """Fetch ticker with rate limiting"""
    return await rate_limiter.execute_with_backoff(
        exchange.fetch_ticker, symbol
    )

async def safe_fetch_tickers(exchange):
    """Fetch all tickers with rate limiting"""
    return await rate_limiter.execute_with_backoff(
        exchange.fetch_tickers
    )

# ---------------- UTILS ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 100):
    """Fetch OHLCV with timeout and rate limiting"""
    try:
        return await asyncio.wait_for(
            safe_fetch_ohlcv(exchange, symbol, timeframe, limit),
            timeout=8.0
        )
    except Exception as e:
        log.debug(f"Failed to fetch {symbol} {timeframe}: {e}")
        return None

def create_dataframe(ohlcv):
    """Create DataFrame from OHLCV"""
    if not ohlcv:
        return None
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

# ---------------- LIQUIDITY POOL IDENTIFICATION ----------------
def identify_liquidity_pools(df, timeframe="1h"):
    """
    Find liquidity pools by identifying:
    1. Equal highs/lows (where stops cluster)
    2. Consolidation zones
    """
    pools = {
        'buy_stops': [],   # Liquidity ABOVE price (shorts have stops here)
        'sell_stops': [],  # Liquidity BELOW price (longs have stops here)
        'equal_highs': [],
        'equal_lows': []
    }
    
    if df is None or len(df) < 20:
        return pools
    
    # Find equal highs (premium zones - where sellers trapped)
    window_size = 5 if timeframe == "15m" else 3
    
    for i in range(window_size, len(df)-window_size):
        # Check for equal highs
        window_highs = df['high'].iloc[i-window_size:i+window_size+1]
        current_high = df['high'].iloc[i]
        
        if current_high == window_highs.max():
            # Count how many candles have this same high
            same_high_count = (window_highs == current_high).sum()
            
            if same_high_count >= 2:
                pools['equal_highs'].append({
                    'price': float(current_high),
                    'timeframe': timeframe,
                    'candle_index': i,
                    'count': same_high_count,
                    'type': 'equal_high'
                })
                
                # This becomes a sell-stop pool (shorts entered here)
                pools['sell_stops'].append({
                    'price': float(current_high),
                    'reason': 'equal_high',
                    'timeframe': timeframe,
                    'strength': same_high_count
                })
    
    # Find equal lows (discount zones - where buyers trapped)
    for i in range(window_size, len(df)-window_size):
        # Check for equal lows
        window_lows = df['low'].iloc[i-window_size:i+window_size+1]
        current_low = df['low'].iloc[i]
        
        if current_low == window_lows.min():
            # Count how many candles have this same low
            same_low_count = (window_lows == current_low).sum()
            
            if same_low_count >= 2:
                pools['equal_lows'].append({
                    'price': float(current_low),
                    'timeframe': timeframe,
                    'candle_index': i,
                    'count': same_low_count,
                    'type': 'equal_low'
                })
                
                # This becomes a buy-stop pool (longs entered here)
                pools['buy_stops'].append({
                    'price': float(current_low),
                    'reason': 'equal_low',
                    'timeframe': timeframe,
                    'strength': same_low_count
                })
    
    # Identify recent consolidation zones (last 20% of data)
    recent_window = max(20, int(len(df) * 0.2))
    recent_data = df.iloc[-recent_window:]
    
    if len(recent_data) >= 10:
        recent_range = recent_data['high'].max() - recent_data['low'].min()
        avg_price = recent_data['close'].mean()
        
        # Check if it's a tight consolidation (less than 1% range)
        if recent_range / avg_price < 0.01:
            consolidation_high = recent_data['high'].max()
            consolidation_low = recent_data['low'].min()
            
            # Consolidation high becomes buy-stop liquidity
            pools['buy_stops'].append({
                'price': float(consolidation_high),
                'reason': 'consolidation_high',
                'timeframe': timeframe,
                'strength': 3
            })
            
            # Consolidation low becomes sell-stop liquidity
            pools['sell_stops'].append({
                'price': float(consolidation_low),
                'reason': 'consolidation_low',
                'timeframe': timeframe,
                'strength': 3
            })
    
    # Remove duplicates and sort
    for key in pools:
        if pools[key]:
            # Remove exact price duplicates
            seen_prices = set()
            unique_pools = []
            for pool in pools[key]:
                if pool['price'] not in seen_prices:
                    seen_prices.add(pool['price'])
                    unique_pools.append(pool)
            pools[key] = unique_pools
            
            # Sort by price
            if key in ['buy_stops', 'equal_lows']:
                pools[key].sort(key=lambda x: x['price'])
            else:
                pools[key].sort(key=lambda x: x['price'], reverse=True)
    
    return pools

# ---------------- LIQUIDITY-BASED TP/SL CALCULATION ----------------
async def calculate_liquidity_tp_sl(exchange, symbol: str, side: str, entry_price: float, 
                                   entry_type: str) -> Tuple[float, List[float], List[Dict], Dict]:
    """
    TP/SL based PURELY on liquidity pools
    Returns: (sl_price, tp_targets, tp_sources, liquidity_analysis)
    """
    
    # Get multi-timeframe data for liquidity analysis
    ohlcv_4h = await fetch_ohlcv(exchange, symbol, "4h", 100)
    ohlcv_1h = await fetch_ohlcv(exchange, symbol, "1h", 200)
    ohlcv_15m = await fetch_ohlcv(exchange, symbol, "15m", 300)
    
    df_4h = create_dataframe(ohlcv_4h)
    df_1h = create_dataframe(ohlcv_1h)
    df_15m = create_dataframe(ohlcv_15m)
    
    # Identify liquidity pools on all timeframes
    pools_4h = identify_liquidity_pools(df_4h, "4h") if df_4h is not None else {'buy_stops': [], 'sell_stops': [], 'equal_highs': [], 'equal_lows': []}
    pools_1h = identify_liquidity_pools(df_1h, "1h") if df_1h is not None else {'buy_stops': [], 'sell_stops': [], 'equal_highs': [], 'equal_lows': []}
    pools_15m = identify_liquidity_pools(df_15m, "15m") if df_15m is not None else {'buy_stops': [], 'sell_stops': [], 'equal_highs': [], 'equal_lows': []}
    
    # Combine all pools, giving higher weight to higher timeframes
    all_pools = {
        'buy_stops': [],
        'sell_stops': [],
        'equal_highs': [],
        'equal_lows': []
    }
    
    # Add with timeframe weighting
    for pool in pools_4h['buy_stops']:
        pool['weight'] = 3.0  # 4H has highest weight
        all_pools['buy_stops'].append(pool)
    
    for pool in pools_1h['buy_stops']:
        pool['weight'] = 2.0  # 1H medium weight
        all_pools['buy_stops'].append(pool)
    
    for pool in pools_15m['buy_stops']:
        pool['weight'] = 1.0  # 15M lowest weight
        all_pools['buy_stops'].append(pool)
    
    # Repeat for other pool types
    for pool_type in ['sell_stops', 'equal_highs', 'equal_lows']:
        for pool in pools_4h[pool_type]:
            pool['weight'] = 3.0
            all_pools[pool_type].append(pool)
        
        for pool in pools_1h[pool_type]:
            pool['weight'] = 2.0
            all_pools[pool_type].append(pool)
        
        for pool in pools_15m[pool_type]:
            pool['weight'] = 1.0
            all_pools[pool_type].append(pool)
    
    # Sort pools
    all_pools['buy_stops'].sort(key=lambda x: x['price'])
    all_pools['sell_stops'].sort(key=lambda x: x['price'], reverse=True)
    all_pools['equal_highs'].sort(key=lambda x: x['price'], reverse=True)
    all_pools['equal_lows'].sort(key=lambda x: x['price'])
    
    current_price = entry_price
    tp_targets = []
    tp_sources = []  # NEW: Store source info for each TP
    sl_price = 0.0
    sl_source = {}  # NEW: Store SL source info
    
    # ========== BUY SIGNAL LOGIC ==========
    if side == "BUY":
        # ----- STOP LOSS: Below nearest sell-stop liquidity -----
        sell_stops_below = [p for p in all_pools['sell_stops'] if p['price'] < current_price]
        
        if sell_stops_below:
            # Prioritize 4H pools, then 1H, then 15M
            for timeframe_weight in [3.0, 2.0, 1.0]:
                timeframe_pools = [p for p in sell_stops_below if p.get('weight', 1.0) == timeframe_weight]
                if timeframe_pools:
                    # Take the LOWEST pool in this timeframe
                    strongest_pool = min(timeframe_pools, key=lambda x: x['price'])
                    sl_price = strongest_pool['price'] * 0.997
                    sl_source = {
                        'type': 'sell_stop_pool',
                        'timeframe': strongest_pool.get('timeframe', 'unknown'),
                        'reason': strongest_pool.get('reason', 'equal_high'),
                        'strength': strongest_pool.get('strength', 1),
                        'original_price': strongest_pool['price']
                    }
                    break
            
            if sl_price == 0:
                strongest_pool = min(sell_stops_below, key=lambda x: x['price'])
                sl_price = strongest_pool['price'] * 0.995
                sl_source = {
                    'type': 'sell_stop_pool',
                    'timeframe': strongest_pool.get('timeframe', 'unknown'),
                    'reason': strongest_pool.get('reason', 'equal_high'),
                    'strength': strongest_pool.get('strength', 1),
                    'original_price': strongest_pool['price']
                }
        else:
            # No sell-stop pools found
            equal_lows_below = [p for p in all_pools['equal_lows'] if p['price'] < current_price]
            if equal_lows_below:
                most_recent_low = max(equal_lows_below, key=lambda x: x.get('candle_index', 0))
                sl_price = most_recent_low['price'] * 0.99
                sl_source = {
                    'type': 'equal_low',
                    'timeframe': most_recent_low.get('timeframe', 'unknown'),
                    'reason': 'recent_equal_low',
                    'strength': most_recent_low.get('count', 1),
                    'original_price': most_recent_low['price']
                }
            else:
                if df_15m is not None and len(df_15m) >= 10:
                    recent_low = df_15m['low'].iloc[-10:].min()
                    sl_price = float(recent_low) * 0.985
                    sl_source = {
                        'type': 'recent_low',
                        'timeframe': '15m',
                        'reason': 'recent_swing_low',
                        'strength': 1,
                        'original_price': float(recent_low)
                    }
                else:
                    sl_price = current_price * 0.97
                    sl_source = {
                        'type': 'fixed_percentage',
                        'timeframe': 'N/A',
                        'reason': 'emergency_3pct',
                        'strength': 0,
                        'original_price': sl_price / 0.97
                    }
        
        # Ensure SL is reasonable
        if sl_price > current_price * 0.995:
            sl_price = current_price * 0.985
            sl_source = {
                'type': 'adjusted',
                'timeframe': 'N/A',
                'reason': 'too_close_adjusted',
                'strength': 0,
                'original_price': current_price * 0.995
            }
        
        # ----- TAKE PROFIT: At buy-stop liquidity -----
        # TP1: Nearest buy-stop pool above entry
        buy_stops_above = [p for p in all_pools['buy_stops'] if p['price'] > current_price]
        
        if buy_stops_above:
            closest_buy_stop = min(buy_stops_above, key=lambda x: x['price'])
            tp1 = closest_buy_stop['price']
            tp_sources.append({
                'tp_level': 1,
                'type': 'buy_stop_pool',
                'timeframe': closest_buy_stop.get('timeframe', 'unknown'),
                'reason': closest_buy_stop.get('reason', 'equal_low'),
                'strength': closest_buy_stop.get('strength', 1),
                'original_price': closest_buy_stop['price']
            })
            
            # Check for equal highs as alternative
            if entry_type == "DISCOUNT_ZONE":
                equal_highs_above = [p for p in all_pools['equal_highs'] if p['price'] > current_price]
                if equal_highs_above:
                    closest_equal_high = min(equal_highs_above, key=lambda x: x['price'])
                    if abs(closest_equal_high['price'] - current_price) < abs(tp1 - current_price) * 1.5:
                        tp1 = closest_equal_high['price']
                        tp_sources[-1] = {
                            'tp_level': 1,
                            'type': 'equal_high',
                            'timeframe': closest_equal_high.get('timeframe', 'unknown'),
                            'reason': 'premium_zone',
                            'strength': closest_equal_high.get('count', 1),
                            'original_price': closest_equal_high['price']
                        }
        else:
            # No buy-stop pools
            equal_highs_above = [p for p in all_pools['equal_highs'] if p['price'] > current_price]
            if equal_highs_above:
                tp1 = min(equal_highs_above, key=lambda x: x['price'])['price']
                tp_sources.append({
                    'tp_level': 1,
                    'type': 'equal_high',
                    'timeframe': min(equal_highs_above, key=lambda x: x['price']).get('timeframe', 'unknown'),
                    'reason': 'premium_zone',
                    'strength': min(equal_highs_above, key=lambda x: x['price']).get('count', 1),
                    'original_price': min(equal_highs_above, key=lambda x: x['price'])['price']
                })
            else:
                risk = current_price - sl_price
                tp1 = current_price + (risk * 1.2)
                tp_sources.append({
                    'tp_level': 1,
                    'type': 'risk_based',
                    'timeframe': 'N/A',
                    'reason': 'no_pool_found',
                    'strength': 0,
                    'original_price': tp1
                })
        
        # TP2: Next significant liquidity pool above TP1
        buy_stops_above_tp1 = [p for p in all_pools['buy_stops'] if p['price'] > tp1]
        
        if buy_stops_above_tp1:
            # Look for a pool with higher timeframe weight
            significant_pools = [p for p in buy_stops_above_tp1 if p.get('weight', 1.0) >= 2.0]
            if significant_pools:
                tp2 = min(significant_pools, key=lambda x: x['price'])['price']
                tp_sources.append({
                    'tp_level': 2,
                    'type': 'buy_stop_pool',
                    'timeframe': min(significant_pools, key=lambda x: x['price']).get('timeframe', 'unknown'),
                    'reason': 'higher_timeframe_pool',
                    'strength': min(significant_pools, key=lambda x: x['price']).get('strength', 1),
                    'original_price': min(significant_pools, key=lambda x: x['price'])['price']
                })
            else:
                tp2 = min(buy_stops_above_tp1, key=lambda x: x['price'])['price']
                tp_sources.append({
                    'tp_level': 2,
                    'type': 'buy_stop_pool',
                    'timeframe': min(buy_stops_above_tp1, key=lambda x: x['price']).get('timeframe', 'unknown'),
                    'reason': 'next_pool',
                    'strength': min(buy_stops_above_tp1, key=lambda x: x['price']).get('strength', 1),
                    'original_price': min(buy_stops_above_tp1, key=lambda x: x['price'])['price']
                })
        else:
            # Look for equal highs above TP1
            equal_highs_above_tp1 = [p for p in all_pools['equal_highs'] if p['price'] > tp1]
            if equal_highs_above_tp1:
                tp2 = min(equal_highs_above_tp1, key=lambda x: x['price'])['price']
                tp_sources.append({
                    'tp_level': 2,
                    'type': 'equal_high',
                    'timeframe': min(equal_highs_above_tp1, key=lambda x: x['price']).get('timeframe', 'unknown'),
                    'reason': 'premium_zone',
                    'strength': min(equal_highs_above_tp1, key=lambda x: x['price']).get('count', 1),
                    'original_price': min(equal_highs_above_tp1, key=lambda x: x['price'])['price']
                })
            else:
                risk = current_price - sl_price
                tp2 = current_price + (risk * 2.0)
                tp_sources.append({
                    'tp_level': 2,
                    'type': 'risk_based',
                    'timeframe': 'N/A',
                    'reason': 'no_pool_found',
                    'strength': 0,
                    'original_price': tp2
                })
        
        tp_targets = [tp1, tp2]
        
        # TP3: Major liquidity pool (only for strong setups)
        if entry_type == "DISCOUNT_ZONE" and len(all_pools['equal_highs']) >= 2:
            if df_4h is not None and len(df_4h) >= 10:
                major_high_idx = df_4h['high'].iloc[-int(len(df_4h)*0.5):].idxmax()
                major_high = df_4h['high'].iloc[major_high_idx]
                
                if major_high > tp2 * 1.05:
                    tp_targets.append(float(major_high))
                    tp_sources.append({
                        'tp_level': 3,
                        'type': 'major_swing_high',
                        'timeframe': '4h',
                        'reason': 'major_structure',
                        'strength': 3,
                        'original_price': float(major_high)
                    })
    
    # ========== SELL SIGNAL LOGIC ==========
    else:
        # ----- STOP LOSS: Above nearest buy-stop liquidity -----
        buy_stops_above = [p for p in all_pools['buy_stops'] if p['price'] > current_price]
        
        if buy_stops_above:
            for timeframe_weight in [3.0, 2.0, 1.0]:
                timeframe_pools = [p for p in buy_stops_above if p.get('weight', 1.0) == timeframe_weight]
                if timeframe_pools:
                    strongest_pool = max(timeframe_pools, key=lambda x: x['price'])
                    sl_price = strongest_pool['price'] * 1.003
                    sl_source = {
                        'type': 'buy_stop_pool',
                        'timeframe': strongest_pool.get('timeframe', 'unknown'),
                        'reason': strongest_pool.get('reason', 'equal_low'),
                        'strength': strongest_pool.get('strength', 1),
                        'original_price': strongest_pool['price']
                    }
                    break
            
            if sl_price == 0:
                strongest_pool = max(buy_stops_above, key=lambda x: x['price'])
                sl_price = strongest_pool['price'] * 1.005
                sl_source = {
                    'type': 'buy_stop_pool',
                    'timeframe': strongest_pool.get('timeframe', 'unknown'),
                    'reason': strongest_pool.get('reason', 'equal_low'),
                    'strength': strongest_pool.get('strength', 1),
                    'original_price': strongest_pool['price']
                }
        else:
            equal_highs_above = [p for p in all_pools['equal_highs'] if p['price'] > current_price]
            if equal_highs_above:
                most_recent_high = max(equal_highs_above, key=lambda x: x.get('candle_index', 0))
                sl_price = most_recent_high['price'] * 1.01
                sl_source = {
                    'type': 'equal_high',
                    'timeframe': most_recent_high.get('timeframe', 'unknown'),
                    'reason': 'recent_equal_high',
                    'strength': most_recent_high.get('count', 1),
                    'original_price': most_recent_high['price']
                }
            else:
                if df_15m is not None and len(df_15m) >= 10:
                    recent_high = df_15m['high'].iloc[-10:].max()
                    sl_price = float(recent_high) * 1.015
                    sl_source = {
                        'type': 'recent_high',
                        'timeframe': '15m',
                        'reason': 'recent_swing_high',
                        'strength': 1,
                        'original_price': float(recent_high)
                    }
                else:
                    sl_price = current_price * 1.03
                    sl_source = {
                        'type': 'fixed_percentage',
                        'timeframe': 'N/A',
                        'reason': 'emergency_3pct',
                        'strength': 0,
                        'original_price': sl_price / 1.03
                    }
        
        # Ensure SL is reasonable
        if sl_price < current_price * 1.005:
            sl_price = current_price * 1.015
            sl_source = {
                'type': 'adjusted',
                'timeframe': 'N/A',
                'reason': 'too_close_adjusted',
                'strength': 0,
                'original_price': current_price * 1.005
            }
        
        # ----- TAKE PROFIT: At sell-stop liquidity -----
        # TP1: Nearest sell-stop pool below entry
        sell_stops_below = [p for p in all_pools['sell_stops'] if p['price'] < current_price]
        
        if sell_stops_below:
            closest_sell_stop = max(sell_stops_below, key=lambda x: x['price'])
            tp1 = closest_sell_stop['price']
            tp_sources.append({
                'tp_level': 1,
                'type': 'sell_stop_pool',
                'timeframe': closest_sell_stop.get('timeframe', 'unknown'),
                'reason': closest_sell_stop.get('reason', 'equal_high'),
                'strength': closest_sell_stop.get('strength', 1),
                'original_price': closest_sell_stop['price']
            })
            
            if entry_type == "PREMIUM_ZONE":
                equal_lows_below = [p for p in all_pools['equal_lows'] if p['price'] < current_price]
                if equal_lows_below:
                    closest_equal_low = max(equal_lows_below, key=lambda x: x['price'])
                    if abs(current_price - closest_equal_low['price']) < abs(current_price - tp1) * 1.5:
                        tp1 = closest_equal_low['price']
                        tp_sources[-1] = {
                            'tp_level': 1,
                            'type': 'equal_low',
                            'timeframe': closest_equal_low.get('timeframe', 'unknown'),
                            'reason': 'discount_zone',
                            'strength': closest_equal_low.get('count', 1),
                            'original_price': closest_equal_low['price']
                        }
        else:
            equal_lows_below = [p for p in all_pools['equal_lows'] if p['price'] < current_price]
            if equal_lows_below:
                tp1 = max(equal_lows_below, key=lambda x: x['price'])['price']
                tp_sources.append({
                    'tp_level': 1,
                    'type': 'equal_low',
                    'timeframe': max(equal_lows_below, key=lambda x: x['price']).get('timeframe', 'unknown'),
                    'reason': 'discount_zone',
                    'strength': max(equal_lows_below, key=lambda x: x['price']).get('count', 1),
                    'original_price': max(equal_lows_below, key=lambda x: x['price'])['price']
                })
            else:
                if df_1h is not None and len(df_1h) >= 20:
                    recent_low = df_1h['low'].iloc[-20:].min()
                    tp1 = float(recent_low)
                    tp_sources.append({
                        'tp_level': 1,
                        'type': 'recent_swing',
                        'timeframe': '1h',
                        'reason': 'recent_low',
                        'strength': 1,
                        'original_price': float(recent_low)
                    })
                else:
                    risk = sl_price - current_price
                    tp1 = current_price - (risk * 1.2)
                    tp_sources.append({
                        'tp_level': 1,
                        'type': 'risk_based',
                        'timeframe': 'N/A',
                        'reason': 'no_pool_found',
                        'strength': 0,
                        'original_price': tp1
                    })
        
        # TP2: Next significant liquidity pool below TP1
        sell_stops_below_tp1 = [p for p in all_pools['sell_stops'] if p['price'] < tp1]
        
        if sell_stops_below_tp1:
            significant_pools = [p for p in sell_stops_below_tp1 if p.get('weight', 1.0) >= 2.0]
            if significant_pools:
                tp2 = max(significant_pools, key=lambda x: x['price'])['price']
                tp_sources.append({
                    'tp_level': 2,
                    'type': 'sell_stop_pool',
                    'timeframe': max(significant_pools, key=lambda x: x['price']).get('timeframe', 'unknown'),
                    'reason': 'higher_timeframe_pool',
                    'strength': max(significant_pools, key=lambda x: x['price']).get('strength', 1),
                    'original_price': max(significant_pools, key=lambda x: x['price'])['price']
                })
            else:
                tp2 = max(sell_stops_below_tp1, key=lambda x: x['price'])['price']
                tp_sources.append({
                    'tp_level': 2,
                    'type': 'sell_stop_pool',
                    'timeframe': max(sell_stops_below_tp1, key=lambda x: x['price']).get('timeframe', 'unknown'),
                    'reason': 'next_pool',
                    'strength': max(sell_stops_below_tp1, key=lambda x: x['price']).get('strength', 1),
                    'original_price': max(sell_stops_below_tp1, key=lambda x: x['price'])['price']
                })
        else:
            equal_lows_below_tp1 = [p for p in all_pools['equal_lows'] if p['price'] < tp1]
            if equal_lows_below_tp1:
                tp2 = max(equal_lows_below_tp1, key=lambda x: x['price'])['price']
                tp_sources.append({
                    'tp_level': 2,
                    'type': 'equal_low',
                    'timeframe': max(equal_lows_below_tp1, key=lambda x: x['price']).get('timeframe', 'unknown'),
                    'reason': 'discount_zone',
                    'strength': max(equal_lows_below_tp1, key=lambda x: x['price']).get('count', 1),
                    'original_price': max(equal_lows_below_tp1, key=lambda x: x['price'])['price']
                })
            else:
                risk = sl_price - current_price
                tp2 = current_price - (risk * 2.0)
                tp_sources.append({
                    'tp_level': 2,
                    'type': 'risk_based',
                    'timeframe': 'N/A',
                    'reason': 'no_pool_found',
                    'strength': 0,
                    'original_price': tp2
                })
        
        tp_targets = [tp1, tp2]
        
        # TP3: Major liquidity pool
        if entry_type == "PREMIUM_ZONE" and len(all_pools['equal_lows']) >= 2:
            if df_4h is not None and len(df_4h) >= 10:
                major_low_idx = df_4h['low'].iloc[-int(len(df_4h)*0.5):].idxmin()
                major_low = df_4h['low'].iloc[major_low_idx]
                
                if major_low < tp2 * 0.95:
                    tp_targets.append(float(major_low))
                    tp_sources.append({
                        'tp_level': 3,
                        'type': 'major_swing_low',
                        'timeframe': '4h',
                        'reason': 'major_structure',
                        'strength': 3,
                        'original_price': float(major_low)
                    })
    
    # ========== FINAL VALIDATION ==========
    if side == "BUY":
        if sl_price >= current_price:
            sl_price = current_price * 0.99
        
        tp_targets = [max(tp, current_price * 1.005) for tp in tp_targets]
        
        filtered_tps = []
        filtered_sources = []
        prev_tp = 0
        for tp, source in zip(tp_targets, tp_sources):
            if prev_tp == 0 or tp > prev_tp * 1.02:
                filtered_tps.append(tp)
                filtered_sources.append(source)
                prev_tp = tp
        tp_targets = filtered_tps[:3]
        tp_sources = filtered_sources[:3]
        
    else:
        if sl_price <= current_price:
            sl_price = current_price * 1.01
        
        tp_targets = [min(tp, current_price * 0.995) for tp in tp_targets]
        
        filtered_tps = []
        filtered_sources = []
        prev_tp = float('inf')
        for tp, source in zip(tp_targets, tp_sources):
            if prev_tp == float('inf') or tp < prev_tp * 0.98:
                filtered_tps.append(tp)
                filtered_sources.append(source)
                prev_tp = tp
        tp_targets = filtered_tps[:3]
        tp_sources = filtered_sources[:3]
    
    # Calculate R:R ratio
    risk = abs(current_price - sl_price)
    if risk > 0 and len(tp_targets) > 0:
        reward = abs(tp_targets[0] - current_price)
        rr_ratio = reward / risk
    else:
        rr_ratio = 0
    
    # Prepare liquidity analysis
    liquidity_analysis = {
        'side': side,
        'entry_type': entry_type,
        'identified_pools': {
            'buy_stops': len(all_pools['buy_stops']),
            'sell_stops': len(all_pools['sell_stops']),
            'equal_highs': len(all_pools['equal_highs']),
            'equal_lows': len(all_pools['equal_lows'])
        },
        'sl_source': sl_source,  # NEW: Include SL source
        'tp_sources': tp_sources,  # NEW: Include TP sources
        'rr_ratio': rr_ratio,
        'risk_pct': risk / current_price * 100,
        'reward_pct': abs(tp_targets[0] - current_price) / current_price * 100 if tp_targets else 0
    }
    
    return sl_price, tp_targets, tp_sources, liquidity_analysis

# ---------------- LAYER 1: FAST ELIGIBILITY CHECK ----------------
async def check_eligibility_fast(exchange, symbol: str) -> SetupEligibility:
    """LAYER 1: FAST FILTER - ELIGIBILITY ONLY"""
    
    # Get current price
    try:
        ticker = await safe_fetch_ticker(exchange, symbol)
        current_price = ticker.get("last", 0)
        if current_price == 0:
            return SetupEligibility(eligible=False, disqualify_reason="No price")
    except Exception as e:
        log.debug(f"Failed to get ticker for {symbol}: {e}")
        return SetupEligibility(eligible=False, disqualify_reason="Ticker error")
    
    # Quick 1H trend
    ohlcv_1h = await fetch_ohlcv(exchange, symbol, "1h", 50)
    if not ohlcv_1h or len(ohlcv_1h) < 30:
        return SetupEligibility(eligible=False, disqualify_reason="Insufficient 1H data")
    
    df_1h = create_dataframe(ohlcv_1h)
    if df_1h is None:
        return SetupEligibility(eligible=False, disqualify_reason="Dataframe error")
    
    # Fast trend detection
    try:
        df_1h['ema_20'] = df_1h['close'].ewm(span=20).mean()
        df_1h['ema_50'] = df_1h['close'].ewm(span=50).mean()
        
        latest_ema20 = df_1h['ema_20'].iloc[-1]
        latest_ema50 = df_1h['ema_50'].iloc[-1]
        
        # Determine bias
        if latest_ema20 > latest_ema50:
            bias = "BULLISH"
            potential_side = "BUY"
        elif latest_ema20 < latest_ema50:
            bias = "BEARISH"
            potential_side = "SELL"
        else:
            # Check price position relative to EMAs
            if current_price > latest_ema20:
                bias = "BULLISH"
                potential_side = "BUY"
            else:
                bias = "BEARISH"
                potential_side = "SELL"
    except Exception as e:
        log.debug(f"Trend detection error for {symbol}: {e}")
        return SetupEligibility(eligible=False, disqualify_reason="Trend detection error")
    
    # Check 15m for entry setup
    ohlcv_15m = await fetch_ohlcv(exchange, symbol, "15m", 30)
    if not ohlcv_15m or len(ohlcv_15m) < 10:
        return SetupEligibility(eligible=False, disqualify_reason="Insufficient 15m data")
    
    df_15m = create_dataframe(ohlcv_15m)
    if df_15m is None:
        return SetupEligibility(eligible=False, disqualify_reason="15m dataframe error")
    
    # Look for entry setup
    entry_found = False
    entry_price = current_price
    entry_type = ""
    
    try:
        # Check recent price action
        recent_low_15m = df_15m['low'].iloc[-5:].min()
        recent_high_15m = df_15m['high'].iloc[-5:].max()
        
        if potential_side == "BUY":
            # Looking for discount zone or bullish engulfing
            if current_price <= recent_low_15m * 1.01:  # Within 1% of recent low
                entry_price = current_price
                entry_type = "DISCOUNT_ZONE"
                entry_found = True
            elif len(df_15m) >= 3:
                # Check for bullish engulfing
                last_candle = df_15m.iloc[-1]
                prev_candle = df_15m.iloc[-2]
                
                if (prev_candle['close'] < prev_candle['open'] and  # Previous bearish
                    last_candle['close'] > last_candle['open'] and   # Current bullish
                    last_candle['close'] > prev_candle['open'] and   # Closes above previous open
                    last_candle['open'] < prev_candle['close']):     # Opens below previous close
                    entry_price = last_candle['close']
                    entry_type = "BULLISH_ENGULFING"
                    entry_found = True
        else:  # SELL
            if current_price >= recent_high_15m * 0.99:  # Within 1% of recent high
                entry_price = current_price
                entry_type = "PREMIUM_ZONE"
                entry_found = True
            elif len(df_15m) >= 3:
                # Check for bearish engulfing
                last_candle = df_15m.iloc[-1]
                prev_candle = df_15m.iloc[-2]
                
                if (prev_candle['close'] > prev_candle['open'] and  # Previous bullish
                    last_candle['close'] < last_candle['open'] and   # Current bearish
                    last_candle['close'] < prev_candle['open'] and   # Closes below previous open
                    last_candle['open'] > prev_candle['close']):     # Opens above previous close
                    entry_price = last_candle['close']
                    entry_type = "BEARISH_ENGULFING"
                    entry_found = True
    except Exception as e:
        log.debug(f"Entry detection error for {symbol}: {e}")
        return SetupEligibility(eligible=False, disqualify_reason="Entry detection error")
    
    if not entry_found:
        return SetupEligibility(eligible=False, disqualify_reason="No entry setup detected")
    
    return SetupEligibility(
        eligible=True,
        side=potential_side,
        entry_price=entry_price,
        entry_type=entry_type
    )

# ---------------- LAYER 2: QUALITY ANALYSIS ----------------
async def analyze_quality(exchange, symbol: str, eligibility: SetupEligibility, 
                         liquidity_setup: LiquiditySetup) -> SetupQuality:
    """LAYER 2: QUALITY ANALYSIS WITH DETAILED 8-STEP TRACKING"""
    
    side = eligibility.side
    entry_type = eligibility.entry_type
    entry_price = eligibility.entry_price
    
    # Initialize scores
    sweep_strength = 0.0
    structure_shift = False
    from_liquidity_exists = False
    confirmation_candle = False
    htfc_alignment_score = 0.0
    
    # Track specific details for each step
    step_details_dict = {}
    
    # Initialize 8-step tracking with detailed info
    eight_steps = {
        'step_1_htf_bias': False,
        'step_2_zone_type': False,
        'step_3_liquidity_sweep': False,
        'step_4_structure_shift': False,
        'step_5_from_liquidity': False,
        'step_6_confirmation_candle': False,
        'step_7_entry_validity': False,
        'step_8_liquidity_alignment': False,
        
        'step_details': step_details_dict,  # Will be populated below
        'rr_ratio': liquidity_setup.rr_ratio,
        
        # Additional details for each step
        'step_specifics': {
            '1': {'trend': '', 'score': 0.0, 'alignment': ''},
            '2': {'entry_type': entry_type, 'zone_quality': ''},
            '3': {'strength': 0.0, 'sweep_type': '', 'details': ''},
            '4': {'shift_type': '', 'confirmed': False},
            '5': {'liquidity_type': '', 'present': False},
            '6': {'candle_type': '', 'direction': ''},
            '7': {'distance_pct': 0.0, 'in_zone': False},
            '8': {'pools_analyzed': 0, 'alignment_score': 0}
        }
    }
    
    try:
        # Get current price
        ticker = await safe_fetch_ticker(exchange, symbol)
        current_price = ticker.get("last", entry_price)
        
        # === STEP 1: HTF Bias Alignment ===
        ohlcv_4h = await fetch_ohlcv(exchange, symbol, "4h", 50)
        htf_trend = "NEUTRAL"
        
        if ohlcv_4h:
            df_4h = create_dataframe(ohlcv_4h)
            if df_4h is not None and len(df_4h) >= 20:
                df_4h['ema_20'] = df_4h['close'].ewm(span=20).mean()
                df_4h['ema_50'] = df_4h['close'].ewm(span=50).mean()
                
                ema20 = df_4h['ema_20'].iloc[-1]
                ema50 = df_4h['ema_50'].iloc[-1]
                
                if side == "BUY":
                    htfc_alignment_score = 1.0 if ema20 > ema50 else 0.5
                    eight_steps['step_1_htf_bias'] = htfc_alignment_score >= 0.7
                    htf_trend = "BULLISH" if ema20 > ema50 else "BEARISH" if ema20 < ema50 else "NEUTRAL"
                else:
                    htfc_alignment_score = 1.0 if ema20 < ema50 else 0.5
                    eight_steps['step_1_htf_bias'] = htfc_alignment_score >= 0.7
                    htf_trend = "BEARISH" if ema20 < ema50 else "BULLISH" if ema20 > ema50 else "NEUTRAL"
                
                eight_steps['step_specifics']['1']['trend'] = htf_trend
                eight_steps['step_specifics']['1']['score'] = htfc_alignment_score
                eight_steps['step_specifics']['1']['alignment'] = "Aligned" if eight_steps['step_1_htf_bias'] else "Not Aligned"
        
        # Step 1 details
        step_details_dict['1'] = f"Higher Timeframe Bias: {htf_trend} (Score: {htfc_alignment_score:.2f}/1.0)"
        
        # === STEP 2: Premium/Discount Zone ===
        if side == "BUY" and entry_type in ["DISCOUNT_ZONE", "BULLISH_ENGULFING"]:
            eight_steps['step_2_zone_type'] = True
            zone_quality = "Optimal"
        elif side == "SELL" and entry_type in ["PREMIUM_ZONE", "BEARISH_ENGULFING"]:
            eight_steps['step_2_zone_type'] = True
            zone_quality = "Optimal"
        else:
            zone_quality = "Suboptimal"
        
        eight_steps['step_specifics']['2']['entry_type'] = entry_type
        eight_steps['step_specifics']['2']['zone_quality'] = zone_quality
        
        step_details_dict['2'] = f"Entry Type: {entry_type} ({zone_quality})"
        
        # === STEP 3: Liquidity Sweep ===
        ohlcv_15m = await fetch_ohlcv(exchange, symbol, "15m", 50)
        sweep_type = "None"
        sweep_details = ""
        
        if ohlcv_15m:
            df_15m = create_dataframe(ohlcv_15m)
            if df_15m is not None and len(df_15m) >= 20:
                if side == "BUY":
                    # Look for sweep of lows
                    recent_low = df_15m['low'].iloc[-5:].min()
                    prev_lows = df_15m['low'].iloc[-20:-5]
                    
                    if len(prev_lows) > 0:
                        prev_significant_low = prev_lows.min()
                        if recent_low < prev_significant_low * 0.995:
                            sweep_strength = 0.8
                            eight_steps['step_3_liquidity_sweep'] = True
                            sweep_type = "LOW_SWEEP"
                            sweep_details = f"Swept low: {prev_significant_low:.8f} → {recent_low:.8f}"
                            
                            # Check if it was a clear wick (liquidity grab)
                            sweep_idx = df_15m['low'].iloc[-5:].idxmin()  # Get most recent low in last 5 candles
                            if sweep_idx < len(df_15m) - 2:
                                sweep_candle = df_15m.iloc[sweep_idx]
                                body_size = abs(sweep_candle['close'] - sweep_candle['open'])
                                lower_wick = min(sweep_candle['open'], sweep_candle['close']) - sweep_candle['low']
                                
                                if lower_wick > body_size * 1.5:
                                    sweep_strength = 1.0
                                    from_liquidity_exists = True
                                    eight_steps['step_5_from_liquidity'] = True
                                    sweep_details += " (Strong wick - smart money entry)"
                else:
                    # Look for sweep of highs
                    recent_high = df_15m['high'].iloc[-5:].max()
                    prev_highs = df_15m['high'].iloc[-20:-5]
                    
                    if len(prev_highs) > 0:
                        prev_significant_high = prev_highs.max()
                        if recent_high > prev_significant_high * 1.005:
                            sweep_strength = 0.8
                            eight_steps['step_3_liquidity_sweep'] = True
                            sweep_type = "HIGH_SWEEP"
                            sweep_details = f"Swept high: {prev_significant_high:.8f} → {recent_high:.8f}"
                            
                            sweep_idx = df_15m['high'].iloc[-5:].idxmax()  # Get most recent high in last 5 candles
                            if sweep_idx < len(df_15m) - 2:
                                sweep_candle = df_15m.iloc[sweep_idx]
                                body_size = abs(sweep_candle['close'] - sweep_candle['open'])
                                upper_wick = sweep_candle['high'] - max(sweep_candle['open'], sweep_candle['close'])
                                
                                if upper_wick > body_size * 1.5:
                                    sweep_strength = 1.0
                                    from_liquidity_exists = True
                                    eight_steps['step_5_from_liquidity'] = True
                                    sweep_details += " (Strong wick - smart money entry)"
        
        eight_steps['step_specifics']['3']['strength'] = sweep_strength
        eight_steps['step_specifics']['3']['sweep_type'] = sweep_type
        eight_steps['step_specifics']['3']['details'] = sweep_details
        
        step_details_dict['3'] = f"Liquidity Sweep: {sweep_type} (Strength: {sweep_strength:.2f}/1.0)"
        if sweep_details:
            step_details_dict['3'] += f"\n   {sweep_details}"
        
        # === STEP 4: Structure Shift ===
        shift_type = "None"
        if ohlcv_4h:
            df_4h = create_dataframe(ohlcv_4h)
            if df_4h is not None and len(df_4h) >= 11:
                if side == "BUY":
                    # Check for higher high
                    recent_highs = df_4h['high'].iloc[-10:-1]
                    if len(recent_highs) > 0:
                        previous_high = recent_highs.max()
                        current_high = df_4h['high'].iloc[-1]
                        
                        if current_high > previous_high:
                            structure_shift = True
                            eight_steps['step_4_structure_shift'] = True
                            shift_type = "HIGHER_HIGH"
                else:
                    # Check for lower low
                    recent_lows = df_4h['low'].iloc[-10:-1]
                    if len(recent_lows) > 0:
                        previous_low = recent_lows.min()
                        current_low = df_4h['low'].iloc[-1]
                        
                        if current_low < previous_low:
                            structure_shift = True
                            eight_steps['step_4_structure_shift'] = True
                            shift_type = "LOWER_LOW"
        
        eight_steps['step_specifics']['4']['shift_type'] = shift_type
        eight_steps['step_specifics']['4']['confirmed'] = structure_shift
        
        step_details_dict['4'] = f"Structure Shift: {shift_type} ({'✅ Confirmed' if structure_shift else '❌ Not Confirmed'})"
        
        # === STEP 5: FROM Liquidity ===
        liquidity_type = "None"
        if from_liquidity_exists:
            liquidity_type = "Smart Money Entry"
        
        eight_steps['step_specifics']['5']['liquidity_type'] = liquidity_type
        eight_steps['step_specifics']['5']['present'] = from_liquidity_exists
        
        step_details_dict['5'] = f"FROM Liquidity: {liquidity_type} ({'✅ Present' if from_liquidity_exists else '❌ Absent'})"
        
        # === STEP 6: Confirmation Candle ===
        ohlcv_5m = await fetch_ohlcv(exchange, symbol, "5m", 10)
        candle_type = "None"
        candle_direction = ""
        
        if ohlcv_5m:
            df_5m = create_dataframe(ohlcv_5m)
            if df_5m is not None and len(df_5m) >= 3:
                if side == "BUY":
                    # Check for bullish confirmation (close above open)
                    last_candle = df_5m.iloc[-1]
                    if last_candle['close'] > last_candle['open']:
                        confirmation_candle = True
                        eight_steps['step_6_confirmation_candle'] = True
                        candle_type = "BULLISH"
                        candle_direction = "Up"
                else:
                    last_candle = df_5m.iloc[-1]
                    if last_candle['close'] < last_candle['open']:
                        confirmation_candle = True
                        eight_steps['step_6_confirmation_candle'] = True
                        candle_type = "BEARISH"
                        candle_direction = "Down"
        
        eight_steps['step_specifics']['6']['candle_type'] = candle_type
        eight_steps['step_specifics']['6']['direction'] = candle_direction
        
        step_details_dict['6'] = f"Confirmation Candle: {candle_type} ({'✅ Confirmed' if confirmation_candle else '❌ Not Confirmed'})"
        
        # === STEP 7: Entry Validity ===
        # Check if current price is still near entry
        price_diff_pct = abs(current_price - entry_price) / entry_price * 100
        in_zone = price_diff_pct <= 1.5
        eight_steps['step_7_entry_validity'] = in_zone
        
        eight_steps['step_specifics']['7']['distance_pct'] = price_diff_pct
        eight_steps['step_specifics']['7']['in_zone'] = in_zone
        
        step_details_dict['7'] = f"Entry Validity: Distance {price_diff_pct:.2f}% ({'✅ In Zone' if in_zone else '❌ Outside Zone'})"
        
        # === STEP 8: Liquidity Alignment ===
        # Check if TP/SL are based on liquidity pools
        liquidity_analysis = liquidity_setup.liquidity_analysis
        pools_analyzed = 0
        alignment_score = 0
        
        if liquidity_analysis:
            pools = liquidity_analysis.get('identified_pools', {})
            pools_analyzed = sum(pools.values())
            
            # Calculate alignment score based on pools found
            if pools_analyzed >= 8:
                alignment_score = 1.0
            elif pools_analyzed >= 4:
                alignment_score = 0.7
            elif pools_analyzed >= 2:
                alignment_score = 0.5
            
            eight_steps['step_8_liquidity_alignment'] = pools_analyzed >= 2
        
        eight_steps['step_specifics']['8']['pools_analyzed'] = pools_analyzed
        eight_steps['step_specifics']['8']['alignment_score'] = alignment_score
        
        step_details_dict['8'] = f"Liquidity Alignment: {pools_analyzed} pools analyzed ({'✅ Aligned' if eight_steps['step_8_liquidity_alignment'] else '❌ Weak Alignment'})"
        
    except Exception as e:
        log.debug(f"Quality analysis error for {symbol}: {e}")
    
    # Calculate total score (0-5)
    total_score = (
        sweep_strength +
        (1.0 if structure_shift else 0.0) +
        (0.5 if from_liquidity_exists else 0.0) +
        (0.5 if confirmation_candle else 0.0) +
        htfc_alignment_score +
        (0.5 if eight_steps['step_7_entry_validity'] else 0.0) +
        (0.5 if eight_steps['step_8_liquidity_alignment'] else 0.0)
    )
    
    return SetupQuality(
        sweep_strength=sweep_strength,
        structure_shift=structure_shift,
        from_liquidity_exists=from_liquidity_exists,
        confirmation_candle=confirmation_candle,
        htfc_alignment_score=htfc_alignment_score,
        total_score=total_score,
        eight_steps_status=eight_steps
    )

# ---------------- FAST SCANNING ----------------
async def scan_symbol_fast(exchange, symbol: str) -> Optional[Dict]:
    """ULTRA-FAST scanning with liquidity-based TP/SL"""
    
    try:
        # LAYER 1: Eligibility check
        eligibility = await check_eligibility_fast(exchange, symbol)
        
        if not eligibility.eligible:
            return None
        
        # Calculate liquidity-based TP/SL WITH SOURCE INFO
        sl_price, tp_targets, tp_sources, liquidity_analysis = await calculate_liquidity_tp_sl(
            exchange, symbol, eligibility.side, eligibility.entry_price, eligibility.entry_type
        )
        
        # Skip if no valid TP/SL
        if sl_price == 0 or not tp_targets:
            return None
        
        # Create liquidity setup WITH SOURCE INFO
        risk = abs(eligibility.entry_price - sl_price)
        reward = abs(tp_targets[0] - eligibility.entry_price) if tp_targets else 0
        rr_ratio = reward / risk if risk > 0 else 0
        
        liquidity_setup = LiquiditySetup(
            sl_price=sl_price,
            tp_targets=tp_targets,
            tp_sources=tp_sources,  # NEW: Include TP sources
            liquidity_analysis=liquidity_analysis,
            rr_ratio=rr_ratio
        )
        
        # LAYER 2: Quality analysis
        quality = await analyze_quality(exchange, symbol, eligibility, liquidity_setup)
        
        # Skip if quality too low
        if quality.total_score < MIN_QUALITY_SCORE:
            return None
        
        # Get current price
        ticker = await safe_fetch_ticker(exchange, symbol)
        current_price = ticker.get("last", eligibility.entry_price)
        
        setup = {
            "symbol": symbol,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "side": eligibility.side,
            "current_price": current_price,
            "entry_price": eligibility.entry_price,
            "entry_type": eligibility.entry_type,
            "sl_price": sl_price,
            "tp_targets": tp_targets,
            "tp_sources": tp_sources,  # NEW: Include TP sources in setup
            "risk": risk,
            "reward": reward,
            "rr_ratio": rr_ratio,
            
            "quality": {
                "tier": quality.quality_tier,
                "total_score": quality.total_score,
                "sweep_strength": quality.sweep_strength,
                "structure_shift": quality.structure_shift,
                "from_liquidity": quality.from_liquidity_exists,
                "confirmation_candle": quality.confirmation_candle,
                "htfc_alignment": quality.htfc_alignment_score,
                "eight_steps": quality.eight_steps_status
            },
            
            "liquidity_analysis": liquidity_analysis
        }
        
        return setup
    except Exception as e:
        log.error(f"Error scanning {symbol}: {e}")
        return None

# ---------------- ALERTS (COMPACT FORMAT) ----------------
async def send_fast_alert(setup: Dict):
    """Send comprehensive alerts with FULL 8-step details (COMPACT VERSION)"""
    
    try:
        symbol = setup.get('symbol', 'UNKNOWN')
        quality = setup.get('quality', {})
        liquidity = setup.get('liquidity_analysis', {})
        eight_steps = quality.get('eight_steps', {})
        step_specifics = eight_steps.get('step_specifics', {})
        
        # Signal key for tracking
        key = signal_tracker.get_signal_key(setup)
        symbol, side, rounded_score = key
        
        # Always NEW SIGNAL due to our tracking logic
        update_emoji = "🆕"
        
        tier_emoji = {
            "A+": "🔥",
            "A": "✅", 
            "B": "⚠️",
            "C": "📊"
        }.get(quality.get("tier", "C"), "📊")
        
        # Format TP targets with SOURCE INFO
        tp_targets = setup.get('tp_targets', [])
        tp_sources = setup.get('tp_sources', [])  # NEW: Get TP sources
        entry_price = setup.get('entry_price', 0)
        
        tp_lines = []
        for i, tp in enumerate(tp_targets):
            if entry_price > 0:
                distance_pct = abs(tp - entry_price) / entry_price * 100
                
                # Get source info for this TP
                source_info = ""
                if i < len(tp_sources):
                    source = tp_sources[i]
                    source_type = source.get('type', 'unknown')
                    timeframe = source.get('timeframe', 'N/A')
                    reason = source.get('reason', 'unknown')
                    
                    # Map source types to emojis
                    source_emoji = {
                        'buy_stop_pool': '🛑',  # Buy stop liquidity
                        'sell_stop_pool': '🛑',  # Sell stop liquidity
                        'equal_high': '🏔️',     # Equal high
                        'equal_low': '🏞️',      # Equal low
                        'major_swing_high': '⛰️', # Major swing high
                        'major_swing_low': '🗻',  # Major swing low
                        'recent_swing': '↕️',    # Recent swing
                        'risk_based': '🎯',      # Risk-based
                        'recent_low': '📉',      # Recent low
                        'recent_high': '📈',     # Recent high
                        'premium_zone': '💰',    # Premium zone
                        'discount_zone': '💸',   # Discount zone
                        'higher_timeframe_pool': '🕒',  # Higher TF pool
                        'next_pool': '➡️',       # Next pool
                        'major_structure': '🏛️', # Major structure
                    }.get(source_type, '📌')
                    
                    source_info = f" {source_emoji}{timeframe}:{reason}"
                
                tp_lines.append(f"TP{i+1}: {tp:.8f} ({distance_pct:.1f}%){source_info}")
        
        # ============ COMPACT 8-STEP ANALYSIS ============
        step_checks = []
        
        # Count passed steps
        pass_count = 0
        step_passes = sum([
            eight_steps.get('step_1_htf_bias', False),
            eight_steps.get('step_2_zone_type', False),
            eight_steps.get('step_3_liquidity_sweep', False),
            eight_steps.get('step_4_structure_shift', False),
            eight_steps.get('step_5_from_liquidity', False),
            eight_steps.get('step_6_confirmation_candle', False),
            eight_steps.get('step_7_entry_validity', False),
            eight_steps.get('step_8_liquidity_alignment', False)
        ])
        
        # Build step checks emojis
        step_checks.append("✅" if eight_steps.get('step_1_htf_bias', False) else "❌")
        step_checks.append("✅" if eight_steps.get('step_2_zone_type', False) else "❌")
        step_checks.append("✅" if eight_steps.get('step_3_liquidity_sweep', False) else "❌")
        step_checks.append("✅" if eight_steps.get('step_4_structure_shift', False) else "❌")
        step_checks.append("✅" if eight_steps.get('step_5_from_liquidity', False) else "❌")
        step_checks.append("✅" if eight_steps.get('step_6_confirmation_candle', False) else "❌")
        step_checks.append("✅" if eight_steps.get('step_7_entry_validity', False) else "❌")
        step_checks.append("✅" if eight_steps.get('step_8_liquidity_alignment', False) else "❌")
        
        # Build compact checklist with single-line steps
        checklist_lines = []
        
        # Step 1: HTF Bias
        step1_spec = step_specifics.get('1', {})
        checklist_lines.append(f"1️⃣ HTF: {step1_spec.get('trend', 'N/A')} ({step1_spec.get('score', 0):.1f})")
        
        # Step 2: Zone Type
        step2_spec = step_specifics.get('2', {})
        checklist_lines.append(f"2️⃣ Zone: {step2_spec.get('entry_type', 'N/A')} ({step2_spec.get('zone_quality', 'N/A')})")
        
        # Step 3: Liquidity Sweep
        step3_spec = step_specifics.get('3', {})
        checklist_lines.append(f"3️⃣ Sweep: {step3_spec.get('sweep_type', 'None')} ({step3_spec.get('strength', 0):.1f})")
        
        # Step 4: Structure Shift
        step4_spec = step_specifics.get('4', {})
        checklist_lines.append(f"4️⃣ Shift: {step4_spec.get('shift_type', 'None')}")
        
        # Step 5: FROM Liquidity
        step5_spec = step_specifics.get('5', {})
        checklist_lines.append(f"5️⃣ Smart $: {'Yes' if step5_spec.get('present', False) else 'No'}")
        
        # Step 6: Confirmation Candle
        step6_spec = step_specifics.get('6', {})
        checklist_lines.append(f"6️⃣ Confirm: {step6_spec.get('candle_type', 'None')}")
        
        # Step 7: Entry Validity
        step7_spec = step_specifics.get('7', {})
        checklist_lines.append(f"7️⃣ Distance: {step7_spec.get('distance_pct', 0):.1f}%")
        
        # Step 8: Liquidity Alignment
        step8_spec = step_specifics.get('8', {})
        checklist_lines.append(f"8️⃣ Pools: {step8_spec.get('pools_analyzed', 0)}")
        
        # Liquidity analysis compact
        liquidity_summary = ""
        if liquidity:
            pools = liquidity.get('identified_pools', {})
            liquidity_summary = f"💧 Pools: B{pools.get('buy_stops', 0)}/S{pools.get('sell_stops', 0)}/H{pools.get('equal_highs', 0)}/L{pools.get('equal_lows', 0)}"
            
            # Show SL source if available
            sl_source = liquidity.get('sl_source', {})
            if sl_source:
                sl_type = sl_source.get('type', 'unknown')
                sl_timeframe = sl_source.get('timeframe', 'N/A')
                sl_reason = sl_source.get('reason', 'unknown')
                
                sl_source_emoji = {
                    'buy_stop_pool': '🛑',
                    'sell_stop_pool': '🛑',
                    'equal_high': '🏔️',
                    'equal_low': '🏞️',
                    'recent_high': '📈',
                    'recent_low': '📉',
                    'fixed_percentage': '⚠️',
                    'adjusted': '⚙️'
                }.get(sl_type, '🛡️')
                
                liquidity_summary += f" | SL: {sl_source_emoji}{sl_timeframe}:{sl_reason}"
        
        # Risk/Reward compact
        risk = setup.get('risk', 0)
        reward = setup.get('reward', 0)
        rr_ratio = setup.get('rr_ratio', 0)
        
        if entry_price > 0:
            risk_pct = risk / entry_price * 100
            reward_pct = reward / entry_price * 100 if reward > 0 else 0
        else:
            risk_pct = 0
            reward_pct = 0
        
        # Signal header compact
        current_price = setup.get('current_price', 0)
        entry_distance_pct = abs(current_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
        
        # Compose compact message
        msg = f"""{update_emoji}{tier_emoji} <b>ROMEOTPT v4.1 - {symbol} | {setup.get('side', 'N/A')} | Score:{rounded_score}</b>
Entry: <code>{entry_price:.8f}</code> | Now: <code>{current_price:.8f}</code> ({entry_distance_pct:.1f}%)
Type: {setup.get('entry_type', 'N/A')}

🎯 <b>Targets:</b>
{chr(10).join(tp_lines)}
🛡️ <b>SL:</b> <code>{setup.get('sl_price', 0):.8f}</code> ({abs(setup.get('sl_price', 0) - entry_price) / entry_price * 100:.1f}%)

⚖️ <b>Risk/Reward:</b> {risk_pct:.1f}% risk | {reward_pct:.1f}% reward | <b>{rr_ratio:.1f}:1</b>
{liquidity_summary}

🔬 <b>8-Step Analysis ({step_passes}/8):</b> {"".join(step_checks)}
{chr(10).join(checklist_lines)}

🏆 <b>Quality:</b> {quality.get('total_score', 0):.1f}/5.0 ({quality.get('tier', 'C')}) | {step_passes}/8 steps

<i>{datetime.datetime.utcnow().strftime('%H:%M:%S UTC')}</i>
"""
        
        await send_telegram(msg)
    except Exception as e:
        log.error(f"Error sending compact alert: {e}")

async def send_outcome_alert(symbol: str, outcome: Dict):
    """Send compact alert when signal hits TP or SL"""
    
    try:
        signal_key = outcome.get('signal_key')
        if not signal_key:
            return
        
        # Get signal data
        signal = signal_tracker.active_signals.get(signal_key, {})
        setup = signal.get('setup', {})
        
        if 'TP' in outcome['type']:
            emoji = "✅" if outcome['tp_level'] == 1 else "🎯" if outcome['tp_level'] == 2 else "🏆"
            result_text = f"TP{outcome['tp_level']} HIT"
            
            # Get TP source info for this TP level
            tp_sources = setup.get('tp_sources', [])
            tp_source_info = ""
            for source in tp_sources:
                if source.get('tp_level') == outcome['tp_level']:
                    source_type = source.get('type', 'unknown')
                    timeframe = source.get('timeframe', 'N/A')
                    reason = source.get('reason', 'unknown')
                    tp_source_info = f" | Source: {timeframe}:{reason}"
                    break
        else:
            emoji = "❌"
            result_text = "SL HIT"
            tp_source_info = ""
        
        bars_held = outcome.get('bars_held', 0)
        if bars_held < 60:
            time_str = f"{bars_held}min"
        else:
            time_str = f"{bars_held//60}h{bars_held%60}m"
        
        # Get quality info
        quality = setup.get('quality', {})
        
        # Count passed steps
        eight_steps = quality.get('eight_steps', {})
        step_passes = sum([
            eight_steps.get('step_1_htf_bias', False),
            eight_steps.get('step_2_zone_type', False),
            eight_steps.get('step_3_liquidity_sweep', False),
            eight_steps.get('step_4_structure_shift', False),
            eight_steps.get('step_5_from_liquidity', False),
            eight_steps.get('step_6_confirmation_candle', False),
            eight_steps.get('step_7_entry_validity', False),
            eight_steps.get('step_8_liquidity_alignment', False)
        ])
        
        msg = f"""{emoji} <b>{result_text} - {symbol}</b>
Signal: {setup.get('side', 'N/A')} | Score: {quality.get('total_score', 0):.1f} | Steps: {step_passes}/8{tp_source_info}

Entry: <code>{setup.get('entry_price', 0):.8f}</code>
Exit: <code>{outcome['price']:.8f}</code>
PnL: <code>{outcome['pnl_pct']:+.2f}%</code> | RR: {setup.get('rr_ratio', 0):.1f}:1

⏱️ {time_str} | Fav: {outcome.get('max_favorable', 0):.1f}% | Adv: {outcome.get('max_adverse', 0):.1f}%

<i>{datetime.datetime.utcnow().strftime('%H:%M')}</i>
"""
        
        await send_telegram(msg)
    except Exception as e:
        log.error(f"Error sending compact outcome alert: {e}")

async def send_deduped_alert(setup: Dict):
    """Send alert ONLY if it's a NEW (symbol, side, rounded_score) combination"""
    try:
        # Check if this is a new signal
        should_alert = signal_tracker.should_send_alert(setup)
        
        if should_alert:
            await send_fast_alert(setup)
            signal_tracker.update_signal(setup, alerted=True)
            
            key = signal_tracker.get_signal_key(setup)
            symbol, side, score = key
            log.info(f"📨 NEW SIGNAL sent: {symbol} {side} Score:{score}")
            return True
        else:
            # Update tracking without alerting
            signal_tracker.update_signal(setup, alerted=False)
            
            # Log occasionally to reduce noise
            if np.random.random() < 0.01:  # 1% chance to log
                key = signal_tracker.get_signal_key(setup)
                if key in signal_tracker.active_signals:
                    signal = signal_tracker.active_signals[key]
                    time_active = (datetime.datetime.utcnow() - signal.get('first_seen', datetime.datetime.utcnow())).total_seconds() / 60
                    symbol, side, score = key
                    log.debug(f"⏸️  Skipping {symbol} {side} Score:{score}: Already has active signal ({time_active:.1f}m)")
            
            return False
    except Exception as e:
        log.error(f"Error in deduped alert: {e}")
        return False

# ---------------- DATABASE MIGRATION & INIT ----------------
async def migrate_database():
    """Migrate old database to new schema if needed"""
    try:
        # Check if old signals table exists without score column
        cursor = await db_conn.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='signals_v4_1'
        """)
        table_exists = await cursor.fetchone()
        
        if not table_exists:
            log.info("✅ No old database found, will create fresh")
            return True
        
        # Check if score column exists in old table
        cursor = await db_conn.execute("PRAGMA table_info(signals_v4_1)")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        if 'score' in column_names:
            log.info("✅ Database already has new schema with score column")
            return True
        
        # OLD SCHEMA DETECTED - need migration
        log.warning("⚠️  Old database schema detected - migrating to new schema")
        
        # Create temporary new table
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signals_v4_1_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                side TEXT,
                score REAL DEFAULT 0.0,
                timestamp TEXT,
                entry_price REAL,
                sl_price REAL,
                tp1 REAL,
                tp2 REAL,
                tp3 REAL,
                rr_ratio REAL,
                quality_tier TEXT,
                quality_score REAL,
                current_price REAL,
                liquidity_buy_stops INTEGER,
                liquidity_sell_stops INTEGER,
                eight_steps_passed INTEGER,
                status TEXT DEFAULT 'active',
                alert_sent BOOLEAN DEFAULT 1,
                closed_at TEXT,
                closed_price REAL,
                outcome TEXT,
                pnl_pct REAL,
                bars_held INTEGER,
                max_favorable_pct REAL,
                max_adverse_pct REAL,
                UNIQUE(symbol, side, score)
            )
        """)
        
        # Copy data from old table, adding default score of 0.0
        await db_conn.execute("""
            INSERT INTO signals_v4_1_new 
            (symbol, side, score, timestamp, entry_price, sl_price, tp1, tp2, tp3, 
             rr_ratio, quality_tier, quality_score, current_price, 
             liquidity_buy_stops, liquidity_sell_stops, eight_steps_passed,
             status, alert_sent, closed_at, closed_price, outcome,
             pnl_pct, bars_held, max_favorable_pct, max_adverse_pct)
            SELECT 
            symbol, side, 0.0, timestamp, entry_price, sl_price, tp1, tp2, tp3,
            rr_ratio, quality_tier, quality_score, current_price,
            liquidity_buy_stops, liquidity_sell_stops, eight_steps_passed,
            status, alert_sent, closed_at, closed_price, outcome,
            pnl_pct, bars_held, max_favorable_pct, max_adverse_pct
            FROM signals_v4_1
        """)
        
        # Do the same for outcomes table
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signal_outcomes_v4_1_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                symbol TEXT,
                side TEXT,
                score REAL DEFAULT 0.0,
                entry_price REAL,
                sl_price REAL,
                tp1_price REAL,
                tp2_price REAL,
                tp3_price REAL,
                quality_score REAL,
                quality_tier TEXT,
                eight_steps_passed INTEGER,
                liquidity_buy_stops INTEGER,
                liquidity_sell_stops INTEGER,
                created_at TEXT,
                status TEXT DEFAULT 'active',
                closed_at TEXT,
                closed_price REAL,
                outcome_type TEXT,
                pnl_pct REAL,
                hold_time_minutes INTEGER,
                max_favorable_pct REAL,
                max_adverse_pct REAL,
                FOREIGN KEY (signal_id) REFERENCES signals_v4_1_new (id)
            )
        """)
        
        # Drop old tables
        await db_conn.execute("DROP TABLE IF EXISTS signal_outcomes_v4_1")
        await db_conn.execute("DROP TABLE IF EXISTS signals_v4_1")
        
        # Rename new tables
        await db_conn.execute("ALTER TABLE signals_v4_1_new RENAME TO signals_v4_1")
        await db_conn.execute("ALTER TABLE signal_outcomes_v4_1_new RENAME TO signal_outcomes_v4_1")
        
        await db_conn.commit()
        log.info("✅ Database migrated successfully to new schema")
        return True
        
    except Exception as e:
        log.error(f"❌ Database migration failed: {e}")
        # If migration fails, drop tables and start fresh
        try:
            await db_conn.execute("DROP TABLE IF EXISTS signals_v4_1")
            await db_conn.execute("DROP TABLE IF EXISTS signal_outcomes_v4_1")
            await db_conn.execute("DROP TABLE IF EXISTS signals_v4_1_new")
            await db_conn.execute("DROP TABLE IF EXISTS signal_outcomes_v4_1_new")
            await db_conn.commit()
            log.info("🔄 Dropped old tables, will create fresh")
            return True
        except Exception as e2:
            log.error(f"❌ Failed to drop old tables: {e2}")
            raise

async def init_database():
    """Initialize database with outcome tracking tables"""
    try:
        # First migrate if needed
        await migrate_database()
        
        # Create tables if they don't exist (they should after migration)
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signals_v4_1 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                side TEXT,
                score REAL,
                timestamp TEXT,
                entry_price REAL,
                sl_price REAL,
                tp1 REAL,
                tp2 REAL,
                tp3 REAL,
                rr_ratio REAL,
                quality_tier TEXT,
                quality_score REAL,
                current_price REAL,
                liquidity_buy_stops INTEGER,
                liquidity_sell_stops INTEGER,
                eight_steps_passed INTEGER,
                status TEXT DEFAULT 'active',
                alert_sent BOOLEAN DEFAULT 1,
                closed_at TEXT,
                closed_price REAL,
                outcome TEXT,
                pnl_pct REAL,
                bars_held INTEGER,
                max_favorable_pct REAL,
                max_adverse_pct REAL,
                UNIQUE(symbol, side, score)
            )
        """)
        
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signal_outcomes_v4_1 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                symbol TEXT,
                side TEXT,
                score REAL,
                entry_price REAL,
                sl_price REAL,
                tp1_price REAL,
                tp2_price REAL,
                tp3_price REAL,
                quality_score REAL,
                quality_tier TEXT,
                eight_steps_passed INTEGER,
                liquidity_buy_stops INTEGER,
                liquidity_sell_stops INTEGER,
                created_at TEXT,
                status TEXT DEFAULT 'active',
                closed_at TEXT,
                closed_price REAL,
                outcome_type TEXT,
                pnl_pct REAL,
                hold_time_minutes INTEGER,
                max_favorable_pct REAL,
                max_adverse_pct REAL,
                FOREIGN KEY (signal_id) REFERENCES signals_v4_1 (id)
            )
        """)
        
        # Create indexes
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_v4_1_signals_symbol_side_score ON signals_v4_1 (symbol, side, score)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_v4_1_signals_status ON signals_v4_1 (status)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_v4_1_signals_outcome ON signals_v4_1 (outcome)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_v4_1_outcomes_symbol_side_score ON signal_outcomes_v4_1 (symbol, side, score)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_v4_1_outcomes_outcome ON signal_outcomes_v4_1 (outcome_type)")
        
        await db_conn.commit()
        log.info("✅ Database v4.1 initialized with (symbol, side, score) tracking")
    except Exception as e:
        log.error(f"❌ Error initializing database: {e}")
        raise

async def store_signal(setup: Dict):
    """Store signal in database"""
    async with db_lock:
        try:
            tp_targets = setup.get("tp_targets", [])
            liquidity = setup.get("liquidity_analysis", {})
            pools = liquidity.get("identified_pools", {})
            quality = setup.get("quality", {})
            eight_steps = quality.get("eight_steps", {})
            
            # Count passed steps
            step_passes = sum([
                eight_steps.get('step_1_htf_bias', False),
                eight_steps.get('step_2_zone_type', False),
                eight_steps.get('step_3_liquidity_sweep', False),
                eight_steps.get('step_4_structure_shift', False),
                eight_steps.get('step_5_from_liquidity', False),
                eight_steps.get('step_6_confirmation_candle', False),
                eight_steps.get('step_7_entry_validity', False),
                eight_steps.get('step_8_liquidity_alignment', False)
            ])
            
            # Get rounded score for database
            key = signal_tracker.get_signal_key(setup)
            _, _, rounded_score = key
            
            # Store in signals table with UNIQUE constraint
            cursor = await db_conn.execute("""
                INSERT OR REPLACE INTO signals_v4_1 (
                    symbol, side, score, timestamp, entry_price, sl_price, 
                    tp1, tp2, tp3, rr_ratio, quality_tier, quality_score,
                    current_price, liquidity_buy_stops, liquidity_sell_stops,
                    eight_steps_passed, status, alert_sent
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', 1)
            """, (
                setup.get("symbol", ""),
                setup.get("side", ""),
                rounded_score,
                setup.get("timestamp", ""),
                setup.get("entry_price", 0),
                setup.get("sl_price", 0),
                tp_targets[0] if len(tp_targets) > 0 else None,
                tp_targets[1] if len(tp_targets) > 1 else None,
                tp_targets[2] if len(tp_targets) > 2 else None,
                setup.get("rr_ratio", 0),
                quality.get("tier", "C"),
                quality.get("total_score", 0),
                setup.get("current_price", 0),
                pools.get("buy_stops", 0),
                pools.get("sell_stops", 0),
                step_passes
            ))
            
            # Get the inserted ID
            signal_id = cursor.lastrowid
            
            # Also store in outcomes table for tracking
            await db_conn.execute("""
                INSERT INTO signal_outcomes_v4_1 (
                    signal_id, symbol, side, score, entry_price, sl_price, tp1_price,
                    tp2_price, tp3_price, quality_score, quality_tier,
                    eight_steps_passed, liquidity_buy_stops, liquidity_sell_stops,
                    created_at, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')
            """, (
                signal_id,
                setup.get("symbol", ""),
                setup.get("side", ""),
                rounded_score,
                setup.get("entry_price", 0),
                setup.get("sl_price", 0),
                tp_targets[0] if len(tp_targets) > 0 else None,
                tp_targets[1] if len(tp_targets) > 1 else None,
                tp_targets[2] if len(tp_targets) > 2 else None,
                quality.get("total_score", 0),
                quality.get("tier", "C"),
                step_passes,
                pools.get("buy_stops", 0),
                pools.get("sell_stops", 0),
                setup.get("timestamp", "")
            ))
            
            await db_conn.commit()
            log.debug(f"📊 Stored signal for {setup.get('symbol', 'UNKNOWN')} {setup.get('side', '')} Score:{rounded_score}")
            
        except Exception as e:
            log.error(f"❌ Error storing signal {setup.get('symbol', 'UNKNOWN')}: {e}")

async def store_outcome(symbol: str, outcome: Dict):
    """Store signal outcome in database"""
    async with db_lock:
        try:
            signal_key = outcome.get('signal_key')
            if not signal_key:
                return
            
            symbol_key, side_key, score_key = signal_key
            now = datetime.datetime.utcnow().isoformat()
            
            # Update signals table
            await db_conn.execute("""
                UPDATE signals_v4_1 
                SET status = 'closed', closed_at = ?, closed_price = ?, outcome = ?,
                    pnl_pct = ?, bars_held = ?, max_favorable_pct = ?, max_adverse_pct = ?
                WHERE symbol = ? AND side = ? AND score = ? AND status = 'active'
            """, (
                now,
                outcome.get('price', 0),
                outcome.get('type', ''),
                outcome.get('pnl_pct', 0),
                outcome.get('bars_held', 0),
                outcome.get('max_favorable', 0),
                outcome.get('max_adverse', 0),
                symbol_key,
                side_key,
                score_key
            ))
            
            # Update signal_outcomes table
            await db_conn.execute("""
                UPDATE signal_outcomes_v4_1 
                SET status = 'closed', closed_at = ?, closed_price = ?, outcome_type = ?,
                    pnl_pct = ?, hold_time_minutes = ?, max_favorable_pct = ?, max_adverse_pct = ?
                WHERE symbol = ? AND side = ? AND score = ? AND status = 'active'
            """, (
                now,
                outcome.get('price', 0),
                outcome.get('type', ''),
                outcome.get('pnl_pct', 0),
                outcome.get('bars_held', 0),
                outcome.get('max_favorable', 0),
                outcome.get('max_adverse', 0),
                symbol_key,
                side_key,
                score_key
            ))
            
            await db_conn.commit()
            log.info(f"📊 Stored outcome for {symbol_key} {side_key} Score:{score_key}: {outcome.get('type', 'UNKNOWN')}")
            
        except Exception as e:
            log.error(f"❌ Error storing outcome for {symbol}: {e}")

# ---------------- OUTCOME CHECKER ----------------
async def outcome_checker_task(exchange):
    """Background task to check signal outcomes - IMPROVED VERSION"""
    log.info("🔄 Outcome checker started - checking ALL active signals")
    
    while True:
        try:
            outcomes_found = 0
            
            # Check each active signal
            for key, signal_data in list(signal_tracker.active_signals.items()):
                if signal_data.get('status') != 'active':
                    continue
                
                symbol = key[0]  # Get symbol from key
                setup = signal_data.get('setup', {})
                
                try:
                    # Get current price
                    ticker = await safe_fetch_ticker(exchange, symbol)
                    if not ticker:
                        continue
                    
                    current_price = ticker.get('last', 0)
                    if current_price == 0:
                        continue
                    
                    # Check outcome FOR THIS SPECIFIC SIGNAL
                    outcome = signal_tracker.check_signal_outcome(setup, current_price)
                    if outcome:
                        await send_outcome_alert(symbol, outcome)
                        await store_outcome(symbol, outcome)
                        outcomes_found += 1
                        
                        symbol_key, side_key, score_key = key
                        log.info(f"📊 Outcome: {symbol_key} {side_key} Score:{score_key} - {outcome.get('type', '')} | PnL: {outcome.get('pnl_pct', 0):+.2f}%")
                        
                except Exception as e:
                    log.debug(f"Error checking outcome for {symbol}: {e}")
                    continue
            
            if outcomes_found:
                log.info(f"📊 Found {outcomes_found} signal outcomes")
            
            # Clean up expired signals
            signal_tracker.cleanup_old_signals()
            
            await asyncio.sleep(30)  # Check every 30 seconds
            
        except Exception as e:
            log.error(f"Outcome checker error: {e}")
            await asyncio.sleep(60)

# ---------------- SCANNER MAIN ----------------
async def process_deduped_results(results) -> int:
    """Process results with deduplication"""
    alerts_sent = 0
    
    for result in results:
        if isinstance(result, Exception):
            log.error(f"Task error: {result}")
            continue
            
        if result:
            try:
                quality_score = result.get("quality", {}).get("total_score", 0)
                if quality_score >= MIN_QUALITY_SCORE:
                    alerted = await send_deduped_alert(result)
                    if alerted:
                        alerts_sent += 1
                    await store_signal(result)
            except Exception as e:
                log.error(f"Error processing result: {e}")
    
    return alerts_sent

async def liquidity_scanner(exchange):
    """Main scanner with liquidity-based TP/SL"""
    
    # Send compact startup message
    startup_msg = f"""🚀 <b>ROMEOTPT v4.1 Started - SIMPLE SIGNAL TRACKING</b>
Scan: {SCAN_INTERVAL}s | Top {TOP_N} | Quality ≥{MIN_QUALITY_SCORE}
Validity: {SIGNAL_VALIDITY_HOURS}h | Rate: {MAX_REQUESTS_PER_SECOND}/s
SIGNAL ID = (Symbol, Side, Rounded_Score)
TP SOURCES NOW SHOWN!"""
    await send_telegram(startup_msg)
    
    # Start outcome checker
    asyncio.create_task(outcome_checker_task(exchange))
    
    scan_cycle = 0
    
    while True:
        scan_cycle += 1
        
        try:
            # Get symbols WITH RATE LIMITING
            tickers = await safe_fetch_tickers(exchange)
            usdt_pairs = []
            
            for symbol, data in tickers.items():
                if symbol.endswith("/USDT") and not symbol.startswith("USDT"):
                    volume = data.get("quoteVolume", 0)
                    if isinstance(volume, (int, float)) and volume > 100000:  # Min $100k volume
                        usdt_pairs.append((symbol, float(volume)))
            
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            symbols_to_scan = [s[0] for s in usdt_pairs[:TOP_N]]
            
            stats = signal_tracker.get_stats()
            
            log.info(f"🔄 Scan #{scan_cycle}: {len(symbols_to_scan)} symbols | Active: {stats.get('active_signals', 0)}")
            
            # Log stats periodically
            if scan_cycle % 5 == 0:
                outcome_stats = stats.get('outcome_stats', {})
                total_closed = outcome_stats.get('tp1_hits', 0) + outcome_stats.get('tp2_hits', 0) + outcome_stats.get('tp3_hits', 0) + outcome_stats.get('sl_hits', 0)
                if total_closed > 0:
                    win_rate = outcome_stats.get('win_rate', 0)
                    avg_pnl = outcome_stats.get('avg_pnl_pct', 0)
                    log.info(f"📈 Stats: WR={win_rate:.1f}% | Avg PnL={avg_pnl:+.2f}% | Active={outcome_stats.get('active', 0)}")
            
            # Scan symbols WITH CONCURRENCY CONTROL
            alerts_this_scan = 0
            tasks = []
            
            # Process in very small batches (conservative)
            batch_size = 1  # One at a time for careful liquidity analysis
            
            for i in range(0, len(symbols_to_scan), batch_size):
                batch = symbols_to_scan[i:i+batch_size]
                
                for symbol in batch:
                    task = asyncio.create_task(scan_symbol_fast(exchange, symbol))
                    tasks.append(task)
                
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    alerts_this_scan += await process_deduped_results(results)
                    tasks = []
                
                # Small delay between batches
                if i + batch_size < len(symbols_to_scan):
                    await asyncio.sleep(0.5)
            
            # Clean up old signals
            signal_tracker.cleanup_old_signals()
            
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"Scanner error: {e}")
            await asyncio.sleep(SCAN_INTERVAL * 2)

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/health")
async def health():
    stats = signal_tracker.get_stats()
    return {
        "status": "healthy", 
        "version": "4.1 - Simple Signal Tracking (Symbol, Side, Score)",
        "active_signals": stats.get('active_signals', 0),
        "signals_by_side": stats.get('signals_by_side', {}),
        "outcome_stats": stats.get('outcome_stats', {})
    }

@app.get("/signals/active")
async def get_active_signals():
    """Get currently active signals"""
    active = []
    for key, data in signal_tracker.active_signals.items():
        if data.get('status') == 'active':
            symbol, side, score = key
            setup = data.get('setup', {})
            quality = setup.get('quality', {})
            eight_steps = quality.get('eight_steps', {})
            
            # Count passed steps
            step_passes = sum([
                eight_steps.get('step_1_htf_bias', False),
                eight_steps.get('step_2_zone_type', False),
                eight_steps.get('step_3_liquidity_sweep', False),
                eight_steps.get('step_4_structure_shift', False),
                eight_steps.get('step_5_from_liquidity', False),
                eight_steps.get('step_6_confirmation_candle', False),
                eight_steps.get('step_7_entry_validity', False),
                eight_steps.get('step_8_liquidity_alignment', False)
            ])
            
            # Get TP sources
            tp_sources = setup.get('tp_sources', [])
            tp_source_info = []
            for source in tp_sources:
                tp_source_info.append({
                    'tp_level': source.get('tp_level', 0),
                    'type': source.get('type', 'unknown'),
                    'timeframe': source.get('timeframe', 'N/A'),
                    'reason': source.get('reason', 'unknown')
                })
            
            active.append({
                "symbol": symbol,
                "side": side,
                "score": score,
                "entry_price": setup.get('entry_price', 0),
                "current_price": setup.get('current_price', 0),
                "sl": setup.get('sl_price', 0),
                "tp1": setup.get('tp_targets', [0])[0] if len(setup.get('tp_targets', [])) > 0 else 0,
                "tp2": setup.get('tp_targets', [0, 0])[1] if len(setup.get('tp_targets', [])) > 1 else 0,
                "tp_sources": tp_source_info,
                "quality_score": quality.get('total_score', 0),
                "quality_tier": quality.get('tier', 'C'),
                "steps_passed": step_passes,
                "rr_ratio": setup.get('rr_ratio', 0),
                "age_minutes": (datetime.datetime.utcnow() - data.get('first_seen', datetime.datetime.utcnow())).total_seconds() / 60
            })
    return {"active_signals": active, "count": len(active)}

@app.get("/outcomes/stats")
async def get_outcome_stats(hours: int = 24):
    """Get outcome statistics"""
    async with db_lock:
        try:
            cursor = await db_conn.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN outcome_type LIKE 'TP%' THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN outcome_type = 'SL_HIT' THEN 1 ELSE 0 END) as losses,
                    AVG(pnl_pct) as avg_pnl,
                    AVG(hold_time_minutes) as avg_hold_time,
                    AVG(liquidity_buy_stops) as avg_buy_stops,
                    AVG(liquidity_sell_stops) as avg_sell_stops,
                    AVG(eight_steps_passed) as avg_steps_passed
                FROM signal_outcomes_v4_1 
                WHERE status = 'closed' 
                AND closed_at > datetime('now', ?)
            """, (f"-{hours} hours",))
            row = await cursor.fetchone()
            
            cursor = await db_conn.execute("""
                SELECT 
                    quality_tier,
                    COUNT(*) as count,
                    SUM(CASE WHEN outcome LIKE 'TP%' THEN 1 ELSE 0 END) as wins,
                    AVG(pnl_pct) as avg_pnl,
                    AVG(rr_ratio) as avg_rr,
                    AVG(eight_steps_passed) as avg_steps_passed
                FROM signals_v4_1 
                WHERE status = 'closed' 
                AND timestamp > datetime('now', ?)
                GROUP BY quality_tier
                ORDER BY quality_tier DESC
            """, (f"-{hours} hours",))
            rows = await cursor.fetchall()
            tier_stats = {}
            for row in rows:
                if row[0]:  # Only add if tier is not None
                    tier_stats[row[0]] = {
                        'count': row[1],
                        'wins': row[2],
                        'avg_pnl': row[3],
                        'avg_rr': row[4],
                        'avg_steps_passed': row[5]
                    }
        except Exception as e:
            log.error(f"Error fetching outcome stats: {e}")
            return {"error": str(e)}
    
    total = row[0] if row else 0
    wins = row[1] if row else 0
    
    return {
        'period_hours': hours,
        'total_signals': total,
        'wins': wins,
        'losses': row[2] if row else 0,
        'win_rate': wins / total * 100 if total > 0 else 0,
        'avg_pnl_pct': row[3] if row else 0,
        'avg_hold_minutes': row[4] if row else 0,
        'avg_liquidity_pools': {
            'buy_stops': row[5] if row else 0,
            'sell_stops': row[6] if row else 0
        },
        'avg_steps_passed': row[7] if row else 0,
        'by_tier': tier_stats,
        'memory_stats': signal_tracker.outcome_stats
    }

# ---------------- MAIN ----------------
async def main():
    global db_conn
    
    try:
        # Initialize database
        db_conn = await aiosqlite.connect(DB_PATH)
        await init_database()
        
        # Create exchange with conservative settings
        exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
            "rateLimit": 300,
            "timeout": 15000,
            "verbose": False,
        })
        
        log.info("🚀 ROMEOTPT v4.1 - SIMPLE SIGNAL TRACKING (Symbol, Side, Score)")
        log.info(f"Signal ID = (Symbol, Side, Rounded_Score)")
        log.info(f"TP/SL: 100% liquidity-based | NO fixed percentages")
        log.info(f"TP SOURCES NOW SHOWN in alerts!")
        log.info(f"Scan: {SCAN_INTERVAL}s | Top {TOP_N} symbols")
        log.info(f"Score changes = NEW SIGNAL | Price changes = SAME SIGNAL")
        
        await liquidity_scanner(exchange)
        
    except Exception as e:
        log.error(f"Fatal error: {e}")
    finally:
        if db_conn:
            await db_conn.close()
        log.info("Scanner shutdown complete")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", help="Run HTTP server")
    args = parser.parse_args()
    
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Scanner stopped by user")