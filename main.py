#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT SCANNER v3.2 - COMPLETE & CORRECTED VERSION
Two-layer architecture + Deduplication + Outcome Tracking + RATE LIMITING FIX
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
DB_PATH = os.getenv("DB_PATH", "/app/data/romeopt_v3_2.db")

# Scanner settings
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 180))  # Increased from 120 to 180
TOP_N = int(os.getenv("TOP_N", 4))  # Reduced from 30 to 15
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 3))  # Reduced from 10 to 3

# Signal thresholds
MIN_QUALITY_SCORE = float(os.getenv("MIN_QUALITY_SCORE", 0.0))

# Deduplication settings
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", 15))
SIGNAL_VALIDITY_HOURS = int(os.getenv("SIGNAL_VALIDITY_HOURS", 24))
PRICE_MOVEMENT_THRESHOLD = float(os.getenv("PRICE_MOVEMENT_THRESHOLD", 0.5))

# Outcome tracking
OUTCOME_CHECK_INTERVAL = int(os.getenv("OUTCOME_CHECK_INTERVAL", 60))
MINIMUM_TRADE_HOLD_SECONDS = int(os.getenv("MINIMUM_TRADE_HOLD_SECONDS", 30))

# Rate limiting settings
MAX_REQUESTS_PER_SECOND = int(os.getenv("MAX_REQUESTS_PER_SECOND", 15))
RATE_LIMIT_RETRIES = int(os.getenv("RATE_LIMIT_RETRIES", 3))
RATE_LIMIT_BACKOFF_FACTOR = float(os.getenv("RATE_LIMIT_BACKOFF_FACTOR", 1.5))

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("romeopt_v3_2")

# ---------------- RATE LIMITER ----------------
class RateLimiter:
    """Rate limiter with exponential backoff for OKX API"""
    
    def __init__(self):
        self.max_rps = MAX_REQUESTS_PER_SECOND
        self.max_concurrent = MAX_CONCURRENT
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.request_times = []
        self.min_delay = 0.1  # Minimum delay between requests
        self.backoff_factor = RATE_LIMIT_BACKOFF_FACTOR
        self.max_retries = RATE_LIMIT_RETRIES
        self.last_error_time = 0
        
    async def wait_if_needed(self):
        """Wait if we're hitting rate limits"""
        now = time.time()
        
        # Clean old request times
        self.request_times = [t for t in self.request_times if now - t < 1.0]
        
        # Check if we're at the limit
        if len(self.request_times) >= self.max_rps:
            wait_time = 1.0 - (now - self.request_times[0])
            if wait_time > 0:
                await asyncio.sleep(wait_time)
        
        # Add this request
        self.request_times.append(now)
    
    async def execute_with_backoff(self, func, *args, **kwargs):
        """Execute function with exponential backoff on rate limits"""
        async with self.semaphore:
            for attempt in range(self.max_retries):
                try:
                    await self.wait_if_needed()
                    return await func(*args, **kwargs)
                except Exception as e:
                    # Check if it's a rate limit error
                    error_str = str(e)
                    if "Too Many Requests" in error_str or "50011" in error_str or "429" in error_str:
                        wait_time = self.min_delay * (self.backoff_factor ** attempt)
                        log.warning(f"Rate limited, attempt {attempt+1}/{self.max_retries}, waiting {wait_time:.2f}s")
                        await asyncio.sleep(wait_time)
                        
                        # Update last error time for global cooldown
                        self.last_error_time = time.time()
                    else:
                        raise e
            # All retries failed
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
    entry_zone: Dict = None
    sl_price: float = 0.0
    tp_targets: List[float] = None
    disqualify_reason: str = ""
    
@dataclass
class SetupQuality:
    """LAYER 2: Quality metrics with 8-step tracking"""
    sweep_strength: float = 0.0
    structure_shift: bool = False
    from_liquidity_exists: bool = False
    confirmation_candle: bool = False
    htfc_alignment_score: float = 0.0
    total_score: float = 0.0
    
    # 8-step tracking
    eight_steps_status: Dict = None
    
    @property
    def quality_tier(self) -> str:
        if self.total_score >= 4.0:
            return "A+"
        elif self.total_score >= 3.0:
            return "A"
        elif self.total_score >= 2.0:
            return "B"
        else:
            return "C"

# ---------------- SIGNAL TRACKER ----------------
class SignalTracker:
    """In-memory signal tracking with deduplication and outcome monitoring"""
    
    def __init__(self):
        self.active_signals = {}
        self.signal_history = []
        self.outcome_stats = {
            'total_signals': 0,
            'tp1_hits': 0,
            'tp2_hits': 0,
            'sl_hits': 0,
            'expired': 0,
            'active': 0,
            'win_rate': 0.0
        }
    
    def is_new_or_updated_signal(self, symbol: str, new_setup: Dict) -> Tuple[bool, str]:
        """Check if this is a NEW signal or UPDATED existing signal"""
        now = datetime.datetime.utcnow()
        
        if symbol not in self.active_signals:
            return True, "New signal"
        
        old_signal = self.active_signals[symbol]
        old_setup = old_signal.get('setup', {})
        
        if not old_setup:
            return True, "Old signal corrupted"
        
        # Has the signal expired?
        if (now - old_signal['first_seen']).total_seconds() > (SIGNAL_VALIDITY_HOURS * 3600):
            self.remove_signal(symbol)
            return True, f"Old signal expired ({SIGNAL_VALIDITY_HOURS}h)"
        
        # Check if it's the same side
        if old_setup.get('side', '') != new_setup.get('side', ''):
            return True, "Side changed"
        
        # Check if price moved significantly
        old_entry = old_setup.get('entry_price', 0)
        new_entry = new_setup.get('entry_price', 0)
        if old_entry == 0:
            return True, "Old entry price invalid"
            
        price_change_pct = abs(new_entry - old_entry) / old_entry * 100
        if price_change_pct > PRICE_MOVEMENT_THRESHOLD:
            return True, f"Price moved {price_change_pct:.2f}%"
        
        # Check if still in cooldown period
        if not old_signal.get('last_alerted'):
            return True, "No previous alert time"
            
        time_since_last_alert = (now - old_signal['last_alerted']).total_seconds() / 60
        if time_since_last_alert < SIGNAL_COOLDOWN_MINUTES:
            return False, f"In cooldown ({int(SIGNAL_COOLDOWN_MINUTES - time_since_last_alert)}min left)"
        
        # Check if quality improved significantly
        old_quality = old_setup.get('quality', {}).get('total_score', 0)
        new_quality = new_setup.get('quality', {}).get('total_score', 0)
        if new_quality - old_quality >= 0.5:
            return True, f"Quality improved {old_quality:.2f}→{new_quality:.2f}"
        
        # Check if entry type changed meaningfully
        old_entry_type = old_setup.get('entry_type', '')
        new_entry_type = new_setup.get('entry_type', '')
        if (old_entry_type in ["DISCOUNT_ZONE", "BULLISH_ENGULFING"] and 
            new_entry_type in ["PREMIUM_ZONE", "BEARISH_ENGULFING"]):
            return True, "Entry type changed significantly"
        
        # Check if RR improved significantly
        old_rr = old_setup.get('rr_ratio', 0)
        new_rr = new_setup.get('rr_ratio', 0)
        if new_rr > old_rr * 1.2:
            return True, f"RR improved {old_rr:.2f}→{new_rr:.2f}"
        
        return False, "Same signal, minimal changes"
    
    def update_signal(self, symbol: str, setup: Dict, alerted: bool = False):
        """Update or add signal to tracker"""
        now = datetime.datetime.utcnow()
        
        if symbol not in self.active_signals:
            self.active_signals[symbol] = {
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
                'outcome_details': None
            }
            self.outcome_stats['total_signals'] += 1
            self.outcome_stats['active'] += 1
        else:
            # Update price extremes
            current_price = setup.get('current_price', 0)
            self.active_signals[symbol]['highest_price'] = max(
                self.active_signals[symbol]['highest_price'],
                current_price
            )
            self.active_signals[symbol]['lowest_price'] = min(
                self.active_signals[symbol]['lowest_price'],
                current_price
            )
            
            self.active_signals[symbol]['setup'] = setup
            if alerted:
                self.active_signals[symbol]['last_alerted'] = now
                self.active_signals[symbol]['alert_count'] += 1
                if not self.active_signals[symbol]['price_at_alert']:
                    self.active_signals[symbol]['price_at_alert'] = current_price
    
    def check_signal_outcome(self, symbol: str, current_price: float) -> Optional[Dict]:
        """Check if signal has hit TP or SL"""
        if symbol not in self.active_signals:
            return None
        
        signal = self.active_signals[symbol]
        setup = signal.get('setup', {})
        
        if not setup:
            return None
        
        # Don't check too soon
        now = datetime.datetime.utcnow()
        time_since_alert = (now - signal['first_seen']).total_seconds()
        if time_since_alert < MINIMUM_TRADE_HOLD_SECONDS:
            return None
        
        side = setup.get('side', '')
        entry = setup.get('entry_price', 0)
        tp_targets = setup.get('tp_targets', [])
        tp1 = tp_targets[0] if len(tp_targets) > 0 else 0
        tp2 = tp_targets[1] if len(tp_targets) > 1 else None
        sl = setup.get('sl_price', 0)
        
        if entry == 0 or tp1 == 0 or sl == 0:
            return None
        
        outcome = None
        
        # Check TP1 hit
        if side == "BUY" and current_price >= tp1:
            pnl_pct = (current_price - entry) / entry * 100
            outcome = {
                'type': 'TP1_HIT',
                'price': current_price,
                'pnl_pct': pnl_pct,
                'bars_held': int(time_since_alert / 60),
                'max_favorable': (signal['highest_price'] - entry) / entry * 100,
                'max_adverse': (entry - signal['lowest_price']) / entry * 100
            }
        elif side == "SELL" and current_price <= tp1:
            pnl_pct = (entry - current_price) / entry * 100
            outcome = {
                'type': 'TP1_HIT',
                'price': current_price,
                'pnl_pct': pnl_pct,
                'bars_held': int(time_since_alert / 60),
                'max_favorable': (entry - signal['lowest_price']) / entry * 100,
                'max_adverse': (signal['highest_price'] - entry) / entry * 100
            }
        
        # Check TP2 hit
        elif tp2 and ((side == "BUY" and current_price >= tp2) or (side == "SELL" and current_price <= tp2)):
            if side == "BUY":
                pnl_pct = (current_price - entry) / entry * 100
                max_fav = (signal['highest_price'] - entry) / entry * 100
            else:
                pnl_pct = (entry - current_price) / entry * 100
                max_fav = (entry - signal['lowest_price']) / entry * 100
            
            outcome = {
                'type': 'TP2_HIT',
                'price': current_price,
                'pnl_pct': pnl_pct,
                'bars_held': int(time_since_alert / 60),
                'max_favorable': max_fav,
                'max_adverse': abs(entry - (signal['lowest_price'] if side == "BUY" else signal['highest_price'])) / entry * 100
            }
        
        # Check SL hit
        elif (side == "BUY" and current_price <= sl) or (side == "SELL" and current_price >= sl):
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
                'max_adverse': abs(entry - sl) / entry * 100
            }
        
        if outcome:
            signal['outcome'] = outcome['type'].lower()
            signal['outcome_details'] = outcome
            signal['closed_at'] = now
            signal['closed_price'] = current_price
            signal['status'] = 'closed'
            
            # Update stats
            self.outcome_stats['active'] -= 1
            if outcome['type'] == 'TP1_HIT':
                self.outcome_stats['tp1_hits'] += 1
            elif outcome['type'] == 'TP2_HIT':
                self.outcome_stats['tp2_hits'] += 1
            elif outcome['type'] == 'SL_HIT':
                self.outcome_stats['sl_hits'] += 1
            
            wins = self.outcome_stats['tp1_hits'] + self.outcome_stats['tp2_hits']
            losses = self.outcome_stats['sl_hits']
            total_closed = wins + losses
            if total_closed > 0:
                self.outcome_stats['win_rate'] = wins / total_closed * 100
            
            return outcome
        
        return None
    
    def remove_signal(self, symbol: str, reason: str = "expired"):
        """Remove signal and mark as expired"""
        if symbol in self.active_signals:
            signal = self.active_signals.pop(symbol)
            signal['status'] = 'expired'
            signal['expired_at'] = datetime.datetime.utcnow()
            signal['expired_reason'] = reason
            
            self.outcome_stats['active'] -= 1
            self.outcome_stats['expired'] += 1
    
    def cleanup_old_signals(self):
        """Remove expired signals"""
        now = datetime.datetime.utcnow()
        expired_symbols = []
        
        for symbol, data in self.active_signals.items():
            age_minutes = (now - data['first_seen']).total_seconds() / 60
            if age_minutes > (SIGNAL_VALIDITY_HOURS * 60):
                expired_symbols.append(symbol)
        
        for symbol in expired_symbols:
            self.remove_signal(symbol, f"Expired after {SIGNAL_VALIDITY_HOURS}h")
        
        if expired_symbols:
            log.debug(f"Cleaned up {len(expired_symbols)} expired signals")
    
    def get_stats(self) -> Dict:
        """Get tracking statistics"""
        active_count = len(self.active_signals)
        
        buy_signals = 0
        sell_signals = 0
        
        for signal in self.active_signals.values():
            setup = signal.get('setup', {})
            if setup.get('side') == 'BUY':
                buy_signals += 1
            elif setup.get('side') == 'SELL':
                sell_signals += 1
        
        return {
            'active_signals': active_count,
            'total_history': len(self.signal_history),
            'signals_by_side': {
                'BUY': buy_signals,
                'SELL': sell_signals
            }
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
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": parse_mode
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
            timeout=5.0  # Increased from 3.0 to 5.0
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
    
    # Quick HTF direction (1H)
    ohlcv_1h = await fetch_ohlcv(exchange, symbol, "1h", 50)
    if not ohlcv_1h or len(ohlcv_1h) < 20:
        return SetupEligibility(eligible=False, disqualify_reason="Insufficient data")
    
    df_1h = create_dataframe(ohlcv_1h)
    if df_1h is None:
        return SetupEligibility(eligible=False, disqualify_reason="Dataframe error")
    
    # Fast trend detection
    try:
        df_1h['ema_20'] = df_1h['close'].ewm(span=20).mean()
        df_1h['ema_50'] = df_1h['close'].ewm(span=50).mean()
        
        latest_ema20 = df_1h['ema_20'].iloc[-1]
        latest_ema50 = df_1h['ema_50'].iloc[-1]
        latest_close = df_1h['close'].iloc[-1]
        
        # Determine bias
        if latest_ema20 > latest_ema50 and latest_close > latest_ema20:
            bias = "BULLISH"
            side = "BUY"
        elif latest_ema20 < latest_ema50 and latest_close < latest_ema20:
            bias = "BEARISH"
            side = "SELL"
        else:
            recent_high = df_1h['high'].iloc[-10:].max()
            recent_low = df_1h['low'].iloc[-10:].min()
            
            if current_price > (recent_high + recent_low) / 2:
                bias = "BULLISH"
                side = "BUY"
            else:
                bias = "BEARISH"
                side = "SELL"
    except Exception as e:
        log.debug(f"Trend detection error for {symbol}: {e}")
        return SetupEligibility(eligible=False, disqualify_reason="Trend detection error")
    
    # Get quick range
    try:
        range_high = float(df_1h['high'].iloc[-20:].max())
        range_low = float(df_1h['low'].iloc[-20:].min())
    except:
        range_high = float(df_1h['high'].max())
        range_low = float(df_1h['low'].min())
    
    # Find entry zone (15m)
    ohlcv_15m = await fetch_ohlcv(exchange, symbol, "15m", 30)
    if not ohlcv_15m:
        return SetupEligibility(eligible=False, disqualify_reason="No 15m data")
    
    df_15m = create_dataframe(ohlcv_15m)
    if df_15m is None:
        return SetupEligibility(eligible=False, disqualify_reason="15m dataframe error")
    
    # Find recent OB/FVG (fast detection)
    entry_found = False
    entry_price = 0
    entry_type = ""
    entry_low = 0
    entry_high = 0
    
    try:
        if side == "BUY":
            recent_low_15m = df_15m['low'].iloc[-5:].min()
            
            if current_price <= recent_low_15m * 1.005:
                entry_price = current_price
                entry_type = "DISCOUNT_ZONE"
                entry_low = recent_low_15m * 0.995
                entry_high = recent_low_15m * 1.01
                entry_found = True
            
            if not entry_found and len(df_15m) >= 3:
                last_candle = df_15m.iloc[-1]
                prev_candle = df_15m.iloc[-2]
                
                if (prev_candle['close'] < prev_candle['open'] and 
                    last_candle['close'] > last_candle['open'] and
                    last_candle['close'] > prev_candle['close']):
                    entry_price = last_candle['close']
                    entry_type = "BULLISH_ENGULFING"
                    entry_low = last_candle['low']
                    entry_high = last_candle['high'] * 1.005
                    entry_found = True
                    
        else:  # SELL
            recent_high_15m = df_15m['high'].iloc[-5:].max()
            
            if current_price >= recent_high_15m * 0.995:
                entry_price = current_price
                entry_type = "PREMIUM_ZONE"
                entry_low = recent_high_15m * 0.99
                entry_high = recent_high_15m * 1.005
                entry_found = True
            
            if not entry_found and len(df_15m) >= 3:
                last_candle = df_15m.iloc[-1]
                prev_candle = df_15m.iloc[-2]
                
                if (prev_candle['close'] > prev_candle['open'] and 
                    last_candle['close'] < last_candle['open'] and
                    last_candle['close'] < prev_candle['close']):
                    entry_price = last_candle['close']
                    entry_type = "BEARISH_ENGULFING"
                    entry_low = last_candle['low'] * 0.995
                    entry_high = last_candle['high']
                    entry_found = True
    except Exception as e:
        log.debug(f"Entry zone detection error for {symbol}: {e}")
        return SetupEligibility(eligible=False, disqualify_reason="Entry zone error")
    
    if not entry_found:
        return SetupEligibility(eligible=False, disqualify_reason="No entry zone")
    
    # SL logic
    try:
        if side == "BUY":
            sl_price = min(recent_low_15m * 0.995, entry_price * 0.99)
        else:
            sl_price = max(recent_high_15m * 1.005, entry_price * 1.01)
    except:
        # Fallback SL
        if side == "BUY":
            sl_price = entry_price * 0.99
        else:
            sl_price = entry_price * 1.01
    
    # TP targets
    tp_targets = []
    
    try:
        if side == "BUY":
            recent_resistance = df_1h['high'].iloc[-10:].max()
            tp_targets.append(float(recent_resistance))
            
            range_height = range_high - range_low
            tp_targets.append(float(min(range_high + range_height * 0.5, entry_price * 1.03)))
        else:
            recent_support = df_1h['low'].iloc[-10:].min()
            tp_targets.append(float(recent_support))
            
            range_height = range_high - range_low
            tp_targets.append(float(max(range_low - range_height * 0.5, entry_price * 0.97)))
    except:
        # Fallback TP
        if side == "BUY":
            tp_targets.append(entry_price * 1.02)
            tp_targets.append(entry_price * 1.04)
        else:
            tp_targets.append(entry_price * 0.98)
            tp_targets.append(entry_price * 0.96)
    
    entry_zone = {
        "type": entry_type,
        "price": entry_price,
        "low": entry_low,
        "high": entry_high,
        "current_in_zone": entry_low <= current_price <= entry_high
    }
    
    return SetupEligibility(
        eligible=True,
        side=side,
        entry_price=entry_price,
        entry_type=entry_type,
        entry_zone=entry_zone,
        sl_price=sl_price,
        tp_targets=tp_targets
    )

# ---------------- LAYER 2: QUALITY ANALYSIS ----------------
async def analyze_quality(exchange, symbol: str, eligibility: SetupEligibility) -> SetupQuality:
    """LAYER 2: QUALITY ANALYSIS WITH 8-STEP TRACKING"""
    
    side = eligibility.side
    entry_type = eligibility.entry_type
    entry_price = eligibility.entry_price
    current_price = entry_price  # Will be updated
    
    # Initialize scores
    sweep_strength = 0.0
    structure_shift = False
    from_liquidity_exists = False
    confirmation_candle = False
    htfc_alignment_score = 0.0
    
    # Initialize 8-step tracking
    eight_steps = {
        'step_1_htf_bias': False,
        'step_2_zone_type': False,
        'step_3_liquidity_sweep': False,
        'step_4_structure_shift': False,
        'step_5_from_liquidity': False,
        'step_6_confirmation_candle': False,
        'step_7_entry_zone': False,
        'step_8_rr_ratio': False,
        
        # Details for display
        'step_details': {
            '1': 'HTF bias aligned with direction',
            '2': 'Premium/Discount zone entry',
            '3': 'Liquidity sweep detected',
            '4': 'Market structure shift',
            '5': 'FROM liquidity present',
            '6': 'Confirmation candle formed',
            '7': 'Price in valid entry zone',
            '8': 'Risk/Reward ≥ 1.5:1'
        }
    }
    
    try:
        # Get current price for accurate checks
        ticker = await safe_fetch_ticker(exchange, symbol)
        current_price = ticker.get("last", entry_price)
        
        # === STEP 1: HTF Bias Alignment ===
        ohlcv_1h = await fetch_ohlcv(exchange, symbol, "1h", 30)
        if ohlcv_1h:
            df_1h = create_dataframe(ohlcv_1h)
            if df_1h is not None and len(df_1h) >= 20:
                df_1h['ema_20'] = df_1h['close'].ewm(span=20).mean()
                df_1h['ema_50'] = df_1h['close'].ewm(span=50).mean()
                
                if side == "BUY":
                    htfc_alignment_score = 1.0 if df_1h['ema_20'].iloc[-1] > df_1h['ema_50'].iloc[-1] else 0.5
                    eight_steps['step_1_htf_bias'] = htfc_alignment_score >= 0.7
                else:
                    htfc_alignment_score = 1.0 if df_1h['ema_20'].iloc[-1] < df_1h['ema_50'].iloc[-1] else 0.5
                    eight_steps['step_1_htf_bias'] = htfc_alignment_score >= 0.7
        
        # === STEP 2: Premium/Discount Zone ===
        if side == "BUY" and entry_type in ["DISCOUNT_ZONE", "BULLISH_ENGULFING"]:
            eight_steps['step_2_zone_type'] = True
        elif side == "SELL" and entry_type in ["PREMIUM_ZONE", "BEARISH_ENGULFING"]:
            eight_steps['step_2_zone_type'] = True
        
        # === STEP 3: Liquidity Sweep ===
        ohlcv_15m = await fetch_ohlcv(exchange, symbol, "15m", 20)
        if ohlcv_15m:
            df_15m = create_dataframe(ohlcv_15m)
            if df_15m is not None and len(df_15m) >= 10:
                if side == "BUY":
                    recent_low = df_15m['low'].iloc[-5:].min()
                    prev_low = df_15m['low'].iloc[-10:-5].min()
                    if recent_low < prev_low:
                        sweep_strength = 0.7
                        eight_steps['step_3_liquidity_sweep'] = True
                        try:
                            sweep_idx = df_15m['low'].idxmin()
                            if sweep_idx < len(df_15m) - 1:
                                sweep_candle = df_15m.iloc[sweep_idx]
                                body_size = abs(sweep_candle['close'] - sweep_candle['open'])
                                wick_size = sweep_candle['high'] - max(sweep_candle['open'], sweep_candle['close'])
                                if body_size > wick_size:
                                    sweep_strength = 1.0
                        except:
                            pass
                else:
                    recent_high = df_15m['high'].iloc[-5:].max()
                    prev_high = df_15m['high'].iloc[-10:-5].max()
                    if recent_high > prev_high:
                        sweep_strength = 0.7
                        eight_steps['step_3_liquidity_sweep'] = True
                        try:
                            sweep_idx = df_15m['high'].idxmax()
                            if sweep_idx < len(df_15m) - 1:
                                sweep_candle = df_15m.iloc[sweep_idx]
                                body_size = abs(sweep_candle['close'] - sweep_candle['open'])
                                wick_size = min(sweep_candle['open'], sweep_candle['close']) - sweep_candle['low']
                                if body_size > wick_size:
                                    sweep_strength = 1.0
                        except:
                            pass
        
        # === STEP 4: Structure Shift ===
        if ohlcv_1h:
            df_1h = create_dataframe(ohlcv_1h)
            if df_1h is not None and len(df_1h) >= 11:
                if side == "BUY":
                    recent_high = df_1h['high'].iloc[-10:-1].max()
                    current_close = df_1h['close'].iloc[-1]
                    if current_close > recent_high:
                        structure_shift = True
                        eight_steps['step_4_structure_shift'] = True
                else:
                    recent_low = df_1h['low'].iloc[-10:-1].min()
                    current_close = df_1h['close'].iloc[-1]
                    if current_close < recent_low:
                        structure_shift = True
                        eight_steps['step_4_structure_shift'] = True
        
        # === STEP 5: FROM Liquidity ===
        if sweep_strength > 0.5:
            from_liquidity_exists = True
            eight_steps['step_5_from_liquidity'] = True
        
        # === STEP 6: Confirmation Candle ===
        ohlcv_5m = await fetch_ohlcv(exchange, symbol, "5m", 5)
        if ohlcv_5m:
            df_5m = create_dataframe(ohlcv_5m)
            if df_5m is not None and len(df_5m) > 0:
                if side == "BUY":
                    if df_5m['close'].iloc[-1] > df_5m['open'].iloc[-1]:
                        confirmation_candle = True
                        eight_steps['step_6_confirmation_candle'] = True
                else:
                    if df_5m['close'].iloc[-1] < df_5m['open'].iloc[-1]:
                        confirmation_candle = True
                        eight_steps['step_6_confirmation_candle'] = True
        
        # === STEP 7: Entry Zone ===
        if entry_price > 0:
            entry_zone_threshold = 0.02  # 2%
            price_diff_pct = abs(current_price - entry_price) / entry_price * 100
            eight_steps['step_7_entry_zone'] = price_diff_pct <= entry_zone_threshold
        
        # === STEP 8: Risk/Reward Ratio ===
        risk = abs(eligibility.entry_price - eligibility.sl_price)
        reward = abs(eligibility.tp_targets[0] - eligibility.entry_price)
        rr_ratio = reward / risk if risk > 0 else 0
        eight_steps['step_8_rr_ratio'] = rr_ratio >= 1.5
        
        # Store RR ratio for later use
        eight_steps['rr_ratio'] = rr_ratio
        
    except Exception as e:
        log.debug(f"Quality analysis error for {symbol}: {e}")
    
    # Calculate total score (0-5)
    total_score = (
        sweep_strength +
        (1.0 if structure_shift else 0.0) +
        (0.5 if from_liquidity_exists else 0.0) +
        (0.5 if confirmation_candle else 0.0) +
        htfc_alignment_score
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
    """ULTRA-FAST scanning: Layer 1 only, Layer 2 optional"""
    
    try:
        # LAYER 1: Eligibility check
        eligibility = await check_eligibility_fast(exchange, symbol)
        
        if not eligibility.eligible:
            return None
        
        # LAYER 2: Quality analysis
        quality = await analyze_quality(exchange, symbol, eligibility)
        
        # Get current price
        ticker = await safe_fetch_ticker(exchange, symbol)
        current_price = ticker.get("last", 0)
        
        # Calculate RR
        risk = abs(eligibility.entry_price - eligibility.sl_price)
        reward = abs(eligibility.tp_targets[0] - eligibility.entry_price)
        rr_ratio = reward / risk if risk > 0 else 0
        
        setup = {
            "symbol": symbol,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "side": eligibility.side,
            "current_price": current_price,
            "entry_price": eligibility.entry_price,
            "entry_type": eligibility.entry_type,
            "sl_price": eligibility.sl_price,
            "tp_targets": eligibility.tp_targets,
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
            }
        }
        
        return setup
    except Exception as e:
        log.error(f"Error scanning {symbol}: {e}")
        return None

# ---------------- ALERTS ----------------
async def send_fast_alert(setup: Dict):
    """Send concise, fast alerts with 8-step numerical display"""
    
    try:
        symbol = setup.get('symbol', 'UNKNOWN')
        quality = setup.get('quality', {})
        eight_steps = quality.get('eight_steps', {})
        step_details = eight_steps.get('step_details', {})
        
        # Check if this is an update
        is_update = symbol in signal_tracker.active_signals
        update_emoji = "🔄" if is_update else "🆕"
        
        tier_emoji = {
            "A+": "🔥",
            "A": "✅", 
            "B": "⚠️",
            "C": "📊"
        }.get(quality.get("tier", "C"), "📊")
        
        # Add update info if applicable
        update_info = ""
        if is_update:
            old_signal = signal_tracker.active_signals.get(symbol, {})
            old_setup = old_signal.get('setup', {})
            old_quality = old_setup.get('quality', {}).get('total_score', 0)
            new_quality = quality.get('total_score', 0)
            if new_quality > old_quality:
                update_info = f"\n📈 <b>Quality UP:</b> {old_quality:.2f} → {new_quality:.2f}"
            elif old_quality > 0:
                update_info = f"\n🔄 <b>Updated signal</b>"
        
        tp_targets = setup.get('tp_targets', [])
        # Format TP values separately to avoid f-string formatting errors
        tp1_display = f"{tp_targets[0]:.8f}" if len(tp_targets) > 0 else 'N/A'
        tp2_display = f"{tp_targets[1]:.8f}" if len(tp_targets) > 1 else 'N/A'
        
        # ============ 8-STEP NUMERICAL DISPLAY ============
        # Build the 8-step checklist with pass/fail status
        checklist_lines = []
        
        # Step 1: HTF Bias Alignment
        step1_status = "✅ PASS" if eight_steps.get('step_1_htf_bias', False) else "❌ FAIL"
        checklist_lines.append(f"1. {step1_status} - HTF bias aligned with direction")
        
        # Step 2: Premium/Discount Zone
        step2_status = "✅ PASS" if eight_steps.get('step_2_zone_type', False) else "❌ FAIL"
        checklist_lines.append(f"2. {step2_status} - Premium/Discount zone entry")
        
        # Step 3: Liquidity Sweep
        step3_status = "✅ PASS" if eight_steps.get('step_3_liquidity_sweep', False) else "❌ FAIL"
        sweep_score = quality.get('sweep_strength', 0)
        checklist_lines.append(f"3. {step3_status} - Liquidity sweep detected (Score: {sweep_score:.2f})")
        
        # Step 4: Structure Shift
        step4_status = "✅ PASS" if eight_steps.get('step_4_structure_shift', False) else "❌ FAIL"
        checklist_lines.append(f"4. {step4_status} - Market structure shift")
        
        # Step 5: FROM Liquidity
        step5_status = "✅ PASS" if eight_steps.get('step_5_from_liquidity', False) else "❌ FAIL"
        checklist_lines.append(f"5. {step5_status} - FROM liquidity present")
        
        # Step 6: Confirmation Candle
        step6_status = "✅ PASS" if eight_steps.get('step_6_confirmation_candle', False) else "❌ FAIL"
        checklist_lines.append(f"6. {step6_status} - Confirmation candle formed")
        
        # Step 7: Entry Zone
        step7_status = "✅ PASS" if eight_steps.get('step_7_entry_zone', False) else "❌ FAIL"
        entry_price = setup.get('entry_price', 0)
        current_price = setup.get('current_price', 0)
        if entry_price > 0:
            zone_diff = abs(current_price - entry_price) / entry_price * 100
            checklist_lines.append(f"7. {step7_status} - Price in valid entry zone ({zone_diff:.2f}% from entry)")
        
        # Step 8: Risk/Reward Ratio
        rr_ratio = setup.get('rr_ratio', 0)
        step8_passed = rr_ratio >= 1.5
        step8_status = "✅ PASS" if step8_passed else "⚠️ MARGINAL" if rr_ratio >= 1.0 else "❌ FAIL"
        checklist_lines.append(f"8. {step8_status} - Risk/Reward ≥ 1.5:1 (Current: {rr_ratio:.2f}:1)")
        
        # Count passes
        pass_count = sum([
            eight_steps.get('step_1_htf_bias', False),
            eight_steps.get('step_2_zone_type', False),
            eight_steps.get('step_3_liquidity_sweep', False),
            eight_steps.get('step_4_structure_shift', False),
            eight_steps.get('step_5_from_liquidity', False),
            eight_steps.get('step_6_confirmation_candle', False),
            eight_steps.get('step_7_entry_zone', False),
            step8_passed
        ])
        
        # Build checklist string
        checklist = "📋 <b>8-STEP CHECKLIST:</b>\n"
        for line in checklist_lines:
            checklist += f"   {line}\n"
        checklist += f"\n   📊 <b>SCORE:</b> {pass_count}/8 steps passed"
        # ==========================================
        
        msg = f"""
{update_emoji}{tier_emoji} <b>ROMEOTPT v3.2 - {quality.get('tier', 'C')} Tier</b>

<b>🎯 {setup.get('symbol', 'UNKNOWN')}</b> | {setup.get('side', 'N/A')}
<b>Entry:</b> {setup.get('entry_price', 0):.8f}
<b>Current:</b> {setup.get('current_price', 0):.8f}
<b>Type:</b> {setup.get('entry_type', 'N/A')}{update_info}

{checklist}

🎯 <b>Targets:</b>
TP1: {tp1_display}
TP2: {tp2_display}

🛡️ <b>Risk:</b>
SL: {setup.get('sl_price', 0):.8f}
RR: {setup.get('rr_ratio', 0):.2f}:1

📈 <b>Quality Score:</b> {quality.get('total_score', 0):.2f}/5.0
• Sweep Strength: {quality.get('sweep_strength', 0):.2f}
• HTF Alignment: {quality.get('htfc_alignment', 0):.2f}
• Structure Shift: {'✅' if quality.get('structure_shift', False) else '❌'}
• Confirmation: {'✅' if quality.get('confirmation_candle', False) else '❌'}

<i>Detected: {datetime.datetime.utcnow().strftime('%H:%M:%S UTC')}</i>
"""
        
        await send_telegram(msg)
    except Exception as e:
        log.error(f"Error sending alert: {e}")

async def send_outcome_alert(symbol: str, outcome: Dict):
    """Send alert when signal hits TP or SL"""
    
    try:
        signal = signal_tracker.active_signals.get(symbol, {})
        setup = signal.get('setup', {})
        
        if outcome['type'] == 'TP1_HIT':
            emoji = "✅"
            result_text = "TAKE PROFIT 1 HIT"
        elif outcome['type'] == 'TP2_HIT':
            emoji = "🎯"
            result_text = "TAKE PROFIT 2 HIT"
        else:
            emoji = "❌"
            result_text = "STOP LOSS HIT"
        
        bars_held = outcome.get('bars_held', 0)
        if bars_held < 60:
            time_str = f"{bars_held}min"
        else:
            time_str = f"{bars_held//60}h {bars_held%60}min"
        
        tp_targets = setup.get('tp_targets', [0])
        # Format TP separately
        tp_display = f"{tp_targets[0]:.8f}" if len(tp_targets) > 0 else 'N/A'
        
        msg = f"""
{emoji} <b>{result_text}</b>

<b>{symbol}</b> | {setup.get('side', 'N/A')}
<b>Entry:</b> {setup.get('entry_price', 0):.8f}
<b>Exit:</b> {outcome['price']:.8f}
<b>PnL:</b> {outcome['pnl_pct']:+.2f}%

⏱️ <b>Held:</b> {time_str}
📊 <b>Quality was:</b> {setup.get('quality', {}).get('tier', 'N/A')}
🎯 <b>Target was:</b> {tp_display}
🛡️ <b>SL was:</b> {setup.get('sl_price', 0):.8f}

<i>Max favorable move: {outcome.get('max_favorable', 0):.2f}%</i>
<i>Max adverse move: {outcome.get('max_adverse', 0):.2f}%</i>

<i>Outcome recorded: {datetime.datetime.utcnow().strftime('%H:%M:%S UTC')}</i>
"""
        
        await send_telegram(msg)
    except Exception as e:
        log.error(f"Error sending outcome alert: {e}")

async def send_deduped_alert(setup: Dict):
    """Send alert only if it's a new or meaningfully updated signal"""
    try:
        symbol = setup.get('symbol', '')
        if not symbol:
            return False
        
        should_alert, reason = signal_tracker.is_new_or_updated_signal(symbol, setup)
        
        if should_alert:
            await send_fast_alert(setup)
            signal_tracker.update_signal(symbol, setup, alerted=True)
            log.info(f"📨 Alert sent for {symbol}: {reason}")
            return True
        else:
            signal_tracker.update_signal(symbol, setup, alerted=False)
            if np.random.random() < 0.01:
                log.debug(f"⏸️  Skipped alert for {symbol}: {reason}")
            return False
    except Exception as e:
        log.error(f"Error in deduped alert for {setup.get('symbol', 'UNKNOWN')}: {e}")
        return False

# ---------------- DATABASE ----------------
async def init_database():
    """Initialize database with outcome tracking tables"""
    try:
        # Create tables
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                timestamp TEXT,
                side TEXT,
                entry_price REAL,
                sl_price REAL,
                tp1 REAL,
                tp2 REAL,
                rr_ratio REAL,
                quality_tier TEXT,
                quality_score REAL,
                current_price REAL,
                status TEXT DEFAULT 'active',
                alert_sent BOOLEAN DEFAULT 1,
                closed_at TEXT,
                closed_price REAL,
                outcome TEXT,
                pnl_pct REAL,
                bars_held INTEGER,
                max_favorable_pct REAL,
                max_adverse_pct REAL
            )
        """)
        
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signal_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                symbol TEXT,
                side TEXT,
                entry_price REAL,
                sl_price REAL,
                tp1_price REAL,
                tp2_price REAL,
                quality_score REAL,
                created_at TEXT,
                status TEXT DEFAULT 'active',
                closed_at TEXT,
                closed_price REAL,
                outcome_type TEXT,
                pnl_pct REAL,
                hold_time_minutes INTEGER,
                max_favorable_pct REAL,
                max_adverse_pct REAL,
                FOREIGN KEY (signal_id) REFERENCES signals (id)
            )
        """)
        
        # Create indexes separately (SQLite syntax fix)
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol_time ON signals (symbol, timestamp)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_status_time ON signals (status, timestamp)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_outcome ON signals (outcome)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_outcomes_symbol_status ON signal_outcomes (symbol, status)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_outcomes_outcome_type ON signal_outcomes (outcome_type)")
        
        await db_conn.commit()
        log.info("Database initialized with indexes")
    except Exception as e:
        log.error(f"Error initializing database: {e}")
        raise

async def store_signal(setup: Dict):
    """Store signal in database"""
    async with db_lock:
        try:
            tp_targets = setup.get("tp_targets", [])
            
            # Store in signals table
            cursor = await db_conn.execute("""
                INSERT INTO signals (
                    symbol, timestamp, side, entry_price, sl_price, 
                    tp1, tp2, rr_ratio, quality_tier, quality_score,
                    current_price, status, alert_sent
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', 1)
            """, (
                setup.get("symbol", ""),
                setup.get("timestamp", ""),
                setup.get("side", ""),
                setup.get("entry_price", 0),
                setup.get("sl_price", 0),
                tp_targets[0] if len(tp_targets) > 0 else None,
                tp_targets[1] if len(tp_targets) > 1 else None,
                setup.get("rr_ratio", 0),
                setup.get("quality", {}).get("tier", "C"),
                setup.get("quality", {}).get("total_score", 0),
                setup.get("current_price", 0)
            ))
            
            # Get the inserted ID
            signal_id = cursor.lastrowid
            
            # Also store in outcomes table for tracking
            await db_conn.execute("""
                INSERT INTO signal_outcomes (
                    signal_id, symbol, side, entry_price, sl_price, tp1_price,
                    tp2_price, quality_score, created_at, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')
            """, (
                signal_id,
                setup.get("symbol", ""),
                setup.get("side", ""),
                setup.get("entry_price", 0),
                setup.get("sl_price", 0),
                tp_targets[0] if len(tp_targets) > 0 else None,
                tp_targets[1] if len(tp_targets) > 1 else None,
                setup.get("quality", {}).get("total_score", 0),
                setup.get("timestamp", "")
            ))
            
            await db_conn.commit()
            log.debug(f"Stored signal for {setup.get('symbol', 'UNKNOWN')} with ID {signal_id}")
            
        except Exception as e:
            log.error(f"Error storing signal {setup.get('symbol', 'UNKNOWN')}: {e}")

async def store_outcome(symbol: str, outcome: Dict):
    """Store signal outcome in database"""
    async with db_lock:
        try:
            now = datetime.datetime.utcnow().isoformat()
            
            # Update signals table
            await db_conn.execute("""
                UPDATE signals 
                SET status = 'closed', closed_at = ?, closed_price = ?, outcome = ?,
                    pnl_pct = ?, bars_held = ?, max_favorable_pct = ?, max_adverse_pct = ?
                WHERE symbol = ? AND status = 'active'
                ORDER BY timestamp DESC LIMIT 1
            """, (
                now,
                outcome.get('price', 0),
                outcome.get('type', ''),
                outcome.get('pnl_pct', 0),
                outcome.get('bars_held', 0),
                outcome.get('max_favorable', 0),
                outcome.get('max_adverse', 0),
                symbol
            ))
            
            # Update signal_outcomes table
            await db_conn.execute("""
                UPDATE signal_outcomes 
                SET status = 'closed', closed_at = ?, closed_price = ?, outcome_type = ?,
                    pnl_pct = ?, hold_time_minutes = ?, max_favorable_pct = ?, max_adverse_pct = ?
                WHERE symbol = ? AND status = 'active'
                ORDER BY created_at DESC LIMIT 1
            """, (
                now,
                outcome.get('price', 0),
                outcome.get('type', ''),
                outcome.get('pnl_pct', 0),
                outcome.get('bars_held', 0),
                outcome.get('max_favorable', 0),
                outcome.get('max_adverse', 0),
                symbol
            ))
            
            await db_conn.commit()
            log.info(f"Stored outcome for {symbol}: {outcome.get('type', 'UNKNOWN')}")
            
        except Exception as e:
            log.error(f"Error storing outcome for {symbol}: {e}")

# ---------------- OUTCOME CHECKER ----------------
async def outcome_checker_task(exchange):
    """Background task to check signal outcomes"""
    log.info("🔄 Outcome checker started")
    
    while True:
        try:
            active_symbols = list(signal_tracker.active_signals.keys())
            
            if active_symbols:
                tickers = await safe_fetch_tickers(exchange)
                outcomes_found = 0
                
                for symbol in active_symbols:
                    if symbol in tickers:
                        current_price = tickers[symbol].get('last', 0)
                        if current_price > 0:
                            outcome = signal_tracker.check_signal_outcome(symbol, current_price)
                            if outcome:
                                await send_outcome_alert(symbol, outcome)
                                await store_outcome(symbol, outcome)
                                outcomes_found += 1
                
                if outcomes_found:
                    log.info(f"📊 Found {outcomes_found} signal outcomes")
            
            await asyncio.sleep(OUTCOME_CHECK_INTERVAL)
            
        except Exception as e:
            log.error(f"Outcome checker error: {e}")
            await asyncio.sleep(OUTCOME_CHECK_INTERVAL * 2)

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

async def outcome_aware_scanner(exchange):
    """Main scanner with outcome tracking"""
    
    # Send startup message
    startup_msg = f"""
🚀 <b>ROMEOTPT v3.2 STARTED</b>

<b>Settings:</b>
• Scan: {SCAN_INTERVAL}s
• Top: {TOP_N} symbols
• Rate limit: {MAX_REQUESTS_PER_SECOND} req/s
• Concurrency: {MAX_CONCURRENT}
• Cooldown: {SIGNAL_COOLDOWN_MINUTES}min
• Validity: {SIGNAL_VALIDITY_HOURS}h
• Outcome check: {OUTCOME_CHECK_INTERVAL}s
• Min quality: {MIN_QUALITY_SCORE}

<i>Now with 8-step numerical checklist display</i>
"""
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
                if symbol.endswith("/USDT"):
                    # Skip stablecoin pairs
                    if symbol in ["USDC/USDT", "USDG/USDT"]:
                        continue
                    
                    volume = data.get("quoteVolume", 0)
                    if isinstance(volume, (int, float)):
                        usdt_pairs.append((symbol, float(volume)))
            
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            symbols_to_scan = [s[0] for s in usdt_pairs[:TOP_N]]
            
            stats = signal_tracker.get_stats()
            
            log.info(f"🔄 Scan #{scan_cycle}: {len(symbols_to_scan)} symbols | Active: {stats.get('active_signals', 0)}")
            
            # Log stats periodically
            if scan_cycle % 10 == 0:
                outcome_stats = signal_tracker.outcome_stats
                total_closed = outcome_stats.get('tp1_hits', 0) + outcome_stats.get('tp2_hits', 0) + outcome_stats.get('sl_hits', 0)
                if total_closed > 0:
                    win_rate = outcome_stats.get('win_rate', 0)
                    log.info(f"📈 Stats: WR={win_rate:.1f}% | TP1={outcome_stats.get('tp1_hits', 0)} | SL={outcome_stats.get('sl_hits', 0)}")
            
            # Scan symbols WITH CONCURRENCY CONTROL
            alerts_this_scan = 0
            tasks = []
            
            # Process in smaller batches
            batch_size = MAX_CONCURRENT
            
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
        "version": "3.2",
        "active_signals": stats.get('active_signals', 0),
        "outcome_stats": signal_tracker.outcome_stats
    }

@app.get("/signals/active")
async def get_active_signals():
    """Get currently active signals"""
    active = []
    for symbol, data in signal_tracker.active_signals.items():
        setup = data.get('setup', {})
        active.append({
            "symbol": symbol,
            "side": setup.get('side', ''),
            "entry_price": setup.get('entry_price', 0),
            "current_price": setup.get('current_price', 0),
            "tp1": setup.get('tp_targets', [0])[0] if len(setup.get('tp_targets', [])) > 0 else 0,
            "sl": setup.get('sl_price', 0),
            "quality": setup.get('quality', {}).get('total_score', 0),
            "tier": setup.get('quality', {}).get('tier', 'C'),
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
                    AVG(hold_time_minutes) as avg_hold_time
                FROM signal_outcomes 
                WHERE status = 'closed' 
                AND closed_at > datetime('now', ?)
            """, (f"-{hours} hours",))
            row = await cursor.fetchone()
            
            cursor = await db_conn.execute("""
                SELECT 
                    quality_tier,
                    COUNT(*) as count,
                    SUM(CASE WHEN outcome LIKE 'TP%' THEN 1 ELSE 0 END) as wins,
                    AVG(pnl_pct) as avg_pnl
                FROM signals 
                WHERE status = 'closed' 
                AND timestamp > datetime('now', ?)
                GROUP BY quality_tier
            """, (f"-{hours} hours",))
            rows = await cursor.fetchall()
            tier_stats = {}
            for row in rows:
                if row[0]:  # Only add if tier is not None
                    tier_stats[row[0]] = {
                        'count': row[1],
                        'wins': row[2],
                        'avg_pnl': row[3]
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
        'by_tier': tier_stats,
        'memory_stats': signal_tracker.outcome_stats
    }

@app.get("/outcomes/recent")
async def get_recent_outcomes(limit: int = 20):
    """Get recent signal outcomes"""
    async with db_lock:
        try:
            cursor = await db_conn.execute("""
                SELECT s.symbol, s.side, s.entry_price, s.closed_price, 
                       s.outcome, s.pnl_pct, s.bars_held, s.quality_tier,
                       s.timestamp, s.closed_at
                FROM signals s
                WHERE s.status = 'closed'
                ORDER BY s.closed_at DESC
                LIMIT ?
            """, (limit,))
            columns = [description[0] for description in cursor.description]
            rows = await cursor.fetchall()
            
            outcomes = []
            for row in rows:
                outcomes.append(dict(zip(columns, row)))
        except Exception as e:
            log.error(f"Error fetching recent outcomes: {e}")
            return {"error": str(e)}
    
    return {"outcomes": outcomes, "count": len(outcomes)}

# ---------------- MAIN ----------------
async def periodic_cleanup():
    """Periodically clean up old signals"""
    while True:
        await asyncio.sleep(300)
        signal_tracker.cleanup_old_signals()

async def main():
    global db_conn
    
    try:
        # Initialize database
        db_conn = await aiosqlite.connect(DB_PATH)
        await init_database()
        
        # Create exchange with MORE conservative settings
        exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
            "rateLimit": 200,  # Increased from 10 to 200ms between requests
            "timeout": 10000,  # Increased timeout
            "verbose": False,
        })
        
        # Add additional rate limiting at exchange level
        exchange.sleep = lambda ms: asyncio.sleep(ms / 1000)
        
        log.info("🚀 ROMEOTPT v3.2 - COMPLETE WITH RATE LIMITING")
        log.info(f"Scan: {SCAN_INTERVAL}s | Top {TOP_N} symbols")
        log.info(f"Rate limit: {MAX_REQUESTS_PER_SECOND} req/s | Concurrency: {MAX_CONCURRENT}")
        log.info(f"Cooldown: {SIGNAL_COOLDOWN_MINUTES}min | Validity: {SIGNAL_VALIDITY_HOURS}h")
        log.info(f"Outcome check: {OUTCOME_CHECK_INTERVAL}s")
        log.info(f"Now with 8-step numerical checklist display")
        
        # Start cleanup task
        asyncio.create_task(periodic_cleanup())
        
        await outcome_aware_scanner(exchange)
        
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