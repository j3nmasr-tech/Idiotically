#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT SCANNER v3.3 - CORE LIQUIDITY MODEL
Two-layer architecture + Deduplication + Outcome Tracking
WITH ROMEOTPT MUST-HAVE STEPS
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
DB_PATH = os.getenv("DB_PATH", "/app/data/romeopt_v3_3.db")

# Scanner settings
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 30))
TOP_N = int(os.getenv("TOP_N", 60))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 10))

# Signal thresholds
MIN_QUALITY_SCORE = float(os.getenv("MIN_QUALITY_SCORE", 2.5))  # Minimum to be valid RomeOTPT

# Deduplication settings
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", 15))
SIGNAL_VALIDITY_HOURS = int(os.getenv("SIGNAL_VALIDITY_HOURS", 2))
PRICE_MOVEMENT_THRESHOLD = float(os.getenv("PRICE_MOVEMENT_THRESHOLD", 0.5))

# Outcome tracking
OUTCOME_CHECK_INTERVAL = int(os.getenv("OUTCOME_CHECK_INTERVAL", 60))
MINIMUM_TRADE_HOLD_SECONDS = int(os.getenv("MINIMUM_TRADE_HOLD_SECONDS", 30))

# Liquidity detection settings
LIQUIDITY_SWEEP_MIN_PCT = float(os.getenv("LIQUIDITY_SWEEP_MIN_PCT", 0.3))  # Min 0.3% sweep
LIQUIDITY_TARGET_MULTIPLIER = float(os.getenv("LIQUIDITY_TARGET_MULTIPLIER", 1.2))  # TP beyond liquidity

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("romeopt_v3_3")

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
class LiquidityData:
    """Liquidity mapping data"""
    from_liquidity_price: float = 0.0  # Liquidity that was taken
    to_liquidity_price: float = 0.0    # Next liquidity target
    sweep_confirmed: bool = False      # Whether sweep actually happened
    sweep_strength: float = 0.0        # 0.0-1.0 strength
    liquidity_distance_pct: float = 0.0  # Distance to target liquidity
    
@dataclass
class SetupQuality:
    """LAYER 2: Quality metrics with RomeOTPT MUST-HAVE steps"""
    # Core RomeOTPT MUST-HAVE steps
    liquidity_map_valid: bool = False      # STEP 1: Liquidity Map defined
    liquidity_sweep_confirmed: bool = False # STEP 2: Liquidity Sweep confirmed
    entry_zone_defined: bool = False       # STEP 3: Entry Zone + SL defined
    take_profit_liquidity: bool = False    # STEP 4: TP at liquidity target
    
    # Additional quality metrics
    structure_shift: bool = False
    confirmation_candle: bool = False
    htfc_alignment_score: float = 0.0
    total_score: float = 0.0
    
    # Liquidity data
    liquidity_data: LiquidityData = None
    
    @property
    def quality_tier(self) -> str:
        # Must have all 4 RomeOTPT steps to be valid
        if not self.is_valid_romeopt:
            return "INVALID"
        
        if self.total_score >= 4.0:
            return "A+"
        elif self.total_score >= 3.0:
            return "A"
        elif self.total_score >= 2.5:
            return "B"
        else:
            return "C"
    
    @property
    def is_valid_romeopt(self) -> bool:
        """Check if setup has all RomeOTPT MUST-HAVE steps"""
        return all([
            self.liquidity_map_valid,      # STEP 1
            self.liquidity_sweep_confirmed, # STEP 2
            self.entry_zone_defined,       # STEP 3
            self.take_profit_liquidity     # STEP 4
        ])

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
            'win_rate': 0.0,
            'romeopt_signals': 0,
            'romeopt_wins': 0,
            'romeopt_win_rate': 0.0
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
        
        # Check if RomeOTPT validity changed
        old_valid = old_setup.get('quality', {}).get('is_valid_romeopt', False)
        new_valid = new_setup.get('quality', {}).get('is_valid_romeopt', False)
        if old_valid != new_valid:
            return True, f"RomeOTPT validity changed: {old_valid}→{new_valid}"
        
        # Check if liquidity target changed
        old_liquidity = old_setup.get('liquidity_data', {}).get('to_liquidity_price', 0)
        new_liquidity = new_setup.get('liquidity_data', {}).get('to_liquidity_price', 0)
        if old_liquidity > 0 and new_liquidity > 0:
            liquidity_change_pct = abs(new_liquidity - old_liquidity) / old_liquidity * 100
            if liquidity_change_pct > 2.0:  # 2% change in liquidity target
                return True, f"Liquidity target changed {liquidity_change_pct:.1f}%"
        
        return False, "Same signal, minimal changes"
    
    def update_signal(self, symbol: str, setup: Dict, alerted: bool = False):
        """Update or add signal to tracker"""
        now = datetime.datetime.utcnow()
        
        if symbol not in self.active_signals:
            is_romeopt = setup.get('quality', {}).get('is_valid_romeopt', False)
            
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
                'outcome_details': None,
                'is_romeopt': is_romeopt
            }
            self.outcome_stats['total_signals'] += 1
            self.outcome_stats['active'] += 1
            if is_romeopt:
                self.outcome_stats['romeopt_signals'] += 1
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
                if signal.get('is_romeopt'):
                    self.outcome_stats['romeopt_wins'] += 1
            elif outcome['type'] == 'TP2_HIT':
                self.outcome_stats['tp2_hits'] += 1
                if signal.get('is_romeopt'):
                    self.outcome_stats['romeopt_wins'] += 1
            elif outcome['type'] == 'SL_HIT':
                self.outcome_stats['sl_hits'] += 1
            
            wins = self.outcome_stats['tp1_hits'] + self.outcome_stats['tp2_hits']
            losses = self.outcome_stats['sl_hits']
            total_closed = wins + losses
            if total_closed > 0:
                self.outcome_stats['win_rate'] = wins / total_closed * 100
            
            if self.outcome_stats['romeopt_signals'] > 0:
                self.outcome_stats['romeopt_win_rate'] = (
                    self.outcome_stats['romeopt_wins'] / self.outcome_stats['romeopt_signals'] * 100
                )
            
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
        romeopt_signals = 0
        
        for signal in self.active_signals.values():
            setup = signal.get('setup', {})
            if setup.get('side') == 'BUY':
                buy_signals += 1
            elif setup.get('side') == 'SELL':
                sell_signals += 1
            
            if signal.get('is_romeopt'):
                romeopt_signals += 1
        
        return {
            'active_signals': active_count,
            'romeopt_signals': romeopt_signals,
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

# ---------------- UTILS ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 100):
    """Fetch OHLCV with timeout"""
    try:
        return await asyncio.wait_for(
            exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit),
            timeout=3.0
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

def detect_liquidity_levels(df, side="BUY", lookback_bars=50):
    """Detect liquidity levels (highs for sells, lows for buys)"""
    if df is None or len(df) < lookback_bars:
        return []
    
    if side == "BUY":
        # For BUY: look for recent highs that were swept (liquidity taken)
        highs = df['high'].values
        liquidity_levels = []
        
        for i in range(len(highs) - 10, len(highs) - 1):
            if highs[i] == max(highs[max(0, i-5):i+1]):
                # Check if this high was swept
                if i < len(highs) - 1 and highs[i+1] > highs[i]:
                    liquidity_levels.append(float(highs[i]))
        
        return sorted(liquidity_levels, reverse=True)[:3]  # Top 3 levels
    
    else:  # SELL
        # For SELL: look for recent lows that were swept
        lows = df['low'].values
        liquidity_levels = []
        
        for i in range(len(lows) - 10, len(lows) - 1):
            if lows[i] == min(lows[max(0, i-5):i+1]):
                # Check if this low was swept
                if i < len(lows) - 1 and lows[i+1] < lows[i]:
                    liquidity_levels.append(float(lows[i]))
        
        return sorted(liquidity_levels)[:3]  # Bottom 3 levels

def detect_liquidity_sweep(df, side="BUY", lookback_bars=20):
    """Detect if a liquidity sweep occurred"""
    if df is None or len(df) < lookback_bars:
        return False, 0.0
    
    if side == "BUY":
        # For BUY: look for sweep of recent highs
        recent_highs = df['high'].iloc[-lookback_bars:-5].values
        current_lowest = df['low'].iloc[-5:].min()
        
        if len(recent_highs) > 0:
            highest_recent = max(recent_highs)
            sweep_pct = (highest_recent - current_lowest) / highest_recent * 100
            
            # Sweep occurred if price went below a recent high
            sweep_detected = current_lowest < highest_recent
            
            return sweep_detected, sweep_pct
    
    else:  # SELL
        # For SELL: look for sweep of recent lows
        recent_lows = df['low'].iloc[-lookback_bars:-5].values
        current_highest = df['high'].iloc[-5:].max()
        
        if len(recent_lows) > 0:
            lowest_recent = min(recent_lows)
            sweep_pct = (current_highest - lowest_recent) / lowest_recent * 100
            
            # Sweep occurred if price went above a recent low
            sweep_detected = current_highest > lowest_recent
            
            return sweep_detected, sweep_pct
    
    return False, 0.0

def find_next_liquidity_target(df, side="BUY", current_price=0.0, lookback_bars=100):
    """Find the next liquidity target"""
    if df is None or len(df) < lookback_bars:
        return 0.0
    
    if side == "BUY":
        # For BUY: find next significant high (liquidity above)
        highs = df['high'].iloc[-lookback_bars:].values
        
        # Sort highs and find the next one above current price
        highs_above = [h for h in highs if h > current_price]
        if highs_above:
            next_target = min(highs_above)
            
            # Find clusters of highs around this level
            cluster_size = 0.005  # 0.5% cluster
            cluster_highs = [h for h in highs if abs(h - next_target) / next_target <= cluster_size]
            
            if len(cluster_highs) >= 2:  # At least 2 highs in cluster
                return float(np.mean(cluster_highs)) * LIQUIDITY_TARGET_MULTIPLIER
    
    else:  # SELL
        # For SELL: find next significant low (liquidity below)
        lows = df['low'].iloc[-lookback_bars:].values
        
        # Sort lows and find the next one below current price
        lows_below = [l for l in lows if l < current_price]
        if lows_below:
            next_target = max(lows_below)
            
            # Find clusters of lows around this level
            cluster_size = 0.005  # 0.5% cluster
            cluster_lows = [l for l in lows if abs(l - next_target) / next_target <= cluster_size]
            
            if len(cluster_lows) >= 2:  # At least 2 lows in cluster
                return float(np.mean(cluster_lows)) / LIQUIDITY_TARGET_MULTIPLIER
    
    return 0.0

# ---------------- LAYER 1: FAST ELIGIBILITY CHECK ----------------
async def check_eligibility_fast(exchange, symbol: str) -> SetupEligibility:
    """LAYER 1: FAST FILTER - ELIGIBILITY ONLY"""
    
    # Get current price
    try:
        ticker = await exchange.fetch_ticker(symbol)
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
            
            if current_price <= recent_low_15m * 1.02:  # Within 2% of recent low
                entry_price = current_price
                entry_type = "DISCOUNT_ZONE"
                entry_low = recent_low_15m * 0.995
                entry_high = recent_low_15m * 1.02
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
            
            if current_price >= recent_high_15m * 0.98:  # Within 2% of recent high
                entry_price = current_price
                entry_type = "PREMIUM_ZONE"
                entry_low = recent_high_15m * 0.98
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
            sl_price = min(recent_low_15m * 0.99, entry_price * 0.985)  # 1-1.5% SL
        else:
            sl_price = max(recent_high_15m * 1.01, entry_price * 1.015)  # 1-1.5% SL
    except:
        # Fallback SL
        if side == "BUY":
            sl_price = entry_price * 0.985
        else:
            sl_price = entry_price * 1.015
    
    # TP targets (will be refined in Layer 2 with liquidity)
    tp_targets = []
    
    try:
        if side == "BUY":
            # Initial TP based on recent structure
            recent_resistance = df_1h['high'].iloc[-10:].max()
            tp_targets.append(float(recent_resistance * 1.01))  # Just above resistance
            
            range_high = float(df_1h['high'].iloc[-20:].max())
            range_low = float(df_1h['low'].iloc[-20:].min())
            range_height = range_high - range_low
            tp_targets.append(float(min(range_high + range_height * 0.5, entry_price * 1.03)))
        else:
            # Initial TP based on recent structure
            recent_support = df_1h['low'].iloc[-10:].min()
            tp_targets.append(float(recent_support * 0.99))  # Just below support
            
            range_high = float(df_1h['high'].iloc[-20:].max())
            range_low = float(df_1h['low'].iloc[-20:].min())
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

# ---------------- LAYER 2: QUALITY ANALYSIS WITH ROMEOTPT MUST-HAVE STEPS ----------------
async def analyze_quality(exchange, symbol: str, eligibility: SetupEligibility) -> SetupQuality:
    """LAYER 2: QUALITY ANALYSIS WITH ROMEOTPT MUST-HAVE STEPS"""
    
    side = eligibility.side
    entry_type = eligibility.entry_type
    entry_price = eligibility.entry_price
    
    # Initialize LiquidityData
    liquidity_data = LiquidityData()
    
    # Get current price
    ticker = await exchange.fetch_ticker(symbol)
    current_price = ticker.get("last", entry_price)
    
    # Initialize quality metrics
    structure_shift = False
    confirmation_candle = False
    htfc_alignment_score = 0.0
    
    # ============ ROMEOTPT MUST-HAVE STEP 1: LIQUIDITY MAP ============
    try:
        # Get 1H data for liquidity mapping
        ohlcv_1h = await fetch_ohlcv(exchange, symbol, "1h", 100)
        if ohlcv_1h:
            df_1h = create_dataframe(ohlcv_1h)
            if df_1h is not None and len(df_1h) >= 50:
                # Detect FROM liquidity (recent highs/lows that were swept)
                liquidity_levels = detect_liquidity_levels(df_1h, side, 50)
                
                if side == "BUY":
                    # For BUY: FROM liquidity is recent highs that were swept
                    if liquidity_levels:
                        # Use the most recent significant high as FROM liquidity
                        liquidity_data.from_liquidity_price = liquidity_levels[0]
                        
                        # Check if we're trading FROM that liquidity (price below recent high)
                        if current_price < liquidity_data.from_liquidity_price:
                            liquidity_map_valid = True
                        else:
                            liquidity_map_valid = False
                    else:
                        liquidity_map_valid = False
                
                else:  # SELL
                    # For SELL: FROM liquidity is recent lows that were swept
                    if liquidity_levels:
                        # Use the most recent significant low as FROM liquidity
                        liquidity_data.from_liquidity_price = liquidity_levels[0]
                        
                        # Check if we're trading FROM that liquidity (price above recent low)
                        if current_price > liquidity_data.from_liquidity_price:
                            liquidity_map_valid = True
                        else:
                            liquidity_map_valid = False
                    else:
                        liquidity_map_valid = False
                
                # Find TO liquidity (next target)
                liquidity_data.to_liquidity_price = find_next_liquidity_target(
                    df_1h, side, current_price, 100
                )
                
                if liquidity_data.to_liquidity_price > 0:
                    # Calculate distance to target liquidity
                    if side == "BUY":
                        liquidity_data.liquidity_distance_pct = (
                            (liquidity_data.to_liquidity_price - current_price) / current_price * 100
                        )
                    else:
                        liquidity_data.liquidity_distance_pct = (
                            (current_price - liquidity_data.to_liquidity_price) / current_price * 100
                        )
                else:
                    liquidity_map_valid = False
            else:
                liquidity_map_valid = False
        else:
            liquidity_map_valid = False
    except Exception as e:
        log.debug(f"Liquidity map error for {symbol}: {e}")
        liquidity_map_valid = False
    
    # ============ ROMEOTPT MUST-HAVE STEP 2: LIQUIDITY SWEEP ============
    try:
        # Get 15m data for sweep detection
        ohlcv_15m = await fetch_ohlcv(exchange, symbol, "15m", 30)
        if ohlcv_15m:
            df_15m = create_dataframe(ohlcv_15m)
            if df_15m is not None and len(df_15m) >= 20:
                sweep_detected, sweep_pct = detect_liquidity_sweep(df_15m, side, 20)
                
                liquidity_data.sweep_confirmed = sweep_detected
                liquidity_data.sweep_strength = min(sweep_pct / 2.0, 1.0)  # Normalize to 0-1
                
                # Sweep must be at least LIQUIDITY_SWEEP_MIN_PCT%
                liquidity_sweep_confirmed = (
                    sweep_detected and 
                    sweep_pct >= LIQUIDITY_SWEEP_MIN_PCT and
                    liquidity_data.sweep_strength > 0.3
                )
            else:
                liquidity_sweep_confirmed = False
        else:
            liquidity_sweep_confirmed = False
    except Exception as e:
        log.debug(f"Liquidity sweep error for {symbol}: {e}")
        liquidity_sweep_confirmed = False
    
    # ============ ROMEOTPT MUST-HAVE STEP 3: ENTRY ZONE + SL ============
    entry_zone_defined = (
        eligibility.entry_type in ["DISCOUNT_ZONE", "BULLISH_ENGULFING", "PREMIUM_ZONE", "BEARISH_ENGULFING"] and
        eligibility.sl_price > 0 and
        abs(entry_price - eligibility.sl_price) / entry_price > 0.005  # SL must be meaningful (>0.5%)
    )
    
    # ============ ROMEOTPT MUST-HAVE STEP 4: TAKE PROFIT AT LIQUIDITY TARGET ============
    if liquidity_data.to_liquidity_price > 0:
        # TP must be at or beyond liquidity target
        if side == "BUY":
            take_profit_liquidity = (
                liquidity_data.to_liquidity_price > 0 and
                eligibility.tp_targets and
                any(tp >= liquidity_data.to_liquidity_price * 0.995 for tp in eligibility.tp_targets)
            )
        else:  # SELL
            take_profit_liquidity = (
                liquidity_data.to_liquidity_price > 0 and
                eligibility.tp_targets and
                any(tp <= liquidity_data.to_liquidity_price * 1.005 for tp in eligibility.tp_targets)
            )
    else:
        take_profit_liquidity = False
    
    # ============ ADDITIONAL QUALITY METRICS ============
    try:
        # HTF Alignment
        if ohlcv_1h:
            df_1h = create_dataframe(ohlcv_1h)
            if df_1h is not None and len(df_1h) >= 20:
                df_1h['ema_20'] = df_1h['close'].ewm(span=20).mean()
                df_1h['ema_50'] = df_1h['close'].ewm(span=50).mean()
                
                if side == "BUY":
                    htfc_alignment_score = 1.0 if df_1h['ema_20'].iloc[-1] > df_1h['ema_50'].iloc[-1] else 0.5
                else:
                    htfc_alignment_score = 1.0 if df_1h['ema_20'].iloc[-1] < df_1h['ema_50'].iloc[-1] else 0.5
        
        # Structure Shift
        if ohlcv_1h and df_1h is not None and len(df_1h) >= 11:
            if side == "BUY":
                recent_high = df_1h['high'].iloc[-10:-1].max()
                current_close = df_1h['close'].iloc[-1]
                structure_shift = current_close > recent_high
            else:
                recent_low = df_1h['low'].iloc[-10:-1].min()
                current_close = df_1h['close'].iloc[-1]
                structure_shift = current_close < recent_low
        
        # Confirmation Candle
        ohlcv_5m = await fetch_ohlcv(exchange, symbol, "5m", 5)
        if ohlcv_5m:
            df_5m = create_dataframe(ohlcv_5m)
            if df_5m is not None and len(df_5m) > 0:
                if side == "BUY":
                    confirmation_candle = df_5m['close'].iloc[-1] > df_5m['open'].iloc[-1]
                else:
                    confirmation_candle = df_5m['close'].iloc[-1] < df_5m['open'].iloc[-1]
                    
    except Exception as e:
        log.debug(f"Additional quality metrics error for {symbol}: {e}")
    
    # Calculate total score
    total_score = (
        (1.0 if liquidity_map_valid else 0.0) +
        (1.0 if liquidity_sweep_confirmed else 0.0) +
        (1.0 if entry_zone_defined else 0.0) +
        (1.0 if take_profit_liquidity else 0.0) +
        (0.5 if structure_shift else 0.0) +
        (0.5 if confirmation_candle else 0.0) +
        htfc_alignment_score
    )
    
    return SetupQuality(
        liquidity_map_valid=liquidity_map_valid,
        liquidity_sweep_confirmed=liquidity_sweep_confirmed,
        entry_zone_defined=entry_zone_defined,
        take_profit_liquidity=take_profit_liquidity,
        structure_shift=structure_shift,
        confirmation_candle=confirmation_candle,
        htfc_alignment_score=htfc_alignment_score,
        total_score=total_score,
        liquidity_data=liquidity_data
    )

# ---------------- FAST SCANNING ----------------
async def scan_symbol_fast(exchange, symbol: str) -> Optional[Dict]:
    """ULTRA-FAST scanning: Layer 1 only, Layer 2 optional"""
    
    try:
        # LAYER 1: Eligibility check
        eligibility = await check_eligibility_fast(exchange, symbol)
        
        if not eligibility.eligible:
            return None
        
        # LAYER 2: Quality analysis with RomeOTPT MUST-HAVE steps
        quality = await analyze_quality(exchange, symbol, eligibility)
        
        # Only proceed if it's a valid RomeOTPT setup
        if not quality.is_valid_romeopt:
            log.debug(f"Skipping {symbol}: Not a valid RomeOTPT setup")
            return None
        
        # Get current price
        ticker = await exchange.fetch_ticker(symbol)
        current_price = ticker.get("last", 0)
        
        # Calculate RR based on liquidity target
        risk = abs(eligibility.entry_price - eligibility.sl_price)
        
        # Use liquidity target as primary TP
        if quality.liquidity_data and quality.liquidity_data.to_liquidity_price > 0:
            primary_tp = quality.liquidity_data.to_liquidity_price
            # Update TP targets to prioritize liquidity target
            eligibility.tp_targets = [primary_tp]
            if side == "BUY":
                eligibility.tp_targets.append(primary_tp * 1.02)  # Second TP 2% beyond liquidity
            else:
                eligibility.tp_targets.append(primary_tp * 0.98)  # Second TP 2% beyond liquidity
        else:
            primary_tp = eligibility.tp_targets[0] if eligibility.tp_targets else 0
        
        reward = abs(primary_tp - eligibility.entry_price)
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
            
            # Liquidity data
            "liquidity_data": {
                "from_liquidity_price": quality.liquidity_data.from_liquidity_price if quality.liquidity_data else 0,
                "to_liquidity_price": quality.liquidity_data.to_liquidity_price if quality.liquidity_data else 0,
                "sweep_confirmed": quality.liquidity_data.sweep_confirmed if quality.liquidity_data else False,
                "sweep_strength": quality.liquidity_data.sweep_strength if quality.liquidity_data else 0,
                "liquidity_distance_pct": quality.liquidity_data.liquidity_distance_pct if quality.liquidity_data else 0
            },
            
            "quality": {
                "tier": quality.quality_tier,
                "is_valid_romeopt": quality.is_valid_romeopt,
                "total_score": quality.total_score,
                
                # RomeOTPT MUST-HAVE steps
                "liquidity_map_valid": quality.liquidity_map_valid,
                "liquidity_sweep_confirmed": quality.liquidity_sweep_confirmed,
                "entry_zone_defined": quality.entry_zone_defined,
                "take_profit_liquidity": quality.take_profit_liquidity,
                
                # Additional metrics
                "structure_shift": quality.structure_shift,
                "confirmation_candle": quality.confirmation_candle,
                "htfc_alignment": quality.htfc_alignment_score
            }
        }
        
        return setup
    except Exception as e:
        log.error(f"Error scanning {symbol}: {e}")
        return None

# ---------------- ALERTS ----------------
async def send_fast_alert(setup: Dict):
    """Send concise, fast alerts with RomeOTPT MUST-HAVE steps"""
    
    try:
        symbol = setup.get('symbol', 'UNKNOWN')
        quality = setup.get('quality', {})
        liquidity_data = setup.get('liquidity_data', {})
        
        # Check if this is an update
        is_update = symbol in signal_tracker.active_signals
        update_emoji = "🔄" if is_update else "🆕"
        
        tier_emoji = {
            "A+": "🔥",
            "A": "✅", 
            "B": "⚠️",
            "C": "📊",
            "INVALID": "❌"
        }.get(quality.get("tier", "INVALID"), "❌")
        
        # Only send alerts for valid RomeOTPT setups
        if not quality.get('is_valid_romeopt', False):
            log.debug(f"Skipping alert for {symbol}: Not a valid RomeOTPT setup")
            return
        
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
        # Format TP values
        tp1_display = f"{tp_targets[0]:.8f}" if len(tp_targets) > 0 else 'N/A'
        tp2_display = f"{tp_targets[1]:.8f}" if len(tp_targets) > 1 else 'N/A'
        
        # ============ ROMEOTPT MUST-HAVE STEPS DISPLAY ============
        # Build the RomeOTPT MUST-HAVE checklist
        romeopt_steps = [
            {
                "number": "1️⃣",
                "name": "LIQUIDITY MAP",
                "status": quality.get('liquidity_map_valid', False),
                "details": f"FROM: {liquidity_data.get('from_liquidity_price', 0):.8f} | TO: {liquidity_data.get('to_liquidity_price', 0):.8f}"
            },
            {
                "number": "2️⃣",
                "name": "LIQUIDITY SWEEP",
                "status": quality.get('liquidity_sweep_confirmed', False),
                "details": f"Sweep: {'✅' if liquidity_data.get('sweep_confirmed', False) else '❌'} | Strength: {liquidity_data.get('sweep_strength', 0):.2f}"
            },
            {
                "number": "3️⃣",
                "name": "ENTRY ZONE + SL",
                "status": quality.get('entry_zone_defined', False),
                "details": f"Type: {setup.get('entry_type', 'N/A')} | SL: {setup.get('sl_price', 0):.8f}"
            },
            {
                "number": "4️⃣",
                "name": "TAKE PROFIT AT LIQUIDITY",
                "status": quality.get('take_profit_liquidity', False),
                "details": f"TP1 at liquidity: {tp1_display}"
            }
        ]
        
        # Build checklist string
        checklist = "🔴 <b>ROMEOTPT MUST-HAVE STEPS:</b>\n\n"
        for step in romeopt_steps:
            status_emoji = "✅" if step['status'] else "❌"
            checklist += f"{step['number']} <b>{step['name']}</b> {status_emoji}\n"
            checklist += f"   └─ {step['details']}\n"
        
        # Count passes
        pass_count = sum(step['status'] for step in romeopt_steps)
        checklist += f"\n📊 <b>ROMEOTPT SCORE:</b> {pass_count}/4 steps passed"
        
        # Additional liquidity info
        liquidity_info = ""
        if liquidity_data.get('liquidity_distance_pct', 0) > 0:
            liquidity_info = f"\n🎯 <b>Liquidity Distance:</b> {liquidity_data.get('liquidity_distance_pct', 0):.2f}%"
        # ==========================================
        
        msg = f"""
{update_emoji}{tier_emoji} <b>ROMEOTPT v3.3 - {quality.get('tier', 'INVALID')}</b>

<b>🎯 {setup.get('symbol', 'UNKNOWN')}</b> | {setup.get('side', 'N/A')}
<b>Entry:</b> {setup.get('entry_price', 0):.8f}
<b>Current:</b> {setup.get('current_price', 0):.8f}
<b>Type:</b> {setup.get('entry_type', 'N/A')}{update_info}

{checklist}{liquidity_info}

🎯 <b>Targets (Liquidity Delivery):</b>
TP1: {tp1_display}
TP2: {tp2_display}

🛡️ <b>Risk:</b>
SL: {setup.get('sl_price', 0):.8f}
RR: {setup.get('rr_ratio', 0):.2f}:1

📈 <b>Quality Score:</b> {quality.get('total_score', 0):.2f}/6.0
• Structure Shift: {'✅' if quality.get('structure_shift', False) else '❌'}
• Confirmation: {'✅' if quality.get('confirmation_candle', False) else '❌'}
• HTF Alignment: {quality.get('htfc_alignment', 0):.2f}

<i>ROMEOTPT: Liquidity delivery model | Without liquidity = no reason for price to move</i>
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
            result_text = "LIQUIDITY DELIVERED - TP1 HIT"
        elif outcome['type'] == 'TP2_HIT':
            emoji = "🎯"
            result_text = "LIQUIDITY DELIVERED - TP2 HIT"
        else:
            emoji = "❌"
            result_text = "STOP LOSS HIT"
        
        bars_held = outcome.get('bars_held', 0)
        if bars_held < 60:
            time_str = f"{bars_held}min"
        else:
            time_str = f"{bars_held//60}h {bars_held%60}min"
        
        tp_targets = setup.get('tp_targets', [0])
        tp_display = f"{tp_targets[0]:.8f}" if len(tp_targets) > 0 else 'N/A'
        
        # Check if this was a RomeOTPT setup
        romeopt_status = ""
        if signal.get('is_romeopt'):
            romeopt_status = "\n🏛️ <b>ROMEOTPT SIGNAL</b>"
        
        msg = f"""
{emoji} <b>{result_text}</b>{romeopt_status}

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
        
        # Only send alerts for valid RomeOTPT setups
        if not setup.get('quality', {}).get('is_valid_romeopt', False):
            return False
        
        should_alert, reason = signal_tracker.is_new_or_updated_signal(symbol, setup)
        
        if should_alert:
            await send_fast_alert(setup)
            signal_tracker.update_signal(symbol, setup, alerted=True)
            log.info(f"🏛️ RomeOTPT alert sent for {symbol}: {reason}")
            return True
        else:
            signal_tracker.update_signal(symbol, setup, alerted=False)
            if np.random.random() < 0.01:
                log.debug(f"⏸️  Skipped RomeOTPT alert for {symbol}: {reason}")
            return False
    except Exception as e:
        log.error(f"Error in deduped alert for {setup.get('symbol', 'UNKNOWN')}: {e}")
        return False

# ---------------- DATABASE ----------------
async def init_database():
    """Initialize database with RomeOTPT tracking"""
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
                is_romeopt BOOLEAN DEFAULT 0,
                current_price REAL,
                status TEXT DEFAULT 'active',
                alert_sent BOOLEAN DEFAULT 1,
                closed_at TEXT,
                closed_price REAL,
                outcome TEXT,
                pnl_pct REAL,
                bars_held INTEGER,
                max_favorable_pct REAL,
                max_adverse_pct REAL,
                
                -- Liquidity data
                from_liquidity_price REAL,
                to_liquidity_price REAL,
                sweep_confirmed BOOLEAN,
                sweep_strength REAL,
                liquidity_distance_pct REAL
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
                is_romeopt BOOLEAN DEFAULT 0,
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
        
        # Create indexes
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol_time ON signals (symbol, timestamp)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_status_time ON signals (status, timestamp)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_romeopt ON signals (is_romeopt)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_outcomes_symbol_status ON signal_outcomes (symbol, status)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_outcomes_romeopt ON signal_outcomes (is_romeopt)")
        
        await db_conn.commit()
        log.info("Database initialized with RomeOTPT tracking")
    except Exception as e:
        log.error(f"Error initializing database: {e}")
        raise

async def store_signal(setup: Dict):
    """Store signal in database"""
    async with db_lock:
        try:
            tp_targets = setup.get("tp_targets", [])
            liquidity_data = setup.get("liquidity_data", {})
            quality = setup.get("quality", {})
            
            # Store in signals table
            cursor = await db_conn.execute("""
                INSERT INTO signals (
                    symbol, timestamp, side, entry_price, sl_price, 
                    tp1, tp2, rr_ratio, quality_tier, quality_score,
                    is_romeopt, current_price, status, alert_sent,
                    from_liquidity_price, to_liquidity_price,
                    sweep_confirmed, sweep_strength, liquidity_distance_pct
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', 1, ?, ?, ?, ?, ?)
            """, (
                setup.get("symbol", ""),
                setup.get("timestamp", ""),
                setup.get("side", ""),
                setup.get("entry_price", 0),
                setup.get("sl_price", 0),
                tp_targets[0] if len(tp_targets) > 0 else None,
                tp_targets[1] if len(tp_targets) > 1 else None,
                setup.get("rr_ratio", 0),
                quality.get("tier", "INVALID"),
                quality.get("total_score", 0),
                1 if quality.get("is_valid_romeopt", False) else 0,
                setup.get("current_price", 0),
                liquidity_data.get("from_liquidity_price", 0),
                liquidity_data.get("to_liquidity_price", 0),
                1 if liquidity_data.get("sweep_confirmed", False) else 0,
                liquidity_data.get("sweep_strength", 0),
                liquidity_data.get("liquidity_distance_pct", 0)
            ))
            
            # Get the inserted ID
            signal_id = cursor.lastrowid
            
            # Also store in outcomes table for tracking
            await db_conn.execute("""
                INSERT INTO signal_outcomes (
                    signal_id, symbol, side, entry_price, sl_price, tp1_price,
                    tp2_price, quality_score, is_romeopt, created_at, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')
            """, (
                signal_id,
                setup.get("symbol", ""),
                setup.get("side", ""),
                setup.get("entry_price", 0),
                setup.get("sl_price", 0),
                tp_targets[0] if len(tp_targets) > 0 else None,
                tp_targets[1] if len(tp_targets) > 1 else None,
                quality.get("total_score", 0),
                1 if quality.get("is_valid_romeopt", False) else 0,
                setup.get("timestamp", "")
            ))
            
            await db_conn.commit()
            log.debug(f"Stored RomeOTPT signal for {setup.get('symbol', 'UNKNOWN')} with ID {signal_id}")
            
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
                tickers = await exchange.fetch_tickers()
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
                is_romeopt = result.get("quality", {}).get("is_valid_romeopt", False)
                
                # Only process valid RomeOTPT setups
                if is_romeopt and quality_score >= MIN_QUALITY_SCORE:
                    alerted = await send_deduped_alert(result)
                    if alerted:
                        alerts_sent += 1
                    await store_signal(result)
                else:
                    log.debug(f"Skipped non-RomeOTPT or low quality signal for {result.get('symbol', 'UNKNOWN')}")
            except Exception as e:
                log.error(f"Error processing result: {e}")
    
    return alerts_sent

async def outcome_aware_scanner(exchange):
    """Main scanner with outcome tracking"""
    
    # Send startup message
    startup_msg = f"""
🏛️ <b>ROMEOTPT v3.3 - LIQUIDITY DELIVERY MODEL STARTED</b>

<b>Core Principles:</b>
• RomeOTPT is a liquidity delivery model
• No liquidity = no reason for price to move
• Trade is meaningless without liquidity map

<b>MUST-HAVE STEPS:</b>
1️⃣ Liquidity Map (FROM/TO liquidity)
2️⃣ Liquidity Sweep (Confirmation)
3️⃣ Entry Zone + SL (Controlled entry)
4️⃣ Take Profit at Liquidity (Target)

<b>Settings:</b>
• Scan: {SCAN_INTERVAL}s
• Top: {TOP_N} symbols
• Min sweep: {LIQUIDITY_SWEEP_MIN_PCT}%
• Min quality: {MIN_QUALITY_SCORE}
• Cooldown: {SIGNAL_COOLDOWN_MINUTES}min

<i>Only valid RomeOTPT setups will be alerted</i>
"""
    await send_telegram(startup_msg)
    
    # Start outcome checker
    asyncio.create_task(outcome_checker_task(exchange))
    
    scan_cycle = 0
    
    while True:
        scan_cycle += 1
        
        try:
            # Get symbols
            tickers = await exchange.fetch_tickers()
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
            
            log.info(f"🔄 Scan #{scan_cycle}: {len(symbols_to_scan)} symbols | Active: {stats.get('active_signals', 0)} | RomeOTPT: {stats.get('romeopt_signals', 0)}")
            
            # Log stats periodically
            if scan_cycle % 10 == 0:
                outcome_stats = signal_tracker.outcome_stats
                total_closed = outcome_stats.get('tp1_hits', 0) + outcome_stats.get('tp2_hits', 0) + outcome_stats.get('sl_hits', 0)
                if total_closed > 0:
                    win_rate = outcome_stats.get('win_rate', 0)
                    romeopt_win_rate = outcome_stats.get('romeopt_win_rate', 0)
                    log.info(f"📈 Stats: All={win_rate:.1f}% | RomeOTPT={romeopt_win_rate:.1f}% | TP1={outcome_stats.get('tp1_hits', 0)} | SL={outcome_stats.get('sl_hits', 0)}")
            
            # Scan symbols
            alerts_this_scan = 0
            tasks = []
            
            for symbol in symbols_to_scan:
                task = asyncio.create_task(scan_symbol_fast(exchange, symbol))
                tasks.append(task)
                
                if len(tasks) >= MAX_CONCURRENT:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    alerts_this_scan += await process_deduped_results(results)
                    tasks = []
            
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                alerts_this_scan += await process_deduped_results(results)
            
            if alerts_this_scan > 0:
                log.info(f"🏛️ Sent {alerts_this_scan} RomeOTPT alerts this scan")
            
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
        "version": "3.3 - RomeOTPT Liquidity Model",
        "active_signals": stats.get('active_signals', 0),
        "romeopt_signals": stats.get('romeopt_signals', 0),
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
            "tier": setup.get('quality', {}).get('tier', 'INVALID'),
            "is_romeopt": setup.get('quality', {}).get('is_valid_romeopt', False),
            "age_minutes": (datetime.datetime.utcnow() - data.get('first_seen', datetime.datetime.utcnow())).total_seconds() / 60
        })
    return {"active_signals": active, "count": len(active)}

@app.get("/outcomes/stats")
async def get_outcome_stats(hours: int = 24):
    """Get outcome statistics"""
    async with db_lock:
        try:
            # All signals
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
            
            # RomeOTPT signals only
            cursor = await db_conn.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN outcome_type LIKE 'TP%' THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN outcome_type = 'SL_HIT' THEN 1 ELSE 0 END) as losses,
                    AVG(pnl_pct) as avg_pnl,
                    AVG(hold_time_minutes) as avg_hold_time
                FROM signal_outcomes 
                WHERE status = 'closed' 
                AND is_romeopt = 1
                AND closed_at > datetime('now', ?)
            """, (f"-{hours} hours",))
            romeopt_row = await cursor.fetchone()
            
            # By tier
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
    
    romeopt_total = romeopt_row[0] if romeopt_row else 0
    romeopt_wins = romeopt_row[1] if romeopt_row else 0
    
    return {
        'period_hours': hours,
        'all_signals': {
            'total': total,
            'wins': wins,
            'losses': row[2] if row else 0,
            'win_rate': wins / total * 100 if total > 0 else 0,
            'avg_pnl_pct': row[3] if row else 0,
            'avg_hold_minutes': row[4] if row else 0
        },
        'romeopt_signals': {
            'total': romeopt_total,
            'wins': romeopt_wins,
            'losses': romeopt_row[2] if romeopt_row else 0,
            'win_rate': romeopt_wins / romeopt_total * 100 if romeopt_total > 0 else 0,
            'avg_pnl_pct': romeopt_row[3] if romeopt_row else 0,
            'avg_hold_minutes': romeopt_row[4] if romeopt_row else 0
        },
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
                       s.is_romeopt, s.timestamp, s.closed_at
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
        
        # Create exchange
        exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
            "rateLimit": 10,
            "timeout": 5000,
        })
        
        log.info("🏛️ ROMEOTPT v3.3 - LIQUIDITY DELIVERY MODEL")
        log.info(f"Scan: {SCAN_INTERVAL}s | Top {TOP_N} symbols")
        log.info(f"Min sweep: {LIQUIDITY_SWEEP_MIN_PCT}% | Min quality: {MIN_QUALITY_SCORE}")
        log.info(f"Cooldown: {SIGNAL_COOLDOWN_MINUTES}min | Validity: {SIGNAL_VALIDITY_HOURS}h")
        log.info(f"Only valid RomeOTPT setups will be alerted")
        
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