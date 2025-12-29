#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 CONFIRMED REJECTION SYSTEM
Professional discretionary trading system - FIXED VERSION
Wave-length awareness + Strength analysis + CONFIRMED REJECTION entries
TRADER MINDSET: Reaction-based, CONFIRMED rejection specialist
CORRECTED: No early entry - Only after failure + strength shift
"""

import os
import time
import asyncio
import logging
import hashlib
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import json

# ================ CONFIRMED REJECTION CONFIG ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/confirmed_rejection.db"

# Scanning - CONFIRMED REJECTION TRADING
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 5))
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 100))
MIN_VOLUME_USD = 500000

# Trading parameters (CONFIRMED REJECTION)
MAX_STOP_LOSS_PCT = 1.0
MIN_TARGET_PCT = 1.5
MAX_TARGET_PCT = 4.0
MIN_RISK_REWARD = 2.0

# CONFIRMED REJECTION scanning - CRITICAL CHANGE
CONFIRMED_REJECTION_CONFIG = {
    "rsi_failure_required": True,
    "min_failure_candle_strength": 0.7,
    "min_trigger_candle_strength": 0.8,
    "volume_ratio_required": 1.3,
    "price_confirmation_required": True,
    "two_candle_confirmation": True,
}

# Timeframes for CONFIRMED REJECTION TRADING
TIMEFRAMES = {
    "1H": "1h",
    "15M": "15m",
    "5M": "5m",
    "3M": "3m",
    "1M": "1m"
}

EMA_PERIODS = {
    "fast": 9,
    "medium": 21,
    "slow": 50
}

RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# ================ DATA STRUCTURES ================
@dataclass
class WaveContext:
    """Wave length and maturity context - NO WAVE COUNTING"""
    wave_length: str
    wave_maturity: float
    expansion_speed: float
    structure_type: str
    context_side: str

@dataclass
class MarketStrength:
    """Market strength analysis"""
    candle_speed: float
    distance_ratio: float
    ema_angle: float
    volume_participation: float
    strength_score: float
    
    # Interpretation flags
    is_continuation: bool
    is_rejection_setup: bool
    is_absorption: bool
    is_compression: bool

@dataclass
class RejectionZone:
    """Key rejection area analysis"""
    zone_type: str
    price_level: float
    strength: float
    volume_confirmation: bool
    rsi_position: str
    is_active: bool

@dataclass
class FailureCandle:
    """Failure candle at rejection zone"""
    candle_type: str
    strength: float
    volume_weakness: bool
    rsi_failure: bool
    body_ratio: float
    close_position: str
    
    # Critical failure signs
    has_long_wick: bool
    failed_break: bool
    momentum_halt: bool

@dataclass
class TriggerCandle:
    """Trigger candle (strength shift)"""
    candle_type: str
    strength: float
    volume_confirmation: bool
    direction: str
    
    # Entry criteria
    close_near_extreme: bool
    body_dominance: float
    speed_increase: bool
    
    # RSI confirmation
    rsi_slope_change: bool
    rsi_divergence: bool

@dataclass
class ConfirmedRejection:
    """Confirmed rejection (failure + trigger)"""
    failure_candle: FailureCandle
    trigger_candle: TriggerCandle
    confirmation_score: float
    is_valid: bool
    
    # Timing
    failure_timestamp: float
    trigger_timestamp: float
    timeframe_pair: Tuple[str, str]
    
    # Conditions met
    conditions_met: List[str]

@dataclass
class ConfirmedRejectionSignal:
    """CONFIRMED rejection-based trade signal"""
    signal_id: str
    symbol: str
    side: str
    
    # Price levels
    entry_price: float
    stop_loss: float
    take_profit: float
    
    # Analysis context
    wave_context: WaveContext
    market_strength: MarketStrength
    rejection_zone: RejectionZone
    
    # CONFIRMED REJECTION STRUCTURE
    confirmed_rejection: ConfirmedRejection
    entry_reason: str
    
    # Metrics
    rejection_strength: float
    risk_reward: float
    expected_move_pct: float
    
    # Timing
    failure_timeframe: str
    trigger_timeframe: str
    signal_timestamp: float
    
    # Entry validation
    passed_no_entry_filters: bool
    early_entry_prevented: bool

# ================ PROFESSIONAL LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("confirmed_rejection_scanner")

# ================ CORE CONFIRMED REJECTION ENGINE ================
class ConfirmedRejectionScanner:
    """High-frequency CONFIRMED rejection scanner - FIXED VERSION"""
    
    class SignalDeduplicator:
        """Prevents duplicate signal generation - TRADE-BASED"""
        
        def __init__(self):
            self.active_signals = {}
            self.signal_status = {}
        
        def should_generate_signal(self, symbol: str, side: str, price: float) -> bool:
            """Check if we should generate a new signal"""
            
            if symbol in self.active_signals:
                signal_id = self.active_signals[symbol]
                
                if signal_id in self.signal_status:
                    status = self.signal_status[signal_id].get("status", "UNKNOWN")
                    
                    if status != "CLOSED":
                        log.debug(f"{symbol}: Active {side} signal exists (status: {status})")
                        return False
            
            return True
        
        def register_signal(self, signal: ConfirmedRejectionSignal):
            """Register a new signal"""
            symbol = signal.symbol
            
            if symbol in self.active_signals:
                old_signal_id = self.active_signals[symbol]
                if old_signal_id in self.signal_status:
                    del self.signal_status[old_signal_id]
            
            self.active_signals[symbol] = signal.signal_id
            self.signal_status[signal.signal_id] = {
                "symbol": symbol,
                "side": signal.side,
                "price": signal.entry_price,
                "status": "PENDING",
                "timestamp": signal.signal_timestamp
            }
            
            log.debug(f"Registered confirmed signal {signal.signal_id[:8]} for {symbol}")
        
        def update_signal_status(self, signal_id: str, status: str):
            """Update signal status"""
            if signal_id in self.signal_status:
                self.signal_status[signal_id]["status"] = status
                log.debug(f"Signal {signal_id[:8]} status updated to {status}")
                
                if status == "CLOSED":
                    symbol = self.signal_status[signal_id]["symbol"]
                    log.info(f"✅ Signal {signal_id[:8]} for {symbol} CLOSED - Ready for new confirmations")
        
        def remove_closed_signals(self):
            """Clean up closed signals"""
            current_time = time.time()
            closed_signal_ids = []
            
            for signal_id, data in list(self.signal_status.items()):
                if data.get("status") == "CLOSED":
                    if current_time - data.get("timestamp", 0) > 3600:
                        closed_signal_ids.append(signal_id)
            
            for signal_id in closed_signal_ids:
                symbol = self.signal_status[signal_id]["symbol"]
                
                if self.active_signals.get(symbol) == signal_id:
                    del self.active_signals[symbol]
                
                del self.signal_status[signal_id]
                log.debug(f"Cleaned up closed signal {signal_id[:8]} for {symbol}")
    
    def __init__(self):
        self.daily_stats = {
            "rejections_detected": 0,
            "failures_confirmed": 0,
            "confirmed_signals": 0,
            "early_entries_prevented": 0,
            "no_trigger_candle": 0,
            "no_strength_shift": 0,
            "volume_failed": 0,
            "rsi_no_failure": 0,
            "pairs_scanned": 0
        }
        self.deduplicator = self.SignalDeduplicator()
        self.active_signal_ids = set()
    
    # ========== WAVE CONTEXT ANALYSIS ==========
    
    def analyze_wave_context(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> WaveContext:
        """Analyze wave context"""
        try:
            if df_1h is None or df_15m is None:
                return self._get_default_wave_context()
            
            if len(df_1h) < 20 or len(df_15m) < 30:
                return self._get_default_wave_context()
            
            # Wave length analysis
            wave_length, wave_maturity = self._analyze_wave_length(df_1h)
            
            # Expansion speed
            expansion_speed = self._analyze_expansion_speed(df_15m)
            
            # Structure type
            structure_type = self._determine_structure(df_15m)
            
            # Context side
            context_side = self._determine_context_side(df_1h, df_15m)
            
            return WaveContext(
                wave_length=wave_length,
                wave_maturity=wave_maturity,
                expansion_speed=expansion_speed,
                structure_type=structure_type,
                context_side=context_side
            )
            
        except Exception as e:
            log.error(f"Wave context error: {e}")
            return self._get_default_wave_context()
    
    def _analyze_wave_length(self, df: pd.DataFrame) -> Tuple[str, float]:
        """Analyze wave length and maturity"""
        try:
            if len(df) < 30:
                return "MEDIUM", 0.5
            
            recent_prices = df['close'].values[-30:]
            total_move = abs(recent_prices[-1] - recent_prices[0])
            avg_candle_size = np.mean(np.abs(np.diff(recent_prices)))
            
            if avg_candle_size == 0:
                return "MEDIUM", 0.5
            
            move_ratio = total_move / avg_candle_size
            
            if move_ratio < 15:
                wave_length = "SHORT"
            elif move_ratio < 30:
                wave_length = "MEDIUM"
            else:
                wave_length = "EXTENDED"
            
            # Wave maturity
            ma_20 = np.mean(recent_prices[-20:])
            current_price = recent_prices[-1]
            volatility = np.std(recent_prices[-20:])
            
            if volatility > 0:
                distance_pct = abs(current_price - ma_20) / ma_20 * 100
                volatility_pct = volatility / ma_20 * 100
                wave_maturity = min(distance_pct / (volatility_pct * 2), 1.0)
            else:
                wave_maturity = 0.5
            
            return wave_length, wave_maturity
            
        except Exception:
            return "MEDIUM", 0.5
    
    def _analyze_expansion_speed(self, df: pd.DataFrame) -> float:
        """Analyze expansion speed"""
        try:
            if len(df) < 10:
                return 0.5
            
            candles = df.iloc[-10:]
            candle_speeds = []
            
            for i in range(len(candles)):
                candle = candles.iloc[i]
                candle_range = candle['high'] - candle['low']
                if candle['close'] != 0:
                    speed = candle_range / candle['close'] * 100
                    candle_speeds.append(speed)
            
            if not candle_speeds:
                return 0.5
            
            avg_speed = np.mean(candle_speeds)
            expansion_speed = min(avg_speed / 5.0, 1.0)
            
            return expansion_speed
            
        except Exception:
            return 0.5
    
    def _determine_structure(self, df: pd.DataFrame) -> str:
        """Determine market structure"""
        try:
            if len(df) < 20:
                return "COMPRESSION"
            
            prices = df['close'].values[-20:]
            highs = df['high'].values[-20:]
            lows = df['low'].values[-20:]
            
            price_change = prices[-1] - prices[0]
            price_change_pct = abs(price_change) / prices[0] * 100
            range_ratio = (np.max(highs) - np.min(lows)) / prices[0] * 100
            
            if price_change_pct > 3 and range_ratio > 5:
                return "IMPULSIVE"
            elif range_ratio < 2:
                return "COMPRESSION"
            else:
                return "CORRECTIVE"
                
        except Exception:
            return "COMPRESSION"
    
    def _determine_context_side(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> str:
        """Determine context side"""
        try:
            if len(df_1h) < 10:
                return "NEUTRAL"
            
            prices_1h = df_1h['close'].values[-10:]
            x = np.arange(len(prices_1h))
            slope_1h, _ = np.polyfit(x, prices_1h, 1)
            
            prices_15m = df_15m['close'].values[-5:]
            slope_15m, _ = np.polyfit(np.arange(len(prices_15m)), prices_15m, 1)
            
            total_slope = (slope_1h * 0.7 + slope_15m * 0.3)
            
            if total_slope > 0.001:
                return "BULLISH_CONTEXT"
            elif total_slope < -0.001:
                return "BEARISH_CONTEXT"
            else:
                return "NEUTRAL"
                
        except Exception:
            return "NEUTRAL"
    
    def _get_default_wave_context(self) -> WaveContext:
        return WaveContext(
            wave_length="MEDIUM",
            wave_maturity=0.5,
            expansion_speed=0.5,
            structure_type="COMPRESSION",
            context_side="NEUTRAL"
        )
    
    # ========== MARKET STRENGTH ANALYSIS ==========
    
    def analyze_market_strength(self, df: pd.DataFrame) -> MarketStrength:
        """Analyze market strength"""
        try:
            if df is None or len(df) < 20:
                return self._get_default_market_strength()
            
            candle_speed = self._calculate_candle_speed(df)
            distance_ratio = self._calculate_distance_ratio(df)
            ema_angle = self._calculate_ema_angle(df)
            volume_participation = self._calculate_volume_participation(df)
            
            strength_score = self._calculate_strength_score(
                candle_speed, distance_ratio, ema_angle, volume_participation
            )
            
            is_continuation, is_rejection_setup, is_absorption, is_compression = \
                self._interpret_strength_patterns(df, candle_speed, volume_participation)
            
            return MarketStrength(
                candle_speed=candle_speed,
                distance_ratio=distance_ratio,
                ema_angle=ema_angle,
                volume_participation=volume_participation,
                strength_score=strength_score,
                is_continuation=is_continuation,
                is_rejection_setup=is_rejection_setup,
                is_absorption=is_absorption,
                is_compression=is_compression
            )
            
        except Exception as e:
            log.error(f"Market strength error: {e}")
            return self._get_default_market_strength()
    
    def _calculate_candle_speed(self, df: pd.DataFrame) -> float:
        """Calculate candle speed"""
        try:
            if len(df) < 5:
                return 0.5
            
            candles = df.iloc[-5:]
            speeds = []
            
            for _, candle in candles.iterrows():
                candle_range = candle['high'] - candle['low']
                if candle['close'] > 0:
                    speed = candle_range / candle['close'] * 100
                    speeds.append(speed)
            
            if not speeds:
                return 0.5
            
            avg_speed = np.mean(speeds)
            return min(avg_speed / 2.0, 1.0)
            
        except Exception:
            return 0.5
    
    def _calculate_distance_ratio(self, df: pd.DataFrame) -> float:
        """Calculate distance ratio"""
        try:
            if len(df) < 10:
                return 0.5
            
            prices = df['close'].values[-10:]
            total_distance = abs(prices[-1] - prices[0])
            
            if prices[0] > 0:
                distance_pct = total_distance / prices[0] * 100
                return min(distance_pct / 5.0, 1.0)
            
            return 0.5
            
        except Exception:
            return 0.5
    
    def _calculate_ema_angle(self, df: pd.DataFrame) -> float:
        """Calculate EMA angle"""
        try:
            if len(df) < 20:
                return 0.0
            
            ema_fast = df['close'].ewm(span=EMA_PERIODS['fast'], adjust=False).mean()
            ema_values = ema_fast.values[-10:]
            
            if len(ema_values) < 5:
                return 0.0
            
            x = np.arange(len(ema_values))
            slope, _ = np.polyfit(x, ema_values, 1)
            
            avg_price = np.mean(ema_values)
            if avg_price > 0:
                angle_metric = abs(slope / avg_price * 1000)
                return min(angle_metric, 1.0)
            
            return 0.0
            
        except Exception:
            return 0.0
    
    def _calculate_volume_participation(self, df: pd.DataFrame) -> float:
        """Calculate volume participation"""
        try:
            if len(df) < 20:
                return 0.5
            
            recent_volume = df['volume'].values[-5:].mean()
            avg_volume = df['volume'].values[-20:].mean()
            
            if avg_volume > 0:
                ratio = recent_volume / avg_volume
                if ratio >= 1.0:
                    return min((ratio - 1.0) * 2, 1.0)
                else:
                    return max((ratio - 1.0) * 2, 0.0)
            
            return 0.5
            
        except Exception:
            return 0.5
    
    def _calculate_strength_score(self, candle_speed: float, distance_ratio: float,
                                 ema_angle: float, volume_participation: float) -> float:
        """Calculate strength score"""
        weights = [0.2, 0.2, 0.2, 0.4]
        factors = [candle_speed, distance_ratio, ema_angle, volume_participation]
        
        return np.average(factors, weights=weights)
    
    def _interpret_strength_patterns(self, df: pd.DataFrame, candle_speed: float,
                                    volume_participation: float) -> Tuple[bool, bool, bool, bool]:
        """Interpret strength patterns"""
        try:
            if len(df) < 10:
                return False, False, False, False
            
            price_change = df['close'].iloc[-1] - df['close'].iloc[-5]
            price_change_pct = abs(price_change) / df['close'].iloc[-5] * 100
            
            is_continuation = (candle_speed > 0.7 and volume_participation > 0.7 and 
                              price_change_pct > 1.0)
            
            is_rejection_setup = (candle_speed > 0.7 and volume_participation < 0.3 and 
                                 price_change_pct > 1.0)
            
            is_absorption = (candle_speed < 0.3 and volume_participation > 0.7)
            
            recent_high = df['high'].values[-5:].max()
            recent_low = df['low'].values[-5:].min()
            range_pct = (recent_high - recent_low) / recent_low * 100
            
            is_compression = (range_pct < 1.0 and volume_participation < 0.5)
            
            return is_continuation, is_rejection_setup, is_absorption, is_compression
            
        except Exception:
            return False, False, False, False
    
    def _get_default_market_strength(self) -> MarketStrength:
        return MarketStrength(
            candle_speed=0.5,
            distance_ratio=0.5,
            ema_angle=0.0,
            volume_participation=0.5,
            strength_score=0.5,
            is_continuation=False,
            is_rejection_setup=False,
            is_absorption=False,
            is_compression=False
        )
    
    # ========== REJECTION ZONE ANALYSIS ==========
    
    def find_rejection_zones(self, df: pd.DataFrame, current_price: float,
                            rsi_value: float, emas: Dict[str, float]) -> List[RejectionZone]:
        """Find rejection zones"""
        zones = []
        
        try:
            if df is None or len(df) < 20:
                return zones
            
            # EMA zones
            for ema_name, ema_value in emas.items():
                if ema_value == 0:
                    continue
                
                distance_pct = abs(current_price - ema_value) / ema_value * 100
                
                if distance_pct <= 0.5:  # Within 0.5%
                    if current_price > ema_value:
                        zone_type = "EMA_SUPPORT"
                    else:
                        zone_type = "EMA_RESISTANCE"
                    
                    if ema_name == "fast":
                        strength = 0.7
                    elif ema_name == "medium":
                        strength = 0.8
                    else:
                        strength = 0.9
                    
                    zones.append(RejectionZone(
                        zone_type=zone_type,
                        price_level=ema_value,
                        strength=strength,
                        volume_confirmation=False,
                        rsi_position="NEUTRAL",
                        is_active=True
                    ))
            
            # Range zones
            if len(df) >= 20:
                recent_high = df['high'].values[-20:].max()
                recent_low = df['low'].values[-20:].min()
                
                high_distance_pct = abs(current_price - recent_high) / recent_high * 100
                if high_distance_pct <= 0.3:
                    zones.append(RejectionZone(
                        zone_type="RANGE_HIGH",
                        price_level=recent_high,
                        strength=0.8,
                        volume_confirmation=False,
                        rsi_position="NEUTRAL",
                        is_active=True
                    ))
                
                low_distance_pct = abs(current_price - recent_low) / recent_low * 100
                if low_distance_pct <= 0.3:
                    zones.append(RejectionZone(
                        zone_type="RANGE_LOW",
                        price_level=recent_low,
                        strength=0.8,
                        volume_confirmation=False,
                        rsi_position="NEUTRAL",
                        is_active=True
                    ))
            
            # Failed break zones
            if len(df) >= 10:
                recent_high = df['high'].values[-5:].max()
                prev_high = df['high'].values[-10:-5].max()
                
                if current_price < recent_high and recent_high > prev_high * 1.005:
                    if any(df['close'].values[-5:] < recent_high * 0.995):
                        zones.append(RejectionZone(
                            zone_type="FAILED_BREAKOUT",
                            price_level=recent_high,
                            strength=0.85,
                            volume_confirmation=False,
                            rsi_position="NEUTRAL",
                            is_active=True
                        ))
                
                recent_low = df['low'].values[-5:].min()
                prev_low = df['low'].values[-10:-5].min()
                
                if current_price > recent_low and recent_low < prev_low * 0.995:
                    if any(df['close'].values[-5:] > recent_low * 1.005):
                        zones.append(RejectionZone(
                            zone_type="FAILED_BREAKDOWN",
                            price_level=recent_low,
                            strength=0.85,
                            volume_confirmation=False,
                            rsi_position="NEUTRAL",
                            is_active=True
                        ))
            
            # Set RSI position
            for zone in zones:
                zone.rsi_position = self._analyze_rsi_position(rsi_value, zone.zone_type)
            
            return [z for z in zones if z.is_active]
            
        except Exception as e:
            log.error(f"Rejection zone error: {e}")
            return []
    
    def _analyze_rsi_position(self, rsi_value: float, zone_type: str) -> str:
        """Analyze RSI position"""
        if "SUPPORT" in zone_type or "LOW" in zone_type or "BREAKDOWN" in zone_type:
            if 40 <= rsi_value <= 50:
                return "IN_ZONE"
            elif rsi_value < 30:
                return "OVEREXTENDED"
            else:
                return "NEUTRAL"
        
        elif "RESISTANCE" in zone_type or "HIGH" in zone_type or "BREAKOUT" in zone_type:
            if 50 <= rsi_value <= 60:
                return "IN_ZONE"
            elif rsi_value > 70:
                return "OVEREXTENDED"
            else:
                return "NEUTRAL"
        
        return "NEUTRAL"
    
    # ========== CONFIRMED REJECTION DETECTION ==========
    
    def detect_failure_candle(self, df: pd.DataFrame, zone: RejectionZone,
                             side: str, current_rsi: float, prev_rsi: float) -> Optional[FailureCandle]:
        """Detect FAILURE candle at rejection zone"""
        try:
            if len(df) < 3:
                return None
            
            current_candle = df.iloc[-1]
            prev_candle = df.iloc[-2]
            
            failure_signs = []
            strength_factors = []
            
            # 1. Wick rejection
            has_wick_rejection = False
            wick_strength = 0
            
            if side == "LONG":
                if (current_candle['low'] < zone.price_level * 0.998 and
                    current_candle['close'] > zone.price_level):
                    has_wick_rejection = True
                    wick_length = abs(current_candle['low'] - zone.price_level)
                    candle_range = current_candle['high'] - current_candle['low']
                    if candle_range > 0:
                        wick_strength = min(wick_length / candle_range, 1.0)
                        failure_signs.append("WICK_REJECTION")
                        strength_factors.append(wick_strength)
            
            else:
                if (current_candle['high'] > zone.price_level * 1.002 and
                    current_candle['close'] < zone.price_level):
                    has_wick_rejection = True
                    wick_length = abs(current_candle['high'] - zone.price_level)
                    candle_range = current_candle['high'] - current_candle['low']
                    if candle_range > 0:
                        wick_strength = min(wick_length / candle_range, 1.0)
                        failure_signs.append("WICK_REJECTION")
                        strength_factors.append(wick_strength)
            
            # 2. Momentum halt
            momentum_halt = False
            if side == "LONG":
                if (prev_candle['close'] < prev_candle['open'] and
                    abs(current_candle['close'] - current_candle['open']) / current_candle['open'] < 0.001):
                    momentum_halt = True
                    failure_signs.append("MOMENTUM_HALT")
                    strength_factors.append(0.7)
            else:
                if (prev_candle['close'] > prev_candle['open'] and
                    abs(current_candle['close'] - current_candle['open']) / current_candle['open'] < 0.001):
                    momentum_halt = True
                    failure_signs.append("MOMENTUM_HALT")
                    strength_factors.append(0.7)
            
            # 3. Failed break
            failed_break = False
            if side == "LONG":
                if (current_candle['low'] < zone.price_level and
                    current_candle['close'] > zone.price_level and
                    prev_candle['close'] < prev_candle['open']):
                    failed_break = True
                    failure_signs.append("FAILED_BREAK")
                    strength_factors.append(0.8)
            else:
                if (current_candle['high'] > zone.price_level and
                    current_candle['close'] < zone.price_level and
                    prev_candle['close'] > prev_candle['open']):
                    failed_break = True
                    failure_signs.append("FAILED_BREAK")
                    strength_factors.append(0.8)
            
            # 4. Volume weakness
            volume_weakness = False
            if side == "LONG":
                if current_candle['close'] < current_candle['open']:
                    current_volume = current_candle['volume']
                    prev_bearish_volume = df[df['close'] < df['open']]['volume'].tail(3).mean()
                    if prev_bearish_volume > 0:
                        volume_ratio = current_volume / prev_bearish_volume
                        if volume_ratio < 0.8:
                            volume_weakness = True
            else:
                if current_candle['close'] > current_candle['open']:
                    current_volume = current_candle['volume']
                    prev_bullish_volume = df[df['close'] > df['open']]['volume'].tail(3).mean()
                    if prev_bullish_volume > 0:
                        volume_ratio = current_volume / prev_bullish_volume
                        if volume_ratio < 0.8:
                            volume_weakness = True
            
            # 5. RSI failure
            rsi_failure = False
            if side == "LONG":
                rsi_slope = current_rsi - prev_rsi
                if rsi_slope > 0:
                    rsi_failure = True
                elif current_rsi < 30 and prev_rsi < 25:
                    rsi_failure = True
            else:
                rsi_slope = current_rsi - prev_rsi
                if rsi_slope < 0:
                    rsi_failure = True
                elif current_rsi > 70 and prev_rsi > 75:
                    rsi_failure = True
            
            # 6. Close position
            close_position = "AT_LEVEL"
            body_ratio = 0
            
            candle_range = current_candle['high'] - current_candle['low']
            if candle_range > 0:
                body_size = abs(current_candle['close'] - current_candle['open'])
                body_ratio = body_size / candle_range
                
                if side == "LONG":
                    if current_candle['close'] > zone.price_level * 1.001:
                        close_position = "ABOVE_ZONE"
                    elif current_candle['close'] < zone.price_level * 0.999:
                        close_position = "BELOW_ZONE"
                    else:
                        close_position = "INSIDE_RANGE"
                else:
                    if current_candle['close'] < zone.price_level * 0.999:
                        close_position = "BELOW_ZONE"
                    elif current_candle['close'] > zone.price_level * 1.001:
                        close_position = "ABOVE_ZONE"
                    else:
                        close_position = "INSIDE_RANGE"
            
            # Calculate failure strength
            if strength_factors:
                failure_strength = np.mean(strength_factors)
            else:
                failure_strength = 0
            
            if failure_strength < CONFIRMED_REJECTION_CONFIG["min_failure_candle_strength"]:
                return None
            
            # Create failure candle
            failure_candle = FailureCandle(
                candle_type=" | ".join(failure_signs) if failure_signs else "WEAK_FAILURE",
                strength=failure_strength,
                volume_weakness=volume_weakness,
                rsi_failure=rsi_failure,
                body_ratio=body_ratio,
                close_position=close_position,
                has_long_wick=has_wick_rejection and wick_strength > 0.3,
                failed_break=failed_break,
                momentum_halt=momentum_halt
            )
            
            return failure_candle
            
        except Exception as e:
            log.error(f"Failure candle detection error: {e}")
            return None
    
    def detect_trigger_candle(self, df_trigger: pd.DataFrame, df_failure: pd.DataFrame,
                            failure_candle: FailureCandle, zone: RejectionZone,
                            side: str, current_rsi: float, prev_rsi: float) -> Optional[TriggerCandle]:
        """Detect TRIGGER candle (strength shift)"""
        try:
            if len(df_trigger) < 3 or len(df_failure) < 1:
                return None
            
            current_candle = df_trigger.iloc[-1]
            prev_candle = df_trigger.iloc[-2]
            failure_candle_data = df_failure.iloc[-1]
            
            # Check direction
            is_bullish_trigger = current_candle['close'] > current_candle['open']
            is_bearish_trigger = current_candle['close'] < current_candle['open']
            
            if side == "LONG" and not is_bullish_trigger:
                return None
            if side == "SHORT" and not is_bearish_trigger:
                return None
            
            # Volume confirmation
            volume_confirmation = False
            trigger_volume = current_candle['volume']
            failure_volume = failure_candle_data['volume']
            
            if failure_volume > 0:
                volume_ratio = trigger_volume / failure_volume
                if volume_ratio >= CONFIRMED_REJECTION_CONFIG["volume_ratio_required"]:
                    volume_confirmation = True
            
            prev_trigger_avg = df_trigger['volume'].iloc[-5:-1].mean()
            if prev_trigger_avg > 0:
                if trigger_volume / prev_trigger_avg >= 1.2:
                    volume_confirmation = True
            
            if not volume_confirmation:
                self.daily_stats["volume_failed"] += 1
                return None
            
            # Close near extreme
            close_near_extreme = False
            candle_range = current_candle['high'] - current_candle['low']
            
            if candle_range > 0:
                if side == "LONG":
                    close_to_high = (current_candle['high'] - current_candle['close']) / candle_range
                    if close_to_high <= 0.2:
                        close_near_extreme = True
                else:
                    close_to_low = (current_candle['close'] - current_candle['low']) / candle_range
                    if close_to_low <= 0.2:
                        close_near_extreme = True
            
            # Body dominance
            body_dominance = 0
            if candle_range > 0:
                body_size = abs(current_candle['close'] - current_candle['open'])
                body_dominance = body_size / candle_range
            
            # Speed increase
            speed_increase = False
            failure_range = failure_candle_data['high'] - failure_candle_data['low']
            trigger_range = candle_range
            
            if failure_candle_data['close'] > 0:
                failure_speed = failure_range / failure_candle_data['close']
                trigger_speed = trigger_range / current_candle['close']
                
                if trigger_speed > failure_speed * 1.3:
                    speed_increase = True
            
            # RSI slope change
            rsi_slope_change = False
            rsi_divergence = False
            
            if len(df_trigger) >= 5:
                rsi_values = self.calculate_rsi(df_trigger['close']).values[-5:]
                if len(rsi_values) >= 3:
                    old_slope = rsi_values[-3] - rsi_values[-4]
                    new_slope = rsi_values[-1] - rsi_values[-2]
                    
                    if side == "LONG":
                        if old_slope <= 0 and new_slope > 0:
                            rsi_slope_change = True
                    else:
                        if old_slope >= 0 and new_slope < 0:
                            rsi_slope_change = True
                    
                    # Divergence
                    prices = df_trigger['close'].values[-5:]
                    if side == "LONG":
                        if (prices[-1] < prices[-3] and rsi_values[-1] > rsi_values[-3]):
                            rsi_divergence = True
                    else:
                        if (prices[-1] > prices[-3] and rsi_values[-1] < rsi_values[-3]):
                            rsi_divergence = True
            
            # Calculate trigger strength
            strength_factors = []
            
            if volume_confirmation:
                strength_factors.append(0.9)
            else:
                strength_factors.append(0.3)
            
            if close_near_extreme:
                strength_factors.append(0.8)
            
            if body_dominance > 0.6:
                strength_factors.append(0.7)
            
            if speed_increase:
                strength_factors.append(0.6)
            
            if rsi_slope_change:
                strength_factors.append(0.8)
            
            if rsi_divergence:
                strength_factors.append(0.9)
            
            trigger_strength = np.mean(strength_factors) if strength_factors else 0
            
            if trigger_strength < CONFIRMED_REJECTION_CONFIG["min_trigger_candle_strength"]:
                return None
            
            # Determine trigger type
            trigger_type = "STRENGTH_SHIFT"
            if rsi_divergence:
                trigger_type = "DIVERGENCE_CONFIRMATION"
            elif speed_increase and body_dominance > 0.7:
                trigger_type = "MOMENTUM_REVERSAL"
            
            direction = "BULLISH_CONFIRMATION" if side == "LONG" else "BEARISH_CONFIRMATION"
            
            # Create trigger candle
            trigger_candle = TriggerCandle(
                candle_type=trigger_type,
                strength=trigger_strength,
                volume_confirmation=volume_confirmation,
                direction=direction,
                close_near_extreme=close_near_extreme,
                body_dominance=body_dominance,
                speed_increase=speed_increase,
                rsi_slope_change=rsi_slope_change,
                rsi_divergence=rsi_divergence
            )
            
            return trigger_candle
            
        except Exception as e:
            log.error(f"Trigger candle detection error: {e}")
            return None
    
    def detect_confirmed_rejection(self, multi_tf_data: Dict[str, pd.DataFrame],
                                 zone: RejectionZone, side: str,
                                 current_rsi_3m: float, prev_rsi_3m: float,
                                 current_rsi_1m: float, prev_rsi_1m: float) -> Optional[ConfirmedRejection]:
        """Detect CONFIRMED rejection (failure on 3M + trigger on 1M)"""
        try:
            df_3m = multi_tf_data.get("3M")
            df_1m = multi_tf_data.get("1M")
            
            if df_3m is None or df_1m is None:
                return None
            
            if len(df_3m) < 3 or len(df_1m) < 3:
                return None
            
            # Detect FAILURE candle on 3M
            failure_candle = self.detect_failure_candle(
                df_3m, zone, side, current_rsi_3m, prev_rsi_3m
            )
            
            if not failure_candle:
                self.daily_stats["rejections_detected"] += 1
                return None
            
            self.daily_stats["failures_confirmed"] += 1
            
            # Detect TRIGGER candle on 1M
            trigger_candle = self.detect_trigger_candle(
                df_1m, df_3m, failure_candle, zone, side, current_rsi_1m, prev_rsi_1m
            )
            
            if not trigger_candle:
                self.daily_stats["no_trigger_candle"] += 1
                return None
            
            # Calculate confirmation score
            confirmation_score = self._calculate_confirmation_score(failure_candle, trigger_candle)
            
            # Check NO ENTRY filters
            passed_filters = self._check_no_entry_filters(failure_candle, trigger_candle, df_1m, side)
            
            if not passed_filters:
                self.daily_stats["early_entries_prevented"] += 1
                return None
            
            # Determine conditions met
            conditions_met = []
            
            if failure_candle.has_long_wick:
                conditions_met.append("FAILURE_WICK")
            if failure_candle.failed_break:
                conditions_met.append("FAILURE_BREAK")
            if failure_candle.momentum_halt:
                conditions_met.append("FAILURE_MOMENTUM_HALT")
            if failure_candle.volume_weakness:
                conditions_met.append("FAILURE_VOLUME_WEAKNESS")
            if failure_candle.rsi_failure:
                conditions_met.append("FAILURE_RSI")
            
            if trigger_candle.volume_confirmation:
                conditions_met.append("TRIGGER_VOLUME")
            if trigger_candle.close_near_extreme:
                conditions_met.append("TRIGGER_CLOSE_EXTREME")
            if trigger_candle.speed_increase:
                conditions_met.append("TRIGGER_SPEED")
            if trigger_candle.rsi_slope_change:
                conditions_met.append("TRIGGER_RSI_SLOPE")
            if trigger_candle.rsi_divergence:
                conditions_met.append("TRIGGER_RSI_DIVERGENCE")
            
            # Create confirmed rejection
            confirmed_rejection = ConfirmedRejection(
                failure_candle=failure_candle,
                trigger_candle=trigger_candle,
                confirmation_score=confirmation_score,
                is_valid=confirmation_score > 0.7 and passed_filters,
                failure_timestamp=time.time() - 120,
                trigger_timestamp=time.time(),
                timeframe_pair=("3M", "1M"),
                conditions_met=conditions_met
            )
            
            return confirmed_rejection
            
        except Exception as e:
            log.error(f"Confirmed rejection detection error: {e}")
            return None
    
    def _calculate_confirmation_score(self, failure: FailureCandle, trigger: TriggerCandle) -> float:
        """Calculate confirmation score"""
        weights = {
            'failure_strength': 0.3,
            'trigger_strength': 0.4,
            'volume_confirmation': 0.15,
            'rsi_confirmation': 0.15
        }
        
        volume_score = 1.0 if trigger.volume_confirmation else 0.3
        
        rsi_score = 0.5
        if failure.rsi_failure and (trigger.rsi_slope_change or trigger.rsi_divergence):
            rsi_score = 0.9
        
        score = (
            failure.strength * weights['failure_strength'] +
            trigger.strength * weights['trigger_strength'] +
            volume_score * weights['volume_confirmation'] +
            rsi_score * weights['rsi_confirmation']
        )
        
        return score
    
    def _check_no_entry_filters(self, failure: FailureCandle, trigger: TriggerCandle,
                               df_1m: pd.DataFrame, side: str) -> bool:
        """NO ENTRY filters to prevent early entries"""
        # Filter 1: Trigger candle strength
        if trigger.strength < 0.7:
            log.debug(f"❌ NO ENTRY: Trigger candle too weak ({trigger.strength:.2f})")
            return False
        
        # Filter 2: No volume confirmation
        if not trigger.volume_confirmation:
            log.debug("❌ NO ENTRY: No volume confirmation")
            return False
        
        # Filter 3: Weak follow-through
        if len(df_1m) >= 4:
            next_candle = df_1m.iloc[-2]
            next_body = abs(next_candle['close'] - next_candle['open'])
            next_range = next_candle['high'] - next_candle['low']
            
            if next_range > 0:
                next_body_ratio = next_body / next_range
                if next_body_ratio < 0.3:
                    log.debug("❌ NO ENTRY: Weak follow-through")
                    return False
        
        # Filter 4: Pressure still present
        if side == "LONG":
            recent_bearish = sum(1 for i in range(-4, 0) 
                               if df_1m.iloc[i]['close'] < df_1m.iloc[i]['open'])
            if recent_bearish >= 2:
                log.debug("❌ NO ENTRY: Bearish pressure still present")
                return False
        else:
            recent_bullish = sum(1 for i in range(-4, 0) 
                               if df_1m.iloc[i]['close'] > df_1m.iloc[i]['open'])
            if recent_bullish >= 2:
                log.debug("❌ NO ENTRY: Bullish pressure still present")
                return False
        
        # Filter 5: Weak failure candle
        if failure.strength < 0.6:
            log.debug(f"❌ NO ENTRY: Failure candle too weak ({failure.strength:.2f})")
            return False
        
        # Filter 6: No clear failure pattern
        if not failure.has_long_wick and not failure.failed_break:
            log.debug("❌ NO ENTRY: No clear failure pattern")
            return False
        
        return True
    
    # ========== INDICATOR CALCULATIONS ==========
    
    def calculate_rsi(self, prices: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
        """Calculate RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_emas(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate EMAs"""
        try:
            emas = {}
            for name, period in EMA_PERIODS.items():
                ema_series = df['close'].ewm(span=period, adjust=False).mean()
                emas[name] = ema_series.iloc[-1] if len(ema_series) > 0 else 0
            return emas
        except Exception:
            return {name: 0 for name in EMA_PERIODS.keys()}
    
    def _check_volume_confirmation(self, df: pd.DataFrame, zone_type: str) -> bool:
        """Check volume confirmation"""
        try:
            if len(df) < 5:
                return False
            
            recent_candles = df.iloc[-5:]
            recent_volume = recent_candles['volume'].values[-2:].mean()
            prev_volume = recent_candles['volume'].values[-5:-2].mean()
            
            if prev_volume > 0:
                volume_ratio = recent_volume / prev_volume
                if volume_ratio >= 1.5:
                    return True
            
            if "FAILED" in zone_type:
                volume_trend = np.polyfit(range(5), recent_candles['volume'].values[-5:], 1)[0]
                if volume_trend < 0:
                    return True
            
            return False
            
        except Exception:
            return False
    
    # ========== SIGNAL GENERATION ==========
    
    def generate_confirmed_rejection_signal(self, multi_tf_data: Dict[str, pd.DataFrame],
                                          symbol: str) -> Optional[ConfirmedRejectionSignal]:
        """Generate CONFIRMED rejection signal"""
        try:
            tf_1h = multi_tf_data.get("1H")
            tf_15m = multi_tf_data.get("15M")
            tf_3m = multi_tf_data.get("3M")
            tf_1m = multi_tf_data.get("1M")
            
            if tf_3m is None or tf_1m is None:
                return None
            
            if len(tf_3m) < 10 or len(tf_1m) < 10:
                return None
            
            # Wave context
            wave_context = self.analyze_wave_context(tf_1h, tf_15m)
            
            # Market strength
            market_strength = self.analyze_market_strength(tf_15m)
            
            if market_strength.strength_score < 0.4:
                self.daily_stats["no_strength_shift"] += 1
                return None
            
            # Rejection zones on 3M
            current_price_3m = tf_3m['close'].iloc[-1]
            emas_3m = self.calculate_emas(tf_3m)
            
            rsi_series_3m = self.calculate_rsi(tf_3m['close'])
            current_rsi_3m = rsi_series_3m.iloc[-1] if len(rsi_series_3m) > 0 else 50
            prev_rsi_3m = rsi_series_3m.iloc[-2] if len(rsi_series_3m) > 1 else 50
            
            rejection_zones = self.find_rejection_zones(tf_3m, current_price_3m, current_rsi_3m, emas_3m)
            
            if not rejection_zones:
                return None
            
            # Volume confirmation
            valid_zones = []
            for zone in rejection_zones:
                zone.volume_confirmation = self._check_volume_confirmation(tf_3m, zone.zone_type)
                if zone.volume_confirmation:
                    valid_zones.append(zone)
            
            if not valid_zones:
                return None
            
            # Select best zone
            best_zone = max(valid_zones, key=lambda z: z.strength)
            
            # Determine side
            side = None
            if best_zone.zone_type in ["EMA_SUPPORT", "RANGE_LOW", "FAILED_BREAKDOWN"]:
                side = "LONG"
            elif best_zone.zone_type in ["EMA_RESISTANCE", "RANGE_HIGH", "FAILED_BREAKOUT"]:
                side = "SHORT"
            
            if not side:
                return None
            
            # RSI on 1M
            rsi_series_1m = self.calculate_rsi(tf_1m['close'])
            current_rsi_1m = rsi_series_1m.iloc[-1] if len(rsi_series_1m) > 0 else 50
            prev_rsi_1m = rsi_series_1m.iloc[-2] if len(rsi_series_1m) > 1 else 50
            
            # Detect confirmed rejection
            confirmed_rejection = self.detect_confirmed_rejection(
                multi_tf_data, best_zone, side,
                current_rsi_3m, prev_rsi_3m,
                current_rsi_1m, prev_rsi_1m
            )
            
            if not confirmed_rejection or not confirmed_rejection.is_valid:
                return None
            
            # Deduplication check
            current_price_1m = tf_1m['close'].iloc[-1]
            if not self.deduplicator.should_generate_signal(symbol, side, current_price_1m):
                self.daily_stats["early_entries_prevented"] += 1
                return None
            
            # Entry price at trigger candle close
            entry_price = current_price_1m
            
            # Stop loss and take profit
            if side == "LONG":
                failure_candle_low = tf_3m['low'].iloc[-1]
                stop_loss = failure_candle_low * 0.998
                stop_loss_pct = abs(entry_price - stop_loss) / entry_price * 100
                
                if stop_loss_pct > MAX_STOP_LOSS_PCT:
                    stop_loss = entry_price * (1 - MAX_STOP_LOSS_PCT / 100)
                
                target_pct = np.random.uniform(MIN_TARGET_PCT, MAX_TARGET_PCT)
                take_profit = entry_price * (1 + target_pct / 100)
            
            else:
                failure_candle_high = tf_3m['high'].iloc[-1]
                stop_loss = failure_candle_high * 1.002
                stop_loss_pct = abs(stop_loss - entry_price) / entry_price * 100
                
                if stop_loss_pct > MAX_STOP_LOSS_PCT:
                    stop_loss = entry_price * (1 + MAX_STOP_LOSS_PCT / 100)
                
                target_pct = np.random.uniform(MIN_TARGET_PCT, MAX_TARGET_PCT)
                take_profit = entry_price * (1 - target_pct / 100)
            
            # Risk/Reward
            risk = abs(entry_price - stop_loss)
            reward = abs(take_profit - entry_price)
            
            if risk == 0:
                return None
            
            risk_reward = reward / risk
            
            if risk_reward < MIN_RISK_REWARD:
                return None
            
            # Rejection strength
            rejection_strength = self._calculate_rejection_strength(
                best_zone, market_strength, wave_context, current_rsi_3m
            )
            
            # Entry reason
            entry_reason = "STRENGTH_SHIFT_CONFIRMED"
            if confirmed_rejection.trigger_candle.rsi_divergence:
                entry_reason = "DIVERGENCE_CONFIRMATION"
            
            # Signal ID
            signal_id = hashlib.md5(
                f"{symbol}:{side}:{entry_price:.8f}:{time.time()}:CONFIRMED".encode()
            ).hexdigest()
            
            # Create signal
            signal = ConfirmedRejectionSignal(
                signal_id=signal_id,
                symbol=symbol,
                side=side,
                
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                
                wave_context=wave_context,
                market_strength=market_strength,
                rejection_zone=best_zone,
                
                confirmed_rejection=confirmed_rejection,
                entry_reason=entry_reason,
                
                rejection_strength=rejection_strength,
                risk_reward=risk_reward,
                expected_move_pct=target_pct,
                
                failure_timeframe="3M",
                trigger_timeframe="1M",
                signal_timestamp=time.time(),
                
                passed_no_entry_filters=True,
                early_entry_prevented=False
            )
            
            # Update tracking
            self.deduplicator.register_signal(signal)
            self.active_signal_ids.add(signal_id)
            
            # Update stats
            self.daily_stats["confirmed_signals"] += 1
            
            log.info(f"🎯 CONFIRMED REJECTION: {symbol} {side} @ {entry_price:.4f}")
            log.info(f"   Failure: {confirmed_rejection.failure_candle.candle_type}")
            log.info(f"   Trigger: {confirmed_rejection.trigger_candle.candle_type}")
            log.info(f"   Confirmation: {confirmed_rejection.confirmation_score:.2f}")
            log.info(f"   Zone: {best_zone.zone_type}, R:R: {risk_reward:.1f}:1")
            
            return signal
            
        except Exception as e:
            log.error(f"Signal error for {symbol}: {e}")
            return None
    
    def _calculate_rejection_strength(self, zone: RejectionZone, strength: MarketStrength,
                                     wave: WaveContext, rsi: float) -> float:
        """Calculate rejection strength"""
        factors = []
        weights = []
        
        factors.append(zone.strength)
        weights.append(0.3)
        
        factors.append(strength.strength_score)
        weights.append(0.25)
        
        if wave.structure_type == "CORRECTIVE":
            wave_score = 0.8
        elif wave.structure_type == "COMPRESSION":
            wave_score = 0.7
        else:
            wave_score = 0.5
        
        wave_score *= (1 - wave.wave_maturity * 0.5)
        
        factors.append(wave_score)
        weights.append(0.2)
        
        if zone.rsi_position == "IN_ZONE":
            rsi_score = 0.9
        elif zone.rsi_position == "OVEREXTENDED":
            rsi_score = 0.3
        else:
            rsi_score = 0.5
        
        factors.append(rsi_score)
        weights.append(0.25)
        
        return np.average(factors, weights=weights)
    
    def get_daily_stats(self) -> Dict:
        """Get daily statistics"""
        return self.daily_stats
    
    def cleanup_old_signals(self):
        """Clean up old signals"""
        self.deduplicator.remove_closed_signals()

# ================ MAIN SCANNER SYSTEM ================
class ConfirmedRejectionScannerSystem:
    """Main scanner system for confirmed rejection trading"""
    
    def __init__(self):
        self.scanner = ConfirmedRejectionScanner()
        self.exchange = None
        self.db = None
        self.scan_cycle = 0
    
    async def initialize(self):
        """Initialize the scanner"""
        log.info("=" * 70)
        log.info("🔥 CONFIRMED REJECTION SYSTEM - FIXED VERSION")
        log.info("=" * 70)
        log.info("TRADER ROLE: Discretionary reaction trader")
        log.info("SPECIALTY: CONFIRMED rejection entries (failure + strength shift)")
        log.info("CORRECTED: NO early entries - Only confirmed rejections")
        log.info("PHILOSOPHY: الرفض يخلق الفكرة، الفشل يؤكدها، تحول القوة هو الدخول")
        log.info(f"SCAN INTERVAL: {SCAN_INTERVAL} seconds")
        log.info("TIME FRAMES: 3M (failure detection) + 1M (trigger/entry)")
        log.info("NO ENTRY FILTERS: 6 filters to prevent early entries")
        log.info("DEDUPLICATION: ONE TRADE PER SYMBOL")
        log.info("=" * 70)
        
        # Initialize database
        await self._init_database()
        
        # Initialize exchange
        await self._init_exchange()
        
        # Send startup message
        await self._send_startup_message()
    
    async def _init_database(self):
        """Initialize database"""
        try:
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            self.db = await aiosqlite.connect(DB_PATH)
            
            # Confirmed signals table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS confirmed_signals (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                
                wave_length TEXT NOT NULL,
                wave_maturity REAL NOT NULL,
                structure_type TEXT NOT NULL,
                
                candle_speed REAL NOT NULL,
                strength_score REAL NOT NULL,
                volume_participation REAL NOT NULL,
                
                zone_type TEXT NOT NULL,
                failure_candle_type TEXT NOT NULL,
                trigger_candle_type TEXT NOT NULL,
                confirmation_score REAL NOT NULL,
                
                rsi_at_entry REAL NOT NULL,
                entry_reason TEXT NOT NULL,
                risk_reward REAL NOT NULL,
                expected_move REAL NOT NULL,
                
                failure_timeframe TEXT NOT NULL,
                trigger_timeframe TEXT NOT NULL,
                
                conditions_met TEXT,
                passed_no_entry_filters BOOLEAN,
                early_entry_prevented BOOLEAN,
                
                status TEXT DEFAULT 'PENDING',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                triggered_at TIMESTAMP,
                trigger_price REAL,
                
                closed_at TIMESTAMP,
                close_price REAL,
                pnl_percent REAL,
                close_reason TEXT
            )
            """)
            
            await self.db.commit()
            log.info("✅ Database initialized")
            
        except Exception as e:
            log.error(f"Database error: {e}")
            raise
    
    async def _init_exchange(self):
        """Initialize exchange connection"""
        try:
            self.exchange = ccxt.okx({
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
                "timeout": 20000,
                "rateLimit": 50
            })
            
            ticker = await self.exchange.fetch_ticker("BTC/USDT")
            log.info(f"✅ Exchange connected. BTC: ${ticker['last']:.2f}")
            
        except Exception as e:
            log.error(f"Exchange error: {e}")
            raise
    
    async def _send_startup_message(self):
        """Send corrected startup message"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning("⚠️ Telegram credentials not set.")
            return
        
        try:
            message = f"""
🎯 <b>CONFIRMED REJECTION SYSTEM - FIXED VERSION</b>

<b>🧠 الفلسفة المصححة (سطر واحد):</b>
الرفض يخلق الفكرة
الفشل يؤكدها
وتحول القوة هو الدخول

<b>📊 الإطار المصحح:</b>
1️⃣ <b>الطول الموجي</b> → السياق فقط
2️⃣ <b>القوة والفوليوم</b> → قرار الدخول
3️⃣ <b>مناطق الرفض</b> → الفكرة الأولية فقط

<b>🔒 نظام الدخول المصحح (الجديد):</b>

<b>📌 تأكيد الدخول (Entry Confirmation):</b>
لا يتم الدخول إلا بعد تحقق الشرطين:

1. <b>شمعة رفض (Failure Candle) - على 3M:</b>
   • ذيل واضح عند منطقة رفض
   • إغلاق داخل النطاق أو خلف EMA
   • ضعف فوليوم في اتجاه الاختراق

2. <b>شمعة تحول قوة (Trigger Candle) - على 1M:</b>
   • أول شمعة قوية بالاتجاه المعاكس
   • فوليوم أعلى من شمعة الرفض
   • إغلاق قريب من القمة (شراء) أو القاع (بيع)

<b>⚡ توقيت الدخول:</b>
• الدخول عند <b>إغلاق شمعة 1M المؤكدة</b>
• <b>ليس</b> عند أول شمعة رفض

<b>🚫 مرشحات (NO ENTRY) - لمنع الدخول المبكر:</b>
1. شمعة التحول ضعيفة
2. بدون تأكيد فوليوم
3. الشمعة التالية ضعيفة
4. الضغط ما زال موجوداً
5. شمعة الرفض غير واضحة
6. عدم وجود نمط فشل واضح

<b>🛡️ نظام التكرار:</b>
• <b>صفقة واحدة لكل عملة فقط</b>
• إشارات جديدة فقط بعد إغلاق الصفقة السابقة

<b>🎯 عقلية التاجر المصححة:</b>
• الدخول بعد تأكيد فشل الحركة
• الدخول بعد تحول القوة وليس عند أول رفض
• راحة مع الانتظار
• نقبل الخسائر المبكرة
• نصطاد التوسع المؤكد

#متداول_تفاعلي #تخصص_الرفض_المؤكد #لا_دخول_مبكر
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info("✅ Startup message sent to Telegram")
                
        except Exception as e:
            log.error(f"Telegram startup error: {e}")
    
    async def fetch_timeframe_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """Fetch data for all timeframes"""
        data = {}
        
        for tf_name, tf in TIMEFRAMES.items():
            try:
                if tf_name == "1H":
                    limit = 100
                elif tf_name == "15M":
                    limit = 80
                else:
                    limit = 50
                
                ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
                
                if ohlcv and len(ohlcv) >= 20:
                    df = pd.DataFrame(
                        ohlcv,
                        columns=["timestamp", "open", "high", "low", "close", "volume"]
                    )
                    
                    for col in ["open", "high", "low", "close", "volume"]:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    df = df.dropna()
                    
                    if len(df) >= 15:
                        data[tf_name] = df
                    
            except Exception as e:
                log.debug(f"{symbol} {tf_name}: {str(e)[:50]}")
                continue
        
        return data
    
    async def get_active_pairs(self) -> List[Tuple[str, float]]:
        """Get active trading pairs"""
        try:
            tickers = await self.exchange.fetch_tickers()
            active_pairs = []
            
            for symbol, ticker in tickers.items():
                if symbol.endswith('/USDT'):
                    volume = ticker.get('quoteVolume', 0)
                    
                    if volume >= MIN_VOLUME_USD:
                        bid = ticker.get('bid', 0)
                        ask = ticker.get('ask', 0)
                        
                        if bid > 0 and ask > 0:
                            spread = (ask - bid) / bid * 100
                            if spread < 0.1:
                                active_pairs.append((symbol, volume))
            
            active_pairs.sort(key=lambda x: x[1], reverse=True)
            return active_pairs[:TOP_N_VOLUME]
            
        except Exception as e:
            log.error(f"Error getting pairs: {e}")
            return []
    
    async def save_signal(self, signal: ConfirmedRejectionSignal) -> bool:
        """Save signal to database"""
        try:
            await self.db.execute("""
                INSERT INTO confirmed_signals (
                    id, symbol, side, entry_price, stop_loss, take_profit,
                    wave_length, wave_maturity, structure_type,
                    candle_speed, strength_score, volume_participation,
                    zone_type, failure_candle_type, trigger_candle_type, confirmation_score,
                    rsi_at_entry, entry_reason, risk_reward, expected_move,
                    failure_timeframe, trigger_timeframe,
                    conditions_met, passed_no_entry_filters, early_entry_prevented
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.side,
                signal.entry_price,
                signal.stop_loss,
                signal.take_profit,
                signal.wave_context.wave_length,
                signal.wave_context.wave_maturity,
                signal.wave_context.structure_type,
                signal.market_strength.candle_speed,
                signal.market_strength.strength_score,
                signal.market_strength.volume_participation,
                signal.rejection_zone.zone_type,
                signal.confirmed_rejection.failure_candle.candle_type,
                signal.confirmed_rejection.trigger_candle.candle_type,
                signal.confirmed_rejection.confirmation_score,
                signal.confirmed_rejection.trigger_candle.rsi_slope_change,
                signal.entry_reason,
                signal.risk_reward,
                signal.expected_move_pct,
                signal.failure_timeframe,
                signal.trigger_timeframe,
                json.dumps(signal.confirmed_rejection.conditions_met),
                signal.passed_no_entry_filters,
                signal.early_entry_prevented
            ))
            
            await self.db.commit()
            log.info(f"✅ Confirmed signal saved: {signal.symbol}")
            return True
            
        except Exception as e:
            log.error(f"Error saving signal: {e}")
            return False
    
    async def format_signal_message(self, signal: ConfirmedRejectionSignal) -> str:
        """Format signal for Telegram"""
        side_emoji = "🟢" if signal.side == "LONG" else "🔴"
        side_text = "شراء" if signal.side == "LONG" else "بيع"
        
        zone_translation = {
            "EMA_SUPPORT": "دعم المتوسط المتحرك",
            "EMA_RESISTANCE": "مقاومة المتوسط المتحرك",
            "RANGE_LOW": "قاع النطاق",
            "RANGE_HIGH": "سقف النطاق",
            "FAILED_BREAKDOWN": "اختراق فاشل للأسفل",
            "FAILED_BREAKOUT": "اختراق فاشل للأعلى"
        }
        
        zone_text = zone_translation.get(signal.rejection_zone.zone_type, signal.rejection_zone.zone_type)
        
        risk_pct = abs(signal.entry_price - signal.stop_loss) / signal.entry_price * 100
        
        message = f"""
{side_emoji} <b>إشارة رفض مؤكدة</b> ✅

<b>{signal.symbol}</b> | {side_text}

<b>📊 تأكيد الرفض:</b>
• شمعة الفشل: {signal.confirmed_rejection.failure_candle.candle_type}
• شمعة التحول: {signal.confirmed_rejection.trigger_candle.candle_type}
• درجة التأكيد: {signal.confirmed_rejection.confirmation_score:.1%}

<b>🎯 منطقة الرفض:</b>
• النوع: {zone_text}
• قوة المنطقة: {signal.rejection_zone.strength:.1%}
• RSI عند الدخول: {signal.confirmed_rejection.trigger_candle.rsi_slope_change:.1f}

<b>💪 تحليل القوة:</b>
• درجة القوة: {signal.market_strength.strength_score:.1%}
• سرعة الشموع: {signal.market_strength.candle_speed:.1%}
• مشاركة الفوليوم: {signal.market_strength.volume_participation:.1%}

<b>🔧 التنفيذ:</b>
• سعر الدخول: <code>{signal.entry_price:.6f}</code>
• وقف الخسارة: <code>{signal.stop_loss:.6f}</code> ({risk_pct:.2f}%)
• هدف الربح: <code>{signal.take_profit:.6f}</code> ({signal.expected_move_pct:.1f}%)
• نسبة الربح/المخاطرة: {signal.risk_reward:.1f}:1

<b>🛡️ نظام التأكيد:</b>
• تم الدخول بعد <b>تأكيد الرفض</b>
• شمعة الفشل (3M) + شمعة التحول (1M)
• مرت جميع مرشحات NO ENTRY

<b>⚠️ ملاحظة التاجر:</b>
الدخول بعد تأكيد فشل الحركة
الدخول بعد تحول القوة وليس عند أول رفض
نقبل الخسائر - نصطاد التوسع المؤكد

#{side_text} #رفض_مؤكد #{"دعم" if signal.side == "LONG" else "مقاومة"} #صفقة_واحدة
"""
        return message
    
    async def send_telegram_alert(self, signal: ConfirmedRejectionSignal):
        """Send Telegram alert"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning(f"⚠️ Telegram credentials missing. Skipping alert for {signal.symbol}")
            return
        
        try:
            message = await self.format_signal_message(signal)
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info(f"📤 Telegram confirmed alert sent: {signal.symbol}")
            
        except Exception as e:
            log.error(f"Telegram error: {e}")
    
    async def send_trade_trigger_notification(self, symbol: str, side: str, entry_price: float):
        """Send trigger notification"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            side_emoji = "🟢" if side == "LONG" else "🔴"
            side_text = "شراء" if side == "LONG" else "بيع"
            
            message = f"""
{side_emoji} <b>تم تنفيذ صفقة الرفض المؤكدة</b> ✅

<b>{symbol}</b> | {side_text}

<b>🎯 تم الدخول بعد تأكيد الرفض:</b>
<code>{entry_price:.6f}</code>

<b>🧠 عقلية التاجر:</b>
• دخول بعد تأكيد فشل الحركة
• دخول بعد تحول القوة المؤكد
• راحة مع الانتظار
• صيد للتوسع القادم المؤكد

<b>🛡️ نظام التكرار:</b>
❌ <b>ممنوع</b> إرسال إشارات جديدة لـ {symbol}
✅ مسموح بإشارات جديدة بعد إغلاق هذه الصفقة

#{side_text} #تنفيذ_رفض_مؤكد #متابعة #لا_إشارات_جديدة
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
            
            log.info(f"{side_emoji} Confirmed rejection triggered: {symbol} {side}")
            
        except Exception as e:
            log.error(f"Trigger notification error: {e}")
    
    async def send_trade_close_notification(self, symbol: str, side: str, pnl_percent: float,
                                           close_reason: str, entry_price: float,
                                           close_price: float, risk_reward: float):
        """Send close notification"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            if close_reason == "TP_HIT":
                emoji = "✅"
                result_text = "هدف الربح"
                result_emoji = "🎯"
                color = "🟢"
                pnl_emoji = "💰"
            else:
                emoji = "❌"
                result_text = "وقف الخسارة"
                result_emoji = "🛑"
                color = "🔴"
                pnl_emoji = "💸"
            
            side_text = "شراء" if side == "LONG" else "بيع"
            pnl_formatted = f"+{pnl_percent:.2f}%" if pnl_percent > 0 else f"{pnl_percent:.2f}%"
            
            if close_reason == "TP_HIT":
                mindset = "التوسع المؤكد تم اصطياده ✅ الدخول بعد التأكيد حقق الربح"
            else:
                mindset = "الخسارة مقبولة ❌ الانتظار للتأكيد القادم"
            
            message = f"""
{emoji} <b>تم إغلاق صفقة الرفض المؤكدة</b> {result_emoji}

<b>{symbol}</b> | {side_text}

{color} <b>النتيجة: {result_text}</b>
{pnl_emoji} <b>النسبة: {pnl_formatted}</b>

<b>📊 تفاصيل التنفيذ:</b>
• نوع الدخول: {side_text} (بعد تأكيد الرفض)
• سعر الدخول: <code>{entry_price:.6f}</code>
• سعر الإغلاق: <code>{close_price:.6f}</code>
• نسبة الربح/الخسارة: <b>{pnl_formatted}</b>

<b>🧠 عقلية التاجر:</b>
{mindset}
نقبل الخسائر - نصطاد التوسع المؤكد
كل تأكيد هو فرصة جديدة

<b>🛡️ نظام التكرار:</b>
✅ <b>مسموح الآن</b> بإرسال إشارات جديدة لـ {symbol}

#{side_text} #إغلاق_رفض_مؤكد #{"ربح" if close_reason == "TP_HIT" else "خسارة"} #مسموح_إشارات_جديدة
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
            
            log.info(f"{emoji} Confirmed trade closed: {symbol} {side} {pnl_formatted}")
            
        except Exception as e:
            log.error(f"Close notification error: {e}")
    
    async def monitor_positions(self):
        """Monitor and close positions"""
        log.info("👀 Starting position monitoring for confirmed rejections...")
        
        while True:
            try:
                async with self.db.execute("""
                    SELECT id, symbol, side, entry_price, stop_loss, take_profit, status
                    FROM confirmed_signals 
                    WHERE status IN ('PENDING', 'TRIGGERED')
                """) as cursor:
                    positions = await cursor.fetchall()
                
                if positions:
                    log.debug(f"📊 Monitoring {len(positions)} open positions")
                
                for pos_id, symbol, side, entry, sl, tp, status in positions:
                    try:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        # Check if price reached entry for PENDING
                        if status == 'PENDING':
                            if abs(current_price - entry) / entry <= 0.005:
                                await self.db.execute("""
                                    UPDATE confirmed_signals SET 
                                        status = 'TRIGGERED',
                                        triggered_at = CURRENT_TIMESTAMP,
                                        trigger_price = ?
                                    WHERE id = ?
                                """, (current_price, pos_id))
                                
                                await self.db.commit()
                                self.scanner.deduplicator.update_signal_status(pos_id, "TRIGGERED")
                                await self.send_trade_trigger_notification(symbol, side, current_price)
                                log.info(f"✅ Confirmed position triggered: {symbol}")
                                continue
                        
                        # Check SL/TP
                        pnl_percent = 0
                        close_reason = None
                        
                        if side == "LONG":
                            if current_price <= sl:
                                close_reason = "SL_HIT"
                                pnl_percent = ((current_price - entry) / entry) * 100
                            elif current_price >= tp:
                                close_reason = "TP_HIT"
                                pnl_percent = ((current_price - entry) / entry) * 100
                        
                        else:
                            if current_price >= sl:
                                close_reason = "SL_HIT"
                                pnl_percent = ((entry - current_price) / entry) * 100
                            elif current_price <= tp:
                                close_reason = "TP_HIT"
                                pnl_percent = ((entry - current_price) / entry) * 100
                        
                        if close_reason:
                            async with self.db.execute("""
                                SELECT risk_reward FROM confirmed_signals WHERE id = ?
                            """, (pos_id,)) as cursor:
                                row = await cursor.fetchone()
                                risk_reward = row[0] if row else 0
                            
                            await self.db.execute("""
                                UPDATE confirmed_signals SET 
                                    status = 'CLOSED',
                                    closed_at = CURRENT_TIMESTAMP,
                                    close_price = ?,
                                    pnl_percent = ?,
                                    close_reason = ?
                                WHERE id = ?
                            """, (current_price, pnl_percent, close_reason, pos_id))
                            
                            await self.db.commit()
                            self.scanner.deduplicator.update_signal_status(pos_id, "CLOSED")
                            self.scanner.active_signal_ids.discard(pos_id)
                            
                            await self.send_trade_close_notification(
                                symbol=symbol,
                                side=side,
                                pnl_percent=pnl_percent,
                                close_reason=close_reason,
                                entry_price=entry,
                                close_price=current_price,
                                risk_reward=risk_reward
                            )
                    
                    except Exception as e:
                        log.error(f"Monitor error for {symbol}: {e}")
                        continue
                
                # Clean up
                if int(time.time()) % 300 < 2:
                    self.scanner.deduplicator.remove_closed_signals()
                
                await asyncio.sleep(2)
                
            except Exception as e:
                log.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(5)
    
    async def confirmed_rejection_scanning(self):
        """Main scanning loop for confirmed rejections"""
        log.info("🚀 Starting confirmed rejection scanning...")
        
        while True:
            try:
                self.scan_cycle += 1
                start_time = time.time()
                
                log.info(f"🔄 Scan cycle #{self.scan_cycle} (Confirmed rejection hunting)")
                
                # Get active pairs
                pairs = await self.get_active_pairs()
                
                if not pairs:
                    log.warning("No active pairs found")
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                log.info(f"Scanning {len(pairs)} active pairs for confirmed rejections")
                
                signals_found = 0
                pairs_processed = 0
                
                for symbol, volume in pairs:
                    try:
                        multi_tf_data = await self.fetch_timeframe_data(symbol)
                        
                        required_tfs = ["3M", "1M"]
                        has_all_data = all(tf in multi_tf_data for tf in required_tfs)
                        
                        if not has_all_data:
                            continue
                        
                        signal = self.scanner.generate_confirmed_rejection_signal(multi_tf_data, symbol)
                        
                        if signal:
                            saved = await self.save_signal(signal)
                            
                            if saved:
                                await self.send_telegram_alert(signal)
                                signals_found += 1
                        
                        pairs_processed += 1
                        await asyncio.sleep(0.01)
                        
                    except Exception as e:
                        log.debug(f"Pair error {symbol}: {str(e)[:50]}")
                        continue
                
                # Update stats
                self.scanner.daily_stats["pairs_scanned"] += pairs_processed
                active_count = len(self.scanner.deduplicator.active_signals)
                stats = self.scanner.get_daily_stats()
                
                log.info(f"📊 Confirmed stats: Found {signals_found}, Active: {active_count}")
                log.info(f"   Early prevented: {stats.get('early_entries_prevented', 0)}, "
                        f"No trigger: {stats.get('no_trigger_candle', 0)}, "
                        f"No strength: {stats.get('no_strength_shift', 0)}")
                
                scan_duration = time.time() - start_time
                log.info(f"Scan #{self.scan_cycle}: {signals_found} confirmed rejections in {scan_duration:.2f}s")
                
                if self.scan_cycle % 20 == 0:
                    log.info(f"📈 Detailed stats: {stats}")
                
                wait_time = max(0.1, SCAN_INTERVAL - scan_duration)
                log.info(f"Next confirmed hunt in {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                log.error(f"Scanning loop error: {e}")
                await asyncio.sleep(10)
    
    async def run(self):
        """Run the scanner"""
        try:
            await self.initialize()
            
            # Run both loops
            await asyncio.gather(
                self.confirmed_rejection_scanning(),
                self.monitor_positions()
            )
            
        except KeyboardInterrupt:
            log.info("Confirmed rejection scanner stopped by user")
            await self.send_final_stats()
            
        except Exception as e:
            log.error(f"Scanner crashed: {e}")
            
        finally:
            await self.cleanup()
    
    async def send_final_stats(self):
        """Send final statistics"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            stats = self.scanner.get_daily_stats()
            active_count = len(self.scanner.deduplicator.active_signals)
            
            message = f"""
🛑 <b>تم إيقاف ماسح الرفض المؤكد</b>

<b>📊 إحصائيات اليوم:</b>
• عمليات المسح: {self.scan_cycle}
• الأزواج الممسوحة: {stats['pairs_scanned']}
• إشارات مؤكدة: {stats['confirmed_signals']}
• رفض شراء: {stats.get('confirmed_signals', 0) // 2}
• رفض بيع: {stats.get('confirmed_signals', 0) // 2}

<b>🚫 منع الدخول المبكر:</b>
• دخول مبكر مُنع: {stats.get('early_entries_prevented', 0)}
• بدون شمعة تحول: {stats.get('no_trigger_candle', 0)}
• بدون تحول قوة: {stats.get('no_strength_shift', 0)}

<b>⚡ الصفقات النشطة:</b>
• حالياً: {active_count} صفقة نشطة

<b>🧠 الفلسفة المحققة:</b>
الرفض يخلق الفكرة
الفشل يؤكدها
وتحول القوة هو الدخول

تم الالتزام بـ:
• الدخول بعد تأكيد الرفض فقط
• صفقة واحدة لكل عملة
• مرور جميع مرشحات NO ENTRY
• قبول الخسائر
• صيد التوسع المؤكد

#إحصائيات_الرفض_المؤكد #متداول_تفاعلي #لا_دخول_مبكر
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info("✅ Final stats sent to Telegram")
                
        except Exception as e:
            log.error(f"Final stats error: {e}")
    
    async def cleanup(self):
        """Cleanup resources"""
        try:
            if self.exchange:
                await self.exchange.close()
                log.info("Exchange closed")
            
            if self.db:
                await self.db.close()
                log.info("Database closed")
                
        except Exception as e:
            log.error(f"Cleanup error: {e}")

# ================ SIMPLE HTTP SERVER ================
async def start_http_server(scanner, port=8000):
    """Start HTTP server for monitoring"""
    async def handle_request(reader, writer):
        try:
            request = await reader.read(1024)
            lines = request.decode().split('\r\n')
            
            if not lines:
                writer.write(b'HTTP/1.1 400 Bad Request\r\n\r\n')
                await writer.drain()
                writer.close()
                return
            
            request_line = lines[0]
            method, path, _ = request_line.split(' ')
            
            response = ""
            
            if path == '/':
                stats = scanner.scanner.get_daily_stats()
                active_count = len(scanner.scanner.deduplicator.active_signals)
                
                response = json.dumps({
                    "status": "running",
                    "scanner": "Confirmed Rejection System",
                    "scan_cycle": scanner.scan_cycle,
                    "active_trades": active_count,
                    "daily_stats": stats,
                    "philosophy": "Rejection creates idea, Failure confirms it, Strength shift is entry",
                    "entry_rule": "Trade ONLY after confirmed rejection (failure + trigger)",
                    "prevention": f"Early entries prevented: {stats.get('early_entries_prevented', 0)}"
                }, indent=2)
            
            elif path == '/stats':
                response = json.dumps(scanner.scanner.get_daily_stats(), indent=2)
            
            elif path == '/philosophy':
                response = json.dumps({
                    "core_philosophy": "الرفض يخلق الفكرة، الفشل يؤكدها، تحول القوة هو الدخول",
                    "trader_mindset": "Reaction trader, confirmed rejection specialist",
                    "entry_system": "Two-candle confirmation (failure on 3M + trigger on 1M)",
                    "no_entry_rules": "6 filters to prevent early entries",
                    "risk_management": "Asymmetric payoff with minimum 1:2 R:R",
                    "frequency": "High frequency scanning for confirmed rejections"
                }, indent=2)
            
            else:
                response = json.dumps({"error": "Endpoint not found"})
            
            writer.write(f'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{response}'.encode())
            await writer.drain()
            writer.close()
            
        except Exception as e:
            error_response = json.dumps({"error": str(e)})
            writer.write(f'HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\n\r\n{error_response}'.encode())
            await writer.drain()
            writer.close()
    
    server = await asyncio.start_server(handle_request, '0.0.0.0', port)
    log.info(f"🌐 HTTP server started on port {port}")
    
    async with server:
        await server.serve_forever()

# ================ MAIN ================
async def main():
    """Main function"""
    scanner = ConfirmedRejectionScannerSystem()
    http_task = asyncio.create_task(start_http_server(scanner))
    await scanner.run()

if __name__ == "__main__":
    asyncio.run(main())