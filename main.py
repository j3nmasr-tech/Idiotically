#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 REJECTION-BASED HIGH-FREQUENCY SCANNER
Professional discretionary trading system
Wave-length awareness + Strength analysis + Rejection entries
TRADER MINDSET: Reaction-based, rejection specialist
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

# ================ HIGH-FREQUENCY CONFIG ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/rejection_scanner.db"

# Ultra high-frequency scanning - REACTION TRADING
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 5))   # 5 seconds - ULTRA FAST FOR REJECTIONS
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 100))   # Scan many pairs
MIN_VOLUME_USD = 500000  # $500K minimum - more opportunities

# Trading parameters - DYNAMIC SL/TP BASED ON MARKET STRUCTURE
MIN_RISK_REWARD = 1.0        # Minimum 1.5:1 risk/reward for dynamic SL/TP

# Rejection scanning
REJECTION_CONFIG = {
    "rsi_long_zone": (40, 50),      # RSI 40-50 for LONG entries
    "rsi_short_zone": (50, 60),     # RSI 50-60 for SHORT entries
    "ema_distance_threshold": 0.5,  # 0.5% from EMA for rejection
    "min_rejection_strength": 0.2,  # Minimum rejection strength score
}

# Timeframes for REACTION TRADING
TIMEFRAMES = {
    "1H": "1h",      # Wave length context + SL/TP ANALYSIS
    "30M": "30m",    # SL/TP ANALYSIS
    "15M": "15m",    # Strength and structure + SL/TP ANALYSIS
    "5M": "5m",      # Primary rejection analysis
    "3M": "3m",      # Fast trigger (MAIN)
    "1M": "1m"       # Entry timing (ULTRA FAST)
}

# EMA periods for rejection detection
EMA_PERIODS = {
    "fast": 9,
    "medium": 21,
    "slow": 50
}

# RSI settings for rejection zones
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# ================ DATA STRUCTURES ================
@dataclass
class WaveContext:
    """Wave length and maturity context - NO WAVE COUNTING"""
    wave_length: str           # SHORT, MEDIUM, EXTENDED
    wave_maturity: float       # 0-1 (0=early, 1=exhausted)
    expansion_speed: float     # 0-1 (slow to fast)
    structure_type: str        # IMPULSIVE, CORRECTIVE, COMPRESSION
    context_side: str          # BULLISH_CONTEXT, BEARISH_CONTEXT, NEUTRAL

@dataclass
class MarketStrength:
    """Market strength analysis"""
    candle_speed: float        # 0-1 (slow to fast)
    distance_ratio: float      # Distance traveled / time
    ema_angle: float           # EMA slope angle in degrees
    volume_participation: float # 0-1 volume participation
    strength_score: float      # 0-1 overall strength
    
    # Interpretation flags
    is_continuation: bool      # Strong move + strong volume
    is_rejection_setup: bool   # Strong move + weak volume
    is_absorption: bool        # Weak move + rising volume
    is_compression: bool       # Flat price + compression

@dataclass
class RejectionZone:
    """Key rejection area analysis"""
    zone_type: str             # EMA_SUPPORT, EMA_RESISTANCE, RANGE_LOW, RANGE_HIGH, 
                               # FAILED_BREAKDOWN, FAILED_BREAKOUT, DEMAND, SUPPLY
    price_level: float
    strength: float            # 0-1 rejection strength
    volume_confirmation: bool  # Volume spike at rejection
    rsi_position: str          # IN_ZONE, OVEREXTENDED, NEUTRAL
    is_active: bool           # Currently being rejected

@dataclass
class RejectionSignal:
    """Rejection-based trade signal"""
    signal_id: str
    symbol: str
    side: str                  # LONG, SHORT
    
    # Price levels - DYNAMIC SL/TP
    entry_price: float
    stop_loss: float
    take_profit: float
    
    # TP Method Information - NEW FIELD
    tp_method: str = "UNKNOWN"           # LIQUIDITY_POOL, MINIMUM_RR
    tp_timeframe: str = "UNKNOWN"        # 15M, 30M, 1H, DYNAMIC
    sl_timeframe: str = "UNKNOWN"        # 15M, 30M, 1H
    sl_source: str = "UNKNOWN"           # SWING_LOW, SWING_HIGH
    
    # Analysis context
    wave_context: WaveContext
    market_strength: MarketStrength
    rejection_zone: RejectionZone
    
    # Entry triggers
    rejection_type: str        # EMA_REJECTION, RANGE_REJECTION, FAILED_BREAKOUT
    trigger_candle: str        # REJECTION_CANDLE, WICK_CONFIRMATION, MOMENTUM_SHIFT
    rsi_at_entry: float
    
    # Metrics
    rejection_strength: float  # 0-1 how strong the rejection is
    risk_reward: float         # Dynamic based on market structure
    expected_move_pct: float   # Dynamic based on market structure
    
    # Timing
    timeframe_used: str        # Which TF triggered entry
    signal_timestamp: float
    conditions_met: List[str]  # Which rejection conditions triggered
    
    # Status tracking - SIMPLIFIED
    status: str = "ACTIVE"     # ACTIVE, CLOSED (removed PENDING/TRIGGERED states)

# ================ PROFESSIONAL LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("rejection_scanner")

# ================ CORE REJECTION ENGINE ================
class RejectionBasedScanner:
    """High-frequency rejection scanner - REACTION TRADING"""
    
    class SignalDeduplicator:
        """Prevents duplicate signal generation - TRADE-BASED"""
        
        def __init__(self):
            self.active_signals = {}  # symbol: signal_id (only one active per symbol)
            self.signal_status = {}   # signal_id: {"symbol", "side", "price", "status"}
        
        def should_generate_signal(self, symbol: str, side: str, price: float) -> bool:
            """Check if we should generate a new signal - TRADE-BASED"""
            
            # Check if symbol already has an active signal
            if symbol in self.active_signals:
                signal_id = self.active_signals[symbol]
                
                # Check signal status
                if signal_id in self.signal_status:
                    status = self.signal_status[signal_id].get("status", "UNKNOWN")
                    
                    # Only allow new signal if previous is CLOSED
                    if status != "CLOSED":
                        log.debug(f"{symbol}: Active {side} signal exists (status: {status})")
                        return False
            
            return True
        
        def register_signal(self, signal: RejectionSignal):
            """Register a new signal"""
            symbol = signal.symbol
            
            # Remove any previous active signal for this symbol
            if symbol in self.active_signals:
                old_signal_id = self.active_signals[symbol]
                if old_signal_id in self.signal_status:
                    del self.signal_status[old_signal_id]
            
            # Register new signal as ACTIVE (no PENDING state)
            self.active_signals[symbol] = signal.signal_id
            self.signal_status[signal.signal_id] = {
                "symbol": symbol,
                "side": signal.side,
                "price": signal.entry_price,
                "status": "ACTIVE",
                "timestamp": signal.signal_timestamp
            }
            
            log.info(f"Registered ACTIVE rejection signal {signal.signal_id[:8]} for {symbol}")
        
        def update_signal_status(self, signal_id: str, status: str):
            """Update signal status (ACTIVE → CLOSED)"""
            if signal_id in self.signal_status:
                self.signal_status[signal_id]["status"] = status
                log.info(f"Signal {signal_id[:8]} status updated to {status}")
                
                # If CLOSED, mark as ready for new signals
                if status == "CLOSED":
                    symbol = self.signal_status[signal_id]["symbol"]
                    log.info(f"✅ Signal {signal_id[:8]} for {symbol} CLOSED - Ready for new rejections")
                    
                    # Immediately remove from active signals
                    if symbol in self.active_signals and self.active_signals[symbol] == signal_id:
                        del self.active_signals[symbol]
        
        def remove_closed_signals(self):
            """Clean up closed signals to free memory"""
            current_time = time.time()
            closed_signal_ids = []
            
            for signal_id, data in list(self.signal_status.items()):
                if data.get("status") == "CLOSED":
                    # Remove if closed more than 30 minutes ago
                    if current_time - data.get("timestamp", 0) > 1800:
                        closed_signal_ids.append(signal_id)
            
            for signal_id in closed_signal_ids:
                if signal_id in self.signal_status:
                    del self.signal_status[signal_id]
                    log.debug(f"Cleaned up closed signal {signal_id[:8]}")
    
    def __init__(self):
        self.daily_stats = {
            "rejections_found": 0,
            "long_rejections": 0,
            "short_rejections": 0,
            "pairs_scanned": 0,
            "rejections_filtered": 0,
            "no_strength": 0,
            "no_rejection_zone": 0,
            "tp_liquidity_pool": 0,
            "tp_minimum_rr": 0
        }
        self.deduplicator = self.SignalDeduplicator()
        self.active_signal_ids = set()
    
    # ========== WAVE LENGTH ANALYSIS (CONTEXT ONLY) ==========
    
    def analyze_wave_context(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> WaveContext:
        """
        Analyze wave length and maturity - NO WAVE COUNTING
        Determine context only
        """
        try:
            if df_1h is None or df_15m is None:
                return self._get_default_wave_context()
            
            if len(df_1h) < 20 or len(df_15m) < 30:
                return self._get_default_wave_context()
            
            # 1. Analyze wave length on 1H
            wave_length, wave_maturity = self._analyze_wave_length(df_1h)
            
            # 2. Analyze expansion speed on 15M
            expansion_speed = self._analyze_expansion_speed(df_15m)
            
            # 3. Determine structure type
            structure_type = self._determine_structure(df_15m)
            
            # 4. Determine context side (not trade direction, just context)
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
            
            # Get recent price move
            recent_prices = df['close'].values[-30:]
            
            # Calculate total move length
            total_move = abs(recent_prices[-1] - recent_prices[0])
            avg_candle_size = np.mean(np.abs(np.diff(recent_prices)))
            
            if avg_candle_size == 0:
                return "MEDIUM", 0.5
            
            # Wave length classification
            move_ratio = total_move / avg_candle_size
            
            if move_ratio < 15:
                wave_length = "SHORT"
            elif move_ratio < 30:
                wave_length = "MEDIUM"
            else:
                wave_length = "EXTENDED"
            
            # Wave maturity (0=early, 1=exhausted)
            # Based on distance from moving average and volatility
            ma_20 = np.mean(recent_prices[-20:])
            current_price = recent_prices[-1]
            volatility = np.std(recent_prices[-20:])
            
            if volatility > 0:
                distance_pct = abs(current_price - ma_20) / ma_20 * 100
                volatility_pct = volatility / ma_20 * 100
                
                # Normalize maturity
                wave_maturity = min(distance_pct / (volatility_pct * 2), 1.0)
            else:
                wave_maturity = 0.5
            
            return wave_length, wave_maturity
            
        except Exception as e:
            return "MEDIUM", 0.5
    
    def _analyze_expansion_speed(self, df: pd.DataFrame) -> float:
        """Analyze expansion speed (0=slow, 1=fast)"""
        try:
            if len(df) < 10:
                return 0.5
            
            # Calculate candle speeds (absolute percentage changes)
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
            max_speed = np.max(candle_speeds)
            
            # Normalize to 0-1
            expansion_speed = min(avg_speed / 5.0, 1.0)  # 5% avg range = max speed
            
            return expansion_speed
            
        except Exception as e:
            return 0.5
    
    def _determine_structure(self, df: pd.DataFrame) -> str:
        """Determine market structure type"""
        try:
            if len(df) < 20:
                return "COMPRESSION"
            
            prices = df['close'].values[-20:]
            highs = df['high'].values[-20:]
            lows = df['low'].values[-20:]
            
            # Check for impulsive moves
            price_change = prices[-1] - prices[0]
            price_change_pct = abs(price_change) / prices[0] * 100
            
            # Check for compression
            range_ratio = (np.max(highs) - np.min(lows)) / prices[0] * 100
            
            if price_change_pct > 3 and range_ratio > 5:
                return "IMPULSIVE"
            elif range_ratio < 2:
                return "COMPRESSION"
            else:
                return "CORRECTIVE"
                
        except Exception as e:
            return "COMPRESSION"
    
    def _determine_context_side(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> str:
        """Determine context side (not trade direction)"""
        try:
            # Use 1H for broader context
            if len(df_1h) < 10:
                return "NEUTRAL"
            
            # Simple slope analysis
            prices_1h = df_1h['close'].values[-10:]
            x = np.arange(len(prices_1h))
            slope_1h, _ = np.polyfit(x, prices_1h, 1)
            
            # Use 15M for recent bias
            prices_15m = df_15m['close'].values[-5:]
            slope_15m, _ = np.polyfit(np.arange(len(prices_15m)), prices_15m, 1)
            
            # Combine with weights
            total_slope = (slope_1h * 0.7 + slope_15m * 0.3)
            
            if total_slope > 0.001:
                return "BULLISH_CONTEXT"
            elif total_slope < -0.001:
                return "BEARISH_CONTEXT"
            else:
                return "NEUTRAL"
                
        except Exception as e:
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
        """
        Analyze market strength using:
        - Candle speed
        - Distance traveled
        - EMA angle
        - Volume participation
        """
        try:
            if df is None or len(df) < 20:
                return self._get_default_market_strength()
            
            # 1. Candle speed analysis
            candle_speed = self._calculate_candle_speed(df)
            
            # 2. Distance traveled vs time
            distance_ratio = self._calculate_distance_ratio(df)
            
            # 3. EMA angle analysis
            ema_angle = self._calculate_ema_angle(df)
            
            # 4. Volume participation
            volume_participation = self._calculate_volume_participation(df)
            
            # 5. Strength score
            strength_score = self._calculate_strength_score(
                candle_speed, distance_ratio, ema_angle, volume_participation
            )
            
            # 6. Interpret strength patterns
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
        """Calculate average candle speed"""
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
            # Normalize: 0.5% = 0.5, 1% = 0.75, 2% = 1.0
            return min(avg_speed / 2.0, 1.0)
            
        except Exception as e:
            return 0.5
    
    def _calculate_distance_ratio(self, df: pd.DataFrame) -> float:
        """Calculate distance traveled vs time ratio"""
        try:
            if len(df) < 10:
                return 0.5
            
            prices = df['close'].values[-10:]
            total_distance = abs(prices[-1] - prices[0])
            
            if prices[0] > 0:
                distance_pct = total_distance / prices[0] * 100
                # Normalize: 5% move in 10 candles = 1.0
                return min(distance_pct / 5.0, 1.0)
            
            return 0.5
            
        except Exception as e:
            return 0.5
    
    def _calculate_ema_angle(self, df: pd.DataFrame) -> float:
        """Calculate EMA angle/slope"""
        try:
            if len(df) < 20:
                return 0.0
            
            # Calculate fast EMA
            ema_fast = df['close'].ewm(span=EMA_PERIODS['fast'], adjust=False).mean()
            ema_values = ema_fast.values[-10:]
            
            if len(ema_values) < 5:
                return 0.0
            
            # Calculate slope
            x = np.arange(len(ema_values))
            slope, _ = np.polyfit(x, ema_values, 1)
            
            # Normalize slope to angle-like metric
            avg_price = np.mean(ema_values)
            if avg_price > 0:
                angle_metric = abs(slope / avg_price * 1000)  # Scale for sensitivity
                return min(angle_metric, 1.0)
            
            return 0.0
            
        except Exception as e:
            return 0.0
    
    def _calculate_volume_participation(self, df: pd.DataFrame) -> float:
        """Calculate volume participation ratio"""
        try:
            if len(df) < 20:
                return 0.5
            
            recent_volume = df['volume'].values[-5:].mean()
            avg_volume = df['volume'].values[-20:].mean()
            
            if avg_volume > 0:
                ratio = recent_volume / avg_volume
                # Normalize: 1.0 = average, 1.5 = 0.75, 2.0 = 1.0, 0.5 = 0.25
                if ratio >= 1.0:
                    return min((ratio - 1.0) * 2, 1.0)  # 1.0→0, 1.5→1.0
                else:
                    return max((ratio - 1.0) * 2, 0.0)  # 0.5→0, 1.0→0
            
            return 0.5
            
        except Exception as e:
            return 0.5
    
    def _calculate_strength_score(self, candle_speed: float, distance_ratio: float, 
                                 ema_angle: float, volume_participation: float) -> float:
        """Calculate overall strength score"""
        # Weighted average with volume being most important
        weights = [0.2, 0.2, 0.2, 0.4]  # candle_speed, distance, ema_angle, volume
        factors = [candle_speed, distance_ratio, ema_angle, volume_participation]
        
        return np.average(factors, weights=weights)
    
    def _interpret_strength_patterns(self, df: pd.DataFrame, candle_speed: float, 
                                    volume_participation: float) -> Tuple[bool, bool, bool, bool]:
        """Interpret strength patterns"""
        try:
            if len(df) < 10:
                return False, False, False, False
            
            # Get price action
            price_change = df['close'].iloc[-1] - df['close'].iloc[-5]
            price_change_pct = abs(price_change) / df['close'].iloc[-5] * 100
            
            # Determine patterns
            is_continuation = (candle_speed > 0.7 and volume_participation > 0.7 and 
                              price_change_pct > 1.0)
            
            is_rejection_setup = (candle_speed > 0.7 and volume_participation < 0.3 and 
                                 price_change_pct > 1.0)
            
            is_absorption = (candle_speed < 0.3 and volume_participation > 0.7)
            
            # Check for compression (low range, low volume)
            recent_high = df['high'].values[-5:].max()
            recent_low = df['low'].values[-5:].min()
            range_pct = (recent_high - recent_low) / recent_low * 100
            
            is_compression = (range_pct < 1.0 and volume_participation < 0.5)
            
            return is_continuation, is_rejection_setup, is_absorption, is_compression
            
        except Exception as e:
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
        """
        Find all active rejection zones
        """
        zones = []
        
        try:
            if df is None or len(df) < 20:
                return zones
            
            # 1. EMA rejection zones
            ema_zones = self._find_ema_rejection_zones(current_price, emas)
            zones.extend(ema_zones)
            
            # 2. Range rejection zones
            range_zones = self._find_range_rejection_zones(df, current_price)
            zones.extend(range_zones)
            
            # 3. Failed breakout/breakdown zones
            failed_zones = self._find_failed_break_zones(df, current_price)
            zones.extend(failed_zones)
            
            # 4. RSI position analysis for each zone
            for zone in zones:
                zone.rsi_position = self._analyze_rsi_position(rsi_value, zone.zone_type)
            
            # Filter to active zones only
            active_zones = [z for z in zones if z.is_active]
            
            return active_zones
            
        except Exception as e:
            log.error(f"Rejection zone error: {e}")
            return []
    
    def _find_ema_rejection_zones(self, current_price: float, emas: Dict[str, float]) -> List[RejectionZone]:
        """Find EMA rejection zones"""
        zones = []
        
        try:
            # Check each EMA for rejection
            for ema_name, ema_value in emas.items():
                if ema_value == 0:
                    continue
                
                distance_pct = abs(current_price - ema_value) / ema_value * 100
                
                # Check if price is near EMA (within threshold)
                if distance_pct <= REJECTION_CONFIG["ema_distance_threshold"]:
                    # Determine if it's support or resistance
                    if current_price > ema_value:
                        # Price above EMA - potential support
                        zone_type = "EMA_SUPPORT"
                        is_active = True
                    else:
                        # Price below EMA - potential resistance
                        zone_type = "EMA_RESISTANCE"
                        is_active = True
                    
                    # Calculate rejection strength based on EMA importance
                    if ema_name == "fast":
                        strength = 0.7
                    elif ema_name == "medium":
                        strength = 0.8
                    else:  # slow
                        strength = 0.9
                    
                    zones.append(RejectionZone(
                        zone_type=zone_type,
                        price_level=ema_value,
                        strength=strength,
                        volume_confirmation=False,  # Will be checked later
                        rsi_position="IN_ZONE",
                        is_active=is_active
                    ))
            
            return zones
            
        except Exception as e:
            return []
    
    def _find_range_rejection_zones(self, df: pd.DataFrame, current_price: float) -> List[RejectionZone]:
        """Find range high/low rejection zones"""
        zones = []
        
        try:
            if len(df) < 20:
                return zones
            
            # Recent range (last 20 candles)
            recent_high = df['high'].values[-20:].max()
            recent_low = df['low'].values[-20:].min()
            
            # Check range high
            high_distance_pct = abs(current_price - recent_high) / recent_high * 100
            if high_distance_pct <= 0.3:  # Very close to range high
                zones.append(RejectionZone(
                    zone_type="RANGE_HIGH",
                    price_level=recent_high,
                    strength=0.8,
                    volume_confirmation=False,
                    rsi_position="IN_ZONE",
                    is_active=True
                ))
            
            # Check range low
            low_distance_pct = abs(current_price - recent_low) / recent_low * 100
            if low_distance_pct <= 0.3:  # Very close to range low
                zones.append(RejectionZone(
                    zone_type="RANGE_LOW",
                    price_level=recent_low,
                    strength=0.8,
                    volume_confirmation=False,
                    rsi_position="IN_ZONE",
                    is_active=True
                ))
            
            return zones
            
        except Exception as e:
            return []
    
    def _find_failed_break_zones(self, df: pd.DataFrame, current_price: float) -> List[RejectionZone]:
        """Find failed breakout/breakdown zones"""
        zones = []
        
        try:
            if len(df) < 10:
                return zones
            
            # Check for recent failed breakouts
            recent_high = df['high'].values[-5:].max()
            prev_high = df['high'].values[-10:-5].max()
            
            # Failed breakout (price tried to break high but failed)
            if current_price < recent_high and recent_high > prev_high * 1.005:  # 0.5% attempt
                # Check if price rejected from the high
                if any(df['close'].values[-5:] < recent_high * 0.995):  # Rejected by 0.5%
                    zones.append(RejectionZone(
                        zone_type="FAILED_BREAKOUT",
                        price_level=recent_high,
                        strength=0.85,
                        volume_confirmation=False,
                        rsi_position="IN_ZONE",
                        is_active=True
                    ))
            
            # Check for recent failed breakdowns
            recent_low = df['low'].values[-5:].min()
            prev_low = df['low'].values[-10:-5].min()
            
            # Failed breakdown (price tried to break low but failed)
            if current_price > recent_low and recent_low < prev_low * 0.995:  # 0.5% attempt
                # Check if price rejected from the low
                if any(df['close'].values[-5:] > recent_low * 1.005):  # Rejected by 0.5%
                    zones.append(RejectionZone(
                        zone_type="FAILED_BREAKDOWN",
                        price_level=recent_low,
                        strength=0.85,
                        volume_confirmation=False,
                        rsi_position="IN_ZONE",
                        is_active=True
                    ))
            
            return zones
            
        except Exception as e:
            return []
    
    def _analyze_rsi_position(self, rsi_value: float, zone_type: str) -> str:
        """Analyze RSI position relative to zone"""
        # Check if RSI is in rejection zones
        if "SUPPORT" in zone_type or "LOW" in zone_type or "BREAKDOWN" in zone_type:
            # For LONG setups, we want RSI in 40-50 zone
            if REJECTION_CONFIG["rsi_long_zone"][0] <= rsi_value <= REJECTION_CONFIG["rsi_long_zone"][1]:
                return "IN_ZONE"
            elif rsi_value < 30:
                return "OVEREXTENDED"
            else:
                return "NEUTRAL"
        
        elif "RESISTANCE" in zone_type or "HIGH" in zone_type or "BREAKOUT" in zone_type:
            # For SHORT setups, we want RSI in 50-60 zone
            if REJECTION_CONFIG["rsi_short_zone"][0] <= rsi_value <= REJECTION_CONFIG["rsi_short_zone"][1]:
                return "IN_ZONE"
            elif rsi_value > 70:
                return "OVEREXTENDED"
            else:
                return "NEUTRAL"
        
        return "NEUTRAL"
    
    # ========== DYNAMIC SL/TP METHODS (USING 15M, 30M, 1H) ==========
    
    def find_nearest_swing_low_multi_tf(self, multi_tf_data: Dict[str, pd.DataFrame], 
                                        side: str, entry_price: float, zone_price: float) -> Tuple[Optional[float], str, str]:
        """
        Find nearest significant swing low for LONG or swing high for SHORT
        Returns (price, source, timeframe)
        """
        try:
            # Define which timeframes to use for swing analysis (HIGHER TIMEFRAMES ONLY)
            swing_tfs = ["15M", "30M", "1H"]
            
            all_swings = []
            
            for tf_name in swing_tfs:
                if tf_name not in multi_tf_data:
                    continue
                    
                df = multi_tf_data[tf_name]
                
                if len(df) < 50:
                    continue
                
                if side == "LONG":
                    # For LONG: look for recent swing lows BELOW current price
                    swing_lows = []
                    
                    # Scan last 50 candles for swing lows
                    for i in range(5, len(df) - 5):
                        low = df['low'].iloc[i]
                        
                        # Check if this is a swing low (lower than 5 candles on each side)
                        is_swing = True
                        for j in range(1, 6):
                            if low >= df['low'].iloc[i-j]:
                                is_swing = False
                                break
                            if low >= df['low'].iloc[i+j]:
                                is_swing = False
                                break
                        
                        if is_swing:
                            # Only consider swing lows BELOW entry zone with reasonable distance
                            if low < zone_price * 0.98:  # At least 2% below
                                swing_lows.append({
                                    'price': low,
                                    'position': i,
                                    'distance': zone_price - low,
                                    'timeframe': tf_name,
                                    'candles_ago': len(df) - i - 1
                                })
                    
                    if not swing_lows:
                        continue
                    
                    # Sort by distance (closest first)
                    swing_lows.sort(key=lambda x: x['distance'])
                    
                    # Get the closest valid swing low
                    for swing in swing_lows:
                        swing_price = swing['price']
                        
                        # Check if this swing is still valid (not broken recently)
                        recent_lows = df['low'].values[swing['position']+1:]
                        if len(recent_lows) > 0 and min(recent_lows) < swing_price:
                            continue  # This swing was broken, skip
                        
                        all_swings.append({
                            'price': swing_price,
                            'original_price': swing_price,
                            'distance': swing['distance'],
                            'timeframe': tf_name,
                            'candles_ago': swing['candles_ago'],
                            'type': 'SWING_LOW'
                        })
                        
                        # Take only the best from each timeframe
                        break
                        
                else:  # SHORT
                    # For SHORT: look for recent swing highs ABOVE current price
                    swing_highs = []
                    
                    # Scan last 50 candles for swing highs
                    for i in range(5, len(df) - 5):
                        high = df['high'].iloc[i]
                        
                        # Check if this is a swing high (higher than 5 candles on each side)
                        is_swing = True
                        for j in range(1, 6):
                            if high <= df['high'].iloc[i-j]:
                                is_swing = False
                                break
                            if high <= df['high'].iloc[i+j]:
                                is_swing = False
                                break
                        
                        if is_swing:
                            # Only consider swing highs ABOVE entry zone with reasonable distance
                            if high > zone_price * 1.02:  # At least 2% above
                                swing_highs.append({
                                    'price': high,
                                    'position': i,
                                    'distance': high - zone_price,
                                    'timeframe': tf_name,
                                    'candles_ago': len(df) - i - 1
                                })
                    
                    if not swing_highs:
                        continue
                    
                    # Sort by distance (closest first)
                    swing_highs.sort(key=lambda x: x['distance'])
                    
                    # Get the closest valid swing high
                    for swing in swing_highs:
                        swing_price = swing['price']
                        
                        # Check if this swing is still valid (not broken recently)
                        recent_highs = df['high'].values[swing['position']+1:]
                        if len(recent_highs) > 0 and max(recent_highs) > swing_price:
                            continue  # This swing was broken, skip
                        
                        all_swings.append({
                            'price': swing_price,
                            'original_price': swing_price,
                            'distance': swing['distance'],
                            'timeframe': tf_name,
                            'candles_ago': swing['candles_ago'],
                            'type': 'SWING_HIGH'
                        })
                        
                        # Take only the best from each timeframe
                        break
            
            if not all_swings:
                return None, "NO_SWING_FOUND", "NONE"
            
            # Sort swings by timeframe priority (1H > 30M > 15M) then distance
            timeframe_priority = {"1H": 3, "30M": 2, "15M": 1}
            all_swings.sort(key=lambda x: (-timeframe_priority.get(x['timeframe'], 0), x['distance']))
            
            # Get the best swing
            best_swing = all_swings[0]
            
            # Add small buffer (0.3%)
            if side == "LONG":
                final_price = best_swing['price'] * 0.997
                source = "SWING_LOW"
            else:  # SHORT
                final_price = best_swing['price'] * 1.003
                source = "SWING_HIGH"
            
            return final_price, source, best_swing['timeframe']
                
        except Exception as e:
            log.error(f"Multi-timeframe swing analysis error: {e}")
            return None, "ERROR", "NONE"
    
    def find_liquidity_pool_target_multi_tf(self, multi_tf_data: Dict[str, pd.DataFrame], side: str, 
                                            entry_price: float, stop_loss: float, 
                                            current_rsi: float) -> Tuple[Optional[float], str, str]:
        """
        Find nearest liquidity pool for take profit
        Returns (price, source, timeframe)
        """
        try:
            # Define which timeframes to use for liquidity analysis
            liquidity_tfs = ["15M", "30M", "1H"]
            
            all_targets = []
            
            for tf_name in liquidity_tfs:
                if tf_name not in multi_tf_data:
                    continue
                    
                df = multi_tf_data[tf_name]
                
                if len(df) < 100:
                    continue
                
                if side == "LONG":
                    # For LONG: look for resistance levels (liquidity above)
                    resistance_levels = []
                    
                    # 1. Recent swing highs (last 100 candles)
                    for i in range(20, len(df) - 20):
                        high = df['high'].iloc[i]
                        
                        # Check if this is a swing high
                        is_swing = True
                        for j in range(1, 8):
                            if high <= df['high'].iloc[i-j] or high <= df['high'].iloc[i+j]:
                                is_swing = False
                                break
                        
                        if is_swing and high > entry_price:
                            # Calculate volume at this level
                            volume_at_level = df['volume'].iloc[max(0, i-3):min(len(df), i+4)].sum()
                            resistance_levels.append({
                                'price': high,
                                'volume': volume_at_level,
                                'distance': high - entry_price,
                                'timeframe': tf_name,
                                'type': 'LIQUIDITY_POOL_SWING'
                            })
                    
                    # 2. High volume nodes (clusters)
                    price_bins = np.linspace(df['low'].min(), df['high'].max(), 30)
                    volume_profile = []
                    
                    for i in range(len(price_bins) - 1):
                        low_bound = price_bins[i]
                        high_bound = price_bins[i + 1]
                        
                        # Sum volume in this price range
                        mask = (df['low'] >= low_bound) & (df['high'] <= high_bound)
                        if mask.any():
                            volume_in_range = df.loc[mask, 'volume'].sum()
                            mid_price = (low_bound + high_bound) / 2
                            if mid_price > entry_price:
                                volume_profile.append({
                                    'price': mid_price,
                                    'volume': volume_in_range,
                                    'distance': mid_price - entry_price,
                                    'timeframe': tf_name,
                                    'type': 'LIQUIDITY_POOL_VOLUME'
                                })
                    
                    # Combine all potential targets
                    all_potential_targets = resistance_levels + volume_profile
                    
                    if not all_potential_targets:
                        continue
                    
                    # Filter targets that provide good risk/reward
                    risk = entry_price - stop_loss
                    valid_targets = []
                    
                    for target in all_potential_targets:
                        reward = target['price'] - entry_price
                        if reward > 0:
                            rr_ratio = reward / risk
                            if rr_ratio >= MIN_RISK_REWARD:
                                # Weight by volume and timeframe
                                volume_weight = np.log1p(target['volume'])
                                timeframe_weight = 1.0
                                if target['timeframe'] == "1H":
                                    timeframe_weight = 1.5
                                elif target['timeframe'] == "30M":
                                    timeframe_weight = 1.3
                                
                                valid_targets.append({
                                    'price': target['price'],
                                    'rr_ratio': rr_ratio,
                                    'volume': target['volume'],
                                    'distance': target['distance'],
                                    'timeframe': target['timeframe'],
                                    'type': target['type'],
                                    'score': rr_ratio * volume_weight * timeframe_weight
                                })
                    
                    if valid_targets:
                        # Select best target
                        valid_targets.sort(key=lambda x: x['score'], reverse=True)
                        best_target = valid_targets[0]
                        all_targets.append(best_target)
                    
                else:  # SHORT
                    # For SHORT: look for support levels (liquidity below)
                    support_levels = []
                    
                    # 1. Recent swing lows (last 100 candles)
                    for i in range(20, len(df) - 20):
                        low = df['low'].iloc[i]
                        
                        # Check if this is a swing low
                        is_swing = True
                        for j in range(1, 8):
                            if low >= df['low'].iloc[i-j] or low >= df['low'].iloc[i+j]:
                                is_swing = False
                                break
                        
                        if is_swing and low < entry_price:
                            # Calculate volume at this level
                            volume_at_level = df['volume'].iloc[max(0, i-3):min(len(df), i+4)].sum()
                            support_levels.append({
                                'price': low,
                                'volume': volume_at_level,
                                'distance': entry_price - low,
                                'timeframe': tf_name,
                                'type': 'LIQUIDITY_POOL_SWING'
                            })
                    
                    # 2. High volume nodes (clusters)
                    price_bins = np.linspace(df['low'].min(), df['high'].max(), 30)
                    volume_profile = []
                    
                    for i in range(len(price_bins) - 1):
                        low_bound = price_bins[i]
                        high_bound = price_bins[i + 1]
                        
                        # Sum volume in this price range
                        mask = (df['low'] >= low_bound) & (df['high'] <= high_bound)
                        if mask.any():
                            volume_in_range = df.loc[mask, 'volume'].sum()
                            mid_price = (low_bound + high_bound) / 2
                            if mid_price < entry_price:
                                volume_profile.append({
                                    'price': mid_price,
                                    'volume': volume_in_range,
                                    'distance': entry_price - mid_price,
                                    'timeframe': tf_name,
                                    'type': 'LIQUIDITY_POOL_VOLUME'
                                })
                    
                    # Combine all potential targets
                    all_potential_targets = support_levels + volume_profile
                    
                    if not all_potential_targets:
                        continue
                    
                    # Filter targets that provide good risk/reward
                    risk = stop_loss - entry_price
                    valid_targets = []
                    
                    for target in all_potential_targets:
                        reward = entry_price - target['price']
                        if reward > 0:
                            rr_ratio = reward / risk
                            if rr_ratio >= MIN_RISK_REWARD:
                                # Weight by volume and timeframe
                                volume_weight = np.log1p(target['volume'])
                                timeframe_weight = 1.0
                                if target['timeframe'] == "1H":
                                    timeframe_weight = 1.5
                                elif target['timeframe'] == "30M":
                                    timeframe_weight = 1.3
                                
                                valid_targets.append({
                                    'price': target['price'],
                                    'rr_ratio': rr_ratio,
                                    'volume': target['volume'],
                                    'distance': target['distance'],
                                    'timeframe': target['timeframe'],
                                    'type': target['type'],
                                    'score': rr_ratio * volume_weight * timeframe_weight
                                })
                    
                    if valid_targets:
                        # Select best target
                        valid_targets.sort(key=lambda x: x['score'], reverse=True)
                        best_target = valid_targets[0]
                        all_targets.append(best_target)
            
            if not all_targets:
                return None, "NO_LIQUIDITY_POOL", "NONE"
            
            # Select the best target across all timeframes
            all_targets.sort(key=lambda x: x['score'], reverse=True)
            best_target = all_targets[0]
            
            # Add small buffer
            if side == "LONG":
                final_price = best_target['price'] * 0.998
            else:  # SHORT
                final_price = best_target['price'] * 1.002
            
            return final_price, "LIQUIDITY_POOL", best_target['timeframe']
                
        except Exception as e:
            log.error(f"Multi-timeframe liquidity pool analysis error: {e}")
            return None, "ERROR", "NONE"
    
    def calculate_dynamic_sl_tp_multi_tf(self, multi_tf_data: Dict[str, pd.DataFrame], side: str, 
                                        entry_price: float, zone_price: float, 
                                        current_rsi: float) -> Tuple[Optional[float], Optional[float], str, str, str, str]:
        """
        Calculate dynamic stop loss and take profit with METHOD INFORMATION
        Returns (stop_loss, take_profit, tp_method, tp_timeframe, sl_source, sl_timeframe)
        """
        try:
            # Find nearest swing for stop loss
            stop_loss, sl_source, sl_timeframe = self.find_nearest_swing_low_multi_tf(
                multi_tf_data=multi_tf_data,
                side=side,
                entry_price=entry_price,
                zone_price=zone_price
            )
            
            if stop_loss is None:
                log.debug(f"No valid swing found for {side} stop loss")
                return None, None, "NO_SL", "NONE", "NO_SOURCE", "NONE"
            
            # Calculate risk
            if side == "LONG":
                risk = entry_price - stop_loss
                if risk <= 0:
                    log.debug(f"Invalid risk calculation for LONG")
                    return None, None, "INVALID_RISK", "NONE", sl_source, sl_timeframe
            else:  # SHORT
                risk = stop_loss - entry_price
                if risk <= 0:
                    log.debug(f"Invalid risk calculation for SHORT")
                    return None, None, "INVALID_RISK", "NONE", sl_source, sl_timeframe
            
            # 1. FIRST TRY: Find liquidity pool target (PRIMARY METHOD)
            take_profit, tp_method, tp_timeframe = self.find_liquidity_pool_target_multi_tf(
                multi_tf_data=multi_tf_data,
                side=side,
                entry_price=entry_price,
                stop_loss=stop_loss,
                current_rsi=current_rsi
            )
            
            # Update stats
            if tp_method == "LIQUIDITY_POOL":
                self.daily_stats["tp_liquidity_pool"] += 1
            
            # 2. SECOND TRY: If no liquidity pool, use minimum R:R fallback
            if take_profit is None or tp_method != "LIQUIDITY_POOL":
                log.debug(f"No valid liquidity pool found, using minimum R:R fallback")
                
                # Calculate minimum TP based on MIN_RISK_REWARD
                if side == "LONG":
                    take_profit = entry_price + (risk * MIN_RISK_REWARD)
                else:  # SHORT
                    take_profit = entry_price - (risk * MIN_RISK_REWARD)
                
                tp_method = "MINIMUM_RR"
                tp_timeframe = "DYNAMIC"  # Not from a specific timeframe
                self.daily_stats["tp_minimum_rr"] += 1
            
            # Final validation
            if take_profit is None:
                return None, None, "NO_TP", "NONE", sl_source, sl_timeframe
            
            # Calculate reward and risk/reward ratio
            if side == "LONG":
                reward = take_profit - entry_price
            else:  # SHORT
                reward = entry_price - take_profit
            
            if reward <= 0:
                log.debug(f"Invalid reward calculation")
                return None, None, "INVALID_REWARD", "NONE", sl_source, sl_timeframe
            
            rr_ratio = reward / risk
            
            # Additional safety checks
            if side == "LONG":
                if stop_loss >= entry_price * 0.99:  # SL too close
                    log.debug(f"Stop loss too close for LONG")
                    return None, None, "SL_TOO_CLOSE", "NONE", sl_source, sl_timeframe
                if take_profit <= entry_price * 1.005:  # TP too close
                    log.debug(f"Take profit too close for LONG")
                    return None, None, "TP_TOO_CLOSE", "NONE", sl_source, sl_timeframe
            else:  # SHORT
                if stop_loss <= entry_price * 1.01:  # SL too close
                    log.debug(f"Stop loss too close for SHORT")
                    return None, None, "SL_TOO_CLOSE", "NONE", sl_source, sl_timeframe
                if take_profit >= entry_price * 0.995:  # TP too close
                    log.debug(f"Take profit too close for SHORT")
                    return None, None, "TP_TOO_CLOSE", "NONE", sl_source, sl_timeframe
            
            log.info(f"Dynamic SL/TP: SL={stop_loss:.6f} ({sl_source}@{sl_timeframe}), "
                    f"TP={take_profit:.6f} ({tp_method}@{tp_timeframe}), "
                    f"R:R={rr_ratio:.2f}:1")
            
            return stop_loss, take_profit, tp_method, tp_timeframe, sl_source, sl_timeframe
            
        except Exception as e:
            log.error(f"Dynamic SL/TP calculation error: {e}")
            return None, None, "ERROR", "NONE", "ERROR", "NONE"
    
    # ========== REJECTION SIGNAL GENERATION ==========
    
    def calculate_rsi(self, prices: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
        """Calculate RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_emas(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate current EMA values"""
        try:
            emas = {}
            for name, period in EMA_PERIODS.items():
                ema_series = df['close'].ewm(span=period, adjust=False).mean()
                emas[name] = ema_series.iloc[-1] if len(ema_series) > 0 else 0
            return emas
        except Exception as e:
            return {name: 0 for name in EMA_PERIODS.keys()}
    
    def generate_rejection_signal(self, multi_tf_data: Dict[str, pd.DataFrame], 
                                 symbol: str) -> Optional[RejectionSignal]:
        """
        Generate rejection-based signal with DYNAMIC SL/TP from higher timeframes
        """
        try:
            # Get timeframe data
            tf_1h = multi_tf_data.get("1H")
            tf_30m = multi_tf_data.get("30M")
            tf_15m = multi_tf_data.get("15M")
            tf_5m = multi_tf_data.get("5M")
            tf_3m = multi_tf_data.get("3M")
            tf_1m = multi_tf_data.get("1M")
            
            # Check data availability
            if tf_15m is None or tf_3m is None:
                log.debug(f"{symbol}: Missing key timeframe data")
                return None
            
            # 1. Analyze wave context
            wave_context = self.analyze_wave_context(tf_1h, tf_15m)
            
            # 2. Analyze market strength on 15M
            market_strength = self.analyze_market_strength(tf_15m)
            
            # CRITICAL: No strength → no trade
            if market_strength.strength_score < 0.2:
                self.daily_stats["no_strength"] += 1
                log.debug(f"{symbol}: No market strength ({market_strength.strength_score:.2f})")
                return None
            
            # 3. Calculate indicators on 3M (entry timeframe)
            current_price = tf_3m['close'].iloc[-1]
            emas = self.calculate_emas(tf_3m)
            
            # Calculate RSI
            rsi_series = self.calculate_rsi(tf_3m['close'])
            current_rsi = rsi_series.iloc[-1] if len(rsi_series) > 0 else 50
            
            # 4. Find rejection zones on 3M
            rejection_zones = self.find_rejection_zones(tf_3m, current_price, current_rsi, emas)
            
            # CRITICAL: No rejection zone → no trade
            if not rejection_zones:
                self.daily_stats["no_rejection_zone"] += 1
                log.debug(f"{symbol}: No active rejection zone")
                return None
            
            # 5. Check volume confirmation for each zone
            valid_zones = []
            for zone in rejection_zones:
                zone.volume_confirmation = self._check_volume_confirmation(tf_3m, zone.zone_type)
                if zone.volume_confirmation:
                    valid_zones.append(zone)
            
            if not valid_zones:
                log.debug(f"{symbol}: No volume confirmation at rejection zones")
                return None
            
            # 6. Select strongest rejection zone
            best_zone = max(valid_zones, key=lambda z: z.strength)
            
            # 7. Determine trade side based on zone type
            side = None
            if best_zone.zone_type in ["EMA_SUPPORT", "RANGE_LOW", "FAILED_BREAKDOWN", "DEMAND"]:
                side = "LONG"
            elif best_zone.zone_type in ["EMA_RESISTANCE", "RANGE_HIGH", "FAILED_BREAKOUT", "SUPPLY"]:
                side = "SHORT"
            
            if not side:
                log.debug(f"{symbol}: Could not determine side for zone {best_zone.zone_type}")
                return None
            
            # 8. Check RSI position for the zone
            if side == "LONG" and best_zone.rsi_position != "IN_ZONE":
                log.debug(f"{symbol}: RSI not in LONG zone ({current_rsi:.1f})")
                return None
            elif side == "SHORT" and best_zone.rsi_position != "IN_ZONE":
                log.debug(f"{symbol}: RSI not in SHORT zone ({current_rsi:.1f})")
                return None
            
            # 9. TRADE-BASED DEDUPLICATION CHECK
            if not self.deduplicator.should_generate_signal(symbol, side, current_price):
                self.daily_stats["rejections_filtered"] += 1
                return None
            
            # 10. Analyze candle for rejection confirmation
            rejection_type, trigger_candle = self._analyze_rejection_candle(tf_3m, side, best_zone)
            
            if not rejection_type:
                log.debug(f"{symbol}: No clear rejection candle")
                return None
            
            # 11. Calculate DYNAMIC SL/TP with METHOD INFORMATION
            zone_price = best_zone.price_level
            
            # Entry at rejection zone
            if side == "LONG":
                entry_price = zone_price * 1.001  # 0.1% above support
            else:  # SHORT
                entry_price = zone_price * 0.999  # 0.1% below resistance
            
            # Calculate dynamic SL/TP with method information
            stop_loss, take_profit, tp_method, tp_timeframe, sl_source, sl_timeframe = \
                self.calculate_dynamic_sl_tp_multi_tf(
                    multi_tf_data=multi_tf_data,
                    side=side,
                    entry_price=entry_price,
                    zone_price=zone_price,
                    current_rsi=current_rsi
                )
            
            if stop_loss is None or take_profit is None:
                log.debug(f"{symbol}: No valid dynamic SL/TP levels found")
                return None
            
            # Calculate Risk/Reward
            if side == "LONG":
                risk = entry_price - stop_loss
                reward = take_profit - entry_price
            else:  # SHORT
                risk = stop_loss - entry_price
                reward = entry_price - take_profit
            
            if risk <= 0:
                log.debug(f"{symbol}: Invalid risk calculation")
                return None
            
            risk_reward = reward / risk
            
            # Minimum R:R check
            if risk_reward < MIN_RISK_REWARD:
                log.debug(f"{symbol}: R:R too low ({risk_reward:.1f}:1)")
                return None
            
            # Calculate expected move percentage
            if side == "LONG":
                expected_move_pct = (take_profit - entry_price) / entry_price * 100
            else:
                expected_move_pct = (entry_price - take_profit) / entry_price * 100
            
            # 12. Calculate rejection strength
            rejection_strength = self._calculate_rejection_strength(
                best_zone, market_strength, wave_context, current_rsi
            )
            
            if rejection_strength < REJECTION_CONFIG["min_rejection_strength"]:
                log.debug(f"{symbol}: Rejection too weak ({rejection_strength:.2f})")
                return None
            
            # 13. Determine conditions met
            conditions_met = self._get_rejection_conditions(
                wave_context, market_strength, best_zone, rejection_type
            )
            
            # 14. Create signal ID
            signal_id = hashlib.md5(
                f"{symbol}:{side}:{entry_price:.8f}:{time.time()}:{best_zone.zone_type}".encode()
            ).hexdigest()
            
            # 15. Create final signal with TP/SL METHOD INFORMATION
            signal = RejectionSignal(
                signal_id=signal_id,
                symbol=symbol,
                side=side,
                
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                
                # TP/SL METHOD INFORMATION
                tp_method=tp_method,
                tp_timeframe=tp_timeframe,
                sl_timeframe=sl_timeframe,
                sl_source=sl_source,
                
                wave_context=wave_context,
                market_strength=market_strength,
                rejection_zone=best_zone,
                
                rejection_type=rejection_type,
                trigger_candle=trigger_candle,
                rsi_at_entry=current_rsi,
                
                rejection_strength=rejection_strength,
                risk_reward=risk_reward,
                expected_move_pct=expected_move_pct,
                
                timeframe_used="3M",
                signal_timestamp=time.time(),
                conditions_met=conditions_met,
                
                # SIMPLIFIED STATUS - IMMEDIATELY ACTIVE
                status="ACTIVE"
            )
            
            # 16. Update tracking and deduplication
            self.deduplicator.register_signal(signal)
            self.active_signal_ids.add(signal_id)
            
            # 17. Update statistics
            self.daily_stats["rejections_found"] += 1
            if side == "LONG":
                self.daily_stats["long_rejections"] += 1
            else:
                self.daily_stats["short_rejections"] += 1
            
            log.info(f"🎯 REJECTION SIGNAL: {symbol} {side} @ {entry_price:.4f}")
            log.info(f"   Zone: {best_zone.zone_type}, Strength: {rejection_strength:.2f}")
            log.info(f"   RSI: {current_rsi:.1f}, Dynamic R:R: {risk_reward:.2f}:1")
            log.info(f"   SL: {stop_loss:.4f} ({sl_source}@{sl_timeframe})")
            log.info(f"   TP: {take_profit:.4f} ({tp_method}@{tp_timeframe})")
            log.info(f"   TP Method: {tp_method}, SL Source: {sl_source}")
            
            return signal
            
        except Exception as e:
            log.error(f"Rejection signal error for {symbol}: {e}")
            return None
    
    def _check_volume_confirmation(self, df: pd.DataFrame, zone_type: str) -> bool:
        """Check volume confirmation at rejection zone"""
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
            
            return False
            
        except Exception as e:
            return False
    
    def _analyze_rejection_candle(self, df: pd.DataFrame, side: str, zone: RejectionZone) -> Tuple[Optional[str], Optional[str]]:
        """Analyze the rejection candle pattern"""
        try:
            if len(df) < 3:
                return None, None
            
            current_candle = df.iloc[-1]
            
            # Check wick rejection
            if side == "LONG":
                if (current_candle['low'] < zone.price_level and 
                    current_candle['close'] > zone.price_level):
                    return "WICK_REJECTION", "SUPPORT_WICK"
            
            else:  # SHORT
                if (current_candle['high'] > zone.price_level and 
                    current_candle['close'] < zone.price_level):
                    return "WICK_REJECTION", "RESISTANCE_WICK"
            
            # Check price rejection
            if side == "LONG":
                if current_candle['low'] <= zone.price_level * 1.001 and current_candle['close'] > zone.price_level:
                    return "PRICE_REJECTION", "SUPPORT_HOLD"
            else:
                if current_candle['high'] >= zone.price_level * 0.999 and current_candle['close'] < zone.price_level:
                    return "PRICE_REJECTION", "RESISTANCE_HOLD"
            
            return None, None
            
        except Exception as e:
            return None, None
    
    def _calculate_rejection_strength(self, zone: RejectionZone, strength: MarketStrength, 
                                     wave: WaveContext, rsi: float) -> float:
        """Calculate overall rejection strength score"""
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
    
    def _get_rejection_conditions(self, wave: WaveContext, strength: MarketStrength, 
                                 zone: RejectionZone, rejection_type: str) -> List[str]:
        """Get list of conditions met for this rejection"""
        conditions = []
        conditions.append(f"WAVE_{wave.wave_length}")
        conditions.append(f"STRUCTURE_{wave.structure_type}")
        
        if strength.is_continuation:
            conditions.append("STRENGTH_CONTINUATION")
        if strength.is_rejection_setup:
            conditions.append("STRENGTH_REJECTION_SETUP")
        
        conditions.append(f"ZONE_{zone.zone_type}")
        if zone.volume_confirmation:
            conditions.append("VOLUME_CONFIRMED")
        
        conditions.append(f"REJECTION_{rejection_type}")
        conditions.append(f"RSI_{zone.rsi_position}")
        
        return conditions
    
    def get_daily_stats(self) -> Dict:
        """Get daily statistics"""
        return self.daily_stats
    
    def cleanup_old_signals(self):
        """Clean up old signals from deduplication"""
        self.deduplicator.remove_closed_signals()

# ================ MAIN SCANNER SYSTEM ================
class RejectionScanner:
    """Main scanner system for rejection-based trading"""
    
    def __init__(self):
        self.scanner = RejectionBasedScanner()
        self.exchange = None
        self.db = None
        self.scan_cycle = 0
        
    async def initialize(self):
        """Initialize the scanner"""
        log.info("=" * 70)
        log.info("🔥 REJECTION-BASED HIGH-FREQUENCY SCANNER")
        log.info("=" * 70)
        log.info("TRADER ROLE: Discretionary reaction trader")
        log.info("SPECIALTY: Wave-length awareness + Strength analysis + Rejection entries")
        log.info("PHILOSOPHY: Wave length sets context, Strength & volume make decision")
        log.info("ENTRY RULE: Rejection pulls the trigger")
        log.info(f"SCAN INTERVAL: {SCAN_INTERVAL} seconds")
        log.info("TIME FRAMES: 1H/30M/15M (SL/TP), 5M/3M/1M (entries)")
        log.info("REJECTION ZONES: EMA, Range, Failed breaks only")
        log.info("RSI ZONES: 40-50 (LONG), 50-60 (SHORT)")
        log.info("🎯 STOP LOSS: Nearest Swing Low/High (15M/30M/1H ONLY)")
        log.info("🎯 TAKE PROFIT: Primary: Liquidity Pool (15M/30M/1H) | Fallback: Minimum R:R")
        log.info(f"🎯 MINIMUM R:R: {MIN_RISK_REWARD}:1")
        log.info("🛑 STRICT RULE: NEVER use 3M/5M for SL/TP - ALWAYS use 15M/30M/1H")
        log.info("DEDUPLICATION: ONE TRADE PER SYMBOL")
        log.info("STATUS: SIGNAL = ACTIVE (No PENDING/TRIGGERED states)")
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
            
            # Rejection signals table with TP/SL method info
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS rejection_signals (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                
                -- TP/SL METHOD INFORMATION --
                tp_method TEXT NOT NULL,
                tp_timeframe TEXT NOT NULL,
                sl_timeframe TEXT NOT NULL,
                sl_source TEXT NOT NULL,
                
                wave_length TEXT NOT NULL,
                wave_maturity REAL NOT NULL,
                expansion_speed REAL NOT NULL,
                structure_type TEXT NOT NULL,
                
                candle_speed REAL NOT NULL,
                distance_ratio REAL NOT NULL,
                ema_angle REAL NOT NULL,
                volume_participation REAL NOT NULL,
                strength_score REAL NOT NULL,
                
                zone_type TEXT NOT NULL,
                rejection_strength REAL NOT NULL,
                rsi_at_entry REAL NOT NULL,
                rejection_type TEXT NOT NULL,
                trigger_candle TEXT NOT NULL,
                
                risk_reward REAL NOT NULL,
                expected_move REAL NOT NULL,
                timeframe_used TEXT NOT NULL,
                
                conditions_met TEXT,
                status TEXT DEFAULT 'ACTIVE',  -- SIMPLIFIED: ACTIVE/CLOSED only
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                closed_at TIMESTAMP,
                close_price REAL,
                pnl_percent REAL,
                close_reason TEXT
            )
            """)
            
            # Performance table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS performance_daily (
                date DATE PRIMARY KEY,
                total_rejections INTEGER,
                long_rejections INTEGER,
                short_rejections INTEGER,
                no_strength_count INTEGER,
                no_zone_count INTEGER,
                tp_liquidity_pool INTEGER,
                tp_minimum_rr INTEGER,
                win_rate REAL,
                avg_win REAL,
                avg_loss REAL,
                total_pnl REAL
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
            
            # Test connection
            ticker = await self.exchange.fetch_ticker("BTC/USDT")
            log.info(f"✅ Exchange connected. BTC: ${ticker['last']:.2f}")
            
        except Exception as e:
            log.error(f"Exchange error: {e}")
            raise
    
    async def _send_startup_message(self):
        """Send startup message to Telegram"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning("⚠️ Telegram credentials not set. Notifications will not be sent.")
            return
        
        try:
            message = f"""
🎯 <b>REJECTION-BASED HIGH-FREQUENCY SCANNER</b>

<b>🧠 TRADER MINDSET:</b>
• Reaction trader (not prediction-based)
• Rejection specialist
• Comfortable being wrong
• Emotionless with losses
• Hunts expansion, accepts losses

<b>📊 ANALYSIS FRAMEWORK:</b>
1️⃣ <b>الطول الموجي (Wave Length)</b> → السياق فقط
‎   • طول الموجة ونضجها
‎   • سرعة التوسع
‎   • لا عد للموجات

2️⃣ <b>القوة (Market Strength)</b> → قرار الدخول
‎   • سرعة الشموع
‎   • المسافة المقطوعة
‎   • زاوية المتوسطات
‎   • مشاركة الفوليوم

3️⃣ <b>مناطق الرفض (Rejection Zones)</b> → الزناد الإجباري
‎   • الدخول عند الرفض فقط
‎   • دعم/مقاومة المتوسطات
‎   • أعلى/أقل النطاق
‎   • اختراقات فاشلة

<b>⚡ إعدادات الدخول:</b>
‎• إطار الدخول: 3M (رئيسي) + 1M (توقيت)
• RSI: 40–50 للشراء، 50–60 للبيع

<b>🎯 <u>إدارة المخاطر الجديدة والمحسنة:</u></b>
<u>🛑 <b>قاعدة صارمة: لا تستخدم 3M/5M أبداً لوقف الخسارة/هدف الربح</b></u>
‎• <b>وقف الخسارة: أقرب قاع تأرجح (15M/30M/1H فقط)</b>
‎• <b>هدف الربح: <u>أولاً</u> أقرب بركة سيولة (15M/30M/1H فقط)</b>
‎• <b>هدف الربح: <u>ثانياً</u> نسبة ربح/خسارة دنيا ({MIN_RISK_REWARD}:1)</b>

<b>🛡️ نظام التكرار:</b>
• <b>صفقة واحدة لكل عملة فقط</b>
‎• إشارات جديدة فقط بعد إغلاق الصفقة السابقة

<b>🎯 نظام الإشارات المبسط:</b>
‎• الإشارة = صفقة نشطة مباشرة
‎• لا حاجة لتنفيذ منفصل
‎• المتابعة التلقائية لوقف الخسارة/هدف الربح

‎الطول الموجي يحدد السياق
‎القوة والفوليوم يحددان القرار
‎والرفض هو الزناد

‎#متداول_تفاعلي #تخصص_الرفض #صفقة_واحدة #نظام_مبسط #لا_حاجة_للتنفيذ
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
                    limit = 150
                elif tf_name == "30M":
                    limit = 120
                elif tf_name == "15M":
                    limit = 100
                elif tf_name == "5M":
                    limit = 80
                elif tf_name == "3M":
                    limit = 60
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
    
    async def save_signal(self, signal: RejectionSignal) -> bool:
        """Save signal to database"""
        try:
            # Insert signal with TP/SL method info
            await self.db.execute("""
                INSERT INTO rejection_signals (
                    id, symbol, side, entry_price, stop_loss, take_profit,
                    tp_method, tp_timeframe, sl_timeframe, sl_source,
                    wave_length, wave_maturity, expansion_speed, structure_type,
                    candle_speed, distance_ratio, ema_angle, volume_participation, strength_score,
                    zone_type, rejection_strength, rsi_at_entry, rejection_type, trigger_candle,
                    risk_reward, expected_move, timeframe_used,
                    conditions_met, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.side,
                signal.entry_price,
                signal.stop_loss,
                signal.take_profit,
                signal.tp_method,
                signal.tp_timeframe,
                signal.sl_timeframe,
                signal.sl_source,
                signal.wave_context.wave_length,
                signal.wave_context.wave_maturity,
                signal.wave_context.expansion_speed,
                signal.wave_context.structure_type,
                signal.market_strength.candle_speed,
                signal.market_strength.distance_ratio,
                signal.market_strength.ema_angle,
                signal.market_strength.volume_participation,
                signal.market_strength.strength_score,
                signal.rejection_zone.zone_type,
                signal.rejection_strength,
                signal.rsi_at_entry,
                signal.rejection_type,
                signal.trigger_candle,
                signal.risk_reward,
                signal.expected_move_pct,
                signal.timeframe_used,
                json.dumps(signal.conditions_met),
                signal.status
            ))
            
            await self.db.commit()
            
            log.info(f"✅ Rejection signal saved: {signal.symbol} (TP: {signal.tp_method}@{signal.tp_timeframe})")
            return True
            
        except Exception as e:
            log.error(f"Error saving signal: {e}")
            return False
    
    async def format_signal_message(self, signal: RejectionSignal) -> str:
        """Format signal for Telegram with TP/SL METHOD INFORMATION"""
        side_emoji = "🟢" if signal.side == "LONG" else "🔴"
        side_text = "شراء" if signal.side == "LONG" else "بيع"
        
        # Calculate risk percentages
        if signal.side == "LONG":
            risk_pct = (signal.entry_price - signal.stop_loss) / signal.entry_price * 100
            target_pct = (signal.take_profit - signal.entry_price) / signal.entry_price * 100
        else:
            risk_pct = (signal.stop_loss - signal.entry_price) / signal.entry_price * 100
            target_pct = (signal.entry_price - signal.take_profit) / signal.entry_price * 100
        
        # Wave context
        wave_info = f"{signal.wave_context.wave_length} ({signal.wave_context.structure_type})"
        
        # Strength info
        strength_type = []
        if signal.market_strength.is_continuation:
            strength_type.append("استمرارية")
        if signal.market_strength.is_rejection_setup:
            strength_type.append("إعداد رفض")
        
        strength_text = "، ".join(strength_type) if strength_type else "محايد"
        
        # Zone info in Arabic
        zone_translation = {
            "EMA_SUPPORT": "دعم المتوسط المتحرك",
            "EMA_RESISTANCE": "مقاومة المتوسط المتحرك",
            "RANGE_LOW": "قاع النطاق",
            "RANGE_HIGH": "سقف النطاق",
            "FAILED_BREAKDOWN": "اختراق فاشل للأسفل",
            "FAILED_BREAKOUT": "اختراق فاشل للأعلى"
        }
        
        zone_text = zone_translation.get(signal.rejection_zone.zone_type, signal.rejection_zone.zone_type)
        
        # TP Method in Arabic
        tp_method_translation = {
            "LIQUIDITY_POOL": "بركة سيولة",
            "MINIMUM_RR": f"نسبة دنيا {MIN_RISK_REWARD}:1",
            "SWING_HIGH_LOW": "قمة/قاع تأرجح"
        }
        
        tp_method_text = tp_method_translation.get(signal.tp_method, signal.tp_method)
        
        # SL Source in Arabic
        sl_source_translation = {
            "SWING_LOW": "قاع تأرجح",
            "SWING_HIGH": "قمة تأرجح",
            "STRUCTURAL": "مستوى هيكلي"
        }
        
        sl_source_text = sl_source_translation.get(signal.sl_source, signal.sl_source)
        
        message = f"""
{side_emoji} <b>إشارة رفض - صفقة نشطة مباشرة</b> ⚡

<b>{signal.symbol}</b> | {side_text}

<b>📊 السياق الموجي:</b>
‎• طول الموجة: {wave_info}
‎• النضج: {signal.wave_context.wave_maturity:.1%}
‎• سرعة التوسع: {signal.wave_context.expansion_speed:.1%}

<b>💪 قوة السوق:</b>
‎• درجة القوة: {signal.market_strength.strength_score:.1%}
‎• النوع: {strength_text}
‎• سرعة الشموع: {signal.market_strength.candle_speed:.1%}
‎• مشاركة الفوليوم: {signal.market_strength.volume_participation:.1%}

<b>🎯 منطقة الرفض:</b>
‎• النوع: {zone_text}
‎• قوة المنطقة: {signal.rejection_zone.strength:.1%}
‎• تأكيد الفوليوم: {"✅" if signal.rejection_zone.volume_confirmation else "❌"}
• RSI عند الدخول: {signal.rsi_at_entry:.1f}

<b>⚡ تفاصيل الرفض:</b>
‎• نوع الرفض: {signal.rejection_type}
‎• شمعة الزناد: {signal.trigger_candle}
‎• قوة الرفض: {signal.rejection_strength:.1%}

<u><b>🎯 طريقة تحديد المستويات:</b></u>

<b>🛡️ وقف الخسارة:</b>
‎• السعر: <code>{signal.stop_loss:.6f}</code>
‎• المصدر: {sl_source_text}
‎• الإطار الزمني: {signal.sl_timeframe}
‎• المخاطرة: {risk_pct:.1f}%

<b>💰 هدف الربح:</b>
‎• السعر: <code>{signal.take_profit:.6f}</code>
‎• الطريقة: {tp_method_text}
‎• الإطار الزمني: {signal.tp_timeframe}
‎• الهدف: {target_pct:.1f}%

<b>📈 نسبة الربح/المخاطرة:</b>
‎• النسبة: {signal.risk_reward:.2f}:1
‎• الحد الأدنى المطلوب: {MIN_RISK_REWARD}:1

<u><b>⚡ حالة الصفقة:</b></u>
‎✅ <b>صفقة نشطة مباشرة</b>
‎✅ <b>متابعة تلقائية لوقف الخسارة/هدف الربح</b>
‎✅ <b>لا حاجة لتنفيذ منفصل</b>

<b>🛡️ نظام التكرار:</b>
‎• نظام: <b>صفقة واحدة لكل عملة</b>
‎• ❌ <b>ممنوع</b> إرسال إشارات جديدة لـ {signal.symbol}
‎• ✅ مسموح بعد إغلاق هذه الصفقة

<b>⚠️ ملاحظة التاجر:</b>
‎الدخول عند الرفض فقط
‎المستويات محددة ديناميكياً
‎نظام مبسط - إشارة = صفقة نشطة

#{side_text} #رفض #{"دعم" if signal.side == "LONG" else "مقاومة"} #صفقة_واحدة #{"بركة_سيولة" if signal.tp_method == "LIQUIDITY_POOL" else "نسبة_دنيا"} #نظام_مبسط
"""
        return message
    
    async def send_telegram_alert(self, signal: RejectionSignal):
        """Send Telegram alert - Signal is immediately ACTIVE"""
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
                
            log.info(f"📤 Telegram rejection alert sent: {signal.symbol} (ACTIVE immediately)")
            
        except Exception as e:
            log.error(f"Telegram error: {e}")
    
    async def send_trade_close_notification(self, symbol: str, side: str, pnl_percent: float, 
                                           close_reason: str, entry_price: float, 
                                           close_price: float, risk_reward: float,
                                           tp_method: str, tp_timeframe: str):
        """Send notification when trade hits TP/SL"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning(f"⚠️ Telegram credentials missing. Skipping close notification for {symbol}")
            return
        
        try:
            log.info(f"📤 Sending close notification for {symbol}: {close_reason} ({pnl_percent:.2f}%)")
            
            if close_reason == "TP_HIT":
                emoji = "✅"
                result_text = "هدف الربح"
                result_emoji = "🎯"
                color = "🟢"
                pnl_emoji = "💰"
            else:  # SL_HIT
                emoji = "❌"
                result_text = "وقف الخسارة"
                result_emoji = "🛑"
                color = "🔴"
                pnl_emoji = "💸"
            
            side_text = "شراء" if side == "LONG" else "بيع"
            
            # TP Method in Arabic
            tp_method_translation = {
                "LIQUIDITY_POOL": "بركة سيولة",
                "MINIMUM_RR": f"نسبة دنيا {MIN_RISK_REWARD}:1",
                "SWING_HIGH_LOW": "قمة/قاع تأرجح"
            }
            
            tp_method_text = tp_method_translation.get(tp_method, tp_method)
            
            # Format P&L
            pnl_formatted = f"+{pnl_percent:.2f}%" if pnl_percent > 0 else f"{pnl_percent:.2f}%"
            
            message = f"""
{emoji} <b>تم إغلاق صفقة الرفض</b> {result_emoji}

<b>{symbol}</b> | {side_text}

{color} <b>النتيجة: {result_text}</b>
{pnl_emoji} <b>النسبة: {pnl_formatted}</b>

<b>📊 تفاصيل التنفيذ:</b>
‎• نوع الدخول: {side_text} (عند الرفض)
‎• سعر الدخول: <code>{entry_price:.6f}</code>
‎• سعر الإغلاق: <code>{close_price:.6f}</code>
‎• نسبة الربح/الخسارة: <b>{pnl_formatted}</b>
‎• نسبة الربح/المخاطرة المحققة: {risk_reward:.2f}:1

<b>🎯 طريقة تحديد الهدف:</b>
‎• طريقة هدف الربح: {tp_method_text}
‎• إطار زمني هدف الربح: {tp_timeframe}

<u><b>🎯 فلسفة النظام:</b></u>
‎• الإشارة = صفقة نشطة مباشرة
‎• المتابعة التلقائية لوقف الخسارة/هدف الربح
‎• لا حاجة لتنفيذ منفصل

<b>🛡️ نظام التكرار:</b>
✅ <b>مسموح الآن</b> بإرسال إشارات جديدة لـ {symbol}
‎يمكن للماسح الضوئي البحث عن رفض جديد لهذه العملة

#{side_text} #إغلاق_رفض #{"ربح" if close_reason == "TP_HIT" else "خسارة"} #مسموح_إشارات_جديدة #{tp_method.replace("_", "")}
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
                if response.status_code == 200:
                    log.info(f"✅ Telegram close notification sent for {symbol}: {close_reason}")
                else:
                    log.error(f"❌ Telegram API error: {response.status_code} - {response.text}")
            
            log.info(f"{emoji} Rejection trade closed: {symbol} {side} {pnl_formatted} ({close_reason})")
            
        except Exception as e:
            log.error(f"Close notification error: {e}")
    
    async def monitor_positions(self):
        """Monitor and close positions - SIMPLIFIED"""
        log.info("👀 Starting position monitoring (Simplified - No PENDING/TRIGGERED states)...")
        
        while True:
            try:
                # Get ALL ACTIVE positions only
                async with self.db.execute("""
                    SELECT id, symbol, side, entry_price, stop_loss, take_profit, 
                           risk_reward, tp_method, tp_timeframe
                    FROM rejection_signals 
                    WHERE status = 'ACTIVE'
                """) as cursor:
                    positions = await cursor.fetchall()
                
                if positions:
                    log.debug(f"📊 Monitoring {len(positions)} ACTIVE positions")
                
                for pos_id, symbol, side, entry, sl, tp, risk_reward, tp_method, tp_timeframe in positions:
                    try:
                        # Get current price
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        # Check SL/TP for ACTIVE positions
                        pnl_percent = 0
                        close_reason = None
                        
                        if side == "LONG":
                            if current_price <= sl:
                                close_reason = "SL_HIT"
                                pnl_percent = ((current_price - entry) / entry) * 100
                            elif current_price >= tp:
                                close_reason = "TP_HIT"
                                pnl_percent = ((current_price - entry) / entry) * 100
                        
                        else:  # SHORT
                            if current_price >= sl:
                                close_reason = "SL_HIT"
                                pnl_percent = ((entry - current_price) / entry) * 100
                            elif current_price <= tp:
                                close_reason = "TP_HIT"
                                pnl_percent = ((entry - current_price) / entry) * 100
                        
                        if close_reason:
                            # Update database
                            await self.db.execute("""
                                UPDATE rejection_signals SET 
                                    status = 'CLOSED',
                                    closed_at = CURRENT_TIMESTAMP,
                                    close_price = ?,
                                    pnl_percent = ?,
                                    close_reason = ?
                                WHERE id = ?
                            """, (current_price, pnl_percent, close_reason, pos_id))
                            
                            await self.db.commit()
                            
                            # UPDATE DEDUPLICATION STATUS to CLOSED
                            self.scanner.deduplicator.update_signal_status(pos_id, "CLOSED")
                            
                            # Clean up from tracking
                            self.scanner.active_signal_ids.discard(pos_id)
                            
                            # SEND CLOSE NOTIFICATION with TP method info
                            await self.send_trade_close_notification(
                                symbol=symbol,
                                side=side,
                                pnl_percent=pnl_percent,
                                close_reason=close_reason,
                                entry_price=entry,
                                close_price=current_price,
                                risk_reward=risk_reward,
                                tp_method=tp_method,
                                tp_timeframe=tp_timeframe
                            )
                    
                    except Exception as e:
                        log.error(f"Monitor error for {symbol}: {e}")
                        continue
                
                # Clean up old closed signals periodically
                if int(time.time()) % 300 < 2:
                    self.scanner.deduplicator.remove_closed_signals()
                
                # Fast monitoring
                await asyncio.sleep(2)
                
            except Exception as e:
                log.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(5)
    
    async def high_freq_scanning(self):
        """Main high-frequency scanning loop for rejections"""
        log.info("🚀 Starting rejection-based high-frequency scanning (Simplified system)...")
        
        while True:
            try:
                self.scan_cycle += 1
                start_time = time.time()
                
                log.info(f"🔄 Scan cycle #{self.scan_cycle} (Signal = Active Trade immediately)")
                
                # Get active pairs
                pairs = await self.get_active_pairs()
                
                if not pairs:
                    log.warning("No active pairs found")
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                log.info(f"Scanning {len(pairs)} active pairs for rejections")
                
                signals_found = 0
                pairs_processed = 0
                
                # Ultra-fast scanning for rejections
                for symbol, volume in pairs:
                    try:
                        # Fetch data
                        multi_tf_data = await self.fetch_timeframe_data(symbol)
                        
                        # Need key timeframes
                        required_tfs = ["15M", "3M"]
                        has_all_data = all(tf in multi_tf_data for tf in required_tfs)
                        
                        if not has_all_data:
                            continue
                        
                        # Generate rejection signal (immediately ACTIVE)
                        signal = self.scanner.generate_rejection_signal(multi_tf_data, symbol)
                        
                        if signal:
                            # Save and send
                            saved = await self.save_signal(signal)
                            
                            if saved:
                                await self.send_telegram_alert(signal)
                                signals_found += 1
                        
                        pairs_processed += 1
                        
                        # Ultra-fast between pairs
                        await asyncio.sleep(0.01)
                        
                    except Exception as e:
                        log.debug(f"Pair error {symbol}: {str(e)[:50]}")
                        continue
                
                # Update scanner stats
                self.scanner.daily_stats["pairs_scanned"] += pairs_processed
                
                # Log rejection stats
                active_count = len(self.scanner.deduplicator.active_signals)
                stats = self.scanner.get_daily_stats()
                
                log.info(f"📊 Rejection stats: Found {signals_found}, Active: {active_count}")
                log.info(f"   TP Methods: Liquidity Pool: {stats.get('tp_liquidity_pool', 0)}, "
                        f"Minimum RR: {stats.get('tp_minimum_rr', 0)}")
                
                scan_duration = time.time() - start_time
                log.info(f"Scan #{self.scan_cycle}: {signals_found} rejections in {scan_duration:.2f}s")
                
                # Wait for next scan
                wait_time = max(0.1, SCAN_INTERVAL - scan_duration)
                log.info(f"Next rejection hunt in {wait_time:.1f}s...")
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
                self.high_freq_scanning(),
                self.monitor_positions()
            )
            
        except KeyboardInterrupt:
            log.info("Rejection scanner stopped by user")
            await self.send_final_stats()
            
        except Exception as e:
            log.error(f"Scanner crashed: {e}")
            
        finally:
            await self.cleanup()
    
    async def send_final_stats(self):
        """Send final statistics"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning("⚠️ Telegram credentials missing. Skipping final stats.")
            return
        
        try:
            stats = self.scanner.get_daily_stats()
            active_count = len(self.scanner.deduplicator.active_signals)
            
            # Calculate percentages
            total_tp = stats.get('tp_liquidity_pool', 0) + stats.get('tp_minimum_rr', 0)
            if total_tp > 0:
                lp_pct = stats.get('tp_liquidity_pool', 0) / total_tp * 100
                rr_pct = stats.get('tp_minimum_rr', 0) / total_tp * 100
            else:
                lp_pct = rr_pct = 0
            
            message = f"""
🛑 <b>تم إيقاف ماسح الرفض</b>

<b>📊 إحصائيات اليوم:</b>
‎• عمليات المسح: {self.scan_cycle}
‎• الأزواج الممسوحة: {stats['pairs_scanned']}
‎• حالات الرفض التي تم العثور عليها: {stats['rejections_found']}
‎• رفض شراء: {stats['long_rejections']}
‎• رفض بيع: {stats['short_rejections']}

<b>🎯 طرق تحديد الهدف:</b>
‎• بركة سيولة: {stats.get('tp_liquidity_pool', 0)} ({lp_pct:.1f}%)
‎• نسبة دنيا: {stats.get('tp_minimum_rr', 0)} ({rr_pct:.1f}%)

<u><b>🎯 نظام الإشارات المبسط:</b></u>
‎✅ <b>الإشارة = صفقة نشطة مباشرة</b>
‎✅ <b>لا حاجة لتنفيذ منفصل</b>
‎✅ <b>متابعة تلقائية لوقف الخسارة/هدف الربح</b>
‎✅ <b>صفقة واحدة لكل عملة</b>

<b>⚡ الصفقات النشطة المتبقية:</b>
‎• حالياً: {active_count} صفقة نشطة

<b>🧠 فلسفة النظام المحققة:</b>
‎• الطول الموجي ← السياق
‎• القوة والفوليوم ← القرار  
‎• الرفض ← الزناد
‎• الإشارة ← صفقة نشطة مباشرة

‎#إحصائيات_الرفض #نظام_مبسط #إشارة_صفقة_نشطة #بركة_سيولة #نسبة_دنيا
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
    """Start simple HTTP server for monitoring"""
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
                    "scanner": "Rejection-Based High-Frequency Scanner",
                    "simplified_system": "Signal = Active Trade immediately",
                    "scan_cycle": scanner.scan_cycle,
                    "active_trades": active_count,
                    "daily_stats": stats,
                    "tp_method_stats": {
                        "liquidity_pool": stats.get('tp_liquidity_pool', 0),
                        "minimum_rr": stats.get('tp_minimum_rr', 0)
                    },
                    "system_features": {
                        "status_states": "ACTIVE/CLOSED only (No PENDING/TRIGGERED)",
                        "auto_monitoring": "SL/TP monitored automatically",
                        "deduplication": "One trade per symbol",
                        "tp_display": "TP method and timeframe included in signals"
                    }
                }, indent=2)
            
            elif path == '/stats':
                response = json.dumps(scanner.scanner.get_daily_stats(), indent=2)
            
            elif path == '/active':
                if scanner.db:
                    scanner.db.row_factory = aiosqlite.Row
                    async with scanner.db.execute("""
                        SELECT symbol, side, entry_price, stop_loss, take_profit,
                               tp_method, tp_timeframe, sl_source, sl_timeframe,
                               risk_reward, created_at
                        FROM rejection_signals 
                        WHERE status = 'ACTIVE'
                        ORDER BY created_at DESC
                    """) as cursor:
                        rows = await cursor.fetchall()
                        signals = [dict(row) for row in rows]
                    
                    response = json.dumps({
                        "active_signals": signals, 
                        "count": len(signals),
                        "note": "Signal = Active Trade immediately"
                    }, indent=2)
                else:
                    response = json.dumps({"error": "Database not available"})
            
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
    """Main entry point"""
    scanner = RejectionScanner()
    
    try:
        await asyncio.gather(
            scanner.run(),
            start_http_server(scanner, port=8000)
        )
    except KeyboardInterrupt:
        log.info("Shutting down rejection scanner...")
    except Exception as e:
        log.error(f"Main error: {e}")
        raise

if __name__ == "__main__":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except:
        pass
    
    asyncio.run(main())