#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌊 WAVE-MOMENTUM TRADING SYSTEM
Professional discretionary system focused on energy transitions
First expansion wave after correction ONLY
Low-price altcoins + Multi-timeframe wave logic
TRADER MINDSET: Wave-energy specialist, energy transition hunter, QUALITY ONLY
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
from enum import Enum

# ================ CORE CONFIG ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/wave_momentum.db"

# Asset selection - BROADENED for testing
MAX_PRICE_USDT = 20.0  # Focus on low-price coins ($0-$10) - increased from $5
MIN_VOLUME_USD = 100000  # $100K minimum (reduced for more opportunities)
MAX_VOLUME_USD = 20000000  # $20M maximum (increased)
PRICE_CHANGE_THRESHOLD = 0.10  # 10% minimum daily range for inefficiency

# Timeframes for wave analysis
class TimeframeRole(Enum):
    PERMISSION = "PERMISSION"  # 1H/4H - Direction only
    WAVE_ID = "WAVE_ID"        # 15M/30M - Wave structure
    EXECUTION = "EXECUTION"    # 3M/5M - Entry timing

TIMEFRAMES = {
    "1H": {"tf": "1h", "role": TimeframeRole.PERMISSION, "candles": 50},
    "15M": {"tf": "15m", "role": TimeframeRole.WAVE_ID, "candles": 75},
    "5M": {"tf": "5m", "role": TimeframeRole.EXECUTION, "candles": 50}
}

# Wave parameters
MIN_CORRECTION_CANDLES = 3  # Minimum correction duration
MAX_CORRECTION_CANDLES = 20  # Maximum correction duration
MIN_IMPULSE_CANDLES = 1  # First impulse candle to trigger
MAX_IMPULSE_AGE = 3  # Max candles since first impulse

# Entry parameters
MAX_STOP_PCT = 1.5  # 1.5% max stop (tight invalidation)
TARGET_RANGE_PCT = (2.0, 6.0)  # 2-6% target range
MIN_RISK_REWARD = 2.0  # 2:1 minimum

# Strength thresholds (strict)
MIN_CANDLE_SPEED = 0.7  # Fast candles only
MIN_BODY_DOMINANCE = 0.6  # Body must dominate wick
MIN_VOLUME_EXPANSION = 1.8  # 80% volume increase on impulse

# RSI zones (confirmation only)
RSI_PERIOD = 14
RSI_MOMENTUM_ZONE = (45, 55)  # Middle zone for transitions

# EMA structure
EMA_PERIODS = [9, 21]  # No slow EMA, no crossovers
EMA_RESPECT_DISTANCE = 0.005  # 0.5% distance for structure respect

# ================ WAVE DATA STRUCTURES ================
@dataclass
class MarketPhase:
    """Market phase detection (HTF only)"""
    phase: str  # IMPULSE_UP, IMPULSE_DOWN, CORRECTION_UP, CORRECTION_DOWN, COMPRESSION
    confidence: float  # 0-1
    bias_allowed: Optional[str]  # LONG, SHORT, SKIP
    wave_count: Optional[int]  # Simple wave count in current phase
    
    @property
    def is_correction_ending(self) -> bool:
        """Check if correction phase is mature enough to end"""
        return self.phase.startswith("CORRECTION") and self.confidence > 0.7

@dataclass
class WaveStructure:
    """Wave structure analysis (MTF)"""
    wave_type: str  # IMPULSE_START, CORRECTION_COMPLETE, CORRECTION_MATURING, UNCLEAR
    wave_length: float  # 0-1 (short to extended)
    symmetry_score: float  # 0-1 (how symmetrical the wave is)
    compression_level: float  # 0-1 (low to high compression)
    expansion_potential: float  # 0-1 (potential for expansion)
    
    # Behavioral metrics
    candle_speed_trend: float  # -1 to 1 (slowing to accelerating)
    body_size_trend: float  # -1 to 1 (shrinking to expanding)
    volume_trend: float  # -1 to 1 (declining to expanding)
    momentum_trend: float  # -1 to 1 (cooling to heating)
    
    @property
    def is_valid_setup(self) -> bool:
        """Check if wave structure is valid for entry"""
        if self.wave_type != "CORRECTION_COMPLETE":
            return False
        
        # Must show correction behavior
        if not (self.candle_speed_trend < 0 and 
                self.body_size_trend < 0 and 
                self.volume_trend < 0 and
                self.momentum_trend < 0):
            return False
        
        # But not too mature (avoid exhaustion)
        if self.compression_level > 0.8:
            return False
            
        return True

@dataclass
class EnergyTransition:
    """Energy transition detection (LTF)"""
    has_transition: bool
    transition_type: Optional[str]  # CORRECTION_TO_IMPULSE, IMPULSE_ACCELERATION
    first_impulse_candle_index: Optional[int]  # Index of first impulse candle
    candle_dominance: float  # 0-1 (body dominance in impulse)
    volume_expansion: float  # 0-1 (volume increase ratio)
    momentum_turn: bool  # RSI turned with price
    ema_structure_hold: bool  # EMA held during correction
    
    # Detailed metrics
    impulse_candle_speed: float
    impulse_body_ratio: float
    correction_candle_speed: float
    correction_body_ratio: float
    
    @property
    def is_valid_entry(self) -> bool:
        """Strict entry validation"""
        if not self.has_transition:
            return False
            
        if self.transition_type != "CORRECTION_TO_IMPULSE":
            return False
            
        # All conditions must be met (STRICT)
        conditions = [
            self.candle_dominance >= MIN_BODY_DOMINANCE,
            self.volume_expansion >= MIN_VOLUME_EXPANSION,
            self.momentum_turn == True,
            self.ema_structure_hold == True,
            self.impulse_candle_speed >= MIN_CANDLE_SPEED,
            self.impulse_body_ratio > self.correction_body_ratio * 1.5  # Significant increase
        ]
        
        return all(conditions)

@dataclass
class WaveSignal:
    """Complete wave-momentum signal"""
    signal_id: str
    symbol: str
    side: str  # LONG, SHORT
    
    # Price levels
    entry_price: float
    invalidation_price: float  # NOT stop-loss, wave invalidation
    expansion_target: float  # Based on wave structure, not fixed %
    
    # Wave analysis
    market_phase: MarketPhase
    wave_structure: WaveStructure
    energy_transition: EnergyTransition
    
    # Strength metrics
    impulse_strength: float  # 0-1
    volume_confidence: float  # 0-1
    structure_quality: float  # 0-1
    
    # Context
    higher_timeframe: str
    wave_timeframe: str
    entry_timeframe: str
    
    # Timing
    correction_duration: int  # Candles in correction
    impulse_age: int  # Candles since first impulse (0 = first candle)
    signal_timestamp: float
    
    @property
    def trade_quality(self) -> float:
        """Overall trade quality score"""
        weights = {
            "phase_confidence": 0.2,
            "wave_valid": 0.3,
            "transition_quality": 0.3,
            "impulse_strength": 0.2
        }
        
        scores = [
            self.market_phase.confidence,
            1.0 if self.wave_structure.is_valid_setup else 0.0,
            1.0 if self.energy_transition.is_valid_entry else 0.0,
            self.impulse_strength
        ]
        
        return np.average(scores, weights=list(weights.values()))

# ================ PROFESSIONAL LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("wave_momentum")

# ================ WAVE-MOMENTUM ENGINE ================
class WaveMomentumTrader:
    """Core trading engine mimicking human wave trader"""
    
    def __init__(self):
        self.active_signals = {}  # symbol: signal_id
        self.trade_stats = {
            "assets_scanned": 0,
            "phase_skipped": 0,
            "wave_structure_skipped": 0,
            "transition_skipped": 0,
            "quality_signals": 0,
            "impulse_entries": 0
        }
    
    # ========== ASSET SELECTION ==========
    
    async def select_wave_assets(self, exchange, all_symbols: List[str]) -> List[str]:
        """Select assets suitable for wave trading"""
        selected = []
        
        for symbol in all_symbols:
            if not symbol.endswith('/USDT'):
                continue
            
            try:
                # Get ticker data
                ticker = await exchange.fetch_ticker(symbol)
                
                # Price filter (low-price focus)
                price = ticker['last']
                if price > MAX_PRICE_USDT:
                    continue
                
                # Volume filter (not dead, not over-efficient)
                volume = ticker.get('quoteVolume', 0)
                if volume < MIN_VOLUME_USD or volume > MAX_VOLUME_USD:
                    continue
                
                # Skip if price is too low (potential illiquidity)
                if price < 0.0001:
                    continue
                
                selected.append(symbol)
                
                # Limit to top 100 for performance
                if len(selected) >= 100:
                    break
                
            except Exception as e:
                log.debug(f"Asset filter error {symbol}: {e}")
                continue
        
        log.info(f"✅ Selected {len(selected)} wave-trading assets")
        return selected
    
    # ========== HIGH TIMEFRAME: PERMISSION ONLY ==========
    
    def analyze_market_phase(self, df_1h: pd.DataFrame) -> MarketPhase:
        """HTF: Detect market phase, set bias permission ONLY"""
        
        if df_1h is None or len(df_1h) < 20:
            return MarketPhase("COMPRESSION", 0.3, "SKIP", None)
        
        try:
            # Simple price structure analysis
            prices = df_1h['close'].values[-20:]
            highs = df_1h['high'].values[-20:]
            lows = df_1h['low'].values[-20:]
            
            # Trend detection
            x = np.arange(len(prices))
            slope, _ = np.polyfit(x, prices, 1)
            slope_pct = slope / prices[0] * 100
            
            # Volatility structure
            price_range = np.max(highs) - np.min(lows)
            avg_range = np.mean(highs[-10:] - lows[-10:])
            range_ratio = price_range / avg_range if avg_range > 0 else 1
            
            # Determine phase
            if abs(slope_pct) > 2.0:  # Strong trend
                if slope_pct > 0:
                    phase = "IMPULSE_UP"
                    bias = "LONG"
                else:
                    phase = "IMPULSE_DOWN"
                    bias = "SHORT"
                confidence = min(abs(slope_pct) / 5.0, 1.0)
                
            elif range_ratio < 1.5:  # Compression
                phase = "COMPRESSION"
                bias = "SKIP"
                confidence = 0.5
                
            else:  # Correction
                # Check if correcting from up or down trend
                prev_trend = self._detect_previous_trend(df_1h)
                if prev_trend == "UP":
                    phase = "CORRECTION_DOWN"
                    bias = "LONG"  # Correction down → prepare for LONG
                else:
                    phase = "CORRECTION_UP"
                    bias = "SHORT"  # Correction up → prepare for SHORT
                confidence = 0.7
            
            # Simple wave count in current phase
            wave_count = self._count_simple_waves(df_1h, phase)
            
            return MarketPhase(phase, confidence, bias, wave_count)
            
        except Exception as e:
            log.error(f"Phase analysis error: {e}")
            return MarketPhase("COMPRESSION", 0.3, "SKIP", None)
    
    def _detect_previous_trend(self, df: pd.DataFrame) -> str:
        """Detect previous trend before current action"""
        if len(df) < 40:
            return "NEUTRAL"
        
        # Look at earlier period
        earlier_prices = df['close'].values[-40:-20]
        if len(earlier_prices) < 10:
            return "NEUTRAL"
        
        x = np.arange(len(earlier_prices))
        slope, _ = np.polyfit(x, earlier_prices, 1)
        
        if slope > 0:
            return "UP"
        elif slope < 0:
            return "DOWN"
        else:
            return "NEUTRAL"
    
    def _count_simple_waves(self, df: pd.DataFrame, phase: str) -> Optional[int]:
        """Simple wave count based on swings"""
        try:
            if len(df) < 30:
                return None
            
            highs = df['high'].values[-30:]
            lows = df['low'].values[-30:]
            
            # Find swing points
            swing_count = 0
            for i in range(2, len(highs)-2):
                if highs[i] > highs[i-1] and highs[i] > highs[i+1]:
                    swing_count += 1
                elif lows[i] < lows[i-1] and lows[i] < lows[i+1]:
                    swing_count += 1
            
            # Count waves in current phase
            if phase.startswith("CORRECTION"):
                # Corrections typically have 3 waves (A-B-C)
                return min(swing_count // 2, 3)
            elif phase.startswith("IMPULSE"):
                # Impulses typically 5 waves
                return min(swing_count // 2, 5)
            
            return None
            
        except Exception as e:
            return None
    
    # ========== MID TIMEFRAME: WAVE IDENTIFICATION ==========
    
    def analyze_wave_structure(self, df_15m: pd.DataFrame, bias: str) -> WaveStructure:
        """MTF: Identify wave structure and correction completion"""
        
        if df_15m is None or len(df_15m) < 30:
            return self._get_default_wave_structure()
        
        try:
            # Analyze recent price action for wave behavior
            recent_data = df_15m.iloc[-30:]
            
            # Calculate behavioral metrics
            candle_speed_trend = self._calculate_candle_speed_trend(recent_data)
            body_size_trend = self._calculate_body_size_trend(recent_data)
            volume_trend = self._calculate_volume_trend(recent_data)
            momentum_trend = self._calculate_momentum_trend(recent_data)
            
            # Determine wave type based on behavior
            wave_type = self._determine_wave_type(
                candle_speed_trend, body_size_trend, 
                volume_trend, momentum_trend
            )
            
            # Calculate wave characteristics
            wave_length = self._calculate_wave_length(recent_data)
            symmetry_score = self._calculate_symmetry(recent_data, bias)
            compression_level = self._calculate_compression(recent_data)
            expansion_potential = self._calculate_expansion_potential(
                wave_type, compression_level, momentum_trend
            )
            
            return WaveStructure(
                wave_type=wave_type,
                wave_length=wave_length,
                symmetry_score=symmetry_score,
                compression_level=compression_level,
                expansion_potential=expansion_potential,
                candle_speed_trend=candle_speed_trend,
                body_size_trend=body_size_trend,
                volume_trend=volume_trend,
                momentum_trend=momentum_trend
            )
            
        except Exception as e:
            log.error(f"Wave structure error: {e}")
            return self._get_default_wave_structure()
    
    def _calculate_candle_speed_trend(self, df: pd.DataFrame) -> float:
        """Calculate trend in candle speed (-1 to 1)"""
        try:
            if len(df) < 10:
                return 0.0
            
            # Calculate speeds for two halves
            half = len(df) // 2
            first_half = df.iloc[:half]
            second_half = df.iloc[half:]
            
            def avg_speed(data):
                ranges = data['high'] - data['low']
                closes = data['close']
                speeds = ranges / closes * 100
                return speeds.mean() if len(speeds) > 0 else 0.5
            
            speed1 = avg_speed(first_half)
            speed2 = avg_speed(second_half)
            
            # Normalize trend
            if speed1 == 0:
                return 0.0
            
            trend = (speed2 - speed1) / speed1
            return max(min(trend, 1.0), -1.0)
            
        except Exception as e:
            return 0.0
    
    def _calculate_body_size_trend(self, df: pd.DataFrame) -> float:
        """Calculate trend in body size (-1 to 1)"""
        try:
            if len(df) < 10:
                return 0.0
            
            half = len(df) // 2
            first_half = df.iloc[:half]
            second_half = df.iloc[half:]
            
            def avg_body_ratio(data):
                bodies = abs(data['close'] - data['open'])
                ranges = data['high'] - data['low']
                ratios = bodies / ranges
                ratios = ratios.replace([np.inf, -np.inf], np.nan).dropna()
                return ratios.mean() if len(ratios) > 0 else 0.5
            
            ratio1 = avg_body_ratio(first_half)
            ratio2 = avg_body_ratio(second_half)
            
            if ratio1 == 0:
                return 0.0
            
            trend = (ratio2 - ratio1) / ratio1
            return max(min(trend, 1.0), -1.0)
            
        except Exception as e:
            return 0.0
    
    def _calculate_volume_trend(self, df: pd.DataFrame) -> float:
        """Calculate trend in volume (-1 to 1)"""
        try:
            if len(df) < 10:
                return 0.0
            
            half = len(df) // 2
            volume1 = df['volume'].iloc[:half].mean()
            volume2 = df['volume'].iloc[half:].mean()
            
            if volume1 == 0:
                return 0.0
            
            trend = (volume2 - volume1) / volume1
            return max(min(trend, 1.0), -1.0)
            
        except Exception as e:
            return 0.0
    
    def _calculate_momentum_trend(self, df: pd.DataFrame) -> float:
        """Calculate trend in momentum (-1 to 1)"""
        try:
            if len(df) < 10:
                return 0.0
            
            # Use RSI slope
            rsi = self._calculate_rsi(df['close'])
            if len(rsi) < 10:
                return 0.0
            
            half = len(rsi) // 2
            rsi1 = rsi.iloc[:half].mean()
            rsi2 = rsi.iloc[half:].mean()
            
            # Momentum cooling = RSI moving toward 50
            # Momentum heating = RSI moving away from 50
            momentum1 = abs(rsi1 - 50)
            momentum2 = abs(rsi2 - 50)
            
            if momentum1 == 0:
                return 0.0
            
            trend = (momentum2 - momentum1) / momentum1
            return max(min(trend, 1.0), -1.0)
            
        except Exception as e:
            return 0.0
    
    def _calculate_rsi(self, prices: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
        """Calculate RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _determine_wave_type(self, speed_trend: float, body_trend: float,
                            volume_trend: float, momentum_trend: float) -> str:
        """Determine wave type based on behavior"""
        
        # Correction behavior: everything slowing down
        if (speed_trend < -0.3 and body_trend < -0.3 and 
            volume_trend < -0.3 and momentum_trend < -0.2):
            return "CORRECTION_COMPLETE"
        
        # Correction maturing: some slowing
        elif (speed_trend < 0 and body_trend < 0 and 
              volume_trend < 0 and momentum_trend < 0):
            return "CORRECTION_MATURING"
        
        # Impulse start: everything accelerating
        elif (speed_trend > 0.3 and body_trend > 0.3 and 
              volume_trend > 0.3 and momentum_trend > 0.2):
            return "IMPULSE_START"
        
        else:
            return "UNCLEAR"
    
    def _calculate_wave_length(self, df: pd.DataFrame) -> float:
        """Calculate normalized wave length (0-1)"""
        try:
            if len(df) < 20:
                return 0.5
            
            # Count significant moves
            prices = df['close'].values
            significant_moves = 0
            
            for i in range(1, len(prices)):
                change_pct = abs(prices[i] - prices[i-1]) / prices[i-1] * 100
                if change_pct > 0.5:  # 0.5% minimum for significant move
                    significant_moves += 1
            
            # Normalize (20 candles max for analysis window)
            wave_length = min(significant_moves / 10, 1.0)
            return wave_length
            
        except Exception as e:
            return 0.5
    
    def _calculate_symmetry(self, df: pd.DataFrame, bias: str) -> float:
        """Calculate wave symmetry (0-1)"""
        try:
            if len(df) < 10:
                return 0.5
            
            # Simple symmetry: compare upward vs downward moves
            prices = df['close'].values
            up_moves = 0
            down_moves = 0
            
            for i in range(1, len(prices)):
                if prices[i] > prices[i-1]:
                    up_moves += 1
                elif prices[i] < prices[i-1]:
                    down_moves += 1
            
            total_moves = up_moves + down_moves
            if total_moves == 0:
                return 0.5
            
            symmetry = min(up_moves, down_moves) / max(up_moves, down_moves)
            
            # Adjust for bias
            if bias == "LONG" and up_moves > down_moves:
                symmetry *= 1.2  # Favor symmetry in direction of bias
            elif bias == "SHORT" and down_moves > up_moves:
                symmetry *= 1.2
            
            return min(symmetry, 1.0)
            
        except Exception as e:
            return 0.5
    
    def _calculate_compression(self, df: pd.DataFrame) -> float:
        """Calculate compression level (0-1)"""
        try:
            if len(df) < 10:
                return 0.5
            
            # Compression = low range + declining volume
            ranges = df['high'] - df['low']
            avg_range = ranges.mean()
            recent_range = ranges.iloc[-5:].mean()
            
            volume_trend = self._calculate_volume_trend(df.iloc[-10:])
            
            if avg_range > 0:
                range_compression = recent_range / avg_range
            else:
                range_compression = 0.5
            
            # Combine range compression with volume trend
            compression = (range_compression + (1 - abs(volume_trend))) / 2
            return min(compression, 1.0)
            
        except Exception as e:
            return 0.5
    
    def _calculate_expansion_potential(self, wave_type: str, 
                                      compression: float, momentum_trend: float) -> float:
        """Calculate potential for expansion (0-1)"""
        
        if wave_type == "CORRECTION_COMPLETE":
            # High compression + momentum ready to turn = high potential
            potential = (1 - compression) * (1 + momentum_trend) / 2
            return min(potential, 1.0)
        
        elif wave_type == "IMPULSE_START":
            # Already expanding
            return 0.8
        
        else:
            return 0.3
    
    def _get_default_wave_structure(self) -> WaveStructure:
        return WaveStructure(
            wave_type="UNCLEAR",
            wave_length=0.5,
            symmetry_score=0.5,
            compression_level=0.5,
            expansion_potential=0.3,
            candle_speed_trend=0.0,
            body_size_trend=0.0,
            volume_trend=0.0,
            momentum_trend=0.0
        )
    
    # ========== LOW TIMEFRAME: ENERGY TRANSITION ==========
    
    def detect_energy_transition(self, df_5m: pd.DataFrame, bias: str) -> EnergyTransition:
        """LTF: Detect first impulse candle after correction"""
        
        if df_5m is None or len(df_5m) < 20:
            return EnergyTransition(False, None, None, 0, 0, False, False, 0, 0, 0, 0)
        
        try:
            # Find correction period (last N candles)
            correction_period = self._identify_correction_period(df_5m)
            
            if correction_period is None:
                return EnergyTransition(False, None, None, 0, 0, False, False, 0, 0, 0, 0)
            
            # Check for first impulse candle AFTER correction
            impulse_candle_idx = self._find_first_impulse_candle(df_5m, correction_period, bias)
            
            if impulse_candle_idx is None:
                return EnergyTransition(False, None, None, 0, 0, False, False, 0, 0, 0, 0)
            
            # Analyze the transition
            transition_analysis = self._analyze_transition(
                df_5m, correction_period, impulse_candle_idx, bias
            )
            
            return transition_analysis
            
        except Exception as e:
            log.error(f"Transition detection error: {e}")
            return EnergyTransition(False, None, None, 0, 0, False, False, 0, 0, 0, 0)
    
    def _identify_correction_period(self, df: pd.DataFrame) -> Optional[Tuple[int, int]]:
        """Identify start and end of correction period"""
        try:
            if len(df) < 10:
                return None
            
            # Look for slowing behavior in last N candles
            for lookback in range(MAX_CORRECTION_CANDLES, MIN_CORRECTION_CANDLES - 1, -1):
                if lookback > len(df):
                    continue
                
                correction_data = df.iloc[-lookback:]
                
                # Calculate trends in correction period
                speed_trend = self._calculate_candle_speed_trend(correction_data)
                body_trend = self._calculate_body_size_trend(correction_data)
                volume_trend = self._calculate_volume_trend(correction_data)
                
                # Check if shows correction behavior
                if (speed_trend < -0.2 and body_trend < -0.2 and 
                    volume_trend < -0.2):
                    start_idx = len(df) - lookback
                    end_idx = len(df) - 1
                    return (start_idx, end_idx)
            
            return None
            
        except Exception as e:
            return None
    
    def _find_first_impulse_candle(self, df: pd.DataFrame, 
                                  correction_period: Tuple[int, int],
                                  bias: str) -> Optional[int]:
        """Find first impulse candle after correction"""
        
        correction_end = correction_period[1]
        
        # Look at candles immediately after correction
        lookahead = min(MAX_IMPULSE_AGE, len(df) - correction_end - 1)
        
        for i in range(1, lookahead + 1):
            candle_idx = correction_end + i
            
            if candle_idx >= len(df):
                break
            
            candle = df.iloc[candle_idx]
            prev_candle = df.iloc[candle_idx - 1]
            
            # Check if this candle shows impulse behavior
            is_impulse = self._is_impulse_candle(candle, prev_candle, bias)
            
            if is_impulse:
                return candle_idx
        
        return None
    
    def _is_impulse_candle(self, candle: pd.Series, prev_candle: pd.Series, 
                          bias: str) -> bool:
        """Check if candle shows impulse behavior"""
        
        candle_range = candle['high'] - candle['low']
        candle_body = abs(candle['close'] - candle['open'])
        
        # Must be fast candle
        if candle['close'] == 0:
            return False
        
        speed = candle_range / candle['close'] * 100
        if speed < 0.5:  # 0.5% minimum range
            return False
        
        # Body must dominate
        if candle_range > 0:
            body_ratio = candle_body / candle_range
            if body_ratio < 0.5:  # 50% minimum body dominance
                return False
        
        # Check direction matches bias
        if bias == "LONG":
            if candle['close'] <= candle['open']:
                return False  # Not bullish
            # Check for significant upward move
            if candle['close'] <= prev_candle['close'] * 1.002:  # Less than 0.2% up
                return False
                
        elif bias == "SHORT":
            if candle['close'] >= candle['open']:
                return False  # Not bearish
            # Check for significant downward move
            if candle['close'] >= prev_candle['close'] * 0.998:  # Less than 0.2% down
                return False
        
        return True
    
    def _analyze_transition(self, df: pd.DataFrame, correction_period: Tuple[int, int],
                           impulse_idx: int, bias: str) -> EnergyTransition:
        """Analyze the correction-to-impulse transition"""
        
        correction_start, correction_end = correction_period
        impulse_candle = df.iloc[impulse_idx]
        
        # Calculate correction metrics
        correction_data = df.iloc[correction_start:correction_end+1]
        correction_candle_speed = self._calculate_candle_speed_trend(correction_data)
        correction_body_ratio = self._calculate_body_size_trend(correction_data)
        
        # Calculate impulse candle metrics
        impulse_range = impulse_candle['high'] - impulse_candle['low']
        impulse_body = abs(impulse_candle['close'] - impulse_candle['open'])
        
        if impulse_range > 0:
            impulse_body_ratio = impulse_body / impulse_range
        else:
            impulse_body_ratio = 0.5
        
        if impulse_candle['close'] > 0:
            impulse_speed = impulse_range / impulse_candle['close'] * 100
        else:
            impulse_speed = 0.5
        
        # Volume expansion check
        correction_volume = correction_data['volume'].mean()
        impulse_volume = impulse_candle['volume']
        
        if correction_volume > 0:
            volume_expansion = impulse_volume / correction_volume
        else:
            volume_expansion = 1.0
        
        # RSI momentum turn check
        rsi_turn = self._check_rsi_turn(df, impulse_idx, bias)
        
        # EMA structure hold check
        ema_hold = self._check_ema_structure(df, correction_period, bias)
        
        # Candle dominance (impulse vs correction)
        candle_dominance = impulse_body_ratio
        
        return EnergyTransition(
            has_transition=True,
            transition_type="CORRECTION_TO_IMPULSE",
            first_impulse_candle_index=impulse_idx,
            candle_dominance=candle_dominance,
            volume_expansion=volume_expansion,
            momentum_turn=rsi_turn,
            ema_structure_hold=ema_hold,
            impulse_candle_speed=impulse_speed,
            impulse_body_ratio=impulse_body_ratio,
            correction_candle_speed=abs(correction_candle_speed),
            correction_body_ratio=abs(correction_body_ratio)
        )
    
    def _check_rsi_turn(self, df: pd.DataFrame, impulse_idx: int, bias: str) -> bool:
        """Check if RSI turned with price"""
        try:
            if len(df) < impulse_idx + 1:
                return False
            
            # Calculate RSI
            rsi_series = self._calculate_rsi(df['close'])
            
            if len(rsi_series) < impulse_idx + 1:
                return False
            
            current_rsi = rsi_series.iloc[impulse_idx]
            prev_rsi = rsi_series.iloc[impulse_idx - 1] if impulse_idx > 0 else 50
            
            if bias == "LONG":
                # RSI should be turning up from oversold/neutral
                return current_rsi > prev_rsi and current_rsi < 70
            
            elif bias == "SHORT":
                # RSI should be turning down from overbought/neutral
                return current_rsi < prev_rsi and current_rsi > 30
            
            return False
            
        except Exception as e:
            return False
    
    def _check_ema_structure(self, df: pd.DataFrame, 
                            correction_period: Tuple[int, int], bias: str) -> bool:
        """Check if EMA held structure during correction"""
        try:
            correction_start, correction_end = correction_period
            correction_data = df.iloc[correction_start:correction_end+1]
            
            # Calculate EMAs
            ema9 = correction_data['close'].ewm(span=EMA_PERIODS[0], adjust=False).mean()
            ema21 = correction_data['close'].ewm(span=EMA_PERIODS[1], adjust=False).mean()
            
            # Check if price respected EMA during correction
            if bias == "LONG":
                # In correction down, price should not break below EMA significantly
                prices = correction_data['close'].values
                ema9_vals = ema9.values
                
                for i in range(len(prices)):
                    if ema9_vals[i] > 0:
                        distance = abs(prices[i] - ema9_vals[i]) / ema9_vals[i]
                        if distance > EMA_RESPECT_DISTANCE * 2:  # Allow some leeway
                            return False
                return True
                
            elif bias == "SHORT":
                # In correction up, price should not break above EMA significantly
                prices = correction_data['close'].values
                ema9_vals = ema9.values
                
                for i in range(len(prices)):
                    if ema9_vals[i] > 0:
                        distance = abs(prices[i] - ema9_vals[i]) / ema9_vals[i]
                        if distance > EMA_RESPECT_DISTANCE * 2:
                            return False
                return True
            
            return False
            
        except Exception as e:
            return False
    
    # ========== SIGNAL GENERATION ==========
    
    def generate_wave_signal(self, multi_tf_data: Dict[str, pd.DataFrame], 
                            symbol: str) -> Optional[WaveSignal]:
        """Generate wave-momentum signal ONLY if all conditions align"""
        
        try:
            # 1. Get timeframe data
            tf_1h = multi_tf_data.get("1H")
            tf_15m = multi_tf_data.get("15M")
            tf_5m = multi_tf_data.get("5M")
            
            # Check data availability
            if tf_1h is None or tf_15m is None or tf_5m is None:
                return None
            
            if len(tf_1h) < 20 or len(tf_15m) < 30 or len(tf_5m) < 20:
                return None
            
            # 2. HIGH TIMEFRAME: Permission only
            market_phase = self.analyze_market_phase(tf_1h)
            
            # Skip if no clear permission
            if market_phase.bias_allowed == "SKIP":
                self.trade_stats["phase_skipped"] += 1
                log.debug(f"{symbol}: HTF says SKIP - {market_phase.phase}")
                return None
            
            # 3. MID TIMEFRAME: Wave identification
            wave_structure = self.analyze_wave_structure(tf_15m, market_phase.bias_allowed)
            
            # Skip if wave structure not valid
            if not wave_structure.is_valid_setup:
                self.trade_stats["wave_structure_skipped"] += 1
                log.debug(f"{symbol}: Invalid wave structure - {wave_structure.wave_type}")
                return None
            
            # 4. LOW TIMEFRAME: Energy transition
            energy_transition = self.detect_energy_transition(tf_5m, market_phase.bias_allowed)
            
            # Skip if no valid transition
            if not energy_transition.is_valid_entry:
                self.trade_stats["transition_skipped"] += 1
                log.debug(f"{symbol}: No valid energy transition")
                return None
            
            # 5. Calculate entry parameters
            current_price = tf_5m['close'].iloc[-1]
            entry_price = current_price  # Enter at current price (first impulse)
            
            # Invalidation based on EMA structure
            ema9 = tf_5m['close'].ewm(span=EMA_PERIODS[0], adjust=False).mean().iloc[-1]
            
            if market_phase.bias_allowed == "LONG":
                invalidation_price = ema9 * (1 - EMA_RESPECT_DISTANCE)
                # Expansion target based on wave structure
                expansion_multiplier = 1.0 + wave_structure.expansion_potential * 0.05  # 0-5%
                expansion_target = entry_price * expansion_multiplier
            else:  # SHORT
                invalidation_price = ema9 * (1 + EMA_RESPECT_DISTANCE)
                expansion_multiplier = 1.0 - wave_structure.expansion_potential * 0.05
                expansion_target = entry_price * expansion_multiplier
            
            # Calculate strength metrics
            impulse_strength = energy_transition.impulse_body_ratio
            volume_confidence = min(energy_transition.volume_expansion / 3.0, 1.0)  # Cap at 3x
            structure_quality = wave_structure.symmetry_score * wave_structure.expansion_potential
            
            # Calculate correction duration
            correction_duration = energy_transition.first_impulse_candle_index - len(tf_5m) + 20
            impulse_age = len(tf_5m) - energy_transition.first_impulse_candle_index - 1
            
            # 6. Create signal
            signal_id = hashlib.md5(
                f"{symbol}:{market_phase.bias_allowed}:{entry_price}:{time.time()}".encode()
            ).hexdigest()
            
            signal = WaveSignal(
                signal_id=signal_id,
                symbol=symbol,
                side=market_phase.bias_allowed,
                
                entry_price=entry_price,
                invalidation_price=invalidation_price,
                expansion_target=expansion_target,
                
                market_phase=market_phase,
                wave_structure=wave_structure,
                energy_transition=energy_transition,
                
                impulse_strength=impulse_strength,
                volume_confidence=volume_confidence,
                structure_quality=structure_quality,
                
                higher_timeframe="1H",
                wave_timeframe="15M",
                entry_timeframe="5M",
                
                correction_duration=correction_duration,
                impulse_age=impulse_age,
                signal_timestamp=time.time()
            )
            
            # 7. Final quality check
            if signal.trade_quality < 0.7:
                log.debug(f"{symbol}: Quality too low ({signal.trade_quality:.2f})")
                return None
            
            # 8. Update statistics
            self.trade_stats["quality_signals"] += 1
            self.trade_stats["impulse_entries"] += 1
            
            # 9. Log the high-quality signal
            log.info(f"🌊 WAVE-MOMENTUM SIGNAL: {symbol} {signal.side}")
            log.info(f"   Phase: {market_phase.phase} ({market_phase.confidence:.1%})")
            log.info(f"   Wave: {wave_structure.wave_type}, Length: {wave_structure.wave_length:.2f}")
            log.info(f"   Transition: First impulse candle detected")
            log.info(f"   Quality: {signal.trade_quality:.1%}")
            log.info(f"   Target: {expansion_target/entry_price*100-100:+.1f}% expansion")
            
            return signal
            
        except Exception as e:
            log.error(f"Signal generation error for {symbol}: {e}")
            return None

# ================ MAIN SCANNER SYSTEM ================
class WaveMomentumScanner:
    """Main scanner system for wave-momentum trading"""
    
    def __init__(self):
        self.trader = WaveMomentumTrader()
        self.exchange = None
        self.db = None
        self.scan_cycle = 0
        
    async def initialize(self):
        """Initialize the scanner"""
        log.info("=" * 70)
        log.info("🌊 WAVE-MOMENTUM TRADING SYSTEM - ENERGY TRANSITION HUNTER")
        log.info("=" * 70)
        log.info("TRADER ROLE: Discretionary wave-energy specialist")
        log.info("SPECIALTY: First expansion wave after correction")
        log.info("PHILOSOPHY: Trade energy transitions, not predictions")
        log.info("ASSET FOCUS: Low-price altcoins ($0-$10), inefficient markets")
        log.info("TIMEFRAMES: 1H (permission), 15M (wave), 5M (entry)")
        log.info("ENTRY RULE: First impulse candle ONLY after correction")
        log.info("QUALITY FILTER: If unclear → NO SIGNAL")
        log.info("=" * 70)
        
        # Initialize database
        await self._init_database()
        
        # Initialize exchange
        await self._init_exchange()
        
        # Send startup message
        await self._send_startup_message()
    
    async def _init_database(self):
        """Initialize database for wave signals"""
        try:
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            self.db = await aiosqlite.connect(DB_PATH)
            
            # Wave signals table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS wave_signals (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                invalidation_price REAL NOT NULL,
                expansion_target REAL NOT NULL,
                
                market_phase TEXT NOT NULL,
                phase_confidence REAL NOT NULL,
                wave_type TEXT NOT NULL,
                wave_length REAL NOT NULL,
                expansion_potential REAL NOT NULL,
                
                impulse_strength REAL NOT NULL,
                volume_confidence REAL NOT NULL,
                structure_quality REAL NOT NULL,
                trade_quality REAL NOT NULL,
                
                correction_duration INTEGER NOT NULL,
                impulse_age INTEGER NOT NULL,
                
                status TEXT DEFAULT 'PENDING',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                triggered_at TIMESTAMP,
                trigger_price REAL,
                
                closed_at TIMESTAMP,
                close_price REAL,
                pnl_percent REAL,
                close_reason TEXT,
                
                wave_win INTEGER DEFAULT 0
            )
            """)
            
            # Performance table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS wave_performance (
                date DATE PRIMARY KEY,
                assets_scanned INTEGER,
                phase_skipped INTEGER,
                wave_structure_skipped INTEGER,
                transition_skipped INTEGER,
                quality_signals INTEGER,
                impulse_entries INTEGER,
                wave_wins INTEGER,
                wave_losses INTEGER,
                win_rate REAL,
                avg_expansion REAL,
                avg_quality REAL
            )
            """)
            
            await self.db.commit()
            
            log.info("✅ Database initialized for wave trading")
            
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
🌊 <b>WAVE-MOMENTUM TRADER ACTIVATED</b>

<b>🎯 CORE PHILOSOPHY:</b>
• Trade ENERGY TRANSITIONS, not predictions
• First expansion wave after correction ONLY
• If unclear → NO SIGNAL (discipline > frequency)

<b>🧠 TIMEFRAME ROLES:</b>
1️⃣ <b>1H (Permission):</b>
‎   • Detect market phase only
‎   • Set bias: LONG, SHORT, or SKIP
‎   • NO entries here

2️⃣ <b>15M (Wave Identification):</b>
‎   • Detect correction completion
‎   • Measure wave symmetry & compression
‎   • Confirm expansion potential exists

3️⃣ <b>5M (Execution):</b>
‎   • Detect FIRST impulse candle
‎   • Tight invalidation at EMA structure
‎   • Exact entry timing

<b>⚡ ENTRY CONDITIONS (ALL MUST BE TRUE):</b>
• Correction complete or ending
• First impulsive candle appears
• Candle body shows dominance (>{MIN_BODY_DOMINANCE*100:.0f}%)
• Volume expands on impulse (>{MIN_VOLUME_EXPANSION:.1f}x)
• RSI turns with price
• EMA holds structure during correction

<b>🎯 ASSET SELECTION:</b>
• Price: ${MAX_PRICE_USDT} maximum
• Volume: ${MIN_VOLUME_USD:,.0f} - ${MAX_VOLUME_USD:,.0f}
• Focus on low-price, inefficient altcoins

<b>🚫 FORBIDDEN:</b>
• Trading every breakout
• Chasing price
• Entering mid-wave
• Fixed indicator thresholds
• Forcing signals for frequency

<b>🧠 TRADER MINDSET:</b>
I am a wave-energy specialist hunting the first expansion after compression.
I wait patiently for clear energy transitions.
I enter early when momentum shifts.
I exit during impulse continuation, not at reversals.

#WaveMomentum #EnergyTransitions #FirstImpulse #QualityOverQuantity
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
        
        for tf_name, tf_config in TIMEFRAMES.items():
            try:
                tf = tf_config["tf"]
                limit = tf_config["candles"]
                
                ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
                
                if ohlcv and len(ohlcv) >= 20:
                    df = pd.DataFrame(
                        ohlcv,
                        columns=["timestamp", "open", "high", "low", "close", "volume"]
                    )
                    
                    # Convert to numeric
                    for col in ["open", "high", "low", "close", "volume"]:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    df = df.dropna()
                    
                    if len(df) >= 15:
                        data[tf_name] = df
                    
            except Exception as e:
                log.debug(f"{symbol} {tf_name}: {str(e)[:50]}")
                continue
        
        return data
    
    async def get_wave_assets(self) -> List[str]:
        """Get assets for wave trading (low-price, inefficient)"""
        try:
            # Get all USDT pairs
            markets = await self.exchange.fetch_markets()
            all_symbols = [m['symbol'] for m in markets if m['quote'] == 'USDT']
            
            # Filter using trader's asset selection
            selected = await self.trader.select_wave_assets(self.exchange, all_symbols)
            
            log.info(f"🌊 Selected {len(selected)} wave-trading assets")
            return selected
            
        except Exception as e:
            log.error(f"Error getting assets: {e}")
            return []
    
    async def save_wave_signal(self, signal: WaveSignal) -> bool:
        """Save wave signal to database"""
        try:
            await self.db.execute("""
                INSERT INTO wave_signals (
                    id, symbol, side, entry_price, invalidation_price, expansion_target,
                    market_phase, phase_confidence, wave_type, wave_length, expansion_potential,
                    impulse_strength, volume_confidence, structure_quality, trade_quality,
                    correction_duration, impulse_age
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.side,
                signal.entry_price,
                signal.invalidation_price,
                signal.expansion_target,
                signal.market_phase.phase,
                signal.market_phase.confidence,
                signal.wave_structure.wave_type,
                signal.wave_structure.wave_length,
                signal.wave_structure.expansion_potential,
                signal.impulse_strength,
                signal.volume_confidence,
                signal.structure_quality,
                signal.trade_quality,
                signal.correction_duration,
                signal.impulse_age
            ))
            
            await self.db.commit()
            
            log.info(f"✅ Wave signal saved: {signal.symbol} (Quality: {signal.trade_quality:.1%})")
            return True
            
        except Exception as e:
            log.error(f"Error saving wave signal: {e}")
            return False
    
    async def format_wave_message(self, signal: WaveSignal) -> str:
        """Format wave signal for Telegram"""
        side_emoji = "🟢" if signal.side == "LONG" else "🔴"
        side_text = "شراء" if signal.side == "LONG" else "بيع"
        
        # Calculate expected expansion
        expected_expansion = (signal.expansion_target / signal.entry_price - 1) * 100
        
        message = f"""
{side_emoji} <b>إشارة موجة دفع - ENERGY TRANSITION</b> 🌊

<b>{signal.symbol}</b> | {side_text}

<b>🎯 فلسفة الدخول:</b>
الموجة التصحيحية انتهت ← أول شمعة دفع ظهرت
نصطاد انتقال الطاقة، لا نتوقع

<b>📊 تحليل الموجة:</b>
‎• المرحلة: {signal.market_phase.phase} ({signal.market_phase.confidence:.1%})
‎• نوع الموجة: {signal.wave_structure.wave_type}
‎• طول الموجة: {signal.wave_structure.wave_length:.2f}
‎• إمكانية التوسع: {signal.wave_structure.expansion_potential:.1%}

<b>⚡ انتقال الطاقة:</b>
‎• أول شمعة دفع بعد {signal.correction_duration} شمعة تصحيح
‎• عمر الدفع: {signal.impulse_age} شموع (مبكر جداً)
‎• قوة الدفع: {signal.impulse_strength:.1%}
‎• تأكيد الفوليوم: {signal.volume_confidence:.1%}

<b>🎯 التنفيذ:</b>
‎• سعر الدخول: <code>{signal.entry_price:.6f}</code>
‎• مستوى الإبطال: <code>{signal.invalidation_price:.6f}</code>
‎• هدف التوسع: <code>{signal.expansion_target:.6f}</code> ({expected_expansion:+.1f}%)

<b>📈 جودة الصفقة:</b>
‎• الجودة الكلية: {signal.trade_quality:.1%}
‎• جودة الهيكل: {signal.structure_quality:.1%}
‎• ثقة الفوليوم: {signal.volume_confidence:.1%}

<b>🧠 عقلية الموجة:</b>
انتقال الطاقة تم اكتشافه
الدخول عند أول شمعة دفع
الإبطال عند كسر هيكل الموجة
الهدف هو توسع الموجة، ليس رقم ثابت

<b>⚠️ ملاحظة التاجر:</b>
هذه إشارة موجية عالية الجودة
الدخول مبكر عند انتقال الطاقة
لا مطاردة ← إذا فاتتك الموجة، انتظر الموجة القادمة

#{side_text} #موجة_دفع #انتقال_طاقة #أول_شمعة #جودة_عالية
"""
        return message
    
    async def send_wave_alert(self, signal: WaveSignal):
        """Send Telegram alert for wave signals"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning(f"⚠️ Telegram credentials missing. Skipping alert for {signal.symbol}")
            return
        
        try:
            message = await self.format_wave_message(signal)
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info(f"📤 Wave alert sent: {signal.symbol}")
            
        except Exception as e:
            log.error(f"Telegram error: {e}")
    
    async def monitor_wave_positions(self):
        """Monitor wave positions with energy-based exits"""
        log.info("👀 Starting wave position monitoring...")
        
        while True:
            try:
                # Get open wave positions
                async with self.db.execute("""
                    SELECT id, symbol, side, entry_price, invalidation_price, 
                           expansion_target, trade_quality, wave_type
                    FROM wave_signals 
                    WHERE status = 'PENDING'
                """) as cursor:
                    positions = await cursor.fetchall()
                
                for pos_id, symbol, side, entry, invalidation, target, quality, wave_type in positions:
                    try:
                        # Get current price
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        # Check if price reached entry (wave entries are immediate)
                        # For wave trading, we enter immediately at detection
                        if abs(current_price - entry) / entry <= 0.01:  # Within 1%
                            # Mark as triggered
                            await self.db.execute("""
                                UPDATE wave_signals SET 
                                    status = 'TRIGGERED',
                                    triggered_at = CURRENT_TIMESTAMP,
                                    trigger_price = ?
                                WHERE id = ?
                            """, (current_price, pos_id))
                            
                            await self.db.commit()
                            
                            log.info(f"✅ Wave position triggered: {symbol} {side} @ {current_price:.4f}")
                        
                        # For triggered positions, check exit conditions
                        async with self.db.execute("""
                            SELECT id FROM wave_signals 
                            WHERE id = ? AND status = 'TRIGGERED'
                        """, (pos_id,)) as cursor:
                            triggered = await cursor.fetchone()
                        
                        if triggered:
                            await self._check_wave_exit(
                                pos_id, symbol, side, entry, invalidation, 
                                target, current_price, quality, wave_type
                            )
                    
                    except Exception as e:
                        log.error(f"Monitor error for {symbol}: {e}")
                        continue
                
                await asyncio.sleep(3)  # Check every 3 seconds
                
            except Exception as e:
                log.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(5)
    
    async def _check_wave_exit(self, pos_id: str, symbol: str, side: str, 
                              entry: float, invalidation: float, target: float,
                              current_price: float, quality: float, wave_type: str):
        """Check exit conditions for wave trade"""
        
        # Calculate P&L
        if side == "LONG":
            pnl_pct = ((current_price - entry) / entry) * 100
            is_invalidated = current_price <= invalidation
            is_target_reached = current_price >= target
        else:  # SHORT
            pnl_pct = ((entry - current_price) / entry) * 100
            is_invalidated = current_price >= invalidation
            is_target_reached = current_price <= target
        
        # Energy-based exit logic
        should_exit = False
        close_reason = ""
        is_wave_win = False
        
        # 1. Wave invalidation (structure broken)
        if is_invalidated:
            should_exit = True
            close_reason = "WAVE_INVALIDATED"
            is_wave_win = False
        
        # 2. Expansion target reached (wave completed)
        elif is_target_reached:
            should_exit = True
            close_reason = "EXPANSION_COMPLETE"
            is_wave_win = True
        
        # 3. Momentum decay (energy fading)
        elif abs(pnl_pct) > 1.0:  # Only check after some move
            # Get recent data to check momentum
            try:
                ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe="5m", limit=10)
                if len(ohlcv) >= 5:
                    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                    
                    # Check if momentum is fading
                    recent_prices = df['close'].values
                    if len(recent_prices) >= 3:
                        # Simple momentum check
                        momentum = abs(recent_prices[-1] - recent_prices[-3]) / recent_prices[-3] * 100
                        entry_momentum = abs(target - entry) / entry * 100
                        
                        if momentum < entry_momentum * 0.3:  # Momentum faded to 30%
                            should_exit = True
                            close_reason = "MOMENTUM_DECAY"
                            is_wave_win = pnl_pct > 0
            except:
                pass
        
        if should_exit:
            # Update database
            await self.db.execute("""
                UPDATE wave_signals SET 
                    status = 'CLOSED',
                    closed_at = CURRENT_TIMESTAMP,
                    close_price = ?,
                    pnl_percent = ?,
                    close_reason = ?,
                    wave_win = ?
                WHERE id = ?
            """, (current_price, pnl_pct, close_reason, 1 if is_wave_win else 0, pos_id))
            
            await self.db.commit()
            
            # Send notification
            await self._send_wave_exit_notification(
                symbol, side, pnl_pct, close_reason, entry, current_price, quality
            )
            
            log.info(f"🌊 Wave trade closed: {symbol} {close_reason} ({pnl_pct:+.2f}%)")
    
    async def _send_wave_exit_notification(self, symbol: str, side: str, pnl_pct: float,
                                          close_reason: str, entry: float, 
                                          close_price: float, quality: float):
        """Send wave exit notification"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            side_text = "شراء" if side == "LONG" else "بيع"
            pnl_formatted = f"+{pnl_pct:.2f}%" if pnl_pct > 0 else f"{pnl_pct:.2f}%"
            
            # Determine emoji based on reason
            if "COMPLETE" in close_reason:
                emoji = "✅"
                result_text = "اكتمال التوسع الموجي"
            elif "INVALIDATED" in close_reason:
                emoji = "❌"
                result_text = "إبطال الموجة"
            else:
                emoji = "⚠️"
                result_text = "ضعف الزخم"
            
            message = f"""
{emoji} <b>إغلاق صفقة موجة</b> 🌊

<b>{symbol}</b> | {side_text}

<b>📊 النتيجة:</b> {result_text}
💰 <b>الأداء:</b> {pnl_formatted}

<b>🎯 تفاصيل الموجة:</b>
‎• جودة الدخول: {quality:.1%}
‎• سعر الدخول: <code>{entry:.6f}</code>
‎• سعر الإغلاق: <code>{close_price:.6f}</code>
‎• السبب: {close_reason}

<b>🧠 فلسجة الموجة:</b>
كل موجة لها بداية ونهاية
نخرج عند اكتمال التوسع أو إبطال الهيكل
نقبل النهاية الطبيعية للموجة

<b>🌊 استعداد للموجة القادمة...</b>

#{side_text} #إغلاق_موجة #{'ربح' if pnl_pct > 0 else 'خسارة'} #استمرارية_الطاقة
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
        except Exception as e:
            log.error(f"Exit notification error: {e}")
    
    async def wave_scanning(self):
        """Main wave scanning loop"""
        log.info("🚀 Starting wave-momentum scanning...")
        
        # FALLBACK ASSETS in case selection fails
        FALLBACK_SYMBOLS = [
            "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
            "ADA/USDT", "AVAX/USDT", "DOT/USDT", "DOGE/USDT", "MATIC/USDT",
            "SHIB/USDT", "TRX/USDT", "LINK/USDT", "UNI/USDT", "ATOM/USDT"
        ]
        
        while True:
            try:
                self.scan_cycle += 1
                start_time = time.time()
                
                log.info(f"🌊 Wave scan cycle #{self.scan_cycle}")
                
                # Get wave assets
                assets = await self.get_wave_assets()
                
                # Use fallback if no assets found
                if not assets:
                    log.warning("No wave assets found, using fallback symbols")
                    assets = FALLBACK_SYMBOLS
                
                log.info(f"Scanning {len(assets)} assets for wave opportunities")
                
                signals_found = 0
                
                # Scan for wave opportunities
                for symbol in assets[:30]:  # Limit to 30 for speed
                    try:
                        # Skip if already has active signal
                        if symbol in self.trader.active_signals:
                            continue
                        
                        # Fetch multi-timeframe data
                        multi_tf_data = await self.fetch_timeframe_data(symbol)
                        
                        # Check if we have all required timeframes
                        required_tfs = ["1H", "15M", "5M"]
                        if not all(tf in multi_tf_data for tf in required_tfs):
                            continue
                        
                        # Generate wave signal
                        signal = self.trader.generate_wave_signal(multi_tf_data, symbol)
                        
                        if signal:
                            # Save and send
                            saved = await self.save_wave_signal(signal)
                            
                            if saved:
                                await self.send_wave_alert(signal)
                                signals_found += 1
                                
                                # Track active signal
                                self.trader.active_signals[signal.symbol] = signal.signal_id
                        
                        # Brief pause between assets
                        await asyncio.sleep(0.05)
                        
                    except Exception as e:
                        log.debug(f"Asset scan error {symbol}: {str(e)[:50]}")
                        continue
                
                # Update statistics
                self.trader.trade_stats["assets_scanned"] += len(assets)
                
                # Log statistics
                stats = self.trader.trade_stats
                active_count = len(self.trader.active_signals)
                
                log.info(f"🌊 Wave stats: Found {signals_found}, Active: {active_count}")
                log.info(f"   Skipped: Phase={stats['phase_skipped']}, "
                        f"Wave={stats['wave_structure_skipped']}, "
                        f"Transition={stats['transition_skipped']}")
                log.info(f"   Quality signals: {stats['quality_signals']}")
                
                scan_duration = time.time() - start_time
                log.info(f"Wave scan #{self.scan_cycle}: {signals_found} signals in {scan_duration:.2f}s")
                
                # Wait for next scan (wave trading needs patience)
                wait_time = max(5, 15 - scan_duration)  # 15s cycle
                log.info(f"Next wave hunt in {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                log.error(f"Wave scanning error: {e}")
                await asyncio.sleep(10)
    
    async def run(self):
        """Run the wave scanner"""
        try:
            await self.initialize()
            
            # Run both loops
            await asyncio.gather(
                self.wave_scanning(),
                self.monitor_wave_positions()
            )
            
        except KeyboardInterrupt:
            log.info("🌊 Wave-momentum scanner stopped by user")
            await self._send_final_wave_stats()
            
        except Exception as e:
            log.error(f"Scanner crashed: {e}")
            
        finally:
            await self.cleanup()
    
    async def _send_final_wave_stats(self):
        """Send final wave statistics"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            stats = self.trader.trade_stats
            
            message = f"""
🌊 <b>إحصائيات نهائية - نظام الموجة الدافعة</b>

<b>📊 أداء المسح:</b>
‎• دورات المسح: {self.scan_cycle}
‎• الأصول الممسوحة: {stats['assets_scanned']}
‎• إشارات الجودة: {stats['quality_signals']}

<b>🚫 عمليات التخطي (دقة النظام):</b>
‎• تخطي المرحلة: {stats['phase_skipped']}
‎• تخطي هيكل الموجة: {stats['wave_structure_skipped']}
‎• تخطي انتقال الطاقة: {stats['transition_skipped']}

<b>🎯 فلسفة محققة:</b>
• تداول انتقالات الطاقة فقط
• أول موجة دفع بعد تصحيح
• إذا غير واضح ← لا إشارة
• الجودة فوق الكمية

<b>🧠 عقلية الموجة المحققة:</b>
كنت صياد موجات
انتظرت انتقالات الطاقة
دخلت عند أول شمعة دفع
خرجت عند اكتمال التوسع

<b>✅ النظام التزم بـ:</b>
• 1H للإذن فقط
• 15M لهيكل الموجة
• 5M للدخول
• دخول عند أول شمعة دفع
• إبطال عند كسر الهيكل

#إحصائيات_الموجة #تداول_الطاقة #نظام_موجة #عقلية_محترف
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
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

# ================ MAIN ================
async def main():
    """Main function"""
    scanner = WaveMomentumScanner()
    await scanner.run()

if __name__ == "__main__":
    asyncio.run(main())