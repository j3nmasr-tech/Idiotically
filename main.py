#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 ELLIOTT WAVE HIGH-FREQUENCY SCALPER
Professional discretionary Elliott Wave trading system
Wave counting + Wave strength + Volume confirmation + Rejection entries
TRADER MINDSET: Wave-based reaction trader
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
DB_PATH = "/app/data/elliott_wave_scanner.db"

# Ultra high-frequency scanning - ELLIOTT WAVE TRADING
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 3))   # 3 seconds - ULTRA FAST FOR WAVE SCALPS
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 80))    # Scan many pairs
MIN_VOLUME_USD = 1000000  # $1M minimum - better wave structure

# Trading parameters (ELLIOTT WAVE-BASED)
MAX_STOP_LOSS_PCT = 1.2    # 1.2% maximum stop loss
MIN_TARGET_PCT = 1.0       # 1% minimum target (sub-wave moves)
MAX_TARGET_PCT = 5.0       # 5% maximum target (impulse waves)
MIN_RISK_REWARD = 1.5      # Minimum 1:1.5 risk/reward

# Elliott Wave scanning
WAVE_CONFIG = {
    "rsi_wave_2_4_zone": (35, 50),     # RSI for Wave 2/4 pullbacks (LONG entries)
    "rsi_wave_3_zone": (60, 75),       # RSI for Wave 3 (strong momentum)
    "rsi_wave_5_zone": (70, 85),       # RSI for Wave 5 (with divergence)
    "min_wave_confidence": 0.65,       # Minimum confidence for wave count
    "max_wave_maturity": 0.85,         # Maximum wave maturity to trade
    "wave_3_volume_multiplier": 1.8,   # Volume should be 1.8x for Wave 3
    "corrective_volume_multiplier": 0.7, # Volume should be lower for corrections
}

# Timeframes for ELLIOTT WAVE ANALYSIS
TIMEFRAMES = {
    "1H": "1h",      # Primary wave degree (Wave 1-5)
    "30M": "30m",    # Intermediate waves
    "15M": "15m",    # Trading waves (main entry)
    "5M": "5m",      # Fast trigger
    "3M": "3m",      # Entry timing (ULTRA FAST)
    "1M": "1m"       # Precision entry
}

# EMA periods for wave structure
EMA_PERIODS = {
    "fast": 9,      # Wave momentum
    "medium": 21,   # Wave structure  
    "slow": 50,     # Primary trend
    "very_slow": 200 # Major trend
}

# RSI settings for wave phases
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# ================ DATA STRUCTURES ================
@dataclass
class WaveContext:
    """Elliott Wave analysis context"""
    wave_type: str              # IMPULSE_1, IMPULSE_3, IMPULSE_5, CORRECTIVE_2, CORRECTIVE_4, CORRECTIVE_A, CORRECTIVE_B, CORRECTIVE_C
    wave_maturity: float        # 0-1 (0=early, 1=complete)
    wave_confidence: float      # 0-1 confidence in wave count
    is_extended: bool           # Is this an extended wave?
    has_divergence: bool        # Wave 5 divergence present?
    
    # Multi-timeframe wave alignment
    higher_tf_wave: str         # Wave on higher timeframe
    lower_tf_wave: str          # Wave on lower timeframe
    subwave_count: int          # Which sub-wave we're in
    
    # Wave characteristics
    wave_strength: float        # 0-1 wave strength (based on volume, momentum)
    trend_direction: str        # BULLISH, BEARISH, NEUTRAL
    fibonacci_level: Optional[float]  # Current Fibonacci level (0.382, 0.5, 0.618, etc.)

@dataclass
class MarketStrength:
    """Market strength analysis specific to Elliott Wave"""
    wave_momentum: float        # 0-1 momentum of current wave
    volume_participation: float # 0-1 volume relative to wave type
    ema_alignment: float        # 0-1 how aligned EMAs are
    price_expansion: float      # 0-1 price expansion in current wave
    
    # Wave-specific flags
    is_wave_3_strength: bool   # Strong move + high volume = Wave 3
    is_wave_5_divergence: bool # Price new high + lower RSI/volume = Wave 5
    is_corrective_setup: bool  # Low volume + shallow move = Wave 2/4
    is_volume_climax: bool     # High volume + little progress = Wave end
    
    # Combined score
    strength_score: float      # 0-1 overall strength

@dataclass
class WaveRejectionZone:
    """Rejection zone in context of Elliott Wave"""
    zone_type: str             # WAVE_2_SUPPORT, WAVE_4_SUPPORT, WAVE_5_RESISTANCE, 
                               # FIBONACCI_38, FIBONACCI_50, FIBONACCI_62,
                               # EMA_CONFLUENCE, TRENDLINE_SUPPORT, TRENDLINE_RESISTANCE
    price_level: float
    wave_context: str          # Which wave this belongs to
    strength: float            # 0-1 rejection strength
    volume_confirmation: bool  # Volume spike at rejection
    rsi_position: str          # WAVE_2_RSI, WAVE_3_RSI, WAVE_5_RSI, etc.
    is_active: bool           # Currently being rejected
    
    # Elliott Wave specific
    fibonacci_level: Optional[float]
    is_impulse: bool          # Impulse wave zone
    is_corrective: bool       # Corrective wave zone

@dataclass
class ElliottWaveSignal:
    """Elliott Wave-based trade signal"""
    signal_id: str
    symbol: str
    side: str                  # LONG, SHORT
    
    # Elliott Wave analysis
    wave_context: WaveContext
    target_wave: str           # Which wave we're targeting (IMPULSE_3, etc.)
    
    # Price levels
    entry_price: float
    stop_loss: float
    take_profit: float
    
    # Analysis context
    market_strength: MarketStrength
    rejection_zone: WaveRejectionZone
    
    # Entry triggers
    trigger_type: str          # WAVE_START, SUBWAVE_ENTRY, DIVERGENCE_ENTRY
    confirmation_candle: str   # Specific candle pattern
    
    # Metrics
    wave_confidence: float     # Confidence in wave count
    risk_reward: float
    expected_move_pct: float   # Based on wave extension
    fibonacci_target: Optional[float]
    
    # Timing
    timeframe_used: str        # Which TF triggered entry
    signal_timestamp: float
    elliott_rules_met: List[str]  # Which Elliott rules are satisfied

# ================ PROFESSIONAL LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("elliott_scanner")

# ================ CORE ELLIOTT WAVE ENGINE ================
class ElliottWaveScanner:
    """High-frequency Elliott Wave scanner"""
    
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
        
        def register_signal(self, signal: ElliottWaveSignal):
            """Register a new signal"""
            symbol = signal.symbol
            
            # Remove any previous active signal for this symbol
            if symbol in self.active_signals:
                old_signal_id = self.active_signals[symbol]
                if old_signal_id in self.signal_status:
                    del self.signal_status[old_signal_id]
            
            # Register new signal as PENDING
            self.active_signals[symbol] = signal.signal_id
            self.signal_status[signal.signal_id] = {
                "symbol": symbol,
                "side": signal.side,
                "price": signal.entry_price,
                "status": "PENDING",
                "timestamp": signal.signal_timestamp,
                "wave_type": signal.wave_context.wave_type
            }
            
            log.debug(f"Registered Elliott wave signal {signal.signal_id[:8]} for {symbol} ({signal.wave_context.wave_type})")
        
        def update_signal_status(self, signal_id: str, status: str):
            """Update signal status (PENDING → TRIGGERED → CLOSED)"""
            if signal_id in self.signal_status:
                self.signal_status[signal_id]["status"] = status
                log.debug(f"Signal {signal_id[:8]} status updated to {status}")
                
                # If CLOSED, mark as ready for new signals
                if status == "CLOSED":
                    symbol = self.signal_status[signal_id]["symbol"]
                    log.info(f"✅ Signal {signal_id[:8]} for {symbol} CLOSED - Ready for new wave setups")
        
        def remove_closed_signals(self):
            """Clean up closed signals to free memory"""
            current_time = time.time()
            closed_signal_ids = []
            
            for signal_id, data in list(self.signal_status.items()):
                if data.get("status") == "CLOSED":
                    # Remove if closed more than 1 hour ago
                    if current_time - data.get("timestamp", 0) > 3600:
                        closed_signal_ids.append(signal_id)
            
            for signal_id in closed_signal_ids:
                symbol = self.signal_status[signal_id]["symbol"]
                
                # Only remove from active_signals if it's the current one
                if self.active_signals.get(symbol) == signal_id:
                    del self.active_signals[symbol]
                
                del self.signal_status[signal_id]
                log.debug(f"Cleaned up closed signal {signal_id[:8]} for {symbol}")
    
    def __init__(self):
        self.daily_stats = {
            "wave_setups_found": 0,
            "wave_3_setups": 0,
            "wave_5_setups": 0,
            "corrective_setups": 0,
            "pairs_scanned": 0,
            "low_confidence": 0,
            "no_wave_structure": 0,
            "volume_rejection": 0
        }
        self.deduplicator = self.SignalDeduplicator()
        self.active_signal_ids = set()
    
    # ========== ELLIOTT WAVE ANALYSIS ==========
    
    def analyze_wave_context(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame, df_5m: pd.DataFrame) -> WaveContext:
        """
        Analyze Elliott Wave context across timeframes
        Returns wave type, confidence, and characteristics
        """
        try:
            if df_1h is None or df_15m is None or df_5m is None:
                return self._get_default_wave_context()
            
            if len(df_1h) < 50 or len(df_15m) < 40 or len(df_5m) < 30:
                return self._get_default_wave_context()
            
            # 1. Determine primary trend direction
            trend_direction = self._determine_trend_direction(df_1h)
            
            # 2. Analyze wave structure on 15M
            wave_type, wave_confidence = self._detect_wave_structure(df_15m, trend_direction)
            
            # 3. Calculate wave maturity
            wave_maturity = self._calculate_wave_maturity(df_5m, wave_type)
            
            # 4. Check for wave characteristics
            is_extended = self._check_wave_extension(df_15m)
            has_divergence = self._check_wave_divergence(df_5m, wave_type)
            
            # 5. Analyze multi-timeframe alignment
            higher_tf_wave = self._get_higher_tf_wave(df_1h)
            lower_tf_wave = self._get_lower_tf_wave(df_5m)
            subwave_count = self._count_subwaves(df_5m)
            
            # 6. Calculate wave strength
            wave_strength = self._calculate_wave_strength(df_15m, wave_type)
            
            # 7. Find Fibonacci level
            fibonacci_level = self._find_fibonacci_level(df_15m, wave_type)
            
            return WaveContext(
                wave_type=wave_type,
                wave_maturity=wave_maturity,
                wave_confidence=wave_confidence,
                is_extended=is_extended,
                has_divergence=has_divergence,
                higher_tf_wave=higher_tf_wave,
                lower_tf_wave=lower_tf_wave,
                subwave_count=subwave_count,
                wave_strength=wave_strength,
                trend_direction=trend_direction,
                fibonacci_level=fibonacci_level
            )
            
        except Exception as e:
            log.error(f"Wave context error: {e}")
            return self._get_default_wave_context()
    
    def _determine_trend_direction(self, df: pd.DataFrame) -> str:
        """Determine primary trend direction using EMA alignment"""
        try:
            if len(df) < 100:
                return "NEUTRAL"
            
            # Calculate EMAs
            ema_50 = df['close'].ewm(span=50).mean().iloc[-1]
            ema_200 = df['close'].ewm(span=200).mean().iloc[-1]
            current_price = df['close'].iloc[-1]
            
            # Bullish: price > EMA50 > EMA200
            if current_price > ema_50 > ema_200:
                return "BULLISH"
            # Bearish: price < EMA50 < EMA200
            elif current_price < ema_50 < ema_200:
                return "BEARISH"
            else:
                return "NEUTRAL"
                
        except Exception as e:
            return "NEUTRAL"
    
    def _detect_wave_structure(self, df: pd.DataFrame, trend: str) -> Tuple[str, float]:
        """Detect Elliott Wave structure"""
        try:
            if len(df) < 40:
                return "UNKNOWN", 0.0
            
            # Calculate indicators
            rsi = self.calculate_rsi(df['close']).iloc[-1]
            recent_volume = df['volume'].iloc[-10:].mean()
            avg_volume = df['volume'].iloc[-40:].mean()
            volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0
            
            # Analyze price swings
            swings = self._analyze_price_swings(df)
            
            # Based on RSI, volume, and swings, determine wave type
            if trend == "BULLISH":
                # Wave 3 characteristics: strong momentum, high volume, RSI 60-75
                if (60 <= rsi <= 75 and volume_ratio >= WAVE_CONFIG["wave_3_volume_multiplier"] and 
                    swings.get("is_strong_impulse", False)):
                    return "IMPULSE_3", 0.75
                
                # Wave 5 characteristics: high RSI, possible divergence, volume may decrease
                elif (70 <= rsi <= 85 and swings.get("has_divergence", False)):
                    return "IMPULSE_5", 0.70
                
                # Wave 2/4 characteristics: pullback, lower RSI, lower volume
                elif (35 <= rsi <= 50 and volume_ratio <= WAVE_CONFIG["corrective_volume_multiplier"] and
                      swings.get("is_corrective", False)):
                    return "CORRECTIVE_2" if swings.get("swing_count", 0) % 2 == 0 else "CORRECTIVE_4", 0.65
            
            elif trend == "BEARISH":
                # Similar logic for bearish markets
                if (25 <= rsi <= 40 and volume_ratio >= WAVE_CONFIG["wave_3_volume_multiplier"] and
                    swings.get("is_strong_impulse", False)):
                    return "IMPULSE_3", 0.75
                elif (15 <= rsi <= 30 and swings.get("has_divergence", False)):
                    return "IMPULSE_5", 0.70
                elif (50 <= rsi <= 65 and volume_ratio <= WAVE_CONFIG["corrective_volume_multiplier"] and
                      swings.get("is_corrective", False)):
                    return "CORRECTIVE_2" if swings.get("swing_count", 0) % 2 == 0 else "CORRECTIVE_4", 0.65
            
            return "UNKNOWN", 0.3
            
        except Exception as e:
            return "UNKNOWN", 0.0
    
    def _analyze_price_swings(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze price swings for wave detection"""
        try:
            if len(df) < 20:
                return {"is_strong_impulse": False, "is_corrective": False, "has_divergence": False, "swing_count": 0}
            
            prices = df['close'].values[-20:]
            highs = df['high'].values[-20:]
            lows = df['low'].values[-20:]
            
            # Find swing highs and lows
            swing_highs = []
            swing_lows = []
            
            for i in range(5, len(prices) - 5):
                if highs[i] == highs[i-5:i+6].max():
                    swing_highs.append((i, highs[i]))
                if lows[i] == lows[i-5:i+6].min():
                    swing_lows.append((i, lows[i]))
            
            swing_count = len(swing_highs) + len(swing_lows)
            
            # Check for impulse move (higher highs and higher lows)
            is_strong_impulse = False
            if len(swing_highs) >= 2 and len(swing_lows) >= 2:
                if (swing_highs[-1][1] > swing_highs[-2][1] and 
                    swing_lows[-1][1] > swing_lows[-2][1]):
                    is_strong_impulse = True
            
            # Check for corrective move (overlapping swings)
            is_corrective = False
            if len(swing_highs) >= 1 and len(swing_lows) >= 1:
                recent_high = swing_highs[-1][1]
                recent_low = swing_lows[-1][1]
                if abs(recent_high - recent_low) / recent_low < 0.03:  # Less than 3% range
                    is_corrective = True
            
            # Check for divergence
            has_divergence = self._check_rsi_divergence(df)
            
            return {
                "is_strong_impulse": is_strong_impulse,
                "is_corrective": is_corrective,
                "has_divergence": has_divergence,
                "swing_count": swing_count
            }
            
        except Exception as e:
            return {"is_strong_impulse": False, "is_corrective": False, "has_divergence": False, "swing_count": 0}
    
    def _check_rsi_divergence(self, df: pd.DataFrame) -> bool:
        """Check for RSI divergence"""
        try:
            if len(df) < 30:
                return False
            
            prices = df['close'].values[-30:]
            rsi_values = self.calculate_rsi(df['close']).values[-30:]
            
            # Find peaks and troughs
            peaks = []
            troughs = []
            
            for i in range(5, len(prices) - 5):
                if prices[i] == prices[i-5:i+6].max():
                    peaks.append((i, prices[i], rsi_values[i]))
                if prices[i] == prices[i-5:i+6].min():
                    troughs.append((i, prices[i], rsi_values[i]))
            
            # Check for bearish divergence (price higher high, RSI lower high)
            if len(peaks) >= 2:
                last_peak = peaks[-1]
                prev_peak = peaks[-2]
                
                if (last_peak[1] > prev_peak[1] and  # Price makes higher high
                    last_peak[2] < prev_peak[2]):    # RSI makes lower high
                    return True
            
            # Check for bullish divergence (price lower low, RSI higher low)
            if len(troughs) >= 2:
                last_trough = troughs[-1]
                prev_trough = troughs[-2]
                
                if (last_trough[1] < prev_trough[1] and  # Price makes lower low
                    last_trough[2] > prev_trough[2]):    # RSI makes higher low
                    return True
            
            return False
            
        except Exception as e:
            return False
    
    def _calculate_wave_maturity(self, df: pd.DataFrame, wave_type: str) -> float:
        """Calculate how mature the current wave is"""
        try:
            if len(df) < 20:
                return 0.5
            
            rsi = self.calculate_rsi(df['close']).iloc[-1]
            
            if "IMPULSE" in wave_type:
                # Impulse waves mature as RSI approaches overbought
                maturity = (rsi - 50) / 40  # Normalize 50-90 to 0-1
                return max(0, min(1, maturity))
            elif "CORRECTIVE" in wave_type:
                # Corrective waves mature as RSI approaches oversold
                maturity = (50 - rsi) / 40  # Normalize 50-10 to 0-1
                return max(0, min(1, maturity))
            else:
                return 0.5
                
        except Exception as e:
            return 0.5
    
    def _check_wave_extension(self, df: pd.DataFrame) -> bool:
        """Check if current wave is extended"""
        try:
            if len(df) < 30:
                return False
            
            prices = df['close'].values[-30:]
            price_change = abs(prices[-1] - prices[0]) / prices[0] * 100
            
            # Extended wave if move is > 10% in 30 periods
            return price_change > 10.0
            
        except Exception as e:
            return False
    
    def _check_wave_divergence(self, df: pd.DataFrame, wave_type: str) -> bool:
        """Check for divergence specific to wave type"""
        try:
            if len(df) < 30:
                return False
            
            # Only check divergence for Wave 5 specifically
            if wave_type == "IMPULSE_5":
                return self._check_wave_5_divergence(df)
            
            # For other wave types, use general divergence check
            return self._check_rsi_divergence(df)
            
        except Exception as e:
            return False
    
    def _check_wave_5_divergence(self, df: pd.DataFrame) -> bool:
        """Check for Wave 5 specific divergence"""
        try:
            if len(df) < 40:
                return False
            
            # Get price and RSI data
            prices = df['close'].values[-30:]
            rsi_values = self.calculate_rsi(df['close']).values[-30:]
            
            # Find peaks for bullish trend (Wave 5 in uptrend)
            peaks = []
            for i in range(5, len(prices) - 5):
                if prices[i] == prices[i-5:i+6].max():
                    peaks.append((i, prices[i], rsi_values[i]))
            
            # Need at least 2 peaks to check divergence
            if len(peaks) < 2:
                return False
            
            # Get last two peaks
            peak1 = peaks[-2]
            peak2 = peaks[-1]
            
            # Classic Wave 5 divergence: Price makes higher high, RSI makes lower high
            if peak2[1] > peak1[1] and peak2[2] < peak1[2]:
                return True
            
            # Also check volume divergence (decreasing volume in Wave 5)
            recent_volume = df['volume'].values[-10:].mean()
            previous_volume = df['volume'].values[-20:-10].mean()
            
            if recent_volume < previous_volume * 0.8:  # Volume decreasing
                return True
            
            return False
            
        except Exception as e:
            return False
    
    def _get_higher_tf_wave(self, df: pd.DataFrame) -> str:
        """Get wave context from higher timeframe"""
        try:
            if len(df) < 50:
                return "UNKNOWN"
            
            trend = self._determine_trend_direction(df)
            
            if trend == "BULLISH":
                return "BULLISH_IMPULSE"
            elif trend == "BEARISH":
                return "BEARISH_IMPULSE"
            else:
                return "CORRECTIVE"
                
        except Exception as e:
            return "UNKNOWN"
    
    def _get_lower_tf_wave(self, df: pd.DataFrame) -> str:
        """Get wave context from lower timeframe"""
        try:
            if len(df) < 20:
                return "UNKNOWN"
            
            rsi = self.calculate_rsi(df['close']).iloc[-1]
            
            if rsi > 65:
                return "IMPULSE_SUBWAVE"
            elif rsi < 35:
                return "CORRECTIVE_SUBWAVE"
            else:
                return "NEUTRAL_SUBWAVE"
                
        except Exception as e:
            return "UNKNOWN"
    
    def _count_subwaves(self, df: pd.DataFrame) -> int:
        """Count sub-waves in current structure"""
        try:
            if len(df) < 15:
                return 1
            
            # Simple fractal analysis
            price_changes = np.abs(np.diff(df['close'].values[-15:]))
            volatility = np.std(price_changes)
            
            if volatility > np.mean(price_changes) * 0.5:
                return 3  # High volatility = more sub-waves
            elif volatility > np.mean(price_changes) * 0.3:
                return 2
            else:
                return 1
                
        except Exception as e:
            return 1
    
    def _calculate_wave_strength(self, df: pd.DataFrame, wave_type: str) -> float:
        """Calculate strength of the current wave"""
        try:
            if len(df) < 20:
                return 0.5
            
            # Factors for wave strength
            factors = []
            
            # 1. Price expansion
            price_change = abs(df['close'].iloc[-1] - df['close'].iloc[-20]) / df['close'].iloc[-20]
            factors.append(min(price_change * 10, 1.0))
            
            # 2. Volume participation
            recent_volume = df['volume'].iloc[-5:].mean()
            avg_volume = df['volume'].iloc[-20:].mean()
            volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0
            factors.append(min(volume_ratio - 1.0, 1.0) if volume_ratio > 1.0 else 0.0)
            
            # 3. RSI momentum
            rsi = self.calculate_rsi(df['close']).iloc[-1]
            if "IMPULSE" in wave_type:
                rsi_strength = (rsi - 50) / 40 if rsi > 50 else 0
            else:
                rsi_strength = (50 - rsi) / 40 if rsi < 50 else 0
            factors.append(max(0, min(1, rsi_strength)))
            
            # 4. EMA alignment
            ema_alignment = self._calculate_ema_alignment(df)
            factors.append(ema_alignment)
            
            return np.mean(factors)
            
        except Exception as e:
            return 0.5
    
    def _calculate_ema_alignment(self, df: pd.DataFrame) -> float:
        """Calculate EMA alignment score"""
        try:
            if len(df) < 50:
                return 0.5
            
            ema_9 = df['close'].ewm(span=9).mean().iloc[-1]
            ema_21 = df['close'].ewm(span=21).mean().iloc[-1]
            ema_50 = df['close'].ewm(span=50).mean().iloc[-1]
            
            # Perfect bullish alignment
            if ema_9 > ema_21 > ema_50:
                return 1.0
            # Perfect bearish alignment
            elif ema_9 < ema_21 < ema_50:
                return 1.0
            # Good alignment
            elif (ema_9 > ema_21 and ema_21 > ema_50) or (ema_9 < ema_21 and ema_21 < ema_50):
                return 0.7
            else:
                return 0.3
                
        except Exception as e:
            return 0.5
    
    def _find_fibonacci_level(self, df: pd.DataFrame, wave_type: str) -> Optional[float]:
        """Find nearest Fibonacci level"""
        try:
            if len(df) < 30:
                return None
            
            # Find recent swing for Fibonacci
            highs = df['high'].values[-30:]
            lows = df['low'].values[-30:]
            
            recent_high = highs.max()
            recent_low = lows.min()
            current_price = df['close'].iloc[-1]
            
            if recent_high == recent_low:
                return None
            
            # Calculate Fibonacci levels
            fib_levels = [
                0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0,
                1.272, 1.618, 2.0, 2.618
            ]
            
            # Find nearest Fibonacci level
            range_size = recent_high - recent_low
            for level in fib_levels:
                fib_price = recent_low + range_size * level
                distance_pct = abs(current_price - fib_price) / current_price * 100
                
                if distance_pct < 0.5:  # Within 0.5%
                    return level
            
            return None
            
        except Exception as e:
            return None
    
    def _get_default_wave_context(self) -> WaveContext:
        return WaveContext(
            wave_type="UNKNOWN",
            wave_maturity=0.5,
            wave_confidence=0.3,
            is_extended=False,
            has_divergence=False,
            higher_tf_wave="UNKNOWN",
            lower_tf_wave="UNKNOWN",
            subwave_count=1,
            wave_strength=0.5,
            trend_direction="NEUTRAL",
            fibonacci_level=None
        )
    
    # ========== MARKET STRENGTH ANALYSIS (WAVE-SPECIFIC) ==========
    
    def analyze_market_strength(self, df: pd.DataFrame, wave_context: WaveContext) -> MarketStrength:
        """
        Analyze market strength in context of Elliott Wave
        """
        try:
            if df is None or len(df) < 30:
                return self._get_default_market_strength()
            
            # 1. Wave momentum analysis
            wave_momentum = self._calculate_wave_momentum(df, wave_context.wave_type)
            
            # 2. Volume participation for wave type
            volume_participation = self._calculate_wave_volume(df, wave_context.wave_type)
            
            # 3. EMA alignment
            ema_alignment = self._calculate_ema_alignment(df)
            
            # 4. Price expansion in current wave
            price_expansion = self._calculate_price_expansion(df, wave_context)
            
            # 5. Wave-specific characteristics
            is_wave_3_strength = self._check_wave_3_strength(df, wave_context)
            is_wave_5_divergence = wave_context.has_divergence
            is_corrective_setup = self._check_corrective_setup(df, wave_context)
            is_volume_climax = self._check_volume_climax(df)
            
            # 6. Combined strength score
            strength_score = self._calculate_wave_strength_score(
                wave_momentum, volume_participation, ema_alignment, price_expansion,
                wave_context
            )
            
            return MarketStrength(
                wave_momentum=wave_momentum,
                volume_participation=volume_participation,
                ema_alignment=ema_alignment,
                price_expansion=price_expansion,
                is_wave_3_strength=is_wave_3_strength,
                is_wave_5_divergence=is_wave_5_divergence,
                is_corrective_setup=is_corrective_setup,
                is_volume_climax=is_volume_climax,
                strength_score=strength_score
            )
            
        except Exception as e:
            log.error(f"Market strength error: {e}")
            return self._get_default_market_strength()
    
    def _calculate_wave_momentum(self, df: pd.DataFrame, wave_type: str) -> float:
        """Calculate momentum specific to wave type"""
        if len(df) < 10:
            return 0.5
        
        # For impulse waves, we want strong momentum
        # For corrective waves, we want weakening momentum
        
        recent_prices = df['close'].values[-10:]
        rsi = self.calculate_rsi(df['close']).iloc[-1]
        
        if "IMPULSE" in wave_type:
            # Strong upward momentum for bullish impulse
            price_slope = np.polyfit(range(len(recent_prices)), recent_prices, 1)[0]
            avg_price = np.mean(recent_prices)
            momentum = price_slope / avg_price * 100 if avg_price > 0 else 0
            
            # Combine with RSI
            rsi_momentum = (rsi - 50) / 30 if rsi > 50 else 0
            return min((momentum * 0.1 + rsi_momentum * 0.9), 1.0)
        
        elif "CORRECTIVE" in wave_type:
            # Weakening momentum for corrections
            price_volatility = np.std(np.diff(recent_prices)) / np.mean(recent_prices) * 100
            return min(price_volatility / 5.0, 1.0)
        
        else:
            return 0.5
    
    def _calculate_wave_volume(self, df: pd.DataFrame, wave_type: str) -> float:
        """Calculate volume participation for specific wave type"""
        if len(df) < 20:
            return 0.5
        
        recent_volume = df['volume'].iloc[-5:].mean()
        avg_volume = df['volume'].iloc[-20:].mean()
        
        if avg_volume == 0:
            return 0.5
        
        volume_ratio = recent_volume / avg_volume
        
        # Different expectations for different waves
        if wave_type == "IMPULSE_3":
            # Wave 3 should have the highest volume
            if volume_ratio >= WAVE_CONFIG["wave_3_volume_multiplier"]:
                return 1.0
            elif volume_ratio >= 1.5:
                return 0.7
            else:
                return 0.3
        
        elif wave_type in ["CORRECTIVE_2", "CORRECTIVE_4"]:
            # Corrective waves should have lower volume
            if volume_ratio <= WAVE_CONFIG["corrective_volume_multiplier"]:
                return 1.0
            elif volume_ratio <= 1.0:
                return 0.7
            else:
                return 0.3
        
        elif wave_type == "IMPULSE_5":
            # Wave 5 volume can vary, often less than wave 3
            if volume_ratio >= 1.2:
                return 0.8
            else:
                return 0.5
        
        else:
            return min(volume_ratio - 1.0, 1.0) if volume_ratio > 1.0 else max(volume_ratio - 1.0, 0.0)
    
    def _calculate_price_expansion(self, df: pd.DataFrame, wave_context: WaveContext) -> float:
        """Calculate price expansion in current wave"""
        if len(df) < 20:
            return 0.5
        
        prices = df['close'].values[-20:]
        wave_start = prices[0]
        current_price = prices[-1]
        
        expansion = abs(current_price - wave_start) / wave_start
        
        # Normalize based on wave type expectations
        if wave_context.wave_type == "IMPULSE_3":
            # Wave 3 often has largest expansion
            return min(expansion * 20, 1.0)  # 5% expansion = 1.0
        elif wave_context.wave_type == "IMPULSE_5":
            # Wave 5 expansion varies
            return min(expansion * 25, 1.0)  # 4% expansion = 1.0
        else:
            return min(expansion * 30, 1.0)  # ~3.3% expansion = 1.0
    
    def _check_wave_3_strength(self, df: pd.DataFrame, wave_context: WaveContext) -> bool:
        """Check for Wave 3 strength characteristics"""
        if len(df) < 10:
            return False
        
        # Wave 3 should have: high volume, strong momentum, RSI in specific zone
        recent_volume = df['volume'].iloc[-5:].mean()
        avg_volume = df['volume'].iloc[-20:].mean()
        volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0
        
        rsi = self.calculate_rsi(df['close']).iloc[-1]
        
        return (volume_ratio >= WAVE_CONFIG["wave_3_volume_multiplier"] and
                WAVE_CONFIG["rsi_wave_3_zone"][0] <= rsi <= WAVE_CONFIG["rsi_wave_3_zone"][1] and
                wave_context.wave_strength > 0.7)
    
    def _check_corrective_setup(self, df: pd.DataFrame, wave_context: WaveContext) -> bool:
        """Check for corrective wave setup"""
        if len(df) < 15:
            return False
        
        rsi = self.calculate_rsi(df['close']).iloc[-1]
        recent_volume = df['volume'].iloc[-5:].mean()
        avg_volume = df['volume'].iloc[-20:].mean()
        volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0
        
        return (WAVE_CONFIG["rsi_wave_2_4_zone"][0] <= rsi <= WAVE_CONFIG["rsi_wave_2_4_zone"][1] and
                volume_ratio <= WAVE_CONFIG["corrective_volume_multiplier"] and
                wave_context.wave_type in ["CORRECTIVE_2", "CORRECTIVE_4"])
    
    def _check_volume_climax(self, df: pd.DataFrame) -> bool:
        """Check for volume climax (end of wave)"""
        if len(df) < 10:
            return False
        
        recent_volume = df['volume'].iloc[-3:].mean()
        avg_volume = df['volume'].iloc[-20:].mean()
        
        return recent_volume > avg_volume * 2.0
    
    def _calculate_wave_strength_score(self, wave_momentum: float, volume_participation: float,
                                      ema_alignment: float, price_expansion: float,
                                      wave_context: WaveContext) -> float:
        """Calculate overall wave strength score"""
        # Different weights for different wave types
        if wave_context.wave_type == "IMPULSE_3":
            weights = [0.3, 0.4, 0.2, 0.1]  # Volume most important for Wave 3
        elif wave_context.wave_type in ["CORRECTIVE_2", "CORRECTIVE_4"]:
            weights = [0.2, 0.3, 0.3, 0.2]  # EMA alignment and momentum important
        elif wave_context.wave_type == "IMPULSE_5":
            weights = [0.4, 0.2, 0.3, 0.1]  # Momentum and EMA alignment important
        else:
            weights = [0.25, 0.25, 0.25, 0.25]
        
        factors = [wave_momentum, volume_participation, ema_alignment, price_expansion]
        return np.average(factors, weights=weights)
    
    def _get_default_market_strength(self) -> MarketStrength:
        return MarketStrength(
            wave_momentum=0.5,
            volume_participation=0.5,
            ema_alignment=0.5,
            price_expansion=0.5,
            is_wave_3_strength=False,
            is_wave_5_divergence=False,
            is_corrective_setup=False,
            is_volume_climax=False,
            strength_score=0.5
        )
    
    # ========== WAVE REJECTION ZONES ==========
    
    def find_wave_rejection_zones(self, df: pd.DataFrame, current_price: float, 
                                 rsi_value: float, wave_context: WaveContext) -> List[WaveRejectionZone]:
        """
        Find rejection zones in context of Elliott Wave
        """
        zones = []
        
        try:
            if df is None or len(df) < 30:
                return zones
            
            # 1. Fibonacci zones
            fib_zones = self._find_fibonacci_rejection_zones(df, current_price, wave_context)
            zones.extend(fib_zones)
            
            # 2. EMA zones
            ema_zones = self._find_ema_rejection_zones(df, current_price, wave_context)
            zones.extend(ema_zones)
            
            # 3. Wave-specific zones
            wave_zones = self._find_wave_specific_zones(df, current_price, wave_context)
            zones.extend(wave_zones)
            
            # 4. Volume-based zones
            volume_zones = self._find_volume_based_zones(df, current_price, wave_context)
            zones.extend(volume_zones)
            
            # Filter to active zones only and add metadata
            active_zones = []
            for zone in zones:
                if zone.is_active:
                    # Add volume confirmation
                    zone.volume_confirmation = self._check_volume_confirmation(df, zone.zone_type)
                    
                    # Add RSI position
                    zone.rsi_position = self._analyze_wave_rsi_position(rsi_value, zone.wave_context)
                    
                    active_zones.append(zone)
            
            return active_zones
            
        except Exception as e:
            log.error(f"Wave rejection zone error: {e}")
            return []
    
    def _find_fibonacci_rejection_zones(self, df: pd.DataFrame, current_price: float,
                                       wave_context: WaveContext) -> List[WaveRejectionZone]:
        """Find Fibonacci retracement zones"""
        zones = []
        
        try:
            if len(df) < 50:
                return zones
            
            # Find recent significant swing
            prices = df['close'].values[-50:]
            recent_high = prices.max()
            recent_low = prices.min()
            
            if recent_high == recent_low:
                return zones
            
            # Important Fibonacci levels for Elliott Wave
            fib_levels = {
                0.236: "FIBONACCI_23",
                0.382: "FIBONACCI_38",
                0.5: "FIBONACCI_50",
                0.618: "FIBONACCI_62",
                0.786: "FIBONACCI_78"
            }
            
            for fib_value, fib_name in fib_levels.items():
                fib_price = recent_low + (recent_high - recent_low) * fib_value
                distance_pct = abs(current_price - fib_price) / current_price * 100
                
                if distance_pct < 0.3:  # Within 0.3%
                    # Determine if support or resistance based on wave context
                    if wave_context.wave_type in ["CORRECTIVE_2", "CORRECTIVE_4"]:
                        zone_type = "FIBONACCI_SUPPORT"
                        is_impulse = False
                        is_corrective = True
                    elif wave_context.wave_type in ["IMPULSE_5"]:
                        zone_type = "FIBONACCI_RESISTANCE"
                        is_impulse = True
                        is_corrective = False
                    else:
                        zone_type = fib_name
                        is_impulse = "IMPULSE" in wave_context.wave_type
                        is_corrective = "CORRECTIVE" in wave_context.wave_type
                    
                    zones.append(WaveRejectionZone(
                        zone_type=zone_type,
                        price_level=fib_price,
                        wave_context=wave_context.wave_type,
                        strength=0.7 if fib_value in [0.382, 0.5, 0.618] else 0.6,
                        volume_confirmation=False,
                        rsi_position="IN_ZONE",
                        is_active=True,
                        fibonacci_level=fib_value,
                        is_impulse=is_impulse,
                        is_corrective=is_corrective
                    ))
            
            return zones
            
        except Exception as e:
            return []
    
    def _find_ema_rejection_zones(self, df: pd.DataFrame, current_price: float,
                                 wave_context: WaveContext) -> List[WaveRejectionZone]:
        """Find EMA rejection zones"""
        zones = []
        
        try:
            # Calculate EMAs
            emas = self.calculate_emas(df)
            
            for ema_name, ema_value in emas.items():
                if ema_value == 0:
                    continue
                
                distance_pct = abs(current_price - ema_value) / current_price * 100
                
                # Check if price is near EMA
                if distance_pct < 0.5:  # Within 0.5%
                    # Determine zone type
                    if current_price > ema_value:
                        zone_type = f"EMA_{ema_name.upper()}_SUPPORT"
                    else:
                        zone_type = f"EMA_{ema_name.upper()}_RESISTANCE"
                    
                    # Determine wave context
                    is_impulse = "IMPULSE" in wave_context.wave_type
                    is_corrective = "CORRECTIVE" in wave_context.wave_type
                    
                    # Strength based on EMA importance
                    if ema_name == "fast":
                        strength = 0.6
                    elif ema_name == "medium":
                        strength = 0.7
                    elif ema_name == "slow":
                        strength = 0.8
                    else:
                        strength = 0.5
                    
                    zones.append(WaveRejectionZone(
                        zone_type=zone_type,
                        price_level=ema_value,
                        wave_context=wave_context.wave_type,
                        strength=strength,
                        volume_confirmation=False,
                        rsi_position="IN_ZONE",
                        is_active=True,
                        fibonacci_level=None,
                        is_impulse=is_impulse,
                        is_corrective=is_corrective
                    ))
            
            # Check for EMA confluence (multiple EMAs close together)
            ema_values = list(emas.values())
            if len(ema_values) >= 3:
                ema_std = np.std(ema_values)
                if ema_std / np.mean(ema_values) < 0.01:  # Less than 1% spread
                    confluence_price = np.mean(ema_values)
                    zones.append(WaveRejectionZone(
                        zone_type="EMA_CONFLUENCE",
                        price_level=confluence_price,
                        wave_context=wave_context.wave_type,
                        strength=0.9,
                        volume_confirmation=False,
                        rsi_position="IN_ZONE",
                        is_active=True,
                        fibonacci_level=None,
                        is_impulse="IMPULSE" in wave_context.wave_type,
                        is_corrective="CORRECTIVE" in wave_context.wave_type
                    ))
            
            return zones
            
        except Exception as e:
            return []
    
    def _find_wave_specific_zones(self, df: pd.DataFrame, current_price: float,
                                 wave_context: WaveContext) -> List[WaveRejectionZone]:
        """Find zones specific to current wave"""
        zones = []
        
        try:
            wave_type = wave_context.wave_type
            
            # Wave 2/4 pullback zones
            if wave_type in ["CORRECTIVE_2", "CORRECTIVE_4"]:
                # Look for potential support levels
                recent_low = df['low'].values[-20:].min()
                distance_pct = abs(current_price - recent_low) / current_price * 100
                
                if distance_pct < 0.5:  # Near recent low
                    zones.append(WaveRejectionZone(
                        zone_type=f"{wave_type}_SUPPORT",
                        price_level=recent_low,
                        wave_context=wave_type,
                        strength=0.8,
                        volume_confirmation=False,
                        rsi_position="WAVE_2_4_RSI",
                        is_active=True,
                        fibonacci_level=None,
                        is_impulse=False,
                        is_corrective=True
                    ))
            
            # Wave 5 resistance zones
            elif wave_type == "IMPULSE_5":
                # Look for potential resistance levels
                recent_high = df['high'].values[-20:].max()
                distance_pct = abs(current_price - recent_high) / current_price * 100
                
                if distance_pct < 0.5:  # Near recent high
                    zones.append(WaveRejectionZone(
                        zone_type="WAVE_5_RESISTANCE",
                        price_level=recent_high,
                        wave_context=wave_type,
                        strength=0.8,
                        volume_confirmation=False,
                        rsi_position="WAVE_5_RSI",
                        is_active=True,
                        fibonacci_level=None,
                        is_impulse=True,
                        is_corrective=False
                    ))
            
            return zones
            
        except Exception as e:
            return []
    
    def _find_volume_based_zones(self, df: pd.DataFrame, current_price: float,
                                wave_context: WaveContext) -> List[WaveRejectionZone]:
        """Find zones based on volume analysis"""
        zones = []
        
        try:
            if len(df) < 10:
                return zones
            
            # Check for volume climax (end of wave)
            recent_volume = df['volume'].iloc[-3:].mean()
            avg_volume = df['volume'].iloc[-20:].mean()
            
            if recent_volume > avg_volume * 2.0:
                zones.append(WaveRejectionZone(
                    zone_type="VOLUME_CLIMAX",
                    price_level=current_price,
                    wave_context=wave_context.wave_type,
                    strength=0.9,
                    volume_confirmation=True,
                    rsi_position="CLIMAX_RSI",
                    is_active=True,
                    fibonacci_level=None,
                    is_impulse="IMPULSE" in wave_context.wave_type,
                    is_corrective="CORRECTIVE" in wave_context.wave_type
                ))
            
            # Check for absorption zones
            recent_candle = df.iloc[-1]
            if (recent_candle['volume'] > df['volume'].iloc[-10:-1].mean() * 1.5 and
                abs(recent_candle['close'] - recent_candle['open']) / recent_candle['open'] < 0.005):
                
                zones.append(WaveRejectionZone(
                    zone_type="ABSORPTION_ZONE",
                    price_level=current_price,
                    wave_context=wave_context.wave_type,
                    strength=0.85,
                    volume_confirmation=True,
                    rsi_position="ABSORPTION_RSI",
                    is_active=True,
                    fibonacci_level=None,
                    is_impulse="IMPULSE" in wave_context.wave_type,
                    is_corrective="CORRECTIVE" in wave_context.wave_type
                ))
            
            return zones
            
        except Exception as e:
            return []
    
    def _check_volume_confirmation(self, df: pd.DataFrame, zone_type: str) -> bool:
        """Check volume confirmation at rejection zone"""
        try:
            if len(df) < 5:
                return False
            
            recent_candles = df.iloc[-5:]
            
            # Different volume expectations for different zones
            if "VOLUME_CLIMAX" in zone_type or "ABSORPTION" in zone_type:
                # Already volume-based zones
                return True
            
            # For other zones, check for volume spike
            recent_volume = recent_candles['volume'].iloc[-2:].mean()
            prev_volume = recent_candles['volume'].iloc[-5:-2].mean()
            
            if prev_volume > 0:
                volume_ratio = recent_volume / prev_volume
                return volume_ratio >= 1.5
            
            return False
            
        except Exception as e:
            return False
    
    def _analyze_wave_rsi_position(self, rsi_value: float, wave_context: str) -> str:
        """Analyze RSI position for specific wave context"""
        if wave_context == "IMPULSE_3":
            low, high = WAVE_CONFIG["rsi_wave_3_zone"]
            if low <= rsi_value <= high:
                return "WAVE_3_RSI"
            elif rsi_value > high:
                return "OVEREXTENDED"
            else:
                return "UNDEREXTENDED"
        
        elif wave_context == "IMPULSE_5":
            low, high = WAVE_CONFIG["rsi_wave_5_zone"]
            if low <= rsi_value <= high:
                return "WAVE_5_RSI"
            elif rsi_value > high:
                return "OVEREXTENDED"
            else:
                return "UNDEREXTENDED"
        
        elif wave_context in ["CORRECTIVE_2", "CORRECTIVE_4"]:
            low, high = WAVE_CONFIG["rsi_wave_2_4_zone"]
            if low <= rsi_value <= high:
                return "WAVE_2_4_RSI"
            elif rsi_value < low:
                return "OVEREXTENDED"
            else:
                return "UNDEREXTENDED"
        
        else:
            if rsi_value > 70:
                return "OVERBOUGHT"
            elif rsi_value < 30:
                return "OVERSOLD"
            else:
                return "NEUTRAL"
    
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
        """Calculate current EMA values"""
        try:
            emas = {}
            for name, period in EMA_PERIODS.items():
                ema_series = df['close'].ewm(span=period, adjust=False).mean()
                emas[name] = ema_series.iloc[-1] if len(ema_series) > 0 else 0
            return emas
        except Exception as e:
            return {name: 0 for name in EMA_PERIODS.keys()}
    
    # ========== SIGNAL GENERATION ==========
    
    def generate_elliott_signal(self, multi_tf_data: Dict[str, pd.DataFrame], 
                               symbol: str) -> Optional[ElliottWaveSignal]:
        """
        Generate Elliott Wave-based signal
        ONLY trade at wave-specific rejection zones
        """
        try:
            # Get timeframe data
            tf_1h = multi_tf_data.get("1H")
            tf_15m = multi_tf_data.get("15M")
            tf_5m = multi_tf_data.get("5M")
            tf_3m = multi_tf_data.get("3M")
            
            # Check data availability
            if tf_1h is None or tf_15m is None or tf_5m is None:
                log.debug(f"{symbol}: Missing key timeframe data")
                return None
            
            # 1. Analyze Elliott Wave context
            wave_context = self.analyze_wave_context(tf_1h, tf_15m, tf_5m)
            
            # CRITICAL: Check wave confidence
            if wave_context.wave_confidence < WAVE_CONFIG["min_wave_confidence"]:
                self.daily_stats["low_confidence"] += 1
                log.debug(f"{symbol}: Wave confidence too low ({wave_context.wave_confidence:.2f})")
                return None
            
            # CRITICAL: Check wave maturity
            if wave_context.wave_maturity > WAVE_CONFIG["max_wave_maturity"]:
                log.debug(f"{symbol}: Wave too mature ({wave_context.wave_maturity:.1%})")
                return None
            
            # 2. Analyze market strength
            market_strength = self.analyze_market_strength(tf_5m, wave_context)
            
            # 3. Get current price and indicators
            current_price = tf_5m['close'].iloc[-1]
            emas = self.calculate_emas(tf_5m)
            
            # Calculate RSI
            rsi_series = self.calculate_rsi(tf_5m['close'])
            current_rsi = rsi_series.iloc[-1] if len(rsi_series) > 0 else 50
            
            # 4. Find wave rejection zones
            rejection_zones = self.find_wave_rejection_zones(tf_5m, current_price, current_rsi, wave_context)
            
            # CRITICAL: No rejection zone → no trade
            if not rejection_zones:
                self.daily_stats["no_wave_structure"] += 1
                log.debug(f"{symbol}: No active wave rejection zone")
                return None
            
            # 5. Check volume confirmation for each zone
            valid_zones = []
            for zone in rejection_zones:
                # Only consider zones with volume confirmation
                if zone.volume_confirmation:
                    valid_zones.append(zone)
            
            if not valid_zones:
                self.daily_stats["volume_rejection"] += 1
                log.debug(f"{symbol}: No volume confirmation at wave zones")
                return None
            
            # 6. Select strongest rejection zone
            best_zone = max(valid_zones, key=lambda z: z.strength)
            
            # 7. Determine trade side based on wave type and zone
            side, target_wave = self._determine_wave_trade_side(wave_context, best_zone)
            
            if not side:
                log.debug(f"{symbol}: Could not determine side for wave {wave_context.wave_type} zone {best_zone.zone_type}")
                return None
            
            # 8. Check RSI position for the wave
            if not self._validate_wave_rsi(current_rsi, wave_context, best_zone):
                log.debug(f"{symbol}: RSI {current_rsi:.1f} not valid for {wave_context.wave_type}")
                return None
            
            # 9. TRADE-BASED DEDUPLICATION CHECK
            if not self.deduplicator.should_generate_signal(symbol, side, current_price):
                return None
            
            # 10. Analyze candle for wave rejection confirmation
            trigger_type, confirmation_candle = self._analyze_wave_trigger_candle(tf_5m, side, best_zone)
            
            if not trigger_type:
                log.debug(f"{symbol}: No clear wave trigger candle")
                return None
            
            # 11. Calculate entry, SL, TP based on wave characteristics
            entry_price, stop_loss, take_profit = self._calculate_wave_levels(
                current_price, wave_context, best_zone, side
            )
            
            # Calculate Risk/Reward
            risk = abs(entry_price - stop_loss)
            reward = abs(take_profit - entry_price)
            
            if risk == 0:
                return None
            
            risk_reward = reward / risk
            
            # Minimum R:R check
            if risk_reward < MIN_RISK_REWARD:
                log.debug(f"{symbol}: R:R too low ({risk_reward:.1f}:1)")
                return None
            
            # 12. Calculate expected move based on wave type
            expected_move_pct = self._calculate_expected_wave_move(wave_context, best_zone)
            
            # 13. Check Elliott Wave rules
            elliott_rules = self._check_elliott_wave_rules(wave_context, market_strength, best_zone)
            
            if not elliott_rules:
                log.debug(f"{symbol}: Elliott Wave rules not satisfied")
                return None
            
            # 14. Create signal ID
            signal_id = hashlib.md5(
                f"{symbol}:{side}:{wave_context.wave_type}:{entry_price:.8f}:{time.time()}".encode()
            ).hexdigest()
            
            # 15. Create final signal
            signal = ElliottWaveSignal(
                signal_id=signal_id,
                symbol=symbol,
                side=side,
                
                wave_context=wave_context,
                target_wave=target_wave,
                
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                
                market_strength=market_strength,
                rejection_zone=best_zone,
                
                trigger_type=trigger_type,
                confirmation_candle=confirmation_candle,
                
                wave_confidence=wave_context.wave_confidence,
                risk_reward=risk_reward,
                expected_move_pct=expected_move_pct,
                fibonacci_target=best_zone.fibonacci_level,
                
                timeframe_used="5M",  # Main entry timeframe
                signal_timestamp=time.time(),
                elliott_rules_met=elliott_rules
            )
            
            # 16. Update tracking and deduplication
            self.deduplicator.register_signal(signal)
            self.active_signal_ids.add(signal_id)
            
            # 17. Update statistics
            self.daily_stats["wave_setups_found"] += 1
            if wave_context.wave_type == "IMPULSE_3":
                self.daily_stats["wave_3_setups"] += 1
            elif wave_context.wave_type == "IMPULSE_5":
                self.daily_stats["wave_5_setups"] += 1
            elif "CORRECTIVE" in wave_context.wave_type:
                self.daily_stats["corrective_setups"] += 1
            
            log.info(f"🌊 ELLIOTT WAVE SIGNAL: {symbol} {side}")
            log.info(f"   Wave: {wave_context.wave_type}, Confidence: {wave_context.wave_confidence:.2f}")
            log.info(f"   Zone: {best_zone.zone_type}, Strength: {best_zone.strength:.2f}")
            log.info(f"   RSI: {current_rsi:.1f}, R:R: {risk_reward:.1f}:1")
            log.info(f"   Expected move: {expected_move_pct:.1f}%")
            
            return signal
            
        except Exception as e:
            log.error(f"Elliott signal error for {symbol}: {e}")
            return None
    
    def _determine_wave_trade_side(self, wave_context: WaveContext, 
                                  zone: WaveRejectionZone) -> Tuple[Optional[str], Optional[str]]:
        """Determine trade side based on wave type and zone"""
        wave_type = wave_context.wave_type
        
        # Wave 3 setups (most profitable)
        if wave_type in ["CORRECTIVE_2", "CORRECTIVE_4"] and zone.is_corrective:
            # LONG at corrective support for Wave 3 entry
            if "SUPPORT" in zone.zone_type or "FIBONACCI" in zone.zone_type:
                return "LONG", "IMPULSE_3"
        
        # Wave 5 divergence setups
        elif wave_type == "IMPULSE_5" and zone.is_impulse:
            # SHORT at impulse resistance with divergence
            if wave_context.has_divergence and ("RESISTANCE" in zone.zone_type or "FIBONACCI" in zone.zone_type):
                return "SHORT", "CORRECTIVE_A"
        
        # EMA confluence trades
        elif "EMA_CONFLUENCE" in zone.zone_type:
            # Trade in direction of primary trend
            if wave_context.trend_direction == "BULLISH":
                return "LONG", wave_context.wave_type
            elif wave_context.trend_direction == "BEARISH":
                return "SHORT", wave_context.wave_type
        
        return None, None
    
    def _validate_wave_rsi(self, rsi: float, wave_context: WaveContext, zone: WaveRejectionZone) -> bool:
        """Validate RSI for wave trade"""
        wave_type = wave_context.wave_type
        
        if wave_type in ["CORRECTIVE_2", "CORRECTIVE_4"]:
            # Wave 2/4 should have RSI in pullback zone
            low, high = WAVE_CONFIG["rsi_wave_2_4_zone"]
            return low <= rsi <= high
        
        elif wave_type == "IMPULSE_3":
            # Wave 3 should have strong RSI
            low, high = WAVE_CONFIG["rsi_wave_3_zone"]
            return low <= rsi <= high
        
        elif wave_type == "IMPULSE_5":
            # Wave 5 can have high RSI, especially with divergence
            low, high = WAVE_CONFIG["rsi_wave_5_zone"]
            if wave_context.has_divergence:
                return rsi >= low  # Can be very high with divergence
            else:
                return low <= rsi <= high
        
        elif "EMA" in zone.zone_type:
            # EMA trades work with neutral to strong RSI
            return 40 <= rsi <= 70
        
        return True
    
    def _analyze_wave_trigger_candle(self, df: pd.DataFrame, side: str, 
                                    zone: WaveRejectionZone) -> Tuple[Optional[str], Optional[str]]:
        """Analyze the wave trigger candle pattern"""
        if len(df) < 3:
            return None, None
        
        current_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]
        
        # Wave reversal triggers
        if side == "LONG":
            # Bullish reversal at wave support
            if (prev_candle['close'] < prev_candle['open'] and  # Previous bearish
                current_candle['close'] > current_candle['open'] and  # Current bullish
                current_candle['low'] <= zone.price_level * 1.005 and  # Near zone
                current_candle['close'] > zone.price_level):
                
                if current_candle['close'] > prev_candle['high']:
                    return "WAVE_REVERSAL", "BULLISH_ENGULFING"
                else:
                    return "WAVE_REVERSAL", "HAMMER"
        
        else:  # SHORT
            # Bearish reversal at wave resistance
            if (prev_candle['close'] > prev_candle['open'] and  # Previous bullish
                current_candle['close'] < current_candle['open'] and  # Current bearish
                current_candle['high'] >= zone.price_level * 0.995 and  # Near zone
                current_candle['close'] < zone.price_level):
                
                if current_candle['close'] < prev_candle['low']:
                    return "WAVE_REVERSAL", "BEARISH_ENGULFING"
                else:
                    return "WAVE_REVERSAL", "SHOOTING_STAR"
        
        # EMA bounce triggers
        if "EMA" in zone.zone_type:
            if side == "LONG" and current_candle['low'] <= zone.price_level * 1.002:
                return "EMA_BOUNCE", "SUPPORT_CANDLE"
            elif side == "SHORT" and current_candle['high'] >= zone.price_level * 0.998:
                return "EMA_REJECTION", "RESISTANCE_CANDLE"
        
        return None, None
    
    def _calculate_wave_levels(self, current_price: float, wave_context: WaveContext,
                              zone: WaveRejectionZone, side: str) -> Tuple[float, float, float]:
        """Calculate entry, SL, TP based on wave characteristics"""
        entry_price = zone.price_level
        
        # Different logic for different wave trades
        if wave_context.wave_type in ["CORRECTIVE_2", "CORRECTIVE_4"]:
            # Wave 2/4 pullback trades
            if side == "LONG":
                stop_loss = entry_price * 0.985  # 1.5% below
                take_profit = entry_price * (1 + np.random.uniform(MIN_TARGET_PCT, MAX_TARGET_PCT) / 100)
            else:
                stop_loss = entry_price * 1.015
                take_profit = entry_price * (1 - np.random.uniform(MIN_TARGET_PCT, MAX_TARGET_PCT) / 100)
        
        elif wave_context.wave_type == "IMPULSE_5":
            # Wave 5 divergence trades
            if side == "SHORT":
                stop_loss = entry_price * 1.012  # 1.2% above
                take_profit = entry_price * 0.97  # 3% target
            else:
                stop_loss = entry_price * 0.988
                take_profit = entry_price * 1.03
        
        else:
            # Default levels
            if side == "LONG":
                stop_loss = entry_price * (1 - np.random.uniform(0.5, MAX_STOP_LOSS_PCT) / 100)
                take_profit = entry_price * (1 + np.random.uniform(MIN_TARGET_PCT, MAX_TARGET_PCT) / 100)
            else:
                stop_loss = entry_price * (1 + np.random.uniform(0.5, MAX_STOP_LOSS_PCT) / 100)
                take_profit = entry_price * (1 - np.random.uniform(MIN_TARGET_PCT, MAX_TARGET_PCT) / 100)
        
        return entry_price, stop_loss, take_profit
    
    def _calculate_expected_wave_move(self, wave_context: WaveContext, 
                                     zone: WaveRejectionZone) -> float:
        """Calculate expected move percentage based on wave type"""
        if wave_context.wave_type == "IMPULSE_3":
            # Wave 3 often largest: 1.618 of wave 1
            return np.random.uniform(3.0, 5.0)  # 3-5%
        elif wave_context.wave_type in ["CORRECTIVE_2", "CORRECTIVE_4"]:
            # Wave 2/4 pullbacks lead to wave 3/5
            return np.random.uniform(1.5, 3.0)  # 1.5-3%
        elif wave_context.wave_type == "IMPULSE_5":
            # Wave 5 often smaller than wave 3
            return np.random.uniform(2.0, 4.0)  # 2-4%
        elif "EMA" in zone.zone_type:
            # EMA confluence trades
            return np.random.uniform(1.0, 2.0)  # 1-2%
        else:
            return np.random.uniform(1.0, 3.0)  # Default 1-3%
    
    def _check_elliott_wave_rules(self, wave_context: WaveContext, 
                                 market_strength: MarketStrength, 
                                 zone: WaveRejectionZone) -> List[str]:
        """Check which Elliott Wave rules are satisfied"""
        rules_met = []
        
        # Basic wave rules
        if wave_context.wave_confidence >= WAVE_CONFIG["min_wave_confidence"]:
            rules_met.append("WAVE_COUNT_VALID")
        
        if wave_context.wave_maturity <= WAVE_CONFIG["max_wave_maturity"]:
            rules_met.append("WAVE_NOT_MATURE")
        
        # Wave-specific rules
        if wave_context.wave_type in ["CORRECTIVE_2", "CORRECTIVE_4"]:
            rules_met.append("CORRECTIVE_WAVE")
            
            # Volume should be lower in corrections
            if market_strength.volume_participation <= 0.5:
                rules_met.append("LOW_CORRECTIVE_VOLUME")
        
        elif wave_context.wave_type == "IMPULSE_3":
            rules_met.append("IMPULSE_WAVE_3")
            
            # Wave 3 should have high volume
            if market_strength.is_wave_3_strength:
                rules_met.append("WAVE_3_STRENGTH")
        
        elif wave_context.wave_type == "IMPULSE_5":
            rules_met.append("IMPULSE_WAVE_5")
            
            if wave_context.has_divergence:
                rules_met.append("WAVE_5_DIVERGENCE")
        
        # Zone rules
        if zone.volume_confirmation:
            rules_met.append("VOLUME_CONFIRMED")
        
        if zone.rsi_position != "NEUTRAL":
            rules_met.append(f"RSI_{zone.rsi_position}")
        
        return rules_met
    
    def get_daily_stats(self) -> Dict:
        """Get daily statistics"""
        return self.daily_stats
    
    def cleanup_old_signals(self):
        """Clean up old signals from deduplication"""
        self.deduplicator.remove_closed_signals()

# ================ MAIN SCANNER SYSTEM (KEEPING YOUR STRUCTURE) ================
class ElliottWaveScannerSystem:
    """Main scanner system for Elliott Wave trading"""
    
    def __init__(self):
        self.scanner = ElliottWaveScanner()
        self.exchange = None
        self.db = None
        self.scan_cycle = 0
        
    async def initialize(self):
        """Initialize the scanner"""
        log.info("=" * 70)
        log.info("🔥 ELLIOTT WAVE HIGH-FREQUENCY SCALPER")
        log.info("=" * 70)
        log.info("TRADER METHOD: موجات اليوت + القوة + الفوليوم + المؤشرات + الرفض")
        log.info("SPECIALTY: Wave counting across timeframes + Volume confirmation")
        log.info("TRADING STYLE: 7-10 trades in couple hours, mostly winning")
        log.info("WAVE FOCUS: Wave 3 setups (strongest), Wave 5 divergences")
        log.info(f"SCAN INTERVAL: {SCAN_INTERVAL} seconds (ULTRA FAST)")
        log.info("TIME FRAMES: 1H/15M (wave context), 5M/3M (entries)")
        log.info("WAVE ZONES: Fibonacci, EMA, Wave-specific support/resistance")
        log.info("RSI ZONES: 35-50 (Wave 2/4), 60-75 (Wave 3), 70-85 (Wave 5)")
        log.info("DEDUPLICATION: ONE TRADE PER SYMBOL (as trader does)")
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
            
            # Elliott Wave signals table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS elliott_wave_signals (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                
                wave_type TEXT NOT NULL,
                wave_confidence REAL NOT NULL,
                wave_maturity REAL NOT NULL,
                wave_strength REAL NOT NULL,
                trend_direction TEXT NOT NULL,
                
                wave_momentum REAL NOT NULL,
                volume_participation REAL NOT NULL,
                ema_alignment REAL NOT NULL,
                price_expansion REAL NOT NULL,
                strength_score REAL NOT NULL,
                
                zone_type TEXT NOT NULL,
                rejection_strength REAL NOT NULL,
                rsi_position TEXT NOT NULL,
                trigger_type TEXT NOT NULL,
                confirmation_candle TEXT NOT NULL,
                
                risk_reward REAL NOT NULL,
                expected_move REAL NOT NULL,
                fibonacci_target REAL,
                timeframe_used TEXT NOT NULL,
                
                elliott_rules_met TEXT,
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
            
            # Performance table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS performance_daily (
                date DATE PRIMARY KEY,
                wave_setups_found INTEGER,
                wave_3_setups INTEGER,
                wave_5_setups INTEGER,
                corrective_setups INTEGER,
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
🌊 <b>إطلاق نظام موجات اليوت للأسكالب</b>

<b>🧠 منهجية التاجر:</b>
‎• مراقبة كل الفريمات
‎• تحديد الطول الموجي (موجات اليوت)
‎• تحليل القوة والفوليوم
‎• المؤشرات: RSI + EMA + VOLUME
‎• الدخول عند الرفض

<b>📊 موجات اليوت التي نبحث عنها:</b>
1️⃣ <b>موجة 3 الدافعة</b> (الأقوى)
‎   • أعلى حجم + أعلى زخم
‎   • RSI بين 60-75
‎   • دخول عند تصحيح موجة 2 أو 4

2️⃣ <b>موجة 5 النهائية</b> (مع تباعد)
‎   • سعر أعلى + RSI أقل
‎   • دخول عند المقاومة

3️⃣ <b>التصحيحات 2 و 4</b>
‎   • حجم منخفض + RSI بين 35-50
‎   • دخول عند الدعم

<b>⚡ نظام التداول:</b>
‎• 7-10 صفقات في ساعات قليلة
‎• خسائر قليلة + أرباح غالباً
‎• الأرباح من 1% إلى 5%
‎• صفقة واحدة لكل عملة فقط

<b>🎯 شروط الدخول:</b>
‎1. تأكيد موجة يوت واضحة (ثقة > 65%)
‎2. حجم يؤكد نوع الموجة
‎3. RSI في المنطقة الصحيحة للموجة
‎4. رفض عند مستوى فيبوناتشي أو متوسط متحرك
‎5. شمعة تأكيد للرفض

<b>🛡️ إدارة المخاطرة:</b>
‎• وقف خسارة: حتى 1.2%
‎• هدف الربح: 1-5%
‎• نسبة الربح/المخاطرة: 1.5:1 كحد أدنى
‎• صفقة واحدة لكل عملة

<b>🧠 عقلية التاجر:</b>
‎نصطاد الموجات، لا نتوقعها
‎ندخل عند الرفض، لا نطارد
‎نقبل الخسائر القليلة، نستفيد من التوسع الكبير

‎#موجات_اليوت #متداول_أسكالب #إطار_متعدد
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
                # Adjust limits based on timeframe
                if tf_name == "1H":
                    limit = 100
                elif tf_name in ["30M", "15M"]:
                    limit = 80
                else:
                    limit = 50
                
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
    
    async def get_active_pairs(self) -> List[Tuple[str, float]]:
        """Get active trading pairs"""
        try:
            tickers = await self.exchange.fetch_tickers()
            active_pairs = []
            
            for symbol, ticker in tickers.items():
                if symbol.endswith('/USDT'):
                    volume = ticker.get('quoteVolume', 0)
                    
                    if volume >= MIN_VOLUME_USD:
                        # Check price spread for liquidity
                        bid = ticker.get('bid', 0)
                        ask = ticker.get('ask', 0)
                        
                        if bid > 0 and ask > 0:
                            spread = (ask - bid) / bid * 100
                            if spread < 0.1:  # Good liquidity
                                active_pairs.append((symbol, volume))
            
            # Sort by volume
            active_pairs.sort(key=lambda x: x[1], reverse=True)
            
            # Take top N
            return active_pairs[:TOP_N_VOLUME]
            
        except Exception as e:
            log.error(f"Error getting pairs: {e}")
            return []
    
    async def save_signal(self, signal: ElliottWaveSignal) -> bool:
        """Save signal to database"""
        try:
            # Insert signal
            await self.db.execute("""
                INSERT INTO elliott_wave_signals (
                    id, symbol, side, entry_price, stop_loss, take_profit,
                    wave_type, wave_confidence, wave_maturity, wave_strength, trend_direction,
                    wave_momentum, volume_participation, ema_alignment, price_expansion, strength_score,
                    zone_type, rejection_strength, rsi_position, trigger_type, confirmation_candle,
                    risk_reward, expected_move, fibonacci_target, timeframe_used,
                    elliott_rules_met
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.side,
                signal.entry_price,
                signal.stop_loss,
                signal.take_profit,
                signal.wave_context.wave_type,
                signal.wave_context.wave_confidence,
                signal.wave_context.wave_maturity,
                signal.wave_context.wave_strength,
                signal.wave_context.trend_direction,
                signal.market_strength.wave_momentum,
                signal.market_strength.volume_participation,
                signal.market_strength.ema_alignment,
                signal.market_strength.price_expansion,
                signal.market_strength.strength_score,
                signal.rejection_zone.zone_type,
                signal.rejection_zone.strength,
                signal.rejection_zone.rsi_position,
                signal.trigger_type,
                signal.confirmation_candle,
                signal.risk_reward,
                signal.expected_move_pct,
                signal.fibonacci_target,
                signal.timeframe_used,
                json.dumps(signal.elliott_rules_met)
            ))
            
            await self.db.commit()
            
            log.info(f"✅ Elliott wave signal saved: {signal.symbol} ({signal.wave_context.wave_type})")
            return True
            
        except Exception as e:
            log.error(f"Error saving signal: {e}")
            return False
    
    async def format_signal_message(self, signal: ElliottWaveSignal) -> str:
        """Format signal for Telegram"""
        side_emoji = "🟢" if signal.side == "LONG" else "🔴"
        side_text = "شراء" if signal.side == "LONG" else "بيع"
        
        # Wave info
        wave_translation = {
            "IMPULSE_3": "موجة 3 دافعة",
            "IMPULSE_5": "موجة 5 نهائية",
            "CORRECTIVE_2": "تصحيح موجة 2",
            "CORRECTIVE_4": "تصحيح موجة 4",
            "CORRECTIVE_A": "موجة A تصحيحية",
            "CORRECTIVE_B": "موجة B تصحيحية",
            "CORRECTIVE_C": "موجة C تصحيحية"
        }
        
        wave_text = wave_translation.get(signal.wave_context.wave_type, signal.wave_context.wave_type)
        
        # Zone info
        zone_translation = {
            "FIBONACCI_SUPPORT": "دعم فيبوناتشي",
            "FIBONACCI_RESISTANCE": "مقاومة فيبوناتشي",
            "EMA_CONFLUENCE": "تقاطع المتوسطات المتحركة",
            "WAVE_2_SUPPORT": "دعم موجة 2",
            "WAVE_4_SUPPORT": "دعم موجة 4",
            "WAVE_5_RESISTANCE": "مقاومة موجة 5",
            "VOLUME_CLIMAX": "ذروة حجم",
            "ABSORPTION_ZONE": "منطقة امتصاص"
        }
        
        zone_text = zone_translation.get(signal.rejection_zone.zone_type, signal.rejection_zone.zone_type)
        
        # Fibonacci info
        fib_text = ""
        if signal.fibonacci_target is not None:
            fib_text = f"مستوى فيبوناتشي: {signal.fibonacci_target}"
        
        # Trigger info
        trigger_translation = {
            "WAVE_REVERSAL": "انعكاس موجة",
            "EMA_BOUNCE": "ارتداد من متوسط",
            "EMA_REJECTION": "رفض من متوسط"
        }
        
        trigger_text = trigger_translation.get(signal.trigger_type, signal.trigger_type)
        
        # Risk info
        risk_pct = abs(signal.entry_price - signal.stop_loss) / signal.entry_price * 100
        
        message = f"""
{side_emoji} <b>إشارة موجة يوت</b> 🌊

<b>{signal.symbol}</b> | {side_text}

<b>📊 تحليل الموجة:</b>
‎• نوع الموجة: {wave_text}
‎• ثقة الموجة: {signal.wave_context.wave_confidence:.1%}
‎• نضج الموجة: {signal.wave_context.wave_maturity:.1%}
‎• قوة الموجة: {signal.wave_context.wave_strength:.1%}
‎• الاتجاه الأساسي: {signal.wave_context.trend_direction}
{fib_text}

<b>💪 قوة السوق:</b>
‎• زخم الموجة: {signal.market_strength.wave_momentum:.1%}
‎• مشاركة الفوليوم: {signal.market_strength.volume_participation:.1%}
‎• محاذاة المتوسطات: {signal.market_strength.ema_alignment:.1%}
‎• توسع السعر: {signal.market_strength.price_expansion:.1%}
‎• درجة القوة: {signal.market_strength.strength_score:.1%}

<b>🎯 منطقة الرفض:</b>
‎• النوع: {zone_text}
‎• قوة المنطقة: {signal.rejection_zone.strength:.2f}
‎• تأكيد الفوليوم: {"✅" if signal.rejection_zone.volume_confirmation else "❌"}
‎• وضعية RSI: {signal.rejection_zone.rsi_position}

<b>⚡ تفاصيل الدخول:</b>
‎• نوع الزناد: {trigger_text}
‎• شمعة التأكيد: {signal.confirmation_candle}
‎• ثقة الموجة: {signal.wave_confidence:.1%}

<b>🔧 التنفيذ:</b>
‎• سعر الدخول: <code>{signal.entry_price:.6f}</code>
‎• وقف الخسارة: <code>{signal.stop_loss:.6f}</code> ({risk_pct:.2f}%)
‎• هدف الربح: <code>{signal.take_profit:.6f}</code> ({signal.expected_move_pct:.1f}%)
‎• نسبة الربح/المخاطرة: {signal.risk_reward:.1f}:1

<b>📋 قواعد يوت المحققة:</b>
{chr(10).join(['• ' + rule for rule in signal.elliott_rules_met[:5]])}

<b>🛡️ نظام التكرار:</b>
‎• نظام: <b>صفقة واحدة لكل عملة</b>
‎• لا إشارات جديدة لـ {signal.symbol} حتى إغلاق هذه الموجة

<b>⚠️ ملاحظة التاجر:</b>
‎ندخل عند رفض الموجة
‎نصطاد التوسع في الموجة التالية
‎نقبل التوقفات الصغيرة للوصول للأهداف الكبيرة

#{side_text} #موجة_يوت #{wave_text.replace(' ', '_')} #صفقة_واحدة
"""
        return message
    
    async def send_trade_trigger_notification(self, symbol: str, side: str, entry_price: float, wave_type: str):
        """Send notification when trade is triggered/entered"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning(f"⚠️ Telegram credentials missing. Skipping trigger notification for {symbol}")
            return
        
        try:
            side_emoji = "🟢" if side == "LONG" else "🔴"
            side_text = "شراء" if side == "LONG" else "بيع"
            
            wave_translation = {
                "IMPULSE_3": "موجة 3 دافعة",
                "IMPULSE_5": "موجة 5 نهائية",
                "CORRECTIVE_2": "تصحيح موجة 2",
                "CORRECTIVE_4": "تصحيح موجة 4"
            }
            
            wave_text = wave_translation.get(wave_type, wave_type)
            
            message = f"""
{side_emoji} <b>تم تنفيذ صفقة موجة يوت</b> 🌊

<b>{symbol}</b> | {side_text}

<b>🎯 تم الدخول عند:</b>
<code>{entry_price:.6f}</code>

<b>📊 تفاصيل الموجة:</b>
‎• نوع الموجة: {wave_text}
‎• الدخول عند رفض الموجة

<b>🧠 عقلية التاجر:</b>
‎• دخول مبكر في بداية الموجة الجديدة
‎• صيد للتوسع في الموجة القادمة
‎• راحة مع التوقفات الصغيرة
‎• تركيز على الموجات الكبيرة

<b>🛡️ نظام التكرار:</b>
❌ <b>ممنوع</b> إرسال إشارات جديدة لـ {symbol}
‎✅ مسموح بإشارات جديدة بعد إغلاق هذه الموجة

<b>⚠️ المتابعة:</b>
‎يتم متابعة الصفقة تلقائياً.
‎ستصلك إشعار عند اكتمال الموجة (وقف خسارة أو هدف ربح).

#{side_text} #تنفيذ_موجة #{wave_text.replace(' ', '_')} #متابعة
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
            
            log.info(f"{side_emoji} Elliott wave trade triggered: {symbol} {side} ({wave_type}) @ {entry_price:.4f}")
            
        except Exception as e:
            log.error(f"Trigger notification error: {e}")
    
    async def send_trade_close_notification(self, symbol: str, side: str, pnl_percent: float, 
                                           close_reason: str, entry_price: float, 
                                           close_price: float, risk_reward: float, wave_type: str):
        """Send notification when trade hits TP/SL"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning(f"⚠️ Telegram credentials missing. Skipping close notification for {symbol}")
            return
        
        try:
            if close_reason == "TP_HIT":
                emoji = "✅"
                result_text = "اكتمال الموجة (هدف الربح)"
                result_emoji = "🎯"
                color = "🟢"
                pnl_emoji = "💰"
            else:  # SL_HIT
                emoji = "❌"
                result_text = "وقف الموجة (وقف الخسارة)"
                result_emoji = "🛑"
                color = "🔴"
                pnl_emoji = "💸"
            
            side_text = "شراء" if side == "LONG" else "بيع"
            
            # Format P&L with sign
            pnl_formatted = f"+{pnl_percent:.2f}%" if pnl_percent > 0 else f"{pnl_percent:.2f}%"
            
            # Wave info
            wave_translation = {
                "IMPULSE_3": "موجة 3 دافعة",
                "IMPULSE_5": "موجة 5 نهائية",
                "CORRECTIVE_2": "تصحيح موجة 2",
                "CORRECTIVE_4": "تصحيح موجة 4"
            }
            
            wave_text = wave_translation.get(wave_type, wave_type)
            
            # Trader mindset message
            if close_reason == "TP_HIT":
                mindset = f"✅ الموجة اكتملت بنجاح ({wave_text}) - التوسع تم اصطياده"
            else:
                mindset = f"❌ الموجة لم تكتمل ({wave_text}) - ننتظر الموجة التالية"
            
            message = f"""
{emoji} <b>اكتمال صفقة موجة يوت</b> {result_emoji}

<b>{symbol}</b> | {side_text}

{color} <b>النتيجة: {result_text}</b>
{pnl_emoji} <b>النسبة: {pnl_formatted}</b>

<b>📊 تفاصيل الموجة:</b>
‎• نوع الموجة: {wave_text}
‎• نوع الدخول: {side_text} (عند رفض الموجة)
‎• سعر الدخول: <code>{entry_price:.6f}</code>
‎• سعر الإغلاق: <code>{close_price:.6f}</code>
‎• نسبة الربح/الخسارة: <b>{pnl_formatted}</b>
‎• نسبة الربح/المخاطرة المحققة: {risk_reward:.1f}:1

<b>🧠 عقلية التاجر:</b>
{mindset}
‎كل موجة تنتهي تفتح فرصة لموجة جديدة
‎نقبل توقف الموجات الصغيرة - نصطاد الموجات الكبيرة

<b>🛡️ نظام التكرار:</b>
✅ <b>مسموح الآن</b> بإرسال إشارات جديدة لـ {symbol}
‎يمكن للماسح الضوئي البحث عن موجة يوت جديدة لهذه العملة

#{side_text} #اكتمال_موجة #{"ربح" if close_reason == "TP_HIT" else "توقف"} #{wave_text.replace(' ', '_')} #مسموح_إشارات_جديدة
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
            
            log.info(f"{emoji} Elliott wave trade closed: {symbol} {side} {pnl_formatted} ({close_reason}, {wave_type})")
            
        except Exception as e:
            log.error(f"Close notification error: {e}")
    
    async def send_telegram_alert(self, signal: ElliottWaveSignal):
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
                
            log.info(f"📤 Telegram Elliott wave alert sent: {signal.symbol} ({signal.wave_context.wave_type})")
            
        except Exception as e:
            log.error(f"Telegram error: {e}")
    
    async def monitor_positions(self):
        """Monitor and close positions"""
        log.info("👀 Starting Elliott wave position monitoring...")
        
        while True:
            try:
                # Get ALL open positions
                async with self.db.execute("""
                    SELECT id, symbol, side, entry_price, stop_loss, take_profit, status, wave_type
                    FROM elliott_wave_signals 
                    WHERE status IN ('PENDING', 'TRIGGERED')
                """) as cursor:
                    positions = await cursor.fetchall()
                
                if positions:
                    log.debug(f"📊 Monitoring {len(positions)} open wave positions")
                
                for pos_id, symbol, side, entry, sl, tp, status, wave_type in positions:
                    try:
                        # Get current price
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        # For PENDING positions: check if price reached entry
                        if status == 'PENDING':
                            # Enter immediately at signal price for wave trading
                            if abs(current_price - entry) / entry <= 0.005:  # Within 0.5% zone
                                # Mark as triggered
                                await self.db.execute("""
                                    UPDATE elliott_wave_signals SET 
                                        status = 'TRIGGERED',
                                        triggered_at = CURRENT_TIMESTAMP,
                                        trigger_price = ?
                                    WHERE id = ?
                                """, (current_price, pos_id))
                                
                                await self.db.commit()
                                
                                # Update deduplication status to TRIGGERED
                                self.scanner.deduplicator.update_signal_status(pos_id, "TRIGGERED")
                                
                                # Send trigger notification
                                await self.send_trade_trigger_notification(symbol, side, current_price, wave_type)
                                
                                log.info(f"✅ Elliott wave position triggered: {symbol} {side} ({wave_type}) @ {current_price:.4f}")
                                continue
                        
                        # Check SL/TP for ALL positions
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
                            # Get risk_reward from database
                            async with self.db.execute("""
                                SELECT risk_reward FROM elliott_wave_signals WHERE id = ?
                            """, (pos_id,)) as cursor:
                                row = await cursor.fetchone()
                                risk_reward = row[0] if row else 0
                            
                            # Update database
                            await self.db.execute("""
                                UPDATE elliott_wave_signals SET 
                                    status = 'CLOSED',
                                    closed_at = CURRENT_TIMESTAMP,
                                    close_price = ?,
                                    pnl_percent = ?,
                                    close_reason = ?
                                WHERE id = ?
                            """, (current_price, pnl_percent, close_reason, pos_id))
                            
                            await self.db.commit()
                            
                            # Update deduplication status to CLOSED
                            self.scanner.deduplicator.update_signal_status(pos_id, "CLOSED")
                            
                            # Clean up from tracking
                            self.scanner.active_signal_ids.discard(pos_id)
                            
                            # Send close notification
                            await self.send_trade_close_notification(
                                symbol=symbol,
                                side=side,
                                pnl_percent=pnl_percent,
                                close_reason=close_reason,
                                entry_price=entry,
                                close_price=current_price,
                                risk_reward=risk_reward,
                                wave_type=wave_type
                            )
                            
                            log.info(f"📤 Wave close notification sent for {symbol}: {close_reason} ({pnl_percent:.2f}%)")
                    
                    except Exception as e:
                        log.error(f"Monitor error for {symbol}: {e}")
                        continue
                
                # Clean up old closed signals periodically
                if int(time.time()) % 300 < 2:
                    self.scanner.deduplicator.remove_closed_signals()
                
                # Fast monitoring for wave trading
                await asyncio.sleep(2)
                
            except Exception as e:
                log.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(5)
    
    async def high_freq_wave_scanning(self):
        """Main high-frequency scanning loop for Elliott Waves"""
        log.info("🚀 Starting Elliott Wave high-frequency scanning...")
        
        while True:
            try:
                self.scan_cycle += 1
                start_time = time.time()
                
                log.info(f"🔄 Wave scan cycle #{self.scan_cycle}")
                
                # Get active pairs
                pairs = await self.get_active_pairs()
                
                if not pairs:
                    log.warning("No active pairs found")
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                log.info(f"Scanning {len(pairs)} active pairs for Elliott Waves")
                
                signals_found = 0
                pairs_processed = 0
                
                # Ultra-fast scanning for wave setups
                for symbol, volume in pairs:
                    try:
                        # Fetch data
                        multi_tf_data = await self.fetch_timeframe_data(symbol)
                        
                        # Need key timeframes for Elliott Wave analysis
                        required_tfs = ["1H", "15M", "5M"]
                        has_all_data = all(tf in multi_tf_data for tf in required_tfs)
                        
                        if not has_all_data:
                            continue
                        
                        # Generate Elliott Wave signal
                        signal = self.scanner.generate_elliott_signal(multi_tf_data, symbol)
                        
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
                
                # Log wave stats
                active_count = len(self.scanner.deduplicator.active_signals)
                stats = self.scanner.get_daily_stats()
                
                log.info(f"📊 Wave stats: Found {signals_found}, Active: {active_count}")
                log.info(f"   Wave 3 setups: {stats.get('wave_3_setups', 0)}")
                log.info(f"   Wave 5 setups: {stats.get('wave_5_setups', 0)}")
                log.info(f"   Corrective setups: {stats.get('corrective_setups', 0)}")
                log.info(f"   Low confidence: {stats.get('low_confidence', 0)}")
                
                scan_duration = time.time() - start_time
                log.info(f"Wave scan #{self.scan_cycle}: {signals_found} setups in {scan_duration:.2f}s")
                
                # Log detailed stats periodically
                if self.scan_cycle % 20 == 0:
                    log.info(f"📈 Detailed wave stats: {stats}")
                
                # Wait for next scan
                wait_time = max(0.1, SCAN_INTERVAL - scan_duration)
                log.info(f"Next wave hunt in {wait_time:.1f}s...")
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
                self.high_freq_wave_scanning(),
                self.monitor_positions()
            )
            
        except KeyboardInterrupt:
            log.info("Elliott Wave scanner stopped by user")
            
            # Send final stats
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
            
            # Get active signals count
            active_count = len(self.scanner.deduplicator.active_signals)
            
            message = f"""
🌊 <b>إحصائيات نهائية لماسح موجات اليوت</b>

<b>📊 إحصائيات اليوم:</b>
‎• عمليات المسح: {self.scan_cycle}
‎• الأزواج الممسوحة: {stats['pairs_scanned']}
‎• إعدادات الموجات التي تم العثور عليها: {stats['wave_setups_found']}
‎• إعدادات موجة 3: {stats.get('wave_3_setups', 0)}
‎• إعدادات موجة 5: {stats.get('wave_5_setups', 0)}
‎• إعدادات تصحيحية: {stats.get('corrective_setups', 0)}

<b>🚫 أسباب الفلترة:</b>
‎• ثقة منخفضة في الموجة: {stats.get('low_confidence', 0)}
‎• بدون هيكل موجة واضح: {stats.get('no_wave_structure', 0)}
‎• رفض بسبب الفوليوم: {stats.get('volume_rejection', 0)}

<b>⚡ الموجات النشطة:</b>
‎• حالياً: {active_count} موجة نشطة

<b>✅ منهجية التاجر المحققة:</b>
‎• مراقبة كل الفريمات ✓
‎• تحديد الطول الموجي (موجات اليوت) ✓
‎• تحليل القوة والفوليوم ✓
‎• المؤشرات: RSI + EMA + VOLUME ✓
‎• الدخول عند الرفض فقط ✓
‎• صفقة واحدة لكل عملة ✓

‎#إحصائيات_موجات_اليوت #متداول_أسكالب #صفقة_واحدة
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
            
            # Parse request
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
                # Get scanner stats
                stats = scanner.scanner.get_daily_stats()
                active_count = len(scanner.scanner.deduplicator.active_signals)
                
                response = json.dumps({
                    "status": "running",
                    "scanner": "Elliott Wave High-Frequency Scalper",
                    "scan_cycle": scanner.scan_cycle,
                    "active_waves": active_count,
                    "daily_stats": stats,
                    "trader_methodology": {
                        "arabic_description": "مراقبة كل الفريمات + تحديد الطول الموجي (موجات اليوت) + تحليل القوة والفوليوم + المؤشرات RSI و EMA و VOLUME + الدخول عند الرفض",
                        "english_description": "Monitor all timeframes + Identify wave length (Elliott Waves) + Analyze strength and volume + RSI, EMA, VOLUME indicators + Enter at rejection",
                        "trading_style": "7-10 trades in couple hours with couple losses but mostly winning, wins from 1% to 5% moves",
                        "wave_focus": "Wave 3 setups (strongest), Wave 5 divergences, Corrective wave entries",
                        "risk_management": "1 trade per symbol, 1.2% max SL, 1-5% targets, min 1.5:1 R:R"
                    }
                }, indent=2)
            
            elif path == '/stats':
                response = json.dumps(scanner.scanner.get_daily_stats(), indent=2)
            
            elif path == '/methodology':
                response = json.dumps({
                    "methodology": "Elliott Wave High-Frequency Scalping",
                    "key_phrases": [
                        "أراقب كل الفريمات",
                        "أحدد الطول الموجي (موجات اليوت)", 
                        "أحلل القوة والفوليوم",
                        "أشوف المؤشرات RSI and EMA and VOL",
                        "أدخل عند الرفض",
                        "7-10 trades in couple hours",
                        "couple losses but mostly winning",
                        "wins from 1% to 5% moves"
                    ],
                    "wave_types_targeted": [
                        "IMPULSE_3 (Strongest wave, high volume, RSI 60-75)",
                        "IMPULSE_5 (With divergence, RSI 70-85)",
                        "CORRECTIVE_2 (Pullback, low volume, RSI 35-50)",
                        "CORRECTIVE_4 (Pullback, low volume, RSI 35-50)"
                    ],
                    "timeframes_used": {
                        "1H": "Primary wave degree",
                        "15M": "Wave structure and strength",
                        "5M": "Main entry timeframe",
                        "3M": "Entry timing",
                        "1M": "Precision entry"
                    }
                }, indent=2)
            
            elif path == '/recent':
                if scanner.db:
                    scanner.db.row_factory = aiosqlite.Row
                    async with scanner.db.execute("""
                        SELECT symbol, side, wave_type, zone_type, rsi_position,
                               entry_price, stop_loss, take_profit, risk_reward,
                               expected_move, created_at, status, close_reason, pnl_percent
                        FROM elliott_wave_signals 
                        ORDER BY created_at DESC 
                        LIMIT 20
                    """) as cursor:
                        rows = await cursor.fetchall()
                        signals = [dict(row) for row in rows]
                    
                    response = json.dumps({"signals": signals, "count": len(signals)}, indent=2)
                else:
                    response = json.dumps({"error": "Database not available"})
            
            else:
                response = json.dumps({"error": "Endpoint not found"})
            
            # Send response
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
    # Create scanner
    scanner = ElliottWaveScannerSystem()
    
    # Start HTTP server in background
    http_task = asyncio.create_task(start_http_server(scanner))
    
    # Run scanner
    await scanner.run()

if __name__ == "__main__":
    # Run the main async function
    asyncio.run(main())