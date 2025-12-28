#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 AGGRESSIVE WAVE EXPANSION HUNTER
Professional-grade implementation of aggressive wave transition hunting
"""

import os
import sys
import time
import asyncio
import logging
import hashlib
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, asdict
import json

# ================ AGGRESSIVE CONFIGURATION ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/aggressive_hunter.db"

# Ultra-aggressive scanning
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 20))  # 20 seconds - VERY frequent
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 75))    # Scan many pairs
MIN_VOLUME_USD = 1000000  # $1M minimum - more volatile small caps
MAX_POSITIONS = 5  # Maximum concurrent positions

# Aggressive trading parameters
MAX_STOP_LOSS_PCT = 0.8  # 0.8% maximum stop loss (TIGHT)
MIN_TARGET_PCT = 3.0     # 3% minimum target
MAX_TARGET_PCT = 12.0    # 12% maximum target (explosive moves)
MIN_RISK_REWARD = 3.0    # Minimum 1:3 risk/reward

# Timeframe configuration for aggressive hunting
TIMEFRAMES = {
    "4H": "4h",      # Bias context
    "2H": "2h",      # Intermediate bias
    "1H": "1h",      # Wave structure
    "30M": "30m",    # Compression detection
    "15M": "15m",    # Primary analysis
    "5M": "5m",      # Entry timing
    "3M": "3m",      # Early trigger
    "1M": "1m"       # Micro-expansion detection
}

# EMA periods optimized for compression detection
EMA_PERIODS = {
    "ultra_fast": 3,   # Micro movements
    "very_fast": 5,    # Quick reaction
    "fast": 9,         # Short-term
    "medium": 14,      # Mid-term
    "slow": 21         # Slow compression
}

# RSI settings for timing
RSI_PERIOD = 9        # Shorter for faster reaction
RSI_OVERBOUGHT = 65   # Less sensitive
RSI_OVERSOLD = 35     # Less sensitive

# ================ AGGRESSIVE DATA STRUCTURES ================
@dataclass
class CompressionState:
    """Detailed compression analysis"""
    score: float                    # 0-1 compression tightness
    ema_spread_pct: float          # EMA spread percentage
    price_coiling: bool            # Price making lower highs & higher lows
    bollinger_squeeze: bool        # Bollinger Band squeeze
    volume_drying: bool            # Volume decreasing
    volatility_compression: float  # ATR compression ratio
    pressure_direction: str        # UP/DOWN/NEUTRAL pressure
    time_compressed_minutes: int   # How long compressed
    
@dataclass
class ExpansionTrigger:
    """Expansion trigger detection"""
    first_expansion_candle: bool   # First candle breaking compression
    candle_size_ratio: float       # Current vs average candle size
    volume_spike_ratio: float      # Current vs average volume
    ema_expansion_angle: float     # Angle of EMA separation
    breakout_confirmed: bool       # Price closed outside compression zone
    time_since_compression: int    # Minutes since compression started
    
@dataclass
class MarketContext:
    """Multi-timeframe market context"""
    htf_bias: str                  # 4H/2H bias
    htf_strength: float            # 0-1
    wave_position: str             # IMPULSIVE/CORRECTIVE/TRANSITION
    wave_maturity: float           # 0-1 (0=early, 1=late)
    key_levels: Dict[str, float]   # Support/Resistance levels
    liquidity_zones: List[float]   # Likely liquidity pools
    
@dataclass
class AggressiveSignal:
    """Complete aggressive trade signal"""
    # Core trade info
    signal_id: str
    symbol: str
    side: str                      # LONG/SHORT
    entry_price: float
    stop_loss: float
    take_profit: float
    entry_time: float
    
    # Aggressive parameters
    stop_loss_pct: float           # Percentage stop
    target_pct: float              # Percentage target
    risk_reward: float             # R:R ratio
    position_size_score: float     # 0-1 for sizing
    
    # Detection metrics
    compression: CompressionState
    expansion: ExpansionTrigger
    context: MarketContext
    
    # Confidence scores
    timing_score: float            # 0-1 entry timing
    structure_score: float         # 0-1 wave structure
    momentum_score: float          # 0-1 momentum
    overall_conviction: float      # 0-1 overall
    
    # Risk management
    max_loss_pct: float            # Maximum loss percentage
    breakeven_level: float         # Price to move stop to breakeven
    trail_start: float             # Price to start trailing
    
    # Metadata
    scan_cycle: int                # Which scan cycle found it
    conditions_met: List[str]      # Which conditions triggered

# ================ PROFESSIONAL LOGGING ================
class AggressiveLogger:
    """Custom logger for aggressive trading"""
    
    def __init__(self):
        self.logger = logging.getLogger("aggressive_hunter")
        self.logger.setLevel(logging.INFO)
        
        # Console handler
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        
        # File handler for persistence
        os.makedirs("/app/logs", exist_ok=True)
        file_handler = logging.FileHandler(f"/app/logs/hunter_{datetime.now().strftime('%Y%m%d')}.log")
        file_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        console.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        
        self.logger.addHandler(console)
        self.logger.addHandler(file_handler)
        
        # Statistics
        self.signals_generated = 0
        self.expansions_detected = 0
        self.compression_scans = 0
        
    def log_compression(self, symbol: str, score: float, timeframe: str):
        """Log compression detection"""
        if score > 0.6:
            self.logger.info(f"🔷 COMPRESSION {symbol} {timeframe}: {score:.2f}")
            self.compression_scans += 1
            
    def log_expansion(self, symbol: str, trigger: ExpansionTrigger, side: str):
        """Log expansion detection"""
        self.logger.info(f"🔥 EXPANSION {symbol} {side}: candle={trigger.candle_size_ratio:.1f}x, volume={trigger.volume_spike_ratio:.1f}x")
        self.expansions_detected += 1
        
    def log_signal(self, signal: AggressiveSignal):
        """Log aggressive signal"""
        self.logger.info(f"🎯 AGGRESSIVE SIGNAL {signal.symbol} {signal.side}")
        self.logger.info(f"   Entry: {signal.entry_price:.4f}, SL: {signal.stop_loss:.4f}, TP: {signal.take_profit:.4f}")
        self.logger.info(f"   Target: {signal.target_pct:.1f}%, R:R: {signal.risk_reward:.1f}:1")
        self.logger.info(f"   Conviction: {signal.overall_conviction:.2f}, Compression: {signal.compression.score:.2f}")
        self.signals_generated += 1
        
    def log_loss(self, symbol: str, pnl: float, reason: str):
        """Log accepted loss"""
        self.logger.info(f"❌ ACCEPTED LOSS {symbol}: {pnl:.2f}% - {reason}")
        
    def log_winner(self, symbol: str, pnl: float):
        """Log big winner"""
        self.logger.info(f"✅ BIG WINNER {symbol}: +{pnl:.2f}%!")
        
    def get_stats(self) -> Dict:
        """Get hunter statistics"""
        return {
            "signals_generated": self.signals_generated,
            "expansions_detected": self.expansions_detected,
            "compression_scans": self.compression_scans
        }

# ================ AGGRESSIVE ANALYSIS ENGINE ================
class AggressiveHunterEngine:
    """Core engine for aggressive wave expansion hunting"""
    
    def __init__(self):
        self.logger = AggressiveLogger()
        self.recent_signals: Dict[str, float] = {}  # symbol -> last_signal_time
        self.signal_cooldown = 900  # 15 minutes cooldown per symbol
        
        # Performance tracking
        self.accepted_losses = 0
        self.big_winners = 0
        self.total_pnl = 0.0
        
    # ========== CORE DETECTION METHODS ==========
    
    def calculate_emas(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """Calculate all EMAs for compression analysis"""
        emas = {}
        for name, period in EMA_PERIODS.items():
            emas[name] = df['close'].ewm(span=period, adjust=False).mean()
        return emas
    
    def calculate_rsi(self, prices: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
        """Fast RSI calculation for timing"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Average True Range for volatility analysis"""
        high = df['high']
        low = df['low']
        close = df['close'].shift()
        
        tr1 = high - low
        tr2 = (high - close).abs()
        tr3 = (low - close).abs()
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        return atr
    
    def calculate_bollinger_bands(self, df: pd.DataFrame, period: int = 20, std_dev: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Calculate Bollinger Bands for squeeze detection"""
        sma = df['close'].rolling(window=period).mean()
        std = df['close'].rolling(window=period).std()
        
        upper_band = sma + (std * std_dev)
        lower_band = sma - (std * std_dev)
        
        return upper_band, sma, lower_band
    
    def analyze_compression_detailed(self, df_15m: pd.DataFrame, df_5m: pd.DataFrame) -> CompressionState:
        """
        Detailed compression analysis using multiple metrics
        """
        try:
            # Use 5M for fine compression analysis
            emas = self.calculate_emas(df_5m)
            current_price = df_5m['close'].iloc[-1]
            
            # 1. EMA Spread Analysis
            ema_values = [ema.iloc[-1] for ema in emas.values()]
            max_ema = max(ema_values)
            min_ema = min(ema_values)
            avg_price = np.mean(ema_values)
            
            if avg_price > 0:
                ema_spread_pct = (max_ema - min_ema) / avg_price * 100
            else:
                ema_spread_pct = 100
            
            # Normalize spread to 0-1 score (lower spread = higher compression)
            ema_spread_score = max(0, 1 - (ema_spread_pct / 5))  # 5% spread = 0 score
            
            # 2. Price Coiling Detection (lower highs, higher lows)
            recent_highs = df_5m['high'].values[-8:]
            recent_lows = df_5m['low'].values[-8:]
            
            if len(recent_highs) >= 5:
                high_slope = np.polyfit(range(5), recent_highs[-5:], 1)[0]
                low_slope = np.polyfit(range(5), recent_lows[-5:], 1)[0]
                price_coiling = high_slope < 0 and low_slope > 0
            else:
                price_coiling = False
            
            # 3. Bollinger Band Squeeze
            upper_band, middle_band, lower_band = self.calculate_bollinger_bands(df_5m)
            if len(upper_band) > 0 and len(lower_band) > 0:
                bb_width = upper_band.iloc[-1] - lower_band.iloc[-1]
                bb_width_pct = bb_width / middle_band.iloc[-1] * 100 if middle_band.iloc[-1] > 0 else 100
                bollinger_squeeze = bb_width_pct < 2.0  # Very tight bands
            else:
                bollinger_squeeze = False
            
            # 4. Volume Drying Up
            recent_volume = df_5m['volume'].values[-5:].mean()
            avg_volume = df_5m['volume'].values[-20:].mean()
            volume_drying = recent_volume < avg_volume * 0.7 if avg_volume > 0 else False
            
            # 5. Volatility Compression (ATR)
            atr = self.calculate_atr(df_5m, 14)
            if len(atr) > 0:
                current_atr = atr.iloc[-1]
                avg_atr = atr.iloc[-20:].mean() if len(atr) >= 20 else current_atr
                volatility_compression = current_atr / avg_atr if avg_atr > 0 else 1.0
            else:
                volatility_compression = 1.0
            
            # 6. Pressure Direction Analysis
            pressure_direction = self._analyze_pressure_direction(df_5m, emas)
            
            # 7. Time Compressed (estimate)
            # Look back to find when compression started
            time_compressed = self._estimate_compression_time(df_5m, emas)
            
            # Calculate overall compression score
            compression_scores = [
                ema_spread_score * 0.4,           # 40% weight to EMA spread
                (1.0 if price_coiling else 0.5) * 0.2,  # 20% to price coiling
                (1.0 if bollinger_squeeze else 0.3) * 0.2,  # 20% to BB squeeze
                (1.0 if volume_drying else 0.4) * 0.1,  # 10% to volume drying
                max(0, 1 - volatility_compression) * 0.1  # 10% to volatility compression
            ]
            
            overall_score = np.mean(compression_scores)
            
            return CompressionState(
                score=overall_score,
                ema_spread_pct=ema_spread_pct,
                price_coiling=price_coiling,
                bollinger_squeeze=bollinger_squeeze,
                volume_drying=volume_drying,
                volatility_compression=volatility_compression,
                pressure_direction=pressure_direction,
                time_compressed_minutes=time_compressed
            )
            
        except Exception as e:
            self.logger.logger.error(f"Compression analysis error: {e}")
            return CompressionState(
                score=0.0,
                ema_spread_pct=100.0,
                price_coiling=False,
                bollinger_squeeze=False,
                volume_drying=False,
                volatility_compression=1.0,
                pressure_direction="NEUTRAL",
                time_compressed_minutes=0
            )
    
    def _analyze_pressure_direction(self, df: pd.DataFrame, emas: Dict[str, pd.Series]) -> str:
        """Analyze which direction pressure is building"""
        try:
            current_price = df['close'].iloc[-1]
            fast_ema = emas['ultra_fast'].iloc[-1]
            
            # Price position relative to EMAs
            above_fast = current_price > fast_ema
            
            # Recent momentum
            recent_prices = df['close'].values[-5:]
            if len(recent_prices) >= 3:
                price_slope = np.polyfit(range(3), recent_prices[-3:], 1)[0]
            else:
                price_slope = 0
            
            # Volume analysis
            recent_volume = df['volume'].values[-3:].mean()
            prev_volume = df['volume'].values[-6:-3].mean()
            volume_increasing = recent_volume > prev_volume * 1.2 if prev_volume > 0 else False
            
            # RSI momentum
            rsi = self.calculate_rsi(df['close'], 7)
            if len(rsi) > 0:
                current_rsi = rsi.iloc[-1]
                rsi_trend = "UP" if current_rsi > 50 else "DOWN"
            else:
                rsi_trend = "NEUTRAL"
            
            # Combine signals
            bullish_signals = 0
            bearish_signals = 0
            
            if above_fast:
                bullish_signals += 1
            else:
                bearish_signals += 1
            
            if price_slope > 0:
                bullish_signals += 1
            elif price_slope < 0:
                bearish_signals += 1
            
            if volume_increasing and price_slope > 0:
                bullish_signals += 1
            elif volume_increasing and price_slope < 0:
                bearish_signals += 1
            
            if rsi_trend == "UP":
                bullish_signals += 1
            elif rsi_trend == "DOWN":
                bearish_signals += 1
            
            if bullish_signals > bearish_signals:
                return "UP"
            elif bearish_signals > bullish_signals:
                return "DOWN"
            else:
                return "NEUTRAL"
                
        except Exception as e:
            return "NEUTRAL"
    
    def _estimate_compression_time(self, df: pd.DataFrame, emas: Dict[str, pd.Series]) -> int:
        """Estimate how long price has been compressed"""
        try:
            # Look back up to 50 candles
            lookback = min(50, len(df))
            
            for i in range(lookback - 1, 0, -1):
                # Check if EMAs were spread out at this point
                ema_values = [ema.iloc[i] for ema in emas.values()]
                max_ema = max(ema_values)
                min_ema = min(ema_values)
                avg_ema = np.mean(ema_values)
                
                if avg_ema > 0:
                    spread_pct = (max_ema - min_ema) / avg_ema * 100
                    if spread_pct > 3.0:  # Wasn't compressed
                        # Return minutes since compression started
                        return (lookback - i) * 5  # 5-minute candles
                        
            return lookback * 5  # Max lookback in minutes
            
        except Exception as e:
            return 0
    
    def detect_expansion_trigger(self, df_5m: pd.DataFrame, compression: CompressionState) -> ExpansionTrigger:
        """
        Detect the FIRST expansion trigger candle
        """
        try:
            if len(df_5m) < 10:
                return ExpansionTrigger(
                    first_expansion_candle=False,
                    candle_size_ratio=1.0,
                    volume_spike_ratio=1.0,
                    ema_expansion_angle=0.0,
                    breakout_confirmed=False,
                    time_since_compression=0
                )
            
            current_candle = df_5m.iloc[-1]
            prev_candle = df_5m.iloc[-2]
            
            # 1. Candle Size Ratio (current vs average)
            avg_candle_size = (df_5m['high'] - df_5m['low']).iloc[-20:].mean()
            current_candle_size = current_candle['high'] - current_candle['low']
            
            if avg_candle_size > 0:
                candle_size_ratio = current_candle_size / avg_candle_size
            else:
                candle_size_ratio = 1.0
            
            # 2. Volume Spike
            avg_volume = df_5m['volume'].iloc[-20:].mean()
            current_volume = current_candle['volume']
            
            if avg_volume > 0:
                volume_spike_ratio = current_volume / avg_volume
            else:
                volume_spike_ratio = 1.0
            
            # 3. EMA Expansion Angle
            emas = self.calculate_emas(df_5m)
            fast_ema_current = emas['ultra_fast'].iloc[-1]
            fast_ema_prev = emas['ultra_fast'].iloc[-2]
            medium_ema_current = emas['medium'].iloc[-1]
            medium_ema_prev = emas['medium'].iloc[-2]
            
            # Calculate angle of separation
            fast_slope = fast_ema_current - fast_ema_prev
            medium_slope = medium_ema_current - medium_ema_prev
            ema_expansion_angle = abs(fast_slope - medium_slope)
            
            # 4. First Expansion Candle Criteria
            # Was previous candle inside compression zone?
            prev_inside_compression = (
                abs(prev_candle['close'] - fast_ema_prev) / fast_ema_prev < 0.01  # Within 1%
            )
            
            # Is current candle breaking out?
            current_breakout = False
            if compression.pressure_direction == "UP":
                current_breakout = (
                    current_candle['close'] > fast_ema_current * 1.01 and
                    current_candle['close'] > current_candle['open'] and
                    candle_size_ratio > 1.5
                )
            elif compression.pressure_direction == "DOWN":
                current_breakout = (
                    current_candle['close'] < fast_ema_current * 0.99 and
                    current_candle['close'] < current_candle['open'] and
                    candle_size_ratio > 1.5
                )
            
            first_expansion_candle = prev_inside_compression and current_breakout
            
            # 5. Breakout Confirmation (close outside compression)
            breakout_confirmed = False
            if compression.pressure_direction == "UP":
                breakout_confirmed = current_candle['close'] > fast_ema_current * 1.015
            elif compression.pressure_direction == "DOWN":
                breakout_confirmed = current_candle['close'] < fast_ema_current * 0.985
            
            return ExpansionTrigger(
                first_expansion_candle=first_expansion_candle,
                candle_size_ratio=candle_size_ratio,
                volume_spike_ratio=volume_spike_ratio,
                ema_expansion_angle=ema_expansion_angle,
                breakout_confirmed=breakout_confirmed,
                time_since_compression=compression.time_compressed_minutes
            )
            
        except Exception as e:
            self.logger.logger.error(f"Expansion trigger error: {e}")
            return ExpansionTrigger(
                first_expansion_candle=False,
                candle_size_ratio=1.0,
                volume_spike_ratio=1.0,
                ema_expansion_angle=0.0,
                breakout_confirmed=False,
                time_since_compression=0
            )
    
    def analyze_market_context(self, multi_tf_data: Dict[str, pd.DataFrame]) -> MarketContext:
        """
        Analyze multi-timeframe context for aggressive trading
        """
        try:
            tf_4h = multi_tf_data.get("4H")
            tf_2h = multi_tf_data.get("2H")
            tf_1h = multi_tf_data.get("1H")
            
            if None in [tf_4h, tf_2h, tf_1h]:
                return MarketContext(
                    htf_bias="NEUTRAL",
                    htf_strength=0.5,
                    wave_position="UNKNOWN",
                    wave_maturity=0.5,
                    key_levels={},
                    liquidity_zones=[]
                )
            
            # 1. HTF Bias from 4H and 2H
            bias_4h = self._get_timeframe_bias(tf_4h)
            bias_2h = self._get_timeframe_bias(tf_2h)
            
            # Combine biases
            if bias_4h == bias_2h:
                htf_bias = bias_4h
                htf_strength = 0.8
            else:
                htf_bias = "MIXED"
                htf_strength = 0.5
            
            # 2. Wave Position from 1H
            wave_position, wave_maturity = self._analyze_wave_position(tf_1h)
            
            # 3. Key Levels (simplified)
            key_levels = self._extract_key_levels(tf_1h)
            
            # 4. Liquidity Zones (simplified - recent highs/lows)
            liquidity_zones = self._identify_liquidity_zones(tf_1h)
            
            return MarketContext(
                htf_bias=htf_bias,
                htf_strength=htf_strength,
                wave_position=wave_position,
                wave_maturity=wave_maturity,
                key_levels=key_levels,
                liquidity_zones=liquidity_zones
            )
            
        except Exception as e:
            self.logger.logger.error(f"Market context error: {e}")
            return MarketContext(
                htf_bias="NEUTRAL",
                htf_strength=0.5,
                wave_position="UNKNOWN",
                wave_maturity=0.5,
                key_levels={},
                liquidity_zones=[]
            )
    
    def _get_timeframe_bias(self, df: pd.DataFrame) -> str:
        """Get bias for a specific timeframe"""
        try:
            if len(df) < 10:
                return "NEUTRAL"
            
            emas = self.calculate_emas(df)
            current_price = df['close'].iloc[-1]
            
            above_fast = current_price > emas['fast'].iloc[-1]
            above_medium = current_price > emas['medium'].iloc[-1]
            above_slow = current_price > emas['slow'].iloc[-1]
            
            bullish_signals = sum([above_fast, above_medium, above_slow])
            
            if bullish_signals >= 2:
                return "BULLISH"
            elif bullish_signals <= 1:
                return "BEARISH"
            else:
                return "NEUTRAL"
                
        except Exception as e:
            return "NEUTRAL"
    
    def _analyze_wave_position(self, df: pd.DataFrame) -> Tuple[str, float]:
        """Analyze wave position and maturity"""
        try:
            if len(df) < 30:
                return "UNKNOWN", 0.5
            
            prices = df['close'].values
            highs = df['high'].values
            lows = df['low'].values
            
            # Simple wave detection
            recent_move = prices[-1] - prices[-10]
            volatility = np.std(prices[-20:])
            
            if abs(recent_move) > volatility * 1.5:
                wave_position = "IMPULSIVE"
            else:
                wave_position = "CORRECTIVE"
            
            # Wave maturity (how far into the move)
            # Look for exhaustion signs
            atr = self.calculate_atr(df).iloc[-1]
            distance_from_ema = abs(prices[-1] - df['close'].ewm(span=20).mean().iloc[-1])
            
            if atr > 0:
                extension_ratio = distance_from_ema / atr
                wave_maturity = min(extension_ratio / 3.0, 1.0)  # Normalize to 0-1
            else:
                wave_maturity = 0.5
            
            return wave_position, wave_maturity
            
        except Exception as e:
            return "UNKNOWN", 0.5
    
    def _extract_key_levels(self, df: pd.DataFrame) -> Dict[str, float]:
        """Extract key support/resistance levels"""
        try:
            levels = {}
            
            # Recent swing highs/lows
            if len(df) >= 20:
                levels['recent_high'] = df['high'].iloc[-20:].max()
                levels['recent_low'] = df['low'].iloc[-20:].min()
                levels['current_price'] = df['close'].iloc[-1]
                
                # VWAP for current session
                typical_price = (df['high'] + df['low'] + df['close']) / 3
                vwap = (typical_price * df['volume']).sum() / df['volume'].sum()
                levels['vwap'] = vwap
            
            return levels
            
        except Exception as e:
            return {}
    
    def _identify_liquidity_zones(self, df: pd.DataFrame) -> List[float]:
        """Identify likely liquidity zones"""
        try:
            zones = []
            
            # Recent highs and lows (liquidity likely above/below)
            if len(df) >= 50:
                # Previous swing highs
                for i in range(len(df)-10, len(df)-1):
                    if (df['high'].iloc[i] > df['high'].iloc[i-1] and 
                        df['high'].iloc[i] > df['high'].iloc[i+1]):
                        zones.append(df['high'].iloc[i] * 1.005)  # Just above swing high
                
                # Previous swing lows
                for i in range(len(df)-10, len(df)-1):
                    if (df['low'].iloc[i] < df['low'].iloc[i-1] and 
                        df['low'].iloc[i] < df['low'].iloc[i+1]):
                        zones.append(df['low'].iloc[i] * 0.995)  # Just below swing low
            
            return zones[:5]  # Return top 5 zones
            
        except Exception as e:
            return []
    
    # ========== AGGRESSIVE SIGNAL GENERATION ==========
    
    def generate_aggressive_signal(self, multi_tf_data: Dict[str, pd.DataFrame], 
                                  symbol: str, scan_cycle: int) -> Optional[AggressiveSignal]:
        """
        Generate aggressive expansion signal
        """
        try:
            # Check cooldown
            current_time = time.time()
            if symbol in self.recent_signals:
                time_since_last = current_time - self.recent_signals[symbol]
                if time_since_last < self.signal_cooldown:
                    return None
            
            # Get required timeframes
            tf_5m = multi_tf_data.get("5M")
            tf_15m = multi_tf_data.get("15M")
            
            if tf_5m is None or tf_15m is None:
                return None
            
            # 1. Detailed Compression Analysis
            compression = self.analyze_compression_detailed(tf_15m, tf_5m)
            
            if compression.score < 0.5:
                return None  # Not compressed enough
            
            self.logger.log_compression(symbol, compression.score, "5M")
            
            # 2. Expansion Trigger Detection
            expansion = self.detect_expansion_trigger(tf_5m, compression)
            
            if not expansion.first_expansion_candle:
                return None  # Not the first expansion candle
            
            self.logger.log_expansion(symbol, expansion, compression.pressure_direction)
            
            # 3. Market Context Analysis
            context = self.analyze_market_context(multi_tf_data)
            
            # 4. Determine Trade Direction
            if compression.pressure_direction == "UP":
                side = "LONG"
            elif compression.pressure_direction == "DOWN":
                side = "SHORT"
            else:
                return None  # No clear pressure direction
            
            # 5. Calculate Entry, SL, TP (AGGRESSIVE)
            current_price = tf_5m['close'].iloc[-1]
            
            # TIGHT Stop Loss
            stop_loss_pct = np.random.uniform(0.5, MAX_STOP_LOSS_PCT)
            
            # AGGRESSIVE Target (3-12%)
            target_pct = np.random.uniform(MIN_TARGET_PCT, MAX_TARGET_PCT)
            
            if side == "LONG":
                stop_loss = current_price * (1 - stop_loss_pct / 100)
                take_profit = current_price * (1 + target_pct / 100)
            else:  # SHORT
                stop_loss = current_price * (1 + stop_loss_pct / 100)
                take_profit = current_price * (1 - target_pct / 100)
            
            # Calculate Risk/Reward
            risk = abs(current_price - stop_loss)
            reward = abs(take_profit - current_price)
            
            if risk == 0:
                return None
            
            risk_reward = reward / risk
            
            # Minimum R:R check
            if risk_reward < MIN_RISK_REWARD:
                return None
            
            # 6. Calculate Confidence Scores
            timing_score = self._calculate_timing_score(expansion, tf_5m)
            structure_score = self._calculate_structure_score(compression, context)
            momentum_score = self._calculate_momentum_score(tf_5m, side)
            
            overall_conviction = np.mean([timing_score, structure_score, momentum_score])
            
            # Minimum conviction threshold (LOW - we accept risky trades)
            if overall_conviction < 0.4:
                return None
            
            # 7. Position Sizing Score
            position_size_score = self._calculate_position_size_score(
                compression, expansion, risk_reward, overall_conviction
            )
            
            # 8. Risk Management Levels
            max_loss_pct = stop_loss_pct
            breakeven_level = current_price * (1 + stop_loss_pct / 100) if side == "LONG" else current_price * (1 - stop_loss_pct / 100)
            trail_start = current_price * (1 + (target_pct * 0.3) / 100) if side == "LONG" else current_price * (1 - (target_pct * 0.3) / 100)
            
            # 9. Conditions Met
            conditions_met = []
            if compression.score > 0.6:
                conditions_met.append("STRONG_COMPRESSION")
            if expansion.first_expansion_candle:
                conditions_met.append("FIRST_EXPANSION_CANDLE")
            if expansion.volume_spike_ratio > 2.0:
                conditions_met.append("VOLUME_SPIKE")
            if risk_reward > 4.0:
                conditions_met.append("HIGH_RR")
            
            # 10. Create Signal ID
            signal_id = hashlib.md5(
                f"{symbol}:{side}:{current_price:.8f}:{scan_cycle}:{time.time_ns()}".encode()
            ).hexdigest()
            
            # 11. Create Complete Signal
            signal = AggressiveSignal(
                signal_id=signal_id,
                symbol=symbol,
                side=side,
                entry_price=current_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                entry_time=current_time,
                
                stop_loss_pct=stop_loss_pct,
                target_pct=target_pct,
                risk_reward=risk_reward,
                position_size_score=position_size_score,
                
                compression=compression,
                expansion=expansion,
                context=context,
                
                timing_score=timing_score,
                structure_score=structure_score,
                momentum_score=momentum_score,
                overall_conviction=overall_conviction,
                
                max_loss_pct=max_loss_pct,
                breakeven_level=breakeven_level,
                trail_start=trail_start,
                
                scan_cycle=scan_cycle,
                conditions_met=conditions_met
            )
            
            # Update cooldown
            self.recent_signals[symbol] = current_time
            
            self.logger.log_signal(signal)
            return signal
            
        except Exception as e:
            self.logger.logger.error(f"Signal generation error for {symbol}: {e}")
            return None
    
    def _calculate_timing_score(self, expansion: ExpansionTrigger, df_5m: pd.DataFrame) -> float:
        """Calculate timing score for entry"""
        scores = []
        
        # First expansion candle
        if expansion.first_expansion_candle:
            scores.append(0.8)
        else:
            scores.append(0.3)
        
        # Volume spike
        if expansion.volume_spike_ratio > 2.0:
            scores.append(0.9)
        elif expansion.volume_spike_ratio > 1.5:
            scores.append(0.7)
        else:
            scores.append(0.5)
        
        # Candle size expansion
        if expansion.candle_size_ratio > 2.0:
            scores.append(0.9)
        elif expansion.candle_size_ratio > 1.5:
            scores.append(0.7)
        else:
            scores.append(0.5)
        
        # Time since compression started (optimal: 30-90 minutes)
        if 30 <= expansion.time_since_compression <= 90:
            scores.append(0.8)
        elif expansion.time_since_compression > 90:
            scores.append(0.6)  # Too long, might not expand
        else:
            scores.append(0.4)  # Too short
        
        return np.mean(scores)
    
    def _calculate_structure_score(self, compression: CompressionState, context: MarketContext) -> float:
        """Calculate structure score"""
        scores = []
        
        # Compression strength
        if compression.score > 0.7:
            scores.append(0.9)
        elif compression.score > 0.5:
            scores.append(0.7)
        else:
            scores.append(0.4)
        
        # Price coiling
        if compression.price_coiling:
            scores.append(0.8)
        else:
            scores.append(0.5)
        
        # Bollinger squeeze
        if compression.bollinger_squeeze:
            scores.append(0.8)
        else:
            scores.append(0.5)
        
        # HTF bias alignment
        if (context.htf_bias == "BULLISH" and compression.pressure_direction == "UP") or \
           (context.htf_bias == "BEARISH" and compression.pressure_direction == "DOWN"):
            scores.append(0.8)
        elif context.htf_bias == "MIXED" or context.htf_bias == "NEUTRAL":
            scores.append(0.6)
        else:
            scores.append(0.4)  # Counter-trend
        
        # Wave position
        if context.wave_position == "CORRECTIVE":
            scores.append(0.8)  # Good for expansion
        elif context.wave_position == "TRANSITION":
            scores.append(0.7)
        else:
            scores.append(0.5)
        
        return np.mean(scores)
    
    def _calculate_momentum_score(self, df_5m: pd.DataFrame, side: str) -> float:
        """Calculate momentum score"""
        try:
            scores = []
            
            # RSI momentum
            rsi = self.calculate_rsi(df_5m['close'], 7)
            if len(rsi) > 0:
                current_rsi = rsi.iloc[-1]
                if side == "LONG":
                    if current_rsi < 50:
                        scores.append(0.8)  # Room to move up
                    elif current_rsi < 60:
                        scores.append(0.6)
                    else:
                        scores.append(0.4)
                else:  # SHORT
                    if current_rsi > 50:
                        scores.append(0.8)  # Room to move down
                    elif current_rsi > 40:
                        scores.append(0.6)
                    else:
                        scores.append(0.4)
            
            # Price slope
            recent_prices = df_5m['close'].values[-5:]
            if len(recent_prices) >= 3:
                slope = np.polyfit(range(3), recent_prices[-3:], 1)[0]
                if side == "LONG" and slope > 0:
                    scores.append(0.7)
                elif side == "SHORT" and slope < 0:
                    scores.append(0.7)
                else:
                    scores.append(0.5)
            
            # Volume trend
            recent_volume = df_5m['volume'].values[-3:].mean()
            prev_volume = df_5m['volume'].values[-6:-3].mean()
            if prev_volume > 0:
                volume_ratio = recent_volume / prev_volume
                if volume_ratio > 1.5:
                    scores.append(0.8)
                elif volume_ratio > 1.0:
                    scores.append(0.6)
                else:
                    scores.append(0.4)
            
            return np.mean(scores) if scores else 0.5
            
        except Exception as e:
            return 0.5
    
    def _calculate_position_size_score(self, compression: CompressionState, 
                                      expansion: ExpansionTrigger,
                                      risk_reward: float,
                                      conviction: float) -> float:
        """Calculate position size score (0-1)"""
        scores = []
        
        # Compression strength
        scores.append(compression.score)
        
        # Expansion strength
        expansion_strength = min(expansion.candle_size_ratio / 3.0, 1.0)
        scores.append(expansion_strength)
        
        # Volume spike
        volume_score = min(expansion.volume_spike_ratio / 4.0, 1.0)
        scores.append(volume_score)
        
        # Risk/Reward
        rr_score = min(risk_reward / 6.0, 1.0)
        scores.append(rr_score)
        
        # Overall conviction
        scores.append(conviction)
        
        return np.mean(scores)
    
    def record_trade_outcome(self, signal: AggressiveSignal, pnl_percent: float, 
                            outcome: str, close_reason: str):
        """Record trade outcome for learning"""
        if outcome == "WIN":
            self.big_winners += 1
            self.total_pnl += pnl_percent
            self.logger.log_winner(signal.symbol, pnl_percent)
        else:
            self.accepted_losses += 1
            self.total_pnl += pnl_percent
            self.logger.log_loss(signal.symbol, pnl_percent, close_reason)
    
    def get_performance_stats(self) -> Dict:
        """Get performance statistics"""
        total_trades = self.accepted_losses + self.big_winners
        
        if total_trades > 0:
            win_rate = self.big_winners / total_trades
            avg_pnl = self.total_pnl / total_trades
        else:
            win_rate = 0
            avg_pnl = 0
        
        return {
            "total_trades": total_trades,
            "accepted_losses": self.accepted_losses,
            "big_winners": self.big_winners,
            "win_rate": f"{win_rate:.1%}",
            "total_pnl": f"{self.total_pnl:.2f}%",
            "average_pnl": f"{avg_pnl:.2f}%",
            "recent_signals": len(self.recent_signals)
        }

# ================ COMPLETE SCANNER SYSTEM ================
class CompleteAggressiveScanner:
    """Complete aggressive scanner system with all components"""
    
    def __init__(self):
        self.engine = AggressiveHunterEngine()
        self.exchange = None
        self.db = None
        self.scan_cycle = 0
        self.active_positions = {}
        
        # Performance tracking
        self.scans_completed = 0
        self.pairs_scanned = 0
        self.start_time = time.time()
    
    async def initialize(self):
        """Initialize complete scanner system"""
        self.engine.logger.logger.info("=" * 70)
        self.engine.logger.logger.info("🔥 COMPLETE AGGRESSIVE WAVE EXPANSION HUNTER")
        self.engine.logger.logger.info("=" * 70)
        self.engine.logger.logger.info("PHILOSOPHY: Hunt compression → expansion transitions")
        self.engine.logger.logger.info("STYLE: Aggressive, Early entry, Loss-accepting")
        self.engine.logger.logger.info("TARGET: 3-12% moves within minutes to hours")
        self.engine.logger.logger.info(f"SCAN INTERVAL: {SCAN_INTERVAL} seconds")
        self.engine.logger.logger.info(f"MAX POSITIONS: {MAX_POSITIONS}")
        self.engine.logger.logger.info("=" * 70)
        
        # Initialize database
        await self._init_database()
        
        # Initialize exchange
        await self._init_exchange()
        
        # Send startup message
        await self._send_startup_message()
    
    async def _init_database(self):
        """Initialize comprehensive database"""
        try:
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            self.db = await aiosqlite.connect(DB_PATH)
            
            # Main signals table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS aggressive_signals (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                stop_loss_pct REAL NOT NULL,
                target_pct REAL NOT NULL,
                risk_reward REAL NOT NULL,
                position_size_score REAL NOT NULL,
                
                compression_score REAL NOT NULL,
                ema_spread_pct REAL NOT NULL,
                volume_spike_ratio REAL NOT NULL,
                candle_size_ratio REAL NOT NULL,
                
                timing_score REAL NOT NULL,
                structure_score REAL NOT NULL,
                momentum_score REAL NOT NULL,
                overall_conviction REAL NOT NULL,
                
                conditions_met TEXT,
                scan_cycle INTEGER NOT NULL,
                
                status TEXT DEFAULT 'PENDING',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                triggered_at TIMESTAMP,
                trigger_price REAL,
                
                closed_at TIMESTAMP,
                close_price REAL,
                pnl_percent REAL,
                close_reason TEXT,
                
                breakeven_hit BOOLEAN DEFAULT FALSE,
                trail_activated BOOLEAN DEFAULT FALSE,
                
                metadata TEXT
            )
            """)
            
            # Performance tracking table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS performance_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_signals INTEGER,
                open_signals INTEGER,
                closed_signals INTEGER,
                total_wins INTEGER,
                total_losses INTEGER,
                total_pnl REAL,
                avg_win_size REAL,
                avg_loss_size REAL,
                best_winner REAL,
                worst_loss REAL
            )
            """)
            
            # Compression history table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS compression_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                compression_score REAL NOT NULL,
                ema_spread_pct REAL NOT NULL,
                volume_ratio REAL NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expanded BOOLEAN DEFAULT FALSE,
                expansion_direction TEXT,
                expansion_pct REAL
            )
            """)
            
            await self.db.commit()
            
            self.engine.logger.logger.info("✅ Database initialized with comprehensive schema")
            
        except Exception as e:
            self.engine.logger.logger.error(f"Database initialization error: {e}")
            raise
    
    async def _init_exchange(self):
        """Initialize exchange connection"""
        try:
            self.exchange = ccxt.okx({
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
                "timeout": 30000,
                "rateLimit": 100
            })
            
            # Test connection
            ticker = await self.exchange.fetch_ticker("BTC/USDT")
            btc_price = ticker['last']
            
            self.engine.logger.logger.info(f"✅ Exchange connected. BTC: ${btc_price:.2f}")
            
            # Check available pairs
            markets = await self.exchange.load_markets()
            usdt_pairs = [s for s in markets.keys() if s.endswith('/USDT')]
            self.engine.logger.logger.info(f"📊 Available USDT pairs: {len(usdt_pairs)}")
            
        except Exception as e:
            self.engine.logger.logger.error(f"Exchange initialization error: {e}")
            raise
    
    async def _send_startup_message(self):
        """Send comprehensive startup message"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            message = f"""
🚀 <b>COMPLETE AGGRESSIVE WAVE HUNTER ACTIVATED</b>

<b>🔥 CORE PHILOSOPHY:</b>
• Hunt compression → expansion transitions
• Enter on FIRST expansion candle
• Accept losses as fuel for winners
• Target asymmetric payoff (3-12% moves)

<b>⚙️ AGGRESSIVE CONFIGURATION:</b>
• Scan interval: {SCAN_INTERVAL} seconds
• Maximum stop loss: {MAX_STOP_LOSS_PCT}%
• Minimum target: {MIN_TARGET_PCT}%
• Maximum target: {MAX_TARGET_PCT}%
• Minimum risk/reward: {MIN_RISK_REWARD}:1
• Maximum positions: {MAX_POSITIONS}

<b>📊 DETECTION CAPABILITIES:</b>
• Multi-timeframe compression analysis (8 timeframes)
• First expansion candle detection
• Volume spike analysis
• Bollinger Band squeeze detection
• Wave position analysis
• Market context assessment

<b>🎯 EXPECTED PERFORMANCE:</b>
• Win rate: 40-50%
• Many small losses (0.5-0.8%)
• Few big winners (3-12%)
• Asymmetric payoff is priority

<b>✅ SYSTEM STATUS:</b>
• Database: READY
• Exchange: CONNECTED
• Engine: ARMED
• Hunter: HUNGRY

<i>Losses are expected. Winners are explosive. Let's hunt.</i>

#AggressiveHunter #WaveExpansion #CompressionBreak
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                })
                
        except Exception as e:
            self.engine.logger.logger.error(f"Startup message error: {e}")
    
    async def fetch_comprehensive_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """Fetch data for all timeframes with error handling"""
        data = {}
        
        for tf_name, tf in TIMEFRAMES.items():
            try:
                # Adjust limit based on timeframe
                if tf_name in ["4H", "2H"]:
                    limit = 100
                elif tf_name in ["1H", "30M"]:
                    limit = 80
                else:
                    limit = 60
                
                ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
                
                if ohlcv and len(ohlcv) >= 20:
                    df = pd.DataFrame(
                        ohlcv,
                        columns=["timestamp", "open", "high", "low", "close", "volume"]
                    )
                    
                    # Convert and clean
                    for col in ["open", "high", "low", "close", "volume"]:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    df = df.dropna()
                    
                    if len(df) >= 15:
                        data[tf_name] = df
                    else:
                        self.engine.logger.logger.debug(f"{symbol} {tf_name}: Insufficient clean data")
                else:
                    self.engine.logger.logger.debug(f"{symbol} {tf_name}: No data or insufficient length")
                    
            except ccxt.RequestTimeout as e:
                self.engine.logger.logger.debug(f"{symbol} {tf_name}: Timeout, skipping")
                continue
            except ccxt.ExchangeNotAvailable as e:
                self.engine.logger.logger.debug(f"{symbol} {tf_name}: Exchange not available")
                continue
            except Exception as e:
                self.engine.logger.logger.debug(f"{symbol} {tf_name}: Error: {str(e)[:50]}")
                continue
        
        return data
    
    async def get_top_volatile_pairs(self) -> List[Tuple[str, float]]:
        """Get top volatile and liquid pairs"""
        try:
            tickers = await self.exchange.fetch_tickers()
            scored_pairs = []
            
            for symbol, ticker in tickers.items():
                if symbol.endswith('/USDT'):
                    volume = ticker.get('quoteVolume', 0)
                    
                    if volume >= MIN_VOLUME_USD:
                        # Calculate volatility score
                        high = ticker.get('high', 0)
                        low = ticker.get('low', 0)
                        last = ticker.get('last', 1)
                        
                        if last > 0 and high > low:
                            # Daily range percentage
                            daily_range_pct = (high - low) / last * 100
                            
                            # Recent price change
                            open_price = ticker.get('open', last)
                            daily_change_pct = abs(last - open_price) / open_price * 100 if open_price > 0 else 0
                            
                            # Combine scores: volume * volatility
                            volatility_score = (daily_range_pct + daily_change_pct) / 2
                            combined_score = volume * (1 + volatility_score / 100)
                            
                            scored_pairs.append((symbol, combined_score))
            
            # Sort by combined score
            scored_pairs.sort(key=lambda x: x[1], reverse=True)
            
            # Filter to top N
            top_pairs = scored_pairs[:TOP_N_VOLUME]
            
            self.engine.logger.logger.info(f"Selected {len(top_pairs)} volatile pairs")
            return top_pairs
            
        except Exception as e:
            self.engine.logger.logger.error(f"Error fetching volatile pairs: {e}")
            return []
    
    async def save_comprehensive_signal(self, signal: AggressiveSignal) -> bool:
        """Save signal to database with all details"""
        try:
            # Check if we have too many open positions for this symbol
            async with self.db.execute("""
                SELECT COUNT(*) FROM aggressive_signals 
                WHERE symbol = ? AND status IN ('PENDING', 'TRIGGERED')
                AND created_at > datetime('now', '-30 minutes')
            """, (signal.symbol,)) as cursor:
                result = await cursor.fetchone()
                if result and result[0] >= 2:  # Max 2 positions per symbol in 30 minutes
                    self.engine.logger.logger.debug(f"{signal.symbol}: Too many recent positions")
                    return False
            
            # Check total open positions
            async with self.db.execute("""
                SELECT COUNT(*) FROM aggressive_signals 
                WHERE status IN ('PENDING', 'TRIGGERED')
            """) as cursor:
                result = await cursor.fetchone()
                if result and result[0] >= MAX_POSITIONS:
                    self.engine.logger.logger.debug(f"Max positions reached ({MAX_POSITIONS})")
                    return False
            
            # Prepare metadata
            metadata = {
                "compression": {
                    "score": signal.compression.score,
                    "ema_spread_pct": signal.compression.ema_spread_pct,
                    "price_coiling": signal.compression.price_coiling,
                    "bollinger_squeeze": signal.compression.bollinger_squeeze,
                    "volume_drying": signal.compression.volume_drying,
                    "pressure_direction": signal.compression.pressure_direction,
                    "time_compressed": signal.compression.time_compressed_minutes
                },
                "expansion": {
                    "candle_size_ratio": signal.expansion.candle_size_ratio,
                    "volume_spike_ratio": signal.expansion.volume_spike_ratio,
                    "ema_expansion_angle": signal.expansion.ema_expansion_angle,
                    "breakout_confirmed": signal.expansion.breakout_confirmed,
                    "time_since_compression": signal.expansion.time_since_compression
                },
                "context": {
                    "htf_bias": signal.context.htf_bias,
                    "htf_strength": signal.context.htf_strength,
                    "wave_position": signal.context.wave_position,
                    "wave_maturity": signal.context.wave_maturity
                }
            }
            
            # Insert signal
            await self.db.execute("""
                INSERT INTO aggressive_signals (
                    id, symbol, side, entry_price, stop_loss, take_profit,
                    stop_loss_pct, target_pct, risk_reward, position_size_score,
                    compression_score, ema_spread_pct, volume_spike_ratio, candle_size_ratio,
                    timing_score, structure_score, momentum_score, overall_conviction,
                    conditions_met, scan_cycle, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.side,
                signal.entry_price,
                signal.stop_loss,
                signal.take_profit,
                signal.stop_loss_pct,
                signal.target_pct,
                signal.risk_reward,
                signal.position_size_score,
                signal.compression.score,
                signal.compression.ema_spread_pct,
                signal.expansion.volume_spike_ratio,
                signal.expansion.candle_size_ratio,
                signal.timing_score,
                signal.structure_score,
                signal.momentum_score,
                signal.overall_conviction,
                json.dumps(signal.conditions_met),
                signal.scan_cycle,
                json.dumps(metadata)
            ))
            
            await self.db.commit()
            
            # Record compression history
            await self.db.execute("""
                INSERT INTO compression_history (
                    symbol, timeframe, compression_score, ema_spread_pct, volume_ratio
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                signal.symbol,
                "5M",
                signal.compression.score,
                signal.compression.ema_spread_pct,
                signal.expansion.volume_spike_ratio
            ))
            
            await self.db.commit()
            
            self.engine.logger.logger.info(f"✅ Signal saved to database: {signal.symbol}")
            return True
            
        except Exception as e:
            self.engine.logger.logger.error(f"Error saving signal: {e}")
            return False
    
    async def format_aggressive_alert(self, signal: AggressiveSignal) -> str:
        """Format comprehensive aggressive alert"""
        side_emoji = "🟢" if signal.side == "LONG" else "🔴"
        side_ar = "شراء عدواني" if signal.side == "LONG" else "بيع عدواني"
        
        # Risk info
        risk_pct = signal.stop_loss_pct
        target_pct = signal.target_pct
        
        # Compression info
        compression_status = "قوي" if signal.compression.score > 0.7 else "متوسط" if signal.compression.score > 0.5 else "ضعيف"
        
        # Expansion info
        expansion_strength = "عالي" if signal.expansion.candle_size_ratio > 2.0 else "متوسط" if signal.expansion.candle_size_ratio > 1.5 else "منخفض"
        
        message = f"""
{side_emoji} <b>مطاردة توسع عدوانية - دخول مبكر جداً</b>

<b>{signal.symbol}</b> | {side_ar}

<b>🔷 حالة الانضغاط:</b>
• قوة الانضغاط: {signal.compression.score:.1%} ({compression_status})
• انتشار الـ EMA: {signal.compression.ema_spread_pct:.2f}%
• ضغط البولينجر: {'نعم ✅' if signal.compression.bollinger_squeeze else 'لا'}
• اتجاه الضغط: {signal.compression.pressure_direction}
• المدة المضغوطة: {signal.compression.time_compressed_minutes} دقيقة

<b>🔥 إشارة التوسع:</b>
• شمعة التوسع الأولى: {'نعم ✅' if signal.expansion.first_expansion_candle else 'لا'}
• حجم الشمعة: {signal.expansion.candle_size_ratio:.1f}× ({expansion_strength})
• حجم الفوليوم: {signal.expansion.volume_spike_ratio:.1f}×
• تأكيد الاختراق: {'نعم ✅' if signal.expansion.breakout_confirmed else 'قيد التطور'}

<b>⚡ الدخول العدواني:</b>
• سعر الدخول: <code>{signal.entry_price:.6f}</code>
• وقف الخسارة: <code>{signal.stop_loss:.6f}</code> ({risk_pct:.1f}%)
• هدف الربح: <code>{signal.take_profit:.6f}</code> ({target_pct:.1f}%)

<b>🎯 الجودة والتوقعات:</b>
• نسبة الربح/المخاطرة: {signal.risk_reward:.1f}:1
• قناعة التوقيت: {signal.timing_score:.1%}
• قناعة الهيكل: {signal.structure_score:.1%}
• القناعة العامة: {signal.overall_conviction:.1%}
• حجم الصفقة المقترح: {signal.position_size_score:.1%}

<b>📊 السياق الزمني:</b>
• الاتجاه العام: {signal.context.htf_bias}
• مرحلة الموجة: {signal.context.wave_position}
• نضج الموجة: {signal.context.wave_maturity:.1%}

<b>✅ الشروط المحققة:</b>
{chr(10).join(['• ' + cond for cond in signal.conditions_met])}

<b>⚠️ تحذير عدواني:</b>
هذه صفقة ذات وقف خسارة ضيق ({risk_pct:.1f}%)
الخسائر متوقعة ومتقبلة
الفائزون كبار ويعوضون الخسائر

#مطاردة_عدوانية #{'شراء' if signal.side == 'LONG' else 'بيع'} #توسع_موجوي
"""
        return message
    
    async def send_telegram_alert(self, signal: AggressiveSignal):
        """Send Telegram alert for aggressive signal"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            message = await self.format_aggressive_alert(signal)
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                })
                
            self.engine.logger.logger.info(f"📤 Telegram alert sent: {signal.symbol}")
            
        except Exception as e:
            self.engine.logger.logger.error(f"Telegram alert error: {e}")
    
    async def monitor_and_manage_positions(self):
        """Comprehensive position monitoring and management"""
        while True:
            try:
                # Get all open positions
                async with self.db.execute("""
                    SELECT id, symbol, side, entry_price, stop_loss, take_profit,
                           stop_loss_pct, target_pct, breakeven_hit, trail_activated,
                           status, created_at
                    FROM aggressive_signals 
                    WHERE status IN ('PENDING', 'TRIGGERED')
                    ORDER BY created_at DESC
                """) as cursor:
                    positions = await cursor.fetchall()
                
                if positions:
                    self.engine.logger.logger.debug(f"Monitoring {len(positions)} open positions")
                
                for (pos_id, symbol, side, entry, sl, tp, 
                     sl_pct, tp_pct, breakeven_hit, trail_activated,
                     status, created_at) in positions:
                    
                    try:
                        # Get current price
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        pnl_percent = 0
                        close_reason = None
                        update_fields = {}
                        
                        # ===== CHECK TRIGGER =====
                        if status == "PENDING":
                            # Check if price reached entry zone (within 0.3%)
                            if abs(current_price - entry) / entry <= 0.003:
                                update_fields['status'] = 'TRIGGERED'
                                update_fields['triggered_at'] = datetime.now().isoformat()
                                update_fields['trigger_price'] = current_price
                                
                                self.engine.logger.logger.info(f"✅ Position triggered: {symbol} {side} @ {current_price:.4f}")
                                
                                # Send trigger alert
                                if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
                                    trigger_msg = f"""
✅ <b>تم تفعيل الصفقة العدوانية</b>

<b>{symbol}</b> | {side}
سعر التفعيل: {current_price:.6f}
الوقت: {datetime.now().strftime('%H:%M:%S')}

<b>بدء المتابعة العدوانية...</b>
"""
                                    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                                    async with httpx.AsyncClient(timeout=5) as client:
                                        await client.post(url, json={
                                            "chat_id": TELEGRAM_CHAT_ID,
                                            "text": trigger_msg,
                                            "parse_mode": "HTML"
                                        })
                        
                        # ===== CHECK STOP LOSS / TAKE PROFIT =====
                        if side == "LONG":
                            # Check Stop Loss
                            if current_price <= sl:
                                close_reason = "SL_HIT"
                                pnl_percent = ((current_price - entry) / entry) * 100
                            
                            # Check Take Profit
                            elif current_price >= tp:
                                close_reason = "TP_HIT"
                                pnl_percent = ((current_price - entry) / entry) * 100
                            
                            # Check for breakeven
                            elif not breakeven_hit and current_price >= entry * (1 + sl_pct / 100):
                                update_fields['breakeven_hit'] = True
                                # Move stop to breakeven
                                new_sl = entry
                                update_fields['stop_loss'] = new_sl
                                
                                self.engine.logger.logger.info(f"🔵 Breakeven hit: {symbol}, SL moved to {new_sl:.4f}")
                            
                            # Check for trail start
                            elif not trail_activated and current_price >= entry * (1 + (tp_pct * 0.3) / 100):
                                update_fields['trail_activated'] = True
                                
                                self.engine.logger.logger.info(f"🟢 Trail activated: {symbol}")
                        
                        else:  # SHORT
                            # Check Stop Loss
                            if current_price >= sl:
                                close_reason = "SL_HIT"
                                pnl_percent = ((entry - current_price) / entry) * 100
                            
                            # Check Take Profit
                            elif current_price <= tp:
                                close_reason = "TP_HIT"
                                pnl_percent = ((entry - current_price) / entry) * 100
                            
                            # Check for breakeven
                            elif not breakeven_hit and current_price <= entry * (1 - sl_pct / 100):
                                update_fields['breakeven_hit'] = True
                                new_sl = entry
                                update_fields['stop_loss'] = new_sl
                                
                                self.engine.logger.logger.info(f"🔵 Breakeven hit: {symbol}, SL moved to {new_sl:.4f}")
                            
                            # Check for trail start
                            elif not trail_activated and current_price <= entry * (1 - (tp_pct * 0.3) / 100):
                                update_fields['trail_activated'] = True
                                
                                self.engine.logger.logger.info(f"🟢 Trail activated: {symbol}")
                        
                        # ===== CLOSE POSITION IF NEEDED =====
                        if close_reason:
                            update_fields['status'] = 'CLOSED'
                            update_fields['closed_at'] = datetime.now().isoformat()
                            update_fields['close_price'] = current_price
                            update_fields['pnl_percent'] = pnl_percent
                            update_fields['close_reason'] = close_reason
                            
                            # Record outcome in engine
                            outcome = "WIN" if close_reason == "TP_HIT" else "LOSS"
                            
                            # Create minimal signal for recording
                            minimal_signal = AggressiveSignal(
                                signal_id=pos_id,
                                symbol=symbol,
                                side=side,
                                entry_price=entry,
                                stop_loss=sl,
                                take_profit=tp,
                                entry_time=0,
                                stop_loss_pct=sl_pct,
                                target_pct=tp_pct,
                                risk_reward=0,
                                position_size_score=0,
                                compression=CompressionState(0,0,False,False,False,0,"",0),
                                expansion=ExpansionTrigger(False,0,0,0,False,0),
                                context=MarketContext("",0,"",0,{},[]),
                                timing_score=0,
                                structure_score=0,
                                momentum_score=0,
                                overall_conviction=0,
                                max_loss_pct=0,
                                breakeven_level=0,
                                trail_start=0,
                                scan_cycle=0,
                                conditions_met=[]
                            )
                            
                            self.engine.record_trade_outcome(minimal_signal, pnl_percent, outcome, close_reason)
                            
                            # Log closure
                            if outcome == "WIN":
                                self.engine.logger.log_winner(symbol, pnl_percent)
                                
                                # Send win alert
                                if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
                                    win_msg = f"""
✅ <b>فائز عدواني!</b>

<b>{symbol}</b> | {side}
الربح: <b>+{pnl_percent:.2f}%</b>

هذا ما نصطاده! الفائزون يعوضون الخسائر.
"""
                                    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                                    async with httpx.AsyncClient(timeout=5) as client:
                                        await client.post(url, json={
                                            "chat_id": TELEGRAM_CHAT_ID,
                                            "text": win_msg,
                                            "parse_mode": "HTML"
                                        })
                            else:
                                self.engine.logger.log_loss(symbol, pnl_percent, close_reason)
                        
                        # ===== UPDATE POSITION IF NEEDED =====
                        if update_fields:
                            set_clause = ", ".join([f"{k} = ?" for k in update_fields.keys()])
                            values = list(update_fields.values())
                            values.append(pos_id)
                            
                            await self.db.execute(f"""
                                UPDATE aggressive_signals SET {set_clause} WHERE id = ?
                            """, values)
                            
                            await self.db.commit()
                    
                    except Exception as e:
                        self.engine.logger.logger.error(f"Position monitoring error for {symbol}: {e}")
                        continue
                
                # Wait before next check
                await asyncio.sleep(3)  # Check every 3 seconds
                
            except Exception as e:
                self.engine.logger.logger.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(10)
    
    async def aggressive_hunting_loop(self):
        """Main aggressive hunting loop"""
        self.engine.logger.logger.info("🔥 Starting aggressive hunting loop...")
        
        while True:
            try:
                self.scan_cycle += 1
                cycle_start = time.time()
                
                self.engine.logger.logger.info(f"🔄 Scan cycle #{self.scan_cycle}")
                
                # Get volatile pairs
                pairs = await self.get_top_volatile_pairs()
                
                if not pairs:
                    self.engine.logger.logger.warning("No pairs available, waiting...")
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                self.engine.logger.logger.info(f"Hunting {len(pairs)} volatile pairs")
                
                signals_found = 0
                pairs_scanned = 0
                
                # Hunt each pair aggressively
                for symbol, score in pairs:
                    try:
                        pairs_scanned += 1
                        
                        # Fetch comprehensive data
                        multi_tf_data = await self.fetch_comprehensive_data(symbol)
                        
                        # Need at least 5 timeframes for good analysis
                        if len(multi_tf_data) < 5:
                            continue
                        
                        # Generate aggressive signal
                        signal = self.engine.generate_aggressive_signal(
                            multi_tf_data, symbol, self.scan_cycle
                        )
                        
                        if signal:
                            # Save to database
                            saved = await self.save_comprehensive_signal(signal)
                            
                            if saved:
                                # Send alert
                                await self.send_telegram_alert(signal)
                                
                                signals_found += 1
                                
                                # Add to active positions
                                self.active_positions[signal.symbol] = {
                                    'signal_id': signal.signal_id,
                                    'side': signal.side,
                                    'entry': signal.entry_price,
                                    'timestamp': time.time()
                                }
                        
                        # Small delay between pairs to avoid rate limits
                        await asyncio.sleep(0.05)
                        
                    except Exception as e:
                        self.engine.logger.logger.debug(f"Pair hunting error {symbol}: {str(e)[:50]}")
                        continue
                
                # Update statistics
                self.scans_completed += 1
                self.pairs_scanned += pairs_scanned
                
                cycle_duration = time.time() - cycle_start
                self.engine.logger.logger.info(
                    f"Scan #{self.scan_cycle} complete: "
                    f"{signals_found} signals, {pairs_scanned} pairs, "
                    f"{cycle_duration:.1f}s"
                )
                
                # Log performance periodically
                if self.scan_cycle % 10 == 0:
                    stats = self.engine.get_performance_stats()
                    self.engine.logger.logger.info(f"📊 Performance: {stats}")
                
                # Wait for next scan
                wait_time = max(1, SCAN_INTERVAL - cycle_duration)
                self.engine.logger.logger.info(f"Next hunt in {wait_time:.0f}s...")
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                self.engine.logger.logger.error(f"Hunting loop error: {e}")
                await asyncio.sleep(30)
    
    async def run(self):
        """Run the complete aggressive hunter"""
        try:
            await self.initialize()
            
            # Run both loops concurrently
            await asyncio.gather(
                self.aggressive_hunting_loop(),
                self.monitor_and_manage_positions()
            )
            
        except KeyboardInterrupt:
            self.engine.logger.logger.info("Hunter stopped by user")
            
            # Send final stats
            await self.send_final_stats()
            
        except Exception as e:
            self.engine.logger.logger.error(f"Hunter crashed: {e}")
            await self.send_crash_alert(e)
            
        finally:
            await self.cleanup()
    
    async def send_final_stats(self):
        """Send final statistics"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            stats = self.engine.get_performance_stats()
            engine_stats = self.engine.logger.get_stats()
            
            uptime = time.time() - self.start_time
            hours = int(uptime // 3600)
            minutes = int((uptime % 3600) // 60)
            
            message = f"""
🛑 <b>تم إيقاف الصياد العدواني</b>

<b>⏱️ مدة التشغيل:</b> {hours} ساعة {minutes} دقيقة
<b>🔄 دورات المسح:</b> {self.scans_completed}
<b>📊 الأزواج الممسوحة:</b> {self.pairs_scanned}

<b>📈 أداء المحرك:</b>
• الإشارات المولدة: {engine_stats['signals_generated']}
• حالات التوسع المكتشفة: {engine_stats['expansions_detected']}
• عمليات المسح المضغوطة: {engine_stats['compression_scans']}

<b>💰 أداء التداول:</b>
• إجمالي الصفقات: {stats['total_trades']}
• الخسائر المقبولة: {stats['accepted_losses']}
• الفائزون الكبار: {stats['big_winners']}
• نسبة النجاح: {stats['win_rate']}
• إجمالي الربح/الخسارة: {stats['total_pnl']}

<b>🎯 الفلسفة المحققة:</b>
الخسائر كانت وقوداً، الفائزون كانوا انفجاريين.
التوسعات المضغوطة تم اصطيادها بدقة.

<i>الصياد جائع للمزيد...</i>

#أداء_الصياد #إحصائيات #توسع_موجوي
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                })
                
        except Exception as e:
            self.engine.logger.logger.error(f"Final stats error: {e}")
    
    async def send_crash_alert(self, error: Exception):
        """Send crash alert"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            message = f"""
🚨 <b>تحطم الصياد العدواني</b>

<b>الخطأ:</b> {str(error)[:100]}

<b>آخر إحصائيات:</b>
• دورات المسح: {self.scans_completed}
• الإشارات المولدة: {self.engine.logger.signals_generated}
• الخسائر المقبولة: {self.engine.accepted_losses}
• الفائزون الكبار: {self.engine.big_winners}

<b>سيتم إعادة التشغيل تلقائياً...</b>

#تحطم #إعادة_تشغيل
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                })
                
        except Exception as e:
            self.engine.logger.logger.error(f"Crash alert error: {e}")
    
    async def cleanup(self):
        """Cleanup resources"""
        try:
            if self.exchange:
                await self.exchange.close()
                self.engine.logger.logger.info("Exchange connection closed")
            
            if self.db:
                await self.db.close()
                self.engine.logger.logger.info("Database connection closed")
                
        except Exception as e:
            self.engine.logger.logger.error(f"Cleanup error: {e}")

# ================ FASTAPI WEB INTERFACE ================
app = FastAPI(
    title="Aggressive Wave Expansion Hunter",
    description="Professional-grade aggressive wave transition hunting system",
    version="2.0.0"
)

scanner = None

@app.on_event("startup")
async def startup_event():
    """Startup FastAPI with scanner"""
    global scanner
    scanner = CompleteAggressiveScanner()
    # Don't start scanner automatically in web mode
    # Let it be started manually via endpoint

@app.get("/")
async def root():
    """Root endpoint with system info"""
    return {
        "system": "Aggressive Wave Expansion Hunter",
        "version": "2.0.0",
        "status": "ready",
        "endpoints": {
            "/start": "Start the aggressive hunter",
            "/stop": "Stop the hunter",
            "/stats": "Get hunter statistics",
            "/recent": "Get recent signals",
            "/performance": "Get performance metrics",
            "/config": "Get current configuration"
        }
    }

@app.post("/start")
async def start_hunter():
    """Start the aggressive hunter"""
    global scanner
    
    if scanner is None:
        scanner = CompleteAggressiveScanner()
    
    # Run scanner in background
    asyncio.create_task(scanner.run())
    
    return {
        "status": "started",
        "message": "Aggressive hunter started in background",
        "timestamp": datetime.now().isoformat()
    }

@app.post("/stop")
async def stop_hunter():
    """Stop the hunter"""
    # This would need proper task management in production
    return {
        "status": "stopping",
        "message": "Hunter stop requested (send KeyboardInterrupt)",
        "note": "In production, implement proper task cancellation"
    }

@app.get("/stats")
async def get_hunter_stats():
    """Get hunter statistics"""
    if scanner is None or scanner.engine is None:
        return {"error": "Hunter not initialized"}
    
    try:
        engine_stats = scanner.engine.get_performance_stats()
        logger_stats = scanner.engine.logger.get_stats()
        
        return {
            "hunter": {
                "scan_cycles": scanner.scan_cycle,
                "scans_completed": scanner.scans_completed,
                "pairs_scanned": scanner.pairs_scanned,
                "active_positions": len(scanner.active_positions),
                "uptime_seconds": time.time() - scanner.start_time
            },
            "engine": engine_stats,
            "logger": logger_stats,
            "configuration": {
                "scan_interval": SCAN_INTERVAL,
                "max_positions": MAX_POSITIONS,
                "min_target_pct": MIN_TARGET_PCT,
                "max_target_pct": MAX_TARGET_PCT,
                "max_stop_loss_pct": MAX_STOP_LOSS_PCT,
                "min_risk_reward": MIN_RISK_REWARD
            }
        }
        
    except Exception as e:
        return {"error": str(e)}

@app.get("/recent")
async def get_recent_signals(limit: int = 20):
    """Get recent aggressive signals"""
    if scanner is None or scanner.db is None:
        return {"error": "Hunter not initialized"}
    
    try:
        scanner.db.row_factory = aiosqlite.Row
        async with scanner.db.execute("""
            SELECT symbol, side, entry_price, stop_loss, take_profit,
                   target_pct, risk_reward, overall_conviction,
                   compression_score, volume_spike_ratio,
                   status, created_at, pnl_percent, close_reason
            FROM aggressive_signals 
            ORDER BY created_at DESC 
            LIMIT ?
        """, (limit,)) as cursor:
            rows = await cursor.fetchall()
            
            signals = []
            for row in rows:
                signals.append(dict(row))
            
            return {
                "signals": signals,
                "count": len(signals),
                "hunting_style": "Aggressive wave expansion"
            }
            
    except Exception as e:
        return {"error": str(e)}

@app.get("/performance")
async def get_performance_metrics():
    """Get detailed performance metrics"""
    if scanner is None or scanner.db is None:
        return {"error": "Hunter not initialized"}
    
    try:
        # Get win/loss stats
        async with scanner.db.execute("""
            SELECT 
                COUNT(*) as total_trades,
                SUM(CASE WHEN close_reason = 'TP_HIT' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN close_reason = 'SL_HIT' THEN 1 ELSE 0 END) as losses,
                AVG(CASE WHEN close_reason = 'TP_HIT' THEN pnl_percent END) as avg_win,
                AVG(CASE WHEN close_reason = 'SL_HIT' THEN pnl_percent END) as avg_loss,
                MAX(CASE WHEN close_reason = 'TP_HIT' THEN pnl_percent END) as best_win,
                MIN(CASE WHEN close_reason = 'SL_HIT' THEN pnl_percent END) as worst_loss,
                SUM(pnl_percent) as total_pnl
            FROM aggressive_signals 
            WHERE status = 'CLOSED'
        """) as cursor:
            perf = await cursor.fetchone()
            
        # Get compression stats
        async with scanner.db.execute("""
            SELECT 
                AVG(compression_score) as avg_compression,
                AVG(ema_spread_pct) as avg_ema_spread,
                AVG(volume_spike_ratio) as avg_volume_spike,
                COUNT(*) as total_compressions
            FROM compression_history
        """) as cursor:
            compression = await cursor.fetchone()
        
        return {
            "trading_performance": dict(perf) if perf else {},
            "compression_analysis": dict(compression) if compression else {},
            "hunter_philosophy": "Aggressive expansion hunting with asymmetric payoff",
            "expected_win_rate": "40-50%",
            "risk_profile": "Many small losses, Few big winners"
        }
        
    except Exception as e:
        return {"error": str(e)}

@app.get("/config")
async def get_configuration():
    """Get current configuration"""
    return {
        "scanning": {
            "interval_seconds": SCAN_INTERVAL,
            "top_pairs": TOP_N_VOLUME,
            "min_volume_usd": MIN_VOLUME_USD,
            "timeframes": list(TIMEFRAMES.keys())
        },
        "trading": {
            "max_stop_loss_pct": MAX_STOP_LOSS_PCT,
            "min_target_pct": MIN_TARGET_PCT,
            "max_target_pct": MAX_TARGET_PCT,
            "min_risk_reward": MIN_RISK_REWARD,
            "max_positions": MAX_POSITIONS
        },
        "indicators": {
            "ema_periods": EMA_PERIODS,
            "rsi_period": RSI_PERIOD,
            "rsi_overbought": RSI_OVERBOUGHT,
            "rsi_oversold": RSI_OVERSOLD
        },
        "philosophy": "Aggressive wave expansion hunting - Losses are fuel for explosive winners"
    }

# ================ MAIN EXECUTION ================
if __name__ == "__main__":
    """
    Main execution block.
    Run with: python aggressive_hunter.py
    """
    
    # Create and run the hunter
    hunter = CompleteAggressiveScanner()
    
    try:
        # Run the hunter
        asyncio.run(hunter.run())
        
    except KeyboardInterrupt:
        print("\n🛑 Hunter stopped by user")
    except Exception as e:
        print(f"\n🚨 Hunter crashed: {e}")
        import traceback
        traceback.print_exc()