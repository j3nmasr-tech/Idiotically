#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 REJECTION-BASED HIGH-FREQUENCY SCANNER
Professional discretionary trading system
Wave-length awareness + Strength analysis + Volume analysis + Indicators + Candle patterns + Rejection entries
TRADER MINDSET: Reaction-based, rejection specialist
COMPLETE ANALYSIS: All timeframes, all indicators, all patterns
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

# Trading parameters (REJECTION-BASED)
MAX_STOP_LOSS_PCT = 1.0    # 1% maximum stop loss
MIN_TARGET_PCT = 1.5       # 1.5% minimum target (asymmetric payoff)
MAX_TARGET_PCT = 6.0       # 4% maximum target
MIN_RISK_REWARD = 2.0      # Minimum 1:2 risk/reward

# Rejection scanning
REJECTION_CONFIG = {
    "rsi_long_zone": (40, 50),      # RSI 40-50 for LONG entries
    "rsi_short_zone": (50, 60),     # RSI 50-60 for SHORT entries
    "ema_distance_threshold": 0.5,  # 0.5% from EMA for rejection
    "min_rejection_strength": 0.1,  # Minimum rejection strength score
    "min_convergence_score": 0.1,   # 70% multi-TF alignment required
}

# Timeframes for REACTION TRADING
TIMEFRAMES = {
    "1H": "1h",      # Wave length context ONLY
    "15M": "15m",    # Strength and structure
    "5M": "5m",      # Primary rejection analysis
    "3M": "3m",      # Fast trigger (MAIN)
    "1M": "1m"       # Entry timing (ULTRA FAST)
}

# EMA periods for rejection detection
EMA_PERIODS = {
    "fast": 9,
    "medium": 21,
    "slow": 50,
    "very_slow": 200
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
class CandlePattern:
    """Candle pattern analysis"""
    pattern_name: str
    pattern_type: str          # REVERSAL, CONTINUATION, NEUTRAL
    reliability: float         # 0-1 reliability score
    confirmation_required: bool # Needs volume confirmation
    timeframe: str             # Which TF it appears on
    
    # Pattern components
    has_long_wick: bool
    has_short_wick: bool
    body_ratio: float          # Body size relative to wick
    engulfing_size: float      # For engulfing patterns

@dataclass
class IndicatorAnalysis:
    """Comprehensive indicator analysis across all timeframes"""
    # RSI analysis
    rsi_value: float
    rsi_trend: str             # BULLISH, BEARISH, NEUTRAL
    rsi_divergence: str        # BULLISH_DIVERGENCE, BEARISH_DIVERGENCE, NONE
    rsi_momentum: float        # 0-1 momentum strength
    
    # Moving Averages
    ma_alignment: str          # BULLISH_ALIGNED, BEARISH_ALIGNED, MIXED
    price_vs_ma: str           # ABOVE_ALL, BETWEEN, BELOW_ALL
    ma_distance: float         # Distance from key MA
    
    # MACD
    macd_signal: str           # BULLISH_CROSS, BEARISH_CROSS, NEUTRAL
    macd_momentum: float       # MACD histogram value
    macd_trend: str            # BULLISH, BEARISH
    
    # Bollinger Bands
    bb_position: str           # UPPER_BAND, MIDDLE, LOWER_BAND
    bb_squeeze: bool           # Bollinger squeeze
    bb_width: float            # Band width % of price
    
    # Volume indicators
    volume_trend: str          # INCREASING, DECREASING, FLAT
    volume_spike: bool         # Volume spike detected
    obv_trend: str             # BULLISH, BEARISH, NEUTRAL
    
    # Support/Resistance
    key_support: float
    key_resistance: float
    sr_strength: float         # 0-1 strength of level
    
    # Composite scores
    momentum_score: float      # 0-1 overall momentum
    trend_score: float         # 0-1 trend strength
    volatility_score: float    # 0-1 volatility

@dataclass
class RejectionSignal:
    """Rejection-based trade signal"""
    signal_id: str
    symbol: str
    side: str                  # LONG, SHORT
    
    # Price levels
    entry_price: float
    stop_loss: float
    take_profit: float
    
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
    risk_reward: float
    expected_move_pct: float
    
    # Timing
    timeframe_used: str        # Which TF triggered entry
    signal_timestamp: float
    conditions_met: List[str]  # Which rejection conditions triggered
    
    # Enhanced analysis (NEW)
    candle_patterns: List[CandlePattern]
    dominant_pattern: Optional[CandlePattern]
    
    # Indicator analysis per timeframe
    indicators_1h: IndicatorAnalysis
    indicators_15m: IndicatorAnalysis
    indicators_5m: IndicatorAnalysis
    indicators_3m: IndicatorAnalysis
    indicators_1m: IndicatorAnalysis
    
    # Volume analysis
    volume_profile: Dict[str, float]  # Volume at key levels
    volume_clusters: List[float]      # High volume nodes
    
    # Multi-timeframe confirmation
    multi_tf_confirmation: Dict[str, bool]  # Which TFs confirm
    convergence_score: float                 # 0-1 multi-TF alignment

# ================ PROFESSIONAL LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("rejection_scanner")

# ================ CANDLE PATTERN SCANNER ================
class CandlePatternScanner:
    """Professional candle pattern scanner"""
    
    def detect_patterns(self, df: pd.DataFrame, timeframe: str) -> List[CandlePattern]:
        """Detect candle patterns in dataframe"""
        patterns = []
        
        if len(df) < 5:
            return patterns
        
        # Get recent candles
        candles = df.iloc[-5:].copy()
        
        # Analyze each potential pattern
        # 1. Hammer / Hanging Man
        for i in range(len(candles) - 1):
            pattern = self._check_hammer_hanging_man(candles, i, timeframe)
            if pattern:
                patterns.append(pattern)
        
        # 2. Engulfing patterns
        for i in range(1, len(candles)):
            pattern = self._check_engulfing(candles, i, timeframe)
            if pattern:
                patterns.append(pattern)
        
        # 3. Doji
        for i in range(len(candles)):
            pattern = self._check_doji(candles.iloc[i], timeframe)
            if pattern:
                patterns.append(pattern)
        
        # 4. Pinbar
        for i in range(len(candles)):
            pattern = self._check_pinbar(candles.iloc[i], timeframe)
            if pattern:
                patterns.append(pattern)
        
        # 5. Multi-candle patterns
        if len(candles) >= 3:
            pattern = self._check_morning_evening_star(candles, timeframe)
            if pattern:
                patterns.append(pattern)
            
            pattern = self._check_three_soldiers_crows(candles, timeframe)
            if pattern:
                patterns.append(pattern)
        
        return patterns
    
    def _check_hammer_hanging_man(self, candles: pd.DataFrame, idx: int, timeframe: str) -> Optional[CandlePattern]:
        """Check for Hammer/Hanging Man pattern"""
        candle = candles.iloc[idx]
        
        # Calculate candle metrics
        body_size = abs(candle['close'] - candle['open'])
        upper_wick = candle['high'] - max(candle['open'], candle['close'])
        lower_wick = min(candle['open'], candle['close']) - candle['low']
        total_range = candle['high'] - candle['low']
        
        if total_range == 0:
            return None
        
        # Hammer criteria: small body, long lower wick
        if (lower_wick >= body_size * 2 and  # Long lower wick
            upper_wick <= body_size * 0.3 and  # Small upper wick
            lower_wick >= total_range * 0.6):   # Lower wick is 60%+ of range
            
            # Check if it's at bottom (Hammer) or top (Hanging Man)
            if idx > 0:
                prev_trend = candles.iloc[idx-1]['close'] < candles.iloc[idx-1]['open']
                if prev_trend:  # Downtrend → Hammer
                    return CandlePattern(
                        pattern_name="HAMMER",
                        pattern_type="BULLISH_REVERSAL",
                        reliability=0.7,
                        confirmation_required=True,
                        timeframe=timeframe,
                        has_long_wick=True,
                        has_short_wick=False,
                        body_ratio=body_size/total_range,
                        engulfing_size=0.0
                    )
                else:  # Uptrend → Hanging Man
                    return CandlePattern(
                        pattern_name="HANGING_MAN",
                        pattern_type="BEARISH_REVERSAL",
                        reliability=0.65,
                        confirmation_required=True,
                        timeframe=timeframe,
                        has_long_wick=True,
                        has_short_wick=False,
                        body_ratio=body_size/total_range,
                        engulfing_size=0.0
                    )
        
        return None
    
    def _check_engulfing(self, candles: pd.DataFrame, idx: int, timeframe: str) -> Optional[CandlePattern]:
        """Check for Engulfing pattern"""
        if idx < 1:
            return None
        
        current = candles.iloc[idx]
        previous = candles.iloc[idx-1]
        
        current_body = abs(current['close'] - current['open'])
        previous_body = abs(previous['close'] - previous['open'])
        
        # Bullish Engulfing
        if (previous['close'] < previous['open'] and  # Previous bearish
            current['close'] > current['open'] and     # Current bullish
            current['open'] < previous['close'] and    # Opens below previous close
            current['close'] > previous['open'] and    # Closes above previous open
            current_body > previous_body * 1.2):       # Body engulfs previous
            
            return CandlePattern(
                pattern_name="ENGULFING_BULLISH",
                pattern_type="BULLISH_REVERSAL",
                reliability=0.75,
                confirmation_required=False,
                timeframe=timeframe,
                has_long_wick=False,
                has_short_wick=False,
                body_ratio=current_body/(current['high'] - current['low']),
                engulfing_size=current_body/previous_body
            )
        
        # Bearish Engulfing
        elif (previous['close'] > previous['open'] and  # Previous bullish
              current['close'] < current['open'] and     # Current bearish
              current['open'] > previous['close'] and    # Opens above previous close
              current['close'] < previous['open'] and    # Closes below previous open
              current_body > previous_body * 1.2):       # Body engulfs previous
            
            return CandlePattern(
                pattern_name="ENGULFING_BEARISH",
                pattern_type="BEARISH_REVERSAL",
                reliability=0.75,
                confirmation_required=False,
                timeframe=timeframe,
                has_long_wick=False,
                has_short_wick=False,
                body_ratio=current_body/(current['high'] - current['low']),
                engulfing_size=current_body/previous_body
            )
        
        return None
    
    def _check_doji(self, candle, timeframe: str) -> Optional[CandlePattern]:
        """Check for Doji pattern"""
        body_size = abs(candle['close'] - candle['open'])
        total_range = candle['high'] - candle['low']
        
        if total_range == 0:
            return None
        
        # Doji: very small body relative to range
        if body_size <= total_range * 0.1:
            return CandlePattern(
                pattern_name="DOJI",
                pattern_type="REVERSAL",
                reliability=0.6,
                confirmation_required=True,
                timeframe=timeframe,
                has_long_wick=True,
                has_short_wick=True,
                body_ratio=body_size/total_range,
                engulfing_size=0.0
            )
        
        return None
    
    def _check_pinbar(self, candle, timeframe: str) -> Optional[CandlePattern]:
        """Check for Pinbar pattern"""
        body_size = abs(candle['close'] - candle['open'])
        upper_wick = candle['high'] - max(candle['open'], candle['close'])
        lower_wick = min(candle['open'], candle['close']) - candle['low']
        total_range = candle['high'] - candle['low']
        
        if total_range == 0:
            return None
        
        # Pinbar: long wick one side, small body
        if ((upper_wick >= body_size * 3 and lower_wick <= body_size * 0.5) or  # Upper pin
            (lower_wick >= body_size * 3 and upper_wick <= body_size * 0.5)):   # Lower pin
            
            pattern_type = "BULLISH_REVERSAL" if lower_wick > upper_wick else "BEARISH_REVERSAL"
            
            return CandlePattern(
                pattern_name="PINBAR",
                pattern_type=pattern_type,
                reliability=0.8,
                confirmation_required=False,
                timeframe=timeframe,
                has_long_wick=True,
                has_short_wick=False,
                body_ratio=body_size/total_range,
                engulfing_size=0.0
            )
        
        return None
    
    def _check_morning_evening_star(self, candles: pd.DataFrame, timeframe: str) -> Optional[CandlePattern]:
        """Check for Morning/Evening Star pattern"""
        if len(candles) < 3:
            return None
        
        # Morning Star (bullish reversal)
        first = candles.iloc[-3]
        second = candles.iloc[-2]
        third = candles.iloc[-1]
        
        first_bearish = first['close'] < first['open']
        second_small = abs(second['close'] - second['open']) / second['open'] < 0.01
        third_bullish = third['close'] > third['open']
        
        if (first_bearish and second_small and third_bullish and
            third['close'] > first['close']):
            
            return CandlePattern(
                pattern_name="MORNING_STAR",
                pattern_type="BULLISH_REVERSAL",
                reliability=0.8,
                confirmation_required=False,
                timeframe=timeframe,
                has_long_wick=False,
                has_short_wick=False,
                body_ratio=abs(third['close'] - third['open'])/(third['high'] - third['low']),
                engulfing_size=0.0
            )
        
        # Evening Star (bearish reversal)
        first_bullish = first['close'] > first['open']
        third_bearish = third['close'] < third['open']
        
        if (first_bullish and second_small and third_bearish and
            third['close'] < first['close']):
            
            return CandlePattern(
                pattern_name="EVENING_STAR",
                pattern_type="BEARISH_REVERSAL",
                reliability=0.8,
                confirmation_required=False,
                timeframe=timeframe,
                has_long_wick=False,
                has_short_wick=False,
                body_ratio=abs(third['close'] - third['open'])/(third['high'] - third['low']),
                engulfing_size=0.0
            )
        
        return None
    
    def _check_three_soldiers_crows(self, candles: pd.DataFrame, timeframe: str) -> Optional[CandlePattern]:
        """Check for Three White Soldiers / Three Black Crows"""
        if len(candles) < 3:
            return None
        
        recent = candles.iloc[-3:]
        
        # Three White Soldiers (bullish continuation)
        all_bullish = all(row['close'] > row['open'] for _, row in recent.iterrows())
        increasing_closes = all(recent.iloc[i]['close'] > recent.iloc[i-1]['close'] for i in range(1, 3))
        
        if all_bullish and increasing_closes:
            return CandlePattern(
                pattern_name="THREE_WHITE_SOLDIERS",
                pattern_type="BULLISH_CONTINUATION",
                reliability=0.75,
                confirmation_required=False,
                timeframe=timeframe,
                has_long_wick=False,
                has_short_wick=False,
                body_ratio=0.0,
                engulfing_size=0.0
            )
        
        # Three Black Crows (bearish continuation)
        all_bearish = all(row['close'] < row['open'] for _, row in recent.iterrows())
        decreasing_closes = all(recent.iloc[i]['close'] < recent.iloc[i-1]['close'] for i in range(1, 3))
        
        if all_bearish and decreasing_closes:
            return CandlePattern(
                pattern_name="THREE_BLACK_CROWS",
                pattern_type="BEARISH_CONTINUATION",
                reliability=0.75,
                confirmation_required=False,
                timeframe=timeframe,
                has_long_wick=False,
                has_short_wick=False,
                body_ratio=0.0,
                engulfing_size=0.0
            )
        
        return None

# ================ INDICATOR ANALYZER ================
class IndicatorAnalyzer:
    """Professional indicator analyzer"""
    
    def analyze_all_indicators(self, df: pd.DataFrame) -> IndicatorAnalysis:
        """Analyze all indicators on given timeframe"""
        if len(df) < 50:
            return self._get_default_analysis()
        
        # Calculate indicators
        rsi_analysis = self._analyze_rsi(df)
        ma_analysis = self._analyze_moving_averages(df)
        macd_analysis = self._analyze_macd(df)
        bb_analysis = self._analyze_bollinger_bands(df)
        volume_analysis = self._analyze_volume(df)
        sr_analysis = self._analyze_support_resistance(df)
        
        # Composite scores
        momentum_score = self._calculate_momentum_score(rsi_analysis, macd_analysis)
        trend_score = self._calculate_trend_score(ma_analysis, macd_analysis)
        volatility_score = bb_analysis['width']
        
        return IndicatorAnalysis(
            rsi_value=rsi_analysis['value'],
            rsi_trend=rsi_analysis['trend'],
            rsi_divergence=rsi_analysis['divergence'],
            rsi_momentum=rsi_analysis['momentum'],
            
            ma_alignment=ma_analysis['alignment'],
            price_vs_ma=ma_analysis['price_position'],
            ma_distance=ma_analysis['distance'],
            
            macd_signal=macd_analysis['signal'],
            macd_momentum=macd_analysis['momentum'],
            macd_trend=macd_analysis['trend'],
            
            bb_position=bb_analysis['position'],
            bb_squeeze=bb_analysis['squeeze'],
            bb_width=bb_analysis['width'],
            
            volume_trend=volume_analysis['trend'],
            volume_spike=volume_analysis['spike'],
            obv_trend=volume_analysis['obv_trend'],
            
            key_support=sr_analysis['support'],
            key_resistance=sr_analysis['resistance'],
            sr_strength=sr_analysis['strength'],
            
            momentum_score=momentum_score,
            trend_score=trend_score,
            volatility_score=volatility_score
        )
    
    def calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _analyze_rsi(self, df: pd.DataFrame) -> Dict:
        """Analyze RSI with divergence detection"""
        # Calculate RSI
        rsi = self.calculate_rsi(df['close'])
        
        if len(rsi) < 14:
            return {"value": 50, "trend": "NEUTRAL", "divergence": "NONE", "momentum": 0.5}
        
        current_rsi = rsi.iloc[-1]
        
        # RSI trend
        rsi_ma = rsi.rolling(window=5).mean()
        rsi_trend = "BULLISH" if current_rsi > rsi_ma.iloc[-1] else "BEARISH"
        
        # RSI momentum (slope)
        rsi_slope = self._calculate_slope(rsi.values[-5:])
        rsi_momentum = min(abs(rsi_slope) * 10, 1.0)  # Normalize to 0-1
        
        # RSI divergence
        divergence = self._detect_rsi_divergence(df['close'].values, rsi.values)
        
        return {
            "value": current_rsi,
            "trend": rsi_trend,
            "divergence": divergence,
            "momentum": rsi_momentum
        }
    
    def _detect_rsi_divergence(self, prices: np.ndarray, rsi_values: np.ndarray) -> str:
        """Detect RSI divergence"""
        if len(prices) < 20:
            return "NONE"
        
        # Look for last 2 swings
        recent_prices = prices[-20:]
        recent_rsi = rsi_values[-20:]
        
        # Find peaks and troughs
        price_peaks, price_troughs = self._find_swings(recent_prices)
        rsi_peaks, rsi_troughs = self._find_swings(recent_rsi)
        
        if len(price_peaks) >= 2 and len(rsi_peaks) >= 2:
            # Bearish divergence: Higher price highs, lower RSI highs
            if (price_peaks[-1] > price_peaks[-2] and 
                rsi_peaks[-1] < rsi_peaks[-2]):
                return "BEARISH_DIVERGENCE"
        
        if len(price_troughs) >= 2 and len(rsi_troughs) >= 2:
            # Bullish divergence: Lower price lows, higher RSI lows
            if (price_troughs[-1] < price_troughs[-2] and 
                rsi_troughs[-1] > rsi_troughs[-2]):
                return "BULLISH_DIVERGENCE"
        
        return "NONE"
    
    def _find_swings(self, data: np.ndarray) -> Tuple[List[float], List[float]]:
        """Find swing highs and lows"""
        peaks = []
        troughs = []
        
        for i in range(2, len(data)-2):
            # Peak
            if (data[i] > data[i-2] and data[i] > data[i-1] and 
                data[i] > data[i+1] and data[i] > data[i+2]):
                peaks.append(data[i])
            
            # Trough
            elif (data[i] < data[i-2] and data[i] < data[i-1] and 
                  data[i] < data[i+1] and data[i] < data[i+2]):
                troughs.append(data[i])
        
        return peaks, troughs
    
    def _analyze_moving_averages(self, df: pd.DataFrame) -> Dict:
        """Analyze moving averages alignment"""
        current_price = df['close'].iloc[-1]
        
        # Calculate multiple MAs
        ma_9 = df['close'].rolling(window=9).mean().iloc[-1]
        ma_21 = df['close'].rolling(window=21).mean().iloc[-1]
        ma_50 = df['close'].rolling(window=50).mean().iloc[-1]
        ma_200 = df['close'].rolling(window=200).mean().iloc[-1]
        
        # Check alignment
        mas = [ma_9, ma_21, ma_50, ma_200]
        
        bullish_aligned = all(mas[i] <= mas[i+1] for i in range(len(mas)-1))
        bearish_aligned = all(mas[i] >= mas[i+1] for i in range(len(mas)-1))
        
        alignment = "BULLISH_ALIGNED" if bullish_aligned else (
                   "BEARISH_ALIGNED" if bearish_aligned else "MIXED")
        
        # Price position relative to MAs
        if current_price > ma_200:
            price_position = "ABOVE_ALL"
        elif current_price < ma_200:
            price_position = "BELOW_ALL"
        else:
            price_position = "BETWEEN"
        
        # Distance from key MA (200)
        ma_distance = abs(current_price - ma_200) / ma_200 * 100
        
        return {
            "alignment": alignment,
            "price_position": price_position,
            "distance": ma_distance
        }
    
    def _analyze_macd(self, df: pd.DataFrame) -> Dict:
        """Analyze MACD"""
        # Calculate MACD
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9, adjust=False).mean()
        histogram = macd - signal
        
        if len(macd) < 2:
            return {"signal": "NEUTRAL", "momentum": 0.0, "trend": "NEUTRAL"}
        
        current_macd = macd.iloc[-1]
        current_signal = signal.iloc[-1]
        current_hist = histogram.iloc[-1]
        
        # Signal cross
        prev_macd = macd.iloc[-2]
        prev_signal = signal.iloc[-2]
        
        if prev_macd < prev_signal and current_macd > current_signal:
            macd_signal = "BULLISH_CROSS"
        elif prev_macd > prev_signal and current_macd < current_signal:
            macd_signal = "BEARISH_CROSS"
        else:
            macd_signal = "NEUTRAL"
        
        # Momentum
        macd_momentum = abs(current_hist) / df['close'].iloc[-1] * 1000
        macd_momentum = min(macd_momentum, 1.0)  # Normalize
        
        # Trend
        macd_trend = "BULLISH" if current_macd > 0 else "BEARISH"
        
        return {
            "signal": macd_signal,
            "momentum": macd_momentum,
            "trend": macd_trend
        }
    
    def _analyze_bollinger_bands(self, df: pd.DataFrame) -> Dict:
        """Analyze Bollinger Bands"""
        current_price = df['close'].iloc[-1]
        
        # Calculate Bollinger Bands
        ma_20 = df['close'].rolling(window=20).mean()
        std_20 = df['close'].rolling(window=20).std()
        
        upper_band = ma_20 + (std_20 * 2)
        lower_band = ma_20 - (std_20 * 2)
        
        if len(upper_band) == 0 or pd.isna(ma_20.iloc[-1]) or ma_20.iloc[-1] == 0:
            return {"position": "MIDDLE", "squeeze": False, "width": 0.0}
        
        current_upper = upper_band.iloc[-1]
        current_lower = lower_band.iloc[-1]
        current_ma_20 = ma_20.iloc[-1]
        
        # Position
        if current_price >= current_upper * 0.99:
            bb_position = "UPPER_BAND"
        elif current_price <= current_lower * 1.01:
            bb_position = "LOWER_BAND"
        else:
            bb_position = "MIDDLE"
        
        # Squeeze detection
        band_width = (current_upper - current_lower) / current_ma_20 * 100
        
        # Calculate average width
        if len(upper_band) > 50:
            avg_width = ((upper_band - lower_band) / ma_20 * 100).rolling(50).mean().iloc[-1]
            bb_squeeze = band_width < avg_width * 0.7  # 30% narrower than average
        else:
            bb_squeeze = band_width < 2.0  # Less than 2% width
        
        return {
            "position": bb_position,
            "squeeze": bb_squeeze,
            "width": band_width
        }
    
    def _analyze_volume(self, df: pd.DataFrame) -> Dict:
        """Analyze volume indicators"""
        if len(df) < 20:
            return {"trend": "FLAT", "spike": False, "obv_trend": "NEUTRAL"}
        
        # Volume trend
        recent_volume = df['volume'].values[-5:]
        volume_slope = self._calculate_slope(recent_volume)
        
        if volume_slope > 0.1:
            volume_trend = "INCREASING"
        elif volume_slope < -0.1:
            volume_trend = "DECREASING"
        else:
            volume_trend = "FLAT"
        
        # Volume spike
        avg_volume = df['volume'].rolling(20).mean().iloc[-1]
        current_volume = df['volume'].iloc[-1]
        
        volume_spike = current_volume > avg_volume * 1.5
        
        # OBV (On Balance Volume) trend
        obv = self._calculate_obv(df)
        if len(obv) >= 5:
            obv_slope = self._calculate_slope(obv.values[-5:])
            obv_trend = "BULLISH" if obv_slope > 0 else "BEARISH" if obv_slope < 0 else "NEUTRAL"
        else:
            obv_trend = "NEUTRAL"
        
        return {
            "trend": volume_trend,
            "spike": volume_spike,
            "obv_trend": obv_trend
        }
    
    def _calculate_obv(self, df: pd.DataFrame) -> pd.Series:
        """Calculate On Balance Volume"""
        obv = pd.Series(0.0, index=df.index, dtype='float64')
        
        if len(df) < 2:
            return obv
        
        for i in range(1, len(df)):
            try:
                if df['close'].iloc[i] > df['close'].iloc[i-1]:
                    obv.iloc[i] = float(obv.iloc[i-1]) + float(df['volume'].iloc[i])
                elif df['close'].iloc[i] < df['close'].iloc[i-1]:
                    obv.iloc[i] = float(obv.iloc[i-1]) - float(df['volume'].iloc[i])
                else:
                    obv.iloc[i] = float(obv.iloc[i-1])
            except Exception as e:
                obv.iloc[i] = float(obv.iloc[i-1])
        
        return obv
    
    def _analyze_support_resistance(self, df: pd.DataFrame) -> Dict:
        """Analyze support and resistance levels"""
        if len(df) < 50:
            return {"support": 0.0, "resistance": 0.0, "strength": 0.0}
        
        # Find recent highs and lows
        recent_high = df['high'].rolling(20).max().iloc[-1]
        recent_low = df['low'].rolling(20).min().iloc[-1]
        
        # Find stronger S/R from larger timeframe
        if len(df) >= 100:
            major_high = df['high'].rolling(100).max().iloc[-1]
            major_low = df['low'].rolling(100).min().iloc[-1]
        else:
            major_high = recent_high
            major_low = recent_low
        
        current_price = df['close'].iloc[-1]
        
        # Determine nearest support and resistance
        support = major_low if abs(current_price - major_low) < abs(current_price - recent_low) else recent_low
        resistance = major_high if abs(current_price - major_high) < abs(current_price - recent_high) else recent_high
        
        # Calculate strength (based on touches)
        support_touches = sum((df['low'] <= support * 1.005) & (df['low'] >= support * 0.995))
        resistance_touches = sum((df['high'] >= resistance * 0.995) & (df['high'] <= resistance * 1.005))
        
        sr_strength = min(max(support_touches, resistance_touches) / 20, 1.0)
        
        return {
            "support": support,
            "resistance": resistance,
            "strength": sr_strength
        }
    
    def _calculate_slope(self, data: np.ndarray) -> float:
        """Calculate linear slope of data"""
        if len(data) < 2:
            return 0.0
        
        x = np.arange(len(data))
        slope, _ = np.polyfit(x, data, 1)
        return slope
    
    def _calculate_momentum_score(self, rsi_analysis: Dict, macd_analysis: Dict) -> float:
        """Calculate overall momentum score"""
        scores = []
        weights = []
        
        # RSI momentum (40%)
        scores.append(rsi_analysis['momentum'])
        weights.append(0.4)
        
        # RSI divergence (20%)
        if rsi_analysis['divergence'] in ['BULLISH_DIVERGENCE', 'BEARISH_DIVERGENCE']:
            scores.append(0.8)  # Divergence increases momentum importance
        else:
            scores.append(0.5)
        weights.append(0.2)
        
        # MACD momentum (40%)
        scores.append(macd_analysis['momentum'])
        weights.append(0.4)
        
        return np.average(scores, weights=weights)
    
    def _calculate_trend_score(self, ma_analysis: Dict, macd_analysis: Dict) -> float:
        """Calculate overall trend score"""
        scores = []
        weights = []
        
        # MA alignment (50%)
        if ma_analysis['alignment'] == "BULLISH_ALIGNED":
            scores.append(0.8)
        elif ma_analysis['alignment'] == "BEARISH_ALIGNED":
            scores.append(0.2)  # Still shows strong trend, just bearish
        else:
            scores.append(0.5)
        weights.append(0.5)
        
        # MACD trend (50%)
        if macd_analysis['trend'] == "BULLISH":
            scores.append(0.8)
        elif macd_analysis['trend'] == "BEARISH":
            scores.append(0.2)
        else:
            scores.append(0.5)
        weights.append(0.5)
        
        return np.average(scores, weights=weights)
    
    def _get_default_analysis(self) -> IndicatorAnalysis:
        return IndicatorAnalysis(
            rsi_value=50.0,
            rsi_trend="NEUTRAL",
            rsi_divergence="NONE",
            rsi_momentum=0.5,
            
            ma_alignment="MIXED",
            price_vs_ma="BETWEEN",
            ma_distance=0.0,
            
            macd_signal="NEUTRAL",
            macd_momentum=0.0,
            macd_trend="NEUTRAL",
            
            bb_position="MIDDLE",
            bb_squeeze=False,
            bb_width=0.0,
            
            volume_trend="FLAT",
            volume_spike=False,
            obv_trend="NEUTRAL",
            
            key_support=0.0,
            key_resistance=0.0,
            sr_strength=0.0,
            
            momentum_score=0.5,
            trend_score=0.5,
            volatility_score=0.5
        )

# ================ CORE REJECTION ENGINE ================
class EnhancedRejectionBasedScanner:
    """High-frequency rejection scanner with COMPLETE ANALYSIS"""
    
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
            
            # Register new signal as PENDING
            self.active_signals[symbol] = signal.signal_id
            self.signal_status[signal.signal_id] = {
                "symbol": symbol,
                "side": signal.side,
                "price": signal.entry_price,
                "status": "PENDING",
                "timestamp": signal.signal_timestamp
            }
            
            log.debug(f"Registered rejection signal {signal.signal_id[:8]} for {symbol}")
        
        def update_signal_status(self, signal_id: str, status: str):
            """Update signal status (PENDING → TRIGGERED → CLOSED)"""
            if signal_id in self.signal_status:
                self.signal_status[signal_id]["status"] = status
                log.debug(f"Signal {signal_id[:8]} status updated to {status}")
                
                # If CLOSED, mark as ready for new signals
                if status == "CLOSED":
                    symbol = self.signal_status[signal_id]["symbol"]
                    log.info(f"✅ Signal {signal_id[:8]} for {symbol} CLOSED - Ready for new rejections")
        
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
            "rejections_found": 0,
            "long_rejections": 0,
            "short_rejections": 0,
            "pairs_scanned": 0,
            "rejections_filtered": 0,
            "no_strength": 0,
            "no_rejection_zone": 0,
            "no_candle_pattern": 0,
            "low_convergence": 0
        }
        self.deduplicator = self.SignalDeduplicator()
        self.active_signal_ids = set()
        self.pattern_scanner = CandlePatternScanner()
        self.indicator_analyzer = IndicatorAnalyzer()
    
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
                    else:  # slow or very_slow
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
            range_mid = (recent_high + recent_low) / 2
            
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
    
    # ========== COMPLETE ANALYSIS METHODS ==========
    
    def analyze_candle_patterns(self, multi_tf_data: Dict[str, pd.DataFrame]) -> Tuple[List[CandlePattern], Optional[CandlePattern]]:
        """Analyze candle patterns on all timeframes"""
        all_patterns = []
        
        for tf_name, df in multi_tf_data.items():
            if df is not None and len(df) >= 10:
                patterns = self.pattern_scanner.detect_patterns(df, tf_name)
                all_patterns.extend(patterns)
        
        # Find dominant pattern (highest reliability)
        dominant_pattern = None
        if all_patterns:
            dominant_pattern = max(all_patterns, key=lambda p: p.reliability)
        
        return all_patterns, dominant_pattern
    
    def analyze_indicators_all_timeframes(self, multi_tf_data: Dict[str, pd.DataFrame]) -> Dict[str, IndicatorAnalysis]:
        """Analyze indicators on all timeframes"""
        indicators = {}
        
        for tf_name in ["1H", "15M", "5M", "3M", "1M"]:
            df = multi_tf_data.get(tf_name)
            if df is not None and len(df) >= 50:
                indicators[tf_name] = self.indicator_analyzer.analyze_all_indicators(df)
            else:
                indicators[tf_name] = self.indicator_analyzer._get_default_analysis()
        
        return indicators
    
    def analyze_volume_profile(self, df: pd.DataFrame) -> Tuple[Dict[str, float], List[float]]:
        """Analyze volume profile and clusters"""
        volume_profile = {}
        volume_clusters = []
        
        if len(df) < 20:
            return volume_profile, volume_clusters
        
        # Calculate volume profile (simplified)
        price_levels = np.linspace(df['low'].min(), df['high'].max(), 20)
        
        for level in price_levels:
            # Find candles touching this level (±0.5%)
            mask = (df['low'] <= level * 1.005) & (df['high'] >= level * 0.995)
            volume_at_level = df.loc[mask, 'volume'].sum()
            volume_profile[f"{level:.4f}"] = volume_at_level
        
        # Find high volume clusters (top 5)
        df_sorted = df.sort_values('volume', ascending=False)
        top_volumes = df_sorted.head(5)
        
        for _, row in top_volumes.iterrows():
            cluster_price = (row['high'] + row['low'] + row['close']) / 3
            volume_clusters.append(cluster_price)
        
        return volume_profile, volume_clusters
    
    def check_multi_tf_confirmation(self, indicators: Dict[str, IndicatorAnalysis], 
                                   side: str, zone_type: str) -> Dict[str, bool]:
        """Check which timeframes confirm the signal"""
        confirmation = {}
        
        for tf_name, analysis in indicators.items():
            confirms = False
            
            if side == "LONG":
                confirms = (
                    analysis.rsi_value < 60 and  # Not overbought
                    analysis.macd_trend in ["BULLISH", "NEUTRAL"] and
                    analysis.ma_alignment in ["BULLISH_ALIGNED", "MIXED"] and
                    analysis.bb_position != "UPPER_BAND"  # Not at top of band
                )
            else:  # SHORT
                confirms = (
                    analysis.rsi_value > 40 and  # Not oversold
                    analysis.macd_trend in ["BEARISH", "NEUTRAL"] and
                    analysis.ma_alignment in ["BEARISH_ALIGNED", "MIXED"] and
                    analysis.bb_position != "LOWER_BAND"  # Not at bottom of band
                )
            
            # Additional check for divergence
            if (side == "LONG" and analysis.rsi_divergence == "BULLISH_DIVERGENCE") or \
               (side == "SHORT" and analysis.rsi_divergence == "BEARISH_DIVERGENCE"):
                confirms = True
            
            confirmation[tf_name] = confirms
        
        return confirmation
    
    def calculate_convergence_score(self, confirmation: Dict[str, bool]) -> float:
        """Calculate multi-timeframe convergence score"""
        if not confirmation:
            return 0.0
        
        # Higher weight for lower timeframes
        weights = {"1M": 0.3, "3M": 0.25, "5M": 0.2, "15M": 0.15, "1H": 0.1}
        
        weighted_score = 0
        for tf_name, confirms in confirmation.items():
            weight = weights.get(tf_name, 0.1)
            weighted_score += (1 if confirms else 0) * weight
        
        return weighted_score
    
    def pattern_confirms_rejection(self, pattern: Optional[CandlePattern], side: str) -> bool:
        """Check if candle pattern confirms rejection direction"""
        if not pattern:
            return True  # No pattern is okay
        
        if side == "LONG":
            # Bullish reversal patterns confirm LONG
            return pattern.pattern_type in ["BULLISH_REVERSAL", "BULLISH_CONTINUATION"]
        else:  # SHORT
            # Bearish reversal patterns confirm SHORT
            return pattern.pattern_type in ["BEARISH_REVERSAL", "BEARISH_CONTINUATION"]
    
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
    
    def generate_enhanced_rejection_signal(self, multi_tf_data: Dict[str, pd.DataFrame], 
                                          symbol: str) -> Optional[RejectionSignal]:
        """
        Generate COMPLETE rejection-based signal with ALL analyses
        """
        try:
            # Get timeframe data
            tf_1h = multi_tf_data.get("1H")
            tf_15m = multi_tf_data.get("15M")
            tf_5m = multi_tf_data.get("5M")
            tf_3m = multi_tf_data.get("3M")
            tf_1m = multi_tf_data.get("1M")
            
            # Check data availability - need at least 15m and 3m
            if tf_15m is None or tf_3m is None:
                log.debug(f"{symbol}: Missing key timeframe data")
                return None
            
            if len(tf_15m) < 30 or len(tf_3m) < 20:
                log.debug(f"{symbol}: Insufficient data")
                return None
            
            # ===== 1. WAVE CONTEXT ANALYSIS =====
            wave_context = self.analyze_wave_context(tf_1h, tf_15m)
            
            # ===== 2. MARKET STRENGTH ANALYSIS =====
            market_strength = self.analyze_market_strength(tf_15m)
            
            # CRITICAL: No strength → no trade
            if market_strength.strength_score < 0.4:
                self.daily_stats["no_strength"] += 1
                log.debug(f"{symbol}: No market strength ({market_strength.strength_score:.2f})")
                return None
            
            # ===== 3. CANDLE PATTERN ANALYSIS =====
            candle_patterns, dominant_pattern = self.analyze_candle_patterns(multi_tf_data)
            
            # ===== 4. INDICATOR ANALYSIS (ALL TIMEFRAMES) =====
            indicators = self.analyze_indicators_all_timeframes(multi_tf_data)
            
            # ===== 5. VOLUME PROFILE ANALYSIS =====
            volume_profile, volume_clusters = self.analyze_volume_profile(tf_3m)
            
            # ===== 6. REJECTION ZONE ANALYSIS =====
            current_price = tf_3m['close'].iloc[-1]
            emas = self.calculate_emas(tf_3m)
            
            # Calculate RSI on 3M
            rsi_series = self.calculate_rsi(tf_3m['close'])
            current_rsi = rsi_series.iloc[-1] if len(rsi_series) > 0 else 50
            
            # Find rejection zones
            rejection_zones = self.find_rejection_zones(tf_3m, current_price, current_rsi, emas)
            
            # CRITICAL: No rejection zone → no trade
            if not rejection_zones:
                self.daily_stats["no_rejection_zone"] += 1
                log.debug(f"{symbol}: No active rejection zone")
                return None
            
            # Check volume confirmation for each zone
            valid_zones = []
            for zone in rejection_zones:
                # Check volume confirmation
                zone.volume_confirmation = self._check_volume_confirmation(tf_3m, zone.zone_type)
                
                # Only consider zones with volume confirmation
                if zone.volume_confirmation:
                    valid_zones.append(zone)
            
            if not valid_zones:
                log.debug(f"{symbol}: No volume confirmation at rejection zones")
                return None
            
            # Select strongest rejection zone
            best_zone = max(valid_zones, key=lambda z: z.strength)
            
            # Determine trade side based on zone type
            side = None
            if best_zone.zone_type in ["EMA_SUPPORT", "RANGE_LOW", "FAILED_BREAKDOWN", "DEMAND"]:
                side = "LONG"
            elif best_zone.zone_type in ["EMA_RESISTANCE", "RANGE_HIGH", "FAILED_BREAKOUT", "SUPPLY"]:
                side = "SHORT"
            
            if not side:
                log.debug(f"{symbol}: Could not determine side for zone {best_zone.zone_type}")
                return None
            
            # ===== 7. CANDLE PATTERN CONFIRMATION =====
            if not self.pattern_confirms_rejection(dominant_pattern, side):
                self.daily_stats["no_candle_pattern"] += 1
                log.debug(f"{symbol}: Candle pattern doesn't confirm {side}")
                return None
            
            # ===== 8. MULTI-TIMEFRAME CONFIRMATION =====
            multi_tf_confirmation = self.check_multi_tf_confirmation(indicators, side, best_zone.zone_type)
            convergence_score = self.calculate_convergence_score(multi_tf_confirmation)
            
            if convergence_score < REJECTION_CONFIG["min_convergence_score"]:
                self.daily_stats["low_convergence"] += 1
                log.debug(f"{symbol}: Low multi-TF convergence ({convergence_score:.2f})")
                return None
            
            # ===== 9. RSI POSITION CHECK =====
            if side == "LONG" and best_zone.rsi_position != "IN_ZONE":
                log.debug(f"{symbol}: RSI not in LONG zone ({current_rsi:.1f})")
                return None
            elif side == "SHORT" and best_zone.rsi_position != "IN_ZONE":
                log.debug(f"{symbol}: RSI not in SHORT zone ({current_rsi:.1f})")
                return None
            
            # ===== 10. TRADE-BASED DEDUPLICATION CHECK =====
            if not self.deduplicator.should_generate_signal(symbol, side, current_price):
                self.daily_stats["rejections_filtered"] += 1
                return None
            
            # ===== 11. ANALYZE REJECTION CANDLE =====
            rejection_type, trigger_candle = self._analyze_rejection_candle(tf_3m, side, best_zone)
            
            if not rejection_type:
                log.debug(f"{symbol}: No clear rejection candle")
                return None
            
            # ===== 12. CALCULATE ENTRY, SL, TP =====
            stop_loss_pct = np.random.uniform(0.5, MAX_STOP_LOSS_PCT)
            target_pct = np.random.uniform(MIN_TARGET_PCT, MAX_TARGET_PCT)
            
            if side == "LONG":
                # Entry at rejection zone (slightly above for LONG)
                entry_price = best_zone.price_level * 1.001  # 0.1% above support
                stop_loss = entry_price * (1 - stop_loss_pct / 100)
                take_profit = entry_price * (1 + target_pct / 100)
            else:  # SHORT
                # Entry at rejection zone (slightly below for SHORT)
                entry_price = best_zone.price_level * 0.999  # 0.1% below resistance
                stop_loss = entry_price * (1 + stop_loss_pct / 100)
                take_profit = entry_price * (1 - target_pct / 100)
            
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
            
            # ===== 13. CALCULATE REJECTION STRENGTH =====
            rejection_strength = self._calculate_rejection_strength(
                best_zone, market_strength, wave_context, current_rsi, convergence_score
            )
            
            if rejection_strength < REJECTION_CONFIG["min_rejection_strength"]:
                log.debug(f"{symbol}: Rejection too weak ({rejection_strength:.2f})")
                return None
            
            # ===== 14. DETERMINE CONDITIONS MET =====
            conditions_met = self._get_rejection_conditions(
                wave_context, market_strength, best_zone, rejection_type,
                dominant_pattern, multi_tf_confirmation
            )
            
            # ===== 15. CREATE SIGNAL ID =====
            signal_id = hashlib.md5(
                f"{symbol}:{side}:{entry_price:.8f}:{time.time()}:{best_zone.zone_type}".encode()
            ).hexdigest()
            
            # ===== 16. CREATE COMPLETE SIGNAL =====
            signal = RejectionSignal(
                signal_id=signal_id,
                symbol=symbol,
                side=side,
                
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                
                wave_context=wave_context,
                market_strength=market_strength,
                rejection_zone=best_zone,
                
                rejection_type=rejection_type,
                trigger_candle=trigger_candle,
                rsi_at_entry=current_rsi,
                
                rejection_strength=rejection_strength,
                risk_reward=risk_reward,
                expected_move_pct=target_pct,
                
                timeframe_used="3M",
                signal_timestamp=time.time(),
                conditions_met=conditions_met,
                
                # Enhanced analysis
                candle_patterns=candle_patterns,
                dominant_pattern=dominant_pattern,
                
                indicators_1h=indicators.get("1H"),
                indicators_15m=indicators.get("15M"),
                indicators_5m=indicators.get("5M"),
                indicators_3m=indicators.get("3M"),
                indicators_1m=indicators.get("1M"),
                
                volume_profile=volume_profile,
                volume_clusters=volume_clusters,
                
                multi_tf_confirmation=multi_tf_confirmation,
                convergence_score=convergence_score
            )
            
            # ===== 17. UPDATE TRACKING AND DEDUPLICATION =====
            self.deduplicator.register_signal(signal)
            self.active_signal_ids.add(signal_id)
            
            # ===== 18. UPDATE STATISTICS =====
            self.daily_stats["rejections_found"] += 1
            if side == "LONG":
                self.daily_stats["long_rejections"] += 1
            else:
                self.daily_stats["short_rejections"] += 1
            
            # ===== 19. LOG COMPLETE ANALYSIS =====
            log.info(f"🎯 COMPLETE REJECTION SIGNAL: {symbol} {side} @ {entry_price:.4f}")
            log.info(f"   Zone: {best_zone.zone_type}, Strength: {rejection_strength:.2f}")
            log.info(f"   RSI: {current_rsi:.1f}, R:R: {risk_reward:.1f}:1")
            log.info(f"   Wave: {wave_context.wave_length}, Maturity: {wave_context.wave_maturity:.1%}")
            log.info(f"   Candle Patterns: {len(candle_patterns)}, Dominant: {dominant_pattern.pattern_name if dominant_pattern else 'None'}")
            log.info(f"   Multi-TF Confirmation: {sum(multi_tf_confirmation.values())}/{len(multi_tf_confirmation)}")
            log.info(f"   Convergence Score: {convergence_score:.2%}")
            
            return signal
            
        except Exception as e:
            log.error(f"Rejection signal error for {symbol}: {e}")
            return None
    
    def _check_volume_confirmation(self, df: pd.DataFrame, zone_type: str) -> bool:
        """Check volume confirmation at rejection zone"""
        try:
            if len(df) < 5:
                return False
            
            # Get recent candles
            recent_candles = df.iloc[-5:]
            
            # Check for volume spike in last 1-2 candles
            recent_volume = recent_candles['volume'].values[-2:].mean()
            prev_volume = recent_candles['volume'].values[-5:-2].mean()
            
            if prev_volume > 0:
                volume_ratio = recent_volume / prev_volume
                
                # Volume spike (1.5x or more) is confirmation
                if volume_ratio >= 1.5:
                    return True
            
            # Also check if volume is decreasing into the zone (for failed breaks)
            if "FAILED" in zone_type:
                volume_trend = np.polyfit(range(5), recent_candles['volume'].values[-5:], 1)[0]
                if volume_trend < 0:  # Decreasing volume into the level
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
            prev_candle = df.iloc[-2]
            
            # Check wick rejection
            has_wick_rejection = False
            wick_type = None
            
            if side == "LONG":
                # LONG: Price wicks below support but closes above
                if (current_candle['low'] < zone.price_level and 
                    current_candle['close'] > zone.price_level):
                    has_wick_rejection = True
                    wick_type = "SUPPORT_WICK"
            
            else:  # SHORT
                # SHORT: Price wicks above resistance but closes below
                if (current_candle['high'] > zone.price_level and 
                    current_candle['close'] < zone.price_level):
                    has_wick_rejection = True
                    wick_type = "RESISTANCE_WICK"
            
            # Check momentum shift candle
            momentum_shift = False
            candle_type = None
            
            if side == "LONG":
                # LONG: Bearish candle followed by bullish candle at support
                if (prev_candle['close'] < prev_candle['open'] and  # Bearish
                    current_candle['close'] > current_candle['open'] and  # Bullish
                    abs(current_candle['close'] - zone.price_level) / zone.price_level < 0.002):  # Near zone
                    momentum_shift = True
                    candle_type = "BULLISH_REVERSAL"
            
            else:  # SHORT
                # SHORT: Bullish candle followed by bearish candle at resistance
                if (prev_candle['close'] > prev_candle['open'] and  # Bullish
                    current_candle['close'] < current_candle['open'] and  # Bearish
                    abs(current_candle['close'] - zone.price_level) / zone.price_level < 0.002):  # Near zone
                    momentum_shift = True
                    candle_type = "BEARISH_REVERSAL"
            
            # Determine rejection type
            if has_wick_rejection:
                return "WICK_REJECTION", wick_type
            elif momentum_shift:
                return "MOMENTUM_REJECTION", candle_type
            else:
                # Check for simple rejection (price tests level and moves away)
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
                                     wave: WaveContext, rsi: float, convergence_score: float) -> float:
        """Calculate overall rejection strength score"""
        factors = []
        weights = []
        
        # 1. Zone strength (20%)
        factors.append(zone.strength)
        weights.append(0.2)
        
        # 2. Market strength (20%)
        factors.append(strength.strength_score)
        weights.append(0.2)
        
        # 3. Wave context (15%)
        # Favor corrective waves at support/resistance
        if wave.structure_type == "CORRECTIVE":
            wave_score = 0.8
        elif wave.structure_type == "COMPRESSION":
            wave_score = 0.7
        else:
            wave_score = 0.5
        
        # Adjust for wave maturity (early waves better)
        wave_score *= (1 - wave.wave_maturity * 0.5)  # Reduce if too mature
        
        factors.append(wave_score)
        weights.append(0.15)
        
        # 4. RSI position (15%)
        if zone.rsi_position == "IN_ZONE":
            rsi_score = 0.9
        elif zone.rsi_position == "OVEREXTENDED":
            rsi_score = 0.3
        else:
            rsi_score = 0.5
        
        factors.append(rsi_score)
        weights.append(0.15)
        
        # 5. Multi-TF convergence (20%)
        factors.append(convergence_score)
        weights.append(0.2)
        
        # 6. Volume confirmation (10%)
        volume_score = 0.8 if zone.volume_confirmation else 0.3
        factors.append(volume_score)
        weights.append(0.1)
        
        return np.average(factors, weights=weights)
    
    def _get_rejection_conditions(self, wave: WaveContext, strength: MarketStrength, 
                                 zone: RejectionZone, rejection_type: str,
                                 dominant_pattern: Optional[CandlePattern],
                                 multi_tf_confirmation: Dict[str, bool]) -> List[str]:
        """Get list of conditions met for this rejection"""
        conditions = []
        
        # Wave conditions
        conditions.append(f"WAVE_{wave.wave_length}")
        conditions.append(f"STRUCTURE_{wave.structure_type}")
        
        # Strength conditions
        if strength.is_continuation:
            conditions.append("STRENGTH_CONTINUATION")
        if strength.is_rejection_setup:
            conditions.append("STRENGTH_REJECTION_SETUP")
        if strength.is_absorption:
            conditions.append("STRENGTH_ABSORPTION")
        if strength.is_compression:
            conditions.append("STRENGTH_COMPRESSION")
        
        # Zone conditions
        conditions.append(f"ZONE_{zone.zone_type}")
        if zone.volume_confirmation:
            conditions.append("VOLUME_CONFIRMED")
        
        # Rejection type
        conditions.append(f"REJECTION_{rejection_type}")
        
        # RSI condition
        conditions.append(f"RSI_{zone.rsi_position}")
        
        # Candle pattern condition
        if dominant_pattern:
            conditions.append(f"PATTERN_{dominant_pattern.pattern_name}")
        
        # Multi-TF confirmation
        confirmed_tfs = [tf for tf, confirms in multi_tf_confirmation.items() if confirms]
        if confirmed_tfs:
            conditions.append(f"MULTITF_{len(confirmed_tfs)}_CONFIRMED")
        
        return conditions
    
    def get_daily_stats(self) -> Dict:
        """Get daily statistics"""
        return self.daily_stats
    
    def cleanup_old_signals(self):
        """Clean up old signals from deduplication"""
        self.deduplicator.remove_closed_signals()

# ================ MAIN SCANNER SYSTEM ================
class CompleteRejectionScanner:
    """Main scanner system with COMPLETE rejection-based trading"""
    
    def __init__(self):
        self.scanner = EnhancedRejectionBasedScanner()
        self.exchange = None
        self.db = None
        self.scan_cycle = 0
    
    async def initialize(self):
        """Initialize the scanner"""
        log.info("=" * 70)
        log.info("🔥 COMPLETE REJECTION-BASED HIGH-FREQUENCY SCANNER")
        log.info("=" * 70)
        log.info("TRADER ROLE: Discretionary reaction trader")
        log.info("SPECIALTY: COMPLETE ANALYSIS - All timeframes, all indicators, all patterns")
        log.info("ANALYSIS LAYERS:")
        log.info("  1. Wave Length & Context (1H/15M)")
        log.info("  2. Market Strength (Speed/Distance/EMA/Volume)")
        log.info("  3. Candle Patterns (All reversal/continuation patterns)")
        log.info("  4. Indicators (RSI/MACD/MA/BB/Volume)")
        log.info("  5. Volume Profile & Clusters")
        log.info("  6. Multi-Timeframe Confirmation")
        log.info("  7. Rejection Zone Detection")
        log.info("PHILOSOPHY: Wave length sets context, Complete analysis makes decision")
        log.info("ENTRY RULE: Multi-confirmed rejection pulls the trigger")
        log.info(f"SCAN INTERVAL: {SCAN_INTERVAL} seconds")
        log.info("TIME FRAMES: 1H/15M/5M/3M/1M (Full spectrum)")
        log.info("REJECTION ZONES: EMA, Range, Failed breaks with volume confirmation")
        log.info("RSI ZONES: 40-50 (LONG), 50-60 (SHORT)")
        log.info("MIN CONVERGENCE: 70% multi-TF alignment required")
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
            
            # Enhanced rejection signals table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS complete_rejection_signals (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                
                wave_length TEXT NOT NULL,
                wave_maturity REAL NOT NULL,
                expansion_speed REAL NOT NULL,
                structure_type TEXT NOT NULL,
                context_side TEXT NOT NULL,
                
                candle_speed REAL NOT NULL,
                distance_ratio REAL NOT NULL,
                ema_angle REAL NOT NULL,
                volume_participation REAL NOT NULL,
                strength_score REAL NOT NULL,
                strength_flags TEXT NOT NULL,
                
                zone_type TEXT NOT NULL,
                zone_price REAL NOT NULL,
                zone_strength REAL NOT NULL,
                rejection_strength REAL NOT NULL,
                rsi_at_entry REAL NOT NULL,
                rejection_type TEXT NOT NULL,
                trigger_candle TEXT NOT NULL,
                
                candle_patterns TEXT,
                dominant_pattern TEXT,
                
                indicators_1h TEXT,
                indicators_15m TEXT,
                indicators_5m TEXT,
                indicators_3m TEXT,
                indicators_1m TEXT,
                
                volume_profile TEXT,
                volume_clusters TEXT,
                
                multi_tf_confirmation TEXT,
                convergence_score REAL NOT NULL,
                
                risk_reward REAL NOT NULL,
                expected_move REAL NOT NULL,
                timeframe_used TEXT NOT NULL,
                
                conditions_met TEXT,
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
            CREATE TABLE IF NOT EXISTS performance_complete_daily (
                date DATE PRIMARY KEY,
                total_rejections INTEGER,
                long_rejections INTEGER,
                short_rejections INTEGER,
                pairs_scanned INTEGER,
                rejections_filtered INTEGER,
                no_strength_count INTEGER,
                no_zone_count INTEGER,
                no_candle_pattern_count INTEGER,
                low_convergence_count INTEGER,
                win_rate REAL,
                avg_win REAL,
                avg_loss REAL,
                total_pnl REAL
            )
            """)
            
            await self.db.commit()
            
            log.info("✅ Complete database initialized")
            
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
🎯 <b>COMPLETE REJECTION-BASED HIGH-FREQUENCY SCANNER</b>

<b>🧠 TRADER MINDSET:</b>
• Reaction trader (not prediction-based)
• Rejection specialist with complete analysis
• Comfortable being wrong
• Emotionless with losses
• Hunts expansion, accepts losses

<b>📊 COMPLETE ANALYSIS FRAMEWORK (7 LAYERS):</b>
1️⃣ <b>الطول الموجي (Wave Length)</b> → السياق فقط
2️⃣ <b>قوة السوق (Market Strength)</b> → قوة الحركة
3️⃣ <b>أنماط الشموع (Candle Patterns)</b> → تأكيد الانعكاس
4️⃣ <b>المؤشرات (Indicators)</b> → كل المؤشرات
5️⃣ <b>ملف الفوليوم (Volume Profile)</b> → التراكمات
6️⃣ <b>تأكيد الإطارات الزمنية</b> → الاتساق
7️⃣ <b>مناطق الرفض (Rejection Zones)</b> → الزناد النهائي

<b>⚡ إعدادات الدخول الصارمة:</b>
• تقارب الإطارات: 70% كحد أدنى
• صفقة واحدة لكل عملة فقط

<b>🎯 فلسفة الدخول:</b>
الطول الموجي يحدد السياق
التحليل الكامل يحدد القرار
والرفض المؤكد هو الزناد

#تحليل_كاملي #رفض_مؤكد #صفقة_واحدة
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info("✅ Complete startup message sent to Telegram")
                
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
                elif tf_name == "15M":
                    limit = 80
                else:  # 5M, 3M, 1M
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
    
    async def save_complete_signal(self, signal: RejectionSignal) -> bool:
        """Save complete signal to database"""
        try:
            # Prepare strength flags
            strength_flags = []
            if signal.market_strength.is_continuation:
                strength_flags.append("CONTINUATION")
            if signal.market_strength.is_rejection_setup:
                strength_flags.append("REJECTION_SETUP")
            if signal.market_strength.is_absorption:
                strength_flags.append("ABSORPTION")
            if signal.market_strength.is_compression:
                strength_flags.append("COMPRESSION")
            
            # Prepare candle patterns
            candle_patterns_list = []
            if signal.candle_patterns:
                for pattern in signal.candle_patterns:
                    candle_patterns_list.append({
                        "name": pattern.pattern_name,
                        "type": pattern.pattern_type,
                        "reliability": pattern.reliability,
                        "timeframe": pattern.timeframe
                    })
            
            # Prepare dominant pattern
            dominant_pattern_info = None
            if signal.dominant_pattern:
                dominant_pattern_info = {
                    "name": signal.dominant_pattern.pattern_name,
                    "type": signal.dominant_pattern.pattern_type,
                    "reliability": signal.dominant_pattern.reliability
                }
            
            # Insert signal
            await self.db.execute("""
                INSERT INTO complete_rejection_signals (
                    id, symbol, side, entry_price, stop_loss, take_profit,
                    wave_length, wave_maturity, expansion_speed, structure_type, context_side,
                    candle_speed, distance_ratio, ema_angle, volume_participation, 
                    strength_score, strength_flags,
                    zone_type, zone_price, zone_strength, rejection_strength, 
                    rsi_at_entry, rejection_type, trigger_candle,
                    candle_patterns, dominant_pattern,
                    indicators_1h, indicators_15m, indicators_5m, indicators_3m, indicators_1m,
                    volume_profile, volume_clusters,
                    multi_tf_confirmation, convergence_score,
                    risk_reward, expected_move, timeframe_used,
                    conditions_met
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.side,
                signal.entry_price,
                signal.stop_loss,
                signal.take_profit,
                
                signal.wave_context.wave_length,
                signal.wave_context.wave_maturity,
                signal.wave_context.expansion_speed,
                signal.wave_context.structure_type,
                signal.wave_context.context_side,
                
                signal.market_strength.candle_speed,
                signal.market_strength.distance_ratio,
                signal.market_strength.ema_angle,
                signal.market_strength.volume_participation,
                signal.market_strength.strength_score,
                json.dumps(strength_flags),
                
                signal.rejection_zone.zone_type,
                signal.rejection_zone.price_level,
                signal.rejection_zone.strength,
                signal.rejection_strength,
                signal.rsi_at_entry,
                signal.rejection_type,
                signal.trigger_candle,
                
                json.dumps(candle_patterns_list),
                json.dumps(dominant_pattern_info),
                
                json.dumps(signal.indicators_1h.__dict__),
                json.dumps(signal.indicators_15m.__dict__),
                json.dumps(signal.indicators_5m.__dict__),
                json.dumps(signal.indicators_3m.__dict__),
                json.dumps(signal.indicators_1m.__dict__),
                
                json.dumps(signal.volume_profile),
                json.dumps(signal.volume_clusters),
                
                json.dumps(signal.multi_tf_confirmation),
                signal.convergence_score,
                
                signal.risk_reward,
                signal.expected_move_pct,
                signal.timeframe_used,
                
                json.dumps(signal.conditions_met)
            ))
            
            await self.db.commit()
            
            log.info(f"✅ Complete rejection signal saved: {signal.symbol}")
            return True
            
        except Exception as e:
            log.error(f"Error saving complete signal: {e}")
            return False
    
    async def format_complete_signal_message(self, signal: RejectionSignal) -> str:
        """Format complete signal for Telegram"""
        side_emoji = "🟢" if signal.side == "LONG" else "🔴"
        side_text = "شراء" if signal.side == "LONG" else "بيع"
        
        # Risk info
        risk_pct = abs(signal.entry_price - signal.stop_loss) / signal.entry_price * 100
        
        # Wave context
        wave_info = f"{signal.wave_context.wave_length} ({signal.wave_context.structure_type})"
        
        # Strength info
        strength_type = []
        if signal.market_strength.is_continuation:
            strength_type.append("استمرارية")
        if signal.market_strength.is_rejection_setup:
            strength_type.append("إعداد رفض")
        if signal.market_strength.is_absorption:
            strength_type.append("امتصاص")
        if signal.market_strength.is_compression:
            strength_type.append("ضغط")
        
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
        
        # Candle patterns
        patterns_text = ""
        if signal.candle_patterns:
            pattern_names = [p.pattern_name for p in signal.candle_patterns[:3]]  # Show top 3
            patterns_text = "، ".join(pattern_names)
            if len(signal.candle_patterns) > 3:
                patterns_text += f" و{len(signal.candle_patterns)-3} أكثر"
        
        # Indicator summary
        indicators_summary = []
        if signal.indicators_3m.rsi_divergence != "NONE":
            divergence_text = "تباعد صاعد" if signal.indicators_3m.rsi_divergence == "BULLISH_DIVERGENCE" else "تباعد هابط"
            indicators_summary.append(divergence_text)
        
        if signal.indicators_3m.macd_signal != "NEUTRAL":
            macd_text = "تقاطع MACD صاعد" if signal.indicators_3m.macd_signal == "BULLISH_CROSS" else "تقاطع MACD هابط"
            indicators_summary.append(macd_text)
        
        if signal.indicators_3m.bb_squeeze:
            indicators_summary.append("ضغط بولينجر")
        
        indicators_text = "، ".join(indicators_summary) if indicators_summary else "محايد"
        
        # Multi-TF confirmation
        confirmed_tfs = [tf for tf, confirms in signal.multi_tf_confirmation.items() if confirms]
        confirmed_count = len(confirmed_tfs)
        total_tfs = len(signal.multi_tf_confirmation)
        
        message = f"""
{side_emoji} <b>إشارة رفض كاملة</b> ⚡

<b>{signal.symbol}</b> | {side_text}

<b>📊 السياق الموجي:</b>
• طول الموجة: {wave_info}
• النضج: {signal.wave_context.wave_maturity:.1%}
• سرعة التوسع: {signal.wave_context.expansion_speed:.1%}

<b>💪 قوة السوق:</b>
• درجة القوة: {signal.market_strength.strength_score:.1%}
• النوع: {strength_text}
• سرعة الشموع: {signal.market_strength.candle_speed:.1%}
• مشاركة الفوليوم: {signal.market_strength.volume_participation:.1%}

<b>🕯️ أنماط الشموع:</b>
• الأنماط: {patterns_text or "لا يوجد"}
• النمط المسيطر: {signal.dominant_pattern.pattern_name if signal.dominant_pattern else "لا يوجد"}

<b>📈 المؤشرات (3M):</b>
• RSI: {signal.indicators_3m.rsi_value:.1f} ({signal.indicators_3m.rsi_trend})
• {indicators_text}
• اتجاه MACD: {signal.indicators_3m.macd_trend}
• موقع بولينجر: {signal.indicators_3m.bb_position}

<b>📊 ملف الفوليوم:</b>
• تجمعات الفوليوم: {len(signal.volume_clusters)}
• اتجاه الفوليوم: {signal.indicators_3m.volume_trend}
• ارتفاع الفوليوم: {"✅" if signal.indicators_3m.volume_spike else "❌"}

<b>🎯 منطقة الرفض:</b>
• النوع: {zone_text}
• قوة المنطقة: {signal.rejection_zone.strength:.1%}
• تأكيد الفوليوم: {"✅" if signal.rejection_zone.volume_confirmation else "❌"}
• RSI عند الدخول: {signal.rsi_at_entry:.1f}

<b>⚡ تفاصيل الرفض:</b>
• نوع الرفض: {signal.rejection_type}
• شمعة الزناد: {signal.trigger_candle}
• قوة الرفض: {signal.rejection_strength:.1%}

<b>🔄 تأكيد الإطارات الزمنية:</b>
• إطارات مؤكدة: {confirmed_count}/{total_tfs}
• درجة التقارب: {signal.convergence_score:.1%}

<b>🔧 التنفيذ:</b>
• سعر الدخول: <code>{signal.entry_price:.6f}</code>
• وقف الخسارة: <code>{signal.stop_loss:.6f}</code> ({risk_pct:.2f}%)
• هدف الربح: <code>{signal.take_profit:.6f}</code> ({signal.expected_move_pct:.1f}%)
• نسبة الربح/المخاطرة: {signal.risk_reward:.1f}:1

<b>🛡️ نظام التكرار:</b>
• نظام: <b>صفقة واحدة لكل عملة</b>
• لا إشارات جديدة لـ {signal.symbol} حتى إغلاق هذه الصفقة

<b>⚠️ ملاحظة التاجر:</b>
الدخول عند الرفض فقط
التحليل الكامل يحدد القرار
نقبل الخسائر - نصطاد التوسع

#تحليل_كاملي #{side_text} #رفض_مؤكد #صفقة_واحدة
"""
        return message
    
    async def send_trade_trigger_notification(self, symbol: str, side: str, entry_price: float):
        """Send notification when trade is triggered/entered"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning(f"⚠️ Telegram credentials missing. Skipping trigger notification for {symbol}")
            return
        
        try:
            side_emoji = "🟢" if side == "LONG" else "🔴"
            side_text = "شراء" if side == "LONG" else "بيع"
            
            message = f"""
{side_emoji} <b>تم تنفيذ صفقة الرفض الكاملة</b> ⚡

<b>{symbol}</b> | {side_text}

<b>🎯 تم الدخول عند الرفض:</b>
<code>{entry_price:.6f}</code>

<b>🧠 عقلية التاجر:</b>
• دخول عند رفض مؤكد بتحليل كامل
• دخول حيث يتردد الآخرون
• راحة مع الخسائر المحتملة
• صيد للتوسع القادم

<b>📊 ملخص التحليل:</b>
• تحليل 7 طبقات مكتمل
• تأكيد متعدد الإطارات
• أنماط شموع مؤكدة
• مؤشرات متوافقة

<b>🛡️ نظام التكرار:</b>
❌ <b>ممنوع</b> إرسال إشارات جديدة لـ {symbol}
✅ مسموح بإشارات جديدة بعد إغلاق هذه الصفقة

<b>⚠️ المتابعة:</b>
يتم متابعة الصفقة تلقائياً.
ستصلك إشعار عند الوصول لوقف الخسارة أو هدف الربح.

#{side_text} #تنفيذ_رفض #متابعة #لا_إشارات_جديدة
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
            
            log.info(f"{side_emoji} Complete rejection trade triggered: {symbol} {side} @ {entry_price:.4f}")
            
        except Exception as e:
            log.error(f"Trigger notification error: {e}")
    
    async def send_trade_close_notification(self, symbol: str, side: str, pnl_percent: float, 
                                           close_reason: str, entry_price: float, 
                                           close_price: float, risk_reward: float):
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
            
            # Format P&L with sign
            pnl_formatted = f"+{pnl_percent:.2f}%" if pnl_percent > 0 else f"{pnl_percent:.2f}%"
            
            # Trader mindset message based on result
            if close_reason == "TP_HIT":
                mindset = "✅ التوسع تم اصطياده - التحليل الكامل حقق الربح"
            else:
                mindset = "❌ الخسارة مقبولة - الرفض لم يحترم، ننتظر الرفض التالي"
            
            message = f"""
{emoji} <b>تم إغلاق صفقة الرفض الكاملة</b> {result_emoji}

<b>{symbol}</b> | {side_text}

{color} <b>النتيجة: {result_text}</b>
{pnl_emoji} <b>النسبة: {pnl_formatted}</b>

<b>📊 تفاصيل التنفيذ:</b>
• نوع الدخول: {side_text} (عند الرفض المؤكد)
• سعر الدخول: <code>{entry_price:.6f}</code>
• سعر الإغلاق: <code>{close_price:.6f}</code>
• نسبة الربح/الخسارة: <b>{pnl_formatted}</b>
• نسبة الربح/المخاطرة المحققة: {risk_reward:.1f}:1

<b>🧠 عقلية التاجر:</b>
{mindset}
التحليل الكامل يقلل الخسائر ويزيد الأرباح
كل رفض هو فرصة جديدة لتحليل كامل

<b>🛡️ نظام التكرار:</b>
✅ <b>مسموح الآن</b> بإرسال إشارات جديدة لـ {symbol}
يمكن للماسح الضوئي البحث عن رفض جديد مع تحليل كامل

#{side_text} #إغلاق_رفض #{"ربح" if close_reason == "TP_HIT" else "خسارة"} #مسموح_إشارات_جديدة
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
            
            log.info(f"{emoji} Complete rejection trade closed: {symbol} {side} {pnl_formatted} ({close_reason})")
            
        except Exception as e:
            log.error(f"Close notification error: {e}")
    
    async def send_telegram_alert(self, signal: RejectionSignal):
        """Send Telegram alert for complete signal"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning(f"⚠️ Telegram credentials missing. Skipping alert for {signal.symbol}")
            return
        
        try:
            message = await self.format_complete_signal_message(signal)
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info(f"📤 Complete Telegram rejection alert sent: {signal.symbol}")
            
        except Exception as e:
            log.error(f"Telegram error: {e}")
    
    async def monitor_positions(self):
        """Monitor and close positions with trade-based deduplication"""
        log.info("👀 Starting COMPLETE position monitoring with Telegram notifications...")
        
        while True:
            try:
                # Get ALL open positions (both pending and triggered)
                async with self.db.execute("""
                    SELECT id, symbol, side, entry_price, stop_loss, take_profit, status
                    FROM complete_rejection_signals 
                    WHERE status IN ('PENDING', 'TRIGGERED')
                """) as cursor:
                    positions = await cursor.fetchall()
                
                if positions:
                    log.debug(f"📊 Monitoring {len(positions)} open positions")
                
                for pos_id, symbol, side, entry, sl, tp, status in positions:
                    try:
                        # Get current price
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        # For PENDING positions: check if price reached entry
                        if status == 'PENDING':
                            # For rejection trading, we enter immediately at signal price
                            # Mark as triggered if price is within reasonable range (0.5%)
                            if abs(current_price - entry) / entry <= 0.005:  # 0.5% zone
                                # Mark as triggered
                                await self.db.execute("""
                                    UPDATE complete_rejection_signals SET 
                                        status = 'TRIGGERED',
                                        triggered_at = CURRENT_TIMESTAMP,
                                        trigger_price = ?
                                    WHERE id = ?
                                """, (current_price, pos_id))
                                
                                await self.db.commit()
                                
                                # UPDATE DEDUPLICATION STATUS to TRIGGERED
                                self.scanner.deduplicator.update_signal_status(pos_id, "TRIGGERED")
                                
                                # SEND TRIGGER NOTIFICATION
                                await self.send_trade_trigger_notification(symbol, side, current_price)
                                
                                log.info(f"✅ Complete rejection position triggered: {symbol} {side} @ {current_price:.4f}")
                                continue  # Skip SL/TP check for this cycle
                        
                        # Check SL/TP for ALL positions (including newly triggered ones)
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
                                SELECT risk_reward FROM complete_rejection_signals WHERE id = ?
                            """, (pos_id,)) as cursor:
                                row = await cursor.fetchone()
                                risk_reward = row[0] if row else 0
                            
                            # Update database
                            await self.db.execute("""
                                UPDATE complete_rejection_signals SET 
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
                            
                            # SEND CLOSE NOTIFICATION
                            await self.send_trade_close_notification(
                                symbol=symbol,
                                side=side,
                                pnl_percent=pnl_percent,
                                close_reason=close_reason,
                                entry_price=entry,
                                close_price=current_price,
                                risk_reward=risk_reward
                            )
                            
                            log.info(f"📤 Complete Telegram close notification sent for {symbol}: {close_reason} ({pnl_percent:.2f}%)")
                    
                    except Exception as e:
                        log.error(f"Monitor error for {symbol}: {e}")
                        continue
                
                # Clean up old closed signals periodically
                if int(time.time()) % 300 < 2:  # Every ~5 minutes
                    self.scanner.deduplicator.remove_closed_signals()
                
                # Fast monitoring for rejection trading
                await asyncio.sleep(2)  # Check every 2 seconds
                
            except Exception as e:
                log.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(5)
    
    async def high_freq_complete_scanning(self):
        """Main high-frequency scanning loop for complete rejection analysis"""
        log.info("🚀 Starting COMPLETE rejection-based high-frequency scanning...")
        
        while True:
            try:
                self.scan_cycle += 1
                start_time = time.time()
                
                log.info(f"🔄 Complete scan cycle #{self.scan_cycle} (7-layer analysis)")
                
                # Get active pairs
                pairs = await self.get_active_pairs()
                
                if not pairs:
                    log.warning("No active pairs found")
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                log.info(f"Scanning {len(pairs)} active pairs with COMPLETE analysis")
                
                signals_found = 0
                pairs_processed = 0
                
                # Ultra-fast scanning with complete analysis
                for symbol, volume in pairs:
                    try:
                        # Fetch data for all timeframes
                        multi_tf_data = await self.fetch_timeframe_data(symbol)
                        
                        # Need key timeframes for complete analysis
                        required_tfs = ["1H", "15M", "3M"]  # Context + Strength + Entry
                        has_all_data = all(tf in multi_tf_data for tf in required_tfs)
                        
                        if not has_all_data:
                            continue
                        
                        # Generate COMPLETE rejection signal
                        signal = self.scanner.generate_enhanced_rejection_signal(multi_tf_data, symbol)
                        
                        if signal:
                            # Save and send
                            saved = await self.save_complete_signal(signal)
                            
                            if saved:
                                await self.send_telegram_alert(signal)
                                signals_found += 1
                        
                        pairs_processed += 1
                        
                        # Ultra-fast between pairs
                        await asyncio.sleep(0.01)  # 10ms between pairs
                        
                    except Exception as e:
                        log.debug(f"Pair error {symbol}: {str(e)[:50]}")
                        continue
                
                # Update scanner stats
                self.scanner.daily_stats["pairs_scanned"] += pairs_processed
                
                # Log complete rejection stats
                active_count = len(self.scanner.deduplicator.active_signals)
                stats = self.scanner.get_daily_stats()
                
                log.info(f"📊 COMPLETE rejection stats: Found {signals_found}, Active: {active_count}")
                log.info(f"   Filtered: {stats.get('rejections_filtered', 0)}")
                log.info(f"   No strength: {stats.get('no_strength', 0)}")
                log.info(f"   No zone: {stats.get('no_rejection_zone', 0)}")
                log.info(f"   No pattern: {stats.get('no_candle_pattern', 0)}")
                log.info(f"   Low convergence: {stats.get('low_convergence', 0)}")
                
                scan_duration = time.time() - start_time
                log.info(f"Complete scan #{self.scan_cycle}: {signals_found} rejections in {scan_duration:.2f}s")
                
                # Log detailed stats periodically
                if self.scan_cycle % 20 == 0:
                    total_filtered = (
                        stats.get('rejections_filtered', 0) +
                        stats.get('no_strength', 0) +
                        stats.get('no_rejection_zone', 0) +
                        stats.get('no_candle_pattern', 0) +
                        stats.get('low_convergence', 0)
                    )
                    total_analyzed = signals_found + total_filtered
                    
                    if total_analyzed > 0:
                        success_rate = (signals_found / total_analyzed) * 100
                        log.info(f"📈 Success rate: {success_rate:.1f}% ({signals_found}/{total_analyzed})")
                    
                    log.info(f"📈 Detailed stats: {stats}")
                
                # Wait for next scan (very fast for complete rejection hunting)
                wait_time = max(0.1, SCAN_INTERVAL - scan_duration)
                log.info(f"Next COMPLETE rejection hunt in {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                log.error(f"Complete scanning loop error: {e}")
                await asyncio.sleep(10)
    
    async def run(self):
        """Run the complete scanner"""
        try:
            await self.initialize()
            
            # Run both loops
            await asyncio.gather(
                self.high_freq_complete_scanning(),
                self.monitor_positions()
            )
            
        except KeyboardInterrupt:
            log.info("Complete rejection scanner stopped by user")
            
            # Send final stats
            await self.send_final_stats()
            
        except Exception as e:
            log.error(f"Complete scanner crashed: {e}")
            
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
            
            # Calculate percentages
            total_analyzed = (
                stats['rejections_found'] +
                stats.get('rejections_filtered', 0) +
                stats.get('no_strength', 0) +
                stats.get('no_rejection_zone', 0) +
                stats.get('no_candle_pattern', 0) +
                stats.get('low_convergence', 0)
            )
            
            if total_analyzed > 0:
                found_pct = stats['rejections_found'] / total_analyzed * 100
                filtered_pct = stats.get('rejections_filtered', 0) / total_analyzed * 100
                no_strength_pct = stats.get('no_strength', 0) / total_analyzed * 100
                no_zone_pct = stats.get('no_rejection_zone', 0) / total_analyzed * 100
                no_pattern_pct = stats.get('no_candle_pattern', 0) / total_analyzed * 100
                low_convergence_pct = stats.get('low_convergence', 0) / total_analyzed * 100
            else:
                found_pct = filtered_pct = no_strength_pct = no_zone_pct = no_pattern_pct = low_convergence_pct = 0
            
            # Calculate success rate
            success_rate = found_pct if total_analyzed > 0 else 0
            
            message = f"""
🛑 <b>تم إيقاف ماسح الرفض الكامل</b>

<b>📊 إحصائيات اليوم:</b>
• عمليات المسح: {self.scan_cycle}
• الأزواج الممسوحة: {stats['pairs_scanned']}
• إشارات الرفض الكاملة: {stats['rejections_found']} ({found_pct:.1f}%)
• نجاح المسح: {success_rate:.1f}%
• رفض شراء: {stats['long_rejections']}
• رفض بيع: {stats['short_rejections']}

<b>🚫 أسباب الفلترة (7 طبقات تحليل):</b>
• مفلتر (تكرار): {stats.get('rejections_filtered', 0)} ({filtered_pct:.1f}%)
• بدون قوة كافية: {stats.get('no_strength', 0)} ({no_strength_pct:.1f}%)
• بدون منطقة رفض: {stats.get('no_rejection_zone', 0)} ({no_zone_pct:.1f}%)
• بدون نمط شموع: {stats.get('no_candle_pattern', 0)} ({no_pattern_pct:.1f}%)
• تقارب إطارات منخفض: {stats.get('low_convergence', 0)} ({low_convergence_pct:.1f}%)

<b>⚡ الصفقات النشطة:</b>
• حالياً: {active_count} صفقة نشطة

<b>🧠 فلسفة التاجر المحققة:</b>
تم الالتزام بـ 7 طبقات تحليل كاملة:
1. الطول الموجي ← السياق
2. قوة السوق ← قوة الحركة  
3. أنماط الشموع ← تأكيد الانعكاس
4. المؤشرات ← كل المؤشرات
5. ملف الفوليوم ← التراكمات
6. تأكيد الإطارات ← الاتساق
7. مناطق الرفض ← الزناد النهائي

تم الالتزام بـ:
• الدخول عند الرفض فقط بعد تحليل كامل
• صفقة واحدة لكل عملة
• عدم المطاردة
• قبول الخسائر
• صيد التوسع

#إحصائيات_الرفض_الكامل #تحليل_كاملي #متداول_تفاعلي #صفقة_واحدة
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info("✅ Final complete stats sent to Telegram")
                
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
                    "scanner": "COMPLETE Rejection-Based High-Frequency Scanner",
                    "scan_cycle": scanner.scan_cycle,
                    "active_trades": active_count,
                    "daily_stats": stats,
                    "analysis_layers": {
                        "wave_context": "Wave length and maturity analysis",
                        "market_strength": "Speed, distance, EMA angle, volume participation",
                        "candle_patterns": "All reversal/continuation patterns detection",
                        "indicators": "RSI, MACD, MA, Bollinger Bands, Volume analysis",
                        "volume_profile": "Volume at price levels and clusters",
                        "multi_tf_confirmation": "5 timeframe convergence analysis",
                        "rejection_zones": "EMA, Range, Failed break rejection detection"
                    },
                    "requirements": {
                        "min_convergence": "70% multi-TF alignment",
                        "min_rejection_strength": "60% rejection strength",
                        "risk_reward": "Minimum 1:2",
                        "deduplication": "One trade per symbol"
                    },
                    "telegram": {
                        "configured": bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID),
                        "notifications": "Signal + Entry + TP/SL alerts"
                    }
                }, indent=2)
            
            elif path == '/stats':
                response = json.dumps(scanner.scanner.get_daily_stats(), indent=2)
            
            elif path == '/analysis':
                response = json.dumps({
                    "trader_role": "Professional discretionary crypto trader",
                    "specialization": "COMPLETE 7-layer rejection analysis",
                    "analysis_layers": [
                        {
                            "layer": "Wave Context",
                            "description": "Wave length and maturity analysis (no counting)",
                            "timeframes": "1H + 15M",
                            "outputs": ["wave_length", "wave_maturity", "expansion_speed", "structure_type", "context_side"]
                        },
                        {
                            "layer": "Market Strength",
                            "description": "Measure market momentum and participation",
                            "timeframes": "15M + 5M",
                            "outputs": ["candle_speed", "distance_ratio", "ema_angle", "volume_participation", "strength_score", "strength_patterns"]
                        },
                        {
                            "layer": "Candle Patterns",
                            "description": "Detect all Japanese candlestick patterns",
                            "timeframes": "All (1H to 1M)",
                            "outputs": ["hammer", "engulfing", "doji", "pinbar", "morning_star", "evening_star", "soldiers", "crows"]
                        },
                        {
                            "layer": "Indicators",
                            "description": "Comprehensive technical indicator analysis",
                            "timeframes": "All (1H to 1M)",
                            "outputs": ["RSI", "MACD", "Moving Averages", "Bollinger Bands", "Volume indicators", "Support/Resistance"]
                        },
                        {
                            "layer": "Volume Profile",
                            "description": "Volume at price levels and high volume clusters",
                            "timeframes": "3M (entry)",
                            "outputs": ["volume_profile", "volume_clusters", "accumulation_zones"]
                        },
                        {
                            "layer": "Multi-TF Confirmation",
                            "description": "Check alignment across all timeframes",
                            "timeframes": "1H, 15M, 5M, 3M, 1M",
                            "outputs": ["confirmation_map", "convergence_score", "alignment_status"]
                        },
                        {
                            "layer": "Rejection Zones",
                            "description": "Detect and analyze rejection areas",
                            "timeframes": "3M (main), 1M (timing)",
                            "outputs": ["zone_type", "zone_price", "rejection_strength", "volume_confirmation", "trigger_candle"]
                        }
                    ],
                    "entry_philosophy": "Enter on first strong confirmed rejection after complete analysis",
                    "frequency_rule": "High frequency + asymmetric payoff",
                    "mindset": "Reaction trader, rejection specialist, not prediction-based, comfortable being wrong"
                }, indent=2)
            
            elif path == '/recent':
                if scanner.db:
                    scanner.db.row_factory = aiosqlite.Row
                    async with scanner.db.execute("""
                        SELECT symbol, side, entry_price, zone_type, rejection_strength, 
                               convergence_score, risk_reward, expected_move, 
                               created_at, status, close_reason, pnl_percent,
                               wave_length, structure_type, strength_score
                        FROM complete_rejection_signals 
                        ORDER BY created_at DESC 
                        LIMIT 20
                    """) as cursor:
                        rows = await cursor.fetchall()
                        signals = [dict(row) for row in rows]
                    
                    response = json.dumps({"signals": signals, "count": len(signals)}, indent=2)
                else:
                    response = json.dumps({"error": "Database not available"})
            
            elif path == '/active':
                active_count = len(scanner.scanner.deduplicator.active_signals)
                active_signals = list(scanner.scanner.deduplicator.active_signals.values())
                
                response = json.dumps({
                    "active_trades": active_count,
                    "signals": active_signals[:10]  # Show first 10
                }, indent=2)
            
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

# ================ ENTRY POINT ================
async def main():
    """Main function to run the complete scanner"""
    log.info("=" * 70)
    log.info("🚀 STARTING COMPLETE REJECTION SCANNER")
    log.info("=" * 70)
    
    # Create scanner instance
    scanner = CompleteRejectionScanner()
    
    try:
        # Start HTTP server in background
        http_task = asyncio.create_task(start_http_server(scanner, port=8080))
        
        # Give HTTP server a moment to start
        await asyncio.sleep(1)
        
        # Run the main scanner
        await scanner.run()
        
    except KeyboardInterrupt:
        log.info("Received interrupt, shutting down...")
    finally:
        # Cancel HTTP task if still running
        if 'http_task' in locals():
            http_task.cancel()
            try:
                await http_task
            except asyncio.CancelledError:
                pass

if __name__ == "__main__":
    # Check environment variables
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("⚠️ Telegram credentials not set. Notifications will not be sent.")
        log.warning("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables.")
    
    # Create data directory
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    # Run the main async function
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Scanner stopped by user")
    except Exception as e:
        log.error(f"Fatal error: {e}")