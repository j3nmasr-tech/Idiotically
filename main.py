#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 REJECTION-BASED DATA COLLECTION SCANNER
Professional discretionary trading system - DATA COLLECTION MODE
All filters are SCORING BONUSES, not requirements
Collect ALL data, analyze patterns later
TRADER MINDSET: Data scientist, pattern researcher
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
DB_PATH = "/app/data/rejection_data_collection.db"

# Ultra high-frequency scanning - DATA COLLECTION
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 3))   # 3 seconds - FAST DATA COLLECTION
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 100))   # Scan many pairs
MIN_VOLUME_USD = 500000  # $500K minimum

# Trading parameters (for scoring reference only)
MAX_STOP_LOSS_PCT = 1.0
MIN_TARGET_PCT = 1.5
MAX_TARGET_PCT = 6.0
MIN_RISK_REWARD = 2.0

# Rejection scanning - ALL AS SCORING BONUSES
REJECTION_CONFIG = {
    "rsi_long_zone": (40, 50),
    "rsi_short_zone": (50, 60),
    "ema_distance_threshold": 0.5,
    "min_rejection_strength": 0.6,  # Scoring reference only
    "min_convergence_score": 0.7,   # Scoring reference only
}

# Timeframes for analysis
TIMEFRAMES = {
    "1H": "1h",
    "15M": "15m",
    "5M": "5m",
    "3M": "3m",
    "1M": "1m"
}

# EMA periods
EMA_PERIODS = {
    "fast": 9,
    "medium": 21,
    "slow": 50,
    "very_slow": 200
}

# RSI settings
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# ================ DATA STRUCTURES ================
@dataclass
class WaveContext:
    """Wave length and maturity context"""
    wave_length: str
    wave_maturity: float
    expansion_speed: float
    structure_type: str
    context_side: str
    
    def to_dict(self) -> Dict:
        return {
            "wave_length": str(self.wave_length),
            "wave_maturity": float(self.wave_maturity),
            "expansion_speed": float(self.expansion_speed),
            "structure_type": str(self.structure_type),
            "context_side": str(self.context_side)
        }

@dataclass
class MarketStrength:
    """Market strength analysis"""
    candle_speed: float
    distance_ratio: float
    ema_angle: float
    volume_participation: float
    strength_score: float
    is_continuation: bool
    is_rejection_setup: bool
    is_absorption: bool
    is_compression: bool
    
    def to_dict(self) -> Dict:
        return {
            "candle_speed": float(self.candle_speed),
            "distance_ratio": float(self.distance_ratio),
            "ema_angle": float(self.ema_angle),
            "volume_participation": float(self.volume_participation),
            "strength_score": float(self.strength_score),
            "is_continuation": bool(self.is_continuation),
            "is_rejection_setup": bool(self.is_rejection_setup),
            "is_absorption": bool(self.is_absorption),
            "is_compression": bool(self.is_compression)
        }

@dataclass
class RejectionZone:
    """Key rejection area analysis"""
    zone_type: str
    price_level: float
    strength: float
    volume_confirmation: bool
    rsi_position: str
    is_active: bool
    
    def to_dict(self) -> Dict:
        return {
            "zone_type": str(self.zone_type),
            "price_level": float(self.price_level),
            "strength": float(self.strength),
            "volume_confirmation": bool(self.volume_confirmation),
            "rsi_position": str(self.rsi_position),
            "is_active": bool(self.is_active)
        }

@dataclass
class CandlePattern:
    """Candle pattern analysis"""
    pattern_name: str
    pattern_type: str
    reliability: float
    confirmation_required: bool
    timeframe: str
    has_long_wick: bool
    has_short_wick: bool
    body_ratio: float
    engulfing_size: float
    
    def to_dict(self) -> Dict:
        return {
            "pattern_name": str(self.pattern_name),
            "pattern_type": str(self.pattern_type),
            "reliability": float(self.reliability),
            "confirmation_required": bool(self.confirmation_required),
            "timeframe": str(self.timeframe),
            "has_long_wick": bool(self.has_long_wick),
            "has_short_wick": bool(self.has_short_wick),
            "body_ratio": float(self.body_ratio),
            "engulfing_size": float(self.engulfing_size)
        }

@dataclass
class IndicatorAnalysis:
    """Comprehensive indicator analysis"""
    rsi_value: float
    rsi_trend: str
    rsi_divergence: str
    rsi_momentum: float
    ma_alignment: str
    price_vs_ma: str
    ma_distance: float
    macd_signal: str
    macd_momentum: float
    macd_trend: str
    bb_position: str
    bb_squeeze: bool
    bb_width: float
    volume_trend: str
    volume_spike: bool
    obv_trend: str
    key_support: float
    key_resistance: float
    sr_strength: float
    momentum_score: float
    trend_score: float
    volatility_score: float
    
    def to_dict(self) -> Dict:
        return {
            "rsi_value": float(self.rsi_value),
            "rsi_trend": str(self.rsi_trend),
            "rsi_divergence": str(self.rsi_divergence),
            "rsi_momentum": float(self.rsi_momentum),
            "ma_alignment": str(self.ma_alignment),
            "price_vs_ma": str(self.price_vs_ma),
            "ma_distance": float(self.ma_distance),
            "macd_signal": str(self.macd_signal),
            "macd_momentum": float(self.macd_momentum),
            "macd_trend": str(self.macd_trend),
            "bb_position": str(self.bb_position),
            "bb_squeeze": bool(self.bb_squeeze),
            "bb_width": float(self.bb_width),
            "volume_trend": str(self.volume_trend),
            "volume_spike": bool(self.volume_spike),
            "obv_trend": str(self.obv_trend),
            "key_support": float(self.key_support),
            "key_resistance": float(self.key_resistance),
            "sr_strength": float(self.sr_strength),
            "momentum_score": float(self.momentum_score),
            "trend_score": float(self.trend_score),
            "volatility_score": float(self.volatility_score)
        }

@dataclass
class RejectionSignal:
    """Rejection-based trade signal - DATA COLLECTION MODE"""
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
    
    # Entry triggers
    rejection_type: str
    trigger_candle: str
    rsi_at_entry: float
    
    # Metrics
    rejection_strength: float
    risk_reward: float
    expected_move_pct: float
    
    # Timing
    timeframe_used: str
    signal_timestamp: float
    conditions_met: List[str]
    
    # Enhanced analysis
    candle_patterns: List[CandlePattern]
    dominant_pattern: Optional[CandlePattern]
    
    # Indicator analysis per timeframe
    indicators_1h: IndicatorAnalysis
    indicators_15m: IndicatorAnalysis
    indicators_5m: IndicatorAnalysis
    indicators_3m: IndicatorAnalysis
    indicators_1m: IndicatorAnalysis
    
    # Volume analysis
    volume_profile: Dict[str, float]
    volume_clusters: List[float]
    
    # Multi-timeframe confirmation
    multi_tf_confirmation: Dict[str, bool]
    convergence_score: float
    
    # DATA COLLECTION FIELDS
    filter_scores: Dict[str, float]  # Scores for each filter (0-1)
    total_score: float               # Overall score (0-100)
    passed_filters: List[str]        # Which filters passed
    failed_filters: List[str]        # Which filters failed
    data_quality: str                # GOOD, MEDIUM, POOR

# ================ PROFESSIONAL LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("rejection_data_collector")

# ================ CANDLE PATTERN SCANNER ================
class CandlePatternScanner:
    """Professional candle pattern scanner"""
    
    def detect_patterns(self, df: pd.DataFrame, timeframe: str) -> List[CandlePattern]:
        """Detect candle patterns in dataframe"""
        patterns = []
        
        if len(df) < 5:
            return patterns
        
        candles = df.iloc[-5:].copy()
        
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
        
        body_size = abs(candle['close'] - candle['open'])
        upper_wick = candle['high'] - max(candle['open'], candle['close'])
        lower_wick = min(candle['open'], candle['close']) - candle['low']
        total_range = candle['high'] - candle['low']
        
        if total_range == 0:
            return None
        
        if (lower_wick >= body_size * 2 and
            upper_wick <= body_size * 0.3 and
            lower_wick >= total_range * 0.6):
            
            if idx > 0:
                prev_trend = candles.iloc[idx-1]['close'] < candles.iloc[idx-1]['open']
                if prev_trend:
                    return CandlePattern(
                        pattern_name="HAMMER",
                        pattern_type="BULLISH_REVERSAL",
                        reliability=0.7,
                        confirmation_required=True,
                        timeframe=timeframe,
                        has_long_wick=True,
                        has_short_wick=False,
                        body_ratio=body_size/total_range if total_range > 0 else 0.0,
                        engulfing_size=0.0
                    )
                else:
                    return CandlePattern(
                        pattern_name="HANGING_MAN",
                        pattern_type="BEARISH_REVERSAL",
                        reliability=0.65,
                        confirmation_required=True,
                        timeframe=timeframe,
                        has_long_wick=True,
                        has_short_wick=False,
                        body_ratio=body_size/total_range if total_range > 0 else 0.0,
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
        if (previous['close'] < previous['open'] and
            current['close'] > current['open'] and
            current['open'] < previous['close'] and
            current['close'] > previous['open'] and
            current_body > previous_body * 1.2):
            
            return CandlePattern(
                pattern_name="ENGULFING_BULLISH",
                pattern_type="BULLISH_REVERSAL",
                reliability=0.75,
                confirmation_required=False,
                timeframe=timeframe,
                has_long_wick=False,
                has_short_wick=False,
                body_ratio=current_body/(current['high'] - current['low']) if (current['high'] - current['low']) > 0 else 0.0,
                engulfing_size=current_body/previous_body if previous_body > 0 else 0.0
            )
        
        # Bearish Engulfing
        elif (previous['close'] > previous['open'] and
              current['close'] < current['open'] and
              current['open'] > previous['close'] and
              current['close'] < previous['open'] and
              current_body > previous_body * 1.2):
            
            return CandlePattern(
                pattern_name="ENGULFING_BEARISH",
                pattern_type="BEARISH_REVERSAL",
                reliability=0.75,
                confirmation_required=False,
                timeframe=timeframe,
                has_long_wick=False,
                has_short_wick=False,
                body_ratio=current_body/(current['high'] - current['low']) if (current['high'] - current['low']) > 0 else 0.0,
                engulfing_size=current_body/previous_body if previous_body > 0 else 0.0
            )
        
        return None
    
    def _check_doji(self, candle, timeframe: str) -> Optional[CandlePattern]:
        """Check for Doji pattern"""
        body_size = abs(candle['close'] - candle['open'])
        total_range = candle['high'] - candle['low']
        
        if total_range == 0:
            return None
        
        if body_size <= total_range * 0.1:
            return CandlePattern(
                pattern_name="DOJI",
                pattern_type="REVERSAL",
                reliability=0.6,
                confirmation_required=True,
                timeframe=timeframe,
                has_long_wick=True,
                has_short_wick=True,
                body_ratio=body_size/total_range if total_range > 0 else 0.0,
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
        
        if ((upper_wick >= body_size * 3 and lower_wick <= body_size * 0.5) or
            (lower_wick >= body_size * 3 and upper_wick <= body_size * 0.5)):
            
            pattern_type = "BULLISH_REVERSAL" if lower_wick > upper_wick else "BEARISH_REVERSAL"
            
            return CandlePattern(
                pattern_name="PINBAR",
                pattern_type=pattern_type,
                reliability=0.8,
                confirmation_required=False,
                timeframe=timeframe,
                has_long_wick=True,
                has_short_wick=False,
                body_ratio=body_size/total_range if total_range > 0 else 0.0,
                engulfing_size=0.0
            )
        
        return None
    
    def _check_morning_evening_star(self, candles: pd.DataFrame, timeframe: str) -> Optional[CandlePattern]:
        """Check for Morning/Evening Star pattern"""
        if len(candles) < 3:
            return None
        
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
                body_ratio=abs(third['close'] - third['open'])/(third['high'] - third['low']) if (third['high'] - third['low']) > 0 else 0.0,
                engulfing_size=0.0
            )
        
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
                body_ratio=abs(third['close'] - third['open'])/(third['high'] - third['low']) if (third['high'] - third['low']) > 0 else 0.0,
                engulfing_size=0.0
            )
        
        return None
    
    def _check_three_soldiers_crows(self, candles: pd.DataFrame, timeframe: str) -> Optional[CandlePattern]:
        """Check for Three White Soldiers / Three Black Crows"""
        if len(candles) < 3:
            return None
        
        recent = candles.iloc[-3:]
        
        # Three White Soldiers
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
        
        # Three Black Crows
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
        
        rsi_analysis = self._analyze_rsi(df)
        ma_analysis = self._analyze_moving_averages(df)
        macd_analysis = self._analyze_macd(df)
        bb_analysis = self._analyze_bollinger_bands(df)
        volume_analysis = self._analyze_volume(df)
        sr_analysis = self._analyze_support_resistance(df)
        
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
        rsi = self.calculate_rsi(df['close'])
        
        if len(rsi) < 14:
            return {"value": 50.0, "trend": "NEUTRAL", "divergence": "NONE", "momentum": 0.5}
        
        current_rsi = float(rsi.iloc[-1])
        rsi_ma = rsi.rolling(window=5).mean()
        rsi_trend = "BULLISH" if current_rsi > rsi_ma.iloc[-1] else "BEARISH"
        
        rsi_slope = self._calculate_slope(rsi.values[-5:])
        rsi_momentum = float(min(abs(rsi_slope) * 10, 1.0))
        
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
        
        recent_prices = prices[-20:]
        recent_rsi = rsi_values[-20:]
        
        price_peaks, price_troughs = self._find_swings(recent_prices)
        rsi_peaks, rsi_troughs = self._find_swings(recent_rsi)
        
        if len(price_peaks) >= 2 and len(rsi_peaks) >= 2:
            if (price_peaks[-1] > price_peaks[-2] and 
                rsi_peaks[-1] < rsi_peaks[-2]):
                return "BEARISH_DIVERGENCE"
        
        if len(price_troughs) >= 2 and len(rsi_troughs) >= 2:
            if (price_troughs[-1] < price_troughs[-2] and 
                rsi_troughs[-1] > rsi_troughs[-2]):
                return "BULLISH_DIVERGENCE"
        
        return "NONE"
    
    def _find_swings(self, data: np.ndarray) -> Tuple[List[float], List[float]]:
        """Find swing highs and lows"""
        peaks = []
        troughs = []
        
        for i in range(2, len(data)-2):
            if (data[i] > data[i-2] and data[i] > data[i-1] and 
                data[i] > data[i+1] and data[i] > data[i+2]):
                peaks.append(float(data[i]))
            elif (data[i] < data[i-2] and data[i] < data[i-1] and 
                  data[i] < data[i+1] and data[i] < data[i+2]):
                troughs.append(float(data[i]))
        
        return peaks, troughs
    
    def _analyze_moving_averages(self, df: pd.DataFrame) -> Dict:
        """Analyze moving averages alignment"""
        current_price = float(df['close'].iloc[-1])
        
        ma_9 = float(df['close'].rolling(window=9).mean().iloc[-1])
        ma_21 = float(df['close'].rolling(window=21).mean().iloc[-1])
        ma_50 = float(df['close'].rolling(window=50).mean().iloc[-1])
        ma_200 = float(df['close'].rolling(window=200).mean().iloc[-1])
        
        mas = [ma_9, ma_21, ma_50, ma_200]
        bullish_aligned = all(mas[i] <= mas[i+1] for i in range(len(mas)-1))
        bearish_aligned = all(mas[i] >= mas[i+1] for i in range(len(mas)-1))
        
        alignment = "BULLISH_ALIGNED" if bullish_aligned else (
                   "BEARISH_ALIGNED" if bearish_aligned else "MIXED")
        
        if current_price > ma_200:
            price_position = "ABOVE_ALL"
        elif current_price < ma_200:
            price_position = "BELOW_ALL"
        else:
            price_position = "BETWEEN"
        
        if ma_200 > 0:
            ma_distance = float(abs(current_price - ma_200) / ma_200 * 100)
        else:
            ma_distance = 0.0
        
        return {
            "alignment": alignment,
            "price_position": price_position,
            "distance": ma_distance
        }
    
    def _analyze_macd(self, df: pd.DataFrame) -> Dict:
        """Analyze MACD"""
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9, adjust=False).mean()
        histogram = macd - signal
        
        if len(macd) < 2:
            return {"signal": "NEUTRAL", "momentum": 0.0, "trend": "NEUTRAL"}
        
        current_macd = float(macd.iloc[-1])
        current_signal = float(signal.iloc[-1])
        current_hist = float(histogram.iloc[-1])
        
        prev_macd = float(macd.iloc[-2])
        prev_signal = float(signal.iloc[-2])
        
        if prev_macd < prev_signal and current_macd > current_signal:
            macd_signal = "BULLISH_CROSS"
        elif prev_macd > prev_signal and current_macd < current_signal:
            macd_signal = "BEARISH_CROSS"
        else:
            macd_signal = "NEUTRAL"
        
        if df['close'].iloc[-1] > 0:
            macd_momentum = float(abs(current_hist) / df['close'].iloc[-1] * 1000)
            macd_momentum = min(macd_momentum, 1.0)
        else:
            macd_momentum = 0.0
        
        macd_trend = "BULLISH" if current_macd > 0 else "BEARISH"
        
        return {
            "signal": macd_signal,
            "momentum": macd_momentum,
            "trend": macd_trend
        }
    
    def _analyze_bollinger_bands(self, df: pd.DataFrame) -> Dict:
        """Analyze Bollinger Bands"""
        current_price = float(df['close'].iloc[-1])
        
        ma_20 = df['close'].rolling(window=20).mean()
        std_20 = df['close'].rolling(window=20).std()
        upper_band = ma_20 + (std_20 * 2)
        lower_band = ma_20 - (std_20 * 2)
        
        if len(upper_band) == 0 or pd.isna(ma_20.iloc[-1]) or ma_20.iloc[-1] == 0:
            return {"position": "MIDDLE", "squeeze": False, "width": 0.0}
        
        current_upper = float(upper_band.iloc[-1])
        current_lower = float(lower_band.iloc[-1])
        current_ma_20 = float(ma_20.iloc[-1])
        
        if current_price >= current_upper * 0.99:
            bb_position = "UPPER_BAND"
        elif current_price <= current_lower * 1.01:
            bb_position = "LOWER_BAND"
        else:
            bb_position = "MIDDLE"
        
        if current_ma_20 > 0:
            band_width = float((current_upper - current_lower) / current_ma_20 * 100)
        else:
            band_width = 0.0
        
        if len(upper_band) > 50:
            avg_width = ((upper_band - lower_band) / ma_20 * 100).rolling(50).mean().iloc[-1]
            bb_squeeze = bool(band_width < float(avg_width) * 0.7)
        else:
            bb_squeeze = bool(band_width < 2.0)
        
        return {
            "position": bb_position,
            "squeeze": bb_squeeze,
            "width": band_width
        }
    
    def _analyze_volume(self, df: pd.DataFrame) -> Dict:
        """Analyze volume indicators"""
        if len(df) < 20:
            return {"trend": "FLAT", "spike": False, "obv_trend": "NEUTRAL"}
        
        recent_volume = df['volume'].values[-5:]
        volume_slope = self._calculate_slope(recent_volume)
        
        if volume_slope > 0.1:
            volume_trend = "INCREASING"
        elif volume_slope < -0.1:
            volume_trend = "DECREASING"
        else:
            volume_trend = "FLAT"
        
        avg_volume = float(df['volume'].rolling(20).mean().iloc[-1])
        current_volume = float(df['volume'].iloc[-1])
        volume_spike = bool(current_volume > avg_volume * 1.5)
        
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
        
        recent_high = float(df['high'].rolling(20).max().iloc[-1])
        recent_low = float(df['low'].rolling(20).min().iloc[-1])
        
        if len(df) >= 100:
            major_high = float(df['high'].rolling(100).max().iloc[-1])
            major_low = float(df['low'].rolling(100).min().iloc[-1])
        else:
            major_high = recent_high
            major_low = recent_low
        
        current_price = float(df['close'].iloc[-1])
        support = major_low if abs(current_price - major_low) < abs(current_price - recent_low) else recent_low
        resistance = major_high if abs(current_price - major_high) < abs(current_price - recent_high) else recent_high
        
        support_touches = sum((df['low'] <= support * 1.005) & (df['low'] >= support * 0.995))
        resistance_touches = sum((df['high'] >= resistance * 0.995) & (df['high'] <= resistance * 1.005))
        sr_strength = float(min(max(support_touches, resistance_touches) / 20, 1.0))
        
        return {
            "support": float(support),
            "resistance": float(resistance),
            "strength": sr_strength
        }
    
    def _calculate_slope(self, data: np.ndarray) -> float:
        """Calculate linear slope of data"""
        if len(data) < 2:
            return 0.0
        
        x = np.arange(len(data))
        slope, _ = np.polyfit(x, data, 1)
        return float(slope)
    
    def _calculate_momentum_score(self, rsi_analysis: Dict, macd_analysis: Dict) -> float:
        """Calculate overall momentum score"""
        scores = []
        weights = []
        
        scores.append(float(rsi_analysis['momentum']))
        weights.append(0.4)
        
        if rsi_analysis['divergence'] in ['BULLISH_DIVERGENCE', 'BEARISH_DIVERGENCE']:
            scores.append(0.8)
        else:
            scores.append(0.5)
        weights.append(0.2)
        
        scores.append(float(macd_analysis['momentum']))
        weights.append(0.4)
        
        return float(np.average(scores, weights=weights))
    
    def _calculate_trend_score(self, ma_analysis: Dict, macd_analysis: Dict) -> float:
        """Calculate overall trend score"""
        scores = []
        weights = []
        
        if ma_analysis['alignment'] == "BULLISH_ALIGNED":
            scores.append(0.8)
        elif ma_analysis['alignment'] == "BEARISH_ALIGNED":
            scores.append(0.2)
        else:
            scores.append(0.5)
        weights.append(0.5)
        
        if macd_analysis['trend'] == "BULLISH":
            scores.append(0.8)
        elif macd_analysis['trend'] == "BEARISH":
            scores.append(0.2)
        else:
            scores.append(0.5)
        weights.append(0.5)
        
        return float(np.average(scores, weights=weights))
    
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

# ================ CORE REJECTION ENGINE - DATA COLLECTION MODE ================
class EnhancedRejectionBasedScanner:
    """High-frequency rejection scanner - DATA COLLECTION MODE"""
    
    class SignalDeduplicator:
        """Prevents duplicate signal generation"""
        
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
        
        def register_signal(self, signal):
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
            
            log.debug(f"Registered rejection signal {signal.signal_id[:8]} for {symbol}")
        
        def update_signal_status(self, signal_id: str, status: str):
            """Update signal status"""
            if signal_id in self.signal_status:
                self.signal_status[signal_id]["status"] = status
                log.debug(f"Signal {signal_id[:8]} status updated to {status}")
                
                if status == "CLOSED":
                    symbol = self.signal_status[signal_id]["symbol"]
                    log.info(f"✅ Signal {signal_id[:8]} for {symbol} CLOSED")
        
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
            "rejections_found": 0,
            "long_rejections": 0,
            "short_rejections": 0,
            "pairs_scanned": 0,
            "signals_collected": 0,
            "high_score_signals": 0,
            "medium_score_signals": 0,
            "low_score_signals": 0
        }
        self.deduplicator = self.SignalDeduplicator()
        self.active_signal_ids = set()
        self.pattern_scanner = CandlePatternScanner()
        self.indicator_analyzer = IndicatorAnalyzer()
    
    # ========== ANALYSIS METHODS ==========
    
    def analyze_wave_context(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> WaveContext:
        """Analyze wave length and maturity"""
        try:
            if df_1h is None or df_15m is None:
                return self._get_default_wave_context()
            
            if len(df_1h) < 20 or len(df_15m) < 30:
                return self._get_default_wave_context()
            
            wave_length, wave_maturity = self._analyze_wave_length(df_1h)
            expansion_speed = self._analyze_expansion_speed(df_15m)
            structure_type = self._determine_structure(df_15m)
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
            
            ma_20 = np.mean(recent_prices[-20:])
            current_price = recent_prices[-1]
            volatility = np.std(recent_prices[-20:])
            
            if volatility > 0:
                distance_pct = abs(current_price - ma_20) / ma_20 * 100
                volatility_pct = volatility / ma_20 * 100
                wave_maturity = min(distance_pct / (volatility_pct * 2), 1.0)
            else:
                wave_maturity = 0.5
            
            return wave_length, float(wave_maturity)
            
        except Exception as e:
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
            return float(min(avg_speed / 5.0, 1.0))
            
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
            
            price_change = prices[-1] - prices[0]
            price_change_pct = abs(price_change) / prices[0] * 100
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
            return float(min(avg_speed / 2.0, 1.0))
            
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
                return float(min(distance_pct / 5.0, 1.0))
            
            return 0.5
            
        except Exception as e:
            return 0.5
    
    def _calculate_ema_angle(self, df: pd.DataFrame) -> float:
        """Calculate EMA angle/slope"""
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
                return float(min(angle_metric, 1.0))
            
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
                if ratio >= 1.0:
                    return float(min((ratio - 1.0) * 2, 1.0))
                else:
                    return float(max((ratio - 1.0) * 2, 0.0))
            
            return 0.5
            
        except Exception as e:
            return 0.5
    
    def _calculate_strength_score(self, candle_speed: float, distance_ratio: float, 
                                 ema_angle: float, volume_participation: float) -> float:
        """Calculate overall strength score"""
        weights = [0.2, 0.2, 0.2, 0.4]
        factors = [candle_speed, distance_ratio, ema_angle, volume_participation]
        return float(np.average(factors, weights=weights))
    
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
            
            return bool(is_continuation), bool(is_rejection_setup), bool(is_absorption), bool(is_compression)
            
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
    
    def find_rejection_zones(self, df: pd.DataFrame, current_price: float, 
                            rsi_value: float, emas: Dict[str, float]) -> List[RejectionZone]:
        """Find all active rejection zones"""
        zones = []
        
        try:
            if df is None or len(df) < 20:
                return zones
            
            ema_zones = self._find_ema_rejection_zones(current_price, emas)
            zones.extend(ema_zones)
            
            range_zones = self._find_range_rejection_zones(df, current_price)
            zones.extend(range_zones)
            
            failed_zones = self._find_failed_break_zones(df, current_price)
            zones.extend(failed_zones)
            
            for zone in zones:
                zone.rsi_position = self._analyze_rsi_position(rsi_value, zone.zone_type)
            
            return zones
            
        except Exception as e:
            log.error(f"Rejection zone error: {e}")
            return []
    
    def _find_ema_rejection_zones(self, current_price: float, emas: Dict[str, float]) -> List[RejectionZone]:
        """Find EMA rejection zones"""
        zones = []
        
        try:
            for ema_name, ema_value in emas.items():
                if ema_value == 0:
                    continue
                
                distance_pct = abs(current_price - ema_value) / ema_value * 100
                
                if distance_pct <= REJECTION_CONFIG["ema_distance_threshold"]:
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
                        price_level=float(ema_value),
                        strength=float(strength),
                        volume_confirmation=False,
                        rsi_position="IN_ZONE",
                        is_active=True
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
            
            recent_high = float(df['high'].values[-20:].max())
            recent_low = float(df['low'].values[-20:].min())
            
            high_distance_pct = abs(current_price - recent_high) / recent_high * 100
            if high_distance_pct <= 0.3:
                zones.append(RejectionZone(
                    zone_type="RANGE_HIGH",
                    price_level=recent_high,
                    strength=0.8,
                    volume_confirmation=False,
                    rsi_position="IN_ZONE",
                    is_active=True
                ))
            
            low_distance_pct = abs(current_price - recent_low) / recent_low * 100
            if low_distance_pct <= 0.3:
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
            
            recent_high = float(df['high'].values[-5:].max())
            prev_high = float(df['high'].values[-10:-5].max())
            
            if current_price < recent_high and recent_high > prev_high * 1.005:
                if any(df['close'].values[-5:] < recent_high * 0.995):
                    zones.append(RejectionZone(
                        zone_type="FAILED_BREAKOUT",
                        price_level=recent_high,
                        strength=0.85,
                        volume_confirmation=False,
                        rsi_position="IN_ZONE",
                        is_active=True
                    ))
            
            recent_low = float(df['low'].values[-5:].min())
            prev_low = float(df['low'].values[-10:-5].min())
            
            if current_price > recent_low and recent_low < prev_low * 0.995:
                if any(df['close'].values[-5:] > recent_low * 1.005):
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
        if "SUPPORT" in zone_type or "LOW" in zone_type or "BREAKDOWN" in zone_type:
            if REJECTION_CONFIG["rsi_long_zone"][0] <= rsi_value <= REJECTION_CONFIG["rsi_long_zone"][1]:
                return "IN_ZONE"
            elif rsi_value < 30:
                return "OVEREXTENDED"
            else:
                return "NEUTRAL"
        
        elif "RESISTANCE" in zone_type or "HIGH" in zone_type or "BREAKOUT" in zone_type:
            if REJECTION_CONFIG["rsi_short_zone"][0] <= rsi_value <= REJECTION_CONFIG["rsi_short_zone"][1]:
                return "IN_ZONE"
            elif rsi_value > 70:
                return "OVEREXTENDED"
            else:
                return "NEUTRAL"
        
        return "NEUTRAL"
    
    # ========== SCORING METHODS ==========
    
    def analyze_candle_patterns(self, multi_tf_data: Dict[str, pd.DataFrame]) -> Tuple[List[CandlePattern], Optional[CandlePattern]]:
        """Analyze candle patterns on all timeframes"""
        all_patterns = []
        
        for tf_name, df in multi_tf_data.items():
            if df is not None and len(df) >= 10:
                patterns = self.pattern_scanner.detect_patterns(df, tf_name)
                all_patterns.extend(patterns)
        
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
        
        price_levels = np.linspace(df['low'].min(), df['high'].max(), 20)
        
        for level in price_levels:
            mask = (df['low'] <= level * 1.005) & (df['high'] >= level * 0.995)
            volume_at_level = float(df.loc[mask, 'volume'].sum())
            volume_profile[f"{level:.4f}"] = volume_at_level
        
        df_sorted = df.sort_values('volume', ascending=False)
        top_volumes = df_sorted.head(5)
        
        for _, row in top_volumes.iterrows():
            cluster_price = float((row['high'] + row['low'] + row['close']) / 3)
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
                    analysis.rsi_value < 60 and
                    analysis.macd_trend in ["BULLISH", "NEUTRAL"] and
                    analysis.ma_alignment in ["BULLISH_ALIGNED", "MIXED"] and
                    analysis.bb_position != "UPPER_BAND"
                )
            else:
                confirms = (
                    analysis.rsi_value > 40 and
                    analysis.macd_trend in ["BEARISH", "NEUTRAL"] and
                    analysis.ma_alignment in ["BEARISH_ALIGNED", "MIXED"] and
                    analysis.bb_position != "LOWER_BAND"
                )
            
            if (side == "LONG" and analysis.rsi_divergence == "BULLISH_DIVERGENCE") or \
               (side == "SHORT" and analysis.rsi_divergence == "BEARISH_DIVERGENCE"):
                confirms = True
            
            confirmation[tf_name] = bool(confirms)
        
        return confirmation
    
    def calculate_convergence_score(self, confirmation: Dict[str, bool]) -> float:
        """Calculate multi-timeframe convergence score"""
        if not confirmation:
            return 0.0
        
        weights = {"1M": 0.3, "3M": 0.25, "5M": 0.2, "15M": 0.15, "1H": 0.1}
        
        weighted_score = 0.0
        for tf_name, confirms in confirmation.items():
            weight = weights.get(tf_name, 0.1)
            weighted_score += (1 if confirms else 0) * weight
        
        return float(weighted_score)
    
    def pattern_confirms_rejection(self, pattern: Optional[CandlePattern], side: str) -> bool:
        """Check if candle pattern confirms rejection direction"""
        if not pattern:
            return True
        
        if side == "LONG":
            return pattern.pattern_type in ["BULLISH_REVERSAL", "BULLISH_CONTINUATION"]
        else:
            return pattern.pattern_type in ["BEARISH_REVERSAL", "BEARISH_CONTINUATION"]
    
    # ========== SCORING FUNCTIONS ==========
    
    def _calculate_filter_scores(self, market_strength: MarketStrength,
                               rejection_zones: List[RejectionZone],
                               candle_patterns: List[CandlePattern],
                               dominant_pattern: Optional[CandlePattern],
                               side: str, zone_type: str,
                               convergence_score: float,
                               risk_reward: float,
                               rejection_strength: float) -> Tuple[Dict[str, float], List[str], List[str]]:
        """Calculate scores for each filter (0-1 scale)"""
        filter_scores = {}
        passed_filters = []
        failed_filters = []
        
        # 1. Market Strength Score
        strength_score = market_strength.strength_score
        filter_scores["market_strength"] = strength_score
        if strength_score >= 0.4:
            passed_filters.append("MARKET_STRENGTH")
        else:
            failed_filters.append("MARKET_STRENGTH")
        
        # 2. Rejection Zone Score
        zone_score = 0.0
        if rejection_zones:
            best_zone = max(rejection_zones, key=lambda z: z.strength)
            zone_score = best_zone.strength
            filter_scores["rejection_zone"] = zone_score
            if zone_score >= 0.6:
                passed_filters.append("REJECTION_ZONE")
            else:
                failed_filters.append("REJECTION_ZONE")
        else:
            filter_scores["rejection_zone"] = 0.0
            failed_filters.append("REJECTION_ZONE")
        
        # 3. Volume Confirmation Score
        volume_score = 0.0
        if rejection_zones:
            best_zone = max(rejection_zones, key=lambda z: z.strength)
            volume_score = 1.0 if best_zone.volume_confirmation else 0.3
        filter_scores["volume_confirmation"] = volume_score
        if volume_score >= 0.8:
            passed_filters.append("VOLUME_CONFIRMATION")
        else:
            failed_filters.append("VOLUME_CONFIRMATION")
        
        # 4. Candle Pattern Score
        pattern_score = 0.5  # Default if no patterns
        if candle_patterns:
            pattern_score = len(candle_patterns) / 10.0
            pattern_score = min(pattern_score, 1.0)
        filter_scores["candle_patterns"] = pattern_score
        if pattern_score >= 0.5:
            passed_filters.append("CANDLE_PATTERNS")
        else:
            failed_filters.append("CANDLE_PATTERNS")
        
        # 5. Multi-TF Convergence Score
        filter_scores["convergence"] = convergence_score
        if convergence_score >= REJECTION_CONFIG["min_convergence_score"]:
            passed_filters.append("CONVERGENCE")
        else:
            failed_filters.append("CONVERGENCE")
        
        # 6. RSI Position Score
        rsi_score = 0.5
        if rejection_zones:
            best_zone = max(rejection_zones, key=lambda z: z.strength)
            if best_zone.rsi_position == "IN_ZONE":
                rsi_score = 0.9
            elif best_zone.rsi_position == "OVEREXTENDED":
                rsi_score = 0.3
            else:
                rsi_score = 0.5
        filter_scores["rsi_position"] = rsi_score
        if rsi_score >= 0.8:
            passed_filters.append("RSI_POSITION")
        else:
            failed_filters.append("RSI_POSITION")
        
        # 7. Risk/Reward Score
        rr_score = min(risk_reward / 3.0, 1.0)  # 3:1 = 1.0, 1:1 = 0.33
        filter_scores["risk_reward"] = rr_score
        if rr_score >= (MIN_RISK_REWARD / 3.0):
            passed_filters.append("RISK_REWARD")
        else:
            failed_filters.append("RISK_REWARD")
        
        # 8. Rejection Strength Score
        filter_scores["rejection_strength"] = rejection_strength
        if rejection_strength >= REJECTION_CONFIG["min_rejection_strength"]:
            passed_filters.append("REJECTION_STRENGTH")
        else:
            failed_filters.append("REJECTION_STRENGTH")
        
        # 9. Pattern Confirmation Score
        pattern_confirmation_score = 1.0 if (dominant_pattern and self.pattern_confirms_rejection(dominant_pattern, side)) else 0.5
        filter_scores["pattern_confirmation"] = pattern_confirmation_score
        if pattern_confirmation_score >= 0.8:
            passed_filters.append("PATTERN_CONFIRMATION")
        else:
            failed_filters.append("PATTERN_CONFIRMATION")
        
        return filter_scores, passed_filters, failed_filters
    
    def _calculate_total_score(self, filter_scores: Dict[str, float]) -> Tuple[float, str]:
        """Calculate total score and data quality"""
        weights = {
            "market_strength": 0.10,
            "rejection_zone": 0.15,
            "volume_confirmation": 0.10,
            "candle_patterns": 0.08,
            "convergence": 0.12,
            "rsi_position": 0.10,
            "risk_reward": 0.12,
            "rejection_strength": 0.13,
            "pattern_confirmation": 0.10
        }
        
        total_score = 0.0
        for filter_name, score in filter_scores.items():
            weight = weights.get(filter_name, 0.1)
            total_score += score * weight
        
        total_score = total_score * 100  # Convert to 0-100 scale
        
        if total_score >= 70:
            data_quality = "GOOD"
        elif total_score >= 50:
            data_quality = "MEDIUM"
        else:
            data_quality = "POOR"
        
        return float(total_score), data_quality
    
    # ========== REJECTION SIGNAL GENERATION - DATA COLLECTION MODE ==========
    
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
                emas[name] = float(ema_series.iloc[-1]) if len(ema_series) > 0 else 0.0
            return emas
        except Exception as e:
            return {name: 0.0 for name in EMA_PERIODS.keys()}
    
    def generate_enhanced_rejection_signal(self, multi_tf_data: Dict[str, pd.DataFrame], 
                                          symbol: str) -> Optional[RejectionSignal]:
        """
        Generate COMPLETE rejection-based signal with ALL analyses
        DATA COLLECTION MODE: All filters are scoring bonuses, not requirements
        """
        try:
            # Get timeframe data
            tf_1h = multi_tf_data.get("1H")
            tf_15m = multi_tf_data.get("15M")
            tf_5m = multi_tf_data.get("5M")
            tf_3m = multi_tf_data.get("3M")
            tf_1m = multi_tf_data.get("1M")
            
            # Check data availability
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
            
            # DATA COLLECTION: No filtering, just record
            if market_strength.strength_score < 0.4:
                log.debug(f"{symbol}: Low market strength ({market_strength.strength_score:.2f}) - recording anyway")
            
            # ===== 3. CANDLE PATTERN ANALYSIS =====
            candle_patterns, dominant_pattern = self.analyze_candle_patterns(multi_tf_data)
            
            # ===== 4. INDICATOR ANALYSIS =====
            indicators = self.analyze_indicators_all_timeframes(multi_tf_data)
            
            # ===== 5. VOLUME PROFILE ANALYSIS =====
            volume_profile, volume_clusters = self.analyze_volume_profile(tf_3m)
            
            # ===== 6. REJECTION ZONE ANALYSIS =====
            current_price = float(tf_3m['close'].iloc[-1])
            emas = self.calculate_emas(tf_3m)
            
            rsi_series = self.calculate_rsi(tf_3m['close'])
            current_rsi = float(rsi_series.iloc[-1]) if len(rsi_series) > 0 else 50.0
            
            rejection_zones = self.find_rejection_zones(tf_3m, current_price, current_rsi, emas)
            
            # DATA COLLECTION: Record even if no zones
            if not rejection_zones:
                log.debug(f"{symbol}: No rejection zones found - recording contextual data")
                # Create a dummy zone for data collection
                dummy_zone = RejectionZone(
                    zone_type="NO_ZONE",
                    price_level=current_price,
                    strength=0.0,
                    volume_confirmation=False,
                    rsi_position="NEUTRAL",
                    is_active=False
                )
                rejection_zones = [dummy_zone]
            
            # Check volume confirmation
            valid_zones = []
            for zone in rejection_zones:
                zone.volume_confirmation = self._check_volume_confirmation(tf_3m, zone.zone_type)
                valid_zones.append(zone)
            
            # Select best zone
            best_zone = max(valid_zones, key=lambda z: z.strength) if valid_zones else rejection_zones[0]
            
            # Determine trade side
            side = None
            if best_zone.zone_type in ["EMA_SUPPORT", "RANGE_LOW", "FAILED_BREAKDOWN", "DEMAND"]:
                side = "LONG"
            elif best_zone.zone_type in ["EMA_RESISTANCE", "RANGE_HIGH", "FAILED_BREAKOUT", "SUPPLY"]:
                side = "SHORT"
            else:
                # For NO_ZONE or unknown zones, determine from context
                if current_rsi < 50:
                    side = "LONG"
                else:
                    side = "SHORT"
            
            # ===== 7. CANDLE PATTERN CONFIRMATION =====
            # DATA COLLECTION: No filtering, just record
            pattern_confirms = self.pattern_confirms_rejection(dominant_pattern, side)
            if not pattern_confirms:
                log.debug(f"{symbol}: Candle pattern doesn't confirm {side} - recording anyway")
            
            # ===== 8. MULTI-TIMEFRAME CONFIRMATION =====
            multi_tf_confirmation = self.check_multi_tf_confirmation(indicators, side, best_zone.zone_type)
            convergence_score = self.calculate_convergence_score(multi_tf_confirmation)
            
            # DATA COLLECTION: Record even with low convergence
            if convergence_score < REJECTION_CONFIG["min_convergence_score"]:
                log.debug(f"{symbol}: Low multi-TF convergence ({convergence_score:.2f}) - recording anyway")
            
            # ===== 9. RSI POSITION CHECK =====
            # DATA COLLECTION: No filtering, just record
            if side == "LONG" and best_zone.rsi_position != "IN_ZONE":
                log.debug(f"{symbol}: RSI not in LONG zone ({current_rsi:.1f}) - recording anyway")
            elif side == "SHORT" and best_zone.rsi_position != "IN_ZONE":
                log.debug(f"{symbol}: RSI not in SHORT zone ({current_rsi:.1f}) - recording anyway")
            
            # ===== 10. TRADE DEDUPLICATION =====
            # Still apply deduplication to avoid spam
            if not self.deduplicator.should_generate_signal(symbol, side, current_price):
                log.debug(f"{symbol}: Duplicate signal filtered")
                return None
            
            # ===== 11. ANALYZE REJECTION CANDLE =====
            rejection_type, trigger_candle = self._analyze_rejection_candle(tf_3m, side, best_zone)
            
            if not rejection_type:
                rejection_type = "NO_CLEAR_REJECTION"
                trigger_candle = "NONE"
                log.debug(f"{symbol}: No clear rejection candle - recording contextual data")
            
            # ===== 12. CALCULATE ENTRY, SL, TP =====
            stop_loss_pct = float(np.random.uniform(0.5, MAX_STOP_LOSS_PCT))
            target_pct = float(np.random.uniform(MIN_TARGET_PCT, MAX_TARGET_PCT))
            
            if side == "LONG":
                entry_price = float(best_zone.price_level * 1.001) if best_zone.zone_type != "NO_ZONE" else current_price
                stop_loss = float(entry_price * (1 - stop_loss_pct / 100))
                take_profit = float(entry_price * (1 + target_pct / 100))
            else:
                entry_price = float(best_zone.price_level * 0.999) if best_zone.zone_type != "NO_ZONE" else current_price
                stop_loss = float(entry_price * (1 + stop_loss_pct / 100))
                take_profit = float(entry_price * (1 - target_pct / 100))
            
            # Calculate Risk/Reward
            risk = abs(entry_price - stop_loss)
            reward = abs(take_profit - entry_price)
            
            if risk == 0:
                risk_reward = 1.0
            else:
                risk_reward = float(reward / risk)
            
            # DATA COLLECTION: Record even with poor R:R
            if risk_reward < MIN_RISK_REWARD:
                log.debug(f"{symbol}: R:R too low ({risk_reward:.1f}:1) - recording anyway")
            
            # ===== 13. CALCULATE REJECTION STRENGTH =====
            rejection_strength = self._calculate_rejection_strength(
                best_zone, market_strength, wave_context, current_rsi, convergence_score
            )
            
            # DATA COLLECTION: Record even with weak rejection
            if rejection_strength < REJECTION_CONFIG["min_rejection_strength"]:
                log.debug(f"{symbol}: Rejection too weak ({rejection_strength:.2f}) - recording anyway")
            
            # ===== 14. CALCULATE FILTER SCORES =====
            filter_scores, passed_filters, failed_filters = self._calculate_filter_scores(
                market_strength, rejection_zones, candle_patterns, dominant_pattern,
                side, best_zone.zone_type, convergence_score, risk_reward, rejection_strength
            )
            
            # ===== 15. CALCULATE TOTAL SCORE =====
            total_score, data_quality = self._calculate_total_score(filter_scores)
            
            # ===== 16. DETERMINE CONDITIONS MET =====
            conditions_met = self._get_rejection_conditions(
                wave_context, market_strength, best_zone, rejection_type,
                dominant_pattern, multi_tf_confirmation
            )
            
            # ===== 17. CREATE SIGNAL ID =====
            signal_id = hashlib.md5(
                f"{symbol}:{side}:{entry_price:.8f}:{time.time()}:{total_score:.2f}".encode()
            ).hexdigest()
            
            # ===== 18. CREATE COMPLETE SIGNAL =====
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
                convergence_score=convergence_score,
                
                # DATA COLLECTION FIELDS
                filter_scores=filter_scores,
                total_score=total_score,
                passed_filters=passed_filters,
                failed_filters=failed_filters,
                data_quality=data_quality
            )
            
            # ===== 19. UPDATE TRACKING =====
            self.deduplicator.register_signal(signal)
            self.active_signal_ids.add(signal_id)
            
            # ===== 20. UPDATE STATISTICS =====
            self.daily_stats["rejections_found"] += 1
            if side == "LONG":
                self.daily_stats["long_rejections"] += 1
            else:
                self.daily_stats["short_rejections"] += 1
            
            if total_score >= 70:
                self.daily_stats["high_score_signals"] += 1
            elif total_score >= 50:
                self.daily_stats["medium_score_signals"] += 1
            else:
                self.daily_stats["low_score_signals"] += 1
            
            # ===== 21. LOG DATA COLLECTION INFO =====
            log.info(f"📊 DATA COLLECTION: {symbol} {side} @ {entry_price:.4f}")
            log.info(f"   Score: {total_score:.1f}/100 ({data_quality})")
            log.info(f"   Passed: {len(passed_filters)}/{len(filter_scores)} filters")
            log.info(f"   Zone: {best_zone.zone_type}, RSI: {current_rsi:.1f}")
            log.info(f"   Filters passed: {', '.join(passed_filters[:3])}{'...' if len(passed_filters) > 3 else ''}")
            
            return signal
            
        except Exception as e:
            log.error(f"Rejection signal error for {symbol}: {e}")
            import traceback
            log.error(f"Traceback: {traceback.format_exc()}")
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
            
            if "FAILED" in zone_type:
                volume_trend = np.polyfit(range(5), recent_candles['volume'].values[-5:], 1)[0]
                if volume_trend < 0:
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
            
            has_wick_rejection = False
            wick_type = None
            
            if side == "LONG":
                if (current_candle['low'] < zone.price_level and 
                    current_candle['close'] > zone.price_level):
                    has_wick_rejection = True
                    wick_type = "SUPPORT_WICK"
            
            else:
                if (current_candle['high'] > zone.price_level and 
                    current_candle['close'] < zone.price_level):
                    has_wick_rejection = True
                    wick_type = "RESISTANCE_WICK"
            
            momentum_shift = False
            candle_type = None
            
            if side == "LONG":
                if (prev_candle['close'] < prev_candle['open'] and
                    current_candle['close'] > current_candle['open'] and
                    abs(current_candle['close'] - zone.price_level) / zone.price_level < 0.002):
                    momentum_shift = True
                    candle_type = "BULLISH_REVERSAL"
            
            else:
                if (prev_candle['close'] > prev_candle['open'] and
                    current_candle['close'] < current_candle['open'] and
                    abs(current_candle['close'] - zone.price_level) / zone.price_level < 0.002):
                    momentum_shift = True
                    candle_type = "BEARISH_REVERSAL"
            
            if has_wick_rejection:
                return "WICK_REJECTION", wick_type
            elif momentum_shift:
                return "MOMENTUM_REJECTION", candle_type
            else:
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
        
        factors.append(float(zone.strength))
        weights.append(0.2)
        
        factors.append(float(strength.strength_score))
        weights.append(0.2)
        
        if wave.structure_type == "CORRECTIVE":
            wave_score = 0.8
        elif wave.structure_type == "COMPRESSION":
            wave_score = 0.7
        else:
            wave_score = 0.5
        
        wave_score *= (1 - wave.wave_maturity * 0.5)
        factors.append(float(wave_score))
        weights.append(0.15)
        
        if zone.rsi_position == "IN_ZONE":
            rsi_score = 0.9
        elif zone.rsi_position == "OVEREXTENDED":
            rsi_score = 0.3
        else:
            rsi_score = 0.5
        
        factors.append(float(rsi_score))
        weights.append(0.15)
        
        factors.append(float(convergence_score))
        weights.append(0.2)
        
        volume_score = 0.8 if zone.volume_confirmation else 0.3
        factors.append(float(volume_score))
        weights.append(0.1)
        
        return float(np.average(factors, weights=weights))
    
    def _get_rejection_conditions(self, wave: WaveContext, strength: MarketStrength, 
                                 zone: RejectionZone, rejection_type: str,
                                 dominant_pattern: Optional[CandlePattern],
                                 multi_tf_confirmation: Dict[str, bool]) -> List[str]:
        """Get list of conditions met for this rejection"""
        conditions = []
        
        conditions.append(f"WAVE_{wave.wave_length}")
        conditions.append(f"STRUCTURE_{wave.structure_type}")
        
        if strength.is_continuation:
            conditions.append("STRENGTH_CONTINUATION")
        if strength.is_rejection_setup:
            conditions.append("STRENGTH_REJECTION_SETUP")
        if strength.is_absorption:
            conditions.append("STRENGTH_ABSORPTION")
        if strength.is_compression:
            conditions.append("STRENGTH_COMPRESSION")
        
        conditions.append(f"ZONE_{zone.zone_type}")
        if zone.volume_confirmation:
            conditions.append("VOLUME_CONFIRMED")
        
        conditions.append(f"RSI_{zone.rsi_position}")
        conditions.append(f"REJECTION_{rejection_type}")
        
        if dominant_pattern:
            conditions.append(f"PATTERN_{dominant_pattern.pattern_name}")
        
        confirmed_tfs = [tf for tf, confirms in multi_tf_confirmation.items() if confirms]
        if confirmed_tfs:
            conditions.append(f"MULTITF_{len(confirmed_tfs)}_CONFIRMED")
        
        return conditions
    
    def get_daily_stats(self) -> Dict:
        """Get daily statistics"""
        return self.daily_stats
    
    def cleanup_old_signals(self):
        """Clean up old signals"""
        self.deduplicator.remove_closed_signals()

# ================ MAIN SCANNER SYSTEM ================
class CompleteRejectionScanner:
    """Main scanner system - DATA COLLECTION MODE"""
    
    def __init__(self):
        self.scanner = EnhancedRejectionBasedScanner()
        self.exchange = None
        self.db = None
        self.scan_cycle = 0
        self.signals_collected = 0
        self.max_signals = 10000  # Stop after collecting this many
    
    async def initialize(self):
        """Initialize the scanner"""
        log.info("=" * 70)
        log.info("📊 REJECTION-BASED DATA COLLECTION SCANNER")
        log.info("=" * 70)
        log.info("MODE: DATA COLLECTION - All filters are scoring bonuses")
        log.info("PURPOSE: Collect data to analyze what actually works")
        log.info("NO FILTERING: All signals recorded regardless of quality")
        log.info("SCORING: Each filter contributes to total score (0-100)")
        log.info("DATA QUALITY: GOOD (70+), MEDIUM (50-70), POOR (<50)")
        log.info(f"MAX SIGNALS: {self.max_signals}")
        log.info("=" * 70)
        
        await self._init_database()
        await self._init_exchange()
        await self._send_startup_message()
    
    async def _init_database(self):
        """Initialize database"""
        try:
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            self.db = await aiosqlite.connect(DB_PATH)
            
            # Enhanced data collection table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS rejection_data_collection (
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
                
                -- DATA COLLECTION FIELDS
                filter_scores TEXT NOT NULL,
                total_score REAL NOT NULL,
                passed_filters TEXT NOT NULL,
                failed_filters TEXT NOT NULL,
                data_quality TEXT NOT NULL,
                
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
            
            # Data analysis table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS data_analysis (
                date DATE PRIMARY KEY,
                total_signals INTEGER,
                high_score_signals INTEGER,
                medium_score_signals INTEGER,
                low_score_signals INTEGER,
                avg_total_score REAL,
                filter_stats TEXT,
                correlation_stats TEXT
            )
            """)
            
            await self.db.commit()
            log.info("✅ Data collection database initialized")
            
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
        """Send startup message to Telegram"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning("⚠️ Telegram credentials not set. Notifications disabled.")
            return
        
        try:
            message = f"""
📊 <b>DATA COLLECTION SCANNER STARTED</b>

<b>🎯 MODE:</b> Data Collection & Analysis
<b>🧠 APPROACH:</b> Scientific - Collect ALL data, analyze patterns later

<b>📈 DATA COLLECTION:</b>
• All filters are SCORING BONUSES, not requirements
• Recording EVERYTHING - good, medium, and poor signals
• Scoring each signal (0-100 scale)
• Data quality: GOOD (70+), MEDIUM (50-70), POOR (<50)

<b>🔍 9 FILTERS SCORED:</b>
1. Market Strength (0-1)
2. Rejection Zone (0-1)  
3. Volume Confirmation (0-1)
4. Candle Patterns (0-1)
5. Multi-TF Convergence (0-1)
6. RSI Position (0-1)
7. Risk/Reward (0-1)
8. Rejection Strength (0-1)
9. Pattern Confirmation (0-1)

<b>📊 OUTPUT:</b>
Complete database for analysis
Can later determine which filters actually matter

<b>⏱️ FREQUENCY:</b>
Scanning every {SCAN_INTERVAL} seconds
Collecting up to {self.max_signals} signals

#DataCollection #RejectionAnalysis #PatternResearch
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info("✅ Data collection startup message sent")
                
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
    
    async def save_data_signal(self, signal: RejectionSignal) -> bool:
        """Save data signal to database"""
        try:
            # Prepare data
            strength_flags = []
            if signal.market_strength.is_continuation:
                strength_flags.append("CONTINUATION")
            if signal.market_strength.is_rejection_setup:
                strength_flags.append("REJECTION_SETUP")
            if signal.market_strength.is_absorption:
                strength_flags.append("ABSORPTION")
            if signal.market_strength.is_compression:
                strength_flags.append("COMPRESSION")
            
            candle_patterns_list = []
            if signal.candle_patterns:
                for pattern in signal.candle_patterns:
                    candle_patterns_list.append(pattern.to_dict())
            
            dominant_pattern_info = None
            if signal.dominant_pattern:
                dominant_pattern_info = signal.dominant_pattern.to_dict()
            
            serialized_volume_profile = {}
            if signal.volume_profile:
                for key, value in signal.volume_profile.items():
                    serialized_volume_profile[str(key)] = float(value)
            
            serialized_volume_clusters = [float(v) for v in signal.volume_clusters]
            
            # FIXED: Database insert with correct number of columns (46 columns, 46 question marks)
            await self.db.execute("""
                INSERT INTO rejection_data_collection (
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
                    conditions_met,
                    filter_scores, total_score, passed_filters, failed_filters, data_quality,
                    status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                         ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.side,
                float(signal.entry_price),
                float(signal.stop_loss),
                float(signal.take_profit),
                
                str(signal.wave_context.wave_length),
                float(signal.wave_context.wave_maturity),
                float(signal.wave_context.expansion_speed),
                str(signal.wave_context.structure_type),
                str(signal.wave_context.context_side),
                
                float(signal.market_strength.candle_speed),
                float(signal.market_strength.distance_ratio),
                float(signal.market_strength.ema_angle),
                float(signal.market_strength.volume_participation),
                float(signal.market_strength.strength_score),
                json.dumps(strength_flags),
                
                str(signal.rejection_zone.zone_type),
                float(signal.rejection_zone.price_level),
                float(signal.rejection_zone.strength),
                float(signal.rejection_strength),
                float(signal.rsi_at_entry),
                str(signal.rejection_type),
                str(signal.trigger_candle),
                
                json.dumps(candle_patterns_list),
                json.dumps(dominant_pattern_info),
                
                json.dumps(signal.indicators_1h.to_dict()),
                json.dumps(signal.indicators_15m.to_dict()),
                json.dumps(signal.indicators_5m.to_dict()),
                json.dumps(signal.indicators_3m.to_dict()),
                json.dumps(signal.indicators_1m.to_dict()),
                
                json.dumps(serialized_volume_profile),
                json.dumps(serialized_volume_clusters),
                
                json.dumps(signal.multi_tf_confirmation),
                float(signal.convergence_score),
                
                float(signal.risk_reward),
                float(signal.expected_move_pct),
                str(signal.timeframe_used),
                
                json.dumps(signal.conditions_met),
                
                json.dumps(signal.filter_scores),
                float(signal.total_score),
                json.dumps(signal.passed_filters),
                json.dumps(signal.failed_filters),
                str(signal.data_quality),
                
                "PENDING",  # status
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # created_at
            ))
            
            await self.db.commit()
            
            self.signals_collected += 1
            log.info(f"✅ Data collected: {signal.symbol} (Score: {signal.total_score:.1f}, Quality: {signal.data_quality})")
            log.info(f"   Total collected: {self.signals_collected}/{self.max_signals}")
            
            return True
            
        except Exception as e:
            log.error(f"Error saving data signal: {e}")
            import traceback
            log.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    async def send_signal_to_telegram(self, signal: RejectionSignal) -> bool:
        """Send EVERY signal to Telegram"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.debug("Telegram credentials not set. Skipping signal notification.")
            return False
        
        try:
            # Get top 3 filter scores
            top_scores = sorted(
                [(name, score) for name, score in signal.filter_scores.items()],
                key=lambda x: x[1],
                reverse=True
            )[:3]
            
            top_scores_text = ", ".join([f"{name}: {score:.2f}" for name, score in top_scores])
            
            emoji = "✅" if signal.data_quality == "GOOD" else "⚠️" if signal.data_quality == "MEDIUM" else "❌"
            
            message = f"""
{emoji} <b>DATA SIGNAL COLLECTED</b>

<b>🎯 {signal.symbol}</b> | {signal.side}
<b>💰 Entry:</b> {signal.entry_price:.4f}
<b>🛡️ Stop Loss:</b> {signal.stop_loss:.4f}
<b>🎯 Take Profit:</b> {signal.take_profit:.4f}

<b>📊 Score:</b> {signal.total_score:.1f}/100
<b>📈 Quality:</b> {signal.data_quality}
<b>📈 R:R:</b> {signal.risk_reward:.2f}:1

<b>🔍 Rejection Zone:</b> {signal.rejection_zone.zone_type}
<b>📉 RSI:</b> {signal.rsi_at_entry:.1f}
<b>🎯 Type:</b> {signal.rejection_type}

<b>✅ Filters Passed:</b> {len(signal.passed_filters)}/9
<b>🚫 Filters Failed:</b> {len(signal.failed_filters)}/9

<b>🏆 Top Scores:</b>
{top_scores_text}

<b>📈 Conditions:</b>
{', '.join(signal.conditions_met[:5])}{'...' if len(signal.conditions_met) > 5 else ''}

<b>⏰ Collected at:</b> {datetime.fromtimestamp(signal.signal_timestamp).strftime('%H:%M:%S')}
<b>#DataCollection</b> #{signal.data_quality} #{signal.side}
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_notification": signal.data_quality == "POOR"
                })
                
                if response.status_code == 200:
                    log.info(f"📤 Signal sent to Telegram: {signal.symbol}")
                    return True
                else:
                    log.warning(f"Telegram response: {response.status_code}")
                    return False
                
        except Exception as e:
            log.error(f"Telegram signal error: {e}")
            return False
    
    async def send_position_update_to_telegram(self, signal_id: str, symbol: str, side: str, 
                                             entry: float, sl: float, tp: float, 
                                             current_price: float, status: str,
                                             total_score: float, data_quality: str,
                                             pnl_percent: float = 0.0, close_reason: str = None):
        """Send position update to Telegram (TRIGGERED, TP_HIT, SL_HIT)"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.debug("Telegram credentials not set. Skipping position update.")
            return False
        
        try:
            if status == "TRIGGERED":
                pnl_emoji = "🎯"
                title = "POSITION TRIGGERED"
                status_text = "Entry hit - position active"
                notification = False
            elif status == "CLOSED":
                if close_reason == "TP_HIT":
                    pnl_emoji = "💰" if pnl_percent > 0 else "📉"
                    title = "TAKE PROFIT HIT!"
                    status_text = f"Target reached: {pnl_percent:+.2f}%"
                    notification = True
                elif close_reason == "SL_HIT":
                    pnl_emoji = "🛑" if pnl_percent < 0 else "📉"
                    title = "STOP LOSS HIT!"
                    status_text = f"Stop loss triggered: {pnl_percent:+.2f}%"
                    notification = True
                else:
                    pnl_emoji = "📊"
                    title = "POSITION CLOSED"
                    status_text = f"Closed: {pnl_percent:+.2f}%"
                    notification = False
            else:
                return False
            
            quality_emoji = "✅" if data_quality == "GOOD" else "⚠️" if data_quality == "MEDIUM" else "❌"
            
            message = f"""
{pnl_emoji} <b>{title}</b>

<b>📈 {symbol}</b> | {side} | {quality_emoji} {data_quality}
<b>💰 Entry:</b> {entry:.4f}
<b>📊 Current:</b> {current_price:.4f}
<b>📈 Score:</b> {total_score:.1f}/100

<b>🎯 Status:</b> {status_text}

<b>🛡️ Stop Loss:</b> {sl:.4f}
<b>🎯 Take Profit:</b> {tp:.4f}

<b>📊 PnL:</b> {pnl_percent:+.2f}%
<b>📍 Distance to SL:</b> {abs(current_price - sl) / entry * 100:.2f}%
<b>📍 Distance to TP:</b> {abs(tp - current_price) / entry * 100:.2f}%

<b>⏰ Time:</b> {datetime.now().strftime('%H:%M:%S')}
<b>#{status}</b> #{'Profit' if pnl_percent > 0 else 'Loss'} #{data_quality}
"""
            
            # Add retry logic for Telegram
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                    async with httpx.AsyncClient(timeout=10) as client:
                        response = await client.post(url, json={
                            "chat_id": TELEGRAM_CHAT_ID,
                            "text": message,
                            "parse_mode": "HTML",
                            "disable_notification": not notification
                        })
                        
                        if response.status_code == 200:
                            log.info(f"📤 Position update sent to Telegram: {symbol} {status}")
                            return True
                        else:
                            log.warning(f"Telegram response {attempt+1}/{max_retries}: {response.status_code}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(1)
                            
                except Exception as e:
                    log.error(f"Telegram send attempt {attempt+1}/{max_retries} failed: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(1)
            
            return False
                
        except Exception as e:
            log.error(f"Telegram position update error: {e}")
            return False
    
    async def send_data_collection_update(self):
        """Send periodic update to Telegram"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            stats = self.scanner.get_daily_stats()
            completion_pct = (self.signals_collected / self.max_signals) * 100
            
            message = f"""
📈 <b>DATA COLLECTION UPDATE</b>

<b>Progress:</b> {self.signals_collected}/{self.max_signals} ({completion_pct:.1f}%)

<b>📊 Collection Stats:</b>
• Total Signals: {stats['rejections_found']}
• High Score (70+): {stats['high_score_signals']}
• Medium Score (50-70): {stats['medium_score_signals']}
• Low Score (<50): {stats['low_score_signals']}
• Long Signals: {stats['long_rejections']}
• Short Signals: {stats['short_rejections']}

<b>🎯 Current Cycle:</b> #{self.scan_cycle}
<b>⏱️ Scan Interval:</b> {SCAN_INTERVAL}s

<b>📝 Notes:</b>
Collecting ALL data points
No filtering - only scoring
Will analyze patterns after collection

#DataCollection #ProgressUpdate #{'AlmostDone' if completion_pct > 80 else 'Collecting'}
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info(f"📤 Data collection update sent: {self.signals_collected}/{self.max_signals}")
                
        except Exception as e:
            log.error(f"Telegram update error: {e}")
    
    async def monitor_positions(self):
        """Monitor positions for data collection - WITH TELEGRAM NOTIFICATIONS"""
        log.info("👀 Starting data collection monitoring with Telegram notifications...")
        
        while True:
            try:
                if self.signals_collected >= self.max_signals:
                    log.info(f"✅ Reached max signals ({self.max_signals}), stopping monitoring")
                    break
                
                async with self.db.execute("""
                    SELECT id, symbol, side, entry_price, stop_loss, take_profit, status, total_score, data_quality
                    FROM rejection_data_collection 
                    WHERE status IN ('PENDING', 'TRIGGERED')
                    LIMIT 10
                """) as cursor:
                    positions = await cursor.fetchall()
                
                for pos_id, symbol, side, entry, sl, tp, status, total_score, data_quality in positions:
                    try:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = float(ticker['last'])
                        
                        if status == 'PENDING':
                            if True:  # Simplest, always true
                                await self.db.execute("""
                                    UPDATE rejection_data_collection SET 
                                        status = 'TRIGGERED',
                                        triggered_at = CURRENT_TIMESTAMP,
                                        trigger_price = ?
                                    WHERE id = ?
                                """, (current_price, pos_id))
                                
                                await self.db.commit()
                                self.scanner.deduplicator.update_signal_status(pos_id, "TRIGGERED")
                                log.info(f"✅ Data position triggered: {symbol} @ {current_price:.4f}")
                                
                                # Send Telegram notification for trigger
                                await self.send_position_update_to_telegram(
                                    pos_id, symbol, side, entry, sl, tp, 
                                    current_price, "TRIGGERED", total_score, data_quality
                                )
                                continue
                        
                        pnl_percent = 0.0
                        close_reason = None
                        
                        # FIXED: Correct PnL calculation for both LONG and SHORT positions
                        if side == "LONG":
                            if current_price <= sl:
                                close_reason = "SL_HIT"
                                pnl_percent = ((current_price - entry) / entry) * 100
                                log.info(f"📊 {symbol} LONG SL HIT: {current_price:.4f} <= {sl:.4f}, PnL: {pnl_percent:.2f}%")
                            elif current_price >= tp:
                                close_reason = "TP_HIT"
                                pnl_percent = ((current_price - entry) / entry) * 100
                                log.info(f"📊 {symbol} LONG TP HIT: {current_price:.4f} >= {tp:.4f}, PnL: {pnl_percent:.2f}%")
                        
                        else:  # SHORT position - FIXED CALCULATION
                            if current_price >= sl:
                                close_reason = "SL_HIT"
                                # For short: loss when price goes up (current > entry)
                                pnl_percent = ((entry - current_price) / entry) * 100
                                log.info(f"📊 {symbol} SHORT SL HIT: {current_price:.4f} >= {sl:.4f}, PnL: {pnl_percent:.2f}%")
                            elif current_price <= tp:
                                close_reason = "TP_HIT"
                                # For short: profit when price goes down (current < entry)
                                pnl_percent = ((entry - current_price) / entry) * 100
                                log.info(f"📊 {symbol} SHORT TP HIT: {current_price:.4f} <= {tp:.4f}, PnL: {pnl_percent:.2f}%")
                        
                        if close_reason:
                            # Update database first
                            await self.db.execute("""
                                UPDATE rejection_data_collection SET 
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
                            
                            log.info(f"📊 Data position closed: {symbol} {close_reason} ({pnl_percent:+.2f}%)")
                            
                            # FIXED: Send Telegram notification for closure
                            await self.send_position_update_to_telegram(
                                pos_id, symbol, side, entry, sl, tp, 
                                current_price, "CLOSED", total_score, data_quality,
                                pnl_percent, close_reason
                            )
                    
                    except Exception as e:
                        log.error(f"Monitor error for {symbol}: {e}")
                        continue
                
                # Clean up old signals every 5 minutes
                if int(time.time()) % 300 < 2:
                    self.scanner.deduplicator.remove_closed_signals()
                
                await asyncio.sleep(2)
                
            except Exception as e:
                log.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(5)
    
    async def high_freq_data_collection(self):
        """Main data collection loop"""
        log.info("🚀 Starting high-frequency data collection...")
        
        while True:
            try:
                if self.signals_collected >= self.max_signals:
                    log.info(f"✅ Reached max signals ({self.max_signals}), stopping collection")
                    await self.send_final_stats()
                    break
                
                self.scan_cycle += 1
                start_time = time.time()
                
                log.info(f"📊 Data collection cycle #{self.scan_cycle} ({self.signals_collected}/{self.max_signals})")
                
                pairs = await self.get_active_pairs()
                
                if not pairs:
                    log.warning("No active pairs found")
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                log.info(f"Scanning {len(pairs)} pairs for data collection")
                
                signals_found = 0
                pairs_processed = 0
                
                for symbol, volume in pairs:
                    try:
                        multi_tf_data = await self.fetch_timeframe_data(symbol)
                        
                        required_tfs = ["1H", "15M", "3M"]
                        has_all_data = all(tf in multi_tf_data for tf in required_tfs)
                        
                        if not has_all_data:
                            continue
                        
                        signal = self.scanner.generate_enhanced_rejection_signal(multi_tf_data, symbol)
                        
                        if signal:
                            saved = await self.save_data_signal(signal)
                            if saved:
                                # Send to Telegram - EVERY SIGNAL
                                await self.send_signal_to_telegram(signal)
                                signals_found += 1
                        
                        pairs_processed += 1
                        await asyncio.sleep(0.01)
                        
                    except Exception as e:
                        log.debug(f"Pair error {symbol}: {str(e)[:50]}")
                        continue
                
                self.scanner.daily_stats["pairs_scanned"] += pairs_processed
                self.scanner.daily_stats["signals_collected"] += signals_found
                
                stats = self.scanner.get_daily_stats()
                log.info(f"📈 Collection stats: Found {signals_found}, Total: {self.signals_collected}/{self.max_signals}")
                log.info(f"   Score distribution: High {stats['high_score_signals']}, Medium {stats['medium_score_signals']}, Low {stats['low_score_signals']}")
                
                scan_duration = time.time() - start_time
                log.info(f"Data collection #{self.scan_cycle}: {signals_found} signals in {scan_duration:.2f}s")
                
                # Send update every 50 cycles or every 100 signals
                if self.scan_cycle % 50 == 0 or self.signals_collected % 100 == 0:
                    await self.send_data_collection_update()
                
                wait_time = max(0.1, SCAN_INTERVAL - scan_duration)
                log.info(f"Next data collection in {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                log.error(f"Data collection loop error: {e}")
                await asyncio.sleep(10)
    
    async def run(self):
        """Run the data collection scanner"""
        try:
            await self.initialize()
            
            await asyncio.gather(
                self.high_freq_data_collection(),
                self.monitor_positions()
            )
            
        except KeyboardInterrupt:
            log.info("Data collection stopped by user")
            await self.send_final_stats()
            
        except Exception as e:
            log.error(f"Data collection crashed: {e}")
            
        finally:
            await self.cleanup()
    
    async def send_final_stats(self):
        """Send final statistics"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning("⚠️ Telegram credentials missing. Skipping final stats.")
            return
        
        try:
            stats = self.scanner.get_daily_stats()
            completion_pct = (self.signals_collected / self.max_signals) * 100
            
            # Calculate average scores
            async with self.db.execute("""
                SELECT total_score, data_quality FROM rejection_data_collection
            """) as cursor:
                rows = await cursor.fetchall()
                total_scores = [row[0] for row in rows]
                data_qualities = [row[1] for row in rows]
            
            avg_score = np.mean(total_scores) if total_scores else 0
            good_count = data_qualities.count("GOOD")
            medium_count = data_qualities.count("MEDIUM")
            poor_count = data_qualities.count("POOR")
            
            message = f"""
✅ <b>DATA COLLECTION COMPLETED</b>

<b>📊 Final Statistics:</b>
• Total Signals Collected: {self.signals_collected}
• Completion: {completion_pct:.1f}%
• Average Score: {avg_score:.1f}/100
• Data Quality Distribution:
  - GOOD (70+): {good_count} signals
  - MEDIUM (50-70): {medium_count} signals  
  - POOR (<50): {poor_count} signals

<b>📈 Collection Details:</b>
• Scan Cycles: {self.scan_cycle}
• Pairs Scanned: {stats['pairs_scanned']}
• Long Signals: {stats['long_rejections']}
• Short Signals: {stats['short_rejections']}

<b>🔍 Next Steps:</b>
1. Analyze database to find patterns
2. Determine which filters actually matter
3. Calculate success rates for each filter
4. Optimize trading system based on data

<b>📁 Database Location:</b>
{DB_PATH}

<b>📚 Data Analysis:</b>
You can now:
• Query the database for patterns
• Calculate correlation between filters and success
• Find optimal filter combinations
• Build data-driven trading rules

#DataCollectionComplete #AnalysisReady #{'FullDataset' if completion_pct > 95 else 'PartialDataset'}
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info("✅ Final data collection stats sent to Telegram")
                
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
                response = json.dumps({
                    "status": "data_collection",
                    "scanner": "Rejection-Based Data Collection Scanner",
                    "scan_cycle": scanner.scan_cycle,
                    "signals_collected": scanner.signals_collected,
                    "max_signals": scanner.max_signals,
                    "completion_percent": (scanner.signals_collected / scanner.max_signals) * 100,
                    "daily_stats": stats,
                    "mode": "ALL filters are scoring bonuses, no filtering",
                    "scoring": "0-100 scale, data quality: GOOD/MEDIUM/POOR"
                }, indent=2)
            
            elif path == '/stats':
                response = json.dumps(scanner.scanner.get_daily_stats(), indent=2)
            
            elif path == '/recent':
                if scanner.db:
                    scanner.db.row_factory = aiosqlite.Row
                    async with scanner.db.execute("""
                        SELECT symbol, side, entry_price, zone_type, total_score, 
                               data_quality, passed_filters, created_at
                        FROM rejection_data_collection 
                        ORDER BY created_at DESC 
                        LIMIT 20
                    """) as cursor:
                        rows = await cursor.fetchall()
                        signals = [dict(row) for row in rows]
                    
                    response = json.dumps({"signals": signals, "count": len(signals)}, indent=2)
                else:
                    response = json.dumps({"error": "Database not available"})
            
            elif path == '/scores':
                if scanner.db:
                    scanner.db.row_factory = aiosqlite.Row
                    async with scanner.db.execute("""
                        SELECT total_score, data_quality, COUNT(*) as count
                        FROM rejection_data_collection 
                        GROUP BY data_quality
                    """) as cursor:
                        rows = await cursor.fetchall()
                        score_dist = [dict(row) for row in rows]
                    
                    response = json.dumps({"score_distribution": score_dist}, indent=2)
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

# ================ ENTRY POINT ================
async def main():
    """Main function to run the data collection scanner"""
    log.info("=" * 70)
    log.info("🚀 STARTING DATA COLLECTION SCANNER")
    log.info("=" * 70)
    
    scanner = CompleteRejectionScanner()
    
    try:
        http_task = asyncio.create_task(start_http_server(scanner, port=8080))
        await asyncio.sleep(1)
        await scanner.run()
        
    except KeyboardInterrupt:
        log.info("Received interrupt, shutting down...")
    finally:
        if 'http_task' in locals():
            http_task.cancel()
            try:
                await http_task
            except asyncio.CancelledError:
                pass

if __name__ == "__main__":
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("⚠️ Telegram credentials not set. Notifications will not be sent.")
        log.warning("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables.")
    
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Scanner stopped by user")
    except Exception as e:
        log.error(f"Fatal error: {e}")