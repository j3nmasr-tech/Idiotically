#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 HIERARCHICAL REJECTION-BASED TRADING SCANNER
Professional trader-like decision making with logical TP/SL
TRADER MINDSET: Sequential confirmation hierarchy
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
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import json

# ================ HIGH-FREQUENCY CONFIG ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/rejection_signals.db"

# Ultra high-frequency scanning
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 30))
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 100))
MIN_VOLUME_USD = 500000

# Trading parameters - NOT RANDOM ANYMORE
MAX_STOP_LOSS_PCT = 1.5  # Based on volatility
MIN_TARGET_PCT = 2.0     # Based on structure
MAX_TARGET_PCT = 5.0     # Based on wave extension
MIN_RISK_REWARD = 2.0

# Rejection scanning - Based on trader's typical zones
REJECTION_CONFIG = {
    "rsi_long_zone": (38, 48),      # Slightly more conservative
    "rsi_short_zone": (52, 62),     # Slightly more conservative
    "ema_distance_threshold": 0.3,  # Tighter for better accuracy
    "min_rejection_strength": 0.75, # Higher minimum
    "min_convergence_score": 0.8,   # Higher convergence required
}

# Timeframes for analysis - Added 4H for better context
TIMEFRAMES = {
    "4H": "4h",
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
    wave_position: str
    should_trade: bool
    
    def to_dict(self) -> Dict:
        return {
            "wave_length": str(self.wave_length),
            "wave_maturity": float(self.wave_maturity),
            "expansion_speed": float(self.expansion_speed),
            "structure_type": str(self.structure_type),
            "context_side": str(self.context_side),
            "wave_position": str(self.wave_position),
            "should_trade": bool(self.should_trade)
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
    volatility_regime: str
    
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
            "is_compression": bool(self.is_compression),
            "volatility_regime": str(self.volatility_regime)
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
    zone_class: str  # PRIMARY, SECONDARY, TERTIARY
    
    def to_dict(self) -> Dict:
        return {
            "zone_type": str(self.zone_type),
            "price_level": float(self.price_level),
            "strength": float(self.strength),
            "volume_confirmation": bool(self.volume_confirmation),
            "rsi_position": str(self.rsi_position),
            "is_active": bool(self.is_active),
            "zone_class": str(self.zone_class)
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
    """Rejection-based trade signal"""
    signal_id: str
    symbol: str
    side: str
    
    # Price levels - CALCULATED NOT RANDOM
    entry_price: float
    stop_loss: float
    take_profit: float
    stop_loss_pct: float
    take_profit_pct: float
    stop_loss_reason: str
    take_profit_reason: str
    
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
    volatility_at_entry: float
    
    # Timing
    timeframe_used: str
    signal_timestamp: float
    conditions_met: List[str]
    
    # Enhanced analysis
    candle_patterns: List[CandlePattern]
    dominant_pattern: Optional[CandlePattern]
    
    # Indicator analysis per timeframe
    indicators_4h: IndicatorAnalysis
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
    
    # Filter scores
    filter_scores: Dict[str, float]
    total_score: float
    passed_filters: List[str]
    failed_filters: List[str]
    
    # Hierarchical decision tracking
    decision_gates_passed: List[str]
    decision_gates_failed: List[str]

# ================ PROFESSIONAL LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("hierarchical_scanner")

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

# ================ HIERARCHICAL REJECTION ENGINE ================
class HierarchicalRejectionScanner:
    """Trader-like hierarchical decision making"""
    
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
            "signals_generated": 0,
            "high_score_signals": 0,
            "medium_score_signals": 0,
            "low_score_signals": 0,
            "gate1_passed": 0,
            "gate2_passed": 0,
            "gate3_passed": 0,
            "gate4_passed": 0,
            "gate5_passed": 0,
            "gate6_passed": 0
        }
        self.deduplicator = self.SignalDeduplicator()
        self.active_signal_ids = set()
        self.pattern_scanner = CandlePatternScanner()
        self.indicator_analyzer = IndicatorAnalyzer()
    
    # ========== HIERARCHICAL DECISION GATES ==========
    
    def generate_trader_like_signal(self, multi_tf_data: Dict[str, pd.DataFrame], 
                                   symbol: str) -> Optional[RejectionSignal]:
        """
        Trader-like hierarchical decision making
        Each gate must pass in sequence
        """
        log.debug(f"🔍 Analyzing {symbol} with hierarchical gates")
        
        # GATE 1: Check wave context and structure
        wave_context = self._gate1_wave_context(multi_tf_data)
        if not wave_context or not wave_context.should_trade:
            log.debug(f"{symbol}: ❌ GATE 1 FAILED - Wrong wave context")
            return None
        self.daily_stats["gate1_passed"] += 1
        
        # GATE 2: Find single best rejection zone
        best_zone = self._gate2_find_best_zone(multi_tf_data)
        if not best_zone or best_zone.zone_class != "PRIMARY":
            log.debug(f"{symbol}: ❌ GATE 2 FAILED - No primary zone found")
            return None
        self.daily_stats["gate2_passed"] += 1
        
        # GATE 3: Check price is actively rejecting zone
        if not self._gate3_check_rejection_now(multi_tf_data, best_zone):
            log.debug(f"{symbol}: ❌ GATE 3 FAILED - Not rejecting now")
            return None
        self.daily_stats["gate3_passed"] += 1
        
        # GATE 4: Multi-timeframe hierarchical confirmation
        tf_confirmation = self._gate4_tf_confirmation(multi_tf_data, best_zone)
        if tf_confirmation["overall_score"] < 0.8:
            log.debug(f"{symbol}: ❌ GATE 4 FAILED - TF confirmation weak: {tf_confirmation['overall_score']:.2f}")
            return None
        self.daily_stats["gate4_passed"] += 1
        
        # GATE 5: Indicator confluence at zone
        confluence_score = self._gate5_indicator_confluence(multi_tf_data, best_zone)
        if confluence_score < 0.75:
            log.debug(f"{symbol}: ❌ GATE 5 FAILED - Low confluence: {confluence_score:.2f}")
            return None
        self.daily_stats["gate5_passed"] += 1
        
        # GATE 6: Entry quality assessment
        entry_quality = self._gate6_entry_quality(multi_tf_data, best_zone, wave_context.context_side)
        if entry_quality["score"] < 0.7:
            log.debug(f"{symbol}: ❌ GATE 6 FAILED - Entry quality low: {entry_quality['score']:.2f}")
            return None
        self.daily_stats["gate6_passed"] += 1
        
        # All gates passed - create comprehensive signal
        signal = self._create_comprehensive_signal(
            symbol, multi_tf_data, wave_context, best_zone, 
            tf_confirmation, confluence_score, entry_quality
        )
        
        if signal:
            log.info(f"🎯 ALL GATES PASSED for {symbol}")
            log.info(f"   Zone: {best_zone.zone_type} @ {best_zone.price_level:.8f}")
            log.info(f"   Wave: {wave_context.wave_length}, Context: {wave_context.context_side}")
            log.info(f"   Score: {signal.total_score:.1f}/100")
        
        return signal
    
    # ========== GATE IMPLEMENTATIONS ==========
    
    def _gate1_wave_context(self, multi_tf_data: Dict) -> Optional[WaveContext]:
        """Gate 1: Analyze wave context - should we even look for trades?"""
        df_4h = multi_tf_data.get("4H")
        df_1h = multi_tf_data.get("1H")
        df_15m = multi_tf_data.get("15M")
        
        if not df_4h or not df_1h or not df_15m:
            return None
        
        if len(df_4h) < 50 or len(df_1h) < 100 or len(df_15m) < 50:
            return None
        
        # Analyze higher timeframe trend
        higher_tf_trend = self._get_trend_direction(df_4h)
        
        # Analyze medium timeframe structure
        structure_type = self._analyze_market_structure(df_1h)
        
        # Analyze current wave
        wave_length, wave_maturity = self._analyze_wave_characteristics(df_15m, df_1h)
        
        # Determine context side
        context_side = self._determine_context_side(df_1h, df_15m)
        
        # Wave position
        wave_position = self._get_wave_position(df_15m, df_1h, df_4h)
        
        # Should we trade in this context?
        should_trade = self._should_trade_in_context(
            higher_tf_trend, structure_type, wave_maturity, context_side
        )
        
        expansion_speed = self._calculate_expansion_speed(df_15m)
        
        return WaveContext(
            wave_length=wave_length,
            wave_maturity=wave_maturity,
            expansion_speed=expansion_speed,
            structure_type=structure_type,
            context_side=context_side,
            wave_position=wave_position,
            should_trade=should_trade
        )
    
    def _gate2_find_best_zone(self, multi_tf_data: Dict) -> Optional[RejectionZone]:
        """Gate 2: Find the SINGLE best rejection zone"""
        zones = []
        
        # Check zones in order of importance
        ema_zone = self._find_ema_rejection_zone(multi_tf_data)
        if ema_zone:
            zones.append(ema_zone)
        
        structure_zone = self._find_structure_zone(multi_tf_data)
        if structure_zone:
            zones.append(structure_zone)
        
        volume_zone = self._find_volume_zone(multi_tf_data)
        if volume_zone:
            zones.append(volume_zone)
        
        fib_zone = self._find_fib_zone(multi_tf_data)
        if fib_zone:
            zones.append(fib_zone)
        
        if not zones:
            return None
        
        # Filter and classify zones
        valid_zones = []
        for zone in zones:
            # Classify zone based on strength
            if zone.strength >= 0.9:
                zone.zone_class = "PRIMARY"
                valid_zones.append(zone)
            elif zone.strength >= 0.7:
                zone.zone_class = "SECONDARY"
            else:
                zone.zone_class = "TERTIARY"
        
        # Return strongest PRIMARY zone
        primary_zones = [z for z in valid_zones if z.zone_class == "PRIMARY"]
        if primary_zones:
            return max(primary_zones, key=lambda z: z.strength)
        
        return None
    
    def _gate3_check_rejection_now(self, multi_tf_data: Dict, zone: RejectionZone) -> bool:
        """Gate 3: Is price actively rejecting the zone RIGHT NOW?"""
        df_3m = multi_tf_data.get("3M")
        df_1m = multi_tf_data.get("1M")
        
        if not df_3m or len(df_3m) < 10:
            return False
        
        current_candle = df_3m.iloc[-1]
        prev_candle = df_3m.iloc[-2]
        
        # Check for wick rejection
        has_wick_rejection = False
        if zone.zone_type in ["EMA_SUPPORT", "DEMAND_ZONE", "FIB_SUPPORT"]:
            if (current_candle['low'] < zone.price_level * 1.001 and 
                current_candle['close'] > zone.price_level):
                has_wick_rejection = True
        
        elif zone.zone_type in ["EMA_RESISTANCE", "SUPPLY_ZONE", "FIB_RESISTANCE"]:
            if (current_candle['high'] > zone.price_level * 0.999 and 
                current_candle['close'] < zone.price_level):
                has_wick_rejection = True
        
        # Check for momentum shift
        has_momentum_shift = False
        if zone.zone_type in ["EMA_SUPPORT", "DEMAND_ZONE", "FIB_SUPPORT"]:
            if (prev_candle['close'] < prev_candle['open'] and
                current_candle['close'] > current_candle['open'] and
                abs(current_candle['close'] - zone.price_level) / zone.price_level < 0.002):
                has_momentum_shift = True
        
        elif zone.zone_type in ["EMA_RESISTANCE", "SUPPLY_ZONE", "FIB_RESISTANCE"]:
            if (prev_candle['close'] > prev_candle['open'] and
                current_candle['close'] < current_candle['open'] and
                abs(current_candle['close'] - zone.price_level) / zone.price_level < 0.002):
                has_momentum_shift = True
        
        return has_wick_rejection or has_momentum_shift
    
    def _gate4_tf_confirmation(self, multi_tf_data: Dict, zone: RejectionZone) -> Dict:
        """Gate 4: Hierarchical timeframe confirmation"""
        confirmation = {
            "higher_tf": False,      # 4H/1H MUST confirm
            "medium_tf": False,      # 15M SHOULD confirm
            "trigger_tf": False,     # 5M/3M entry trigger
            "overall_score": 0.0
        }
        
        # Check 4H/1H MUST confirm
        df_4h = multi_tf_data.get("4H")
        df_1h = multi_tf_data.get("1H")
        
        if df_4h and df_1h:
            higher_confirm = self._does_higher_tf_confirm(df_4h, df_1h, zone)
            confirmation["higher_tf"] = higher_confirm
        
        # Check 15M SHOULD confirm
        df_15m = multi_tf_data.get("15M")
        if df_15m:
            medium_confirm = self._does_medium_tf_confirm(df_15m, zone)
            confirmation["medium_tf"] = medium_confirm
        
        # Check 5M/3M trigger
        df_5m = multi_tf_data.get("5M")
        df_3m = multi_tf_data.get("3M")
        
        if df_5m and df_3m:
            trigger_confirm = self._does_trigger_tf_confirm(df_5m, df_3m, zone)
            confirmation["trigger_tf"] = trigger_confirm
        
        # Calculate weighted score
        weights = {"higher_tf": 0.5, "medium_tf": 0.3, "trigger_tf": 0.2}
        confirmation["overall_score"] = (
            (confirmation["higher_tf"] * weights["higher_tf"]) +
            (confirmation["medium_tf"] * weights["medium_tf"]) +
            (confirmation["trigger_tf"] * weights["trigger_tf"])
        )
        
        return confirmation
    
    def _gate5_indicator_confluence(self, multi_tf_data: Dict, zone: RejectionZone) -> float:
        """Gate 5: Indicator confluence at the zone"""
        df_15m = multi_tf_data.get("15M")
        df_5m = multi_tf_data.get("5M")
        
        if not df_15m or not df_5m:
            return 0.0
        
        confluence_signals = []
        
        # 1. RSI confirmation
        rsi_value = self.indicator_analyzer.calculate_rsi(df_5m['close']).iloc[-1]
        rsi_confirms = self._does_rsi_confirm_zone(zone, rsi_value)
        confluence_signals.append(1.0 if rsi_confirms else 0.0)
        
        # 2. Volume confirmation
        volume_confirms = self._does_volume_confirm_zone(df_5m, zone)
        confluence_signals.append(1.0 if volume_confirms else 0.0)
        
        # 3. Candle pattern confirmation
        patterns = self.pattern_scanner.detect_patterns(df_5m, "5M")
        pattern_confirms = any(
            self._does_pattern_confirm_zone(p, zone) 
            for p in patterns[:3]  # Check top 3 patterns
        )
        confluence_signals.append(1.0 if pattern_confirms else 0.0)
        
        # 4. Structure confirmation
        structure_confirms = self._does_structure_confirm_zone(df_15m, zone)
        confluence_signals.append(1.0 if structure_confirms else 0.0)
        
        # 5. Trend alignment
        trend_confirms = self._does_trend_align_with_zone(df_15m, zone)
        confluence_signals.append(1.0 if trend_confirms else 0.0)
        
        return float(np.mean(confluence_signals))
    
    def _gate6_entry_quality(self, multi_tf_data: Dict, zone: RejectionZone, context_side: str) -> Dict:
        """Gate 6: Entry quality assessment"""
        df_3m = multi_tf_data.get("3M")
        if not df_3m or len(df_3m) < 10:
            return {"score": 0.0, "reasons": ["Insufficient data"]}
        
        quality_factors = []
        reasons = []
        
        # 1. Volatility assessment
        volatility = self._calculate_volatility(df_3m)
        if volatility < 0.5:  # Low volatility is better for entries
            quality_factors.append(0.9)
            reasons.append("Low volatility")
        elif volatility < 1.0:
            quality_factors.append(0.7)
            reasons.append("Medium volatility")
        else:
            quality_factors.append(0.4)
            reasons.append("High volatility")
        
        # 2. Candlestick quality at entry
        current_candle = df_3m.iloc[-1]
        candle_quality = self._assess_candle_quality(current_candle, context_side)
        quality_factors.append(candle_quality["score"])
        reasons.append(candle_quality["reason"])
        
        # 3. Spread from zone
        current_price = float(df_3m['close'].iloc[-1])
        distance_pct = abs(current_price - zone.price_level) / zone.price_level * 100
        if distance_pct < 0.2:
            quality_factors.append(0.9)
            reasons.append("Close to zone")
        elif distance_pct < 0.5:
            quality_factors.append(0.7)
            reasons.append("Reasonable distance")
        else:
            quality_factors.append(0.4)
            reasons.append("Far from zone")
        
        # 4. Recent price action
        recent_action = self._analyze_recent_price_action(df_3m, context_side)
        quality_factors.append(recent_action["score"])
        reasons.append(recent_action["reason"])
        
        return {
            "score": float(np.mean(quality_factors)),
            "reasons": reasons,
            "volatility": volatility,
            "distance_pct": distance_pct
        }
    
    # ========== SUPPORTING METHODS ==========
    
    def _get_trend_direction(self, df: pd.DataFrame) -> str:
        """Get trend direction"""
        if len(df) < 20:
            return "NEUTRAL"
        
        prices = df['close'].values[-20:]
        x = np.arange(len(prices))
        slope, _ = np.polyfit(x, prices, 1)
        
        if slope > 0.001:
            return "BULLISH"
        elif slope < -0.001:
            return "BEARISH"
        return "NEUTRAL"
    
    def _analyze_market_structure(self, df: pd.DataFrame) -> str:
        """Analyze market structure"""
        if len(df) < 30:
            return "COMPRESSION"
        
        highs = df['high'].values[-30:]
        lows = df['low'].values[-30:]
        closes = df['close'].values[-30:]
        
        # Check for higher highs/higher lows
        higher_highs = highs[-1] > highs[-10] > highs[-20]
        higher_lows = lows[-1] > lows[-10] > lows[-20]
        
        # Check for lower highs/lower lows
        lower_highs = highs[-1] < highs[-10] < highs[-20]
        lower_lows = lows[-1] < lows[-10] < lows[-20]
        
        if higher_highs and higher_lows:
            return "UPTREND"
        elif lower_highs and lower_lows:
            return "DOWNTREND"
        
        # Check for range
        range_high = np.max(highs)
        range_low = np.min(lows)
        range_pct = (range_high - range_low) / range_low * 100
        
        if range_pct < 3:
            return "COMPRESSION"
        else:
            return "RANGING"
    
    def _analyze_wave_characteristics(self, df_15m: pd.DataFrame, df_1h: pd.DataFrame) -> Tuple[str, float]:
        """Analyze wave characteristics"""
        if len(df_15m) < 50 or len(df_1h) < 30:
            return "MEDIUM", 0.5
        
        # Analyze on 15M
        recent_prices_15m = df_15m['close'].values[-30:]
        total_move_15m = abs(recent_prices_15m[-1] - recent_prices_15m[0])
        avg_candle_15m = np.mean(np.abs(np.diff(recent_prices_15m[-10:])))
        
        if avg_candle_15m == 0:
            wave_length = "MEDIUM"
        else:
            move_ratio_15m = total_move_15m / avg_candle_15m
            
            if move_ratio_15m < 15:
                wave_length = "SHORT"
            elif move_ratio_15m < 30:
                wave_length = "MEDIUM"
            else:
                wave_length = "EXTENDED"
        
        # Analyze wave maturity on 1H
        recent_prices_1h = df_1h['close'].values[-20:]
        ma_20_1h = np.mean(recent_prices_1h[-20:])
        current_price_1h = recent_prices_1h[-1]
        volatility_1h = np.std(recent_prices_1h[-20:])
        
        if volatility_1h > 0 and ma_20_1h > 0:
            distance_pct = abs(current_price_1h - ma_20_1h) / ma_20_1h * 100
            volatility_pct = volatility_1h / ma_20_1h * 100
            wave_maturity = min(distance_pct / (volatility_pct * 2), 1.0)
        else:
            wave_maturity = 0.5
        
        return wave_length, float(wave_maturity)
    
    def _determine_context_side(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> str:
        """Determine market context side"""
        if len(df_1h) < 10 or len(df_15m) < 5:
            return "NEUTRAL"
        
        prices_1h = df_1h['close'].values[-10:]
        prices_15m = df_15m['close'].values[-5:]
        
        x_1h = np.arange(len(prices_1h))
        slope_1h, _ = np.polyfit(x_1h, prices_1h, 1)
        
        x_15m = np.arange(len(prices_15m))
        slope_15m, _ = np.polyfit(x_15m, prices_15m, 1)
        
        total_slope = (slope_1h * 0.6 + slope_15m * 0.4)
        
        if total_slope > 0.001:
            return "BULLISH_CONTEXT"
        elif total_slope < -0.001:
            return "BEARISH_CONTEXT"
        return "NEUTRAL"
    
    def _get_wave_position(self, df_15m: pd.DataFrame, df_1h: pd.DataFrame, df_4h: pd.DataFrame) -> str:
        """Get wave position in larger structure"""
        if not df_4h or len(df_4h) < 50:
            return "MIDDLE"
        
        # Check if near structure extremes
        current_price = float(df_15m['close'].iloc[-1])
        
        # Check 4H highs/lows
        recent_high_4h = float(df_4h['high'].values[-20:].max())
        recent_low_4h = float(df_4h['low'].values[-20:].min())
        
        distance_to_high = abs(current_price - recent_high_4h) / recent_high_4h * 100
        distance_to_low = abs(current_price - recent_low_4h) / recent_low_4h * 100
        
        if distance_to_high < 1.0:
            return "NEAR_HIGH"
        elif distance_to_low < 1.0:
            return "NEAR_LOW"
        
        # Check 1H trend
        prices_1h = df_1h['close'].values[-10:]
        slope_1h, _ = np.polyfit(np.arange(len(prices_1h)), prices_1h, 1)
        
        if slope_1h > 0.001:
            return "UPTREND_MIDDLE"
        elif slope_1h < -0.001:
            return "DOWNTREND_MIDDLE"
        
        return "MIDDLE"
    
    def _should_trade_in_context(self, trend: str, structure: str, 
                               wave_maturity: float, context_side: str) -> bool:
        """Determine if we should trade in this context"""
        # Don't trade if wave is too mature
        if wave_maturity > 0.9:
            return False
        
        # Don't trade if market is in compression
        if structure == "COMPRESSION":
            return False
        
        # Prefer trending or ranging markets
        if structure not in ["UPTREND", "DOWNTREND", "RANGING"]:
            return False
        
        # Context should be clear
        if context_side == "NEUTRAL" and structure == "RANGING":
            return True  # Okay for ranging
        
        if context_side == "NEUTRAL":
            return False  # Not okay for trends
        
        return True
    
    def _calculate_expansion_speed(self, df: pd.DataFrame) -> float:
        """Calculate expansion speed"""
        if len(df) < 10:
            return 0.5
        
        candles = df.iloc[-10:]
        candle_speeds = []
        
        for _, candle in candles.iterrows():
            candle_range = candle['high'] - candle['low']
            if candle['close'] > 0:
                speed = candle_range / candle['close'] * 100
                candle_speeds.append(speed)
        
        if not candle_speeds:
            return 0.5
        
        avg_speed = np.mean(candle_speeds)
        return float(min(avg_speed / 5.0, 1.0))
    
    # ========== ZONE FINDING METHODS ==========
    
    def _find_ema_rejection_zone(self, multi_tf_data: Dict) -> Optional[RejectionZone]:
        """Find EMA rejection zone"""
        df_3m = multi_tf_data.get("3M")
        df_15m = multi_tf_data.get("15M")
        
        if not df_3m or not df_15m:
            return None
        
        current_price_3m = float(df_3m['close'].iloc[-1])
        current_price_15m = float(df_15m['close'].iloc[-1])
        
        # Calculate EMAs on 15M for more reliability
        emas = {}
        for name, period in EMA_PERIODS.items():
            ema_series = df_15m['close'].ewm(span=period, adjust=False).mean()
            emas[name] = float(ema_series.iloc[-1]) if len(ema_series) > 0 else 0.0
        
        # Find closest EMA
        closest_ema = None
        min_distance = float('inf')
        
        for name, ema_value in emas.items():
            if ema_value == 0:
                continue
            
            distance_pct = abs(current_price_15m - ema_value) / ema_value * 100
            
            if distance_pct < REJECTION_CONFIG["ema_distance_threshold"]:
                if distance_pct < min_distance:
                    min_distance = distance_pct
                    closest_ema = (name, ema_value, distance_pct)
        
        if not closest_ema:
            return None
        
        name, ema_value, distance_pct = closest_ema
        
        # Determine zone type
        if current_price_15m > ema_value:
            zone_type = "EMA_SUPPORT"
        else:
            zone_type = "EMA_RESISTANCE"
        
        # Calculate strength based on EMA importance and distance
        if name == "very_slow":
            base_strength = 0.95
        elif name == "slow":
            base_strength = 0.85
        elif name == "medium":
            base_strength = 0.75
        else:
            base_strength = 0.65
        
        # Adjust strength based on distance (closer = stronger)
        distance_strength = 1.0 - (distance_pct / REJECTION_CONFIG["ema_distance_threshold"])
        strength = base_strength * distance_strength
        
        # Check volume confirmation
        volume_confirmation = self._check_volume_at_ema(df_15m, ema_value, zone_type)
        
        # Get RSI position
        rsi_value = self.indicator_analyzer.calculate_rsi(df_15m['close']).iloc[-1]
        rsi_position = self._get_rsi_position_for_zone(rsi_value, zone_type)
        
        return RejectionZone(
            zone_type=zone_type,
            price_level=ema_value,
            strength=float(strength),
            volume_confirmation=volume_confirmation,
            rsi_position=rsi_position,
            is_active=True,
            zone_class="PRIMARY" if strength >= 0.9 else "SECONDARY"
        )
    
    def _find_structure_zone(self, multi_tf_data: Dict) -> Optional[RejectionZone]:
        """Find structure-based support/resistance zone"""
        df_1h = multi_tf_data.get("1H")
        df_15m = multi_tf_data.get("15M")
        
        if not df_1h or not df_15m:
            return None
        
        if len(df_1h) < 50 or len(df_15m) < 30:
            return None
        
        current_price = float(df_15m['close'].iloc[-1])
        
        # Find recent swing highs/lows on 1H
        highs = df_1h['high'].values[-50:]
        lows = df_1h['low'].values[-50:]
        
        swing_highs = []
        swing_lows = []
        
        for i in range(2, len(highs)-2):
            if (highs[i] > highs[i-2] and highs[i] > highs[i-1] and 
                highs[i] > highs[i+1] and highs[i] > highs[i+2]):
                swing_highs.append((i, highs[i]))
            
            if (lows[i] < lows[i-2] and lows[i] < lows[i-1] and 
                lows[i] < lows[i+1] and lows[i] < lows[i+2]):
                swing_lows.append((i, lows[i]))
        
        # Find closest structure level
        closest_level = None
        min_distance = float('inf')
        level_type = None
        
        for idx, high in swing_highs[-3:]:  # Last 3 swing highs
            distance_pct = abs(current_price - high) / high * 100
            if distance_pct < 0.5 and distance_pct < min_distance:
                min_distance = distance_pct
                closest_level = high
                level_type = "STRUCTURE_RESISTANCE"
        
        for idx, low in swing_lows[-3:]:  # Last 3 swing lows
            distance_pct = abs(current_price - low) / low * 100
            if distance_pct < 0.5 and distance_pct < min_distance:
                min_distance = distance_pct
                closest_level = low
                level_type = "STRUCTURE_SUPPORT"
        
        if not closest_level:
            return None
        
        # Calculate strength based on touches
        touches = 0
        if level_type == "STRUCTURE_SUPPORT":
            touches = sum((df_1h['low'] <= closest_level * 1.005) & 
                         (df_1h['low'] >= closest_level * 0.995))
        else:
            touches = sum((df_1h['high'] >= closest_level * 0.995) & 
                         (df_1h['high'] <= closest_level * 1.005))
        
        strength = min(touches / 5.0, 1.0) * 0.9
        
        # Check volume
        volume_confirmation = self._check_volume_at_structure(df_15m, closest_level, level_type)
        
        # Get RSI
        rsi_value = self.indicator_analyzer.calculate_rsi(df_15m['close']).iloc[-1]
        rsi_position = self._get_rsi_position_for_zone(rsi_value, level_type)
        
        return RejectionZone(
            zone_type=level_type,
            price_level=closest_level,
            strength=float(strength),
            volume_confirmation=volume_confirmation,
            rsi_position=rsi_position,
            is_active=True,
            zone_class="PRIMARY" if strength >= 0.9 else "SECONDARY"
        )
    
    def _find_volume_zone(self, multi_tf_data: Dict) -> Optional[RejectionZone]:
        """Find volume-based zone"""
        df_15m = multi_tf_data.get("15M")
        if not df_15m or len(df_15m) < 30:
            return None
        
        # Find high volume nodes
        df_sorted = df_15m.sort_values('volume', ascending=False)
        top_volume_candles = df_sorted.head(5)
        
        if len(top_volume_candles) == 0:
            return None
        
        current_price = float(df_15m['close'].iloc[-1])
        
        # Find closest high volume level
        closest_level = None
        min_distance = float('inf')
        level_type = None
        
        for _, candle in top_volume_candles.iterrows():
            level = float((candle['high'] + candle['low'] + candle['close']) / 3)
            distance_pct = abs(current_price - level) / level * 100
            
            if distance_pct < 0.5 and distance_pct < min_distance:
                min_distance = distance_pct
                closest_level = level
                
                if current_price > level:
                    level_type = "VOLUME_SUPPORT"
                else:
                    level_type = "VOLUME_RESISTANCE"
        
        if not closest_level:
            return None
        
        # Strength based on volume ratio
        volume_at_level = 0
        mask = (df_15m['low'] <= closest_level * 1.01) & (df_15m['high'] >= closest_level * 0.99)
        volume_at_level = float(df_15m.loc[mask, 'volume'].sum())
        avg_volume = float(df_15m['volume'].mean())
        
        volume_ratio = volume_at_level / avg_volume if avg_volume > 0 else 1.0
        strength = min(volume_ratio / 3.0, 1.0) * 0.8
        
        # Always true for volume zones
        volume_confirmation = True
        
        # Get RSI
        rsi_value = self.indicator_analyzer.calculate_rsi(df_15m['close']).iloc[-1]
        rsi_position = self._get_rsi_position_for_zone(rsi_value, level_type)
        
        return RejectionZone(
            zone_type=level_type,
            price_level=closest_level,
            strength=float(strength),
            volume_confirmation=volume_confirmation,
            rsi_position=rsi_position,
            is_active=True,
            zone_class="SECONDARY"  # Volume zones are usually secondary
        )
    
    def _find_fib_zone(self, multi_tf_data: Dict) -> Optional[RejectionZone]:
        """Find Fibonacci retracement zone"""
        df_1h = multi_tf_data.get("1H")
        if not df_1h or len(df_1h) < 100:
            return None
        
        # Find last significant swing
        highs = df_1h['high'].values[-100:]
        lows = df_1h['low'].values[-100:]
        
        # Find major swing high and low
        swing_high = float(np.max(highs[-50:]))
        swing_low = float(np.min(lows[-50:]))
        
        if swing_high <= swing_low:
            return None
        
        current_price = float(df_1h['close'].iloc[-1])
        
        # Fibonacci levels
        fib_levels = {
            0.236: swing_low + (swing_high - swing_low) * 0.236,
            0.382: swing_low + (swing_high - swing_low) * 0.382,
            0.500: swing_low + (swing_high - swing_low) * 0.500,
            0.618: swing_low + (swing_high - swing_low) * 0.618,
            0.786: swing_low + (swing_high - swing_low) * 0.786
        }
        
        # Find closest Fib level
        closest_level = None
        closest_fib = None
        min_distance = float('inf')
        
        for fib, level in fib_levels.items():
            distance_pct = abs(current_price - level) / level * 100
            if distance_pct < 0.3 and distance_pct < min_distance:
                min_distance = distance_pct
                closest_level = level
                closest_fib = fib
        
        if not closest_level:
            return None
        
        # Determine zone type based on trend
        trend = self._get_trend_direction(df_1h)
        
        if trend == "BULLISH" and current_price > closest_level:
            zone_type = "FIB_SUPPORT"
        elif trend == "BULLISH":
            zone_type = "FIB_RESISTANCE"
        elif trend == "BEARISH" and current_price < closest_level:
            zone_type = "FIB_RESISTANCE"
        else:
            zone_type = "FIB_SUPPORT"
        
        # Strength based on Fibonacci level importance
        if closest_fib in [0.382, 0.618]:
            base_strength = 0.9
        elif closest_fib == 0.500:
            base_strength = 0.85
        elif closest_fib in [0.236, 0.786]:
            base_strength = 0.7
        else:
            base_strength = 0.6
        
        # Adjust for distance
        distance_strength = 1.0 - (min_distance / 0.3)
        strength = base_strength * distance_strength
        
        # Check volume
        volume_confirmation = self._check_volume_at_level(df_1h, closest_level)
        
        # Get RSI
        rsi_value = self.indicator_analyzer.calculate_rsi(df_1h['close']).iloc[-1]
        rsi_position = self._get_rsi_position_for_zone(rsi_value, zone_type)
        
        return RejectionZone(
            zone_type=zone_type,
            price_level=closest_level,
            strength=float(strength),
            volume_confirmation=volume_confirmation,
            rsi_position=rsi_position,
            is_active=True,
            zone_class="PRIMARY" if strength >= 0.9 else "SECONDARY"
        )
    
    def _check_volume_at_ema(self, df: pd.DataFrame, ema_level: float, zone_type: str) -> bool:
        """Check volume at EMA level"""
        if len(df) < 10:
            return False
        
        recent_candles = df.iloc[-10:]
        
        if zone_type == "EMA_SUPPORT":
            touch_candles = recent_candles[recent_candles['low'] <= ema_level * 1.005]
        else:
            touch_candles = recent_candles[recent_candles['high'] >= ema_level * 0.995]
        
        if len(touch_candles) == 0:
            return False
        
        # Check if volume is above average on touch candles
        avg_volume = float(recent_candles['volume'].mean())
        touch_volume = float(touch_candles['volume'].mean())
        
        return touch_volume > avg_volume * 1.2
    
    def _check_volume_at_structure(self, df: pd.DataFrame, level: float, zone_type: str) -> bool:
        """Check volume at structure level"""
        return self._check_volume_at_level(df, level)
    
    def _check_volume_at_level(self, df: pd.DataFrame, level: float) -> bool:
        """Check volume at specific level"""
        if len(df) < 20:
            return False
        
        # Find candles that touched the level
        touch_mask = (df['low'] <= level * 1.01) & (df['high'] >= level * 0.99)
        touch_candles = df[touch_mask]
        
        if len(touch_candles) < 2:
            return False
        
        # Compare volume on touch vs non-touch
        touch_volume = float(touch_candles['volume'].mean())
        non_touch_volume = float(df[~touch_mask]['volume'].mean()) if len(df[~touch_mask]) > 0 else 0
        
        if non_touch_volume > 0:
            return touch_volume > non_touch_volume * 1.3
        
        return True
    
    def _get_rsi_position_for_zone(self, rsi_value: float, zone_type: str) -> str:
        """Get RSI position for zone type"""
        if "SUPPORT" in zone_type or "DEMAND" in zone_type:
            if REJECTION_CONFIG["rsi_long_zone"][0] <= rsi_value <= REJECTION_CONFIG["rsi_long_zone"][1]:
                return "IN_ZONE"
            elif rsi_value < 30:
                return "OVERSOLD"
            else:
                return "NEUTRAL"
        
        elif "RESISTANCE" in zone_type or "SUPPLY" in zone_type:
            if REJECTION_CONFIG["rsi_short_zone"][0] <= rsi_value <= REJECTION_CONFIG["rsi_short_zone"][1]:
                return "IN_ZONE"
            elif rsi_value > 70:
                return "OVERBOUGHT"
            else:
                return "NEUTRAL"
        
        return "NEUTRAL"
    
    # ========== CONFIRMATION METHODS ==========
    
    def _does_higher_tf_confirm(self, df_4h: pd.DataFrame, df_1h: pd.DataFrame, zone: RejectionZone) -> bool:
        """Check if higher timeframes confirm the zone"""
        # Check 4H trend direction
        trend_4h = self._get_trend_direction(df_4h)
        trend_1h = self._get_trend_direction(df_1h)
        
        # For support zones, prefer bullish or neutral trends
        if "SUPPORT" in zone.zone_type:
            return trend_4h in ["BULLISH", "NEUTRAL"] and trend_1h in ["BULLISH", "NEUTRAL"]
        
        # For resistance zones, prefer bearish or neutral trends
        elif "RESISTANCE" in zone.zone_type:
            return trend_4h in ["BEARISH", "NEUTRAL"] and trend_1h in ["BEARISH", "NEUTRAL"]
        
        return True
    
    def _does_medium_tf_confirm(self, df_15m: pd.DataFrame, zone: RejectionZone) -> bool:
        """Check if medium timeframe confirms"""
        if len(df_15m) < 20:
            return False
        
        current_price = float(df_15m['close'].iloc[-1])
        
        # Check if price is respecting the zone
        if "SUPPORT" in zone.zone_type:
            # For support, price should be above or very close
            return current_price >= zone.price_level * 0.995
        
        elif "RESISTANCE" in zone.zone_type:
            # For resistance, price should be below or very close
            return current_price <= zone.price_level * 1.005
        
        return True
    
    def _does_trigger_tf_confirm(self, df_5m: pd.DataFrame, df_3m: pd.DataFrame, zone: RejectionZone) -> bool:
        """Check if trigger timeframes confirm"""
        if len(df_5m) < 10 or len(df_3m) < 10:
            return False
        
        # Check for rejection patterns on trigger TFs
        patterns_5m = self.pattern_scanner.detect_patterns(df_5m, "5M")
        patterns_3m = self.pattern_scanner.detect_patterns(df_3m, "3M")
        
        # Look for rejection patterns
        for pattern in patterns_5m + patterns_3m:
            if pattern.pattern_type in ["BULLISH_REVERSAL", "BEARISH_REVERSAL"]:
                if "SUPPORT" in zone.zone_type and pattern.pattern_type == "BULLISH_REVERSAL":
                    return True
                elif "RESISTANCE" in zone.zone_type and pattern.pattern_type == "BEARISH_REVERSAL":
                    return True
        
        return False
    
    def _does_rsi_confirm_zone(self, zone: RejectionZone, rsi_value: float) -> bool:
        """Check if RSI confirms the zone"""
        if zone.rsi_position == "IN_ZONE":
            return True
        elif "SUPPORT" in zone.zone_type:
            return rsi_value < 60  # Not overbought
        elif "RESISTANCE" in zone.zone_type:
            return rsi_value > 40  # Not oversold
        return True
    
    def _does_volume_confirm_zone(self, df: pd.DataFrame, zone: RejectionZone) -> bool:
        """Check if volume confirms the zone"""
        return zone.volume_confirmation
    
    def _does_pattern_confirm_zone(self, pattern: CandlePattern, zone: RejectionZone) -> bool:
        """Check if candle pattern confirms the zone"""
        if "SUPPORT" in zone.zone_type:
            return pattern.pattern_type in ["BULLISH_REVERSAL", "BULLISH_CONTINUATION"]
        elif "RESISTANCE" in zone.zone_type:
            return pattern.pattern_type in ["BEARISH_REVERSAL", "BEARISH_CONTINUATION"]
        return False
    
    def _does_structure_confirm_zone(self, df: pd.DataFrame, zone: RejectionZone) -> bool:
        """Check if structure confirms the zone"""
        if len(df) < 30:
            return True
        
        current_price = float(df['close'].iloc[-1])
        
        if "SUPPORT" in zone.zone_type:
            # Check if this is a higher low in uptrend
            prices = df['close'].values[-20:]
            if len(prices) >= 10:
                recent_low = np.min(prices[-10:])
                prev_low = np.min(prices[-20:-10])
                return recent_low > prev_low * 0.995
        
        elif "RESISTANCE" in zone.zone_type:
            # Check if this is a lower high in downtrend
            prices = df['close'].values[-20:]
            if len(prices) >= 10:
                recent_high = np.max(prices[-10:])
                prev_high = np.max(prices[-20:-10])
                return recent_high < prev_high * 1.005
        
        return True
    
    def _does_trend_align_with_zone(self, df: pd.DataFrame, zone: RejectionZone) -> bool:
        """Check if trend aligns with zone"""
        trend = self._get_trend_direction(df)
        
        if trend == "NEUTRAL":
            return True
        
        if "SUPPORT" in zone.zone_type:
            return trend in ["BULLISH", "NEUTRAL"]
        elif "RESISTANCE" in zone.zone_type:
            return trend in ["BEARISH", "NEUTRAL"]
        
        return True
    
    def _calculate_volatility(self, df: pd.DataFrame) -> float:
        """Calculate current volatility"""
        if len(df) < 20:
            return 0.5
        
        recent_candles = df.iloc[-10:]
        candle_ranges = []
        
        for _, candle in recent_candles.iterrows():
            candle_range = candle['high'] - candle['low']
            if candle['close'] > 0:
                range_pct = candle_range / candle['close'] * 100
                candle_ranges.append(range_pct)
        
        if not candle_ranges:
            return 0.5
        
        avg_range = np.mean(candle_ranges)
        
        # Normalize to 0-1 scale
        if avg_range < 0.5:
            return 0.3  # Low volatility
        elif avg_range < 1.0:
            return 0.6  # Medium volatility
        else:
            return 0.9  # High volatility
    
    def _assess_candle_quality(self, candle, context_side: str) -> Dict:
        """Assess quality of current candle for entry"""
        body_size = abs(candle['close'] - candle['open'])
        total_range = candle['high'] - candle['low']
        
        if total_range == 0:
            return {"score": 0.3, "reason": "No range candle"}
        
        body_ratio = body_size / total_range
        
        if context_side == "BULLISH_CONTEXT":
            # For bullish entries, prefer bullish candles
            if candle['close'] > candle['open']:
                if body_ratio > 0.6:
                    return {"score": 0.9, "reason": "Strong bullish candle"}
                else:
                    return {"score": 0.7, "reason": "Bullish candle"}
            else:
                return {"score": 0.4, "reason": "Bearish candle in bullish context"}
        
        elif context_side == "BEARISH_CONTEXT":
            # For bearish entries, prefer bearish candles
            if candle['close'] < candle['open']:
                if body_ratio > 0.6:
                    return {"score": 0.9, "reason": "Strong bearish candle"}
                else:
                    return {"score": 0.7, "reason": "Bearish candle"}
            else:
                return {"score": 0.4, "reason": "Bullish candle in bearish context"}
        
        # For neutral context
        if body_ratio > 0.7:
            return {"score": 0.8, "reason": "Strong directional candle"}
        elif body_ratio < 0.3:
            return {"score": 0.6, "reason": "Indecision candle"}
        else:
            return {"score": 0.7, "reason": "Normal candle"}
    
    def _analyze_recent_price_action(self, df: pd.DataFrame, context_side: str) -> Dict:
        """Analyze recent price action for entry quality"""
        if len(df) < 5:
            return {"score": 0.5, "reason": "Insufficient data"}
        
        recent_candles = df.iloc[-5:]
        closes = recent_candles['close'].values
        
        # Check momentum
        if len(closes) >= 3:
            momentum = closes[-1] - closes[-3]
            momentum_pct = abs(momentum) / closes[-3] * 100
            
            if context_side == "BULLISH_CONTEXT":
                if momentum > 0:
                    if momentum_pct > 0.5:
                        return {"score": 0.9, "reason": "Strong bullish momentum"}
                    else:
                        return {"score": 0.7, "reason": "Mild bullish momentum"}
                else:
                    return {"score": 0.4, "reason": "Bearish momentum in bullish context"}
            
            elif context_side == "BEARISH_CONTEXT":
                if momentum < 0:
                    if momentum_pct > 0.5:
                        return {"score": 0.9, "reason": "Strong bearish momentum"}
                    else:
                        return {"score": 0.7, "reason": "Mild bearish momentum"}
                else:
                    return {"score": 0.4, "reason": "Bullish momentum in bearish context"}
        
        return {"score": 0.6, "reason": "Neutral momentum"}
    
    # ========== SIGNAL CREATION ==========
    
    def _create_comprehensive_signal(self, symbol: str, multi_tf_data: Dict,
                                   wave_context: WaveContext, best_zone: RejectionZone,
                                   tf_confirmation: Dict, confluence_score: float,
                                   entry_quality: Dict) -> Optional[RejectionSignal]:
        """Create comprehensive signal after all gates passed"""
        try:
            # Get entry timeframe data
            df_3m = multi_tf_data.get("3M")
            if not df_3m or len(df_3m) < 10:
                return None
            
            current_price = float(df_3m['close'].iloc[-1])
            
            # ===== CALCULATE LOGICAL TP/SL (NOT RANDOM) =====
            tp_sl_calculation = self._calculate_logical_tp_sl(
                best_zone, wave_context, entry_quality["volatility"], 
                entry_quality["distance_pct"], df_3m
            )
            
            entry_price = tp_sl_calculation["entry_price"]
            stop_loss = tp_sl_calculation["stop_loss"]
            take_profit = tp_sl_calculation["take_profit"]
            stop_loss_pct = tp_sl_calculation["stop_loss_pct"]
            take_profit_pct = tp_sl_calculation["take_profit_pct"]
            stop_loss_reason = tp_sl_calculation["stop_loss_reason"]
            take_profit_reason = tp_sl_calculation["take_profit_reason"]
            
            # Calculate risk/reward
            risk = abs(entry_price - stop_loss)
            reward = abs(take_profit - entry_price)
            risk_reward = reward / risk if risk > 0 else 1.0
            
            # Determine side
            if "SUPPORT" in best_zone.zone_type:
                side = "LONG"
            else:
                side = "SHORT"
            
            # Check deduplication
            if not self.deduplicator.should_generate_signal(symbol, side, entry_price):
                log.debug(f"{symbol}: Duplicate signal filtered")
                return None
            
            # ===== ANALYZE INDICATORS =====
            indicators = {}
            for tf_name in ["4H", "1H", "15M", "5M", "3M", "1M"]:
                df = multi_tf_data.get(tf_name)
                if df is not None and len(df) >= 50:
                    indicators[tf_name] = self.indicator_analyzer.analyze_all_indicators(df)
                else:
                    indicators[tf_name] = self.indicator_analyzer._get_default_analysis()
            
            # ===== ANALYZE CANDLE PATTERNS =====
            candle_patterns = []
            dominant_pattern = None
            
            for tf_name, df in multi_tf_data.items():
                if df is not None and len(df) >= 10:
                    patterns = self.pattern_scanner.detect_patterns(df, tf_name)
                    candle_patterns.extend(patterns)
            
            if candle_patterns:
                dominant_pattern = max(candle_patterns, key=lambda p: p.reliability)
            
            # ===== ANALYZE VOLUME =====
            volume_profile, volume_clusters = self._analyze_volume_profile(df_3m)
            
            # ===== CHECK MULTI-TF CONFIRMATION =====
            multi_tf_confirmation = {}
            for tf_name in ["4H", "1H", "15M", "5M", "3M"]:
                if tf_name in multi_tf_data:
                    analysis = indicators[tf_name]
                    if side == "LONG":
                        confirms = (
                            analysis.rsi_value < 60 and
                            analysis.macd_trend in ["BULLISH", "NEUTRAL"] and
                            analysis.bb_position != "UPPER_BAND"
                        )
                    else:
                        confirms = (
                            analysis.rsi_value > 40 and
                            analysis.macd_trend in ["BEARISH", "NEUTRAL"] and
                            analysis.bb_position != "LOWER_BAND"
                        )
                    multi_tf_confirmation[tf_name] = bool(confirms)
            
            # Calculate convergence score
            convergence_score = tf_confirmation["overall_score"]
            
            # ===== CALCULATE REJECTION STRENGTH =====
            rejection_strength = self._calculate_rejection_strength(
                best_zone, wave_context, confluence_score, 
                entry_quality["score"], convergence_score
            )
            
            # ===== CALCULATE FILTER SCORES =====
            filter_scores, passed_filters, failed_filters = self._calculate_hierarchical_scores(
                wave_context, best_zone, tf_confirmation, confluence_score,
                entry_quality, risk_reward, rejection_strength
            )
            
            # ===== CALCULATE TOTAL SCORE =====
            total_score = self._calculate_total_score(filter_scores)
            
            # ===== GET CONDITIONS MET =====
            conditions_met = self._get_hierarchical_conditions(
                wave_context, best_zone, tf_confirmation, 
                confluence_score, entry_quality, candle_patterns,
                dominant_pattern, multi_tf_confirmation
            )
            
            # ===== GET RSI AT ENTRY =====
            rsi_at_entry = indicators["3M"].rsi_value
            
            # ===== GET REJECTION TYPE =====
            rejection_type, trigger_candle = self._get_rejection_type(df_3m, best_zone, side)
            
            # ===== CALCULATE VOLATILITY =====
            volatility_at_entry = entry_quality["volatility"]
            
            # ===== CREATE SIGNAL ID =====
            signal_id = hashlib.md5(
                f"{symbol}:{side}:{entry_price:.8f}:{time.time()}:{total_score:.2f}".encode()
            ).hexdigest()
            
            # ===== CREATE SIGNAL =====
            signal = RejectionSignal(
                signal_id=signal_id,
                symbol=symbol,
                side=side,
                
                # Price levels - LOGICAL NOT RANDOM
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                stop_loss_pct=stop_loss_pct,
                take_profit_pct=take_profit_pct,
                stop_loss_reason=stop_loss_reason,
                take_profit_reason=take_profit_reason,
                
                # Analysis context
                wave_context=wave_context,
                market_strength=self._analyze_market_strength_hierarchical(df_3m),
                rejection_zone=best_zone,
                
                # Entry triggers
                rejection_type=rejection_type,
                trigger_candle=trigger_candle,
                rsi_at_entry=rsi_at_entry,
                
                # Metrics
                rejection_strength=rejection_strength,
                risk_reward=risk_reward,
                expected_move_pct=take_profit_pct,
                volatility_at_entry=volatility_at_entry,
                
                # Timing
                timeframe_used="3M",
                signal_timestamp=time.time(),
                conditions_met=conditions_met,
                
                # Enhanced analysis
                candle_patterns=candle_patterns,
                dominant_pattern=dominant_pattern,
                
                # Indicator analysis
                indicators_4h=indicators.get("4H"),
                indicators_1h=indicators.get("1H"),
                indicators_15m=indicators.get("15M"),
                indicators_5m=indicators.get("5M"),
                indicators_3m=indicators.get("3M"),
                indicators_1m=indicators.get("1M"),
                
                # Volume analysis
                volume_profile=volume_profile,
                volume_clusters=volume_clusters,
                
                # Multi-timeframe confirmation
                multi_tf_confirmation=multi_tf_confirmation,
                convergence_score=convergence_score,
                
                # Filter scores
                filter_scores=filter_scores,
                total_score=total_score,
                passed_filters=passed_filters,
                failed_filters=failed_filters,
                
                # Decision tracking
                decision_gates_passed=["GATE1", "GATE2", "GATE3", "GATE4", "GATE5", "GATE6"],
                decision_gates_failed=[]
            )
            
            # ===== UPDATE STATISTICS =====
            self._update_statistics(signal)
            
            # ===== REGISTER SIGNAL =====
            self.deduplicator.register_signal(signal)
            self.active_signal_ids.add(signal_id)
            
            return signal
            
        except Exception as e:
            log.error(f"Signal creation error for {symbol}: {e}")
            import traceback
            log.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    def _calculate_logical_tp_sl(self, zone: RejectionZone, wave_context: WaveContext,
                               volatility: float, distance_pct: float, 
                               df_3m: pd.DataFrame) -> Dict:
        """
        Calculate logical TP/SL based on market structure, NOT random
        """
        current_price = float(df_3m['close'].iloc[-1])
        zone_price = zone.price_level
        
        # Determine if it's support or resistance
        is_support = "SUPPORT" in zone.zone_type
        
        # ===== STOP LOSS CALCULATION =====
        # Based on structure, not random percentage
        
        if is_support:
            # For LONG trades at support
            entry_price = zone_price * 1.001  # Slightly above support
            
            # Find next significant support level for SL
            if len(df_3m) >= 30:
                # Look for previous swing low
                lows = df_3m['low'].values[-30:]
                swing_lows = []
                
                for i in range(2, len(lows)-2):
                    if (lows[i] < lows[i-2] and lows[i] < lows[i-1] and 
                        lows[i] < lows[i+1] and lows[i] < lows[i+2]):
                        swing_lows.append(lows[i])
                
                if swing_lows:
                    # Use the lowest swing low that's below entry
                    valid_sl_levels = [sl for sl in swing_lows if sl < entry_price]
                    if valid_sl_levels:
                        stop_loss = min(valid_sl_levels) * 0.999
                        stop_loss_reason = "Previous swing low"
                    else:
                        # If no swing low below, use volatility-based SL
                        atr = self._calculate_atr(df_3m)
                        stop_loss = entry_price * (1 - (atr * 1.5 / 100))
                        stop_loss_reason = "Volatility-based (no swing low)"
                else:
                    # Volatility-based SL
                    atr = self._calculate_atr(df_3m)
                    stop_loss = entry_price * (1 - (atr * 2.0 / 100))
                    stop_loss_reason = "Volatility-based"
            else:
                # Simple volatility-based SL
                atr = self._calculate_atr(df_3m)
                stop_loss = entry_price * (1 - (atr * 2.0 / 100))
                stop_loss_reason = "Simple volatility-based"
            
            # Ensure SL is not too tight
            min_sl_distance = entry_price * 0.005  # 0.5% minimum
            if (entry_price - stop_loss) < min_sl_distance:
                stop_loss = entry_price * (1 - 0.005)
                stop_loss_reason = f"Adjusted to minimum {min_sl_distance/entry_price*100:.1f}%"
            
            # ===== TAKE PROFIT CALCULATION =====
            # Based on wave context and structure
            
            if wave_context.wave_length == "SHORT":
                # Short waves = smaller targets
                base_target = 1.5  # 1.5%
            elif wave_context.wave_length == "MEDIUM":
                base_target = 2.5  # 2.5%
            else:  # EXTENDED
                base_target = 4.0  # 4.0%
            
            # Adjust for volatility
            if volatility > 0.7:  # High volatility
                target_multiplier = 1.3
            elif volatility < 0.4:  # Low volatility
                target_multiplier = 0.8
            else:
                target_multiplier = 1.0
            
            # Adjust for wave maturity
            if wave_context.wave_maturity > 0.7:
                target_multiplier *= 0.8  # Mature waves = smaller moves expected
            elif wave_context.wave_maturity < 0.3:
                target_multiplier *= 1.2  # Early waves = larger moves possible
            
            take_profit_pct = base_target * target_multiplier
            take_profit = entry_price * (1 + take_profit_pct / 100)
            take_profit_reason = f"Wave-based: {wave_context.wave_length} wave"
            
            # Look for structure resistance
            if len(df_3m) >= 50:
                highs = df_3m['high'].values[-50:]
                swing_highs = []
                
                for i in range(2, len(highs)-2):
                    if (highs[i] > highs[i-2] and highs[i] > highs[i-1] and 
                        highs[i] > highs[i+1] and highs[i] > highs[i+2]):
                        swing_highs.append(highs[i])
                
                if swing_highs:
                    # Find nearest resistance above entry
                    valid_tp_levels = [tp for tp in swing_highs if tp > entry_price]
                    if valid_tp_levels:
                        nearest_resistance = min(valid_tp_levels)
                        tp_distance_pct = (nearest_resistance - entry_price) / entry_price * 100
                        
                        if 1.0 <= tp_distance_pct <= 5.0:  # Reasonable distance
                            take_profit = nearest_resistance * 0.998
                            take_profit_pct = tp_distance_pct * 0.95  # Slightly below resistance
                            take_profit_reason = "Structure resistance"
        
        else:  # RESISTANCE - SHORT trade
            entry_price = zone_price * 0.999  # Slightly below resistance
            
            # Find next significant resistance level for SL
            if len(df_3m) >= 30:
                # Look for previous swing high
                highs = df_3m['high'].values[-30:]
                swing_highs = []
                
                for i in range(2, len(highs)-2):
                    if (highs[i] > highs[i-2] and highs[i] > highs[i-1] and 
                        highs[i] > highs[i+1] and highs[i] > highs[i+2]):
                        swing_highs.append(highs[i])
                
                if swing_highs:
                    # Use the highest swing high that's above entry
                    valid_sl_levels = [sl for sl in swing_highs if sl > entry_price]
                    if valid_sl_levels:
                        stop_loss = max(valid_sl_levels) * 1.001
                        stop_loss_reason = "Previous swing high"
                    else:
                        # Volatility-based SL
                        atr = self._calculate_atr(df_3m)
                        stop_loss = entry_price * (1 + (atr * 1.5 / 100))
                        stop_loss_reason = "Volatility-based (no swing high)"
                else:
                    # Volatility-based SL
                    atr = self._calculate_atr(df_3m)
                    stop_loss = entry_price * (1 + (atr * 2.0 / 100))
                    stop_loss_reason = "Volatility-based"
            else:
                # Simple volatility-based SL
                atr = self._calculate_atr(df_3m)
                stop_loss = entry_price * (1 + (atr * 2.0 / 100))
                stop_loss_reason = "Simple volatility-based"
            
            # Ensure SL is not too tight
            min_sl_distance = entry_price * 0.005  # 0.5% minimum
            if (stop_loss - entry_price) < min_sl_distance:
                stop_loss = entry_price * (1 + 0.005)
                stop_loss_reason = f"Adjusted to minimum {min_sl_distance/entry_price*100:.1f}%"
            
            # ===== TAKE PROFIT CALCULATION =====
            if wave_context.wave_length == "SHORT":
                base_target = 1.5  # 1.5%
            elif wave_context.wave_length == "MEDIUM":
                base_target = 2.5  # 2.5%
            else:  # EXTENDED
                base_target = 4.0  # 4.0%
            
            # Adjust for volatility
            if volatility > 0.7:  # High volatility
                target_multiplier = 1.3
            elif volatility < 0.4:  # Low volatility
                target_multiplier = 0.8
            else:
                target_multiplier = 1.0
            
            # Adjust for wave maturity
            if wave_context.wave_maturity > 0.7:
                target_multiplier *= 0.8
            elif wave_context.wave_maturity < 0.3:
                target_multiplier *= 1.2
            
            take_profit_pct = base_target * target_multiplier
            take_profit = entry_price * (1 - take_profit_pct / 100)
            take_profit_reason = f"Wave-based: {wave_context.wave_length} wave"
            
            # Look for structure support
            if len(df_3m) >= 50:
                lows = df_3m['low'].values[-50:]
                swing_lows = []
                
                for i in range(2, len(lows)-2):
                    if (lows[i] < lows[i-2] and lows[i] < lows[i-1] and 
                        lows[i] < lows[i+1] and lows[i] < lows[i+2]):
                        swing_lows.append(lows[i])
                
                if swing_lows:
                    # Find nearest support below entry
                    valid_tp_levels = [tp for tp in swing_lows if tp < entry_price]
                    if valid_tp_levels:
                        nearest_support = max(valid_tp_levels)  # Max because we want highest support
                        tp_distance_pct = (entry_price - nearest_support) / entry_price * 100
                        
                        if 1.0 <= tp_distance_pct <= 5.0:
                            take_profit = nearest_support * 1.002
                            take_profit_pct = tp_distance_pct * 0.95
                            take_profit_reason = "Structure support"
        
        # Calculate percentages
        stop_loss_pct = abs(stop_loss - entry_price) / entry_price * 100
        take_profit_pct = abs(take_profit - entry_price) / entry_price * 100
        
        # Ensure reasonable risk/reward
        if stop_loss_pct > MAX_STOP_LOSS_PCT:
            # Adjust TP to maintain minimum risk/reward
            required_tp_pct = stop_loss_pct * MIN_RISK_REWARD
            if required_tp_pct <= MAX_TARGET_PCT:
                take_profit_pct = required_tp_pct
                if is_support:
                    take_profit = entry_price * (1 + take_profit_pct / 100)
                else:
                    take_profit = entry_price * (1 - take_profit_pct / 100)
                take_profit_reason = f"Adjusted for min R:R {MIN_RISK_REWARD}:1"
        
        # Final validation
        stop_loss_pct = min(stop_loss_pct, MAX_STOP_LOSS_PCT)
        take_profit_pct = min(max(take_profit_pct, MIN_TARGET_PCT), MAX_TARGET_PCT)
        
        return {
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "stop_loss_pct": stop_loss_pct,
            "take_profit_pct": take_profit_pct,
            "stop_loss_reason": stop_loss_reason,
            "take_profit_reason": take_profit_reason
        }
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate Average True Range"""
        if len(df) < period + 1:
            return 0.01  # Default 1%
        
        high = df['high']
        low = df['low']
        close = df['close']
        
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()
        
        current_atr = float(atr.iloc[-1])
        current_price = float(close.iloc[-1])
        
        if current_price > 0:
            return current_atr / current_price * 100
        
        return 0.01
    
    def _analyze_market_strength_hierarchical(self, df: pd.DataFrame) -> MarketStrength:
        """Analyze market strength for hierarchical system"""
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
            
            volatility_regime = self._determine_volatility_regime(df)
            
            return MarketStrength(
                candle_speed=candle_speed,
                distance_ratio=distance_ratio,
                ema_angle=ema_angle,
                volume_participation=volume_participation,
                strength_score=strength_score,
                is_continuation=is_continuation,
                is_rejection_setup=is_rejection_setup,
                is_absorption=is_absorption,
                is_compression=is_compression,
                volatility_regime=volatility_regime
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
    
    def _determine_volatility_regime(self, df: pd.DataFrame) -> str:
        """Determine volatility regime"""
        if len(df) < 20:
            return "NORMAL"
        
        atr_pct = self._calculate_atr(df)
        
        if atr_pct < 0.5:
            return "LOW"
        elif atr_pct < 1.0:
            return "NORMAL"
        else:
            return "HIGH"
    
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
            is_compression=False,
            volatility_regime="NORMAL"
        )
    
    def _analyze_volume_profile(self, df: pd.DataFrame) -> Tuple[Dict[str, float], List[float]]:
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
    
    def _calculate_rejection_strength(self, zone: RejectionZone, wave: WaveContext, 
                                     confluence_score: float, entry_quality: float,
                                     convergence_score: float) -> float:
        """Calculate overall rejection strength"""
        factors = []
        weights = []
        
        factors.append(float(zone.strength))
        weights.append(0.25)
        
        factors.append(float(wave.wave_maturity))
        weights.append(0.15)
        
        factors.append(float(confluence_score))
        weights.append(0.25)
        
        factors.append(float(entry_quality))
        weights.append(0.20)
        
        factors.append(float(convergence_score))
        weights.append(0.15)
        
        return float(np.average(factors, weights=weights))
    
    def _calculate_hierarchical_scores(self, wave_context: WaveContext, zone: RejectionZone,
                                     tf_confirmation: Dict, confluence_score: float,
                                     entry_quality: Dict, risk_reward: float,
                                     rejection_strength: float) -> Tuple[Dict[str, float], List[str], List[str]]:
        """Calculate hierarchical filter scores"""
        filter_scores = {}
        passed_filters = []
        failed_filters = []
        
        # 1. Wave Context Score
        wave_score = 0.8 if wave_context.should_trade else 0.3
        filter_scores["wave_context"] = wave_score
        if wave_score >= 0.7:
            passed_filters.append("WAVE_CONTEXT")
        else:
            failed_filters.append("WAVE_CONTEXT")
        
        # 2. Zone Strength Score
        zone_score = zone.strength
        filter_scores["zone_strength"] = zone_score
        if zone_score >= 0.8:
            passed_filters.append("ZONE_STRENGTH")
        else:
            failed_filters.append("ZONE_STRENGTH")
        
        # 3. Zone Class Score
        class_score = 1.0 if zone.zone_class == "PRIMARY" else 0.7 if zone.zone_class == "SECONDARY" else 0.4
        filter_scores["zone_class"] = class_score
        if class_score >= 0.9:
            passed_filters.append("ZONE_CLASS")
        else:
            failed_filters.append("ZONE_CLASS")
        
        # 4. Volume Confirmation Score
        volume_score = 1.0 if zone.volume_confirmation else 0.4
        filter_scores["volume_confirmation"] = volume_score
        if volume_score >= 0.8:
            passed_filters.append("VOLUME_CONFIRMATION")
        else:
            failed_filters.append("VOLUME_CONFIRMATION")
        
        # 5. RSI Position Score
        rsi_score = 0.9 if zone.rsi_position == "IN_ZONE" else 0.5
        filter_scores["rsi_position"] = rsi_score
        if rsi_score >= 0.8:
            passed_filters.append("RSI_POSITION")
        else:
            failed_filters.append("RSI_POSITION")
        
        # 6. TF Confirmation Score
        tf_score = tf_confirmation["overall_score"]
        filter_scores["tf_confirmation"] = tf_score
        if tf_score >= 0.8:
            passed_filters.append("TF_CONFIRMATION")
        else:
            failed_filters.append("TF_CONFIRMATION")
        
        # 7. Confluence Score
        filter_scores["confluence"] = confluence_score
        if confluence_score >= 0.75:
            passed_filters.append("CONFLUENCE")
        else:
            failed_filters.append("CONFLUENCE")
        
        # 8. Entry Quality Score
        entry_score = entry_quality["score"]
        filter_scores["entry_quality"] = entry_score
        if entry_score >= 0.7:
            passed_filters.append("ENTRY_QUALITY")
        else:
            failed_filters.append("ENTRY_QUALITY")
        
        # 9. Risk/Reward Score
        rr_score = min(risk_reward / 3.0, 1.0)
        filter_scores["risk_reward"] = rr_score
        if rr_score >= (MIN_RISK_REWARD / 3.0):
            passed_filters.append("RISK_REWARD")
        else:
            failed_filters.append("RISK_REWARD")
        
        # 10. Rejection Strength Score
        filter_scores["rejection_strength"] = rejection_strength
        if rejection_strength >= REJECTION_CONFIG["min_rejection_strength"]:
            passed_filters.append("REJECTION_STRENGTH")
        else:
            failed_filters.append("REJECTION_STRENGTH")
        
        return filter_scores, passed_filters, failed_filters
    
    def _calculate_total_score(self, filter_scores: Dict[str, float]) -> float:
        """Calculate total score with hierarchical weights"""
        # Hierarchical weights - earlier gates more important
        weights = {
            "wave_context": 0.12,     # Gate 1
            "zone_strength": 0.15,    # Gate 2
            "zone_class": 0.10,       # Gate 2
            "volume_confirmation": 0.08,  # Gate 2
            "rsi_position": 0.08,     # Gate 2
            "tf_confirmation": 0.12,  # Gate 4
            "confluence": 0.10,       # Gate 5
            "entry_quality": 0.10,    # Gate 6
            "risk_reward": 0.07,      # Calculated
            "rejection_strength": 0.08 # Calculated
        }
        
        total_score = 0.0
        for filter_name, score in filter_scores.items():
            weight = weights.get(filter_name, 0.1)
            total_score += score * weight
        
        total_score = total_score * 100  # Convert to 0-100 scale
        
        return float(total_score)
    
    def _get_rejection_type(self, df: pd.DataFrame, zone: RejectionZone, side: str) -> Tuple[str, str]:
        """Get rejection type and trigger candle"""
        if len(df) < 3:
            return "NO_CLEAR_REJECTION", "NONE"
        
        current_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]
        
        if side == "LONG":
            # Check for wick rejection
            if (current_candle['low'] < zone.price_level and 
                current_candle['close'] > zone.price_level):
                return "WICK_REJECTION", "SUPPORT_WICK"
            
            # Check for momentum shift
            if (prev_candle['close'] < prev_candle['open'] and
                current_candle['close'] > current_candle['open'] and
                abs(current_candle['close'] - zone.price_level) / zone.price_level < 0.002):
                return "MOMENTUM_REJECTION", "BULLISH_REVERSAL"
            
            # Check for price hold
            if current_candle['low'] <= zone.price_level * 1.001 and current_candle['close'] > zone.price_level:
                return "PRICE_REJECTION", "SUPPORT_HOLD"
        
        else:  # SHORT
            # Check for wick rejection
            if (current_candle['high'] > zone.price_level and 
                current_candle['close'] < zone.price_level):
                return "WICK_REJECTION", "RESISTANCE_WICK"
            
            # Check for momentum shift
            if (prev_candle['close'] > prev_candle['open'] and
                current_candle['close'] < current_candle['open'] and
                abs(current_candle['close'] - zone.price_level) / zone.price_level < 0.002):
                return "MOMENTUM_REJECTION", "BEARISH_REVERSAL"
            
            # Check for price hold
            if current_candle['high'] >= zone.price_level * 0.999 and current_candle['close'] < zone.price_level:
                return "PRICE_REJECTION", "RESISTANCE_HOLD"
        
        return "NO_CLEAR_REJECTION", "NONE"
    
    def _get_hierarchical_conditions(self, wave_context: WaveContext, zone: RejectionZone,
                                   tf_confirmation: Dict, confluence_score: float,
                                   entry_quality: Dict, candle_patterns: List[CandlePattern],
                                   dominant_pattern: Optional[CandlePattern],
                                   multi_tf_confirmation: Dict[str, bool]) -> List[str]:
        """Get hierarchical conditions met"""
        conditions = []
        
        # Wave conditions
        conditions.append(f"WAVE_{wave_context.wave_length}")
        conditions.append(f"STRUCTURE_{wave_context.structure_type}")
        conditions.append(f"CONTEXT_{wave_context.context_side}")
        conditions.append(f"POSITION_{wave_context.wave_position}")
        
        # Zone conditions
        conditions.append(f"ZONE_{zone.zone_type}")
        conditions.append(f"ZONE_CLASS_{zone.zone_class}")
        if zone.volume_confirmation:
            conditions.append("VOLUME_CONFIRMED")
        conditions.append(f"RSI_{zone.rsi_position}")
        
        # TF confirmation conditions
        if tf_confirmation["higher_tf"]:
            conditions.append("HIGHER_TF_CONFIRMED")
        if tf_confirmation["medium_tf"]:
            conditions.append("MEDIUM_TF_CONFIRMED")
        if tf_confirmation["trigger_tf"]:
            conditions.append("TRIGGER_TF_CONFIRMED")
        
        # Confluence conditions
        if confluence_score >= 0.8:
            conditions.append("HIGH_CONFLUENCE")
        elif confluence_score >= 0.7:
            conditions.append("MEDIUM_CONFLUENCE")
        
        # Entry quality conditions
        conditions.extend([f"ENTRY_{reason}" for reason in entry_quality["reasons"][:3]])
        
        # Pattern conditions
        if dominant_pattern:
            conditions.append(f"PATTERN_{dominant_pattern.pattern_name}")
        
        # Count confirmed timeframes
        confirmed_tfs = sum(1 for v in multi_tf_confirmation.values() if v)
        conditions.append(f"MTF_{confirmed_tfs}_CONFIRMED")
        
        return conditions
    
    def _update_statistics(self, signal: RejectionSignal):
        """Update statistics"""
        self.daily_stats["rejections_found"] += 1
        if signal.side == "LONG":
            self.daily_stats["long_rejections"] += 1
        else:
            self.daily_stats["short_rejections"] += 1
        
        if signal.total_score >= 70:
            self.daily_stats["high_score_signals"] += 1
        elif signal.total_score >= 50:
            self.daily_stats["medium_score_signals"] += 1
        else:
            self.daily_stats["low_score_signals"] += 1
    
    def get_daily_stats(self) -> Dict:
        """Get daily statistics"""
        return self.daily_stats
    
    def cleanup_old_signals(self):
        """Clean up old signals"""
        self.deduplicator.remove_closed_signals()

# ================ MAIN SCANNER SYSTEM ================
class HierarchicalCompleteScanner:
    """Main scanner system with hierarchical decision making"""
    
    def __init__(self):
        self.scanner = HierarchicalRejectionScanner()
        self.exchange = None
        self.db = None
        self.scan_cycle = 0
        self.signals_generated = 0
        self.max_signals = 10000
    
    async def initialize(self):
        """Initialize the scanner"""
        log.info("=" * 70)
        log.info("🎯 HIERARCHICAL REJECTION-BASED TRADING SCANNER")
        log.info("=" * 70)
        log.info(f"SCAN INTERVAL: {SCAN_INTERVAL}s")
        log.info(f"MAX SIGNALS: {self.max_signals}")
        log.info(f"TP/SL: LOGICAL (not random)")
        log.info(f"DECISION: 6 hierarchical gates")
        log.info("=" * 70)
        
        await self._init_database()
        await self._init_exchange()
        await self._send_startup_message()
    
    async def _init_database(self):
        """Initialize database"""
        try:
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            self.db = await aiosqlite.connect(DB_PATH)
            
            # Enhanced signals table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS hierarchical_signals (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                stop_loss_pct REAL NOT NULL,
                take_profit_pct REAL NOT NULL,
                stop_loss_reason TEXT NOT NULL,
                take_profit_reason TEXT NOT NULL,
                
                wave_length TEXT NOT NULL,
                wave_maturity REAL NOT NULL,
                expansion_speed REAL NOT NULL,
                structure_type TEXT NOT NULL,
                context_side TEXT NOT NULL,
                wave_position TEXT NOT NULL,
                should_trade BOOLEAN NOT NULL,
                
                candle_speed REAL NOT NULL,
                distance_ratio REAL NOT NULL,
                ema_angle REAL NOT NULL,
                volume_participation REAL NOT NULL,
                strength_score REAL NOT NULL,
                strength_flags TEXT NOT NULL,
                volatility_regime TEXT NOT NULL,
                
                zone_type TEXT NOT NULL,
                zone_price REAL NOT NULL,
                zone_strength REAL NOT NULL,
                zone_class TEXT NOT NULL,
                rejection_strength REAL NOT NULL,
                rsi_at_entry REAL NOT NULL,
                rejection_type TEXT NOT NULL,
                trigger_candle TEXT NOT NULL,
                
                candle_patterns TEXT,
                dominant_pattern TEXT,
                
                indicators_4h TEXT,
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
                volatility_at_entry REAL NOT NULL,
                timeframe_used TEXT NOT NULL,
                
                conditions_met TEXT,
                
                filter_scores TEXT NOT NULL,
                total_score REAL NOT NULL,
                passed_filters TEXT NOT NULL,
                failed_filters TEXT NOT NULL,
                decision_gates_passed TEXT NOT NULL,
                decision_gates_failed TEXT NOT NULL,
                
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
            
            # Statistics table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS hierarchical_stats (
                date DATE PRIMARY KEY,
                total_signals INTEGER,
                high_score_signals INTEGER,
                medium_score_signals INTEGER,
                low_score_signals INTEGER,
                gate1_passed INTEGER,
                gate2_passed INTEGER,
                gate3_passed INTEGER,
                gate4_passed INTEGER,
                gate5_passed INTEGER,
                gate6_passed INTEGER,
                avg_total_score REAL,
                avg_risk_reward REAL,
                filter_stats TEXT
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
        """Send startup message to Telegram"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning("⚠️ Telegram credentials not set. Notifications disabled.")
            return
        
        try:
            message = f"""
HIERARCHICAL REJECTION SCANNER STARTED

SCANNER CONFIGURATION:
Scan Interval: {SCAN_INTERVAL}s
Max Signals: {self.max_signals}
Pairs: Top {TOP_N_VOLUME} by volume
Min Volume: ${MIN_VOLUME_USD:,}

HIERARCHICAL DECISION GATES:
1. Wave Context Analysis
2. Primary Zone Identification
3. Active Rejection Check
4. Multi-Timeframe Confirmation
5. Indicator Confluence
6. Entry Quality Assessment

TP/SL: LOGICAL (based on structure & volatility)
Database: {DB_PATH}

#HierarchicalScanner #LogicalTP_SL
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message
                })
                
            log.info("✅ Startup message sent")
                
        except Exception as e:
            log.error(f"Telegram startup error: {e}")
    
    async def fetch_timeframe_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """Fetch data for all timeframes"""
        data = {}
        
        for tf_name, tf in TIMEFRAMES.items():
            try:
                if tf_name == "4H":
                    limit = 120
                elif tf_name == "1H":
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
    
    async def save_signal(self, signal: RejectionSignal) -> bool:
        """Save signal to database"""
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
            
            await self.db.execute("""
                INSERT INTO hierarchical_signals (
                    id, symbol, side, entry_price, stop_loss, take_profit,
                    stop_loss_pct, take_profit_pct, stop_loss_reason, take_profit_reason,
                    
                    wave_length, wave_maturity, expansion_speed, structure_type, 
                    context_side, wave_position, should_trade,
                    
                    candle_speed, distance_ratio, ema_angle, volume_participation, 
                    strength_score, strength_flags, volatility_regime,
                    
                    zone_type, zone_price, zone_strength, zone_class, rejection_strength, 
                    rsi_at_entry, rejection_type, trigger_candle,
                    
                    candle_patterns, dominant_pattern,
                    
                    indicators_4h, indicators_1h, indicators_15m, indicators_5m, 
                    indicators_3m, indicators_1m,
                    
                    volume_profile, volume_clusters,
                    
                    multi_tf_confirmation, convergence_score,
                    
                    risk_reward, expected_move, volatility_at_entry, timeframe_used,
                    
                    conditions_met,
                    
                    filter_scores, total_score, passed_filters, failed_filters,
                    decision_gates_passed, decision_gates_failed,
                    
                    status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                         ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                         ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                         ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.side,
                float(signal.entry_price),
                float(signal.stop_loss),
                float(signal.take_profit),
                float(signal.stop_loss_pct),
                float(signal.take_profit_pct),
                str(signal.stop_loss_reason),
                str(signal.take_profit_reason),
                
                str(signal.wave_context.wave_length),
                float(signal.wave_context.wave_maturity),
                float(signal.wave_context.expansion_speed),
                str(signal.wave_context.structure_type),
                str(signal.wave_context.context_side),
                str(signal.wave_context.wave_position),
                bool(signal.wave_context.should_trade),
                
                float(signal.market_strength.candle_speed),
                float(signal.market_strength.distance_ratio),
                float(signal.market_strength.ema_angle),
                float(signal.market_strength.volume_participation),
                float(signal.market_strength.strength_score),
                json.dumps(strength_flags),
                str(signal.market_strength.volatility_regime),
                
                str(signal.rejection_zone.zone_type),
                float(signal.rejection_zone.price_level),
                float(signal.rejection_zone.strength),
                str(signal.rejection_zone.zone_class),
                float(signal.rejection_strength),
                float(signal.rsi_at_entry),
                str(signal.rejection_type),
                str(signal.trigger_candle),
                
                json.dumps(candle_patterns_list),
                json.dumps(dominant_pattern_info),
                
                json.dumps(signal.indicators_4h.to_dict()),
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
                float(signal.volatility_at_entry),
                str(signal.timeframe_used),
                
                json.dumps(signal.conditions_met),
                
                json.dumps(signal.filter_scores),
                float(signal.total_score),
                json.dumps(signal.passed_filters),
                json.dumps(signal.failed_filters),
                json.dumps(signal.decision_gates_passed),
                json.dumps(signal.decision_gates_failed),
                
                "PENDING",
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
            
            await self.db.commit()
            
            self.signals_generated += 1
            log.info(f"✅ Hierarchical signal saved: {signal.symbol} (Score: {signal.total_score:.1f})")
            log.info(f"   SL: {signal.stop_loss_pct:.2f}% ({signal.stop_loss_reason})")
            log.info(f"   TP: {signal.take_profit_pct:.2f}% ({signal.take_profit_reason})")
            log.info(f"   Total: {self.signals_generated}/{self.max_signals}")
            
            return True
            
        except Exception as e:
            log.error(f"Error saving signal: {e}")
            import traceback
            log.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    async def send_signal_to_telegram(self, signal: RejectionSignal) -> bool:
        """Send professional signal breakdown to Telegram"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.debug("Telegram credentials not set. Skipping signal notification.")
            return False
        
        try:
            # Count confirmed timeframes
            confirmed_tfs = sum(1 for v in signal.multi_tf_confirmation.values() if v)
            total_tfs = len(signal.multi_tf_confirmation)
            
            # Get best and worst filters
            best_filter = max(signal.filter_scores.items(), key=lambda x: x[1])
            worst_filter = min(signal.filter_scores.items(), key=lambda x: x[1])
            
            message = f"""
🎯 HIERARCHICAL REJECTION SIGNAL

{symbol} {signal.side} | Score: {signal.total_score:.1f}/100
ID: {signal.signal_id[:12]} | {datetime.fromtimestamp(signal.signal_timestamp).strftime('%H:%M:%S')}

📊 LOGICAL TP/SL (NOT RANDOM):
Entry: {signal.entry_price:.8f}
Stop Loss: {signal.stop_loss:.8f} ({signal.stop_loss_pct:.2f}%)
Take Profit: {signal.take_profit:.8f} ({signal.take_profit_pct:.2f}%)
Risk/Reward: {signal.risk_reward:.2f}:1
SL Reason: {signal.stop_loss_reason}
TP Reason: {signal.take_profit_reason}

🎯 HIERARCHICAL GATES (6/6 passed):
1. Wave Context: {signal.wave_context.wave_length} {signal.wave_context.structure_type}
2. Zone: {signal.rejection_zone.zone_type} ({signal.rejection_zone.zone_class})
3. Rejection: {signal.rejection_type} at {signal.rejection_zone.price_level:.8f}
4. TF Confirmed: {confirmed_tfs}/{total_tfs} timeframes
5. Confluence: {signal.convergence_score:.2f}
6. Entry Quality: {signal.volatility_at_entry:.2f} volatility

📈 ANALYSIS:
Rejection Strength: {signal.rejection_strength:.2f}
Zone Strength: {signal.rejection_zone.strength:.2f}
RSI: {signal.rsi_at_entry:.1f} ({signal.rejection_zone.rsi_position})
Volatility: {signal.volatility_at_entry:.2f} ({signal.market_strength.volatility_regime})

🔍 FILTER PERFORMANCE:
Best: {best_filter[0]}: {best_filter[1]:.2f}
Worst: {worst_filter[0]}: {worst_filter[1]:.2f}
Passed: {len(signal.passed_filters)}/{len(signal.filter_scores)} filters

#HierarchicalSignal #{signal.side} #{signal.symbol.replace('/', '')}
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "disable_web_page_preview": True
                })
                
                if response.status_code == 200:
                    log.info(f"📤 Hierarchical signal sent: {signal.symbol} (Score: {signal.total_score:.1f})")
                    return True
                else:
                    log.error(f"Telegram error: {response.status_code} - {response.text[:100]}")
                    return False
            
        except Exception as e:
            log.error(f"Telegram signal error: {e}")
            import traceback
            log.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    async def simple_tp_sl_monitor(self):
        """Simple TP/SL monitor"""
        log.info("🎯 Starting TP/SL monitor...")
        
        while True:
            try:
                # Get signals that haven't hit TP/SL yet
                async with self.db.execute("""
                    SELECT id, symbol, side, entry_price, stop_loss, take_profit, 
                           total_score, stop_loss_reason, take_profit_reason
                    FROM hierarchical_signals 
                    WHERE status NOT IN ('TP_HIT', 'SL_HIT')
                    ORDER BY created_at DESC
                    LIMIT 100
                """) as cursor:
                    signals = await cursor.fetchall()
                
                for signal in signals:
                    signal_id, symbol, side, entry, sl, tp, total_score, sl_reason, tp_reason = signal
                    
                    try:
                        # Get current price
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = float(ticker['last'])
                        
                        # Check TP/SL
                        tp_hit = False
                        sl_hit = False
                        
                        if side == "LONG":
                            tp_hit = current_price >= tp
                            sl_hit = current_price <= sl
                        else:  # SHORT
                            tp_hit = current_price <= tp
                            sl_hit = current_price >= sl
                        
                        # If TP or SL hit, update and notify
                        if tp_hit or sl_hit:
                            close_reason = "TP_HIT" if tp_hit else "SL_HIT"
                            
                            # Calculate PnL
                            if side == "LONG":
                                pnl_percent = ((current_price - entry) / entry) * 100
                            else:
                                pnl_percent = ((entry - current_price) / entry) * 100
                            
                            # Update database
                            await self.db.execute("""
                                UPDATE hierarchical_signals SET 
                                    status = ?,
                                    closed_at = CURRENT_TIMESTAMP,
                                    close_price = ?,
                                    pnl_percent = ?,
                                    close_reason = ?
                                WHERE id = ?
                            """, (close_reason, current_price, pnl_percent, close_reason, signal_id))
                            
                            await self.db.commit()
                            
                            # Send Telegram notification
                            await self.send_tp_sl_update_to_telegram(
                                signal_id, symbol, side, entry, sl, tp, 
                                current_price, close_reason, total_score, pnl_percent,
                                sl_reason, tp_reason
                            )
                            
                            log.info(f"✅ {symbol} {side} {close_reason} at {current_price:.4f} (PnL: {pnl_percent:+.2f}%)")
                    
                    except Exception as e:
                        log.debug(f"Price check error for {symbol}: {str(e)[:50]}")
                        continue
                
                # Wait 3 seconds between checks
                await asyncio.sleep(3)
                
            except Exception as e:
                log.error(f"Monitor error: {e}")
                await asyncio.sleep(5)
    
    async def send_tp_sl_update_to_telegram(self, signal_id: str, symbol: str, side: str, 
                                          entry: float, sl: float, tp: float, 
                                          current_price: float, close_reason: str,
                                          total_score: float, pnl_percent: float,
                                          sl_reason: str, tp_reason: str):
        """Send TP/SL updates to Telegram"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return False
        
        try:
            if close_reason == "TP_HIT":
                title = "🎯 TAKE PROFIT HIT"
            else:
                title = "🛑 STOP LOSS HIT"
            
            result = f"{pnl_percent:+.2f}% {'PROFIT' if pnl_percent > 0 else 'LOSS'}"
            
            text = f"""
{title}

{symbol} | {side}
Score: {total_score:.1f}/100
Entry: {entry:.4f}
Close: {current_price:.4f}
Result: {result}

SL: {sl:.4f} ({sl_reason})
TP: {tp:.4f} ({tp_reason})

Time: {datetime.now().strftime('%H:%M:%S')}
#{close_reason} #{'Profit' if pnl_percent > 0 else 'Loss'} #{symbol.replace('/', '')}
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": text
                })
            
            log.info(f"📤 Sent {close_reason} update for {symbol} to Telegram")
            return True
            
        except Exception as e:
            log.error(f"Telegram TP/SL error: {e}")
            return False
    
    async def send_scanner_update(self):
        """Send periodic update to Telegram"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            stats = self.scanner.get_daily_stats()
            completion_pct = (self.signals_generated / self.max_signals) * 100
            
            gate_stats = f"""
Gate 1 (Wave Context): {stats.get('gate1_passed', 0)}
Gate 2 (Zone): {stats.get('gate2_passed', 0)}
Gate 3 (Rejection): {stats.get('gate3_passed', 0)}
Gate 4 (TF Confirmation): {stats.get('gate4_passed', 0)}
Gate 5 (Confluence): {stats.get('gate5_passed', 0)}
Gate 6 (Entry Quality): {stats.get('gate6_passed', 0)}
"""
            
            message = f"""
HIERARCHICAL SCANNER UPDATE

Progress: {self.signals_generated}/{self.max_signals} ({completion_pct:.1f}%)

Gate Statistics:
{gate_stats}

Signal Quality:
High Score (70+): {stats['high_score_signals']}
Medium Score (50-70): {stats['medium_score_signals']}
Low Score (<50): {stats['low_score_signals']}

Total Signals: {stats['signals_generated']}
Long: {stats['long_rejections']} | Short: {stats['short_rejections']}

Current Cycle: #{self.scan_cycle}
#HierarchicalUpdate #{'AlmostComplete' if completion_pct > 80 else 'Running'}
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message
                })
                
            log.info(f"📤 Hierarchical update sent: {self.signals_generated}/{self.max_signals}")
                
        except Exception as e:
            log.error(f"Telegram update error: {e}")
    
    async def hierarchical_scanner(self):
        """Main hierarchical scanning loop"""
        log.info("🚀 Starting hierarchical scanner...")
        
        while True:
            try:
                if self.signals_generated >= self.max_signals:
                    log.info(f"✅ Reached max signals ({self.max_signals}), stopping scanner")
                    await self.send_final_stats()
                    break
                
                self.scan_cycle += 1
                start_time = time.time()
                
                log.info(f"📊 Hierarchical scan cycle #{self.scan_cycle} ({self.signals_generated}/{self.max_signals})")
                
                pairs = await self.get_active_pairs()
                
                if not pairs:
                    log.warning("No active pairs found")
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                log.info(f"Scanning {len(pairs)} pairs with hierarchical gates")
                
                signals_found = 0
                pairs_processed = 0
                
                for symbol, volume in pairs:
                    try:
                        multi_tf_data = await self.fetch_timeframe_data(symbol)
                        
                        # Check we have all necessary timeframes
                        required_tfs = ["4H", "1H", "15M", "5M", "3M"]
                        has_all_data = all(tf in multi_tf_data for tf in required_tfs)
                        
                        if not has_all_data:
                            continue
                        
                        # Use hierarchical decision making
                        signal = self.scanner.generate_trader_like_signal(multi_tf_data, symbol)
                        
                        if signal:
                            saved = await self.save_signal(signal)
                            if saved:
                                # Send to Telegram
                                await self.send_signal_to_telegram(signal)
                                signals_found += 1
                        
                        pairs_processed += 1
                        await asyncio.sleep(0.01)
                        
                    except Exception as e:
                        log.debug(f"Pair error {symbol}: {str(e)[:50]}")
                        continue
                
                self.scanner.daily_stats["pairs_scanned"] += pairs_processed
                self.scanner.daily_stats["signals_generated"] += signals_found
                
                stats = self.scanner.get_daily_stats()
                log.info(f"📈 Hierarchical stats: Found {signals_found}, Total: {self.signals_generated}/{self.max_signals}")
                log.info(f"   Gate performance: {stats.get('gate1_passed', 0)}/{stats.get('gate2_passed', 0)}/{stats.get('gate3_passed', 0)}/{stats.get('gate4_passed', 0)}/{stats.get('gate5_passed', 0)}/{stats.get('gate6_passed', 0)}")
                
                scan_duration = time.time() - start_time
                log.info(f"Hierarchical cycle #{self.scan_cycle}: {signals_found} signals in {scan_duration:.2f}s")
                
                # Send update every 50 cycles or every 100 signals
                if self.scan_cycle % 50 == 0 or self.signals_generated % 100 == 0:
                    await self.send_scanner_update()
                
                wait_time = max(0.1, SCAN_INTERVAL - scan_duration)
                log.info(f"Next hierarchical scan in {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                log.error(f"Hierarchical scanner error: {e}")
                await asyncio.sleep(10)
    
    async def run(self):
        """Run the scanner"""
        try:
            await self.initialize()
            
            # Run scanner and TP/SL monitoring
            await asyncio.gather(
                self.hierarchical_scanner(),
                self.simple_tp_sl_monitor()
            )
            
        except KeyboardInterrupt:
            log.info("Hierarchical scanner stopped by user")
            await self.send_final_stats()
            
        except Exception as e:
            log.error(f"Hierarchical scanner crashed: {e}")
            
        finally:
            await self.cleanup()
    
    async def send_final_stats(self):
        """Send final statistics"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning("⚠️ Telegram credentials missing. Skipping final stats.")
            return
        
        try:
            stats = self.scanner.get_daily_stats()
            completion_pct = (self.signals_generated / self.max_signals) * 100
            
            # Calculate average scores
            async with self.db.execute("""
                SELECT total_score, risk_reward FROM hierarchical_signals
            """) as cursor:
                rows = await cursor.fetchall()
                total_scores = [row[0] for row in rows]
                risk_rewards = [row[1] for row in rows]
            
            avg_score = np.mean(total_scores) if total_scores else 0
            avg_rr = np.mean(risk_rewards) if risk_rewards else 0
            
            gate_stats = f"""
Gate 1 (Wave Context): {stats.get('gate1_passed', 0)}
Gate 2 (Zone): {stats.get('gate2_passed', 0)}
Gate 3 (Rejection): {stats.get('gate3_passed', 0)}
Gate 4 (TF Confirmation): {stats.get('gate4_passed', 0)}
Gate 5 (Confluence): {stats.get('gate5_passed', 0)}
Gate 6 (Entry Quality): {stats.get('gate6_passed', 0)}
"""
            
            message = f"""
HIERARCHICAL SCANNER COMPLETED

Final Statistics:
Total Signals Generated: {self.signals_generated}
Completion: {completion_pct:.1f}%
Average Score: {avg_score:.1f}/100
Average Risk/Reward: {avg_rr:.2f}:1

Gate Statistics:
{gate_stats}

Signal Quality:
High Score (70+): {stats['high_score_signals']}
Medium Score (50-70): {stats['medium_score_signals']}
Low Score (<50): {stats['low_score_signals']}

Scanner Details:
Scan Cycles: {self.scan_cycle}
Pairs Scanned: {stats['pairs_scanned']}
Long Signals: {stats['long_rejections']}
Short Signals: {stats['short_rejections']}

Database: {DB_PATH}

#HierarchicalComplete #LogicalTrading
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message
                })
                
            log.info("✅ Final hierarchical scanner stats sent to Telegram")
                
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
                    "status": "hierarchical_rejection_scanner",
                    "scanner": "Hierarchical Rejection-Based Trading Scanner",
                    "scan_cycle": scanner.scan_cycle,
                    "signals_generated": scanner.signals_generated,
                    "max_signals": scanner.max_signals,
                    "completion_percent": (scanner.signals_generated / scanner.max_signals) * 100,
                    "daily_stats": stats,
                    "mode": "6-gate hierarchical decision making",
                    "tp_sl_method": "logical (not random)"
                }, indent=2)
            
            elif path == '/stats':
                response = json.dumps(scanner.scanner.get_daily_stats(), indent=2)
            
            elif path == '/recent':
                if scanner.db:
                    scanner.db.row_factory = aiosqlite.Row
                    async with scanner.db.execute("""
                        SELECT symbol, side, entry_price, zone_type, total_score, 
                               stop_loss_pct, take_profit_pct, stop_loss_reason, 
                               take_profit_reason, decision_gates_passed, created_at
                        FROM hierarchical_signals 
                        ORDER BY created_at DESC 
                        LIMIT 20
                    """) as cursor:
                        rows = await cursor.fetchall()
                        signals = [dict(row) for row in rows]
                    
                    response = json.dumps({"signals": signals, "count": len(signals)}, indent=2)
                else:
                    response = json.dumps({"error": "Database not available"})
            
            elif path == '/gates':
                stats = scanner.scanner.get_daily_stats()
                gate_stats = {
                    "gate1_wave_context": stats.get('gate1_passed', 0),
                    "gate2_zone": stats.get('gate2_passed', 0),
                    "gate3_rejection": stats.get('gate3_passed', 0),
                    "gate4_tf_confirmation": stats.get('gate4_passed', 0),
                    "gate5_confluence": stats.get('gate5_passed', 0),
                    "gate6_entry_quality": stats.get('gate6_passed', 0)
                }
                response = json.dumps({"gate_statistics": gate_stats}, indent=2)
            
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
    """Main function to run the hierarchical scanner"""
    log.info("=" * 70)
    log.info("🚀 STARTING HIERARCHICAL REJECTION SCANNER")
    log.info("=" * 70)
    
    scanner = HierarchicalCompleteScanner()
    
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
        log.info("Hierarchical scanner stopped by user")
    except Exception as e:
        log.error(f"Fatal error: {e}")