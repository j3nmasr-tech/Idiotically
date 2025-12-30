#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 REJECTION-BASED HIGH-FREQUENCY SCANNER - ENHANCED
Professional discretionary trading system
Wave-length awareness + Strength analysis + Rejection entries
WITH ENHANCED: Multi-timeframe consensus + Full indicators + Strength analysis + Telegram alerts
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
from dataclasses import dataclass, asdict
import json
from collections import defaultdict

# ================ HIGH-FREQUENCY CONFIG ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/enhanced_rejection_scanner.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 5))
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 100))
MIN_VOLUME_USD = 500000

# Trading parameters
MAX_STOP_LOSS_PCT = 1.0
MIN_TARGET_PCT = 1.5
MAX_TARGET_PCT = 6.0
MIN_RISK_REWARD = 2.0

# Rejection scanning
REJECTION_CONFIG = {
    "rsi_long_zone": (40, 50),
    "rsi_short_zone": (50, 60),
    "ema_distance_threshold": 0.5,
    "min_rejection_strength": 0.6,
}

# ================ ENHANCED: ALL TIMEFRAMES FOR MONITORING ================
TIMEFRAMES_ALL = {
    "1D": "1d",      # للتحليل الشامل
    "4H": "4h",      # للاتجاه المتوسط
    "1H": "1h",      # للمدى الموجي
    "15M": "15m",    # للقوة والبنية
    "5M": "5m",      # للزخم
    "3M": "3m",      # للتداول (رئيسي)
    "1M": "1m"       # للتوقيت
}

# ================ ENHANCED: FULL INDICATOR SETTINGS ================
EMA_PERIODS_FULL = [9, 21, 50, 100, 200]  # 5 EMAs للتحليل الشامل
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30
VOLUME_MA_PERIOD = 20

# ================ ENHANCED DATA STRUCTURES ================

@dataclass
class EnhancedWaveAnalysis:
    """تحليل مدى موجي محسن"""
    timeframe: str
    wave_length: str           # SHORT, MEDIUM, LONG
    wave_maturity: float       # 0-1
    wave_stage: str           # ACCUMULATION, EXPANSION, DISTRIBUTION, CORRECTION
    wave_strength: float      # 0-1
    trend_direction: str      # STRONG_BULL, BULL, NEUTRAL, BEAR, STRONG_BEAR
    trend_strength: float     # 0-1
    price_action: str        # IMPULSIVE, CORRECTIVE, COMPRESSION

@dataclass  
class EnhancedIndicatorAnalysis:
    """تحليل مؤشرات محسن"""
    timeframe: str
    # RSI Analysis
    rsi_value: float
    rsi_signal: str          # OVERSOLD, BULLISH, NEUTRAL, BEARISH, OVERBOUGHT
    rsi_divergence: str      # BULLISH_DIVERGENCE, BEARISH_DIVERGENCE, NONE
    rsi_trend: str          # RISING, FALLING, FLAT
    
    # EMA Analysis (5 EMAs)
    ema_alignment: str       # BULLISH_ALIGNMENT, BEARISH_ALIGNMENT, MIXED, NEUTRAL
    price_vs_emas: Dict[int, str]  # ABOVE/BELOW لكل EMA
    ema_distances: Dict[int, float]  # المسافة النسبية
    
    # Volume Analysis
    volume_ratio: float      # نسبة الفوليوم الحالي إلى المتوسط
    volume_trend: str        # RISING, FALLING, NEUTRAL
    volume_spike: bool       # طفرة فوليوم
    volume_signal: str      # HIGH_VOLUME, LOW_VOLUME, NORMAL
    
    # Candle Analysis
    candle_strength: float   # 0-1 قوة الشمعة
    candle_pattern: str      # نمط الشمعة
    candle_signal: str      # BULLISH, BEARISH, NEUTRAL

@dataclass
class EnhancedMarketStrength:
    """تحليل قوة محسن"""
    timeframe: str
    strength_score: float    # 0-100
    momentum_score: float    # 0-100
    volume_score: float      # 0-100
    trend_score: float       # 0-100
    
    strength_level: str      # VERY_WEAK, WEAK, NEUTRAL, STRONG, VERY_STRONG
    momentum_direction: str  # BULLISH, BEARISH, SIDEWAYS
    volume_participation: str # LOW, MODERATE, HIGH, VERY_HIGH
    
    warnings: List[str]
    signals: List[str]

@dataclass
class TimeframeConsensus:
    """إجماع جميع الأطر الزمنية - المكون (أ)"""
    symbol: str
    overall_trend: str      # STRONG_BULL, BULL, NEUTRAL, BEAR, STRONG_BEAR
    trend_confidence: float # 0-100%
    
    bullish_timeframes: List[str]
    bearish_timeframes: List[str]
    neutral_timeframes: List[str]
    
    strength_by_timeframe: Dict[str, float]
    avg_strength: float
    
    dominant_wave: str
    wave_alignment: str
    
    recommendation: str     # STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL
    risk_level: str        # LOW, MEDIUM, HIGH
    
    summary_ar: str
    summary_en: str

@dataclass
class EnhancedAnalysis:
    """التحليل المحسن الكامل - يجمع كل المكونات"""
    symbol: str
    current_price: float
    analysis_time: datetime
    
    # المكون (ب): تحليل جميع المؤشرات لكل إطار
    indicator_analysis: Dict[str, EnhancedIndicatorAnalysis]
    
    # المكون (ج): تحليل القوة لكل إطار
    strength_analysis: Dict[str, EnhancedMarketStrength]
    
    # المكون (أ): إجماع الأطر
    consensus: TimeframeConsensus
    
    # المكون (هـ): التنبيهات والإشارات
    alerts: List[str]
    strong_signals: List[str]
    
    # للنظام الأصلي (Rejection)
    wave_context: Any  # من النظام الأصلي
    rejection_zones: List[Any]
    has_rejection_signal: bool
    rejection_signal: Optional[Any] = None

# ================ ORIGINAL DATA STRUCTURES (KEPT) ================
@dataclass
class WaveContext:
    wave_length: str
    wave_maturity: float
    expansion_speed: float
    structure_type: str
    context_side: str

@dataclass
class MarketStrength:
    candle_speed: float
    distance_ratio: float
    ema_angle: float
    volume_participation: float
    strength_score: float
    is_continuation: bool
    is_rejection_setup: bool
    is_absorption: bool
    is_compression: bool

@dataclass
class RejectionZone:
    zone_type: str
    price_level: float
    strength: float
    volume_confirmation: bool
    rsi_position: str
    is_active: bool

@dataclass
class RejectionSignal:
    signal_id: str
    symbol: str
    side: str
    entry_price: float
    stop_loss: float
    take_profit: float
    wave_context: WaveContext
    market_strength: MarketStrength
    rejection_zone: RejectionZone
    rejection_type: str
    trigger_candle: str
    rsi_at_entry: float
    rejection_strength: float
    risk_reward: float
    expected_move_pct: float
    timeframe_used: str
    signal_timestamp: float
    conditions_met: List[str]

# ================ PROFESSIONAL LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("enhanced_scanner")

# ================ ENHANCED ANALYZER (أ، ب، ج، هـ) ================

class EnhancedAnalyzer:
    """محلل محسن لإضافة المكونات المطلوبة"""
    
    def __init__(self):
        self.history = defaultdict(list)
    
    # ========== المكون (ب): تحليل المؤشرات الكامل ==========
    
    def analyze_indicators_enhanced(self, df: pd.DataFrame, timeframe: str) -> EnhancedIndicatorAnalysis:
        """تحليل كامل للمؤشرات لإطار زمني"""
        try:
            if len(df) < 50:
                return self._default_indicators(timeframe)
            
            # RSI Analysis
            rsi_value, rsi_signal, rsi_divergence, rsi_trend = self._analyze_rsi_full(df)
            
            # EMA Analysis (5 EMAs)
            ema_alignment, price_vs_emas, ema_distances = self._analyze_emas_full(df)
            
            # Volume Analysis
            volume_ratio, volume_trend, volume_spike, volume_signal = self._analyze_volume_full(df)
            
            # Candle Analysis
            candle_strength, candle_pattern, candle_signal = self._analyze_candles_full(df)
            
            return EnhancedIndicatorAnalysis(
                timeframe=timeframe,
                rsi_value=rsi_value,
                rsi_signal=rsi_signal,
                rsi_divergence=rsi_divergence,
                rsi_trend=rsi_trend,
                ema_alignment=ema_alignment,
                price_vs_emas=price_vs_emas,
                ema_distances=ema_distances,
                volume_ratio=volume_ratio,
                volume_trend=volume_trend,
                volume_spike=volume_spike,
                volume_signal=volume_signal,
                candle_strength=candle_strength,
                candle_pattern=candle_pattern,
                candle_signal=candle_signal
            )
            
        except Exception as e:
            log.error(f"Enhanced indicators error ({timeframe}): {e}")
            return self._default_indicators(timeframe)
    
    def _calculate_rsi(self, prices: pd.Series) -> pd.Series:
        """حساب RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=RSI_PERIOD).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=RSI_PERIOD).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    def _analyze_rsi_full(self, df: pd.DataFrame) -> Tuple[float, str, str, str]:
        """تحليل RSI كامل"""
        try:
            rsi = self._calculate_rsi(df['close'])
            
            if len(rsi) < 20:
                return 50, "NEUTRAL", "NONE", "FLAT"
            
            current = rsi.iloc[-1]
            
            # Signal
            if current > 70:
                signal = "OVERBOUGHT"
            elif current > 55:
                signal = "BEARISH"
            elif current < 30:
                signal = "OVERSOLD"
            elif current < 45:
                signal = "BULLISH"
            else:
                signal = "NEUTRAL"
            
            # Divergence
            divergence = self._find_rsi_divergence(df, rsi)
            
            # Trend
            rsi_recent = rsi.values[-5:]
            if len(rsi_recent) >= 2:
                slope = np.polyfit(range(len(rsi_recent)), rsi_recent, 1)[0]
                if slope > 0.5:
                    trend = "RISING"
                elif slope < -0.5:
                    trend = "FALLING"
                else:
                    trend = "FLAT"
            else:
                trend = "FLAT"
            
            return current, signal, divergence, trend
            
        except Exception as e:
            return 50, "NEUTRAL", "NONE", "FLAT"
    
    def _find_rsi_divergence(self, df: pd.DataFrame, rsi: pd.Series) -> str:
        """البحث عن اختلاف RSI"""
        try:
            if len(df) < 25 or len(rsi) < 25:
                return "NONE"
            
            prices = df['close'].values[-25:]
            rsi_values = rsi.values[-25:]
            
            # Simplified divergence detection
            if len(prices) >= 10 and len(rsi_values) >= 10:
                price_slope = np.polyfit(range(10), prices[-10:], 1)[0]
                rsi_slope = np.polyfit(range(10), rsi_values[-10:], 1)[0]
                
                if price_slope > 0 and rsi_slope < -0.5:
                    return "BEARISH_DIVERGENCE"
                elif price_slope < 0 and rsi_slope > 0.5:
                    return "BULLISH_DIVERGENCE"
            
            return "NONE"
            
        except Exception as e:
            return "NONE"
    
    def _analyze_emas_full(self, df: pd.DataFrame) -> Tuple[str, Dict, Dict]:
        """تحليل 5 EMAs"""
        try:
            current = df['close'].iloc[-1]
            price_vs_emas = {}
            ema_distances = {}
            
            for period in EMA_PERIODS_FULL:
                ema = df['close'].ewm(span=period, adjust=False).mean().iloc[-1]
                
                if current > ema * 1.005:
                    price_vs_emas[period] = "ABOVE"
                elif current < ema * 0.995:
                    price_vs_emas[period] = "BELOW"
                else:
                    price_vs_emas[period] = "NEAR"
                
                if ema > 0:
                    distance = abs(current - ema) / ema * 100
                    ema_distances[period] = distance
            
            # Check alignment
            above_count = sum(1 for v in price_vs_emas.values() if v == "ABOVE")
            below_count = sum(1 for v in price_vs_emas.values() if v == "BELOW")
            
            if above_count == len(EMA_PERIODS_FULL):
                alignment = "BULLISH_ALIGNMENT"
            elif below_count == len(EMA_PERIODS_FULL):
                alignment = "BEARISH_ALIGNMENT"
            elif above_count > below_count:
                alignment = "MOSTLY_BULLISH"
            elif below_count > above_count:
                alignment = "MOSTLY_BEARISH"
            else:
                alignment = "MIXED"
            
            return alignment, price_vs_emas, ema_distances
            
        except Exception as e:
            return "MIXED", {}, {}
    
    def _analyze_volume_full(self, df: pd.DataFrame) -> Tuple[float, str, bool, str]:
        """تحليل الفوليوم"""
        try:
            if len(df) < VOLUME_MA_PERIOD:
                return 1.0, "NEUTRAL", False, "NORMAL"
            
            current_volume = df['volume'].iloc[-1]
            avg_volume = df['volume'].rolling(VOLUME_MA_PERIOD).mean().iloc[-1]
            
            volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1.0
            
            # Volume trend
            recent_volumes = df['volume'].values[-10:]
            if len(recent_volumes) >= 3:
                slope = np.polyfit(range(len(recent_volumes)), recent_volumes, 1)[0]
                if slope > 0:
                    volume_trend = "RISING"
                elif slope < 0:
                    volume_trend = "FALLING"
                else:
                    volume_trend = "NEUTRAL"
            else:
                volume_trend = "NEUTRAL"
            
            # Volume spike
            volume_spike = volume_ratio > 2.0
            
            # Volume signal
            if volume_ratio > 2.5:
                volume_signal = "EXTREME_HIGH"
            elif volume_ratio > 1.5:
                volume_signal = "HIGH_VOLUME"
            elif volume_ratio < 0.5:
                volume_signal = "LOW_VOLUME"
            else:
                volume_signal = "NORMAL"
            
            return volume_ratio, volume_trend, volume_spike, volume_signal
            
        except Exception as e:
            return 1.0, "NEUTRAL", False, "NORMAL"
    
    def _analyze_candles_full(self, df: pd.DataFrame) -> Tuple[float, str, str]:
        """تحليل الشموع"""
        try:
            if len(df) < 3:
                return 0.5, "NORMAL", "NEUTRAL"
            
            current = df.iloc[-1]
            
            # Candle strength
            candle_range = current['high'] - current['low']
            if current['close'] > 0:
                strength = min(candle_range / current['close'] * 100 / 10, 1.0)
            else:
                strength = 0.5
            
            # Simple pattern detection
            pattern = "NORMAL"
            if abs(current['close'] - current['open']) / (current['high'] - current['low']) < 0.1:
                pattern = "DOJI"
            elif current['close'] > current['open'] and (current['low'] < current['open'] * 0.99):
                pattern = "HAMMER"
            elif current['close'] < current['open'] and (current['high'] > current['open'] * 1.01):
                pattern = "SHOOTING_STAR"
            
            # Candle signal
            if pattern in ["HAMMER"]:
                candle_signal = "BULLISH"
            elif pattern in ["SHOOTING_STAR"]:
                candle_signal = "BEARISH"
            else:
                candle_signal = "NEUTRAL"
            
            return strength, pattern, candle_signal
            
        except Exception as e:
            return 0.5, "NORMAL", "NEUTRAL"
    
    # ========== المكون (ج): تحليل القوة المحسن ==========
    
    def analyze_strength_enhanced(self, df: pd.DataFrame, timeframe: str) -> EnhancedMarketStrength:
        """تحليل القوة المحسن"""
        try:
            if len(df) < 30:
                return self._default_strength(timeframe)
            
            # Calculate scores
            strength_score = self._calculate_strength_score(df)
            momentum_score = self._calculate_momentum_score(df)
            volume_score = self._calculate_volume_score(df)
            trend_score = self._calculate_trend_score(df)
            
            # Interpretations
            strength_level = self._get_strength_level(strength_score)
            momentum_direction = self._get_momentum_direction(df)
            volume_participation = self._get_volume_participation(volume_score)
            
            # Warnings and signals
            warnings = self._get_warnings(df)
            signals = self._get_signals(df)
            
            return EnhancedMarketStrength(
                timeframe=timeframe,
                strength_score=strength_score,
                momentum_score=momentum_score,
                volume_score=volume_score,
                trend_score=trend_score,
                strength_level=strength_level,
                momentum_direction=momentum_direction,
                volume_participation=volume_participation,
                warnings=warnings,
                signals=signals
            )
            
        except Exception as e:
            log.error(f"Enhanced strength error ({timeframe}): {e}")
            return self._default_strength(timeframe)
    
    def _calculate_strength_score(self, df: pd.DataFrame) -> float:
        """حساب درجة القوة"""
        try:
            factors = []
            
            # Momentum
            momentum = self._get_momentum_strength(df)
            factors.append(momentum * 100)
            
            # Volume
            volume_strength = self._get_volume_strength(df)
            factors.append(volume_strength * 100)
            
            # Trend
            trend_dir, trend_strength = self._get_trend(df)
            factors.append(trend_strength * 100)
            
            # Consistency
            consistency = self._get_consistency(df)
            factors.append(consistency * 100)
            
            return np.mean(factors) if factors else 50.0
            
        except Exception as e:
            return 50.0
    
    def _get_momentum_strength(self, df: pd.DataFrame) -> float:
        """قوة الزخم"""
        try:
            if len(df) < 10:
                return 0.5
            
            recent = df['close'].values[-10:]
            returns = np.diff(np.log(recent))
            momentum = np.mean(returns) * 100
            strength = min(abs(momentum) / 5.0, 1.0)
            return strength
            
        except Exception as e:
            return 0.5
    
    def _get_volume_strength(self, df: pd.DataFrame) -> float:
        """قوة الفوليوم"""
        try:
            if len(df) < VOLUME_MA_PERIOD:
                return 0.5
            
            current = df['volume'].iloc[-1]
            avg = df['volume'].rolling(VOLUME_MA_PERIOD).mean().iloc[-1]
            
            if avg > 0:
                ratio = current / avg
                strength = min((ratio - 0.5) / 1.5, 1.0)
                return max(strength, 0)
            
            return 0.5
            
        except Exception as e:
            return 0.5
    
    def _calculate_momentum_score(self, df: pd.DataFrame) -> float:
        return self._get_momentum_strength(df) * 100
    
    def _calculate_volume_score(self, df: pd.DataFrame) -> float:
        return self._get_volume_strength(df) * 100
    
    def _calculate_trend_score(self, df: pd.DataFrame) -> float:
        _, trend_strength = self._get_trend(df)
        return trend_strength * 100
    
    def _get_trend(self, df: pd.DataFrame) -> Tuple[str, float]:
        """تحليل الاتجاه"""
        try:
            if len(df) < 30:
                return "NEUTRAL", 0.5
            
            ema_9 = df['close'].ewm(span=9, adjust=False).mean().iloc[-1]
            ema_21 = df['close'].ewm(span=21, adjust=False).mean().iloc[-1]
            ema_50 = df['close'].ewm(span=50, adjust=False).mean().iloc[-1]
            
            current = df['close'].iloc[-1]
            
            bull_align = current > ema_9 > ema_21 > ema_50
            bear_align = current < ema_9 < ema_21 < ema_50
            
            recent = df['close'].values[-10:]
            slope, _ = np.polyfit(range(len(recent)), recent, 1)
            avg_price = np.mean(recent)
            
            if avg_price > 0:
                strength = min(abs(slope / avg_price * 1000), 1.0)
            else:
                strength = 0.5
            
            if bull_align and strength > 0.7:
                return "STRONG_BULL", strength
            elif bull_align and strength > 0.4:
                return "BULL", strength
            elif bear_align and strength > 0.7:
                return "STRONG_BEAR", strength
            elif bear_align and strength > 0.4:
                return "BEAR", strength
            else:
                return "NEUTRAL", strength
                
        except Exception as e:
            return "NEUTRAL", 0.5
    
    def _get_consistency(self, df: pd.DataFrame) -> float:
        """الاتساق"""
        try:
            if len(df) < 5:
                return 0.5
            
            closes = df['close'].values[-5:]
            directions = []
            
            for i in range(1, len(closes)):
                if closes[i] > closes[i-1]:
                    directions.append(1)
                elif closes[i] < closes[i-1]:
                    directions.append(-1)
                else:
                    directions.append(0)
            
            if not directions:
                return 0.5
            
            first_dir = directions[0]
            same_count = sum(1 for d in directions if d == first_dir)
            return same_count / len(directions)
            
        except Exception as e:
            return 0.5
    
    def _get_strength_level(self, score: float) -> str:
        """مستوى القوة"""
        if score >= 80:
            return "VERY_STRONG"
        elif score >= 60:
            return "STRONG"
        elif score >= 40:
            return "NEUTRAL"
        elif score >= 20:
            return "WEAK"
        else:
            return "VERY_WEAK"
    
    def _get_momentum_direction(self, df: pd.DataFrame) -> str:
        """اتجاه الزخم"""
        try:
            if len(df) < 5:
                return "SIDEWAYS"
            
            recent = df['close'].values[-5:]
            slope, _ = np.polyfit(range(len(recent)), recent, 1)
            
            if slope > 0.001:
                return "BULLISH"
            elif slope < -0.001:
                return "BEARISH"
            else:
                return "SIDEWAYS"
                
        except Exception as e:
            return "SIDEWAYS"
    
    def _get_volume_participation(self, score: float) -> str:
        """مشاركة الفوليوم"""
        if score >= 80:
            return "VERY_HIGH"
        elif score >= 60:
            return "HIGH"
        elif score >= 40:
            return "MODERATE"
        else:
            return "LOW"
    
    def _get_warnings(self, df: pd.DataFrame) -> List[str]:
        """التحذيرات"""
        warnings = []
        
        try:
            # RSI warnings
            rsi = self._calculate_rsi(df['close'])
            if len(rsi) > 0:
                current = rsi.iloc[-1]
                if current > 70:
                    warnings.append("RSI_OVERBOUGHT")
                elif current < 30:
                    warnings.append("RSI_OVERSOLD")
            
            # Volume warnings
            volume_ratio, _, _, _ = self._analyze_volume_full(df)
            if volume_ratio < 0.3:
                warnings.append("LOW_VOLUME")
            elif volume_ratio > 3.0:
                warnings.append("HIGH_VOLUME")
            
            # Divergence
            divergence = self._find_rsi_divergence(df, rsi)
            if divergence != "NONE":
                warnings.append(divergence)
            
        except Exception as e:
            pass
        
        return warnings
    
    def _get_signals(self, df: pd.DataFrame) -> List[str]:
        """الإشارات"""
        signals = []
        
        try:
            # Volume spike
            _, _, volume_spike, _ = self._analyze_volume_full(df)
            if volume_spike:
                signals.append("VOLUME_SPIKE")
            
            # Candle pattern
            _, pattern, _ = self._analyze_candles_full(df)
            if pattern != "NORMAL":
                signals.append(f"CANDLE_{pattern}")
            
            # EMA alignment
            ema_alignment, _, _ = self._analyze_emas_full(df)
            if ema_alignment in ["BULLISH_ALIGNMENT", "BEARISH_ALIGNMENT"]:
                signals.append(f"EMA_{ema_alignment}")
            
        except Exception as e:
            pass
        
        return signals
    
    # ========== المكون (أ): حساب إجماع الأطر ==========
    
    def calculate_timeframe_consensus(self, symbol: str, 
                                    strength_analysis: Dict[str, EnhancedMarketStrength]) -> TimeframeConsensus:
        """حساب إجماع الأطر الزمنية"""
        
        # تجميع الاتجاهات من تحليل القوة
        bullish_tfs = []
        bearish_tfs = []
        neutral_tfs = []
        
        for tf, strength in strength_analysis.items():
            if strength.momentum_direction == "BULLISH":
                bullish_tfs.append(tf)
            elif strength.momentum_direction == "BEARISH":
                bearish_tfs.append(tf)
            else:
                neutral_tfs.append(tf)
        
        # حساب الاتجاه العام
        bull_count = len(bullish_tfs)
        bear_count = len(bearish_tfs)
        total = bull_count + bear_count + len(neutral_tfs)
        
        if total == 0:
            return self._default_consensus(symbol)
        
        if bull_count == total:
            overall_trend = "STRONG_BULL"
            confidence = 100.0
        elif bear_count == total:
            overall_trend = "STRONG_BEAR"
            confidence = 100.0
        elif bull_count > bear_count:
            if bull_count >= total * 0.7:
                overall_trend = "STRONG_BULL"
            else:
                overall_trend = "BULL"
            confidence = (bull_count / total) * 100
        elif bear_count > bull_count:
            if bear_count >= total * 0.7:
                overall_trend = "STRONG_BEAR"
            else:
                overall_trend = "BEAR"
            confidence = (bear_count / total) * 100
        else:
            overall_trend = "NEUTRAL"
            confidence = 50.0
        
        # حساب القوة
        strength_by_tf = {}
        strengths = []
        for tf, strength in strength_analysis.items():
            strength_by_tf[tf] = strength.strength_score
            strengths.append(strength.strength_score)
        
        avg_strength = np.mean(strengths) if strengths else 50.0
        
        # الموجة المسيطرة (مبسط)
        dominant_wave = "UNKNOWN"
        wave_alignment = "GOOD_ALIGNMENT" if len(set([s.momentum_direction for s in strength_analysis.values()])) <= 2 else "MIXED"
        
        # التوصية
        recommendation, risk_level = self._generate_recommendation(overall_trend, confidence, avg_strength)
        
        # الملخص
        summary_ar, summary_en = self._generate_summaries(symbol, overall_trend, confidence, recommendation, 
                                                         bullish_tfs, bearish_tfs, dominant_wave)
        
        return TimeframeConsensus(
            symbol=symbol,
            overall_trend=overall_trend,
            trend_confidence=confidence,
            bullish_timeframes=bullish_tfs,
            bearish_timeframes=bearish_tfs,
            neutral_timeframes=neutral_tfs,
            strength_by_timeframe=strength_by_tf,
            avg_strength=avg_strength,
            dominant_wave=dominant_wave,
            wave_alignment=wave_alignment,
            recommendation=recommendation,
            risk_level=risk_level,
            summary_ar=summary_ar,
            summary_en=summary_en
        )
    
    def _generate_recommendation(self, trend: str, confidence: float, avg_strength: float) -> Tuple[str, str]:
        """توليد التوصية"""
        if trend in ["STRONG_BULL", "BULL"]:
            if confidence > 80 and avg_strength > 70:
                return "STRONG_BUY", "LOW"
            elif confidence > 60 and avg_strength > 50:
                return "BUY", "MEDIUM"
            else:
                return "HOLD", "MEDIUM"
        
        elif trend in ["STRONG_BEAR", "BEAR"]:
            if confidence > 80 and avg_strength > 70:
                return "STRONG_SELL", "LOW"
            elif confidence > 60 and avg_strength > 50:
                return "SELL", "MEDIUM"
            else:
                return "HOLD", "MEDIUM"
        
        else:  # NEUTRAL
            if avg_strength < 30:
                return "AVOID", "HIGH"
            else:
                return "HOLD", "HIGH"
    
    def _generate_summaries(self, symbol: str, trend: str, confidence: float,
                           recommendation: str, bullish_tfs: List[str], 
                           bearish_tfs: List[str], dominant_wave: str) -> Tuple[str, str]:
        """توليد الملخصات"""
        
        trend_ar = {
            "STRONG_BULL": "📈 صاعد قوي جداً",
            "BULL": "📈 صاعد",
            "NEUTRAL": "⚪ محايد",
            "BEAR": "📉 هابط",
            "STRONG_BEAR": "📉 هابط قوي جداً"
        }
        
        rec_ar = {
            "STRONG_BUY": "🟢 شراء قوي",
            "BUY": "🟢 شراء",
            "HOLD": "⚪ انتظار",
            "SELL": "🔴 بيع",
            "STRONG_SELL": "🔴 بيع قوي",
            "AVOID": "⛔ تجنب"
        }
        
        summary_ar = f"""
📊 **إجماع الأطر لـ {symbol}**

{trend_ar.get(trend, trend)}
• الثقة: {confidence:.1f}%
• التوصية: {rec_ar.get(recommendation, recommendation)}

🔄 **توزيع الأطر:**
• 🟢 صاعد: {len(bullish_tfs)} أطر
• 🔴 هابط: {len(bearish_tfs)} أطر
• ⚪ محايد: {len([tf for tf in TIMEFRAMES_ALL.keys() if tf not in bullish_tfs + bearish_tfs])} أطر

🌊 **الموجة:** {dominant_wave}
"""
        
        summary_en = f"""
📊 **Timeframe Consensus for {symbol}**

{trend} Trend
• Confidence: {confidence:.1f}%
• Recommendation: {recommendation}

🔄 **Distribution:**
• 🟢 Bullish: {len(bullish_tfs)} timeframes
• 🔴 Bearish: {len(bearish_tfs)} timeframes
• ⚪ Neutral: {len([tf for tf in TIMEFRAMES_ALL.keys() if tf not in bullish_tfs + bearish_tfs])} timeframes

🌊 **Wave:** {dominant_wave}
"""
        
        return summary_ar, summary_en
    
    # ========== المكون (هـ): تجميع التنبيهات والإشارات ==========
    
    def collect_alerts_signals(self, strength_analysis: Dict[str, EnhancedMarketStrength]) -> Tuple[List[str], List[str]]:
        """تجميع التنبيهات والإشارات من جميع الأطر"""
        all_alerts = []
        all_signals = []
        
        for tf, strength in strength_analysis.items():
            all_alerts.extend([f"{tf}_{alert}" for alert in strength.warnings])
            all_signals.extend([f"{tf}_{signal}" for signal in strength.signals])
        
        # إزالة التكرارات
        all_alerts = list(set(all_alerts))
        all_signals = list(set(all_signals))
        
        return all_alerts, all_signals
    
    # ========== DEFAULT VALUES ==========
    
    def _default_indicators(self, timeframe: str) -> EnhancedIndicatorAnalysis:
        return EnhancedIndicatorAnalysis(
            timeframe=timeframe,
            rsi_value=50.0,
            rsi_signal="NEUTRAL",
            rsi_divergence="NONE",
            rsi_trend="FLAT",
            ema_alignment="MIXED",
            price_vs_emas={},
            ema_distances={},
            volume_ratio=1.0,
            volume_trend="NEUTRAL",
            volume_spike=False,
            volume_signal="NORMAL",
            candle_strength=0.5,
            candle_pattern="NORMAL",
            candle_signal="NEUTRAL"
        )
    
    def _default_strength(self, timeframe: str) -> EnhancedMarketStrength:
        return EnhancedMarketStrength(
            timeframe=timeframe,
            strength_score=50.0,
            momentum_score=50.0,
            volume_score=50.0,
            trend_score=50.0,
            strength_level="NEUTRAL",
            momentum_direction="SIDEWAYS",
            volume_participation="MODERATE",
            warnings=[],
            signals=[]
        )
    
    def _default_consensus(self, symbol: str) -> TimeframeConsensus:
        return TimeframeConsensus(
            symbol=symbol,
            overall_trend="NEUTRAL",
            trend_confidence=50.0,
            bullish_timeframes=[],
            bearish_timeframes=[],
            neutral_timeframes=[],
            strength_by_timeframe={},
            avg_strength=50.0,
            dominant_wave="UNKNOWN",
            wave_alignment="UNKNOWN",
            recommendation="HOLD",
            risk_level="HIGH",
            summary_ar="لا توجد بيانات كافية",
            summary_en="Insufficient data"
        )

# ================ ORIGINAL REJECTION SCANNER (ENHANCED) ================

class EnhancedRejectionBasedScanner:
    """الماسح الضوئي الأصلي مع المكونات المحسنة"""
    
    class SignalDeduplicator:
        """Prevents duplicate signal generation"""
        
        def __init__(self):
            self.active_signals = {}
            self.signal_status = {}
        
        def should_generate_signal(self, symbol: str, side: str, price: float) -> bool:
            if symbol in self.active_signals:
                signal_id = self.active_signals[symbol]
                if signal_id in self.signal_status:
                    status = self.signal_status[signal_id].get("status", "UNKNOWN")
                    if status != "CLOSED":
                        return False
            return True
        
        def register_signal(self, signal: RejectionSignal):
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
        
        def update_signal_status(self, signal_id: str, status: str):
            if signal_id in self.signal_status:
                self.signal_status[signal_id]["status"] = status
                if status == "CLOSED":
                    symbol = self.signal_status[signal_id]["symbol"]
                    log.info(f"✅ Signal {signal_id[:8]} for {symbol} CLOSED")
        
        def remove_closed_signals(self):
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
    
    def __init__(self):
        self.daily_stats = {
            "rejections_found": 0,
            "long_rejections": 0,
            "short_rejections": 0,
            "pairs_scanned": 0,
            "rejections_filtered": 0,
            "no_strength": 0,
            "no_rejection_zone": 0
        }
        self.deduplicator = self.SignalDeduplicator()
        self.active_signal_ids = set()
        
        # Enhanced components
        self.enhanced_analyzer = EnhancedAnalyzer()
        self.enhanced_analyses = {}  symbol: EnhancedAnalysis
    
    # ========== ORIGINAL METHODS (KEPT) ==========
    
    def analyze_wave_context(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> WaveContext:
        """Original wave context analysis"""
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
    
    def analyze_market_strength(self, df: pd.DataFrame) -> MarketStrength:
        """Original market strength analysis"""
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
    
    def find_rejection_zones(self, df: pd.DataFrame, current_price: float, 
                            rsi_value: float, emas: Dict[str, float]) -> List[RejectionZone]:
        """Original rejection zones analysis"""
        zones = []
        
        try:
            if df is None or len(df) < 20:
                return zones
            
            # EMA rejection zones
            ema_zones = self._find_ema_rejection_zones(current_price, emas)
            zones.extend(ema_zones)
            
            # Range rejection zones
            range_zones = self._find_range_rejection_zones(df, current_price)
            zones.extend(range_zones)
            
            # Failed breakout/breakdown zones
            failed_zones = self._find_failed_break_zones(df, current_price)
            zones.extend(failed_zones)
            
            # Filter to active zones only
            active_zones = [z for z in zones if z.is_active]
            
            return active_zones
            
        except Exception as e:
            log.error(f"Rejection zone error: {e}")
            return []
    
    def generate_rejection_signal(self, multi_tf_data: Dict[str, pd.DataFrame], 
                                 symbol: str) -> Optional[RejectionSignal]:
        """Original rejection signal generation"""
        try:
            tf_1h = multi_tf_data.get("1H")
            tf_15m = multi_tf_data.get("15M")
            tf_5m = multi_tf_data.get("5M")
            tf_3m = multi_tf_data.get("3M")
            
            if tf_15m is None or tf_3m is None:
                return None
            
            if len(tf_15m) < 30 or len(tf_3m) < 20:
                return None
            
            # 1. Analyze wave context
            wave_context = self.analyze_wave_context(tf_1h, tf_15m)
            
            # 2. Analyze market strength
            market_strength = self.analyze_market_strength(tf_15m)
            
            if market_strength.strength_score < 0.4:
                self.daily_stats["no_strength"] += 1
                return None
            
            # 3. Calculate indicators
            current_price = tf_3m['close'].iloc[-1]
            emas = self.calculate_emas(tf_3m)
            
            rsi_series = self.calculate_rsi(tf_3m['close'])
            current_rsi = rsi_series.iloc[-1] if len(rsi_series) > 0 else 50
            
            # 4. Find rejection zones
            rejection_zones = self.find_rejection_zones(tf_3m, current_price, current_rsi, emas)
            
            if not rejection_zones:
                self.daily_stats["no_rejection_zone"] += 1
                return None
            
            # 5. Check volume confirmation
            valid_zones = []
            for zone in rejection_zones:
                zone.volume_confirmation = self._check_volume_confirmation(tf_3m, zone.zone_type)
                if zone.volume_confirmation:
                    valid_zones.append(zone)
            
            if not valid_zones:
                return None
            
            # 6. Select strongest zone
            best_zone = max(valid_zones, key=lambda z: z.strength)
            
            # 7. Determine trade side
            side = None
            if best_zone.zone_type in ["EMA_SUPPORT", "RANGE_LOW", "FAILED_BREAKDOWN"]:
                side = "LONG"
            elif best_zone.zone_type in ["EMA_RESISTANCE", "RANGE_HIGH", "FAILED_BREAKOUT"]:
                side = "SHORT"
            
            if not side:
                return None
            
            # 8. RSI check
            if side == "LONG" and not (40 <= current_rsi <= 50):
                return None
            elif side == "SHORT" and not (50 <= current_rsi <= 60):
                return None
            
            # 9. Deduplication check
            if not self.deduplicator.should_generate_signal(symbol, side, current_price):
                self.daily_stats["rejections_filtered"] += 1
                return None
            
            # 10. Rejection candle analysis
            rejection_type, trigger_candle = self._analyze_rejection_candle(tf_3m, side, best_zone)
            
            if not rejection_type:
                return None
            
            # 11. Calculate entry, SL, TP
            stop_loss_pct = np.random.uniform(0.5, MAX_STOP_LOSS_PCT)
            target_pct = np.random.uniform(MIN_TARGET_PCT, MAX_TARGET_PCT)
            
            if side == "LONG":
                entry_price = best_zone.price_level * 1.001
                stop_loss = entry_price * (1 - stop_loss_pct / 100)
                take_profit = entry_price * (1 + target_pct / 100)
            else:
                entry_price = best_zone.price_level * 0.999
                stop_loss = entry_price * (1 + stop_loss_pct / 100)
                take_profit = entry_price * (1 - target_pct / 100)
            
            risk = abs(entry_price - stop_loss)
            reward = abs(take_profit - entry_price)
            
            if risk == 0:
                return None
            
            risk_reward = reward / risk
            
            if risk_reward < MIN_RISK_REWARD:
                return None
            
            # 12. Rejection strength
            rejection_strength = self._calculate_rejection_strength(
                best_zone, market_strength, wave_context, current_rsi
            )
            
            if rejection_strength < REJECTION_CONFIG["min_rejection_strength"]:
                return None
            
            # 13. Conditions met
            conditions_met = self._get_rejection_conditions(
                wave_context, market_strength, best_zone, rejection_type
            )
            
            # 14. Create signal
            signal_id = hashlib.md5(
                f"{symbol}:{side}:{entry_price:.8f}:{time.time()}:{best_zone.zone_type}".encode()
            ).hexdigest()
            
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
                conditions_met=conditions_met
            )
            
            # 15. Register signal
            self.deduplicator.register_signal(signal)
            self.active_signal_ids.add(signal_id)
            
            # 16. Update stats
            self.daily_stats["rejections_found"] += 1
            if side == "LONG":
                self.daily_stats["long_rejections"] += 1
            else:
                self.daily_stats["short_rejections"] += 1
            
            log.info(f"🎯 REJECTION SIGNAL: {symbol} {side} @ {entry_price:.4f}")
            
            return signal
            
        except Exception as e:
            log.error(f"Rejection signal error for {symbol}: {e}")
            return None
    
    # ========== ENHANCED METHODS (NEW) ==========
    
    async def perform_enhanced_analysis(self, symbol: str, all_timeframe_data: Dict[str, pd.DataFrame]) -> EnhancedAnalysis:
        """إجراء التحليل المحسن"""
        
        try:
            current_price = 0
            indicator_analysis = {}
            strength_analysis = {}
            
            # تحليل كل إطار زمني
            for timeframe, df in all_timeframe_data.items():
                if df is not None and len(df) >= 30:
                    # تحديث السعر
                    if timeframe == "1M" or current_price == 0:
                        current_price = df['close'].iloc[-1]
                    
                    # المكون (ب): تحليل المؤشرات الكامل
                    indicators = self.enhanced_analyzer.analyze_indicators_enhanced(df, timeframe)
                    indicator_analysis[timeframe] = indicators
                    
                    # المكون (ج): تحليل القوة المحسن
                    strength = self.enhanced_analyzer.analyze_strength_enhanced(df, timeframe)
                    strength_analysis[timeframe] = strength
            
            # المكون (أ): حساب إجماع الأطر
            consensus = self.enhanced_analyzer.calculate_timeframe_consensus(symbol, strength_analysis)
            
            # المكون (هـ): تجميع التنبيهات والإشارات
            alerts, strong_signals = self.enhanced_analyzer.collect_alerts_signals(strength_analysis)
            
            # تحليل الرفض (من النظام الأصلي)
            wave_context = None
            rejection_zones = []
            rejection_signal = None
            has_rejection_signal = False
            
            # استخدام بيانات 1H و15M لتحليل المدى الموجي
            if "1H" in all_timeframe_data and "15M" in all_timeframe_data:
                wave_context = self.analyze_wave_context(
                    all_timeframe_data.get("1H"), 
                    all_timeframe_data.get("15M")
                )
            
            # استخدام بيانات 3M للبحث عن مناطق الرفض
            if "3M" in all_timeframe_data:
                tf_3m = all_timeframe_data["3M"]
                if len(tf_3m) >= 20:
                    current_price_tf = tf_3m['close'].iloc[-1]
                    rsi_series = self.calculate_rsi(tf_3m['close'])
                    current_rsi = rsi_series.iloc[-1] if len(rsi_series) > 0 else 50
                    emas = self.calculate_emas(tf_3m)
                    
                    rejection_zones = self.find_rejection_zones(
                        tf_3m, current_price_tf, current_rsi, emas
                    )
                    
                    # توليد إشارة الرفض
                    key_timeframes = {
                        "1H": all_timeframe_data.get("1H"),
                        "15M": all_timeframe_data.get("15M"),
                        "5M": all_timeframe_data.get("5M"),
                        "3M": tf_3m,
                        "1M": all_timeframe_data.get("1M")
                    }
                    
                    rejection_signal = self.generate_rejection_signal(key_timeframes, symbol)
                    has_rejection_signal = rejection_signal is not None
            
            return EnhancedAnalysis(
                symbol=symbol,
                current_price=current_price,
                analysis_time=datetime.now(),
                indicator_analysis=indicator_analysis,
                strength_analysis=strength_analysis,
                consensus=consensus,
                alerts=alerts,
                strong_signals=strong_signals,
                wave_context=wave_context,
                rejection_zones=rejection_zones,
                has_rejection_signal=has_rejection_signal,
                rejection_signal=rejection_signal
            )
            
        except Exception as e:
            log.error(f"Enhanced analysis error for {symbol}: {e}")
            return None
    
    # ========== HELPER METHODS (ORIGINAL) ==========
    
    def calculate_rsi(self, prices: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_emas(self, df: pd.DataFrame) -> Dict[str, float]:
        try:
            emas = {}
            for name, period in {"fast": 9, "medium": 21, "slow": 50}.items():
                ema_series = df['close'].ewm(span=period, adjust=False).mean()
                emas[name] = ema_series.iloc[-1] if len(ema_series) > 0 else 0
            return emas
        except Exception as e:
            return {"fast": 0, "medium": 0, "slow": 0}
    
    def get_daily_stats(self) -> Dict:
        return self.daily_stats
    
    def cleanup_old_signals(self):
        self.deduplicator.remove_closed_signals()
    
    # ========== PRIVATE METHODS (ORIGINAL) ==========
    
    def _analyze_wave_length(self, df: pd.DataFrame) -> Tuple[str, float]:
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
            
            return wave_length, wave_maturity
            
        except Exception as e:
            return "MEDIUM", 0.5
    
    def _analyze_expansion_speed(self, df: pd.DataFrame) -> float:
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
            
        except Exception as e:
            return 0.5
    
    def _determine_structure(self, df: pd.DataFrame) -> str:
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
    
    def _calculate_candle_speed(self, df: pd.DataFrame) -> float:
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
            
        except Exception as e:
            return 0.5
    
    def _calculate_distance_ratio(self, df: pd.DataFrame) -> float:
        try:
            if len(df) < 10:
                return 0.5
            
            prices = df['close'].values[-10:]
            total_distance = abs(prices[-1] - prices[0])
            
            if prices[0] > 0:
                distance_pct = total_distance / prices[0] * 100
                return min(distance_pct / 5.0, 1.0)
            
            return 0.5
            
        except Exception as e:
            return 0.5
    
    def _calculate_ema_angle(self, df: pd.DataFrame) -> float:
        try:
            if len(df) < 20:
                return 0.0
            
            ema_fast = df['close'].ewm(span=9, adjust=False).mean()
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
            
        except Exception as e:
            return 0.0
    
    def _calculate_volume_participation(self, df: pd.DataFrame) -> float:
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
            
        except Exception as e:
            return 0.5
    
    def _calculate_strength_score(self, candle_speed: float, distance_ratio: float, 
                                 ema_angle: float, volume_participation: float) -> float:
        weights = [0.2, 0.2, 0.2, 0.4]
        factors = [candle_speed, distance_ratio, ema_angle, volume_participation]
        return np.average(factors, weights=weights)
    
    def _interpret_strength_patterns(self, df: pd.DataFrame, candle_speed: float, 
                                    volume_participation: float) -> Tuple[bool, bool, bool, bool]:
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
            
        except Exception as e:
            return False, False, False, False
    
    def _find_ema_rejection_zones(self, current_price: float, emas: Dict[str, float]) -> List[RejectionZone]:
        zones = []
        
        try:
            for ema_name, ema_value in emas.items():
                if ema_value == 0:
                    continue
                
                distance_pct = abs(current_price - ema_value) / ema_value * 100
                
                if distance_pct <= REJECTION_CONFIG["ema_distance_threshold"]:
                    if current_price > ema_value:
                        zone_type = "EMA_SUPPORT"
                        is_active = True
                    else:
                        zone_type = "EMA_RESISTANCE"
                        is_active = True
                    
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
                        rsi_position="IN_ZONE",
                        is_active=is_active
                    ))
            
            return zones
            
        except Exception as e:
            return []
    
    def _find_range_rejection_zones(self, df: pd.DataFrame, current_price: float) -> List[RejectionZone]:
        zones = []
        
        try:
            if len(df) < 20:
                return zones
            
            recent_high = df['high'].values[-20:].max()
            recent_low = df['low'].values[-20:].min()
            
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
        zones = []
        
        try:
            if len(df) < 10:
                return zones
            
            recent_high = df['high'].values[-5:].max()
            prev_high = df['high'].values[-10:-5].max()
            
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
            
            recent_low = df['low'].values[-5:].min()
            prev_low = df['low'].values[-10:-5].min()
            
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
    
    def _check_volume_confirmation(self, df: pd.DataFrame, zone_type: str) -> bool:
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
                                     wave: WaveContext, rsi: float) -> float:
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
        
        if zone.zone_type in ["EMA_SUPPORT", "RANGE_LOW", "FAILED_BREAKDOWN"]:
            if 40 <= rsi <= 50:
                rsi_score = 0.9
            elif rsi < 30:
                rsi_score = 0.3
            else:
                rsi_score = 0.5
        else:
            if 50 <= rsi <= 60:
                rsi_score = 0.9
            elif rsi > 70:
                rsi_score = 0.3
            else:
                rsi_score = 0.5
        
        factors.append(rsi_score)
        weights.append(0.25)
        
        return np.average(factors, weights=weights)
    
    def _get_rejection_conditions(self, wave: WaveContext, strength: MarketStrength, 
                                 zone: RejectionZone, rejection_type: str) -> List[str]:
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
        
        conditions.append(f"REJECTION_{rejection_type}")
        
        return conditions
    
    def _get_default_wave_context(self) -> WaveContext:
        return WaveContext(
            wave_length="MEDIUM",
            wave_maturity=0.5,
            expansion_speed=0.5,
            structure_type="COMPRESSION",
            context_side="NEUTRAL"
        )
    
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

# ================ ENHANCED DATABASE ================

class EnhancedDatabase:
    """قاعدة بيانات محسنة"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = None
    
    async def connect(self):
        """الاتصال بقاعدة البيانات"""
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self.connection = await aiosqlite.connect(self.db_path)
            
            await self._create_tables()
            
            log.info(f"✅ تم الاتصال بقاعدة البيانات المحسنة")
            return True
            
        except Exception as e:
            log.error(f"❌ خطأ في الاتصال بقاعدة البيانات: {e}")
            return False
    
    async def _create_tables(self):
        """إنشاء الجداول"""
        
        # جدول التحليلات المحسنة
        await self.connection.execute("""
        CREATE TABLE IF NOT EXISTS enhanced_analyses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            analysis_time TIMESTAMP NOT NULL,
            current_price REAL,
            overall_trend TEXT,
            trend_confidence REAL,
            recommendation TEXT,
            risk_level TEXT,
            
            bullish_timeframes TEXT,
            bearish_timeframes TEXT,
            neutral_timeframes TEXT,
            
            avg_strength REAL,
            dominant_wave TEXT,
            wave_alignment TEXT,
            
            alerts TEXT,
            strong_signals TEXT,
            
            has_rejection_signal BOOLEAN,
            rejection_signal_id TEXT,
            
            analysis_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # جدول إشارات الرفض (الأصلي)
        await self.connection.execute("""
        CREATE TABLE IF NOT EXISTS rejection_signals (
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
        
        await self.connection.commit()
    
    async def save_enhanced_analysis(self, analysis: EnhancedAnalysis) -> bool:
        """حفظ التحليل المحسن"""
        try:
            # Convert lists to JSON strings
            bullish_json = json.dumps(analysis.consensus.bullish_timeframes)
            bearish_json = json.dumps(analysis.consensus.bearish_timeframes)
            neutral_json = json.dumps(analysis.consensus.neutral_timeframes)
            alerts_json = json.dumps(analysis.alerts)
            signals_json = json.dumps(analysis.strong_signals)
            
            # Full analysis JSON
            analysis_json = json.dumps(asdict(analysis), default=str)
            
            # Save enhanced analysis
            cursor = await self.connection.execute("""
                INSERT INTO enhanced_analyses 
                (symbol, analysis_time, current_price, overall_trend, trend_confidence,
                 recommendation, risk_level, bullish_timeframes, bearish_timeframes,
                 neutral_timeframes, avg_strength, dominant_wave, wave_alignment,
                 alerts, strong_signals, has_rejection_signal, rejection_signal_id,
                 analysis_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                analysis.symbol,
                analysis.analysis_time.isoformat(),
                analysis.current_price,
                analysis.consensus.overall_trend,
                analysis.consensus.trend_confidence,
                analysis.consensus.recommendation,
                analysis.consensus.risk_level,
                bullish_json,
                bearish_json,
                neutral_json,
                analysis.consensus.avg_strength,
                analysis.consensus.dominant_wave,
                analysis.consensus.wave_alignment,
                alerts_json,
                signals_json,
                analysis.has_rejection_signal,
                analysis.rejection_signal.signal_id if analysis.rejection_signal else None,
                analysis_json
            ))
            
            # Save rejection signal if exists
            if analysis.rejection_signal:
                await self.save_rejection_signal(analysis.rejection_signal)
            
            await self.connection.commit()
            log.debug(f"✅ تم حفظ التحليل المحسن لـ {analysis.symbol}")
            return True
            
        except Exception as e:
            log.error(f"❌ خطأ في حفظ التحليل المحسن: {e}")
            return False
    
    async def save_rejection_signal(self, signal: RejectionSignal) -> bool:
        """حفظ إشارة الرفض"""
        try:
            await self.connection.execute("""
                INSERT OR REPLACE INTO rejection_signals (
                    id, symbol, side, entry_price, stop_loss, take_profit,
                    wave_length, wave_maturity, expansion_speed, structure_type,
                    candle_speed, distance_ratio, ema_angle, volume_participation, strength_score,
                    zone_type, rejection_strength, rsi_at_entry, rejection_type, trigger_candle,
                    risk_reward, expected_move, timeframe_used,
                    conditions_met
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                json.dumps(signal.conditions_met)
            ))
            
            await self.connection.commit()
            return True
            
        except Exception as e:
            log.error(f"❌ خطأ في حفظ إشارة الرفض: {e}")
            return False
    
    async def close(self):
        """إغلاق الاتصال"""
        if self.connection:
            await self.connection.close()

# ================ ENHANCED TELEGRAM BOT ================

class EnhancedTelegramBot:
    """بوت Telegram محسن"""
    
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)
        
        if self.enabled:
            log.info("✅ Telegram bot enabled")
        else:
            log.warning("⚠️ Telegram bot disabled")
    
    async def send_message(self, text: str, parse_mode: str = "HTML") -> bool:
        """إرسال رسالة"""
        if not self.enabled:
            return False
        
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": True
                })
                
                if response.status_code == 200:
                    return True
                else:
                    log.error(f"❌ خطأ Telegram API: {response.status_code}")
                    return False
                    
        except Exception as e:
            log.error(f"❌ خطأ في إرسال Telegram: {e}")
            return False
    
    def format_enhanced_analysis_message(self, analysis: EnhancedAnalysis) -> str:
        """تنسيق رسالة التحليل المحسن"""
        
        symbol = analysis.symbol
        price = analysis.current_price
        consensus = analysis.consensus
        
        # Trend translation
        trend_map = {
            "STRONG_BULL": "📈 <b>صاعد قوي جداً</b>",
            "BULL": "📈 <b>صاعد</b>",
            "NEUTRAL": "⚪ <b>محايد</b>",
            "BEAR": "📉 <b>هابط</b>",
            "STRONG_BEAR": "📉 <b>هابط قوي جداً</b>"
        }
        
        # Recommendation translation
        rec_map = {
            "STRONG_BUY": "🟢 <b>شراء قوي</b>",
            "BUY": "🟢 <b>شراء</b>",
            "HOLD": "⚪ <b>انتظار</b>",
            "SELL": "🔴 <b>بيع</b>",
            "STRONG_SELL": "🔴 <b>بيع قوي</b>",
            "AVOID": "⛔ <b>تجنب</b>"
        }
        
        # Risk translation
        risk_map = {
            "LOW": "🟢 منخفضة",
            "MEDIUM": "🟡 متوسطة",
            "HIGH": "🔴 عالية"
        }
        
        # Create message
        message = f"""
🎯 <b>تحليل محسن + إشارات رفض</b>

<b>{symbol}</b> | ${price:,.4f}

<b>📊 إجماع الأطر:</b>
{trend_map.get(consensus.overall_trend, consensus.overall_trend)}
• الثقة: <b>{consensus.trend_confidence:.1f}%</b>
• التوصية: {rec_map.get(consensus.recommendation, consensus.recommendation)}
• المخاطرة: {risk_map.get(consensus.risk_level, consensus.risk_level)}

<b>🔄 توزيع الأطر:</b>
• 🟢 <b>صاعد:</b> {len(consensus.bullish_timeframes)} ({', '.join(consensus.bullish_timeframes[:3])})
• 🔴 <b>هابط:</b> {len(consensus.bearish_timeframes)} ({', '.join(consensus.bearish_timeframes[:3])})
• ⚪ <b>محايد:</b> {len(consensus.neutral_timeframes)}

<b>💪 القوة المتوسطة:</b> {consensus.avg_strength:.1f}/100
<b>🌊 الموجة:</b> {consensus.dominant_wave}
"""
        
        # Add alerts if any
        if analysis.alerts:
            message += f"\n<b>⚠️ تنبيهات ({len(analysis.alerts)}):</b>\n"
            for alert in analysis.alerts[:3]:
                message += f"• {alert}\n"
        
        # Add strong signals if any
        if analysis.strong_signals:
            message += f"\n<b>📶 إشارات ({len(analysis.strong_signals)}):</b>\n"
            for signal in analysis.strong_signals[:3]:
                message += f"• {signal}\n"
        
        # Add rejection signal info
        if analysis.has_rejection_signal and analysis.rejection_signal:
            rs = analysis.rejection_signal
            message += f"\n<b>🎯 إشارة رفض:</b>\n"
            message += f"• الجانب: {'🟢 شراء' if rs.side == 'LONG' else '🔴 بيع'}\n"
            message += f"• السعر: ${rs.entry_price:.4f}\n"
            message += f"• منطقة: {rs.rejection_zone.zone_type}\n"
            message += f"• قوة الرفض: {rs.rejection_strength:.1%}\n"
            message += f"• نسبة الربح/المخاطرة: {rs.risk_reward:.1f}:1\n"
        
        # Add timeframe strength
        if consensus.strength_by_timeframe:
            message += "\n<b>💪 قوة الأطر:</b>\n"
            for tf in ["1D", "4H", "1H", "15M", "5M", "3M", "1M"]:
                if tf in consensus.strength_by_timeframe:
                    score = consensus.strength_by_timeframe[tf]
                    message += f"• {tf}: {score:.0f}/100\n"
        
        message += f"\n🕐 <i>آخر تحديث: {analysis.analysis_time.strftime('%H:%M:%S')}</i>"
        message += f"\n\n#{symbol.replace('/', '').replace('-USDT', '')}"
        message += f" #{consensus.recommendation}"
        message += " #نظام_محسن"
        
        return message
    
    async def send_rejection_signal_message(self, signal: RejectionSignal):
        """إرسال رسالة إشارة الرفض"""
        if not self.enabled:
            return
        
        side_emoji = "🟢" if signal.side == "LONG" else "🔴"
        side_text = "شراء" if signal.side == "LONG" else "بيع"
        
        message = f"""
{side_emoji} <b>إشارة رفض - الدخول</b> ⚡

<b>{signal.symbol}</b> | {side_text}

<b>🎯 منطقة الرفض:</b>
• النوع: {signal.rejection_zone.zone_type}
• قوة المنطقة: {signal.rejection_zone.strength:.1%}
• تأكيد الفوليوم: {"✅" if signal.rejection_zone.volume_confirmation else "❌"}

<b>⚡ تفاصيل الرفض:</b>
• نوع الرفض: {signal.rejection_type}
• شمعة الزناد: {signal.trigger_candle}
• قوة الرفض: {signal.rejection_strength:.1%}
• RSI عند الدخول: {signal.rsi_at_entry:.1f}

<b>🔧 التنفيذ:</b>
• سعر الدخول: <code>{signal.entry_price:.6f}</code>
• وقف الخسارة: <code>{signal.stop_loss:.6f}</code>
• هدف الربح: <code>{signal.take_profit:.6f}</code>
• نسبة الربح/المخاطرة: {signal.risk_reward:.1f}:1
• الهدف المتوقع: {signal.expected_move_pct:.1f}%

<b>🧠 السياق:</b>
• الطول الموجي: {signal.wave_context.wave_length}
• البنية: {signal.wave_context.structure_type}
• قوة السوق: {signal.market_strength.strength_score:.1%}

#رفض #{side_text} #دخول
"""
        
        await self.send_message(message)
    
    async def send_startup_message(self):
        """رسالة بدء التشغيل"""
        if not self.enabled:
            return
        
        message = """
🚀 <b>بدء النظام المحسن للتداول بالرفض</b>

<b>🎯 المهام الرئيسية:</b>
1️⃣ <b>النظام الأصلي:</b> تداول بالرفض عالي التردد
2️⃣ <b>المكونات المحسنة:</b>
   • (أ) إجماع جميع الأطر الزمنية
   • (ب) تحليل كامل للمؤشرات (5 EMAs + RSI + Volume)
   • (ج) تحليل القوة الشامل
   • (هـ) نظام التنبيهات والإشارات

<b>⚡ خصائص النظام:</b>
• تردد المسح: كل 5 ثواني
• عدد الأطر: 7 أطر (1D إلى 1M)
• المؤشرات: RSI, 5 EMAs, Volume analysis
• نظام الرفض: مناطق EMA, Range, Failed breaks

<b>🔔 الإشعارات:</b>
• تحليل شامل كل تحديث
• إشارات رفح فورية
• تنبيهات المؤشرات

#نظام_محسن #تداول_الرفض #مراقبة_شاملة
"""
        
        await self.send_message(message)

# ================ ENHANCED MAIN SCANNER ================

class EnhancedRejectionScanner:
    """النظام الرئيسي المحسن"""
    
    def __init__(self):
        self.scanner = EnhancedRejectionBasedScanner()
        self.exchange = None
        self.db = EnhancedDatabase(DB_PATH)
        self.telegram = EnhancedTelegramBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
        self.scan_cycle = 0
        self.monitoring = False
    
    async def initialize(self):
        """تهيئة النظام"""
        log.info("=" * 70)
        log.info("🔥 ENHANCED REJECTION-BASED SCANNER")
        log.info("=" * 70)
        log.info("ORIGINAL: Rejection-based high-frequency trading")
        log.info("ENHANCED: + Timeframe consensus + Full indicators + Strength analysis + Alerts")
        log.info("=" * 70)
        
        # Initialize database
        await self.db.connect()
        
        # Initialize exchange
        await self._init_exchange()
        
        # Send startup message
        await self.telegram.send_startup_message()
        
        return True
    
    async def _init_exchange(self):
        """تهيئة Exchange"""
        try:
            self.exchange = ccxt.okx({
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
                "timeout": 20000,
                "rateLimit": 50
            })
            
            ticker = await self.exchange.fetch_ticker("BTC/USDT")
            log.info(f"✅ Exchange connected. BTC: ${ticker['last']:.2f}")
            
            return True
            
        except Exception as e:
            log.error(f"Exchange error: {e}")
            return False
    
    async def fetch_all_timeframe_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """جلب بيانات جميع الأطر"""
        data = {}
        
        for tf_name, tf in TIMEFRAMES_ALL.items():
            try:
                # Adjust limits
                if tf_name in ["1D", "4H"]:
                    limit = 100
                elif tf_name in ["1H", "15M"]:
                    limit = 80
                else:
                    limit = 60
                
                ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
                
                if ohlcv and len(ohlcv) >= 30:
                    df = pd.DataFrame(
                        ohlcv,
                        columns=["timestamp", "open", "high", "low", "close", "volume"]
                    )
                    
                    for col in ["open", "high", "low", "close", "volume"]:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    df = df.dropna()
                    
                    if len(df) >= 30:
                        data[tf_name] = df
                    
            except Exception as e:
                log.debug(f"{symbol} {tf_name}: {str(e)[:50]}")
                continue
        
        return data
    
    async def get_active_pairs(self) -> List[Tuple[str, float]]:
        """الحصول على أزواج التداول النشطة"""
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
    
    async def perform_enhanced_scan(self):
        """إجراء مسح محسن"""
        self.scan_cycle += 1
        start_time = time.time()
        
        log.info(f"🔄 Scan cycle #{self.scan_cycle} (Enhanced)")
        
        # Get active pairs
        pairs = await self.get_active_pairs()
        
        if not pairs:
            log.warning("No active pairs found")
            return
        
        log.info(f"Scanning {len(pairs)} pairs")
        
        signals_found = 0
        enhanced_analyses = 0
        
        for symbol, volume in pairs:
            try:
                # Fetch all timeframe data
                all_data = await self.fetch_all_timeframe_data(symbol)
                
                # Need minimum data for analysis
                required_tfs = ["1H", "15M", "3M"]
                has_min_data = all(tf in all_data for tf in required_tfs)
                
                if not has_min_data:
                    continue
                
                # Perform enhanced analysis
                enhanced_analysis = await self.scanner.perform_enhanced_analysis(symbol, all_data)
                
                if enhanced_analysis:
                    # Save to database
                    await self.db.save_enhanced_analysis(enhanced_analysis)
                    
                    # Send Telegram alert for important signals
                    if enhanced_analysis.has_rejection_signal or enhanced_analysis.alerts:
                        message = self.telegram.format_enhanced_analysis_message(enhanced_analysis)
                        await self.telegram.send_message(message)
                    
                    # Send rejection signal separately if exists
                    if enhanced_analysis.has_rejection_signal and enhanced_analysis.rejection_signal:
                        await self.telegram.send_rejection_signal_message(enhanced_analysis.rejection_signal)
                        signals_found += 1
                    
                    enhanced_analyses += 1
                
                # Small delay between pairs
                await asyncio.sleep(0.05)
                
            except Exception as e:
                log.debug(f"Pair error {symbol}: {str(e)[:50]}")
                continue
        
        # Update stats
        self.scanner.daily_stats["pairs_scanned"] += len(pairs)
        
        # Log results
        scan_duration = time.time() - start_time
        active_signals = len(self.scanner.deduplicator.active_signals)
        
        log.info(f"📊 Enhanced scan results:")
        log.info(f"   Analyses: {enhanced_analyses}, Rejection signals: {signals_found}")
        log.info(f"   Active signals: {active_signals}")
        log.info(f"   Duration: {scan_duration:.2f}s")
        
        # Cleanup
        self.scanner.cleanup_old_signals()
    
    async def monitor_positions(self):
        """مراقبة الصفقات"""
        log.info("👀 Starting position monitoring...")
        
        while self.monitoring:
            try:
                # Get open positions from database
                async with self.db.connection.execute("""
                    SELECT id, symbol, side, entry_price, stop_loss, take_profit, status
                    FROM rejection_signals 
                    WHERE status IN ('PENDING', 'TRIGGERED')
                """) as cursor:
                    positions = await cursor.fetchall()
                
                for pos_id, symbol, side, entry, sl, tp, status in positions:
                    try:
                        # Get current price
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        # Check for trigger
                        if status == 'PENDING':
                            if abs(current_price - entry) / entry <= 0.005:
                                # Mark as triggered
                                await self.db.connection.execute("""
                                    UPDATE rejection_signals SET 
                                        status = 'TRIGGERED',
                                        triggered_at = CURRENT_TIMESTAMP,
                                        trigger_price = ?
                                    WHERE id = ?
                                """, (current_price, pos_id))
                                
                                await self.db.connection.commit()
                                
                                self.scanner.deduplicator.update_signal_status(pos_id, "TRIGGERED")
                                
                                log.info(f"✅ Position triggered: {symbol} {side} @ {current_price:.4f}")
                        
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
                        
                        else:  # SHORT
                            if current_price >= sl:
                                close_reason = "SL_HIT"
                                pnl_percent = ((entry - current_price) / entry) * 100
                            elif current_price <= tp:
                                close_reason = "TP_HIT"
                                pnl_percent = ((entry - current_price) / entry) * 100
                        
                        if close_reason:
                            # Update database
                            await self.db.connection.execute("""
                                UPDATE rejection_signals SET 
                                    status = 'CLOSED',
                                    closed_at = CURRENT_TIMESTAMP,
                                    close_price = ?,
                                    pnl_percent = ?,
                                    close_reason = ?
                                WHERE id = ?
                            """, (current_price, pnl_percent, close_reason, pos_id))
                            
                            await self.db.connection.commit()
                            
                            self.scanner.deduplicator.update_signal_status(pos_id, "CLOSED")
                            self.scanner.active_signal_ids.discard(pos_id)
                            
                            log.info(f"📤 Position closed: {symbol} {side} {pnl_percent:.2f}% ({close_reason})")
                    
                    except Exception as e:
                        log.error(f"Monitor error for {symbol}: {e}")
                        continue
                
                # Wait before next check
                await asyncio.sleep(2)
                
            except Exception as e:
                log.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(5)
    
    async def run(self):
        """تشغيل النظام"""
        try:
            await self.initialize()
            self.monitoring = True
            
            # Run both scanning and monitoring
            await asyncio.gather(
                self._scanning_loop(),
                self.monitor_positions()
            )
            
        except KeyboardInterrupt:
            log.info("Enhanced scanner stopped by user")
        except Exception as e:
            log.error(f"Scanner crashed: {e}")
        finally:
            self.monitoring = False
            await self.cleanup()
    
    async def _scanning_loop(self):
        """حلقة المسح"""
        while self.monitoring:
            try:
                await self.perform_enhanced_scan()
                
                # Wait for next scan
                wait_time = max(0.1, SCAN_INTERVAL)
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                log.error(f"Scanning loop error: {e}")
                await asyncio.sleep(10)
    
    async def cleanup(self):
        """تنظيف الموارد"""
        try:
            if self.exchange:
                await self.exchange.close()
                log.info("Exchange closed")
            
            await self.db.close()
            log.info("Database closed")
                
        except Exception as e:
            log.error(f"Cleanup error: {e}")

# ================ MAIN ================

async def main():
    """الدالة الرئيسية"""
    scanner = EnhancedRejectionScanner()
    await scanner.run()

if __name__ == "__main__":
    asyncio.run(main())