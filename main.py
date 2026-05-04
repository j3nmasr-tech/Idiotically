#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT SCANNER v6.1 - WAVE RANGE + MOMENTUM BREAKOUT + SCALP ENGINE
Multi-Timeframe Explosive Move Detection System
PRIMARY METHOD: SMA Trend → Wave ABC Correction → RSI Divergence → MACD Cross → Volume Breakout
With Fast Momentum Scalps + Order‑Book Filter + Live Outcome Alerts
"""

import os
import time
import asyncio
import logging
import datetime
import json
import math
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI
import uvicorn
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, field
from enum import Enum
from collections import deque

# ============ ENUMS ============
class TrendBias(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"

class WavePattern(str, Enum):
    ABC_CORRECTION = "ABC_CORRECTION"
    FALLING_WEDGE = "FALLING_WEDGE"
    RISING_WEDGE = "RISING_WEDGE"
    BULL_FLAG = "BULL_FLAG"
    BEAR_FLAG = "BEAR_FLAG"
    NONE = "NONE"

class DivergenceType(str, Enum):
    BULLISH_REGULAR = "BULLISH_REGULAR"
    BEARISH_REGULAR = "BEARISH_REGULAR"
    HIDDEN_BULLISH = "HIDDEN_BULLISH"
    HIDDEN_BEARISH = "HIDDEN_BEARISH"
    NONE = "NONE"

class DirectionTier(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

class TrappedSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NONE = "NONE"
    CONFLICT = "CONFLICT"

class MicroConfirmationType(str, Enum):
    WICK_REJECTION = "WICK_REJECTION"
    ABSORPTION = "ABSORPTION"
    BREAKOUT = "BREAKOUT"
    NONE = "NONE"

class SignalTier(str, Enum):
    S_PLUS = "S+"
    A_PLUS = "A+"
    A = "A"
    B = "B"
    C = "C"
    D = "D"

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH = os.getenv("DB_PATH", "/app/data/romeopt_v6_1.db")

# Scanner settings
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 45))
TOP_N = int(os.getenv("TOP_N", 80))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 1))

# Wave Momentum Engine thresholds
MIN_FIB_RETRACEMENT = float(os.getenv("MIN_FIB_RETRACEMENT", 0.5))
OPTIMAL_FIB_ZONE_MIN = float(os.getenv("OPTIMAL_FIB_ZONE_MIN", 0.618))
OPTIMAL_FIB_ZONE_MAX = float(os.getenv("OPTIMAL_FIB_ZONE_MAX", 0.705))
MIN_DIVERGENCE_STRENGTH = float(os.getenv("MIN_DIVERGENCE_STRENGTH", 0.6))
VOLUME_SPIKE_MULTIPLIER = float(os.getenv("VOLUME_SPIKE_MULTIPLIER", 2.0))

# Direction Engine thresholds
MIN_DIRECTION_CONFIDENCE = float(os.getenv("MIN_DIRECTION_CONFIDENCE", 0.4))
FUNDING_EXTREME_THRESHOLD = float(os.getenv("FUNDING_EXTREME_THRESHOLD", 0.03))
OI_ACCUMULATION_THRESHOLD = float(os.getenv("OI_ACCUMULATION_THRESHOLD", 0.15))

# Signal thresholds
MIN_QUALITY_SCORE = float(os.getenv("MIN_QUALITY_SCORE", 0.5))

# Deduplication settings
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", 15))
SIGNAL_VALIDITY_HOURS = int(os.getenv("SIGNAL_VALIDITY_HOURS", 48))

# Rate limiting settings
MAX_REQUESTS_PER_SECOND = int(os.getenv("MAX_REQUESTS_PER_SECOND", 4))
RATE_LIMIT_RETRIES = int(os.getenv("RATE_LIMIT_RETRIES", 3))
RATE_LIMIT_BACKOFF_FACTOR = float(os.getenv("RATE_LIMIT_BACKOFF_FACTOR", 2.5))

# Outcome monitor settings
OUTCOME_CHECK_INTERVAL = int(os.getenv("OUTCOME_CHECK_INTERVAL", 30))

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("romeopt_v6_1")

# ============ DATA STRUCTURES ============
@dataclass
class WaveStructure:
    """Multi-timeframe wave analysis"""
    pattern: WavePattern = WavePattern.NONE
    pattern_confidence: float = 0.0
    
    # Impulse wave (the main move)
    impulse_start: float = 0.0
    impulse_end: float = 0.0
    impulse_size_pct: float = 0.0
    
    # Corrective wave (the setup zone)
    correction_start: float = 0.0
    correction_end: float = 0.0
    correction_size_pct: float = 0.0
    
    # Fibonacci retracement
    fib_236: float = 0.0
    fib_382: float = 0.0
    fib_500: float = 0.0
    fib_618: float = 0.0
    fib_705: float = 0.0
    fib_786: float = 0.0
    current_retracement: float = 0.0
    
    # Entry zone
    in_optimal_zone: bool = False
    distance_to_zone_pct: float = 999.0
    zone_price_high: float = 0.0
    zone_price_low: float = 0.0
    
    # Structure details
    swing_points: List[Dict] = field(default_factory=list)
    candle_count: int = 0

@dataclass
class MomentumSignals:
    """RSI divergence + MACD momentum signals"""
    # RSI Divergence
    divergence_type: DivergenceType = DivergenceType.NONE
    divergence_strength: float = 0.0
    divergence_points: List[Dict] = field(default_factory=list)
    
    # RSI values
    rsi_current: float = 50.0
    rsi_at_price_low: float = 50.0
    rsi_at_price_high: float = 50.0
    
    # MACD
    macd_crossed: bool = False
    macd_cross_direction: str = ""
    macd_histogram_reversal: bool = False
    macd_line: float = 0.0
    macd_signal_line: float = 0.0
    macd_histogram: float = 0.0
    
    # Combined
    momentum_score: float = 0.0
    momentum_aligned: bool = False

@dataclass
class VolumeBreakout:
    """Volume-based entry trigger"""
    triggered: bool = False
    breakout_candle_volume: float = 0.0
    avg_volume_20: float = 0.0
    volume_ratio: float = 0.0
    breakout_direction: str = ""
    breakout_price: float = 0.0
    pattern_break: bool = False
    fakeout_detected: bool = False
    sweep_then_reclaim: bool = False
    volume_score: float = 0.0

@dataclass
class InstitutionalData:
    """Exchange-specific institutional data"""
    open_interest: float = 0.0
    oi_change_24h: float = 0.0
    oi_change_1h: float = 0.0
    oi_timestamp: Optional[datetime.datetime] = None
    
    funding_rate: float = 0.0
    funding_history: List[float] = field(default_factory=list)
    funding_timestamp: Optional[datetime.datetime] = None
    
    basis_rate: float = 0.0
    perpetual_premium: float = 0.0
    
    top_bid_size: float = 0.0
    top_ask_size: float = 0.0
    bid_ask_ratio: float = 0.0
    
    liquidation_zones: Dict[str, List[float]] = field(default_factory=dict)
    
    @property
    def is_funding_extreme(self) -> bool:
        return abs(self.funding_rate) > FUNDING_EXTREME_THRESHOLD
    
    @property
    def funding_bleeding_side(self) -> str:
        if self.funding_rate > FUNDING_EXTREME_THRESHOLD:
            return "LONG"
        elif self.funding_rate < -FUNDING_EXTREME_THRESHOLD:
            return "SHORT"
        return ""

@dataclass
class DirectionMetrics:
    """Institutional direction signals (now secondary)"""
    trapped_side: TrappedSide = TrappedSide.NONE
    trapped_confidence: float = 0.0
    trapped_details: List[Dict] = field(default_factory=list)
    
    bleeding_side: str = ""
    funding_extreme: float = 0.0
    funding_analysis: Dict = field(default_factory=dict)
    
    micro_confirmation: bool = False
    micro_timeframe: str = ""
    rejection_type: MicroConfirmationType = MicroConfirmationType.NONE
    micro_details: Dict = field(default_factory=dict)
    
    orderbook_imbalance: float = 0.0
    
    direction_score: float = 0.0
    confidence_tier: DirectionTier = DirectionTier.LOW
    
    conflict_warnings: List[str] = field(default_factory=list)
    
    @property
    def is_high_confidence(self) -> bool:
        return (self.confidence_tier == DirectionTier.HIGH and 
                abs(self.direction_score) > 0.7)
    
    @property
    def has_major_conflicts(self) -> bool:
        return len(self.conflict_warnings) >= 2

@dataclass
class EnhancedSetup:
    """Complete v6.0 setup with all analysis layers"""
    symbol: str = ""
    timestamp: datetime.datetime = field(default_factory=datetime.datetime.utcnow)
    side: str = ""
    current_price: float = 0.0
    entry_price: float = 0.0
    entry_type: str = ""
    sl_price: float = 0.0
    tp_targets: List[float] = field(default_factory=list)
    tp_sources: List[Dict] = field(default_factory=list)
    risk: float = 0.0
    reward: float = 0.0
    rr_ratio: float = 0.0
    
    # New v6.0 layers
    trend_bias: TrendBias = TrendBias.NEUTRAL
    wave_structure: WaveStructure = field(default_factory=WaveStructure)
    momentum_signals: MomentumSignals = field(default_factory=MomentumSignals)
    volume_breakout: VolumeBreakout = field(default_factory=VolumeBreakout)
    
    quality_tier: str = ""
    quality_score: float = 0.0
    eight_steps_status: Dict = field(default_factory=dict)
    
    liquidity_analysis: Dict = field(default_factory=dict)
    direction_metrics: DirectionMetrics = field(default_factory=DirectionMetrics)
    
    @property
    def weighted_score(self) -> float:
        base_score = self.quality_score
        direction_bonus = abs(self.direction_metrics.direction_score) * 0.15 * base_score
        wave_bonus = self.wave_structure.pattern_confidence * 0.25 * base_score
        momentum_bonus = self.momentum_signals.momentum_score * 0.2 * base_score
        volume_bonus = self.volume_breakout.volume_score * 0.15 * base_score
        return base_score + direction_bonus + wave_bonus + momentum_bonus + volume_bonus
    
    @property
    def forced_move_probability(self) -> str:
        score = (
            (1.0 if self.wave_structure.in_optimal_zone else 0.0) * 0.3 +
            self.momentum_signals.momentum_score * 0.25 +
            self.volume_breakout.volume_score * 0.25 +
            (1.0 if self.momentum_signals.momentum_aligned else 0.0) * 0.2
        )
        if score > 0.7:
            return "HIGH"
        elif score > 0.5:
            return "MODERATE"
        return "LOW"

@dataclass
class SetupEligibility:
    eligible: bool = False
    side: str = ""
    entry_price: float = 0.0
    entry_type: str = ""
    disqualify_reason: str = ""

@dataclass
class LiquiditySetup:
    sl_price: float = 0.0
    tp_targets: List[float] = field(default_factory=list)
    tp_sources: List[Dict] = field(default_factory=list)
    liquidity_analysis: Dict = field(default_factory=dict)
    rr_ratio: float = 0.0

@dataclass
class SetupQuality:
    sweep_strength: float = 0.0
    structure_shift: bool = False
    from_liquidity_exists: bool = False
    confirmation_candle: bool = False
    htfc_alignment_score: float = 0.0
    total_score: float = 0.0
    eight_steps_status: Dict = field(default_factory=dict)
    
    @property
    def quality_tier(self) -> str:
        if self.total_score >= 4.5:
            return "S+"
        elif self.total_score >= 4.0:
            return "A+"
        elif self.total_score >= 3.0:
            return "A"
        elif self.total_score >= 2.5:
            return "B"
        else:
            return "C"

# ---------------- ENHANCED RATE LIMITER ----------------
class EnhancedRateLimiter:
    def __init__(self):
        self.max_rps = MAX_REQUESTS_PER_SECOND
        self.max_concurrent = MAX_CONCURRENT
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.general_requests = []
        self.funding_requests = []
        self.oi_requests = []
        self.min_delay = 0.25
        self.backoff_factor = RATE_LIMIT_BACKOFF_FACTOR
        self.max_retries = RATE_LIMIT_RETRIES
        
    async def wait_for_endpoint(self, endpoint_type: str = "general"):
        now = time.time()
        if endpoint_type == "funding":
            request_list = self.funding_requests
            cooldown = 1.5
        elif endpoint_type == "oi":
            request_list = self.oi_requests
            cooldown = 2.0
        else:
            request_list = self.general_requests
            cooldown = 1.0
        
        request_list[:] = [t for t in request_list if now - t < cooldown]
        
        if len(request_list) >= 1:
            wait_time = cooldown - (now - request_list[0])
            if wait_time > 0:
                wait_time += np.random.uniform(0.1, 0.3)
                await asyncio.sleep(wait_time)
        
        request_list.append(now)
        await asyncio.sleep(0.1)
    
    async def execute_with_backoff(self, func, *args, endpoint_type="general", **kwargs):
        async with self.semaphore:
            for attempt in range(self.max_retries):
                try:
                    await self.wait_for_endpoint(endpoint_type)
                    result = await func(*args, **kwargs)
                    extra_delay = {
                        "funding": 0.15,
                        "oi": 0.2,
                        "general": 0.05
                    }.get(endpoint_type, 0.05)
                    await asyncio.sleep(extra_delay)
                    return result
                except Exception as e:
                    error_str = str(e)
                    if any(phrase in error_str for phrase in ["Too Many Requests", "50011", "429", "rate limit"]):
                        wait_time = self.min_delay * (self.backoff_factor ** attempt)
                        wait_time += np.random.uniform(0.2, 0.5)
                        log.warning(f"Rate limited on {endpoint_type}, attempt {attempt+1}/{self.max_retries}, waiting {wait_time:.2f}s")
                        await asyncio.sleep(wait_time)
                    else:
                        raise e
            raise Exception(f"Failed after {self.max_retries} retries")

rate_limiter = EnhancedRateLimiter()

# ---------------- UTILS ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 100):
    try:
        result = await rate_limiter.execute_with_backoff(
            exchange.fetch_ohlcv, symbol, timeframe=timeframe, limit=limit
        )
        return result
    except Exception as e:
        log.debug(f"Failed to fetch {symbol} {timeframe}: {e}")
        return None

def create_dataframe(ohlcv):
    if not ohlcv:
        return None
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

async def safe_fetch_ticker(exchange, symbol: str):
    try:
        return await rate_limiter.execute_with_backoff(exchange.fetch_ticker, symbol)
    except Exception as e:
        log.debug(f"Failed to fetch ticker for {symbol}: {e}")
        return None

async def safe_fetch_tickers(exchange):
    try:
        return await rate_limiter.execute_with_backoff(exchange.fetch_tickers)
    except Exception as e:
        log.debug(f"Failed to fetch tickers: {e}")
        return {}

# ============ WAVE RANGE DETECTOR ============
class WaveRangeDetector:
    """
    Multi-timeframe wave structure analysis
    Core method: Identify ABC corrections into optimal Fibonacci zones
    """
    
    def __init__(self):
        self.min_wave_size_pct = 2.0  # Minimum impulse wave size
        self.max_correction_pct = 0.80  # Max correction (structure invalidation)
    
    def detect_trend_bias(self, df_daily, df_4h) -> Tuple[TrendBias, float]:
        """
        Step 1: Determine macro direction using SMA 50/200
        Price must be clearly above or below both SMAs
        """
        if df_daily is None or df_4h is None:
            return TrendBias.NEUTRAL, 0.0
        
        try:
            # Daily SMA calculation
            df_daily_copy = df_daily.copy()
            df_daily_copy['sma_50'] = df_daily_copy['close'].rolling(window=50).mean()
            df_daily_copy['sma_200'] = df_daily_copy['close'].rolling(window=200).mean()
            
            current_price = df_daily_copy['close'].iloc[-1]
            sma_50_daily = df_daily_copy['sma_50'].iloc[-1]
            sma_200_daily = df_daily_copy['sma_200'].iloc[-1]
            
            # 4H SMA confirmation
            df_4h_copy = df_4h.copy()
            df_4h_copy['sma_50'] = df_4h_copy['close'].rolling(window=50).mean()
            df_4h_copy['sma_200'] = df_4h_copy['close'].rolling(window=200).mean()
            
            sma_50_4h = df_4h_copy['sma_50'].iloc[-1]
            sma_200_4h = df_4h_copy['sma_200'].iloc[-1]
            price_4h = df_4h_copy['close'].iloc[-1]
            
            # Bullish: Price above both SMAs on both timeframes
            daily_bullish = current_price > sma_50_daily and current_price > sma_200_daily
            h4_bullish = price_4h > sma_50_4h and price_4h > sma_200_4h
            
            # Bearish: Price below both SMAs on both timeframes
            daily_bearish = current_price < sma_50_daily and current_price < sma_200_daily
            h4_bearish = price_4h < sma_50_4h and price_4h < sma_200_4h
            
            # Score based on alignment
            alignment_score = 0.0
            
            if daily_bullish and h4_bullish:
                alignment_score = 1.0
                return TrendBias.BULLISH, alignment_score
            elif daily_bearish and h4_bearish:
                alignment_score = 1.0
                return TrendBias.BEARISH, alignment_score
            elif daily_bullish and price_4h > sma_50_4h:
                alignment_score = 0.7
                return TrendBias.BULLISH, alignment_score
            elif daily_bearish and price_4h < sma_50_4h:
                alignment_score = 0.7
                return TrendBias.BEARISH, alignment_score
            elif current_price > sma_50_daily and current_price > sma_200_daily:
                alignment_score = 0.5
                return TrendBias.BULLISH, alignment_score
            elif current_price < sma_50_daily and current_price < sma_200_daily:
                alignment_score = 0.5
                return TrendBias.BEARISH, alignment_score
            
            return TrendBias.NEUTRAL, 0.0
            
        except Exception as e:
            log.debug(f"Trend bias detection error: {e}")
            return TrendBias.NEUTRAL, 0.0
    
    def identify_abc_correction(self, df_4h, trend_bias: TrendBias) -> WaveStructure:
        """
        Step 2: Identify ABC corrective wave pattern on 4H
        For BULLISH trend: Look for 3-wave downward correction (A-B-C)
        For BEARISH trend: Look for 3-wave upward correction (A-B-C)
        """
        wave = WaveStructure()
        
        if df_4h is None or len(df_4h) < 30:
            return wave
        
        try:
            highs = df_4h['high'].values
            lows = df_4h['low'].values
            closes = df_4h['close'].values
            
            if trend_bias == TrendBias.BULLISH:
                return self._identify_bullish_correction(df_4h, highs, lows, closes)
            elif trend_bias == TrendBias.BEARISH:
                return self._identify_bearish_correction(df_4h, highs, lows, closes)
            
        except Exception as e:
            log.debug(f"ABC correction detection error: {e}")
        
        return wave
    
    def _identify_bullish_correction(self, df_4h, highs, lows, closes) -> WaveStructure:
        """Find impulse up + ABC correction down"""
        wave = WaveStructure()
        
        try:
            # Find local maxima as potential swing highs
            swing_highs = self._find_swing_points(highs, is_high=True, window=3)
            swing_lows = self._find_swing_points(lows, is_high=False, window=3)
            
            if len(swing_highs) < 1 or len(swing_lows) < 2:
                return wave
            
            # Most recent major swing high (potential end of impulse)
            recent_swing_highs = sorted(swing_highs, key=lambda x: x['index'])
            recent_swing_lows = sorted(swing_lows, key=lambda x: x['index'])
            
            if len(recent_swing_highs) < 1 or len(recent_swing_lows) < 1:
                return wave
            
            # Find impulse wave: significant move up
            for sh in reversed(recent_swing_highs[-5:]):
                impulse_end = sh['price']
                impulse_end_idx = sh['index']
                
                # Find the lowest low before this high
                prev_lows = [sl for sl in recent_swing_lows if sl['index'] < impulse_end_idx]
                if not prev_lows:
                    continue
                
                impulse_start = prev_lows[-1]['price']
                impulse_start_idx = prev_lows[-1]['index']
                
                impulse_size_pct = abs(impulse_end - impulse_start) / impulse_start * 100
                
                if impulse_size_pct < self.min_wave_size_pct:
                    continue
                
                # Now look for correction from the high
                correction_start = impulse_end
                correction_start_idx = impulse_end_idx
                
                # Find lowest low after the high
                later_lows = [sl for sl in recent_swing_lows if sl['index'] > correction_start_idx]
                if not later_lows:
                    continue
                
                correction_end = later_lows[0]['price']
                correction_end_idx = later_lows[0]['index']
                
                correction_size_pct = abs(correction_start - correction_end) / correction_start * 100
                
                # Calculate Fibonacci retracement levels
                fib_range = impulse_end - impulse_start
                if fib_range <= 0:
                    continue
                
                correction_amount = impulse_end - correction_end
                retracement_pct = correction_amount / fib_range
                
                # Validate: correction must retrace 0.5-0.8 of impulse
                if retracement_pct < 0.5 or retracement_pct > 0.8:
                    continue
                
                # Looks like a valid ABC correction
                wave.pattern = WavePattern.ABC_CORRECTION
                wave.pattern_confidence = min(1.0, impulse_size_pct / 5.0) * min(1.0, retracement_pct)
                
                wave.impulse_start = impulse_start
                wave.impulse_end = impulse_end
                wave.impulse_size_pct = impulse_size_pct
                
                wave.correction_start = correction_start
                wave.correction_end = correction_end
                wave.correction_size_pct = correction_size_pct
                
                # Calculate all Fibonacci levels
                wave.fib_236 = impulse_end - (fib_range * 0.236)
                wave.fib_382 = impulse_end - (fib_range * 0.382)
                wave.fib_500 = impulse_end - (fib_range * 0.5)
                wave.fib_618 = impulse_end - (fib_range * 0.618)
                wave.fib_705 = impulse_end - (fib_range * 0.705)
                wave.fib_786 = impulse_end - (fib_range * 0.786)
                
                wave.current_retracement = retracement_pct
                
                # Check if price is in optimal zone (0.5-0.705 Fibonacci)
                current_close = closes[-1]
                wave.zone_price_high = wave.fib_500
                wave.zone_price_low = wave.fib_705
                
                if wave.fib_705 <= current_close <= wave.fib_500:
                    wave.in_optimal_zone = True
                
                # Distance to zone
                if current_close > wave.fib_500:
                    wave.distance_to_zone_pct = (current_close - wave.fib_500) / wave.fib_500 * 100
                elif current_close < wave.fib_705:
                    wave.distance_to_zone_pct = (wave.fib_705 - current_close) / wave.fib_705 * 100
                else:
                    wave.distance_to_zone_pct = 0.0
                
                wave.candle_count = len(df_4h)
                
                # Detect additional patterns
                if retracement_pct > 0.618 and current_close <= wave.fib_618:
                    wave.pattern = WavePattern.FALLING_WEDGE
                elif self._is_flag_pattern(df_4h, is_bull=True):
                    wave.pattern = WavePattern.BULL_FLAG
                
                break
            
        except Exception as e:
            log.debug(f"Bullish correction detection error: {e}")
        
        return wave
    
    def _identify_bearish_correction(self, df_4h, highs, lows, closes) -> WaveStructure:
        """Find impulse down + ABC correction up"""
        wave = WaveStructure()
        
        try:
            swing_highs = self._find_swing_points(highs, is_high=True, window=3)
            swing_lows = self._find_swing_points(lows, is_high=False, window=3)
            
            if len(swing_highs) < 2 or len(swing_lows) < 1:
                return wave
            
            recent_swing_highs = sorted(swing_highs, key=lambda x: x['index'])
            recent_swing_lows = sorted(swing_lows, key=lambda x: x['index'])
            
            # Find impulse down
            for sl in reversed(recent_swing_lows[-5:]):
                impulse_end = sl['price']
                impulse_end_idx = sl['index']
                
                prev_highs = [sh for sh in recent_swing_highs if sh['index'] < impulse_end_idx]
                if not prev_highs:
                    continue
                
                impulse_start = prev_highs[-1]['price']
                impulse_start_idx = prev_highs[-1]['index']
                
                impulse_size_pct = abs(impulse_start - impulse_end) / impulse_start * 100
                
                if impulse_size_pct < self.min_wave_size_pct:
                    continue
                
                correction_start = impulse_end
                correction_start_idx = impulse_end_idx
                
                later_highs = [sh for sh in recent_swing_highs if sh['index'] > correction_start_idx]
                if not later_highs:
                    continue
                
                correction_end = later_highs[0]['price']
                
                fib_range = impulse_start - impulse_end
                if fib_range <= 0:
                    continue
                
                correction_amount = correction_end - impulse_end
                retracement_pct = correction_amount / fib_range
                
                if retracement_pct < 0.5 or retracement_pct > 0.8:
                    continue
                
                wave.pattern = WavePattern.ABC_CORRECTION
                wave.pattern_confidence = min(1.0, impulse_size_pct / 5.0) * min(1.0, retracement_pct)
                
                wave.impulse_start = impulse_start
                wave.impulse_end = impulse_end
                wave.impulse_size_pct = impulse_size_pct
                
                wave.correction_start = correction_start
                wave.correction_end = correction_end
                
                # Fibonacci levels for bearish (retrace up)
                wave.fib_236 = impulse_end + (fib_range * 0.236)
                wave.fib_382 = impulse_end + (fib_range * 0.382)
                wave.fib_500 = impulse_end + (fib_range * 0.5)
                wave.fib_618 = impulse_end + (fib_range * 0.618)
                wave.fib_705 = impulse_end + (fib_range * 0.705)
                wave.fib_786 = impulse_end + (fib_range * 0.786)
                
                wave.current_retracement = retracement_pct
                
                current_close = closes[-1]
                wave.zone_price_high = wave.fib_705
                wave.zone_price_low = wave.fib_500
                
                if wave.fib_500 <= current_close <= wave.fib_705:
                    wave.in_optimal_zone = True
                
                if current_close < wave.fib_500:
                    wave.distance_to_zone_pct = (wave.fib_500 - current_close) / wave.fib_500 * 100
                elif current_close > wave.fib_705:
                    wave.distance_to_zone_pct = (current_close - wave.fib_705) / wave.fib_705 * 100
                else:
                    wave.distance_to_zone_pct = 0.0
                
                wave.candle_count = len(df_4h)
                
                if retracement_pct > 0.618 and current_close >= wave.fib_618:
                    wave.pattern = WavePattern.RISING_WEDGE
                elif self._is_flag_pattern(df_4h, is_bull=False):
                    wave.pattern = WavePattern.BEAR_FLAG
                
                break
                
        except Exception as e:
            log.debug(f"Bearish correction detection error: {e}")
        
        return wave
    
    def _find_swing_points(self, prices, is_high: bool, window: int = 3) -> List[Dict]:
        """Find local swing highs or lows"""
        swing_points = []
        
        for i in range(window, len(prices) - window):
            if is_high:
                if prices[i] == max(prices[i-window:i+window+1]):
                    # Check it's actually significant
                    left_cond = all(prices[i] > prices[j] for j in range(i-window, i))
                    right_cond = all(prices[i] > prices[j] for j in range(i+1, i+window+1))
                    if left_cond or right_cond:
                        swing_points.append({'index': i, 'price': prices[i]})
            else:
                if prices[i] == min(prices[i-window:i+window+1]):
                    left_cond = all(prices[i] < prices[j] for j in range(i-window, i))
                    right_cond = all(prices[i] < prices[j] for j in range(i+1, i+window+1))
                    if left_cond or right_cond:
                        swing_points.append({'index': i, 'price': prices[i]})
        
        return swing_points
    
    def _is_flag_pattern(self, df, is_bull: bool) -> bool:
        """Detect flag/pennant consolidation pattern"""
        if len(df) < 15:
            return False
        
        try:
            recent = df.iloc[-12:]
            highs = recent['high'].values
            lows = recent['low'].values
            
            # Check for converging range (tightening)
            high_range = max(highs) - min(highs)
            low_range = max(lows) - min(lows)
            avg_price = recent['close'].mean()
            
            if avg_price > 0:
                compression = (high_range + low_range) / 2 / avg_price * 100
                return compression < 3.0  # Less than 3% range
        except:
            pass
        
        return False

wave_detector = WaveRangeDetector()

# ============ MOMENTUM DIVERGENCE ENGINE ============
class MomentumDivergenceEngine:
    """
    RSI Divergence + MACD Confirmation
    Core method: Detect momentum exhaustion before explosive move
    """
    
    def __init__(self):
        self.rsi_period = 14
        self.macd_fast = 12
        self.macd_slow = 26
        self.macd_signal = 9
    
    def analyze_momentum(self, df_1h, df_15m, trend_bias: TrendBias, 
                         wave: WaveStructure) -> MomentumSignals:
        """
        Step 3 & 4 combined: RSI Divergence on 1H/15M + MACD confirmation
        """
        momentum = MomentumSignals()
        
        if df_1h is None or df_15m is None:
            return momentum
        
        try:
            # Calculate RSI for both timeframes
            rsi_1h = self._calculate_rsi(df_1h['close'])
            rsi_15m = self._calculate_rsi(df_15m['close'])
            
            # Calculate MACD for 15M
            macd_15m = self._calculate_macd(df_15m['close'])
            
            momentum.rsi_current = rsi_15m[-1]
            
            # Detect divergence based on trend bias
            if trend_bias == TrendBias.BULLISH:
                divergence = self._detect_bullish_divergence(df_15m, rsi_15m, df_1h, rsi_1h)
            elif trend_bias == TrendBias.BEARISH:
                divergence = self._detect_bearish_divergence(df_15m, rsi_15m, df_1h, rsi_1h)
            else:
                divergence = DivergenceType.NONE, 0.0, []
            
            momentum.divergence_type = divergence[0]
            momentum.divergence_strength = divergence[1]
            momentum.divergence_points = divergence[2]
            
            # MACD analysis
            if len(macd_15m['macd_line']) >= 2 and len(macd_15m['signal_line']) >= 2:
                prev_macd = macd_15m['macd_line'][-2]
                prev_signal = macd_15m['signal_line'][-2]
                curr_macd = macd_15m['macd_line'][-1]
                curr_signal = macd_15m['signal_line'][-1]
                
                momentum.macd_line = curr_macd
                momentum.macd_signal_line = curr_signal
                momentum.macd_histogram = macd_15m['histogram'][-1]
                
                # Cross detection
                if prev_macd < prev_signal and curr_macd > curr_signal:
                    momentum.macd_crossed = True
                    momentum.macd_cross_direction = "BULLISH"
                elif prev_macd > prev_signal and curr_macd < curr_signal:
                    momentum.macd_crossed = True
                    momentum.macd_cross_direction = "BEARISH"
                
                # Histogram reversal (earlier signal than cross)
                if len(macd_15m['histogram']) >= 3:
                    hist_3 = macd_15m['histogram'][-3]
                    hist_2 = macd_15m['histogram'][-2]
                    hist_1 = macd_15m['histogram'][-1]
                    
                    if trend_bias == TrendBias.BULLISH:
                        if hist_3 < hist_2 and hist_2 < hist_1 and hist_3 < 0:
                            momentum.macd_histogram_reversal = True
                    elif trend_bias == TrendBias.BEARISH:
                        if hist_3 > hist_2 and hist_2 > hist_1 and hist_3 > 0:
                            momentum.macd_histogram_reversal = True
            
            # Calculate momentum score
            momentum.momentum_score = self._calculate_momentum_score(momentum, trend_bias)
            
            # Check alignment
            if trend_bias == TrendBias.BULLISH:
                momentum.momentum_aligned = (
                    momentum.divergence_type == DivergenceType.BULLISH_REGULAR and
                    (momentum.macd_crossed and momentum.macd_cross_direction == "BULLISH" or
                     momentum.macd_histogram_reversal)
                )
            elif trend_bias == TrendBias.BEARISH:
                momentum.momentum_aligned = (
                    momentum.divergence_type == DivergenceType.BEARISH_REGULAR and
                    (momentum.macd_crossed and momentum.macd_cross_direction == "BEARISH" or
                     momentum.macd_histogram_reversal)
                )
            
        except Exception as e:
            log.debug(f"Momentum analysis error: {e}")
        
        return momentum
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> np.ndarray:
        """Calculate RSI values"""
        delta = prices.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        
        return rsi.fillna(50).values
    
    def _calculate_macd(self, prices: pd.Series) -> Dict:
        """Calculate MACD values"""
        ema_fast = prices.ewm(span=self.macd_fast).mean()
        ema_slow = prices.ewm(span=self.macd_slow).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=self.macd_signal).mean()
        histogram = macd_line - signal_line
        
        return {
            'macd_line': macd_line.values,
            'signal_line': signal_line.values,
            'histogram': histogram.values
        }
    
    def _detect_bullish_divergence(self, df_15m, rsi_15m, df_1h, rsi_1h) -> Tuple[DivergenceType, float, List[Dict]]:
        """
        Detect regular bullish divergence:
        Price makes lower low, RSI makes higher low
        This indicates selling momentum is dying
        """
        divergence_points = []
        
        try:
            prices = df_15m['low'].values
            closes = df_15m['close'].values
            
            # Find local lows in the last 30-50 candles
            lookback = min(50, len(prices) - 5)
            recent_prices = prices[-lookback:]
            recent_rsi = rsi_15m[-lookback:]
            recent_closes = closes[-lookback:]
            
            # Find price lows
            price_lows = self._find_local_lows(recent_prices, window=5)
            
            if len(price_lows) >= 2:
                # Compare last two significant lows
                last_low = price_lows[-1]
                prev_low = price_lows[-2]
                
                last_idx = last_low['index']
                prev_idx = prev_low['index']
                
                # Price: lower low?
                if last_low['price'] < prev_low['price']:
                    # RSI: higher low?
                    if recent_rsi[last_idx] > recent_rsi[prev_idx]:
                        # Valid regular bullish divergence!
                        strength = self._calculate_divergence_strength(
                            prev_low['price'], last_low['price'],
                            recent_rsi[prev_idx], recent_rsi[last_idx]
                        )
                        
                        divergence_points.append({
                            'type': 'price_low',
                            'point1': {'index': prev_idx, 'price': prev_low['price'], 'rsi': recent_rsi[prev_idx]},
                            'point2': {'index': last_idx, 'price': last_low['price'], 'rsi': recent_rsi[last_idx]}
                        })
                        
                        return DivergenceType.BULLISH_REGULAR, strength, divergence_points
            
            # Check 1H for additional confirmation
            if len(rsi_1h) >= 3:
                last_rsi_1h = rsi_1h[-1]
                # In uptrend, RSI should hold above 40
                if last_rsi_1h > 40:
                    return DivergenceType.BULLISH_REGULAR, 0.4, []
            
        except Exception as e:
            log.debug(f"Bullish divergence error: {e}")
        
        return DivergenceType.NONE, 0.0, []
    
    def _detect_bearish_divergence(self, df_15m, rsi_15m, df_1h, rsi_1h) -> Tuple[DivergenceType, float, List[Dict]]:
        """
        Detect regular bearish divergence:
        Price makes higher high, RSI makes lower high
        This indicates buying momentum is dying
        """
        divergence_points = []
        
        try:
            prices = df_15m['high'].values
            closes = df_15m['close'].values
            
            lookback = min(50, len(prices) - 5)
            recent_prices = prices[-lookback:]
            recent_rsi = rsi_15m[-lookback:]
            recent_closes = closes[-lookback:]
            
            # Find price highs
            price_highs = self._find_local_highs(recent_prices, window=5)
            
            if len(price_highs) >= 2:
                last_high = price_highs[-1]
                prev_high = price_highs[-2]
                
                last_idx = last_high['index']
                prev_idx = prev_high['index']
                
                # Price: higher high?
                if last_high['price'] > prev_high['price']:
                    # RSI: lower high?
                    if recent_rsi[last_idx] < recent_rsi[prev_idx]:
                        strength = self._calculate_divergence_strength(
                            prev_high['price'], last_high['price'],
                            recent_rsi[prev_idx], recent_rsi[last_idx]
                        )
                        
                        divergence_points.append({
                            'type': 'price_high',
                            'point1': {'index': prev_idx, 'price': prev_high['price'], 'rsi': recent_rsi[prev_idx]},
                            'point2': {'index': last_idx, 'price': last_high['price'], 'rsi': recent_rsi[last_idx]}
                        })
                        
                        return DivergenceType.BEARISH_REGULAR, strength, divergence_points
            
            # Check 1H
            if len(rsi_1h) >= 3:
                last_rsi_1h = rsi_1h[-1]
                if last_rsi_1h < 60:
                    return DivergenceType.BEARISH_REGULAR, 0.4, []
            
        except Exception as e:
            log.debug(f"Bearish divergence error: {e}")
        
        return DivergenceType.NONE, 0.0, []
    
    def _find_local_lows(self, prices, window: int = 3) -> List[Dict]:
        """Find local price lows"""
        lows = []
        for i in range(window, len(prices) - window):
            if prices[i] == min(prices[i-window:i+window+1]):
                lows.append({'index': i, 'price': prices[i]})
        return lows
    
    def _find_local_highs(self, prices, window: int = 3) -> List[Dict]:
        """Find local price highs"""
        highs = []
        for i in range(window, len(prices) - window):
            if prices[i] == max(prices[i-window:i+window+1]):
                highs.append({'index': i, 'price': prices[i]})
        return highs
    
    def _calculate_divergence_strength(self, price1, price2, rsi1, rsi2) -> float:
        """Calculate strength of the divergence signal"""
        if abs(price1 - price2) < 0.0001:
            return 0.0
        
        price_change_pct = abs(price2 - price1) / price1 * 100
        rsi_change = abs(rsi2 - rsi1)
        
        # Strong divergence: big price move, opposite RSI move
        strength = min(1.0, (price_change_pct / 2.0) * 0.5 + (rsi_change / 10.0) * 0.5)
        return max(0.0, min(1.0, strength))
    
    def _calculate_momentum_score(self, momentum: MomentumSignals, trend_bias: TrendBias) -> float:
        """Calculate overall momentum alignment score"""
        score = 0.0
        max_score = 0.0
        
        # Divergence presence
        max_score += 0.4
        if momentum.divergence_type != DivergenceType.NONE:
            score += 0.4 * momentum.divergence_strength
        
        # MACD confirmation
        max_score += 0.3
        if momentum.macd_crossed:
            if ((trend_bias == TrendBias.BULLISH and momentum.macd_cross_direction == "BULLISH") or
                (trend_bias == TrendBias.BEARISH and momentum.macd_cross_direction == "BEARISH")):
                score += 0.3
            else:
                score += 0.1
        
        # Histogram reversal (early signal)
        max_score += 0.2
        if momentum.macd_histogram_reversal:
            score += 0.2
        
        # RSI zone check
        max_score += 0.1
        if trend_bias == TrendBias.BULLISH and momentum.rsi_current > 40:
            score += 0.1
        elif trend_bias == TrendBias.BEARISH and momentum.rsi_current < 60:
            score += 0.1
        
        return score / max_score if max_score > 0 else 0.0

momentum_engine = MomentumDivergenceEngine()

# ============ VOLUME BREAKOUT TRIGGER ============
class VolumeBreakoutTrigger:
    """
    Step 5: Volume spike confirmation for entry
    Now uses lower threshold (1.5x) and flags.
    """
    
    def __init__(self):
        self.min_volume_ratio = 1.5   # lowered from 2.0
        self.volume_lookback = 20
    
    def detect_breakout(self, df_5m, df_15m, trend_bias: TrendBias,
                        wave: WaveStructure, entry_price: float) -> VolumeBreakout:
        breakout = VolumeBreakout()
        
        if df_5m is None or len(df_5m) < self.volume_lookback + 3:
            return breakout
        
        if df_15m is None or len(df_15m) < 5:
            return breakout
        
        try:
            recent_volume = df_5m['volume'].values[-(self.volume_lookback+3):]
            latest_candles = recent_volume[-3:]
            avg_volume_base = recent_volume[:self.volume_lookback]
            
            if len(avg_volume_base) == 0:
                return breakout
            
            avg_volume = np.mean(avg_volume_base)
            breakout.avg_volume_20 = avg_volume
            
            if avg_volume <= 0:
                return breakout
            
            # Check for volume spike in last 3 candles
            for i in range(3):
                candle_idx = -(3-i)
                candle_volume = latest_candles[i]
                volume_ratio = candle_volume / avg_volume
                
                if volume_ratio < self.min_volume_ratio:
                    # try flag breakout even with lower volume
                    if not self._is_flag_pattern(df_15m, is_bull=(trend_bias == TrendBias.BULLISH)):
                        continue
                
                try:
                    candle = df_5m.iloc[candle_idx]
                except IndexError:
                    continue
                
                candle_open = candle['open']
                candle_close = candle['close']
                candle_high = candle['high']
                candle_low = candle['low']
                
                # Check for pattern break (original fib zone break)
                if wave.in_optimal_zone or wave.distance_to_zone_pct < 2.0:
                    if trend_bias == TrendBias.BULLISH:
                        if candle_close > wave.fib_500 or candle_high > wave.fib_500:
                            breakout.triggered = True
                            breakout.breakout_direction = "BULLISH"
                            breakout.breakout_price = candle_close
                            breakout.pattern_break = True
                            breakout.volume_ratio = volume_ratio
                            breakout.breakout_candle_volume = candle_volume
                            breakout.volume_score = min(1.0, (volume_ratio - 0.8) / 3.5)
                            if candle_low < wave.fib_705 * 0.998:
                                breakout.sweep_then_reclaim = True
                                breakout.volume_score += 0.2
                                breakout.volume_score = min(1.0, breakout.volume_score)
                            return breakout
                    elif trend_bias == TrendBias.BEARISH:
                        if candle_close < wave.fib_500 or candle_low < wave.fib_500:
                            breakout.triggered = True
                            breakout.breakout_direction = "BEARISH"
                            breakout.breakout_price = candle_close
                            breakout.pattern_break = True
                            breakout.volume_ratio = volume_ratio
                            breakout.breakout_candle_volume = candle_volume
                            breakout.volume_score = min(1.0, (volume_ratio - 0.8) / 3.5)
                            if candle_high > wave.fib_705 * 1.002:
                                breakout.sweep_then_reclaim = True
                                breakout.volume_score += 0.2
                                breakout.volume_score = min(1.0, breakout.volume_score)
                            return breakout
                
                # Flag breakout without fib zone condition
                if self._is_flag_pattern(df_15m, is_bull=(trend_bias == TrendBias.BULLISH)):
                    flag_high = df_15m['high'].iloc[-5:].max()
                    flag_low = df_15m['low'].iloc[-5:].min()
                    if trend_bias == TrendBias.BULLISH and candle_close > flag_high:
                        breakout.triggered = True
                        breakout.breakout_direction = "BULLISH"
                        breakout.breakout_price = candle_close
                        breakout.pattern_break = True
                        breakout.volume_ratio = volume_ratio
                        breakout.breakout_candle_volume = candle_volume
                        breakout.volume_score = min(1.0, (volume_ratio - 0.8) / 2.5)
                        breakout.sweep_then_reclaim = (candle_low < flag_low * 0.998)
                        return breakout
                    elif trend_bias == TrendBias.BEARISH and candle_close < flag_low:
                        breakout.triggered = True
                        breakout.breakout_direction = "BEARISH"
                        breakout.breakout_price = candle_close
                        breakout.pattern_break = True
                        breakout.volume_ratio = volume_ratio
                        breakout.breakout_candle_volume = candle_volume
                        breakout.volume_score = min(1.0, (volume_ratio - 0.8) / 2.5)
                        breakout.sweep_then_reclaim = (candle_high > flag_high * 1.002)
                        return breakout
            
            # Final fallback: strong volume with decent body (original logic)
            for i in range(3):
                candle_idx = -(3-i)
                candle_volume = latest_candles[i]
                volume_ratio = candle_volume / avg_volume
                
                if volume_ratio >= self.min_volume_ratio * 1.2:
                    try:
                        candle = df_5m.iloc[candle_idx]
                    except IndexError:
                        continue
                    
                    body_size = abs(candle['close'] - candle['open'])
                    total_range = candle['high'] - candle['low']
                    
                    if total_range > 0 and body_size / total_range > 0.6:
                        if trend_bias == TrendBias.BULLISH and candle['close'] > candle['open']:
                            breakout.triggered = True
                            breakout.breakout_direction = "BULLISH"
                            breakout.breakout_price = candle['close']
                            breakout.volume_ratio = volume_ratio
                            breakout.breakout_candle_volume = candle_volume
                            breakout.volume_score = 0.5
                            return breakout
                        elif trend_bias == TrendBias.BEARISH and candle['close'] < candle['open']:
                            breakout.triggered = True
                            breakout.breakout_direction = "BEARISH"
                            breakout.breakout_price = candle['close']
                            breakout.volume_ratio = volume_ratio
                            breakout.breakout_candle_volume = candle_volume
                            breakout.volume_score = 0.5
                            return breakout
            
        except Exception as e:
            log.debug(f"Volume breakout detection error: {e}")
        
        return breakout
    
    def _is_flag_pattern(self, df, is_bull: bool) -> bool:
        """Reuse flag detection from wave detector"""
        return wave_detector._is_flag_pattern(df, is_bull)

volume_trigger = VolumeBreakoutTrigger()

# ============ LIQUIDITY POOL IDENTIFICATION (PRESERVED) ============
def identify_liquidity_pools(df, timeframe="1h"):
    pools = {
        'buy_stops': [],
        'sell_stops': [],
        'equal_highs': [],
        'equal_lows': []
    }
    
    if df is None or len(df) < 20:
        return pools
    
    window_size = 5 if timeframe == "15m" else 3
    
    for i in range(window_size, len(df)-window_size):
        window_highs = df['high'].iloc[i-window_size:i+window_size+1]
        current_high = df['high'].iloc[i]
        
        if current_high == window_highs.max():
            same_high_count = round((window_highs == current_high).sum())
            if same_high_count >= 2:
                pools['equal_highs'].append({
                    'price': float(current_high),
                    'timeframe': timeframe,
                    'candle_index': i,
                    'count': same_high_count,
                    'type': 'equal_high'
                })
                pools['sell_stops'].append({
                    'price': float(current_high),
                    'reason': 'equal_high',
                    'timeframe': timeframe,
                    'strength': same_high_count
                })
    
    for i in range(window_size, len(df)-window_size):
        window_lows = df['low'].iloc[i-window_size:i+window_size+1]
        current_low = df['low'].iloc[i]
        
        if current_low == window_lows.min():
            same_low_count = round((window_lows == current_low).sum())
            if same_low_count >= 2:
                pools['equal_lows'].append({
                    'price': float(current_low),
                    'timeframe': timeframe,
                    'candle_index': i,
                    'count': same_low_count,
                    'type': 'equal_low'
                })
                pools['buy_stops'].append({
                    'price': float(current_low),
                    'reason': 'equal_low',
                    'timeframe': timeframe,
                    'strength': same_low_count
                })
    
    for key in pools:
        if pools[key]:
            seen_prices = set()
            unique_pools = []
            for pool in pools[key]:
                if pool['price'] not in seen_prices:
                    seen_prices.add(pool['price'])
                    unique_pools.append(pool)
            pools[key] = unique_pools
            
            if key in ['buy_stops', 'equal_lows']:
                pools[key].sort(key=lambda x: x['price'])
            else:
                pools[key].sort(key=lambda x: x['price'], reverse=True)
    
    return pools

# ============ LIQUIDITY-BASED TP/SL (PRESERVED) ============
async def calculate_liquidity_tp_sl(exchange, symbol: str, side: str, entry_price: float,
                                   entry_type: str) -> Tuple[float, List[float], List[Dict], Dict]:
    
    ohlcv_4h = await fetch_ohlcv(exchange, symbol, "4h", 100)
    ohlcv_1h = await fetch_ohlcv(exchange, symbol, "1h", 200)
    ohlcv_15m = await fetch_ohlcv(exchange, symbol, "15m", 300)
    
    df_4h = create_dataframe(ohlcv_4h)
    df_1h = create_dataframe(ohlcv_1h)
    df_15m = create_dataframe(ohlcv_15m)
    
    pools_4h = identify_liquidity_pools(df_4h, "4h") if df_4h is not None else {'buy_stops': [], 'sell_stops': [], 'equal_highs': [], 'equal_lows': []}
    pools_1h = identify_liquidity_pools(df_1h, "1h") if df_1h is not None else {'buy_stops': [], 'sell_stops': [], 'equal_highs': [], 'equal_lows': []}
    pools_15m = identify_liquidity_pools(df_15m, "15m") if df_15m is not None else {'buy_stops': [], 'sell_stops': [], 'equal_highs': [], 'equal_lows': []}
    
    all_pools = {'buy_stops': [], 'sell_stops': [], 'equal_highs': [], 'equal_lows': []}
    
    for pool in pools_4h['buy_stops']:
        pool['weight'] = 3.0
        all_pools['buy_stops'].append(pool)
    for pool in pools_1h['buy_stops']:
        pool['weight'] = 2.0
        all_pools['buy_stops'].append(pool)
    for pool in pools_15m['buy_stops']:
        pool['weight'] = 1.0
        all_pools['buy_stops'].append(pool)
    
    for pool_type in ['sell_stops', 'equal_highs', 'equal_lows']:
        for pool in pools_4h[pool_type]:
            pool['weight'] = 3.0
            all_pools[pool_type].append(pool)
        for pool in pools_1h[pool_type]:
            pool['weight'] = 2.0
            all_pools[pool_type].append(pool)
        for pool in pools_15m[pool_type]:
            pool['weight'] = 1.0
            all_pools[pool_type].append(pool)
    
    all_pools['buy_stops'].sort(key=lambda x: x['price'])
    all_pools['sell_stops'].sort(key=lambda x: x['price'], reverse=True)
    all_pools['equal_highs'].sort(key=lambda x: x['price'], reverse=True)
    all_pools['equal_lows'].sort(key=lambda x: x['price'])
    
    current_price = entry_price
    tp_targets = []
    tp_sources = []
    sl_price = 0.0
    sl_source = {}
    
    if side == "BUY":
        sell_stops_below = [p for p in all_pools['sell_stops'] if p['price'] < current_price]
        if sell_stops_below:
            for timeframe_weight in [3.0, 2.0, 1.0]:
                timeframe_pools = [p for p in sell_stops_below if p.get('weight', 1.0) == timeframe_weight]
                if timeframe_pools:
                    strongest_pool = min(timeframe_pools, key=lambda x: x['price'])
                    sl_price = strongest_pool['price'] * 0.997
                    sl_source = {'type': 'sell_stop_pool', 'timeframe': strongest_pool.get('timeframe', 'unknown'),
                                'reason': strongest_pool.get('reason', ''), 'strength': strongest_pool.get('strength', 1),
                                'original_price': strongest_pool['price']}
                    break
            if sl_price == 0:
                strongest_pool = min(sell_stops_below, key=lambda x: x['price'])
                sl_price = strongest_pool['price'] * 0.995
        else:
            equal_lows_below = [p for p in all_pools['equal_lows'] if p['price'] < current_price]
            if equal_lows_below:
                most_recent_low = max(equal_lows_below, key=lambda x: x.get('candle_index', 0))
                sl_price = most_recent_low['price'] * 0.99
            else:
                return sl_price, [], [], {}
        
        if sl_price > current_price * 0.995:
            sl_price = current_price * 0.985
        
        buy_stops_above = [p for p in all_pools['buy_stops'] if p['price'] > current_price]
        if buy_stops_above:
            tp1_pool = min(buy_stops_above, key=lambda x: x['price'])
            tp_targets.append(tp1_pool['price'])
            tp_sources.append({'tp_level': 1, 'type': 'buy_stop_pool', 'timeframe': tp1_pool.get('timeframe', 'unknown'),
                              'reason': tp1_pool.get('reason', ''), 'strength': tp1_pool.get('strength', 1)})
            
            buy_stops_above_tp1 = [p for p in all_pools['buy_stops'] if p['price'] > tp_targets[0] * 1.01]
            if buy_stops_above_tp1:
                tp2_pool = min(buy_stops_above_tp1, key=lambda x: x['price'])
                tp_targets.append(tp2_pool['price'])
                tp_sources.append({'tp_level': 2, 'type': 'buy_stop_pool', 'timeframe': tp2_pool.get('timeframe', 'unknown'),
                                  'reason': 'next_pool', 'strength': tp2_pool.get('strength', 1)})
        else:
            return sl_price, [], [], {}
    
    else:  # SELL
        buy_stops_above = [p for p in all_pools['buy_stops'] if p['price'] > current_price]
        if buy_stops_above:
            for timeframe_weight in [3.0, 2.0, 1.0]:
                timeframe_pools = [p for p in buy_stops_above if p.get('weight', 1.0) == timeframe_weight]
                if timeframe_pools:
                    strongest_pool = max(timeframe_pools, key=lambda x: x['price'])
                    sl_price = strongest_pool['price'] * 1.003
                    sl_source = {'type': 'buy_stop_pool', 'timeframe': strongest_pool.get('timeframe', 'unknown'),
                                'reason': strongest_pool.get('reason', ''), 'strength': strongest_pool.get('strength', 1)}
                    break
            if sl_price == 0:
                strongest_pool = max(buy_stops_above, key=lambda x: x['price'])
                sl_price = strongest_pool['price'] * 1.005
        else:
            equal_highs_above = [p for p in all_pools['equal_highs'] if p['price'] > current_price]
            if equal_highs_above:
                most_recent_high = max(equal_highs_above, key=lambda x: x.get('candle_index', 0))
                sl_price = most_recent_high['price'] * 1.01
            else:
                return sl_price, [], [], {}
        
        if sl_price < current_price * 1.005:
            sl_price = current_price * 1.015
        
        sell_stops_below = [p for p in all_pools['sell_stops'] if p['price'] < current_price]
        if sell_stops_below:
            tp1_pool = max(sell_stops_below, key=lambda x: x['price'])
            tp_targets.append(tp1_pool['price'])
            tp_sources.append({'tp_level': 1, 'type': 'sell_stop_pool', 'timeframe': tp1_pool.get('timeframe', 'unknown'),
                              'reason': tp1_pool.get('reason', ''), 'strength': tp1_pool.get('strength', 1)})
            
            sell_stops_below_tp1 = [p for p in all_pools['sell_stops'] if p['price'] < tp_targets[0] * 0.99]
            if sell_stops_below_tp1:
                tp2_pool = max(sell_stops_below_tp1, key=lambda x: x['price'])
                tp_targets.append(tp2_pool['price'])
                tp_sources.append({'tp_level': 2, 'type': 'sell_stop_pool', 'timeframe': tp2_pool.get('timeframe', 'unknown'),
                                  'reason': 'next_pool', 'strength': tp2_pool.get('strength', 1)})
        else:
            return sl_price, [], [], {}
    
    risk = abs(current_price - sl_price)
    reward = abs(tp_targets[0] - current_price) if tp_targets else 0
    rr_ratio = reward / risk if risk > 0 else 0
    
    liquidity_analysis = {
        'side': side,
        'entry_type': entry_type,
        'identified_pools': {
            'buy_stops': len(all_pools['buy_stops']),
            'sell_stops': len(all_pools['sell_stops']),
            'equal_highs': len(all_pools['equal_highs']),
            'equal_lows': len(all_pools['equal_lows'])
        },
        'sl_source': sl_source,
        'tp_sources': tp_sources,
        'rr_ratio': rr_ratio,
        'risk_pct': risk / current_price * 100 if current_price > 0 else 0,
        'reward_pct': reward / current_price * 100 if current_price > 0 and tp_targets else 0
    }
    
    return sl_price, tp_targets, tp_sources, liquidity_analysis

# ============ INSTITUTIONAL DATA FETCHER (WITH ORDER BOOK BIAS) ============
class InstitutionalDataFetcher:
    def __init__(self):
        self.cache = {}
        self.cache_ttl = {'funding': 300, 'oi': 600, 'ticker': 30}
        
    async def get_institutional_data(self, exchange, symbol: str) -> InstitutionalData:
        cache_key = f"{symbol}_institutional"
        now = time.time()
        
        if cache_key in self.cache:
            data, timestamp = self.cache[cache_key]
            if now - timestamp < 300:
                return data
        
        try:
            futures_symbol = self._get_futures_symbol(symbol)
            tasks = [
                self._fetch_funding_data(exchange, futures_symbol),
                self._fetch_open_interest(exchange, futures_symbol),
                self._fetch_spot_futures_spread(exchange, symbol, futures_symbol)
            ]
            funding_data, oi_data, spread_data = await asyncio.gather(*tasks, return_exceptions=True)
            
            data = InstitutionalData()
            
            if not isinstance(funding_data, Exception) and funding_data:
                data.funding_rate = funding_data.get('fundingRate', 0) * 100
                data.funding_timestamp = datetime.datetime.utcnow()
                try:
                    funding_history = await rate_limiter.execute_with_backoff(
                        exchange.fetch_funding_rate_history, futures_symbol, limit=8, endpoint_type="funding"
                    )
                    if funding_history:
                        data.funding_history = [f['fundingRate'] * 100 for f in funding_history]
                except:
                    pass
            
            if not isinstance(oi_data, Exception) and oi_data:
                data.open_interest = oi_data.get('openInterest', 0)
                data.oi_timestamp = datetime.datetime.utcnow()
                try:
                    oi_history = await rate_limiter.execute_with_backoff(
                        exchange.fetch_open_interest_history, futures_symbol, '1h', limit=24, endpoint_type="oi"
                    )
                    if oi_history and len(oi_history) >= 2:
                        latest = oi_history[0]['openInterest']
                        oldest = oi_history[-1]['openInterest']
                        if oldest > 0:
                            data.oi_change_24h = (latest - oldest) / oldest * 100
                except:
                    pass
            
            self.cache[cache_key] = (data, now)
            return data
            
        except Exception as e:
            log.warning(f"Failed to fetch institutional data for {symbol}: {e}")
            return InstitutionalData()
    
    def _get_futures_symbol(self, spot_symbol: str) -> str:
        if "USDT" in spot_symbol:
            return spot_symbol.replace("/USDT", "-USDT-SWAP")
        return spot_symbol
    
    async def _fetch_funding_data(self, exchange, futures_symbol: str) -> Dict:
        try:
            return await rate_limiter.execute_with_backoff(
                exchange.fetch_funding_rate, futures_symbol, endpoint_type="funding"
            )
        except:
            return {}
    
    async def _fetch_open_interest(self, exchange, futures_symbol: str) -> Dict:
        try:
            return await rate_limiter.execute_with_backoff(
                exchange.fetch_open_interest, futures_symbol, endpoint_type="oi"
            )
        except:
            return {}
    
    async def _fetch_spot_futures_spread(self, exchange, spot_symbol: str, futures_symbol: str) -> Dict:
        try:
            spot_ticker = await rate_limiter.execute_with_backoff(exchange.fetch_ticker, spot_symbol, endpoint_type="general")
            futures_ticker = await rate_limiter.execute_with_backoff(exchange.fetch_ticker, futures_symbol, endpoint_type="general")
            if spot_ticker and futures_ticker:
                spot_price = spot_ticker.get('last', 0)
                futures_price = futures_ticker.get('last', futures_ticker.get('mark', 0))
                if spot_price > 0:
                    basis = (futures_price - spot_price) / spot_price * 100
                    return {'basis': basis, 'premium': basis, 'spot_price': spot_price, 'futures_price': futures_price}
        except:
            pass
        return {}
    
    async def get_orderbook_bias(self, exchange, symbol: str) -> float:
        """Return bid/ask volume ratio. >1 = more bids, <1 = more asks."""
        try:
            ob = await rate_limiter.execute_with_backoff(
                exchange.fetch_order_book, symbol, limit=20, endpoint_type="general"
            )
            bid_vol = sum(b[1] for b in ob['bids'][:10])
            ask_vol = sum(a[1] for a in ob['asks'][:10])
            if ask_vol > 0:
                return bid_vol / ask_vol
        except:
            pass
        return 1.0

data_fetcher = InstitutionalDataFetcher()

# ============ DIRECTION ENGINE (SIMPLIFIED - NOW SECONDARY) ============
class DirectionEngine:
    def __init__(self):
        self.layer_weights = {'liquidity': 0.25, 'trapped': 0.35, 'bleeding': 0.25, 'micro': 0.15}
    
    async def analyze_direction(self, exchange, symbol: str, proposed_side: str,
                               current_price: float) -> DirectionMetrics:
        metrics = DirectionMetrics()
        
        try:
            institutional_data = await data_fetcher.get_institutional_data(exchange, symbol)
            
            # Trapped side detection (simplified)
            trapped_side, trapped_conf = self._quick_trapped_check(institutional_data, proposed_side, current_price)
            metrics.trapped_side = trapped_side
            metrics.trapped_confidence = trapped_conf
            
            # Bleeding side
            bleeding_side, funding_extreme = self._quick_bleeding_check(institutional_data)
            metrics.bleeding_side = bleeding_side
            metrics.funding_extreme = funding_extreme
            
            # Calculate direction score
            direction_score = 0.0
            if proposed_side == "BUY":
                if trapped_side == TrappedSide.SHORT:
                    direction_score += 0.3
                if bleeding_side == "LONG":
                    direction_score += 0.2
            else:
                if trapped_side == TrappedSide.LONG:
                    direction_score += 0.3
                if bleeding_side == "SHORT":
                    direction_score += 0.2
            
            abs_score = abs(direction_score)
            if abs_score > 0.4:
                confidence_tier = DirectionTier.HIGH
            elif abs_score > 0.2:
                confidence_tier = DirectionTier.MEDIUM
            else:
                confidence_tier = DirectionTier.LOW
            
            metrics.direction_score = direction_score
            metrics.confidence_tier = confidence_tier
            
            conflicts = []
            if trapped_side == TrappedSide.LONG and proposed_side == "BUY":
                conflicts.append("Trapped LONG vs BUY")
            if trapped_side == TrappedSide.SHORT and proposed_side == "SELL":
                conflicts.append("Trapped SHORT vs SELL")
            metrics.conflict_warnings = conflicts
            
        except Exception as e:
            log.debug(f"Direction engine error: {e}")
        
        return metrics
    
    def _quick_trapped_check(self, inst_data: InstitutionalData, side: str, price: float) -> Tuple[TrappedSide, float]:
        if inst_data.oi_change_24h > OI_ACCUMULATION_THRESHOLD * 100 and inst_data.funding_rate > FUNDING_EXTREME_THRESHOLD:
            return TrappedSide.LONG, 0.6
        elif inst_data.oi_change_24h < -OI_ACCUMULATION_THRESHOLD * 100 and inst_data.funding_rate < -FUNDING_EXTREME_THRESHOLD:
            return TrappedSide.SHORT, 0.6
        return TrappedSide.NONE, 0.0
    
    def _quick_bleeding_check(self, inst_data: InstitutionalData) -> Tuple[str, float]:
        if inst_data.funding_rate > FUNDING_EXTREME_THRESHOLD:
            return "LONG", inst_data.funding_rate
        elif inst_data.funding_rate < -FUNDING_EXTREME_THRESHOLD:
            return "SHORT", abs(inst_data.funding_rate)
        return "", 0.0

direction_engine = DirectionEngine()

# ============ FAST MOMENTUM SCALPER (EXTRA TRADES) ============
class FastMomentumScalper:
    """
    Quick scalp entries on 3m/5m using pure momentum + volume,
    no full Elliott wave needed.
    """
    def __init__(self):
        self.min_vol_ratio = 1.5       # volume spike threshold
        self.min_body_pct = 0.3        # candle body > 30% of range
        self.min_rr = 2.0              # minimum risk/reward
        self.ema_fast = 9
        self.ema_slow = 21

    async def scan(self, exchange, symbol: str, current_price: float,
                   trend_bias_hint: TrendBias = TrendBias.NEUTRAL) -> Optional[Dict]:
        try:
            df_3m = create_dataframe(await fetch_ohlcv(exchange, symbol, "3m", 50))
            if df_3m is None or len(df_3m) < 30:
                return None

            closes = df_3m['close'].values
            highs = df_3m['high'].values
            lows = df_3m['low'].values
            volumes = df_3m['volume'].values
            opens = df_3m['open'].values

            # 1. Volume spike in last 2 candles
            avg_vol = np.mean(volumes[-22:-2]) if len(volumes) >= 22 else np.mean(volumes[:-2])
            last_vol = volumes[-1]
            if avg_vol <= 0 or last_vol < avg_vol * self.min_vol_ratio:
                return None

            # 2. Short-term trend with EMAs
            ema9 = pd.Series(closes).ewm(span=self.ema_fast).mean().iloc[-1]
            ema21 = pd.Series(closes).ewm(span=self.ema_slow).mean().iloc[-1]
            rsi = self._calc_rsi(closes, 14)

            # 3. Candle quality – decisive body
            body = abs(closes[-1] - opens[-1])
            wick_high = highs[-1] - max(opens[-1], closes[-1])
            wick_low = min(opens[-1], closes[-1]) - lows[-1]
            total_range = highs[-1] - lows[-1]
            if total_range == 0:
                return None
            body_ratio = body / total_range if total_range > 0 else 0

            side = None
            sl_price = 0.0
            tp_price = 0.0

            # Bullish setup
            if (ema9 > ema21 and rsi > 50 and closes[-1] > opens[-1]
                    and body_ratio > self.min_body_pct and wick_high < body * 0.5):
                entry = highs[-1] * 1.001   # small buffer
                sl = lows[-1] * 0.997
                tp = current_price + (current_price - sl) * 2.0   # 2:1 RR minimum
                if tp / current_price - 1 < 0.02:  # target at least 2%
                    tp = current_price * 1.025
                risk = entry - sl
                reward = tp - entry
                if reward / risk < self.min_rr:
                    return None
                side = "BUY"
                sl_price, tp_price = sl, tp

            # Bearish setup
            elif (ema9 < ema21 and rsi < 50 and closes[-1] < opens[-1]
                  and body_ratio > self.min_body_pct and wick_low < body * 0.5):
                entry = lows[-1] * 0.999
                sl = highs[-1] * 1.003
                tp = current_price - (sl - current_price) * 2.0
                if 1 - tp / current_price < 0.02:
                    tp = current_price * 0.975
                risk = sl - entry
                reward = entry - tp
                if reward / risk < self.min_rr:
                    return None
                side = "SELL"
                sl_price, tp_price = sl, tp

            if side is None:
                return None

            # Build quick setup dict
            setup = {
                "symbol": symbol,
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "side": side,
                "current_price": current_price,
                "entry_price": current_price,   # approximate, real entry on break
                "entry_type": "MOMENTUM_SCALP",
                "sl_price": sl_price,
                "tp_targets": [tp_price],
                "risk": abs(current_price - sl_price),
                "reward": abs(tp_price - current_price),
                "rr_ratio": abs(tp_price - current_price) / abs(current_price - sl_price),
                "trend_bias": trend_bias_hint.value,
                "quality": {
                    "total_score": 2.2,  # fixed moderate score
                    "tier": "B"
                },
                "forced_move_probability": "MODERATE",
                "method": "MOMENTUM_SCALP"
            }
            return setup
        except Exception as e:
            log.debug(f"FastMomentumScalper error {symbol}: {e}")
            return None

    def _calc_rsi(self, prices, period=14):
        delta = np.diff(prices)
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)
        avg_gain = np.convolve(gain, np.ones(period)/period, mode='valid')
        avg_loss = np.convolve(loss, np.ones(period)/period, mode='valid')
        with np.errstate(divide='ignore', invalid='ignore'):
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
        rsi_full = np.pad(rsi, (period, 0), constant_values=50)
        return rsi_full[-1]

fast_scalper = FastMomentumScalper()

# ============ SIGNAL TRACKER (PRESERVED) ============
class SignalTracker:
    def __init__(self):
        self.active_signals = {}
        self.outcome_stats = {
            'total_signals': 0, 'tp1_hits': 0, 'tp2_hits': 0, 'tp3_hits': 0,
            'sl_hits': 0, 'expired': 0, 'active': 0, 'win_rate': 0.0, 'avg_pnl_pct': 0.0
        }
        self.bucket_hits = {}
    
    def get_signal_key(self, setup: Dict) -> tuple:
        symbol = setup.get('symbol', '')
        side = setup.get('side', '')
        quality_score = setup.get('quality', {}).get('total_score', 0)
        bucket = math.floor(quality_score * 2) / 2
        return (symbol, side, bucket)
    
    def should_send_alert(self, setup: Dict) -> bool:
        key = self.get_signal_key(setup)
        if key in self.active_signals:
            signal = self.active_signals[key]
            if signal.get('status') == 'active':
                now = datetime.datetime.utcnow()
                age_minutes = (now - signal['first_seen']).total_seconds() / 60
                if age_minutes > (SIGNAL_VALIDITY_HOURS * 60):
                    self.remove_signal_by_key(key, f"Expired after {SIGNAL_VALIDITY_HOURS}h")
                    return True
                return False
        return True
    
    def update_signal(self, setup: Dict, alerted: bool = False):
        key = self.get_signal_key(setup)
        now = datetime.datetime.utcnow()
        
        if key not in self.active_signals:
            self.active_signals[key] = {
                'setup': setup, 'first_seen': now, 'last_alerted': now if alerted else None,
                'last_checked': now, 'alert_count': 1 if alerted else 0, 'status': 'active',
                'outcome': 'active', 'highest_price': setup.get('current_price', 0),
                'lowest_price': setup.get('current_price', 0),
                'price_at_alert': setup.get('current_price', 0) if alerted else None
            }
            self.outcome_stats['total_signals'] += 1
            self.outcome_stats['active'] += 1
        else:
            current_price = setup.get('current_price', 0)
            self.active_signals[key]['highest_price'] = max(self.active_signals[key]['highest_price'], current_price)
            self.active_signals[key]['lowest_price'] = min(self.active_signals[key]['lowest_price'], current_price)
            self.active_signals[key]['last_checked'] = now
    
    def check_signal_outcome(self, setup: Dict, current_price: float) -> Optional[Dict]:
        key = self.get_signal_key(setup)
        if key not in self.active_signals:
            return None
        
        signal = self.active_signals[key]
        if signal.get('status') != 'active':
            return None
        
        now = datetime.datetime.utcnow()
        time_since_alert = (now - signal['first_seen']).total_seconds()
        if time_since_alert < 180:
            return None
        
        setup_data = signal.get('setup', {})
        if not setup_data:
            return None
        
        side = setup_data.get('side', '')
        entry = setup_data.get('entry_price', 0)
        tp_targets = setup_data.get('tp_targets', [])
        sl = setup_data.get('sl_price', 0)
        
        if entry == 0:
            return None
        
        outcome = None
        
        for i, tp in enumerate(tp_targets):
            if tp == 0:
                continue
            if side == "BUY" and current_price >= tp:
                pnl_pct = (current_price - entry) / entry * 100
                outcome = {'type': f'TP{i+1}_HIT', 'price': current_price, 'pnl_pct': pnl_pct,
                          'bars_held': int(time_since_alert / 60), 'max_favorable': (signal['highest_price'] - entry) / entry * 100,
                          'max_adverse': (entry - signal['lowest_price']) / entry * 100, 'tp_level': i+1}
                break
            elif side == "SELL" and current_price <= tp:
                pnl_pct = (entry - current_price) / entry * 100
                outcome = {'type': f'TP{i+1}_HIT', 'price': current_price, 'pnl_pct': pnl_pct,
                          'bars_held': int(time_since_alert / 60), 'max_favorable': (entry - signal['lowest_price']) / entry * 100,
                          'max_adverse': (signal['highest_price'] - entry) / entry * 100, 'tp_level': i+1}
                break
        
        if not outcome and sl > 0:
            if (side == "BUY" and current_price <= sl) or (side == "SELL" and current_price >= sl):
                pnl_pct = (current_price - entry) / entry * 100 if side == "BUY" else (entry - current_price) / entry * 100
                outcome = {'type': 'SL_HIT', 'price': current_price, 'pnl_pct': pnl_pct,
                          'bars_held': int(time_since_alert / 60), 'max_favorable': 0, 'max_adverse': 0}
        
        if outcome:
            signal['status'] = 'closed'
            signal['outcome'] = outcome.get('type', '').lower()
            self.outcome_stats['active'] -= 1
            
            if 'TP1_HIT' in outcome.get('type', ''):
                self.outcome_stats['tp1_hits'] += 1
            elif 'TP2_HIT' in outcome.get('type', ''):
                self.outcome_stats['tp2_hits'] += 1
            elif 'TP3_HIT' in outcome.get('type', ''):
                self.outcome_stats['tp3_hits'] += 1
            elif outcome.get('type') == 'SL_HIT':
                self.outcome_stats['sl_hits'] += 1
            
            wins = self.outcome_stats['tp1_hits'] + self.outcome_stats['tp2_hits'] + self.outcome_stats['tp3_hits']
            total_closed = wins + self.outcome_stats['sl_hits']
            if total_closed > 0:
                self.outcome_stats['win_rate'] = wins / total_closed * 100
        
        return outcome
    
    def remove_signal_by_key(self, key: tuple, reason: str = "expired"):
        if key in self.active_signals:
            self.active_signals.pop(key)
            self.outcome_stats['active'] -= 1
            self.outcome_stats['expired'] += 1
    
    def cleanup_old_signals(self):
        now = datetime.datetime.utcnow()
        expired_keys = []
        for key, data in self.active_signals.items():
            if data.get('status') == 'active':
                age_minutes = (now - data['first_seen']).total_seconds() / 60
                if age_minutes > (SIGNAL_VALIDITY_HOURS * 60):
                    expired_keys.append(key)
        for key in expired_keys:
            self.remove_signal_by_key(key, f"Expired after {SIGNAL_VALIDITY_HOURS}h")
    
    def get_stats(self) -> Dict:
        active_count = len([s for s in self.active_signals.values() if s.get('status') == 'active'])
        return {'active_signals': active_count, 'outcome_stats': self.outcome_stats}

signal_tracker = SignalTracker()
db_lock = asyncio.Lock()
db_conn = None

# ============ TELEGRAM ============
async def send_telegram(msg: str, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": parse_mode, "disable_web_page_preview": True})
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

# ============ ALERT FORMATTING ============
async def send_v6_alert(setup: Dict):
    """v6.1 alert without partial TP, just the main setup."""
    try:
        symbol = setup.get('symbol', 'UNKNOWN')
        side = setup.get('side', '')
        quality = setup.get('quality', {})
        wave = setup.get('wave_structure', {})
        momentum = setup.get('momentum_signals', {})
        volume = setup.get('volume_breakout', {})
        direction = setup.get('direction_metrics', {})
        liquidity = setup.get('liquidity_analysis', {})
        
        entry_price = setup.get('entry_price', 0)
        current_price = setup.get('current_price', 0)
        tp_targets = setup.get('tp_targets', [])
        sl_price = setup.get('sl_price', 0)
        rr_ratio = setup.get('rr_ratio', 0)
        
        # Determine signal tier
        quality_score = quality.get('total_score', 0)
        wave_confidence = getattr(wave, 'pattern_confidence', 0) if isinstance(wave, WaveStructure) else wave.get('pattern_confidence', 0)
        momentum_score = getattr(momentum, 'momentum_score', 0) if isinstance(momentum, MomentumSignals) else momentum.get('momentum_score', 0)
        volume_triggered = getattr(volume, 'triggered', False) if isinstance(volume, VolumeBreakout) else volume.get('triggered', False)
        
        if quality_score >= 4.5 and wave_confidence >= 0.7 and momentum_score >= 0.7 and volume_triggered:
            signal_tier = "S+"
            tier_emoji = "🔮"
        elif quality_score >= 3.5 and momentum_score >= 0.6 and volume_triggered:
            signal_tier = "A+"
            tier_emoji = "🔥"
        elif quality_score >= 2.5:
            signal_tier = "A"
            tier_emoji = "✅"
        elif quality_score >= 2.0:
            signal_tier = "B"
            tier_emoji = "⚠️"
        else:
            signal_tier = "C"
            tier_emoji = "📊"
        
        # Wave details
        wave_lines = []
        if isinstance(wave, WaveStructure) and wave.pattern != WavePattern.NONE:
            wave_lines.append(f"📐 Pattern: {wave.pattern.value} (Conf: {wave.pattern_confidence:.0%})")
            wave_lines.append(f"📏 Impulse: {wave.impulse_size_pct:.1f}% | Retrace: {wave.current_retracement:.0%}")
            wave_lines.append(f"🎯 Zone: {wave.fib_500:.8f} - {wave.fib_705:.8f}")
            if wave.in_optimal_zone:
                wave_lines.append("📍 IN OPTIMAL ZONE ✅")
            else:
                wave_lines.append(f"📍 Distance to zone: {wave.distance_to_zone_pct:.1f}%")
        
        # Momentum details
        momentum_lines = []
        if isinstance(momentum, MomentumSignals):
            div_emoji = "🐂" if momentum.divergence_type == DivergenceType.BULLISH_REGULAR else "🐻" if momentum.divergence_type == DivergenceType.BEARISH_REGULAR else "⚪"
            momentum_lines.append(f"{div_emoji} Divergence: {momentum.divergence_type.value} ({momentum.divergence_strength:.0%})")
            macd_emoji = "✅" if momentum.macd_crossed else "🔄" if momentum.macd_histogram_reversal else "⏳"
            momentum_lines.append(f"{macd_emoji} MACD Cross: {'Bullish' if momentum.macd_cross_direction == 'BULLISH' else 'Bearish' if momentum.macd_cross_direction == 'BEARISH' else 'None'}")
            momentum_lines.append(f"📊 RSI: {momentum.rsi_current:.1f} | Score: {momentum.momentum_score:.0%}")
            if momentum.momentum_aligned:
                momentum_lines.append("🎯 MOMENTUM ALIGNED ✅")
        
        # Volume details
        volume_lines = []
        if isinstance(volume, VolumeBreakout):
            if volume.triggered:
                volume_lines.append(f"🚀 BREAKOUT: Volume {volume.volume_ratio:.1f}x avg")
                volume_lines.append(f"📊 Avg Vol: {volume.avg_volume_20:.0f} | Candle: {volume.breakout_candle_volume:.0f}")
                if volume.sweep_then_reclaim:
                    volume_lines.append("🧹 SWEEP + RECLAIM detected!")
            else:
                volume_lines.append("⏳ Waiting for volume confirmation...")
        
        # TP lines
        tp_lines = []
        for i, tp in enumerate(tp_targets):
            if entry_price > 0:
                distance_pct = abs(tp - entry_price) / entry_price * 100
                tp_lines.append(f"TP{i+1}: {tp:.8f} ({distance_pct:.1f}%)")
        
        # Direction context
        direction_context = ""
        if isinstance(direction, DirectionMetrics):
            if direction.trapped_side != TrappedSide.NONE:
                direction_context += f" | Trapped: {direction.trapped_side.value}"
        
        msg = f"""{tier_emoji} <b>ROMEOTPT v6.1 - {symbol} | {side}</b>
<b>Tier: {signal_tier} | Wave-Momentum Breakout</b>

<b>📐 WAVE STRUCTURE:</b>
{chr(10).join(wave_lines) if wave_lines else 'No wave pattern detected'}

<b>📈 MOMENTUM:</b>
{chr(10).join(momentum_lines) if momentum_lines else 'No momentum signals'}

<b>📊 VOLUME:</b>
{chr(10).join(volume_lines) if volume_lines else 'No volume data'}

<b>🎯 SETUP:</b>
Entry: <code>{entry_price:.8f}</code> | Now: <code>{current_price:.8f}</code>
{chr(10).join(tp_lines)}
🛡️ SL: <code>{sl_price:.8f}</code>

⚖️ RR: <b>{rr_ratio:.1f}:1</b>{direction_context}

🏆 Quality: {quality_score:.1f}/5.0 ({quality.get('tier', 'C')})
📊 Forced Move: {setup.get('forced_move_probability', 'LOW')}

<i>Wave+Momentum+Volume Method | {datetime.datetime.utcnow().strftime('%H:%M:%S UTC')}</i>
"""
        await send_telegram(msg)
    except Exception as e:
        log.error(f"Error sending v6 alert: {e}")

async def send_deduped_v6_alert(setup: Dict):
    try:
        should_alert = signal_tracker.should_send_alert(setup)
        if should_alert:
            await send_v6_alert(setup)
            signal_tracker.update_signal(setup, alerted=True)
            return True
        else:
            signal_tracker.update_signal(setup, alerted=False)
            return False
    except Exception as e:
        log.error(f"Deduped alert error: {e}")
        return False

# ============ DATABASE ============
async def init_database():
    global db_conn
    try:
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signals_v6_1 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, side TEXT, score REAL, timestamp TEXT,
                entry_price REAL, sl_price REAL, tp1 REAL, tp2 REAL, tp3 REAL,
                rr_ratio REAL, quality_tier TEXT, quality_score REAL,
                current_price REAL, trend_bias TEXT,
                wave_pattern TEXT, wave_confidence REAL, fib_retracement REAL,
                in_optimal_zone BOOLEAN, divergence_type TEXT, divergence_strength REAL,
                momentum_score REAL, macd_crossed BOOLEAN, momentum_aligned BOOLEAN,
                volume_triggered BOOLEAN, volume_ratio REAL, sweep_reclaim BOOLEAN,
                direction_tier TEXT, direction_score REAL, trapped_side TEXT,
                status TEXT DEFAULT 'active', alert_sent BOOLEAN DEFAULT 1,
                closed_at TEXT, closed_price REAL, outcome TEXT, pnl_pct REAL,
                UNIQUE(symbol, side, score)
            )
        """)
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_v6_1_signals_status ON signals_v6_1 (status)")
        await db_conn.commit()
        log.info("✅ Database v6.1 initialized")
    except Exception as e:
        log.error(f"Database init error: {e}")

async def store_signal(setup: Dict):
    async with db_lock:
        try:
            tp_targets = setup.get("tp_targets", [])
            quality = setup.get("quality", {})
            wave = setup.get("wave_structure", {})
            momentum = setup.get("momentum_signals", {})
            volume = setup.get("volume_breakout", {})
            direction = setup.get("direction_metrics", {})
            
            key = signal_tracker.get_signal_key(setup)
            _, _, bucket = key
            
            # Extract wave data
            if isinstance(wave, WaveStructure):
                wave_pattern = wave.pattern.value
                wave_conf = wave.pattern_confidence
                fib_ret = wave.current_retracement
                in_zone = wave.in_optimal_zone
            else:
                wave_pattern = wave.get('pattern', 'NONE') if isinstance(wave, dict) else 'NONE'
                wave_conf = wave.get('pattern_confidence', 0) if isinstance(wave, dict) else 0
                fib_ret = wave.get('current_retracement', 0) if isinstance(wave, dict) else 0
                in_zone = wave.get('in_optimal_zone', False) if isinstance(wave, dict) else False
            
            # Extract momentum data
            if isinstance(momentum, MomentumSignals):
                div_type = momentum.divergence_type.value
                div_strength = momentum.divergence_strength
                mom_score = momentum.momentum_score
                macd_cross = momentum.macd_crossed
                mom_aligned = momentum.momentum_aligned
            else:
                div_type = momentum.get('divergence_type', 'NONE') if isinstance(momentum, dict) else 'NONE'
                div_strength = momentum.get('divergence_strength', 0) if isinstance(momentum, dict) else 0
                mom_score = momentum.get('momentum_score', 0) if isinstance(momentum, dict) else 0
                macd_cross = momentum.get('macd_crossed', False) if isinstance(momentum, dict) else False
                mom_aligned = momentum.get('momentum_aligned', False) if isinstance(momentum, dict) else False
            
            # Extract volume data
            if isinstance(volume, VolumeBreakout):
                vol_triggered = volume.triggered
                vol_ratio = volume.volume_ratio
                sweep = volume.sweep_then_reclaim
            else:
                vol_triggered = volume.get('triggered', False) if isinstance(volume, dict) else False
                vol_ratio = volume.get('volume_ratio', 0) if isinstance(volume, dict) else 0
                sweep = volume.get('sweep_then_reclaim', False) if isinstance(volume, dict) else False
            
            # Extract direction data
            if isinstance(direction, DirectionMetrics):
                dir_tier = direction.confidence_tier.value
                dir_score = direction.direction_score
                trapped = direction.trapped_side.value
            else:
                dir_tier = direction.get('confidence_tier', 'LOW') if isinstance(direction, dict) else 'LOW'
                dir_score = direction.get('direction_score', 0) if isinstance(direction, dict) else 0
                trapped = direction.get('trapped_side', 'NONE') if isinstance(direction, dict) else 'NONE'
            
            await db_conn.execute("""
                INSERT OR REPLACE INTO signals_v6_1 (
                    symbol, side, score, timestamp, entry_price, sl_price, tp1, tp2, tp3,
                    rr_ratio, quality_tier, quality_score, current_price, trend_bias,
                    wave_pattern, wave_confidence, fib_retracement, in_optimal_zone,
                    divergence_type, divergence_strength, momentum_score, macd_crossed, momentum_aligned,
                    volume_triggered, volume_ratio, sweep_reclaim,
                    direction_tier, direction_score, trapped_side, status, alert_sent
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                setup.get("symbol", ""), setup.get("side", ""), float(bucket),
                setup.get("timestamp", ""), float(setup.get("entry_price", 0)),
                float(setup.get("sl_price", 0)),
                float(tp_targets[0]) if len(tp_targets) > 0 else None,
                float(tp_targets[1]) if len(tp_targets) > 1 else None,
                float(tp_targets[2]) if len(tp_targets) > 2 else None,
                float(setup.get("rr_ratio", 0)), quality.get("tier", "C"),
                float(quality.get("total_score", 0)), float(setup.get("current_price", 0)),
                setup.get("trend_bias", "NEUTRAL"),
                wave_pattern, float(wave_conf), float(fib_ret), 1 if in_zone else 0,
                div_type, float(div_strength), float(mom_score), 1 if macd_cross else 0, 1 if mom_aligned else 0,
                1 if vol_triggered else 0, float(vol_ratio), 1 if sweep else 0,
                dir_tier, float(dir_score), trapped, 'active', 1
            ))
            await db_conn.commit()
        except Exception as e:
            log.error(f"Error storing signal: {e}")

# ============ ENHANCED SCANNER v6.1 ============
async def scan_symbol_v6(exchange, symbol: str) -> Optional[Dict]:
    """
    v6.1 scanner: Wave Range → Momentum → Volume Breakout Method
    With Orderbook Bias and fallback to fast scalp.
    """
    try:
        # ======= STEP 0: Fetch ALL timeframes =======
        df_daily = create_dataframe(await fetch_ohlcv(exchange, symbol, "1d", 200))
        df_4h = create_dataframe(await fetch_ohlcv(exchange, symbol, "4h", 100))
        df_1h = create_dataframe(await fetch_ohlcv(exchange, symbol, "1h", 100))
        df_15m = create_dataframe(await fetch_ohlcv(exchange, symbol, "15m", 100))
        df_5m = create_dataframe(await fetch_ohlcv(exchange, symbol, "5m", 50))
        
        if df_daily is None or df_4h is None or df_1h is None:
            return None
        
        ticker = await safe_fetch_ticker(exchange, symbol)
        if not ticker:
            return None
        current_price = ticker.get('last', 0)
        if current_price <= 0:
            return None
        
        # ======= STEP 1: MACRO DIRECTION =======
        trend_bias, trend_score = wave_detector.detect_trend_bias(df_daily, df_4h)
        
        if trend_bias == TrendBias.NEUTRAL:
            log.debug(f"{symbol}: NEUTRAL trend - skipped")
            return None
        
        # ======= STEP 2: WAVE RANGE =======
        wave = wave_detector.identify_abc_correction(df_4h, trend_bias)
        
        if wave.pattern == WavePattern.NONE:
            log.debug(f"{symbol}: No ABC correction pattern")
            # fallback later
            return None
        
        if wave.pattern_confidence < 0.3:
            log.debug(f"{symbol}: Low wave confidence ({wave.pattern_confidence:.2f})")
            return None
        
        # ======= STEP 3: MOMENTUM =======
        momentum = momentum_engine.analyze_momentum(df_1h, df_15m, trend_bias, wave)
        
        if momentum.divergence_type == DivergenceType.NONE and momentum.momentum_score < 0.3:
            log.debug(f"{symbol}: No divergence and low momentum")
            return None
        
        # ======= STEP 4: VOLUME BREAKOUT =======
        entry_price = current_price
        entry_type = "FIB_ZONE"
        
        if trend_bias == TrendBias.BULLISH:
            side = "BUY"
            entry_type = "DISCOUNT_FIB_ZONE"
        else:
            side = "SELL"
            entry_type = "PREMIUM_FIB_ZONE"
        
        volume_breakout = volume_trigger.detect_breakout(df_5m, df_15m, trend_bias, wave, entry_price)
        
        # Order book bias filter
        ob_bias = await data_fetcher.get_orderbook_bias(exchange, symbol)
        if side == "BUY" and ob_bias > 1.5:
            volume_breakout.volume_score += 0.1
        elif side == "SELL" and ob_bias < 0.67:
            volume_breakout.volume_score += 0.1
        volume_breakout.volume_score = min(1.0, volume_breakout.volume_score)
        
        # ======= STEP 5: LIQUIDITY TP/SL =======
        sl_price, tp_targets, tp_sources, liquidity_analysis = await calculate_liquidity_tp_sl(
            exchange, symbol, side, entry_price, entry_type
        )
        
        if sl_price <= 0 or not tp_targets:
            log.debug(f"{symbol}: No valid TP/SL from liquidity")
            return None
        
        risk = abs(entry_price - sl_price)
        reward = abs(tp_targets[0] - entry_price) if tp_targets else 0
        rr_ratio = reward / risk if risk > 0 else 0
        
        if rr_ratio < 1.5:
            log.debug(f"{symbol}: Low RR ratio ({rr_ratio:.1f})")
            return None
        
        # ======= STEP 6: DIRECTION ENGINE =======
        direction_metrics = await direction_engine.analyze_direction(
            exchange, symbol, side, current_price
        )
        
        # ======= STEP 7: QUALITY SCORING =======
        quality_score = 0.0
        quality_score += wave.pattern_confidence * 1.5
        quality_score += momentum.momentum_score * 1.5
        quality_score += volume_breakout.volume_score * 1.0
        if wave.in_optimal_zone:
            quality_score += 0.5
        if direction_metrics.confidence_tier == DirectionTier.HIGH:
            quality_score += 0.5
        elif direction_metrics.confidence_tier == DirectionTier.MEDIUM:
            quality_score += 0.3
        
        if quality_score >= 4.5:
            tier = "S+"
        elif quality_score >= 4.0:
            tier = "A+"
        elif quality_score >= 3.0:
            tier = "A"
        elif quality_score >= 2.5:
            tier = "B"
        else:
            tier = "C"
        
        if quality_score < MIN_QUALITY_SCORE:
            return None
        
        # ======= BUILD SETUP =======
        setup = {
            "symbol": symbol,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "side": side,
            "current_price": current_price,
            "entry_price": entry_price,
            "entry_type": entry_type,
            "sl_price": sl_price,
            "tp_targets": tp_targets,
            "tp_sources": tp_sources,
            "risk": risk,
            "reward": reward,
            "rr_ratio": rr_ratio,
            "trend_bias": trend_bias.value,
            "wave_structure": wave,
            "momentum_signals": momentum,
            "volume_breakout": volume_breakout,
            "quality": {
                "tier": tier,
                "total_score": quality_score,
                "trend_score": trend_score,
                "wave_confidence": wave.pattern_confidence,
                "momentum_score": momentum.momentum_score,
                "volume_score": volume_breakout.volume_score
            },
            "liquidity_analysis": liquidity_analysis,
            "direction_metrics": direction_metrics,
            "forced_move_probability": (
                "HIGH" if (wave.in_optimal_zone and momentum.momentum_aligned and volume_breakout.triggered and quality_score >= 3.5)
                else "MODERATE" if (momentum.momentum_aligned and quality_score >= 2.5)
                else "LOW"
            ),
            "method": "WAVE_MOMENTUM"
        }
        
        return setup
        
    except Exception as e:
        log.error(f"v6 scanner error for {symbol}: {e}")
        return None
    finally:
        # If primary method didn't produce a setup, try fast scalp
        # This is done outside the try-except to avoid scraper fallback interference
        pass

async def main_scan_with_fallback(exchange, symbol):
    result = await scan_symbol_v6(exchange, symbol)
    if result is not None:
        return result
    
    # fallback: fast momentum scalp
    ticker = await safe_fetch_ticker(exchange, symbol)
    if not ticker:
        return None
    current_price = ticker.get('last', 0)
    if current_price <= 0:
        return None
    
    # We need a rough trend, but we can guess from 15m data
    df_15m = create_dataframe(await fetch_ohlcv(exchange, symbol, "15m", 50))
    if df_15m is not None and len(df_15m) > 20:
        trend_bias_hint = TrendBias.BULLISH if df_15m['close'].iloc[-1] > df_15m['close'].iloc[-20:].mean() else TrendBias.BEARISH
    else:
        trend_bias_hint = TrendBias.NEUTRAL
    
    scalp_setup = await fast_scalper.scan(exchange, symbol, current_price, trend_bias_hint)
    if scalp_setup:
        # Add direction metrics if available (optional)
        try:
            scalp_setup['direction_metrics'] = await direction_engine.analyze_direction(
                exchange, symbol, scalp_setup.get('side', 'BUY'), current_price
            )
        except:
            scalp_setup['direction_metrics'] = DirectionMetrics()
        return scalp_setup
    return None

# ============ OUTCOME MONITOR ============
class OutcomeMonitor:
    """
    Periodically checks all active signals in DB,
    sends Telegram alert when TP/SL hit, updates DB.
    """
    def __init__(self, exchange, interval=OUTCOME_CHECK_INTERVAL):
        self.exchange = exchange
        self.interval = interval

    async def monitor_loop(self):
        log.info("OUTCOME MONITOR STARTED")
        while True:
            try:
                async with db_lock:
                    cursor = await db_conn.execute(
                        "SELECT symbol, side, entry_price, sl_price, tp1, tp2, tp3, id "
                        "FROM signals_v6_1 WHERE status='active'"
                    )
                    rows = await cursor.fetchall()
                if not rows:
                    await asyncio.sleep(self.interval)
                    continue

                symbols = set(row[0] for row in rows)
                tickers = {}
                for sym in symbols:
                    ticker = await safe_fetch_ticker(self.exchange, sym)
                    if ticker:
                        tickers[sym] = ticker.get('last', 0)

                for (symbol, side, entry, sl, tp1, tp2, tp3, sig_id) in rows:
                    price = tickers.get(symbol)
                    if not price or price <= 0:
                        continue
                    outcome = None
                    tp_level = 0
                    # Check SL first
                    if (side == "BUY" and price <= sl) or (side == "SELL" and price >= sl):
                        outcome = "SL_HIT"
                    elif tp1 and ((side == "BUY" and price >= tp1) or (side == "SELL" and price <= tp1)):
                        outcome = "TP1_HIT"
                        tp_level = 1
                    elif tp2 and ((side == "BUY" and price >= tp2) or (side == "SELL" and price <= tp2)):
                        outcome = "TP2_HIT"
                        tp_level = 2
                    elif tp3 and ((side == "BUY" and price >= tp3) or (side == "SELL" and price <= tp3)):
                        outcome = "TP3_HIT"
                        tp_level = 3

                    if outcome:
                        pnl_pct = ((price - entry) / entry * 100) if side == "BUY" else ((entry - price) / entry * 100)
                        # Send alert
                        emoji = "✅" if outcome != "SL_HIT" else "❌"
                        msg = (
                            f"{emoji} <b>OUTCOME ALERT</b>\n"
                            f"<b>{symbol}</b> | {side}\n"
                            f"<b>{outcome}</b> at <code>{price:.8f}</code>\n"
                            f"Profit: <b>{pnl_pct:+.2f}%</b>\n"
                            f"Entry: <code>{entry:.8f}</code> | SL: <code>{sl:.8f}</code>"
                        )
                        if tp_level:
                            tp_price = [tp1, tp2, tp3][tp_level-1]
                            msg += f"\nTP{tp_level}: <code>{tp_price:.8f}</code>"
                        await send_telegram(msg)

                        # Update DB
                        async with db_lock:
                            await db_conn.execute(
                                "UPDATE signals_v6_1 SET status='closed', outcome=?, closed_at=?, closed_price=?, pnl_pct=? "
                                "WHERE id=?",
                                (outcome, datetime.datetime.utcnow().isoformat(), price, pnl_pct, sig_id)
                            )
                            await db_conn.commit()
                        log.info(f"Outcome processed: {symbol} {outcome} PnL={pnl_pct:.2f}%")

            except Exception as e:
                log.error(f"Outcome monitor error: {e}")
            await asyncio.sleep(self.interval)

# ============ MAIN SCANNER LOOP ============
async def v6_scanner_main(exchange):
    """Main v6.1 scanner loop"""
    
    startup_msg = f"""🚀 <b>ROMEOTPT v6.1 Started - WAVE MOMENTUM BREAKOUT + SCALP</b>
Scan: {SCAN_INTERVAL}s | Top {TOP_N} | Quality ≥{MIN_QUALITY_SCORE}
<b>Primary Method: SMA Trend → ABC Correction → RSI Divergence → MACD → Volume Breakout</b>
Secondary: Fast Scalp + Orderbook filter + Outcome alerts
Fibonacci Zone: {OPTIMAL_FIB_ZONE_MIN}-{OPTIMAL_FIB_ZONE_MAX}
Volume Spike: {VOLUME_SPIKE_MULTIPLIER}x average"""
    await send_telegram(startup_msg)
    
    # Start outcome monitor
    monitor = OutcomeMonitor(exchange)
    asyncio.create_task(monitor.monitor_loop())
    
    scan_cycle = 0
    
    while True:
        scan_cycle += 1        
        try:
            tickers = await safe_fetch_tickers(exchange)
            usdt_pairs = []
            
            for symbol, data in tickers.items():
                if symbol.endswith("/USDT") and not symbol.startswith("USDT"):
                    volume = data.get("quoteVolume", 0)
                    if isinstance(volume, (int, float)) and volume > 100000:
                        usdt_pairs.append((symbol, float(volume)))
            
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            symbols_to_scan = [s[0] for s in usdt_pairs[:TOP_N]]
            
            stats = signal_tracker.get_stats()
            log.info(f"🔄 v6.1 Scan #{scan_cycle}: {len(symbols_to_scan)} symbols | Active signals: {stats.get('active_signals', 0)}")
            
            alerts_this_scan = 0
            tasks = []
            
            for symbol in symbols_to_scan:
                task = asyncio.create_task(main_scan_with_fallback(exchange, symbol))
                tasks.append(task)
                
                if len(tasks) >= 3:  # Process in small batches
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for result in results:
                        if isinstance(result, Exception):
                            continue
                        if result:
                            alerted = await send_deduped_v6_alert(result)
                            if alerted:
                                alerts_this_scan += 1
                            await store_signal(result)
                    
                    tasks = []
                    await asyncio.sleep(0.3)
            
            # Process remaining
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        continue
                    if result:
                        alerted = await send_deduped_v6_alert(result)
                        if alerted:
                            alerts_this_scan += 1
                        await store_signal(result)
            
            signal_tracker.cleanup_old_signals()
            
            if scan_cycle % 5 == 0:
                outcome_stats = stats.get('outcome_stats', {})
                wins = outcome_stats.get('tp1_hits', 0) + outcome_stats.get('tp2_hits', 0) + outcome_stats.get('tp3_hits', 0)
                losses = outcome_stats.get('sl_hits', 0)
                total = wins + losses
                if total > 0:
                    log.info(f"📈 Stats: WR={outcome_stats.get('win_rate', 0):.1f}% | Active={outcome_stats.get('active', 0)}")
            
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"Scanner error: {e}")
            await asyncio.sleep(SCAN_INTERVAL * 2)

# ============ FASTAPI ============
app = FastAPI()

@app.get("/health")
async def health():
    stats = signal_tracker.get_stats()
    return {
        "status": "healthy",
        "version": "6.1 - WAVE MOMENTUM BREAKOUT + SCALP + OUTCOME ALERTS",
        "method": "SMA Trend → ABC Correction → RSI Divergence → MACD → Volume Breakout + Fast Scalp",
        "active_signals": stats.get('active_signals', 0),
        "outcome_stats": stats.get('outcome_stats', {})
    }

@app.get("/signals/active")
async def get_active_signals():
    active = []
    for key, data in signal_tracker.active_signals.items():
        if data.get('status') == 'active':
            symbol, side, bucket = key
            setup = data.get('setup', {})
            active.append({
                "symbol": symbol,
                "side": side,
                "quality_score": setup.get('quality', {}).get('total_score', 0),
                "quality_tier": setup.get('quality', {}).get('tier', 'C'),
                "trend_bias": setup.get('trend_bias', 'NEUTRAL'),
                "entry_price": setup.get('entry_price', 0),
                "current_price": setup.get('current_price', 0),
                "sl": setup.get('sl_price', 0),
                "tp1": setup.get('tp_targets', [0])[0] if len(setup.get('tp_targets', [])) > 0 else 0,
                "rr_ratio": setup.get('rr_ratio', 0),
                "forced_move_probability": setup.get('forced_move_probability', 'LOW'),
                "age_minutes": (datetime.datetime.utcnow() - data.get('first_seen', datetime.datetime.utcnow())).total_seconds() / 60
            })
    return {"active_signals": active, "count": len(active)}

# ============ MAIN ============
async def main():
    global db_conn
    
    try:
        db_conn = await aiosqlite.connect(DB_PATH)
        await init_database()
        
        exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {
                "defaultType": "spot",
                "fetchOpenInterest": True,
                "fetchFundingRateHistory": True
            },
            "rateLimit": 500,
            "timeout": 30000,
            "verbose": False,
        })
        
        log.info("🚀 ROMEOTPT v6.1 - WAVE MOMENTUM BREAKOUT + SCALP")
        log.info(f"Fibonacci Zone: {OPTIMAL_FIB_ZONE_MIN}-{OPTIMAL_FIB_ZONE_MAX}")
        log.info(f"Volume Spike Threshold: {VOLUME_SPIKE_MULTIPLIER}x")
        log.info(f"Scan: {SCAN_INTERVAL}s | Top {TOP_N} symbols")
        
        await v6_scanner_main(exchange)
        
    except Exception as e:
        log.error(f"Fatal error: {e}")
        import traceback
        log.error(f"Traceback: {traceback.format_exc()}")
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