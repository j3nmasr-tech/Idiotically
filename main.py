#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MEXC FUTURES PUMP & DUMP SNIPER v1.0
SPECIALIZATION: Low-cap pump AND dump detection on MEXC Futures
FOCUS: Identify accumulation before pumps, distribution before dumps
STRATEGY: Long accumulation breakouts, Short distribution breakdowns
"""

import os
import sys
import time
import asyncio
import logging
import hashlib
import traceback
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, asdict
import json
from collections import deque

# ================ CONFIGURATION ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH = os.getenv("DB_PATH", "/app/data/mexc_pump_dump.db")

# Exchange config
EXCHANGE_NAME = "mexc"
CONTRACT_TYPE = "swap"
LEVERAGE = 3
MARGIN_MODE = "isolated"

# Low-cap targeting
MIN_MARKET_CAP = 500_000
MAX_MARKET_CAP = 20_000_000
MIN_PRICE = 0.00001
MAX_PRICE = 0.50
MIN_VOLUME_24H = 100_000

# Detection parameters
VOLUME_SPIKE_THRESHOLD = 3.5
VOLUME_DECLINE_THRESHOLD = 0.4
PRICE_COMPRESSION_THRESHOLD = 0.15
PARABOLIC_THRESHOLD = 25.0
VOLUME_CLIMAX_RATIO = 2.8
RSI_OVERBOUGHT = 75
RSI_OVERSOLD = 25

# Trade parameters
TARGET_PROFIT_PUMP = (8.0, 20.0)
TARGET_PROFIT_DUMP = (6.0, 15.0)
MAX_STOP_LOSS = 3.0
MIN_RISK_REWARD = 3.0
MIN_CONFLUENCE_SCORE = 2.5

# Timeframes
TIMEFRAMES = {
    "4H": "4h",
    "1H": "1h",
    "15M": "15m",
    "5M": "5m",
    "1M": "1m"
}

# ================ LOGGING SETUP ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("mexc_pump_dump")

# ================ UTILITY FUNCTIONS ================
def calculate_rsi(close_prices: pd.Series, period: int = 14) -> pd.Series:
    """Calculate RSI"""
    delta = close_prices.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_ema(prices: pd.Series, period: int) -> pd.Series:
    """Calculate EMA"""
    return prices.ewm(span=period, adjust=False).mean()

def calculate_bollinger_bands(prices: pd.Series, period: int = 20, std_dev: float = 2.0):
    """Calculate Bollinger Bands"""
    sma = prices.rolling(window=period).mean()
    std = prices.rolling(window=period).std()
    
    upper_band = sma + (std * std_dev)
    lower_band = sma - (std * std_dev)
    bandwidth = (upper_band - lower_band) / sma
    
    return upper_band, lower_band, bandwidth

# ================ LOGIC BREAKDOWN CLASSES ================
@dataclass
class AccumulationLogic:
    """Accumulation phase logic breakdown"""
    price_compression_pct: float = 0.0
    compression_score: float = 0.0
    days_in_accumulation: float = 0.0
    accumulation_candles: int = 0
    volume_trend: str = "neutral"
    volume_accumulation_score: float = 0.0
    lower_wick_absorption: float = 0.0
    wick_score: float = 0.0
    order_blocks_present: bool = False
    order_block_count: int = 0
    liquidity_sweeps_detected: bool = False
    stop_hunts_detected: bool = False
    liquidity_score: float = 0.0
    higher_lows_count: int = 0
    structure_score: float = 0.0
    accumulation_confidence: float = 0.0
    accumulation_phase: str = "none"
    ready_for_breakout: bool = False

@dataclass
class VolumeAnomalyLogic:
    """Volume anomaly logic breakdown"""
    volume_spike_ratio: float = 1.0
    spike_confidence: float = 0.0
    spike_duration_candles: int = 0
    buy_volume_pct: float = 50.0
    sell_volume_pct: float = 50.0
    volume_imbalance: float = 0.0
    large_buy_orders: int = 0
    large_sell_orders: int = 0
    large_order_ratio: float = 0.0
    price_volume_divergence: str = "none"
    divergence_strength: float = 0.0
    volume_climax: bool = False
    climax_ratio: float = 1.0
    volume_anomaly_score: float = 0.0
    anomaly_type: str = "none"
    anomaly_confidence: float = 0.0

@dataclass
class LiquidityEngineeringLogic:
    """Liquidity engineering logic breakdown"""
    stop_hunt_detected: bool = False
    stop_hunt_direction: str = "none"
    stop_hunt_strength: float = 0.0
    stop_hunt_level: float = 0.0
    fake_resistance_levels: List[float] = None
    fake_support_levels: List[float] = None
    fake_level_count: int = 0
    liquidity_clusters: Dict[str, float] = None
    cluster_strength: float = 0.0
    bid_wall_size: float = 0.0
    ask_wall_size: float = 0.0
    orderbook_imbalance: float = 0.0
    squeeze_detected: bool = False
    squeeze_strength: float = 0.0
    volatility_expansion_expected: bool = False
    engineering_score: float = 0.0
    manipulation_present: bool = False
    next_target_level: float = 0.0
    
    def __post_init__(self):
        if self.fake_resistance_levels is None:
            self.fake_resistance_levels = []
        if self.fake_support_levels is None:
            self.fake_support_levels = []
        if self.liquidity_clusters is None:
            self.liquidity_clusters = {}

@dataclass
class MomentumLogic:
    """Momentum logic breakdown"""
    rsi_value: float = 50.0
    rsi_zone: str = "neutral"
    rsi_divergence: str = "none"
    rsi_divergence_strength: float = 0.0
    macd_histogram: float = 0.0
    macd_trend: str = "neutral"
    macd_cross_signal: str = "none"
    candle_pattern: str = "none"
    pattern_strength: float = 0.0
    consecutive_green: int = 0
    consecutive_red: int = 0
    trend_strength: float = 0.0
    trend_direction: str = "neutral"
    ema_alignment: str = "neutral"
    volume_confirmation: bool = False
    volume_momentum: float = 0.0
    momentum_score: float = 0.0
    momentum_bias: str = "neutral"
    exhaustion_detected: bool = False

@dataclass
class BreakoutLogic:
    """Breakout logic breakdown"""
    breakout_type: str = "none"
    breakout_level: float = 0.0
    breakout_strength: float = 0.0
    breakout_volume_ratio: float = 1.0
    volume_confirmation: bool = False
    retest_occurred: bool = False
    retest_successful: bool = False
    retest_depth_pct: float = 0.0
    follow_through_candles: int = 0
    follow_through_strength: float = 0.0
    liquidity_grabbed: bool = False
    next_liquidity_level: float = 0.0
    breakout_quality: float = 0.0
    breakout_valid: bool = False
    expected_move_pct: float = 0.0

@dataclass
class RiskLogic:
    """Risk management logic"""
    position_size_pct: float = 1.0
    leverage: int = 1
    max_capital_risk: float = 1.0
    stop_loss_type: str = "technical"
    stop_loss_level: float = 0.0
    stop_loss_distance_pct: float = 0.0
    stop_loss_confidence: float = 0.0
    take_profit_levels: List[float] = None
    scale_out_percentages: List[float] = None
    risk_reward_ratio: float = 0.0
    probability_score: float = 0.0
    expectancy: float = 0.0
    market_regime: str = "neutral"
    volatility_adjusted: bool = False
    correlation_risk: float = 0.0
    
    def __post_init__(self):
        if self.take_profit_levels is None:
            self.take_profit_levels = []
        if self.scale_out_percentages is None:
            self.scale_out_percentages = []

@dataclass
class CompleteSignal:
    """Complete pump/dump signal"""
    signal_id: str
    symbol: str
    signal_type: str
    timestamp: float
    current_price: float
    market_cap: float
    volume_24h: float
    accumulation_logic: AccumulationLogic
    volume_logic: VolumeAnomalyLogic
    liquidity_logic: LiquidityEngineeringLogic
    momentum_logic: MomentumLogic
    breakout_logic: BreakoutLogic
    risk_logic: RiskLogic
    entry_price: float
    entry_type: str
    entry_conditions: List[str]
    stop_loss: float
    take_profit: float
    breakeven_level: float
    overall_score: float
    confidence_level: float
    urgency_level: str
    timeframes_analyzed: List[str]
    primary_timeframe: str
    status: str = "generated"

# ================ DETECTION ENGINES ================
class AccumulationDetector:
    def analyze(self, df_4h: pd.DataFrame, df_1h: pd.DataFrame) -> AccumulationLogic:
        logic = AccumulationLogic()
        
        if len(df_4h) < 50 or len(df_1h) < 100:
            return logic
        
        try:
            # Price compression
            recent_high = df_4h['high'].iloc[-30:].max()
            recent_low = df_4h['low'].iloc[-30:].min()
            price_range = recent_high - recent_low
            logic.price_compression_pct = (price_range / recent_low) * 100 if recent_low > 0 else 0
            
            # Compression score
            _, _, bb_width = calculate_bollinger_bands(df_4h['close'], period=20, std_dev=2)
            if not bb_width.empty:
                current_width = bb_width.iloc[-1]
                if current_width < 0.1:
                    logic.compression_score = 0.9
                elif current_width < 0.15:
                    logic.compression_score = 0.7
                elif current_width < 0.2:
                    logic.compression_score = 0.5
                else:
                    logic.compression_score = 0.3
            
            # Accumulation candles
            logic.accumulation_candles = self._count_accumulation_candles(df_4h, recent_low, recent_high)
            logic.days_in_accumulation = (logic.accumulation_candles * 4) / 24
            
            # Volume trend
            logic.volume_accumulation_score = self._analyze_volume_profile(df_4h)
            
            # Wick absorption
            logic.lower_wick_absorption = self._analyze_wicks(df_1h)
            
            # Confidence calculation
            logic.accumulation_confidence = (
                logic.compression_score * 0.3 +
                logic.volume_accumulation_score * 0.3 +
                logic.lower_wick_absorption * 0.4
            )
            
            logic.ready_for_breakout = logic.accumulation_confidence >= 0.7
            
            if logic.accumulation_confidence >= 0.8:
                logic.accumulation_phase = "late"
            elif logic.accumulation_confidence >= 0.6:
                logic.accumulation_phase = "mid"
            else:
                logic.accumulation_phase = "early"
                
        except Exception as e:
            log.error(f"Accumulation analysis error: {e}")
        
        return logic
    
    def _count_accumulation_candles(self, df: pd.DataFrame, support: float, resistance: float) -> int:
        count = 0
        lookback = min(100, len(df))
        
        for i in range(lookback):
            idx = -i - 1
            candle = df.iloc[idx]
            
            if support <= candle['close'] <= resistance:
                count += 1
            else:
                if i < 10 and (candle['close'] > resistance * 1.05 or candle['close'] < support * 0.95):
                    break
        
        return count
    
    def _analyze_volume_profile(self, df: pd.DataFrame) -> float:
        if len(df) < 40:
            return 0.5
        
        recent_volume = df['volume'].iloc[-20:].mean()
        older_volume = df['volume'].iloc[-40:-20].mean()
        
        if older_volume == 0:
            return 0.5
        
        volume_ratio = recent_volume / older_volume
        
        if volume_ratio > 1.5:
            return 0.8
        elif volume_ratio > 1.2:
            return 0.7
        elif volume_ratio > 0.8:
            return 0.5
        else:
            return 0.3
    
    def _analyze_wicks(self, df: pd.DataFrame) -> float:
        if len(df) < 30:
            return 0.0
        
        absorption_count = 0
        total_candles = min(30, len(df))
        
        for i in range(1, total_candles):
            candle = df.iloc[-i]
            lower_wick = min(candle['open'], candle['close']) - candle['low']
            body = abs(candle['close'] - candle['open'])
            
            if lower_wick > body * 1.5:
                if i > 1:
                    next_candle = df.iloc[-i+1]
                    if next_candle['close'] > candle['close']:
                        absorption_count += 1
        
        return absorption_count / total_candles if total_candles > 0 else 0.0

class VolumeAnomalyDetector:
    def analyze(self, df_15m: pd.DataFrame, df_5m: pd.DataFrame, signal_type: str) -> VolumeAnomalyLogic:
        logic = VolumeAnomalyLogic()
        
        if len(df_15m) < 48 or len(df_5m) < 50:
            return logic
        
        try:
            # Volume spike detection
            spike_data = self._detect_volume_spike(df_15m)
            logic.volume_spike_ratio = spike_data['ratio']
            logic.spike_confidence = spike_data['confidence']
            logic.spike_duration_candles = spike_data['duration']
            
            # Volume profile
            profile = self._analyze_volume_profile(df_5m)
            logic.buy_volume_pct = profile['buy_pct']
            logic.sell_volume_pct = profile['sell_pct']
            logic.volume_imbalance = profile['imbalance']
            
            # Large transactions
            large_tx = self._analyze_large_transactions(df_15m)
            logic.large_buy_orders = large_tx['buy_orders']
            logic.large_sell_orders = large_tx['sell_orders']
            
            # Volume climax for dumps
            if signal_type == "dump_short":
                climax = self._detect_volume_climax(df_15m)
                logic.volume_climax = climax['detected']
                logic.climax_ratio = climax['ratio']
            
            # Score calculation
            logic.volume_anomaly_score = self._calculate_volume_score(logic, signal_type)
            logic.anomaly_confidence = logic.spike_confidence
            
            if signal_type == "pump_long":
                logic.anomaly_type = "accumulation" if logic.volume_spike_ratio > 2.0 else "normal"
            else:
                logic.anomaly_type = "distribution" if logic.volume_climax else "normal"
                
        except Exception as e:
            log.error(f"Volume analysis error: {e}")
        
        return logic
    
    def _detect_volume_spike(self, df: pd.DataFrame) -> Dict:
        if len(df) < 24:
            return {"ratio": 1.0, "confidence": 0.0, "duration": 0}
        
        current_volume = df['volume'].iloc[-1]
        avg_volume_6h = df['volume'].iloc[-24:].mean()
        
        ratio = current_volume / avg_volume_6h if avg_volume_6h > 0 else 1.0
        
        confidence = 0.0
        if ratio >= VOLUME_SPIKE_THRESHOLD:
            confidence = 0.9
        elif ratio >= VOLUME_SPIKE_THRESHOLD * 0.7:
            confidence = 0.5
        
        duration = 1
        for i in range(1, min(6, len(df))):
            if df['volume'].iloc[-i-1] > avg_volume_6h * 2:
                duration += 1
            else:
                break
        
        return {"ratio": float(ratio), "confidence": confidence, "duration": duration}
    
    def _analyze_volume_profile(self, df: pd.DataFrame) -> Dict:
        if len(df) < 20:
            return {"buy_pct": 50.0, "sell_pct": 50.0, "imbalance": 0.0}
        
        buy_volume = 0
        total_volume = 0
        
        for i in range(min(20, len(df))):
            idx = -i - 1
            candle = df.iloc[idx]
            
            if candle['close'] > candle['open']:
                buy_volume += candle['volume'] * 0.7
            elif candle['close'] < candle['open']:
                buy_volume += candle['volume'] * 0.3
            else:
                buy_volume += candle['volume'] * 0.5
            
            total_volume += candle['volume']
        
        buy_pct = (buy_volume / total_volume * 100) if total_volume > 0 else 50.0
        sell_pct = 100 - buy_pct
        imbalance = (buy_pct - 50) / 50
        
        return {"buy_pct": buy_pct, "sell_pct": sell_pct, "imbalance": imbalance}
    
    def _analyze_large_transactions(self, df: pd.DataFrame) -> Dict:
        if len(df) < 10:
            return {"buy_orders": 0, "sell_orders": 0, "ratio": 0.0}
        
        volumes = df['volume'].values
        q75 = np.percentile(volumes, 75)
        
        large_buy = 0
        large_sell = 0
        
        for i in range(min(10, len(df))):
            idx = -i - 1
            candle = df.iloc[idx]
            
            if candle['volume'] > q75 * 3:
                if candle['close'] > candle['open']:
                    large_buy += 1
                else:
                    large_sell += 1
        
        total_large = large_buy + large_sell
        ratio = large_buy / total_large if total_large > 0 else 0.0
        
        return {"buy_orders": large_buy, "sell_orders": large_sell, "ratio": ratio}
    
    def _detect_volume_climax(self, df: pd.DataFrame) -> Dict:
        if len(df) < 10:
            return {"detected": False, "ratio": 1.0}
        
        recent_volume = df['volume'].iloc[-1]
        avg_volume = df['volume'].iloc[-10:].mean()
        
        ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0
        detected = ratio > VOLUME_CLIMAX_RATIO
        
        return {"detected": detected, "ratio": ratio}
    
    def _calculate_volume_score(self, logic: VolumeAnomalyLogic, signal_type: str) -> float:
        score = 0.0
        
        if signal_type == "pump_long":
            if logic.volume_spike_ratio >= 3.0:
                score += 0.4
            elif logic.volume_spike_ratio >= 2.0:
                score += 0.2
            
            if logic.volume_imbalance > 0.2:
                score += 0.3
            
            if logic.large_buy_orders > logic.large_sell_orders:
                score += 0.3
        else:  # dump_short
            if logic.volume_climax:
                score += 0.5
            
            if logic.volume_imbalance < -0.2:
                score += 0.3
            
            if logic.large_sell_orders > logic.large_buy_orders:
                score += 0.2
        
        return min(score, 1.0)

class MomentumAnalyzer:
    def analyze(self, df_15m: pd.DataFrame, df_5m: pd.DataFrame, signal_type: str) -> MomentumLogic:
        logic = MomentumLogic()
        
        if len(df_15m) < 30 or len(df_5m) < 20:
            return logic
        
        try:
            # RSI analysis
            rsi_values = calculate_rsi(df_15m['close'], period=14)
            if not rsi_values.empty:
                logic.rsi_value = float(rsi_values.iloc[-1])
                logic.rsi_zone = self._determine_rsi_zone(logic.rsi_value)
            
            # Candle patterns
            logic.candle_pattern, logic.pattern_strength = self._analyze_candle_patterns(df_5m)
            
            # Consecutive candles
            logic.consecutive_green = self._count_consecutive(df_5m, 'green')
            logic.consecutive_red = self._count_consecutive(df_5m, 'red')
            
            # Trend analysis
            logic.trend_strength = self._analyze_trend_strength(df_15m)
            logic.trend_direction = self._determine_trend_direction(df_15m)
            
            # Momentum bias
            if signal_type == "pump_long":
                if logic.rsi_value < 60 and logic.trend_strength > 0.5:
                    logic.momentum_bias = "bullish"
                elif logic.rsi_value > 70:
                    logic.exhaustion_detected = True
            else:  # dump_short
                if logic.rsi_value > 40 and logic.trend_strength < 0.5:
                    logic.momentum_bias = "bearish"
                elif logic.rsi_value < 30:
                    logic.exhaustion_detected = True
            
            # Score calculation
            logic.momentum_score = self._calculate_momentum_score(logic, signal_type)
            
        except Exception as e:
            log.error(f"Momentum analysis error: {e}")
        
        return logic
    
    def _determine_rsi_zone(self, rsi_value: float) -> str:
        if rsi_value <= RSI_OVERSOLD:
            return "oversold"
        elif rsi_value >= RSI_OVERBOUGHT:
            return "overbought"
        else:
            return "neutral"
    
    def _analyze_candle_patterns(self, df: pd.DataFrame) -> Tuple[str, float]:
        if len(df) < 3:
            return "none", 0.0
        
        current = df.iloc[-1]
        prev = df.iloc[-2]
        
        # Bullish engulfing
        if (prev['close'] < prev['open'] and
            current['close'] > current['open'] and
            current['close'] > prev['open'] and
            current['open'] < prev['close']):
            return "bullish_engulfing", 0.8
        
        # Bearish engulfing
        if (prev['close'] > prev['open'] and
            current['close'] < current['open'] and
            current['close'] < prev['open'] and
            current['open'] > prev['close']):
            return "bearish_engulfing", 0.8
        
        return "none", 0.0
    
    def _count_consecutive(self, df: pd.DataFrame, candle_type: str) -> int:
        count = 0
        for i in range(min(10, len(df))):
            idx = -i - 1
            candle = df.iloc[idx]
            
            if candle_type == 'green':
                if candle['close'] > candle['open']:
                    count += 1
                else:
                    break
            else:  # red
                if candle['close'] < candle['open']:
                    count += 1
                else:
                    break
        
        return count
    
    def _analyze_trend_strength(self, df: pd.DataFrame) -> float:
        if len(df) < 20:
            return 0.0
        
        ema_fast = calculate_ema(df['close'], 9)
        ema_slow = calculate_ema(df['close'], 21)
        
        if len(ema_fast) < 1 or len(ema_slow) < 1:
            return 0.0
        
        # Check alignment
        if ema_fast.iloc[-1] > ema_slow.iloc[-1]:
            # Bullish trend
            strength = (ema_fast.iloc[-1] - ema_slow.iloc[-1]) / ema_slow.iloc[-1]
        else:
            # Bearish trend
            strength = (ema_slow.iloc[-1] - ema_fast.iloc[-1]) / ema_fast.iloc[-1]
        
        return min(abs(strength) * 10, 1.0)
    
    def _determine_trend_direction(self, df: pd.DataFrame) -> str:
        if len(df) < 20:
            return "neutral"
        
        ema_fast = calculate_ema(df['close'], 9)
        ema_slow = calculate_ema(df['close'], 21)
        
        if len(ema_fast) < 1 or len(ema_slow) < 1:
            return "neutral"
        
        if ema_fast.iloc[-1] > ema_slow.iloc[-1]:
            return "bullish"
        elif ema_fast.iloc[-1] < ema_slow.iloc[-1]:
            return "bearish"
        else:
            return "neutral"
    
    def _calculate_momentum_score(self, logic: MomentumLogic, signal_type: str) -> float:
        score = 0.0
        
        # RSI contribution
        if signal_type == "pump_long":
            if logic.rsi_zone == "oversold":
                score += 0.3
            elif logic.rsi_value < 60:
                score += 0.2
        else:  # dump_short
            if logic.rsi_zone == "overbought":
                score += 0.3
            elif logic.rsi_value > 40:
                score += 0.2
        
        # Trend contribution
        if signal_type == "pump_long" and logic.trend_direction == "bullish":
            score += logic.trend_strength * 0.3
        elif signal_type == "dump_short" and logic.trend_direction == "bearish":
            score += logic.trend_strength * 0.3
        
        # Candle pattern contribution
        score += logic.pattern_strength * 0.2
        
        return min(score, 1.0)

# ================ MEXC PUMP & DUMP SNIPER ================
class MexcPumpDumpSniper:
    def __init__(self):
        self.exchange = None
        self.accumulation_detector = AccumulationDetector()
        self.volume_detector = VolumeAnomalyDetector()
        self.momentum_analyzer = MomentumAnalyzer()
        self.active_signals = {}
        self.signal_history = deque(maxlen=100)
        
        self.stats = {
            "total_scans": 0,
            "pairs_analyzed": 0,
            "pump_signals": 0,
            "dump_signals": 0,
            "high_quality_signals": 0,
            "rejections": {
                "low_score": 0,
                "no_accumulation": 0,
                "no_volume": 0,
                "bad_risk_reward": 0
            }
        }
    
    async def initialize_exchange(self):
        """Initialize MEXC Futures connection"""
        try:
            self.exchange = ccxt.mexc({
                "enableRateLimit": True,
                "options": {"defaultType": "swap"},
                "timeout": 30000,
            })
            
            markets = await self.exchange.fetch_markets()
            log.info(f"✅ MEXC connected. {len(markets)} markets available")
            return True
            
        except Exception as e:
            log.error(f"Failed to connect to MEXC: {e}")
            return False
    
    async def close_exchange(self):
        """Close exchange connection properly"""
        if self.exchange:
            try:
                await self.exchange.close()
                log.info("✅ Exchange connection closed")
            except Exception as e:
                log.error(f"Error closing exchange: {e}")
    
    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 100):
        """Fetch OHLCV data"""
        try:
            ohlcv = await self.exchange.fetch_ohlcv(
                symbol,
                timeframe=timeframe,
                limit=limit
            )
            
            if ohlcv and len(ohlcv) > 0:
                df = pd.DataFrame(
                    ohlcv,
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                )
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                df = df.dropna()
                return df
            
            return None
            
        except Exception as e:
            log.debug(f"OHLCV fetch error for {symbol} {timeframe}: {e}")
            return None
    
    async def analyze_symbol(self, symbol: str) -> Optional[CompleteSignal]:
        """Analyze symbol for pump/dump opportunities"""
        try:
            self.stats["pairs_analyzed"] += 1
            
            # Fetch data for all timeframes
            timeframe_data = {}
            for tf_name, tf in TIMEFRAMES.items():
                if tf_name in ["4H", "1H", "15M", "5M"]:
                    df = await self.fetch_ohlcv(symbol, tf, limit=100)
                    if df is not None and len(df) >= 30:
                        timeframe_data[tf_name] = df
            
            if len(timeframe_data) < 4:
                return None
            
            # Get current price and volume
            ticker = await self.exchange.fetch_ticker(symbol)
            current_price = ticker['last']
            volume_24h = ticker['quoteVolume']
            
            # Check if low-cap candidate
            if not (MIN_PRICE <= current_price <= MAX_PRICE):
                return None
            
            if volume_24h < MIN_VOLUME_24H:
                return None
            
            # Analyze for pump potential
            pump_signal = await self._analyze_pump_potential(
                symbol, timeframe_data, current_price, volume_24h
            )
            
            # Analyze for dump potential
            dump_signal = await self._analyze_dump_potential(
                symbol, timeframe_data, current_price, volume_24h
            )
            
            # Select best signal
            best_signal = None
            if pump_signal and dump_signal:
                if pump_signal.overall_score >= dump_signal.overall_score:
                    best_signal = pump_signal
                    self.stats["pump_signals"] += 1
                else:
                    best_signal = dump_signal
                    self.stats["dump_signals"] += 1
            elif pump_signal:
                best_signal = pump_signal
                self.stats["pump_signals"] += 1
            elif dump_signal:
                best_signal = dump_signal
                self.stats["dump_signals"] += 1
            
            if best_signal:
                if best_signal.overall_score >= 8.0:
                    self.stats["high_quality_signals"] += 1
                
                signal_id = hashlib.md5(
                    f"{symbol}_{best_signal.signal_type}_{time.time()}".encode()
                ).hexdigest()
                best_signal.signal_id = signal_id
                self.active_signals[signal_id] = best_signal
                self.signal_history.append(best_signal)
                
                return best_signal
            
            return None
            
        except Exception as e:
            log.error(f"Analysis error for {symbol}: {e}")
            return None
    
    async def _analyze_pump_potential(self, symbol: str, timeframe_data: Dict, 
                                    current_price: float, volume_24h: float) -> Optional[CompleteSignal]:
        """Analyze for pump (long) opportunity"""
        try:
            df_4h = timeframe_data.get("4H")
            df_1h = timeframe_data.get("1H")
            df_15m = timeframe_data.get("15M")
            df_5m = timeframe_data.get("5M")
            
            if not all([df_4h is not None, df_1h is not None, df_15m is not None, df_5m is not None]):
                return None
            
            # 1. Accumulation analysis
            accumulation_logic = self.accumulation_detector.analyze(df_4h, df_1h)
            if not accumulation_logic.ready_for_breakout:
                self.stats["rejections"]["no_accumulation"] += 1
                return None
            
            # 2. Volume analysis
            volume_logic = self.volume_detector.analyze(df_15m, df_5m, "pump_long")
            if volume_logic.volume_anomaly_score < 0.6:
                self.stats["rejections"]["no_volume"] += 1
                return None
            
            # 3. Momentum analysis
            momentum_logic = self.momentum_analyzer.analyze(df_15m, df_5m, "pump_long")
            
            # 4. Breakout detection
            breakout_logic = BreakoutLogic()
            breakout_logic.breakout_type = "pump_breakout"
            
            # Find resistance level
            recent_high = df_4h['high'].iloc[-30:].max()
            breakout_logic.breakout_level = recent_high * 1.005  # 0.5% above resistance
            
            # Check if we're near breakout
            if current_price < recent_high * 0.995:
                breakout_logic.breakout_valid = False
                return None
            
            breakout_logic.breakout_valid = True
            breakout_logic.breakout_strength = accumulation_logic.accumulation_confidence
            breakout_logic.volume_confirmation = volume_logic.volume_spike_ratio > 2.0
            breakout_logic.expected_move_pct = 10.0  # Default expected move
            
            # 5. Risk calculation
            stop_loss = recent_high * 0.97  # 3% stop loss
            take_profit = breakout_logic.breakout_level * 1.15  # 15% target
            
            risk_pct = (breakout_logic.breakout_level - stop_loss) / breakout_logic.breakout_level * 100
            reward_pct = (take_profit - breakout_logic.breakout_level) / breakout_logic.breakout_level * 100
            
            if reward_pct / risk_pct < MIN_RISK_REWARD:
                self.stats["rejections"]["bad_risk_reward"] += 1
                return None
            
            risk_logic = RiskLogic(
                position_size_pct=1.0,
                leverage=LEVERAGE,
                max_capital_risk=1.0,
                stop_loss_type="percentage",
                stop_loss_level=stop_loss,
                stop_loss_distance_pct=risk_pct,
                stop_loss_confidence=0.8,
                take_profit_levels=[take_profit],
                scale_out_percentages=[100.0],
                risk_reward_ratio=reward_pct / risk_pct,
                probability_score=accumulation_logic.accumulation_confidence,
                expectancy=(reward_pct * accumulation_logic.accumulation_confidence - 
                          risk_pct * (1 - accumulation_logic.accumulation_confidence)),
                market_regime="accumulation",
                volatility_adjusted=True
            )
            
            # 6. Calculate overall score
            overall_score = (
                accumulation_logic.accumulation_confidence * 0.35 +
                volume_logic.volume_anomaly_score * 0.35 +
                momentum_logic.momentum_score * 0.15 +
                breakout_logic.breakout_strength * 0.15
            ) * 10  # Scale to 0-10
            
            if overall_score < MIN_CONFLUENCE_SCORE:
                self.stats["rejections"]["low_score"] += 1
                return None
            
            # 7. Create complete signal
            signal = CompleteSignal(
                signal_id="",
                symbol=symbol,
                signal_type="pump_long",
                timestamp=time.time(),
                current_price=current_price,
                market_cap=current_price * 1_000_000,  # Simplified estimation
                volume_24h=volume_24h,
                accumulation_logic=accumulation_logic,
                volume_logic=volume_logic,
                liquidity_logic=LiquidityEngineeringLogic(),  # Simplified for now
                momentum_logic=momentum_logic,
                breakout_logic=breakout_logic,
                risk_logic=risk_logic,
                entry_price=breakout_logic.breakout_level,
                entry_type="limit",
                entry_conditions=[
                    f"Break above {recent_high:.8f}",
                    f"Volume > {VOLUME_SPIKE_THRESHOLD}x average",
                    "RSI < 60"
                ],
                stop_loss=stop_loss,
                take_profit=take_profit,
                breakeven_level=breakout_logic.breakout_level * 1.01,
                overall_score=overall_score,
                confidence_level=accumulation_logic.accumulation_confidence,
                urgency_level="high" if volume_logic.volume_spike_ratio > 4.0 else "medium",
                timeframes_analyzed=["4H", "1H", "15M", "5M"],
                primary_timeframe="4H"
            )
            
            return signal
            
        except Exception as e:
            log.error(f"Pump analysis error for {symbol}: {e}")
            return None
    
    async def _analyze_dump_potential(self, symbol: str, timeframe_data: Dict,
                                    current_price: float, volume_24h: float) -> Optional[CompleteSignal]:
        """Analyze for dump (short) opportunity"""
        try:
            df_4h = timeframe_data.get("4H")
            df_15m = timeframe_data.get("15M")
            df_5m = timeframe_data.get("5M")
            
            if not all([df_4h is not None, df_15m is not None, df_5m is not None]):
                return None
            
            # Check for parabolic move (potential top)
            recent_high = df_4h['high'].iloc[-10:].max()
            recent_low = df_4h['low'].iloc[-10:].min()
            move_pct = (recent_high - recent_low) / recent_low * 100
            
            if move_pct < PARABOLIC_THRESHOLD:
                return None  # Not parabolic enough
            
            # Check volume climax
            volume_logic = self.volume_detector.analyze(df_15m, df_5m, "dump_short")
            if not volume_logic.volume_climax:
                return None
            
            # Check RSI overbought
            momentum_logic = self.momentum_analyzer.analyze(df_15m, df_5m, "dump_short")
            if momentum_logic.rsi_value < RSI_OVERBOUGHT:
                return None
            
            # Look for breakdown level
            support_level = df_4h['low'].iloc[-20:].min()
            breakdown_level = support_level * 0.995
            
            # Check if we're near breakdown
            if current_price > support_level * 1.01:
                return None  # Too far from breakdown
            
            # Calculate risk for short
            stop_loss = recent_high * 1.03  # 3% above recent high
            take_profit = breakdown_level * 0.92  # 8% down from breakdown
            
            risk_pct = (stop_loss - current_price) / current_price * 100
            reward_pct = (current_price - take_profit) / current_price * 100
            
            if reward_pct / risk_pct < MIN_RISK_REWARD:
                return None
            
            # Calculate score
            score_components = []
            
            # Volume climax contributes highly
            if volume_logic.volume_climax:
                score_components.append(0.3)
            
            # RSI overbought contributes
            if momentum_logic.rsi_value > 70:
                score_components.append(0.2)
            
            # Parabolic move detection
            if move_pct > 20:
                score_components.append(0.2)
            
            overall_score = sum(score_components) * 10  # Scale to 0-10
            
            if overall_score < MIN_CONFLUENCE_SCORE:
                return None
            
            # Create dump signal
            signal = CompleteSignal(
                signal_id="",
                symbol=symbol,
                signal_type="dump_short",
                timestamp=time.time(),
                current_price=current_price,
                market_cap=current_price * 1_000_000,
                volume_24h=volume_24h,
                accumulation_logic=AccumulationLogic(),  # Not applicable for dumps
                volume_logic=volume_logic,
                liquidity_logic=LiquidityEngineeringLogic(),
                momentum_logic=momentum_logic,
                breakout_logic=BreakoutLogic(
                    breakout_type="dump_breakdown",
                    breakout_level=breakdown_level,
                    breakout_strength=0.7,
                    breakout_valid=True,
                    expected_move_pct=8.0
                ),
                risk_logic=RiskLogic(
                    position_size_pct=1.0,
                    leverage=LEVERAGE,
                    stop_loss_level=stop_loss,
                    stop_loss_distance_pct=risk_pct,
                    take_profit_levels=[take_profit],
                    risk_reward_ratio=reward_pct / risk_pct,
                    probability_score=0.6
                ),
                entry_price=breakdown_level,
                entry_type="limit",
                entry_conditions=[
                    f"Break below {support_level:.8f}",
                    f"Volume climax detected ({volume_logic.climax_ratio:.1f}x)",
                    f"RSI > {RSI_OVERBOUGHT}"
                ],
                stop_loss=stop_loss,
                take_profit=take_profit,
                breakeven_level=breakdown_level * 0.99,
                overall_score=overall_score,
                confidence_level=0.6,
                urgency_level="high" if volume_logic.volume_climax else "medium",
                timeframes_analyzed=["4H", "15M", "5M"],
                primary_timeframe="4H"
            )
            
            return signal
            
        except Exception as e:
            log.error(f"Dump analysis error for {symbol}: {e}")
            return None

# ================ SIGNAL FORMATTER ================
class SignalFormatter:
    @staticmethod
    def format_for_telegram(signal: CompleteSignal) -> str:
        """Format signal for Telegram"""
        
        if signal.signal_type == "pump_long":
            main_emoji = "🚀"
            side_emoji = "🟢"
        else:
            main_emoji = "💥"
            side_emoji = "🔴"
        
        if signal.overall_score >= 9.0:
            quality_emoji = "🔥🔥🔥"
        elif signal.overall_score >= 8.0:
            quality_emoji = "🔥🔥"
        elif signal.overall_score >= 7.5:
            quality_emoji = "🔥"
        else:
            quality_emoji = "⚠️"
        
        # Format logic sections
        logic_sections = []
        
        # Accumulation Logic
        acc = signal.accumulation_logic
        if signal.signal_type == "pump_long":
            logic_sections.append(f"""
<b>🏗️ ACCUMULATION LOGIC ({acc.accumulation_confidence:.0%})</b>
• Phase: {acc.accumulation_phase.upper()}
• Duration: {acc.days_in_accumulation:.1f} days
• Compression: {acc.price_compression_pct:.1f}% range
• Wick Absorption: {acc.lower_wick_absorption:.0%}
• Ready for Breakout: {'✅ YES' if acc.ready_for_breakout else '❌ NO'}
""")
        
        # Volume Logic
        vol = signal.volume_logic
        logic_sections.append(f"""
<b>💧 VOLUME LOGIC ({vol.volume_anomaly_score:.1f}/1.0)</b>
• Spike: {vol.volume_spike_ratio:.1f}x
• Buy/Sell: {vol.buy_volume_pct:.0%}/{vol.sell_volume_pct:.0%}
• Large Orders: {vol.large_buy_orders} buy, {vol.large_sell_orders} sell
• Climax: {'✅ YES' if vol.volume_climax else '❌ NO'}
""")
        
        # Momentum Logic
        mom = signal.momentum_logic
        logic_sections.append(f"""
<b>📈 MOMENTUM LOGIC ({mom.momentum_score:.1f}/1.0)</b>
• RSI: {mom.rsi_value:.1f} ({mom.rsi_zone.upper()})
• Trend: {mom.trend_direction.upper()} ({mom.trend_strength:.0%})
• Bias: {mom.momentum_bias.upper()}
• Exhaustion: {'⚠️ YES' if mom.exhaustion_detected else '✅ NO'}
""")
        
        # Breakout Logic
        brk = signal.breakout_logic
        logic_sections.append(f"""
<b>🎯 BREAKOUT LOGIC</b>
• Type: {brk.breakout_type.upper().replace('_', ' ')}
• Level: {brk.breakout_level:.8f}
• Expected Move: {brk.expected_move_pct:.1f}%
• Valid: {'✅ YES' if brk.breakout_valid else '❌ NO'}
""")
        
        # Risk Logic
        risk = signal.risk_logic
        logic_sections.append(f"""
<b>🛡️ RISK LOGIC</b>
• Position: {risk.position_size_pct:.1f}% ({risk.leverage}x)
• Stop Loss: {risk.stop_loss_level:.8f} ({risk.stop_loss_distance_pct:.1f}%)
• Take Profit: {risk.take_profit_levels[0]:.8f}
• Risk/Reward: {risk.risk_reward_ratio:.1f}:1
• Probability: {risk.probability_score:.0%}
""")
        
        # Build final message
        message = f"""{main_emoji} <b>{side_emoji} {signal.signal_type.upper().replace('_', ' ')} SIGNAL</b> {quality_emoji}

<b>{signal.symbol}</b> | Score: <b>{signal.overall_score:.1f}/10</b> | Confidence: <b>{signal.confidence_level:.0%}</b>
Urgency: <b>{signal.urgency_level.upper()}</b> | Timeframes: {', '.join(signal.timeframes_analyzed)}

<b>💰 MARKET DATA:</b>
• Price: {signal.current_price:.8f}
• Est. Market Cap: ${signal.market_cap:,.0f}
• 24h Volume: ${signal.volume_24h:,.0f}

<b>🎯 TRADE SETUP:</b>
• Entry: {signal.entry_price:.8f} ({signal.entry_type.upper()})
• Stop Loss: {signal.stop_loss:.8f}
• Take Profit: {signal.take_profit:.8f}
• Breakeven: {signal.breakeven_level:.8f}

<b>📋 ENTRY CONDITIONS:</b>
{chr(10).join([f'• {cond}' for cond in signal.entry_conditions])}
"""
        
        # Add all logic sections
        for section in logic_sections:
            message += section
        
        # Add hashtags
        clean_symbol = signal.symbol.replace('/', '').replace('-', '').replace('_', '')
        message += f"""
<b>🏷️ TAGS:</b>
#{clean_symbol} #{signal.signal_type.split('_')[0].upper()} #{signal.signal_type.split('_')[1].upper()} 
#Score{int(signal.overall_score)} #{signal.urgency_level.upper()}URGENCY #MEXCFutures
#LogicBreakdown #PumpDumpSniper
"""
        
        return message

# ================ DATABASE MANAGER ================
class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = None
    
    async def initialize(self):
        """Initialize database"""
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self.db = await aiosqlite.connect(self.db_path)
            
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                timestamp REAL NOT NULL,
                current_price REAL NOT NULL,
                overall_score REAL NOT NULL,
                confidence_level REAL NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                risk_reward_ratio REAL NOT NULL,
                status TEXT DEFAULT 'generated',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS signal_details (
                signal_id TEXT PRIMARY KEY,
                accumulation_logic TEXT,
                volume_logic TEXT,
                momentum_logic TEXT,
                breakout_logic TEXT,
                risk_logic TEXT,
                FOREIGN KEY (signal_id) REFERENCES signals (id)
            )
            """)
            
            await self.db.commit()
            log.info("✅ Database initialized")
            
        except Exception as e:
            log.error(f"Database initialization error: {e}")
            raise
    
    async def save_signal(self, signal: CompleteSignal):
        """Save signal to database"""
        try:
            # Save main signal
            await self.db.execute("""
            INSERT INTO signals (
                id, symbol, signal_type, timestamp, current_price,
                overall_score, confidence_level, entry_price,
                stop_loss, take_profit, risk_reward_ratio
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.signal_type,
                signal.timestamp,
                signal.current_price,
                signal.overall_score,
                signal.confidence_level,
                signal.entry_price,
                signal.stop_loss,
                signal.take_profit,
                signal.risk_logic.risk_reward_ratio
            ))
            
            # Save detailed logic
            await self.db.execute("""
            INSERT INTO signal_details (
                signal_id, accumulation_logic, volume_logic,
                momentum_logic, breakout_logic, risk_logic
            ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                json.dumps(asdict(signal.accumulation_logic)),
                json.dumps(asdict(signal.volume_logic)),
                json.dumps(asdict(signal.momentum_logic)),
                json.dumps(asdict(signal.breakout_logic)),
                json.dumps(asdict(signal.risk_logic))
            ))
            
            await self.db.commit()
            log.info(f"✅ Signal saved to database: {signal.symbol}")
            
        except Exception as e:
            log.error(f"Error saving signal: {e}")
    
    async def close(self):
        """Close database connection"""
        if self.db:
            await self.db.close()
            log.info("✅ Database connection closed")

# ================ TELEGRAM MANAGER ================
class TelegramManager:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.client = httpx.AsyncClient(timeout=10.0)
    
    async def send_message(self, message: str):
        """Send message to Telegram"""
        if not self.token or not self.chat_id:
            log.warning("Telegram credentials not set")
            return False
        
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            
            response = await self.client.post(url, json=payload)
            
            if response.status_code == 200:
                log.info("✅ Message sent to Telegram")
                return True
            else:
                log.error(f"Telegram error: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            log.error(f"Telegram send error: {e}")
            return False
    
    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()

# ================ MAIN BOT ================
class MexcPumpDumpBot:
    def __init__(self):
        self.sniper = MexcPumpDumpSniper()
        self.formatter = SignalFormatter()
        self.db_manager = None
        self.telegram_manager = None
        self.scanning_active = False
    
    async def initialize(self):
        """Initialize bot components"""
        log.info("=" * 70)
        log.info("🚀 MEXC FUTURES PUMP & DUMP SNIPER v1.0")
        log.info("=" * 70)
        
        # Initialize exchange
        if not await self.sniper.initialize_exchange():
            return False
        
        # Initialize database
        self.db_manager = DatabaseManager(DB_PATH)
        await self.db_manager.initialize()
        
        # Initialize Telegram
        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            self.telegram_manager = TelegramManager(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
            await self._send_startup_message()
        
        log.info("✅ Bot initialized successfully")
        return True
    
    async def cleanup(self):
        """Cleanup all resources"""
        log.info("🛑 Cleaning up resources...")
        
        # Close exchange
        await self.sniper.close_exchange()
        
        # Close database
        if self.db_manager:
            await self.db_manager.close()
        
        # Close Telegram
        if self.telegram_manager:
            await self.telegram_manager.close()
        
        log.info("✅ Cleanup completed")
    
    async def _send_startup_message(self):
        """Send startup message"""
        if not self.telegram_manager:
            return
        
        message = """🚀 <b>MEXC PUMP & DUMP SNIPER - ONLINE</b>

<b>🎯 TARGET:</b> Low-cap futures ($500K-$20M)
<b>⚡ STRATEGY:</b> Detect accumulations (pumps) and distributions (dumps)
<b>📊 LOGIC:</b> Complete breakdown in every signal
<b>🛡️ RISK:</b> 3% max loss, 3:1 R:R minimum

Bot is now scanning for opportunities.

#PumpDumpSniper #MEXCFutures #Online"""
        
        await self.telegram_manager.send_message(message)
    
    async def _get_mexc_futures_pairs(self) -> List[str]:
        """Get low-cap futures pairs from MEXC"""
        try:
            markets = await self.sniper.exchange.fetch_markets()
            
            pairs = []
            for market in markets:
                symbol = market['symbol']
                
                # Filter for USDT pairs
                if symbol.endswith('/USDT:USDT') or symbol.endswith('/USDT'):
                    try:
                        ticker = await self.sniper.exchange.fetch_ticker(symbol)
                        price = ticker['last']
                        volume = ticker['quoteVolume']
                        
                        # Basic filtering
                        if (MIN_PRICE <= price <= MAX_PRICE and
                            volume >= MIN_VOLUME_24H):
                            pairs.append(symbol)
                    except:
                        continue
            
            log.info(f"Found {len(pairs)} potential low-cap pairs")
            return pairs[:20]  # Limit to 20 pairs
            
        except Exception as e:
            log.error(f"Error getting pairs: {e}")
            return []
    
    async def run_scanning(self, scan_interval: int = 30):
        """Run continuous scanning"""
        self.scanning_active = True
        scan_count = 0
        
        try:
            while self.scanning_active:
                scan_count += 1
                log.info(f"🔄 Scan #{scan_count}")
                
                # Get pairs
                pairs = await self._get_mexc_futures_pairs()
                
                # Analyze each pair
                signals_found = 0
                for symbol in pairs:
                    signal = await self.sniper.analyze_symbol(symbol)
                    
                    if signal:
                        signals_found += 1
                        
                        # Format and send
                        await self._process_signal(signal)
                        
                        # Save to database
                        await self.db_manager.save_signal(signal)
                
                # Log statistics
                stats = self.sniper.stats
                log.info(f"📊 Scan complete - Signals: {signals_found}")
                log.info(f"   Total pump: {stats['pump_signals']}")
                log.info(f"   Total dump: {stats['dump_signals']}")
                log.info(f"   High quality: {stats['high_quality_signals']}")
                
                # Wait for next scan
                await asyncio.sleep(scan_interval)
                
        except KeyboardInterrupt:
            log.info("Scanning stopped by user")
        except Exception as e:
            log.error(f"Scanning error: {e}")
        finally:
            self.scanning_active = False
    
    async def _process_signal(self, signal: CompleteSignal):
        """Process detected signal"""
        try:
            # Format signal
            telegram_message = self.formatter.format_for_telegram(signal)
            
            # Send to Telegram
            if self.telegram_manager:
                await self.telegram_manager.send_message(telegram_message)
            
            log.info(f"📤 Signal processed: {signal.symbol} ({signal.signal_type})")
            
        except Exception as e:
            log.error(f"Signal processing error: {e}")

# ================ MAIN EXECUTION ================
async def main():
    """Main execution function"""
    bot = MexcPumpDumpBot()
    
    try:
        # Initialize bot
        if await bot.initialize():
            log.info("✅ Starting scanning...")
            
            # Run scanning
            await bot.run_scanning(scan_interval=30)
            
        else:
            log.error("❌ Failed to initialize bot")
            
    except KeyboardInterrupt:
        log.info("🛑 Bot stopped by user")
    except Exception as e:
        log.error(f"❌ Bot crashed: {e}")
        log.error(traceback.format_exc())
    finally:
        # Always cleanup
        await bot.cleanup()

if __name__ == "__main__":
    # Run the bot
    asyncio.run(main())