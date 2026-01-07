#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MEXC FUTURES PUMP & DUMP SNIPER v1.0
SPECIALIZATION: Low-cap pump AND dump detection on MEXC Futures
FOCUS: Identify accumulation before pumps, distribution before dumps
STRATEGY: Long accumulation breakouts, Short distribution breakdowns
"""

import os
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

# ================ MEXC FUTURES CONFIG ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/mexc_pump_dump.db"

# Exchange config
EXCHANGE_NAME = "mexc"
CONTRACT_TYPE = "swap"  # Perpetual futures
LEVERAGE = 3  # Conservative leverage for low-caps
MARGIN_MODE = "isolated"

# Low-cap targeting
MIN_MARKET_CAP = 500_000  # $500K - more aggressive
MAX_MARKET_CAP = 20_000_000  # $20M maximum
MIN_PRICE = 0.00001
MAX_PRICE = 0.50
MIN_VOLUME_24H = 100_000  # Minimum liquidity for futures
MAX_HOLDERS = 5000  # Avoid widely distributed tokens

# PUMP DETECTION PARAMETERS
ACCUMULATION_MIN_DAYS = 2
ACCUMULATION_MAX_DAYS = 14
VOLUME_SPIKE_THRESHOLD = 3.5  # 3.5x volume spike
VOLUME_DECLINE_THRESHOLD = 0.4  # 60% volume decline for dump detection
PRICE_COMPRESSION_THRESHOLD = 0.15  # Max 15% range during accumulation

# DUMP DETECTION PARAMETERS
DISTRIBUTION_MIN_CANDLES = 10
PARABOLIC_THRESHOLD = 25.0  # 25% move in 4h for parabolic detection
VOLUME_CLIMAX_RATIO = 2.8  # Volume climax signal
RSI_OVERBOUGHT = 75
RSI_OVERSOLD = 25

# Trade parameters
TARGET_PROFIT_PUMP = (8.0, 20.0)  # 8-20% for pumps
TARGET_PROFIT_DUMP = (6.0, 15.0)  # 6-15% for dumps
MAX_STOP_LOSS = 3.0  # 3% max
MIN_RISK_REWARD = 3.0  # 3:1 minimum
MIN_CONFLUENCE_SCORE = 2.5  # Strict threshold

# Timeframes
TIMEFRAMES = {
    "4H": "4h",  # Primary accumulation/distribution
    "1H": "1h",  # Structure confirmation
    "15M": "15m", # Volume and momentum
    "5M": "5m",  # Entry precision
    "1M": "1m"   # Exit timing
}

# Logic scoring weights
LOGIC_WEIGHTS = {
    "accumulation_quality": 0.25,
    "volume_anomaly": 0.25,
    "liquidity_setup": 0.20,
    "momentum_alignment": 0.15,
    "breakout_cleanliness": 0.15,
}

# ================ PURE PYTHON TA ================
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

def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """Calculate Average True Range"""
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    return atr

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
    """Detailed accumulation phase logic breakdown"""
    
    # Detection metrics
    price_compression_pct: float = 0.0
    compression_score: float = 0.0
    days_in_accumulation: float = 0.0
    accumulation_candles: int = 0
    
    # Volume analysis
    volume_trend: str = "neutral"  # increasing/decreasing/neutral
    volume_accumulation_score: float = 0.0
    smart_money_volume_pct: float = 0.0
    
    # Wick analysis
    lower_wick_absorption: float = 0.0  # % of wicks absorbed
    upper_wick_supply: float = 0.0  # % of wicks rejected
    wick_score: float = 0.0
    
    # Order flow
    order_blocks_present: bool = False
    order_block_count: int = 0
    order_block_quality: float = 0.0
    
    # Liquidity
    liquidity_sweeps_detected: bool = False
    stop_hunts_detected: bool = False
    liquidity_score: float = 0.0
    
    # Structure
    higher_lows_count: int = 0
    lower_highs_count: int = 0
    structure_score: float = 0.0
    
    # Summary
    accumulation_confidence: float = 0.0
    accumulation_phase: str = "none"  # early/mid/late
    ready_for_breakout: bool = False
    
    def to_dict(self):
        return asdict(self)

@dataclass
class VolumeAnomalyLogic:
    """Detailed volume anomaly logic breakdown"""
    
    # Volume spikes
    volume_spike_ratio: float = 1.0
    spike_confidence: float = 0.0
    spike_duration_candles: int = 0
    
    # Volume profile
    buy_volume_pct: float = 50.0
    sell_volume_pct: float = 50.0
    volume_imbalance: float = 0.0  # positive = buy, negative = sell
    
    # Large transactions
    large_buy_orders: int = 0
    large_sell_orders: int = 0
    large_order_ratio: float = 0.0
    
    # Volume divergence
    price_volume_divergence: str = "none"  # bullish/bearish/none
    divergence_strength: float = 0.0
    
    # Volume climax detection (for dumps)
    volume_climax: bool = False
    climax_ratio: float = 1.0
    climax_candle_size_pct: float = 0.0
    
    # Summary
    volume_anomaly_score: float = 0.0
    anomaly_type: str = "none"  # accumulation/distribution/climax
    anomaly_confidence: float = 0.0
    
    def to_dict(self):
        return asdict(self)

@dataclass
class LiquidityEngineeringLogic:
    """Detailed liquidity engineering logic breakdown"""
    
    # Stop hunts
    stop_hunt_detected: bool = False
    stop_hunt_direction: str = "none"  # long/short
    stop_hunt_strength: float = 0.0
    stop_hunt_level: float = 0.0
    
    # Fake levels
    fake_resistance_levels: List[float] = None
    fake_support_levels: List[float] = None
    fake_level_count: int = 0
    
    # Liquidity pools
    liquidity_clusters: Dict[str, float] = None  # "support": price, "resistance": price
    cluster_strength: float = 0.0
    
    # Order book analysis (estimated)
    bid_wall_size: float = 0.0
    ask_wall_size: float = 0.0
    orderbook_imbalance: float = 0.0
    
    # Manipulation patterns
    squeeze_detected: bool = False
    squeeze_strength: float = 0.0
    volatility_expansion_expected: bool = False
    
    # Summary
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
    
    def to_dict(self):
        return asdict(self)

@dataclass
class MomentumLogic:
    """Detailed momentum logic breakdown"""
    
    # RSI analysis
    rsi_value: float = 50.0
    rsi_zone: str = "neutral"  # oversold/neutral/overbought
    rsi_divergence: str = "none"  # bullish/bearish/hidden_bullish/hidden_bearish
    rsi_divergence_strength: float = 0.0
    
    # MACD analysis
    macd_histogram: float = 0.0
    macd_trend: str = "neutral"  # bullish/bearish
    macd_cross_signal: str = "none"  # bullish_cross/bearish_cross
    
    # Price action
    candle_pattern: str = "none"
    pattern_strength: float = 0.0
    consecutive_green: int = 0
    consecutive_red: int = 0
    
    # Trend strength
    trend_strength: float = 0.0  # 0-1
    trend_direction: str = "neutral"
    ema_alignment: str = "neutral"  # bullish/bearish
    
    # Volume confirmation
    volume_confirmation: bool = False
    volume_momentum: float = 0.0
    
    # Summary
    momentum_score: float = 0.0
    momentum_bias: str = "neutral"  # bullish/bearish
    exhaustion_detected: bool = False
    
    def to_dict(self):
        return asdict(self)

@dataclass
class BreakoutLogic:
    """Detailed breakout/breakdown logic breakdown"""
    
    # Breakout characteristics
    breakout_type: str = "none"  # pump_breakout/dump_breakdown
    breakout_level: float = 0.0
    breakout_strength: float = 0.0
    
    # Volume confirmation
    breakout_volume_ratio: float = 1.0
    volume_confirmation: bool = False
    
    # Retest analysis
    retest_occurred: bool = False
    retest_successful: bool = False
    retest_depth_pct: float = 0.0
    
    # Follow-through
    follow_through_candles: int = 0
    follow_through_strength: float = 0.0
    
    # Liquidity after breakout
    liquidity_grabbed: bool = False
    next_liquidity_level: float = 0.0
    
    # Summary
    breakout_quality: float = 0.0
    breakout_valid: bool = False
    expected_move_pct: float = 0.0
    
    def to_dict(self):
        return asdict(self)

@dataclass
class RiskLogic:
    """Detailed risk management logic"""
    
    # Position sizing
    position_size_pct: float = 1.0
    leverage: int = 1
    max_capital_risk: float = 1.0
    
    # Stop loss logic
    stop_loss_type: str = "technical"  # technical/volatility/percentage
    stop_loss_level: float = 0.0
    stop_loss_distance_pct: float = 0.0
    stop_loss_confidence: float = 0.0
    
    # Take profit logic
    take_profit_levels: List[float] = None
    scale_out_percentages: List[float] = None
    
    # Risk metrics
    risk_reward_ratio: float = 0.0
    probability_score: float = 0.0
    expectancy: float = 0.0
    
    # Market conditions
    market_regime: str = "neutral"
    volatility_adjusted: bool = False
    correlation_risk: float = 0.0
    
    def __post_init__(self):
        if self.take_profit_levels is None:
            self.take_profit_levels = []
        if self.scale_out_percentages is None:
            self.scale_out_percentages = []
    
    def to_dict(self):
        return asdict(self)

# ================ COMPLETE SIGNAL WITH LOGIC BREAKDOWN ================
@dataclass
class CompleteSignal:
    """Complete pump/dump signal with full logic breakdown"""
    
    # Basic info
    signal_id: str
    symbol: str
    signal_type: str  # pump_long/dump_short
    timestamp: float
    
    # Current market state
    current_price: float
    market_cap: float
    volume_24h: float
    
    # Logic breakdown sections
    accumulation_logic: AccumulationLogic
    volume_logic: VolumeAnomalyLogic
    liquidity_logic: LiquidityEngineeringLogic
    momentum_logic: MomentumLogic
    breakout_logic: BreakoutLogic
    risk_logic: RiskLogic
    
    # Entry parameters
    entry_price: float
    entry_type: str  # limit/market/stop
    entry_conditions: List[str]
    
    # Trade parameters
    stop_loss: float
    take_profit: float
    breakeven_level: float
    
    # Scoring
    overall_score: float
    confidence_level: float
    urgency_level: str  # high/medium/low
    
    # Timeframes used
    timeframes_analyzed: List[str]
    primary_timeframe: str
    
    # Status
    status: str = "generated"  # generated/triggered/closed
    
    def to_dict(self):
        """Convert to dictionary for storage/display"""
        result = asdict(self)
        
        # Convert nested dataclasses
        result['accumulation_logic'] = self.accumulation_logic.to_dict()
        result['volume_logic'] = self.volume_logic.to_dict()
        result['liquidity_logic'] = self.liquidity_logic.to_dict()
        result['momentum_logic'] = self.momentum_logic.to_dict()
        result['breakout_logic'] = self.breakout_logic.to_dict()
        result['risk_logic'] = self.risk_logic.to_dict()
        
        return result

# ================ DETECTION ENGINES ================
class AccumulationDetector:
    """Detect accumulation phases with detailed logic"""
    
    def analyze(self, df_4h: pd.DataFrame, df_1h: pd.DataFrame) -> AccumulationLogic:
        """Comprehensive accumulation analysis"""
        logic = AccumulationLogic()
        
        if len(df_4h) < 50 or len(df_1h) < 100:
            return logic
        
        try:
            # 1. Price compression analysis
            recent_high = df_4h['high'].iloc[-30:].max()
            recent_low = df_4h['low'].iloc[-30:].min()
            price_range = recent_high - recent_low
            current_price = df_4h['close'].iloc[-1]
            
            logic.price_compression_pct = (price_range / recent_low) * 100
            logic.compression_score = self._calculate_compression_score(df_4h)
            
            # 2. Time in accumulation
            accumulation_candles = self._count_accumulation_candles(df_4h, recent_low, recent_high)
            logic.accumulation_candles = accumulation_candles
            logic.days_in_accumulation = (accumulation_candles * 4) / 24  # 4h candles to days
            
            # 3. Volume analysis
            volume_analysis = self._analyze_volume_profile(df_4h)
            logic.volume_trend = volume_analysis['trend']
            logic.volume_accumulation_score = volume_analysis['score']
            logic.smart_money_volume_pct = self._estimate_smart_money_volume(df_1h)
            
            # 4. Wick analysis
            wick_analysis = self._analyze_wicks(df_1h)
            logic.lower_wick_absorption = wick_analysis['lower_absorption']
            logic.upper_wick_supply = wick_analysis['upper_supply']
            logic.wick_score = wick_analysis['score']
            
            # 5. Order blocks
            order_blocks = self._find_order_blocks(df_1h)
            logic.order_blocks_present = len(order_blocks) > 0
            logic.order_block_count = len(order_blocks)
            logic.order_block_quality = self._calculate_order_block_quality(order_blocks)
            
            # 6. Liquidity engineering
            liquidity_analysis = self._analyze_liquidity_patterns(df_1h)
            logic.liquidity_sweeps_detected = liquidity_analysis['sweeps_detected']
            logic.stop_hunts_detected = liquidity_analysis['stop_hunts_detected']
            logic.liquidity_score = liquidity_analysis['score']
            
            # 7. Structure analysis
            structure = self._analyze_structure(df_4h)
            logic.higher_lows_count = structure['higher_lows']
            logic.lower_highs_count = structure['lower_highs']
            logic.structure_score = structure['score']
            
            # 8. Summary scores
            logic.accumulation_confidence = self._calculate_accumulation_confidence(logic)
            logic.accumulation_phase = self._determine_accumulation_phase(logic)
            logic.ready_for_breakout = logic.accumulation_confidence >= 0.7
            
        except Exception as e:
            logging.error(f"Accumulation analysis error: {e}")
        
        return logic
    
    def _calculate_compression_score(self, df: pd.DataFrame) -> float:
        """Calculate price compression score"""
        if len(df) < 30:
            return 0.0
        
        # Bollinger Band width compression
        _, _, bb_width = calculate_bollinger_bands(df['close'], period=20, std_dev=2)
        if bb_width.iloc[-1] < 0.1:
            return 0.9
        elif bb_width.iloc[-1] < 0.15:
            return 0.7
        elif bb_width.iloc[-1] < 0.2:
            return 0.5
        
        return 0.3
    
    def _count_accumulation_candles(self, df: pd.DataFrame, support: float, resistance: float) -> int:
        """Count candles within accumulation range"""
        count = 0
        lookback = min(100, len(df))
        
        for i in range(lookback):
            idx = -i - 1
            candle = df.iloc[idx]
            
            # Check if candle is within accumulation range
            if support <= candle['close'] <= resistance:
                count += 1
            else:
                # If we break significantly outside range, stop counting
                if i < 10 and (candle['close'] > resistance * 1.05 or candle['close'] < support * 0.95):
                    break
        
        return count
    
    def _analyze_volume_profile(self, df: pd.DataFrame) -> Dict:
        """Analyze volume during potential accumulation"""
        if len(df) < 40:
            return {"trend": "neutral", "score": 0.5}
        
        # Compare volume in recent vs older periods
        recent_volume = df['volume'].iloc[-20:].mean()
        older_volume = df['volume'].iloc[-40:-20].mean()
        
        if older_volume == 0:
            return {"trend": "neutral", "score": 0.5}
        
        volume_ratio = recent_volume / older_volume
        
        if volume_ratio > 1.5:
            trend = "increasing"
            score = 0.8
        elif volume_ratio > 1.2:
            trend = "increasing"
            score = 0.7
        elif volume_ratio > 0.8:
            trend = "neutral"
            score = 0.5
        else:
            trend = "decreasing"
            score = 0.3
        
        return {"trend": trend, "score": score}
    
    def _estimate_smart_money_volume(self, df: pd.DataFrame) -> float:
        """Estimate smart money volume percentage"""
        if len(df) < 50:
            return 0.0
        
        # Look for absorption patterns
        absorption_count = 0
        total_candles = min(30, len(df))
        
        for i in range(1, total_candles):
            candle = df.iloc[-i]
            
            # Check for long lower wick followed by bullish candle
            lower_wick = min(candle['open'], candle['close']) - candle['low']
            body = abs(candle['close'] - candle['open'])
            
            if lower_wick > body * 1.5:
                # Check next candle
                if i > 1:
                    next_candle = df.iloc[-i+1]
                    if next_candle['close'] > candle['close']:
                        absorption_count += 1
        
        return (absorption_count / total_candles) * 100

class VolumeAnomalyDetector:
    """Detect volume anomalies with detailed logic"""
    
    def analyze(self, df_15m: pd.DataFrame, df_5m: pd.DataFrame, signal_type: str) -> VolumeAnomalyLogic:
        """Comprehensive volume analysis"""
        logic = VolumeAnomalyLogic()
        
        if len(df_15m) < 48 or len(df_5m) < 50:
            return logic
        
        try:
            # 1. Volume spike detection
            spike_analysis = self._detect_volume_spike(df_15m)
            logic.volume_spike_ratio = spike_analysis['ratio']
            logic.spike_confidence = spike_analysis['confidence']
            logic.spike_duration_candles = spike_analysis['duration']
            
            # 2. Volume profile analysis
            profile = self._analyze_volume_profile(df_5m)
            logic.buy_volume_pct = profile['buy_pct']
            logic.sell_volume_pct = profile['sell_pct']
            logic.volume_imbalance = profile['imbalance']
            
            # 3. Large transaction analysis
            large_tx = self._analyze_large_transactions(df_15m)
            logic.large_buy_orders = large_tx['buy_orders']
            logic.large_sell_orders = large_tx['sell_orders']
            logic.large_order_ratio = large_tx['ratio']
            
            # 4. Volume divergence
            divergence = self._detect_volume_divergence(df_15m)
            logic.price_volume_divergence = divergence['type']
            logic.divergence_strength = divergence['strength']
            
            # 5. Volume climax detection (for dumps)
            if signal_type == "dump_short":
                climax = self._detect_volume_climax(df_15m)
                logic.volume_climax = climax['detected']
                logic.climax_ratio = climax['ratio']
                logic.climax_candle_size_pct = climax['candle_size']
            
            # 6. Summary scores
            logic.volume_anomaly_score = self._calculate_volume_score(logic, signal_type)
            logic.anomaly_type = self._determine_anomaly_type(logic, signal_type)
            logic.anomaly_confidence = self._calculate_anomaly_confidence(logic)
            
        except Exception as e:
            logging.error(f"Volume analysis error: {e}")
        
        return logic
    
    def _detect_volume_spike(self, df: pd.DataFrame) -> Dict:
        """Detect and analyze volume spikes"""
        if len(df) < 24:
            return {"ratio": 1.0, "confidence": 0.0, "duration": 0}
        
        # Current volume vs average
        current_volume = df['volume'].iloc[-1]
        avg_volume_6h = df['volume'].iloc[-24:].mean()
        avg_volume_24h = df['volume'].iloc[-96:].mean() if len(df) >= 96 else avg_volume_6h
        
        ratio_6h = current_volume / avg_volume_6h if avg_volume_6h > 0 else 1.0
        ratio_24h = current_volume / avg_volume_24h if avg_volume_24h > 0 else 1.0
        
        # Calculate confidence
        confidence = 0.0
        if ratio_6h >= VOLUME_SPIKE_THRESHOLD and ratio_24h >= VOLUME_SPIKE_THRESHOLD * 0.8:
            confidence = 0.9
        elif ratio_6h >= VOLUME_SPIKE_THRESHOLD:
            confidence = 0.7
        elif ratio_6h >= VOLUME_SPIKE_THRESHOLD * 0.7:
            confidence = 0.5
        
        # Check duration
        duration = 1
        for i in range(1, min(6, len(df))):
            if df['volume'].iloc[-i-1] > avg_volume_6h * 2:
                duration += 1
            else:
                break
        
        return {
            "ratio": float(max(ratio_6h, ratio_24h)),
            "confidence": confidence,
            "duration": duration
        }

class LiquidityEngineeringDetector:
    """Detect liquidity engineering patterns"""
    
    def analyze(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> LiquidityEngineeringLogic:
        """Analyze liquidity engineering patterns"""
        logic = LiquidityEngineeringLogic()
        
        if len(df_1h) < 50 or len(df_15m) < 40:
            return logic
        
        try:
            # 1. Stop hunt detection
            stop_hunt = self._detect_stop_hunt(df_1h)
            logic.stop_hunt_detected = stop_hunt['detected']
            logic.stop_hunt_direction = stop_hunt['direction']
            logic.stop_hunt_strength = stop_hunt['strength']
            logic.stop_hunt_level = stop_hunt['level']
            
            # 2. Fake level detection
            fake_levels = self._find_fake_levels(df_15m)
            logic.fake_resistance_levels = fake_levels['resistance']
            logic.fake_support_levels = fake_levels['support']
            logic.fake_level_count = len(fake_levels['resistance']) + len(fake_levels['support'])
            
            # 3. Liquidity clusters
            clusters = self._identify_liquidity_clusters(df_1h)
            logic.liquidity_clusters = clusters
            logic.cluster_strength = self._calculate_cluster_strength(clusters)
            
            # 4. Order book analysis (estimated)
            orderbook = self._estimate_orderbook(df_15m)
            logic.bid_wall_size = orderbook['bid_wall']
            logic.ask_wall_size = orderbook['ask_wall']
            logic.orderbook_imbalance = orderbook['imbalance']
            
            # 5. Manipulation patterns
            manipulation = self._detect_manipulation_patterns(df_1h)
            logic.squeeze_detected = manipulation['squeeze']
            logic.squeeze_strength = manipulation['strength']
            logic.volatility_expansion_expected = manipulation['expansion_expected']
            
            # 6. Summary
            logic.engineering_score = self._calculate_engineering_score(logic)
            logic.manipulation_present = logic.engineering_score > 0.6
            logic.next_target_level = self._determine_next_target(logic, df_1h)
            
        except Exception as e:
            logging.error(f"Liquidity analysis error: {e}")
        
        return logic

class MomentumAnalyzer:
    """Analyze momentum with detailed logic"""
    
    def analyze(self, df_15m: pd.DataFrame, df_5m: pd.DataFrame, signal_type: str) -> MomentumLogic:
        """Comprehensive momentum analysis"""
        logic = MomentumLogic()
        
        if len(df_15m) < 30 or len(df_5m) < 20:
            return logic
        
        try:
            current_price = df_15m['close'].iloc[-1]
            
            # 1. RSI analysis
            rsi_values = calculate_rsi(df_15m['close'], period=14)
            if not rsi_values.empty:
                logic.rsi_value = float(rsi_values.iloc[-1])
                logic.rsi_zone = self._determine_rsi_zone(logic.rsi_value)
                logic.rsi_divergence, logic.rsi_divergence_strength = self._detect_rsi_divergence(df_15m, rsi_values)
            
            # 2. MACD analysis
            macd_analysis = self._analyze_macd(df_15m)
            logic.macd_histogram = macd_analysis['histogram']
            logic.macd_trend = macd_analysis['trend']
            logic.macd_cross_signal = macd_analysis['cross_signal']
            
            # 3. Price action
            candle_pattern = self._analyze_candle_patterns(df_5m)
            logic.candle_pattern = candle_pattern['pattern']
            logic.pattern_strength = candle_pattern['strength']
            logic.consecutive_green = self._count_consecutive(df_5m, 'green')
            logic.consecutive_red = self._count_consecutive(df_5m, 'red')
            
            # 4. Trend strength
            trend = self._analyze_trend_strength(df_15m)
            logic.trend_strength = trend['strength']
            logic.trend_direction = trend['direction']
            logic.ema_alignment = self._check_ema_alignment(df_15m)
            
            # 5. Volume confirmation
            volume_conf = self._check_volume_confirmation(df_5m, signal_type)
            logic.volume_confirmation = volume_conf['confirmed']
            logic.volume_momentum = volume_conf['momentum']
            
            # 6. Summary
            logic.momentum_score = self._calculate_momentum_score(logic, signal_type)
            logic.momentum_bias = self._determine_momentum_bias(logic)
            logic.exhaustion_detected = self._detect_exhaustion(logic, df_5m)
            
        except Exception as e:
            logging.error(f"Momentum analysis error: {e}")
        
        return logic

# ================ MEXC FUTURES PUMP & DUMP SNIPER ================
class MexcPumpDumpSniper:
    """Main MEXC Futures Pump & Dump Sniper"""
    
    def __init__(self):
        self.exchange = None
        self.accumulation_detector = AccumulationDetector()
        self.volume_detector = VolumeAnomalyDetector()
        self.liquidity_detector = LiquidityEngineeringDetector()
        self.momentum_analyzer = MomentumAnalyzer()
        self.active_signals = {}
        self.signal_history = deque(maxlen=100)
        
        # Statistics
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
                "options": {
                    "defaultType": "swap",
                    "fetchMarkets": "swap",
                },
                "timeout": 30000,
            })
            
            # Test connection
            markets = await self.exchange.fetch_markets(params={'type': 'swap'})
            log.info(f"✅ MEXC Futures connected. {len(markets)} swap markets available")
            
            return True
            
        except Exception as e:
            log.error(f"Failed to connect to MEXC: {e}")
            return False
    
    async def analyze_symbol(self, symbol: str) -> Optional[CompleteSignal]:
        """
        Comprehensive analysis for pump AND dump opportunities
        Returns detailed signal with full logic breakdown
        """
        try:
            self.stats["pairs_analyzed"] += 1
            
            # 1. Fetch multi-timeframe data
            timeframe_data = await self._fetch_multi_timeframe_data(symbol)
            if not timeframe_data:
                return None
            
            # 2. Check if it's a low-cap candidate
            if not await self._is_low_cap_candidate(symbol, timeframe_data):
                return None
            
            # 3. Analyze for PUMP potential
            pump_signal = await self._analyze_pump_potential(symbol, timeframe_data)
            
            # 4. Analyze for DUMP potential
            dump_signal = await self._analyze_dump_potential(symbol, timeframe_data)
            
            # 5. Select best signal
            best_signal = None
            if pump_signal and dump_signal:
                # Choose higher confidence signal
                if pump_signal.confidence_level >= dump_signal.confidence_level:
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
                
                # Store signal
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
    
    async def _analyze_pump_potential(self, symbol: str, timeframe_data: Dict) -> Optional[CompleteSignal]:
        """Analyze for pump (long) opportunity"""
        try:
            df_4h = timeframe_data.get("4H")
            df_1h = timeframe_data.get("1H")
            df_15m = timeframe_data.get("15M")
            df_5m = timeframe_data.get("5M")
            
            if not all([df_4h is not None, df_1h is not None, df_15m is not None, df_5m is not None]):
                return None
            
            current_price = df_5m['close'].iloc[-1]
            
            # 1. Accumulation analysis
            accumulation_logic = self.accumulation_detector.analyze(df_4h, df_1h)
            if not accumulation_logic.ready_for_breakout:
                self.stats["rejections"]["no_accumulation"] += 1
                return None
            
            # 2. Volume anomaly analysis
            volume_logic = self.volume_detector.analyze(df_15m, df_5m, "pump_long")
            if volume_logic.volume_anomaly_score < 0.6:
                self.stats["rejections"]["no_volume"] += 1
                return None
            
            # 3. Liquidity engineering analysis
            liquidity_logic = self.liquidity_detector.analyze(df_1h, df_15m)
            
            # 4. Momentum analysis
            momentum_logic = self.momentum_analyzer.analyze(df_15m, df_5m, "pump_long")
            
            # 5. Breakout detection
            breakout_logic = self._analyze_breakout(df_4h, df_1h, df_15m, "pump_long")
            if not breakout_logic.breakout_valid:
                return None
            
            # 6. Calculate risk parameters
            risk_logic = self._calculate_risk_parameters(
                current_price, accumulation_logic, breakout_logic, "pump_long"
            )
            
            if risk_logic.risk_reward_ratio < MIN_RISK_REWARD:
                self.stats["rejections"]["bad_risk_reward"] += 1
                return None
            
            # 7. Calculate overall score
            overall_score = self._calculate_overall_score(
                accumulation_logic, volume_logic, liquidity_logic,
                momentum_logic, breakout_logic, risk_logic, "pump_long"
            )
            
            if overall_score < MIN_CONFLUENCE_SCORE:
                self.stats["rejections"]["low_score"] += 1
                return None
            
            # 8. Create complete signal
            signal = CompleteSignal(
                signal_id="",  # Will be set later
                symbol=symbol,
                signal_type="pump_long",
                timestamp=time.time(),
                
                current_price=current_price,
                market_cap=await self._estimate_market_cap(symbol, current_price),
                volume_24h=await self._get_24h_volume(symbol),
                
                accumulation_logic=accumulation_logic,
                volume_logic=volume_logic,
                liquidity_logic=liquidity_logic,
                momentum_logic=momentum_logic,
                breakout_logic=breakout_logic,
                risk_logic=risk_logic,
                
                entry_price=breakout_logic.breakout_level,
                entry_type="limit",
                entry_conditions=self._generate_entry_conditions(breakout_logic),
                
                stop_loss=risk_logic.stop_loss_level,
                take_profit=risk_logic.take_profit_levels[-1] if risk_logic.take_profit_levels else current_price * 1.15,
                breakeven_level=self._calculate_breakeven(breakout_logic.breakout_level, risk_logic.stop_loss_level),
                
                overall_score=overall_score,
                confidence_level=self._calculate_confidence(
                    overall_score, accumulation_logic, volume_logic, breakout_logic
                ),
                urgency_level=self._determine_urgency(breakout_logic, volume_logic),
                
                timeframes_analyzed=list(TIMEFRAMES.keys()),
                primary_timeframe="4H"
            )
            
            return signal
            
        except Exception as e:
            log.error(f"Pump analysis error for {symbol}: {e}")
            return None
    
    async def _analyze_dump_potential(self, symbol: str, timeframe_data: Dict) -> Optional[CompleteSignal]:
        """Analyze for dump (short) opportunity"""
        try:
            df_4h = timeframe_data.get("4H")
            df_1h = timeframe_data.get("1H")
            df_15m = timeframe_data.get("15M")
            df_5m = timeframe_data.get("5M")
            
            if not all([df_4h is not None, df_1h is not None, df_15m is not None, df_5m is not None]):
                return None
            
            current_price = df_5m['close'].iloc[-1]
            
            # Check for distribution patterns (inverse of accumulation)
            distribution_detected = self._detect_distribution(df_4h, df_1h)
            if not distribution_detected:
                return None
            
            # Check for volume climax (distribution volume)
            volume_logic = self.volume_detector.analyze(df_15m, df_5m, "dump_short")
            if not volume_logic.volume_climax:
                return None
            
            # Check for parabolic move (topping pattern)
            parabolic = self._detect_parabolic_move(df_4h)
            if not parabolic:
                return None
            
            # Check momentum exhaustion
            momentum_logic = self.momentum_analyzer.analyze(df_15m, df_5m, "dump_short")
            if not momentum_logic.exhaustion_detected:
                return None
            
            # Check RSI overbought
            if momentum_logic.rsi_value < RSI_OVERBOUGHT:
                return None
            
            # Look for breakdown setup
            breakdown_logic = self._analyze_breakdown(df_4h, df_1h, df_15m)
            if not breakdown_logic.breakout_valid:
                return None
            
            # Calculate risk for short
            risk_logic = self._calculate_risk_parameters(
                current_price, None, breakdown_logic, "dump_short"
            )
            
            if risk_logic.risk_reward_ratio < MIN_RISK_REWARD:
                return None
            
            # Create dump signal (simplified for this example)
            # Full implementation would mirror pump analysis
            
            return None
            
        except Exception as e:
            log.error(f"Dump analysis error for {symbol}: {e}")
            return None

# ================ SIGNAL FORMATTER ================
class SignalFormatter:
    """Format complete signals with detailed logic breakdown"""
    
    @staticmethod
    def format_for_telegram(signal: CompleteSignal) -> str:
        """Format signal for Telegram with full logic breakdown"""
        
        # Emojis based on signal type and quality
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
        
        # Format logic breakdown sections
        logic_sections = []
        
        # 1. Accumulation Logic
        acc = signal.accumulation_logic
        logic_sections.append(f"""
<b>🏗️ ACCUMULATION LOGIC ({acc.accumulation_confidence:.0%})</b>
• Phase: {acc.accumulation_phase.upper()}
• Duration: {acc.days_in_accumulation:.1f} days
• Compression: {acc.price_compression_pct:.1f}% range
• Wick Absorption: {acc.lower_wick_absorption:.0%}
• Order Blocks: {acc.order_block_count} (Quality: {acc.order_block_quality:.0%})
• Ready for Breakout: {'✅ YES' if acc.ready_for_breakout else '❌ NO'}
""")
        
        # 2. Volume Logic
        vol = signal.volume_logic
        logic_sections.append(f"""
<b>💧 VOLUME LOGIC ({vol.volume_anomaly_score:.1f}/1.0)</b>
• Spike: {vol.volume_spike_ratio:.1f}x (Confidence: {vol.spike_confidence:.0%})
• Buy/Sell Ratio: {vol.buy_volume_pct:.0%}/{vol.sell_volume_pct:.0%}
• Imbalance: {vol.volume_imbalance:+.2f}
• Large Orders: {vol.large_buy_orders} buy, {vol.large_sell_orders} sell
• Divergence: {vol.price_volume_divergence.upper() if vol.price_volume_divergence != 'none' else 'NONE'}
• Anomaly Type: {vol.anomaly_type.upper()}
""")
        
        # 3. Liquidity Logic
        liq = signal.liquidity_logic
        logic_sections.append(f"""
<b>⚡ LIQUIDITY ENGINEERING ({liq.engineering_score:.1f}/1.0)</b>
• Stop Hunt: {'✅ DETECTED' if liq.stop_hunt_detected else '❌ NONE'}
• Fake Levels: {liq.fake_level_count}
• Liquidity Clusters: {len(liq.liquidity_clusters)}
• Orderbook Imbalance: {liq.orderbook_imbalance:+.2f}
• Squeeze: {'✅ DETECTED' if liq.squeeze_detected else '❌ NONE'}
• Next Target: {liq.next_target_level:.8f}
""")
        
        # 4. Momentum Logic
        mom = signal.momentum_logic
        logic_sections.append(f"""
<b>📈 MOMENTUM LOGIC ({mom.momentum_score:.1f}/1.0)</b>
• RSI: {mom.rsi_value:.1f} ({mom.rsi_zone.upper()})
• Divergence: {mom.rsi_divergence.upper() if mom.rsi_divergence != 'none' else 'NONE'}
• MACD: {mom.macd_trend.upper()} ({mom.macd_cross_signal if mom.macd_cross_signal != 'none' else 'NO CROSS'})
• Pattern: {mom.candle_pattern.upper().replace('_', ' ') if mom.candle_pattern != 'none' else 'NONE'}
• Trend: {mom.trend_direction.upper()} ({mom.trend_strength:.0%})
• Volume Confirmed: {'✅ YES' if mom.volume_confirmation else '❌ NO'}
• Bias: {mom.momentum_bias.upper()}
• Exhaustion: {'⚠️ DETECTED' if mom.exhaustion_detected else '✅ NONE'}
""")
        
        # 5. Breakout Logic
        brk = signal.breakout_logic
        logic_sections.append(f"""
<b>🎯 BREAKOUT LOGIC ({brk.breakout_quality:.1f}/1.0)</b>
• Type: {brk.breakout_type.upper().replace('_', ' ')}
• Level: {brk.breakout_level:.8f}
• Strength: {brk.breakout_strength:.0%}
• Volume Confirmation: {'✅ YES' if brk.volume_confirmation else '❌ NO'}
• Retest: {'✅ SUCCESSFUL' if brk.retest_successful else '❌ FAILED' if brk.retest_occurred else '⏳ PENDING'}
• Follow-through: {brk.follow_through_candles} candles ({brk.follow_through_strength:.0%})
• Expected Move: {brk.expected_move_pct:.1f}%
• Valid: {'✅ YES' if brk.breakout_valid else '❌ NO'}
""")
        
        # 6. Risk Logic
        risk = signal.risk_logic
        logic_sections.append(f"""
<b>🛡️ RISK LOGIC</b>
• Position Size: {risk.position_size_pct:.1f}%
• Leverage: {risk.leverage}x
• Stop Loss: {risk.stop_loss_level:.8f} ({risk.stop_loss_distance_pct:.1f}%)
• Stop Type: {risk.stop_loss_type.upper()}
• Take Profit: {' | '.join([f'{tp:.8f}' for tp in risk.take_profit_levels[:3]])}
• Risk/Reward: {risk.risk_reward_ratio:.1f}:1
• Probability: {risk.probability_score:.0%}
• Expectancy: {risk.expectancy:+.3f}
• Market Regime: {risk.market_regime.upper()}
""")
        
        # 7. Trade Parameters
        entry_conditions = "\n".join([f"• {cond}" for cond in signal.entry_conditions[:5]])
        
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
{entry_conditions}
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

# ================ LOGGING SETUP ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("mexc_pump_dump")

# ================ MAIN BOT ================
class MexcPumpDumpBot:
    """Main bot orchestrating the pump & dump sniper"""
    
    def __init__(self):
        self.sniper = MexcPumpDumpSniper()
        self.formatter = SignalFormatter()
        self.db = None
        self.scanning_active = False
        
    async def initialize(self):
        """Initialize the bot"""
        log.info("=" * 70)
        log.info("🚀 MEXC FUTURES PUMP & DUMP SNIPER v1.0")
        log.info("SPECIALIZATION: Low-cap pump AND dump detection")
        log.info("FEATURE: Complete logic breakdown in every signal")
        log.info("EXCHANGE: MEXC Futures (Public API)")
        log.info("=" * 70)
        log.info("🎯 TARGET: $500K-$20M market cap tokens")
        log.info("⚡ STRATEGY: Long accumulations, Short distributions")
        log.info("📊 OUTPUT: Detailed logic breakdown for every signal")
        log.info("🛡️ RISK: 3% max loss, 3:1 minimum R:R")
        log.info("=" * 70)
        
        # Initialize exchange
        if not await self.sniper.initialize_exchange():
            log.error("Failed to initialize exchange")
            return False
        
        # Initialize database
        await self._init_database()
        
        # Send startup message
        await self._send_startup_message()
        
        return True
    
    async def run_scanning(self, scan_interval: int = 30):
        """Run continuous scanning"""
        self.scanning_active = True
        scan_count = 0
        
        while self.scanning_active:
            try:
                scan_count += 1
                log.info(f"🔄 Scan #{scan_count}")
                
                # Get low-cap futures pairs from MEXC
                pairs = await self._get_mexc_futures_pairs()
                log.info(f"Found {len(pairs)} potential low-cap pairs")
                
                # Analyze each pair
                signals_found = 0
                for symbol in pairs[:20]:  # Limit to 20 pairs per scan
                    signal = await self.sniper.analyze_symbol(symbol)
                    
                    if signal:
                        signals_found += 1
                        
                        # Format and send signal
                        await self._process_signal(signal)
                        
                        # Save to database
                        await self._save_signal(signal)
                
                # Log statistics
                stats = self.sniper.stats
                log.info(f"📊 Scan #{scan_count} Complete:")
                log.info(f"   Signals found: {signals_found}")
                log.info(f"   Total pump signals: {stats['pump_signals']}")
                log.info(f"   Total dump signals: {stats['dump_signals']}")
                log.info(f"   High quality: {stats['high_quality_signals']}")
                
                # Wait for next scan
                await asyncio.sleep(scan_interval)
                
            except Exception as e:
                log.error(f"Scanning error: {e}")
                await asyncio.sleep(10)
    
    async def _process_signal(self, signal: CompleteSignal):
        """Process and send a detected signal"""
        try:
            # Format signal with full logic breakdown
            telegram_message = self.formatter.format_for_telegram(signal)
            
            # Send to Telegram
            await self._send_telegram_message(telegram_message)
            
            log.info(f"📤 Signal sent: {signal.symbol} ({signal.signal_type})")
            
        except Exception as e:
            log.error(f"Signal processing error: {e}")

# ================ MAIN EXECUTION ================
async def main():
    """Main execution function"""
    bot = MexcPumpDumpBot()
    
    if await bot.initialize():
        log.info("✅ Bot initialized successfully")
        
        # Run scanning
        try:
            await bot.run_scanning(scan_interval=30)
        except KeyboardInterrupt:
            log.info("Bot stopped by user")
    else:
        log.error("Failed to initialize bot")

if __name__ == "__main__":
    asyncio.run(main())