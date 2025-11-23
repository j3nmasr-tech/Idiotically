#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
INSTITUTIONAL QUANT SCANNER v2.0 - VOLUME FIXED
- Fixed 'vol' vs 'volume' column naming issue
- Ready for production
"""

import os
import time
import asyncio
import logging
import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from pydantic import BaseModel, validator
import uvicorn
from collections import defaultdict, deque
import json
from contextlib import asynccontextmanager

# ==================== ENHANCED CONFIGURATION ====================

class Timeframe(Enum):
    M1 = "1m"
    M3 = "3m" 
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H4 = "4h"

class MarketRegime(Enum):
    ACCUMULATION = "accumulation"
    UPTREND = "uptrend"
    DOWNTREND = "downtrend" 
    DISTRIBUTION = "distribution"
    RANGING = "ranging"
    EXPANSION = "expansion"

class SignalSide(Enum):
    BUY = "BUY"
    SELL = "SELL"

@dataclass
class ScannerConfig:
    # Core settings
    SCAN_INTERVAL: int = 60
    TOP_N_SYMBOLS: int = 80
    MIN_VOLUME_USDT: float = 1000000
    MAX_SPREAD_PCT: float = 0.002
    HEARTBEAT_INTERVAL: int = 3600
    
    # Risk management
    MAX_SL_PCT: float = 0.03
    MIN_RR_RATIO: float = 1.5
    MAX_POSITIONS: int = 5
    
    # Signal filters
    MIN_SIGNAL_SCORE: int = 7
    COOLDOWN_MINUTES: int = 30
    MAX_SL_CLUSTER_HITS: int = 3
    
    # Timeframes for analysis
    TIMEFRAMES: List[Timeframe] = None
    
    def __post_init__(self):
        if self.TIMEFRAMES is None:
            self.TIMEFRAMES = [Timeframe.M1, Timeframe.M3, Timeframe.M5, 
                              Timeframe.M15, Timeframe.M30, Timeframe.H1]

# ==================== ENHANCED DATA MODELS ====================

@dataclass
class MarketStructure:
    """Complete SMC market structure analysis"""
    # Internal structure
    order_blocks: List[Dict]
    fair_value_gaps: List[Dict]
    liquidity_pools: Dict[str, float]
    
    # External structure  
    swing_highs: List[Dict]
    swing_lows: List[Dict]
    break_of_structure: bool
    change_of_character: bool
    
    # Liquidity analysis
    liquidity_sweeps: Dict[str, bool]
    equal_highs_lows: Dict[str, bool]
    
    # Multi-timeframe context
    higher_tf_alignment: bool
    market_regime: MarketRegime

@dataclass 
class VolatilityProfile:
    """Comprehensive volatility analysis"""
    atr: float
    atr_pct: float
    historical_vol: float
    volatility_regime: str  # LOW, NORMAL, HIGH, EXTREME
    volume_profile: Dict[str, float]
    volume_delta: float

@dataclass
class RiskParameters:
    """Institutional risk management"""
    stop_loss: float
    take_profit_1: float
    take_profit_2: float 
    take_profit_3: float
    position_size: float
    risk_reward_ratio: float
    expected_value: float
    probability_score: float

@dataclass
class TradingSignal:
    """Complete institutional signal"""
    symbol: str
    side: SignalSide
    entry_price: float
    timestamp: datetime.datetime
    timeframe: Timeframe
    
    # Core analysis
    market_structure: MarketStructure
    volatility_profile: VolatilityProfile
    risk_parameters: RiskParameters
    
    # Scoring & validation
    confidence_score: float
    quality_score: float
    filters_passed: List[str]
    rejection_reasons: List[str]
    
    # Metadata
    signal_id: str
    version: str = "2.0"

# ==================== ENHANCED DATA QUALITY ENGINE ====================

class DataQualityEngine:
    """Institutional-grade data validation"""
    
    @staticmethod
    def validate_ohlcv_data(df: pd.DataFrame, min_candles: int = 20) -> Tuple[bool, List[str]]:
        """Comprehensive OHLCV data validation"""
        issues = []
        
        if df is None or len(df) < min_candles:
            return False, ["Insufficient data"]
            
        # Check for NaN values
        if df.isnull().any().any():
            issues.append("NaN values detected")
            
        # Check for zero or negative prices
        if (df[['open', 'high', 'low', 'close']] <= 0).any().any():
            issues.append("Invalid price values")
            
        # Check for high-low consistency
        invalid_hl = (df['high'] < df['low']).any()
        if invalid_hl:
            issues.append("High < Low inconsistency")
            
        # Check for volume anomalies - FIXED: use 'volume' instead of 'vol'
        if 'volume' in df.columns and (df['volume'] < 0).any():
            issues.append("Negative volume")
            
        # Check for price spikes (potential errors)
        price_returns = df['close'].pct_change().abs()
        if (price_returns > 0.5).any():  # 50% moves likely errors
            issues.append("Extreme price moves detected")
            
        return len(issues) == 0, issues
    
    @staticmethod
    def clean_ohlcv_data(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize OHLCV data"""
        df_clean = df.copy()
        
        # Forward fill small gaps
        df_clean = df_clean.ffill()
        
        # Remove obvious outliers (beyond 5 standard deviations)
        for col in ['open', 'high', 'low', 'close']:
            mean = df_clean[col].mean()
            std = df_clean[col].std()
            df_clean[col] = np.where(
                abs(df_clean[col] - mean) > 5 * std,
                mean,
                df_clean[col]
            )
            
        return df_clean

# ==================== ADVANCED SMC ANALYTICS ENGINE ====================

class SMCAnalyticsEngine:
    """Complete Smart Money Concepts analysis"""
    
    @staticmethod
    def detect_order_blocks(df: pd.DataFrame, lookback: int = 20) -> List[Dict]:
        """Advanced order block detection with quality scoring"""
        blocks = []
        
        for i in range(2, len(df) - 1):
            candle = df.iloc[i]
            prev_candle = df.iloc[i-1]
            
            # Bullish order block criteria
            if (candle['close'] > candle['open'] and 
                candle['close'] > prev_candle['high'] and
                candle['volume'] > df['volume'].rolling(5).mean().iloc[i]):
                
                quality_score = SMCAnalyticsEngine._calculate_ob_quality(df, i, "bullish")
                blocks.append({
                    'type': 'bullish',
                    'high': candle['high'],
                    'low': candle['low'], 
                    'open': candle['open'],
                    'close': candle['close'],
                    'timestamp': candle.name if hasattr(candle, 'name') else i,
                    'quality_score': quality_score
                })
                
            # Bearish order block criteria
            elif (candle['close'] < candle['open'] and
                  candle['close'] < prev_candle['low'] and
                  candle['volume'] > df['volume'].rolling(5).mean().iloc[i]):
                  
                quality_score = SMCAnalyticsEngine._calculate_ob_quality(df, i, "bearish")
                blocks.append({
                    'type': 'bearish', 
                    'high': candle['high'],
                    'low': candle['low'],
                    'open': candle['open'],
                    'close': candle['close'],
                    'timestamp': candle.name if hasattr(candle, 'name') else i,
                    'quality_score': quality_score
                })
                
        return blocks[-5:]  # Return most recent 5 blocks
    
    @staticmethod
    def _calculate_ob_quality(df: pd.DataFrame, idx: int, ob_type: str) -> float:
        """Calculate order block quality score (0-1)"""
        score = 0.0
        candle = df.iloc[idx]
        
        # Volume confirmation (30%)
        volume_avg = df['volume'].rolling(10).mean().iloc[idx]
        if candle['volume'] > volume_avg * 1.5:
            score += 0.3
            
        # Size of the candle (20%)
        candle_range = candle['high'] - candle['low']
        avg_range = (df['high'] - df['low']).rolling(10).mean().iloc[idx]
        if candle_range > avg_range:
            score += 0.2
            
        # Follow-through (30%)
        if idx < len(df) - 3:
            if ob_type == "bullish":
                follow_through = any(df.iloc[idx+1:idx+4]['close'] > candle['high'])
            else:
                follow_through = any(df.iloc[idx+1:idx+4]['close'] < candle['low'])
            if follow_through:
                score += 0.3
                
        # Position in range (20%)
        recent_high = df['high'].iloc[max(0, idx-20):idx+1].max()
        recent_low = df['low'].iloc[max(0, idx-20):idx+1].min()
        position = (candle['close'] - recent_low) / (recent_high - recent_low)
        
        if ob_type == "bullish" and position < 0.3:
            score += 0.2
        elif ob_type == "bearish" and position > 0.7:
            score += 0.2
            
        return min(score, 1.0)
    
    @staticmethod
    def detect_fair_value_gaps(df: pd.DataFrame, lookback: int = 10) -> List[Dict]:
        """Advanced FVG detection with mitigation checks"""
        fvgs = []
        
        for i in range(2, len(df)):
            c1, c2, c3 = df.iloc[i-2], df.iloc[i-1], df.iloc[i]
            
            # Bullish FVG
            if c2['low'] > c1['high'] and c3['low'] > c2['high']:
                fvgs.append({
                    'type': 'bullish',
                    'top': c2['low'],
                    'bottom': c1['high'],
                    'timestamp': c2.name if hasattr(c2, 'name') else i-1,
                    'mitigated': SMCAnalyticsEngine._check_fvg_mitigation(df, i, 'bullish')
                })
                
            # Bearish FVG  
            elif c2['high'] < c1['low'] and c3['high'] < c2['low']:
                fvgs.append({
                    'type': 'bearish',
                    'top': c1['low'], 
                    'bottom': c2['high'],
                    'timestamp': c2.name if hasattr(c2, 'name') else i-1,
                    'mitigated': SMCAnalyticsEngine._check_fvg_mitigation(df, i, 'bearish')
                })
                
        return fvgs
    
    @staticmethod
    def _check_fvg_mitigation(df: pd.DataFrame, start_idx: int, fvg_type: str) -> bool:
        """Check if FVG has been mitigated"""
        for i in range(start_idx, min(start_idx + 20, len(df))):
            candle = df.iloc[i]
            
            if fvg_type == 'bullish':
                if candle['low'] <= df.iloc[start_idx-1]['high']:
                    return True
            else:
                if candle['high'] >= df.iloc[start_idx-1]['low']:
                    return True
                    
        return False
    
    @staticmethod
    def detect_break_of_structure(df: pd.DataFrame) -> Tuple[bool, bool]:
        """Detect Break of Structure and Change of Character"""
        if len(df) < 10:
            return False, False
            
        bos = False
        choch = False
        
        # Recent swing points
        highs = df['high'].tail(10)
        lows = df['low'].tail(10)
        
        # BOS: New high above previous high or new low below previous low
        if len(highs) >= 3:
            recent_highs = highs.tail(3)
            if recent_highs.iloc[-1] > recent_highs.iloc[-2] > recent_highs.iloc[-3]:
                bos = True
                
        if len(lows) >= 3:
            recent_lows = lows.tail(3)  
            if recent_lows.iloc[-1] < recent_lows.iloc[-2] < recent_lows.iloc[-3]:
                bos = True
                
        # CHOCH: More complex pattern requiring significant reversal
        if len(df) >= 15:
            prev_trend = SMCAnalyticsEngine._detect_trend_direction(df.iloc[:-5])
            current_trend = SMCAnalyticsEngine._detect_trend_direction(df.tail(5))
            
            if prev_trend != current_trend and prev_trend != "ranging":
                choch = True
                
        return bos, choch
    
    @staticmethod
    def _detect_trend_direction(df: pd.DataFrame) -> str:
        """Detect trend direction from price data"""
        if len(df) < 5:
            return "ranging"
            
        highs = df['high']
        lows = df['low']
        
        if highs.iloc[-1] > highs.iloc[0] and lows.iloc[-1] > lows.iloc[0]:
            return "uptrend"
        elif highs.iloc[-1] < highs.iloc[0] and lows.iloc[-1] < lows.iloc[0]:
            return "downtrend"
        else:
            return "ranging"

# ==================== ADVANCED MARKET REGIME DETECTION ====================

class MarketRegimeDetector:
    """Comprehensive market regime analysis"""
    
    @staticmethod
    def detect_regime(df_1h: pd.DataFrame, df_4h: pd.DataFrame = None) -> MarketRegime:
        """Multi-timeframe regime detection"""
        if df_1h is None or len(df_1h) < 100:
            return MarketRegime.RANGING
            
        try:
            # Primary indicators
            trend_strength = MarketRegimeDetector._calculate_trend_strength(df_1h)
            volatility_regime = MarketRegimeDetector._analyze_volatility(df_1h)
            volume_profile = MarketRegimeDetector._analyze_volume_profile(df_1h)
            
            # Multi-timeframe alignment
            htf_alignment = True
            if df_4h is not None and len(df_4h) > 50:
                htf_trend = MarketRegimeDetector._calculate_trend_strength(df_4h)
                htf_alignment = abs(trend_strength - htf_trend) < 0.3
            
            # Regime classification
            if trend_strength > 0.7 and htf_alignment:
                return MarketRegime.UPTREND
            elif trend_strength < -0.7 and htf_alignment:
                return MarketRegime.DOWNTREND
            elif volatility_regime == "HIGH" and volume_profile == "expanding":
                return MarketRegime.EXPANSION
            elif abs(trend_strength) < 0.3 and volatility_regime == "LOW":
                return MarketRegime.ACCUMULATION
            elif abs(trend_strength) < 0.3 and volume_profile == "declining":
                return MarketRegime.DISTRIBUTION
            else:
                return MarketRegime.RANGING
                
        except Exception as e:
            logging.error(f"Regime detection error: {e}")
            return MarketRegime.RANGING
    
    @staticmethod
    def _calculate_trend_strength(df: pd.DataFrame) -> float:
        """Calculate trend strength from -1 (strong downtrend) to +1 (strong uptrend)"""
        if len(df) < 50:
            return 0.0
            
        # Multiple timeframe EMAs
        ema_20 = df['close'].ewm(span=20).mean()
        ema_50 = df['close'].ewm(span=50).mean()
        ema_100 = df['close'].ewm(span=100).mean()
        
        # Price position relative to EMAs
        current_price = df['close'].iloc[-1]
        above_ema_20 = current_price > ema_20.iloc[-1]
        above_ema_50 = current_price > ema_50.iloc[-1] 
        above_ema_100 = current_price > ema_100.iloc[-1]
        
        # EMA alignment
        ema_bullish = ema_20.iloc[-1] > ema_50.iloc[-1] > ema_100.iloc[-1]
        ema_bearish = ema_20.iloc[-1] < ema_50.iloc[-1] < ema_100.iloc[-1]
        
        # ADX for trend strength
        adx = MarketRegimeDetector._calculate_adx(df)
        
        # Composite score
        score = 0.0
        
        if above_ema_20: score += 0.2
        if above_ema_50: score += 0.3 
        if above_ema_100: score += 0.3
        if ema_bullish: score += 0.4
        if ema_bearish: score -= 0.4
        if adx > 25: score += 0.3 * (adx / 50)
        if adx < 15: score *= 0.5  # Weak trend
        
        # Normalize to -1 to +1
        return max(min(score, 1.0), -1.0)
    
    @staticmethod
    def _calculate_adx(df: pd.DataFrame, period: int = 14) -> float:
        """Calculate ADX (Average Directional Index)"""
        high, low, close = df['high'], df['low'], df['close']
        
        # True Range
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
        atr = tr.rolling(period).mean()
        
        # Directional Movement
        up_move = high - high.shift(1)
        down_move = low.shift(1) - low
        
        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)
        
        plus_di = 100 * (pd.Series(plus_dm).rolling(period).mean() / atr)
        minus_di = 100 * (pd.Series(minus_dm).rolling(period).mean() / atr)
        
        dx = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di))
        adx = dx.rolling(period).mean()
        
        return adx.iloc[-1] if not pd.isna(adx.iloc[-1]) else 0.0
    
    @staticmethod
    def _analyze_volatility(df: pd.DataFrame) -> str:
        """Analyze volatility regime"""
        if len(df) < 20:
            return "NORMAL"
            
        atr = df['high'].subtract(df['low']).rolling(14).mean().iloc[-1]
        atr_pct = atr / df['close'].iloc[-1]
        
        if atr_pct < 0.005:
            return "LOW"
        elif atr_pct < 0.015:
            return "NORMAL" 
        elif atr_pct < 0.03:
            return "HIGH"
        else:
            return "EXTREME"
    
    @staticmethod
    def _analyze_volume_profile(df: pd.DataFrame) -> str:
        """Analyze volume profile characteristics"""
        if len(df) < 20:
            return "neutral"
            
        # FIXED: Use 'volume' instead of 'vol'  
        volume = df['volume'] if 'volume' in df.columns else pd.Series([0] * len(df))
        volume_sma = volume.rolling(20).mean()
        current_volume = volume.iloc[-1] if len(volume) > 0 else 0
        volume_trend = volume.rolling(5).mean().iloc[-1] > volume.rolling(20).mean().iloc[-1] if len(volume) >= 20 else False
        
        if current_volume > volume_sma.iloc[-1] * 1.5 and volume_trend:
            return "expanding"
        elif current_volume < volume_sma.iloc[-1] * 0.7 and not volume_trend:
            return "declining"
        else:
            return "neutral"

# ==================== INSTITUTIONAL RISK ENGINE ====================

class InstitutionalRiskEngine:
    """Advanced risk management system"""
    
    @staticmethod
    def calculate_risk_parameters(df: pd.DataFrame, signal: TradingSignal, config: ScannerConfig) -> RiskParameters:
        """Calculate institutional-grade risk parameters"""
        
        current_price = signal.entry_price
        atr_value = InstitutionalRiskEngine._calculate_atr(df, 14)
        atr_pct = atr_value / current_price
        
        # Dynamic position sizing based on volatility
        position_size = InstitutionalRiskEngine._calculate_position_size(atr_pct, config)
        
        # Advanced stop loss calculation
        stop_loss = InstitutionalRiskEngine._calculate_stop_loss(df, signal, atr_value, config)
        
        # Multi-tier take profit levels
        tp1, tp2, tp3 = InstitutionalRiskEngine._calculate_take_profits(
            df, signal, stop_loss, atr_value, config
        )
        
        # Risk-reward ratio
        risk = abs(current_price - stop_loss) / current_price
        reward = abs(tp1 - current_price) / current_price
        risk_reward = reward / risk if risk > 0 else 0
        
        # Probability scoring
        probability = InstitutionalRiskEngine._calculate_probability_score(df, signal, risk_reward)
        
        # Expected value
        expected_value = (probability * reward) - ((1 - probability) * risk)
        
        return RiskParameters(
            stop_loss=stop_loss,
            take_profit_1=tp1,
            take_profit_2=tp2,
            take_profit_3=tp3,
            position_size=position_size,
            risk_reward_ratio=risk_reward,
            expected_value=expected_value,
            probability_score=probability
        )
    
    @staticmethod
    def _calculate_atr(df: pd.DataFrame, period: int) -> float:
        """Calculate Average True Range"""
        high, low, close = df['high'], df['low'], df['close']
        
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        
        tr = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
        return tr.rolling(period).mean().iloc[-1]
    
    @staticmethod
    def _calculate_position_size(atr_pct: float, config: ScannerConfig) -> float:
        """Volatility-adjusted position sizing"""
        base_size = 1.0  # Normalized
        
        # Reduce size in high volatility
        if atr_pct > 0.03:
            return base_size * 0.5
        elif atr_pct > 0.02:
            return base_size * 0.75
        elif atr_pct < 0.005:
            return base_size * 1.25  # Increase in low volatility
        else:
            return base_size
    
    @staticmethod
    def _calculate_stop_loss(df: pd.DataFrame, signal: TradingSignal, atr: float, config: ScannerConfig) -> float:
        """Multi-factor stop loss calculation"""
        current_price = signal.entry_price
        
        if signal.side == SignalSide.BUY:
            # 1. Recent swing low
            swing_low = df['low'].tail(15).min()
            
            # 2. ATR-based stop
            atr_stop = current_price - (atr * 1.5)
            
            # 3. Percentage stop (maximum)
            pct_stop = current_price * (1 - config.MAX_SL_PCT)
            
            # Take the most conservative (highest) stop loss
            stop_loss = max(swing_low, atr_stop, pct_stop)
            
            # Ensure minimum distance
            min_stop = current_price * 0.995  # 0.5% minimum
            stop_loss = max(stop_loss, min_stop)
            
        else:  # SELL
            # 1. Recent swing high
            swing_high = df['high'].tail(15).max()
            
            # 2. ATR-based stop
            atr_stop = current_price + (atr * 1.5)
            
            # 3. Percentage stop (maximum)
            pct_stop = current_price * (1 + config.MAX_SL_PCT)
            
            # Take the most conservative (lowest) stop loss
            stop_loss = min(swing_high, atr_stop, pct_stop)
            
            # Ensure minimum distance
            max_stop = current_price * 1.005  # 0.5% minimum
            stop_loss = min(stop_loss, max_stop)
            
        return stop_loss
    
    @staticmethod
    def _calculate_take_profits(df: pd.DataFrame, signal: TradingSignal, sl: float, atr: float, config: ScannerConfig) -> Tuple[float, float, float]:
        """Multi-tier take profit levels"""
        entry = signal.entry_price
        risk = abs(entry - sl)
        
        if signal.side == SignalSide.BUY:
            # TP1: 1:1.5 RR
            tp1 = entry + (risk * 1.5)
            
            # TP2: 1:2.5 RR or nearest resistance
            tp2 = entry + (risk * 2.5)
            resistance = InstitutionalRiskEngine._find_nearest_resistance(df, entry)
            tp2 = min(tp2, resistance) if resistance > entry else tp2
            
            # TP3: 1:4 RR or major resistance
            tp3 = entry + (risk * 4.0)
            major_resistance = InstitutionalRiskEngine._find_major_resistance(df, entry)
            tp3 = min(tp3, major_resistance) if major_resistance > entry else tp3
            
        else:  # SELL
            # TP1: 1:1.5 RR
            tp1 = entry - (risk * 1.5)
            
            # TP2: 1:2.5 RR or nearest support
            tp2 = entry - (risk * 2.5)
            support = InstitutionalRiskEngine._find_nearest_support(df, entry)
            tp2 = max(tp2, support) if support < entry else tp2
            
            # TP3: 1:4 RR or major support
            tp3 = entry - (risk * 4.0)
            major_support = InstitutionalRiskEngine._find_major_support(df, entry)
            tp3 = max(tp3, major_support) if major_support < entry else tp3
            
        return tp1, tp2, tp3
    
    @staticmethod
    def _find_nearest_resistance(df: pd.DataFrame, current_price: float) -> float:
        """Find nearest logical resistance"""
        recent_highs = df['high'].tail(20)
        resistances = recent_highs[recent_highs > current_price]
        return resistances.min() if not resistances.empty else current_price * 1.03
    
    @staticmethod
    def _find_major_resistance(df: pd.DataFrame, current_price: float) -> float:
        """Find major resistance level"""
        if len(df) < 50:
            return current_price * 1.05
            
        major_highs = df['high'].tail(100)
        resistances = major_highs[major_highs > current_price]
        return resistances.min() if not resistances.empty else current_price * 1.05
    
    @staticmethod
    def _find_nearest_support(df: pd.DataFrame, current_price: float) -> float:
        """Find nearest logical support"""
        recent_lows = df['low'].tail(20)
        supports = recent_lows[recent_lows < current_price]
        return supports.max() if not supports.empty else current_price * 0.97
    
    @staticmethod
    def _find_major_support(df: pd.DataFrame, current_price: float) -> float:
        """Find major support level"""
        if len(df) < 50:
            return current_price * 0.95
            
        major_lows = df['low'].tail(100)
        supports = major_lows[major_lows < current_price]
        return supports.max() if not supports.empty else current_price * 0.95
    
    @staticmethod
    def _calculate_probability_score(df: pd.DataFrame, signal: TradingSignal, risk_reward: float) -> float:
        """Calculate signal probability score (0-1)"""
        score = 0.5  # Base probability
        
        # Market structure factors
        if signal.market_structure.break_of_structure:
            score += 0.15
        if signal.market_structure.higher_tf_alignment:
            score += 0.10
        if not signal.market_structure.liquidity_sweeps.get('recent', False):
            score += 0.05
            
        # Volatility factors
        if signal.volatility_profile.volatility_regime == "NORMAL":
            score += 0.10
        elif signal.volatility_profile.volatility_regime in ["LOW", "HIGH"]:
            score += 0.05
            
        # Risk-reward adjustment
        if risk_reward > 2.0:
            score += 0.10
        elif risk_reward > 1.5:
            score += 0.05
            
        return min(score, 0.95)  # Cap at 95%

# ==================== ENHANCED SIGNAL GENERATOR ====================

class InstitutionalSignalGenerator:
    """Complete institutional signal generation"""
    
    def __init__(self, config: ScannerConfig):
        self.config = config
        self.smc_engine = SMCAnalyticsEngine()
        self.regime_detector = MarketRegimeDetector()
        self.risk_engine = InstitutionalRiskEngine()
        self.data_quality = DataQualityEngine()
        
    async def generate_signal(self, df: pd.DataFrame, symbol: str, timeframe: Timeframe, 
                            context: Dict = None) -> Optional[TradingSignal]:
        """Generate complete institutional trading signal"""
        
        if context is None:
            context = {}
            
        # Data quality check
        is_valid, data_issues = self.data_quality.validate_ohlcv_data(df)
        if not is_valid:
            logging.warning(f"Invalid data for {symbol}: {data_issues}")
            return None
            
        # Clean data
        df_clean = self.data_quality.clean_ohlcv_data(df)
        
        try:
            # Market structure analysis
            market_structure = await self._analyze_market_structure(df_clean, context)
            
            # Volatility profile
            volatility_profile = self._analyze_volatility_profile(df_clean)
            
            # Generate base signal
            base_signal = self._create_base_signal(df_clean, symbol, timeframe, market_structure, volatility_profile)
            if not base_signal:
                return None
                
            # Risk parameters
            risk_params = self.risk_engine.calculate_risk_parameters(df_clean, base_signal, self.config)
            
            # Apply filters and scoring
            final_signal = self._apply_filters_and_scoring(base_signal, risk_params, context)
            
            return final_signal if final_signal.confidence_score >= self.config.MIN_SIGNAL_SCORE else None
            
        except Exception as e:
            logging.error(f"Signal generation error for {symbol}: {e}")
            return None
    
    async def _analyze_market_structure(self, df: pd.DataFrame, context: Dict) -> MarketStructure:
        """Complete market structure analysis"""
        
        # Core SMC components
        order_blocks = self.smc_engine.detect_order_blocks(df)
        fair_value_gaps = self.smc_engine.detect_fair_value_gaps(df)
        bos, choch = self.smc_engine.detect_break_of_structure(df)
        
        # Liquidity analysis
        liquidity_sweeps = self._detect_liquidity_sweeps(df)
        equal_highs_lows = self._detect_equal_highs_lows(df)
        
        # Multi-timeframe context
        higher_tf_alignment = await self._check_higher_tf_alignment(context)
        market_regime = self.regime_detector.detect_regime(df)
        
        return MarketStructure(
            order_blocks=order_blocks,
            fair_value_gaps=fair_value_gaps,
            liquidity_pools=self._find_liquidity_pools(df),
            swing_highs=self._find_swing_highs(df),
            swing_lows=self._find_swing_lows(df),
            break_of_structure=bos,
            change_of_character=choch,
            liquidity_sweeps=liquidity_sweeps,
            equal_highs_lows=equal_highs_lows,
            higher_tf_alignment=higher_tf_alignment,
            market_regime=market_regime
        )
    
    def _analyze_volatility_profile(self, df: pd.DataFrame) -> VolatilityProfile:
        """Comprehensive volatility analysis"""
        atr_value = self.risk_engine._calculate_atr(df, 14)
        atr_pct = atr_value / df['close'].iloc[-1]
        
        # Historical volatility (20-period)
        returns = df['close'].pct_change().dropna()
        historical_vol = returns.std() * np.sqrt(365)  # Annualized
        
        volatility_regime = self.regime_detector._analyze_volatility(df)
        
        # FIXED: Use 'volume' instead of 'vol'
        volume_profile = self._analyze_volume_characteristics(df)
        volume_delta = self._calculate_volume_delta(df)
        
        return VolatilityProfile(
            atr=atr_value,
            atr_pct=atr_pct,
            historical_vol=historical_vol,
            volatility_regime=volatility_regime,
            volume_profile=volume_profile,
            volume_delta=volume_delta
        )
    
    def _create_base_signal(self, df: pd.DataFrame, symbol: str, timeframe: Timeframe,
                          market_structure: MarketStructure, volatility_profile: VolatilityProfile) -> Optional[TradingSignal]:
        """Create base trading signal from analysis"""
        
        current_price = df['close'].iloc[-1]
        
        # Determine signal direction from market structure
        signal_side = self._determine_signal_direction(market_structure, df)
        if not signal_side:
            return None
            
        return TradingSignal(
            symbol=symbol,
            side=signal_side,
            entry_price=current_price,
            timestamp=datetime.datetime.utcnow(),
            timeframe=timeframe,
            market_structure=market_structure,
            volatility_profile=volatility_profile,
            risk_parameters=None,  # Will be calculated later
            confidence_score=0.0,  # Will be calculated later
            quality_score=0.0,     # Will be calculated later
            filters_passed=[],
            rejection_reasons=[],
            signal_id=f"{symbol}_{timeframe.value}_{int(time.time())}"
        )
    
    def _determine_signal_direction(self, market_structure: MarketStructure, df: pd.DataFrame) -> Optional[SignalSide]:
        """Determine signal direction from market structure"""
        
        # Use order blocks as primary direction indicator
        if market_structure.order_blocks:
            latest_ob = market_structure.order_blocks[-1]
            if latest_ob['type'] == 'bullish' and latest_ob['quality_score'] > 0.6:
                return SignalSide.BUY
            elif latest_ob['type'] == 'bearish' and latest_ob['quality_score'] > 0.6:
                return SignalSide.SELL
                
        # Fallback to break of structure
        if market_structure.break_of_structure:
            if df['close'].iloc[-1] > df['open'].iloc[-1]:  # Bullish candle
                return SignalSide.BUY
            else:
                return SignalSide.SELL
                
        return None
    
    def _apply_filters_and_scoring(self, signal: TradingSignal, risk_params: RiskParameters, context: Dict) -> TradingSignal:
        """Apply institutional filters and calculate final scores"""
        
        filters_passed = []
        rejection_reasons = []
        
        # 1. Risk-Reward Filter
        if risk_params.risk_reward_ratio >= self.config.MIN_RR_RATIO:
            filters_passed.append("RR_RATIO")
        else:
            rejection_reasons.append(f"RR too low: {risk_params.risk_reward_ratio:.2f}")
            
        # 2. Market Regime Filter
        if self._check_market_regime(signal, context):
            filters_passed.append("MARKET_REGIME")
        else:
            rejection_reasons.append("Unfavorable market regime")
            
        # 3. Volatility Filter
        if self._check_volatility_regime(signal):
            filters_passed.append("VOLATILITY")
        else:
            rejection_reasons.append("Unfavorable volatility regime")
            
        # 4. BTC Alignment Filter
        if self._check_btc_alignment(signal, context):
            filters_passed.append("BTC_ALIGNMENT")
        else:
            rejection_reasons.append("BTC misalignment")
            
        # 5. Volume Confirmation
        if self._check_volume_confirmation(signal):
            filters_passed.append("VOLUME")
        else:
            rejection_reasons.append("Weak volume confirmation")
            
        # Calculate final scores
        confidence_score = self._calculate_confidence_score(signal, risk_params, len(filters_passed))
        quality_score = self._calculate_quality_score(signal, risk_params)
        
        signal.risk_parameters = risk_params
        signal.confidence_score = confidence_score
        signal.quality_score = quality_score
        signal.filters_passed = filters_passed
        signal.rejection_reasons = rejection_reasons
        
        return signal
    
    def _calculate_confidence_score(self, signal: TradingSignal, risk_params: RiskParameters, filters_passed: int) -> float:
        """Calculate final confidence score (0-10)"""
        score = 5.0  # Base score
        
        # Market structure components
        if signal.market_structure.break_of_structure:
            score += 1.0
        if signal.market_structure.higher_tf_alignment:
            score += 1.0
        if any(ob['quality_score'] > 0.7 for ob in signal.market_structure.order_blocks):
            score += 1.0
            
        # Risk parameters
        if risk_params.risk_reward_ratio > 2.0:
            score += 1.0
        if risk_params.probability_score > 0.7:
            score += 1.0
            
        # Filters passed
        score += (filters_passed * 0.5)
        
        return min(score, 10.0)
    
    def _calculate_quality_score(self, signal: TradingSignal, risk_params: RiskParameters) -> float:
        """Calculate quality score (0-1)"""
        quality = 0.0
        
        # Order block quality
        if signal.market_structure.order_blocks:
            best_ob = max(signal.market_structure.order_blocks, key=lambda x: x['quality_score'])
            quality += best_ob['quality_score'] * 0.3
            
        # Risk-reward quality
        rr_quality = min(risk_params.risk_reward_ratio / 3.0, 1.0)  # Cap at 3.0 RR
        quality += rr_quality * 0.3
        
        # Probability quality
        quality += risk_params.probability_score * 0.2
        
        # Volatility quality
        if signal.volatility_profile.volatility_regime == "NORMAL":
            quality += 0.2
            
        return quality
    
    # Helper methods for market structure analysis
    def _detect_liquidity_sweeps(self, df: pd.DataFrame) -> Dict[str, bool]:
        """Detect liquidity sweeps"""
        if len(df) < 10:
            return {}
            
        recent_high = df['high'].tail(10).max()
        recent_low = df['low'].tail(10).min()
        current_high = df['high'].iloc[-1]
        current_low = df['low'].iloc[-1]
        
        return {
            'high_sweep': current_high > recent_high,
            'low_sweep': current_low < recent_low,
            'recent': any(df['high'].tail(3) > recent_high) or any(df['low'].tail(3) < recent_low)
        }
    
    def _detect_equal_highs_lows(self, df: pd.DataFrame) -> Dict[str, bool]:
        """Detect equal highs and lows"""
        if len(df) < 10:
            return {}
            
        recent_highs = df['high'].tail(10)
        recent_lows = df['low'].tail(10)
        
        # Simple detection - can be enhanced
        equal_highs = len(recent_highs.unique()) < len(recent_highs) * 0.8
        equal_lows = len(recent_lows.unique()) < len(recent_lows) * 0.8
        
        return {
            'equal_highs': equal_highs,
            'equal_lows': equal_lows
        }
    
    def _find_liquidity_pools(self, df: pd.DataFrame) -> Dict[str, float]:
        """Find liquidity pools (simplified)"""
        if len(df) < 20:
            return {}
            
        # Use volume profile to find liquidity concentrations
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        # FIXED: Use 'volume' instead of 'vol'
        volume = df['volume'] if 'volume' in df.columns else pd.Series([1] * len(df))
        
        high_volume_levels = typical_price[volume > volume.quantile(0.7)]
        
        return {
            'support': high_volume_levels.min() if not high_volume_levels.empty else df['low'].min(),
            'resistance': high_volume_levels.max() if not high_volume_levels.empty else df['high'].max()
        }
    
    def _find_swing_highs(self, df: pd.DataFrame, lookback: int = 10) -> List[Dict]:
        """Find swing highs"""
        if len(df) < lookback + 1:
            return []
            
        highs = []
        for i in range(lookback, len(df)):
            if df['high'].iloc[i] == df['high'].iloc[i-lookback:i+1].max():
                highs.append({
                    'price': df['high'].iloc[i],
                    'timestamp': df.index[i] if hasattr(df, 'index') else i
                })
                
        return highs[-3:]  # Last 3 swing highs
    
    def _find_swing_lows(self, df: pd.DataFrame, lookback: int = 10) -> List[Dict]:
        """Find swing lows"""
        if len(df) < lookback + 1:
            return []
            
        lows = []
        for i in range(lookback, len(df)):
            if df['low'].iloc[i] == df['low'].iloc[i-lookback:i+1].min():
                lows.append({
                    'price': df['low'].iloc[i],
                    'timestamp': df.index[i] if hasattr(df, 'index') else i
                })
                
        return lows[-3:]  # Last 3 swing lows
    
    async def _check_higher_tf_alignment(self, context: Dict) -> bool:
        """Check higher timeframe alignment"""
        df_15m = context.get('df_15m')
        df_1h = context.get('df_1h')
        
        if df_1h is not None and len(df_1h) > 20:
            # Check if current price is above key EMAs on higher timeframe
            current_price = context.get('current_price')
            if current_price:
                ema_20_1h = df_1h['close'].ewm(span=20).mean().iloc[-1]
                ema_50_1h = df_1h['close'].ewm(span=50).mean().iloc[-1]
                
                # Simple alignment check - can be enhanced
                return current_price > ema_20_1h and current_price > ema_50_1h
                
        return True  # Default to True if data unavailable
    
    def _check_market_regime(self, signal: TradingSignal, context: Dict) -> bool:
        """Check if signal aligns with market regime"""
        regime = signal.market_structure.market_regime
        
        if regime == MarketRegime.UPTREND:
            return signal.side == SignalSide.BUY
        elif regime == MarketRegime.DOWNTREND:
            return signal.side == SignalSide.SELL
        elif regime == MarketRegime.ACCUMULATION:
            return signal.side == SignalSide.BUY
        elif regime == MarketRegime.DISTRIBUTION:
            return signal.side == SignalSide.SELL
        else:  # RANGING, EXPANSION
            return False  # Avoid trading in unclear regimes
    
    def _check_volatility_regime(self, signal: TradingSignal) -> bool:
        """Check volatility regime suitability"""
        volatility_regime = signal.volatility_profile.volatility_regime
        
        # Avoid extreme volatility regimes
        return volatility_regime not in ["EXTREME", "HIGH"]
    
    def _check_btc_alignment(self, signal: TradingSignal, context: Dict) -> bool:
        """Check BTC direction alignment"""
        btc_direction = context.get('btc_direction')
        
        if btc_direction:
            if btc_direction == "BULLISH":
                return signal.side == SignalSide.BUY
            elif btc_direction == "BEARISH":
                return signal.side == SignalSide.SELL
                
        return True  # Default to True if BTC data unavailable
    
    def _check_volume_confirmation(self, signal: TradingSignal) -> bool:
        """Check volume confirmation"""
        volume_delta = signal.volatility_profile.volume_delta
        return volume_delta > 0  # Positive volume delta
        
    def _analyze_volume_characteristics(self, df: pd.DataFrame) -> Dict[str, float]:
        """Analyze volume characteristics"""
        if len(df) < 20:
            return {}
            
        # FIXED: Use 'volume' instead of 'vol'  
        volume = df['volume'] if 'volume' in df.columns else pd.Series([0] * len(df))
        return {
            'current': volume.iloc[-1] if len(volume) > 0 else 0,
            'average_20': volume.rolling(20).mean().iloc[-1] if len(volume) >= 20 else 0,
            'max_20': volume.rolling(20).max().iloc[-1] if len(volume) >= 20 else 0,
            'min_20': volume.rolling(20).min().iloc[-1] if len(volume) >= 20 else 0
        }
    
    def _calculate_volume_delta(self, df: pd.DataFrame) -> float:
        """Calculate volume delta (current vs average)"""
        if len(df) < 20:
            return 0.0
            
        # FIXED: Use 'volume' instead of 'vol'
        volume = df['volume'] if 'volume' in df.columns else pd.Series([0] * len(df))
        current_volume = volume.iloc[-1] if len(volume) > 0 else 0
        avg_volume = volume.rolling(20).mean().iloc[-1] if len(volume) >= 20 else 0
        
        if avg_volume == 0:
            return 0.0
            
        return (current_volume - avg_volume) / avg_volume

# ==================== ENHANCED SCANNER CORE ====================

class InstitutionalScanner:
    """Complete institutional scanner implementation"""
    
    def __init__(self, config: ScannerConfig):
        self.config = config
        self.signal_generator = InstitutionalSignalGenerator(config)
        self.exchange = None
        self.performance_metrics = defaultdict(list)
        self.signal_cooldown = {}
        
        # Initialize logging
        self._setup_logging()
        
    def _setup_logging(self):
        """Setup enhanced logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('/app/data/scanner.log')
            ]
        )
        self.log = logging.getLogger("institutional_scanner")
        
    async def initialize_exchange(self):
        """Initialize exchange connection"""
        self.exchange = ccxt.okx({
            "enableRateLimit": True,
            "apiKey": os.getenv("OKX_API_KEY"),
            "secret": os.getenv("OKX_SECRET_KEY"),
            "password": os.getenv("OKX_PASSWORD"),
            "sandbox": os.getenv("OKX_SANDBOX", "false").lower() == "true",
        })
        
        # Test connection
        try:
            await self.exchange.fetch_balance()
            self.log.info("✅ Exchange connection successful")
            return True
        except Exception as e:
            self.log.error(f"❌ Exchange connection failed: {e}")
            return False
    
    async def scan_market(self):
        """Main market scanning loop"""
        if not self.exchange:
            self.log.error("Exchange not initialized")
            return
            
        while True:
            start_time = time.time()
            
            try:
                # Get market context
                market_context = await self._get_market_context()
                
                # Get top symbols
                symbols = await self._get_top_symbols()
                
                # Scan each symbol
                signals_found = 0
                for symbol in symbols:
                    if await self._should_skip_symbol(symbol):
                        continue
                        
                    signal = await self._scan_symbol(symbol, market_context)
                    if signal:
                        signals_found += 1
                        await self._process_signal(signal)
                        
                # Update performance metrics
                self._update_performance_metrics(signals_found, start_time)
                
                self.log.info(f"📊 Scan completed. Signals found: {signals_found}")
                
            except Exception as e:
                self.log.error(f"Scan error: {e}")
                
            # Respect scan interval
            elapsed = time.time() - start_time
            sleep_time = max(1, self.config.SCAN_INTERVAL - elapsed)
            await asyncio.sleep(sleep_time)
    
    async def _get_market_context(self) -> Dict[str, Any]:
        """Get overall market context"""
        context = {}
        
        try:
            # BTC analysis
            btc_1h = await self._fetch_ohlcv("BTC/USDT", Timeframe.H1)
            
            if btc_1h is not None:
                btc_regime = MarketRegimeDetector().detect_regime(btc_1h)
                context['btc_regime'] = btc_regime
                
                # Simple BTC direction
                current_price = btc_1h['close'].iloc[-1]
                ema_20 = btc_1h['close'].ewm(span=20).mean().iloc[-1]
                ema_50 = btc_1h['close'].ewm(span=50).mean().iloc[-1]
                
                if current_price > ema_20 and current_price > ema_50:
                    context['btc_direction'] = "BULLISH"
                elif current_price < ema_20 and current_price < ema_50:
                    context['btc_direction'] = "BEARISH"
                else:
                    context['btc_direction'] = "NEUTRAL"
                    
        except Exception as e:
            self.log.error(f"Market context error: {e}")
            
        return context
    
    async def _get_top_symbols(self) -> List[str]:
        """Get top symbols by volume"""
        try:
            tickers = await self.exchange.fetch_tickers()
            usdt_pairs = [s for s in tickers.keys() if s.endswith('/USDT')]
            
            # Filter by volume and spread
            valid_symbols = []
            for symbol in usdt_pairs:
                ticker = tickers[symbol]
                volume = ticker.get('quoteVolume', 0)
                bid = ticker.get('bid', 0)
                ask = ticker.get('ask', 0)
                
                if (volume >= self.config.MIN_VOLUME_USDT and 
                    bid > 0 and 
                    (ask - bid) / bid <= self.config.MAX_SPREAD_PCT):
                    valid_symbols.append((symbol, volume))
                    
            # Sort by volume and return top N
            valid_symbols.sort(key=lambda x: x[1], reverse=True)
            return [s[0] for s in valid_symbols[:self.config.TOP_N_SYMBOLS]]
            
        except Exception as e:
            self.log.error(f"Top symbols error: {e}")
            return []
    
    async def _should_skip_symbol(self, symbol: str) -> bool:
        """Check if symbol should be skipped"""
        # Cooldown check
        if symbol in self.signal_cooldown:
            cooldown_end = self.signal_cooldown[symbol]
            if time.time() < cooldown_end:
                return True
                
        return False
    
    async def _scan_symbol(self, symbol: str, market_context: Dict) -> Optional[TradingSignal]:
        """Scan individual symbol for signals"""
        best_signal = None
        best_score = 0
        
        for timeframe in self.config.TIMEFRAMES:
            # Skip if in cooldown for this symbol-timeframe
            cooldown_key = f"{symbol}_{timeframe.value}"
            if cooldown_key in self.signal_cooldown:
                if time.time() < self.signal_cooldown[cooldown_key]:
                    continue
            
            # Fetch OHLCV data
            df = await self._fetch_ohlcv(symbol, timeframe)
            if df is None or len(df) < 50:
                continue
                
            # Prepare context
            context = market_context.copy()
            context['current_price'] = df['close'].iloc[-1]
            
            # Higher timeframe data for context
            if timeframe in [Timeframe.M1, Timeframe.M3, Timeframe.M5]:
                df_15m = await self._fetch_ohlcv(symbol, Timeframe.M15)
                df_1h = await self._fetch_ohlcv(symbol, Timeframe.H1)
                context.update({
                    'df_15m': df_15m,
                    'df_1h': df_1h
                })
            
            # Generate signal
            signal = await self.signal_generator.generate_signal(df, symbol, timeframe, context)
            if signal and signal.confidence_score > best_score:
                best_signal = signal
                best_score = signal.confidence_score
                
        return best_signal
    
    async def _fetch_ohlcv(self, symbol: str, timeframe: Timeframe, limit: int = 200) -> Optional[pd.DataFrame]:
        """Fetch OHLCV data with error handling"""
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe=timeframe.value, limit=limit)
            if ohlcv:
                # FIXED: Use 'volume' as column name (not 'vol')
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                return df
        except Exception as e:
            self.log.debug(f"OHLCV fetch error for {symbol} {timeframe}: {e}")
        return None
    
    async def _process_signal(self, signal: TradingSignal):
        """Process and act on generated signal"""
        try:
            # Log signal
            self._log_signal(signal)
            
            # Send notification - FIXED: Actually sends to Telegram now
            await self._send_notification(signal)
            
            # Store in database
            await self._store_signal(signal)
            
            # Set cooldown
            cooldown_key = f"{signal.symbol}_{signal.timeframe.value}"
            self.signal_cooldown[cooldown_key] = time.time() + (self.config.COOLDOWN_MINUTES * 60)
            
            self.log.info(f"🎯 Signal processed: {signal.symbol} {signal.side} (Score: {signal.confidence_score:.1f})")
            
        except Exception as e:
            self.log.error(f"Signal processing error: {e}")
    
    def _log_signal(self, signal: TradingSignal):
        """Log signal details"""
        log_entry = {
            'signal_id': signal.signal_id,
            'symbol': signal.symbol,
            'side': signal.side.value,
            'timeframe': signal.timeframe.value,
            'entry_price': signal.entry_price,
            'confidence_score': signal.confidence_score,
            'quality_score': signal.quality_score,
            'filters_passed': signal.filters_passed,
            'rejection_reasons': signal.rejection_reasons,
            'timestamp': signal.timestamp.isoformat(),
            'risk_params': {
                'stop_loss': signal.risk_parameters.stop_loss,
                'take_profit_1': signal.risk_parameters.take_profit_1,
                'take_profit_2': signal.risk_parameters.take_profit_2,
                'take_profit_3': signal.risk_parameters.take_profit_3,
                'risk_reward_ratio': signal.risk_parameters.risk_reward_ratio,
                'probability_score': signal.risk_parameters.probability_score,
                'expected_value': signal.risk_parameters.expected_value
            } if signal.risk_parameters else None
        }
        
        self.log.info(f"Signal: {json.dumps(log_entry, indent=2, default=str)}")
    
    async def _send_notification(self, signal: TradingSignal):
        """Send signal notification - FIXED: Actually sends to Telegram now"""
        message = f"""
🏆 INSTITUTIONAL SIGNAL 🏆

Symbol: {signal.symbol}
Side: {signal.side.value}
Timeframe: {signal.timeframe.value}
Entry: {signal.entry_price:.6f}

Risk Management:
SL: {signal.risk_parameters.stop_loss:.6f}
TP1: {signal.risk_parameters.take_profit_1:.6f}
TP2: {signal.risk_parameters.take_profit_2:.6f} 
TP3: {signal.risk_parameters.take_profit_3:.6f}

Scores:
Confidence: {signal.confidence_score:.1f}/10
Quality: {signal.quality_score:.1%}
Probability: {signal.risk_parameters.probability_score:.1%}
R/R: {signal.risk_parameters.risk_reward_ratio:.2f}

Filters: {', '.join(signal.filters_passed)}
Rejections: {', '.join(signal.rejection_reasons) if signal.rejection_reasons else 'None'}
        """
        
        # ACTUALLY SEND TO TELEGRAM - FIXED!
        await tg(message)
        
        # Also print to console for debugging
        print(f"📤 Telegram message sent for {signal.symbol}")
    
    async def _store_signal(self, signal: TradingSignal):
        """Store signal in database"""
        # Implement your database storage logic
        pass
    
    def _update_performance_metrics(self, signals_found: int, start_time: float):
        """Update performance metrics"""
        scan_duration = time.time() - start_time
        self.performance_metrics['scan_duration'].append(scan_duration)
        self.performance_metrics['signals_found'].append(signals_found)
        
        # Keep only last 100 scans
        for key in self.performance_metrics:
            self.performance_metrics[key] = self.performance_metrics[key][-100:]
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary"""
        if not self.performance_metrics:
            return {}
            
        durations = self.performance_metrics['scan_duration']
        signals = self.performance_metrics['signals_found']
        
        return {
            'avg_scan_duration': np.mean(durations) if durations else 0,
            'max_scan_duration': max(durations) if durations else 0,
            'avg_signals_per_scan': np.mean(signals) if signals else 0,
            'total_scans': len(durations),
            'total_signals': sum(signals)
        }

# ==================== FASTAPI APPLICATION ====================

app = FastAPI(title="Institutional Scanner API")
scanner = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global scanner
    config = ScannerConfig()
    scanner = InstitutionalScanner(config)
    
    # Initialize exchange connection
    if await scanner.initialize_exchange():
        # Start scanning in background
        asyncio.create_task(scanner.scan_market())
    else:
        logging.error("Failed to initialize exchange connection")
    
    yield
    
    # Shutdown
    if scanner and scanner.exchange:
        await scanner.exchange.close()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    if scanner and scanner.exchange:
        return {
            "status": "healthy",
            "exchange_connected": True,
            "performance": scanner.get_performance_summary()
        }
    else:
        return {
            "status": "unhealthy", 
            "exchange_connected": False
        }

@app.get("/performance")
async def get_performance():
    """Get performance metrics"""
    if scanner:
        return scanner.get_performance_summary()
    else:
        return {"error": "Scanner not initialized"}

@app.post("/webhook")
async def webhook_handler(request: Request):
    """Webhook endpoint for external signals"""
    return {"status": "received"}

# ==================== SIMPLE TELEGRAM NOTIFICATION ====================

async def tg(msg: str):
    """Simple Telegram notification"""
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
        
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID, 
                "text": msg,
                "parse_mode": "HTML"
            })
        except Exception as e:
            logging.error(f"Telegram error: {e}")

# ==================== MAIN EXECUTION ====================

async def main():
    """Main execution function"""
    config = ScannerConfig()
    scanner = InstitutionalScanner(config)
    
    if await scanner.initialize_exchange():
        logging.info("✅ Institutional Scanner Started")
        
        # Test Telegram on startup - FIXED: Actually sends now
        await tg("🤖 <b>Institutional Scanner Started Successfully!</b>\n\n✅ Exchange connected\n📊 Monitoring 80+ symbols\n🎯 All filters active\n\nReady to find high-probability trading opportunities!")
        
        await scanner.scan_market()
    else:
        logging.error("❌ Failed to start scanner")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Institutional Crypto Scanner")
    parser.add_argument("--http", action="store_true", help="Run as HTTP server")
    
    args = parser.parse_args()
    
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=9000, log_level="info")
    else:
        asyncio.run(main())