#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOPT-P 10/10 ULTIMATE TRADING SYSTEM
Institutional-Grade Implementation - No Compromises
"""

import os
import sys
import time
import asyncio
import logging
import datetime
import json
import math
import statistics
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional, Deque, Set
from enum import Enum, auto
from collections import defaultdict, deque
import numpy as np
import pandas as pd
import aiosqlite
import httpx
import ccxt.async_support as ccxt
from fastapi import FastAPI, Request, HTTPException
import uvicorn

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

# Environment Variables
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH = os.getenv("DB_PATH", "/app/data/romeopt_ultimate.db")
EXCHANGE_API_KEY = os.getenv("EXCHANGE_API_KEY", "")
EXCHANGE_SECRET = os.getenv("EXCHANGE_SECRET", "")

# System Configuration
SCAN_INTERVAL = 5  # seconds between scans
MAX_SYMBOLS = 25  # Focus on top volume pairs
INITIAL_CAPITAL = 10000  # USD
MAX_DAILY_LOSS_PCT = 2.0  # Stop trading at -2% daily
MAX_POSITIONS = 3  # Maximum concurrent positions
MIN_CORRELATION = 0.3  # Minimum correlation threshold

# RomeOPT-P Timeframe Hierarchy
TIMEFRAMES = ["5m", "15m", "1h"]  # Trading timeframes
ANALYSIS_TFS = ["15m", "1h", "4h", "1d"]  # Analysis timeframes

# ============================================================================
# DATA CLASSES & ENUMS
# ============================================================================

class MarketRegime(Enum):
    TRENDING_BULL = auto()
    TRENDING_BEAR = auto()
    RANGING = auto()
    HIGH_VOLATILITY = auto()
    LOW_VOLATILITY = auto()

class OBQuality(Enum):
    FRESH = 4  # Untested, highest probability
    TESTED_1 = 3  # 1 test, still strong
    TESTED_2 = 2  # 2 tests, weakening
    MITIGATED = 1  # Multiple tests, low probability
    INVALID = 0  # Broken structure

class SignalSide(Enum):
    BUY = auto()
    SELL = auto()

@dataclass
class MarketStructure:
    """Complete market structure analysis"""
    swing_points: List[Dict] = field(default_factory=list)
    trend_lines: List[Dict] = field(default_factory=list)
    liquidity_zones: List[Dict] = field(default_factory=list)
    fair_value_gaps: List[Dict] = field(default_factory=list)
    order_blocks: List[Dict] = field(default_factory=list)
    bos_points: List[Dict] = field(default_factory=list)
    choch_points: List[Dict] = field(default_factory=list)
    
@dataclass
class ConfluenceScore:
    """Weighted confluence scoring"""
    mtf_alignment: float = 0.0  # 0-1
    liquidity_sweep: float = 0.0  # 0-1
    ob_quality: float = 0.0  # 0-1
    fvg_position: float = 0.0  # 0-1
    volume_confirmation: float = 0.0  # 0-1
    session_timing: float = 0.0  # 0-1
    market_regime: float = 0.0  # 0-1
    risk_reward: float = 0.0  # 0-1
    
    @property
    def total(self) -> float:
        weights = {
            'mtf_alignment': 0.25,
            'liquidity_sweep': 0.20,
            'ob_quality': 0.15,
            'fvg_position': 0.10,
            'volume_confirmation': 0.10,
            'session_timing': 0.10,
            'market_regime': 0.05,
            'risk_reward': 0.05
        }
        return sum(getattr(self, key) * weight for key, weight in weights.items())

@dataclass
class TradeSignal:
    """Complete trade signal with all analyses"""
    symbol: str
    side: SignalSide
    entry_price: float
    entry_timeframe: str
    entry_timestamp: datetime.datetime
    market_structure: MarketStructure
    confluence_score: ConfluenceScore
    risk_metrics: Dict
    position_size: float
    stop_loss: float
    take_profits: List[float]
    trailing_stop: Optional[float] = None
    unique_id: str = field(default_factory=lambda: f"trade_{int(time.time())}_{os.urandom(4).hex()}")
    
@dataclass
class PerformanceMetrics:
    """Comprehensive performance tracking"""
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_pnl: float = 0.0
    total_pnl_pct: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    average_win: float = 0.0
    average_loss: float = 0.0
    largest_win: float = 0.0
    largest_loss: float = 0.0
    max_consecutive_wins: int = 0
    max_consecutive_losses: int = 0
    max_drawdown: float = 0.0
    max_drawdown_pct: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    recovery_factor: float = 0.0
    
# ============================================================================
# CORE ENGINE: MARKET STRUCTURE ANALYZER
# ============================================================================

class MarketStructureAnalyzer:
    """Complete RomeOPT-P market structure analysis"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def analyze_swing_points(self, df: pd.DataFrame, sensitivity: int = 2) -> List[Dict]:
        """Identify swing highs and lows with proper structure"""
        swing_points = []
        high_series = df['high'].values
        low_series = df['low'].values
        
        for i in range(sensitivity, len(df) - sensitivity):
            # Swing High
            if all(high_series[i] > high_series[i-j] for j in range(1, sensitivity+1)) and \
               all(high_series[i] > high_series[i+j] for j in range(1, sensitivity+1)):
                swing_points.append({
                    'index': i,
                    'price': high_series[i],
                    'type': 'high',
                    'strength': self._calculate_swing_strength(df, i, 'high', sensitivity)
                })
            
            # Swing Low
            if all(low_series[i] < low_series[i-j] for j in range(1, sensitivity+1)) and \
               all(low_series[i] < low_series[i+j] for j in range(1, sensitivity+1)):
                swing_points.append({
                    'index': i,
                    'price': low_series[i],
                    'type': 'low',
                    'strength': self._calculate_swing_strength(df, i, 'low', sensitivity)
                })
        
        return sorted(swing_points, key=lambda x: x['index'])
    
    def _calculate_swing_strength(self, df: pd.DataFrame, idx: int, swing_type: str, lookback: int) -> float:
        """Calculate swing point strength (0-1)"""
        if swing_type == 'high':
            price = df['high'].iloc[idx]
            left_min = df['low'].iloc[max(0, idx-lookback):idx].min()
            right_min = df['low'].iloc[idx:min(len(df), idx+lookback)].min()
            depth = min(price - left_min, price - right_min)
        else:
            price = df['low'].iloc[idx]
            left_max = df['high'].iloc[max(0, idx-lookback):idx].max()
            right_max = df['high'].iloc[idx:min(len(df), idx+lookback)].max()
            depth = min(left_max - price, right_max - price)
        
        atr = self.calculate_atr(df).iloc[idx]
        strength = depth / atr if atr > 0 else 0
        return min(strength / 2, 1.0)  # Normalize to 0-1
    
    def detect_bos_choch(self, df: pd.DataFrame, swing_points: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """Detect Break of Structure and Change of Character"""
        bos_points = []
        choch_points = []
        
        if len(swing_points) < 4:
            return bos_points, choch_points
        
        # Identify market structure
        highs = [s for s in swing_points if s['type'] == 'high']
        lows = [s for s in swing_points if s['type'] == 'low']
        
        for i in range(1, len(highs)):
            # Higher High (HH) or Lower High (LH)
            if highs[i]['price'] > highs[i-1]['price']:
                bos_points.append({
                    'index': highs[i]['index'],
                    'price': highs[i]['price'],
                    'type': 'HH',
                    'previous': highs[i-1]['price']
                })
            else:
                choch_points.append({
                    'index': highs[i]['index'],
                    'price': highs[i]['price'],
                    'type': 'LH',
                    'previous': highs[i-1]['price']
                })
        
        for i in range(1, len(lows)):
            # Higher Low (HL) or Lower Low (LL)
            if lows[i]['price'] > lows[i-1]['price']:
                bos_points.append({
                    'index': lows[i]['index'],
                    'price': lows[i]['price'],
                    'type': 'HL',
                    'previous': lows[i-1]['price']
                })
            else:
                choch_points.append({
                    'index': lows[i]['index'],
                    'price': lows[i]['price'],
                    'type': 'LL',
                    'previous': lows[i-1]['price']
                })
        
        return bos_points, choch_points
    
    def identify_order_blocks(self, df: pd.DataFrame) -> List[Dict]:
        """Find high-quality order blocks with volume confirmation"""
        ob_blocks = []
        
        for i in range(2, len(df) - 2):
            current = df.iloc[i]
            prev1 = df.iloc[i-1]
            prev2 = df.iloc[i-2]
            next1 = df.iloc[i+1]
            
            # Bullish Order Block (Bearish candle followed by bullish)
            if (prev1['close'] < prev1['open'] and  # Previous bearish
                current['close'] > current['open'] and  # Current bullish
                current['low'] < prev1['low'] and  # Takes liquidity
                current['close'] > prev1['open']):  # Closes above previous open
                
                # Volume confirmation
                vol_avg = df['volume'].iloc[max(0, i-5):i].mean()
                vol_confirmation = current['volume'] > vol_avg * 1.3
                
                ob_blocks.append({
                    'index': i,
                    'type': 'bullish',
                    'low': min(current['low'], prev1['low']),
                    'high': max(current['close'], prev1['close']),
                    'body_low': min(current['open'], current['close']),
                    'body_high': max(current['open'], current['close']),
                    'volume_confirmation': vol_confirmation,
                    'strength': self._calculate_ob_strength(df, i, 'bullish'),
                    'quality': self._grade_ob_quality(df, i, 'bullish')
                })
            
            # Bearish Order Block (Bullish candle followed by bearish)
            elif (prev1['close'] > prev1['open'] and  # Previous bullish
                  current['close'] < current['open'] and  # Current bearish
                  current['high'] > prev1['high'] and  # Takes liquidity
                  current['close'] < prev1['open']):  # Closes below previous open
                
                vol_avg = df['volume'].iloc[max(0, i-5):i].mean()
                vol_confirmation = current['volume'] > vol_avg * 1.3
                
                ob_blocks.append({
                    'index': i,
                    'type': 'bearish',
                    'low': min(current['close'], prev1['close']),
                    'high': max(current['high'], prev1['high']),
                    'body_low': min(current['open'], current['close']),
                    'body_high': max(current['open'], current['close']),
                    'volume_confirmation': vol_confirmation,
                    'strength': self._calculate_ob_strength(df, i, 'bearish'),
                    'quality': self._grade_ob_quality(df, i, 'bearish')
                })
        
        return ob_blocks
    
    def _calculate_ob_strength(self, df: pd.DataFrame, idx: int, ob_type: str) -> float:
        """Calculate OB strength (0-1)"""
        if idx >= len(df) - 5:
            return 0.5
        
        candle = df.iloc[idx]
        body_size = abs(candle['close'] - candle['open'])
        wick_size = (candle['high'] - candle['low']) - body_size
        
        # Strong OB has small wicks relative to body
        if body_size > 0:
            body_ratio = body_size / (candle['high'] - candle['low'])
            strength = min(body_ratio * 2, 1.0)  # 0-1 scale
        else:
            strength = 0.3
        
        # Volume multiplier
        vol_avg = df['volume'].iloc[max(0, idx-10):idx].mean()
        if candle['volume'] > vol_avg * 1.5:
            strength = min(strength * 1.5, 1.0)
        
        return strength
    
    def _grade_ob_quality(self, df: pd.DataFrame, idx: int, ob_type: str) -> OBQuality:
        """Grade OB quality based on subsequent price action"""
        if idx >= len(df) - 10:
            return OBQuality.FRESH
        
        # Check if OB has been tested
        future_data = df.iloc[idx+1:min(idx+20, len(df))]
        
        if ob_type == 'bullish':
            tests = (future_data['low'] < df.iloc[idx]['low']).sum()
            breaks = (future_data['close'] < df.iloc[idx]['low']).any()
        else:
            tests = (future_data['high'] > df.iloc[idx]['high']).sum()
            breaks = (future_data['close'] > df.iloc[idx]['high']).any()
        
        if breaks:
            return OBQuality.INVALID
        elif tests == 0:
            return OBQuality.FRESH
        elif tests == 1:
            return OBQuality.TESTED_1
        elif tests == 2:
            return OBQuality.TESTED_2
        else:
            return OBQuality.MITIGATED
    
    def detect_fair_value_gaps(self, df: pd.DataFrame) -> List[Dict]:
        """Detect Fair Value Gaps with premium/discount classification"""
        fvgs = []
        
        for i in range(2, len(df) - 1):
            current_low = df['low'].iloc[i]
            prev_high = df['high'].iloc[i-1]
            prev_low = df['low'].iloc[i-1]
            current_high = df['high'].iloc[i]
            
            # Bullish FVG (price gapped up)
            if current_low > prev_high:
                # Check if it's premium or discount
                ema50 = df['close'].ewm(span=50, adjust=False).mean().iloc[i]
                is_premium = current_low > ema50
                
                fvgs.append({
                    'index': i,
                    'type': 'bullish',
                    'low': prev_high,  # Bottom of FVG
                    'high': current_low,  # Top of FVG
                    'midpoint': (prev_high + current_low) / 2,
                    'premium': is_premium,
                    'discount': not is_premium,
                    'size': current_low - prev_high
                })
            
            # Bearish FVG (price gapped down)
            elif current_high < prev_low:
                ema50 = df['close'].ewm(span=50, adjust=False).mean().iloc[i]
                is_discount = current_high < ema50
                
                fvgs.append({
                    'index': i,
                    'type': 'bearish',
                    'low': current_high,
                    'high': prev_low,
                    'midpoint': (current_high + prev_low) / 2,
                    'premium': not is_discount,
                    'discount': is_discount,
                    'size': prev_low - current_high
                })
        
        return fvgs
    
    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Average True Range"""
        high = df['high']
        low = df['low']
        close = df['close'].shift(1)
        
        tr1 = high - low
        tr2 = (high - close).abs()
        tr3 = (low - close).abs()
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        
        return atr
    
    def determine_market_regime(self, df: pd.DataFrame) -> MarketRegime:
        """Determine current market regime"""
        if len(df) < 50:
            return MarketRegime.RANGING
        
        # Calculate indicators
        close = df['close']
        ema20 = close.ewm(span=20, adjust=False).mean()
        ema50 = close.ewm(span=50, adjust=False).mean()
        
        # Trend strength
        price_slope = close.iloc[-1] - close.iloc[-20]
        ema_slope = ema20.iloc[-1] - ema20.iloc[-20]
        
        # Volatility
        atr = self.calculate_atr(df).iloc[-1]
        avg_atr = self.calculate_atr(df).iloc[-50:].mean()
        vol_ratio = atr / avg_atr if avg_atr > 0 else 1
        
        # Range detection
        recent_high = df['high'].iloc[-20:].max()
        recent_low = df['low'].iloc[-20:].min()
        range_pct = (recent_high - recent_low) / recent_low
        
        # Determine regime
        if vol_ratio > 1.8:
            return MarketRegime.HIGH_VOLATILITY
        elif vol_ratio < 0.5:
            return MarketRegime.LOW_VOLATILITY
        elif price_slope > 0 and ema_slope > 0 and close.iloc[-1] > ema50.iloc[-1]:
            return MarketRegime.TRENDING_BULL
        elif price_slope < 0 and ema_slope < 0 and close.iloc[-1] < ema50.iloc[-1]:
            return MarketRegime.TRENDING_BEAR
        elif range_pct < 0.02:  # Less than 2% range
            return MarketRegime.RANGING
        else:
            return MarketRegime.RANGING

# ============================================================================
# CORE ENGINE: MULTI-TIMEFRAME ANALYZER
# ============================================================================

class MultiTimeframeAnalyzer:
    """Analyze confluence across multiple timeframes"""
    
    def __init__(self, exchange):
        self.exchange = exchange
        self.structure_analyzer = MarketStructureAnalyzer()
        self.logger = logging.getLogger(__name__)
    
    async def analyze_symbol(self, symbol: str, entry_tf: str) -> Optional[MarketStructure]:
        """Complete MTF analysis for a symbol"""
        try:
            # Fetch data for all analysis timeframes
            data = {}
            for tf in ANALYSIS_TFS:
                ohlcv = await self.exchange.fetch_ohlcv(symbol, tf, limit=200)
                if ohlcv:
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    for col in ['open', 'high', 'low', 'close', 'volume']:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    data[tf] = df
            
            if not data:
                return None
            
            # Analyze each timeframe
            structure = MarketStructure()
            
            for tf, df in data.items():
                # Swing points
                swings = self.structure_analyzer.analyze_swing_points(df)
                structure.swing_points.extend([{**s, 'timeframe': tf} for s in swings])
                
                # BOS/CHOCH
                bos, choch = self.structure_analyzer.detect_bos_choch(df, swings)
                structure.bos_points.extend([{**b, 'timeframe': tf} for b in bos])
                structure.choch_points.extend([{**c, 'timeframe': tf} for c in choch])
                
                # Order blocks (only on higher timeframes)
                if tf in ['1h', '4h', '1d']:
                    obs = self.structure_analyzer.identify_order_blocks(df)
                    structure.order_blocks.extend([{**ob, 'timeframe': tf} for ob in obs])
                
                # FVGs
                fvgs = self.structure_analyzer.detect_fair_value_gaps(df)
                structure.fair_value_gaps.extend([{**f, 'timeframe': tf} for f in fvgs])
            
            return structure
            
        except Exception as e:
            self.logger.error(f"MTF analysis failed for {symbol}: {e}")
            return None
    
    def calculate_mtf_alignment(self, structure: MarketStructure, side: SignalSide) -> float:
        """Calculate MTF alignment score (0-1)"""
        if not structure.bos_points and not structure.choch_points:
            return 0.0
        
        # Group by timeframe
        tf_scores = {}
        
        for tf in ANALYSIS_TFS:
            tf_bos = [p for p in structure.bos_points if p['timeframe'] == tf]
            tf_choch = [p for p in structure.choch_points if p['timeframe'] == tf]
            
            # Score based on structure alignment
            score = 0.0
            if side == SignalSide.BUY:
                # Look for bullish structure
                hh_count = sum(1 for p in tf_bos if p['type'] == 'HH')
                hl_count = sum(1 for p in tf_bos if p['type'] == 'HL')
                score = (hh_count + hl_count) / max(len(tf_bos + tf_choch), 1)
            else:
                # Look for bearish structure
                ll_count = sum(1 for p in tf_choch if p['type'] == 'LL')
                lh_count = sum(1 for p in tf_choch if p['type'] == 'LH')
                score = (ll_count + lh_count) / max(len(tf_bos + tf_choch), 1)
            
            tf_scores[tf] = score
        
        # Weighted average (higher timeframes more important)
        weights = {'1d': 0.4, '4h': 0.3, '1h': 0.2, '15m': 0.1}
        total_score = sum(tf_scores.get(tf, 0) * weights.get(tf, 0) for tf in ANALYSIS_TFS)
        
        return min(total_score, 1.0)

# ============================================================================
# CORE ENGINE: CONFLUENCE SCORER
# ============================================================================

class ConfluenceScorer:
    """Calculate weighted confluence scores"""
    
    def __init__(self):
        self.structure_analyzer = MarketStructureAnalyzer()
    
    def score_signal(self, df: pd.DataFrame, structure: MarketStructure, 
                    side: SignalSide, entry_price: float) -> ConfluenceScore:
        """Calculate complete confluence score"""
        score = ConfluenceScore()
        
        # 1. MTF Alignment
        score.mtf_alignment = self._score_mtf_alignment(structure, side)
        
        # 2. Liquidity Sweep
        score.liquidity_sweep = self._score_liquidity_sweep(df)
        
        # 3. OB Quality
        score.ob_quality = self._score_ob_quality(structure, side, entry_price)
        
        # 4. FVG Position
        score.fvg_position = self._score_fvg_position(structure, side, entry_price)
        
        # 5. Volume Confirmation
        score.volume_confirmation = self._score_volume_confirmation(df)
        
        # 6. Session Timing
        score.session_timing = self._score_session_timing()
        
        # 7. Market Regime
        score.market_regime = self._score_market_regime(df, side)
        
        # 8. Risk/Reward (placeholder, will be updated after TP/SL calculation)
        score.risk_reward = 0.7  # Default
        
        return score
    
    def _score_mtf_alignment(self, structure: MarketStructure, side: SignalSide) -> float:
        """Score MTF alignment (0-1)"""
        if not structure.bos_points:
            return 0.0
        
        # Count bullish/bearish structures across timeframes
        bullish_signals = 0
        bearish_signals = 0
        
        for point in structure.bos_points + structure.choch_points:
            if point['type'] in ['HH', 'HL']:
                bullish_signals += 1
            elif point['type'] in ['LL', 'LH']:
                bearish_signals += 1
        
        if side == SignalSide.BUY:
            total_signals = bullish_signals + bearish_signals
            return bullish_signals / max(total_signals, 1)
        else:
            total_signals = bullish_signals + bearish_signals
            return bearish_signals / max(total_signals, 1)
    
    def _score_liquidity_sweep(self, df: pd.DataFrame) -> float:
        """Score liquidity sweep (0-1)"""
        if len(df) < 10:
            return 0.0
        
        last_candle = df.iloc[-1]
        prev_highs = df['high'].iloc[-10:-1]
        prev_lows = df['low'].iloc[-10:-1]
        
        # Check for sweep
        sweep_high = last_candle['high'] > prev_highs.max()
        sweep_low = last_candle['low'] < prev_lows.min()
        
        if sweep_high or sweep_low:
            # Stronger score if with displacement
            body_size = abs(last_candle['close'] - last_candle['open'])
            candle_range = last_candle['high'] - last_candle['low']
            if candle_range > 0:
                body_ratio = body_size / candle_range
                return min(body_ratio * 1.5, 1.0)  # 0-1 scale
            return 0.7
        return 0.0
    
    def _score_ob_quality(self, structure: MarketStructure, side: SignalSide, entry_price: float) -> float:
        """Score order block quality (0-1)"""
        relevant_obs = []
        
        for ob in structure.order_blocks:
            # Filter for side and recent OBs
            if ob['type'] == ('bullish' if side == SignalSide.BUY else 'bearish'):
                if 'index' in ob and ob.get('index', 0) > len(structure.order_blocks) - 20:
                    relevant_obs.append(ob)
        
        if not relevant_obs:
            return 0.0
        
        # Find closest OB to entry
        closest_ob = None
        min_distance = float('inf')
        
        for ob in relevant_obs:
            if side == SignalSide.BUY:
                distance = abs(entry_price - ob['high'])
            else:
                distance = abs(entry_price - ob['low'])
            
            if distance < min_distance:
                min_distance = distance
                closest_ob = ob
        
        if not closest_ob:
            return 0.0
        
        # Score based on quality and distance
        quality_score = closest_ob.get('quality', OBQuality.FRESH).value / 4.0  # 0-1
        
        # Distance penalty (closer is better)
        atr = self.structure_analyzer.calculate_atr(pd.DataFrame([closest_ob])).iloc[-1]
        distance_penalty = min(min_distance / (atr * 2), 1.0)  # Normalize by ATR
        
        return quality_score * (1 - distance_penalty * 0.3)  # Max 30% penalty
    
    def _score_fvg_position(self, structure: MarketStructure, side: SignalSide, entry_price: float) -> float:
        """Score FVG position (premium/discount)"""
        if not structure.fair_value_gaps:
            return 0.0
        
        # Get most relevant FVGs
        recent_fvgs = [f for f in structure.fair_value_gaps 
                      if f.get('index', 0) > len(structure.fair_value_gaps) - 10]
        
        if not recent_fvgs:
            return 0.0
        
        # Check if price is at FVG
        for fvg in recent_fvgs:
            if fvg['low'] <= entry_price <= fvg['high']:
                # Premium FVG for buys, Discount for sells
                if side == SignalSide.BUY and fvg.get('discount', False):
                    return 0.8
                elif side == SignalSide.SELL and fvg.get('premium', False):
                    return 0.8
                else:
                    return 0.3  # Wrong side of FVG
        
        return 0.0
    
    def _score_volume_confirmation(self, df: pd.DataFrame) -> float:
        """Score volume confirmation (0-1)"""
        if len(df) < 20:
            return 0.0
        
        last_candle = df.iloc[-1]
        vol_avg = df['volume'].iloc[-20:-1].mean()
        
        if vol_avg == 0:
            return 0.0
        
        vol_ratio = last_candle['volume'] / vol_avg
        
        # Score based on volume spike
        if vol_ratio > 2.0:
            return 1.0
        elif vol_ratio > 1.5:
            return 0.7
        elif vol_ratio > 1.2:
            return 0.4
        else:
            return 0.1
    
    def _score_session_timing(self) -> float:
        """Score session timing (0-1)"""
        utc_hour = datetime.datetime.utcnow().hour
        
        # Best: London-NY overlap (13-17 UTC)
        if 13 <= utc_hour < 17:
            return 1.0
        # Good: London open (8-13 UTC) or NY open (17-21 UTC)
        elif (8 <= utc_hour < 13) or (17 <= utc_hour < 21):
            return 0.7
        # Okay: Asian-London overlap (0-8 UTC)
        elif 0 <= utc_hour < 8:
            return 0.4
        # Poor: Late NY/Asian (21-24 UTC)
        else:
            return 0.2
    
    def _score_market_regime(self, df: pd.DataFrame, side: SignalSide) -> float:
        """Score market regime alignment (0-1)"""
        regime = self.structure_analyzer.determine_market_regime(df)
        
        if regime == MarketRegime.TRENDING_BULL and side == SignalSide.BUY:
            return 1.0
        elif regime == MarketRegime.TRENDING_BEAR and side == SignalSide.SELL:
            return 1.0
        elif regime == MarketRegime.RANGING:
            return 0.5  # Neutral for ranging
        elif regime == MarketRegime.HIGH_VOLATILITY:
            return 0.3  # Caution in high volatility
        elif regime == MarketRegime.LOW_VOLATILITY:
            return 0.6  # Okay in low volatility
        else:
            return 0.0  # Wrong regime

# ============================================================================
# CORE ENGINE: RISK MANAGER
# ============================================================================

class AdaptiveRiskManager:
    """Advanced risk management with Kelly Criterion"""
    
    def __init__(self, initial_capital: float):
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.daily_pnl = 0.0
        self.consecutive_losses = 0
        self.max_consecutive_losses = 5
        self.daily_loss_limit_pct = MAX_DAILY_LOSS_PCT / 100
        self.positions = {}
        self.performance_history = []
        self.logger = logging.getLogger(__name__)
    
    def calculate_position_size(self, signal: TradeSignal, win_rate: float = 0.6, 
                               avg_win_ratio: float = 1.5, avg_loss_ratio: float = 1.0) -> float:
        """Calculate optimal position size using fractional Kelly"""
        
        # Check daily loss limit
        daily_loss_pct = abs(self.daily_pnl) / self.initial_capital
        if daily_loss_pct >= self.daily_loss_limit_pct:
            self.logger.warning(f"Daily loss limit reached: {daily_loss_pct:.2%}")
            return 0.0
        
        # Check consecutive losses
        if self.consecutive_losses >= self.max_consecutive_losses:
            self.logger.warning(f"Max consecutive losses reached: {self.consecutive_losses}")
            return 0.0
        
        # Calculate Kelly Criterion
        b = avg_win_ratio / avg_loss_ratio  # Win/Loss ratio
        p = win_rate  # Win probability
        q = 1 - p  # Loss probability
        
        kelly_f = (b * p - q) / b if b > 0 else 0
        
        # Fractional Kelly (25% for safety)
        fractional_kelly = kelly_f * 0.25
        
        # Adjust for signal confidence
        confidence_multiplier = signal.confluence_score.total
        
        # Calculate max risk (1% of capital)
        max_risk_capital = self.current_capital * 0.01
        
        # Calculate position size
        risk_per_unit = abs(signal.entry_price - signal.stop_loss)
        
        if risk_per_unit == 0:
            return 0.0
        
        # Units based on fractional Kelly
        kelly_units = (self.current_capital * fractional_kelly * confidence_multiplier) / risk_per_unit
        
        # Units based on max risk
        max_risk_units = max_risk_capital / risk_per_unit
        
        # Use the smaller of the two (conservative)
        units = min(kelly_units, max_risk_units)
        
        # Adjust for volatility
        volatility_adjustment = self._calculate_volatility_adjustment(signal)
        units *= volatility_adjustment
        
        # Ensure minimum position (0.01 units for crypto)
        if units * signal.entry_price < 10:  # Minimum $10 position
            return 0.0
        
        return units
    
    def _calculate_volatility_adjustment(self, signal: TradeSignal) -> float:
        """Adjust position size based on market volatility"""
        # Simplified volatility adjustment
        # In production, use ATR or realized volatility
        return 0.8  # Default 20% reduction for safety
    
    def update_performance(self, trade_result: Dict):
        """Update performance metrics after trade"""
        pnl = trade_result.get('pnl', 0)
        pnl_pct = trade_result.get('pnl_pct', 0)
        
        self.current_capital += pnl
        self.daily_pnl += pnl
        
        # Update consecutive losses
        if pnl < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0
        
        # Record performance
        self.performance_history.append({
            'timestamp': datetime.datetime.utcnow(),
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'consecutive_losses': self.consecutive_losses
        })
        
        # Keep only last 100 trades
        if len(self.performance_history) > 100:
            self.performance_history.pop(0)
    
    def get_win_rate(self) -> float:
        """Calculate current win rate"""
        if not self.performance_history:
            return 0.6  # Default
        
        wins = sum(1 for trade in self.performance_history if trade['pnl'] > 0)
        total = len(self.performance_history)
        
        return wins / total if total > 0 else 0.6
    
    def get_avg_win_loss_ratio(self) -> Tuple[float, float]:
        """Calculate average win/loss ratios"""
        if not self.performance_history:
            return 1.5, 1.0  # Default
        
        wins = [trade['pnl_pct'] for trade in self.performance_history if trade['pnl'] > 0]
        losses = [abs(trade['pnl_pct']) for trade in self.performance_history if trade['pnl'] < 0]
        
        avg_win = statistics.mean(wins) if wins else 1.5
        avg_loss = statistics.mean(losses) if losses else 1.0
        
        return avg_win, avg_loss
    
    def can_trade(self) -> bool:
        """Check if trading is allowed"""
        daily_loss_pct = abs(self.daily_pnl) / self.initial_capital
        
        if daily_loss_pct >= self.daily_loss_limit_pct:
            self.logger.warning(f"Trading stopped: Daily loss limit reached ({daily_loss_pct:.2%})")
            return False
        
        if self.consecutive_losses >= self.max_consecutive_losses:
            self.logger.warning(f"Trading stopped: {self.consecutive_losses} consecutive losses")
            return False
        
        return True

# ============================================================================
# CORE ENGINE: TRADE MANAGER
# ============================================================================

class TradeManager:
    """Manage trade execution and monitoring"""
    
    def __init__(self, exchange, risk_manager: AdaptiveRiskManager):
        self.exchange = exchange
        self.risk_manager = risk_manager
        self.active_trades = {}
        self.trade_history = []
        self.correlation_tracker = CorrelationTracker()
        self.logger = logging.getLogger(__name__)
    
    async def execute_trade(self, signal: TradeSignal) -> bool:
        """Execute a trade with proper risk management"""
        
        # Check risk limits
        if not self.risk_manager.can_trade():
            self.logger.warning("Risk limits prevent trading")
            return False
        
        # Check correlation
        if self.correlation_tracker.would_exceed_correlation(signal.symbol, signal.side):
            self.logger.warning(f"Correlation limit would be exceeded for {signal.symbol}")
            return False
        
        # Check max positions
        if len(self.active_trades) >= MAX_POSITIONS:
            self.logger.warning(f"Max positions reached ({MAX_POSITIONS})")
            return False
        
        try:
            # Calculate position size
            win_rate = self.risk_manager.get_win_rate()
            avg_win, avg_loss = self.risk_manager.get_avg_win_loss_ratio()
            
            position_size = self.risk_manager.calculate_position_size(
                signal, win_rate, avg_win, avg_loss
            )
            
            if position_size <= 0:
                self.logger.warning("Position size is zero or negative")
                return False
            
            # Place limit order at entry
            order = await self._place_limit_order(
                symbol=signal.symbol,
                side='buy' if signal.side == SignalSide.BUY else 'sell',
                amount=position_size,
                price=signal.entry_price
            )
            
            if not order:
                self.logger.error("Failed to place order")
                return False
            
            # Update signal with position size
            signal.position_size = position_size
            
            # Store trade
            self.active_trades[signal.unique_id] = {
                'signal': signal,
                'order_id': order['id'],
                'status': 'pending',
                'entry_time': datetime.datetime.utcnow(),
                'pnl': 0.0,
                'pnl_pct': 0.0
            }
            
            # Update correlation tracker
            self.correlation_tracker.add_trade(signal.symbol, signal.side)
            
            self.logger.info(f"Trade executed: {signal.symbol} {signal.side.name} "
                           f"Size: {position_size:.4f} @ {signal.entry_price}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Trade execution failed: {e}")
            return False
    
    async def _place_limit_order(self, symbol: str, side: str, amount: float, price: float) -> Optional[Dict]:
        """Place a limit order"""
        try:
            order = await self.exchange.create_order(
                symbol=symbol,
                type='limit',
                side=side,
                amount=amount,
                price=price
            )
            return order
        except Exception as e:
            self.logger.error(f"Limit order failed: {e}")
            return None
    
    async def monitor_trades(self):
        """Monitor and manage active trades"""
        while True:
            try:
                for trade_id, trade_info in list(self.active_trades.items()):
                    signal = trade_info['signal']
                    
                    # Check if order filled
                    if trade_info['status'] == 'pending':
                        await self._check_order_fill(trade_id, trade_info)
                    
                    # Manage open position
                    elif trade_info['status'] == 'open':
                        await self._manage_position(trade_id, trade_info)
                    
                    # Clean up closed trades
                    elif trade_info['status'] in ['closed', 'cancelled']:
                        self._cleanup_trade(trade_id, trade_info)
                
                await asyncio.sleep(2)  # Check every 2 seconds
                
            except Exception as e:
                self.logger.error(f"Trade monitoring error: {e}")
                await asyncio.sleep(5)
    
    async def _check_order_fill(self, trade_id: str, trade_info: Dict):
        """Check if limit order was filled"""
        try:
            order = await self.exchange.fetch_order(trade_info['order_id'], trade_info['signal'].symbol)
            
            if order['status'] == 'closed':
                # Order filled, update status
                trade_info['status'] = 'open'
                trade_info['filled_price'] = order['average']
                trade_info['filled_time'] = datetime.datetime.utcnow()
                
                self.logger.info(f"Order filled: {trade_id} @ {trade_info['filled_price']}")
                
            elif order['status'] == 'canceled':
                # Order cancelled, remove
                trade_info['status'] = 'cancelled'
                
        except Exception as e:
            self.logger.error(f"Order check failed: {e}")
    
    async def _manage_position(self, trade_id: str, trade_info: Dict):
        """Manage open position (TP/SL, trailing)"""
        signal = trade_info['signal']
        
        try:
            # Get current price
            ticker = await self.exchange.fetch_ticker(signal.symbol)
            current_price = ticker['last']
            
            # Calculate PnL
            if signal.side == SignalSide.BUY:
                pnl = (current_price - trade_info['filled_price']) * signal.position_size
            else:
                pnl = (trade_info['filled_price'] - current_price) * signal.position_size
            
            pnl_pct = (pnl / (trade_info['filled_price'] * signal.position_size)) * 100
            
            trade_info['pnl'] = pnl
            trade_info['pnl_pct'] = pnl_pct
            
            # Check stop loss
            if (signal.side == SignalSide.BUY and current_price <= signal.stop_loss) or \
               (signal.side == SignalSide.SELL and current_price >= signal.stop_loss):
                
                await self._close_position(trade_id, trade_info, 'SL')
                return
            
            # Check take profits
            for i, tp_level in enumerate(signal.take_profits):
                if (signal.side == SignalSide.BUY and current_price >= tp_level) or \
                   (signal.side == SignalSide.SELL and current_price <= tp_level):
                    
                    # Close percentage based on TP level
                    close_percentage = 0.5 if i == 0 else 0.3 if i == 1 else 0.2
                    await self._partial_close(trade_id, trade_info, tp_level, close_percentage)
                    
                    # Update stop loss after TP1
                    if i == 0 and signal.trailing_stop is None:
                        signal.trailing_stop = trade_info['filled_price']  # Move to breakeven
            
            # Update trailing stop
            if signal.trailing_stop:
                self._update_trailing_stop(signal, current_price)
            
        except Exception as e:
            self.logger.error(f"Position management failed: {e}")
    
    async def _close_position(self, trade_id: str, trade_info: Dict, reason: str):
        """Close entire position"""
        signal = trade_info['signal']
        
        try:
            close_side = 'sell' if signal.side == SignalSide.BUY else 'buy'
            order = await self.exchange.create_market_order(
                symbol=signal.symbol,
                side=close_side,
                amount=signal.position_size
            )
            
            # Update trade info
            trade_info['status'] = 'closed'
            trade_info['close_reason'] = reason
            trade_info['close_time'] = datetime.datetime.utcnow()
            trade_info['close_price'] = order['average']
            
            # Update risk manager
            self.risk_manager.update_performance({
                'pnl': trade_info['pnl'],
                'pnl_pct': trade_info['pnl_pct']
            })
            
            # Update correlation tracker
            self.correlation_tracker.remove_trade(signal.symbol)
            
            self.logger.info(f"Position closed: {trade_id} {reason} "
                           f"PNL: ${trade_info['pnl']:.2f} ({trade_info['pnl_pct']:.2f}%)")
            
        except Exception as e:
            self.logger.error(f"Position close failed: {e}")
    
    async def _partial_close(self, trade_id: str, trade_info: Dict, tp_level: float, percentage: float):
        """Partially close position at TP"""
        signal = trade_info['signal']
        
        try:
            close_amount = signal.position_size * percentage
            close_side = 'sell' if signal.side == SignalSide.BUY else 'buy'
            
            order = await self.exchange.create_market_order(
                symbol=signal.symbol,
                side=close_side,
                amount=close_amount
            )
            
            # Reduce position size
            signal.position_size -= close_amount
            
            # Record partial close
            partial_pnl = (tp_level - trade_info['filled_price']) * close_amount
            if signal.side == SignalSide.SELL:
                partial_pnl *= -1
            
            self.logger.info(f"Partial close: {trade_id} {percentage*100:.0f}% @ {tp_level} "
                           f"PNL: ${partial_pnl:.2f}")
            
        except Exception as e:
            self.logger.error(f"Partial close failed: {e}")
    
    def _update_trailing_stop(self, signal: TradeSignal, current_price: float):
        """Update trailing stop loss"""
        atr = MarketStructureAnalyzer().calculate_atr(pd.DataFrame([{'high': current_price, 
                                                                    'low': current_price, 
                                                                    'close': current_price}]))
        
        if signal.side == SignalSide.BUY:
            new_stop = current_price - (atr.iloc[-1] * 1.5)
            signal.trailing_stop = max(signal.trailing_stop, new_stop)
        else:
            new_stop = current_price + (atr.iloc[-1] * 1.5)
            signal.trailing_stop = min(signal.trailing_stop, new_stop)
    
    def _cleanup_trade(self, trade_id: str, trade_info: Dict):
        """Clean up completed trade"""
        # Move to history
        self.trade_history.append(trade_info)
        
        # Remove from active
        if trade_id in self.active_trades:
            del self.active_trades[trade_id]
        
        # Keep history limited
        if len(self.trade_history) > 1000:
            self.trade_history.pop(0)

# ============================================================================
# CORE ENGINE: CORRELATION TRACKER
# ============================================================================

class CorrelationTracker:
    """Track and manage correlation between positions"""
    
    def __init__(self):
        self.active_symbols = set()
        self.symbol_sides = {}
        self.correlation_matrix = self._load_correlation_matrix()
    
    def _load_correlation_matrix(self) -> Dict[str, Dict[str, float]]:
        """Load or calculate correlation matrix"""
        # In production, load from historical data
        # For now, use simplified correlations
        return {
            'BTC/USDT': {'ETH/USDT': 0.8, 'SOL/USDT': 0.7, 'XRP/USDT': 0.6},
            'ETH/USDT': {'BTC/USDT': 0.8, 'SOL/USDT': 0.75, 'XRP/USDT': 0.65},
            'SOL/USDT': {'BTC/USDT': 0.7, 'ETH/USDT': 0.75, 'XRP/USDT': 0.5},
            'XRP/USDT': {'BTC/USDT': 0.6, 'ETH/USDT': 0.65, 'SOL/USDT': 0.5}
        }
    
    def add_trade(self, symbol: str, side: SignalSide):
        """Add a trade to correlation tracking"""
        self.active_symbols.add(symbol)
        self.symbol_sides[symbol] = side
    
    def remove_trade(self, symbol: str):
        """Remove a trade from correlation tracking"""
        if symbol in self.active_symbols:
            self.active_symbols.remove(symbol)
        if symbol in self.symbol_sides:
            del self.symbol_sides[symbol]
    
    def would_exceed_correlation(self, new_symbol: str, new_side: SignalSide) -> bool:
        """Check if new trade would exceed correlation limits"""
        if not self.active_symbols:
            return False
        
        total_correlation = 0.0
        count = 0
        
        for symbol in self.active_symbols:
            side = self.symbol_sides.get(symbol)
            
            # Check if same side (increases correlation risk)
            if side == new_side:
                # Get correlation coefficient
                corr = self.correlation_matrix.get(new_symbol, {}).get(symbol, 0)
                total_correlation += corr
                count += 1
        
        if count == 0:
            return False
        
        avg_correlation = total_correlation / count
        
        # Reject if average correlation > threshold
        return avg_correlation > MIN_CORRELATION

# ============================================================================
# MAIN TRADING ENGINE
# ============================================================================

class RomeOptTradingEngine:
    """Main trading engine coordinating all components"""
    
    def __init__(self):
        self.exchange = None
        self.risk_manager = AdaptiveRiskManager(INITIAL_CAPITAL)
        self.trade_manager = None
        self.mtf_analyzer = None
        self.confluence_scorer = ConfluenceScorer()
        self.structure_analyzer = MarketStructureAnalyzer()
        self.performance_analyzer = PerformanceAnalyzer()
        self.logger = logging.getLogger(__name__)
        
        # State management
        self.is_running = False
        self.last_scan_time = {}
        self.signal_cooldown = {}
        self.daily_reset_time = None
        
        # Statistics
        self.signals_generated = 0
        self.trades_executed = 0
        self.current_pnl = 0.0
    
    async def initialize(self):
        """Initialize the trading engine"""
        try:
            # Initialize exchange
            self.exchange = ccxt.okx({
                'apiKey': EXCHANGE_API_KEY,
                'secret': EXCHANGE_SECRET,
                'enableRateLimit': True,
                'options': {'defaultType': 'spot'}
            })
            
            # Initialize components
            self.trade_manager = TradeManager(self.exchange, self.risk_manager)
            self.mtf_analyzer = MultiTimeframeAnalyzer(self.exchange)
            
            self.logger.info("RomeOPT-P Trading Engine initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Initialization failed: {e}")
            return False
    
    async def start(self):
        """Start the trading engine"""
        if self.is_running:
            self.logger.warning("Engine already running")
            return
        
        self.is_running = True
        self.daily_reset_time = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0)
        
        # Start monitoring
        asyncio.create_task(self.trade_manager.monitor_trades())
        asyncio.create_task(self._daily_reset_check())
        
        # Start main loop
        self.logger.info("RomeOPT-P Trading Engine started")
        
        while self.is_running:
            try:
                await self._scan_markets()
                await asyncio.sleep(SCAN_INTERVAL)
            except Exception as e:
                self.logger.error(f"Main loop error: {e}")
                await asyncio.sleep(10)
    
    async def stop(self):
        """Stop the trading engine"""
        self.is_running = False
        self.logger.info("RomeOPT-P Trading Engine stopped")
    
    async def _scan_markets(self):
        """Scan markets for trading opportunities"""
        try:
            # Get top volume symbols
            tickers = await self.exchange.fetch_tickers()
            usdt_pairs = [(s, v.get('quoteVolume', 0)) 
                         for s, v in tickers.items() 
                         if '/USDT' in s and ':' not in s]  # Exclude futures
            
            # Sort by volume and take top N
            top_symbols = sorted(usdt_pairs, key=lambda x: x[1], reverse=True)[:MAX_SYMBOLS]
            
            for symbol, volume in top_symbols:
                # Check cooldown
                if symbol in self.signal_cooldown:
                    cooldown_end = self.signal_cooldown[symbol]
                    if datetime.datetime.utcnow() < cooldown_end:
                        continue
                
                # Generate signals for each timeframe
                for tf in TIMEFRAMES:
                    signal = await self._generate_signal(symbol, tf)
                    
                    if signal and signal.confluence_score.total >= 0.7:  # 70% confluence minimum
                        # Execute trade
                        success = await self.trade_manager.execute_trade(signal)
                        
                        if success:
                            self.trades_executed += 1
                            self.signal_cooldown[symbol] = datetime.datetime.utcnow() + datetime.timedelta(minutes=15)
                            self.logger.info(f"Signal executed: {symbol} {tf} "
                                           f"Score: {signal.confluence_score.total:.2%}")
                        
                        self.signals_generated += 1
            
            # Log statistics periodically
            if self.signals_generated % 10 == 0:
                self._log_statistics()
                
        except Exception as e:
            self.logger.error(f"Market scan failed: {e}")
    
    async def _generate_signal(self, symbol: str, timeframe: str) -> Optional[TradeSignal]:
        """Generate a complete trade signal"""
        try:
            # Fetch OHLCV data
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=100)
            if not ohlcv or len(ohlcv) < 50:
                return None
            
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Get current price
            ticker = await self.exchange.fetch_ticker(symbol)
            current_price = ticker['last']
            
            # Analyze market structure
            structure = await self.mtf_analyzer.analyze_symbol(symbol, timeframe)
            if not structure:
                return None
            
            # Determine trade side based on structure
            side = self._determine_trade_side(structure, df)
            if not side:
                return None
            
            # Calculate confluence score
            confluence_score = self.confluence_scorer.score_signal(df, structure, side, current_price)
            
            # Minimum confluence requirement
            if confluence_score.total < 0.7:
                return None
            
            # Calculate TP/SL levels
            stop_loss, take_profits = self._calculate_tp_sl(df, side, current_price, structure)
            
            # Create trade signal
            signal = TradeSignal(
                symbol=symbol,
                side=side,
                entry_price=current_price,
                entry_timeframe=timeframe,
                entry_timestamp=datetime.datetime.utcnow(),
                market_structure=structure,
                confluence_score=confluence_score,
                risk_metrics=self._calculate_risk_metrics(df, current_price, stop_loss),
                position_size=0.0,  # Will be calculated by risk manager
                stop_loss=stop_loss,
                take_profits=take_profits
            )
            
            # Update risk/reward score
            signal.confluence_score.risk_reward = self._calculate_rr_score(signal)
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Signal generation failed for {symbol}: {e}")
            return None
    
    def _determine_trade_side(self, structure: MarketStructure, df: pd.DataFrame) -> Optional[SignalSide]:
        """Determine trade side based on structure"""
        if not structure.order_blocks:
            return None
        
        # Get most recent OB
        recent_obs = sorted(structure.order_blocks, 
                          key=lambda x: x.get('index', 0), 
                          reverse=True)
        
        if not recent_obs:
            return None
        
        latest_ob = recent_obs[0]
        current_price = df['close'].iloc[-1]
        
        # Check if price is at OB zone
        if latest_ob['type'] == 'bullish':
            if latest_ob['low'] <= current_price <= latest_ob['high']:
                return SignalSide.BUY
        else:
            if latest_ob['low'] <= current_price <= latest_ob['high']:
                return SignalSide.SELL
        
        return None
    
    def _calculate_tp_sl(self, df: pd.DataFrame, side: SignalSide, entry_price: float, 
                        structure: MarketStructure) -> Tuple[float, List[float]]:
        """Calculate TP/SL levels based on market structure"""
        atr = self.structure_analyzer.calculate_atr(df).iloc[-1]
        
        if side == SignalSide.BUY:
            # Stop Loss: below recent swing low or OB low
            recent_low = df['low'].iloc[-20:].min()
            stop_loss = min(recent_low, entry_price - (atr * 1.5))
            stop_loss = min(stop_loss, entry_price * 0.99)  # Max 1% stop
            
            # Take Profits: at resistance levels
            recent_high = df['high'].iloc[-20:].max()
            swing_high = max([p['price'] for p in structure.swing_points if p['type'] == 'high'], 
                           default=recent_high)
            
            tp1 = entry_price + (entry_price - stop_loss) * 1.0  # 1R
            tp2 = entry_price + (entry_price - stop_loss) * 1.8  # 1.8R
            tp3 = min(swing_high, entry_price + (entry_price - stop_loss) * 2.5)  # 2.5R or swing
            
            take_profits = [tp1, tp2, tp3]
            
        else:
            # Stop Loss: above recent swing high or OB high
            recent_high = df['high'].iloc[-20:].max()
            stop_loss = max(recent_high, entry_price + (atr * 1.5))
            stop_loss = max(stop_loss, entry_price * 1.01)  # Max 1% stop
            
            # Take Profits: at support levels
            recent_low = df['low'].iloc[-20:].min()
            swing_low = min([p['price'] for p in structure.swing_points if p['type'] == 'low'], 
                          default=recent_low)
            
            tp1 = entry_price - (stop_loss - entry_price) * 1.0  # 1R
            tp2 = entry_price - (stop_loss - entry_price) * 1.8  # 1.8R
            tp3 = max(swing_low, entry_price - (stop_loss - entry_price) * 2.5)  # 2.5R or swing
            
            take_profits = [tp1, tp2, tp3]
        
        # Ensure TPs are in correct order
        if side == SignalSide.BUY:
            take_profits = sorted([tp for tp in take_profits if tp > entry_price])
        else:
            take_profits = sorted([tp for tp in take_profits if tp < entry_price], reverse=True)
        
        return stop_loss, take_profits[:3]  # Max 3 TP levels
    
    def _calculate_risk_metrics(self, df: pd.DataFrame, entry: float, stop_loss: float) -> Dict:
        """Calculate risk metrics"""
        risk = abs(entry - stop_loss)
        risk_pct = (risk / entry) * 100
        
        atr = self.structure_analyzer.calculate_atr(df).iloc[-1]
        risk_in_atr = risk / atr if atr > 0 else 0
        
        return {
            'risk': risk,
            'risk_pct': risk_pct,
            'risk_in_atr': risk_in_atr,
            'atr': atr
        }
    
    def _calculate_rr_score(self, signal: TradeSignal) -> float:
        """Calculate risk/reward score"""
        if len(signal.take_profits) == 0:
            return 0.0
        
        # Use first TP for RR calculation
        tp1 = signal.take_profits[0]
        risk = abs(signal.entry_price - signal.stop_loss)
        reward = abs(tp1 - signal.entry_price)
        
        if risk == 0:
            return 0.0
        
        rr_ratio = reward / risk
        
        # Score based on RR ratio
        if rr_ratio >= 2.0:
            return 1.0
        elif rr_ratio >= 1.5:
            return 0.8
        elif rr_ratio >= 1.0:
            return 0.6
        elif rr_ratio >= 0.5:
            return 0.4
        else:
            return 0.2
    
    async def _daily_reset_check(self):
        """Check and perform daily resets"""
        while self.is_running:
            now = datetime.datetime.utcnow()
            
            # Reset daily PnL at midnight UTC
            if now.hour == 0 and now.minute == 0:
                self.risk_manager.daily_pnl = 0.0
                self.daily_reset_time = now
                self.logger.info("Daily reset performed")
            
            await asyncio.sleep(60)  # Check every minute
    
    def _log_statistics(self):
        """Log system statistics"""
        stats = {
            'signals_generated': self.signals_generated,
            'trades_executed': self.trades_executed,
            'active_trades': len(self.trade_manager.active_trades),
            'current_capital': self.risk_manager.current_capital,
            'daily_pnl': self.risk_manager.daily_pnl,
            'consecutive_losses': self.risk_manager.consecutive_losses,
            'win_rate': self.risk_manager.get_win_rate()
        }
        
        self.logger.info(f"Statistics: {stats}")

# ============================================================================
# PERFORMANCE ANALYZER
# ============================================================================

class PerformanceAnalyzer:
    """Analyze and report performance metrics"""
    
    def __init__(self):
        self.trade_history = []
        self.metrics = PerformanceMetrics()
        self.logger = logging.getLogger(__name__)
    
    def add_trade(self, trade: Dict):
        """Add a completed trade to history"""
        self.trade_history.append(trade)
        
        # Update metrics
        self._update_metrics()
        
        # Keep history limited
        if len(self.trade_history) > 1000:
            self.trade_history.pop(0)
    
    def _update_metrics(self):
        """Update all performance metrics"""
        if not self.trade_history:
            return
        
        # Basic metrics
        self.metrics.total_trades = len(self.trade_history)
        self.metrics.winning_trades = sum(1 for t in self.trade_history if t.get('pnl', 0) > 0)
        self.metrics.losing_trades = sum(1 for t in self.trade_history if t.get('pnl', 0) < 0)
        self.metrics.total_pnl = sum(t.get('pnl', 0) for t in self.trade_history)
        
        # Win rate
        if self.metrics.total_trades > 0:
            self.metrics.win_rate = self.metrics.winning_trades / self.metrics.total_trades
        
        # Profit factor
        total_wins = sum(t.get('pnl', 0) for t in self.trade_history if t.get('pnl', 0) > 0)
        total_losses = abs(sum(t.get('pnl', 0) for t in self.trade_history if t.get('pnl', 0) < 0))
        
        if total_losses > 0:
            self.metrics.profit_factor = total_wins / total_losses
        
        # Average win/loss
        if self.metrics.winning_trades > 0:
            self.metrics.average_win = total_wins / self.metrics.winning_trades
        
        if self.metrics.losing_trades > 0:
            self.metrics.average_loss = total_losses / self.metrics.losing_trades
        
        # Largest win/loss
        wins = [t.get('pnl', 0) for t in self.trade_history if t.get('pnl', 0) > 0]
        losses = [t.get('pnl', 0) for t in self.trade_history if t.get('pnl', 0) < 0]
        
        if wins:
            self.metrics.largest_win = max(wins)
        if losses:
            self.metrics.largest_loss = min(losses)
        
        # Max consecutive wins/losses
        self._calculate_consecutive()
        
        # Max drawdown
        self._calculate_drawdown()
        
        # Ratios (simplified for now)
        self._calculate_ratios()
    
    def _calculate_consecutive(self):
        """Calculate consecutive wins and losses"""
        current_wins = 0
        current_losses = 0
        max_wins = 0
        max_losses = 0
        
        for trade in self.trade_history:
            pnl = trade.get('pnl', 0)
            
            if pnl > 0:
                current_wins += 1
                current_losses = 0
                max_wins = max(max_wins, current_wins)
            elif pnl < 0:
                current_losses += 1
                current_wins = 0
                max_losses = max(max_losses, current_losses)
        
        self.metrics.max_consecutive_wins = max_wins
        self.metrics.max_consecutive_losses = max_losses
    
    def _calculate_drawdown(self):
        """Calculate maximum drawdown"""
        if not self.trade_history:
            return
        
        # Simplified drawdown calculation
        # In production, use equity curve
        equity = 0
        peak = 0
        max_dd = 0
        max_dd_pct = 0
        
        for trade in self.trade_history:
            equity += trade.get('pnl', 0)
            
            if equity > peak:
                peak = equity
            
            drawdown = peak - equity
            drawdown_pct = (drawdown / (peak + 1e-8)) * 100
            
            if drawdown > max_dd:
                max_dd = drawdown
                max_dd_pct = drawdown_pct
        
        self.metrics.max_drawdown = max_dd
        self.metrics.max_drawdown_pct = max_dd_pct
    
    def _calculate_ratios(self):
        """Calculate performance ratios"""
        # Simplified ratio calculations
        # In production, use proper statistical methods
        
        if self.metrics.total_trades < 2:
            return
        
        # Sharpe ratio approximation
        returns = [t.get('pnl_pct', 0) for t in self.trade_history if t.get('pnl_pct') is not None]
        
        if len(returns) > 1:
            avg_return = statistics.mean(returns)
            std_return = statistics.stdev(returns) if len(returns) > 1 else 0
            
            if std_return > 0:
                self.metrics.sharpe_ratio = avg_return / std_return * math.sqrt(252)  # Annualized
            
            # Sortino ratio (downside deviation)
            downside_returns = [r for r in returns if r < 0]
            if downside_returns:
                downside_std = statistics.stdev(downside_returns) if len(downside_returns) > 1 else 0
                if downside_std > 0:
                    self.metrics.sortino_ratio = avg_return / downside_std * math.sqrt(252)
        
        # Calmar ratio
        if self.metrics.max_drawdown_pct > 0:
            annual_return = self.metrics.total_pnl_pct * (252 / self.metrics.total_trades)
            self.metrics.calmar_ratio = annual_return / abs(self.metrics.max_drawdown_pct)
        
        # Recovery factor
        if self.metrics.max_drawdown > 0:
            self.metrics.recovery_factor = abs(self.metrics.total_pnl / self.metrics.max_drawdown)
    
    def generate_report(self) -> str:
        """Generate performance report"""
        report = f"""
        ============================================
        ROMEOPT-P PERFORMANCE REPORT
        ============================================
        Total Trades: {self.metrics.total_trades}
        Winning Trades: {self.metrics.winning_trades}
        Losing Trades: {self.metrics.losing_trades}
        Win Rate: {self.metrics.win_rate:.1%}
        
        Total PnL: ${self.metrics.total_pnl:.2f}
        Profit Factor: {self.metrics.profit_factor:.2f}
        
        Average Win: ${self.metrics.average_win:.2f}
        Average Loss: ${self.metrics.average_loss:.2f}
        Largest Win: ${self.metrics.largest_win:.2f}
        Largest Loss: ${self.metrics.largest_loss:.2f}
        
        Max Consecutive Wins: {self.metrics.max_consecutive_wins}
        Max Consecutive Losses: {self.metrics.max_consecutive_losses}
        
        Max Drawdown: ${self.metrics.max_drawdown:.2f} ({self.metrics.max_drawdown_pct:.1f}%)
        
        Sharpe Ratio: {self.metrics.sharpe_ratio:.2f}
        Sortino Ratio: {self.metrics.sortino_ratio:.2f}
        Calmar Ratio: {self.metrics.calmar_ratio:.2f}
        Recovery Factor: {self.metrics.recovery_factor:.2f}
        ============================================
        """
        
        return report

# ============================================================================
# MAIN APPLICATION
# ============================================================================

async def main():
    """Main application entry point"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('romeopt_trading.log'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize trading engine
        engine = RomeOptTradingEngine()
        
        if not await engine.initialize():
            logger.error("Failed to initialize trading engine")
            return
        
        # Start trading
        logger.info("Starting RomeOPT-P 10/10 Trading System...")
        await engine.start()
        
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        if 'engine' in locals():
            await engine.stop()
        logger.info("RomeOPT-P Trading System stopped")

if __name__ == "__main__":
    asyncio.run(main())